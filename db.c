#include "db.h"

#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>

void assert_fail(const char *ass, const char *file, const char *func, int line) {
	fprintf(stderr, "assertion \"%s\" failed in %s on %s:%d\n", ass, func, file, line);
	exit(1);
}

rbtree_head_t *tagtree;
rbtree_head_t *tagaliastree;
rbtree_head_t *tagguidtree;
rbtree_head_t *posttree;
rbtree_head_t *usertree;

extern user_t *loguser;

// @@TODO: Locking/locklessness.
int post_tag_add(post_t *post, tag_t *tag, truth_t weak) {
	tag_postlist_t *pl, *ppl = NULL;
	post_taglist_t *tl, *ptl = NULL;
	int i;

	assert(post);
	assert(tag);
	assert(weak == T_YES || weak == T_NO);
	if (post_has_tag(post, tag, T_DONTCARE)) return 1;
	if (weak) {
		pl = tag->weak_posts;
		if (!pl) pl = tag->weak_posts = mm_alloc(sizeof(*pl));
		tl = post->weak_tags;
		if (!tl) tl = post->weak_tags = mm_alloc(sizeof(*tl));
	} else {
		pl = &tag->posts;
		tl = &post->tags;
	}
	tag->of_posts++;
	post->of_tags++;
	while (tl) {
		for (i = 0; i < POST_TAGLIST_PER_NODE; i++) {
			if (!tl->tags[i]) {
				tl->tags[i] = tag;
				post->of_holes--;
				goto pt_ok;
			}
		}
		ptl = tl;
		tl  = tl->next;
	}
	tl = mm_alloc(sizeof(*tl));
	tl->tags[0]     = tag;
	post->of_holes += POST_TAGLIST_PER_NODE - 1;
	ptl->next       = tl;

pt_ok:
	while (pl) {
		for (i = 0; i < TAG_POSTLIST_PER_NODE; i++) {
			if (!pl->posts[i]) {
				pl->posts[i] = post;
				tag->of_holes--;
				return 0;
			}
		}
		ppl = pl;
		pl  = pl->next;
	}
	pl = mm_alloc(sizeof(*pl));
	pl->posts[0]   = post;
	tag->of_holes += TAG_POSTLIST_PER_NODE - 1;
	ppl->next      = pl;
	return 0;
}

static int md5_digit2digit(char digit) {
	if (digit >= '0' && digit <= '9') return digit - '0';
	if (digit >= 'a' && digit <= 'f') return digit - 'a' + 10;
	if (digit >= 'A' && digit <= 'F') return digit - 'A' + 10;
	return -1;
}

int md5_str2md5(md5_t *res_md5, const char *md5str) {
	int i;

	if (strlen(md5str) != 32) return 1;
	for (i = 0; i < 16; i++) {
		int l = md5_digit2digit(md5str[i * 2]);
		int r = md5_digit2digit(md5str[i * 2 + 1]);
		if (l < 0 || r < 0) return 1;
		res_md5->m[i] = l << 4 | r;
	}
	return 0;
}

const char *md5_md52str(md5_t md5) {
	static char buf[33];
	static const char digits[] = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
	int i;

	for (i = 0; i < 16; i++) {
		buf[i * 2    ] = digits[md5.m[i] >> 4];
		buf[i * 2 + 1] = digits[md5.m[i] & 15];
	}
	buf[32] = 0;
	return buf;
}

tag_t *tag_find_guid(const guid_t guid) {
	void         *tag = NULL;
	rbtree_find(tagguidtree, &tag, guid.key);
	return (tag_t *)tag;
}

tag_t *tag_find_guidstr(const char *guidstr) {
	guid_t guid;
	if (guid_str2guid(&guid, guidstr, GUIDTYPE_TAG)) return NULL;
	return tag_find_guid(guid);
}

tag_t *tag_find_name(const char *name) {
	rbtree_key_t hash = rbtree_str2key(name);
	void         *tag = NULL;

	rbtree_find(tagtree, &tag, hash);
	if (!tag) {
		rbtree_find(tagaliastree, &tag, hash);
		if (tag) tag = ((tagalias_t *)tag)->tag;
	}
	return (tag_t *)tag;
}

int post_has_tag(post_t *post, tag_t *tag, truth_t weak) {
	assert(post);
	assert(tag);
again:
	if (post->of_tags < tag->of_posts) {
		post_taglist_t *tl;
		if (weak == T_NO) {
			tl = &post->tags;
		} else {
			tl = post->weak_tags;
		}
		while (tl) {
			int i;
			for (i = 0; i < POST_TAGLIST_PER_NODE; i++) {
				if (tl->tags[i] == tag) return 1;
			}
			tl = tl->next;
		}
	} else {
		tag_postlist_t *pl;
		if (weak == T_NO) {
			pl = &tag->posts;
		} else {
			pl = tag->weak_posts;
		}
		while (pl) {
			int i;
			for (i = 0; i < TAG_POSTLIST_PER_NODE; i++) {
				if (pl->posts[i] == post) return 1;
			}
			pl = pl->next;
		}
	}
	if (weak == T_DONTCARE) {
		weak = T_NO;
		goto again;
	}
	return 0;
}

int post_find_md5str(post_t **res_post, const char *md5str) {
	md5_t md5;
	*res_post = NULL;
	if (md5_str2md5(&md5, md5str)) return -1;
	return rbtree_find(posttree, (void *)res_post, md5.key);
}

static int read_log_line(FILE *fh, char *buf, int len) {
	if (!fgets(buf, len, fh)) {
		assert(feof(fh));
		return 0;
	}
	len = strlen(buf) - 1;
	assert(len > 8 && buf[len] == '\n');
	buf[len] = 0;
	return len;
}

static int dummy_error(const char *msg) {
	(void)msg;
	return 1;
}

static void populate_from_log_line(char *line) {
	int r;
	switch (*line) {
		case 'A': // 'A'dd something
			r = prot_add(loguser, line + 1, NULL, dummy_error);
			break;
		case 'T': // 'T'ag post
			r = prot_tag_post(loguser, line + 1, NULL, dummy_error);
			break;
		case 'M': // 'M'odify post
			r = prot_modify(loguser, line + 1, NULL, dummy_error);
			break;
		default:
			printf("Log: What? %s\n", line);
			r = 1;
	}
	assert(!r);
}

#define MAX_CONCURRENT_TRANSACTIONS 64
static int find_trans(uint32_t *trans, uint32_t needle) {
	int i;
	for (i = 0; i < MAX_CONCURRENT_TRANSACTIONS; i++) {
		if (trans[i] == needle) return i;
	}
	return -1;
}

void populate_from_log(const char *filename) {
	FILE     *fh;
	char     buf[4096];
	uint32_t trans[MAX_CONCURRENT_TRANSACTIONS] = {0};
	int      len;

	fh = fopen(filename, "r");
	assert(fh);
	while ((len = read_log_line(fh, buf, sizeof(buf)))) {
		char     *end;
		uint32_t tid = strtoul(buf + 1, &end, 16);
		assert(end == buf + 9);
		if (*buf == 'T') { // New transaction
			assert(len == 10);
			if (buf[9] == 'O') { // Complete transaction
				int trans_pos = find_trans(trans, 0);
				assert(trans_pos != -1);
				trans[trans_pos] = tid;
			} else if (buf[9] == 'U') { // Unfinished transaction
				// Do nothing
			} else { // What?
				assert(0);
			}
		} else if (*buf == 'D') { // Data from transaction
			assert(len > 10);
			if (find_trans(trans, tid) >= 0) {
				populate_from_log_line(buf + 10);
			} else {
				printf("Skipping data from incomplete transaction: %s\n", buf);
			}
		} else if (*buf == 'E') { // End of transaction
			int pos;
			assert(len == 9);
			pos = find_trans(trans, tid);
			if (pos != -1) trans[pos] = 0;
		} else { // What?
			assert(0);
		}
	}
}

void db_serve(void) {
	int s, c, r, one;
	struct sockaddr_in addr;

	s = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
	assert(s >= 0);
	one = 1;
	r = setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
	assert(!r);
	memset(&addr, 0, sizeof(addr));
	addr.sin_len    = sizeof(addr);
	addr.sin_family = AF_INET;
	addr.sin_port   = htons(2225);
	r = bind(s, (struct sockaddr *)&addr, sizeof(addr));
	assert(!r);
	r = listen(s, 5);
	assert(!r);
	while (1) {
		log_rotate(0);
		c = accept(s, NULL, NULL);
		if (c < 0) {
			perror("accept");
		} else {
			pid_t pid;
			pid = rfork(RFFDG | RFPROC | RFNOWAIT);
			if (pid == -1) {
				perror("rfork");
			}
			if (!pid) client_handle(c);
			close(c);
		}
	}
}
