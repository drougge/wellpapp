#include "db.h"

#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <poll.h>
#include <errno.h>
#include <md5.h>

void assert_fail(const char *ass, const char *file,
                 const char *func, int line) {
	fprintf(stderr, "assertion \"%s\" failed in %s on %s:%d\n",
	        ass, func, file, line);
	exit(1);
}

ss128_head_t *tagtree;
ss128_head_t *tagaliastree;
ss128_head_t *tagguidtree;
ss128_head_t *posttree;
ss128_head_t *usertree;

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

static int md5_digit2digit(int digit) {
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

const char *md5_md52str(const md5_t md5) {
	static char buf[33];
	static const char digits[] = {'0', '1', '2', '3', '4', '5', '6', '7',
	                              '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
	int i;

	for (i = 0; i < 16; i++) {
		buf[i * 2    ] = digits[md5.m[i] >> 4];
		buf[i * 2 + 1] = digits[md5.m[i] & 15];
	}
	buf[32] = 0;
	return buf;
}

int str2id(const char *str, const char * const *ids) {
	int id   = 0;
	int sign = 1;
	if (*str == '-') {
		sign = -1;
		str++;
	}
	while (ids[id]) {
		if (!strcmp(str, ids[id])) return (id + 1) * sign;
		id++;
	}
	return 0;
}

tag_t *tag_find_guid(const guid_t guid) {
	void *tag = NULL;
	ss128_find(tagguidtree, &tag, guid.key);
	return (tag_t *)tag;
}

tag_t *tag_find_guidstr(const char *guidstr) {
	guid_t guid;
	if (guid_str2guid(&guid, guidstr, GUIDTYPE_TAG)) return NULL;
	return tag_find_guid(guid);
}

tag_t *tag_find_name(const char *name) {
	ss128_key_t hash = ss128_str2key(name);
	void        *tag = NULL;

	ss128_find(tagtree, &tag, hash);
	if (!tag) {
		ss128_find(tagaliastree, &tag, hash);
		if (tag) tag = ((tagalias_t *)tag)->tag;
	}
	return (tag_t *)tag;
}

int post_has_tag(const post_t *post, const tag_t *tag, truth_t weak) {
	assert(post);
	assert(tag);
again:
	if (post->of_tags < tag->of_posts) {
		const post_taglist_t *tl;
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
		const tag_postlist_t *pl;
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
	return ss128_find(posttree, (void *)res_post, md5.key);
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

static void populate_from_log_line(char *line) {
	int r;
	switch (*line) {
		case 'A': // 'A'dd something
			r = prot_add(logconn, line + 1);
			break;
		case 'T': // 'T'ag post
			r = prot_tag_post(logconn, line + 1);
			break;
		case 'M': // 'M'odify post
			r = prot_modify(logconn, line + 1);
			break;
		default:
			printf("Log: What? %s\n", line);
			r = 1;
	}
	assert(!r);
}

#define MAX_CONCURRENT_TRANSACTIONS 64
static int find_trans(trans_id_t *trans, trans_id_t needle) {
	int i;
	for (i = 0; i < MAX_CONCURRENT_TRANSACTIONS; i++) {
		if (trans[i] == needle) return i;
	}
	return -1;
}

int populate_from_log(const char *filename, void (*callback)(const char *line)) {
	FILE       *fh;
	char       buf[4096];
	trans_id_t trans[MAX_CONCURRENT_TRANSACTIONS] = {0};
	int        len;

	fh = fopen(filename, "r");
	if (!fh) {
		assert(errno == ENOENT);
		return 1;
	}
	while ((len = read_log_line(fh, buf, sizeof(buf)))) {
		char       *end;
		trans_id_t tid = strtoull(buf + 1, &end, 16);
		assert(end == buf + 17);
		if (*buf == 'T') { // New transaction
			assert(len == 34);
			if (buf[17] == 'O') { // Complete transaction
				int trans_pos = find_trans(trans, 0);
				assert(trans_pos != -1);
				trans[trans_pos] = tid;
				logconn->trans.now = strtoull(buf + 18, &end, 16);
				assert(end == buf + 34);
			} else if (buf[17] == 'U') { // Unfinished transaction
				// Do nothing
			} else { // What?
				assert(0);
			}
		} else if (*buf == 'D') { // Data from transaction
			assert(len > 18);
			if (find_trans(trans, tid) >= 0) {
				populate_from_log_line(buf + 18);
			} else {
				printf("Skipping data from incomplete transaction: %s\n", buf);
			}
		} else if (*buf == 'E') { // End of transaction
			int pos;
			assert(len == 17);
			pos = find_trans(trans, tid);
			if (pos != -1) trans[pos] = 0;
		} else { // What?
			assert(callback);
			callback(buf);
		}
	}
	return 0;
}

#define MAX_CONNECTIONS 100

struct pollfd fds[MAX_CONNECTIONS + 1];
connection_t connections[MAX_CONNECTIONS];
int connection_count = 0;
int server_running = 1;
static user_t anonymous;

static void new_connection(void) {
	int s = accept(fds[MAX_CONNECTIONS].fd, NULL, NULL);
	if (s < 0) {
		perror("accept");
	} else {
		connection_t *conn;
		int           i;

		for (i = 0; i < MAX_CONNECTIONS; i++) {
			if (fds[i].fd == -1) break;
		}
		if (i == MAX_CONNECTIONS) {
			close(s);
			return;
		}
		fds[i].fd = s;
		fds[i].revents = 0;
		conn = &connections[i];
		memset(conn, 0, sizeof(*conn));
		conn->sock = s;
		conn->user = &anonymous;
		conn->flags = CONNFLAG_GOING;
		conn->error = client_error;
		connection_count++;
	}
}

void db_serve(void) {
	int s, r, one, i;
	struct sockaddr_in addr;

	anonymous.name = "A";
	anonymous.caps = DEFAULT_CAPS;
	for (i = 0; i < MAX_CONNECTIONS + 1; i++) {
		fds[i].fd = -1;
		fds[i].events = POLLIN;
	}

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
	fds[MAX_CONNECTIONS].fd = s;

	while (server_running) {
		r = poll(fds, MAX_CONNECTIONS + 1, INFTIM);
		if (r == -1) {
			assert(!server_running);
			return;
		}
		if (fds[MAX_CONNECTIONS].revents & POLLIN) new_connection();
		for (i = 0; i < MAX_CONNECTIONS; i++) {
			if (fds[i].revents & POLLIN) {
				client_read_data(&connections[i]);
				if (client_get_line(&connections[i]) > 0) {
					client_handle(&connections[i]);
				}
				if (!(connections[i].flags & CONNFLAG_GOING)) {
					close(fds[i].fd);
					fds[i].fd = -1;
					connection_count--;
				}
			}
		}
	}
}

static const char *strndup(const char *str, unsigned int len) {
	char *res = malloc(len + 1);
	memcpy(res, str, len);
	res[len] = '\0';
	return res;
}

static void cfg_parse_list(const char * const **res_list, const char *str) {
	int          words = 1;
	int          word;
	const char   **list;
	const char   *p;
	unsigned int len;

	p = str;
	while (*p) if (*p++ == ' ') words++;
	list = malloc(sizeof(const char *) * (words + 1));
	p = str;
	word = 0;
	while (*p) {
		str = p;
		assert(*p != ' ');
		while (*p && *p != ' ') p++;
		len = p - str;
		if (*p) p++;
		assert(word < words);
		list[word++] = strndup(str, len);
	}
	list[word] = NULL;
	assert(word == words);
	*res_list = list;
}

const char * const *tagtype_names = NULL;
const char * const *rating_names = NULL;
const char * const *filetype_names = NULL;
const char * const *cap_names = NULL;

const char *basedir = NULL;

const guid_t *server_guid = NULL;
static guid_t server_guid_;

md5_t config_md5;

void db_read_cfg(const char *filename) {
	char    buf[1024];
	FILE    *fh;
	MD5_CTX ctx;

	MD5Init(&ctx);
	fh = fopen(filename, "r");
	assert(fh);
	while (fgets(buf, sizeof(buf), fh)) {
		int len = strlen(buf);
		assert(len && buf[len - 1] == '\n');
		MD5Update(&ctx, (unsigned char *)buf, len);
		buf[len - 1] = '\0';
		if (!memcmp("tagtypes=", buf, 9)) {
			cfg_parse_list(&tagtype_names, buf + 9);
		} else if (!memcmp("ratings=", buf, 8)) {
			cfg_parse_list(&rating_names, buf + 8);
		} else if (!memcmp("basedir=", buf, 8)) {
			basedir = strdup(buf + 8);
			assert(basedir && *basedir == '/');
		} else if (!memcmp("guid=", buf, 5)) {
			int r = guid_str2guid(&server_guid_, buf + 5, GUIDTYPE_SERVER);
			assert(!r);
			server_guid = &server_guid_;
		} else {
			assert(*buf == '\0' || *buf == '#');
		}
	}
	assert(feof(fh));
	fclose(fh);
	cfg_parse_list(&filetype_names, FILETYPE_NAMES_STR);
	cfg_parse_list(&cap_names, CAP_NAMES_STR);
	assert(tagtype_names && rating_names && basedir && server_guid);
	MD5Final(config_md5.m, &ctx);
}
