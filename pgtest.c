#include "db.h"

#include <time.h>
#include <libpq-fe.h>
#include <md5.h>
#include <sys/socket.h>
#include <netinet/in.h>

void assert_fail(const char *ass, const char *file, const char *func, int line) {
	fprintf(stderr, "assertion \"%s\" failed in %s on %s:%d\n", ass, func, file, line);
	exit(1);
}

#define MAX_TAGS  409600
#define MAX_POSTS 204800

rbtree_head_t *tagtree;
rbtree_head_t *tagaliastree;
rbtree_head_t *tagguidtree;
rbtree_head_t *posttree;

guid_t server_guid;

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

static time_t time_str2unix(const char *str) {
	struct tm time;
	char *p;
	memset(&time, 0, sizeof(time));
	p = strptime(str, "%Y-%m-%d %H:%M:%S", &time);
	assert(p && !*p);
	return mktime(&time);
}

static int md5_digit2digit(char digit) {
	if (digit >= '0' && digit <= '9') return digit - '0';
	if (digit >= 'a' && digit <= 'f') return digit - 'a' + 10;
	if (digit >= 'A' && digit <= 'F') return digit - 'A' + 10;
	return -1;
}

static int md5_str2md5(md5_t *res_md5, const char *md5str) {
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

static rbtree_key_t md5_key(const char *data, int len) {
	MD5_CTX ctx;
	md5_t   md5;

	MD5Init(&ctx);
	MD5Update(&ctx, data, len);
	MD5Final(md5.m, &ctx);
	return md5.key;
}

static rbtree_key_t guid2hash(const guid_t guid) {
	return md5_key((char *)&guid, sizeof(guid));
}

static rbtree_key_t name2hash(const char *name) {
	return md5_key(name, strlen(name));
}

tag_t *tag_find_guid(const guid_t guid) {
	rbtree_key_t hash = guid2hash(guid);
	void         *tag = NULL;

	rbtree_find(tagguidtree, &tag, hash);
	return (tag_t *)tag;
}

tag_t *tag_find_guidstr(const char *guidstr) {
	guid_t guid;
	if (guid_str2guid(&guid, guidstr)) return NULL;
	return tag_find_guid(guid);
}

tag_t *tag_find_name(const char *name) {
	rbtree_key_t hash = name2hash(name);
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

static void add_tag(const char *name, tag_t *tag) {
	tag->name = mm_strdup(name);
	if (rbtree_insert(tagtree, tag, name2hash(name))) {
		assert(0);
	}
	if (rbtree_insert(tagguidtree, tag, guid2hash(tag->guid))) {
		assert(0);
	}
}

static void add_tagalias(const char *name, tag_t *tag) {
	rbtree_key_t hash = name2hash(name);
	tagalias_t   *alias;
	alias = mm_alloc(sizeof(*alias));
	alias->name = mm_strdup(name);
	alias->tag  = tag;
	if (rbtree_insert(tagaliastree, alias, hash)) {
		assert(0);
	}
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
	assert(len > 16 && buf[len] == '\n');
	buf[len] = 0;
	return len;
}

static void populate_from_log_line(const char *line) {
	if (*line == 'C') {
		if (line[1] == 'T') {
			tag_t *tag;
			tag = mm_alloc(sizeof(*tag));
			add_tag(line + 2, tag);
		} else {
			char   m[33];
			post_t *post;
			char   *line_;
			int     r;

			assert(line[1] == 'P');
			post = mm_alloc(sizeof(*post));
			line += 2;
			assert(strlen(line) > 34); /* @ A bit more, really. */
			memcpy(m, line, 32);
			m[32] = 0;
			r = md5_str2md5(&post->md5, m);
			assert(!r);
			line += 33;
			post->width   = strtol(line, &line_, 0);
			post->height  = strtol(line_, &line_, 0);
			post->created = strtoll(line_, &line_, 0);
			post->uid     = 0; // @@
			post->score   = 0;
			post->source  = NULL;
			if (rbtree_insert(posttree, post, post->md5.key)) {
				assert(0);
			}
		}
	} else if (*line == 'P') {
		md5_t  md5;
		char   m[33];
		tag_t  *tag;
		post_t *post;
		int    r;

		line++;
		memcpy(m, line, 32);
		m[32] = 0;
		r = md5_str2md5(&md5, m);
		assert(!r);
		rbtree_find(posttree, (void *)&post, md5.key);
		assert(post);
		line += 33;
		if (!memcmp(line, "tag ", 4)) {
			line += 4;
			tag = tag_find_name(line);
if (!tag) printf("no tag '%s' %p '%s'\n",line,(void *)line,line-4);
			assert(tag);
			int r = post_tag_add(post, tag, T_NO);
			assert(!r);
		} else if (!memcmp(line, "source ", 7)) {
			line += 7;
			post->source = mm_strdup(line);
		} else {
			printf("P? %s\n", line);
		}
	} else {
		printf("? %s\n", line);
	}
}

static void populate_from_log(const char *filename) {
	uint64_t trans_id = 0;
	FILE     *fh;
	char     cmp[18];
	char     buf[4096];
	int      len;

	fh = fopen(filename, "r");
	assert(fh);
	while ((len = read_log_line(fh, buf, sizeof(buf)))) {
		if (trans_id) {
			if (!memcmp(cmp + 1, buf + 1, 16)) {
				if (*buf == 'E') {
					trans_id = 0;
				} else {
					assert(*buf == 'D');
					populate_from_log_line(buf + 18);
				}
			}
		} else if (*buf == 'S') {
			long pos = ftell(fh);
			int r;

			assert(pos > 0);
			assert(len == 17);
			memcpy(cmp, buf, 18);
			*cmp = 'E';
			while (42) {
				r = read_log_line(fh, buf, sizeof(buf));
				if (!r) {
					printf("Skipping incomplete transaction %s\n", cmp + 1);
					break;
				}
				if (!strcmp(buf, cmp)) {
					trans_id = strtoll(cmp + 1, NULL, 16);
					*cmp = 'D';
					break;
				}
			}
			r = fseek(fh, pos, SEEK_SET);
			assert(!r);
		}
	}
}

typedef struct {
	const char *ext;
	int        filetype;
} ext2ft_t;
static int ext2filetype(const char *ext) {
	int i;
	const ext2ft_t map[] = {
		{"GIF", FILETYPE_GIF},
		{"JPG", FILETYPE_JPEG},
		{"PNG", FILETYPE_PNG},
		{"Png", FILETYPE_PNG},
		{"bmp", FILETYPE_BMP},
		{"gif", FILETYPE_GIF},
		{"jpeg", FILETYPE_JPEG},
		{"jpg", FILETYPE_JPEG},
		{"png", FILETYPE_PNG},
		{"swf", FILETYPE_FLASH},
		{NULL, 0}
	};
//  1 avi html mp3 mp4 mpg pdf php?attachmentid=280808&d=1155260176 php?fn=147089 rar svg wmv zip
	for (i = 0; map[i].ext; i++) {
		if (!strcmp(map[i].ext, ext)) return map[i].filetype;
	}
	return -1;
}

static rating_t danboorurating2rating(const char *dr) {
	switch(*dr) {
		case 's': return RATING_SAFE;
		case 'q': return RATING_QUESTIONABLE;
		case 'e': return RATING_EXPLICIT;
		default : return RATING_UNSPECIFIED;
	}
}

static tagtype_t danboorutype2type[] = {
	TAGTYPE_UNSPECIFIED, // "general"
	TAGTYPE_ARTIST,
	TAGTYPE_AMBIGUOUS,
	TAGTYPE_COPYRIGHT,
	TAGTYPE_CHARACTER,
};

static int populate_from_db(PGconn *conn) {
	PGresult *res = NULL;
	int r = 0;
	int cols, rows;
	int i;
	tag_t  **tags  = NULL;
	post_t **posts = NULL;

	tags  = calloc(MAX_TAGS , sizeof(void *));
	posts = calloc(MAX_POSTS, sizeof(void *));
	res = PQexec(conn, "SELECT id, created_at, user_id, score, source, md5, width, height, file_ext, rating FROM posts");
	err(!res, 2);
	err(PQresultStatus(res) != PGRES_TUPLES_OK, 3);
	rows = PQntuples(res);
	cols = PQnfields(res);
	printf("posts: %d %d\n", rows, cols);
	for (i = 0; i < rows; i++) {
		post_t *post;
		char   *source;
		int    filetype;

		filetype = ext2filetype(PQgetvalue(res, i, 8));
		if (filetype < 0) continue; // @@
		post = mm_alloc(sizeof(*post));
		post->created  = time_str2unix(PQgetvalue(res, i, 1));
		post->uid      = atol(PQgetvalue(res, i, 2));
		post->score    = atol(PQgetvalue(res, i, 3));
		r = md5_str2md5(&post->md5, PQgetvalue(res, i, 5));
		assert(!r);
		post->width    = atol(PQgetvalue(res, i, 6));
		post->height   = atol(PQgetvalue(res, i, 7));
		post->rating   = danboorurating2rating(PQgetvalue(res, i, 9));
		post->filetype = filetype;
		source = PQgetvalue(res, i, 4);
		if (source && *source) {
			post->source = mm_strdup(source);
		} else {
			post->source = NULL;
		}
		posts[atol(PQgetvalue(res, i, 0))] = post;
		if (rbtree_insert(posttree, post, *(rbtree_key_t *)post->md5.m)) {
			assert(0);
		}
	}
	PQclear(res);
	res = NULL;
	printf("whee..\n");
	/* 31316K 27228K här */

	res = PQexec(conn, "SELECT post_id, tag_id FROM posts_tags");
	err(!res, 2);
	err(PQresultStatus(res) != PGRES_TUPLES_OK, 3);
	rows = PQntuples(res);
	cols = PQnfields(res);
	printf("posts_tags: %d %d\n", rows, cols);
	for (i = 0; i < rows; i++) {
		tag_t    *tag;
		tag_id_t tag_id;

		tag_id = atol(PQgetvalue(res, i, 1));
		assert(tag_id < MAX_TAGS);
		tag = tags[tag_id];
		if (!tag) {
			tag = mm_alloc(sizeof(*tag));
			tag->guid = guid_gen_tag_guid();
			tags[tag_id]  = tag;
		}
if (!posts[atol(PQgetvalue(res, i, 0))]) {
printf("Tag %d on post %s has no post\n",tag_id, PQgetvalue(res, i, 0));
} else
		r = post_tag_add(posts[atol(PQgetvalue(res, i, 0))], tag, T_NO);
		if (r) {
			printf("WARN: post %s already tagged as %d?\n", PQgetvalue(res, i, 0), tag_id);
		}
	}
	PQclear(res);
	res = NULL;
	printf("whee..\n");
	/* 38912K 34268K här */

	res = PQexec(conn, "SELECT id, name, tag_type FROM tags");
	err(!res, 2);
	err(PQresultStatus(res) != PGRES_TUPLES_OK, 3);
	rows = PQntuples(res);
	cols = PQnfields(res);
	printf("tags: %d %d\n", rows, cols);
	for (i = 0; i < rows; i++) {
		tag_t    *tag;
		tag_id_t tag_id;

		tag_id = atol(PQgetvalue(res, i, 0));
		assert(tag_id < MAX_TAGS);
		tag = tags[tag_id];
		if (tag) {
			tag->type = danboorutype2type[atol(PQgetvalue(res, i, 2))];
			add_tag(PQgetvalue(res, i, 1), tag);
		}
	}
	PQclear(res);
	res = NULL;
	printf("whee..\n");

	res = PQexec(conn, "SELECT a.name, t.name FROM tag_aliases a, tags t WHERE a.alias_id = t.id");
	err(!res, 2);
	err(PQresultStatus(res) != PGRES_TUPLES_OK, 3);
	rows = PQntuples(res);
	printf("tag_aliases: %d\n", rows);
	for (i = 0; i < rows; i++) {
		tag_t        *tag;
		const char   *name;

		name = PQgetvalue(res, i, 0);
		tag  = tag_find_name(PQgetvalue(res, i, 1));
		if (tag) {
			add_tagalias(name, tag);
		} else {
			printf("WARN: tag-alias '%s' has no tag\n", name);
		}
	}
	PQclear(res);
	res = NULL;
	printf("whee..\n");
err:
	if (res  ) PQclear(res);
	if (tags ) free(tags);
	if (posts) free(posts);
	return r;
}

static void serve(void) {
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

int main(int argc, char **argv) {
	int r = 0;
	int dump = 0;

	assert(argc == 2);
	r = guid_str2guid(&server_guid, "eTBfgp-qto48a-aaaaaa-aaaaaa");
	assert(!r);
	printf("initing mm..\n");
	if (mm_init("/tmp/db.datastore", !access("/tmp/db.datastore/0.db", F_OK))) {
		printf("populating from %s..\n", argv[1]);
		if (!strcmp(argv[1], "db")) {
			PGconn *conn = PQconnectdb("user=danbooru");
			// conn = PQconnectdb("user=danbooru password=db host=db");
			err(!conn, 2);
			err(PQstatus(conn) != CONNECTION_OK, 2);
			err(populate_from_db(conn), 3);
//			dump = 1;
		} else {
			populate_from_log(argv[1]);
		}
	}
	mm_print();
	/*
	printf("mapd   %p\nstackd %p\nheapd  %p.\n", (void *)posttree, (void *)&conn, (void *)malloc(4));
	*/
	if (dump) {
		printf("dumping..\n");
		dump_log("/tmp/db.log");
	}
	printf("serving..\n");
	serve();
err:
	return r;
}
