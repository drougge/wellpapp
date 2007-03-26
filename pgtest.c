#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdint.h>
#include <libpq-fe.h>

#include <sys/types.h>
#include <md5.h>

#include "db.h"

void assert_fail(const char *ass, const char *file, const char *func, int line) {
	fprintf(stderr, "assertion \"%s\" failed in %s on %s:%d\n", ass, func, file, line);
	exit(1);
}

typedef struct md5 {
	uint8_t m[16];
} md5_t;

struct tag;
#define MAX_TAGS_PER_POST 64

typedef struct post {
	char       *source;
	time_t     created;
	md5_t      md5;
	uint16_t   uid;
	int16_t    score;
	uint16_t   width;
	uint16_t   height;
	struct tag *tags[MAX_TAGS_PER_POST];
} post_t;

#define TAG_POSTLIST_PER_NODE 62
#define MAX_TAGS  409600
#define MAX_POSTS 204800

typedef struct tag_postlist {
	struct tag_postlist *succ;
	struct tag_postlist *pred;
	post_t *posts[TAG_POSTLIST_PER_NODE];
} tag_postlist_t;

typedef struct tag {
	tag_postlist_t *head;
	tag_postlist_t *tail;
	tag_postlist_t *tailpred;
	uint32_t       holes;
	char           *name;
} tag_t;

typedef uint32_t tag_id_t;

rbtree_head_t *tagtree;
rbtree_head_t *posttree;

static void post_tag_add(post_t *post, tag_t *tag) {
	tag_postlist_t *pl;
	int i;

	assert(post);
	assert(tag);
	pl = tag->head;
	assert(pl);
	while (pl->succ) {
		for (i = 0; i < TAG_POSTLIST_PER_NODE; i++) {
			if (!pl->posts[i]) {
				pl->posts[i] = post;
				tag->holes--;
				return;
			}
		}
		pl = pl->succ;
	}
	pl = mm_alloc(sizeof(*pl));
	pl->posts[0]    = post;
	tag->holes     += TAG_POSTLIST_PER_NODE - 1;
	pl->succ        = tag->head;
	pl->pred        = (tag_postlist_t *)tag->head;
	tag->head->pred = pl;
	tag->head       = pl;
}

#if 0
static void post_tag_remove(post_t *post, tag_t *tag) {
	tag_postlist_t *pl;
	int i;

	assert(post);
	assert(tag);
	pl = tag->head;
	assert(pl);
	while (pl->succ) {
		for (i = 0; i < TAG_POSTLIST_PER_NODE; i++) {
			if (pl->posts[i] == post) {
				pl->posts[i] = NULL;
				tag->holes++;
				/* @@ if holes > cutoff ... */
				return;
			}
		}
		pl = pl->succ;
	}
	/* @@ warn/error? */
}
#endif

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
	assert(0);
	return 0;
}

static md5_t md5_str2md5(const char *str) {
	md5_t md5;
	int i;

	assert(strlen(str) == 32);
	for (i = 0; i < 16; i++) {
		md5.m[i] = md5_digit2digit(str[i * 2]) << 4 | md5_digit2digit(str[i * 2 + 1]);
	}
	return md5;
}

static const char *md5_md52str(md5_t md5) {
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

static rbtree_key_t name2hash(const char *name) {
	MD5_CTX ctx;
	unsigned char m[16];

	MD5Init(&ctx);
	MD5Update(&ctx, name, strlen(name));
	MD5Final(m, &ctx);
	return *(rbtree_key_t*)m;
}

static tag_t *find_tag(const char *name) {
	void *tag = NULL;
	rbtree_find(tagtree, &tag, name2hash(name));
	return (tag_t *)tag;
}

static int post_has_tag(post_t *post, tag_t *tag) {
	tag_postlist_t *pl;
	int i;

	assert(post);
	assert(tag);
	pl = tag->head;
	assert(pl);
	while (pl->succ) {
		for (i = 0; i < TAG_POSTLIST_PER_NODE; i++) {
			if (pl->posts[i] == post) return 1;
		}
		pl = pl->succ;
	}
	return 0;
}

static void test(void) {
	tag_t          *tag;
	tag_t          *filter_tag;
	tag_postlist_t *pl;
	int i;

	tag = find_tag("monochrome"); /* 4726 posts */
	assert(tag);
	filter_tag = find_tag("original"); /* 5196 posts */
	assert(filter_tag);
	pl = tag->head;
	assert(pl);
	/* Should give 244 results */
	while (pl->succ) {
		for (i = 0; i < TAG_POSTLIST_PER_NODE; i++) {
			if (pl->posts[i]) {
				if (post_has_tag(pl->posts[i], filter_tag)) {
					printf("%s\n", md5_md52str(pl->posts[i]->md5));
				}
			}
		}
		pl = pl->succ;
	}
}

static void add_tag(const char *name, tag_t *tag) {
	rbtree_key_t hash = name2hash(name);
	tag->name = mm_strdup(name);
	if (rbtree_insert(tagtree, tag, hash)) {
		assert(0);
	}
}

static int populate_from_db(PGconn *conn) {
	PGresult *res = NULL;
	int r = 0;
	int cols, rows;
	int i;
	tag_t  **tags  = NULL;
	post_t **posts = NULL;

	tags  = calloc(MAX_TAGS , sizeof(void *));
	posts = calloc(MAX_POSTS, sizeof(void *));
	// res = PQexec(conn, "SELECT id, created_at, user_id, score, source, md5, rating, width, height, file_ext FROM posts");
	res = PQexec(conn, "SELECT id, created_at, user_id, score, source, md5, width, height FROM posts");
	err(!res, 2);
	err(PQresultStatus(res) != PGRES_TUPLES_OK, 3);
	rows = PQntuples(res);
	cols = PQnfields(res);
	printf("posts: %d %d\n", rows, cols);
	for (i = 0; i < rows; i++) {
		post_t *post;
		char   *source;

		post = mm_alloc(sizeof(*post));
		post->created = time_str2unix(PQgetvalue(res, i, 1));
		post->uid     = atol(PQgetvalue(res, i, 2));
		post->score   = atol(PQgetvalue(res, i, 3));
		post->md5     = md5_str2md5(PQgetvalue(res, i, 5));
		post->width   = atol(PQgetvalue(res, i, 6));
		post->height  = atol(PQgetvalue(res, i, 7));
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
			tag->head = (tag_postlist_t *)&tag->tail;
			tag->tail = NULL;
			tag->tailpred = (tag_postlist_t *)tag;
			tags[tag_id] = tag;
		}
		post_tag_add(posts[atol(PQgetvalue(res, i, 0))], tag);
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
			add_tag(PQgetvalue(res, i, 1), tag);
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

int main(void) {
	PGconn *conn;
	int r = 0;

	printf("initing mm..\n");
	if (mm_init("/tmp/db.datastore", &posttree, &tagtree, 1)) {
		printf("populating..\n");
		conn = PQconnectdb("user=danbooru");
		// conn = PQconnectdb("user=danbooru password=db host=db");
		err(!conn, 2);
		err(PQstatus(conn) != CONNECTION_OK, 2);
		err(populate_from_db(conn), 3);
	}
	printf("testing..\n");
	test();
	mm_print();
err:
	return r;
}
