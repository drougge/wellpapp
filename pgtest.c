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
rbtree_head_t *posttree;

static void post_tag_add(post_t *post, tag_t *tag) {
	tag_postlist_t *pl, *ppl = NULL;
	post_taglist_t *tl, *ptl = NULL;
	int i;

	assert(post);
	assert(tag);
	pl = &tag->posts;
	tl = &post->tags;
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
				return;
			}
		}
		ppl = pl;
		pl  = pl->next;
	}
	pl = mm_alloc(sizeof(*pl));
	pl->posts[0]   = post;
	tag->of_holes += TAG_POSTLIST_PER_NODE - 1;
	ppl->next      = pl;
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

static rbtree_key_t name2hash(const char *name) {
	MD5_CTX ctx;
	md5_t   md5;

	MD5Init(&ctx);
	MD5Update(&ctx, name, strlen(name));
	MD5Final(md5.m, &ctx);
	return md5.key;
}

tag_t *find_tag(const char *name) {
	void *tag = NULL;
	rbtree_find(tagtree, &tag, name2hash(name));
	return (tag_t *)tag;
}

int post_has_tag(post_t *post, tag_t *tag) {
	assert(post);
	assert(tag);
	if (post->of_tags < tag->of_posts) {
		post_taglist_t *tl = &post->tags;
		while (tl) {
			int i;
			for (i = 0; i < POST_TAGLIST_PER_NODE; i++) {
				if (tl->tags[i] == tag) return 1;
			}
			tl = tl->next;
		}
	} else {
		tag_postlist_t *pl = &tag->posts;
		while (pl) {
			int i;
			for (i = 0; i < TAG_POSTLIST_PER_NODE; i++) {
				if (pl->posts[i] == post) return 1;
			}
			pl = pl->next;
		}
	}
	return 0;
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
		post->created  = time_str2unix(PQgetvalue(res, i, 1));
		post->uid      = atol(PQgetvalue(res, i, 2));
		post->score    = atol(PQgetvalue(res, i, 3));
		post->md5      = md5_str2md5(PQgetvalue(res, i, 5));
		post->width    = atol(PQgetvalue(res, i, 6));
		post->height   = atol(PQgetvalue(res, i, 7));
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
			tags[tag_id]  = tag;
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

int main(void) {
	PGconn *conn;
	int r = 0;

	printf("initing mm..\n");
	if (mm_init("/tmp/db.datastore", &posttree, &tagtree, !access("/tmp/db.datastore/0.db", F_OK))) {
		printf("populating..\n");
		conn = PQconnectdb("user=danbooru");
		// conn = PQconnectdb("user=danbooru password=db host=db");
		err(!conn, 2);
		err(PQstatus(conn) != CONNECTION_OK, 2);
		err(populate_from_db(conn), 3);
	}
	/*
	mm_print();
	printf("mapd   %p\nstackd %p\nheapd  %p.\n", (void *)posttree, (void *)&conn, (void *)malloc(4));
	*/
	printf("serving..\n");
	serve();
err:
	return r;
}
