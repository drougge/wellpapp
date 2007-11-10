#include "db.h"

#include <stdarg.h>

#define PROT_TAGS_PER_SEARCH   16
#define PROT_ORDERS_PER_SEARCH 4

typedef enum {
	E_LINETOOLONG,
	E_READ,
	E_COMMAND,
	E_SYNTAX,
	E_OVERFLOW,
	E_MEM,
	E_AUTH,
} error_t;

static const char *errors[] = {
	"line too long",
	"read",
	"unknown command",
	"syntax error",
	"overflow",
	"out of memory",
	"bad auth",
};

typedef enum {
	ORDER_NONE,
	ORDER_DATE,
	ORDER_SCORE,
} order_t;

static const char *orders[] = {"date", "score", NULL};

static const char *flagnames[] = {"tagname", "tagguid", "ext", "created",
                                  "width", "height", "score", NULL};

static void c_printf(connection_t *conn, const char *fmt, ...);
static void FLAGPRINT_EXTENSION(connection_t *conn, post_t *post) {
	c_printf(conn, "%s", filetype_names[post->filetype]);
}
static void FLAGPRINT_DATE(connection_t *conn, post_t *post) {
	c_printf(conn, "%llu", (unsigned long long)post->created);
}
static void FLAGPRINT_WIDTH(connection_t *conn, post_t *post) {
	c_printf(conn, "%u", post->width);
}
static void FLAGPRINT_HEIGHT(connection_t *conn, post_t *post) {
	c_printf(conn, "%u", post->height);
}
static void FLAGPRINT_SCORE(connection_t *conn, post_t *post) {
	c_printf(conn, "%d", post->score);
}

typedef void (*flag_printer_t)(connection_t *, post_t *);
static const flag_printer_t flag_printers[] = {
	NULL,
	NULL,
	FLAGPRINT_EXTENSION,
	FLAGPRINT_DATE,
	FLAGPRINT_WIDTH,
	FLAGPRINT_HEIGHT,
	FLAGPRINT_SCORE,
};

typedef enum {
	FLAG_RETURN_TAGNAMES,
	FLAG_RETURN_TAGIDS,
	FLAG_RETURN_EXTENSION,
	FLAG_RETURN_DATE,
	FLAG_RETURN_WIDTH,
	FLAG_RETURN_HEIGHT,
	FLAG_RETURN_SCORE,
	FLAG_LAST,
} flag_t;
#define FLAG(n) (1 << (n))
#define FLAG_FIRST_SINGLE FLAG_RETURN_EXTENSION

typedef struct search_tag {
	tag_t   *tag;
	truth_t weak;
} search_tag_t;

typedef struct search {
	search_tag_t tags[PROT_TAGS_PER_SEARCH];
	search_tag_t excluded_tags[PROT_TAGS_PER_SEARCH];
	post_t       *post;
	unsigned int of_tags;
	unsigned int of_excluded_tags;
	order_t      orders[PROT_ORDERS_PER_SEARCH];
	unsigned int of_orders;
	int          flags;
} search_t;
static post_t null_post; /* search->post for not found posts */

typedef struct result {
	post_t **posts;
	uint32_t of_posts;
	uint32_t room;
} result_t;

#define OUTBUF_MIN_FREE 1024

static void c_flush(connection_t *conn) {
	if (conn->outlen) {
		ssize_t w = write(conn->sock, conn->outbuf, conn->outlen);
		assert(w > 0 && (unsigned int)w == conn->outlen);
		conn->outlen = 0;
	}
}

static void c_printf(connection_t *conn, const char *fmt, ...) {
	va_list ap;
	int     len;

	va_start(ap, fmt);
	len = vsnprintf(conn->outbuf + conn->outlen,
	                sizeof(conn->outbuf) - conn->outlen, fmt, ap);
	if (len >= (int)(sizeof(conn->outbuf) - conn->outlen)) { // Overflow
		c_flush(conn);
		len = vsnprintf(conn->outbuf, sizeof(conn->outbuf), fmt, ap);
		assert(len < (int)sizeof(conn->outbuf));
	}
	va_end(ap);
	conn->outlen += len;
	if (conn->outlen + OUTBUF_MIN_FREE > sizeof(conn->outbuf)) c_flush(conn);
}

int client_error(connection_t *conn, const char *what) {
	c_printf(conn, "RE %s\n", what);
	return 1;
}

static int close_error(connection_t *conn, error_t e) {
	c_printf(conn, "E%d %s\n", e, errors[e]);
	c_flush(conn);
	conn->flags &= ~CONNFLAG_GOING;
	return 1;
}

int client_get_line(connection_t *conn) {
	int len = 0;
	int size = sizeof(conn->linebuf);

	if (!(conn->flags & CONNFLAG_GOING)) return -1;
	while (size > len) {
		if (conn->getlen > conn->getpos) {
			char c = conn->getbuf[conn->getpos];
			conn->getpos++;
			/* \r is ignored, for easier testing with telnet */
			if (c == '\n') {
				conn->linebuf[len] = 0;
				return len;
			} else if (c != '\r') {
				conn->linebuf[len] = c;
				len++;
			}
		} else {
			conn->getpos = 0;
			conn->getlen = read(conn->sock, conn->getbuf,
			                    sizeof(conn->getbuf));
			if (conn->getlen <= 0) {
				close_error(conn, E_READ);
				return -1;
			}
		}
	}
	close_error(conn, E_LINETOOLONG);
	return -1;
}

static int sort_search(const void *_t1, const void *_t2) {
	const search_tag_t *t1 = (const search_tag_t *)_t1;
	const search_tag_t *t2 = (const search_tag_t *)_t2;
	if (t1->tag->of_posts < t2->tag->of_posts) return -1;
	if (t1->tag->of_posts > t2->tag->of_posts) return 1;
	return 0;
}

static int build_search_cmd(connection_t *conn, const char *cmd, void *search_,
                            prot_cmd_flag_t flags) {
	tag_t      *tag;
	truth_t    weak = T_DONTCARE;
	search_t   *search = search_;
	const char *args = cmd + 1;
	int        i;
	int        r;

	(void)flags;

	switch(*cmd) {
		case 'T': // Tag
		case 't': // Removed tag
			if (*args == '~') {
				args++;
				weak = T_YES;
			} else if (*args == '!') {
				args++;
				weak = T_NO;
			}
			if (*args == 'G') {
				tag = tag_find_guidstr(args + 1);
			} else if (*args == 'N') {
				tag = tag_find_name(args + 1);
			} else {
				return conn->error(conn, cmd);
			}
			if (!tag) return conn->error(conn, cmd);
			if (*cmd == 'T') {
				if (search->of_tags == PROT_TAGS_PER_SEARCH) {
					return close_error(conn, E_OVERFLOW);
				}
				search->tags[search->of_tags].tag  = tag;
				search->tags[search->of_tags].weak = weak;
				search->of_tags++;
			} else {
				if (search->of_excluded_tags == PROT_TAGS_PER_SEARCH) {
					return close_error(conn, E_OVERFLOW);
				}
				search->excluded_tags[search->of_excluded_tags].tag  = tag;
				search->excluded_tags[search->of_excluded_tags].weak = weak;
				search->of_excluded_tags++;
			}
			break;
		case 'O': // Ordering
			if (search->of_orders == PROT_ORDERS_PER_SEARCH) {
				return close_error(conn, E_OVERFLOW);
			}
			search->orders[search->of_orders] = str2id(args, orders);
			if (!search->orders[search->of_orders]) {
				return conn->error(conn, cmd);
			}
			search->of_orders++;
			break;
		case 'F': // Flag (option)
			i = str2id(args, flagnames);
			if (i < 1) return conn->error(conn, cmd);
			search->flags |= FLAG(i - 1);
			break;
		case 'M': // md5 (specific post)
			if (search->post) return conn->error(conn, cmd);
			r = post_find_md5str(&search->post, args);
			if (r < 0) return conn->error(conn, cmd);
			if (r > 0) { /* Not found */
				search->post = &null_post;
			}
			break;
		default:
			return close_error(conn, E_SYNTAX);
			break;
	}
	return 0;
}

static int build_search(connection_t *conn, char *cmd, search_t *search) {
	memset(search, 0, sizeof(*search));
	if (prot_cmd_loop(conn, cmd, search, build_search_cmd, CMDFLAG_NONE)) return 1;
	if (!search->of_tags && !search->post) {
		return conn->error(conn, "E Specify at least one included tag");
	}
	/* Searching is faster if ordered by post-count */
	qsort(search->tags, search->of_tags, sizeof(search_tag_t), sort_search);
	return 0;
}

static void result_free(result_t *result) {
	if (result->posts) free(result->posts);
}

static int add_post_to_result(post_t *post, result_t *result) {
	if (result->room == result->of_posts) {
		if (result->room == 0) {
			result->room = 64;
		} else {
			result->room *= 2;
		}
		post_t **p = realloc(result->posts, result->room * sizeof(post_t *));
		if (!p) return 1;
		result->posts = p;
	}
	result->posts[result->of_posts] = post;
	result->of_posts++;
	return 0;
}

static int remove_tag(result_t *result, tag_t *tag, truth_t weak) {
	result_t new_result;
	uint32_t i;

	memset(&new_result, 0, sizeof(new_result));
	for (i = 0; i < result->of_posts; i++) {
		post_t *post = result->posts[i];
		if (!post_has_tag(post, tag, weak)) {
			if (add_post_to_result(post, &new_result)) return 1;
		}
	}
	result_free(result);
	*result = new_result;
	return 0;
}

static int intersect(result_t *result, tag_t *tag, truth_t weak) {
	result_t new_result;
	memset(&new_result, 0, sizeof(new_result));
	if (result->of_posts) {
		uint32_t i;
		for (i = 0; i < result->of_posts; i++) {
			post_t *post = result->posts[i];
			if (post_has_tag(post, tag, weak)) {
				if (add_post_to_result(post, &new_result)) {
					return 1;
				}
			}
		}
	} else {
		tag_postlist_t *pl;
		pl = &tag->posts;
		while (pl) {
			uint32_t i;
			for (i = 0; i < TAG_POSTLIST_PER_NODE; i++) {
				if (pl->posts[i]) {
					int r = add_post_to_result(pl->posts[i],
					                           &new_result);
					if (r) return 1;
				}
			}
			pl = pl->next;
		}
	}
	result_free(result);
	*result = new_result;
	return 0;
}

static int sorter_date(const post_t *p1, const post_t *p2) {
	if (p1->created < p2->created) return -1;
	if (p1->created > p2->created) return 1;
	return 0;
}

static int sorter_score(const post_t *p1, const post_t *p2) {
	if (p1->score < p2->score) return -1;
	if (p1->score > p2->score) return 1;
	return 0;
}

typedef int (*sorter_f)(const post_t *p1, const post_t *p2);
static sorter_f sorters[] = {sorter_date, sorter_score};

static int sorter(void *_search, const void *_p1, const void *_p2) {
	const post_t *p1 = *(const post_t * const *)_p1;
	const post_t *p2 = *(const post_t * const *)_p2;
	search_t     *search = _search;
	unsigned int i;

	for (i = 0; i < search->of_orders; i++) {
		int order = search->orders[i];
		int sign = -1;
		int r;

		if (order < 0) {
			sign  = 1;
			order = -order;
		}
		r = sorters[order - 1](p1, p2);
		if (r) return r * sign;
	}
	return 0;
}

static void return_post(connection_t *conn, post_t *post, int flags) {
	int i;
	c_printf(conn, "RP%s", md5_md52str(post->md5));
	if (flags & (FLAG(FLAG_RETURN_TAGNAMES) | FLAG(FLAG_RETURN_TAGIDS))) {
		post_taglist_t *tags = &post->tags;
		const char     *prefix = "";
again:
		while (tags) {
			for (i = 0; i < POST_TAGLIST_PER_NODE; i++) {
				if (tags->tags[i]) {
					if (flags & FLAG(FLAG_RETURN_TAGNAMES)) {
						c_printf(conn, " T%s%s", prefix, tags->tags[i]->name);
					}
					if (flags & FLAG(FLAG_RETURN_TAGIDS)) {
						c_printf(conn, " G%s%s", prefix, guid_guid2str(tags->tags[i]->guid));
					}
				}
			}
			tags = tags->next;
		}
		if (!*prefix) {
			prefix = "~";
			tags = post->weak_tags;
			goto again;
		}
	}
	for (i = FLAG_FIRST_SINGLE; i < FLAG_LAST; i++) {
		if (flags & FLAG(i)) {
			c_printf(conn, " F%s=", flagnames[i]);
			flag_printers[i](conn, post);
		}
	}
	c_printf(conn, "\n");
}

static void do_search(connection_t *conn, search_t *search) {
	result_t result;
	unsigned int i;

	if (search->post) {
		if (search->of_tags || search->of_excluded_tags) {
			conn->error(conn, "E mutually exclusive options specified");
			return;
		}
		if (search->post != &null_post) {
			return_post(conn, search->post, search->flags);
		}
		goto done;
	}
	memset(&result, 0, sizeof(result));
	for (i = 0; i < search->of_tags; i++) {
		search_tag_t *t = &search->tags[i];
		if (intersect(&result, t->tag, t->weak)) {
			close_error(conn, E_MEM);
			return;
		}
		if (!result.of_posts) goto done;
	}
	for (i = 0; i < search->of_excluded_tags; i++) {
		search_tag_t *t = &search->excluded_tags[i];
		if (remove_tag(&result, t->tag, t->weak)) {
			close_error(conn, E_MEM);
			return;
		}
		if (!result.of_posts) goto done;
	}
	if (result.of_posts) {
		qsort_r(result.posts, result.of_posts, sizeof(post_t *), search, sorter);
		for (i = 0; i < result.of_posts; i++) {
			return_post(conn, result.posts[i], search->flags);
		}
	}
done:
	c_printf(conn, "OK\n");
	result_free(&result);
}

static void tag_search(connection_t *conn, const char *spec) {
	tag_t *tag = NULL;
	if (*spec == 'G') {
		guid_t guid;
		if (guid_str2guid(&guid, spec + 1, GUIDTYPE_TAG)) {
			conn->error(conn, spec);
			return;
		}
		tag = tag_find_guid(guid);
	} else if (*spec == 'N') {
		tag = tag_find_name(spec + 1);
	}
	if (tag) {
		c_printf(conn, "RG%s ", guid_guid2str(tag->guid));
		c_printf(conn, "N%s ", tag->name);
		c_printf(conn, "T%s ", tagtype_names[tag->type]);
		c_printf(conn, "P%u\n", tag->of_posts);
	}
	c_printf(conn, "OK\n");
}

static void modifying_command(connection_t *conn,
                              int (*func)(connection_t *, char *), char *cmd) {
	int ok;

	log_trans_start(conn);
	ok = !func(conn, cmd);
	log_trans_end(conn);
	if (ok) c_printf(conn, "OK\n");
}

void client_handle(connection_t *conn) {
	char *buf = conn->linebuf;
	switch (*buf) {
		case 'S': // 'S'earch
			if (buf[1] == 'P') {
				search_t search;
				int r = build_search(conn, buf + 2, &search);
				if (!r) do_search(conn, &search);
			} else if (buf[1] == 'T') {
				tag_search(conn, buf + 2);
			} else {
				close_error(conn, E_COMMAND);
			}
			break;
		case 'T': // 'T'ag post
			modifying_command(conn, prot_tag_post, buf + 1);
			break;
		case 'A': // 'A'dd something
			modifying_command(conn, prot_add, buf + 1);
			break;
		case 'M': // 'M'odify something
			modifying_command(conn, prot_modify, buf + 1);
			break;
		case 'N': // 'N'OP
			c_printf(conn, "OK\n");
			break;
		case 'Q': // 'Q'uit
			c_printf(conn, "Q bye bye\n");
			conn->flags &= ~CONNFLAG_GOING;
			break;
		case 'a': // 'a'uthenticate
			do {
				user_t *user = prot_auth(buf + 1);
				if (user) {
					c_printf(conn, "OK\n");
					conn->user = user;
				} else {
					c_printf(conn, "E\n");
				}
			} while (0);
			break;
		default:
			close_error(conn, E_COMMAND);
			break;
	}
	c_flush(conn);
}
