#include "db.h"

#define PROT_TAGS_PER_SEARCH   16
#define PROT_ORDERS_PER_SEARCH 4

typedef enum {
	ORDER_NONE,
	ORDER_DATE,
	ORDER_SCORE,
} order_t;

static const char *orders[] = {"date", "score", NULL};

static const char *flagnames[] = {"tagname", "tagguid", "ext", "created",
                                  "width", "height", "score", NULL};

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

static int sort_search(const void *_t1, const void *_t2) {
	const search_tag_t *t1 = (const search_tag_t *)_t1;
	const search_tag_t *t2 = (const search_tag_t *)_t2;
	if (t1->tag->posts.count < t2->tag->posts.count) return -1;
	if (t1->tag->posts.count > t2->tag->posts.count) return 1;
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
					return c_close_error(conn, E_OVERFLOW);
				}
				search->tags[search->of_tags].tag  = tag;
				search->tags[search->of_tags].weak = weak;
				search->of_tags++;
			} else {
				if (search->of_excluded_tags == PROT_TAGS_PER_SEARCH) {
					return c_close_error(conn, E_OVERFLOW);
				}
				search->excluded_tags[search->of_excluded_tags].tag  = tag;
				search->excluded_tags[search->of_excluded_tags].weak = weak;
				search->of_excluded_tags++;
			}
			break;
		case 'O': // Ordering
			if (search->of_orders == PROT_ORDERS_PER_SEARCH) {
				return c_close_error(conn, E_OVERFLOW);
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
			return c_close_error(conn, E_SYNTAX);
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
		post_taglist_t *tl = &post->tags;
		const char     *prefix = "";
again:
		while (tl) {
			for (i = 0; i < POST_TAGLIST_PER_NODE; i++) {
				if (tl->tags[i]) {
					if (flags & FLAG(FLAG_RETURN_TAGNAMES)) {
						c_printf(conn, " T%s%s", prefix, tl->tags[i]->name);
					}
					if (flags & FLAG(FLAG_RETURN_TAGIDS)) {
						c_printf(conn, " G%s%s", prefix, guid_guid2str(tl->tags[i]->guid));
					}
				}
			}
			tl = tl->next;
		}
		if (!*prefix) {
			prefix = "~";
			tl = post->weak_tags;
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

	memset(&result, 0, sizeof(result));
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
	for (i = 0; i < search->of_tags; i++) {
		search_tag_t *t = &search->tags[i];
		if (result_intersect(conn, &result, t->tag, t->weak)) {
			c_close_error(conn, E_MEM);
			return;
		}
		if (!result.of_posts) goto done;
	}
	for (i = 0; i < search->of_excluded_tags; i++) {
		search_tag_t *t = &search->excluded_tags[i];
		if (result_remove_tag(conn, &result, t->tag, t->weak)) {
			c_close_error(conn, E_MEM);
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
	result_free(conn, &result);
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
		c_printf(conn, "P%u\n", tag->posts.count);
		c_printf(conn, "W%u\n", tag->weak_posts.count);
	}
	c_printf(conn, "OK\n");
}

static void modifying_command(connection_t *conn,
                              int (*func)(connection_t *, char *), char *cmd) {
	int ok;

	log_trans_start(conn, time(NULL));
	ok = !func(conn, cmd);
	log_trans_end(conn);
	if (ok) c_printf(conn, "OK\n");
}

extern int connection_count;

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
				c_close_error(conn, E_COMMAND);
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
		case 'R': // Add 'R'elationship
			modifying_command(conn, prot_rel_add, buf + 1);
			break;
		case 'r': // Remove 'r'elationship
			modifying_command(conn, prot_rel_remove, buf + 1);
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
		case ' ': // Special commands. Hopefully tmp.
			if (connection_count > 1) {
				c_printf(conn, "E other connections\n");
				break;
			}
			if (!strcmp(buf, " dump")) {
				c_printf(conn, "...\n");
				c_flush(conn);
				log_dump();
				c_printf(conn, "OK\n");
			} else if (!strcmp(buf, " quit")) {
				server_running = 0;
				c_printf(conn, "poof!\n");
			}
			break;
		default:
			c_close_error(conn, E_COMMAND);
			break;
	}
	c_flush(conn);
}
