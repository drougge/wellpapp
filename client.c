#include "db.h"

#include <stdarg.h>

#define PROT_MAXLEN            4096
#define PROT_TAGS_PER_SEARCH   16
#define PROT_ORDERS_PER_SEARCH 4

static int s;
static user_t *current_user;

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

static void c_printf(const char *fmt, ...);
static void FLAGPRINT_EXTENSION(post_t *post) {
	c_printf("%s", filetype_names[post->filetype]);
}
static void FLAGPRINT_DATE(post_t *post) {
	c_printf("%llu", (unsigned long long)post->created);
}
static void FLAGPRINT_WIDTH(post_t *post) {
	c_printf("%u", post->width);
}
static void FLAGPRINT_HEIGHT(post_t *post) {
	c_printf("%u", post->height);
}
static void FLAGPRINT_SCORE(post_t *post) {
	c_printf("%d", post->score);
}

typedef void (*flag_printer_t)(post_t *);
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

static unsigned int c_buf_used = 0;
static char c_buf[PROT_MAXLEN];
#define BUF_MIN_FREE 1024

static void c_flush(void) {
	if (c_buf_used) {
		ssize_t w = write(s, c_buf, c_buf_used);
		assert(w > 0 && (unsigned int)w == c_buf_used);
		c_buf_used = 0;
	}
}

static void c_printf(const char *fmt, ...) {
	va_list ap;
	int     len;

	va_start(ap, fmt);
	len = vsnprintf(c_buf + c_buf_used, sizeof(c_buf) - c_buf_used, fmt, ap);
	if (len >= (int)(sizeof(c_buf) - c_buf_used)) { // Overflow
		c_flush();
		len = vsnprintf(c_buf, sizeof(c_buf), fmt, ap);
		assert(len < (int)sizeof(c_buf));
	}
	va_end(ap);
	c_buf_used += len;
	if (c_buf_used + BUF_MIN_FREE > sizeof(c_buf)) c_flush();
}

static int client_error(const char *what) {
	c_printf("RE %s\n", what);
	return 1;
}

static void close_error(error_t e) {
	c_printf("E%d %s\n", e, errors[e]);
	c_flush();
	close(s);
	exit(1);
}

static int get_line(char *buf, int size) {
	static char getbuf[256];
	static int getlen = 0;
	static int getpos = 0;
	int len = 0;

	while (size > len) {
		if (getlen > getpos) {
			char c = getbuf[getpos];
			getpos++;
			/* \r is ignored, for easier testing with telnet */
			if (c == '\n') {
				buf[len] = 0;
				return len;
			} else if (c != '\r') {
				buf[len] = c;
				len++;
			}
		} else {
			getpos = 0;
			getlen = read(s, getbuf, sizeof(getbuf));
			if (getlen <= 0) close_error(E_READ);
		}
	}
	close_error(E_LINETOOLONG);
	return -1; /* NOTREACHED */
}

static int sort_search(const void *_t1, const void *_t2) {
	const search_tag_t *t1 = (const search_tag_t *)_t1;
	const search_tag_t *t2 = (const search_tag_t *)_t2;
	if (t1->tag->of_posts < t2->tag->of_posts) return -1;
	if (t1->tag->of_posts > t2->tag->of_posts) return 1;
	return 0;
}

static int build_search_cmd(const user_t *user, const char *cmd, void *search_,
                            prot_cmd_flag_t flags, trans_t *trans,
                            prot_err_func_t error) {
	tag_t      *tag;
	truth_t    weak = T_DONTCARE;
	search_t   *search = search_;
	const char *args = cmd + 1;
	int        i;
	int        r;

	(void)user;
	(void)flags;
	(void)trans;

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
				return error(cmd);
			}
			if (!tag) return error(cmd);
			if (*cmd == 'T') {
				if (search->of_tags == PROT_TAGS_PER_SEARCH) close_error(E_OVERFLOW);
				search->tags[search->of_tags].tag  = tag;
				search->tags[search->of_tags].weak = weak;
				search->of_tags++;
			} else {
				if (search->of_excluded_tags == PROT_TAGS_PER_SEARCH) close_error(E_OVERFLOW);
				search->excluded_tags[search->of_excluded_tags].tag  = tag;
				search->excluded_tags[search->of_excluded_tags].weak = weak;
				search->of_excluded_tags++;
			}
			break;
		case 'O': // Ordering
			if (search->of_orders == PROT_ORDERS_PER_SEARCH) close_error(E_OVERFLOW);
			search->orders[search->of_orders] = str2id(args, orders);
			if (!search->orders[search->of_orders]) return error(cmd);
			search->of_orders++;
			break;
		case 'F': // Flag (option)
			i = str2id(args, flagnames);
			if (i < 1) return error(cmd);
			search->flags |= FLAG(i - 1);
			break;
		case 'M': // md5 (specific post)
			if (search->post) return error(cmd);
			r = post_find_md5str(&search->post, args);
			if (r < 0) return error(cmd);
			if (r > 0) { /* Not found */
				search->post = &null_post;
			}
			break;
		default:
			close_error(E_SYNTAX);
			return 1; /* NOTREACHED */
			break;
	}
	return 0;
}

static int build_search(char *cmd, search_t *search) {
	memset(search, 0, sizeof(*search));
	if (prot_cmd_loop(current_user, cmd, search, build_search_cmd, CMDFLAG_NONE, NULL, client_error)) return 1;
	if (!search->of_tags && !search->post) {
		return client_error("E Specify at least one included tag");
	}
	/* Searching is faster if ordered by post-count */
	qsort(search->tags, search->of_tags, sizeof(search_tag_t), sort_search);
	return 0;
}

static void add_post_to_result(post_t *post, result_t *result) {
	if (result->room == result->of_posts) {
		result->room += 50;
		post_t **p = realloc(result->posts, result->room * sizeof(post_t *));
		if (!p) close_error(E_MEM);
		result->posts = p;
	}
	result->posts[result->of_posts] = post;
	result->of_posts++;
}

static result_t remove_tag(result_t old_result, tag_t *tag, truth_t weak) {
	result_t new_result;
	uint32_t i;

	memset(&new_result, 0, sizeof(new_result));
	for (i = 0; i < old_result.of_posts; i++) {
		post_t *post = old_result.posts[i];
		if (!post_has_tag(post, tag, weak)) {
			add_post_to_result(post, &new_result);
		}
	}
	return new_result;
}

static result_t intersect(result_t old_result, tag_t *tag, truth_t weak) {
	result_t new_result;
	memset(&new_result, 0, sizeof(new_result));
	if (old_result.of_posts) {
		uint32_t i;
		for (i = 0; i < old_result.of_posts; i++) {
			post_t *post = old_result.posts[i];
			if (post_has_tag(post, tag, weak)) {
				add_post_to_result(post, &new_result);
			}
		}
	} else {
		tag_postlist_t *pl;
		pl = &tag->posts;
		while (pl) {
			uint32_t i;
			for (i = 0; i < TAG_POSTLIST_PER_NODE; i++) {
				if (pl->posts[i]) {
					add_post_to_result(pl->posts[i], &new_result);
				}
			}
			pl = pl->next;
		}
	}
	return new_result;
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

static void return_post(post_t *post, int flags) {
	int i;
	c_printf("RP%s", md5_md52str(post->md5));
	if (flags & (FLAG(FLAG_RETURN_TAGNAMES) | FLAG(FLAG_RETURN_TAGIDS))) {
		post_taglist_t *tags = &post->tags;
		const char     *prefix = "";
again:
		while (tags) {
			for (i = 0; i < POST_TAGLIST_PER_NODE; i++) {
				if (tags->tags[i]) {
					if (flags & FLAG(FLAG_RETURN_TAGNAMES)) {
						c_printf(" T%s%s", prefix, tags->tags[i]->name);
					}
					if (flags & FLAG(FLAG_RETURN_TAGIDS)) {
						c_printf(" G%s%s", prefix, guid_guid2str(tags->tags[i]->guid));
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
			c_printf(" F%s=", flagnames[i]);
			flag_printers[i](post);
		}
	}
	c_printf("\n");
}

static void do_search(search_t *search) {
	result_t result;
	unsigned int i;

	if (search->post) {
		if (search->of_tags || search->of_excluded_tags) {
			client_error("E mutually exclusive options specified");
			return;
		}
		if (search->post != &null_post) {
			return_post(search->post, search->flags);
		}
		goto done;
	}
	memset(&result, 0, sizeof(result));
	for (i = 0; i < search->of_tags; i++) {
		search_tag_t *t = &search->tags[i];
		result = intersect(result, t->tag, t->weak);
		if (!result.of_posts) goto done;
	}
	for (i = 0; i < search->of_excluded_tags; i++) {
		search_tag_t *t = &search->excluded_tags[i];
		result = remove_tag(result, t->tag, t->weak);
		if (!result.of_posts) goto done;
	}
	if (result.of_posts) {
		qsort_r(result.posts, result.of_posts, sizeof(post_t *), search, sorter);
		for (i = 0; i < result.of_posts; i++) {
			return_post(result.posts[i], search->flags);
		}
	}
done:
	c_printf("OK\n");
}

static void tag_search(const char *spec) {
	tag_t *tag = NULL;
	if (*spec == 'G') {
		guid_t guid;
		if (guid_str2guid(&guid, spec + 1, GUIDTYPE_TAG)) {
			client_error(spec);
			return;
		}
		tag = tag_find_guid(guid);
	} else if (*spec == 'N') {
		tag = tag_find_name(spec + 1);
	}
	if (tag) {
		c_printf("RG%s ", guid_guid2str(tag->guid));
		c_printf("N%s ", tag->name);
		c_printf("T%s ", tagtype_names[tag->type]);
		c_printf("P%u\n", tag->of_posts);
	}
	c_printf("OK\n");
}

static void modifying_command(int (*func)(const user_t *, char *, trans_t *, prot_err_func_t), char *cmd) {
	trans_t trans;
	int ok;

	log_trans_start(&trans, current_user);
	ok = !func(current_user, cmd, &trans, client_error);
	log_trans_end(&trans);
	if (ok) c_printf("OK\n");
}

void client_handle(int _s) {
	char   buf[PROT_MAXLEN];
	int    len;
	user_t anonymous;

	s = _s;
	anonymous.name = "A";
	anonymous.caps = DEFAULT_CAPS;
	current_user = &anonymous;

	while (42) {
		c_flush();
		len = get_line(buf, sizeof(buf));
		switch (*buf) {
			case 'S': // 'S'earch
				if (buf[1] == 'P') {
					search_t search;
					int r = build_search(buf + 2, &search);
					if (!r) do_search(&search);
				} else if (buf[1] == 'T') {
					tag_search(buf + 2);
				} else {
					close_error(E_COMMAND);
				}
				break;
			case 'T': // 'T'ag post
				modifying_command(prot_tag_post, buf + 1);
				break;
			case 'A': // 'A'dd something
				modifying_command(prot_add, buf + 1);
				break;
			case 'M': // 'M'odify something
				modifying_command(prot_modify, buf + 1);
				break;
			case 'N': // 'N'OP
				c_printf("OK\n");
				break;
			case 'Q': // 'Q'uit
				c_printf("Q bye bye\n");
				c_flush();
				close(s);
				exit(0);
				break;
			case 'a': // 'a'uthenticate
				current_user = prot_auth(buf + 1);
				if (current_user) {
					c_printf("OK\n");
				} else {
					c_printf("E\n");
					current_user = &anonymous;
				}
				break;
			default:
				close_error(E_COMMAND);
				break;
		}
	}
}
