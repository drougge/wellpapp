#include "db.h"

#include <stdarg.h>

#define PROT_MAXLEN            4096
#define PROT_TAGS_PER_SEARCH   16
#define PROT_ORDERS_PER_SEARCH 4

static int s;

typedef enum {
	E_LINETOOLONG,
	E_READ,
	E_COMMAND,
	E_SYNTAX,
	E_OVERFLOW,
	E_MEM,
} error_t;
static char *errors[] = {
	"line too long",
	"read",
	"unknown command",
	"syntax error",
	"overflow",
	"out of memory",
};

typedef enum {
	ORDER_NONE,
	ORDER_DATE,
	ORDER_SCORE,
} order_t;

static const char *orders[] = {"date", "score", NULL};

typedef struct search {
	tag_t   *tags[PROT_TAGS_PER_SEARCH];
	tag_t   *excluded_tags[PROT_TAGS_PER_SEARCH];
	int     of_tags;
	int     of_excluded_tags;
	order_t orders[PROT_ORDERS_PER_SEARCH];
	int     of_orders;
} search_t;

typedef struct result {
	post_t **posts;
	uint32_t of_posts;
	uint32_t room;
} result_t;

static int str2id(const char *str, const char **ids) {
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

static void c_printf(const char *fmt, ...) {
	va_list ap;
	char    buf[PROT_MAXLEN];
	int     len;

	va_start(ap, fmt);
	len = vsnprintf(buf, sizeof(buf), fmt, ap);
	va_end(ap);
	assert(len < sizeof(buf));
	write(s, buf, len);
}

static int error(const char *what) {
	c_printf("RE %s\n", what);
	return 1;
}

static void close_error(error_t e) {
	c_printf("E%d %s\n", e, errors[e]);
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
	const tag_t *t1 = *(const tag_t **)_t1;
	const tag_t *t2 = *(const tag_t **)_t2;
	if (t1->of_posts < t2->of_posts) return -1;
	if (t1->of_posts > t2->of_posts) return 1;
	return 0;
}

static int build_search(char *cmd, search_t *search) {
	tag_t *tag;
	memset(search, 0, sizeof(*search));
	while(*cmd) {
		int len = 0;
		char *args = cmd + 1;

		while (cmd[len] && cmd[len] != ' ') len++;
		if (cmd[len]) {
			cmd[len] = 0;
			len++;
		}
		switch(*cmd) {
			case 'T': // Tag
			case 't': // Removed tag
				tag = find_tag(args);
				if (!tag) return error(cmd);
				if (*cmd == 'T') {
					if (search->of_tags == PROT_TAGS_PER_SEARCH) close_error(E_OVERFLOW);
					search->tags[search->of_tags] = tag;
					search->of_tags++;
				} else {
					if (search->of_excluded_tags == PROT_TAGS_PER_SEARCH) close_error(E_OVERFLOW);
					search->excluded_tags[search->of_excluded_tags] = tag;
					search->of_excluded_tags++;
				}
				break;
			case 'O': // Ordering
				if (search->of_orders == PROT_ORDERS_PER_SEARCH) close_error(E_OVERFLOW);
				search->orders[search->of_orders] = str2id(args, orders);
				if (!search->orders[search->of_orders]) return error(cmd);
				search->of_orders++;
				break;
			default:
				close_error(E_SYNTAX);
				break; /* NOTREACHED */
		}
		cmd += len;
	}
	if (!search->of_tags) return error("E Specify at least one included tag");
	/* Searching is faster if ordered by post-count */
	qsort(search->tags, search->of_tags, sizeof(tag_t *), sort_search);
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

static result_t remove_tag(result_t old_result, tag_t *tag) {
	result_t new_result;
	uint32_t i;

	memset(&new_result, 0, sizeof(new_result));
	for (i = 0; i < old_result.of_posts; i++) {
		post_t *post = old_result.posts[i];
		if (!post_has_tag(post, tag)) {
			add_post_to_result(post, &new_result);
		}
	}
	return new_result;
}

static result_t intersect(result_t old_result, tag_t *tag) {
	result_t new_result;
	memset(&new_result, 0, sizeof(new_result));
	if (old_result.of_posts) {
		uint32_t i;
		for (i = 0; i < old_result.of_posts; i++) {
			post_t *post = old_result.posts[i];
			if (post_has_tag(post, tag)) {
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
	const post_t *p1 = *(const post_t **)_p1;
	const post_t *p2 = *(const post_t **)_p2;
	search_t *search = _search;
	int i;

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

static void do_search(search_t *search) {
	result_t result;
	int i;

	memset(&result, 0, sizeof(result));
	for (i = 0; i < search->of_tags; i++) {
		result = intersect(result, search->tags[i]);
		if (!result.of_posts) goto done;
	}
	for (i = 0; i < search->of_excluded_tags; i++) {
		result = remove_tag(result, search->excluded_tags[i]);
		if (!result.of_posts) goto done;
	}
	if (result.of_posts) {
		qsort_r(result.posts, result.of_posts, sizeof(post_t *), search, sorter);
		for (i = 0; i < result.of_posts; i++) {
			c_printf("RP%s\n", md5_md52str(result.posts[i]->md5));
		}
	}
done:
	c_printf("RO\n");
}

void client_handle(int _s) {
	char buf[PROT_MAXLEN];
	int len;

	s = _s;
	while (42) {
		len = get_line(buf, sizeof(buf));
		if (*buf == 'S') {
			search_t search;
			int r = build_search(buf + 1, &search);
			if (!r) do_search(&search);
		} else if (*buf == 'N') {
			c_printf("RO\n");
		} else if (*buf == 'Q') {
			c_printf("Q bye bye\n");
			close(s);
			exit(0);
		} else {
			close_error(E_COMMAND);
		}
	}
}
