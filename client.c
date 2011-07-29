#include "db.h"

#define PROT_TAGS_PER_SEARCH   16
#define PROT_ORDERS_PER_SEARCH 4

typedef enum {
	ORDER_NONE,
	ORDER_CREATED,
	ORDER_IMAGEDATE,
	ORDER_SCORE,
	ORDER_GROUP,
	ORDER_MODIFIED,
	ORDER_WIDTH,
	ORDER_HEIGHT,
	ORDER_AREA,
	ORDER_TAGCOUNT,
} order_t;

typedef enum {
	TAG_ORDER_NONE,
	TAG_ORDER_POST,
	TAG_ORDER_WEAK,
	TAG_ORDER_ALLPOST,
} tag_order_t;

static const char *flagnames[] = {"tagname", "tagguid", "implied", "tagdata",
                                  "ext", "created", "width", "height", "score",
                                  "source", "title", "imgdate", "modified",
                                  "rotate", NULL};

static void FLAGPRINT_EXTENSION(connection_t *conn, post_t *post) {
	c_printf(conn, " Fext=%s", filetype_names[post->filetype]);
}
static void FLAGPRINT_CREATED(connection_t *conn, post_t *post) {
	c_printf(conn, " Fcreated=%llx", (unsigned long long)post->created);
}
static void FLAGPRINT_IMGDATE(connection_t *conn, post_t *post) {
	c_printf(conn, " Fimgdate=%llx", (unsigned long long)post->imgdate);
}
static void FLAGPRINT_MODIFIED(connection_t *conn, post_t *post) {
	c_printf(conn, " Fmodified=%llx", (unsigned long long)post->modified);
}
static void FLAGPRINT_WIDTH(connection_t *conn, post_t *post) {
	c_printf(conn, " Fwidth=%x", post->width);
}
static void FLAGPRINT_HEIGHT(connection_t *conn, post_t *post) {
	c_printf(conn, " Fheight=%x", post->height);
}
static void FLAGPRINT_SCORE(connection_t *conn, post_t *post) {
	c_printf(conn, " Fscore=%d", post->score);
}
static void FLAGPRINT_ROTATE(connection_t *conn, post_t *post) {
	c_printf(conn, " Frotate=%d", post->rotate);
}
static void FLAGPRINT_SOURCE(connection_t *conn, post_t *post) {
	if (post->source) {
		c_printf(conn, " Fsource=%s", str_str2enc(post->source));
	}
}
static void FLAGPRINT_TITLE(connection_t *conn, post_t *post) {
	if (post->title) {
		c_printf(conn, " Ftitle=%s", str_str2enc(post->title));
	}
}

typedef void (*flag_printer_t)(connection_t *, post_t *);
static const flag_printer_t flag_printers[] = {
	NULL,
	NULL,
	NULL,
	NULL,
	FLAGPRINT_EXTENSION,
	FLAGPRINT_CREATED,
	FLAGPRINT_WIDTH,
	FLAGPRINT_HEIGHT,
	FLAGPRINT_SCORE,
	FLAGPRINT_SOURCE,
	FLAGPRINT_TITLE,
	FLAGPRINT_IMGDATE,
	FLAGPRINT_MODIFIED,
	FLAGPRINT_ROTATE,
};

typedef enum {
	FLAG_RETURN_TAGNAMES,
	FLAG_RETURN_TAGIDS,
	FLAG_RETURN_IMPLIED,
	FLAG_RETURN_TAGDATA,
	FLAG_RETURN_EXTENSION,
	FLAG_RETURN_CREATED,
	FLAG_RETURN_WIDTH,
	FLAG_RETURN_HEIGHT,
	FLAG_RETURN_SCORE,
	FLAG_RETURN_SOURCE,
	FLAG_RETURN_TITLE,
	FLAG_RETURN_IMGDATE,
	FLAG_RETURN_MODIFIED,
	FLAG_RETURN_ROTATE,
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
	long         range_start;
	long         range_end;
	unsigned int range_used : 1;
	unsigned int failed : 1;
} search_t;
static post_t null_post; /* search->post for not found posts */


#define SORTER(field) \
	static int sorter_ ## field(const post_t *p1, const post_t *p2) \
	{ \
		if (p1->field < p2->field) return -1; \
		if (p1->field > p2->field) return 1; \
		return 0; \
	}
SORTER(created)
SORTER(imgdate)
SORTER(score)
SORTER(modified)
SORTER(width)
SORTER(height)
SORTER(of_tags)
static int sorter_area(const post_t *p1, const post_t *p2)
{
	unsigned int p1a = (unsigned int)p1->width * p1->height;
	unsigned int p2a = (unsigned int)p2->width * p2->height;
	if (p1a < p2a) return -1;
	if (p1a > p2a) return 1;
	return 0;
}
static int sorter_group(const post_t *p1, const post_t *p2)
{
	(void) p1;
	(void) p2;
	return 0; // This just keeps the ordering from the tag
}

typedef int (*sorter_f)(const post_t *p1, const post_t *p2);
static sorter_f sorters[] = {sorter_created, sorter_imgdate, sorter_score,
                             sorter_group, sorter_modified, sorter_width,
                             sorter_height, sorter_area, sorter_of_tags};
static const char *orders[] = {"created", "imagedate", "score", "group",
                               "modified", "width", "height", "area",
                               "tagcount", NULL};
static const char *tag_orders[] = {"post", "weak", "allpost", NULL};

typedef struct taglimit_tag {
	const tag_t *tag;
	uint32_t    count[2];
} taglimit_tag_t;

typedef struct taglimit {
	ss128_head_t tree;
} taglimit_t;

static uint32_t tag_count(const tag_t *tag, truth_t weak, taglimit_t *limit)
{
	uint32_t count, weak_count;
	if (limit) {
		ss128_value_t res;
		if (ss128_find(&limit->tree, &res, tag->guid.key)) {
			count = weak_count = 0;
		} else {
			taglimit_tag_t *tt = (taglimit_tag_t *)res;
			count = tt->count[0];
			weak_count = tt->count[1];
		}
	} else {
		count = tag->posts.count;
		weak_count = tag->weak_posts.count;
	}
	switch (weak) {
		case T_NO:
			return count;
			break;
		case T_YES:
			return weak_count;
			break;
		case T_DONTCARE:
		default:
			return count + weak_count;
			break;
	}
}

static int taglimit_add_tag(connection_t *conn, taglimit_t *limit,
                            const tag_t *tag, int weak)
{
	if (!tag) return 0;
	ss128_key_t key = tag->guid.key;
	ss128_value_t res;
	if (ss128_find(&limit->tree, &res, key)) {
		taglimit_tag_t *tt;
		void *mem;
		if (c_alloc(conn, &mem, sizeof(*tt))) return 1;
		res = mem;
		tt  = mem;
		if (ss128_insert(&limit->tree, res, key)) {
			c_free(conn, tt, sizeof(*tt));
			return 1;
		}
		tt->tag = tag;
		tt->count[0] = tt->count[1] = 0;
	}
	taglimit_tag_t *tt = (taglimit_tag_t *)res;
	tt->count[weak]++;
	return 0;
}

static int taglimit_add(connection_t *conn, taglimit_t *limit,
                        const post_t *post)
{
	const post_taglist_t *tl = &post->tags;
	int weak = 0;
again:
	while (tl) {
		for (int i = 0; i < arraylen(tl->tags); i++) {
			err1(taglimit_add_tag(conn, limit, tl->tags[i], weak));
		}
		tl = tl->next;
	}
	if (!weak) {
		weak = 1;
		tl = post->weak_tags;
		goto again;
	}
	return 0;
err:
	return 1;
}

static int ss128_c_alloc(void *data, void *res, unsigned int z)
{
	connection_t *conn = data;
	return c_alloc(conn, res, z);
}

static void ss128_c_free(void *data, void *ptr, unsigned int z)
{
	connection_t *conn = data;
	c_free(conn, ptr, z);
}

static int search2taglimit(connection_t *conn, search_t *search,
                           result_t *result, taglimit_t *limit)
{
	memset(limit, 0, sizeof(*limit));
	ss128_init(&limit->tree, ss128_c_alloc, ss128_c_free, conn);
	for (long i = search->range_start; i < search->range_end; i++) {
		err1(taglimit_add(conn, limit, result->posts[i]));
	}
	return 0;
err:
	return 1;
}

static void taglimit_free_node(ss128_key_t key, ss128_value_t value, void *data_)
{
	(void)key;
	taglimit_tag_t *tt = (taglimit_tag_t *)value;
	connection_t   *conn = data_;
	c_free(conn, tt, sizeof(*tt));
}

static void taglimit_free(connection_t *conn, taglimit_t *limit)
{
	ss128_iterate(&limit->tree, taglimit_free_node, conn);
	ss128_free(&limit->tree);
}

static int sort_search(const void *_t1, const void *_t2, void *_data)
{
	const search_tag_t *t1 = (const search_tag_t *)_t1;
	const search_tag_t *t2 = (const search_tag_t *)_t2;
	uint32_t c1 = tag_count(t1->tag, t1->weak, NULL);
	uint32_t c2 = tag_count(t2->tag, t2->weak, NULL);
	(void) _data;
	if (c1 < c2) return -1;
	if (c1 > c2) return 1;
	return 0;
}

typedef struct {
	connection_t *conn;
	const char   *text;
	guid_t       guid;
	int          tlen;
	int          fuzzy;
	const tag_t  **tag;
	taglimit_t   *limits;
	long         range_start;
	long         range_end;
	long         tag_pos;
	long         tag_len;
	int          order;
	truth_t      aliases;
	dberror_t    error;
	char         type;
	int          filtered;
	search_t     search;
} tag_search_data_t;

static int sort_tag(const void *_t1, const void *_t2, void *_data)
{
	const tag_t *t1 = *(const tag_t * const *)_t1;
	const tag_t *t2 = *(const tag_t * const *)_t2;
	tag_search_data_t *data = _data;
	int order = data->order;
	truth_t weak;
	switch (abs(order)) {
		case TAG_ORDER_POST:
			weak = T_NO;
			break;
		case TAG_ORDER_WEAK:
			weak = T_YES;
			break;
		case TAG_ORDER_ALLPOST:
			weak = T_DONTCARE;
			break;
		default:
			return 0;
			break;
	}
	uint32_t c1 = tag_count(t1, weak, data->limits);
	uint32_t c2 = tag_count(t2, weak, data->limits);
	if (c1 < c2) return order < 0 ? 1 : -1;
	if (c1 > c2) return order < 0 ? -1 : 1;
	return 0;
}

static int parse_range(const char *args, long *r_start, long *r_end)
{
	if (*r_start != -1) return 1;
	char *end;
	*r_start = strtol(args, &end, 16);
	if (*end != ':') return 1;
	if (end[1]) {
		long rend = strtol(end + 1, &end, 16);
		if (*end || rend < *r_start) {
			 return 1;
		}
		*r_end = rend;
	}
	return 0;
}

static int build_search_cmd(connection_t *conn, const char *cmd, void *search_,
                            prot_cmd_flag_t flags)
{
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
				tag = tag_find_name(args + 1, T_DONTCARE, NULL);
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
		case 'R': // 'R'ange
			if (parse_range(args, &search->range_start,
			                &search->range_end)) {
				conn->error(conn, cmd);
			}
			break;
		default:
			return c_close_error(conn, E_SYNTAX);
			break;
	}
	return 0;
}

static void init_search(search_t *search)
{
	memset(search, 0, sizeof(*search));
	search->range_start = -1;
	search->range_end = LONG_MAX - 1;
}

static int setup_search(search_t *search)
{
	if (!search->of_tags && !search->post) {
		return 0;
	}
	/* Searching is faster if ordered by post-count,  *
	 * but group ordering is implicit in match order. */
	int can_sort = 1;
	for (unsigned int i = 0; i < search->of_orders; i++) {
		if (search->orders[i] == ORDER_GROUP) can_sort = 0;
	}
	if (can_sort) sort(search->tags, search->of_tags, sizeof(search_tag_t),
	                   sort_search, NULL);
	return 0;
}

static int sorter(const void *_p1, const void *_p2, void *_search)
{
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

static void c_print_tag(connection_t *conn, const tag_t *tag, int flags,
                        int aliases, taglimit_t *limits);
static void return_post(connection_t *conn, post_t *post, int flags)
{
	int i;
	c_printf(conn, "RP%s", md5_md52str(post->md5));
	for (i = FLAG_FIRST_SINGLE; i < FLAG_LAST; i++) {
		if (flags & FLAG(i)) {
			flag_printers[i](conn, post);
		}
	}
	if (flags & (FLAG(FLAG_RETURN_TAGNAMES) | FLAG(FLAG_RETURN_TAGIDS))) {
		post_taglist_t *tl = &post->tags;
		post_taglist_t *impltl = post->implied_tags;
		const char     *prefix = "";
		c_printf(conn, " ");
again:
		while (tl) {
			for (i = 0; i < arraylen(tl->tags); i++) {
				if (tl->tags[i]) {
					const tag_t *tag = tl->tags[i];
					c_printf(conn, ":");
					if (flags & FLAG(FLAG_RETURN_IMPLIED)
					    && taglist_contains(impltl, tag)
					   ) {
						c_printf(conn, "I");
					}
					c_printf(conn, "%s ", prefix);
					c_print_tag(conn, tag, flags, 0, NULL);
				}
			}
			tl = tl->next;
		}
		if (!*prefix) {
			prefix = "~";
			tl = post->weak_tags;
			impltl = post->implied_weak_tags;
			goto again;
		}
		c_printf(conn, ":");
	}
	c_printf(conn, "\n");
}

typedef struct post2result_data {
	connection_t *conn;
	result_t     *result;
	unsigned int error : 1;
} post2result_data_t;

static void post2result(ss128_key_t key, ss128_value_t value, void *data_)
{
	(void) key;
	post_t *post = (post_t *)value;
	post2result_data_t *data = data_;
	if (data->error) return;
	data->error |= result_add_post(data->conn, data->result, post);
}

static void do_search(connection_t *conn, search_t *search, result_t *result)
{
	memset(result, 0, sizeof(*result));
	if (search->post) {
		if (search->of_tags || search->of_excluded_tags) {
			conn->error(conn, "E mutually exclusive options specified");
			goto err;
		}
		if (search->post != &null_post) {
			err1(result_add_post(conn, result, search->post));
		}
		goto done;
	}
	for (unsigned int i = 0; i < search->of_tags; i++) {
		search_tag_t *t = &search->tags[i];
		if (result_intersect(conn, result, t->tag, t->weak)) {
			c_close_error(conn, E_MEM);
			goto err;
		}
		if (!result->of_posts) goto done;
	}
	for (unsigned int i = 0; i < search->of_excluded_tags; i++) {
		search_tag_t *t = &search->excluded_tags[i];
		if (result_remove_tag(conn, result, t->tag, t->weak)) {
			c_close_error(conn, E_MEM);
			goto err;
		}
		if (!result->of_posts) goto done;
	}
	if (!result->of_posts) { // No criteria -> return all posts
		post2result_data_t data;
		data.conn   = conn;
		data.result = result;
		data.error  = 0;
		ss128_iterate(posts, post2result, &data);
		if (data.error) {
			c_close_error(conn, E_MEM);
			goto err;
		}
	}
done:
	if (result->of_posts > 1) {
		sort(result->posts, result->of_posts, sizeof(post_t *),
		     sorter, search);
	}
	if (search->range_start == -1) {
		search->range_start = 0;
		search->range_end = result->of_posts;
	} else {
		search->range_used = 1;
		search->range_end++;
		if (search->range_end > (long)result->of_posts) {
			search->range_end = result->of_posts;
		}
	}
	return;
err:
	search->failed = 1;
}

static void print_search(connection_t *conn, search_t *search, result_t *result)
{
	if (search->range_used) c_printf(conn, "RR%x\n", result->of_posts);
	if (result->of_posts && !search->failed) {
		for (long i = search->range_start; i < search->range_end; i++) {
			return_post(conn, result->posts[i], search->flags);
		}
	}
	c_printf(conn, "OK\n");
	result_free(conn, result);
}

typedef struct c_print_alias {
	connection_t *conn;
	const tag_t  *tag;
} c_print_alias_t;

static void c_print_alias_cb(ss128_key_t key, ss128_value_t value, void *data_)
{
	c_print_alias_t *data = data_;
	tagalias_t *tagalias = (tagalias_t *)value;
	(void) key;
	if (tagalias->tag == data->tag) {
		c_printf(data->conn, "A%s ", tagalias->name);
	}
}

static void c_print_tag(connection_t *conn, const tag_t *tag, int flags,
                        int aliases, taglimit_t *limits)
{
	if (!tag) return;
	if (flags == ~0) c_printf(conn, "R");
	if (flags & FLAG(FLAG_RETURN_TAGIDS)) {
		c_printf(conn, "G%s ", guid_guid2str(tag->guid));
	}
	if (flags & FLAG(FLAG_RETURN_TAGNAMES)) {
		c_printf(conn, "N%s ", tag->name);
	}
	if (aliases) {
		c_print_alias_t data;
		data.conn = conn;
		data.tag  = tag;
		ss128_iterate(tagaliases, c_print_alias_cb, &data);
	}
	if (flags & FLAG(FLAG_RETURN_TAGDATA)) {
		int count, weak_count;
		if (limits) {
			ss128_value_t res;
			if (ss128_find(&limits->tree, &res, tag->guid.key)) {
				count = weak_count = 0;
			} else {
				taglimit_tag_t *tt = (taglimit_tag_t *)res;
				count = tt->count[0];
				weak_count = tt->count[1];
			}
		} else {
			count = tag->posts.count;
			weak_count = tag->weak_posts.count;
		}
		c_printf(conn, "T%s ", tagtype_names[tag->type]);
		c_printf(conn, "P%x ", count);
		c_printf(conn, "W%x",  weak_count);
		if (tag->ordered) c_printf(conn, " Fordered");
		if (tag->unsettable) c_printf(conn, " Funsettable");
		if (flags != ~0) c_printf(conn, " ");
	}
	if (flags == ~0) c_printf(conn, "\n");
}

static void tag_search_add_res(tag_search_data_t *data, const tag_t *tag,
                               int check_dup)
{
	if (data->error) return;
	if (data->limits) {
		ss128_value_t v;
		if (ss128_find(&data->limits->tree, &v, tag->guid.key)) return;
	}
	if (check_dup) {
		// This is terrible inefficient
		for (int i = 0; i < data->tag_pos; i++) {
			if (data->tag[i] == tag) return;
		}
	}
	if (data->tag_pos == data->tag_len) {
		int old_len = data->tag_len;
		int new_len = old_len ? old_len * 2 : 16;
		int r;
		data->tag = c_realloc(data->conn, data->tag,
		                      sizeof(*data->tag) * old_len,
		                      sizeof(*data->tag) * new_len, &r);
		if (r) {
			data->error = E_MEM;
			return;
		}
		data->tag_len = new_len;
	}
	data->tag[data->tag_pos++] = tag;
}

static void tag_search_i(const char *name, const tag_t *tag,
                         int check_dup, tag_search_data_t *data)
{
	if (data->error) return;
	if (data->type == 'P') {
		if (strstr(name, data->text)) {
			tag_search_add_res(data, tag, check_dup);
		}
	} else {
		int nlen = strlen(name);
		if ((nlen == data->tlen
		     || (nlen > data->tlen && data->type == 'I')
		    ) && !memcmp(name, data->text, data->tlen)
		   ) {
			tag_search_add_res(data, tag, check_dup);
		}
	}
}

static void tag_search_P(ss128_key_t key, ss128_value_t value, void *data_)
{
	tag_search_data_t *data = data_;
	const tag_t       *tag = (const tag_t *)value;
	const char *name = data->fuzzy ? tag->fuzzy_name : tag->name;
	(void)key;
	tag_search_i(name, tag, 0, data);
}

static void tag_search_P_alias(ss128_key_t key, ss128_value_t value,
                               void *data_)
{
	tag_search_data_t *data = data_;
	const tagalias_t  *tagalias = (const tagalias_t *)value;
	const char *name = data->fuzzy ? tagalias->fuzzy_name : tagalias->name;
	(void)key;
	tag_search_i(name, tagalias->tag, 1, data);
}

static int tag_search_cmd_last(connection_t *conn, tag_search_data_t *data)
{
	int r = 1;
	char c = data->type;
	int aliases = data->aliases;
	taglimit_t *limits = NULL;
	taglimit_t limits_store;
	if (data->filtered) {
		result_t result;
		search_t *search = &data->search;
		err1(setup_search(search));
		do_search(conn, search, &result);
		if (!search->failed) {
			limits = &limits_store;
			if (search2taglimit(conn, search, &result, limits)) {
				search->failed = 1;
			}
		}
		result_free(conn, &result);
		err1(search->failed);
	}
	if (c == 'G') {
		c_print_tag(conn, tag_find_guid(data->guid), ~0, aliases, limits);
	} else {
		if (c != 'N' && c != 'P' && c != 'I') goto err;
		if (c == 'N' && !data->fuzzy) {
			tag_t *tag = tag_find_name(data->text, aliases, NULL);
			c_print_tag(conn, tag, ~0, aliases, limits);
		} else {
			char         *fuzztext;
			unsigned int fuzzlen;
			if (data->fuzzy) {
				if (utf_fuzz_c(conn, data->text, &fuzztext,
				               &fuzzlen)) {
					return c_close_error(conn, E_MEM);
				}
				data->text = fuzztext;
			}
			data->tlen = strlen(data->text);
			data->limits = limits;
			ss128_iterate(tags, tag_search_P, data);
			if (aliases) {
				ss128_iterate(tagaliases, tag_search_P_alias,
				              data);
			}
			if (data->fuzzy) c_free(conn, fuzztext, fuzzlen);
			if (data->error) return c_close_error(conn, data->error);
			if (data->tag_pos) {
				if (data->order) {
					sort(data->tag, data->tag_pos,
					     sizeof(*data->tag), sort_tag, data);
				}
				unsigned int z = sizeof(*data->tag) * data->tag_len;
				long start = 0;
				long stop  = data->tag_pos;
				if (data->range_start != -1) {
					c_printf(conn, "RR%lx\n", data->tag_pos);
					start = data->range_start;
					stop  = data->range_end + 1;
					if (stop > data->tag_pos) {
						stop = data->tag_pos;
					}
					if (start >= stop) goto done;
				}
				for (long i = start; i < stop; i++) {
					c_print_tag(conn, data->tag[i], ~0, aliases, limits);
				}
done:
				c_free(conn, data->tag, z);
			}
		}
	}
	r = 0;
err:
	if (limits) taglimit_free(conn, limits);
	if (r) conn->error(conn, "");
	return r;
}

static int tag_search_cmd(connection_t *conn, const char *cmd, void *data_,
                         prot_cmd_flag_t flags)
{
	tag_search_data_t *data = data_;

	if (data->filtered) {
		err1(build_search_cmd(conn, cmd, &data->search, flags));
	} else if (data->type) {
		switch (*cmd) {
			case 'O': // 'O'rder
				data->order = str2id(cmd + 1, tag_orders);
				if (!data->order) goto err;
				break;
			case 'R': // 'R'ange
				if (parse_range(cmd + 1, &data->range_start,
				                &data->range_end)) goto err;
				break;
			case ':': // Filter
				data->filtered = 1;
				init_search(&data->search);
				err1(build_search_cmd(conn, cmd + 1,
				                      &data->search, flags));
				break;
			default:
				goto err;
				break;
		}
	} else {
		if (*cmd == 'F') {
			data->fuzzy = 1;
		} else if (*cmd != 'E') {
			goto err;
		}
		cmd++;
		if (*cmd == 'A') {
			data->aliases = T_DONTCARE;
			cmd++;
		}
		data->type = *cmd;
		if (data->type == 'G') {
			if (data->fuzzy) goto err;
			if (guid_str2guid(&data->guid, cmd + 1, GUIDTYPE_TAG)) goto err;
		} else {
			data->text = cmd + 1;
		}
	}

	if (flags & CMDFLAG_LAST) return tag_search_cmd_last(conn, data);
	return 0;
err:
	conn->error(conn, cmd);
	return 1;
}

static void tag_search(connection_t *conn, char *cmd)
{
	tag_search_data_t data;
	data.conn     = conn;
	data.text     = NULL;
	data.fuzzy    = 0;
	data.aliases  = T_NO;
	data.type     = 0;
	data.order    = TAG_ORDER_NONE;
	data.error    = 0;
	data.tag      = NULL;
	data.limits   = NULL;
	data.tag_pos  = 0;
	data.tag_len  = 0;
	data.range_start = -1;
	data.range_end   = LONG_MAX - 1;
	data.filtered = 0;
	if (!prot_cmd_loop(conn, cmd, &data, tag_search_cmd, 0)) {
		c_printf(conn, "OK\n");
	}
}

typedef struct show_rels_data {
	connection_t *conn;
	char         md5[33];
} show_rels_data_t;

static void show_rels_cb(list_node_t *ln, void *data_)
{
	show_rels_data_t *rd = data_;
	post_t *post = ((postlist_node_t *)ln)->post;
	c_printf(rd->conn, "R%s %s\n", rd->md5, md5_md52str(post->md5));
}

static int show_rels_cmd(connection_t *conn, const char *cmd, void *data,
                         prot_cmd_flag_t flags)
{
	show_rels_data_t rd;
	post_t           *post;
	(void)data;
	(void)flags;
	if (post_find_md5str(&post, cmd)) return conn->error(conn, cmd);
	rd.conn = conn;
	strcpy(rd.md5, md5_md52str(post->md5));
	list_iterate(&post->related_posts.h.l, &rd, show_rels_cb);
	return 0;
}

static int show_impl_cmd(connection_t *conn, const char *cmd, void *data,
                         prot_cmd_flag_t flags)
{
	(void) data;
	(void) flags;
	tag_t *tag = tag_find_guidstr(cmd);
	if (!tag) return 1;
	impllist_t *tl = tag->implications;
	int to_go = 0;
	const char *newline = "";
	while (tl) {
		for (int i = 0; i < arraylen(tl->impl); i++) {
			if (tl->impl[i].tag) {
				implication_t *impl = &tl->impl[i];
				if (!to_go) {
					to_go = 10;
					c_printf(conn, "%sRI%s", newline,
					         guid_guid2str(tag->guid));
					newline = "\n";
				}
				to_go--;
				c_printf(conn, " %c%s:%ld",
				         impl->positive ? 'I' : 'i',
				         guid_guid2str(impl->tag->guid),
				         (long)impl->priority);
			}
		}
		tl = tl->next;
	}
	c_printf(conn, "%s", newline);
	return 0;
}

typedef struct rev_impl_data {
	connection_t *conn;
	tag_t *tag;
} rev_impl_data_t;

static void show_rev_impl_cb(ss128_key_t key, ss128_value_t value, void *data_)
{
	(void) key;
	tag_t *tag = (tag_t *)value;
	rev_impl_data_t *data = data_;
	impllist_t *impllist = tag->implications;
	while (impllist) {
		for (int i = 0; i < arraylen(impllist->impl); i++) {
			implication_t *impl = &impllist->impl[i];
			if (impl->tag == data->tag) {
				c_printf(data->conn, "RI%s ",
				         guid_guid2str(tag->guid));
				c_printf(data->conn, "%c%s:%ld\n",
					 impl->positive ? 'I' : 'i',
					 guid_guid2str(impl->tag->guid),
					 (long)impl->priority);
				return;
			}
		}
		impllist = impllist->next;
	}
}

static int show_rev_impl_cmd(connection_t *conn, const char *cmd, void *data,
                             prot_cmd_flag_t flags)
{
	(void) data;
	(void) flags;
	rev_impl_data_t revdata;
	revdata.conn = conn;
	revdata.tag = tag_find_guidstr(cmd);
	if (!revdata.tag) return 1;
	ss128_iterate(tags, show_rev_impl_cb, &revdata);
	return 0;
}

typedef struct metalist {
	const char *name;
	const char * const *list;
} metalist_t;

static void list_print(connection_t *conn, const char * const *list)
{
	while (*list) {
		c_printf(conn, "RN%s\n", *list);
		list++;
	}
}

static void list_cmd(connection_t *conn, const char *cmd)
{
	const metalist_t list[] = {{"tagtypes", tagtype_names},
	                           {"ratings", rating_names},
	                           {NULL, NULL}
	                          };
	for (const metalist_t *p = list; p->name; p++) {
		if (!strcmp(p->name, cmd)) {
			list_print(conn, p->list);
			c_printf(conn, "OK\n");
			return;
		}
	}
	conn->error(conn, cmd);
}

static void modifying_command(connection_t *conn,
                              int (*func)(connection_t *, char *), char *cmd)
{
	int ok;

	log_trans_start(conn, time(NULL));
	ok = !func(conn, cmd);
	log_trans_end(conn);
	if (ok) c_printf(conn, "OK\n");
}

extern int connection_count;

void client_handle(connection_t *conn, char *buf)
{
	switch (*buf) {
		case 'S': // 'S'earch
			if (buf[1] == 'P') {
				search_t search;
				init_search(&search);
				int r = prot_cmd_loop(conn, buf + 2, &search,
				                      build_search_cmd,
				                      CMDFLAG_NONE);
				if (!r) r = setup_search(&search);
				result_t result;
				if (!r) {
					do_search(conn, &search, &result);
					print_search(conn, &search, &result);
				}
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
		case 'D': // 'D'elete something
			modifying_command(conn, prot_delete, buf + 1);
			break;
		case 'R': // 'R'elationship
			switch (buf[1]) {
				case 'R':
					modifying_command(conn, prot_rel_add,
					                  buf + 2);
					break;
				case 'r':
					modifying_command(conn, prot_rel_remove,
					                  buf + 2);
					break;
				case 'S':
					prot_cmd_loop(conn, buf + 2, NULL,
					              show_rels_cmd, 0);
					c_printf(conn, "OK\n");
					break;
				default:
					c_close_error(conn, E_COMMAND);
					break;
			}
			break;
		case 'I': // 'I'mplication management
			switch (buf[1]) {
				case 'I':
				case 'i':
					modifying_command(conn, prot_implication, buf + 1);
					break;
				case 'S':
					prot_cmd_loop(conn, buf + 2, NULL,
					              show_impl_cmd, 0);
					c_printf(conn, "OK\n");
					break;
				case 'R':
					prot_cmd_loop(conn, buf + 2, NULL,
					              show_rev_impl_cmd, 0);
					c_printf(conn, "OK\n");
					break;
				default:
					c_close_error(conn, E_COMMAND);
					break;
			}
			break;
		case 'O': // 'O'rder
			modifying_command(conn, prot_order, buf + 1);
			break;
		case 'L': // 'L'ist
			list_cmd(conn, buf + 1);
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
		case 't': // 't'ransaction
			if ((buf[1] != 'B' && buf[1] != 'E') || buf[2]) {
				c_close_error(conn, E_COMMAND);
			}
			int bad;
			if (buf[1] == 'B') {
				bad = log_trans_start_outer(conn, time(NULL));
			} else { // 'E'
				bad = log_trans_end_outer(conn);
			}
			if (bad) {
				c_printf(conn, "E\n");
			} else {
				c_printf(conn, "OK\n");
			}
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
				server_running = 0;
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
