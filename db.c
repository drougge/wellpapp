#include "db.h"

#include <math.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <poll.h>
#include <errno.h>
#include <openssl/md5.h>
#include <utf8proc.h>
#include <bzlib.h>

#ifndef INFTIM
#define INFTIM -1
#endif

void NORETURN assert_fail(const char *ass, const char *file,
                          const char *func, int line)
{
	fprintf(stderr, "assertion \"%s\" failed in %s on %s:%d\n",
	        ass, func, file, line);
	exit(1);
}

ss128_head_t *tags;
ss128_head_t *tagaliases;
ss128_head_t *tagguids;
ss128_head_t *posts;
list_head_t  *postlist_nodes;

int default_timezone = 0;
int log_version = -1;

static postlist_node_t *postlist_alloc(void)
{
	postlist_node_t *pn = (postlist_node_t *)list_remhead(postlist_nodes);
	if (!pn) return mm_alloc(sizeof(postlist_node_t));
	return pn;
}

static void postlist_free(postlist_node_t *pn)
{
	list_addhead(postlist_nodes, &pn->n.l);
}

static int postlist_remove(postlist_t *pl, post_t *post)
{
	postlist_node_t *pn;

	assert(pl);
	assert(post);

	pn = pl->h.p.head;
	while (pn->n.p.succ) {
		if (pn->post == post) {
			list_remove(&pn->n.l);
			postlist_free(pn);
			pl->count--;
			return 0;
		}
		pn = pn->n.p.succ;
	}
	return 1;
}

static void postlist_add(postlist_t *pl, post_t *post)
{
	assert(pl);
	assert(post);
	pl->count++;
	postlist_node_t *pn = postlist_alloc();
	pn->post = post;
	list_addtail(&pl->h.l, &pn->n.l);
}

static int postlist_contains(const postlist_t *pl, const post_t *post)
{
	const postlist_node_t *pn = pl->h.p.head;
	while (pn->n.p.succ) {
		if (pn->post == post) return 1;
		pn = pn->n.p.succ;
	}
	return 0;
}

int taglist_contains(const post_taglist_t *tl, const tag_t *tag)
{
	while (tl) {
		for (int i = 0; i < arraylen(tl->tags); i++) {
			if (tl->tags[i] == tag) return 1;
		}
		tl = tl->next;
	}
	return 0;
}

typedef struct alloc_seg {
	struct alloc_seg *next;
} alloc_seg_t;
typedef struct alloc_data {
	alloc_seg_t *segs;
} alloc_data_t;
typedef void *(*alloc_func_t)(alloc_data_t *data, unsigned int size);

static void *alloc_temp(alloc_data_t *data, unsigned int size)
{
	alloc_seg_t *seg;
	seg = calloc(1, sizeof(seg) + size);
	seg->next = data->segs;
	data->segs = seg;
	return seg + 1;
}
static void alloc_temp_free(alloc_data_t *data)
{
	alloc_seg_t *seg = data->segs;
	while (seg) {
		alloc_seg_t *next = seg->next;
		free(seg);
		seg = next;
	}
}
static void *alloc_mm(alloc_data_t *data, unsigned int size)
{
	(void) data;
	return mm_alloc(size);
}

#define TAG_VALUE_PARSER(vtype, vfunc, ftype, ffunc)                        \
	static int tv_parser_##vtype(const char *val, vtype *v, ftype *f)   \
	{                                                                   \
		char *end;                                                  \
		*v = vfunc(val, &end);                                      \
		*f = 0;                                                     \
		if (val == end) return 1;                                   \
		if (end[0] == '+' && end[1] == '-') {                       \
			val = end + 2;                                      \
			*f = ffunc(val, &end);                              \
			if (!*val || (double)*f < 0) return 1;              \
		}                                                           \
		if (*end) {                                                 \
			return 1;                                           \
		}                                                           \
		return 0;                                                   \
	}
#define str2SI(v, e) strtoll(v, e, 10)
#define str2UI(v, e) strtoull(v, e, 16)
static double fractod(const char *val, char **r_end)
{
	char *end;
	double n = strtoll(val, &end, 10);
	if (end != val && *end == '/') {
		double d = strtoll(end + 1, &end, 10);
		if (d > 0.0 && (*end == 0 || *end == '+')) {
			*r_end = end;
			return n / d;
		}
	}
	return strtod(val, r_end);
}
TAG_VALUE_PARSER(int64_t, str2SI, uint64_t, str2UI)
TAG_VALUE_PARSER(uint64_t, str2UI, uint64_t, str2UI)
TAG_VALUE_PARSER(double, fractod, double, fractod)

static int tvp_datetimefuzz(const char *val, uint64_t *r)
{
	if (!*val) return 1;
	char *end;
	double fuzz = fractod(val, &end);
	if (val == end) fuzz = 1.0; // For "+-H" etc
	if (fuzz < 0.0) return 1;
	if (*end) {
		if (end[1]) return 1;
		switch (*end) {
			case 'Y':
				fuzz *= 12.0;
			case 'm':
				fuzz *= 30.5;
			case 'd':
				fuzz *= 24.0;
			case 'H':
				fuzz *= 60.0;
			case 'M':
				fuzz *= 60.0;
			case 'S':
				break;
			default:
				return 1;
				break;
		}
	}
	*r = ceil(fuzz);
	return 0;
}

static int tvp_timezone(const char *val, int *r_offset, const char **r_end)
{
	switch (*val) {
		case '+':
		case '-':
			for (int i = 1; i <= 4; i++) {
				if (val[i] < '0' || val[i] > '9') return 1;
			}
			int hour = (val[1] - '0') * 10 + val[2] - '0';
			int minute = (val[3] - '0') * 10 + val[4] - '0';
			if (hour >= 12 || minute >= 60) return 1;
			int offset = (hour * 60 + minute) * 60;
			if (*val == '+') offset = -offset;
			*r_offset = offset;
			*r_end = val + 5;
			break;
		case 'Z': // UTC
			*r_offset = 0;
			*r_end = val + 1;
			break;
		default:
			return 1;
			break;
	}
	return 0;
}

static int tv_parser_datetime(const char *val, int64_t *v, uint64_t *f)
{
	char fmt[] = "%Y-%m-%dT%H:%M:%S";
	struct tm tm;
	const char *ptr = NULL;
	int fmt_pos = strlen(fmt);
	memset(&tm, 0, sizeof(tm));
	tm.tm_mday  = 1;
	// Try to parse succesively less specific dates
	while (!ptr && fmt_pos > 0) {
		fmt[fmt_pos] = 0;
		ptr = strptime(val, fmt, &tm);
		fmt_pos -= 3;
	}
	if (!ptr) return 1;
	time_t unixtime = mktime(&tm);
	// So I can't say 1969-12-31T23:59:59 then? Bloody POSIX.
	if (unixtime == (time_t)-1) return 1;
	// Check that what was parsed matches the input (after mktime normalises it)
	char buf[64];
	int len = strftime(buf, sizeof(buf), fmt, &tm);
	if (memcmp(buf, val, len)) return 1;
	// If the time was not fully specified, there's an implicit fuzz.
	int pos2f[] = {30 * 60 * 24 * 365, 0, 0, 30 * 60 * 24 * 30.4375, 0, 0,
	               30 * 60 * 24, 0, 0, 30 * 60, 0, 0, 30, 0, 0, 0};
	// Stupid leap years.
	int y = tm.tm_year + 1900;
	if (y % 4 == 0 && (y % 400 == 0 || y % 100)) pos2f[0] += 30*60*24;
	*f = pos2f[fmt_pos + 1];
	// Move the time forward to middle of its' possible range.
	unixtime += *f / 2;
	// Is there a timezone and/or a fuzz?
	if (ptr[0] == '+' && ptr[1] == '-') { // Only fuzz
		unixtime += default_timezone;
		if (tvp_datetimefuzz(ptr + 2, f)) return 1;
	} else if (*ptr) { // Must be a timezone
		int offset;
		if (tvp_timezone(ptr, &offset, &ptr)) return 1;
		unixtime += offset;
		if (*ptr) {
			if (ptr[0] != '+' || ptr[1] != '-') return 1;
			if (tvp_datetimefuzz(ptr + 2, f)) return 1;
		}
	} else {
		unixtime += default_timezone;
	}
	*v = unixtime;
	return 0;
}

/* fstop is externally specified as f/num where num is how much smaller
 * than the focal length the (virtual) aperture is. This is what everyone
 * uses these days. But it's not particularly convenient to calculate
 * differences in, so we store it differently. We store x, where:
 * fstop = sqrt(2) ** x, because that's linear.
 * (So if fstop is, say, 8, we can do x+-1 to get 5.6 to 11.)
 */
static void scale_f_stop(tag_value_t *tval)
{
	double fstop = tval->val.v_double;
	tval->val.v_double = 2.0 * log2(fstop);
}

/* Same deal with ISO, externally we use the modern arithmetic scale, 100
 * is a fairly slow film. Internally we use the old DIN scale - 1 divided by 3,
 * so each stop is 1. (That same fairly slow film is 20/3.)
 * This also works for shutter speeds.
 */
static void scale_stop(tag_value_t *tval)
{
	double iso = tval->val.v_double;
	tval->val.v_double = 10.0 * log10(iso) / 3.0;
}

int tag_value_parse(tag_t *tag, const char *val, tag_value_t *tval, int tmp)
{
	if (!val) return 1;
	switch (tag->valuetype) {
		case VT_STRING:
			tval->v_str = mm_strdup(str_enc2str(val));
			return 0;
			break;
		case VT_INT:
			if (!tv_parser_int64_t(val, &tval->val.v_int,
			                       &tval->fuzz.f_int)) {
				return 0;
			}
			break;
		case VT_UINT:
			if (!tv_parser_uint64_t(val, &tval->val.v_uint,
			                        &tval->fuzz.f_uint)) {
				return 0;
			}
			break;
		case VT_FLOAT:
		case VT_F_STOP:
		case VT_STOP:
			if (!tv_parser_double(val, &tval->val.v_double,
			                      &tval->fuzz.f_double)) {
				if (tmp) {
					tval->v_str = val;
				} else {
					tval->v_str = mm_strdup(val);
				}
				if (tag->valuetype == VT_F_STOP) {
					scale_f_stop(tval);
				} else if (tag->valuetype == VT_STOP) {
					scale_stop(tval);
				}
				return 0;
			}
			break;
		case VT_DATETIME:
			if (!tv_parser_datetime(val, &tval->val.v_int,
			                        &tval->fuzz.f_int)) {
				if (tmp) {
					tval->v_str = val;
				} else {
					tval->v_str = mm_strdup(val);
				}
				return 0;
			}
			break;
		default: // VT_NONE, or bad value
			break;
	}
	return 1;
}

static int taglist_add(post_taglist_t **tlp, tag_t *tag, alloc_func_t alloc,
                       alloc_data_t *adata)
{
	post_taglist_t *tl = *tlp;
	while (tl) {
		for (int i = 0; i < arraylen(tl->tags); i++) {
			if (!tl->tags[i]) {
				tl->tags[i] = tag;
				return 0;
			}
			if (tl->tags[i] == tag) return 1;
		}
		tl = tl->next;
	}
	tl = alloc(adata, sizeof(*tl));
	tl->tags[0] = tag;
	tl->next = *tlp;
	*tlp = tl;
	return 0;
}

struct impl_iterator_data;
typedef struct impl_iterator_data impl_iterator_data_t;
typedef void (*impl_callback_t)(implication_t *impl, impl_iterator_data_t *data);
typedef struct implcomp_data {
	implication_t *impl;
	truth_t       weak;
} implcomp_data_t;
struct impl_iterator_data {
	implcomp_data_t *list;
	int             len;
	truth_t         weak;
	impl_callback_t callback;
};

static void impllist_iterate(impllist_t *impl, impl_iterator_data_t *data)
{
	while (impl) {
		for (int i = 0; i < arraylen(impl->impl); i++) {
			if (impl->impl[i].tag) {
				data->callback(&impl->impl[i], data);
			}
		}
		impl = impl->next;
	}
}

static void impl_cb(implication_t *impl, impl_iterator_data_t *data)
{
	if (data->list) {
		data->list[data->len].impl = impl;
		data->list[data->len].weak = data->weak;
	}
	data->len++;
}

static int impl_comp(const void *a_, const void *b_, void *data)
{
	const implcomp_data_t *a = a_;
	const implcomp_data_t *b = b_;
	(void) data;
	if (a->impl->priority == b->impl->priority) {
		if (a->weak == b->weak) return 0;
		if (a->weak) return 1;
		return -1;
	}
	return b->impl->priority - a->impl->priority;
}

static void post_implications(post_t *post, alloc_func_t alloc,
                              alloc_data_t *adata, post_taglist_t **res)
{
	impl_iterator_data_t impldata;
	post_taglist_t *tl;
	assert(post);
	assert(alloc);
	impldata.list = NULL;
	impldata.len = 0;
	impldata.weak = T_NO;
	impldata.callback = impl_cb;
again:
	tl = impldata.weak ? post->weak_tags : &post->tags;
	while (tl) {
		for (int i = 0; i < arraylen(tl->tags); i++) {
			tag_t *tag = tl->tags[i];
			if (tag && tag->implications) {
				impllist_iterate(tag->implications, &impldata);
			}
		}
		tl = tl->next;
	}
	if (!impldata.weak) {
		impldata.weak = T_YES;
		goto again;
	}
	if (!impldata.list && impldata.len) {
		impldata.list = malloc(sizeof(*impldata.list) * impldata.len);
		impldata.len = 0;
		impldata.weak = T_NO;
		goto again;
	}
	if (impldata.list) {
		implcomp_data_t *list = impldata.list;
		int             len = impldata.len;
		sort(list, len, sizeof(*list), impl_comp, NULL);
		for (int i = 0; i < len; i++) {
			int skip = 0;
			for (int j = 0; j < i; j++) {
				if (list[i].impl->tag == list[j].impl->tag) {
					skip = 1;
					break;
				}
			}
			if (!skip && list[i].impl->positive) {
				taglist_add(&res[list[i].weak],
				           list[i].impl->tag, alloc, adata);
			}
		}
		free(list);
	}
}

static int post_tag_add_i(post_t *post, tag_t *tag, truth_t weak, int implied,
                          tag_value_t *tval);
static int post_tag_rem_i(post_t *post, tag_t *tag, int implied);
static int impl_apply_change(post_t *post, post_taglist_t **old,
                             post_taglist_t *new, truth_t weak)
{
	post_taglist_t *tl;
	int changed = 0;
	tl = new;
	while (tl) {
		for (int i = 0; i < arraylen(tl->tags); i++) {
			tag_t *tag = tl->tags[i];
			if (tag && !taglist_contains(*old, tag)) {
				if (!post_has_tag(post, tag, T_DONTCARE)) {
					post_tag_add_i(post, tag, weak, 1, NULL);
					taglist_add(old, tag, alloc_mm, NULL);
					changed = 1;
				} else {
					tl->tags[i] = NULL;
				}
			}
		}
		tl = tl->next;
	}
	tl = *old;
	while (tl) {
		for (int i = 0; i < arraylen(tl->tags); i++) {
			tag_t *tag = tl->tags[i];
			if (tag && !taglist_contains(new, tag)) {
				post_tag_rem_i(post, tag, 1);
				tl->tags[i] = NULL;
				changed = 1;
			}
		}
		tl = tl->next;
	}
	return changed;
}

static void post_recompute_implications(post_t *post)
{
	alloc_data_t adata;
	post_taglist_t *implied[2];
	int again;
	int depth = 0;
again:
	again = 0;
	adata.segs = NULL;
	implied[0] = implied[1] = NULL;
	post_implications(post, alloc_temp, &adata, implied);
	again |= impl_apply_change(post, &post->implied_tags, implied[0], T_NO);
	again |= impl_apply_change(post, &post->implied_weak_tags, implied[1],
	                           T_YES);
	alloc_temp_free(&adata);
	if (again) {
		if (depth++ > 64) { // Really, how complicated do you need?
			fprintf(stderr, "Bailing out of implications on %s\n",
			        md5_md52str(post->md5));
		} else {
			goto again;
		}
	}
}

static void post_recompute_implications_iter(list_node_t *ln, void *data)
{
	(void) data;
	post_recompute_implications(((postlist_node_t *)ln)->post);
}

static void postlist_recompute_implications(postlist_t *pl)
{
	list_iterate(&pl->h.l, NULL, post_recompute_implications_iter);
}

int tag_add_implication(tag_t *from, tag_t *to, int positive, int32_t priority)
{
	impllist_t *tl = from->implications;
	int done = 0;
	while (tl) {
		for (int i = 0; i < arraylen(tl->impl); i++) {
			tag_t *tltag = tl->impl[i].tag;
			if ((!tltag || tltag == to) && !done) {
				tl->impl[i].tag = to;
				tl->impl[i].priority = priority;
				tl->impl[i].positive = positive;
				done = 1;
			} else if (tltag == to) {
				tl->impl[i].tag = NULL;
			}
		}
		tl = tl->next;
	}
	if (!done) {
		tl = mm_alloc(sizeof(*tl));
		tl->impl[0].tag = to;
		tl->impl[0].priority = priority;
		tl->impl[0].positive = positive;
		tl->next = from->implications;
		from->implications = tl;
	}
	postlist_recompute_implications(&from->posts);
	postlist_recompute_implications(&from->weak_posts);
	return 0;
}

int tag_rem_implication(tag_t *from, tag_t *to, int positive, int32_t priority)
{
	impllist_t *tl = from->implications;
	(void) priority;
	while (tl) {
		for (int i = 0; i < arraylen(tl->impl); i++) {
			if (tl->impl[i].tag == to
			    && tl->impl[i].positive == positive
			   ) {
				tl->impl[i].tag = NULL;
				postlist_recompute_implications(&from->posts);
				postlist_recompute_implications(&from->weak_posts);
				return 0;
			}
		}
		tl = tl->next;
	}
	return 1;
}

static int taglist_remove(post_taglist_t *tl, const tag_t *tag)
{
	while (tl) {
		for (unsigned int i = 0; i < arraylen(tl->tags); i++) {
			if (tl->tags[i] == tag) {
				tl->tags[i] = NULL;
				return 0;
			}
		}
		tl = tl->next;
	}
	return 1;
}

static int post_tag_rem_i(post_t *post, tag_t *tag, int implied)
{
	assert(post);
	assert(tag);
	if (!implied && (taglist_contains(post->implied_tags, tag)
	                 || taglist_contains(post->implied_weak_tags, tag))
	   ) return 1;
	if (!taglist_remove(&post->tags, tag)) {
		post->of_tags--;
		return postlist_remove(&tag->posts, post);
	}
	if (!taglist_remove(post->weak_tags, tag)) {
		post->of_weak_tags--;
		return postlist_remove(&tag->weak_posts, post);
	}
	return 1;
}

int post_tag_rem(post_t *post, tag_t *tag)
{
	int r = post_tag_rem_i(post, tag, 0);
	if (!r) post_recompute_implications(post);
	return r;
}

static int post_tag_add_i(post_t *post, tag_t *tag, truth_t weak, int implied,
                          tag_value_t *tval)
{
	post_taglist_t *tl;
	post_taglist_t *ptl = NULL;
	int i;

	assert(post);
	assert(tag);
	assert(weak == T_YES || weak == T_NO);
	assert(!implied || !tval);
	if (!implied) {
		if (taglist_contains(post->implied_tags, tag)) {
			int r = taglist_remove(post->implied_tags, tag);
			assert(!r);
			if (!weak) return 0;
		} else if (taglist_contains(post->implied_weak_tags, tag)) {
			int r = taglist_remove(post->implied_weak_tags, tag);
			assert(!r);
			if (weak) return 0;
		}
	}
	if (!tval && post_has_tag(post, tag, weak)) return 1;
	if (post_has_tag(post, tag, T_DONTCARE)) {
		if (post_tag_rem_i(post, tag, 0)) return 1;
	}
	if (weak) {
		postlist_add(&tag->weak_posts, post);
		tl = post->weak_tags;
		if (!tl) tl = post->weak_tags = mm_alloc(sizeof(*tl));
		post->of_weak_tags++;
	} else {
		postlist_add(&tag->posts, post);
		tl = &post->tags;
		post->of_tags++;
	}
	while (tl) {
		for (i = 0; i < arraylen(tl->tags); i++) {
			if (!tl->tags[i]) {
				tl->tags[i] = tag;
				tl->values[i] = mm_dup(tval, sizeof(*tval));
				return 0;
			}
		}
		ptl = tl;
		tl  = tl->next;
	}
	tl = mm_alloc(sizeof(*tl));
	tl->tags[0]  = tag;
	tl->values[0] = mm_dup(tval, sizeof(*tval));
	ptl->next    = tl;
	return 0;
}

int post_tag_add(post_t *post, tag_t *tag, truth_t weak, tag_value_t *tval)
{
	int r = post_tag_add_i(post, tag, weak, 0, tval);
	if (!r) post_recompute_implications(post);
	return r;
}

int post_has_rel(const post_t *post, const post_t *rel)
{
	return postlist_contains(&post->related_posts, rel);
}

int post_rel_add(post_t *a, post_t *b)
{
	if (post_has_rel(a, b)) return 1;
	assert(!post_has_rel(b, a));
	postlist_add(&a->related_posts, b);
	postlist_add(&b->related_posts, a);
	return 0;
}

int post_rel_remove(post_t *a, post_t *b)
{
	int r;
	r = postlist_remove(&a->related_posts, b);
	if (r) return 1;
	r = postlist_remove(&b->related_posts, a);
	assert(!r);
	return 0;
}

static int md5_digit2digit(int digit)
{
	if (digit >= '0' && digit <= '9') return digit - '0';
	if (digit >= 'a' && digit <= 'f') return digit - 'a' + 10;
	if (digit >= 'A' && digit <= 'F') return digit - 'A' + 10;
	return -1;
}

int md5_str2md5(md5_t *res_md5, const char *md5str)
{
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

const char *md5_md52str(const md5_t md5)
{
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

int str2id(const char *str, const char * const *ids)
{
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

tag_t *tag_find_guid(const guid_t guid)
{
	void *tag = NULL;
	ss128_find(tagguids, &tag, guid.key);
	return (tag_t *)tag;
}

tag_t *tag_find_guidstr(const char *guidstr)
{
	guid_t guid;
	if (guid_str2guid(&guid, guidstr, GUIDTYPE_TAG)) return NULL;
	return tag_find_guid(guid);
}

tag_t *tag_find_guidstr_value(const char *guidstr, tagvalue_cmp_t *r_cmp,
                              tag_value_t *value, int tmp)
{
	int len = strlen(guidstr);
	*r_cmp = CMP_NONE;
	if (len <= 27) return tag_find_guidstr(guidstr);
	char guid[28];
	memcpy(guid, guidstr, 27);
	guid[27] = '\0';
	const char *v = guidstr + 27;
	tag_t *tag = tag_find_guidstr(guid);
	if (!tag) return NULL;
	if (!tag->valuetype) return NULL;
	switch (v[0]) {
		case '=':
			*r_cmp = CMP_EQ;
			if (v[1] == '~') {
				if (tag->valuetype != VT_STRING) return NULL;
				*r_cmp = CMP_REGEXP;
				v++;
			}
			break;
		case '>':
			*r_cmp = CMP_GT;
			if (v[1] == '=') {
				*r_cmp = CMP_GE;
				v++;
			}
			break;
		case '<':
			*r_cmp = CMP_LT;
			if (v[1] == '=') {
				*r_cmp = CMP_LE;
				v++;
			}
			break;
		default:
			return NULL;
			break;
	}
	if (tag_value_parse(tag, v + 1, value, tmp)) return NULL;
	return tag;
}

tag_t *tag_find_name(const char *name, truth_t alias, tagalias_t **r_tagalias)
{
	ss128_key_t hash = ss128_str2key(name);
	void *found = NULL;
	tag_t *tag = NULL;

	if (alias != T_YES) {
		ss128_find(tags, &found, hash);
		tag = found;
		if (r_tagalias) *r_tagalias = NULL;
	}
	if (!tag && alias != T_NO) {
		ss128_find(tagaliases, &found, hash);
		tagalias_t *tagalias = found;
		if (tagalias) tag = tagalias->tag;
		if (r_tagalias) *r_tagalias = tagalias;
	}
	return tag;
}

static int post_tag(const post_t *post, const tag_t *tag, truth_t weak,
                    const post_taglist_t **r_tl, int *r_i)
{
	const post_taglist_t *tl;
	assert(post);
	assert(tag);
again:
	if (weak) {
		tl = post->weak_tags;
	} else {
		tl = &post->tags;
	}
	while (tl) {
		unsigned int i;
		for (i = 0; i < arraylen(tl->tags); i++) {
			if (tl->tags[i] == tag) {
				*r_tl = tl;
				*r_i  = i;
				return 1;
			}
		}
		tl = tl->next;
	}
	if (weak == T_DONTCARE) {
		weak = T_NO;
		goto again;
	}
	return 0;
}

int post_has_tag(const post_t *post, const tag_t *tag, truth_t weak)
{
	const post_taglist_t *tl;
	int i;
	return post_tag(post, tag, weak, &tl, &i);
}

tag_value_t *post_tag_value(const post_t *post, const tag_t *tag)
{
	const post_taglist_t *tl;
	int i;
	if (post_tag(post, tag, T_DONTCARE, &tl, &i)) {
		return tl->values[i];
	}
	return NULL;
}

int post_find_md5str(post_t **res_post, const char *md5str)
{
	md5_t md5;
	*res_post = NULL;
	if (md5_str2md5(&md5, md5str)) return -1;
	return ss128_find(posts, (void *)res_post, md5.key);
}

void post_modify(post_t *post, time_t now)
{
	tag_value_t tval;
	tag_value_t *tval_p = post_tag_value(post, magic_tag_modified);
	if (!tval_p) {
		memset(&tval, 0, sizeof(tval));
		tval_p = &tval;
	}
	if (tval_p->val.v_int != now) {
		tval_p->val.v_int = now;
		datetime_strfix(tval_p);
	}
	if (!tval_p->v_str) datetime_strfix(tval_p);
	if (tval_p == &tval) post_tag_add(post, magic_tag_modified, T_NO, &tval);
}

typedef struct logfh {
	FILE   *fh;
	BZFILE *bzfh;
	int    pos;
	int    len;
	int    eof;
	char   buf[4096];
} logfh_t;

static int read_log_line(logfh_t *fh, char *buf, int len)
{
	int blen = 0;
	if (fh->eof > 1) {
		*buf = '\0';
		return 0;
	}
again:
	while (fh->pos < fh->len) {
		int c = fh->buf[fh->pos++];
		if (++blen == len || c == '\n') {
			*buf = '\0';
			assert(blen > 8);
			return blen - 1;
		}
		*(buf++) = c;
	}
	fh->pos = 0;
	if (fh->eof) {
		fh->len = 0;
	} else if (fh->bzfh) {
		int e = 0;
		fh->len = BZ2_bzRead(&e, fh->bzfh, fh->buf, sizeof(fh->buf));
		if (e == BZ_STREAM_END) {
			fh->eof = 1;
		} else {
			assert(e == BZ_OK);
		}
	} else {
		fh->len = fread(fh->buf, 1, sizeof(fh->buf), fh->fh);
	}
	if (fh->len <= 0) {
		assert(feof(fh->fh));
		if (fh->bzfh) {
			int e = 0;
			void *bzptr;
			int bzlen;
			BZ2_bzReadGetUnused(&e, fh->bzfh, &bzptr, &bzlen);
			assert(e == BZ_OK && bzlen == 0);
		}
		fh->eof = 2;
		*buf = '\0';
		assert(blen >= 8 || blen == 0);
		return blen;
	}
	goto again;
}

static int populate_from_log_line(char *line)
{
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
		case 'D': // 'D'elete something
			r = prot_delete(logconn, line + 1);
			break;
		case 'R': // 'R'elationship
			if (line[1] == 'R') {
				r = prot_rel_add(logconn, line + 2);
			} else {
				assert(line[1] == 'r');
				r = prot_rel_remove(logconn, line + 2);
			}
			break;
		case 'I': // 'I'mplication
			r = prot_implication(logconn, line + 1);
			break;
		case 'O': // 'O'rder
			r = prot_order(logconn, line + 1);
			break;
		default:
			printf("Log: What? %s\n", line);
			r = 1;
	}
	return r;
}

#define MAX_CONCURRENT_TRANSACTIONS 64
static int find_trans(trans_id_t *trans, trans_id_t needle)
{
	int i;
	for (i = 0; i < MAX_CONCURRENT_TRANSACTIONS; i++) {
		if (trans[i] == needle) return i;
	}
	return -1;
}

int populate_from_log(const char *filename, void (*callback)(const char *line))
{
	logfh_t    fh;
	char       buf[4096];
	trans_id_t trans[MAX_CONCURRENT_TRANSACTIONS] = {0};
	time_t     transnow[MAX_CONCURRENT_TRANSACTIONS];
	int        len;
	long       line_nr = 0;
	int        bze = 0;

	memset(&fh, 0, sizeof(fh));
	fh.fh = fopen(filename, "r");
	if (!fh.fh) {
		assert(errno == ENOENT);
		snprintf(buf, sizeof(buf), "%s.bz2", filename);
		fh.fh = fopen(buf, "rb");
		if (!fh.fh) {
			assert(errno == ENOENT);
			return 1;
		}
		fh.bzfh = BZ2_bzReadOpen(&bze, fh.fh, 0, 0, NULL, 0);
		assert(bze == BZ_OK && fh.bzfh);
	}
	while ((len = read_log_line(&fh, buf, sizeof(buf)))) {
		char       *end;
		trans_id_t tid = strtoull(buf + 1, &end, 16);
		line_nr++;
		if (*buf == '#' && log_version < 0) continue;
		err1(end != buf + 17);
		if (*buf == 'T') { // New transaction
			err1(len != 34);
			if (buf[17] == 'O') { // Complete transaction
				int trans_pos = find_trans(trans, tid);
				assert(trans_pos == -1);
				trans_pos = find_trans(trans, 0);
				assert(trans_pos != -1);
				trans[trans_pos] = tid;
				int trans_version = buf[18] - '0';
				err1(trans_version < log_version);
				err1(trans_version > LOG_VERSION);
				log_version = trans_version;
				transnow[trans_pos] = strtoull(buf + 19, &end, 16);
				err1(end != buf + 34);
			} else if (buf[17] == 'U') { // Unfinished transaction
				// Do nothing
			} else { // What?
				goto err;
			}
		} else if (*buf == 'D') { // Data from transaction
			err1(len <= 18);
			int trans_pos = find_trans(trans, tid);
			if (trans_pos >= 0) {
				logconn->trans.now = transnow[trans_pos];
				err1(populate_from_log_line(buf + 18));
			} else {
				printf("Skipping data from incomplete transaction: %s\n", buf);
			}
		} else if (*buf == 'E') { // End of transaction
			int pos;
			err1(len != 17);
			pos = find_trans(trans, tid);
			if (pos != -1) trans[pos] = 0;
		} else { // What?
			err1(!callback);
			callback(buf);
		}
	}
	if (fh.bzfh) {
		BZ2_bzReadClose(&bze, fh.bzfh);
		assert(bze == BZ_OK);
	}
	struct stat sb;
	int r = fstat(fileno(fh.fh), &sb);
	assert(!r);
	fclose(fh.fh);
	mm_last_log(sb.st_size, sb.st_mtime);
	return 0;
err:
	printf("Failed on line %ld:\n%s\n", line_nr, buf);
	exit(1);
}

#define MAX_CONNECTIONS 100

struct pollfd fds[MAX_CONNECTIONS + 1];
connection_t *connections[MAX_CONNECTIONS];
int connection_count = 0;
int server_running = 1;

void conn_cleanup(void)
{
	for(int i = 0; i < arraylen(connections); i++) {
		connection_t *conn = connections[i];
		if (conn) c_cleanup(conn);
	}
}

static void new_connection(void)
{
	int s = accept(fds[MAX_CONNECTIONS].fd, NULL, NULL);
	if (s < 0) {
		perror("accept");
	} else {
		connection_t *conn;
		int           i;

		for (i = 0; i < MAX_CONNECTIONS; i++) {
			if (!connections[i]) break;
		}
		if (i == MAX_CONNECTIONS
		 || c_init(&conn, s, c_error)) {
			close(s);
			return;
		}
		connections[i] = conn;
		fds[i].fd = s;
		fds[i].revents = 0;
		connection_count++;
	}
}

static char *utf_compose(connection_t *conn)
{
	uint8_t *buf;
	ssize_t res;
	int     flags = UTF8PROC_NULLTERM | UTF8PROC_STABLE | UTF8PROC_COMPOSE;

	res = utf8proc_map((uint8_t *)conn->linebuf, 0, &buf, flags);
	if (res < 0) {
		c_close_error(conn, E_UTF8);
		return NULL;
	}
	return (char *)buf;
}

static int bind_port = 0;
static in_addr_t bind_addr = 0;

void db_serve(void)
{
	int s, r, one, i;
	struct sockaddr_in addr;

	for (i = 0; i < MAX_CONNECTIONS; i++) {
		fds[i].fd = -1;
		fds[i].events = POLLIN;
		connections[i] = NULL;
	}

	s = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
	assert(s >= 0);
	one = 1;
	r = setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
	assert(!r);
	memset(&addr, 0, sizeof(addr));
#if !defined(__linux__) && !defined(__svr4__)
	addr.sin_len    = sizeof(addr);
#endif
	addr.sin_family = AF_INET;
	assert(bind_port);
	addr.sin_addr.s_addr = bind_addr;
	addr.sin_port   = htons(bind_port);
	r = bind(s, (struct sockaddr *)&addr, sizeof(addr));
	assert(!r);
	r = listen(s, 5);
	assert(!r);
	fds[MAX_CONNECTIONS].fd = s;
	fds[MAX_CONNECTIONS].events = POLLIN;

	while (server_running) {
		int have_unread = 0;
		r = poll(fds, MAX_CONNECTIONS + 1, have_unread ? 0 : INFTIM);
		if (r == -1) {
			if (!server_running) return;
			perror("poll");
			continue;
		}
		if (fds[MAX_CONNECTIONS].revents & POLLIN) new_connection();
		for (i = 0; i < MAX_CONNECTIONS; i++) {
			connection_t *conn = connections[i];
			if (!conn) continue;
			if (fds[i].revents & POLLIN) {
				c_read_data(conn);
			}
			if (c_get_line(conn) > 0) {
				char *buf = utf_compose(conn);
				if (buf) {
					client_handle(conn, buf);
					free(buf);
				}
			}
			if (!(conn->flags & CONNFLAG_GOING)) {
				close(fds[i].fd);
				fds[i].fd = -1;
				connection_count--;
				c_cleanup(conn);
				connections[i] = NULL;
			} else {
				if (conn->getlen > conn->getpos) {
					have_unread = 1;
				}
			}
		}
	}
}

/* Pretty much strndup, but without checking for NUL, *
 * and without portability concerns.                  */
static const char *memdup(const char *src, size_t len) {
	char *res = malloc(len + 1);
	memcpy(res, src, len);
	res[len] = '\0';
	return res;
}

static void cfg_parse_list(const char * const **res_list, const char *str)
{
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
		list[word++] = memdup(str, len);
	}
	list[word] = NULL;
	assert(word == words);
	*res_list = list;
}

const char * const *tagtype_names = NULL;
const char * const *rating_names = NULL;
const char * const *filetype_names = NULL;

const char *basedir = NULL;

const guid_t *server_guid = NULL;
static guid_t server_guid_;

md5_t config_md5;
extern uint8_t *MM_BASE_ADDR;
extern unsigned int cache_walk_speed;

void db_read_cfg(const char *filename)
{
	char    buf[1024];
	FILE    *fh;
	MD5_CTX ctx;

	MD5_Init(&ctx);
	fh = fopen(filename, "r");
	if (!fh) {
		fprintf(stderr, "Failed to open config (%s)\n", filename);
		exit(1);
	}
	while (fgets(buf, sizeof(buf), fh)) {
		int len = strlen(buf);
		assert(len && buf[len - 1] == '\n');
		MD5_Update(&ctx, (unsigned char *)buf, len);
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
		} else if (!memcmp("port=", buf, 5)) {
			bind_port = atoi(buf + 5);
		} else if (!memcmp("addr=", buf, 5)) {
			bind_addr = inet_addr(buf + 5);
			assert(bind_addr != INADDR_NONE);
		} else if (!memcmp("mm_base=", buf, 8)) {
			unsigned long long addr = strtoull(buf + 8, NULL, 0);
			MM_BASE_ADDR = (uint8_t *)(intptr_t)addr;
		} else if (!memcmp("cache_walk_speed=", buf, 17)) {
			cache_walk_speed = atoi(buf + 17);
		} else if (!memcmp("timezone=", buf, 9)) {
			const char *end;
			tvp_timezone(buf, &default_timezone, &end);
		} else {
			assert(*buf == '\0' || *buf == '#');
		}
	}
	assert(feof(fh));
	fclose(fh);
	cfg_parse_list(&filetype_names, FILETYPE_NAMES_STR);
	assert(tagtype_names && rating_names && basedir && server_guid);
	MD5_Final(config_md5.m, &ctx);
}
