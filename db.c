#include "db.h"

#include <math.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <sys/un.h>
#include <poll.h>
#include <errno.h>
#include <openssl/md5.h>
#include <bzlib.h>

#ifndef INFTIM
#define INFTIM -1
#endif

ss128_head_t *tags;
ss128_head_t *tagaliases;
ss128_head_t *tagguids;
ss128_head_t *posts;
hash_t       *strings;
post_list_t  *postlist_nodes;

int default_timezone = 0;
int log_version = -1;

static post_node_t *postlist_alloc(void)
{
	post_node_t *pn = post_remhead(postlist_nodes);
	if (!pn) return mm_alloc(sizeof(post_node_t));
	return pn;
}

static int postlist_remove(post_list_t *pl, post_t *post)
{
	post_node_t *pn;

	assert(pl);
	assert(post);

	pn = pl->head;
	while (pn) {
		if (pn->post == post) {
			post_remove(pl, pn);
			post_addhead(postlist_nodes, pn);
			pl->count--;
			return 0;
		}
		pn = pn->succ;
	}
	return 1;
}

static void postlist_add(post_list_t *pl, post_t *post)
{
	assert(pl);
	assert(post);
	pl->count++;
	post_node_t *pn = postlist_alloc();
	pn->post = post;
	post_addtail(pl, pn);
}

static int postlist_contains(const post_list_t *pl, const post_t *post)
{
	const post_node_t *pn = pl->head;
	while (pn) {
		if (pn->post == post) return 1;
		pn = pn->succ;
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
	static int tv_parser_##vtype(const char *val, vtype *v, ftype *f,   \
	                             tagvalue_cmp_t cmp)                    \
	{                                                                   \
		(void) cmp;                                                 \
		char *end;                                                  \
		*v = vfunc(val, &end);                                      \
		*f = 0;                                                     \
		if (val == end) return 1;                                   \
		if (end[0] == '+') {                                        \
			val = end + 1;                                      \
			if (!*val) return 1;                                \
			*f = ffunc(val, &end);                              \
		}                                                           \
		if (*end) {                                                 \
			return 1;                                           \
		}                                                           \
		return 0;                                                   \
	}
#define str2SI(v, e) strtoll(v, e, 10)
#define str2UI(v, e) strtoull(v, e, 16)
double fractod(const char *val, char **r_end)
{
	char *end;
	double n = strtoll(val, &end, 10);
	if (end != val && *end == '/') {
		double d = strtoll(end + 1, &end, 10);
		if (d > 0.0) {
			*r_end = end;
			return n / d;
		}
	}
	return strtod(val, r_end);
}
TAG_VALUE_PARSER(int64_t , str2SI, int64_t, str2SI)
TAG_VALUE_PARSER(uint64_t, str2UI, int64_t, str2SI)
TAG_VALUE_PARSER(double , fractod, double, fractod)

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
	tval->val.v_double = 2.0 * log(fstop) / M_LN2;
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

int tag_value_parse(tag_t *tag, const char *val, tag_value_t *tval, char *buf,
                    tagvalue_cmp_t cmp)
{
	if (!val) return 1;
	if (!*val && tag->valuetype != VT_NONE && cmp == CMP_EQ) {
		tval->v_str = tag_value_null_marker;
		return 0;
	}
	switch (tag->valuetype) {
		case VT_WORD:
			tval->v_str = buf ? val : mm_strdup(val);
			return 0;
			break;
		case VT_STRING:
			tval->v_str = str_enc2str(val, buf);
			if (!tval->v_str) return 1;
			return 0;
			break;
		case VT_INT:
			if (!tv_parser_int64_t(val, &tval->val.v_int,
			                       &tval->fuzz.f_int, cmp)) {
				tval->v_str = 0;
				return 0;
			}
			break;
		case VT_UINT:
			if (!tv_parser_uint64_t(val, &tval->val.v_uint,
			                        &tval->fuzz.f_uint, cmp)) {
				tval->v_str = 0;
				return 0;
			}
			break;
		case VT_FLOAT:
		case VT_F_STOP:
		case VT_STOP:
			if (!tv_parser_double(val, &tval->val.v_double,
			                      &tval->fuzz.f_double, cmp)) {
				if (buf) {
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
			if (!tv_parser_datetime(val, &tval->val.v_datetime,
			                        &tval->fuzz.f_datetime, cmp)) {
				if (buf) {
					tval->v_str = val;
				} else {
					tval->v_str = mm_strdup(val);
				}
				return 0;
			}
			break;
		case VT_GPS:
			if (!tv_parser_gps(val, &tval->val.v_gps,
			                   &tval->fuzz.f_gps)) {
				if (buf) {
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

static int taglist_add(post_taglist_t **tlp, tag_t *tag, tag_value_t *value,
                       alloc_func_t alloc, alloc_data_t *adata)
{
	post_taglist_t *tl = *tlp;
	while (tl) {
		for (int i = 0; i < arraylen(tl->tags); i++) {
			if (!tl->tags[i]) {
				tl->tags[i] = tag;
				tl->values[i] = value;
				return 0;
			}
			if (tl->tags[i] == tag) return 1;
		}
		tl = tl->next;
	}
	tl = alloc(adata, sizeof(*tl));
	tl->tags[0] = tag;
	tl->values[0] = value;
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
	tag_value_t   *i_value;
} implcomp_data_t;
struct impl_iterator_data {
	implcomp_data_t *list;
	int             len;
	truth_t         weak;
	impl_callback_t callback;
	const tag_t     *tag;
	tag_value_t     *tagvalue;
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
	const tagvalue_cmp_t cmp = impl->filter_cmp;
	if (cmp) {
		tv_cmp_t *cmp_f = tv_cmp[data->tag->valuetype];
		const tag_value_t *tval = data->tagvalue;
		if (!tval) {
			tval = tag_value_null;
		}
		if (!cmp_f(tval, cmp, impl->filter_value, NULL)) {
			return;
		}
	}
	if (data->list) {
		data->list[data->len].impl = impl;
		data->list[data->len].weak = data->weak;
		data->list[data->len].i_value = data->tagvalue;
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
				impldata.tag = tag;
				impldata.tagvalue = tl->values[i];
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
				tag_value_t *value = list[i].impl->set_value;
				if (list[i].impl->inherit_value) {
					value = list[i].i_value;
				}
				taglist_add(&res[list[i].weak],
				            list[i].impl->tag, value,
				            alloc, adata);
			}
		}
		free(list);
	}
}

// Mostly the same thing as post_tag()
static int post_tag_set_value(post_t *post, const tag_t *tag, truth_t weak,
                              tag_value_t *value)
{
	assert(post);
	assert(tag);
	post_taglist_t *tl = (weak ? post->weak_tags : &post->tags);
	while (tl) {
		unsigned int i;
		for (i = 0; i < arraylen(tl->tags); i++) {
			if (tl->tags[i] == tag) {
				if (tl->values[i] == value) {
					return 0;
				} else {
					tl->values[i] = value;
					return 1;
				}
			}
		}
		tl = tl->next;
	}
	return 0;
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
			if (!tag) continue;
			int update_value = 0;
			if (taglist_contains(*old, tag)) {
				update_value = 1;
			} else {
				if (!post_has_tag(post, tag, T_DONTCARE)) {
					post_tag_add_i(post, tag, weak, 1, NULL);
					taglist_add(old, tag, NULL, alloc_mm, NULL);
					changed = 1;
					update_value = 1;
				} else {
					tl->tags[i] = NULL;
				}
			}
			if (update_value) {
				changed |= post_tag_set_value(post, tag, weak,
				                              tl->values[i]);
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

static void post_recompute_implications_iter(post_node_t *pn, void *data)
{
	(void) data;
	post_recompute_implications(pn->post);
}

static void postlist_recompute_implications(post_list_t *pl)
{
	post_iterate(pl, NULL, post_recompute_implications_iter);
}

static int impl_eq(const implication_t *a, const implication_t *b, int poscare)
{
	if (a->tag != b->tag) return 0;
	if (poscare && a->positive != b->positive) return 0;
	if (a->filter_value || b->filter_value) {
		if (!a->filter_value || !b->filter_value) return 0;
		if (a->filter_cmp != b->filter_cmp) return 0;
		const valuetype_t vt = a->tag->valuetype;
		if (vt == VT_STRING || vt == VT_WORD) {
			const char *as = a->filter_value->v_str;
			const char *bs = b->filter_value->v_str;
			return (!strcmp(as, bs));
		} else {
			tag_value_t av, bv;
			av = *a->filter_value;
			bv = *b->filter_value;
			if ((av.v_str == tag_value_null_marker
			     || bv.v_str == tag_value_null_marker)
			    && av.v_str != bv.v_str) {
				// One (but not both) are null values
				return 0;
			}
			av.v_str = bv.v_str = NULL;
			// It's not generally valid to compare tag values
			// like this, but the implication parser clears them
			// before parsing, so it should be safe here.
			// (Not if you take a strict C standard view, I think.)
			if (memcmp(&av, &bv, sizeof(av))) return 0;
		}
	}
	return 1;
}

int tag_add_implication(tag_t *from, const implication_t *impl)
{
	impllist_t *tl = from->implications;
	int done = 0;
	
	while (tl) {
		for (int i = 0; i < arraylen(tl->impl); i++) {
			int eq = impl_eq(&tl->impl[i], impl, 0);
			if ((!tl->impl[i].tag || eq) && !done) {
				tl->impl[i] = *impl;
				done = 1;
			} else if (eq) {
				tl->impl[i].tag = NULL;
				// Can leak tag values
			}
		}
		tl = tl->next;
	}
	if (!done) {
		tl = mm_alloc(sizeof(*tl));
		tl->impl[0] = *impl;
		tl->next = from->implications;
		from->implications = tl;
	}
	postlist_recompute_implications(&from->posts);
	postlist_recompute_implications(&from->weak_posts);
	return 0;
}

int tag_rem_implication(tag_t *from, const implication_t *impl)
{
	impllist_t *tl = from->implications;
	
	while (tl) {
		for (int i = 0; i < arraylen(tl->impl); i++) {
			if (impl_eq(impl, &tl->impl[i], 1)) {
				tl->impl[i].tag = NULL;
				// Can leak tag values
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
			if (!weak && !tval) return 0;
		} else if (taglist_contains(post->implied_weak_tags, tag)) {
			int r = taglist_remove(post->implied_weak_tags, tag);
			assert(!r);
			if (weak && !tval) return 0;
		}
	}
	if (post_has_tag(post, tag, weak)) {
		if (tval) {
			if (tval->v_str == tag_value_null_marker) {
				tval = 0;
			} else {
				tval = mm_dup(tval, sizeof(*tval));
			}
			int r = post_tag_set_value(post, tag, weak, tval);
			assert(r || !tval);
			return 0;
		} else {
			return 1;
		}
	}
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
                              tag_value_t *value, char *buf)
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
				if (tag->valuetype != VT_STRING
				    && tag->valuetype != VT_WORD) return NULL;
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
	if (tag_value_parse(tag, v + 1, value, buf, *r_cmp)) return NULL;
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

int post_set_md5(post_t *post, const char *md5str)
{
	md5_t md5;
	void *other_post;
	assert(post);
	assert(md5str);
	if (md5_str2md5(&md5, md5str)) return 1;
	if (!ss128_find(posts, &other_post, md5.key)) return 1;
	int r = ss128_delete(posts, post->md5.key);
	assert(!r);
	post->md5 = md5;
	r = ss128_insert(posts, post, post->md5.key);
	assert(!r);
	return 0;
}

void post_modify(post_t *post, time_t now)
{
	tag_value_t tval;
	tag_value_t *tval_p = post_tag_value(post, magic_tag_modified);
	if (!tval_p) {
		memset(&tval, 0, sizeof(tval));
		tval_p = &tval;
	}
	if (datetime_get_simple(&tval_p->val.v_datetime) != now) {
		datetime_set_simple(&tval_p->val.v_datetime, now);
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
			assert(blen > 2);
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

void internal_fixups0(void)
{
	char a[] = "ATGaaaaaa-aaaads-faketg-create Vdatetime Ncreated";
	char b[] = "ATGaaaaaa-aaaaas-faketg-chaage Vdatetime Nmodified";
	char c[] = "ATGaaaaaa-aaaac8-faketg-bddate Vdatetime Nimgdate";
	char d[] = "ATGaaaaaa-aaaaeL-faketg-bbredd Vuint Nwidth";
	char e[] = "ATGaaaaaa-aaaaf9-faketg-heyght Vuint Nheight";
	char f[] = "ATGaaaaaa-aaaacr-faketg-FLekst Vword Next";
	char g[] = "ATGaaaaaa-aaaade-faketg-rotate Vint Nrotate";
	char *fixup[] = {a, b, c, d, e, f, g, NULL};
	for (int i = 0; fixup[i]; i++) {
		int r = populate_from_log_line(fixup[i]);
		assert(!r);
	}
}

void internal_fixups1(void)
{
	char h[] = "ATGaaaaaa-aaaadt-faketg-gpspos Vgps Ngps";
	int r = populate_from_log_line(h);
	assert(!r);
}

static int first_log = 1;
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
		if (*buf == 'T') { // New transaction
			if (*end == 'O') { // Complete transaction
				int trans_pos = find_trans(trans, tid);
				assert(trans_pos == -1);
				trans_pos = find_trans(trans, 0);
				assert(trans_pos != -1);
				trans[trans_pos] = tid;
				int trans_version = end[1] - '0';
				err1(trans_version < log_version);
				err1(trans_version > LOG_VERSION);
				if (first_log
				    && trans_version >= 0
				    && log_version < 0
				   ) {
					if (trans_version < 1) {
						apply_fixups(0);
					} else {
						internal_fixups0();
					}
					if (trans_version < 3) {
						apply_fixups(1);
					} else {
						internal_fixups1();
					}
					after_fixups();
				}
				first_log = 0;
				if (log_version != trans_version) {
					log_version = trans_version;
				}
				if (log_version >= 2) err1(end[2] != 'T');
				transnow[trans_pos] = strtoull(end + 3, &end, 16);
				if (log_version < 2) err1(end != buf + 34);
				err1(*end);
			} else if (*end == 'U') { // Unfinished transaction
				// Do nothing
			} else { // What?
				goto err;
			}
		} else if (*buf == 'D') { // Data from transaction
			err1(len <= (end - buf) + 1);
			err1(*end != ' ');
			int trans_pos = find_trans(trans, tid);
			if (trans_pos >= 0) {
				logconn->trans.now = transnow[trans_pos];
				err1(populate_from_log_line(end + 1));
			} else {
				printf("Skipping data from incomplete transaction: %s\n", buf);
			}
		} else if (*buf == 'E') { // End of transaction
			int pos;
			if (log_version < 2) err1(len != 17);
			err1(*end);
			pos = find_trans(trans, tid);
			if (pos != -1) {
				trans[pos] = 0;
			} else {
				printf("Stray trans end %llx on line %ld.\n",
				       ULL tid, line_nr);
			}
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

static int bind_port = 0;
static in_addr_t bind_addr = 0;
static char *socket_path = 0;

static int bind_inet(void)
{
	int s, r, one;
	struct sockaddr_in addr;

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
	addr.sin_addr.s_addr = bind_addr;
	addr.sin_port   = htons(bind_port);
	r = bind(s, (struct sockaddr *)&addr, sizeof(addr));
	if (r) {
		perror("bind");
		fprintf(stderr, "Failed to bind to %s:%d\n",
		        inet_ntoa(addr.sin_addr), bind_port);
		exit(1);
	}
	return s;
}

static void rm_socket(void)
{
	if (unlink(socket_path)) {
		perror("unlink");
	}
}

static int init_unix_socket(struct sockaddr_un *addr)
{
	int s;

	if (strlen(socket_path) >= sizeof(addr->sun_path)) {
		fprintf(stderr, "Socket path %s too long\n", socket_path);
		exit(1);
	}
	memset(addr, 0, sizeof(*addr));
	strcpy(addr->sun_path, socket_path);
	addr->sun_family = AF_UNIX;
	s = socket(PF_UNIX, SOCK_STREAM, 0);
	assert(s >= 0);
	return s;
}

static void pre_cleanup_unix(void)
{
	struct stat sb;
	struct sockaddr_un addr;
	int s;

	if (lstat(socket_path, &sb)) {
		if (errno == ENOENT) return;
		perror("lstat socket");
		exit(1);
	}
	if ((sb.st_mode & S_IFMT) != S_IFSOCK) {
		fprintf(stderr, "Socket path %s exists as non-socket\n", socket_path);
		exit(1);
	}
	s = init_unix_socket(&addr);
	if (!connect(s, &addr, sizeof(addr))) {
		fprintf(stderr, "Something is already listening to %s\n", socket_path);
		exit(1);
	}
	if (errno == ECONNREFUSED) {
		rm_socket();
		return;
	}
	perror("testing socket connect");
	exit(1);
}

static int bind_unix(void)
{
	int s, r;
	struct sockaddr_un addr;

	pre_cleanup_unix();
	s = init_unix_socket(&addr);
	r = bind(s, (struct sockaddr *)&addr, sizeof(addr));
	if (r) {
		perror("bind");
		fprintf(stderr, "Failed to bind to %s\n", socket_path);
		exit(1);
	}
	atexit(rm_socket);
	return s;
}

void db_serve(void)
{
	int s, r, i;

	for (i = 0; i < MAX_CONNECTIONS; i++) {
		fds[i].fd = -1;
		fds[i].events = POLLIN;
		connections[i] = NULL;
	}

	if (bind_port) {
		s = bind_inet();
	} else {
		s = bind_unix();
	}
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
				char *buf = utf_compose(conn, conn->linebuf, 0);
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

extern transflag_t transflags_default;

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
		} else if (!memcmp("filetypes=", buf, 10)) {
			cfg_parse_list(&filetype_names, buf + 10);
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
		} else if (!memcmp("socket=", buf, 7)) {
			socket_path = strdup(buf + 7);
			assert(socket_path);
		} else if (!memcmp("mm_base=", buf, 8)) {
			unsigned long long addr = strtoull(buf + 8, NULL, 0);
			MM_BASE_ADDR = (uint8_t *)(intptr_t)addr;
		} else if (!memcmp("cache_walk_speed=", buf, 17)) {
			cache_walk_speed = atoi(buf + 17);
		} else if (!memcmp("timezone=", buf, 9)) {
			int tlen = strlen(buf + 9);
			tvp_timezone(buf + 9, &tlen, &default_timezone);
		} else if (!memcmp("fsync_logfile=", buf, 14)) {
			char *endptr;
			long fsync_logfile = strtol(buf + 14, &endptr, 0);
			assert(!*endptr);
			if (!fsync_logfile) transflags_default = 0;
		} else {
			assert(*buf == '\0' || *buf == '#');
		}
	}
	assert(feof(fh));
	fclose(fh);
	assert(tagtype_names && rating_names && basedir && server_guid && filetype_names);
	MD5_Final(config_md5.m, &ctx);
	if (socket_path && *socket_path != '/') {
		int r = asprintf(&socket_path, "%s/%s", basedir, socket_path);
		assert(r > 0);
	}
	if (socket_path && (bind_port || bind_addr)) {
		fprintf(stderr, "Only specify one of socket or addr/port in config\n");
		exit(1);
	}
}
