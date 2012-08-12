#include "db.h"

#include <regex.h>

void result_free(connection_t *conn, result_t *result)
{
	if (result->posts) {
		c_free(conn, result->posts, result->room * sizeof(post_t *));
	}
}

int result_add_post(connection_t *conn, result_t *result, post_t *post)
{
	if (result->room == result->of_posts) {
		unsigned int old_size;
		if (result->room == 0) {
			old_size = 0;
			result->room = 64;
		} else {
			old_size = result->room * sizeof(post_t *);
			result->room *= 2;
		}
		int r;
		result->posts = c_realloc(conn, result->posts, old_size,
		                          result->room * sizeof(post_t *), &r);
		if (r) return 1;
	}
	result->posts[result->of_posts] = post;
	result->of_posts++;
	return 0;
}

// The comparison functions never get called with LT/LE.
// They return !0 for "match".

static int tvc_none(tag_value_t *a, tagvalue_cmp_t cmp, tag_value_t *b,
                    regex_t *re)
{
	(void) a;
	(void) cmp;
	(void) b;
	(void) re;
	return 0;
}

static int tvc_string(tag_value_t *a, tagvalue_cmp_t cmp, tag_value_t *b,
                      regex_t *re)
{
	if (cmp == CMP_REGEXP) {
		return !regexec(re, a->v_str, 0, NULL, 0);
	} else {
		int eq = strcmp(a->v_str, b->v_str);
		switch (cmp) {
			case CMP_EQ:
				return !eq;
				break;
			case CMP_GT:
				return eq > 0;
				break;
			case CMP_GE:
				return eq >= 0;
				break;
			default:
				return 0;
				break;
		}
	}
}

#define TVC_NUM(t, ft, ff, n)                                                  \
	static int tvc_##n(tag_value_t *a, tagvalue_cmp_t cmp, tag_value_t *b, \
	                   regex_t *re)                                        \
	{                                                                      \
		(void) re;                                                     \
		ft a_fuzz = a->fuzz.f_##n;                                     \
		if (a_fuzz) a_fuzz += ff;                                      \
		ft b_fuzz = b->fuzz.f_##n;                                     \
		if (b_fuzz) b_fuzz += ff;                                      \
		t a_low  = a->val.v_##n - a_fuzz;                              \
		t a_high = a->val.v_##n + a_fuzz;                              \
		t b_low  = b->val.v_##n - b_fuzz;                              \
		t b_high = b->val.v_##n + b_fuzz;                              \
		switch (cmp) {                                                 \
			case CMP_EQ:                                           \
				return a_low <= b_high && b_low <= a_high;     \
				break;                                         \
			case CMP_GT:                                           \
				return a_high > b_low;                         \
				break;                                         \
			case CMP_GE:                                           \
				return a_high >= b_low;                        \
				break;                                         \
			default:                                               \
				return 0;                                      \
				break;                                         \
		}                                                              \
	}
TVC_NUM(int64_t , uint64_t, 0   , int)
TVC_NUM(uint64_t, uint64_t, 0   , uint)
TVC_NUM(double  , double  , 0.07, double)

typedef int (tv_cmp_t)(tag_value_t *, tagvalue_cmp_t, tag_value_t *, regex_t *);
tv_cmp_t *tv_cmp[] = {tvc_none, // NONE
                      tvc_string, // STRING
                      tvc_int, // INT
                      tvc_uint, // UINT
                      tvc_double, // FLOAT
                      tvc_double, // F_STOP
                      tvc_double, // ISO
                     };

static int result_add_post_if(connection_t *conn, result_t *result,
                              post_t *post, search_tag_t *t, regex_t *re)
{
	tagvalue_cmp_t cmp = t->cmp;
	if (!cmp) return result_add_post(conn, result, post);
	tag_value_t *pval = post_tag_value(post, t->tag);
	if (!pval) return 0;
	tag_value_t *a, *b;
	if (cmp == CMP_LT || cmp == CMP_LE) {
		cmp = (cmp == CMP_LT) ? CMP_GT : CMP_GE;
		a = &t->val;
		b = pval;
	} else {
		a = pval;
		b = &t->val;
	}
	if (!tv_cmp[t->tag->valuetype](a, cmp, b, re)) return 0;
	return result_add_post(conn, result, post);
}

// @@ do values mean anything useful here? (otherwise forbid them)
int result_remove_tag(connection_t *conn, result_t *result, search_tag_t *t)
{
	tag_t    *tag = t->tag;
	truth_t  weak = t->weak;
	result_t new_result;
	uint32_t i;

	memset(&new_result, 0, sizeof(new_result));
	for (i = 0; i < result->of_posts; i++) {
		post_t *post = result->posts[i];
		if (!post_has_tag(post, tag, weak)) {
			if (result_add_post(conn, &new_result, post)) return 1;
		}
	}
	result_free(conn, result);
	*result = new_result;
	return 0;
}

int result_intersect(connection_t *conn, result_t *result, search_tag_t *t)
{
	tag_t    *tag = t->tag;
	truth_t  weak = t->weak;
	result_t new_result;
	regex_t  re;

	memset(&new_result, 0, sizeof(new_result));
	if (t->cmp == CMP_REGEXP) {
		if (regcomp(&re, t->val.v_str, REG_EXTENDED | REG_NOSUB)) {
			return 1;
		}
	}
	if (result->of_posts) {
		uint32_t i;
		for (i = 0; i < result->of_posts; i++) {
			post_t *post = result->posts[i];
			if (post_has_tag(post, tag, weak)) {
				err1(result_add_post_if(conn, &new_result, post, t, &re));
			}
		}
	} else {
		postlist_node_t *pn;
again:
		if (weak) {
			pn = tag->weak_posts.h.p.head;
		} else {
			pn = tag->posts.h.p.head;
		}
		while (pn->n.p.succ) {
			err1(result_add_post_if(conn, &new_result, pn->post, t, &re));
			pn = pn->n.p.succ;
		}
		if (weak == T_DONTCARE) {
			weak = T_NO;
			goto again;
		}
	}
	result_free(conn, result);
	if (t->cmp == CMP_REGEXP) regfree(&re);
	*result = new_result;
	return 0;
err:
	if (t->cmp == CMP_REGEXP) regfree(&re);
	result_free(conn, &new_result);
	return 1;
}
