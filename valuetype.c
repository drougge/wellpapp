#include "db.h"

#define U2S (to_value < 0)
#define S2U (from_value < 0)
#define CEQ (from_value != to_value)

#define MAKE_CONVERTER(ff, f_vt, tf, t_vt, chk)               \
	static int conv_##ff##tf(tag_value_t *val, int dummy) \
	{                                                     \
		const f_vt from_value = val->val.v_##ff;      \
		const t_vt to_value = from_value;             \
		if (chk) return 1;                            \
		if (!dummy) {                                 \
			val->val.v_##tf = to_value;           \
			val->fuzz.f_##tf = val->fuzz.f_##ff;  \
		}                                             \
		return 0;                                     \
	}

MAKE_CONVERTER(int, int64_t, uint, uint64_t, S2U)
MAKE_CONVERTER(uint, uint64_t, int, int64_t, U2S)
MAKE_CONVERTER(int, int64_t, double, double, CEQ)
MAKE_CONVERTER(uint, uint64_t, double, double, CEQ)
MAKE_CONVERTER(double, double, int, int64_t, CEQ)
MAKE_CONVERTER(double, double, uint, uint64_t, CEQ)

typedef int (conv_func_t)(tag_value_t *, int);

static conv_func_t * const conv_int[] = {
	NULL, NULL, NULL,
	NULL,
	conv_intuint,
	conv_intdouble,
	NULL, NULL, NULL
};
static conv_func_t * const conv_uint[] = {
	NULL, NULL, NULL,
	conv_uintint,
	NULL,
	conv_uintdouble,
	NULL, NULL, NULL
};
static conv_func_t * const conv_double[] = {
	NULL, NULL, NULL,
	conv_doubleint,
	conv_doubleuint,
	NULL,
	NULL, NULL, NULL
};
static conv_func_t * const * const convs[] = {
	NULL, NULL, NULL,
	conv_int,
	conv_uint,
	conv_double,
	NULL, NULL, NULL
};

typedef struct check_data {
	const tag_t *tag;
	conv_func_t *conv;
	int         bad;
} check_data_t;

static void check_cb(post_node_t *node, void *data_)
{
	check_data_t *data = data_;
	if (data->bad) return;
	tag_value_t *val = post_tag_value(node->post, data->tag);
	if (!val) return;
	data->bad = data->conv(val, 1);
}

static void change_cb(post_node_t *node, void *data_)
{
	check_data_t *data = data_;
	tag_value_t *val = post_tag_value(node->post, data->tag);
	if (!val) return;
	int r = data->conv(val, 1);
	assert(!r);
}

int tag_check_vt_change(tag_t *tag, valuetype_t vt)
{
	static_assert(arraylen(conv_int   ) == VT_MAX, "All value types");
	static_assert(arraylen(conv_uint  ) == VT_MAX, "All value types");
	static_assert(arraylen(conv_double) == VT_MAX, "All value types");
	static_assert(arraylen(convs      ) == VT_MAX, "All value types");
	assert(tag->valuetype < VT_MAX && vt < VT_MAX);
	if (!tag->valuetype) return 0;
	if (!convs[tag->valuetype]) return 1;
	check_data_t data;
	data.conv = convs[tag->valuetype][vt];
	if (!data.conv) return 1;
	data.tag = tag;
	data.bad = 0;
	post_iterate(&tag->posts, &data, check_cb);
	post_iterate(&tag->weak_posts, &data, check_cb);
	if (data.bad) return 1;
	post_iterate(&tag->posts, &data, change_cb);
	post_iterate(&tag->weak_posts, &data, change_cb);
	return 0;
}
