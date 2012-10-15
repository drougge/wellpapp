#include "db.h"

#include <errno.h>

static int tag_post_cmd(connection_t *conn, char *cmd, void *post_,
                        prot_cmd_flag_t flags)
{
	post_t     **post = post_;
	const char *args = cmd + 1;

	(void)flags;

	switch (*cmd) {
		case 'P': // Which post
			if (*post) return conn->error(conn, cmd);
			post_find_md5str(post, args);
			if (!*post) return conn->error(conn, cmd);
			log_set_init(&conn->trans, "TP%s", args);
			break;
		case 'T': // Add tag
		case 't': // Remove tag
			if (!*post) return conn->error(conn, cmd);
			truth_t weak = T_NO;
			if (*args == '~' && *cmd == 'T') { // Weak tag
				args++;
				weak = T_YES;
			}
			tag_value_t    val;
			tagvalue_cmp_t cmp;
			tag_t *tag = tag_find_guidstr_value(args, &cmp, &val, 0);
			if (tag && *cmd == 'T' && tag->unsettable) tag = NULL;
			if (tag && *cmd == 't' && tag->datatag) tag = NULL;
			if (!tag) return conn->error(conn, cmd);
			if (cmp && cmp != CMP_EQ) return conn->error(conn, cmd);
			if (*cmd == 'T') {
				tag_value_t *val_p = cmp ? &val : NULL;
				int r = post_tag_add(*post, tag, weak, val_p);
				if (r) return conn->error(conn, cmd);
			} else {
				if (cmp) return conn->error(conn, cmd);
				int r = post_tag_rem(*post, tag);
				if (r) return conn->error(conn, cmd);
			}
			log_write(&conn->trans, "%s", cmd);
			break;
		default:
			return conn->error(conn, cmd);
			break;
	}
	if (*cmd != 'P' && *post) post_modify(*post, conn->trans.now);
	return 0;
}

int prot_cmd_loop(connection_t *conn, char *cmd, void *data,
                  prot_cmd_func_t func, prot_cmd_flag_t flags)
{
	while (*cmd) {
		int  len = 0;
		while (cmd[len] && cmd[len] != ' ') len++;
		if (cmd[len]) {
			cmd[len] = 0;
			len++;
		}
		if (!cmd[len]) flags |= CMDFLAG_LAST;
		if (func(conn, cmd, data, flags)) return 1;
		cmd += len;
	}
	return 0;
}

int prot_tag_post(connection_t *conn, char *cmd)
{
	post_t *post = NULL;
	return prot_cmd_loop(conn, cmd, &post, tag_post_cmd, CMDFLAG_NONE);
}

static int error1(connection_t *conn, char *cmd)
{
	int len = 0;
	while (cmd[len] && cmd[len] != ' ') len++;
	cmd[len] = 0;
	return conn->error(conn, cmd);
}

static int put_enum_value_gen(uint16_t *res, const char * const *array,
                              const char *val)
{
	uint16_t i;
	for (i = 0; array[i]; i++) {
		if (!strcmp(array[i], val)) {
			*res = i;
			return 0;
		}
	}
	return 1;
}

static void tag_delete(tag_t *tag)
{
	ss128_key_t key = ss128_str2key(tag->name);
	int r = ss128_delete(tags, key);
	assert(!r);
	r = ss128_delete(tagguids, tag->guid.key);
	assert(!r);
}

static void post_delete(post_t *post)
{
	int r = ss128_delete(posts, post->md5.key);
	assert(!r);
	// @@ We could reuse the post, but for now we just leak it.
}

typedef struct mergedata {
	tag_t   *tag;
	tag_t   *rmtag;
	truth_t weak;
	int     bad;
} mergedata_t;

static void merge_tags_chk_cb(post_node_t *ln, void *data_)
{
	mergedata_t *data = data_;
	data->bad |= post_has_tag(ln->post, data->tag, data->weak);
}

// @@todo valued tags
static void merge_tags_cb(post_node_t *ln, void *data_)
{
	mergedata_t *data = data_;
	post_t *post = ln->post;
	if (!post_has_tag(post, data->tag, data->weak)
	    || taglist_contains(post->implied_tags, data->tag)
	    || taglist_contains(post->implied_weak_tags, data->tag)
	   ) {
		int r = post_tag_add(post, data->tag, data->weak, NULL);
		assert(!r);
	}
	int r = post_tag_rem(post, data->rmtag);
	assert(!r);
}

typedef struct mergedata_alias {
	tag_t *from;
	tag_t *into;
} mergedata_alias_t;

static void merge_tags_realias(ss128_key_t key, ss128_value_t value,
                               void *data_)
{
	(void) key;
	tagalias_t *tagalias = (tagalias_t *)value;
	mergedata_alias_t *data = data_;
	if (tagalias->tag == data->from) tagalias->tag = data->into;
}

static void merge_tags_chk_impl(ss128_key_t key, ss128_value_t value,
                                void *data_)
{
	(void) key;
	tag_t *tag = (tag_t *)value;
	mergedata_t *data = data_;
	impllist_t *impl = tag->implications;
	while (impl) {
		for (int i = 0; i < arraylen(impl->impl); i++) {
			if (impl->impl[i].tag == data->tag) data->bad = 1;
		}
		impl = impl->next;
	}
}

static int merge_tags(connection_t *conn, tag_t *into, tag_t *from)
{
	(void)conn;
	if (!into || !from) return 1;
	if (from->datatag || into->datatag) return 1;
	mergedata_t data;
	data.bad  = 0;
	data.tag  = from;
	data.weak = T_YES;
	post_iterate(&into->posts, &data, merge_tags_chk_cb);
	data.weak = T_NO;
	post_iterate(&into->weak_posts, &data, merge_tags_chk_cb);
	data.tag  = into;
	data.weak = T_YES;
	post_iterate(&from->posts, &data, merge_tags_chk_cb);
	data.weak = T_NO;
	post_iterate(&from->weak_posts, &data, merge_tags_chk_cb);
	data.tag = from;
	ss128_iterate(tags, merge_tags_chk_impl, &data);
	if (data.bad) return 1;
	// Ok, there are no conflicts.

	data.tag   = into;
	data.rmtag = from;
	data.weak  = T_NO;
	post_iterate(&from->posts, &data, merge_tags_cb);
	data.weak  = T_YES;
	post_iterate(&from->weak_posts, &data, merge_tags_cb);
	// into now has all the posts from from.

	// create from->name as alias for into.
	tagalias_t *tagalias;
	tagalias = mm_alloc(sizeof(*tagalias));
	tagalias->name = mm_strdup(from->name);
	tagalias->fuzzy_name = mm_strdup(from->fuzzy_name);
	tagalias->tag = into;
	ss128_key_t key = ss128_str2key(tagalias->name);
	int r = ss128_insert(tagaliases, tagalias, key);
	assert(!r);
	// Redirect aliases from from to into
	mergedata_alias_t aliasdata;
	aliasdata.from = from;
	aliasdata.into = into;
	ss128_iterate(tagaliases, merge_tags_realias, &aliasdata);

	tag_delete(from);
	return 0;
}

typedef struct tag_cmd_data {
	tag_t        *tag;
	const char   *name;
	guid_t       merge_guid;
	unsigned int is_add : 1;
	unsigned int merge : 1;
	unsigned int flag_unsettable : 1;
} tag_cmd_data_t;

// needs to match valuetype_t in db.h
const char * const tag_value_types[] = {"none", "word", "string", "int", "uint",
                                        "float", "f-stop", "stop", "datetime",
                                        NULL};

static int tag_cmd(connection_t *conn, char *cmd, void *data_,
                   prot_cmd_flag_t flags)
{
	tag_cmd_data_t *data = data_;
	tag_t      *tag = data->tag;
	int        r = 1;
	const char *args = cmd + 1;
	char       *ptr;
	int         u_value;

	if (!*cmd || !*args) return conn->error(conn, cmd);
	switch (*cmd) {
		case 'G':
			if (data->is_add) {
				r = guid_str2guid(&tag->guid, args,
				                  GUIDTYPE_TAG);
			} else if (!tag) {
				data->tag = tag = tag_find_guidstr(args);
				r = !tag;
			}
			if (r) return conn->error(conn, cmd);
			break;
		case 'N':
			if ((data->is_add || strcmp(tag->name, args))
			    && tag_find_name(args, T_DONTCARE, NULL)
			   ) {
				return conn->error(conn, cmd);
			}
			if (data->is_add) {
				if (tag->name) return conn->error(conn, cmd);
				tag->name = mm_strdup(args);
				tag->fuzzy_name = utf_fuzz_mm(tag->name);
			} else {
				if (data->name) return conn->error(conn, cmd);
				data->name = args;
			}
			break;
		case 'T':
			if (!tag) return conn->error(conn, cmd);
			if (put_enum_value_gen(&tag->type, tagtype_names, args)) {
				return conn->error(conn, cmd);
			}
			break;
		case 'M':
			if (data->is_add || data->merge) {
				return conn->error(conn, cmd);
			}
			tag_t *merge_tag = tag_find_guidstr(args);
			if (!merge_tag) return conn->error(conn, cmd);
			data->merge = 1;
			data->merge_guid = merge_tag->guid;
			if (merge_tags(conn, tag, merge_tag)) {
				return conn->error(conn, cmd);
			}
			break;
		case 'V':
			if (tag->valuetype) {
				if (data->is_add) {
					return conn->error(conn, cmd);
				}
			}
			int vt = str2id(args, tag_value_types);
			if (vt <= 0) return conn->error(conn, cmd);
			valuetype_t real_vt = vt - 1;
			if (tag->valuetype == real_vt) break;
			if (tag_check_vt_change(tag, real_vt)) {
				return conn->error(conn, cmd);
			}
			tag->valuetype = real_vt;
			break;
		case 'F':
			u_value = 1;
			if (*args == '-') {
				u_value = 0;
				args++;
			}
			if (strcmp(args, "unsettable")) {
				return conn->error(conn, cmd);
			}
			tag->unsettable = u_value;
			data->flag_unsettable = 1;
			break;
		default:
			return conn->error(conn, cmd);
	}
	if (flags & CMDFLAG_LAST) {
		ss128_key_t  key;
		if (!tag || !tag->name) {
			return conn->error(conn, cmd);
		}
		key = ss128_str2key(tag->name);
		if (data->is_add) {
			unsigned int i;
			ptr = (char *)&tag->guid;
			for (i = 0; i < sizeof(tag->guid); i++) {
				if (ptr[i]) break;
			}
			if (i == sizeof(tag->guid)) {
				tag->guid = guid_gen_tag_guid();
			} else {
				guid_update_last(tag->guid);
			}
			if (ss128_insert(tags, tag, key)) {
				return conn->error(conn, cmd);
			}
			if (ss128_insert(tagguids, tag, tag->guid.key)) {
				ss128_delete(tags, key);
				return conn->error(conn, cmd);
			}
		} else if (data->name && strcmp(data->name, tag->name)) {
			r = ss128_delete(tags, key);
			assert(!r);
			tag->name = mm_strdup(data->name);
			tag->fuzzy_name = utf_fuzz_mm(tag->name);
			key = ss128_str2key(tag->name);
			r = ss128_insert(tags, tag, key);
			assert(!r);
		}
		guid_t *merge = NULL;
		if (data->merge) merge = &data->merge_guid;
		log_write_tag(&conn->trans, tag, data->is_add,
		              data->flag_unsettable, merge);
	}
	return 0;
}

static int add_alias_cmd(connection_t *conn, char *cmd, void *data,
                         prot_cmd_flag_t flags)
{
	tagalias_t *tagalias = *(tagalias_t **)data;
	const char *args = cmd + 1;

	if (!*cmd || !*args) return conn->error(conn, cmd);
	switch (*cmd) {
		case 'G':
			tagalias->tag = tag_find_guidstr(args);
			break;
		case 'N':
			tagalias->name = mm_strdup(args);
			tagalias->fuzzy_name = utf_fuzz_mm(tagalias->name);
			break;
		default:
			return conn->error(conn, cmd);
	}
	if (flags & CMDFLAG_LAST) {
		ss128_key_t key;
		if (!tagalias->tag || !tagalias->name) {
			return conn->error(conn, cmd);
		}
		key = ss128_str2key(tagalias->name);
		if (!ss128_find(tagaliases, NULL, key)
		    || !ss128_find(tags, NULL, key)
		    || ss128_insert(tagaliases, tagalias, key)
		   ) {
		 	return conn->error(conn, cmd);
		}
		log_write_tagalias(&conn->trans, tagalias);
	}
	return 0;
}

#define POST_FIELD_DEF(str, valuelist, magic_tag, fuzz, version) \
                      {#str, strlen(#str), valuelist, magic_tag, fuzz, version}

const field_t *post_fields = NULL;
const char *magic_tag_guids[] = {"aaaaaa-aaaaeL-faketg-bbredd", // width
                                 "aaaaaa-aaaaf9-faketg-heyght", // height
                                 "aaaaaa-aaaacr-faketg-FLekst", // ext
                                 "aaaaaa-aaaads-faketg-create", // created
                                 "aaaaaa-aaaac8-faketg-bddate", // imgdate
                                 "aaaaaa-aaaade-faketg-rotate", // rotate
                                 "aaaaaa-aaaaas-faketg-chaage", // modified
                                 "aaaaaa-aaaaeQ-faketg-pscore", // score
                                 "aaaaaa-aaaacc-faketg-soorce", // source
                                 "aaaaaa-aaaac9-faketg-pTYTLE", // title
                                 NULL};
tag_t *magic_tag[10] = {0};

int prot_init(void) {
	field_t post_fields_[] = {
		POST_FIELD_DEF(width          , NULL           , &magic_tag[0], 0, LOG_VERSION),
		POST_FIELD_DEF(height         , NULL           , &magic_tag[1], 0, LOG_VERSION),
		POST_FIELD_DEF(created        , NULL           , &magic_tag[3], 0, LOG_VERSION),
		POST_FIELD_DEF(modified       , NULL           , &magic_tag[6], 0, LOG_VERSION),
		POST_FIELD_DEF(rotate         , NULL           , &magic_tag[5], 0, LOG_VERSION),
		POST_FIELD_DEF(ext            , &filetype_names, &magic_tag[2], 0, LOG_VERSION),
		POST_FIELD_DEF(imgdate        , NULL           , &magic_tag[4], 0, LOG_VERSION),
		POST_FIELD_DEF(image_date     , NULL           , &magic_tag[4], 0, 0),
		POST_FIELD_DEF(image_date_fuzz, NULL           , &magic_tag[4], 1, 0),
		POST_FIELD_DEF(imgdate_fuzz   , NULL           , &magic_tag[4], 1, 0),
		POST_FIELD_DEF(source         , NULL           , &magic_tag[8], 0, 0),
		POST_FIELD_DEF(title          , NULL           , &magic_tag[9], 0, 0),
		POST_FIELD_DEF(filetype       , &filetype_names, &magic_tag[2], 0, 0),
		POST_FIELD_DEF(score          , NULL           , &magic_tag[7], 0, 0),
		POST_FIELD_DEF(rating         , &rating_names  , NULL         , 0, 0),
		{NULL, 0, NULL, NULL, 0, 0}
	};
	void *mem = malloc(sizeof(post_fields_));
	if (!mem) return 1;
	memcpy(mem, post_fields_, sizeof(post_fields_));
	post_fields = mem;
	return 0;
}

static int do_magic_tag(post_t *post, tag_t *tag, const char *valp,
                        const field_t *field)
{
	tag_value_t tval;
	tag_value_t *tval_p = post_tag_value(post, tag);
	int add = 0;
	if (!tval_p) {
		memset(&tval, 0, sizeof(tval));
		tval_p = &tval;
		add = 1;
	}
	if (field->is_fuzz) {
		long long v = strtoull(valp, NULL, 16);
		if (tval_p->fuzz.f_datetime.d_fuzz != -v) {
			tval_p->fuzz.f_datetime.d_fuzz = -v;
			datetime_strfix(tval_p);
		} else {
			add = 0;
		}
	} else {
		if (field->valuelist) {
			uint16_t i;
			if (put_enum_value_gen(&i, *field->valuelist, valp)) {
				return 1;
			}
			tval_p->v_str = mm_strdup((*field->valuelist)[i]);
		}
		if (log_version < 1 && tag->valuetype == VT_DATETIME) {
			long long v = strtoull(valp, NULL, 16);
			if (v || v != datetime_get_simple(&tval_p->val.v_datetime)) {
				datetime_set_simple(&tval_p->val.v_datetime, v);
				datetime_strfix(tval_p);
			} else {
				add = 0;
			}
		} else if (!field->valuelist) {
			if (tag_value_parse(tag, valp, tval_p, 0, 0)) return 1;
		}
	}
	if (tag == magic_tag_rotate) {
		if (tval_p->val.v_int == -1) { // Magic "unknown" value
			if (!add) post_tag_rem(post, tag);
			add = 0;
		}
	}
	if (add) post_tag_add(post, tag, T_NO, &tval);
	return 0;
}

static int put_in_post_field(post_t *post, const char *str, unsigned int nlen)
{
	const field_t *field = post_fields;
	while (field->name && field->log_version >= log_version) {
		if (field->namelen == nlen && !memcmp(field->name, str, nlen)) {
			const char *valp = str + nlen + 1;
			tag_t * const *tag = field->magic_tag;
			if (!*valp) return 1;
			if (!tag || !*tag) return 0;
			return do_magic_tag(post, *tag, valp, field);
		}
		field++;
	}
	return 1;
}

static int post_cmd(connection_t *conn, char *cmd, void *data,
                    prot_cmd_flag_t flags)
{
	post_t     *post = *(post_t **)data;
	const char *eqp;

	eqp = strchr(cmd, '=');
	if (eqp) {
		unsigned int len = eqp - cmd;
		if (!post) return conn->error(conn, cmd);
		if (put_in_post_field(post, cmd, len)) {
			return conn->error(conn, cmd);
		}
		post_modify(post, conn->trans.now);
		if (flags & CMDFLAG_MODIFY) log_write(&conn->trans, "%s", cmd);
	} else { // This is the md5
		if (flags & CMDFLAG_MODIFY) {
			post_t **postp = data;
			int r = post_find_md5str(&post, cmd);
			if (r) return conn->error(conn, cmd);
			if (*postp) return conn->error(conn, cmd);
			*postp = post;
			log_set_init(&conn->trans, "MP%s", cmd);
		} else {
			int r = md5_str2md5(&post->md5, cmd);
			if (r) return conn->error(conn, cmd);
		}
	}
	if ((flags & CMDFLAG_LAST) && !(flags & CMDFLAG_MODIFY)) {
		int r;
		md5_t null_md5;
		memset(&null_md5, 0, sizeof(md5_t));
		if (!memcmp(&post->md5, &null_md5, sizeof(md5_t))) {
			return conn->error(conn, cmd);
		}
		for (int i = 0; i < MANDATORY_MAGIC_TAGS; i++) {
			if (!post_has_tag(post, magic_tag[i], T_NO)) {
				return conn->error(conn, cmd);
			}
		}
		r = ss128_insert(posts, post, post->md5.key);
		if (r) {
			return conn->error(conn, cmd);
		}
		log_write_post(&conn->trans, post);
	}
	return 0;
}

int prot_add(connection_t *conn, char *cmd)
{
	prot_cmd_func_t func;
	void *data = NULL;
	void *dataptr = &data;
	tag_cmd_data_t tag_data;
	int r;

	switch (*cmd) {
		case 'T':
			func = tag_cmd;
			memset(&tag_data, 0, sizeof(tag_data));
			tag_data.tag = mm_alloc(sizeof(tag_t));
			post_newlist(&tag_data.tag->posts);
			post_newlist(&tag_data.tag->weak_posts);
			tag_data.is_add = 1;
			dataptr = &tag_data;
			break;
		case 'A':
			func = add_alias_cmd;
			data = mm_alloc(sizeof(tagalias_t));
			break;
		case 'P':
			func = post_cmd;
			data = mm_alloc(sizeof(post_t));
			post_t *post = data;
			tag_value_t val;
			memset(&val, 0, sizeof(val));
			datetime_set_simple(&val.val.v_datetime, conn->trans.now);
			datetime_strfix(&val);
			r = post_tag_add(post, magic_tag_created, T_NO, &val);
			assert(!r);
			post_newlist(&post->related_posts);
			break;
		default:
			return error1(conn, cmd);
	}
	return prot_cmd_loop(conn, cmd + 1, dataptr, func, CMDFLAG_NONE);
}

int prot_modify(connection_t *conn, char *cmd)
{
	prot_cmd_func_t func;
	void *data = NULL;
	void *dataptr = &data;
	tag_cmd_data_t tag_data;

	switch (*cmd) {
		case 'P':
			func = post_cmd;
			break;
		case 'T':
			memset(&tag_data, 0, sizeof(tag_data));
			dataptr = &tag_data;
			func = tag_cmd;
			break;
		default:
			return error1(conn, cmd);
	}
	return prot_cmd_loop(conn, cmd + 1, dataptr, func, CMDFLAG_MODIFY);
}

typedef struct freetag_data {
	tag_t *tag;
	int   bad;
} freetag_data_t;

static void check_freepost_tag(ss128_key_t key, ss128_value_t value,
                               void *data_)
{
	tag_t *tag = (tag_t *)value;
	freetag_data_t *data = data_;
	(void) key;
	// @@ This should be impllist_iterate
	impllist_t *impl = tag->implications;
	while (impl) {
		for (int i = 0; i < arraylen(impl->impl); i++) {
			if (impl->impl[i].tag == data->tag) data->bad = 1;
		}
		impl = impl->next;
	}
}

static void check_freepost_alias(ss128_key_t key, ss128_value_t value,
                                 void *data_)
{
	tagalias_t *alias = (tagalias_t *)value;
	freetag_data_t *data = data_;
	(void) key;
	if (alias->tag == data->tag) data->bad = 1;
}

int prot_delete(connection_t *conn, char *cmd)
{
	char *args = cmd + 1;
	char *name = cmd + 2;
	post_t *post;
	tag_t *tag;
	ss128_key_t key;
	switch (*cmd) {
		case 'A':
			if (*args != 'N') return error1(conn, args);
			void *alias = NULL;
			key = ss128_str2key(name);
			ss128_find(tagaliases, &alias, key);
			if (!alias) return error1(conn, args);
			ss128_delete(tagaliases, key);
			break;
		case 'T':
			if (*args != 'G') return error1(conn, args);
			tag = tag_find_guidstr(args + 1);
			if (!tag) return error1(conn, args);
			if (tag->datatag) return error1(conn, args);
			if (tag->posts.count || tag->weak_posts.count) {
				return error1(conn, args);
			}
			freetag_data_t data;
			data.tag = tag;
			data.bad = 0;
			ss128_iterate(tags, check_freepost_tag, &data);
			ss128_iterate(tagaliases, check_freepost_alias, &data);
			if (data.bad) return error1(conn, args);
			tag_delete(tag);
			break;
		case 'P':
			if (post_find_md5str(&post, args)) {
				return error1(conn, args);
			}
			unsigned int datatags = 0;
			const int dt_count = log_version > 0 ? REALLY_MAGIC_TAGS : arraylen(magic_tag);
			for (int i = 0; i < dt_count; i++) {
				tag = magic_tag[i];
				if (tag && post_has_tag(post, tag, T_NO)) {
					datatags++;
				}
			}
			if (post->of_tags > datatags || post->of_weak_tags
			    || post->related_posts.head) {
				return error1(conn, args);
			}
			for (int i = 0; i < dt_count; i++) {
				tag = magic_tag[i];
				if (tag) post_tag_rem(post, tag);
			}
			assert(!post->of_tags);
			post_delete(post);
			break;
		default:
			return error1(conn, cmd);
			break;
	}
	log_write(&conn->trans, "D%s", cmd);
	return 0;
}

typedef struct rel_data {
	post_t *post;
	char type;
	int (*func)(post_t *, post_t *);
} rel_data_t;

static int rel_cmd(connection_t *conn, char *cmd, void *data_,
                   prot_cmd_flag_t flags)
{
	post_t     *post;
	rel_data_t *data = data_;

	(void)flags;

	if (post_find_md5str(&post, cmd)) return conn->error(conn, cmd);
	if (!data->post) {
		data->post = post;
		log_set_init(&conn->trans, "R%c%s", data->type, cmd);
		return 0;
	}
	if (data->func(data->post, post)) return conn->error(conn, cmd);
	log_write(&conn->trans, "%s", cmd);
	return 0;
}

int prot_rel_add(connection_t *conn, char *cmd)
{
	rel_data_t data = {NULL, 'R', post_rel_add};
	return prot_cmd_loop(conn, cmd, &data, rel_cmd, CMDFLAG_MODIFY);
}

int prot_rel_remove(connection_t *conn, char *cmd)
{
	rel_data_t data = {NULL, 'r', post_rel_remove};
	return prot_cmd_loop(conn, cmd, &data, rel_cmd, CMDFLAG_MODIFY);
}

typedef int (*implfunc_t)(tag_t *from, const implication_t *impl);
typedef struct impldata {
	tag_t          *tag;
	implfunc_t     func;
	implication_t  impl;
	tag_value_t    *filter_value;
	tagvalue_cmp_t filter_cmp;
	const char     *set_value_str;
	char           master_cmd;
} impldata_t;

static int impl_apply(connection_t *conn, const impldata_t *data)
{
	char buf[1024];
	char *ptr = buf;
	const implication_t *impl = &data->impl;
	
	if (data->func(data->tag, &data->impl)) return 1;
	ptr += sprintf(ptr, "%c%s", 'i' - (32 * impl->positive),
	               guid_guid2str(impl->tag->guid));
	if (impl->priority) {
		ptr += sprintf(ptr, " P%ld", (long)impl->priority);
	}
	if (impl->inherit_value) {
		ptr += sprintf(ptr, " V");
	}
	if (impl->set_value) {
		ptr += sprintf(ptr, " V%s", data->set_value_str);
	}
	log_write(&conn->trans, "%s", buf);
	return 0;
}

static int impl_cmd(connection_t *conn, char *cmd, void *data_,
                    prot_cmd_flag_t flags)
{
	impldata_t    *data = data_;
	implication_t *impl = &data->impl;
	tag_value_t   tval;
	char *end;
	
	if (*cmd == 'I' || *cmd == 'i') {
		if (impl->tag) {
			err1(impl_apply(conn, data));
		}
		memset(impl, 0, sizeof(*impl));
		impl->filter_value = data->filter_value;
		impl->filter_cmp   = data->filter_cmp;
	} else {
		err1(log_version < 2);
		err1(!impl->tag);
	}
	if (log_version < 2) {
		char *colon = strchr(cmd + 1, ':');
		if (colon) {
			impl->priority = strtol(colon + 1, &end, 10);
			err1(*end)
			*colon = 0;
		}
	}
	switch (*cmd) {
		case 'I':
			impl->positive = 1;
			// Fall through
		case 'i':
			impl->tag = tag_find_guidstr(cmd + 1);
			if (!impl->tag || impl->tag == data->tag) {
				return conn->error(conn, cmd);
			}
			break;
		case 'P':
			err1(data->master_cmd == 'i');
			impl->priority = strtol(cmd + 1, &end, 10);
			err1(*end);
			break;
		case 'V':
			err1(data->master_cmd == 'i');
			err1(!impl->positive);
			err1(!impl->tag->valuetype);
			if (cmd[1]) {
				err1(impl->inherit_value);
				memset(&tval, 0, sizeof(tval));
				err1(tag_value_parse(impl->tag, cmd + 1, &tval, 0, 0));
				data->set_value_str = cmd + 1;
				impl->set_value = mm_alloc(sizeof(tval));
				*impl->set_value = tval;
			} else {
				err1(impl->set_value);
				err1(data->tag->valuetype != impl->tag->valuetype);
				impl->inherit_value = 1;
			}
			break;
		default:
			return conn->error(conn, cmd);
			break;
	}
	if (flags & CMDFLAG_LAST) {
		err1(impl_apply(conn, data));
	}
	return 0;
err:
	return conn->error(conn, cmd);
}

int prot_implication(connection_t *conn, char *cmd)
{
	impldata_t data;
	char *end = strchr(cmd, ' ');
	if (!end) return conn->error(conn, cmd);
	*end = 0;
	memset(&data, 0, sizeof(data));
	data.master_cmd = *cmd;
	tag_value_t tval;
	memset(&tval, 0, sizeof(tval));
	data.tag = tag_find_guidstr_value(cmd + 1, &data.filter_cmp, &tval, NULL);
	if (!data.tag) return conn->error(conn, cmd);
	if (data.filter_cmp) {
		if (log_version < 2) return conn->error(conn, cmd);
		data.filter_value = mm_alloc(sizeof(tval));
		*data.filter_value = tval;
	}
	switch (*cmd) {
		case 'I':
			data.func = tag_add_implication;
			break;
		case 'i':
			data.func = tag_rem_implication;
			break;
		default:
			return conn->error(conn, cmd);
			break;
	}
	log_set_init(&conn->trans, "I%s", cmd);
	return prot_cmd_loop(conn, end + 1, &data, impl_cmd, CMDFLAG_MODIFY);
}

typedef struct orderdata {
	tag_t       *tag;
	post_node_t *node;
} orderdata_t;

static int order_cmd(connection_t *conn, char *cmd, void *data_,
                     prot_cmd_flag_t flags)
{
	orderdata_t *data = data_;
	post_t *post;
	if (*cmd != 'P') return conn->error(conn, cmd);
	if (post_find_md5str(&post, cmd + 1)) return conn->error(conn, cmd);
	if (!post_has_tag(post, data->tag, T_NO)) return conn->error(conn, cmd);
	post_list_t *pl = &data->tag->posts;
	post_node_t *pn = pl->head;
	while (pn) {
		if (pn->post == post) break;
		pn = pn->succ;
	}
	assert(pn);
	if (pn == data->node) return 0; // Ignore repeated posts
	if (!data->tag->ordered) {
		// This tag was unordered, put first post first.
		data->tag->ordered = 1;
		post_remove(pl, pn);
		post_addhead(pl, pn);
	}
	if (data->node) {
		post_remove(pl, pn);
		pn->pred = data->node;
		pn->succ = data->node->succ;
		if (pn->succ) {
			pn->succ->pred = pn;
		} else {
			pl->tail = pn;
		}
		data->node->succ = pn;
	} else if (flags & CMDFLAG_LAST) {
		// This is the only post, put it first.
		post_remove(pl, pn);
		post_addhead(pl, pn);
	}
	data->node = pn;
	log_write(&conn->trans, "%s", cmd);
	return 0;
}

typedef struct order_check {
	int ok;
	tag_t *tag;
} order_check_t;

static void order_check_implied(post_node_t *ln, void *data_)
{
	order_check_t *chk = data_;
	if (taglist_contains(ln->post->implied_tags, chk->tag)) chk->ok = 0;
}

int prot_order(connection_t *conn, char *cmd)
{
	orderdata_t data;
	if (*cmd != 'G') return conn->error(conn, cmd);
	char *end = strchr(cmd, ' ');
	if (!end) return conn->error(conn, cmd);
	*end = 0;
	tag_t *tag = tag_find_guidstr(cmd + 1);
	if (!tag || tag->weak_posts.count) return conn->error(conn, cmd);
	order_check_t chk;
	chk.ok  = 1;
	chk.tag = tag;
	post_iterate(&tag->posts, &chk, order_check_implied);
	if (!chk.ok) return conn->error(conn, cmd);
	data.tag  = tag;
	data.node = NULL;
	log_set_init(&conn->trans, "O%s", cmd);
	return prot_cmd_loop(conn, end + 1, &data, order_cmd, CMDFLAG_MODIFY);
}
