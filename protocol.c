#include "db.h"

#include <stddef.h> /* offsetof() */
#include <errno.h>
#include <time.h>

static int tag_post_cmd(connection_t *conn, const char *cmd, void *post_,
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
			tag_t *tag = tag_find_guidstr(args);
			if (!tag) return conn->error(conn, cmd);
			if (*cmd == 'T') {
				int r = post_tag_add(*post, tag, weak);
				if (r) return conn->error(conn, cmd);
			} else {
				int r = post_tag_rem(*post, tag);
				if (r) return conn->error(conn, cmd);
			}
			log_write(&conn->trans, "%s", cmd);
			break;
		default:
			return conn->error(conn, cmd);
			break;
	}
	if (*cmd != 'P' && *post) (*post)->modified = conn->trans.now;
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

typedef struct tag_cmd_data {
	tag_t      *tag;
	int        is_add;
	const char *name;
} tag_cmd_data_t;

static int tag_cmd(connection_t *conn, const char *cmd, void *data_,
                   prot_cmd_flag_t flags)
{
	tag_cmd_data_t *data = data_;
	tag_t      *tag = data->tag;
	int        r = 1;
	const char *args = cmd + 1;
	char       *ptr;

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
			    && tag_find_name(args, T_DONTCARE)
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
		log_write_tag(&conn->trans, tag, data->is_add);
	}
	return 0;
}

static int add_alias_cmd(connection_t *conn, const char *cmd, void *data,
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

#define POST_FIELD_DEF(name, type, cap, array)                   \
                      {#name, sizeof(((post_t *)0)->name),       \
                       offsetof(post_t, name), type, cap, array}

const field_t post_fields[] = {
	POST_FIELD_DEF(width    , FIELDTYPE_UNSIGNED, CAP_POST, NULL),
	POST_FIELD_DEF(height   , FIELDTYPE_UNSIGNED, CAP_POST, NULL),
	POST_FIELD_DEF(modified , FIELDTYPE_UNSIGNED, CAP_SUPER, NULL), // Could be signed
	POST_FIELD_DEF(created  , FIELDTYPE_UNSIGNED, CAP_SUPER, NULL), // Could be signed
	POST_FIELD_DEF(image_date, FIELDTYPE_UNSIGNED, CAP_POST, NULL), // Could be signed
	POST_FIELD_DEF(image_date_fuzz, FIELDTYPE_UNSIGNED, CAP_POST, NULL),
	POST_FIELD_DEF(score    , FIELDTYPE_SIGNED  , CAP_POST, NULL),
	POST_FIELD_DEF(filetype , FIELDTYPE_ENUM    , CAP_POST, &filetype_names),
	POST_FIELD_DEF(rating   , FIELDTYPE_ENUM    , CAP_POST, &rating_names),
	POST_FIELD_DEF(source   , FIELDTYPE_STRING  , CAP_POST, NULL),
	POST_FIELD_DEF(title    , FIELDTYPE_STRING  , CAP_POST, NULL),
	{NULL, 0, 0, 0, 0, NULL}
};

/* I need a setter for both signed and unsigned ints. This is mostly *
 * the same function but with different types. The magnificent       *
 * preprocessor comes to the rescue!                                 */

#define PUT_INT_VALUE_INNER(type, bytes)                  \
	type rv = v;                                      \
	if (v != rv) return 1;                            \
	memcpy((char *)post + field->offset, &rv, bytes);

#define PUT_INT_VALUE_FUNC(signed, type, strtot, base, check_v)             \
	static int put_##signed##_value(post_t *post, const field_t *field, \
	                                const char *val)                    \
	{                                                                   \
		char *end;                                                  \
		if (!*val) return 1;                                        \
		errno = 0;                                                  \
		signed long long v = strtot(val, &end, base);               \
		if (errno || *end) return 1;                                \
		if (check_v) return 1;                                      \
		if (field->size == 8) {                                     \
			PUT_INT_VALUE_INNER(type##64_t, 8);                 \
		} else if (field->size == 4) {                              \
			PUT_INT_VALUE_INNER(type##32_t, 4);                 \
		} else {                                                    \
			PUT_INT_VALUE_INNER(type##16_t, 2);                 \
			assert(field->size == 2);                           \
		}                                                           \
		return 0;                                                   \
	}

PUT_INT_VALUE_FUNC(signed, int, strtoll, 10, v == LLONG_MAX || v == LLONG_MIN)
PUT_INT_VALUE_FUNC(unsigned, uint, strtoull, 16, v == ULLONG_MAX)

static int put_enum_value_post(post_t *post, const field_t *field,
                               const char *val)
{
	uint16_t *p = (uint16_t *)((char *)post + field->offset);
	assert(field->size == 2);
	return put_enum_value_gen(p, *field->array, val);
}

static int put_string_value(post_t *post, const field_t *field, const char *val)
{
	const char **res = (const char **)((char *)post + field->offset);
	const char *decoded;

	decoded = str_enc2str(val);
	if (!decoded) return 1;
	*res = mm_strdup(decoded);
	return 0;
}

static int put_in_post_field(const user_t *user, post_t *post, const char *str,
                             unsigned int nlen)
{
	const field_t *field = post_fields;
	int (*func[])(post_t *, const field_t *, const char *) = {
		put_unsigned_value,
		put_signed_value,
		put_enum_value_post,
		put_string_value,
	};

	while (field->name) {
		if (!memcmp(field->name, str, nlen)) {
			const char *valp = str + nlen + 1;
			if (field->name[nlen]) return 1;
			if (!*valp) return 1;
			if (!(user->caps & field->modcap)) return 1;
			if (func[field->type](post, field, valp)) {
				return 1;
			}
			return 0;
		}
		field++;
	}
	return 1;
}

static int post_cmd(connection_t *conn, const char *cmd, void *data,
                    prot_cmd_flag_t flags)
{
	post_t     *post = *(post_t **)data;
	const char *eqp;

	eqp = strchr(cmd, '=');
	if (eqp) {
		unsigned int len = eqp - cmd;
		if (!post) return conn->error(conn, cmd);
		if (put_in_post_field(conn->user, post, cmd, len)) {
			return conn->error(conn, cmd);
		}
		post->modified = conn->trans.now;
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
		if (!memcmp(&post->md5, &null_md5, sizeof(md5_t))
		    || !post->height || !post->width
		    || post->filetype == (uint16_t)~0) {
			return conn->error(conn, cmd);
		}
		r = ss128_insert(posts, post, post->md5.key);
		if (r) {
			return conn->error(conn, cmd);
		}
		log_write_post(&conn->trans, post);
	}
	return 0;
}

static user_t *user_find(const char *name)
{
	void        *user;
	ss128_key_t key = ss128_str2key(name);
	if (ss128_find(users, &user, key)) return NULL;
	return (user_t *)user;
}

static int user_cmd(connection_t *conn, const char *cmd, void *data,
                    prot_cmd_flag_t flags)
{
	user_t     *moduser = *(user_t **)data;
	const char *args = cmd + 1;
	const char *name;
	uint16_t   u16;
	int        r;

	if (!*cmd || !*args) return conn->error(conn, cmd);
	switch (*cmd) {
		case 'N':
			name = str_enc2str(args);
			if (flags & CMDFLAG_MODIFY) {
				user_t **userp = data;
				if (moduser) return conn->error(conn, cmd);
				moduser = user_find(name);
				*userp = moduser;
				log_set_init(&conn->trans, "MUN%s", args);
			} else {
				moduser->name = mm_strdup(name);
			}
			break;
		case 'C': // Set cap
		case 'c': // Remove cap
			u16 = 0; // GCC is (sometimes) an idiot
			r = put_enum_value_gen(&u16, cap_names, args);
			if (r || !moduser) return conn->error(conn, cmd);
			if (*cmd == 'C') {
				moduser->caps |= 1 << u16;
			} else {
				moduser->caps &= ~(1 << u16);
			}
			if (flags & CMDFLAG_MODIFY) {
				log_write(&conn->trans, "%s", cmd);
			}
			break;
		case 'P':
			if (!moduser) return conn->error(conn, cmd);
			moduser->password = mm_strdup(str_enc2str(args));
			if (flags & CMDFLAG_MODIFY) {
				log_write(&conn->trans, "%s", cmd);
			}
			break;
		default:
			return conn->error(conn, cmd);
	}
	if ((flags & CMDFLAG_LAST) && !(flags & CMDFLAG_MODIFY)) {
		ss128_key_t key;
		if (!moduser->name || !moduser->password) {
			return conn->error(conn, cmd);
		}
		key = ss128_str2key(moduser->name);
		r = ss128_insert(users, moduser, key);
		if (r) return conn->error(conn, cmd);
		log_write_user(&conn->trans, moduser);
	}
	return 0;
}

int prot_add(connection_t *conn, char *cmd)
{
	prot_cmd_func_t func;
	void *data = NULL;
	void *dataptr = &data;
	tag_cmd_data_t tag_data;

	switch (*cmd) {
		case 'T':
			func = tag_cmd;
			tag_data.tag = mm_alloc(sizeof(tag_t));
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
			((post_t *)data)->created  = time(NULL);
			((post_t *)data)->filetype = (uint16_t)~0;
			break;
		case 'U':
			func = user_cmd;
			data = mm_alloc(sizeof(user_t));
			((user_t *)data)->caps = DEFAULT_CAPS;
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
		case 'U':
			func = user_cmd;
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
			tag_t *tag = tag_find_guidstr(args + 1);
			if (!tag) return error1(conn, args);
			if (tag->posts.count || tag->weak_posts.count) {
				return error1(conn, args);
			}
			freetag_data_t data;
			data.tag = tag;
			data.bad = 0;
			ss128_iterate(tags, check_freepost_tag, &data);
			ss128_iterate(tagaliases, check_freepost_alias, &data);
			if (data.bad) return error1(conn, args);
			key = ss128_str2key(tag->name);
			int r = ss128_delete(tags, key);
			assert(!r);
			r = ss128_delete(tagguids, tag->guid.key);
			assert(!r);
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

static int rel_cmd(connection_t *conn, const char *cmd, void *data_,
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

typedef int (*implfunc_t)(tag_t *from, tag_t *to, int positive, int priority);
typedef struct impldata {
	tag_t      *tag;
	implfunc_t func;
} impldata_t;

static int impl_cmd(connection_t *conn, const char *cmd, void *data_,
                    prot_cmd_flag_t flags)
{
	impldata_t *data = data_;

	(void)flags;

	char *colon = strchr(cmd + 1, ':');
	int32_t priority = 0;
	if (colon) {
		char *end;
		priority = strtol(colon + 1, &end, 10);
		if (*end) return conn->error(conn, cmd);
		*colon = 0;
	}
	tag_t *implied_tag = tag_find_guidstr(cmd + 1);
	if (!implied_tag) return conn->error(conn, cmd);
	int positive;
	switch (*cmd) {
		case 'I':
			positive = 1;
			break;
		case 'i':
			positive = 0;
			break;
		default:
			return conn->error(conn, cmd);
			break;
	}
	if (data->func(data->tag, implied_tag, positive, priority)) {
		return conn->error(conn, cmd);
	}
	log_write(&conn->trans, "%s:%ld", cmd, (long)priority);
	return 0;
}

int prot_implication(connection_t *conn, char *cmd)
{
	impldata_t data;
	char *end = strchr(cmd, ' ');
	if (!end) return conn->error(conn, cmd);
	*end = 0;
	data.tag = tag_find_guidstr(cmd + 1);
	if (!data.tag) return conn->error(conn, cmd);
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
	tag_t  *tag;
	postlist_node_t *node;
	int pos;
} orderdata_t;

static void order_move_back(postlist_node_t *from_node, int from_pos,
                            postlist_node_t *to_node, int to_pos)
{
	if (++to_pos == arraylen(to_node->posts)) {
		assert(to_node->next);
		to_pos = 0;
		to_node = to_node->next;
	}
	post_t *value = to_node->posts[to_pos];
	to_node->posts[to_pos] = from_node->posts[from_pos];
	while(from_node != to_node || to_pos < from_pos) {
		if (++to_pos == arraylen(to_node->posts)) {
			assert(to_node->next);
			to_pos = 0;
			to_node = to_node->next;
		}
		post_t *next_value = to_node->posts[to_pos];
		to_node->posts[to_pos] = value;
		value = next_value;
	}
}

static void order_move_forward(postlist_node_t *from_node, int from_pos,
                               postlist_node_t *to_node, int to_pos)
{
	post_t *value = from_node->posts[from_pos];
	while(from_node != to_node || to_pos > from_pos) {
		post_t *next;
		if (from_pos < arraylen(from_node->posts) - 1) {
			next = from_node->posts[from_pos + 1];
		} else {
			next = from_node->next->posts[0];
		}
		from_node->posts[from_pos] = next;
		if (++from_pos == arraylen(from_node->posts)) {
			assert(from_node->next);
			from_node = from_node->next;
			from_pos = 0;
		}
	}
	to_node->posts[to_pos] = value;
}

static int order_cmd(connection_t *conn, const char *cmd, void *data_,
                     prot_cmd_flag_t flags)
{
	orderdata_t *data = data_;
	post_t *post;
	if (*cmd != 'P') return conn->error(conn, cmd);
	if (post_find_md5str(&post, cmd + 1)) return conn->error(conn, cmd);
	if (!post_has_tag(post, data->tag, T_NO)) return conn->error(conn, cmd);
	int pos;
	int past = 0;
	int found = 0;
	int first = 0;
	postlist_node_t *node = data->tag->posts.head;
again:
	for (int i = 0; i < arraylen(node->posts); i++) {
		if (node->posts[i] == post) {
			pos = i;
			found = 1;
			break;
		}
	}
	if (!found) {
		assert(node->next);
		if (node == data->node) past = 1;
		node = node->next;
		goto again;
	}
	if (node == data->node) {
		if (pos > data->pos) past = 1;
		if (pos == data->pos) return conn->error(conn, cmd);
	}
	if (!data->node && (flags & CMDFLAG_LAST)) {
		// This is the only post, put it first.
		if (node != data->tag->posts.head || pos) {
			data->node = data->tag->posts.head;
			data->pos = 0;
			past = 1;
			first = 1;
		}
	}
	if (data->node) {
		if (past) {
			order_move_back(node, pos, data->node, data->pos);
		} else {
			order_move_forward(node, pos, data->node, data->pos);
		}
		if (first) {
			node = data->tag->posts.head;
			node->posts[1] = node->posts[0];
			node->posts[0] = post;
		}
	}
	if (!data->node) {
		data->node = node;
		data->pos = pos;
	} else if (past) {
		if (++data->pos == arraylen(data->node->posts)) {
			assert(data->node->next);
			data->node = data->node->next;
			data->pos = 0;
		}
	}
	log_write(&conn->trans, "%s", cmd);
	return 0;
}

typedef struct order_check {
	int ok;
	tag_t *tag;
} order_check_t;

static void order_check_implied(void *data_, post_t *post)
{
	order_check_t *chk = data_;
	if (taglist_contains(post->implied_tags, chk->tag)) chk->ok = 0;
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
	postlist_iterate(&tag->posts, &chk, order_check_implied);
	if (!chk.ok) return conn->error(conn, cmd);
	data.tag  = tag;
	data.node = NULL;
	data.pos  = -1;
	log_set_init(&conn->trans, "O%s", cmd);
	return prot_cmd_loop(conn, end + 1, &data, order_cmd, CMDFLAG_MODIFY);
}

user_t *prot_auth(char *cmd)
{
	char   *pass;
	user_t *user;

	pass = strchr(cmd, ' ');
	if (!pass) return NULL;
	*pass++ = '\0';
	if (!*pass) return NULL;
	user = user_find(cmd);
	if (!user) return NULL;
	if (strcmp(user->password, pass)) return NULL;
	return user;
}
