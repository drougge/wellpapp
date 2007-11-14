#include "db.h"

#include <stddef.h> /* offsetof() */
#include <errno.h>
#include <time.h>

static int tag_post_cmd(connection_t *conn, const char *cmd, void *post_,
                        prot_cmd_flag_t flags) {
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
			if (*args == '~') { // Weak tag
				args++;
				weak = T_YES;
			}
			tag_t *tag = tag_find_guidstr(args);
			if (!tag) return conn->error(conn, cmd);
			if (*cmd == 'T') {
				int r = post_tag_add(*post, tag, weak);
				if (r) return conn->error(conn, cmd);
				log_write(&conn->trans, "%s", cmd);
			} else {
				return conn->error(conn, cmd); // @@TODO: Implement removal
			}
			break;
		default:
			return conn->error(conn, cmd);
			break;
	}
	if (*cmd != 'P' && *post) (*post)->modified = conn->trans.now;
	return 0;
}

int prot_cmd_loop(connection_t *conn, char *cmd, void *data,
                  prot_cmd_func_t func, prot_cmd_flag_t flags) {
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

int prot_tag_post(connection_t *conn, char *cmd) {
	post_t *post = NULL;
	return prot_cmd_loop(conn, cmd, &post, tag_post_cmd, CMDFLAG_NONE);
}

static int error1(connection_t *conn, char *cmd) {
	int len = 0;
	while (cmd[len] && cmd[len] != ' ') len++;
	cmd[len] = 0;
	return conn->error(conn, cmd);
}

static int put_enum_value_gen(uint16_t *res, const char * const *array,
                              const char *val) {
	uint16_t i;
	for (i = 0; array[i]; i++) {
		if (!strcmp(array[i], val)) {
			*res = i;
			return 0;
		}
	}
	return 1;
}

static int add_tag_cmd(connection_t *conn, const char *cmd, void *data,
                       prot_cmd_flag_t flags) {
	tag_t      *tag = *(tag_t **)data;
	int        r;
	const char *args = cmd + 1;
	char       *ptr;

	if (!*cmd || !*args) return conn->error(conn, cmd);
	switch (*cmd) {
		case 'G':
			r = guid_str2guid(&tag->guid, args, GUIDTYPE_TAG);
			if (r) return conn->error(conn, cmd);
			break;
		case 'N':
			tag->name = mm_strdup(args);
			break;
		case 'T':
			if (put_enum_value_gen(&tag->type, tagtype_names, args)) {
				return conn->error(conn, cmd);
			}
			break;
		default:
			return conn->error(conn, cmd);
	}
	if (flags & CMDFLAG_LAST) {
		rbtree_key_t key;
		unsigned int i;
		ptr = (char *)&tag->guid;
		for (i = 0; i < sizeof(tag->guid); i++) {
			if (ptr[i]) break;
		}
		if (i == sizeof(tag->guid) || !tag->name) {
			return conn->error(conn, cmd);
		}
		key = rbtree_str2key(tag->name);
		mm_lock();
		if (rbtree_insert(tagtree, tag, key)) {
			mm_unlock();
			return conn->error(conn, cmd);
		}
		if (rbtree_insert(tagguidtree, tag, tag->guid.key)) {
			rbtree_delete(tagtree, key);
			mm_unlock();
			return conn->error(conn, cmd);
		}
		log_write_tag(&conn->trans, tag);
		mm_unlock();
	}
	return 0;
}

static int add_alias_cmd(connection_t *conn, const char *cmd, void *data,
                         prot_cmd_flag_t flags) {
	tagalias_t *tagalias = *(tagalias_t **)data;
	const char *args = cmd + 1;

	if (!*cmd || !*args) return conn->error(conn, cmd);
	switch (*cmd) {
		case 'G':
			tagalias->tag = tag_find_guidstr(args);
			break;
		case 'N':
			tagalias->name = mm_strdup(args);
			break;
		default:
			return conn->error(conn, cmd);
	}
	if (flags & CMDFLAG_LAST) {
		rbtree_key_t key;
		if (!tagalias->tag || !tagalias->name) {
			return conn->error(conn, cmd);
		}
		key = rbtree_str2key(tagalias->name);
		mm_lock();
		if (!rbtree_find(tagaliastree, NULL, key)
		 || rbtree_insert(tagaliastree, tagalias, key)) {
		 	mm_unlock();
		 	return conn->error(conn, cmd);
		}
		log_write_tagalias(&conn->trans, tagalias);
		mm_unlock();
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

#define PUT_INT_VALUE_FUNC(signed, type, strtot, check_v) \
	static int put_##signed##_value(post_t *post, const field_t *field, \
	                                const char *val) {                  \
		char *end;                                                  \
		if (!*val) return 1;                                        \
		errno = 0;                                                  \
		signed long long v = strtot(val, &end, 10);                 \
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

PUT_INT_VALUE_FUNC(signed, int, strtoll, v == LLONG_MAX || v == LLONG_MIN)
PUT_INT_VALUE_FUNC(unsigned, uint, strtoull, v == ULLONG_MAX)

static int put_enum_value_post(post_t *post, const field_t *field, const char *val) {
	uint16_t *p = (uint16_t *)((char *)post + field->offset);
	assert(field->size == 2);
	return put_enum_value_gen(p, *field->array, val);
}

static int put_string_value(post_t *post, const field_t *field, const char *val) {
	const char **res = (const char **)((char *)post + field->offset);
	const char *decoded;

	decoded = str_enc2str(val);
	if (!decoded) return 1;
	*res = mm_strdup(decoded);
	return 0;
}

static int put_in_post_field(const user_t *user, post_t *post, const char *str,
                             unsigned int nlen) {
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
                    prot_cmd_flag_t flags) {
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
		mm_lock();
		r = rbtree_insert(posttree, post, post->md5.key);
		if (r) {
			mm_unlock();
			return conn->error(conn, cmd);
		}
		log_write_post(&conn->trans, post);
		mm_unlock();
	}
	return 0;
}

static user_t *user_find(const char *name) {
	void         *user;
	rbtree_key_t key = rbtree_str2key(name);
	if (rbtree_find(usertree, &user, key)) return NULL;
	return (user_t *)user;
}

static int user_cmd(connection_t *conn, const char *cmd, void *data,
                    prot_cmd_flag_t flags) {
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
		rbtree_key_t key;
		if (!moduser->name || !moduser->password) {
			return conn->error(conn, cmd);
		}
		key = rbtree_str2key(moduser->name);
		mm_lock();
		r = rbtree_insert(usertree, moduser, key);
		mm_unlock();
		if (r) return conn->error(conn, cmd);
		log_write_user(&conn->trans, moduser);
	}
	return 0;
}

int prot_add(connection_t *conn, char *cmd) {
	prot_cmd_func_t func;
	void *data = NULL;

	switch (*cmd) {
		case 'T':
			func = add_tag_cmd;
			data = mm_alloc(sizeof(tag_t));
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
	return prot_cmd_loop(conn, cmd + 1, &data, func, CMDFLAG_NONE);
}

int prot_modify(connection_t *conn, char *cmd) {
	prot_cmd_func_t func;
	void *data = NULL;

	switch (*cmd) {
		case 'P':
			func = post_cmd;
			break;
		case 'U':
			func = user_cmd;
			break;
		default:
			return error1(conn, cmd);
	}
	return prot_cmd_loop(conn, cmd + 1, &data, func, CMDFLAG_MODIFY);
}

user_t *prot_auth(char *cmd) {
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
