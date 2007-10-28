#include "db.h"

#include <stddef.h> /* offsetof() */
#include <errno.h>

const char *tagtype_names[] = {
	"unspecified",
	"inimage",
	"artist",
	"character",
	"copyright",
	"meta",
	"ambiguous",
	NULL
};

static int tag_post_cmd(user_t *user, const char *cmd, void *post_, prot_cmd_flag_t flags, trans_t *trans, prot_err_func_t error) {
	post_t     **post = post_;
	const char *args = cmd + 1;

// @@ trans
	(void)flags;
	switch (*cmd) {
		case 'P': // Which post
			if (*post) {
				return error(cmd);
			} else {
				post_find_md5str(post, args);
				if (!*post) return error(cmd);
			}
			break;
		case 'T': // Add tag
		case 't': // Remove tag
			if (!*post) return error(cmd);
			truth_t weak = T_NO;
			if (*args == '~') { // Weak tag
				args++;
				weak = T_YES;
			}
			tag_t *tag = tag_find_guidstr(args);
			if (!tag) return error(cmd);
			if (*cmd == 'T') {
				int r = post_tag_add(*post, tag, weak);
				if (r) return error(cmd);
			} else {
				return error(cmd); // @@TODO: Implement removal
			}
			break;
		default:
			return error(cmd);
			break;
	}
	return 0;
}

int prot_cmd_loop(user_t *user, char *cmd, void *data, prot_cmd_func_t func, prot_cmd_flag_t flags, trans_t *trans, prot_err_func_t error) {
	while (*cmd) {
		int  len = 0;
		while (cmd[len] && cmd[len] != ' ') len++;
		if (cmd[len]) {
			cmd[len] = 0;
			len++;
		}
		if (!cmd[len]) flags |= CMDFLAG_LAST;
		if (func(user, cmd, data, flags, trans, error)) return 1;
		cmd += len;
	}
	return 0;
}

int prot_tag_post(user_t *user, char *cmd, trans_t *trans, prot_err_func_t error) {
	post_t *post = NULL;
	return prot_cmd_loop(user, cmd, &post, tag_post_cmd, CMDFLAG_NONE, trans, error);
}

static int error1(char *cmd, prot_err_func_t error) {
	int len = 0;
	while (cmd[len] && cmd[len] != ' ') len++;
	cmd[len] = 0;
	return error(cmd);
}

static int put_enum_value_gen(uint16_t *res, const char **array, const char *val) {
	uint16_t i;
	for (i = 0; array[i]; i++) {
		if (!strcmp(array[i], val)) {
			*res = i;
			return 0;
		}
	}
	return 1;
}

static int add_tag_cmd(user_t *user, const char *cmd, void *data, prot_cmd_flag_t flags, trans_t *trans, prot_err_func_t error) {
	tag_t      *tag = *(tag_t **)data;
	int        r;
	const char *args = cmd + 1;
	char       *ptr;

// @@ trans
	if (!*cmd || !*args) return error(cmd);
	switch (*cmd) {
		case 'G':
			r = guid_str2guid(&tag->guid, args, GUIDTYPE_TAG);
			if (r) return error(cmd);
			break;
		case 'N':
			tag->name = mm_strdup(args);
			break;
		case 'T':
			if (put_enum_value_gen(&tag->type, tagtype_names, args)) {
				return error(cmd);
			}
			break;
		default:
			return error(cmd);
	}
	if (flags & CMDFLAG_LAST) {
		rbtree_key_t key;
		int i;
		ptr = (char *)&tag->guid;
		for (i = 0; i < sizeof(tag->guid); i++) {
			if (ptr[i]) break;
		}
		if (i == sizeof(tag->guid) || !tag->name) return error(cmd);
		key = rbtree_str2key(tag->name);
		mm_lock();
		if (rbtree_insert(tagtree, tag, key)) {
			mm_unlock();
			return error(cmd);
		}
		if (rbtree_insert(tagguidtree, tag, tag->guid.key)) {
			rbtree_delete(tagtree, key);
			mm_unlock();
			return error(cmd);
		}
		mm_unlock();
	}
	return 0;
}

static int add_alias_cmd(user_t *user, const char *cmd, void *data, prot_cmd_flag_t flags, trans_t *trans, prot_err_func_t error) {
	tagalias_t *tagalias = *(tagalias_t **)data;
	const char *args = cmd + 1;

// @@ trans
	if (!*cmd || !*args) return error(cmd);
	switch (*cmd) {
		case 'G':
			tagalias->tag = tag_find_guidstr(args);
			break;
		case 'N':
			tagalias->name = mm_strdup(args);
			break;
		default:
			return error(cmd);
	}
	if (flags & CMDFLAG_LAST) {
		rbtree_key_t key;
		if (!tagalias->tag || !tagalias->name) return error(cmd);
		key = rbtree_str2key(tagalias->name);
		mm_lock();
		if (!rbtree_find(tagaliastree, NULL, key)
		 || rbtree_insert(tagaliastree, tagalias, key)) {
		 	mm_unlock();
		 	return error(cmd);
		}
		mm_unlock();
	}
	return 0;
}

typedef enum {
	FIELDTYPE_UNSIGNED,
	FIELDTYPE_SIGNED,
	FIELDTYPE_ENUM,
	FIELDTYPE_STRING,
} fieldtype_t;

typedef struct {
	const char  *name;
	int         size;
	int         offset;
	fieldtype_t type;
	const char  **array;
} post_field_t;

const char *rating_names[] = {
	"unspecified",
	"safe",
	"questionable",
	"explicit",
	NULL
};

const char *filetype_names[] = {
	"jpeg",
	"gif",
	"png",
	"bmp",
	"swf",
	NULL
};

#define POST_FIELD_DEF(name, type, array) {#name, sizeof(((post_t *)0)->name), \
                                           offsetof(post_t, name), type, array}
post_field_t post_fields[] = {
	POST_FIELD_DEF(width, FIELDTYPE_UNSIGNED, NULL),
	POST_FIELD_DEF(height, FIELDTYPE_UNSIGNED, NULL),
	POST_FIELD_DEF(created, FIELDTYPE_UNSIGNED, NULL), // Could be signed, but I don't care.
	POST_FIELD_DEF(score, FIELDTYPE_SIGNED, NULL),
	POST_FIELD_DEF(filetype, FIELDTYPE_ENUM, filetype_names),
	POST_FIELD_DEF(rating, FIELDTYPE_ENUM, rating_names),
	POST_FIELD_DEF(source, FIELDTYPE_STRING, NULL),
	POST_FIELD_DEF(title, FIELDTYPE_STRING, NULL),
	{NULL}
};

static int put_int_value(post_t *post, post_field_t *field, const char *val) {
	/* I can see no reasonable way to merge these cases. *
	 * (Other than horrible preprocessor madness.)       */
	if (!*val) return 1;
	errno = 0;
	if (field->type == FIELDTYPE_SIGNED) {
		char *end;
		long long v = strtoll(val, &end, 10);
		if (errno || *end) return 1;
		if (v == LLONG_MAX || v == LLONG_MIN) return 1;
		if (field->size == 8) {
			int64_t rv = v;
			if (v != rv) return 1;
			memcpy((char *)post + field->offset, &rv, 8);
		} else if (field->size == 4) {
			int32_t rv = v;
			if (v != rv) return 1;
			memcpy((char *)post + field->offset, &rv, 4);
		} else {
			int16_t rv = v;
			assert(field->size == 2);
			if (v != rv) return 1;
			memcpy((char *)post + field->offset, &rv, 2);
		}
	} else {
		char *end;
		unsigned long long v = strtoull(val, &end, 10);
		if (errno || *end) return 1;
		if (v == ULLONG_MAX) return 1;
		if (field->size == 8) {
			uint64_t rv = v;
			if (v != rv) return 1;
			memcpy((char *)post + field->offset, &rv, 8);
		} else if (field->size == 4) {
			uint32_t rv = v;
			if (v != rv) return 1;
			memcpy((char *)post + field->offset, &rv, 4);
		} else {
			uint16_t rv = v;
			assert(field->size == 2);
			if (v != rv) return 1;
			memcpy((char *)post + field->offset, &rv, 2);
		}
	}
	return 0;
}

static int put_enum_value_post(post_t *post, post_field_t *field, const char *val) {
	assert(field->size == 2);
	return put_enum_value_gen((uint16_t *)((char *)post + field->offset), field->array, val);
}

static int put_string_value(post_t *post, post_field_t *field, const char *val) {
	const char **res = (const char **)((char *)post + field->offset);
	const char *decoded;

	decoded = str_enc2str(val);
	if (!decoded) return 1;
	*res = mm_strdup(decoded);
	return 0;
}

static int put_in_post_field(post_t *post, const char *str, int nlen) {
	post_field_t *field = post_fields;
	int (*func[])(post_t *, post_field_t *, const char *) = {
		put_int_value,
		put_int_value,
		put_enum_value_post,
		put_string_value,
	};

	while (field->name) {
		if (!memcmp(field->name, str, nlen)) {
			const char *valp = str + nlen + 1;
			if (field->name[nlen]) return 1;
			if (!*valp) return 1;
			if (func[field->type](post, field, valp)) {
				return 1;
			}
			return 0;
		}
		field++;
	}
	return 1;
}

static int post_cmd(user_t *user, const char *cmd, void *data, prot_cmd_flag_t flags, trans_t *trans, prot_err_func_t error) {
	post_t     *post = *(post_t **)data;
	const char *eqp;

// @@ trans
	eqp = strchr(cmd, '=');
	if (eqp) {
		if (!post) return error(cmd);
		if (put_in_post_field(post, cmd, eqp - cmd)) {
			return error(cmd);
		}
	} else { // This is the md5
		if (flags & CMDFLAG_MODIFY) {
			post_t **postp = data;
			int r = post_find_md5str(&post, cmd);
			if (r) return error(cmd);
			if (*postp) return error(cmd);
			*postp = post;
		} else {
			int r = md5_str2md5(&post->md5, cmd);
			if (r) return error(cmd);
		}
	}
	if ((flags & CMDFLAG_LAST) && !(flags & CMDFLAG_MODIFY)) {
		int r;
		md5_t null_md5;
		memset(&null_md5, 0, sizeof(md5_t));
		if (!memcmp(&post->md5, &null_md5, sizeof(md5_t))
		 || !post->height || !post->width || !post->created) {
			return error(cmd);
		}
		mm_lock();
		r = rbtree_insert(posttree, post, post->md5.key);
		mm_unlock();
		if (r) return error(cmd);
	}
	return 0;
}

static user_t *user_find(const char *name) {
	void         *user;
	rbtree_key_t key = rbtree_str2key(name);
	if (rbtree_find(usertree, &user, key)) return NULL;
	return (user_t *)user;
}

const char *cap_names[] = {
	"post",
	"delete",
	"mkuser",
	"tag",
	"untag",
	"modcap",
	"mktag",
	NULL
};

static int user_cmd(user_t *user, const char *cmd, void *data, prot_cmd_flag_t flags, trans_t *trans, prot_err_func_t error) {
	user_t     *moduser = *(user_t **)data;
	const char *args = cmd + 1;
	const char *name;
	uint16_t   u16;
	int        r;

// @@ trans
	if (!*cmd || !*args) return error(cmd);
	switch (*cmd) {
		case 'N':
			name = str_enc2str(args);
			if (flags & CMDFLAG_MODIFY) {
				user_t **userp = data;
				if (moduser) return error(cmd);
				moduser = user_find(name);
				*userp = moduser;
			} else {
				moduser->name = mm_strdup(name);
			}
			break;
		case 'C': // Set cap
		case 'c': // Remove cap
			r = put_enum_value_gen(&u16, cap_names, args);
			if (r || !moduser) return error(cmd);
			if (*cmd == 'C') {
				moduser->caps |= 1 << u16;
			} else {
				moduser->caps &= ~(1 << u16);
			}
			break;
		case 'P':
			if (!moduser) return error(cmd);
			moduser->password = mm_strdup(str_enc2str(args));
			break;
		default:
			return error(cmd);
	}
	if ((flags & CMDFLAG_LAST) && !(flags & CMDFLAG_MODIFY)) {
		rbtree_key_t key;
		if (!moduser->name || !moduser->password) return error(cmd);
		key = rbtree_str2key(moduser->name);
		mm_lock();
		r = rbtree_insert(usertree, moduser, key);
		mm_unlock();
		if (r) return error(cmd);
	}
	return 0;
}

int prot_add(user_t *user, char *cmd, trans_t *trans, prot_err_func_t error) {
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
			break;
		case 'U':
			func = user_cmd;
			data = mm_alloc(sizeof(user_t));
			((user_t *)data)->caps = DEFAULT_CAPS;
			break;
		default:
			return error1(cmd, error);
	}
	return prot_cmd_loop(user, cmd + 1, &data, func, CMDFLAG_NONE, trans, error);
}

int prot_modify(user_t *user, char *cmd, trans_t *trans, prot_err_func_t error) {
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
			return error1(cmd, error);
	}
	return prot_cmd_loop(user, cmd + 1, &data, func, CMDFLAG_MODIFY, trans, error);
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
