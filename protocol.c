#include "db.h"

extern rbtree_head_t *tagtree;
extern rbtree_head_t *tagguidtree;

static int tag_post_cmd(const char *cmd, void *post_, int last, prot_err_func_t error) {
	post_t     **post = post_;
	const char *args = cmd + 1;

	(void)last;
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

int prot_cmd_loop(char *cmd, void *data, prot_cmd_func_t func, prot_err_func_t error) {
	while (*cmd) {
		int  len = 0;
		while (cmd[len] && cmd[len] != ' ') len++;
		if (cmd[len]) {
			cmd[len] = 0;
			len++;
		}
		if (func(cmd, data, cmd[len], error)) return 1;
		cmd += len;
	}
	return 0;
}

int prot_tag_post(char *cmd, prot_err_func_t error) {
	post_t *post = NULL;
	return prot_cmd_loop(cmd, &post, tag_post_cmd, error);
}

static int error1(char *cmd, prot_err_func_t error) {
	int len = 0;
	while (cmd[len] && cmd[len] != ' ') len++;
	cmd[len] = 0;
	return error(cmd);
}

static int add_tag_cmd(const char *cmd, void *data, int last, prot_err_func_t error) {
	tag_t      *tag = data;
	int        r;
	const char *args = cmd + 1;
	char       *ptr;

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
			tag->type = strtoul(args, &ptr, 10);
			if (*ptr) return error(cmd);
			break;
		default:
			return error(cmd);
	}
	if (last) {
		rbtree_key_t key;
		int i;
		ptr = (char *)&tag->guid;
		for (i = 0; i < sizeof(tag->guid); i++) {
			if (ptr[i]) break;
		}
		if (i != sizeof(tag->guid) || !tag->name) return error(cmd);
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

static int add_alias_cmd(const char *cmd, void *data, int last, prot_err_func_t error) {
	return 1;
}

static int add_post_cmd(const char *cmd, void *data, int last, prot_err_func_t error) {
	return 1;
}

int prot_add(char *cmd, prot_err_func_t error) {
	prot_cmd_func_t func;
	void *data = NULL;

	switch (*cmd) {
		case 'T':
			func = add_tag_cmd;
			data = mm_alloc(sizeof(tag_t));
			break;
		case 'A':
			func = add_alias_cmd; break;
		case 'P':
			func = add_post_cmd; break;
		default:
			return error1(cmd, error);
	}
	return prot_cmd_loop(cmd, data, func, error);
}
