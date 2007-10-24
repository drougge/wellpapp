#include "db.h"

static int tag_post_cmd(const char *cmd, void *post_, prot_err_func_t error) {
	post_t     **post = post_;
	const char *args = cmd + 1;

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
		if (func(cmd, data, error)) return 1;
		cmd += len;
	}
	return 0;
}

int prot_tag_post(char *cmd, prot_err_func_t error) {
	post_t *post = NULL;
	return prot_cmd_loop(cmd, &post, tag_post_cmd, error);
}
