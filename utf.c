#include "db.h"

#include <utf8proc.h>

int utf_fuzz(connection_t *conn, const char *str, char **res,
             unsigned int *res_len) {
	int flags = UTF8PROC_NULLTERM | UTF8PROC_STABLE  | UTF8PROC_DECOMPOSE |
	            UTF8PROC_IGNORE   | UTF8PROC_STRIPCC | UTF8PROC_CASEFOLD  |
	            UTF8PROC_STRIPMARK;
	ssize_t      ret;
	int32_t      *buf = NULL;
	unsigned int bufsize;
	unsigned int final_len;
	char         *ptr1, *ptr2;

	ret = utf8proc_decompose((const uint8_t *)str, 0, NULL, 0, flags);
	err1(ret < 0);
	bufsize = ret * sizeof(*buf) + 1;
	err1(c_alloc(conn, (void **)&buf, bufsize));
	ret = utf8proc_decompose((const uint8_t *)str, 0, buf, ret, flags);
	err1(ret < 0);
	ret = utf8proc_reencode(buf, ret, flags);
	err1(ret < 0);

	final_len = 1;
	ptr1 = (char *)buf;
	while (*ptr1) {
		if (*ptr1 != '-' && *ptr1 != '_' && *ptr1 != ' ') final_len++;
		ptr1++;
	}
	*res_len = final_len;
	err1(c_alloc(conn, (void **)res, final_len));
	ptr1 = (char *)buf;
	ptr2 = *res;
	while (*ptr1) {
		if (*ptr1 != '-' && *ptr1 != '_' && *ptr1 != ' ') *ptr2++ = *ptr1;
		ptr1++;
	}
	*ptr2 = '\0';
	c_free(conn, buf, bufsize);
	return 0;
err:
	if (buf) c_free(conn, buf, bufsize);
	return 1;
}
