#include "db.h"

#include <utf8proc.h>

static char fuzz_ignore[127] = { 0 };

static void init_ignore(void)
{
	for (int i = 0; i < 32; i++) fuzz_ignore[i] = 1;
	fuzz_ignore[' '] = 1;
	fuzz_ignore['-'] = 1;
	fuzz_ignore['_'] = 1;
	fuzz_ignore['('] = 1;
	fuzz_ignore[')'] = 1;
	fuzz_ignore['['] = 1;
	fuzz_ignore[']'] = 1;
	fuzz_ignore['{'] = 1;
	fuzz_ignore['}'] = 1;
	fuzz_ignore['.'] = 1;
	fuzz_ignore[','] = 1;
	fuzz_ignore['!'] = 1;
	fuzz_ignore['/'] = 1;
	fuzz_ignore['"'] = 1;
	fuzz_ignore['\''] = 1;
	fuzz_ignore['?'] = 1;
	fuzz_ignore['<'] = 1;
	fuzz_ignore['>'] = 1;
	fuzz_ignore['@'] = 1;
	fuzz_ignore['='] = 1;
	fuzz_ignore['+'] = 1;
	fuzz_ignore['%'] = 1;
	fuzz_ignore['$'] = 1;
	fuzz_ignore['#'] = 1;
	fuzz_ignore['|'] = 1;
	fuzz_ignore['\\'] = 1;
}

static int utf_fuzz_i1(const char *str, unsigned char **res_buf,
                       unsigned int *res_len)
{
	int flags = UTF8PROC_NULLTERM | UTF8PROC_STABLE  | UTF8PROC_DECOMPOSE |
	            UTF8PROC_IGNORE   | UTF8PROC_STRIPCC | UTF8PROC_CASEFOLD  |
	            UTF8PROC_STRIPMARK;
	ssize_t       ret;
	int32_t       *buf = NULL;
	unsigned int  bufsize;
	unsigned int  final_len;
	unsigned char *ptr;

	ret = utf8proc_decompose((const uint8_t *)str, 0, NULL, 0, flags);
	err1(ret < 0);
	bufsize = ret * sizeof(*buf) + 1;
	buf = malloc(bufsize);
	err1(!buf);
	ret = utf8proc_decompose((const uint8_t *)str, 0, buf, ret, flags);
	err1(ret < 0);
	ret = utf8proc_reencode(buf, ret, flags);
	err1(ret < 0);

	final_len = 1;
	ptr = (unsigned char *)buf;
	if (!fuzz_ignore[0]) init_ignore();
	while (*ptr) {
		if (*ptr > 127 || !fuzz_ignore[*ptr]) final_len++;
		ptr++;
	}
	*res_len = final_len;
	*res_buf = (unsigned char *)buf;
	return 0;
err:
	if (buf) free(buf);
	return 1;
}

static void utf_fuzz_i2(unsigned char *buf, char *res)
{
	unsigned char *ptr1 = buf;
	unsigned char *ptr2 = (unsigned char *)res;
	while (*ptr1) {
		if (*ptr1 > 127 || !fuzz_ignore[*ptr1]) *ptr2++ = *ptr1;
		ptr1++;
	}
	*ptr2 = '\0';
}

int utf_fuzz_c(connection_t *conn, const char *str, char **res,
               unsigned int *res_len)
{
	unsigned char *buf = NULL;
	int            r;

	r = utf_fuzz_i1(str, &buf, res_len);
	if (!r) {
		r = c_alloc(conn, (void **)res, *res_len);
		if (!r) utf_fuzz_i2(buf, *res);
	}
	if (buf) free(buf);
	return r;
}

const char *utf_fuzz_mm(const char *str)
{
	unsigned char *buf = NULL;
	char          *res;
	int            r;
	unsigned int  res_len;

	r = utf_fuzz_i1(str, &buf, &res_len);
	assert(!r);
	res = mm_alloc_s(res_len);
	utf_fuzz_i2(buf, res);
	free(buf);
	return res;
}
