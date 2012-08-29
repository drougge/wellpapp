#include "db.h"

// This is just sanity checking, no strict limit is needed.
// (Except PROT_MAXLEN limits it anyway.)
#define STR_MAXLEN 2047

static const unsigned char alpha[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijk"
                                     "lmnopqrstuvwxyz0123456789_-";
static const unsigned char rev[] = {
	 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
	 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
	 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0, 63,  0,  0, 52, 53, 54,
	55, 56, 57, 58, 59, 60, 61,  0,  0,  0,  0,  0,  0,  0,  0,  1,  2,
	 3,  4,  5,  6,  7,  8,  9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
	20, 21, 22, 23, 24, 25,  0,  0,  0,  0, 62,  0, 26, 27, 28, 29, 30,
	31, 32, 33, 34, 35, 36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47,
	48, 49, 50, 51,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
	 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
	 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
	 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
	 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
	 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
	 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
	 0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,  0,
	 0
};
/*
for (i = 0; i < 64; i++) {
	rev[alpha[i]] = i;
}
*/

#define GETONE(s) ((uint32_t)(*s ? (unsigned char)*s++ : 0))
char *str_str2enc(const char *str)
{
	unsigned int len = strlen(str);
	assert(len <= STR_MAXLEN);
	char *res = malloc(len + len / 3 + 4);
	char *ptr = res;
	while (*str) {
		uint32_t n = GETONE(str) << 16;
		n |= GETONE(str) << 8;
		n |= GETONE(str);
		*ptr++ = alpha[(n >> 18) & 63];
		*ptr++ = alpha[(n >> 12) & 63];
		*ptr++ = alpha[(n >>  6) & 63];
		*ptr++ = alpha[(n      ) & 63];
	}
	*ptr = '\0';
	return res;
}

const char *str_enc2str(const char *enc)
{
	const char  *chk = enc;
	while (*chk) {
		const unsigned char c = *chk++;
		if (!rev[c] && c != 65) return NULL;
	}
	unsigned int len = chk - enc;
	assert(chk - enc == len);
	if (len % 4) return NULL;
	len = len / 4 * 3;
	if (len > STR_MAXLEN) return NULL;
	char *res = mm_alloc_s(len + 1);

	char *ptr = res;
	while (*enc) {
		uint32_t n = rev[GETONE(enc)] << 18;
		n |= rev[GETONE(enc)] << 12;
		n |= rev[GETONE(enc)] << 6;
		n |= rev[GETONE(enc)];
		*ptr++ = (n >> 16) & 255;
		*ptr++ = (n >>  8) & 255;
		*ptr++ = (n      ) & 255;
	}
	*ptr = '\0';
	return res;
}
