#include "db.h"

#include <md5.h>
#include <netinet/in.h> /* For htonl */

static const char *guid_charset = "abcdefghkopqrstyABCDEFGHKLPQRSTY234567890";
#define GUID_BASE 41

extern const guid_t server_guid;
extern uint32_t     *tag_guid_last;

static void guid_checksum(const guid_t guid, const char *what, unsigned char *res) {
	unsigned char digest[16];
	MD5_CTX ctx;
	MD5Init(&ctx);
	MD5Update(&ctx, what, strlen(what));
	MD5Update(&ctx, guid.data, sizeof(guid.data));
	MD5Final(digest, &ctx);
	memcpy(res, digest, 4);
}

guid_t guid_gen_tag_guid(void) {
	guid_t guid;
	guid = server_guid;
	tag_guid_last[1]++;
	if (tag_guid_last[1] == 0) {
		tag_guid_last[0]++;
		assert(tag_guid_last[0]);
	}
	guid.data_u32[1] = htonl(tag_guid_last[0]);
	guid.data_u32[2] = htonl(tag_guid_last[1]);
	guid_checksum(guid, "TAG", guid.check);
	return guid;
}

static int guid_c2i(int c) {
	int i;
	for (i = 0; guid_charset[i]; i++) {
		if (guid_charset[i] == c) return i;
	}
	return -1;
}

static void guid_int2str(uint32_t val, char *str) {
	int i;
	for (i = 5; i >= 0; i--) {
		str[i] = guid_charset[val % GUID_BASE];
		val   /= GUID_BASE;
	}
	assert(val == 0);
}

const char *guid_guid2str(guid_t guid) {
	static char buf[7*4];
	char        *strp = buf;
	int         i;

	for (i = 0; i < 4; i++) {
		guid_int2str(ntohl(((uint32_t *)&guid)[i]), strp);
		strp[6] = '-';
		strp += 7;
	}
	strp[-1] = '\0';
	return buf;
}

int guid_str2guid(guid_t *res_guid, const char *str) {
	uint32_t val[4] = {0, 0, 0, 0};
	uint32_t pval;
	int      consumed = 0;
	int      valnr = 0;

	while (*str) {
		int  i;
		char c = *str++;
		if (c == '-') {
			if (consumed != 6) return 1;
			consumed = 0;
			valnr++;
			if (valnr == 4) return 1;
		} else {
			i = guid_c2i(c);
			consumed++;
			pval = val[valnr];
			val[valnr] = val[valnr] * GUID_BASE + i;
			if (val[valnr] / GUID_BASE != pval) return 1;
		}
	}
	if (consumed != 6 || valnr != 3) return 1;
	for (valnr = 0; valnr < 4; valnr++) {
		unsigned char *p = ((unsigned char *)res_guid) + valnr * 4 + 3;
		uint32_t      v = val[valnr];
		int           i;
		for (i = 0; i < 4; i++) {
			*p-- = v;
			v >>= 8;
		}
		assert(v == 0);
	}
	return 0;
}

static int guid_is_valid_something(const guid_t guid, const char *what) {
	unsigned char csum[4];
	guid_checksum(guid, what, csum);
	return !memcmp(csum, guid.check, sizeof(guid.check));
}

int guid_is_valid_server_guid(const guid_t guid) {
	return guid_is_valid_something(guid, "SERVER")
	       && guid.data_u32[1] == 0 && guid.data_u32[2] == 0;
}

int guid_is_valid_tag_guid(const guid_t guid, int must_be_local) {
	if (!guid_is_valid_something(guid, "TAG")) return 0;
	return !must_be_local || guid.data_u32[0] == server_guid.data_u32[0];
}