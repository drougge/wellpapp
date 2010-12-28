#include "db.h"

#include <netinet/in.h> /* For htonl */

static const char *guid_charset = "abcdefghkopqrstyABCDEFGHKLPQRSTY234567890";
#define GUID_BASE 41

extern const guid_t *server_guid;
extern uint32_t     *tag_guid_last;

static uint8_t guid_checksum(const guid_t guid, const guidtype_t what) {
	uint8_t sum = what;
	int i;
	for (i = 0; i < 16; i++) {
		if (i != 7) sum += guid.data_u8[i];
	}
	return sum;
}

guid_t guid_gen_tag_guid(void) {
	guid_t guid;
	guid = *server_guid;
	tag_guid_last[1]++;
	if (tag_guid_last[1] == 0) {
		tag_guid_last[0]++;
		assert(tag_guid_last[0]);
	}
	guid.data_u32[2] = htonl(tag_guid_last[0]);
	guid.data_u32[3] = htonl(tag_guid_last[1]);
	guid.data_u8[7] = guid_checksum(guid, GUIDTYPE_TAG);
	return guid;
}

void guid_update_last(guid_t guid) {
	for (int i = 0; i < 7; i++) {
		if (guid.data_u8[i] != server_guid->data_u8[i]) return;
	}
	if (ntohl(guid.data_u32[2]) > tag_guid_last[0]) {
		tag_guid_last[0] = ntohl(guid.data_u32[2]);
		tag_guid_last[1] = ntohl(guid.data_u32[3]);
		return;
	}
	if (ntohl(guid.data_u32[2]) == tag_guid_last[0]
	    && ntohl(guid.data_u32[3]) > tag_guid_last[1]) {
		tag_guid_last[1] = ntohl(guid.data_u32[3]);
		return;
	}
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

static int guid_is_valid_something(const guid_t guid, guidtype_t what) {
	return guid.data_u8[7] == guid_checksum(guid, what);
}

int guid_str2guid(guid_t *res_guid, const char *str, guidtype_t type) {
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
	return !guid_is_valid_something(*res_guid, type);
}

int guid_is_valid_server_guid(const guid_t guid) {
	return guid_is_valid_something(guid, GUIDTYPE_SERVER)
	       && guid.data_u32[2] == 0 && guid.data_u32[3] == 0;
}

int guid_is_valid_tag_guid(const guid_t guid, int must_be_local) {
	if (!guid_is_valid_something(guid, GUIDTYPE_TAG)) return 0;
	if (must_be_local) {
		int i;
		for (i = 0; i < 7; i++) {
			if (guid.data_u8[i] != server_guid->data_u8[i]) return 0;
		}
	}
	return 1;
}
