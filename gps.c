#include "db.h"

#include <math.h>

TVC_PROTO(gps)
{
	int rlat = tvc_gpslat(a, cmp, b, re);
	int rlon = tvc_gpslon(a, cmp, b, re);
	if (rlat == rlon) return rlat;
	switch (cmp) {
		case CMP_GT: // Fall through
		case CMP_GE: // Fall through
		case CMP_LT: // Fall through
		case CMP_LE: // Fall through
		case CMP_CMP:
			if (rlat) return rlat;
			return rlon;
			break;
		default:
			return 0;
			break;
	}
}

static int parse_fixed(const char *val, int32_t range, char **end, int32_t *r)
{
	int32_t a;
	a = strtol(val, end, 10);
	err1(a < -range || a > range);
	a *= 10000000;
	if (**end == '.') {
		long long b = strtoll(*end + 1, end, 10);
		err1(b < 0);
		if (b) {
			while (b >= 10000000) b /= 10;
			while (b * 10 < 10000000) b *= 10;
			a += b;
		}
	}
	*r = a;
	return 0;
err:
	return 1;
}

static int parse_coord(const char *val, int32_t *r_pos, int32_t *r_fuzz,
                       int32_t range)
{
	char *end;
	err1(parse_fixed(val, range, &end, r_pos));
	if (*end != ',' && *end) {
		err1(*end != '+');
		err1(parse_fixed(end + 1, range, &end, r_fuzz));
	} else {
		*r_fuzz = 0;
	}
	err1(*end && *end != ',');
	return 0;
err:
	return 1;
}

int tv_parser_gps(const char *val, gps_pos_t *v, gps_fuzz_t *f)
{
	const char *divider = strchr(val, ',');
	err1(!divider);
	const char *lon = divider + 1;
	err1(parse_coord(val, &v->lat, &f->lat, 90));
	err1(parse_coord(lon, &v->lon, &f->lon, 180));
	divider = strchr(lon, ',');
	if (divider) {
		const char *ele = divider + 1;
		err1(strchr(ele, ','));
		int32_t a, b;
		err1(parse_coord(ele, &a, &b, 0xfffff));
	}
	return 0;
err:
	return 1;
}
