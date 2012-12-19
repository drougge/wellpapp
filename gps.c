#include "db.h"

#include <math.h>

static const double R = 6371000; // Earth's radius in meters (approx)
static const double DEG2RAD = M_PI / 180;
static const double EXTRA_FUZZ = 0.00000000000002;

static double sphere_dist(const gps_pos_t a, const gps_pos_t b)
{
	const double d = acos(sin(a.lat) * sin(b.lat)
	                      + cos(a.lat) * cos(b.lat) * cos(b.lon - a.lon)
	                     );
	return isnormal(d) ? d : M_PI;
}

static double sphere_fuzz_lon(const double lat, const double fuzz)
{
	const double clat = cos(lat);
	if (clat == 0) return M_PI; // pole
	const double slat = sin(lat);
	return acos((cos(fuzz) - slat * slat) / (clat * clat));
}

TVC_PROTO(gps)
{
	(void) re;
	if (cmp == CMP_GT || cmp == CMP_GE) {
		const tag_value_t *tmp = a;
		a = b;
		b = tmp;
		cmp = (cmp == CMP_GT ? CMP_LT : CMP_LE);
	}
	if (cmp == CMP_LT || cmp == CMP_LE) {
		double f = a->fuzz.f_gps;
		double f_lon = sphere_fuzz_lon(a->val.v_gps.lat, f);
		gps_pos_t ga, gb;
		ga.lat = a->val.v_gps.lat - f;
		ga.lon = a->val.v_gps.lon - f_lon;
		f = b->fuzz.f_gps;
		f_lon = sphere_fuzz_lon(b->val.v_gps.lat, f);
		gb.lat = b->val.v_gps.lat + f;
		gb.lon = b->val.v_gps.lon + f_lon;
		if (cmp == CMP_LT) {
			return ga.lat < gb.lat && ga.lon < gb.lon;
		} else {
			return ga.lat <= gb.lat && ga.lon <= gb.lon;
		}
	}
	if (cmp != CMP_CMP) return 0;
	const double dist = sphere_dist(a->val.v_gps, b->val.v_gps);
	return dist <= a->fuzz.f_gps + b->fuzz.f_gps;
}

static int parse_coord(const char **val, double *r_pos, const double range,
                       const char e1, const char e2, const char e3)
{
	char *end;
	double v = strtod(*val, &end);
	if (*val == end) return 1;
	if (v > range || v < -range) return 1;
	if (*end != e1 && *end != e2 && *end != e3) return 1;
	*r_pos = v;
	*val = end;
	return 0;
}

int tv_parser_gps(const char *val, gps_pos_t *v, gps_fuzz_t *f)
{
	double tmp;
	err1(parse_coord(&val, &tmp, 90.0, ',', ',', ','));
	v->lat = tmp * DEG2RAD;
	val++;
	err1(parse_coord(&val, &tmp, 180.0, ',', '+', 0));
	v->lon = tmp * DEG2RAD;
	if (*val == ',') {
		val++;
		err1(parse_coord(&val, &tmp, 1e9, '+', 0, 0));
	}
	double fuzz = 0;
	if (*val) {
		err1(*(val++) != '+');
		err1(*(val++) != '-');
		err1(parse_coord(&val, &fuzz, 1e9, 0, 0, 0));
		err1(fuzz < 0);
	}
	*f = fuzz / R + EXTRA_FUZZ;
	return 0;
err:
	return 1;
}
