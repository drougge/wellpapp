#include "db.h"

#include <math.h>

static const int step_size[] = {365*24*60*60, 30.5*24*60*60, 24*60*60, 60*60};

static int tvc_datetime_step(tag_value_t *a, tagvalue_cmp_t cmp,
                             tag_value_t *b, int inner)
{
	if (a->val.v_datetime.simple) {
		tag_value_t fa;
		fa.val.v_int = datetime_get_simple(&a->val.v_datetime);
		fa.fuzz.f_int = a->fuzz.f_datetime.d_fuzz;
		if (inner) {
			return tvc_int(b, cmp, &fa, NULL);
		} else {
			return tvc_datetime_step(b, cmp, &fa, 1);
		}
	} else {
		struct tm tm;
		int *field[] = {&tm.tm_year, &tm.tm_mon, &tm.tm_mday,
		                &tm.tm_hour, &tm.tm_min, &tm.tm_sec};
		memset(&tm, 0, sizeof(tm));
		tm.tm_year = a->val.v_datetime.year;
		tm.tm_mon  = a->val.v_datetime.month;
		for (int i = 2; i < arraylen(field); i++) {
			*field[i] = a->val.v_datetime.data.field[i - 2];
		}
		struct tm tm2;
		memcpy(&tm2, &tm, sizeof(tm));
		tag_value_t fa;
		fa.fuzz.f_int = a->fuzz.f_datetime.d_fuzz;
		for (int f = 0; f < 4; f++) {
			int fuzz = a->fuzz.f_datetime.d_step[f];
			int start = fuzz < 0 ? fuzz : 0;
			int stop = abs(fuzz);
			for (int i = start; i <= stop; i++) {
				memcpy(&tm, &tm2, sizeof(tm));
				*field[f] += i;
				fa.val.v_int = mktime(&tm);
				int r;
				if (inner) {
					r = tvc_int(b, cmp, &fa, NULL);
				} else {
					r = tvc_datetime_step(b, cmp, &fa, 1);
				}
				if (r) return 1;
			}
		}
		return 0;
	}
}

int tvc_datetime(tag_value_t *a, tagvalue_cmp_t cmp, tag_value_t *b,
                 regex_t *re)
{
	(void) re;
	return tvc_datetime_step(a, cmp, b, 0);
}

static int tvp_datetimefuzz(const char *val, double *r, char *r_unit)
{
	if (!*val) return 1;
	char *end;
	double fuzz = fractod(val, &end);
	if (val == end) fuzz = 1.0; // For "+-H" etc
	if (fuzz < 0.0) return 1;
	if (*end) {
		if (end[1]) return 1;
		*r_unit = *end;
		switch (*end) {
			case 'Y':
				fuzz *= 12.0;
			case 'm':
				fuzz *= 30.5;
			case 'd':
				fuzz *= 24.0;
			case 'H':
				fuzz *= 60.0;
			case 'M':
				fuzz *= 60.0;
			case 'S':
				break;
			default:
				return 1;
				break;
		}
	} else {
		*r_unit = 0;
	}
	*r = fuzz;
	return 0;
}

int tvp_timezone(const char *val, int *r_offset, const char **r_end)
{
	switch (*val) {
		case '+':
		case '-':
			for (int i = 1; i <= 4; i++) {
				if (val[i] < '0' || val[i] > '9') return 1;
			}
			int hour = (val[1] - '0') * 10 + val[2] - '0';
			int minute = (val[3] - '0') * 10 + val[4] - '0';
			if (hour >= 12 || minute >= 60) return 1;
			int offset = (hour * 60 + minute) * 60;
			if (*val == '+') offset = -offset;
			*r_offset = offset;
			*r_end = val + 5;
			break;
		case 'Z': // UTC
			*r_offset = 0;
			*r_end = val + 1;
			break;
		default:
			return 1;
			break;
	}
	return 0;
}

int tv_parser_datetime(const char *val, datetime_time_t *v, datetime_fuzz_t *f,
                       tagvalue_cmp_t cmp)
{
	struct tm tm;
	int *field[] = {&tm.tm_year, &tm.tm_mon, &tm.tm_mday,
	                &tm.tm_hour, &tm.tm_min, &tm.tm_sec};
	int chk[] = {0, 0, 0, 0, 0, 0};
	int range[] = {INT_MAX, 12, 31, 23, 59, 59};
	char sep[] = "--T::";
	assert(arraylen(field) == arraylen(range));
	assert(arraylen(field) == arraylen(sep));
	assert(arraylen(field) == arraylen(chk));
	int pos = 0;
	int with_steps = 0;
	const char *ptr = val;
	memset(f, 0, sizeof(*f));
	memset(&tm, 0, sizeof(tm));
	tm.tm_mon  = 1;
	tm.tm_mday = 1;
	while (*ptr && pos < arraylen(field)) {
		if (*ptr < '0' || *ptr > '9') return 1;
		char *end;
		long long el = strtoll(ptr, &end, 10);
		if (el > range[pos] || el < 0) return 1;
		if (pos < 3 && !el) return 1;
		if (pos > 0 && end - ptr != 2) return 1;
		if (pos < 4 && *end == '+') {
			long long pf = strtoll(end + 1, &end, 10);
			if (pf < -127 || pf > 127) return 1;
			f->d_step[pos] = pf;
			with_steps = 1;
		}
		*field[pos] = chk[pos] = el;
		ptr = end;
		pos++;
		if (*end == 0) break;
		if (*end == '+') break;
		if (pos < 5 && *end != sep[pos - 1]) return 1;
		ptr++;
	}
	if (!pos) return 1;
	tm.tm_year -= 1900; chk[0] -= 1900;
	tm.tm_mon--; chk[1]--;
	time_t unixtime = mktime(&tm);
	// So I can't say 1969-12-31T23:59:59 then? Bloody POSIX.
	if (unixtime == (time_t)-1) return 1;
	// Check that mktime hasn't needed to normalise the time
	for (int i = 0; i < arraylen(field); i++) {
		if (*field[i] != chk[i]) return 1;
	}
	// If the time was not fully specified, there's an implicit fuzz.
	// If there are no steps, this is calculated now, otherwise we
	// need to redo it for every step.
	int implfuzz = 0;
	if (!with_steps && pos < arraylen(field)) {
		struct tm tm2;
		int *field2[] = {&tm2.tm_year, &tm2.tm_mon, &tm2.tm_mday,
		                 &tm2.tm_hour, &tm2.tm_min, &tm2.tm_sec};
		assert(arraylen(field) == arraylen(field2));
		for (int i = 0; i < pos; i++) {
			*field2[i] = *field[i];
		}
		for (int i = pos; i < arraylen(field); i++) {
			*field2[i] = range[i];
		}
		if (pos == 2) { // year + month specified
			// "day 0" of next month
			tm2.tm_mon++;
			tm2.tm_mday = 0;
		}
		time_t t2 = mktime(&tm2);
		if (t2 == (time_t)-1) return 1;
		implfuzz = (t2 - unixtime) / 2;
	}
	unixtime += implfuzz;
	char f_u;
	double f_v;
	if (ptr[0] == '+' && ptr[1] == '-') { // Only fuzz
		unixtime += default_timezone;
		if (tvp_datetimefuzz(ptr + 2, &f_v, &f_u)) return 1;
	} else if (*ptr) { // Must be a timezone
		int offset;
		if (tvp_timezone(ptr, &offset, &ptr)) return 1;
		unixtime += offset;
		if (*ptr) {
			if (ptr[0] != '+' || ptr[1] != '-') return 1;
			if (tvp_datetimefuzz(ptr + 2, &f_v, &f_u)) return 1;
		}
	} else {
		unixtime += default_timezone;
		f_u = 0;
		f_v = 0.0;
	}
	if (!f_u && implfuzz) f_v *= implfuzz * 2;
	if (cmp == CMP_GT) {
		unixtime = unixtime + implfuzz - f_v;
		f_v = 0.0;
		f->d_step[0] = f->d_step[1] = f->d_step[2] = f->d_step[3] = 0;
		with_steps = 0;
	} else if (cmp == CMP_LT) {
		unixtime = unixtime - implfuzz + f_v;
		f_v = 0.0;
		for (int i = 0; i < 4; i++) {
			unixtime += f->d_step[i] * step_size[i];
			f->d_step[i] = 0;
		}
		with_steps = 0;
	} else {
		f_v += implfuzz;
	}
	datetime_set_simple(v, unixtime);
	if (with_steps || datetime_get_simple(v) != unixtime) {
		v->simple = 0;
		v->year = tm.tm_year;
		v->month = tm.tm_mon;
		for (int i = 2; i < arraylen(field); i++) {
			v->data.field[i - 2] = *field[i];
		}
	}
	f->d_fuzz = ceil(f_v);
	return 0;
}

time_t datetime_get_simple(const datetime_time_t *val)
{
	assert(val->simple);
	return (int64_t)val->year << 32 | val->data.simple_part;
}

void datetime_set_simple(datetime_time_t *val, time_t simple)
{
	val->simple = 1;
	val->year = (int64_t)simple >> 32;
	val->data.simple_part = simple;
}

void datetime_strfix(tag_value_t *val)
{
	struct tm tm;
	time_t ttime = datetime_get_simple(&val->val.v_datetime);
	assert (val->val.v_datetime.simple);
	gmtime_r(&ttime, &tm);
	char buf[128];
	int l = strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
	if (val->fuzz.f_int) {
		unsigned long long pv = val->fuzz.f_int;
		static const int scale[] = {60, 60, 24, 0};
		static const char suff[] = "SMHd";
		int sp = 0;
		while (sp < 3 && pv % scale[sp] == 0) {
			pv /= scale[sp++];
		}
		snprintf(buf + l, sizeof(buf) - l, "+-%llu%c", pv, suff[sp]);
	}
	val->v_str = mm_strdup(buf);
}
