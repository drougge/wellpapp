#include "db.h"

#include <math.h>

static const int step_size[] = {365*24*60*60, 30.5*24*60*60, 24*60*60, 60*60};

static int tvc_datetime_step(tag_value_t *a, int64_t val, tagvalue_cmp_t cmp,
                             tag_value_t *b, int step, int inner)
{
	const int ss = step_size[step];
	const int stop = a->fuzz.f_datetime.d_step[step];
	for (int s = -1; s < stop; s++) {
		int r;
		if (step < 3) {
			r = tvc_datetime_step(a, val, cmp, b, step + 1, inner);
		} else {
			tag_value_t fake;
			fake.val.v_int = val;
			fake.fuzz.f_int = a->fuzz.f_datetime.d_fuzz;
			if (inner) {
				r = tvc_int(b, cmp, &fake, NULL);
			} else {
				r = tvc_datetime_step(b, b->val.v_int, cmp,
				                      &fake, 0, 1);
			}
		}
		if (r) return 1;
		val += ss;
	}
	return 0;
}

int tvc_datetime(tag_value_t *a, tagvalue_cmp_t cmp, tag_value_t *b,
                 regex_t *re)
{
	(void) re;
	return tvc_datetime_step(a, a->val.v_int, cmp, b, 0, 0);
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

int tv_parser_datetime(const char *val, int64_t *v, datetime_fuzz_t *f,
                       tagvalue_cmp_t cmp)
{
	struct tm tm;
	int *field[] = {&tm.tm_year,
	                &tm.tm_mon,
	                &tm.tm_mday,
	                &tm.tm_hour,
	                &tm.tm_min,
	                &tm.tm_sec,
	               };
	int range[] = {INT_MAX, 12, 31, 23, 59, 59};
	char sep[] = "--T::";
	assert(arraylen(field) == arraylen(range));
	assert(arraylen(field) == arraylen(sep));
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
			const char plusminus = end[1];
			ptr = (plusminus == '-') ? end + 2 : end + 1;
			long long pf = strtoll(ptr, &end, 10);
			if (pf <= 0 || pf > 255) return 1;
			if (plusminus == '-') {
				el -= pf;
				pf = pf * 2 + 1;
				if (pf > 255) return 1;
			}
			f->d_step[pos] = pf;
			with_steps = 1;
		}
		*field[pos] = el;
		ptr = end;
		pos++;
		if (*end == 0) break;
		if (*end == '+') break;
		if (pos < 5 && *end != sep[pos - 1]) return 1;
		ptr++;
	}
	if (!pos) return 1;
	tm.tm_year -= 1900;
	if (pos > 0) tm.tm_mon--;
	time_t unixtime = mktime(&tm);
	// So I can't say 1969-12-31T23:59:59 then? Bloody POSIX.
	if (unixtime == (time_t)-1) return 1;
	// Check that what was parsed matches the input (after mktime
	// normalises it), if possible (no steps).
	if (!with_steps) {
		char fmt[] = "%Y-%m-%dT%H:%M:%S";
		char buf[64];
		assert(arraylen(field) * 3 == sizeof(fmt));
		fmt[pos * 3 - 1] = 0;
		int len = strftime(buf, sizeof(buf), fmt, &tm);
		if (memcmp(buf, val, len)) return 1;
	}
	// If the time was not fully specified, there's an implicit fuzz.
	int pos2f[] = {30 * 60 * 24 * 365, 30 * 60 * 24 * 30.5,
	               30 * 60 * 24, 30 * 60, 30, 0};
	// Stupid leap years.
	int y = tm.tm_year + 1900;
	if (y % 4 == 0 && (y % 400 == 0 || y % 100)) pos2f[0] += 30*60*24;
	int implfuzz = pos2f[pos - 1];
	// Move the time forward to middle of its' possible range.
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
	} else if (cmp == CMP_LT) {
		unixtime = unixtime - implfuzz + f_v;
		f_v = 0.0;
		for (int i = 0; i < 4; i++) {
			unixtime += f->d_step[i] * step_size[i];
			f->d_step[i] = 0;
		}
	} else {
		f_v += implfuzz;
	}
	*v = unixtime;
	f->d_fuzz = ceil(f_v);
	return 0;
}

void datetime_strfix(tag_value_t *val)
{
	struct tm tm;
	time_t ttime = val->val.v_int;
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
