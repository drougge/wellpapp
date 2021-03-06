#include "db.h"

#include <math.h>

/* Standard time functions tend to only work in a fairly narrow window
 * around 1970 on 32bit platforms, which is not acceptable here.
 */

static __attribute__((__const__)) int is_leap_year(const int year)
{
	if (year % 400 == 0) return 1;
	if (year % 100 == 0) return 0;
	if (year % 4 == 0) return 1;
	return 0;
}

static const int std_mlen[]  = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
static const int leap_mlen[] = {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
static const int * const mlen[] = {std_mlen, leap_mlen};
static const int ylen_s[] = {365 * 24 * 60 * 60, 366 * 24 * 60 * 60};

#define MLEN(year, mon) mlen[is_leap_year(year)][mon]
#define YLEN(year) ylen_s[is_leap_year(year)]

static inline void clamp(int *a, int *b, const int max)
{
	*b += *a / max;
	*a %= max;
}

static int fixed_mktime(const struct tm *tm, int64_t *res)
{
	int sec = tm->tm_sec;
	int min = tm->tm_min;
	int hour = tm->tm_hour;
	int day = tm->tm_mday;
	int mon = tm->tm_mon;
	int year = tm->tm_year + 1900;
	
	clamp(&sec, &min, 60);
	clamp(&min, &hour, 60);
	clamp(&hour, &day, 24);
	clamp(&mon, &year, 12);
	
	day--; // one-indexed to zero-indexed
	while (day < 0) {
		mon--;
		clamp(&mon, &year, 12);
		day += MLEN(year, mon);
	}
	while (day >= MLEN(year, mon)) {
		day -= MLEN(year, mon);
		mon++;
		clamp(&mon, &year, 12);
	}
	
	for (int i = 0; i < mon; i++) {
		day += MLEN(year, i);
	}
	*res = ((day * 24 + hour) * 60 + min) * 60 + sec;
	while (year > 1970) {
		*res += YLEN(--year);
	}
	while (year < 1970) {
		*res -= YLEN(year++);
	}
	
	return 0;
}

static int fixed_gmtime(int64_t t, struct tm *res)
{
	int year = 1970, mon = 0, day, hour, min, sec;
	while (t >= YLEN(year)) {
		t -= YLEN(year++);
	}
	while (t < 0) {
		t += YLEN(year--);
	}
	sec = t % 60;
	min = t / 60;
	hour = min / 60;
	min %= 60;
	day = hour / 24;
	hour %= 24;
	while (day >= MLEN(year, mon)) {
		day -= MLEN(year, mon++);
	}
	res->tm_year = year - 1900;
	res->tm_mon = mon;
	res->tm_mday = day + 1;
	res->tm_hour = hour;
	res->tm_min = min;
	res->tm_sec = sec;
	return 0;
}

static const int range[] = {INT_MAX, 12, 31, 23, 59, 59};

static int64_t dt_step_end(int valid_steps, struct tm tm)
{
	int *field[] = {&tm.tm_year, &tm.tm_mon, &tm.tm_mday,
			&tm.tm_hour, &tm.tm_min, &tm.tm_sec};
	for (int i = valid_steps; i < arraylen(field); i++) {
		*field[i] = range[i];
	}
	if (valid_steps == 1) { // just year
		// Range is 12, but mon is zero indexed (but mday isn't, sigh).
		tm.tm_mon = 11;
	}
	if (valid_steps == 2) { // year + month specified
		// "day 0" of next month
		tm.tm_mon++;
		tm.tm_mday = 0;
	}
	int64_t res;
	fixed_mktime(&tm, &res);
	return res;
}

static int tvc_datetime_step(const tag_value_t *a, tagvalue_cmp_t cmp,
                             const tag_value_t *b, int inner)
{
	if (!a->val.v_datetime.valid_steps) {
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
		int pos = a->val.v_datetime.valid_steps;
		int tz_offset = a->val.v_datetime.tz_mins * 60;
		int64_t psf = a->fuzz.f_datetime.d_fuzz;
		int64_t nsf = 0;
		if (psf < 0) {
			nsf = psf;
			psf *= -2;
		}
		for (int f = 0; f < 4; f++) {
			int fuzz = a->fuzz.f_datetime.d_step[f];
			int start = fuzz < 0 ? fuzz : 0;
			int stop = abs(fuzz);
			for (int i = start; i <= stop; i++) {
				memcpy(&tm, &tm2, sizeof(tm));
				*field[f] += i;
				int64_t unixtime;
				fixed_mktime(&tm, &unixtime);
				int implfuzz = 0;
				if (pos < 6) {
					int64_t t2 = dt_step_end(pos, tm);
					implfuzz = t2 - unixtime;
				}
				fa.val.v_int = unixtime + nsf + tz_offset;
				fa.fuzz.f_int = psf + implfuzz;
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

static int64_t dt_make_simple(const datetime_time_t *val)
{
	if (!val->valid_steps) return datetime_get_simple(val);
	struct tm tm;
	int *field[] = {&tm.tm_mday, &tm.tm_hour, &tm.tm_min, &tm.tm_sec};
	memset(&tm, 0, sizeof(tm));
	tm.tm_year = val->year;
	tm.tm_mon  = val->month;
	for (int i = 0; i < arraylen(field); i++) {
		*field[i] = val->data.field[i];
	}
	int64_t res;
	fixed_mktime(&tm, &res);
	return res;
}

int tvc_datetime(const tag_value_t *a, tagvalue_cmp_t cmp,
                 const tag_value_t *b, regex_t *re)
{
	(void) re;
	if (cmp == CMP_CMP) {
		int64_t av = dt_make_simple(&a->val.v_datetime);
		int64_t bv = dt_make_simple(&b->val.v_datetime);
		if (av < bv) return -1;
		if (av > bv) return 1;
		return 0;
	}
	return tvc_datetime_step(a, cmp, b, 0);
}

static int tvp_datetimefuzz(const char *val, double *r, char *r_unit)
{
	if (!*val) return 1;
	char *end;
	double fuzz = fractod(val, &end);
	if (val == end) { // For "+-H" etc
		if (*end == '-') {
			fuzz = -1.0;
			end++;
		} else {
			fuzz = 1.0;
		}
	}
	if (*end) {
		if (end[1]) return 1;
		*r_unit = *end;
		switch (*end) {
			case 'Y':
				fuzz *= 12.0;
				//-fallthrough
			case 'm':
				fuzz *= 30.5;
				//-fallthrough
			case 'd':
				fuzz *= 24.0;
				//-fallthrough
			case 'H':
				fuzz *= 60.0;
				//-fallthrough
			case 'M':
				fuzz *= 60.0;
				//-fallthrough
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

int tvp_timezone(const char *val, int *len, int *r_offset)
{
	if (*len <= 0) return 1;
	if (val[*len - 1] == 'Z') {
		*len -= 1;
		*r_offset = 0;
		return 0;
	}
	if (*len < 5) return 1;
	val += *len - 5;
	if (*val != '+' && *val != '-') return 1;
	for (int i = 1; i <= 4; i++) {
		if (val[i] < '0' || val[i] > '9') return 1;
	}
	int hour = (val[1] - '0') * 10 + val[2] - '0';
	int minute = (val[3] - '0') * 10 + val[4] - '0';
	if (hour >= 12 || minute >= 60) return 1;
	int offset = (hour * 60 + minute) * 60;
	if (*val == '+') offset = -offset;
	*r_offset = offset;
	*len -= 5;
	return 0;
}

int tv_parser_datetime(const char *val, datetime_time_t *v, datetime_fuzz_t *f,
                       tagvalue_cmp_t cmp)
{
	int len = strlen(val);
	if (!len) return 1;
	char valc[len + 1];
	memcpy(valc, val, len);
	valc[len] = 0;
	char f_unit = 0;
	double f_val = 0.0;
	int tz_offset = 0;
	if (tvp_timezone(valc, &len, &tz_offset)) {
		tz_offset = default_timezone;
	}
	if (len <= 0) return 1;
	valc[len] = 0;
	char last_c = valc[len - 1];
	int fuzz_pos = 0;
	if (last_c >= '9') {
		while (--len) {
			if (valc[len] == '+') {
				fuzz_pos = len;
				break;
			}
		}
	}
	if (fuzz_pos) {
		if (tvp_datetimefuzz(valc + fuzz_pos + 1, &f_val, &f_unit)) {
			return 1;
		}
		valc[fuzz_pos] = 0;
		len = fuzz_pos;
	} else {
		len = strlen(valc);
	}
	const char *ptr = valc;
	struct tm tm;
	int *field[] = {&tm.tm_year, &tm.tm_mon, &tm.tm_mday,
	                &tm.tm_hour, &tm.tm_min, &tm.tm_sec};
	int chk[] = {0, 1, 1, 0, 0, 0};
	char sep[] = "--T::";
	static_assert(arraylen(field) == arraylen(range), "length mismatch");
	static_assert(arraylen(field) == arraylen(sep), "length mismatch");
	static_assert(arraylen(field) == arraylen(chk), "length mismatch");
	int pos = 0;
	int with_steps = 0;
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
		if (pos < 6 && *end != sep[pos - 1]) return 1;
		ptr++;
	}
	if (!pos) return 1;
	if (*ptr) {
		if (fuzz_pos || *ptr != '+') return 1;
		if (tvp_datetimefuzz(ptr + 1, &f_val, &f_unit)) {
			return 1;
		}
	}
	tm.tm_year -= 1900; chk[0] -= 1900;
	tm.tm_mon--; chk[1]--;
	int64_t unixtime;
	if (fixed_mktime(&tm, &unixtime)) return 1;
	// Check that tm was already normalised
	fixed_gmtime(unixtime, &tm);
	for (int i = 0; i < arraylen(field); i++) {
		if (*field[i] != chk[i]) return 1;
	}
	// If the time was not fully specified, there's an implicit fuzz.
	// If there are no steps, this is calculated now, otherwise we
	// need to redo it for every step.
	// Steps don't count for GT/LT comparisons.
	int implfuzz = 0;
	if ((!with_steps || (cmp >= CMP_GT && cmp <= CMP_LE))
	    && pos < arraylen(field)
	   ) {
		int64_t t2 = dt_step_end(pos, tm);
		if (t2 == -1) return 1;
		implfuzz = t2 - unixtime;
		pos = 6;
	}
	if (!f_unit && implfuzz) f_val *= implfuzz;
	if (cmp == CMP_GT || cmp == CMP_GE) {
		for (int i = 0; i < 4; i++) {
			if (f->d_step[i] < 0) *field[i] += f->d_step[i];
			f->d_step[i] = 0;
		}
		fixed_mktime(&tm, &unixtime);
		unixtime -= f_val;
		if (cmp == CMP_GT) unixtime += implfuzz;
		f_val = 0.0;
		implfuzz = 0;
		with_steps = 0;
	} else if (cmp == CMP_LT || cmp == CMP_LE) {
		for (int i = 0; i < 4; i++) {
			*field[i] += abs(f->d_step[i]);
			f->d_step[i] = 0;
		}
		fixed_mktime(&tm, &unixtime);
		unixtime += f_val;
		if (cmp == CMP_LE) unixtime += implfuzz;
		f_val = 0.0;
		implfuzz = 0;
		with_steps = 0;
	}
	// Apply the timezone.
	if (tz_offset && !with_steps) {
		unixtime += tz_offset;
		if (fixed_gmtime(unixtime, &tm)) return 1;
		tz_offset = 0;
	}
	datetime_set_simple(v, unixtime);
	if (with_steps || datetime_get_simple(v) != unixtime) {
		v->valid_steps = pos;
		v->year = tm.tm_year;
		v->month = tm.tm_mon;
		for (int i = 2; i < arraylen(field); i++) {
			v->data.field[i - 2] = *field[i];
		}
		v->tz_mins = tz_offset / 60;
	} else {
		if (f_val < 0) {
			implfuzz = ceil(implfuzz / 2.0);
			unixtime += implfuzz;
			datetime_set_simple(v, unixtime);
			if (datetime_get_simple(v) != unixtime) return 1;
			f_val -= implfuzz;
		} else {
			f_val += implfuzz;
		}
	}
	f->d_fuzz = ceil(f_val);
	return 0;
}

int64_t datetime_get_simple(const datetime_time_t *val)
{
	assert(!val->valid_steps);
	return (int64_t)val->year << 32 | val->data.simple_part;
}

void datetime_set_simple(datetime_time_t *val, int64_t simple)
{
	val->valid_steps = 0;
	val->year = (int64_t)simple >> 32;
	val->data.simple_part = simple;
}

void datetime_strfix(tag_value_t *val)
{
	struct tm tm;
	int64_t ttime = datetime_get_simple(&val->val.v_datetime);
	assert (!val->val.v_datetime.valid_steps);
	fixed_gmtime(ttime, &tm);
	char buf[128];
	int l = strftime(buf, sizeof(buf), "%Y-%m-%dT%H:%M:%SZ", &tm);
	if (val->fuzz.f_datetime.d_fuzz) {
		long long pv = val->fuzz.f_datetime.d_fuzz;
		static const int scale[] = {60, 60, 24, 0};
		static const char suff[] = "SMHd";
		int sp = 0;
		while (sp < 3 && pv % scale[sp] == 0) {
			pv /= scale[sp++];
		}
		snprintf(buf + l, sizeof(buf) - l, "+%lld%c", pv, suff[sp]);
	}
	val->v_str = mm_strdup(buf);
}
