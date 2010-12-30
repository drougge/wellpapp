#include <stdio.h>
#include <stdint.h>
#include <math.h>
#include <string.h>
#include <stdlib.h>

#include "config.h"

#define RES 4
//#define FN  (PREFIX "id.8x8")
#define FN  (PREFIX "id.4x4")

static float pearsonr(uint8_t *x, uint8_t *y, int len)
{
	float mx = 0.0f, my = 0.0f;
	float xmi, ymi;
	float dx = 0.0f, dy = 0.0f;
	float r_num = 0.0f, r_den;
	int i;

	for (i = 0; i < len; i++) {
		mx += x[i];
		my += y[i];
	}
	mx /= len;
	my /= len;
	for (i = 0; i < len; i++) {
		xmi = (float)x[i] - mx;
		ymi = (float)y[i] - my;
		r_num += xmi * ymi;
		dx += xmi * xmi;
		dy += ymi * ymi;
	}
	r_num *= len;
	r_den = sqrtf(dx * dy) * len;
	return fabsf(r_num / r_den);
}

static void deser(uint8_t *a, const char *buf)
{
	int i;
	for (i = 0; i < RES*RES; i++) {
		uint8_t v1, v2;
		v1 = buf[i * 2] - '0';
		v2 = buf[i * 2 + 1] - '0';
		if (v1 > 9) v1 -= 'a' - '0' - 10;
		if (v2 > 9) v2 -= 'a' - '0' - 10;
		a[i] = v1 << 4 | v2;
	}
}

typedef struct imgid {
	uint8_t a[RES*RES];
	char    md5[32];
	float   p;
} imgid_t;

static imgid_t *ids;
static unsigned long of_ids;
static imgid_t cmp_id;
static const char *cmp_img;
static int cmp_img_ok;

static int comp_id(const void *a, const void *b) {
	const imgid_t *x = a;
	const imgid_t *y = b;
	if (x->p < y->p) return 1;
	if (x->p > y->p) return -1;
	return 0;
}

#define THRES 0.90f

int main(int argc, char **argv)
{
	FILE *fh;
	char buf[256];
	unsigned long id;
	float p;

	fh = fopen(FN, "r");
	if (!fh) return 1;
	if (argc == 3 && !strcmp(argv[1], "-id") && strlen(argv[2]) == 32) {
		memcpy(cmp_id.md5, "00000000000000000000000000000000", 32);
		deser(cmp_id.a, argv[2]);
	} else if (argc == 2 && strlen(argv[1]) == 32) {
		cmp_img = argv[1];
		while (fgets(buf, sizeof(buf), fh)) {
			if (!memcmp(buf, cmp_img, 32)) {
				memcpy(cmp_id.md5, buf, 32);
				deser(cmp_id.a, buf + 33);
				cmp_img_ok = 1;
				break;
			}
		}
		if (!cmp_img_ok) return 1;
		if (fseek(fh, 0, SEEK_SET)) return 1;
	} else {
		return 1;
	}
	id = 0;
	of_ids = 100;
	ids = malloc(sizeof(imgid_t) * of_ids);
	while (fgets(buf, sizeof(buf), fh)) {
		if (memcmp(buf, cmp_id.md5, 32)) {
			deser(ids[id].a, buf + 33);
			p = pearsonr(ids[id].a, cmp_id.a, RES*RES);
			if (p > THRES) {
				ids[id].p = p;
				memcpy(ids[id].md5, buf, 32);
				id++;
				if (id == of_ids) {
					of_ids *= 2;
					ids = realloc(ids, sizeof(imgid_t) * of_ids);
					if (!ids) return 1;
				}
			}
		}
	}
	fclose(fh);
	if (!id) return 0;
	of_ids = id;
	qsort(ids, of_ids, sizeof(imgid_t), comp_id);
	char md5[33];
	md5[32] = 0;
	if (of_ids > 10) of_ids = 10;
	for (id = 0; id < of_ids; id++) {
		memcpy(md5, ids[id].md5, 32);
		printf("%s %f\n", md5, ids[id].p);
	}
	return 0;
}

#if 0
static int read_db(const char *filename)
{
	FILE *fh;
	char buf[256];
	unsigned long id = 0;

	fh = fopen(filename, "r");
	if (!fh) return 1;
	of_ids = 1024;
	ids = malloc(sizeof(imgid_t) * of_ids);
	if (!ids) goto err;
	while (fgets(buf, sizeof(buf), fh)) {
		if (memcmp(buf, cmp_img, 32)) {
			if (id == of_ids) {
				of_ids *= 2;
				ids = realloc(ids, sizeof(imgid_t) * of_ids);
				if (!ids) goto err;
			}
			memcpy(ids[id].md5, buf, 32);
			deser(ids[id].a, buf + 33);
			id++;
		} else {
			memcpy(cmp_id.md5, buf, 32);
			deser(cmp_id.a, buf + 33);
			cmp_img_ok = 1;
		}
	}
	if (!feof(fh)) goto err;
	fclose(fh);
	of_ids = id;
	return 0;
err:
	fclose(fh);
	of_ids = 0;
	return 1;
}

int mai__n(int argc, char **argv)
{
	unsigned long i;

	if (argc != 2 || strlen(argv[1]) != 32) return 1;
	cmp_img = argv[1];
	if (read_db(FN)) return 1;
	if (!cmp_img_ok) return 1;
	for (i = 0; i < of_ids; i++) {
		ids[i].p = pearsonr(ids[i].a, cmp_id.a, RES*RES);
	}
	qsort(ids, of_ids, sizeof(imgid_t), comp_id);
	char md5[33];
	md5[32] = 0;
	for (i = 0; i < 10; i++) {
		memcpy(md5, ids[i].md5, 32);
		printf("%s %f\n", md5, ids[i].p);
	}
	return 0;
}

int mai_n(void)
{
	uint8_t a[RES*RES];
	uint8_t b[RES*RES];
	char buf[256];
	FILE *fh;
	float p;

	fh = fopen(FN, "r");
	if (!fh) return 1;
	deser(a, "ffe6e4effea2b4c2ffbcc9dcffc6a2f1"); // bild b86f100afd6f668360fe85728788cf82
	printf("%x%x\n",a[0], a[1]);
	while (fgets(buf, sizeof(buf), fh)) {
		deser(b, buf + 33);
		p = pearsonr(a, b, RES*RES);
		if (p > 0.93f) {
			buf[32] = 0;
			printf("%s %f\n", buf, p);
		}
	}
	return 0;
}
#endif
