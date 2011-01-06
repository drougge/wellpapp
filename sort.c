/* Merge sort, but can be replaced by whatever. */
/* Could undoubtedly be sped up by stoping partitioning sooner. */
/* May not be optimal to duplicate the left array. */

#include "db.h"

#define ELEM(a, e) (((char *)(a)) + (size * (e)))
static void sort_merge(void *left, int left_nmemb, void *right, int right_nmemb,
                       size_t size, sort_compar_t comp, void *data)
{
	void *base = left;
	char left_copy[left_nmemb * size];
	memcpy(left_copy, left, sizeof(left_copy));
	left = left_copy;
	int li = 0, ri = 0, i = 0;
	while (li < left_nmemb && ri < right_nmemb) {
		int c = comp(ELEM(left, li), ELEM(right, ri), data);
		if (c <= 0) {
			memcpy(ELEM(base, i), ELEM(left, li), size);
			i++;
			li++;
		}
		if (c >= 0) {
			void *dest = ELEM(base, i);
			void *src = ELEM(right, ri);
			if (dest != src) memcpy(dest, src, size);
			i++;
			ri++;
		}
	}
	memcpy(ELEM(base, i), ELEM(left, li), size * (left_nmemb - li));
}

void sort(void *base, int nmemb, size_t size, sort_compar_t comp, void *data)
{
	char *cbase = base;
	if (nmemb < 2) return;
	sort(base, nmemb / 2, size, comp, data);
	cbase += size * (nmemb / 2);
	sort(cbase, (nmemb + 1) / 2, size, comp, data);
	sort_merge(base, nmemb / 2, cbase, (nmemb + 1) / 2, size, comp, data);
}
