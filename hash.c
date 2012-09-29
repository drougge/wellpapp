#include "db.h"

static const unsigned long hash_sizes[] = {6151, 49157, 196613, 393241, 786433,
                                           2000003};

static unsigned long djb(const char *key)
{
	unsigned long hash = 5381;
	int c;

	while ((c = *key++)) {
		hash = ((hash << 5) + hash) + c;
	}
	return hash;
}

static void hash_init_i(hash_t *h)
{
	h->used = 0;
	h->data = mm_alloc_lax(sizeof(*h->data) * (hash_sizes[h->size] + 1));
}

void hash_init(hash_t *h)
{
	h->size = 0;
	hash_init_i(h);
}

const char *hash_find(hash_t *h, const char *key)
{
	unsigned long hash = djb(key);
	unsigned long size = hash_sizes[h->size];
	unsigned long i = hash % size;
	const char **data = h->data;

	while (data[i]) {
		if (!strcmp(data[i], key)) return data[i];
		i = (i + 1) % size;
	}
	return NULL;
}

static void hash_add_i(hash_t *h, const char *key)
{
	unsigned long hash = djb(key);
	unsigned long size = hash_sizes[h->size];
	unsigned long i = hash % size;
	const char **data = h->data;

	while (data[i]) {
		i = (i + 1) % size;
	}
	data[i] = key;
	h->used++;
}

void hash_add(hash_t *h, const char *key)
{
	if (hash_sizes[h->size] * 0.8 < h->used) {
		const char **old_data = h->data;
		unsigned long size = hash_sizes[h->size];
		h->size++;
		assert(h->size < arraylen(hash_sizes));
		hash_init_i(h);
		while (size--) {
			if (old_data[size]) hash_add_i(h, old_data[size]);
		}
		// @@ free old_data
	}
	hash_add_i(h, key);
}
