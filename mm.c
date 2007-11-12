#include "db.h"

#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/file.h>

#define MM_MAGIC0 0x4d4d0402 /* "MM^D^B" */
#define MM_MAGIC1 0x4d4d4845 /* "MMHE" */
#define MM_FLAG_CLEAN 1
typedef struct mm_head {
	uint32_t      magic0;
	uint32_t      flags;
	uint64_t      size;
	uint32_t      segment_size;
	uint32_t      of_segments;
	uint64_t      used;
	uint64_t      free;
	uint64_t      wasted;
	uint64_t      used_small;
	uint64_t      logindex;
	uint64_t      logdumpindex;
	uint32_t      tag_guid_last[2];
	rbtree_head_t posttree;
	rbtree_head_t tagtree;
	rbtree_head_t tagaliastree;
	rbtree_head_t tagguidtree;
	rbtree_head_t usertree;
	uint8_t       *addr;
	uint8_t       *top;
	uint8_t       *bottom;
	uint32_t      pad0;
	uint32_t      magic1;
} mm_head_t;

#define MM_ALIGN 4

#define MM_BASE_ADDR ((uint8_t *)(1024 * 1024 * 1024))
#define MM_SEGMENT_SIZE (4 * 1024 * 1024)

static mm_head_t *mm_head;

uint32_t *tag_guid_last;
uint64_t *logindex;
uint64_t *logdumpindex;

static int mm_open_segment(unsigned int nr, int flags) {
	char fn[1024];
	int  fd;

	snprintf(fn, sizeof(fn), "%s/mm_cache/%08x", basedir, nr);
	fd = open(fn, flags, 0600);
	assert(fd >= 0);
	return fd;
}

static void *mm_map_segment(unsigned int nr, int fd) {
	uint8_t *addr, *want_addr;

	want_addr = MM_BASE_ADDR + (nr * MM_SEGMENT_SIZE);
	addr = mmap(want_addr, MM_SEGMENT_SIZE, PROT_READ | PROT_WRITE,
	            MAP_FIXED | MAP_NOCORE | MAP_SHARED, fd, 0);
	close(fd);
	assert(addr == want_addr);
	return addr;
}

static void mm_new_segment(void) {
	char         buf[16384];
	int          fd;
	unsigned int nr;
	uint8_t      *addr;
	unsigned int z;

	nr = mm_head ? mm_head->of_segments : 0;
	fd = mm_open_segment(nr, O_RDWR | O_CREAT | O_EXCL);
	memset(buf, 0, sizeof(buf));
	z = MM_SEGMENT_SIZE;
	while (z) {
		int len;
		len = write(fd, buf, z < sizeof(buf) ? z : sizeof(buf));
		assert(len > 0);
		z -= len;
	}
	addr = mm_map_segment(nr, fd);
	close(fd);
	if (mm_head) {
		mm_head->size   += MM_SEGMENT_SIZE;
		mm_head->wasted += mm_head->free;
		mm_head->free    = MM_SEGMENT_SIZE;
		mm_head->bottom  = addr;
		mm_head->top     = addr + MM_SEGMENT_SIZE;
		mm_head->of_segments++;
	}
}

static int lock_fd;

int mm_init(int use_existing) {
	mm_head = (mm_head_t *)MM_BASE_ADDR;
	assert(sizeof(mm_head_t) % MM_ALIGN == 0);
	tag_guid_last = mm_head->tag_guid_last;
	posttree      = &mm_head->posttree;
	tagtree       = &mm_head->tagtree;
	tagaliastree  = &mm_head->tagaliastree;
	tagguidtree   = &mm_head->tagguidtree;
	usertree      = &mm_head->usertree;
	logindex      = &mm_head->logindex;
	logdumpindex  = &mm_head->logdumpindex;
	if (use_existing) {
		mm_head_t head;
		int fd;
		unsigned int i;

		fd = mm_open_segment(0, O_RDWR);
		lock_fd = dup(fd);
		assert(lock_fd != -1);
		read(fd, &head, sizeof(head));
		assert(head.magic0 == MM_MAGIC0);
		assert(head.magic1 == MM_MAGIC1);
		mm_map_segment(0, fd);
		for (i = 1; i < head.of_segments; i++) {
			fd = mm_open_segment(i, O_RDWR);
			mm_map_segment(i, fd);
		}
		return 0;
	} else {
		int r;
		mm_head = NULL;
		mm_new_segment();
		mm_head = (mm_head_t *)MM_BASE_ADDR;
		mm_head->addr     = MM_BASE_ADDR;
		mm_head->magic0   = MM_MAGIC0;
		mm_head->magic1   = MM_MAGIC1;
		mm_head->size     = MM_SEGMENT_SIZE;
		/* mm_head, tag_guid_last[2] (posttree, tagtree, tagaliastree, tagguidtree, usertree) */
		mm_head->used     = sizeof(*mm_head) + 8 + (sizeof(rbtree_head_t) * 5);
		mm_head->free     = mm_head->size - mm_head->used;
		mm_head->bottom   = mm_head->addr + mm_head->used;
		mm_head->top      = mm_head->addr + mm_head->size;
		mm_head->segment_size = MM_SEGMENT_SIZE;
		mm_head->of_segments  = 1;
		r  = rbtree_init(posttree, RBTREE_ALLOCATION_POLICY_CHUNKED, 255);
		r |= rbtree_init(tagtree, RBTREE_ALLOCATION_POLICY_CHUNKED, 255);
		r |= rbtree_init(tagaliastree, RBTREE_ALLOCATION_POLICY_CHUNKED, 255);
		r |= rbtree_init(tagguidtree, RBTREE_ALLOCATION_POLICY_CHUNKED, 255);
		r |= rbtree_init(usertree, RBTREE_ALLOCATION_POLICY_CHUNKED, 255);
		assert(!r);
		lock_fd = mm_open_segment(0, O_RDWR);
		return 1;
	}
}

static void *mm_alloc_(unsigned int size, int unaligned) {
	if (size % MM_ALIGN) {
		assert(unaligned);
		if (mm_head->top - size < mm_head->bottom) {
			mm_new_segment();
		}
		mm_head->top -= size;
		assert(mm_head->top >= mm_head->bottom);
		mm_head->free -= size;
		mm_head->used += size;
		mm_head->used_small += size;
		return mm_head->top;
	} else {
		void *ptr = mm_head->bottom;
		if (mm_head->bottom + size > mm_head->top) {
			mm_new_segment();
			ptr = mm_head->bottom;
		}
		mm_head->bottom += size;
		assert(mm_head->bottom <= mm_head->top);
		mm_head->free -= size;
		mm_head->used += size;
		return ptr;
	}
}

void *mm_alloc(unsigned int size) {
	return mm_alloc_(size, 0);
}

void mm_free(void *mem) {
	assert(0);
	(void)mem;
/*
	uint8_t **ptr = mem;
	assert(mem);
	mm_head->free += MM_SIZE;
	mm_head->used -= MM_SIZE;
	*ptr = mm_head->freelist;
	mm_head->freelist = mem;
	memset(ptr + sizeof(void *), 0, MM_SIZE - sizeof(void *));
*/
}

const char *mm_strdup(const char *str) {
	char *new;
	int len = strlen(str);

	new = mm_alloc_(len + 1, 1);
	strcpy(new, str);
	return new;
}

void mm_print(void) {
	printf("%llu of %llu bytes used, %llu free (%llu wasted). %d segments.\n", mm_head->used, mm_head->size, mm_head->free, mm_head->wasted, mm_head->of_segments);
	printf("%llu bytes small, %llu bytes aligned.\n", mm_head->used_small, mm_head->used - mm_head->used_small);
}

void mm_lock(void) {
	int r = flock(lock_fd, LOCK_EX);
	assert(!r);
}

void mm_unlock(void) {
	int r = flock(lock_fd, LOCK_UN);
	assert(!r);
}
