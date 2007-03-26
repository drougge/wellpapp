#include "db.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/mman.h>

#define MM_MAGIC0 0x4d4d0402 /* "MM^D^B" */
#define MM_MAGIC1 0x4d4d4845 /* "MMHE" */
#define MM_FLAG_CLEAN 1
typedef struct mm_head {
	uint32_t magic0;
	uint32_t size;
	uint32_t segment_size;
	uint32_t of_segments;
	uint32_t used;
	uint32_t free;
	uint32_t flags;
	uint8_t  *addr;
	uint8_t  *top;
	uint8_t  *bottom;
	uint8_t  *pad0;
	uint32_t magic1;
} mm_head_t;

#define MM_ALIGN 4

#define MM_BASE_ADDR ((uint8_t *)(1024 * 1024 * 1024))
#define MM_SEGMENT_SIZE (4 * 1024 * 1024)

static mm_head_t *mm_head;
static const char *mm_basedir;

static int mm_open_segment(int nr, int flags) {
	char fn[80];
	int  fd;

	snprintf(fn, sizeof(fn), "%s/%d.db", mm_basedir, nr);
	fd = open(fn, flags, 0600);
	assert(fd >= 0);
	return fd;
}

static void *mm_map_segment(int nr, int fd) {
	uint8_t *addr, *want_addr;

	want_addr = MM_BASE_ADDR + (nr * MM_SEGMENT_SIZE);
	addr = mmap(want_addr, MM_SEGMENT_SIZE, PROT_READ | PROT_WRITE, MAP_FIXED | MAP_NOCORE | MAP_SHARED, fd, 0);
	close(fd);
	assert(addr == want_addr);
	return addr;
}

static void mm_new_segment(void) {
	char    buf[16384];
	int     nr, z, fd;
	uint8_t *addr;

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
		mm_head->size  += MM_SEGMENT_SIZE;
		mm_head->free  += MM_SEGMENT_SIZE;
		mm_head->bottom = addr;
		mm_head->top    = addr + MM_SEGMENT_SIZE;
		mm_head->of_segments++;
	}
}

int mm_init(const char *filename, rbtree_head_t **posttree, rbtree_head_t **tagtree, int use_existing) {
	mm_basedir = strdup(filename);
	assert(mm_basedir);
	assert(sizeof(mm_head_t) % MM_ALIGN == 0);
	*posttree = (rbtree_head_t *)(MM_BASE_ADDR + sizeof(*mm_head));
	*tagtree  = *posttree + 1;
	if (use_existing) {
		mm_head_t head;
		int fd, i;

		fd = mm_open_segment(0, O_RDWR);
		read(fd, &head, sizeof(head));
		assert(head.magic0 == MM_MAGIC0);
		assert(head.magic1 == MM_MAGIC1);
		mm_map_segment(0, fd);
		mm_head = (mm_head_t *)MM_BASE_ADDR;
		for (i = 1; i < head.of_segments; i++) {
			fd = mm_open_segment(i, O_RDWR);
			mm_map_segment(i, fd);
		}
		return 0;
	} else {
		int r;
		mm_new_segment();
		mm_head = (mm_head_t *)MM_BASE_ADDR;
		mm_head->addr     = MM_BASE_ADDR;
		mm_head->magic0   = MM_MAGIC0;
		mm_head->magic1   = MM_MAGIC1;
		mm_head->size     = MM_SEGMENT_SIZE;
		mm_head->used     = sizeof(*mm_head) + (sizeof(rbtree_head_t) * 2);
		mm_head->free     = mm_head->size - mm_head->used;
		mm_head->bottom   = mm_head->addr + mm_head->used;
		mm_head->top      = mm_head->addr + mm_head->size;
		mm_head->segment_size = MM_SEGMENT_SIZE;
		mm_head->of_segments  = 1;
		r = rbtree_init(*posttree, RBTREE_ALLOCATION_POLICY_CHUNKED, 255);
		assert(!r);
		r = rbtree_init(*tagtree, RBTREE_ALLOCATION_POLICY_CHUNKED, 255);
		assert(!r);
		return 1;
	}
}

static int unaligned = 0;
void *mm_alloc(unsigned int size) {
	if (size % MM_ALIGN) {
		assert(unaligned);
		if (mm_head->top - size < mm_head->bottom) {
			mm_new_segment();
		}
		mm_head->top -= size;
		assert(mm_head->top >= mm_head->bottom);
		mm_head->free -= size;
		mm_head->used += size;
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

void mm_free(void *mem) {
	assert(0);
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

char *mm_strdup(const char *str) {
	char *new;
	int len = strlen(str);

	unaligned = 1;
	new = mm_alloc(len + 1);
	unaligned = 0;
	strcpy(new, str);
	return new;
}

void mm_print(void) {
	printf("%d of %d bytes used, %d free. %d segments.\n", mm_head->used, mm_head->size, mm_head->free, mm_head->of_segments);
	printf("%d bytes small, %d bytes aligned.\n", (int)(mm_head->addr + mm_head->size - mm_head->top), (int)(mm_head->bottom - mm_head->addr));
}
