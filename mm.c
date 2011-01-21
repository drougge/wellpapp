#include "db.h"

#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/file.h>

#ifndef MAP_NOCORE
#define MAP_NOCORE 0
#endif

static const size_t sizes[] = {
	sizeof(guid_t),
	sizeof(post_taglist_t),
	sizeof(implication_t),
	sizeof(impllist_t),
	sizeof(postlist_t),
	sizeof(post_t),
	sizeof(field_t),
	sizeof(postlist_node_t),
	sizeof(tag_t),
	sizeof(tagalias_t),
	sizeof(user_t),
};

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
	ss128_head_t  posts;
	ss128_head_t  tags;
	ss128_head_t  tagaliases;
	ss128_head_t  tagguids;
	ss128_head_t  users;
	uint8_t       *addr;
	uint8_t       *top;
	uint8_t       *bottom;
	md5_t         config_md5;
	size_t        struct_sizes[arraylen(sizes)];
	uint32_t      clean;
	uint32_t      magic1;
} mm_head_t;

#define MM_ALIGN 4
#define MM_SEGMENT_SIZE (4 * 1024 * 1024)
#define MM_MAX_SEGMENTS 1024

uint8_t *MM_BASE_ADDR = 0;
static int mm_fd[MM_MAX_SEGMENTS];

static mm_head_t *mm_head;

uint32_t *tag_guid_last;
uint64_t *logindex;
uint64_t *logdumpindex;

static int mm_open_segment(unsigned int nr, int flags)
{
	char fn[1024];
	int  fd;
	int  len;

	assert(nr < MM_MAX_SEGMENTS && mm_fd[nr] == -1);
	len = snprintf(fn, sizeof(fn), "%s/mm_cache/%08x", basedir, nr);
	assert(len < (int)sizeof(fn));
	fd = open(fn, flags, 0600);
	mm_fd[nr] = fd;
	return fd;
}

static void *mm_map_segment(unsigned int nr)
{
	uint8_t *addr, *want_addr;
	static volatile int touch = 0;

	want_addr = MM_BASE_ADDR + (nr * MM_SEGMENT_SIZE);
	addr = mmap(want_addr, MM_SEGMENT_SIZE, PROT_READ | PROT_WRITE,
	            MAP_FIXED | MAP_NOCORE | MAP_SHARED, mm_fd[nr], 0);
	assert(addr == want_addr);
	// Try to make sure everything is faulted in.
	// I don't trust [posix_]madvise.
	for (int i = 0; i < MM_SEGMENT_SIZE; i += 4096) {
		touch += addr[i];
	}
	return addr;
}

static void mm_unmap_segment(unsigned int nr)
{
	uint8_t *addr = MM_BASE_ADDR + (nr * MM_SEGMENT_SIZE);
	int r = munmap(addr, MM_SEGMENT_SIZE);
	assert(!r);
}

static void mm_new_segment(void)
{
	char         buf[16384];
	int          fd;
	unsigned int nr;
	uint8_t      *addr;
	unsigned int z;

	nr = mm_head ? mm_head->of_segments : 0;
	fd = mm_open_segment(nr, O_RDWR | O_CREAT | O_TRUNC | O_EXLOCK);
	assert(fd >= 0);
	memset(buf, 0, sizeof(buf));
	z = MM_SEGMENT_SIZE;
	while (z) {
		int len;
		len = write(fd, buf, z < sizeof(buf) ? z : sizeof(buf));
		assert(len > 0);
		z -= len;
	}
	addr = mm_map_segment(nr);
	if (mm_head) {
		mm_head->size   += MM_SEGMENT_SIZE;
		mm_head->wasted += mm_head->free;
		mm_head->free    = MM_SEGMENT_SIZE;
		mm_head->bottom  = addr;
		mm_head->top     = addr + MM_SEGMENT_SIZE;
		mm_head->of_segments++;
	}
}

static int mm_lock_fd = -1;

static void mm_init_new(void)
{
	int r;

	mm_head = NULL;
	mm_new_segment();
	mm_head = (mm_head_t *)MM_BASE_ADDR;
	mm_head->addr     = MM_BASE_ADDR;
	mm_head->magic0   = MM_MAGIC0;
	mm_head->magic1   = MM_MAGIC1;
	mm_head->size     = MM_SEGMENT_SIZE;
	mm_head->used     = sizeof(*mm_head);
	mm_head->free     = mm_head->size - mm_head->used;
	mm_head->bottom   = mm_head->addr + mm_head->used;
	mm_head->top      = mm_head->addr + mm_head->size;
	mm_head->segment_size = MM_SEGMENT_SIZE;
	mm_head->of_segments  = 1;
	memcpy(mm_head->config_md5.m, config_md5.m, sizeof(config_md5.m));
	memcpy(mm_head->struct_sizes, sizes, sizeof(sizes));
	r  = ss128_init(posts);
	r |= ss128_init(tags);
	r |= ss128_init(tagaliases);
	r |= ss128_init(tagguids);
	r |= ss128_init(users);
	assert(!r);
}

static int mm_init_old(void)
{
	mm_head_t    head;
	int          fd;
	unsigned int i;
	ssize_t      r;

	fd = mm_open_segment(0, O_RDWR);
	if (fd == -1) return 1;
	r = read(fd, &head, sizeof(head));
	assert(r == sizeof(head));
	if ((head.magic0 != MM_MAGIC0)
	    || (head.magic1 != MM_MAGIC1)
	    || (head.addr != MM_BASE_ADDR)
	    || (head.segment_size != MM_SEGMENT_SIZE)
	    || (!head.clean)
	    || memcmp(head.struct_sizes, sizes, sizeof(sizes))
	    || memcmp(head.config_md5.m, config_md5.m, sizeof(config_md5.m))) {
		close(fd);
		mm_fd[0] = -1;
		return 1;
	}
	mm_map_segment(0);
	mm_head->clean = 0;
	for (i = 1; i < head.of_segments; i++) {
		fd = mm_open_segment(i, O_RDWR);
		assert(fd >= 0);
		mm_map_segment(i);
	}
	return 0;
}

/* Note that this returns 1 for a new cache, not failure as such. */
int mm_init(void)
{
	char  fn[1024];
	int   i;
	int   len;
	char  clean;
	int   r;
	off_t pos;

	assert(MM_BASE_ADDR);
	for (i = 0; i < MM_MAX_SEGMENTS; i++) mm_fd[i] = -1;
	mm_head = (mm_head_t *)MM_BASE_ADDR;
	assert(sizeof(mm_head_t) % MM_ALIGN == 0);
	tag_guid_last = mm_head->tag_guid_last;
	posts         = &mm_head->posts;
	tags          = &mm_head->tags;
	tagaliases    = &mm_head->tagaliases;
	tagguids      = &mm_head->tagguids;
	users         = &mm_head->users;
	logindex      = &mm_head->logindex;
	logdumpindex  = &mm_head->logdumpindex;

	len = snprintf(fn, sizeof(fn), "%s/LOCK", basedir);
	assert(len < (int)sizeof(fn));
	mm_lock_fd = open(fn, O_RDWR | O_CREAT | O_EXLOCK, 0600);
	assert(mm_lock_fd != -1);
	len = read(mm_lock_fd, &clean, 1);
	if (len != 1) clean = 'U';
	pos = lseek(mm_lock_fd, 0, SEEK_SET);
	assert(pos == 0);
	len = write(mm_lock_fd, "U", 1);
	assert(len == 1);
	r = fsync(mm_lock_fd);
	assert(!r);
	if (clean == 'C') {
		if (!mm_init_old()) return 0;
	}
	mm_init_new();
	return 1;
}

static void mm_sync(unsigned int nr)
{
	int r = fsync(mm_fd[nr]);
	assert(!r);
}

void mm_cleanup(void)
{
	int     i;
	off_t   pos;
	ssize_t r;

	for (i = mm_head->of_segments - 1; i >= 0; i--) {
		if (i == 0) {
			mm_sync(0);
			mm_head->clean = 1;
		}
		mm_unmap_segment(i);
		mm_sync(i);
	}
	pos = lseek(mm_lock_fd, 0, SEEK_SET);
	assert(pos == 0);
	r = write(mm_lock_fd, "C", 1);
	assert(r == 1);
	close(mm_lock_fd);
}

static void *mm_alloc_(unsigned int size, int unaligned)
{
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

void *mm_alloc(unsigned int size)
{
	return mm_alloc_(size, 0);
}

void *mm_alloc_s(unsigned int size)
{
	return mm_alloc_(size, 1);
}

void mm_free(void *mem)
{
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

const char *mm_strdup(const char *str)
{
	char *new;
	int len = strlen(str);

	new = mm_alloc_(len + 1, 1);
	strcpy(new, str);
	return new;
}

void mm_print(void)
{
	printf("%llu of %llu bytes used, %llu free (%llu wasted). %d segments.\n", ULL mm_head->used, ULL mm_head->size, ULL mm_head->free, ULL mm_head->wasted, mm_head->of_segments);
	printf("%llu bytes small, %llu bytes aligned.\n", ULL mm_head->used_small, ULL (mm_head->used - mm_head->used_small));
}
