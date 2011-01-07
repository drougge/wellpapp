#include <stdio.h>
#include <stdint.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <sys/uio.h>
#include <unistd.h>
#include <fcntl.h>

#define SIZE (1024 * 1024 * 256)

static void try(int fd, uint64_t *addr, int len)
{
	for (int i = 0; i < len; i++) {
		void *want = (void *)(intptr_t)addr[i];
		void *res = mmap(want, SIZE, PROT_READ,
		                 MAP_FIXED | MAP_SHARED, fd, 0);
		if (res == want) printf("%p\n", res);
	}
}

int main(void)
{
	static uint64_t addr[] = {0x20000000UL, 0x40000000UL, 0x60000000UL,
	                          0x80000000UL, 0xa0000000UL, 0xc0000000UL,
	         /* 64 bit */     0x120000000ULL, 0x140000000ULL,
	                          0x220000000ULL, 0x2a0000000ULL,
	                          0x420000000ULL, 0x820000000ULL,
	                          0xc20000000ULL};
	int len = 6;
	int middle;
	int fd;
	if (sizeof(void *) > 4) len += 7;
	fd = open("/dev/urandom", O_RDONLY);
	if (fd == -1) return 1;
	if (read(fd, &middle, sizeof(middle)) != sizeof(middle)) return 1;
	close(fd);
	middle %= len;
	// I would use /dev/zero, but darwin can't map that.
	fd = open("suggest_mm_base", O_RDONLY);
	if (fd == -1) return 1;
	try(fd, addr + middle, len - middle);
	try(fd, addr, middle);
	return 0;
}
