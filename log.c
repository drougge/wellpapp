#include "db.h"

#include <stdarg.h>

static int fd, e;
static uint64_t trans_id = 1;

void d_log_start(int fd, void *user) {
	char buf[80];
	int len, r;

	len = snprintf(buf, sizeof(buf), "T%016llx\n", trans_id);
	r = write(fd, buf, len);
	assert(r == len);
}

void d_log_end(int fd, void *user) {
	char buf[80];
	int len, r;

	len = snprintf(buf, sizeof(buf), "t%016llx\n", trans_id);
	r = write(fd, buf, len);
	assert(r == len);
	trans_id++;
}

void d_log(int fd, void *user, const char *data, ...) {
	char *buf1, *buf2;
	int len, r;
	va_list ap;

	va_start(ap, data);
	vasprintf(&buf1, data, ap);
	va_end(ap);
	assert(buf1);
	len = asprintf(&buf2, "%s\n", buf1);
	assert(buf2);
	free(buf1);
	r = write(fd, buf2, len);
	free(buf2);
	assert(r == len);

}

void d_log1(int fd, void *user, const char *data, ...) {
	char *buf1, *buf2;
	int len1, len2, r;
	va_list ap;

	va_start(ap, data);
	len1 = vasprintf(&buf1, data, ap);
	va_end(ap);
	assert(buf1);
	len2 = asprintf(&buf2, "T%016llx\n%s\nt%016llx\n", trans_id, buf1, trans_id);
	assert(buf2);
	trans_id++;
	r = write(fd, buf2, len2);
	free(buf2);
	free(buf1);
	assert(r == len2);
}

void tag_iter(rbtree_key_t key, rbtree_value_t value) {
	tag_t *tag = (tag_t *)value;
	d_log1(fd, NULL, "CT%s", tag->name);
}

void post_iter(rbtree_key_t key, rbtree_value_t value) {
	post_t *post = (post_t *)value;
	const char *md5;
	post_taglist_t *tl;

	md5 = md5_md52str(post->md5);
	d_log_start(fd, NULL);
	d_log(fd, NULL, "CP%s %d %d %llu", md5, post->width, post->height, (unsigned long long)post->created);
	if (post->source) {
		d_log(fd, NULL, "P%s source %s", md5, post->source);
	}
	tl = &post->tags;
	while (tl) {
		int i;
		for (i = 0; i < POST_TAGLIST_PER_NODE; i++) {
			if (tl->tags[i]) {
				d_log(fd, NULL, "P%s tag %s", md5, tl->tags[i]->name);
			}
		}
		tl = tl->next;
	}
	d_log_end(fd, NULL);
}

extern rbtree_head_t *tagtree;
extern rbtree_head_t *posttree;

int dump_log(const char *filename) {
	fd = open(filename, O_WRONLY | O_CREAT | O_EXCL);
	if (fd < 0) return 1;
	e = 0;
	rbtree_iterate(tagtree, tag_iter);
	rbtree_iterate(posttree, post_iter);
	if (fsync(fd)) return 1;
	if (close(fd)) return 1;
	fd = -1;
	return e;
}
