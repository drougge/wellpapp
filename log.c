#include "db.h"

#include <stdarg.h>
#include <sys/file.h>

static int fd;
static trans_id_t next_trans_id = 1;

static void trans_lock(void) {
	int r = flock(fd, LOCK_EX);
	assert(!r);
}

static void trans_unlock(void) {
	int r = flock(fd, LOCK_UN);
	assert(!r);
}

static int do_sync = 1;
static void trans_sync(void) {
	if (do_sync) {
		int r = fsync(fd);
		assert(!r);
	}
}

void log_trans_start(trans_t *trans, void *user) {
	char buf[12];
	int  len, r;
	
	trans_lock();
	trans->id = next_trans_id++;
	len = snprintf(buf, sizeof(buf), "T%08xU\n", trans->id);
	assert(len == 11);
	r = write(fd, buf, len);
	trans->mark_offset = lseek(fd, 0, SEEK_CUR);
	trans_unlock();
	assert(r == len);
	assert(trans->mark_offset != -1);
	trans->mark_offset -= 2;
}

void log_trans_end(trans_t *trans) {
	off_t pos, r2;
	int   r;
	
	trans_sync();
	trans_lock();
	pos = lseek(fd, trans->mark_offset, SEEK_SET);
	assert(pos == trans->mark_offset);
	r = write(fd, "D", 1);
	r2  = lseek(fd, 0, SEEK_END);
	assert(r == 1);
	assert(r2 != -1);
	trans_unlock();
	assert(pos == trans->mark_offset);
}

static void log_write_(trans_t *trans, const char *fmt, va_list ap) {
	char *buf = NULL;
	char idbuf[12];
	struct iovec iov[3];
	int len, wlen;

	iov[0].iov_base = idbuf;
	iov[0].iov_len  = snprintf(idbuf, sizeof(idbuf), "D%08x ", trans->id);
	assert(iov[0].iov_len == 10);
	iov[2].iov_base = "\n";
	iov[2].iov_len  = 1;
	iov[1].iov_len = vasprintf(&buf, fmt, ap);
	assert(buf);
char *ptr = buf;
while (*ptr) assert(*ptr++ != '\n');
	iov[1].iov_base = buf;
	wlen = iov[0].iov_len + iov[1].iov_len + iov[2].iov_len;
	trans_lock();
	len = writev(fd, iov, 3);
	trans_unlock();
	free(buf);
	assert(len == wlen);
}

void log_write(trans_t *trans, const char *fmt, ...) {
	va_list ap;
	va_start(ap, fmt);
	log_write_(trans, fmt, ap);
	va_end(ap);
}

void log_write_single(void *user, const char *fmt, ...) {
	va_list ap;
	trans_t trans;

	log_trans_start(&trans, user);
	va_start(ap, fmt);
	log_write_(&trans, fmt, ap);
	va_end(ap);
	log_trans_end(&trans);
}

static void tag_iter(rbtree_key_t key, rbtree_value_t value) {
	tag_t *tag = (tag_t *)value;
	log_write_single(NULL, "ATG%s N%s T%d", guid_guid2str(tag->guid), tag->name, tag->type);
}

static void post_iter(rbtree_key_t key, rbtree_value_t value) {
	trans_t trans;
	post_t *post = (post_t *)value;
	const char *md5;
	post_taglist_t *tl;

	md5 = md5_md52str(post->md5);
	log_trans_start(&trans, NULL);
	log_write(&trans, "AP%s width=%d height=%d created=%llu score=%d filetype=%d rating=%d", md5, post->width, post->height, (unsigned long long)post->created, post->score, post->filetype, post->rating);
	if (post->source) {
		log_write(&trans, "MP%s source=%s", md5, str_str2enc(post->source));
	}
	if (post->title) {
		log_write(&trans, "MP%s title=%s", md5, str_str2enc(post->title));
	}
	tl = &post->tags;
	while (tl) {
		int i;
		for (i = 0; i < POST_TAGLIST_PER_NODE; i++) {
			if (tl->tags[i]) {
				log_write(&trans, "TP%s TG%s", md5, guid_guid2str(tl->tags[i]->guid));
			}
		}
		tl = tl->next;
	}
	tl = post->weak_tags;
	while (tl) {
		int i;
		for (i = 0; i < POST_TAGLIST_PER_NODE; i++) {
			if (tl->tags[i]) {
				log_write(&trans, "TP%s TG~%s", md5, guid_guid2str(tl->tags[i]->guid));
			}
		}
		tl = tl->next;
	}
	log_trans_end(&trans);
}

extern rbtree_head_t *tagtree;
extern rbtree_head_t *posttree;

int dump_log(const char *filename) {
	fd = open(filename, O_WRONLY | O_CREAT | O_EXCL, 0666);
	if (fd < 0) return 1;
	do_sync = 0;
	rbtree_iterate(tagtree, tag_iter);
	rbtree_iterate(posttree, post_iter);
	do_sync = 1;
	trans_sync();
	if (close(fd)) return 1;
	fd = -1;
	return 0;
}
