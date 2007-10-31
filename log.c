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
	
	trans->init_len = 0;
	trans->buf_used = 0;
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

static void trans_line_done_(trans_t *trans) {
	char idbuf[12];
	struct iovec iov[3];
	int len, wlen;

	if (trans->buf_used == trans->init_len) return;
	iov[0].iov_base = idbuf;
	iov[0].iov_len  = snprintf(idbuf, sizeof(idbuf), "D%08x ", trans->id);
	assert(iov[0].iov_len == 10);
	iov[1].iov_base = trans->buf;
	iov[1].iov_len  = trans->buf_used;
	iov[2].iov_base = "\n";
	iov[2].iov_len  = 1;
char *ptr = trans->buf;
while (*ptr) assert(*ptr++ != '\n');
	wlen = iov[0].iov_len + iov[1].iov_len + iov[2].iov_len;
	trans_lock();
	len = writev(fd, iov, 3);
	trans_unlock();
	assert(len == wlen);
	trans->buf_used = trans->init_len;
}

static void log_write_(trans_t *trans, const char *fmt, va_list ap) {
	int looped = 0;
	int len;
again:
	if (trans->buf_used) {
		trans->buf[trans->buf_used] = ' ';
		trans->buf_used++;
	}
	len = vsnprintf(trans->buf + trans->buf_used, sizeof(trans->buf) - trans->buf_used, fmt, ap);
	if (trans->buf_used + len >= sizeof(trans->buf)) {
		assert(!looped);
		trans_line_done_(trans);
		looped = 1;
		goto again;
	}
	trans->buf_used += len;
	if (trans->init_len == 0                         // No init -> lines can't be appended to
	 || trans->buf_used + 20 > sizeof(trans->buf)) { // Nothing more will fit.
		trans_line_done_(trans);
	}
}

void log_trans_end(trans_t *trans) {
	off_t pos, r2;
	int   r;
	char  buf[12];
	int   len;
	
	trans_line_done_(trans);
	len = snprintf(buf, sizeof(buf), "E%08x\n", trans->id);
	assert(len == 10);
	trans_lock();
	len = write(fd, buf, 10);
	assert(len == 10);
	trans_unlock();
	trans_sync();
	trans_lock();
	pos = lseek(fd, trans->mark_offset, SEEK_SET);
	assert(pos == trans->mark_offset);
	r = write(fd, "O", 1);
	r2  = lseek(fd, 0, SEEK_END);
	assert(r == 1);
	assert(r2 != -1);
	trans_unlock();
	assert(pos == trans->mark_offset);
}

void log_set_init(trans_t *trans, const char *fmt, ...) {
	va_list ap;

	log_clear_init(trans);
	va_start(ap, fmt);
	trans->init_len = vsnprintf(trans->buf, sizeof(trans->buf), fmt, ap);
	va_end(ap);
	assert(trans->init_len < sizeof(trans->buf));
	trans->buf_used = trans->init_len;
}

void log_clear_init(trans_t *trans) {
	trans_line_done_(trans);
	trans->buf_used = trans->init_len = 0;
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

static trans_t dump_trans;
static void tag_iter(rbtree_key_t key, rbtree_value_t value) {
	tag_t *tag = (tag_t *)value;
	log_write(&dump_trans, "ATG%s N%s T%s", guid_guid2str(tag->guid), tag->name, tagtype_names[tag->type]);
}


static void tagalias_iter(rbtree_key_t key, rbtree_value_t value) {
	tagalias_t *tagalias = (tagalias_t *)value;
	log_write(&dump_trans, "AAG%s N%s", guid_guid2str(tagalias->tag->guid), tagalias->name);
}

static void post_taglist(post_taglist_t *tl) {
	while (tl) {
		int i;
		for (i = 0; i < POST_TAGLIST_PER_NODE; i++) {
			if (tl->tags[i]) {
				log_write(&dump_trans, "T%s", guid_guid2str(tl->tags[i]->guid));
			}
		}
		tl = tl->next;
	}
}

static void post_iter(rbtree_key_t key, rbtree_value_t value) {
	post_t *post = (post_t *)value;
	const char *md5;

	md5 = md5_md52str(post->md5);
	log_write(&dump_trans, "AP%s width=%d height=%d created=%llu score=%d filetype=%s rating=%s", md5, post->width, post->height, (unsigned long long)post->created, post->score, filetype_names[post->filetype], rating_names[post->rating]);
	log_set_init(&dump_trans, "MP%s", md5);
	if (post->source) {
		log_write(&dump_trans, "source=%s", str_str2enc(post->source));
	}
	if (post->title) {
		log_write(&dump_trans, "title=%s", str_str2enc(post->title));
	}
	log_set_init(&dump_trans, "TP%s", md5);
	post_taglist(&post->tags);
	post_taglist(post->weak_tags);
	log_clear_init(&dump_trans);
}

static void user_iter(rbtree_key_t key, rbtree_value_t value) {
	user_t *user = (user_t *)value;
	char   *name, *pass;
	int    i;

	name = strdup(str_str2enc(user->name));
	pass = strdup(str_str2enc(user->password));
	log_write(&dump_trans, "AUN%s P%s", name, pass);
	log_set_init(&dump_trans, "MUN%s", name);
	for (i = 0; (1UL << i) <= CAP_MAX; i++) {
		capability_t cap = 1UL << i;
		if (DEFAULT_CAPS & cap) {
			if (!(user->caps & cap)) log_write(&dump_trans, "c%s", cap_names[i]);
		} else {
			if (user->caps & cap) log_write(&dump_trans, "C%s", cap_names[i]);
		}
	}
	log_clear_init(&dump_trans);
	free(pass);
	free(name);
}

int dump_log(const char *filename) {
	int org_fd = fd;
	int r = 1;
	fd = open(filename, O_WRONLY | O_CREAT | O_EXCL, 0666);
	err1(fd < 0);
	log_trans_start(&dump_trans, NULL);
	rbtree_iterate(usertree, user_iter);
	rbtree_iterate(tagtree, tag_iter);
	rbtree_iterate(tagaliastree, tagalias_iter);
	rbtree_iterate(posttree, post_iter);
	log_trans_end(&dump_trans);
	err1(close(fd));
	r = 0;
err:
	fd = org_fd;
	return r;
}

void log_init(const char *filename) {
	int   r;
	off_t o;

	fd = open(filename, O_WRONLY | O_CREAT | O_EXLOCK, 0666);
	assert(fd >= 0);
	o = lseek(fd, 0, SEEK_END);
	assert(o != -1);
	r = write(fd, "\n", 1); // Old unclean shutdown could leave an incomplete line.
	assert(r == 1);
}
