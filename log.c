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
	unsigned int len, r;
	
	(void)user;

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
	char newline = '\n';
	int len, wlen;

	if (trans->buf_used == trans->init_len) return;
	iov[0].iov_base = idbuf;
	iov[0].iov_len  = snprintf(idbuf, sizeof(idbuf), "D%08x ", trans->id);
	assert(iov[0].iov_len == 10);
	iov[1].iov_base = trans->buf;
	iov[1].iov_len  = trans->buf_used;
	iov[2].iov_base = &newline;
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

static void log_write_(trans_t *trans, int complete,
                       const char *fmt, va_list ap) {
	int looped = 0;
	int len;
again:
	if (trans->buf_used) {
		assert(trans->buf_used < sizeof(trans->buf));
		trans->buf[trans->buf_used] = ' ';
		trans->buf_used++;
	}
	len = vsnprintf(trans->buf + trans->buf_used,
	                sizeof(trans->buf) - trans->buf_used, fmt, ap);
	if (trans->buf_used + len >= sizeof(trans->buf)) {
		assert(complete && !looped);
		trans_line_done_(trans);
		looped = 1;
		goto again;
	}
	trans->buf_used += len;
	if (!complete) return;
	if (trans->init_len == 0    // No init -> lines can't be appended to.
	 || trans->buf_used + 20 > sizeof(trans->buf)) { // No more will fit.
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
}

void log_set_init(trans_t *trans, const char *fmt, ...) {
	va_list ap;

	if (!trans) return;
	log_clear_init(trans);
	va_start(ap, fmt);
	trans->init_len = vsnprintf(trans->buf, sizeof(trans->buf), fmt, ap);
	va_end(ap);
	assert(trans->init_len < sizeof(trans->buf));
	trans->buf_used = trans->init_len;
}

void log_clear_init(trans_t *trans) {
	if (!trans) return;
	trans_line_done_(trans);
	trans->buf_used = trans->init_len = 0;
}

void log_write(trans_t *trans, const char *fmt, ...) {
	va_list ap;
	if (!trans) return;
	va_start(ap, fmt);
	log_write_(trans, 1, fmt, ap);
	va_end(ap);
}

static void log_write_nl(trans_t *trans, int last, const char *fmt, ...) {
	va_list ap;
	if (!trans) return;
	va_start(ap, fmt);
	log_write_(trans, last, fmt, ap);
	va_end(ap);
}

void log_write_single(void *user, const char *fmt, ...) {
	va_list ap;
	trans_t trans;

	log_trans_start(&trans, user);
	va_start(ap, fmt);
	log_write_(&trans, 1, fmt, ap);
	va_end(ap);
	log_trans_end(&trans);
}

void log_write_tag(trans_t *trans, tag_t *tag) {
	log_write(trans, "ATG%s N%s T%s", guid_guid2str(tag->guid),
	          tag->name, tagtype_names[tag->type]);
}

void log_write_tagalias(trans_t *trans, tagalias_t *tagalias) {
	log_write(trans, "AAG%s N%s", guid_guid2str(tagalias->tag->guid),
	          tagalias->name);
}

static void log_int_field(trans_t *trans, int last, const void *data,
                          const field_t *field) {
	const char         *fp;
	const char         *fmt;
	unsigned long long value;

	fp = ((const char *)data) + field->offset;
	if (field->size == 8) {
		value = *(const uint64_t *)fp;
	} else if (field->size == 4) {
		value = *(const uint32_t *)fp;
	} else {
		assert(field->size == 2);
		value = *(const uint16_t *)fp;
	}
	fmt = (field->type == FIELDTYPE_SIGNED ? "%s=%lld" : "%s=%llu");
	log_write_nl(trans, last, fmt, field->name, value);
}

static void log_enum_field(trans_t *trans, int last, const void *data,
                           const field_t *field) {
	const char * const *array = *field->array;
	uint16_t   value = *(const uint16_t *)(((const char *)data) + field->offset);
	log_write_nl(trans, last, "%s=%s", field->name, array[value]);
}

static void log_string_field(trans_t *trans, int last, const void *data,
                           const field_t *field) {
	const char *value = *(const char * const *)(((const char *)data) + field->offset);
	if (value) {
		log_write_nl(trans, last, "%s=%s", field->name, str_str2enc(value));
	}
}

void log_write_post(trans_t *trans, post_t *post) {
	const field_t *field = post_fields;
	const char    *md5 = md5_md52str(post->md5);
	void (*func[])(trans_t *, int, const void *, const field_t *) = {
	                log_int_field,
	                log_int_field,
	                log_enum_field,
	                log_string_field,
	};

	log_write_nl(trans, 0, "AP%s", md5);
	while (field->name) {
		func[field->type](trans, !field[1].name, post, field);
		field++;
	}
}

void log_write_user(trans_t *trans, user_t *user) {
	char *name;
	int  i;

	name = strdup(str_str2enc(user->name));
	log_write(trans, "AUN%s P%s", name, str_str2enc(user->password));
	log_set_init(trans, "MUN%s", name);
	free(name);
	for (i = 0; (1UL << i) <= CAP_MAX; i++) {
		capability_t cap = 1UL << i;
		if (DEFAULT_CAPS & cap) {
			if (!(user->caps & cap)) {
				log_write(trans, "c%s", cap_names[i]);
			}
		} else {
			if (user->caps & cap) {
				log_write(trans, "C%s", cap_names[i]);
			}
		}
	}
	log_clear_init(trans);
}

extern uint64_t *logindex;
static const char *logdir;
#define LOG_ROTATE_SIZE (1024 * 1024)

void log_rotate(int force) {
	char filename[1024];
	int  len;

	if (!force) {
		off_t size;
		trans_lock();
		size = lseek(fd, 0, SEEK_CUR);
		trans_unlock();
		if (size < LOG_ROTATE_SIZE) return;
	}

	len = snprintf(filename, sizeof(filename), "%s/%llu.log", logdir,
	               (unsigned long long)*logindex);
	assert(len < (int)sizeof(filename));
	if (fd != -1) close(fd);
	fd = open(filename, O_WRONLY | O_CREAT | O_EXLOCK, 0666);
	assert(fd != -1);
	*logindex += 1;
}

void log_init(const char *dirname) {
	logdir = strdup(dirname);
	assert(logdir);
	log_rotate(1);
}

/********************************
 ** Below here is only dumping **
 ********************************/
static trans_t dump_trans;
static void tag_iter(rbtree_key_t key, rbtree_value_t value) {
	(void)key;
	log_write_tag(&dump_trans, (tag_t *)value);
}

static void tagalias_iter(rbtree_key_t key, rbtree_value_t value) {
	(void)key;
	log_write_tagalias(&dump_trans, (tagalias_t *)value);
}

static void post_taglist(post_taglist_t *tl, const char *prefix) {
	while (tl) {
		int i;
		for (i = 0; i < POST_TAGLIST_PER_NODE; i++) {
			if (tl->tags[i]) {
				log_write(&dump_trans, "T%s%s", prefix,
				          guid_guid2str(tl->tags[i]->guid));
			}
		}
		tl = tl->next;
	}
}

static void post_iter(rbtree_key_t key, rbtree_value_t value) {
	post_t *post = (post_t *)value;

	(void)key;
	log_write_post(&dump_trans, post);
	log_set_init(&dump_trans, "TP%s", md5_md52str(post->md5));
	post_taglist(&post->tags, "");
	post_taglist(post->weak_tags, "~");
	log_clear_init(&dump_trans);
}

static void user_iter(rbtree_key_t key, rbtree_value_t value) {
	(void)key;
	log_write_user(&dump_trans, (user_t *)value);
}

void log_dump(void) {
	log_rotate(1);
	log_trans_start(&dump_trans, NULL);
	rbtree_iterate(usertree, user_iter);
	rbtree_iterate(tagtree, tag_iter);
	rbtree_iterate(tagaliastree, tagalias_iter);
	rbtree_iterate(posttree, post_iter);
	log_trans_end(&dump_trans);
	log_rotate(1);
}
