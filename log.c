#include "db.h"

#include <stdarg.h>
#include <sys/file.h>

static int log_fd;
static trans_id_t next_trans_id = 1;

static void trans_lock(trans_t *trans)
{
	(void) trans;
}

static void trans_unlock(trans_t *trans)
{
	(void) trans;
}

static void trans_sync(trans_t *trans)
{
	if (trans->flags & TRANSFLAG_SYNC) {
		int r = fsync(trans->fd);
		assert(!r);
	}
}

transflag_t transflags_default = TRANSFLAG_SYNC;

static int log_trans_start_(trans_t *trans, time_t now, int fd, int outer)
{
	char buf[36];
	unsigned int len, r;
	
	trans->init_len = 0;
	trans->buf_used = 0;
	if (outer) {
		if (trans->flags & TRANSFLAG_OUTER) return 1;
	} else {
		assert(!(trans->flags & TRANSFLAG_GOING));
	}
	if (trans->flags & TRANSFLAG_OUTER) {
		trans->flags |= TRANSFLAG_GOING;
		return 0;
	}
	if (outer) {
		trans->flags = TRANSFLAG_OUTER;
	} else {
		trans->flags = TRANSFLAG_GOING;
	}
	trans->flags   |= transflags_default;
	trans->fd       = fd;
	trans->conn     = NULL;
	trans->now      = now;
	trans->id = next_trans_id++;
	len = snprintf(buf, sizeof(buf), "T%llxU%cT%llx\n",
	               ULL trans->id, '0' + LOG_VERSION, ULL trans->now);
	assert(len <= 35);
	trans_lock(trans);
	r = write(trans->fd, buf, len);
	trans->mark_offset = lseek(trans->fd, 0, SEEK_CUR);
	trans_unlock(trans);
	assert(r == len);
	assert(trans->mark_offset != -1);
	trans->mark_offset -= len - (strchr(buf, 'U') - buf);
	return 0;
}

void log_trans_start(connection_t *conn, time_t now)
{
	(void) log_trans_start_(&conn->trans, now, log_fd, 0);
	conn->trans.conn = conn;
}

int log_trans_start_outer(connection_t *conn, time_t now)
{
	int r = log_trans_start_(&conn->trans, now, log_fd, 1);
	conn->trans.conn = conn;
	return r;
}

static void trans_line_done_(trans_t *trans)
{
	char idbuf[20];
	struct iovec iov[3];
	char newline = '\n';
	int len, wlen;

	if (trans->buf_used == trans->init_len) return;
	iov[0].iov_base = idbuf;
	iov[0].iov_len  = snprintf(idbuf, sizeof(idbuf), "D%llx ",
	                           (unsigned long long)trans->id);
	assert(iov[0].iov_len <= 18);
	iov[1].iov_base = trans->buf;
	iov[1].iov_len  = trans->buf_used;
	iov[2].iov_base = &newline;
	iov[2].iov_len  = 1;
char *ptr = trans->buf;
while (*ptr) assert(*ptr++ != '\n');
	wlen = iov[0].iov_len + iov[1].iov_len + iov[2].iov_len;
	trans_lock(trans);
	len = writev(trans->fd, iov, 3);
	trans_unlock(trans);
	assert(len == wlen);
	trans->buf_used = trans->init_len;
}

static void log_write_(trans_t *trans, int complete,
                       const char *fmt, va_list ap)
{
	int looped = 0;
	int len;

	if (trans->conn && trans->conn->flags & CONNFLAG_LOG) {
		return; // Log-reader doesn't log
	}
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

static int log_trans_end_(trans_t *trans, int outer)
{
	off_t pos, r2;
	int   r;
	char  buf[20];
	int   len, wlen;
	
	if (outer) {
		if (!(trans->flags & TRANSFLAG_OUTER)) return 1;
	} else {
		assert(trans->flags & TRANSFLAG_GOING);
	}
	log_clear_init(trans);
	trans->flags &= ~TRANSFLAG_GOING;
	if (trans->flags & TRANSFLAG_OUTER && !outer) return 0;
	len = snprintf(buf, sizeof(buf), "E%llx\n", ULL trans->id);
	assert(len <= 18);
	trans_lock(trans);
	wlen = write(trans->fd, buf, len);
	assert(wlen == len);
	trans_unlock(trans);
	trans_sync(trans);
	trans_lock(trans);
	pos = lseek(trans->fd, trans->mark_offset, SEEK_SET);
	assert(pos == trans->mark_offset);
	r  = write(trans->fd, "O", 1);
	r2 = lseek(trans->fd, 0, SEEK_END);
	assert(r == 1);
	assert(r2 != -1);
	trans_unlock(trans);
	trans->flags = 0;
	return 0;
}

void log_trans_end(connection_t *conn)
{
	(void) log_trans_end_(&conn->trans, 0);
}

int log_trans_end_outer(connection_t *conn)
{
	return log_trans_end_(&conn->trans, 1);
}

void log_set_init(trans_t *trans, const char *fmt, ...)
{
	va_list ap;

	if (!trans) return;
	log_clear_init(trans);
	va_start(ap, fmt);
	trans->init_len = vsnprintf(trans->buf, sizeof(trans->buf), fmt, ap);
	va_end(ap);
	assert(trans->init_len < sizeof(trans->buf));
	trans->buf_used = trans->init_len;
}

void log_clear_init(trans_t *trans)
{
	if (!trans) return;
	trans_line_done_(trans);
	trans->buf_used = trans->init_len = 0;
}

void log_write(trans_t *trans, const char *fmt, ...)
{
	va_list ap;
	if (!trans) return;
	va_start(ap, fmt);
	log_write_(trans, 1, fmt, ap);
	va_end(ap);
}

static void log_write_nl(trans_t *trans, int last, const char *fmt, ...)
{
	va_list ap;
	if (!trans) return;
	va_start(ap, fmt);
	log_write_(trans, last, fmt, ap);
	va_end(ap);
}

void log_write_tag(trans_t *trans, const tag_t *tag, int is_add,
                   int write_flags, guid_t *merge)
{
	char mbuf[32] = "";
	char fbuf[32] = "";
	if (merge) {
		snprintf(mbuf, sizeof(mbuf), " M%s", guid_guid2str(*merge));
	}
	if (write_flags) {
		const char *minus = "";
		if (!tag->unsettable) minus = "-";
		snprintf(fbuf, sizeof(fbuf), " F%sunsettable", minus);
	}
	log_write(trans, "%cTG%s N%s T%s%s%s V%s", is_add ? 'A' : 'M',
	          guid_guid2str(tag->guid), tag->name,
	          tagtype_names[tag->type], fbuf, mbuf,
	          tag_value_types[tag->valuetype]);
}

void log_write_tagalias(trans_t *trans, const tagalias_t *tagalias)
{
	log_write(trans, "AAG%s N%s", guid_guid2str(tagalias->tag->guid),
	          tagalias->name);
}

static const char *tag_value_str(tag_t *tag, tag_value_t *tval, char *buf)
{
	switch (tag->valuetype) {
		case VT_WORD:
		case VT_DATETIME:
			return tval->v_str;
			break;
		case VT_UINT:
			sprintf(buf, "%llx", ULL tval->val.v_uint);
			return buf;
			break;
		case VT_INT:
			sprintf(buf, "%lld", LL tval->val.v_int);
			return buf;
			break;
		default:
			assert("BUG" == 0);
			break;
	}
	return NULL; // NOTREACHED
}

void log_write_post(trans_t *trans, const post_t *post)
{
	const field_t *field = post_fields;
	const char    *md5 = md5_md52str(post->md5);
	int           last = 0;

	log_write_nl(trans, 0, "AP%s", md5);
	do {
		last = !(field[1].name && field[1].log_version >= LOG_VERSION);
		tag_t *tag = *field->magic_tag;
		tag_value_t *tval = NULL;
		int skip = 0;
		if (tag == magic_tag_created || tag == magic_tag_modified) {
			tval = post_tag_value(post, tag);
			if (!tval || datetime_get_simple(&tval->val.v_datetime) == trans->now) {
				skip = 1;
			}
		}
		if (!skip) {
			if (!tval) tval = post_tag_value(post, tag);
			if (tval) {
				char buf[32];
				const char *sval;
				sval = tag_value_str(tag, tval, buf);
				log_write_nl(trans, last, "%s=%s",
				             field->name, sval);
			}
		}
		field++;
	} while (!last);
}

void log_init(void)
{
	char filename[1024];
	int  len;

	len = snprintf(filename, sizeof(filename), "%s/log/%016llx",
	               basedir, (unsigned long long)*logindex);
	assert(len < (int)sizeof(filename));
	log_fd = open(filename, O_WRONLY | O_CREAT | O_EXCL, 0666);
	assert(log_fd != -1);
	*logindex += 1;
}

void log_cleanup(void)
{
	struct stat sb;
	(void) fsync(log_fd);
	int r = fstat(log_fd, &sb);
	close(log_fd);
	if (!r && !sb.st_size) {
		char filename[1024];
		int  len;
		len = snprintf(filename, sizeof(filename), "%s/log/%016llx",
			       basedir, (unsigned long long)*logindex - 1);
		assert(len < (int)sizeof(filename));
		if (!unlink(filename)) *logindex -= 1;
	} else if (!r) {
		mm_last_log(sb.st_size, sb.st_mtime);
	}
}

/********************************
 ** Below here is only dumping **
 ********************************/
static void tag_iter(ss128_key_t key, ss128_value_t value, void *trans)
{
	tag_t *tag = (tag_t *)value;
	(void)key;
	log_write_tag(trans, tag, 1, tag->unsettable, NULL);
}

static void tag_iter_impl(ss128_key_t key, ss128_value_t value, void *trans)
{
	tag_t *tag = (tag_t *)value;
	impllist_t *l;
	(void)key;
	l = tag->implications;
	char guid[7*4];
	memcpy(guid, guid_guid2str(tag->guid), sizeof(guid));
	while (l) {
		for (int i = 0; i < arraylen(l->impl); i++) {
			if (l->impl[i].tag) {
				log_write(trans, "I%s %c%s:%d", guid,
				          l->impl[i].positive ? 'I' : 'i',
				          guid_guid2str(l->impl[i].tag->guid),
				          l->impl[i].priority);
			}
		}
		l = l->next;
	}
}

static void tagalias_iter(ss128_key_t key, ss128_value_t value, void *trans)
{
	(void)key;
	log_write_tagalias(trans, (tagalias_t *)value);
}

static void post_taglist(trans_t *trans, post_taglist_t *taglist,
                         post_taglist_t *tl, const char *prefix)
{
	while (tl) {
		int i;
		for (i = 0; i < arraylen(tl->tags); i++) {
			if (tl->tags[i]
			    && !taglist_contains(taglist, tl->tags[i])
			   ) {
				log_write(trans, "T%s%s", prefix,
				          guid_guid2str(tl->tags[i]->guid));
			}
		}
		tl = tl->next;
	}
}

static void post_iter(ss128_key_t key, ss128_value_t value, void *fdp)
{
	post_t  *post = (post_t *)value;
	int     fd = *(int *)fdp;
	trans_t trans;
	time_t  modified;

	(void) key;
	tag_value_t *mv = post_tag_value(post, magic_tag_modified);
	modified = datetime_get_simple(&mv->val.v_datetime);
	(void) log_trans_start_(&trans, modified, fd, 0);
	trans.flags &= ~TRANSFLAG_SYNC;
	log_write_post(&trans, post);
	log_set_init(&trans, "TP%s", md5_md52str(post->md5));
	post_taglist(&trans, post->implied_tags, &post->tags, "");
	post_taglist(&trans, post->implied_weak_tags, post->weak_tags, "~");
	(void) log_trans_end_(&trans, 0);
}

void log_dump(void)
{
	char    filename[1024];
	int     len, w;
	char    buf[20];
	trans_t dump_trans;
	int     dump_fd;

	len = snprintf(filename, sizeof(filename), "%s/dump/%016llx",
	               basedir, (unsigned long long)*logdumpindex);
	assert(len < (int)sizeof(filename));
	dump_fd = open(filename, O_WRONLY | O_CREAT | O_EXCL, 0666);
	assert(dump_fd != -1);
	*logdumpindex += 1;

	/* Tags, tagaliases and implications are dumped in a single          *
	 * transaction with the current time. Posts are dumped in individual *
	 * transactions with the modification time of the post.              */
	(void) log_trans_start_(&dump_trans, time(NULL), dump_fd, 0);
	ss128_iterate(tags, tag_iter, &dump_trans);
	ss128_iterate(tags, tag_iter_impl, &dump_trans);
	ss128_iterate(tagaliases, tagalias_iter, &dump_trans);
	(void) log_trans_end_(&dump_trans, 0);
	ss128_iterate(posts, post_iter, &dump_fd);

	len = snprintf(buf, sizeof(buf), "L%016llx\n",
	               (unsigned long long)*logindex);
	assert(len == 18);
	w = write(dump_fd, buf, len);
	assert(len == w);
	close(dump_fd);
}
