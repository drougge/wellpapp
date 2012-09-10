#include "db.h"

#include <stdarg.h>

/* Keep synced to dberror_t in db.h */
static const char *errors[] = {
	"line too long",
	"read",
	"unknown command",
	"syntax error",
	"overflow",
	"out of memory",
	"bad utf8 sequence",
};

int c_init(connection_t **res_conn, int sock, prot_err_func_t error)
{
	connection_t *conn;

	conn = malloc(sizeof(*conn));
	if (!conn) return 1;
	memset(conn, 0, sizeof(*conn));
	mem_newlist(&conn->mem_list);
	conn->mem_used = sizeof(*conn);
	conn->sock  = sock;
	conn->error = error;
	conn->flags = CONNFLAG_GOING;
	*res_conn = conn;
	return 0;
}

void c_cleanup(connection_t *conn)
{
	if (conn->trans.flags & TRANSFLAG_GOING) {
		log_trans_end(conn);
	}
	if (conn->trans.flags & TRANSFLAG_OUTER) {
		log_trans_end_outer(conn);
	}
	mem_node_t *node = conn->mem_list.head;
	while (node) {
		mem_node_t *next = node->succ;
		free(node);
		node = next;
	}
	free(conn);
}

int c_alloc(connection_t *conn, void **res, unsigned int size)
{
	unsigned int new_used;
	unsigned int new_size;
	void *mem;

	new_size = size + sizeof(mem_node_t);
	assert(new_size > size);
	new_used = conn->mem_used + new_size;
	assert(new_used > conn->mem_used && new_used > new_size);
	mem = malloc(new_size);
	if (mem) {
		mem_node_t *node = mem;
		mem_addtail(&conn->mem_list, node);
		node->size = size;
		conn->mem_used = new_used;
		*res = node + 1;
		return 0;
	} else {
		*res = NULL;
		return 1;
	}
}

/* Can only expand allocation. Leaves old allocation in case of failure. */
void *c_realloc(connection_t *conn, void *ptr, unsigned int old_size,
                unsigned int new_size, int *res)
{
	void *new;

	assert(old_size < new_size);
	*res = c_alloc(conn, &new, new_size);
	if (*res) return ptr;
	if (old_size) {
		assert(ptr);
		memcpy(new, ptr, old_size);
		c_free(conn, ptr, old_size);
	} else {
		assert(!ptr);
	}
	return new;
}

void c_free(connection_t *conn, void *mem, unsigned int size)
{
	unsigned int new_used;
	unsigned int new_size;
	mem_node_t   *node;

	new_size = size + sizeof(*node);
	assert(new_size > size);
	node = ((mem_node_t *)mem) - 1;
	mem_remove(&conn->mem_list, node);
	assert(node->size == size);
	new_used = conn->mem_used - new_size;
	assert(new_used < conn->mem_used);
	conn->mem_used = new_used;
	free(node);
}

void c_flush(connection_t *conn)
{
	const char *buf = conn->outbuf;
	ssize_t left = conn->outlen;
	while (left && (conn->flags & CONNFLAG_GOING)) {
		ssize_t w = write(conn->sock, buf, left);
		if (w < 0) {
			conn->flags &= ~CONNFLAG_GOING;
		} else {
			left -= w;
			buf += w;
		}
	}
	conn->outlen = 0;
}

#define OUTBUF_MINFREE 512
void c_printf(connection_t *conn, const char *fmt, ...)
{
	va_list ap;
	int     len;

	va_start(ap, fmt);
	len = vsnprintf(conn->outbuf + conn->outlen,
	                sizeof(conn->outbuf) - conn->outlen, fmt, ap);
	if (len >= (int)(sizeof(conn->outbuf) - conn->outlen)) { // Overflow
		c_flush(conn);
		len = vsnprintf(conn->outbuf, sizeof(conn->outbuf), fmt, ap);
		assert(len < (int)sizeof(conn->outbuf));
	}
	va_end(ap);
	conn->outlen += len;
	if (conn->outlen + OUTBUF_MINFREE > sizeof(conn->outbuf)) c_flush(conn);
}

void c_read_data(connection_t *conn)
{
	if (conn->getlen != conn->getpos) return;
	conn->getpos = 0;
	conn->getlen = read(conn->sock, conn->getbuf, sizeof(conn->getbuf));
	if (conn->getlen <= 0) c_close_error(conn, E_READ);
}

int c_get_line(connection_t *conn)
{
	unsigned int size = sizeof(conn->linebuf);

	if (!(conn->flags & CONNFLAG_GOING)) return -1;
	while (size > conn->linelen) {
		if (conn->getlen > conn->getpos) {
			char c = conn->getbuf[conn->getpos];
			conn->getpos++;
			/* \r is ignored, for easier testing with telnet */
			if (c == '\n') {
				int len = conn->linelen;
				conn->linebuf[conn->linelen] = 0;
				conn->linelen = 0;
				return len;
			} else if (c != '\r') {
				conn->linebuf[conn->linelen] = c;
				conn->linelen++;
			}
		} else {
			return 0;
		}
	}
	c_close_error(conn, E_LINETOOLONG);
	return -1;
}

int c_error(connection_t *conn, const char *what)
{
	c_printf(conn, "RE %s\n", what);
	return 1;
}

int c_close_error(connection_t *conn, dberror_t e)
{
	c_printf(conn, "E%d %s\n", e, errors[e]);
	c_flush(conn);
	conn->flags &= ~CONNFLAG_GOING;
	return 1;
}
