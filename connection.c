#include "db.h"

#include <stdarg.h>

/* Keep synced to error_t in db.h */
static const char *errors[] = {
	"line too long",
	"read",
	"unknown command",
	"syntax error",
	"overflow",
	"out of memory",
};

static void list_newlist(list_head_t *list) {
	list->head = (list_node_t *)&list->tail;
	list->tail = NULL;
	list->tailpred = (list_node_t *)list;
}

static void list_addtail(list_head_t *list, list_node_t *node) {
	node->succ = (list_node_t *)&list->tail;
	node->pred = list->tailpred;
	list->tailpred->succ = node;
	list->tailpred = node;
}

static void list_remove(list_node_t *node) {
	node->pred->succ = node->succ;
	node->succ->pred = node->pred;
}

int c_init(connection_t **res_conn, int sock, user_t *user,
           prot_err_func_t error) {
	connection_t *conn;

	conn = malloc(sizeof(*conn));
	if (!conn) return 1;
	memset(conn, 0, sizeof(*conn));
	list_newlist(&conn->mem_list);
	conn->mem_used = sizeof(*conn);
	conn->sock  = sock;
	conn->user  = user;
	conn->error = error;
	conn->flags = CONNFLAG_GOING;
	*res_conn = conn;
	return 0;
}

void c_cleanup(connection_t *conn) {
	list_node_t *node;
	list_node_t *next;

	node = conn->mem_list.head;
	while ((next = node->succ)) {
		free(node);
		node = next;
	}
	free(conn);
}

int c_alloc(connection_t *conn, void **res, unsigned int size) {
	unsigned int new_used;
	unsigned int new_size;
	void *mem;

	new_size = size + sizeof(list_node_t);
	assert(new_size > size);
	new_used = conn->mem_used + new_size;
	assert(new_used > conn->mem_used && new_used > new_size);
	mem = malloc(new_size);
	if (mem) {
		list_node_t *node = mem;
		list_addtail(&conn->mem_list, node);
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
int c_realloc(connection_t *conn, void **res, unsigned int old_size,
              unsigned int new_size) {
	void *new;
	int   r;

	assert(old_size < new_size);
	r = c_alloc(conn, &new, new_size);
	if (old_size) {
		assert(*res);
		if (!r) memcpy(new, *res, old_size);
		c_free(conn, *res, old_size);
	} else {
		assert(!*res);
	}
	*res = new;
	return r;
}

void c_free(connection_t *conn, void *mem, unsigned int size) {
	unsigned int new_used;
	unsigned int new_size;
	list_node_t *node;

	new_size = size + sizeof(list_node_t);
	assert(new_size > size);
	node = ((list_node_t *)mem) - 1;
	list_remove(node);
	assert(node->size == size);
	new_used = conn->mem_used - new_size;
	assert(new_used < conn->mem_used);
	conn->mem_used = new_used;
	free(node);
}

void c_flush(connection_t *conn) {
	if (conn->outlen) {
		ssize_t w = write(conn->sock, conn->outbuf, conn->outlen);
		assert(w > 0 && (unsigned int)w == conn->outlen);
		conn->outlen = 0;
	}
}

#define OUTBUF_MINFREE 512
void c_printf(connection_t *conn, const char *fmt, ...) {
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

void c_read_data(connection_t *conn) {
	if (conn->getlen != conn->getpos) return;
	conn->getpos = 0;
	conn->getlen = read(conn->sock, conn->getbuf, sizeof(conn->getbuf));
	if (conn->getlen <= 0) c_close_error(conn, E_READ);
}

int c_get_line(connection_t *conn) {
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

int c_error(connection_t *conn, const char *what) {
	c_printf(conn, "RE %s\n", what);
	return 1;
}

int c_close_error(connection_t *conn, error_t e) {
	c_printf(conn, "E%d %s\n", e, errors[e]);
	c_flush(conn);
	conn->flags &= ~CONNFLAG_GOING;
	return 1;
}
