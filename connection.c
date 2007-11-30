#include "db.h"

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
	mem = malloc(size);
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

int c_realloc(connection_t *conn, void **res, unsigned int old_size,
              unsigned int new_size) {
	void *new;
	int   r;

	assert(old_size < new_size);
	r = c_alloc(conn, &new, new_size);
	if (!r) memcpy(new, *res, old_size);
	c_free(conn, *res, old_size);
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
