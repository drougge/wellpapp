#include "db.h"

#include <openssl/md5.h>

struct ss128_node {
	struct ss128_node *child[2];
	struct ss128_node *parent;
	ss128_key_t       key;
	ss128_value_t     value;
	unsigned int      red : 1;
};

typedef enum {
	RBTREE_ALLOCATION_POLICY_NORMAL,
	RBTREE_ALLOCATION_POLICY_PREALLOC,
	RBTREE_ALLOCATION_POLICY_CHUNKED
} rbtree_allocation_policy_t;

int ss128_init(ss128_head_t *head, ss128_allocmem_t allocmem, ss128_freemem_t freemem, void *memarg) {
	head->allocmem          = allocmem;
	head->freemem           = freemem;
	head->memarg            = memarg;
	head->root              = NULL;
	head->freelist          = NULL;
	head->chunklist         = NULL;
	head->allocation_policy = RBTREE_ALLOCATION_POLICY_CHUNKED;
	head->allocation_value  = 256;
	return 0;
}


#define panic(bs, a) do_panic(a)
static void do_panic(const char *msg) {
	fprintf(stderr, "rbtree.c: %s", msg);
	exit(1);
}

#define ss128_allocmem(a, b) head->allocmem(head->memarg, a, b)
#define ss128_freemem(a, b)  head->freemem(head->memarg, a, b)

static void ss128_iterate_i(ss128_node_t *node, ss128_callback_t callback,
                            void *data) {
	if (node->child[0]) ss128_iterate_i(node->child[0], callback, data);
	callback(node->key, node->value, data);
	if (node->child[1]) ss128_iterate_i(node->child[1], callback, data);
}

void ss128_iterate(ss128_head_t *head, ss128_callback_t callback, void *data) {
	if (!head->root) return;
	ss128_iterate_i(head->root, callback, data);
}

static int rbtree_key_lt(ss128_key_t a, ss128_key_t b) {
	if (a.a < b.a) return 1;
	if (a.a == b.a && a.b < b.b) return 1;
	return 0;
}

static int rbtree_key_eq(ss128_key_t a, ss128_key_t b) {
	return a.a == b.a && a.b == b.b;
}

ss128_key_t ss128_str2key(const char *str) {
	MD5_CTX ctx;
	md5_t   md5;

	MD5_Init(&ctx);
	MD5_Update(&ctx, (const unsigned char *)str, strlen(str));
	MD5_Final(md5.m, &ctx);
	return md5.key;
}

#define ss128_thischild(p, c) ((p)->child[(p)->child[0] != (c)])
#define ss128_otherchild(p, c) ((p)->child[(p)->child[0] == (c)])
#define ss128_isred(n) ((n) && (n)->red)

static void ss128_alloc_chunk(ss128_head_t *head) {
	ss128_node_t *chunk;
	ss128_node_t *node;
	int          i;

	if (ss128_allocmem(&chunk, sizeof(*node) * head->allocation_value)) return;
	chunk->child[0] = head->chunklist;
	head->chunklist = chunk;
	node = head->freelist = chunk + 1;
	for (i = 0; i < head->allocation_value - 2; i++) {
		node->child[0] = node + 1;
		node += 1;
	}
	node->child[0] = NULL;
}

static int ss128_node_alloc(ss128_head_t *head, ss128_node_t **r_node, ss128_value_t value, ss128_key_t key) {

	switch (head->allocation_policy) {
		case RBTREE_ALLOCATION_POLICY_NORMAL:
			ss128_allocmem(r_node, sizeof(ss128_node_t));
			break;
		case RBTREE_ALLOCATION_POLICY_CHUNKED:
			if (!head->freelist) ss128_alloc_chunk(head);
			//-fallthrough
		case RBTREE_ALLOCATION_POLICY_PREALLOC:
			*r_node = head->freelist;
			if (head->freelist) head->freelist = head->freelist->child[0];
			break;
	}
	if (!*r_node) return 1;
	(*r_node)->child[0] = NULL;
	(*r_node)->child[1] = NULL;
	(*r_node)->red      = 1;
	(*r_node)->key      = key;
	(*r_node)->value    = value;
	return 0;
}

static void ss128_node_free(ss128_head_t *head, ss128_node_t *node) {
	switch (head->allocation_policy) {
		case RBTREE_ALLOCATION_POLICY_NORMAL:
			ss128_freemem(node, sizeof(*node));
			break;
		case RBTREE_ALLOCATION_POLICY_PREALLOC:
		case RBTREE_ALLOCATION_POLICY_CHUNKED:
			node->child[0] = head->freelist;
			head->freelist = node;
			break;
	}
}

static void ss128_rotate(ss128_head_t *head, ss128_node_t *node, int isright) {
	ss128_node_t *tmp;

	tmp = node->child[!isright];
	node->child[!isright] = tmp->child[isright];
	tmp->child[isright] = node;
	if (node->parent) {
		ss128_thischild(node->parent, node) = tmp;
	} else {
		head->root = tmp;
	}
	tmp->parent = node->parent;
	node->parent = tmp;
	if (node->child[!isright]) node->child[!isright]->parent = node;
}

static void ss128_balance_after_insert(ss128_head_t *head, ss128_node_t *node) {
	ss128_node_t *uncle;
	int pleft, gpleft;

	if (!node->parent) {
		node->red = 0;
		return;
	}
	if (!node->parent->red) return;
	uncle = ss128_otherchild(node->parent->parent, node->parent);
	if (ss128_isred(uncle)) {
		node->parent->red   = 0;
		uncle->red          = 0;
		uncle->parent->red  = 1;
		ss128_balance_after_insert(head, uncle->parent);
		return;
	}
	pleft  = (node->parent->child[0] == node);
	gpleft = (node->parent->parent->child[0] == node->parent);
	if (pleft && !gpleft) {
		ss128_rotate(head, node->parent, 1);
		node = node->child[1]; /* Former parent */
	} else if (!pleft && gpleft) {
		ss128_rotate(head, node->parent, 0);
		node = node->child[0]; /* Former parent */
	}
	node->parent->red         = 0;
	node->parent->parent->red = 1;
	pleft  = (node->parent->child[0] == node);
	gpleft = (node->parent->parent->child[0] == node->parent);
	if (pleft && gpleft) {
		ss128_rotate(head, node->parent->parent, 1);
	} else {
		if (!(!pleft && !gpleft)) panic(base, "tankefel\n");
		ss128_rotate(head, node->parent->parent, 0);
	}
}

int ss128_insert(ss128_head_t *head, ss128_value_t value, ss128_key_t key) {
	ss128_node_t *node;
	ss128_node_t *newnode = 0;
	int          child;

	err1(ss128_node_alloc(head, &newnode, value, key));
	node = head->root;
	if (!node) {
		newnode->parent = NULL;
		newnode->red    = 0;
		head->root = newnode;
		return 0;
	}
	while (42) {
		err1(rbtree_key_eq(node->key, key));
		child = rbtree_key_lt(node->key, key);
		if (node->child[child]) {
			node = node->child[child];
		} else {
			node->child[child] = newnode;
			break;
		}
	}
	newnode->parent = node;
	ss128_balance_after_insert(head, newnode);
	return 0;
err:
	if (newnode) ss128_node_free(head, newnode);
	return 1;
}

static void ss128_balance_before_delete(ss128_head_t *head, ss128_node_t *node) {
	ss128_node_t *sibling;
	int child;

	child = (node->parent->child[1] == node);
	sibling = node->parent->child[!child];
	if (sibling->red) {
		node->parent->red = 1;
		sibling->red      = 0;
		ss128_rotate(head, node->parent, child);
		sibling = node->parent->child[!child];
	} else if (!node->parent->red && !ss128_isred(sibling->child[0]) && !ss128_isred(sibling->child[1])) {
		sibling->red = 1;
		if (!node->parent->parent) return;
		ss128_balance_before_delete(head, node->parent);
		return;
	}
	if (node->parent->red && !sibling->red && !ss128_isred(sibling->child[0]) && !ss128_isred(sibling->child[1])) {
		node->parent->red = 0;
		sibling->red      = 1;
		return;
	}
	if (!sibling->red && ss128_isred(sibling->child[child]) && !ss128_isred(sibling->child[!child])) {
		sibling->red               = 1;
		sibling->child[child]->red = 0;
		ss128_rotate(head, sibling, !child);
		sibling = node->parent->child[!child];
	}
	if (!sibling->red && ss128_isred(sibling->child[!child])) {
		int red = node->parent->red;
		node->parent->red           = sibling->red;
		sibling->red                = red;
		sibling->child[!child]->red = 0;
		ss128_rotate(head, node->parent, child);
	}
}

int ss128_delete(ss128_head_t *head, ss128_key_t key) {
	ss128_node_t *node;
	ss128_node_t *child;

	node = head->root;
	while (42) {
		if (rbtree_key_eq(node->key, key)) break;
		node = node->child[rbtree_key_lt(node->key, key)];
		if (!node) return 1;
	}
	if (node->child[0] && node->child[1]) { /* We can't really delete this node */
		child = node->child[1];
		while (child->child[0]) child = child->child[0];
		node->key   = child->key;
		node->value = child->value;
		node = child;
	}
	child = node->child[!node->child[0]];
	if (!node->parent) {
		head->root = child;
		if (child) {
			child->parent = NULL;
			child->red    = 0;
		}
		goto ok;
	}
	if (child) {
		ss128_thischild(node->parent, node) = child;
		child->parent = node->parent;
		if (node->red) panic(base, "tankefel\n");
		if (!child->red) panic(base, "tankefel\n");
		child->red = 0;
		goto ok;
	} else {
		if (node->red) {
			ss128_thischild(node->parent, node) = NULL;
			goto ok;
		}
		/* There is no child, and the node to remove is black, so we have to rebalance */
		ss128_balance_before_delete(head, node);
		if (node->child[!node->child[0]]) panic(base, "tankefel\n");
		ss128_thischild(node->parent, node) = NULL;
	}
ok:
	ss128_node_free(head, node);
	return 0;
}

int ss128_find(ss128_head_t *head, ss128_value_t *r_value, ss128_key_t key) {
	ss128_node_t *node;

	node = head->root;
	while (node) {
		if (rbtree_key_eq(node->key, key)) {
			if (r_value) *r_value = node->value;
			return 0;
		}
		node = node->child[rbtree_key_lt(node->key, key)];
	}
	return 1;
}

static void ss128_free_i(ss128_head_t *head, ss128_node_t *node) {
	if (!node) return;
	ss128_free_i(head, node->child[0]);
	ss128_free_i(head, node->child[1]);
	ss128_freemem(node, sizeof(*node));
}

void ss128_free(ss128_head_t *head) {
	ss128_node_t *node;
	size_t       z;

	if (head->allocation_policy == RBTREE_ALLOCATION_POLICY_NORMAL) {
		ss128_free_i(head, head->root);
		node = head->freelist;
		z = sizeof(*node);
	} else {
		node = head->chunklist;
		z = sizeof(*node) * head->allocation_value;
	}
	while (node) {
		ss128_node_t *next = node->child[0];
		ss128_freemem(node, z);
		node = next;
	}
}

static int ss128_count_i(ss128_node_t *node) {
	if (!node) return 0;
	return ss128_count_i(node->child[0]) + ss128_count_i(node->child[1]) + 1;
}

int ss128_count(ss128_head_t *head) {
	return ss128_count_i(head->root);
}
