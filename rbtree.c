/* Oförändrad från den i efs förutom det här uppe. */

#include "db.h"

#define efs_rbtree_count(bs, a)        rbtree_count(a)
#define efs_rbtree_delete(bs, a, b)    rbtree_delete(a, b)
#define efs_rbtree_find(bs, a, b, c)   rbtree_find(a, b, c)
#define efs_rbtree_free(bs, a)         rbtree_free(a)
#define efs_rbtree_init(bs, a, b, c)   rbtree_init(a, b, c)
#define efs_rbtree_insert(bs, a, b, c) rbtree_insert(a, b, c)

#define efs_rbtree_node_alloc(bs, a, b, c, d)      rbtree_node_alloc(a, b, c, d)
#define efs_rbtree_alloc_chunk(bs, a)              rbtree_alloc_chunk(a)
#define efs_rbtree_balance_after_insert(bs, a, b)  rbtree_balance_after_insert(a, b)
#define efs_rbtree_rotate(bs, a, b, c)             rbtree_rotate(a, b, c)
#define efs_rbtree_node_free(bs, a, b)             rbtree_node_free(a, b)
#define efs_rbtree_balance_before_delete(bs, a, b) rbtree_balance_before_delete(a, b)
#define efs_rbtree_free_i(bs, a)                   rbtree_free_i(a)

#define efs_rbtree_key_t   rbtree_key_t
#define efs_rbtree_value_t rbtree_value_t

#define efs_rbtree_allocation_policy_t        rbtree_allocation_policy_t
#define EFS_RBTREE_ALLOCATION_POLICY_NORMAL   RBTREE_ALLOCATION_POLICY_NORMAL
#define EFS_RBTREE_ALLOCATION_POLICY_PREALLOC RBTREE_ALLOCATION_POLICY_PREALLOC
#define EFS_RBTREE_ALLOCATION_POLICY_CHUNKED  RBTREE_ALLOCATION_POLICY_CHUNKED

#define efs_rbtree_node_t rbtree_node_t
#define efs_rbtree_head_t rbtree_head_t

#define panic(bs, a) do_panic(a)
static void do_panic(const char *msg) {
	fprintf(stderr, "rbtree.c: %s", msg);
	exit(1);
}

#define efs_allocmem(bs, a, b, c) allocmem(a, b)
static int allocmem(void *res, int z) {
	void *ptr;
	efs_u8_t **res8 = res;

	ptr = mm_alloc(z);
	*res8 = ptr;
	return !ptr;
}

#define efs_freemem(dummy1, ptr, dummy2) mm_free(ptr)

/* --###-- slut på det ändrade --###-- */

#define efs_rbtree_thischild(p, c) ((p)->child[(p)->child[0] != (c)])
#define efs_rbtree_otherchild(p, c) ((p)->child[(p)->child[0] == (c)])
#define efs_rbtree_isred(n) ((n) && (n)->red)

static void efs_rbtree_alloc_chunk(efs_base_t *base, efs_rbtree_head_t *head) {
	void              **chunk;
	efs_rbtree_node_t *node;
	int               i;

	if (efs_allocmem(base, &chunk, sizeof(*node) * head->allocation_value + sizeof(void *), 0)) return;
	*chunk = head->chunklist;
	head->chunklist = chunk;
	node = head->freelist = (efs_rbtree_node_t *)(chunk + 1);
	for (i = 0; i < head->allocation_value - 1; i++) {
		node->child[0] = node + 1;
		node += 1;
	}
	node->child[0] = NULL;
	
}

static int efs_rbtree_node_alloc(efs_base_t *base, efs_rbtree_head_t *head, efs_rbtree_node_t **r_node, efs_rbtree_value_t value, efs_rbtree_key_t key) {

	switch (head->allocation_policy) {
		case EFS_RBTREE_ALLOCATION_POLICY_NORMAL:
			efs_allocmem(base, r_node, sizeof(efs_rbtree_node_t), 0);
			break;
		case EFS_RBTREE_ALLOCATION_POLICY_CHUNKED:
			if (!head->freelist) efs_rbtree_alloc_chunk(base, head);
		case EFS_RBTREE_ALLOCATION_POLICY_PREALLOC:
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

static void efs_rbtree_node_free(efs_base_t *base, efs_rbtree_head_t *head, efs_rbtree_node_t *node) {
	switch (head->allocation_policy) {
		case EFS_RBTREE_ALLOCATION_POLICY_NORMAL:
			efs_freemem(base, node, sizeof(*node));
			break;
		case EFS_RBTREE_ALLOCATION_POLICY_PREALLOC:
		case EFS_RBTREE_ALLOCATION_POLICY_CHUNKED:
			node->child[0] = head->freelist;
			head->freelist = node;
			break;
	}
}

static void efs_rbtree_rotate(efs_base_t *base, efs_rbtree_head_t *head, efs_rbtree_node_t *node, int isright) {
	efs_rbtree_node_t *tmp;

	tmp = node->child[!isright];
	node->child[!isright] = tmp->child[isright];
	tmp->child[isright] = node;
	if (node->parent) {
		efs_rbtree_thischild(node->parent, node) = tmp;
	} else {
		head->root = tmp;
	}
	tmp->parent = node->parent;
	node->parent = tmp;
	if (node->child[!isright]) node->child[!isright]->parent = node;
}

static void efs_rbtree_balance_after_insert(efs_base_t *base, efs_rbtree_head_t *head, efs_rbtree_node_t *node) {
	efs_rbtree_node_t *uncle;
	int pleft, gpleft;

	if (!node->parent) {
		node->red = 0;
		return;
	}
	if (!node->parent->red) return;
	uncle = efs_rbtree_otherchild(node->parent->parent, node->parent);
	if (efs_rbtree_isred(uncle)) {
		node->parent->red   = 0;
		uncle->red          = 0;
		uncle->parent->red  = 1;
		efs_rbtree_balance_after_insert(base, head, uncle->parent);
		return;
	}
	pleft  = (node->parent->child[0] == node);
	gpleft = (node->parent->parent->child[0] == node->parent);
	if (pleft && !gpleft) {
		efs_rbtree_rotate(base, head, node->parent, 1);
		node = node->child[1]; /* Former parent */
	} else if (!pleft && gpleft) {
		efs_rbtree_rotate(base, head, node->parent, 0);
		node = node->child[0]; /* Former parent */
	}
	node->parent->red         = 0;
	node->parent->parent->red = 1;
	pleft  = (node->parent->child[0] == node);
	gpleft = (node->parent->parent->child[0] == node->parent);
	if (pleft && gpleft) {
		efs_rbtree_rotate(base, head, node->parent->parent, 1);
	} else {
		if (!(!pleft && !gpleft)) panic(base, "tankefel\n");
		efs_rbtree_rotate(base, head, node->parent->parent, 0);
	}
}

int efs_rbtree_insert(efs_base_t *base, efs_rbtree_head_t *head, efs_rbtree_value_t value, efs_rbtree_key_t key) {
	efs_rbtree_node_t *node;
	efs_rbtree_node_t *newnode;
	int               child;

	err1(efs_rbtree_node_alloc(base, head, &newnode, value, key));
	node = head->root;
	if (!node) {
		newnode->parent = NULL;
		newnode->red    = 0;
		head->root = newnode;
		return 0;
	}
	while (42) {
		err1(node->key == key);
		child = (node->key < key);
		if (node->child[child]) {
			node = node->child[child];
		} else {
			node->child[child] = newnode;
			break;
		}
	}
	newnode->parent = node;
	efs_rbtree_balance_after_insert(base, head, newnode);
	return 0;
err:
	if (newnode) efs_rbtree_node_free(base, head, newnode);
	return 1;
}

static void efs_rbtree_balance_before_delete(efs_base_t *base, efs_rbtree_head_t *head, efs_rbtree_node_t *node) {
	efs_rbtree_node_t *sibling;
	int child;

	child = (node->parent->child[1] == node);
	sibling = node->parent->child[!child];
	if (sibling->red) {
		node->parent->red = 1;
		sibling->red      = 0;
		efs_rbtree_rotate(base, head, node->parent, child);
		sibling = node->parent->child[!child];
	} else if (!node->parent->red && !efs_rbtree_isred(sibling->child[0]) && !efs_rbtree_isred(sibling->child[1])) {
		sibling->red = 1;
		if (!node->parent->parent) return;
		efs_rbtree_balance_before_delete(base, head, node->parent);
		return;
	}
	if (node->parent->red && !sibling->red && !efs_rbtree_isred(sibling->child[0]) && !efs_rbtree_isred(sibling->child[1])) {
		node->parent->red = 0;
		sibling->red      = 1;
		return;
	}
	if (!sibling->red && efs_rbtree_isred(sibling->child[child]) && !efs_rbtree_isred(sibling->child[!child])) {
		sibling->red               = 1;
		sibling->child[child]->red = 0;
		efs_rbtree_rotate(base, head, sibling, !child);
		sibling = node->parent->child[!child];
	}
	if (!sibling->red && efs_rbtree_isred(sibling->child[!child])) {
		int red = node->parent->red;
		node->parent->red           = sibling->red;
		sibling->red                = red;
		sibling->child[!child]->red = 0;
		efs_rbtree_rotate(base, head, node->parent, child);
	}
}

int efs_rbtree_delete(efs_base_t *base, efs_rbtree_head_t *head, efs_rbtree_key_t key) {
	efs_rbtree_node_t *node;
	efs_rbtree_node_t *child;

	node = head->root;
	while (42) {
		if (node->key == key) break;
		node = node->child[key > node->key];
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
		efs_rbtree_thischild(node->parent, node) = child;
		child->parent = node->parent;
		if (node->red) panic(base, "tankefel\n");
		if (!child->red) panic(base, "tankefel\n");
		child->red = 0;
		goto ok;
	} else {
		if (node->red) {
			efs_rbtree_thischild(node->parent, node) = NULL;
			goto ok;
		}
		/* There is no child, and the node to remove is black, so we have to rebalance */
		efs_rbtree_balance_before_delete(base, head, node);
		if (node->child[!node->child[0]]) panic(base, "tankefel\n");
		efs_rbtree_thischild(node->parent, node) = NULL;
	}
ok:
	efs_rbtree_node_free(base, head, node);
	return 0;
}

int efs_rbtree_find(efs_base_t *base, efs_rbtree_head_t *head, efs_rbtree_value_t *r_value, efs_rbtree_key_t key) {
	efs_rbtree_node_t *node;

	node = head->root;
	while (node) {
		if (node->key == key) {
			if (r_value) *r_value = node->value;
			return 0;
		}
		node = node->child[key > node->key];
	}
	return 1;
}

int efs_rbtree_init(efs_base_t *base, efs_rbtree_head_t *head, efs_rbtree_allocation_policy_t allocation_policy, int allocation_value) {
	head->root              = NULL;
	head->freelist          = NULL;
	head->chunklist         = NULL;
	head->allocation_policy = allocation_policy;
	head->allocation_value  = allocation_value;
	if (allocation_policy == EFS_RBTREE_ALLOCATION_POLICY_PREALLOC) {
		efs_rbtree_alloc_chunk(base, head);
		if (!head->freelist) return 1;
	}
	return 0;
}

static void efs_rbtree_free_i(efs_base_t *base, efs_rbtree_node_t *node) {
	if (!node) return;
	efs_rbtree_free_i(base, node->child[0]);
	efs_rbtree_free_i(base, node->child[1]);
	efs_freemem(base, node, sizeof(*node));
}

void efs_rbtree_free(efs_base_t *base, efs_rbtree_head_t *head) {
	if (head->allocation_policy == EFS_RBTREE_ALLOCATION_POLICY_NORMAL) {
		efs_rbtree_node_t *node;
		efs_rbtree_node_t *next;

		node = head->freelist;
		while (node) {
			next = node->child[0];
			efs_freemem(base, node, sizeof(*node));
			node = next;
		}
		efs_rbtree_free_i(base, head->root);
	} else {
		void **chunk;

		while (head->chunklist) {
			chunk = head->chunklist;
			head->chunklist = *chunk;
			efs_freemem(base, chunk, sizeof(efs_rbtree_node_t) * head->allocation_value + sizeof(void *));
		}
	}
}

static int efs_rbtree_count_i(efs_rbtree_node_t *node) {
	if (!node) return 0;
	return efs_rbtree_count_i(node->child[0]) + efs_rbtree_count_i(node->child[1]) + 1;
}

int efs_rbtree_count(efs_base_t *base, efs_rbtree_head_t *head) {
	return efs_rbtree_count_i(head->root);
}
