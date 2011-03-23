#include "db.h"

void list_newlist(list_head_t *list)
{
	list->head = (list_node_t *)&list->tail;
	list->tail = NULL;
	list->tailpred = (list_node_t *)list;
}

void list_addhead(list_head_t *list, list_node_t *node)
{
	node->succ = list->head;
	node->pred = (list_node_t *)&list->head;
	list->head->pred = node;
	list->head = node;
}

void list_addtail(list_head_t *list, list_node_t *node)
{
	node->succ = (list_node_t *)&list->tail;
	node->pred = list->tailpred;
	list->tailpred->succ = node;
	list->tailpred = node;
}

list_node_t *list_remhead(list_head_t *list)
{
	list_node_t *node = list->head;
	if (!node->succ) return NULL;
	list_remove(node);
	return node;
}

void list_remove(list_node_t *node)
{
	node->pred->succ = node->succ;
	node->succ->pred = node->pred;
}

void list_iterate(list_head_t *list, void *data, list_callback_t callback)
{
	list_node_t *node = list->head;
	while (node->succ) {
		// Callback is allowed to remove node from list (but not
		// otherwise modify list), so we must save succ.
		list_node_t *succ = node->succ;
		callback(node, data);
		node = succ;
	}
}
