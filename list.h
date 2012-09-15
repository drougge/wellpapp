void listname(newlist)(listname(list_t) *list)
{
	list->head = NULL;
	list->tail = NULL;
}

void listname(addtail)(listname(list_t) *list, listname(node_t) *node)
{
	node->succ = NULL;
	node->pred = list->tail;
	if (list->tail) {
		list->tail->succ = node;
	} else {
		list->head = node;
	}
	list->tail = node;
}

void listname(remove)(listname(list_t) *list, listname(node_t) *node)
{
	if (node->pred) {
		node->pred->succ = node->succ;
	} else {
		list->head = node->succ;
		if (list->head) list->head->pred = NULL;
	}
	if (node->succ) {
		node->succ->pred = node->pred;
	} else {
		list->tail = node->pred;
		if (list->tail) list->tail->succ = NULL;
	}
}

#ifdef LIST_ALL
void listname(addhead)(listname(list_t) *list, listname(node_t) *node)
{
	node->succ = list->head;
	node->pred = NULL;
	if (list->head) {
		list->head->pred = node;
	} else {
		list->tail = node;
	}
	list->head = node;
}

listname(node_t) *listname(remhead)(listname(list_t) *list)
{
	listname(node_t) *node = list->head;
	if (!node) return NULL;
	list->head = node->succ;
	if (list->head) {
		list->head->pred = NULL;
	} else {
		list->tail = NULL;
	}
	return node;
}

void listname(iterate)(listname(list_t) *list, void *data, listname(callback_t) callback)
{
	listname(node_t) *node = list->head;
	while (node) {
		// Callback is allowed to remove node from list (but not
		// otherwise modify list), so we must save succ.
		listname(node_t) *succ = node->succ;
		callback(node, data);
		node = succ;
	}
}
#endif
