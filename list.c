#include "db.h"

#define listname(n) mem_ ## n
#include "list.h"

#undef listname
#define listname(n) post_ ## n
#define LIST_ALL
#include "list.h"
