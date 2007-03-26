#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>

#define err1(v) if(v) goto err;
#define err(v, res) if(v) { r = (res); goto err; }
#define assert(v) if (!(v)) assert_fail(#v, __FILE__, __FUNCTION__, __LINE__)

void assert_fail(const char *ass, const char *file, const char *func, int line);

typedef void     efs_base_t;
typedef int8_t   efs_8_t;
typedef int16_t  efs_16_t;
typedef int32_t  efs_32_t;
typedef int64_t  efs_64_t;
typedef uint8_t  efs_u8_t;
typedef uint16_t efs_u16_t;
typedef uint32_t efs_u32_t;
typedef uint64_t efs_u64_t;

typedef efs_u64_t rbtree_key_t;
typedef void *    rbtree_value_t;

typedef enum {
	RBTREE_ALLOCATION_POLICY_NORMAL,
	RBTREE_ALLOCATION_POLICY_PREALLOC,
	RBTREE_ALLOCATION_POLICY_CHUNKED
} rbtree_allocation_policy_t;

typedef struct rbtree_node {
	struct rbtree_node *child[2];
	struct rbtree_node *parent;
	rbtree_key_t       key;
	rbtree_value_t     value;
	unsigned int       red : 1;
} rbtree_node_t;

typedef struct rbtree_head {
	rbtree_node_t              *root;
	rbtree_node_t              *freelist;
	void                       *chunklist;
	rbtree_allocation_policy_t allocation_policy;
	int                        allocation_value;
} rbtree_head_t;

int rbtree_insert(rbtree_head_t *head, rbtree_value_t value, rbtree_key_t key);
int rbtree_delete(rbtree_head_t *head, rbtree_key_t key);
int rbtree_find(rbtree_head_t *head, rbtree_value_t *r_value, rbtree_key_t key);
int rbtree_init(rbtree_head_t *head, rbtree_allocation_policy_t allocation_policy, int allocation_value);
void rbtree_free(rbtree_head_t *head);
int rbtree_count(rbtree_head_t *head);

int  mm_init(const char *filename, rbtree_head_t **posttree, rbtree_head_t **tagtree, int use_existing);
void *mm_alloc(unsigned int size);
void mm_free(void *mem);
char *mm_strdup(const char *str);
void mm_print(void);

void client_handle(int s);
