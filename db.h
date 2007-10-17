#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <fcntl.h>

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

typedef union md5 {
	uint8_t      m[16];
	rbtree_key_t key;
} md5_t;

typedef struct guid {
	unsigned char check[4];
	union {
		unsigned char data[12];
		uint32_t      data_u32[3];
	};
} guid_t;

#define POST_TAGLIST_PER_NODE 14
struct tag;
typedef struct post_taglist {
	struct tag          *tags[POST_TAGLIST_PER_NODE];
	struct post_taglist *next;
} post_taglist_t;

typedef enum {
	RATING_UNSPECIFIED,
	RATING_SAFE,
	RATING_QUESTIONABLE,
	RATING_EXPLICIT,
} rating_t;

typedef struct post {
	md5_t          md5;
	char           *source;
	time_t         created;
	uint16_t       uid; // Danbooru post-ID. Probably shouldn't be here.
	int16_t        score;
	uint16_t       width;
	uint16_t       height;
	uint16_t       filetype;
	uint16_t       rating;
	uint16_t       of_tags;
	uint16_t       of_holes;
	post_taglist_t tags;
} post_t;

#define TAG_POSTLIST_PER_NODE 30
typedef struct tag_postlist {
	post_t *posts[TAG_POSTLIST_PER_NODE];
	struct tag_postlist *next;
} tag_postlist_t;

typedef enum {
	TAGTYPE_UNSPECIFIED, // Not specified (not known)
	TAGTYPE_INIMAGE,     // Something visible in the image ("thighhighs", ...)
	TAGTYPE_ARTIST,      // Someone who worked on the image.
	TAGTYPE_CHARACTER,   // A person in the image ("fate_testarossa", ...)
	TAGTYPE_COPYRIGTH,   // Where it's from. Name of anime, book, ...
	TAGTYPE_META,        // Things like "tagme", "what", ...
	TAGTYPE_AMBIGUOUS,   // A tag that needs to split in several tags.
} tagtype_t;

typedef struct tag {
	const char     *name;
	guid_t         guid;
	uint16_t       type;
	uint16_t       of_holes;
	uint32_t       of_posts;
	tag_postlist_t posts;
} tag_t;

typedef struct tagalias {
	const char *name;
	tag_t      *tag;
} tagalias_t;

typedef uint32_t tag_id_t;

/* Keep synced to extensions[] in client.c */
typedef enum {
	FILETYPE_JPEG,
	FILETYPE_GIF,
	FILETYPE_PNG,
	FILETYPE_BMP,
	FILETYPE_FLASH,
} filetype_t;

tag_t *tag_find_name(const char *name);
tag_t *tag_find_guid(const guid_t guid);
int post_has_tag(post_t *post, tag_t *tag);
const char *md5_md52str(md5_t md5);

typedef void (*rbtree_callback_t)(rbtree_key_t key, rbtree_value_t value);
void rbtree_iterate(rbtree_head_t *head, rbtree_callback_t callback);
int rbtree_insert(rbtree_head_t *head, rbtree_value_t value, rbtree_key_t key);
int rbtree_delete(rbtree_head_t *head, rbtree_key_t key);
int rbtree_find(rbtree_head_t *head, rbtree_value_t *r_value, rbtree_key_t key);
int rbtree_init(rbtree_head_t *head, rbtree_allocation_policy_t allocation_policy, int allocation_value);
void rbtree_free(rbtree_head_t *head);
int rbtree_count(rbtree_head_t *head);

int  mm_init(const char *filename, int use_existing);
void *mm_alloc(unsigned int size);
void mm_free(void *mem);
char *mm_strdup(const char *str);
void mm_print(void);
void mm_lock(void);
void mm_unlock(void);

void client_handle(int s);

int dump_log(const char *filename);

guid_t guid_gen_tag_guid(void);
const char *guid_guid2str(guid_t guid);
int guid_str2guid(guid_t *res_guid, const char *str);
int guid_is_valid_server_guid(const guid_t guid);
int guid_is_valid_tag_guid(const guid_t guid, int must_be_local);
