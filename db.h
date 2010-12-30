#define _XOPEN_SOURCE
#define _GNU_SOURCE

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>
#include <time.h>

#define err1(v) if(v) goto err;
#define err(v, res) if(v) { r = (res); goto err; }
#define assert(v) if (!(v)) assert_fail(#v, __FILE__, __FUNCTION__, __LINE__)
#define NORETURN __attribute__((noreturn))
#define ULL (unsigned long long)

#define arraylen(a) ((int)(sizeof(a)/sizeof(*(a))))

void NORETURN assert_fail(const char *ass, const char *file, const char *func, int line);

typedef void     efs_base_t;
typedef int8_t   efs_8_t;
typedef int16_t  efs_16_t;
typedef int32_t  efs_32_t;
typedef int64_t  efs_64_t;
typedef uint8_t  efs_u8_t;
typedef uint16_t efs_u16_t;
typedef uint32_t efs_u32_t;
typedef uint64_t efs_u64_t;

typedef struct ss128_key {
	uint64_t a;
	uint64_t b;
} ss128_key_t;
typedef void * ss128_value_t;

struct ss128_node;
typedef struct ss128_node ss128_node_t;

typedef struct ss128_head {
	ss128_node_t *root;
	ss128_node_t *freelist;
	void         *chunklist;
	int          allocation_policy;
	int          allocation_value;
} ss128_head_t;

typedef struct list_node {
	struct list_node *succ;
	struct list_node *pred;
	unsigned int     size;
} list_node_t;

typedef struct list_head {
	list_node_t *head;
	list_node_t *tail;
	list_node_t *tailpred;
} list_head_t;

/* Keep enum and #define synced */
/* OR:able flags */
#define CAP_NAMES_STR "post delete mkuser tag untag modcap mktag super"
typedef enum {
	CAP_NONE   = 0,
	CAP_POST   = 1,   // Can post new images
	CAP_DELETE = 2,   // Can delete posts
	CAP_MKUSER = 4,   // Can create new users
	CAP_TAG    = 8,   // Can tag posts
	CAP_UNTAG  = 16,  // Can remove tags from posts
	CAP_MODCAP = 32,  // Can modify capabilities
	CAP_MKTAG  = 64,  // Can create new tags
	CAP_SUPER  = 128, // Can modify things that are not supposed to be modified.
} capability_t;
#define CAP_MAX CAP_SUPER
#define DEFAULT_CAPS (CAP_POST | CAP_TAG | CAP_UNTAG | CAP_MKTAG)

typedef union md5 {
	uint8_t     m[16];
	ss128_key_t key;
} md5_t;

typedef union guid {
	uint8_t     data_u8[16];
	uint32_t    data_u32[4];
	ss128_key_t key;
} guid_t;

typedef enum {
	GUIDTYPE_SERVER,
	GUIDTYPE_TAG,
} guidtype_t;

struct tag;
typedef struct post_taglist {
	struct tag          *tags[14];
	struct post_taglist *next;
} post_taglist_t;

typedef struct impllist {
	struct tag      *tags[14];
	int32_t         priority[14];
	struct impllist *next;
} impllist_t;

struct postlist_node;
typedef struct postlist {
	struct postlist_node *head;
	uint32_t count;
	uint32_t holes;
} postlist_t;

typedef struct post {
	md5_t          md5;
	const char     *source;
	const char     *title;
	time_t         modified;
	time_t         created;
	time_t         image_date;
	uint16_t       image_date_fuzz;
	int16_t        score;
	uint16_t       width;
	uint16_t       height;
	uint16_t       filetype;
	uint16_t       rating;
	uint16_t       of_holes;
	uint16_t       of_weak_holes;
	uint32_t       of_tags;
	uint32_t       of_weak_tags;
	postlist_t     related_posts;
	post_taglist_t tags;
	post_taglist_t *weak_tags;
	post_taglist_t *implied_tags;
	post_taglist_t *implied_weak_tags;
} post_t;

/* Keep this synced with function-arrays in protocol.c:put_in_post_field() *
 * and log.c:log_write_post()                                              */
typedef enum {
	FIELDTYPE_UNSIGNED,
	FIELDTYPE_SIGNED,
	FIELDTYPE_ENUM,
	FIELDTYPE_STRING,
} fieldtype_t;

typedef struct field {
	const char   *name;
	int          size;
	int          offset;
	fieldtype_t  type;
	capability_t modcap; // Capability needed to modify field
	const char * const **array;
} field_t;

extern const field_t post_fields[];

typedef struct postlist_node {
	post_t *posts[30];
	struct postlist_node *next;
} postlist_node_t;

typedef struct tag {
	const char *name;
	const char *fuzzy_name;
	guid_t     guid;
	uint16_t   type;
	postlist_t posts;
	postlist_t weak_posts;
	impllist_t *implications;
} tag_t;

typedef struct tagalias {
	const char *name;
	const char *fuzzy_name;
	tag_t      *tag;
} tagalias_t;

typedef uint32_t tag_id_t;

/* Keep synced to errors[] in connection.c */
typedef enum {
	E_LINETOOLONG,
	E_READ,
	E_COMMAND,
	E_SYNTAX,
	E_OVERFLOW,
	E_MEM,
	E_UTF8,
} dberror_t;

/* Keep enum and #define synced */
#define FILETYPE_NAMES_STR "jpeg gif png bmp swf"
typedef enum {
	FILETYPE_JPEG,
	FILETYPE_GIF,
	FILETYPE_PNG,
	FILETYPE_BMP,
	FILETYPE_FLASH,
} filetype_t;

typedef enum {
	T_NO,
	T_YES,
	T_DONTCARE
} truth_t;

/* OR:able flags */
typedef enum {
	CMDFLAG_NONE   = 0,
	CMDFLAG_LAST   = 1,
	CMDFLAG_MODIFY = 2,
} prot_cmd_flag_t;

typedef struct user {
	const char   *name;
	const char   *password;
	capability_t caps;
} user_t;

typedef uint64_t trans_id_t;

typedef enum {
	TRANSFLAG_SYNC = 1,
} transflag_t;

struct connection;
typedef struct connection connection_t;

typedef struct trans {
	off_t        mark_offset;
	trans_id_t   id;
	unsigned int init_len;
	unsigned int buf_used;
	int          fd;
	transflag_t  flags;
	const user_t *user;
	connection_t *conn;
	time_t       now;
	char         buf[4000];
} trans_t;

#define PROT_MAXLEN 4096

typedef int (*prot_err_func_t)(connection_t *conn, const char *msg);
typedef int (*prot_cmd_func_t)(connection_t *conn, const char *cmd,
                               void *data, prot_cmd_flag_t flags);

typedef enum {
	CONNFLAG_GOING = 1, // Connection is still in use
	CONNFLAG_LOG   = 2, // This is the log-reader.
} connflag_t;

struct connection {
	const user_t    *user;
	prot_err_func_t error;
	trans_t         trans;
	int             sock;
	connflag_t      flags;
	list_head_t     mem_list;
	unsigned int    mem_used;
	unsigned int    getlen;
	unsigned int    getpos;
	unsigned int    outlen;
	unsigned int    linelen;
	char            getbuf[256];
	char            linebuf[PROT_MAXLEN];
	char            outbuf[PROT_MAXLEN];
};

typedef struct result {
	post_t **posts;
	uint32_t of_posts;
	uint32_t room;
} result_t;

void result_free(connection_t *conn, result_t *result);
int result_add_post(connection_t *conn, result_t *result, post_t *post);
int result_remove_tag(connection_t *conn, result_t *result,
                      tag_t *tag, truth_t weak);
int result_intersect(connection_t *conn, result_t *result,
                     tag_t *tag, truth_t weak);

int c_init(connection_t **res_conn, int sock, user_t *user,
           prot_err_func_t error);
int c_alloc(connection_t *conn, void **res, unsigned int size);
void *c_realloc(connection_t *conn, void *ptr, unsigned int old_size,
                unsigned int new_size, int *res);
void c_free(connection_t *conn, void *mem, unsigned int size);
void c_cleanup(connection_t *conn);
void c_printf(connection_t *conn, const char *fmt, ...);
void c_flush(connection_t *conn);
void c_read_data(connection_t *conn);
int c_get_line(connection_t *conn);
int c_error(connection_t *conn, const char *what);
int c_close_error(connection_t *conn, dberror_t what);

/* Note that these modify *cmd. */
int prot_cmd_loop(connection_t *conn, char *cmd, void *data,
                  prot_cmd_func_t func, prot_cmd_flag_t flags);
int prot_tag_post(connection_t *conn, char *cmd);
int prot_add(connection_t *conn, char *cmd);
int prot_modify(connection_t *conn, char *cmd);
int prot_rel_add(connection_t *conn, char *cmd);
int prot_rel_remove(connection_t *conn, char *cmd);
int prot_implication(connection_t *conn, char *cmd);
user_t *prot_auth(char *cmd);

tag_t *tag_find_name(const char *name, truth_t alias);
tag_t *tag_find_guid(const guid_t guid);
tag_t *tag_find_guidstr(const char *guidstr);
int tag_add_implication(tag_t *from, tag_t *to, int32_t priority);
int tag_rem_implication(tag_t *from, tag_t *to, int32_t priority);
int taglist_contains(const post_taglist_t *tl, const tag_t *tag);
int post_tag_rem(post_t *post, tag_t *tag);
int post_tag_add(post_t *post, tag_t *tag, truth_t weak);
int post_has_tag(const post_t *post, const tag_t *tag, truth_t weak);
int post_find_md5str(post_t **res_post, const char *md5str);
int post_has_rel(const post_t *post, const post_t *rel);
int post_rel_add(post_t *a, post_t *b);
int post_rel_remove(post_t *a, post_t *b);
const char *md5_md52str(const md5_t md5);
int md5_str2md5(md5_t *res_md5, const char *md5str);
int populate_from_log(const char *filename, void (*callback)(const char *line));
void db_serve(void);
void db_read_cfg(const char *filename);
int str2id(const char *str, const char * const *ids);
typedef void (*postlist_callback_t)(void *data, post_t *post);
void postlist_iterate(postlist_t *pl, void *data,
                      postlist_callback_t callback);

typedef void (*ss128_callback_t)(ss128_key_t key, ss128_value_t value,
              void *data);
void ss128_iterate(ss128_head_t *head, ss128_callback_t callback, void *data);
int ss128_insert(ss128_head_t *head, ss128_value_t value, ss128_key_t key);
int ss128_delete(ss128_head_t *head, ss128_key_t key);
int ss128_find(ss128_head_t *head, ss128_value_t *r_value, ss128_key_t key);
int ss128_init(ss128_head_t *head);
void ss128_free(ss128_head_t *head);
int ss128_count(ss128_head_t *head);
ss128_key_t ss128_str2key(const char *str);

int  mm_init(void);
void mm_cleanup(void);
void *mm_alloc(unsigned int size);
void *mm_alloc_s(unsigned int size);
void mm_free(void *mem);
const char *mm_strdup(const char *str);
void mm_print(void);
void mm_lock(void);
void mm_unlock(void);

void client_handle(connection_t *conn, char *buf);

void log_trans_start(connection_t *conn, time_t now);
void log_trans_end(connection_t *conn);
void log_set_init(trans_t *trans, const char *fmt, ...);
void log_clear_init(trans_t *trans);
void log_write(trans_t *trans, const char *fmt, ...);
void log_init(void);
void log_cleanup(void);
void log_write_tag(trans_t *trans, const tag_t *tag);
void log_write_tagalias(trans_t *trans, const tagalias_t *tagalias);
void log_write_post(trans_t *trans, const post_t *post);
void log_write_user(trans_t *trans, const user_t *user);
void log_dump(void);

guid_t guid_gen_tag_guid(void);
void guid_update_last(guid_t guid);
const char *guid_guid2str(guid_t guid);
int guid_str2guid(guid_t *res_guid, const char *str, guidtype_t type);
int guid_is_valid_server_guid(const guid_t guid);
int guid_is_valid_tag_guid(const guid_t guid, int must_be_local);

const char *str_str2enc(const char *str);
const char *str_enc2str(const char *enc);

int utf_fuzz_c(connection_t *conn, const char *str, char **res,
               unsigned int *res_len);
const char *utf_fuzz_mm(const char *str);

extern ss128_head_t *posts;
extern ss128_head_t *tags;
extern ss128_head_t *tagaliases;
extern ss128_head_t *tagguids;
extern ss128_head_t *users;

extern uint64_t *logindex;
extern uint64_t *logdumpindex;

extern const char * const *filetype_names;
extern const char * const *rating_names;
extern const char * const *tagtype_names;
extern const char * const *cap_names;
extern md5_t config_md5;

extern const char *basedir;

extern connection_t *logconn;

extern int server_running;

#ifndef O_EXLOCK
#define O_EXLOCK 0
#endif
