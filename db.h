#ifdef __svr4__
#define _XOPEN_SOURCE 600
#endif

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/uio.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <limits.h>
#include <time.h>
#include <regex.h>

#ifdef __GNUC__
#  define _ALIGN(d) d __attribute__((aligned(__BIGGEST_ALIGNMENT__)))
#  define MM_ALIGN __BIGGEST_ALIGNMENT__
#else
#  error You need to specify an _ALIGN definition for your compiler. (Maybe nothing)
#  error And MM_ALIGN too. (Try 4.)
#endif

#define err1(v) if(v) goto err;
#define err(v, res) if(v) { r = (res); goto err; }
#define assert(v) if (!(v)) assert_fail(#v, __FILE__, __FUNCTION__, __LINE__)
#define NORETURN __attribute__((noreturn))
#define ULL (unsigned long long)
#define LL  (long long)

#define arraylen(a) ((int)(sizeof(a)/sizeof(*(a))))

void NORETURN assert_fail(const char *ass, const char *file, const char *func, int line);

typedef _ALIGN(struct ss128_key {
	uint64_t a;
	uint64_t b;
}) ss128_key_t;
typedef void * ss128_value_t;

struct ss128_node;
typedef struct ss128_node ss128_node_t;

typedef int(*ss128_allocmem_t)(void *, void *, unsigned int);
typedef void(*ss128_freemem_t)(void *, void *, unsigned int);

typedef _ALIGN(struct ss128_head {
	ss128_node_t *root;
	ss128_node_t *freelist;
	ss128_node_t *chunklist;
	int          allocation_policy;
	int          allocation_value;
	ss128_allocmem_t allocmem;
	ss128_freemem_t  freemem;
	void             *memarg;
}) ss128_head_t;

typedef struct list_node {
	struct list_node *succ;
	struct list_node *pred;
} list_node_t;

typedef struct list_head {
	list_node_t *head;
	list_node_t *tail;
	list_node_t *tailpred;
} list_head_t;

typedef union md5 {
	uint8_t     m[16];
	ss128_key_t key;
} md5_t;

typedef union guid {
	uint8_t     data_u8[16];
	uint32_t    data_u32[4];
	ss128_key_t key;
} guid_t;

typedef _ALIGN(struct hash {
	const char **data;
	unsigned long used;
	int size;
}) hash_t;

typedef enum {
	GUIDTYPE_SERVER,
	GUIDTYPE_TAG,
} guidtype_t;

struct tag;
typedef struct tag tag_t;

typedef struct datetime_fuzz {
	uint8_t  d_step[4];
	uint32_t d_fuzz;
} datetime_fuzz_t;

// F-stops as used by cameras tend to be lies (e.g. "3.5" for both 2.8+1/2
// and 2.8+2/3). This is currently not handled, use +-.67 for +-.5 maybe?
// (And even more since they're rounded too.)
// ISO is the arithmetic scale, 100 is a typical slow film. No enforcing
// of defined speeds, fuzz is calculated a little bigger to compensate.
// @@ asa = int(10*log10(iso) + 1); iso = int(pow(10, (asa - 1) / 10))
// @@ might be helpful when calucating iso fuzz.
// v_str is not in val because doubles also store the exact value in v_str.
typedef _ALIGN(struct tag_value {
	const char *v_str;
	union {
		uint64_t   v_uint;
		int64_t    v_int;
		double     v_double;
	} val;
	union {
		uint64_t   f_uint;
		uint64_t   f_int;
		double     f_double;
		datetime_fuzz_t f_datetime;
	} fuzz;
}) tag_value_t;

typedef _ALIGN(struct post_taglist {
	tag_t               *tags[14];
	tag_value_t         *values[14];
	struct post_taglist *next;
}) post_taglist_t;

typedef _ALIGN(struct implication {
	tag_t   *tag;
	int32_t priority;
	int     positive;
}) implication_t;

typedef _ALIGN(struct impllist {
	implication_t   impl[7];
	struct impllist *next;
}) impllist_t;

struct postlist_node;
typedef struct postlist_node postlist_node_t;

typedef struct postlist_head {
	postlist_node_t *head;
	postlist_node_t *tail;
	postlist_node_t *tailpred;
} postlist_head_t;

typedef _ALIGN(struct postlist {
	union {
		list_head_t     l;
		postlist_head_t p;
	} h;
	uint32_t count;
}) postlist_t;

typedef _ALIGN(struct post {
	md5_t          md5;
	uint32_t       of_tags;
	uint32_t       of_weak_tags;
	postlist_t     related_posts;
	post_taglist_t tags;
	post_taglist_t *weak_tags;
	post_taglist_t *implied_tags;
	post_taglist_t *implied_weak_tags;
}) post_t;

typedef struct field {
	const char         *name;
	unsigned int       namelen;
	const char * const **valuelist;
	tag_t * const      *magic_tag;
	int                is_fuzz;
	int                log_version;
} field_t;

extern const field_t *post_fields;
extern const char * const tag_value_types[];

struct postlist_node_node {
	postlist_node_t *succ;
	postlist_node_t *pred;
};
struct _ALIGN(postlist_node {
	union {
		list_node_t l;
		struct postlist_node_node p;
	} n;
	post_t      *post;
});

// Needs to match tag_value_types in protocol.c,
// tv_printer in client.c, and tv_cmp in result.c.
// Needs to be handled in tag_value_parse in db.c.
typedef enum {
	VT_NONE,
	VT_WORD,
	VT_STRING,
	VT_INT,
	VT_UINT,
	VT_FLOAT,
	VT_F_STOP,
	VT_STOP,
	VT_DATETIME,
} valuetype_t;

typedef enum {
	CMP_NONE,
	CMP_EQ,
	CMP_GT,
	CMP_GE,
	CMP_LT,
	CMP_LE,
	CMP_REGEXP,
} tagvalue_cmp_t;

struct _ALIGN(tag {
	const char *name;
	const char *fuzzy_name;
	guid_t     guid;
	uint16_t   type;
	postlist_t posts;
	postlist_t weak_posts;
	impllist_t *implications;
	valuetype_t  valuetype;
	unsigned int ordered    : 1;
	unsigned int unsettable : 1;
	unsigned int datatag    : 1;
});

typedef _ALIGN(struct tagalias {
	const char *name;
	const char *fuzzy_name;
	tag_t      *tag;
}) tagalias_t;

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
#define FILETYPE_NAMES_STR "jpeg gif png bmp dng pef nef swf"
typedef enum {
	FILETYPE_JPEG,
	FILETYPE_GIF,
	FILETYPE_PNG,
	FILETYPE_BMP,
	FILETYPE_DNG,
	FILETYPE_PEF,
	FILETYPE_NEF,
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

typedef uint64_t trans_id_t;

typedef enum {
	TRANSFLAG_SYNC = 1,
	TRANSFLAG_GOING = 2,
	TRANSFLAG_OUTER = 4,
} transflag_t;

struct connection;
typedef struct connection connection_t;

#define PROT_MAXLEN 4096
#define LOG_VERSION 1

typedef struct trans {
	off_t        mark_offset;
	trans_id_t   id;
	unsigned int init_len;
	unsigned int buf_used;
	int          fd;
	transflag_t  flags;
	connection_t *conn;
	time_t       now;
	char         buf[PROT_MAXLEN + 256];
} trans_t;

typedef int (*prot_err_func_t)(connection_t *conn, const char *msg);
typedef int (*prot_cmd_func_t)(connection_t *conn, char *cmd, void *data,
                               prot_cmd_flag_t flags);

typedef enum {
	CONNFLAG_GOING = 1, // Connection is still in use
	CONNFLAG_LOG   = 2, // This is the log-reader.
} connflag_t;

typedef struct memlist_node {
	list_node_t  ln;
	unsigned int size;
} memlist_node_t;

struct connection {
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

typedef struct search_tag {
	tag_t          *tag;
	truth_t        weak;
	tagvalue_cmp_t cmp;
	tag_value_t    val;
} search_tag_t;

void result_free(connection_t *conn, result_t *result);
int result_add_post(connection_t *conn, result_t *result, post_t *post);
int result_remove_tag(connection_t *conn, result_t *result, search_tag_t *t);
int result_intersect(connection_t *conn, result_t *result, search_tag_t *t);

int c_init(connection_t **res_conn, int sock, prot_err_func_t error);
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
int prot_delete(connection_t *conn, char *cmd);
int prot_rel_add(connection_t *conn, char *cmd);
int prot_rel_remove(connection_t *conn, char *cmd);
int prot_implication(connection_t *conn, char *cmd);
int prot_order(connection_t *conn, char *cmd);
int prot_init(void);
void datetime_strfix(tag_value_t *val);

tag_t *tag_find_name(const char *name, truth_t alias, tagalias_t **r_tagalias);
tag_t *tag_find_guid(const guid_t guid);
tag_t *tag_find_guidstr(const char *guidstr);
tag_t *tag_find_guidstr_value(const char *guidstr, tagvalue_cmp_t *r_cmp,
                              tag_value_t *value, char *buf);
int tag_value_parse(tag_t *tag, const char *val, tag_value_t *tval, char *buf,
                    tagvalue_cmp_t cmp);
int tag_add_implication(tag_t *from, tag_t *to, int positive, int32_t priority);
int tag_rem_implication(tag_t *from, tag_t *to, int positive, int32_t priority);
int taglist_contains(const post_taglist_t *tl, const tag_t *tag);
int post_tag_rem(post_t *post, tag_t *tag);
int post_tag_add(post_t *post, tag_t *tag, truth_t weak, tag_value_t *tval);
int post_has_tag(const post_t *post, const tag_t *tag, truth_t weak);
tag_value_t *post_tag_value(const post_t *post, const tag_t *tag);
int post_find_md5str(post_t **res_post, const char *md5str);
void post_modify(post_t *post, time_t now);
int post_has_rel(const post_t *post, const post_t *rel);
int post_rel_add(post_t *a, post_t *b);
int post_rel_remove(post_t *a, post_t *b);
const char *md5_md52str(const md5_t md5);
int md5_str2md5(md5_t *res_md5, const char *md5str);
int populate_from_log(const char *filename, void (*callback)(const char *line));
void conn_cleanup(void);
void db_serve(void);
void db_read_cfg(const char *filename);
int str2id(const char *str, const char * const *ids);

typedef void (*ss128_callback_t)(ss128_key_t key, ss128_value_t value,
              void *data);
void ss128_iterate(ss128_head_t *head, ss128_callback_t callback, void *data);
int ss128_insert(ss128_head_t *head, ss128_value_t value, ss128_key_t key);
int ss128_delete(ss128_head_t *head, ss128_key_t key);
int ss128_find(ss128_head_t *head, ss128_value_t *r_value, ss128_key_t key);
int ss128_init(ss128_head_t *head, ss128_allocmem_t, ss128_freemem_t, void *memarg);
void ss128_free(ss128_head_t *head);
int ss128_count(ss128_head_t *head);
ss128_key_t ss128_str2key(const char *str);

void hash_init(hash_t *h);
const char *hash_find(hash_t *h, const char *key);
void hash_add(hash_t *h, const char *key);

int  mm_init(void);
void mm_cleanup(void);
void mm_last_log(off_t size, time_t mtime);
void *mm_alloc(unsigned int size);
void *mm_alloc_s(unsigned int size);
void mm_free(void *mem);
const char *mm_strdup(const char *str);
void *mm_dup(void *d, size_t z);
void mm_print(void);
void mm_start_walker(void);

void client_handle(connection_t *conn, char *buf);

void log_trans_start(connection_t *conn, time_t now);
int log_trans_start_outer(connection_t *conn, time_t now);
void log_trans_end(connection_t *conn);
int log_trans_end_outer(connection_t *conn);
void log_set_init(trans_t *trans, const char *fmt, ...);
void log_clear_init(trans_t *trans);
void log_write(trans_t *trans, const char *fmt, ...);
void log_init(void);
void log_cleanup(void);
void log_write_tag(trans_t *trans, const tag_t *tag, int is_add,
                   int write_flags, guid_t *merge);
void log_write_tagalias(trans_t *trans, const tagalias_t *tagalias);
void log_write_post(trans_t *trans, const post_t *post);
void log_dump(void);

guid_t guid_gen_tag_guid(void);
void guid_update_last(guid_t guid);
const char *guid_guid2str(guid_t guid);
int guid_str2guid(guid_t *res_guid, const char *str, guidtype_t type);
int guid_is_valid_server_guid(const guid_t guid);
int guid_is_valid_tag_guid(const guid_t guid, int must_be_local);

char *str_str2enc(const char *str);
const char *str_enc2str(const char *enc, char *buf);

int utf_fuzz_c(connection_t *conn, const char *str, char **res,
               unsigned int *res_len);
const char *utf_fuzz_mm(const char *str);
char *utf_compose(connection_t *conn, const char *str, int len);

typedef int (*sort_compar_t)(const void *a, const void *b, void *data);
void sort(void *base, int nmemb, size_t size, sort_compar_t comp, void *data);

void list_newlist(list_head_t *list);
void list_addhead(list_head_t *list, list_node_t *node);
void list_addtail(list_head_t *list, list_node_t *node);
void list_remove(list_node_t *node);
list_node_t *list_remhead(list_head_t *list);
typedef void (*list_callback_t)(list_node_t *node, void *data);
void list_iterate(list_head_t *list, void *data, list_callback_t callback);

void after_fixups(void);
void internal_fixups(void);

int tvc_none(tag_value_t *a, tagvalue_cmp_t cmp, tag_value_t *b, regex_t *re);
int tvc_string(tag_value_t *a, tagvalue_cmp_t cmp, tag_value_t *b, regex_t *re);
int tvc_int(tag_value_t *a, tagvalue_cmp_t cmp, tag_value_t *b, regex_t *re);
int tvc_uint(tag_value_t *a, tagvalue_cmp_t cmp, tag_value_t *b, regex_t *re);
int tvc_double(tag_value_t *a, tagvalue_cmp_t cmp, tag_value_t *b, regex_t *re);
int tvc_datetime(tag_value_t *a, tagvalue_cmp_t cmp, tag_value_t *b,
                 regex_t *re);
double fractod(const char *val, char **r_end);
int tv_parser_datetime(const char *val, int64_t *v, datetime_fuzz_t *f,
                       tagvalue_cmp_t cmp);
int tvp_timezone(const char *val, int *r_offset, const char **r_end);

extern ss128_head_t *posts;
extern ss128_head_t *tags;
extern ss128_head_t *tagaliases;
extern ss128_head_t *tagguids;
extern hash_t       *strings;
extern list_head_t  *postlist_nodes;

extern uint64_t *logindex;
extern uint64_t *first_logindex;
extern uint64_t *logdumpindex;

extern const char * const *filetype_names;
extern const char * const *rating_names;
extern const char * const *tagtype_names;
extern md5_t config_md5;

extern const char *basedir;

extern connection_t *logconn;

extern int server_running;
extern int log_version;
extern int default_timezone;

#define MANDATORY_MAGIC_TAGS 3
#define REALLY_MAGIC_TAGS 7
extern tag_t *magic_tag[10];
extern const char *magic_tag_guids[];
extern tag_t *magic_tag_rotate;
extern tag_t *magic_tag_modified;
extern tag_t *magic_tag_created;
