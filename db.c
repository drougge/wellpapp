#include "db.h"

#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <poll.h>
#include <errno.h>
#include <openssl/md5.h>
#include <utf8proc.h>

#ifndef INFTIM
#define INFTIM -1
#endif

void NORETURN assert_fail(const char *ass, const char *file,
                          const char *func, int line)
{
	fprintf(stderr, "assertion \"%s\" failed in %s on %s:%d\n",
	        ass, func, file, line);
	exit(1);
}

ss128_head_t *tags;
ss128_head_t *tagaliases;
ss128_head_t *tagguids;
ss128_head_t *posts;
ss128_head_t *users;

// @@TODO: Locking/locklessness.

void postlist_iterate(postlist_t *pl, void *data,
                      postlist_callback_t callback)
{
	postlist_node_t *pn = pl->head;
	while (pn) {
		unsigned int i;
		for (i = 0; i < arraylen(pn->posts); i++) {
			if (pn->posts[i]) {
				callback(data, pn->posts[i]);
			}
		}
		pn = pn->next;
	}
}

static int postlist_remove(postlist_t *pl, post_t *post)
{
	postlist_node_t *pn;

	assert(pl);
	assert(post);

	pn = pl->head;
	while (pn) {
		unsigned int i;
		for (i = 0; i < arraylen(pn->posts); i++) {
			if (pn->posts[i] == post) {
				pn->posts[i] = NULL;
				pl->count--;
				pl->holes++;
				return 0;
			}
		}
		pn = pn->next;
	}
	return 1;
}

static void postlist_add(postlist_t *pl, post_t *post)
{
	postlist_node_t *pn;

	assert(pl);
	assert(post);

	pl->count++;
	if (pl->holes) {
		pn = pl->head;
		while (pn) {
			unsigned int i;
			for (i = 0; i < arraylen(pn->posts); i++) {
				if (!pn->posts[i]) {
					pn->posts[i] = post;
					pl->holes--;
					return;
				}
			}
			pn = pn->next;
		}
		assert(!pl->holes);
		return; // NOTREACHED
	}
	pn = mm_alloc(sizeof(*pn));
	pn->posts[0]  = post;
	pl->holes    += arraylen(pn->posts) - 1;
	pn->next      = pl->head;
	pl->head      = pn;
}

static int postlist_contains(const postlist_t *pl, const post_t *post)
{
	const postlist_node_t *pn = pl->head;
	while (pn) {
		unsigned int i;
		for (i = 0; i < arraylen(pn->posts); i++) {
			if (pn->posts[i] == post) return 1;
		}
		pn = pn->next;
	}
	return 0;
}

int taglist_contains(const post_taglist_t *tl, const tag_t *tag)
{
	while (tl) {
		for (int i = 0; i < arraylen(tl->tags); i++) {
			if (tl->tags[i] == tag) return 1;
		}
		tl = tl->next;
	}
	return 0;
}

typedef struct alloc_seg {
	struct alloc_seg *next;
} alloc_seg_t;
typedef struct alloc_data {
	alloc_seg_t *segs;
} alloc_data_t;
typedef void *(*alloc_func_t)(alloc_data_t *data, unsigned int size);

static void *alloc_temp(alloc_data_t *data, unsigned int size)
{
	alloc_seg_t *seg;
	seg = calloc(1, sizeof(seg) + size);
	seg->next = data->segs;
	data->segs = seg;
	return seg + 1;
}
static void alloc_temp_free(alloc_data_t *data)
{
	alloc_seg_t *seg = data->segs;
	while (seg) {
		alloc_seg_t *next = seg->next;
		free(seg);
		seg = next;
	}
}
static void *alloc_mm(alloc_data_t *data, unsigned int size)
{
	(void) data;
	return mm_alloc(size);
}

static int taglist_add(post_taglist_t **tlp, tag_t *tag, alloc_func_t alloc,
                       alloc_data_t *adata)
{
	post_taglist_t *tl = *tlp;
	while (tl) {
		for (int i = 0; i < arraylen(tl->tags); i++) {
			if (!tl->tags[i]) {
				tl->tags[i] = tag;
				return 0;
			}
			if (tl->tags[i] == tag) return 1;
		}
		tl = tl->next;
	}
	tl = alloc(adata, sizeof(*tl));
	tl->tags[0] = tag;
	tl->next = *tlp;
	*tlp = tl;
	return 0;
}

struct impl_iterator_data;
typedef struct impl_iterator_data impl_iterator_data_t;
typedef void (*impl_callback_t)(implication_t *impl, impl_iterator_data_t *data);
typedef struct implcomp_data {
	implication_t *impl;
	truth_t       weak;
} implcomp_data_t;
struct impl_iterator_data {
	implcomp_data_t *list;
	int             len;
	truth_t         weak;
	alloc_func_t    alloc;
	alloc_data_t    *adata;
	impl_callback_t callback;
	void            *callback_data;
};

static void impllist_iterate(impllist_t *impl, impl_iterator_data_t *data)
{
	while (impl) {
		for (int i = 0; i < arraylen(impl->impl); i++) {
			if (impl->impl[i].tag) {
				data->callback(&impl->impl[i], data);
			}
		}
		impl = impl->next;
	}
}

static void impl_cb(implication_t *impl, impl_iterator_data_t *data)
{
	if (data->list) {
		data->list[data->len].impl = impl;
		data->list[data->len].weak = data->weak;
	}
	data->len++;
}

static int impl_comp(const void *a_, const void *b_, void *data)
{
	const implcomp_data_t *a = a_;
	const implcomp_data_t *b = b_;
	(void) data;
	if (a->impl->priority == b->impl->priority) {
		if (a->weak == b->weak) return 0;
		if (a->weak) return 1;
		return -1;
	}
	return b->impl->priority - a->impl->priority;
}

static void post_implications(post_t *post, alloc_func_t alloc,
                              alloc_data_t *adata, post_taglist_t **res)
{
	impl_iterator_data_t impldata;
	post_taglist_t *tl;
	assert(post);
	assert(alloc);
	impldata.list = NULL;
	impldata.len = 0;
	impldata.weak = T_NO;
	impldata.alloc = alloc;
	impldata.adata = adata;
	impldata.callback = impl_cb;
again:
	tl = impldata.weak ? post->weak_tags : &post->tags;
	while (tl) {
		for (int i = 0; i < arraylen(tl->tags); i++) {
			tag_t *tag = tl->tags[i];
			if (tag && tag->implications) {
				impllist_iterate(tag->implications, &impldata);
			}
		}
		tl = tl->next;
	}
	if (!impldata.weak) {
		impldata.weak = T_YES;
		goto again;
	}
	if (!impldata.list && impldata.len) {
		impldata.list = malloc(sizeof(*impldata.list) * impldata.len);
		impldata.len = 0;
		impldata.weak = T_NO;
		goto again;
	}
	if (impldata.list) {
		implcomp_data_t *list = impldata.list;
		int             len = impldata.len;
		sort(list, len, sizeof(*list), impl_comp, NULL);
		for (int i = 0; i < len; i++) {
			int skip = 0;
			for (int j = 0; j < i; j++) {
				if (list[i].impl->tag == list[j].impl->tag) {
					skip = 1;
					break;
				}
			}
			if (!skip && list[i].impl->positive) {
				taglist_add(&res[list[i].weak],
				           list[i].impl->tag, alloc_mm, NULL);
			}
		}
		free(list);
	}
}

static int post_tag_add_i(post_t *post, tag_t *tag, truth_t weak);
static int post_tag_rem_i(post_t *post, tag_t *tag);
static int impl_apply_change(post_t *post, post_taglist_t **old,
                             post_taglist_t *new, truth_t weak)
{
	post_taglist_t *tl;
	int changed = 0;
	tl = new;
	while (tl) {
		for (int i = 0; i < arraylen(tl->tags); i++) {
			tag_t *tag = tl->tags[i];
			if (tag && !taglist_contains(*old, tag)) {
				if (!post_has_tag(post, tag, T_DONTCARE)) {
					post_tag_add_i(post, tag, weak);
					taglist_add(old, tag, alloc_mm, NULL);
					changed = 1;
				} else {
					tl->tags[i] = NULL;
				}
			}
		}
		tl = tl->next;
	}
	tl = *old;
	while (tl) {
		for (int i = 0; i < arraylen(tl->tags); i++) {
			tag_t *tag = tl->tags[i];
			if (tag && !taglist_contains(new, tag)) {
				post_tag_rem_i(post, tag);
				tl->tags[i] = NULL;
				changed = 1;
			}
		}
		tl = tl->next;
	}
	return changed;
}

static void post_recompute_implications(post_t *post)
{
	alloc_data_t adata;
	post_taglist_t *implied[2] = {NULL, NULL};
	int again;
again:
	again = 0;
	adata.segs = NULL;
	post_implications(post, alloc_temp, &adata, implied);
	again |= impl_apply_change(post, &post->implied_tags, implied[0], T_NO);
	again |= impl_apply_change(post, &post->implied_weak_tags, implied[1],
	                           T_YES);
	alloc_temp_free(&adata);
	if (again) goto again;
}

static void post_recompute_implications_iter(void *data, post_t *post)
{
	(void) data;
	post_recompute_implications(post);
}

static void postlist_recompute_implications(postlist_t *pl)
{
	postlist_iterate(pl, NULL, post_recompute_implications_iter);
}

int tag_add_implication(tag_t *from, tag_t *to, int positive, int32_t priority)
{
	impllist_t *tl = from->implications;
	int done = 0;
	while (tl) {
		for (int i = 0; i < arraylen(tl->impl); i++) {
			tag_t *tltag = tl->impl[i].tag;
			if ((!tltag || tltag == to) && !done) {
				tl->impl[i].tag = to;
				tl->impl[i].priority = priority;
				tl->impl[i].positive = positive;
				done = 1;
			} else if (tltag == to) {
				tl->impl[i].tag = NULL;
			}
		}
		tl = tl->next;
	}
	if (!done) {
		tl = mm_alloc(sizeof(*tl));
		tl->impl[0].tag = to;
		tl->impl[0].priority = priority;
		tl->impl[0].positive = positive;
		tl->next = from->implications;
		from->implications = tl;
	}
	postlist_recompute_implications(&from->posts);
	postlist_recompute_implications(&from->weak_posts);
	return 0;
}

int tag_rem_implication(tag_t *from, tag_t *to, int positive, int32_t priority)
{
	impllist_t *tl = from->implications;
	(void) priority;
	while (tl) {
		for (int i = 0; i < arraylen(tl->impl); i++) {
			if (tl->impl[i].tag == to
			    && tl->impl[i].positive == positive
			   ) {
				tl->impl[i].tag = NULL;
				postlist_recompute_implications(&from->posts);
				postlist_recompute_implications(&from->weak_posts);
				return 0;
			}
		}
		tl = tl->next;
	}
	return 1;
}

static int post_tag_rem_i(post_t *post, tag_t *tag)
{
	post_taglist_t *tl;
	postlist_t     *pl;
	int finished = 0;
	uint16_t *of_holes;

	assert(post);
	assert(tag);
	tl = &post->tags;
	pl = &tag->posts;
	of_holes = &post->of_holes;
again:
	while (tl) {
		unsigned int i;
		for (i = 0; i < arraylen(tl->tags); i++) {
			if (tl->tags[i] == tag) {
				tl->tags[i] = NULL;
				(*of_holes)++;
				return postlist_remove(pl, post);
			}
		}
		tl = tl->next;
	}
	if (finished) return 1;
	finished = 1;
	tl = post->weak_tags;
	pl = &tag->weak_posts;
	of_holes = &post->of_weak_holes;
	goto again;
}

int post_tag_rem(post_t *post, tag_t *tag)
{
	int r = post_tag_rem_i(post, tag);
	if (!r) post_recompute_implications(post);
	return r;
}

static int post_tag_add_i(post_t *post, tag_t *tag, truth_t weak)
{
	post_taglist_t *tl;
	post_taglist_t *ptl = NULL;
	uint16_t       *of_holes;
	int i;

	assert(post);
	assert(tag);
	assert(weak == T_YES || weak == T_NO);
	if (post_has_tag(post, tag, weak)) return 1;
	if (post_has_tag(post, tag, !weak)) {
		if (post_tag_rem_i(post, tag)) return 1;
	}
	if (weak) {
		postlist_add(&tag->weak_posts, post);
		tl = post->weak_tags;
		if (!tl) tl = post->weak_tags = mm_alloc(sizeof(*tl));
		post->of_weak_tags++;
		of_holes = &post->of_weak_holes;
	} else {
		postlist_add(&tag->posts, post);
		tl = &post->tags;
		post->of_tags++;
		of_holes = &post->of_holes;
	}
	while (tl) {
		for (i = 0; i < arraylen(tl->tags); i++) {
			if (!tl->tags[i]) {
				tl->tags[i] = tag;
				(*of_holes)--;
				return 0;
			}
		}
		ptl = tl;
		tl  = tl->next;
	}
	tl = mm_alloc(sizeof(*tl));
	tl->tags[0]  = tag;
	*of_holes   += arraylen(tl->tags) - 1;
	ptl->next    = tl;
	return 0;
}

int post_tag_add(post_t *post, tag_t *tag, truth_t weak)
{
	int r = post_tag_add_i(post, tag, weak);
	if (!r) post_recompute_implications(post);
	return r;
}

int post_has_rel(const post_t *post, const post_t *rel)
{
	return postlist_contains(&post->related_posts, rel);
}

int post_rel_add(post_t *a, post_t *b)
{
	if (post_has_rel(a, b)) return 1;
	assert(!post_has_rel(b, a));
	postlist_add(&a->related_posts, b);
	postlist_add(&b->related_posts, a);
	return 0;
}

int post_rel_remove(post_t *a, post_t *b)
{
	int r;
	r = postlist_remove(&a->related_posts, b);
	if (r) return 1;
	r = postlist_remove(&b->related_posts, a);
	assert(!r);
	return 0;
}

static int md5_digit2digit(int digit)
{
	if (digit >= '0' && digit <= '9') return digit - '0';
	if (digit >= 'a' && digit <= 'f') return digit - 'a' + 10;
	if (digit >= 'A' && digit <= 'F') return digit - 'A' + 10;
	return -1;
}

int md5_str2md5(md5_t *res_md5, const char *md5str)
{
	int i;

	if (strlen(md5str) != 32) return 1;
	for (i = 0; i < 16; i++) {
		int l = md5_digit2digit(md5str[i * 2]);
		int r = md5_digit2digit(md5str[i * 2 + 1]);
		if (l < 0 || r < 0) return 1;
		res_md5->m[i] = l << 4 | r;
	}
	return 0;
}

const char *md5_md52str(const md5_t md5)
{
	static char buf[33];
	static const char digits[] = {'0', '1', '2', '3', '4', '5', '6', '7',
	                              '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
	int i;

	for (i = 0; i < 16; i++) {
		buf[i * 2    ] = digits[md5.m[i] >> 4];
		buf[i * 2 + 1] = digits[md5.m[i] & 15];
	}
	buf[32] = 0;
	return buf;
}

int str2id(const char *str, const char * const *ids)
{
	int id   = 0;
	int sign = 1;
	if (*str == '-') {
		sign = -1;
		str++;
	}
	while (ids[id]) {
		if (!strcmp(str, ids[id])) return (id + 1) * sign;
		id++;
	}
	return 0;
}

tag_t *tag_find_guid(const guid_t guid)
{
	void *tag = NULL;
	ss128_find(tagguids, &tag, guid.key);
	return (tag_t *)tag;
}

tag_t *tag_find_guidstr(const char *guidstr)
{
	guid_t guid;
	if (guid_str2guid(&guid, guidstr, GUIDTYPE_TAG)) return NULL;
	return tag_find_guid(guid);
}

tag_t *tag_find_name(const char *name, truth_t alias)
{
	ss128_key_t hash = ss128_str2key(name);
	void        *tag = NULL;

	if (alias != T_YES) {
		ss128_find(tags, &tag, hash);
	}
	if (!tag && alias != T_NO) {
		ss128_find(tagaliases, &tag, hash);
		if (tag) tag = ((tagalias_t *)tag)->tag;
	}
	return (tag_t *)tag;
}

int post_has_tag(const post_t *post, const tag_t *tag, truth_t weak)
{
	const post_taglist_t *tl;
	assert(post);
	assert(tag);
again:
	if (weak) {
		tl = post->weak_tags;
	} else {
		tl = &post->tags;
	}
	while (tl) {
		unsigned int i;
		for (i = 0; i < arraylen(tl->tags); i++) {
			if (tl->tags[i] == tag) return 1;
		}
		tl = tl->next;
	}
	if (weak == T_DONTCARE) {
		weak = T_NO;
		goto again;
	}
	return 0;
}

int post_find_md5str(post_t **res_post, const char *md5str)
{
	md5_t md5;
	*res_post = NULL;
	if (md5_str2md5(&md5, md5str)) return -1;
	return ss128_find(posts, (void *)res_post, md5.key);
}

static int read_log_line(FILE *fh, char *buf, int len)
{
	if (!fgets(buf, len, fh)) {
		assert(feof(fh));
		return 0;
	}
	len = strlen(buf) - 1;
	assert(len > 8 && buf[len] == '\n');
	buf[len] = 0;
	return len;
}

static int populate_from_log_line(char *line)
{
	int r;
	switch (*line) {
		case 'A': // 'A'dd something
			r = prot_add(logconn, line + 1);
			break;
		case 'T': // 'T'ag post
			r = prot_tag_post(logconn, line + 1);
			break;
		case 'M': // 'M'odify post
			r = prot_modify(logconn, line + 1);
			break;
		case 'D': // 'D'elete something
			r = prot_delete(logconn, line + 1);
			break;
		case 'R': // 'R'elationship
			if (line[1] == 'R') {
				r = prot_rel_add(logconn, line + 2);
			} else {
				assert(line[1] == 'r');
				r = prot_rel_remove(logconn, line + 2);
			}
			break;
		case 'I': // 'I'mplication
			r = prot_implication(logconn, line + 1);
			break;
		case 'O': // 'O'rder
			r = prot_order(logconn, line + 1);
			break;
		default:
			printf("Log: What? %s\n", line);
			r = 1;
	}
	return r;
}

#define MAX_CONCURRENT_TRANSACTIONS 64
static int find_trans(trans_id_t *trans, trans_id_t needle)
{
	int i;
	for (i = 0; i < MAX_CONCURRENT_TRANSACTIONS; i++) {
		if (trans[i] == needle) return i;
	}
	return -1;
}

int populate_from_log(const char *filename, void (*callback)(const char *line))
{
	FILE       *fh;
	char       buf[4096];
	trans_id_t trans[MAX_CONCURRENT_TRANSACTIONS] = {0};
	time_t     transnow[MAX_CONCURRENT_TRANSACTIONS];
	int        len;
	long       line_nr = 0;

	fh = fopen(filename, "r");
	if (!fh) {
		assert(errno == ENOENT);
		return 1;
	}
	while ((len = read_log_line(fh, buf, sizeof(buf)))) {
		char       *end;
		trans_id_t tid = strtoull(buf + 1, &end, 16);
		line_nr++;
		err1(end != buf + 17);
		if (*buf == 'T') { // New transaction
			err1(len != 34);
			if (buf[17] == 'O') { // Complete transaction
				int trans_pos = find_trans(trans, tid);
				assert(trans_pos == -1);
				trans_pos = find_trans(trans, 0);
				assert(trans_pos != -1);
				trans[trans_pos] = tid;
				transnow[trans_pos] = strtoull(buf + 18, &end, 16);
				err1(end != buf + 34);
			} else if (buf[17] == 'U') { // Unfinished transaction
				// Do nothing
			} else { // What?
				goto err;
			}
		} else if (*buf == 'D') { // Data from transaction
			err1(len <= 18);
			int trans_pos = find_trans(trans, tid);
			if (trans_pos >= 0) {
				logconn->trans.now = transnow[trans_pos];
				err1(populate_from_log_line(buf + 18));
			} else {
				printf("Skipping data from incomplete transaction: %s\n", buf);
			}
		} else if (*buf == 'E') { // End of transaction
			int pos;
			err1(len != 17);
			pos = find_trans(trans, tid);
			if (pos != -1) trans[pos] = 0;
		} else { // What?
			err1(!callback);
			callback(buf);
		}
	}
	struct stat sb;
	int r = fstat(fileno(fh), &sb);
	assert(!r);
	fclose(fh);
	mm_last_log(sb.st_size, sb.st_mtime);
	return 0;
err:
	printf("Failed on line %ld:\n%s\n", line_nr, buf);
	exit(1);
}

#define MAX_CONNECTIONS 100

struct pollfd fds[MAX_CONNECTIONS + 1];
connection_t *connections[MAX_CONNECTIONS];
int connection_count = 0;
int server_running = 1;
static user_t anonymous;

void conn_cleanup(void)
{
	for(int i = 0; i < arraylen(connections); i++) {
		connection_t *conn = connections[i];
		if (conn) c_cleanup(conn);
	}
}

static void new_connection(void)
{
	int s = accept(fds[MAX_CONNECTIONS].fd, NULL, NULL);
	if (s < 0) {
		perror("accept");
	} else {
		connection_t *conn;
		int           i;

		for (i = 0; i < MAX_CONNECTIONS; i++) {
			if (!connections[i]) break;
		}
		if (i == MAX_CONNECTIONS
		 || c_init(&conn, s, &anonymous, c_error)) {
			close(s);
			return;
		}
		connections[i] = conn;
		fds[i].fd = s;
		fds[i].revents = 0;
		connection_count++;
	}
}

static char *utf_compose(connection_t *conn)
{
	uint8_t *buf;
	ssize_t res;
	int     flags = UTF8PROC_NULLTERM | UTF8PROC_STABLE | UTF8PROC_COMPOSE;

	res = utf8proc_map((uint8_t *)conn->linebuf, 0, &buf, flags);
	if (res < 0) {
		c_close_error(conn, E_UTF8);
		return NULL;
	}
	return (char *)buf;
}

static int port = 0;

void db_serve(void)
{
	int s, r, one, i;
	struct sockaddr_in addr;

	anonymous.name = "A";
	anonymous.caps = DEFAULT_CAPS;
	for (i = 0; i < MAX_CONNECTIONS; i++) {
		fds[i].fd = -1;
		fds[i].events = POLLIN;
		connections[i] = NULL;
	}

	s = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
	assert(s >= 0);
	one = 1;
	r = setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
	assert(!r);
	memset(&addr, 0, sizeof(addr));
#ifndef __linux__
	addr.sin_len    = sizeof(addr);
#endif
	addr.sin_family = AF_INET;
	assert(port);
	addr.sin_port   = htons(port);
	r = bind(s, (struct sockaddr *)&addr, sizeof(addr));
	assert(!r);
	r = listen(s, 5);
	assert(!r);
	fds[MAX_CONNECTIONS].fd = s;
	fds[MAX_CONNECTIONS].events = POLLIN;

	while (server_running) {
		int have_unread = 0;
		r = poll(fds, MAX_CONNECTIONS + 1, have_unread ? 0 : INFTIM);
		if (r == -1) {
			if (!server_running) return;
			perror("poll");
			continue;
		}
		if (fds[MAX_CONNECTIONS].revents & POLLIN) new_connection();
		for (i = 0; i < MAX_CONNECTIONS; i++) {
			connection_t *conn = connections[i];
			if (!conn) continue;
			if (fds[i].revents & POLLIN) {
				c_read_data(conn);
			}
			if (c_get_line(conn) > 0) {
				char *buf = utf_compose(conn);
				if (buf) {
					client_handle(conn, buf);
					free(buf);
				}
			}
			if (!(conn->flags & CONNFLAG_GOING)) {
				close(fds[i].fd);
				fds[i].fd = -1;
				connection_count--;
				c_cleanup(conn);
				connections[i] = NULL;
			} else {
				if (conn->getlen > conn->getpos) {
					have_unread = 1;
				}
			}
		}
	}
}

/* Pretty much strndup, but without checking for NUL, *
 * and without portability concerns.                  */
static const char *memdup(const char *src, size_t len) {
	char *res = malloc(len + 1);
	memcpy(res, src, len);
	res[len] = '\0';
	return res;
}

static void cfg_parse_list(const char * const **res_list, const char *str)
{
	int          words = 1;
	int          word;
	const char   **list;
	const char   *p;
	unsigned int len;

	p = str;
	while (*p) if (*p++ == ' ') words++;
	list = malloc(sizeof(const char *) * (words + 1));
	p = str;
	word = 0;
	while (*p) {
		str = p;
		assert(*p != ' ');
		while (*p && *p != ' ') p++;
		len = p - str;
		if (*p) p++;
		assert(word < words);
		list[word++] = memdup(str, len);
	}
	list[word] = NULL;
	assert(word == words);
	*res_list = list;
}

const char * const *tagtype_names = NULL;
const char * const *rating_names = NULL;
const char * const *filetype_names = NULL;
const char * const *cap_names = NULL;

const char *basedir = NULL;

const guid_t *server_guid = NULL;
static guid_t server_guid_;

md5_t config_md5;
extern uint8_t *MM_BASE_ADDR;

void db_read_cfg(const char *filename)
{
	char    buf[1024];
	FILE    *fh;
	MD5_CTX ctx;

	MD5_Init(&ctx);
	fh = fopen(filename, "r");
	assert(fh);
	while (fgets(buf, sizeof(buf), fh)) {
		int len = strlen(buf);
		assert(len && buf[len - 1] == '\n');
		MD5_Update(&ctx, (unsigned char *)buf, len);
		buf[len - 1] = '\0';
		if (!memcmp("tagtypes=", buf, 9)) {
			cfg_parse_list(&tagtype_names, buf + 9);
		} else if (!memcmp("ratings=", buf, 8)) {
			cfg_parse_list(&rating_names, buf + 8);
		} else if (!memcmp("basedir=", buf, 8)) {
			basedir = strdup(buf + 8);
			assert(basedir && *basedir == '/');
		} else if (!memcmp("guid=", buf, 5)) {
			int r = guid_str2guid(&server_guid_, buf + 5, GUIDTYPE_SERVER);
			assert(!r);
			server_guid = &server_guid_;
		} else if (!memcmp("port=", buf, 5)) {
			port = atoi(buf + 5);
		} else if (!memcmp("mm_base=", buf, 8)) {
			unsigned long long addr = strtoull(buf + 8, NULL, 0);
			MM_BASE_ADDR = (uint8_t *)(intptr_t)addr;
		} else {
			assert(*buf == '\0' || *buf == '#');
		}
	}
	assert(feof(fh));
	fclose(fh);
	cfg_parse_list(&filetype_names, FILETYPE_NAMES_STR);
	cfg_parse_list(&cap_names, CAP_NAMES_STR);
	assert(tagtype_names && rating_names && basedir && server_guid);
	MD5_Final(config_md5.m, &ctx);
}
