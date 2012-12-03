#include "db.h"

#include <signal.h>
#include <dirent.h>

connection_t *logconn;

static int dummy_error(connection_t *conn, const char *msg)
{
	(void)conn;
	(void)msg;
	return 1;
}

static uint64_t str2u64(const char *str)
{
	uint64_t val;
	char    *end;
	int     len = strlen(str);
	val = strtoull(str, &end, 16);
	assert((len == 16 && !*end) || (len == 20 && !strcmp(end, ".bz2")));
	return val;
}

static void log_next(const char *line)
{
	assert(*line == 'L');
	*logindex = *first_logindex = str2u64(line + 1);
}

tag_t *magic_tag_rotate = NULL;
tag_t *magic_tag_modified = NULL;
tag_t *magic_tag_created = NULL;
tag_t *magic_tag_gps = NULL;

void after_fixups(void)
{
	const valuetype_t fixup_type[] = {VT_UINT,     // width
	                                  VT_UINT,     // height
	                                  VT_WORD,     // ext
	                                  VT_DATETIME, // created
	                                  VT_DATETIME, // imgdate
	                                  VT_INT,      // rotate
	                                  VT_DATETIME, // modified
	                                  VT_INT,      // score
	                                  VT_STRING,   // source
	                                  VT_STRING,   // title
	                                  VT_GPS,      // gps
	                                 };
	for (int i = 0; magic_tag_guids[i]; i++) {
		tag_t *tag = tag_find_guidstr(magic_tag_guids[i]);
		if (tag) {
			err1(tag->valuetype != fixup_type[i]);
			magic_tag[i] = tag;
			if (i < REALLY_MAGIC_TAGS) {
				tag->unsettable = 1;
				tag->datatag    = 1;
			}
		} else {
			err1(i < REALLY_MAGIC_TAGS);
		}
	}
	magic_tag_rotate   = magic_tag[5];
	magic_tag_modified = magic_tag[6];
	magic_tag_created = magic_tag[3];
	magic_tag[4]->unsettable = 0; // imgdate is settable
	magic_tag_gps = magic_tag[10];
	err1(!magic_tag_gps);
	magic_tag_gps->datatag = 1;
	return;
err:
	printf("Missing/bad fixups. Please read UPGRADE.\n");
	exit(1);
}

void apply_fixups(int i)
{
	char buf[1024];
	int  len;
	len = snprintf(buf, sizeof(buf), "%s/fixup.%d", basedir, i);
	assert(len < (int)sizeof(buf));
	if (!access(buf, F_OK)) {
		printf("Reading fixup.%d..\n", i);
		int r = populate_from_log(buf, NULL);
		assert(!r);
	}
}

static void populate_from_dump(void)
{
	uint64_t      last_dump = ~0ULL;
	char          buf[1024];
	int           len;
	DIR           *dir;
	struct dirent *dirent;

	len = snprintf(buf, sizeof(buf), "%s/dump", basedir);
	assert(len < (int)sizeof(buf));
	dir = opendir(buf);
	assert(dir);
	while ((dirent = readdir(dir))) {
		if (*dirent->d_name != '.') {
			uint64_t dumpnr = str2u64(dirent->d_name);
			if (dumpnr > last_dump || last_dump == ~0ULL) {
				last_dump = dumpnr;
			}
		}
	}
	closedir(dir);
	if (last_dump != ~0ULL) {
		printf("Reading dump %016llx..\n", (unsigned long long)last_dump);
		len = snprintf(buf, sizeof(buf), "%s/dump/%016llx",
		               basedir, (unsigned long long)last_dump);
		assert(len < (int)sizeof(buf));
		*logdumpindex = last_dump + 1;
		*logindex = ~0ULL;
		populate_from_log(buf, log_next);
		assert(*logindex != ~0ULL);
	}
	while (1) {
		int r;

		printf("Reading log %016llx..\n", (unsigned long long)*logindex);
		len = snprintf(buf, sizeof(buf), "%s/log/%016llx",
		               basedir, (unsigned long long)*logindex);
		assert(len < (int)sizeof(buf));
		(*logindex)++;
		r = populate_from_log(buf, NULL);
		if (r) break;
	}
	printf("Log recovery complete.\n");
	(*logindex)--;
}

static void sig_die(int sig)
{
	(void)sig;
	server_running = 0;
}

extern guid_t *server_guid;
static int blacklisted_guid(void)
{
	guid_t example;
	int r = guid_str2guid(&example, "fSaP69-3QS9RA-aaaaaa-aaaaaa", GUIDTYPE_SERVER);
	assert(!r);
	return !memcmp(&example, server_guid, sizeof(example));
}

static void itercount(ss128_key_t key, ss128_value_t value, void *data_)
{
	long long *count = data_;
	(void) key;
	(void) value;
	(*count)++;
}

int main(int argc, char **argv)
{
	connection_t logconn_;

	// mktime should assume UTC.
	char env_no_TZ[] = "TZ=";
	putenv(env_no_TZ);
	tzset();

	logconn = &logconn_;
	memset(logconn, 0, sizeof(*logconn));
	logconn->error = dummy_error;
	logconn->sock  = -1;
	logconn->flags = CONNFLAG_LOG;
	logconn->trans.conn = logconn;

	if (argc != 2) {
		fprintf(stderr, "Usage: %s configfile\n", argv[0]);
		return 1;
	}
	db_read_cfg(argv[1]);
	printf("initing mm..\n");
	if (prot_init()) return 1;
	if (mm_init()) {
		populate_from_dump();
		if (!magic_tag[0]) {
			internal_fixups0();
			internal_fixups1();
		}
	}
	after_fixups();
	if (!*logdumpindex && blacklisted_guid()) {
		fprintf(stderr, "Don't use the example GUID\n");
		return 1;
	}
	mm_print();
	long long post_count = 0, tag_count = 0;
	ss128_iterate(posts, itercount, &post_count);
	ss128_iterate(tags, itercount, &tag_count);
	printf("%lld posts, %lld tags, %lu strings.\n", post_count, tag_count,
	       strings->used);
	mm_start_walker();
	log_version = LOG_VERSION;
	log_init();
	printf("serving..\n");
	signal(SIGINT, sig_die);
	signal(SIGPIPE, SIG_IGN);
	db_serve();
	printf("Cleaning up mm..\n");
	conn_cleanup();
	log_cleanup();
	mm_cleanup();
	return 0;
}
