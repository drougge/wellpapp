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
	assert(strlen(str) == 16);
	val = strtoull(str, &end, 16);
	assert(!*end);
	return val;
}

static void log_next(const char *line)
{
	assert(*line == 'L');
	*logindex = *first_logindex = str2u64(line + 1);
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

int main(int argc, char **argv)
{
	user_t       loguser_;
	connection_t logconn_;

	loguser_.name = "LOG-READER";
	loguser_.caps = ~0;
	logconn = &logconn_;
	memset(logconn, 0, sizeof(*logconn));
	logconn->user  = &loguser_;
	logconn->error = dummy_error;
	logconn->sock  = -1;
	logconn->flags = CONNFLAG_LOG;
	logconn->trans.conn = logconn;

	assert(argc == 2);
	db_read_cfg(argv[1]);
	printf("initing mm..\n");
	if (mm_init()) populate_from_dump();
	mm_print();
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
