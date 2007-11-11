#include "db.h"

#include <signal.h>

guid_t server_guid;

connection_t *logconn;

static int dummy_error(connection_t *conn, const char *msg) {
	(void)conn;
	(void)msg;
	return 1;
}

static void sig_dump(int sig) {
	(void)sig;
	printf("Dumping complete log..\n");
	log_dump();
	printf("Dump done.\n");
}

int main(int argc, char **argv) {
	int          r = 0;
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

	db_read_cfg();
	r = guid_str2guid(&server_guid, "fSaP69-3QS9RA-aaaaaa-aaaaaa", GUIDTYPE_SERVER);
	assert(!r);
	printf("initing mm..\n");
	if (mm_init(!access("/tmp/db/mm_cache/00000000", F_OK))) {
		assert(argc == 2);
		populate_from_log(argv[1]);
	}
	mm_print();
	log_init();
	signal(SIGUSR1, sig_dump);
	printf("serving..\n");
	db_serve();
	return 1;
}
