#include "db.h"

connection_t *logconn;

static int dummy_error(connection_t *conn, const char *msg) {
	(void)conn;
	(void)msg;
	return 1;
}

int main(int argc, char **argv) {
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
	printf("initing mm..\n");
	if (mm_init()) {
		assert(argc == 2);
		populate_from_log(argv[1]);
	}
	mm_print();
	log_init();
	printf("serving..\n");
	db_serve();
	printf("Cleaning up mm..\n");
	mm_cleanup();
	return 0;
}
