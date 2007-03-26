#include "db.h"

void client_handle(int s) {
	write(s, "test\n", 5);
}
