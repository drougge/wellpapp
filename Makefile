CFLAGS  += -I/usr/local/include -Wall -Werror -W -Wundef -Wshadow -Wpointer-arith -Wbad-function-cast -Wcast-qual -Wcast-align -Wwrite-strings -Wsign-compare -Wstrict-prototypes -Wmissing-prototypes -Wmissing-declarations -Wredundant-decls -Wnested-externs -Winline -g
LDFLAGS += -L/usr/local/lib -lpq -lmd

pgtest: pgtest.o db.o rbtree.o mm.o client.o log.o guid.o string.o protocol.o
	$(CC) -o pgtest pgtest.o db.o rbtree.o mm.o client.o log.o guid.o string.o protocol.o $(LDFLAGS)

*.o: db.h config.h

pearsonr: pearsonr.c
	$(CC) -o pearsonr pearsonr.c -lm

clean:
	rm -f pgtest pearsonr *.o
