CFLAGS  += -std=c99 -pedantic -I/usr/local/include -Wall -Werror -W -Wundef -Wshadow -Wpointer-arith -Wbad-function-cast -Wcast-qual -Wcast-align -Wwrite-strings -Wsign-compare -Wstrict-prototypes -Wmissing-prototypes -Wmissing-declarations -Wredundant-decls -Wnested-externs -Winline -g

OBJS=db.o rbtree.o mm.o client.o log.o guid.o string.o protocol.o

all: server pgtest pearsonr

pgtest: pgtest.o $(OBJS)
	$(CC) -o pgtest pgtest.o $(OBJS) -L/usr/local/lib -lpq -lmd

server: server.o $(OBJS)
	$(CC) -o server server.o $(OBJS) -lmd

*.o: db.h config.h

pearsonr: pearsonr.c
	$(CC) $(CFLAGS) -o pearsonr pearsonr.c -lm

clean:
	rm -f server pgtest pearsonr *.o
