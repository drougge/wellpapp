CFLAGS  += -std=gnu99 -pedantic -I/usr/local/include -I/opt/utf8proc/1.1.2/include -I/usr/include/postgresql -Wall -Werror -W -Wundef -Wshadow -Wpointer-arith -Wbad-function-cast -Wcast-qual -Wcast-align -Wwrite-strings -Wsign-compare -Wstrict-prototypes -Wmissing-prototypes -Wmissing-declarations -Wredundant-decls -Wnested-externs -Winline -g

OBJS=db.o rbtree.o mm.o client.o log.o guid.o string.o protocol.o result.o \
     connection.o utf.o

LIBS=-L/opt/utf8proc/1.1.2/lib -L/opt/libmd/unknown/lib -lutf8proc -lbsd

LDFLAGS += -Wl,-rpath /opt/utf8proc/1.1.2/lib

all: server pgtest pearsonr

pgtest: pgtest.o $(OBJS)
	$(CC) -o pgtest pgtest.o $(OBJS) $(LIBS) -lpq

server: server.o $(OBJS)
	$(CC) $(LDFLAGS) -o server server.o $(OBJS) $(LIBS)

*.o: db.h config.h

pearsonr: pearsonr.c
	$(CC) $(CFLAGS) -o pearsonr pearsonr.c -lm

clean:
	rm -f server pgtest pearsonr *.o
