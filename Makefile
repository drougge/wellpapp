CFLAGS  += -std=gnu99 -Wall -Werror -W -Wundef -Wshadow -Wpointer-arith -Wbad-function-cast -Wcast-qual -Wcast-align -Wwrite-strings -Wsign-compare -Wstrict-prototypes -Wmissing-prototypes -Wmissing-declarations -Wredundant-decls -Wnested-externs -Winline -Wold-style-definition -g

CPPFLAGS += -Iutf8proc
LDFLAGS += -Lutf8proc

OBJS=db.o rbtree.o mm.o client.o log.o guid.o string.o protocol.o result.o \
     connection.o utf.o sort.o list.o hash.o datetime.o valuetype.o

LIBS= -lutf8proc -lcrypto -lm -lbz2 -pthread

default: server

all: server pearsonr

server: server.o $(OBJS) utf8proc/libutf8proc.a
	$(CC) $(LDFLAGS) -o server server.o $(OBJS) $(LIBS)

*.o: db.h
list.o: list.c list.h db.h

pearsonr: pearsonr.c
	$(CC) $(CFLAGS) -o pearsonr pearsonr.c -lm

utf8proc/libutf8proc.a:
	cd utf8proc; make libutf8proc.a

clean:
	cd utf8proc; make clean
	rm -f server pearsonr *.o
