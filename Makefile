CFLAGS  += -I/usr/local/include -Wall -Werror -g
LDFLAGS += -L/usr/local/lib -lpq -lmd

pgtest: pgtest.o rbtree.o mm.o client.o log.o guid.o
	$(CC) -o pgtest pgtest.o rbtree.o mm.o client.o log.o guid.o $(LDFLAGS)

*.o: db.h config.h

pearsonr: pearsonr.c
	$(CC) -o pearsonr pearsonr.c -lm

clean:
	rm -f pgtest pearsonr *.o
