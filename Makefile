CFLAGS  += -I/usr/local/include -Wall -Werror -g
LDFLAGS += -L/usr/local/lib -lpq -lmd

pgtest: pgtest.o rbtree.o mm.o client.o log.o
	$(CC) -o pgtest pgtest.o rbtree.o mm.o client.o log.o $(LDFLAGS)

pearsonr: pearsonr.c
	$(CC) -o pearsonr pearsonr.c -lm

clean:
	rm -f pgtest *.o
