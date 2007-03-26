CFLAGS  += -I/usr/local/include -Wall -Werror -g
LDFLAGS += -L/usr/local/lib -lpq -lmd

pgtest: pgtest.o rbtree.o mm.o
	$(CC) -o pgtest pgtest.o rbtree.o mm.o $(LDFLAGS)

clean:
	rm -f pgtest *.o
