CFLAGS  += -I/usr/local/include -Wall -Werror -g
LDFLAGS += -L/usr/local/lib -lpq -lmd

pgtest: pgtest.o rbtree.o mm.o client.o
	$(CC) -o pgtest pgtest.o rbtree.o mm.o client.o $(LDFLAGS)

clean:
	rm -f pgtest *.o
