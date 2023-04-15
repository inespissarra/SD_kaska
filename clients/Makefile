CFLAGS=-Wall -g

all: libkaska test

libkaska:
	$(MAKE) -C ../libkaska

test: test.o libkaska.so
	$(CC) -o $@ $< ./libkaska.so -Wl

test.o: kaska.h

clean:
	rm -f *.o test


