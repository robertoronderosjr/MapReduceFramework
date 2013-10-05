CC=g++
CFLAGS=-c -Wall

all: mapred

mapred: main.o 
	$(CC) main.o -o mapred

main.o: main.cpp framework.h Wordcount.h Sort.h
	$(CC) $(CFLAGS) main.cpp framework.h Wordcount.h Sort.h

clean:
	rm -rf *o mapred
