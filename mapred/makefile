CC=g++
CFLAGS=-c -Wall

all: mapred

mapred: main.o 
	$(CC) main.o -o mapred -pthread

main.o: main.cpp framework.h 
	$(CC) $(CFLAGS) main.cpp framework.h -pthread

clean:
	rm -rf *o mapred
