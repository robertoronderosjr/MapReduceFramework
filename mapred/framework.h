/*
 * framework.h
 *
 *  Created on: Oct 4, 2013
 *      Author: cataladu
 */

#ifndef FRAMEWORK_H_
#define FRAMEWORK_H_
#include <string>
#include <stdlib.h>
#include <vector>
#include <map>
using namespace std;

pthread_mutex_t mutex1 = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex2 = PTHREAD_MUTEX_INITIALIZER;
bool WORDCOUNT;

struct KVPair {
	string key;
	int value;
};

vector< vector<KVPair> > mapEmitters;
map <string,
vector<int> > buffer;
string* start_keys;
int* offsets;

struct thread_data {
	int thread_id;
	int length;
	string* mapper_lines;
};

struct thread_data2 {
	int thread_id;
	string start_word;
};

int splitter(int&, string);
void assign_lines(int[], int, int);
void assign_keys(int&);
void create_map_threads(int, int[], string);
void create_reduce_threads(int&);
void *mapper(void*);
void *reducer(void*);
void wordcount_map(string, int);
void sort_map(string, int);
void wordcount_reducer(string, vector<int>);
void sort_reducer(string, vector<int>);
void emitter_map(string, int, int);
void create_buffer();
void generate_output(string output);

#endif /* FRAMEWORK_H_ */
