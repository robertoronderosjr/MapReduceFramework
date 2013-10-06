/*
 * main.cpp
 *
 *  Created on: Oct 4, 2013
 *      Author: cataladu
 */

#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <fstream>
#include <string.h>
#include <vector>
#include <pthread.h>
#include "framework.h"
using namespace std;

int main(int argc, char* argv[]) {
	string app, impl, infile, outfile;
	int num_maps, num_reducers;
	int num_lines;

	if (argc != 13) {
		cout
				<< "Incorrect Number of Arguments! \nUsage: --app [wordcount, sort] --impl [procs, threads]";
		cout
				<< " --maps num_maps --reduces num_reduces --input infile --output outfile";
		exit(1);
	}

	app = argv[2];
	impl = argv[4];
	num_maps = atoi(argv[6]);
	num_reducers = atoi(argv[8]);
	infile = argv[10];
	outfile = argv[12];

	if (app.compare("wordcount") == 0)
		WORDCOUNT = true;
	else
		WORDCOUNT = false;

	cout << "Arguments are: " << app << " " << impl << " " << num_maps << " "
			<< num_reducers << " " << infile << " " << outfile << "\n";

	num_lines = splitter(num_maps, infile);
	int num_map_lines[num_maps];
	assign_lines(num_map_lines, num_lines, num_maps);
	create_map_threads(num_maps, num_map_lines, infile);
	cout << "Map Phase Completed" << endl;
	create_buffer();
	assign_keys(num_reducers);
	create_reduce_threads(num_reducers);
	cout << "Reduce Phase Completed" << endl;
	generate_output(outfile);
	cout << "Output Created";

	/* DEBUGGING TO BE DELETED
	 for (map<string, vector<int> >::iterator ii = buffer.begin();
	 ii != buffer.end(); ++ii) {
	 //keys.push_back((*ii).first);
	 cout << (*ii).first << " [";
	 vector<int> value = (*ii).second;
	 for (unsigned int k = 0; k < value.size(); k++) {
	 cout << value.at(k);
	 if (k + 1 < value.size())
	 cout << ",";
	 }
	 cout << "]" << endl;
	 }*/
	//free(offsets);
	//free(start_keys);
	return 0;
}

int splitter(int &num_maps, string file) {

	int num_lines = 0;
	string line;
	ifstream infile(file.c_str());

	while (getline(infile, line)) {
		num_lines++;
	}

	if (num_lines <= num_maps)
		num_maps = num_lines;

	return num_lines;
}

void assign_lines(int num_map_lines[], int num_lines, int num_maps) {
	/*
	 cout << "Number of lines in the file is: " << num_lines
	 << "\nNumbers of lines divided into mappers: "
	 << num_lines / num_maps << "\nThe mod: " << num_lines % num_maps
	 << "\n";*/

	int lines = (num_lines / num_maps);

	for (int i = 0; i < num_maps - 1; i++) {
		num_map_lines[i] = lines;
	}

	int extra_lines = num_lines % num_maps;

	if (extra_lines > 0)
		num_map_lines[num_maps - 1] = (lines + extra_lines);
	else
		num_map_lines[num_maps - 1] = lines;

}

void assign_keys(int &num_reducers) {

	int keys, keys_per_reducer, index, counter;
	int* start_reducers;
	keys = buffer.size();

	if (keys < num_reducers)
		num_reducers = keys;

	start_reducers = new int[num_reducers];
	start_keys = new string[num_reducers];
	offsets = new int[num_reducers];

	keys_per_reducer = keys / num_reducers;

	//cout << "Keys per reducer: " << keys_per_reducer;

	for (int i = 0; i < num_reducers - 1; i++) {

		start_reducers[i] = i * keys_per_reducer;
		offsets[i] = keys_per_reducer;
	}

	index = num_reducers - 1;

	if (keys % num_reducers == 0) {
		start_reducers[index] = index * keys_per_reducer;
		offsets[index] = keys_per_reducer;
	} else {
		start_reducers[index] = index * keys_per_reducer;
		offsets[index] = keys - start_reducers[index];
	}

	counter = 0;
	int tempPos = 0;

	for (map<string, vector<int> >::iterator ii = buffer.begin();
			ii != buffer.end(); ++ii) {

		if (counter == start_reducers[tempPos]) {
			start_keys[tempPos] = ((*ii).first);
			tempPos++;
		}

		counter++;
	}
	/*DEBUGGING TO BE DELETED!
	cout << endl;
	for (int j = 0; j < num_reducers; j++) {
		cout << "Start: " << start_reducers[j] << " Start word: "
				<< start_keys[j] << endl;
	}*/

	//free(start_reducers);
}

void create_map_threads(int num_maps, int num_map_lines[], string file) {

	pthread_t threads[num_maps];
	pthread_attr_t attr;
	thread_data td[num_maps];
	void* status;
	int rc;
	mapEmitters.resize(num_maps);
	int counter_test = 0;
	int mapper_length;
	ifstream infile(file.c_str());

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

	for (int i = 0; i < num_maps; i++) {

		mapper_length = num_map_lines[i];
		string* mapper_lines = new string[mapper_length];

		for (int j = 0; j < mapper_length; j++) {
			getline(infile, mapper_lines[j]);
			counter_test++;
		}
		//Thread data
		td[i].thread_id = i;
		td[i].mapper_lines = mapper_lines;
		td[i].length = mapper_length;

		rc = pthread_create(&threads[i], NULL, mapper, (void*) &td[i]);
		if (rc) {
			cout << "Error: Unable to create thread, " << rc << endl;
			exit(-1);
		}
		//free(mapper_lines);
	}

	//Free attributes memory and create barrier
	pthread_attr_destroy(&attr);

	for (int k = 0; k < num_maps; k++) {
		rc = pthread_join(threads[k], &status);
		if (rc) {
			cout << "Error: Unable to join thread, " << rc << endl;
			exit(-1);
		}
		//DEBUGGIN TEST - TO BE REMOVED
		cout << "Completed map thread id: " << k;
		cout << " exiting with status: " << status << endl;
	}
	/*
	 cout << "Number of lines processed: " << counter_test << "\n";
	 cout << "Printing vector: " << endl;

	 for (int p = 0; p < num_maps; p++) {
	 cout << "Position: " << p << endl;
	 for (unsigned int q = 0; q < mapEmitters.at(p).size(); q++) {
	 cout << mapEmitters.at(p).at(q).key << " "
	 << mapEmitters.at(p).at(q).value << endl;
	 }
	 cout << endl;
	 }*/
}

void create_reduce_threads(int &num_reducers) {

	pthread_t threads[num_reducers];
	pthread_attr_t attr;
	thread_data2 td[num_reducers];
	void* status;
	int rc;

	pthread_attr_init(&attr);
	pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);

	for (int i = 0; i < num_reducers; i++) {

		//Thread data
		td[i].thread_id = i;
		td[i].start_word = start_keys[i];

		rc = pthread_create(&threads[i], NULL, reducer, (void*) &td[i]);
		if (rc) {
			cout << "Error: Unable to create thread, " << rc << endl;
			exit(-1);
		}
	}

	//Free attributes memory and create barrier
	pthread_attr_destroy(&attr);

	for (int k = 0; k < num_reducers; k++) {
		rc = pthread_join(threads[k], &status);
		if (rc) {
			cout << "Error: Unable to join thread, " << rc << endl;
			exit(-1);
		}
		//DEBUGGIN TEST - TO BE REMOVED
		//cout << "Completed reduce thread id: " << k;
		//cout << " exiting with status: " << status << endl;
	}

}

void *mapper(void* threadarg) {

	struct thread_data *my_data;
	my_data = (struct thread_data*) threadarg;
	/*
	 cout << endl << "I'm thread with id: " << my_data->thread_id << " and length: " << my_data->length << endl;

	 for(int j=0; j<my_data->length; j++){
	 cout << my_data->mapper_lines[j] << endl;
	 }
	 cout << endl;*/

	for (int i = 0; i < my_data->length; i++) {
		pthread_mutex_lock(&mutex1);
		if (WORDCOUNT) {
			wordcount_map(my_data->mapper_lines[i], my_data->thread_id);
		} else {
			sort_map(my_data->mapper_lines[i], my_data->thread_id);
		}
		pthread_mutex_unlock(&mutex1);
	}

	return 0;
}

void *reducer(void* threadarg) {

	struct thread_data2 *my_data;
	my_data = (struct thread_data2*) threadarg;
	map<string, vector<int> >::iterator ii;

	for (int i = 0; i < offsets[my_data->thread_id]; i++) {
		pthread_mutex_lock(&mutex2);
		if (i == 0) {
			ii = buffer.find(my_data->start_word);
		}

		string key = (*ii).first;
		vector<int> values = (*ii).second;

		if (WORDCOUNT) {
			wordcount_reducer(key, values);
		} else {
			sort_reducer(key, values);
		}

		++ii;
		pthread_mutex_unlock(&mutex2);
	}

	return 0;
}

void wordcount_map(string line, int thread_id) {

	char* line_str = strdup(line.c_str());
	char* word;
	word = strtok(line_str, " ");

	while (word != NULL) {
		//cout << "Word: " << word << " Thread_id " << thread_id << endl;
		emitter_map(word, 1, thread_id);
		word = strtok(NULL, " ");
	}
}

void sort_map(string line, int thread_id) {

	char* line_str = strdup(line.c_str());
	char* number;
	number = strtok(line_str, " ");

	while (number != NULL) {
		emitter_map(number, 1, thread_id);
		number = strtok(NULL, " ");
	}
}

void emitter_map(string key, int value, int thread_id) {

	KVPair KVP;
	KVP.key = key;
	KVP.value = value;

	mapEmitters.at(thread_id).push_back(KVP);
}

void create_buffer() {

	int maps = mapEmitters.size();
//cout << maps;
	for (int i = 0; i < maps; i++) {

		for (unsigned int j = 0; j < mapEmitters.at(i).size(); j++) {

			string key = mapEmitters.at(i).at(j).key;

			if (buffer.find(key) == buffer.end()) {
				//cout << "here 1!";
				vector<int> value;
				value.push_back(1);
				buffer.insert(pair<string, vector<int> >(key, value));
			} else {
				//cout << "here 2!";
				(buffer.find(key)->second).push_back(1);
			}
			/*
			 cout << mapEmitters.at(i).at(j).key << " "
			 << mapEmitters.at(i).at(j).value << endl;*/
		}
		//cout << endl;
	}
//cout << endl << buffer.size() << endl;
/*
	for (map<string, vector<int> >::iterator ii = buffer.begin();
			ii != buffer.end(); ++ii) {
		//keys.push_back((*ii).first);
		cout << (*ii).first << " [";
		vector<int> value = (*ii).second;
		for (unsigned int k = 0; k < value.size(); k++) {
			cout << value.at(k);
			if (k + 1 < value.size())
				cout << ",";
		}
		cout << "]" << endl;
	}*/
}

void wordcount_reducer(string key, vector<int> values) {

	int count = 0;
	map<string, vector<int> >::iterator ii;

	for (vector<int>::iterator it = values.begin(); it != values.end(); ++it) {
		count++;
	}

	ii = buffer.find(key);
	(ii->second).clear();
	(ii->second).push_back(count);
}

void sort_reducer(string key, vector<int> values) {

	int count = 0;
	map<string, vector<int> >::iterator ii;

	for (vector<int>::iterator it = values.begin(); it != values.end(); ++it) {
		count++;
	}

	ii = buffer.find(key);
	(ii->second).clear();
	(ii->second).push_back(count);

}

void generate_output(string outfile) {

	ofstream myfile;
	myfile.open(outfile.c_str());

	if (WORDCOUNT) {
		for (map<string, vector<int> >::iterator ii = buffer.begin();
				ii != buffer.end(); ++ii) {
			myfile << (*ii).first << " ";
			vector<int> value = (*ii).second;
			myfile << value.at(0) << "\n";
		}
	} else {
		for (map<string, vector<int> >::iterator ii = buffer.begin();
				ii != buffer.end(); ++ii) {

			vector<int> value = (*ii).second;
			int occ = value.at(0);

			for(int i=0; i<occ; i++){
				myfile << (*ii).first << "\n";
			}
		}
	}
}
