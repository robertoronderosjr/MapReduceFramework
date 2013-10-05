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
#include "framework.h"
#include "Wordcount.h"
#include "Sort.h"
using namespace std;

int main (int argc, char* argv[])
{
	string app, impl, infile, outfile;
	int num_maps, num_reducers;
	long num_lines;

	if(argc != 13){
		cout << "Incorrect Number of Arguments! \nUsage: --app [wordcount, sort] --impl [procs, threads]";
		cout << " --maps num_maps --reduces num_reduces --input infile --output outfile";
		exit(1);
	}

	app = argv[2];
	impl = argv[4];
	num_maps = atoi(argv[6]);
	num_reducers = atoi(argv[8]);
	infile = argv[10];
	outfile = argv[12];

	if(app.compare("wordcount")==0)
		WORDCOUNT = true;
	else
		WORDCOUNT = false;

	cout << "Arguments are: " << app << " " << impl << " " << num_maps << " " << num_reducers << " " << infile << " " << outfile << "\n";
	num_lines = splitter(num_maps, infile);
	int num_map_lines[num_maps];
	assign_lines(num_map_lines,num_lines, num_maps);

	for(int i=0; i<num_maps; i++){

		cout << "Map " << i << ": " << num_map_lines[i] << "\n";
	}

    create_map_threads(num_maps,num_map_lines,infile);

	return 0;
}

long splitter(int &num_maps, string file){

	int num_lines = 0;
	string line;
	ifstream infile(file.c_str());

	while( getline (infile,line) ){
		num_lines++;
	}

	if(num_lines<=num_maps)
		num_maps = num_lines;

	return num_lines;
}

void assign_lines(int num_map_lines[], int num_lines, int num_maps){

	cout << "Number of lines in the file is: " << num_lines << "\nNumbers of lines divided into mappers: " << num_lines/num_maps << "\nThe mod: " << num_lines%num_maps << "\n";

	int lines = (num_lines/num_maps);

	for(int i=0; i<num_maps-1; i++){
		num_map_lines[i] = lines;
	}

	int extra_lines = num_lines%num_maps;

	if(extra_lines > 0)
		num_map_lines[num_maps-1] = (lines+extra_lines);
	else
		num_map_lines[num_maps-1] = lines;

}

void create_map_threads(int num_maps, int num_map_lines[], string file){

	int counter_test = 0;
	int mapper_length;
	ifstream infile(file.c_str());

	for(int i=0; i<num_maps; i++){

		mapper_length = num_map_lines[i];
		string mapper_lines[mapper_length];

		for(int j=0; j<mapper_length; j++)
		{
			getline(infile,mapper_lines[j]);
			counter_test++;
		}

		//Create Thread and send mapper_lines.
	}

	cout << "Number of lines processed: " << counter_test << "\n";
}

void mapper(int length, string mapper_lines[]){

	Wordcount app_word;
	Sort app_sort;

	for(int i=0; i<length; i++){
		if (WORDCOUNT)
		    app_word.map(mapper_lines[i]);
		else
			app_sort.map(mapper_lines[i]);
	}
}
