/*
 * Wordcount.h
 *
 *  Created on: Oct 4, 2013
 *      Author: cataladu
 */

#ifndef WORDCOUNT_H_
#define WORDCOUNT_H_
#include <stdio.h>

class Wordcount {

public:
	void map(string line){

		char* line_str = strdup(line.c_str());
		char* word;
		word = strtok(line_str," ");

		while (word != NULL)
		  {
			emitter_map(word,1);
			word = strtok (NULL, " ");
		  }
	}

	void reduce(){}
};

#endif /* WORDCOUNT_H_ */
