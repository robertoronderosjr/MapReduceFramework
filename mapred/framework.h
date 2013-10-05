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
using namespace std;

bool WORDCOUNT;

long splitter(int&, string);
void assign_lines(int[], int, int);
void create_map_threads(int, int[], string);
void mapper(int, string[]);

#endif /* FRAMEWORK_H_ */
