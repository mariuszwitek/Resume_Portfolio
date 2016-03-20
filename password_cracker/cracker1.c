/**
 * Machine Problem: Password Cracker
 * CS 241 - Spring 2016
 */

#include "cracker1.h"
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include "libs/utils.h"
#include "libs/queue.h"
#include "libs/format.h"
#include "pthread.h"
#include <crypt.h>
#include <signal.h>

typedef struct p_data {
	char name[9];
	char passhash[14];
	char pre[9];
	size_t periods;
} p_data;

Queue* que;
pthread_mutex_t mux = PTHREAD_MUTEX_INITIALIZER;
int numRecovered;
int numFailed;

char check(char* pre, char* check_string, char* passhash, struct crypt_data* cryp) {
	char cc[14];
	cc[0] = '\0';
	strcat(cc,pre);
	strcat(cc,check_string);
	const char* checkhash = crypt_r(cc,"xx",cryp);		
	if (strcmp(checkhash,passhash)==0) {
		return 1;
	}
	return 0;
}

void solve(p_data* dat, int id, double t_time) {
	v1_print_thread_start(id, dat->name);
	char add[dat->periods + 1];	
	//create more for v2
	struct crypt_data cdata;
	cdata.initialized = 0;
	int notfound = 0;
	size_t i;
	for (i = 0; i < dat->periods; i++) add[i] = 'a';
	add[dat->periods] = '\0';
	i = 0;
	do {
		i++;
		if (check(dat->pre, add, dat->passhash, &cdata)) {
			notfound = 1;
			break;
		}
	} while (incrementString(add));
	char cc[14];
	cc[0] = '\0';
	strcat(cc,dat->pre);
	strcat(cc,add);
	pthread_mutex_lock(&mux);
	if (!notfound) numFailed++;
	else numRecovered++;
	pthread_mutex_unlock(&mux);
	v1_print_thread_result(id, dat->name, cc, i,getThreadCPUTime() - t_time , !notfound);
	free(dat);
}

void* roll(void* arg) {
	int id = *(int*)arg;
	double t_time = getThreadCPUTime();
	p_data* dat;
	while (1) {
		dat = (p_data*)Queue_pull(que);
		if (dat == NULL) {

			return NULL;
		}
		solve(dat, id, t_time);
		t_time = getThreadCPUTime();
	}
}

int start(size_t thread_count) {
	double timeElapsed = getTime();
	double totalCPUTime = getCPUTime(); 
	que = malloc(sizeof(Queue));
	Queue_init(que,0);
	char *line = NULL;
	size_t len = 0;
	while (getline(&line, &len, stdin) != -1) {
		p_data* ent = malloc(sizeof(p_data));
		char known[9];
		sscanf(line, "%s %s %s", ent->name, ent->passhash, known);
		size_t i = 0;
		strcpy(ent->pre, known);
	    char* pos = strchr(known, '.');
		if (pos) {
			while (*pos++ == '.') {
				i++;
			}
		}
		ent->pre[getPrefixLength(ent->pre)] = '\0';
		ent->periods = i;
		Queue_push(que,ent);
	}
	for (size_t i = 0; i < thread_count; i++) {
		Queue_push(que,NULL);
	}
	free(line);
	
	pthread_t dreads[thread_count];
	int thread_ids[thread_count];
	for (size_t i = 0; i < thread_count; i++) {
		thread_ids[i] = i+1;
		pthread_create(&dreads[i], NULL, roll, thread_ids+i);
	}
	for (size_t i = 0; i < thread_count; i++) {
		pthread_join(dreads[i],NULL);
	}
	Queue_destroy(que);
	free(que);
	v1_print_summary(numRecovered, numFailed, getTime() - timeElapsed, getCPUTime() - totalCPUTime);
    return 0;
}
