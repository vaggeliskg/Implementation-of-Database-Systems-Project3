#ifndef RECORD_H
#define RECORD_H


typedef struct Record {
  	char name[15];
	char surname[15];
	char city[15];
	int id;	
	char delimiter[2];
} Record;

Record randomRecord();

void printRecord(Record record);

#endif
