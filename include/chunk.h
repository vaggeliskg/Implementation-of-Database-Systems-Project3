#ifndef CHUNK_H
#define CHUNK_H

#include <stdio.h>
#include <stdlib.h>
#include "hp_file.h"


/* Represents a chunk of records in a file, defining the file descriptor, starting and ending block IDs, and the counts of records and blocks in the chunk. Useful for managing and sorting records within specific chunks. */
typedef struct {
    int file_desc;
    int from_BlockId;
    int to_BlockId;
    int recordsInChunk;
    int blocksInChunk;
} CHUNK;

/* Represents an iterator for traversing chunks within a file, storing the file descriptor, current block, last block ID, and the total number of blocks in each chunk. Useful for efficiently iterating over file chunks.*/
typedef struct {
    int file_desc;
    int current;
    int lastBlocksID;
    int blocksInChunk;
} CHUNK_Iterator;

/* Creates a ChunkIterator for efficient traversal of chunks within a file, specified by the file descriptor. The iterator is configured with a defined range of blocks (usually starting from block 1), along with the size of each chunk and the maximum number of records in each block.*/
CHUNK_Iterator CHUNK_CreateIterator(int fileDesc, int blocksInChunk);

/* Retrieves the next CHUNK in the sequence as per the provided CHUNK_Iterator. */
int CHUNK_GetNext(CHUNK_Iterator *iterator,CHUNK* chunk);

/* Retrieves the ith record from a CHUNK of blocks in a heap file. Returns 0 if successful, populating the 'record' parameter; otherwise, -1. Assumes sequential ordering of records within the chunk.*/
int CHUNK_GetIthRecordInChunk(CHUNK* chunk,  int i, Record* record);//

/* Updates the ith record in a chunk. Returns 0 if successful; -1 if unsuccessful. Facilitates efficient and controlled updates within a chunk.*/
int CHUNK_UpdateIthRecord(CHUNK* chunk,  int i, Record record);//

/* This function is used to print the records within a chunk.*/
void CHUNK_Print(CHUNK chunk);//


/* Iterates through records in a CHUNK, encapsulating the id of the current block and a cursor in that block. */
typedef struct CHUNK_RecordIterator {
    CHUNK chunk;
    int currentBlockId;
    int cursor;
} CHUNK_RecordIterator;

/* Creates a record iterator for efficient traversal within a CHUNK. */
CHUNK_RecordIterator CHUNK_CreateRecordIterator(CHUNK *chunk);


/* Function to get the next record from the iterator. */
int CHUNK_GetNextRecord(CHUNK_RecordIterator *iterator,Record* record);

int countRecordsInChunk(CHUNK* chunk);


#endif  // MY_HEADER_H