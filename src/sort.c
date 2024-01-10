#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "bf.h"
#include "hp_file.h"
#include "record.h"
#include "sort.h"
#include "merge.h"
#include "chunk.h"

bool shouldSwap(Record* rec1,Record* rec2){
    
    // Sort based on name and surname
    int nameCmp = strcmp(rec1->name, rec2->name);
    int surnameCmp = strcmp(rec1->surname, rec2->surname);

    // If names are equal, sort based on surname
    if (nameCmp == 0) {
        return surnameCmp > 0;  // return true if rec1>rec2, so swap
    }

    // Otherwise, sort based on name
    return nameCmp > 0;         // return true if rec1.name > rec2.name
}

void sort_FileInChunks(int file_desc, int numBlocksInChunk){
    CHUNK_Iterator iterator = CHUNK_CreateIterator(file_desc, numBlocksInChunk);
    CHUNK chunk;

    // Iterate through chunks
    while (CHUNK_GetNext(&iterator, &chunk) == 0) {

        sort_Chunk(&chunk);
    }
}

void sort_Chunk(CHUNK* chunk){
    Record record_i, record_j;

    // Sort the chunk using a simple sorting algorithm 
    for (int i = 0; i < chunk->recordsInChunk; ++i) {
        CHUNK_GetIthRecordInChunk(chunk, i, &record_i);

        for (int j = i + 1; j < chunk->recordsInChunk; ++j) {
            CHUNK_GetIthRecordInChunk(chunk, j, &record_j);

            if (shouldSwap(&record_i,&record_j)) {
                // Swap records if necessary (based on ID)
                CHUNK_UpdateIthRecord(chunk, i, record_j);
                CHUNK_UpdateIthRecord(chunk, j, record_i);

                // Update record_i for next comparison
                record_i = record_j;
            }
        }
    }
}