#include <merge.h>
#include <stdio.h>
#include "chunk.h"


CHUNK_Iterator CHUNK_CreateIterator(int fileDesc, int blocksInChunk){
    CHUNK_Iterator iterator;
    iterator.file_desc = fileDesc;
    iterator.current = 1;  
    iterator.lastBlocksID = HP_GetIdOfLastBlock(fileDesc);
    iterator.blocksInChunk = blocksInChunk;

    return iterator;
}


int CHUNK_GetNext(CHUNK_Iterator *iterator,CHUNK* chunk){
    // Check if there are more chunks to traverse
    if (iterator->current > iterator->lastBlocksID) {
        // No more chunks available
        return -1;  // Indicate end of chunks
    }

    // Assign information to the given 'chunk'
    chunk->file_desc = iterator->file_desc;
    chunk->from_BlockId = iterator->current;
    chunk->to_BlockId = iterator->current + iterator->blocksInChunk - 1;

    // Ensure 'to_BlockId' does not exceed the last block ID
    if (chunk->to_BlockId > iterator->lastBlocksID) {
        chunk->to_BlockId = iterator->lastBlocksID;
    }

    // Calculate records and blocks in this chunk
    chunk->recordsInChunk = countRecordsInChunk(chunk);/* Calculate number of records in this chunk */
    chunk->blocksInChunk = chunk->to_BlockId - chunk->from_BlockId + 1;

    // Move the iterator to the next chunk
    iterator->current = chunk->to_BlockId + 1;

    return 0;  // Successfully retrieved the next chunk
}


int CHUNK_GetIthRecordInChunk(CHUNK* chunk,  int i, Record* record){

    int currentRecord = 0;

    for (int blockId = chunk->from_BlockId; blockId <= chunk->to_BlockId; ++blockId) {
        int blockRecordCount = HP_GetRecordCounter(chunk->file_desc, blockId);
        for (int j = 0; j < blockRecordCount; ++j) {
            if (currentRecord == i) {
                // Found the ith record in the chunk
                if (HP_GetRecord(chunk->file_desc, blockId, j, record) == -1) {
                    // Failed to retrieve the record
                    return -1;
                }
                // Unpin the block after reading the record
                if (HP_Unpin(chunk->file_desc, blockId) != 0) {
                    printf("Unpin Error\n");
                    return -1;
                }
                return 0;  // Successfully retrieved the record
            }
            currentRecord++;
        }
    }

    // If i is out of bounds for the chunk
    return -1;
}


int CHUNK_UpdateIthRecord(CHUNK* chunk,  int i, Record record){

    int currentRecord = 0;

    for (int blockId = chunk->from_BlockId; blockId <= chunk->to_BlockId; ++blockId) {
        int blockRecordCount = HP_GetRecordCounter(chunk->file_desc, blockId);
        for (int j = 0; j < blockRecordCount; ++j) {
            if (currentRecord == i) {
                // Found the ith record in the chunk, update it
                if (HP_UpdateRecord(chunk->file_desc, blockId, j, record) == -1) {
                    // Failed to update the record
                    return -1;
                }
                // Unpin the block after updating the record
                if (HP_Unpin(chunk->file_desc, blockId) != 0) {
                    printf("Unpin error 2\n");
                    return -1;
                }
                return 0;  // Successfully updated the record
            }
            currentRecord++;
        }
    }
}


void CHUNK_Print(CHUNK chunk) {
    Record record;
    printf("Printing records in chunk from block %d to block %d:\n", chunk.from_BlockId, chunk.to_BlockId);
    
    for (int blockId = chunk.from_BlockId; blockId <= chunk.to_BlockId; ++blockId) {
        int blockRecordCount = HP_GetRecordCounter(chunk.file_desc, blockId);
        for (int j = 0; j < blockRecordCount; ++j) {
            if (HP_GetRecord(chunk.file_desc, blockId, j, &record) != -1) {
                // Print the retrieved record
                printf("ID: %d, Name: %s, Surname: %s, City: %s\n", record.id, record.name, record.surname, record.city);
            }
            if (HP_Unpin(chunk.file_desc, blockId) != 0) {
                    printf("Unpin error 3\n");
                }
        }
    }
}



CHUNK_RecordIterator CHUNK_CreateRecordIterator(CHUNK *chunk){
    CHUNK_RecordIterator iterator;
    iterator.chunk = *chunk;
    iterator.currentBlockId = chunk->from_BlockId;
    iterator.cursor = 0; // Starting cursor position

    return iterator;
}

int CHUNK_GetNextRecord(CHUNK_RecordIterator *iterator,Record* record){
    CHUNK* chunk = &(iterator->chunk);
    int blockId = iterator->currentBlockId;
    int cursor = iterator->cursor;

    while(true){
        int records_in_block = HP_GetRecordCounter(chunk->file_desc, blockId);

        if(blockId == chunk->to_BlockId && cursor >= records_in_block){
            return -1;
        }else if (cursor >= records_in_block){
            cursor = 0;
            iterator->cursor = 0;
            blockId ++;
            iterator->currentBlockId ++;
        }

        if (HP_GetRecord(chunk->file_desc, blockId, cursor, record) != -1) {
            // Successfully retrieved the record, update iterator's position

            iterator->cursor = iterator->cursor + 1;

            if (HP_Unpin(chunk->file_desc, blockId) != 0) {
                printf("Unpin error 4\n");
                return -1;
            }
            return 0;  // Return success
        }

    }
}



int countRecordsInChunk(CHUNK* chunk) {
    int totalRecords = 0;
    Record record;
    for (int i = chunk->from_BlockId; i <= chunk->to_BlockId; ++i) {
        for (int j = 0; j < HP_GetRecordCounter(chunk->file_desc, i); ++j) {
                totalRecords++;
        }
    }
    return totalRecords;
}

