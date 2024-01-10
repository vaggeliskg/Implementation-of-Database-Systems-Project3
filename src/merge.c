#include <merge.h>
#include <stdio.h>
#include <stdbool.h>
#include "chunk.h"
#include "hp_file.h"
#include <string.h>
#include "sort.h"

/* Merges every b chunks of size chunkSize from the input file to the specified output file. The function takes input file and output file descriptors, chunk size, and the number of chunks to merge. It should internally use a CHUNK_Iterator and a CHUNK_RecordIterator. */
void merge(int input_FileDesc, int chunkSize, int bWay, int output_FileDesc) {
    CHUNK chunk;
    CHUNK_Iterator iterator =  CHUNK_CreateIterator(input_FileDesc, chunkSize);
    CHUNK_RecordIterator* Record_Iterator = malloc(bWay* sizeof(CHUNK_RecordIterator));
    Record* records = malloc(bWay * sizeof(Record));
    
    int total_chunks = HP_GetIdOfLastBlock(input_FileDesc) / chunkSize;
    int algo_bway = bWay;

    for(int b=0; b<total_chunks; b++){
        bWay = algo_bway;

        // Get an iterator in the beggining of each b chunks
        for(int i=0; i<bWay; i++){
            if(CHUNK_GetNext(&iterator, &chunk)== -1) {
                // printf("no more chunks\n");
                bWay = bWay - (bWay-i);
                // break;
            }
            Record_Iterator[i] = CHUNK_CreateRecordIterator(&chunk);
            if(CHUNK_GetNextRecord(&Record_Iterator[i], &records[i]) == -1){
                // printf("no more records in this chunk\n");
                // break;
            }
        }

        Record min_record;
        int min_record_pos = 0;
        min_record = records[0];
        
        // Find min record and its position from the first bway records in each chunk
        for(int i=0; i<bWay; i++){
            if(shouldSwap(&min_record, &records[i])){
                min_record = records[i];
                min_record_pos = i;
            }
        }

        // Compare all records in each chunk and insert them in a sorted order in outputfile
        while(true){
            if(bWay<=0){
                break;
            }

            // Insert the min_record from the b records in records[] array
            HP_InsertEntry(output_FileDesc, min_record);

            // Move iterator to the next records from the one moved(min_record) and change the previous one in records[] array
            if(CHUNK_GetNextRecord(&Record_Iterator[min_record_pos], &records[min_record_pos]) == -1){
                // resize record iterator array and move its values accordingly
                //If is the last chunk
                if(min_record_pos == bWay-1){
                    bWay --;
                    min_record_pos = 0;
                }else if(min_record_pos < bWay-1){
                    for(int i = min_record_pos; i<bWay; i++){
                        Record_Iterator[i] = Record_Iterator[i+1];
                        records[i] = records[i+1];
                    }
                    bWay --;
                    min_record_pos = bWay-1;       
                }
                
            }

            // Initialize min_record
            min_record = records[min_record_pos];

            // Find the real min_record from records[] array
            for(int i=0; i<bWay; i++){
                if(shouldSwap(&min_record, &records[i]) == true){
                    min_record = records[i];
                    min_record_pos = i;
                }
            }

        }
    }

}
