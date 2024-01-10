#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "merge.h"

#define RECORDS_NUM 10000 // you can change it if you want
#define FILE_NAME "data.db"
#define OUT_NAME "out"




int createAndPopulateHeapFile(char* filename);

void sortPhase(int file_desc,int chunkSize);

void mergePhases(int inputFileDesc,int chunkSize,int bWay, int* fileCounter);

int nextOutputFile(int* fileCounter);

//////////////////////////////
#include <dirent.h>     //////
#include <unistd.h>     //////
//////////////////////////////
int main() {
  // Delete the file before execution
	// Remember to delete this, is just for testing
	/////////////////////////////////////////////////
    DIR *dir;
    struct dirent *entry;
    // Open the current directory
    dir = opendir(".");
    if (dir == NULL) {
        perror("Error opening directory");
        return 1;
    }
    // Iterate over each entry in the directory
    while ((entry = readdir(dir)) != NULL) {
        // Check if the entry starts with "ouy"
        if (strncmp(entry->d_name, "out", 3) == 0) {
            // Construct the full path of the file
            char filepath[512];
            snprintf(filepath, sizeof(filepath), "%s/%s", ".", entry->d_name);
            // Remove the file
            if (unlink(filepath) == 0) {
                printf("Removed file: %s\n", entry->d_name);
            } else {
                perror("Error removing file");
            }
        }
    }
    // Close the directory
    closedir(dir);

  if (remove("data.db") == 0) {					\
      printf("file deleted successfully.\n");		\
  } else {										\
      perror("Error deleting file");				\
  }												\
	/////////////////////////////////////////////////

  int chunkSize = 5;
  int bWay= 4;
  int fileIterator;
  
  BF_Init(LRU);
  int file_desc = createAndPopulateHeapFile(FILE_NAME);
  sortPhase(file_desc,chunkSize);
  mergePhases(file_desc,chunkSize,bWay,&fileIterator);
}

int createAndPopulateHeapFile(char* filename){
  HP_CreateFile(filename);
  
  int file_desc;
  HP_OpenFile(filename, &file_desc);

  Record record;
  srand(12569874);
  for (int id = 0; id < RECORDS_NUM; ++id)
  {
    record = randomRecord();
    HP_InsertEntry(file_desc, record);
  }
  return file_desc;
}

void printAllRecordss(int file_desc) {
    CHUNK_Iterator iterator = CHUNK_CreateIterator(file_desc, 5);
    CHUNK chunk;

    while (CHUNK_GetNext(&iterator, &chunk) == 0) {
        CHUNK_Print(chunk);
    }
}

/*Performs the sorting phase of external merge sort algorithm on a file specified by 'file_desc', using chunks of size 'chunkSize'*/
void sortPhase(int file_desc,int chunkSize){ 
  sort_FileInChunks( file_desc, chunkSize);
}

/* Performs the merge phase of the external merge sort algorithm  using chunks of size 'chunkSize' and 'bWay' merging. The merge phase may be performed in more than one cycles.*/
void mergePhases(int inputFileDesc,int chunkSize,int bWay, int* fileCounter){
  int oututFileDesc;
  while(chunkSize<=HP_GetIdOfLastBlock(inputFileDesc)){
    oututFileDesc =   nextOutputFile(fileCounter);
    merge(inputFileDesc, chunkSize, bWay, oututFileDesc );
    HP_CloseFile(inputFileDesc);
    chunkSize*=bWay;
    inputFileDesc = oututFileDesc;
  }
  printAllRecordss(oututFileDesc);
  HP_CloseFile(oututFileDesc);
}

/*Creates a sequence of heap files: out0.db, out1.db, ... and returns for each heap file its corresponding file descriptor. */
int nextOutputFile(int* fileCounter){
    char mergedFile[50];
    char tmp[] = "out";
    sprintf(mergedFile, "%s%d.db", tmp, (*fileCounter)++);
    int file_desc;
    HP_CreateFile(mergedFile);
    HP_OpenFile(mergedFile, &file_desc);
    return file_desc;
}

