#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "hashmap.h"
#include <assert.h>
#include <unistd.h>
#include <math.h>

#define KEY_MAX_LENGTH (64)
//#define KEY_PREFIX ("somekey")
#define KEY_COUNT (10)
struct Config 
{
    int world_rank;
    int world_size;

    char* file;
	MPI_File dataFile;
    MPI_Offset dataFileSize;
    char* text;
    int textSize;


    char* textBlock;
    int textBlockSize;
    
    MPI_Request request;
    map_t mymap;
};
struct Config config;

void load_file();
void load_file_old();
void dispurse_data();
int jump_back(int position);

typedef struct data_struct_s
{
    char key_string[KEY_MAX_LENGTH];
    int number;
} data_struct_t;

int main(int argc, char **argv){
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &config.world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &config.world_size);

    if(config.world_rank == 0){
        config.file = "test.txt";
        load_file();

        /*printf("[%d]\n", config.world_rank);
        for(int i = 0; i < config.textSize; i++){
            printf("%c", config.text[i]);
        }
        printf("\n");*/

        distribute_data();
        
    } 
    else {
        MPI_Recv(&config.textBlockSize, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        config.textBlock = (char*)malloc ((config.textBlockSize)*sizeof(char));
        MPI_Recv(config.textBlock, config.textBlockSize, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }

    
    sleep(config.world_rank);
    printf("[%d] blockSize: %d\n", config.world_rank, config.textBlockSize);
    //printf("[%d]\n", config.world_rank);
    for(int i = 0; i < config.textBlockSize; i++){
        printf("%c", config.textBlock[i]);

    }
    printf("\n");

    sleep((config.world_rank+1)*1000);
    //MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
}

void load_file(){
    MPI_File_open(MPI_COMM_SELF, config.file, MPI_MODE_RDONLY, MPI_INFO_NULL, &config.dataFile);
    MPI_File_get_size(config.dataFile, &config.dataFileSize);
    config.textSize = (int) config.dataFileSize;
    config.text = (char*)malloc ((config.dataFileSize)*sizeof(char));
    MPI_File_read_all(config.dataFile, config.text, config.dataFileSize, MPI_CHAR, MPI_STATUS_IGNORE);
    MPI_File_close(&config.dataFile);
}

void distribute_data(){
    int filePointerIndex = 0;
    int preferredBlockSize = config.dataFileSize / config.world_size + 1;
    int* blockLengths = (int*)malloc ((config.world_size)*sizeof(int));
    
    blockLengths[0] = preferredBlockSize - jump_back(filePointerIndex + preferredBlockSize);
    filePointerIndex += blockLengths[0] + 1;
    config.textBlockSize = blockLengths[0];
    config.textBlock = config.text;

    for(int i = 1; i < config.world_size; i++){
        if(i != config.world_size-1){
            blockLengths[i] = preferredBlockSize - jump_back(filePointerIndex + preferredBlockSize);
            MPI_Isend(&(blockLengths[i]), 1, MPI_INT, i, 0, MPI_COMM_WORLD, &config.request);
            MPI_Isend(&(config.text[filePointerIndex]), blockLengths[i], MPI_CHAR, i, 1, MPI_COMM_WORLD, &config.request);
        }
        else{
            blockLengths[i] = config.textSize - filePointerIndex;
            MPI_Isend(&(blockLengths[i]), 1, MPI_INT, i, 0, MPI_COMM_WORLD, &config.request);
            MPI_Isend(&(config.text[filePointerIndex]), blockLengths[i], MPI_CHAR, i, 1, MPI_COMM_WORLD, &config.request);
        }
        filePointerIndex += blockLengths[i]+1;
    }
}

int jump_back(int position){
    int i = 0;
    while(1){
        if(config.text[position - i] == ' ' || config.text[position - i] == '\0' || config.text[position - i] == '\n'){
            return i;
        }
        i++;
    }
}