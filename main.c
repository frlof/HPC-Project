#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "hashmap.h"
#include <assert.h>
#include <unistd.h>

#define KEY_MAX_LENGTH (64)
//#define KEY_PREFIX ("somekey")
#define KEY_COUNT (10)
struct Config 
{
	MPI_File dataBlock;
    int world_rank;
    int world_size;
    char* text;
    char* textBlock;
    int textSize;
    int blockSize;
    MPI_Request request;
    map_t mymap;
};
struct Config config;

typedef struct data_struct_s
{
    char key_string[KEY_MAX_LENGTH];
    int number;
} data_struct_t;

int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &config.world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &config.world_size);
    if(config.world_rank == 0){
        load_file();
        dispurse_data();
    } 
    MPI_Recv(&config.blockSize, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

    config.textBlock = (char*)malloc ((config.blockSize)*sizeof(char));

    MPI_Irecv(config.textBlock, config.blockSize, MPI_CHAR, 0, 1, MPI_COMM_WORLD, &config.request);
    replace();
    //printf("apaaaaa\n");
    sleep(config.world_rank);
    /*if(config.world_rank == 3)
    {
        //printf("%c\n", config.textBlock[config.blockSize+1]);
    for(int i = 0; i < config.blockSize; i++)
        {
            //printf("[%d] %c %d\n", config.world_rank, config.textBlock[i], config.textBlock[i]);
            printf("%c",config.textBlock[i]);
        }
        printf("Config size: %d\n", config.blockSize);
    } */

    if(config.world_rank != -1)
    {
        create_hash_map();
    }
 
    MPI_Finalize();
}

void create_hash_map()
{
    //int index;
    int error;
    char key_string[KEY_MAX_LENGTH];
    data_struct_t* value;
    //printf("First: %d\n", config.world_rank);
    config.mymap = hashmap_new();
    //printf("Second: %d\n", config.world_rank);
    /* First, populate the hash map with ascending values */
    char *KEY_PREFIX;
    int pos = 0;
    int index = 0;
    while(pos < config.blockSize)
    {
        KEY_PREFIX = &config.textBlock[pos];
        /* Store the key string along side the numerical value so we can free it later */
        value = malloc(sizeof(data_struct_t));
        snprintf(value->key_string, KEY_MAX_LENGTH, "%s%d", &config.textBlock[pos], index);
        value->number = index;
        error = hashmap_put(config.mymap, value->key_string, value);
        snprintf(key_string, KEY_MAX_LENGTH, "%s%d", &config.textBlock[pos], index);
        hashmap_get(config.mymap, key_string, (void**)(&value));
        assert(error==MAP_OK);
        pos = pos + word_length(pos);
        index += 1;
    }
    /* Now, check all of the expected values are there */
    pos = 0;
    index = 0;
    while(pos < config.blockSize)
    {
        KEY_PREFIX = &config.textBlock[pos];
        snprintf(key_string, KEY_MAX_LENGTH, "%s%d", &config.textBlock[pos], index);

        error = hashmap_get(config.mymap, key_string, (void**)(&value));
        
        /* Make sure the value was both found and the correct number */
        assert(error==MAP_OK);
        assert(value->number==index);
        pos = pos + word_length(pos);
        index += 1;
    }
    //printf("Third: %d\n", config.world_rank);
    /* Make sure that a value that wasn't in the map can't be found */
    snprintf(key_string, KEY_MAX_LENGTH, "%s%d", &config.textBlock[pos], KEY_COUNT);

    error = hashmap_get(config.mymap, key_string, (void**)(&value));
        
    /* Make sure the value was not found */
    assert(error==MAP_MISSING);

    /* Free all of the values we allocated and remove them from the map */
    pos = 0;
    index = 0;
    while(pos < config.blockSize)
    {
        KEY_PREFIX = &config.textBlock[pos];
        printf("CHar: %c\n", config.textBlock[pos]);
        snprintf(key_string, KEY_MAX_LENGTH, "%s%d", &config.textBlock[pos], index);

        error = hashmap_get(config.mymap, key_string, (void**)(&value));
        assert(error==MAP_OK);
        printf("World rank[%d] %s\n", config.world_rank, value->key_string);
        error = hashmap_remove(config.mymap, key_string);
        assert(error==MAP_OK);

        free(value);        
        pos = pos + word_length(pos);
        index += 1;
    }
    
    /* Now, destroy the map */
    hashmap_free(config.mymap);

}

void replace()
{
    for(int i = 0; i < config.blockSize; i++)
    {
        if(config.textBlock[i] == ' ' || config.textBlock[i] == '\n')
        {
            config.textBlock[i] = '\0';
        }
    }
}

int word_length(int position)
{
    int i = 0;
    while(1)
    {
        if(config.textBlock[position + i] == '\0')
        {
            return i+1;
        }
        i = i + 1;
    }
}
int jump_back(int position)
{
    int i = 0;
    while(1)
    {
        if(config.text[position - i] == ' ' || config.text[position - i] == '\0')
        {
            return i;
        }
        i = i + 1;
    }
}

void dispurse_data()
{
    int i;
    int length;
    int all = 0;
    for(i = 0; i < config.world_size; i++)
    {
        length = config.blockSize - jump_back(all + config.blockSize) + 1;
        if(i != config.world_size-1)
        {
            MPI_Isend(&length, 1, MPI_INT, i, 0, MPI_COMM_WORLD, &config.request);
            MPI_Isend(&(config.text[all]), length, 
            MPI_CHAR, i, 1, MPI_COMM_WORLD, &config.request);
        }
        else
        {
            int temp = config.blockSize*(i+1) - all;
            MPI_Isend(&temp, 1, MPI_INT, i, 0, MPI_COMM_WORLD, &config.request);
            MPI_Isend(&(config.text[all]), temp, 
            MPI_CHAR, i, 1, MPI_COMM_WORLD, &config.request);
        }
        all += length;
    }
}

void load_file()
{
    config.text;
    FILE * f;
    f = fopen ("test.txt", "rb"); //was "rb"
    if (f)
    {
      fseek (f, 0, SEEK_END);
      config.textSize = ftell (f);
      printf("config.text; %d\n", config.textSize);
      config.blockSize = (config.textSize+1) / config.world_size;
      fseek (f, 0, SEEK_SET);
      config.text = (char*)malloc ((config.textSize+1)*sizeof(char));
      if (config.text)
      {
        fread (config.text, sizeof(char), config.textSize, f);
      }
      fclose (f);
    }
    printf("apaa %c\n", config.text[config.textSize-1]);
    config.text[config.textSize] = '\0';
    printf("banan %c\n", config.text[config.textSize-1]);
} 
