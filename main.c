#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include "hashmap.h"
#include <assert.h>

#define KEY_MAX_LENGTH (64)
#define KEY_PREFIX ("somekey")
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
    //printf("apa");
    if(config.world_rank == 0) load_file();
    MPI_Bcast(&config.blockSize, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if(config.world_rank == 0)
    {
        dispurse_data();
    } 
    else 
    {
        config.textBlock = (char*)malloc ((config.blockSize)*sizeof(char));
    }
    MPI_Irecv(config.textBlock, config.blockSize, MPI_CHAR, 0, 0, 
    MPI_COMM_WORLD, &config.request);
    replace();
    create_hash_map();
    //printf("%d\n", config.world_rank);
    for (int i = 0; i < config.blockSize; i++) {
        //printf("buffer[%d] == %c\n", i, config.textBlock[i]);
    }
    if(config.world_rank == 0)
    {
        printf("apa: %c\n", config.textBlock[config.blockSize - 1]);
        printf("size %d Worldrank : %d buffer = %s\n", config.blockSize, config.world_rank, config.textBlock); 
    }

    
    MPI_Finalize();
}

void create_hash_map()
{
    int index;
    int error;
    map_t mymap;
    char key_string[KEY_MAX_LENGTH];
    data_struct_t* value;
    
    mymap = hashmap_new();
    /* First, populate the hash map with ascending values */
    for (index=0; index<KEY_COUNT; index+=1)
    {
        /* Store the key string along side the numerical value so we can free it later */
        value = malloc(sizeof(data_struct_t));
        snprintf(value->key_string, KEY_MAX_LENGTH, "%s%d", KEY_PREFIX, index);
        value->number = index;
        error = hashmap_put(mymap, value->key_string, value);
        snprintf(key_string, KEY_MAX_LENGTH, "%s%d", KEY_PREFIX, index);
        hashmap_get(mymap, key_string, (void**)(&value));
        assert(error==MAP_OK);
    }

    /* Now, check all of the expected values are there */
    for (index=0; index<KEY_COUNT; index+=1)
    {
        snprintf(key_string, KEY_MAX_LENGTH, "%s%d", KEY_PREFIX, index);

        error = hashmap_get(mymap, key_string, (void**)(&value));
        
        /* Make sure the value was both found and the correct number */
        assert(error==MAP_OK);
        assert(value->number==index);
    }
    
    /* Make sure that a value that wasn't in the map can't be found */
    snprintf(key_string, KEY_MAX_LENGTH, "%s%d", KEY_PREFIX, KEY_COUNT);

    error = hashmap_get(mymap, key_string, (void**)(&value));
        
    /* Make sure the value was not found */
    assert(error==MAP_MISSING);

    /* Free all of the values we allocated and remove them from the map */
    for (index=0; index<KEY_COUNT; index+=1)
    {
        snprintf(key_string, KEY_MAX_LENGTH, "%s%d", KEY_PREFIX, index);

        error = hashmap_get(mymap, key_string, (void**)(&value));
        assert(error==MAP_OK);

        error = hashmap_remove(mymap, key_string);
        assert(error==MAP_OK);

        free(value);        
    }
    
    /* Now, destroy the map */
    hashmap_free(mymap);

}

void replace()
{
    int i = 0;
    if(config.textBlock[i] == ' ' || config.textBlock[i] == '\n' || config.textBlock[i] == '\0')
    {
        config.textBlock++;
    }
    i = config.blockSize - 1;
    if(config.textBlock[i] == ' ' || config.textBlock[i] == '\n' || config.textBlock[i] == '\0')
    {
        &config.blockSize = config.blockSize - 1;
        printf("apa\n");
    }
    /*while(1)
    {
        if(i == config.blockSize) break;
        if(config.textBlock[i] == ' ' || config.textBlock[i] == '\n' || config.textBlock[i] == '\0')
        {
            config.textBlock[i] = ' ';
        }
        i = i + 1;
    } */
}

int word_length(int position)
{
    int i = 0;
    //printf("apa");
    while(1)
    {
        i = i + 1;
        if(config.textBlock[position + i] == '\0')
        {
            //printf("%d\n", i);
            return i+1;
        }
    }
}

void dispurse_data()
{
    int i;
    for(i = 1; i < config.world_size; i++)
    {
        MPI_Isend(&(config.text[i*config.blockSize]), config.blockSize, 
        MPI_CHAR, i, 0, MPI_COMM_WORLD, &config.request);
    }
    config.textBlock = (char*)malloc ((config.blockSize)*sizeof(char));
    for(i = 0; i < config.blockSize; i++)
    {
        config.textBlock[i] = config.text[i];
    }
    //printf("%c\n", config.text[config.blockSize - 1]);
}

void load_file()
{
    config.text = 0;
    FILE * f;
    f = fopen ("apa.txt", "rb"); //was "rb"
   
    if (f)
    {
      fseek (f, 0, SEEK_END);
      config.textSize = ftell (f);
      config.blockSize = (config.textSize + 1) / config.world_size;
      fseek (f, 0, SEEK_SET);
      config.text = (char*)malloc ((config.textSize+1)*sizeof(char));
      if (config.text)
      {
        fread (config.text, sizeof(char), config.textSize, f);
      }
      fclose (f);
    }
    config.text[config.textSize] = '\0';
  /*  for (int i = 0; i < config.textSize; i++) {
         printf("buffer[%d] == %c\n", i, config.text[i]);
     }
    printf("buffer = %s\n", config.text); */
} 
