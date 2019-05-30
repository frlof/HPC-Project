#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <math.h>

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
    //map_t mymap;
};
struct Config config;

typedef struct entry_s {
    char* key;
    int count;
    struct entry_s* next;
} entry_t;

typedef struct hashtable_s {
	int size;
	entry_t **table;	
} hashtable_t;

void load_file();
void load_file_old();
int jump_back(int position);

unsigned long hashVal(unsigned char *str);
void hashmap_add(hashtable_t* hashmap, char* key);
int hashmap_get(hashtable_t* hashmap, char* key);



hashtable_t* create_hash_map(int size){
    hashtable_t* hashtable = malloc(sizeof(hashtable_t));
    hashtable->size = size;
    hashtable->table = malloc(hashtable->size * sizeof(entry_t*));
    for(int i = 0; i < size; i++){
        hashtable->table[i] = NULL;
    }
    return hashtable;
}

void hashmap_add(hashtable_t* hashmap, char* key){
    unsigned long hashval = hashVal(key);
    hashval = hashval % hashmap->size;
    //hashval = 10;
    if(hashmap->table[hashval] != NULL){
        entry_t* temp = hashmap->table[hashval];
        while(1){
            if(strcmp(temp->key, key) == 0){
                temp->count = temp->count + 1;
                break;
            }
            if(temp->next == NULL){
                entry_t* entry = malloc(sizeof(entry_t));
                entry->key = key;//unsafe operation
                //entry->key = strdup(key);//safe operation
                entry->count = 1;
                entry->next = NULL;
                temp->next = entry;
                break;
            }
            temp = temp->next;
        }
    }else{
        entry_t* entry = malloc(sizeof(entry_t));
        entry->key = key;//unsafe operation
        //entry->key = strdup(key);//safe operation
        entry->count = 1;
        entry->next = NULL;
        hashmap->table[hashval] = entry;
    }
}

int hashmap_get(hashtable_t* hashmap, char* key){
    unsigned long hashval = hashVal(key);
    hashval = hashval % hashmap->size;
    //hashval = 10;
    if(hashmap->table[hashval] != NULL){
        entry_t* temp = hashmap->table[hashval];
        while(1){
            if(strcmp(temp->key, key) == 0){
                return temp->count;
            }
            if(temp->next == NULL){
                return 0;
            }
            temp = temp->next;
        }
    }
    return 0;
}

unsigned long hashVal(unsigned char *str){
    unsigned long hash = 5381;
    int c;
    while (c = *str++) hash = ((hash << 5) + hash) + c; /* hash * 33 + c */
    return hash;
}


int main(int argc, char **argv){
    hashtable_t* hashmap = create_hash_map(500);

    char pirre[3] = "apa";

    printf("string: %s\n", pirre);
    hashmap_add(hashmap, pirre) ;

    //printf("char: %c\n", pirre[1]);
    //pirre = "klafskalle";
    pirre[0] = 'b';
    printf("string: %s\n", pirre);
    
    //printf("char: %c\n", pirre[2]);
    //printf("char: %c\n", pirre[3]);

    hashmap_add(hashmap, "kalle");
    hashmap_add(hashmap, "kalle");
    hashmap_add(hashmap, "sill");

    int count = hashmap_get(hashmap, "apa");
    printf("count apa: %d\n", count);
    

    count = hashmap_get(hashmap, "kalle");
    printf("count kalle: %d\n", count);

    count = hashmap_get(hashmap, "sill");
    printf("count sill: %d\n", count);

    count = hashmap_get(hashmap, "sipponen");
    printf("count sipponen: %d\n", count);

    //hashtable->table[0] = malloc(sizeof(entry_t));
    //if(hashtable[0] == NULL){
    //    printf("");
    //}
    printf("pointer: %p\n", hashmap->table[0]);


    
    printf("size: %d\n", hashmap->size);

    unsigned long test = hashVal("struts");
    printf("hashValue: %d\n", test);

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &config.world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &config.world_size);

    if(config.world_rank == 0){
        config.file = "apa.txt";
        load_file();
        /*printf("[%d]\n", config.world_rank);
        for(int i = 0; i < config.textSize; i++) printf("%c", config.text[i]);
        printf("\n");*/
        distribute_data();
    } else {
        MPI_Recv(&config.textBlockSize, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        config.textBlock = (char*)malloc ((config.textBlockSize)*sizeof(char));
        MPI_Recv(config.textBlock, config.textBlockSize, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    }

    sleep(config.world_rank);
    printf("[%d] blockSize: %d\n", config.world_rank, config.textBlockSize);
    for(int i = 0; i < config.textBlockSize; i++) printf("%c", config.textBlock[i]);
    printf("\n");

    MPI_Barrier(MPI_COMM_WORLD);

    /*if(config.world_rank == 0){
        create_hash_map();
    }*/


    //sleep((config.world_rank+1)*1000);
    //MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
}

void load_file(){
    MPI_File_open(MPI_COMM_SELF, config.file, MPI_MODE_RDONLY, MPI_INFO_NULL, &config.dataFile);
    MPI_File_get_size(config.dataFile, &config.dataFileSize);
    config.textSize = (int) config.dataFileSize;
    config.text = (char*)malloc ((config.dataFileSize + 1)*sizeof(char));
    MPI_File_read_all(config.dataFile, config.text, config.dataFileSize, MPI_CHAR, MPI_STATUS_IGNORE);
    MPI_File_close(&config.dataFile);
    config.textSize = config.textSize + 1;
    config.text[config.textSize - 1] = '\n';
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
        if(i != config.world_size - 1){
            blockLengths[i] = preferredBlockSize - jump_back(filePointerIndex + preferredBlockSize);
            MPI_Isend(&(blockLengths[i]), 1, MPI_INT, i, 0, MPI_COMM_WORLD, &config.request);
            MPI_Isend(&(config.text[filePointerIndex]), blockLengths[i], MPI_CHAR, i, 1, MPI_COMM_WORLD, &config.request);
        } else{
            blockLengths[i] = config.textSize - filePointerIndex;
            MPI_Isend(&(blockLengths[i]), 1, MPI_INT, i, 0, MPI_COMM_WORLD, &config.request);
            MPI_Isend(&(config.text[filePointerIndex]), blockLengths[i], MPI_CHAR, i, 1, MPI_COMM_WORLD, &config.request);
        }
        filePointerIndex += blockLengths[i] + 1;
    }
}
int jump_back(int position){
    int i = 0;
    while(1){
        if(config.text[position - i] == ' ' || config.text[position - i] == '\0' || config.text[position - i] == '\n') return i;
        i++;
    }
}