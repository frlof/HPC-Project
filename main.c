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
};
struct Config config;

typedef struct entry_s {
    char* key;
    int count;
    int wordLength;
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
int hashmap_add(hashtable_t* hashmap, char* key, int wordLength);
int hashmap_get(hashtable_t* hashmap, char* key);
void printHashmap(hashtable_t* hashmap, int myHashPart);

int skipChar(char character);



hashtable_t* create_hash_map(int size){
    hashtable_t* hashtable = malloc(sizeof(hashtable_t));
    hashtable->size = size;
    hashtable->table = malloc(hashtable->size * sizeof(entry_t*));
    for(int i = 0; i < size; i++){
        hashtable->table[i] = NULL;
    }
    return hashtable;
}

int hashmap_add(hashtable_t* hashmap, char* key, int wordLength){
    unsigned long hashval = hashVal(key);
    int index = hashval % hashmap->size;
    //hashval = 10;
    int newValue = 1;
    if(hashmap->table[index] != NULL){
        entry_t* temp = hashmap->table[index];
        while(1){
            if(strcmp(temp->key, key) == 0){
                temp->count = temp->count + 1;
                newValue = 0;
                break;
            }
            if(temp->next == NULL){
                entry_t* entry = malloc(sizeof(entry_t));
                entry->key = key;//unsafe operation
                //entry->key = strdup(key);//safe operation
                entry->count = 1;
                entry->next = NULL;
                entry->wordLength = wordLength;
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
        entry->wordLength = wordLength;
        hashmap->table[index] = entry;
    }
    if(newValue) return index;
    else return -1;
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

void printHashmap(hashtable_t* hashmap, int myHashPart){

    printf("[%d][ ", config.world_rank);
    int first = 1;
    for(int i = 0; i < hashmap->size; i++){
        if(myHashPart){
            if(i % config.world_size != config.world_rank){
                continue;
            }
        }
        if(hashmap->table[i] == NULL){
            continue;
        }
        
        entry_t* temp = hashmap->table[i];
        if(!first) {
            printf(", ", temp->key, temp->count);
        }
        first = 0;
        while(1){       
            printf("{key:\"%s\" count:%d}", temp->key, temp->count);
            if(temp->next == NULL){
                break;
            }
            printf(", ", temp->key, temp->count);
            temp = temp->next;
        }
    }
    printf(" ]\n");
}


int main(int argc, char **argv){
    /*hashtable_t* hashmap = create_hash_map(500);

    char pirre[3] = "apa";

    printf("string: %s\n", pirre);
    hashmap_add(hashmap, pirre) ;

    printf("string: %s\n", pirre);
    

    hashmap_add(hashmap, "kalle");
    hashmap_add(hashmap, "kalle");
    hashmap_add(hashmap, "sill");

    printHashmap(hashmap);

    int count = hashmap_get(hashmap, "apa");
    printf("count apa: %d\n", count);
    

    count = hashmap_get(hashmap, "kalle");
    printf("count kalle: %d\n", count);

    count = hashmap_get(hashmap, "sill");
    printf("count sill: %d\n", count);

    count = hashmap_get(hashmap, "sipponen");
    printf("count sipponen: %d\n", count);

    printf("size: %d\n", hashmap->size);

    unsigned long test = hashVal("struts");
    printf("hashValue: %d\n", test);*/

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
        config.textBlock[config.textBlockSize-1] = '\0';
    }

    sleep(config.world_rank);

    /*printf("[%d] blockSize: %d\n", config.world_rank, config.textBlockSize);
    for(int i = 0; i < config.textBlockSize; i++) printf("%c", config.textBlock[i]);
    printf("\n");*/

    hashtable_t* hashmap = create_hash_map(1000);
    int pointer = 0;
    int desintationCount[config.world_size];
    
    for(int i = 0; i < config.world_size; i++) desintationCount[i] = 0;

    while(1){
        for(int i = pointer; i < config.textBlockSize; i++){
            if(skipChar(config.textBlock[i])) pointer++;
            else break;
        }
        if(pointer > config.textBlockSize-1) break;
        
        int wordLength = 0;
        for(int i = pointer; i < config.textBlockSize; i++){
            if(skipChar(config.textBlock[i])){
                config.textBlock[i] = '\0';
                wordLength++;
                break;
            }else wordLength++;
        }
        //printf("addedStrings: %s\n", &(config.textBlock[pointer]));
        int index = hashmap_add(hashmap, &(config.textBlock[pointer]), wordLength);
        if(index != -1){
            index = index % config.world_size;
            desintationCount[index]++;
        }
        pointer += wordLength;   
    }

    printHashmap(hashmap, 0);
    
    
    //sending the number of words to expect
    for(int i = 0; i < config.world_size; i++){
        if(i != config.world_rank){
            //printf("[%d] sending: %d\n", config.world_rank, desintationCount[i]);
            MPI_Isend(&(desintationCount[i]), 1, MPI_INT, i, 0, MPI_COMM_WORLD, &config.request);
        } 
    }


    int currentDesintationTag[config.world_size];
    for(int i = 0; i < config.world_size; i++){
        currentDesintationTag[i] = 1;
    }
    for(int i = 0; i < hashmap->size; i++){
        int dest = i % config.world_size;
        if(dest != config.world_rank){
            if(hashmap->table[i] != NULL){
                entry_t* temp = hashmap->table[i];
                while(1){
                    MPI_Isend(&temp->wordLength, 1, MPI_INT, dest, currentDesintationTag[dest], MPI_COMM_WORLD, &config.request);
                    currentDesintationTag[dest]++;
                    MPI_Isend(temp->key, temp->wordLength, MPI_CHAR, dest, currentDesintationTag[dest], MPI_COMM_WORLD, &config.request);
                    currentDesintationTag[dest]++;
                    MPI_Isend(&temp->count, 1, MPI_INT, dest, currentDesintationTag[dest], MPI_COMM_WORLD, &config.request);
                    currentDesintationTag[dest]++;
                    if(temp->next == NULL){
                        break;
                    }
                    temp = temp->next;
                }
            }
        }
    }

    for(int i = 0; i < config.world_size; i++){
        if(i != config.world_rank){
            int sourceCount;
            MPI_Recv(&sourceCount, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            //printf("[%d] %d\n", config.world_rank, sourceCount);
            int tag = 1;
            for(int j = 0; j < sourceCount; j++){
                int wordLength;
                MPI_Recv(&wordLength, 1, MPI_INT, i, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                tag++;
                char* temp = (char*)malloc ((wordLength)*sizeof(char));
                MPI_Recv(temp, wordLength, MPI_CHAR, i, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                tag++;
                int count;
                MPI_Recv(&count, 1, MPI_INT, i, tag, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                //printf("[%d]{key:\"%s\" count:%d}\n", config.world_rank, s, count);
                tag++;
                hashmap_add(hashmap, temp, wordLength);
            }
        }
    }
    printHashmap(hashmap, 1);

    //MPI_Recv(&config.textBlockSize, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
    //config.textBlock = (char*)malloc ((config.textBlockSize)*sizeof(char));
    //MPI_Recv(config.textBlock, config.textBlockSize, MPI_INT, 0, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);


    //MPI_Isend(&(blockLengths[i]), 1, MPI_INT, i, 0, MPI_COMM_WORLD, &config.request);
    //MPI_Isend(&(config.text[filePointerIndex]), blockLengths[i], MPI_CHAR, i, 1, MPI_COMM_WORLD, &config.request);

    //MPI_Barrier(MPI_COMM_WORLD);

    MPI_Finalize();
}
int skipChar(char character){
    if(0 <= character && character <= 32){
        return 1;
    }else{
        return 0;
    }
}
void load_file(){
    MPI_File_open(MPI_COMM_SELF, config.file, MPI_MODE_RDONLY, MPI_INFO_NULL, &config.dataFile);
    MPI_File_get_size(config.dataFile, &config.dataFileSize);
    config.textSize = (int) config.dataFileSize;
    config.text = (char*)malloc ((config.dataFileSize + 1)*sizeof(char));
    MPI_File_read_all(config.dataFile, config.text, config.dataFileSize, MPI_CHAR, MPI_STATUS_IGNORE);
    MPI_File_close(&config.dataFile);
    config.textSize = config.textSize + 1;
    config.text[config.textSize - 1] = '\0';
}
void distribute_data(){
    int filePointerIndex = 0;
    int preferredBlockSize = config.dataFileSize / config.world_size + 1;
    int* blockLengths = (int*)malloc ((config.world_size)*sizeof(int));
    
    blockLengths[0] = preferredBlockSize - jump_back(filePointerIndex + preferredBlockSize) + 1;
    filePointerIndex += blockLengths[0];
    config.textBlockSize = blockLengths[0];
    config.textBlock = config.text;

    config.text[config.textBlockSize-1] = '\0';//for the block to work on thread 0 without copying the original text

    for(int i = 1; i < config.world_size; i++){
        if(i != config.world_size - 1){
            blockLengths[i] = preferredBlockSize - jump_back(filePointerIndex + preferredBlockSize) + 1;

            MPI_Isend(&(blockLengths[i]), 1, MPI_INT, i, 0, MPI_COMM_WORLD, &config.request);
            MPI_Isend(&(config.text[filePointerIndex]), blockLengths[i], MPI_CHAR, i, 1, MPI_COMM_WORLD, &config.request);
        } else{
            blockLengths[i] = config.textSize - filePointerIndex;
            MPI_Isend(&(blockLengths[i]), 1, MPI_INT, i, 0, MPI_COMM_WORLD, &config.request);
            MPI_Isend(&(config.text[filePointerIndex]), blockLengths[i], MPI_CHAR, i, 1, MPI_COMM_WORLD, &config.request);
        }
        filePointerIndex += blockLengths[i];
    }
}
int jump_back(int position){
    int i = 0;
    while(1){
        if(skipChar(config.text[position - i])) return i;
        i++;
    }
}