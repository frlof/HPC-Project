#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <unistd.h>
#include <math.h>
#include <time.h>

struct Config 
{
    int world_rank;
    int world_size;

    char* file;
	MPI_File dataFile;
    MPI_Offset dataFileSize;
    //char* text;
    unsigned long textSize;

    unsigned long pointer;
    int preferredBlockSize;

    char* textBlock;
    int textBlockSize;
    
    MPI_Request request;
};
struct Config config;

typedef struct entry_s {
    char* key;
    unsigned long count;
    int wordLength;
    struct entry_s* next;
} entry_t;

typedef struct hashtable_s {
	int size;
	entry_t **table;	
} hashtable_t;

//void read_input();
void init_read();
void read_input_scatter();
void sendDone();
int jump_back(int position);

unsigned long hashVal(unsigned char *str);
unsigned long hashmap_add(hashtable_t* hashmap, char* key, int wordLength, unsigned long value);
unsigned long hashmap_get(hashtable_t* hashmap, char* key);
void printHashmap(hashtable_t* hashmap, int myHashPart);

void scatter_tasks();

int skipChar(char character);



hashtable_t* create_hash_map(unsigned long size){
    hashtable_t* hashtable = malloc(sizeof(hashtable_t));
    hashtable->size = size;
    hashtable->table = malloc(hashtable->size * sizeof(entry_t*));
    int i;
    for(i = 0; i < size; i++){
        hashtable->table[i] = NULL;
    }
    return hashtable;
}

unsigned long hashmap_add(hashtable_t* hashmap, char* key, int wordLength, unsigned long value){
    unsigned long hashval = hashVal(key);
    int index = hashval % hashmap->size;
    //hashval = 10;
    int newValue = 1;
    if(hashmap->table[index] != NULL){
        entry_t* temp = hashmap->table[index];
        while(1){
            if(strcmp(temp->key, key) == 0){
                //temp->count = temp->count + 1;
                temp->count = temp->count + value;
                newValue = 0;
                break;
            }
            if(temp->next == NULL){
                entry_t* entry = malloc(sizeof(entry_t));
                //entry->key = key;//unsafe operation
                entry->key = strdup(key);//safe operation
                //entry->count = 1;
                entry->count = value;
                entry->next = NULL;
                entry->wordLength = wordLength;
                temp->next = entry;
                break;
            }
            temp = temp->next;
        }
    }else{
        entry_t* entry = malloc(sizeof(entry_t));
        //entry->key = key;//unsafe operation
        entry->key = strdup(key);//safe operation
        //entry->count = 1;
        entry->count = value;
        entry->next = NULL;
        entry->wordLength = wordLength;
        hashmap->table[index] = entry;
    }
    if(newValue) return index;
    else return -1;
}

int hashmap_set(hashtable_t* hashmap, char* key, int wordLength, unsigned long value){
    unsigned long hashval = hashVal(key);
    int index = hashval % hashmap->size;
    //hashval = 10;
    int newValue = 1;
    if(hashmap->table[index] != NULL){
        entry_t* temp = hashmap->table[index];
        while(1){
            if(strcmp(temp->key, key) == 0){
                //temp->count = temp->count + 1;
                temp->count = value;
                newValue = 0;
                break;
            }
            if(temp->next == NULL){
                entry_t* entry = malloc(sizeof(entry_t));
                //entry->key = key;//unsafe operation
                entry->key = strdup(key);//safe operation
                //entry->count = 1;
                entry->count = value;
                entry->next = NULL;
                entry->wordLength = wordLength;
                temp->next = entry;
                break;
            }
            temp = temp->next;
        }
    }else{
        entry_t* entry = malloc(sizeof(entry_t));
        //entry->key = key;//unsafe operation
        entry->key = strdup(key);//safe operation
        //entry->count = 1;
        entry->count = value;
        entry->next = NULL;
        entry->wordLength = wordLength;
        hashmap->table[index] = entry;
    }
    if(newValue) return index;
    else return -1;
}

unsigned long hashmap_get(hashtable_t* hashmap, char* key){
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
    int i;
    for(i = 0; i < hashmap->size; i++){
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


void map(hashtable_t* hashmap, int *desintationCount){
    int pointer = 0;
    int i;
    //for(i = 0; i < config.world_size; i++) desintationCount[i] = 0;
    //printf("KALLE %d\n", config.textBlockSize);
    while(1){
        //printf("pointer:%d  blockSize:%d\n", pointer , config.textBlockSize);
        //sleep(config.world_rank);
        //printf("KALLE\n");
        //printf("struts\n");
        for(i = pointer; i < config.textBlockSize; i++){
            //printf("%c\n", config.textBlock[i]);
            if(skipChar(config.textBlock[i])) pointer++;
            else break;
        }
        if(pointer >= config.textBlockSize-1){
            
            break;
        } 
        //printf("struts pointer: %d\n", pointer);
        int wordLength = 0;
        for(i = pointer; i < config.textBlockSize; i++){
            if(skipChar(config.textBlock[i])){
                config.textBlock[i] = '\0';
                wordLength++;
                break;
            }else wordLength++;
        }
        //printf("addedStrings: %s\n", &(config.textBlock[pointer]));
        int index = hashmap_add(hashmap, &(config.textBlock[pointer]), wordLength, 1);
        if(index != -1){
            index = index % config.world_size;
            desintationCount[index]++;
        }
        pointer += wordLength;   
    }
}

void redistribute_key_values(hashtable_t* hashmap, int *desintationCount){
    int i;
    //sending the number of words to expect
    for(i = 0; i < config.world_size; i++){
        if(i != config.world_rank){
            //printf("[%d] sending: %d\n", config.world_rank, desintationCount[i]);
            MPI_Isend(&(desintationCount[i]), 1, MPI_INT, i, 0, MPI_COMM_WORLD, &config.request);
            //MPI_Wait(&config.request, MPI_STATUS_IGNORE);
        } 
    }

    for(i = 0; i < hashmap->size; i++){
        int dest = i % config.world_size;
        if(dest != config.world_rank){
            if(hashmap->table[i] != NULL){
                entry_t* temp = hashmap->table[i];
                while(1){
                    MPI_Isend(&temp->wordLength, 1, MPI_INT, dest, 0, MPI_COMM_WORLD, &config.request);
                    //MPI_Wait(&config.request, MPI_STATUS_IGNORE);
                    MPI_Isend(temp->key, temp->wordLength, MPI_CHAR, dest, 0, MPI_COMM_WORLD, &config.request);
                    //MPI_Wait(&config.request, MPI_STATUS_IGNORE);
                    MPI_Isend(&temp->count, 1, MPI_UNSIGNED_LONG, dest, 0, MPI_COMM_WORLD, &config.request);
                    //MPI_Wait(&config.request, MPI_STATUS_IGNORE);
                    if(temp->next == NULL){
                        break;
                    }
                    temp = temp->next;
                }
            }
        }
    }
}


void reduce(hashtable_t* hashmap, int *desintationCount){
    int i;
    for(i = 0; i < config.world_size; i++){
        if(i != config.world_rank){
            int sourceCount;
            MPI_Recv(&sourceCount, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            int j;
            for(j = 0; j < sourceCount; j++){
                int wordLength;
                MPI_Recv(&wordLength, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                char temp[wordLength];
                MPI_Recv(temp, wordLength, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                unsigned long count;
                MPI_Recv(&count, 1, MPI_UNSIGNED_LONG, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                int index = hashmap_add(hashmap, temp, wordLength, count);
                if(index != -1){
                    index = index % config.world_size;
                    desintationCount[index]++;
                }

            }
        }
    }
}




// void redistribute_key_values_reduce(hashtable_t* hashmap, int *desintationCount){

//     int i;
//     //sending the number of words to expect
//     for(i = 0; i < config.world_size; i++){
//         if(i != config.world_rank){
//             //printf("[%d] sending: %d\n", config.world_rank, desintationCount[i]);
//             MPI_Isend(&(desintationCount[i]), 1, MPI_INT, i, 0, MPI_COMM_WORLD, &config.request);
//             //MPI_Wait(&config.request, MPI_STATUS_IGNORE);
//         } 
//     }
//     int messages = 0;
//     for(i = 0; i < config.world_size; i++){
//         if(i != config.world_rank){
//             int sourceCount;
//             MPI_Recv(&sourceCount, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
//             messages += sourceCount;
//             //printf("[%d] sourceCount: %d\n", config.world_rank, sourceCount);
//             //printf("[%d] MessageSize: %d\n", config.world_rank, messages);
//         }
//     }

//     for(i = 0; i < hashmap->size; i++){
//         int dest = i % config.world_size;
//         if(dest != config.world_rank){
//             if(hashmap->table[i] != NULL){
//                 entry_t* temp = hashmap->table[i];
//                 while(1){
                    

//                     MPI_Isend(&temp->wordLength, 1, MPI_INT, dest, 0, MPI_COMM_WORLD, &config.request);
//                     //MPI_Wait(&config.request, MPI_STATUS_IGNORE);
//                     MPI_Isend(temp->key, temp->wordLength, MPI_CHAR, dest, 0, MPI_COMM_WORLD, &config.request);
//                     //MPI_Wait(&config.request, MPI_STATUS_IGNORE);
//                     MPI_Isend(&temp->count, 1, MPI_UNSIGNED_LONG, dest, 0, MPI_COMM_WORLD, &config.request);
//                     //MPI_Wait(&config.request, MPI_STATUS_IGNORE);
//                     if(temp->next == NULL){
//                         break;
//                     }
//                     temp = temp->next;
//                 }
//             }
//         }
//     }

//     int receivedMessages = 0;
//     MPI_Status status;
//     int wordLength;
//     int waitingForMessage = 0;
//     while(receivedMessages < messages){
//         if(!waitingForMessage){
//             MPI_Irecv(&wordLength, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &config.request);
//             waitingForMessage = 1;
//         }
//         int flag;
//         MPI_Test(&config.request, &flag, &status);
//         if(flag){
//             char temp[wordLength];
//             MPI_Recv(temp, wordLength, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

//             unsigned long count;
//             MPI_Recv(&count, 1, MPI_UNSIGNED_LONG, status.MPI_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

//             int index = hashmap_add(hashmap, temp, wordLength, count);
//             if(index != -1){
//                 index = index % config.world_size;
//                 desintationCount[index]++;
//             }
//             //MPI_Irecv(&wordLength, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &config.request);
//             waitingForMessage = 0;
//             receivedMessages++;
//         }
//     }

//     /*for(i = 0; i < messages; i++){
//         MPI_Status status;
//         int wordLength;
//         //MPI_Recv(&wordLength, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
//         MPI_Irecv(&wordLength, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &config.request);
        
//         //MPI_Wait(&config.request, &status);
//         int flag = 0;
//         while(!flag){
//             MPI_Test(&config.request, &flag, &status);
//         }

//         char temp[wordLength];
//         MPI_Recv(temp, wordLength, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

//         unsigned long count;
//         MPI_Recv(&count, 1, MPI_UNSIGNED_LONG, status.MPI_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

//         int index = hashmap_add(hashmap, temp, wordLength, count);
//         if(index != -1){
//             index = index % config.world_size;
//             desintationCount[index]++;
//         }
//     }*/
// }

void redistribute_key_values_reduce(hashtable_t* hashmap, int *desintationCount){

    int i;
    //sending the number of words to expect
    for(i = 0; i < config.world_size; i++){
        if(i != config.world_rank){
            //printf("[%d] sending: %d\n", config.world_rank, desintationCount[i]);
            MPI_Isend(&(desintationCount[i]), 1, MPI_INT, i, 0, MPI_COMM_WORLD, &config.request);
            //MPI_Wait(&config.request, MPI_STATUS_IGNORE);
        } 
    }
    int messages = 0;
    for(i = 0; i < config.world_size; i++){
        if(i != config.world_rank){
            int sourceCount;
            MPI_Recv(&sourceCount, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            messages += sourceCount;
            //printf("[%d] sourceCount: %d\n", config.world_rank, sourceCount);
            //printf("[%d] MessageSize: %d\n", config.world_rank, messages);
        }
    }

    int receivedMessages = 0;
    MPI_Status status;
    int wordLength;
    int waitingForMessage = 0;
    MPI_Request request;
    for(i = 0; i < hashmap->size; i++){

        while(receivedMessages < messages){
            if(!waitingForMessage){
                MPI_Irecv(&wordLength, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &config.request);
                waitingForMessage = 1;
            }
            int flag;
            MPI_Test(&config.request, &flag, &status);
            if(flag){
                char temp[wordLength];
                MPI_Recv(temp, wordLength, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                unsigned long count;
                MPI_Recv(&count, 1, MPI_UNSIGNED_LONG, status.MPI_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                int index = hashmap_add(hashmap, temp, wordLength, count);
                if(index != -1){
                    index = index % config.world_size;
                    desintationCount[index]++;
                }
                waitingForMessage = 0;
                receivedMessages++;
                continue;
            }else{
                break;
            }
        }



        int dest = i % config.world_size;
        if(dest != config.world_rank){
            if(hashmap->table[i] != NULL){
                entry_t* temp = hashmap->table[i];
                while(1){

                    if(receivedMessages < messages){
                        if(!waitingForMessage){
                            MPI_Irecv(&wordLength, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &config.request);
                            waitingForMessage = 1;
                        }
                        int flag;
                        MPI_Test(&config.request, &flag, &status);
                        if(flag){
                            char temp[wordLength];
                            MPI_Recv(temp, wordLength, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                            unsigned long count;
                            MPI_Recv(&count, 1, MPI_UNSIGNED_LONG, status.MPI_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

                            int index = hashmap_add(hashmap, temp, wordLength, count);
                            if(index != -1){
                                index = index % config.world_size;
                                desintationCount[index]++;
                            }
                            //MPI_Irecv(&wordLength, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &config.request);
                            waitingForMessage = 0;
                            receivedMessages++;
                            continue;
                        }
                    }
                    

                    MPI_Isend(&temp->wordLength, 1, MPI_INT, dest, 0, MPI_COMM_WORLD, &request);
                    //MPI_Wait(&config.request, MPI_STATUS_IGNORE);
                    MPI_Isend(temp->key, temp->wordLength, MPI_CHAR, dest, 0, MPI_COMM_WORLD, &request);
                    //MPI_Wait(&config.request, MPI_STATUS_IGNORE);
                    MPI_Isend(&temp->count, 1, MPI_UNSIGNED_LONG, dest, 0, MPI_COMM_WORLD, &request);
                    //printf("sending %s\n", temp->key);
                    //MPI_Wait(&config.request, MPI_STATUS_IGNORE);
                    if(temp->next == NULL){
                        break;
                    }
                    temp = temp->next;
                }
            }
        }
    }
    //printf("[%d]apa1\n", config.world_rank);
    /*if(waitingForMessage){
        MPI_Wait(&config.request, MPI_STATUS_IGNORE);
        char temp[wordLength];
        MPI_Recv(temp, wordLength, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        unsigned long count;
        MPI_Recv(&count, 1, MPI_UNSIGNED_LONG, status.MPI_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        int index = hashmap_add(hashmap, temp, wordLength, count);
        if(index != -1){
            index = index % config.world_size;
            desintationCount[index]++;
        }
        //MPI_Irecv(&wordLength, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &config.request);
        waitingForMessage = 0;
        receivedMessages++;
    }
    //printf("[%d]apa2\n", config.world_rank);
    for(i = receivedMessages; i < messages; i++){

        MPI_Recv(&wordLength, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);

        char temp[wordLength];
        MPI_Recv(temp, wordLength, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        unsigned long count;
        MPI_Recv(&count, 1, MPI_UNSIGNED_LONG, status.MPI_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        int index = hashmap_add(hashmap, temp, wordLength, count);
        if(index != -1){
            index = index % config.world_size;
            desintationCount[index]++;
        }
        //MPI_Irecv(&wordLength, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &config.request);
        waitingForMessage = 0;
        receivedMessages++;
    }
    printf("[%d]apa3\n", config.world_rank);*/
    
    while(receivedMessages < messages){
        if(!waitingForMessage){
            MPI_Irecv(&wordLength, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &config.request);
            waitingForMessage = 1;
        }
        int flag;
        MPI_Test(&config.request, &flag, &status);
        if(flag){
            char temp[wordLength];
            MPI_Recv(temp, wordLength, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            unsigned long count;
            MPI_Recv(&count, 1, MPI_UNSIGNED_LONG, status.MPI_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

            int index = hashmap_add(hashmap, temp, wordLength, count);
            if(index != -1){
                index = index % config.world_size;
                desintationCount[index]++;
            }
            //MPI_Irecv(&wordLength, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &config.request);
            waitingForMessage = 0;
            receivedMessages++;
        }
    }

}

void gather_result(hashtable_t* hashmap, int *desintationCount){
    if(config.world_rank == 0){

        int messages = 0;
        int i = 0;
        for(i = 0; i < config.world_size; i++){
            if(i != config.world_rank){
                int sourceCount;
                MPI_Recv(&sourceCount, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                messages += sourceCount;
                //printf("[%d] sourceCount: %d\n", config.world_rank, sourceCount);
                //printf("[%d] MessageSize: %d\n", config.world_rank, messages);
            }
        }

        int receivedMessages = 0;
        MPI_Status status;
        int wordLength;
        int waitingForMessage = 0;
        MPI_Request request;
        
        for(i = 0; i < messages; i++){
            MPI_Recv(&wordLength, 1, MPI_INT, MPI_ANY_SOURCE, 0, MPI_COMM_WORLD, &status);
            char temp[wordLength];
            MPI_Recv(temp, wordLength, MPI_CHAR, status.MPI_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            unsigned long count;
            MPI_Recv(&count, 1, MPI_UNSIGNED_LONG, status.MPI_SOURCE, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            hashmap_set(hashmap, temp, wordLength, count);
        }


        /*int i;
        for(i = 0; i < config.world_size; i++){
            if(i != config.world_rank){
                int sourceCount;
                MPI_Recv(&sourceCount, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                //desintationCount[index] += sourceCount
                //printf("[%d] %d\n", config.world_rank, sourceCount);
                //int tag = 1;
                int j;
                for(j = 0; j < sourceCount; j++){
                    int wordLength;
                    MPI_Recv(&wordLength, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    //tag++;
                    //char* temp = (char*)malloc ((wordLength)*sizeof(char));
                    char temp[wordLength];
                    MPI_Recv(temp, wordLength, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    
                    //tag++;
                    unsigned long count;
                    MPI_Recv(&count, 1, MPI_UNSIGNED_LONG, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    //printf("[%d]{key:\"%s\" count:%d}\n", config.world_rank, temp, count);
                    //tag++;
                    int index = hashmap_set(hashmap, temp, wordLength, count);
                    //int index = hashmap_add(hashmap, &(config.textBlock[pointer]), wordLength, 1);
                    if(index != -1){
                        index = index % config.world_size;
                        desintationCount[index]++;
                    }

                }
            }
        }
        //printf("KALLE\n");
        //printHashmap(hashmap, 0);*/
    } else{
        MPI_Isend(&(desintationCount[config.world_rank]), 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &config.request);
        //MPI_Wait(&config.request, MPI_STATUS_IGNORE);
        //int destTag = 1;
        int i;
        for(i = 0; i < hashmap->size; i++){
            int source = i % config.world_size;
            if(source == config.world_rank){
                if(hashmap->table[i] != NULL){
                    entry_t* temp = hashmap->table[i];
                    while(1){
                        MPI_Isend(&temp->wordLength, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &config.request);
                        MPI_Wait(&config.request, MPI_STATUS_IGNORE);
                        //destTag++;
                        MPI_Isend(temp->key, temp->wordLength, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &config.request);
                        MPI_Wait(&config.request, MPI_STATUS_IGNORE);
                        //destTag++;
                        MPI_Isend(&temp->count, 1, MPI_UNSIGNED_LONG, 0, 0, MPI_COMM_WORLD, &config.request);
                        MPI_Wait(&config.request, MPI_STATUS_IGNORE);

                        //destTag++;
                        if(temp->next == NULL){
                            break;
                        }
                        entry_t* next = temp->next;
                        free(temp->key);
                        free(temp);
                        temp = next;
                        //temp = temp->next;

                    }
                }
            }
        }
    }
    //printHashmap(hashmap, 1);
}

/*void gather_result(hashtable_t* hashmap, int *desintationCount){
    if(config.world_rank == 0){
        int i;
        for(i = 0; i < config.world_size; i++){
            if(i != config.world_rank){
                int sourceCount;
                MPI_Recv(&sourceCount, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                //desintationCount[index] += sourceCount
                //printf("[%d] %d\n", config.world_rank, sourceCount);
                //int tag = 1;
                int j;
                for(j = 0; j < sourceCount; j++){
                    int wordLength;
                    MPI_Recv(&wordLength, 1, MPI_INT, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    //tag++;
                    //char* temp = (char*)malloc ((wordLength)*sizeof(char));
                    char temp[wordLength];
                    MPI_Recv(temp, wordLength, MPI_CHAR, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    
                    //tag++;
                    unsigned long count;
                    MPI_Recv(&count, 1, MPI_UNSIGNED_LONG, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                    //printf("[%d]{key:\"%s\" count:%d}\n", config.world_rank, temp, count);
                    //tag++;
                    int index = hashmap_set(hashmap, temp, wordLength, count);
                    //int index = hashmap_add(hashmap, &(config.textBlock[pointer]), wordLength, 1);
                    if(index != -1){
                        index = index % config.world_size;
                        desintationCount[index]++;
                    }

                }
            }
        }
        //printf("KALLE\n");
        //printHashmap(hashmap, 0);
    } else{
        MPI_Isend(&(desintationCount[config.world_rank]), 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &config.request);
        //MPI_Wait(&config.request, MPI_STATUS_IGNORE);
        //int destTag = 1;
        int i;
        for(i = 0; i < hashmap->size; i++){
            int source = i % config.world_size;
            if(source == config.world_rank){
                if(hashmap->table[i] != NULL){
                    entry_t* temp = hashmap->table[i];
                    while(1){
                        MPI_Isend(&temp->wordLength, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, &config.request);
                        MPI_Wait(&config.request, MPI_STATUS_IGNORE);
                        //destTag++;
                        MPI_Isend(temp->key, temp->wordLength, MPI_CHAR, 0, 0, MPI_COMM_WORLD, &config.request);
                        MPI_Wait(&config.request, MPI_STATUS_IGNORE);
                        //destTag++;
                        MPI_Isend(&temp->count, 1, MPI_UNSIGNED_LONG, 0, 0, MPI_COMM_WORLD, &config.request);
                        MPI_Wait(&config.request, MPI_STATUS_IGNORE);

                        //destTag++;
                        if(temp->next == NULL){
                            break;
                        }
                        entry_t* next = temp->next;
                        free(temp->key);
                        free(temp);
                        temp = next;
                        //temp = temp->next;

                    }
                }
            }
        }
    }
    //printHashmap(hashmap, 1);
}*/


int main(int argc, char **argv){
    //tag does not matter if sent in order??

    //printf("%s\n", argv[1]);

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &config.world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &config.world_size);

    config.preferredBlockSize = 64000000;
    hashtable_t* hashmap;// = create_hash_map(2000000);
    if(config.world_rank == 0){
        hashmap = create_hash_map(2000000);
    }else{
        hashmap = create_hash_map(2000000);
    }
    config.textBlock = (char*)malloc ((config.preferredBlockSize+1)*sizeof(char));
    config.textBlock[config.preferredBlockSize] = '\0';

    int desintationCount[config.world_size];
    int i;
    for(i = 0; i < config.world_size; i++){
        desintationCount[i] = 0;
    }

    //printf("[%d] pelle: %lu\n", config.world_rank, desintationCount[1]);
    //{key:"may" count:2474472}, {key:"user," count:2474472}, {key:"avoid" count:1237236}
    //{key:"may" count:2474472}, {key:"user," count:2474472}, {key:"avoid" count:1237236}
    clock_t start, end;
    if(config.world_rank == 0){
        
        if(argc < 2){
            config.file = "./data/bigFile5gig";
        }else{
            config.file = argv[1];
        }
        init_read();

        //sleep(10);
        start = clock();
        read_input_scatter();
        //sendDone();

        

        //{key:"may" count:2474472}, {key:"user," count:2474472}, {key:"avoid" count:1237236}

            
            //printf("BALLE\n");
        
        MPI_File_close(&config.dataFile);
    } else {
        //unsigned long desintationCount[config.world_size-1];
        //printf("waiting...\n");
        //sleep(config.world_rank*10);
        //printf("started...\n");
        //int tag = 0;
        while(1){
            
            
            //printf("bako\n");
            MPI_Recv(&config.textBlockSize, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            //printf("SIZE: %d\n", config.textBlockSize);
            //tag++;
            //printf("struts1 %d\n", config.textBlockSize);
            if(config.textBlockSize == 0){
                break;
            }
            //printf("pirre");
            MPI_Recv(config.textBlock, config.textBlockSize, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            config.textBlockSize++;
            config.textBlock[config.textBlockSize-1] = '\0'; 
            //tag++;

            //sleep(config.world_rank);

            /*printf("[%d] blockSize: %d\n", config.world_rank, config.textBlockSize);
            for(int i = 0; i < config.textBlockSize; i++) printf("%c", config.textBlock[i]);
            printf("\n");*/

            //printf("struts2\n");

            map(hashmap, desintationCount);
        }
        
        //config.textBlock[config.textBlockSize-1] = '\0';
    }
    //printf("[%d] passed first part...\n", config.world_rank);
    
    /*sleep(config.world_rank);

    printf("[%d] blockSize: %d\n", config.world_rank, config.textBlockSize);
    for(int i = 0; i < config.textBlockSize; i++) printf("%c", config.textBlock[i]);
    printf("\n");*/
    //printf("kalle\n");
    if(1){
        MPI_Barrier(MPI_COMM_WORLD);
        if(config.world_rank == 0){
            printf("redistribute_key_values_reduce\n");
        }

        redistribute_key_values_reduce(hashmap, desintationCount);
        
        /*redistribute_key_values(hashmap, desintationCount);
        //printf("kalle\n");
        //printHashmap(hashmap, 0);
        MPI_Barrier(MPI_COMM_WORLD);
        if(config.world_rank == 0){
            printf("reduce\n");
        }
        reduce(hashmap, desintationCount);*/
        //printf("kalle\n");
        //printHashmap(hashmap, 1);
        MPI_Barrier(MPI_COMM_WORLD);
        if(config.world_rank == 0){
            printf("gather_result\n");
        }
        gather_result(hashmap, desintationCount);

        //output result
        if(config.world_rank == 0){
            //printHashmap(hashmap, 0);
            end = clock();
            double time = ((double) (end - start)/1000);
            printf("Done in %f milliseconds\n", time);
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);
    MPI_Finalize();
}
int skipChar(char character){
    /*if(0 <= character && character <= 32){
        return 1;
    }else{
        return 0;
    }*/
    if(0 <= character && character <= 47 || 58 <= character && character <= 63 || 91 <= character && character <= 96 ){
        return 1;
    }else{
        return 0;
    }
}
void read_input_scatter(){

    //{key:"user," count:2474471}, {key:"avoid" count:1237236}

    int i;
    //int tags[config.world_size];
    //for(i = 0; i < config.world_size; i++) tags[i] = 0;
    i=0;
    int blockSize;
    //int* blockLengths = (int*)malloc ((config.world_size)*sizeof(int));
    while(config.pointer < config.textSize){
        //int percentage = config.pointer/config.textSize;
        printf("%lu of %lu\n", config.pointer, config.textSize);
        //printf("%d\%\n");  
        //printf("Kalle\n");
        int dest = i % (config.world_size-1) + 1;

        if(config.pointer + config.preferredBlockSize < config.textSize){
            blockSize = config.preferredBlockSize;
            //blockLengths[dest] = config.preferredBlockSize;

        }else{
            blockSize = config.textSize - config.pointer;
            //blockLengths[dest] = config.textSize - config.pointer;
        }
        
        
        MPI_File_read_all(config.dataFile, config.textBlock, blockSize, MPI_CHAR, MPI_STATUS_IGNORE);

        //printf("blockSize: %d\n", blockSize);

        //blockSize = blockSize - jump_back(blockSize-1);
        
        MPI_Isend(&blockSize, 1, MPI_INT, dest, 0, MPI_COMM_WORLD, &config.request);
        MPI_Wait(&config.request, MPI_STATUS_IGNORE);
        //tags[dest]++;

        /*sleep(1);

        printf("%d[%d] blockSize: %d\n", i, config.world_rank, blockLengths[dest]);
        for(int i = 0; i < blockLengths[dest]; i++) printf("%c(%d)", config.textBlock[i], config.textBlock[i]);
        printf("\n");*/

        //{key:"may" count:2474472}, {key:"user," count:2474472}, {key:"avoid" count:1237236}
        //{key:"may" count:2474472}, {key:"user," count:2474472}, {key:"avoid" count:1237236}
        


        //sleep(0.1);
        MPI_Isend(config.textBlock, blockSize, MPI_CHAR, dest, 0, MPI_COMM_WORLD, &config.request);
        MPI_Wait(&config.request, MPI_STATUS_IGNORE);
        //tags[dest]++;
        
        //sleep(0.9);
        config.pointer += blockSize;
        i++;    
    }
    int temp = 0;
    for(i = 1; i < config.world_size; i++){
        MPI_Isend(&temp, 1, MPI_INT, i, 0, MPI_COMM_WORLD, &config.request);
        //MPI_Wait(&config.request, MPI_STATUS_IGNORE);
    }
}


void init_read(){
    MPI_File_open(MPI_COMM_SELF, config.file, MPI_MODE_RDONLY, MPI_INFO_NULL, &config.dataFile);
    MPI_File_get_size(config.dataFile, &config.dataFileSize);
    config.textSize =  config.dataFileSize;
    config.pointer = 0;

}
/*void scatter_tasks(){
    unsigned long filePointerIndex = 0;
    unsigned long preferredBlockSize = config.chunkSize / config.world_size;
    printf("preferredBlockSize: %d\n", preferredBlockSize);
    unsigned long* blockLengths = (unsigned long*)malloc ((config.world_size)*sizeof(unsigned long));
    
    
    blockLengths[0] = preferredBlockSize - jump_back(filePointerIndex + preferredBlockSize);
    filePointerIndex += blockLengths[0];
    config.textBlockSize = blockLengths[0];
    config.textBlock = config.text;

    //config.text[config.textBlockSize-1] = '\0';//for the block to work on thread 0 without copying the original text
    int i;
    for(i = 1; i < config.world_size; i++){
        if(i != config.world_size - 1){
            blockLengths[i] = preferredBlockSize - jump_back(filePointerIndex + preferredBlockSize);

            MPI_Isend(&(blockLengths[i]), 1, MPI_UNSIGNED_LONG, i, 0, MPI_COMM_WORLD, &config.request);
            MPI_Isend(&(config.text[filePointerIndex]), blockLengths[i], MPI_CHAR, i, 1, MPI_COMM_WORLD, &config.request);
        } else{
            blockLengths[i] = config.chunkSize - filePointerIndex;
            MPI_Isend(&(blockLengths[i]), 1, MPI_UNSIGNED_LONG, i, 0, MPI_COMM_WORLD, &config.request);
            MPI_Isend(&(config.text[filePointerIndex]), blockLengths[i], MPI_CHAR, i, 1, MPI_COMM_WORLD, &config.request);
        }
        filePointerIndex += blockLengths[i];
    }
}*/

int jump_back(int position){
    int i = 0;
    while(1){
        //printf("skip? %c\n", config.textBlock[position - i]);
        if(skipChar(config.textBlock[position - i])) return i;
        i++;
    }
}