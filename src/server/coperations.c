#include <ctype.h>
#include <stdlib.h>
#include <limits.h>

#include "client.h"
#include "coperations.h"
#include "string.h"
#include "constants.h"
#include "../common/constants.h"

unsigned int chash(const char *key) {
    unsigned long hash = 5381;  // Prime number
    int c;
    
    while ((c = *key++)) {
        hash = ((hash << 5) + hash) + (unsigned char)c;  // hash * 33 + c
    }
    
    return (unsigned int)(hash % (unsigned long)MAX_KEYS_COUNT);
}

struct IntHashTable* create_int_hash_table() {
    IntHashTable *ht = malloc(sizeof(IntHashTable));
    if (!ht) return NULL;
    
    for(int i = 0; i < MAX_KEYS_COUNT; i++) {
        ht->nodes[i] = malloc(sizeof(IntNode));
        ht->nodes[i]->count = 0;
        pthread_rwlock_init(&ht->nodes[i]->nodeLock, NULL);
    }
    pthread_rwlock_init(&ht->tableLock, NULL);
    return ht;
}

void destroy_int_hash_table(IntHashTable *ht) {
    for(int i = 0; i < MAX_KEYS_COUNT; i++) {
        pthread_rwlock_destroy(&ht->nodes[i]->nodeLock);
        free(ht->nodes[i]);
    }
    pthread_rwlock_destroy(&ht->tableLock);
    free(ht);
}

int add_key(IntHashTable *ht, const char *key, int value) {
    unsigned int index = chash(key);

    pthread_rwlock_wrlock(&ht->tableLock);
    IntNode *node = ht->nodes[index];
    pthread_rwlock_unlock(&ht->tableLock);
    
    pthread_rwlock_wrlock(&node->nodeLock);
    // Should never happen
    if (node->count >= MAX_SESSION_COUNT) {
        pthread_rwlock_unlock(&node->nodeLock);
        return 1;
    }
    
    node->values[node->count++] = value;
    pthread_rwlock_unlock(&node->nodeLock);
    return 0;
}

int remove_key(IntHashTable *ht, const char *key, int value) {
    unsigned int index = chash(key);

    pthread_rwlock_wrlock(&ht->tableLock);
    IntNode *node = ht->nodes[index];
    pthread_rwlock_unlock(&ht->tableLock);
    
    pthread_rwlock_wrlock(&node->nodeLock);
    for(int i = 0; i < node->count; i++) {
        if(node->values[i] == value) {
            // Move last element to the position of the removed element
            node->values[i] = node->values[--node->count];
            pthread_rwlock_unlock(&node->nodeLock);
            return 0;
        }
    }
    pthread_rwlock_unlock(&node->nodeLock);
    return 1;
}

int* get_fds(IntHashTable *ht, const char *key, int *count) {
    unsigned int index = chash(key);

    pthread_rwlock_rdlock(&ht->tableLock);
    IntNode *node = ht->nodes[index];
    pthread_rwlock_unlock(&ht->tableLock);

    pthread_rwlock_rdlock(&node->nodeLock);
    int *values = malloc((size_t)node->count * sizeof(int));
    if(!values) {
        pthread_rwlock_unlock(&node->nodeLock);
        return NULL;
    }
    
    for(int i = 0; i < node->count; i++) {
        values[i] = node->values[i];
    }
    
    *count = node->count;
    pthread_rwlock_unlock(&node->nodeLock);
    return values;
}

int subscribe_key_index_list(const char* key, char keys_list[MAX_NUMBER_SUB][MAX_STRING_SIZE+1]) {
    for(int i = 0; i < MAX_NUMBER_SUB; i++) {
        if(strcmp(keys_list[i], key) == 0) {
            return i;
        }
    }
    return -1;
}

int add_key_to_subscribed_list(const char* key, char keys_list[MAX_NUMBER_SUB][MAX_STRING_SIZE+1], int num_keys) {
    // Check if the key already exists
    if (subscribe_key_index_list(key, keys_list) != -1) {
        return 1;
    }
    strncpy(keys_list[num_keys], key, MAX_STRING_SIZE);
    keys_list[num_keys][MAX_STRING_SIZE] = '\0';
    return 0;
}

int remove_key_from_subscribed_list(const char* key, char keys_list[MAX_NUMBER_SUB][MAX_STRING_SIZE+1], int num_keys) {
    int index = subscribe_key_index_list(key, keys_list);
    if (index == -1) {
        return 1;
    }
    for(int j = index; j < num_keys - 1; j++) {
        strncpy(keys_list[j], keys_list[j+1], MAX_STRING_SIZE);
        keys_list[j][MAX_STRING_SIZE] = '\0';
    }
    return 0;
}