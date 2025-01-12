#ifndef COPERATIONS_H
#define COPERATIONS_H

#include <pthread.h>
#include "constants.h"
#include "../common/constants.h"

typedef struct IntNode {
    int values[MAX_SESSION_COUNT];
    int count; // Number of values
    pthread_rwlock_t nodeLock;
} IntNode;

typedef struct IntHashTable {
    IntNode *nodes[MAX_KEYS_COUNT];
    pthread_rwlock_t tableLock;
} IntHashTable;

/// Hash function
/// @param key Key to hash
/// @return Hashed value
unsigned int chash(const char *key);

/// Create a new hash table
/// @return Newly created hash table, NULL on failure
struct IntHashTable* create_int_hash_table();

/// Destroy a hash table
/// @param ht Hash table to destroy
void destroy_int_hash_table(IntHashTable *ht);

/// Add a key to the hash table
/// @param ht Hash table
/// @param key Key to add
/// @param value Value to add
/// @return 0 on success, -1 on failure
int add_key(IntHashTable *ht, const char *key, int value);

/// Remove a key from the hash table
/// @param ht Hash table
/// @param key Key to remove
/// @param value Value to remove
/// @return 0 on success, -1 on failure
int remove_key(IntHashTable *ht, const char *key, int value);

/// Get the list of values associated with a key
/// @param ht Hash table
/// @param key Key to search
/// @param count Number of values found
/// @return An array of values, NULL on failure
int* get_fds(IntHashTable *ht, const char *key, int *count);

/// Get the index of a key in the list
/// @param key Key to search
/// @param keys_list List of keys
/// @return Index of the key, -1 if not found
int subscribe_key_index_list(const char* key, char keys_list[MAX_NUMBER_SUB][MAX_STRING_SIZE+1]);

/// Add a key to the list
/// @param key Key to add
/// @param keys_list List of keys
/// @param num_keys Number of keys
/// @return 0 on success, -1 on failure
int add_key_to_subscribed_list(const char* key, char keys_list[MAX_NUMBER_SUB][MAX_STRING_SIZE+1], int num_keys);

/// Remove a key from the list
/// @param key Key to remove
/// @param keys_list List of keys
/// @param num_keys Number of keys
/// @return 0 on success, -1 on failure
int remove_key_from_subscribed_list(const char* key, char keys_list[MAX_NUMBER_SUB][MAX_STRING_SIZE+1], int num_keys);

#endif