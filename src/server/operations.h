#ifndef KVS_OPERATIONS_H
#define KVS_OPERATIONS_H

#include <stddef.h>

#include "constants.h"

// Callback function type for key value pairs.
// @param key Key of the pair.
// @param value Value of the pair.
typedef void (*kvs_callback_t)(const char* key, const char* value);

/// Initializes the KVS state.
/// @return 0 if the KVS state was initialized successfully, 1 otherwise.
int kvs_init();

/// Destroys the KVS state.
/// @return 0 if the KVS state was terminated successfully, 1 otherwise.
int kvs_terminate();

/// Writes a key value pair to the KVS. If key already exists it is updated.
/// @param num_pairs Number of pairs being written.
/// @param keys Array of keys' strings.
/// @param values Array of values' strings.
/// @return 0 if the pairs were written successfully, 1 otherwise.
int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE],
              char values[][MAX_STRING_SIZE]);

/// Reads values from the KVS.
/// @param num_pairs Number of pairs to read.
/// @param keys Array of keys' strings.
/// @param fd File descriptor to write the (successful) output.
/// @return 0 if the key reading, 1 otherwise.
int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd);

/// Deletes key value pairs from the KVS.
/// @param num_pairs Number of pairs to read.
/// @param keys Array of keys' strings.
/// @return 0 if the pairs were deleted successfully, 1 otherwise.
int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int fd);

/// Writes the state of the KVS.
/// @param fd File descriptor to write the output.
void kvs_show(int fd);

/// Creates a backup of the KVS state and stores it in the correspondent
/// backup file
/// @return 0 if the backup was successful, 1 otherwise.
int kvs_backup(size_t num_backup, char *job_filename, char *directory);

/// Waits for the last backup to be called.
void kvs_wait_backup();

/// Waits for a given amount of time.
/// @param delay_us Delay in milliseconds.
void kvs_wait(unsigned int delay_ms);

// Setter for max_backups
// @param _max_backups
void set_max_backups(int _max_backups);

// Setter for n_current_backups
// @param _n_current_backups
void set_n_current_backups(int _n_current_backups);

// Getter for n_current_backups
// @return n_current_backups
int get_n_current_backups();

// Initializes the fifo with the given name
// @param fifo_name
// @return 0 if the fifo was initialized successfully, 1 otherwise.
int fifo_init(char* fifo_name);

// Removes white spaces from the end of a string.
// @param str String to remove white spaces from.
void trim_trailing_whitespace(char *str);

// Checks if a key exists in the KVS.
// @param key Key to check.
// @return 0 if the key exists, 1 otherwise.
int kvs_key_exists(const char* key);

// Registers a callback function for write operations in the KVS.
/// @param callback The function to be called when a write operation is performed.
void register_write_callback(kvs_callback_t callback);

/// Registers a callback function for delete operations in the KVS.
/// @param callback The function to be called when a delete operation is performed.
void register_delete_callback(kvs_callback_t callback); 

#endif // KVS_OPERATIONS_H
