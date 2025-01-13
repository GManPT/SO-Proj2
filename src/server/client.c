#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <signal.h>

#include "client.h"
#include "constants.h"
#include "coperations.h"
#include "operations.h"
#include "../common/io.h"
#include "../common/constants.h"
#include "../common/protocol.h"

// Array of thread identifiers for managing up to MAX_SESSION_COUNT client threads
pthread_t client_threads[MAX_SESSION_COUNT];

// Array to store data related to each client session
ClientData clients_data[MAX_SESSION_COUNT];

// Semaphore to control access to the client session queue or synchronization between threads
sem_t client_sem;

// Mutex to protect shared resources or critical sections related to client list management
pthread_mutex_t listc_mutex = PTHREAD_MUTEX_INITIALIZER;

// Pointer to a hash table structure for storing keys to which clients are subscribed
static struct IntHashTable *subscribed_keys = NULL;

/// Notifies all clients subscribed to a specific key with a given value or a "DELETED" message.
/// @param key The key for which the clients are subscribed.
/// @param value The value to be sent with the notification. If NULL, the notification will include "DELETED".
void notify_clients(const char* key, const char* value) {
    int count;
    int* notify_fds = get_fds(subscribed_keys, key, &count);
    if (!notify_fds) {
        return;
    }
    if (count == 0) {
        free(notify_fds);
        return;
    }
    char notification[MAX_WRITE_SIZE_RESPONSE] = {0};

    // Iterate over the fds and send the notification
    for (int i = 0; i < count; i++) {
        if (value) {
            memset(notification, '\0', MAX_WRITE_SIZE_RESPONSE);
            snprintf(notification, MAX_WRITE_SIZE_RESPONSE, "(%.*s,%.*s)", MAX_STRING_SIZE, key, MAX_WRITE_SIZE, value);
        } else {
            memset(notification, '\0', MAX_WRITE_SIZE_RESPONSE);
            snprintf(notification, MAX_WRITE_SIZE_RESPONSE, "(%.*s,DELETED%.*s)", MAX_STRING_SIZE, key, MAX_WRITE_SIZE-7, "");
        }

        if (write_all(notify_fds[i], notification, MAX_WRITE_SIZE_RESPONSE-1) == -1) {
            fprintf(stderr, "Failed to send notification\n");
        }
    }
    
    free(notify_fds);
}

/// Registers callback functions for write and delete events.
void register_callbacks() {
    register_write_callback(notify_clients);
    register_delete_callback(notify_clients);
}

/// Disconnect a client
/// @param client_data Client data
/// @param cdisconnected 1 if the client is already disconnected, 0 otherwise
void disconnect_client(ClientData* client_data, int cdisconnected, int lock) {
    char response_wrong[3] = {OP_CODE_DISCONNECT + '0', OP_CODE_ERROR_CDU + '0', '\0'};
    char response_right[3] = {OP_CODE_DISCONNECT + '0', OP_CODE_OK_CDU + '0', '\0'};
    int fail = 0;

    if (lock) {
        if (pthread_mutex_lock(&client_data->clientMutex) != 0) {
            fprintf(stderr, "Failed to lock mutex for thread in position: %d\n", client_data->thread_id);
            return;
        }
    }
    
    if (client_data->fds[0] >= 0 && close(client_data->fds[0]) == -1) {
        fprintf(stderr, "Failed to close request pipe\n");
        fail = 1;
    }
    if (client_data->fds[2] >= 0 && close(client_data->fds[2]) == -1) {
        fprintf(stderr, "Failed to close notification pipe\n");
        fail = 1;
    }

    if (!cdisconnected) {
        if (write_all(client_data->fds[1], fail ? response_wrong : response_right, MAX_RESPONSE_SIZE-1) == -1) {
            fprintf(stderr, "Failed to send response\n");
        }
    }
    if (client_data->fds[1] >= 0 && close(client_data->fds[1]) == -1) {
        fprintf(stderr, "Failed to close response pipe\n");
    }

    for (int i = 0; i < client_data->num_keys; i++) {
        remove_key(subscribed_keys, client_data->keys[i], client_data->fds[2]);
    }
    client_data->fds[0] = -1;
    client_data->fds[1] = -1;
    client_data->fds[2] = -1;
    client_data->num_keys = 0;
    memset(client_data->keys, 0, sizeof(client_data->keys));
    client_data->active = 0;
    sem_post(&client_sem);

    if (lock) {
        if (pthread_mutex_unlock(&client_data->clientMutex) != 0) {
            fprintf(stderr, "Failed to unlock mutex for thread in position: %d\n", client_data->thread_id);
        }
    }
}

void disconnect_all_clients() {
    if (pthread_mutex_lock(&listc_mutex) != 0) {
        fprintf(stderr, "Failed to lock listc_mutex\n");
        return;
    }

    int waiting_disconnects = 0;

    for (int i = 0; i < MAX_SESSION_COUNT; i++) {
        if (pthread_mutex_lock(&clients_data[i].clientMutex) != 0) {
            fprintf(stderr, "Failed to lock mutex for thread in position: %d\n", clients_data[i].thread_id);
        }
        if (clients_data[i].active) {
            clients_data[i].force_disconnect = 1;
            waiting_disconnects++;
        }
        if (pthread_mutex_unlock(&clients_data[i].clientMutex) != 0) {
            fprintf(stderr, "Failed to unlock mutex for thread in position: %d\n", clients_data[i].thread_id);
        }
    }

    while (waiting_disconnects > 0) {
        for (int i = 0; i < MAX_SESSION_COUNT; i++) {
            if (pthread_mutex_lock(&clients_data[i].clientMutex) != 0) {
                continue;
            }
            
            if (clients_data[i].force_disconnect && !clients_data[i].active) {
                clients_data[i].force_disconnect = 0;
                waiting_disconnects--;
            }
            
            if (pthread_mutex_unlock(&clients_data[i].clientMutex) != 0) {
                fprintf(stderr, "Failed to unlock mutex for thread in position: %d\n", i);
            }
        }
    }

    if (pthread_mutex_unlock(&listc_mutex) != 0) {
        fprintf(stderr, "Failed to unlock listc_mutex\n");
    }
}

/// Handles client subscription to a key in the KVS.
/// @param client_data The data associated with the client requesting the subscription.
/// @param key The key the client wants to subscribe to.
void client_subscribe_key(ClientData* client_data, const char* key) {
    char response_wrong[3] = {OP_CODE_SUBSCRIBE + '0', OP_CODE_ERROR_S + '0', '\0'};
    char response_right[3] = {OP_CODE_SUBSCRIBE + '0', OP_CODE_OK_S + '0', '\0'};
    int fail = 0;
    
    // Check if the key exists in kvs
    if (kvs_key_exists(key)) {
        fprintf(stderr, "Client %d tried to subscribe a key that does not exist!\n", client_data->thread_id);
        fail = 1;
    } else {
        if (client_data->num_keys >= MAX_NUMBER_SUB) {
            fprintf(stderr, "Client %d tried to subscribe more keys than allowed!\n", client_data->thread_id);
            fail = 1;
        } else if (add_key_to_subscribed_list(key, client_data->keys, client_data->num_keys)) {
            fprintf(stderr, "Client %d is already subscribed to key: %s\n", client_data->thread_id, key);
            fail = 1;
        } else {
            client_data->num_keys++;
            add_key(subscribed_keys, key, client_data->fds[2]);
        }
    }

    if (write_all(client_data->fds[1], fail ? response_wrong : response_right, MAX_RESPONSE_SIZE-1) == -1) {
        fprintf(stderr, "Failed to send response\n");
    }
}

/// Handles client unsubscription from a key in the KVS.
/// @param client_data The data associated with the client requesting the unsubscription.
/// @param key The key the client wants to unsubscribe from.
void client_unsubscribe_key(ClientData* client_data, const char* key) {
    char response_wrong[3] = {OP_CODE_UNSUBSCRIBE + '0', OP_CODE_ERROR_CDU + '0', '\0'};
    char response_right[3] = {OP_CODE_UNSUBSCRIBE + '0', OP_CODE_OK_CDU + '0', '\0'};
    int fail = 0;
    
    // Check if the client was subscribed to the key
    if (remove_key_from_subscribed_list(key, client_data->keys, client_data->num_keys)) {
        fprintf(stderr, "Client %d is not subscribed to key: %s\n", client_data->thread_id, key);
        fail = 1;
    } else {
        remove_key(subscribed_keys, key, client_data->fds[2]);
        client_data->num_keys--;
    }
    
    if (write_all(client_data->fds[1], fail ? response_wrong : response_right, MAX_RESPONSE_SIZE-1) == -1) {
        fprintf(stderr, "Failed to send response\n");
    }
}

/// Handles the client thread logic, processing client requests and managing their subscription status.
/// @param data A pointer to the `ClientData` structure containing the client’s information 
/// (file descriptors, active status, etc.).
/// @return NULL
void *client_thread(void *data) {
    ClientData *client_data = (ClientData *)data;
    int result, intr = 0, disconnect = 0, fail = 0, request_copy;

    // Buffer for reading from pipe
    char buffer[MAX_SIZE_OPCODE] = {0};
    char key[MAX_STRING_SIZE + 1] = {0};
    char rw[3];

    // Block sigusr1
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGUSR1);
    if (pthread_sigmask(SIG_BLOCK, &set, NULL) != 0) {
        fprintf(stderr, "Failed to block SIGUSR1\n");
    }

    while(1) {
        if (pthread_mutex_lock(&client_data->clientMutex) != 0) {
            fprintf(stderr, "Failed to lock mutex for thread in position: %d\n", client_data->thread_id);
            continue;
        }

        if (client_data->force_disconnect && client_data->active) {
            disconnect = 1;
            fail = 1;
            pthread_mutex_unlock(&client_data->clientMutex);
            disconnect_client(client_data, fail, 1);
            disconnect = 0;
            fail = 0;
            continue;
        }

        while (!client_data->active && !client_data->force_disconnect) {
            pthread_cond_wait(&client_data->clientCond, &client_data->clientMutex);
        }

        if (client_data->force_disconnect) {
            pthread_mutex_unlock(&client_data->clientMutex);
            continue;
        }

        if (pthread_mutex_unlock(&client_data->clientMutex) != 0) {
            fprintf(stderr, "Failed to unlock mutex for thread in position: %d\n", client_data->thread_id);
            continue;
        }
        
        // Send OK to client
        rw[0] = OP_CODE_CONNECT + '0';
        rw[1] = OP_CODE_OK_CDU + '0';
        rw[2] = '\0';
        if (write_all(client_data->fds[1], rw, MAX_RESPONSE_SIZE-1) == -1) {
            fprintf(stderr, "Failed to send response\n");
            disconnect = 1;
        } else {
            fprintf(stdout, "Client connected to thread %d\n", client_data->thread_id);
        }
        
        request_copy = client_data->fds[0];

        while (!disconnect) {
            if (pthread_mutex_lock(&client_data->clientMutex) != 0) {
                fprintf(stderr, "Failed to lock mutex for thread\n");
            }
            
            if (client_data->force_disconnect) {
                disconnect = 1;
                fail = 1;
                if (pthread_mutex_unlock(&client_data->clientMutex) != 0) {
                    fprintf(stderr, "Failed to unlock mutex for thread\n");
                }
                break;
            }
            
            if (pthread_mutex_unlock(&client_data->clientMutex) != 0) {
                fprintf(stderr, "Failed to unlock mutex for thread\n");
            }

            if ((result = read_all(client_data->fds[0], buffer, MAX_SIZE_OPCODE-1, &intr)) == -1) {
                if (intr) {
                    if (request_copy == client_data->fds[0]) {
                        fprintf(stderr, "Client disconnected\n");
                        intr = 0;
                        disconnect = 1;
                        fail = 1;
                        continue;
                    }
                    // Client disconnected
                    fprintf(stderr, "Read was interrupted\n");
                    intr = 0;
                    break;
                }
                fprintf(stderr, "Failed to read from request pipe\n");
                continue;
            } else if (result == 0) {
                if (request_copy == client_data->fds[0]) {
                    fprintf(stderr, "Client disconnected\n");
                    disconnect = 1;
                    fail = 1;
                    continue;
                }
                // Client changed
                break;
            }

            char op_code = buffer[0] - '0';
            switch (op_code) {
                case OP_CODE_DISCONNECT:
                    disconnect = 1;
                    fail = 0;
                    break;
                
                case OP_CODE_UNSUBSCRIBE:
                case OP_CODE_SUBSCRIBE:
                    if ((result = read_all(client_data->fds[0], key, MAX_STRING_SIZE, &intr)) == -1) {
                        if (intr) {
                            fprintf(stderr, "Read was interrupted\n");
                            intr = 0;
                            disconnect = 1;
                            fail = 1;
                            break;
                        }
                    } else if (result == 0) {
                        fprintf(stderr, "Client disconnected\n");
                        disconnect = 1;
                        fail = 1;
                        break;
                    }
                            
                    if (op_code == OP_CODE_SUBSCRIBE) {
                        client_subscribe_key(client_data, key);
                    } else {
                        client_unsubscribe_key(client_data, key);
                    }
                    break;
                default:
                    fprintf(stderr, "Invalid opcode\n");
                    break;
            }

        }
        if (disconnect) {
            disconnect_client(client_data, fail, 1);
            disconnect = 0;
            fail = 0;
        }
    }
    
    return NULL;
}

int start_client_threads() {
    if (sem_init(&client_sem, 0, MAX_SESSION_COUNT) != 0) {
        fprintf(stderr, "Failed to initialize semaphore\n");
        return 1;
    }

    // Alloc memory for the subscribed keys hash table
    subscribed_keys = create_int_hash_table();
    if (!subscribed_keys) {
        fprintf(stderr, "Failed to create hash table\n");
        return 1;
    }

    register_callbacks();

    for (int i = 0; i < MAX_SESSION_COUNT; i++) {
        // Create a new thread structure
        memset(&clients_data[i], 0, sizeof(ClientData));
        clients_data[i].fds[0] = -1;
        clients_data[i].fds[1] = -1;
        clients_data[i].fds[2] = -1;
        clients_data[i].num_keys = 0;
        clients_data[i].thread_id = i;
        clients_data[i].active = 0;
        if (pthread_mutex_init(&clients_data[i].clientMutex, NULL) != 0) {
            fprintf(stderr, "Failed to initialize mutex for thread in position: %d\n", i);
            return 1;
        }
        if (pthread_cond_init(&clients_data[i].clientCond, NULL) != 0) {
            fprintf(stderr, "Failed to initialize condition for thread in position: %d\n", i);
            return 1;
        }
        
        // New thread
        if (pthread_create(&client_threads[i], NULL, client_thread, (void *)&clients_data[i]) != 0) {
            fprintf(stderr, "Failed to create thread in position: %d\n", i);
            return 1;
        }
    }
    return 0;
}

int activate_client(int request_fd, int response_fd, int notification_fd) {
    // Wait for a client to disconnect
    sigset_t set;
    sigemptyset(&set);
    sigaddset(&set, SIGUSR1);
    sigprocmask(SIG_BLOCK, &set, NULL);
    sem_wait(&client_sem);
    sigprocmask(SIG_UNBLOCK, &set, NULL);

    if (pthread_mutex_lock(&listc_mutex) != 0) {
        fprintf(stderr, "Failed to lock listc_mutex\n");
        return 1;
    }

    for (int i = 0; i < MAX_SESSION_COUNT; i++) {
        if (pthread_mutex_lock(&clients_data[i].clientMutex) != 0) {
            fprintf(stderr, "Failed to lock mutex for thread in position: %d\n", i);
            if (pthread_mutex_unlock(&listc_mutex) != 0) {
                fprintf(stderr, "Failed to unlock listc_mutex\n");
                return 1;
            }
            return 1;
        }
        if (!clients_data[i].active) {
            if (request_fd == -1 || response_fd == -1 || notification_fd == -1) {
                fprintf(stderr, "Invalid fd. Could not connect\n");
                if (pthread_mutex_unlock(&clients_data[i].clientMutex) != 0) {
                    fprintf(stderr, "Failed to unlock mutex for thread in position: %d\n", i);
                    return 1;
                }
                if (pthread_mutex_unlock(&listc_mutex) != 0) {
                    fprintf(stderr, "Failed to unlock listc_mutex\n");
                    return 1;
                }
                return 1;
            }
            
            clients_data[i].fds[0] = request_fd;
            clients_data[i].fds[1] = response_fd;
            clients_data[i].fds[2] = notification_fd;
            clients_data[i].active = 1;

            pthread_cond_signal(&clients_data[i].clientCond);
            if (pthread_mutex_unlock(&clients_data[i].clientMutex) != 0) {
                if (pthread_mutex_unlock(&listc_mutex) != 0) {
                    fprintf(stderr, "Failed to unlock listc_mutex\n");
                    return 1;
                }
                fprintf(stderr, "Failed to unlock mutex for thread in position: %d\n", i);
                return 1;
            }
            if (pthread_mutex_unlock(&listc_mutex) != 0) {
                fprintf(stderr, "Failed to unlock listc_mutex\n");
                return 1;
            }
            return 0;
        }
        if (pthread_mutex_unlock(&clients_data[i].clientMutex) != 0) {
            if (pthread_mutex_unlock(&listc_mutex) != 0) {
                fprintf(stderr, "Failed to unlock listc_mutex\n");
                return 1;
            }
            fprintf(stderr, "Failed to unlock mutex for thread in position: %d\n", i);
            return 1;
        }
    }
    if (pthread_mutex_unlock(&listc_mutex) != 0) {
        fprintf(stderr, "Failed to unlock listc_mutex\n");
        return 1;
    }
    return -1;
}
