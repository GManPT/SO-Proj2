#include <pthread.h>
#include <semaphore.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

#include "client.h"
#include "constants.h"
#include "coperations.h"
#include "operations.h"
#include "../common/io.h"
#include "../common/constants.h"
#include "../common/protocol.h"

pthread_t client_threads[MAX_SESSION_COUNT];
ClientData clients_data[MAX_SESSION_COUNT];
sem_t client_sem;
static struct IntHashTable *subscribed_keys = NULL;

void print_node_values(const char* key) {
    unsigned int index = chash(key);
    pthread_rwlock_rdlock(&subscribed_keys->tableLock);
    IntNode *node = subscribed_keys->nodes[index];
    if (!node) {
        printf("Key %s: No values\n", key);
        pthread_rwlock_unlock(&subscribed_keys->tableLock);
        return;
    }
    pthread_rwlock_unlock(&subscribed_keys->tableLock);

    pthread_rwlock_rdlock(&node->nodeLock);
    printf("Key %s values: [", key);
    for(int i = 0; i < node->count; i++) {
        printf("%d%s", node->values[i], i < node->count - 1 ? ", " : "");
    }
    printf("]\n");
    pthread_rwlock_unlock(&node->nodeLock);
}

void print_client_keys(ClientData* client_data) {
    printf("Thread %d keys: [", client_data->thread_id);
    for(int i = 0; i < client_data->num_keys; i++) {
        printf("%s%s", client_data->keys[i], i < client_data->num_keys - 1 ? ", " : "");
    }
    printf("]\n");
}

void notify_clients(const char* key, const char* value) {
    int count;
    int* notify_fds = get_fds(subscribed_keys, key, &count);
    if (!notify_fds) {
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
    
}


void register_callbacks() {
    register_write_callback(notify_clients);
    register_delete_callback(notify_clients);
}

void disconnect_client(ClientData* client_data) {
    char response_wrong[3] = {OP_CODE_DISCONNECT + '0', OP_CODE_ERROR_CDU + '0', '\0'};
    char response_right[3] = {OP_CODE_DISCONNECT + '0', OP_CODE_OK_CDU + '0', '\0'};
    int fail = 0;
    if (client_data->fds[0] >= 0 && close(client_data->fds[0]) == -1) {
        fprintf(stderr, "Failed to close request pipe\n");
        fail = 1;
    }
    if (client_data->fds[2] >= 0 && close(client_data->fds[2]) == -1) {
        fprintf(stderr, "Failed to close notification pipe\n");
        fail = 1;
    }
    if (write_all(client_data->fds[1], fail ? response_wrong : response_right, MAX_RESPONSE_SIZE-1) == -1) {
        fprintf(stderr, "Failed to send response\n");
    }
    if (client_data->fds[1] >= 0 && close(client_data->fds[1]) == -1) {
        fprintf(stderr, "Failed to close response pipe\n");
    }

    for (int i = 0; i < client_data->num_keys; i++) {
        remove_key(subscribed_keys, client_data->keys[i], client_data->fds[2]);
    }
    client_data->num_keys = 0;
    memset(client_data->keys, 0, sizeof(client_data->keys));
    client_data->active = 0;
    sem_post(&client_sem);
}

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
    //print_node_values(key);
    //print_client_keys(client_data);
    if (write_all(client_data->fds[1], fail ? response_wrong : response_right, MAX_RESPONSE_SIZE-1) == -1) {
        fprintf(stderr, "Failed to send response\n");
    }
}

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
    //print_node_values(key);
    //print_client_keys(client_data);
    if (write_all(client_data->fds[1], fail ? response_wrong : response_right, MAX_RESPONSE_SIZE-1) == -1) {
        fprintf(stderr, "Failed to send response\n");
    }
}

void *client_thread(void *data) {
    ClientData *client_data = (ClientData *)data;
    int result, intr = 0, disconnect = 0;

    // Buffer for reading from pipe
    char buffer[MAX_SIZE_OPCODE] = {0};
    char key[MAX_STRING_SIZE + 1] = {0};
    char rw[3];

    while(1) {
        if (pthread_mutex_lock(&client_data->clientMutex) != 0) {
            fprintf(stderr, "Failed to lock mutex for thread in position: %d\n", client_data->thread_id);
        }
        while (!client_data->active) {
            pthread_cond_wait(&client_data->clientCond, &client_data->clientMutex);
        }
        if (pthread_mutex_unlock(&client_data->clientMutex) != 0) {
            fprintf(stderr, "Failed to unlock mutex for thread in position: %d\n", client_data->thread_id);
        }
        
        // Send OK to client
        rw[0] = OP_CODE_CONNECT + '0';
        rw[1] = OP_CODE_OK_CDU + '0';
        rw[2] = '\0';
        if (write_all(client_data->fds[1], rw, 2) == -1) {
            fprintf(stderr, "Failed to send response\n");
            disconnect = 1;
        } else {
            fprintf(stdout, "Client connected to thread %d\n", client_data->thread_id);
        }

        while (1) {
            if (disconnect) {
                if (pthread_mutex_lock(&client_data->clientMutex) != 0) {
                    fprintf(stderr, "Failed to lock mutex for thread in position: %d\n", client_data->thread_id);
                }
                disconnect_client(client_data);
                if (pthread_mutex_unlock(&client_data->clientMutex) != 0) {
                    fprintf(stderr, "Failed to unlock mutex for thread in position: %d\n", client_data->thread_id);
                }

                disconnect = 0;
                break;
            }

            if ((result = read_all(client_data->fds[0], buffer, MAX_SIZE_OPCODE-1, &intr)) == -1) {
                if (intr) {
                    // Client disconnected
                    fprintf(stderr, "Read was interrupted\n");
                    intr = 0;
                    disconnect = 1;
                    break;
                }
                fprintf(stderr, "Failed to read from request pipe\n");
            } else if (result == 0) {
                // Client disconnected
                fprintf(stderr, "Client disconnected\n");
                disconnect = 1;
                break;
            }

            char op_code = buffer[0] - '0';
            switch (op_code) {
                case OP_CODE_DISCONNECT:
                    disconnect = 1;
                    break;
                
                case OP_CODE_UNSUBSCRIBE:
                case OP_CODE_SUBSCRIBE:
                    if ((result = read_all(client_data->fds[0], key, MAX_STRING_SIZE, &intr)) == -1) {
                        if (intr) {
                            fprintf(stderr, "Read was interrupted\n");
                            intr = 0;
                            disconnect = 1;
                            break;
                        }
                    } else if (result == 0) {
                        fprintf(stderr, "Client disconnected\n");
                        disconnect = 1;
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
    sem_wait(&client_sem);
    for (int i = 0; i < MAX_SESSION_COUNT; i++) {
        if (pthread_mutex_lock(&clients_data[i].clientMutex) != 0) {
            fprintf(stderr, "Failed to lock mutex for thread in position: %d\n", i);
            return 1;
        }
        if (!clients_data[i].active) {
            if (request_fd == -1 || response_fd == -1 || notification_fd == -1) {
                fprintf(stderr, "Invalid fd. Could not connect\n");
                return 1;
            }
            
            clients_data[i].fds[0] = request_fd;
            clients_data[i].fds[1] = response_fd;
            clients_data[i].fds[2] = notification_fd;
            clients_data[i].active = 1;

            pthread_cond_signal(&clients_data[i].clientCond);
            if (pthread_mutex_unlock(&clients_data[i].clientMutex) != 0) {
                fprintf(stderr, "Failed to unlock mutex for thread in position: %d\n", i);
                return 1;
            }
            return 0;
        }
        if (pthread_mutex_unlock(&clients_data[i].clientMutex) != 0) {
            fprintf(stderr, "Failed to unlock mutex for thread in position: %d\n", i);
            return 1;
        }
    }
    return -1;
}
