#ifndef CLIENT_H
#define CLIENT_H

#include <pthread.h>
#include "../common/constants.h"

/// Structure to hold client data
typedef struct ClientData {
    int fds[3]; // 0: request_fd, 1: response_fd, 2: notification_fd
    int num_keys; // Number of keys subscribed
    int thread_id; // Thread ID
    int active; // 1 if the client is active, 0 otherwise
    char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE + 1]; // Subscribed keys
    int force_disconnect; // 1 if the client should be disconnected, 0 otherwise
    pthread_mutex_t clientMutex;
    pthread_cond_t clientCond;
} ClientData;

/// Start client threads at the beginning of the server
/// @return 0 if successful, -1 otherwise
int start_client_threads();

/// Activate a client
/// @param request_fd File descriptor for the request
/// @param response_fd File descriptor for the response
/// @param notification_fd File descriptor for the notification
/// @return 0 if successful, 1 otherwise
int activate_client(int request_fd, int response_fd, int notification_fd);

void disconnect_client(ClientData* client_data, int fail, int lock);

/// Disconnect all clients
void disconnect_all_clients();

#endif