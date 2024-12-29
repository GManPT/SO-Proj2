#ifndef CLIENT_H
#define CLIENT_H

#include "constants.h"
#include "../common/constants.h"

typedef struct client {
  int id;
  char notify_fifo_name[MAX_PIPE_PATH_LENGTH];
  char request_fifo_name[MAX_PIPE_PATH_LENGTH];
  char response_fifo_name[MAX_PIPE_PATH_LENGTH];
  int notify_fifo, request_fifo, response_fifo;
  int subscribed_keys_num;
  char keys_subscribed[MAX_NUMBER_SUB][MAX_STRING_SIZE];
} Client;

Client clients[MAX_NUMBER_SUB];
int number_of_clients = 0;

char* read_msg(int fifo_fd, size_t size);
char* write_msg(int fifo_fd, char* msg, size_t size);
int inform_subscribed_clients(Client* clients[], char* key, char* msg);
int keyListExists(Client* client, char const* key);
int keyListAdd(Client* client, char const* key);
int keyListDelete(Client* client, char const* key);
int addClient(Client* clients[], Client* client);
int removeClient(Client* clients[], int id);

#endif