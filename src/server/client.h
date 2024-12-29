#ifndef CLIENT_H
#define CLIENT_H

typedef struct keys_subscribed_list {
  char subscribe_key[MAX_KEY_NAME_SIZE];
} KeysSubscribedList;

typedef struct client {
  int id;
  char notify_fifo_name[MAX_PIPE_PATH_LENGTH];
  char request_fifo_name[MAX_PIPE_PATH_LENGTH];
  char response_fifo_name[MAX_PIPE_PATH_LENGTH];
  int notify_fifo, request_fifo, response_fifo;
  int subscribed_keys_num = 0;
  KeysSubscribedList keys_subscribed[MAX_NUMBER_SUB];
} Client;

int* inform_subscribed_clients(Client client);
int keyListExists(Client* client, char const* key);
int keyListAdd(Client* client, char const* key);
int keyListDelete(Client* client, char const* key);

#endif