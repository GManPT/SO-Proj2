#include <stdio.h>
#include <fcntl.h>
#include <errno.h>

#include "constants.h"
#include "client.h"
#include "operations.h"
#include "io.h"
#include "../common/constants.h"
#include "../common/io.h"

char* read_msg(int fifo_fd, size_t size) {
  char msg[size];
  int intr = 0;
  int result = read_all(fifo_fd, msg, size, &intr);
  if (result == -1) {
    if (intr)
      fprintf(stderr, "Read operation interrupted.\n");
  } else if (result == 0)
    //TODO PENSO QUE CONTINUA A LER ATE ACABAR
    fprintf(stderr, "End of file reached.\n");
  else if (result == 1)
    return msg;
  return NULL;
}

int write_msg(int fifo_fd, char* msg, size_t size) {
  if (write_all(fifo_fd, msg, size) == -1) {
    fprintf(stderr, "Client connection failed\n");
    return 1;
  }
  return 0;
}


int keyListExists(Client* client, char const* key) {
  for (int i = 0; i < client->subscribed_keys_num; i++) {
    if (strcmp(client->keys_subscribed[i], key) == 0) {
      return 1;
    }
  }
  return 0;
}

int keyListAdd(Client* client, char const* key) {
  if (client->subscribed_keys_num >= MAX_NUMBER_SUB) {
    return 1;
  }
  if (keyListExists(client, key)) {
    return 2;
  }
  strncpy(client->keys_subscribed[client->subscribed_keys_num], key, MAX_STRING_SIZE - 1);
  client->keys_subscribed[client->subscribed_keys_num][MAX_STRING_SIZE - 1] = '\0';
  client->subscribed_keys_num++;
  return 0;
}

int keyListDelete(Client* client, char const* key) {
  for (int i = 0; i < client->subscribed_keys_num; i++) {
    if (strcmp(client->keys_subscribed[i], key) == 0) {
      for (int j = i; j < client->subscribed_keys_num - 1; j++) {
        strcpy(client->keys_subscribed[j], client->keys_subscribed[j + 1]);
      }
      client->keys_subscribed[client->subscribed_keys_num - 1][0] = '\0';
      client->subscribed_keys_num--;
      return 0;
    }
  }
  return 1;
}

int addClient(Client* clients[], Client* client) {
  for (int i = 0; i < MAX_SESSION_COUNT; i++) {
    if (clients[i]->id == -1) {
      clients[i] = client;
      return 0;
    }
  }
  return 1;
}


int removeClient(Client* clients[], int id) {
  for (int i = 0; i < MAX_SESSION_COUNT; i++) {
    if (clients[i]->id == id) {
      for (int j = i; j < MAX_SESSION_COUNT - 1; j++) {
        clients[j] = clients[j + 1];
      }
      return 0;
    }
  }
  return 1;
}

int add_client_info(int fifo_fd, char* client_pipes_names, Client* client) {
  if (client == NULL) {
    fprintf(stderr, "Failed to allocate memory for client\n");
    return 1;
  }
  size_t offset = 0;
  offset += strn_memcpy(client->request_fifo, client_pipes_names + offset, MAX_PIPE_PATH_LENGTH + 1);
  offset += strn_memcpy(client->response_fifo_name, client_pipes_names + offset, MAX_PIPE_PATH_LENGTH + 1);
  offset += strn_memcpy(client->notify_fifo_name, client_pipes_names + offset, MAX_PIPE_PATH_LENGTH + 1);
  client->response_fifo = open(client->response_fifo_name, O_WRONLY | O_NONBLOCK);
  if (client->response_fifo < 0) {
    fprintf(stderr, "Failed to open FIFO\n");
    free(client);
    return 1;
  }
  client->request_fifo = open(client->request_fifo_name, O_RDONLY | O_NONBLOCK);
  if (client->request_fifo < 0) {
    fprintf(stderr, "Failed to open FIFO\n");
    return 1;
  }
  client->notify_fifo = open(client->notify_fifo_name, O_RDWR | O_NONBLOCK);
  if (client->notify_fifo < 0) {
    close(client->request_fifo);
    fprintf(stderr, "Failed to open FIFO\n");
    return 1;
  }
  client->subscribed_keys_num = 0;
  number_of_clients++;
  client->id = number_of_clients;
  addClient(clients, client);
  return 0;
}

void close_client_connection(Client* client) {
  int result = client != NULL ? 0 : 1;
  number_of_clients--;
  removeClient(clients, client->id);
  char msg[RESPONSE_SIZE+1];
  snprintf(msg, RESPONSE_SIZE+1, "2%i", result);
  if (write_msg(client->response_fifo, msg, RESPONSE_SIZE+1) == 1)
    close_client_connection(client);
  close(client->notify_fifo);
  close(client->request_fifo);
  close(client->response_fifo);
  free(client);
}

int subscribe_key(Client* client, char const* key) {
  char* value = kvs_get_value(key);
  int response_code = value != NULL ? 1 : 0;
  char msg[RESPONSE_SIZE+1];
  snprintf(msg, RESPONSE_SIZE+1, "3%i", response_code);
  if (write_msg(client->response_fifo, msg, sizeof(msg)) == 1) {
    close_client_connection(client);
    return 1;
  }
  if (!response_code) return 1;
  int code = keyListAdd(client->keys_subscribed, key);
  if (code == 1) {
    fprintf(stderr, "The client already subscribed the max num of keys!");
  } else if (code == 2) {
    fprintf(stderr, "The client already subscribed that key!");
  }
  return 0;
}

int unsubscribe_key(Client* client, char const* key) {
  int response_code = 0;
  if (keyListDelete(client, key) != 0) {
    response_code = 1;
    fprintf(stderr, "The subscription doesn't exist!");
  }
  char msg[RESPONSE_SIZE+1];
  snprintf(msg, RESPONSE_SIZE+1, "3%i", response_code);
  if (write_msg(client->response_fifo, msg, RESPONSE_SIZE+1) == 1) {
    close_client_connection(client);
    return 1;
  }
  return 0;
}

int inform_subscribed_clients(Client* clients[], char* key, char* msg) {
  size_t numClients = sizeof(clients) / sizeof(clients[0]);
  for (int i = 0; i < numClients; i++) {
    if (keyListExists(clients[i], key)) {
      if (write_msg(clients[i]->notify_fifo, msg, sizeof(msg)) == 1)
        close_client_connection(clients[i]);
    }
  }
}
