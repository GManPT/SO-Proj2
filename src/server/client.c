#include "constants.h"

int keyListExists(Client* client, char const* key) {
  for (int i = 0; i < client->subscribed_keys_num; i++) {
    if (strcmp(client->keys_subscribed[i].subscribe_key, key) == 0) {
      return 1;
    }
  }
  return 0;
}

int keyListAdd(Client* client, char const* key) {
  if (client->num_keys >= MAX_NUMBER_SUB) {
    return 1;
  }
  if (keyListExists(client, key)) {
    return 2;
  }
  strncpy(client->keys_subscribed[client->subscribed_keys_num].subscribe_key, key, MAX_KEY_NAME_SIZE - 1);
  client->keys_subscribed[client->subscribed_keys_num].subscribe_key[MAX_KEY_NAME_SIZE - 1] = '\0';
  client->subscribed_keys_num++;
  return 0;
}

int keyListDelete(Client* client, char const* key) {
  for (int i = 0; i < client->subscribed_keys_num; i++) {
    if (strcmp(client->keys_subscribed[i].subscribe_key, key) == 0) {
      for (int j = i; j < client->num_keys - 1; j++) {
        client->keys_subscribed[j] = client->keys_subscribed[j + 1];
      }
      client->subscribed_keys_num--;
      return 0;
    }
  }
  return 1;
}

int addClient(Client* clients, Client client) {
    for (int i = 0; i < MAX_SESSION_COUNT; i++) {
        if (clients[i].id == -1) {
            clients[i] = client;
            return 0;
        }
    }

    return 1;
}


int removeClient(Client* clients, int id) {
    for (int i = 0; i < MAX_SESSION_COUNT; i++) {
        if (clients[i].id == id) {
            for (int j = i; j < MAX_SESSION_COUNT - 1; j++) {
                clients[j] = clients[j + 1];
            }
            return 0;
        }
    }
    return 1
}

int* inform_subscribed_clients(Client clients[], char* key, char* msg) {
  size_t numClients = sizeof(clients) / sizeof(clients[0]);
  for (int i = 0; i < numClients; i++) {
    if (keyListExists(clients[i].keys_subscribed)) {
      write_all(clients[i].notify_fifo, msg, sizeof(msg));
    }
  }
}