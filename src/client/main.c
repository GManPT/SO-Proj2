#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "parser.h"
#include "src/client/api.h"
#include "src/common/constants.h"
#include "src/common/io.h"

int main(int argc, char *argv[]) {
  if (argc < 3) {
    fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n",
            argv[0]);
    return 1;
  }

  char req_pipe_path[MAX_PIPE_PATH_LENGTH] = "/tmp/req";
  char resp_pipe_path[MAX_PIPE_PATH_LENGTH] = "/tmp/resp";
  char notif_pipe_path[MAX_PIPE_PATH_LENGTH] = "/tmp/notif";
  char server_pipe_path[MAX_PIPE_PATH_LENGTH] = "/tmp/";

  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
  unsigned int delay_ms;
  size_t num;
  int result = 0;

  strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
  strncat(server_pipe_path, argv[2], strlen(argv[2]) * sizeof(char));

  // Connect to the server
  if (kvs_connect(req_pipe_path, resp_pipe_path, server_pipe_path,
                  notif_pipe_path) != 0) {
    fprintf(stderr, "Failed to connect to the server\n");
    return 1;
  }

  // Start notifications thread
  pthread_t notifications_thread;
  if (pthread_create(&notifications_thread, NULL, kvs_notifications, NULL) != 0) {
    fprintf(stderr, "Failed to create notifications thread\n");
    if (kvs_disconnect() != 0) {
      fprintf(stderr, "Failed to disconnect to the server\n");
      return 1;
    }
  }

  while (1) {
    switch (get_next(STDIN_FILENO)) {
    case CMD_DISCONNECT:
      if (kvs_disconnect() != 0) {
        fprintf(stderr, "Failed to disconnect to the server\n");
      }
      
      if (pthread_join(notifications_thread, NULL) != 0) {
        fprintf(stderr, "Failed to join notifications thread\n");
        return 1;
      }

      fprintf(stdout, "Disconnected from server\n");
      return 0;

    case CMD_SUBSCRIBE:
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      if (num == 0) {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }
      
      if ((result = kvs_subscribe(keys[0])) != 0) {
        if (result == 2) {
          if (pthread_join(notifications_thread, NULL) != 0) {
            fprintf(stderr, "Failed to join notifications thread\n");
            return 1;
          }
          return 0;
        }
        fprintf(stderr, "Command subscribe failed\n");
      }

      break;

    case CMD_UNSUBSCRIBE:
      num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
      if (num == 0) {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if ((result = kvs_unsubscribe(keys[0])) != 0) {
        if (result == 2) {
          if (pthread_join(notifications_thread, NULL) != 0) {
            fprintf(stderr, "Failed to join notifications thread\n");
            return 1;
          }
          return 0;
        }
        fprintf(stderr, "Command unsubscribe failed\n");
      }

      break;

    case CMD_DELAY:
      if (parse_delay(STDIN_FILENO, &delay_ms) == -1) {
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay_ms > 0) {
        printf("Waiting...\n");
        delay(delay_ms);
      }
      break;

    case CMD_INVALID:
      fprintf(stderr, "Invalid command. See HELP for usage\n");
      break;

    case CMD_EMPTY:
      break;

    case EOC:
      if (kvs_disconnect() != 0) {
        fprintf(stderr, "Failed to disconnect to the server\n");
        return 1;
      }

      if (pthread_join(notifications_thread, NULL) != 0) {
        fprintf(stderr, "Failed to join notifications thread\n");
        return 1;
      }
      
      return 0;
    }
  }
}