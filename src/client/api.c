#include "api.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"
#include "src/common/io.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static char req_path[MAX_PIPE_PATH_LENGTH];
static char resp_path[MAX_PIPE_PATH_LENGTH];
static char notif_path[MAX_PIPE_PATH_LENGTH];

static int request_fd = -1;
static int response_fd = -1;
static int notification_fd = -1;

int kvs_disconnect() {
  int result, intr = 0;

  // Send disconnect message
  if (write_all(request_fd, "2", 1) == -1) {
    fprintf(stderr, "Failed to send disconnect message\n");
    return 1;
  }

  // Wait for server response
  char response[MAX_RESPONSE_SIZE] = {0};
  result = read_all(response_fd, response, MAX_RESPONSE_SIZE-1, &intr);
  if (result == -1) {
    if (intr) {
      fprintf(stderr, "Read was interrupted\n");
    } else {
      fprintf(stderr, "Failed to read from request pipe\n");
    }
    return 1;
  }

  // Close pipes
  close(request_fd);
  close(response_fd);
  close(notification_fd);

  // Unlink pipes
  unlink(req_path);
  unlink(resp_path);
  unlink(notif_path);

  return 0;
}

int kvs_connect(char const* req_pipe_path, char const* resp_pipe_path, char const* server_pipe_path,
                char const* notif_pipe_path) {
  int server_fd, result, intr = 0;

  // Store the paths
  strncpy(req_path, req_pipe_path, MAX_PIPE_PATH_LENGTH - 1);
  req_path[MAX_PIPE_PATH_LENGTH] = '\0';
  strncpy(resp_path, resp_pipe_path, MAX_PIPE_PATH_LENGTH - 1);
  resp_path[MAX_PIPE_PATH_LENGTH] = '\0';
  strncpy(notif_path, notif_pipe_path, MAX_PIPE_PATH_LENGTH - 1);
  notif_path[MAX_PIPE_PATH_LENGTH] = '\0';

  // Unlink pipes if they already exist
  unlink(req_path);
  unlink(resp_path);
  unlink(notif_path);

  // Create pipes for request, response and notification
  if (mkfifo(req_path, 0777) == -1) {
    fprintf(stderr, "Failed to create request pipe\n");
    return 1;
  }
  if (mkfifo(resp_path, 0777) == -1) {
    fprintf(stderr, "Failed to create response pipe\n");
    return 1;
  }
  if (mkfifo(notif_path, 0777) == -1) {
    fprintf(stderr, "Failed to create notification pipe\n");
    return 1;
  }

  // Open server pipe
  server_fd = open(server_pipe_path, O_WRONLY);
  if (server_fd == -1) {
      perror("Failed to open server pipe");
      return 1;
  }

  // Send registration message
  char registration_message[BUFFER_SIZE] = {0};
  snprintf(registration_message, BUFFER_SIZE, "1%-40.40s%-40.40s%-40.40s", req_path, resp_path, notif_path);
  if (write_all(server_fd, registration_message, BUFFER_SIZE-1) == -1) {
    fprintf(stderr, "Failed to send registration message\n");
    close(server_fd);
    return 1;
  }

  // Open pipes
  request_fd = open(req_path, O_WRONLY);
  if (request_fd == -1) {
    fprintf(stderr, "Failed to open request pipe\n");
    close(server_fd);
    return 1;
  }
  response_fd = open(resp_path, O_RDWR);
  if (response_fd == -1) {
    fprintf(stderr, "Failed to open response pipe\n");
    close(request_fd);
    close(server_fd);
    return 1;
  }
  notification_fd = open(notif_path, O_RDWR);
  if (notification_fd == -1) {
    fprintf(stderr, "Failed to open notification pipe\n");
    close(request_fd);
    close(response_fd);
    close(server_fd);
    return 1;
  }

  // Read message
  char response[MAX_RESPONSE_SIZE] = {0};
  result = read_all(response_fd, response, MAX_RESPONSE_SIZE-1, &intr);
  if (result == -1) {
    if (intr) {
      fprintf(stderr, "Read was interrupted\n");
    } else {
      fprintf(stderr, "Failed to read from request pipe\n");
    }
    close(request_fd);
    close(response_fd);
    close(notification_fd);
    return 1;
  }

  close(server_fd);

  // Check response
  return response[1] == '0' ? 0 : 1;
}

int kvs_subscribe(char const* key) {
  int result, intr = 0;

  if (request_fd == -1 || response_fd == -1) {
    fprintf(stderr, "Not connected to server\n");
    return 1;
  }

  // Send subscribe message
  char subscribe_message[BUFFER_SIZE_ACONNECT] = {0};
  snprintf(subscribe_message, BUFFER_SIZE_ACONNECT, "3%-41.41s", key);
  if (write_all(request_fd, subscribe_message, BUFFER_SIZE_ACONNECT-1) == -1) {
    fprintf(stderr, "Failed to send subscribe message\n");
    return 1;
  }

  // Wait for server response
  char response[MAX_RESPONSE_SIZE] = {0};
  result = read_all(response_fd, response, MAX_RESPONSE_SIZE-1, &intr);
  if (result == -1) {
    if (intr) {
      fprintf(stderr, "Read was interrupted\n");
      if (kvs_disconnect()) {
        fprintf(stderr, "Failed to disconnect\n");
      }
    } else {
      fprintf(stderr, "Failed to read from request pipe\n");
    }
    return 1;
  }

  // Check response
  return response[1] == '0' ? 0 : 1;
}

int kvs_unsubscribe(char const* key) {
  int result, intr = 0;

  if (request_fd == -1 || response_fd == -1) {
    fprintf(stderr, "Not connected to server\n");
    return 1;
  }

  // Send unsubscribe message
  char unsubscribe_message[BUFFER_SIZE_ACONNECT] = {0};
  snprintf(unsubscribe_message, BUFFER_SIZE_ACONNECT, "4%-41.41s", key);
  if (write_all(request_fd, unsubscribe_message, BUFFER_SIZE_ACONNECT-1) == -1) {
    fprintf(stderr, "Failed to send unsubscribe message\n");
    return 1;
  }

  // Wait for server response
  char response[MAX_RESPONSE_SIZE] = {0};
  result = read_all(response_fd, response, MAX_RESPONSE_SIZE-1, &intr);
  if (result == -1) {
    if (intr) {
      fprintf(stderr, "Read was interrupted\n");
      if (kvs_disconnect()) {
        fprintf(stderr, "Failed to disconnect\n");
      }
    } else {
      fprintf(stderr, "Failed to read from request pipe\n");
    }
    return 1;
  }

  // Check response
  return response[1] == '0' ? 0 : 1;
}

void* kvs_notifications(void* arg) {
  (void)arg;
  int result, intr = 0;

  if (notification_fd == -1) {
    fprintf(stderr, "Not connected to server\n");
    return NULL;
  }

  // Read notification
  char notification[MAX_WRITE_SIZE_RESPONSE] = {0};
  while (1) {
    result = read_all(notification_fd, notification, MAX_WRITE_SIZE_RESPONSE-1, &intr);
    if (result == -1) {
      if (intr) {
        fprintf(stderr, "Read was interrupted\n");
        if (kvs_disconnect()) {
          fprintf(stderr, "Failed to disconnect\n");
        }
      } else {
        fprintf(stderr, "Failed to read from notification pipe\n");
      }
      return NULL;
    }

    // Print notification
    char printstring[MAX_WRITE_SIZE_RESPONSE+1] = {0};
    char *token;

    token = strtok(notification, " ");
    while (token != NULL) {
      strcat(printstring, token);
      token = strtok(NULL, " ");
    }

    printstring[strlen(printstring)] = '\n';
    printstring[strlen(printstring)+1] = '\0';
    write_str(STDOUT_FILENO, printstring);
  }

  return NULL;
}


