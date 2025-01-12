#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "api.h"
#include "src/common/io.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"


static char req_path[MAX_PIPE_PATH_LENGTH];
static char resp_path[MAX_PIPE_PATH_LENGTH];
static char notif_path[MAX_PIPE_PATH_LENGTH];

static int request_fd = -1;
static int response_fd = -1;
static int notification_fd = -1;

int kvs_disconnect(void) {
  int result, intr = 0;
  
  // Send disconnect message
  char message[MAX_SIZE_OPCODE] = {OP_CODE_DISCONNECT + '0', '\0'};
  if (write_all(request_fd, message, MAX_SIZE_OPCODE - 1) == -1) {
    fprintf(stderr, "Failed to write to request pipe\n");
    return 1;
  }

  // Wait for server response
  char response[MAX_RESPONSE_SIZE] = {0};
  if ((result = read_all(response_fd, response, MAX_RESPONSE_SIZE - 1, &intr)) == -1) {
    if (intr) {
      fprintf(stderr, "Read was interrupted (Pipe closed)\n");
    } else {
      fprintf(stderr, "Failed to read from request pipe\n");
      return 1;
    }
  } else if (result == 0) {
    fprintf(stderr, "Server disconnected\n");
  }

  if (response[1] - '0' != OP_CODE_OK_CDU) {
    fprintf(stderr, "Server failed to disconnect\n");
    return 1;
  }

  // Close pipes
  if (close(request_fd) == -1) {
    fprintf(stderr, "Failed to close request pipe\n");
    return 1;
  }
  if (close(response_fd) == -1) {
    fprintf(stderr, "Failed to close response pipe\n");
    return 1;
  }
  if (close(notification_fd) == -1) {
    fprintf(stderr, "Failed to close notification pipe\n");
    return 1;
  }

  // Unlink pipes
  unlink(req_path);
  unlink(resp_path);
  unlink(notif_path);

  return 0;
}

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *server_pipe_path, char const *notif_pipe_path) {
  int server_fd, result = 0, intr = 0;

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

  // Send connection message
  char rmessage[BUFFER_SIZE] = {0};
  rmessage[0] = OP_CODE_CONNECT + '0';
  strncpy(rmessage + 1, req_path, MAX_PIPE_PATH_LENGTH);
  strncpy(rmessage + 1 + MAX_PIPE_PATH_LENGTH, resp_path, MAX_PIPE_PATH_LENGTH);
  strncpy(rmessage + 1 + 2 * MAX_PIPE_PATH_LENGTH, notif_path, MAX_PIPE_PATH_LENGTH);
  memset(rmessage + 1 + 3 * MAX_PIPE_PATH_LENGTH, '\0', BUFFER_SIZE - 1 - 3 * MAX_PIPE_PATH_LENGTH);
  if (write_all(server_fd, rmessage, BUFFER_SIZE-1) == -1) {
    fprintf(stderr, "Failed to write to server pipe\n");
    close(server_fd);
    return 1;
  }

  close(server_fd);

  // Open pipes
  response_fd = open(resp_path, O_RDONLY);
  if (response_fd == -1) {
    fprintf(stderr, "Failed to open response pipe\n");
    close(request_fd);
    return 1;
  }

  request_fd = open(req_path, O_WRONLY);
  if (request_fd == -1) {
    fprintf(stderr, "Failed to open request pipe\n");
    return 1;
  }

  notification_fd = open(notif_path, O_RDONLY);
  if (notification_fd == -1) {
    fprintf(stderr, "Failed to open notification pipe\n");
    close(request_fd);
    close(response_fd);
    return 1;
  }

  // Read response
  char response[MAX_RESPONSE_SIZE] = {0};
  result = read_all(response_fd, response, MAX_RESPONSE_SIZE - 1, &intr);
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

  // Check response
  char responsev = response[1] - '0';
  fprintf(stdout, "Server returned %d for operation: connect\n", responsev);
  return responsev == OP_CODE_OK_CDU ? 0 : 1;
}

int kvs_subscribe(const char *key) {
  int result, intr = 0;

  if (request_fd == -1 || response_fd == -1) {
    fprintf(stderr, "Not connected to server\n");
    return 1;
  }

  // Send subscribe message
  char message[BUFFER_SIZE_UNS] = {0};
  message[0] = OP_CODE_SUBSCRIBE + '0';
  strncpy(message + 1, key, MAX_STRING_SIZE+1);
  memset(message + 1 + MAX_STRING_SIZE + 1, '\0', BUFFER_SIZE_UNS - 1 - MAX_STRING_SIZE - 1);
  if (write_all(request_fd, message, BUFFER_SIZE_UNS - 2) == -1) {
    fprintf(stderr, "Failed to write to request pipe\n");
    return 1;
  }

  // Wait for response
  char response[MAX_RESPONSE_SIZE] = {0};
  if ((result = read_all(response_fd, response, MAX_RESPONSE_SIZE - 1, &intr)) == -1) {
    if (intr) {
      fprintf(stderr, "Read was interrupted (Pipe closed)\n");
      if (kvs_disconnect()) {
        fprintf(stderr, "Failed to disconnect\n");
      }
      return 1;
    } 
    fprintf(stderr, "Failed to read from request pipe\n");
  } else if (result == 0) {
    fprintf(stderr, "Server disconnected\n");
    if (kvs_disconnect()) {
      fprintf(stderr, "Failed to disconnect\n");
    }
    return 1;
  }
  
  // Check response
  char responsev = response[1] - '0';
  fprintf(stdout, "Server returned %d for operation: subscribe\n", responsev);
  return responsev == OP_CODE_OK_S ? 0 : 1;
}

int kvs_unsubscribe(const char *key) {
  int result, intr = 0;

  if (request_fd == -1 || response_fd == -1) {
    fprintf(stderr, "Not connected to server\n");
    return 1;
  }

  // Send subscribe message
  char message[BUFFER_SIZE_UNS] = {0};
  message[0] = OP_CODE_UNSUBSCRIBE + '0';
  strncpy(message + 1, key, MAX_STRING_SIZE+1);
  memset(message + 1 + MAX_STRING_SIZE + 1, '\0', BUFFER_SIZE_UNS - 1 - MAX_STRING_SIZE - 1);
  if (write_all(request_fd, message, BUFFER_SIZE_UNS - 2) == -1) {
    fprintf(stderr, "Failed to write to request pipe\n");
    return 1;
  }

  // Wait for response
  char response[MAX_RESPONSE_SIZE] = {0};
  if ((result = read_all(response_fd, response, MAX_RESPONSE_SIZE - 1, &intr)) == -1) {
    if (intr) {
      fprintf(stderr, "Read was interrupted (Pipe closed)\n");
      if (kvs_disconnect()) {
        fprintf(stderr, "Failed to disconnect\n");
      }
      return 1;
    } 
    fprintf(stderr, "Failed to read from request pipe\n");
  } else if (result == 0) {
    fprintf(stderr, "Server disconnected\n");
    if (kvs_disconnect()) {
      fprintf(stderr, "Failed to disconnect\n");
    }
    return 1;
  }
  
  // Check response
  char responsev = response[1] - '0';
  fprintf(stdout, "Server returned %d for operation: unsubscribe\n", responsev);
  return responsev == OP_CODE_OK_CDU ? 0 : 1;
}
