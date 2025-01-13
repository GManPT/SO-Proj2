#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <errno.h>
#include <signal.h>

#include "constants.h"
#include "../common/constants.h"
#include "../common/io.h"
#include "io.h"
#include "../common/protocol.h"
#include "operations.h"
#include "parser.h"
#include "pthread.h"
#include "client.h"

struct SharedData {
  DIR *dir;
  char *dir_name;
  pthread_mutex_t directory_mutex;
};

/// Mutex for protecting the KVS from concurrent access.
pthread_mutex_t kvs_lock = PTHREAD_MUTEX_INITIALIZER;
/// Mutex for protecting the current backup state.
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

size_t active_backups = 0; // Number of active backups
size_t max_backups;        // Maximum allowed simultaneous backups
size_t max_threads;        // Maximum allowed simultaneous threads
char *jobs_directory = NULL;

char regist_fifo_name[MAX_PIPE_PATH_LENGTH]; // FIFO of registration

/// Handles the SIGUSR1 signal by disconnecting all clients.
/// @param signal The signal number (not used in this function).
void handle_sigusr1() {
  disconnect_all_clients();
}

/// Processes the entries in a given directory.
/// @param dir The directory being processed.
/// @param entry The current directory entry to be processed.
/// @param in_path The path to the input directory or file.
/// @param out_path The path to the output directory or file.
/// @return A status code indicating the success or failure of the operation.
static int entry_files(const char *dir, struct dirent *entry, char *in_path,
                       char *out_path) {
  const char *dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 ||
      strcmp(dot, ".job")) {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

/// Executes a job based on the provided input and output file descriptors, and a specified filename.
/// @param in_fd The file descriptor for reading input data.
/// @param out_fd The file descriptor for writing output data.
/// @param filename The name of the file associated with the job.
/// @return A status code indicating the success or failure of the job execution.
static int run_job(int in_fd, int out_fd, char *filename) {
  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
    case CMD_WRITE:
      num_pairs =
          parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_write(num_pairs, keys, values)) {
        write_str(STDERR_FILENO, "Failed to write pair\n");
      }
      break;

    case CMD_READ:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_read(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to read pair\n");
      }
      break;

    case CMD_DELETE:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_delete(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to delete pair\n");
      }
      break;

    case CMD_SHOW:
      kvs_show(out_fd);
      break;

    case CMD_WAIT:
      if (parse_wait(in_fd, &delay, NULL) == -1) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay > 0) {
        printf("Waiting %d seconds\n", delay / 1000);
        kvs_wait(delay);
      }
      break;

    case CMD_BACKUP:
      pthread_mutex_lock(&n_current_backups_lock);
      if (active_backups >= max_backups) {
        wait(NULL);
      } else {
        active_backups++;
      }
      pthread_mutex_unlock(&n_current_backups_lock);
      int aux = kvs_backup(++file_backups, filename, jobs_directory);

      if (aux < 0) {
        write_str(STDERR_FILENO, "Failed to do backup\n");
      } else if (aux == 1) {
        return 1;
      }
      break;

    case CMD_INVALID:
      write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
      break;

    case CMD_HELP:
      write_str(STDOUT_FILENO,
                "Available commands:\n"
                "  WRITE [(key,value)(key2,value2),...]\n"
                "  READ [key,key2,...]\n"
                "  DELETE [key,key2,...]\n"
                "  SHOW\n"
                "  WAIT <delay_ms>\n"
                "  BACKUP\n" // Not implemented
                "  HELP\n");

      break;

    case CMD_EMPTY:
      break;

    case EOC:
      printf("EOF\n");
      return 0;
    }
  }
}

/// Frees arguments and processes files in a given directory.
/// @param arguments A pointer to the `SharedData` structure containing directory-related data.
/// @return NULL
static void *get_file(void *arguments) {
  struct SharedData *thread_data = (struct SharedData *)arguments;
  DIR *dir = thread_data->dir;
  char *dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent *entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out) {
      if (closedir(dir) == -1) {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      _exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}

/// Handles FIFO communication for client registration and connection.
/// @return None
void handle_fifo() {
  int regist_fifo, intr, result;
  int request_fd, response_fd, notification_fd;
  char buffer[BUFFER_SIZE] = {0};
  char rw[3] = {OP_CODE_CONNECT + '0', OP_CODE_ERROR_CDU + '0', '\0'};

  // Set up signal handler
  struct sigaction sa;
  sa.sa_handler = handle_sigusr1;
  sa.sa_flags = SA_RESTART;
  sigemptyset(&sa.sa_mask);
  if (sigaction(SIGUSR1, &sa, NULL) == -1) {
    fprintf(stderr, "Erro ao configurar signal handler\n");
    return;
  }

  // Open FIFO
  regist_fifo = open(regist_fifo_name, O_RDONLY);
  if (regist_fifo == -1) {
    fprintf(stderr, "Failed to open registration FIFO\n");
    return;
  }

  while (1) {
    result = read_all(regist_fifo, buffer, BUFFER_SIZE-1, &intr);
    if (result == -1 || result == 0) {
      if (intr) intr = 0;
      continue;
    }
    if (errno == EINTR) continue;

    char op_code = buffer[0] - '0';
    if (op_code == OP_CODE_CONNECT) {
      // Parse message
      char request[MAX_PIPE_PATH_LENGTH + 1] = {0};
      char response[MAX_PIPE_PATH_LENGTH + 1] = {0};
      char notification[MAX_PIPE_PATH_LENGTH + 1] = {0};
      
      // Skip op_code
      const char *ptr = buffer + 1;
      strncpy(request, ptr, MAX_PIPE_PATH_LENGTH);
      request[MAX_PIPE_PATH_LENGTH - 1] = '\0';
      ptr += MAX_PIPE_PATH_LENGTH;

      strncpy(response, ptr, MAX_PIPE_PATH_LENGTH);
      response[MAX_PIPE_PATH_LENGTH - 1] = '\0';
      ptr += MAX_PIPE_PATH_LENGTH;

      strncpy(notification, ptr, MAX_PIPE_PATH_LENGTH);
      notification[MAX_PIPE_PATH_LENGTH - 1] = '\0';

      trim_trailing_whitespace(request);
      trim_trailing_whitespace(response);
      trim_trailing_whitespace(notification);

      // Open pipes
      response_fd = open(response, O_WRONLY);
      if (response_fd == -1) {
        fprintf(stderr, "Failed to open response pipe\n");
        continue;
      }
      
      request_fd = open(request, O_RDONLY);
      if (request_fd == -1) {
        fprintf(stderr, "Failed to open request pipe\n");
        if (write_all(response_fd, rw, MAX_RESPONSE_SIZE-1) == -1) {
          fprintf(stderr, "Failed to write to response pipe\n");
        }
        close(response_fd);
        continue;
      }

      notification_fd = open(notification, O_WRONLY);
      if (notification_fd == -1) {
        fprintf(stderr, "Failed to open notification pipe\n");
        if (write_all(response_fd, rw, MAX_RESPONSE_SIZE-1) == -1) {
          fprintf(stderr, "Failed to write to response pipe\n");
        }
        close(request_fd);
        close(response_fd);
        continue;
      }

      if (activate_client(request_fd, response_fd, notification_fd) != 0) {
        fprintf(stderr, "Failed to connect client\n");
        if (write_all(response_fd, rw, MAX_RESPONSE_SIZE-1) == -1) {
          fprintf(stderr, "Failed to write to response pipe\n");
        }
        close(request_fd);
        close(response_fd);
        close(notification_fd);
        continue;
      }
    }
  }
  // Never reached
  close(regist_fifo);
}

/// Dispatches threads to process files in a directory and handles FIFO communication.
/// @param dir A pointer to the directory stream that is passed to each thread for processing files.
/// @return None
static void dispatch_threads(DIR *dir) {
  pthread_t *threads = malloc(max_threads * sizeof(pthread_t));

  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  struct SharedData thread_data = {dir, jobs_directory,
                                   PTHREAD_MUTEX_INITIALIZER};

  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void *)&thread_data) !=
        0) {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  handle_fifo();

  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
  }
  
  free(threads);
}

int main(int argc, char **argv) {
  if (argc < 5) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
		write_str(STDERR_FILENO, " <max_threads>");
		write_str(STDERR_FILENO, " <max_backups>");
    write_str(STDERR_FILENO, " <fifo_name>\n");
    return 1;
  }

  jobs_directory = argv[1];

  char *endptr;
  max_backups = strtoul(argv[3], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_proc value\n");
    return 1;
  }

  max_threads = strtoul(argv[2], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_threads value\n");
    return 1;
  }

  if (max_backups <= 0) {
    write_str(STDERR_FILENO, "Invalid number of backups\n");
    return 0;
  }

  if (max_threads <= 0) {
    write_str(STDERR_FILENO, "Invalid number of threads\n");
    return 0;
  }

  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  DIR *dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }

  // Handle FIFO
  snprintf(regist_fifo_name, MAX_PIPE_PATH_LENGTH, "%s%s", TEMP_FOLDER, argv[4]);
  if (fifo_init(regist_fifo_name)) {
    write_str(STDERR_FILENO, "Failed to initialize fifo\n");
    return 1;
  }
  if (start_client_threads() != 0) {
    write_str(STDERR_FILENO, "Failed to start client threads\n");
    return 1;
  }

  dispatch_threads(dir);

  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 0;
  }

  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }

  // Terminate KVS (never reached)
  kvs_terminate();
  unlink(regist_fifo_name);
  return 0;
}
