#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <stdio.h>
#include <errno.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"
#include "io.h"
#include "pthread.h"

struct SharedData {
  DIR* dir;
  char* dir_name;
  pthread_mutex_t directory_mutex;
};

typedef struct client {
  int id;
  char notify_fifo_name[MAX_PIPE_NAME_SIZE];
  char request_fifo_name[MAX_PIPE_NAME_SIZE];
  char response_fifo_name[MAX_PIPE_NAME_SIZE];
  int notify_fifo, request_fifo, response_fifo;
  KeysSubscribedList* keys_subscribed;
} Client;

typedef struct keys_subscribed_list {
  char subscribe_key[MAX_KEY_NAME_SIZE];
  keys_subscribed_list* next;
} KeysSubscribedList;

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

size_t active_backups = 0;     // Number of active backups
size_t max_backups;            // Maximum allowed simultaneous backups
size_t max_threads;            // Maximum allowed simultaneous threads
size_t active_client_connections = 0;
char* jobs_directory = NULL;

char regist_fifo_name[MAX_PIPE_NAME_SIZE];
int regist_fifo;

static int entry_files(const char* dir, struct dirent* entry, char* in_path, char* out_path) {
  const char* dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 || strcmp(dot, ".job")) {
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

static int run_job(int in_fd, int out_fd, char* filename) {
  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
      case CMD_WRITE:
        num_pairs = parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_write(num_pairs, keys, values)) {
          write_str(STDERR_FILENO, "Failed to write pair\n");
        }
        break;

      case CMD_READ:
        num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_read(num_pairs, keys, out_fd)) {
          write_str(STDERR_FILENO, "Failed to read pair\n");
        }
        break;

      case CMD_DELETE:
        num_pairs = parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

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

//frees arguments
static void* get_file(void* arguments) {
  struct SharedData* thread_data = (struct SharedData*) arguments;
  DIR* dir = thread_data->dir;
  char* dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent* entry;
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

      exit(0);
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

char* read_msg(int fifo_fd, size_t buffer_size) {
  char buf[buffer_size];
  while ((bytesRead = read(fifo_fd, buf, sizeof(buf) - 1)) > 0) {
    buf[bytesRead] = '\0';
  }
  return buf;
}

int write_msg(int fifo_fd, const char *msg) {
  if (msg == NULL || strlen(msg) == 0) {
    fprintf(stderr, "Message is empty or NULL\n");
    return 1;
  }
  write_str(fifo_fd, msg);
  return 0;
}

Client add_client_info(int fifo_fd) {
  char* client_pipes_names = read_msg(fifo_fd, (MAX_PIPE_NAME_SIZE+1) * 3 + 1);
  Client client = malloc(sizeof(Client));
  sscanf(client_pipes_names, "|%s|%s|%s", client.notify_fifo_name, client.request_fifo_name, client.response_fifo_name);
  client.notify_fifo = open(client.notify_fifo_name, O_WRONLY | O_NONBLOCK);
  if (client.notify_fifo < 0) {
    write_str(STDERR_FILENO, "Failed to open FIFO\n");
    free(client);
    return NULL;
  }
  client.request_fifo = open(client.request_fifo_name, O_RDONLY | O_NONBLOCK);
  if (client.request_fifo < 0) {
    write_str(STDERR_FILENO, "Failed to open FIFO\n");
    close(client.notify_fifo);
    free(client);
    return NULL;
  }
  client.response_fifo = open(client.response_fifo_name, O_WRONLY | O_NONBLOCK);
  if (client.response_fifo < 0) {
    write_str(STDERR_FILENO, "Failed to open FIFO\n");
    close(client.notify_fifo);
    close(client.request_fifo);
    free(client);
    return NULL;
  }
  int result = client != NULL ? 0 : 1;
  char msg[RESPONSE_CONNECT_DISCONNECT_SIZE];
  snprintf(msg, RESPONSE_CONNECT_DISCONNECT_SIZE, "%c|%i", opcode, result);
  write_msg(client.response_fifo, msg);
  return client;
}

void close_client_connection(Client client) {
  int result = client != NULL ? 0 : 1;
  char msg[RESPONSE_CONNECT_DISCONNECT_SIZE];
  snprintf(msg, RESPONSE_CONNECT_DISCONNECT_SIZE, "%c|%i", opcode, result);
  write_msg(client.response_fifo, msg);
  close(client.notify_fifo);
  close(client.request_fifo);
  close(client.response_fifo);
  free(client);
}

int subscribe_key(Client client, char const* key) {
  char* value = read_pair(kvs_table, key);
  int response_code = value != NULL ? 1 : 0;
  write_msg(client.response_fifo, response_code);
  while (key)
}

void* process_messages() {
  char opcode;
  Client client = NULL;
  int fifo_fd;
  int regist_client = 0;
  while (1) {
    fifo_fd = !regist_client ? regist_fifo : client.request_fifo;
    ssize_t bytesRead = read(fifo_fd, &opcode, sizeof(opcode));
    if (bytesRead <= 0) return NULL;
    if (!regist_client && opcode != '1') continue;
    if (opcode == '1' && !regist_client) {
      client = add_client_info(regist_fifo);
      regist_client = 1;
    }
    else if (opcode == '2') {
      close_client_connection();
      return NULL;
    }
    else if (opcode == '3') {
      read_msg(fifo_fd, MAX_KEY_NAME_SIZE + 1);
      subscribe_key(client, msg);
    } else if (opcode == '4') {
      read_msg(fifo_fd, MAX_KEY_NAME_SIZE + 1);
      unsubsribe_key(client, msg);
    } else {
      fprintf(stderr, "Invalid Message OP_CODE\n");
    }
  }
}


static void dispatch_threads(DIR* dir) {
  pthread_t* threads = malloc(max_threads * sizeof(pthread_t));

  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  struct SharedData thread_data = {dir, jobs_directory, PTHREAD_MUTEX_INITIALIZER};


  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void*)&thread_data) != 0) {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  // Ler do FIFO
  pthread_t client_thread;
  if (pthread_create(&client_thread, NULL, process_messages, NULL) != 0) {
    fprintf(stderr, "Failed to create client thread\n");
    pthread_mutex_destroy(&thread_data.directory_mutex);
    free(threads);
    return;
  }

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


int main(int argc, char** argv) {
  if (argc < 5) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
		write_str(STDERR_FILENO, " <max_threads>");
		write_str(STDERR_FILENO, " <max_backups> \n");
    write_str(STDERR_FILENO, " <nome_do_FIFO_de_registo> \n");
    return 1;
  }

  jobs_directory = argv[1];

  char* endptr;
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
  
  snprintf(regist_fifo_name, MAX_PIPE_NAME_SIZE, "%s%s", TEMP_FOLDER, argv[4]);
  fprintf(stderr, "%s\n", regist_fifo_name);

  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  DIR* dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }

  // Create FIFO
  if (mkfifo(regist_fifo_name, 0777) < 0) {
    if (errno == EEXIST) {
      write_str(STDERR_FILENO, "FIFO already exists\n");
    } else {
      write_str(STDERR_FILENO, "Failed to create FIFO\n");
      return 1;
    }
  }
  regist_fifo = open(regist_fifo_name, O_RDONLY | O_NONBLOCK);
  if (regist_fifo < 0) {
    write_str(STDERR_FILENO, "Failed to open FIFO\n");
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

  kvs_terminate();
  close(regist_fifo);
  unlink(regist_fifo_name);
  return 0;
}

kill(client->client_pid, SIGTERM);