#include <unistd.h>
#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <stdio.h>
#include <errno.h>
#include <semaphore.h>
#include <signal.h>

#include "constants.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"
#include "parser.h"
#include "operations.h"
#include "io.h"
#include "src/common/io.h"
#include "pthread.h"

struct SharedData {
  DIR* dir;
  char* dir_name;
  pthread_mutex_t directory_mutex;
};

struct ClientData {
  int fds[3];
  char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE + 1];
  int num_keys;
  pthread_mutex_t keys_mutex;
  struct ClientData* next;
};

struct ClientList {
  struct ClientData* head;
  pthread_mutex_t list_mutex;
};

pthread_mutex_t kvs_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

size_t active_backups = 0;     // Number of active backups
size_t max_backups;            // Maximum allowed simultaneous backups
size_t max_threads;            // Maximum allowed simultaneous threads
char* jobs_directory = NULL;

char regist_fifo_name[MAX_PIPE_PATH_LENGTH]; // Name of the FIFO
sem_t client_sem; // Semaphore to control the number of threads
struct ClientList client_list = {NULL, PTHREAD_MUTEX_INITIALIZER}; // List of clients

void handle_sigusr1(int) {
  // Block more SIGUSR1 signals
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  sigprocmask(SIG_BLOCK, &set, NULL);

  // Lock the KVS mutex to block run_job threads
  if (pthread_mutex_lock(&kvs_lock) != 0) {
    fprintf(stderr, "Failed to lock kvs_lock\n");
    return;
  }

  if (pthread_mutex_lock(&client_list.list_mutex) != 0) {
    fprintf(stderr, "Failed to lock list_mutex\n");
    pthread_mutex_unlock(&kvs_lock);
    return;
  }

  struct ClientData* current = client_list.head;
  struct ClientData* temp;

  // Iterate over the client list and free all clients
  while (current != NULL) {
    for (int i = 0; i < 3; i++) {
      close(current->fds[i]);
      current->fds[i] = -1;
    }

    if (pthread_mutex_destroy(&current->keys_mutex) != 0) {
      fprintf(stderr, "Failed to destroy keys_mutex\n");
    }

    // Free the client data structure
    temp = current;
    current = current->next;
    free(temp);
  }
  client_list.head = NULL;

  // Unlock the list mutex
  if (pthread_mutex_unlock(&client_list.list_mutex) != 0) {
    fprintf(stderr, "Failed to unlock list_mutex\n");
  }

  // Destroy and reinitialize the client semaphore
  if (sem_destroy(&client_sem) == -1) {
    fprintf(stderr, "Failed to destroy semaphore\n");
  }
  if (sem_init(&client_sem, 0, MAX_SESSION_COUNT) == -1) {
    fprintf(stderr, "Failed to initialize semaphore\n");
  }

  // Terminate and reinitialize the KVS
  kvs_terminate();
  kvs_init();

  // Unlock the KVS mutex
  if (pthread_mutex_unlock(&kvs_lock) != 0) {
    fprintf(stderr, "Failed to unlock kvs_lock\n");
  }

  sigprocmask(SIG_UNBLOCK, &set, NULL);
}

static int client_disconnect(struct ClientData* client) {
  // Remove client from list
  if (pthread_mutex_lock(&client_list.list_mutex) != 0) {
    fprintf(stderr, "Failed to lock list_mutex\n");
    if (write_all(client->fds[1], "21", MAX_RESPONSE_SIZE-1) == -1) {
      fprintf(stderr, "Failed to send response\n");
    }
    return 1;
  }
  
  struct ClientData* current = client_list.head;
  struct ClientData* prev = NULL;
  while (current != NULL) {
    if (current == client) {
      if (prev == NULL) {
        client_list.head = current->next;
      } else {
        prev->next = current->next;
      }
      break;
    }
    prev = current;
    current = current->next;
  }

  if (pthread_mutex_unlock(&client_list.list_mutex) != 0) {
    fprintf(stderr, "Failed to unlock list_mutex\n");
    if (write_all(client->fds[1], "21", MAX_RESPONSE_SIZE-1) == -1) {
      fprintf(stderr, "Failed to send response\n");
    }
    return 1;
  }

  // Free client data
  if (pthread_mutex_destroy(&client->keys_mutex) != 0) {
    fprintf(stderr, "Failed to destroy keys_mutex\n");
    if (write_all(client->fds[1], "21", MAX_RESPONSE_SIZE-1) == -1) {
      fprintf(stderr, "Failed to send response\n");
    }
    return 1;
  }

  // Send response
  if (write_all(client->fds[1], "20", MAX_RESPONSE_SIZE-1) == -1) {
    fprintf(stderr, "Failed to send response\n");
    return 1;
  }

  // Close pipes
  close(client->fds[0]);
  close(client->fds[1]);
  close(client->fds[2]);

  sem_post(&client_sem);
  free(client);
  return 0;
}

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
        write_str(STDOUT_FILENO, "End of file\n");
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

void notify_clients(const char* key, const char* value) {
  if (pthread_mutex_lock(&client_list.list_mutex) != 0) {
    fprintf(stderr, "Failed to lock list_mutex. Could not notify clients\n");
    return;
  }

  char notification[MAX_WRITE_SIZE_RESPONSE] = {0};

  // Iterate over clients
  struct ClientData* current = client_list.head;
  while (current != NULL) {
    if (pthread_mutex_lock(&current->keys_mutex) != 0) {
      fprintf(stderr, "Failed to lock keys_mutex. Could not notify client.\n");
      break;
    }

    for (int i = 0; i < current->num_keys; i++) {
      if (strcmp(current->keys[i], key) == 0) {
        if (value) {
          snprintf(notification, MAX_WRITE_SIZE_RESPONSE, "(%-40.40s,%-256.256s)", key, value);
        } else {
          snprintf(notification, MAX_WRITE_SIZE_RESPONSE, "(%-40.40s,DELETED%-249.249s)", key, "");
        }
        
        if (current->fds[2] == -1) {
          break;
        }

        if (write_all(current->fds[2], notification, MAX_WRITE_SIZE_RESPONSE-1) == -1) {
          fprintf(stderr, "Failed to send notification\n");
        }

        break;
      }
    }

    if (pthread_mutex_unlock(&current->keys_mutex) != 0) {
      fprintf(stderr, "Failed to unlock keys_mutex\n");
      break;
    }
    current = current->next;
  }

  if (pthread_mutex_unlock(&client_list.list_mutex) != 0) {
    fprintf(stderr, "Failed to unlock list_mutex\n");
  }
}

// Callbacks for the clients
void register_callbacks() {
  register_write_callback(notify_clients);
  register_delete_callback(notify_clients);
}

void* handle_client(void* arg) {
  // Block SIGUSR1 in this thread
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  pthread_sigmask(SIG_BLOCK, &set, NULL);

  struct ClientData* data = (struct ClientData*)arg;
  int request_fd = data->fds[0];
  int response_fd = data->fds[1];

  // Sucess on connection
  if (write_all(response_fd, "10", MAX_RESPONSE_SIZE-1) == -1) {
    fprintf(stderr, "Failed to send response\n");
  }

  // Add client to list
  pthread_mutex_lock(&client_list.list_mutex);
  data->next = client_list.head;
  client_list.head = data;
  pthread_mutex_unlock(&client_list.list_mutex);

  char buffer[MAX_SIZE_OPCODE];
  char key[MAX_STRING_SIZE + 1] = {0};
  int result, key_exists, intr = 0;
  
  while (1) {
    // Read opcode from client
    if ((result = read_all(request_fd, buffer, MAX_SIZE_OPCODE-1, &intr)) == -1) {
      if (intr) {
        fprintf(stderr, "Read was interrupted\n");
        intr = 0;
      }
      fprintf(stderr, "Failed to read from client\n");
      continue;
    } else if (result == 0) {
      sem_post(&client_sem);
      break;
    }

    // Handle opcode
    char op_code = buffer[0] - '0';
    switch (op_code) {
      case OP_CODE_DISCONNECT:
        if (client_disconnect(arg)) {
          fprintf(stderr, "Failed to disconnect client\n");
        }
        pthread_exit(NULL);

      case OP_CODE_SUBSCRIBE:
        // Parse key
        if ((result = read_all(request_fd, key, MAX_STRING_SIZE+1, &intr)) == -1) {
          if (intr) {
            fprintf(stderr, "Read was interrupted\n");
            intr = 0;
            if (client_disconnect(arg)) {
              fprintf(stderr, "Failed to disconnect client\n");
            }
          }
          break;
        } else if (result == 0) {
          break;
        }
        strtok(key, " ");

        // 1st check: Key exists in KVS
        if (kvs_key_exists(key)) {
          if (write_all(response_fd, "31", MAX_RESPONSE_SIZE-1) == -1) {
            fprintf(stderr, "Failed to send response\n");
          }
          break;
        }

        // 2nd check: Key is already subscribed or max number of subscriptions reached
        if (pthread_mutex_lock(&data->keys_mutex) != 0) {
          fprintf(stderr, "Failed to lock keys_mutex\n");
          if (write_all(response_fd, "31", MAX_RESPONSE_SIZE-1) == -1) {
            fprintf(stderr, "Failed to send response\n");
          }
          break;
        }

        key_exists = 0;
        for (int i = 0; i < data->num_keys; i++) {
          if (strcmp(data->keys[i], key) == 0) {
            key_exists = 1;
            break;
          }
        }

        if (key_exists || data->num_keys > MAX_NUMBER_SUB) {
          if (write_all(response_fd, "31", MAX_RESPONSE_SIZE-1) == -1) {
            fprintf(stderr, "Failed to send response\n");
          }
          pthread_mutex_unlock(&data->keys_mutex);
          break;
        } else {
          strncpy(data->keys[data->num_keys], key, MAX_STRING_SIZE);
          data->num_keys++;
        }
        
        if (pthread_mutex_unlock(&data->keys_mutex) != 0) {
          fprintf(stderr, "Failed to unlock keys_mutex\n");
          if (write_all(response_fd, "31", MAX_RESPONSE_SIZE-1) == -1) {
            fprintf(stderr, "Failed to send response\n");
          }
          break;
        }

        // Send response of success
        if (write_all(response_fd, "30", MAX_RESPONSE_SIZE-1) == -1) {
          fprintf(stderr, "Failed to send response\n");
        }

        break;

      case OP_CODE_UNSUBSCRIBE:
        if ((result = read_all(request_fd, key, MAX_STRING_SIZE+1, &intr)) == -1) {
          if (intr) {
            fprintf(stderr, "Read was interrupted\n");
            intr = 0;
            continue;
          }
          fprintf(stderr, "Failed to read key\n");
          break;
        } else if (result == 0) {
          break; // TODO: Properly handle client disconnection
        }
        strtok(key, " ");

        // Check if key is subscribed and remove it
        if (pthread_mutex_lock(&data->keys_mutex) != 0) {
          fprintf(stderr, "Failed to lock keys_mutex\n");
          if (write_all(response_fd, "31", MAX_RESPONSE_SIZE-1) == -1) {
            fprintf(stderr, "Failed to send response\n");
          }
          break;
        }

        key_exists = 0;
        for (int i = 0; i < data->num_keys; i++) {
          if (strcmp(data->keys[i], key) == 0) {
            key_exists = 1;
            for (int j = i; j < data->num_keys - 1; j++) {
              strncpy(data->keys[j], data->keys[j+1], MAX_STRING_SIZE+1); // Check later (SIZE)
            }
            data->num_keys--;
            break;
          }
        }

        if (pthread_mutex_unlock(&data->keys_mutex) != 0) {
          fprintf(stderr, "Failed to unlock keys_mutex\n");
          if (write_all(response_fd, "31", MAX_RESPONSE_SIZE-1) == -1) {
            fprintf(stderr, "Failed to send response\n");
          }
          break;
        }

        write_all(response_fd, key_exists ? "40" : "41", MAX_RESPONSE_SIZE-1);
        break;
      
      default:
        fprintf(stderr, "Invalid opcode received: %c\n", buffer[0]);
        break;
    }
  }

  return NULL;
}

void* handle_fifo() {
  // Configurar máscara de sinais para esta thread
  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  pthread_sigmask(SIG_UNBLOCK, &set, NULL);  // Apenas esta thread recebe SIGUSR1

  // Configurar handler SIGUSR1
  struct sigaction sa;
  sa.sa_handler = handle_sigusr1;
  sigemptyset(&sa.sa_mask);
  sa.sa_flags = SA_RESTART;
  sigaction(SIGUSR1, &sa, NULL);

  int server_fd, intr, result;
  char buffer[BUFFER_SIZE] = {0};
  
  while(1) {
    // Open FIFO
    server_fd = open(regist_fifo_name, O_RDONLY);
    if (server_fd == -1) {
      fprintf(stderr, "Failed to open main FIFO\n");
      continue;
    }

    while(1) {
      // Read from FIFO
      result = read_all(server_fd, buffer, BUFFER_SIZE-1, &intr);
      if (result == -1) {
        if (intr) {
          fprintf(stderr, "Read was interrupted\n");
          intr = 0;
          continue;
        }
        fprintf(stderr, "Failed to read from FIFO\n");
        break;
      } else if (result == 0) {
        break;
      }

      char op_code = buffer[0] - '0';
      if (op_code == OP_CODE_CONNECT) {
        // Wait for client semaphore
        sem_wait(&client_sem);

        // Parse message
        char request[MAX_STRING_SIZE + 1] = {0};
        char response[MAX_STRING_SIZE + 1] = {0};
        char notification[MAX_STRING_SIZE + 1] = {0};

        strncpy(request, &buffer[1], MAX_STRING_SIZE);
        strncpy(response, &buffer[1 + MAX_STRING_SIZE], MAX_STRING_SIZE);
        strncpy(notification, &buffer[1 + 2 * MAX_STRING_SIZE], MAX_STRING_SIZE);
        strtok(request, " ");
        strtok(response, " ");
        strtok(notification, " ");

        // Open pipes
        int client_fds[3];
        if ((client_fds[0] = open(request, O_RDWR)) == -1) {
          fprintf(stderr, "Failed to open request FIFO\n");
          sem_post(&client_sem);
          break;
        }
        if ((client_fds[1] = open(response, O_WRONLY)) == -1) {
          fprintf(stderr, "Failed to open response FIFO\n");
          close(client_fds[0]);
          sem_post(&client_sem);
          break;
        }   
        if ((client_fds[2] = open(notification, O_WRONLY)) == -1) {
          fprintf(stderr, "Failed to open notification FIFO\n");
          close(client_fds[0]);
          close(client_fds[1]);
          sem_post(&client_sem);
          break;
        }

        // Allocate memory for client data
        struct ClientData* client_data = malloc(sizeof(struct ClientData));
        if (client_data == NULL) {
          fprintf(stderr, "Failed to allocate memory for client data\n");
          if (write_all(client_fds[1], "11", MAX_RESPONSE_SIZE-1) == -1) {
            fprintf(stderr, "Failed to send response\n");
          }
          close(client_fds[0]);
          close(client_fds[1]);
          close(client_fds[2]);
          sem_post(&client_sem);
          break;
        }
        memcpy(client_data->fds, client_fds, sizeof(client_fds));
        client_data->num_keys = 0;
        if (pthread_mutex_init(&client_data->keys_mutex, NULL) != 0) {
          fprintf(stderr, "Failed to initialize keys_mutex\n");
          if (write_all(client_fds[1], "11", MAX_RESPONSE_SIZE-1) == -1) {
            fprintf(stderr, "Failed to send response\n");
          }
          close(client_fds[0]);
          close(client_fds[1]);
          close(client_fds[2]);
          free(client_data);
          sem_post(&client_sem);
          break;
        }

        // Thread to handle client
        pthread_t client_thread;
        if (pthread_create(&client_thread, NULL, handle_client, (void*)client_data) != 0) {
          fprintf(stderr, "Failed to create client thread\n");
          if (write_all(client_fds[1], "11", MAX_RESPONSE_SIZE-1) == -1) {
            fprintf(stderr, "Failed to send response\n");
          }
          close(client_fds[0]);
          close(client_fds[1]);
          close(client_fds[2]);
          free(client_data);
          sem_post(&client_sem);
          break;
        }

        // Detach thread
        if (pthread_detach(client_thread) != 0) {
          fprintf(stderr, "Failed to detach client thread\n");
          if (write_all(client_fds[1], "11", MAX_RESPONSE_SIZE-1) == -1) {
            fprintf(stderr, "Failed to send response\n");
          }
          close(client_fds[0]);
          close(client_fds[1]);
          close(client_fds[2]);
          free(client_data);
          sem_post(&client_sem);
          break;
        }
      }
    }

    close(server_fd);
  }

  return NULL;
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
      fprintf(stderr, "Failed to create thread %lu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  // Start register callbacks
  register_callbacks();

  // Create FIFO handling thread
  pthread_t fifo_thread;
  if (pthread_create(&fifo_thread, NULL, handle_fifo, NULL) != 0) {
    fprintf(stderr, "Failed to create FIFO handling thread\n");
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

  // Wait for the FIFO handling thread to finish
  if (pthread_join(fifo_thread, NULL) != 0) {
    fprintf(stderr, "Failed to join FIFO handling thread\n");
  }

  free(threads);
}


int main(int argc, char** argv) {
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

  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  DIR* dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }

  // Handle fifo
  snprintf(regist_fifo_name, MAX_PIPE_PATH_LENGTH, "%s%s", TEMP_FOLDER, argv[4]);
  if (fifo_init(regist_fifo_name)) {
    write_str(STDERR_FILENO, "Failed to initialize fifo\n");
    return 1;
  }
  if (sem_init(&client_sem, 0, MAX_SESSION_COUNT) == -1) {
    write_str(STDERR_FILENO, "Failed to initialize semaphore\n");
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

  // Terminate server
  kvs_terminate();
  unlink(regist_fifo_name);
  sem_destroy(&client_sem);
  return 0;
}
