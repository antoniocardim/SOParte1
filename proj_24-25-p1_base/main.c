#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/wait.h>
#include <pthread.h>

#include "constants.h"
#include "operations.h"
#include "parser.h"




int max_threads, max_backups, file_counter = 0, queue_size;

pthread_mutex_t locker = PTHREAD_MUTEX_INITIALIZER;

struct file_t{
    char name [MAX_JOB_FILE_NAME_SIZE];
    char directory [MAX_JOB_FILE_NAME_SIZE];
    int backup_count;
    int process_count;
    pthread_mutex_t file_lock;
} ;

typedef struct queue_node *queue;

struct queue_node {
  struct file_t file;
  queue next;
};

struct list {
  queue head;
  queue tail;
};



typedef struct list *Q;

  Q q;


int kvs_processor(int input_fd, int output_fd, char input_path[], struct file_t file);
void insert_to_end( struct file_t file);
queue new_args (struct file_t file, queue next);
void init_head_and_tail();
void *thread_processer(void* arg);
struct file_t get_and_delete();
int q_empty();

int main(int argc, char *argv[]) {
  if (argc != 4) {
    fprintf(stderr, "Usage: %s <jobs path> <max backup> <max threads>\n", argv[0]);
    return 1;
  }

  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }
  
  
  max_threads = atoi(argv[3]);
  max_backups = atoi(argv[2]);
  DIR *dir = opendir(argv[1]);
  struct dirent* dp;
  queue_size = max_threads;
  pthread_mutex_init(&locker,NULL);

  if (!dir) {
    fprintf(stderr, "Failed to open directory\n");
    return 1;
  }
  
  pthread_t thread[max_threads];
  init_head_and_tail(q);
  
  
  
  while ((dp = readdir(dir)) != NULL) {
    struct file_t new_file;
    strncpy(new_file.name, dp->d_name, MAX_JOB_FILE_NAME_SIZE);
    strncpy(new_file.directory, argv[1], MAX_JOB_FILE_NAME_SIZE);
    new_file.backup_count = 0;
    new_file.process_count = 0;
    insert_to_end(new_file);
    file_counter++;
  }

  for(int i = 0; i < max_threads; ++i) {
    if(pthread_create(&thread[i], NULL, thread_processer, (void*)q) != 0){
      fprintf(stderr, "Failed to create thread: %s\n", strerror(errno));
      return 1;
    }
  }
  
  for (int i = 0; i < max_threads; i++){
    if(pthread_join(thread[i], NULL) != 0){
        fprintf(stderr, "Failed to join thread begining: %s\n", strerror(errno));
        return 1;
    }
    printf("Thread %d joined\n", i);
  }
  printf("All threads joined\n");
  pthread_mutex_destroy(&locker);
  kvs_terminate();
  closedir(dir);

  return 0;
}

int kvs_processor(int input_fd, int output_fd, char input_path[], struct file_t file) {
    while (1) {
      char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
      char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
      unsigned int delay;
      size_t num_pairs;

      switch (get_next(input_fd)) {
      case CMD_WRITE:
        num_pairs =
            parse_write(input_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_write(num_pairs, keys, values)) {
          fprintf(stderr, "Failed to write pair\n");
        }

        break;

      case CMD_READ:
        num_pairs =
            parse_read_delete(input_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_read(num_pairs, keys, output_fd)) {
          fprintf(stderr, "Failed to read pair\n");
        }
        break;

      case CMD_DELETE:
        num_pairs =
            parse_read_delete(input_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_delete(num_pairs, keys, output_fd)) {
          fprintf(stderr, "Failed to delete pair\n");
        }
        break;

      case CMD_SHOW:

        kvs_show(output_fd);
        break;

      case CMD_WAIT:
        if (parse_wait(input_fd, &delay, NULL) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay > 0) {
          char *buf = "Waiting...\n";
          write(output_fd, buf, strlen(buf));
          kvs_wait(delay);
        }
        break;

      case CMD_BACKUP:
        printf("Backup\n");
        file.process_count++;
        file.backup_count++;
        
        if(file.process_count > max_backups) {
          wait(NULL);
          file.process_count--;
        }
        
        pid_t pid = fork();
        if (pid == 0) {
          kvs_backup(input_path, file.backup_count);
          printf("Processes: %d\nBackup count: %d\n", file.process_count, file.backup_count); 
          exit(0);
        } else if (pid < 0) {
          fprintf(stderr, "Failed to create backup\n"); 
          break;
          
        } else {
          break;
        }
        break;

      case CMD_INVALID:
        fprintf(stderr, "Invalid command. See HELP for usage\n");
        break;

      case CMD_HELP: {
        char *buf = "Available commands:\n  WRITE [(key,value)(key2,value2),...]\n"
                    "  READ [key,key2,...]\n"
                    "  DELETE [key,key2,...]\n"
                    "  SHOW\n"
                    "  WAIT <delay_ms>\n"
                    "  BACKUP\n" // Not implemented
                    "  HELP\n";
        write(output_fd, buf, strlen(buf));

        break;
      }
      case CMD_EMPTY:
        break;

      case EOC:
        return 0;
      }
    }
  }
  

void insert_to_end(struct file_t file){
  if (q->head == NULL){
    q->head = (q->tail = new_args(file, q->head));
    return;
  }
  q->tail->next = new_args(file, q->tail->next);
  q->tail = q->tail->next;

}
struct file_t get_and_delete(){
  struct file_t file = q->head->file;
  queue temp = q->head->next;
  free(q->head);
  q->head = temp;
  return file;
}



queue new_args (struct file_t file, queue next){
  queue temp = malloc(sizeof(struct queue_node));

  temp->file = file;
  pthread_mutex_init(&temp->file.file_lock, NULL);
  temp->next = next;
  return temp;
  
}

void init_head_and_tail(){
  q = malloc(sizeof(struct list));
  q->head = NULL;
  q->tail = NULL;
}

int q_empty(){
  return q->head == NULL;
}

void *thread_processer(void *arg){
  pthread_mutex_lock(&locker);
  Q args = (Q)arg;
  pthread_mutex_unlock(&locker);
  while (1){
    pthread_mutex_lock(&locker);
    if (!q_empty(args)){
      
      struct file_t file_arg = get_and_delete(args);
      pthread_mutex_unlock(&locker);
      printf("File %s processing\n", file_arg.name);
      printf("Current thread: %ld || Current file: %s \n", pthread_self(), file_arg.name); 
      if (strcmp(file_arg.name, ".") != 0 && strcmp(file_arg.name, "..") != 0 && strcmp(strrchr(file_arg.name, '.'), ".job") == 0) {
        char input_path[MAX_JOB_FILE_NAME_SIZE] = "";
        strcpy(input_path, file_arg.directory);
        strcat(input_path, "/");
        strcat(input_path, file_arg.name);
        int input_fd = open(input_path, O_RDONLY);
        if (input_fd == -1) {
          fprintf(stderr, "Failed to open file: %s\n", strerror(errno));
          return NULL;
        }
        char output_path[MAX_JOB_FILE_NAME_SIZE] = "";
        strncpy(output_path, input_path, strlen(input_path) - 4);
        strcat(output_path, ".out");
        int output_fd = open(output_path, O_CREAT | O_WRONLY | O_TRUNC, S_IRUSR | S_IWUSR | S_IROTH | S_IRGRP);
        if (output_fd == -1) {
          fprintf(stderr, "Failed to open file: %s\n", strerror(errno));
          return NULL;
        }
        pthread_mutex_lock(&locker);
        kvs_processor(input_fd, output_fd, input_path, file_arg);
        pthread_mutex_unlock(&locker);
        close(output_fd);
        close(input_fd);
      }  
      pthread_mutex_lock(&locker);
      file_counter--;
      printf("File %s processed\n", file_arg.name);
      printf("Files_left: %d \n", file_counter);
      
      if(q_empty(args)){
        pthread_mutex_unlock(&locker);
        return NULL;
      }
      pthread_mutex_unlock(&locker);
    }
    if(q_empty(args) && file_counter == 0){
      pthread_mutex_unlock(&locker);
      return NULL;
    }
    pthread_mutex_unlock(&locker);
  }
  return NULL;
}
