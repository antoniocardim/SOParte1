#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

#include "constants.h"
#include "operations.h"
#include "parser.h"

int kvs_processor(int input_fd, int output_fd);
size_t find_index(char *file_name);

int main(int argc, char *argv[]) {
  if (argc > 3 || argc < 2) {
    fprintf(stderr, "Usage: %s <jobs path> <max backup>\n", argv[0]);
    return 1;
  }

  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }

  DIR *dir = opendir(argv[1]);
  struct dirent* dp;

  if (!dir) {
    fprintf(stderr, "Failed to open directory\n");
    return 1;
  }
  while ((dp = readdir(dir)) != NULL) {
    if (strcmp(dp->d_name, ".") != 0 && strcmp(dp->d_name, "..") != 0 && strcmp(strrchr(dp->d_name, '.'), ".job") == 0) {
      char input_path[MAX_JOB_FILE_NAME_SIZE] = "";
      strcpy(input_path, argv[1]);
      strcat(input_path, "/");
      strcat(input_path, dp->d_name);
      int input_fd = open(input_path, O_RDONLY);
      if (input_fd == -1) {
        fprintf(stderr, "Failed to open file: %s\n", strerror(errno));
        closedir(dir);
        return 1;
      }
      char output_path[MAX_JOB_FILE_NAME_SIZE] = "";
      strncpy(output_path, input_path, strlen(input_path) - 4);
      strcat(output_path, ".out");
      int output_fd = open(output_path, O_CREAT | O_RDWR | O_TRUNC, S_IRUSR | S_IWUSR | S_IROTH | S_IRGRP);
      kvs_processor(input_fd, output_fd);
      
      close(output_fd);
      close(input_fd);
    }
  }
  kvs_terminate();
  closedir(dir);

  return 0;
}

size_t find_index(char *file_name) {
  char character_to_find = '.';
  const char *char_pointer = strrchr(file_name, character_to_find);
  if (char_pointer != NULL) {
    return (size_t)(char_pointer - file_name);
  }
  return 1;
}

int kvs_processor(int input_fd, int output_fd) {
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

      if (kvs_backup()) {
        fprintf(stderr, "Failed to perform backup.\n");
      }
      break;

    case CMD_INVALID:
      fprintf(stderr, "Invalid command. See HELP for usage\n");
      break;

    case CMD_HELP:
      char *buf = "Available commands:\n"
                  "  WRITE [(key,value)(key2,value2),...]\n"
                  "  READ [key,key2,...]\n"
                  "  DELETE [key,key2,...]\n"
                  "  SHOW\n"
                  "  WAIT <delay_ms>\n"
                  "  BACKUP\n" // Not implemented
                  "  HELP\n";
      write(output_fd, buf, strlen(buf));

      break;

    case CMD_EMPTY:
      break;

    case EOC:
      
      return 0;
    }
  }
}
