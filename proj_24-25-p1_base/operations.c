#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "constants.h"
#include "kvs.h"

static struct HashTable *kvs_table = NULL;

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  free_table(kvs_table);
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  char final[MAX_WRITE_SIZE] = "";
  strcat(final, "[(");
  for (size_t i = 0; i < num_pairs; i++) {
    strcat(final, keys[i]);
    char *result = read_pair(kvs_table, keys[i]);
    if (result == NULL) {
      strcat(final, ",KVSERROR)");
    } else {
      strcat(final, ",");
      strcat(final, result);
      strcat(final, ")");
      
    }
    free(result);
  }

  strcat(final, "]\n");
  write(output_fd, final, strlen(final));
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  char final[MAX_WRITE_SIZE] = "";
  int aux = 0;
  
  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if(!aux){
        strcat(final, "[");
        aux = 1;
      }
      strcat(final, "(");
      strcat(final, keys[i]);
      strcat(final,",KVSMISSING)");
    }
  }
  if(aux){
    strcat (final, "]\n");
  }
  write(output_fd, final, strlen(final));

  return 0;
}

void kvs_show(int output_fd) {
  char final[MAX_WRITE_SIZE];
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      sprintf(final, "(%s, %s)\n", keyNode->key, keyNode->value);
      keyNode = keyNode->next; // Move to the next node
      write(output_fd, final, strlen(final));
    }
  }
  
}

int kvs_backup() { return 0; }

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}