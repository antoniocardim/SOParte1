#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>

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
  pthread_mutex_init(&kvs_table->locker_hashtable, NULL);
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  pthread_mutex_destroy(&kvs_table->locker_hashtable);
  free_table(kvs_table);
  return 0;
}

//modificado
// estrutura auxiliar
typedef struct KeyValuePair {
    char key[MAX_STRING_SIZE];
    char value[MAX_STRING_SIZE];
} KeyValuePair;

//funcao auxiliar que compara 
int compareKeyValuePairs(const void *a, const void *b) {
    KeyValuePair *pairA = (KeyValuePair *)a;
    KeyValuePair *pairB = (KeyValuePair *)b;
    return strcmp(pairA->key, pairB->key);
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  pthread_mutex_lock(&kvs_table->locker_hashtable);
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    pthread_mutex_unlock(&kvs_table->locker_hashtable);
    return 1;
  }
  pthread_mutex_unlock(&kvs_table->locker_hashtable);
  
  // cria a estrutura auxiliar
  KeyValuePair pairs[num_pairs];
  for (size_t i = 0; i < num_pairs; i++) {
    strncpy(pairs[i].key, keys[i], MAX_STRING_SIZE);
    strncpy(pairs[i].value, values[i], MAX_STRING_SIZE);
  }
  //ordena a lista por ordem alfabetica
  qsort(pairs, num_pairs, sizeof(KeyValuePair), compareKeyValuePairs);


  for (size_t i = 0; i < num_pairs; i++) {
    pthread_mutex_lock(&kvs_table->locker_hashtable);
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
      pthread_mutex_unlock(&kvs_table->locker_hashtable);
    }
    pthread_mutex_unlock(&kvs_table->locker_hashtable);
  }

  return 0;
}

int kvs_read(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd) {
  pthread_mutex_lock(&kvs_table->locker_hashtable);
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    pthread_mutex_unlock(&kvs_table->locker_hashtable);
    return 1;
    
  }
  pthread_mutex_unlock(&kvs_table->locker_hashtable);

  KeyValuePair pairs[num_pairs];  //cria a estrutura auxiliar
  for (size_t i = 0; i < num_pairs; i++) {
    strncpy(pairs[i].key, keys[i], MAX_STRING_SIZE);
    pthread_mutex_lock(&kvs_table->locker_hashtable);
    char *result = read_pair(kvs_table, keys[i]);
    pthread_mutex_unlock(&kvs_table->locker_hashtable);
    if (result) {
      strncpy(pairs[i].value, result, MAX_STRING_SIZE);
      free(result);
    } else {
      strcpy(pairs[i].value, "KVSERROR");
    }
  }

  //sort da lista de estruturas auxiliares
  qsort(pairs, num_pairs, sizeof(KeyValuePair), compareKeyValuePairs);

  char final[MAX_WRITE_SIZE] = "[";

  for (size_t i = 0; i < num_pairs; i++) {
    strcat(final, "(");
    strcat(final, pairs[i].key);
    strcat(final, ",");
    strcat(final, pairs[i].value);
    strcat(final, ")");
    //alteracao tirei o if
  }

  strcat(final, "]\n");
  write(output_fd, final, strlen(final));
  return 0;
}

int kvs_delete(size_t num_pairs, char keys[][MAX_STRING_SIZE], int output_fd) {
  pthread_mutex_lock(&kvs_table->locker_hashtable);
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    pthread_mutex_unlock(&kvs_table->locker_hashtable);
    return 1;
  }
  pthread_mutex_unlock(&kvs_table->locker_hashtable);
  char final[MAX_WRITE_SIZE] = "";
  int aux = 0;
  
  for (size_t i = 0; i < num_pairs; i++) {
    pthread_mutex_lock(&kvs_table->locker_hashtable);
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if(!aux){
        strcat(final, "[");
        aux = 1;
      }
      strcat(final, "(");
      strcat(final, keys[i]);
      strcat(final,",KVSMISSING)");
      pthread_mutex_unlock(&kvs_table->locker_hashtable);
    }
    pthread_mutex_unlock(&kvs_table->locker_hashtable);
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
    pthread_mutex_lock(&kvs_table->locker_hashtable);
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      sprintf(final, "(%s, %s)\n", keyNode->key, keyNode->value);
      keyNode = keyNode->next; // Move to the next node
      write(output_fd, final, strlen(final));
    }
    pthread_mutex_unlock(&kvs_table->locker_hashtable);
  }
  
}

void kvs_backup(char input_path[], int backup_count) { 
  //alteracao
  int lenght = snprintf(NULL, 0, "%d", backup_count);
  char *str = malloc((size_t)lenght + 1); 
  char backup_path[MAX_JOB_FILE_NAME_SIZE] = "";
  strncpy(backup_path, input_path, strlen(input_path) - 4);
  strcat(backup_path, "-");
  sprintf(str, "%d", backup_count);
  strcat(backup_path, str);
  strcat(backup_path, ".bck");
  int backup_fd = open(backup_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
  free(str);
  kvs_show(backup_fd);
  close(backup_fd);

}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}