#include "kvs_lru.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

typedef struct kvs_lru_node {
  char key[KVS_KEY_MAX];
  char value[KVS_VALUE_MAX];
  int last_used;
} kvs_lru_node_t;

struct kvs_lru {
  kvs_base_t* kvs_base;
  int capacity;
  int size;
  kvs_lru_node_t* cache;
};

kvs_lru_t* kvs_lru_new(kvs_base_t* kvs, int capacity) {
  kvs_lru_t* kvs_lru = malloc(sizeof(kvs_lru_t));
  kvs_lru->kvs_base = kvs;
  kvs_lru->capacity = capacity;
  kvs_lru->size = 0;
  kvs_lru->cache = malloc(sizeof(kvs_lru_node_t) * capacity);
  return kvs_lru;
}

void kvs_lru_free(kvs_lru_t** ptr) {
  kvs_lru_t* kvs_lru = *ptr;
  free(kvs_lru->cache);
  free(kvs_lru);
  *ptr = NULL;
}

int kvs_lru_set(kvs_lru_t* kvs_lru, const char* key, const char* value) {
  kvs_base_set(kvs_lru->kvs_base, key, value);

  for (int i = 0; i < kvs_lru->size; i++) {
    if (strcmp(kvs_lru->cache[i].key, key) == 0) {
      strcpy(kvs_lru->cache[i].value, value);
      kvs_lru->cache[i].last_used++;
      return SUCCESS;
    }
  }

  if (kvs_lru->size < kvs_lru->capacity) {
    strcpy(kvs_lru->cache[kvs_lru->size].key, key);
    strcpy(kvs_lru->cache[kvs_lru->size].value, value);
    kvs_lru->cache[kvs_lru->size].last_used = 0;
    kvs_lru->size++;
  } else {
    int lru_index = 0;
    for (int i = 1; i < kvs_lru->size; i++) {
      if (kvs_lru->cache[i].last_used < kvs_lru->cache[lru_index].last_used) {
        lru_index = i;
      }
    }
    strcpy(kvs_lru->cache[lru_index].key, key);
    strcpy(kvs_lru->cache[lru_index].value, value);
    kvs_lru->cache[lru_index].last_used = 0;
  }

  return SUCCESS;
}

int kvs_lru_get(kvs_lru_t* kvs_lru, const char* key, char* value) {
  for (int i = 0; i < kvs_lru->size; i++) {
    if (strcmp(kvs_lru->cache[i].key, key) == 0) {
      strcpy(value, kvs_lru->cache[i].value);
      kvs_lru->cache[i].last_used++;
      return SUCCESS;
    }
  }

  if (kvs_base_get(kvs_lru->kvs_base, key, value) == FAILURE) {
    return FAILURE;
  }

  if (kvs_lru->size < kvs_lru->capacity) {
    strcpy(kvs_lru->cache[kvs_lru->size].key, key);
    strcpy(kvs_lru->cache[kvs_lru->size].value, value);
    kvs_lru->cache[kvs_lru->size].last_used = 0;
    kvs_lru->size++;
  } else {
    int lru_index = 0;
    for (int i = 1; i < kvs_lru->size; i++) {
      if (kvs_lru->cache[i].last_used < kvs_lru->cache[lru_index].last_used) {
        lru_index = i;
      }
    }
    strcpy(kvs_lru->cache[lru_index].key, key);
    strcpy(kvs_lru->cache[lru_index].value, value);
    kvs_lru->cache[lru_index].last_used = 0;
  }

  return SUCCESS;
}

int kvs_lru_flush(kvs_lru_t* kvs_lru) { return SUCCESS; }
