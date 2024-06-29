#include "kvs_clock.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

typedef struct kvs_clock_node {
  char key[KVS_KEY_MAX];
  char value[KVS_VALUE_MAX];
  bool reference_bit;
  bool is_dirty;
} kvs_clock_node_t;

struct kvs_clock {
  kvs_base_t* kvs_base;
  int capacity;
  int size;
  kvs_clock_node_t* cache;
  int clock_hand;
};

kvs_clock_t* kvs_clock_new(kvs_base_t* kvs, int capacity) {
  kvs_clock_t* kvs_clock = malloc(sizeof(kvs_clock_t));
  kvs_clock->kvs_base = kvs;
  kvs_clock->capacity = capacity;
  kvs_clock->size = 0;
  kvs_clock->cache = malloc(sizeof(kvs_clock_node_t) * capacity);
  kvs_clock->clock_hand = 0;
  return kvs_clock;
}

void kvs_clock_free(kvs_clock_t** ptr) {
  kvs_clock_t* kvs_clock = *ptr;
  free(kvs_clock->cache);
  free(kvs_clock);
  *ptr = NULL;
}

int kvs_clock_set(kvs_clock_t* kvs_clock, const char* key, const char* value) {
  for (int i = 0; i < kvs_clock->size; i++) {
    if (strcmp(kvs_clock->cache[i].key, key) == 0) {
      strcpy(kvs_clock->cache[i].value, value);
      kvs_clock->cache[i].is_dirty = true;
      kvs_clock->cache[i].reference_bit = true;
      return SUCCESS;
    }
  }

  if (kvs_clock->size < kvs_clock->capacity) {
    strcpy(kvs_clock->cache[kvs_clock->size].key, key);
    strcpy(kvs_clock->cache[kvs_clock->size].value, value);
    kvs_clock->cache[kvs_clock->size].reference_bit = true;
    kvs_clock->cache[kvs_clock->size].is_dirty = true;
    kvs_clock->size++;
  } else {
    while (kvs_clock->cache[kvs_clock->clock_hand].reference_bit) {
      kvs_clock->cache[kvs_clock->clock_hand].reference_bit = false;
      kvs_clock->clock_hand = (kvs_clock->clock_hand + 1) % kvs_clock->capacity;
    }

    if (kvs_clock->cache[kvs_clock->clock_hand].is_dirty) {
      if (kvs_base_set(
              kvs_clock->kvs_base, kvs_clock->cache[kvs_clock->clock_hand].key,
              kvs_clock->cache[kvs_clock->clock_hand].value) == FAILURE) {
        return FAILURE;
      }
    }

    strcpy(kvs_clock->cache[kvs_clock->clock_hand].key, key);
    strcpy(kvs_clock->cache[kvs_clock->clock_hand].value, value);
    kvs_clock->cache[kvs_clock->clock_hand].reference_bit = true;
    kvs_clock->cache[kvs_clock->clock_hand].is_dirty = true;
    kvs_clock->clock_hand = (kvs_clock->clock_hand + 1) % kvs_clock->capacity;
  }

  return SUCCESS;
}

int kvs_clock_get(kvs_clock_t* kvs_clock, const char* key, char* value) {
  for (int i = 0; i < kvs_clock->size; i++) {
    if (strcmp(kvs_clock->cache[i].key, key) == 0) {
      strcpy(value, kvs_clock->cache[i].value);
      kvs_clock->cache[i].reference_bit = true;
      return SUCCESS;
    }
  }

  if (kvs_base_get(kvs_clock->kvs_base, key, value) == FAILURE) {
    return FAILURE;
  }

  if (kvs_clock->size < kvs_clock->capacity) {
    strcpy(kvs_clock->cache[kvs_clock->size].key, key);
    strcpy(kvs_clock->cache[kvs_clock->size].value, value);
    kvs_clock->cache[kvs_clock->size].reference_bit = true;
    kvs_clock->cache[kvs_clock->size].is_dirty = false;
    kvs_clock->size++;
  } else {
    while (kvs_clock->cache[kvs_clock->clock_hand].reference_bit) {
      kvs_clock->cache[kvs_clock->clock_hand].reference_bit = false;
      kvs_clock->clock_hand = (kvs_clock->clock_hand + 1) % kvs_clock->capacity;
    }

    if (kvs_clock->cache[kvs_clock->clock_hand].is_dirty) {
      if (kvs_base_set(
              kvs_clock->kvs_base, kvs_clock->cache[kvs_clock->clock_hand].key,
              kvs_clock->cache[kvs_clock->clock_hand].value) == FAILURE) {
        return FAILURE;
      }
    }

    strcpy(kvs_clock->cache[kvs_clock->clock_hand].key, key);
    strcpy(kvs_clock->cache[kvs_clock->clock_hand].value, value);
    kvs_clock->cache[kvs_clock->clock_hand].reference_bit = true;
    kvs_clock->cache[kvs_clock->clock_hand].is_dirty = false;
    kvs_clock->clock_hand = (kvs_clock->clock_hand + 1) % kvs_clock->capacity;
  }

  return SUCCESS;
}

int kvs_clock_flush(kvs_clock_t* kvs_clock) {
  for (int i = 0; i < kvs_clock->size; i++) {
    if (kvs_clock->cache[i].is_dirty) {
      if (kvs_base_set(kvs_clock->kvs_base, kvs_clock->cache[i].key,
                       kvs_clock->cache[i].value) == FAILURE) {
        return FAILURE;
      }
      kvs_clock->cache[i].is_dirty = false;
    }
  }
  return SUCCESS;
}
