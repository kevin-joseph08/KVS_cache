#include "kvs_fifo.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#include "kvs_base.h"

typedef struct kvs_fifo_node {
  char key[KVS_KEY_MAX];
  char value[KVS_VALUE_MAX];
  bool is_dirty;
  struct kvs_fifo_node* next;
} kvs_fifo_node_t;

struct kvs_fifo {
  kvs_base_t* kvs_base;
  int capacity;
  int size;
  kvs_fifo_node_t* head;
  kvs_fifo_node_t* tail;
};

kvs_fifo_t* kvs_fifo_new(kvs_base_t* kvs, int capacity) {
  kvs_fifo_t* kvs_fifo = malloc(sizeof(kvs_fifo_t));
  kvs_fifo->kvs_base = kvs;
  kvs_fifo->capacity = capacity;
  kvs_fifo->size = 0;
  kvs_fifo->head = NULL;
  kvs_fifo->tail = NULL;
  return kvs_fifo;
}

void kvs_fifo_free(kvs_fifo_t** ptr) {
  kvs_fifo_node_t* current = (*ptr)->head;
  while (current != NULL) {
    kvs_fifo_node_t* next = current->next;
    free(current);
    current = next;
  }
  free(*ptr);
  *ptr = NULL;
}

int kvs_fifo_set(kvs_fifo_t* kvs_fifo, const char* key, const char* value) {
  kvs_fifo_node_t* node = kvs_fifo->head;
  while (node != NULL) {
    if (strcmp(node->key, key) == 0) {
      strcpy(node->value, value);
      node->is_dirty = true;
      return SUCCESS;
    }
    node = node->next;
  }

  if (kvs_fifo->size >= kvs_fifo->capacity) {
    kvs_fifo_node_t* evicted = kvs_fifo->head;
    kvs_fifo->head = kvs_fifo->head->next;
    if (kvs_fifo->head == NULL) {
      kvs_fifo->tail = NULL;
    }
    kvs_fifo->size--;

    if (evicted->is_dirty) {
      if (kvs_base_set(kvs_fifo->kvs_base, evicted->key, evicted->value) ==
          FAILURE) {
        free(evicted);
        return FAILURE;
      }
    }
    free(evicted);
  }

  kvs_fifo_node_t* new_node = (kvs_fifo_node_t*)malloc(sizeof(kvs_fifo_node_t));
  strcpy(new_node->key, key);
  strcpy(new_node->value, value);
  new_node->is_dirty = true;
  new_node->next = NULL;

  if (kvs_fifo->tail != NULL) {
    kvs_fifo->tail->next = new_node;
  } else {
    kvs_fifo->head = new_node;
  }
  kvs_fifo->tail = new_node;
  kvs_fifo->size++;

  return SUCCESS;
}

int kvs_fifo_get(kvs_fifo_t* kvs_fifo, const char* key, char* value) {
  kvs_fifo_node_t* node = kvs_fifo->head;
  while (node != NULL) {
    if (strcmp(node->key, key) == 0) {
      strcpy(value, node->value);
      return SUCCESS;
    }
    node = node->next;
  }

  if (kvs_base_get(kvs_fifo->kvs_base, key, value) == FAILURE) {
    return FAILURE;
  }

  if (kvs_fifo->size >= kvs_fifo->capacity) {
    kvs_fifo_node_t* evicted = kvs_fifo->head;
    kvs_fifo->head = kvs_fifo->head->next;
    if (kvs_fifo->head == NULL) {
      kvs_fifo->tail = NULL;
    }
    kvs_fifo->size--;

    if (evicted->is_dirty) {
      if (kvs_base_set(kvs_fifo->kvs_base, evicted->key, evicted->value) ==
          FAILURE) {
        free(evicted);
        return FAILURE;
      }
    }
    free(evicted);
  }

  kvs_fifo_node_t* new_node = (kvs_fifo_node_t*)malloc(sizeof(kvs_fifo_node_t));
  strcpy(new_node->key, key);
  strcpy(new_node->value, value);
  new_node->is_dirty = false;
  new_node->next = NULL;

  if (kvs_fifo->tail != NULL) {
    kvs_fifo->tail->next = new_node;
  } else {
    kvs_fifo->head = new_node;
  }
  kvs_fifo->tail = new_node;
  kvs_fifo->size++;

  return SUCCESS;
}

int kvs_fifo_flush(kvs_fifo_t* kvs_fifo) {
  kvs_fifo_node_t* node = kvs_fifo->head;
  while (node != NULL) {
    if (node->is_dirty) {
      if (kvs_base_set(kvs_fifo->kvs_base, node->key, node->value) == FAILURE) {
        return FAILURE;
      }
      node->is_dirty = false;
    }
    node = node->next;
  }
  return SUCCESS;
}
