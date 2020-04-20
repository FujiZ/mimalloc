//
// Created by fuji on 2020/4/19.
//
#include <infiniband/verbs.h>

#include "mimalloc-ctx.h"
#include "benchmark.h"

static const char* dev_name = "rocep8s0";
static struct ibv_context* ibv_ctx = NULL;
static struct ibv_pd* ibv_pd = NULL;
static mi_ctx_t* mi_ctx = NULL;
static mi_decl_thread mi_heap_t* mi_heap = NULL;

static void register_memory(mi_ctx_t *ctx, mi_segment_t *segment, size_t segment_size) {
  // assign mr to data field
  segment->data = ibv_reg_mr(ctx->data, segment, segment_size,
                             IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ);
}

static void deregister_memory(mi_ctx_t *ctx, mi_segment_t *segment, size_t segment_size) {
  ibv_dereg_mr(segment->data);
  // reset data field in segment
  segment->data = NULL;
}

static mi_mem_hook_t mem_hook = {
  .register_fun = register_memory,
  .deregister_fun = deregister_memory,
};

int
benchmark_initialize() {
  int num_devices = 0;
  struct ibv_device **devices = ibv_get_device_list(&num_devices);
  for (int i = 0; i < num_devices; ++i) {
    if (strcmp(dev_name, ibv_get_device_name(devices[i])) == 0) {
      // we got the target device
      ibv_ctx = ibv_open_device(devices[i]);
      ibv_pd = ibv_alloc_pd(ibv_ctx);
      break;
    }
  }
  ibv_free_device_list(devices);
  // create mimalloc context
  mi_ctx = mi_ctx_new(ibv_pd, &mem_hook, false);
  return 0;
}

int
benchmark_finalize(void) {
  mi_ctx_delete(mi_ctx);
  mi_ctx = NULL;
  return 0;
}

int
benchmark_thread_initialize(void) {
  mi_heap = mi_ctx_thread_init(mi_ctx);
  return 0;
}

int
benchmark_thread_finalize(void) {
  mi_ctx_thread_done(mi_heap);
  mi_heap = NULL;
  return 0;
}

void*
benchmark_malloc(size_t alignment, size_t size) {
  return mi_heap_malloc_aligned(mi_heap, size, alignment);
}

extern void
benchmark_free(void* ptr) {
  mi_free(ptr);
}

const char*
benchmark_name(void) {
  return "rmalloc";
}

void
benchmark_thread_collect(void) {
}
