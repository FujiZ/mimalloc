//
// Created by fuji on 2019/12/2.
//
#if defined(MI_ZRPC_EXTENSION)
#include <string.h>

#include "mimalloc-ctx.h"

static mi_mem_hook_t mi_mem_hook_empty = {
    NULL,
    NULL,
};

mi_ctx_t* mi_ctx_new(void* data, const mi_mem_hook_t* mem_hook, bool mem_huge) {
  // here we use mi_heap_malloc to allocate context object
  mi_ctx_t* ctx = mi_malloc_tp(mi_ctx_t);
  if (!ctx) return NULL;
  ctx->abandoned = ATOMIC_VAR_INIT(NULL);
  ctx->abandoned_count = ATOMIC_VAR_INIT(0);
  ctx->thread_count = ATOMIC_VAR_INIT(1);
  ctx->mem_hook = mem_hook ? *mem_hook : mi_mem_hook_empty;
  ctx->data = data;
  ctx->mem_huge = mem_huge;
  return ctx;
}

/// @note all heaps must be deleted before free the context
static void _mi_ctx_free(mi_ctx_t* ctx) mi_attr_noexcept {
  // check abandoned segments
  mi_assert_internal(!mi_atomic_read(&ctx->abandoned_count));
  mi_free(ctx);
}

void mi_ctx_delete(mi_ctx_t* ctx) mi_attr_noexcept {
  // decrease thread_count
  if (!mi_atomic_decrement(&ctx->thread_count))
    _mi_ctx_free(ctx);
}

typedef struct mi_thread_ctx_s {
  mi_heap_t heap;
  mi_tld_t  tld;
} mi_thread_ctx_t;

/**
 * @brief init thread data like _mi_heap_init() do.
 * the life time of thread is managed by caller.
 * mi_ctx_thread_done() should be called when the thread is to terminate.
 * @return thread backing heap, NULL if failed.
 */
mi_heap_t* mi_ctx_thread_init(mi_ctx_t* ctx) {
  mi_thread_ctx_t* td = mi_malloc_tp(mi_thread_ctx_t);
  if (td == NULL) {
    _mi_error_message("failed to allocate thread local context memory\n");
    return NULL;
  }
  mi_tld_t*  tld = &td->tld;
  mi_heap_t* heap = &td->heap;
  memcpy(heap, &_mi_heap_empty, sizeof(*heap));
  heap->thread_id = _mi_thread_id();
  heap->random = _mi_random_init(heap->thread_id);
  heap->cookie = ((uintptr_t)heap ^ _mi_heap_random(heap)) | 1;
  heap->tld = tld;
  memset(tld, 0, sizeof(*tld));
  tld->heap_backing = heap;
  tld->segments.stats = &tld->stats;
  tld->os.stats = &tld->stats;
  tld->ctx = ctx;
  // increase ctx->thread_count
  mi_atomic_increment(&ctx->thread_count);

  _mi_stat_increase(&tld->stats.threads, 1);
  return heap;
}

void mi_ctx_thread_done(mi_heap_t* heap) mi_attr_noexcept {
  if (!mi_heap_is_initialized(heap))
    return;
  // Currently we do not allow mi_heap_new() on ctx
  mi_assert_internal(heap == heap->tld->heap_backing);
  _mi_stat_decrease(&heap->tld->stats.threads, 1);

  // the backing heap abandons its pages
  _mi_heap_collect_abandon(heap);
  // merge stats
  _mi_stats_done(&heap->tld->stats);

  // decrease thread count
  mi_ctx_t *ctx = heap->tld->ctx;
  if (!mi_atomic_decrement(&ctx->thread_count))
    _mi_ctx_free(ctx);

  // free the thread local context
  mi_free(mi_container_of(heap, mi_thread_ctx_t, heap));
}

#endif
