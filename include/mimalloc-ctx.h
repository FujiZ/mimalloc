//
// Created by fuji on 2019/12/3.
//
#pragma once
#ifndef MIMALLOC_CTX_H_
#define MIMALLOC_CTX_H_

#if defined(MI_ZRPC_EXTENSION)
#include "mimalloc.h"
#include "mimalloc-internal.h"

#ifdef __cplusplus
extern "C" {
#endif

// zrpc extension
mi_decl_export mi_ctx_t*  mi_ctx_new(void* data, const mi_mem_hook_t* mem_hook, bool mem_huge);
mi_decl_export void mi_ctx_delete(mi_ctx_t* ctx) mi_attr_noexcept;
mi_decl_export mi_heap_t* mi_ctx_thread_init(mi_ctx_t* ctx);
mi_decl_export void mi_ctx_thread_done(mi_heap_t* heap) mi_attr_noexcept;


#ifdef __cplusplus
}
#endif


#endif //MI_ZRPC_EXTENSION
#endif //MIMALLOC_CTX_H_
