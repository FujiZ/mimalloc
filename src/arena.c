/* ----------------------------------------------------------------------------
Copyright (c) 2019, Microsoft Research, Daan Leijen
This is free software; you can redistribute it and/or modify it under the
terms of the MIT license. A copy of the license can be found in the file
"LICENSE" at the root of this distribution.
-----------------------------------------------------------------------------*/

/* ----------------------------------------------------------------------------
"Arenas" are fixed area's of OS memory from which we can allocate
large blocks (>= MI_ARENA_BLOCK_SIZE, 32MiB). 
In contrast to the rest of mimalloc, the arenas are shared between 
threads and need to be accessed using atomic operations.

Currently arenas are only used to for huge OS page (1GiB) reservations,
otherwise it delegates to direct allocation from the OS.
In the future, we can expose an API to manually add more kinds of arenas 
which is sometimes needed for embedded devices or shared memory for example.
(We can also employ this with WASI or `sbrk` systems to reserve large arenas
 on demand and be able to reuse them efficiently).

The arena allocation needs to be thread safe and we use an atomic
bitmap to allocate. The current implementation of the bitmap can
only do this within a field (`uintptr_t`) so we can allocate at most
blocks of 2GiB (64*32MiB) and no object can cross the boundary. This
can lead to fragmentation but fortunately most objects will be regions
of 256MiB in practice.
-----------------------------------------------------------------------------*/
#include "mimalloc.h"
#include "mimalloc-internal.h"
#include "mimalloc-atomic.h"

#include <string.h>  // memset

#include "bitmap.inc.c"  // atomic bitmap

// os.c
void* _mi_os_alloc_aligned(size_t size, size_t alignment, bool commit, bool* large, mi_os_tld_t* tld);
void  _mi_os_free(void* p, size_t size, mi_stats_t* stats);

void* _mi_os_alloc_huge_os_pages(size_t pages, int numa_node, mi_msecs_t max_secs, size_t* pages_reserved, size_t* psize);
void  _mi_os_free_huge_pages(void* p, size_t size, mi_stats_t* stats);

int   _mi_os_numa_node_count(void);

/* -----------------------------------------------------------
  Arena allocation
----------------------------------------------------------- */

#define MI_SEGMENT_ALIGN      MI_SEGMENT_SIZE
#define MI_ARENA_BLOCK_SIZE   MI_SEGMENT_SIZE

#define MI_ARENA_DEFAULT_SIZE (MI_BITMAP_FIELD_BITS * MI_ARENA_BLOCK_SIZE)        // 256MiB (64MiB on 32-bit)

#if MI_INTPTR_SIZE >= 8
#define MI_MAX_ARENAS         ((256 * GiB) / MI_ARENA_DEFAULT_SIZE)               // 1024 (at most 256GiB in default arenas)
#else
#define MI_MAX_ARENAS         ((3 * GiB) / MI_ARENA_DEFAULT_SIZE)                 // 48 (at most 3GiB in default arenas)
#endif

// Define in a packed way as we statically allocate MAX_ARENAS for 3 lists (3*1024*(4*8) = 96KiB on 64 bit)

// Use either a direct bitmap field or a pointer to the fields for large arena's
typedef union mi_arena_bitmap_u {
  mi_bitmap_field_t  field;
  mi_bitmap_field_t* bitmap;
} mi_arena_bitmap_t;

// A memory arena descriptor
typedef struct mi_arena_s {
  uint8_t*   start;                       // the start of the memory area
  int16_t    numa_node;                   // associated NUMA node + 1
  uint16_t   field_count;                 // number of bitmap fields
  uint16_t   block_count;                 // size of the area in arena blocks (of `MI_ARENA_BLOCK_SIZE`)
  uint8_t    is_zero_init : 1;            // is the arena zero initialized?
  uint8_t    is_fixed : 1;                // fixed memory (cannot be decommitted or reset) (like large OS pages)
  uint8_t    is_committed : 1;            // is the memory committed
  mi_arena_bitmap_t  blocks_map;           // bitmap of in-use blocks
  mi_arena_bitmap_t  blocks_dirty;         // are the blocks potentially non-zero?
  _Atomic(uintptr_t) search_idx;
} mi_arena_t;


// There are 3 arena lists: reserved, committed, and fixed memory
#define MI_ARENA_KINDS (3)
typedef enum mi_arena_kind_e {
  mi_arena_fixed,
  mi_arena_committed,
  mi_arena_reserved
} mi_arena_kind_t;

static mi_arena_t mi_arenas[MI_ARENA_KINDS*MI_MAX_ARENAS];
static _Atomic(uintptr_t) mi_arenas_count[MI_ARENA_KINDS];



/* -----------------------------------------------------------
  Arena allocations get a memory id where the lower 16 bits are
  the arena index +1, and the upper bits the bitmap index.
----------------------------------------------------------- */

// Use `0` as a special id for direct OS allocated memory.
#define MI_MEMID_OS   0

static size_t mi_memid_create(size_t arena_index, mi_bitmap_index_t bitmap_index) {
  mi_assert_internal(arena_index < 0xFFFF);
  mi_assert_internal(((bitmap_index << 16) >> 16) == bitmap_index); // no overflow?
  return ((bitmap_index << 16) | ((arena_index+1) & 0xFFFF));
}

static void mi_memid_indices(size_t memid, size_t* arena_index, mi_bitmap_index_t* bitmap_index) {
  mi_assert_internal(memid != MI_MEMID_OS);
  *arena_index = (memid & 0xFFFF) - 1;
  *bitmap_index = (memid >> 16);
}

static size_t mi_block_count_of_size(size_t size) {
  return _mi_divide_up(size, MI_ARENA_BLOCK_SIZE);
}

/* -----------------------------------------------------------
  Thread safe allocation in an arena
----------------------------------------------------------- */

static bool mi_arena_try_claim(mi_arena_t* const arena, int numa_node, size_t blocks, mi_bitmap_index_t* bitmap_idx)
{
  const size_t fcount = arena->field_count;
  mi_assert_internal(fcount>0);
  if (numa_node>=0) {
    const int nnode = arena->numa_node-1;
    if (nnode>=0&&nnode!=numa_node) { return false; } // arena is not on the same NUMA node
  }
  if (mi_likely(fcount==1)) {
    // single field bitmap
    return mi_bitmap_try_claim_field(&arena->blocks_map.field, 0, blocks, bitmap_idx);
  }
  else {
    // larger bitmap (like for reserved huge OS pages)
    size_t idx = mi_atomic_read(&arena->search_idx);  // start from last search
    for (size_t visited = 0; visited<fcount; visited++, idx++) {
      if (idx>=fcount) idx = 0;  // wrap around
      if (mi_bitmap_try_claim_field(arena->blocks_map.bitmap, idx, blocks, bitmap_idx)) {
        mi_atomic_write(&arena->search_idx, idx);  // start search from here next time
        return true;
      }
    }
    return false;
  }
}

static bool mi_arenas_try_claim(mi_arena_t* arenas, size_t arena_count, size_t start_idx, int numa_node, size_t blocks, size_t* arena_idx, mi_bitmap_index_t* bitmap_idx) 
{
  size_t i = start_idx;
  for (size_t visited = 0; visited < arena_count; visited++, i++ ) {
    if (i>=arena_count) { i = 0; }  // wrap around
    if (mi_arena_try_claim(&arenas[i], numa_node, blocks, bitmap_idx)) {
      *arena_idx = i;
      return true;
    }
  }
  return false;
}

static bool mi_arena_kind_try_alloc(mi_arena_kind_t kind, int numa_node, size_t blocks, size_t* arena_idx, mi_bitmap_index_t* bitmap_idx, mi_os_tld_t* tld) 
{
  mi_arena_t* arenas = &mi_arenas[kind*MI_MAX_ARENAS];
  size_t arenas_count = mi_atomic_read_relaxed(&mi_arenas_count[kind]);
  return mi_arenas_try_claim(arenas, arenas_count, tld->arena_idx[kind], numa_node, blocks, arena_idx, bitmap_idx);
}

static bool mi_arena_try_alloc(size_t blocks, bool commit, bool large, size_t* arena_idx, mi_bitmap_index_t* bitmap_idx, mi_os_tld_t* tld) 
{
  int numa_node = (_mi_os_numa_node_count()<=1 ? -1 : _mi_os_numa_node(tld));
  if (commit&&large) {
    if (mi_arena_kind_try_alloc(mi_arena_fixed, numa_node, blocks, &arena_idx, &bitmap_idx, tld)) return true;
  }
  if (commit) {
    if (mi_arena_kind_try_alloc(mi_arena_committed, numa_node, blocks, &arena_idx, &bitmap_idx, tld)) return true;
  }
  if (mi_arena_kind_try_alloc(mi_arena_reserved, numa_node, blocks, &arena_idx, &bitmap_idx, tld)) return true;

  // failed to allocate, try to allocate on a different numa node for large OS page memory
  if (numa_node >= 0 && commit && large) {
    if (mi_arena_kind_try_alloc(mi_arena_fixed, -1, blocks, &arena_idx, &bitmap_idx, tld)) return true;
  }

  // failed to allocate
  return false;
}


static bool mi_arena_allocx(mi_arena_t* arena, size_t blocks, mi_bitmap_index_t* bitmap_idx) 
{
  const size_t fcount = arena->field_count;
  size_t idx = mi_atomic_read(&arena->search_idx);  // start from last search
  for (size_t visited = 0; visited < fcount; visited++, idx++) {
    if (idx >= fcount) idx = 0;  // wrap around
    if (mi_bitmap_try_claim_field(arena->blocks_map, idx, blocks, bitmap_idx)) {
      mi_atomic_write(&arena->search_idx, idx);  // start search from here next time
      return true;
    }
  }
  return false;
}


/* -----------------------------------------------------------
  Arena Allocation
----------------------------------------------------------- */

static void* mi_arena_alloc_from(mi_arena_t* arena, size_t arena_index, size_t needed_bcount, 
                                 bool* commit, bool* large, bool* is_zero, size_t* memid) 
{
  mi_bitmap_index_t bitmap_index;
  if (mi_arena_alloc(arena, needed_bcount, &bitmap_index)) {
    // claimed it! set the dirty bits (todo: no need for an atomic op here?)
    *is_zero = mi_bitmap_claim(arena->blocks_dirty, arena->field_count, needed_bcount, bitmap_index);
    *memid   = mi_memid_create(arena_index, bitmap_index);
    *commit  = true;           // TODO: support commit on demand?
    *large   = arena->is_large;
    return (arena->start + (mi_bitmap_index_bit(bitmap_index)*MI_ARENA_BLOCK_SIZE));
  }
  return NULL;
}

void* _mi_arena_alloc_aligned(size_t size, size_t alignment, 
                              bool* commit, bool* large, bool* is_zero, 
                              size_t* memid, mi_os_tld_t* tld) 
{
  mi_assert_internal(memid != NULL && tld != NULL);
  mi_assert_internal(size > 0);
  *memid   = MI_MEMID_OS;
  *is_zero = false;
  bool default_large = false;
  if (large==NULL) large = &default_large;  // ensure `large != NULL`

  // try to allocate in an arena if the alignment is small enough
  // and the object is not too large or too small.
  if (alignment <= MI_SEGMENT_ALIGN && 
      size <= MI_ARENA_MAX_OBJ_SIZE && 
      size >= MI_ARENA_MIN_OBJ_SIZE)
  {
    const size_t bcount = mi_block_count_of_size(size);
    const int numa_node = _mi_os_numa_node(tld); // current numa node

    mi_assert_internal(size <= bcount*MI_ARENA_BLOCK_SIZE);
    // try numa affine allocation
    for (size_t i = 0; i < MI_MAX_ARENAS; i++) {
      mi_arena_t* arena = (mi_arena_t*)mi_atomic_read_ptr_relaxed(mi_atomic_cast(void*, &mi_arenas[i]));
      if (arena==NULL) break; // end reached
      if ((arena->numa_node<0 || arena->numa_node==numa_node) && // numa local?
          (*large || !arena->is_large)) // large OS pages allowed, or arena is not large OS pages
      { 
        void* p = mi_arena_alloc_from(arena, i, bcount, commit, large, is_zero, memid);
        mi_assert_internal((uintptr_t)p % alignment == 0);
        if (p != NULL) return p;
      }
    }
    // try from another numa node instead..
    for (size_t i = 0; i < MI_MAX_ARENAS; i++) {
      mi_arena_t* arena = (mi_arena_t*)mi_atomic_read_ptr_relaxed(mi_atomic_cast(void*, &mi_arenas[i]));
      if (arena==NULL) break; // end reached
      if ((arena->numa_node>=0 && arena->numa_node!=numa_node) && // not numa local!
          (*large || !arena->is_large)) // large OS pages allowed, or arena is not large OS pages
      {
        void* p = mi_arena_alloc_from(arena, i, bcount, commit, large, is_zero, memid);
        mi_assert_internal((uintptr_t)p % alignment == 0);
        if (p != NULL) return p;
      }
    }
  }

  // finally, fall back to the OS
  *is_zero = true;
  *memid = MI_MEMID_OS;
  return _mi_os_alloc_aligned(size, alignment, *commit, large, tld);
}

void* _mi_arena_alloc(size_t size, bool* commit, bool* large, bool* is_zero, size_t* memid, mi_os_tld_t* tld)
{
  return _mi_arena_alloc_aligned(size, MI_ARENA_BLOCK_SIZE, commit, large, is_zero, memid, tld);
}

/* -----------------------------------------------------------
  Arena free
----------------------------------------------------------- */

void _mi_arena_free(void* p, size_t size, size_t memid, mi_stats_t* stats) {
  mi_assert_internal(size > 0 && stats != NULL);
  if (p==NULL) return;
  if (size==0) return;
  if (memid == MI_MEMID_OS) {
    // was a direct OS allocation, pass through
    _mi_os_free(p, size, stats);
  }
  else {
    // allocated in an arena
    size_t arena_idx;
    size_t bitmap_idx;
    mi_memid_indices(memid, &arena_idx, &bitmap_idx);
    mi_assert_internal(arena_idx < MI_MAX_ARENAS);
    mi_arena_t* arena = (mi_arena_t*)mi_atomic_read_ptr_relaxed(mi_atomic_cast(void*, &mi_arenas[arena_idx]));
    mi_assert_internal(arena != NULL);
    if (arena == NULL) {
      _mi_fatal_error("trying to free from non-existent arena: %p, size %zu, memid: 0x%zx\n", p, size, memid);
      return;
    }
    mi_assert_internal(arena->field_count > mi_bitmap_index_field(bitmap_idx));
    if (arena->field_count <= mi_bitmap_index_field(bitmap_idx)) {
      _mi_fatal_error("trying to free from non-existent arena block: %p, size %zu, memid: 0x%zx\n", p, size, memid);
      return;
    }
    const size_t blocks = mi_block_count_of_size(size);
    bool ones = mi_bitmap_unclaim(arena->blocks_map, arena->field_count, blocks, bitmap_idx);
    if (!ones) {
      _mi_fatal_error("trying to free an already freed block: %p, size %zu\n", p, size);
      return;
    };
  }
}

/* -----------------------------------------------------------
  Add an arena.
----------------------------------------------------------- */

static bool mi_arena_add(mi_arena_t* arena) {
  mi_assert_internal(arena != NULL);
  mi_assert_internal((uintptr_t)arena->start % MI_SEGMENT_ALIGN == 0);
  mi_assert_internal(arena->block_count > 0);
  
  uintptr_t i = mi_atomic_addu(&mi_arena_count,1);
  if (i >= MI_MAX_ARENAS) {
    mi_atomic_subu(&mi_arena_count, 1);
    return false;
  }
  mi_atomic_write_ptr(mi_atomic_cast(void*,&mi_arenas[i]), arena);
  return true;
}


/* -----------------------------------------------------------
  Reserve a huge page arena.
----------------------------------------------------------- */
#include <errno.h> // ENOMEM

// reserve at a specific numa node
int mi_reserve_huge_os_pages_at(size_t pages, int numa_node, size_t timeout_msecs) mi_attr_noexcept {
  if (pages==0) return 0;
  if (numa_node < -1) numa_node = -1;
  if (numa_node >= 0) numa_node = numa_node % _mi_os_numa_node_count();
  size_t hsize = 0;
  size_t pages_reserved = 0;
  void* p = _mi_os_alloc_huge_os_pages(pages, numa_node, timeout_msecs, &pages_reserved, &hsize);
  if (p==NULL || pages_reserved==0) {
    _mi_warning_message("failed to reserve %zu gb huge pages\n", pages);
    return ENOMEM;
  }
  _mi_verbose_message("reserved %zu gb huge pages\n", pages_reserved);
  
  size_t bcount = mi_block_count_of_size(hsize);
  size_t fields = (bcount + MI_BITMAP_FIELD_BITS - 1) / MI_BITMAP_FIELD_BITS;
  size_t asize = sizeof(mi_arena_t) + (2*fields*sizeof(mi_bitmap_field_t));  
  mi_arena_t* arena = (mi_arena_t*)_mi_os_alloc(asize, &_mi_stats_main); // TODO: can we avoid allocating from the OS?
  if (arena == NULL) {
    _mi_os_free_huge_pages(p, hsize, &_mi_stats_main);
    return ENOMEM;
  }
  arena->block_count = bcount;
  arena->field_count = fields;
  arena->start = (uint8_t*)p;  
  arena->numa_node = numa_node; // TODO: or get the current numa node if -1? (now it allows anyone to allocate on -1)
  arena->is_large = true;
  arena->is_zero_init = true;
  arena->search_idx = 0;
  arena->blocks_dirty = &arena->blocks_map[bcount];
  // the bitmaps are already zero initialized due to os_alloc
  // just claim leftover blocks if needed
  size_t post = (fields * MI_BITMAP_FIELD_BITS) - bcount;
  if (post > 0) {
    // don't use leftover bits at the end
    mi_bitmap_index_t postidx = mi_bitmap_index_create(fields - 1, MI_BITMAP_FIELD_BITS - post);
    mi_bitmap_claim(arena->blocks_map, fields, post, postidx); 
  }
  
  mi_arena_add(arena);
  return 0;
}


// reserve huge pages evenly among all numa nodes. 
int mi_reserve_huge_os_pages_interleave(size_t pages, size_t timeout_msecs) mi_attr_noexcept {
  if (pages == 0) return 0;

  // pages per numa node
  int numa_count = _mi_os_numa_node_count();
  if (numa_count <= 0) numa_count = 1;
  const size_t pages_per = pages / numa_count;
  const size_t pages_mod = pages % numa_count;
  const size_t timeout_per = (timeout_msecs / numa_count) + 50;
  
  // reserve evenly among numa nodes
  for (int numa_node = 0; numa_node < numa_count && pages > 0; numa_node++) {
    size_t node_pages = pages_per;  // can be 0
    if ((size_t)numa_node < pages_mod) node_pages++;
    int err = mi_reserve_huge_os_pages_at(node_pages, numa_node, timeout_per);
    if (err) return err;
    if (pages < node_pages) {
      pages = 0;
    }
    else {
      pages -= node_pages;
    }
  }

  return 0;
}

int mi_reserve_huge_os_pages(size_t pages, double max_secs, size_t* pages_reserved) mi_attr_noexcept {
  UNUSED(max_secs);
  _mi_warning_message("mi_reserve_huge_os_pages is deprecated: use mi_reserve_huge_os_pages_interleave/at instead\n");
  if (pages_reserved != NULL) *pages_reserved = 0;
  int err = mi_reserve_huge_os_pages_interleave(pages, (size_t)(max_secs * 1000.0));  
  if (err==0 && pages_reserved!=NULL) *pages_reserved = pages;
  return err;
}
