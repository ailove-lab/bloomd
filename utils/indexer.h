#ifndef __INDEXER_H__
#define __INDEXER_H__

#include "khash.h"
#include "kvec.h"
#include "bloom.h"
#include "dstr.h"

typedef struct {
    char *start;
    unsigned long length;
} block;

struct keys_vec { 
    size_t n, m; 
    char **a; 
} keys_vec;

// INIT HASHMAPS
KHASH_MAP_INIT_STR(key_dstr_hm ,   dstr*        ); // key -> dynamic string
KHASH_MAP_INIT_STR(key_int_hm  ,   unsigned long); // key -> int
KHASH_MAP_INIT_STR(key_bloom_hm, struct bloom*); // key -> bloom

// this structure passed to threaded parser
typedef struct {
    block              *b;        // parsed data block
    khash_t(key_dstr_hm) **seg_keys; // seg -> keys hash map
    khash_t(key_int_hm ) **seg_cnts; // seg -> count hash map
} data_seg_t;

// this structure passed to threaded reader
typedef struct {
    struct keys_vec *keys;
    khash_t(key_bloom_hm) *seg_bloom;
} data_bloom_t;

// decalred at kthread.c
void kt_for(int n_threads, void (*func)(void*, long, int), void *data, long n);

// hash maps
static khash_t(key_dstr_hm) **seg_keys;  // [seg -> keys string] per thread
static khash_t(key_int_hm ) **seg_cnts;  // [seg -> counters   ] per thread
static khash_t(key_int_hm)   *seg_cnt;   //  seg -> counter      integral
static khash_t(key_bloom_hm) *seg_bloom; //  seg -> bloom        bloom filters

static block data_block;
static data_seg_t d;

// test
static block keys_block;
static struct keys_vec test_keys_vec;

#endif