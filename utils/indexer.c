
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <assert.h>

#include "indexer.h"
#include "timer.h"

#include "khash.h"
#include "kvec.h"
#include "bloom.h"
#include "dstr.h"
#include "map.h"

  ///////////
 /// T&V ///
///////////

// vector of strings (kvec_t(char*))
typedef struct {
    size_t n, m;
    char **a;
} char_vec_t;

typedef struct {
    char_vec_t  keys;
    char_vec_t *segs;
} rec_keys_t;

// INIT HASHMAPS
KHASH_MAP_INIT_STR(key_dstr_hm ,   dstr*        ); // key -> dynamic string
KHASH_MAP_INIT_STR(key_int_hm  ,   unsigned long); // key -> int
KHASH_MAP_INIT_STR(key_bloom_hm,   struct bloom*); // key -> bloom

// this structure passed to threaded parser
typedef struct {
    map_block_t               *b;        // parsed data block
    khash_t(key_dstr_hm) **seg_keys; // seg -> keys hash map
    khash_t(key_int_hm ) **seg_cnts; // seg -> count hash map
} data_seg_t;

// this structure passed to threaded reader
typedef struct {
    struct char_vec_t *keys;
    khash_t(key_bloom_hm) *seg_bloom;
} data_bloom_t;

// number of threads
static int nthr;

// hash maps
static khash_t(key_dstr_hm) **seg_keys;  // [seg -> keys string] per thread
static khash_t(key_int_hm ) **seg_cnts;  // [seg -> counters   ] per thread
static khash_t(key_int_hm)   *seg_cnt;   //  seg -> counter      integral
static khash_t(key_bloom_hm) *seg_bloom; //  seg -> bloom        bloom filters

static map_block_t raw_data_bl;
// static data_seg_t d;

  ////////////////
 // FUNCTIONS ///
////////////////

// external kthread.c
void kt_for(int n_threads, void (*func)(void*, long, int), void *data, long n);

static void cleanup();
static void raw_data_parser(void *data, long i, int tid);
static void reconstruct_key(void *data, long i, int tid);
static void indexing(struct bloom *bloom, char *keys, char *seg);
static void allocate_hashmaps();
static void integrate_counters();
static void print_segments();
static void allocate_bloom_filters();
static void fill_bloom_filters();
static void save_blooms();
static void print_blooms();

// static void test_bloom(void *data, long i, int tid);

static char_vec_t tokenize_block(map_block_t *b, char *token);
static int usage(){ printf("usage: pack file\n"); return 1;}

// TEST //

static void test_reconstruction(char *filename);


  ////////////
 /// MAIN ///
////////////


int main(int argc, char *argv[]) {

    if(argc < 2) return usage();

    nthr = sysconf(_SC_NPROCESSORS_ONLN);

    fprintf(stderr, "PROC: %d\n", nthr);

    fprintf(stderr, "//// LOADING ////\n");
    timer_start();
    map_load_block(argv[1], &raw_data_bl);
    assert(raw_data_bl.start != NULL);
    timer_stop();

    allocate_hashmaps();

    fprintf(stderr, "//// MAP ////\n");
    timer_start();
    kt_for(nthr, raw_data_parser, 0, nthr);
    timer_stop();

    fprintf(stderr, "//// REDUCE ////\n");
    timer_start();
    integrate_counters();
    timer_stop();

    if(0) {
        fprintf(stderr, "//// OUTPUT ////\n");
        timer_start();
        print_segments();
        timer_stop();
    }

    fprintf(stderr, "//// INDEXING ////\n");

    fprintf(stderr, "Allocate filters\n");
    timer_start();
    allocate_bloom_filters();
    timer_stop();

    fprintf(stderr, "Filling filters\n");
    timer_start();
    fill_bloom_filters();
    timer_stop();

    save_blooms();
    // print_blooms();

    if(argc >= 3) {
        fprintf(stderr, "//// TEST ////\n");
        test_reconstruction(argv[2]);
    }

    fprintf(stderr, "//// CLEANUP ////\n");
    timer_start();
    cleanup();
    timer_stop();

    return 0;

}

// dealocate memory
static void cleanup() {

    fprintf(stderr, "Free blooms\n");
    for (khiter_t ki=kh_begin(seg_bloom); ki!=kh_end(seg_bloom); ++ki) {
        if (kh_exist(seg_bloom, ki)) {
            struct bloom *b = kh_value(seg_bloom, ki);
            bloom_free(b);
            free(b);
        }
    }
    kh_destroy(key_bloom_hm, seg_bloom);

    fprintf(stderr, "Free integral seg counter\n");
    kh_destroy(key_int_hm, seg_cnt);

    fprintf(stderr, "Free seg_keys[] and seg_cnts[]\n");
    for(int i=0; i<nthr; i++) {
        // free dynamic strings
        for (khiter_t ki=kh_begin(seg_keys[i]); ki!=kh_end(seg_keys[i]); ++ki) {
            if (kh_exist(seg_keys[i], ki)) dstr_free(kh_value(seg_keys[i], ki));
        }
        // free has maps
        kh_destroy(key_dstr_hm, seg_keys[i]);
        kh_destroy(key_int_hm , seg_cnts[i]);
    }
    free(seg_keys);
    free(seg_cnts);

    // dealocate file memory
    free(raw_data_bl.start);
    raw_data_bl.start = NULL;

}

// parser for raw format
// threaded function
static void raw_data_parser(void *data, long i, int tid) {

    assert(&raw_data_bl.start != NULL);

    map_block_t sub = {NULL, 0};
    map_get_sub_block(&raw_data_bl, &sub, i, nthr);

    assert(sub.start != NULL);

    char *key, *seg, *tab, *end, *E;
    E = sub.start + sub.length;
    key = sub.start;

    // iterate throug lines
    do {
        end = strchr(key, '\n');     // find end
        if (end == NULL) break;
        *end = 0;                    // split entry

        tab = strchr(key, '\t');     // find tab
        if(tab == NULL) {
            key = end+1;
            continue;
        }
        *tab = 0;                    // split key/data

        seg  = strrchr(tab+1, ' ');   // find segments
        *seg = 0;                     // data to flags/segments
        seg++;

        // iterate through segments
        char *s, *sv;
        s = strtok_r(seg, "/",&sv);
        while (s!=NULL) {

            int ret;
            khiter_t ki;
            dstr *ks = NULL;
            // add key to segment
            ki = kh_put(key_dstr_hm, seg_keys[i], s, &ret);
            if(ret == 0) {
                ks = kh_value(seg_keys[i], ki);
            } else if(ret == 1) {
                ks = dstr_new();
                kh_value(seg_keys[i], ki) = ks;
            }
            dstr_add(ks, key);

            // incriment segment
            ki = kh_put(key_int_hm, seg_cnts[i], s, &ret);
            if(ret == 0) { // sgement exists
                kh_value(seg_cnts[i], ki)+=1;
            } else if(ret == 1) { // new segment
                kh_value(seg_cnts[i], ki) =1;
            }
            // next token
            s = strtok_r(NULL, "/", &sv);
        }

        key = end+1;
    } while (*key!='\0' && key<E);
}


// threaded reconstruction of single key
// iterates through blooms and recover seg names
static void reconstruct_key(void *rec_keys, long i, int tid) {

    char_vec_t  keys = ((rec_keys_t*)rec_keys)->keys;
    char_vec_t *segs = ((rec_keys_t*)rec_keys)->segs;
    // printf("%p %p\n", keys, segs);
    char *key = (char*) kv_A(keys, i);
    for(khiter_t ki=kh_begin(seg_bloom); ki!=kh_end(seg_bloom); ++ki) {
        if(kh_exist(seg_bloom, ki)) {
            struct bloom *b = kh_value(seg_bloom, ki);
            char *seg = (char*) kh_key(seg_bloom, ki);
            int s = bloom_check(b, key, strlen(key));
            if(s) kv_push(char*, segs[i], seg);
        }
    }
    // printf("thread: %d, key %ld, segs: %zu\n", tid, i, segs[i].n);
}


// put space separated keys to bloom filter
static void indexing(struct bloom *bloom, char *keys, char *seg) {

    // iterate through keys
    char *keys_dup, *key, *sv;
    keys_dup = strdup(keys);
    key = strtok_r(keys_dup, " ",&sv);
    while (key!=NULL) {
        // 0 - added; 1 - collision; -1 - filter not initialized
        bloom_add(bloom, key, strlen(key));
        // next token
        key = strtok_r(NULL, " ", &sv);
    }
    free(keys_dup);
}


// generate array of segments
// original string modified, token replaced with zero
static char_vec_t tokenize_block(map_block_t *b, char *token) {

    char_vec_t v;
    kv_init(v);

    char *seg, *sv;
    seg = strtok_r(b->start, token, &sv);

    while (seg!=NULL) {
        kv_push(char*, v, seg);
        seg = strtok_r(NULL, token, &sv);
    }
    return v;
}


// init hashmaps for evrsed seg -> kes data
static void allocate_hashmaps() {
    // init hash map storages
    // seg -> dstr (keys)
    // seg -> usage couter
    seg_keys = (khash_t(key_dstr_hm)**) malloc(nthr * sizeof(khash_t(key_dstr_hm)*));
    seg_cnts = (khash_t(key_int_hm )**) malloc(nthr * sizeof(khash_t(key_int_hm )*));

    // init segment hashes
    for(int i=0; i<nthr; i++) {
        seg_keys[i] = kh_init(key_dstr_hm);
        seg_cnts[i] = kh_init(key_int_hm );
    }
}


// sum all seg_cnts[] to one aray
static void integrate_counters() {
    // Summing segment counters
    seg_cnt = kh_init(key_int_hm);
    for(int i=0; i<nthr; i++) {
        fprintf(stderr, "Block %d\n", i);
        for (khiter_t ki=kh_begin(seg_cnts[i]); ki!=kh_end(seg_cnts[i]); ++ki) {
            if (kh_exist(seg_cnts[i], ki)) {
                int ret;
                char *seg = (char*) kh_key(seg_cnts[i], ki);
                khiter_t si;
                // printf("%s %lu \n", seg, kh_value(seg_cnts[i], ki));
                si = kh_put(key_int_hm, seg_cnt, seg, &ret);
                if(ret == 0) { // sеgment exists
                    kh_value(seg_cnt, si)+= kh_value(seg_cnts[i], ki);
                } else if(ret == 1) { // new segment
                    kh_value(seg_cnt, si) = kh_value(seg_cnts[i], ki);
                }
            }
        }
    }
}


// print reversed seg -> keys data if bloomd format
static void print_segments() {

    // segment counters
    for (khiter_t ki=kh_begin(seg_cnt); ki!=kh_end(seg_cnt); ++ki) {
        if (kh_exist(seg_cnt, ki)) {
            char *key = (char*) kh_key(seg_cnt, ki);
            long cnt = kh_value(seg_cnt, ki);
            long cap = (long) pow(10.0, floor(1.0+log10(cnt)));
            fprintf(stderr, "// %s %lu -> %lu\n", key, cnt, cap);
            printf("create %s capacity=%lu\n", key, cap);
        }
    }

    // segment keys
    for(int i=0; i<nthr; i++) {
        for (khiter_t ki=kh_begin(seg_keys[i]); ki!=kh_end(seg_keys[i]); ++ki) {
            if (kh_exist(seg_keys[i], ki)) {
                char *seg = (char*) kh_key(seg_keys[i], ki);
                printf("b %s %s\n", seg, kh_value(seg_keys[i], ki)->buf);
            }
        }
    }
}


// allocate bloom filters
static void allocate_bloom_filters() {

    seg_bloom = kh_init(key_bloom_hm);
    for (khiter_t ki=kh_begin(seg_cnt); ki!=kh_end(seg_cnt); ++ki) {
       if (kh_exist(seg_cnt, ki)) {
           // get key counter
           char *key = (char*) kh_key(seg_cnt, ki);
           long cnt = kh_value(seg_cnt, ki);
           // long cap = (long) pow(10.0, floor(1.0+log10(cnt)));
           // insret
           int ret;
           khiter_t ki;
           struct bloom *bloom;
           // add key to segment
           ki = kh_put(key_bloom_hm, seg_bloom, key, &ret);
           if(ret) {
               bloom = (struct bloom*)malloc(sizeof(struct bloom));
               //bloom_init(bloom, cap, 0.01);   //allocate with power of set
               bloom_init(bloom, cnt, 0.01);     //precise size
               kh_value(seg_bloom, ki) = bloom;
           } else {
               fprintf(stderr, "Something wrong with %s\n", key);
               break;
           }
       }
    }
    fprintf(stderr, "Created %d bloom filters\n", kh_size(seg_bloom));

}


// populate bloom filters with agregated data
static void fill_bloom_filters() {
    for(int i=0; i<nthr; i++) {
        for (khiter_t ki=kh_begin(seg_keys[i]); ki!=kh_end(seg_keys[i]); ++ki) {
            if (kh_exist(seg_keys[i], ki)) {
                // get seg name
                char *seg = (char*) kh_key(seg_keys[i], ki);
                // get bloom iterator
                khiter_t bi = kh_get(key_bloom_hm, seg_bloom, seg);
                if (bi != kh_end(seg_bloom)) {
                    struct bloom *bloom = kh_value(seg_bloom, bi);
                    indexing(bloom, kh_value(seg_keys[i], ki)->buf, seg);
                }
            }
        }
    }
}

// print bloom filters stat
static void print_blooms() {
    for(khiter_t ki=kh_begin(seg_bloom); ki!=kh_end(seg_bloom); ++ki) {
        if(kh_exist(seg_bloom, ki)) {
              struct bloom *bloom = kh_value(seg_bloom, ki);
              // fprintf(stderr,"bloom at %p\n", (void *)bloom);
              fprintf(stderr," ->entries       = %d\n", bloom->entries);
              fprintf(stderr," ->error         = %f\n", bloom->error);
              fprintf(stderr," ->bits          = %d\n", bloom->bits);
              fprintf(stderr," ->bits per elem = %f\n", bloom->bpe);
              fprintf(stderr," ->bytes         = %d\n", bloom->bytes);
              fprintf(stderr," ->buckets       = %u\n", bloom->buckets);
              // fprintf(stderr," ->bucket_bytes  = %u\n", bloom->bucket_bytes);
              // fprintf(stderr," ->bucket_bytes_exponent = %u\n",
              //        bloom->bucket_bytes_exponent);
              // fprintf(stderr," ->bucket_bits_fast_mod_operand = 0%o\n",
              //        bloom->bucket_bits_fast_mod_operand);
              // fprintf(stderr," ->hash functions = %d\n", bloom->hashes);
        }
    }
}

// safe blooms
static void save_blooms() {
    for(khiter_t ki=kh_begin(seg_bloom); ki!=kh_end(seg_bloom); ++ki) {
        if(kh_exist(seg_bloom, ki)) {
               struct bloom *bloom = kh_value(seg_bloom, ki);
               char *seg = (char*) kh_key(seg_bloom, ki);
               char filename[128];
               sprintf(filename,"./blooms/%s", seg);
               bloom_save(bloom, filename);
        }
    }
}

  /////////////
 /// TESTS ///
/////////////


// Run through keys and reconstruct
static void test_reconstruction(char *filename) {

    map_block_t     test_keys_bl;
    char_vec_t  test_keys_vec; // vector of test keys
    char_vec_t *test_keys_segs; // array of segment vectors

    fprintf(stderr, "Loading keys ");
    timer_start();
    map_load_block(filename, &    test_keys_bl);
    timer_stop();
    assert(test_keys_bl.start != NULL);

    fprintf(stderr, "Vectorize keys ");
    timer_start();
    // tokenize string
    test_keys_vec = tokenize_block(&test_keys_bl, "\n");
    timer_stop();

    fprintf(stderr,
        "seg_bloom: [%d..%d) %d\n",
        kh_begin(seg_bloom),
        kh_end(seg_bloom),
        kh_size(seg_bloom));

    // init test_keys_segs
    // size_t segs_count = kh_size(seg_bloom);
    size_t keys_count = kv_size(test_keys_vec);
    fprintf(stderr, "Init storage for %zu keys ", keys_count);
    timer_start();
    test_keys_segs = (char_vec_t*) malloc(keys_count * sizeof(char_vec_t));
    for(int i=0; i<keys_count; ++i) kv_init(test_keys_segs[i]);
    timer_stop();

    rec_keys_t rec_keys = {test_keys_vec, test_keys_segs};
    fprintf(stderr, "Restore %zu keys ", keys_count);
    timer_start();
    kt_for(nthr, reconstruct_key, &rec_keys, keys_count);
    timer_stop();

    // char *key = (char*) kv_A(*keys, i);
    // for(khiter_t ki=kh_begin(seg_bloom); ki!=kh_end(seg_bloom); ++ki) {
    //     if(kh_exist(seg_bloom, ki)) {
    //         struct bloom *b = kh_value(seg_bloom, ki);
    //         char *seg = (char*) kh_key(seg_bloom, ki);
    //         int s = bloom_check(b, key, strlen(key));
    //         if(s) kv_push(char*, *(segs[i]), seg);
    //     }
    // }

    // CLEAN UP //
    fprintf(stderr, "Cleanup ");
    timer_start();

    // clear segments list
    for(int i=0; i<keys_count; ++i) kv_destroy(test_keys_segs[i]);

    free(test_keys_segs);

    free(test_keys_bl.start);
    test_keys_bl.start = NULL;

    // cleanup
    kv_destroy(test_keys_vec);

    // dealocate file memory
    free(test_keys_bl.start);
        test_keys_bl.start = NULL;

    timer_stop();
}

/*
// threaded function
static void test_bloom(void *data, long i, int tid) {

    if(kh_exist(seg_bloom, i)) {

        struct bloom *b = kh_value(seg_bloom, i);
        fprintf(stderr, "thread: %d, seg: %ld, keys: %zu\n", tid, i, kv_size(test_keys_vec));

        for(unsigned int ki=0; ki<kv_size(test_keys_vec); ki++ ) {

            char *key = kv_A(test_keys_vec, ki);
            if(bloom_check(b, key, strlen(key))) {

            };
            //fprintf(stderr, "%d", s);

    //        end = strchr(key, '\n');     // find end
    //        if (end == NULL) break
    //        *end = 0;
    //        // fprintf(stdout, "%s\t", key);
    //        for (khiter_t ki=kh_begin(seg_bloom); ki!=kh_end(seg_bloom); ++ki) {
    //            if (kh_exist(seg_bloom, ki)) {
    //                struct bloom *b = kh_value(seg_bloom, ki);
    //                char* seg = (char*) kh_key(seg_bloom, ki);
    //                // 0 - not present; 1 - present or collision; -1 - filter not initialized
    //                int s = bloom_check(b, key, strlen(key));
    //                // if (s) fprintf(stdout," %s", seg);
    //            }
    //       }

        }
        //fprintf(stderr, "\n");
    }
}
*/
