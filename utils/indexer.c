
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <assert.h>

#include "indexer.h"
#include "timer.h"

static void cleanup();
static void load_file(char *filename, block *b);
static void get_sub_block(block *blk, block *sub, unsigned int n, unsigned int p);
static void raw_data_parser(void *data, long i, int tid);
static void reconstruct_key(void *data, long i, int tid);
static void indexing(struct bloom *bloom, char *keys, char *seg);
static void allocate_hashmaps();
static void integrate_counters();
static void print_segments();
static void allocate_bloom_filters();
static void fill_bloom_filters();

static void test_reconstruction(char *filename);
// static void test_bloom(void *data, long i, int tid);

static struct keys_vec tokenize_block(block *b, char *token);

static int usage(){ printf("usage: pack file\n"); return 1;}

int main(int argc, char *argv[]) {

    if(argc < 2) return usage();

    nthr = sysconf(_SC_NPROCESSORS_ONLN);

    fprintf(stderr, "PROC: %d\n", nthr);

    fprintf(stderr, "//// LOADING ////\n");
    timer_start();
    load_file(argv[1], &raw_data_bl);
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


    if(argc >= 3) {
        fprintf(stderr, "//// TEST ////\n");
        timer_start();
        test_reconstruction(argv[2]);
        timer_stop();
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

// load whole file to mem
// adds \n\0 at the end
static void load_file(char *filename, block *b) {

    FILE *infile;

    infile = fopen(filename, "r");
    if(infile == NULL) return;

    fseek(infile, 0L, SEEK_END);
    b->length = ftell(infile);

    fseek(infile, 0L, SEEK_SET);

    b->start = (char*)calloc(b->length+2, sizeof(char));
    if(b->start == NULL) return;

    fread(b->start, sizeof(char), b->length, infile);
    fclose(infile);

    b->start[b->length  ] = '\n';
    b->start[b->length+1] = '\0';
    b->length += 2;
}


// split block on 'p' parts
// get part n
// search first \n for start
// search last  \n for length
// WARNING p must be less than number of strings x3!
static void get_sub_block(block *blk,
                   block *sub,
                   unsigned int n,
                   unsigned int p) {

    if(n>p-1 || p > 128) return;

    char *E = blk->start + blk->length;
    unsigned int l = blk->length / p;
    char *s = blk->start + n*l;
    char *e = s + l;

    if (n>0)
        while ( s < E && s[-1] != '\n') s++;

    // if (n<p-1)
        while ( e < E && e[-1] != '\n') e++;

    if (s == e) return;
    sub->start  = s;
    sub->length = (n<p-1) ? e-s : E-s;

}

// parser for raw format
// threaded function
static void raw_data_parser(void *data, long i, int tid) {

    assert(&raw_data_bl.start != NULL);

    block sub = {NULL, 0};
    get_sub_block(&raw_data_bl, &sub, i, nthr);

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
static void reconstruct_key(void *data, long i, int tid) {
    char *key = (char*) data;
    for(khiter_t ki=kh_begin(seg_bloom); ki!=kh_end(seg_bloom); ++ki) {
        if(kh_exist(seg_bloom, ki)) {
             struct bloom *b = kh_value(seg_bloom, ki);
             char *seg = (char*) kh_key(seg_bloom, ki);
             int s = bloom_check(b, key, strlen(key));
             if(s) dstr_add(key_segs[i], seg);
        }
    }
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
static struct keys_vec tokenize_block(block *b, char *token) {

    struct keys_vec v;
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
           long cap = (long) pow(10.0, floor(1.0+log10(cnt)));
           // insret
           int ret;
           khiter_t ki;
           struct bloom *bloom;
           // add key to segment
           ki = kh_put(key_bloom_hm, seg_bloom, key, &ret);
           if(ret) {
               bloom = (struct bloom*)malloc(sizeof(struct bloom)); 
               bloom_init(bloom, cap, 0.01);
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


// Run through keys and reconstruct
static void test_reconstruction(char *filename) {

    load_file(filename, &keys_bl);
    assert(keys_bl.start != NULL);

    // tokenize string
    test_keys_vec = tokenize_block(&keys_bl, "\n");

    fprintf(stderr, "seg_bloom: [%d..%d) %d\n", kh_begin(seg_bloom), kh_end(seg_bloom), kh_size(seg_bloom));
    // kid -> linked list
    kt_for(nthr, reconstruct_key, 0, kv_size(test_keys_vec));

    ////////////////////////
    // dealocate file memory
    free(keys_bl.start);
    keys_bl.start = NULL;

    // cleanup
    kv_destroy(test_keys_vec);

    // dealocate file memory
    free(keys_bl.start);
    keys_bl.start = NULL;
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
