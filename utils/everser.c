
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <assert.h>
#include <sys/time.h>

#include "khash.h"

typedef struct {
    char *start;
    unsigned long length;
} block;

typedef struct {
    char *buf;
    char *crs;
    unsigned int length;
} dstr;

int nthr;

KHASH_MAP_INIT_STR(key_dstr, dstr*);
KHASH_MAP_INIT_STR(key_int , unsigned long);

typedef struct {
    block              *b;
    khash_t(key_dstr) **seg_keys;
    khash_t(key_int ) **seg_cnts;

} data_t;

dstr* dstr_new() {
    dstr* k = malloc(sizeof(dstr));
    if(k == NULL) return NULL;
    k->buf = malloc(sizeof(char)*256);
    if(k->buf == NULL) {free(k); return NULL;}
    k->crs = k->buf;
    k->length = 256;
    return k;
}

static struct timeval tm1;

static inline void timer_start() {
    gettimeofday(&tm1, NULL);
}

static inline void timer_stop() {
    struct timeval tm2;
    gettimeofday(&tm2, NULL);

    unsigned long long t = 1000 * (tm2.tv_sec - tm1.tv_sec) + (tm2.tv_usec - tm1.tv_usec) / 1000;
    fprintf(stderr, "%llu ms\n", t);
}

void dstr_free(dstr* k) {
    free(k->buf);
    free(k);
}

void dstr_add(dstr *k, char *v) {
    int l = strlen(v);
    int s = k->crs - k->buf;
    // printf("add %s %d %d\n", v, l, s);
    if(s+l+2 > k->length) {
        k->buf = realloc(k->buf, sizeof(char)*k->length*2);
        k->crs = k->buf + s;
        k->length *= 2;
    }
    strcpy(k->crs, v);
    k->crs += l+1;
    k->crs[-1] = ' ';
    k->crs[ 0] = 0;
}

void loadFile(char *filename, block *b) {

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

static int usage(){ printf("usage: pack file\n"); return 1;}

// split blocke on 'p' parts
// get part n
// search first \n for start
// search last  \n for length
// WARNING p must be less than number of strings x3!

void getSubBlock(block *blk, 
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

// threaded function
static void parser(void *data, long i, int tid) {

    data_t *d = (data_t*)data;
    block sub = {NULL, 0};
    getSubBlock(d->b, &sub, i, nthr);

    // printf("%.*s\n", sub.length, sub.start);
    
    if(sub.start == NULL) return;

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
            dstr *ks;

            // add key to segment
            ki = kh_put(key_dstr, d->seg_keys[i], s, &ret);
            if(ret == 0) { 
                ks = kh_value(d->seg_keys[i], ki);
            } else if(ret == 1) {
                ks = dstr_new();
                kh_value(d->seg_keys[i], ki) = ks;
            }
            dstr_add(ks, key);

            // incriment segment
            ki = kh_put(key_int, d->seg_cnts[i], s, &ret);
            if(ret == 0) { // sgement exists
                kh_value(d->seg_cnts[i], ki)+=1; 
            } else if(ret == 1) { // new segment
                kh_value(d->seg_cnts[i], ki) =1; 
            }

            // next token
            s = strtok_r(NULL, "/", &sv);
        }

        key = end+1;
    } while (*key!='\0' && key<E);
    
}

// static void print_data(data_t *d) {
//     for (khiter_t ki=kh_begin(d->seg_keys[i]); ki!=kh_end(d->seg_keys[i]); ++ki) {
//         if (kh_exist(d->seg_keys[i], ki)) {
//             char *key = (char*) kh_key(d->seg_keys[i], ki);
//             printf("create %s\n", key);
//             printf("b %s %s\n", key, kh_value(d->seg_keys[i], ki)->buf);
//         }    
//     }
// }

// n_threads - number of threads
// function (data, call number, thread id)
// data
// number of calls
void kt_for(int n_threads, void (*func)(void*, long, int), void *data, long n);

int main(int argc, char *argv[]) {

    if(argc < 2) return usage();

    nthr = sysconf(_SC_NPROCESSORS_ONLN);

    fprintf(stderr, "PROC: %d\n", nthr);

    

    fprintf(stderr, "//// LOADING ////\n");
    
    timer_start();    
    block b = {NULL, 0};
    
    loadFile(argv[1], &b);
    assert(b.start != NULL);

    // init hash map storages
    // seg -> dstr (keys)
    // seg -> usage couter
    khash_t(key_dstr) *seg_keys[nthr];
    khash_t(key_int ) *seg_cnts[nthr];

    for(int i=0; i<nthr; i++) {
        seg_keys[i] = kh_init(key_dstr);
        seg_cnts[i] = kh_init(key_int );
    }

    data_t d = {
        .b        = &b, 
        .seg_keys =  seg_keys, 
        .seg_cnts =  seg_cnts
    };
    // block s = {NULL, 0};
    // getSubBlock(&b, &s, i, bc);
    timer_stop();
    
    
    fprintf(stderr, "//// MAP ////\n");
    
    timer_start();
    // number of concurent threads, calee(data, iteration, thread id), data, number of iterations
    kt_for(nthr, parser, &d, nthr);
    timer_stop();

    
    fprintf(stderr, "//// REDUCE ////\n");

    timer_start();
    // Summing segment counters
    khash_t(key_int) *seg_cnt = kh_init(key_int);
    for(int i=0; i<nthr; i++) {
        fprintf(stderr, "Block %d\n", i);
        for (khiter_t ki=kh_begin(d.seg_cnts[i]); ki!=kh_end(d.seg_cnts[i]); ++ki) {
            if (kh_exist(d.seg_cnts[i], ki)) {
                int ret;
                char *seg = (char*) kh_key(d.seg_cnts[i], ki);
                khiter_t si;
                // printf("%s %lu \n", seg, kh_value(d.seg_cnts[i], ki));
                si = kh_put(key_int, seg_cnt, seg, &ret);
                if(ret == 0) { // sеgment exists
                    kh_value(seg_cnt, si)+= kh_value(d.seg_cnts[i], ki); 
                } else if(ret == 1) { // new segment
                    kh_value(seg_cnt, si) = kh_value(d.seg_cnts[i], ki); 
                }
            }
        }
    }
    timer_stop();
    
    
    fprintf(stderr, "//// OUTPUT ////\n");

    timer_start();
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
        for (khiter_t ki=kh_begin(d.seg_keys[i]); ki!=kh_end(d.seg_keys[i]); ++ki) {
            if (kh_exist(d.seg_keys[i], ki)) {
                int ret;
                char *seg = (char*) kh_key(d.seg_keys[i], ki);
                printf("b %s %s\n", seg, kh_value(d.seg_keys[i], ki)->buf);
            }
        }
    }
    timer_stop();
    
    
    fprintf(stderr, "//// CLEANUP ////\n");
    timer_start();

    kh_destroy(key_int, seg_cnt);
    
    // free hash map storages
    for(int i=0; i<nthr; i++) {    
        // free dynamic strings
        for (khiter_t ki=kh_begin(seg_keys[i]); ki!=kh_end(seg_keys[i]); ++ki) {
            if (kh_exist(seg_keys[i], ki)) dstr_free(kh_value(seg_keys[i], ki));
        }
        // free has maps
        kh_destroy(key_dstr, seg_keys[i]);
        kh_destroy(key_int , seg_cnts[i]);
    }

    // free file data
    free(b.start);
    timer_stop();

    return 0;

}
