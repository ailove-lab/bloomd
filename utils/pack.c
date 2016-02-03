
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
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
} keys;

int nthr;


keys* keys_new() {
    keys* k = malloc(sizeof(keys));
    if(k == NULL) return NULL;
    k->buf = malloc(sizeof(char)*256);
    if(k->buf == NULL) {free(k); return NULL;}
    k->crs = k->buf;
    k->length = 256;
    return k;
}

static struct timeval tm1;

static inline void start()
{
    gettimeofday(&tm1, NULL);
}

static inline void stop()
{
    struct timeval tm2;
    gettimeofday(&tm2, NULL);

    unsigned long long t = 1000 * (tm2.tv_sec - tm1.tv_sec) + (tm2.tv_usec - tm1.tv_usec) / 1000;
    fprintf(stderr, "%llu ms\n", t);
}

void keys_free(keys* k) {
    free(k->buf);
    free(k);
}

void keys_add(keys *k, char *v) {
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
KHASH_MAP_INIT_STR(hash, keys*)

static void parser(void *blk, long i, int tid) {

    block *b = (block*) blk;
    block sub = {NULL, 0};
    getSubBlock(b, &sub, i, nthr);
    if(sub.start == NULL) return;

    khash_t(hash) *s2k = kh_init(hash);

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
        *seg = 0;                     // sukot data to flags/segments
        seg++;

        // iterate through segments
        char *s, *sv;
        s = strtok_r(seg, "/",&sv);
        while (s!=NULL) {
            
            int ret;
            khiter_t ki;
            keys *ks;
            ki = kh_put(hash, s2k, seg, &ret);
            
            if(ret == 0) {
                ks = kh_value(s2k, ki);
            } else if(ret == 1) {
                ks = keys_new();
                kh_value(s2k, ki) = ks;
            }
            keys_add(ks, key);

            // next token
            s = strtok_r(NULL, "/",&sv);
        }

        key = end+1;
    } while (*key!='\0' && key<E);

    // SEND HERE
    for (khiter_t ki=kh_begin(s2k); ki!=kh_end(s2k); ++ki) {
        if (kh_exist(s2k, ki)) {
            char *key = (char*) kh_key(s2k, ki);
            printf("create %s\n", key);
            printf("b %s %s\n", key, kh_value(s2k, ki)->buf);
        }

    }

    // FREE
    for (khiter_t ki=kh_begin(s2k); ki!=kh_end(s2k); ++ki) {
        if (kh_exist(s2k, ki)) keys_free(kh_value(s2k, ki));
    }
    kh_destroy(hash, s2k);
    
}


// n_threads - number of threads
// function (data, call number, thread id)
// data
// number of calls
void kt_for(int n_threads, void (*func)(void*, long, int), void *data, long n);

int main(int argc, char *argv[]) {

    if(argc < 2) return usage();

    nthr = sysconf(_SC_NPROCESSORS_ONLN);

    fprintf(stderr, "Processors: %d\n", nthr);
    block b = {NULL, 0};

    loadFile(argv[1], &b);
    assert(b.start != NULL);
    
    // block s = {NULL, 0};
    // getSubBlock(&b, &s, i, bc);
    start();
    kt_for(nthr, parser, &b, nthr);
    stop();
    free(b.start);

    return 0;

}
