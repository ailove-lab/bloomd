
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

static int usage(){ printf("usage: stat file\n"); return 1;}

// template for hash type, 
KHASH_MAP_INIT_STR(hash, int)

static void parse(block *b) {

    khash_t(hash) *stat = kh_init(hash);

    char *key, *seg, *tab, *end, *E;
    E = b->start + b->length;
    key = b->start;

    // iterate throug lines
    do {

        // [HASH]\t[FLAGS] [SEGMENTS]\n
        // ^key  ^tab                ^end

        end = strchr(key, '\n');     // find end
        if (end == NULL) break;
        *end = 0;                    // split entry
        
        tab = strchr(key, '\t');     // find tab
        if(tab == NULL) {
            key = end+1;
            continue;
        } 
        *tab = 0;                    // split key/data
        
        seg  = strrchr(tab+1, ' ');   // find segments from tab to last space
        *seg = 0;                     // data to flags/segments
        seg++;

        // iterate through segments
        char *s, *sv;
        s = strtok_r(seg, "/",&sv);
        while (s!=NULL) {
            
            int ret;
            khiter_t ki;
            keys *ks;
            // create key in stat with s name
            ki = kh_put(hash, stat, s, &ret);
            if(ret == 0) { // key already exist 
                kh_value(stat, ki) += 1;
            } else if(ret == 1) { // new key
                kh_value(stat, ki) = 1;
            }
            // next token
            s = strtok_r(NULL, "/",&sv);
        }
        key = end+1;
    } while (*key!='\0' && key<E);

    // SEND HERE
    for (khiter_t ki=kh_begin(stat); ki!=kh_end(stat); ++ki) {
        if (kh_exist(stat, ki)) {
            char *key = (char*) kh_key(stat, ki);
            printf("%s: %d\n", key, kh_value(stat, ki));
        }
    
    }

    kh_destroy(hash, stat);
    
}

int main(int argc, char *argv[]) {

    if(argc < 2) return usage();

    block b = {NULL, 0};

    loadFile(argv[1], &b);
    assert(b.start != NULL);
    parse(&b);

    free(b.start);

    return 0;

}
