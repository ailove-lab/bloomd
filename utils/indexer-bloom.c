#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bloom.h"
#include "khash.h"

#include "timer.h"

// init type
KHASH_MAP_INIT_INT(key_bloom_hm, struct bloom*); // key -> filter*
khash_t(key_bloom_hm) *seg_bloom;

int usage();
int init_bloom(char *filename);
void index_pipe();
void raw_line_parser(char *line);
void save_filters(); 
void cleanup();

int main(int argc, char **argv) {
    
    if(argc != 2 ) return usage();

    if(init_bloom(argv[1])) {
        printf("Fail to open %s\n", argv[1]);
        return usage();
    }
    index_pipe();
    save_filters();
    cleanup();
}

// Read stat file
// init bloom filters
int init_bloom(char *filename) {

    fprintf(stderr, "Read stat and init filter\n");
    
    FILE * fp;
    char * line = NULL;
    size_t len = 0;
    size_t read;

    fp = fopen(filename, "r");
    if (fp == NULL) return -1;

    char filtername[256];
    seg_bloom = kh_init(key_bloom_hm);
    while ((read = getline(&line, &len, fp)) != -1) {
        
        char *cnt = strchr(line, ' ');
        if (cnt == NULL) continue;
        *cnt = 0;
        cnt++;
        
        int seg = atoi(line); 
        int counter = atoi(cnt);
        if (!seg || !counter) continue;
        if(counter<50) continue;
        int ret;
        khiter_t ki = kh_put(key_bloom_hm, seg_bloom, seg, &ret);
        if(ret == 1) {         // new
            struct bloom *bloom = (struct bloom*) malloc(sizeof(struct bloom));
            sprintf(filtername,"./blooms/%d", seg);
            timer_start();
            if(bloom_load(bloom, filtername) != 0) {
                fprintf(stderr, "Can't load %d, create new filter\n", seg);
                if(bloom_init(bloom, counter, 0.01) != 0) {
                    fprintf(stderr, "ERROR CREATING FILTER %d\n", seg);
                    continue;
                } else {
                    fprintf(stderr, "%d = %d (%p)\n", seg, counter, bloom);
                }
            } else fprintf(stderr, "Load ok %d\n", seg);
            timer_stop();
            kh_value(seg_bloom, ki) = bloom;
        };
    }

    fclose(fp);
    if (line) free(line);
    return 0;
}

void index_pipe() {

    char *line = NULL;
    size_t len = 0;
    while (getline(&line, &len, stdin) != -1) raw_line_parser(line);
    free(line);

}

// parse raw string
// base64 key [\t] spaced flags [\x20] eeeslashed segments [\n\0]
void raw_line_parser(char *line) {
    
    char *seg, *tab, *end;
    
    end = strchr(line, '\n');     // find end / new line
    if(end == NULL) return;       
    *end = 0;                     // replace it fith terminator

    tab = strchr(line, '\t');     // find tab
    if(tab == NULL) return;
    *tab = 0;                    // split key/data
    // get last space
    seg  = strrchr(tab+1, ' ');  // find segments
    if(seg == NULL) return;
    *seg = 0;                    // data to flags/segments
    seg++;

    // iterate through segments
    char *s, *sv;
    s = strtok_r(seg, "/", &sv);
    while (s!=NULL) {
        khiter_t ki;
        // incriment segment
        int seg = atoi(s);
        ki = kh_get(key_bloom_hm, seg_bloom, seg);
        if(ki == kh_end(seg_bloom)) {
            fprintf(stderr, "missing %d filter\n", seg);
        } else {
            struct bloom *bloom = (struct bloom*) kh_value(seg_bloom, ki);
            // fprintf(stderr, "%d (%p) << %s (%lu)\n", seg, bloom, line, strlen(line));
            if (bloom != NULL)
                bloom_add(bloom, line, strlen(line));
        }

        // next token
        s = strtok_r(NULL, "/", &sv);
    }
}

void save_filters() {
    fprintf(stderr, "Save bloom filters\n");
    char filename[256];
    for (khiter_t ki=kh_begin(seg_bloom); ki!=kh_end(seg_bloom); ++ki) {
        if (kh_exist(seg_bloom, ki)) {
            int key = kh_key(seg_bloom, ki);
            fprintf(stderr, "Save %d ", key);
            struct bloom *bloom = kh_value(seg_bloom, ki);
            sprintf(filename, "./blooms/%d", key);
            timer_start();
            if (bloom != NULL && bloom_save(bloom, filename) != 0) fprintf(stderr, "ERR ");
            timer_stop();
        }
    }
}

void cleanup() {
    fprintf(stderr, "Free bloom filter\n");
    for (khiter_t ki=kh_begin(seg_bloom); ki!=kh_end(seg_bloom); ++ki) {
        if (kh_exist(seg_bloom, ki)) {
            struct bloom *bloom = kh_value(seg_bloom, ki);
            bloom_free(bloom);
            free(bloom);
        }
    }
    kh_destroy(key_bloom_hm, seg_bloom);
}

int usage() {
    printf("Usage: ./indexer-bloom stat < data\n");
    return 1;
}
