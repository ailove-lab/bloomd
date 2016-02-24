#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cuckoo_filter.h"
#include "khash.h"

// init type
KHASH_MAP_INIT_INT(key_cuckoo_hm, cuckoo_filter_t*); // key -> filter*
khash_t(key_cuckoo_hm) *seg_cuckoo;

int usage();
int init_cuckoo(char *filename);
void index_pipe();
void raw_line_parser(char *line);
void cleanup();

int main(int argc, char **argv) {
    
    if(argc != 2 ) return usage();

    if(init_cuckoo(argv[1])) {
        printf("Fail to open %s\n", argv[1]);
        return usage();
    }
    index_pipe();
    cleanup();
}

// Read stat file
// init cucko filters
int init_cuckoo(char *filename) {

    fprintf(stderr, "Read stat and init filter\n");
    
    FILE * fp;
    char * line = NULL;
    size_t len = 0;
    size_t read;

    fp = fopen(filename, "r");
    if (fp == NULL) return -1;

    seg_cuckoo = kh_init(key_cuckoo_hm);
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
        khiter_t ki = kh_put(key_cuckoo_hm, seg_cuckoo, seg, &ret);
        if(ret == 1) {         // new
            cuckoo_filter_t *cuckoo = NULL;
            CUCKOO_FILTER_RETURN rc = cuckoo_filter_new(&cuckoo, counter, 100, (uint32_t) (time(NULL) & 0xffffffff));
            fprintf(stderr, "%d = %d (%p) %d\n", seg, counter, cuckoo, rc);
            if(rc != CUCKOO_FILTER_OK) {
                fprintf(stderr, "ERROR CREATING FILTER %d", seg);
                continue;
            }
            kh_value(seg_cuckoo, ki) = cuckoo;
        };
        // fprintf(stderr, "\t %p %d = %d\n",cuckoo, seg, counter);
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
        ki = kh_get(key_cuckoo_hm, seg_cuckoo, seg);
        // fprintf(stderr, "%d %s %lu %u\n", seg, line, strlen(line), ki);
        if(ki == kh_end(seg_cuckoo)) {
            fprintf(stderr, "missing %d filter\n", seg);
        } else {
            cuckoo_filter_t *cuckoo = (cuckoo_filter_t*) kh_value(seg_cuckoo, ki);
            fprintf(stderr, "%d (%p) << %s (%d)\n", seg, cuckoo, line, strlen(line));
            CUCKOO_FILTER_RETURN rc = cuckoo_filter_add(cuckoo, line, strlen(line));
            // CUCKOO_FILTER_RETURN rc = cuckoo_filter_add(cuckoo, "test", 4);
            if (rc != CUCKOO_FILTER_OK) fprintf(stderr, "CAN'T ADD %s to %d\n", line, seg);
        }
        // next token
        s = strtok_r(NULL, "/", &sv);
    }
}


void cleanup() {

    fprintf(stderr, "Free cuckoo filter\n");
    for (khiter_t ki=kh_begin(seg_cuckoo); ki!=kh_end(seg_cuckoo); ++ki) {
        if (kh_exist(seg_cuckoo, ki)) {
            cuckoo_filter_t *cuckoo = kh_value(seg_cuckoo, ki);
            cuckoo_filter_free(&cuckoo);
        }
    }
    kh_destroy(key_cuckoo_hm, seg_cuckoo);
}

int usage() {
    printf("Usage: ./indexer-cuckoo stat < data\n");
    return 1;
}
