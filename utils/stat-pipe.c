
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>

#include "khash.h"

KHASH_MAP_INIT_STR(key_int_hm, unsigned long); // key -> int
khash_t(key_int_hm) *seg_cnt;                  // seg -> counter

void raw_line_parser(char *line);
void stat_print();
void free_keys();

  //////////
 // MAIN //
//////////

int main(int argc, char *argv[]) {
    
    seg_cnt = kh_init(key_int_hm); 

    char *line = NULL;
    size_t len = 0;
    while (getline(&line, &len, stdin) != -1) raw_line_parser(line);
    free(line);

    stat_print();

    free_keys();
    kh_destroy(key_int_hm, seg_cnt);

    return 0;

}

  ///////////////
 // FUNCTIONS //
///////////////

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
        int ret;
        khiter_t ki;
        // incriment segment
        ki = kh_get(key_int_hm, seg_cnt, s);
        if(ki == kh_end(seg_cnt)) {
            ki = kh_put(key_int_hm, seg_cnt, strdup(s), &ret);
            kh_value(seg_cnt, ki) = 1;
        } else {
            kh_value(seg_cnt, ki) += 1;
        }
        // next token
        s = strtok_r(NULL, "/", &sv);
    }
}

// Print statistic
void stat_print() {
    // segment counters
    for (khiter_t ki=kh_begin(seg_cnt); ki!=kh_end(seg_cnt); ++ki) {
        if (kh_exist(seg_cnt, ki)) {
            char *key = (char*) kh_key(seg_cnt, ki);
            long cnt = kh_value(seg_cnt, ki);
            printf("%s %lu\n", key, cnt);
        }
    }
}

// free keys created by strdup()
void free_keys() {
    // segment counters
    for (khiter_t ki=kh_begin(seg_cnt); ki!=kh_end(seg_cnt); ++ki) {
        if (kh_exist(seg_cnt, ki)) {
            free((char*) kh_key(seg_cnt, ki));
        }
    }
}
