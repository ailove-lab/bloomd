#include <stdio.h>
#include <sys/types.h>
#include <dirent.h>

#include "bloom.h"
#include "khash.h"

#include "timer.h"

// init type
KHASH_MAP_INIT_INT(key_bloom_hm, struct bloom*); // key -> filter*
khash_t(key_bloom_hm) *seg_bloom;

void load_filters();
int  usage();
void query_keys();
void query_key(char* key);
void cleanup();

int main(int argc, char **argv) {
    load_filters();
    query_keys();
    cleanup();
}

void load_filters() {
   
    DIR *dp;
    struct dirent *ep;
    char filtername[256];

    timer_start();
    fprintf(stderr, "/// LOAD START ///\n");
    dp = opendir ("./blooms/");
    if (dp != NULL) {
        seg_bloom = kh_init(key_bloom_hm);

        while((ep=readdir(dp)) != NULL) if(ep->d_name[0] != '.') {
            
            fprintf(stderr, "L %s ", ep->d_name);

            int ret;
            int seg = atoi(ep->d_name);
            khiter_t ki = kh_put(key_bloom_hm, seg_bloom, seg, &ret);
            
            if(ret == 1) { // new
                struct bloom *bloom = (struct bloom*) calloc(1, sizeof(struct bloom));
                sprintf(filtername,"./blooms/%s", ep->d_name);
                if(bloom_load(bloom, filtername) != 0) {
                    free(bloom);
                    continue;
                }
                kh_value(seg_bloom, ki) = bloom;
            }
        }

        (void) closedir (dp);
    } else fprintf(stderr, "Couldn't open the directory!");
    fprintf(stderr, "\n/// LOAD END ///\n");
    timer_stop();
}

void query_keys() {

    char *line = NULL;
    size_t len = 0;

    fprintf(stderr, "\n/// QUERY START ///\n");
    timer_start();
    while (getline(&line, &len, stdin) != -1) query_key(line);
    fprintf(stderr, "\n/// QUERY END ///\n");
    timer_stop();

    if(line!=NULL) free(line);
}

void query_key(char* key) {
    char *nl = strchr(key, '\n');
    if(nl != NULL) *nl = 0;
    else return;

    printf("%s\t", key);

    murmur_t murmur = {0, 0};
    bloom_get_murmur(key, strlen(key), &murmur);
    for (khiter_t ki=kh_begin(seg_bloom); ki!=kh_end(seg_bloom); ++ki) {
        if (kh_exist(seg_bloom, ki)) {
            struct bloom *bloom = kh_value(seg_bloom, ki);
            if(bloom_check_murmur(bloom, &murmur)) 
                printf("%d/", kh_key(seg_bloom, ki));
        }
    }
    printf("\n");
}

void cleanup() {
    fprintf(stderr, "\n/// CLEAN UP ///\n");
    timer_start();
    for (khiter_t ki=kh_begin(seg_bloom); ki!=kh_end(seg_bloom); ++ki) {
        if (kh_exist(seg_bloom, ki)) {
            struct bloom *bloom = kh_value(seg_bloom, ki);
            if(bloom!=NULL) {
                bloom_free(bloom);
                free(bloom);
            }
        }
    }
    kh_destroy(key_bloom_hm, seg_bloom);
    timer_stop();
}

int usage() {
    printf("Usage: ./query-bloom < keys >restored-keys\n");
    return 1;
}
