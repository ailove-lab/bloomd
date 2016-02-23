#include <stdio.h>
#include <string.h>

#include "bloom.h"

int usage();

int main(int argc, char **argv) {
    if(argc !=2) return usage();
    struct bloom bloom;
    if(bloom_load(&bloom, argv[1])) return usage();
    bloom_print(&bloom);

    char *key = NULL;
    size_t len = 0;
    while (getline(&key, &len, stdin) != -1) {
        int s = bloom_check(&bloom, key, strlen(key)-1);
        if(s) printf("%s", key);
    }
    bloom_free(&bloom);
}

int usage() {
   printf("Usage: ./test_bloom_load filename < keys");
   return 1;
}