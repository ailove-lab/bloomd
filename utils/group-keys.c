
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>

#include "dstr.h"

int keys_count = 0;
int usage();

  //////////
 // MAIN //
//////////

int main(int argc, char *argv[]) {
    
    if(argc !=2 ) return usage();
    keys_count = atoi(argv[1]);
    if(keys_count <2 ) return usage();
    
    dstr *keys = NULL;
    int cnt = 0;

    char *line = NULL;
    size_t len = 0;
    while (getline(&line, &len, stdin) != -1) {
        if(cnt%keys_count == 0) {
            if(keys!=NULL) dstr_free(keys);
            keys = dstr_new();
            dstr_add(keys, "g");
        }
        char *nl = strchr(line, '\n');
        *nl = 0;
        dstr_add(keys, line);
        cnt++;
        if(cnt % keys_count == 0) printf("%s\n", keys->buf);
    }

    if(keys!=NULL) {
        printf("%s\n", keys->buf);
        dstr_free(keys);
    }

    return 0;

}

int usage() {
    printf("Usage: group-keys 100 < keys\n");
    return 1;
}