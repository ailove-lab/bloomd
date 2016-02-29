#include <stdio.h>
#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>

#include "bloom.h"

  ////////////
 /// MAIN ///
////////////

int main(int argc, char **argv) {

    DIR *dp;
    struct dirent *ep;
    char filtername[256];

    dp = opendir ("./blooms/");
    if (dp != NULL) {
        while((ep=readdir(dp)) != NULL) if(ep->d_name[0] != '.') {
            printf("\n%s\n---------------------\n", ep->d_name);
            sprintf(filtername,"./blooms/%s", ep->d_name);
            bloom_load_info(filtername);
        }
        (void) closedir (dp);
    } else fprintf(stderr, "Couldn't open the directory!");

}
