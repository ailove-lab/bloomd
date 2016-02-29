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
    struct bloom bloom;

    dp = opendir ("./blooms/");
    if (dp != NULL) {
        while((ep=readdir(dp)) != NULL) if(ep->d_name[0] != '.') {
            printf("%s", ep->d_name);
            sprintf(filtername,"./blooms/%s", ep->d_name);
            FILE * fd = fopen(filtername, "rb");
            if(fd != NULL) {
              fread(&bloom, sizeof(struct bloom), 1, fd);
              printf("\t%lu\t%lu\n", bloom.insertions, bloom.collisions);
              fclose(fd);
            }
        }
        (void) closedir (dp);
    } else fprintf(stderr, "Couldn't open the directory!");

}
