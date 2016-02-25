
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <assert.h>
#include <sys/time.h>

void raw_line_parser(char *line);

  //////////
 // MAIN //
//////////

int main(int argc, char *argv[]) {
    
    char *line = NULL;
    size_t len = 0;
    while (getline(&line, &len, stdin) != -1) raw_line_parser(line);
    free(line);

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
        printf("s %s %s\n", s, line);
        // next token
        s = strtok_r(NULL, "/", &sv);
    }
}