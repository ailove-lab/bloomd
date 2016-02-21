#include <stdio.h>
#include <string.h>

#include "map.h"

// load whole file to mem
// adds \n\0 at the end
void map_load_block(char *filename, map_block_t *b) {

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


// split block on 'p' parts
// get part n
// search first \n for start
// search last  \n for length
// WARNING p must be less than number of strings x3!
void map_get_sub_block(map_block_t *blk,
                   map_block_t *sub,
                   unsigned int n,
                   unsigned int p) {

    if(n>p-1 || p > 128) return;

    char *E = blk->start + blk->length;
    unsigned int l = blk->length / p;
    char *s = blk->start + n*l;
    char *e = s + l;

    if (n>0)
        while ( s < E && s[-1] != '\n') s++;

    // if (n<p-1)
        while ( e < E && e[-1] != '\n') e++;

    if (s == e) return;
    sub->start  = s;
    sub->length = (n<p-1) ? e-s : E-s;

}