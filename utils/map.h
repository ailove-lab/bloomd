#ifndef __MAP_H__
#define __MAP_H__

// structure for file parts storage
typedef struct {
    char *start;
    unsigned long length;
} map_block_t;

void map_load_block(char *filename, map_block_t *b);
void map_get_sub_block(map_block_t *blk, map_block_t *sub, unsigned int n, unsigned int p);


#endif