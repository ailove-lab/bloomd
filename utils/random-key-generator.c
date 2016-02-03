#include <stdio.h>
#include <stdlib.h>
#include <time.h>

// static unsigned long x=123456789, y=362436069, z=521288629;

// unsigned long xorshf96(void) {          //period 2^96-1
// unsigned long t;
//     x ^= x << 16;
//     x ^= x >> 5;
//     x ^= x << 1;

//    t = x;
//    x = y;
//    y = z;
//    z = t ^ x ^ y;

//    return z;
// }

#include <stdint.h>
// #define UINT64_C(val) (val##ULL)

uint64_t x; /* The state must be seeded with a nonzero value. */

uint64_t xorshift64star(void) {
    x ^= x >> 12; // a
    x ^= x << 25; // b
    x ^= x >> 27; // c
    return x * UINT64_C(2685821657736338717);
}

int main(int argc, char ** argv) {
    x = (unsigned long) clock();
    unsigned int cnt=0;
    if (argc>=2 && (cnt=atoi(argv[1]))>0)
        for (int i=0; i<cnt; i++) {
            printf("%016lx%016lx\n", xorshift64star(),xorshift64star());
        }
    else printf("Usage: generator count\n");
    return 0;
}