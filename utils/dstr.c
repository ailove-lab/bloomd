
#include "dstr.h"
#include <string.h>
#include <stdlib.h>

dstr* dstr_new() {
    dstr* k = malloc(sizeof(dstr));
    if(k == NULL) return NULL;
    k->buf = malloc(sizeof(char)*256);
    if(k->buf == NULL) {free(k); return NULL;}
    k->crs = k->buf;
    k->length = 256;
    return k;
}

void dstr_free(dstr* k) {
    free(k->buf);
    free(k);
}

void dstr_add(dstr *k, char *v) {
    int l = strlen(v);
    int s = k->crs - k->buf;
    // printf("add %s %d %d\n", v, l, s);
    if(s+l+2 > k->length) {
        k->buf = realloc(k->buf, sizeof(char)*k->length*2);
        k->crs = k->buf + s;
        k->length *= 2;
    }
    strcpy(k->crs, v);
    k->crs += l+1;
    k->crs[-1] = ' ';
    k->crs[ 0] = 0;
}
