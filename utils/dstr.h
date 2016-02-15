#ifndef __DSTR_H__
#define __DSTR_H__

typedef struct {
    char *buf;
    char *crs;
    unsigned int length;
} dstr;

dstr* dstr_new();
void dstr_free(dstr* k);
void dstr_add(dstr *k, char *v);

#endif