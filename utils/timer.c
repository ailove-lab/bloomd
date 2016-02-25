#include <stdio.h>
#include <sys/time.h>

#include "timer.h"

static struct timeval tm1;

void timer_start() {
    gettimeofday(&tm1, NULL);
}

void timer_stop() {
    struct timeval tm2;
    gettimeofday(&tm2, NULL);

    unsigned long long t = 1000 * (tm2.tv_sec - tm1.tv_sec) + (tm2.tv_usec - tm1.tv_usec) / 1000;
    fprintf(stderr, "%llu ms\n", t);
}
