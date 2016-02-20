#!/bin/bash
clang -Ofast -Wall -o indexer      \
-I./libbloom -I./libbloom/murmur2  \
-D __linux__                       \
indexer.c kthread.c dstr.c timer.c \
./libbloom/bloom.c                 \
./libbloom/linux.c                 \
./libbloom/murmur2/MurmurHash2.c   \
-pthread -lm

# clang -O3 -o everser everser.c kthread.c -pthread -lm
# clang -O3 -o stat stat.c