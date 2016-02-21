#!/bin/bash
<<<<<<< HEAD
clang -Ofast -Wall -o indexer \
=======
clang -Ofast -Wall -o indexer      \
>>>>>>> c1ee0a6388f37f74cafce55b43509061e7e7b8e8
-I./libbloom -I./libbloom/murmur2  \
-D __linux__                       \
indexer.c                          \
kthread.c dstr.c timer.c map.c     \
./libbloom/bloom.c                 \
./libbloom/linux.c                 \
./libbloom/murmur2/MurmurHash2.c   \
-pthread -lm

clang -Ofast -Wall -o stat-pipe stat-pipe.c

# clang -O3 -o everser everser.c kthread.c -pthread -lm
# clang -O3 -o stat stat.c
