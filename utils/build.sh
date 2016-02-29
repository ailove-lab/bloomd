#!/bin/bash

#clang -O0 -g3 -Wall -std=c99 -o indexer-cuckoo         \
#-I./cuckoo/libcuckoofilter/include           \
#./cuckoo/libcuckoofilter/src/cuckoo_filter.c \
#indexer-cuckoo.c                             \

# clang -pg -Wall -o indexer         \
# clang -Ofast -Wall -o indexer      \
# -I./libbloom -I./libbloom/murmur2  \
# -D __linux__                       \
# indexer.c                          \
# kthread.c dstr.c timer.c map.c     \
# ./libbloom/bloom.c                 \
# ./libbloom/linux.c                 \
# ./libbloom/murmur2/MurmurHash2.c   \
# -pthread -lm

clang -Ofast -Wall -o indexer-bloom \
-I./libbloom -I./libbloom/murmur2   \
-D __linux__                        \
indexer-bloom.c                     \
kthread.c timer.c                   \
./libbloom/bloom.c                  \
./libbloom/linux.c                  \
./libbloom/murmur2/MurmurHash2.c    \
-pthread -lm

clang -Ofast -Wall -o query-bloom   \
-I./libbloom -I./libbloom/murmur2   \
-D __linux__                        \
query-bloom.c                       \
kthread.c timer.c                   \
./libbloom/bloom.c                  \
./libbloom/linux.c                  \
./libbloom/murmur2/MurmurHash2.c    \
-pthread -lm
 
clang -Ofast -Wall -o info-bloom   \
-I./libbloom -I./libbloom/murmur2   \
-D __linux__                        \
info-bloom.c                       \
./libbloom/bloom.c                  \
./libbloom/linux.c                  \
./libbloom/murmur2/MurmurHash2.c    \
-lm

clang -Ofast -Wall -o entries-bloom   \
-I./libbloom -I./libbloom/murmur2   \
-D __linux__                        \
entries-bloom.c                       \
./libbloom/bloom.c                  \
./libbloom/linux.c                  \
./libbloom/murmur2/MurmurHash2.c    \
-lm

# clang -Ofast -Wall -o bloom-info    \
# -I./libbloom -I./libbloom/murmur2   \
# -D __linux__                        \
# bloom-info.c                        \
# ./libbloom/bloom.c                  \
# ./libbloom/linux.c                  \
# ./libbloom/murmur2/MurmurHash2.c    \
# -lm

# clang -Ofast -Wall -o test_bloom_load \
# -I./libbloom -I./libbloom/murmur2  \
# -D __linux__                       \
# test_bloom_load.c                  \
# ./libbloom/bloom.c                 \
# ./libbloom/linux.c                 \
# ./libbloom/murmur2/MurmurHash2.c   \
# -pthread -lm

# gcc -Ofast -pg -Wall -o indexer    \
# -I./libbloom -I./libbloom/murmur2  \
# -D __linux__                       \
# indexer.c                          \
# kthread.c dstr.c timer.c map.c     \
# ./libbloom/bloom.c                 \
# ./libbloom/linux.c                 \
# ./libbloom/murmur2/MurmurHash2.c   \
# -pthread -lm

# clang -Ofast -Wall -o stat-pipe stat-pipe.c

# clang -O3 -o everser everser.c kthread.c -pthread -lm
# clang -O3 -o stat stat.c
