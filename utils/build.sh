#!/bin/bash
clang -O3 -o everser everser.c kthread.c -pthread
clang -O3 -o stat stat.c