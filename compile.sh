#!/bin/sh

g++ -O3 -Wall -fPIC -shared -o libval_limit.so -I/usr/include/mysql val_limit.cc 2>&1
g++ -O3 -Wall -fPIC -shared -o libudf_bitset.so -I/usr/include/mysql bitset.cc 2>&1

