#!/bin/sh

gcc -O3 -Wall -shared -o libval_limit.so -I/usr/include/mysql -fPIC val_limit.cc 2>&1

