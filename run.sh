#!/bin/bash
mpicc main.c hashmap.c -o main
mpirun -np $1 ./main
