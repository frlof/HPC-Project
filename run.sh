#!/bin/bash
mpicc main.c -o main
mpirun -np $1 ./main $2
