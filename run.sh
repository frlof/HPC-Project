#!/bin/bash
mpicc ./src/main.c -o main
mpirun -np $1 ./main $2
