# High Performance Computing | Project

This repository contains the the code for the `Project Assignment` from the course *DD2356* at *KTH*.

**Group:** 3

**Group Members:** Fredrik Löf, Victor Carle

### Build

Instructions for running the code on `Beskow`.

1. *Start by setting up the environment:*
   1. **Kinit** to get ssh key to beskow  
      *Example: `kinit --forward username@NADA.KTH.SE`*
   2. **ssh** into beskow  
      *Example: `ssh username@beskow.pdc.kth.se`*
   3. **salloc** to allocate node  
      *Example: `salloc -t 1:00:00 -A edu19.DD2356 --nodes=2`*
   4. **module  swap** to change compiler to gnu  
      *Example: `module  swap  PrgEnv-cray  PrgEnv-gnu`*
   5. **module load** git  
      *Example: `module load git`*
   6. **git clone** to download the code from this repository  
      *Example: `git clone https://github.com/frlof/HPC-Project`*

2. *Run the code:*
   1. **cc** to comile the code  
      *NOTE: it is important to be in the project directory when running this command*  
      *Example: `cc ./main.c -o main.out`*
   2. **aprun** to run the code  
      *NOTE: flag `n` specifies the number of threads. A directory to a text file is the first argument to the program*  
      *Example: `aprun -n 32 ./main.out /cfs/klemming/scratch/s/sergiorg/DD2356/input/wikipedia_10GB.txt`*

