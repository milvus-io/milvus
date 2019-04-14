#include <stdio.h>

void c_intface_(int *i)
{
   fprintf(stdout, "ADD_\n");
}

void c_intface(int *i)
{
   fprintf(stdout, "NOCHANGE\n");
}

void c_intface__(int *i)
{
   fprintf(stdout, "f77IsF2C\n");
}

void C_INTFACE(int *i)
{
   fprintf(stdout, "UPPER\n");
}
