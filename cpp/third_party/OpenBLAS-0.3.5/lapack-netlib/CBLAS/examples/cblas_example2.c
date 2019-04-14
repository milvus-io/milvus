/* cblas_example2.c */

#include <stdio.h>
#include <stdlib.h>
#include "cblas.h"
#include "cblas_f77.h"

#define INVALID -1

int main (int argc, char **argv )
{
   int rout=-1,info=0,m,n,k,lda,ldb,ldc;
   double A[2] = {0.0,0.0},
          B[2] = {0.0,0.0},
          C[2] = {0.0,0.0},
          ALPHA=0.0, BETA=0.0;

   if (argc > 2){
      rout = atoi(argv[1]);
      info = atoi(argv[2]);
   }

   if (rout == 1) {
      if (info==0) {
         printf("Checking if cblas_dgemm fails on parameter 4\n");
         cblas_dgemm( CblasRowMajor,  CblasTrans, CblasNoTrans, INVALID, 0, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      }
      if (info==1) {
         printf("Checking if cblas_dgemm fails on parameter 5\n");
         cblas_dgemm( CblasRowMajor,  CblasNoTrans, CblasTrans, 0, INVALID, 0,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      }
      if (info==2) {
         printf("Checking if cblas_dgemm fails on parameter 9\n");
         cblas_dgemm( CblasRowMajor,  CblasNoTrans, CblasNoTrans, 0, 0, 2,
                   ALPHA, A, 1, B, 1, BETA, C, 2 );
      }
      if (info==3) {
         printf("Checking if cblas_dgemm fails on parameter 11\n");
         cblas_dgemm( CblasRowMajor,  CblasNoTrans, CblasNoTrans, 0, 2, 2,
                   ALPHA, A, 1, B, 1, BETA, C, 1 );
      }
   } else {
      if (info==0) {
         printf("Checking if F77_dgemm fails on parameter 3\n");
         m=INVALID; n=0; k=0; lda=1; ldb=1; ldc=1;
         F77_dgemm( "T", "N", &m, &n, &k,
                   &ALPHA, A, &lda, B, &ldb, &BETA, C, &ldc );
      }
      if (info==1) {
         m=0; n=INVALID; k=0; lda=1; ldb=1; ldc=1;
         printf("Checking if F77_dgemm fails on parameter 4\n");
         F77_dgemm( "N", "T", &m, &n, &k,
                   &ALPHA, A, &lda, B, &ldb, &BETA, C, &ldc );
      }
      if (info==2) {
         printf("Checking if F77_dgemm fails on parameter 8\n");
         m=2; n=0; k=0; lda=1; ldb=1; ldc=2;
         F77_dgemm( "N", "N" , &m, &n, &k,
                   &ALPHA, A, &lda, B, &ldb, &BETA, C, &ldc );
      }
      if (info==3) {
         printf("Checking if F77_dgemm fails on parameter 10\n");
         m=0; n=0; k=2; lda=1; ldb=1; ldc=1;
         F77_dgemm( "N", "N" , &m, &n, &k,
                   &ALPHA, A, &lda, B, &ldb, &BETA, C, &ldc );
      }
   }

   return 0;
}
