/*
 * cblas_zgbmv.c
 * The program is a C interface of zgbmv
 *
 * Keita Teranishi  5/20/98
 *
 */
#include <stdio.h>
#include <stdlib.h>
#include "cblas.h"
#include "cblas_f77.h"
void cblas_zgbmv(const CBLAS_LAYOUT layout,
                 const CBLAS_TRANSPOSE TransA, const int M, const int N,
                 const int KL, const int KU,
                 const void *alpha, const void  *A, const int lda,
                 const void  *X, const int incX, const void *beta,
                 void  *Y, const int incY)
{
   char TA;
#ifdef F77_CHAR
   F77_CHAR F77_TA;
#else
   #define F77_TA &TA
#endif
#ifdef F77_INT
   F77_INT F77_M=M, F77_N=N, F77_lda=lda, F77_incX=incX, F77_incY=incY;
   F77_INT F77_KL=KL,F77_KU=KU;
#else
   #define F77_M M
   #define F77_N N
   #define F77_lda lda
   #define F77_KL KL
   #define F77_KU KU
   #define F77_incX incx
   #define F77_incY incY
#endif
   int n, i=0, incx=incX;
   const double *xx= (double *)X, *alp= (double *)alpha, *bet = (double *)beta;
   double ALPHA[2],BETA[2];
   int tincY, tincx;
   double *x=(double *)X, *y=(double *)Y, *st=0, *tx;
   extern int CBLAS_CallFromC;
   extern int RowMajorStrg;
   RowMajorStrg = 0;

   CBLAS_CallFromC = 1;
   if (layout == CblasColMajor)
   {
      if (TransA == CblasNoTrans) TA = 'N';
      else if (TransA == CblasTrans) TA = 'T';
      else if (TransA == CblasConjTrans) TA = 'C';
      else
      {
         cblas_xerbla(2, "cblas_zgbmv","Illegal TransA setting, %d\n", TransA);
         CBLAS_CallFromC = 0;
         RowMajorStrg = 0;
         return;
      }
      #ifdef F77_CHAR
         F77_TA = C2F_CHAR(&TA);
      #endif
      F77_zgbmv(F77_TA, &F77_M, &F77_N, &F77_KL, &F77_KU, alpha,
                     A, &F77_lda, X, &F77_incX, beta, Y, &F77_incY);
   }
   else if (layout == CblasRowMajor)
   {
      RowMajorStrg = 1;
      if (TransA == CblasNoTrans) TA = 'T';
      else if (TransA == CblasTrans) TA = 'N';
      else if (TransA == CblasConjTrans)
      {
         ALPHA[0]= *alp;
         ALPHA[1]= -alp[1];
         BETA[0]= *bet;
         BETA[1]= -bet[1];
         TA = 'N';
         if (M > 0)
         {
            n = M << 1;
            x = malloc(n*sizeof(double));
            tx = x;

            if( incX > 0 ) {
               i = incX << 1 ;
               tincx = 2;
               st= x+n;
            } else {
               i = incX *(-2);
               tincx = -2;
               st = x-2;
               x +=(n-2);
            }
            do
            {
               *x = *xx;
               x[1] = -xx[1];
               x += tincx ;
               xx += i;
            }
            while (x != st);
            x=tx;

            #ifdef F77_INT
               F77_incX = 1;
            #else
               incx = 1;
            #endif

            if( incY > 0 )
              tincY = incY;
            else
              tincY = -incY;

            y++;

            if (N > 0)
            {
               i = tincY << 1;
               n = i * N ;
               st = y + n;
               do {
                  *y = -(*y);
                  y += i;
               } while(y != st);
               y -= n;
            }
         }
         else x = (double *) X;


      }
      else
      {
         cblas_xerbla(2, "cblas_zgbmv","Illegal TransA setting, %d\n", TransA);
         CBLAS_CallFromC = 0;
         RowMajorStrg = 0;
         return;
      }
      #ifdef F77_CHAR
         F77_TA = C2F_CHAR(&TA);
      #endif
      if (TransA == CblasConjTrans)
         F77_zgbmv(F77_TA, &F77_N, &F77_M, &F77_KU, &F77_KL, ALPHA,
                        A ,&F77_lda, x,&F77_incX, BETA, Y, &F77_incY);
      else
         F77_zgbmv(F77_TA, &F77_N, &F77_M, &F77_KU, &F77_KL, alpha,
                        A ,&F77_lda, x,&F77_incX, beta, Y, &F77_incY);
      if (TransA == CblasConjTrans)
      {
         if (x != X) free(x);
         if (N > 0)
         {
            do
            {
               *y = -(*y);
               y += i;
            }
            while (y != st);
         }
      }
   }
   else cblas_xerbla(1, "cblas_zgbmv", "Illegal layout setting, %d\n", layout);
   CBLAS_CallFromC = 0;
   RowMajorStrg = 0;
   return;
}
