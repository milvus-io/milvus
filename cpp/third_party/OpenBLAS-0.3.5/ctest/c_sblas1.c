/*
 * c_sblas1.c
 *
 * The program is a C wrapper for scblat1.
 *
 * Written by Keita Teranishi.  2/11/1998
 *
 */
#include "common.h"
#include "cblas_test.h"

float F77_sasum(blasint *N, float *X, blasint *incX)
{
   return cblas_sasum(*N, X, *incX);
}

void F77_saxpy(blasint *N, const float *alpha, OPENBLAS_CONST float *X,
                    blasint *incX, float *Y, blasint *incY)
{
   cblas_saxpy(*N, *alpha, X, *incX, Y, *incY);
   return;
}

float F77_scasum(blasint *N, float *X, blasint *incX)
{
   return cblas_scasum(*N, X, *incX);
}

float F77_scnrm2(blasint *N, OPENBLAS_CONST float *X, blasint *incX)
{
   return cblas_scnrm2(*N, X, *incX);
}

void F77_scopy(blasint *N, OPENBLAS_CONST float *X, blasint *incX,
                    float *Y, blasint *incY)
{
   cblas_scopy(*N, X, *incX, Y, *incY);
   return;
}

float F77_sdot(blasint *N, OPENBLAS_CONST float *X, blasint *incX,
                        OPENBLAS_CONST float *Y, blasint *incY)
{
   return cblas_sdot(*N, X, *incX, Y, *incY);
}

float F77_snrm2(blasint *N, OPENBLAS_CONST float *X, blasint *incX)
{
   return cblas_snrm2(*N, X, *incX);
}

void F77_srotg( float *a, float *b, float *c, float *s)
{
   cblas_srotg(a,b,c,s);
   return;
}

void F77_srot( blasint *N, float *X, blasint *incX, float *Y,
              blasint *incY, const float  *c, const float  *s)
{
   cblas_srot(*N,X,*incX,Y,*incY,*c,*s);
   return;
}

void F77_sscal(blasint *N, const float *alpha, float *X,
                         blasint *incX)
{
   cblas_sscal(*N, *alpha, X, *incX);
   return;
}

void F77_sswap( blasint *N, float *X, blasint *incX,
                          float *Y, blasint *incY)
{
   cblas_sswap(*N,X,*incX,Y,*incY);
   return;
}

int F77_isamax(blasint *N, OPENBLAS_CONST float *X, blasint *incX)
{
   if (*N < 1 || *incX < 1) return(0);
   return (cblas_isamax(*N, X, *incX)+1);
}
