/*
 * c_dblas1.c
 *
 * The program is a C wrapper for dcblat1.
 *
 * Written by Keita Teranishi.  2/11/1998
 *
 */
#include "cblas_test.h"
#include "cblas.h"
double F77_dasum(const int *N, double *X, const int *incX)
{
   return cblas_dasum(*N, X, *incX);
}

void F77_daxpy(const int *N, const double *alpha, const double *X,
                    const int *incX, double *Y, const int *incY)
{
   cblas_daxpy(*N, *alpha, X, *incX, Y, *incY);
   return;
}

void F77_dcopy(const int *N, double *X, const int *incX,
                    double *Y, const int *incY)
{
   cblas_dcopy(*N, X, *incX, Y, *incY);
   return;
}

double F77_ddot(const int *N, const double *X, const int *incX,
                const double *Y, const int *incY)
{
   return cblas_ddot(*N, X, *incX, Y, *incY);
}

double F77_dnrm2(const int *N, const double *X, const int *incX)
{
   return cblas_dnrm2(*N, X, *incX);
}

void F77_drotg( double *a, double *b, double *c, double *s)
{
   cblas_drotg(a,b,c,s);
   return;
}

void F77_drot( const int *N, double *X, const int *incX, double *Y,
       const int *incY, const double *c, const double *s)
{

   cblas_drot(*N,X,*incX,Y,*incY,*c,*s);
   return;
}

void F77_dscal(const int *N, const double *alpha, double *X,
                         const int *incX)
{
   cblas_dscal(*N, *alpha, X, *incX);
   return;
}

void F77_dswap( const int *N, double *X, const int *incX,
                          double *Y, const int *incY)
{
   cblas_dswap(*N,X,*incX,Y,*incY);
   return;
}

double F77_dzasum(const int *N, void *X, const int *incX)
{
   return cblas_dzasum(*N, X, *incX);
}

double F77_dznrm2(const int *N, const void *X, const int *incX)
{
   return cblas_dznrm2(*N, X, *incX);
}

int F77_idamax(const int *N, const double *X, const int *incX)
{
   if (*N < 1 || *incX < 1) return(0);
   return (cblas_idamax(*N, X, *incX)+1);
}
