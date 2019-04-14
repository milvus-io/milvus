/*
 * cblas_izamax.c
 *
 * The program is a C interface to izamax.
 * It calls the fortran wrapper before calling izamax.
 *
 * Written by Keita Teranishi.  2/11/1998
 *
 */
#include "cblas.h"
#include "cblas_f77.h"
CBLAS_INDEX cblas_izamax( const int N, const void *X, const int incX)
{
   CBLAS_INDEX iamax;
#ifdef F77_INT
   F77_INT F77_N=N, F77_incX=incX;
#else
   #define F77_N N
   #define F77_incX incX
#endif
   F77_izamax_sub( &F77_N, X, &F77_incX, &iamax);
   return (iamax ? iamax-1 : 0);
}
