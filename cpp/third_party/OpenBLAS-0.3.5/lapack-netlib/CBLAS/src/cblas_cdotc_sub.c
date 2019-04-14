/*
 * cblas_cdotc_sub.c
 *
 * The program is a C interface to cdotc.
 * It calls the fortran wrapper before calling cdotc.
 *
 * Written by Keita Teranishi.  2/11/1998
 *
 */
#include "cblas.h"
#include "cblas_f77.h"
void cblas_cdotc_sub( const int N, const void *X, const int incX,
                      const void *Y, const int incY, void *dotc)
{
#ifdef F77_INT
   F77_INT F77_N=N, F77_incX=incX, F77_incY=incY;
#else
   #define F77_N N
   #define F77_incX incX
   #define F77_incY incY
#endif
   F77_cdotc_sub( &F77_N, X, &F77_incX, Y, &F77_incY, dotc);
   return;
}
