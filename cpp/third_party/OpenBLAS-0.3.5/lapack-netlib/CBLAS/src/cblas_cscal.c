/*
 * cblas_cscal.c
 *
 * The program is a C interface to cscal.
 *
 * Written by Keita Teranishi.  2/11/1998
 *
 */
#include "cblas.h"
#include "cblas_f77.h"
void cblas_cscal( const int N, const void *alpha, void *X,
                       const int incX)
{
#ifdef F77_INT
   F77_INT F77_N=N, F77_incX=incX;
#else
   #define F77_N N
   #define F77_incX incX
#endif
   F77_cscal( &F77_N, alpha, X, &F77_incX);
}
