/*
 * cblas_csscal.c
 *
 * The program is a C interface to csscal.
 *
 * Written by Keita Teranishi.  2/11/1998
 *
 */
#include "cblas.h"
#include "cblas_f77.h"
void cblas_csscal( const int N, const float alpha, void *X,
                       const int incX)
{
#ifdef F77_INT
   F77_INT F77_N=N, F77_incX=incX;
#else
   #define F77_N N
   #define F77_incX incX
#endif
   F77_csscal( &F77_N, &alpha, X, &F77_incX);
}
