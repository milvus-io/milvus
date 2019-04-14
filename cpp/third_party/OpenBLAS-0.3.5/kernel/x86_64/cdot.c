/***************************************************************************
Copyright (c) 2015, The OpenBLAS Project
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in
the documentation and/or other materials provided with the
distribution.
3. Neither the name of the OpenBLAS project nor the names of
its contributors may be used to endorse or promote products
derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*****************************************************************************/


#include "common.h"
#include <complex.h>


#if defined(BULLDOZER)
#include "cdot_microk_bulldozer-2.c"
#elif defined(STEAMROLLER) || defined(PILEDRIVER)  || defined(EXCAVATOR)
#include "cdot_microk_steamroller-2.c"
#elif defined(HASWELL) || defined(ZEN) || defined (SKYLAKEX)
#include "cdot_microk_haswell-2.c"
#elif defined(SANDYBRIDGE)
#include "cdot_microk_sandy-2.c"
#endif


#ifndef HAVE_KERNEL_16

static void cdot_kernel_16(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT *d) __attribute__ ((noinline));

static void cdot_kernel_16(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT *d)
{
	BLASLONG register i = 0;
	FLOAT dot[8] = { 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0 };
	BLASLONG j=0;

	while( i < n )
        {

            dot[0] += x[j]   * y[j]   ;
            dot[1] += x[j+1] * y[j+1] ;
            dot[4] += x[j]   * y[j+1] ;
            dot[5] += x[j+1] * y[j]   ;

            dot[2] += x[j+2] * y[j+2] ;
            dot[3] += x[j+3] * y[j+3] ;
            dot[6] += x[j+2] * y[j+3] ;
            dot[7] += x[j+3] * y[j+2] ;

            dot[0] += x[j+4] * y[j+4] ;
            dot[1] += x[j+5] * y[j+5] ;
            dot[4] += x[j+4] * y[j+5] ;
            dot[5] += x[j+5] * y[j+4] ;

            dot[2] += x[j+6] * y[j+6] ;
            dot[3] += x[j+7] * y[j+7] ;
            dot[6] += x[j+6] * y[j+7] ;
            dot[7] += x[j+7] * y[j+6] ;

	    j+=8;
            i+=4;

        }
	d[0] = dot[0];
	d[1] = dot[1];
	d[2] = dot[2];
	d[3] = dot[3];
	d[4] = dot[4];
	d[5] = dot[5];
	d[6] = dot[6];
	d[7] = dot[7];

}

#endif

OPENBLAS_COMPLEX_FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
{
	BLASLONG i;
	BLASLONG ix,iy;
	FLOAT  dot[8] = { 0.0, 0.0, 0.0 , 0.0, 0.0, 0.0, 0.0, 0.0 } ; 

	if ( n <= 0 ) 
	{
	    OPENBLAS_COMPLEX_FLOAT result = OPENBLAS_MAKE_COMPLEX_FLOAT (0.0, 0.0) ;
		return(result);

	}

	if ( (inc_x == 1) && (inc_y == 1) )
	{

		BLASLONG n1 = n & -16;

		if ( n1 )
		{
			cdot_kernel_16(n1, x, y , dot );
			dot[0] += dot[2];
			dot[1] += dot[3];
			dot[4] += dot[6];
			dot[5] += dot[7];
		}

		i = n1;
		BLASLONG j = i * 2;

		while( i < n )
		{

			dot[0] += x[j]   * y[j]   ;
			dot[1] += x[j+1] * y[j+1] ;
			dot[4] += x[j]   * y[j+1] ;
			dot[5] += x[j+1] * y[j]   ;

			j+=2;
			i++ ;

		}


	}
	else
	{
		i=0;
		ix=0;
		iy=0;
		inc_x <<= 1;
		inc_y <<= 1;
		while(i < n)
		{

			dot[0] += x[ix]   * y[iy]   ;
			dot[1] += x[ix+1] * y[iy+1] ;
			dot[4] += x[ix]   * y[iy+1] ;
			dot[5] += x[ix+1] * y[iy]   ;

			ix  += inc_x ;
			iy  += inc_y ;
			i++ ;

		}
	}

#if !defined(CONJ)
	OPENBLAS_COMPLEX_FLOAT result = OPENBLAS_MAKE_COMPLEX_FLOAT (dot[0]-dot[1], dot[4]+dot[5]) ;
//	CREAL(result) = dot[0] - dot[1];
//	CIMAG(result) = dot[4] + dot[5];
#else
	OPENBLAS_COMPLEX_FLOAT result = OPENBLAS_MAKE_COMPLEX_FLOAT (dot[0]+dot[1], dot[4]-dot[5]) ;
//	CREAL(result) = dot[0] + dot[1];
//	CIMAG(result) = dot[4] - dot[5];

#endif

	return(result);

}


