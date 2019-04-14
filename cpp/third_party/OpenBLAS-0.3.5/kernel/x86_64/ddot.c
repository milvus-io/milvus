/***************************************************************************
Copyright (c) 2014, The OpenBLAS Project
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


#if defined(BULLDOZER)
#include "ddot_microk_bulldozer-2.c"
#elif defined(STEAMROLLER)  || defined(EXCAVATOR)
#include "ddot_microk_steamroller-2.c"
#elif defined(PILEDRIVER)
#include "ddot_microk_piledriver-2.c"
#elif defined(NEHALEM)
#include "ddot_microk_nehalem-2.c"
#elif defined(HASWELL) || defined(ZEN)
#include "ddot_microk_haswell-2.c"
#elif defined (SKYLAKEX)
#include "ddot_microk_skylakex-2.c"
#elif defined(SANDYBRIDGE)
#include "ddot_microk_sandy-2.c"
#endif

#if !defined(DSDOT)
#define RETURN_TYPE     FLOAT
#else
#define RETURN_TYPE     double
#endif


#ifndef HAVE_KERNEL_8

static void ddot_kernel_8(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT *d)
{
	BLASLONG register i = 0;
	FLOAT dot = 0.0;

	while(i < n)
        {
              dot += y[i]  * x[i]
                  + y[i+1] * x[i+1]
                  + y[i+2] * x[i+2]
                  + y[i+3] * x[i+3]
                  + y[i+4] * x[i+4]
                  + y[i+5] * x[i+5]
                  + y[i+6] * x[i+6]
                  + y[i+7] * x[i+7] ;

              i+=8 ;

       }
       *d += dot;

}

#endif

static FLOAT dot_compute(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
{
	BLASLONG i=0;
	BLASLONG ix=0,iy=0;

	FLOAT  dot = 0.0 ;

	if ( n <= 0 )  return(dot);

	if ( (inc_x == 1) && (inc_y == 1) )
	{

		BLASLONG n1 = n & -16;

		if ( n1 )
			ddot_kernel_8(n1, x, y , &dot );

		i = n1;
		while(i < n)
		{

			dot += y[i] * x[i] ;
			i++ ;

		}
		return(dot);


	}

	FLOAT temp1 = 0.0;
	FLOAT temp2 = 0.0;

        BLASLONG n1 = n & -4;

	while(i < n1)
	{

		FLOAT m1 = y[iy]       * x[ix] ;
		FLOAT m2 = y[iy+inc_y] * x[ix+inc_x] ;

		FLOAT m3 = y[iy+2*inc_y] * x[ix+2*inc_x] ;
		FLOAT m4 = y[iy+3*inc_y] * x[ix+3*inc_x] ;

		ix  += inc_x*4 ;
		iy  += inc_y*4 ;

		temp1 += m1+m3;
		temp2 += m2+m4;

		i+=4 ;

	}

	while(i < n)
	{

		temp1 += y[iy] * x[ix] ;
		ix  += inc_x ;
		iy  += inc_y ;
		i++ ;

	}
	dot = temp1 + temp2;
	return(dot);

}

#if defined(SMP)
static int dot_thread_function(BLASLONG n, BLASLONG dummy0,
        BLASLONG dummy1, FLOAT dummy2, FLOAT *x, BLASLONG inc_x, FLOAT *y,
        BLASLONG inc_y, RETURN_TYPE *result, BLASLONG dummy3)
{
        *(RETURN_TYPE *)result = dot_compute(n, x, inc_x, y, inc_y);

        return 0;
}

extern int blas_level1_thread_with_return_value(int mode, BLASLONG m, BLASLONG n,
        BLASLONG k, void *alpha, void *a, BLASLONG lda, void *b, BLASLONG ldb,
        void *c, BLASLONG ldc, int (*function)(), int nthreads);
#endif

FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
{
#if defined(SMP)
	int nthreads;
	FLOAT dummy_alpha;
#endif
	FLOAT dot = 0.0;

#if defined(SMP)
	if (inc_x == 0 || inc_y == 0 || n <= 10000)
		nthreads = 1;
	else
		nthreads = num_cpu_avail(1);

	if (nthreads == 1) {
		dot = dot_compute(n, x, inc_x, y, inc_y);
	} else {
		int mode, i;
		char result[MAX_CPU_NUMBER * sizeof(double) * 2];
		RETURN_TYPE *ptr;

#if !defined(DOUBLE)
		mode = BLAS_SINGLE  | BLAS_REAL;
#else
		mode = BLAS_DOUBLE  | BLAS_REAL;
#endif
		blas_level1_thread_with_return_value(mode, n, 0, 0, &dummy_alpha,
				   x, inc_x, y, inc_y, result, 0,
				   ( void *)dot_thread_function, nthreads);

		ptr = (RETURN_TYPE *)result;
		for (i = 0; i < nthreads; i++) {
			dot = dot + (*ptr);
			ptr = (RETURN_TYPE *)(((char *)ptr) + sizeof(double) * 2);
		}
	}
#else
	dot = dot_compute(n, x, inc_x, y, inc_y);
#endif

	return dot;
}
