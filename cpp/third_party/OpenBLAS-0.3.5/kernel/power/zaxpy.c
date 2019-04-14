/***************************************************************************
Copyright (c) 2013-2016, The OpenBLAS Project
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

/**************************************************************************************
* 2016/03/23 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/

#include "common.h"


#if defined(POWER8)
#include "zaxpy_microk_power8.c"
#endif


#ifndef HAVE_KERNEL_4

static void zaxpy_kernel_4(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT *alpha)
{
	BLASLONG register i  = 0;
	BLASLONG register ix = 0;
	FLOAT da_r = alpha[0];
	FLOAT da_i = alpha[1];
	

	while(i < n)
        {
#if !defined(CONJ)
              y[ix]   += ( da_r * x[ix]   - da_i * x[ix+1] ) ;
              y[ix+1] += ( da_r * x[ix+1] + da_i * x[ix]   ) ;
              y[ix+2] += ( da_r * x[ix+2] - da_i * x[ix+3] ) ;
              y[ix+3] += ( da_r * x[ix+3] + da_i * x[ix+2] ) ;
#else
              y[ix]   += ( da_r * x[ix]   + da_i * x[ix+1] ) ;
              y[ix+1] -= ( da_r * x[ix+1] - da_i * x[ix]   ) ;
              y[ix+2] += ( da_r * x[ix+2] + da_i * x[ix+3] ) ;
              y[ix+3] -= ( da_r * x[ix+3] - da_i * x[ix+2] ) ;
#endif

              ix+=4 ;
              i+=2 ;

       }

}

#endif

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da_r, FLOAT da_i, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
{
	BLASLONG i=0;
	BLASLONG ix=0,iy=0;

	if ( n <= 0 )  return(0);

	if ( (inc_x == 1) && (inc_y == 1) )
	{

		BLASLONG n1 = n & -16;

		if ( n1 )
		{
			zaxpy_kernel_4 (n1, x, y, da_r, da_i);
			ix = 2 * n1;
		}
		i = n1;
		while(i < n)
		{
#if !defined(CONJ)
                	y[ix]   += ( da_r * x[ix]   - da_i * x[ix+1] ) ;
                	y[ix+1] += ( da_r * x[ix+1] + da_i * x[ix]   ) ;
#else
                	y[ix]   += ( da_r * x[ix]   + da_i * x[ix+1] ) ;
                	y[ix+1] -= ( da_r * x[ix+1] - da_i * x[ix]   ) ;
#endif
			i++ ;
			ix += 2;

		}
		return(0);


	}

	inc_x *=2;
	inc_y *=2;

	while(i < n)
	{

#if !defined(CONJ)
                y[iy]   += ( da_r * x[ix]   - da_i * x[ix+1] ) ;
                y[iy+1] += ( da_r * x[ix+1] + da_i * x[ix]   ) ;
#else
                y[iy]   += ( da_r * x[ix]   + da_i * x[ix+1] ) ;
                y[iy+1] -= ( da_r * x[ix+1] - da_i * x[ix]   ) ;
#endif
		ix  += inc_x ;
		iy  += inc_y ;
		i++ ;

	}
	return(0);

}


