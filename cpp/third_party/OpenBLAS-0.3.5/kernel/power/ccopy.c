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
* 2016/03/25 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/

#include "common.h"

#if defined(POWER8)
#include "ccopy_microk_power8.c"
#endif

#ifndef HAVE_KERNEL_32

static void ccopy_kernel_32(BLASLONG n, FLOAT *x, FLOAT *y)
{

	BLASLONG i=0;
	FLOAT f0, f1, f2, f3, f4, f5, f6, f7;
	FLOAT *x1=x;
	FLOAT *y1=y;

	while ( i<n )
	{

		f0 = x1[0];
		f1 = x1[1];
		f2 = x1[2];
		f3 = x1[3];
		f4 = x1[4];
		f5 = x1[5];
		f6 = x1[6];
		f7 = x1[7];

		y1[0] = f0;
		y1[1] = f1;
		y1[2] = f2;
		y1[3] = f3;
		y1[4] = f4;
		y1[5] = f5;
		y1[6] = f6;
		y1[7] = f7;

		x1 += 8;
		y1 += 8;

		i+=4;
	}
	return;

}


#endif



int CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
{
	BLASLONG i=0;
	BLASLONG ix=0,iy=0;

	if ( n <= 0     )  return(0);

	if ( (inc_x == 1) && (inc_y == 1 ))
	{

		BLASLONG n1 = n & -32;
		if ( n1 > 0 )
		{
			ccopy_kernel_32(n1, x, y);
			i=n1;
			ix=n1*2;
			iy=n1*2;
		}

		while(i < n)
		{
			y[iy] = x[iy] ;
			y[iy+1] = x[ix+1] ;
			ix+=2;
			iy+=2;
			i++ ;

		}


	}
	else
	{

		BLASLONG inc_x2 = 2 * inc_x;
		BLASLONG inc_y2 = 2 * inc_y;

		while(i < n)
		{
			y[iy] = x[ix] ;
			y[iy+1] = x[ix+1] ;
			ix += inc_x2 ;
			iy += inc_y2 ;
			i++ ;

		}

	}
	return(0);
	

}


