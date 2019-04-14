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
* 2016/03/26 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/



#include "common.h"

#pragma GCC optimize "O1"

#if defined(POWER8)
#include "srot_microk_power8.c"
#endif


#ifndef HAVE_KERNEL_16

static void srot_kernel_16(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT c, FLOAT s)
{

	BLASLONG i=0;
	FLOAT f0, f1, f2, f3;
	FLOAT x00, x01, x02, x03;
	FLOAT g0, g1, g2, g3;
	FLOAT y00, y01, y02, y03;
	FLOAT *x1=x;
	FLOAT *y1=y;

	while ( i<n )
	{

		x00 = x1[0];
		y00 = y1[0];
		x01 = x1[1];
		y01 = y1[1];
		x02 = x1[2];
		y02 = y1[2];
		x03 = x1[3];
		y03 = y1[3];

		f0 = c*x00 + s*y00;
		g0 = c*y00 - s*x00;
		f1 = c*x01 + s*y01;
		g1 = c*y01 - s*x01;
		f2 = c*x02 + s*y02;
		g2 = c*y02 - s*x02;
		f3 = c*x03 + s*y03;
		g3 = c*y03 - s*x03;

		x1[0] = f0;
		y1[0] = g0;
		x1[1] = f1;
		y1[1] = g1;
		x1[2] = f2;
		y1[2] = g2;
		x1[3] = f3;
		y1[3] = g3;

		x1 += 4;
		y1 += 4;

		i+=4;
	}
	return;

}


#endif


int CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT c, FLOAT s)
{
	BLASLONG i=0;
	BLASLONG ix=0,iy=0;
	FLOAT *x1=x;
	FLOAT *y1=y;
	FLOAT temp;

	if ( n <= 0     )  return(0);

	if ( (inc_x == 1) && (inc_y == 1) )
	{

		BLASLONG n1 = n & -16;
		if ( n1 > 0 )
		{
			srot_kernel_16(n1, x1, y1, c, s);
			i=n1;
		}

		while(i < n)
		{
			temp  = c*x[i] + s*y[i] ;
			y[i]  = c*y[i] - s*x[i] ;
			x[i]  = temp ;

			i++ ;

		}


	}
	else
	{

		while(i < n)
		{
			temp   = c*x[ix] + s*y[iy] ;
			y[iy]  = c*y[iy] - s*x[ix] ;
			x[ix]  = temp ;

			ix += inc_x ;
			iy += inc_y ;
			i++ ;

		}

	}
	return(0);

}


