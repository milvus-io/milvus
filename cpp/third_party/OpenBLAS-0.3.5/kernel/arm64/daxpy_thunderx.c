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
#include <arm_neon.h>

#define prefetch(a) __asm__("prfm PLDL1STRM, [%0]"::"r"(a):"memory");
//#define prefetch(a)

static void daxpy_kernel_8(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT *alpha)
{
	BLASLONG register i = 0;
	double a = *alpha;

#if 0
        prefetch(x + 128/sizeof(*x));
        prefetch(y + 128/sizeof(*y));
#endif
        prefetch(x + 2*128/sizeof(*x));
        prefetch(y + 2*128/sizeof(*y));
        prefetch(x + 3*128/sizeof(*x));
        prefetch(y + 3*128/sizeof(*y));
        prefetch(x + 4*128/sizeof(*x));
        prefetch(y + 4*128/sizeof(*y));

	while(i < n)
        {
	      double y0, y1, y2, y3;
	      double y4, y5, y6, y7;
	      double *xx;
	      double *yy;
              y0 = a * x[0] + y[0];
              y1 = a * x[1] + y[1];
              y2 = a * x[2] + y[2];
              y3 = a * x[3] + y[3];
              y4 = a * x[4] + y[4];
              y5 = a * x[5] + y[5];
              y6 = a * x[6] + y[6];
              y7 = a * x[7] + y[7];
	      asm("":"+w"(y0),"+w"(y1),"+w"(y2),"+w"(y3),"+w"(y4),"+w"(y5),"+w"(y6),"+w"(y7));
	      y[0] = y0;
	      y[1] = y1;
	      y[2] = y2;
	      y[3] = y3;
	      y[4] = y4;
	      y[5] = y5;
	      y[6] = y6;
	      y[7] = y7;

              xx = (x + 4*128/sizeof(*x));
              yy = (y + 4*128/sizeof(*y));
	      asm("":"+r"(yy)::"memory");
	      prefetch(xx);
	      prefetch(yy);

	      y += 8;
	      x += 8;
              i += 8 ;
	}

}


int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
{
	BLASLONG i=0;
	BLASLONG ix=0,iy=0;

	if ( n <= 0 )  return(0);

	if ( (inc_x == 1) && (inc_y == 1) )
	{

		BLASLONG n1 = n & -32;

		if ( n1 )
			daxpy_kernel_8(n1, x, y , &da );

		i = n1;
		while(i < n)
		{

			y[i] += da * x[i] ;
			i++ ;

		}
		return(0);


	}

	BLASLONG n1 = n & -4;

	while(i < n1)
	{

		FLOAT m1      = da * x[ix] ;
		FLOAT m2      = da * x[ix+inc_x] ;
		FLOAT m3      = da * x[ix+2*inc_x] ;
		FLOAT m4      = da * x[ix+3*inc_x] ;

		y[iy]         += m1 ;
		y[iy+inc_y]   += m2 ;
		y[iy+2*inc_y] += m3 ;
		y[iy+3*inc_y] += m4 ;

		ix  += inc_x*4 ;
		iy  += inc_y*4 ;
		i+=4 ;

	}

	while(i < n)
	{

		y[iy] += da * x[ix] ;
		ix  += inc_x ;
		iy  += inc_y ;
		i++ ;

	}
	return(0);

}


