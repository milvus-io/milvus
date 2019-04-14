/***************************************************************************
Copyright (c) 2013-2017, The OpenBLAS Project
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
* 2016/03/21 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/

#include "common.h"

#if defined(POWER8) 
#include "sdot_microk_power8.c"
#endif


#ifndef HAVE_KERNEL_16

static FLOAT sdot_kernel_16(BLASLONG n, FLOAT *x, FLOAT *y)
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
       return dot;
}

#endif

#if defined (DSDOT)
double CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
#else
FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
#endif
{
	BLASLONG i=0;
	BLASLONG ix=0,iy=0;
	double dot = 0.0 ;

#if defined (DSDOT)
        double mydot = 0.0;
        FLOAT asmdot = 0.0;
#else
	FLOAT mydot=0.0;
#endif
	BLASLONG n1;

	if ( n <= 0 )  return(dot);

	if ( (inc_x == 1) && (inc_y == 1) )
	{

	        n1 = n & (BLASLONG)(-32);

		if ( n1 )
#if defined(DSDOT)
			{
			FLOAT *x1=x;
			FLOAT *y1=y;
			BLASLONG n2 = 32;
			while (i<n1) {
				asmdot = sdot_kernel_16(n2, x1, y1);
				mydot += (double)asmdot;
				asmdot=0.;
				x1+=32;
				y1+=32;
				i+=32;
			}
		}
#else		
			mydot = sdot_kernel_16(n1, x, y);
#endif
		i = n1;
		while(i < n)
		{
#if defined(DSDOT)
			dot += (double)y[i] * (double)x[i] ;
#else
			dot += y[i] * x[i] ;
#endif
			i++ ;

		}

		dot+=mydot;
		return(dot);


	}

	n1 = n & (BLASLONG)(-2);

	while(i < n1)
	{
#if defined (DSDOT)
		dot += (double)y[iy] * (double)x[ix] + (double)y[iy+inc_y] * (double)x[ix+inc_x];
#else
		dot += y[iy] * x[ix] + y[iy+inc_y] * x[ix+inc_x];
#endif
		ix  += inc_x*2 ;
		iy  += inc_y*2 ;
		i+=2 ;

	}

	while(i < n)
	{
#if defined (DSDOT)
		dot += (double)y[iy] * (double)x[ix] ;
#else
		dot += y[iy] * x[ix] ;
#endif
		ix  += inc_x ;
		iy  += inc_y ;
		i++ ;

	}
	return(dot);

}


