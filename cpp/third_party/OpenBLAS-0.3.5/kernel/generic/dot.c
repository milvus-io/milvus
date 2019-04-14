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

#if defined(DSDOT)
double CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
#else
FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
#endif
{
	BLASLONG i=0;
	BLASLONG ix=0,iy=0;

#if defined(DSDOT)
	double dot = 0.0 ;
#else
	FLOAT  dot = 0.0 ;
#endif

	if ( n < 0 )  return(dot);

	if ( (inc_x == 1) && (inc_y == 1) )
	{

		int n1 = n & -4;

		while(i < n1)
		{

#if defined(DSDOT)
			dot += (double) y[i] * (double) x[i]
			    + (double) y[i+1] * (double) x[i+1]
			    + (double) y[i+2] * (double) x[i+2]
			    + (double) y[i+3] * (double) x[i+3] ;
#else
			dot += y[i] * x[i]
			    + y[i+1] * x[i+1]
			    + y[i+2] * x[i+2]
			    + y[i+3] * x[i+3] ;
#endif
			i+=4 ;

		}

		while(i < n)
		{

#if defined(DSDOT)
			dot += (double) y[i] * (double) x[i] ;
#else
			dot += y[i] * x[i] ;
#endif
			i++ ;

		}
		return(dot);


	}

	while(i < n)
	{

#if defined(DSDOT)
		dot += (double) y[iy] * (double) x[ix] ;
#else
		dot += y[iy] * x[ix] ;
#endif
		ix  += inc_x ;
		iy  += inc_y ;
		i++ ;

	}
	return(dot);

}


