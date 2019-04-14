/***************************************************************************
Copyright (c) 2016, The OpenBLAS Project
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

int CNAME(BLASLONG n, FLOAT alpha, FLOAT *x, BLASLONG inc_x, FLOAT beta, FLOAT *y, BLASLONG inc_y)
{
	BLASLONG i=0;
	BLASLONG ix,iy;

	if ( n < 0     )  return(0);

	ix = 0;
	iy = 0;

	if ( beta == 0.0 )
	{

		if ( alpha == 0.0 )
		{
			while(i < n)
			{
				y[iy] = 0.0 ;
				iy += inc_y ;
				i++ ;
			}
		}
		else
		{
			while(i < n)
			{
				y[iy] = alpha * x[ix] ;
				ix += inc_x ;
				iy += inc_y ;
				i++ ;
			}


		}

	}
	else
	{

		if ( alpha == 0.0 )
		{
			while(i < n)
			{
				y[iy] =  beta * y[iy] ;
				iy += inc_y ;
				i++ ;
			}
		}
		else
		{
			while(i < n)
			{
				y[iy] = alpha * x[ix] + beta * y[iy] ;
				ix += inc_x ;
				iy += inc_y ;
				i++ ;
			}
		}

	}

	return(0);

}


