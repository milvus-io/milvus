/***************************************************************************
Copyright (c) 2013, The OpenBLAS Project
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
 * * 2013/11/23 Saar
 * *	 BLASTEST float		: OK
 * * 	 BLASTEST double	: OK
 * 	 CTEST			: OK
 * 	 TEST			: OK
 * *
 * **************************************************************************************/


#include "common.h"

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha_r, FLOAT alpha_i, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
	BLASLONG i;
	BLASLONG ix,iy;
	BLASLONG j;
	FLOAT *a_ptr;
	FLOAT temp_r,temp_i;
	BLASLONG inc_x2,inc_y2;
	BLASLONG lda2;
	BLASLONG i2;

	lda2 = 2*lda;

	iy = 0;
	a_ptr = a;

	if ( inc_x == 1 && inc_y == 1 )
	{

	   for (j=0; j<n; j++)
	   {
		temp_r = 0.0;
		temp_i = 0.0;
		ix = 0;
		i2=0;

		for (i=0; i<m; i++)
		{

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
			temp_r += a_ptr[i2] * x[ix]   - a_ptr[i2+1] * x[ix+1];
			temp_i += a_ptr[i2] * x[ix+1] + a_ptr[i2+1] * x[ix];
#else
			temp_r += a_ptr[i2] * x[ix]   + a_ptr[i2+1] * x[ix+1];
			temp_i += a_ptr[i2] * x[ix+1] - a_ptr[i2+1] * x[ix];
#endif

			i2 += 2;
			ix += 2;
		}

#if !defined(XCONJ)
		y[iy]   += alpha_r * temp_r - alpha_i * temp_i;
		y[iy+1] += alpha_r * temp_i + alpha_i * temp_r;
#else
		y[iy]   += alpha_r * temp_r + alpha_i * temp_i;
		y[iy+1] -= alpha_r * temp_i - alpha_i * temp_r;
#endif

		a_ptr += lda2;
		iy    += 2;
	   }

	   return(0);

	}


	inc_x2 = 2 * inc_x;
	inc_y2 = 2 * inc_y;

	for (j=0; j<n; j++)
	{
		temp_r = 0.0;
		temp_i = 0.0;
		ix = 0;
		i2=0;

		for (i=0; i<m; i++)
		{

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
			temp_r += a_ptr[i2] * x[ix]   - a_ptr[i2+1] * x[ix+1];
			temp_i += a_ptr[i2] * x[ix+1] + a_ptr[i2+1] * x[ix];
#else
			temp_r += a_ptr[i2] * x[ix]   + a_ptr[i2+1] * x[ix+1];
			temp_i += a_ptr[i2] * x[ix+1] - a_ptr[i2+1] * x[ix];
#endif

			i2 += 2;
			ix += inc_x2;
		}

#if !defined(XCONJ)
		y[iy]   += alpha_r * temp_r - alpha_i * temp_i;
		y[iy+1] += alpha_r * temp_i + alpha_i * temp_r;
#else
		y[iy]   += alpha_r * temp_r + alpha_i * temp_i;
		y[iy+1] -= alpha_r * temp_i - alpha_i * temp_r;
#endif

		a_ptr += lda2;
		iy    += inc_y2;
	}

	return(0);

}



