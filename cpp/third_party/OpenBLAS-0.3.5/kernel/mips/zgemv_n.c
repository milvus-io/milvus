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

	ix = 0;
	a_ptr = a;

	if ( inc_x == 1 && inc_y == 1 )
	{

	   for (j=0; j<n; j++)
	   {

#if !defined(XCONJ)
		temp_r = alpha_r * x[ix]   - alpha_i * x[ix+1];
		temp_i = alpha_r * x[ix+1] + alpha_i * x[ix];
#else
		temp_r = alpha_r * x[ix]   + alpha_i * x[ix+1];
		temp_i = alpha_r * x[ix+1] - alpha_i * x[ix];
#endif
		iy = 0;
		i2=0;

		for (i=0; i<m; i++)
		{
#if !defined(CONJ)

#if !defined(XCONJ)
			y[iy]   += temp_r * a_ptr[i2]   - temp_i * a_ptr[i2+1];
			y[iy+1] += temp_r * a_ptr[i2+1] + temp_i * a_ptr[i2];
#else
			y[iy]   += temp_r * a_ptr[i2]   + temp_i * a_ptr[i2+1];
			y[iy+1] += temp_r * a_ptr[i2+1] - temp_i * a_ptr[i2];
#endif

#else

#if !defined(XCONJ)
			y[iy]   += temp_r * a_ptr[i2]   + temp_i * a_ptr[i2+1];
			y[iy+1] -= temp_r * a_ptr[i2+1] - temp_i * a_ptr[i2];
#else
			y[iy]   += temp_r * a_ptr[i2]   - temp_i * a_ptr[i2+1];
			y[iy+1] -= temp_r * a_ptr[i2+1] + temp_i * a_ptr[i2];
#endif

#endif
			i2 += 2;
			iy += 2;
		}
		a_ptr += lda2;
		ix    += 2;
	   }

	   return(0);

	}


	inc_x2 = 2 * inc_x;
	inc_y2 = 2 * inc_y;

	for (j=0; j<n; j++)
	{

#if !defined(XCONJ)
		temp_r = alpha_r * x[ix]   - alpha_i * x[ix+1];
		temp_i = alpha_r * x[ix+1] + alpha_i * x[ix];
#else
		temp_r = alpha_r * x[ix]   + alpha_i * x[ix+1];
		temp_i = alpha_r * x[ix+1] - alpha_i * x[ix];
#endif
		iy = 0;
		i2=0;

		for (i=0; i<m; i++)
		{
#if !defined(CONJ)

#if !defined(XCONJ)
			y[iy]   += temp_r * a_ptr[i2]   - temp_i * a_ptr[i2+1];
			y[iy+1] += temp_r * a_ptr[i2+1] + temp_i * a_ptr[i2];
#else
			y[iy]   += temp_r * a_ptr[i2]   + temp_i * a_ptr[i2+1];
			y[iy+1] += temp_r * a_ptr[i2+1] - temp_i * a_ptr[i2];
#endif

#else

#if !defined(XCONJ)
			y[iy]   += temp_r * a_ptr[i2]   + temp_i * a_ptr[i2+1];
			y[iy+1] -= temp_r * a_ptr[i2+1] - temp_i * a_ptr[i2];
#else
			y[iy]   += temp_r * a_ptr[i2]   - temp_i * a_ptr[i2+1];
			y[iy+1] -= temp_r * a_ptr[i2+1] + temp_i * a_ptr[i2];
#endif

#endif
			i2 += 2;
			iy += inc_y2;
		}
		a_ptr += lda2;
		ix    += inc_x2;
	}


	return(0);
}


