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

int CNAME(BLASLONG n, FLOAT alpha_r, FLOAT alpha_i, FLOAT *x, BLASLONG inc_x, FLOAT beta_r, FLOAT beta_i,FLOAT *y, BLASLONG inc_y)
{
	BLASLONG i=0;
	BLASLONG ix,iy;
	FLOAT temp;
	BLASLONG inc_x2, inc_y2;

	if ( n <= 0     )  return(0);

	ix = 0;
	iy = 0;

	inc_x2 = 2 * inc_x;
	inc_y2 = 2 * inc_y;

	if ( beta_r == 0.0 && beta_i == 0.0)
	{
		if ( alpha_r == 0.0 && alpha_i == 0.0 )
		{

			while(i < n)
			{
				y[iy]   = 0.0 ;
				y[iy+1] = 0.0 ;
				iy += inc_y2 ;
				i++ ;
			}

		}
		else
		{

			while(i < n)
			{
				y[iy]   = ( alpha_r * x[ix]   - alpha_i * x[ix+1] ) ;
				y[iy+1] = ( alpha_r * x[ix+1] + alpha_i * x[ix]   ) ;
				ix += inc_x2 ;
				iy += inc_y2 ;
				i++ ;
			}


		}

	}
	else
	{
		if ( alpha_r == 0.0 && alpha_i == 0.0 )
		{

			while(i < n)
			{
				temp    = ( beta_r * y[iy]   - beta_i * y[iy+1] ) ;
				y[iy+1] = ( beta_r * y[iy+1] + beta_i * y[iy]   ) ;
				y[iy]   = temp;
				iy += inc_y2 ;
				i++ ;
			}

		}
		else
		{

			while(i < n)
			{
				temp    = ( alpha_r * x[ix]   - alpha_i * x[ix+1] ) + ( beta_r * y[iy]   - beta_i * y[iy+1] ) ;
				y[iy+1] = ( alpha_r * x[ix+1] + alpha_i * x[ix]   ) + ( beta_r * y[iy+1] + beta_i * y[iy]   ) ;
				y[iy]   = temp;
				ix += inc_x2 ;
				iy += inc_y2 ;
				i++ ;
			}


		}



	}
	return(0);

}


