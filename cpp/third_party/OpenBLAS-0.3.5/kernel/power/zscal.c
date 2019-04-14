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
* 2016/03/27 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/


#include "common.h"

#pragma GCC optimize "O1"

#if defined(POWER8)
#if defined(DOUBLE)
#include "zscal_microk_power8.c"
#endif
#endif


#ifndef HAVE_KERNEL_8

static void zscal_kernel_8(BLASLONG n, FLOAT *x, FLOAT da_r, FLOAT da_i)
{

	BLASLONG i=0;
	FLOAT *x1=x;
	FLOAT  alpha_r1=da_r;
	FLOAT  alpha_r2=da_r;
	FLOAT  alpha_i1=-da_i;
	FLOAT  alpha_i2=da_i;
	FLOAT  temp00, temp01, temp10, temp11, temp20, temp21, temp30, temp31;
	FLOAT  x0_r, x0_i, x1_r, x1_i, x2_r, x2_i, x3_r, x3_i;

	while ( i<n )
	{
		x0_r = x1[0];
		x0_i = x1[1];
		x1_r = x1[2];
		x1_i = x1[3];
		x2_r = x1[4];
		x2_i = x1[5];
		x3_r = x1[6];
		x3_i = x1[7];

		temp00  = x0_r * alpha_r1;
		temp10  = x1_r * alpha_r1;
		temp20  = x2_r * alpha_r1;
		temp30  = x3_r * alpha_r1;

		temp01  = x0_i * alpha_r2;
		temp11  = x1_i * alpha_r2;
		temp21  = x2_i * alpha_r2;
		temp31  = x3_i * alpha_r2;

		temp00 += x0_i * alpha_i1;
		temp10 += x1_i * alpha_i1;
		temp20 += x2_i * alpha_i1;
		temp30 += x3_i * alpha_i1;

		temp01 += x0_r * alpha_i2;
		temp11 += x1_r * alpha_i2;
		temp21 += x2_r * alpha_i2;
		temp31 += x3_r * alpha_i2;

		x1[0] = temp00;
		x1[1] = temp01;
		x1[2] = temp10;
		x1[3] = temp11;
		x1[4] = temp20;
		x1[5] = temp21;
		x1[6] = temp30;
		x1[7] = temp31;

		x1 += 8;
		i+=4;

	}
	return;


}

#endif

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da_r,FLOAT da_i, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
{
	BLASLONG i=0;
	BLASLONG inc_x2;
	BLASLONG ip = 0;
	FLOAT temp;
	BLASLONG n1;

	if ( n <= 0 )
		return(0);

	if ( inc_x <= 0 )
		return(0);

	if (da_r == ZERO && da_i == ZERO) {
	  //clear the vector and return
	  if (inc_x == 1) {
	    memset(x, 0, n*COMPSIZE*SIZE);
	  }else{
	    inc_x2 = 2 * inc_x;
	    for(i=0; i<n; i++){
	      x[ip]=ZERO; 
	      x[ip+1]=ZERO;
	      ip += inc_x2;
	    }
	  }
	  return 0;
	}

	if ( inc_x == 1 )
	{


		n1 = n & -8;
		if ( n1 > 0 )
		{
			zscal_kernel_8(n1, x, da_r, da_i);
			i=n1;
			ip = n1 * 2;

		}

		while ( i < n )
		{

				temp    = da_r * x[ip]   - da_i * x[ip+1] ;
				x[ip+1] = da_r * x[ip+1] + da_i * x[ip]   ;
				x[ip]   = temp;
				ip += 2;
				i++;
		}

	}
	else
	{

		inc_x2 = 2 * inc_x;

		while ( i < n )
		{

				temp    = da_r * x[ip]   - da_i * x[ip+1] ;
				x[ip+1] = da_r * x[ip+1] + da_i * x[ip]   ;
				x[ip]   = temp;
				ip += inc_x2;
				i++;
		}


	}

	return(0);

}


