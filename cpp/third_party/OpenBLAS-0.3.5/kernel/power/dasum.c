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
* 2016/03/28 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/

#include "common.h"
#include <math.h>

#if defined(DOUBLE)

#define ABS fabs

#else

#error supports double only

#endif

#if defined(POWER8)
#include "dasum_microk_power8.c"
#endif


#ifndef HAVE_KERNEL_16

static FLOAT dasum_kernel_16(BLASLONG n, FLOAT *x1)
{

	BLASLONG i=0;
	FLOAT *x = x1;
	FLOAT temp0, temp1, temp2, temp3;
	FLOAT temp4, temp5, temp6, temp7;
	FLOAT sum0 = 0.0;
	FLOAT sum1 = 0.0;
	FLOAT sum2 = 0.0;
	FLOAT sum3 = 0.0;

	while ( i< n )
	{

		temp0 = ABS(x[0]);
		temp1 = ABS(x[1]);
		temp2 = ABS(x[2]);
		temp3 = ABS(x[3]);
		temp4 = ABS(x[4]);
		temp5 = ABS(x[5]);
		temp6 = ABS(x[6]);
		temp7 = ABS(x[7]);

		sum0 += temp0;
		sum1 += temp1;
		sum2 += temp2;
		sum3 += temp3;

		sum0 += temp4;
		sum1 += temp5;
		sum2 += temp6;
		sum3 += temp7;

		x+=8;
		i+=8;

	}

	return sum0+sum1+sum2+sum3;
}

#endif

FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
	BLASLONG i=0;
	FLOAT sumf = 0.0;
	BLASLONG n1;

	if (n <= 0 || inc_x <= 0) return(sumf);

	if ( inc_x == 1 )
	{

		n1 = n & -16;
		if ( n1 > 0 )
		{

			sumf = dasum_kernel_16(n1, x);
			i=n1;
		}

		while(i < n)
		{
			sumf += ABS(x[i]);
			i++;
		}

	}
	else
	{

		n *= inc_x;
		while(i < n)
		{
			sumf += ABS(x[i]);
			i += inc_x;
		}

	}
	return(sumf);
}


