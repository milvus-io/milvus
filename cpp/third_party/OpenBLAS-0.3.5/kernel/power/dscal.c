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
* 2016/03/25 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/

#include "common.h"

#if defined(POWER8) 
#include "dscal_microk_power8.c"
#endif

#if !defined(HAVE_KERNEL_8)

static void dscal_kernel_8 (BLASLONG n, FLOAT *x, FLOAT alpha)
{

        BLASLONG i;

        for( i=0; i<n; i+=8 )
        {
                x[0] *= alpha;
                x[1] *= alpha;
                x[2] *= alpha;
                x[3] *= alpha;
                x[4] *= alpha;
                x[5] *= alpha;
                x[6] *= alpha;
                x[7] *= alpha;
                x+=8;
        }

}

static void dscal_kernel_8_zero (BLASLONG n, FLOAT *x)
{

        BLASLONG i;
	FLOAT alpha=0.0;

        for( i=0; i<n; i+=8 )
        {
                x[0] = alpha;
                x[1] = alpha;
                x[2] = alpha;
                x[3] = alpha;
                x[4] = alpha;
                x[5] = alpha;
                x[6] = alpha;
                x[7] = alpha;
                x+=8;
        }

}


#endif

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
{
	BLASLONG i=0,j=0;
	if ( n <= 0 || inc_x <=0 )
		return(0);


	if ( inc_x == 1 )
	{

		if ( da == 0.0 )
		{		

			BLASLONG n1 = n & -16;
			if ( n1 > 0 )
			{
				dscal_kernel_8_zero(n1, x);
				j=n1;
			}

			while(j < n)
			{

				x[j]=0.0;
				j++;
			}

		}
		else
		{

			BLASLONG n1 = n & -16;
			if ( n1 > 0 )
			{
				dscal_kernel_8(n1, x, da);
				j=n1;
			}
			while(j < n)
			{

				x[j] = da * x[j] ;
				j++;
			}
		}


	}
	else
	{

		if ( da == 0.0 )
		{		

			while(j < n)
			{

				x[i]=0.0;
				i += inc_x ;
				j++;
			}

		}
		else
		{

			while(j < n)
			{

				x[i] = da * x[i] ;
				i += inc_x ;
				j++;
			}
		}

	}
	return 0;

}


