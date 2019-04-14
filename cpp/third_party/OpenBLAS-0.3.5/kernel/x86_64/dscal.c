/***************************************************************************
Copyright (c) 2013 - 2015, The OpenBLAS Project
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

#if defined(BULLDOZER) || defined(PILEDRIVER) || defined(STEAMROLLER) || defined(EXCAVATOR)
#include "dscal_microk_bulldozer-2.c"
#elif defined(SANDYBRIDGE)
#include "dscal_microk_sandy-2.c"
#elif defined(HASWELL) || defined(ZEN)
#include "dscal_microk_haswell-2.c"
#elif  defined (SKYLAKEX)
#include "dscal_microk_skylakex-2.c"
#endif


#if !defined(HAVE_KERNEL_8)

static void dscal_kernel_8( BLASLONG n, FLOAT *da , FLOAT *x )
{

	BLASLONG i;
	FLOAT alpha = *da;

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


static void dscal_kernel_8_zero( BLASLONG n, FLOAT *alpha , FLOAT *x )
{

	BLASLONG i;
	for( i=0; i<n; i+=8 )
	{
		x[0] = 0.0;	
		x[1] = 0.0;	
		x[2] = 0.0;	
		x[3] = 0.0;	
		x[4] = 0.0;	
		x[5] = 0.0;	
		x[6] = 0.0;	
		x[7] = 0.0;	
		x+=8;
	}

}

#endif


static void dscal_kernel_inc_8(BLASLONG n, FLOAT *alpha, FLOAT *x, BLASLONG inc_x)  __attribute__ ((noinline));

static void dscal_kernel_inc_8(BLASLONG n, FLOAT *alpha, FLOAT *x, BLASLONG inc_x)
{

	FLOAT *x1=NULL;
	BLASLONG inc_x3;

	inc_x <<= 3;
	inc_x3 = (inc_x << 1) + inc_x;

        __asm__  __volatile__
        (
        "movddup               (%3), %%xmm0                 \n\t"  // alpha     

	"leaq		(%1,%4,4), %2		            \n\t"

        ".p2align 4                                          \n\t"

        "1:                                                 \n\t"
	"movsd	(%1)     , %%xmm4			    \n\t"
	"movhpd (%1,%4,1), %%xmm4			    \n\t"
	"movsd	(%1,%4,2), %%xmm5			    \n\t"
	"movhpd (%1,%5,1), %%xmm5			    \n\t"

	"movsd	(%2)     , %%xmm6			    \n\t"
	"movhpd (%2,%4,1), %%xmm6			    \n\t"
	"movsd	(%2,%4,2), %%xmm7			    \n\t"
	"movhpd (%2,%5,1), %%xmm7			    \n\t"

	"mulpd  %%xmm0, %%xmm4				    \n\t"
	"mulpd  %%xmm0, %%xmm5				    \n\t"
	"mulpd  %%xmm0, %%xmm6				    \n\t"
	"mulpd  %%xmm0, %%xmm7				    \n\t"

	"movsd  %%xmm4 , (%1)				    \n\t"
	"movhpd %%xmm4 , (%1,%4,1)			    \n\t"
	"movsd  %%xmm5 , (%1,%4,2)			    \n\t"
	"movhpd %%xmm5 , (%1,%5,1)			    \n\t"

	"movsd  %%xmm6 , (%2)				    \n\t"
	"movhpd %%xmm6 , (%2,%4,1)			    \n\t"
	"movsd  %%xmm7 , (%2,%4,2)			    \n\t"
	"movhpd %%xmm7 , (%2,%5,1)			    \n\t"

	"leaq   (%1,%4,8), %1				    \n\t"
	"leaq   (%2,%4,8), %2				    \n\t"

	"subq	$8, %0					    \n\t"
	"jnz    1b					    \n\t"

        :
        :
          "r" (n),      // 0
          "r" (x),      // 1
          "r" (x1),     // 2
          "r" (alpha),  // 3
          "r" (inc_x),  // 4
          "r" (inc_x3)  // 5
        : "cc", //"%0", "%1", "%2",
          "%xmm0", "%xmm1", "%xmm2", "%xmm3",
          "%xmm4", "%xmm5", "%xmm6", "%xmm7",
          "%xmm8", "%xmm9", "%xmm10", "%xmm11",
          "%xmm12", "%xmm13", "%xmm14", "%xmm15",
          "memory"
        );


}

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
{
	BLASLONG i=0,j=0;

	if ( inc_x != 1 )
	{

		if ( da == 0.0 )
		{

			BLASLONG n1 = n & -2;

			while(j < n1)
			{
			
				x[i]=0.0;
				x[i+inc_x]=0.0;
				i += 2*inc_x ;
				j+=2;

			}

			while(j < n)
			{
			
				x[i]=0.0;
				i += inc_x ;
				j++;

			}
		}
		else
		{

			BLASLONG n1 = n & -8;
			if ( n1 > 0 )
			{
				dscal_kernel_inc_8(n1, &da, x, inc_x);
				i = n1 * inc_x;
				j = n1;
		        }			

			while(j < n)
			{
			
				x[i] *= da;
				i += inc_x ;
				j++;

			}

		}

		return(0);
	}

	BLASLONG n1 = n & -8;
	if ( n1 > 0 )
	{
		if ( da == 0.0 )
			dscal_kernel_8_zero(n1 , &da , x);
		else
			dscal_kernel_8(n1 , &da , x);
	}

	if ( da == 0.0 )
	{
		for ( i=n1 ; i<n; i++ )
		{
			x[i] = 0.0;
		}
	}
	else
	{

		for ( i=n1 ; i<n; i++ )
		{
			x[i] *= da;
		}
	}
	return(0);
}


