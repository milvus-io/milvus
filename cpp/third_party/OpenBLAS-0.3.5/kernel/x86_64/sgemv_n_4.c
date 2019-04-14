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


#if defined(BULLDOZER) || defined(PILEDRIVER) || defined(STEAMROLLER)  || defined(EXCAVATOR)
#include "sgemv_n_microk_bulldozer-4.c"
#elif defined(NEHALEM)
#include "sgemv_n_microk_nehalem-4.c"
#elif defined(SANDYBRIDGE)
#include "sgemv_n_microk_sandy-4.c"
#elif defined(HASWELL) || defined(ZEN) || defined (SKYLAKEX)
#include "sgemv_n_microk_haswell-4.c"
#endif

#if defined(STEAMROLLER)  || defined(EXCAVATOR)
#define NBMAX 2048
#else
#define NBMAX 4096
#endif

#ifndef HAVE_KERNEL_4x8

static void sgemv_kernel_4x8(BLASLONG n, FLOAT **ap, FLOAT *xo, FLOAT *y, BLASLONG lda4, FLOAT *alpha)
{
	BLASLONG i;
	FLOAT *a0,*a1,*a2,*a3;
	FLOAT *b0,*b1,*b2,*b3;
	FLOAT *x4;
	FLOAT x[8];
	a0 = ap[0];
	a1 = ap[1];
	a2 = ap[2];
	a3 = ap[3];
	b0 = a0 + lda4 ;
	b1 = a1 + lda4 ;
	b2 = a2 + lda4 ;
	b3 = a3 + lda4 ;
	x4 = x + 4;

	for ( i=0; i<8; i++)
		x[i] = xo[i] * *alpha;

	for ( i=0; i< n; i+=4 )
	{

		y[i] += a0[i]*x[0] + a1[i]*x[1] + a2[i]*x[2] + a3[i]*x[3];		
		y[i+1] += a0[i+1]*x[0] + a1[i+1]*x[1] + a2[i+1]*x[2] + a3[i+1]*x[3];		
		y[i+2] += a0[i+2]*x[0] + a1[i+2]*x[1] + a2[i+2]*x[2] + a3[i+2]*x[3];		
		y[i+3] += a0[i+3]*x[0] + a1[i+3]*x[1] + a2[i+3]*x[2] + a3[i+3]*x[3];		

		y[i] += b0[i]*x4[0] + b1[i]*x4[1] + b2[i]*x4[2] + b3[i]*x4[3];		
		y[i+1] += b0[i+1]*x4[0] + b1[i+1]*x4[1] + b2[i+1]*x4[2] + b3[i+1]*x4[3];		
		y[i+2] += b0[i+2]*x4[0] + b1[i+2]*x4[1] + b2[i+2]*x4[2] + b3[i+2]*x4[3];		
		y[i+3] += b0[i+3]*x4[0] + b1[i+3]*x4[1] + b2[i+3]*x4[2] + b3[i+3]*x4[3];		

	}
}
	
#endif


#ifndef HAVE_KERNEL_4x4

static void sgemv_kernel_4x4(BLASLONG n, FLOAT **ap, FLOAT *xo, FLOAT *y, FLOAT *alpha)
{
	BLASLONG i;
	FLOAT *a0,*a1,*a2,*a3;
	FLOAT x[4];
	a0 = ap[0];
	a1 = ap[1];
	a2 = ap[2];
	a3 = ap[3];

	for ( i=0; i<4; i++)
		x[i] = xo[i] * *alpha;

	for ( i=0; i< n; i+=4 )
	{
		y[i] += a0[i]*x[0] + a1[i]*x[1] + a2[i]*x[2] + a3[i]*x[3];		
		y[i+1] += a0[i+1]*x[0] + a1[i+1]*x[1] + a2[i+1]*x[2] + a3[i+1]*x[3];		
		y[i+2] += a0[i+2]*x[0] + a1[i+2]*x[1] + a2[i+2]*x[2] + a3[i+2]*x[3];		
		y[i+3] += a0[i+3]*x[0] + a1[i+3]*x[1] + a2[i+3]*x[2] + a3[i+3]*x[3];		
	}
}
	
#endif

#ifndef HAVE_KERNEL_4x2

static void sgemv_kernel_4x2( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y, FLOAT *alpha) __attribute__ ((noinline));

static void sgemv_kernel_4x2( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y, FLOAT *alpha)
{

	BLASLONG register i = 0;

	__asm__  __volatile__
	(
	"movss    (%2)  , %%xmm12	 \n\t"	// x0 
	"movss    (%6)  , %%xmm4 	 \n\t"	// alpha 
	"movss   4(%2)  , %%xmm13	 \n\t"	// x1 
        "mulss  %%xmm4  , %%xmm12        \n\t"  // alpha 
        "mulss  %%xmm4  , %%xmm13        \n\t"  // alpha 
	"shufps $0,  %%xmm12, %%xmm12    \n\t"	
	"shufps $0,  %%xmm13, %%xmm13    \n\t"	

	//	".align 16				       \n\t"
	"1:				       \n\t"
	"movups	       (%3,%0,4), %%xmm4	       \n\t"	// 4 * y

	"movups             (%4,%0,4), %%xmm8          \n\t" 
	"movups             (%5,%0,4), %%xmm9          \n\t" 
	"mulps		%%xmm12, %%xmm8		       \n\t"
	"mulps		%%xmm13, %%xmm9		       \n\t"
	"addps		%%xmm8 , %%xmm4		       \n\t"
        "addq		$4 , %0	  	 	       \n\t"
	"addps		%%xmm9 , %%xmm4		       \n\t"

	"movups  %%xmm4 , -16(%3,%0,4)		       \n\t"	// 4 * y

	"subq	        $4 , %1			       \n\t"		
	"jnz		1b		       \n\t"

	:
        : 
          "r" (i),	// 0	
	  "r" (n),  	// 1
          "r" (x),      // 2
          "r" (y),      // 3
          "r" (ap[0]),  // 4
          "r" (ap[1]),  // 5
          "r" (alpha)   // 6
	: "cc", 
	  "%xmm4", "%xmm5", 
	  "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11",
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

} 

#endif

#ifndef HAVE_KERNEL_4x1

static void sgemv_kernel_4x1(BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT *alpha) __attribute__ ((noinline));

static void sgemv_kernel_4x1(BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT *alpha)
{

        BLASLONG register i = 0;
	BLASLONG register n1 = n & -8 ;
	BLASLONG register n2 = n & 4  ;

        __asm__  __volatile__
        (
        "movss          (%2), %%xmm12            \n\t"  // x0 
        "mulss          (%6), %%xmm12            \n\t"  // alpha 
        "shufps $0,  %%xmm12, %%xmm12            \n\t"

        "cmpq           $0, %1                   \n\t"
        "je             2f                \n\t"

	//        ".align 16                               \n\t"
        "1:                             \n\t"
        "movups       (%3,%0,4), %%xmm4          \n\t"  // 4 * y
        "movups     16(%3,%0,4), %%xmm5          \n\t"  // 4 * y
        "movups       (%4,%0,4), %%xmm8          \n\t"  // 4 * a
        "movups     16(%4,%0,4), %%xmm9          \n\t"  // 4 * a
	"mulps          %%xmm12, %%xmm8          \n\t"
	"mulps          %%xmm12, %%xmm9          \n\t"
        "addps          %%xmm4 , %%xmm8          \n\t"
        "addps          %%xmm5 , %%xmm9          \n\t"

        "addq           $8 , %0                  \n\t"
	"movups  %%xmm8 , -32(%3,%0,4)           \n\t"    // 4 * y
	"movups  %%xmm9 , -16(%3,%0,4)           \n\t"    // 4 * y

        "subq           $8 , %1                  \n\t"

        "jnz            1b               \n\t"

        "2:                              \n\t"

        "testq          $0x04, %5                \n\t"
        "jz             3f              \n\t"

        "movups       (%3,%0,4), %%xmm4          \n\t"  // 4 * y
        "movups       (%4,%0,4), %%xmm8          \n\t"  // 4 * a
	"mulps          %%xmm12, %%xmm8          \n\t"
        "addps          %%xmm8 , %%xmm4          \n\t"
	"movups  %%xmm4 ,    (%3,%0,4)           \n\t"    // 4 * y
        "addq           $4 , %0                  \n\t"
        "subq           $4 , %1                  \n\t"

        "3:      			 \n\t" 
        :
        :
          "r" (i),      // 0    
          "r" (n1),     // 1
          "r" (x),      // 2
          "r" (y),      // 3
          "r" (ap),     // 4
          "r" (n2),     // 5
          "r" (alpha)   // 6
        : "cc",
          "%xmm4", "%xmm5",
          "%xmm6", "%xmm7",
          "%xmm8", "%xmm9", "%xmm10", "%xmm11",
          "%xmm12", "%xmm13", "%xmm14", "%xmm15",
          "memory"
        );

}

#endif

static void add_y(BLASLONG n, FLOAT *src, FLOAT *dest, BLASLONG inc_dest) __attribute__ ((noinline));

static void add_y(BLASLONG n, FLOAT *src, FLOAT *dest, BLASLONG inc_dest)
{
	BLASLONG i;
	if ( inc_dest != 1 )
	{
		for ( i=0; i<n; i++ )
		{
			*dest += *src;
			src++;
			dest += inc_dest;
		}
		return;
	}

        i=0;

        __asm__  __volatile__
        (

	 //        ".align 16                              \n\t"
        "1:                            \n\t"

        "movups  (%2,%0,4) , %%xmm12            \n\t"
        "movups  (%3,%0,4) , %%xmm11            \n\t"
        "addps   %%xmm12   , %%xmm11            \n\t"
        "addq           $4 , %0                 \n\t"
        "movups  %%xmm11, -16(%3,%0,4)          \n\t"

        "subq           $4 , %1                 \n\t"
        "jnz            1b              \n\t"

        :
        :
        "r" (i),          // 0
        "r" (n),          // 1
        "r" (src),        // 2
        "r" (dest)        // 3
        : "cc",
        "%xmm10", "%xmm11", "%xmm12",
        "memory"
        );

}

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
	BLASLONG i;
	FLOAT *a_ptr;
	FLOAT *x_ptr;
	FLOAT *y_ptr;
	FLOAT *ap[4];
	BLASLONG n1;
	BLASLONG m1;
	BLASLONG m2;
	BLASLONG m3;
	BLASLONG n2;
	BLASLONG lda4 =  lda << 2;
	BLASLONG lda8 =  lda << 3;
	FLOAT xbuffer[8],*ybuffer;

        if ( m < 1 ) return(0);
        if ( n < 1 ) return(0);

	ybuffer = buffer;
	
        if ( inc_x == 1 )
	{
		n1 = n >> 3 ;
		n2 = n &  7 ;
	}
	else
	{
		n1 = n >> 2 ;
		n2 = n &  3 ;

	}
	
        m3 = m & 3  ;
        m1 = m & -4 ;
        m2 = (m & (NBMAX-1)) - m3 ;


	y_ptr = y;

	BLASLONG NB = NBMAX;

	while ( NB == NBMAX )
	{
		
		m1 -= NB;
		if ( m1 < 0)
		{
			if ( m2 == 0 ) break;	
			NB = m2;
		}
		
		a_ptr = a;
		x_ptr = x;
		
		ap[0] = a_ptr;
		ap[1] = a_ptr + lda;
		ap[2] = ap[1] + lda;
		ap[3] = ap[2] + lda;

		if ( inc_y != 1 )
			memset(ybuffer,0,NB*4);
		else
			ybuffer = y_ptr;

		if ( inc_x == 1 )
		{


			for( i = 0; i < n1 ; i++)
			{
				sgemv_kernel_4x8(NB,ap,x_ptr,ybuffer,lda4,&alpha);
				ap[0] += lda8; 
				ap[1] += lda8; 
				ap[2] += lda8; 
				ap[3] += lda8; 
				a_ptr += lda8;
				x_ptr += 8;	
			}


			if ( n2 & 4 )
			{
				sgemv_kernel_4x4(NB,ap,x_ptr,ybuffer,&alpha);
				ap[0] += lda4; 
				ap[1] += lda4; 
				ap[2] += lda4; 
				ap[3] += lda4; 
				a_ptr += lda4;
				x_ptr += 4;	
			}

			if ( n2 & 2 )
			{
				sgemv_kernel_4x2(NB,ap,x_ptr,ybuffer,&alpha);
				a_ptr += lda*2;
				x_ptr += 2;	
			}


			if ( n2 & 1 )
			{
				sgemv_kernel_4x1(NB,a_ptr,x_ptr,ybuffer,&alpha);
				/* a_ptr += lda;
				x_ptr += 1a; */

			}


		}
		else
		{

			for( i = 0; i < n1 ; i++)
			{
				xbuffer[0] = x_ptr[0];
				x_ptr += inc_x;	
				xbuffer[1] =  x_ptr[0];
				x_ptr += inc_x;	
				xbuffer[2] =  x_ptr[0];
				x_ptr += inc_x;	
				xbuffer[3] = x_ptr[0];
				x_ptr += inc_x;	
				sgemv_kernel_4x4(NB,ap,xbuffer,ybuffer,&alpha);
				ap[0] += lda4; 
				ap[1] += lda4; 
				ap[2] += lda4; 
				ap[3] += lda4; 
				a_ptr += lda4;
			}

			for( i = 0; i < n2 ; i++)
			{
				xbuffer[0] = x_ptr[0];
				x_ptr += inc_x;	
				sgemv_kernel_4x1(NB,a_ptr,xbuffer,ybuffer,&alpha);
				a_ptr += lda;

			}

		}

		a     += NB;
		if ( inc_y != 1 )
		{
			add_y(NB,ybuffer,y_ptr,inc_y);
			y_ptr += NB * inc_y;
		}
		else
			y_ptr += NB ;

	}

	if ( m3 == 0 ) return(0);

	if ( m3 == 3 )
	{
		a_ptr = a;
		x_ptr = x;
		FLOAT temp0 = 0.0;
		FLOAT temp1 = 0.0;
		FLOAT temp2 = 0.0;
		if ( lda == 3 && inc_x ==1 )
		{

			for( i = 0; i < ( n & -4 ); i+=4 )
			{

				temp0 += a_ptr[0] * x_ptr[0] + a_ptr[3] * x_ptr[1];
				temp1 += a_ptr[1] * x_ptr[0] + a_ptr[4] * x_ptr[1];
				temp2 += a_ptr[2] * x_ptr[0] + a_ptr[5] * x_ptr[1];

				temp0 += a_ptr[6] * x_ptr[2] + a_ptr[9]  * x_ptr[3];
				temp1 += a_ptr[7] * x_ptr[2] + a_ptr[10] * x_ptr[3];
				temp2 += a_ptr[8] * x_ptr[2] + a_ptr[11] * x_ptr[3];

				a_ptr += 12;
				x_ptr += 4;
			}

			for( ; i < n; i++ )
			{
				temp0 += a_ptr[0] * x_ptr[0];
				temp1 += a_ptr[1] * x_ptr[0];
				temp2 += a_ptr[2] * x_ptr[0];
				a_ptr += 3;
				x_ptr ++;
			}

		}
		else
		{

			for( i = 0; i < n; i++ )
			{
				temp0 += a_ptr[0] * x_ptr[0];
				temp1 += a_ptr[1] * x_ptr[0];
				temp2 += a_ptr[2] * x_ptr[0];
				a_ptr += lda;
				x_ptr += inc_x;


			}

		}
		y_ptr[0] += alpha * temp0;
		y_ptr += inc_y;
		y_ptr[0] += alpha * temp1;
		y_ptr += inc_y;
		y_ptr[0] += alpha * temp2;
		return(0);
	}


	if ( m3 == 2 )
	{
		a_ptr = a;
		x_ptr = x;
		FLOAT temp0 = 0.0;
		FLOAT temp1 = 0.0;
		if ( lda == 2 && inc_x ==1 )
		{

			for( i = 0; i < (n & -4) ; i+=4 )
			{
				temp0 += a_ptr[0] * x_ptr[0] + a_ptr[2] * x_ptr[1];
				temp1 += a_ptr[1] * x_ptr[0] + a_ptr[3] * x_ptr[1];
				temp0 += a_ptr[4] * x_ptr[2] + a_ptr[6] * x_ptr[3];
				temp1 += a_ptr[5] * x_ptr[2] + a_ptr[7] * x_ptr[3];
				a_ptr += 8;
				x_ptr += 4;

			}


			for( ; i < n; i++ )
			{
				temp0 += a_ptr[0]   * x_ptr[0];
				temp1 += a_ptr[1]   * x_ptr[0];
				a_ptr += 2;
				x_ptr ++;
			}

		}
		else
		{

			for( i = 0; i < n; i++ )
			{
				temp0 += a_ptr[0] * x_ptr[0];
				temp1 += a_ptr[1] * x_ptr[0];
				a_ptr += lda;
				x_ptr += inc_x;


			}

		}
		y_ptr[0] += alpha * temp0;
		y_ptr += inc_y;
		y_ptr[0] += alpha * temp1;
		return(0);
	}

	if ( m3 == 1 )
	{
		a_ptr = a;
		x_ptr = x;
		FLOAT temp = 0.0;
		if ( lda == 1 && inc_x ==1 )
		{

			for( i = 0; i < (n & -4); i+=4 )
			{
				temp += a_ptr[i] * x_ptr[i] + a_ptr[i+1] * x_ptr[i+1] + a_ptr[i+2] * x_ptr[i+2] + a_ptr[i+3] * x_ptr[i+3];
	
			}

			for( ; i < n; i++ )
			{
				temp += a_ptr[i] * x_ptr[i];
			}

		}
		else
		{

			for( i = 0; i < n; i++ )
			{
				temp += a_ptr[0] * x_ptr[0];
				a_ptr += lda;
				x_ptr += inc_x;
			}

		}
		y_ptr[0] += alpha * temp;
		return(0);
	}


	return(0);
}


