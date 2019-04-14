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

#include <stdlib.h>
#include <stdio.h>
#include "common.h"

#if defined(HASWELL) || defined(ZEN) || defined (SKYLAKEX)
#include "cgemv_n_microk_haswell-4.c"
#elif defined(BULLDOZER) || defined(PILEDRIVER) || defined(STEAMROLLER) || defined(EXCAVATOR)
#include "cgemv_n_microk_bulldozer-4.c"
#endif


#define NBMAX 2048

#ifndef HAVE_KERNEL_4x4

static void cgemv_kernel_4x4(BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y)
{
	BLASLONG i;
	FLOAT *a0,*a1,*a2,*a3;
	a0 = ap[0];
	a1 = ap[1];
	a2 = ap[2];
	a3 = ap[3];

	for ( i=0; i< 2*n; i+=2 )
	{
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
		y[i]   += a0[i]*x[0] - a0[i+1] * x[1];
		y[i+1] += a0[i]*x[1] + a0[i+1] * x[0];
		y[i]   += a1[i]*x[2] - a1[i+1] * x[3];
		y[i+1] += a1[i]*x[3] + a1[i+1] * x[2];
		y[i]   += a2[i]*x[4] - a2[i+1] * x[5];
		y[i+1] += a2[i]*x[5] + a2[i+1] * x[4];
		y[i]   += a3[i]*x[6] - a3[i+1] * x[7];
		y[i+1] += a3[i]*x[7] + a3[i+1] * x[6];
#else 
		y[i]   += a0[i]*x[0] + a0[i+1] * x[1];
		y[i+1] += a0[i]*x[1] - a0[i+1] * x[0];
		y[i]   += a1[i]*x[2] + a1[i+1] * x[3];
		y[i+1] += a1[i]*x[3] - a1[i+1] * x[2];
		y[i]   += a2[i]*x[4] + a2[i+1] * x[5];
		y[i+1] += a2[i]*x[5] - a2[i+1] * x[4];
		y[i]   += a3[i]*x[6] + a3[i+1] * x[7];
		y[i+1] += a3[i]*x[7] - a3[i+1] * x[6];
#endif
	}
}
	
#endif



#ifndef HAVE_KERNEL_4x2

static void cgemv_kernel_4x2(BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y)
{
	BLASLONG i;
	FLOAT *a0,*a1;
	a0 = ap[0];
	a1 = ap[1];

	for ( i=0; i< 2*n; i+=2 )
	{
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
		y[i]   += a0[i]*x[0] - a0[i+1] * x[1];
		y[i+1] += a0[i]*x[1] + a0[i+1] * x[0];
		y[i]   += a1[i]*x[2] - a1[i+1] * x[3];
		y[i+1] += a1[i]*x[3] + a1[i+1] * x[2];
#else 
		y[i]   += a0[i]*x[0] + a0[i+1] * x[1];
		y[i+1] += a0[i]*x[1] - a0[i+1] * x[0];
		y[i]   += a1[i]*x[2] + a1[i+1] * x[3];
		y[i+1] += a1[i]*x[3] - a1[i+1] * x[2];
#endif
	}
}
	
#endif




#ifndef HAVE_KERNEL_4x1


static void cgemv_kernel_4x1(BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y)
{
	BLASLONG i;
	FLOAT *a0;
	a0 = ap;

	for ( i=0; i< 2*n; i+=2 )
	{
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
		y[i]   += a0[i]*x[0] - a0[i+1] * x[1];
		y[i+1] += a0[i]*x[1] + a0[i+1] * x[0];
#else 
		y[i]   += a0[i]*x[0] + a0[i+1] * x[1];
		y[i+1] += a0[i]*x[1] - a0[i+1] * x[0];
#endif

	}
}


#endif


#ifndef HAVE_KERNEL_ADDY

static void add_y(BLASLONG n, FLOAT *src, FLOAT *dest, BLASLONG inc_dest,FLOAT alpha_r, FLOAT alpha_i)  __attribute__ ((noinline));

static void add_y(BLASLONG n, FLOAT *src, FLOAT *dest, BLASLONG inc_dest,FLOAT alpha_r, FLOAT alpha_i)
{
	BLASLONG i;

	if ( inc_dest != 2 )
	{

		FLOAT temp_r;
		FLOAT temp_i;
		for ( i=0; i<n; i++ )
		{
#if !defined(XCONJ) 
			temp_r = alpha_r * src[0] - alpha_i * src[1];
			temp_i = alpha_r * src[1] + alpha_i * src[0];
#else
			temp_r =  alpha_r * src[0] + alpha_i * src[1];
			temp_i = -alpha_r * src[1] + alpha_i * src[0];
#endif

			*dest += temp_r;
			*(dest+1) += temp_i;

			src+=2;
			dest += inc_dest;
		}
		return;
	}

	FLOAT temp_r0;
	FLOAT temp_i0;
	FLOAT temp_r1;
	FLOAT temp_i1;
	FLOAT temp_r2;
	FLOAT temp_i2;
	FLOAT temp_r3;
	FLOAT temp_i3;
	for ( i=0; i<n; i+=4 )
	{
#if !defined(XCONJ) 
		temp_r0 = alpha_r * src[0] - alpha_i * src[1];
		temp_i0 = alpha_r * src[1] + alpha_i * src[0];
		temp_r1 = alpha_r * src[2] - alpha_i * src[3];
		temp_i1 = alpha_r * src[3] + alpha_i * src[2];
		temp_r2 = alpha_r * src[4] - alpha_i * src[5];
		temp_i2 = alpha_r * src[5] + alpha_i * src[4];
		temp_r3 = alpha_r * src[6] - alpha_i * src[7];
		temp_i3 = alpha_r * src[7] + alpha_i * src[6];
#else
		temp_r0 =  alpha_r * src[0] + alpha_i * src[1];
		temp_i0 = -alpha_r * src[1] + alpha_i * src[0];
		temp_r1 =  alpha_r * src[2] + alpha_i * src[3];
		temp_i1 = -alpha_r * src[3] + alpha_i * src[2];
		temp_r2 =  alpha_r * src[4] + alpha_i * src[5];
		temp_i2 = -alpha_r * src[5] + alpha_i * src[4];
		temp_r3 =  alpha_r * src[6] + alpha_i * src[7];
		temp_i3 = -alpha_r * src[7] + alpha_i * src[6];
#endif

		dest[0]   += temp_r0;
		dest[1]   += temp_i0;
		dest[2]   += temp_r1;
		dest[3]   += temp_i1;
		dest[4]   += temp_r2;
		dest[5]   += temp_i2;
		dest[6]   += temp_r3;
		dest[7]   += temp_i3;

		src  += 8;
		dest += 8;
	}
	return;

}

#endif

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha_r,FLOAT alpha_i, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
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
	BLASLONG lda4;
	FLOAT xbuffer[8],*ybuffer;


#if 0
printf("%s %d %d %.16f %.16f %d %d %d\n","zgemv_n",m,n,alpha_r,alpha_i,lda,inc_x,inc_y);
#endif

	if ( m < 1 ) return(0);
	if ( n < 1 ) return(0);

	ybuffer = buffer;
	
	inc_x *= 2;
	inc_y *= 2;
	lda   *= 2;
	lda4  = 4 * lda;

	n1 = n / 4 ;
	n2 = n % 4 ;
	
	m3 = m % 4;
	m1 = m - ( m % 4 );
	m2 = (m % NBMAX) - (m % 4) ;
	
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
		ap[0] = a_ptr;
		ap[1] = a_ptr + lda;
		ap[2] = ap[1] + lda;
		ap[3] = ap[2] + lda;
		x_ptr = x;
		//zero_y(NB,ybuffer);
		memset(ybuffer,0,NB*8);

		if ( inc_x == 2 )
		{

			for( i = 0; i < n1 ; i++)
			{
				cgemv_kernel_4x4(NB,ap,x_ptr,ybuffer);
				ap[0] += lda4;
				ap[1] += lda4;
				ap[2] += lda4;
				ap[3] += lda4;
				a_ptr += lda4;
				x_ptr += 8;	
			}

			if ( n2 & 2 )
			{
				cgemv_kernel_4x2(NB,ap,x_ptr,ybuffer);
				x_ptr += 4;	
				a_ptr += 2 * lda;

			}

			if ( n2 & 1 )
			{
				cgemv_kernel_4x1(NB,a_ptr,x_ptr,ybuffer);
				/* x_ptr += 2;	
				a_ptr += lda; */

			}
		}
		else
		{

			for( i = 0; i < n1 ; i++)
			{

				xbuffer[0] = x_ptr[0];
				xbuffer[1] = x_ptr[1];
				x_ptr += inc_x;	
				xbuffer[2] = x_ptr[0];
				xbuffer[3] = x_ptr[1];
				x_ptr += inc_x;	
				xbuffer[4] = x_ptr[0];
				xbuffer[5] = x_ptr[1];
				x_ptr += inc_x;	
				xbuffer[6] = x_ptr[0];
				xbuffer[7] = x_ptr[1];
				x_ptr += inc_x;	

				cgemv_kernel_4x4(NB,ap,xbuffer,ybuffer);
				ap[0] += lda4;
				ap[1] += lda4;
				ap[2] += lda4;
				ap[3] += lda4;
				a_ptr += lda4;
			}

			for( i = 0; i < n2 ; i++)
			{
				xbuffer[0] = x_ptr[0];
				xbuffer[1] = x_ptr[1];
				x_ptr += inc_x;	
				cgemv_kernel_4x1(NB,a_ptr,xbuffer,ybuffer);
				a_ptr += 1 * lda;

			}

		}

		add_y(NB,ybuffer,y_ptr,inc_y,alpha_r,alpha_i);
		a     += 2 * NB;
		y_ptr += NB * inc_y;
	}

	if ( m3 == 0 ) return(0);

	if ( m3 == 1 )
	{
		a_ptr = a;
		x_ptr = x;
		FLOAT temp_r = 0.0;
		FLOAT temp_i = 0.0;

		if ( lda == 2 && inc_x == 2 )
		{


			for( i=0 ; i < (n & -2); i+=2 )
			{
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
				temp_r += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
				temp_i += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
				temp_r += a_ptr[2] * x_ptr[2] - a_ptr[3] * x_ptr[3];
				temp_i += a_ptr[2] * x_ptr[3] + a_ptr[3] * x_ptr[2];
#else
				temp_r += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
				temp_i += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
				temp_r += a_ptr[2] * x_ptr[2] + a_ptr[3] * x_ptr[3];
				temp_i += a_ptr[2] * x_ptr[3] - a_ptr[3] * x_ptr[2];
#endif

				a_ptr += 4;
				x_ptr += 4;
			}



			for( ; i < n; i++ )
			{
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
				temp_r += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
				temp_i += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
#else
				temp_r += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
				temp_i += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
#endif

				a_ptr += 2;
				x_ptr += 2;
			}


		}
		else
		{

			for( i = 0; i < n; i++ )
			{
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
				temp_r += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
				temp_i += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
#else
				temp_r += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
				temp_i += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
#endif

				a_ptr += lda;
				x_ptr += inc_x;
			}

		}
#if !defined(XCONJ) 
		y_ptr[0] += alpha_r * temp_r - alpha_i * temp_i;
		y_ptr[1] += alpha_r * temp_i + alpha_i * temp_r;
#else
		y_ptr[0] += alpha_r * temp_r + alpha_i * temp_i;
		y_ptr[1] -= alpha_r * temp_i - alpha_i * temp_r;
#endif
		return(0);
	}

	if ( m3 == 2 )
	{
		a_ptr = a;
		x_ptr = x;
		FLOAT temp_r0 = 0.0;
		FLOAT temp_i0 = 0.0;
		FLOAT temp_r1 = 0.0;
		FLOAT temp_i1 = 0.0;

		if ( lda == 4 && inc_x == 2 )
		{

			for( i = 0; i < (n & -2); i+=2 )
			{
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )

				temp_r0 += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
				temp_i0 += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
				temp_r1 += a_ptr[2] * x_ptr[0] - a_ptr[3] * x_ptr[1];
				temp_i1 += a_ptr[2] * x_ptr[1] + a_ptr[3] * x_ptr[0];

				temp_r0 += a_ptr[4] * x_ptr[2] - a_ptr[5] * x_ptr[3];
				temp_i0 += a_ptr[4] * x_ptr[3] + a_ptr[5] * x_ptr[2];
				temp_r1 += a_ptr[6] * x_ptr[2] - a_ptr[7] * x_ptr[3];
				temp_i1 += a_ptr[6] * x_ptr[3] + a_ptr[7] * x_ptr[2];

#else
				temp_r0 += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
				temp_i0 += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
				temp_r1 += a_ptr[2] * x_ptr[0] + a_ptr[3] * x_ptr[1];
				temp_i1 += a_ptr[2] * x_ptr[1] - a_ptr[3] * x_ptr[0];

				temp_r0 += a_ptr[4] * x_ptr[2] + a_ptr[5] * x_ptr[3];
				temp_i0 += a_ptr[4] * x_ptr[3] - a_ptr[5] * x_ptr[2];
				temp_r1 += a_ptr[6] * x_ptr[2] + a_ptr[7] * x_ptr[3];
				temp_i1 += a_ptr[6] * x_ptr[3] - a_ptr[7] * x_ptr[2];

#endif

				a_ptr += 8;
				x_ptr += 4;
			}


			for( ; i < n; i++ )
			{
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
				temp_r0 += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
				temp_i0 += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
				temp_r1 += a_ptr[2] * x_ptr[0] - a_ptr[3] * x_ptr[1];
				temp_i1 += a_ptr[2] * x_ptr[1] + a_ptr[3] * x_ptr[0];
#else
				temp_r0 += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
				temp_i0 += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
				temp_r1 += a_ptr[2] * x_ptr[0] + a_ptr[3] * x_ptr[1];
				temp_i1 += a_ptr[2] * x_ptr[1] - a_ptr[3] * x_ptr[0];
#endif

				a_ptr += 4;
				x_ptr += 2;
			}


		}
		else
		{

			for( i=0 ; i < n; i++ )
			{
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
				temp_r0 += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
				temp_i0 += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
				temp_r1 += a_ptr[2] * x_ptr[0] - a_ptr[3] * x_ptr[1];
				temp_i1 += a_ptr[2] * x_ptr[1] + a_ptr[3] * x_ptr[0];
#else
				temp_r0 += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
				temp_i0 += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
				temp_r1 += a_ptr[2] * x_ptr[0] + a_ptr[3] * x_ptr[1];
				temp_i1 += a_ptr[2] * x_ptr[1] - a_ptr[3] * x_ptr[0];
#endif

				a_ptr += lda;
				x_ptr += inc_x;
			}


		}
#if !defined(XCONJ) 
		y_ptr[0] += alpha_r * temp_r0 - alpha_i * temp_i0;
		y_ptr[1] += alpha_r * temp_i0 + alpha_i * temp_r0;
		y_ptr    += inc_y;
		y_ptr[0] += alpha_r * temp_r1 - alpha_i * temp_i1;
		y_ptr[1] += alpha_r * temp_i1 + alpha_i * temp_r1;
#else
		y_ptr[0] += alpha_r * temp_r0 + alpha_i * temp_i0;
		y_ptr[1] -= alpha_r * temp_i0 - alpha_i * temp_r0;
		y_ptr    += inc_y;
		y_ptr[0] += alpha_r * temp_r1 + alpha_i * temp_i1;
		y_ptr[1] -= alpha_r * temp_i1 - alpha_i * temp_r1;
#endif
		return(0);
	}


	if ( m3 == 3 )
	{
		a_ptr = a;
		x_ptr = x;
		FLOAT temp_r0 = 0.0;
		FLOAT temp_i0 = 0.0;
		FLOAT temp_r1 = 0.0;
		FLOAT temp_i1 = 0.0;
		FLOAT temp_r2 = 0.0;
		FLOAT temp_i2 = 0.0;

		if ( lda == 6 && inc_x == 2 )
		{

			for( i=0 ; i < n; i++ )
			{
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
				temp_r0 += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
				temp_i0 += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
				temp_r1 += a_ptr[2] * x_ptr[0] - a_ptr[3] * x_ptr[1];
				temp_i1 += a_ptr[2] * x_ptr[1] + a_ptr[3] * x_ptr[0];
				temp_r2 += a_ptr[4] * x_ptr[0] - a_ptr[5] * x_ptr[1];
				temp_i2 += a_ptr[4] * x_ptr[1] + a_ptr[5] * x_ptr[0];
#else
				temp_r0 += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
				temp_i0 += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
				temp_r1 += a_ptr[2] * x_ptr[0] + a_ptr[3] * x_ptr[1];
				temp_i1 += a_ptr[2] * x_ptr[1] - a_ptr[3] * x_ptr[0];
				temp_r2 += a_ptr[4] * x_ptr[0] + a_ptr[5] * x_ptr[1];
				temp_i2 += a_ptr[4] * x_ptr[1] - a_ptr[5] * x_ptr[0];
#endif

				a_ptr += 6;
				x_ptr += 2;
			}


		}
		else
		{

			for( i = 0; i < n; i++ )
			{
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
				temp_r0 += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
				temp_i0 += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
				temp_r1 += a_ptr[2] * x_ptr[0] - a_ptr[3] * x_ptr[1];
				temp_i1 += a_ptr[2] * x_ptr[1] + a_ptr[3] * x_ptr[0];
				temp_r2 += a_ptr[4] * x_ptr[0] - a_ptr[5] * x_ptr[1];
				temp_i2 += a_ptr[4] * x_ptr[1] + a_ptr[5] * x_ptr[0];
#else
				temp_r0 += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
				temp_i0 += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
				temp_r1 += a_ptr[2] * x_ptr[0] + a_ptr[3] * x_ptr[1];
				temp_i1 += a_ptr[2] * x_ptr[1] - a_ptr[3] * x_ptr[0];
				temp_r2 += a_ptr[4] * x_ptr[0] + a_ptr[5] * x_ptr[1];
				temp_i2 += a_ptr[4] * x_ptr[1] - a_ptr[5] * x_ptr[0];
#endif

				a_ptr += lda;
				x_ptr += inc_x;
			}

		}
#if !defined(XCONJ) 
		y_ptr[0] += alpha_r * temp_r0 - alpha_i * temp_i0;
		y_ptr[1] += alpha_r * temp_i0 + alpha_i * temp_r0;
		y_ptr    += inc_y;
		y_ptr[0] += alpha_r * temp_r1 - alpha_i * temp_i1;
		y_ptr[1] += alpha_r * temp_i1 + alpha_i * temp_r1;
		y_ptr    += inc_y;
		y_ptr[0] += alpha_r * temp_r2 - alpha_i * temp_i2;
		y_ptr[1] += alpha_r * temp_i2 + alpha_i * temp_r2;
#else
		y_ptr[0] += alpha_r * temp_r0 + alpha_i * temp_i0;
		y_ptr[1] -= alpha_r * temp_i0 - alpha_i * temp_r0;
		y_ptr    += inc_y;
		y_ptr[0] += alpha_r * temp_r1 + alpha_i * temp_i1;
		y_ptr[1] -= alpha_r * temp_i1 - alpha_i * temp_r1;
		y_ptr    += inc_y;
		y_ptr[0] += alpha_r * temp_r2 + alpha_i * temp_i2;
		y_ptr[1] -= alpha_r * temp_i2 - alpha_i * temp_r2;
#endif
		return(0);
	}





	return(0);
}


