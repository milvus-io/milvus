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

#if defined(HASWELL) || defined(ZEN) || defined (SKYLAKEX)
#include "cgemv_t_microk_haswell-4.c"
#elif defined(BULLDOZER) || defined(PILEDRIVER) || defined(STEAMROLLER)  || defined(EXCAVATOR)
#include "cgemv_t_microk_bulldozer-4.c"
#endif

#define NBMAX 2048

#ifndef HAVE_KERNEL_4x4

static void cgemv_kernel_4x4(BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y, FLOAT *alpha)
{
	BLASLONG i;
	FLOAT *a0,*a1,*a2,*a3;
	a0 = ap[0];
	a1 = ap[1];
	a2 = ap[2];
	a3 = ap[3];
	FLOAT alpha_r = alpha[0];
	FLOAT alpha_i = alpha[1];
	FLOAT temp_r0 = 0.0;
	FLOAT temp_r1 = 0.0;
	FLOAT temp_r2 = 0.0;
	FLOAT temp_r3 = 0.0;
	FLOAT temp_i0 = 0.0;
	FLOAT temp_i1 = 0.0;
	FLOAT temp_i2 = 0.0;
	FLOAT temp_i3 = 0.0;


	for ( i=0; i< 2*n; i+=2 )
	{
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
		temp_r0 += a0[i]*x[i]   - a0[i+1]*x[i+1];		
		temp_i0 += a0[i]*x[i+1] + a0[i+1]*x[i];		
		temp_r1 += a1[i]*x[i]   - a1[i+1]*x[i+1];		
		temp_i1 += a1[i]*x[i+1] + a1[i+1]*x[i];		
		temp_r2 += a2[i]*x[i]   - a2[i+1]*x[i+1];		
		temp_i2 += a2[i]*x[i+1] + a2[i+1]*x[i];		
		temp_r3 += a3[i]*x[i]   - a3[i+1]*x[i+1];		
		temp_i3 += a3[i]*x[i+1] + a3[i+1]*x[i];		
#else
		temp_r0 += a0[i]*x[i]   + a0[i+1]*x[i+1];		
		temp_i0 += a0[i]*x[i+1] - a0[i+1]*x[i];		
		temp_r1 += a1[i]*x[i]   + a1[i+1]*x[i+1];		
		temp_i1 += a1[i]*x[i+1] - a1[i+1]*x[i];		
		temp_r2 += a2[i]*x[i]   + a2[i+1]*x[i+1];		
		temp_i2 += a2[i]*x[i+1] - a2[i+1]*x[i];		
		temp_r3 += a3[i]*x[i]   + a3[i+1]*x[i+1];		
		temp_i3 += a3[i]*x[i+1] - a3[i+1]*x[i];		
#endif
	}

#if !defined(XCONJ)

	y[0] +=  alpha_r * temp_r0 - alpha_i * temp_i0;
	y[1] +=  alpha_r * temp_i0 + alpha_i * temp_r0;
	y[2] +=  alpha_r * temp_r1 - alpha_i * temp_i1;
	y[3] +=  alpha_r * temp_i1 + alpha_i * temp_r1;
	y[4] +=  alpha_r * temp_r2 - alpha_i * temp_i2;
	y[5] +=  alpha_r * temp_i2 + alpha_i * temp_r2;
	y[6] +=  alpha_r * temp_r3 - alpha_i * temp_i3;
	y[7] +=  alpha_r * temp_i3 + alpha_i * temp_r3;

#else

	y[0] +=  alpha_r * temp_r0 + alpha_i * temp_i0;
	y[1] -=  alpha_r * temp_i0 - alpha_i * temp_r0;
	y[2] +=  alpha_r * temp_r1 + alpha_i * temp_i1;
	y[3] -=  alpha_r * temp_i1 - alpha_i * temp_r1;
	y[4] +=  alpha_r * temp_r2 + alpha_i * temp_i2;
	y[5] -=  alpha_r * temp_i2 - alpha_i * temp_r2;
	y[6] +=  alpha_r * temp_r3 + alpha_i * temp_i3;
	y[7] -=  alpha_r * temp_i3 - alpha_i * temp_r3;

#endif
}
	
#endif

#ifndef HAVE_KERNEL_4x2

static void cgemv_kernel_4x2(BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y, FLOAT *alpha)
{
	BLASLONG i;
	FLOAT *a0,*a1;
	a0 = ap[0];
	a1 = ap[1];
	FLOAT alpha_r = alpha[0];
	FLOAT alpha_i = alpha[1];
	FLOAT temp_r0 = 0.0;
	FLOAT temp_r1 = 0.0;
	FLOAT temp_i0 = 0.0;
	FLOAT temp_i1 = 0.0;


	for ( i=0; i< 2*n; i+=2 )
	{
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
		temp_r0 += a0[i]*x[i]   - a0[i+1]*x[i+1];		
		temp_i0 += a0[i]*x[i+1] + a0[i+1]*x[i];		
		temp_r1 += a1[i]*x[i]   - a1[i+1]*x[i+1];		
		temp_i1 += a1[i]*x[i+1] + a1[i+1]*x[i];		
#else
		temp_r0 += a0[i]*x[i]   + a0[i+1]*x[i+1];		
		temp_i0 += a0[i]*x[i+1] - a0[i+1]*x[i];		
		temp_r1 += a1[i]*x[i]   + a1[i+1]*x[i+1];		
		temp_i1 += a1[i]*x[i+1] - a1[i+1]*x[i];		
#endif
	}

#if !defined(XCONJ)

	y[0] +=  alpha_r * temp_r0 - alpha_i * temp_i0;
	y[1] +=  alpha_r * temp_i0 + alpha_i * temp_r0;
	y[2] +=  alpha_r * temp_r1 - alpha_i * temp_i1;
	y[3] +=  alpha_r * temp_i1 + alpha_i * temp_r1;

#else

	y[0] +=  alpha_r * temp_r0 + alpha_i * temp_i0;
	y[1] -=  alpha_r * temp_i0 - alpha_i * temp_r0;
	y[2] +=  alpha_r * temp_r1 + alpha_i * temp_i1;
	y[3] -=  alpha_r * temp_i1 - alpha_i * temp_r1;

#endif
}
	
#endif


#ifndef HAVE_KERNEL_4x1

static void cgemv_kernel_4x1(BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT *alpha)
{
	BLASLONG i;
	FLOAT *a0;
	a0 = ap;
	FLOAT alpha_r = alpha[0];
	FLOAT alpha_i = alpha[1];
	FLOAT temp_r0 = 0.0;
	FLOAT temp_i0 = 0.0;

	for ( i=0; i< 2*n; i+=2 )
	{
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
		temp_r0 += a0[i]*x[i]   - a0[i+1]*x[i+1];		
		temp_i0 += a0[i]*x[i+1] + a0[i+1]*x[i];		
#else
		temp_r0 += a0[i]*x[i]   + a0[i+1]*x[i+1];		
		temp_i0 += a0[i]*x[i+1] - a0[i+1]*x[i];		
#endif
	}

#if !defined(XCONJ)

	y[0] +=  alpha_r * temp_r0 - alpha_i * temp_i0;
	y[1] +=  alpha_r * temp_i0 + alpha_i * temp_r0;

#else

	y[0] +=  alpha_r * temp_r0 + alpha_i * temp_i0;
	y[1] -=  alpha_r * temp_i0 - alpha_i * temp_r0;

#endif


}

#endif


static void copy_x(BLASLONG n, FLOAT *src, FLOAT *dest, BLASLONG inc_src)
{
        BLASLONG i;
        for ( i=0; i<n; i++ )
        {
                *dest     = *src;
                *(dest+1) = *(src+1);
                dest+=2;
                src += inc_src;
        }
}


int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha_r, FLOAT alpha_i, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
	BLASLONG i;
	BLASLONG j;
	FLOAT *a_ptr;
	FLOAT *x_ptr;
	FLOAT *y_ptr;
	FLOAT *ap[8];
	BLASLONG n1;
	BLASLONG m1;
	BLASLONG m2;
	BLASLONG m3;
	BLASLONG n2;
	BLASLONG lda4;
	FLOAT ybuffer[8],*xbuffer;
	FLOAT alpha[2];

        if ( m < 1 ) return(0);
        if ( n < 1 ) return(0);

        inc_x <<= 1;
        inc_y <<= 1;
        lda   <<= 1;
	lda4    = lda << 2;

	xbuffer = buffer;
	
	n1 = n  >> 2 ;
	n2 = n  &  3 ;
	
	m3 = m & 3 ;
	m1 = m - m3;
	m2 = (m & (NBMAX-1)) - m3 ;
	
	alpha[0] = alpha_r;
	alpha[1] = alpha_i;

	BLASLONG NB = NBMAX;

	while ( NB == NBMAX )
	{
		
		m1 -= NB;
		if ( m1 < 0)
		{
			if ( m2 == 0 ) break;	
			NB = m2;
		}
		
		y_ptr = y;
		a_ptr = a;
		x_ptr = x;
		ap[0] = a_ptr;
		ap[1] = a_ptr + lda;
		ap[2] = ap[1] + lda;
		ap[3] = ap[2] + lda;
		if ( inc_x != 2 )
			copy_x(NB,x_ptr,xbuffer,inc_x);
		else
			xbuffer = x_ptr;
		
		if ( inc_y == 2 )
		{

			for( i = 0; i < n1 ; i++)
			{
				cgemv_kernel_4x4(NB,ap,xbuffer,y_ptr,alpha);
				ap[0] += lda4;
				ap[1] += lda4;
				ap[2] += lda4;
				ap[3] += lda4;
				a_ptr += lda4;
				y_ptr += 8;
				
			}

			if ( n2 & 2 )
			{
				cgemv_kernel_4x2(NB,ap,xbuffer,y_ptr,alpha);
				a_ptr += lda * 2;
				y_ptr += 4;

			}

			if ( n2 & 1 )
			{
				cgemv_kernel_4x1(NB,a_ptr,xbuffer,y_ptr,alpha);
				/* a_ptr += lda;
				y_ptr += 2; */

			}

		}
		else
		{

			for( i = 0; i < n1 ; i++)
			{
				memset(ybuffer,0,32);
				cgemv_kernel_4x4(NB,ap,xbuffer,ybuffer,alpha);
				ap[0] += lda4;
				ap[1] += lda4;
				ap[2] += lda4;
				ap[3] += lda4;
				a_ptr += lda4;

				y_ptr[0] += ybuffer[0];
				y_ptr[1] += ybuffer[1];
				y_ptr  += inc_y;
				y_ptr[0] += ybuffer[2];
				y_ptr[1] += ybuffer[3];
				y_ptr  += inc_y;
				y_ptr[0] += ybuffer[4];
				y_ptr[1] += ybuffer[5];
				y_ptr  += inc_y;
				y_ptr[0] += ybuffer[6];
				y_ptr[1] += ybuffer[7];
				y_ptr  += inc_y;

			}

			for( i = 0; i < n2 ; i++)
			{
				memset(ybuffer,0,32);
				cgemv_kernel_4x1(NB,a_ptr,xbuffer,ybuffer,alpha);
				a_ptr += lda;
				y_ptr[0] += ybuffer[0];
				y_ptr[1] += ybuffer[1];
				y_ptr  += inc_y;

			}

		}
		a += 2 * NB;
		x += NB * inc_x;	
	}



	if ( m3 == 0 ) return(0);

        x_ptr = x;
        j=0;
        a_ptr = a;
        y_ptr = y;

	if ( m3 == 3 )
	{

                FLOAT temp_r ;
                FLOAT temp_i ;
		FLOAT x0 = x_ptr[0];
		FLOAT x1 = x_ptr[1];
		x_ptr += inc_x;
		FLOAT x2 = x_ptr[0];
		FLOAT x3 = x_ptr[1];
		x_ptr += inc_x;
		FLOAT x4 = x_ptr[0];
		FLOAT x5 = x_ptr[1];
	        while ( j < n)
        	{
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
                       	temp_r  = a_ptr[0] * x0 - a_ptr[1] * x1; 
                       	temp_i  = a_ptr[0] * x1 + a_ptr[1] * x0; 
                       	temp_r += a_ptr[2] * x2 - a_ptr[3] * x3; 
                       	temp_i += a_ptr[2] * x3 + a_ptr[3] * x2; 
                       	temp_r += a_ptr[4] * x4 - a_ptr[5] * x5;
                       	temp_i += a_ptr[4] * x5 + a_ptr[5] * x4;
#else

                       	temp_r  = a_ptr[0] * x0 + a_ptr[1] * x1; 
                       	temp_i  = a_ptr[0] * x1 - a_ptr[1] * x0; 
                       	temp_r += a_ptr[2] * x2 + a_ptr[3] * x3; 
                       	temp_i += a_ptr[2] * x3 - a_ptr[3] * x2; 
                       	temp_r += a_ptr[4] * x4 + a_ptr[5] * x5;
                       	temp_i += a_ptr[4] * x5 - a_ptr[5] * x4;
#endif

#if !defined(XCONJ) 
                	y_ptr[0] += alpha_r * temp_r - alpha_i * temp_i;
                	y_ptr[1] += alpha_r * temp_i + alpha_i * temp_r;
#else
                	y_ptr[0] += alpha_r * temp_r + alpha_i * temp_i;
                	y_ptr[1] -= alpha_r * temp_i - alpha_i * temp_r;
#endif

                	a_ptr += lda;
                	y_ptr += inc_y;
                	j++;
        	}
        	return(0);
	}


	if ( m3 == 2 )
	{

                FLOAT temp_r ;
                FLOAT temp_i ;
                FLOAT temp_r1 ;
                FLOAT temp_i1 ;
		FLOAT x0 = x_ptr[0];
		FLOAT x1 = x_ptr[1];
		x_ptr += inc_x;
		FLOAT x2 = x_ptr[0];
		FLOAT x3 = x_ptr[1];
		FLOAT ar = alpha[0];
		FLOAT ai = alpha[1];

	        while ( j < ( n & -2 ))
        	{
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
                       	temp_r  = a_ptr[0] * x0 - a_ptr[1] * x1; 
                       	temp_i  = a_ptr[0] * x1 + a_ptr[1] * x0; 
                       	temp_r += a_ptr[2] * x2 - a_ptr[3] * x3; 
                       	temp_i += a_ptr[2] * x3 + a_ptr[3] * x2; 
                	a_ptr += lda;
                       	temp_r1  = a_ptr[0] * x0 - a_ptr[1] * x1; 
                       	temp_i1  = a_ptr[0] * x1 + a_ptr[1] * x0; 
                       	temp_r1 += a_ptr[2] * x2 - a_ptr[3] * x3; 
                       	temp_i1 += a_ptr[2] * x3 + a_ptr[3] * x2; 
#else

                       	temp_r  = a_ptr[0] * x0 + a_ptr[1] * x1; 
                       	temp_i  = a_ptr[0] * x1 - a_ptr[1] * x0; 
                       	temp_r += a_ptr[2] * x2 + a_ptr[3] * x3; 
                       	temp_i += a_ptr[2] * x3 - a_ptr[3] * x2; 
                	a_ptr += lda;
                       	temp_r1  = a_ptr[0] * x0 + a_ptr[1] * x1; 
                       	temp_i1  = a_ptr[0] * x1 - a_ptr[1] * x0; 
                       	temp_r1 += a_ptr[2] * x2 + a_ptr[3] * x3; 
                       	temp_i1 += a_ptr[2] * x3 - a_ptr[3] * x2; 
#endif

#if !defined(XCONJ) 
                	y_ptr[0] += ar * temp_r - ai * temp_i;
                	y_ptr[1] += ar * temp_i + ai * temp_r;
                	y_ptr += inc_y;
                	y_ptr[0] += ar * temp_r1 - ai * temp_i1;
                	y_ptr[1] += ar * temp_i1 + ai * temp_r1;
#else
                	y_ptr[0] += ar * temp_r + ai * temp_i;
                	y_ptr[1] -= ar * temp_i - ai * temp_r;
                	y_ptr += inc_y;
                	y_ptr[0] += ar * temp_r1 + ai * temp_i1;
                	y_ptr[1] -= ar * temp_i1 - ai * temp_r1;
#endif

                	a_ptr += lda;
                	y_ptr += inc_y;
                	j+=2;
        	}


	        while ( j < n)
        	{
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
                       	temp_r  = a_ptr[0] * x0 - a_ptr[1] * x1; 
                       	temp_i  = a_ptr[0] * x1 + a_ptr[1] * x0; 
                       	temp_r += a_ptr[2] * x2 - a_ptr[3] * x3; 
                       	temp_i += a_ptr[2] * x3 + a_ptr[3] * x2; 
#else

                       	temp_r  = a_ptr[0] * x0 + a_ptr[1] * x1; 
                       	temp_i  = a_ptr[0] * x1 - a_ptr[1] * x0; 
                       	temp_r += a_ptr[2] * x2 + a_ptr[3] * x3; 
                       	temp_i += a_ptr[2] * x3 - a_ptr[3] * x2; 
#endif

#if !defined(XCONJ) 
                	y_ptr[0] += ar * temp_r - ai * temp_i;
                	y_ptr[1] += ar * temp_i + ai * temp_r;
#else
                	y_ptr[0] += ar * temp_r + ai * temp_i;
                	y_ptr[1] -= ar * temp_i - ai * temp_r;
#endif

                	a_ptr += lda;
                	y_ptr += inc_y;
                	j++;
        	}

        	return(0);
	}


	if ( m3 == 1 )
	{

                FLOAT temp_r ;
                FLOAT temp_i ;
                FLOAT temp_r1 ;
                FLOAT temp_i1 ;
		FLOAT x0 = x_ptr[0];
		FLOAT x1 = x_ptr[1];
		FLOAT ar = alpha[0];
		FLOAT ai = alpha[1];

	        while ( j < ( n & -2 ))
        	{
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
                       	temp_r  = a_ptr[0] * x0 - a_ptr[1] * x1; 
                       	temp_i  = a_ptr[0] * x1 + a_ptr[1] * x0; 
                	a_ptr += lda;
                       	temp_r1  = a_ptr[0] * x0 - a_ptr[1] * x1; 
                       	temp_i1  = a_ptr[0] * x1 + a_ptr[1] * x0; 
#else

                       	temp_r  = a_ptr[0] * x0 + a_ptr[1] * x1; 
                       	temp_i  = a_ptr[0] * x1 - a_ptr[1] * x0; 
                	a_ptr += lda;
                       	temp_r1  = a_ptr[0] * x0 + a_ptr[1] * x1; 
                       	temp_i1  = a_ptr[0] * x1 - a_ptr[1] * x0; 
#endif

#if !defined(XCONJ) 
                	y_ptr[0] += ar * temp_r - ai * temp_i;
                	y_ptr[1] += ar * temp_i + ai * temp_r;
                	y_ptr += inc_y;
                	y_ptr[0] += ar * temp_r1 - ai * temp_i1;
                	y_ptr[1] += ar * temp_i1 + ai * temp_r1;
#else
                	y_ptr[0] += ar * temp_r + ai * temp_i;
                	y_ptr[1] -= ar * temp_i - ai * temp_r;
                	y_ptr += inc_y;
                	y_ptr[0] += ar * temp_r1 + ai * temp_i1;
                	y_ptr[1] -= ar * temp_i1 - ai * temp_r1;
#endif

                	a_ptr += lda;
                	y_ptr += inc_y;
                	j+=2;
        	}

	        while ( j < n)
        	{
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
                       	temp_r  = a_ptr[0] * x0 - a_ptr[1] * x1; 
                       	temp_i  = a_ptr[0] * x1 + a_ptr[1] * x0; 
#else

                       	temp_r  = a_ptr[0] * x0 + a_ptr[1] * x1; 
                       	temp_i  = a_ptr[0] * x1 - a_ptr[1] * x0; 
#endif

#if !defined(XCONJ) 
                	y_ptr[0] += ar * temp_r - ai * temp_i;
                	y_ptr[1] += ar * temp_i + ai * temp_r;
#else
                	y_ptr[0] += ar * temp_r + ai * temp_i;
                	y_ptr[1] -= ar * temp_i - ai * temp_r;
#endif

                	a_ptr += lda;
                	y_ptr += inc_y;
                	j++;
        	}
        	return(0);
	}

	return(0);


}


