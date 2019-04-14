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


#define NBMAX 4096

#ifndef HAVE_KERNEL_16x4

static void sgemv_kernel_16x4(BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y)
{
	BLASLONG i;
	FLOAT *a0,*a1,*a2,*a3;
	a0 = ap[0];
	a1 = ap[1];
	a2 = ap[2];
	a3 = ap[3];

	for ( i=0; i< n; i+=4 )
	{
		y[i] += a0[i]*x[0] + a1[i]*x[1] + a2[i]*x[2] + a3[i]*x[3];		
		y[i+1] += a0[i+1]*x[0] + a1[i+1]*x[1] + a2[i+1]*x[2] + a3[i+1]*x[3];		
		y[i+2] += a0[i+2]*x[0] + a1[i+2]*x[1] + a2[i+2]*x[2] + a3[i+2]*x[3];		
		y[i+3] += a0[i+3]*x[0] + a1[i+3]*x[1] + a2[i+3]*x[2] + a3[i+3]*x[3];		
	}
}
	
#endif

static void sgemv_kernel_16x1(BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y)
{
	BLASLONG i;
	FLOAT *a0;
	a0 = ap;

	for ( i=0; i< n; i+=4 )
	{
		y[i] += a0[i]*x[0];		
		y[i+1] += a0[i+1]*x[0];		
		y[i+2] += a0[i+2]*x[0];		
		y[i+3] += a0[i+3]*x[0];		
	}
}
	

static void zero_y(BLASLONG n, FLOAT *dest)
{
	BLASLONG i;
	for ( i=0; i<n; i++ )
	{
		*dest = 0.0;
		dest++;
	}
}



static void add_y(BLASLONG n, FLOAT *src, FLOAT *dest, BLASLONG inc_dest)
{
	BLASLONG i;
	if ( inc_dest == 1 )
	{
		for ( i=0; i<n; i+=4 )
		{
			dest[i] += src[i];
			dest[i+1] += src[i+1];
			dest[i+2] += src[i+2];
			dest[i+3] += src[i+3];
		}

	}
	else
	{
		for ( i=0; i<n; i++ )
		{
			*dest += *src;
			src++;
			dest += inc_dest;
		}
	}
}

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
	BLASLONG i;
	BLASLONG j;
	FLOAT *a_ptr;
	FLOAT *x_ptr;
	FLOAT *y_ptr;
	FLOAT *ap[4];
	BLASLONG n1;
	BLASLONG m1;
	BLASLONG m2;
	BLASLONG n2;
	FLOAT xbuffer[4],*ybuffer;

        if ( m < 1 ) return(0);
        if ( n < 1 ) return(0);

	ybuffer = buffer;
	
	n1 = n / 4 ;
	n2 = n % 4 ;
	
	m1 = m - ( m % 16 );
	m2 = (m % NBMAX) - (m % 16) ;
	
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
		zero_y(NB,ybuffer);
		for( i = 0; i < n1 ; i++)
		{
			xbuffer[0] = alpha * x_ptr[0];
			x_ptr += inc_x;	
			xbuffer[1] = alpha * x_ptr[0];
			x_ptr += inc_x;	
			xbuffer[2] = alpha * x_ptr[0];
			x_ptr += inc_x;	
			xbuffer[3] = alpha * x_ptr[0];
			x_ptr += inc_x;	
			ap[0] = a_ptr;
			ap[1] = a_ptr + lda;
			ap[2] = ap[1] + lda;
			ap[3] = ap[2] + lda;
			sgemv_kernel_16x4(NB,ap,xbuffer,ybuffer);
			a_ptr += 4 * lda;
		}

		for( i = 0; i < n2 ; i++)
		{
			xbuffer[0] = alpha * x_ptr[0];
			x_ptr += inc_x;	
			sgemv_kernel_16x1(NB,a_ptr,xbuffer,ybuffer);
			a_ptr += 1 * lda;

		}
		add_y(NB,ybuffer,y_ptr,inc_y);
		a     += NB;
		y_ptr += NB * inc_y;
	}
	j=0;
	while ( j < (m % 16))
	{
		a_ptr = a;
		x_ptr = x;
		FLOAT temp = 0.0;
		for( i = 0; i < n; i++ )
		{
			temp += a_ptr[0] * x_ptr[0];
			a_ptr += lda;
			x_ptr += inc_x;
		}
		y_ptr[0] += alpha * temp;
		y_ptr += inc_y;
		a++;
		j++;
	}
	return(0);
}


