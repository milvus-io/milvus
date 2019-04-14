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
	FLOAT temp0 = 0.0;
	FLOAT temp1 = 0.0;
	FLOAT temp2 = 0.0;
	FLOAT temp3 = 0.0;

	for ( i=0; i< n; i+=4 )
	{
		temp0 += a0[i]*x[i] + a0[i+1]*x[i+1] + a0[i+2]*x[i+2] + a0[i+3]*x[i+3];		
		temp1 += a1[i]*x[i] + a1[i+1]*x[i+1] + a1[i+2]*x[i+2] + a1[i+3]*x[i+3];		
		temp2 += a2[i]*x[i] + a2[i+1]*x[i+1] + a2[i+2]*x[i+2] + a2[i+3]*x[i+3];		
		temp3 += a3[i]*x[i] + a3[i+1]*x[i+1] + a3[i+2]*x[i+2] + a3[i+3]*x[i+3];		
	}
	y[0] = temp0;
	y[1] = temp1;
	y[2] = temp2;
	y[3] = temp3;
}
	
#endif

static void sgemv_kernel_16x1(BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y)
{
	BLASLONG i;
	FLOAT *a0;
	a0 = ap;
	FLOAT temp = 0.0;

	for ( i=0; i< n; i+=4 )
	{
		temp += a0[i]*x[i] + a0[i+1]*x[i+1] + a0[i+2]*x[i+2] + a0[i+3]*x[i+3];		
	}
	*y = temp;
}
	
static void copy_x(BLASLONG n, FLOAT *src, FLOAT *dest, BLASLONG inc_src)
{
        BLASLONG i;
        for ( i=0; i<n; i++ )
        {
                *dest = *src;
                dest++;
                src += inc_src;
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
	FLOAT ybuffer[4],*xbuffer;

        if ( m < 1 ) return(0);
        if ( n < 1 ) return(0);

	xbuffer = buffer;
	
	n1 = n / 4 ;
	n2 = n % 4 ;
	
	m1 = m - ( m % 16 );
	m2 = (m % NBMAX) - (m % 16) ;
	

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
		copy_x(NB,x_ptr,xbuffer,inc_x);
		for( i = 0; i < n1 ; i++)
		{
			ap[0] = a_ptr;
			ap[1] = a_ptr + lda;
			ap[2] = ap[1] + lda;
			ap[3] = ap[2] + lda;
			sgemv_kernel_16x4(NB,ap,xbuffer,ybuffer);
			a_ptr += 4 * lda;
			*y_ptr += ybuffer[0]*alpha;
			y_ptr  += inc_y;
			*y_ptr += ybuffer[1]*alpha;
			y_ptr  += inc_y;
			*y_ptr += ybuffer[2]*alpha;
			y_ptr  += inc_y;
			*y_ptr += ybuffer[3]*alpha;
			y_ptr  += inc_y;
		}

		for( i = 0; i < n2 ; i++)
		{
			sgemv_kernel_16x1(NB,a_ptr,xbuffer,ybuffer);
			a_ptr += 1 * lda;
			*y_ptr += ybuffer[0]*alpha;
			y_ptr  += inc_y;

		}
		a += NB;
		x += NB * inc_x;	
	}

	BLASLONG m3 = m % 16;
	if ( m3 == 0 ) return(0);
	x_ptr = x;
	for ( i=0; i< m3; i++ )
	{
		xbuffer[i] = *x_ptr;
		x_ptr += inc_x;
	}
	j=0;
	a_ptr = a;
	y_ptr = y;
	while ( j < n)
	{
		FLOAT temp = 0.0;
		for( i = 0; i < m3; i++ )
		{
			temp += a_ptr[i] * xbuffer[i];
		}
		a_ptr += lda;
		y_ptr[0] += alpha * temp;
		y_ptr += inc_y;
		j++;
	}
	return(0);
}


