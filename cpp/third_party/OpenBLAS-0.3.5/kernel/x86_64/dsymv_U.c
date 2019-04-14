/***************************************************************************
Copyright (c) 2013, The OpenBLAS Project
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
#include "dsymv_U_microk_bulldozer-2.c"
#elif defined(HASWELL) || defined(ZEN) || defined (SKYLAKEX)
#include "dsymv_U_microk_haswell-2.c"
#elif defined(SANDYBRIDGE)
#include "dsymv_U_microk_sandy-2.c"
#elif defined(NEHALEM)
#include "dsymv_U_microk_nehalem-2.c"
#endif

#ifndef HAVE_KERNEL_4x4

static void dsymv_kernel_4x4(BLASLONG n, FLOAT *a0, FLOAT *a1, FLOAT *a2, FLOAT *a3, FLOAT *xp, FLOAT *yp, FLOAT *temp1, FLOAT *temp2)
{
	FLOAT at0,at1,at2,at3;
	FLOAT x;
	FLOAT tmp2[4] = { 0.0, 0.0, 0.0, 0.0 };
	FLOAT tp0;
	FLOAT tp1;
	FLOAT tp2;
	FLOAT tp3;
	BLASLONG i;

	tp0 = temp1[0];
	tp1 = temp1[1];
	tp2 = temp1[2];
	tp3 = temp1[3];
	
	for (i=0; i<n; i++)
	{
		at0     = a0[i];
		at1     = a1[i];
		at2     = a2[i];
		at3     = a3[i];
		x       = xp[i];
		yp[i]   += tp0 * at0 + tp1 *at1 + tp2 * at2 + tp3 * at3;
		tmp2[0] += at0 * x;
		tmp2[1] += at1 * x;
		tmp2[2] += at2 * x;
		tmp2[3] += at3 * x;

	}

	temp2[0] += tmp2[0];
	temp2[1] += tmp2[1];
	temp2[2] += tmp2[2];
	temp2[3] += tmp2[3];
}

#endif


#ifndef HAVE_KERNEL_1x4

static void dsymv_kernel_1x4(BLASLONG from, BLASLONG to, FLOAT *a0, FLOAT *a1, FLOAT *a2, FLOAT *a3, FLOAT *xp, FLOAT *yp, FLOAT *temp1, FLOAT *temp2)
{
	FLOAT at0,at1,at2,at3;
	FLOAT x;
	FLOAT tmp2[4] = { 0.0, 0.0, 0.0, 0.0 };
	FLOAT tp0;
	FLOAT tp1;
	FLOAT tp2;
	FLOAT tp3;
	BLASLONG i;

	tp0 = temp1[0];
	tp1 = temp1[1];
	tp2 = temp1[2];
	tp3 = temp1[3];
	
	for (i=from; i<to; i++)
	{
		at0     = a0[i];
		at1     = a1[i];
		at2     = a2[i];
		at3     = a3[i];
		x       = xp[i];
		yp[i]   += tp0 * at0 + tp1 *at1 + tp2 * at2 + tp3 * at3;
		tmp2[0] += at0 * x;
		tmp2[1] += at1 * x;
		tmp2[2] += at2 * x;
		tmp2[3] += at3 * x;

	}

	temp2[0] += tmp2[0];
	temp2[1] += tmp2[1];
	temp2[2] += tmp2[2];
	temp2[3] += tmp2[3];
}

#endif


static void dsymv_kernel_8x1(BLASLONG n, FLOAT *a0, FLOAT *xp, FLOAT *yp, FLOAT *temp1, FLOAT *temp2)
{
	FLOAT at0,at1,at2,at3;
	FLOAT temp = 0.0;
	FLOAT t1   = *temp1;
	BLASLONG i;
	
	for (i=0; i<(n/4)*4; i+=4)
	{
		at0     = a0[i];
		at1     = a0[i+1];
		at2     = a0[i+2];
		at3     = a0[i+3];

		yp[i]   += t1    * at0;
		temp    += at0   * xp[i];
		yp[i+1] += t1    * at1;
		temp    += at1   * xp[i+1];

		yp[i+2] += t1    * at2;
		temp    += at2   * xp[i+2];
		yp[i+3] += t1    * at3;
		temp    += at3   * xp[i+3];

	}
	*temp2 = temp;
}

int CNAME(BLASLONG m, BLASLONG offset, FLOAT alpha, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
	BLASLONG i;
	BLASLONG ix,iy;
	BLASLONG jx,jy;
	BLASLONG j;
	BLASLONG j1;
	BLASLONG j2;
	BLASLONG m2;
	FLOAT temp1;
	FLOAT temp2;
	FLOAT *xp, *yp;
	FLOAT *a0,*a1,*a2,*a3;
	FLOAT tmp1[4];
	FLOAT tmp2[4];

#if 0
	if( m != offset )
		printf("Symv_U: m=%d offset=%d\n",m,offset);
#endif

	BLASLONG m1 = m - offset;
	BLASLONG mrange = m -m1;

	if ( (inc_x!=1) || (inc_y!=1) || (mrange<16) )
	{

		jx = m1 * inc_x;
		jy = m1 * inc_y;

		for (j=m1; j<m; j++)
		{
			temp1 = alpha * x[jx];
			temp2 = 0.0;
			iy = 0;
			ix = 0;
			for (i=0; i<j; i++)
			{
				y[iy] += temp1 * a[j*lda+i];
				temp2 += a[j*lda+i] * x[ix];
				ix += inc_x;
				iy += inc_y;
			
			}
			y[jy] += temp1 * a[j*lda+j] + alpha * temp2;
			jx    += inc_x;
			jy    += inc_y;
		}
		return(0);
	}

	xp = x;
	yp = y;

	m2 = m - ( mrange % 4 );

	for (j=m1; j<m2; j+=4)
	{
		tmp1[0] = alpha * xp[j];
		tmp1[1] = alpha * xp[j+1];
		tmp1[2] = alpha * xp[j+2];
		tmp1[3] = alpha * xp[j+3];
		tmp2[0] = 0.0;
		tmp2[1] = 0.0;
		tmp2[2] = 0.0;
		tmp2[3] = 0.0;
		a0    = &a[j*lda];
		a1    = a0+lda;
		a2    = a1+lda;
		a3    = a2+lda;
		j1 = (j/8)*8;		
		if ( j1 )
			dsymv_kernel_4x4(j1, a0, a1, a2, a3, xp, yp, tmp1, tmp2);
		if ( j1 < j )
			dsymv_kernel_1x4(j1, j,  a0, a1, a2, a3, xp, yp, tmp1, tmp2);

		j2 = 0;
		for ( j1 = j ; j1 < j+4 ; j1++ )
		{
			temp1 = tmp1[j2];
			temp2 = tmp2[j2];
			a0    = &a[j1*lda];
			for ( i=j ; i<j1; i++ )
			{
				yp[i] += temp1 * a0[i];	
				temp2 += a0[i] * xp[i];
				
			}
			y[j1] += temp1 * a0[j1] + alpha * temp2;
			j2++;

		}

	}

	for ( ; j<m; j++)
	{
		temp1 = alpha * xp[j];
		temp2 = 0.0;
		a0    = &a[j*lda];
		FLOAT at0;
		j1 = (j/8)*8;		

		if ( j1 )
			dsymv_kernel_8x1(j1, a0, xp, yp, &temp1, &temp2);

		for (i=j1 ; i<j; i++)
		{
			at0     = a0[i];
			yp[i] += temp1 * at0;
			temp2 += at0 * xp[i];
			
		}

		yp[j] += temp1 * a0[j] + alpha * temp2;
	}

	return(0);
	

}


