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
#include "ssymv_L_microk_bulldozer-2.c"
#elif defined(NEHALEM)
#include "ssymv_L_microk_nehalem-2.c"
#elif defined(HASWELL) || defined(ZEN) || defined (SKYLAKEX)
#include "ssymv_L_microk_haswell-2.c"
#elif defined(SANDYBRIDGE)
#include "ssymv_L_microk_sandy-2.c"
#endif


#ifndef HAVE_KERNEL_4x4

static void ssymv_kernel_4x4(BLASLONG from, BLASLONG to, FLOAT **ap, FLOAT *x, FLOAT *y, FLOAT *tmp1, FLOAT *temp2)
{
        FLOAT tmp2[4] = { 0.0, 0.0, 0.0, 0.0 };
        BLASLONG i;

        for (i=from; i<to; i+=4)
        {

		y[i]    += tmp1[0] * ap[0][i];
		tmp2[0] += ap[0][i] * x[i];
		y[i]    += tmp1[1] * ap[1][i];
		tmp2[1] += ap[1][i] * x[i];
		y[i]    += tmp1[2] * ap[2][i];
		tmp2[2] += ap[2][i] * x[i];
		y[i]    += tmp1[3] * ap[3][i];
		tmp2[3] += ap[3][i] * x[i];

		y[i+1]  += tmp1[0] * ap[0][i+1];
		tmp2[0] += ap[0][i+1] * x[i+1];
		y[i+1]  += tmp1[1] * ap[1][i+1];
		tmp2[1] += ap[1][i+1] * x[i+1];
		y[i+1]  += tmp1[2] * ap[2][i+1];
		tmp2[2] += ap[2][i+1] * x[i+1];
		y[i+1]  += tmp1[3] * ap[3][i+1];
		tmp2[3] += ap[3][i+1] * x[i+1];

		y[i+2]  += tmp1[0] * ap[0][i+2];
		tmp2[0] += ap[0][i+2] * x[i+2];
		y[i+2]  += tmp1[1] * ap[1][i+2];
		tmp2[1] += ap[1][i+2] * x[i+2];
		y[i+2]  += tmp1[2] * ap[2][i+2];
		tmp2[2] += ap[2][i+2] * x[i+2];
		y[i+2]  += tmp1[3] * ap[3][i+2];
		tmp2[3] += ap[3][i+2] * x[i+2];

		y[i+3]  += tmp1[0] * ap[0][i+3];
		tmp2[0] += ap[0][i+3] * x[i+3];
		y[i+3]  += tmp1[1] * ap[1][i+3];
		tmp2[1] += ap[1][i+3] * x[i+3];
		y[i+3]  += tmp1[2] * ap[2][i+3];
		tmp2[2] += ap[2][i+3] * x[i+3];
		y[i+3]  += tmp1[3] * ap[3][i+3];
		tmp2[3] += ap[3][i+3] * x[i+3];

        }

        temp2[0] += tmp2[0];
        temp2[1] += tmp2[1];
        temp2[2] += tmp2[2];
        temp2[3] += tmp2[3];
}

#endif




int CNAME(BLASLONG m, BLASLONG offset, FLOAT alpha, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer)
{
	BLASLONG i;
	BLASLONG ix,iy;
	BLASLONG jx,jy;
	BLASLONG j;
	FLOAT temp1;
	FLOAT temp2;
	FLOAT tmp1[4];
	FLOAT tmp2[4];
	FLOAT *ap[4];

#if 0
	if ( m != offset )
		printf("Symv_L: m=%d offset=%d\n",m,offset);
#endif


	if ( (inc_x != 1) || (inc_y != 1) )
	{

		jx = 0;
		jy = 0;

		for (j=0; j<offset; j++)
		{
			temp1 = alpha * x[jx];
			temp2 = 0.0;
			y[jy] += temp1 * a[j*lda+j];
			iy = jy;
			ix = jx;
			for (i=j+1; i<m; i++)
			{
				ix += inc_x;
				iy += inc_y;
				y[iy] += temp1 * a[j*lda+i];
				temp2 += a[j*lda+i] * x[ix];
			
			}
			y[jy] += alpha * temp2;
			jx    += inc_x;
			jy    += inc_y;
		}
		return(0);
	}

	BLASLONG offset1 = (offset/4)*4;

	for (j=0; j<offset1; j+=4)
	{
		tmp1[0] = alpha * x[j];
		tmp1[1] = alpha * x[j+1];
		tmp1[2] = alpha * x[j+2];
		tmp1[3] = alpha * x[j+3];
		tmp2[0] = 0.0;
		tmp2[1] = 0.0;
		tmp2[2] = 0.0;
		tmp2[3] = 0.0;
		ap[0]   = &a[j*lda];
		ap[1]   = ap[0] + lda;
		ap[2]   = ap[1] + lda;
		ap[3]   = ap[2] + lda;
		y[j]   += tmp1[0] * ap[0][j];
		y[j+1] += tmp1[1] * ap[1][j+1];
		y[j+2] += tmp1[2] * ap[2][j+2];
		y[j+3] += tmp1[3] * ap[3][j+3];
		BLASLONG from = j+1;
		if ( m - from >=12 )
		{
			BLASLONG m2 = (m/4)*4;
			for (i=j+1; i<j+4; i++)
			{
				y[i] += tmp1[0] * ap[0][i];
				tmp2[0] += ap[0][i] * x[i];
			}

			for (i=j+2; i<j+4; i++)
			{
				y[i] += tmp1[1] * ap[1][i];
				tmp2[1] += ap[1][i] * x[i];
			}

			for (i=j+3; i<j+4; i++)
			{
				y[i] += tmp1[2] * ap[2][i];
				tmp2[2] += ap[2][i] * x[i];
			}

			if ( m2 > j+4 )
				ssymv_kernel_4x4(j+4,m2,ap,x,y,tmp1,tmp2);


			for (i=m2; i<m; i++)
			{
				y[i] += tmp1[0] * ap[0][i];
				tmp2[0] += ap[0][i] * x[i];

				y[i] += tmp1[1] * ap[1][i];
				tmp2[1] += ap[1][i] * x[i];

				y[i] += tmp1[2] * ap[2][i];
				tmp2[2] += ap[2][i] * x[i];

				y[i] += tmp1[3] * ap[3][i];
				tmp2[3] += ap[3][i] * x[i];

			}


		}
		else
		{

			for (i=j+1; i<j+4; i++)
			{
				y[i] += tmp1[0] * ap[0][i];
				tmp2[0] += ap[0][i] * x[i];
			}

			for (i=j+2; i<j+4; i++)
			{
				y[i] += tmp1[1] * ap[1][i];
				tmp2[1] += ap[1][i] * x[i];
			}

			for (i=j+3; i<j+4; i++)
			{
				y[i] += tmp1[2] * ap[2][i];
				tmp2[2] += ap[2][i] * x[i];
			}

			for (i=j+4; i<m; i++)
			{
				y[i] += tmp1[0] * ap[0][i];
				tmp2[0] += ap[0][i] * x[i];

				y[i] += tmp1[1] * ap[1][i];
				tmp2[1] += ap[1][i] * x[i];

				y[i] += tmp1[2] * ap[2][i];
				tmp2[2] += ap[2][i] * x[i];

				y[i] += tmp1[3] * ap[3][i];
				tmp2[3] += ap[3][i] * x[i];

			}

		}
		y[j]   += alpha * tmp2[0];
		y[j+1] += alpha * tmp2[1];
		y[j+2] += alpha * tmp2[2];
		y[j+3] += alpha * tmp2[3];
	}


	for (j=offset1; j<offset; j++)
	{
		temp1 = alpha * x[j];
		temp2 = 0.0;
		y[j] += temp1 * a[j*lda+j];
		BLASLONG from = j+1;
		if ( m - from >=8 )
		{
			BLASLONG j1 = ((from + 4)/4)*4;
			BLASLONG j2 = (m/4)*4;
			for (i=from; i<j1; i++)
			{
				y[i] += temp1 * a[j*lda+i];
				temp2 += a[j*lda+i] * x[i];
			
			}

			for (i=j1; i<j2; i++)
			{
				y[i] += temp1 * a[j*lda+i];
				temp2 += a[j*lda+i] * x[i];
			
			}

			for (i=j2; i<m; i++)
			{
				y[i] += temp1 * a[j*lda+i];
				temp2 += a[j*lda+i] * x[i];
			
			}

		}
		else
		{
			for (i=from; i<m; i++)
			{
				y[i] += temp1 * a[j*lda+i];
				temp2 += a[j*lda+i] * x[i];
			
			}

		}
		y[j] += alpha * temp2;
	}
	return(0);
}


