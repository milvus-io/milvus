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


#if defined(HASWELL) || defined(ZEN) || defined (SKYLAKEX)
#include "cscal_microk_haswell-2.c"
#elif defined(BULLDOZER)  || defined(PILEDRIVER)
#include "cscal_microk_bulldozer-2.c"
#elif defined(STEAMROLLER)  || defined(EXCAVATOR)
#include "cscal_microk_steamroller-2.c"
#elif defined(SANDYBRIDGE)
#include "cscal_microk_bulldozer-2.c"
#endif


#if !defined(HAVE_KERNEL_16)

static void cscal_kernel_16( BLASLONG n, FLOAT *alpha , FLOAT *x )  __attribute__ ((noinline));
static void cscal_kernel_16_zero( BLASLONG n, FLOAT *alpha , FLOAT *x )  __attribute__ ((noinline));
static void cscal_kernel_16_zero_r( BLASLONG n, FLOAT *alpha , FLOAT *x )  __attribute__ ((noinline));
static void cscal_kernel_16_zero_i( BLASLONG n, FLOAT *alpha , FLOAT *x )  __attribute__ ((noinline));

static void cscal_kernel_16( BLASLONG n, FLOAT *alpha , FLOAT *x )
{

	BLASLONG i;
	FLOAT da_r = alpha[0];
	FLOAT da_i = alpha[1];
	FLOAT t0,t1,t2,t3;

	for( i=0; i<n; i+=4 )
	{
		t0 = da_r *x[0] - da_i *x[1];	
		t1 = da_r *x[2] - da_i *x[3];	
		t2 = da_r *x[4] - da_i *x[5];	
		t3 = da_r *x[6] - da_i *x[7];	

		x[1] = da_r * x[1] + da_i * x[0];
		x[3] = da_r * x[3] + da_i * x[2];
		x[5] = da_r * x[5] + da_i * x[4];
		x[7] = da_r * x[7] + da_i * x[6];
		
		x[0] = t0;
		x[2] = t1;
		x[4] = t2;
		x[6] = t3;

		x+=8;
	}

}


static void cscal_kernel_16_zero_r( BLASLONG n, FLOAT *alpha , FLOAT *x )
{

	BLASLONG i;
	FLOAT da_i = alpha[1];
	FLOAT t0,t1,t2,t3;

	for( i=0; i<n; i+=4 )
	{
		t0 =  - da_i *x[1];	
		t1 =  - da_i *x[3];	
		t2 =  - da_i *x[5];	
		t3 =  - da_i *x[7];	

		x[1] =  da_i * x[0];
		x[3] =  da_i * x[2];
		x[5] =  da_i * x[4];
		x[7] =  da_i * x[6];
		
		x[0] = t0;
		x[2] = t1;
		x[4] = t2;
		x[6] = t3;

		x+=8;
	}

}


static void cscal_kernel_16_zero_i( BLASLONG n, FLOAT *alpha , FLOAT *x )
{

	BLASLONG i;
	FLOAT da_r = alpha[0];
	FLOAT t0,t1,t2,t3;

	for( i=0; i<n; i+=4 )
	{
		t0 = da_r *x[0];	
		t1 = da_r *x[2];	
		t2 = da_r *x[4];	
		t3 = da_r *x[6];	

		x[1] = da_r * x[1];
		x[3] = da_r * x[3];
		x[5] = da_r * x[5];
		x[7] = da_r * x[7];
		
		x[0] = t0;
		x[2] = t1;
		x[4] = t2;
		x[6] = t3;

		x+=8;
	}

}



static void cscal_kernel_16_zero( BLASLONG n, FLOAT *alpha , FLOAT *x )
{

	BLASLONG i;
	for( i=0; i<n; i+=4 )
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

static void cscal_kernel_inc_8(BLASLONG n, FLOAT *alpha, FLOAT *x, BLASLONG inc_x)  __attribute__ ((noinline));

static void cscal_kernel_inc_8(BLASLONG n, FLOAT *alpha, FLOAT *x, BLASLONG inc_x)
{

	BLASLONG i;
	BLASLONG inc_x2 = 2 * inc_x;
	BLASLONG inc_x3 = inc_x2 + inc_x;
	FLOAT t0,t1,t2,t3;
	FLOAT da_r = alpha[0];
	FLOAT da_i = alpha[1];

	for ( i=0; i<n; i+=4 )
	{
		t0 = da_r * x[0]         - da_i *x[1];	
		t1 = da_r * x[inc_x]     - da_i *x[inc_x  + 1];	
		t2 = da_r * x[inc_x2]    - da_i *x[inc_x2 + 1];	
		t3 = da_r * x[inc_x3]    - da_i *x[inc_x3 + 1];	

		x[1]               = da_i * x[0]       + da_r * x[1];
		x[inc_x  +1]       = da_i * x[inc_x]   + da_r * x[inc_x  +1];
		x[inc_x2 +1]       = da_i * x[inc_x2]  + da_r * x[inc_x2 +1];
		x[inc_x3 +1]       = da_i * x[inc_x3]  + da_r * x[inc_x3 +1];

		x[0]        = t0;
		x[inc_x]    = t1;
		x[inc_x2]   = t2;
		x[inc_x3]   = t3;

		x+=4*inc_x;

	}


}


int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da_r, FLOAT da_i, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
{
	BLASLONG i=0,j=0;
	FLOAT temp0;
	FLOAT temp1;
	FLOAT alpha[2];

	if ( inc_x != 1 )
	{
		inc_x <<= 1;

		if ( da_r == 0.0 )
		{

			BLASLONG n1 = n & -2;

			if ( da_i == 0.0 )
			{

				while(j < n1)
				{
			
					x[i]=0.0;
					x[i+1]=0.0;
					x[i+inc_x]=0.0;
					x[i+1+inc_x]=0.0;
					i += 2*inc_x ;
					j+=2;

				}

				while(j < n)
				{
			
					x[i]=0.0;
					x[i+1]=0.0;
					i += inc_x ;
					j++;

				}

			}
			else
			{

				while(j < n1)
				{
			
					temp0        = -da_i * x[i+1];
					x[i+1]       =  da_i * x[i];
					x[i]         =  temp0;
					temp1        = -da_i * x[i+1+inc_x];
					x[i+1+inc_x] =  da_i * x[i+inc_x];
					x[i+inc_x]   =  temp1;
					i += 2*inc_x ;
					j+=2;

				}

				while(j < n)
				{
			
					temp0        = -da_i * x[i+1];
					x[i+1]       =  da_i * x[i];
					x[i]         =  temp0;
					i += inc_x ;
					j++;

				}



			}

		}
		else
		{


			if ( da_i == 0.0 )
			{
				BLASLONG n1 = n & -2;

				while(j < n1)
				{
			
					temp0        =  da_r * x[i];
					x[i+1]       =  da_r * x[i+1];
					x[i]         =  temp0;
					temp1        =  da_r * x[i+inc_x];
					x[i+1+inc_x] =  da_r * x[i+1+inc_x];
					x[i+inc_x]   =  temp1;
					i += 2*inc_x ;
					j+=2;

				}

				while(j < n)
				{
			
					temp0        =  da_r * x[i];
					x[i+1]       =  da_r * x[i+1];
					x[i]         =  temp0;
					i += inc_x ;
					j++;

				}

			}
			else
			{

				BLASLONG n1 = n & -8;
				if ( n1 > 0 )
				{
					alpha[0] = da_r;
			                alpha[1] = da_i;
					cscal_kernel_inc_8(n1, alpha, x, inc_x);
					j = n1 ;
					i = n1 * inc_x;
				}

				while(j < n)
				{

					temp0        =  da_r * x[i]   - da_i * x[i+1];
					x[i+1]       =  da_r * x[i+1] + da_i * x[i];
					x[i]         =  temp0;
					i += inc_x ;
					j++;

				}

			}

		}

		return(0);
	}


	BLASLONG n1 = n & -16;
	if ( n1 > 0 )
	{

		alpha[0] = da_r;
		alpha[1] = da_i;
	
		if ( da_r == 0.0 )
			if ( da_i == 0 )
				cscal_kernel_16_zero(n1 , alpha , x);
			else
				cscal_kernel_16_zero_r(n1 , alpha , x);
		else
			if ( da_i == 0 )
				cscal_kernel_16_zero_i(n1 , alpha , x);
			else
				cscal_kernel_16(n1 , alpha , x);

		i = n1 << 1;
		j = n1;
	}


	if ( da_r == 0.0 )
	{

		if ( da_i == 0.0 )
		{

			while(j < n)
			{
		
					x[i]=0.0;
					x[i+1]=0.0;
					i += 2 ;
					j++;

			}

		}
		else
		{

			while(j < n)
			{
			
				temp0        = -da_i * x[i+1];
				x[i+1]       =  da_i * x[i];
				x[i]         =  temp0;
				i += 2 ;
				j++;

			}

		}

	}
	else
	{

		if ( da_i == 0.0 )
		{

			while(j < n)
			{
			
					temp0        =  da_r * x[i];
					x[i+1]       =  da_r * x[i+1];
					x[i]         =  temp0;
					i += 2 ;
					j++;

			}

		}
		else
		{

			BLASLONG n2 = n & -2;

			while(j < n2)
			{

				temp0        =  da_r * x[i]   - da_i * x[i+1];
				temp1        =  da_r * x[i+2] - da_i * x[i+3];
				x[i+1]       =  da_r * x[i+1] + da_i * x[i];
				x[i+3]       =  da_r * x[i+3] + da_i * x[i+2];
				x[i]         =  temp0;
				x[i+2]       =  temp1;
				i += 4 ;
				j+=2;

			}

			while(j < n)
			{

				temp0        =  da_r * x[i]   - da_i * x[i+1];
				x[i+1]       =  da_r * x[i+1] + da_i * x[i];
				x[i]         =  temp0;
				i += 2 ;
				j++;

			}

		}		

	}

	return(0);
}


