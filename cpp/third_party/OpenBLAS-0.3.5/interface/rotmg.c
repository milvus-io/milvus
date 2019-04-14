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

/**************************************************************************************
* 2014/05/02 Saar
* 	fixed two bugs as reported by Brendan Tracey
*	Test with lapack-3.5.0	: OK
*
**************************************************************************************/


#include "common.h"
#ifdef FUNCTION_PROFILE
#include "functable.h"
#endif

#define  GAM     4096.e0
#define  GAMSQ   16777216.e0
#define  RGAMSQ  5.9604645e-8

#define  TWO 2.e0

#ifdef DOUBLE
#define ABS(x) fabs(x)
#else
#define ABS(x) fabsf(x)
#endif

#ifndef CBLAS

void NAME(FLOAT *dd1, FLOAT *dd2, FLOAT *dx1, FLOAT *DY1, FLOAT *dparam){

  FLOAT	dy1 = *DY1;

#else

void CNAME(FLOAT *dd1, FLOAT *dd2, FLOAT *dx1, FLOAT dy1, FLOAT *dparam){

#endif

	FLOAT du, dp1, dp2, dq2, dq1, dh11=ZERO, dh21=ZERO, dh12=ZERO, dh22=ZERO, dflag=-ONE, dtemp;

	if (*dd2 == ZERO || dy1 == ZERO)
	{
		dflag = -TWO;
		dparam[0] = dflag;
		return;
	}
		
	if(*dd1 < ZERO)
	{
		dflag = -ONE;
		dh11  = ZERO;
		dh12  = ZERO;
		dh21  = ZERO;
		dh22  = ZERO;

		*dd1  = ZERO;
		*dd2  = ZERO;
		*dx1  = ZERO;
	}
	else if ((*dd1 == ZERO || *dx1 == ZERO) && *dd2 > ZERO)
	{
		dflag = ONE;
		dh12 = 1;
		dh21 = -1;
		*dx1 = dy1;
		dtemp = *dd1;
		*dd1 = *dd2;
		*dd2 = dtemp;
	} 
	else
	{
		dp2 = *dd2 * dy1;
		if(dp2 == ZERO)
		{
			dflag = -TWO;
			dparam[0] = dflag;
			return;
		}
		dp1 = *dd1 * *dx1;
		dq2 =  dp2 * dy1;
		dq1 =  dp1 * *dx1;
		if(ABS(dq1) > ABS(dq2))
		{
			dflag = ZERO;
			dh11  =  ONE;
			dh22  =  ONE;
			dh21 = -  dy1 / *dx1;
			dh12 =    dp2 /  dp1;

			du   = ONE - dh12 * dh21;
			if(du > ZERO)
			{
				dflag = ZERO;
				*dd1  = *dd1 / du;
				*dd2  = *dd2 / du;
				*dx1  = *dx1 * du;
			} else {
				dflag = -ONE;

				dh11  = ZERO;
				dh12  = ZERO;
				dh21  = ZERO;
				dh22  = ZERO;

				*dd1  = ZERO;
				*dd2  = ZERO;
				*dx1  = ZERO;
			}
			
		}
		else
		{
			if(dq2 < ZERO)
			{
				dflag = -ONE;

				dh11  = ZERO;
				dh12  = ZERO;
				dh21  = ZERO;
				dh22  = ZERO;

				*dd1  = ZERO;
				*dd2  = ZERO;
				*dx1  = ZERO;
			}
			else
			{
				dflag =  ONE;
				dh21  = -ONE;
				dh12  =  ONE;

				dh11  =  dp1 /  dp2;
				dh22  = *dx1 /  dy1;
				du    =  ONE + dh11 * dh22;
				dtemp = *dd2 / du;

				*dd2  = *dd1 / du;
				*dd1  = dtemp;
				*dx1  = dy1 * du;
			}
		}


		while ( *dd1 <= RGAMSQ && *dd1 != ZERO)
		{
			dflag = -ONE;
			*dd1  = *dd1 * (GAM * GAM);
			*dx1  = *dx1 / GAM;
			dh11  = dh11 / GAM;
			dh12  = dh12 / GAM;
		}
		while (ABS(*dd1) > GAMSQ) {
			dflag = -ONE;
			*dd1  = *dd1 / (GAM * GAM);
			*dx1  = *dx1 * GAM;
			dh11  = dh11 * GAM;
			dh12  = dh12 * GAM;
		}

		while (ABS(*dd2) <= RGAMSQ && *dd2 != ZERO) {
			dflag = -ONE;
			*dd2  = *dd2 * (GAM * GAM);
			dh21  = dh21 / GAM;
			dh22  = dh22 / GAM;
		}
		while (ABS(*dd2) > GAMSQ) {
			dflag = -ONE;
			*dd2  = *dd2 / (GAM * GAM);
			dh21  = dh21 * GAM;
			dh22  = dh22 * GAM;
		}

	}

	if(dflag < ZERO)
	{
		dparam[1] = dh11;
		dparam[2] = dh21;
		dparam[3] = dh12;
		dparam[4] = dh22;
	}
	else
	{
		if(dflag == ZERO)
		{
			dparam[2] = dh21;
			dparam[3] = dh12;
		}
		else
		{
			dparam[1] = dh11;
			dparam[4] = dh22;
		}
	}


	dparam[0] = dflag;
	return;
}


