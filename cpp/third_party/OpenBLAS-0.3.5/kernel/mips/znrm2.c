/***************************************************************************
Copyright (c) 2016, The OpenBLAS Project
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
#include <math.h>

#if defined(DOUBLE)

#define ABS fabs

#else

#define ABS fabsf

#endif



FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
	BLASLONG i=0;
	FLOAT scale = 0.0;
	FLOAT ssq   = 1.0;
	BLASLONG inc_x2;
	FLOAT temp;

	if (n <= 0 || inc_x <= 0) return(0.0);

	inc_x2 = 2 * inc_x;

	n *= inc_x2;
	while(i < n)
	{

		if ( x[i] != 0.0 )
		{
			temp = ABS( x[i] );
			if ( scale < temp )
			{
				ssq = 1 + ssq * ( scale / temp ) * ( scale / temp );
				scale = temp ;
			}
			else
			{
				ssq += ( temp / scale ) * ( temp / scale );
			}

		}

		if ( x[i+1] != 0.0 )
		{
			temp = ABS( x[i+1] );
			if ( scale < temp )
			{
				ssq = 1 + ssq * ( scale / temp ) * ( scale / temp );
				scale = temp ;
			}
			else
			{
				ssq += ( temp / scale ) * ( temp / scale );
			}

		}


		i += inc_x2;
	}
	scale = scale * sqrt( ssq );
	return(scale);

}


