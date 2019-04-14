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

#include <arm_neon.h>

#define prefetch(a) __asm__("prfm PLDL1STRM, [%0]"::"r"(a):"memory");

FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
{
	BLASLONG i=0;
	BLASLONG ix=0,iy=0;

	FLOAT  dot = 0.0 ;

	if ( n < 0 )  return(dot);

	if ( (inc_x == 1) && (inc_y == 1) )
	{

                float64x2_t vdot0 = {0.0, 0.0};
                float64x2_t vdot1 = {0.0, 0.0};
                float64x2_t vdot2 = {0.0, 0.0};
                float64x2_t vdot3 = {0.0, 0.0};
                float64x2_t *vx = (float64x2_t*)x;
                float64x2_t *vy = (float64x2_t*)y;
#if 0
		prefetch(x + 128/sizeof(*x));
		prefetch(y + 128/sizeof(*y));
#endif
		prefetch(x + 2*128/sizeof(*x));
		prefetch(y + 2*128/sizeof(*y));
		prefetch(x + 3*128/sizeof(*x));
		prefetch(y + 3*128/sizeof(*y));

		int n1 = n&-8;

		while(i < n1)
		{
#if 0
			vdot0 = vfmaq_f64 (vdot0,
					   vy[0],
					   vx[0]);
			vdot1 = vfmaq_f64 (vdot1,
					   vy[1],
					   vx[1]);
			vdot2 = vfmaq_f64 (vdot2,
					   vy[2],
					   vx[2]);
			vdot3 = vfmaq_f64 (vdot3,
					   vy[3],
					   vx[3]);
#else
			vdot0 = vy[0] * vx[0] + vdot0;
			vdot1 = vy[1] * vx[1] + vdot1;
			vdot2 = vy[2] * vx[2] + vdot2;
			vdot3 = vy[3] * vx[3] + vdot3;
#endif
			vy += 4;
			vx += 4;
			i += 8;
			prefetch(vx + 3*128/sizeof(*x));
			prefetch(vy + 3*128/sizeof(*y));

		}
		dot = vaddvq_f64 (vdot0 + vdot1);
		dot += vaddvq_f64 (vdot2 + vdot3);
		i = n1;

		while(i < n)
		{
			dot += y[i] * x[i] ;
			i++ ;

		}
		return(dot);


	}

	while(i < n)
	{
		dot += y[iy] * x[ix] ;
		ix  += inc_x ;
		iy  += inc_y ;
		i++ ;

	}
	return(dot);

}


