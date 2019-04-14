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

/*****************************************************
 * 2015-09-07 grisuthedragon 
******************************************************/

int CNAME(BLASLONG rows, BLASLONG cols, FLOAT alpha_r, FLOAT alpha_i, FLOAT *a, BLASLONG lda)
{
	BLASLONG i,j,ia,ib;
	FLOAT *aptr,*bptr;
    FLOAT t0, t1; 

	if ( rows <= 0     )  return(0);
	if ( cols <= 0     )  return(0);

    aptr = a;
	lda *= 2;
	ib = 0;
	for ( i=0; i<rows ; i++ )
	{
		bptr = &a[ib+i*lda];
		ia = 2*i;
        
        /* Diagonal Element  */
        t0 = bptr[0]; 
        t1 = bptr[1]; 
		bptr[0]   = alpha_r * t0   - alpha_i * t1;
		bptr[1]   = alpha_r * t1   + alpha_i * t0;

        bptr +=lda; 
        ia += 2; 

		for(j=i+1; j<cols; j++)
		{
            t0 = bptr[0]; 
            t1 = bptr[1]; 
			bptr[0]   = alpha_r * aptr[ia]   - alpha_i * aptr[ia+1];
			bptr[1]   = alpha_r * aptr[ia+1] + alpha_i * aptr[ia];
    		aptr[ia]   = alpha_r * t0   - alpha_i * t1;
			aptr[ia+1] = alpha_r * t1   + alpha_i * t0;

			ia += 2;
			bptr += lda;
		}
		aptr += lda;
		ib += 2;
	}

	return(0);

}


