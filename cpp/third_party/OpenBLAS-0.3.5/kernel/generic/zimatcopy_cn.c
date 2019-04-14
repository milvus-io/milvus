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

int CNAME(BLASLONG rows, BLASLONG cols, FLOAT alpha_r, FLOAT alpha_i, FLOAT *a , BLASLONG lda)
{
	BLASLONG i,j,ia;
	FLOAT *aptr;
    FLOAT a0, a1; 

	if ( rows <= 0     )  return(0);
	if ( cols <= 0     )  return(0);
    if ( alpha_r == 1.0 && alpha_i == 0.0) return(0); 

	aptr = a;

	lda *= 2;

	for ( i=0; i<cols ; i++ )
	{
		ia = 0;

		for(j=0; j<rows; j++)
		{
            a0 = aptr[ia]; 
            a1 = aptr[ia+1]; 
			aptr[ia]   = alpha_r * a0   - alpha_i * a1;
			aptr[ia+1] = alpha_r * a1   + alpha_i * a0;
			ia+=2;
		}
		aptr += lda;
	}

	return(0);

}


