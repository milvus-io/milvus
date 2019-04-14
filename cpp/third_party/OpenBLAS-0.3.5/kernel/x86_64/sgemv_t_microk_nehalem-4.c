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

#define HAVE_KERNEL_4x4 1
static void sgemv_kernel_4x4( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y) __attribute__ ((noinline));

static void sgemv_kernel_4x4( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y)
{

	BLASLONG register i = 0;

	__asm__  __volatile__
	(
	"xorps		%%xmm4 , %%xmm4	         \n\t"
	"xorps		%%xmm5 , %%xmm5	         \n\t"
	"xorps		%%xmm6 , %%xmm6	         \n\t"
	"xorps		%%xmm7 , %%xmm7	         \n\t"

	".p2align 4				 \n\t"
	"1:				 \n\t"

	"movups	       (%2,%0,4), %%xmm12              \n\t"   // 4 * x
	"movups        (%4,%0,4), %%xmm8               \n\t"   // 4 * a0
	"movups        (%5,%0,4), %%xmm9               \n\t"   // 4 * a1
	"movups        (%6,%0,4), %%xmm10              \n\t"   // 4 * a2
	"movups        (%7,%0,4), %%xmm11              \n\t"   // 4 * a3

	"mulps		%%xmm12, %%xmm8		       \n\t"
	"mulps		%%xmm12, %%xmm9		       \n\t"
	"mulps		%%xmm12, %%xmm10	       \n\t"
	"mulps		%%xmm12, %%xmm11	       \n\t"
	"addps		%%xmm8 , %%xmm4		       \n\t"
        "addq		$4 , %0	  	 	       \n\t"
	"addps		%%xmm9 , %%xmm5		       \n\t"
	"subq	        $4 , %1			       \n\t"		
	"addps		%%xmm10, %%xmm6		       \n\t"
	"addps		%%xmm11, %%xmm7		       \n\t"

	"jnz		1b		       \n\t"

        "haddps        %%xmm4, %%xmm4  \n\t"
        "haddps        %%xmm5, %%xmm5  \n\t"
        "haddps        %%xmm6, %%xmm6  \n\t"
        "haddps        %%xmm7, %%xmm7  \n\t"

        "haddps        %%xmm4, %%xmm4  \n\t"
        "haddps        %%xmm5, %%xmm5  \n\t"
        "haddps        %%xmm6, %%xmm6  \n\t"
        "haddps        %%xmm7, %%xmm7  \n\t"

        "movss         %%xmm4,    (%3)         \n\t"
        "movss         %%xmm5,   4(%3)         \n\t"
        "movss         %%xmm6,   8(%3)         \n\t"
        "movss         %%xmm7,  12(%3)         \n\t"

	:
          "+r" (i),	// 0	
	  "+r" (n)  	// 1
        : 
          "r" (x),      // 2
          "r" (y),      // 3
          "r" (ap[0]),  // 4
          "r" (ap[1]),  // 5
          "r" (ap[2]),  // 6
          "r" (ap[3])   // 7
	: "cc", 
	  "%xmm4", "%xmm5", "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11",
	  "%xmm12",
	  "memory"
	);

} 


