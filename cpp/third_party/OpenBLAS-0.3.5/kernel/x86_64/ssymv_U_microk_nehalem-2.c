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
static void ssymv_kernel_4x4( BLASLONG n, FLOAT *a0, FLOAT *a1, FLOAT *a2, FLOAT *a3,  FLOAT *x, FLOAT *y, FLOAT *temp1, FLOAT *temp2) __attribute__ ((noinline));

static void ssymv_kernel_4x4(BLASLONG n, FLOAT *a0, FLOAT *a1, FLOAT *a2, FLOAT *a3, FLOAT *x, FLOAT *y, FLOAT *temp1, FLOAT *temp2)
{

	BLASLONG register i = 0;

	__asm__  __volatile__
	(
	"xorps          %%xmm0 , %%xmm0              \n\t"	// temp2[0]
	"xorps          %%xmm1 , %%xmm1              \n\t"	// temp2[1]
	"xorps          %%xmm2 , %%xmm2              \n\t"	// temp2[2]
	"xorps          %%xmm3 , %%xmm3              \n\t"	// temp2[3]
	"movss	       (%8),    %%xmm4	             \n\t"	// temp1[0]
	"movss        4(%8),    %%xmm5	             \n\t"	// temp1[1]
	"movss        8(%8),    %%xmm6	             \n\t"	// temp1[2]
	"movss       12(%8),    %%xmm7	             \n\t"	// temp1[3]
	"shufps $0,  %%xmm4, %%xmm4                  \n\t"
	"shufps $0,  %%xmm5, %%xmm5                  \n\t"
	"shufps $0,  %%xmm6, %%xmm6                  \n\t"
	"shufps $0,  %%xmm7, %%xmm7                  \n\t"

	"xorq		%0,%0			     \n\t"

	".p2align 4		  		       \n\t"
	"1:				       \n\t"
	"movups	            (%2,%0,4), %%xmm8	       \n\t"	// 4 * x
	"movups	            (%3,%0,4), %%xmm9         \n\t"	// 4 * y

	"movups	            (%4,%0,4), %%xmm12	       \n\t"	// 4 * a
	"movups	            (%5,%0,4), %%xmm13	       \n\t"	// 4 * a

	"movups		    %%xmm12  , %%xmm11	       \n\t"
	"mulps		    %%xmm4   , %%xmm11	       \n\t"    // temp1 * a
	"addps		    %%xmm11  , %%xmm9	       \n\t"    // y += temp1 * a
	"mulps		    %%xmm8   , %%xmm12	       \n\t"    // a * x
	"addps		    %%xmm12  , %%xmm0 	       \n\t"    // temp2 += x * a

	"movups	            (%6,%0,4), %%xmm14	       \n\t"	// 4 * a
	"movups	            (%7,%0,4), %%xmm15	       \n\t"	// 4 * a

	"movups		    %%xmm13  , %%xmm11	       \n\t"
	"mulps		    %%xmm5   , %%xmm11	       \n\t"    // temp1 * a
	"addps		    %%xmm11  , %%xmm9	       \n\t"    // y += temp1 * a
	"mulps		    %%xmm8   , %%xmm13	       \n\t"    // a * x
	"addps		    %%xmm13  , %%xmm1 	       \n\t"    // temp2 += x * a

	"movups		    %%xmm14  , %%xmm11	       \n\t"
	"mulps		    %%xmm6   , %%xmm11	       \n\t"    // temp1 * a
	"addps		    %%xmm11  , %%xmm9	       \n\t"    // y += temp1 * a
	"mulps		    %%xmm8   , %%xmm14	       \n\t"    // a * x
	"addps		    %%xmm14  , %%xmm2 	       \n\t"    // temp2 += x * a

	"movups		    %%xmm15  , %%xmm11	       \n\t"
	"mulps		    %%xmm7   , %%xmm11	       \n\t"    // temp1 * a
	"addps		    %%xmm11  , %%xmm9	       \n\t"    // y += temp1 * a
	"mulps		    %%xmm8   , %%xmm15	       \n\t"    // a * x
	"addps		    %%xmm15  , %%xmm3 	       \n\t"    // temp2 += x * a

	"movups             %%xmm9,   (%3,%0,4)       \n\t"    // 4 * y

        "addq		$4 , %0	  	 	      \n\t"
	"subq	        $4 , %1			      \n\t"		
	"jnz		1b		      \n\t"

	"haddps        %%xmm0, %%xmm0  \n\t"
	"haddps        %%xmm1, %%xmm1  \n\t"
	"haddps        %%xmm2, %%xmm2  \n\t"
	"haddps        %%xmm3, %%xmm3  \n\t"
	"haddps        %%xmm0, %%xmm0  \n\t"
	"haddps        %%xmm1, %%xmm1  \n\t"
	"haddps        %%xmm2, %%xmm2  \n\t"
	"haddps        %%xmm3, %%xmm3  \n\t"

	"movss         %%xmm0 ,   (%9)		\n\t"	// save temp2
	"movss         %%xmm1 ,  4(%9)		\n\t"	// save temp2
	"movss         %%xmm2 ,  8(%9)		\n\t"	// save temp2
	"movss         %%xmm3 , 12(%9)		\n\t"	// save temp2

	:
        : 
          "r" (i),	// 0	
	  "r" (n),  	// 1
          "r" (x),      // 2
          "r" (y),      // 3
          "r" (a0),     // 4
          "r" (a1),     // 5
          "r" (a2),     // 6
          "r" (a3),     // 7
          "r" (temp1),  // 8
          "r" (temp2)   // 9
	: "cc", 
	  "%xmm0", "%xmm1", "%xmm2", "%xmm3", 
	  "%xmm4", "%xmm5", "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11", 
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

} 


