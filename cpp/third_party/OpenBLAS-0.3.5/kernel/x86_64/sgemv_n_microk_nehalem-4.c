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



#define HAVE_KERNEL_4x8 1
static void sgemv_kernel_4x8( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y, BLASLONG lda4, FLOAT *alpha) __attribute__ ((noinline));

static void sgemv_kernel_4x8( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y, BLASLONG lda4, FLOAT *alpha)
{

	BLASLONG register i = 0;

	__asm__  __volatile__
	(
	"movss    (%2), %%xmm12	 \n\t"	// x0 
	"movss   4(%2), %%xmm13	 \n\t"	// x1 
	"movss   8(%2), %%xmm14	 \n\t"	// x2 
	"movss  12(%2), %%xmm15	 \n\t"	// x3 
	"shufps $0,  %%xmm12, %%xmm12\n\t"	
	"shufps $0,  %%xmm13, %%xmm13\n\t"	
	"shufps $0,  %%xmm14, %%xmm14\n\t"	
	"shufps $0,  %%xmm15, %%xmm15\n\t"	

	"movss  16(%2), %%xmm0	 \n\t"	// x4 
	"movss  20(%2), %%xmm1	 \n\t"	// x5 
	"movss  24(%2), %%xmm2	 \n\t"	// x6 
	"movss  28(%2), %%xmm3	 \n\t"	// x7 
	"shufps $0,  %%xmm0 , %%xmm0 \n\t"	
	"shufps $0,  %%xmm1 , %%xmm1 \n\t"	
	"shufps $0,  %%xmm2 , %%xmm2 \n\t"	
	"shufps $0,  %%xmm3 , %%xmm3 \n\t"	

	"movss    (%9), %%xmm6	     \n\t"	// alpha 
	"shufps $0,  %%xmm6 , %%xmm6 \n\t"	


	".p2align 4				 \n\t"
	"1:				 \n\t"
	"xorps           %%xmm4 , %%xmm4	 \n\t"
	"xorps           %%xmm5 , %%xmm5	 \n\t"
	"movups             (%3,%0,4), %%xmm7          \n\t" // 4 * y

	".p2align 1				       \n\t"
	"movups             (%4,%0,4), %%xmm8          \n\t" 
	"movups             (%5,%0,4), %%xmm9          \n\t" 
	"movups             (%6,%0,4), %%xmm10         \n\t" 
	"movups             (%7,%0,4), %%xmm11         \n\t" 
	".p2align 1				       \n\t"
	"mulps		%%xmm12, %%xmm8		       \n\t"
	"mulps		%%xmm13, %%xmm9		       \n\t"
	"mulps		%%xmm14, %%xmm10	       \n\t"
	"mulps		%%xmm15, %%xmm11	       \n\t"
	"addps		%%xmm8 , %%xmm4		       \n\t"
	"addps		%%xmm9 , %%xmm5		       \n\t"
	"addps		%%xmm10, %%xmm4	               \n\t"
	"addps		%%xmm11, %%xmm5 	       \n\t"

	"movups             (%4,%8,4), %%xmm8          \n\t" 
	"movups             (%5,%8,4), %%xmm9          \n\t" 
	"movups             (%6,%8,4), %%xmm10         \n\t" 
	"movups             (%7,%8,4), %%xmm11         \n\t" 
	".p2align 1				       \n\t"
	"mulps		%%xmm0 , %%xmm8		       \n\t"
	"mulps		%%xmm1 , %%xmm9		       \n\t"
	"mulps		%%xmm2 , %%xmm10	       \n\t"
	"mulps		%%xmm3 , %%xmm11	       \n\t"
	"addps		%%xmm8 , %%xmm4		       \n\t"
	"addps		%%xmm9 , %%xmm5		       \n\t"
	"addps		%%xmm10, %%xmm4	       	       \n\t"
	"addps		%%xmm11, %%xmm5 	       \n\t"

        "addq		$4 , %8	  	 	       \n\t"
	"addps		%%xmm5 , %%xmm4 	       \n\t"
        "addq		$4 , %0	  	 	       \n\t"
	"mulps		%%xmm6 , %%xmm4		       \n\t" 
	"subq	        $4 , %1			       \n\t"		
	"addps		%%xmm4 , %%xmm7 	       \n\t"

	"movups  %%xmm7 , -16(%3,%0,4)		       \n\t"	// 4 * y

	"jnz		1b		       \n\t"

	:
          "+r" (i),	// 0	
	  "+r" (n)  	// 1
        : 
          "r" (x),      // 2
          "r" (y),      // 3
          "r" (ap[0]),  // 4
          "r" (ap[1]),  // 5
          "r" (ap[2]),  // 6
          "r" (ap[3]),  // 7
          "r" (lda4),   // 8
          "r" (alpha)   // 9
	: "cc", 
	  "%xmm0", "%xmm1", 
	  "%xmm2", "%xmm3", 
	  "%xmm4", "%xmm5", 
	  "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11",
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

} 




#define HAVE_KERNEL_4x4 1
static void sgemv_kernel_4x4( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y, FLOAT *alpha) __attribute__ ((noinline));

static void sgemv_kernel_4x4( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y, FLOAT *alpha)
{

	BLASLONG register i = 0;

	__asm__  __volatile__
	(
	"movss    (%2), %%xmm12	 \n\t"	// x0 
	"movss   4(%2), %%xmm13	 \n\t"	// x1 
	"movss   8(%2), %%xmm14	 \n\t"	// x2 
	"movss  12(%2), %%xmm15	 \n\t"	// x3 
	"shufps $0,  %%xmm12, %%xmm12\n\t"	
	"shufps $0,  %%xmm13, %%xmm13\n\t"	
	"shufps $0,  %%xmm14, %%xmm14\n\t"	
	"shufps $0,  %%xmm15, %%xmm15\n\t"	

	"movss    (%8), %%xmm6	     \n\t"	// alpha 
	"shufps $0,  %%xmm6 , %%xmm6 \n\t"	

	".p2align 4				 \n\t"
	"1:				 \n\t"
	"xorps           %%xmm4 , %%xmm4	 \n\t"
	"movups	       (%3,%0,4), %%xmm7	 \n\t"	// 4 * y

	"movups             (%4,%0,4), %%xmm8          \n\t" 
	"movups             (%5,%0,4), %%xmm9          \n\t" 
	"movups             (%6,%0,4), %%xmm10         \n\t" 
	"movups             (%7,%0,4), %%xmm11         \n\t" 
	"mulps		%%xmm12, %%xmm8		       \n\t"
	"mulps		%%xmm13, %%xmm9		       \n\t"
	"mulps		%%xmm14, %%xmm10	       \n\t"
	"mulps		%%xmm15, %%xmm11	       \n\t"
	"addps		%%xmm8 , %%xmm4		       \n\t"
        "addq		$4 , %0	  	 	       \n\t"
	"addps		%%xmm9 , %%xmm4		       \n\t"
	"subq	        $4 , %1			       \n\t"		
	"addps		%%xmm10 , %%xmm4	       \n\t"
	"addps		%%xmm4 , %%xmm11	       \n\t"

	"mulps		%%xmm6 , %%xmm11	       \n\t" 
	"addps		%%xmm7 , %%xmm11 	       \n\t"
	"movups  %%xmm11, -16(%3,%0,4)		       \n\t"	// 4 * y

	"jnz		1b		       \n\t"

	:
          "+r" (i),	// 0	
	  "+r" (n)  	// 1
        : 
          "r" (x),      // 2
          "r" (y),      // 3
          "r" (ap[0]),  // 4
          "r" (ap[1]),  // 5
          "r" (ap[2]),  // 6
          "r" (ap[3]),  // 7
          "r" (alpha)   // 8
	: "cc", 
	  "%xmm4", "%xmm5", 
	  "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11",
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

} 


