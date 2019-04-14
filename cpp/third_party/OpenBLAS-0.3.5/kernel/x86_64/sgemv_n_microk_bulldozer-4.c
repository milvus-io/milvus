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
	"vbroadcastss    (%2), %%xmm12	 \n\t"	// x0 
	"vbroadcastss   4(%2), %%xmm13	 \n\t"	// x1 
	"vbroadcastss   8(%2), %%xmm14	 \n\t"	// x2 
	"vbroadcastss  12(%2), %%xmm15	 \n\t"	// x3 
	"vbroadcastss  16(%2), %%xmm0 	 \n\t"	// x4 
	"vbroadcastss  20(%2), %%xmm1 	 \n\t"	// x5 
	"vbroadcastss  24(%2), %%xmm2 	 \n\t"	// x6 
	"vbroadcastss  28(%2), %%xmm3 	 \n\t"	// x7 

	"vbroadcastss    (%9), %%xmm8 	 \n\t"	// alpha 

        "testq          $0x04, %1                      \n\t"
        "jz             2f                    \n\t"

	"vxorps		%%xmm4, %%xmm4 , %%xmm4  \n\t"
	"vxorps		%%xmm5, %%xmm5 , %%xmm5  \n\t"

	"vfmaddps %%xmm4,   (%4,%0,4), %%xmm12, %%xmm4 \n\t" 
	"vfmaddps %%xmm5,   (%5,%0,4), %%xmm13, %%xmm5 \n\t" 
	"vfmaddps %%xmm4,   (%6,%0,4), %%xmm14, %%xmm4 \n\t" 
	"vfmaddps %%xmm5,   (%7,%0,4), %%xmm15, %%xmm5 \n\t" 
        "addq		$4 , %0	  	 	       \n\t"

	"vfmaddps %%xmm4,   (%4,%8,4), %%xmm0 , %%xmm4 \n\t" 
	"vfmaddps %%xmm5,   (%5,%8,4), %%xmm1 , %%xmm5 \n\t" 
	"vfmaddps %%xmm4,   (%6,%8,4), %%xmm2 , %%xmm4 \n\t" 
	"vfmaddps %%xmm5,   (%7,%8,4), %%xmm3 , %%xmm5 \n\t" 
        "addq		$4 , %8	  	 	       \n\t"
	
	"vaddps		%%xmm5 , %%xmm4, %%xmm4        \n\t"
	"vfmaddps -16(%3,%0,4) , %%xmm4, %%xmm8,%%xmm6 \n\t"
	"subq	        $4 , %1			       \n\t"		
	"vmovups  %%xmm6, -16(%3,%0,4)		       \n\t"	// 4 * y

	"2:                                  \n\t"

        "testq          $0x08, %1                      \n\t"
        "jz             3f                    \n\t"

	"vxorps		%%xmm4, %%xmm4 , %%xmm4  \n\t"
	"vxorps		%%xmm5, %%xmm5 , %%xmm5  \n\t"

	"vfmaddps %%xmm4,   (%4,%0,4), %%xmm12, %%xmm4 \n\t" 
	"vfmaddps %%xmm5, 16(%4,%0,4), %%xmm12, %%xmm5 \n\t" 
	"vfmaddps %%xmm4,   (%5,%0,4), %%xmm13, %%xmm4 \n\t" 
	"vfmaddps %%xmm5, 16(%5,%0,4), %%xmm13, %%xmm5 \n\t" 
	"vfmaddps %%xmm4,   (%6,%0,4), %%xmm14, %%xmm4 \n\t" 
	"vfmaddps %%xmm5, 16(%6,%0,4), %%xmm14, %%xmm5 \n\t" 
	"vfmaddps %%xmm4,   (%7,%0,4), %%xmm15, %%xmm4 \n\t" 
	"vfmaddps %%xmm5, 16(%7,%0,4), %%xmm15, %%xmm5 \n\t" 

	"vfmaddps %%xmm4,   (%4,%8,4), %%xmm0 , %%xmm4 \n\t" 
        "vfmaddps %%xmm5, 16(%4,%8,4), %%xmm0 , %%xmm5 \n\t" 
	"vfmaddps %%xmm4,   (%5,%8,4), %%xmm1 , %%xmm4 \n\t" 
	"vfmaddps %%xmm5, 16(%5,%8,4), %%xmm1 , %%xmm5 \n\t" 
	"vfmaddps %%xmm4,   (%6,%8,4), %%xmm2 , %%xmm4 \n\t" 
	"vfmaddps %%xmm5, 16(%6,%8,4), %%xmm2 , %%xmm5 \n\t" 
	"vfmaddps %%xmm4,   (%7,%8,4), %%xmm3 , %%xmm4 \n\t" 
	"vfmaddps %%xmm5, 16(%7,%8,4), %%xmm3 , %%xmm5 \n\t" 
	
	"vfmaddps    (%3,%0,4) , %%xmm4,%%xmm8,%%xmm4 \n\t"
	"vfmaddps  16(%3,%0,4) , %%xmm5,%%xmm8,%%xmm5 \n\t"
	"vmovups  %%xmm4,   (%3,%0,4)		      \n\t"	// 4 * y
	"vmovups  %%xmm5, 16(%3,%0,4)		      \n\t"	// 4 * y

        "addq		$8 , %0	  	 	      \n\t"
        "addq		$8 , %8	  	 	      \n\t"
	"subq	        $8 , %1			      \n\t"		


        "3:                                  \n\t"

        "cmpq           $0, %1                         \n\t"
        "je             4f                      \n\t"

	".align 16				 \n\t"
	"1:				 \n\t"

	"vxorps		%%xmm4, %%xmm4 , %%xmm4  \n\t"
	"vxorps		%%xmm5, %%xmm5 , %%xmm5  \n\t"
	"vxorps		%%xmm6, %%xmm6 , %%xmm6  \n\t"
	"vxorps		%%xmm7, %%xmm7 , %%xmm7  \n\t"

        "prefetcht0      192(%4,%0,4)                  \n\t"
	"vfmaddps %%xmm4,   (%4,%0,4), %%xmm12, %%xmm4 \n\t" 
	"vfmaddps %%xmm5, 16(%4,%0,4), %%xmm12, %%xmm5 \n\t" 
        "prefetcht0      192(%5,%0,4)                  \n\t"
	"vfmaddps %%xmm4,   (%5,%0,4), %%xmm13, %%xmm4 \n\t" 
	"vfmaddps %%xmm5, 16(%5,%0,4), %%xmm13, %%xmm5 \n\t" 
        "prefetcht0      192(%6,%0,4)                  \n\t"
	"vfmaddps %%xmm4,   (%6,%0,4), %%xmm14, %%xmm4 \n\t" 
	"vfmaddps %%xmm5, 16(%6,%0,4), %%xmm14, %%xmm5 \n\t" 
        "prefetcht0      192(%7,%0,4)                  \n\t"
	"vfmaddps %%xmm4,   (%7,%0,4), %%xmm15, %%xmm4 \n\t" 
	".align 2				 \n\t"
	"vfmaddps %%xmm5, 16(%7,%0,4), %%xmm15, %%xmm5 \n\t" 

	"vfmaddps %%xmm6, 32(%4,%0,4), %%xmm12, %%xmm6 \n\t" 
	"vfmaddps %%xmm7, 48(%4,%0,4), %%xmm12, %%xmm7 \n\t" 
	"vfmaddps %%xmm6, 32(%5,%0,4), %%xmm13, %%xmm6 \n\t" 
	"vfmaddps %%xmm7, 48(%5,%0,4), %%xmm13, %%xmm7 \n\t" 
	"vfmaddps %%xmm6, 32(%6,%0,4), %%xmm14, %%xmm6 \n\t" 
	"vfmaddps %%xmm7, 48(%6,%0,4), %%xmm14, %%xmm7 \n\t" 
	"vfmaddps %%xmm6, 32(%7,%0,4), %%xmm15, %%xmm6 \n\t" 
	"vfmaddps %%xmm7, 48(%7,%0,4), %%xmm15, %%xmm7 \n\t" 

        "prefetcht0      192(%4,%8,4)                  \n\t"
	"vfmaddps %%xmm4,   (%4,%8,4), %%xmm0 , %%xmm4 \n\t" 
        "vfmaddps %%xmm5, 16(%4,%8,4), %%xmm0 , %%xmm5 \n\t" 
        "prefetcht0      192(%5,%8,4)                  \n\t"
	"vfmaddps %%xmm4,   (%5,%8,4), %%xmm1 , %%xmm4 \n\t" 
	"vfmaddps %%xmm5, 16(%5,%8,4), %%xmm1 , %%xmm5 \n\t" 
        "prefetcht0      192(%6,%8,4)                  \n\t"
	"vfmaddps %%xmm4,   (%6,%8,4), %%xmm2 , %%xmm4 \n\t" 
	"vfmaddps %%xmm5, 16(%6,%8,4), %%xmm2 , %%xmm5 \n\t" 
        "prefetcht0      192(%7,%8,4)                  \n\t"
	"vfmaddps %%xmm4,   (%7,%8,4), %%xmm3 , %%xmm4 \n\t" 
	"vfmaddps %%xmm5, 16(%7,%8,4), %%xmm3 , %%xmm5 \n\t" 
	
	"vfmaddps %%xmm6, 32(%4,%8,4), %%xmm0 , %%xmm6 \n\t" 
        "vfmaddps %%xmm7, 48(%4,%8,4), %%xmm0 , %%xmm7 \n\t" 
	"vfmaddps %%xmm6, 32(%5,%8,4), %%xmm1 , %%xmm6 \n\t" 
	"vfmaddps %%xmm7, 48(%5,%8,4), %%xmm1 , %%xmm7 \n\t" 
	"vfmaddps %%xmm6, 32(%6,%8,4), %%xmm2 , %%xmm6 \n\t" 
	"vfmaddps %%xmm7, 48(%6,%8,4), %%xmm2 , %%xmm7 \n\t" 
	"vfmaddps %%xmm6, 32(%7,%8,4), %%xmm3 , %%xmm6 \n\t" 
	"vfmaddps %%xmm7, 48(%7,%8,4), %%xmm3 , %%xmm7 \n\t" 
	
	"vfmaddps    (%3,%0,4) , %%xmm4,%%xmm8,%%xmm4 \n\t"
	"vfmaddps  16(%3,%0,4) , %%xmm5,%%xmm8,%%xmm5 \n\t"
	"vfmaddps  32(%3,%0,4) , %%xmm6,%%xmm8,%%xmm6 \n\t"
	"vfmaddps  48(%3,%0,4) , %%xmm7,%%xmm8,%%xmm7 \n\t"

        "addq		$16, %0	  	 	      \n\t"
	"vmovups  %%xmm4,-64(%3,%0,4)		      \n\t"	// 4 * y
	"vmovups  %%xmm5,-48(%3,%0,4)		      \n\t"	// 4 * y
        "addq		$16, %8	  	 	      \n\t"
	"vmovups  %%xmm6,-32(%3,%0,4)		      \n\t"	// 4 * y
	"vmovups  %%xmm7,-16(%3,%0,4)		      \n\t"	// 4 * y

	"subq	        $16, %1			      \n\t"		
	"jnz		1b		      \n\t"

	"4:                             \n\t"

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
	  "%xmm8", "%xmm9", 
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
	"vbroadcastss    (%2), %%xmm12	 \n\t"	// x0 
	"vbroadcastss   4(%2), %%xmm13	 \n\t"	// x1 
	"vbroadcastss   8(%2), %%xmm14	 \n\t"	// x2 
	"vbroadcastss  12(%2), %%xmm15	 \n\t"	// x3 

	"vbroadcastss    (%8), %%xmm8 	 \n\t"	// alpha 

	".align 16				 \n\t"
	"1:				 \n\t"
	"vxorps		%%xmm4, %%xmm4 , %%xmm4  \n\t"
	"vxorps		%%xmm5, %%xmm5 , %%xmm5  \n\t"

	"vfmaddps %%xmm4,   (%4,%0,4), %%xmm12, %%xmm4 \n\t" 
	"vfmaddps %%xmm5,   (%5,%0,4), %%xmm13, %%xmm5 \n\t" 
	"vfmaddps %%xmm4,   (%6,%0,4), %%xmm14, %%xmm4 \n\t" 
	"vfmaddps %%xmm5,   (%7,%0,4), %%xmm15, %%xmm5 \n\t" 
	
	"vaddps	  %%xmm4, %%xmm5, %%xmm4	       \n\t"

	"vfmaddps    (%3,%0,4) , %%xmm4,%%xmm8,%%xmm6 \n\t"
	"vmovups  %%xmm6,   (%3,%0,4)		      \n\t"	// 4 * y

        "addq		$4 , %0	  	 	      \n\t"
	"subq	        $4 , %1			      \n\t"		
	"jnz		1b		      	\n\t"

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
	  "%xmm8", "%xmm9", 
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

} 


