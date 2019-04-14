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

#define HAVE_KERNEL_16 1
static void sger_kernel_16( BLASLONG n, FLOAT *x, FLOAT *y , FLOAT *alpha) __attribute__ ((noinline));

static void sger_kernel_16( BLASLONG n, FLOAT *x, FLOAT *y, FLOAT *alpha)
{


	BLASLONG register i = 0;

	__asm__  __volatile__
	(
	"vbroadcastss		(%4), %%xmm0		    \n\t"  // alpha	
	"prefetcht0	256(%3,%0,4)			    \n\t"
	"vmovups	  (%3,%0,4), %%xmm8         \n\t"
	"vmovups	16(%3,%0,4), %%xmm9         \n\t"
	"vmovups	32(%3,%0,4), %%xmm10        \n\t"
	"vmovups	48(%3,%0,4), %%xmm11        \n\t"

	"prefetcht0	256(%2,%0,4)			    \n\t"
	"vmovups	  (%2,%0,4), %%xmm4         \n\t"
	"vmovups	16(%2,%0,4), %%xmm5         \n\t"
	"vmovups	32(%2,%0,4), %%xmm6         \n\t"
	"vmovups	48(%2,%0,4), %%xmm7        \n\t"

	"addq		$16, %0	  	 	             \n\t"
	"subq	        $16, %1			             \n\t"		
	"jz		2f		             \n\t"

	".p2align 4				            \n\t"
	"1:				            \n\t"

	"vmulps		%%xmm4, %%xmm0, %%xmm4		\n\t"
	"vaddps		%%xmm8 , %%xmm4, %%xmm12	     \n\t"
	"vmulps		%%xmm5, %%xmm0, %%xmm5		\n\t"
	"vaddps		%%xmm9 , %%xmm5, %%xmm13	     \n\t"
	"vmulps		%%xmm6, %%xmm0, %%xmm6		\n\t"
	"vaddps		%%xmm10, %%xmm6, %%xmm14	     \n\t"
	"vmulps		%%xmm7, %%xmm0, %%xmm7		\n\t"
	"vaddps		%%xmm11, %%xmm7, %%xmm15	     \n\t"

	"prefetcht0	256(%3,%0,4)			    \n\t"
	"vmovups	  (%3,%0,4), %%xmm8         \n\t"
	"vmovups	16(%3,%0,4), %%xmm9         \n\t"
	"vmovups	32(%3,%0,4), %%xmm10        \n\t"
	"vmovups	48(%3,%0,4), %%xmm11        \n\t"

	"prefetcht0	256(%2,%0,4)			    \n\t"
	"vmovups	  (%2,%0,4), %%xmm4         \n\t"
	"vmovups	16(%2,%0,4), %%xmm5         \n\t"
	"vmovups	32(%2,%0,4), %%xmm6         \n\t"
	"vmovups	48(%2,%0,4), %%xmm7        \n\t"

	"vmovups	%%xmm12,  -64(%3,%0,4)		     \n\t"
	"vmovups	%%xmm13,  -48(%3,%0,4)		     \n\t"
	"vmovups	%%xmm14,  -32(%3,%0,4)		     \n\t"
	"vmovups	%%xmm15,  -16(%3,%0,4)		     \n\t"

	"addq		$16, %0	  	 	             \n\t"
	"subq	        $16, %1			             \n\t"		
	"jnz		1b		             \n\t"

	"2:				            \n\t"
	"vmulps		%%xmm4, %%xmm0, %%xmm4		\n\t"
	"vmulps		%%xmm5, %%xmm0, %%xmm5		\n\t"
	"vmulps		%%xmm6, %%xmm0, %%xmm6		\n\t"
	"vmulps		%%xmm7, %%xmm0, %%xmm7		\n\t"

	"vaddps		%%xmm8 , %%xmm4, %%xmm12	     \n\t"
	"vaddps		%%xmm9 , %%xmm5, %%xmm13	     \n\t"
	"vaddps		%%xmm10, %%xmm6, %%xmm14	     \n\t"
	"vaddps		%%xmm11, %%xmm7, %%xmm15	     \n\t"

	"vmovups	%%xmm12,  -64(%3,%0,4)		     \n\t"
	"vmovups	%%xmm13,  -48(%3,%0,4)		     \n\t"
	"vmovups	%%xmm14,  -32(%3,%0,4)		     \n\t"
	"vmovups	%%xmm15,  -16(%3,%0,4)		     \n\t"

	"vzeroupper					     \n\t"

	:
        : 
          "r" (i),	// 0	
	  "r" (n),  	// 1
          "r" (x),      // 2
          "r" (y),      // 3
          "r" (alpha)   // 4
	: "cc", 
	  "%xmm0", "%xmm1", "%xmm2", "%xmm3", 
	  "%xmm4", "%xmm5", "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11",
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

} 


