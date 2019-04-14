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
static void sdot_kernel_16( BLASLONG n, FLOAT *x, FLOAT *y , FLOAT *dot) __attribute__ ((noinline));

static void sdot_kernel_16( BLASLONG n, FLOAT *x, FLOAT *y, FLOAT *dot)
{


	BLASLONG register i = 0;

	__asm__  __volatile__
	(
	"vxorps		%%ymm4, %%ymm4, %%ymm4	             \n\t"
	"vxorps		%%ymm5, %%ymm5, %%ymm5	             \n\t"
	"vxorps		%%ymm6, %%ymm6, %%ymm6	             \n\t"
	"vxorps		%%ymm7, %%ymm7, %%ymm7	             \n\t"

	".p2align 4				             \n\t"
	"1:				             \n\t"
        "vmovups                  (%2,%0,4), %%ymm12         \n\t"  // 2 * x
        "vmovups                32(%2,%0,4), %%ymm13         \n\t"  // 2 * x
        "vmovups                64(%2,%0,4), %%ymm14         \n\t"  // 2 * x
        "vmovups                96(%2,%0,4), %%ymm15         \n\t"  // 2 * x

	"vmulps      (%3,%0,4), %%ymm12, %%ymm12 \n\t"  // 2 * y
	"vmulps    32(%3,%0,4), %%ymm13, %%ymm13 \n\t"  // 2 * y
	"vmulps    64(%3,%0,4), %%ymm14, %%ymm14 \n\t"  // 2 * y
	"vmulps    96(%3,%0,4), %%ymm15, %%ymm15 \n\t"  // 2 * y

	"vaddps    %%ymm4 , %%ymm12, %%ymm4 \n\t"  // 2 * y
	"vaddps    %%ymm5 , %%ymm13, %%ymm5 \n\t"  // 2 * y
	"vaddps    %%ymm6 , %%ymm14, %%ymm6 \n\t"  // 2 * y
	"vaddps    %%ymm7 , %%ymm15, %%ymm7 \n\t"  // 2 * y

	"addq		$32 , %0	  	     \n\t"
	"subq	        $32 , %1		     \n\t"		
	"jnz		1b		             \n\t"

	"vextractf128	$1 , %%ymm4 , %%xmm12	     \n\t"
	"vextractf128	$1 , %%ymm5 , %%xmm13	     \n\t"
	"vextractf128	$1 , %%ymm6 , %%xmm14	     \n\t"
	"vextractf128	$1 , %%ymm7 , %%xmm15	     \n\t"

	"vaddps        %%xmm4, %%xmm12, %%xmm4	\n\t"
	"vaddps        %%xmm5, %%xmm13, %%xmm5	\n\t"
	"vaddps        %%xmm6, %%xmm14, %%xmm6	\n\t"
	"vaddps        %%xmm7, %%xmm15, %%xmm7	\n\t"

	"vaddps        %%xmm4, %%xmm5, %%xmm4	\n\t"
	"vaddps        %%xmm6, %%xmm7, %%xmm6	\n\t"
	"vaddps        %%xmm4, %%xmm6, %%xmm4	\n\t"

	"vhaddps        %%xmm4, %%xmm4, %%xmm4	\n\t"
	"vhaddps        %%xmm4, %%xmm4, %%xmm4	\n\t"

	"vmovss		%%xmm4,    (%4)		\n\t"
	"vzeroupper				\n\t"

	:
        : 
          "r" (i),	// 0	
	  "r" (n),  	// 1
          "r" (x),      // 2
          "r" (y),      // 3
          "r" (dot)     // 4
	: "cc", 
	  "%xmm4", "%xmm5", 
	  "%xmm6", "%xmm7", 
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

} 


