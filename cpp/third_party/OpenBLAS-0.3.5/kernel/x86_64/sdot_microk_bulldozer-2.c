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
	"vxorps		%%xmm4, %%xmm4, %%xmm4	             \n\t"
	"vxorps		%%xmm5, %%xmm5, %%xmm5	             \n\t"
	"vxorps		%%xmm6, %%xmm6, %%xmm6	             \n\t"
	"vxorps		%%xmm7, %%xmm7, %%xmm7	             \n\t"

	".align 16				             \n\t"
	"1:				             \n\t"
        "vmovups                  (%2,%0,4), %%xmm12         \n\t"  // 4 * x
        "vmovups                16(%2,%0,4), %%xmm13         \n\t"  // 4 * x
        "vmovups                32(%2,%0,4), %%xmm14         \n\t"  // 4 * x
        "vmovups                48(%2,%0,4), %%xmm15         \n\t"  // 4 * x

	"vfmaddps       %%xmm4,   (%3,%0,4), %%xmm12, %%xmm4 \n\t"  // 4 * y
	"vfmaddps       %%xmm5, 16(%3,%0,4), %%xmm13, %%xmm5 \n\t"  // 4 * y
	"vfmaddps       %%xmm6, 32(%3,%0,4), %%xmm14, %%xmm6 \n\t"  // 4 * y
	"vfmaddps       %%xmm7, 48(%3,%0,4), %%xmm15, %%xmm7 \n\t"  // 4 * y

	"addq		$16, %0	  	 	             \n\t"
	"subq	        $16, %1			             \n\t"		
	"jnz		1b		             \n\t"

	"vaddps        %%xmm4, %%xmm5, %%xmm4	\n\t"
	"vaddps        %%xmm6, %%xmm7, %%xmm6	\n\t"
	"vaddps        %%xmm4, %%xmm6, %%xmm4	\n\t"

	"vhaddps        %%xmm4, %%xmm4, %%xmm4	\n\t"
	"vhaddps        %%xmm4, %%xmm4, %%xmm4	\n\t"

	"vmovss		%%xmm4,    (%4)		\n\t"

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


