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

#define HAVE_KERNEL_8 1
static void zdot_kernel_8( BLASLONG n, FLOAT *x, FLOAT *y , FLOAT *dot) __attribute__ ((noinline));

static void zdot_kernel_8( BLASLONG n, FLOAT *x, FLOAT *y, FLOAT *dot)
{


	BLASLONG register i = 0;

if ( n < 1280 )
{

	__asm__  __volatile__
	(
	"vzeroupper					     \n\t"
	"vxorpd		%%ymm0, %%ymm0, %%ymm0	             \n\t"
	"vxorpd		%%ymm1, %%ymm1, %%ymm1	             \n\t"
	"vxorpd		%%ymm2, %%ymm2, %%ymm2	             \n\t"
	"vxorpd		%%ymm3, %%ymm3, %%ymm3	             \n\t"
	"vxorpd		%%ymm4, %%ymm4, %%ymm4	             \n\t"
	"vxorpd		%%ymm5, %%ymm5, %%ymm5	             \n\t"
	"vxorpd		%%ymm6, %%ymm6, %%ymm6	             \n\t"
	"vxorpd		%%ymm7, %%ymm7, %%ymm7	             \n\t"

	".p2align 4			             \n\t"
	"1:				             \n\t"
        "vmovups                  (%2,%0,8), %%ymm8          \n\t"  // 2 * x
        "vmovups                32(%2,%0,8), %%ymm9          \n\t"  // 2 * x

        "vmovups                  (%3,%0,8), %%ymm12         \n\t"  // 2 * y
        "vmovups                32(%3,%0,8), %%ymm13         \n\t"  // 2 * y

        "vmovups                64(%3,%0,8), %%ymm14         \n\t"  // 2 * y
        "vmovups                96(%3,%0,8), %%ymm15         \n\t"  // 2 * y

	"vmulpd		   %%ymm8 , %%ymm12, %%ymm10    \n\t"
	"vmulpd		   %%ymm9 , %%ymm13, %%ymm11    \n\t"
	"vpermilpd      $0x5 , %%ymm12, %%ymm12               \n\t"
	"vpermilpd      $0x5 , %%ymm13, %%ymm13               \n\t"
	"vaddpd		   %%ymm0 , %%ymm10, %%ymm0	\n\t"
	"vaddpd		   %%ymm1 , %%ymm11, %%ymm1	\n\t"
	"vmulpd		   %%ymm8 , %%ymm12, %%ymm10    \n\t"
	"vmulpd		   %%ymm9 , %%ymm13, %%ymm11    \n\t"
        "vmovups                64(%2,%0,8), %%ymm8         \n\t"  // 2 * x
        "vmovups                96(%2,%0,8), %%ymm9         \n\t"  // 2 * x
	"vaddpd		   %%ymm4 , %%ymm10, %%ymm4	\n\t"
	"vaddpd		   %%ymm5 , %%ymm11, %%ymm5	\n\t"


	"vmulpd		   %%ymm8 , %%ymm14, %%ymm10    \n\t"
	"vmulpd		   %%ymm9 , %%ymm15, %%ymm11    \n\t"
	"vpermilpd      $0x5 , %%ymm14, %%ymm14               \n\t"
	"vpermilpd      $0x5 , %%ymm15, %%ymm15               \n\t"
	"vaddpd		   %%ymm2 , %%ymm10, %%ymm2	\n\t"
	"vaddpd		   %%ymm3 , %%ymm11, %%ymm3	\n\t"
	"vmulpd		   %%ymm8 , %%ymm14, %%ymm10    \n\t"
	"addq		$16 , %0	  	 	\n\t"
	"vmulpd		   %%ymm9 , %%ymm15, %%ymm11    \n\t"
	"vaddpd		   %%ymm6 , %%ymm10, %%ymm6	\n\t"
	"subq	        $8 , %1			        \n\t"		
	"vaddpd		   %%ymm7 , %%ymm11, %%ymm7	\n\t"

	"jnz		1b		             \n\t"

	"vaddpd        %%ymm0, %%ymm1, %%ymm0	\n\t"
	"vaddpd        %%ymm2, %%ymm3, %%ymm2	\n\t"
	"vaddpd        %%ymm0, %%ymm2, %%ymm0	\n\t"

	"vaddpd        %%ymm4, %%ymm5, %%ymm4	\n\t"
	"vaddpd        %%ymm6, %%ymm7, %%ymm6	\n\t"
	"vaddpd        %%ymm4, %%ymm6, %%ymm4	\n\t"

	"vextractf128 $1 , %%ymm0 , %%xmm1	\n\t"
	"vextractf128 $1 , %%ymm4 , %%xmm5	\n\t"

	"vaddpd        %%xmm0, %%xmm1, %%xmm0	\n\t"
	"vaddpd        %%xmm4, %%xmm5, %%xmm4	\n\t"

	"vmovups       %%xmm0,    (%4)		\n\t"
	"vmovups       %%xmm4,  16(%4)		\n\t"
	"vzeroupper					     \n\t"

	:
        : 
          "r" (i),	// 0	
	  "r" (n),  	// 1
          "r" (x),      // 2
          "r" (y),      // 3
          "r" (dot)     // 4
	: "cc", 
	  "%xmm0", "%xmm1", "%xmm2", "%xmm3", 
	  "%xmm4", "%xmm5", "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11",
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);
	return;

  }

	__asm__  __volatile__
	(
	"vzeroupper					     \n\t"
	"vxorpd		%%ymm0, %%ymm0, %%ymm0	             \n\t"
	"vxorpd		%%ymm1, %%ymm1, %%ymm1	             \n\t"
	"vxorpd		%%ymm2, %%ymm2, %%ymm2	             \n\t"
	"vxorpd		%%ymm3, %%ymm3, %%ymm3	             \n\t"
	"vxorpd		%%ymm4, %%ymm4, %%ymm4	             \n\t"
	"vxorpd		%%ymm5, %%ymm5, %%ymm5	             \n\t"
	"vxorpd		%%ymm6, %%ymm6, %%ymm6	             \n\t"
	"vxorpd		%%ymm7, %%ymm7, %%ymm7	             \n\t"

	".p2align 4			             \n\t"
	"1:				             \n\t"
	"prefetcht0	512(%2,%0,8)		     \n\t"
        "vmovups                  (%2,%0,8), %%ymm8          \n\t"  // 2 * x
        "vmovups                32(%2,%0,8), %%ymm9          \n\t"  // 2 * x

	"prefetcht0	512(%3,%0,8)		     \n\t"
        "vmovups                  (%3,%0,8), %%ymm12         \n\t"  // 2 * y
        "vmovups                32(%3,%0,8), %%ymm13         \n\t"  // 2 * y

        "vmovups                64(%3,%0,8), %%ymm14         \n\t"  // 2 * y
        "vmovups                96(%3,%0,8), %%ymm15         \n\t"  // 2 * y

	"prefetcht0	576(%3,%0,8)		     \n\t"
	"vmulpd		   %%ymm8 , %%ymm12, %%ymm10    \n\t"
	"vmulpd		   %%ymm9 , %%ymm13, %%ymm11    \n\t"
	"prefetcht0	576(%2,%0,8)		     \n\t"
	"vpermilpd      $0x5 , %%ymm12, %%ymm12               \n\t"
	"vpermilpd      $0x5 , %%ymm13, %%ymm13               \n\t"
	"vaddpd		   %%ymm0 , %%ymm10, %%ymm0	\n\t"
	"vaddpd		   %%ymm1 , %%ymm11, %%ymm1	\n\t"
	"vmulpd		   %%ymm8 , %%ymm12, %%ymm10    \n\t"
	"vmulpd		   %%ymm9 , %%ymm13, %%ymm11    \n\t"
        "vmovups                64(%2,%0,8), %%ymm8         \n\t"  // 2 * x
        "vmovups                96(%2,%0,8), %%ymm9         \n\t"  // 2 * x
	"vaddpd		   %%ymm4 , %%ymm10, %%ymm4	\n\t"
	"vaddpd		   %%ymm5 , %%ymm11, %%ymm5	\n\t"


	"vmulpd		   %%ymm8 , %%ymm14, %%ymm10    \n\t"
	"vmulpd		   %%ymm9 , %%ymm15, %%ymm11    \n\t"
	"vpermilpd      $0x5 , %%ymm14, %%ymm14               \n\t"
	"vpermilpd      $0x5 , %%ymm15, %%ymm15               \n\t"
	"vaddpd		   %%ymm2 , %%ymm10, %%ymm2	\n\t"
	"vaddpd		   %%ymm3 , %%ymm11, %%ymm3	\n\t"
	"vmulpd		   %%ymm8 , %%ymm14, %%ymm10    \n\t"
	"addq		$16 , %0	  	 	\n\t"
	"vmulpd		   %%ymm9 , %%ymm15, %%ymm11    \n\t"
	"vaddpd		   %%ymm6 , %%ymm10, %%ymm6	\n\t"
	"subq	        $8 , %1			        \n\t"		
	"vaddpd		   %%ymm7 , %%ymm11, %%ymm7	\n\t"

	"jnz		1b		             \n\t"

	"vaddpd        %%ymm0, %%ymm1, %%ymm0	\n\t"
	"vaddpd        %%ymm2, %%ymm3, %%ymm2	\n\t"
	"vaddpd        %%ymm0, %%ymm2, %%ymm0	\n\t"

	"vaddpd        %%ymm4, %%ymm5, %%ymm4	\n\t"
	"vaddpd        %%ymm6, %%ymm7, %%ymm6	\n\t"
	"vaddpd        %%ymm4, %%ymm6, %%ymm4	\n\t"

	"vextractf128 $1 , %%ymm0 , %%xmm1	\n\t"
	"vextractf128 $1 , %%ymm4 , %%xmm5	\n\t"

	"vaddpd        %%xmm0, %%xmm1, %%xmm0	\n\t"
	"vaddpd        %%xmm4, %%xmm5, %%xmm4	\n\t"

	"vmovups       %%xmm0,    (%4)		\n\t"
	"vmovups       %%xmm4,  16(%4)		\n\t"
	"vzeroupper					     \n\t"

	:
        : 
          "r" (i),	// 0	
	  "r" (n),  	// 1
          "r" (x),      // 2
          "r" (y),      // 3
          "r" (dot)     // 4
	: "cc", 
	  "%xmm0", "%xmm1", "%xmm2", "%xmm3", 
	  "%xmm4", "%xmm5", "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11",
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);




} 


