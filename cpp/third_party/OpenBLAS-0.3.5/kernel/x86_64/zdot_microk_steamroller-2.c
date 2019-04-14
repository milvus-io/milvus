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

  if ( n < 640 )
  {

	__asm__  __volatile__
	(
	"vzeroupper					     \n\t"
	"vxorpd		%%xmm0, %%xmm0, %%xmm0	             \n\t"
	"vxorpd		%%xmm1, %%xmm1, %%xmm1	             \n\t"
	"vxorpd		%%xmm2, %%xmm2, %%xmm2	             \n\t"
	"vxorpd		%%xmm3, %%xmm3, %%xmm3	             \n\t"
	"vxorpd		%%xmm4, %%xmm4, %%xmm4	             \n\t"
	"vxorpd		%%xmm5, %%xmm5, %%xmm5	             \n\t"
	"vxorpd		%%xmm6, %%xmm6, %%xmm6	             \n\t"
	"vxorpd		%%xmm7, %%xmm7, %%xmm7	             \n\t"

	".align 16			             \n\t"
	"1:				             \n\t"
	//"prefetcht0            512(%2,%0,8)		     \n\t"
        "vmovups                  (%2,%0,8), %%xmm8          \n\t"  // 1 * x
        "vmovups                16(%2,%0,8), %%xmm9          \n\t"  // 1 * x

	// "prefetcht0            512(%3,%0,8)		     \n\t"
        "vmovups                  (%3,%0,8), %%xmm12         \n\t"  // 1 * y
        "vmovups                16(%3,%0,8), %%xmm13         \n\t"  // 1 * y

        "vmovups                32(%2,%0,8), %%xmm10         \n\t"  // 1 * x
        "vmovups                48(%2,%0,8), %%xmm11         \n\t"  // 1 * x

        "vmovups                32(%3,%0,8), %%xmm14         \n\t"  // 1 * y
        "vmovups                48(%3,%0,8), %%xmm15         \n\t"  // 1 * y

	"vfmadd231pd       %%xmm8 , %%xmm12, %%xmm0     \n\t"  // x_r * y_r, x_i * y_i
	"vfmadd231pd       %%xmm9 , %%xmm13, %%xmm1     \n\t"  // x_r * y_r, x_i * y_i
	"vpermilpd      $0x1 , %%xmm13, %%xmm13               \n\t"
	"vpermilpd      $0x1 , %%xmm12, %%xmm12               \n\t"
	"vfmadd231pd       %%xmm10, %%xmm14, %%xmm2     \n\t"  // x_r * y_r, x_i * y_i
	"vfmadd231pd       %%xmm11, %%xmm15, %%xmm3     \n\t"  // x_r * y_r, x_i * y_i
	"vpermilpd      $0x1 , %%xmm14, %%xmm14               \n\t"
	"vpermilpd      $0x1 , %%xmm15, %%xmm15               \n\t"

	"vfmadd231pd       %%xmm8 , %%xmm12, %%xmm4     \n\t"  // x_r * y_i, x_i * y_r
	"addq		$8 , %0	  	 	             \n\t"
	"vfmadd231pd       %%xmm9 , %%xmm13, %%xmm5     \n\t"  // x_r * y_i, x_i * y_r
	"vfmadd231pd       %%xmm10, %%xmm14, %%xmm6     \n\t"  // x_r * y_i, x_i * y_r
	"subq	        $4 , %1			             \n\t"		
	"vfmadd231pd       %%xmm11, %%xmm15, %%xmm7     \n\t"  // x_r * y_i, x_i * y_r

	"jnz		1b		             \n\t"

	"vaddpd        %%xmm0, %%xmm1, %%xmm0	\n\t"
	"vaddpd        %%xmm2, %%xmm3, %%xmm2	\n\t"
	"vaddpd        %%xmm0, %%xmm2, %%xmm0	\n\t"

	"vaddpd        %%xmm4, %%xmm5, %%xmm4	\n\t"
	"vaddpd        %%xmm6, %%xmm7, %%xmm6	\n\t"
	"vaddpd        %%xmm4, %%xmm6, %%xmm4	\n\t"

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
	"vxorpd		%%xmm0, %%xmm0, %%xmm0	             \n\t"
	"vxorpd		%%xmm1, %%xmm1, %%xmm1	             \n\t"
	"vxorpd		%%xmm2, %%xmm2, %%xmm2	             \n\t"
	"vxorpd		%%xmm3, %%xmm3, %%xmm3	             \n\t"
	"vxorpd		%%xmm4, %%xmm4, %%xmm4	             \n\t"
	"vxorpd		%%xmm5, %%xmm5, %%xmm5	             \n\t"
	"vxorpd		%%xmm6, %%xmm6, %%xmm6	             \n\t"
	"vxorpd		%%xmm7, %%xmm7, %%xmm7	             \n\t"

	".align 16			             \n\t"
	"1:				             \n\t"
	"prefetcht0            512(%2,%0,8)		     \n\t"
        "vmovups                  (%2,%0,8), %%xmm8          \n\t"  // 1 * x
        "vmovups                16(%2,%0,8), %%xmm9          \n\t"  // 1 * x

	"prefetcht0            512(%3,%0,8)		     \n\t"
        "vmovups                  (%3,%0,8), %%xmm12         \n\t"  // 1 * y
        "vmovups                16(%3,%0,8), %%xmm13         \n\t"  // 1 * y

        "vmovups                32(%2,%0,8), %%xmm10         \n\t"  // 1 * x
        "vmovups                48(%2,%0,8), %%xmm11         \n\t"  // 1 * x

        "vmovups                32(%3,%0,8), %%xmm14         \n\t"  // 1 * y
        "vmovups                48(%3,%0,8), %%xmm15         \n\t"  // 1 * y

	"vfmadd231pd       %%xmm8 , %%xmm12, %%xmm0     \n\t"  // x_r * y_r, x_i * y_i
	"vfmadd231pd       %%xmm9 , %%xmm13, %%xmm1     \n\t"  // x_r * y_r, x_i * y_i
	"vpermilpd      $0x1 , %%xmm13, %%xmm13               \n\t"
	"vpermilpd      $0x1 , %%xmm12, %%xmm12               \n\t"
	"vfmadd231pd       %%xmm10, %%xmm14, %%xmm2     \n\t"  // x_r * y_r, x_i * y_i
	"vfmadd231pd       %%xmm11, %%xmm15, %%xmm3     \n\t"  // x_r * y_r, x_i * y_i
	"vpermilpd      $0x1 , %%xmm14, %%xmm14               \n\t"
	"vpermilpd      $0x1 , %%xmm15, %%xmm15               \n\t"

	"vfmadd231pd       %%xmm8 , %%xmm12, %%xmm4     \n\t"  // x_r * y_i, x_i * y_r
	"addq		$8 , %0	  	 	             \n\t"
	"vfmadd231pd       %%xmm9 , %%xmm13, %%xmm5     \n\t"  // x_r * y_i, x_i * y_r
	"vfmadd231pd       %%xmm10, %%xmm14, %%xmm6     \n\t"  // x_r * y_i, x_i * y_r
	"subq	        $4 , %1			             \n\t"		
	"vfmadd231pd       %%xmm11, %%xmm15, %%xmm7     \n\t"  // x_r * y_i, x_i * y_r

	"jnz		1b		             \n\t"

	"vaddpd        %%xmm0, %%xmm1, %%xmm0	\n\t"
	"vaddpd        %%xmm2, %%xmm3, %%xmm2	\n\t"
	"vaddpd        %%xmm0, %%xmm2, %%xmm0	\n\t"

	"vaddpd        %%xmm4, %%xmm5, %%xmm4	\n\t"
	"vaddpd        %%xmm6, %%xmm7, %%xmm6	\n\t"
	"vaddpd        %%xmm4, %%xmm6, %%xmm4	\n\t"

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


