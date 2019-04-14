/***************************************************************************
Copyright (c) 2014, The OpenBLAS Project
All rights reserved.
Redistribution and use in source and binary froms, with or without
modification, are permitted provided that the following conditions are
met:
1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
2. Redistributions in binary from must reproduce the above copyright
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
static void zgemv_kernel_4x4( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y, FLOAT *alpha) __attribute__ ((noinline));

static void zgemv_kernel_4x4( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y, FLOAT *alpha)
{

	BLASLONG register i = 0;

	__asm__  __volatile__
	(
	"vzeroupper			 \n\t"

	"vxorpd		%%xmm8 , %%xmm8 , %%xmm8 	\n\t" // temp
	"vxorpd		%%xmm9 , %%xmm9 , %%xmm9 	\n\t" // temp
	"vxorpd		%%xmm10, %%xmm10, %%xmm10	\n\t" // temp
	"vxorpd		%%xmm11, %%xmm11, %%xmm11	\n\t" // temp
	"vxorpd		%%xmm12, %%xmm12, %%xmm12	\n\t" // temp
	"vxorpd		%%xmm13, %%xmm13, %%xmm13	\n\t"
	"vxorpd		%%xmm14, %%xmm14, %%xmm14	\n\t"
	"vxorpd		%%xmm15, %%xmm15, %%xmm15	\n\t"

	".align 16				        \n\t"
	"1:				        \n\t"

	"vmovddup	   (%2,%0,8), %%xmm0            \n\t"  // real value from x0
	"vmovddup	  8(%2,%0,8), %%xmm1            \n\t"  // imag value from x0

	"prefetcht0  192(%4,%0,8)                       \n\t"
	"vmovups	(%4,%0,8), %%xmm4	        \n\t" // 1 complex values from a0
	"prefetcht0  192(%5,%0,8)                       \n\t"
	"vmovups	(%5,%0,8), %%xmm5               \n\t" // 1 complex values from a1
	"prefetcht0  192(%6,%0,8)                       \n\t"
	"vmovups	(%6,%0,8), %%xmm6	        \n\t" // 1 complex values from a2
	"prefetcht0  192(%7,%0,8)                       \n\t"
	"vmovups	(%7,%0,8), %%xmm7               \n\t" // 1 complex values from a3

	"vfmaddpd   %%xmm8 ,   %%xmm4 , %%xmm0, %%xmm8       \n\t" // ar0*xr0,al0*xr0 
	"vfmaddpd   %%xmm9 ,   %%xmm4 , %%xmm1, %%xmm9       \n\t" // ar0*xl0,al0*xl0 
	"vfmaddpd   %%xmm10,   %%xmm5 , %%xmm0, %%xmm10      \n\t" // ar0*xr0,al0*xr0
	"vfmaddpd   %%xmm11,   %%xmm5 , %%xmm1, %%xmm11      \n\t" // ar0*xl0,al0*xl0 
	"vfmaddpd   %%xmm12,   %%xmm6 , %%xmm0, %%xmm12      \n\t" // ar0*xr0,al0*xr0 
	"vfmaddpd   %%xmm13,   %%xmm6 , %%xmm1, %%xmm13      \n\t" // ar0*xl0,al0*xl0 
	"vfmaddpd   %%xmm14,   %%xmm7 , %%xmm0, %%xmm14      \n\t" // ar0*xr0,al0*xr0 
	"vfmaddpd   %%xmm15,   %%xmm7 , %%xmm1, %%xmm15      \n\t" // ar0*xl0,al0*xl0 

	"vmovddup	 16(%2,%0,8), %%xmm0            \n\t"  // real value from x0
	"vmovddup	 24(%2,%0,8), %%xmm1            \n\t"  // imag value from x0

	"vmovups      16(%4,%0,8), %%xmm4	        \n\t" // 1 complex values from a0
	"vmovups      16(%5,%0,8), %%xmm5               \n\t" // 1 complex values from a1
	"vmovups      16(%6,%0,8), %%xmm6	        \n\t" // 1 complex values from a2
	"vmovups      16(%7,%0,8), %%xmm7               \n\t" // 1 complex values from a3

	"vfmaddpd   %%xmm8 ,   %%xmm4 , %%xmm0, %%xmm8       \n\t" // ar0*xr0,al0*xr0 
	"vfmaddpd   %%xmm9 ,   %%xmm4 , %%xmm1, %%xmm9       \n\t" // ar0*xl0,al0*xl0 
	"vfmaddpd   %%xmm10,   %%xmm5 , %%xmm0, %%xmm10      \n\t" // ar0*xr0,al0*xr0
	"vfmaddpd   %%xmm11,   %%xmm5 , %%xmm1, %%xmm11      \n\t" // ar0*xl0,al0*xl0 
	"vfmaddpd   %%xmm12,   %%xmm6 , %%xmm0, %%xmm12      \n\t" // ar0*xr0,al0*xr0 
	"vfmaddpd   %%xmm13,   %%xmm6 , %%xmm1, %%xmm13      \n\t" // ar0*xl0,al0*xl0 
	"vfmaddpd   %%xmm14,   %%xmm7 , %%xmm0, %%xmm14      \n\t" // ar0*xr0,al0*xr0 
	"vfmaddpd   %%xmm15,   %%xmm7 , %%xmm1, %%xmm15      \n\t" // ar0*xl0,al0*xl0 

	"vmovddup	 32(%2,%0,8), %%xmm0            \n\t"  // real value from x0
	"vmovddup	 40(%2,%0,8), %%xmm1            \n\t"  // imag value from x0

	"vmovups      32(%4,%0,8), %%xmm4	        \n\t" // 1 complex values from a0
	"vmovups      32(%5,%0,8), %%xmm5               \n\t" // 1 complex values from a1
	"vmovups      32(%6,%0,8), %%xmm6	        \n\t" // 1 complex values from a2
	"vmovups      32(%7,%0,8), %%xmm7               \n\t" // 1 complex values from a3

	"vfmaddpd   %%xmm8 ,   %%xmm4 , %%xmm0, %%xmm8       \n\t" // ar0*xr0,al0*xr0 
	"vfmaddpd   %%xmm9 ,   %%xmm4 , %%xmm1, %%xmm9       \n\t" // ar0*xl0,al0*xl0 
	"vfmaddpd   %%xmm10,   %%xmm5 , %%xmm0, %%xmm10      \n\t" // ar0*xr0,al0*xr0
	"vfmaddpd   %%xmm11,   %%xmm5 , %%xmm1, %%xmm11      \n\t" // ar0*xl0,al0*xl0 
	"vfmaddpd   %%xmm12,   %%xmm6 , %%xmm0, %%xmm12      \n\t" // ar0*xr0,al0*xr0 
	"vfmaddpd   %%xmm13,   %%xmm6 , %%xmm1, %%xmm13      \n\t" // ar0*xl0,al0*xl0 
	"vfmaddpd   %%xmm14,   %%xmm7 , %%xmm0, %%xmm14      \n\t" // ar0*xr0,al0*xr0 
	"vfmaddpd   %%xmm15,   %%xmm7 , %%xmm1, %%xmm15      \n\t" // ar0*xl0,al0*xl0 

	"vmovddup	 48(%2,%0,8), %%xmm0            \n\t"  // real value from x0
	"vmovddup	 56(%2,%0,8), %%xmm1            \n\t"  // imag value from x0

	"vmovups      48(%4,%0,8), %%xmm4	        \n\t" // 1 complex values from a0
	"vmovups      48(%5,%0,8), %%xmm5               \n\t" // 1 complex values from a1
	"vmovups      48(%6,%0,8), %%xmm6	        \n\t" // 1 complex values from a2
	"vmovups      48(%7,%0,8), %%xmm7               \n\t" // 1 complex values from a3

	"vfmaddpd   %%xmm8 ,   %%xmm4 , %%xmm0, %%xmm8       \n\t" // ar0*xr0,al0*xr0 
	"vfmaddpd   %%xmm9 ,   %%xmm4 , %%xmm1, %%xmm9       \n\t" // ar0*xl0,al0*xl0 
	"vfmaddpd   %%xmm10,   %%xmm5 , %%xmm0, %%xmm10      \n\t" // ar0*xr0,al0*xr0
	"vfmaddpd   %%xmm11,   %%xmm5 , %%xmm1, %%xmm11      \n\t" // ar0*xl0,al0*xl0 
	"vfmaddpd   %%xmm12,   %%xmm6 , %%xmm0, %%xmm12      \n\t" // ar0*xr0,al0*xr0 
	"vfmaddpd   %%xmm13,   %%xmm6 , %%xmm1, %%xmm13      \n\t" // ar0*xl0,al0*xl0 
	"vfmaddpd   %%xmm14,   %%xmm7 , %%xmm0, %%xmm14      \n\t" // ar0*xr0,al0*xr0 
	"vfmaddpd   %%xmm15,   %%xmm7 , %%xmm1, %%xmm15      \n\t" // ar0*xl0,al0*xl0 

        "addq		$8 , %0	  	 	        \n\t"
	"subq	        $4 , %1			        \n\t"		
	"jnz		1b		        \n\t"

	"vmovddup               (%8)  , %%xmm0                \n\t"  // value from alpha
	"vmovddup	       8(%8)  , %%xmm1                \n\t"  // value from alpha

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        "vpermilpd      $0x1 , %%xmm9 , %%xmm9                \n\t"
        "vpermilpd      $0x1 , %%xmm11, %%xmm11               \n\t"
        "vpermilpd      $0x1 , %%xmm13, %%xmm13               \n\t"
        "vpermilpd      $0x1 , %%xmm15, %%xmm15               \n\t"
        "vaddsubpd      %%xmm9 , %%xmm8, %%xmm8               \n\t" 
        "vaddsubpd      %%xmm11, %%xmm10, %%xmm10             \n\t"
        "vaddsubpd      %%xmm13, %%xmm12, %%xmm12             \n\t"
        "vaddsubpd      %%xmm15, %%xmm14, %%xmm14             \n\t"
#else
        "vpermilpd      $0x1 , %%xmm8 , %%xmm8                \n\t"
        "vpermilpd      $0x1 , %%xmm10, %%xmm10               \n\t"
        "vpermilpd      $0x1 , %%xmm12, %%xmm12               \n\t"
        "vpermilpd      $0x1 , %%xmm14, %%xmm14               \n\t"
        "vaddsubpd      %%xmm8 , %%xmm9 , %%xmm8              \n\t"
        "vaddsubpd      %%xmm10, %%xmm11, %%xmm10             \n\t"
        "vaddsubpd      %%xmm12, %%xmm13, %%xmm12             \n\t"
        "vaddsubpd      %%xmm14, %%xmm15, %%xmm14             \n\t"
        "vpermilpd      $0x1 , %%xmm8 , %%xmm8                \n\t"
        "vpermilpd      $0x1 , %%xmm10, %%xmm10               \n\t"
        "vpermilpd      $0x1 , %%xmm12, %%xmm12               \n\t"
        "vpermilpd      $0x1 , %%xmm14, %%xmm14               \n\t"
#endif

	"vmulpd		%%xmm8 , %%xmm1 , %%xmm9              \n\t"  // t_r * alpha_i , t_i * alpha_i
	"vmulpd		%%xmm8 , %%xmm0 , %%xmm8              \n\t"  // t_r * alpha_r , t_i * alpha_r
	"vmulpd		%%xmm10, %%xmm1 , %%xmm11             \n\t"  // t_r * alpha_i , t_i * alpha_i
	"vmulpd		%%xmm10, %%xmm0 , %%xmm10             \n\t"  // t_r * alpha_r , t_i * alpha_r
	"vmulpd		%%xmm12, %%xmm1 , %%xmm13             \n\t"  // t_r * alpha_i , t_i * alpha_i
	"vmulpd		%%xmm12, %%xmm0 , %%xmm12             \n\t"  // t_r * alpha_r , t_i * alpha_r
	"vmulpd		%%xmm14, %%xmm1 , %%xmm15             \n\t"  // t_r * alpha_i , t_i * alpha_i
	"vmulpd		%%xmm14, %%xmm0 , %%xmm14             \n\t"  // t_r * alpha_r , t_i * alpha_r

#if !defined(XCONJ) 
        "vpermilpd      $0x1 , %%xmm9 , %%xmm9                \n\t"
        "vpermilpd      $0x1 , %%xmm11, %%xmm11               \n\t"
        "vpermilpd      $0x1 , %%xmm13, %%xmm13               \n\t"
        "vpermilpd      $0x1 , %%xmm15, %%xmm15               \n\t"
        "vaddsubpd      %%xmm9 , %%xmm8, %%xmm8               \n\t" 
        "vaddsubpd      %%xmm11, %%xmm10, %%xmm10             \n\t"
        "vaddsubpd      %%xmm13, %%xmm12, %%xmm12             \n\t"
        "vaddsubpd      %%xmm15, %%xmm14, %%xmm14             \n\t"
#else
        "vpermilpd      $0x1 , %%xmm8 , %%xmm8                \n\t"
        "vpermilpd      $0x1 , %%xmm10, %%xmm10               \n\t"
        "vpermilpd      $0x1 , %%xmm12, %%xmm12               \n\t"
        "vpermilpd      $0x1 , %%xmm14, %%xmm14               \n\t"
        "vaddsubpd      %%xmm8 , %%xmm9 , %%xmm8              \n\t"
        "vaddsubpd      %%xmm10, %%xmm11, %%xmm10             \n\t"
        "vaddsubpd      %%xmm12, %%xmm13, %%xmm12             \n\t"
        "vaddsubpd      %%xmm14, %%xmm15, %%xmm14             \n\t"
        "vpermilpd      $0x1 , %%xmm8 , %%xmm8                \n\t"
        "vpermilpd      $0x1 , %%xmm10, %%xmm10               \n\t"
        "vpermilpd      $0x1 , %%xmm12, %%xmm12               \n\t"
        "vpermilpd      $0x1 , %%xmm14, %%xmm14               \n\t"
#endif

	"vaddpd           (%3) , %%xmm8 , %%xmm8              \n\t"
	"vaddpd         16(%3) , %%xmm10, %%xmm10             \n\t"
	"vaddpd         32(%3) , %%xmm12, %%xmm12             \n\t"
	"vaddpd         48(%3) , %%xmm14, %%xmm14             \n\t"

	"vmovups	%%xmm8 ,   (%3)			\n\t"
	"vmovups	%%xmm10, 16(%3)			\n\t"
	"vmovups	%%xmm12, 32(%3)			\n\t"
	"vmovups	%%xmm14, 48(%3)			\n\t"

	"vzeroupper			 \n\t"

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
	  "%xmm0", "%xmm1", "%xmm2", "%xmm3", 
	  "%xmm4", "%xmm5", "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11", 
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

} 

#define HAVE_KERNEL_4x2 1
static void zgemv_kernel_4x2( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y, FLOAT *alpha) __attribute__ ((noinline));

static void zgemv_kernel_4x2( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y, FLOAT *alpha)
{

	BLASLONG register i = 0;

	__asm__  __volatile__
	(
	"vzeroupper			 \n\t"

	"vxorpd		%%xmm8 , %%xmm8 , %%xmm8 	\n\t" // temp
	"vxorpd		%%xmm9 , %%xmm9 , %%xmm9 	\n\t" // temp
	"vxorpd		%%xmm10, %%xmm10, %%xmm10	\n\t" // temp
	"vxorpd		%%xmm11, %%xmm11, %%xmm11	\n\t" // temp

	".align 16				        \n\t"
	"1:				        \n\t"

	"vmovddup	   (%2,%0,8), %%xmm0            \n\t"  // real value from x0
	"vmovddup	  8(%2,%0,8), %%xmm1            \n\t"  // imag value from x0

	"prefetcht0  192(%4,%0,8)                       \n\t"
	"vmovups	(%4,%0,8), %%xmm4	        \n\t" // 1 complex values from a0
	"prefetcht0  192(%5,%0,8)                       \n\t"
	"vmovups	(%5,%0,8), %%xmm5               \n\t" // 1 complex values from a1

	"vfmaddpd   %%xmm8 ,   %%xmm4 , %%xmm0, %%xmm8       \n\t" // ar0*xr0,al0*xr0 
	"vfmaddpd   %%xmm9 ,   %%xmm4 , %%xmm1, %%xmm9       \n\t" // ar0*xl0,al0*xl0 
	"vfmaddpd   %%xmm10,   %%xmm5 , %%xmm0, %%xmm10      \n\t" // ar0*xr0,al0*xr0
	"vfmaddpd   %%xmm11,   %%xmm5 , %%xmm1, %%xmm11      \n\t" // ar0*xl0,al0*xl0 

	"vmovddup	 16(%2,%0,8), %%xmm0            \n\t"  // real value from x0
	"vmovddup	 24(%2,%0,8), %%xmm1            \n\t"  // imag value from x0

	"vmovups      16(%4,%0,8), %%xmm4	        \n\t" // 1 complex values from a0
	"vmovups      16(%5,%0,8), %%xmm5               \n\t" // 1 complex values from a1

	"vfmaddpd   %%xmm8 ,   %%xmm4 , %%xmm0, %%xmm8       \n\t" // ar0*xr0,al0*xr0 
	"vfmaddpd   %%xmm9 ,   %%xmm4 , %%xmm1, %%xmm9       \n\t" // ar0*xl0,al0*xl0 
	"vfmaddpd   %%xmm10,   %%xmm5 , %%xmm0, %%xmm10      \n\t" // ar0*xr0,al0*xr0
	"vfmaddpd   %%xmm11,   %%xmm5 , %%xmm1, %%xmm11      \n\t" // ar0*xl0,al0*xl0 

	"vmovddup	 32(%2,%0,8), %%xmm0            \n\t"  // real value from x0
	"vmovddup	 40(%2,%0,8), %%xmm1            \n\t"  // imag value from x0

	"vmovups      32(%4,%0,8), %%xmm4	        \n\t" // 1 complex values from a0
	"vmovups      32(%5,%0,8), %%xmm5               \n\t" // 1 complex values from a1

	"vfmaddpd   %%xmm8 ,   %%xmm4 , %%xmm0, %%xmm8       \n\t" // ar0*xr0,al0*xr0 
	"vfmaddpd   %%xmm9 ,   %%xmm4 , %%xmm1, %%xmm9       \n\t" // ar0*xl0,al0*xl0 
	"vfmaddpd   %%xmm10,   %%xmm5 , %%xmm0, %%xmm10      \n\t" // ar0*xr0,al0*xr0
	"vfmaddpd   %%xmm11,   %%xmm5 , %%xmm1, %%xmm11      \n\t" // ar0*xl0,al0*xl0 

	"vmovddup	 48(%2,%0,8), %%xmm0            \n\t"  // real value from x0
	"vmovddup	 56(%2,%0,8), %%xmm1            \n\t"  // imag value from x0

	"vmovups      48(%4,%0,8), %%xmm4	        \n\t" // 1 complex values from a0
	"vmovups      48(%5,%0,8), %%xmm5               \n\t" // 1 complex values from a1

	"vfmaddpd   %%xmm8 ,   %%xmm4 , %%xmm0, %%xmm8       \n\t" // ar0*xr0,al0*xr0 
	"vfmaddpd   %%xmm9 ,   %%xmm4 , %%xmm1, %%xmm9       \n\t" // ar0*xl0,al0*xl0 
	"vfmaddpd   %%xmm10,   %%xmm5 , %%xmm0, %%xmm10      \n\t" // ar0*xr0,al0*xr0
	"vfmaddpd   %%xmm11,   %%xmm5 , %%xmm1, %%xmm11      \n\t" // ar0*xl0,al0*xl0 

        "addq		$8 , %0	  	 	        \n\t"
	"subq	        $4 , %1			        \n\t"		
	"jnz		1b		        \n\t"

	"vmovddup               (%6)  , %%xmm0                \n\t"  // value from alpha
	"vmovddup	       8(%6)  , %%xmm1                \n\t"  // value from alpha

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        "vpermilpd      $0x1 , %%xmm9 , %%xmm9                \n\t"
        "vpermilpd      $0x1 , %%xmm11, %%xmm11               \n\t"
        "vaddsubpd      %%xmm9 , %%xmm8, %%xmm8               \n\t" 
        "vaddsubpd      %%xmm11, %%xmm10, %%xmm10             \n\t"
#else
        "vpermilpd      $0x1 , %%xmm8 , %%xmm8                \n\t"
        "vpermilpd      $0x1 , %%xmm10, %%xmm10               \n\t"
        "vaddsubpd      %%xmm8 , %%xmm9 , %%xmm8              \n\t"
        "vaddsubpd      %%xmm10, %%xmm11, %%xmm10             \n\t"
        "vpermilpd      $0x1 , %%xmm8 , %%xmm8                \n\t"
        "vpermilpd      $0x1 , %%xmm10, %%xmm10               \n\t"
#endif

	"vmulpd		%%xmm8 , %%xmm1 , %%xmm9              \n\t"  // t_r * alpha_i , t_i * alpha_i
	"vmulpd		%%xmm8 , %%xmm0 , %%xmm8              \n\t"  // t_r * alpha_r , t_i * alpha_r
	"vmulpd		%%xmm10, %%xmm1 , %%xmm11             \n\t"  // t_r * alpha_i , t_i * alpha_i
	"vmulpd		%%xmm10, %%xmm0 , %%xmm10             \n\t"  // t_r * alpha_r , t_i * alpha_r

#if !defined(XCONJ) 
        "vpermilpd      $0x1 , %%xmm9 , %%xmm9                \n\t"
        "vpermilpd      $0x1 , %%xmm11, %%xmm11               \n\t"
        "vaddsubpd      %%xmm9 , %%xmm8, %%xmm8               \n\t" 
        "vaddsubpd      %%xmm11, %%xmm10, %%xmm10             \n\t"
#else
        "vpermilpd      $0x1 , %%xmm8 , %%xmm8                \n\t"
        "vpermilpd      $0x1 , %%xmm10, %%xmm10               \n\t"
        "vaddsubpd      %%xmm8 , %%xmm9 , %%xmm8              \n\t"
        "vaddsubpd      %%xmm10, %%xmm11, %%xmm10             \n\t"
        "vpermilpd      $0x1 , %%xmm8 , %%xmm8                \n\t"
        "vpermilpd      $0x1 , %%xmm10, %%xmm10               \n\t"
#endif

	"vaddpd           (%3) , %%xmm8 , %%xmm8              \n\t"
	"vaddpd         16(%3) , %%xmm10, %%xmm10             \n\t"

	"vmovups	%%xmm8 ,   (%3)			\n\t"
	"vmovups	%%xmm10, 16(%3)			\n\t"

	"vzeroupper			 \n\t"

	:
          "+r" (i),	// 0	
	  "+r" (n)  	// 1
        : 
          "r" (x),      // 2
          "r" (y),      // 3
          "r" (ap[0]),  // 4
          "r" (ap[1]),  // 5
          "r" (alpha)   // 6
	: "cc", 
	  "%xmm0", "%xmm1", "%xmm2", "%xmm3", 
	  "%xmm4", "%xmm5", "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11", 
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

} 



#define HAVE_KERNEL_4x1 1
static void zgemv_kernel_4x1( BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT *alpha) __attribute__ ((noinline));

static void zgemv_kernel_4x1( BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT *alpha)
{

	BLASLONG register i = 0;

	__asm__  __volatile__
	(
	"vzeroupper			 \n\t"

	"vxorpd		%%xmm8 , %%xmm8 , %%xmm8 	\n\t" // temp
	"vxorpd		%%xmm9 , %%xmm9 , %%xmm9 	\n\t" // temp

	".align 16				        \n\t"
	"1:				        \n\t"

	"vmovddup	   (%2,%0,8), %%xmm0            \n\t"  // real value from x0
	"vmovddup	  8(%2,%0,8), %%xmm1            \n\t"  // imag value from x0

	"prefetcht0  192(%4,%0,8)                       \n\t"
	"vmovups	(%4,%0,8), %%xmm4	        \n\t" // 1 complex values from a0
	"vmovups      16(%4,%0,8), %%xmm5	        \n\t" // 1 complex values from a0

	"vmovddup	 16(%2,%0,8), %%xmm2            \n\t"  // real value from x0
	"vmovddup	 24(%2,%0,8), %%xmm3            \n\t"  // imag value from x0

	"vfmaddpd   %%xmm8 ,   %%xmm4 , %%xmm0, %%xmm8       \n\t" // ar0*xr0,al0*xr0 
	"vfmaddpd   %%xmm9 ,   %%xmm4 , %%xmm1, %%xmm9       \n\t" // ar0*xl0,al0*xl0 

	"vmovddup	 32(%2,%0,8), %%xmm0            \n\t"  // real value from x0
	"vmovddup	 40(%2,%0,8), %%xmm1            \n\t"  // imag value from x0

	"vfmaddpd   %%xmm8 ,   %%xmm5 , %%xmm2, %%xmm8       \n\t" // ar0*xr0,al0*xr0 
	"vfmaddpd   %%xmm9 ,   %%xmm5 , %%xmm3, %%xmm9       \n\t" // ar0*xl0,al0*xl0 

	"vmovups      32(%4,%0,8), %%xmm4	        \n\t" // 1 complex values from a0
	"vmovups      48(%4,%0,8), %%xmm5	        \n\t" // 1 complex values from a0

	"vmovddup	 48(%2,%0,8), %%xmm2            \n\t"  // real value from x0
	"vmovddup	 56(%2,%0,8), %%xmm3            \n\t"  // imag value from x0

        "addq		$8 , %0	  	 	        \n\t"
	"vfmaddpd   %%xmm8 ,   %%xmm4 , %%xmm0, %%xmm8       \n\t" // ar0*xr0,al0*xr0 
	"vfmaddpd   %%xmm9 ,   %%xmm4 , %%xmm1, %%xmm9       \n\t" // ar0*xl0,al0*xl0 

	"subq	        $4 , %1			        \n\t"		
	"vfmaddpd   %%xmm8 ,   %%xmm5 , %%xmm2, %%xmm8       \n\t" // ar0*xr0,al0*xr0 
	"vfmaddpd   %%xmm9 ,   %%xmm5 , %%xmm3, %%xmm9       \n\t" // ar0*xl0,al0*xl0 

	"jnz		1b		        \n\t"

	"vmovddup               (%5)  , %%xmm0                \n\t"  // value from alpha
	"vmovddup	       8(%5)  , %%xmm1                \n\t"  // value from alpha

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        "vpermilpd      $0x1 , %%xmm9 , %%xmm9                \n\t"
        "vaddsubpd      %%xmm9 , %%xmm8, %%xmm8               \n\t" 
#else
        "vpermilpd      $0x1 , %%xmm8 , %%xmm8                \n\t"
        "vaddsubpd      %%xmm8 , %%xmm9 , %%xmm8              \n\t"
        "vpermilpd      $0x1 , %%xmm8 , %%xmm8                \n\t"
#endif

	"vmulpd		%%xmm8 , %%xmm1 , %%xmm9              \n\t"  // t_r * alpha_i , t_i * alpha_i
	"vmulpd		%%xmm8 , %%xmm0 , %%xmm8              \n\t"  // t_r * alpha_r , t_i * alpha_r

#if !defined(XCONJ) 
        "vpermilpd      $0x1 , %%xmm9 , %%xmm9                \n\t"
        "vaddsubpd      %%xmm9 , %%xmm8, %%xmm8               \n\t" 
#else
        "vpermilpd      $0x1 , %%xmm8 , %%xmm8                \n\t"
        "vaddsubpd      %%xmm8 , %%xmm9 , %%xmm8              \n\t"
        "vpermilpd      $0x1 , %%xmm8 , %%xmm8                \n\t"
#endif

	"vaddpd           (%3) , %%xmm8 , %%xmm8              \n\t"

	"vmovups	%%xmm8 ,   (%3)			\n\t"

	"vzeroupper			 \n\t"

	:
          "+r" (i),	// 0	
	  "+r" (n)  	// 1
        : 
          "r" (x),      // 2
          "r" (y),      // 3
          "r" (ap),     // 4
          "r" (alpha)   // 5
	: "cc", 
	  "%xmm0", "%xmm1", "%xmm2", "%xmm3", 
	  "%xmm4", "%xmm5", "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11", 
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

} 


