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
static void cgemv_kernel_4x4( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y, FLOAT *alpha) __attribute__ ((noinline));

static void cgemv_kernel_4x4( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y, FLOAT *alpha)
{

	BLASLONG register i = 0;

	__asm__  __volatile__
	(
	"vzeroupper			 \n\t"

	"vxorps		%%ymm8 , %%ymm8 , %%ymm8 	\n\t" // temp
	"vxorps		%%ymm9 , %%ymm9 , %%ymm9 	\n\t" // temp
	"vxorps		%%ymm10, %%ymm10, %%ymm10	\n\t" // temp
	"vxorps		%%ymm11, %%ymm11, %%ymm11	\n\t" // temp
	"vxorps		%%ymm12, %%ymm12, %%ymm12	\n\t" // temp
	"vxorps		%%ymm13, %%ymm13, %%ymm13	\n\t"
	"vxorps		%%ymm14, %%ymm14, %%ymm14	\n\t"
	"vxorps		%%ymm15, %%ymm15, %%ymm15	\n\t"

        "testq          $0x04, %1                      \n\t"
        "jz             2f                      \n\t"

	"vmovups	(%4,%0,4), %%ymm4	        \n\t" // 4 complex values from a0
	"vmovups	(%5,%0,4), %%ymm5               \n\t" // 4 complex values from a1

	"vmovups	    (%2,%0,4)  , %%ymm6		\n\t" // 4 complex values from x
	"vpermilps        $0xb1, %%ymm6, %%ymm7		\n\t" // exchange real and imap parts
	"vblendps $0x55, %%ymm6, %%ymm7, %%ymm0         \n\t" // only the real parts
	"vblendps $0x55, %%ymm7, %%ymm6, %%ymm1         \n\t" // only the imag parts
	
	"vmovups	(%6,%0,4), %%ymm6	        \n\t" // 4 complex values from a2
	"vmovups	(%7,%0,4), %%ymm7               \n\t" // 4 complex values from a3

	"vfmaddps      %%ymm8 , %%ymm4 , %%ymm0, %%ymm8       \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm9 , %%ymm4 , %%ymm1, %%ymm9       \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 
	"vfmaddps      %%ymm10, %%ymm5 , %%ymm0, %%ymm10      \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm11, %%ymm5 , %%ymm1, %%ymm11      \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 
	"vfmaddps      %%ymm12, %%ymm6 , %%ymm0, %%ymm12      \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm13, %%ymm6 , %%ymm1, %%ymm13      \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 
	"vfmaddps      %%ymm14, %%ymm7 , %%ymm0, %%ymm14      \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm15, %%ymm7 , %%ymm1, %%ymm15      \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 
	
        "addq		$8  , %0	  	 	        \n\t"
	"subq	        $4  , %1			        \n\t"		

        "2:                                  \n\t"
	"cmpq           $0, %1                         \n\t"
        "je             3f                      \n\t"

	//	".align 16				        \n\t"
	"1:				        \n\t"
        "prefetcht0      384(%4,%0,4)                   \n\t"
	"vmovups	(%4,%0,4), %%ymm4	        \n\t" // 4 complex values from a0
        "prefetcht0      384(%5,%0,4)                   \n\t"
	"vmovups	(%5,%0,4), %%ymm5               \n\t" // 4 complex values from a1

        "prefetcht0      384(%2,%0,4)                   \n\t"
	"vmovups	    (%2,%0,4)  , %%ymm6		\n\t" // 4 complex values from x
	"vpermilps        $0xb1, %%ymm6, %%ymm7		\n\t" // exchange real and imap parts
	"vblendps $0x55, %%ymm6, %%ymm7, %%ymm0         \n\t" // only the real parts
	"vblendps $0x55, %%ymm7, %%ymm6, %%ymm1         \n\t" // only the imag parts
	
        "prefetcht0      384(%6,%0,4)                   \n\t"
	"vmovups	(%6,%0,4), %%ymm6	        \n\t" // 4 complex values from a2
        "prefetcht0      384(%7,%0,4)                   \n\t"
	"vmovups	(%7,%0,4), %%ymm7               \n\t" // 4 complex values from a3

	"vfmaddps      %%ymm8 , %%ymm4 , %%ymm0, %%ymm8       \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm9 , %%ymm4 , %%ymm1, %%ymm9       \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 
	"vfmaddps      %%ymm10, %%ymm5 , %%ymm0, %%ymm10      \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm11, %%ymm5 , %%ymm1, %%ymm11      \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 
	"vfmaddps      %%ymm12, %%ymm6 , %%ymm0, %%ymm12      \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm13, %%ymm6 , %%ymm1, %%ymm13      \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 
	"vfmaddps      %%ymm14, %%ymm7 , %%ymm0, %%ymm14      \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm15, %%ymm7 , %%ymm1, %%ymm15      \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 
	

	"vmovups       32(%4,%0,4), %%ymm4	        \n\t" // 4 complex values from a0
	"vmovups       32(%5,%0,4), %%ymm5              \n\t" // 4 complex values from a1

	"vmovups	  32(%2,%0,4)  , %%ymm6		\n\t" // 4 complex values from x
	"vpermilps        $0xb1, %%ymm6, %%ymm7		\n\t" // exchange real and imap parts
	"vblendps $0x55, %%ymm6, %%ymm7, %%ymm0         \n\t" // only the real parts
	"vblendps $0x55, %%ymm7, %%ymm6, %%ymm1         \n\t" // only the imag parts

	"vmovups       32(%6,%0,4), %%ymm6	        \n\t" // 4 complex values from a2
	"vmovups       32(%7,%0,4), %%ymm7              \n\t" // 4 complex values from a3

	"vfmaddps      %%ymm8 , %%ymm4 , %%ymm0, %%ymm8       \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm9 , %%ymm4 , %%ymm1, %%ymm9       \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 
	"vfmaddps      %%ymm10, %%ymm5 , %%ymm0, %%ymm10      \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm11, %%ymm5 , %%ymm1, %%ymm11      \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 
	"vfmaddps      %%ymm12, %%ymm6 , %%ymm0, %%ymm12      \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm13, %%ymm6 , %%ymm1, %%ymm13      \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 
	"vfmaddps      %%ymm14, %%ymm7 , %%ymm0, %%ymm14      \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm15, %%ymm7 , %%ymm1, %%ymm15      \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 
	
        "addq		$16 , %0	  	 	        \n\t"
	"subq	        $8  , %1			        \n\t"		
	"jnz		1b		        \n\t"

        "3:                                   \n\t"

        "vbroadcastss    (%8)  , %%xmm0                \n\t"  // value from alpha
        "vbroadcastss   4(%8)  , %%xmm1                \n\t"  // value from alpha


#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        "vpermilps      $0xb1 , %%ymm9 , %%ymm9                \n\t"
        "vpermilps      $0xb1 , %%ymm11, %%ymm11               \n\t"
        "vpermilps      $0xb1 , %%ymm13, %%ymm13               \n\t"
        "vpermilps      $0xb1 , %%ymm15, %%ymm15               \n\t"
        "vaddsubps      %%ymm9 , %%ymm8, %%ymm8                \n\t" 
        "vaddsubps      %%ymm11, %%ymm10, %%ymm10              \n\t"
        "vaddsubps      %%ymm13, %%ymm12, %%ymm12              \n\t"
        "vaddsubps      %%ymm15, %%ymm14, %%ymm14              \n\t"
#else
        "vpermilps      $0xb1 , %%ymm8 , %%ymm8                \n\t"
        "vpermilps      $0xb1 , %%ymm10, %%ymm10               \n\t"
        "vpermilps      $0xb1 , %%ymm12, %%ymm12               \n\t"
        "vpermilps      $0xb1 , %%ymm14, %%ymm14               \n\t"
        "vaddsubps      %%ymm8 , %%ymm9 , %%ymm8               \n\t"
        "vaddsubps      %%ymm10, %%ymm11, %%ymm10              \n\t"
        "vaddsubps      %%ymm12, %%ymm13, %%ymm12              \n\t"
        "vaddsubps      %%ymm14, %%ymm15, %%ymm14              \n\t"
        "vpermilps      $0xb1 , %%ymm8 , %%ymm8                \n\t"
        "vpermilps      $0xb1 , %%ymm10, %%ymm10               \n\t"
        "vpermilps      $0xb1 , %%ymm12, %%ymm12               \n\t"
        "vpermilps      $0xb1 , %%ymm14, %%ymm14               \n\t"
#endif

	"vmovsd         (%3), %%xmm4			\n\t" // read y
	"vmovsd        8(%3), %%xmm5			\n\t"
	"vmovsd       16(%3), %%xmm6			\n\t"
	"vmovsd       24(%3), %%xmm7			\n\t"

	"vextractf128   $1, %%ymm8 , %%xmm9		      \n\t"
	"vextractf128   $1, %%ymm10, %%xmm11	      	      \n\t"
	"vextractf128   $1, %%ymm12, %%xmm13		      \n\t"
	"vextractf128   $1, %%ymm14, %%xmm15		      \n\t"

	"vaddps		%%xmm8 , %%xmm9 , %%xmm8       \n\t"
	"vaddps		%%xmm10, %%xmm11, %%xmm10      \n\t"
	"vaddps		%%xmm12, %%xmm13, %%xmm12      \n\t"
	"vaddps		%%xmm14, %%xmm15, %%xmm14      \n\t"

	"vshufpd        $0x1, %%xmm8 , %%xmm8 , %%xmm9   \n\t"
	"vshufpd        $0x1, %%xmm10, %%xmm10, %%xmm11  \n\t"
	"vshufpd        $0x1, %%xmm12, %%xmm12, %%xmm13  \n\t"
	"vshufpd        $0x1, %%xmm14, %%xmm14, %%xmm15  \n\t"

	"vaddps		%%xmm8 , %%xmm9 , %%xmm8       \n\t"
	"vaddps		%%xmm10, %%xmm11, %%xmm10      \n\t"
	"vaddps		%%xmm12, %%xmm13, %%xmm12      \n\t"
	"vaddps		%%xmm14, %%xmm15, %%xmm14      \n\t"


        "vmulps         %%xmm8 , %%xmm1 , %%xmm9              \n\t"  // t_r * alpha_i , t_i * alpha_i
        "vmulps         %%xmm8 , %%xmm0 , %%xmm8              \n\t"  // t_r * alpha_r , t_i * alpha_r
        "vmulps         %%xmm10, %%xmm1 , %%xmm11             \n\t"  // t_r * alpha_i , t_i * alpha_i
        "vmulps         %%xmm10, %%xmm0 , %%xmm10             \n\t"  // t_r * alpha_r , t_i * alpha_r
        "vmulps         %%xmm12, %%xmm1 , %%xmm13             \n\t"  // t_r * alpha_i , t_i * alpha_i
        "vmulps         %%xmm12, %%xmm0 , %%xmm12             \n\t"  // t_r * alpha_r , t_i * alpha_r
        "vmulps         %%xmm14, %%xmm1 , %%xmm15             \n\t"  // t_r * alpha_i , t_i * alpha_i
        "vmulps         %%xmm14, %%xmm0 , %%xmm14             \n\t"  // t_r * alpha_r , t_i * alpha_r

#if !defined(XCONJ)
        "vpermilps      $0xb1 , %%xmm9 , %%xmm9                \n\t"
        "vpermilps      $0xb1 , %%xmm11, %%xmm11               \n\t"
        "vpermilps      $0xb1 , %%xmm13, %%xmm13               \n\t"
        "vpermilps      $0xb1 , %%xmm15, %%xmm15               \n\t"
        "vaddsubps      %%xmm9 , %%xmm8, %%xmm8               \n\t"
        "vaddsubps      %%xmm11, %%xmm10, %%xmm10             \n\t"
        "vaddsubps      %%xmm13, %%xmm12, %%xmm12             \n\t"
        "vaddsubps      %%xmm15, %%xmm14, %%xmm14             \n\t"
#else
        "vpermilps      $0xb1 , %%xmm8 , %%xmm8                \n\t"
        "vpermilps      $0xb1 , %%xmm10, %%xmm10               \n\t"
        "vpermilps      $0xb1 , %%xmm12, %%xmm12               \n\t"
        "vpermilps      $0xb1 , %%xmm14, %%xmm14               \n\t"
        "vaddsubps      %%xmm8 , %%xmm9 , %%xmm8              \n\t"
        "vaddsubps      %%xmm10, %%xmm11, %%xmm10             \n\t"
        "vaddsubps      %%xmm12, %%xmm13, %%xmm12             \n\t"
        "vaddsubps      %%xmm14, %%xmm15, %%xmm14             \n\t"
        "vpermilps      $0xb1 , %%xmm8 , %%xmm8                \n\t"
        "vpermilps      $0xb1 , %%xmm10, %%xmm10               \n\t"
        "vpermilps      $0xb1 , %%xmm12, %%xmm12               \n\t"
        "vpermilps      $0xb1 , %%xmm14, %%xmm14               \n\t"
#endif


	"vaddps		%%xmm8 , %%xmm4 , %%xmm8       \n\t"
	"vaddps		%%xmm10, %%xmm5 , %%xmm10      \n\t"
	"vaddps		%%xmm12, %%xmm6 , %%xmm12      \n\t"
	"vaddps		%%xmm14, %%xmm7 , %%xmm14      \n\t"

	"vmovsd	%%xmm8 ,   (%3)			\n\t"
	"vmovsd	%%xmm10,  8(%3)			\n\t"
	"vmovsd	%%xmm12, 16(%3)			\n\t"
	"vmovsd	%%xmm14, 24(%3)			\n\t"

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
static void cgemv_kernel_4x2( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y, FLOAT *alpha) __attribute__ ((noinline));

static void cgemv_kernel_4x2( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y, FLOAT *alpha)
{

	BLASLONG register i = 0;

	__asm__  __volatile__
	(
	"vzeroupper			 \n\t"

	"vxorps		%%ymm8 , %%ymm8 , %%ymm8 	\n\t" // temp
	"vxorps		%%ymm9 , %%ymm9 , %%ymm9 	\n\t" // temp
	"vxorps		%%ymm10, %%ymm10, %%ymm10	\n\t" // temp
	"vxorps		%%ymm11, %%ymm11, %%ymm11	\n\t" // temp

        "testq          $0x04, %1                      \n\t"
        "jz             2f                    \n\t"

	"vmovups	(%4,%0,4), %%ymm4	        \n\t" // 4 complex values from a0
	"vmovups	(%5,%0,4), %%ymm5               \n\t" // 4 complex values from a1

	"vmovups	    (%2,%0,4)  , %%ymm6		\n\t" // 4 complex values from x
	"vpermilps        $0xb1, %%ymm6, %%ymm7		\n\t" // exchange real and imap parts
	"vblendps $0x55, %%ymm6, %%ymm7, %%ymm0         \n\t" // only the real parts
	"vblendps $0x55, %%ymm7, %%ymm6, %%ymm1         \n\t" // only the imag parts
	

	"vfmaddps      %%ymm8 , %%ymm4 , %%ymm0, %%ymm8       \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm9 , %%ymm4 , %%ymm1, %%ymm9       \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 
	"vfmaddps      %%ymm10, %%ymm5 , %%ymm0, %%ymm10      \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm11, %%ymm5 , %%ymm1, %%ymm11      \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 
	
        "addq		$8  , %0	  	 	        \n\t"
	"subq	        $4  , %1			        \n\t"		

        "2:                                  \n\t"
	"cmpq           $0, %1                         \n\t"
        "je             3f                      \n\t"

	//	".align 16				        \n\t"
	"1:				        \n\t"
        "prefetcht0      384(%4,%0,4)                   \n\t"
	"vmovups	(%4,%0,4), %%ymm4	        \n\t" // 4 complex values from a0
        "prefetcht0      384(%5,%0,4)                   \n\t"
	"vmovups	(%5,%0,4), %%ymm5               \n\t" // 4 complex values from a1

        "prefetcht0      384(%2,%0,4)                   \n\t"
	"vmovups	    (%2,%0,4)  , %%ymm6		\n\t" // 4 complex values from x
	"vpermilps        $0xb1, %%ymm6, %%ymm7		\n\t" // exchange real and imap parts
	"vblendps $0x55, %%ymm6, %%ymm7, %%ymm0         \n\t" // only the real parts
	"vblendps $0x55, %%ymm7, %%ymm6, %%ymm1         \n\t" // only the imag parts
	
	"vfmaddps      %%ymm8 , %%ymm4 , %%ymm0, %%ymm8       \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm9 , %%ymm4 , %%ymm1, %%ymm9       \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 
	"vfmaddps      %%ymm10, %%ymm5 , %%ymm0, %%ymm10      \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm11, %%ymm5 , %%ymm1, %%ymm11      \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 

	"vmovups       32(%4,%0,4), %%ymm4	        \n\t" // 4 complex values from a0
	"vmovups       32(%5,%0,4), %%ymm5              \n\t" // 4 complex values from a1

	"vmovups	  32(%2,%0,4)  , %%ymm6		\n\t" // 4 complex values from x
	"vpermilps        $0xb1, %%ymm6, %%ymm7		\n\t" // exchange real and imap parts
	"vblendps $0x55, %%ymm6, %%ymm7, %%ymm0         \n\t" // only the real parts
	"vblendps $0x55, %%ymm7, %%ymm6, %%ymm1         \n\t" // only the imag parts

	"vfmaddps      %%ymm8 , %%ymm4 , %%ymm0, %%ymm8       \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm9 , %%ymm4 , %%ymm1, %%ymm9       \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 
	"vfmaddps      %%ymm10, %%ymm5 , %%ymm0, %%ymm10      \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm11, %%ymm5 , %%ymm1, %%ymm11      \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 
	
        "addq		$16 , %0	  	 	        \n\t"
	"subq	        $8  , %1			        \n\t"		
	"jnz		1b		        \n\t"

        "3:                                   \n\t"

        "vbroadcastss    (%6)  , %%xmm0                \n\t"  // value from alpha
        "vbroadcastss   4(%6)  , %%xmm1                \n\t"  // value from alpha


#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        "vpermilps      $0xb1 , %%ymm9 , %%ymm9                \n\t"
        "vpermilps      $0xb1 , %%ymm11, %%ymm11               \n\t"
        "vaddsubps      %%ymm9 , %%ymm8, %%ymm8                \n\t" 
        "vaddsubps      %%ymm11, %%ymm10, %%ymm10              \n\t"
#else
        "vpermilps      $0xb1 , %%ymm8 , %%ymm8                \n\t"
        "vpermilps      $0xb1 , %%ymm10, %%ymm10               \n\t"
        "vaddsubps      %%ymm8 , %%ymm9 , %%ymm8               \n\t"
        "vaddsubps      %%ymm10, %%ymm11, %%ymm10              \n\t"
        "vpermilps      $0xb1 , %%ymm8 , %%ymm8                \n\t"
        "vpermilps      $0xb1 , %%ymm10, %%ymm10               \n\t"
#endif

	"vmovsd         (%3), %%xmm4			\n\t" // read y
	"vmovsd        8(%3), %%xmm5			\n\t"

	"vextractf128   $1, %%ymm8 , %%xmm9		      \n\t"
	"vextractf128   $1, %%ymm10, %%xmm11	      	      \n\t"

	"vaddps		%%xmm8 , %%xmm9 , %%xmm8       \n\t"
	"vaddps		%%xmm10, %%xmm11, %%xmm10      \n\t"

	"vshufpd        $0x1, %%xmm8 , %%xmm8 , %%xmm9   \n\t"
	"vshufpd        $0x1, %%xmm10, %%xmm10, %%xmm11  \n\t"

	"vaddps		%%xmm8 , %%xmm9 , %%xmm8       \n\t"
	"vaddps		%%xmm10, %%xmm11, %%xmm10      \n\t"

        "vmulps         %%xmm8 , %%xmm1 , %%xmm9              \n\t"  // t_r * alpha_i , t_i * alpha_i
        "vmulps         %%xmm8 , %%xmm0 , %%xmm8              \n\t"  // t_r * alpha_r , t_i * alpha_r
        "vmulps         %%xmm10, %%xmm1 , %%xmm11             \n\t"  // t_r * alpha_i , t_i * alpha_i
        "vmulps         %%xmm10, %%xmm0 , %%xmm10             \n\t"  // t_r * alpha_r , t_i * alpha_r

#if !defined(XCONJ)
        "vpermilps      $0xb1 , %%xmm9 , %%xmm9                \n\t"
        "vpermilps      $0xb1 , %%xmm11, %%xmm11               \n\t"
        "vaddsubps      %%xmm9 , %%xmm8, %%xmm8               \n\t"
        "vaddsubps      %%xmm11, %%xmm10, %%xmm10             \n\t"
#else
        "vpermilps      $0xb1 , %%xmm8 , %%xmm8                \n\t"
        "vpermilps      $0xb1 , %%xmm10, %%xmm10               \n\t"
        "vaddsubps      %%xmm8 , %%xmm9 , %%xmm8              \n\t"
        "vaddsubps      %%xmm10, %%xmm11, %%xmm10             \n\t"
        "vpermilps      $0xb1 , %%xmm8 , %%xmm8                \n\t"
        "vpermilps      $0xb1 , %%xmm10, %%xmm10               \n\t"
#endif


	"vaddps		%%xmm8 , %%xmm4 , %%xmm8       \n\t"
	"vaddps		%%xmm10, %%xmm5 , %%xmm10      \n\t"

	"vmovsd	%%xmm8 ,   (%3)			\n\t"
	"vmovsd	%%xmm10,  8(%3)			\n\t"

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
static void cgemv_kernel_4x1( BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT *alpha) __attribute__ ((noinline));

static void cgemv_kernel_4x1( BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT *alpha)
{

	BLASLONG register i = 0;

	__asm__  __volatile__
	(
	"vzeroupper			 \n\t"

	"vxorps		%%ymm8 , %%ymm8 , %%ymm8 	\n\t" // temp
	"vxorps		%%ymm9 , %%ymm9 , %%ymm9 	\n\t" // temp

        "testq          $0x04, %1                      \n\t"
        "jz             2f                    \n\t"

	"vmovups	(%4,%0,4), %%ymm4	        \n\t" // 4 complex values from a0

	"vmovups	    (%2,%0,4)  , %%ymm6		\n\t" // 4 complex values from x
	"vpermilps        $0xb1, %%ymm6, %%ymm7		\n\t" // exchange real and imap parts
	"vblendps $0x55, %%ymm6, %%ymm7, %%ymm0         \n\t" // only the real parts
	"vblendps $0x55, %%ymm7, %%ymm6, %%ymm1         \n\t" // only the imag parts
	

	"vfmaddps      %%ymm8 , %%ymm4 , %%ymm0, %%ymm8       \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm9 , %%ymm4 , %%ymm1, %%ymm9       \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 
	
        "addq		$8  , %0	  	 	        \n\t"
	"subq	        $4  , %1			        \n\t"		

        "2:                                  \n\t"
	"cmpq           $0, %1                         \n\t"
        "je             3f                      \n\t"

	//	".align 16				        \n\t"
	"1:				        \n\t"
        "prefetcht0      384(%4,%0,4)                   \n\t"
	"vmovups	(%4,%0,4), %%ymm4	        \n\t" // 4 complex values from a0

        "prefetcht0      384(%2,%0,4)                   \n\t"
	"vmovups	    (%2,%0,4)  , %%ymm6		\n\t" // 4 complex values from x
	"vpermilps        $0xb1, %%ymm6, %%ymm7		\n\t" // exchange real and imap parts
	"vblendps $0x55, %%ymm6, %%ymm7, %%ymm0         \n\t" // only the real parts
	"vblendps $0x55, %%ymm7, %%ymm6, %%ymm1         \n\t" // only the imag parts
	
	"vfmaddps      %%ymm8 , %%ymm4 , %%ymm0, %%ymm8       \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm9 , %%ymm4 , %%ymm1, %%ymm9       \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 
	
	"vmovups       32(%4,%0,4), %%ymm4	        \n\t" // 4 complex values from a0

	"vmovups	  32(%2,%0,4)  , %%ymm6		\n\t" // 4 complex values from x
	"vpermilps        $0xb1, %%ymm6, %%ymm7		\n\t" // exchange real and imap parts
	"vblendps $0x55, %%ymm6, %%ymm7, %%ymm0         \n\t" // only the real parts
	"vblendps $0x55, %%ymm7, %%ymm6, %%ymm1         \n\t" // only the imag parts

	"vfmaddps      %%ymm8 , %%ymm4 , %%ymm0, %%ymm8       \n\t" // ar0*xr0,al0*xr0,ar1*xr1,al1*xr1 
	"vfmaddps      %%ymm9 , %%ymm4 , %%ymm1, %%ymm9       \n\t" // ar0*xl0,al0*xl0,ar1*xl1,al1*xl1 
	

        "addq		$16 , %0	  	 	        \n\t"
	"subq	        $8  , %1			        \n\t"		
	"jnz		1b		        \n\t"

        "3:                                   \n\t"

        "vbroadcastss    (%5)  , %%xmm0                \n\t"  // value from alpha
        "vbroadcastss   4(%5)  , %%xmm1                \n\t"  // value from alpha


#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        "vpermilps      $0xb1 , %%ymm9 , %%ymm9                \n\t"
        "vaddsubps      %%ymm9 , %%ymm8, %%ymm8                \n\t" 
#else
        "vpermilps      $0xb1 , %%ymm8 , %%ymm8                \n\t"
        "vaddsubps      %%ymm8 , %%ymm9 , %%ymm8               \n\t"
        "vpermilps      $0xb1 , %%ymm8 , %%ymm8                \n\t"
#endif

	"vmovsd         (%3), %%xmm4			\n\t" // read y

	"vextractf128   $1, %%ymm8 , %%xmm9		      \n\t"

	"vaddps		%%xmm8 , %%xmm9 , %%xmm8       \n\t"

	"vshufpd        $0x1, %%xmm8 , %%xmm8 , %%xmm9   \n\t"

	"vaddps		%%xmm8 , %%xmm9 , %%xmm8       \n\t"

        "vmulps         %%xmm8 , %%xmm1 , %%xmm9              \n\t"  // t_r * alpha_i , t_i * alpha_i
        "vmulps         %%xmm8 , %%xmm0 , %%xmm8              \n\t"  // t_r * alpha_r , t_i * alpha_r

#if !defined(XCONJ)
        "vpermilps      $0xb1 , %%xmm9 , %%xmm9                \n\t"
        "vaddsubps      %%xmm9 , %%xmm8, %%xmm8               \n\t"
#else
        "vpermilps      $0xb1 , %%xmm8 , %%xmm8                \n\t"
        "vaddsubps      %%xmm8 , %%xmm9 , %%xmm8              \n\t"
        "vpermilps      $0xb1 , %%xmm8 , %%xmm8                \n\t"
#endif


	"vaddps		%%xmm8 , %%xmm4 , %%xmm8       \n\t"

	"vmovsd	%%xmm8 ,   (%3)			\n\t"

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


