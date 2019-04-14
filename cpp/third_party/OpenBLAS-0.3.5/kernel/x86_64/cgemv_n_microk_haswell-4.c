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
static void cgemv_kernel_4x4( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y) __attribute__ ((noinline));

static void cgemv_kernel_4x4( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y)
{

	BLASLONG register i = 0;
	BLASLONG register n1 = n & -8 ;
	BLASLONG register n2 = n &  4 ;

	__asm__  __volatile__
	(
	"vzeroupper			 \n\t"

	"vbroadcastss	  (%2), %%ymm0                  \n\t"  // real part x0
	"vbroadcastss	 4(%2), %%ymm1                  \n\t"  // imag part x0
	"vbroadcastss	 8(%2), %%ymm2                  \n\t"  // real part x1
	"vbroadcastss	12(%2), %%ymm3                  \n\t"  // imag part x1
	"vbroadcastss	16(%2), %%ymm4                  \n\t"  // real part x2
	"vbroadcastss	20(%2), %%ymm5                  \n\t"  // imag part x2
	"vbroadcastss	24(%2), %%ymm6                  \n\t"  // real part x3
	"vbroadcastss	28(%2), %%ymm7                  \n\t"  // imag part x3

	"cmpq		$0 , %1				\n\t"
	"je		2f			        \n\t"

	//	".align 16				        \n\t"
	"1:				        \n\t"
	"prefetcht0      320(%4,%0,4)			\n\t"
	"vmovups	(%4,%0,4), %%ymm8	        \n\t" // 4 complex values form a0
	"vmovups      32(%4,%0,4), %%ymm9	        \n\t" // 4 complex values form a0

	"prefetcht0      320(%5,%0,4)			\n\t"
	"vmovups	(%5,%0,4), %%ymm10              \n\t" // 4 complex values form a1
	"vmovups      32(%5,%0,4), %%ymm11              \n\t" // 4 complex values form a1

	"vmulps      %%ymm8 , %%ymm0, %%ymm12      \n\t" // a_r[0] * x_r , a_i[0] * x_r, a_r[1] * x_r, a_i[1] * x_r
	"vmulps      %%ymm8 , %%ymm1, %%ymm13      \n\t" // a_r[0] * x_i , a_i[0] * x_i, a_r[1] * x_i, a_i[1] * x_i
	"vmulps      %%ymm9 , %%ymm0, %%ymm14      \n\t" // a_r[2] * x_r , a_i[2] * x_r, a_r[3] * x_r, a_i[3] * x_r
	"vmulps      %%ymm9 , %%ymm1, %%ymm15      \n\t" // a_r[2] * x_i , a_i[2] * x_i, a_r[3] * x_i, a_i[3] * x_i

	"prefetcht0      320(%6,%0,4)			\n\t"
	"vmovups	(%6,%0,4), %%ymm8	        \n\t" // 4 complex values form a2
	"vmovups      32(%6,%0,4), %%ymm9	        \n\t" // 4 complex values form a2

	"vfmadd231ps      %%ymm10, %%ymm2, %%ymm12      \n\t" // a_r[0] * x_r , a_i[0] * x_r, a_r[1] * x_r, a_i[1] * x_r
	"vfmadd231ps      %%ymm10, %%ymm3, %%ymm13      \n\t" // a_r[0] * x_i , a_i[0] * x_i, a_r[1] * x_i, a_i[1] * x_i
	"vfmadd231ps      %%ymm11, %%ymm2, %%ymm14      \n\t" // a_r[2] * x_r , a_i[2] * x_r, a_r[3] * x_r, a_i[3] * x_r
	"vfmadd231ps      %%ymm11, %%ymm3, %%ymm15      \n\t" // a_r[2] * x_i , a_i[2] * x_i, a_r[3] * x_i, a_i[3] * x_i

	"prefetcht0      320(%7,%0,4)			\n\t"
	"vmovups	(%7,%0,4), %%ymm10              \n\t" // 4 complex values form a3
	"vmovups      32(%7,%0,4), %%ymm11              \n\t" // 4 complex values form a3

	"vfmadd231ps      %%ymm8 , %%ymm4, %%ymm12      \n\t" // a_r[0] * x_r , a_i[0] * x_r, a_r[1] * x_r, a_i[1] * x_r
	"vfmadd231ps      %%ymm8 , %%ymm5, %%ymm13      \n\t" // a_r[0] * x_i , a_i[0] * x_i, a_r[1] * x_i, a_i[1] * x_i
	"vfmadd231ps      %%ymm9 , %%ymm4, %%ymm14      \n\t" // a_r[2] * x_r , a_i[2] * x_r, a_r[3] * x_r, a_i[3] * x_r
	"vfmadd231ps      %%ymm9 , %%ymm5, %%ymm15      \n\t" // a_r[2] * x_i , a_i[2] * x_i, a_r[3] * x_i, a_i[3] * x_i

	"vfmadd231ps      %%ymm10, %%ymm6, %%ymm12      \n\t" // a_r[0] * x_r , a_i[0] * x_r, a_r[1] * x_r, a_i[1] * x_r
	"vfmadd231ps      %%ymm10, %%ymm7, %%ymm13      \n\t" // a_r[0] * x_i , a_i[0] * x_i, a_r[1] * x_i, a_i[1] * x_i
	"vfmadd231ps      %%ymm11, %%ymm6, %%ymm14      \n\t" // a_r[2] * x_r , a_i[2] * x_r, a_r[3] * x_r, a_i[3] * x_r
	"vfmadd231ps      %%ymm11, %%ymm7, %%ymm15      \n\t" // a_r[2] * x_i , a_i[2] * x_i, a_r[3] * x_i, a_i[3] * x_i

	"prefetcht0      320(%3,%0,4)			\n\t"
	"vmovups	  (%3,%0,4),  %%ymm10           \n\t"
	"vmovups	32(%3,%0,4),  %%ymm11           \n\t"

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        "vpermilps      $0xb1 , %%ymm13, %%ymm13               \n\t"
        "vpermilps      $0xb1 , %%ymm15, %%ymm15               \n\t"
        "vaddsubps      %%ymm13, %%ymm12, %%ymm8              \n\t"
        "vaddsubps      %%ymm15, %%ymm14, %%ymm9              \n\t"
#else
        "vpermilps      $0xb1 , %%ymm12, %%ymm12               \n\t"
        "vpermilps      $0xb1 , %%ymm14, %%ymm14               \n\t"
        "vaddsubps      %%ymm12, %%ymm13, %%ymm8              \n\t"
        "vaddsubps      %%ymm14, %%ymm15, %%ymm9              \n\t"
        "vpermilps      $0xb1 , %%ymm8 , %%ymm8                \n\t"
        "vpermilps      $0xb1 , %%ymm9 , %%ymm9                \n\t"
#endif

        "vaddps         %%ymm8, %%ymm10, %%ymm12              \n\t"
        "vaddps         %%ymm9, %%ymm11, %%ymm13              \n\t"

	"vmovups  %%ymm12,   (%3,%0,4)		        \n\t" // 4 complex values to y	
	"vmovups  %%ymm13, 32(%3,%0,4)		        \n\t"	

        "addq		$16, %0	  	 	        \n\t"
	"subq	        $8 , %1			        \n\t"		
	"jnz		1b		        \n\t"

	"2:				        \n\t"

	"cmpq		$4, %8				\n\t"
	"jne		3f				\n\t"

	"vmovups	(%4,%0,4), %%ymm8	        \n\t" // 4 complex values form a0
	"vmovups	(%5,%0,4), %%ymm10              \n\t" // 4 complex values form a1

	"vmulps      %%ymm8 , %%ymm0, %%ymm12      \n\t" // a_r[0] * x_r , a_i[0] * x_r, a_r[1] * x_r, a_i[1] * x_r
	"vmulps      %%ymm8 , %%ymm1, %%ymm13      \n\t" // a_r[0] * x_i , a_i[0] * x_i, a_r[1] * x_i, a_i[1] * x_i

	"vfmadd231ps      %%ymm10, %%ymm2, %%ymm12      \n\t" // a_r[0] * x_r , a_i[0] * x_r, a_r[1] * x_r, a_i[1] * x_r
	"vfmadd231ps      %%ymm10, %%ymm3, %%ymm13      \n\t" // a_r[0] * x_i , a_i[0] * x_i, a_r[1] * x_i, a_i[1] * x_i

	"vmovups	(%6,%0,4), %%ymm8	        \n\t" // 4 complex values form a2
	"vmovups	(%7,%0,4), %%ymm10              \n\t" // 4 complex values form a3

	"vfmadd231ps      %%ymm8 , %%ymm4, %%ymm12      \n\t" // a_r[0] * x_r , a_i[0] * x_r, a_r[1] * x_r, a_i[1] * x_r
	"vfmadd231ps      %%ymm8 , %%ymm5, %%ymm13      \n\t" // a_r[0] * x_i , a_i[0] * x_i, a_r[1] * x_i, a_i[1] * x_i

	"vfmadd231ps      %%ymm10, %%ymm6, %%ymm12      \n\t" // a_r[0] * x_r , a_i[0] * x_r, a_r[1] * x_r, a_i[1] * x_r
	"vfmadd231ps      %%ymm10, %%ymm7, %%ymm13      \n\t" // a_r[0] * x_i , a_i[0] * x_i, a_r[1] * x_i, a_i[1] * x_i

	"vmovups	  (%3,%0,4),  %%ymm10           \n\t"

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        "vpermilps      $0xb1 , %%ymm13, %%ymm13               \n\t"
        "vaddsubps      %%ymm13, %%ymm12, %%ymm8              \n\t"
#else
        "vpermilps      $0xb1 , %%ymm12, %%ymm12               \n\t"
        "vaddsubps      %%ymm12, %%ymm13, %%ymm8              \n\t"
        "vpermilps      $0xb1 , %%ymm8 , %%ymm8                \n\t"
#endif

        "vaddps         %%ymm8, %%ymm10, %%ymm12              \n\t"

	"vmovups  %%ymm12,   (%3,%0,4)		        \n\t" // 4 complex values to y	

	"3:				        \n\t"
	"vzeroupper			 \n\t"

	:
          "+r" (i),	// 0	
	  "+r" (n1)  	// 1
	:
          "r" (x),      // 2
          "r" (y),      // 3
          "r" (ap[0]),  // 4
          "r" (ap[1]),  // 5
          "r" (ap[2]),  // 6
          "r" (ap[3]),  // 7
          "r" (n2)      // 8
	: "cc", 
	  "%xmm0", "%xmm1", "%xmm2", "%xmm3", 
	  "%xmm4", "%xmm5", "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11", 
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

} 


#define HAVE_KERNEL_4x2 1
static void cgemv_kernel_4x2( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y) __attribute__ ((noinline));

static void cgemv_kernel_4x2( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y)
{

	BLASLONG register i = 0;
	BLASLONG register n1 = n & -8 ;
	BLASLONG register n2 = n &  4 ;

	__asm__  __volatile__
	(
	"vzeroupper			 \n\t"

	"vbroadcastss	  (%2), %%ymm0                  \n\t"  // real part x0
	"vbroadcastss	 4(%2), %%ymm1                  \n\t"  // imag part x0
	"vbroadcastss	 8(%2), %%ymm2                  \n\t"  // real part x1
	"vbroadcastss	12(%2), %%ymm3                  \n\t"  // imag part x1

	"cmpq		$0 , %1				\n\t"
	"je		2f			        \n\t"

	//	".align 16				        \n\t"
	"1:				        \n\t"
	"prefetcht0      320(%4,%0,4)			\n\t"
	"vmovups	(%4,%0,4), %%ymm8	        \n\t" // 4 complex values form a0
	"vmovups      32(%4,%0,4), %%ymm9	        \n\t" // 4 complex values form a0

	"prefetcht0      320(%5,%0,4)			\n\t"
	"vmovups	(%5,%0,4), %%ymm10              \n\t" // 4 complex values form a1
	"vmovups      32(%5,%0,4), %%ymm11              \n\t" // 4 complex values form a1

	"vmulps      %%ymm8 , %%ymm0, %%ymm12      \n\t" // a_r[0] * x_r , a_i[0] * x_r, a_r[1] * x_r, a_i[1] * x_r
	"vmulps      %%ymm8 , %%ymm1, %%ymm13      \n\t" // a_r[0] * x_i , a_i[0] * x_i, a_r[1] * x_i, a_i[1] * x_i
	"vmulps      %%ymm9 , %%ymm0, %%ymm14      \n\t" // a_r[2] * x_r , a_i[2] * x_r, a_r[3] * x_r, a_i[3] * x_r
	"vmulps      %%ymm9 , %%ymm1, %%ymm15      \n\t" // a_r[2] * x_i , a_i[2] * x_i, a_r[3] * x_i, a_i[3] * x_i

	"vfmadd231ps      %%ymm10, %%ymm2, %%ymm12      \n\t" // a_r[0] * x_r , a_i[0] * x_r, a_r[1] * x_r, a_i[1] * x_r
	"vfmadd231ps      %%ymm10, %%ymm3, %%ymm13      \n\t" // a_r[0] * x_i , a_i[0] * x_i, a_r[1] * x_i, a_i[1] * x_i
	"vfmadd231ps      %%ymm11, %%ymm2, %%ymm14      \n\t" // a_r[2] * x_r , a_i[2] * x_r, a_r[3] * x_r, a_i[3] * x_r
	"vfmadd231ps      %%ymm11, %%ymm3, %%ymm15      \n\t" // a_r[2] * x_i , a_i[2] * x_i, a_r[3] * x_i, a_i[3] * x_i

	"prefetcht0      320(%3,%0,4)			\n\t"
	"vmovups	  (%3,%0,4),  %%ymm10           \n\t"
	"vmovups	32(%3,%0,4),  %%ymm11           \n\t"

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        "vpermilps      $0xb1 , %%ymm13, %%ymm13               \n\t"
        "vpermilps      $0xb1 , %%ymm15, %%ymm15               \n\t"
        "vaddsubps      %%ymm13, %%ymm12, %%ymm8              \n\t"
        "vaddsubps      %%ymm15, %%ymm14, %%ymm9              \n\t"
#else
        "vpermilps      $0xb1 , %%ymm12, %%ymm12               \n\t"
        "vpermilps      $0xb1 , %%ymm14, %%ymm14               \n\t"
        "vaddsubps      %%ymm12, %%ymm13, %%ymm8              \n\t"
        "vaddsubps      %%ymm14, %%ymm15, %%ymm9              \n\t"
        "vpermilps      $0xb1 , %%ymm8 , %%ymm8                \n\t"
        "vpermilps      $0xb1 , %%ymm9 , %%ymm9                \n\t"
#endif

        "vaddps         %%ymm8, %%ymm10, %%ymm12              \n\t"
        "vaddps         %%ymm9, %%ymm11, %%ymm13              \n\t"

	"vmovups  %%ymm12,   (%3,%0,4)		        \n\t" // 4 complex values to y	
	"vmovups  %%ymm13, 32(%3,%0,4)		        \n\t"	

        "addq		$16, %0	  	 	        \n\t"
	"subq	        $8 , %1			        \n\t"		
	"jnz		1b		        \n\t"

	"2:				        \n\t"

	"cmpq		$4, %6				\n\t"
	"jne		3f				\n\t"

	"vmovups	(%4,%0,4), %%ymm8	        \n\t" // 4 complex values form a0
	"vmovups	(%5,%0,4), %%ymm10              \n\t" // 4 complex values form a1

	"vmulps      %%ymm8 , %%ymm0, %%ymm12      \n\t" // a_r[0] * x_r , a_i[0] * x_r, a_r[1] * x_r, a_i[1] * x_r
	"vmulps      %%ymm8 , %%ymm1, %%ymm13      \n\t" // a_r[0] * x_i , a_i[0] * x_i, a_r[1] * x_i, a_i[1] * x_i

	"vfmadd231ps      %%ymm10, %%ymm2, %%ymm12      \n\t" // a_r[0] * x_r , a_i[0] * x_r, a_r[1] * x_r, a_i[1] * x_r
	"vfmadd231ps      %%ymm10, %%ymm3, %%ymm13      \n\t" // a_r[0] * x_i , a_i[0] * x_i, a_r[1] * x_i, a_i[1] * x_i

	"vmovups	  (%3,%0,4),  %%ymm10           \n\t"

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        "vpermilps      $0xb1 , %%ymm13, %%ymm13               \n\t"
        "vaddsubps      %%ymm13, %%ymm12, %%ymm8              \n\t"
#else
        "vpermilps      $0xb1 , %%ymm12, %%ymm12               \n\t"
        "vaddsubps      %%ymm12, %%ymm13, %%ymm8              \n\t"
        "vpermilps      $0xb1 , %%ymm8 , %%ymm8                \n\t"
#endif

        "vaddps         %%ymm8, %%ymm10, %%ymm12              \n\t"

	"vmovups  %%ymm12,   (%3,%0,4)		        \n\t" // 4 complex values to y	

	"3:				        \n\t"
	"vzeroupper			 \n\t"

	:
          "+r" (i),	// 0	
	  "+r" (n1)  	// 1
	:
          "r" (x),      // 2
          "r" (y),      // 3
          "r" (ap[0]),  // 4
          "r" (ap[1]),  // 5
          "r" (n2)      // 6
	: "cc", 
	  "%xmm0", "%xmm1", "%xmm2", "%xmm3", 
	  "%xmm4", "%xmm5", "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11", 
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

} 


#define HAVE_KERNEL_4x1 1
static void cgemv_kernel_4x1( BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y) __attribute__ ((noinline));

static void cgemv_kernel_4x1( BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y)
{

	BLASLONG register i = 0;
	BLASLONG register n1 = n & -8 ;
	BLASLONG register n2 = n &  4 ;

	__asm__  __volatile__
	(
	"vzeroupper			 \n\t"

	"vbroadcastss	  (%2), %%ymm0                  \n\t"  // real part x0
	"vbroadcastss	 4(%2), %%ymm1                  \n\t"  // imag part x0

	"cmpq		$0 , %1				\n\t"
	"je		2f			        \n\t"

	//	".align 16				        \n\t"
	"1:				        \n\t"
	"prefetcht0      320(%4,%0,4)			\n\t"
	"vmovups	(%4,%0,4), %%ymm8	        \n\t" // 4 complex values form a0
	"vmovups      32(%4,%0,4), %%ymm9	        \n\t" // 4 complex values form a0

	"vmulps      %%ymm8 , %%ymm0, %%ymm12      \n\t" // a_r[0] * x_r , a_i[0] * x_r, a_r[1] * x_r, a_i[1] * x_r
	"vmulps      %%ymm8 , %%ymm1, %%ymm13      \n\t" // a_r[0] * x_i , a_i[0] * x_i, a_r[1] * x_i, a_i[1] * x_i
	"vmulps      %%ymm9 , %%ymm0, %%ymm14      \n\t" // a_r[2] * x_r , a_i[2] * x_r, a_r[3] * x_r, a_i[3] * x_r
	"vmulps      %%ymm9 , %%ymm1, %%ymm15      \n\t" // a_r[2] * x_i , a_i[2] * x_i, a_r[3] * x_i, a_i[3] * x_i

	"prefetcht0      320(%3,%0,4)			\n\t"
	"vmovups	  (%3,%0,4),  %%ymm10           \n\t"
	"vmovups	32(%3,%0,4),  %%ymm11           \n\t"

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        "vpermilps      $0xb1 , %%ymm13, %%ymm13               \n\t"
        "vpermilps      $0xb1 , %%ymm15, %%ymm15               \n\t"
        "vaddsubps      %%ymm13, %%ymm12, %%ymm8              \n\t"
        "vaddsubps      %%ymm15, %%ymm14, %%ymm9              \n\t"
#else
        "vpermilps      $0xb1 , %%ymm12, %%ymm12               \n\t"
        "vpermilps      $0xb1 , %%ymm14, %%ymm14               \n\t"
        "vaddsubps      %%ymm12, %%ymm13, %%ymm8              \n\t"
        "vaddsubps      %%ymm14, %%ymm15, %%ymm9              \n\t"
        "vpermilps      $0xb1 , %%ymm8 , %%ymm8                \n\t"
        "vpermilps      $0xb1 , %%ymm9 , %%ymm9                \n\t"
#endif

        "addq		$16, %0	  	 	        \n\t"
        "vaddps         %%ymm8, %%ymm10, %%ymm12              \n\t"
        "vaddps         %%ymm9, %%ymm11, %%ymm13              \n\t"

	"subq	        $8 , %1			        \n\t"		
	"vmovups  %%ymm12,-64(%3,%0,4)		        \n\t" // 4 complex values to y	
	"vmovups  %%ymm13,-32(%3,%0,4)		        \n\t"	

	"jnz		1b		        \n\t"

	"2:				        \n\t"

	"cmpq		$4, %5				\n\t"
	"jne		3f				\n\t"

	"vmovups	(%4,%0,4), %%ymm8	        \n\t" // 4 complex values form a0

	"vmulps      %%ymm8 , %%ymm0, %%ymm12      \n\t" // a_r[0] * x_r , a_i[0] * x_r, a_r[1] * x_r, a_i[1] * x_r
	"vmulps      %%ymm8 , %%ymm1, %%ymm13      \n\t" // a_r[0] * x_i , a_i[0] * x_i, a_r[1] * x_i, a_i[1] * x_i

	"vmovups	  (%3,%0,4),  %%ymm10           \n\t"

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        "vpermilps      $0xb1 , %%ymm13, %%ymm13               \n\t"
        "vaddsubps      %%ymm13, %%ymm12, %%ymm8              \n\t"
#else
        "vpermilps      $0xb1 , %%ymm12, %%ymm12               \n\t"
        "vaddsubps      %%ymm12, %%ymm13, %%ymm8              \n\t"
        "vpermilps      $0xb1 , %%ymm8 , %%ymm8                \n\t"
#endif

        "vaddps         %%ymm8, %%ymm10, %%ymm12              \n\t"

	"vmovups  %%ymm12,   (%3,%0,4)		        \n\t" // 4 complex values to y	

	"3:				        \n\t"
	"vzeroupper			 \n\t"

	:
          "+r" (i),	// 0	
	  "+r" (n1)  	// 1
	:
          "r" (x),      // 2
          "r" (y),      // 3
          "r" (ap),     // 4
          "r" (n2)      // 5
	: "cc", 
	  "%xmm0", "%xmm1", "%xmm2", "%xmm3", 
	  "%xmm4", "%xmm5", "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11", 
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

} 


#define HAVE_KERNEL_ADDY 1

static void add_y(BLASLONG n, FLOAT *src, FLOAT *dest, BLASLONG inc_dest,FLOAT alpha_r, FLOAT alpha_i)  __attribute__ ((noinline));

static void add_y(BLASLONG n, FLOAT *src, FLOAT *dest, BLASLONG inc_dest,FLOAT alpha_r, FLOAT alpha_i)
{
	BLASLONG i;

	if ( inc_dest != 2 )
	{

		FLOAT temp_r;
		FLOAT temp_i;
		for ( i=0; i<n; i++ )
		{
#if !defined(XCONJ) 
			temp_r = alpha_r * src[0] - alpha_i * src[1];
			temp_i = alpha_r * src[1] + alpha_i * src[0];
#else
			temp_r =  alpha_r * src[0] + alpha_i * src[1];
			temp_i = -alpha_r * src[1] + alpha_i * src[0];
#endif

			*dest += temp_r;
			*(dest+1) += temp_i;

			src+=2;
			dest += inc_dest;
		}
		return;
	}

	i=0;
	BLASLONG register n1 = n & -8 ;
	BLASLONG register n2 = n &  4 ;

	__asm__  __volatile__
	(
	"vzeroupper			 \n\t"

	"vbroadcastss	  (%4), %%ymm0                  \n\t"  // alpha_r
	"vbroadcastss	  (%5), %%ymm1                  \n\t"  // alpha_i

	"cmpq		$0 , %1				\n\t"
	"je		2f			        \n\t"

	//	".align 16				        \n\t"
	"1:				        \n\t"
	"vmovups	(%2,%0,4), %%ymm8	        \n\t" // 4 complex values from src
	"vmovups      32(%2,%0,4), %%ymm9	        \n\t" 

	"vmulps      %%ymm8 , %%ymm0, %%ymm12      \n\t" // a_r[0] * x_r , a_i[0] * x_r, a_r[1] * x_r, a_i[1] * x_r
	"vmulps      %%ymm8 , %%ymm1, %%ymm13      \n\t" // a_r[0] * x_i , a_i[0] * x_i, a_r[1] * x_i, a_i[1] * x_i
	"vmulps      %%ymm9 , %%ymm0, %%ymm14      \n\t" // a_r[2] * x_r , a_i[2] * x_r, a_r[3] * x_r, a_i[3] * x_r
	"vmulps      %%ymm9 , %%ymm1, %%ymm15      \n\t" // a_r[2] * x_i , a_i[2] * x_i, a_r[3] * x_i, a_i[3] * x_i

	"vmovups	  (%3,%0,4),  %%ymm10           \n\t" // 4 complex values from dest
	"vmovups	32(%3,%0,4),  %%ymm11           \n\t"

#if  !defined(XCONJ)  
        "vpermilps      $0xb1 , %%ymm13, %%ymm13               \n\t"
        "vpermilps      $0xb1 , %%ymm15, %%ymm15               \n\t"
        "vaddsubps      %%ymm13, %%ymm12, %%ymm8              \n\t"
        "vaddsubps      %%ymm15, %%ymm14, %%ymm9              \n\t"
#else
        "vpermilps      $0xb1 , %%ymm12, %%ymm12               \n\t"
        "vpermilps      $0xb1 , %%ymm14, %%ymm14               \n\t"
        "vaddsubps      %%ymm12, %%ymm13, %%ymm8              \n\t"
        "vaddsubps      %%ymm14, %%ymm15, %%ymm9              \n\t"
        "vpermilps      $0xb1 , %%ymm8 , %%ymm8                \n\t"
        "vpermilps      $0xb1 , %%ymm9 , %%ymm9                \n\t"
#endif

        "addq		$16, %0	  	 	        \n\t"
        "vaddps         %%ymm8, %%ymm10, %%ymm12              \n\t"
        "vaddps         %%ymm9, %%ymm11, %%ymm13              \n\t"

	"subq	        $8 , %1			        \n\t"		
	"vmovups  %%ymm12,-64(%3,%0,4)		        \n\t" // 4 complex values to y	
	"vmovups  %%ymm13,-32(%3,%0,4)		        \n\t"	

	"jnz		1b		        \n\t"

	"2:				        \n\t"

	"cmpq		$4, %6				\n\t"
	"jne		3f				\n\t"

	"vmovups	(%2,%0,4), %%ymm8	        \n\t" // 4 complex values src

	"vmulps      %%ymm8 , %%ymm0, %%ymm12      \n\t" // a_r[0] * x_r , a_i[0] * x_r, a_r[1] * x_r, a_i[1] * x_r
	"vmulps      %%ymm8 , %%ymm1, %%ymm13      \n\t" // a_r[0] * x_i , a_i[0] * x_i, a_r[1] * x_i, a_i[1] * x_i

	"vmovups	  (%3,%0,4),  %%ymm10           \n\t"

#if !defined(XCONJ)
        "vpermilps      $0xb1 , %%ymm13, %%ymm13               \n\t"
        "vaddsubps      %%ymm13, %%ymm12, %%ymm8              \n\t"
#else
        "vpermilps      $0xb1 , %%ymm12, %%ymm12               \n\t"
        "vaddsubps      %%ymm12, %%ymm13, %%ymm8              \n\t"
        "vpermilps      $0xb1 , %%ymm8 , %%ymm8                \n\t"
#endif

        "vaddps         %%ymm8, %%ymm10, %%ymm12              \n\t"

	"vmovups  %%ymm12,   (%3,%0,4)		        \n\t" // 4 complex values to y	

	"3:				        \n\t"
	"vzeroupper			 \n\t"

	:
          "+r" (i),	      // 0	
	  "+r" (n1)  	      // 1
	:
          "r" (src),          // 2
          "r" (dest),         // 3
          "r" (&alpha_r),     // 4
          "r" (&alpha_i),     // 5
          "r" (n2)            // 6
	: "cc", 
	  "%xmm0", "%xmm1", "%xmm2", "%xmm3", 
	  "%xmm4", "%xmm5", "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11", 
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

	return;

}

