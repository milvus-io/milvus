/*********************************************************************/
/* Copyright 2009, 2010 The University of Texas at Austin.           */
/* All rights reserved.                                              */
/*                                                                   */
/* Redistribution and use in source and binary forms, with or        */
/* without modification, are permitted provided that the following   */
/* conditions are met:                                               */
/*                                                                   */
/*   1. Redistributions of source code must retain the above         */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer.                                                  */
/*                                                                   */
/*   2. Redistributions in binary form must reproduce the above      */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer in the documentation and/or other materials       */
/*      provided with the distribution.                              */
/*                                                                   */
/*    THIS  SOFTWARE IS PROVIDED  BY THE  UNIVERSITY OF  TEXAS AT    */
/*    AUSTIN  ``AS IS''  AND ANY  EXPRESS OR  IMPLIED WARRANTIES,    */
/*    INCLUDING, BUT  NOT LIMITED  TO, THE IMPLIED  WARRANTIES OF    */
/*    MERCHANTABILITY  AND FITNESS FOR  A PARTICULAR  PURPOSE ARE    */
/*    DISCLAIMED.  IN  NO EVENT SHALL THE UNIVERSITY  OF TEXAS AT    */
/*    AUSTIN OR CONTRIBUTORS BE  LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL,  SPECIAL, EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES    */
/*    (INCLUDING, BUT  NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE    */
/*    GOODS  OR  SERVICES; LOSS  OF  USE,  DATA,  OR PROFITS;  OR    */
/*    BUSINESS INTERRUPTION) HOWEVER CAUSED  AND ON ANY THEORY OF    */
/*    LIABILITY, WHETHER  IN CONTRACT, STRICT  LIABILITY, OR TORT    */
/*    (INCLUDING NEGLIGENCE OR OTHERWISE)  ARISING IN ANY WAY OUT    */
/*    OF  THE  USE OF  THIS  SOFTWARE,  EVEN  IF ADVISED  OF  THE    */
/*    POSSIBILITY OF SUCH DAMAGE.                                    */
/*                                                                   */
/* The views and conclusions contained in the software and           */
/* documentation are those of the authors and should not be          */
/* interpreted as representing official policies, either expressed   */
/* or implied, of The University of Texas at Austin.                 */
/*********************************************************************/

#include "common.h"

static FLOAT dm1 = -1.;

#ifdef CONJ
#define GEMM_KERNEL   GEMM_KERNEL_L
#else
#define GEMM_KERNEL   GEMM_KERNEL_N
#endif

#if GEMM_DEFAULT_UNROLL_M == 1
#define GEMM_UNROLL_M_SHIFT 0
#endif

#if GEMM_DEFAULT_UNROLL_M == 2
#define GEMM_UNROLL_M_SHIFT 1
#endif

#if GEMM_DEFAULT_UNROLL_M == 4
#define GEMM_UNROLL_M_SHIFT 2
#endif

#if GEMM_DEFAULT_UNROLL_M == 6
#define GEMM_UNROLL_M_SHIFT 2
#endif

#if GEMM_DEFAULT_UNROLL_M == 8
#define GEMM_UNROLL_M_SHIFT 3
#endif

#if GEMM_DEFAULT_UNROLL_M == 16
#define GEMM_UNROLL_M_SHIFT 4
#endif

#if GEMM_DEFAULT_UNROLL_N == 1
#define GEMM_UNROLL_N_SHIFT 0
#endif

#if GEMM_DEFAULT_UNROLL_N == 2
#define GEMM_UNROLL_N_SHIFT 1
#endif

#if GEMM_DEFAULT_UNROLL_N == 4
#define GEMM_UNROLL_N_SHIFT 2
#endif

#if GEMM_DEFAULT_UNROLL_N == 8
#define GEMM_UNROLL_N_SHIFT 3
#endif

#if GEMM_DEFAULT_UNROLL_N == 16
#define GEMM_UNROLL_N_SHIFT 4
#endif

static void dtrsm_LN_solve_opt(BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, FLOAT *as, FLOAT *bs)  __attribute__ ((noinline));

static void dtrsm_LN_solve_opt(BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, FLOAT *as, FLOAT *bs)
{

	FLOAT *c1 = c + ldc ;
	BLASLONG n1 = n * 8;
	BLASLONG i=0;

  	bs += 14;

        __asm__  __volatile__
        (
	"	vzeroupper							\n\t"
	"	prefetcht0	(%4)						\n\t"
	"	prefetcht0	(%5)						\n\t"
	"	vxorpd	%%xmm8 , %%xmm8 , %%xmm8				\n\t"
	"	vxorpd	%%xmm9 , %%xmm9 , %%xmm9				\n\t"
	"	vxorpd	%%xmm10, %%xmm10, %%xmm10				\n\t"
	"	vxorpd	%%xmm11, %%xmm11, %%xmm11				\n\t"
	"	vxorpd	%%xmm12, %%xmm12, %%xmm12				\n\t"
	"	vxorpd	%%xmm13, %%xmm13, %%xmm13				\n\t"
	"	vxorpd	%%xmm14, %%xmm14, %%xmm14				\n\t"
	"	vxorpd	%%xmm15, %%xmm15, %%xmm15				\n\t"

	"	cmpq	       $0, %0						\n\t"
	"	je	       2f						\n\t"

	"	.align 16							\n\t"
	"1:									\n\t"

	"	prefetcht0	384(%2,%1,8)					\n\t"
	"	prefetcht0	384(%3,%1,8)					\n\t"
	"	vmovddup	(%3,%1,2), %%xmm0				\n\t"	// read b
	"	vmovups         (%2,%1,8), %%xmm4				\n\t"
	"	vmovddup       8(%3,%1,2), %%xmm1				\n\t"	
	"	vmovups       16(%2,%1,8), %%xmm5				\n\t"
	"	vmovups       32(%2,%1,8), %%xmm6				\n\t"
	"	vmovups       48(%2,%1,8), %%xmm7				\n\t"

	"	vfmaddpd	%%xmm8 , %%xmm0 , %%xmm4 , %%xmm8		\n\t"
	"	vfmaddpd	%%xmm12, %%xmm1 , %%xmm4 , %%xmm12		\n\t"
	"	vfmaddpd	%%xmm9 , %%xmm0 , %%xmm5 , %%xmm9		\n\t"
	"	vfmaddpd	%%xmm13, %%xmm1 , %%xmm5 , %%xmm13		\n\t"
	"	vfmaddpd	%%xmm10, %%xmm0 , %%xmm6 , %%xmm10		\n\t"
	"	vfmaddpd	%%xmm14, %%xmm1 , %%xmm6 , %%xmm14		\n\t"
	"	addq		$8, %1						\n\t"
	"	vfmaddpd	%%xmm11, %%xmm0 , %%xmm7 , %%xmm11		\n\t"
	"	vfmaddpd	%%xmm15, %%xmm1 , %%xmm7 , %%xmm15		\n\t"
	"	cmpq		%1, %0						\n\t"

	"	jz		2f						\n\t"

	"	prefetcht0	384(%2,%1,8)					\n\t"
	"	vmovddup	(%3,%1,2), %%xmm0				\n\t"	// read b
	"	vmovups         (%2,%1,8), %%xmm4				\n\t"
	"	vmovddup       8(%3,%1,2), %%xmm1				\n\t"	
	"	vmovups       16(%2,%1,8), %%xmm5				\n\t"
	"	vmovups       32(%2,%1,8), %%xmm6				\n\t"
	"	vmovups       48(%2,%1,8), %%xmm7				\n\t"

	"	vfmaddpd	%%xmm8 , %%xmm0 , %%xmm4 , %%xmm8		\n\t"
	"	vfmaddpd	%%xmm12, %%xmm1 , %%xmm4 , %%xmm12		\n\t"
	"	vfmaddpd	%%xmm9 , %%xmm0 , %%xmm5 , %%xmm9		\n\t"
	"	vfmaddpd	%%xmm13, %%xmm1 , %%xmm5 , %%xmm13		\n\t"
	"	vfmaddpd	%%xmm10, %%xmm0 , %%xmm6 , %%xmm10		\n\t"
	"	vfmaddpd	%%xmm14, %%xmm1 , %%xmm6 , %%xmm14		\n\t"
	"	addq		$8, %1						\n\t"
	"	vfmaddpd	%%xmm11, %%xmm0 , %%xmm7 , %%xmm11		\n\t"
	"	vfmaddpd	%%xmm15, %%xmm1 , %%xmm7 , %%xmm15		\n\t"
	"	cmpq		%1, %0						\n\t"

	"	jz		2f						\n\t"

	"	prefetcht0	384(%2,%1,8)					\n\t"
	"	vmovddup	(%3,%1,2), %%xmm0				\n\t"	// read b
	"	vmovups         (%2,%1,8), %%xmm4				\n\t"
	"	vmovddup       8(%3,%1,2), %%xmm1				\n\t"	
	"	vmovups       16(%2,%1,8), %%xmm5				\n\t"
	"	vmovups       32(%2,%1,8), %%xmm6				\n\t"
	"	vmovups       48(%2,%1,8), %%xmm7				\n\t"

	"	vfmaddpd	%%xmm8 , %%xmm0 , %%xmm4 , %%xmm8		\n\t"
	"	vfmaddpd	%%xmm12, %%xmm1 , %%xmm4 , %%xmm12		\n\t"
	"	vfmaddpd	%%xmm9 , %%xmm0 , %%xmm5 , %%xmm9		\n\t"
	"	vfmaddpd	%%xmm13, %%xmm1 , %%xmm5 , %%xmm13		\n\t"
	"	vfmaddpd	%%xmm10, %%xmm0 , %%xmm6 , %%xmm10		\n\t"
	"	vfmaddpd	%%xmm14, %%xmm1 , %%xmm6 , %%xmm14		\n\t"
	"	addq		$8, %1						\n\t"
	"	vfmaddpd	%%xmm11, %%xmm0 , %%xmm7 , %%xmm11		\n\t"
	"	vfmaddpd	%%xmm15, %%xmm1 , %%xmm7 , %%xmm15		\n\t"
	"	cmpq		%1, %0						\n\t"

	"	jz		2f						\n\t"

	"	prefetcht0	384(%2,%1,8)					\n\t"
	"	vmovddup	(%3,%1,2), %%xmm0				\n\t"	// read b
	"	vmovddup       8(%3,%1,2), %%xmm1				\n\t"	
	"	vmovups         (%2,%1,8), %%xmm4				\n\t"
	"	vmovups       16(%2,%1,8), %%xmm5				\n\t"
	"	vmovups       32(%2,%1,8), %%xmm6				\n\t"
	"	vmovups       48(%2,%1,8), %%xmm7				\n\t"

	"	vfmaddpd	%%xmm8 , %%xmm0 , %%xmm4 , %%xmm8		\n\t"
	"	vfmaddpd	%%xmm12, %%xmm1 , %%xmm4 , %%xmm12		\n\t"
	"	vfmaddpd	%%xmm9 , %%xmm0 , %%xmm5 , %%xmm9		\n\t"
	"	vfmaddpd	%%xmm13, %%xmm1 , %%xmm5 , %%xmm13		\n\t"
	"	vfmaddpd	%%xmm10, %%xmm0 , %%xmm6 , %%xmm10		\n\t"
	"	vfmaddpd	%%xmm14, %%xmm1 , %%xmm6 , %%xmm14		\n\t"
	"	addq		$8, %1						\n\t"
	"	vfmaddpd	%%xmm11, %%xmm0 , %%xmm7 , %%xmm11		\n\t"
	"	vfmaddpd	%%xmm15, %%xmm1 , %%xmm7 , %%xmm15		\n\t"
	"	cmpq		%1, %0						\n\t"

	"	jnz		1b						\n\t"

	"2:									\n\t"


	"	vmovups		  (%4) , %%xmm0					\n\t"
	"	vmovups		16(%4) , %%xmm1					\n\t"
	"	vmovups		32(%4) , %%xmm2					\n\t"
	"	vmovups		48(%4) , %%xmm3					\n\t"

	"	vmovups		  (%5) , %%xmm4					\n\t"
	"	vmovups		16(%5) , %%xmm5					\n\t"
	"	vmovups		32(%5) , %%xmm6					\n\t"
	"	vmovups		48(%5) , %%xmm7					\n\t"

	"	vsubpd		%%xmm8 , %%xmm0 , %%xmm8			\n\t"
	"	vsubpd		%%xmm9 , %%xmm1 , %%xmm9			\n\t"
	"	vsubpd		%%xmm10, %%xmm2 , %%xmm10			\n\t"
	"	vsubpd		%%xmm11, %%xmm3 , %%xmm11			\n\t"

	"	vsubpd		%%xmm12, %%xmm4 , %%xmm12			\n\t"
	"	vsubpd		%%xmm13, %%xmm5 , %%xmm13			\n\t"
	"	vsubpd		%%xmm14, %%xmm6 , %%xmm14			\n\t"
	"	vsubpd		%%xmm15, %%xmm7 , %%xmm15			\n\t"

	"3:									\n\t"

	"	movq		$56, %1						\n\t"	// i = 7
	"	xorq		%0, %0						\n\t"	// pointer for bs

	"	vmovddup	56(%6, %1, 8)  , %%xmm0 			\n\t"	// read aa[i]
	"	vunpckhpd	%%xmm11 , %%xmm11, %%xmm1			\n\t"	// extract bb0
	"	vunpckhpd	%%xmm15 , %%xmm15, %%xmm2			\n\t"	// extract bb1
	"	vmulpd		%%xmm0  , %%xmm1 , %%xmm1			\n\t"	// bb0 * aa
	"	vmulpd		%%xmm0  , %%xmm2 , %%xmm2			\n\t"	// bb0 * aa
	"	vmovsd		%%xmm1  , 56(%4)				\n\t"   // c[i] = bb0 * aa
	"	vmovsd		%%xmm2  , 56(%5)				\n\t"   // c[i] = bb1 * aa
	"	vmovsd		%%xmm1  ,   (%7 , %0, 8)			\n\t"   // b[0] = bb0 * aa
	"	vmovsd		%%xmm2  ,  8(%7 , %0, 8)			\n\t"   // b[1] = bb1 * aa

	"	vmovups	 	0(%6, %1, 8)  , %%xmm4				\n\t"   // read a[k]
	"	vmovups	       16(%6, %1, 8)  , %%xmm5				\n\t"   // read a[k]
	"	vmovups	       32(%6, %1, 8)  , %%xmm6				\n\t"   // read a[k]
	"	vmovsd	       48(%6, %1, 8)  , %%xmm7				\n\t"   // read a[k]
	"	vfnmaddpd	%%xmm8  , %%xmm1 , %%xmm4 , %%xmm8		\n\t"
	"	vfnmaddpd	%%xmm12 , %%xmm2 , %%xmm4 , %%xmm12		\n\t"
	"	vfnmaddpd	%%xmm9  , %%xmm1 , %%xmm5 , %%xmm9		\n\t"
	"	vfnmaddpd	%%xmm13 , %%xmm2 , %%xmm5 , %%xmm13		\n\t"
	"	vfnmaddpd	%%xmm10 , %%xmm1 , %%xmm6 , %%xmm10		\n\t"
	"	vfnmaddpd	%%xmm14 , %%xmm2 , %%xmm6 , %%xmm14		\n\t"
	"	vfnmaddsd	%%xmm11 , %%xmm1 , %%xmm7 , %%xmm11		\n\t"
	"	vfnmaddsd	%%xmm15 , %%xmm2 , %%xmm7 , %%xmm15		\n\t"

	"	subq		$8, %1						\n\t" // i = 6
	"	subq		$2, %0						\n\t" // b-= n

	"	vmovddup	48(%6, %1, 8)  , %%xmm0 			\n\t"	// read aa[i]
	"	vunpcklpd	%%xmm11 , %%xmm11, %%xmm1			\n\t"	// extract bb0
	"	vunpcklpd	%%xmm15 , %%xmm15, %%xmm2			\n\t"	// extract bb1
	"	vmulpd		%%xmm0  , %%xmm1 , %%xmm1			\n\t"	// bb0 * aa
	"	vmulpd		%%xmm0  , %%xmm2 , %%xmm2			\n\t"	// bb1 * aa
	"	vmovsd		%%xmm1  , 48(%4)				\n\t"   // c[i] = bb0 * aa
	"	vmovsd		%%xmm2  , 48(%5)				\n\t"   // c[i] = bb1 * aa
	"	vmovsd		%%xmm1  ,   (%7 , %0, 8)			\n\t"   // b[0] = bb0 * aa
	"	vmovsd		%%xmm2  ,  8(%7 , %0, 8)			\n\t"   // b[1] = bb1 * aa

	"	vmovups	 	0(%6, %1, 8)  , %%xmm4				\n\t"   // read a[k]
	"	vmovups	       16(%6, %1, 8)  , %%xmm5				\n\t"   // read a[k]
	"	vmovups	       32(%6, %1, 8)  , %%xmm6				\n\t"   // read a[k]
	"	vfnmaddpd	%%xmm8  , %%xmm1 , %%xmm4 , %%xmm8		\n\t"
	"	vfnmaddpd	%%xmm12 , %%xmm2 , %%xmm4 , %%xmm12		\n\t"
	"	vfnmaddpd	%%xmm9  , %%xmm1 , %%xmm5 , %%xmm9		\n\t"
	"	vfnmaddpd	%%xmm13 , %%xmm2 , %%xmm5 , %%xmm13		\n\t"
	"	vfnmaddpd	%%xmm10 , %%xmm1 , %%xmm6 , %%xmm10		\n\t"
	"	vfnmaddpd	%%xmm14 , %%xmm2 , %%xmm6 , %%xmm14		\n\t"

	"	subq		$8, %1						\n\t" // i = 5
	"	subq		$2, %0						\n\t" // b-= n

	"	vmovddup	40(%6, %1, 8)  , %%xmm0 			\n\t"	// read aa[i]
	"	vunpckhpd	%%xmm10 , %%xmm10, %%xmm1			\n\t"	// extract bb0
	"	vunpckhpd	%%xmm14 , %%xmm14, %%xmm2			\n\t"	// extract bb1
	"	vmulpd		%%xmm0  , %%xmm1 , %%xmm1			\n\t"	// bb0 * aa
	"	vmulpd		%%xmm0  , %%xmm2 , %%xmm2			\n\t"	// bb0 * aa
	"	vmovsd		%%xmm1  , 40(%4)				\n\t"   // c[i] = bb0 * aa
	"	vmovsd		%%xmm2  , 40(%5)				\n\t"   // c[i] = bb1 * aa
	"	vmovsd		%%xmm1  ,   (%7 , %0, 8)			\n\t"   // b[0] = bb0 * aa
	"	vmovsd		%%xmm2  ,  8(%7 , %0, 8)			\n\t"   // b[1] = bb1 * aa

	"	vmovups	 	0(%6, %1, 8)  , %%xmm4				\n\t"   // read a[k]
	"	vmovups	       16(%6, %1, 8)  , %%xmm5				\n\t"   // read a[k]
	"	vmovsd 	       32(%6, %1, 8)  , %%xmm6				\n\t"   // read a[k]
	"	vfnmaddpd	%%xmm8  , %%xmm1 , %%xmm4 , %%xmm8		\n\t"
	"	vfnmaddpd	%%xmm12 , %%xmm2 , %%xmm4 , %%xmm12		\n\t"
	"	vfnmaddpd	%%xmm9  , %%xmm1 , %%xmm5 , %%xmm9		\n\t"
	"	vfnmaddpd	%%xmm13 , %%xmm2 , %%xmm5 , %%xmm13		\n\t"
	"	vfnmaddsd	%%xmm10 , %%xmm1 , %%xmm6 , %%xmm10		\n\t"
	"	vfnmaddsd	%%xmm14 , %%xmm2 , %%xmm6 , %%xmm14		\n\t"

	"	subq		$8, %1						\n\t" // i = 4
	"	subq		$2, %0						\n\t" // b-= n

	"	vmovddup	32(%6, %1, 8)  , %%xmm0 			\n\t"	// read aa[i]
	"	vunpcklpd	%%xmm10 , %%xmm10, %%xmm1			\n\t"	// extract bb0
	"	vunpcklpd	%%xmm14 , %%xmm14, %%xmm2			\n\t"	// extract bb1
	"	vmulpd		%%xmm0  , %%xmm1 , %%xmm1			\n\t"	// bb0 * aa
	"	vmulpd		%%xmm0  , %%xmm2 , %%xmm2			\n\t"	// bb0 * aa
	"	vmovsd		%%xmm1  , 32(%4)				\n\t"   // c[i] = bb0 * aa
	"	vmovsd		%%xmm2  , 32(%5)				\n\t"   // c[i] = bb1 * aa
	"	vmovsd		%%xmm1  ,   (%7 , %0, 8)			\n\t"   // b[0] = bb0 * aa
	"	vmovsd		%%xmm2  ,  8(%7 , %0, 8)			\n\t"   // b[1] = bb1 * aa


	"	vmovups	 	0(%6, %1, 8)  , %%xmm4				\n\t"   // read a[k]
	"	vmovups	       16(%6, %1, 8)  , %%xmm5				\n\t"   // read a[k]
	"	vfnmaddpd	%%xmm8  , %%xmm1 , %%xmm4 , %%xmm8		\n\t"
	"	vfnmaddpd	%%xmm12 , %%xmm2 , %%xmm4 , %%xmm12		\n\t"
	"	vfnmaddpd	%%xmm9  , %%xmm1 , %%xmm5 , %%xmm9		\n\t"
	"	vfnmaddpd	%%xmm13 , %%xmm2 , %%xmm5 , %%xmm13		\n\t"

	"	subq		$8, %1						\n\t" // i = 3
	"	subq		$2, %0						\n\t" // b-= n

	"	vmovddup	24(%6, %1, 8)  , %%xmm0 			\n\t"	// read aa[i]
	"	vunpckhpd	%%xmm9  , %%xmm9 , %%xmm1			\n\t"	// extract bb0
	"	vunpckhpd	%%xmm13 , %%xmm13, %%xmm2			\n\t"	// extract bb1
	"	vmulpd		%%xmm0  , %%xmm1 , %%xmm1			\n\t"	// bb0 * aa
	"	vmulpd		%%xmm0  , %%xmm2 , %%xmm2			\n\t"	// bb0 * aa
	"	vmovsd		%%xmm1  , 24(%4)				\n\t"   // c[i] = bb0 * aa
	"	vmovsd		%%xmm2  , 24(%5)				\n\t"   // c[i] = bb1 * aa
	"	vmovsd		%%xmm1  ,   (%7 , %0, 8)			\n\t"   // b[0] = bb0 * aa
	"	vmovsd		%%xmm2  ,  8(%7 , %0, 8)			\n\t"   // b[1] = bb1 * aa

	"	vmovups	 	0(%6, %1, 8)  , %%xmm4				\n\t"   // read a[k]
	"	vmovsd 	       16(%6, %1, 8)  , %%xmm5				\n\t"   // read a[k]
	"	vfnmaddpd	%%xmm8  , %%xmm1 , %%xmm4 , %%xmm8		\n\t"
	"	vfnmaddpd	%%xmm12 , %%xmm2 , %%xmm4 , %%xmm12		\n\t"
	"	vfnmaddsd	%%xmm9  , %%xmm1 , %%xmm5 , %%xmm9		\n\t"
	"	vfnmaddsd	%%xmm13 , %%xmm2 , %%xmm5 , %%xmm13		\n\t"

	"	subq		$8, %1						\n\t" // i = 2
	"	subq		$2, %0						\n\t" // b-= n

	"	vmovddup	16(%6, %1, 8)  , %%xmm0 			\n\t"	// read aa[i]
	"	vunpcklpd	%%xmm9  , %%xmm9 , %%xmm1			\n\t"	// extract bb0
	"	vunpcklpd	%%xmm13 , %%xmm13, %%xmm2			\n\t"	// extract bb1
	"	vmulpd		%%xmm0  , %%xmm1 , %%xmm1			\n\t"	// bb0 * aa
	"	vmulpd		%%xmm0  , %%xmm2 , %%xmm2			\n\t"	// bb0 * aa
	"	vmovsd		%%xmm1  , 16(%4)				\n\t"   // c[i] = bb0 * aa
	"	vmovsd		%%xmm2  , 16(%5)				\n\t"   // c[i] = bb1 * aa
	"	vmovsd		%%xmm1  ,   (%7 , %0, 8)			\n\t"   // b[0] = bb0 * aa
	"	vmovsd		%%xmm2  ,  8(%7 , %0, 8)			\n\t"   // b[1] = bb1 * aa

	"	vmovups	 	0(%6, %1, 8)  , %%xmm4				\n\t"   // read a[k]
	"	vfnmaddpd	%%xmm8  , %%xmm1 , %%xmm4 , %%xmm8		\n\t"
	"	vfnmaddpd	%%xmm12 , %%xmm2 , %%xmm4 , %%xmm12		\n\t"

	"	subq		$8, %1						\n\t" // i = 1
	"	subq		$2, %0						\n\t" // b-= n

	"	vmovddup	 8(%6, %1, 8)  , %%xmm0 			\n\t"	// read aa[i]
	"	vunpckhpd	%%xmm8  , %%xmm8 , %%xmm1			\n\t"	// extract bb0
	"	vunpckhpd	%%xmm12 , %%xmm12, %%xmm2			\n\t"	// extract bb1
	"	vmulpd		%%xmm0  , %%xmm1 , %%xmm1			\n\t"	// bb0 * aa
	"	vmulpd		%%xmm0  , %%xmm2 , %%xmm2			\n\t"	// bb0 * aa
	"	vmovsd		%%xmm1  ,  8(%4)				\n\t"   // c[i] = bb0 * aa
	"	vmovsd		%%xmm2  ,  8(%5)				\n\t"   // c[i] = bb1 * aa
	"	vmovsd		%%xmm1  ,   (%7 , %0, 8)			\n\t"   // b[0] = bb0 * aa
	"	vmovsd		%%xmm2  ,  8(%7 , %0, 8)			\n\t"   // b[1] = bb1 * aa

	"	vmovsd 	 	0(%6, %1, 8)  , %%xmm4				\n\t"   // read a[k]
	"	vfnmaddsd	%%xmm8  , %%xmm1 , %%xmm4 , %%xmm8		\n\t"
	"	vfnmaddsd	%%xmm12 , %%xmm2 , %%xmm4 , %%xmm12		\n\t"

	"	subq		$8, %1						\n\t" // i = 0
	"	subq		$2, %0						\n\t" // b-= n

	"	vmovddup	 0(%6, %1, 8)  , %%xmm0 			\n\t"	// read aa[i]
	"	vunpcklpd	%%xmm8  , %%xmm8 , %%xmm1			\n\t"	// extract bb0
	"	vunpcklpd	%%xmm12 , %%xmm12, %%xmm2			\n\t"	// extract bb1
	"	vmulpd		%%xmm0  , %%xmm1 , %%xmm1			\n\t"	// bb0 * aa
	"	vmulpd		%%xmm0  , %%xmm2 , %%xmm2			\n\t"	// bb0 * aa
	"	vmovsd		%%xmm1  ,  0(%4)				\n\t"   // c[i] = bb0 * aa
	"	vmovsd		%%xmm2  ,  0(%5)				\n\t"   // c[i] = bb1 * aa
	"	vmovsd		%%xmm1  ,   (%7 , %0, 8)			\n\t"   // b[0] = bb0 * aa
	"	vmovsd		%%xmm2  ,  8(%7 , %0, 8)			\n\t"   // b[1] = bb1 * aa

	"	vzeroupper							\n\t"

        :
        :
          "r" (n1),     // 0    
          "a" (i),      // 1    
          "r" (a),      // 2
          "r" (b),      // 3
          "r" (c),      // 4
          "r" (c1),     // 5
          "r" (as),     // 6
          "r" (bs)      // 7
        : "cc",
          "%xmm0", "%xmm1", "%xmm2", "%xmm3",
          "%xmm4", "%xmm5", "%xmm6", "%xmm7",
          "%xmm8", "%xmm9", "%xmm10", "%xmm11",
          "%xmm12", "%xmm13", "%xmm14", "%xmm15",
          "memory"
        );

}


#ifndef COMPLEX
static inline void solve(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {

  FLOAT aa,  bb;
  FLOAT *cj;


  BLASLONG i, j, k;

  a += (m - 1) * m;
  b += (m - 1) * n;

  for (i = m - 1; i >= 0; i--) {

    aa = *(a + i);

    for (j = 0; j < n; j ++) {
      cj = c + j * ldc;
      bb = *(cj + i);
      bb *= aa;
      *b             = bb;
      *(cj + i) = bb;
      b ++;

/*
      BLASLONG i1 = i & -4 ;
*/
      FLOAT t0,t1,t2,t3;

      k=0;


      if ( i & 4 )
      {
	t0 = cj[k];
	t1 = cj[k+1];
	t2 = cj[k+2];
	t3 = cj[k+3];

	t0 -= bb * a[k+0];
	t1 -= bb * a[k+1];
	t2 -= bb * a[k+2];
	t3 -= bb * a[k+3];

	cj[k+0] = t0;
	cj[k+1] = t1;
	cj[k+2] = t2;
	cj[k+3] = t3;

	k+=4;
      }

      if ( i & 2 )
      {
	t0 = a[k];
	t1 = a[k+1];

	t0 *= bb;
	t1 *= bb;

	cj[k+0] -= t0;
	cj[k+1] -= t1;

	k+=2;
      }

      if ( i & 1 ) 
      {
	t0 = bb * a[k];
	cj[k+0] -= t0;

      }


    }
    a -= m;
    b -= 2 * n;
  }

}

#else

static inline void solve(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {

  FLOAT aa1, aa2;
  FLOAT bb1, bb2;
  FLOAT cc1, cc2;

  int i, j, k;

  ldc *= 2;
  a += (m - 1) * m * 2;
  b += (m - 1) * n * 2;

  for (i = m - 1; i >= 0; i--) {

    aa1 = *(a + i * 2 + 0);
    aa2 = *(a + i * 2 + 1);

    for (j = 0; j < n; j ++) {
      bb1 = *(c + i * 2 + 0 + j * ldc);
      bb2 = *(c + i * 2 + 1 + j * ldc);

#ifndef CONJ
      cc1 = aa1 * bb1 - aa2 * bb2;
      cc2 = aa1 * bb2 + aa2 * bb1;
#else
      cc1 = aa1 * bb1 + aa2 * bb2;
      cc2 = aa1 * bb2 - aa2 * bb1;
#endif


      *(b + 0) = cc1;
      *(b + 1) = cc2;
      *(c + i * 2 + 0 + j * ldc) = cc1;
      *(c + i * 2 + 1 + j * ldc) = cc2;
      b += 2;

      for (k = 0; k < i; k ++){
#ifndef CONJ
	*(c + k * 2 + 0 + j * ldc) -= cc1 * *(a + k * 2 + 0) - cc2 * *(a + k * 2 + 1);
	*(c + k * 2 + 1 + j * ldc) -= cc1 * *(a + k * 2 + 1) + cc2 * *(a + k * 2 + 0);
#else
	*(c + k * 2 + 0 + j * ldc) -=   cc1 * *(a + k * 2 + 0) + cc2 * *(a + k * 2 + 1);
	*(c + k * 2 + 1 + j * ldc) -= - cc1 * *(a + k * 2 + 1) + cc2 * *(a + k * 2 + 0);
#endif
      }

    }
    a -= m * 2;
    b -= 4 * n;
  }

}

#endif


int CNAME(BLASLONG m, BLASLONG n, BLASLONG k,  FLOAT dummy1,
#ifdef COMPLEX
	   FLOAT dummy2,
#endif
	   FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG offset){

  BLASLONG i, j;
  FLOAT *aa, *cc;
  BLASLONG  kk;

#if 0
  fprintf(stderr, "TRSM KERNEL LN : m = %3ld  n = %3ld  k = %3ld offset = %3ld\n",
	  m, n, k, offset);
#endif

  j = (n >> GEMM_UNROLL_N_SHIFT);

  while (j > 0) {

    kk = m + offset;

    if (m & (GEMM_UNROLL_M - 1)) {
      for (i = 1; i < GEMM_UNROLL_M; i *= 2){
	if (m & i) {
	  aa = a + ((m & ~(i - 1)) - i) * k * COMPSIZE;
	  cc = c + ((m & ~(i - 1)) - i)     * COMPSIZE;

	  if (k - kk > 0) {

	    GEMM_KERNEL(i, GEMM_UNROLL_N, k - kk, dm1,
#ifdef COMPLEX
			ZERO,
#endif
			aa + i             * kk * COMPSIZE,
			b  + GEMM_UNROLL_N * kk * COMPSIZE,
			cc,
			ldc);

	  }

	  solve(i, GEMM_UNROLL_N,
		aa + (kk - i) * i             * COMPSIZE,
		b  + (kk - i) * GEMM_UNROLL_N * COMPSIZE,
		cc, ldc);

	  kk -= i;
	}
      }
    }

    i = (m >> GEMM_UNROLL_M_SHIFT);
    if (i > 0) {
      aa = a + ((m & ~(GEMM_UNROLL_M - 1)) - GEMM_UNROLL_M) * k * COMPSIZE;
      cc = c + ((m & ~(GEMM_UNROLL_M - 1)) - GEMM_UNROLL_M)     * COMPSIZE;

      do {
		dtrsm_LN_solve_opt(k-kk, aa + GEMM_UNROLL_M * kk * COMPSIZE, b +  GEMM_UNROLL_N * kk * COMPSIZE, cc, ldc
                             , aa + (kk - GEMM_UNROLL_M) * GEMM_UNROLL_M * COMPSIZE,b  + (kk - GEMM_UNROLL_M) * GEMM_UNROLL_N * COMPSIZE);
	

		aa -= GEMM_UNROLL_M * k * COMPSIZE;
		cc -= GEMM_UNROLL_M     * COMPSIZE;
		kk -= GEMM_UNROLL_M;
		i --;
         } while (i > 0);
    }

    b += GEMM_UNROLL_N * k * COMPSIZE;
    c += GEMM_UNROLL_N * ldc * COMPSIZE;
    j --;
  }

  if (n & (GEMM_UNROLL_N - 1)) {

    j = (GEMM_UNROLL_N >> 1);
    while (j > 0) {
      if (n & j) {

	kk = m + offset;

	if (m & (GEMM_UNROLL_M - 1)) {
	  for (i = 1; i < GEMM_UNROLL_M; i *= 2){
	    if (m & i) {
	      aa = a + ((m & ~(i - 1)) - i) * k * COMPSIZE;
	      cc = c + ((m & ~(i - 1)) - i)     * COMPSIZE;

	      if (k - kk > 0) {
		GEMM_KERNEL(i, j, k - kk, dm1,
#ifdef COMPLEX
			    ZERO,
#endif
			    aa + i * kk * COMPSIZE,
			    b  + j * kk * COMPSIZE,
			    cc, ldc);
	      }

	      solve(i, j,
		    aa + (kk - i) * i * COMPSIZE,
		    b  + (kk - i) * j * COMPSIZE,
		    cc, ldc);

	      kk -= i;
	    }
	  }
	}

	i = (m >> GEMM_UNROLL_M_SHIFT);
	if (i > 0) {
	  aa = a + ((m & ~(GEMM_UNROLL_M - 1)) - GEMM_UNROLL_M) * k * COMPSIZE;
	  cc = c + ((m & ~(GEMM_UNROLL_M - 1)) - GEMM_UNROLL_M)     * COMPSIZE;

	  do {
	    if (k - kk > 0) {
	      GEMM_KERNEL(GEMM_UNROLL_M, j, k - kk, dm1,
#ifdef COMPLEX
			  ZERO,
#endif
			  aa + GEMM_UNROLL_M * kk * COMPSIZE,
			  b +  j             * kk * COMPSIZE,
			  cc,
			  ldc);
	    }

	    solve(GEMM_UNROLL_M, j,
		  aa + (kk - GEMM_UNROLL_M) * GEMM_UNROLL_M * COMPSIZE,
		  b  + (kk - GEMM_UNROLL_M) * j             * COMPSIZE,
		  cc, ldc);

	    aa -= GEMM_UNROLL_M * k * COMPSIZE;
	    cc -= GEMM_UNROLL_M     * COMPSIZE;
	    kk -= GEMM_UNROLL_M;
	    i --;
	  } while (i > 0);
	}

	b += j * k   * COMPSIZE;
	c += j * ldc * COMPSIZE;
      }
      j >>= 1;
    }
  }

  return 0;
}
