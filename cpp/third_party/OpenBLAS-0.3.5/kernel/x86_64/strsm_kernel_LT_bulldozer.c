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



static void strsm_LT_solve_opt(BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, FLOAT *as, FLOAT *bs)  __attribute__ ((noinline));

static void strsm_LT_solve_opt(BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, FLOAT *as, FLOAT *bs)
{

	FLOAT *c1 = c + ldc ;
	BLASLONG n1 = n * 8;
	BLASLONG i=0;

        __asm__  __volatile__
        (
	"	vzeroupper							\n\t"
	"	prefetcht0	(%4)						\n\t"
	"	prefetcht0	(%5)						\n\t"
	"	vxorps	%%xmm8 , %%xmm8 , %%xmm8				\n\t"
	"	vxorps	%%xmm9 , %%xmm9 , %%xmm9				\n\t"
	"	vxorps	%%xmm10, %%xmm10, %%xmm10				\n\t"
	"	vxorps	%%xmm11, %%xmm11, %%xmm11				\n\t"
	"	vxorps	%%xmm12, %%xmm12, %%xmm12				\n\t"
	"	vxorps	%%xmm13, %%xmm13, %%xmm13				\n\t"
	"	vxorps	%%xmm14, %%xmm14, %%xmm14				\n\t"
	"	vxorps	%%xmm15, %%xmm15, %%xmm15				\n\t"

	"	cmpq	       $0, %0						\n\t"
	"	je	       2f						\n\t"

	"	.align 16							\n\t"
	"1:									\n\t"

	"	vbroadcastss	(%3,%1,1), %%xmm0				\n\t"	// read b
	"	vmovups         (%2,%1,8), %%xmm4				\n\t"
	"	vbroadcastss   4(%3,%1,1), %%xmm1				\n\t"	
	"	vmovups       16(%2,%1,8), %%xmm5				\n\t"
	"	vmovups       32(%2,%1,8), %%xmm6				\n\t"
	"	vmovups       48(%2,%1,8), %%xmm7				\n\t"

	"	vfmaddps	%%xmm8 , %%xmm0 , %%xmm4 , %%xmm8		\n\t"
	"	vfmaddps	%%xmm12, %%xmm1 , %%xmm4 , %%xmm12		\n\t"
	"	vfmaddps	%%xmm9 , %%xmm0 , %%xmm5 , %%xmm9		\n\t"
	"	vfmaddps	%%xmm13, %%xmm1 , %%xmm5 , %%xmm13		\n\t"
	"	vfmaddps	%%xmm10, %%xmm0 , %%xmm6 , %%xmm10		\n\t"
	"	vfmaddps	%%xmm14, %%xmm1 , %%xmm6 , %%xmm14		\n\t"
	"	addq		$8, %1						\n\t"
	"	vfmaddps	%%xmm11, %%xmm0 , %%xmm7 , %%xmm11		\n\t"
	"	vfmaddps	%%xmm15, %%xmm1 , %%xmm7 , %%xmm15		\n\t"
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

	"	vsubps		%%xmm8 , %%xmm0 , %%xmm8			\n\t"
	"	vsubps		%%xmm9 , %%xmm1 , %%xmm9			\n\t"
	"	vsubps		%%xmm10, %%xmm2 , %%xmm10			\n\t"
	"	vsubps		%%xmm11, %%xmm3 , %%xmm11			\n\t"

	"	vsubps		%%xmm12, %%xmm4 , %%xmm12			\n\t"
	"	vsubps		%%xmm13, %%xmm5 , %%xmm13			\n\t"
	"	vsubps		%%xmm14, %%xmm6 , %%xmm14			\n\t"
	"	vsubps		%%xmm15, %%xmm7 , %%xmm15			\n\t"

	"3:									\n\t"	

	"	vbroadcastss     0(%6) , %%xmm0					\n\t" // i=0, read aa[i]		
	"	vshufps		$0x00  , %%xmm8  , %%xmm8  , %%xmm1		\n\t" // extract bb0
	"	vshufps		$0x00  , %%xmm12 , %%xmm12 , %%xmm2		\n\t" // extract bb1
	"       vmulps          %%xmm0  , %%xmm1 , %%xmm1                       \n\t"   // bb0 * aa
	"       vmulps          %%xmm0  , %%xmm2 , %%xmm2                       \n\t"   // bb1 * aa
        "       vmovss          %%xmm1  ,  0(%4)                                \n\t"   // c[i] = bb0 * aa
        "       vmovss          %%xmm2  ,  0(%5)                                \n\t"   // c[i] = bb1 * aa
        "       vmovss          %%xmm1  ,   (%7)                        	\n\t"   // b[0] = bb0 * aa
        "       vmovss          %%xmm2  ,  4(%7)                        	\n\t"   // b[1] = bb1 * aa

	"       vmovups         0(%6)  , %%xmm4                                 \n\t"   // read a[k]
	"       vmovups        16(%6)  , %%xmm5                                 \n\t"   // read a[k]
	"       vmovups        32(%6)  , %%xmm6                                 \n\t"   // read a[k]
	"       vmovups        48(%6)  , %%xmm7                                 \n\t"   // read a[k]
	"       vfnmaddps       %%xmm8  , %%xmm1 , %%xmm4 , %%xmm8              \n\t"
        "       vfnmaddps       %%xmm12 , %%xmm2 , %%xmm4 , %%xmm12             \n\t"
        "       vfnmaddps       %%xmm9  , %%xmm1 , %%xmm5 , %%xmm9              \n\t"
        "       vfnmaddps       %%xmm13 , %%xmm2 , %%xmm5 , %%xmm13             \n\t"
        "       vfnmaddps       %%xmm10 , %%xmm1 , %%xmm6 , %%xmm10             \n\t"
        "       vfnmaddps       %%xmm14 , %%xmm2 , %%xmm6 , %%xmm14             \n\t"
        "       vfnmaddps       %%xmm11 , %%xmm1 , %%xmm7 , %%xmm11             \n\t"
        "       vfnmaddps       %%xmm15 , %%xmm2 , %%xmm7 , %%xmm15             \n\t"

	"	addq		$64 , %6					\n\t"   // a -= m
	"	addq		$8  , %7					\n\t"   // b -= n

	"	vbroadcastss     4(%6) , %%xmm0					\n\t" // i=1, read aa[i]		
	"	vshufps		$0x55  , %%xmm8  , %%xmm8  , %%xmm1		\n\t" // extract bb0
	"	vshufps		$0x55  , %%xmm12 , %%xmm12 , %%xmm2		\n\t" // extract bb1
	"       vmulps          %%xmm0  , %%xmm1 , %%xmm1                       \n\t"   // bb0 * aa
	"       vmulps          %%xmm0  , %%xmm2 , %%xmm2                       \n\t"   // bb1 * aa
        "       vmovss          %%xmm1  ,  4(%4)                                \n\t"   // c[i] = bb0 * aa
        "       vmovss          %%xmm2  ,  4(%5)                                \n\t"   // c[i] = bb1 * aa
        "       vmovss          %%xmm1  ,   (%7)                        	\n\t"   // b[0] = bb0 * aa
        "       vmovss          %%xmm2  ,  4(%7)                        	\n\t"   // b[1] = bb1 * aa

	"       vmovups         0(%6)  , %%xmm4                                 \n\t"   // read a[k]
	"       vmovups        16(%6)  , %%xmm5                                 \n\t"   // read a[k]
	"       vmovups        32(%6)  , %%xmm6                                 \n\t"   // read a[k]
	"       vmovups        48(%6)  , %%xmm7                                 \n\t"   // read a[k]
	"       vfnmaddps       %%xmm8  , %%xmm1 , %%xmm4 , %%xmm8              \n\t"
        "       vfnmaddps       %%xmm12 , %%xmm2 , %%xmm4 , %%xmm12             \n\t"
        "       vfnmaddps       %%xmm9  , %%xmm1 , %%xmm5 , %%xmm9              \n\t"
        "       vfnmaddps       %%xmm13 , %%xmm2 , %%xmm5 , %%xmm13             \n\t"
        "       vfnmaddps       %%xmm10 , %%xmm1 , %%xmm6 , %%xmm10             \n\t"
        "       vfnmaddps       %%xmm14 , %%xmm2 , %%xmm6 , %%xmm14             \n\t"
        "       vfnmaddps       %%xmm11 , %%xmm1 , %%xmm7 , %%xmm11             \n\t"
        "       vfnmaddps       %%xmm15 , %%xmm2 , %%xmm7 , %%xmm15             \n\t"

	"	addq		$64 , %6					\n\t"   // a -= m
	"	addq		$8  , %7					\n\t"   // b -= n

	"	vbroadcastss     8(%6) , %%xmm0					\n\t" // i=2, read aa[i]		
	"	vshufps		$0xaa  , %%xmm8  , %%xmm8  , %%xmm1		\n\t" // extract bb0
	"	vshufps		$0xaa  , %%xmm12 , %%xmm12 , %%xmm2		\n\t" // extract bb1
	"       vmulps          %%xmm0  , %%xmm1 , %%xmm1                       \n\t"   // bb0 * aa
	"       vmulps          %%xmm0  , %%xmm2 , %%xmm2                       \n\t"   // bb1 * aa
        "       vmovss          %%xmm1  ,  8(%4)                                \n\t"   // c[i] = bb0 * aa
        "       vmovss          %%xmm2  ,  8(%5)                                \n\t"   // c[i] = bb1 * aa
        "       vmovss          %%xmm1  ,   (%7)                        	\n\t"   // b[0] = bb0 * aa
        "       vmovss          %%xmm2  ,  4(%7)                        	\n\t"   // b[1] = bb1 * aa

	"       vmovups         0(%6)  , %%xmm4                                 \n\t"   // read a[k]
	"       vmovups        16(%6)  , %%xmm5                                 \n\t"   // read a[k]
	"       vmovups        32(%6)  , %%xmm6                                 \n\t"   // read a[k]
	"       vmovups        48(%6)  , %%xmm7                                 \n\t"   // read a[k]
	"       vfnmaddps       %%xmm8  , %%xmm1 , %%xmm4 , %%xmm8              \n\t"
        "       vfnmaddps       %%xmm12 , %%xmm2 , %%xmm4 , %%xmm12             \n\t"
        "       vfnmaddps       %%xmm9  , %%xmm1 , %%xmm5 , %%xmm9              \n\t"
        "       vfnmaddps       %%xmm13 , %%xmm2 , %%xmm5 , %%xmm13             \n\t"
        "       vfnmaddps       %%xmm10 , %%xmm1 , %%xmm6 , %%xmm10             \n\t"
        "       vfnmaddps       %%xmm14 , %%xmm2 , %%xmm6 , %%xmm14             \n\t"
        "       vfnmaddps       %%xmm11 , %%xmm1 , %%xmm7 , %%xmm11             \n\t"
        "       vfnmaddps       %%xmm15 , %%xmm2 , %%xmm7 , %%xmm15             \n\t"

	"	addq		$64 , %6					\n\t"   // a -= m
	"	addq		$8  , %7					\n\t"   // b -= n

	"	vbroadcastss    12(%6) , %%xmm0					\n\t" // i=3, read aa[i]		
	"	vshufps		$0xff  , %%xmm8  , %%xmm8  , %%xmm1		\n\t" // extract bb0
	"	vshufps		$0xff  , %%xmm12 , %%xmm12 , %%xmm2		\n\t" // extract bb1
	"       vmulps          %%xmm0  , %%xmm1 , %%xmm1                       \n\t"   // bb0 * aa
	"       vmulps          %%xmm0  , %%xmm2 , %%xmm2                       \n\t"   // bb1 * aa
        "       vmovss          %%xmm1  , 12(%4)                                \n\t"   // c[i] = bb0 * aa
        "       vmovss          %%xmm2  , 12(%5)                                \n\t"   // c[i] = bb1 * aa
        "       vmovss          %%xmm1  ,   (%7)                        	\n\t"   // b[0] = bb0 * aa
        "       vmovss          %%xmm2  ,  4(%7)                        	\n\t"   // b[1] = bb1 * aa

	"       vmovups        16(%6)  , %%xmm5                                 \n\t"   // read a[k]
	"       vmovups        32(%6)  , %%xmm6                                 \n\t"   // read a[k]
	"       vmovups        48(%6)  , %%xmm7                                 \n\t"   // read a[k]
        "       vfnmaddps       %%xmm9  , %%xmm1 , %%xmm5 , %%xmm9              \n\t"
        "       vfnmaddps       %%xmm13 , %%xmm2 , %%xmm5 , %%xmm13             \n\t"
        "       vfnmaddps       %%xmm10 , %%xmm1 , %%xmm6 , %%xmm10             \n\t"
        "       vfnmaddps       %%xmm14 , %%xmm2 , %%xmm6 , %%xmm14             \n\t"
        "       vfnmaddps       %%xmm11 , %%xmm1 , %%xmm7 , %%xmm11             \n\t"
        "       vfnmaddps       %%xmm15 , %%xmm2 , %%xmm7 , %%xmm15             \n\t"

	"	addq		$64 , %6					\n\t"   // a -= m
	"	addq		$8  , %7					\n\t"   // b -= n

	"	vbroadcastss    16(%6) , %%xmm0					\n\t" // i=4, read aa[i]		
	"	vshufps		$0x00  , %%xmm9  , %%xmm9  , %%xmm1		\n\t" // extract bb0
	"	vshufps		$0x00  , %%xmm13 , %%xmm13 , %%xmm2		\n\t" // extract bb1
	"       vmulps          %%xmm0  , %%xmm1 , %%xmm1                       \n\t"   // bb0 * aa
	"       vmulps          %%xmm0  , %%xmm2 , %%xmm2                       \n\t"   // bb1 * aa
        "       vmovss          %%xmm1  , 16(%4)                                \n\t"   // c[i] = bb0 * aa
        "       vmovss          %%xmm2  , 16(%5)                                \n\t"   // c[i] = bb1 * aa
        "       vmovss          %%xmm1  ,   (%7)                        	\n\t"   // b[0] = bb0 * aa
        "       vmovss          %%xmm2  ,  4(%7)                        	\n\t"   // b[1] = bb1 * aa

	"       vmovups        16(%6)  , %%xmm5                                 \n\t"   // read a[k]
	"       vmovups        32(%6)  , %%xmm6                                 \n\t"   // read a[k]
	"       vmovups        48(%6)  , %%xmm7                                 \n\t"   // read a[k]
        "       vfnmaddps       %%xmm9  , %%xmm1 , %%xmm5 , %%xmm9              \n\t"
        "       vfnmaddps       %%xmm13 , %%xmm2 , %%xmm5 , %%xmm13             \n\t"
        "       vfnmaddps       %%xmm10 , %%xmm1 , %%xmm6 , %%xmm10             \n\t"
        "       vfnmaddps       %%xmm14 , %%xmm2 , %%xmm6 , %%xmm14             \n\t"
        "       vfnmaddps       %%xmm11 , %%xmm1 , %%xmm7 , %%xmm11             \n\t"
        "       vfnmaddps       %%xmm15 , %%xmm2 , %%xmm7 , %%xmm15             \n\t"

	"	addq		$64 , %6					\n\t"   // a -= m
	"	addq		$8  , %7					\n\t"   // b -= n

	"	vbroadcastss    20(%6) , %%xmm0					\n\t" // i=5, read aa[i]		
	"	vshufps		$0x55  , %%xmm9  , %%xmm9  , %%xmm1		\n\t" // extract bb0
	"	vshufps		$0x55  , %%xmm13 , %%xmm13 , %%xmm2		\n\t" // extract bb1
	"       vmulps          %%xmm0  , %%xmm1 , %%xmm1                       \n\t"   // bb0 * aa
	"       vmulps          %%xmm0  , %%xmm2 , %%xmm2                       \n\t"   // bb1 * aa
        "       vmovss          %%xmm1  , 20(%4)                                \n\t"   // c[i] = bb0 * aa
        "       vmovss          %%xmm2  , 20(%5)                                \n\t"   // c[i] = bb1 * aa
        "       vmovss          %%xmm1  ,   (%7)                        	\n\t"   // b[0] = bb0 * aa
        "       vmovss          %%xmm2  ,  4(%7)                        	\n\t"   // b[1] = bb1 * aa

	"       vmovups        16(%6)  , %%xmm5                                 \n\t"   // read a[k]
	"       vmovups        32(%6)  , %%xmm6                                 \n\t"   // read a[k]
	"       vmovups        48(%6)  , %%xmm7                                 \n\t"   // read a[k]
        "       vfnmaddps       %%xmm9  , %%xmm1 , %%xmm5 , %%xmm9              \n\t"
        "       vfnmaddps       %%xmm13 , %%xmm2 , %%xmm5 , %%xmm13             \n\t"
        "       vfnmaddps       %%xmm10 , %%xmm1 , %%xmm6 , %%xmm10             \n\t"
        "       vfnmaddps       %%xmm14 , %%xmm2 , %%xmm6 , %%xmm14             \n\t"
        "       vfnmaddps       %%xmm11 , %%xmm1 , %%xmm7 , %%xmm11             \n\t"
        "       vfnmaddps       %%xmm15 , %%xmm2 , %%xmm7 , %%xmm15             \n\t"

	"	addq		$64 , %6					\n\t"   // a -= m
	"	addq		$8  , %7					\n\t"   // b -= n

	"	vbroadcastss    24(%6) , %%xmm0					\n\t" // i=6, read aa[i]		
	"	vshufps		$0xaa  , %%xmm9  , %%xmm9  , %%xmm1		\n\t" // extract bb0
	"	vshufps		$0xaa  , %%xmm13 , %%xmm13 , %%xmm2		\n\t" // extract bb1
	"       vmulps          %%xmm0  , %%xmm1 , %%xmm1                       \n\t"   // bb0 * aa
	"       vmulps          %%xmm0  , %%xmm2 , %%xmm2                       \n\t"   // bb1 * aa
        "       vmovss          %%xmm1  , 24(%4)                                \n\t"   // c[i] = bb0 * aa
        "       vmovss          %%xmm2  , 24(%5)                                \n\t"   // c[i] = bb1 * aa
        "       vmovss          %%xmm1  ,   (%7)                        	\n\t"   // b[0] = bb0 * aa
        "       vmovss          %%xmm2  ,  4(%7)                        	\n\t"   // b[1] = bb1 * aa

	"       vmovups        16(%6)  , %%xmm5                                 \n\t"   // read a[k]
	"       vmovups        32(%6)  , %%xmm6                                 \n\t"   // read a[k]
	"       vmovups        48(%6)  , %%xmm7                                 \n\t"   // read a[k]
        "       vfnmaddps       %%xmm9  , %%xmm1 , %%xmm5 , %%xmm9              \n\t"
        "       vfnmaddps       %%xmm13 , %%xmm2 , %%xmm5 , %%xmm13             \n\t"
        "       vfnmaddps       %%xmm10 , %%xmm1 , %%xmm6 , %%xmm10             \n\t"
        "       vfnmaddps       %%xmm14 , %%xmm2 , %%xmm6 , %%xmm14             \n\t"
        "       vfnmaddps       %%xmm11 , %%xmm1 , %%xmm7 , %%xmm11             \n\t"
        "       vfnmaddps       %%xmm15 , %%xmm2 , %%xmm7 , %%xmm15             \n\t"

	"	addq		$64 , %6					\n\t"   // a -= m
	"	addq		$8  , %7					\n\t"   // b -= n

	"	vbroadcastss    28(%6) , %%xmm0					\n\t" // i=7, read aa[i]		
	"	vshufps		$0xff  , %%xmm9  , %%xmm9  , %%xmm1		\n\t" // extract bb0
	"	vshufps		$0xff  , %%xmm13 , %%xmm13 , %%xmm2		\n\t" // extract bb1
	"       vmulps          %%xmm0  , %%xmm1 , %%xmm1                       \n\t"   // bb0 * aa
	"       vmulps          %%xmm0  , %%xmm2 , %%xmm2                       \n\t"   // bb1 * aa
        "       vmovss          %%xmm1  , 28(%4)                                \n\t"   // c[i] = bb0 * aa
        "       vmovss          %%xmm2  , 28(%5)                                \n\t"   // c[i] = bb1 * aa
        "       vmovss          %%xmm1  ,   (%7)                        	\n\t"   // b[0] = bb0 * aa
        "       vmovss          %%xmm2  ,  4(%7)                        	\n\t"   // b[1] = bb1 * aa

	"       vmovups        32(%6)  , %%xmm6                                 \n\t"   // read a[k]
	"       vmovups        48(%6)  , %%xmm7                                 \n\t"   // read a[k]
        "       vfnmaddps       %%xmm10 , %%xmm1 , %%xmm6 , %%xmm10             \n\t"
        "       vfnmaddps       %%xmm14 , %%xmm2 , %%xmm6 , %%xmm14             \n\t"
        "       vfnmaddps       %%xmm11 , %%xmm1 , %%xmm7 , %%xmm11             \n\t"
        "       vfnmaddps       %%xmm15 , %%xmm2 , %%xmm7 , %%xmm15             \n\t"

	"	addq		$64 , %6					\n\t"   // a -= m
	"	addq		$8  , %7					\n\t"   // b -= n

	"	vbroadcastss    32(%6) , %%xmm0					\n\t" // i=8, read aa[i]		
	"	vshufps		$0x00  , %%xmm10 , %%xmm10 , %%xmm1		\n\t" // extract bb0
	"	vshufps		$0x00  , %%xmm14 , %%xmm14 , %%xmm2		\n\t" // extract bb1
	"       vmulps          %%xmm0  , %%xmm1 , %%xmm1                       \n\t"   // bb0 * aa
	"       vmulps          %%xmm0  , %%xmm2 , %%xmm2                       \n\t"   // bb1 * aa
        "       vmovss          %%xmm1  , 32(%4)                                \n\t"   // c[i] = bb0 * aa
        "       vmovss          %%xmm2  , 32(%5)                                \n\t"   // c[i] = bb1 * aa
        "       vmovss          %%xmm1  ,   (%7)                        	\n\t"   // b[0] = bb0 * aa
        "       vmovss          %%xmm2  ,  4(%7)                        	\n\t"   // b[1] = bb1 * aa

	"       vmovups        32(%6)  , %%xmm6                                 \n\t"   // read a[k]
	"       vmovups        48(%6)  , %%xmm7                                 \n\t"   // read a[k]
        "       vfnmaddps       %%xmm10 , %%xmm1 , %%xmm6 , %%xmm10             \n\t"
        "       vfnmaddps       %%xmm14 , %%xmm2 , %%xmm6 , %%xmm14             \n\t"
        "       vfnmaddps       %%xmm11 , %%xmm1 , %%xmm7 , %%xmm11             \n\t"
        "       vfnmaddps       %%xmm15 , %%xmm2 , %%xmm7 , %%xmm15             \n\t"

	"	addq		$64 , %6					\n\t"   // a -= m
	"	addq		$8  , %7					\n\t"   // b -= n

	"	vbroadcastss    36(%6) , %%xmm0					\n\t" // i=9, read aa[i]		
	"	vshufps		$0x55  , %%xmm10 , %%xmm10 , %%xmm1		\n\t" // extract bb0
	"	vshufps		$0x55  , %%xmm14 , %%xmm14 , %%xmm2		\n\t" // extract bb1
	"       vmulps          %%xmm0  , %%xmm1 , %%xmm1                       \n\t"   // bb0 * aa
	"       vmulps          %%xmm0  , %%xmm2 , %%xmm2                       \n\t"   // bb1 * aa
        "       vmovss          %%xmm1  , 36(%4)                                \n\t"   // c[i] = bb0 * aa
        "       vmovss          %%xmm2  , 36(%5)                                \n\t"   // c[i] = bb1 * aa
        "       vmovss          %%xmm1  ,   (%7)                        	\n\t"   // b[0] = bb0 * aa
        "       vmovss          %%xmm2  ,  4(%7)                        	\n\t"   // b[1] = bb1 * aa

	"       vmovups        32(%6)  , %%xmm6                                 \n\t"   // read a[k]
	"       vmovups        48(%6)  , %%xmm7                                 \n\t"   // read a[k]
        "       vfnmaddps       %%xmm10 , %%xmm1 , %%xmm6 , %%xmm10             \n\t"
        "       vfnmaddps       %%xmm14 , %%xmm2 , %%xmm6 , %%xmm14             \n\t"
        "       vfnmaddps       %%xmm11 , %%xmm1 , %%xmm7 , %%xmm11             \n\t"
        "       vfnmaddps       %%xmm15 , %%xmm2 , %%xmm7 , %%xmm15             \n\t"

	"	addq		$64 , %6					\n\t"   // a -= m
	"	addq		$8  , %7					\n\t"   // b -= n

	"	vbroadcastss    40(%6) , %%xmm0					\n\t" // i=10, read aa[i]		
	"	vshufps		$0xaa  , %%xmm10 , %%xmm10 , %%xmm1		\n\t" // extract bb0
	"	vshufps		$0xaa  , %%xmm14 , %%xmm14 , %%xmm2		\n\t" // extract bb1
	"       vmulps          %%xmm0  , %%xmm1 , %%xmm1                       \n\t"   // bb0 * aa
	"       vmulps          %%xmm0  , %%xmm2 , %%xmm2                       \n\t"   // bb1 * aa
        "       vmovss          %%xmm1  , 40(%4)                                \n\t"   // c[i] = bb0 * aa
        "       vmovss          %%xmm2  , 40(%5)                                \n\t"   // c[i] = bb1 * aa
        "       vmovss          %%xmm1  ,   (%7)                        	\n\t"   // b[0] = bb0 * aa
        "       vmovss          %%xmm2  ,  4(%7)                        	\n\t"   // b[1] = bb1 * aa

	"       vmovups        32(%6)  , %%xmm6                                 \n\t"   // read a[k]
	"       vmovups        48(%6)  , %%xmm7                                 \n\t"   // read a[k]
        "       vfnmaddps       %%xmm10 , %%xmm1 , %%xmm6 , %%xmm10             \n\t"
        "       vfnmaddps       %%xmm14 , %%xmm2 , %%xmm6 , %%xmm14             \n\t"
        "       vfnmaddps       %%xmm11 , %%xmm1 , %%xmm7 , %%xmm11             \n\t"
        "       vfnmaddps       %%xmm15 , %%xmm2 , %%xmm7 , %%xmm15             \n\t"

	"	addq		$64 , %6					\n\t"   // a -= m
	"	addq		$8  , %7					\n\t"   // b -= n

	"	vbroadcastss    44(%6) , %%xmm0					\n\t" // i=11, read aa[i]		
	"	vshufps		$0xff  , %%xmm10 , %%xmm10 , %%xmm1		\n\t" // extract bb0
	"	vshufps		$0xff  , %%xmm14 , %%xmm14 , %%xmm2		\n\t" // extract bb1
	"       vmulps          %%xmm0  , %%xmm1 , %%xmm1                       \n\t"   // bb0 * aa
	"       vmulps          %%xmm0  , %%xmm2 , %%xmm2                       \n\t"   // bb1 * aa
        "       vmovss          %%xmm1  , 44(%4)                                \n\t"   // c[i] = bb0 * aa
        "       vmovss          %%xmm2  , 44(%5)                                \n\t"   // c[i] = bb1 * aa
        "       vmovss          %%xmm1  ,   (%7)                        	\n\t"   // b[0] = bb0 * aa
        "       vmovss          %%xmm2  ,  4(%7)                        	\n\t"   // b[1] = bb1 * aa

	"       vmovups        48(%6)  , %%xmm7                                 \n\t"   // read a[k]
        "       vfnmaddps       %%xmm11 , %%xmm1 , %%xmm7 , %%xmm11             \n\t"
        "       vfnmaddps       %%xmm15 , %%xmm2 , %%xmm7 , %%xmm15             \n\t"

	"	addq		$64 , %6					\n\t"   // a -= m
	"	addq		$8  , %7					\n\t"   // b -= n

	"	vbroadcastss    48(%6) , %%xmm0					\n\t" // i=12, read aa[i]		
	"	vshufps		$0x00  , %%xmm11 , %%xmm11 , %%xmm1		\n\t" // extract bb0
	"	vshufps		$0x00  , %%xmm15 , %%xmm15 , %%xmm2		\n\t" // extract bb1
	"       vmulps          %%xmm0  , %%xmm1 , %%xmm1                       \n\t"   // bb0 * aa
	"       vmulps          %%xmm0  , %%xmm2 , %%xmm2                       \n\t"   // bb1 * aa
        "       vmovss          %%xmm1  , 48(%4)                                \n\t"   // c[i] = bb0 * aa
        "       vmovss          %%xmm2  , 48(%5)                                \n\t"   // c[i] = bb1 * aa
        "       vmovss          %%xmm1  ,   (%7)                        	\n\t"   // b[0] = bb0 * aa
        "       vmovss          %%xmm2  ,  4(%7)                        	\n\t"   // b[1] = bb1 * aa

	"       vmovups        48(%6)  , %%xmm7                                 \n\t"   // read a[k]
        "       vfnmaddps       %%xmm11 , %%xmm1 , %%xmm7 , %%xmm11             \n\t"
        "       vfnmaddps       %%xmm15 , %%xmm2 , %%xmm7 , %%xmm15             \n\t"

	"	addq		$64 , %6					\n\t"   // a -= m
	"	addq		$8  , %7					\n\t"   // b -= n

	"	vbroadcastss    52(%6) , %%xmm0					\n\t" // i=13, read aa[i]		
	"	vshufps		$0x55  , %%xmm11 , %%xmm11 , %%xmm1		\n\t" // extract bb0
	"	vshufps		$0x55  , %%xmm15 , %%xmm15 , %%xmm2		\n\t" // extract bb1
	"       vmulps          %%xmm0  , %%xmm1 , %%xmm1                       \n\t"   // bb0 * aa
	"       vmulps          %%xmm0  , %%xmm2 , %%xmm2                       \n\t"   // bb1 * aa
        "       vmovss          %%xmm1  , 52(%4)                                \n\t"   // c[i] = bb0 * aa
        "       vmovss          %%xmm2  , 52(%5)                                \n\t"   // c[i] = bb1 * aa
        "       vmovss          %%xmm1  ,   (%7)                        	\n\t"   // b[0] = bb0 * aa
        "       vmovss          %%xmm2  ,  4(%7)                        	\n\t"   // b[1] = bb1 * aa

	"       vmovups        48(%6)  , %%xmm7                                 \n\t"   // read a[k]
        "       vfnmaddps       %%xmm11 , %%xmm1 , %%xmm7 , %%xmm11             \n\t"
        "       vfnmaddps       %%xmm15 , %%xmm2 , %%xmm7 , %%xmm15             \n\t"

	"	addq		$64 , %6					\n\t"   // a -= m
	"	addq		$8  , %7					\n\t"   // b -= n

	"	vbroadcastss    56(%6) , %%xmm0					\n\t" // i=14, read aa[i]		
	"	vshufps		$0xaa  , %%xmm11 , %%xmm11 , %%xmm1		\n\t" // extract bb0
	"	vshufps		$0xaa  , %%xmm15 , %%xmm15 , %%xmm2		\n\t" // extract bb1
	"       vmulps          %%xmm0  , %%xmm1 , %%xmm1                       \n\t"   // bb0 * aa
	"       vmulps          %%xmm0  , %%xmm2 , %%xmm2                       \n\t"   // bb1 * aa
        "       vmovss          %%xmm1  , 56(%4)                                \n\t"   // c[i] = bb0 * aa
        "       vmovss          %%xmm2  , 56(%5)                                \n\t"   // c[i] = bb1 * aa
        "       vmovss          %%xmm1  ,   (%7)                        	\n\t"   // b[0] = bb0 * aa
        "       vmovss          %%xmm2  ,  4(%7)                        	\n\t"   // b[1] = bb1 * aa

	"       vmovups        48(%6)  , %%xmm7                                 \n\t"   // read a[k]
        "       vfnmaddps       %%xmm11 , %%xmm1 , %%xmm7 , %%xmm11             \n\t"
        "       vfnmaddps       %%xmm15 , %%xmm2 , %%xmm7 , %%xmm15             \n\t"

	"	addq		$64 , %6					\n\t"   // a -= m
	"	addq		$8  , %7					\n\t"   // b -= n

	"	vbroadcastss    60(%6) , %%xmm0					\n\t" // i=15, read aa[i]		
	"	vshufps		$0xff  , %%xmm11 , %%xmm11 , %%xmm1		\n\t" // extract bb0
	"	vshufps		$0xff  , %%xmm15 , %%xmm15 , %%xmm2		\n\t" // extract bb1
	"       vmulps          %%xmm0  , %%xmm1 , %%xmm1                       \n\t"   // bb0 * aa
	"       vmulps          %%xmm0  , %%xmm2 , %%xmm2                       \n\t"   // bb1 * aa
        "       vmovss          %%xmm1  , 60(%4)                                \n\t"   // c[i] = bb0 * aa
        "       vmovss          %%xmm2  , 60(%5)                                \n\t"   // c[i] = bb1 * aa
        "       vmovss          %%xmm1  ,   (%7)                        	\n\t"   // b[0] = bb0 * aa
        "       vmovss          %%xmm2  ,  4(%7)                        	\n\t"   // b[1] = bb1 * aa

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

  FLOAT aa, bb;

  int i, j, k;

  for (i = 0; i < m; i++) {

    aa = *(a + i);

    for (j = 0; j < n; j ++) {
      bb = *(c + i + j * ldc);
      bb *= aa;
      *b             = bb;
      *(c + i + j * ldc) = bb;
      b ++;

      for (k = i + 1; k < m; k ++){
	*(c + k + j * ldc) -= bb * *(a + k);
      }

    }
    a += m;
  }
}

#else

static inline void solve(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {

  FLOAT aa1, aa2;
  FLOAT bb1, bb2;
  FLOAT cc1, cc2;

  int i, j, k;

  ldc *= 2;

  for (i = 0; i < m; i++) {

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

      for (k = i + 1; k < m; k ++){
#ifndef CONJ
	*(c + k * 2 + 0 + j * ldc) -= cc1 * *(a + k * 2 + 0) - cc2 * *(a + k * 2 + 1);
	*(c + k * 2 + 1 + j * ldc) -= cc1 * *(a + k * 2 + 1) + cc2 * *(a + k * 2 + 0);
#else
	*(c + k * 2 + 0 + j * ldc) -= cc1 * *(a + k * 2 + 0) + cc2 * *(a + k * 2 + 1);
	*(c + k * 2 + 1 + j * ldc) -= -cc1 * *(a + k * 2 + 1) + cc2 * *(a + k * 2 + 0);
#endif
      }

    }
    a += m * 2;
  }
}

#endif


int CNAME(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT dummy1,
#ifdef COMPLEX
	   FLOAT dummy2,
#endif
	   FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG offset){

  FLOAT *aa, *cc;
  BLASLONG  kk;
  BLASLONG i, j, jj;

#if 0
  fprintf(stderr, "TRSM KERNEL LT : m = %3ld  n = %3ld  k = %3ld offset = %3ld\n",
	  m, n, k, offset);
#endif

  jj = 0;

  j = (n >> GEMM_UNROLL_N_SHIFT);

  while (j > 0) {

    kk = offset;
    aa = a;
    cc = c;

    i = (m >> GEMM_UNROLL_M_SHIFT);

    while (i > 0) {

	strsm_LT_solve_opt(kk , aa , b , cc, ldc, aa + kk * GEMM_UNROLL_M * COMPSIZE, b  + kk * GEMM_UNROLL_N * COMPSIZE);


      aa += GEMM_UNROLL_M * k * COMPSIZE;
      cc += GEMM_UNROLL_M     * COMPSIZE;
      kk += GEMM_UNROLL_M;
      i --;
    }

    if (m & (GEMM_UNROLL_M - 1)) {
      i = (GEMM_UNROLL_M >> 1);
      while (i > 0) {
	if (m & i) {
	    if (kk > 0) {
	      GEMM_KERNEL(i, GEMM_UNROLL_N, kk, dm1,
#ifdef COMPLEX
			  ZERO,
#endif
			  aa, b, cc, ldc);
	    }
	  solve(i, GEMM_UNROLL_N,
		aa + kk * i             * COMPSIZE,
		b  + kk * GEMM_UNROLL_N * COMPSIZE,
		cc, ldc);

	  aa += i * k * COMPSIZE;
	  cc += i     * COMPSIZE;
	  kk += i;
	}
	i >>= 1;
      }
    }

    b += GEMM_UNROLL_N * k   * COMPSIZE;
    c += GEMM_UNROLL_N * ldc * COMPSIZE;
    j --;
    jj += GEMM_UNROLL_M;
  }

  if (n & (GEMM_UNROLL_N - 1)) {

    j = (GEMM_UNROLL_N >> 1);
    while (j > 0) {
      if (n & j) {

	kk = offset;
	aa = a;
	cc = c;

	i = (m >> GEMM_UNROLL_M_SHIFT);

	while (i > 0) {
	  if (kk > 0) {
	    GEMM_KERNEL(GEMM_UNROLL_M, j, kk, dm1,
#ifdef COMPLEX
			ZERO,
#endif
			aa,
			b,
			cc,
			ldc);
	  }

	  solve(GEMM_UNROLL_M, j,
		aa + kk * GEMM_UNROLL_M * COMPSIZE,
		b  + kk * j             * COMPSIZE, cc, ldc);

	  aa += GEMM_UNROLL_M * k * COMPSIZE;
	  cc += GEMM_UNROLL_M     * COMPSIZE;
	  kk += GEMM_UNROLL_M;
	  i --;
	}

	if (m & (GEMM_UNROLL_M - 1)) {
	  i = (GEMM_UNROLL_M >> 1);
	  while (i > 0) {
	    if (m & i) {
	      if (kk > 0) {
		GEMM_KERNEL(i, j, kk, dm1,
#ifdef COMPLEX
			    ZERO,
#endif
			    aa,
			    b,
			    cc,
			    ldc);
	      }

	      solve(i, j,
		    aa + kk * i * COMPSIZE,
		    b  + kk * j * COMPSIZE, cc, ldc);

	      aa += i * k * COMPSIZE;
	      cc += i     * COMPSIZE;
	      kk += i;
	      }
	    i >>= 1;
	  }
	}

	b += j * k   * COMPSIZE;
	c += j * ldc * COMPSIZE;
      }
      j >>= 1;
    }
  }

  return 0;
}
