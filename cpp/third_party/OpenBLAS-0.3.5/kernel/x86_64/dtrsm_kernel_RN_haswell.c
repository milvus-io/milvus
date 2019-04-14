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
#define GEMM_KERNEL   GEMM_KERNEL_R
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



static void dtrsm_RN_solve_opt(BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, FLOAT *as, FLOAT *bs)  __attribute__ ((noinline));

static void dtrsm_RN_solve_opt(BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, FLOAT *as, FLOAT *bs)
{

	FLOAT *c3 = c + ldc + ldc*2 ;
	FLOAT *c6 = c + ldc*4 + ldc*2 ;
	ldc = ldc *8;
	BLASLONG n1 = n * 8;
	BLASLONG i=0;

        __asm__  __volatile__
        (
	"	vzeroupper							\n\t"

	"	vxorpd	%%ymm8 , %%ymm8 , %%ymm8				\n\t"
	"	vxorpd	%%ymm9 , %%ymm9 , %%ymm9				\n\t"
	"	vxorpd	%%ymm10, %%ymm10, %%ymm10				\n\t"
	"	vxorpd	%%ymm11, %%ymm11, %%ymm11				\n\t"
	"	vxorpd	%%ymm12, %%ymm12, %%ymm12				\n\t"
	"	vxorpd	%%ymm13, %%ymm13, %%ymm13				\n\t"
	"	vxorpd	%%ymm14, %%ymm14, %%ymm14				\n\t"
	"	vxorpd	%%ymm15, %%ymm15, %%ymm15				\n\t"

	"	cmpq	       $0, %0						\n\t"
	"	je	       4f						\n\t"

	"	vmovups         (%2,%1,4), %%ymm0				\n\t"	// read a
	"	vmovups         (%3,%1,8), %%ymm1				\n\t"	// read b0
	"	vmovups       32(%3,%1,8), %%ymm2				\n\t"	// read b1


	"	addq		$8, %1						\n\t"
	"	cmpq		%1, %0						\n\t"
	"	je	       21f						\n\t"

	"	.p2align 4							\n\t"
	"1:									\n\t"

	"	vmovups         (%2,%1,4), %%ymm4				\n\t"	// read a
        "       vpermpd         $0xb1  , %%ymm0 , %%ymm3                	\n\t"

	"	vfmadd231pd	%%ymm0 , %%ymm1 , %%ymm8			\n\t"
	"	vfmadd231pd	%%ymm0 , %%ymm2 , %%ymm12			\n\t"

	"	vmovups         (%3,%1,8), %%ymm5				\n\t"	// read b0
	"	vfmadd231pd	%%ymm3 , %%ymm1 , %%ymm9			\n\t"
	"	vfmadd231pd	%%ymm3 , %%ymm2 , %%ymm13			\n\t"

        "       vpermpd         $0x1b  , %%ymm3 , %%ymm0                	\n\t"
	"	vmovups       32(%3,%1,8), %%ymm6				\n\t"	// read b1
        "       vpermpd         $0xb1  , %%ymm0 , %%ymm3                	\n\t"
	"	vfmadd231pd	%%ymm0 , %%ymm1 , %%ymm10			\n\t"
	"	vfmadd231pd	%%ymm0 , %%ymm2 , %%ymm14			\n\t"

	"	addq		$8, %1						\n\t"
	"	vfmadd231pd	%%ymm3 , %%ymm1 , %%ymm11			\n\t"
	"	vfmadd231pd	%%ymm3 , %%ymm2 , %%ymm15			\n\t"

	"	cmpq		%1, %0						\n\t"

	"	jz		22f						\n\t"

	"	vmovups         (%2,%1,4), %%ymm0				\n\t"	// read a

	"	vfmadd231pd	%%ymm4 , %%ymm5 , %%ymm8			\n\t"
	"	vfmadd231pd	%%ymm4 , %%ymm6 , %%ymm12			\n\t"

        "       vpermpd         $0xb1  , %%ymm4 , %%ymm4                	\n\t"
	"	vmovups         (%3,%1,8), %%ymm1				\n\t"	// read b0
	"	vfmadd231pd	%%ymm4 , %%ymm5 , %%ymm9			\n\t"
	"	vfmadd231pd	%%ymm4 , %%ymm6 , %%ymm13			\n\t"

        "       vpermpd         $0x1b  , %%ymm4 , %%ymm4                	\n\t"
	"	vmovups       32(%3,%1,8), %%ymm2				\n\t"	// read b1
	"	vfmadd231pd	%%ymm4 , %%ymm5 , %%ymm10			\n\t"
	"	vfmadd231pd	%%ymm4 , %%ymm6 , %%ymm14			\n\t"

        "       vpermpd         $0xb1  , %%ymm4 , %%ymm4                	\n\t"
	"	addq		$8, %1						\n\t"
	"	vfmadd231pd	%%ymm4 , %%ymm5 , %%ymm11			\n\t"
	"	vfmadd231pd	%%ymm4 , %%ymm6 , %%ymm15			\n\t"

	"	cmpq		%1, %0						\n\t"

	"	jnz		1b						\n\t"


	"21:									\n\t"

	"	vfmadd231pd	%%ymm0 , %%ymm1 , %%ymm8			\n\t"
	"	vfmadd231pd	%%ymm0 , %%ymm2 , %%ymm12			\n\t"

        "       vpermpd         $0xb1  , %%ymm0 , %%ymm0                	\n\t"
	"	vfmadd231pd	%%ymm0 , %%ymm1 , %%ymm9			\n\t"
	"	vfmadd231pd	%%ymm0 , %%ymm2 , %%ymm13			\n\t"

        "       vpermpd         $0x1b  , %%ymm0 , %%ymm0                	\n\t"
	"	vfmadd231pd	%%ymm0 , %%ymm1 , %%ymm10			\n\t"
	"	vfmadd231pd	%%ymm0 , %%ymm2 , %%ymm14			\n\t"

        "       vpermpd         $0xb1  , %%ymm0 , %%ymm0                	\n\t"
	"	vfmadd231pd	%%ymm0 , %%ymm1 , %%ymm11			\n\t"
	"	vfmadd231pd	%%ymm0 , %%ymm2 , %%ymm15			\n\t"

	"	jmp	3f							\n\t"

	"22:									\n\t"
	
	"	vfmadd231pd	%%ymm4 , %%ymm5 , %%ymm8			\n\t"
	"	vfmadd231pd	%%ymm4 , %%ymm6 , %%ymm12			\n\t"

        "       vpermpd         $0xb1  , %%ymm4 , %%ymm4                	\n\t"
	"	vfmadd231pd	%%ymm4 , %%ymm5 , %%ymm9			\n\t"
	"	vfmadd231pd	%%ymm4 , %%ymm6 , %%ymm13			\n\t"

        "       vpermpd         $0x1b  , %%ymm4 , %%ymm4                	\n\t"
	"	vfmadd231pd	%%ymm4 , %%ymm5 , %%ymm10			\n\t"
	"	vfmadd231pd	%%ymm4 , %%ymm6 , %%ymm14			\n\t"

        "       vpermpd         $0xb1  , %%ymm4 , %%ymm4                	\n\t"
	"	vfmadd231pd	%%ymm4 , %%ymm5 , %%ymm11			\n\t"
	"	vfmadd231pd	%%ymm4 , %%ymm6 , %%ymm15			\n\t"

	"3:								\n\t"	

        "       vpermpd         $0xb1  , %%ymm9 , %%ymm9                \n\t"
        "       vpermpd         $0xb1  , %%ymm11, %%ymm11               \n\t"

        "       vblendpd        $0x0a  , %%ymm9 , %%ymm8 , %%ymm0       \n\t"
        "       vblendpd        $0x05  , %%ymm9 , %%ymm8 , %%ymm1       \n\t"
        "       vblendpd        $0x0a  , %%ymm11, %%ymm10, %%ymm2       \n\t"
        "       vblendpd        $0x05  , %%ymm11, %%ymm10, %%ymm3       \n\t"

        "       vpermpd         $0x1b  , %%ymm2 , %%ymm2                \n\t"
        "       vpermpd         $0x1b  , %%ymm3 , %%ymm3                \n\t"
        "       vpermpd         $0xb1  , %%ymm2 , %%ymm2                \n\t"
        "       vpermpd         $0xb1  , %%ymm3 , %%ymm3                \n\t"

        "       vblendpd        $0x03  , %%ymm0 , %%ymm2 , %%ymm8       \n\t"
        "       vblendpd        $0x03  , %%ymm1 , %%ymm3 , %%ymm9       \n\t"
        "       vblendpd        $0x03  , %%ymm2 , %%ymm0 , %%ymm10      \n\t"
        "       vblendpd        $0x03  , %%ymm3 , %%ymm1 , %%ymm11      \n\t"

        "       vpermpd         $0xb1  , %%ymm13, %%ymm13               \n\t"
        "       vpermpd         $0xb1  , %%ymm15, %%ymm15               \n\t"

        "       vblendpd        $0x0a  , %%ymm13, %%ymm12, %%ymm0       \n\t"
        "       vblendpd        $0x05  , %%ymm13, %%ymm12, %%ymm1       \n\t"
        "       vblendpd        $0x0a  , %%ymm15, %%ymm14, %%ymm2       \n\t"
        "       vblendpd        $0x05  , %%ymm15, %%ymm14, %%ymm3       \n\t"

        "       vpermpd         $0x1b  , %%ymm2 , %%ymm2                \n\t"
        "       vpermpd         $0x1b  , %%ymm3 , %%ymm3                \n\t"
        "       vpermpd         $0xb1  , %%ymm2 , %%ymm2                \n\t"
        "       vpermpd         $0xb1  , %%ymm3 , %%ymm3                \n\t"

        "       vblendpd        $0x03  , %%ymm0 , %%ymm2 , %%ymm12      \n\t"
        "       vblendpd        $0x03  , %%ymm1 , %%ymm3 , %%ymm13      \n\t"
        "       vblendpd        $0x03  , %%ymm2 , %%ymm0 , %%ymm14      \n\t"
        "       vblendpd        $0x03  , %%ymm3 , %%ymm1 , %%ymm15      \n\t"


	"4:								\n\t"	

	"	vmovups		  (%4)      , %%ymm0			\n\t"	// read c0
	"	vmovups		  (%4,%7,1) , %%ymm1			\n\t"	// read c1
	"	vmovups		  (%4,%7,2) , %%ymm2			\n\t"	// read c2
	"	vmovups		  (%5)      , %%ymm3			\n\t"	// read c3

	"	vmovups		  (%5,%7,1) , %%ymm4			\n\t"	// read c4
	"	vmovups		  (%5,%7,2) , %%ymm5			\n\t"	// read c5
	"	vmovups		  (%6)      , %%ymm6			\n\t"	// read c6
	"	vmovups		  (%6,%7,1) , %%ymm7			\n\t"	// read c7

	"	vsubpd		%%ymm8 , %%ymm0 , %%ymm8		\n\t"
	"	vmovups           (%9),  %%ymm0				\n\t"
	"	vsubpd		%%ymm9 , %%ymm1 , %%ymm9		\n\t"
	"	vpermpd		$0x55 ,  %%ymm0 , %%ymm1		\n\t"
	"	vsubpd		%%ymm10, %%ymm2 , %%ymm10		\n\t"
	"	vpermpd		$0xaa ,  %%ymm0 , %%ymm2		\n\t"
	"	vsubpd		%%ymm11, %%ymm3 , %%ymm11		\n\t"
	"	vpermpd		$0xff ,  %%ymm0 , %%ymm3		\n\t"
	"	vpermpd		$0x00 ,  %%ymm0 , %%ymm0		\n\t"

	"	vsubpd		%%ymm12, %%ymm4 , %%ymm12		\n\t"
	"	vmovups         32(%9),  %%ymm4				\n\t"
	"	vsubpd		%%ymm13, %%ymm5 , %%ymm13		\n\t"
	"	vpermpd		$0x55 ,  %%ymm4 , %%ymm5		\n\t"
	"	vsubpd		%%ymm14, %%ymm6 , %%ymm14		\n\t"
	"	vpermpd		$0xaa ,  %%ymm4 , %%ymm6		\n\t"
	"	vsubpd		%%ymm15, %%ymm7 , %%ymm15		\n\t"
	"	vpermpd		$0xff ,  %%ymm4 , %%ymm7		\n\t"
	"	vpermpd		$0x00 ,  %%ymm4 , %%ymm4		\n\t"


	"5:								\n\t"	// i = 0

	"	addq	$64, %9						\n\t"	// b=b+8

	"	vmulpd		%%ymm8 , %%ymm0, %%ymm8			\n\t"	// a *bb
	"	vmovups           (%9),  %%ymm0				\n\t"
	"	vmovups		%%ymm8 , (%8)				\n\t"	// write a
	"	vmovups		%%ymm8 , (%4)				\n\t"	// write c

	"	vfnmadd231pd	%%ymm8 , %%ymm1 , %%ymm9		\n\t"
	"	vmovups         32(%9),  %%ymm1				\n\t"
	"	vfnmadd231pd	%%ymm8 , %%ymm2 , %%ymm10		\n\t"
	"	vpermpd		$0xaa ,  %%ymm0 , %%ymm2		\n\t"
	"	vfnmadd231pd	%%ymm8 , %%ymm3 , %%ymm11		\n\t"
	"	vpermpd		$0xff ,  %%ymm0 , %%ymm3		\n\t"
	"	vfnmadd231pd	%%ymm8 , %%ymm4 , %%ymm12		\n\t"
	"	vpermpd		$0x55 ,  %%ymm0 , %%ymm0		\n\t"
	"	vfnmadd231pd	%%ymm8 , %%ymm5 , %%ymm13		\n\t"
	"	vpermpd		$0x55 ,  %%ymm1 , %%ymm5		\n\t"
	"	vfnmadd231pd	%%ymm8 , %%ymm6 , %%ymm14		\n\t"
	"	vpermpd		$0xaa ,  %%ymm1 , %%ymm6		\n\t"
	"	vfnmadd231pd	%%ymm8 , %%ymm7 , %%ymm15		\n\t"
	"	vpermpd		$0xff ,  %%ymm1 , %%ymm7		\n\t"
	"	vpermpd		$0x00 ,  %%ymm1 , %%ymm4		\n\t"

	"	addq	$64, %9						\n\t"	// b=b+8
	"	addq	$32, %8						\n\t"	// a=a+8



	"	vmulpd		%%ymm9 , %%ymm0, %%ymm9			\n\t"	// a *bb
	"	vmovups           (%9),  %%ymm0				\n\t"
	"	vmovups         32(%9),  %%ymm1				\n\t"
	"	vmovups		%%ymm9 , (%8)				\n\t"	// write a
	"	vmovups		%%ymm9 , (%4,%7,1)			\n\t"	// write c

	"	vfnmadd231pd	%%ymm9 , %%ymm2 , %%ymm10		\n\t"
	"	vfnmadd231pd	%%ymm9 , %%ymm3 , %%ymm11		\n\t"
	"	vpermpd		$0xff ,  %%ymm0 , %%ymm3		\n\t"
	"	vfnmadd231pd	%%ymm9 , %%ymm4 , %%ymm12		\n\t"
	"	vpermpd		$0xaa ,  %%ymm0 , %%ymm0		\n\t"
	"	vfnmadd231pd	%%ymm9 , %%ymm5 , %%ymm13		\n\t"
	"	vpermpd		$0x55 ,  %%ymm1 , %%ymm5		\n\t"
	"	vfnmadd231pd	%%ymm9 , %%ymm6 , %%ymm14		\n\t"
	"	vpermpd		$0xaa ,  %%ymm1 , %%ymm6		\n\t"
	"	vfnmadd231pd	%%ymm9 , %%ymm7 , %%ymm15		\n\t"
	"	vpermpd		$0xff ,  %%ymm1 , %%ymm7		\n\t"
	"	vpermpd		$0x00 ,  %%ymm1 , %%ymm4		\n\t"

	"	addq	$64, %9						\n\t"	// b=b+8
	"	addq	$32, %8						\n\t"	// a=a+8

	"	vmulpd		%%ymm10, %%ymm0, %%ymm10		\n\t"	// a *bb
	"	vmovups           (%9),  %%ymm0				\n\t"
	"	vmovups         32(%9),  %%ymm1				\n\t"
	"	vmovups		%%ymm10, (%8)				\n\t"	// write a
	"	vmovups		%%ymm10, (%4,%7,2)			\n\t"	// write c

	"	vfnmadd231pd	%%ymm10, %%ymm3 , %%ymm11		\n\t"
	"	vpermpd		$0xff ,  %%ymm0 , %%ymm0		\n\t"
	"	vfnmadd231pd	%%ymm10, %%ymm4 , %%ymm12		\n\t"
	"	vfnmadd231pd	%%ymm10, %%ymm5 , %%ymm13		\n\t"
	"	vpermpd		$0x55 ,  %%ymm1 , %%ymm5		\n\t"
	"	vfnmadd231pd	%%ymm10, %%ymm6 , %%ymm14		\n\t"
	"	vpermpd		$0xaa ,  %%ymm1 , %%ymm6		\n\t"
	"	vfnmadd231pd	%%ymm10, %%ymm7 , %%ymm15		\n\t"
	"	vpermpd		$0xff ,  %%ymm1 , %%ymm7		\n\t"
	"	vpermpd		$0x00 ,  %%ymm1 , %%ymm4		\n\t"


	"	addq	$64, %9						\n\t"	// b=b+8
	"	addq	$32, %8						\n\t"	// a=a+8



	"	vmulpd		%%ymm11, %%ymm0, %%ymm11		\n\t"	// a *bb
	"	vmovups         32(%9),  %%ymm1				\n\t"
	"	vmovups		%%ymm11, (%8)				\n\t"	// write a
	"	vmovups		%%ymm11, (%5)     			\n\t"	// write c

	"	vfnmadd231pd	%%ymm11, %%ymm4 , %%ymm12		\n\t"
	"	vfnmadd231pd	%%ymm11, %%ymm5 , %%ymm13		\n\t"
	"	vpermpd		$0x55 ,  %%ymm1 , %%ymm5		\n\t"
	"	vfnmadd231pd	%%ymm11, %%ymm6 , %%ymm14		\n\t"
	"	vpermpd		$0xaa ,  %%ymm1 , %%ymm6		\n\t"
	"	vfnmadd231pd	%%ymm11, %%ymm7 , %%ymm15		\n\t"
	"	vpermpd		$0xff ,  %%ymm1 , %%ymm7		\n\t"
	"	vpermpd		$0x00 ,  %%ymm1 , %%ymm0		\n\t"


	"	addq	$64, %9						\n\t"	// b=b+8
	"	addq	$32, %8						\n\t"	// a=a+8


	"	vmulpd		%%ymm12, %%ymm0, %%ymm12		\n\t"	// a *bb
	"	vmovups         32(%9),  %%ymm1				\n\t"
	"	vmovups		%%ymm12, (%8)				\n\t"	// write a
	"	vmovups		%%ymm12, (%5,%7,1)			\n\t"	// write c

	"	vfnmadd231pd	%%ymm12, %%ymm5 , %%ymm13		\n\t"
	"	vfnmadd231pd	%%ymm12, %%ymm6 , %%ymm14		\n\t"
	"	vpermpd		$0xaa ,  %%ymm1 , %%ymm6		\n\t"
	"	vfnmadd231pd	%%ymm12, %%ymm7 , %%ymm15		\n\t"
	"	vpermpd		$0xff ,  %%ymm1 , %%ymm7		\n\t"
	"	vpermpd		$0x55 ,  %%ymm1 , %%ymm0		\n\t"

	"	addq	$64, %9						\n\t"	// b=b+8
	"	addq	$32, %8						\n\t"	// a=a+8

	"	vmulpd		%%ymm13, %%ymm0, %%ymm13		\n\t"	// a *bb
	"	vmovups         32(%9),  %%ymm1				\n\t"
	"	vmovups		%%ymm13, (%8)				\n\t"	// write a
	"	vmovups		%%ymm13, (%5,%7,2)			\n\t"	// write c

	"	vfnmadd231pd	%%ymm13, %%ymm6 , %%ymm14		\n\t"
	"	vfnmadd231pd	%%ymm13, %%ymm7 , %%ymm15		\n\t"
	"	vpermpd		$0xff ,  %%ymm1 , %%ymm7		\n\t"
	"	vpermpd		$0xaa ,  %%ymm1 , %%ymm0		\n\t"


	"	addq	$64, %9						\n\t"	// b=b+8
	"	addq	$32, %8						\n\t"	// a=a+8


	"	vmulpd		%%ymm14, %%ymm0, %%ymm14		\n\t"	// a *bb
	"	vmovups         32(%9),  %%ymm1				\n\t"
	"	vmovups		%%ymm14, (%8)				\n\t"	// write a
	"	vmovups		%%ymm14, (%6)     			\n\t"	// write c

	"	vfnmadd231pd	%%ymm14, %%ymm7 , %%ymm15		\n\t"

	"	vpermpd		$0xff ,  %%ymm1 , %%ymm0		\n\t"

	"	addq	$32, %8						\n\t"	// a=a+8

	"	vmulpd		%%ymm15, %%ymm0, %%ymm15		\n\t"	// a *bb
	"	vmovups		%%ymm15, (%8)				\n\t"	// write a
	"	vmovups		%%ymm15, (%6,%7,1)			\n\t"	// write c

	"	vzeroupper						\n\t"

        :
        :
          "r" (n1),     // 0    
          "a" (i),      // 1    
          "r" (a),      // 2
          "r" (b),      // 3
          "r" (c),      // 4
          "r" (c3),     // 5
          "r" (c6),     // 6
          "r" (ldc),    // 7
          "r" (as),     // 8
          "r" (bs)      // 9
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

  for (i = 0; i < n; i++) {

    bb = *(b + i);

    for (j = 0; j < m; j ++) {
      aa = *(c + j + i * ldc);
      aa *= bb;
      *a  = aa;
      *(c + j + i * ldc) = aa;
      a ++;

      for (k = i + 1; k < n; k ++){
	*(c + j + k * ldc) -= aa * *(b + k);
      }

    }
    b += n;
  }
}

#else

static inline void solve(BLASLONG m, BLASLONG n, FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc) {

  FLOAT aa1, aa2;
  FLOAT bb1, bb2;
  FLOAT cc1, cc2;

  int i, j, k;

  ldc *= 2;

  for (i = 0; i < n; i++) {

    bb1 = *(b + i * 2 + 0);
    bb2 = *(b + i * 2 + 1);

    for (j = 0; j < m; j ++) {
      aa1 = *(c + j * 2 + 0 + i * ldc);
      aa2 = *(c + j * 2 + 1 + i * ldc);

#ifndef CONJ
      cc1 = aa1 * bb1 - aa2 * bb2;
      cc2 = aa1 * bb2 + aa2 * bb1;
#else
      cc1 =  aa1 * bb1 + aa2 * bb2;
      cc2 = -aa1 * bb2 + aa2 * bb1;
#endif

      *(a + 0) = cc1;
      *(a + 1) = cc2;
      *(c + j * 2 + 0 + i * ldc) = cc1;
      *(c + j * 2 + 1 + i * ldc) = cc2;
      a += 2;

      for (k = i + 1; k < n; k ++){
#ifndef CONJ
	*(c + j * 2 + 0 + k * ldc) -= cc1 * *(b + k * 2 + 0) - cc2 * *(b + k * 2 + 1);
	*(c + j * 2 + 1 + k * ldc) -= cc1 * *(b + k * 2 + 1) + cc2 * *(b + k * 2 + 0);
#else
	*(c + j * 2 + 0 + k * ldc) -=   cc1 * *(b + k * 2 + 0) + cc2 * *(b + k * 2 + 1);
	*(c + j * 2 + 1 + k * ldc) -= - cc1 * *(b + k * 2 + 1) + cc2 * *(b + k * 2 + 0);
#endif
      }

    }
    b += n * 2;
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
  fprintf(stderr, "TRSM RN KERNEL m = %3ld  n = %3ld  k = %3ld offset = %3ld\n",
	  m, n, k, offset);
#endif

  jj = 0;
  j = (n >> GEMM_UNROLL_N_SHIFT);
  kk = -offset;

  while (j > 0) {

    aa = a;
    cc = c;

    i = (m >> GEMM_UNROLL_M_SHIFT);

    if (i > 0) {
      do {

	dtrsm_RN_solve_opt(kk, aa, b, cc, ldc, aa + kk * GEMM_UNROLL_M * COMPSIZE, b + kk * GEMM_UNROLL_N * COMPSIZE);
/* 
        solve(GEMM_UNROLL_M, GEMM_UNROLL_N,
              aa + kk * GEMM_UNROLL_M * COMPSIZE,
              b  + kk * GEMM_UNROLL_N * COMPSIZE,
              cc, ldc);
*/
	aa += GEMM_UNROLL_M * k * COMPSIZE;
	cc += GEMM_UNROLL_M     * COMPSIZE;
	i --;
      } while (i > 0);
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
	}
	i >>= 1;
      }
    }

    kk += GEMM_UNROLL_N;
    b += GEMM_UNROLL_N * k   * COMPSIZE;
    c += GEMM_UNROLL_N * ldc * COMPSIZE;
    j --;
    jj += GEMM_UNROLL_M;
  }

  if (n & (GEMM_UNROLL_N - 1)) {

    j = (GEMM_UNROLL_N >> 1);
    while (j > 0) {
      if (n & j) {

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
	      }
	    i >>= 1;
	  }
	}

	b += j * k   * COMPSIZE;
	c += j * ldc * COMPSIZE;
	kk += j;
      }
      j >>= 1;
    }
  }

  return 0;
}
