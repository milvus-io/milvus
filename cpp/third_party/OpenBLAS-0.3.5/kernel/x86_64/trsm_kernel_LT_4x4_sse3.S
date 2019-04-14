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

#define ASSEMBLER
#include "common.h"

#define M	%rdi
#define N	%rsi
#define K	%rdx

#define A	%rcx
#define B	%r8
#define C	%r9
#define LDC	%r10

#define I	%r11
#define AO	%r13
#define BO	%r14
#define	CO1	%r15
#define CO2	%rbx
#define KK	%rbp
#define BB	%r12

#ifndef WINDOWS_ABI

#define STACKSIZE 128

#define OLD_LDC		 8 + STACKSIZE(%rsp)
#define OLD_OFFSET	16 + STACKSIZE(%rsp)

#define OFFSET	 48(%rsp)
#define J	 56(%rsp)
#define KKK	 64(%rsp)
#define AORIG	 72(%rsp)

#else

#define STACKSIZE 256

#define OLD_A		40 + STACKSIZE(%rsp)
#define OLD_B		48 + STACKSIZE(%rsp)
#define OLD_C		56 + STACKSIZE(%rsp)
#define OLD_LDC		64 + STACKSIZE(%rsp)
#define OLD_OFFSET	72 + STACKSIZE(%rsp)

#define OFFSET	 224(%rsp)
#define J	 232(%rsp)
#define KKK	 240(%rsp)
#define AORIG	 248(%rsp)

#endif

#define PREFETCH     prefetcht1
#define PREFETCHSIZE (16 * 12 + 3)
#define PREFETCH_R    (4 *  4 + 0)

#define KERNEL1(address) \
	mulpd	%xmm8, %xmm9 ;\
	PREFETCH  (PREFETCHSIZE +  0) * SIZE + (address) * 2 * SIZE(AO);\
	addpd	%xmm9, %xmm0;\
	movddup	 1 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	addpd	%xmm9, %xmm1;\
	movddup	 2 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	addpd	%xmm9, %xmm2;\
	movddup	 3 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	movapd	 2 * SIZE + (address) * 2 * SIZE(AO), %xmm8;\
	addpd	%xmm9, %xmm3;\
	movddup	 0 * SIZE + (address) * 2 * SIZE(BO), %xmm9

#define KERNEL2(address) \
	mulpd	%xmm8, %xmm9;\
	addpd	%xmm9, %xmm4;\
	movddup	 1 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	addpd	%xmm9, %xmm5;\
	movddup	 2 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	addpd	%xmm9, %xmm6;\
	movddup	 3 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	movapd	 4 * SIZE + (address) * 2 * SIZE(AO), %xmm8;\
	addpd	%xmm9, %xmm7;\
	movddup	 4 * SIZE + (address) * 2 * SIZE(BO), %xmm9

#define KERNEL3(address) \
	mulpd	%xmm8, %xmm9;\
	addpd	%xmm9, %xmm0;\
	movddup	 5 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	addpd	%xmm9, %xmm1;\
	movddup	 6 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	addpd	%xmm9, %xmm2;\
	movddup	 7 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	movapd	 6 * SIZE + (address) * 2 * SIZE(AO), %xmm8;\
	addpd	%xmm9, %xmm3;\
	movddup	 4 * SIZE + (address) * 2 * SIZE(BO), %xmm9

#define KERNEL4(address) \
	mulpd	%xmm8, %xmm9;\
	addpd	%xmm9, %xmm4;\
	movddup	 5 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	addpd	%xmm9, %xmm5;\
	movddup	 6 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	addpd	%xmm9, %xmm6;\
	movddup	 7 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	movapd	32 * SIZE + (address) * 2 * SIZE(AO), %xmm8;\
	addpd	%xmm9, %xmm7;\
	movddup	32 * SIZE + (address) * 2 * SIZE(BO), %xmm9

#define KERNEL5(address) \
	mulpd	%xmm10, %xmm11;\
	addpd	%xmm11, %xmm0;\
	movddup	 9 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	addpd	%xmm11, %xmm1;\
	movddup	10 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	addpd	%xmm11, %xmm2;\
	movddup	11 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	movapd	10 * SIZE + (address) * 2 * SIZE(AO), %xmm10;\
	addpd	%xmm11, %xmm3;\
	movddup	 8 * SIZE + (address) * 2 * SIZE(BO), %xmm11

#define KERNEL6(address) \
	mulpd	%xmm10, %xmm11;\
	addpd	%xmm11, %xmm4;\
	movddup	 9 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	addpd	%xmm11, %xmm5;\
	movddup	10 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	addpd	%xmm11, %xmm6;\
	movddup	11 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	movapd	12 * SIZE + (address) * 2 * SIZE(AO), %xmm10;\
	addpd	%xmm11, %xmm7;\
	movddup	12 * SIZE + (address) * 2 * SIZE(BO), %xmm11

#define KERNEL7(address) \
	mulpd	%xmm10, %xmm11;\
	addpd	%xmm11, %xmm0;\
	movddup	13 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	addpd	%xmm11, %xmm1;\
	movddup	14 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	addpd	%xmm11, %xmm2;\
	movddup	15 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	movapd	14 * SIZE + (address) * 2 * SIZE(AO), %xmm10;\
	addpd	%xmm11, %xmm3;\
	movddup	12 * SIZE + (address) * 2 * SIZE(BO), %xmm11

#define KERNEL8(address) \
	mulpd	%xmm10, %xmm11;\
	addpd	%xmm11, %xmm4;\
	movddup	13 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	addpd	%xmm11, %xmm5;\
	movddup	14 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	addpd	%xmm11, %xmm6;\
	movddup	15 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	movapd	40 * SIZE + (address) * 2 * SIZE(AO), %xmm10;\
	addpd	%xmm11, %xmm7;\
	movddup	40 * SIZE + (address) * 2 * SIZE(BO), %xmm11

#define KERNEL9(address) \
	mulpd	%xmm12, %xmm13;\
	PREFETCH  (PREFETCHSIZE + 16) * SIZE + (address) * 2 * SIZE(AO);\
	addpd	%xmm13, %xmm0;\
	movddup	17 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	addpd	%xmm13, %xmm1;\
	movddup	18 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	addpd	%xmm13, %xmm2;\
	movddup	19 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	movapd	18 * SIZE + (address) * 2 * SIZE(AO), %xmm12;\
	addpd	%xmm13, %xmm3;\
	movddup	16 * SIZE + (address) * 2 * SIZE(BO), %xmm13

#define KERNEL10(address) \
	mulpd	%xmm12, %xmm13;\
	addpd	%xmm13, %xmm4;\
	movddup	17 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	addpd	%xmm13, %xmm5;\
	movddup	18 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	addpd	%xmm13, %xmm6;\
	movddup	19 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	movapd	20 * SIZE + (address) * 2 * SIZE(AO), %xmm12;\
	addpd	%xmm13, %xmm7;\
	movddup	20 * SIZE + (address) * 2 * SIZE(BO), %xmm13

#define KERNEL11(address) \
	mulpd	%xmm12, %xmm13;\
	addpd	%xmm13, %xmm0;\
	movddup	21 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	addpd	%xmm13, %xmm1;\
	movddup	22 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	addpd	%xmm13, %xmm2;\
	movddup	23 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	movapd	22 * SIZE + (address) * 2 * SIZE(AO), %xmm12;\
	addpd	%xmm13, %xmm3;\
	movddup	20 * SIZE + (address) * 2 * SIZE(BO), %xmm13

#define KERNEL12(address) \
	mulpd	%xmm12, %xmm13;\
	addpd	%xmm13, %xmm4;\
	movddup 21 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	addpd	%xmm13, %xmm5;\
	movddup	22 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	addpd	%xmm13, %xmm6;\
	movddup	23 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	movapd	48 * SIZE + (address) * 2 * SIZE(AO), %xmm12;\
	addpd	%xmm13, %xmm7;\
	movddup	48 * SIZE + (address) * 2 * SIZE(BO), %xmm13

#define KERNEL13(address) \
	mulpd	%xmm14, %xmm15;\
	addpd	%xmm15, %xmm0;\
	movddup	25 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	addpd	%xmm15, %xmm1;\
	movddup	26 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	addpd	%xmm15, %xmm2;\
	movddup	27 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	movapd	26 * SIZE + (address) * 2 * SIZE(AO), %xmm14;\
	addpd	%xmm15, %xmm3;\
	movddup	24 * SIZE + (address) * 2 * SIZE(BO), %xmm15

#define KERNEL14(address) \
	mulpd	%xmm14, %xmm15;\
	addpd	%xmm15, %xmm4;\
	movddup	25 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	addpd	%xmm15, %xmm5;\
	movddup	26 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	addpd	%xmm15, %xmm6;\
	movddup	27 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	movapd	28 * SIZE + (address) * 2 * SIZE(AO), %xmm14;\
	addpd	%xmm15, %xmm7;\
	movddup	28 * SIZE + (address) * 2 * SIZE(BO), %xmm15

#define KERNEL15(address) \
	mulpd	%xmm14, %xmm15;\
	addpd	%xmm15, %xmm0;\
	movddup	29 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	addpd	%xmm15, %xmm1;\
	movddup	30 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	addpd	%xmm15, %xmm2;\
	movddup	31 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	movapd	30 * SIZE + (address) * 2 * SIZE(AO), %xmm14;\
	addpd	%xmm15, %xmm3;\
	movddup	28 * SIZE + (address) * 2 * SIZE(BO), %xmm15

#define KERNEL16(address) \
	mulpd	%xmm14, %xmm15;\
	addpd	%xmm15, %xmm4;\
	movddup	29 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	addpd	%xmm15, %xmm5;\
	movddup	30 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	addpd	%xmm15, %xmm6;\
	movddup	31 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	movapd	56 * SIZE + (address) * 2 * SIZE(AO), %xmm14;\
	addpd	%xmm15, %xmm7;\
	movddup	56 * SIZE + (address) * 2 * SIZE(BO), %xmm15

	PROLOGUE
	PROFCODE

	subq	$STACKSIZE, %rsp
	movq	%rbx,  0(%rsp)
	movq	%rbp,  8(%rsp)
	movq	%r12, 16(%rsp)
	movq	%r13, 24(%rsp)
	movq	%r14, 32(%rsp)
	movq	%r15, 40(%rsp)

#ifdef WINDOWS_ABI
	movq	%rdi,    48(%rsp)
	movq	%rsi,    56(%rsp)
	movups	%xmm6,   64(%rsp)
	movups	%xmm7,   80(%rsp)
	movups	%xmm8,   96(%rsp)
	movups	%xmm9,  112(%rsp)
	movups	%xmm10, 128(%rsp)
	movups	%xmm11, 144(%rsp)
	movups	%xmm12, 160(%rsp)
	movups	%xmm13, 176(%rsp)
	movups	%xmm14, 192(%rsp)
	movups	%xmm15, 208(%rsp)

	movq	ARG1,      M
	movq	ARG2,      N
	movq	ARG3,      K
	movq	OLD_A,     A
	movq	OLD_B,     B
	movq	OLD_C,     C
#endif

	movq	OLD_LDC,   LDC
	movq	OLD_OFFSET, KK

	movq	KK, OFFSET

	leaq	(, LDC, SIZE), LDC

#ifdef LN
       leaq	(, M, SIZE), %rax
       addq	%rax, C
       imulq	K, %rax
       addq	%rax, A
#endif

#ifdef RT
       leaq	(, N, SIZE), %rax
       imulq	K, %rax
       addq	%rax, B
       movq	N, %rax
       imulq	LDC, %rax
       addq	%rax, C
#endif

#ifdef RN
	negq	KK
#endif

#ifdef RT
       movq	N, %rax
       subq	OFFSET, %rax
       movq	%rax, KK
#endif

	movq	N,  J
	sarq	$2, J		# j = (n >> 2)
	jle	.L40
	ALIGN_4

.L10:
#if defined(LT) || defined(RN)
	movq	A, AO
#else
	movq	A, AORIG
#endif

#ifdef RT
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       subq	%rax, B

       leaq	(, LDC, 4), %rax
       subq	%rax, C
#endif

	movq	C, CO1
	leaq	(C, LDC, 1), CO2
#ifndef RT
	leaq	(C, LDC, 4), C
#endif

#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

	movq	K, %rax
	salq	$BASE_SHIFT + 2, %rax
	leaq	(B, %rax), BB

#ifdef LT
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L20
	ALIGN_4

.L11:
#ifdef LN
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 4), BO
#else
	movq	B, BO
#endif

	prefetcht0	  0 * SIZE(BB)
	subq	   $-8 * SIZE, BB

	movapd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movddup	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movapd	 8 * SIZE(AO), %xmm10
	pxor	%xmm2, %xmm2
	movddup	 8 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3

	movapd	16 * SIZE(AO), %xmm12
	movddup 16 * SIZE(BO), %xmm13
	movapd	24 * SIZE(AO), %xmm14
	movddup	24 * SIZE(BO), %xmm15

	prefetchnta     4 * SIZE(CO1)
	pxor	%xmm4, %xmm4
	prefetchnta     4 * SIZE(CO2)
	pxor	%xmm5, %xmm5
	prefetchnta     4 * SIZE(CO1, LDC, 2)
	pxor	%xmm6, %xmm6
	prefetchnta     4 * SIZE(CO2, LDC, 2)
	pxor	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif

#if 1
	andq	$-8, %rax
	salq	$4, %rax
	je	.L15
.L1X:
	KERNEL1 (16  *  0)
	KERNEL2 (16  *  0)
	KERNEL3 (16  *  0)
	KERNEL4 (16  *  0)
	KERNEL5 (16  *  0)
	KERNEL6 (16  *  0)
	KERNEL7 (16  *  0)
	KERNEL8 (16  *  0)
	KERNEL9 (16  *  0)
	KERNEL10(16  *  0)
	KERNEL11(16  *  0)
	KERNEL12(16  *  0)
	KERNEL13(16  *  0)
	KERNEL14(16  *  0)
	KERNEL15(16  *  0)
	KERNEL16(16  *  0)
	cmpq	$128 *  1, %rax
	NOBRANCH
	jle	.L12
	KERNEL1 (16  *  1)
	KERNEL2 (16  *  1)
	KERNEL3 (16  *  1)
	KERNEL4 (16  *  1)
	KERNEL5 (16  *  1)
	KERNEL6 (16  *  1)
	KERNEL7 (16  *  1)
	KERNEL8 (16  *  1)
	KERNEL9 (16  *  1)
	KERNEL10(16  *  1)
	KERNEL11(16  *  1)
	KERNEL12(16  *  1)
	KERNEL13(16  *  1)
	KERNEL14(16  *  1)
	KERNEL15(16  *  1)
	KERNEL16(16  *  1)
	cmpq	$128 *  2, %rax
	NOBRANCH
	jle	.L12
	KERNEL1 (16  *  2)
	KERNEL2 (16  *  2)
	KERNEL3 (16  *  2)
	KERNEL4 (16  *  2)
	KERNEL5 (16  *  2)
	KERNEL6 (16  *  2)
	KERNEL7 (16  *  2)
	KERNEL8 (16  *  2)
	KERNEL9 (16  *  2)
	KERNEL10(16  *  2)
	KERNEL11(16  *  2)
	KERNEL12(16  *  2)
	KERNEL13(16  *  2)
	KERNEL14(16  *  2)
	KERNEL15(16  *  2)
	KERNEL16(16  *  2)
	cmpq	$128 *  3, %rax
	NOBRANCH
	jle	.L12
	KERNEL1 (16  *  3)
	KERNEL2 (16  *  3)
	KERNEL3 (16  *  3)
	KERNEL4 (16  *  3)
	KERNEL5 (16  *  3)
	KERNEL6 (16  *  3)
	KERNEL7 (16  *  3)
	KERNEL8 (16  *  3)
	KERNEL9 (16  *  3)
	KERNEL10(16  *  3)
	KERNEL11(16  *  3)
	KERNEL12(16  *  3)
	KERNEL13(16  *  3)
	KERNEL14(16  *  3)
	KERNEL15(16  *  3)
	KERNEL16(16  *  3)
	cmpq	$128 *  4, %rax
	NOBRANCH
	jle	.L12
	KERNEL1 (16  *  4)
	KERNEL2 (16  *  4)
	KERNEL3 (16  *  4)
	KERNEL4 (16  *  4)
	KERNEL5 (16  *  4)
	KERNEL6 (16  *  4)
	KERNEL7 (16  *  4)
	KERNEL8 (16  *  4)
	KERNEL9 (16  *  4)
	KERNEL10(16  *  4)
	KERNEL11(16  *  4)
	KERNEL12(16  *  4)
	KERNEL13(16  *  4)
	KERNEL14(16  *  4)
	KERNEL15(16  *  4)
	KERNEL16(16  *  4)
	cmpq	$128 *  5, %rax
	NOBRANCH
	jle	.L12
	KERNEL1 (16  *  5)
	KERNEL2 (16  *  5)
	KERNEL3 (16  *  5)
	KERNEL4 (16  *  5)
	KERNEL5 (16  *  5)
	KERNEL6 (16  *  5)
	KERNEL7 (16  *  5)
	KERNEL8 (16  *  5)
	KERNEL9 (16  *  5)
	KERNEL10(16  *  5)
	KERNEL11(16  *  5)
	KERNEL12(16  *  5)
	KERNEL13(16  *  5)
	KERNEL14(16  *  5)
	KERNEL15(16  *  5)
	KERNEL16(16  *  5)
	cmpq	$128 *  6, %rax
	NOBRANCH
	jle	.L12
	KERNEL1 (16  *  6)
	KERNEL2 (16  *  6)
	KERNEL3 (16  *  6)
	KERNEL4 (16  *  6)
	KERNEL5 (16  *  6)
	KERNEL6 (16  *  6)
	KERNEL7 (16  *  6)
	KERNEL8 (16  *  6)
	KERNEL9 (16  *  6)
	KERNEL10(16  *  6)
	KERNEL11(16  *  6)
	KERNEL12(16  *  6)
	KERNEL13(16  *  6)
	KERNEL14(16  *  6)
	KERNEL15(16  *  6)
	KERNEL16(16  *  6)
	cmpq	$128 *  7, %rax
	NOBRANCH
	jle	.L12
	KERNEL1 (16  *  7)
	KERNEL2 (16  *  7)
	KERNEL3 (16  *  7)
	KERNEL4 (16  *  7)
	KERNEL5 (16  *  7)
	KERNEL6 (16  *  7)
	KERNEL7 (16  *  7)
	KERNEL8 (16  *  7)
	KERNEL9 (16  *  7)
	KERNEL10(16  *  7)
	KERNEL11(16  *  7)
	KERNEL12(16  *  7)
	KERNEL13(16  *  7)
	KERNEL14(16  *  7)
	KERNEL15(16  *  7)
	KERNEL16(16  *  7)

	addq	$32 * 8  * SIZE, AO
	addq	$32 * 8  * SIZE, BO
	subq	$128 * 8, %rax
	jg	.L1X

.L12:
	leaq	(AO, %rax, 2), AO	# * 16
	leaq	(BO, %rax, 2), BO	# * 64
#else
	sarq	$3, %rax
	je	.L15
	ALIGN_4

.L12:
	mulpd	%xmm8, %xmm9
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
	addpd	%xmm9, %xmm0
	movddup	 1 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm1
	movddup	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm2
	movddup	 3 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 2 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm3
	movddup	 0 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm4
	movddup	 1 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm5
	movddup	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm6
	movddup	 3 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 4 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm7
	movddup	 4 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0
	movddup	 5 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm1
	movddup	 6 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm2
	movddup	 7 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 6 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm3
	movddup	 4 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm4
	movddup	 5 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm5
	movddup	 6 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm6
	movddup	 7 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	32 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm7

	movddup	32 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm0
	movddup	 9 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm1
	movddup	10 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm2
	movddup	11 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	movapd	10 * SIZE(AO), %xmm10
	addpd	%xmm11, %xmm3

	movddup	 8 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm4
	movddup	 9 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm5
	movddup	10 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm6
	movddup	11 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	movapd	12 * SIZE(AO), %xmm10
	addpd	%xmm11, %xmm7
	movddup	12 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm0
	movddup	13 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm1
	movddup	14 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm2
	movddup	15 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	movapd	14 * SIZE(AO), %xmm10
	addpd	%xmm11, %xmm3

	movddup	12 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm4
	movddup	13 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm5
	movddup	14 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm6
	movddup	15 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	movapd	40 * SIZE(AO), %xmm10
	addpd	%xmm11, %xmm7
	movddup	40 * SIZE(BO), %xmm11

	mulpd	%xmm12, %xmm13
	PREFETCH  (PREFETCHSIZE + 16) * SIZE(AO)
	addpd	%xmm13, %xmm0
	movddup	17 * SIZE(BO), %xmm13
	mulpd	%xmm12, %xmm13
	addpd	%xmm13, %xmm1
	movddup	18 * SIZE(BO), %xmm13
	mulpd	%xmm12, %xmm13
	addpd	%xmm13, %xmm2
	movddup	19 * SIZE(BO), %xmm13
	mulpd	%xmm12, %xmm13
	movapd	18 * SIZE(AO), %xmm12
	addpd	%xmm13, %xmm3

	movddup	16 * SIZE(BO), %xmm13
	mulpd	%xmm12, %xmm13
	addpd	%xmm13, %xmm4
	movddup	17 * SIZE(BO), %xmm13
	mulpd	%xmm12, %xmm13
	addpd	%xmm13, %xmm5
	movddup	18 * SIZE(BO), %xmm13
	mulpd	%xmm12, %xmm13
	addpd	%xmm13, %xmm6
	movddup	19 * SIZE(BO), %xmm13
	mulpd	%xmm12, %xmm13
	movapd	20 * SIZE(AO), %xmm12
	addpd	%xmm13, %xmm7

	movddup	20 * SIZE(BO), %xmm13
	mulpd	%xmm12, %xmm13
	addpd	%xmm13, %xmm0
	movddup	21 * SIZE(BO), %xmm13
	mulpd	%xmm12, %xmm13
	addpd	%xmm13, %xmm1
	movddup	22 * SIZE(BO), %xmm13
	mulpd	%xmm12, %xmm13
	addpd	%xmm13, %xmm2
	movddup	23 * SIZE(BO), %xmm13
	mulpd	%xmm12, %xmm13
	movapd	22 * SIZE(AO), %xmm12
	addpd	%xmm13, %xmm3

	movddup	20 * SIZE(BO), %xmm13
	mulpd	%xmm12, %xmm13
	addpd	%xmm13, %xmm4
	movddup 21 * SIZE(BO), %xmm13
	mulpd	%xmm12, %xmm13
	addpd	%xmm13, %xmm5
	movddup	22 * SIZE(BO), %xmm13
	mulpd	%xmm12, %xmm13
	addpd	%xmm13, %xmm6
	movddup	23 * SIZE(BO), %xmm13
	mulpd	%xmm12, %xmm13
	movapd	48 * SIZE(AO), %xmm12
	addpd	%xmm13, %xmm7
	movddup	48 * SIZE(BO), %xmm13

	mulpd	%xmm14, %xmm15
	addpd	%xmm15, %xmm0
	movddup	25 * SIZE(BO), %xmm15
	mulpd	%xmm14, %xmm15
	addpd	%xmm15, %xmm1
	movddup	26 * SIZE(BO), %xmm15
	mulpd	%xmm14, %xmm15
	addpd	%xmm15, %xmm2
	movddup	27 * SIZE(BO), %xmm15
	mulpd	%xmm14, %xmm15
	movapd	26 * SIZE(AO), %xmm14
	addpd	%xmm15, %xmm3

	movddup	24 * SIZE(BO), %xmm15
	mulpd	%xmm14, %xmm15
	addpd	%xmm15, %xmm4
	movddup	25 * SIZE(BO), %xmm15
	mulpd	%xmm14, %xmm15
	addpd	%xmm15, %xmm5
	movddup	26 * SIZE(BO), %xmm15
	mulpd	%xmm14, %xmm15
	addpd	%xmm15, %xmm6
	movddup	27 * SIZE(BO), %xmm15
	mulpd	%xmm14, %xmm15
	movapd	28 * SIZE(AO), %xmm14
	addpd	%xmm15, %xmm7

	movddup	28 * SIZE(BO), %xmm15
	mulpd	%xmm14, %xmm15
	addpd	%xmm15, %xmm0
	movddup	29 * SIZE(BO), %xmm15
	mulpd	%xmm14, %xmm15
	addpd	%xmm15, %xmm1
	movddup	30 * SIZE(BO), %xmm15
	mulpd	%xmm14, %xmm15
	addpd	%xmm15, %xmm2
	movddup	31 * SIZE(BO), %xmm15
	mulpd	%xmm14, %xmm15
	movapd	30 * SIZE(AO), %xmm14
	addpd	%xmm15, %xmm3

	movddup	28 * SIZE(BO), %xmm15
	mulpd	%xmm14, %xmm15
	addpd	%xmm15, %xmm4
	movddup	29 * SIZE(BO), %xmm15
	mulpd	%xmm14, %xmm15
	addpd	%xmm15, %xmm5
	movddup	30 * SIZE(BO), %xmm15
	mulpd	%xmm14, %xmm15
	addpd	%xmm15, %xmm6
	movddup	31 * SIZE(BO), %xmm15
	mulpd	%xmm14, %xmm15
	movapd	56 * SIZE(AO), %xmm14
	addpd	%xmm15, %xmm7
	movddup	56 * SIZE(BO), %xmm15

	addq   $32 * SIZE, BO
	addq   $32 * SIZE, AO
	decq   %rax
	BRANCH
	jne    .L12
#endif
	ALIGN_4

.L15:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L19
	ALIGN_4

.L16:
	mulpd	%xmm8, %xmm9
	movapd	 2 * SIZE(AO), %xmm10
	addpd	%xmm9, %xmm0
	movddup	 1 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movddup	 0 * SIZE(BO), %xmm11
	addpd	%xmm9, %xmm1
	movddup	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm2
	movddup	 3 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 4 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm3
	movddup	 4 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm4
	movddup	 1 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm5
	movddup	 2 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm6
	movddup	 3 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm7

	addq	$4 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L16
	ALIGN_4

.L19:
#if defined(LN) || defined(RT)
	movq	KK, %rax
	subq	$4, %rax

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm0, %xmm8
	unpcklpd %xmm1, %xmm0
	unpckhpd %xmm1, %xmm8

	movapd	%xmm2, %xmm10
	unpcklpd %xmm3, %xmm2
	unpckhpd %xmm3, %xmm10

	movapd	%xmm4, %xmm12
	unpcklpd %xmm5, %xmm4
	unpckhpd %xmm5, %xmm12

	movapd	%xmm6, %xmm14
	unpcklpd %xmm7, %xmm6
	unpckhpd %xmm7, %xmm14

	movapd	 0 * SIZE(BO), %xmm1
	movapd	 2 * SIZE(BO), %xmm3
	movapd	 4 * SIZE(BO), %xmm5
	movapd	 6 * SIZE(BO), %xmm7
	movapd	 8 * SIZE(BO), %xmm9
	movapd	10 * SIZE(BO), %xmm11
	movapd	12 * SIZE(BO), %xmm13
	movapd	14 * SIZE(BO), %xmm15

	subpd	%xmm0,  %xmm1
	subpd	%xmm2,  %xmm3
	subpd	%xmm8,  %xmm5
	subpd	%xmm10, %xmm7
	subpd	%xmm4,  %xmm9
	subpd	%xmm6,  %xmm11
	subpd	%xmm12, %xmm13
	subpd	%xmm14, %xmm15
#else

	movapd	 0 * SIZE(AO), %xmm8
	movapd	 2 * SIZE(AO), %xmm9
	movapd	 4 * SIZE(AO), %xmm10
	movapd	 6 * SIZE(AO), %xmm11

	movapd	 8 * SIZE(AO), %xmm12
	movapd	10 * SIZE(AO), %xmm13
	movapd	12 * SIZE(AO), %xmm14
	movapd	14 * SIZE(AO), %xmm15

	subpd	%xmm0, %xmm8
	subpd	%xmm4, %xmm9
	subpd	%xmm1, %xmm10
	subpd	%xmm5, %xmm11
	subpd	%xmm2, %xmm12
	subpd	%xmm6, %xmm13
	subpd	%xmm3, %xmm14
	subpd	%xmm7, %xmm15
#endif


#ifdef LN
	movddup	15 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm13
	mulpd	 %xmm0, %xmm15

	movddup	14 * SIZE(AO), %xmm2
	mulpd	 %xmm13, %xmm2
	subpd	 %xmm2, %xmm9
	movddup	14 * SIZE(AO), %xmm2
	mulpd	 %xmm15, %xmm2
	subpd	 %xmm2, %xmm11

	movddup	13 * SIZE(AO), %xmm4
	mulpd	 %xmm13, %xmm4
	subpd	 %xmm4, %xmm5
	movddup	13 * SIZE(AO), %xmm4
	mulpd	 %xmm15, %xmm4
	subpd	 %xmm4, %xmm7

	movddup	12 * SIZE(AO), %xmm6
	mulpd	 %xmm13, %xmm6
	subpd	 %xmm6, %xmm1
	movddup	12 * SIZE(AO), %xmm6
	mulpd	 %xmm15, %xmm6
	subpd	 %xmm6, %xmm3

	movddup	10 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm0, %xmm11

	movddup	 9 * SIZE(AO), %xmm2
	mulpd	 %xmm9, %xmm2
	subpd	 %xmm2, %xmm5
	movddup	 9 * SIZE(AO), %xmm2
	mulpd	 %xmm11, %xmm2
	subpd	 %xmm2, %xmm7

	movddup	 8 * SIZE(AO), %xmm4
	mulpd	 %xmm9, %xmm4
	subpd	 %xmm4, %xmm1
	movddup	 8 * SIZE(AO), %xmm4
	mulpd	 %xmm11, %xmm4
	subpd	 %xmm4, %xmm3

	movddup	 5 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm5
	mulpd	 %xmm0, %xmm7

	movddup	 4 * SIZE(AO), %xmm2
	mulpd	 %xmm5, %xmm2
	subpd	 %xmm2, %xmm1
	movddup	 4 * SIZE(AO), %xmm2
	mulpd	 %xmm7, %xmm2
	subpd	 %xmm2, %xmm3

	movddup	 0 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm1
	mulpd	 %xmm0, %xmm3
#endif

#ifdef LT
	movddup	 0 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm1
	mulpd	 %xmm0, %xmm3

	movddup	 1 * SIZE(AO), %xmm2
	mulpd	 %xmm1, %xmm2
	subpd	 %xmm2, %xmm5
	movddup	 1 * SIZE(AO), %xmm2
	mulpd	 %xmm3, %xmm2
	subpd	 %xmm2, %xmm7

	movddup	 2 * SIZE(AO), %xmm4
	mulpd	 %xmm1, %xmm4
	subpd	 %xmm4, %xmm9
	movddup	 2 * SIZE(AO), %xmm4
	mulpd	 %xmm3, %xmm4
	subpd	 %xmm4, %xmm11

	movddup	 3 * SIZE(AO), %xmm6
	mulpd	 %xmm1, %xmm6
	subpd	 %xmm6, %xmm13
	movddup	 3 * SIZE(AO), %xmm6
	mulpd	 %xmm3, %xmm6
	subpd	 %xmm6, %xmm15

	movddup	 5 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm5
	mulpd	 %xmm0, %xmm7

	movddup	 6 * SIZE(AO), %xmm2
	mulpd	 %xmm5, %xmm2
	subpd	 %xmm2, %xmm9
	movddup	 6 * SIZE(AO), %xmm2
	mulpd	 %xmm7, %xmm2
	subpd	 %xmm2, %xmm11

	movddup	 7 * SIZE(AO), %xmm4
	mulpd	 %xmm5, %xmm4
	subpd	 %xmm4, %xmm13
	movddup	 7 * SIZE(AO), %xmm4
	mulpd	 %xmm7, %xmm4
	subpd	 %xmm4, %xmm15

	movddup	10 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm0, %xmm11

	movddup	11 * SIZE(AO), %xmm2
	mulpd	 %xmm9, %xmm2
	subpd	 %xmm2, %xmm13
	movddup	11 * SIZE(AO), %xmm2
	mulpd	 %xmm11, %xmm2
	subpd	 %xmm2, %xmm15

	movddup	15 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm13
	mulpd	 %xmm0, %xmm15
#endif


#ifdef RN
	movddup	 0 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm8
	mulpd	 %xmm0, %xmm9

	movddup	 1 * SIZE(BO), %xmm1
	mulpd	 %xmm8, %xmm1
	subpd	 %xmm1, %xmm10
	movddup	 1 * SIZE(BO), %xmm1
	mulpd	 %xmm9, %xmm1
	subpd	 %xmm1, %xmm11

	movddup	 2 * SIZE(BO), %xmm2
	mulpd	 %xmm8, %xmm2
	subpd	 %xmm2, %xmm12
	movddup	 2 * SIZE(BO), %xmm2
	mulpd	 %xmm9, %xmm2
	subpd	 %xmm2, %xmm13

	movddup	 3 * SIZE(BO), %xmm3
	mulpd	 %xmm8, %xmm3
	subpd	 %xmm3, %xmm14
	movddup	 3 * SIZE(BO), %xmm3
	mulpd	 %xmm9, %xmm3
	subpd	 %xmm3, %xmm15

	movddup	 5 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm10
	mulpd	 %xmm0, %xmm11

	movddup	 6 * SIZE(BO), %xmm1
	mulpd	 %xmm10, %xmm1
	subpd	 %xmm1, %xmm12
	movddup	 6 * SIZE(BO), %xmm1
	mulpd	 %xmm11, %xmm1
	subpd	 %xmm1, %xmm13

	movddup	 7 * SIZE(BO), %xmm2
	mulpd	 %xmm10, %xmm2
	subpd	 %xmm2, %xmm14
	movddup	 7 * SIZE(BO), %xmm2
	mulpd	 %xmm11, %xmm2
	subpd	 %xmm2, %xmm15

	movddup	10 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm12
	mulpd	 %xmm0, %xmm13

	movddup	11 * SIZE(BO), %xmm1
	mulpd	 %xmm12, %xmm1
	subpd	 %xmm1, %xmm14
	movddup	11 * SIZE(BO), %xmm1
	mulpd	 %xmm13, %xmm1
	subpd	 %xmm1, %xmm15

	movddup	15 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm14
	mulpd	 %xmm0, %xmm15
#endif

#ifdef RT
	movddup	15 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm14
	mulpd	 %xmm0, %xmm15

	movddup	14 * SIZE(BO), %xmm1
	mulpd	 %xmm14, %xmm1
	subpd	 %xmm1, %xmm12
	movddup	14 * SIZE(BO), %xmm1
	mulpd	 %xmm15, %xmm1
	subpd	 %xmm1, %xmm13

	movddup	13 * SIZE(BO), %xmm2
	mulpd	 %xmm14, %xmm2
	subpd	 %xmm2, %xmm10
	movddup	13 * SIZE(BO), %xmm2
	mulpd	 %xmm15, %xmm2
	subpd	 %xmm2, %xmm11

	movddup	12 * SIZE(BO), %xmm3
	mulpd	 %xmm14, %xmm3
	subpd	 %xmm3, %xmm8
	movddup	12 * SIZE(BO), %xmm3
	mulpd	 %xmm15, %xmm3
	subpd	 %xmm3, %xmm9

	movddup	10 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm12
	mulpd	 %xmm0, %xmm13

	movddup	 9 * SIZE(BO), %xmm1
	mulpd	 %xmm12, %xmm1
	subpd	 %xmm1, %xmm10
	movddup	 9 * SIZE(BO), %xmm1
	mulpd	 %xmm13, %xmm1
	subpd	 %xmm1, %xmm11

	movddup	 8 * SIZE(BO), %xmm2
	mulpd	 %xmm12, %xmm2
	subpd	 %xmm2, %xmm8
	movddup	 8 * SIZE(BO), %xmm2
	mulpd	 %xmm13, %xmm2
	subpd	 %xmm2, %xmm9

	movddup	 5 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm10
	mulpd	 %xmm0, %xmm11

	movddup	 4 * SIZE(BO), %xmm1
	mulpd	 %xmm10, %xmm1
	subpd	 %xmm1, %xmm8
	movddup	 4 * SIZE(BO), %xmm1
	mulpd	 %xmm11, %xmm1
	subpd	 %xmm1, %xmm9

	movddup	 0 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm8
	mulpd	 %xmm0, %xmm9
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm1,  0 * SIZE(CO1)
	movsd	%xmm5,  1 * SIZE(CO1)
	movsd	%xmm9,  2 * SIZE(CO1)
	movsd	%xmm13, 3 * SIZE(CO1)

	movhpd	%xmm1,  0 * SIZE(CO2)
	movhpd	%xmm5,  1 * SIZE(CO2)
	movhpd	%xmm9,  2 * SIZE(CO2)
	movhpd	%xmm13, 3 * SIZE(CO2)

	movsd	%xmm3,  0 * SIZE(CO1, LDC, 2)
	movsd	%xmm7,  1 * SIZE(CO1, LDC, 2)
	movsd	%xmm11, 2 * SIZE(CO1, LDC, 2)
	movsd	%xmm15, 3 * SIZE(CO1, LDC, 2)

	movhpd	%xmm3,  0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm7,  1 * SIZE(CO2, LDC, 2)
	movhpd	%xmm11, 2 * SIZE(CO2, LDC, 2)
	movhpd	%xmm15, 3 * SIZE(CO2, LDC, 2)
#else
	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm9,  2 * SIZE(CO1)
	movhpd	%xmm9,  3 * SIZE(CO1)

	movsd	%xmm10,  0 * SIZE(CO2)
	movhpd	%xmm10,  1 * SIZE(CO2)
	movsd	%xmm11,  2 * SIZE(CO2)
	movhpd	%xmm11,  3 * SIZE(CO2)

	movsd	%xmm12,  0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm12,  1 * SIZE(CO1, LDC, 2)
	movsd	%xmm13,  2 * SIZE(CO1, LDC, 2)
	movhpd	%xmm13,  3 * SIZE(CO1, LDC, 2)

	movsd	%xmm14,  0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm14,  1 * SIZE(CO2, LDC, 2)
	movsd	%xmm15,  2 * SIZE(CO2, LDC, 2)
	movhpd	%xmm15,  3 * SIZE(CO2, LDC, 2)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm1,   0 * SIZE(BO)
	movapd	%xmm3,   2 * SIZE(BO)
	movapd	%xmm5,   4 * SIZE(BO)
	movapd	%xmm7,   6 * SIZE(BO)
	movapd	%xmm9,   8 * SIZE(BO)
	movapd	%xmm11, 10 * SIZE(BO)
	movapd	%xmm13, 12 * SIZE(BO)
	movapd	%xmm15, 14 * SIZE(BO)
#else
	movapd	%xmm8,   0 * SIZE(AO)
	movapd	%xmm9,   2 * SIZE(AO)
	movapd	%xmm10,  4 * SIZE(AO)
	movapd	%xmm11,  6 * SIZE(AO)
	movapd	%xmm12,  8 * SIZE(AO)
	movapd	%xmm13, 10 * SIZE(AO)
	movapd	%xmm14, 12 * SIZE(AO)
	movapd	%xmm15, 14 * SIZE(AO)
#endif

#ifndef LN
	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

#ifdef LN
	subq	$4, KK
#endif

#ifdef LT
	addq	$4, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif

	decq	I			# i --
	jg	.L11
	ALIGN_4

.L20:
	testq	$2, M
	BRANCH
	je	.L30
	ALIGN_4

.L21:

#ifdef LN
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 4), BO
#else
	movq	B, BO
#endif

	movapd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movddup	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movapd	 8 * SIZE(AO), %xmm10
	pxor	%xmm2, %xmm2
	movddup	 8 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L25
	ALIGN_4

.L22:
	mulpd	%xmm8, %xmm9
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
	addpd	%xmm9, %xmm0
	movddup	 1 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm1
	movddup	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm2
	movddup	 3 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 2 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm3
	movddup	 4 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0
	movddup	 5 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm1
	movddup	 6 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm2
	movddup	 7 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 4 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm3
	movddup	16 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm11
	addpd	%xmm11, %xmm0
	movddup	 9 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	addpd	%xmm11, %xmm1
	movddup	10 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	addpd	%xmm11, %xmm2
	movddup	11 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movapd	 6 * SIZE(AO), %xmm8
	addpd	%xmm11, %xmm3
	movddup	12 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	addpd	%xmm11, %xmm0
	movddup	13 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	addpd	%xmm11, %xmm1
	movddup	14 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	addpd	%xmm11, %xmm2
	movddup	15 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movapd	16 * SIZE(AO), %xmm8
	addpd	%xmm11, %xmm3
	movddup	24 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm9
	addpd	%xmm9, %xmm0
	movddup	17 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	addpd	%xmm9, %xmm1
	movddup	18 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	addpd	%xmm9, %xmm2
	movddup	19 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	movapd	10 * SIZE(AO), %xmm10
	addpd	%xmm9, %xmm3
	movddup	20 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	addpd	%xmm9, %xmm0
	movddup	21 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	addpd	%xmm9, %xmm1
	movddup	22 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	addpd	%xmm9, %xmm2
	movddup	23 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	movapd	12 * SIZE(AO), %xmm10
	addpd	%xmm9, %xmm3
	movddup	32 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm0
	movddup	25 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm1
	movddup	26 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm2
	movddup	27 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	movapd	14 * SIZE(AO), %xmm10
	addpd	%xmm11, %xmm3
	movddup	28 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm0
	movddup	29 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm1
	movddup	30 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm2
	movddup	31 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	movapd	24 * SIZE(AO), %xmm10
	addpd	%xmm11, %xmm3
	movddup	40 * SIZE(BO), %xmm11

	addq   $16 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L22
	ALIGN_4

.L25:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L29
	ALIGN_4

.L26:
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0
	movddup	 1 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm1
	movddup	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm2
	movddup	 3 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 2 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm3
	movddup	 4 * SIZE(BO), %xmm9

	addq	$2 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L26
	ALIGN_4

.L29:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$2, %rax
#else
	subq	$4, %rax
#endif

	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm0, %xmm8
	unpcklpd %xmm1, %xmm0
	unpckhpd %xmm1, %xmm8

	movapd	%xmm2, %xmm10
	unpcklpd %xmm3, %xmm2
	unpckhpd %xmm3, %xmm10

	movapd	 0 * SIZE(BO), %xmm1
	movapd	 2 * SIZE(BO), %xmm3
	movapd	 4 * SIZE(BO), %xmm5
	movapd	 6 * SIZE(BO), %xmm7

	subpd	%xmm0,  %xmm1
	subpd	%xmm2,  %xmm3
	subpd	%xmm8,  %xmm5
	subpd	%xmm10, %xmm7
#else

	movapd	 0 * SIZE(AO), %xmm8
	movapd	 2 * SIZE(AO), %xmm10
	movapd	 4 * SIZE(AO), %xmm12
	movapd	 6 * SIZE(AO), %xmm14

	subpd	%xmm0, %xmm8
	subpd	%xmm1, %xmm10
	subpd	%xmm2, %xmm12
	subpd	%xmm3, %xmm14
#endif

#ifdef LN
	movddup	 3 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm5
	mulpd	 %xmm0, %xmm7

	movddup	 2 * SIZE(AO), %xmm2
	mulpd	 %xmm5, %xmm2
	subpd	 %xmm2, %xmm1
	movddup	 2 * SIZE(AO), %xmm2
	mulpd	 %xmm7, %xmm2
	subpd	 %xmm2, %xmm3

	movddup	 0 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm1
	mulpd	 %xmm0, %xmm3
#endif

#ifdef LT
	movddup	 0 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm1
	mulpd	 %xmm0, %xmm3

	movddup	 1 * SIZE(AO), %xmm2
	mulpd	 %xmm1, %xmm2
	subpd	 %xmm2, %xmm5
	movddup	 1 * SIZE(AO), %xmm2
	mulpd	 %xmm3, %xmm2
	subpd	 %xmm2, %xmm7

	movddup	 3 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm5
	mulpd	 %xmm0, %xmm7
#endif

#ifdef RN
	movddup	 0 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm8

	movddup	 1 * SIZE(BO), %xmm1
	mulpd	 %xmm8, %xmm1
	subpd	 %xmm1, %xmm10
	movddup	 2 * SIZE(BO), %xmm2
	mulpd	 %xmm8, %xmm2
	subpd	 %xmm2, %xmm12
	movddup	 3 * SIZE(BO), %xmm3
	mulpd	 %xmm8, %xmm3
	subpd	 %xmm3, %xmm14

	movddup	 5 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm10
	movddup	 6 * SIZE(BO), %xmm1
	mulpd	 %xmm10, %xmm1
	subpd	 %xmm1, %xmm12
	movddup	 7 * SIZE(BO), %xmm2
	mulpd	 %xmm10, %xmm2
	subpd	 %xmm2, %xmm14

	movddup	10 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm12

	movddup	11 * SIZE(BO), %xmm1
	mulpd	 %xmm12, %xmm1
	subpd	 %xmm1, %xmm14

	movddup	15 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm14
#endif

#ifdef RT
	movddup	15 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm14

	movddup	14 * SIZE(BO), %xmm1
	mulpd	 %xmm14, %xmm1
	subpd	 %xmm1, %xmm12
	movddup	13 * SIZE(BO), %xmm2
	mulpd	 %xmm14, %xmm2
	subpd	 %xmm2, %xmm10
	movddup	12 * SIZE(BO), %xmm3
	mulpd	 %xmm14, %xmm3
	subpd	 %xmm3, %xmm8

	movddup	10 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm12
	movddup	 9 * SIZE(BO), %xmm1
	mulpd	 %xmm12, %xmm1
	subpd	 %xmm1, %xmm10
	movddup	 8 * SIZE(BO), %xmm2
	mulpd	 %xmm12, %xmm2
	subpd	 %xmm2, %xmm8

	movddup	 5 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm10
	movddup	 4 * SIZE(BO), %xmm1
	mulpd	 %xmm10, %xmm1
	subpd	 %xmm1, %xmm8

	movddup	 0 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm8
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm1,  0 * SIZE(CO1)
	movsd	%xmm5,  1 * SIZE(CO1)
	movhpd	%xmm1,  0 * SIZE(CO2)
	movhpd	%xmm5,  1 * SIZE(CO2)

	movsd	%xmm3,  0 * SIZE(CO1, LDC, 2)
	movsd	%xmm7,  1 * SIZE(CO1, LDC, 2)
	movhpd	%xmm3,  0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm7,  1 * SIZE(CO2, LDC, 2)
#else
	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm10,  0 * SIZE(CO2)
	movhpd	%xmm10,  1 * SIZE(CO2)

	movsd	%xmm12,  0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm12,  1 * SIZE(CO1, LDC, 2)
	movsd	%xmm14,  0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm14,  1 * SIZE(CO2, LDC, 2)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm1,   0 * SIZE(BO)
	movapd	%xmm3,   2 * SIZE(BO)
	movapd	%xmm5,   4 * SIZE(BO)
	movapd	%xmm7,   6 * SIZE(BO)
#else
	movapd	%xmm8,   0 * SIZE(AO)
	movapd	%xmm10,  2 * SIZE(AO)
	movapd	%xmm12,  4 * SIZE(AO)
	movapd	%xmm14,  6 * SIZE(AO)
#endif

#ifndef LN
	addq	$2 * SIZE, CO1
	addq	$2 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

#ifdef LN
	subq	$2, KK
#endif

#ifdef LT
	addq	$2, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L30:
	testq	$1, M
	je	.L39
	ALIGN_4

.L31:
#ifdef LN
       movq	K, %rax
       salq	$0 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif


#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 4), BO
#else
	movq	B, BO
#endif

	movddup	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movapd	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movddup	 4 * SIZE(AO), %xmm10
	pxor	%xmm2, %xmm2
	movapd	 8 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L35
	ALIGN_4

.L32:
	mulpd	%xmm8, %xmm9
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
	addpd	%xmm9, %xmm0
	movapd	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movddup	 1 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm1
	movapd	 4 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0
	movapd	 6 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movddup	 2 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm1
	movapd	16 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm11
	addpd	%xmm11, %xmm0
	movapd	10 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movddup	 3 * SIZE(AO), %xmm8
	addpd	%xmm11, %xmm1
	movapd	12 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	addpd	%xmm11, %xmm0
	movapd	14 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movddup	 8 * SIZE(AO), %xmm8
	addpd	%xmm11, %xmm1
	movapd	24 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm9
	addpd	%xmm9, %xmm0
	movapd	18 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	movddup	 5 * SIZE(AO), %xmm10
	addpd	%xmm9, %xmm1
	movapd	20 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	addpd	%xmm9, %xmm0
	movapd	22 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	movddup	 6 * SIZE(AO), %xmm10
	addpd	%xmm9, %xmm1
	movapd	32 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm0
	movapd	26 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	movddup	 7 * SIZE(AO), %xmm10
	addpd	%xmm11, %xmm1
	movapd	28 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm0
	movapd	30 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	movddup	12 * SIZE(AO), %xmm10
	addpd	%xmm11, %xmm1
	movapd	40 * SIZE(BO), %xmm11

	addq   $ 8 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L32
	ALIGN_4

.L35:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L38
	ALIGN_4

.L36:
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0
	movapd	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movddup	 1 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm1
	movapd	 4 * SIZE(BO), %xmm9

	addq	$1 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L36
	ALIGN_4

.L38:

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$1, %rax
#else
	subq	$4, %rax
#endif

	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 4), BO
#endif


#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(BO), %xmm2
	movapd	 2 * SIZE(BO), %xmm3

	subpd	%xmm0,  %xmm2
	subpd	%xmm1,  %xmm3
#else
	movapd	 0 * SIZE(AO), %xmm2
	movapd	 2 * SIZE(AO), %xmm3

	subpd	%xmm0, %xmm2
	subpd	%xmm1, %xmm3
#endif

#ifdef LN
	movddup	 0 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm2
	mulpd	 %xmm0, %xmm3
#endif

#ifdef LT
	movddup	 0 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm2
	mulpd	 %xmm0, %xmm3
#endif

#ifdef RN
	movapd	%xmm2, %xmm0
        unpckhpd %xmm0, %xmm0

	movapd	%xmm3, %xmm1
        unpckhpd %xmm1, %xmm1

	movsd	 0 * SIZE(BO), %xmm4
	mulsd	 %xmm4, %xmm2

	movsd	 1 * SIZE(BO), %xmm5
	mulsd	 %xmm2, %xmm5
	subsd	 %xmm5, %xmm0
	movsd	 2 * SIZE(BO), %xmm6
	mulsd	 %xmm2, %xmm6
	subsd	 %xmm6, %xmm3
	movsd	 3 * SIZE(BO), %xmm7
	mulsd	 %xmm2, %xmm7
	subsd	 %xmm7, %xmm1

	movsd	 5 * SIZE(BO), %xmm4
	mulsd	 %xmm4, %xmm0

	movsd	 6 * SIZE(BO), %xmm5
	mulsd	 %xmm0, %xmm5
	subsd	 %xmm5, %xmm3
	movsd	 7 * SIZE(BO), %xmm6
	mulsd	 %xmm0, %xmm6
	subsd	 %xmm6, %xmm1

	movsd	10 * SIZE(BO), %xmm4
	mulsd	 %xmm4, %xmm3

	movsd	11 * SIZE(BO), %xmm5
	mulsd	 %xmm3, %xmm5
	subsd	 %xmm5, %xmm1

	movsd	15 * SIZE(BO), %xmm4
	mulsd	 %xmm4, %xmm1

	unpcklpd %xmm0, %xmm2
	unpcklpd %xmm1, %xmm3
#endif

#ifdef RT
	movapd	%xmm2, %xmm0
        unpckhpd %xmm0, %xmm0

	movapd	%xmm3, %xmm1
        unpckhpd %xmm1, %xmm1

	movsd	15 * SIZE(BO), %xmm4
	mulsd	 %xmm4, %xmm1

	movsd	14 * SIZE(BO), %xmm5
	mulsd	 %xmm1, %xmm5
	subsd	 %xmm5, %xmm3
	movsd	13 * SIZE(BO), %xmm6
	mulsd	 %xmm1, %xmm6
	subsd	 %xmm6, %xmm0
	movsd	12 * SIZE(BO), %xmm7
	mulsd	 %xmm1, %xmm7
	subsd	 %xmm7, %xmm2

	movsd	10 * SIZE(BO), %xmm4
	mulsd	 %xmm4, %xmm3

	movsd	 9 * SIZE(BO), %xmm5
	mulsd	 %xmm3, %xmm5
	subsd	 %xmm5, %xmm0
	movsd	 8 * SIZE(BO), %xmm6
	mulsd	 %xmm3, %xmm6
	subsd	 %xmm6, %xmm2

	movsd	 5 * SIZE(BO), %xmm4
	mulsd	 %xmm4, %xmm0

	movsd	 4 * SIZE(BO), %xmm5
	mulsd	 %xmm0, %xmm5
	subsd	 %xmm5, %xmm2

	movsd	 0 * SIZE(BO), %xmm4
	mulsd	 %xmm4, %xmm2

	unpcklpd %xmm0, %xmm2
	unpcklpd %xmm1, %xmm3

#endif

#ifdef LN
	subq	$1 * SIZE, CO1
	subq	$1 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm2,  0 * SIZE(CO1)
	movhpd	%xmm2,  0 * SIZE(CO2)
	movsd	%xmm3,  0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm3,  0 * SIZE(CO2, LDC, 2)
#else
	movsd	%xmm2,  0 * SIZE(CO1)
	movhpd	%xmm2,  0 * SIZE(CO2)
	movsd	%xmm3,  0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm3,  0 * SIZE(CO2, LDC, 2)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm2,   0 * SIZE(BO)
	movapd	%xmm3,   2 * SIZE(BO)
#else
	movapd	%xmm2,   0 * SIZE(AO)
	movapd	%xmm3,   2 * SIZE(AO)
#endif

#ifndef LN
	addq	$1 * SIZE, CO1
	addq	$1 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

#ifdef LN
	subq	$1, KK
#endif

#ifdef LT
	addq	$1, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$0 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L39:
#ifdef LN
       leaq	(, K, SIZE), %rax
       leaq	(B, %rax, 4), B
#endif
#if defined(LT) || defined(RN)
	movq	BO, B
#endif

#ifdef RN
	addq	$4, KK
#endif

#ifdef RT
	subq	$4, KK
#endif

	decq	J			# j --
	jg	.L10
	ALIGN_4

.L40:
	testq	$2, N
	je	.L80
	ALIGN_4

#if defined(LT) || defined(RN)
	movq	A, AO
#else
	movq	A, AORIG
#endif

#ifdef RT
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       subq	%rax, B

       leaq	(, LDC, 2), %rax
       subq	%rax, C
#endif

	movq	C, CO1
	leaq	(C, LDC, 1), CO2
#ifndef RT
	leaq	(C, LDC, 2), C
#endif

#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

	movq	K, %rax
	salq	$BASE_SHIFT + 1, %rax
	leaq	(B, %rax), BB

#ifdef LT
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L60
	ALIGN_4

.L51:
#ifdef LN
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 2), BO
#else
	movq	B, BO
#endif

	prefetcht0	  0 * SIZE(BB)
	subq	   $-4 * SIZE, BB

	movapd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movddup	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movapd	 8 * SIZE(AO), %xmm10
	pxor	%xmm4, %xmm4
	movddup	 8 * SIZE(BO), %xmm11
	pxor	%xmm5, %xmm5

#ifdef HAVE_3DNOW
	prefetchw      4 * SIZE(CO1)
	prefetchw      4 * SIZE(CO2)
#else
	prefetchnta     4 * SIZE(CO1)
	prefetchnta     4 * SIZE(CO2)
#endif

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L55
	ALIGN_4

.L52:
	mulpd	%xmm8, %xmm9
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
	addpd	%xmm9, %xmm0
	movddup	 1 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 2 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm1
	movddup	 0 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm4
	movddup	 1 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 4 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm5
	movddup	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0
	movddup	 3 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 6 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm1
	movddup	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm4
	movddup	 3 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	16 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm5
	movddup	 4 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	addpd	%xmm9, %xmm0
	movddup	 5 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	movapd	10 * SIZE(AO), %xmm10
	addpd	%xmm9, %xmm1
	movddup	 4 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	addpd	%xmm9, %xmm4
	movddup	 5 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	movapd	12 * SIZE(AO), %xmm10
	addpd	%xmm9, %xmm5
	movddup	 6 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	addpd	%xmm9, %xmm0
	movddup	 7 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	movapd	14 * SIZE(AO), %xmm10
	addpd	%xmm9, %xmm1
	movddup	 6 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	addpd	%xmm9, %xmm4
	movddup	 7 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	movapd	40 * SIZE(AO), %xmm10
	addpd	%xmm9, %xmm5
	movddup	16 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm11
	PREFETCH  (PREFETCHSIZE + 16) * SIZE(AO)
	addpd	%xmm11, %xmm0
	movddup	 9 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movapd	18 * SIZE(AO), %xmm8
	addpd	%xmm11, %xmm1
	movddup	 8 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	addpd	%xmm11, %xmm4
	movddup	 9 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movapd	20 * SIZE(AO), %xmm8
	addpd	%xmm11, %xmm5
	movddup	10 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	addpd	%xmm11, %xmm0
	movddup	11 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movapd	22 * SIZE(AO), %xmm8
	addpd	%xmm11, %xmm1
	movddup	10 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	addpd	%xmm11, %xmm4
	movddup	11 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movapd	24 * SIZE(AO), %xmm8
	addpd	%xmm11, %xmm5
	movddup	12 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	addpd	%xmm11, %xmm0
	movddup	13 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movapd	26 * SIZE(AO), %xmm8
	addpd	%xmm11, %xmm1
	movddup	12 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	addpd	%xmm11, %xmm4
	movddup	13 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movapd	28 * SIZE(AO), %xmm8
	addpd	%xmm11, %xmm5
	movddup	14 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	addpd	%xmm11, %xmm0
	movddup	15 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movapd	30 * SIZE(AO), %xmm8
	addpd	%xmm11, %xmm1
	movddup	14 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	addpd	%xmm11, %xmm4
	movddup	15 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movapd	32 * SIZE(AO), %xmm8
	addpd	%xmm11, %xmm5
	movddup	24 * SIZE(BO), %xmm11

	addq   $32 * SIZE, AO
	addq   $16 * SIZE, BO
	decq   %rax
	jne    .L52
	ALIGN_4

.L55:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L59
	ALIGN_4

.L56:
	mulpd	%xmm8, %xmm9
	movapd	 2 * SIZE(AO), %xmm10
	addpd	%xmm9, %xmm0
	movddup	 1 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movddup	 0 * SIZE(BO), %xmm11
	addpd	%xmm9, %xmm1
	movddup	 2 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm11
	movapd	 4 * SIZE(AO), %xmm8
	addpd	%xmm11, %xmm4
	movddup	 1 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm5

	addq	$4 * SIZE, AO		# aoffset  += 4
	addq	$2 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L56
	ALIGN_4

.L59:
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$4, %rax
#else
	subq	$2, %rax
#endif
	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 2), BO
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm0, %xmm8
	unpcklpd %xmm1, %xmm0
	unpckhpd %xmm1, %xmm8

	movapd	%xmm4, %xmm12
	unpcklpd %xmm5, %xmm4
	unpckhpd %xmm5, %xmm12

	movapd	 0 * SIZE(BO), %xmm1
	movapd	 2 * SIZE(BO), %xmm5
	movapd	 4 * SIZE(BO), %xmm9
	movapd	 6 * SIZE(BO), %xmm13

	subpd	%xmm0,  %xmm1
	subpd	%xmm8,  %xmm5
	subpd	%xmm4,  %xmm9
	subpd	%xmm12, %xmm13
#else

	movapd	 0 * SIZE(AO), %xmm8
	movapd	 2 * SIZE(AO), %xmm9
	movapd	 4 * SIZE(AO), %xmm10
	movapd	 6 * SIZE(AO), %xmm11

	subpd	%xmm0, %xmm8
	subpd	%xmm4, %xmm9
	subpd	%xmm1, %xmm10
	subpd	%xmm5, %xmm11
#endif


#ifdef LN
	movddup	15 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm13

	movddup	14 * SIZE(AO), %xmm2
	mulpd	 %xmm13, %xmm2
	subpd	 %xmm2, %xmm9
	movddup	13 * SIZE(AO), %xmm4
	mulpd	 %xmm13, %xmm4
	subpd	 %xmm4, %xmm5
	movddup	12 * SIZE(AO), %xmm6
	mulpd	 %xmm13, %xmm6
	subpd	 %xmm6, %xmm1

	movddup	10 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm9
	movddup	 9 * SIZE(AO), %xmm2
	mulpd	 %xmm9, %xmm2
	subpd	 %xmm2, %xmm5
	movddup	 8 * SIZE(AO), %xmm4
	mulpd	 %xmm9, %xmm4
	subpd	 %xmm4, %xmm1

	movddup	 5 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm5
	movddup	 4 * SIZE(AO), %xmm2
	mulpd	 %xmm5, %xmm2
	subpd	 %xmm2, %xmm1

	movddup	 0 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm1
#endif


#ifdef LT
	movddup	 0 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm1

	movddup	 1 * SIZE(AO), %xmm2
	mulpd	 %xmm1, %xmm2
	subpd	 %xmm2, %xmm5
	movddup	 2 * SIZE(AO), %xmm4
	mulpd	 %xmm1, %xmm4
	subpd	 %xmm4, %xmm9
	movddup	 3 * SIZE(AO), %xmm6
	mulpd	 %xmm1, %xmm6
	subpd	 %xmm6, %xmm13

	movddup	 5 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm5

	movddup	 6 * SIZE(AO), %xmm2
	mulpd	 %xmm5, %xmm2
	subpd	 %xmm2, %xmm9
	movddup	 7 * SIZE(AO), %xmm4
	mulpd	 %xmm5, %xmm4
	subpd	 %xmm4, %xmm13

	movddup	10 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm9

	movddup	11 * SIZE(AO), %xmm2
	mulpd	 %xmm9, %xmm2
	subpd	 %xmm2, %xmm13

	movddup	15 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm13
#endif

#ifdef RN
	movddup	 0 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm8
	mulpd	 %xmm0, %xmm9

	movddup	 1 * SIZE(BO), %xmm1
	mulpd	 %xmm8, %xmm1
	subpd	 %xmm1, %xmm10
	movddup	 1 * SIZE(BO), %xmm1
	mulpd	 %xmm9, %xmm1
	subpd	 %xmm1, %xmm11

	movddup	 3 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm10
	mulpd	 %xmm0, %xmm11
#endif

#ifdef RT
	movddup	 3 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm10
	mulpd	 %xmm0, %xmm11

	movddup	 2 * SIZE(BO), %xmm1
	mulpd	 %xmm10, %xmm1
	subpd	 %xmm1, %xmm8
	movddup	 2 * SIZE(BO), %xmm1
	mulpd	 %xmm11, %xmm1
	subpd	 %xmm1, %xmm9

	movddup	 0 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm8
	mulpd	 %xmm0, %xmm9
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm1,  0 * SIZE(CO1)
	movsd	%xmm5,  1 * SIZE(CO1)
	movsd	%xmm9,  2 * SIZE(CO1)
	movsd	%xmm13, 3 * SIZE(CO1)

	movhpd	%xmm1,  0 * SIZE(CO2)
	movhpd	%xmm5,  1 * SIZE(CO2)
	movhpd	%xmm9,  2 * SIZE(CO2)
	movhpd	%xmm13, 3 * SIZE(CO2)
#else
	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm9,  2 * SIZE(CO1)
	movhpd	%xmm9,  3 * SIZE(CO1)

	movsd	%xmm10,  0 * SIZE(CO2)
	movhpd	%xmm10,  1 * SIZE(CO2)
	movsd	%xmm11,  2 * SIZE(CO2)
	movhpd	%xmm11,  3 * SIZE(CO2)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm1,   0 * SIZE(BO)
	movapd	%xmm5,   2 * SIZE(BO)
	movapd	%xmm9,   4 * SIZE(BO)
	movapd	%xmm13,  6 * SIZE(BO)
#else
	movapd	%xmm8,   0 * SIZE(AO)
	movapd	%xmm9,   2 * SIZE(AO)
	movapd	%xmm10,  4 * SIZE(AO)
	movapd	%xmm11,  6 * SIZE(AO)
#endif

#ifndef LN
	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

#ifdef LN
	subq	$4, KK
#endif

#ifdef LT
	addq	$4, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif

	decq	I			# i --
	jg	.L51
	ALIGN_4

.L60:
	testq	$2, M
	je	.L70
	ALIGN_4

.L61:
#ifdef LN
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 2), BO
#else
	movq	B, BO
#endif

	movapd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movddup	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movapd	 8 * SIZE(AO), %xmm10
	pxor	%xmm2, %xmm2
	movddup	 8 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L65
	ALIGN_4

.L62:
	mulpd	%xmm8, %xmm9
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AO)
	addpd	%xmm9, %xmm0
	movddup	 1 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 2 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm1
	movddup	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm2
	movddup	 3 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 4 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm3
	movddup	 4 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0
	movddup	 5 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 6 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm1
	movddup	 6 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm2
	movddup	 7 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	16 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm3
	movddup	16 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm0
	movddup	 9 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	movapd	10 * SIZE(AO), %xmm10
	addpd	%xmm11, %xmm1
	movddup	10 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm2
	movddup	11 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	movapd	12 * SIZE(AO), %xmm10
	addpd	%xmm11, %xmm3
	movddup	12 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm0
	movddup	13 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	movapd	14 * SIZE(AO), %xmm10
	addpd	%xmm11, %xmm1
	movddup	14 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	addpd	%xmm11, %xmm2
	movddup	15 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	movapd	24 * SIZE(AO), %xmm10
	addpd	%xmm11, %xmm3
	movddup	24 * SIZE(BO), %xmm11

	addq   $16 * SIZE, AO
	addq   $16 * SIZE, BO
	decq   %rax
	jne    .L62
	ALIGN_4

.L65:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L69
	ALIGN_4

.L66:
	mulpd	%xmm8, %xmm9
	addpd	%xmm9, %xmm0
	movddup	 1 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 2 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm1
	movddup	 2 * SIZE(BO), %xmm9

	addq	$2 * SIZE, AO		# aoffset  += 4
	addq	$2 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L66
	ALIGN_4

.L69:
	addpd	%xmm2, %xmm0
	addpd	%xmm3, %xmm1

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$2, %rax
#else
	subq	$2, %rax
#endif
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 2), BO
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm0, %xmm8
	unpcklpd %xmm1, %xmm0
	unpckhpd %xmm1, %xmm8

	movapd	 0 * SIZE(BO), %xmm1
	movapd	 2 * SIZE(BO), %xmm5

	subpd	%xmm0,  %xmm1
	subpd	%xmm8,  %xmm5
#else

	movapd	 0 * SIZE(AO), %xmm8
	movapd	 2 * SIZE(AO), %xmm10

	subpd	%xmm0, %xmm8
	subpd	%xmm1, %xmm10
#endif

#ifdef LN
	movddup	 3 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm5
	movddup	 2 * SIZE(AO), %xmm2
	mulpd	 %xmm5, %xmm2
	subpd	 %xmm2, %xmm1

	movddup	 0 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm1
#endif

#ifdef LT
	movddup	 0 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm1

	movddup	 1 * SIZE(AO), %xmm2
	mulpd	 %xmm1, %xmm2
	subpd	 %xmm2, %xmm5

	movddup	 3 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm5
#endif

#ifdef RN
	movddup	 0 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm8

	movddup	 1 * SIZE(BO), %xmm1
	mulpd	 %xmm8, %xmm1
	subpd	 %xmm1, %xmm10

	movddup	 3 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm10
#endif

#ifdef RT
	movddup	 3 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm10

	movddup	 2 * SIZE(BO), %xmm1
	mulpd	 %xmm10, %xmm1
	subpd	 %xmm1, %xmm8

	movddup	 0 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm8
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm1,  0 * SIZE(CO1)
	movsd	%xmm5,  1 * SIZE(CO1)
	movhpd	%xmm1,  0 * SIZE(CO2)
	movhpd	%xmm5,  1 * SIZE(CO2)
#else
	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm10, 0 * SIZE(CO2)
	movhpd	%xmm10, 1 * SIZE(CO2)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm1,   0 * SIZE(BO)
	movapd	%xmm5,   2 * SIZE(BO)
#else
	movapd	%xmm8,   0 * SIZE(AO)
	movapd	%xmm10,  2 * SIZE(AO)
#endif

#ifndef LN
	addq	$2 * SIZE, CO1
	addq	$2 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#endif

#ifdef LN
	subq	$2, KK
#endif

#ifdef LT
	addq	$2, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L70:
	testq	$1, M
	je	.L79
	ALIGN_4

.L71:
#ifdef LN
       movq	K, %rax
       salq	$0 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 2), BO
#else
	movq	B, BO
#endif

	movddup	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movapd	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movddup	 4 * SIZE(AO), %xmm10
	pxor	%xmm2, %xmm2
	movapd	 8 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L75
	ALIGN_4

.L72:
	mulpd	%xmm8, %xmm9
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AO)
	movddup	 1 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm0
	mulpd	 2 * SIZE(BO), %xmm8
	movapd	16 * SIZE(BO), %xmm9
	addpd	%xmm8, %xmm1
	movddup	 2 * SIZE(AO), %xmm8
	mulpd	 4 * SIZE(BO), %xmm8
	addpd	%xmm8, %xmm2
	movddup	 3 * SIZE(AO), %xmm8
	mulpd	 6 * SIZE(BO), %xmm8
	addpd	%xmm8, %xmm3
	movddup	 8 * SIZE(AO), %xmm8
	mulpd	%xmm10, %xmm11
	movddup	 5 * SIZE(AO), %xmm10
	addpd	%xmm11, %xmm0
	mulpd	10 * SIZE(BO), %xmm10
	movapd	24 * SIZE(BO), %xmm11
	addpd	%xmm10, %xmm1
	movddup	 6 * SIZE(AO), %xmm10
	mulpd	12 * SIZE(BO), %xmm10
	addpd	%xmm10, %xmm2
	movddup	 7 * SIZE(AO), %xmm10
	mulpd	14 * SIZE(BO), %xmm10
	addpd	%xmm10, %xmm3
	movddup	12 * SIZE(AO), %xmm10

	addq   $ 8 * SIZE, AO
	addq   $16 * SIZE, BO
	decq   %rax
	jne    .L72
	ALIGN_4

.L75:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L78
	ALIGN_4

.L76:
	mulpd	%xmm8, %xmm9
	movddup	 1 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm0
	movapd	 2 * SIZE(BO), %xmm9

	addq	$1 * SIZE, AO		# aoffset  += 4
	addq	$2 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L76
	ALIGN_4

.L78:
	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	addpd	%xmm2, %xmm0

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$1, %rax
#else
	subq	$2, %rax
#endif
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 2), BO
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(BO), %xmm2
	subpd	%xmm0, %xmm2
#else
	movapd	 0 * SIZE(AO), %xmm2
	subpd	%xmm0, %xmm2
#endif

#ifdef LN
	movddup	 0 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm2
#endif

#ifdef LT
	movddup	 0 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm2
#endif

#ifdef RN
	movapd	%xmm2, %xmm0
        unpckhpd %xmm0, %xmm0

	movsd	 0 * SIZE(BO), %xmm4
	mulsd	 %xmm4, %xmm2

	movsd	 1 * SIZE(BO), %xmm5
	mulsd	 %xmm2, %xmm5
	subsd	 %xmm5, %xmm0

	movsd	 3 * SIZE(BO), %xmm4
	mulsd	 %xmm4, %xmm0

	unpcklpd %xmm0, %xmm2
#endif

#ifdef RT
	movapd	%xmm2, %xmm0
        unpckhpd %xmm0, %xmm0

	movsd	 3 * SIZE(BO), %xmm4
	mulsd	 %xmm4, %xmm0

	movsd	 2 * SIZE(BO), %xmm5
	mulsd	 %xmm0, %xmm5
	subsd	 %xmm5, %xmm2

	movsd	 0 * SIZE(BO), %xmm4
	mulsd	 %xmm4, %xmm2

	unpcklpd %xmm0, %xmm2
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
	subq	$1 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm2,  0 * SIZE(CO1)
	movhpd	%xmm2,  0 * SIZE(CO2)
#else
	movsd	%xmm2,  0 * SIZE(CO1)
	movhpd	%xmm2,  0 * SIZE(CO2)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm2,   0 * SIZE(BO)
#else
	movapd	%xmm2,   0 * SIZE(AO)
#endif

#ifndef LN
	addq	$1 * SIZE, CO1
	addq	$1 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
#endif

#ifdef LN
	subq	$1, KK
#endif

#ifdef LT
	addq	$1, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$0 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L79:
#ifdef LN
       leaq	(, K, SIZE), %rax
       leaq	(B, %rax, 2), B
#endif

#if defined(LT) || defined(RN)
	movq	BO, B
#endif

#ifdef RN
	addq	$2, KK
#endif

#ifdef RT
	subq	$2, KK
#endif
	ALIGN_4

.L80:
	testq	$1, N
	je	.L999
	ALIGN_4

#if defined(LT) || defined(RN)
	movq	A, AO
#else
	movq	A, AORIG
#endif

#ifdef RT
       movq	K, %rax
       salq	$0 + BASE_SHIFT, %rax
       subq	%rax, B

       subq	LDC, C
#endif


	movq	C, CO1
#ifndef RT
	addq	LDC, C
#endif

#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

#ifdef LT
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L100
	ALIGN_4

.L91:
#ifdef LN
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 1), BO
#else
	movq	B, BO
#endif

	movapd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movddup	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movapd	 8 * SIZE(AO), %xmm10
	pxor	%xmm2, %xmm2
	movddup	 4 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3

#ifdef HAVE_3DNOW
	prefetchw      4 * SIZE(CO1)
#else
	prefetchnta     4 * SIZE(CO1)
#endif

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L95
	ALIGN_4

.L92:
	mulpd	%xmm9, %xmm8
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AO)
	mulpd	 2 * SIZE(AO), %xmm9
	addpd	%xmm8, %xmm0
	movapd	 4 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm1
	movddup	 1 * SIZE(BO), %xmm9
	mulpd	%xmm9, %xmm8
	mulpd	 6 * SIZE(AO), %xmm9
	addpd	%xmm8, %xmm2
	movapd	16 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm3
	movddup	 2 * SIZE(BO), %xmm9
	mulpd	%xmm9, %xmm10
	mulpd	10 * SIZE(AO), %xmm9
	addpd	%xmm10, %xmm0
	movapd	12 * SIZE(AO), %xmm10
	addpd	%xmm9, %xmm1
	movddup	 3 * SIZE(BO), %xmm9
	mulpd	%xmm9, %xmm10
	mulpd	14 * SIZE(AO), %xmm9
	addpd	%xmm10, %xmm2
	movapd	24 * SIZE(AO), %xmm10
	PREFETCH  (PREFETCHSIZE + 16) * SIZE(AO)
	addpd	%xmm9, %xmm3
	movddup	 8 * SIZE(BO), %xmm9
	mulpd	%xmm11, %xmm8
	mulpd	18 * SIZE(AO), %xmm11
	addpd	%xmm8, %xmm0
	movapd	20 * SIZE(AO), %xmm8
	addpd	%xmm11, %xmm1
	movddup	 5 * SIZE(BO), %xmm11
	mulpd	%xmm11, %xmm8
	mulpd	22 * SIZE(AO), %xmm11
	addpd	%xmm8, %xmm2
	movapd	32 * SIZE(AO), %xmm8
	addpd	%xmm11, %xmm3
	movddup	 6 * SIZE(BO), %xmm11
	mulpd	%xmm11, %xmm10
	mulpd	26 * SIZE(AO), %xmm11
	addpd	%xmm10, %xmm0
	movapd	28 * SIZE(AO), %xmm10
	addpd	%xmm11, %xmm1
	movddup	 7 * SIZE(BO), %xmm11
	mulpd	%xmm11, %xmm10
	mulpd	30 * SIZE(AO), %xmm11
	addpd	%xmm10, %xmm2
	movapd	40 * SIZE(AO), %xmm10
	addpd	%xmm11, %xmm3
	movddup	12 * SIZE(BO), %xmm11

	addq   $32 * SIZE, AO
	addq   $8 * SIZE, BO
	decq   %rax
	jne    .L92
	ALIGN_4

.L95:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L99
	ALIGN_4

.L96:
	mulpd	%xmm9, %xmm8
	mulpd	 2 * SIZE(AO), %xmm9
	addpd	%xmm8, %xmm0
	movapd	 4 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm1
	movddup	 1 * SIZE(BO), %xmm9

	addq	$4 * SIZE, AO		# aoffset  += 4
	addq	$1 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L96
	ALIGN_4

.L99:
	addpd	%xmm2, %xmm0
	addpd	%xmm3, %xmm1

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$4, %rax
#else
	subq	$1, %rax
#endif
	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 1), BO
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(BO), %xmm2
	movapd	 2 * SIZE(BO), %xmm3

	subpd	%xmm0,  %xmm2
	subpd	%xmm1,  %xmm3
#else
	movapd	 0 * SIZE(AO), %xmm2
	movapd	 2 * SIZE(AO), %xmm3

	subpd	%xmm0, %xmm2
	subpd	%xmm1, %xmm3
#endif

#ifdef LN
	movapd	%xmm2, %xmm0
        unpckhpd %xmm0, %xmm0

	movapd	%xmm3, %xmm1
        unpckhpd %xmm1, %xmm1

	movsd	15 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm1

	movsd	14 * SIZE(AO), %xmm5
	mulsd	 %xmm1, %xmm5
	subsd	 %xmm5, %xmm3
	movsd	13 * SIZE(AO), %xmm6
	mulsd	 %xmm1, %xmm6
	subsd	 %xmm6, %xmm0
	movsd	12 * SIZE(AO), %xmm7
	mulsd	 %xmm1, %xmm7
	subsd	 %xmm7, %xmm2

	movsd	10 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm3

	movsd	 9 * SIZE(AO), %xmm5
	mulsd	 %xmm3, %xmm5
	subsd	 %xmm5, %xmm0
	movsd	 8 * SIZE(AO), %xmm6
	mulsd	 %xmm3, %xmm6
	subsd	 %xmm6, %xmm2

	movsd	 5 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm0

	movsd	 4 * SIZE(AO), %xmm5
	mulsd	 %xmm0, %xmm5
	subsd	 %xmm5, %xmm2

	movsd	 0 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm2

	unpcklpd %xmm0, %xmm2
	unpcklpd %xmm1, %xmm3
#endif

#ifdef LT
	movapd	%xmm2, %xmm0
        unpckhpd %xmm0, %xmm0

	movapd	%xmm3, %xmm1
        unpckhpd %xmm1, %xmm1

	movsd	 0 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm2

	movsd	 1 * SIZE(AO), %xmm5
	mulsd	 %xmm2, %xmm5
	subsd	 %xmm5, %xmm0
	movsd	 2 * SIZE(AO), %xmm6
	mulsd	 %xmm2, %xmm6
	subsd	 %xmm6, %xmm3
	movsd	 3 * SIZE(AO), %xmm7
	mulsd	 %xmm2, %xmm7
	subsd	 %xmm7, %xmm1

	movsd	 5 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm0

	movsd	 6 * SIZE(AO), %xmm5
	mulsd	 %xmm0, %xmm5
	subsd	 %xmm5, %xmm3
	movsd	 7 * SIZE(AO), %xmm6
	mulsd	 %xmm0, %xmm6
	subsd	 %xmm6, %xmm1

	movsd	10 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm3

	movsd	11 * SIZE(AO), %xmm5
	mulsd	 %xmm3, %xmm5
	subsd	 %xmm5, %xmm1

	movsd	15 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm1

	unpcklpd %xmm0, %xmm2
	unpcklpd %xmm1, %xmm3
#endif

#ifdef RN
	movddup	 0 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm2
	mulpd	 %xmm0, %xmm3
#endif

#ifdef RT
	movddup	 0 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm2
	mulpd	 %xmm0, %xmm3
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm2,  0 * SIZE(CO1)
	movhpd	%xmm2,  1 * SIZE(CO1)
	movsd	%xmm3,  2 * SIZE(CO1)
	movhpd	%xmm3,  3 * SIZE(CO1)
#else
	movsd	%xmm2,  0 * SIZE(CO1)
	movhpd	%xmm2,  1 * SIZE(CO1)
	movsd	%xmm3,  2 * SIZE(CO1)
	movhpd	%xmm3,  3 * SIZE(CO1)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm2,   0 * SIZE(BO)
	movapd	%xmm3,   2 * SIZE(BO)
#else
	movapd	%xmm2,   0 * SIZE(AO)
	movapd	%xmm3,   2 * SIZE(AO)
#endif

#ifndef LN
	addq	$4 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 1), BO
#endif

#ifdef LN
	subq	$4, KK
#endif

#ifdef LT
	addq	$4, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif

	decq	I			# i --
	jg	.L91
	ALIGN_4

.L100:
	testq	$2, M
	je	.L110
	ALIGN_4

.L101:
#ifdef LN
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 1), BO
#else
	movq	B, BO
#endif

	movapd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movddup	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movapd	 8 * SIZE(AO), %xmm10
	pxor	%xmm2, %xmm2
	movddup	 4 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L105
	ALIGN_4

.L102:
	mulpd	%xmm9, %xmm8
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AO)
	movddup	 1 * SIZE(BO), %xmm9
	addpd	%xmm8, %xmm0
	mulpd	 2 * SIZE(AO), %xmm9
	movapd	16 * SIZE(AO), %xmm8
	addpd	%xmm9, %xmm1
	movddup	 2 * SIZE(BO), %xmm9
	mulpd	 4 * SIZE(AO), %xmm9
	addpd	%xmm9, %xmm2
	movddup	 3 * SIZE(BO), %xmm9
	mulpd	 6 * SIZE(AO), %xmm9
	addpd	%xmm9, %xmm3
	movddup	 8 * SIZE(BO), %xmm9
	mulpd	%xmm11, %xmm10
	movddup	 5 * SIZE(BO), %xmm11
	addpd	%xmm10, %xmm0
	mulpd	10 * SIZE(AO), %xmm11
	movapd	24 * SIZE(AO), %xmm10
	addpd	%xmm11, %xmm1
	movddup	 6 * SIZE(BO), %xmm11
	mulpd	12 * SIZE(AO), %xmm11
	addpd	%xmm11, %xmm2
	movddup	 7 * SIZE(BO), %xmm11
	mulpd	14 * SIZE(AO), %xmm11
	addpd	%xmm11, %xmm3
	movddup	12 * SIZE(BO), %xmm11

	addq   $16 * SIZE, AO
	addq   $ 8 * SIZE, BO
	decq   %rax
	jne    .L102
	ALIGN_4

.L105:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L109
	ALIGN_4

.L106:
	mulpd	%xmm9, %xmm8
	movddup	 1 * SIZE(BO), %xmm9
	addpd	%xmm8, %xmm0
	movapd	 2 * SIZE(AO), %xmm8

	addq	$2 * SIZE, AO		# aoffset  += 4
	addq	$1 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L106
	ALIGN_4

.L109:
	addpd	%xmm1, %xmm0
	addpd	%xmm3, %xmm2
	addpd	%xmm2, %xmm0

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$2, %rax
#else
	subq	$1, %rax
#endif
	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 1), BO
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(BO), %xmm2
	subpd	%xmm0,  %xmm2
#else
	movapd	 0 * SIZE(AO), %xmm2
	subpd	%xmm0, %xmm2
#endif

#ifdef LN
	movapd	%xmm2, %xmm0
        unpckhpd %xmm0, %xmm0

	movsd	 3 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm0

	movsd	 2 * SIZE(AO), %xmm5
	mulsd	 %xmm0, %xmm5
	subsd	 %xmm5, %xmm2

	movsd	 0 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm2

	unpcklpd %xmm0, %xmm2
#endif

#ifdef LT
	movapd	%xmm2, %xmm0
        unpckhpd %xmm0, %xmm0

	movsd	 0 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm2

	movsd	 1 * SIZE(AO), %xmm5
	mulsd	 %xmm2, %xmm5
	subsd	 %xmm5, %xmm0

	movsd	 3 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm0

	unpcklpd %xmm0, %xmm2
#endif

#ifdef RN
	movddup	 0 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm2
#endif

#ifdef RT
	movddup	 0 * SIZE(BO), %xmm0
	mulpd	 %xmm0, %xmm2
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm2,  0 * SIZE(CO1)
	movhpd	%xmm2,  1 * SIZE(CO1)
#else
	movsd	%xmm2,  0 * SIZE(CO1)
	movhpd	%xmm2,  1 * SIZE(CO1)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm2,   0 * SIZE(BO)
#else
	movapd	%xmm2,   0 * SIZE(AO)
#endif

#ifndef LN
	addq	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 1), BO
#endif

#ifdef LN
	subq	$2, KK
#endif

#ifdef LT
	addq	$2, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L110:
	testq	$1, M
	je	.L119
	ALIGN_4

.L111:
#ifdef LN
       movq	K, %rax
       salq	$0 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 1), BO
#else
	movq	B, BO
#endif

	movapd	 0 * SIZE(AO), %xmm9
	pxor	%xmm0, %xmm0
	movapd	 0 * SIZE(BO), %xmm8
	pxor	%xmm1, %xmm1
	movapd	 4 * SIZE(AO), %xmm11
	pxor	%xmm2, %xmm2
	movapd	 4 * SIZE(BO), %xmm10
	pxor	%xmm3, %xmm3

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	sarq	$3, %rax
	je	.L115
	ALIGN_4

.L112:
	mulpd	%xmm9, %xmm8
	movapd	 2 * SIZE(AO), %xmm9
	addpd	%xmm8, %xmm0
	mulpd	 2 * SIZE(BO), %xmm9
	movapd	 8 * SIZE(BO), %xmm8
	addpd	%xmm9, %xmm1
	movapd	 8 * SIZE(AO), %xmm9
	mulpd	%xmm11, %xmm10
	movapd	 6 * SIZE(AO), %xmm11
	addpd	%xmm10, %xmm0
	mulpd	 6 * SIZE(BO), %xmm11
	movapd	12 * SIZE(BO), %xmm10
	addpd	%xmm11, %xmm1
	movapd	12 * SIZE(AO), %xmm11

	addq   $8 * SIZE, AO
	addq   $8 * SIZE, BO
	decq   %rax
	jne    .L112
	ALIGN_4

.L115:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L118
	ALIGN_4

.L116:
	mulsd	 0 * SIZE(BO), %xmm9
	addsd	%xmm9, %xmm0
	movsd	 1 * SIZE(AO), %xmm9

	addq	$1 * SIZE, AO		# aoffset  += 4
	addq	$1 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L116
	ALIGN_4

.L118:
	addpd	%xmm1, %xmm0
	haddpd	%xmm0, %xmm0

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$1, %rax
#else
	subq	$1, %rax
#endif
	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 1), BO
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(BO), %xmm2
	subsd	%xmm0,  %xmm2
#else
	movsd	 0 * SIZE(AO), %xmm2
	subsd	%xmm0, %xmm2
#endif

#ifdef LN
	movsd	 0 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm2
#endif

#ifdef LT
	movsd	 0 * SIZE(AO), %xmm4
	mulsd	 %xmm4, %xmm2
#endif

#ifdef RN
	movsd	 0 * SIZE(BO), %xmm0
	mulsd	 %xmm0, %xmm2
#endif

#ifdef RT
	movsd	 0 * SIZE(BO), %xmm0
	mulsd	 %xmm0, %xmm2
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm2,  0 * SIZE(CO1)
#else
	movsd	%xmm2,  0 * SIZE(CO1)
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm2,   0 * SIZE(BO)
#else
	movsd	%xmm2,   0 * SIZE(AO)
#endif

#ifndef LN
	addq	$1 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 1), BO
#endif

#ifdef LN
	subq	$1, KK
#endif

#ifdef LT
	addq	$1, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$0 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L119:
#ifdef LN
       leaq	(, K, SIZE), %rax
       leaq	(B, %rax, 1), B
#endif

#if defined(LT) || defined(RN)
	movq	BO, B
#endif

#ifdef RN
	addq	$1, KK
#endif

#ifdef RT
	subq	$1, KK
#endif
	ALIGN_2

.L999:
	movq	  0(%rsp), %rbx
	movq	  8(%rsp), %rbp
	movq	 16(%rsp), %r12
	movq	 24(%rsp), %r13
	movq	 32(%rsp), %r14
	movq	 40(%rsp), %r15

#ifdef WINDOWS_ABI
	movq	 48(%rsp), %rdi
	movq	 56(%rsp), %rsi
	movups	 64(%rsp), %xmm6
	movups	 80(%rsp), %xmm7
	movups	 96(%rsp), %xmm8
	movups	112(%rsp), %xmm9
	movups	128(%rsp), %xmm10
	movups	144(%rsp), %xmm11
	movups	160(%rsp), %xmm12
	movups	176(%rsp), %xmm13
	movups	192(%rsp), %xmm14
	movups	208(%rsp), %xmm15
#endif

	addq	$STACKSIZE, %rsp
	ret

	EPILOGUE
