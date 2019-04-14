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
#define J	%r12
#define AO	%r13
#define BO	%r14
#define	CO1	%r15
#define CO2	%rbx
#define KK	%rbp

#ifndef WINDOWS_ABI

#define STACKSIZE 128

#define OLD_LDC		 8 + STACKSIZE(%rsp)
#define OLD_OFFSET	16 + STACKSIZE(%rsp)

#define OFFSET	 48(%rsp)
#define KKK	 56(%rsp)
#define AORIG    64(%rsp)

#else

#define STACKSIZE 256

#define OLD_A		48 + STACKSIZE(%rsp)
#define OLD_B		56 + STACKSIZE(%rsp)
#define OLD_C		64 + STACKSIZE(%rsp)
#define OLD_LDC		72 + STACKSIZE(%rsp)
#define OLD_OFFSET	80 + STACKSIZE(%rsp)

#define OFFSET	 224(%rsp)
#define KKK	 232(%rsp)
#define AORIG    240(%rsp)
#endif

#define PREFETCH     prefetcht1
#define PREFETCHSIZE (16 * 12 + 3)
#define PREFETCH_R    (4 *  4 + 0)

#ifndef CONJ
#define ADD1	  addpd
#define ADD2	  addpd
#else
#define ADD1	  subpd
#define ADD2	  addpd
#endif

#define KERNEL1(address) \
	mulpd	%xmm8, %xmm9;\
	PREFETCH  (PREFETCHSIZE +  0) * SIZE + (address) * 2 * SIZE(AO);\
	ADD1	%xmm9, %xmm0;\
	movddup	 1 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	ADD2	%xmm9, %xmm1;\
	movddup	 2 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	ADD1	%xmm9, %xmm2;\
	movddup	 3 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	movapd	 2 * SIZE + (address) * 2 * SIZE(AO), %xmm8;\
	ADD2	%xmm9, %xmm3;\
	movddup	 0 * SIZE + (address) * 2 * SIZE(BO), %xmm9

#define KERNEL2(address) \
	mulpd	%xmm8, %xmm9;\
	ADD1	%xmm9, %xmm4;\
	movddup	 1 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	ADD2	%xmm9, %xmm5;\
	movddup	 2 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	ADD1	%xmm9, %xmm6;\
	movddup	 3 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	movapd	 4 * SIZE + (address) * 2 * SIZE(AO), %xmm8;\
	ADD2	%xmm9, %xmm7;\
	movddup	 4 * SIZE + (address) * 2 * SIZE(BO), %xmm9

#define KERNEL3(address) \
	mulpd	%xmm8, %xmm9;\
	ADD1	%xmm9, %xmm0;\
	movddup	 5 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	ADD2	%xmm9, %xmm1;\
	movddup	 6 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	ADD1	%xmm9, %xmm2;\
	movddup	 7 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	movapd	 6 * SIZE + (address) * 2 * SIZE(AO), %xmm8;\
	ADD2	%xmm9, %xmm3;\
	movddup	 4 * SIZE + (address) * 2 * SIZE(BO), %xmm9

#define KERNEL4(address) \
	mulpd	%xmm8, %xmm9;\
	ADD1	%xmm9, %xmm4;\
	movddup	 5 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	ADD2	%xmm9, %xmm5;\
	movddup	 6 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	ADD1	%xmm9, %xmm6;\
	movddup	 7 * SIZE + (address) * 2 * SIZE(BO), %xmm9;\
	mulpd	%xmm8, %xmm9;\
	movapd	32 * SIZE + (address) * 2 * SIZE(AO), %xmm8;\
	ADD2	%xmm9, %xmm7;\
	movddup	32 * SIZE + (address) * 2 * SIZE(BO), %xmm9

#define KERNEL5(address) \
	mulpd	%xmm10, %xmm11;\
	ADD1	%xmm11, %xmm0;\
	movddup	 9 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	ADD2	%xmm11, %xmm1;\
	movddup	10 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	ADD1	%xmm11, %xmm2;\
	movddup	11 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	movapd	10 * SIZE + (address) * 2 * SIZE(AO), %xmm10;\
	ADD2	%xmm11, %xmm3;\
	movddup	 8 * SIZE + (address) * 2 * SIZE(BO), %xmm11

#define KERNEL6(address) \
	mulpd	%xmm10, %xmm11;\
	ADD1	%xmm11, %xmm4;\
	movddup	 9 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	ADD2	%xmm11, %xmm5;\
	movddup	10 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	ADD1	%xmm11, %xmm6;\
	movddup	11 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	movapd	12 * SIZE + (address) * 2 * SIZE(AO), %xmm10;\
	ADD2	%xmm11, %xmm7;\
	movddup	12 * SIZE + (address) * 2 * SIZE(BO), %xmm11

#define KERNEL7(address) \
	mulpd	%xmm10, %xmm11;\
	ADD1	%xmm11, %xmm0;\
	movddup	13 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	ADD2	%xmm11, %xmm1;\
	movddup	14 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	ADD1	%xmm11, %xmm2;\
	movddup	15 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	movapd	14 * SIZE + (address) * 2 * SIZE(AO), %xmm10;\
	ADD2	%xmm11, %xmm3;\
	movddup	12 * SIZE + (address) * 2 * SIZE(BO), %xmm11

#define KERNEL8(address) \
	mulpd	%xmm10, %xmm11;\
	ADD1	%xmm11, %xmm4;\
	movddup	13 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	ADD2	%xmm11, %xmm5;\
	movddup	14 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	ADD1	%xmm11, %xmm6;\
	movddup	15 * SIZE + (address) * 2 * SIZE(BO), %xmm11;\
	mulpd	%xmm10, %xmm11;\
	movapd	40 * SIZE + (address) * 2 * SIZE(AO), %xmm10;\
	ADD2	%xmm11, %xmm7;\
	movddup	40 * SIZE + (address) * 2 * SIZE(BO), %xmm11

#define KERNEL9(address) \
	mulpd	%xmm12, %xmm13;\
	PREFETCH  (PREFETCHSIZE + 16) * SIZE + (address) * 2 * SIZE(AO);\
	ADD1	%xmm13, %xmm0;\
	movddup	17 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	ADD2	%xmm13, %xmm1;\
	movddup	18 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	ADD1	%xmm13, %xmm2;\
	movddup	19 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	movapd	18 * SIZE + (address) * 2 * SIZE(AO), %xmm12;\
	ADD2	%xmm13, %xmm3;\
	movddup	16 * SIZE + (address) * 2 * SIZE(BO), %xmm13

#define KERNEL10(address) \
	mulpd	%xmm12, %xmm13;\
	ADD1	%xmm13, %xmm4;\
	movddup	17 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	ADD2	%xmm13, %xmm5;\
	movddup	18 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	ADD1	%xmm13, %xmm6;\
	movddup	19 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	movapd	20 * SIZE + (address) * 2 * SIZE(AO), %xmm12;\
	ADD2	%xmm13, %xmm7;\
	movddup	20 * SIZE + (address) * 2 * SIZE(BO), %xmm13

#define KERNEL11(address) \
	mulpd	%xmm12, %xmm13;\
	ADD1	%xmm13, %xmm0;\
	movddup	21 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	ADD2	%xmm13, %xmm1;\
	movddup	22 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	ADD1	%xmm13, %xmm2;\
	movddup	23 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	movapd	22 * SIZE + (address) * 2 * SIZE(AO), %xmm12;\
	ADD2	%xmm13, %xmm3;\
	movddup	20 * SIZE + (address) * 2 * SIZE(BO), %xmm13

#define KERNEL12(address) \
	mulpd	%xmm12, %xmm13;\
	ADD1	%xmm13, %xmm4;\
	movddup 21 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	ADD2	%xmm13, %xmm5;\
	movddup	22 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	ADD1	%xmm13, %xmm6;\
	movddup	23 * SIZE + (address) * 2 * SIZE(BO), %xmm13;\
	mulpd	%xmm12, %xmm13;\
	movapd	48 * SIZE + (address) * 2 * SIZE(AO), %xmm12;\
	ADD2	%xmm13, %xmm7;\
	movddup	48 * SIZE + (address) * 2 * SIZE(BO), %xmm13

#define KERNEL13(address) \
	mulpd	%xmm14, %xmm15;\
	ADD1	%xmm15, %xmm0;\
	movddup	25 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	ADD2	%xmm15, %xmm1;\
	movddup	26 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	ADD1	%xmm15, %xmm2;\
	movddup	27 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	movapd	26 * SIZE + (address) * 2 * SIZE(AO), %xmm14;\
	ADD2	%xmm15, %xmm3;\
	movddup	24 * SIZE + (address) * 2 * SIZE(BO), %xmm15

#define KERNEL14(address) \
	mulpd	%xmm14, %xmm15;\
	ADD1	%xmm15, %xmm4;\
	movddup	25 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	ADD2	%xmm15, %xmm5;\
	movddup	26 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	ADD1	%xmm15, %xmm6;\
	movddup	27 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	movapd	28 * SIZE + (address) * 2 * SIZE(AO), %xmm14;\
	ADD2	%xmm15, %xmm7;\
	movddup	28 * SIZE + (address) * 2 * SIZE(BO), %xmm15

#define KERNEL15(address) \
	mulpd	%xmm14, %xmm15;\
	ADD1	%xmm15, %xmm0;\
	movddup	29 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	ADD2	%xmm15, %xmm1;\
	movddup	30 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	ADD1	%xmm15, %xmm2;\
	movddup	31 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	movapd	30 * SIZE + (address) * 2 * SIZE(AO), %xmm14;\
	ADD2	%xmm15, %xmm3;\
	movddup	28 * SIZE + (address) * 2 * SIZE(BO), %xmm15

#define KERNEL16(address) \
	mulpd	%xmm14, %xmm15;\
	ADD1	%xmm15, %xmm4;\
	movddup	29 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	ADD2	%xmm15, %xmm5;\
	movddup	30 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	ADD1	%xmm15, %xmm6;\
	movddup	31 * SIZE + (address) * 2 * SIZE(BO), %xmm15;\
	mulpd	%xmm14, %xmm15;\
	movapd	56 * SIZE + (address) * 2 * SIZE(AO), %xmm14;\
	ADD2	%xmm15, %xmm7;\
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
	movq	OLD_LDC,   LDC
#endif

	movq	OLD_LDC,    LDC
	movq	OLD_OFFSET, KK

	movq	KK, OFFSET

	salq	$ZBASE_SHIFT, LDC

#ifdef LN
       movq	M, %rax
       salq	$ZBASE_SHIFT, %rax
       addq	%rax, C
       imulq	K, %rax
       addq	%rax, A
#endif

#ifdef RT
       movq	N, %rax
       salq	$ZBASE_SHIFT, %rax
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
       movq	N, KK
       subq	OFFSET, KK
#endif

	movq	N,  J
	sarq	$1, J		# j = (n >> 2)
	jle	.L100
	ALIGN_4

.L01:
#if defined(LT) || defined(RN)
	movq	A, AO
#else
	movq	A, AORIG
#endif

#ifdef RT
       movq	K, %rax
       salq	$1 + ZBASE_SHIFT, %rax
       subq	%rax, B

       leaq	(, LDC, 2), %rax
       subq	%rax, C
#endif

	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc

#ifndef RT
	leaq	(C, LDC, 2), C
#endif

#ifdef LN
	movq	OFFSET, KK
	addq	M, KK
#endif
#ifdef LT
	movq	OFFSET, KK
#endif

	testq	$1, M
	jle	.L30

#ifdef LN
       movq	K, %rax
       salq	$0 + ZBASE_SHIFT, %rax
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
	je	.L42

.L41:
	mulpd	%xmm8, %xmm9
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
	ADD1	%xmm9, %xmm0
	movddup	 1 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	ADD2	%xmm9, %xmm1
	movddup	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	ADD1	%xmm9, %xmm2
	movddup	 3 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 2 * SIZE(AO), %xmm8
	ADD2	%xmm9, %xmm3
	movddup	 4 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	ADD1	%xmm9, %xmm0
	movddup	 5 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	ADD2	%xmm9, %xmm1
	movddup	 6 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	ADD1	%xmm9, %xmm2
	movddup	 7 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 4 * SIZE(AO), %xmm8
	ADD2	%xmm9, %xmm3
	movddup	16 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm11
	ADD1	%xmm11, %xmm0
	movddup	 9 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	ADD2	%xmm11, %xmm1
	movddup	10 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	ADD1	%xmm11, %xmm2
	movddup	11 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movapd	 6 * SIZE(AO), %xmm8
	ADD2	%xmm11, %xmm3
	movddup	12 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	ADD1	%xmm11, %xmm0
	movddup	13 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	ADD2	%xmm11, %xmm1
	movddup	14 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	ADD1	%xmm11, %xmm2
	movddup	15 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movapd	16 * SIZE(AO), %xmm8
	ADD2	%xmm11, %xmm3
	movddup	24 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm9
	ADD1	%xmm9, %xmm0
	movddup	17 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	ADD2	%xmm9, %xmm1
	movddup	18 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	ADD1	%xmm9, %xmm2
	movddup	19 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	movapd	10 * SIZE(AO), %xmm10
	ADD2	%xmm9, %xmm3
	movddup	20 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	ADD1	%xmm9, %xmm0
	movddup	21 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	ADD2	%xmm9, %xmm1
	movddup	22 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	ADD1	%xmm9, %xmm2
	movddup	23 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	movapd	12 * SIZE(AO), %xmm10
	ADD2	%xmm9, %xmm3
	movddup	32 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm11
	ADD1	%xmm11, %xmm0
	movddup	25 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	ADD2	%xmm11, %xmm1
	movddup	26 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	ADD1	%xmm11, %xmm2
	movddup	27 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	movapd	14 * SIZE(AO), %xmm10
	ADD2	%xmm11, %xmm3
	movddup	28 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	ADD1	%xmm11, %xmm0
	movddup	29 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	ADD2	%xmm11, %xmm1
	movddup	30 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	ADD1	%xmm11, %xmm2
	movddup	31 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	movapd	24 * SIZE(AO), %xmm10
	ADD2	%xmm11, %xmm3
	movddup	40 * SIZE(BO), %xmm11

	addq   $16 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L41

.L42:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	jle .L44

.L43:
	mulpd	%xmm8, %xmm9
	ADD1	%xmm9, %xmm0
	movddup	 1 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	ADD2	%xmm9, %xmm1
	movddup	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	ADD1	%xmm9, %xmm2
	movddup	 3 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 2 * SIZE(AO), %xmm8
	ADD2	%xmm9, %xmm3
	movddup	 4 * SIZE(BO), %xmm9

	addq	$2 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L43
	ALIGN_4

.L44:
	SHUFPD_1 %xmm1, %xmm1
	SHUFPD_1 %xmm3, %xmm3

#ifndef CONJ
	addsubpd	%xmm1, %xmm0
	addsubpd	%xmm3, %xmm2
#else
	addsubpd	%xmm0, %xmm1
	addsubpd	%xmm2, %xmm3
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$1, %rax
#else
	subq	$2, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(BO), %xmm8
	movapd	 2 * SIZE(BO), %xmm9
#else
	movapd	 0 * SIZE(AO), %xmm8
	movapd	 2 * SIZE(AO), %xmm9
#endif

#if   (defined(LN) || defined(LT)) && !defined(CONJ)
	subpd	%xmm0,  %xmm8
	subpd	%xmm2,  %xmm9
#elif (defined(LN) || defined(LT)) &&  defined(CONJ)
	subpd	%xmm1,  %xmm8
	subpd	%xmm3,  %xmm9
#elif (defined(RN) || defined(RT)) && !defined(CONJ)
	subpd	%xmm0,  %xmm8
	subpd	%xmm2,  %xmm9
#else
	addsubpd %xmm1, %xmm8
	addsubpd %xmm3, %xmm9
#endif

#ifdef CONJ
	pcmpeqb	%xmm7,  %xmm7
	psllq	$63,    %xmm7
#endif

#ifdef LN
	movddup	 0 * SIZE(AO), %xmm4
	movddup	 1 * SIZE(AO), %xmm5

#ifdef CONJ
	xorpd	%xmm7, %xmm5
#endif

	movapd	%xmm8,  %xmm12
	movapd	%xmm9,  %xmm13
	SHUFPD_1 %xmm12, %xmm12
	SHUFPD_1 %xmm13, %xmm13

	mulpd	 %xmm4, %xmm8
	mulpd	 %xmm4, %xmm9
	mulpd	 %xmm5, %xmm12
	mulpd	 %xmm5, %xmm13

	addsubpd %xmm12, %xmm8
	addsubpd %xmm13, %xmm9
#endif

#ifdef LT
	movddup	 0 * SIZE(AO), %xmm0
	movddup	 1 * SIZE(AO), %xmm1

#ifdef CONJ
	xorpd	%xmm7, %xmm1
#endif

	movapd	%xmm8,  %xmm12
	movapd	%xmm9,  %xmm13
	SHUFPD_1 %xmm12, %xmm12
	SHUFPD_1 %xmm13, %xmm13

	mulpd	 %xmm0, %xmm8
	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm12
	mulpd	 %xmm1, %xmm13

	addsubpd %xmm12, %xmm8
	addsubpd %xmm13, %xmm9
#endif

#ifdef RN
	movddup	 0 * SIZE(BO), %xmm0
	movddup	 1 * SIZE(BO), %xmm1
	movddup	 2 * SIZE(BO), %xmm2
	movddup	 3 * SIZE(BO), %xmm3
	movddup	 6 * SIZE(BO), %xmm4
	movddup	 7 * SIZE(BO), %xmm5

#ifdef CONJ
	xorpd	%xmm7, %xmm1
	xorpd	%xmm7, %xmm3
	xorpd	%xmm7, %xmm5
#endif

	movapd	%xmm8,  %xmm12
	SHUFPD_1 %xmm12, %xmm12

	mulpd	 %xmm0, %xmm8
	mulpd	 %xmm1, %xmm12

	addsubpd %xmm12, %xmm8

	movapd	%xmm8,  %xmm12
	movapd	%xmm8,  %xmm13

	SHUFPD_1 %xmm13, %xmm13

	mulpd	%xmm2, %xmm12
	mulpd	%xmm3, %xmm13

	addsubpd %xmm13, %xmm12

	subpd	 %xmm12, %xmm9

	movapd	%xmm9,  %xmm12
	SHUFPD_1 %xmm12, %xmm12

	mulpd	 %xmm4, %xmm9
	mulpd	 %xmm5, %xmm12

	addsubpd %xmm12, %xmm9
#endif


#ifdef RT
	movddup	 6 * SIZE(BO), %xmm0
	movddup	 7 * SIZE(BO), %xmm1
	movddup	 4 * SIZE(BO), %xmm2
	movddup	 5 * SIZE(BO), %xmm3
	movddup	 0 * SIZE(BO), %xmm4
	movddup	 1 * SIZE(BO), %xmm5

#ifdef CONJ
	xorpd	%xmm7, %xmm1
	xorpd	%xmm7, %xmm3
	xorpd	%xmm7, %xmm5
#endif

	movapd	%xmm9,  %xmm12
	SHUFPD_1 %xmm12, %xmm12

	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm12

	addsubpd %xmm12, %xmm9

	movapd	%xmm9,  %xmm12
	movapd	%xmm9,  %xmm13

	SHUFPD_1 %xmm13, %xmm13

	mulpd	%xmm2, %xmm12
	mulpd	%xmm3, %xmm13

	addsubpd %xmm13, %xmm12

	subpd	 %xmm12, %xmm8

	movapd	%xmm8,  %xmm12
	SHUFPD_1 %xmm12, %xmm12

	mulpd	 %xmm4, %xmm8
	mulpd	 %xmm5, %xmm12

	addsubpd %xmm12, %xmm8
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)

	movsd	%xmm9,  0 * SIZE(CO2)
	movhpd	%xmm9,  1 * SIZE(CO2)
#else
	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)

	movsd	%xmm9,  0 * SIZE(CO2)
	movhpd	%xmm9,  1 * SIZE(CO2)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm8,   0 * SIZE(BO)
	movapd	%xmm9,   2 * SIZE(BO)
#else
	movapd	%xmm8,   0 * SIZE(AO)
	movapd	%xmm9,   2 * SIZE(AO)
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
	subq	$1, KK
#endif

#ifdef LT
	addq	$1, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$0 + ZBASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L30:
	movq	M,  I
	sarq	$1, I		# i = (m >> 2)
	jle	.L99
	ALIGN_4

.L10:
#ifdef LN
       movq	K, %rax
       salq	$1 + ZBASE_SHIFT, %rax
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

#ifdef LN
	prefetchnta    -4 * SIZE(CO1)
	prefetchnta    -4 * SIZE(CO2)
#else
	prefetchnta     4 * SIZE(CO1)
	prefetchnta     4 * SIZE(CO2)
#endif
	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$-8, %rax
	salq	$4, %rax
	je	.L12

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
	jle	.L11
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
	jle	.L11
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
	jle	.L11
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
	jle	.L11
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
	jle	.L11
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
	jle	.L11
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
	jle	.L11
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

.L11:
	leaq	(AO, %rax, 2), AO	# * 16
	leaq	(BO, %rax, 2), BO	# * 64
	ALIGN_4

.L12:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L14
	ALIGN_4

.L13:
	mulpd	%xmm8, %xmm9
	movapd	 2 * SIZE(AO), %xmm10
	ADD1	%xmm9, %xmm0
	movddup	 1 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movddup	 0 * SIZE(BO), %xmm11
	ADD2	%xmm9, %xmm1
	movddup	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	ADD1	%xmm9, %xmm2
	movddup	 3 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 4 * SIZE(AO), %xmm8
	ADD2	%xmm9, %xmm3
	movddup	 4 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm11
	ADD1	%xmm11, %xmm4
	movddup	 1 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	ADD2	%xmm11, %xmm5
	movddup	 2 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	ADD1	%xmm11, %xmm6
	movddup	 3 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	ADD2	%xmm11, %xmm7

	addq	$4 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L13
	ALIGN_4

.L14:
	SHUFPD_1 %xmm1, %xmm1
	SHUFPD_1 %xmm3, %xmm3
	SHUFPD_1 %xmm5, %xmm5
	SHUFPD_1 %xmm7, %xmm7

#ifndef CONJ
	addsubpd	%xmm1, %xmm0
	addsubpd	%xmm3, %xmm2
	addsubpd	%xmm5, %xmm4
	addsubpd	%xmm7, %xmm6
#else
	addsubpd	%xmm0, %xmm1
	addsubpd	%xmm2, %xmm3
	addsubpd	%xmm4, %xmm5
	addsubpd	%xmm6, %xmm7
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	subq	$2, %rax

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(BO), %xmm8
	movapd	 2 * SIZE(BO), %xmm9
	movapd	 4 * SIZE(BO), %xmm10
	movapd	 6 * SIZE(BO), %xmm11
#else
	movapd	 0 * SIZE(AO), %xmm8
	movapd	 2 * SIZE(AO), %xmm9
	movapd	 4 * SIZE(AO), %xmm10
	movapd	 6 * SIZE(AO), %xmm11
#endif

#if   (defined(LN) || defined(LT)) && !defined(CONJ)
	subpd	%xmm0,  %xmm8
	subpd	%xmm2,  %xmm9
	subpd	%xmm4,  %xmm10
	subpd	%xmm6,  %xmm11
#elif (defined(LN) || defined(LT)) &&  defined(CONJ)
	subpd	%xmm1,  %xmm8
	subpd	%xmm3,  %xmm9
	subpd	%xmm5,  %xmm10
	subpd	%xmm7,  %xmm11
#elif (defined(RN) || defined(RT)) && !defined(CONJ)
	subpd	%xmm0,  %xmm8
	subpd	%xmm4,  %xmm9
	subpd	%xmm2,  %xmm10
	subpd	%xmm6,  %xmm11
#else
	addsubpd %xmm1,  %xmm8
	addsubpd %xmm5,  %xmm9
	addsubpd %xmm3,  %xmm10
	addsubpd %xmm7,  %xmm11
#endif

#ifdef CONJ
	pcmpeqb	%xmm7,  %xmm7
	psllq	$63,    %xmm7
#endif

#if defined(LN) || defined(RT)
#ifdef LN
	movddup	 6 * SIZE(AO), %xmm0
	movddup	 7 * SIZE(AO), %xmm1
	movddup	 4 * SIZE(AO), %xmm2
	movddup	 5 * SIZE(AO), %xmm3
	movddup	 0 * SIZE(AO), %xmm4
	movddup	 1 * SIZE(AO), %xmm5
#else
	movddup	 6 * SIZE(BO), %xmm0
	movddup	 7 * SIZE(BO), %xmm1
	movddup	 4 * SIZE(BO), %xmm2
	movddup	 5 * SIZE(BO), %xmm3
	movddup	 0 * SIZE(BO), %xmm4
	movddup	 1 * SIZE(BO), %xmm5
#endif

#ifdef CONJ
	xorpd	%xmm7, %xmm1
	xorpd	%xmm7, %xmm3
	xorpd	%xmm7, %xmm5
#endif

	movapd	%xmm10,  %xmm12
	movapd	%xmm11,  %xmm13
	SHUFPD_1 %xmm12, %xmm12
	SHUFPD_1 %xmm13, %xmm13

	mulpd	 %xmm0, %xmm10
	mulpd	 %xmm0, %xmm11
	mulpd	 %xmm1, %xmm12
	mulpd	 %xmm1, %xmm13

	addsubpd %xmm12, %xmm10
	addsubpd %xmm13, %xmm11

	movapd	%xmm10,  %xmm12
	movapd	%xmm10,  %xmm13
	movapd	%xmm11,  %xmm14
	movapd	%xmm11,  %xmm15

	SHUFPD_1 %xmm13, %xmm13
	SHUFPD_1 %xmm15, %xmm15

	mulpd	%xmm2, %xmm12
	mulpd	%xmm2, %xmm14
	mulpd	%xmm3, %xmm13
	mulpd	%xmm3, %xmm15

	addsubpd %xmm13, %xmm12
	addsubpd %xmm15, %xmm14

	subpd	 %xmm12, %xmm8
	subpd	 %xmm14, %xmm9

	movapd	%xmm8,  %xmm12
	movapd	%xmm9,  %xmm13
	SHUFPD_1 %xmm12, %xmm12
	SHUFPD_1 %xmm13, %xmm13

	mulpd	 %xmm4, %xmm8
	mulpd	 %xmm4, %xmm9
	mulpd	 %xmm5, %xmm12
	mulpd	 %xmm5, %xmm13

	addsubpd %xmm12, %xmm8
	addsubpd %xmm13, %xmm9
#endif

#if defined(LT) || defined(RN)

#ifdef LT
	movddup	 0 * SIZE(AO), %xmm0
	movddup	 1 * SIZE(AO), %xmm1
	movddup	 2 * SIZE(AO), %xmm2
	movddup	 3 * SIZE(AO), %xmm3
	movddup	 6 * SIZE(AO), %xmm4
	movddup	 7 * SIZE(AO), %xmm5
#else
	movddup	 0 * SIZE(BO), %xmm0
	movddup	 1 * SIZE(BO), %xmm1
	movddup	 2 * SIZE(BO), %xmm2
	movddup	 3 * SIZE(BO), %xmm3
	movddup	 6 * SIZE(BO), %xmm4
	movddup	 7 * SIZE(BO), %xmm5
#endif

#ifdef CONJ
	xorpd	%xmm7, %xmm1
	xorpd	%xmm7, %xmm3
	xorpd	%xmm7, %xmm5
#endif

	movapd	%xmm8,  %xmm12
	movapd	%xmm9,  %xmm13
	SHUFPD_1 %xmm12, %xmm12
	SHUFPD_1 %xmm13, %xmm13

	mulpd	 %xmm0, %xmm8
	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm12
	mulpd	 %xmm1, %xmm13

	addsubpd %xmm12, %xmm8
	addsubpd %xmm13, %xmm9

	movapd	%xmm8,  %xmm12
	movapd	%xmm8,  %xmm13
	movapd	%xmm9,  %xmm14
	movapd	%xmm9,  %xmm15

	SHUFPD_1 %xmm13, %xmm13
	SHUFPD_1 %xmm15, %xmm15

	mulpd	%xmm2, %xmm12
	mulpd	%xmm2, %xmm14
	mulpd	%xmm3, %xmm13
	mulpd	%xmm3, %xmm15

	addsubpd %xmm13, %xmm12
	addsubpd %xmm15, %xmm14

	subpd	 %xmm12, %xmm10
	subpd	 %xmm14, %xmm11

	movapd	%xmm10,  %xmm12
	movapd	%xmm11,  %xmm13
	SHUFPD_1 %xmm12, %xmm12
	SHUFPD_1 %xmm13, %xmm13

	mulpd	 %xmm4, %xmm10
	mulpd	 %xmm4, %xmm11
	mulpd	 %xmm5, %xmm12
	mulpd	 %xmm5, %xmm13

	addsubpd %xmm12, %xmm10
	addsubpd %xmm13, %xmm11
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm10, 2 * SIZE(CO1)
	movhpd	%xmm10, 3 * SIZE(CO1)

	movsd	%xmm9,   0 * SIZE(CO2)
	movhpd	%xmm9,   1 * SIZE(CO2)
	movsd	%xmm11,  2 * SIZE(CO2)
	movhpd	%xmm11,  3 * SIZE(CO2)
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
	movapd	%xmm8,   0 * SIZE(BO)
	movapd	%xmm9,   2 * SIZE(BO)
	movapd	%xmm10,  4 * SIZE(BO)
	movapd	%xmm11,  6 * SIZE(BO)
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
       salq	$1 + ZBASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	decq	I			# i --
	jg	.L10
	ALIGN_4

.L99:
#ifdef LN
       leaq	(, K, SIZE), %rax
       leaq	(B, %rax, 4), B
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
	decq	J			# j --
	jg	.L01

.L100:
	testq	$1, N
	jle	.L999

.L101:
#if defined(LT) || defined(RN)
	movq	A, AO
#else
	movq	A, AORIG
#endif

#ifdef RT
       movq	K, %rax
       salq	$0 + ZBASE_SHIFT, %rax
       subq	%rax, B

       subq	LDC, C
#endif
	movq	C, CO1		# coffset1 = c
#ifndef RT
	addq	LDC, C
#endif

#ifdef LN
	movq	OFFSET, KK
	addq	M, KK
#endif
#ifdef LT
	movq	OFFSET, KK
#endif

	testq	$1, M
	jle	.L130
	ALIGN_4

#ifdef LN
       movq	K, %rax
       salq	$0 + ZBASE_SHIFT, %rax
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
	je	.L142

.L141:
	mulpd	%xmm8, %xmm9
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AO)
	ADD1	%xmm9, %xmm0
	movddup	 1 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 2 * SIZE(AO), %xmm8
	ADD2	%xmm9, %xmm1
	movddup	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	ADD1	%xmm9, %xmm2
	movddup	 3 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 4 * SIZE(AO), %xmm8
	ADD2	%xmm9, %xmm3
	movddup	 4 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	ADD1	%xmm9, %xmm0
	movddup	 5 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 6 * SIZE(AO), %xmm8
	ADD2	%xmm9, %xmm1
	movddup	 6 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	ADD1	%xmm9, %xmm2
	movddup	 7 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	16 * SIZE(AO), %xmm8
	ADD2	%xmm9, %xmm3
	movddup	16 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm11
	ADD1	%xmm11, %xmm0
	movddup	 9 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	movapd	10 * SIZE(AO), %xmm10
	ADD2	%xmm11, %xmm1
	movddup	10 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	ADD1	%xmm11, %xmm2
	movddup	11 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	movapd	12 * SIZE(AO), %xmm10
	ADD2	%xmm11, %xmm3
	movddup	12 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	ADD1	%xmm11, %xmm0
	movddup	13 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	movapd	14 * SIZE(AO), %xmm10
	ADD2	%xmm11, %xmm1
	movddup	14 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	ADD1	%xmm11, %xmm2
	movddup	15 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	movapd	24 * SIZE(AO), %xmm10
	ADD2	%xmm11, %xmm3
	movddup	24 * SIZE(BO), %xmm11

	addq   $16 * SIZE, AO
	addq   $16 * SIZE, BO
	decq   %rax
	jne    .L141

.L142:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	jle .L144

.L143:
	mulpd	%xmm8, %xmm9
	ADD1	%xmm9, %xmm0
	movddup	 1 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 2 * SIZE(AO), %xmm8
	ADD2	%xmm9, %xmm1
	movddup	 2 * SIZE(BO), %xmm9

	addq	$2 * SIZE, AO		# aoffset  += 4
	addq	$2 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L143
	ALIGN_4

.L144:
	addpd	%xmm2, %xmm0
	addpd	%xmm3, %xmm1

	SHUFPD_1 %xmm1, %xmm1

#ifndef CONJ
	addsubpd	%xmm1, %xmm0
#else
	addsubpd	%xmm0, %xmm1
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	subq	$1, %rax

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 2), BO
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(BO), %xmm8
#else
	movapd	 0 * SIZE(AO), %xmm8
#endif

#if   (defined(LN) || defined(LT)) && !defined(CONJ)
	subpd	%xmm0,  %xmm8
#elif (defined(LN) || defined(LT)) &&  defined(CONJ)
	subpd	%xmm1,  %xmm8
#elif (defined(RN) || defined(RT)) && !defined(CONJ)
	subpd	%xmm0,  %xmm8
#else
	addsubpd %xmm1, %xmm8
#endif

#ifdef CONJ
	pcmpeqb	%xmm7,  %xmm7
	psllq	$63,    %xmm7
#endif

#ifdef LN
	movddup	 0 * SIZE(AO), %xmm4
	movddup	 1 * SIZE(AO), %xmm5

#ifdef CONJ
	xorpd	%xmm7, %xmm5
#endif

	movapd	%xmm8,  %xmm12
	SHUFPD_1 %xmm12, %xmm12
	mulpd	 %xmm4, %xmm8
	mulpd	 %xmm5, %xmm12
	addsubpd %xmm12, %xmm8
#endif

#ifdef LT
	movddup	 0 * SIZE(AO), %xmm0
	movddup	 1 * SIZE(AO), %xmm1

#ifdef CONJ
	xorpd	%xmm7, %xmm1
#endif

	movapd	%xmm8,  %xmm12
	SHUFPD_1 %xmm12, %xmm12
	mulpd	 %xmm0, %xmm8
	mulpd	 %xmm1, %xmm12
	addsubpd %xmm12, %xmm8
#endif

#ifdef RN
	movddup	 0 * SIZE(BO), %xmm0
	movddup	 1 * SIZE(BO), %xmm1

#ifdef CONJ
	xorpd	%xmm7, %xmm1
#endif

	movapd	%xmm8,  %xmm12
	SHUFPD_1 %xmm12, %xmm12
	mulpd	 %xmm0, %xmm8
	mulpd	 %xmm1, %xmm12

	addsubpd %xmm12, %xmm8
#endif

#ifdef RT
	movddup	 0 * SIZE(BO), %xmm4
	movddup	 1 * SIZE(BO), %xmm5

#ifdef CONJ
	xorpd	%xmm7, %xmm5
#endif

	movapd	%xmm8,  %xmm12
	SHUFPD_1 %xmm12, %xmm12
	mulpd	 %xmm4, %xmm8
	mulpd	 %xmm5, %xmm12

	addsubpd %xmm12, %xmm8
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
#else
	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm8,   0 * SIZE(BO)
#else
	movapd	%xmm8,   0 * SIZE(AO)
#endif

#ifndef LN
	addq	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
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
       salq	$0 + ZBASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L130:
	movq	M,  I
	sarq	$1, I		# i = (m >> 2)
	jle	.L149
	ALIGN_4

.L110:
#ifdef LN
       movq	K, %rax
       salq	$1 + ZBASE_SHIFT, %rax
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

	movapd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movddup	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movapd	 8 * SIZE(AO), %xmm10
	pxor	%xmm4, %xmm4
	movddup	 8 * SIZE(BO), %xmm11
	pxor	%xmm5, %xmm5

#ifdef LN
	prefetchnta    -4 * SIZE(CO1)
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
	je	.L112

.L111:
	mulpd	%xmm8, %xmm9
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
	ADD1	%xmm9, %xmm0
	movddup	 1 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 2 * SIZE(AO), %xmm8
	ADD2	%xmm9, %xmm1
	movddup	 0 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	ADD1	%xmm9, %xmm4
	movddup	 1 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 4 * SIZE(AO), %xmm8
	ADD2	%xmm9, %xmm5
	movddup	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	ADD1	%xmm9, %xmm0
	movddup	 3 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	 6 * SIZE(AO), %xmm8
	ADD2	%xmm9, %xmm1
	movddup	 2 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	ADD1	%xmm9, %xmm4
	movddup	 3 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movapd	16 * SIZE(AO), %xmm8
	ADD2	%xmm9, %xmm5
	movddup	 4 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	ADD1	%xmm9, %xmm0
	movddup	 5 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	movapd	10 * SIZE(AO), %xmm10
	ADD2	%xmm9, %xmm1
	movddup	 4 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	ADD1	%xmm9, %xmm4
	movddup	 5 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	movapd	12 * SIZE(AO), %xmm10
	ADD2	%xmm9, %xmm5
	movddup	 6 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	ADD1	%xmm9, %xmm0
	movddup	 7 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	movapd	14 * SIZE(AO), %xmm10
	ADD2	%xmm9, %xmm1
	movddup	 6 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	ADD1	%xmm9, %xmm4
	movddup	 7 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm9
	movapd	40 * SIZE(AO), %xmm10
	ADD2	%xmm9, %xmm5
	movddup	16 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm11
	PREFETCH  (PREFETCHSIZE + 16) * SIZE(AO)
	ADD1	%xmm11, %xmm0
	movddup	 9 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movapd	18 * SIZE(AO), %xmm8
	ADD2	%xmm11, %xmm1
	movddup	 8 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	ADD1	%xmm11, %xmm4
	movddup	 9 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movapd	20 * SIZE(AO), %xmm8
	ADD2	%xmm11, %xmm5
	movddup	10 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	ADD1	%xmm11, %xmm0
	movddup	11 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movapd	22 * SIZE(AO), %xmm8
	ADD2	%xmm11, %xmm1
	movddup	10 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	ADD1	%xmm11, %xmm4
	movddup	11 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movapd	24 * SIZE(AO), %xmm8
	ADD2	%xmm11, %xmm5
	movddup	12 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	ADD1	%xmm11, %xmm0
	movddup	13 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movapd	26 * SIZE(AO), %xmm8
	ADD2	%xmm11, %xmm1
	movddup	12 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	ADD1	%xmm11, %xmm4
	movddup	13 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movapd	28 * SIZE(AO), %xmm8
	ADD2	%xmm11, %xmm5
	movddup	14 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	ADD1	%xmm11, %xmm0
	movddup	15 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movapd	30 * SIZE(AO), %xmm8
	ADD2	%xmm11, %xmm1
	movddup	14 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	ADD1	%xmm11, %xmm4
	movddup	15 * SIZE(BO), %xmm11
	mulpd	%xmm8, %xmm11
	movapd	32 * SIZE(AO), %xmm8
	ADD2	%xmm11, %xmm5
	movddup	24 * SIZE(BO), %xmm11

	addq   $32 * SIZE, AO
	addq   $16 * SIZE, BO
	decq   %rax
	jne    .L111
	ALIGN_4

.L112:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	andq	$7, %rax		# if (k & 1)
	BRANCH
	jle .L114

.L113:
	mulpd	%xmm8, %xmm9
	movapd	 2 * SIZE(AO), %xmm10
	ADD1	%xmm9, %xmm0
	movddup	 1 * SIZE(BO), %xmm9
	mulpd	%xmm8, %xmm9
	movddup	 0 * SIZE(BO), %xmm11
	ADD2	%xmm9, %xmm1
	movddup	 2 * SIZE(BO), %xmm9
	mulpd	%xmm10, %xmm11
	movapd	 4 * SIZE(AO), %xmm8
	ADD1	%xmm11, %xmm4
	movddup	 1 * SIZE(BO), %xmm11
	mulpd	%xmm10, %xmm11
	ADD2	%xmm11, %xmm5

	addq	$4 * SIZE, AO		# aoffset  += 4
	addq	$2 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L113
	ALIGN_4

.L114:
	SHUFPD_1 %xmm1, %xmm1
	SHUFPD_1 %xmm5, %xmm5

#ifndef CONJ
	addsubpd	%xmm1, %xmm0
	addsubpd	%xmm5, %xmm4
#else
	addsubpd	%xmm0, %xmm1
	addsubpd	%xmm4, %xmm5
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$2, %rax
#else
	subq	$1, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 2), BO
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(BO), %xmm8
	movapd	 2 * SIZE(BO), %xmm9
#else
	movapd	 0 * SIZE(AO), %xmm8
	movapd	 2 * SIZE(AO), %xmm9
#endif

#if   (defined(LN) || defined(LT)) && !defined(CONJ)
	subpd	%xmm0,  %xmm8
	subpd	%xmm4,  %xmm9
#elif (defined(LN) || defined(LT)) &&  defined(CONJ)
	subpd	%xmm1,  %xmm8
	subpd	%xmm5,  %xmm9
#elif (defined(RN) || defined(RT)) && !defined(CONJ)
	subpd	%xmm0,  %xmm8
	subpd	%xmm4,  %xmm9
#else
	addsubpd %xmm1, %xmm8
	addsubpd %xmm5, %xmm9
#endif

#ifdef CONJ
	pcmpeqb	%xmm7,  %xmm7
	psllq	$63,    %xmm7
#endif

#ifdef LN
	movddup	 6 * SIZE(AO), %xmm0
	movddup	 7 * SIZE(AO), %xmm1
	movddup	 4 * SIZE(AO), %xmm2
	movddup	 5 * SIZE(AO), %xmm3
	movddup	 0 * SIZE(AO), %xmm4
	movddup	 1 * SIZE(AO), %xmm5

#ifdef CONJ
	xorpd	%xmm7, %xmm1
	xorpd	%xmm7, %xmm3
	xorpd	%xmm7, %xmm5
#endif

	movapd	%xmm9,  %xmm12
	SHUFPD_1 %xmm12, %xmm12
	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm12
	addsubpd %xmm12, %xmm9
	movapd	%xmm9,  %xmm12
	movapd	%xmm9,  %xmm13
	SHUFPD_1 %xmm13, %xmm13
	mulpd	%xmm2, %xmm12
	mulpd	%xmm3, %xmm13
	addsubpd %xmm13, %xmm12
	subpd	 %xmm12, %xmm8
	movapd	%xmm8,  %xmm12
	SHUFPD_1 %xmm12, %xmm12
	mulpd	 %xmm4, %xmm8
	mulpd	 %xmm5, %xmm12
	addsubpd %xmm12, %xmm8
#endif

#ifdef LT
	movddup	 0 * SIZE(AO), %xmm0
	movddup	 1 * SIZE(AO), %xmm1
	movddup	 2 * SIZE(AO), %xmm2
	movddup	 3 * SIZE(AO), %xmm3
	movddup	 6 * SIZE(AO), %xmm4
	movddup	 7 * SIZE(AO), %xmm5

#ifdef CONJ
	xorpd	%xmm7, %xmm1
	xorpd	%xmm7, %xmm3
	xorpd	%xmm7, %xmm5
#endif

	movapd	%xmm8,  %xmm12
	SHUFPD_1 %xmm12, %xmm12
	mulpd	 %xmm0, %xmm8
	mulpd	 %xmm1, %xmm12
	addsubpd %xmm12, %xmm8
	movapd	%xmm8,  %xmm12
	movapd	%xmm8,  %xmm13
	SHUFPD_1 %xmm13, %xmm13
	mulpd	%xmm2, %xmm12
	mulpd	%xmm3, %xmm13
	addsubpd %xmm13, %xmm12
	subpd	 %xmm12, %xmm9
	movapd	%xmm9,  %xmm12
	SHUFPD_1 %xmm12, %xmm12
	mulpd	 %xmm4, %xmm9
	mulpd	 %xmm5, %xmm12
	addsubpd %xmm12, %xmm9
#endif

#ifdef RN
	movddup	 0 * SIZE(BO), %xmm0
	movddup	 1 * SIZE(BO), %xmm1

#ifdef CONJ
	xorpd	%xmm7, %xmm1
#endif

	movapd	%xmm8,  %xmm12
	movapd	%xmm9,  %xmm13
	SHUFPD_1 %xmm12, %xmm12
	SHUFPD_1 %xmm13, %xmm13

	mulpd	 %xmm0, %xmm8
	mulpd	 %xmm0, %xmm9
	mulpd	 %xmm1, %xmm12
	mulpd	 %xmm1, %xmm13

	addsubpd %xmm12, %xmm8
	addsubpd %xmm13, %xmm9
#endif

#ifdef RT
	movddup	 0 * SIZE(BO), %xmm4
	movddup	 1 * SIZE(BO), %xmm5

#ifdef CONJ
	xorpd	%xmm7, %xmm5
#endif

	movapd	%xmm8,  %xmm12
	movapd	%xmm9,  %xmm13
	SHUFPD_1 %xmm12, %xmm12
	SHUFPD_1 %xmm13, %xmm13

	mulpd	 %xmm4, %xmm8
	mulpd	 %xmm4, %xmm9
	mulpd	 %xmm5, %xmm12
	mulpd	 %xmm5, %xmm13

	addsubpd %xmm12, %xmm8
	addsubpd %xmm13, %xmm9
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm9,  2 * SIZE(CO1)
	movhpd	%xmm9,  3 * SIZE(CO1)
#else
	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm9,  2 * SIZE(CO1)
	movhpd	%xmm9,  3 * SIZE(CO1)
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm8,   0 * SIZE(BO)
	movapd	%xmm9,   2 * SIZE(BO)
#else
	movapd	%xmm8,   0 * SIZE(AO)
	movapd	%xmm9,   2 * SIZE(AO)
#endif

#ifndef LN
	addq	$4 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
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
       salq	$1 + ZBASE_SHIFT, %rax
       addq	%rax, AORIG
#endif

	decq	I			# i --
	jg	.L110
	ALIGN_4

.L149:
#ifdef LN
       leaq	(, K, SIZE), %rax
       leaq	(B, %rax, 2), B
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
	ALIGN_3

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
