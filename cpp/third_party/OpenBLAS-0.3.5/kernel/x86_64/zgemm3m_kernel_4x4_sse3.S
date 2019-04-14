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
#define BB	%rbp

#ifndef WINDOWS_ABI

#define STACKSIZE 128

#define OLD_LDC		 8 + STACKSIZE(%rsp)
#define OLD_OFFSET	16 + STACKSIZE(%rsp)

#define ALPHA_R	 48(%rsp)
#define ALPHA_I	 56(%rsp)
#define OFFSET	 64(%rsp)
#define KKK	 72(%rsp)
#define KK	 80(%rsp)

#else

#define STACKSIZE 512

#define OLD_ALPHA_I	40 + STACKSIZE(%rsp)
#define OLD_A		48 + STACKSIZE(%rsp)
#define OLD_B		56 + STACKSIZE(%rsp)
#define OLD_C		64 + STACKSIZE(%rsp)
#define OLD_LDC		72 + STACKSIZE(%rsp)
#define OLD_OFFSET	80 + STACKSIZE(%rsp)

#define ALPHA_R	 224(%rsp)
#define ALPHA_I	 232(%rsp)
#define OFFSET	 240(%rsp)
#define KK	 248(%rsp)
#define KKK	 256(%rsp)

#endif

#define PREFETCH     prefetcht2
#define PREFETCHSIZE (16 * 12 + 3)

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

#if defined(OS_LINUX) && defined(CORE_BARCELONA)
	.align 32768
#endif
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

	movaps	%xmm3, %xmm0
	movsd	OLD_ALPHA_I, %xmm1
#else
	movq	OLD_LDC,   LDC
#endif

	movsd	 %xmm0, ALPHA_R
	movsd	 %xmm1, ALPHA_I

	salq	$ZBASE_SHIFT, LDC

	movq	N,  J
	sarq	$2, J		# j = (n >> 2)
	jle	.L40
	ALIGN_4

.L10:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc
	movq	A, AO		# aoffset = a

	leaq	(, K, 4), BB
	leaq	(B, BB, SIZE), BB

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L20
	ALIGN_4

.L11:
	prefetcht0	0 * SIZE(BB)
	prefetcht0	8 * SIZE(BB)
	subq	   $-8 * SIZE, BB

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 4), BO
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
	pxor	%xmm4, %xmm4
	movddup 16 * SIZE(BO), %xmm13
	pxor	%xmm5, %xmm5
	movapd	24 * SIZE(AO), %xmm14
	pxor	%xmm6, %xmm6
	movddup	24 * SIZE(BO), %xmm15
	pxor	%xmm7, %xmm7

	prefetchnta     7 * SIZE(CO1)
	prefetchnta     7 * SIZE(CO2)
	prefetchnta     7 * SIZE(CO1, LDC, 2)
	prefetchnta     7 * SIZE(CO2, LDC, 2)

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$4, %rax
#else
	addq	$4, %rax
#endif
	movq	%rax, KKK
#endif

#if 1
	andq	$-8, %rax
	salq	$4, %rax
	NOBRANCH
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
	BRANCH
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
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movsd	ALPHA_R, %xmm15
	movhpd	ALPHA_I, %xmm15
	andq	$7, %rax		# if (k & 1)
	BRANCH
	BRANCH
	je	.L19
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
	BRANCH
	jg	.L16
	ALIGN_4

.L19:
	movsd	0 * SIZE(CO1), %xmm8
	movhpd	1 * SIZE(CO1), %xmm8
	movsd	2 * SIZE(CO1), %xmm9
	movhpd	3 * SIZE(CO1), %xmm9

	movsd	4 * SIZE(CO1), %xmm10
	movhpd	5 * SIZE(CO1), %xmm10
	movsd	6 * SIZE(CO1), %xmm11
	movhpd	7 * SIZE(CO1), %xmm11

	movddup	 %xmm0, %xmm12
	unpckhpd %xmm0, %xmm0
	movddup	 %xmm4, %xmm13
	unpckhpd %xmm4, %xmm4

	mulpd	 %xmm15, %xmm12
	mulpd	 %xmm15, %xmm0
	mulpd	 %xmm15, %xmm13
	mulpd	 %xmm15, %xmm4

	addpd	 %xmm12, %xmm8
	addpd	 %xmm0,  %xmm9
	addpd	 %xmm13, %xmm10
	addpd	 %xmm4,  %xmm11

	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm9,  2 * SIZE(CO1)
	movhpd	%xmm9,  3 * SIZE(CO1)

	movsd	%xmm10, 4 * SIZE(CO1)
	movhpd	%xmm10, 5 * SIZE(CO1)
	movsd	%xmm11, 6 * SIZE(CO1)
	movhpd	%xmm11, 7 * SIZE(CO1)

	movsd	0 * SIZE(CO2), %xmm8
	movhpd	1 * SIZE(CO2), %xmm8
	movsd	2 * SIZE(CO2), %xmm9
	movhpd	3 * SIZE(CO2), %xmm9

	movsd	4 * SIZE(CO2), %xmm10
	movhpd	5 * SIZE(CO2), %xmm10
	movsd	6 * SIZE(CO2), %xmm11
	movhpd	7 * SIZE(CO2), %xmm11

	movddup	 %xmm1, %xmm12
	unpckhpd %xmm1, %xmm1
	movddup	 %xmm5, %xmm13
	unpckhpd %xmm5, %xmm5

	mulpd	 %xmm15, %xmm12
	mulpd	 %xmm15, %xmm1
	mulpd	 %xmm15, %xmm13
	mulpd	 %xmm15, %xmm5

	addpd	 %xmm12, %xmm8
	addpd	 %xmm1,  %xmm9
	addpd	 %xmm13, %xmm10
	addpd	 %xmm5,  %xmm11

	movsd	%xmm8,  0 * SIZE(CO2)
	movhpd	%xmm8,  1 * SIZE(CO2)
	movsd	%xmm9,  2 * SIZE(CO2)
	movhpd	%xmm9,  3 * SIZE(CO2)

	movsd	%xmm10, 4 * SIZE(CO2)
	movhpd	%xmm10, 5 * SIZE(CO2)
	movsd	%xmm11, 6 * SIZE(CO2)
	movhpd	%xmm11, 7 * SIZE(CO2)

	movsd	0 * SIZE(CO1, LDC, 2), %xmm8
	movhpd	1 * SIZE(CO1, LDC, 2), %xmm8
	movsd	2 * SIZE(CO1, LDC, 2), %xmm9
	movhpd	3 * SIZE(CO1, LDC, 2), %xmm9

	movsd	4 * SIZE(CO1, LDC, 2), %xmm10
	movhpd	5 * SIZE(CO1, LDC, 2), %xmm10
	movsd	6 * SIZE(CO1, LDC, 2), %xmm11
	movhpd	7 * SIZE(CO1, LDC, 2), %xmm11

	movddup	 %xmm2, %xmm12
	unpckhpd %xmm2, %xmm2
	movddup	 %xmm6, %xmm13
	unpckhpd %xmm6, %xmm6

	mulpd	 %xmm15, %xmm12
	mulpd	 %xmm15, %xmm2
	mulpd	 %xmm15, %xmm13
	mulpd	 %xmm15, %xmm6

	addpd	 %xmm12, %xmm8
	addpd	 %xmm2,  %xmm9
	addpd	 %xmm13, %xmm10
	addpd	 %xmm6,  %xmm11

	movsd	%xmm8,  0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm8,  1 * SIZE(CO1, LDC, 2)
	movsd	%xmm9,  2 * SIZE(CO1, LDC, 2)
	movhpd	%xmm9,  3 * SIZE(CO1, LDC, 2)

	movsd	%xmm10, 4 * SIZE(CO1, LDC, 2)
	movhpd	%xmm10, 5 * SIZE(CO1, LDC, 2)
	movsd	%xmm11, 6 * SIZE(CO1, LDC, 2)
	movhpd	%xmm11, 7 * SIZE(CO1, LDC, 2)

	movsd	0 * SIZE(CO2, LDC, 2), %xmm8
	movhpd	1 * SIZE(CO2, LDC, 2), %xmm8
	movsd	2 * SIZE(CO2, LDC, 2), %xmm9
	movhpd	3 * SIZE(CO2, LDC, 2), %xmm9

	movsd	4 * SIZE(CO2, LDC, 2), %xmm10
	movhpd	5 * SIZE(CO2, LDC, 2), %xmm10
	movsd	6 * SIZE(CO2, LDC, 2), %xmm11
	movhpd	7 * SIZE(CO2, LDC, 2), %xmm11

	movddup	 %xmm3, %xmm12
	unpckhpd %xmm3, %xmm3
	movddup	 %xmm7, %xmm13
	unpckhpd %xmm7, %xmm7

	mulpd	 %xmm15, %xmm12
	mulpd	 %xmm15, %xmm3
	mulpd	 %xmm15, %xmm13
	mulpd	 %xmm15, %xmm7

	addpd	 %xmm12, %xmm8
	addpd	 %xmm3,  %xmm9
	addpd	 %xmm13, %xmm10
	addpd	 %xmm7,  %xmm11

	movsd	%xmm8,  0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm8,  1 * SIZE(CO2, LDC, 2)
	movsd	%xmm9,  2 * SIZE(CO2, LDC, 2)
	movhpd	%xmm9,  3 * SIZE(CO2, LDC, 2)

	movsd	%xmm10, 4 * SIZE(CO2, LDC, 2)
	movhpd	%xmm10, 5 * SIZE(CO2, LDC, 2)
	movsd	%xmm11, 6 * SIZE(CO2, LDC, 2)
	movhpd	%xmm11, 7 * SIZE(CO2, LDC, 2)

	addq	$8 * SIZE, CO1		# coffset += 4
	addq	$8 * SIZE, CO2		# coffset += 4

	decq	I			# i --
	jg	.L11
	jmp	.L20
	ALIGN_4

.L20:
	testq	$2, M
	BRANCH
	je	.L30
	ALIGN_4

.L21:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 4), BO
#endif

	movapd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movddup	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movapd	 8 * SIZE(AO), %xmm10
	pxor	%xmm2, %xmm2
	movddup	 8 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$2, %rax
#else
	addq	$4, %rax
#endif
	movq	%rax, KKK
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
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movsd	ALPHA_R, %xmm15
	movhpd	ALPHA_I, %xmm15
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
	movsd	0 * SIZE(CO1), %xmm8
	movhpd	1 * SIZE(CO1), %xmm8
	movsd	2 * SIZE(CO1), %xmm9
	movhpd	3 * SIZE(CO1), %xmm9

	movddup	 %xmm0, %xmm12
	unpckhpd %xmm0, %xmm0

	mulpd	 %xmm15, %xmm12
	mulpd	 %xmm15, %xmm0
	addpd	 %xmm12, %xmm8
	addpd	 %xmm0,  %xmm9

	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm9,  2 * SIZE(CO1)
	movhpd	%xmm9,  3 * SIZE(CO1)

	movsd	0 * SIZE(CO2), %xmm8
	movhpd	1 * SIZE(CO2), %xmm8
	movsd	2 * SIZE(CO2), %xmm9
	movhpd	3 * SIZE(CO2), %xmm9

	movddup	 %xmm1, %xmm12
	unpckhpd %xmm1, %xmm1

	mulpd	 %xmm15, %xmm12
	mulpd	 %xmm15, %xmm1
	addpd	 %xmm12, %xmm8
	addpd	 %xmm1,  %xmm9

	movsd	%xmm8,  0 * SIZE(CO2)
	movhpd	%xmm8,  1 * SIZE(CO2)
	movsd	%xmm9,  2 * SIZE(CO2)
	movhpd	%xmm9,  3 * SIZE(CO2)

	movsd	0 * SIZE(CO1, LDC, 2), %xmm8
	movhpd	1 * SIZE(CO1, LDC, 2), %xmm8
	movsd	2 * SIZE(CO1, LDC, 2), %xmm9
	movhpd	3 * SIZE(CO1, LDC, 2), %xmm9

	movddup	 %xmm2, %xmm12
	unpckhpd %xmm2, %xmm2

	mulpd	 %xmm15, %xmm12
	mulpd	 %xmm15, %xmm2
	addpd	 %xmm12, %xmm8
	addpd	 %xmm2,  %xmm9

	movsd	%xmm8,  0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm8,  1 * SIZE(CO1, LDC, 2)
	movsd	%xmm9,  2 * SIZE(CO1, LDC, 2)
	movhpd	%xmm9,  3 * SIZE(CO1, LDC, 2)

	movsd	0 * SIZE(CO2, LDC, 2), %xmm8
	movhpd	1 * SIZE(CO2, LDC, 2), %xmm8
	movsd	2 * SIZE(CO2, LDC, 2), %xmm9
	movhpd	3 * SIZE(CO2, LDC, 2), %xmm9

	movddup	 %xmm3, %xmm12
	unpckhpd %xmm3, %xmm3

	mulpd	 %xmm15, %xmm12
	mulpd	 %xmm15, %xmm3
	addpd	 %xmm12, %xmm8
	addpd	 %xmm3,  %xmm9

	movsd	%xmm8,  0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm8,  1 * SIZE(CO2, LDC, 2)
	movsd	%xmm9,  2 * SIZE(CO2, LDC, 2)
	movhpd	%xmm9,  3 * SIZE(CO2, LDC, 2)

	addq	$4 * SIZE, CO1		# coffset += 4
	addq	$4 * SIZE, CO2		# coffset += 4
	ALIGN_4

.L30:
	testq	$1, M
	je	.L39
	ALIGN_4

.L31:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 4), BO
#endif

	movddup	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movapd	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movddup	 4 * SIZE(AO), %xmm10
	pxor	%xmm2, %xmm2
	movapd	 8 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$1, %rax
#else
	addq	$4, %rax
#endif
	movq	%rax, KKK
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
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movsd	ALPHA_R, %xmm15
	movhpd	ALPHA_I, %xmm15
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
	movsd	0 * SIZE(CO1), %xmm8
	movhpd	1 * SIZE(CO1), %xmm8

	movddup	 %xmm0, %xmm12

	mulpd	 %xmm15, %xmm12
	addpd	 %xmm12, %xmm8

	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)

	movsd	0 * SIZE(CO2), %xmm8
	movhpd	1 * SIZE(CO2), %xmm8

	unpckhpd  %xmm0, %xmm0

	mulpd	 %xmm15, %xmm0
	addpd	 %xmm0,  %xmm8

	movsd	%xmm8,  0 * SIZE(CO2)
	movhpd	%xmm8,  1 * SIZE(CO2)

	movsd	0 * SIZE(CO1, LDC, 2), %xmm8
	movhpd	1 * SIZE(CO1, LDC, 2), %xmm8

	movddup	 %xmm1,  %xmm12

	mulpd	 %xmm15, %xmm12
	addpd	 %xmm12, %xmm8

	movsd	%xmm8,  0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm8,  1 * SIZE(CO1, LDC, 2)

	movsd	0 * SIZE(CO2, LDC, 2), %xmm8
	movhpd	1 * SIZE(CO2, LDC, 2), %xmm8

	unpckhpd %xmm1, %xmm1

	mulpd	 %xmm15, %xmm1
	addpd	 %xmm1, %xmm8

	movsd	%xmm8,  0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm8,  1 * SIZE(CO2, LDC, 2)
	ALIGN_4

.L39:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$4, KK
#endif

	leaq	(C, LDC, 4), C		# c += 4 * ldc
	movq	BO, B
	decq	J			# j --
	jg	.L10
	ALIGN_4

.L40:
	testq	$2, N
	je	.L80
	ALIGN_4

#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc
	movq	A, AO		# aoffset = a

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L60
	ALIGN_4

.L51:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 2), BO
#endif

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

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$4, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
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
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movsd	ALPHA_R, %xmm15
	movhpd	ALPHA_I, %xmm15
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
	movsd	0 * SIZE(CO1), %xmm8
	movhpd	1 * SIZE(CO1), %xmm8
	movsd	2 * SIZE(CO1), %xmm9
	movhpd	3 * SIZE(CO1), %xmm9

	movsd	4 * SIZE(CO1), %xmm10
	movhpd	5 * SIZE(CO1), %xmm10
	movsd	6 * SIZE(CO1), %xmm11
	movhpd	7 * SIZE(CO1), %xmm11

	movddup	 %xmm0, %xmm12
	unpckhpd %xmm0, %xmm0
	movddup	 %xmm4, %xmm13
	unpckhpd %xmm4, %xmm4

	mulpd	 %xmm15, %xmm12
	mulpd	 %xmm15, %xmm0
	mulpd	 %xmm15, %xmm13
	mulpd	 %xmm15, %xmm4

	addpd	 %xmm12, %xmm8
	addpd	 %xmm0,  %xmm9
	addpd	 %xmm13, %xmm10
	addpd	 %xmm4,  %xmm11

	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm9,  2 * SIZE(CO1)
	movhpd	%xmm9,  3 * SIZE(CO1)

	movsd	%xmm10, 4 * SIZE(CO1)
	movhpd	%xmm10, 5 * SIZE(CO1)
	movsd	%xmm11, 6 * SIZE(CO1)
	movhpd	%xmm11, 7 * SIZE(CO1)

	movsd	0 * SIZE(CO2), %xmm8
	movhpd	1 * SIZE(CO2), %xmm8
	movsd	2 * SIZE(CO2), %xmm9
	movhpd	3 * SIZE(CO2), %xmm9

	movsd	4 * SIZE(CO2), %xmm10
	movhpd	5 * SIZE(CO2), %xmm10
	movsd	6 * SIZE(CO2), %xmm11
	movhpd	7 * SIZE(CO2), %xmm11

	movddup	 %xmm1, %xmm12
	unpckhpd %xmm1, %xmm1
	movddup	 %xmm5, %xmm13
	unpckhpd %xmm5, %xmm5

	mulpd	 %xmm15, %xmm12
	mulpd	 %xmm15, %xmm1
	mulpd	 %xmm15, %xmm13
	mulpd	 %xmm15, %xmm5

	addpd	 %xmm12, %xmm8
	addpd	 %xmm1,  %xmm9
	addpd	 %xmm13, %xmm10
	addpd	 %xmm5,  %xmm11

	movsd	%xmm8,  0 * SIZE(CO2)
	movhpd	%xmm8,  1 * SIZE(CO2)
	movsd	%xmm9,  2 * SIZE(CO2)
	movhpd	%xmm9,  3 * SIZE(CO2)

	movsd	%xmm10, 4 * SIZE(CO2)
	movhpd	%xmm10, 5 * SIZE(CO2)
	movsd	%xmm11, 6 * SIZE(CO2)
	movhpd	%xmm11, 7 * SIZE(CO2)

	addq	$8 * SIZE, CO1		# coffset += 4
	addq	$8 * SIZE, CO2		# coffset += 4

	decq	I			# i --
	jg	.L51
	ALIGN_4

.L60:
	testq	$2, M
	je	.L70
	ALIGN_4

.L61:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 2), BO
#endif

	movapd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movddup	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movapd	 8 * SIZE(AO), %xmm10
	pxor	%xmm2, %xmm2
	movddup	 8 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$2, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
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
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movsd	ALPHA_R, %xmm15
	movhpd	ALPHA_I, %xmm15
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

	movsd	0 * SIZE(CO1), %xmm8
	movhpd	1 * SIZE(CO1), %xmm8
	movsd	2 * SIZE(CO1), %xmm9
	movhpd	3 * SIZE(CO1), %xmm9

	movddup	 %xmm0, %xmm12
	unpckhpd %xmm0, %xmm0

	mulpd	 %xmm15, %xmm12
	mulpd	 %xmm15, %xmm0
	addpd	 %xmm12, %xmm8
	addpd	 %xmm0,  %xmm9

	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm9,  2 * SIZE(CO1)
	movhpd	%xmm9,  3 * SIZE(CO1)

	movsd	0 * SIZE(CO2), %xmm8
	movhpd	1 * SIZE(CO2), %xmm8
	movsd	2 * SIZE(CO2), %xmm9
	movhpd	3 * SIZE(CO2), %xmm9

	movddup	 %xmm1, %xmm12
	unpckhpd %xmm1, %xmm1

	mulpd	 %xmm15, %xmm12
	mulpd	 %xmm15, %xmm1
	addpd	 %xmm12, %xmm8
	addpd	 %xmm1,  %xmm9

	movsd	%xmm8,  0 * SIZE(CO2)
	movhpd	%xmm8,  1 * SIZE(CO2)
	movsd	%xmm9,  2 * SIZE(CO2)
	movhpd	%xmm9,  3 * SIZE(CO2)

	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
	ALIGN_4

.L70:
	testq	$1, M
	je	.L79
	ALIGN_4

.L71:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 2), BO
#endif

	movddup	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movapd	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movddup	 4 * SIZE(AO), %xmm10
	pxor	%xmm2, %xmm2
	movapd	 8 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$1, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
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
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movsd	ALPHA_R, %xmm15
	movhpd	ALPHA_I, %xmm15
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

	movsd	0 * SIZE(CO1), %xmm8
	movhpd	1 * SIZE(CO1), %xmm8

	movddup	 %xmm0,  %xmm12
	mulpd	 %xmm15, %xmm12
	addpd	 %xmm12, %xmm8

	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)

	movsd	0 * SIZE(CO2), %xmm8
	movhpd	1 * SIZE(CO2), %xmm8

	unpckhpd %xmm0, %xmm0

	mulpd	 %xmm15, %xmm0
	addpd	 %xmm0,  %xmm8

	movsd	%xmm8,  0 * SIZE(CO2)
	movhpd	%xmm8,  1 * SIZE(CO2)
	ALIGN_4

.L79:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif
	leaq	(C, LDC, 2), C
	movq	BO, B
	ALIGN_4

.L80:
	testq	$1, N
	je	.L999
	ALIGN_4

#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	C, CO1
	movq	A, AO

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L100
	ALIGN_4

.L91:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 1), BO
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

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$4, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
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
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movsd	ALPHA_R, %xmm15
	movhpd	ALPHA_I, %xmm15
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

	movsd	0 * SIZE(CO1), %xmm8
	movhpd	1 * SIZE(CO1), %xmm8
	movsd	2 * SIZE(CO1), %xmm9
	movhpd	3 * SIZE(CO1), %xmm9

	movsd	4 * SIZE(CO1), %xmm10
	movhpd	5 * SIZE(CO1), %xmm10
	movsd	6 * SIZE(CO1), %xmm11
	movhpd	7 * SIZE(CO1), %xmm11

	movddup	 %xmm0, %xmm12
	unpckhpd %xmm0, %xmm0
	movddup	 %xmm1, %xmm13
	unpckhpd %xmm1, %xmm1

	mulpd	 %xmm15, %xmm12
	mulpd	 %xmm15, %xmm0
	mulpd	 %xmm15, %xmm13
	mulpd	 %xmm15, %xmm1

	addpd	 %xmm12, %xmm8
	addpd	 %xmm0,  %xmm9
	addpd	 %xmm13, %xmm10
	addpd	 %xmm1,  %xmm11

	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm9,  2 * SIZE(CO1)
	movhpd	%xmm9,  3 * SIZE(CO1)

	movsd	%xmm10, 4 * SIZE(CO1)
	movhpd	%xmm10, 5 * SIZE(CO1)
	movsd	%xmm11, 6 * SIZE(CO1)
	movhpd	%xmm11, 7 * SIZE(CO1)

	addq	$8 * SIZE, CO1		# coffset += 4
	decq	I			# i --
	jg	.L91
	ALIGN_4

.L100:
	testq	$2, M
	je	.L110
	ALIGN_4

.L101:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 1), BO
#endif

	movapd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movddup	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movapd	 8 * SIZE(AO), %xmm10
	pxor	%xmm2, %xmm2
	movddup	 4 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$2, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
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
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movsd	ALPHA_R, %xmm15
	movhpd	ALPHA_I, %xmm15
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

	movsd	0 * SIZE(CO1), %xmm8
	movhpd	1 * SIZE(CO1), %xmm8
	movsd	2 * SIZE(CO1), %xmm9
	movhpd	3 * SIZE(CO1), %xmm9

	movddup	 %xmm0, %xmm12
	unpckhpd %xmm0, %xmm0

	mulpd	 %xmm15, %xmm12
	mulpd	 %xmm15, %xmm0
	addpd	 %xmm12, %xmm8
	addpd	 %xmm0,  %xmm9

	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm9,  2 * SIZE(CO1)
	movhpd	%xmm9,  3 * SIZE(CO1)

	addq	$4 * SIZE, CO1
	ALIGN_4

.L110:
	testq	$1, M
	je	.L999
	ALIGN_4

.L111:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movq	B, BO
#else
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 1), BO
#endif

	movsd	 0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movsd	 0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movsd	 4 * SIZE(AO), %xmm10
	pxor	%xmm2, %xmm2
	movsd	 4 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3

	movapd	 0 * SIZE(AO), %xmm9
	movapd	 0 * SIZE(BO), %xmm8
	movapd	 4 * SIZE(AO), %xmm11
	movapd	 4 * SIZE(BO), %xmm10

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$1, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
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
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movsd	ALPHA_R, %xmm15
	movhpd	ALPHA_I, %xmm15
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

	movsd	0 * SIZE(CO1), %xmm8
	movhpd	1 * SIZE(CO1), %xmm8

	movddup	 %xmm0, %xmm12

	mulpd	 %xmm15, %xmm12
	addpd	 %xmm12, %xmm8

	movsd	%xmm8,  0 * SIZE(CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
	ALIGN_4

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
