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
#define KKK	 248(%rsp)
#define KK	 256(%rsp)
#endif

#define PREFETCH     prefetcht1
#define PREFETCHSIZE (16 * 12 + 3)
#define PREFETCH_R    (4 *  4 + 0)

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define ADD1	  addpd
#define ADD2	  addpd
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define ADD1	  addpd
#define ADD2	  subpd
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define ADD1	  subpd
#define ADD2	  addpd
#else
#define ADD1	  subpd
#define ADD2	  subpd
#endif

#define ADDSUB	subpd

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
#ifdef TRMMKERNEL
	movsd	OLD_OFFSET,  %xmm4
#endif
	movaps	%xmm3,       %xmm0
	movsd	OLD_ALPHA_I, %xmm1
#else
	movq	OLD_LDC,   LDC
#ifdef TRMMKERNEL
	movsd	OLD_OFFSET,  %xmm4
#endif

#endif

	movsd	 %xmm0, ALPHA_R
	movsd	 %xmm1, ALPHA_I

#ifdef TRMMKERNEL
	movsd	%xmm4, OFFSET
	movsd	%xmm4, KK
#ifndef LEFT
	negq	KK
#endif
#endif

	salq	$ZBASE_SHIFT, LDC
	movq	N,  J
	sarq	$1, J		# j = (n >> 2)
	jle	.L100
	ALIGN_4

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc
	movq	A, AO		# aoffset = a

	movq	K, %rax
	salq	$ZBASE_SHIFT + 1, %rax
	leaq	(B, %rax), BB

	movq	M,  I
	sarq	$1, I		# i = (m >> 2)
	jle	.L30
	ALIGN_4

.L10:
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

	prefetcht0	 0 * SIZE(BB)
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
	pxor	%xmm4, %xmm4
	movddup 16 * SIZE(BO), %xmm13
	pxor	%xmm5, %xmm5
	movapd	24 * SIZE(AO), %xmm14
	pxor	%xmm6, %xmm6
	movddup	24 * SIZE(BO), %xmm15
	pxor	%xmm7, %xmm7

	prefetchnta     3 * SIZE(CO1)
	prefetchnta     3 * SIZE(CO2)

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
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movddup	ALPHA_R, %xmm14
	movddup	ALPHA_I, %xmm15
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

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

	addsubpd	%xmm1, %xmm0
	addsubpd	%xmm3, %xmm2
	addsubpd	%xmm5, %xmm4
	addsubpd	%xmm7, %xmm6

	movapd	%xmm0, %xmm1
	movapd	%xmm2, %xmm3
	movapd	%xmm4, %xmm5
	movapd	%xmm6, %xmm7
#else
	addsubpd	%xmm0, %xmm1
	addsubpd	%xmm2, %xmm3
	addsubpd	%xmm4, %xmm5
	addsubpd	%xmm6, %xmm7

	movapd		%xmm1, %xmm0
	movapd		%xmm3, %xmm2
	movapd		%xmm5, %xmm4
	movapd		%xmm7, %xmm6
#endif

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm8
	movhpd	1 * SIZE(CO1), %xmm8
	movsd	2 * SIZE(CO1), %xmm10
	movhpd	3 * SIZE(CO1), %xmm10

	movsd	0 * SIZE(CO2), %xmm9
	movhpd	1 * SIZE(CO2), %xmm9
	movsd	2 * SIZE(CO2), %xmm11
	movhpd	3 * SIZE(CO2), %xmm11
#endif

	SHUFPD_1 %xmm1, %xmm1
	SHUFPD_1 %xmm3, %xmm3
	SHUFPD_1 %xmm5, %xmm5
	SHUFPD_1 %xmm7, %xmm7

	mulpd	%xmm14, %xmm0
	mulpd	%xmm14, %xmm2
	mulpd	%xmm14, %xmm4
	mulpd	%xmm14, %xmm6

	mulpd	%xmm15, %xmm1
	mulpd	%xmm15, %xmm3
	mulpd	%xmm15, %xmm5
	mulpd	%xmm15, %xmm7

	addsubpd	%xmm1, %xmm0
	addsubpd	%xmm3, %xmm2
	addsubpd	%xmm5, %xmm4
	addsubpd	%xmm7, %xmm6

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	addpd	%xmm8,  %xmm0
	addpd	%xmm9,  %xmm2
	addpd	%xmm10, %xmm4
	addpd	%xmm11, %xmm6
#endif

	movsd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 1 * SIZE(CO1)
	movsd	%xmm4, 2 * SIZE(CO1)
	movhpd	%xmm4, 3 * SIZE(CO1)

	movsd	%xmm2, 0 * SIZE(CO2)
	movhpd	%xmm2, 1 * SIZE(CO2)
	movsd	%xmm6, 2 * SIZE(CO2)
	movhpd	%xmm6, 3 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	addq	$4 * SIZE, CO2		# coffset += 4
	decq	I			# i --
	jg	.L10
	ALIGN_4

.L30:
	testq	$1, M
	jle	.L99

.L40:
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
	addq	$1, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
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
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movddup	ALPHA_R, %xmm14
	movddup	ALPHA_I, %xmm15
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

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

	addsubpd	%xmm1, %xmm0
	addsubpd	%xmm3, %xmm2

	movapd	%xmm0, %xmm1
	movapd	%xmm2, %xmm3
#else
	addsubpd	%xmm0, %xmm1
	addsubpd	%xmm2, %xmm3

	movapd		%xmm1, %xmm0
	movapd		%xmm3, %xmm2
#endif

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm8
	movhpd	1 * SIZE(CO1), %xmm8

	movsd	0 * SIZE(CO2), %xmm9
	movhpd	1 * SIZE(CO2), %xmm9
#endif

	SHUFPD_1 %xmm1, %xmm1
	SHUFPD_1 %xmm3, %xmm3

	mulpd	%xmm14, %xmm0
	mulpd	%xmm14, %xmm2

	mulpd	%xmm15, %xmm1
	mulpd	%xmm15, %xmm3

	addsubpd	%xmm1, %xmm0
	addsubpd	%xmm3, %xmm2

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	addpd	%xmm8,  %xmm0
	addpd	%xmm9,  %xmm2
#endif

	movsd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 1 * SIZE(CO1)
	movsd	%xmm2, 0 * SIZE(CO2)
	movhpd	%xmm2, 1 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif
	ALIGN_4

.L99:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addq	$2, KK
#endif

	leaq	(C, LDC, 2), C		# c += 2 * ldc
	movq	BO, B
	decq	J			# j --
	jg	.L01

.L100:
	testq	$1, N
	jle	.L999

.L101:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	C, CO1		# coffset1 = c
	movq	A, AO		# aoffset = a

	movq	M,  I
	sarq	$1, I		# i = (m >> 2)
	jle	.L130
	ALIGN_4

.L110:
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

	prefetchnta     4 * SIZE(CO1)

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
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movddup	ALPHA_R, %xmm14
	movddup	ALPHA_I, %xmm15
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

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

	addsubpd	%xmm1, %xmm0
	addsubpd	%xmm5, %xmm4

	movapd	%xmm0, %xmm1
	movapd	%xmm4, %xmm5
#else
	addsubpd	%xmm0, %xmm1
	addsubpd	%xmm4, %xmm5

	movapd		%xmm1, %xmm0
	movapd		%xmm5, %xmm4
#endif

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm8
	movhpd	1 * SIZE(CO1), %xmm8
	movsd	2 * SIZE(CO1), %xmm10
	movhpd	3 * SIZE(CO1), %xmm10
#endif

	SHUFPD_1 %xmm1, %xmm1
	SHUFPD_1 %xmm5, %xmm5

	mulpd	%xmm14, %xmm0
	mulpd	%xmm14, %xmm4

	mulpd	%xmm15, %xmm1
	mulpd	%xmm15, %xmm5

	addsubpd	%xmm1, %xmm0
	addsubpd	%xmm5, %xmm4

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	addpd	%xmm8,  %xmm0
	addpd	%xmm10, %xmm4
#endif

	movsd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 1 * SIZE(CO1)
	movsd	%xmm4, 2 * SIZE(CO1)
	movhpd	%xmm4, 3 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	decq	I			# i --
	jg	.L110
	ALIGN_4

.L130:
	testq	$1, M
	jle	.L999
	ALIGN_4

.L140:
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
	addq	$1, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
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
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movddup	ALPHA_R, %xmm14
	movddup	ALPHA_I, %xmm15
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

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

	addsubpd	%xmm1, %xmm0
	movapd	%xmm0, %xmm1
#else
	addsubpd	%xmm0, %xmm1
	movapd		%xmm1, %xmm0
#endif

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm8
	movhpd	1 * SIZE(CO1), %xmm8
#endif

	SHUFPD_1 %xmm1, %xmm1
	mulpd	%xmm14, %xmm0
	mulpd	%xmm15, %xmm1
	addsubpd	%xmm1, %xmm0

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	addpd	%xmm8,  %xmm0
#endif

	movsd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 1 * SIZE(CO1)
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
