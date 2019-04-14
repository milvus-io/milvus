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
#define AO	%r12
#define BO	%r13
#define	CO1	%r14
#define CO2	%r15
#define BB	%rbp

#ifndef WINDOWS_ABI

#define STACKSIZE 64

#define OLD_LDC		 8 + STACKSIZE(%rsp)
#define OLD_OFFSET	16 + STACKSIZE(%rsp)

#else

#define STACKSIZE 256

#define OLD_A		40 + STACKSIZE(%rsp)
#define OLD_B		48 + STACKSIZE(%rsp)
#define OLD_C		56 + STACKSIZE(%rsp)
#define OLD_LDC		64 + STACKSIZE(%rsp)
#define OLD_OFFSET	72 + STACKSIZE(%rsp)

#endif

#define ALPHA	  0(%rsp)
#define J	 16(%rsp)
#define OFFSET	 24(%rsp)
#define KK	 32(%rsp)
#define KKK	 40(%rsp)
#define BUFFER	128(%rsp)

#define PREFETCH     prefetcht0
#define PREFETCHSIZE  320

#define KERNEL1(address) \
	mulps	%xmm8, %xmm9; \
	PREFETCH  (PREFETCHSIZE +  0) * SIZE + (address) * SIZE(AO); \
	addps	%xmm9, %xmm0; \
	movshdup  0 * SIZE + (address) * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	addps	%xmm9, %xmm1; \
	movsldup  4 * SIZE + (address) * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	addps	%xmm9, %xmm2; \
	movshdup  4 * SIZE + (address) * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	movaps	  4 * SIZE + (address) * SIZE(AO), %xmm8; \
	addps	%xmm9, %xmm3; \
	movsldup  0 * SIZE + (address) * SIZE(BO), %xmm9

#define KERNEL2(address) \
	mulps	%xmm8, %xmm9; \
	addps	%xmm9, %xmm4; \
	movshdup  0 * SIZE + (address) * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	addps	%xmm9, %xmm5; \
	movsldup  4 * SIZE + (address) * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	addps	%xmm9, %xmm6; \
	movshdup  4 * SIZE + (address) * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	movaps	  8 * SIZE + (address) * SIZE(AO), %xmm8; \
	addps	%xmm9, %xmm7; \
	movsldup  8 * SIZE + (address) * SIZE(BO), %xmm9

#define KERNEL3(address) \
	mulps	%xmm8, %xmm9; \
	addps	%xmm9, %xmm0; \
	movshdup  8 * SIZE + (address) * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	addps	%xmm9, %xmm1; \
	movsldup 12 * SIZE + (address) * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	addps	%xmm9, %xmm2; \
	movshdup 12 * SIZE + (address) * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	movaps	 12 * SIZE + (address) * SIZE(AO), %xmm8; \
	addps	%xmm9, %xmm3; \
	movsldup  8 * SIZE + (address) * SIZE(BO), %xmm9

#define KERNEL4(address) \
	mulps	%xmm8, %xmm9; \
	addps	%xmm9, %xmm4; \
	movshdup  8 * SIZE + (address) * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	addps	%xmm9, %xmm5; \
	movsldup 12 * SIZE + (address) * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	addps	%xmm9, %xmm6; \
	movshdup 12 * SIZE + (address) * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	movaps	  64 * SIZE + (address) * SIZE(AO), %xmm8; \
	addps	%xmm9, %xmm7; \
	movsldup  64 * SIZE + (address) * SIZE(BO), %xmm9

#define KERNEL5(address) \
	mulps	%xmm10, %xmm11; \
	addps	%xmm11, %xmm0; \
	movshdup 16 * SIZE + (address) * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	addps	%xmm11, %xmm1; \
	movsldup 20 * SIZE + (address) * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	addps	%xmm11, %xmm2; \
	movshdup 20 * SIZE + (address) * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	movaps	 20 * SIZE + (address) * SIZE(AO), %xmm10; \
	addps	%xmm11, %xmm3; \
	movsldup 16 * SIZE + (address) * SIZE(BO), %xmm11

#define KERNEL6(address) \
	mulps	%xmm10, %xmm11; \
	addps	%xmm11, %xmm4; \
	movshdup 16 * SIZE + (address) * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	addps	%xmm11, %xmm5; \
	movsldup 20 * SIZE + (address) * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	addps	%xmm11, %xmm6; \
	movshdup 20 * SIZE + (address) * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	movaps	 24 * SIZE + (address) * SIZE(AO), %xmm10; \
	addps	%xmm11, %xmm7; \
	movsldup 24 * SIZE + (address) * SIZE(BO), %xmm11

#define KERNEL7(address) \
	mulps	%xmm10, %xmm11; \
	addps	%xmm11, %xmm0; \
	movshdup 24 * SIZE + (address) * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	addps	%xmm11, %xmm1; \
	movsldup 28 * SIZE + (address) * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	addps	%xmm11, %xmm2; \
	movshdup 28 * SIZE + (address) * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	movaps	 28 * SIZE + (address) * SIZE(AO), %xmm10; \
	addps	%xmm11, %xmm3; \
	movsldup 24 * SIZE + (address) * SIZE(BO), %xmm11

#define KERNEL8(address) \
	mulps	%xmm10, %xmm11; \
	addps	%xmm11, %xmm4; \
	movshdup 24 * SIZE + (address) * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	addps	%xmm11, %xmm5; \
	movsldup 28 * SIZE + (address) * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	addps	%xmm11, %xmm6; \
	movshdup 28 * SIZE + (address) * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	movaps	  80 * SIZE + (address) * SIZE(AO), %xmm10; \
	addps	%xmm11, %xmm7; \
	movsldup  80 * SIZE + (address) * SIZE(BO), %xmm11

#define KERNEL9(address) \
	mulps	%xmm12, %xmm13; \
	PREFETCH  (PREFETCHSIZE + 32) * SIZE + (address) * SIZE(AO); \
	addps	%xmm13, %xmm0; \
	movshdup 32 * SIZE + (address) * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	addps	%xmm13, %xmm1; \
	movsldup 36 * SIZE + (address) * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	addps	%xmm13, %xmm2; \
	movshdup 36 * SIZE + (address) * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	movaps	 36 * SIZE + (address) * SIZE(AO), %xmm12; \
	addps	%xmm13, %xmm3; \
	movsldup 32 * SIZE + (address) * SIZE(BO), %xmm13

#define KERNEL10(address) \
	mulps	%xmm12, %xmm13; \
	addps	%xmm13, %xmm4; \
	movshdup 32 * SIZE + (address) * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	addps	%xmm13, %xmm5; \
	movsldup 36 * SIZE + (address) * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	addps	%xmm13, %xmm6; \
	movshdup 36 * SIZE + (address) * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	movaps	 40 * SIZE + (address) * SIZE(AO), %xmm12; \
	addps	%xmm13, %xmm7; \
	movsldup 40 * SIZE + (address) * SIZE(BO), %xmm13

#define KERNEL11(address) \
	mulps	%xmm12, %xmm13; \
	addps	%xmm13, %xmm0; \
	movshdup 40 * SIZE + (address) * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	addps	%xmm13, %xmm1; \
	movsldup 44 * SIZE + (address) * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	addps	%xmm13, %xmm2; \
	movshdup 44 * SIZE + (address) * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	movaps	 44 * SIZE + (address) * SIZE(AO), %xmm12; \
	addps	%xmm13, %xmm3; \
	movsldup 40 * SIZE + (address) * SIZE(BO), %xmm13

#define KERNEL12(address) \
	mulps	%xmm12, %xmm13; \
	addps	%xmm13, %xmm4; \
	movshdup 40 * SIZE + (address) * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	addps	%xmm13, %xmm5; \
	movsldup 44 * SIZE + (address) * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	addps	%xmm13, %xmm6; \
	movshdup 44 * SIZE + (address) * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	movaps	  96 * SIZE + (address) * SIZE(AO), %xmm12; \
	addps	%xmm13, %xmm7; \
	movsldup  96 * SIZE + (address) * SIZE(BO), %xmm13

#define KERNEL13(address) \
	mulps	%xmm14, %xmm15; \
	addps	%xmm15, %xmm0; \
	movshdup 48 * SIZE + (address) * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	addps	%xmm15, %xmm1; \
	movsldup 52 * SIZE + (address) * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	addps	%xmm15, %xmm2; \
	movshdup 52 * SIZE + (address) * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	movaps	 52 * SIZE + (address) * SIZE(AO), %xmm14; \
	addps	%xmm15, %xmm3; \
	movsldup 48 * SIZE + (address) * SIZE(BO), %xmm15

#define KERNEL14(address) \
	mulps	%xmm14, %xmm15; \
	addps	%xmm15, %xmm4; \
	movshdup 48 * SIZE + (address) * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	addps	%xmm15, %xmm5; \
	movsldup 52 * SIZE + (address) * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	addps	%xmm15, %xmm6; \
	movshdup 52 * SIZE + (address) * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	movaps	 56 * SIZE + (address) * SIZE(AO), %xmm14; \
	addps	%xmm15, %xmm7; \
	movsldup 56 * SIZE + (address) * SIZE(BO), %xmm15

#define KERNEL15(address) \
	mulps	%xmm14, %xmm15; \
	addps	%xmm15, %xmm0; \
	movshdup 56 * SIZE + (address) * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	addps	%xmm15, %xmm1; \
	movsldup 60 * SIZE + (address) * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	addps	%xmm15, %xmm2; \
	movshdup 60 * SIZE + (address) * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	movaps	 60 * SIZE + (address) * SIZE(AO), %xmm14; \
	addps	%xmm15, %xmm3; \
	movsldup 56 * SIZE + (address) * SIZE(BO), %xmm15

#define KERNEL16(address) \
	mulps	%xmm14, %xmm15; \
	addps	%xmm15, %xmm4; \
	movshdup 56 * SIZE + (address) * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	addps	%xmm15, %xmm5; \
	movsldup 60 * SIZE + (address) * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	addps	%xmm15, %xmm6; \
	movshdup 60 * SIZE + (address) * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	movaps	 112 * SIZE + (address) * SIZE(AO), %xmm14; \
	addps	%xmm15, %xmm7; \
	movsldup 112 * SIZE + (address) * SIZE(BO), %xmm15

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
	movsd	OLD_OFFSET, %xmm4
#endif
	movaps	%xmm3, %xmm0

#else
	movq	OLD_LDC,   LDC
#ifdef TRMMKERNEL
	movsd	OLD_OFFSET, %xmm4
#endif

#endif

	movq	%rsp, %rbx	# save old stack
	subq	$128 + LOCAL_BUFFER_SIZE, %rsp
	andq	$-4096, %rsp	# align stack

	STACK_TOUCHING

	shufps	$0, %xmm0, %xmm0
	movaps	%xmm0, ALPHA

#ifdef TRMMKERNEL
	movsd	%xmm4, OFFSET
	movsd	%xmm4, KK
#ifndef LEFT
	negq	KK
#endif
#endif

	leaq	(, LDC, SIZE), LDC

	movq	N,  J
	sarq	$2, J		# j = (n >> 2)
	jle	.L50

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

/* Copying to Sub Buffer */
	leaq	BUFFER, BO

	movq	K, %rax
	sarq	$2, %rax
	jle	.L03
	ALIGN_4

.L02:
	movddup	 0 * SIZE(B), %xmm0
	movddup	 2 * SIZE(B), %xmm1
	movddup	 4 * SIZE(B), %xmm2
	movddup	 6 * SIZE(B), %xmm3
	movddup	 8 * SIZE(B), %xmm4
	movddup	10 * SIZE(B), %xmm5
	movddup	12 * SIZE(B), %xmm6
	movddup	14 * SIZE(B), %xmm7

	movaps	%xmm0,  0 * SIZE(BO)
	movaps	%xmm1,  4 * SIZE(BO)
	movaps	%xmm2,  8 * SIZE(BO)
	movaps	%xmm3, 12 * SIZE(BO)
	movaps	%xmm4, 16 * SIZE(BO)
	movaps	%xmm5, 20 * SIZE(BO)
	movaps	%xmm6, 24 * SIZE(BO)
	movaps	%xmm7, 28 * SIZE(BO)

	prefetcht1	128 * SIZE(BO)
	prefetcht0	112 * SIZE(B)

	addq	$16 * SIZE, B
	addq	$32 * SIZE, BO

	decq	%rax
	jne	.L02
	ALIGN_4

.L03:
	movq	K, %rax
	andq	$3, %rax
	BRANCH
	jle	.L10
	ALIGN_4

.L04:
	movddup	 0 * SIZE(B), %xmm0
	movddup	 2 * SIZE(B), %xmm1

	movaps	%xmm0,  0 * SIZE(BO)
	movaps	%xmm1,  4 * SIZE(BO)

	addq	$4 * SIZE, B
	addq	$8 * SIZE, BO
	decq	%rax
	jne	.L04
	ALIGN_4

.L10:
	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc
	movq	A, AO		# aoffset = a

	leaq	112 * SIZE(B), BB

	movq	M,  I
	sarq	$3, I	# i = (m >> 3)
	jle	.L20
	ALIGN_4

.L11:
	prefetcht0	 0 * SIZE(BB)
	prefetcht0	 8 * SIZE(BB)
	subq	   $-16 * SIZE, BB

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

	movaps	  0 * SIZE(AO), %xmm8
	movaps	 16 * SIZE(AO), %xmm10
	movaps	 32 * SIZE(AO), %xmm12
	movaps	 48 * SIZE(AO), %xmm14

	movsldup  0 * SIZE(BO), %xmm9
	movsldup 16 * SIZE(BO), %xmm11
	movsldup 32 * SIZE(BO), %xmm13
	movsldup 48 * SIZE(BO), %xmm15

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
	pxor	%xmm3, %xmm3

	prefetchnta     8 * SIZE(CO1)
	pxor	%xmm4, %xmm4
	prefetchnta     8 * SIZE(CO2)
	pxor	%xmm5, %xmm5
	prefetchnta     8 * SIZE(CO1, LDC, 2)
	pxor	%xmm6, %xmm6
	prefetchnta     8 * SIZE(CO2, LDC, 2)
	pxor	%xmm7, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$8, %rax
#else
	addq	$4, %rax
#endif
	movq	%rax, KKK
#endif

#if 1
	andq	$-8, %rax
	salq	$4, %rax
	je	.L15

.L1X:
	KERNEL1 (64  *  0)
	KERNEL2 (64  *  0)
	KERNEL3 (64  *  0)
	KERNEL4 (64  *  0)
	KERNEL5 (64  *  0)
	KERNEL6 (64  *  0)
	KERNEL7 (64  *  0)
	KERNEL8 (64  *  0)
	KERNEL9 (64  *  0)
	KERNEL10(64  *  0)
	KERNEL11(64  *  0)
	KERNEL12(64  *  0)
	KERNEL13(64  *  0)
	KERNEL14(64  *  0)
	KERNEL15(64  *  0)
	KERNEL16(64  *  0)
	cmpq	$128 *  1, %rax
	NOBRANCH
	jle	.L12
	KERNEL1 (64  *  1)
	KERNEL2 (64  *  1)
	KERNEL3 (64  *  1)
	KERNEL4 (64  *  1)
	KERNEL5 (64  *  1)
	KERNEL6 (64  *  1)
	KERNEL7 (64  *  1)
	KERNEL8 (64  *  1)
	KERNEL9 (64  *  1)
	KERNEL10(64  *  1)
	KERNEL11(64  *  1)
	KERNEL12(64  *  1)
	KERNEL13(64  *  1)
	KERNEL14(64  *  1)
	KERNEL15(64  *  1)
	KERNEL16(64  *  1)
	cmpq	$128 *  2, %rax
	NOBRANCH
	jle	.L12
	KERNEL1 (64  *  2)
	KERNEL2 (64  *  2)
	KERNEL3 (64  *  2)
	KERNEL4 (64  *  2)
	KERNEL5 (64  *  2)
	KERNEL6 (64  *  2)
	KERNEL7 (64  *  2)
	KERNEL8 (64  *  2)
	KERNEL9 (64  *  2)
	KERNEL10(64  *  2)
	KERNEL11(64  *  2)
	KERNEL12(64  *  2)
	KERNEL13(64  *  2)
	KERNEL14(64  *  2)
	KERNEL15(64  *  2)
	KERNEL16(64  *  2)
	cmpq	$128 *  3, %rax
	NOBRANCH
	jle	.L12
	KERNEL1 (64  *  3)
	KERNEL2 (64  *  3)
	KERNEL3 (64  *  3)
	KERNEL4 (64  *  3)
	KERNEL5 (64  *  3)
	KERNEL6 (64  *  3)
	KERNEL7 (64  *  3)
	KERNEL8 (64  *  3)
	KERNEL9 (64  *  3)
	KERNEL10(64  *  3)
	KERNEL11(64  *  3)
	KERNEL12(64  *  3)
	KERNEL13(64  *  3)
	KERNEL14(64  *  3)
	KERNEL15(64  *  3)
	KERNEL16(64  *  3)
	cmpq	$128 *  4, %rax
	NOBRANCH
	jle	.L12
	KERNEL1 (64  *  4)
	KERNEL2 (64  *  4)
	KERNEL3 (64  *  4)
	KERNEL4 (64  *  4)
	KERNEL5 (64  *  4)
	KERNEL6 (64  *  4)
	KERNEL7 (64  *  4)
	KERNEL8 (64  *  4)
	KERNEL9 (64  *  4)
	KERNEL10(64  *  4)
	KERNEL11(64  *  4)
	KERNEL12(64  *  4)
	KERNEL13(64  *  4)
	KERNEL14(64  *  4)
	KERNEL15(64  *  4)
	KERNEL16(64  *  4)
	cmpq	$128 *  5, %rax
	NOBRANCH
	jle	.L12
	KERNEL1 (64  *  5)
	KERNEL2 (64  *  5)
	KERNEL3 (64  *  5)
	KERNEL4 (64  *  5)
	KERNEL5 (64  *  5)
	KERNEL6 (64  *  5)
	KERNEL7 (64  *  5)
	KERNEL8 (64  *  5)
	KERNEL9 (64  *  5)
	KERNEL10(64  *  5)
	KERNEL11(64  *  5)
	KERNEL12(64  *  5)
	KERNEL13(64  *  5)
	KERNEL14(64  *  5)
	KERNEL15(64  *  5)
	KERNEL16(64  *  5)
	cmpq	$128 *  6, %rax
	NOBRANCH
	jle	.L12
	KERNEL1 (64  *  6)
	KERNEL2 (64  *  6)
	KERNEL3 (64  *  6)
	KERNEL4 (64  *  6)
	KERNEL5 (64  *  6)
	KERNEL6 (64  *  6)
	KERNEL7 (64  *  6)
	KERNEL8 (64  *  6)
	KERNEL9 (64  *  6)
	KERNEL10(64  *  6)
	KERNEL11(64  *  6)
	KERNEL12(64  *  6)
	KERNEL13(64  *  6)
	KERNEL14(64  *  6)
	KERNEL15(64  *  6)
	KERNEL16(64  *  6)
	cmpq	$128 *  7, %rax
	NOBRANCH
	jle	.L12
	KERNEL1 (64  *  7)
	KERNEL2 (64  *  7)
	KERNEL3 (64  *  7)
	KERNEL4 (64  *  7)
	KERNEL5 (64  *  7)
	KERNEL6 (64  *  7)
	KERNEL7 (64  *  7)
	KERNEL8 (64  *  7)
	KERNEL9 (64  *  7)
	KERNEL10(64  *  7)
	KERNEL11(64  *  7)
	KERNEL12(64  *  7)
	KERNEL13(64  *  7)
	KERNEL14(64  *  7)
	KERNEL15(64  *  7)
	KERNEL16(64  *  7)

	addq	$64 * 8  * SIZE, AO
	addq	$64 * 8  * SIZE, BO
	subq	$128 * 8, %rax
	jg	.L1X

.L12:
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#else
	sarq	$3, %rax
	je	.L15
	ALIGN_4

.L12:
	KERNEL1 (64  *  0)
	KERNEL2 (64  *  0)
	KERNEL3 (64  *  0)
	KERNEL4 (64  *  0)
	KERNEL5 (64  *  0)
	KERNEL6 (64  *  0)
	KERNEL7 (64  *  0)
	KERNEL8 (64  *  0)
	KERNEL9 (64  *  0)
	KERNEL10(64  *  0)
	KERNEL11(64  *  0)
	KERNEL12(64  *  0)
	KERNEL13(64  *  0)
	KERNEL14(64  *  0)
	KERNEL15(64  *  0)
	KERNEL16(64  *  0)

	addq   $64 * SIZE, AO
	addq   $64 * SIZE, BO
	decq   %rax
	jne    .L12
#endif
	ALIGN_4

.L15:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm15
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L18
	ALIGN_4

.L16:
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movshdup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm1
	movsldup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movshdup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	  4 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm3
	movsldup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm4
	movshdup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm5
	movsldup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm6
	movshdup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
 	movaps	  8 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm7
	movsldup  8 * SIZE(BO), %xmm9

	addq	$8 * SIZE, AO
	addq	$8 * SIZE, BO
	decq	%rax
	jg	.L16
	ALIGN_4

.L18:
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm8
	mulps	%xmm15, %xmm0
	movhps	2 * SIZE(CO1), %xmm8
	mulps	%xmm15, %xmm1
	movsd	4 * SIZE(CO1), %xmm9
	mulps	%xmm15, %xmm2
	movhps	6 * SIZE(CO1), %xmm9
	mulps	%xmm15, %xmm3
	movsd	0 * SIZE(CO2), %xmm10
	mulps	%xmm15, %xmm4
	movhps	2 * SIZE(CO2), %xmm10
	mulps	%xmm15, %xmm5
	movsd	4 * SIZE(CO2), %xmm11
	mulps	%xmm15, %xmm6
	movhps	6 * SIZE(CO2), %xmm11
	mulps	%xmm15, %xmm7

	movsd	0 * SIZE(CO1, LDC, 2), %xmm12
	movhps	2 * SIZE(CO1, LDC, 2), %xmm12
	movsd	4 * SIZE(CO1, LDC, 2), %xmm13
	movhps	6 * SIZE(CO1, LDC, 2), %xmm13
	movsd	0 * SIZE(CO2, LDC, 2), %xmm14
	movhps	2 * SIZE(CO2, LDC, 2), %xmm14
	movsd	4 * SIZE(CO2, LDC, 2), %xmm15
	movhps	6 * SIZE(CO2, LDC, 2), %xmm15

	addps	%xmm8,  %xmm0
	addps	%xmm9,  %xmm4
	addps	%xmm10, %xmm1
	addps	%xmm11, %xmm5
	addps	%xmm12, %xmm2
	movsd	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 2 * SIZE(CO1)
	addps	%xmm13, %xmm6
	movsd	%xmm4, 4 * SIZE(CO1)
	movhps	%xmm4, 6 * SIZE(CO1)
	addps	%xmm14, %xmm3
	movsd	%xmm1, 0 * SIZE(CO2)
	movhps	%xmm1, 2 * SIZE(CO2)
	addps	%xmm15, %xmm7
	movsd	%xmm5, 4 * SIZE(CO2)
	movhps	%xmm5, 6 * SIZE(CO2)
#else
	mulps	%xmm15, %xmm0
	mulps	%xmm15, %xmm1
	mulps	%xmm15, %xmm2
	mulps	%xmm15, %xmm3
	mulps	%xmm15, %xmm4
	mulps	%xmm15, %xmm5
	mulps	%xmm15, %xmm6
	mulps	%xmm15, %xmm7

	movsd	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 2 * SIZE(CO1)
	movsd	%xmm4, 4 * SIZE(CO1)
	movhps	%xmm4, 6 * SIZE(CO1)
	movsd	%xmm1, 0 * SIZE(CO2)
	movhps	%xmm1, 2 * SIZE(CO2)
	movsd	%xmm5, 4 * SIZE(CO2)
	movhps	%xmm5, 6 * SIZE(CO2)
#endif

	movsd	%xmm2, 0 * SIZE(CO1, LDC, 2)
	movhps	%xmm2, 2 * SIZE(CO1, LDC, 2)
	movsd	%xmm6, 4 * SIZE(CO1, LDC, 2)
	movhps	%xmm6, 6 * SIZE(CO1, LDC, 2)

	movsd	%xmm3, 0 * SIZE(CO2, LDC, 2)
	movhps	%xmm3, 2 * SIZE(CO2, LDC, 2)
	movsd	%xmm7, 4 * SIZE(CO2, LDC, 2)
	movhps	%xmm7, 6 * SIZE(CO2, LDC, 2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax,    8), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$8, KK
#endif

	addq	$8 * SIZE, CO1		# coffset += 4
	addq	$8 * SIZE, CO2		# coffset += 4
	decq	I			# i --
	jg	.L11
	ALIGN_4

.L20:
	testq	$4, M
	je	.L30

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

	movaps	  0 * SIZE(AO), %xmm8
	movaps	 16 * SIZE(AO), %xmm10

	movsldup  0 * SIZE(BO), %xmm9
	movsldup 16 * SIZE(BO), %xmm11
	movsldup 32 * SIZE(BO), %xmm13
	movsldup 48 * SIZE(BO), %xmm15

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
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
	addq	$4, %rax
#else
	addq	$4, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$3, %rax
	je	.L25
	ALIGN_4

.L22:
	mulps	%xmm8, %xmm9
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
	addps	%xmm9, %xmm0
	movshdup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm1
	movsldup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movshdup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	  4 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm3
	movsldup  8 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movshdup  8 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm1
	movsldup 12 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movshdup 12 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	  8 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm3
	movsldup 64 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm0
	movshdup 16 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm1
	movsldup 20 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm2
	movshdup 20 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	movaps	 12 * SIZE(AO), %xmm8
	addps	%xmm11, %xmm3
	movsldup 24 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm0
	movshdup 24 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm1
	movsldup 28 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm2
	movshdup 28 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	movaps	 32 * SIZE(AO), %xmm8
	addps	%xmm11, %xmm3
	movsldup 80 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm0
	movshdup 32 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm1
	movsldup 36 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm2
	movshdup 36 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	movaps	 20 * SIZE(AO), %xmm10
	addps	%xmm13, %xmm3
	movsldup 40 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm0
	movshdup 40 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm1
	movsldup 44 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm2
	movshdup 44 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	movaps	 24 * SIZE(AO), %xmm10
	addps	%xmm13, %xmm3
	movsldup 96 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm0
	movshdup 48 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm1
	movsldup 52 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm2
	movshdup 52 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	movaps	 28 * SIZE(AO), %xmm10
	addps	%xmm15, %xmm3
	movsldup 56 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm0
	movshdup 56 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm1
	movsldup 60 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm2
	movshdup 60 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	movaps	 48 * SIZE(AO), %xmm10
	addps	%xmm15, %xmm3
	movsldup 112 * SIZE(BO), %xmm15

	addq   $32 * SIZE, AO
	addq   $64 * SIZE, BO
	decq   %rax
	jne    .L22
	ALIGN_4

.L25:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm15
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L28
	ALIGN_4

.L26:
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movshdup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm1
	movsldup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movshdup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	  4 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm3
	movsldup  8 * SIZE(BO), %xmm9

	addq	$4 * SIZE, AO
	addq	$8 * SIZE, BO
	decq	%rax
	jg	.L26
	ALIGN_4

.L28:
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm8
	movhps	2 * SIZE(CO1), %xmm8
	movsd	0 * SIZE(CO2), %xmm10
	movhps	2 * SIZE(CO2), %xmm10

	mulps	%xmm15, %xmm0
	mulps	%xmm15, %xmm1
 	mulps	%xmm15, %xmm2
	mulps	%xmm15, %xmm3

	movsd	0 * SIZE(CO1, LDC, 2), %xmm12
	movhps	2 * SIZE(CO1, LDC, 2), %xmm12
	movsd	0 * SIZE(CO2, LDC, 2), %xmm14
	movhps	2 * SIZE(CO2, LDC, 2), %xmm14

	addps	%xmm8,  %xmm0
	addps	%xmm10, %xmm1
	addps	%xmm12, %xmm2
	addps	%xmm14, %xmm3
#else
	mulps	%xmm15, %xmm0
	mulps	%xmm15, %xmm1
 	mulps	%xmm15, %xmm2
	mulps	%xmm15, %xmm3
#endif

	movsd	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 2 * SIZE(CO1)
	movsd	%xmm1, 0 * SIZE(CO2)
	movhps	%xmm1, 2 * SIZE(CO2)

	movsd	%xmm2, 0 * SIZE(CO1, LDC, 2)
	movhps	%xmm2, 2 * SIZE(CO1, LDC, 2)
	movsd	%xmm3, 0 * SIZE(CO2, LDC, 2)
	movhps	%xmm3, 2 * SIZE(CO2, LDC, 2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax,    8), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	addq	$4 * SIZE, CO2		# coffset += 4
	ALIGN_4

.L30:
	testq	$2, M
	je	.L40

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

	movddup	  0 * SIZE(AO), %xmm8
	movddup	  8 * SIZE(AO), %xmm10
	movsd     0 * SIZE(BO), %xmm9
	movsd    32 * SIZE(BO), %xmm11

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
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
	je	.L35
	ALIGN_4

.L32:
	shufps	  $0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
	addps	%xmm9, %xmm0
	movsd     4 * SIZE(BO), %xmm9
	shufps	  $0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
	movddup	  2 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movsd     8 * SIZE(BO), %xmm9
	shufps	  $0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movsd    12 * SIZE(BO), %xmm9
	shufps	  $0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
	movddup	  4 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm3
	movsd    16 * SIZE(BO), %xmm9
	shufps	  $0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movsd    20 * SIZE(BO), %xmm9
	shufps	  $0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
	movddup	  6 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movsd    24 * SIZE(BO), %xmm9
	shufps	  $0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movsd    28 * SIZE(BO), %xmm9
	shufps	  $0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
	movddup	 16 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm3
	shufps	  $0x50, %xmm11, %xmm11
	mulps	%xmm10, %xmm11
	movsd    64 * SIZE(BO), %xmm9
	addps	%xmm11, %xmm0
	movsd    36 * SIZE(BO), %xmm11
	shufps	  $0x50, %xmm11, %xmm11
	mulps	%xmm10, %xmm11
	movddup	 10 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm1
	movsd    40 * SIZE(BO), %xmm11
	shufps	  $0x50, %xmm11, %xmm11
	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm2
	movsd    44 * SIZE(BO), %xmm11
	shufps	  $0x50, %xmm11, %xmm11
	mulps	%xmm10, %xmm11
	movddup	 12 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm3
	movsd    48 * SIZE(BO), %xmm11
	shufps	  $0x50, %xmm11, %xmm11
	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm0
	movsd    52 * SIZE(BO), %xmm11
	shufps	  $0x50, %xmm11, %xmm11
	mulps	%xmm10, %xmm11
	movddup	 14 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm1
	movsd    56 * SIZE(BO), %xmm11
	shufps	  $0x50, %xmm11, %xmm11
	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm2
	movsd    60 * SIZE(BO), %xmm11
	shufps	  $0x50, %xmm11, %xmm11
	mulps	%xmm10, %xmm11
	movddup	 24 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm3
	movsd    96 * SIZE(BO), %xmm11

	addq   $16 * SIZE, AO
	addq   $64 * SIZE, BO
	decq   %rax
	jne    .L32
	ALIGN_4

.L35:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm15
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L38
	ALIGN_4

.L36:
	shufps	  $0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movsd     4 * SIZE(BO), %xmm9
	shufps	  $0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
	movddup	  2 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movsd     8 * SIZE(BO), %xmm9

	addq	$2 * SIZE, AO
	addq	$8 * SIZE, BO
	decq	%rax
	jg	.L36
	ALIGN_4

.L38:
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm8
	movhps	0 * SIZE(CO2), %xmm8
	movsd	0 * SIZE(CO1, LDC, 2), %xmm9
	movhps	0 * SIZE(CO2, LDC, 2), %xmm9
#endif

	addps	%xmm2, %xmm0
	addps	%xmm3, %xmm1

	mulps	%xmm15, %xmm0
	mulps	%xmm15, %xmm1

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	addps	%xmm8, %xmm0
	addps	%xmm9, %xmm1
#endif

	movsd	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 0 * SIZE(CO2)
	movsd	%xmm1, 0 * SIZE(CO1, LDC, 2)
	movhps	%xmm1, 0 * SIZE(CO2, LDC, 2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax,    8), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$2 * SIZE, CO1		# coffset += 4
	addq	$2 * SIZE, CO2		# coffset += 4
	ALIGN_4

.L40:
	testq	$1, M
	je	.L49

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   4), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 8), BO
#endif

	movss	 0 * SIZE(AO), %xmm8
	movss	 4 * SIZE(AO), %xmm10
	movsd	 0 * SIZE(BO), %xmm9
	movsd	32 * SIZE(BO), %xmm11

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1

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
	je	.L45
	ALIGN_4

.L42:
	shufps	$0, %xmm8, %xmm8
	movhps	 4 * SIZE(BO), %xmm9
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
	mulps	%xmm8, %xmm9
	movss	 1 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movsd	 8 * SIZE(BO), %xmm9
	shufps	$0, %xmm8, %xmm8
	movhps	12 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movss	 2 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movsd	16 * SIZE(BO), %xmm9
	shufps	$0, %xmm8, %xmm8
	movhps	20 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movss	 3 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movsd	24 * SIZE(BO), %xmm9
	shufps	$0, %xmm8, %xmm8
	movhps	28 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movss	 8 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movsd	64 * SIZE(BO), %xmm9
	shufps	$0, %xmm10, %xmm10
	movhps	36 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	movss	 5 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm0
	movsd	40 * SIZE(BO), %xmm11
	shufps	$0, %xmm10, %xmm10
	movhps	44 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	movss	 6 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm1
	movsd	48 * SIZE(BO), %xmm11
	shufps	$0, %xmm10, %xmm10
	movhps	52 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	movss	 7 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm0
	movsd	56 * SIZE(BO), %xmm11
	shufps	$0, %xmm10, %xmm10
	movhps	60 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	movss	12 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm1
	movsd	96 * SIZE(BO), %xmm11

	addq   $ 8 * SIZE, AO
	addq   $64 * SIZE, BO
	decq   %rax
	jne    .L42
	ALIGN_4

.L45:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm15
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L48
	ALIGN_4

.L46:
	shufps	$0, %xmm8, %xmm8
	movhps	 4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movss	 1 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movsd	 8 * SIZE(BO), %xmm9

	addq	$1 * SIZE, AO		# aoffset  += 4
	addq	$8 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L46
	ALIGN_4

.L48:
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movss	0 * SIZE(CO1), %xmm8
	movss	0 * SIZE(CO2), %xmm9
	movss	0 * SIZE(CO1, LDC, 2), %xmm10
	movss	0 * SIZE(CO2, LDC, 2), %xmm11
#endif

	addps	%xmm1, %xmm0

	mulps	%xmm15, %xmm0

	movhlps %xmm0, %xmm1

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	addss	%xmm0, %xmm8
	psrlq	$32, %xmm0
	addss	%xmm0, %xmm9
	addss	%xmm1, %xmm10
	psrlq	$32, %xmm1
	addss	%xmm1, %xmm11

	movss	%xmm8,  0 * SIZE(CO1)
	movss	%xmm9,  0 * SIZE(CO2)
	movss	%xmm10, 0 * SIZE(CO1, LDC, 2)
	movss	%xmm11, 0 * SIZE(CO2, LDC, 2)
#else
	movss	%xmm0,  0 * SIZE(CO1)
	psrlq	$32, %xmm0
	movss	%xmm0,  0 * SIZE(CO2)
	movss	%xmm1,  0 * SIZE(CO1, LDC, 2)
	psrlq	$32, %xmm1
	movss	%xmm1,  0 * SIZE(CO2, LDC, 2)
#endif

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax,    4), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 8), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif
	ALIGN_4

.L49:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$4, KK
#endif
	leaq	(C, LDC, 4), C		# c += 4 * ldc
	decq	J			# j --
	jg	.L01

.L50:
	testq	$2, N
	je	.L100

.L51:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

/* Copying to Sub Buffer */
	leaq	BUFFER, BO

	movq	K, %rax
	sarq	$3, %rax
	jle	.L53
	ALIGN_4

.L52:
	movddup	 0 * SIZE(B), %xmm0
	movddup	 2 * SIZE(B), %xmm1
	movddup	 4 * SIZE(B), %xmm2
	movddup	 6 * SIZE(B), %xmm3
	movddup	 8 * SIZE(B), %xmm4
	movddup	10 * SIZE(B), %xmm5
	movddup	12 * SIZE(B), %xmm6
	movddup	14 * SIZE(B), %xmm7

	movaps	%xmm0,  0 * SIZE(BO)
	movaps	%xmm1,  4 * SIZE(BO)
	movaps	%xmm2,  8 * SIZE(BO)
	movaps	%xmm3, 12 * SIZE(BO)
	movaps	%xmm4, 16 * SIZE(BO)
	movaps	%xmm5, 20 * SIZE(BO)
	movaps	%xmm6, 24 * SIZE(BO)
	movaps	%xmm7, 28 * SIZE(BO)

	prefetcht1	128 * SIZE(BO)
	prefetcht0	112 * SIZE(B)

	addq	$16 * SIZE, B
	addq	$32 * SIZE, BO

	decq	%rax
	jne	.L52
	ALIGN_4

.L53:
	movq	K, %rax
	andq	$7, %rax
	BRANCH
	jle	.L60
	ALIGN_4

.L54:
	movddup	 0 * SIZE(B), %xmm0
	movaps	%xmm0,  0 * SIZE(BO)

	addq	$ 2 * SIZE, B
	addq	$ 4 * SIZE, BO
	decq	%rax
	jne	.L54
	ALIGN_4

.L60:
	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc
	movq	A, AO		# aoffset = a

	movq	M,  I
	sarq	$3, I	# i = (m >> 3)
	jle	.L70
	ALIGN_4

.L61:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

 	movaps	  0 * SIZE(AO), %xmm8
 	movaps	 16 * SIZE(AO), %xmm10
 	movaps	 32 * SIZE(AO), %xmm12
 	movaps	 48 * SIZE(AO), %xmm14

	movsldup  0 * SIZE(BO), %xmm9
	movsldup 16 * SIZE(BO), %xmm11

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	prefetcht2     4 * SIZE(CO1)
	pxor	%xmm4, %xmm4
	prefetcht2     4 * SIZE(CO2)
	pxor	%xmm5, %xmm5

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$8, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$3, %rax
	je	.L65
	ALIGN_4

.L62:
	mulps	%xmm8, %xmm9
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
	addps	%xmm9, %xmm0
	movshdup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	  4 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movsldup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm4
	movshdup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
 	movaps	  8 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm5
	movsldup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movshdup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	 12 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movsldup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm4
	movshdup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
 	movaps	 64 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm5
	movsldup  8 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm0
	movshdup  8 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	movaps	 20 * SIZE(AO), %xmm10
	addps	%xmm9, %xmm1
	movsldup  8 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm4
	movshdup  8 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
 	movaps	 24 * SIZE(AO), %xmm10
	addps	%xmm9, %xmm5
	movsldup 12 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm0
	movshdup 12 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	movaps	 28 * SIZE(AO), %xmm10
	addps	%xmm9, %xmm1
	movsldup 12 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm4
	movshdup 12 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
 	movaps	 80 * SIZE(AO), %xmm10
	addps	%xmm9, %xmm5
	movsldup 32 * SIZE(BO), %xmm9
	mulps	%xmm12, %xmm11
	PREFETCH  (PREFETCHSIZE + 32) * SIZE(AO)
	addps	%xmm11, %xmm0
	movshdup 16 * SIZE(BO), %xmm11
	mulps	%xmm12, %xmm11
	movaps	 36 * SIZE(AO), %xmm12
	addps	%xmm11, %xmm1
	movsldup 16 * SIZE(BO), %xmm11
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm4
	movshdup 16 * SIZE(BO), %xmm11
	mulps	%xmm12, %xmm11
 	movaps	 40 * SIZE(AO), %xmm12
	addps	%xmm11, %xmm5
	movsldup 20 * SIZE(BO), %xmm11
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm0
	movshdup 20 * SIZE(BO), %xmm11
	mulps	%xmm12, %xmm11
	movaps	 44 * SIZE(AO), %xmm12
	addps	%xmm11, %xmm1
	movsldup 20 * SIZE(BO), %xmm11
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm4
	movshdup 20 * SIZE(BO), %xmm11
	mulps	%xmm12, %xmm11
 	movaps	 96 * SIZE(AO), %xmm12
	addps	%xmm11, %xmm5
	movsldup 24 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm11
	addps	%xmm11, %xmm0
	movshdup 24 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm11
	movaps	 52 * SIZE(AO), %xmm14
	addps	%xmm11, %xmm1
	movsldup 24 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm11
	addps	%xmm11, %xmm4
	movshdup 24 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm11
 	movaps	 56 * SIZE(AO), %xmm14
	addps	%xmm11, %xmm5
	movsldup 28 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm11
	addps	%xmm11, %xmm0
	movshdup 28 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm11
	movaps	 60 * SIZE(AO), %xmm14
	addps	%xmm11, %xmm1
	movsldup 28 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm11
	addps	%xmm11, %xmm4
	movshdup 28 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm11
 	movaps	112 * SIZE(AO), %xmm14
	addps	%xmm11, %xmm5
	movsldup 48 * SIZE(BO), %xmm11

	addq   $64 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L62
	ALIGN_4

.L65:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm15
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L68
	ALIGN_4

.L66:
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movshdup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	  4 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movsldup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm4
	movshdup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
 	movaps	  8 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm5
	movsldup  4 * SIZE(BO), %xmm9

	addq	$8 * SIZE, AO
	addq	$4 * SIZE, BO
	decq	%rax
	jg	.L66
	ALIGN_4

.L68:
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm8
	movhps	2 * SIZE(CO1), %xmm8
	movsd	4 * SIZE(CO1), %xmm9
	movhps	6 * SIZE(CO1), %xmm9

	movsd	0 * SIZE(CO2), %xmm10
	movhps	2 * SIZE(CO2), %xmm10
	movsd	4 * SIZE(CO2), %xmm11
	movhps	6 * SIZE(CO2), %xmm11
#endif

	mulps	%xmm15, %xmm0
	mulps	%xmm15, %xmm4
	mulps	%xmm15, %xmm1
	mulps	%xmm15, %xmm5

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	addps	%xmm8,  %xmm0
	addps	%xmm9,  %xmm4
	addps	%xmm10, %xmm1
	addps	%xmm11, %xmm5
#endif

	movsd	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 2 * SIZE(CO1)
	movsd	%xmm4, 4 * SIZE(CO1)
	movhps	%xmm4, 6 * SIZE(CO1)

	movsd	%xmm1, 0 * SIZE(CO2)
	movhps	%xmm1, 2 * SIZE(CO2)
	movsd	%xmm5, 4 * SIZE(CO2)
	movhps	%xmm5, 6 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax,    8), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$8, KK
#endif

	addq	$8 * SIZE, CO1		# coffset += 4
	addq	$8 * SIZE, CO2		# coffset += 4
	decq	I			# i --
	jg	.L61
	ALIGN_4

.L70:
	testq	$4, M
	je	.L80

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#endif

 	movaps	  0 * SIZE(AO), %xmm8
	movsldup  0 * SIZE(BO), %xmm9
 	movaps	 16 * SIZE(AO), %xmm10
	movsldup 16 * SIZE(BO), %xmm11

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
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
	addq	$4, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$3, %rax
	je	.L75
	ALIGN_4

.L72:
	mulps	%xmm8, %xmm9
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
	addps	%xmm9, %xmm0
	movshdup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
 	movaps	  4 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movsldup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movshdup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
 	movaps	  8 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm3
	movsldup  8 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movshdup  8 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
 	movaps	 12 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movsldup 12 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movshdup 12 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
 	movaps	 32 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm3
	movsldup 32 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm0
	movshdup 16 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
 	movaps	 20 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm1
	movsldup 20 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm2
	movshdup 20 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
 	movaps	 24 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm3
	movsldup 24 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm0
	movshdup 24 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
 	movaps	 28 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm1
	movsldup 28 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm2
	movshdup 28 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
 	movaps	 48 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm3
	movsldup 48 * SIZE(BO), %xmm11

	addq   $32 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L72
	ALIGN_4

.L75:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm15
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L78
	ALIGN_4

.L76:
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movshdup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
 	movaps	  4 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movsldup  4 * SIZE(BO), %xmm9

	addq	$4 * SIZE, AO
	addq	$4 * SIZE, BO
	decq	%rax
	jg	.L76
	ALIGN_4

.L78:
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm8
	movhps	2 * SIZE(CO1), %xmm8
	movsd	0 * SIZE(CO2), %xmm10
	movhps	2 * SIZE(CO2), %xmm10
#endif

	addps	%xmm2, %xmm0
	addps	%xmm3, %xmm1

	mulps	%xmm15, %xmm0
	mulps	%xmm15, %xmm1

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	addps	%xmm8,  %xmm0
	addps	%xmm10, %xmm1
#endif

	movsd	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 2 * SIZE(CO1)
	movsd	%xmm1, 0 * SIZE(CO2)
	movhps	%xmm1, 2 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax,    8), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	addq	$4 * SIZE, CO2		# coffset += 4
	ALIGN_4

.L80:
	testq	$2, M
	je	.L90

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
#endif

	movddup	  0 * SIZE(AO), %xmm8
	movddup	  8 * SIZE(AO), %xmm10
	movsd     0 * SIZE(BO), %xmm9
	movsd    16 * SIZE(BO), %xmm11

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1

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
	je	.L85
	ALIGN_4

.L82:
	shufps	  $0x50, %xmm9, %xmm9
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
	mulps	%xmm8, %xmm9
	movddup	  2 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movsd     4 * SIZE(BO), %xmm9
	shufps	  $0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
	movddup	  4 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movsd     8 * SIZE(BO), %xmm9
	shufps	  $0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
	movddup	  6 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movsd    12 * SIZE(BO), %xmm9
	shufps	  $0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
	movddup	 16 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movsd    32 * SIZE(BO), %xmm9
	shufps	  $0x50, %xmm11, %xmm11
	mulps	%xmm10, %xmm11
	movddup	 10 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm0
	movsd    20 * SIZE(BO), %xmm11
	shufps	  $0x50, %xmm11, %xmm11
	mulps	%xmm10, %xmm11
	movddup	 12 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm1
	movsd    24 * SIZE(BO), %xmm11
	shufps	  $0x50, %xmm11, %xmm11
	mulps	%xmm10, %xmm11
	movddup	 14 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm0
	movsd    28 * SIZE(BO), %xmm11
	shufps	  $0x50, %xmm11, %xmm11
	mulps	%xmm10, %xmm11
	movddup	 24 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm1
	movsd    48 * SIZE(BO), %xmm11

	addq   $16 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L82
	ALIGN_4

.L85:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm15
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L88
	ALIGN_4

.L86:
	shufps	  $0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
	movddup	  2 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movsd     4 * SIZE(BO), %xmm9

	addq	$2 * SIZE, AO
	addq	$4 * SIZE, BO
	decq	%rax
	jg	.L86
	ALIGN_4

.L88:
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm8
	movhps	0 * SIZE(CO2), %xmm8
#endif

	addps	%xmm1, %xmm0
	mulps	%xmm15, %xmm0
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	addps	%xmm8,  %xmm0
#endif

	movsd	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 0 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax,    8), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$2 * SIZE, CO1		# coffset += 4
	addq	$2 * SIZE, CO2		# coffset += 4
	ALIGN_4

.L90:
	testq	$1, M
	je	.L99

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   4), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

	movss	 0 * SIZE(AO), %xmm8
	movss	 4 * SIZE(AO), %xmm10
	movsd	 0 * SIZE(BO), %xmm9
	movsd	16 * SIZE(BO), %xmm11

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1

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
	je	.L95
	ALIGN_4

.L92:
	shufps	$0, %xmm8, %xmm8
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
	mulps	%xmm8, %xmm9
	movss	 1 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movsd	 4 * SIZE(BO), %xmm9
	shufps	$0, %xmm8, %xmm8
	mulps	%xmm8, %xmm9
	movss	 2 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movsd	 8 * SIZE(BO), %xmm9
	shufps	$0, %xmm8, %xmm8
	mulps	%xmm8, %xmm9
	movss	 3 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movsd	12 * SIZE(BO), %xmm9
	shufps	$0, %xmm8, %xmm8
	mulps	%xmm8, %xmm9
	movss	 8 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movsd	32 * SIZE(BO), %xmm9
	shufps	$0, %xmm10, %xmm10
	mulps	%xmm10, %xmm11
	movss	 5 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm0
	movsd	20 * SIZE(BO), %xmm11
	shufps	$0, %xmm10, %xmm10
	mulps	%xmm10, %xmm11
	movss	 6 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm1
	movsd	24 * SIZE(BO), %xmm11
	shufps	$0, %xmm10, %xmm10
	mulps	%xmm10, %xmm11
	movss	 7 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm0
	movsd	28 * SIZE(BO), %xmm11
	shufps	$0, %xmm10, %xmm10
	mulps	%xmm10, %xmm11
	movss	12 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm1
	movsd	48 * SIZE(BO), %xmm11

	addq   $ 8 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L92
	ALIGN_4

.L95:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm15
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L98
	ALIGN_4

.L96:
	shufps	$0, %xmm8, %xmm8
	mulps	%xmm8, %xmm9
	movss	 1 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movsd	 4 * SIZE(BO), %xmm9

	addq	$1 * SIZE, AO
	addq	$4 * SIZE, BO
	decq	%rax
	jg	.L96
	ALIGN_4

.L98:
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movss	0 * SIZE(CO1), %xmm8
	movss	0 * SIZE(CO2), %xmm9

	addps	%xmm1, %xmm0
	mulps	%xmm15, %xmm0
	addss	%xmm0, %xmm8
	psrlq	$32, %xmm0
	addss	%xmm0, %xmm9

	movss	%xmm8, 0 * SIZE(CO1)
	movss	%xmm9, 0 * SIZE(CO2)
#else
	addps	%xmm1, %xmm0
	mulps	%xmm15, %xmm0

	movss	%xmm0, 0 * SIZE(CO1)
	psrlq	$32, %xmm0
	movss	%xmm0, 0 * SIZE(CO2)
#endif

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax,    4), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif
	ALIGN_4

.L99:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif
	leaq	(C, LDC, 2), C		# c += 4 * ldc
	ALIGN_4

.L100:
	testq	$1, N
	je	.L999

.L101:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

/* Copying to Sub Buffer */
	leaq	BUFFER, BO

	movq	K, %rax
	sarq	$3, %rax
	jle	.L103
	ALIGN_4


.L102:
	movss	 0 * SIZE(B), %xmm0
	movss	 1 * SIZE(B), %xmm1
	movss	 2 * SIZE(B), %xmm2
	movss	 3 * SIZE(B), %xmm3
	movss	 4 * SIZE(B), %xmm4
	movss	 5 * SIZE(B), %xmm5
	movss	 6 * SIZE(B), %xmm6
	movss	 7 * SIZE(B), %xmm7

	movss	%xmm0,  0 * SIZE(BO)
	movss	%xmm0,  1 * SIZE(BO)
	movss	%xmm1,  2 * SIZE(BO)
	movss	%xmm1,  3 * SIZE(BO)
	movss	%xmm2,  4 * SIZE(BO)
	movss	%xmm2,  5 * SIZE(BO)
	movss	%xmm3,  6 * SIZE(BO)
	movss	%xmm3,  7 * SIZE(BO)
	movss	%xmm4,  8 * SIZE(BO)
	movss	%xmm4,  9 * SIZE(BO)
	movss	%xmm5, 10 * SIZE(BO)
	movss	%xmm5, 11 * SIZE(BO)
	movss	%xmm6, 12 * SIZE(BO)
	movss	%xmm6, 13 * SIZE(BO)
	movss	%xmm7, 14 * SIZE(BO)
	movss	%xmm7, 15 * SIZE(BO)

	addq	$ 8 * SIZE, B
	addq	$16 * SIZE, BO

	decq	%rax
	jne	.L102
	ALIGN_4

.L103:
	movq	K, %rax
	andq	$7, %rax
	BRANCH
	jle	.L110
	ALIGN_4

.L104:
	movss	 0 * SIZE(B), %xmm0
	movss	%xmm0,  0 * SIZE(BO)
	movss	%xmm0,  1 * SIZE(BO)

	addq	$ 1 * SIZE, B
	addq	$ 2 * SIZE, BO
	decq	%rax
	jne	.L104
	ALIGN_4

.L110:
	movq	C, CO1			# coffset1 = c
	movq	A, AO		# aoffset = a

	movq	M,  I
	sarq	$3, I	# i = (m >> 3)
	jle	.L120
	ALIGN_4

.L111:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 1), BO
#endif

 	movaps	  0 * SIZE(AO), %xmm8
 	movaps	 16 * SIZE(AO), %xmm10
 	movaps	 32 * SIZE(AO), %xmm12
 	movaps	 48 * SIZE(AO), %xmm14

	movddup   0 * SIZE(BO), %xmm9
	movddup   8 * SIZE(BO), %xmm11

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	prefetchnta      8 * SIZE(CO1)
	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$8, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$3, %rax
	je	.L115
	ALIGN_4

.L112:
	mulps	%xmm8, %xmm9
	movaps	  4 * SIZE(AO), %xmm8
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
	addps	%xmm9, %xmm0
	movddup   0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
 	movaps	  8 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm4
	movddup   2 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	 12 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movddup   2 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
 	movaps	 64 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm5
	movddup   4 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	movaps	 20 * SIZE(AO), %xmm10
	addps	%xmm9, %xmm0
	movddup   4 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
 	movaps	 24 * SIZE(AO), %xmm10
	addps	%xmm9, %xmm4
	movddup   6 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	movaps	 28 * SIZE(AO), %xmm10
	addps	%xmm9, %xmm1
	movddup   6 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
 	movaps	 80 * SIZE(AO), %xmm10
	addps	%xmm9, %xmm5
	PREFETCH  (PREFETCHSIZE + 32) * SIZE(AO)
	movddup   8 * SIZE(BO), %xmm9
	mulps	%xmm12, %xmm9
	movaps	 36 * SIZE(AO), %xmm12
	addps	%xmm9, %xmm0
	movddup  16 * SIZE(BO), %xmm9
	mulps	%xmm12, %xmm11
 	movaps	 40 * SIZE(AO), %xmm12
	addps	%xmm11, %xmm4
	movddup  10 * SIZE(BO), %xmm11
	mulps	%xmm12, %xmm11
	movaps	 44 * SIZE(AO), %xmm12
	addps	%xmm11, %xmm1
	movddup  10 * SIZE(BO), %xmm11
	mulps	%xmm12, %xmm11
 	movaps	 96 * SIZE(AO), %xmm12
	addps	%xmm11, %xmm5
	movddup  12 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm11
	movaps	 52 * SIZE(AO), %xmm14
	addps	%xmm11, %xmm0
	movddup  12 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm11
 	movaps	 56 * SIZE(AO), %xmm14
	addps	%xmm11, %xmm4
	movddup  14 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm11
	movaps	 60 * SIZE(AO), %xmm14
	addps	%xmm11, %xmm1
	movddup  14 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm11
 	movaps	112 * SIZE(AO), %xmm14
	addps	%xmm11, %xmm5
	movddup  24 * SIZE(BO), %xmm11

	addq   $64 * SIZE, AO
	addq   $16 * SIZE, BO
	decq   %rax
	jne    .L112
	ALIGN_4

.L115:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm15
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L118
	ALIGN_4

.L116:
	mulps	%xmm8, %xmm9
	movaps	  4 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movddup   0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
 	movaps	  8 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm4
	movddup   2 * SIZE(BO), %xmm9

	addq	$8 * SIZE, AO
	addq	$2 * SIZE, BO
	decq	%rax
	jg	.L116
	ALIGN_4

.L118:
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm8
	movhps	2 * SIZE(CO1), %xmm8
	movsd	4 * SIZE(CO1), %xmm9
	movhps	6 * SIZE(CO1), %xmm9
#endif

	addps	%xmm1, %xmm0
	addps	%xmm5, %xmm4

	mulps	%xmm15, %xmm0
	mulps	%xmm15, %xmm4
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	addps	%xmm8,  %xmm0
	addps	%xmm9,  %xmm4
#endif

	movsd	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 2 * SIZE(CO1)
	movsd	%xmm4, 4 * SIZE(CO1)
	movhps	%xmm4, 6 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax,    8), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 1), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$8, KK
#endif

	addq	$8 * SIZE, CO1		# coffset += 4
	decq	I			# i --
	jg	.L111
	ALIGN_4

.L120:
	testq	$4, M
	je	.L130


#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 1), BO
#endif

	movaps	 0 * SIZE(AO), %xmm8
	movaps	16 * SIZE(AO), %xmm10

	movaps	 0 * SIZE(BO), %xmm9
	movaps	16 * SIZE(BO), %xmm11

 	movaps	  0 * SIZE(AO), %xmm8
	movddup   0 * SIZE(BO), %xmm9
 	movaps	 16 * SIZE(AO), %xmm10
	movddup   8 * SIZE(BO), %xmm11

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1

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
	je	.L125
	ALIGN_4

.L122:
	mulps	%xmm8, %xmm9
 	movaps	  4 * SIZE(AO), %xmm8
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
	addps	%xmm9, %xmm0
	movddup   2 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
 	movaps	  8 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movddup   4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
 	movaps	 12 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movddup   6 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
 	movaps	 32 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movddup  16 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm11
 	movaps	 20 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm0
	movddup  10 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
 	movaps	 24 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm1
	movddup  12 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
 	movaps	 28 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm0
	movddup  14 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
 	movaps	 48 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm1
	movddup  24 * SIZE(BO), %xmm11

	addq   $32 * SIZE, AO
	addq   $16 * SIZE, BO
	decq   %rax
	jne    .L122
	ALIGN_4

.L125:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm15
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L128
	ALIGN_4

.L126:
	mulps	%xmm8, %xmm9
 	movaps	  4 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movddup   2 * SIZE(BO), %xmm9

	addq	$4 * SIZE, AO
	addq	$2 * SIZE, BO
	decq	%rax
	jg	.L126
	ALIGN_4

.L128:
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm8
	movhps	2 * SIZE(CO1), %xmm8
#endif

	addps	%xmm1, %xmm0
	mulps	%xmm15, %xmm0

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	addps	%xmm8,  %xmm0
#endif

	movsd	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 2 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax,    8), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 1), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	ALIGN_4

.L130:
	testq	$2, M
	je	.L140

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   8), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 1), BO
#endif

 	movaps	  0 * SIZE(AO), %xmm8
	movaps    0 * SIZE(BO), %xmm9
 	movaps	 16 * SIZE(AO), %xmm10
	movaps   16 * SIZE(BO), %xmm11

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
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
	sarq	$4, %rax
	je	.L135
	ALIGN_4

.L132:
	mulps	%xmm8, %xmm9
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
 	movaps	  4 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	mulps     4 * SIZE(BO), %xmm8
	addps	%xmm8, %xmm1
 	movaps	  8 * SIZE(AO), %xmm8
	mulps     8 * SIZE(BO), %xmm8
	addps	%xmm8, %xmm2
 	movaps	 12 * SIZE(AO), %xmm8
	mulps    12 * SIZE(BO), %xmm8
	addps	%xmm8, %xmm3
 	movaps	 32 * SIZE(AO), %xmm8
	movaps   32 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm11
 	movaps	 20 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm0
	movaps   48 * SIZE(BO), %xmm11
	mulps    20 * SIZE(BO), %xmm10
	addps	%xmm10, %xmm1
 	movaps	 24 * SIZE(AO), %xmm10
	mulps    24 * SIZE(BO), %xmm10
	addps	%xmm10, %xmm2
 	movaps	 28 * SIZE(AO), %xmm10
	mulps    28 * SIZE(BO), %xmm10
	addps	%xmm10, %xmm3
 	movaps	 48 * SIZE(AO), %xmm10

	addq   $32 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L132
	ALIGN_4

.L135:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA, %xmm15
	andq	$15, %rax		# if (k & 1)
	BRANCH
	je .L138
	ALIGN_4

.L136:
 	movsd	  0 * SIZE(AO), %xmm8
	movsd     0 * SIZE(BO), %xmm9

	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0

	addq	$2 * SIZE, AO
	addq	$2 * SIZE, BO
	decq	%rax
	jg	.L136
	ALIGN_4

.L138:
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm8
#endif
	addps	%xmm1, %xmm0
	addps	%xmm3, %xmm2
	addps	%xmm2, %xmm0

	movhlps	%xmm0, %xmm1
	addps	%xmm1, %xmm0

	mulps	%xmm15, %xmm0
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	addps	%xmm8,  %xmm0
#endif
	movsd	%xmm0, 0 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax,    8), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 1), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$2 * SIZE, CO1		# coffset += 4
	ALIGN_4

.L140:
	testq	$1, M
	je	.L999

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax,   4), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
#endif

	movss	 0 * SIZE(AO), %xmm8
	movss	 4 * SIZE(AO), %xmm10
	movss	 0 * SIZE(BO), %xmm9
	movss	 8 * SIZE(BO), %xmm11

	pxor	%xmm0, %xmm0
	pxor	%xmm1, %xmm1
	pxor	%xmm2, %xmm2
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
	je	.L145
	ALIGN_4

.L142:
	mulss	%xmm8, %xmm9
	PREFETCH	(PREFETCHSIZE +  0) * SIZE(AO)
	movss	 1 * SIZE(AO), %xmm8
	mulss	 2 * SIZE(BO), %xmm8
	addss	%xmm9, %xmm0
	movss	16 * SIZE(BO), %xmm9
	addss	%xmm8, %xmm1
	movss	 2 * SIZE(AO), %xmm8
	mulss	 4 * SIZE(BO), %xmm8
	addss	%xmm8, %xmm2
	movss	 3 * SIZE(AO), %xmm8
	mulss	 6 * SIZE(BO), %xmm8
	addss	%xmm8, %xmm3
	movss	 8 * SIZE(AO), %xmm8
	mulss	%xmm10, %xmm11
	movss	 5 * SIZE(AO), %xmm10
	mulss	10 * SIZE(BO), %xmm10
	addss	%xmm11, %xmm0
	movss	24 * SIZE(BO), %xmm11
	addss	%xmm10, %xmm1
	movss	 6 * SIZE(AO), %xmm10
	mulss	12 * SIZE(BO), %xmm10
	addss	%xmm10, %xmm2
	movss	 7 * SIZE(AO), %xmm10
	mulss	14 * SIZE(BO), %xmm10
	addss	%xmm10, %xmm3
	movss	12 * SIZE(AO), %xmm10

	addq   $ 8 * SIZE, AO
	addq   $16 * SIZE, BO
	decq   %rax
	jne    .L142
	ALIGN_4

.L145:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movss	ALPHA, %xmm15
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L148
	ALIGN_4

.L146:
 	movss	  0 * SIZE(AO), %xmm8
	movss     0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0

	addq	$1 * SIZE, AO
	addq	$2 * SIZE, BO
	decq	%rax
	jg	.L146
	ALIGN_4

.L148:
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movss	0 * SIZE(CO1), %xmm8
#endif
	addss	%xmm1, %xmm0
	addss	%xmm3, %xmm2
	addss	%xmm2, %xmm0

	mulss	%xmm15, %xmm0
#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	addss	%xmm8,  %xmm0
#endif
	movss	%xmm0, 0 * SIZE(CO1)
	ALIGN_4

.L999:
	movq	%rbx, %rsp
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
