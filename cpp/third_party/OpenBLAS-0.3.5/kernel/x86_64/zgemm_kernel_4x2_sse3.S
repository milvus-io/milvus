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

#else

#define STACKSIZE 256

#define OLD_ALPHA_I	40 + STACKSIZE(%rsp)
#define OLD_A		48 + STACKSIZE(%rsp)
#define OLD_B		56 + STACKSIZE(%rsp)
#define OLD_C		64 + STACKSIZE(%rsp)
#define OLD_LDC		72 + STACKSIZE(%rsp)
#define OLD_OFFSET	80 + STACKSIZE(%rsp)

#endif

#define ALPHA_R	  0(%rsp)
#define ALPHA_I	 16(%rsp)
#define J	 32(%rsp)
#define OFFSET	 40(%rsp)
#define KK	 48(%rsp)
#define KKK	 56(%rsp)
#define BUFFER	128(%rsp)

#define PREFETCH     prefetcht0
#define PREFETCHSIZE  320

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define ADDSUB	addps
#else
#define ADDSUB	subps
#endif

#define KERNEL1(address) \
	mulps	%xmm8, %xmm9; \
	PREFETCH  (PREFETCHSIZE +  0) * SIZE + (address) * 2 * SIZE(AO); \
	addps	%xmm9, %xmm0; \
	movshdup  0 * SIZE + (address) * 2 * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	ADDSUB	%xmm9, %xmm1; \
	movsldup  4 * SIZE + (address) * 2 * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	addps	%xmm9, %xmm2; \
	movshdup  4 * SIZE + (address) * 2 * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	movaps	  4 * SIZE + (address) * 2 * SIZE(AO), %xmm8; \
	ADDSUB	%xmm9, %xmm3; \
	movsldup  0 * SIZE + (address) * 2 * SIZE(BO), %xmm9

#define KERNEL2(address) \
	mulps	%xmm8, %xmm9; \
	addps	%xmm9, %xmm4; \
	movshdup  0 * SIZE + (address) * 2 * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	ADDSUB	%xmm9, %xmm5; \
	movsldup  4 * SIZE + (address) * 2 * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	addps	%xmm9, %xmm6; \
	movshdup  4 * SIZE + (address) * 2 * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	movaps	  8 * SIZE + (address) * 2 * SIZE(AO), %xmm8; \
	ADDSUB	%xmm9, %xmm7; \
	movsldup  8 * SIZE + (address) * 2 * SIZE(BO), %xmm9

#define KERNEL3(address) \
	mulps	%xmm8, %xmm9; \
	addps	%xmm9, %xmm0; \
	movshdup  8 * SIZE + (address) * 2 * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	ADDSUB	%xmm9, %xmm1; \
	movsldup 12 * SIZE + (address) * 2 * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	addps	%xmm9, %xmm2; \
	movshdup 12 * SIZE + (address) * 2 * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	movaps	 12 * SIZE + (address) * 2 * SIZE(AO), %xmm8; \
	ADDSUB	%xmm9, %xmm3; \
	movsldup  8 * SIZE + (address) * 2 * SIZE(BO), %xmm9

#define KERNEL4(address) \
	mulps	%xmm8, %xmm9; \
	addps	%xmm9, %xmm4; \
	movshdup  8 * SIZE + (address) * 2 * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	ADDSUB	%xmm9, %xmm5; \
	movsldup 12 * SIZE + (address) * 2 * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	addps	%xmm9, %xmm6; \
	movshdup 12 * SIZE + (address) * 2 * SIZE(BO), %xmm9; \
	mulps	%xmm8, %xmm9; \
	movaps	 64 * SIZE + (address) * 2 * SIZE(AO), %xmm8; \
	ADDSUB	%xmm9, %xmm7; \
	movsldup 64 * SIZE + (address) * 2 * SIZE(BO), %xmm9

#define KERNEL5(address) \
	mulps	%xmm10, %xmm11; \
	addps	%xmm11, %xmm0; \
	movshdup 16 * SIZE + (address) * 2 * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	ADDSUB	%xmm11, %xmm1; \
	movsldup 20 * SIZE + (address) * 2 * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	addps	%xmm11, %xmm2; \
	movshdup 20 * SIZE + (address) * 2 * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	movaps	 20 * SIZE + (address) * 2 * SIZE(AO), %xmm10; \
	ADDSUB	%xmm11, %xmm3; \
	movsldup 16 * SIZE + (address) * 2 * SIZE(BO), %xmm11

#define KERNEL6(address) \
	mulps	%xmm10, %xmm11; \
	addps	%xmm11, %xmm4; \
	movshdup 16 * SIZE + (address) * 2 * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	ADDSUB	%xmm11, %xmm5; \
	movsldup 20 * SIZE + (address) * 2 * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	addps	%xmm11, %xmm6; \
	movshdup 20 * SIZE + (address) * 2 * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	movaps	 24 * SIZE + (address) * 2 * SIZE(AO), %xmm10; \
	ADDSUB	%xmm11, %xmm7; \
	movsldup 24 * SIZE + (address) * 2 * SIZE(BO), %xmm11

#define KERNEL7(address) \
	mulps	%xmm10, %xmm11; \
	addps	%xmm11, %xmm0; \
	movshdup 24 * SIZE + (address) * 2 * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	ADDSUB	%xmm11, %xmm1; \
	movsldup 28 * SIZE + (address) * 2 * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	addps	%xmm11, %xmm2; \
	movshdup 28 * SIZE + (address) * 2 * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	movaps	 28 * SIZE + (address) * 2 * SIZE(AO), %xmm10; \
	ADDSUB	%xmm11, %xmm3; \
	movsldup 24 * SIZE + (address) * 2 * SIZE(BO), %xmm11

#define KERNEL8(address) \
	mulps	%xmm10, %xmm11; \
	addps	%xmm11, %xmm4; \
	movshdup 24 * SIZE + (address) * 2 * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	ADDSUB	%xmm11, %xmm5; \
	movsldup 28 * SIZE + (address) * 2 * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	addps	%xmm11, %xmm6; \
	movshdup 28 * SIZE + (address) * 2 * SIZE(BO), %xmm11; \
	mulps	%xmm10, %xmm11; \
	movaps	 80 * SIZE + (address) * 2 * SIZE(AO), %xmm10; \
	ADDSUB	%xmm11, %xmm7; \
	movsldup 80 * SIZE + (address) * 2 * SIZE(BO), %xmm11

#define KERNEL9(address) \
	mulps	%xmm12, %xmm13; \
	PREFETCH  (PREFETCHSIZE + 32) * SIZE + (address) * 2 * SIZE(AO); \
	addps	%xmm13, %xmm0; \
	movshdup 32 * SIZE + (address) * 2 * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	ADDSUB	%xmm13, %xmm1; \
	movsldup 36 * SIZE + (address) * 2 * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	addps	%xmm13, %xmm2; \
	movshdup 36 * SIZE + (address) * 2 * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	movaps	 36 * SIZE + (address) * 2 * SIZE(AO), %xmm12; \
	ADDSUB	%xmm13, %xmm3; \
	movsldup 32 * SIZE + (address) * 2 * SIZE(BO), %xmm13

#define KERNEL10(address) \
	mulps	%xmm12, %xmm13; \
	addps	%xmm13, %xmm4; \
	movshdup 32 * SIZE + (address) * 2 * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	ADDSUB	%xmm13, %xmm5; \
	movsldup 36 * SIZE + (address) * 2 * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	addps	%xmm13, %xmm6; \
	movshdup 36 * SIZE + (address) * 2 * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	movaps	 40 * SIZE + (address) * 2 * SIZE(AO), %xmm12; \
	ADDSUB	%xmm13, %xmm7; \
	movsldup 40 * SIZE + (address) * 2 * SIZE(BO), %xmm13

#define KERNEL11(address) \
	mulps	%xmm12, %xmm13; \
	addps	%xmm13, %xmm0; \
	movshdup 40 * SIZE + (address) * 2 * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	ADDSUB	%xmm13, %xmm1; \
	movsldup 44 * SIZE + (address) * 2 * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	addps	%xmm13, %xmm2; \
	movshdup 44 * SIZE + (address) * 2 * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	movaps	 44 * SIZE + (address) * 2 * SIZE(AO), %xmm12; \
	ADDSUB	%xmm13, %xmm3; \
	movsldup 40 * SIZE + (address) * 2 * SIZE(BO), %xmm13

#define KERNEL12(address) \
	mulps	%xmm12, %xmm13; \
	addps	%xmm13, %xmm4; \
	movshdup 40 * SIZE + (address) * 2 * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	ADDSUB	%xmm13, %xmm5; \
	movsldup 44 * SIZE + (address) * 2 * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
	addps	%xmm13, %xmm6; \
	movshdup 44 * SIZE + (address) * 2 * SIZE(BO), %xmm13; \
	mulps	%xmm12, %xmm13; \
 	movaps	 96 * SIZE + (address) * 2 * SIZE(AO), %xmm12; \
	ADDSUB	%xmm13, %xmm7; \
	movsldup 96 * SIZE + (address) * 2 * SIZE(BO), %xmm13

#define KERNEL13(address) \
	mulps	%xmm14, %xmm15; \
	addps	%xmm15, %xmm0; \
	movshdup 48 * SIZE + (address) * 2 * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	ADDSUB	%xmm15, %xmm1; \
	movsldup 52 * SIZE + (address) * 2 * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	addps	%xmm15, %xmm2; \
	movshdup 52 * SIZE + (address) * 2 * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	movaps	 52 * SIZE + (address) * 2 * SIZE(AO), %xmm14; \
	ADDSUB	%xmm15, %xmm3; \
	movsldup 48 * SIZE + (address) * 2 * SIZE(BO), %xmm15

#define KERNEL14(address) \
	mulps	%xmm14, %xmm15; \
	addps	%xmm15, %xmm4; \
	movshdup 48 * SIZE + (address) * 2 * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	ADDSUB	%xmm15, %xmm5; \
	movsldup 52 * SIZE + (address) * 2 * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	addps	%xmm15, %xmm6; \
	movshdup 52 * SIZE + (address) * 2 * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	movaps	 56 * SIZE + (address) * 2 * SIZE(AO), %xmm14; \
	ADDSUB	%xmm15, %xmm7; \
	movsldup 56 * SIZE + (address) * 2 * SIZE(BO), %xmm15

#define KERNEL15(address) \
	mulps	%xmm14, %xmm15; \
	addps	%xmm15, %xmm0; \
	movshdup 56 * SIZE + (address) * 2 * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	ADDSUB	%xmm15, %xmm1; \
	movsldup 60 * SIZE + (address) * 2 * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	addps	%xmm15, %xmm2; \
	movshdup 60 * SIZE + (address) * 2 * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	movaps	 60 * SIZE + (address) * 2 * SIZE(AO), %xmm14; \
	ADDSUB	%xmm15, %xmm3; \
	movsldup 56 * SIZE + (address) * 2 * SIZE(BO), %xmm15

#define KERNEL16(address) \
	mulps	%xmm14, %xmm15; \
	addps	%xmm15, %xmm4; \
	movshdup 56 * SIZE + (address) * 2 * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	ADDSUB	%xmm15, %xmm5; \
	movsldup 60 * SIZE + (address) * 2 * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	addps	%xmm15, %xmm6; \
	movshdup 60 * SIZE + (address) * 2 * SIZE(BO), %xmm15; \
	mulps	%xmm14, %xmm15; \
	movaps	112 * SIZE + (address) * 2 * SIZE(AO), %xmm14; \
	ADDSUB	%xmm15, %xmm7; \
	movsldup 112 * SIZE + (address) * 2 * SIZE(BO), %xmm15

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
	movaps	%xmm3,       %xmm0
	movsd	OLD_ALPHA_I, %xmm1
#else
	movq	72(%rsp), LDC
#ifdef TRMMKERNEL
	movsd	80(%rsp), %xmm4
#endif

#endif

	movq	%rsp, %rbx	# save old stack
	subq	$128 + LOCAL_BUFFER_SIZE, %rsp
	andq	$-4096, %rsp	# align stack

	STACK_TOUCHING

	pxor	%xmm15, %xmm15
	cmpeqps	%xmm15, %xmm15
	pslld	$31, %xmm15	# Generate mask
	pxor	%xmm2, %xmm2

	shufps	$0, %xmm0, %xmm0
	movaps	 %xmm0,  0 + ALPHA_R

	movss	 %xmm1,  4 + ALPHA_I
	movss	 %xmm1, 12 + ALPHA_I
	xorps	 %xmm15, %xmm1
	movss	 %xmm1,  0 + ALPHA_I
	movss	 %xmm1,  8 + ALPHA_I

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
	jle	.L40
	ALIGN_4

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
	movddup	  0 * SIZE(B), %xmm0
	movddup	  2 * SIZE(B), %xmm1
	movddup	  4 * SIZE(B), %xmm2
	movddup	  6 * SIZE(B), %xmm3
	movddup	  8 * SIZE(B), %xmm4
	movddup	 10 * SIZE(B), %xmm5
	movddup	 12 * SIZE(B), %xmm6
	movddup	 14 * SIZE(B), %xmm7

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
	sarq	$2, I		# i = (m >> 2)
	jle	.L20
	ALIGN_4

.L11:
	prefetcht0	 0 * SIZE(BB)
	subq	   $-8 * SIZE, BB

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
	pxor	%xmm0, %xmm0
	movaps	 16 * SIZE(AO), %xmm10
	pxor	%xmm1, %xmm1
	movaps	 32 * SIZE(AO), %xmm12
	pxor	%xmm2, %xmm2
	movaps	 48 * SIZE(AO), %xmm14
	pxor	%xmm3, %xmm3

	movsldup  0 * SIZE(BO), %xmm9
	pxor	%xmm4, %xmm4
	movsldup 16 * SIZE(BO), %xmm11
	pxor	%xmm5, %xmm5
	movsldup 32 * SIZE(BO), %xmm13
	pxor	%xmm6, %xmm6
	movsldup 48 * SIZE(BO), %xmm15
	pxor	%xmm7, %xmm7

	prefetchnta     8 * SIZE(CO1)
	prefetchnta     8 * SIZE(CO2)

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
#if 1
	andq	$-8, %rax
	salq	$4, %rax
	je	.L15

.L1X:
	KERNEL1 (32  *  0)
	KERNEL2 (32  *  0)
	KERNEL3 (32  *  0)
	KERNEL4 (32  *  0)
	KERNEL5 (32  *  0)
	KERNEL6 (32  *  0)
	KERNEL7 (32  *  0)
	KERNEL8 (32  *  0)
	KERNEL9 (32  *  0)
	KERNEL10(32  *  0)
	KERNEL11(32  *  0)
	KERNEL12(32  *  0)
	KERNEL13(32  *  0)
	KERNEL14(32  *  0)
	KERNEL15(32  *  0)
	KERNEL16(32  *  0)
	cmpq	$128 *  1, %rax
	jle	.L12
	KERNEL1 (32  *  1)
	KERNEL2 (32  *  1)
	KERNEL3 (32  *  1)
	KERNEL4 (32  *  1)
	KERNEL5 (32  *  1)
	KERNEL6 (32  *  1)
	KERNEL7 (32  *  1)
	KERNEL8 (32  *  1)
	KERNEL9 (32  *  1)
	KERNEL10(32  *  1)
	KERNEL11(32  *  1)
	KERNEL12(32  *  1)
	KERNEL13(32  *  1)
	KERNEL14(32  *  1)
	KERNEL15(32  *  1)
	KERNEL16(32  *  1)
	cmpq	$128 *  2, %rax
	jle	.L12
	KERNEL1 (32  *  2)
	KERNEL2 (32  *  2)
	KERNEL3 (32  *  2)
	KERNEL4 (32  *  2)
	KERNEL5 (32  *  2)
	KERNEL6 (32  *  2)
	KERNEL7 (32  *  2)
	KERNEL8 (32  *  2)
	KERNEL9 (32  *  2)
	KERNEL10(32  *  2)
	KERNEL11(32  *  2)
	KERNEL12(32  *  2)
	KERNEL13(32  *  2)
	KERNEL14(32  *  2)
	KERNEL15(32  *  2)
	KERNEL16(32  *  2)
	cmpq	$128 *  3, %rax
	jle	.L12
	KERNEL1 (32  *  3)
	KERNEL2 (32  *  3)
	KERNEL3 (32  *  3)
	KERNEL4 (32  *  3)
	KERNEL5 (32  *  3)
	KERNEL6 (32  *  3)
	KERNEL7 (32  *  3)
	KERNEL8 (32  *  3)
	KERNEL9 (32  *  3)
	KERNEL10(32  *  3)
	KERNEL11(32  *  3)
	KERNEL12(32  *  3)
	KERNEL13(32  *  3)
	KERNEL14(32  *  3)
	KERNEL15(32  *  3)
	KERNEL16(32  *  3)
	cmpq	$128 *  4, %rax
	jle	.L12
	KERNEL1 (32  *  4)
	KERNEL2 (32  *  4)
	KERNEL3 (32  *  4)
	KERNEL4 (32  *  4)
	KERNEL5 (32  *  4)
	KERNEL6 (32  *  4)
	KERNEL7 (32  *  4)
	KERNEL8 (32  *  4)
	KERNEL9 (32  *  4)
	KERNEL10(32  *  4)
	KERNEL11(32  *  4)
	KERNEL12(32  *  4)
	KERNEL13(32  *  4)
	KERNEL14(32  *  4)
	KERNEL15(32  *  4)
	KERNEL16(32  *  4)
	cmpq	$128 *  5, %rax
	jle	.L12
	KERNEL1 (32  *  5)
	KERNEL2 (32  *  5)
	KERNEL3 (32  *  5)
	KERNEL4 (32  *  5)
	KERNEL5 (32  *  5)
	KERNEL6 (32  *  5)
	KERNEL7 (32  *  5)
	KERNEL8 (32  *  5)
	KERNEL9 (32  *  5)
	KERNEL10(32  *  5)
	KERNEL11(32  *  5)
	KERNEL12(32  *  5)
	KERNEL13(32  *  5)
	KERNEL14(32  *  5)
	KERNEL15(32  *  5)
	KERNEL16(32  *  5)
	cmpq	$128 *  6, %rax
	jle	.L12
	KERNEL1 (32  *  6)
	KERNEL2 (32  *  6)
	KERNEL3 (32  *  6)
	KERNEL4 (32  *  6)
	KERNEL5 (32  *  6)
	KERNEL6 (32  *  6)
	KERNEL7 (32  *  6)
	KERNEL8 (32  *  6)
	KERNEL9 (32  *  6)
	KERNEL10(32  *  6)
	KERNEL11(32  *  6)
	KERNEL12(32  *  6)
	KERNEL13(32  *  6)
	KERNEL14(32  *  6)
	KERNEL15(32  *  6)
	KERNEL16(32  *  6)
	cmpq	$128 *  7, %rax
	jle	.L12
	KERNEL1 (32  *  7)
	KERNEL2 (32  *  7)
	KERNEL3 (32  *  7)
	KERNEL4 (32  *  7)
	KERNEL5 (32  *  7)
	KERNEL6 (32  *  7)
	KERNEL7 (32  *  7)
	KERNEL8 (32  *  7)
	KERNEL9 (32  *  7)
	KERNEL10(32  *  7)
	KERNEL11(32  *  7)
	KERNEL12(32  *  7)
	KERNEL13(32  *  7)
	KERNEL14(32  *  7)
	KERNEL15(32  *  7)
	KERNEL16(32  *  7)

	addq	$64 * 8  * SIZE, AO
	addq	$64 * 8  * SIZE, BO
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
	KERNEL1 (32  *  0)
	KERNEL2 (32  *  0)
	KERNEL3 (32  *  0)
	KERNEL4 (32  *  0)
	KERNEL5 (32  *  0)
	KERNEL6 (32  *  0)
	KERNEL7 (32  *  0)
	KERNEL8 (32  *  0)
	KERNEL9 (32  *  0)
	KERNEL10(32  *  0)
	KERNEL11(32  *  0)
	KERNEL12(32  *  0)
	KERNEL13(32  *  0)
	KERNEL14(32  *  0)
	KERNEL15(32  *  0)
	KERNEL16(32  *  0)

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
	movaps	ALPHA_R, %xmm14
	movaps	ALPHA_I, %xmm15
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L18
	ALIGN_4

.L16:
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movshdup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	ADDSUB	%xmm9, %xmm1
	movsldup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movshdup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	  4 * SIZE(AO), %xmm8
	ADDSUB	%xmm9, %xmm3
	movsldup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm4
	movshdup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	ADDSUB	%xmm9, %xmm5
	movsldup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm6
	movshdup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	  8 * SIZE(AO), %xmm8
	ADDSUB	%xmm9, %xmm7
	movsldup  8 * SIZE(BO), %xmm9

	addq	$8 * SIZE, AO
	addq	$8 * SIZE, BO
	decq	%rax
	jg	.L16
	ALIGN_4

.L18:
#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

	shufps	$0xb1, %xmm1, %xmm1
	shufps	$0xb1, %xmm3, %xmm3
	shufps	$0xb1, %xmm5, %xmm5
	shufps	$0xb1, %xmm7, %xmm7

	addsubps	%xmm1, %xmm0
	addsubps	%xmm3, %xmm2
	addsubps	%xmm5, %xmm4
	addsubps	%xmm7, %xmm6

	movaps	%xmm0, %xmm1
	movaps	%xmm2, %xmm3
	movaps	%xmm4, %xmm5
	movaps	%xmm6, %xmm7

	shufps	$0xb1, %xmm0, %xmm0
	shufps	$0xb1, %xmm2, %xmm2
	shufps	$0xb1, %xmm4, %xmm4
	shufps	$0xb1, %xmm6, %xmm6
#else
	shufps	$0xb1, %xmm0, %xmm0
	shufps	$0xb1, %xmm2, %xmm2
	shufps	$0xb1, %xmm4, %xmm4
	shufps	$0xb1, %xmm6, %xmm6

	addsubps	%xmm0, %xmm1
	addsubps	%xmm2, %xmm3
	addsubps	%xmm4, %xmm5
	addsubps	%xmm6, %xmm7

	movaps	%xmm1, %xmm0
	movaps	%xmm3, %xmm2
	movaps	%xmm5, %xmm4
	movaps	%xmm7, %xmm6

	shufps	$0xb1, %xmm1, %xmm1
	shufps	$0xb1, %xmm3, %xmm3
	shufps	$0xb1, %xmm5, %xmm5
	shufps	$0xb1, %xmm7, %xmm7
#endif

	mulps	%xmm14, %xmm1
	mulps	%xmm15, %xmm0
	mulps	%xmm14, %xmm3
	mulps	%xmm15, %xmm2

	mulps	%xmm14, %xmm5
	mulps	%xmm15, %xmm4
	mulps	%xmm14, %xmm7
	mulps	%xmm15, %xmm6

	addps	%xmm1, %xmm0
	addps	%xmm3, %xmm2
	addps	%xmm5, %xmm4
	addps	%xmm7, %xmm6

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	shufps	$0xe4, %xmm8,  %xmm8
	shufps	$0xe4, %xmm9,  %xmm9
	shufps	$0xe4, %xmm10, %xmm10
	shufps	$0xe4, %xmm11, %xmm11

	movsd	0 * SIZE(CO1), %xmm8
	movhps	2 * SIZE(CO1), %xmm8
	movsd	4 * SIZE(CO1), %xmm10
	movhps	6 * SIZE(CO1), %xmm10

	movsd	0 * SIZE(CO2), %xmm9
	movhps	2 * SIZE(CO2), %xmm9
	movsd	4 * SIZE(CO2), %xmm11
	movhps	6 * SIZE(CO2), %xmm11

	addps	%xmm8,  %xmm0
	addps	%xmm9,  %xmm2
	addps	%xmm10, %xmm4
	addps	%xmm11, %xmm6
#endif

	movsd	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 2 * SIZE(CO1)
	movsd	%xmm4, 4 * SIZE(CO1)
	movhps	%xmm4, 6 * SIZE(CO1)

	movsd	%xmm2, 0 * SIZE(CO2)
	movhps	%xmm2, 2 * SIZE(CO2)
	movsd	%xmm6, 4 * SIZE(CO2)
	movhps	%xmm6, 6 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, 8), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$8 * SIZE, CO1		# coffset += 4
	addq	$8 * SIZE, CO2		# coffset += 4
	decq	I			# i --
	jg	.L11
	ALIGN_4

.L20:
	testq	$2, M
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
	pxor	%xmm0, %xmm0
	movaps	 16 * SIZE(AO), %xmm10
	pxor	%xmm1, %xmm1

	movsldup  0 * SIZE(BO), %xmm9
	pxor	%xmm2, %xmm2
	movsldup 16 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3
	movsldup 32 * SIZE(BO), %xmm13
	movsldup 48 * SIZE(BO), %xmm15

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
	je	.L25
	ALIGN_4

.L22:
	mulps	%xmm8, %xmm9
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
	addps	%xmm9, %xmm0
	movshdup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	ADDSUB	%xmm9, %xmm1
	movsldup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movshdup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	  4 * SIZE(AO), %xmm8
	ADDSUB	%xmm9, %xmm3
	movsldup  8 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movshdup  8 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	ADDSUB	%xmm9, %xmm1
	movsldup 12 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movshdup 12 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	  8 * SIZE(AO), %xmm8
	ADDSUB	%xmm9, %xmm3
	movsldup 64 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm0
	movshdup 16 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	ADDSUB	%xmm11, %xmm1
	movsldup 20 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm2
	movshdup 20 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	movaps	 12 * SIZE(AO), %xmm8
	ADDSUB	%xmm11, %xmm3
	movsldup 24 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm0
	movshdup 24 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	ADDSUB	%xmm11, %xmm1
	movsldup 28 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm2
	movshdup 28 * SIZE(BO), %xmm11
	mulps	%xmm8, %xmm11
	movaps	 32 * SIZE(AO), %xmm8
	ADDSUB	%xmm11, %xmm3
	movsldup 80 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm0
	movshdup 32 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	ADDSUB	%xmm13, %xmm1
	movsldup 36 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm2
	movshdup 36 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	movaps	 20 * SIZE(AO), %xmm10
	ADDSUB	%xmm13, %xmm3
	movsldup 40 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm0
	movshdup 40 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	ADDSUB	%xmm13, %xmm1
	movsldup 44 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm2
	movshdup 44 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm13
	movaps	 24 * SIZE(AO), %xmm10
	ADDSUB	%xmm13, %xmm3
	movsldup 96 * SIZE(BO), %xmm13
	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm0
	movshdup 48 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	ADDSUB	%xmm15, %xmm1
	movsldup 52 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm2
	movshdup 52 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	movaps	 28 * SIZE(AO), %xmm10
	ADDSUB	%xmm15, %xmm3
	movsldup 56 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm0
	movshdup 56 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	ADDSUB	%xmm15, %xmm1
	movsldup 60 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm2
	movshdup 60 * SIZE(BO), %xmm15
	mulps	%xmm10, %xmm15
	movaps	 48 * SIZE(AO), %xmm10
	ADDSUB	%xmm15, %xmm3
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
	movaps	ALPHA_R, %xmm14
	movaps	ALPHA_I, %xmm15
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L28
	ALIGN_4

.L26:
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movshdup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	ADDSUB	%xmm9, %xmm1
	movsldup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm2
	movshdup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	  4 * SIZE(AO), %xmm8
	ADDSUB	%xmm9, %xmm3
	movsldup  8 * SIZE(BO), %xmm9

	addq	$ 4 * SIZE, AO
	addq	$ 8 * SIZE, BO
	decq	%rax
	jg	.L26
	ALIGN_4

.L28:
#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

	shufps	$0xb1, %xmm1, %xmm1
	shufps	$0xb1, %xmm3, %xmm3

	addsubps	%xmm1, %xmm0
	addsubps	%xmm3, %xmm2

	movaps	%xmm0, %xmm1
	movaps	%xmm2, %xmm3

	shufps	$0xb1, %xmm0, %xmm0
	shufps	$0xb1, %xmm2, %xmm2
#else
	shufps	$0xb1, %xmm0, %xmm0
	shufps	$0xb1, %xmm2, %xmm2

	addsubps	%xmm0, %xmm1
	addsubps	%xmm2, %xmm3

	movaps	%xmm1, %xmm0
	movaps	%xmm3, %xmm2

	shufps	$0xb1, %xmm1, %xmm1
	shufps	$0xb1, %xmm3, %xmm3
#endif

	mulps	%xmm14, %xmm1
	mulps	%xmm15, %xmm0
	mulps	%xmm14, %xmm3
	mulps	%xmm15, %xmm2

	addps	%xmm1, %xmm0
	addps	%xmm3, %xmm2

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	shufps	$0xe4, %xmm8,  %xmm8
	shufps	$0xe4, %xmm10, %xmm10

	movsd	0 * SIZE(CO1), %xmm8
	movhps	2 * SIZE(CO1), %xmm8
	movsd	0 * SIZE(CO2), %xmm10
	movhps	2 * SIZE(CO2), %xmm10

	addps	%xmm8,  %xmm0
	addps	%xmm10, %xmm2
#endif

	movsd	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 2 * SIZE(CO1)
	movsd	%xmm2, 0 * SIZE(CO2)
	movhps	%xmm2, 2 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax,    8), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	addq	$4 * SIZE, CO2		# coffset += 4
	ALIGN_4

.L30:
	testq	$1, M
	je	.L39

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
	pxor	%xmm0, %xmm0
 	movddup	  8 * SIZE(AO), %xmm10
	pxor	%xmm1, %xmm1
	movsd     0 * SIZE(BO), %xmm9
	pxor	%xmm2, %xmm2
	movsd    16 * SIZE(BO), %xmm11
	pxor	%xmm3, %xmm3
	movsd    32 * SIZE(BO), %xmm13
	movsd    48 * SIZE(BO), %xmm15

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
	je	.L35
	ALIGN_4

.L32:
	shufps	$0x50, %xmm9, %xmm9
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movsd     4 * SIZE(BO), %xmm9
	shufps	$0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
 	movddup	  2 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movsd     8 * SIZE(BO), %xmm9
	shufps	$0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movsd    12 * SIZE(BO), %xmm9
	shufps	$0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
 	movddup	  4 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movsd    64 * SIZE(BO), %xmm9
	shufps	$0x50, %xmm11, %xmm11
	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm0
	movsd    20 * SIZE(BO), %xmm11
	shufps	$0x50, %xmm11, %xmm11
	mulps	%xmm8, %xmm11
 	movddup	  6 * SIZE(AO), %xmm8
	addps	%xmm11, %xmm1
	movsd    24 * SIZE(BO), %xmm11
	shufps	$0x50, %xmm11, %xmm11
	mulps	%xmm8, %xmm11
	addps	%xmm11, %xmm0
	movsd    28 * SIZE(BO), %xmm11
	shufps	$0x50, %xmm11, %xmm11
	mulps	%xmm8, %xmm11
 	movddup	 16 * SIZE(AO), %xmm8
	addps	%xmm11, %xmm1
	movsd    80 * SIZE(BO), %xmm11
	shufps	$0x50, %xmm13, %xmm13
	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm0
	movsd    36 * SIZE(BO), %xmm13
	shufps	$0x50, %xmm13, %xmm13
	mulps	%xmm10, %xmm13
 	movddup	 10 * SIZE(AO), %xmm10
	addps	%xmm13, %xmm1
	movsd    40 * SIZE(BO), %xmm13
	shufps	$0x50, %xmm13, %xmm13
	mulps	%xmm10, %xmm13
	addps	%xmm13, %xmm0
	movsd    44 * SIZE(BO), %xmm13
	shufps	$0x50, %xmm13, %xmm13
	mulps	%xmm10, %xmm13
 	movddup	 12 * SIZE(AO), %xmm10
	addps	%xmm13, %xmm1
	movsd    96 * SIZE(BO), %xmm13
	shufps	$0x50, %xmm15, %xmm15
	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm0
	movsd    52 * SIZE(BO), %xmm15
	shufps	$0x50, %xmm15, %xmm15
	mulps	%xmm10, %xmm15
 	movddup	 14 * SIZE(AO), %xmm10
	addps	%xmm15, %xmm1
	movsd    56 * SIZE(BO), %xmm15
	shufps	$0x50, %xmm15, %xmm15
	mulps	%xmm10, %xmm15
	addps	%xmm15, %xmm0
	movsd    60 * SIZE(BO), %xmm15
	shufps	$0x50, %xmm15, %xmm15
	mulps	%xmm10, %xmm15
  	movddup	 24 * SIZE(AO), %xmm10
	addps	%xmm15, %xmm1
	movsd   112 * SIZE(BO), %xmm15

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
	movaps	ALPHA_R, %xmm14
	movaps	ALPHA_I, %xmm15
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L38
	ALIGN_4

.L36:
	shufps	$0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movsd     4 * SIZE(BO), %xmm9
	shufps	$0x50, %xmm9, %xmm9
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
	movaps	%xmm0, %xmm6
	movlhps	%xmm1, %xmm0
	movhlps %xmm6, %xmm1

#if  defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
     defined(RR) || defined(RC) || defined(CR) || defined(CC)
	cmpeqps	%xmm7, %xmm7
	pslld	$31,   %xmm7
	xorps	%xmm7, %xmm1
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	shufps	$0xb1, %xmm1, %xmm1

	addsubps	%xmm1, %xmm0

	movaps	%xmm0, %xmm1

	shufps	$0xb1, %xmm0, %xmm0
#else
	shufps	$0xb1, %xmm0, %xmm0

	addsubps	%xmm0, %xmm1

	movaps	%xmm1, %xmm0

	shufps	$0xb1, %xmm1, %xmm1
#endif

	mulps	%xmm14, %xmm1
	mulps	%xmm15, %xmm0

	addps	%xmm1, %xmm0

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm8
	movhps	0 * SIZE(CO2), %xmm8

	addps	%xmm8, %xmm0
#endif

	movsd	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 0 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax,    8), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif
	ALIGN_4

.L39:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	leaq	(C, LDC, 2), C		# c += 2 * ldc
	decq	J			# j --
	jg	.L01
	ALIGN_4

.L40:
	testq	$1, N
	je	.L999
	ALIGN_4

.L41:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

/* Copying to Sub Buffer */
	leaq	BUFFER, BO

	movq	K, %rax
	sarq	$3, %rax
	jle	.L43
	ALIGN_4

.L42:
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
	jne	.L42
	ALIGN_4

.L43:
	movq	K, %rax
	andq	$7, %rax
	BRANCH
	jle	.L50
	ALIGN_4

.L44:
	movddup	 0 * SIZE(B), %xmm0

	movaps	%xmm0,  0 * SIZE(BO)

	addq	$2 * SIZE, B
	addq	$4 * SIZE, BO
	decq	%rax
	jne	.L44
	ALIGN_4

.L50:
	movq	C, CO1			# coffset1 = c
	movq	A, AO		# aoffset = a

	movq	M,  I
	sarq	$2, I		# i = (m >> 2)
	jle	.L60
	ALIGN_4

.L51:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, 8), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

	movaps	  0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movaps	 16 * SIZE(AO), %xmm10
	pxor	%xmm1, %xmm1
	movaps	 32 * SIZE(AO), %xmm12
	pxor	%xmm4, %xmm4
	movaps	 48 * SIZE(AO), %xmm14
	pxor	%xmm5, %xmm5

	movsldup  0 * SIZE(BO), %xmm9
	movsldup 16 * SIZE(BO), %xmm11

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
	addq	$4, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$3, %rax
	je	.L55
	ALIGN_4

.L52:
	mulps	%xmm8, %xmm9
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
	addps	%xmm9, %xmm0
	movshdup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	  4 * SIZE(AO), %xmm8
	ADDSUB	%xmm9, %xmm1
	movsldup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm4
	movshdup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	  8 * SIZE(AO), %xmm8
	ADDSUB	%xmm9, %xmm5
	movsldup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movshdup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	 12 * SIZE(AO), %xmm8
	ADDSUB	%xmm9, %xmm1
	movsldup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm4
	movshdup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	 64 * SIZE(AO), %xmm8
	ADDSUB	%xmm9, %xmm5
	movsldup  8 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm0
	movshdup  8 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	movaps	 20 * SIZE(AO), %xmm10
	ADDSUB	%xmm9, %xmm1
	movsldup  8 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm4
	movshdup  8 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	movaps	 24 * SIZE(AO), %xmm10
	ADDSUB	%xmm9, %xmm5
	movsldup 12 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm0
	movshdup 12 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	movaps	 28 * SIZE(AO), %xmm10
	ADDSUB	%xmm9, %xmm1
	movsldup 12 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	addps	%xmm9, %xmm4
	movshdup 12 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm9
	movaps	 80 * SIZE(AO), %xmm10
	ADDSUB	%xmm9, %xmm5
	movsldup 32 * SIZE(BO), %xmm9
	mulps	%xmm12, %xmm11
	PREFETCH  (PREFETCHSIZE + 32) * SIZE(AO)
	addps	%xmm11, %xmm0
	movshdup 16 * SIZE(BO), %xmm11
	mulps	%xmm12, %xmm11
	movaps	 36 * SIZE(AO), %xmm12
	ADDSUB	%xmm11, %xmm1
	movsldup 16 * SIZE(BO), %xmm11
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm4
	movshdup 16 * SIZE(BO), %xmm11
	mulps	%xmm12, %xmm11
	movaps	 40 * SIZE(AO), %xmm12
	ADDSUB	%xmm11, %xmm5
	movsldup 20 * SIZE(BO), %xmm11
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm0
	movshdup 20 * SIZE(BO), %xmm11
	mulps	%xmm12, %xmm11
	movaps	 44 * SIZE(AO), %xmm12
	ADDSUB	%xmm11, %xmm1
	movsldup 20 * SIZE(BO), %xmm11
	mulps	%xmm12, %xmm11
	addps	%xmm11, %xmm4
	movshdup 20 * SIZE(BO), %xmm11
	mulps	%xmm12, %xmm11
	movaps	 96 * SIZE(AO), %xmm12
	ADDSUB	%xmm11, %xmm5
	movsldup 24 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm11
	addps	%xmm11, %xmm0
	movshdup 24 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm11
	movaps	 52 * SIZE(AO), %xmm14
	ADDSUB	%xmm11, %xmm1
	movsldup 24 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm11
	addps	%xmm11, %xmm4
	movshdup 24 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm11
	movaps	 56 * SIZE(AO), %xmm14
	ADDSUB	%xmm11, %xmm5
	movsldup 28 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm11
	addps	%xmm11, %xmm0
	movshdup 28 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm11
	movaps	 60 * SIZE(AO), %xmm14
	ADDSUB	%xmm11, %xmm1
	movsldup 28 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm11
	addps	%xmm11, %xmm4
	movshdup 28 * SIZE(BO), %xmm11
	mulps	%xmm14, %xmm11
	movaps	112 * SIZE(AO), %xmm14
	ADDSUB	%xmm11, %xmm5
	movsldup 48 * SIZE(BO), %xmm11

	addq   $64 * SIZE, AO
	addq   $32 * SIZE, BO
	decq   %rax
	jne    .L52
	ALIGN_4

.L55:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	movaps	ALPHA_R, %xmm14
	movaps	ALPHA_I, %xmm15
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L58
	ALIGN_4

.L56:
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movshdup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	  4 * SIZE(AO), %xmm8
	ADDSUB	%xmm9, %xmm1
	movsldup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm4
	movshdup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	  8 * SIZE(AO), %xmm8
	ADDSUB	%xmm9, %xmm5
	movsldup  4 * SIZE(BO), %xmm9

	addq	$ 8 * SIZE, AO
	addq	$ 4 * SIZE, BO
	decq	%rax
	jg	.L56
	ALIGN_4

.L58:
#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

	shufps	$0xb1, %xmm1, %xmm1
	shufps	$0xb1, %xmm5, %xmm5

	addsubps	%xmm1, %xmm0
	addsubps	%xmm5, %xmm4

	movaps	%xmm0, %xmm1
	movaps	%xmm4, %xmm5

	shufps	$0xb1, %xmm0, %xmm0
	shufps	$0xb1, %xmm4, %xmm4
#else
	shufps	$0xb1, %xmm0, %xmm0
	shufps	$0xb1, %xmm4, %xmm4

	addsubps	%xmm0, %xmm1
	addsubps	%xmm4, %xmm5

	movaps	%xmm1, %xmm0
	movaps	%xmm5, %xmm4

	shufps	$0xb1, %xmm1, %xmm1
	shufps	$0xb1, %xmm5, %xmm5
#endif

	mulps	%xmm14, %xmm1
	mulps	%xmm15, %xmm0
	mulps	%xmm14, %xmm5
	mulps	%xmm15, %xmm4

	addps	%xmm1, %xmm0
	addps	%xmm5, %xmm4

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm8
	movhps	2 * SIZE(CO1), %xmm8
	movsd	4 * SIZE(CO1), %xmm9
	movhps	6 * SIZE(CO1), %xmm9

	addps	%xmm8, %xmm0
	addps	%xmm9, %xmm4
#endif

	movsd	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 2 * SIZE(CO1)
	movsd	%xmm4, 4 * SIZE(CO1)
	movhps	%xmm4, 6 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, 8), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$8 * SIZE, CO1		# coffset += 4
	decq	I			# i --
	jg	.L51
	ALIGN_4

.L60:
	testq	$2, M
	je	.L70

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, 8), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#endif

	movaps	  0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movsldup  0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
	movaps	 16 * SIZE(AO), %xmm10
	movsldup 16 * SIZE(BO), %xmm11

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
	je	.L65
	ALIGN_4

.L62:
	mulps	%xmm8, %xmm9
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
	addps	%xmm9, %xmm0
	movshdup  0 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	  4 * SIZE(AO), %xmm8
	ADDSUB	%xmm9, %xmm1
	movsldup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movshdup  4 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	  8 * SIZE(AO), %xmm8
	ADDSUB	%xmm9, %xmm1
	movsldup  8 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movshdup  8 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	 12 * SIZE(AO), %xmm8
	ADDSUB	%xmm9, %xmm1
	movsldup 12 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	addps	%xmm9, %xmm0
	movshdup 12 * SIZE(BO), %xmm9
	mulps	%xmm8, %xmm9
	movaps	 32 * SIZE(AO), %xmm8
	ADDSUB	%xmm9, %xmm1
	movsldup 32 * SIZE(BO), %xmm9
	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm0
	movshdup 16 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	movaps	 20 * SIZE(AO), %xmm10
	ADDSUB	%xmm11, %xmm1
	movsldup 20 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm0
	movshdup 20 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	movaps	 24 * SIZE(AO), %xmm10
	ADDSUB	%xmm11, %xmm1
	movsldup 24 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm0
	movshdup 24 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	movaps	 28 * SIZE(AO), %xmm10
	ADDSUB	%xmm11, %xmm1
	movsldup 28 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	addps	%xmm11, %xmm0
	movshdup 28 * SIZE(BO), %xmm11
	mulps	%xmm10, %xmm11
	movaps	 48 * SIZE(AO), %xmm10
	ADDSUB	%xmm11, %xmm1
	movsldup 48 * SIZE(BO), %xmm11

	addq   $32 * SIZE, AO
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
	movaps	ALPHA_R, %xmm14
	movaps	ALPHA_I, %xmm15
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
	ADDSUB	%xmm9, %xmm1
	movsldup  4 * SIZE(BO), %xmm9

	addq	$4 * SIZE, AO		# aoffset  += 4
	addq	$4 * SIZE, BO		# boffset1 += 8
	decq	%rax
	jg	.L66
	ALIGN_4

.L68:
#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	shufps	$0xb1, %xmm1, %xmm1
	addsubps	%xmm1, %xmm0
	movaps	%xmm0, %xmm1
	shufps	$0xb1, %xmm0, %xmm0
#else
	shufps	$0xb1, %xmm0, %xmm0
	addsubps	%xmm0, %xmm1
	movaps	%xmm1, %xmm0
	shufps	$0xb1, %xmm1, %xmm1
#endif

	mulps	%xmm14, %xmm1
	mulps	%xmm15, %xmm0
	addps	%xmm1, %xmm0

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm8
	movhps	2 * SIZE(CO1), %xmm8

	addps	%xmm8, %xmm0
#endif

	movsd	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 2 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax,  8), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif
	addq	$4 * SIZE, CO1		# coffset += 4
	ALIGN_4

.L70:
	testq	$1, M
	je	.L999

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leaq	BUFFER, BO
#else
	leaq	BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, 8), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
#endif

 	movddup	  0 * SIZE(AO), %xmm8
	pxor	%xmm0, %xmm0
	movsd     0 * SIZE(BO), %xmm9
	pxor	%xmm1, %xmm1
 	movddup	  8 * SIZE(AO), %xmm10
	movsd    16 * SIZE(BO), %xmm11

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
	je	.L75
	ALIGN_4

.L72:
	shufps	$0x50, %xmm9, %xmm9
	PREFETCH  (PREFETCHSIZE +  0) * SIZE(AO)
	mulps	%xmm8, %xmm9
 	movddup	  2 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movsd     4 * SIZE(BO), %xmm9
	shufps	$0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
 	movddup	  4 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movsd     8 * SIZE(BO), %xmm9
	shufps	$0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
 	movddup	  6 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movsd    12 * SIZE(BO), %xmm9
	shufps	$0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
 	movddup	 16 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm1
	movsd    32 * SIZE(BO), %xmm9
	shufps	$0x50, %xmm11, %xmm11
	mulps	%xmm10, %xmm11
 	movddup	 10 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm0
	movsd    20 * SIZE(BO), %xmm11
	shufps	$0x50, %xmm11, %xmm11
	mulps	%xmm10, %xmm11
 	movddup	 12 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm1
	movsd    24 * SIZE(BO), %xmm11
	shufps	$0x50, %xmm11, %xmm11
	mulps	%xmm10, %xmm11
 	movddup	 14 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm0
	movsd    28 * SIZE(BO), %xmm11
	shufps	$0x50, %xmm11, %xmm11
	mulps	%xmm10, %xmm11
 	movddup	 24 * SIZE(AO), %xmm10
	addps	%xmm11, %xmm1
	movsd    48 * SIZE(BO), %xmm11

	addq   $16 * SIZE, AO
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
	movaps	ALPHA_R, %xmm14
	movaps	ALPHA_I, %xmm15
	andq	$7, %rax		# if (k & 1)
	BRANCH
	je .L78
	ALIGN_4

.L76:
	shufps	$0x50, %xmm9, %xmm9
	mulps	%xmm8, %xmm9
 	movddup	  2 * SIZE(AO), %xmm8
	addps	%xmm9, %xmm0
	movsd     4 * SIZE(BO), %xmm9

	addq	$2 * SIZE, AO
	addq	$4 * SIZE, BO
	decq	%rax
	jg	.L76
	ALIGN_4

.L78:
	addps	%xmm1, %xmm0

	movhlps	%xmm0, %xmm1

#if  defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
     defined(RR) || defined(RC) || defined(CR) || defined(CC)
	cmpeqps	%xmm7, %xmm7
	pslld	$31,   %xmm7
	xorps	%xmm7, %xmm1
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	shufps	$0xb1, %xmm1, %xmm1

	addsubps	%xmm1, %xmm0

	movaps	%xmm0, %xmm1

	shufps	$0xb1, %xmm0, %xmm0
#else
	shufps	$0xb1, %xmm0, %xmm0

	addsubps	%xmm0, %xmm1

	movaps	%xmm1, %xmm0

	shufps	$0xb1, %xmm1, %xmm1
#endif

	mulps	%xmm14, %xmm1
	mulps	%xmm15, %xmm0

	addps	%xmm1, %xmm0

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(CO1), %xmm8
	addps	%xmm8, %xmm0
#endif
	movsd	%xmm0, 0 * SIZE(CO1)
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
