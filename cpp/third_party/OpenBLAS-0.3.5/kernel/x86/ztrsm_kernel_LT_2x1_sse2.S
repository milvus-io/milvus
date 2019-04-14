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

#define PREFETCHSIZE (8 * 4)

#if !defined(HAVE_SSE2) || !defined(HAVE_MMX)
#error  You have to check your configuration.
#endif

#define STACK	16
#define ARGS	 0

#define STACK_M	 4 + STACK + ARGS(%esi)
#define STACK_N	 8 + STACK + ARGS(%esi)
#define STACK_K	12 + STACK + ARGS(%esi)
#define STACK_ALPHA_R	16 + STACK + ARGS(%esi)
#define STACK_ALPHA_I	24 + STACK + ARGS(%esi)
#define STACK_A	32 + STACK + ARGS(%esi)
#define STACK_B	36 + STACK + ARGS(%esi)
#define STACK_C	40 + STACK + ARGS(%esi)
#define STACK_LDC	44 + STACK + ARGS(%esi)
#define STACK_OFFT	48 + STACK + ARGS(%esi)

#define POSINV	 0(%esp)
#define K	16(%esp)
#define N	20(%esp)
#define M	24(%esp)
#define A	28(%esp)
#define C	32(%esp)
#define J	36(%esp)
#define OLD_STACK 40(%esp)
#define OFFSET  44(%esp)
#define KK	48(%esp)
#define KKK	52(%esp)
#define AORIG   56(%esp)
#define BORIG	60(%esp)
#define BUFFER 128(%esp)

#define STACK_ALIGN	4096
#define STACK_OFFSET	1024

#define B	%edi
#define LDC	%ebp
#define AA	%edx
#define BB	%ecx
#define CO1	%esi

#define KERNEL1(address) \
	movq	 (PREFETCHSIZE + 0) * SIZE + (address) * SIZE(AA), %mm2; \
	mulpd	%xmm0, %xmm2; \
	mulpd	 2 * SIZE + (address) * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm4; \
	movapd	 0 * SIZE + (address) * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm5; \
	movapd	 2 * SIZE + (address) * SIZE(AA), %xmm0; \
	mulpd	%xmm0, %xmm2; \
	mulpd	 2 * SIZE + (address) * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm6; \
	movapd	 4 * SIZE + (address) * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm7; \
	movapd	 4 * SIZE + (address) * SIZE(AA), %xmm0

#define KERNEL2(address) \
	mulpd	%xmm0, %xmm2; \
	mulpd	 6 * SIZE + (address) * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm4; \
	movapd	 4 * SIZE + (address) * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm5; \
	movapd	 6 * SIZE + (address) * SIZE(AA), %xmm0; \
	mulpd	%xmm0, %xmm2; \
	mulpd	 6 * SIZE + (address) * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm6; \
	movapd	16 * SIZE + (address) * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm7; \
	movapd	16 * SIZE + (address) * SIZE(AA), %xmm0

#define KERNEL3(address) \
	movq	 (PREFETCHSIZE + 8) * SIZE + (address) * SIZE(AA), %mm2; \
	mulpd	%xmm1, %xmm3; \
	mulpd	10 * SIZE + (address) * SIZE(BB), %xmm1; \
	addpd	%xmm3, %xmm4; \
	movapd	 8 * SIZE + (address) * SIZE(BB), %xmm3; \
	addpd	%xmm1, %xmm5; \
	movapd	10 * SIZE + (address) * SIZE(AA), %xmm1; \
	mulpd	%xmm1, %xmm3; \
	mulpd	10 * SIZE + (address) * SIZE(BB), %xmm1; \
	addpd	%xmm3, %xmm6; \
	movapd	12 * SIZE + (address) * SIZE(BB), %xmm3; \
	addpd	%xmm1, %xmm7; \
	movapd	12 * SIZE + (address) * SIZE(AA), %xmm1

#define KERNEL4(address) \
	mulpd	%xmm1, %xmm3; \
	mulpd	14 * SIZE + (address) * SIZE(BB), %xmm1; \
	addpd	%xmm3, %xmm4; \
	movapd	12 * SIZE + (address) * SIZE(BB), %xmm3; \
	addpd	%xmm1, %xmm5; \
	movapd	14 * SIZE + (address) * SIZE(AA), %xmm1; \
	mulpd	%xmm1, %xmm3; \
	mulpd	14 * SIZE + (address) * SIZE(BB), %xmm1; \
	addpd	%xmm3, %xmm6; \
	movapd	24 * SIZE + (address) * SIZE(BB), %xmm3; \
	addpd	%xmm1, %xmm7; \
	movapd	24 * SIZE + (address) * SIZE(AA), %xmm1

#define KERNEL5(address) \
	movq	 (PREFETCHSIZE + 16) * SIZE + (address) * SIZE(AA), %mm2; \
	mulpd	%xmm0, %xmm2; \
	mulpd	18 * SIZE + (address) * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm4; \
	movapd	16 * SIZE + (address) * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm5; \
	movapd	18 * SIZE + (address) * SIZE(AA), %xmm0; \
	mulpd	%xmm0, %xmm2; \
	mulpd	18 * SIZE + (address) * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm6; \
	movapd	20 * SIZE + (address) * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm7; \
	movapd	20 * SIZE + (address) * SIZE(AA), %xmm0

#define KERNEL6(address) \
	mulpd	%xmm0, %xmm2; \
	mulpd	22 * SIZE + (address) * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm4; \
	movapd	20 * SIZE + (address) * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm5; \
	movapd	22 * SIZE + (address) * SIZE(AA), %xmm0; \
	mulpd	%xmm0, %xmm2; \
	mulpd	22 * SIZE + (address) * SIZE(BB), %xmm0; \
	addpd	%xmm2, %xmm6; \
	movapd	32 * SIZE + (address) * SIZE(BB), %xmm2; \
	addpd	%xmm0, %xmm7; \
	movapd	32 * SIZE + (address) * SIZE(AA), %xmm0

#define KERNEL7(address) \
	movq	 (PREFETCHSIZE + 24) * SIZE + (address) * SIZE(AA), %mm2; \
	mulpd	%xmm1, %xmm3; \
	mulpd	26 * SIZE + (address) * SIZE(BB), %xmm1; \
	addpd	%xmm3, %xmm4; \
	movapd	24 * SIZE + (address) * SIZE(BB), %xmm3; \
	addpd	%xmm1, %xmm5; \
	movapd	26 * SIZE + (address) * SIZE(AA), %xmm1; \
	mulpd	%xmm1, %xmm3; \
	mulpd	26 * SIZE + (address) * SIZE(BB), %xmm1; \
	addpd	%xmm3, %xmm6; \
	movapd	28 * SIZE + (address) * SIZE(BB), %xmm3; \
	addpd	%xmm1, %xmm7; \
	movapd	28 * SIZE + (address) * SIZE(AA), %xmm1

#define KERNEL8(address) \
	mulpd	%xmm1, %xmm3; \
	mulpd	30 * SIZE + (address) * SIZE(BB), %xmm1; \
	addpd	%xmm3, %xmm4; \
	movapd	28 * SIZE + (address) * SIZE(BB), %xmm3; \
	addpd	%xmm1, %xmm5; \
	movapd	30 * SIZE + (address) * SIZE(AA), %xmm1; \
	mulpd	%xmm1, %xmm3; \
	mulpd	30 * SIZE + (address) * SIZE(BB), %xmm1; \
	addpd	%xmm3, %xmm6; \
	movapd	40 * SIZE + (address) * SIZE(BB), %xmm3; \
	addpd	%xmm1, %xmm7; \
	movapd	40 * SIZE + (address) * SIZE(AA), %xmm1

#ifndef CONJ
#define NN
#else
#if defined(LN) || defined(LT)
#define CN
#else
#define NC
#endif
#endif

	PROLOGUE

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	EMMS

	movl	%esp, %esi	# save old stack

	subl	$128 + LOCAL_BUFFER_SIZE + STACK_OFFSET, %esp
	andl	$-STACK_ALIGN, %esp	# align stack
	addl	$STACK_OFFSET, %esp

	STACK_TOUCHING

	movd	STACK_M, %mm0
	movl	STACK_N, %eax
	movd	STACK_K, %mm1
	movd	STACK_A, %mm2
	movl	STACK_B, B
	movd	STACK_C, %mm3
	movl	STACK_LDC, LDC
	movd	STACK_OFFT, %mm4

	pcmpeqb	%xmm7, %xmm7
	psllq	$63, %xmm7	# Generate mask
	pxor	%xmm2, %xmm2

	movsd	  %xmm2,  0 + POSINV
	movsd	  %xmm7,  8 + POSINV

	movd	%mm1, K
	movl	%eax, N
	movd	%mm0, M
	movd	%mm2, A
	movd	%mm3, C
	movl	%esi, OLD_STACK
	movd	%mm4, OFFSET
	movd	%mm4, KK

	sall	$ZBASE_SHIFT, LDC

#ifdef LN
       movl	M, %eax
       sall	$ZBASE_SHIFT, %eax
       addl	%eax, C
       imull	K, %eax
       addl	%eax, A
#endif

#ifdef RT
       movl	N, %eax
       sall	$ZBASE_SHIFT, %eax
       imull	K, %eax
       addl	%eax, B

       movl	N, %eax
       imull	LDC, %eax
       addl	%eax, C
#endif

#ifdef RN
	negl	KK
#endif

#ifdef RT
       movl	N, %eax
       subl	OFFSET, %eax
       movl	%eax, KK
#endif

	movl	N, %eax
	movl	%eax, J			# j = n
	testl	%eax, %eax
	jle	.L999
	ALIGN_2

.L01:
#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

	leal	BUFFER, BB

#ifdef RT
       movl	K, %eax
       sall	$ZBASE_SHIFT, %eax
       subl	%eax, B
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	B, BORIG
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, B
	leal	(BB, %eax, 2), BB
#endif

#if defined(LT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	sarl	$2, %eax
	jle	.L03
	ALIGN_2

.L02:
	movsd	 0 * SIZE(B), %xmm0
	movsd	 1 * SIZE(B), %xmm1
	movsd	 2 * SIZE(B), %xmm2
	movsd	 3 * SIZE(B), %xmm3
	movsd	 4 * SIZE(B), %xmm4
	movsd	 5 * SIZE(B), %xmm5
	movsd	 6 * SIZE(B), %xmm6
	movsd	 7 * SIZE(B), %xmm7

	unpcklpd %xmm0, %xmm0
	unpcklpd %xmm1, %xmm1
	unpcklpd %xmm2, %xmm2
	unpcklpd %xmm3, %xmm3
	unpcklpd %xmm4, %xmm4
	unpcklpd %xmm5, %xmm5
	unpcklpd %xmm6, %xmm6
	unpcklpd %xmm7, %xmm7

	movapd	%xmm0,  0 * SIZE(BB)
	movapd	%xmm1,  2 * SIZE(BB)
	movapd	%xmm2,  4 * SIZE(BB)
	movapd	%xmm3,  6 * SIZE(BB)
	movapd	%xmm4,  8 * SIZE(BB)
	movapd	%xmm5, 10 * SIZE(BB)
	movapd	%xmm6, 12 * SIZE(BB)
	movapd	%xmm7, 14 * SIZE(BB)

	prefetcht0	104 * SIZE(B)

	addl	$ 8 * SIZE, B
	addl	$16 * SIZE, BB
	decl	%eax
	jne	.L02
	ALIGN_2

.L03:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$3, %eax
	BRANCH
	jle	.L05
	ALIGN_2

.L04:
	movsd	 0 * SIZE(B), %xmm0
	movsd	 1 * SIZE(B), %xmm1

	unpcklpd %xmm0, %xmm0
	unpcklpd %xmm1, %xmm1

	movapd	%xmm0,  0 * SIZE(BB)
	movapd	%xmm1,  2 * SIZE(BB)

	addl	$ 2 * SIZE, B
	addl	$ 4 * SIZE, BB
	decl	%eax
	jne	.L04
	ALIGN_4

.L05:
#if defined(LT) || defined(RN)
	movl	A, %eax
	movl	%eax, AA
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

#ifdef RT
       subl	LDC, C
#endif

	movl	C, CO1

#ifndef RT
	addl	LDC, C
#endif

	movl	M,  %ebx
	sarl	$1, %ebx	# i = (m >> 2)
	jle	.L50
	ALIGN_4

.L10:
#ifdef LN
       movl	K, %eax
       sall	$1 + ZBASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	AORIG, %eax
	movl	%eax, AA

	movl	KK, %eax
	sall	$1 + ZBASE_SHIFT, %eax
	addl	%eax, AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + ZBASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movapd	 0 * SIZE(BB), %xmm2
	pxor	%xmm4, %xmm4
	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movapd	 8 * SIZE(BB), %xmm3
	pxor	%xmm6, %xmm6
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm7, %xmm7

#ifdef LN
	prefetchnta	-4 * SIZE(CO1)
#else
	prefetchnta	 4 * SIZE(CO1)
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$-8, %eax
	NOBRANCH
	je	.L12
	sall	$3, %eax

.L1X:
	KERNEL1(32  *  0)
	KERNEL2(32  *  0)
	KERNEL3(32  *  0)
	KERNEL4(32  *  0)
	KERNEL5(32  *  0)
	KERNEL6(32  *  0)
	KERNEL7(32  *  0)
	KERNEL8(32  *  0)
	cmpl	$64 *  1, %eax
	NOBRANCH
	jle	.L11
	KERNEL1(32  *  1)
	KERNEL2(32  *  1)
	KERNEL3(32  *  1)
	KERNEL4(32  *  1)
	KERNEL5(32  *  1)
	KERNEL6(32  *  1)
	KERNEL7(32  *  1)
	KERNEL8(32  *  1)
	cmpl	$64 *  2, %eax
	NOBRANCH
	jle	.L11
	KERNEL1(32  *  2)
	KERNEL2(32  *  2)
	KERNEL3(32  *  2)
	KERNEL4(32  *  2)
	KERNEL5(32  *  2)
	KERNEL6(32  *  2)
	KERNEL7(32  *  2)
	KERNEL8(32  *  2)
	cmpl	$64 *  3, %eax
	NOBRANCH
	jle	.L11
	KERNEL1(32  *  3)
	KERNEL2(32  *  3)
	KERNEL3(32  *  3)
	KERNEL4(32  *  3)
	KERNEL5(32  *  3)
	KERNEL6(32  *  3)
	KERNEL7(32  *  3)
	KERNEL8(32  *  3)
	cmpl	$64 *  4, %eax
	NOBRANCH
	jle	.L11
	KERNEL1(32  *  4)
	KERNEL2(32  *  4)
	KERNEL3(32  *  4)
	KERNEL4(32  *  4)
	KERNEL5(32  *  4)
	KERNEL6(32  *  4)
	KERNEL7(32  *  4)
	KERNEL8(32  *  4)
	cmpl	$64 *  5, %eax
	NOBRANCH
	jle	.L11
	KERNEL1(32  *  5)
	KERNEL2(32  *  5)
	KERNEL3(32  *  5)
	KERNEL4(32  *  5)
	KERNEL5(32  *  5)
	KERNEL6(32  *  5)
	KERNEL7(32  *  5)
	KERNEL8(32  *  5)
	cmpl	$64 *  6, %eax
	NOBRANCH
	jle	.L11
	KERNEL1(32  *  6)
	KERNEL2(32  *  6)
	KERNEL3(32  *  6)
	KERNEL4(32  *  6)
	KERNEL5(32  *  6)
	KERNEL6(32  *  6)
	KERNEL7(32  *  6)
	KERNEL8(32  *  6)
	cmpl	$64 *  7, %eax
	NOBRANCH
	jle	.L11
	KERNEL1(32  *  7)
	KERNEL2(32  *  7)
	KERNEL3(32  *  7)
	KERNEL4(32  *  7)
	KERNEL5(32  *  7)
	KERNEL6(32  *  7)
	KERNEL7(32  *  7)
	KERNEL8(32  *  7)

	addl	$64 * 4  * SIZE, AA
	addl	$64 * 4  * SIZE, BB
	subl	$64 * 8, %eax
	BRANCH
	jg	.L1X

.L11:
	leal	(BB, %eax, 4), BB
	leal	(AA, %eax, 4), AA

.L12:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L14

.L13:
	movapd	 2 * SIZE(BB), %xmm1
	mulpd	%xmm0, %xmm2
	addpd	%xmm2, %xmm4
	movapd	 0 * SIZE(BB), %xmm2
	mulpd	%xmm0, %xmm1
	movapd	 2 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm5
	movapd	 2 * SIZE(BB), %xmm1
	mulpd	%xmm0, %xmm2
	addpd	%xmm2, %xmm6
	movapd	 4 * SIZE(BB), %xmm2
	mulpd	%xmm0, %xmm1
	movapd	 4 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm7

	addl	$4 * SIZE, AA		# aoffset  += 8
	addl	$4 * SIZE, BB		# boffset1 += 8
	subl	$1, %eax
	jg	.L13

.L14:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$2, %eax
#else
	subl	$1, %eax
#endif

	movl	AORIG, AA
	movl	BORIG, B
	leal	BUFFER, BB

	sall	$ZBASE_SHIFT, %eax
	leal	(AA, %eax, 2), AA
	addl	%eax, B
	leal	(BB, %eax, 2), BB
#endif

	movapd	POSINV,  %xmm1

	SHUFPD_1 %xmm5, %xmm5
	SHUFPD_1 %xmm7, %xmm7

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	xorpd	%xmm1, %xmm5
	xorpd	%xmm1, %xmm7
#else
	xorpd	%xmm1, %xmm4
	xorpd	%xmm1, %xmm6
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subpd	%xmm5, %xmm4
	subpd	%xmm7, %xmm6
#else
	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(B), %xmm5
	movapd	 2 * SIZE(B), %xmm7

	subpd	%xmm4,  %xmm5
	subpd	%xmm6,  %xmm7
#else
	movapd	 0 * SIZE(AA), %xmm5
	movapd	 2 * SIZE(AA), %xmm7

	subpd	%xmm4,  %xmm5
	subpd	%xmm6,  %xmm7
#endif

#ifndef CONJ
	SHUFPD_1 %xmm1, %xmm1
#endif


#ifdef LN
	movsd	 6 * SIZE(AA), %xmm2
	movhpd	 6 * SIZE(AA), %xmm2
	movsd	 7 * SIZE(AA), %xmm3
	movhpd	 7 * SIZE(AA), %xmm3

	pshufd	$0x4e, %xmm7, %xmm6

	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm7
	mulpd	 %xmm3, %xmm6

	addpd	 %xmm6, %xmm7

	movsd	 4 * SIZE(AA), %xmm2
	movhpd	 4 * SIZE(AA), %xmm2
	movsd	 5 * SIZE(AA), %xmm3
	movhpd	 5 * SIZE(AA), %xmm3

	movapd	 %xmm7, %xmm4
	pshufd	 $0x4e, %xmm7, %xmm6

	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm4
	mulpd	 %xmm3, %xmm6

	subpd	 %xmm4, %xmm5
	subpd	 %xmm6, %xmm5

	movsd	 0 * SIZE(AA), %xmm2
	movhpd	 0 * SIZE(AA), %xmm2
	movsd	 1 * SIZE(AA), %xmm3
	movhpd	 1 * SIZE(AA), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5
#endif

#ifdef LT
	movsd	 0 * SIZE(AA), %xmm2
	movhpd	 0 * SIZE(AA), %xmm2
	movsd	 1 * SIZE(AA), %xmm3
	movhpd	 1 * SIZE(AA), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5

	movsd	 2 * SIZE(AA), %xmm2
	movhpd	 2 * SIZE(AA), %xmm2
	movsd	 3 * SIZE(AA), %xmm3
	movhpd	 3 * SIZE(AA), %xmm3

	movapd	 %xmm5, %xmm4
	pshufd	 $0x4e, %xmm5, %xmm6

	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm4
	mulpd	 %xmm3, %xmm6

	subpd	 %xmm4, %xmm7
	subpd	 %xmm6, %xmm7

	movsd	 6 * SIZE(AA), %xmm2
	movhpd	 6 * SIZE(AA), %xmm2
	movsd	 7 * SIZE(AA), %xmm3
	movhpd	 7 * SIZE(AA), %xmm3

	pshufd	$0x4e, %xmm7, %xmm6

	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm7
	mulpd	 %xmm3, %xmm6

	addpd	 %xmm6, %xmm7
#endif

#ifdef RN
	movsd	 0 * SIZE(B), %xmm2
	movhpd	 0 * SIZE(B), %xmm2
	movsd	 1 * SIZE(B), %xmm3
	movhpd	 1 * SIZE(B), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4
	pshufd	$0x4e, %xmm7, %xmm6

	xorpd	 %xmm1, %xmm4
	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4
	mulpd	 %xmm2, %xmm7
	mulpd	 %xmm3, %xmm6

	addpd	 %xmm4, %xmm5
	addpd	 %xmm6, %xmm7
#endif

#ifdef RT
	movsd	 0 * SIZE(B), %xmm2
	movhpd	 0 * SIZE(B), %xmm2
	movsd	 1 * SIZE(B), %xmm3
	movhpd	 1 * SIZE(B), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4
	pshufd	$0x4e, %xmm7, %xmm6

	xorpd	 %xmm1, %xmm4
	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4
	mulpd	 %xmm2, %xmm7
	mulpd	 %xmm3, %xmm6

	addpd	 %xmm4, %xmm5
	addpd	 %xmm6, %xmm7
#endif

#ifdef LN
	subl	$4 * SIZE, CO1
#endif

	movsd	%xmm5,   0 * SIZE(CO1)
	movhpd	%xmm5,   1 * SIZE(CO1)
	movsd	%xmm7,   2 * SIZE(CO1)
	movhpd	%xmm7,   3 * SIZE(CO1)

#if defined(LN) || defined(LT)
	movapd	%xmm5,   0 * SIZE(B)
	movapd	%xmm7,   2 * SIZE(B)

	movsd	%xmm5,   0 * SIZE(BB)
	movsd	%xmm5,   1 * SIZE(BB)
	movhpd	%xmm5,   2 * SIZE(BB)
	movhpd	%xmm5,   3 * SIZE(BB)
	movsd	%xmm7,   4 * SIZE(BB)
	movsd	%xmm7,   5 * SIZE(BB)
	movhpd	%xmm7,   6 * SIZE(BB)
	movhpd	%xmm7,   7 * SIZE(BB)
#else
	movapd	%xmm5,   0 * SIZE(AA)
	movapd	%xmm7,   2 * SIZE(AA)

#endif

#ifndef LN
	addl	$4 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$1 + ZBASE_SHIFT, %eax
	addl	%eax, AA
#ifdef LT
	addl	$4 * SIZE, B
#endif
#endif

#ifdef LN
	subl	$2, KK
	movl	BORIG, B
#endif

#ifdef LT
	addl	$2, KK
#endif

#ifdef RT
	movl	K, %eax
	movl	BORIG, B
	sall	$1 + ZBASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx			# i --
	jg	.L10

.L50:
	movl	M,  %ebx
	testl	$1, %ebx
	je	.L99

#ifdef LN
       movl	K, %eax
       sall	$ZBASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	AORIG, %eax
	movl	%eax, AA

	movl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AA
#endif

	leal	BUFFER, %ecx

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + ZBASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movapd	 0 * SIZE(BB), %xmm1
	pxor	%xmm4, %xmm4
	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movapd	 8 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$2, %eax		# l = (k >> 2)
	jle	.L52

.L51:
	mulpd	%xmm0, %xmm1
	movapd	 2 * SIZE(BB), %xmm3
	addpd	%xmm1, %xmm4
	movapd	16 * SIZE(BB), %xmm1

	mulpd	%xmm0, %xmm3
	movapd	 2 * SIZE(AA), %xmm0
	addpd	%xmm3, %xmm5
	movapd	 4 * SIZE(BB), %xmm3

	mulpd	%xmm0, %xmm3
	mulpd	 6 * SIZE(BB), %xmm0
	addpd	%xmm3, %xmm4

	addpd	%xmm0, %xmm5
	movapd	 4 * SIZE(AA), %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	10 * SIZE(BB), %xmm0

	addpd	%xmm2, %xmm4
	addpd	%xmm0, %xmm5
	movapd	6 * SIZE(AA), %xmm0

	movapd	12 * SIZE(BB), %xmm2
	mulpd	%xmm0, %xmm2
	addpd	%xmm2, %xmm4
	movapd	24 * SIZE(BB), %xmm2

	mulpd	14 * SIZE(BB), %xmm0
	addpd	%xmm0, %xmm5
	movapd	 8 * SIZE(AA), %xmm0

	addl	$ 8 * SIZE, AA	# aoffset  += 2
	addl	$16 * SIZE, BB	# boffset1 += 4

	decl	%eax			# l--
	jg	.L51
	ALIGN_2

.L52:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	andl	$3, %eax		# l = (k & 3)
	jle	.L54
	ALIGN_2

.L53:
	movapd	0 * SIZE(BB), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	movapd	2 * SIZE(BB), %xmm1
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm5
	movapd	2 * SIZE(AA), %xmm0

	addl	$2 * SIZE, AA		# aoffset  += 2
	addl	$4 * SIZE, BB		# boffset1 += 4
	decl	%eax			# l--
	jg	.L53

.L54:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$1, %eax
#endif

	movl	AORIG, AA
	movl	BORIG, B
	leal	BUFFER, BB

	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AA
	addl	%eax, B
	leal	(BB, %eax, 2), BB
#endif

	movapd	POSINV,  %xmm1

	SHUFPD_1 %xmm5, %xmm5

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	xorpd	%xmm1, %xmm5
#else
	xorpd	%xmm1, %xmm4
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subpd	%xmm5, %xmm4
#else
	addpd	%xmm5, %xmm4
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(B), %xmm5

	subpd	%xmm4,  %xmm5
#else
	movapd	 0 * SIZE(AA), %xmm5

	subpd	%xmm4,  %xmm5
#endif

#ifndef CONJ
	SHUFPD_1 %xmm1, %xmm1
#endif

#ifdef LN
	movsd	 0 * SIZE(AA), %xmm2
	movhpd	 0 * SIZE(AA), %xmm2
	movsd	 1 * SIZE(AA), %xmm3
	movhpd	 1 * SIZE(AA), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5
#endif

#ifdef LT
	movsd	 0 * SIZE(AA), %xmm2
	movhpd	 0 * SIZE(AA), %xmm2
	movsd	 1 * SIZE(AA), %xmm3
	movhpd	 1 * SIZE(AA), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5
#endif

#ifdef RN
	movsd	 0 * SIZE(B), %xmm2
	movhpd	 0 * SIZE(B), %xmm2
	movsd	 1 * SIZE(B), %xmm3
	movhpd	 1 * SIZE(B), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5
#endif

#ifdef RT
	movsd	 0 * SIZE(B), %xmm2
	movhpd	 0 * SIZE(B), %xmm2
	movsd	 1 * SIZE(B), %xmm3
	movhpd	 1 * SIZE(B), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

	movsd	%xmm5,   0 * SIZE(CO1)
	movhpd	%xmm5,   1 * SIZE(CO1)

#if defined(LN) || defined(LT)
	movapd	%xmm5,   0 * SIZE(B)

	movsd	%xmm5,   0 * SIZE(BB)
	movsd	%xmm5,   1 * SIZE(BB)
	movhpd	%xmm5,   2 * SIZE(BB)
	movhpd	%xmm5,   3 * SIZE(BB)
#else
	movapd	%xmm5,   0 * SIZE(AA)

#endif

#ifndef LN
	addl	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AA
#ifdef LT
	addl	$2 * SIZE, B
#endif
#endif

#ifdef LN
	subl	$1, KK
	movl	BORIG, B
#endif

#ifdef LT
	addl	$1, KK
#endif

#ifdef RT
	movl	K, %eax
	movl	BORIG, B
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AORIG
#endif
	ALIGN_4

.L99:
#ifdef LN
       movl	K, %eax
       sall	$ZBASE_SHIFT, %eax
       addl	%eax, B
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, B
#endif

#ifdef RN
	addl	$1, KK
#endif

#ifdef RT
	subl	$1, KK
#endif

	decl	J			# j --
	jg	.L01

.L999:
	movl	OLD_STACK, %esp

	EMMS

	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
