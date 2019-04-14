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

#if !defined(HAVE_SSE2) || !defined(HAVE_MMX)
#error  You have to check your configuration.
#endif

#define STACK	16
#define ARGS	 0

#define STACK_M	 4 + STACK + ARGS(%esi)
#define STACK_N	 8 + STACK + ARGS(%esi)
#define STACK_K	12 + STACK + ARGS(%esi)
#define STACK_ALPHA	16 + STACK + ARGS(%esi)
#define STACK_A	24 + STACK + ARGS(%esi)
#define STACK_B	28 + STACK + ARGS(%esi)
#define STACK_C	32 + STACK + ARGS(%esi)
#define STACK_LDC	36 + STACK + ARGS(%esi)
#define STACK_OFFT	40 + STACK + ARGS(%esi)

#define ALPHA	 0(%esp)
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
#define AORIG	56(%esp)
#define BORIG	60(%esp)
#define BUFFER 128(%esp)

#define STACK_ALIGN	4096
#define STACK_OFFSET	1024

#define B	%edi
#define AA	%edx
#define BB	%ecx
#define LDC	%ebp

#define PREFETCHSIZE (8 * 4)

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

	PROLOGUE

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	EMMS

	movl	%esp, %esi	# save old stack

	subl	$128 + LOCAL_BUFFER_SIZE + STACK_OFFSET, %esp
	andl	$-STACK_ALIGN, %esp
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

	movd	%mm1, K
	movl	%eax, N
	movd	%mm0, M
	movd	%mm2, A
	movd	%mm3, C
	movl	%esi, OLD_STACK
	movd	%mm4, OFFSET
	movd	%mm4, KK

	sall	$BASE_SHIFT, LDC

#ifdef LN
       movl	M, %eax
       leal	(, %eax, SIZE), %eax
       addl	%eax, C
       imull	K, %eax
       addl	%eax, A
#endif

#ifdef RT
       movl	N, %eax
       leal	(, %eax, SIZE), %eax
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

	movl	N,  %eax
	sarl	$1, %eax
	movl	%eax, J
	jle	.L100
	ALIGN_2

.L01:
/* Copying to Sub Buffer */
#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

	leal	BUFFER, %ecx

#ifdef RT
       movl	K, %eax
       sall	$1 + BASE_SHIFT, %eax
       subl	%eax, B
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	B, BORIG
	leal	(, %eax, SIZE), %eax
	leal	(B,  %eax, 2), B
	leal	(BB, %eax, 4), BB
#endif

#ifdef LT
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
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

	unpcklpd  %xmm0, %xmm0
	unpcklpd  %xmm1, %xmm1
	unpcklpd  %xmm2, %xmm2
	unpcklpd  %xmm3, %xmm3
	unpcklpd  %xmm4, %xmm4
	unpcklpd  %xmm5, %xmm5
	unpcklpd  %xmm6, %xmm6
	unpcklpd  %xmm7, %xmm7

	movapd	%xmm0,  0 * SIZE(%ecx)
	movapd	%xmm1,  2 * SIZE(%ecx)
	movapd	%xmm2,  4 * SIZE(%ecx)
	movapd	%xmm3,  6 * SIZE(%ecx)
	movapd	%xmm4,  8 * SIZE(%ecx)
	movapd	%xmm5, 10 * SIZE(%ecx)
	movapd	%xmm6, 12 * SIZE(%ecx)
	movapd	%xmm7, 14 * SIZE(%ecx)

	prefetcht0	104 * SIZE(B)

	addl	$ 8 * SIZE, B
	addl	$16 * SIZE, %ecx
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
	ALIGN_4

.L04:
	movsd	 0 * SIZE(B), %xmm0
	movsd	 1 * SIZE(B), %xmm1

	unpcklpd  %xmm0, %xmm0
	unpcklpd  %xmm1, %xmm1

	movapd	%xmm0,  0 * SIZE(%ecx)
	movapd	%xmm1,  2 * SIZE(%ecx)

	addl	$2 * SIZE, B
	addl	$4 * SIZE, %ecx
	decl	%eax
	jne	.L04
	ALIGN_4

.L05:
#if defined(LT) || defined(RN)
	movl	A, AA
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

	leal	(, LDC, 2), %eax

#ifdef RT
	subl	%eax, C
#endif
	movl	C, %esi		# coffset = c
#ifndef RT
	addl	%eax, C
#endif

	movl	M,  %ebx
	sarl	$2, %ebx	# i = (m >> 2)
	jle	.L30
	ALIGN_4

.L10:
#ifdef LN
       movl	K, %eax
       sall	$2 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + BASE_SHIFT, %eax
	leal	(BB, %eax, 2), BB
#endif

	movapd	 0 * SIZE(BB), %xmm2
	pxor	%xmm4, %xmm4
	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movapd	 8 * SIZE(BB), %xmm3
	pxor	%xmm6, %xmm6
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm7, %xmm7

	prefetcht2	4 * SIZE(%esi)
	prefetcht2	4 * SIZE(%esi, LDC)

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif

#ifdef PENTIUM4
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
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 4), BB

#else
	sarl	$3, %eax
	je	.L12

.L11:
	KERNEL1(32  *  0)
	KERNEL2(32  *  0)
	KERNEL3(32  *  0)
	KERNEL4(32  *  0)
	KERNEL5(32  *  0)
	KERNEL6(32  *  0)
	KERNEL7(32  *  0)
	KERNEL8(32  *  0)

	addl   $32 * SIZE, %ecx
	addl   $32 * SIZE, %edx
	decl   %eax
	jne    .L11
#endif

.L12:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L14

.L13:
	mulpd	%xmm0, %xmm2
	mulpd	 2 * SIZE(BB), %xmm0
	addpd	%xmm2, %xmm4
	movapd	 0 * SIZE(BB), %xmm2
	addpd	%xmm0, %xmm5
	movapd	 2 * SIZE(AA), %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	 2 * SIZE(BB), %xmm0
	addpd	%xmm2, %xmm6
	movapd	 4 * SIZE(BB), %xmm2
	addpd	%xmm0, %xmm7
	movapd	 4 * SIZE(AA), %xmm0

	addl	$4 * SIZE, AA		# aoffset  += 8
	addl	$4 * SIZE, BB		# boffset1 += 8
	subl	$1, %eax
	jg	.L13
	ALIGN_4

.L14:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$4, %eax
#else
	subl	$2, %eax
#endif

	movl	AORIG, AA
	movl	BORIG, B
	leal	BUFFER, BB

	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(B,  %eax, 2), B
	leal	(BB, %eax, 4), BB
#endif

#if defined(LN) || defined(LT)
	movapd	 %xmm4, %xmm0
	unpcklpd %xmm5, %xmm4
	unpckhpd %xmm5, %xmm0

	movapd	 %xmm6, %xmm1
	unpcklpd %xmm7, %xmm6
	unpckhpd %xmm7, %xmm1

	movapd	 0 * SIZE(B), %xmm2
	movapd	 2 * SIZE(B), %xmm3
	movapd	 4 * SIZE(B), %xmm5
	movapd	 6 * SIZE(B), %xmm7

	subpd	%xmm4,  %xmm2
	subpd	%xmm0,  %xmm3
	subpd	%xmm6,  %xmm5
	subpd	%xmm1,  %xmm7
#else
	movapd	 0 * SIZE(AA), %xmm0
	movapd	 2 * SIZE(AA), %xmm1
	movapd	 4 * SIZE(AA), %xmm2
	movapd	 6 * SIZE(AA), %xmm3

	subpd	%xmm4, %xmm0
	subpd	%xmm6, %xmm1
	subpd	%xmm5, %xmm2
	subpd	%xmm7, %xmm3
#endif

#ifdef LN
	movsd	15 * SIZE(AA), %xmm0
	movhpd	15 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm7
	movsd	14 * SIZE(AA), %xmm0
	movhpd	14 * SIZE(AA), %xmm0
	mulpd	 %xmm7, %xmm0
	subpd	 %xmm0, %xmm5
	movsd	13 * SIZE(AA), %xmm0
	movhpd	13 * SIZE(AA), %xmm0
	mulpd	 %xmm7, %xmm0
	subpd	 %xmm0, %xmm3
	movsd	12 * SIZE(AA), %xmm0
	movhpd	12 * SIZE(AA), %xmm0
	mulpd	 %xmm7, %xmm0
	subpd	 %xmm0, %xmm2

	movsd	10 * SIZE(AA), %xmm0
	movhpd	10 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm5
	movsd	 9 * SIZE(AA), %xmm0
	movhpd	 9 * SIZE(AA), %xmm0
	mulpd	 %xmm5, %xmm0
	subpd	 %xmm0, %xmm3
	movsd	 8 * SIZE(AA), %xmm0
	movhpd	 8 * SIZE(AA), %xmm0
	mulpd	 %xmm5, %xmm0
	subpd	 %xmm0, %xmm2

	movsd	 5 * SIZE(AA), %xmm0
	movhpd	 5 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm3
	movsd	 4 * SIZE(AA), %xmm0
	movhpd	 4 * SIZE(AA), %xmm0
	mulpd	 %xmm3, %xmm0
	subpd	 %xmm0, %xmm2

	movsd	 0 * SIZE(AA), %xmm0
	movhpd	 0 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm2
#endif

#ifdef LT
	movsd	 0 * SIZE(AA), %xmm0
	movhpd	 0 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm2
	movsd	 1 * SIZE(AA), %xmm0
	movhpd	 1 * SIZE(AA), %xmm0
	mulpd	 %xmm2, %xmm0
	subpd	 %xmm0, %xmm3
	movsd	 2 * SIZE(AA), %xmm0
	movhpd	 2 * SIZE(AA), %xmm0
	mulpd	 %xmm2, %xmm0
	subpd	 %xmm0, %xmm5
	movsd	 3 * SIZE(AA), %xmm0
	movhpd	 3 * SIZE(AA), %xmm0
	mulpd	 %xmm2, %xmm0
	subpd	 %xmm0, %xmm7

	movsd	 5 * SIZE(AA), %xmm0
	movhpd	 5 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm3

	movsd	 6 * SIZE(AA), %xmm0
	movhpd	 6 * SIZE(AA), %xmm0
	mulpd	 %xmm3, %xmm0
	subpd	 %xmm0, %xmm5
	movsd	 7 * SIZE(AA), %xmm0
	movhpd	 7 * SIZE(AA), %xmm0
	mulpd	 %xmm3, %xmm0
	subpd	 %xmm0, %xmm7

	movsd	10 * SIZE(AA), %xmm0
	movhpd	10 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm5
	movsd	11 * SIZE(AA), %xmm0
	movhpd	11 * SIZE(AA), %xmm0
	mulpd	 %xmm5, %xmm0
	subpd	 %xmm0, %xmm7

	movsd	15 * SIZE(AA), %xmm0
	movhpd	15 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm7
#endif

#ifdef RN
	movsd	 0 * SIZE(B), %xmm4
	movhpd	 0 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm0
	mulpd	 %xmm4, %xmm1

	movsd	 1 * SIZE(B), %xmm4
	movhpd	 1 * SIZE(B), %xmm4
	mulpd	 %xmm0, %xmm4
	subpd	 %xmm4, %xmm2
	movsd	 1 * SIZE(B), %xmm4
	movhpd	 1 * SIZE(B), %xmm4
	mulpd	 %xmm1, %xmm4
	subpd	 %xmm4, %xmm3

	movsd	 3 * SIZE(B), %xmm4
	movhpd	 3 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm2
	mulpd	 %xmm4, %xmm3
#endif

#ifdef RT
	movsd	 3 * SIZE(B), %xmm4
	movhpd	 3 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm2
	mulpd	 %xmm4, %xmm3

	movsd	 2 * SIZE(B), %xmm4
	movhpd	 2 * SIZE(B), %xmm4
	mulpd	 %xmm2, %xmm4
	subpd	 %xmm4, %xmm0
	movsd	 2 * SIZE(B), %xmm4
	movhpd	 2 * SIZE(B), %xmm4
	mulpd	 %xmm3, %xmm4
	subpd	 %xmm4, %xmm1

	movsd	 0 * SIZE(B), %xmm4
	movhpd	 0 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm0
	mulpd	 %xmm4, %xmm1
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm2,   0 * SIZE(B)
	movapd	%xmm3,   2 * SIZE(B)
	movapd	%xmm5,   4 * SIZE(B)
	movapd	%xmm7,   6 * SIZE(B)

	movsd	%xmm2,   0 * SIZE(BB)
	movsd	%xmm2,   1 * SIZE(BB)
	movhpd	%xmm2,   2 * SIZE(BB)
	movhpd	%xmm2,   3 * SIZE(BB)
	movsd	%xmm3,   4 * SIZE(BB)
	movsd	%xmm3,   5 * SIZE(BB)
	movhpd	%xmm3,   6 * SIZE(BB)
	movhpd	%xmm3,   7 * SIZE(BB)
	movsd	%xmm5,   8 * SIZE(BB)
	movsd	%xmm5,   9 * SIZE(BB)
	movhpd	%xmm5,  10 * SIZE(BB)
	movhpd	%xmm5,  11 * SIZE(BB)
	movsd	%xmm7,  12 * SIZE(BB)
	movsd	%xmm7,  13 * SIZE(BB)
	movhpd	%xmm7,  14 * SIZE(BB)
	movhpd	%xmm7,  15 * SIZE(BB)
#else
	movapd	%xmm0,   0 * SIZE(AA)
	movapd	%xmm1,   2 * SIZE(AA)
	movapd	%xmm2,   4 * SIZE(AA)
	movapd	%xmm3,   6 * SIZE(AA)
#endif

#ifdef LN
	subl	$4 * SIZE, %esi
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm2,  0 * SIZE(%esi)
	movsd	%xmm3,  1 * SIZE(%esi)
	movsd	%xmm5,  2 * SIZE(%esi)
	movsd	%xmm7,  3 * SIZE(%esi)

	movhpd	%xmm2,  0 * SIZE(%esi, LDC)
	movhpd	%xmm3,  1 * SIZE(%esi, LDC)
	movhpd	%xmm5,  2 * SIZE(%esi, LDC)
	movhpd	%xmm7,  3 * SIZE(%esi, LDC)
#else
	movsd	%xmm0,  0 * SIZE(%esi)
	movhpd	%xmm0,  1 * SIZE(%esi)
	movsd	%xmm1,  2 * SIZE(%esi)
	movhpd	%xmm1,  3 * SIZE(%esi)

	movsd	%xmm2,  0 * SIZE(%esi, LDC)
	movhpd	%xmm2,  1 * SIZE(%esi, LDC)
	movsd	%xmm3,  2 * SIZE(%esi, LDC)
	movhpd	%xmm3,  3 * SIZE(%esi, LDC)
#endif

#ifndef LN
	addl	$4 * SIZE, %esi
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
#ifdef LT
	addl	$8 * SIZE, B
#endif
#endif

#ifdef LN
	subl	$4, KK
	movl	BORIG, B
#endif

#ifdef LT
	addl	$4, KK
#endif

#ifdef RT
	movl	K, %eax
	movl	BORIG, B
	sall	$2 + BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx			# i --
	jg	.L10
	ALIGN_2

.L30:
	movl	M,  %ebx
	testl	$2, %ebx
	jle	.L50

#ifdef LN
       movl	K, %eax
       sall	$1 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + BASE_SHIFT, %eax
	leal	(BB, %eax, 2), BB
#endif

	movapd	 0 * SIZE(BB), %xmm2
	pxor	%xmm4, %xmm4
	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movapd	 8 * SIZE(BB), %xmm3
	pxor	%xmm6, %xmm6
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L32

.L31:
	mulpd	%xmm0, %xmm2
	mulpd	 2 * SIZE(BB), %xmm0
	addpd	%xmm2, %xmm4
	movapd	 4 * SIZE(BB), %xmm2
	addpd	%xmm0, %xmm5
	movapd	 2 * SIZE(AA), %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	 6 * SIZE(BB), %xmm0
	addpd	%xmm2, %xmm6
	movapd	16 * SIZE(BB), %xmm2
	addpd	%xmm0, %xmm7
	movapd	 4 * SIZE(AA), %xmm0
	mulpd	%xmm0, %xmm3
	mulpd	10 * SIZE(BB), %xmm0
	addpd	%xmm3, %xmm4
	movapd	12 * SIZE(BB), %xmm3
	addpd	%xmm0, %xmm5
	movapd	 6 * SIZE(AA), %xmm0
	mulpd	%xmm0, %xmm3
	mulpd	14 * SIZE(BB), %xmm0
	addpd	%xmm3, %xmm6
	movapd	24 * SIZE(BB), %xmm3
	addpd	%xmm0, %xmm7
	movapd	16 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2
	mulpd	18 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4
	movapd	20 * SIZE(BB), %xmm2
	addpd	%xmm1, %xmm5
	movapd	10 * SIZE(AA), %xmm1
	mulpd	%xmm1, %xmm2
	mulpd	22 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm6
	movapd	32 * SIZE(BB), %xmm2
	addpd	%xmm1, %xmm7
	movapd	12 * SIZE(AA), %xmm1
	mulpd	%xmm1, %xmm3
	mulpd	26 * SIZE(BB), %xmm1
	addpd	%xmm3, %xmm4
	movapd	28 * SIZE(BB), %xmm3
	addpd	%xmm1, %xmm5
	movapd	14 * SIZE(AA), %xmm1
	mulpd	%xmm1, %xmm3
	mulpd	30 * SIZE(BB), %xmm1
	addpd	%xmm3, %xmm6
	movapd	40 * SIZE(BB), %xmm3
	addpd	%xmm1, %xmm7
	movapd	24 * SIZE(AA), %xmm1

	addl   $16 * SIZE, AA
	addl   $32 * SIZE, BB
	BRANCH
	decl   %eax
	jne    .L31

.L32:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L34

.L33:
	mulpd	%xmm0, %xmm2
	mulpd	 2 * SIZE(BB), %xmm0
	addpd	%xmm2, %xmm4
	movapd	 4 * SIZE(BB), %xmm2
	addpd	%xmm0, %xmm5
	movapd	 2 * SIZE(AA), %xmm0

	addl	$2 * SIZE, AA		# aoffset  += 8
	addl	$4 * SIZE, BB		# boffset1 += 8
	decl	%eax
	BRANCH
	jg	.L33
	ALIGN_4

.L34:
	addpd	%xmm6, %xmm4
	addpd	%xmm7, %xmm5

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$2, %eax
#else
	subl	$2, %eax
#endif

	movl	AORIG, AA
	movl	BORIG, B
	leal	BUFFER, BB

	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 2), B
	leal	(BB, %eax, 4), BB
#endif

#if defined(LN) || defined(LT)
	movapd	 %xmm4, %xmm0
	unpcklpd %xmm5, %xmm4
	unpckhpd %xmm5, %xmm0

	movapd	 0 * SIZE(B), %xmm2
	movapd	 2 * SIZE(B), %xmm3

	subpd	%xmm4,  %xmm2
	subpd	%xmm0,  %xmm3
#else
	movapd	 0 * SIZE(AA), %xmm0
	movapd	 2 * SIZE(AA), %xmm1

	subpd	%xmm4, %xmm0
	subpd	%xmm5, %xmm1
#endif

#ifdef LN
	movsd	 3 * SIZE(AA), %xmm0
	movhpd	 3 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm3

	movsd	 2 * SIZE(AA), %xmm0
	movhpd	 2 * SIZE(AA), %xmm0
	mulpd	 %xmm3, %xmm0
	subpd	 %xmm0, %xmm2

	movsd	 0 * SIZE(AA), %xmm0
	movhpd	 0 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm2
#endif

#ifdef LT
	movsd	 0 * SIZE(AA), %xmm0
	movhpd	 0 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm2
	movsd	 1 * SIZE(AA), %xmm0
	movhpd	 1 * SIZE(AA), %xmm0
	mulpd	 %xmm2, %xmm0
	subpd	 %xmm0, %xmm3
	movsd	 3 * SIZE(AA), %xmm0
	movhpd	 3 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm3
#endif

#ifdef RN
	movsd	 0 * SIZE(B), %xmm4
	movhpd	 0 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm0

	movsd	 1 * SIZE(B), %xmm4
	movhpd	 1 * SIZE(B), %xmm4
	mulpd	 %xmm0, %xmm4
	subpd	 %xmm4, %xmm1

	movsd	 3 * SIZE(B), %xmm4
	movhpd	 3 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm1
#endif

#ifdef RT
	movsd	 3 * SIZE(B), %xmm4
	movhpd	 3 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm1

	movsd	 2 * SIZE(B), %xmm4
	movhpd	 2 * SIZE(B), %xmm4
	mulpd	 %xmm1, %xmm4
	subpd	 %xmm4, %xmm0

	movsd	 0 * SIZE(B), %xmm4
	movhpd	 0 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm2,   0 * SIZE(B)
	movapd	%xmm3,   2 * SIZE(B)

	movsd	%xmm2,   0 * SIZE(BB)
	movsd	%xmm2,   1 * SIZE(BB)
	movhpd	%xmm2,   2 * SIZE(BB)
	movhpd	%xmm2,   3 * SIZE(BB)
	movsd	%xmm3,   4 * SIZE(BB)
	movsd	%xmm3,   5 * SIZE(BB)
	movhpd	%xmm3,   6 * SIZE(BB)
	movhpd	%xmm3,   7 * SIZE(BB)
#else
	movapd	%xmm0,   0 * SIZE(AA)
	movapd	%xmm1,   2 * SIZE(AA)
#endif

#ifdef LN
	subl	$2 * SIZE, %esi
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm2,  0 * SIZE(%esi)
	movsd	%xmm3,  1 * SIZE(%esi)
	movhpd	%xmm2,  0 * SIZE(%esi, LDC)
	movhpd	%xmm3,  1 * SIZE(%esi, LDC)
#else
	movsd	%xmm0,  0 * SIZE(%esi)
	movhpd	%xmm0,  1 * SIZE(%esi)
	movsd	%xmm1,  0 * SIZE(%esi, LDC)
	movhpd	%xmm1,  1 * SIZE(%esi, LDC)
#endif

#ifndef LN
	addl	$2 * SIZE, %esi
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
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
	sall	$1 + BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif
	ALIGN_2

.L50:
	movl	M,  %ebx
	testl	$1, %ebx
	jle	.L99

#ifdef LN
       movl	K, %eax
       sall	$0 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
#endif

	leal	BUFFER, %ecx

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + BASE_SHIFT, %eax
	leal	(BB, %eax, 2), BB
#endif

	movsd	 0 * SIZE(BB), %xmm2
	pxor	%xmm4, %xmm4
	movsd	 0 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movsd	 8 * SIZE(BB), %xmm3
	pxor	%xmm6, %xmm6
	movsd	 4 * SIZE(AA), %xmm1
	pxor	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L52

.L51:
	mulsd	%xmm0, %xmm2
	mulsd	 2 * SIZE(BB), %xmm0
	addsd	%xmm2, %xmm4
	movsd	 4 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm5
	movsd	 1 * SIZE(AA), %xmm0
	mulsd	%xmm0, %xmm2
	mulsd	 6 * SIZE(BB), %xmm0
	addsd	%xmm2, %xmm4
	movsd	16 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm5
	movsd	 2 * SIZE(AA), %xmm0
	mulsd	%xmm0, %xmm3
	mulsd	10 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm4
	movsd	12 * SIZE(BB), %xmm3
	addsd	%xmm0, %xmm5
	movsd	 3 * SIZE(AA), %xmm0
	mulsd	%xmm0, %xmm3
	mulsd	14 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm4
	movsd	24 * SIZE(BB), %xmm3
	addsd	%xmm0, %xmm5
	movsd	 8 * SIZE(AA), %xmm0
	mulsd	%xmm1, %xmm2
	mulsd	18 * SIZE(BB), %xmm1
	addsd	%xmm2, %xmm4
	movsd	20 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm5
	movsd	 5 * SIZE(AA), %xmm1
	mulsd	%xmm1, %xmm2
	mulsd	22 * SIZE(BB), %xmm1
	addsd	%xmm2, %xmm4
	movsd	32 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm5
	movsd	 6 * SIZE(AA), %xmm1
	mulsd	%xmm1, %xmm3
	mulsd	26 * SIZE(BB), %xmm1
	addsd	%xmm3, %xmm4
	movsd	28 * SIZE(BB), %xmm3
	addsd	%xmm1, %xmm5
	movsd	 7 * SIZE(AA), %xmm1
	mulsd	%xmm1, %xmm3
	mulsd	30 * SIZE(BB), %xmm1
	addsd	%xmm3, %xmm4
	movsd	40 * SIZE(BB), %xmm3
	addsd	%xmm1, %xmm5
	movsd	12 * SIZE(AA), %xmm1

	addl   $ 8 * SIZE, AA
	addl   $32 * SIZE, BB
	BRANCH
	decl   %eax
	jne    .L51

.L52:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L54

.L53:
	mulsd	%xmm0, %xmm2
	mulsd	 2 * SIZE(BB), %xmm0
	addsd	%xmm2, %xmm4
	movsd	 4 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm5
	movsd	 1 * SIZE(AA), %xmm0

	addl	$1 * SIZE, AA		# aoffset  += 8
	addl	$4 * SIZE, BB		# boffset1 += 8
	decl	%eax
	BRANCH
	jg	.L53
	ALIGN_4

.L54:
	addsd	%xmm6, %xmm4
	addsd	%xmm7, %xmm5

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$2, %eax
#endif

	movl	AORIG, AA
	movl	BORIG, B
	leal	BUFFER, BB

	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 2), B
	leal	(BB, %eax, 4), BB
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(B), %xmm0
	movsd	 1 * SIZE(B), %xmm1
#else
	movsd	 0 * SIZE(AA), %xmm0
	movsd	 1 * SIZE(AA), %xmm1
#endif

	subsd	%xmm4,  %xmm0
	subsd	%xmm5,  %xmm1

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(AA), %xmm2
	mulsd	 %xmm2, %xmm0
	mulsd	 %xmm2, %xmm1
#endif

#ifdef RN
	mulsd	 0 * SIZE(B), %xmm0
	movsd	 1 * SIZE(B), %xmm4
	mulsd	 %xmm0, %xmm4
	subsd	 %xmm4, %xmm1
	mulsd	 3 * SIZE(B), %xmm1
#endif

#ifdef RT
	mulsd	 3 * SIZE(B), %xmm1
	movsd	 2 * SIZE(B), %xmm4
	mulsd	 %xmm1, %xmm4
	subsd	 %xmm4, %xmm0

	mulsd	 0 * SIZE(B), %xmm0
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm0,   0 * SIZE(B)
	movsd	%xmm1,   1 * SIZE(B)

	movsd	%xmm0,   0 * SIZE(BB)
	movsd	%xmm0,   1 * SIZE(BB)
	movsd	%xmm1,   2 * SIZE(BB)
	movsd	%xmm1,   3 * SIZE(BB)
#else
	movsd	%xmm0,   0 * SIZE(AA)
	movsd	%xmm1,   1 * SIZE(AA)
#endif

#ifdef LN
	subl	$1 * SIZE, %esi
#endif

	movsd	%xmm0,  0 * SIZE(%esi)
	movsd	%xmm1,  0 * SIZE(%esi, LDC)

#ifndef LN
	addl	$1 * SIZE, %esi
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
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
	sall	$0 + BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif
	ALIGN_2

.L99:
#ifdef LN
       movl	K, %eax
       leal	(, %eax, SIZE), %eax
       leal 	(B, %eax, 2), B
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(B,  %eax, 2), B
#endif

#ifdef RN
	addl	$2, KK
#endif

#ifdef RT
	subl	$2, KK
#endif

	decl	J			# j --
	jg	.L01
	ALIGN_2

.L100:
	movl	N, %eax
	testl	$1, %eax
	jle	.L999
	ALIGN_2

.L101:
/* Copying to Sub Buffer */
#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

	leal	BUFFER, %ecx

#ifdef RT
       movl	K, %eax
       sall	$0 + BASE_SHIFT, %eax
       subl	%eax, B
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	B, BORIG
	leal	(, %eax, SIZE), %eax
	leal	(B,  %eax, 1), B
	leal	(BB, %eax, 2), BB
#endif

#ifdef LT
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	jle	.L103
	ALIGN_4

.L102:
	movsd	 0 * SIZE(B), %xmm0
	movsd	 1 * SIZE(B), %xmm1
	movsd	 2 * SIZE(B), %xmm2
	movsd	 3 * SIZE(B), %xmm3
	movsd	 4 * SIZE(B), %xmm4
	movsd	 5 * SIZE(B), %xmm5
	movsd	 6 * SIZE(B), %xmm6
	movsd	 7 * SIZE(B), %xmm7

	unpcklpd  %xmm0, %xmm0
	unpcklpd  %xmm1, %xmm1
	unpcklpd  %xmm2, %xmm2
	unpcklpd  %xmm3, %xmm3
	unpcklpd  %xmm4, %xmm4
	unpcklpd  %xmm5, %xmm5
	unpcklpd  %xmm6, %xmm6
	unpcklpd  %xmm7, %xmm7

	movapd	%xmm0,  0 * SIZE(%ecx)
	movapd	%xmm1,  2 * SIZE(%ecx)
	movapd	%xmm2,  4 * SIZE(%ecx)
	movapd	%xmm3,  6 * SIZE(%ecx)
	movapd	%xmm4,  8 * SIZE(%ecx)
	movapd	%xmm5, 10 * SIZE(%ecx)
	movapd	%xmm6, 12 * SIZE(%ecx)
	movapd	%xmm7, 14 * SIZE(%ecx)

	prefetcht0	104 * SIZE(B)

	addl	$ 8 * SIZE, B
	addl	$16 * SIZE, %ecx
	decl	%eax
	BRANCH
	jne	.L102
	ALIGN_2

.L103:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax
	BRANCH
	jle	.L105
	ALIGN_2

.L104:
	movsd	 0 * SIZE(B), %xmm0

	unpcklpd  %xmm0, %xmm0

	movapd	%xmm0,  0 * SIZE(%ecx)

	addl	$1 * SIZE, B
	addl	$2 * SIZE, %ecx
	decl	%eax
	jne	.L104
	ALIGN_4

.L105:
#if defined(LT) || defined(RN)
	movl	A, AA
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

#ifdef RT
	subl	LDC, C
#endif
	movl	C, %esi		# coffset = c
#ifndef RT
	addl	LDC, C
#endif

	movl	M,  %ebx
	sarl	$2, %ebx	# i = (m >> 2)
	jle	.L130
	ALIGN_4

.L110:
#ifdef LN
       movl	K, %eax
       sall	$2 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$0 + BASE_SHIFT, %eax
	leal	(BB, %eax, 2), BB
#endif

	movapd	 0 * SIZE(BB), %xmm2
	pxor	%xmm4, %xmm4
	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movapd	 8 * SIZE(BB), %xmm3
	pxor	%xmm6, %xmm6
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L112

.L111:
	mulpd	%xmm2, %xmm0
	mulpd	 2 * SIZE(AA), %xmm2
	addpd	%xmm0, %xmm4
	movapd	 4 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm6
	movapd	 2 * SIZE(BB), %xmm2
	mulpd	%xmm2, %xmm0
	mulpd	 6 * SIZE(AA), %xmm2
	addpd	%xmm0, %xmm5
	movapd	16 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm7
	movapd	 4 * SIZE(BB), %xmm2
	mulpd	%xmm2, %xmm1
	mulpd	10 * SIZE(AA), %xmm2
	addpd	%xmm1, %xmm4
	movapd	12 * SIZE(AA), %xmm1
	addpd	%xmm2, %xmm6
	movapd	 6 * SIZE(BB), %xmm2
	mulpd	%xmm2, %xmm1
	mulpd	14 * SIZE(AA), %xmm2
	addpd	%xmm1, %xmm5
	movapd	24 * SIZE(AA), %xmm1
	addpd	%xmm2, %xmm7
	movapd	16 * SIZE(BB), %xmm2
	mulpd	%xmm3, %xmm0
	mulpd	18 * SIZE(AA), %xmm3
	addpd	%xmm0, %xmm4
	movapd	20 * SIZE(AA), %xmm0
	addpd	%xmm3, %xmm6
	movapd	10 * SIZE(BB), %xmm3
	mulpd	%xmm3, %xmm0
	mulpd	22 * SIZE(AA), %xmm3
	addpd	%xmm0, %xmm5
	movapd	32 * SIZE(AA), %xmm0
	addpd	%xmm3, %xmm7
	movapd	12 * SIZE(BB), %xmm3
	mulpd	%xmm3, %xmm1
	mulpd	26 * SIZE(AA), %xmm3
	addpd	%xmm1, %xmm4
	movapd	28 * SIZE(AA), %xmm1
	addpd	%xmm3, %xmm6
	movapd	14 * SIZE(BB), %xmm3
	mulpd	%xmm3, %xmm1
	mulpd	30 * SIZE(AA), %xmm3
	addpd	%xmm1, %xmm5
	movapd	40 * SIZE(AA), %xmm1
	addpd	%xmm3, %xmm7
	movapd	24 * SIZE(BB), %xmm3

	addl   $32 * SIZE, AA
	addl   $16 * SIZE, BB
	decl   %eax
	jne    .L111

.L112:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L114

.L113:
	mulpd	%xmm2, %xmm0
	mulpd	 2 * SIZE(AA), %xmm2
	addpd	%xmm0, %xmm4
	movapd	 4 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm6
	movapd	 2 * SIZE(BB), %xmm2

	addl	$4 * SIZE, AA		# aoffset  += 8
	addl	$2 * SIZE, BB		# boffset1 += 8
	subl	$1, %eax
	jg	.L113
	ALIGN_4

.L114:
	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$4, %eax
#else
	subl	$1, %eax
#endif

	movl	AORIG, AA
	movl	BORIG, B
	leal	BUFFER, BB

	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(B,  %eax, 1), B
	leal	(BB, %eax, 2), BB
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(B), %xmm0
	movapd	 2 * SIZE(B), %xmm1
#else
	movapd	 0 * SIZE(AA), %xmm0
	movapd	 2 * SIZE(AA), %xmm1
#endif

	subpd	%xmm4, %xmm0
	subpd	%xmm6, %xmm1

#ifdef LN
	movapd	%xmm0, %xmm2
        unpckhpd %xmm2, %xmm2

	movapd	%xmm1, %xmm3
        unpckhpd %xmm3, %xmm3

	movsd	15 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm3

	movsd	14 * SIZE(AA), %xmm5
	mulsd	 %xmm3, %xmm5
	subsd	 %xmm5, %xmm1
	movsd	13 * SIZE(AA), %xmm6
	mulsd	 %xmm3, %xmm6
	subsd	 %xmm6, %xmm2
	movsd	12 * SIZE(AA), %xmm7
	mulsd	 %xmm3, %xmm7
	subsd	 %xmm7, %xmm0

	movsd	10 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm1

	movsd	 9 * SIZE(AA), %xmm5
	mulsd	 %xmm1, %xmm5
	subsd	 %xmm5, %xmm2
	movsd	 8 * SIZE(AA), %xmm6
	mulsd	 %xmm1, %xmm6
	subsd	 %xmm6, %xmm0

	movsd	 5 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm2

	movsd	 4 * SIZE(AA), %xmm5
	mulsd	 %xmm2, %xmm5
	subsd	 %xmm5, %xmm0

	movsd	 0 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm0

	unpcklpd %xmm2, %xmm0
	unpcklpd %xmm3, %xmm1
#endif

#ifdef LT
	movapd	%xmm0, %xmm2
        unpckhpd %xmm2, %xmm2

	movapd	%xmm1, %xmm3
        unpckhpd %xmm3, %xmm3

	movsd	 0 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm0

	movsd	 1 * SIZE(AA), %xmm5
	mulsd	 %xmm0, %xmm5
	subsd	 %xmm5, %xmm2
	movsd	 2 * SIZE(AA), %xmm6
	mulsd	 %xmm0, %xmm6
	subsd	 %xmm6, %xmm1
	movsd	 3 * SIZE(AA), %xmm7
	mulsd	 %xmm0, %xmm7
	subsd	 %xmm7, %xmm3

	movsd	 5 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm2

	movsd	 6 * SIZE(AA), %xmm5
	mulsd	 %xmm2, %xmm5
	subsd	 %xmm5, %xmm1
	movsd	 7 * SIZE(AA), %xmm6
	mulsd	 %xmm2, %xmm6
	subsd	 %xmm6, %xmm3

	movsd	10 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm1

	movsd	11 * SIZE(AA), %xmm5
	mulsd	 %xmm1, %xmm5
	subsd	 %xmm5, %xmm3

	movsd	15 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm3

	unpcklpd %xmm2, %xmm0
	unpcklpd %xmm3, %xmm1
#endif

#if defined(RN) || defined(RT)
	movsd	 0 * SIZE(B), %xmm4
	movhpd	 0 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm0
	mulpd	 %xmm4, %xmm1
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm0,   0 * SIZE(B)
	movapd	%xmm1,   2 * SIZE(B)

	movsd	%xmm0,   0 * SIZE(BB)
	movsd	%xmm0,   1 * SIZE(BB)
	movhpd	%xmm0,   2 * SIZE(BB)
	movhpd	%xmm0,   3 * SIZE(BB)
	movsd	%xmm1,   4 * SIZE(BB)
	movsd	%xmm1,   5 * SIZE(BB)
	movhpd	%xmm1,   6 * SIZE(BB)
	movhpd	%xmm1,   7 * SIZE(BB)
#else
	movapd	%xmm0,   0 * SIZE(AA)
	movapd	%xmm1,   2 * SIZE(AA)
#endif

#ifdef LN
	subl	$4 * SIZE, %esi
#endif

	movsd	%xmm0,  0 * SIZE(%esi)
	movhpd	%xmm0,  1 * SIZE(%esi)
	movsd	%xmm1,  2 * SIZE(%esi)
	movhpd	%xmm1,  3 * SIZE(%esi)

#ifndef LN
	addl	$4 * SIZE, %esi
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
#ifdef LT
	addl	$4 * SIZE, B
#endif
#endif

#ifdef LN
	subl	$4, KK
	movl	BORIG, B
#endif

#ifdef LT
	addl	$4, KK
#endif

#ifdef RT
	movl	K, %eax
	movl	BORIG, B
	sall	$2 + BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	BRANCH
	decl	%ebx			# i --
	jg	.L110
	ALIGN_2

.L130:
	movl	M,  %ebx
	testl	$2, %ebx
	jle	.L150

#ifdef LN
       movl	K, %eax
       sall	$1 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
#endif

	leal	BUFFER, BB

	movapd	 0 * SIZE(BB), %xmm2
	pxor	%xmm4, %xmm4
	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movapd	 8 * SIZE(BB), %xmm3
	pxor	%xmm6, %xmm6
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm7, %xmm7

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$0 + BASE_SHIFT, %eax
	leal	(BB, %eax, 2), BB
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L132

.L131:
	mulpd	%xmm0, %xmm2
	movapd	 2 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm4
	mulpd	 2 * SIZE(BB), %xmm0
	movapd	16 * SIZE(BB), %xmm2
	addpd	%xmm0, %xmm5
	movapd	 4 * SIZE(AA), %xmm0
	mulpd	 4 * SIZE(BB), %xmm0
	addpd	%xmm0, %xmm6
	movapd	 6 * SIZE(AA), %xmm0
	mulpd	 6 * SIZE(BB), %xmm0
	addpd	%xmm0, %xmm7
	movapd	16 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm3
	movapd	10 * SIZE(AA), %xmm1
	addpd	%xmm3, %xmm4
	mulpd	10 * SIZE(BB), %xmm1
	movapd	24 * SIZE(BB), %xmm3
	addpd	%xmm1, %xmm5
	movapd	12 * SIZE(AA), %xmm1
	mulpd	12 * SIZE(BB), %xmm1
	addpd	%xmm1, %xmm6
	movapd	14 * SIZE(AA), %xmm1
	mulpd	14 * SIZE(BB), %xmm1
	addpd	%xmm1, %xmm7
	movapd	24 * SIZE(AA), %xmm1

	addl   $16 * SIZE, AA
	addl   $16 * SIZE, BB
	BRANCH
	decl   %eax
	jne    .L131

.L132:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L134

.L133:
	movapd	 0 * SIZE(AA), %xmm0
	mulpd	 0 * SIZE(BB), %xmm0
	addpd	%xmm0, %xmm4

	addl	$2 * SIZE, AA		# aoffset  += 8
	addl	$2 * SIZE, BB		# boffset1 += 8
	decl	%eax
	BRANCH
	jg	.L133
	ALIGN_4

.L134:
	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6
	addpd	%xmm6, %xmm4

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

	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 1), B
	leal	(BB, %eax, 2), BB
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(B), %xmm0
#else
	movapd	 0 * SIZE(AA), %xmm0
#endif

	subpd	%xmm4, %xmm0

#ifdef LN
	movapd	%xmm0, %xmm2
        unpckhpd %xmm2, %xmm2

	movsd	 3 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm2

	movsd	 2 * SIZE(AA), %xmm5
	mulsd	 %xmm2, %xmm5
	subsd	 %xmm5, %xmm0

	movsd	 0 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm0

	unpcklpd %xmm2, %xmm0
#endif

#ifdef LT
	movapd	%xmm0, %xmm2
        unpckhpd %xmm2, %xmm2

	movsd	 0 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm0

	movsd	 1 * SIZE(AA), %xmm5
	mulsd	 %xmm0, %xmm5
	subsd	 %xmm5, %xmm2
	movsd	 3 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm2

	unpcklpd %xmm2, %xmm0
#endif

#if defined(RN) || defined(RT)
	movsd	 0 * SIZE(B), %xmm4
	movhpd	 0 * SIZE(B), %xmm4
	mulpd	 %xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm0,   0 * SIZE(B)

	movsd	%xmm0,   0 * SIZE(BB)
	movsd	%xmm0,   1 * SIZE(BB)
	movhpd	%xmm0,   2 * SIZE(BB)
	movhpd	%xmm0,   3 * SIZE(BB)
#else
	movapd	%xmm0,   0 * SIZE(AA)
#endif

#ifdef LN
	subl	$2 * SIZE, %esi
#endif

	movsd	%xmm0,  0 * SIZE(%esi)
	movhpd	%xmm0,  1 * SIZE(%esi)

#ifndef LN
	addl	$2 * SIZE, %esi
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
#ifdef LT
	addl	$2 * SIZE, B
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
	sall	$1 + BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif
	ALIGN_2

.L150:
	movl	M,  %ebx
	testl	$1, %ebx
	jle	.L159

#ifdef LN
       movl	K, %eax
       sall	$0 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
#endif

	leal	BUFFER, BB

	movsd	 0 * SIZE(BB), %xmm2
	pxor	%xmm4, %xmm4
	movsd	 0 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movsd	 8 * SIZE(BB), %xmm3
	pxor	%xmm6, %xmm6
	movsd	 4 * SIZE(AA), %xmm1
	pxor	%xmm7, %xmm7

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$0 + BASE_SHIFT, %eax
	leal	(BB, %eax, 2), BB
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L152

.L151:
	mulsd	%xmm0, %xmm2
	movsd	 1 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm4
	mulsd	 2 * SIZE(BB), %xmm0
	movsd	16 * SIZE(BB), %xmm2
	addsd	%xmm0, %xmm4
	movsd	 2 * SIZE(AA), %xmm0
	mulsd	 4 * SIZE(BB), %xmm0
	addsd	%xmm0, %xmm4
	movsd	 3 * SIZE(AA), %xmm0
	mulsd	 6 * SIZE(BB), %xmm0
	addsd	%xmm0, %xmm4
	movsd	 8 * SIZE(AA), %xmm0
	mulsd	%xmm1, %xmm3
	movsd	 5 * SIZE(AA), %xmm1
	addsd	%xmm3, %xmm4
	mulsd	10 * SIZE(BB), %xmm1
	movsd	24 * SIZE(BB), %xmm3
	addsd	%xmm1, %xmm4
	movsd	 6 * SIZE(AA), %xmm1
	mulsd	12 * SIZE(BB), %xmm1
	addsd	%xmm1, %xmm4
	movsd	 7 * SIZE(AA), %xmm1
	mulsd	14 * SIZE(BB), %xmm1
	addsd	%xmm1, %xmm4
	movsd	12 * SIZE(AA), %xmm1

	addl   $ 8 * SIZE, AA
	addl   $16 * SIZE, BB
	BRANCH
	decl   %eax
	jne    .L151

.L152:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L154

.L153:
	movsd	 0 * SIZE(AA), %xmm0
	mulsd	 0 * SIZE(BB), %xmm0
	addsd	%xmm0, %xmm4

	addl	$1 * SIZE, AA		# aoffset  += 8
	addl	$2 * SIZE, BB		# boffset1 += 8
	decl	%eax
	BRANCH
	jg	.L153
	ALIGN_4

.L154:
	addsd	%xmm6, %xmm4
	addsd	%xmm7, %xmm5

#if defined(LN) || defined(RT)
	movl	KK, %eax
	subl	$1, %eax

	movl	AORIG, AA
	movl	BORIG, B
	leal	BUFFER, BB

	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 1), B
	leal	(BB, %eax, 2), BB
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(B), %xmm0
#else
	movsd	 0 * SIZE(AA), %xmm0
#endif

	subsd	%xmm4, %xmm0

#if defined(LN) || defined(LT)
	mulsd	 0 * SIZE(AA), %xmm0
#endif

#if defined(RN) || defined(RT)
	mulsd	 0 * SIZE(B), %xmm0
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm0,   0 * SIZE(B)

	movsd	%xmm0,   0 * SIZE(BB)
	movsd	%xmm0,   1 * SIZE(BB)
#else
	movsd	%xmm0,   0 * SIZE(AA)
#endif

#ifdef LN
	subl	$1 * SIZE, %esi
#endif

	movsd	%xmm0,  0 * SIZE(%esi)

#ifndef LN
	addl	$1 * SIZE, %esi
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
#ifdef LT
	addl	$1 * SIZE, B
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
	sall	$0 + BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif
	ALIGN_2

.L159:
#ifdef LN
       movl	K, %eax
       leal	(, %eax, SIZE), %eax
       leal 	(B, %eax, 1), B
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(B,  %eax, 1), B
#endif

#ifdef RN
	addl	$1, KK
#endif

#ifdef RT
	subl	$1, KK
#endif
	ALIGN_2

.L999:
	movl	OLD_STACK, %esp

	EMMS

	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret
	ALIGN_2


	EPILOGUE
