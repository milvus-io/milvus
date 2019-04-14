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

#if !defined(HAVE_SSE) || !defined(HAVE_MMX)
#error  You have to check your configuration.
#endif

#define STACK	16
#define ARGS	 0

#define STACK_M	 4 + STACK + ARGS(%esi)
#define STACK_N	 8 + STACK + ARGS(%esi)
#define STACK_K	12 + STACK + ARGS(%esi)
#define STACK_A	20 + STACK + ARGS(%esi)
#define STACK_B	24 + STACK + ARGS(%esi)
#define STACK_C	28 + STACK + ARGS(%esi)
#define STACK_LDC	32 + STACK + ARGS(%esi)
#define STACK_OFFT	36 + STACK + ARGS(%esi)

#define TRMASK	 0(%esp)
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

#ifdef HAVE_3DNOW
#define PREFETCH     prefetch
#define PREFETCHW    prefetchw
#define PREFETCHSIZE (16 * 10 + 8)
#else
#define PREFETCH     prefetcht0
#define PREFETCHW    prefetcht0
#define PREFETCHSIZE   96
#endif

#define B	%edi
#define AA	%edx
#define	BB	%ecx
#define LDC	%ebp
#define CO1	%esi

#define STACK_ALIGN	4096
#define STACK_OFFSET	1024

#if !defined(HAVE_SSE2) || defined(OPTERON)
#define movsd	movlps
#endif

#ifdef HAVE_SSE2
#define xorps	pxor
#endif

	PROLOGUE

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	%esp, %esi	# save old stack

	subl	$128 + LOCAL_BUFFER_SIZE, %esp
	andl	$-STACK_ALIGN, %esp

	STACK_TOUCHING

	movss	STACK_M, %xmm0
	movl	STACK_N, %eax
	movss	STACK_K, %xmm1
	movss	STACK_A, %xmm2
	movl	STACK_B, B
	movss	STACK_C, %xmm3
	movl	STACK_LDC, LDC
	movss	STACK_OFFT, %xmm4

	movss	%xmm1, K
	movl	%eax,  N
	movss	%xmm0, M
	movss	%xmm2, A
	movss	%xmm3, C
	movl	%esi,  OLD_STACK
	movss	%xmm4, OFFSET
	movss	%xmm4, KK

	leal	(, LDC, SIZE), LDC

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

#if defined(LN) || defined(LT)
	movl	  $0x3f800000,   0 + TRMASK	#  1.0
	movl	  $0x00000000,   4 + TRMASK	#  0.0
	movl	  $0x3f800000,   8 + TRMASK	#  1.0
	movl	  $0x00000000,  12 + TRMASK	#  0.0
#endif

	movl	N,  %eax
	sarl	$1, %eax	# j = (n >> 1)
	movl	%eax, J
	jle	.L100
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
       sall	$1 + BASE_SHIFT, %eax
       subl	%eax, B
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	B, BORIG
        sall	$1 + BASE_SHIFT, %eax
	leal	(B,  %eax, 1), B
	leal	(BB, %eax, 4), BB
#endif

#ifdef LT
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
	ALIGN_4

.L02:
	movsd	 0 * SIZE(B), %xmm3
	movhps	 2 * SIZE(B), %xmm3
	movsd	 4 * SIZE(B), %xmm7
	movhps	 6 * SIZE(B), %xmm7

#ifdef HAVE_SSE2
	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

	pshufd	 $0x00, %xmm7, %xmm4
	pshufd	 $0x55, %xmm7, %xmm5
	pshufd	 $0xaa, %xmm7, %xmm6
	pshufd	 $0xff, %xmm7, %xmm7
#else
	movaps	%xmm3, %xmm0
	shufps	 $0x00, %xmm0, %xmm0
	movaps	%xmm3, %xmm1
	shufps	 $0x55, %xmm1, %xmm1
	movaps	%xmm3, %xmm2
	shufps	 $0xaa, %xmm2, %xmm2
	shufps	 $0xff, %xmm3, %xmm3

	movaps	%xmm7, %xmm4
	shufps	 $0x00, %xmm4, %xmm4
	movaps	%xmm7, %xmm5
	shufps	 $0x55, %xmm5, %xmm5
	movaps	%xmm7, %xmm6
	shufps	 $0xaa, %xmm6, %xmm6
	shufps	 $0xff, %xmm7, %xmm7
#endif

	movaps	%xmm0,  0 * SIZE(BB)
	movaps	%xmm1,  4 * SIZE(BB)
	movaps	%xmm2,  8 * SIZE(BB)
	movaps	%xmm3, 12 * SIZE(BB)
	movaps	%xmm4, 16 * SIZE(BB)
	movaps	%xmm5, 20 * SIZE(BB)
	movaps	%xmm6, 24 * SIZE(BB)
	movaps	%xmm7, 28 * SIZE(BB)

	addl	$ 8 * SIZE, B
	addl	$32 * SIZE, BB
	decl	%eax
	BRANCH
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
	movsd	 0 * SIZE(B), %xmm3

#ifdef HAVE_SSE2
	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
#else
	movaps	%xmm3, %xmm0
	shufps	 $0x00, %xmm0, %xmm0
	movaps	%xmm3, %xmm1
	shufps	 $0x55, %xmm1, %xmm1
#endif

	movaps	%xmm0,  0 * SIZE(BB)
	movaps	%xmm1,  4 * SIZE(BB)

	addl	$2 * SIZE, B
	addl	$8 * SIZE, BB

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
	movl	C, CO1
#ifndef RT
	addl	%eax, C
#endif

	testl	$1, M
	jle	.L30

#ifdef LN
       movl	K, %eax
       sall	$BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	sall	$BASE_SHIFT, %eax
	addl	%eax, AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + BASE_SHIFT, %eax
	leal	(BB, %eax, 4), BB
#endif

	movss	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
	movss	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movss	 16 * SIZE(BB), %xmm3
	xorps	%xmm6, %xmm6
	movss	 4 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L72
	ALIGN_2

.L71:
	mulss	%xmm0, %xmm2
	mulss	 4 * SIZE(BB), %xmm0
	addss	%xmm2, %xmm4
	movss	 8 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm5
	movss	 1 * SIZE(AA), %xmm0
	mulss	%xmm0, %xmm2
	mulss	12 * SIZE(BB), %xmm0
	addss	%xmm2, %xmm6
	movss	32 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm7
	movss	 2 * SIZE(AA), %xmm0
	mulss	%xmm0, %xmm3
	mulss	20 * SIZE(BB), %xmm0
	addss	%xmm3, %xmm4
	movss	24 * SIZE(BB), %xmm3
	addss	%xmm0, %xmm5
	movss	 3 * SIZE(AA), %xmm0
	mulss	%xmm0, %xmm3
	mulss	28 * SIZE(BB), %xmm0
	addss	%xmm3, %xmm6
	movss	48 * SIZE(BB), %xmm3
	addss	%xmm0, %xmm7
	movss	 8 * SIZE(AA), %xmm0
	mulss	%xmm1, %xmm2
	mulss	36 * SIZE(BB), %xmm1
	addss	%xmm2, %xmm4
	movss	40 * SIZE(BB), %xmm2
	addss	%xmm1, %xmm5
	movss	 5 * SIZE(AA), %xmm1
	mulss	%xmm1, %xmm2
	mulss	44 * SIZE(BB), %xmm1
	addss	%xmm2, %xmm6
	movss	64 * SIZE(BB), %xmm2
	addss	%xmm1, %xmm7
	movss	 6 * SIZE(AA), %xmm1
	mulss	%xmm1, %xmm3
	mulss	52 * SIZE(BB), %xmm1
	addss	%xmm3, %xmm4
	movss	56 * SIZE(BB), %xmm3
	addss	%xmm1, %xmm5
	movss	 7 * SIZE(AA), %xmm1
	mulss	%xmm1, %xmm3
	mulss	60 * SIZE(BB), %xmm1
	addss	%xmm3, %xmm6
	movss	80 * SIZE(BB), %xmm3
	addss	%xmm1, %xmm7
	movss	12 * SIZE(AA), %xmm1

	addl   $ 8 * SIZE, AA
	addl   $64 * SIZE, BB
	decl   %eax
	jne    .L71
	ALIGN_2

.L72:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L74

.L73:
	mulss	%xmm0, %xmm2
	mulss	 4 * SIZE(BB), %xmm0
	addss	%xmm2, %xmm4
	movss	 8 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm5
	movss	 1 * SIZE(AA), %xmm0

	addl	$1 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L73
	ALIGN_4

.L74:
	addss	%xmm6, %xmm4
	addss	%xmm7, %xmm5

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

	sall	$BASE_SHIFT, %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 2), B
	leal	(BB, %eax, 8), BB
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm5, %xmm4

#ifdef	movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	 0 * SIZE(B), %xmm2

	subps	%xmm4,  %xmm2
#else
	movss	 0 * SIZE(AA), %xmm0
	movss	 1 * SIZE(AA), %xmm2

	subss	%xmm4, %xmm0
	subss	%xmm5, %xmm2
#endif

#if defined(LN) || defined(LT)
	movaps	TRMASK, %xmm6
#endif

#if defined(LN) || defined(LT)
	movss	 0 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm2
#endif

#ifdef RN
	movss	 0 * SIZE(B), %xmm6
	mulss	%xmm6, %xmm0

	movss	 1 * SIZE(B), %xmm6
	movaps	%xmm6, %xmm5

	mulss	%xmm0, %xmm5
	subss	%xmm5, %xmm2

	movss	 3 * SIZE(B), %xmm6
	mulss	%xmm6, %xmm2
#endif

#ifdef RT
	movss	 3 * SIZE(B), %xmm6
	mulss	%xmm6, %xmm2

	movss	 2 * SIZE(B), %xmm6
	movaps	%xmm6, %xmm5

	mulss	%xmm2, %xmm5
	subss	%xmm5, %xmm0

	movss	 0 * SIZE(B), %xmm6
	mulss	%xmm6, %xmm0
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm2,   0 * SIZE(B)

	movaps	%xmm2, %xmm0
	shufps	$0x00, %xmm0, %xmm0
	movaps	%xmm2, %xmm1
	shufps	$0x55, %xmm1, %xmm1

	movaps	%xmm0,   0 * SIZE(BB)
	movaps	%xmm1,   4 * SIZE(BB)
#else
	movss	%xmm0,   0 * SIZE(AA)
	movss	%xmm2,   1 * SIZE(AA)
#endif

#ifdef LN
	subl	$1 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movaps	 %xmm2, %xmm0
	shufps	 $0x88, %xmm3, %xmm2
	shufps	 $0xdd, %xmm3, %xmm0

	movss	%xmm2, 0 * SIZE(CO1)
	movss	%xmm0, 0 * SIZE(CO1, LDC)
#else
	movss	%xmm0, 0 * SIZE(CO1)
	movss	%xmm2, 0 * SIZE(CO1, LDC)
#endif

#ifndef LN
	addl	$1 * SIZE, CO1
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
	sall	$BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif
	ALIGN_2

.L30:
	testl	$2, M
	jle	.L50

#ifdef LN
       movl	K, %eax
       sall	$1 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	sall	$1 + BASE_SHIFT, %eax
	addl	%eax, AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + BASE_SHIFT, %eax
	leal	(BB, %eax, 4), BB
#endif

	movaps	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 16 * SIZE(BB), %xmm3
	xorps	%xmm6, %xmm6
	movaps	 8 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L52
	ALIGN_2

.L51:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm6
	movaps	12 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm7
	movaps	32 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm4
	movaps	20 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movsd	 6 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	movaps	24 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm6
	movaps	28 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movsd	16 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm7
	movaps	48 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm4
	movaps	36 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movsd	10 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm5
	movaps	40 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm6
	movaps	44 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movsd	12 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm7
	movaps	64 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movaps	52 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	14 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movaps	56 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm6
	movaps	60 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	24 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm7
	movaps	80 * SIZE(BB), %xmm3

	addl   $16 * SIZE, AA
	addl   $64 * SIZE, BB
	decl   %eax
	jne    .L51
	ALIGN_2

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
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L53
	ALIGN_4

.L54:
	addps	%xmm6, %xmm4
	addps	%xmm7, %xmm5

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

	sall	$BASE_SHIFT, %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 2), B
	leal	(BB, %eax, 8), BB
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm5, %xmm4

	movsd	 0 * SIZE(B), %xmm2
	movhps	 2 * SIZE(B), %xmm2

	subps	%xmm4,  %xmm2
#else
#ifdef	movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	 0 * SIZE(AA), %xmm0
#ifdef	movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	 2 * SIZE(AA), %xmm2

	subps	%xmm4, %xmm0
	subps	%xmm5, %xmm2
#endif

#if defined(LN) || defined(LT)
	movaps	TRMASK, %xmm6
#endif

#ifdef LN
	movss	 3 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm2

	movaps	%xmm2, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movss	 2 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movss	 0 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm2

#endif

#ifdef LT
	movss	 0 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm2

	movaps	%xmm2, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movss	 1 * SIZE(AA), %xmm0
	shufps	$0x05, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movss	 3 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm2
#endif

#ifdef RN
	movss	 0 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6

	mulps	%xmm6, %xmm0

	movss	 1 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6
	movaps	%xmm6, %xmm5

	mulps	%xmm0, %xmm5
	subps	%xmm5, %xmm2

	movss	 3 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6

	mulps	%xmm6, %xmm2
#endif

#ifdef RT
	movss	 3 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6

	mulps	%xmm6, %xmm2

	movss	 2 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6
	movaps	%xmm6, %xmm5

	mulps	%xmm2, %xmm5

	subps	%xmm5, %xmm0

	movss	 0 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6

	mulps	%xmm6, %xmm0
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm2,   0 * SIZE(B)
	movhps	%xmm2,   2 * SIZE(B)

#ifdef HAVE_SSE2
	pshufd	$0x00, %xmm2, %xmm0
	pshufd	$0x55, %xmm2, %xmm1
	pshufd	$0xaa, %xmm2, %xmm4
	pshufd	$0xff, %xmm2, %xmm6
#else
	movaps	%xmm2, %xmm0
	shufps	$0x00, %xmm0, %xmm0
	movaps	%xmm2, %xmm1
	shufps	$0x55, %xmm1, %xmm1
	movaps	%xmm2, %xmm4
	shufps	$0xaa, %xmm4, %xmm4
	movaps	%xmm2, %xmm6
	shufps	$0xff, %xmm6, %xmm6
#endif

	movaps	%xmm0,   0 * SIZE(BB)
	movaps	%xmm1,   4 * SIZE(BB)
	movaps	%xmm4,   8 * SIZE(BB)
	movaps	%xmm6,  12 * SIZE(BB)
#else
	movlps	%xmm0,   0 * SIZE(AA)
	movlps	%xmm2,   2 * SIZE(AA)
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movaps	 %xmm2, %xmm0
	shufps	 $0x88, %xmm3, %xmm2
	shufps	 $0xdd, %xmm3, %xmm0

	movlps	%xmm2, 0 * SIZE(CO1)
	movlps	%xmm0, 0 * SIZE(CO1, LDC)
#else
	movlps	%xmm0, 0 * SIZE(CO1)
	movlps	%xmm2, 0 * SIZE(CO1, LDC)
#endif

#ifndef LN
	addl	$2 * SIZE, CO1
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
	testl	$4, M
	jle	.L70

#ifdef LN
       movl	K, %eax
       sall	$2 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	sall	$2 + BASE_SHIFT, %eax
	addl	%eax, AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + BASE_SHIFT, %eax
	leal	(BB, %eax, 4), BB
#endif

	movaps	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 16 * SIZE(BB), %xmm3
	xorps	%xmm6, %xmm6
	movaps	 16 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L32
	ALIGN_2

.L31:
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	 8 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	 4 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm2
	mulps	12 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm6
	movaps	32 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movaps	 8 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm3
	mulps	20 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm4
	movaps	24 * SIZE(BB), %xmm3
	addps	%xmm0, %xmm5
	movaps	12 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm3
	mulps	28 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm6
	movaps	48 * SIZE(BB), %xmm3
	addps	%xmm0, %xmm7
	movaps	32 * SIZE(AA), %xmm0
	mulps	%xmm1, %xmm2
	mulps	36 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm4
	movaps	40 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm5
	movaps	20 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm2
	mulps	44 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm6
	movaps	64 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm7
	movaps	24 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	mulps	52 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm4
	movaps	56 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm5
	movaps	28 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	mulps	60 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm6
	movaps	80 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movaps	48 * SIZE(AA), %xmm1

	addl   $32 * SIZE, AA
	addl   $64 * SIZE, BB
	decl   %eax
	jne    .L31
	ALIGN_2

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
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	 8 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	 4 * SIZE(AA), %xmm0

	addl	$4 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L33
	ALIGN_4

.L34:
	addps	%xmm6, %xmm4
	addps	%xmm7, %xmm5

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

	sall	$BASE_SHIFT, %eax
	leal	(AA, %eax, 4), AA
	leal	(B,  %eax, 2), B
	leal	(BB, %eax, 8), BB
#endif

#if defined(LN) || defined(LT)
	movaps	 %xmm4, %xmm0
	unpcklps %xmm5, %xmm4
	unpckhps %xmm5, %xmm0

	movsd	 0 * SIZE(B), %xmm2
	movhps	 2 * SIZE(B), %xmm2
	movsd	 4 * SIZE(B), %xmm3
	movhps	 6 * SIZE(B), %xmm3

	subps	%xmm4,  %xmm2
	subps	%xmm0,  %xmm3
#else
	movaps	 0 * SIZE(AA), %xmm0
	movaps	 4 * SIZE(AA), %xmm2

	subps	%xmm4, %xmm0
	subps	%xmm5, %xmm2
#endif

#if defined(LN) || defined(LT)
	movaps	TRMASK, %xmm6
#endif

#ifdef LN
	movss	15 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm3

	movaps	%xmm3, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movss	14 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movsd	12 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movss	10 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm3

	movaps	%xmm3, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movsd	 8 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movss	 5 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm2

	movaps	%xmm2, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movss	 4 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movss	 0 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm2

#endif

#ifdef LT
	movss	 0 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm2

	movaps	%xmm2, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movss	 1 * SIZE(AA), %xmm0
	shufps	$0x05, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movsd	 2 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movss	 5 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm2

	movaps	%xmm2, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movsd	 6 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movss	10 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm3

	movaps	%xmm3, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movss	11 * SIZE(AA), %xmm0
	shufps	$0x05, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movss	15 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm3
#endif

#ifdef RN
	movss	 0 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6

	mulps	%xmm6, %xmm0

	movss	 1 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6
	movaps	%xmm6, %xmm5

	mulps	%xmm0, %xmm5
	subps	%xmm5, %xmm2

	movss	 3 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6

	mulps	%xmm6, %xmm2
#endif

#ifdef RT
	movss	 3 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6

	mulps	%xmm6, %xmm2

	movss	 2 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6
	movaps	%xmm6, %xmm5

	mulps	%xmm2, %xmm5

	subps	%xmm5, %xmm0

	movss	 0 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6

	mulps	%xmm6, %xmm0
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm2,   0 * SIZE(B)
	movhps	%xmm2,   2 * SIZE(B)
	movlps	%xmm3,   4 * SIZE(B)
	movhps	%xmm3,   6 * SIZE(B)

#ifdef HAVE_SSE2
	pshufd	$0x00, %xmm2, %xmm0
	pshufd	$0x55, %xmm2, %xmm1
	pshufd	$0xaa, %xmm2, %xmm4
	pshufd	$0xff, %xmm2, %xmm6
#else
	movaps	%xmm2, %xmm0
	shufps	$0x00, %xmm0, %xmm0
	movaps	%xmm2, %xmm1
	shufps	$0x55, %xmm1, %xmm1
	movaps	%xmm2, %xmm4
	shufps	$0xaa, %xmm4, %xmm4
	movaps	%xmm2, %xmm6
	shufps	$0xff, %xmm6, %xmm6
#endif

	movaps	%xmm0,   0 * SIZE(BB)
	movaps	%xmm1,   4 * SIZE(BB)
	movaps	%xmm4,   8 * SIZE(BB)
	movaps	%xmm6,  12 * SIZE(BB)

#ifdef HAVE_SSE2
	pshufd	$0x00, %xmm3, %xmm0
	pshufd	$0x55, %xmm3, %xmm1
	pshufd	$0xaa, %xmm3, %xmm4
	pshufd	$0xff, %xmm3, %xmm6
#else
	movaps	%xmm3, %xmm0
	shufps	$0x00, %xmm0, %xmm0
	movaps	%xmm3, %xmm1
	shufps	$0x55, %xmm1, %xmm1
	movaps	%xmm3, %xmm4
	shufps	$0xaa, %xmm4, %xmm4
	movaps	%xmm3, %xmm6
	shufps	$0xff, %xmm6, %xmm6
#endif

	movaps	%xmm0,  16 * SIZE(BB)
	movaps	%xmm1,  20 * SIZE(BB)
	movaps	%xmm4,  24 * SIZE(BB)
	movaps	%xmm6,  28 * SIZE(BB)
#else
	movaps	%xmm0,   0 * SIZE(AA)
	movaps	%xmm2,   4 * SIZE(AA)
#endif

#ifdef LN
	subl	$4 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movaps	 %xmm2, %xmm0
	shufps	 $0x88, %xmm3, %xmm2
	shufps	 $0xdd, %xmm3, %xmm0

	movlps	%xmm2, 0 * SIZE(CO1)
	movhps	%xmm2, 2 * SIZE(CO1)
	movlps	%xmm0, 0 * SIZE(CO1, LDC)
	movhps	%xmm0, 2 * SIZE(CO1, LDC)
#else
	movlps	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 2 * SIZE(CO1)
	movlps	%xmm2, 0 * SIZE(CO1, LDC)
	movhps	%xmm2, 2 * SIZE(CO1, LDC)
#endif

#ifndef LN
	addl	$4 * SIZE, CO1
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
	ALIGN_2

.L70:
	movl	M,  %ebx
	sarl	$3, %ebx
	jle	.L99
	ALIGN_4

.L10:
#ifdef LN
       movl	K, %eax
       sall	$3 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	sall	$3 + BASE_SHIFT, %eax
	addl	%eax, AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + BASE_SHIFT, %eax
	leal	(BB, %eax, 4), BB
#endif

	movaps	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 8 * SIZE(BB), %xmm3
	xorps	%xmm6, %xmm6
	movaps	 8 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7

	PREFETCHW      7 * SIZE(CO1)
	PREFETCHW      7 * SIZE(CO1, LDC)

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L12
	ALIGN_2

.L11:
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	 0 * SIZE(BB), %xmm2

	addps	%xmm0, %xmm5
	movaps	 4 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0

	addps	%xmm2, %xmm6
	movaps	16 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movaps	16 * SIZE(AA), %xmm0

	mulps	%xmm1, %xmm3
	mulps	12 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm4
	movaps	 8 * SIZE(BB), %xmm3

	addps	%xmm1, %xmm5
	movaps	12 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	mulps	12 * SIZE(BB), %xmm1

	addps	%xmm3, %xmm6
	movaps	24 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movaps	24 * SIZE(AA), %xmm1

	mulps	%xmm0, %xmm2
	mulps	20 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	16 * SIZE(BB), %xmm2

	addps	%xmm0, %xmm5
	movaps	20 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm2
	mulps	20 * SIZE(BB), %xmm0

	addps	%xmm2, %xmm6
	movaps	32 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movaps	32 * SIZE(AA), %xmm0

	mulps	%xmm1, %xmm3
	mulps	28 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm4
	movaps	24 * SIZE(BB), %xmm3

	addps	%xmm1, %xmm5
	movaps	28 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	mulps	28 * SIZE(BB), %xmm1

	addps	%xmm3, %xmm6
	movaps	40 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movaps	40 * SIZE(AA), %xmm1

	mulps	%xmm0, %xmm2
	mulps	36 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	32 * SIZE(BB), %xmm2

	addps	%xmm0, %xmm5
	movaps	36 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm2
	mulps	36 * SIZE(BB), %xmm0

	addps	%xmm2, %xmm6
	movaps	48 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movaps	48 * SIZE(AA), %xmm0

	mulps	%xmm1, %xmm3
	mulps	44 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm4
	movaps	40 * SIZE(BB), %xmm3

	addps	%xmm1, %xmm5
	movaps	44 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	mulps	44 * SIZE(BB), %xmm1

	addps	%xmm3, %xmm6
	movaps	56 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movaps	56 * SIZE(AA), %xmm1

	mulps	%xmm0, %xmm2
	mulps	52 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	48 * SIZE(BB), %xmm2

	addps	%xmm0, %xmm5
	movaps	52 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm2
	mulps	52 * SIZE(BB), %xmm0

	addps	%xmm2, %xmm6
	movaps	64 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movaps	64 * SIZE(AA), %xmm0

	mulps	%xmm1, %xmm3
	mulps	60 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm4
	movaps	56 * SIZE(BB), %xmm3

	addps	%xmm1, %xmm5
	movaps	60 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	mulps	60 * SIZE(BB), %xmm1

	addps	%xmm3, %xmm6
	movaps	72 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movaps	72 * SIZE(AA), %xmm1

	addl   $64 * SIZE, BB
	addl   $64 * SIZE, AA
	decl   %eax
	jne    .L11
	ALIGN_2

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
	movaps	 4 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 0 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm1
	movaps	 4 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm5
	movaps	 4 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm6
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm1
	movaps	 8 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm7

	addl	$8 * SIZE, AA
	addl	$8 * SIZE, BB
	subl	$1, %eax
	jg	.L13
	ALIGN_4

.L14:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$8, %eax
#else
	subl	$2, %eax
#endif

	movl	AORIG, AA
	movl	BORIG, B
	leal	BUFFER, BB

	sall	$BASE_SHIFT, %eax
	leal	(AA, %eax, 8), AA
	leal	(B,  %eax, 2), B
	leal	(BB, %eax, 8), BB
#endif

#if defined(LN) || defined(LT)
	movaps	 %xmm4, %xmm0
	unpcklps %xmm5, %xmm4
	unpckhps %xmm5, %xmm0

	movaps	 %xmm6, %xmm1
	unpcklps %xmm7, %xmm6
	unpckhps %xmm7, %xmm1

	movsd	 0 * SIZE(B), %xmm2
	movhps	 2 * SIZE(B), %xmm2
	movsd	 4 * SIZE(B), %xmm3
	movhps	 6 * SIZE(B), %xmm3
	movsd	 8 * SIZE(B), %xmm5
	movhps	10 * SIZE(B), %xmm5
	movsd	12 * SIZE(B), %xmm7
	movhps	14 * SIZE(B), %xmm7

	subps	%xmm4,  %xmm2
	subps	%xmm0,  %xmm3
	subps	%xmm6,  %xmm5
	subps	%xmm1,  %xmm7
#else
	movaps	 0 * SIZE(AA), %xmm0
	movaps	 4 * SIZE(AA), %xmm1
	movaps	 8 * SIZE(AA), %xmm2
	movaps	12 * SIZE(AA), %xmm3

	subps	%xmm4, %xmm0
	subps	%xmm6, %xmm1
	subps	%xmm5, %xmm2
	subps	%xmm7, %xmm3
#endif

#if defined(LN) || defined(LT)
	movaps	TRMASK, %xmm6
#endif

#ifdef LN
	movss	63 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm7

	movaps	%xmm7, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movss	62 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm7

	movsd	60 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm5

	movsd	58 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movsd	56 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movss	54 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm7

	movaps	%xmm7, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movsd	52 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm5

	movsd	50 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movsd	48 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2


	movss	45 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm5

	movaps	%xmm5, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movss	44 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm5

	movsd	42 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movsd	40 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movss	36 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm5

	movaps	%xmm5, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movsd	34 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movsd	32 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movss	27 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm3

	movaps	%xmm3, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movss	26 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movsd	24 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movss	18 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm3

	movaps	%xmm3, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movsd	16 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movss	 9 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm2

	movaps	%xmm2, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movss	 8 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movss	 0 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm2
#endif

#ifdef LT
	movss	 0 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm2

	movaps	%xmm2, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movss	 1 * SIZE(AA), %xmm0
	shufps	$0x05, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movsd	 2 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movsd	 4 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm5

	movsd	 6 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm7

	movss	 9 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm2

	movaps	%xmm2, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movsd	10 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movsd	12 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm5

	movsd	14 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm7

	movss	18 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm3

	movaps	%xmm3, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movss	19 * SIZE(AA), %xmm0
	shufps	$0x05, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movsd	20 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm5

	movsd	22 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm7

	movss	27 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm3

	movaps	%xmm3, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movsd	28 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm5

	movsd	30 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm7

	movss	36 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm5

	movaps	%xmm5, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movss	37 * SIZE(AA), %xmm0
	shufps	$0x05, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm5

	movsd	38 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm7

	movss	45 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm5

	movaps	%xmm5, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movsd	46 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm7

	movss	54 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm7

	movaps	%xmm7, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movss	55 * SIZE(AA), %xmm0
	shufps	$0x05, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm7

	movss	63 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm7
#endif

#ifdef RN
	movss	 0 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6

	mulps	%xmm6, %xmm0
	mulps	%xmm6, %xmm1

	movss	 1 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6
	movaps	%xmm6, %xmm5

	mulps	%xmm0, %xmm5
	mulps	%xmm1, %xmm6

	subps	%xmm5, %xmm2
	subps	%xmm6, %xmm3

	movss	 3 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6

	mulps	%xmm6, %xmm2
	mulps	%xmm6, %xmm3
#endif

#ifdef RT
	movss	 3 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6

	mulps	%xmm6, %xmm2
	mulps	%xmm6, %xmm3

	movss	 2 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6
	movaps	%xmm6, %xmm5

	mulps	%xmm2, %xmm5
	mulps	%xmm3, %xmm6

	subps	%xmm5, %xmm0
	subps	%xmm6, %xmm1

	movss	 0 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6

	mulps	%xmm6, %xmm0
	mulps	%xmm6, %xmm1
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm2,   0 * SIZE(B)
	movhps	%xmm2,   2 * SIZE(B)
	movlps	%xmm3,   4 * SIZE(B)
	movhps	%xmm3,   6 * SIZE(B)
	movlps	%xmm5,   8 * SIZE(B)
	movhps	%xmm5,  10 * SIZE(B)
	movlps	%xmm7,  12 * SIZE(B)
	movhps	%xmm7,  14 * SIZE(B)

#ifdef HAVE_SSE2
	pshufd	$0x00, %xmm2, %xmm0
	pshufd	$0x55, %xmm2, %xmm1
	pshufd	$0xaa, %xmm2, %xmm4
	pshufd	$0xff, %xmm2, %xmm6
#else
	movaps	%xmm2, %xmm0
	shufps	$0x00, %xmm0, %xmm0
	movaps	%xmm2, %xmm1
	shufps	$0x55, %xmm1, %xmm1
	movaps	%xmm2, %xmm4
	shufps	$0xaa, %xmm4, %xmm4
	movaps	%xmm2, %xmm6
	shufps	$0xff, %xmm6, %xmm6
#endif
	movaps	%xmm0,   0 * SIZE(BB)
	movaps	%xmm1,   4 * SIZE(BB)
	movaps	%xmm4,   8 * SIZE(BB)
	movaps	%xmm6,  12 * SIZE(BB)

#ifdef HAVE_SSE2
	pshufd	$0x00, %xmm3, %xmm0
	pshufd	$0x55, %xmm3, %xmm1
	pshufd	$0xaa, %xmm3, %xmm4
	pshufd	$0xff, %xmm3, %xmm6
#else
	movaps	%xmm3, %xmm0
	shufps	$0x00, %xmm0, %xmm0
	movaps	%xmm3, %xmm1
	shufps	$0x55, %xmm1, %xmm1
	movaps	%xmm3, %xmm4
	shufps	$0xaa, %xmm4, %xmm4
	movaps	%xmm3, %xmm6
	shufps	$0xff, %xmm6, %xmm6
#endif
	movaps	%xmm0,  16 * SIZE(BB)
	movaps	%xmm1,  20 * SIZE(BB)
	movaps	%xmm4,  24 * SIZE(BB)
	movaps	%xmm6,  28 * SIZE(BB)

#ifdef HAVE_SSE2
	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1
	pshufd	$0xaa, %xmm5, %xmm4
	pshufd	$0xff, %xmm5, %xmm6
#else
	movaps	%xmm5, %xmm0
	shufps	$0x00, %xmm0, %xmm0
	movaps	%xmm5, %xmm1
	shufps	$0x55, %xmm1, %xmm1
	movaps	%xmm5, %xmm4
	shufps	$0xaa, %xmm4, %xmm4
	movaps	%xmm5, %xmm6
	shufps	$0xff, %xmm6, %xmm6
#endif
	movaps	%xmm0,  32 * SIZE(BB)
	movaps	%xmm1,  36 * SIZE(BB)
	movaps	%xmm4,  40 * SIZE(BB)
	movaps	%xmm6,  44 * SIZE(BB)

#ifdef HAVE_SSE2
	pshufd	$0x00, %xmm7, %xmm0
	pshufd	$0x55, %xmm7, %xmm1
	pshufd	$0xaa, %xmm7, %xmm4
	pshufd	$0xff, %xmm7, %xmm6
#else
	movaps	%xmm7, %xmm0
	shufps	$0x00, %xmm0, %xmm0
	movaps	%xmm7, %xmm1
	shufps	$0x55, %xmm1, %xmm1
	movaps	%xmm7, %xmm4
	shufps	$0xaa, %xmm4, %xmm4
	movaps	%xmm7, %xmm6
	shufps	$0xff, %xmm6, %xmm6
#endif
	movaps	%xmm0,  48 * SIZE(BB)
	movaps	%xmm1,  52 * SIZE(BB)
	movaps	%xmm4,  56 * SIZE(BB)
	movaps	%xmm6,  60 * SIZE(BB)
#else
	movaps	%xmm0,   0 * SIZE(AA)
	movaps	%xmm1,   4 * SIZE(AA)
	movaps	%xmm2,   8 * SIZE(AA)
	movaps	%xmm3,  12 * SIZE(AA)
#endif

#ifdef LN
	subl	$8 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movaps	 %xmm2, %xmm0
	shufps	 $0x88, %xmm3, %xmm2
	shufps	 $0xdd, %xmm3, %xmm0

	movaps	 %xmm5, %xmm4
	shufps	 $0x88, %xmm7, %xmm5
	shufps	 $0xdd, %xmm7, %xmm4

	movlps	%xmm2, 0 * SIZE(CO1)
	movhps	%xmm2, 2 * SIZE(CO1)
	movlps	%xmm5, 4 * SIZE(CO1)
	movhps	%xmm5, 6 * SIZE(CO1)
	movlps	%xmm0, 0 * SIZE(CO1, LDC)
	movhps	%xmm0, 2 * SIZE(CO1, LDC)
	movlps	%xmm4, 4 * SIZE(CO1, LDC)
	movhps	%xmm4, 6 * SIZE(CO1, LDC)
#else
	movlps	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 2 * SIZE(CO1)
	movlps	%xmm1, 4 * SIZE(CO1)
	movhps	%xmm1, 6 * SIZE(CO1)
	movlps	%xmm2, 0 * SIZE(CO1, LDC)
	movhps	%xmm2, 2 * SIZE(CO1, LDC)
	movlps	%xmm3, 4 * SIZE(CO1, LDC)
	movhps	%xmm3, 6 * SIZE(CO1, LDC)
#endif

#ifndef LN
	addl	$8 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 8), AA
#ifdef LT
	addl	$16 * SIZE, B
#endif
#endif

#ifdef LN
	subl	$8, KK
	movl	BORIG, B
#endif

#ifdef LT
	addl	$8, KK
#endif

#ifdef RT
	movl	K, %eax
	movl	BORIG, B
	sall	$3 + BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx			# i --
	jg	.L10
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
	testl	$1, N
	jle	.L999

#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

	leal	BUFFER, BB

#ifdef RT
       movl	K, %eax
       sall	$BASE_SHIFT, %eax
       subl	%eax, B
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	B, BORIG
        sall	$BASE_SHIFT, %eax
	leal	(B,  %eax, 1), B
	leal	(BB, %eax, 4), BB
#endif

#ifdef LT
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	jle	.L103
	ALIGN_4

.L102:
	movsd	 0 * SIZE(B), %xmm3
	movhps	 2 * SIZE(B), %xmm3
	movsd	 4 * SIZE(B), %xmm7
	movhps	 6 * SIZE(B), %xmm7

#ifdef HAVE_SSE2
	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

	pshufd	 $0x00, %xmm7, %xmm4
	pshufd	 $0x55, %xmm7, %xmm5
	pshufd	 $0xaa, %xmm7, %xmm6
	pshufd	 $0xff, %xmm7, %xmm7
#else
	movaps	%xmm3, %xmm0
	shufps	 $0x00, %xmm0, %xmm0
	movaps	%xmm3, %xmm1
	shufps	 $0x55, %xmm1, %xmm1
	movaps	%xmm3, %xmm2
	shufps	 $0xaa, %xmm2, %xmm2
	shufps	 $0xff, %xmm3, %xmm3

	movaps	%xmm7, %xmm4
	shufps	 $0x00, %xmm4, %xmm4
	movaps	%xmm7, %xmm5
	shufps	 $0x55, %xmm5, %xmm5
	movaps	%xmm7, %xmm6
	shufps	 $0xaa, %xmm6, %xmm6
	shufps	 $0xff, %xmm7, %xmm7
#endif

	movaps	%xmm0,  0 * SIZE(BB)
	movaps	%xmm1,  4 * SIZE(BB)
	movaps	%xmm2,  8 * SIZE(BB)
	movaps	%xmm3, 12 * SIZE(BB)
	movaps	%xmm4, 16 * SIZE(BB)
	movaps	%xmm5, 20 * SIZE(BB)
	movaps	%xmm6, 24 * SIZE(BB)
	movaps	%xmm7, 28 * SIZE(BB)

	addl	$ 8 * SIZE, B
	addl	$32 * SIZE, BB

	decl	%eax
	BRANCH
	jne	.L102
	ALIGN_2

.L103:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	andl	$7, %eax
	BRANCH
	jle	.L105
	ALIGN_2

.L104:
	movss	 0 * SIZE(B), %xmm0

	shufps	 $0x00, %xmm0, %xmm0

	movaps	%xmm0,  0 * SIZE(BB)

	addl	$1 * SIZE, B
	addl	$4 * SIZE, BB

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
	movl	C, CO1
#ifndef RT
	addl	LDC, C
#endif

	testl	$1, M
	jle	.L130

#ifdef LN
       movl	K, %eax
       sall	$BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	leal	(AA, %eax, SIZE), AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$BASE_SHIFT, %eax
	leal	(BB, %eax, 4), BB
#endif

	movss	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
	movss	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movss	16 * SIZE(BB), %xmm3
	xorps	%xmm6, %xmm6
	movss	 4 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L172
	ALIGN_2

.L171:
	mulss	%xmm0, %xmm2
	movss	 1 * SIZE(AA), %xmm0
	addss	%xmm2, %xmm4
	mulss	 4 * SIZE(BB), %xmm0
	movss	32 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm5
	movss	 2 * SIZE(AA), %xmm0
	mulss	 8 * SIZE(BB), %xmm0
	addss	%xmm0, %xmm6
	movss	 3 * SIZE(AA), %xmm0
	mulss	12 * SIZE(BB), %xmm0
	addss	%xmm0, %xmm7
	movss	 8 * SIZE(AA), %xmm0
	mulss	%xmm1, %xmm3
	movss	 5 * SIZE(AA), %xmm1
	addss	%xmm3, %xmm4
	mulss	20 * SIZE(BB), %xmm1
	movss	48 * SIZE(BB), %xmm3
	addss	%xmm1, %xmm5
	movss	 6 * SIZE(AA), %xmm1
	mulss	24 * SIZE(BB), %xmm1
	addss	%xmm1, %xmm6
	movss	 7 * SIZE(AA), %xmm1
	mulss	28 * SIZE(BB), %xmm1
	addss	%xmm1, %xmm7
	movss	12 * SIZE(AA), %xmm1

	addl   $ 8 * SIZE, AA
	addl   $32 * SIZE, BB
	decl   %eax
	jne    .L171
	ALIGN_2

.L172:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L174

.L173:
	movss	 0 * SIZE(AA), %xmm0
	movss	 0 * SIZE(BB), %xmm2
	mulss	%xmm0, %xmm2
	addss	%xmm2, %xmm4

	addl	$1 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L173
	ALIGN_4

.L174:
	addss	%xmm5, %xmm4
	addss	%xmm7, %xmm6
	addss	%xmm6, %xmm4

#if defined(LN) || defined(RT)
	movl	KK, %eax
	subl	$1, %eax

	movl	AORIG, AA
	movl	BORIG, B
	leal	BUFFER, BB

	sall	$ BASE_SHIFT, %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 1), B
	leal	(BB, %eax, 4), BB
#endif

#if defined(LN) || defined(LT)
	movss	 0 * SIZE(B), %xmm1
	subss	%xmm4,  %xmm1
#else
	movss	 0 * SIZE(AA), %xmm0
	subss	%xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	mulss	 0 * SIZE(AA), %xmm1
#endif

#if defined(RN) || defined(RT)
	mulss	 0 * SIZE(B), %xmm0
#endif

#if defined(LN) || defined(LT)
	movss	%xmm1,   0 * SIZE(B)

	shufps	$0x00, %xmm1, %xmm1
	movaps	%xmm1,   0 * SIZE(BB)
#else
	movss	%xmm0,   0 * SIZE(AA)
#endif

#ifdef LN
	subl	$1 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movss	%xmm1, 0 * SIZE(CO1)
#else
	movss	%xmm0, 0 * SIZE(CO1)
#endif

#ifndef LN
	addl	$1 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(AA, %eax, SIZE), AA
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
	sall	$BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif
	ALIGN_2

.L130:
	testl	$2, M
	jle	.L150

#ifdef LN
       movl	K, %eax
       sall	$1 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	sall	$1 + BASE_SHIFT, %eax
	addl	%eax, AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$BASE_SHIFT, %eax
	leal	(BB, %eax, 4), BB
#endif

	movaps	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
#ifdef	movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 16 * SIZE(BB), %xmm3
	xorps	%xmm6, %xmm6
#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	 8 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L152
	ALIGN_2

.L151:
	mulps	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 6 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm6
	movaps	12 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	16 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm7
	movaps	32 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm3
	movsd	10 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm4
	movaps	20 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	12 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movaps	24 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	14 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm6
	movaps	28 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	24 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm7
	movaps	48 * SIZE(BB), %xmm3

	addl   $16 * SIZE, AA
	addl   $32 * SIZE, BB
	decl   %eax
	jne    .L151
	ALIGN_2

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
	mulps	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L153
	ALIGN_4

.L154:
	addps	%xmm5, %xmm4
	addps	%xmm7, %xmm6
	addps	%xmm6, %xmm4

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

	sall	$BASE_SHIFT, %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 1), B
	leal	(BB, %eax, 4), BB
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm4, %xmm5
	shufps	$1, %xmm5, %xmm5

	movss	 0 * SIZE(B), %xmm0
	movss	 1 * SIZE(B), %xmm1

	subss	%xmm4,  %xmm0
	subss	%xmm5,  %xmm1
#else
#ifdef	movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	 0 * SIZE(AA), %xmm0
	subps	%xmm4, %xmm0
#endif

#ifdef LN
	movaps	  0 * SIZE(AA), %xmm4

	movaps	 %xmm4, %xmm6
	shufps	 $0xff, %xmm6, %xmm6
	mulss	 %xmm6, %xmm1

	movaps	 %xmm4, %xmm6
	shufps	 $0xaa, %xmm6, %xmm6
	mulss	 %xmm1, %xmm6
	subss	 %xmm6, %xmm0
	mulss	 %xmm4, %xmm0
#endif

#ifdef LT
	movaps	 0 * SIZE(AA), %xmm4
	mulss	 %xmm4, %xmm0
	movaps	 %xmm4, %xmm6
	shufps	 $0x55, %xmm6, %xmm6
	mulss	 %xmm0, %xmm6
	subss	 %xmm6, %xmm1
	movaps	 %xmm4, %xmm6
	shufps	 $0xff, %xmm6, %xmm6
	mulss	 %xmm6, %xmm1
#endif

#ifdef RN
	movss	 0 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6
	mulps	%xmm6, %xmm0
#endif

#ifdef RT
	movss	 0 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6
	mulps	%xmm6, %xmm0
#endif

#if defined(LN) || defined(LT)
	movss	%xmm0,   0 * SIZE(B)
	movss	%xmm1,   1 * SIZE(B)

	shufps	$0x00, %xmm0, %xmm0
	shufps	$0x00, %xmm1, %xmm1
	movaps	%xmm0,   0 * SIZE(BB)
	movaps	%xmm1,   4 * SIZE(BB)
#else
	movlps	%xmm0,   0 * SIZE(AA)
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movss	%xmm0, 0 * SIZE(CO1)
	movss	%xmm1, 1 * SIZE(CO1)
#else
	movlps	%xmm0, 0 * SIZE(CO1)
#endif

#ifndef LN
	addl	$2 * SIZE, CO1
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
	testl	$4, M
	jle	.L170

#ifdef LN
       movl	K, %eax
       sall	$2 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	sall	$2 + BASE_SHIFT, %eax
	addl	%eax, AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$BASE_SHIFT, %eax
	leal	(BB, %eax, 4), BB
#endif

	movaps	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
	movsd	 0 * SIZE(AA), %xmm0
	movhps	 2 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	16 * SIZE(BB), %xmm3
	xorps	%xmm6, %xmm6
	movsd	16 * SIZE(AA), %xmm1
	movhps	18 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L132
	ALIGN_2

.L131:
	mulps	%xmm0, %xmm2
	movaps	 4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	mulps	 4 * SIZE(BB), %xmm0
	movaps	32 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	 8 * SIZE(AA), %xmm0
	mulps	 8 * SIZE(BB), %xmm0
	addps	%xmm0, %xmm6
	movaps	12 * SIZE(AA), %xmm0
	mulps	12 * SIZE(BB), %xmm0
	addps	%xmm0, %xmm7
	movaps	32 * SIZE(AA), %xmm0
	mulps	%xmm1, %xmm3
	movaps	20 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm4
	mulps	20 * SIZE(BB), %xmm1
	movaps	48 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm5
	movaps	24 * SIZE(AA), %xmm1
	mulps	24 * SIZE(BB), %xmm1
	addps	%xmm1, %xmm6
	movaps	28 * SIZE(AA), %xmm1
	mulps	28 * SIZE(BB), %xmm1
	addps	%xmm1, %xmm7
	movaps	48 * SIZE(AA), %xmm1

	addl   $32 * SIZE, AA
	addl   $32 * SIZE, BB
	decl   %eax
	jne    .L131
	ALIGN_2

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
	movaps	 0 * SIZE(BB), %xmm2
	movaps	 0 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4

	addl	$4 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L133
	ALIGN_4

.L134:
	addps	%xmm5, %xmm4
	addps	%xmm7, %xmm6
	addps	%xmm6, %xmm4

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

	sall	$BASE_SHIFT, %eax
	leal	(AA, %eax, 4), AA
	leal	(B,  %eax, 1), B
	leal	(BB, %eax, 4), BB
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(B), %xmm2
	movhps	 2 * SIZE(B), %xmm2

	subps	%xmm4,  %xmm2

	xorps	%xmm5, %xmm5

	movaps	 %xmm2, %xmm3
	unpcklps %xmm5, %xmm2
	unpckhps %xmm5, %xmm3
#else
	movaps	 0 * SIZE(AA), %xmm0
	subps	%xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movaps	TRMASK, %xmm6
#endif

#ifdef LN
	movss	15 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm3

	movaps	%xmm3, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movss	14 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movsd	12 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movss	10 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm3

	movaps	%xmm3, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movsd	 8 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movss	 5 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm2

	movaps	%xmm2, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movss	 4 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movss	 0 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm2
#endif

#ifdef LT
	movss	 0 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm2

	movaps	%xmm2, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movss	 1 * SIZE(AA), %xmm0
	shufps	$0x05, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movsd	 2 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movss	 5 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm2

	movaps	%xmm2, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movsd	 6 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movss	10 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm3

	movaps	%xmm3, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movss	11 * SIZE(AA), %xmm0
	shufps	$0x05, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movss	15 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm3
#endif

#ifdef RN
	movss	 0 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6
	mulps	%xmm6, %xmm0
#endif

#ifdef RT
	movss	 0 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6
	mulps	%xmm6, %xmm0
#endif

#if defined(LN) || defined(LT)
	shufps	$0x88, %xmm3, %xmm2

	movlps	%xmm2,   0 * SIZE(B)
	movhps	%xmm2,   2 * SIZE(B)

#ifdef HAVE_SSE2
	pshufd	$0x00, %xmm2, %xmm0
	pshufd	$0x55, %xmm2, %xmm1
	pshufd	$0xaa, %xmm2, %xmm4
	pshufd	$0xff, %xmm2, %xmm6
#else
	movaps	%xmm2, %xmm0
	shufps	$0x00, %xmm0, %xmm0
	movaps	%xmm2, %xmm1
	shufps	$0x55, %xmm1, %xmm1
	movaps	%xmm2, %xmm4
	shufps	$0xaa, %xmm4, %xmm4
	movaps	%xmm2, %xmm6
	shufps	$0xff, %xmm6, %xmm6
#endif

	movaps	%xmm0,   0 * SIZE(BB)
	movaps	%xmm1,   4 * SIZE(BB)
	movaps	%xmm4,   8 * SIZE(BB)
	movaps	%xmm6,  12 * SIZE(BB)
#else
	movaps	%xmm0,   0 * SIZE(AA)
#endif

#ifdef LN
	subl	$4 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm2, 0 * SIZE(CO1)
	movhps	%xmm2, 2 * SIZE(CO1)
#else
	movlps	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 2 * SIZE(CO1)
#endif

#ifndef LN
	addl	$4 * SIZE, CO1
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
	ALIGN_2

.L170:
	movl	M,  %ebx
	sarl	$3, %ebx	# i = (m >> 2)
	jle	.L179
	ALIGN_4

.L110:
#ifdef LN
       movl	K, %eax
       sall	$3 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	sall	$3 + BASE_SHIFT, %eax
	addl	%eax, AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$BASE_SHIFT, %eax
	leal	(BB, %eax, 4), BB
#endif

	movaps	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 16 * SIZE(BB), %xmm3
	xorps	%xmm6, %xmm6
	movaps	 16 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7

	PREFETCHW      7 * SIZE(CO1)

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L112
	ALIGN_2

.L111:
	mulps	%xmm2, %xmm0
	mulps	 4 * SIZE(AA), %xmm2
	addps	%xmm0, %xmm4
	movaps	 8 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm6
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm2, %xmm0
	mulps	12 * SIZE(AA), %xmm2
	addps	%xmm0, %xmm5
	movaps	32 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm7
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm2, %xmm1
	mulps	20 * SIZE(AA), %xmm2
	addps	%xmm1, %xmm4
	movaps	24 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm6
	movaps	12 * SIZE(BB), %xmm2
	mulps	%xmm2, %xmm1
	mulps	28 * SIZE(AA), %xmm2
	addps	%xmm1, %xmm5
	movaps	48 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm7
	movaps	32 * SIZE(BB), %xmm2
	mulps	%xmm3, %xmm0
	mulps	36 * SIZE(AA), %xmm3
	addps	%xmm0, %xmm4
	movaps	40 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm6
	movaps	20 * SIZE(BB), %xmm3
	mulps	%xmm3, %xmm0
	mulps	44 * SIZE(AA), %xmm3
	addps	%xmm0, %xmm5
	movaps	64 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm7
	movaps	24 * SIZE(BB), %xmm3
	mulps	%xmm3, %xmm1
	mulps	52 * SIZE(AA), %xmm3
	addps	%xmm1, %xmm4
	movaps	56 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm6
	movaps	28 * SIZE(BB), %xmm3
	mulps	%xmm3, %xmm1
	mulps	60 * SIZE(AA), %xmm3
	addps	%xmm1, %xmm5
	movaps	80 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm7
	movaps	48 * SIZE(BB), %xmm3

	addl   $64 * SIZE, AA
	addl   $32 * SIZE, BB
	decl   %eax
	jne    .L111
	ALIGN_2

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
	movaps	 0 * SIZE(BB), %xmm2
	movaps	 0 * SIZE(AA), %xmm0
	mulps	%xmm2, %xmm0
	addps	%xmm0, %xmm4
	mulps	 4 * SIZE(AA), %xmm2
	addps	%xmm2, %xmm6

	addl	$8 * SIZE, AA
	addl	$4 * SIZE, BB
	subl	$1, %eax
	jg	.L113
	ALIGN_4

.L114:
	addps	%xmm5, %xmm4
	addps	%xmm7, %xmm6

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$8, %eax
#else
	subl	$1, %eax
#endif

	movl	AORIG, AA
	movl	BORIG, B
	leal	BUFFER, BB

	sall	$BASE_SHIFT, %eax
	leal	(AA, %eax, 8), AA
	leal	(B,  %eax, 1), B
	leal	(BB, %eax, 4), BB
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(B), %xmm2
	movhps	 2 * SIZE(B), %xmm2
	movsd	 4 * SIZE(B), %xmm5
	movhps	 6 * SIZE(B), %xmm5

	subps	%xmm4,  %xmm2
	subps	%xmm6,  %xmm5

	xorps	%xmm0,  %xmm0

	movaps	 %xmm2, %xmm3
	unpcklps %xmm0, %xmm2
	unpckhps %xmm0, %xmm3

	movaps	 %xmm5, %xmm7
	unpcklps %xmm0, %xmm5
	unpckhps %xmm0, %xmm7
#else
	movaps	 0 * SIZE(AA), %xmm0
	movaps	 4 * SIZE(AA), %xmm1

	subps	%xmm4, %xmm0
	subps	%xmm6, %xmm1
#endif

#if defined(LN) || defined(LT)
	movaps	TRMASK, %xmm6
#endif

#ifdef LN
	movss	63 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm7

	movaps	%xmm7, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movss	62 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm7

	movsd	60 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm5

	movsd	58 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movsd	56 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movss	54 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm7

	movaps	%xmm7, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movsd	52 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm5

	movsd	50 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movsd	48 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2


	movss	45 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm5

	movaps	%xmm5, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movss	44 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm5

	movsd	42 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movsd	40 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movss	36 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm5

	movaps	%xmm5, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movsd	34 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movsd	32 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movss	27 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm3

	movaps	%xmm3, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movss	26 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movsd	24 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movss	18 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm3

	movaps	%xmm3, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movsd	16 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movss	 9 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm2

	movaps	%xmm2, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movss	 8 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movss	 0 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm2
#endif

#ifdef LT
	movss	 0 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm2

	movaps	%xmm2, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movss	 1 * SIZE(AA), %xmm0
	shufps	$0x05, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm2

	movsd	 2 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movsd	 4 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm5

	movsd	 6 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm7

	movss	 9 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm2

	movaps	%xmm2, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movsd	10 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movsd	12 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm5

	movsd	14 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm7

	movss	18 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm3

	movaps	%xmm3, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movss	19 * SIZE(AA), %xmm0
	shufps	$0x05, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm3

	movsd	20 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm5

	movsd	22 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm7

	movss	27 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm3

	movaps	%xmm3, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movsd	28 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm5

	movsd	30 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm7

	movss	36 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm5

	movaps	%xmm5, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movss	37 * SIZE(AA), %xmm0
	shufps	$0x05, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm5

	movsd	38 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm7

	movss	45 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm5

	movaps	%xmm5, %xmm1
	shufps	$0xee, %xmm1, %xmm1

	movsd	46 * SIZE(AA), %xmm0
	shufps	$0x50, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm7

	movss	54 * SIZE(AA), %xmm0
	shufps	$0x00, %xmm6,  %xmm0
	mulps	%xmm0, %xmm7

	movaps	%xmm7, %xmm1
	shufps	$0x44, %xmm1, %xmm1

	movss	55 * SIZE(AA), %xmm0
	shufps	$0x05, %xmm0, %xmm0
	mulps	%xmm1, %xmm0
	subps	%xmm0, %xmm7

	movss	63 * SIZE(AA), %xmm0
	movaps	 %xmm6, %xmm1
	shufps	$0x00, %xmm0,  %xmm1
	mulps	%xmm1, %xmm7
#endif

#if defined(RN) || defined(RT)
	movss	 0 * SIZE(B), %xmm6
	shufps	$0x00, %xmm6, %xmm6

	mulps	%xmm6, %xmm0
	mulps	%xmm6, %xmm1
#endif

#if defined(LN) || defined(LT)
	shufps	$0x88, %xmm3, %xmm2
	shufps	$0x88, %xmm7, %xmm5

	movlps	%xmm2,   0 * SIZE(B)
	movhps	%xmm2,   2 * SIZE(B)
	movlps	%xmm5,   4 * SIZE(B)
	movhps	%xmm5,   6 * SIZE(B)

#ifdef HAVE_SSE2
	pshufd	$0x00, %xmm2, %xmm0
	pshufd	$0x55, %xmm2, %xmm1
	pshufd	$0xaa, %xmm2, %xmm4
	pshufd	$0xff, %xmm2, %xmm6
#else
	movaps	%xmm2, %xmm0
	shufps	$0x00, %xmm0, %xmm0
	movaps	%xmm2, %xmm1
	shufps	$0x55, %xmm1, %xmm1
	movaps	%xmm2, %xmm4
	shufps	$0xaa, %xmm4, %xmm4
	movaps	%xmm2, %xmm6
	shufps	$0xff, %xmm6, %xmm6
#endif

	movaps	%xmm0,   0 * SIZE(BB)
	movaps	%xmm1,   4 * SIZE(BB)
	movaps	%xmm4,   8 * SIZE(BB)
	movaps	%xmm6,  12 * SIZE(BB)

#ifdef HAVE_SSE2
	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1
	pshufd	$0xaa, %xmm5, %xmm4
	pshufd	$0xff, %xmm5, %xmm6
#else
	movaps	%xmm5, %xmm0
	shufps	$0x00, %xmm0, %xmm0
	movaps	%xmm5, %xmm1
	shufps	$0x55, %xmm1, %xmm1
	movaps	%xmm5, %xmm4
	shufps	$0xaa, %xmm4, %xmm4
	movaps	%xmm5, %xmm6
	shufps	$0xff, %xmm6, %xmm6
#endif

	movaps	%xmm0,  16 * SIZE(BB)
	movaps	%xmm1,  20 * SIZE(BB)
	movaps	%xmm4,  24 * SIZE(BB)
	movaps	%xmm6,  28 * SIZE(BB)
#else
	movaps	%xmm0,   0 * SIZE(AA)
	movaps	%xmm1,   4 * SIZE(AA)
#endif

#ifdef LN
	subl	$8 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm2, 0 * SIZE(CO1)
	movhps	%xmm2, 2 * SIZE(CO1)
	movlps	%xmm5, 4 * SIZE(CO1)
	movhps	%xmm5, 6 * SIZE(CO1)
#else
	movlps	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 2 * SIZE(CO1)
	movlps	%xmm1, 4 * SIZE(CO1)
	movhps	%xmm1, 6 * SIZE(CO1)
#endif

#ifndef LN
	addl	$8 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 8), AA
#ifdef LT
	addl	$8 * SIZE, B
#endif
#endif

#ifdef LN
	subl	$8, KK
	movl	BORIG, B
#endif

#ifdef LT
	addl	$8, KK
#endif

#ifdef RT
	movl	K, %eax
	movl	BORIG, B
	sall	$3 + BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx			# i --
	jg	.L110
	ALIGN_2

.L179:
#ifdef LN
       movl	K, %eax
       leal 	(B, %eax, SIZE), B
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(B,  %eax, SIZE), B
#endif

#ifdef RN
	addl	$1, KK
#endif

#ifdef RT
	subl	$1, KK
#endif
	ALIGN_4

.L999:
	movl	OLD_STACK, %esp

	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
