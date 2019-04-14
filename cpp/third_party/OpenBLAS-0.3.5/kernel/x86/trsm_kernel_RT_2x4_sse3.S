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

#define STACK	16
#define ARGS	16

#define M	 4 + STACK + ARGS(%esp)
#define N	 8 + STACK + ARGS(%esp)
#define K	12 + STACK + ARGS(%esp)
#define ALPHA	16 + STACK + ARGS(%esp)
#define A	24 + STACK + ARGS(%esp)
#define ARG_B	28 + STACK + ARGS(%esp)
#define C	32 + STACK + ARGS(%esp)
#define ARG_LDC	36 + STACK + ARGS(%esp)
#define OFFSET	40 + STACK + ARGS(%esp)

#define J	 0 + STACK(%esp)
#define KK	 4 + STACK(%esp)
#define KKK	 8 + STACK(%esp)
#define AORIG	12 + STACK(%esp)

#ifdef PENTIUM4
#define PREFETCH     prefetcht1
#define PREFETCHSIZE 84
#endif

#if defined(PENRYN) || defined(DUNNINGTON)
#define PREFETCH     prefetcht1
#define PREFETCHSIZE 84
#endif

#ifdef PENTIUMM
#define PREFETCH     prefetcht1
#define PREFETCHSIZE 84
#endif

#define AA	%edx
#define BB	%ecx
#define LDC	%ebp
#define B	%edi
#define CO1	%esi

	PROLOGUE

	subl	$ARGS, %esp

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	ARG_B,   B
	movl	ARG_LDC, LDC

	movl	OFFSET, %eax
#ifdef RN
	negl	%eax
#endif
	movl	%eax, KK

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

#ifdef RT
	movl	N, %eax
	subl	OFFSET, %eax
	movl	%eax, KK
#endif

	testl	$1, N
	je	.L30

#if defined(LT) || defined(RN)
	movl	A, AA
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

#ifdef RT
	movl	K, %eax
	sall	$BASE_SHIFT, %eax
	subl	%eax, B
#endif

#ifdef RT
	subl	LDC, C
#endif
	movl	C, CO1
#ifndef RT
	addl	LDC, C
#endif

#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

#ifdef LT
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	M,  %ebx
	sarl	$1, %ebx	# i = (m >> 2)
	jle	.L80
	ALIGN_4

.L71:
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

	movl	B, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$BASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movddup	 0 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	movddup	 4 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

#ifdef LN
	prefetchnta	-2 * SIZE(CO1)
#else
	prefetchnta	 2 * SIZE(CO1)
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L75
	ALIGN_4

.L72:
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)
	mulpd	 %xmm2, %xmm0
	movddup	 1 * SIZE(BB), %xmm2
	addpd	 %xmm0, %xmm4
	movapd	16 * SIZE(AA), %xmm0
	mulpd	 2 * SIZE(AA), %xmm2
	addpd	 %xmm2, %xmm5
	movddup	 2 * SIZE(BB), %xmm2
	mulpd	 4 * SIZE(AA), %xmm2
	addpd	 %xmm2, %xmm6
	movddup	 3 * SIZE(BB), %xmm2
	mulpd	 6 * SIZE(AA), %xmm2
	addpd	 %xmm2, %xmm7
	movddup	 8 * SIZE(BB), %xmm2
	mulpd	 %xmm3, %xmm1
	movddup	 5 * SIZE(BB), %xmm3
	addpd	 %xmm1, %xmm4
	movapd	24 * SIZE(AA), %xmm1
	mulpd	10 * SIZE(AA), %xmm3
	addpd	 %xmm3, %xmm5
	movddup	 6 * SIZE(BB), %xmm3
	mulpd	12 * SIZE(AA), %xmm3
	addpd	 %xmm3, %xmm6
	movddup	 7 * SIZE(BB), %xmm3
	mulpd	14 * SIZE(AA), %xmm3
	addpd	 %xmm3, %xmm7
	movddup	12 * SIZE(BB), %xmm3

	addl   $16 * SIZE, AA
	addl   $ 8 * SIZE, BB
	decl   %eax
	jne    .L72
	ALIGN_4

.L75:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L78
	ALIGN_3

.L76:
	mulpd	 %xmm2, %xmm0
	movddup	 1 * SIZE(BB), %xmm2
	addpd	 %xmm0, %xmm4
	movapd	 2 * SIZE(AA), %xmm0

	addl	$2 * SIZE, AA
	addl	$1 * SIZE, BB
	decl	%eax
	jg	.L76
	ALIGN_4

.L78:
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

	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 1), BB
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(BB), %xmm1

	subpd	%xmm4,  %xmm1

	movapd	%xmm1, %xmm0
	unpckhpd %xmm1, %xmm1
#else
	movapd	 0 * SIZE(AA), %xmm0

	subpd	%xmm4, %xmm0
#endif

#ifdef LN
	movsd	 3 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm1

	movsd	 2 * SIZE(AA), %xmm4
	mulsd	 %xmm1, %xmm4
	subsd	 %xmm4, %xmm0

	movsd	 0 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm0

#endif

#ifdef LT
	movsd	 0 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm0

	movsd	 1 * SIZE(AA), %xmm4
	mulsd	 %xmm0, %xmm4
	subsd	 %xmm4, %xmm1

	movsd	 3 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm1
#endif

#ifdef RN
	movddup	 0 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm0
#endif

#ifdef RT
	movddup	 0 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm0,   0 * SIZE(BB)
	movsd	%xmm1,   1 * SIZE(BB)
#else
	movapd	%xmm0,   0 * SIZE(AA)
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm0, 0 * SIZE(CO1)
	movsd	%xmm1, 1 * SIZE(CO1)
#else
	movsd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 1 * SIZE(CO1)
#endif

#ifndef LN
	addl	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	addl	%eax, BB
#endif

#ifdef LN
	subl	$2, KK
#endif

#ifdef LT
	addl	$2, KK
#endif

#ifdef RT
	movl	K, %eax
	sall	$1 + BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx			# i --
	jg	.L71
	ALIGN_4

.L80:
	movl	M,  %ebx
	testl	$1, %ebx	# i = (m >> 2)
	jle	.L89

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

	movl	B, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$BASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movsd	 0 * SIZE(AA), %xmm0
	movhpd	 1 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movsd	 8 * SIZE(AA), %xmm1
	movhpd	 9 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movsd	 0 * SIZE(BB), %xmm2
	movhpd	 1 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	movsd	 8 * SIZE(BB), %xmm3
	movhpd	 9 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$4, %eax
	je	.L85
	ALIGN_4

.L82:
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)
	mulpd	 %xmm0, %xmm2
	movapd	 2 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm4
	mulpd	 2 * SIZE(BB), %xmm0
	movapd	16 * SIZE(BB), %xmm2
	addpd	 %xmm0, %xmm5
	movapd	 4 * SIZE(AA), %xmm0
	mulpd	 4 * SIZE(BB), %xmm0
	addpd	 %xmm0, %xmm6
	movapd	 6 * SIZE(AA), %xmm0
	mulpd	 6 * SIZE(BB), %xmm0
	addpd	 %xmm0, %xmm7
	movapd	16 * SIZE(AA), %xmm0
	mulpd	 %xmm1, %xmm3
	movapd	10 * SIZE(AA), %xmm1
	addpd	 %xmm3, %xmm4
	mulpd	10 * SIZE(BB), %xmm1
	movapd	24 * SIZE(BB), %xmm3
	addpd	 %xmm1, %xmm5
	movapd	12 * SIZE(AA), %xmm1
	mulpd	12 * SIZE(BB), %xmm1
	addpd	 %xmm1, %xmm6
	movapd	14 * SIZE(AA), %xmm1
	mulpd	14 * SIZE(BB), %xmm1
	addpd	 %xmm1, %xmm7
	movapd	24 * SIZE(AA), %xmm1

	addl   $16 * SIZE, AA
	addl   $16 * SIZE, BB
	decl   %eax
	jne    .L82
	ALIGN_4

.L85:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$15, %eax		# if (k & 1)
	BRANCH
	je .L88

.L86:
	mulsd	 %xmm0, %xmm2
	movsd	 1 * SIZE(AA), %xmm0
	addsd	 %xmm2, %xmm4
	movsd	 1 * SIZE(BB), %xmm2

	addl	$1 * SIZE, AA
	addl	$1 * SIZE, BB
	decl	%eax
	jg	.L86
	ALIGN_4

.L88:
	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6
	addpd	%xmm6, %xmm4

	haddpd	%xmm4, %xmm4

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$1, %eax
#endif

	movl	AORIG, AA

	leal	(, %eax, SIZE), %eax
	addl	%eax, AA
	leal	(B,  %eax, 1), BB
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(BB), %xmm0
	subsd	%xmm4,  %xmm0
#else
	movsd	 0 * SIZE(AA), %xmm0
	subsd	%xmm4, %xmm0
#endif

#ifdef LN
	movsd	 0 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm0
#endif

#ifdef LT
	movsd	 0 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm0
#endif

#ifdef RN
	movsd	 0 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm0
#endif

#ifdef RT
	movsd	 0 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm0,   0 * SIZE(BB)
#else
	movsd	%xmm0,   0 * SIZE(AA)
#endif

#ifdef LN
	subl	$1 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm0, 0 * SIZE(CO1)
#else
	movsd	%xmm0, 0 * SIZE(CO1)
#endif

#ifndef LN
	addl	$1 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	addl	%eax, AA
	addl	%eax, BB
#endif

#ifdef LN
	subl	$1, KK
#endif

#ifdef LT
	addl	$1, KK
#endif

#ifdef RT
	movl	K, %eax
	sall	$BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif
	ALIGN_4

.L89:
#ifdef LN
       movl	K, %eax
       leal	(B, %eax, SIZE), B
#endif

#if defined(LT) || defined(RN)
	movl	BB, B
#endif

#ifdef RN
	addl	$1, KK
#endif

#ifdef RT
	subl	$1, KK
#endif
	ALIGN_4

.L30:
	testl	$2, N
	je	.L60

#if defined(LT) || defined(RN)
	movl	A, AA
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

#ifdef RT
	movl	K, %eax
	sall	$1 + BASE_SHIFT, %eax
	subl	%eax, B
#endif

	leal	(, LDC, 2), %eax

#ifdef RT
	subl	%eax, C
#endif
	movl	C, CO1
#ifndef RT
	addl	%eax, C
#endif

#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

#ifdef LT
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	M,  %ebx
	sarl	$1, %ebx	# i = (m >> 2)
	jle	.L50
	ALIGN_4

.L41:
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

	movl	B, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + BASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movddup	 0 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	movddup	 8 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

#ifdef LN
	prefetchnta	-2 * SIZE(CO1)
	prefetchnta	-2 * SIZE(CO1, LDC, 1)
#else
	prefetchnta	 2 * SIZE(CO1)
	prefetchnta	 2 * SIZE(CO1, LDC, 1)
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L45
	ALIGN_4

.L42:
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	 1 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 2 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm5
	movddup	 2 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm6
	movddup	 3 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 4 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm7
	movddup	 4 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	 5 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 6 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm5
	movddup	 6 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm6
	movddup	 7 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	16 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm7
	movddup	16 * SIZE(BB), %xmm2
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm4
	movddup	 9 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	movapd	10 * SIZE(AA), %xmm1
	addpd	 %xmm3, %xmm5
	movddup	10 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm6
	movddup	11 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	movapd	12 * SIZE(AA), %xmm1
	addpd	 %xmm3, %xmm7
	movddup	12 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm4
	movddup	13 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	movapd	14 * SIZE(AA), %xmm1
	addpd	 %xmm3, %xmm5
	movddup	14 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm6
	movddup	15 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	movapd	24 * SIZE(AA), %xmm1
	addpd	 %xmm3, %xmm7
	movddup	24 * SIZE(BB), %xmm3

	addl   $16 * SIZE, AA
	addl   $16 * SIZE, BB
	decl   %eax
	jne    .L42
	ALIGN_4

.L45:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L48
	ALIGN_3

.L46:
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	 1 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 2 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm5
	movddup	 2 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L46
	ALIGN_4

.L48:
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

	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 2), BB
#endif

#if defined(LN) || defined(LT)
	movapd	 %xmm4, %xmm0
	unpcklpd %xmm5, %xmm4
	unpckhpd %xmm5, %xmm0

	movapd	 0 * SIZE(BB), %xmm2
	movapd	 2 * SIZE(BB), %xmm3

	subpd	%xmm4,  %xmm2
	subpd	%xmm0,  %xmm3
#else
	movapd	 0 * SIZE(AA), %xmm0
	movapd	 2 * SIZE(AA), %xmm1

	subpd	%xmm4, %xmm0
	subpd	%xmm5, %xmm1
#endif

#ifdef LN
	movddup	 3 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm3

	movddup	 2 * SIZE(AA), %xmm4
	mulpd	 %xmm3, %xmm4
	subpd	 %xmm4, %xmm2

	movddup	 0 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm2

#endif

#ifdef LT
	movddup	 0 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm2

	movddup	 1 * SIZE(AA), %xmm4
	mulpd	 %xmm2, %xmm4
	subpd	 %xmm4, %xmm3

	movddup	 3 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm3
#endif

#ifdef RN
	movddup	 0 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm0

	movddup	 1 * SIZE(BB), %xmm4
	mulpd	 %xmm0, %xmm4
	subpd	 %xmm4, %xmm1

	movddup	 3 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm1
#endif

#ifdef RT
	movddup	 3 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm1

	movddup	 2 * SIZE(BB), %xmm4
	mulpd	 %xmm1, %xmm4
	subpd	 %xmm4, %xmm0

	movddup	 0 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm2,   0 * SIZE(BB)
	movapd	%xmm3,   2 * SIZE(BB)
#else
	movapd	%xmm0,   0 * SIZE(AA)
	movapd	%xmm1,   2 * SIZE(AA)
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm2, 0 * SIZE(CO1)
	movsd	%xmm3, 1 * SIZE(CO1)
	movhpd	%xmm2, 0 * SIZE(CO1, LDC, 1)
	movhpd	%xmm3, 1 * SIZE(CO1, LDC, 1)
#else
	movsd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 1 * SIZE(CO1)
	movsd	%xmm1, 0 * SIZE(CO1, LDC, 1)
	movhpd	%xmm1, 1 * SIZE(CO1, LDC, 1)
#endif

#ifndef LN
	addl	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 2), BB
#endif

#ifdef LN
	subl	$2, KK
#endif

#ifdef LT
	addl	$2, KK
#endif

#ifdef RT
	movl	K, %eax
	sall	$1 + BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx			# i --
	jg	.L41
	ALIGN_4

.L50:
	movl	M,  %ebx
	testl	$1, %ebx	# i = (m >> 2)
	jle	.L59

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

	movl	B, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + BASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movddup	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movddup	 8 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movapd	 0 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	movapd	 8 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$4, %eax
	je	.L55
	ALIGN_4

.L52:
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)
	mulpd	 %xmm0, %xmm2
	movddup	 1 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm4
	mulpd	 2 * SIZE(BB), %xmm0
	movapd	16 * SIZE(BB), %xmm2
	addpd	 %xmm0, %xmm5
	movddup	 2 * SIZE(AA), %xmm0
	mulpd	 4 * SIZE(BB), %xmm0
	addpd	 %xmm0, %xmm6
	movddup	 3 * SIZE(AA), %xmm0
	mulpd	 6 * SIZE(BB), %xmm0
	addpd	 %xmm0, %xmm7
	movddup	 4 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm3
	movddup	 5 * SIZE(AA), %xmm0
	addpd	 %xmm3, %xmm4
	mulpd	10 * SIZE(BB), %xmm0
	movapd	24 * SIZE(BB), %xmm3
	addpd	 %xmm0, %xmm5
	movddup	 6 * SIZE(AA), %xmm0
	mulpd	12 * SIZE(BB), %xmm0
	addpd	 %xmm0, %xmm6
	movddup	 7 * SIZE(AA), %xmm0
	mulpd	14 * SIZE(BB), %xmm0
	addpd	 %xmm0, %xmm7
	movddup	16 * SIZE(AA), %xmm0
	mulpd	 %xmm1, %xmm2
	movddup	 9 * SIZE(AA), %xmm1
	addpd	 %xmm2, %xmm4
	mulpd	18 * SIZE(BB), %xmm1
	movapd	32 * SIZE(BB), %xmm2
	addpd	 %xmm1, %xmm5
	movddup	10 * SIZE(AA), %xmm1
	mulpd	20 * SIZE(BB), %xmm1
	addpd	 %xmm1, %xmm6
	movddup	11 * SIZE(AA), %xmm1
	mulpd	22 * SIZE(BB), %xmm1
	addpd	 %xmm1, %xmm7
	movddup	12 * SIZE(AA), %xmm1
	mulpd	 %xmm1, %xmm3
	movddup	13 * SIZE(AA), %xmm1
	addpd	 %xmm3, %xmm4
	mulpd	26 * SIZE(BB), %xmm1
	movapd	40 * SIZE(BB), %xmm3
	addpd	 %xmm1, %xmm5
	movddup	14 * SIZE(AA), %xmm1
	mulpd	28 * SIZE(BB), %xmm1
	addpd	 %xmm1, %xmm6
	movddup	15 * SIZE(AA), %xmm1
	mulpd	30 * SIZE(BB), %xmm1
	addpd	 %xmm1, %xmm7
	movddup	24 * SIZE(AA), %xmm1

	addl   $16 * SIZE, AA
	addl   $32 * SIZE, BB
	decl   %eax
	jne    .L52
	ALIGN_4

.L55:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$15, %eax		# if (k & 1)
	BRANCH
	je .L58

.L56:
	mulpd	 %xmm0, %xmm2
	movddup	 1 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm4
	movapd	 2 * SIZE(BB), %xmm2

	addl	$1 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L56
	ALIGN_4

.L58:
	addpd	%xmm5, %xmm4
	addpd	%xmm7, %xmm6
	addpd	%xmm6, %xmm4

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$2, %eax
#endif

	movl	AORIG, AA

	leal	(, %eax, SIZE), %eax
	addl	%eax, AA
	leal	(B,  %eax, 2), BB
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(BB), %xmm0

	subpd	%xmm4,  %xmm0
#else
	movapd	 0 * SIZE(AA), %xmm1

	subpd	%xmm4, %xmm1

	movapd	       %xmm1, %xmm0
	unpckhpd       %xmm1, %xmm1
#endif

#ifdef LN
	movddup	 0 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm0
#endif

#ifdef LT
	movddup	 0 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm0
#endif

#ifdef RN
	movsd	 0 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm0

	movsd	 1 * SIZE(BB), %xmm4
	mulsd	 %xmm0, %xmm4
	subsd	 %xmm4, %xmm1

	movsd	 3 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm1
#endif

#ifdef RT
	movsd	 3 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm1

	movsd	 2 * SIZE(BB), %xmm4
	mulsd	 %xmm1, %xmm4
	subsd	 %xmm4, %xmm0

	movsd	 0 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm0,   0 * SIZE(BB)
#else
	movsd	%xmm0,   0 * SIZE(AA)
	movsd	%xmm1,   1 * SIZE(AA)
#endif

#ifdef LN
	subl	$1 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 0 * SIZE(CO1, LDC, 1)
#else
	movsd	%xmm0, 0 * SIZE(CO1)
	movsd	%xmm1, 0 * SIZE(CO1, LDC, 1)
#endif

#ifndef LN
	addl	$1 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 2), BB
#endif

#ifdef LN
	subl	$1, KK
#endif

#ifdef LT
	addl	$1, KK
#endif

#ifdef RT
	movl	K, %eax
	sall	$1 + BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif
	ALIGN_4

.L59:
#ifdef LN
       movl	K, %eax
       leal	(, %eax, SIZE), %eax
       leal 	(B, %eax, 2), B
#endif

#if defined(LT) || defined(RN)
	movl	BB, B
#endif

#ifdef RN
	addl	$2, KK
#endif

#ifdef RT
	subl	$2, KK
#endif
	ALIGN_4

.L60:
	movl	N, %eax
	sarl	$2, %eax
	movl	%eax, J
	jle	.L999
	ALIGN_2

.L10:
#if defined(LT) || defined(RN)
	movl	A, AA
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

#ifdef RT
	movl	K, %eax
	sall	$2 + BASE_SHIFT, %eax
	subl	%eax, B
#endif

	leal	(, LDC, 4), %eax

#ifdef RT
	subl	%eax, C
#endif
	movl	C, CO1
#ifndef RT
	addl	%eax, C
#endif

#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

#ifdef LT
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	M,  %ebx
	sarl	$1, %ebx	# i = (m >> 2)
	jle	.L20
	ALIGN_4

.L11:
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

	movl	B, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$2 + BASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movapd	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movapd	 8 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movddup	 0 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	movddup	 8 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

	leal	(LDC, LDC, 2), %eax

#ifdef LN
	prefetchnta	-2 * SIZE(CO1)
	prefetchnta	-2 * SIZE(CO1, LDC, 1)
	prefetchnta	-2 * SIZE(CO1, LDC, 2)
	prefetchnta	-2 * SIZE(CO1, %eax, 1)
#else
	prefetchnta	 2 * SIZE(CO1)
	prefetchnta	 2 * SIZE(CO1, LDC, 1)
	prefetchnta	 2 * SIZE(CO1, LDC, 2)
	prefetchnta	 2 * SIZE(CO1, %eax, 1)
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L15
	ALIGN_4

.L12:
	mulpd	 %xmm0, %xmm2
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)
	addpd	 %xmm2, %xmm4
	movddup	 1 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm5
	movddup	 2 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm6
	movddup	 3 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 2 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm7
	movddup	 4 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	 5 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm5
	movddup	 6 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm6
	movddup	 7 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 4 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm7
	movddup	16 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm3
	addpd	 %xmm3, %xmm4
	movddup	 9 * SIZE(BB), %xmm3
	mulpd	 %xmm0, %xmm3
	addpd	 %xmm3, %xmm5
	movddup	10 * SIZE(BB), %xmm3
	mulpd	 %xmm0, %xmm3
	addpd	 %xmm3, %xmm6
	movddup	11 * SIZE(BB), %xmm3
	mulpd	 %xmm0, %xmm3
	movapd	 6 * SIZE(AA), %xmm0
	addpd	 %xmm3, %xmm7
	movddup	12 * SIZE(BB), %xmm3
	mulpd	 %xmm0, %xmm3
	addpd	 %xmm3, %xmm4
	movddup	13 * SIZE(BB), %xmm3
	mulpd	 %xmm0, %xmm3
	addpd	 %xmm3, %xmm5
	movddup	14 * SIZE(BB), %xmm3
	mulpd	 %xmm0, %xmm3
	addpd	 %xmm3, %xmm6
	movddup	15 * SIZE(BB), %xmm3
	mulpd	 %xmm0, %xmm3
	movapd	16 * SIZE(AA), %xmm0
	addpd	 %xmm3, %xmm7
	movddup	24 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	17 * SIZE(BB), %xmm2
	mulpd	 %xmm1, %xmm2
	addpd	 %xmm2, %xmm5
	movddup	18 * SIZE(BB), %xmm2
	mulpd	 %xmm1, %xmm2
	addpd	 %xmm2, %xmm6
	movddup	19 * SIZE(BB), %xmm2
	mulpd	 %xmm1, %xmm2
	movapd	10 * SIZE(AA), %xmm1
	addpd	 %xmm2, %xmm7
	movddup	20 * SIZE(BB), %xmm2
	mulpd	 %xmm1, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	21 * SIZE(BB), %xmm2
	mulpd	 %xmm1, %xmm2
	addpd	 %xmm2, %xmm5
	movddup	22 * SIZE(BB), %xmm2
	mulpd	 %xmm1, %xmm2
	addpd	 %xmm2, %xmm6
	movddup	23 * SIZE(BB), %xmm2
	mulpd	 %xmm1, %xmm2
	movapd	12 * SIZE(AA), %xmm1
	addpd	 %xmm2, %xmm7
	movddup	32 * SIZE(BB), %xmm2
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm4
	movddup	25 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm5
	movddup	26 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm6
	movddup	27 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	movapd	14 * SIZE(AA), %xmm1
	addpd	 %xmm3, %xmm7
	movddup	28 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm4
	movddup	29 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm5
	movddup	30 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	addpd	 %xmm3, %xmm6
	movddup	31 * SIZE(BB), %xmm3
	mulpd	 %xmm1, %xmm3
	movapd	24 * SIZE(AA), %xmm1
	addpd	 %xmm3, %xmm7
	movddup	40 * SIZE(BB), %xmm3

	addl   $32 * SIZE, BB
	addl   $16 * SIZE, AA
	decl   %eax
	jne    .L12
	ALIGN_4

.L15:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L18
	ALIGN_3

.L16:
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm4
	movddup	 1 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm5
	movddup	 2 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	addpd	 %xmm2, %xmm6
	movddup	 3 * SIZE(BB), %xmm2
	mulpd	 %xmm0, %xmm2
	movapd	 2 * SIZE(AA), %xmm0
	addpd	 %xmm2, %xmm7
	movddup	 4 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L16
	ALIGN_4

.L18:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$2, %eax
#else
	subl	$4, %eax
#endif

	movl	AORIG, AA

	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 4), BB
#endif

#if defined(LN) || defined(LT)
	movapd	 %xmm4, %xmm0
	unpcklpd %xmm5, %xmm4
	unpckhpd %xmm5, %xmm0

	movapd	 %xmm6, %xmm1
	unpcklpd %xmm7, %xmm6
	unpckhpd %xmm7, %xmm1

	movapd	 0 * SIZE(BB), %xmm2
	movapd	 2 * SIZE(BB), %xmm5
	movapd	 4 * SIZE(BB), %xmm3
	movapd	 6 * SIZE(BB), %xmm7

	subpd	%xmm4,  %xmm2
	subpd	%xmm6,  %xmm5
	subpd	%xmm0,  %xmm3
	subpd	%xmm1,  %xmm7
#else
	movapd	 0 * SIZE(AA), %xmm0
	movapd	 2 * SIZE(AA), %xmm1
	movapd	 4 * SIZE(AA), %xmm2
	movapd	 6 * SIZE(AA), %xmm3

	subpd	%xmm4, %xmm0
	subpd	%xmm5, %xmm1
	subpd	%xmm6, %xmm2
	subpd	%xmm7, %xmm3
#endif

#ifdef LN
	movddup	 3 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm3
	mulpd	 %xmm4, %xmm7

	movddup	 2 * SIZE(AA), %xmm4
	movapd	 %xmm4, %xmm6
	mulpd	 %xmm3, %xmm4
	subpd	 %xmm4, %xmm2
	mulpd	 %xmm7, %xmm6
	subpd	 %xmm6, %xmm5

	movddup	 0 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm2
	mulpd	 %xmm4, %xmm5

#endif

#ifdef LT
	movddup	 0 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm2
	mulpd	 %xmm4, %xmm5

	movddup	 1 * SIZE(AA), %xmm4
	movapd	 %xmm4, %xmm6
	mulpd	 %xmm2, %xmm4
	subpd	 %xmm4, %xmm3
	mulpd	 %xmm5, %xmm6
	subpd	 %xmm6, %xmm7

	movddup	 3 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm3
	mulpd	 %xmm4, %xmm7
#endif

#ifdef RN
	movddup	 0 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm0
	movddup	 1 * SIZE(BB), %xmm4
	mulpd	 %xmm0, %xmm4
	subpd	 %xmm4, %xmm1
	movddup	 2 * SIZE(BB), %xmm4
	mulpd	 %xmm0, %xmm4
	subpd	 %xmm4, %xmm2
	movddup	 3 * SIZE(BB), %xmm4
	mulpd	 %xmm0, %xmm4
	subpd	 %xmm4, %xmm3

	movddup	 5 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm1
	movddup	 6 * SIZE(BB), %xmm4
	mulpd	 %xmm1, %xmm4
	subpd	 %xmm4, %xmm2
	movddup	 7 * SIZE(BB), %xmm4
	mulpd	 %xmm1, %xmm4
	subpd	 %xmm4, %xmm3

	movddup	10 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm2
	movddup	11 * SIZE(BB), %xmm4
	mulpd	 %xmm2, %xmm4
	subpd	 %xmm4, %xmm3

	movddup	15 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm3
#endif

#ifdef RT
	movddup	15 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm3
	movddup	14 * SIZE(BB), %xmm4
	mulpd	 %xmm3, %xmm4
	subpd	 %xmm4, %xmm2
	movddup	13 * SIZE(BB), %xmm4
	mulpd	 %xmm3, %xmm4
	subpd	 %xmm4, %xmm1
	movddup	12 * SIZE(BB), %xmm4
	mulpd	 %xmm3, %xmm4
	subpd	 %xmm4, %xmm0

	movddup	10 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm2
	movddup	 9 * SIZE(BB), %xmm4
	mulpd	 %xmm2, %xmm4
	subpd	 %xmm4, %xmm1
	movddup	 8 * SIZE(BB), %xmm4
	mulpd	 %xmm2, %xmm4
	subpd	 %xmm4, %xmm0

	movddup	 5 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm1
	movddup	 4 * SIZE(BB), %xmm4
	mulpd	 %xmm1, %xmm4
	subpd	 %xmm4, %xmm0

	movddup	 0 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm2,   0 * SIZE(BB)
	movapd	%xmm5,   2 * SIZE(BB)
	movapd	%xmm3,   4 * SIZE(BB)
	movapd	%xmm7,   6 * SIZE(BB)
#else
	movapd	%xmm0,   0 * SIZE(AA)
	movapd	%xmm1,   2 * SIZE(AA)
	movapd	%xmm2,   4 * SIZE(AA)
	movapd	%xmm3,   6 * SIZE(AA)
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

	leal	(LDC, LDC, 2), %eax

#if defined(LN) || defined(LT)
	movsd	%xmm2, 0 * SIZE(CO1)
	movsd	%xmm3, 1 * SIZE(CO1)
	movhpd	%xmm2, 0 * SIZE(CO1, LDC, 1)
	movhpd	%xmm3, 1 * SIZE(CO1, LDC, 1)
	movsd	%xmm5, 0 * SIZE(CO1, LDC, 2)
	movsd	%xmm7, 1 * SIZE(CO1, LDC, 2)
	movhpd	%xmm5, 0 * SIZE(CO1, %eax, 1)
	movhpd	%xmm7, 1 * SIZE(CO1, %eax, 1)
#else
	movsd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 1 * SIZE(CO1)
	movsd	%xmm1, 0 * SIZE(CO1, LDC, 1)
	movhpd	%xmm1, 1 * SIZE(CO1, LDC, 1)
	movsd	%xmm2, 0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm2, 1 * SIZE(CO1, LDC, 2)
	movsd	%xmm3, 0 * SIZE(CO1, %eax, 1)
	movhpd	%xmm3, 1 * SIZE(CO1, %eax, 1)
#endif

#ifndef LN
	addl	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB
#endif

#ifdef LN
	subl	$2, KK
#endif

#ifdef LT
	addl	$2, KK
#endif

#ifdef RT
	movl	K, %eax
	sall	$1 + BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx			# i --
	jg	.L11
	ALIGN_4

.L20:
	movl	M,  %ebx
	testl	$1, %ebx	# i = (m >> 2)
	jle	.L29

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

	movl	B, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$2 + BASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movddup	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movddup	 8 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movapd	 0 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	movapd	 8 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$4, %eax
	je	.L25
	ALIGN_4

.L22:
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)
	mulpd	 %xmm0, %xmm2
	mulpd	 2 * SIZE(BB), %xmm0
	addpd	 %xmm2, %xmm4
	movapd	 4 * SIZE(BB), %xmm2
	addpd	 %xmm0, %xmm5
	movddup	 1 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm2
	mulpd	 6 * SIZE(BB), %xmm0
	addpd	 %xmm2, %xmm6
	movapd	16 * SIZE(BB), %xmm2
	addpd	 %xmm0, %xmm7
	movddup	 2 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm3
	mulpd	10 * SIZE(BB), %xmm0
	addpd	 %xmm3, %xmm4
	movapd	12 * SIZE(BB), %xmm3
	addpd	 %xmm0, %xmm5
	movddup	 3 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm3
	mulpd	14 * SIZE(BB), %xmm0
	addpd	 %xmm3, %xmm6
	movapd	24 * SIZE(BB), %xmm3
	addpd	 %xmm0, %xmm7
	movddup	 4 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm2
	mulpd	18 * SIZE(BB), %xmm0
	addpd	 %xmm2, %xmm4
	movapd	20 * SIZE(BB), %xmm2
	addpd	 %xmm0, %xmm5
	movddup	 5 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm2
	mulpd	22 * SIZE(BB), %xmm0
	addpd	 %xmm2, %xmm6
	movapd	32 * SIZE(BB), %xmm2
	addpd	 %xmm0, %xmm7
	movddup	 6 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm3
	mulpd	26 * SIZE(BB), %xmm0
	addpd	 %xmm3, %xmm4
	movapd	28 * SIZE(BB), %xmm3
	addpd	 %xmm0, %xmm5
	movddup	 7 * SIZE(AA), %xmm0
	mulpd	 %xmm0, %xmm3
	mulpd	30 * SIZE(BB), %xmm0
	addpd	 %xmm3, %xmm6
	movapd	40 * SIZE(BB), %xmm3
	addpd	 %xmm0, %xmm7
	movddup	16 * SIZE(AA), %xmm0
	mulpd	 %xmm1, %xmm2
	mulpd	34 * SIZE(BB), %xmm1
	addpd	 %xmm2, %xmm4
	movapd	36 * SIZE(BB), %xmm2
	addpd	 %xmm1, %xmm5
	movddup	 9 * SIZE(AA), %xmm1
	mulpd	 %xmm1, %xmm2
	mulpd	38 * SIZE(BB), %xmm1
	addpd	 %xmm2, %xmm6
	movapd	48 * SIZE(BB), %xmm2
	addpd	 %xmm1, %xmm7
	movddup	10 * SIZE(AA), %xmm1
	mulpd	 %xmm1, %xmm3
	mulpd	42 * SIZE(BB), %xmm1
	addpd	 %xmm3, %xmm4
	movapd	44 * SIZE(BB), %xmm3
	addpd	 %xmm1, %xmm5
	movddup	11 * SIZE(AA), %xmm1
	mulpd	 %xmm1, %xmm3
	mulpd	46 * SIZE(BB), %xmm1
	addpd	 %xmm3, %xmm6
	movapd	56 * SIZE(BB), %xmm3
	addpd	 %xmm1, %xmm7
	movddup	12 * SIZE(AA), %xmm1
	mulpd	 %xmm1, %xmm2
	mulpd	50 * SIZE(BB), %xmm1
	addpd	 %xmm2, %xmm4
	movapd	52 * SIZE(BB), %xmm2
	addpd	 %xmm1, %xmm5
	movddup	13 * SIZE(AA), %xmm1
	mulpd	 %xmm1, %xmm2
	mulpd	54 * SIZE(BB), %xmm1
	addpd	 %xmm2, %xmm6
	movapd	64 * SIZE(BB), %xmm2
	addpd	 %xmm1, %xmm7
	movddup	14 * SIZE(AA), %xmm1
	mulpd	 %xmm1, %xmm3
	mulpd	58 * SIZE(BB), %xmm1
	addpd	 %xmm3, %xmm4
	movapd	60 * SIZE(BB), %xmm3
	addpd	 %xmm1, %xmm5
	movddup	15 * SIZE(AA), %xmm1
	mulpd	 %xmm1, %xmm3
	mulpd	62 * SIZE(BB), %xmm1
	addpd	 %xmm3, %xmm6
	movapd	72 * SIZE(BB), %xmm3
	addpd	 %xmm1, %xmm7
	movddup	24 * SIZE(AA), %xmm1

	addl   $16 * SIZE, AA
	addl   $64 * SIZE, BB
	decl   %eax
	jne    .L22
	ALIGN_4

.L25:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$15, %eax		# if (k & 1)
	BRANCH
	je .L28

.L26:
	mulpd	 %xmm0, %xmm2
	mulpd	 2 * SIZE(BB), %xmm0
	addpd	 %xmm2, %xmm4
	movapd	 4 * SIZE(BB), %xmm2
	addpd	 %xmm0, %xmm5
	movddup	 1 * SIZE(AA), %xmm0

	addl	$1 * SIZE, AA
	addl	$4 * SIZE, BB

	decl	%eax
	jg	.L26
	ALIGN_4

.L28:
	addpd	%xmm6, %xmm4
	addpd	%xmm7, %xmm5

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$4, %eax
#endif

	movl	AORIG, AA

	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 4), BB
#endif

#if defined(LN) || defined(LT)
	movapd	 0 * SIZE(BB), %xmm0
	movapd	 2 * SIZE(BB), %xmm1

	subpd	%xmm4,  %xmm0
	subpd	%xmm5,  %xmm1
#else
	movapd	 0 * SIZE(AA), %xmm1
	movapd	 2 * SIZE(AA), %xmm3

	subpd	%xmm4, %xmm1
	subpd	%xmm5, %xmm3

	movapd	       %xmm1, %xmm0
	unpckhpd       %xmm1, %xmm1
	movapd	       %xmm3, %xmm2
	unpckhpd       %xmm3, %xmm3
#endif

#ifdef LN
	movddup	 0 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm0
	mulpd	 %xmm4, %xmm1
#endif

#ifdef LT
	movddup	 0 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm0
	mulpd	 %xmm4, %xmm1
#endif

#ifdef RN
	movsd	 0 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm0
	movsd	 1 * SIZE(BB), %xmm4
	mulsd	 %xmm0, %xmm4
	subsd	 %xmm4, %xmm1
	movsd	 2 * SIZE(BB), %xmm4
	mulsd	 %xmm0, %xmm4
	subsd	 %xmm4, %xmm2
	movsd	 3 * SIZE(BB), %xmm4
	mulsd	 %xmm0, %xmm4
	subsd	 %xmm4, %xmm3

	movsd	 5 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm1
	movsd	 6 * SIZE(BB), %xmm4
	mulsd	 %xmm1, %xmm4
	subsd	 %xmm4, %xmm2
	movsd	 7 * SIZE(BB), %xmm4
	mulsd	 %xmm1, %xmm4
	subsd	 %xmm4, %xmm3

	movsd	10 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm2
	movsd	11 * SIZE(BB), %xmm4
	mulsd	 %xmm2, %xmm4
	subsd	 %xmm4, %xmm3

	movsd	15 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm3
#endif

#ifdef RT
	movsd	15 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm3
	movsd	14 * SIZE(BB), %xmm4
	mulsd	 %xmm3, %xmm4
	subsd	 %xmm4, %xmm2
	movsd	13 * SIZE(BB), %xmm4
	mulsd	 %xmm3, %xmm4
	subsd	 %xmm4, %xmm1
	movsd	12 * SIZE(BB), %xmm4
	mulsd	 %xmm3, %xmm4
	subsd	 %xmm4, %xmm0

	movsd	10 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm2
	movsd	 9 * SIZE(BB), %xmm4
	mulsd	 %xmm2, %xmm4
	subsd	 %xmm4, %xmm1
	movsd	 8 * SIZE(BB), %xmm4
	mulsd	 %xmm2, %xmm4
	subsd	 %xmm4, %xmm0

	movsd	 5 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm1
	movsd	 4 * SIZE(BB), %xmm4
	mulsd	 %xmm1, %xmm4
	subsd	 %xmm4, %xmm0

	movsd	 0 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm0,   0 * SIZE(BB)
	movapd	%xmm1,   2 * SIZE(BB)
#else
	movsd	%xmm0,   0 * SIZE(AA)
	movsd	%xmm1,   1 * SIZE(AA)
	movsd	%xmm2,   2 * SIZE(AA)
	movsd	%xmm3,   3 * SIZE(AA)
#endif

#ifdef LN
	subl	$1 * SIZE, CO1
#endif

	leal	(LDC, LDC, 2), %eax

#if defined(LN) || defined(LT)
	movsd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 0 * SIZE(CO1, LDC, 1)
	movsd	%xmm1, 0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm1, 0 * SIZE(CO1, %eax, 1)
#else
	movsd	%xmm0, 0 * SIZE(CO1)
	movsd	%xmm1, 0 * SIZE(CO1, LDC, 1)
	movsd	%xmm2, 0 * SIZE(CO1, LDC, 2)
	movsd	%xmm3, 0 * SIZE(CO1, %eax, 1)
#endif

#ifndef LN
	addl	$1 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB
#endif

#ifdef LN
	subl	$1, KK
#endif

#ifdef LT
	addl	$1, KK
#endif

#ifdef RT
	movl	K, %eax
	sall	$BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif
	ALIGN_4

.L29:
#ifdef LN
       movl	K, %eax
       leal	(, %eax, SIZE), %eax
       leal 	(B, %eax, 4), B
#endif

#if defined(LT) || defined(RN)
	movl	BB, B
#endif

#ifdef RN
	addl	$4, KK
#endif

#ifdef RT
	subl	$4, KK
#endif

	decl	J			# j --
	jg	.L10
	ALIGN_4

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp

	addl	$ARGS, %esp
	ret

	EPILOGUE
