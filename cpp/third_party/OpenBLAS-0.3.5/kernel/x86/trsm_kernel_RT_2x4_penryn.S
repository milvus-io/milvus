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

#if defined(PENRYN) || defined(DUNNINGTON)
#define PREFETCH     prefetcht0
#define PREFETCHSIZE  (8 * 21 + 4)
#endif

#if defined(NEHALEM) || defined(SANDYBRIDGE) || defined(HASWELL) || defined (SKYLAKEX)
#define PREFETCH     prefetcht0
#define PREFETCHSIZE  (8 * 21 + 4)
#endif

#ifdef NANO
#define PREFETCH	prefetcht0
#define PREFETCHSIZE (8 * 2)
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

	subl	$-16 * SIZE, A
	subl	$-16 * SIZE, B

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

	movsd	-16 * SIZE(AA), %xmm0
	movhps	-15 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movsd	-16 * SIZE(BB), %xmm1
	movhps	-15 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
#ifdef LN
	prefetcht0	-2 * SIZE(CO1)
#else
	prefetcht0	 1 * SIZE(CO1)
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
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	pshufd	$0x44, %xmm1, %xmm2
	mulpd	%xmm0, %xmm2
	movaps	-14 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm4

	pshufd	$0xee, %xmm1, %xmm2
	movaps	-14 * SIZE(BB), %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-12 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm5

	pshufd	$0x44, %xmm1, %xmm2
	mulpd	%xmm0, %xmm2
	movaps	-10 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm4

	pshufd	$0xee, %xmm1, %xmm2
	movaps	-12 * SIZE(BB), %xmm1
	mulpd	%xmm0, %xmm2
	movaps	 -8 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm5

	PREFETCH (PREFETCHSIZE +  8) * SIZE(AA)

	pshufd	$0x44, %xmm1, %xmm2
	mulpd	%xmm0, %xmm2
	movaps	 -6 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm4

	pshufd	$0xee, %xmm1, %xmm2
	movaps	-10 * SIZE(BB), %xmm1
	mulpd	%xmm0, %xmm2
	movaps	 -4 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm5

	pshufd	$0x44, %xmm1, %xmm2
	mulpd	%xmm0, %xmm2
	movaps	 -2 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm4

	pshufd	$0xee, %xmm1, %xmm2
	movaps	 -8 * SIZE(BB), %xmm1
	mulpd	%xmm0, %xmm2
	movaps	  0 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm5

	subl   $-16 * SIZE, AA
	subl   $ -8 * SIZE, BB

	subl   $1, %eax
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
	pshufd	$0x44, %xmm1, %xmm2
	movsd	-15 * SIZE(BB), %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-14 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm4

	addl	$2 * SIZE, AA
	addl	$1 * SIZE, BB
	decl	%eax
	jg	.L76
	ALIGN_4

.L78:
	addpd	%xmm5, %xmm4

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
	movapd	-16 * SIZE(BB), %xmm1

	subpd	%xmm4,  %xmm1

	movapd	%xmm1, %xmm0
	unpckhpd %xmm1, %xmm1
#else
	movapd	-16 * SIZE(AA), %xmm0

	subpd	%xmm4, %xmm0
#endif

#ifdef LN
	movsd	-13 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm1

	movsd	-14 * SIZE(AA), %xmm4
	mulsd	 %xmm1, %xmm4
	subsd	 %xmm4, %xmm0

	movsd	-16 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm0

#endif

#ifdef LT
	movsd	-16 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm0

	movsd	-15 * SIZE(AA), %xmm4
	mulsd	 %xmm0, %xmm4
	subsd	 %xmm4, %xmm1

	movsd	-13 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm1
#endif

#ifdef RN
	movddup	-16 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm0
#endif

#ifdef RT
	movddup	-16 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm0,  -16 * SIZE(BB)
	movsd	%xmm1,  -15 * SIZE(BB)
#else
	movapd	%xmm0,  -16 * SIZE(AA)
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm0, 0 * SIZE(CO1)
	movsd	%xmm1, 1 * SIZE(CO1)
#else
	movsd	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 1 * SIZE(CO1)
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

	movsd	-16 * SIZE(AA), %xmm0
	movhps	-15 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movsd	-16 * SIZE(BB), %xmm2
	movhps	-15 * SIZE(BB), %xmm2
	pxor	%xmm5, %xmm5

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L85
	ALIGN_4

.L82:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	mulpd	%xmm0, %xmm2
	movaps	-14 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm4
	movaps	-14 * SIZE(BB), %xmm2

	mulpd	%xmm0, %xmm2
	movaps	-12 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm5
	movaps	-12 * SIZE(BB), %xmm2

	mulpd	%xmm0, %xmm2
	movaps	-10 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm4
	movaps	-10 * SIZE(BB), %xmm2

	mulpd	%xmm0, %xmm2
	movaps	 -8 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm5
	movaps	 -8 * SIZE(BB), %xmm2

	subl   $-8 * SIZE, AA
	subl   $-8 * SIZE, BB
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
	andl	$7, %eax
	BRANCH
	je .L88

.L86:
	mulsd	%xmm0, %xmm2
	movsd	-15 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm4
	movsd	-15 * SIZE(BB), %xmm2

	addl	$1 * SIZE, AA
	addl	$1 * SIZE, BB
	decl	%eax
	jg	.L86
	ALIGN_4

.L88:
	addpd	%xmm5, %xmm4
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
	movsd	-16 * SIZE(BB), %xmm0
	subsd	%xmm4,  %xmm0
#else
	movsd	-16 * SIZE(AA), %xmm0
	subsd	%xmm4, %xmm0
#endif

#ifdef LN
	movsd	-16 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm0
#endif

#ifdef LT
	movsd	-16 * SIZE(AA), %xmm4
	mulsd	 %xmm4, %xmm0
#endif

#ifdef RN
	movsd	-16 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm0
#endif

#ifdef RT
	movsd	-16 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm0,  -16 * SIZE(BB)
#else
	movsd	%xmm0,  -16 * SIZE(AA)
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

	movaps	-16 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movaps	-16 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
#ifdef LN
	prefetcht0	-2 * SIZE(CO1)
	pxor	%xmm6, %xmm6
	prefetcht0	-2 * SIZE(CO1, LDC)
	pxor	%xmm7, %xmm7
#else
	prefetcht0	 1 * SIZE(CO1)
	pxor	%xmm6, %xmm6
	prefetcht0	 1 * SIZE(CO1, LDC)
	pxor	%xmm7, %xmm7
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
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-14 * SIZE(AA), %xmm0

	addpd	%xmm1, %xmm5
	movaps	-14 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-12 * SIZE(AA), %xmm0

	addpd	%xmm1, %xmm7
	movaps	-12 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm6

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-10 * SIZE(AA), %xmm0

	addpd	%xmm1, %xmm5
	movaps	-10 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	 -8 * SIZE(AA), %xmm0

	addpd	%xmm1, %xmm7
	movaps	 -8 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm6

	PREFETCH (PREFETCHSIZE +  8) * SIZE(AA)

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	 -6 * SIZE(AA), %xmm0

	addpd	%xmm1, %xmm5
	movaps	 -6 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	 -4 * SIZE(AA), %xmm0

	addpd	%xmm1, %xmm7
	movaps	 -4 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm6

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	 -2 * SIZE(AA), %xmm0

	addpd	%xmm1, %xmm5
	movaps	 -2 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	  0 * SIZE(AA), %xmm0

	addpd	%xmm1, %xmm7
	movaps	  0 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm6

	subl   $-16 * SIZE, AA
	subl   $-16 * SIZE, BB

	subl   $1, %eax
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
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-14 * SIZE(AA), %xmm0

	addpd	%xmm1, %xmm5
	movaps	-14 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4

	addl	$2 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L46
	ALIGN_4

.L48:
	addpd	%xmm6, %xmm4
	addpd	%xmm7, %xmm5

	movaps	 %xmm4, %xmm0
	movsd    %xmm5, %xmm4
	movsd    %xmm0, %xmm5

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

	movapd	-16 * SIZE(BB), %xmm2
	movapd	-14 * SIZE(BB), %xmm3

	subpd	%xmm4,  %xmm2
	subpd	%xmm0,  %xmm3
#else
	movapd	-16 * SIZE(AA), %xmm0
	movapd	-14 * SIZE(AA), %xmm1

	subpd	%xmm4, %xmm0
	subpd	%xmm5, %xmm1
#endif

#ifdef LN
	movddup	-13 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm3

	movddup	-14 * SIZE(AA), %xmm4
	mulpd	 %xmm3, %xmm4
	subpd	 %xmm4, %xmm2

	movddup	-16 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm2

#endif

#ifdef LT
	movddup	-16 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm2

	movddup	-15 * SIZE(AA), %xmm4
	mulpd	 %xmm2, %xmm4
	subpd	 %xmm4, %xmm3

	movddup	-13 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm3
#endif

#ifdef RN
	movddup	-16 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm0

	movddup	-15 * SIZE(BB), %xmm4
	mulpd	 %xmm0, %xmm4
	subpd	 %xmm4, %xmm1

	movddup	-13 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm1
#endif

#ifdef RT
	movddup	-13 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm1

	movddup	-14 * SIZE(BB), %xmm4
	mulpd	 %xmm1, %xmm4
	subpd	 %xmm4, %xmm0

	movddup	-16 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm2,  -16 * SIZE(BB)
	movapd	%xmm3,  -14 * SIZE(BB)
#else
	movapd	%xmm0,  -16 * SIZE(AA)
	movapd	%xmm1,  -14 * SIZE(AA)
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm2, 0 * SIZE(CO1)
	movsd	%xmm3, 1 * SIZE(CO1)
	movhps	%xmm2, 0 * SIZE(CO1, LDC, 1)
	movhps	%xmm3, 1 * SIZE(CO1, LDC, 1)
#else
	movsd	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 1 * SIZE(CO1)
	movsd	%xmm1, 0 * SIZE(CO1, LDC, 1)
	movhps	%xmm1, 1 * SIZE(CO1, LDC, 1)
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

	movaps	-16 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movaps	-16 * SIZE(BB), %xmm2
	pxor	%xmm5, %xmm5

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L55
	ALIGN_4

.L52:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	pshufd	$0x44, %xmm0, %xmm1
	mulpd	%xmm1, %xmm2

	addpd	%xmm2, %xmm4
	movaps	-14 * SIZE(BB), %xmm2

	pshufd	$0xee, %xmm0, %xmm1
	movaps	-14 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2

	addpd	%xmm2, %xmm5
	movaps	-12 * SIZE(BB), %xmm2

	pshufd	$0x44, %xmm0, %xmm1
	mulpd	%xmm1, %xmm2

	addpd	%xmm2, %xmm4
	movaps	-10 * SIZE(BB), %xmm2

	pshufd	$0xee, %xmm0, %xmm1
	movaps	-12 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2

	addpd	%xmm2, %xmm5
	movaps	 -8 * SIZE(BB), %xmm2

	pshufd	$0x44, %xmm0, %xmm1
	mulpd	%xmm1, %xmm2

	addpd	%xmm2, %xmm4
	movaps	 -6 * SIZE(BB), %xmm2

	pshufd	$0xee, %xmm0, %xmm1
	movaps	-10 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2

	addpd	%xmm2, %xmm5
	movaps	 -4 * SIZE(BB), %xmm2

	pshufd	$0x44, %xmm0, %xmm1
	mulpd	%xmm1, %xmm2

	addpd	%xmm2, %xmm4
	movaps	 -2 * SIZE(BB), %xmm2

	pshufd	$0xee, %xmm0, %xmm1
	movaps	 -8 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2

	addpd	%xmm2, %xmm5
	movaps	 0 * SIZE(BB), %xmm2

	subl   $ -8 * SIZE, AA
	subl   $-16 * SIZE, BB

	subl   $1, %eax
	jne    .L52
	ALIGN_4

.L55:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax
	BRANCH
	je .L58

.L56:
	pshufd	$0x44, %xmm0, %xmm1
	movsd	-15 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2

	addpd	%xmm2, %xmm4
	movaps	-14 * SIZE(BB), %xmm2

	addl	$1 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L56
	ALIGN_4

.L58:
	addpd	%xmm5, %xmm4

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
	movapd	-16 * SIZE(BB), %xmm0

	subpd	%xmm4,  %xmm0
#else
	movapd	-16 * SIZE(AA), %xmm1

	subpd	%xmm4, %xmm1

	movapd	       %xmm1, %xmm0
	unpckhpd       %xmm1, %xmm1
#endif

#ifdef LN
	movddup	-16 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm0
#endif

#ifdef LT
	movddup	-16 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm0
#endif

#ifdef RN
	movsd	-16 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm0

	movsd	-15 * SIZE(BB), %xmm4
	mulsd	 %xmm0, %xmm4
	subsd	 %xmm4, %xmm1

	movsd	-13 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm1
#endif

#ifdef RT
	movsd	-13 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm1

	movsd	-14 * SIZE(BB), %xmm4
	mulsd	 %xmm1, %xmm4
	subsd	 %xmm4, %xmm0

	movsd	-16 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm0,  -16 * SIZE(BB)
#else
	movsd	%xmm0,  -16 * SIZE(AA)
	movsd	%xmm1,  -15 * SIZE(AA)
#endif

#ifdef LN
	subl	$1 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movsd	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 0 * SIZE(CO1, LDC, 1)
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
	movl	N,  %eax
	sarl	$2, %eax
	movl	%eax, J
	jle	.L999
	ALIGN_4

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

	leal		(CO1, LDC, 2), %eax

	movaps	-16 * SIZE(AA), %xmm0
	pxor	%xmm2, %xmm2
	movaps	-16 * SIZE(BB), %xmm1
	pxor	%xmm3, %xmm3

#ifdef LN
	pxor	%xmm4, %xmm4
	prefetcht0	-2 * SIZE(CO1)
	pxor	%xmm5, %xmm5
	prefetcht0	-2 * SIZE(CO1, LDC)
	pxor	%xmm6, %xmm6
	prefetcht0	-2 * SIZE(%eax)
	pxor	%xmm7, %xmm7
	prefetcht0	-2 * SIZE(%eax, LDC)
#else
	pxor	%xmm4, %xmm4
	prefetcht0	1 * SIZE(CO1)
	pxor	%xmm5, %xmm5
	prefetcht0	1 * SIZE(CO1, LDC)
	pxor	%xmm6, %xmm6
	prefetcht0	1 * SIZE(%eax)
	pxor	%xmm7, %xmm7
	prefetcht0	1 * SIZE(%eax, LDC)
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
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	addpd	%xmm3, %xmm7
	movaps	-14 * SIZE(BB), %xmm3
	addpd	%xmm2, %xmm6
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm1, %xmm5
	movaps	-12 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4
	pshufd	$0x4e, %xmm3, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm2
	movaps	-14 * SIZE(AA), %xmm0

	addpd	%xmm3, %xmm7
	movaps	-10 * SIZE(BB), %xmm3
	addpd	%xmm2, %xmm6
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm1, %xmm5
	movaps	 -8 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4
	pshufd	$0x4e, %xmm3, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm2
	movaps	-12 * SIZE(AA), %xmm0

	addpd	%xmm3, %xmm7
	movaps	 -6 * SIZE(BB), %xmm3
	addpd	%xmm2, %xmm6
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm1, %xmm5
	movaps	 -4 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4
	pshufd	$0x4e, %xmm3, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm2
	movaps	-10 * SIZE(AA), %xmm0

	addpd	%xmm3, %xmm7
	movaps	 -2 * SIZE(BB), %xmm3
	addpd	%xmm2, %xmm6
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm1, %xmm5
	movaps	  0 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4
	pshufd	$0x4e, %xmm3, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm2
	movaps	 -8 * SIZE(AA), %xmm0

	PREFETCH (PREFETCHSIZE +  8) * SIZE(AA)

	addpd	%xmm3, %xmm7
	movaps	  2 * SIZE(BB), %xmm3
	addpd	%xmm2, %xmm6
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm1, %xmm5
	movaps	  4 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4
	pshufd	$0x4e, %xmm3, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm2
	movaps	 -6 * SIZE(AA), %xmm0

	addpd	%xmm3, %xmm7
	movaps	  6 * SIZE(BB), %xmm3
	addpd	%xmm2, %xmm6
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm1, %xmm5
	movaps	  8 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4
	pshufd	$0x4e, %xmm3, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm2
	movaps	 -4 * SIZE(AA), %xmm0

	addpd	%xmm3, %xmm7
	movaps	 10 * SIZE(BB), %xmm3
	addpd	%xmm2, %xmm6
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm1, %xmm5
	movaps	 12 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4
	pshufd	$0x4e, %xmm3, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm2
	movaps	 -2 * SIZE(AA), %xmm0

	addpd	%xmm3, %xmm7
	movaps	 14 * SIZE(BB), %xmm3
	addpd	%xmm2, %xmm6
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm1, %xmm5
	movaps	 16 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4
	pshufd	$0x4e, %xmm3, %xmm2
	mulpd	%xmm0, %xmm3
	subl   $-32 * SIZE, BB
	mulpd	%xmm0, %xmm2
	movaps	  0 * SIZE(AA), %xmm0

	subl   $-16 * SIZE, AA

	subl   $1, %eax
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
	addpd	%xmm3, %xmm7
	movaps	-14 * SIZE(BB), %xmm3
	addpd	%xmm2, %xmm6
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	addpd	%xmm1, %xmm5
	movaps	-12 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4
	pshufd	$0x4e, %xmm3, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm2

	movaps	-14 * SIZE(AA), %xmm0

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

	addpd	%xmm2, %xmm6
	addpd	%xmm3, %xmm7

	movaps	 %xmm4, %xmm0
	movsd    %xmm5, %xmm4
	movsd    %xmm0, %xmm5

	movaps	 %xmm6, %xmm0
	movsd    %xmm7, %xmm6
	movsd    %xmm0, %xmm7

#if defined(LN) || defined(LT)
	movapd	 %xmm4, %xmm0
	unpcklpd %xmm5, %xmm4
	unpckhpd %xmm5, %xmm0

	movapd	 %xmm6, %xmm1
	unpcklpd %xmm7, %xmm6
	unpckhpd %xmm7, %xmm1

	movapd	-16 * SIZE(BB), %xmm2
	movapd	-14 * SIZE(BB), %xmm5
	movapd	-12 * SIZE(BB), %xmm3
	movapd	-10 * SIZE(BB), %xmm7

	subpd	%xmm4,  %xmm2
	subpd	%xmm6,  %xmm5
	subpd	%xmm0,  %xmm3
	subpd	%xmm1,  %xmm7
#else
	movapd	-16 * SIZE(AA), %xmm0
	movapd	-14 * SIZE(AA), %xmm1
	movapd	-12 * SIZE(AA), %xmm2
	movapd	-10 * SIZE(AA), %xmm3

	subpd	%xmm4, %xmm0
	subpd	%xmm5, %xmm1
	subpd	%xmm6, %xmm2
	subpd	%xmm7, %xmm3
#endif

#ifdef LN
	movddup	-13 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm3
	mulpd	 %xmm4, %xmm7

	movddup	-14 * SIZE(AA), %xmm4
	movapd	 %xmm4, %xmm6
	mulpd	 %xmm3, %xmm4
	subpd	 %xmm4, %xmm2
	mulpd	 %xmm7, %xmm6
	subpd	 %xmm6, %xmm5

	movddup	 -16 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm2
	mulpd	 %xmm4, %xmm5

#endif

#ifdef LT
	movddup	 -16 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm2
	mulpd	 %xmm4, %xmm5

	movddup	 -15 * SIZE(AA), %xmm4
	movapd	 %xmm4, %xmm6
	mulpd	 %xmm2, %xmm4
	subpd	 %xmm4, %xmm3
	mulpd	 %xmm5, %xmm6
	subpd	 %xmm6, %xmm7

	movddup	 -13 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm3
	mulpd	 %xmm4, %xmm7
#endif

#ifdef RN
	movddup	 -16 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm0
	movddup	 -15 * SIZE(BB), %xmm4
	mulpd	 %xmm0, %xmm4
	subpd	 %xmm4, %xmm1
	movddup	 -14 * SIZE(BB), %xmm4
	mulpd	 %xmm0, %xmm4
	subpd	 %xmm4, %xmm2
	movddup	 -13 * SIZE(BB), %xmm4
	mulpd	 %xmm0, %xmm4
	subpd	 %xmm4, %xmm3

	movddup	 -11 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm1
	movddup	 -10 * SIZE(BB), %xmm4
	mulpd	 %xmm1, %xmm4
	subpd	 %xmm4, %xmm2
	movddup	  -9 * SIZE(BB), %xmm4
	mulpd	 %xmm1, %xmm4
	subpd	 %xmm4, %xmm3

	movddup	 -6 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm2
	movddup	 -5 * SIZE(BB), %xmm4
	mulpd	 %xmm2, %xmm4
	subpd	 %xmm4, %xmm3

	movddup	 -1 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm3
#endif

#ifdef RT
	movddup	-1 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm3
	movddup	-2 * SIZE(BB), %xmm4
	mulpd	 %xmm3, %xmm4
	subpd	 %xmm4, %xmm2
	movddup	-3 * SIZE(BB), %xmm4
	mulpd	 %xmm3, %xmm4
	subpd	 %xmm4, %xmm1
	movddup	-4 * SIZE(BB), %xmm4
	mulpd	 %xmm3, %xmm4
	subpd	 %xmm4, %xmm0

	movddup	-6 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm2
	movddup	-7 * SIZE(BB), %xmm4
	mulpd	 %xmm2, %xmm4
	subpd	 %xmm4, %xmm1
	movddup	-8 * SIZE(BB), %xmm4
	mulpd	 %xmm2, %xmm4
	subpd	 %xmm4, %xmm0

	movddup	-11 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm1
	movddup	-12 * SIZE(BB), %xmm4
	mulpd	 %xmm1, %xmm4
	subpd	 %xmm4, %xmm0

	movddup	 -16 * SIZE(BB), %xmm4
	mulpd	 %xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm2,   -16 * SIZE(BB)
	movapd	%xmm5,   -14 * SIZE(BB)
	movapd	%xmm3,   -12 * SIZE(BB)
	movapd	%xmm7,   -10 * SIZE(BB)
#else
	movapd	%xmm0,   -16 * SIZE(AA)
	movapd	%xmm1,   -14 * SIZE(AA)
	movapd	%xmm2,   -12 * SIZE(AA)
	movapd	%xmm3,   -10 * SIZE(AA)
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

	leal	(LDC, LDC, 2), %eax

#if defined(LN) || defined(LT)
	movsd	%xmm2, 0 * SIZE(CO1)
	movsd	%xmm3, 1 * SIZE(CO1)
	movhps	%xmm2, 0 * SIZE(CO1, LDC, 1)
	movhps	%xmm3, 1 * SIZE(CO1, LDC, 1)
	movsd	%xmm5, 0 * SIZE(CO1, LDC, 2)
	movsd	%xmm7, 1 * SIZE(CO1, LDC, 2)
	movhps	%xmm5, 0 * SIZE(CO1, %eax, 1)
	movhps	%xmm7, 1 * SIZE(CO1, %eax, 1)
#else
	movsd	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 1 * SIZE(CO1)
	movsd	%xmm1, 0 * SIZE(CO1, LDC, 1)
	movhps	%xmm1, 1 * SIZE(CO1, LDC, 1)
	movsd	%xmm2, 0 * SIZE(CO1, LDC, 2)
	movhps	%xmm2, 1 * SIZE(CO1, LDC, 2)
	movsd	%xmm3, 0 * SIZE(CO1, %eax, 1)
	movhps	%xmm3, 1 * SIZE(CO1, %eax, 1)
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

	movaps	-16 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movaps	-16 * SIZE(BB), %xmm2
	pxor	%xmm5, %xmm5
	movaps	-14 * SIZE(BB), %xmm3
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L25
	ALIGN_4

.L22:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	pshufd	$0x44, %xmm0, %xmm1
	mulpd	%xmm1, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm2, %xmm4
	movaps	-12 * SIZE(BB), %xmm2
	addpd	%xmm3, %xmm5
	movaps	-10 * SIZE(BB), %xmm3

	pshufd	$0xee, %xmm0, %xmm1
	movaps	-14 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm2, %xmm6
	movaps	 -8 * SIZE(BB), %xmm2
	addpd	%xmm3, %xmm7
	movaps	 -6 * SIZE(BB), %xmm3

	pshufd	$0x44, %xmm0, %xmm1
	mulpd	%xmm1, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm2, %xmm4
	movaps	 -4 * SIZE(BB), %xmm2
	addpd	%xmm3, %xmm5
	movaps	 -2 * SIZE(BB), %xmm3

	pshufd	$0xee, %xmm0, %xmm1
	movaps	-12 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm2, %xmm6
	movaps	  0 * SIZE(BB), %xmm2
	addpd	%xmm3, %xmm7
	movaps	  2 * SIZE(BB), %xmm3

	pshufd	$0x44, %xmm0, %xmm1
	mulpd	%xmm1, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm2, %xmm4
	movaps	  4 * SIZE(BB), %xmm2
	addpd	%xmm3, %xmm5
	movaps	  6 * SIZE(BB), %xmm3

	pshufd	$0xee, %xmm0, %xmm1
	movaps	-10 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm2, %xmm6
	movaps	  8 * SIZE(BB), %xmm2
	addpd	%xmm3, %xmm7
	movaps	 10 * SIZE(BB), %xmm3

	pshufd	$0x44, %xmm0, %xmm1
	mulpd	%xmm1, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm2, %xmm4
	movaps	 12 * SIZE(BB), %xmm2
	addpd	%xmm3, %xmm5
	movaps	 14 * SIZE(BB), %xmm3

	pshufd	$0xee, %xmm0, %xmm1
	movaps	 -8 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm2, %xmm6
	movaps	 16 * SIZE(BB), %xmm2
	addpd	%xmm3, %xmm7
	movaps	 18 * SIZE(BB), %xmm3

	subl   $ -8 * SIZE, AA
	subl   $-32 * SIZE, BB

	subl   $1, %eax
	jne    .L22
	ALIGN_4

.L25:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax
	BRANCH
	je .L28

.L26:
	pshufd	$0x44, %xmm0, %xmm1
	movsd	-15 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm2, %xmm4
	movaps	-12 * SIZE(BB), %xmm2
	addpd	%xmm3, %xmm5
	movaps	-10 * SIZE(BB), %xmm3

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
	movapd	-16 * SIZE(BB), %xmm0
	movapd	-14 * SIZE(BB), %xmm1

	subpd	%xmm4,  %xmm0
	subpd	%xmm5,  %xmm1
#else
	movapd	-16 * SIZE(AA), %xmm1
	movapd	-14 * SIZE(AA), %xmm3

	subpd	%xmm4, %xmm1
	subpd	%xmm5, %xmm3

	movapd	       %xmm1, %xmm0
	unpckhpd       %xmm1, %xmm1
	movapd	       %xmm3, %xmm2
	unpckhpd       %xmm3, %xmm3
#endif

#ifdef LN
	movddup	-16 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm0
	mulpd	 %xmm4, %xmm1
#endif

#ifdef LT
	movddup	-16 * SIZE(AA), %xmm4
	mulpd	 %xmm4, %xmm0
	mulpd	 %xmm4, %xmm1
#endif

#ifdef RN
	movsd	-16 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm0
	movsd	-15 * SIZE(BB), %xmm4
	mulsd	 %xmm0, %xmm4
	subsd	 %xmm4, %xmm1
	movsd	-14 * SIZE(BB), %xmm4
	mulsd	 %xmm0, %xmm4
	subsd	 %xmm4, %xmm2
	movsd	-13 * SIZE(BB), %xmm4
	mulsd	 %xmm0, %xmm4
	subsd	 %xmm4, %xmm3

	movsd	-11 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm1
	movsd	-10 * SIZE(BB), %xmm4
	mulsd	 %xmm1, %xmm4
	subsd	 %xmm4, %xmm2
	movsd	 -9 * SIZE(BB), %xmm4
	mulsd	 %xmm1, %xmm4
	subsd	 %xmm4, %xmm3

	movsd	 -6 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm2
	movsd	 -5 * SIZE(BB), %xmm4
	mulsd	 %xmm2, %xmm4
	subsd	 %xmm4, %xmm3

	movsd	 -1 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm3
#endif

#ifdef RT
	movsd	 -1 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm3
	movsd	 -2 * SIZE(BB), %xmm4
	mulsd	 %xmm3, %xmm4
	subsd	 %xmm4, %xmm2
	movsd	 -3 * SIZE(BB), %xmm4
	mulsd	 %xmm3, %xmm4
	subsd	 %xmm4, %xmm1
	movsd	 -4 * SIZE(BB), %xmm4
	mulsd	 %xmm3, %xmm4
	subsd	 %xmm4, %xmm0

	movsd	 -6 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm2
	movsd	 -7 * SIZE(BB), %xmm4
	mulsd	 %xmm2, %xmm4
	subsd	 %xmm4, %xmm1
	movsd	 -8 * SIZE(BB), %xmm4
	mulsd	 %xmm2, %xmm4
	subsd	 %xmm4, %xmm0

	movsd	 -11 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm1
	movsd	 -12 * SIZE(BB), %xmm4
	mulsd	 %xmm1, %xmm4
	subsd	 %xmm4, %xmm0

	movsd	 -16 * SIZE(BB), %xmm4
	mulsd	 %xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm0,  -16 * SIZE(BB)
	movapd	%xmm1,  -14 * SIZE(BB)
#else
	movsd	%xmm0,  -16 * SIZE(AA)
	movsd	%xmm1,  -15 * SIZE(AA)
	movsd	%xmm2,  -14 * SIZE(AA)
	movsd	%xmm3,  -13 * SIZE(AA)
#endif

#ifdef LN
	subl	$1 * SIZE, CO1
#endif

	leal	(LDC, LDC, 2), %eax

#if defined(LN) || defined(LT)
	movsd	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 0 * SIZE(CO1, LDC, 1)
	movsd	%xmm1, 0 * SIZE(CO1, LDC, 2)
	movhps	%xmm1, 0 * SIZE(CO1, %eax, 1)
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
