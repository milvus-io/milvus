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
#define PREFETCH	prefetcht1
#define PREFETCHSIZE 84
#endif

#if defined(NEHALEM) || defined(SANDYBRIDGE) || defined(HASWELL) || defined (SKYLAKEX)
#define PREFETCH	prefetcht1
#define PREFETCHSIZE 84
#endif

#ifdef ATOM
#define PREFETCH	prefetcht0
#define PREFETCHSIZE 84
#endif

#ifdef NANO
#define PREFETCH	prefetcht0
#define PREFETCHSIZE (16 * 2)
#endif

#define B	%edi
#define	LDC	%ebp
#define AA	%edx
#define BB	%ecx
#define CO1	%esi

#define ADD1	  addps
#define ADD2	  addps

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

	movl	M,    %ebx
	testl	%ebx, %ebx
	jle	.L999

	subl	$-32 * SIZE, A
	subl	$-32 * SIZE, B

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
	movl	%eax, J
	sarl	$1, J
	jle	.L100
	ALIGN_4

.L01:
#if defined(LT) || defined(RN)
	movl	A, %eax
	movl	%eax, AA
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

#ifdef RT
	movl	K, %eax
	sall	$1 + ZBASE_SHIFT, %eax
	subl	%eax, B
#endif

       leal	(, LDC, 2), %eax

#ifdef RT
       subl	%eax, C
#endif
	movl	C,  CO1
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
	andl	$1, %ebx
	jle	.L30

#ifdef LN
       movl	K, %eax
       sall	$ZBASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AA
#endif

	movl	B, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + ZBASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movsd	-32 * SIZE(AA), %xmm0
	pxor	%xmm2, %xmm2
	movaps	-32 * SIZE(BB), %xmm1
	pxor	%xmm3, %xmm3

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L42
	ALIGN_4

.L41:
	addps	%xmm2, %xmm6
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm7
	pshufd	$0x55, %xmm1, %xmm3
	mulps	%xmm0, %xmm3

	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	addps	%xmm2, %xmm4
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0xff, %xmm1, %xmm3
	movaps	-28 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movsd	-30 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm6
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm7
	pshufd	$0x55, %xmm1, %xmm3
	mulps	%xmm0, %xmm3

	addps	%xmm2, %xmm4
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0xff, %xmm1, %xmm3
	movaps	-24 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movsd	-28 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm6
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm7
	pshufd	$0x55, %xmm1, %xmm3
	mulps	%xmm0, %xmm3

	addps	%xmm2, %xmm4
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0xff, %xmm1, %xmm3
	movaps	-20 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movsd	-26 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm6
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm7
	pshufd	$0x55, %xmm1, %xmm3
	mulps	%xmm0, %xmm3

	addps	%xmm2, %xmm4
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0xff, %xmm1, %xmm3
	movaps	-16 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movsd	-24 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm6
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm7
	pshufd	$0x55, %xmm1, %xmm3
	mulps	%xmm0, %xmm3

	addps	%xmm2, %xmm4
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0xff, %xmm1, %xmm3
	movaps	-12 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movsd	-22 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm6
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm7
	pshufd	$0x55, %xmm1, %xmm3
	mulps	%xmm0, %xmm3

	addps	%xmm2, %xmm4
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0xff, %xmm1, %xmm3
	movaps	 -8 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movsd	-20 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm6
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm7
	pshufd	$0x55, %xmm1, %xmm3
	mulps	%xmm0, %xmm3

	addps	%xmm2, %xmm4
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0xff, %xmm1, %xmm3
	movaps	 -4 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movsd	-18 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm6
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm7
	pshufd	$0x55, %xmm1, %xmm3
	mulps	%xmm0, %xmm3

	addps	%xmm2, %xmm4
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0xff, %xmm1, %xmm3
	movaps	  0 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movsd	-16 * SIZE(AA), %xmm0

	subl   $-16 * SIZE, AA
	subl   $-32 * SIZE, BB
	decl	%eax
	jne	.L41
	ALIGN_4

.L42:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L44
	ALIGN_4

.L43:
	addps	%xmm2, %xmm6
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm7
	pshufd	$0x55, %xmm1, %xmm3
	mulps	%xmm0, %xmm3

	addps	%xmm2, %xmm4
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0xff, %xmm1, %xmm3
	movaps	-28 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movsd	-30 * SIZE(AA), %xmm0

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L43
	ALIGN_4

.L44:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$2, %eax
#endif

	movl	AORIG, AA
	sall	$ZBASE_SHIFT, %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 2), BB
#endif

	addps	%xmm2, %xmm6
	addps	%xmm3, %xmm7

	pshufd	 $0xb1, %xmm5, %xmm5
	pcmpeqb	%xmm0, %xmm0
	pshufd	 $0xb1, %xmm7, %xmm7
	psllq	$63,   %xmm0

#ifndef CONJ
	shufps	$0xb1, %xmm0, %xmm0

	pxor	%xmm0, %xmm5
	pxor	%xmm0, %xmm7
#else
#if defined(LN) || defined(LT)
	pxor	%xmm0, %xmm4
	pxor	%xmm0, %xmm6
#else
	pxor	%xmm0, %xmm5
	pxor	%xmm0, %xmm7
#endif
#endif

	addps	%xmm5, %xmm4
	addps	%xmm7, %xmm6

#if defined(LN) || defined(LT)
	unpcklpd %xmm6, %xmm4

	movaps	-32 * SIZE(BB), %xmm2

	subps	%xmm4,  %xmm2
#else
	movsd	-32 * SIZE(AA), %xmm1
	movsd	-30 * SIZE(AA), %xmm5

	subps	%xmm4,  %xmm1
	subps	%xmm6,  %xmm5
#endif

#if defined(LN) || defined(LT)
	movaps	-32 * SIZE(AA), %xmm5

	pshufd	 $0x44, %xmm5, %xmm6
	pshufd	 $0x11, %xmm5, %xmm7

	pshufd	 $0xa0, %xmm2, %xmm4
	pshufd	 $0xf5, %xmm2, %xmm2

#ifndef CONJ
	xorps	 %xmm0, %xmm2
#else
	xorps	 %xmm0, %xmm4
#endif

	mulps	%xmm6,  %xmm4
	mulps	%xmm7,  %xmm2
	addps	%xmm4,  %xmm2
#endif

#ifdef RN
	movaps	-32 * SIZE(BB), %xmm4

	pshufd	 $0x44, %xmm4, %xmm6
	pshufd	 $0x11, %xmm4, %xmm7

	pshufd	 $0xa0, %xmm1, %xmm3
	pshufd	 $0xf5, %xmm1, %xmm1

#ifndef CONJ
	xorps	 %xmm0, %xmm1
#else
	xorps	 %xmm0, %xmm3
#endif

	mulps	%xmm6,  %xmm3
	mulps	%xmm7,  %xmm1

	addps	%xmm3,  %xmm1

	pshufd	 $0xee, %xmm4, %xmm6
	pshufd	 $0xbb, %xmm4, %xmm7

	pshufd	 $0xa0, %xmm1, %xmm3
	pshufd	 $0xf5, %xmm1, %xmm2

#ifndef CONJ
	xorps	 %xmm0, %xmm2
#else
	xorps	 %xmm0, %xmm3
#endif

	mulps	%xmm6,  %xmm3
	mulps	%xmm7,  %xmm2

	subps	%xmm3,  %xmm5
	subps	%xmm2,  %xmm5

	movaps	-28 * SIZE(BB), %xmm4

	pshufd	 $0xee, %xmm4, %xmm6
	pshufd	 $0xbb, %xmm4, %xmm7

	pshufd	 $0xa0, %xmm5, %xmm3
	pshufd	 $0xf5, %xmm5, %xmm5

#ifndef CONJ
	xorps	 %xmm0, %xmm5
#else
	xorps	 %xmm0, %xmm3
#endif

	mulps	%xmm6,  %xmm3
	mulps	%xmm7,  %xmm5

	addps	%xmm3,  %xmm5
#endif

#ifdef RT
	movaps	-28 * SIZE(BB), %xmm4

	pshufd	 $0xee, %xmm4, %xmm6
	pshufd	 $0xbb, %xmm4, %xmm7

	pshufd	 $0xa0, %xmm5, %xmm3
	pshufd	 $0xf5, %xmm5, %xmm5

#ifndef CONJ
	xorps	 %xmm0, %xmm5
#else
	xorps	 %xmm0, %xmm3
#endif

	mulps	%xmm6,  %xmm3
	mulps	%xmm7,  %xmm5

	addps	%xmm3,  %xmm5

	pshufd	 $0x44, %xmm4, %xmm6
	pshufd	 $0x11, %xmm4, %xmm7

	pshufd	 $0xa0, %xmm5, %xmm3
	pshufd	 $0xf5, %xmm5, %xmm2

#ifndef CONJ
	xorps	 %xmm0, %xmm2
#else
	xorps	 %xmm0, %xmm3
#endif

	mulps	%xmm6,  %xmm3
	mulps	%xmm7,  %xmm2

	subps	%xmm3,  %xmm1
	subps	%xmm2,  %xmm1

	movaps	-32 * SIZE(BB), %xmm4

	pshufd	 $0x44, %xmm4, %xmm6
	pshufd	 $0x11, %xmm4, %xmm7

	pshufd	 $0xa0, %xmm1, %xmm3
	pshufd	 $0xf5, %xmm1, %xmm1

#ifndef CONJ
	xorps	 %xmm0, %xmm1
#else
	xorps	 %xmm0, %xmm3
#endif

	mulps	%xmm6,  %xmm3
	mulps	%xmm7,  %xmm1

	addps	%xmm3,  %xmm1
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm2, -32 * SIZE(BB)

	movlps	%xmm2,   0 * SIZE(CO1)
	movhps	%xmm2,   0 * SIZE(CO1, LDC)
#else
	movlps	%xmm1, -32 * SIZE(AA)
	movlps	%xmm5, -30 * SIZE(AA)

	movlps	%xmm1,   0 * SIZE(CO1)
	movlps	%xmm5,   0 * SIZE(CO1, LDC)
#endif

#ifndef LN
	addl	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
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
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AORIG
#endif
	ALIGN_4

.L30:
	movl	M,  %ebx
	sarl	$1, %ebx
	jle	.L99
	ALIGN_4

.L10:
#ifdef LN
       movl	K, %eax
       sall	$1 + ZBASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	sall	$1 + ZBASE_SHIFT, %eax
	addl	%eax, AA
#endif

	movl	B, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + ZBASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movaps	-32 * SIZE(AA), %xmm0
	pxor	%xmm2, %xmm2
	movaps	-32 * SIZE(BB), %xmm1
	pxor	%xmm3, %xmm3

#ifdef LN
	pxor	%xmm4, %xmm4
	prefetcht0     -4 * SIZE(CO1)
	pxor	%xmm5, %xmm5
	prefetcht0     -4 * SIZE(CO1, LDC)
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7
#else
	pxor	%xmm4, %xmm4
	prefetcht0	3 * SIZE(CO1)
	pxor	%xmm5, %xmm5
	prefetcht0	3 * SIZE(CO1, LDC)
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7
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

.L11:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	ADD2	%xmm2, %xmm7
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	ADD1	%xmm3, %xmm6
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	ADD2	%xmm2, %xmm5
	pshufd	$0xb1, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	ADD1	%xmm1, %xmm4
	movaps	-28 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(AA), %xmm0

	ADD2	%xmm2, %xmm7
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	ADD1	%xmm3, %xmm6
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	ADD2	%xmm2, %xmm5
	pshufd	$0xb1, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	ADD1	%xmm1, %xmm4
	movaps	-24 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-24 * SIZE(AA), %xmm0

	ADD2	%xmm2, %xmm7
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	ADD1	%xmm3, %xmm6
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	ADD2	%xmm2, %xmm5
	pshufd	$0xb1, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	ADD1	%xmm1, %xmm4
	movaps	-20 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-20 * SIZE(AA), %xmm0

	ADD2	%xmm2, %xmm7
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	ADD1	%xmm3, %xmm6
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	ADD2	%xmm2, %xmm5
	pshufd	$0xb1, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	ADD1	%xmm1, %xmm4
	movaps	-16 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-16 * SIZE(AA), %xmm0

	PREFETCH (PREFETCHSIZE + 16) * SIZE(AA)

	ADD2	%xmm2, %xmm7
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	ADD1	%xmm3, %xmm6
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	ADD2	%xmm2, %xmm5
	pshufd	$0xb1, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	ADD1	%xmm1, %xmm4
	movaps	-12 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-12 * SIZE(AA), %xmm0

	ADD2	%xmm2, %xmm7
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	ADD1	%xmm3, %xmm6
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	ADD2	%xmm2, %xmm5
	pshufd	$0xb1, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	ADD1	%xmm1, %xmm4
	movaps	 -8 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	 -8 * SIZE(AA), %xmm0

	ADD2	%xmm2, %xmm7
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	ADD1	%xmm3, %xmm6
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	ADD2	%xmm2, %xmm5
	pshufd	$0xb1, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	ADD1	%xmm1, %xmm4
	movaps	 -4 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	 -4 * SIZE(AA), %xmm0

	ADD2	%xmm2, %xmm7
	subl   $-32 * SIZE, BB
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	ADD1	%xmm3, %xmm6
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	ADD2	%xmm2, %xmm5
	subl   $-32 * SIZE, AA
	pshufd	$0xb1, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	ADD1	%xmm1, %xmm4
	movaps	-32 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-32 * SIZE(AA), %xmm0

	decl   %eax
	jne    .L11
	ALIGN_4

.L15:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L14
	ALIGN_4

.L13:
	ADD2	%xmm2, %xmm7
	pshufd	$0xb1, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	ADD1	%xmm3, %xmm6
	pshufd	$0x1b, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	ADD2	%xmm2, %xmm5
	pshufd	$0xb1, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	ADD1	%xmm1, %xmm4
	movaps	-28 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(AA), %xmm0

	addl	$4 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L13
	ALIGN_4

.L14:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$2, %eax
#else
	subl	$2, %eax
#endif

	movl	AORIG, AA
	sall	$ZBASE_SHIFT, %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 2), BB
#endif

	ADD2	%xmm2, %xmm7
	pcmpeqb	%xmm0, %xmm0
	ADD1	%xmm3, %xmm6
	psllq	$63,   %xmm0

#ifndef CONJ
	pxor	%xmm0, %xmm4
	pxor	%xmm0, %xmm6

	shufps	$0xb1, %xmm0, %xmm0
#else
#if defined(LN) || defined(LT)
	pxor	%xmm0, %xmm5
	pxor	%xmm0, %xmm7
#else
	pshufd	$0xb1, %xmm0, %xmm1

	pxor	%xmm1, %xmm5
	pxor	%xmm1, %xmm7
#endif
#endif

	haddps	%xmm5, %xmm4
	haddps	%xmm7, %xmm6

	shufps	$0xd8, %xmm4, %xmm4
	shufps	$0xd8, %xmm6, %xmm6

	movaps	%xmm4, %xmm5
	shufps	$0xe4, %xmm6, %xmm4
	shufps	$0xe4, %xmm5, %xmm6

#if defined(LN) || defined(LT)
	movaps	%xmm4,  %xmm5
	unpcklpd %xmm6, %xmm4
	unpckhpd %xmm6, %xmm5

	movaps	-32 * SIZE(BB), %xmm2
	movaps	-28 * SIZE(BB), %xmm3

	subps	%xmm4,  %xmm2
	subps	%xmm5,  %xmm3
#else
	movaps	-32 * SIZE(AA), %xmm1
	movaps	-28 * SIZE(AA), %xmm5

	subps	%xmm4,  %xmm1
	subps	%xmm6,  %xmm5
#endif

#ifdef LN
	movaps	-28 * SIZE(AA), %xmm5

	pshufd	 $0xee, %xmm5, %xmm6
	pshufd	 $0xbb, %xmm5, %xmm7

	pshufd	 $0xa0, %xmm3, %xmm4
	pshufd	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 %xmm0, %xmm3
#else
	xorps	 %xmm0, %xmm4
#endif

	mulps	%xmm6,  %xmm4
	mulps	%xmm7, %xmm3
	addps	%xmm4,  %xmm3

	pshufd	 $0x44, %xmm5, %xmm6
	pshufd	 $0x11, %xmm5, %xmm7

	pshufd	 $0xa0, %xmm3, %xmm4
	pshufd	 $0xf5, %xmm3, %xmm1

#ifndef CONJ
	xorps	 %xmm0, %xmm1
#else
	xorps	 %xmm0, %xmm4
#endif

	mulps	%xmm6,  %xmm4
	mulps	%xmm7, %xmm1
	subps	%xmm4,  %xmm2
	subps	%xmm1,  %xmm2

	movaps	-32 * SIZE(AA), %xmm5

	pshufd	 $0x44, %xmm5, %xmm6
	pshufd	 $0x11, %xmm5, %xmm7

	pshufd	 $0xa0, %xmm2, %xmm4
	pshufd	 $0xf5, %xmm2, %xmm2

#ifndef CONJ
	xorps	 %xmm0, %xmm2
#else
	xorps	 %xmm0, %xmm4
#endif

	mulps	%xmm6,  %xmm4
	mulps	%xmm7, %xmm2
	addps	%xmm4,  %xmm2
#endif

#ifdef LT
	movaps	-32 * SIZE(AA), %xmm5

	pshufd	 $0x44, %xmm5, %xmm6
	pshufd	 $0x11, %xmm5, %xmm7

	pshufd	 $0xa0, %xmm2, %xmm4
	pshufd	 $0xf5, %xmm2, %xmm2

#ifndef CONJ
	xorps	 %xmm0, %xmm2
#else
	xorps	 %xmm0, %xmm4
#endif

	mulps	%xmm6,  %xmm4
	mulps	%xmm7, %xmm2
	addps	%xmm4,  %xmm2

	pshufd	 $0xee, %xmm5, %xmm6
	pshufd	 $0xbb, %xmm5, %xmm7

	pshufd	 $0xa0, %xmm2, %xmm4
	pshufd	 $0xf5, %xmm2, %xmm1

#ifndef CONJ
	xorps	 %xmm0, %xmm1
#else
	xorps	 %xmm0, %xmm4
#endif

	mulps	%xmm6,  %xmm4
	mulps	%xmm7, %xmm1
	subps	%xmm4,  %xmm3
	subps	%xmm1,  %xmm3

	movaps	-28 * SIZE(AA), %xmm5

	pshufd	 $0xee, %xmm5, %xmm6
	pshufd	 $0xbb, %xmm5, %xmm7

	pshufd	 $0xa0, %xmm3, %xmm4
	pshufd	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 %xmm0, %xmm3
#else
	xorps	 %xmm0, %xmm4
#endif

	mulps	%xmm6,  %xmm4
	mulps	%xmm7,  %xmm3
	addps	%xmm4,  %xmm3
#endif

#ifdef RN
	movaps	-32 * SIZE(BB), %xmm4

	pshufd	 $0x44, %xmm4, %xmm6
	pshufd	 $0x11, %xmm4, %xmm7

	pshufd	 $0xa0, %xmm1, %xmm3
	pshufd	 $0xf5, %xmm1, %xmm1

#ifndef CONJ
	xorps	 %xmm0, %xmm1
#else
	xorps	 %xmm0, %xmm3
#endif

	mulps	%xmm6,  %xmm3
	mulps	%xmm7,  %xmm1

	addps	%xmm3,  %xmm1

	pshufd	 $0xee, %xmm4, %xmm6
	pshufd	 $0xbb, %xmm4, %xmm7

	pshufd	 $0xa0, %xmm1, %xmm3
	pshufd	 $0xf5, %xmm1, %xmm2

#ifndef CONJ
	xorps	 %xmm0, %xmm2
#else
	xorps	 %xmm0, %xmm3
#endif

	mulps	%xmm6,  %xmm3
	mulps	%xmm7,  %xmm2

	subps	%xmm3,  %xmm5
	subps	%xmm2,  %xmm5

	movaps	-28 * SIZE(BB), %xmm4

	pshufd	 $0xee, %xmm4, %xmm6
	pshufd	 $0xbb, %xmm4, %xmm7

	pshufd	 $0xa0, %xmm5, %xmm3
	pshufd	 $0xf5, %xmm5, %xmm5

#ifndef CONJ
	xorps	 %xmm0, %xmm5
#else
	xorps	 %xmm0, %xmm3
#endif

	mulps	%xmm6,  %xmm3
	mulps	%xmm7,  %xmm5

	addps	%xmm3,  %xmm5
#endif

#ifdef RT
	movaps	-28 * SIZE(BB), %xmm4

	pshufd	 $0xee, %xmm4, %xmm6
	pshufd	 $0xbb, %xmm4, %xmm7

	pshufd	 $0xa0, %xmm5, %xmm3
	pshufd	 $0xf5, %xmm5, %xmm5

#ifndef CONJ
	xorps	 %xmm0, %xmm5
#else
	xorps	 %xmm0, %xmm3
#endif

	mulps	%xmm6,  %xmm3
	mulps	%xmm7,  %xmm5

	addps	%xmm3,  %xmm5

	pshufd	 $0x44, %xmm4, %xmm6
	pshufd	 $0x11, %xmm4, %xmm7

	pshufd	 $0xa0, %xmm5, %xmm3
	pshufd	 $0xf5, %xmm5, %xmm2

#ifndef CONJ
	xorps	 %xmm0, %xmm2
#else
	xorps	 %xmm0, %xmm3
#endif

	mulps	%xmm6,  %xmm3
	mulps	%xmm7,  %xmm2

	subps	%xmm3,  %xmm1
	subps	%xmm2,  %xmm1

	movaps	-32 * SIZE(BB), %xmm4

	pshufd	 $0x44, %xmm4, %xmm6
	pshufd	 $0x11, %xmm4, %xmm7

	pshufd	 $0xa0, %xmm1, %xmm3
	pshufd	 $0xf5, %xmm1, %xmm1

#ifndef CONJ
	xorps	 %xmm0, %xmm1
#else
	xorps	 %xmm0, %xmm3
#endif

	mulps	%xmm6,  %xmm3
	mulps	%xmm7,  %xmm1

	addps	%xmm3,  %xmm1
#endif

#ifdef LN
	subl	$4 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm2,  -32 * SIZE(BB)
	movaps	%xmm3,  -28 * SIZE(BB)

	movlps	%xmm2,   0 * SIZE(CO1)
	movlps	%xmm3,   2 * SIZE(CO1)
	movhps	%xmm2,   0 * SIZE(CO1, LDC)
	movhps	%xmm3,   2 * SIZE(CO1, LDC)
#else
	movaps	%xmm1,  -32 * SIZE(AA)
	movaps	%xmm5,  -28 * SIZE(AA)

	movlps	%xmm1,   0 * SIZE(CO1)
	movhps	%xmm1,   2 * SIZE(CO1)

	movlps	%xmm5,   0 * SIZE(CO1, LDC)
	movhps	%xmm5,   2 * SIZE(CO1, LDC)
#endif

#ifndef LN
	addl	$4 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
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
	sall	$1 + ZBASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx
	jg	.L10
	ALIGN_4

.L99:
#ifdef LN
       movl	K, %eax
       sall	$1 + ZBASE_SHIFT, %eax
       addl	%eax, B
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

	decl	J			# j --
	jg	.L01
	ALIGN_4

.L100:
	movl	N, %eax
	andl	$1, %eax
	jle	.L999

#if defined(LT) || defined(RN)
	movl	A, %eax
	movl	%eax, AA
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

#ifdef RT
	movl	K, %eax
	sall	$ZBASE_SHIFT, %eax
	subl	%eax, B
#endif

#ifdef RT
       subl	LDC, C
#endif
	movl	C,  CO1
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
	andl	$1, %ebx
	jle	.L130

#ifdef LN
       movl	K, %eax
       sall	$ZBASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AA
#endif

	movl	B, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movsd	-32 * SIZE(AA), %xmm0
	pxor	%xmm2, %xmm2
	movsd	-32 * SIZE(BB), %xmm1
	pxor	%xmm3, %xmm3

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L142
	ALIGN_4

.L141:
	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0x55, %xmm1, %xmm3
	movsd	-30 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movsd	-30 * SIZE(AA), %xmm0

	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0x55, %xmm1, %xmm3
	movsd	-28 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movsd	-28 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0x55, %xmm1, %xmm3
	movsd	-26 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movsd	-26 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0x55, %xmm1, %xmm3
	movsd	-24 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movsd	-24 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0x55, %xmm1, %xmm3
	movsd	-22 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movsd	-22 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0x55, %xmm1, %xmm3
	movsd	-20 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movsd	-20 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0x55, %xmm1, %xmm3
	movsd	-18 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movsd	-18 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0x55, %xmm1, %xmm3
	movsd	-16 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movsd	-16 * SIZE(AA), %xmm0

	subl   $-16 * SIZE, AA
	subl   $-16 * SIZE, BB

	decl	%eax
	jne	.L141
	ALIGN_4

.L142:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L144
	ALIGN_4

.L143:
	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0x55, %xmm1, %xmm3
	movsd	-30 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movsd	-30 * SIZE(AA), %xmm0

	addl	$2 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L143
	ALIGN_4

.L144:
#if defined(LN) || defined(RT)
	movl	KK, %eax
	subl	$1, %eax

	movl	AORIG, AA
	sall	$ZBASE_SHIFT, %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 1), BB
#endif

	addps	%xmm2, %xmm4
	addps	%xmm3, %xmm5

	pshufd	 $0xb1, %xmm5, %xmm5
	pcmpeqb	%xmm0, %xmm0
	psllq	$63,   %xmm0

#ifndef CONJ
	shufps	$0xb1, %xmm0, %xmm0

	pxor	%xmm0, %xmm5
#else
#if defined(LN) || defined(LT)
	pxor	%xmm0, %xmm4
#else
	pxor	%xmm0, %xmm5
#endif
#endif

	addps	%xmm5, %xmm4

#if defined(LN) || defined(LT)
	movsd	-32 * SIZE(BB), %xmm2

	subps	%xmm4,  %xmm2
#else
	movsd	-32 * SIZE(AA), %xmm1

	subps	%xmm4,  %xmm1
#endif

#if defined(LN) || defined(LT)
	movaps	-32 * SIZE(AA), %xmm5

	pshufd	 $0x44, %xmm5, %xmm6
	pshufd	 $0x11, %xmm5, %xmm7

	pshufd	 $0xa0, %xmm2, %xmm4
	pshufd	 $0xf5, %xmm2, %xmm2

#ifndef CONJ
	xorps	 %xmm0, %xmm2
#else
	xorps	 %xmm0, %xmm4
#endif

	mulps	%xmm6,  %xmm4
	mulps	%xmm7,  %xmm2
	addps	%xmm4,  %xmm2
#endif

#if defined(RN) || defined(RT)
	movaps	-32 * SIZE(BB), %xmm4

	pshufd	 $0x44, %xmm4, %xmm6
	pshufd	 $0x11, %xmm4, %xmm7

	pshufd	 $0xa0, %xmm1, %xmm3
	pshufd	 $0xf5, %xmm1, %xmm1

#ifndef CONJ
	xorps	 %xmm0, %xmm1
#else
	xorps	 %xmm0, %xmm3
#endif

	mulps	%xmm6,  %xmm3
	mulps	%xmm7,  %xmm1

	addps	%xmm3,  %xmm1
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm2,  -32 * SIZE(BB)

	movlps	%xmm2,   0 * SIZE(CO1)
#else
	movlps	%xmm1,  -32 * SIZE(AA)

	movlps	%xmm1,   0 * SIZE(CO1)
#endif

#ifndef LN
	addl	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 1), BB
#endif

#ifdef LN
	subl	$1, KK
#endif

#ifdef LT
	addl	$1, KK
#endif

#ifdef RT
	movl	K, %eax
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AORIG
#endif
	ALIGN_4

.L130:
	movl	M,  %ebx
	sarl	$1, %ebx
	jle	.L149
	ALIGN_4

.L110:
#ifdef LN
       movl	K, %eax
       sall	$1 + ZBASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	AORIG, AA
	sall	$1 + ZBASE_SHIFT, %eax
	addl	%eax, AA
#endif

	movl	B, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movaps	-32 * SIZE(AA), %xmm0
	pxor	%xmm2, %xmm2
	movsd	-32 * SIZE(BB), %xmm1
	pxor	%xmm3, %xmm3
	movhps	-30 * SIZE(BB), %xmm1
	pxor	%xmm4, %xmm4
#ifdef LN
	prefetcht0	-4 * SIZE(CO1)
#else
	prefetcht0	 3 * SIZE(CO1)
#endif
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L112
	ALIGN_4

.L111:
	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)
	addps	%xmm3, %xmm5
	pshufd	$0x55, %xmm1, %xmm3
	mulps	%xmm0, %xmm3
	movaps	-28 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm4
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0xff, %xmm1, %xmm3
	movaps	-28 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movaps	-24 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0x55, %xmm1, %xmm3
	mulps	%xmm0, %xmm3
	movaps	-20 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm4
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0xff, %xmm1, %xmm3
	movaps	-24 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movaps	-16 * SIZE(AA), %xmm0

	PREFETCH (PREFETCHSIZE + 16) * SIZE(AA)

	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0x55, %xmm1, %xmm3
	mulps	%xmm0, %xmm3
	movaps	-12 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm4
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0xff, %xmm1, %xmm3
	movaps	-20 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movaps	 -8 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0x55, %xmm1, %xmm3
	mulps	%xmm0, %xmm3
	movaps	 -4 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm4
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0xff, %xmm1, %xmm3
	movaps	-16 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movaps	  0 * SIZE(AA), %xmm0

	subl   $-32 * SIZE, AA
	subl   $-16 * SIZE, BB

	decl   %eax
	jne	.L111
	ALIGN_4

.L112:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L114
	ALIGN_4

.L113:
	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0x55, %xmm1, %xmm3
	movsd	-30 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movaps	-28 * SIZE(AA), %xmm0

	addl	$4 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L113
	ALIGN_4

.L114:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$2, %eax
#else
	subl	$1, %eax
#endif

	movl	AORIG, AA
	sall	$ZBASE_SHIFT, %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 1), BB
#endif

	addps	%xmm2, %xmm4
	addps	%xmm3, %xmm5

	pshufd	 $0xb1, %xmm5, %xmm5
	pcmpeqb	%xmm0, %xmm0
	psllq	$63,   %xmm0

#ifndef CONJ
	shufps	$0xb1, %xmm0, %xmm0

	pxor	%xmm0, %xmm5
#else
#if defined(LN) || defined(LT)
	pxor	%xmm0, %xmm4
#else
	pxor	%xmm0, %xmm5
#endif
#endif

	addps	%xmm5, %xmm4

#if defined(LN) || defined(LT)
	movaps	%xmm4, %xmm5
	unpcklpd %xmm6, %xmm4
	unpckhpd %xmm6, %xmm5

	movsd	-32 * SIZE(BB), %xmm2
	movsd	-30 * SIZE(BB), %xmm3

	subps	%xmm4,  %xmm2
	subps	%xmm5,  %xmm3
#else
	movaps	-32 * SIZE(AA), %xmm1

	subps	%xmm4,  %xmm1
#endif

#ifdef LN
	movaps	-28 * SIZE(AA), %xmm5

	pshufd	 $0xee, %xmm5, %xmm6
	pshufd	 $0xbb, %xmm5, %xmm7

	pshufd	 $0xa0, %xmm3, %xmm4
	pshufd	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 %xmm0, %xmm3
#else
	xorps	 %xmm0, %xmm4
#endif

	mulps	%xmm6,  %xmm4
	mulps	%xmm7, %xmm3
	addps	%xmm4,  %xmm3

	pshufd	 $0x44, %xmm5, %xmm6
	pshufd	 $0x11, %xmm5, %xmm7

	pshufd	 $0xa0, %xmm3, %xmm4
	pshufd	 $0xf5, %xmm3, %xmm1

#ifndef CONJ
	xorps	 %xmm0, %xmm1
#else
	xorps	 %xmm0, %xmm4
#endif

	mulps	%xmm6,  %xmm4
	mulps	%xmm7, %xmm1
	subps	%xmm4,  %xmm2
	subps	%xmm1,  %xmm2

	movaps	-32 * SIZE(AA), %xmm5

	pshufd	 $0x44, %xmm5, %xmm6
	pshufd	 $0x11, %xmm5, %xmm7

	pshufd	 $0xa0, %xmm2, %xmm4
	pshufd	 $0xf5, %xmm2, %xmm2

#ifndef CONJ
	xorps	 %xmm0, %xmm2
#else
	xorps	 %xmm0, %xmm4
#endif

	mulps	%xmm6,  %xmm4
	mulps	%xmm7, %xmm2
	addps	%xmm4,  %xmm2
#endif

#ifdef LT
	movaps	-32 * SIZE(AA), %xmm5

	pshufd	 $0x44, %xmm5, %xmm6
	pshufd	 $0x11, %xmm5, %xmm7

	pshufd	 $0xa0, %xmm2, %xmm4
	pshufd	 $0xf5, %xmm2, %xmm2

#ifndef CONJ
	xorps	 %xmm0, %xmm2
#else
	xorps	 %xmm0, %xmm4
#endif

	mulps	%xmm6,  %xmm4
	mulps	%xmm7, %xmm2
	addps	%xmm4,  %xmm2

	pshufd	 $0xee, %xmm5, %xmm6
	pshufd	 $0xbb, %xmm5, %xmm7

	pshufd	 $0xa0, %xmm2, %xmm4
	pshufd	 $0xf5, %xmm2, %xmm1

#ifndef CONJ
	xorps	 %xmm0, %xmm1
#else
	xorps	 %xmm0, %xmm4
#endif

	mulps	%xmm6,  %xmm4
	mulps	%xmm7, %xmm1
	subps	%xmm4,  %xmm3
	subps	%xmm1,  %xmm3

	movaps	-28 * SIZE(AA), %xmm5

	pshufd	 $0xee, %xmm5, %xmm6
	pshufd	 $0xbb, %xmm5, %xmm7

	pshufd	 $0xa0, %xmm3, %xmm4
	pshufd	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 %xmm0, %xmm3
#else
	xorps	 %xmm0, %xmm4
#endif

	mulps	%xmm6,  %xmm4
	mulps	%xmm7,  %xmm3
	addps	%xmm4,  %xmm3
#endif

#if defined(RN) || defined(RT)
	movaps	-32 * SIZE(BB), %xmm4

	pshufd	 $0x44, %xmm4, %xmm6
	pshufd	 $0x11, %xmm4, %xmm7

	pshufd	 $0xa0, %xmm1, %xmm3
	pshufd	 $0xf5, %xmm1, %xmm1

#ifndef CONJ
	xorps	 %xmm0, %xmm1
#else
	xorps	 %xmm0, %xmm3
#endif

	mulps	%xmm6,  %xmm3
	mulps	%xmm7,  %xmm1

	addps	%xmm3,  %xmm1
#endif

#ifdef LN
	subl	$4 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm2, -32 * SIZE(BB)
	movlps	%xmm3, -30 * SIZE(BB)

	movlps	%xmm2,   0 * SIZE(CO1)
	movlps	%xmm3,   2 * SIZE(CO1)
#else
	movaps	%xmm1, -32 * SIZE(AA)

	movlps	%xmm1,   0 * SIZE(CO1)
	movhps	%xmm1,   2 * SIZE(CO1)
#endif

#ifndef LN
	addl	$4 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 1), BB
#endif

#ifdef LN
	subl	$2, KK
#endif

#ifdef LT
	addl	$2, KK
#endif

#ifdef RT
	movl	K, %eax
	sall	$1 + ZBASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx			# i --
	jg	.L110
	ALIGN_4

.L149:
#ifdef LN
       movl	K, %eax
       sall	$ZBASE_SHIFT, %eax
       addl	%eax, B
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

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp

	addl	$ARGS, %esp
	ret

	EPILOGUE
