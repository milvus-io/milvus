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
#define A	20 + STACK + ARGS(%esp)
#define ARG_B	24 + STACK + ARGS(%esp)
#define C	28 + STACK + ARGS(%esp)
#define ARG_LDC	32 + STACK + ARGS(%esp)
#define OFFSET	36 + STACK + ARGS(%esp)

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

#ifdef ATOM
#define PREFETCH     prefetcht0
#define PREFETCHSIZE  (8 * 8 + 4)
#endif

#ifdef NANO
#define PREFETCH	prefetcht0
#define PREFETCHSIZE (16 * 2)
#endif

#define B	%edi
#define AA	%edx
#define	BB	%ecx
#define LDC	%ebp
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

	subl	$-32 * SIZE, A
	subl	$-32 * SIZE, B

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

	movl	N, %eax
	sarl	$2, %eax
	movl	%eax, J
	jle	.L40

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

	testl	$1, M
	je	.L20

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

	pxor	%xmm4, %xmm4
	movsd	-32 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movaps	-32 * SIZE(BB), %xmm1
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L35
	ALIGN_4

.L32:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	pshufd	$0x00, %xmm0, %xmm2
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movaps	-28 * SIZE(BB), %xmm1

	pshufd	$0x55, %xmm0, %xmm2
	movsd	-30 * SIZE(AA), %xmm0

	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movaps	-24 * SIZE(BB), %xmm1

	pshufd	$0x00, %xmm0, %xmm2
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movaps	-20 * SIZE(BB), %xmm1

	pshufd	$0x55, %xmm0, %xmm2
	movsd	-28 * SIZE(AA), %xmm0

	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movaps	-16 * SIZE(BB), %xmm1

	pshufd	$0x00, %xmm0, %xmm2
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movaps	-12 * SIZE(BB), %xmm1

	pshufd	$0x55, %xmm0, %xmm2
	movsd	-26 * SIZE(AA), %xmm0

	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movaps	 -8 * SIZE(BB), %xmm1

	pshufd	$0x00, %xmm0, %xmm2
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movaps	 -4 * SIZE(BB), %xmm1

	pshufd	$0x55, %xmm0, %xmm2
	movsd	-24 * SIZE(AA), %xmm0

	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movaps	  0 * SIZE(BB), %xmm1

	subl   $ -8 * SIZE, AA
	subl   $-32 * SIZE, BB

	subl   $1, %eax
	jne    .L32
	ALIGN_4

.L35:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L38
	ALIGN_4

.L36:
	pshufd	$0x00, %xmm0, %xmm2
	movss	-31 * SIZE(AA), %xmm0

	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movaps	-28 * SIZE(BB), %xmm1

	addl	$1 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L36
	ALIGN_4

.L38:
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
	movaps	-32 * SIZE(BB), %xmm1

	subps	%xmm4,  %xmm1
#else
	movsd	-32 * SIZE(AA), %xmm0
	movhps	-30 * SIZE(AA), %xmm0

	subps	%xmm4, %xmm0

	pshufd	$0xff, %xmm0, %xmm3
	pshufd	$0xaa, %xmm0, %xmm2
	pshufd	$0x55, %xmm0, %xmm1
	pshufd	$0x00, %xmm0, %xmm0
#endif

#if defined(LN) || defined(LT)
	movss	 -32 * SIZE(AA), %xmm4
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm6, %xmm1
#endif

#ifdef RN
	movaps	-32 * SIZE(BB), %xmm6
	pshufd	 $0x00, %xmm6, %xmm7
	mulss	 %xmm7, %xmm0
	pshufd	 $0x55, %xmm6, %xmm7
	mulss	 %xmm0, %xmm7
	subss	 %xmm7, %xmm1
	pshufd	 $0xaa, %xmm6, %xmm7
	mulss	 %xmm0, %xmm7
	subss	 %xmm7, %xmm2
	pshufd	 $0xff, %xmm6, %xmm7
	mulss	 %xmm0, %xmm7
	subss	 %xmm7, %xmm3

	movaps	-28 * SIZE(BB), %xmm6
	pshufd	 $0x55, %xmm6, %xmm7
	mulss	 %xmm7, %xmm1
	pshufd	 $0xaa, %xmm6, %xmm7
	mulss	 %xmm1, %xmm7
	subss	 %xmm7, %xmm2
	pshufd	 $0xff, %xmm6, %xmm7
	mulss	 %xmm1, %xmm7
	subss	 %xmm7, %xmm3

	movaps	-24 * SIZE(BB), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm7
	mulss	 %xmm7, %xmm2
	pshufd	 $0xff, %xmm6, %xmm7
	mulss	 %xmm2, %xmm7
	subss	 %xmm7, %xmm3

	movaps	-20 * SIZE(BB), %xmm6
	pshufd	 $0xff, %xmm6, %xmm7
	mulss	 %xmm7, %xmm3
#endif

#ifdef RT
	movaps	-20 * SIZE(BB), %xmm6
	pshufd	 $0xff, %xmm6, %xmm7
	mulss	 %xmm7, %xmm3
	pshufd	 $0xaa, %xmm6, %xmm7
	mulss	 %xmm3, %xmm7
	subss	 %xmm7, %xmm2
	pshufd	 $0x55, %xmm6, %xmm7
	mulss	 %xmm3, %xmm7
	subss	 %xmm7, %xmm1
	pshufd	 $0x00, %xmm6, %xmm7
	mulss	 %xmm3, %xmm7
	subss	 %xmm7, %xmm0

	movaps	-24 * SIZE(BB), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm7
	mulss	 %xmm7, %xmm2
	pshufd	 $0x55, %xmm6, %xmm7
	mulss	 %xmm2, %xmm7
	subss	 %xmm7, %xmm1
	pshufd	 $0x00, %xmm6, %xmm7
	mulss	 %xmm2, %xmm7
	subss	 %xmm7, %xmm0

	movaps	-28 * SIZE(BB), %xmm6
	pshufd	 $0x55, %xmm6, %xmm7
	mulss	 %xmm7, %xmm1
	pshufd	 $0x00, %xmm6, %xmm7
	mulss	 %xmm1, %xmm7
	subss	 %xmm7, %xmm0

	movaps	-32 * SIZE(BB), %xmm6
	pshufd	 $0x00, %xmm6, %xmm7
	mulss	 %xmm7, %xmm0
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm1,  -32 * SIZE(BB)
#else
	movss	%xmm0,  -32 * SIZE(AA)
	movss	%xmm1,  -31 * SIZE(AA)
	movss	%xmm2,  -30 * SIZE(AA)
	movss	%xmm3,  -29 * SIZE(AA)
#endif

#ifdef LN
	subl	$1 * SIZE, CO1
#endif

	leal	(LDC, LDC, 2), %eax

#if defined(LN) || defined(LT)
	movaps	 %xmm1, %xmm0
	unpcklps %xmm5, %xmm1
	unpckhps %xmm5, %xmm0

	movaps	 %xmm3, %xmm4
	unpcklps %xmm7, %xmm3
	unpckhps %xmm7, %xmm4

	movaps	 %xmm1, %xmm2
	unpcklps %xmm3, %xmm1
	unpckhps %xmm3, %xmm2

	movaps	 %xmm0, %xmm6
	unpcklps %xmm4, %xmm0
	unpckhps %xmm4, %xmm6

	movss	%xmm1, 0 * SIZE(CO1)
	movss	%xmm2, 0 * SIZE(CO1, LDC, 1)
	movss	%xmm0, 0 * SIZE(CO1, LDC, 2)
	movss	%xmm6, 0 * SIZE(CO1, %eax, 1)
#else
	movss	%xmm0, 0 * SIZE(CO1)
	movss	%xmm1, 0 * SIZE(CO1, LDC, 1)
	movss	%xmm2, 0 * SIZE(CO1, LDC, 2)
	movss	%xmm3, 0 * SIZE(CO1, %eax, 1)
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

.L20:
	testl	$2, M
	je	.L30

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

	pxor	%xmm4, %xmm4
	movaps	-32 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movaps	-32 * SIZE(BB), %xmm1
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

	pshufd	$0x44, %xmm0, %xmm2
	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm4
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	-28 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm6

	pshufd	$0xee, %xmm0, %xmm2
	movaps	-28 * SIZE(AA), %xmm0

	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm5
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	-24 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm7

	pshufd	$0x44, %xmm0, %xmm2
	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm4
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	-20 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm6

	pshufd	$0xee, %xmm0, %xmm2
	movaps	-24 * SIZE(AA), %xmm0

	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm5
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	-16 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm7

	pshufd	$0x44, %xmm0, %xmm2
	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm4
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	-12 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm6

	pshufd	$0xee, %xmm0, %xmm2
	movaps	-20 * SIZE(AA), %xmm0

	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm5
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	 -8 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm7

	pshufd	$0x44, %xmm0, %xmm2
	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm4
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	 -4 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm6

	pshufd	$0xee, %xmm0, %xmm2
	movaps	-16 * SIZE(AA), %xmm0

	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm5
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	  0 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm7

	subl   $-16 * SIZE, AA
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
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L28
	ALIGN_4

.L26:
	pshufd	$0x44, %xmm0, %xmm2
	movsd	-30 * SIZE(AA), %xmm0

	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm4
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	-28 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm6

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L26
	ALIGN_4

.L28:
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

	addps	%xmm5, %xmm4
	addps	%xmm7, %xmm6

	movhlps	%xmm4, %xmm5
	movhlps	%xmm6, %xmm7


#if defined(LN) || defined(LT)
	unpcklps %xmm6, %xmm4
	unpcklps %xmm7, %xmm5

	movaps	 %xmm4, %xmm6
	unpcklps %xmm5, %xmm4
	unpckhps %xmm5, %xmm6

	movaps	-32 * SIZE(BB), %xmm1
	movaps	-28 * SIZE(BB), %xmm3

	subps	%xmm4,  %xmm1
	subps	%xmm6,  %xmm3
#else
	movsd	-32 * SIZE(AA), %xmm0
	movsd	-30 * SIZE(AA), %xmm1
	movsd	-28 * SIZE(AA), %xmm2
	movsd	-26 * SIZE(AA), %xmm3

	subps	%xmm4, %xmm0
	subps	%xmm5, %xmm1
	subps	%xmm6, %xmm2
	subps	%xmm7, %xmm3
#endif

#ifdef LN
	movaps	-32 * SIZE(AA), %xmm4
	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm6, %xmm3
	pshufd	 $0xaa, %xmm4, %xmm6
	mulps	 %xmm3, %xmm6
	subps	 %xmm6, %xmm1

	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm6, %xmm1
#endif

#ifdef LT
	movaps	-32 * SIZE(AA), %xmm4
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm6, %xmm1

	pshufd	 $0x55, %xmm4, %xmm6
	mulps	 %xmm1, %xmm6
	subps	 %xmm6, %xmm3

	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm6, %xmm3
#endif

#ifdef RN
	movaps	-32 * SIZE(BB), %xmm6
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm7, %xmm0
	pshufd	 $0x55, %xmm6, %xmm7
	mulps	 %xmm0, %xmm7
	subps	 %xmm7, %xmm1
	pshufd	 $0xaa, %xmm6, %xmm7
	mulps	 %xmm0, %xmm7
	subps	 %xmm7, %xmm2
	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm0, %xmm7
	subps	 %xmm7, %xmm3

	movaps	-28 * SIZE(BB), %xmm6
	pshufd	 $0x55, %xmm6, %xmm7
	mulps	 %xmm7, %xmm1
	pshufd	 $0xaa, %xmm6, %xmm7
	mulps	 %xmm1, %xmm7
	subps	 %xmm7, %xmm2
	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm1, %xmm7
	subps	 %xmm7, %xmm3

	movaps	-24 * SIZE(BB), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm7
	mulps	 %xmm7, %xmm2
	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm2, %xmm7
	subps	 %xmm7, %xmm3

	movaps	-20 * SIZE(BB), %xmm6
	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm7, %xmm3
#endif

#ifdef RT
	movaps	-20 * SIZE(BB), %xmm6
	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm7, %xmm3
	pshufd	 $0xaa, %xmm6, %xmm7
	mulps	 %xmm3, %xmm7
	subps	 %xmm7, %xmm2
	pshufd	 $0x55, %xmm6, %xmm7
	mulps	 %xmm3, %xmm7
	subps	 %xmm7, %xmm1
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm3, %xmm7
	subps	 %xmm7, %xmm0

	movaps	-24 * SIZE(BB), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm7
	mulps	 %xmm7, %xmm2
	pshufd	 $0x55, %xmm6, %xmm7
	mulps	 %xmm2, %xmm7
	subps	 %xmm7, %xmm1
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm2, %xmm7
	subps	 %xmm7, %xmm0

	movaps	-28 * SIZE(BB), %xmm6
	pshufd	 $0x55, %xmm6, %xmm7
	mulps	 %xmm7, %xmm1
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm1, %xmm7
	subps	 %xmm7, %xmm0

	movaps	-32 * SIZE(BB), %xmm6
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm7, %xmm0
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm1, -32 * SIZE(BB)
	movaps	%xmm3, -28 * SIZE(BB)
#else
	movlps	%xmm0, -32 * SIZE(AA)
	movlps	%xmm1, -30 * SIZE(AA)
	movlps	%xmm2, -28 * SIZE(AA)
	movlps	%xmm3, -26 * SIZE(AA)
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

	leal	(LDC, LDC, 2), %eax

#if defined(LN) || defined(LT)
	movaps	 %xmm1, %xmm0
	unpcklps %xmm5, %xmm1
	unpckhps %xmm5, %xmm0

	movaps	 %xmm3, %xmm4
	unpcklps %xmm7, %xmm3
	unpckhps %xmm7, %xmm4

	movaps	 %xmm1, %xmm2
	unpcklps %xmm3, %xmm1
	unpckhps %xmm3, %xmm2

	movaps	 %xmm0, %xmm6
	unpcklps %xmm4, %xmm0
	unpckhps %xmm4, %xmm6

	movlps	%xmm1, 0 * SIZE(CO1)
	movlps	%xmm2, 0 * SIZE(CO1, LDC, 1)
	movlps	%xmm0, 0 * SIZE(CO1, LDC, 2)
	movlps	%xmm6, 0 * SIZE(CO1, %eax, 1)
#else
	movlps	%xmm0, 0 * SIZE(CO1)
	movlps	%xmm1, 0 * SIZE(CO1, LDC, 1)
	movlps	%xmm2, 0 * SIZE(CO1, LDC, 2)
	movlps	%xmm3, 0 * SIZE(CO1, %eax, 1)
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
	ALIGN_4

.L30:
	movl	M,  %ebx
	sarl	$2, %ebx	# i = (m >> 2)
	jle	.L39
	ALIGN_4

.L11:
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

	movl	B, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$2 + BASE_SHIFT, %eax
	addl	%eax, BB
#endif

	leal		(CO1, LDC, 2), %eax

	movaps	-32 * SIZE(AA), %xmm0
	pxor	%xmm2, %xmm2
	movaps	-32 * SIZE(BB), %xmm1
	pxor	%xmm3, %xmm3

#ifdef LN
	pxor	%xmm4, %xmm4
	prefetcht0	-4 * SIZE(CO1)
	pxor	%xmm5, %xmm5
	prefetcht0	-4 * SIZE(CO1, LDC)
	pxor	%xmm6, %xmm6
	prefetcht0	-4 * SIZE(%eax)
	pxor	%xmm7, %xmm7
	prefetcht0	-4 * SIZE(%eax, LDC)
#else
	pxor	%xmm4, %xmm4
	prefetcht0	3 * SIZE(CO1)
	pxor	%xmm5, %xmm5
	prefetcht0	3 * SIZE(CO1, LDC)
	pxor	%xmm6, %xmm6
	prefetcht0	3 * SIZE(%eax)
	pxor	%xmm7, %xmm7
	prefetcht0	3 * SIZE(%eax, LDC)
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

	addps	%xmm2, %xmm7
	pshufd	$0x93, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm6
	pshufd	$0x93, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm2, %xmm5
	pshufd	$0x93, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm1, %xmm4
	movaps	-28 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm7
	pshufd	$0x93, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm6
	pshufd	$0x93, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm2, %xmm5
	pshufd	$0x93, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm1, %xmm4
	movaps	-24 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-24 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm7
	pshufd	$0x93, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm6
	pshufd	$0x93, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm2, %xmm5
	pshufd	$0x93, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm1, %xmm4
	movaps	-20 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-20 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm7
	pshufd	$0x93, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm6
	pshufd	$0x93, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm2, %xmm5
	pshufd	$0x93, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm1, %xmm4
	movaps	-16 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-16 * SIZE(AA), %xmm0

	PREFETCH (PREFETCHSIZE + 16) * SIZE(AA)

	addps	%xmm2, %xmm7
	pshufd	$0x93, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm6
	pshufd	$0x93, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm2, %xmm5
	pshufd	$0x93, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm1, %xmm4
	movaps	-12 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-12 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm7
	pshufd	$0x93, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm6
	pshufd	$0x93, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm2, %xmm5
	pshufd	$0x93, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm1, %xmm4
	movaps	 -8 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	 -8 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm7
	pshufd	$0x93, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm6
	pshufd	$0x93, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm2, %xmm5
	pshufd	$0x93, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm1, %xmm4
	movaps	 -4 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	 -4 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm7
	subl   $-32 * SIZE, BB
	pshufd	$0x93, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm6
	pshufd	$0x93, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm2, %xmm5
	subl   $-32 * SIZE, AA
	pshufd	$0x93, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm1, %xmm4
	movaps	-32 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-32 * SIZE(AA), %xmm0

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
	ALIGN_4

.L16:
	addps	%xmm2, %xmm7
	pshufd	$0x93, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm6
	pshufd	$0x93, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm2, %xmm5
	pshufd	$0x93, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm1, %xmm4
	movaps	-28 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(AA), %xmm0

	addl	$4 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L16
	ALIGN_4

.L18:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$4, %eax
#else
	subl	$4, %eax
#endif

	movl	AORIG, AA

	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(B,  %eax, 4), BB
#endif

	addps	%xmm3, %xmm6
	addps	%xmm2, %xmm7

#if defined(LN) || defined(LT)
	movaps	 %xmm4, %xmm0
	unpcklps %xmm7, %xmm0
	unpckhps %xmm7, %xmm4

	movaps	 %xmm6, %xmm2
	unpcklps %xmm5, %xmm2
	unpckhps %xmm5, %xmm6

	movaps	 %xmm0, %xmm1
	movlhps	 %xmm2, %xmm0
	movhlps	 %xmm2, %xmm1

	movaps	 %xmm6, %xmm7
	movlhps	 %xmm4, %xmm6
	movhlps	 %xmm4, %xmm7

	pshufd	 $0x39, %xmm1, %xmm2
	pshufd	 $0x39, %xmm7, %xmm4

	movaps	-32 * SIZE(BB), %xmm1
	movaps	-28 * SIZE(BB), %xmm3
	movaps	-24 * SIZE(BB), %xmm5
	movaps	-20 * SIZE(BB), %xmm7

	subps	%xmm0,  %xmm1
	subps	%xmm2,  %xmm3
	subps	%xmm6,  %xmm5
	subps	%xmm4,  %xmm7
#else
	pshufd	$0x39, %xmm5, %xmm2
	pshufd	$0x4e, %xmm6, %xmm0
	pshufd	$0x93, %xmm7, %xmm7

	movaps	 %xmm4, %xmm6
	unpcklps %xmm0, %xmm4
	unpckhps %xmm0, %xmm6

	movaps	 %xmm2, %xmm1
	unpcklps %xmm7, %xmm2
	unpckhps %xmm7, %xmm1

	movaps	 %xmm4, %xmm5
	unpcklps %xmm2, %xmm4
	unpckhps %xmm2, %xmm5

	movaps	 %xmm6, %xmm7
	unpcklps %xmm1, %xmm6
	unpckhps %xmm1, %xmm7

	pshufd	 $0x93, %xmm5, %xmm5
	pshufd	 $0x4e, %xmm6, %xmm6
	pshufd	 $0x39, %xmm7, %xmm7

	movaps	-32 * SIZE(AA), %xmm0
	movaps	-28 * SIZE(AA), %xmm1
	movaps	-24 * SIZE(AA), %xmm2
	movaps	-20 * SIZE(AA), %xmm3

	subps	%xmm4, %xmm0
	subps	%xmm5, %xmm1
	subps	%xmm6, %xmm2
	subps	%xmm7, %xmm3
#endif

#ifdef LN
	movaps	 -20 * SIZE(AA), %xmm4
	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm6, %xmm7
	pshufd	 $0xaa, %xmm4, %xmm6
	mulps	 %xmm7, %xmm6
	subps	 %xmm6, %xmm5
	pshufd	 $0x55, %xmm4, %xmm6
	mulps	 %xmm7, %xmm6
	subps	 %xmm6, %xmm3
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm7, %xmm6
	subps	 %xmm6, %xmm1

	movaps	 -24 * SIZE(AA), %xmm4
	pshufd	 $0xaa, %xmm4, %xmm6
	mulps	 %xmm6, %xmm5
	pshufd	 $0x55, %xmm4, %xmm6
	mulps	 %xmm5, %xmm6
	subps	 %xmm6, %xmm3
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm5, %xmm6
	subps	 %xmm6, %xmm1

	movaps	 -28 * SIZE(AA), %xmm4
	pshufd	 $0x55, %xmm4, %xmm6
	mulps	 %xmm6, %xmm3
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm3, %xmm6
	subps	 %xmm6, %xmm1

	movaps	 -32 * SIZE(AA), %xmm4
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm6, %xmm1
#endif

#ifdef LT
	movaps	 -32 * SIZE(AA), %xmm4
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm6, %xmm1

	pshufd	 $0x55, %xmm4, %xmm6
	mulps	 %xmm1, %xmm6
	subps	 %xmm6, %xmm3
	pshufd	 $0xaa, %xmm4, %xmm6
	mulps	 %xmm1, %xmm6
	subps	 %xmm6, %xmm5
	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm1, %xmm6
	subps	 %xmm6, %xmm7

	movaps	 -28 * SIZE(AA), %xmm4
	pshufd	 $0x55, %xmm4, %xmm6
	mulps	 %xmm6, %xmm3
	pshufd	 $0xaa, %xmm4, %xmm6
	mulps	 %xmm3, %xmm6
	subps	 %xmm6, %xmm5
	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm3, %xmm6
	subps	 %xmm6, %xmm7

	movaps	 -24 * SIZE(AA), %xmm4
	pshufd	 $0xaa, %xmm4, %xmm6
	mulps	 %xmm6, %xmm5
	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm5, %xmm6
	subps	 %xmm6, %xmm7

	movaps	 -20 * SIZE(AA), %xmm4
	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm6, %xmm7
#endif

#ifdef RN
	movaps	 -32 * SIZE(BB), %xmm6
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm7, %xmm0
	pshufd	 $0x55, %xmm6, %xmm7
	mulps	 %xmm0, %xmm7
	subps	 %xmm7, %xmm1
	pshufd	 $0xaa, %xmm6, %xmm7
	mulps	 %xmm0, %xmm7
	subps	 %xmm7, %xmm2
	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm0, %xmm7
	subps	 %xmm7, %xmm3

	movaps	 -28 * SIZE(BB), %xmm6
	pshufd	 $0x55, %xmm6, %xmm7
	mulps	 %xmm7, %xmm1
	pshufd	 $0xaa, %xmm6, %xmm7
	mulps	 %xmm1, %xmm7
	subps	 %xmm7, %xmm2
	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm1, %xmm7
	subps	 %xmm7, %xmm3

	movaps	 -24 * SIZE(BB), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm7
	mulps	 %xmm7, %xmm2
	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm2, %xmm7
	subps	 %xmm7, %xmm3

	movaps	 -20 * SIZE(BB), %xmm6
	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm7, %xmm3
#endif

#ifdef RT
	movaps	 -20 * SIZE(BB), %xmm6
	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm7, %xmm3
	pshufd	 $0xaa, %xmm6, %xmm7
	mulps	 %xmm3, %xmm7
	subps	 %xmm7, %xmm2
	pshufd	 $0x55, %xmm6, %xmm7
	mulps	 %xmm3, %xmm7
	subps	 %xmm7, %xmm1
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm3, %xmm7
	subps	 %xmm7, %xmm0

	movaps	 -24 * SIZE(BB), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm7
	mulps	 %xmm7, %xmm2
	pshufd	 $0x55, %xmm6, %xmm7
	mulps	 %xmm2, %xmm7
	subps	 %xmm7, %xmm1
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm2, %xmm7
	subps	 %xmm7, %xmm0

	movaps	 -28 * SIZE(BB), %xmm6
	pshufd	 $0x55, %xmm6, %xmm7
	mulps	 %xmm7, %xmm1
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm1, %xmm7
	subps	 %xmm7, %xmm0

	movaps	 -32 * SIZE(BB), %xmm6
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm7, %xmm0
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm1, -32 * SIZE(BB)
	movaps	%xmm3, -28 * SIZE(BB)
	movaps	%xmm5, -24 * SIZE(BB)
	movaps	%xmm7, -20 * SIZE(BB)
#else
	movaps	%xmm0, -32 * SIZE(AA)
	movaps	%xmm1, -28 * SIZE(AA)
	movaps	%xmm2, -24 * SIZE(AA)
	movaps	%xmm3, -20 * SIZE(AA)
#endif

#ifdef LN
	subl	$4 * SIZE, CO1
#endif

	leal	(LDC, LDC, 2), %eax

#if defined(LN) || defined(LT)
	movaps	 %xmm1, %xmm0
	unpcklps %xmm5, %xmm1
	unpckhps %xmm5, %xmm0

	movaps	 %xmm3, %xmm4
	unpcklps %xmm7, %xmm3
	unpckhps %xmm7, %xmm4

	movaps	 %xmm1, %xmm2
	unpcklps %xmm3, %xmm1
	unpckhps %xmm3, %xmm2

	movaps	 %xmm0, %xmm6
	unpcklps %xmm4, %xmm0
	unpckhps %xmm4, %xmm6

	movlps	%xmm1, 0 * SIZE(CO1)
	movhps	%xmm1, 2 * SIZE(CO1)
	movlps	%xmm2, 0 * SIZE(CO1, LDC, 1)
	movhps	%xmm2, 2 * SIZE(CO1, LDC, 1)
	movlps	%xmm0, 0 * SIZE(CO1, LDC, 2)
	movhps	%xmm0, 2 * SIZE(CO1, LDC, 2)
	movlps	%xmm6, 0 * SIZE(CO1, %eax, 1)
	movhps	%xmm6, 2 * SIZE(CO1, %eax, 1)
#else
	movlps	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 2 * SIZE(CO1)
	movlps	%xmm1, 0 * SIZE(CO1, LDC, 1)
	movhps	%xmm1, 2 * SIZE(CO1, LDC, 1)
	movlps	%xmm2, 0 * SIZE(CO1, LDC, 2)
	movhps	%xmm2, 2 * SIZE(CO1, LDC, 2)
	movlps	%xmm3, 0 * SIZE(CO1, %eax, 1)
	movhps	%xmm3, 2 * SIZE(CO1, %eax, 1)
#endif

#ifndef LN
	addl	$4 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 4), BB
#endif

#ifdef LN
	subl	$4, KK
#endif

#ifdef LT
	addl	$4, KK
#endif

#ifdef RT
	movl	K, %eax
	sall	$2 + BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx			# i --
	jg	.L11
	ALIGN_4

.L39:
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

.L40:
	testl	$2, N
	je	.L80

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

	testl	$1, M
	je	.L60

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

	pxor	%xmm4, %xmm4
	movsd	-32 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movsd	-32 * SIZE(BB), %xmm1

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

	pshufd	$0x00, %xmm0, %xmm2
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movsd	-30 * SIZE(BB), %xmm1

	pshufd	$0x55, %xmm0, %xmm2
	movsd	-30 * SIZE(AA), %xmm0
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm5
	movsd	-28 * SIZE(BB), %xmm1

	pshufd	$0x00, %xmm0, %xmm2
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movsd	-26 * SIZE(BB), %xmm1

	pshufd	$0x55, %xmm0, %xmm2
	movsd	-28 * SIZE(AA), %xmm0
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm5
	movsd	-24 * SIZE(BB), %xmm1

	pshufd	$0x00, %xmm0, %xmm2
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movsd	-22 * SIZE(BB), %xmm1

	pshufd	$0x55, %xmm0, %xmm2
	movsd	-26 * SIZE(AA), %xmm0
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm5
	movsd	-20 * SIZE(BB), %xmm1

	pshufd	$0x00, %xmm0, %xmm2
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movsd	-18 * SIZE(BB), %xmm1

	pshufd	$0x55, %xmm0, %xmm2
	movsd	-24 * SIZE(AA), %xmm0
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm5
	movsd	-16 * SIZE(BB), %xmm1

	subl   $ -8 * SIZE, AA
	subl   $-16 * SIZE, BB

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
	ALIGN_4

.L76:
	pshufd	$0x00, %xmm0, %xmm2
	movss	-31 * SIZE(AA), %xmm0
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movsd	-30 * SIZE(BB), %xmm1

	addl	$1 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L76
	ALIGN_4

.L78:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$2, %eax
#endif

	movl	AORIG, AA

	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 2), BB
#endif

	addps	%xmm5, %xmm4

	pshufd	$0x55, %xmm4, %xmm5
	pshufd	$0x00, %xmm4, %xmm4

#if defined(LN) || defined(LT)
	unpcklps %xmm5, %xmm4

	movsd	-32 * SIZE(BB), %xmm1

	subps	%xmm4,  %xmm1
#else
	movss	-32 * SIZE(AA), %xmm0
	movss	-31 * SIZE(AA), %xmm1

	subss	%xmm4, %xmm0
	subss	%xmm5, %xmm1
#endif

#if defined(LN) || defined(LT)
	movss	 -32 * SIZE(AA), %xmm4
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm6, %xmm1
#endif

#ifdef RN
	movaps	-32 * SIZE(BB), %xmm6
	pshufd	 $0x00, %xmm6, %xmm7
	mulss	 %xmm7, %xmm0
	pshufd	 $0x55, %xmm6, %xmm7
	mulss	 %xmm0, %xmm7
	subss	 %xmm7, %xmm1

	pshufd	 $0xff, %xmm6, %xmm7
	mulss	 %xmm7, %xmm1
#endif

#ifdef RT
	movaps	-32 * SIZE(BB), %xmm6
	pshufd	 $0xff, %xmm6, %xmm7
	mulss	 %xmm7, %xmm1
	pshufd	 $0xaa, %xmm6, %xmm7
	mulss	 %xmm1, %xmm7
	subss	 %xmm7, %xmm0

	pshufd	 $0x00, %xmm6, %xmm7
	mulss	 %xmm7, %xmm0
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm1, -32 * SIZE(BB)
#else
	movss	%xmm0, -32 * SIZE(AA)
	movss	%xmm1, -31 * SIZE(AA)
#endif

#ifdef LN
	subl	$1 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	pshufd	$1, %xmm1, %xmm3

	movss	%xmm1, 0 * SIZE(CO1)
	movss	%xmm3, 0 * SIZE(CO1, LDC)
#else
	movss	%xmm0, 0 * SIZE(CO1)
	movss	%xmm1, 0 * SIZE(CO1, LDC)
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
	sall	$BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif
	ALIGN_4

.L60:
	testl	$2, M
	je	.L70

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

	movaps	-32 * SIZE(AA), %xmm0
	pxor	%xmm3, %xmm3
	movaps	-32 * SIZE(BB), %xmm1
	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L65
	ALIGN_4

.L62:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	pshufd	$0x44, %xmm0, %xmm2
	addps	%xmm3, %xmm4
	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3

	pshufd	$0xee, %xmm0, %xmm2
	movaps	-28 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	-28 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3

	pshufd	$0x44, %xmm0, %xmm2
	addps	%xmm3, %xmm4
	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3

	pshufd	$0xee, %xmm0, %xmm2
	movaps	-24 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	-24 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3

	pshufd	$0x44, %xmm0, %xmm2
	addps	%xmm3, %xmm4
	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3

	pshufd	$0xee, %xmm0, %xmm2
	movaps	-20 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	-20 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3

	pshufd	$0x44, %xmm0, %xmm2
	addps	%xmm3, %xmm4
	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3

	pshufd	$0xee, %xmm0, %xmm2
	movaps	-16 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	-16 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3

	subl   $-16 * SIZE, AA
	subl   $-16 * SIZE, BB

	subl   $1, %eax
	jne    .L62
	ALIGN_4

.L65:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L68
	ALIGN_4

.L66:
	pshufd	$0x44, %xmm0, %xmm2
	movsd	-30 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm4
	pshufd	$0x50, %xmm1, %xmm3
	movsd	-30 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3

	addl	$2 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L66
	ALIGN_4

.L68:
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

	addps	%xmm3, %xmm4
	addps	%xmm5, %xmm4

	movhlps	%xmm4, %xmm5

#if defined(LN) || defined(LT)
	unpcklps %xmm6, %xmm4
	unpcklps %xmm7, %xmm5

	movaps	 %xmm4, %xmm6
	unpcklps %xmm5, %xmm4
	unpckhps %xmm5, %xmm6

	movsd	-32 * SIZE(BB), %xmm1
	movsd	-30 * SIZE(BB), %xmm3

	subps	%xmm4,  %xmm1
	subps	%xmm6,  %xmm3
#else
	movsd	-32 * SIZE(AA), %xmm0
	movsd	-30 * SIZE(AA), %xmm1

	subps	%xmm4, %xmm0
	subps	%xmm5, %xmm1
#endif

#ifdef LN
	movaps	-32 * SIZE(AA), %xmm4
	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm6, %xmm3
	pshufd	 $0xaa, %xmm4, %xmm6
	mulps	 %xmm3, %xmm6
	subps	 %xmm6, %xmm1

	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm6, %xmm1
#endif

#ifdef LT
	movaps	-32 * SIZE(AA), %xmm4
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm6, %xmm1
	pshufd	 $0x55, %xmm4, %xmm6
	mulps	 %xmm1, %xmm6
	subps	 %xmm6, %xmm3

	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm6, %xmm3
#endif

#ifdef RN
	movaps	-32 * SIZE(BB), %xmm6
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm7, %xmm0
	pshufd	 $0x55, %xmm6, %xmm7
	mulps	 %xmm0, %xmm7
	subps	 %xmm7, %xmm1

	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm7, %xmm1
#endif

#ifdef RT
	movaps	-32 * SIZE(BB), %xmm6
	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm7, %xmm1
	pshufd	 $0xaa, %xmm6, %xmm7
	mulps	 %xmm1, %xmm7
	subps	 %xmm7, %xmm0

	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm7, %xmm0
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm1, -32 * SIZE(BB)
	movlps	%xmm3, -30 * SIZE(BB)
#else
	movlps	%xmm0, -32 * SIZE(AA)
	movlps	%xmm1, -30 * SIZE(AA)
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm3, %xmm1

	movlps	%xmm1, 0 * SIZE(CO1)
	movhps	%xmm1, 0 * SIZE(CO1, LDC)
#else
	movlps	%xmm0, 0 * SIZE(CO1)
	movlps	%xmm1, 0 * SIZE(CO1, LDC)
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
	ALIGN_4

.L70:
	movl	M,  %ebx
	sarl	$2, %ebx	# i = (m >> 2)
	jle	.L79
	ALIGN_4

.L51:
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

	movl	B, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + BASE_SHIFT, %eax
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
#else
	pxor	%xmm4, %xmm4
	prefetcht0	3 * SIZE(CO1)
	pxor	%xmm5, %xmm5
	prefetcht0	3 * SIZE(CO1, LDC)
#endif
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

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

	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0x55, %xmm1, %xmm3
	mulps	%xmm0, %xmm3
	movaps	-28 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm6
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm7
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

	addps	%xmm2, %xmm6
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm7
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

	addps	%xmm2, %xmm6
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm7
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

	addps	%xmm2, %xmm6
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm7
	pshufd	$0xff, %xmm1, %xmm3
	movaps	-16 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movaps	  0 * SIZE(AA), %xmm0

	subl   $-32 * SIZE, AA
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
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L58
	ALIGN_4

.L56:
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
	jg	.L56
	ALIGN_4

.L58:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$4, %eax
#else
	subl	$2, %eax
#endif

	movl	AORIG, AA

	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(B,  %eax, 2), BB
#endif

	addps	%xmm6, %xmm4
	addps	%xmm7, %xmm5

	addps	%xmm2, %xmm4
	addps	%xmm3, %xmm5

#if defined(LN) || defined(LT)
	movaps	 %xmm4, %xmm0
	unpcklps %xmm6, %xmm4
	unpckhps %xmm6, %xmm0

	movaps	 %xmm5, %xmm1
	unpcklps %xmm7, %xmm5
	unpckhps %xmm7, %xmm1

	movaps	 %xmm4, %xmm6
	unpcklps %xmm5, %xmm4
	unpckhps %xmm5, %xmm6

	movaps	 %xmm0, %xmm2
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm2

	movsd	-32 * SIZE(BB), %xmm1
	movsd	-30 * SIZE(BB), %xmm3
	movsd	-28 * SIZE(BB), %xmm5
	movsd	-26 * SIZE(BB), %xmm7

	subps	%xmm4,  %xmm1
	subps	%xmm6,  %xmm3
	subps	%xmm0,  %xmm5
	subps	%xmm2,  %xmm7
#else
	movaps	-32 * SIZE(AA), %xmm0
	movaps	-28 * SIZE(AA), %xmm1

	subps	%xmm4, %xmm0
	subps	%xmm5, %xmm1
#endif

#ifdef LN
	movaps	-20 * SIZE(AA), %xmm4
	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm6, %xmm7
	pshufd	 $0xaa, %xmm4, %xmm6
	mulps	 %xmm7, %xmm6
	subps	 %xmm6, %xmm5
	pshufd	 $0x55, %xmm4, %xmm6
	mulps	 %xmm7, %xmm6
	subps	 %xmm6, %xmm3
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm7, %xmm6
	subps	 %xmm6, %xmm1

	movaps	-24 * SIZE(AA), %xmm4
	pshufd	 $0xaa, %xmm4, %xmm6
	mulps	 %xmm6, %xmm5
	pshufd	 $0x55, %xmm4, %xmm6
	mulps	 %xmm5, %xmm6
	subps	 %xmm6, %xmm3
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm5, %xmm6
	subps	 %xmm6, %xmm1

	movaps	-28 * SIZE(AA), %xmm4
	pshufd	 $0x55, %xmm4, %xmm6
	mulps	 %xmm6, %xmm3
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm3, %xmm6
	subps	 %xmm6, %xmm1

	movaps	-32 * SIZE(AA), %xmm4
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm6, %xmm1
#endif

#ifdef LT
	movaps	-32 * SIZE(AA), %xmm4
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm6, %xmm1

	pshufd	 $0x55, %xmm4, %xmm6
	mulps	 %xmm1, %xmm6
	subps	 %xmm6, %xmm3
	pshufd	 $0xaa, %xmm4, %xmm6
	mulps	 %xmm1, %xmm6
	subps	 %xmm6, %xmm5
	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm1, %xmm6
	subps	 %xmm6, %xmm7

	movaps	-28 * SIZE(AA), %xmm4
	pshufd	 $0x55, %xmm4, %xmm6
	mulps	 %xmm6, %xmm3
	pshufd	 $0xaa, %xmm4, %xmm6
	mulps	 %xmm3, %xmm6
	subps	 %xmm6, %xmm5
	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm3, %xmm6
	subps	 %xmm6, %xmm7

	movaps	-24 * SIZE(AA), %xmm4
	pshufd	 $0xaa, %xmm4, %xmm6
	mulps	 %xmm6, %xmm5
	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm5, %xmm6
	subps	 %xmm6, %xmm7

	movaps	-20 * SIZE(AA), %xmm4
	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm6, %xmm7
#endif

#ifdef RN
	movaps	-32 * SIZE(BB), %xmm6
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm7, %xmm0
	pshufd	 $0x55, %xmm6, %xmm7
	mulps	 %xmm0, %xmm7
	subps	 %xmm7, %xmm1

	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm7, %xmm1
#endif

#ifdef RT
	movaps	-32 * SIZE(BB), %xmm6
	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm7, %xmm1
	pshufd	 $0xaa, %xmm6, %xmm7
	mulps	 %xmm1, %xmm7
	subps	 %xmm7, %xmm0

	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm7, %xmm0
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm1, -32 * SIZE(BB)
	movlps	%xmm3, -30 * SIZE(BB)
	movlps	%xmm5, -28 * SIZE(BB)
	movlps	%xmm7, -26 * SIZE(BB)
#else
	movaps	%xmm0, -32 * SIZE(AA)
	movaps	%xmm1, -28 * SIZE(AA)
#endif

#ifdef LN
	subl	$4 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm5, %xmm1
	unpcklps %xmm7, %xmm3

	movaps	 %xmm1, %xmm2
	unpcklps %xmm3, %xmm1
	unpckhps %xmm3, %xmm2

	movlps	%xmm1, 0 * SIZE(CO1)
	movhps	%xmm1, 2 * SIZE(CO1)
	movlps	%xmm2, 0 * SIZE(CO1, LDC, 1)
	movhps	%xmm2, 2 * SIZE(CO1, LDC, 1)
#else
	movlps	%xmm0, 0 * SIZE(CO1)
	movhps	%xmm0, 2 * SIZE(CO1)
	movlps	%xmm1, 0 * SIZE(CO1, LDC, 1)
	movhps	%xmm1, 2 * SIZE(CO1, LDC, 1)
#endif

#ifndef LN
	addl	$4 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 2), BB
#endif

#ifdef LN
	subl	$4, KK
#endif

#ifdef LT
	addl	$4, KK
#endif

#ifdef RT
	movl	K, %eax
	sall	$2 + BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx			# i --
	jg	.L51
	ALIGN_4

.L79:
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

.L80:
	testl	$1, N
	je	.L999

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

	testl	$1, M
	je	.L100

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

	pxor	%xmm4, %xmm4
	movsd	-32 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movsd	-32 * SIZE(BB), %xmm1

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L115
	ALIGN_4

.L112:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	mulps	%xmm0, %xmm1
	movsd	-30 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm4
	movsd	-30 * SIZE(BB), %xmm1

	mulps	%xmm0, %xmm1
	movsd	-28 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm4
	movsd	-28 * SIZE(BB), %xmm1

	mulps	%xmm0, %xmm1
	movsd	-26 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm4
	movsd	-26 * SIZE(BB), %xmm1

	mulps	%xmm0, %xmm1
	movsd	-24 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm4
	movsd	-24 * SIZE(BB), %xmm1

	subl   $-8 * SIZE, AA
	subl   $-8 * SIZE, BB

	subl   $1, %eax
	jne    .L112
	ALIGN_4

.L115:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L118
	ALIGN_4

.L116:
	mulss	%xmm0, %xmm1
	movss	-31 * SIZE(AA), %xmm0
	addss	%xmm1, %xmm4
	movss	-31 * SIZE(BB), %xmm1

	addl	$1 * SIZE, AA
	addl	$1 * SIZE, BB
	decl	%eax
	jg	.L116
	ALIGN_4

.L118:
#if defined(LN) || defined(RT)
	movl	KK, %eax
	subl	$1, %eax

	movl	AORIG, AA

	leal	(AA, %eax, SIZE), AA
	leal	(B,  %eax, SIZE), BB
#endif

	haddps	%xmm4, %xmm4

#if defined(LN) || defined(LT)
	movss	-32 * SIZE(BB), %xmm1
	subss	%xmm4,  %xmm1
#else
	movss	-32 * SIZE(AA), %xmm0
	subss	%xmm4, %xmm0
#endif

#if defined(LN) || defined(LT)
	mulss	-32 * SIZE(AA), %xmm1
#endif

#if defined(RN) || defined(RT)
	mulss	-32 * SIZE(BB), %xmm0
#endif

#if defined(LN) || defined(LT)
	movss	%xmm1, -32 * SIZE(BB)
#else
	movss	%xmm0, -32 * SIZE(AA)
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
	leal	(BB, %eax, SIZE), BB
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

.L100:
	testl	$2, M
	je	.L110

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

	movsd	-32 * SIZE(AA), %xmm0
	pxor	%xmm3, %xmm3
	movsd	-32 * SIZE(BB), %xmm1
	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L105
	ALIGN_4

.L102:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	movsd	-30 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4

	pshufd	$0x55, %xmm1, %xmm2
	movsd	-30 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movsd	-28 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5

	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	movsd	-26 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4

	pshufd	$0x55, %xmm1, %xmm2
	movsd	-28 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movsd	-24 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5

	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	movsd	-22 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4

	pshufd	$0x55, %xmm1, %xmm2
	movsd	-26 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movsd	-20 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5

	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	movsd	-18 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4

	pshufd	$0x55, %xmm1, %xmm2
	movsd	-24 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movsd	-16 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5

	subl   $-16 * SIZE, AA
	subl   $ -8 * SIZE, BB

	subl   $1, %eax
	jne    .L102
	ALIGN_4

.L105:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L108
	ALIGN_4

.L106:
	pshufd	$0x00, %xmm1, %xmm2
	movss	-31 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movsd	-30 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4

	addl	$2 * SIZE, AA
	addl	$1 * SIZE, BB
	decl	%eax
	jg	.L106
	ALIGN_4

.L108:
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

	addps	%xmm5, %xmm4

#if defined(LN) || defined(LT)
	pshufd	$1, %xmm4, %xmm6

	movss	-32 * SIZE(BB), %xmm1
	movss	-31 * SIZE(BB), %xmm3

	subss	%xmm4,  %xmm1
	subss	%xmm6,  %xmm3
#else
	movsd	-32 * SIZE(AA), %xmm0

	subps	%xmm4, %xmm0
#endif

#ifdef LN
	movsd	-32 * SIZE(AA), %xmm4
	movhps	-30 * SIZE(AA), %xmm4
	pshufd	 $0xff, %xmm4, %xmm6
	mulss	 %xmm6, %xmm3
	pshufd	 $0xaa, %xmm4, %xmm6
	mulss	 %xmm3, %xmm6
	subss	 %xmm6, %xmm1

	pshufd	 $0x00, %xmm4, %xmm6
	mulss	 %xmm6, %xmm1
#endif

#ifdef LT
	movaps	-32 * SIZE(AA), %xmm4
	pshufd	 $0x00, %xmm4, %xmm6
	mulss	 %xmm6, %xmm1
	pshufd	 $0x55, %xmm4, %xmm6
	mulss	 %xmm1, %xmm6
	subss	 %xmm6, %xmm3

	pshufd	 $0xff, %xmm4, %xmm6
	mulss	 %xmm6, %xmm3
#endif

#if defined(RN) || defined(RT)
	movss	-32 * SIZE(BB), %xmm6
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm7, %xmm0
#endif

#if defined(LN) || defined(LT)
	movss	%xmm1, -32 * SIZE(BB)
	movss	%xmm3, -31 * SIZE(BB)
#else
	movlps	%xmm0, -32 * SIZE(AA)
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movss	%xmm1, 0 * SIZE(CO1)
	movss	%xmm3, 1 * SIZE(CO1)
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
	sall	$1 + BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif
	ALIGN_4

.L110:
	movl	M,  %ebx
	sarl	$2, %ebx	# i = (m >> 2)
	jle	.L119
	ALIGN_4

.L91:
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

	movl	B, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$BASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movaps	-32 * SIZE(AA), %xmm0
	pxor	%xmm2, %xmm2
	movsd	-32 * SIZE(BB), %xmm1

	pxor	%xmm4, %xmm4
#ifdef LN
	prefetcht0	-4 * SIZE(CO1)
#else
	prefetcht0	 3 * SIZE(CO1)
#endif
	pxor	%xmm5, %xmm5

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L95
	ALIGN_4

.L92:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm5
	pshufd	$0x55, %xmm1, %xmm2
	movsd	-30 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-24 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	movaps	-20 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm5
	pshufd	$0x55, %xmm1, %xmm2
	movsd	-28 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-16 * SIZE(AA), %xmm0

	PREFETCH (PREFETCHSIZE + 16) * SIZE(AA)

	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	movaps	-12 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm5
	pshufd	$0x55, %xmm1, %xmm2
	movsd	-26 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	 -8 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	movaps	 -4 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm5
	pshufd	$0x55, %xmm1, %xmm2
	movsd	-24 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	  0 * SIZE(AA), %xmm0

	subl   $-32 * SIZE, AA
	subl   $ -8 * SIZE, BB

	subl   $1, %eax
	jne    .L92
	ALIGN_4

.L95:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L98
	ALIGN_4

.L96:
	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	movss	-31 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(AA), %xmm0

	addl	$4 * SIZE, AA
	addl	$1 * SIZE, BB
	decl	%eax
	jg	.L96
	ALIGN_4

.L98:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$4, %eax
#else
	subl	$1, %eax
#endif

	movl	AORIG, AA

	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(B,  %eax, 1), BB
#endif

	addps	%xmm2, %xmm4
	addps	%xmm5, %xmm4

#if defined(LN) || defined(LT)
	movaps	 %xmm4, %xmm0
	unpcklps %xmm6, %xmm4
	unpckhps %xmm6, %xmm0

	movaps	 %xmm5, %xmm1
	unpcklps %xmm7, %xmm5
	unpckhps %xmm7, %xmm1

	movaps	 %xmm4, %xmm6
	unpcklps %xmm5, %xmm4
	unpckhps %xmm5, %xmm6

	movaps	 %xmm0, %xmm2
	unpcklps %xmm1, %xmm0
	unpckhps %xmm1, %xmm2

	movss	-32 * SIZE(BB), %xmm1
	movss	-31 * SIZE(BB), %xmm3
	movss	-30 * SIZE(BB), %xmm5
	movss	-29 * SIZE(BB), %xmm7

	subss	%xmm4,  %xmm1
	subss	%xmm6,  %xmm3
	subss	%xmm0,  %xmm5
	subss	%xmm2,  %xmm7
#else
	movaps	-32 * SIZE(AA), %xmm0

	subps	%xmm4, %xmm0
#endif

#ifdef LN
	movaps	-20 * SIZE(AA), %xmm4
	pshufd	 $0xff, %xmm4, %xmm6
	mulss	 %xmm6, %xmm7
	pshufd	 $0xaa, %xmm4, %xmm6
	mulss	 %xmm7, %xmm6
	subss	 %xmm6, %xmm5
	pshufd	 $0x55, %xmm4, %xmm6
	mulss	 %xmm7, %xmm6
	subss	 %xmm6, %xmm3
	pshufd	 $0x00, %xmm4, %xmm6
	mulss	 %xmm7, %xmm6
	subss	 %xmm6, %xmm1

	movaps	 -24 * SIZE(AA), %xmm4
	pshufd	 $0xaa, %xmm4, %xmm6
	mulss	 %xmm6, %xmm5
	pshufd	 $0x55, %xmm4, %xmm6
	mulss	 %xmm5, %xmm6
	subss	 %xmm6, %xmm3
	pshufd	 $0x00, %xmm4, %xmm6
	mulss	 %xmm5, %xmm6
	subss	 %xmm6, %xmm1

	movaps	-28 * SIZE(AA), %xmm4
	pshufd	 $0x55, %xmm4, %xmm6
	mulss	 %xmm6, %xmm3
	pshufd	 $0x00, %xmm4, %xmm6
	mulss	 %xmm3, %xmm6
	subss	 %xmm6, %xmm1

	movaps	-32 * SIZE(AA), %xmm4
	pshufd	 $0x00, %xmm4, %xmm6
	mulss	 %xmm6, %xmm1
#endif

#ifdef LT
	movaps	-32 * SIZE(AA), %xmm4
	pshufd	 $0x00, %xmm4, %xmm6
	mulss	 %xmm6, %xmm1

	pshufd	 $0x55, %xmm4, %xmm6
	mulss	 %xmm1, %xmm6
	subss	 %xmm6, %xmm3
	pshufd	 $0xaa, %xmm4, %xmm6
	mulss	 %xmm1, %xmm6
	subss	 %xmm6, %xmm5
	pshufd	 $0xff, %xmm4, %xmm6
	mulss	 %xmm1, %xmm6
	subss	 %xmm6, %xmm7

	movaps	-28 * SIZE(AA), %xmm4
	pshufd	 $0x55, %xmm4, %xmm6
	mulss	 %xmm6, %xmm3
	pshufd	 $0xaa, %xmm4, %xmm6
	mulss	 %xmm3, %xmm6
	subss	 %xmm6, %xmm5
	pshufd	 $0xff, %xmm4, %xmm6
	mulss	 %xmm3, %xmm6
	subss	 %xmm6, %xmm7

	movaps	-24 * SIZE(AA), %xmm4
	pshufd	 $0xaa, %xmm4, %xmm6
	mulss	 %xmm6, %xmm5
	pshufd	 $0xff, %xmm4, %xmm6
	mulss	 %xmm5, %xmm6
	subss	 %xmm6, %xmm7

	movaps	-20 * SIZE(AA), %xmm4
	pshufd	 $0xff, %xmm4, %xmm6
	mulss	 %xmm6, %xmm7
#endif

#if defined(RN) || defined(RT)
	movss	-32 * SIZE(BB), %xmm6
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm7, %xmm0
#endif

#if defined(LN) || defined(LT)
	movss	%xmm1, -32 * SIZE(BB)
	movss	%xmm3, -31 * SIZE(BB)
	movss	%xmm5, -30 * SIZE(BB)
	movss	%xmm7, -29 * SIZE(BB)
#else
	movaps	%xmm0, -32 * SIZE(AA)
#endif

#ifdef LN
	subl	$4 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm5, %xmm1
	unpcklps %xmm7, %xmm3

	unpcklps %xmm3, %xmm1

	movlps	%xmm1, 0 * SIZE(CO1)
	movhps	%xmm1, 2 * SIZE(CO1)
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
	leal	(BB, %eax, 1), BB
#endif

#ifdef LN
	subl	$4, KK
#endif

#ifdef LT
	addl	$4, KK
#endif

#ifdef RT
	movl	K, %eax
	sall	$2 + BASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx			# i --
	jg	.L91
	ALIGN_4

.L119:
#ifdef LN
       movl	K, %eax
       leal 	(B, %eax, SIZE), B
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
