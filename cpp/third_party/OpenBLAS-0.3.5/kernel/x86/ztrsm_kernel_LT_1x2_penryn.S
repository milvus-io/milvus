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
#define ALPHA_R	16 + STACK + ARGS(%esp)
#define ALPHA_I	24 + STACK + ARGS(%esp)
#define A	32 + STACK + ARGS(%esp)
#define ARG_B	36 + STACK + ARGS(%esp)
#define C	40 + STACK + ARGS(%esp)
#define ARG_LDC	44 + STACK + ARGS(%esp)
#define OFFSET	48 + STACK + ARGS(%esp)

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

#ifdef NANO
#define PREFETCH	prefetcht0
#define PREFETCHSIZE (8 * 2)
#endif

#define AA	%edx
#define BB	%ecx
#define LDC	%ebp
#define B	%edi
#define CO1	%esi

#define ADD1	  addpd
#define ADD2	  addpd

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

	subl	$-16 * SIZE, A
	subl	$-16 * SIZE, B

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

#ifdef RT
       movl	N, %eax
       subl	OFFSET, %eax
       movl	%eax, KK
#endif

	movl	N, %eax
	sarl	$1, %eax
	movl	%eax, J			# j = n
	jle	.L100
	ALIGN_4

.L01:
#if defined(LT) || defined(RN)
	movl	A, AA
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
	ALIGN_4

.L10:
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

	movaps	-16 * SIZE(AA), %xmm0
	pxor	%xmm2, %xmm2
	movaps	-16 * SIZE(BB), %xmm1
	pxor	%xmm3, %xmm3

#ifdef LN
	pxor	%xmm4, %xmm4
	prefetcht0     -2 * SIZE(CO1)
	pxor	%xmm5, %xmm5
	prefetcht0     -2 * SIZE(CO1, LDC)
#else
	pxor	%xmm4, %xmm4
	prefetcht0	1 * SIZE(CO1)
	pxor	%xmm5, %xmm5
	prefetcht0	1 * SIZE(CO1, LDC)
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
	je	.L15
	ALIGN_4

.L12:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	ADD1	%xmm3, %xmm6
	movaps	-14 * SIZE(BB), %xmm3
	ADD2	%xmm2, %xmm7
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm1, %xmm4
	movaps	-12 * SIZE(BB), %xmm1
	ADD2	%xmm2, %xmm5
	pshufd	$0x4e, %xmm3, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm2
	movaps	-14 * SIZE(AA), %xmm0

	ADD1	%xmm3, %xmm6
	movaps	-10 * SIZE(BB), %xmm3
	ADD2	%xmm2, %xmm7
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm1, %xmm4
	movaps	 -8 * SIZE(BB), %xmm1
	ADD2	%xmm2, %xmm5
	pshufd	$0x4e, %xmm3, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm2
	movaps	-12 * SIZE(AA), %xmm0

	ADD1	%xmm3, %xmm6
	movaps	 -6 * SIZE(BB), %xmm3
	ADD2	%xmm2, %xmm7
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm1, %xmm4
	movaps	 -4 * SIZE(BB), %xmm1
	ADD2	%xmm2, %xmm5
	pshufd	$0x4e, %xmm3, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm2
	movaps	-10 * SIZE(AA), %xmm0

	ADD1	%xmm3, %xmm6
	movaps	 -2 * SIZE(BB), %xmm3
	ADD2	%xmm2, %xmm7
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm1, %xmm4
	movaps	  0 * SIZE(BB), %xmm1
	ADD2	%xmm2, %xmm5
	pshufd	$0x4e, %xmm3, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm2
	movaps	 -8 * SIZE(AA), %xmm0

	PREFETCH (PREFETCHSIZE +  8) * SIZE(AA)

	ADD1	%xmm3, %xmm6
	movaps	  2 * SIZE(BB), %xmm3
	ADD2	%xmm2, %xmm7
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm1, %xmm4
	movaps	  4 * SIZE(BB), %xmm1
	ADD2	%xmm2, %xmm5
	pshufd	$0x4e, %xmm3, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm2
	movaps	 -6 * SIZE(AA), %xmm0

	ADD1	%xmm3, %xmm6
	movaps	  6 * SIZE(BB), %xmm3
	ADD2	%xmm2, %xmm7
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm1, %xmm4
	movaps	  8 * SIZE(BB), %xmm1
	ADD2	%xmm2, %xmm5
	pshufd	$0x4e, %xmm3, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm2
	movaps	 -4 * SIZE(AA), %xmm0

	ADD1	%xmm3, %xmm6
	movaps	 10 * SIZE(BB), %xmm3
	ADD2	%xmm2, %xmm7
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm1, %xmm4
	movaps	 12 * SIZE(BB), %xmm1
	ADD2	%xmm2, %xmm5
	pshufd	$0x4e, %xmm3, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm2
	movaps	 -2 * SIZE(AA), %xmm0

	ADD1	%xmm3, %xmm6
	movaps	 14 * SIZE(BB), %xmm3
	ADD2	%xmm2, %xmm7
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm1, %xmm4
	movaps	 16 * SIZE(BB), %xmm1
	ADD2	%xmm2, %xmm5
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
	ALIGN_4

.L16:
	ADD1	%xmm3, %xmm6
	movaps	-14 * SIZE(BB), %xmm3
	ADD2	%xmm2, %xmm7
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2

	ADD1	%xmm1, %xmm4
	movaps	-12 * SIZE(BB), %xmm1
	ADD2	%xmm2, %xmm5
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
	subl	$1, %eax
#else
	subl	$2, %eax
#endif

	movl	AORIG, AA
	sall	$ZBASE_SHIFT, %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 2), BB
#endif

	ADD1	%xmm3, %xmm6
	pcmpeqb	%xmm1, %xmm1
	ADD2	%xmm2, %xmm7
	psllq	$63,   %xmm1

#ifndef CONJ
	pshufd	$0x40, %xmm1, %xmm0
	shufps	$0x04, %xmm1, %xmm1

	pxor	%xmm0, %xmm4
	pxor	%xmm0, %xmm6
#else
#if defined(LN) || defined(LT)
	pshufd	$0x40, %xmm1, %xmm0
#else
	pshufd	$0x04, %xmm1, %xmm0
#endif
	shufps	$0x40, %xmm1, %xmm1

	pxor	%xmm0, %xmm5
	pxor	%xmm0, %xmm7
#endif

	haddpd	%xmm5, %xmm4
	haddpd	%xmm7, %xmm6


#if defined(LN) || defined(LT)
	movapd	-16 * SIZE(BB), %xmm5
	movapd	-14 * SIZE(BB), %xmm7

	subpd	%xmm4,  %xmm5
	subpd	%xmm6,  %xmm7
#else
	movapd	-16 * SIZE(AA), %xmm5
	movapd	-14 * SIZE(AA), %xmm7

	subpd	%xmm4,  %xmm5
	subpd	%xmm6,  %xmm7
#endif

#if defined(LN) || defined(LT)
	movddup	-16 * SIZE(AA), %xmm2
	movddup	-15 * SIZE(AA), %xmm3

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

#ifdef RN
	movddup	-16 * SIZE(BB), %xmm2
	movddup	-15 * SIZE(BB), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5

	movddup	-14 * SIZE(BB), %xmm2
	movddup -13 * SIZE(BB), %xmm3

	movapd	 %xmm5, %xmm4
	pshufd	$0x4e, %xmm5, %xmm6

	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm4
	mulpd	 %xmm3, %xmm6

	subpd	 %xmm4, %xmm7
	subpd	 %xmm6, %xmm7

	movddup	-10 * SIZE(BB), %xmm2
	movddup	 -9 * SIZE(BB), %xmm3

	pshufd	$0x4e, %xmm7, %xmm6

	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm7
	mulpd	 %xmm3, %xmm6

	addpd	 %xmm6, %xmm7
#endif

#ifdef RT
	movddup	-10 * SIZE(BB), %xmm2
	movddup	 -9 * SIZE(BB), %xmm3

	pshufd	$0x4e, %xmm7, %xmm6

	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm7
	mulpd	 %xmm3, %xmm6

	addpd	 %xmm6, %xmm7

	movddup	-12 * SIZE(BB), %xmm2
	movddup	-11 * SIZE(BB), %xmm3

	movapd	 %xmm7, %xmm4
	pshufd	$0x4e, %xmm7, %xmm6

	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm4
	mulpd	 %xmm3, %xmm6

	subpd	 %xmm4, %xmm5
	subpd	 %xmm6, %xmm5

	movddup	-16 * SIZE(BB), %xmm2
	movddup	-15 * SIZE(BB), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

	movlpd	%xmm5,   0 * SIZE(CO1)
	movhpd	%xmm5,   1 * SIZE(CO1)

	movlpd	%xmm7,   0 * SIZE(CO1, LDC)
	movhpd	%xmm7,   1 * SIZE(CO1, LDC)

#if defined(LN) || defined(LT)
	movapd	%xmm5,  -16 * SIZE(BB)
	movapd	%xmm7,  -14 * SIZE(BB)
#else
	movapd	%xmm5,  -16 * SIZE(AA)
	movapd	%xmm7,  -14 * SIZE(AA)
#endif

#ifndef LN
	addl	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AA
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

	decl	%ebx			# i --
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
	testl	$1, %eax
	jle	.L999

#if defined(LT) || defined(RN)
	movl	A, AA
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
	ALIGN_4

L110:
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

	movaps	-16 * SIZE(AA), %xmm0
	pxor	%xmm2, %xmm2
	movaps	-16 * SIZE(BB), %xmm1
	pxor	%xmm3, %xmm3

	pxor	%xmm4, %xmm4
#ifdef LN
	prefetcht0	-2 * SIZE(CO1)
#else
	prefetcht0	 1 * SIZE(CO1)
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
	je	L115
	ALIGN_4

L112:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-14 * SIZE(AA), %xmm0

	ADD1	%xmm1, %xmm4
	movaps	-14 * SIZE(BB), %xmm1
	ADD2	%xmm2, %xmm5

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-12 * SIZE(AA), %xmm0

	ADD1	%xmm1, %xmm6
	movaps	-12 * SIZE(BB), %xmm1
	ADD2	%xmm2, %xmm7

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-10 * SIZE(AA), %xmm0

	ADD1	%xmm1, %xmm4
	movaps	-10 * SIZE(BB), %xmm1
	ADD2	%xmm2, %xmm5

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	 -8 * SIZE(AA), %xmm0

	ADD1	%xmm1, %xmm6
	movaps	 -8 * SIZE(BB), %xmm1
	ADD2	%xmm2, %xmm7

	PREFETCH (PREFETCHSIZE +  8) * SIZE(AA)

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	 -6 * SIZE(AA), %xmm0

	ADD1	%xmm1, %xmm4
	movaps	 -6 * SIZE(BB), %xmm1
	ADD2	%xmm2, %xmm5

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	 -4 * SIZE(AA), %xmm0

	ADD1	%xmm1, %xmm6
	movaps	 -4 * SIZE(BB), %xmm1
	ADD2	%xmm2, %xmm7

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	 -2 * SIZE(AA), %xmm0

	ADD1	%xmm1, %xmm4
	movaps	 -2 * SIZE(BB), %xmm1
	ADD2	%xmm2, %xmm5

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	  0 * SIZE(AA), %xmm0

	ADD1	%xmm1, %xmm6
	movaps	  0 * SIZE(BB), %xmm1
	ADD2	%xmm2, %xmm7

	subl   $-16 * SIZE, AA
	subl   $-16 * SIZE, BB

	subl   $1, %eax
	jne    L112
	ALIGN_4

L115:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je L118
	ALIGN_4

L116:
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-14 * SIZE(AA), %xmm0

	ADD1	%xmm1, %xmm4
	movaps	-14 * SIZE(BB), %xmm1
	ADD2	%xmm2, %xmm5

	addl	$2 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	L116
	ALIGN_4

L118:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$1, %eax
#endif

	movl	AORIG, AA
	sall	$ZBASE_SHIFT, %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 1), BB
#endif

	addpd	%xmm6, %xmm4
	pcmpeqb	%xmm1, %xmm1
	addpd	%xmm7, %xmm5
	psllq	$63,   %xmm1

#ifndef CONJ
	pshufd	$0x40, %xmm1, %xmm0
	shufps	$0x04, %xmm1, %xmm1

	pxor	%xmm0, %xmm4
#else
#if defined(LN) || defined(LT)
	pshufd	$0x40, %xmm1, %xmm0
#else
	pshufd	$0x04, %xmm1, %xmm0
#endif
	shufps	$0x40, %xmm1, %xmm1

	pxor	%xmm0, %xmm5
#endif

	haddpd	%xmm5, %xmm4

#if defined(LN) || defined(LT)
	movapd	-16 * SIZE(BB), %xmm5
	subpd	%xmm4,  %xmm5
#else
	movapd	-16 * SIZE(AA), %xmm5
	subpd	%xmm4,  %xmm5
#endif

#if defined(LN) || defined(LT)
	movddup	-16 * SIZE(AA), %xmm2
	movddup	-15 * SIZE(AA), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5
#endif

#if defined(RN) || defined(RT)
	movddup	-16 * SIZE(BB), %xmm2
	movddup	-15 * SIZE(BB), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5
#endif

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

	movlpd	%xmm5,   0 * SIZE(CO1)
	movhpd	%xmm5,   1 * SIZE(CO1)

#if defined(LN) || defined(LT)
	movapd	%xmm5, -16 * SIZE(BB)
#else
	movapd	%xmm5, -16 * SIZE(AA)
#endif

#ifndef LN
	addl	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
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
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx			# i --
	jg	L110

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
