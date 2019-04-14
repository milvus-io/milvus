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

#define ADD1	  addpd
#define ADD2	  addpd

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

	subl	$-16 * SIZE, A
	subl	$-16 * SIZE, B

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

	leal	16 * SIZE + BUFFER, BB

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
	movddup	 -16 * SIZE(B), %xmm0
	movddup	 -15 * SIZE(B), %xmm1
	movddup	 -14 * SIZE(B), %xmm2
	movddup	 -13 * SIZE(B), %xmm3
	movddup	 -12 * SIZE(B), %xmm4
	movddup	 -11 * SIZE(B), %xmm5
	movddup	 -10 * SIZE(B), %xmm6
	movddup	  -9 * SIZE(B), %xmm7

	movapd	%xmm0,  -16 * SIZE(BB)
	movapd	%xmm1,  -14 * SIZE(BB)
	movapd	%xmm2,  -12 * SIZE(BB)
	movapd	%xmm3,  -10 * SIZE(BB)
	movapd	%xmm4,   -8 * SIZE(BB)
	movapd	%xmm5,   -6 * SIZE(BB)
	movapd	%xmm6,   -4 * SIZE(BB)
	movapd	%xmm7,   -2 * SIZE(BB)

	addl	$  8 * SIZE, B
	subl	$-16 * SIZE, BB
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
	movddup	 -16 * SIZE(B), %xmm0
	movddup	 -15 * SIZE(B), %xmm1

	movapd	%xmm0,  -16 * SIZE(BB)
	movapd	%xmm1,  -14 * SIZE(BB)

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

	leal	16 * SIZE + BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + ZBASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movapd	-16 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movapd	-16 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
	movapd	 -8 * SIZE(AA), %xmm3
	pxor	%xmm6, %xmm6
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
	sarl	$3, %eax
	je	.L15
	ALIGN_4

.L12:
	movapd	%xmm1,  %xmm2
	mulpd	%xmm0,  %xmm1
	ADD1	%xmm1,  %xmm4
	movapd	-14 * SIZE(BB), %xmm1
	mulpd	%xmm1,  %xmm0
	ADD2	%xmm0,  %xmm5
	movapd	-14 * SIZE(AA), %xmm0
	mulpd	%xmm0,  %xmm2
	mulpd	%xmm0,  %xmm1
	movapd	-12 * SIZE(AA), %xmm0
	ADD1	%xmm2,  %xmm6
	ADD2	%xmm1,  %xmm7

	movapd	-12 * SIZE(BB), %xmm1
	movapd	%xmm1,  %xmm2
	mulpd	%xmm0,  %xmm1
	ADD1	%xmm1,  %xmm4
	movapd	-10 * SIZE(BB), %xmm1
	mulpd	%xmm1,  %xmm0
	ADD2	%xmm0,  %xmm5
	movapd	-10 * SIZE(AA), %xmm0
	mulpd	%xmm0,  %xmm2
	mulpd	%xmm0,  %xmm1
	movapd	  0 * SIZE(AA), %xmm0
	ADD1	%xmm2,  %xmm6
	ADD2	%xmm1,  %xmm7

	movapd	 -8 * SIZE(BB), %xmm1
	movapd	%xmm1,  %xmm2
	mulpd	%xmm3,  %xmm1
	ADD1	%xmm1,  %xmm4
	movapd	 -6 * SIZE(BB), %xmm1
	mulpd	%xmm1,  %xmm3
	ADD2	%xmm3,  %xmm5
	movapd	 -6 * SIZE(AA), %xmm3
	mulpd	%xmm3,  %xmm2
	mulpd	%xmm3,  %xmm1
	movapd	 -4 * SIZE(AA), %xmm3
	ADD1	%xmm2,  %xmm6
	ADD2	%xmm1,  %xmm7

	movapd	 -4 * SIZE(BB), %xmm1
	movapd	%xmm1,  %xmm2
	mulpd	%xmm3,  %xmm1
	ADD1	%xmm1,  %xmm4
	movapd	 -2 * SIZE(BB), %xmm1
	mulpd	%xmm1,  %xmm3
	ADD2	%xmm3,  %xmm5
	movapd	 -2 * SIZE(AA), %xmm3
	mulpd	%xmm3,  %xmm2
	mulpd	%xmm3,  %xmm1
	movapd	  8 * SIZE(AA), %xmm3
	ADD1	%xmm2,  %xmm6
	ADD2	%xmm1,  %xmm7
	movapd	  0 * SIZE(BB), %xmm1

	movapd	%xmm1,  %xmm2
	mulpd	%xmm0,  %xmm1
	ADD1	%xmm1,  %xmm4
	movapd	  2 * SIZE(BB), %xmm1
	mulpd	%xmm1,  %xmm0
	ADD2	%xmm0,  %xmm5
	movapd	  2 * SIZE(AA), %xmm0
	mulpd	%xmm0,  %xmm2
	mulpd	%xmm0,  %xmm1
	movapd	  4 * SIZE(AA), %xmm0
	ADD1	%xmm2,  %xmm6
	ADD2	%xmm1,  %xmm7

	movapd	  4 * SIZE(BB), %xmm1
	movapd	%xmm1,  %xmm2
	mulpd	%xmm0,  %xmm1
	ADD1	%xmm1,  %xmm4
	movapd	  6 * SIZE(BB), %xmm1
	mulpd	%xmm1,  %xmm0
	ADD2	%xmm0,  %xmm5
	movapd	  6 * SIZE(AA), %xmm0
	mulpd	%xmm0,  %xmm2
	mulpd	%xmm0,  %xmm1
	movapd	 16 * SIZE(AA), %xmm0
	ADD1	%xmm2,  %xmm6
	ADD2	%xmm1,  %xmm7

	movapd	  8 * SIZE(BB), %xmm1
	movapd	%xmm1,  %xmm2
	mulpd	%xmm3,  %xmm1
	ADD1	%xmm1,  %xmm4
	movapd	 10 * SIZE(BB), %xmm1
	mulpd	%xmm1,  %xmm3
	ADD2	%xmm3,  %xmm5
	movapd	 10 * SIZE(AA), %xmm3
	mulpd	%xmm3,  %xmm2
	mulpd	%xmm3,  %xmm1
	ADD1	%xmm2,  %xmm6
	movapd	 12 * SIZE(AA), %xmm3
	ADD2	%xmm1,  %xmm7

	movapd	 12 * SIZE(BB), %xmm1
	movapd	%xmm1,  %xmm2
	mulpd	%xmm3,  %xmm1
	ADD1	%xmm1,  %xmm4
	movapd	 14 * SIZE(BB), %xmm1
	mulpd	%xmm1,  %xmm3
	ADD2	%xmm3,  %xmm5
	movapd	 14 * SIZE(AA), %xmm3
	mulpd	%xmm3,  %xmm2
	mulpd	%xmm3,  %xmm1
	subl   $-32 * SIZE, BB
	movapd	 24 * SIZE(AA), %xmm3
	subl   $-32 * SIZE, AA
	ADD1	%xmm2,  %xmm6
	ADD2	%xmm1,  %xmm7
	movapd	-16 * SIZE(BB), %xmm1

	decl   %eax
	jne    .L12
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

.L16:
	movapd	%xmm1, %xmm2
	mulpd	%xmm0,  %xmm1
	ADD1	%xmm1, %xmm4
	movapd	-14 * SIZE(BB), %xmm1
	movapd	%xmm1, %xmm3
	mulpd	%xmm0,  %xmm1
	movapd	-14 * SIZE(AA), %xmm0
	ADD2	%xmm1, %xmm5
	movapd	-12 * SIZE(BB), %xmm1
	mulpd	%xmm0,  %xmm2
	ADD1	%xmm2, %xmm6
	mulpd	%xmm0,  %xmm3
	movapd	-12 * SIZE(AA), %xmm0
	ADD2	%xmm3, %xmm7

	addl	$4 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L16
	ALIGN_4

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
	leal	16 * SIZE + BUFFER, BB

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
	movapd	-16 * SIZE(B), %xmm5
	movapd	-14 * SIZE(B), %xmm7

	subpd	%xmm4,  %xmm5
	subpd	%xmm6,  %xmm7
#else
	movapd	-16 * SIZE(AA), %xmm5
	movapd	-14 * SIZE(AA), %xmm7

	subpd	%xmm4,  %xmm5
	subpd	%xmm6,  %xmm7
#endif

#ifndef CONJ
	SHUFPD_1 %xmm1, %xmm1
#endif

#ifdef LN
	movddup	-10 * SIZE(AA), %xmm2
	movddup	 -9 * SIZE(AA), %xmm3

	pshufd	$0x4e, %xmm7, %xmm6

	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm7
	mulpd	 %xmm3, %xmm6

	addpd	 %xmm6, %xmm7

	movddup	-12 * SIZE(AA), %xmm2
	movddup	-11 * SIZE(AA), %xmm3

	movapd	 %xmm7, %xmm4
	pshufd	 $0x4e, %xmm7, %xmm6

	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm4
	mulpd	 %xmm3, %xmm6

	subpd	 %xmm4, %xmm5
	subpd	 %xmm6, %xmm5

	movddup	-16 * SIZE(AA), %xmm2
	movddup	-15 * SIZE(AA), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5
#endif

#ifdef LT
	movddup	-16 * SIZE(AA), %xmm2
	movddup	-15 * SIZE(AA), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5

	movddup	-14 * SIZE(AA), %xmm2
	movddup	-13 * SIZE(AA), %xmm3

	movapd	 %xmm5, %xmm4
	pshufd	 $0x4e, %xmm5, %xmm6

	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm4
	mulpd	 %xmm3, %xmm6

	subpd	 %xmm4, %xmm7
	subpd	 %xmm6, %xmm7

	movddup	-10 * SIZE(AA), %xmm2
	movddup	 -9 * SIZE(AA), %xmm3

	pshufd	$0x4e, %xmm7, %xmm6

	xorpd	 %xmm1, %xmm6

	mulpd	 %xmm2, %xmm7
	mulpd	 %xmm3, %xmm6

	addpd	 %xmm6, %xmm7
#endif

#ifdef RN
	movddup	-16 * SIZE(B), %xmm2
	movddup	-15 * SIZE(B), %xmm3

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
	movddup	-16 * SIZE(B), %xmm2
	movddup	-15 * SIZE(B), %xmm3

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
	movapd	%xmm5, -16 * SIZE(B)
	movapd	%xmm7, -14 * SIZE(B)

	movddup	%xmm5, %xmm4
	unpckhpd %xmm5, %xmm5
	movddup	%xmm7, %xmm6
	unpckhpd %xmm7, %xmm7

	movapd	%xmm4, -16 * SIZE(BB)
	movapd	%xmm5, -14 * SIZE(BB)
	movapd	%xmm6, -12 * SIZE(BB)
	movapd	%xmm7, -10 * SIZE(BB)
#else
	movapd	%xmm5, -16 * SIZE(AA)
	movapd	%xmm7, -14 * SIZE(AA)

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

	leal	16 * SIZE + BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + ZBASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movapd	-16 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movapd	-16 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
	movapd	 -8 * SIZE(AA), %xmm2
	pxor	%xmm6, %xmm6
	movapd	 -8 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	jle	.L52

.L51:
	mulpd	%xmm0,  %xmm1
	mulpd	-14 * SIZE(BB), %xmm0
	ADD1	%xmm1, %xmm4
	movapd	-12 * SIZE(BB), %xmm1
	ADD2	%xmm0, %xmm5
	movapd	-14 * SIZE(AA), %xmm0
	mulpd	%xmm0,  %xmm1
	mulpd	-10 * SIZE(BB), %xmm0
	ADD1	%xmm1, %xmm6
	movapd	  0 * SIZE(BB), %xmm1
	ADD2	%xmm0, %xmm7
	movapd	-12 * SIZE(AA), %xmm0
	mulpd	%xmm0,  %xmm3
	mulpd	 -6 * SIZE(BB), %xmm0
	ADD1	%xmm3, %xmm4
	movapd	 -4 * SIZE(BB), %xmm3
	ADD2	%xmm0, %xmm5
	movapd	-10 * SIZE(AA), %xmm0
	mulpd	%xmm0,  %xmm3
	mulpd	 -2 * SIZE(BB), %xmm0
	ADD1	%xmm3, %xmm6
	movapd	  8 * SIZE(BB), %xmm3
	ADD2	%xmm0, %xmm7
	movapd	  0 * SIZE(AA), %xmm0
	mulpd	%xmm2,  %xmm1
	mulpd	  2 * SIZE(BB), %xmm2
	ADD1	%xmm1, %xmm4
	movapd	  4 * SIZE(BB), %xmm1
	ADD2	%xmm2, %xmm5
	movapd	 -6 * SIZE(AA), %xmm2
	mulpd	%xmm2,  %xmm1
	mulpd	  6 * SIZE(BB), %xmm2
	ADD1	%xmm1, %xmm6
	movapd	 16 * SIZE(BB), %xmm1
	ADD2	%xmm2, %xmm7
	movapd	 -4 * SIZE(AA), %xmm2
	mulpd	%xmm2,  %xmm3
	mulpd	 10 * SIZE(BB), %xmm2
	ADD1	%xmm3, %xmm4
	movapd	 12 * SIZE(BB), %xmm3
	ADD2	%xmm2, %xmm5
	movapd	 -2 * SIZE(AA), %xmm2
	mulpd	%xmm2,  %xmm3
	mulpd	 14 * SIZE(BB), %xmm2
	ADD1	%xmm3, %xmm6
	movapd	 24 * SIZE(BB), %xmm3
	ADD2	%xmm2, %xmm7
	movapd	  8 * SIZE(AA), %xmm2

	subl   $-16 * SIZE, AA
	addl   $ 32 * SIZE, BB
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
	andl	$7, %eax		# l = (k & 3)
	jle	.L54
	ALIGN_2

.L53:
	mulpd	%xmm0,  %xmm1
	mulpd	-14 * SIZE(BB), %xmm0
	ADD1	%xmm1, %xmm4
	movapd	-12 * SIZE(BB), %xmm1
	ADD2	%xmm0, %xmm5
	movapd	-14 * SIZE(AA), %xmm0

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax			# l--
	jg	.L53

.L54:
	addpd	%xmm6, %xmm4
	addpd	%xmm7, %xmm5

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$1, %eax
#endif

	movl	AORIG, AA
	movl	BORIG, B
	leal	16 * SIZE + BUFFER, BB

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
	movapd	-16 * SIZE(B), %xmm5

	subpd	%xmm4,  %xmm5
#else
	movapd	-16 * SIZE(AA), %xmm5

	subpd	%xmm4,  %xmm5
#endif

#ifndef CONJ
	SHUFPD_1 %xmm1, %xmm1
#endif

#ifdef LN
	movddup	-16 * SIZE(AA), %xmm2
	movddup	-15 * SIZE(AA), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5
#endif

#ifdef LT
	movddup	-16 * SIZE(AA), %xmm2
	movddup	-15 * SIZE(AA), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5
#endif

#ifdef RN
	movddup	-16 * SIZE(B), %xmm2
	movddup	-15 * SIZE(B), %xmm3

	pshufd	$0x4e, %xmm5, %xmm4

	xorpd	 %xmm1, %xmm4

	mulpd	 %xmm2, %xmm5
	mulpd	 %xmm3, %xmm4

	addpd	 %xmm4, %xmm5
#endif

#ifdef RT
	movddup	-16 * SIZE(B), %xmm2
	movddup	-15 * SIZE(B), %xmm3

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
	movapd	%xmm5, -16 * SIZE(B)

	movddup	%xmm5, %xmm4
	unpckhpd %xmm5, %xmm5

	movapd	%xmm4, -16 * SIZE(BB)
	movapd	%xmm5, -14 * SIZE(BB)
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
