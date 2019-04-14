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
#define STACK_A	24 + STACK + ARGS(%esi)
#define STACK_B	28 + STACK + ARGS(%esi)
#define STACK_C	32 + STACK + ARGS(%esi)
#define STACK_LDC	36 + STACK + ARGS(%esi)
#define STACK_OFFT	40 + STACK + ARGS(%esi)

#define POSINV	 0(%esp)
#define K	16(%esp)
#define N	20(%esp)
#define M	24(%esp)
#define A	28(%esp)
#define C	32(%esp)
#define J	36(%esp)
#define OLD_STACK 40(%esp)
#define OFFSET  48(%esp)
#define KK	52(%esp)
#define KKK	56(%esp)
#define AORIG   60(%esp)
#define BORIG	64(%esp)
#define BUFFER 128(%esp)

#define B	%edi
#define LDC	%ebp
#define AA	%edx
#define BB	%ecx
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

	subl	$128 + LOCAL_BUFFER_SIZE + STACK_OFFSET, %esp
	andl	$-STACK_ALIGN, %esp	# align stack
	addl	$STACK_OFFSET, %esp

	STACK_TOUCHING

	movl	STACK_M, %ebx
	movl	STACK_N, %eax
	movl	STACK_K, %ecx
	movl	STACK_A, %edx

	movl	%ebx, M
	movl	%eax, N
	movl	%ecx, K
	movl	%edx, A
	movl	%esi, OLD_STACK

	movl	STACK_B, %edi
	movl	STACK_C, %ebx
	movss	STACK_OFFT, %xmm4

#ifndef CONJ
	movl	  $0x80000000,   0 + POSINV
	movl	  $0x00000000,   4 + POSINV
	movl	  $0x80000000,   8 + POSINV
	movl	  $0x00000000,  12 + POSINV
#else
	movl	  $0x00000000,   0 + POSINV
	movl	  $0x80000000,   4 + POSINV
	movl	  $0x00000000,   8 + POSINV
	movl	  $0x80000000,  12 + POSINV
#endif

	movl	%ebx, C
	movl	STACK_LDC, LDC

	movss	%xmm4, OFFSET
	movss	%xmm4, KK

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
	leal	(BB, %eax, 4), BB
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

.L02:
	movss	0 * SIZE(B), %xmm0
	movss	1 * SIZE(B), %xmm1
	movss	2 * SIZE(B), %xmm2
	movss	3 * SIZE(B), %xmm3

	shufps	$0, %xmm0, %xmm0
	shufps	$0, %xmm1, %xmm1
	shufps	$0, %xmm2, %xmm2
	shufps	$0, %xmm3, %xmm3

	movaps	%xmm0,  0 * SIZE(BB)
	movaps	%xmm1,  4 * SIZE(BB)
	movaps	%xmm2,  8 * SIZE(BB)
	movaps	%xmm3, 12 * SIZE(BB)

	movss	4 * SIZE(B), %xmm0
	movss	5 * SIZE(B), %xmm1
	movss	6 * SIZE(B), %xmm2
	movss	7 * SIZE(B), %xmm3

	shufps	$0, %xmm0, %xmm0
	shufps	$0, %xmm1, %xmm1
	shufps	$0, %xmm2, %xmm2
	shufps	$0, %xmm3, %xmm3

	movaps	%xmm0, 16 * SIZE(BB)
	movaps	%xmm1, 20 * SIZE(BB)
	movaps	%xmm2, 24 * SIZE(BB)
	movaps	%xmm3, 28 * SIZE(BB)

	prefetcht0	 104 * SIZE(B)

	addl	$ 8 * SIZE, B
	addl	$32 * SIZE, BB
	decl	%eax
	jne	.L02

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

.L04:
	movss	0 * SIZE(B), %xmm0
	movss	1 * SIZE(B), %xmm1

	shufps	$0, %xmm0, %xmm0
	shufps	$0, %xmm1, %xmm1

	movaps	%xmm0,  0 * SIZE(BB)
	movaps	%xmm1,  4 * SIZE(BB)

	addl	$2 * SIZE, B
	addl	$8 * SIZE, BB
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
	sarl	$2, %ebx
	jle	.L50
	ALIGN_4

.L10:
#ifdef LN
       movl	K, %eax
       sall	$2 + ZBASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	AORIG, %eax
	movl	%eax, AA

	movl	KK, %eax
	sall	$2 + ZBASE_SHIFT, %eax
	addl	%eax, AA
#endif

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$2 + ZBASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movaps	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 8 * SIZE(BB), %xmm3
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
	prefetcht0   8 * SIZE(CO1)
	je	.L12
	ALIGN_4

#define PREFETCHSIZE 48

.L11:
#ifdef CORE_KATMAI
	prefetcht0	PREFETCHSIZE * SIZE(AA)
#endif

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

#ifdef CORE_KATMAI
	prefetcht0	(PREFETCHSIZE + 8) * SIZE(AA)
#endif

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

#ifdef CORE_KATMAI
	prefetcht0	(PREFETCHSIZE + 16) * SIZE(AA)
#endif

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

#ifdef CORE_KATMAI
	prefetcht0	(PREFETCHSIZE + 24) * SIZE(AA)
#endif

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

#ifdef CORE_KATMAI
	prefetcht0	(PREFETCHSIZE + 32) * SIZE(AA)
#endif

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

#ifdef CORE_KATMAI
	prefetcht0	(PREFETCHSIZE + 40) * SIZE(AA)
#endif

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

#ifdef CORE_KATMAI
	prefetcht0	(PREFETCHSIZE + 48) * SIZE(AA)
#endif

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

#ifdef CORE_KATMAI
	prefetcht0	(PREFETCHSIZE + 56) * SIZE(AA)
#endif

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

	addl	$64 * SIZE, BB
	addl	$64 * SIZE, AA
	decl	%eax
	jne	.L11

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
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	 0 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	 4 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm6
	movaps	 8 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movaps	 8 * SIZE(AA), %xmm0

	addl	$8 * SIZE, AA		# aoffset  += 8
	addl	$8 * SIZE, BB		# boffset1 += 8

	decl	%eax
	jg	.L13

.L14:
	movaps	POSINV,  %xmm0

	shufps	$0xb1, %xmm5, %xmm5
	shufps	$0xb1, %xmm7, %xmm7

#if defined(LN) || defined(LT)
#ifndef CONJ
	xorps	 %xmm0, %xmm5
	xorps	 %xmm0, %xmm7
#else
	xorps	 %xmm0, %xmm4
	xorps	 %xmm0, %xmm6
#endif
#else
	xorps	 %xmm0, %xmm5
	xorps	 %xmm0, %xmm7
#endif

	addps	%xmm5, %xmm4
	addps	%xmm7, %xmm6

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

	sall	$ZBASE_SHIFT, %eax
	leal	(AA, %eax, 4), AA
	leal	(B,  %eax, 1), B
	leal	(BB, %eax, 4), BB
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(B), %xmm5
	movhps	 2 * SIZE(B), %xmm5
	movsd	 4 * SIZE(B), %xmm7
	movhps	 6 * SIZE(B), %xmm7
#else
	movaps	 0 * SIZE(AA), %xmm5
	movaps	 4 * SIZE(AA), %xmm7
#endif

	subps	%xmm4,  %xmm5
	subps	%xmm6,  %xmm7

#if defined(LN) || defined(LT)
	movhlps	%xmm5, %xmm4
	movhlps	%xmm7, %xmm6
#endif

#ifdef LN
#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
 	movsd	30 * SIZE(AA), %xmm1

	movaps	%xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	%xmm6, %xmm3
	shufps	 $0xa0, %xmm3, %xmm3
	shufps	 $0xf5, %xmm6, %xmm6

#ifndef CONJ
	xorps	POSINV, %xmm6
#else
	xorps	POSINV, %xmm3
#endif

	mulps	%xmm0,  %xmm3
	mulps	%xmm1,  %xmm6

	addps	%xmm3,  %xmm6

 	movsd	28 * SIZE(AA), %xmm1

	movaps	%xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	%xmm6, %xmm2
	shufps	 $0xa0, %xmm2, %xmm2
	movaps	%xmm6, %xmm3
	shufps	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 POSINV, %xmm3
#else
	xorps	 POSINV, %xmm2
#endif

	mulps	%xmm0,  %xmm2
	mulps	%xmm1,  %xmm3

	subps	%xmm2,  %xmm7
	subps	%xmm3,  %xmm7

 	movsd	26 * SIZE(AA), %xmm1

	movaps	%xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	%xmm6, %xmm2
	shufps	 $0xa0, %xmm2, %xmm2
	movaps	%xmm6, %xmm3
	shufps	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 POSINV, %xmm3
#else
	xorps	 POSINV, %xmm2
#endif

	mulps	%xmm0,  %xmm2
	mulps	%xmm1,  %xmm3

	subps	%xmm2,  %xmm4
	subps	%xmm3,  %xmm4

 	movsd	24 * SIZE(AA), %xmm1

	movaps	%xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	%xmm6, %xmm2
	shufps	 $0xa0, %xmm2, %xmm2
	movaps	%xmm6, %xmm3
	shufps	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 POSINV, %xmm3
#else
	xorps	 POSINV, %xmm2
#endif

	mulps	%xmm0,  %xmm2
	mulps	%xmm1,  %xmm3

	subps	%xmm2,  %xmm5
	subps	%xmm3,  %xmm5

 	movsd	20 * SIZE(AA), %xmm1

	movaps	%xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	%xmm7, %xmm3
	shufps	 $0xa0, %xmm3, %xmm3
	shufps	 $0xf5, %xmm7, %xmm7

#ifndef CONJ
	xorps	POSINV, %xmm7
#else
	xorps	POSINV, %xmm3
#endif

	mulps	%xmm0,  %xmm3
	mulps	%xmm1,  %xmm7

	addps	%xmm3,  %xmm7

 	movsd	18 * SIZE(AA), %xmm1

	movaps	%xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	%xmm7, %xmm2
	shufps	 $0xa0, %xmm2, %xmm2
	movaps	%xmm7, %xmm3
	shufps	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 POSINV, %xmm3
#else
	xorps	 POSINV, %xmm2
#endif

	mulps	%xmm0,  %xmm2
	mulps	%xmm1,  %xmm3

	subps	%xmm2,  %xmm4
	subps	%xmm3,  %xmm4

 	movsd	16 * SIZE(AA), %xmm1

	movaps	%xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	%xmm7, %xmm2
	shufps	 $0xa0, %xmm2, %xmm2
	movaps	%xmm7, %xmm3
	shufps	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 POSINV, %xmm3
#else
	xorps	 POSINV, %xmm2
#endif

	mulps	%xmm0,  %xmm2
	mulps	%xmm1,  %xmm3

	subps	%xmm2,  %xmm5
	subps	%xmm3,  %xmm5

 	movsd	 10 * SIZE(AA), %xmm1

	movaps	%xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	%xmm4, %xmm3
	shufps	 $0xa0, %xmm3, %xmm3
	shufps	 $0xf5, %xmm4, %xmm4

#ifndef CONJ
	xorps	POSINV, %xmm4
#else
	xorps	POSINV, %xmm3
#endif

	mulps	%xmm0,  %xmm3
	mulps	%xmm1,  %xmm4

	addps	%xmm3,  %xmm4

 	movsd	 8 * SIZE(AA), %xmm1

	movaps	%xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	%xmm4, %xmm2
	shufps	 $0xa0, %xmm2, %xmm2
	movaps	%xmm4, %xmm3
	shufps	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 POSINV, %xmm3
#else
	xorps	 POSINV, %xmm2
#endif

	mulps	%xmm0,  %xmm2
	mulps	%xmm1,  %xmm3

	subps	%xmm2,  %xmm5
	subps	%xmm3,  %xmm5

 	movsd	  0 * SIZE(AA), %xmm1

	movaps	%xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	%xmm5, %xmm3
	shufps	 $0xa0, %xmm3, %xmm3
	shufps	 $0xf5, %xmm5, %xmm5

#ifndef CONJ
	xorps	POSINV, %xmm5
#else
	xorps	POSINV, %xmm3
#endif

	mulps	%xmm0,  %xmm3
	mulps	%xmm1,  %xmm5

	addps	%xmm3,  %xmm5
#endif

#ifdef LT
#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
 	movsd	 0 * SIZE(AA), %xmm1

	movaps	%xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	%xmm5, %xmm3
	shufps	 $0xa0, %xmm3, %xmm3
	shufps	 $0xf5, %xmm5, %xmm5

#ifndef CONJ
	xorps	POSINV, %xmm5
#else
	xorps	POSINV, %xmm3
#endif

	mulps	%xmm0,  %xmm3
	mulps	%xmm1,  %xmm5

	addps	%xmm3,  %xmm5

 	movsd	 2 * SIZE(AA), %xmm1

	movaps	%xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	%xmm5, %xmm2
	shufps	 $0xa0, %xmm2, %xmm2
	movaps	%xmm5, %xmm3
	shufps	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 POSINV, %xmm3
#else
	xorps	 POSINV, %xmm2
#endif

	mulps	%xmm0,  %xmm2
	mulps	%xmm1,  %xmm3

	subps	%xmm2,  %xmm4
	subps	%xmm3,  %xmm4

 	movsd	 4 * SIZE(AA), %xmm1

	movaps	%xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	%xmm5, %xmm2
	shufps	 $0xa0, %xmm2, %xmm2
	movaps	%xmm5, %xmm3
	shufps	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 POSINV, %xmm3
#else
	xorps	 POSINV, %xmm2
#endif

	mulps	%xmm0,  %xmm2
	mulps	%xmm1,  %xmm3

	subps	%xmm2,  %xmm7
	subps	%xmm3,  %xmm7

 	movsd	 6 * SIZE(AA), %xmm1

	movaps	%xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	%xmm5, %xmm2
	shufps	 $0xa0, %xmm2, %xmm2
	movaps	%xmm5, %xmm3
	shufps	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 POSINV, %xmm3
#else
	xorps	 POSINV, %xmm2
#endif

	mulps	%xmm0,  %xmm2
	mulps	%xmm1,  %xmm3

	subps	%xmm2,  %xmm6
	subps	%xmm3,  %xmm6

 	movsd	10 * SIZE(AA), %xmm1

	movaps	%xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	%xmm4, %xmm3
	shufps	 $0xa0, %xmm3, %xmm3
	shufps	 $0xf5, %xmm4, %xmm4

#ifndef CONJ
	xorps	POSINV, %xmm4
#else
	xorps	POSINV, %xmm3
#endif

	mulps	%xmm0,  %xmm3
	mulps	%xmm1,  %xmm4

	addps	%xmm3,  %xmm4

 	movsd	12 * SIZE(AA), %xmm1

	movaps	%xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	%xmm4, %xmm2
	shufps	 $0xa0, %xmm2, %xmm2
	movaps	%xmm4, %xmm3
	shufps	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 POSINV, %xmm3
#else
	xorps	 POSINV, %xmm2
#endif

	mulps	%xmm0,  %xmm2
	mulps	%xmm1,  %xmm3

	subps	%xmm2,  %xmm7
	subps	%xmm3,  %xmm7

 	movsd	14 * SIZE(AA), %xmm1

	movaps	%xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	%xmm4, %xmm2
	shufps	 $0xa0, %xmm2, %xmm2
	movaps	%xmm4, %xmm3
	shufps	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 POSINV, %xmm3
#else
	xorps	 POSINV, %xmm2
#endif

	mulps	%xmm0,  %xmm2
	mulps	%xmm1,  %xmm3

	subps	%xmm2,  %xmm6
	subps	%xmm3,  %xmm6

 	movsd	20 * SIZE(AA), %xmm1

	movaps	%xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	%xmm7, %xmm3
	shufps	 $0xa0, %xmm3, %xmm3
	shufps	 $0xf5, %xmm7, %xmm7

#ifndef CONJ
	xorps	POSINV, %xmm7
#else
	xorps	POSINV, %xmm3
#endif

	mulps	%xmm0,  %xmm3
	mulps	%xmm1,  %xmm7

	addps	%xmm3,  %xmm7

 	movsd	22 * SIZE(AA), %xmm1

	movaps	%xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	%xmm7, %xmm2
	shufps	 $0xa0, %xmm2, %xmm2
	movaps	%xmm7, %xmm3
	shufps	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 POSINV, %xmm3
#else
	xorps	 POSINV, %xmm2
#endif

	mulps	%xmm0,  %xmm2
	mulps	%xmm1,  %xmm3

	subps	%xmm2,  %xmm6
	subps	%xmm3,  %xmm6

 	movsd	30 * SIZE(AA), %xmm1

	movaps	%xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	%xmm6, %xmm3
	shufps	 $0xa0, %xmm3, %xmm3
	shufps	 $0xf5, %xmm6, %xmm6

#ifndef CONJ
	xorps	POSINV, %xmm6
#else
	xorps	POSINV, %xmm3
#endif

	mulps	%xmm0,  %xmm3
	mulps	%xmm1,  %xmm6

	addps	%xmm3,  %xmm6
#endif

#if defined(RN) || defined(RT)
	movsd	  0 * SIZE(B), %xmm1
	movhps	  2 * SIZE(B), %xmm1

#ifdef HAVE_SSE2
	pshufd	 $0x44, %xmm1, %xmm2
	pshufd	 $0x11, %xmm1, %xmm3

	pshufd	 $0xa0, %xmm5, %xmm4
	pshufd	 $0xf5, %xmm5, %xmm5
	pshufd	 $0xa0, %xmm7, %xmm6
	pshufd	 $0xf5, %xmm7, %xmm7
#else
	movaps	%xmm1, %xmm2
	shufps	 $0x44, %xmm2, %xmm2
	movaps	%xmm1, %xmm3
	shufps	 $0x11, %xmm3, %xmm3

	movaps	%xmm5, %xmm4
	shufps	 $0xa0, %xmm4, %xmm4
	shufps	 $0xf5, %xmm5, %xmm5
	movaps	%xmm7, %xmm6
	shufps	 $0xa0, %xmm6, %xmm6
	shufps	 $0xf5, %xmm7, %xmm7
#endif

#ifndef CONJ
	xorps	 %xmm0, %xmm5
	xorps	 %xmm0, %xmm7
#else
	xorps	 %xmm0, %xmm4
	xorps	 %xmm0, %xmm6
#endif

	mulps	%xmm2,  %xmm4
	mulps	%xmm3,  %xmm5
	mulps	%xmm2,  %xmm6
	mulps	%xmm3,  %xmm7

	addps	%xmm4,  %xmm5
	addps	%xmm6,  %xmm7
#endif

#ifdef LN
	subl	$8 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlhps	%xmm4, %xmm5
	movlhps	%xmm6, %xmm7

	movlps	%xmm5,   0 * SIZE(B)
	movhps	%xmm5,   2 * SIZE(B)
	movlps	%xmm7,   4 * SIZE(B)
	movhps	%xmm7,   6 * SIZE(B)

#ifdef HAVE_SSE2
	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1
	pshufd	$0xaa, %xmm5, %xmm2
	pshufd	$0xff, %xmm5, %xmm3
#else
	movaps	%xmm5, %xmm0
	shufps	$0x00, %xmm0, %xmm0
	movaps	%xmm5, %xmm1
	shufps	$0x55, %xmm1, %xmm1
	movaps	%xmm5, %xmm2
	shufps	$0xaa, %xmm2, %xmm2
	movaps	%xmm5, %xmm3
	shufps	$0xff, %xmm3, %xmm3
#endif

	movaps	%xmm0,   0 * SIZE(BB)
	movaps	%xmm1,   4 * SIZE(BB)
	movaps	%xmm2,   8 * SIZE(BB)
	movaps	%xmm3,  12 * SIZE(BB)

#ifdef HAVE_SSE2
	pshufd	$0x00, %xmm7, %xmm0
	pshufd	$0x55, %xmm7, %xmm1
	pshufd	$0xaa, %xmm7, %xmm2
	pshufd	$0xff, %xmm7, %xmm3
#else
	movaps	%xmm7, %xmm0
	shufps	$0x00, %xmm0, %xmm0
	movaps	%xmm7, %xmm1
	shufps	$0x55, %xmm1, %xmm1
	movaps	%xmm7, %xmm2
	shufps	$0xaa, %xmm2, %xmm2
	movaps	%xmm7, %xmm3
	shufps	$0xff, %xmm3, %xmm3
#endif

	movaps	%xmm0,  16 * SIZE(BB)
	movaps	%xmm1,  20 * SIZE(BB)
	movaps	%xmm2,  24 * SIZE(BB)
	movaps	%xmm3,  28 * SIZE(BB)
#else
	movaps	%xmm5,   0 * SIZE(AA)
	movaps	%xmm7,   4 * SIZE(AA)
#endif

	movlps	%xmm5,   0 * SIZE(CO1)
	movhps	%xmm5,   2 * SIZE(CO1)
	movlps	%xmm7,   4 * SIZE(CO1)
	movhps	%xmm7,   6 * SIZE(CO1)

#ifndef LN
	addl	$8 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$2 + ZBASE_SHIFT, %eax
	addl	%eax, AA
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
	sall	$2 + ZBASE_SHIFT, %eax
	addl	%eax, AORIG
#endif

	decl	%ebx			# i --
	jg	.L10
	ALIGN_2

.L50:
	movl	M,  %ebx
	testl	$2, %ebx
	jle	.L70

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
	sall	$2 + ZBASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movaps	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 8 * SIZE(BB), %xmm3
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
	ALIGN_4

.L51:
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	16 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	 4 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm3
	mulps	12 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm6
	movaps	24 * SIZE(BB), %xmm3
	addps	%xmm0, %xmm7
	movaps	16 * SIZE(AA), %xmm0
	mulps	%xmm1, %xmm2
	mulps	20 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm4
	movaps	32 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm5
	movaps	12 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	mulps	28 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm6
	movaps	40 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movaps	24 * SIZE(AA), %xmm1
	mulps	%xmm0, %xmm2
	mulps	36 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	48 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	20 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm3
	mulps	44 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm6
	movaps	56 * SIZE(BB), %xmm3
	addps	%xmm0, %xmm7
	movaps	 32 * SIZE(AA), %xmm0
	mulps	%xmm1, %xmm2
	mulps	52 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm4
	movaps	 64 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm5
	movaps	28 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	mulps	60 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm6
	movaps	 72 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movaps	 40 * SIZE(AA), %xmm1

	addl	$32 * SIZE, AA
	addl	$64 * SIZE, BB
	decl	%eax
	jne	.L51
	ALIGN_4

.L52:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L54

.L53:
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	 8 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	 4 * SIZE(AA), %xmm0

	addl	$4 * SIZE, AA		# aoffset  += 8
	addl	$8 * SIZE, BB		# boffset1 += 8
	decl	%eax
	jg	.L53

.L54:
	addps	%xmm6, %xmm4
	addps	%xmm7, %xmm5

	movaps	POSINV,  %xmm0

	shufps	$0xb1, %xmm5, %xmm5

#if defined(LN) || defined(LT)
#ifndef CONJ
	xorps	 %xmm0, %xmm5
#else
	xorps	 %xmm0, %xmm4
#endif
#else
	xorps	 %xmm0, %xmm5
#endif

	addps	%xmm5, %xmm4

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
	leal	(B,  %eax, 1), B
	leal	(BB, %eax, 4), BB
#endif

#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(B), %xmm5
	movhps	 2 * SIZE(B), %xmm5
#else
	movaps	 0 * SIZE(AA), %xmm5
#endif

	subps	%xmm4,  %xmm5

#if defined(LN) || defined(LT)
	movhlps	%xmm5, %xmm4
#endif

#ifdef LN
#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
 	movsd	  6 * SIZE(AA), %xmm1

	movaps	 %xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	 %xmm4, %xmm3
	shufps	 $0xa0, %xmm3, %xmm3
	shufps	 $0xf5, %xmm4, %xmm4

#ifndef CONJ
	xorps	POSINV, %xmm4
#else
	xorps	POSINV, %xmm3
#endif

	mulps	%xmm0,  %xmm3
	mulps	%xmm1,  %xmm4

	addps	%xmm3,  %xmm4

 	movsd	 4 * SIZE(AA), %xmm1

	movaps	 %xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	 %xmm4, %xmm2
	shufps	 $0xa0, %xmm2, %xmm2
	movaps	 %xmm4, %xmm3
	shufps	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 POSINV, %xmm3
#else
	xorps	 POSINV, %xmm2
#endif

	mulps	%xmm0,  %xmm2
	mulps	%xmm1,  %xmm3

	subps	%xmm2,  %xmm5
	subps	%xmm3,  %xmm5


 	movsd	 0 * SIZE(AA), %xmm1

	movaps	 %xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	 %xmm5, %xmm3
	shufps	 $0xa0, %xmm3, %xmm3
	shufps	 $0xf5, %xmm5, %xmm5

#ifndef CONJ
	xorps	POSINV, %xmm5
#else
	xorps	POSINV, %xmm3
#endif

	mulps	%xmm0,  %xmm3
	mulps	%xmm1,  %xmm5

	addps	%xmm3,  %xmm5
#endif

#ifdef LT
#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
 	movsd	 0 * SIZE(AA), %xmm1

	movaps	 %xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	 %xmm5, %xmm3
	shufps	 $0xa0, %xmm3, %xmm3
	shufps	 $0xf5, %xmm5, %xmm5

#ifndef CONJ
	xorps	POSINV, %xmm5
#else
	xorps	POSINV, %xmm3
#endif

	mulps	%xmm0,  %xmm3
	mulps	%xmm1,  %xmm5

	addps	%xmm3,  %xmm5

 	movsd	 2 * SIZE(AA), %xmm1

	movaps	 %xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	 %xmm5, %xmm2
	shufps	 $0xa0, %xmm2, %xmm2
	movaps	 %xmm5, %xmm3
	shufps	 $0xf5, %xmm3, %xmm3

#ifndef CONJ
	xorps	 POSINV, %xmm3
#else
	xorps	 POSINV, %xmm2
#endif

	mulps	%xmm0,  %xmm2
	mulps	%xmm1,  %xmm3

	subps	%xmm2,  %xmm4
	subps	%xmm3,  %xmm4

 	movsd	 6 * SIZE(AA), %xmm1

	movaps	 %xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	 %xmm4, %xmm3
	shufps	 $0xa0, %xmm3, %xmm3
	shufps	 $0xf5, %xmm4, %xmm4

#ifndef CONJ
	xorps	POSINV, %xmm4
#else
	xorps	POSINV, %xmm3
#endif

	mulps	%xmm0,  %xmm3
	mulps	%xmm1,  %xmm4

	addps	%xmm3,  %xmm4
#endif

#if defined(RN) || defined(RT)
	movsd	  0 * SIZE(B), %xmm1
	movhps	  2 * SIZE(B), %xmm1

	movaps	 %xmm1, %xmm2
	shufps	 $0x44, %xmm2, %xmm2
	movaps	 %xmm1, %xmm3
	shufps	 $0x11, %xmm2, %xmm3

	movaps	 %xmm5, %xmm4
	shufps	 $0xa0, %xmm4, %xmm4
	shufps	 $0xf5, %xmm5, %xmm5

#ifndef CONJ
	xorps	 %xmm0, %xmm5
#else
	xorps	 %xmm0, %xmm4
#endif

	mulps	%xmm2,  %xmm4
	mulps	%xmm3,  %xmm5

	addps	%xmm4,  %xmm5
#endif

#ifdef LN
	subl	$4 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlhps	%xmm4, %xmm5

	movlps	%xmm5,   0 * SIZE(B)
	movhps	%xmm5,   2 * SIZE(B)

#ifdef HAVE_SSE2
	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm1
	pshufd	$0xaa, %xmm5, %xmm2
	pshufd	$0xff, %xmm5, %xmm3
#else
	movaps	%xmm5, %xmm0
	shufps	$0x00, %xmm0, %xmm0
	movaps	%xmm5, %xmm1
	shufps	$0x55, %xmm1, %xmm1
	movaps	%xmm5, %xmm2
	shufps	$0xaa, %xmm2, %xmm2
	movaps	%xmm5, %xmm3
	shufps	$0xff, %xmm3, %xmm3
#endif

	movaps	%xmm0,   0 * SIZE(BB)
	movaps	%xmm1,   4 * SIZE(BB)
	movaps	%xmm2,   8 * SIZE(BB)
	movaps	%xmm3,  12 * SIZE(BB)
#else
	movaps	%xmm5,   0 * SIZE(AA)
#endif

	movlps	%xmm5,   0 * SIZE(CO1)
	movhps	%xmm5,   2 * SIZE(CO1)

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
	ALIGN_2

.L70:
	movl	M,  %ebx
	testl	$1, %ebx
	jle	.L99

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

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$2 + ZBASE_SHIFT, %eax
	addl	%eax, BB
#endif


	movaps	 0 * SIZE(BB), %xmm2

	xorps	%xmm4, %xmm4
#ifdef	movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 8 * SIZE(BB), %xmm3
#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	 8 * SIZE(AA), %xmm1

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	je	.L72
	ALIGN_4

.L71:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	16 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm4
	movaps	12 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movsd	 4 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	movaps	24 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	20 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 6 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	32 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm4
	movaps	28 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movsd	16 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	movaps	40 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm4
	movaps	36 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movsd	10 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm5
	movaps	48 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movaps	44 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	12 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movaps	56 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm4
	movaps	52 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movsd	14 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm5
	movaps	64 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movaps	60 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
 	movsd	24 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movaps	72 * SIZE(BB), %xmm3

	addl	$16 * SIZE, AA
	addl	$64 * SIZE, BB
	decl	%eax
	jne	.L71
	ALIGN_2

.L72:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	andl	$7, %eax
	je .L74

.L73:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA		# aoffset  += 8
	addl	$8 * SIZE, BB		# boffset1 += 8
	decl	%eax
	jg	.L73

.L74:
	movaps	POSINV,  %xmm0

	shufps	$0xb1, %xmm5, %xmm5

#if defined(LN) || defined(LT)
#ifndef CONJ
	xorps	 %xmm0, %xmm5
#else
	xorps	 %xmm0, %xmm4
#endif
#else
	xorps	 %xmm0, %xmm5
#endif

	addps	%xmm5, %xmm4

#if defined(LN) || defined(RT)
	movl	KK, %eax
	subl	$1, %eax

	movl	AORIG, AA
	movl	BORIG, B
	leal	BUFFER, BB

	sall	$ZBASE_SHIFT, %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 1), B
	leal	(BB, %eax, 4), BB
#endif

#ifdef	movsd
	xorps	%xmm5, %xmm5
#endif
#if defined(LN) || defined(LT)
	movsd	 0 * SIZE(B),  %xmm5
#else
	movsd	 0 * SIZE(AA), %xmm5
#endif
	subps	%xmm4, %xmm5

#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
#if defined(LN) || defined(LT)
 	movsd	 0 * SIZE(AA), %xmm1
#else
 	movsd	 0 * SIZE(B),  %xmm1
#endif

	movaps	 %xmm1, %xmm0
	shufps	 $0x44, %xmm0, %xmm0
	shufps	 $0x11, %xmm1, %xmm1

	movaps	 %xmm5, %xmm3
	shufps	 $0xa0, %xmm3, %xmm3
	shufps	 $0xf5, %xmm5, %xmm5

#ifndef CONJ
	xorps	POSINV, %xmm5
#else
	xorps	POSINV, %xmm3
#endif

	mulps	%xmm0,  %xmm3
	mulps	%xmm1,  %xmm5

	addps	%xmm3,  %xmm5

#ifdef LN
	subl	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm5,   0 * SIZE(B)

	movaps	 %xmm5, %xmm0
	shufps	$0x00, %xmm0, %xmm0
	movaps	 %xmm5, %xmm1
	shufps	$0x55, %xmm1, %xmm1

	movaps	%xmm0,   0 * SIZE(BB)
	movaps	%xmm1,   4 * SIZE(BB)
#else
	movlps	%xmm5,   0 * SIZE(AA)
#endif

	movlps	%xmm5,   0 * SIZE(CO1)

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
	ALIGN_2

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
	ALIGN_2

.L999:
	movl	OLD_STACK, %esp

	EMMS

	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
