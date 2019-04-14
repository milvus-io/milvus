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
#define	LDC	%ebp
#define AA	%edx
#define BB	%ecx
#define CO1	%esi

#define STACK_ALIGN	4096
#define STACK_OFFSET	1024

#if defined(OPTERON) || defined(BARCELONA) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
#define PREFETCHSIZE (16 * 10 + 8)
#define WPREFETCHSIZE 112
#define PREFETCH      prefetch
#define PREFETCHW     prefetchw
#endif

#if defined(PENTIUM4) || defined(PENTIUMM)
#define PREFETCH	prefetcht1
#define PREFETCHSIZE 168
#define PREFETCHW     prefetcht0
#endif

#if defined(PENRYN) || defined(DUNNINGTON)
#define PREFETCH	prefetcht1
#define PREFETCHSIZE 168
#define PREFETCHW     prefetcht0
#endif

#if defined(OPTERON) || !defined(HAVE_SSE2)
#define movsd	movlps
#endif

#ifdef HAVE_SSE2
#define	xorps	pxor
#endif

#define KERNEL1(address) \
	mulps	%xmm0, %xmm2; \
	PREFETCH (PREFETCHSIZE + 0) * SIZE + (address) * 1 * SIZE(AA); \
	addps	%xmm2, %xmm4; \
	movaps	 4 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	addps	%xmm2, %xmm5; \
	movaps	 8 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	mulps	12 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addps	%xmm2, %xmm6; \
	movaps	32 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addps	%xmm0, %xmm7; \
	movaps	 4 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL2(address) \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm4; \
	movaps	20 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm5; \
	movaps	24 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	mulps	28 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addps	%xmm3, %xmm6; \
	movaps	48 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addps	%xmm0, %xmm7; \
	movaps	 8 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL3(address) \
	mulps	%xmm0, %xmm2; \
	addps	%xmm2, %xmm4; \
	movaps	36 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	addps	%xmm2, %xmm5; \
	movaps	40 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	mulps	44 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addps	%xmm2, %xmm6; \
	movaps	64 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addps	%xmm0, %xmm7; \
	movaps	12 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL4(address) \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm4; \
	movaps	52 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm5; \
	movaps	56 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	mulps	60 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addps	%xmm3, %xmm6; \
	movaps	80 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addps	%xmm0, %xmm7; \
	movaps	 32 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL5(address) \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm4; \
	movaps	68 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm5; \
	movaps	72 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	mulps	76 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addps	%xmm2, %xmm6; \
	movaps	 96 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addps	%xmm1, %xmm7; \
	movaps	20 * SIZE + (address) * 1 * SIZE(AA), %xmm1

#define KERNEL6(address) \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm4; \
	movaps	84 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm5; \
	movaps	88 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	mulps	92 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addps	%xmm3, %xmm6; \
	movaps	112 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addps	%xmm1, %xmm7; \
	movaps	 24 * SIZE + (address) * 1 * SIZE(AA), %xmm1

#define KERNEL7(address) \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm4; \
	movaps	100 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm5; \
	movaps	104 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	mulps	108 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addps	%xmm2, %xmm6; \
	movaps	128 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addps	%xmm1, %xmm7; \
	movaps	28 * SIZE + (address) * 1 * SIZE(AA), %xmm1

#define KERNEL8(address) \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm4; \
	movaps	116 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm5; \
	movaps	120 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	mulps	124 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addps	%xmm3, %xmm6; \
	movaps	144 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addps	%xmm1, %xmm7; \
	movaps	 48 * SIZE + (address) * 1 * SIZE(AA), %xmm1;

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

	xorps	%xmm7, %xmm7
	pcmpeqb	%xmm7, %xmm7
	pslld	$31,   %xmm7
	xorps	%xmm2, %xmm2

#ifndef CONJ
	movss	  %xmm7,   0 + POSINV
	movss	  %xmm2,   4 + POSINV
	movss	  %xmm7,   8 + POSINV
	movss	  %xmm2,  12 + POSINV
#else
	movss	  %xmm2,   0 + POSINV
	movss	  %xmm7,   4 + POSINV
	movss	  %xmm2,   8 + POSINV
	movss	  %xmm7,  12 + POSINV
#endif

	EMMS

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
	movl	%eax, J
	sarl	$1, J
	jle	.L100
	ALIGN_4

.L01:
#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

	leal	BUFFER, %ecx

#ifdef RT
       movl	K, %eax
       sall	$1 + ZBASE_SHIFT, %eax
       subl	%eax, B
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	B, BORIG
	sall	$1 + ZBASE_SHIFT, %eax
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
	sarl	$1, %eax
	jle	.L03
	ALIGN_4

.L02:
	movaps	 0 * SIZE(B), %xmm3
	movaps	 4 * SIZE(B), %xmm7

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

	movaps	%xmm0,  0 * SIZE(BB)
	movaps	%xmm1,  4 * SIZE(BB)
	movaps	%xmm2,  8 * SIZE(BB)
	movaps	%xmm3, 12 * SIZE(BB)

	pshufd	 $0x00, %xmm7, %xmm4
	pshufd	 $0x55, %xmm7, %xmm5
	pshufd	 $0xaa, %xmm7, %xmm6
	pshufd	 $0xff, %xmm7, %xmm7

	movaps	%xmm4, 16 * SIZE(BB)
	movaps	%xmm5, 20 * SIZE(BB)
	movaps	%xmm6, 24 * SIZE(BB)
	movaps	%xmm7, 28 * SIZE(BB)

	addl	$ 8 * SIZE, B
	addl	$32 * SIZE, BB

	decl	%eax
	jne	.L02
	ALIGN_4

.L03:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$1, %eax
	BRANCH
	jle	.L05
	ALIGN_4

.L04:
	movaps	 0 * SIZE(B), %xmm3

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

	movaps	%xmm0,  0 * SIZE(BB)
	movaps	%xmm1,  4 * SIZE(BB)
	movaps	%xmm2,  8 * SIZE(BB)
	movaps	%xmm3, 12 * SIZE(BB)

	addl	$ 4 * SIZE, B
	ALIGN_4

.L05:
#if defined(LT) || defined(RN)
	movl	A, %eax
	movl	%eax, AA
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

       leal	(, LDC, 2), %eax

#ifdef RT
       subl	%eax, C
#endif

	movl	C,  CO1

#ifndef RT
	addl	%eax, C
#endif

	movl	M,  %ebx
	sarl	$1, %ebx
	jle	.L30
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

	leal	BUFFER, BB	# boffset1 = boffset

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$3 + ZBASE_SHIFT, %eax
	addl	%eax, BB
#endif

	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm4, %xmm4
	movaps	16 * SIZE(AA), %xmm1
	xorps	%xmm5, %xmm5
	movaps	 0 * SIZE(BB), %xmm2
	xorps	%xmm6, %xmm6
	movaps	16 * SIZE(BB), %xmm3
	xorps	%xmm7, %xmm7

	PREFETCHW   3 * SIZE(CO1)
	PREFETCHW   3 * SIZE(CO1, LDC)

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
	KERNEL1(0 * 16)
	KERNEL2(0 * 16)
	KERNEL3(0 * 16)
	KERNEL4(0 * 16)
	KERNEL5(0 * 16)
	KERNEL6(0 * 16)
	KERNEL7(0 * 16)
	KERNEL8(0 * 16)

	addl	$ 32 * SIZE, AA
	addl	$128 * SIZE, BB
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
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	mulps	12 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm6
	movaps	16 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movaps	 4 * SIZE(AA), %xmm0

	addl	$ 4 * SIZE, AA
	addl	$16 * SIZE, BB
	decl	%eax
	jg	.L13
	ALIGN_4

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
	subl	$2, %eax
#else
	subl	$2, %eax
#endif

	movl	AORIG, AA
	movl	BORIG, B
	leal	BUFFER, BB

	sall	$ZBASE_SHIFT, %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 2), B
	leal	(BB, %eax, 8), BB
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm4, %xmm5
	unpcklpd %xmm6, %xmm4
	unpckhpd %xmm6, %xmm5

	movaps	 0 * SIZE(B), %xmm2
	movaps	 4 * SIZE(B), %xmm3

	subps	%xmm4,  %xmm2
	subps	%xmm5,  %xmm3
#else
	movaps	 0 * SIZE(AA), %xmm1
	movaps	 4 * SIZE(AA), %xmm5

	subps	%xmm4,  %xmm1
	subps	%xmm6,  %xmm5
#endif

#ifdef LN
	movaps	 4 * SIZE(AA), %xmm5

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

	movaps	 0 * SIZE(AA), %xmm5

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
	movaps	 0 * SIZE(AA), %xmm5

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

	movaps	 4 * SIZE(AA), %xmm5

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
	movaps	 0 * SIZE(B), %xmm4

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

	movaps	 4 * SIZE(B), %xmm4

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
	movaps	 4 * SIZE(B), %xmm4

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

	movaps	 0 * SIZE(B), %xmm4

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
	movaps	%xmm2,   0 * SIZE(B)
	movaps	%xmm3,   4 * SIZE(B)

	pshufd	$0x00, %xmm2, %xmm0
	pshufd	$0x55, %xmm2, %xmm1
	pshufd	$0xaa, %xmm2, %xmm4
	pshufd	$0xff, %xmm2, %xmm5

	movaps	%xmm0,   0 * SIZE(BB)
	movaps	%xmm1,   4 * SIZE(BB)
	movaps	%xmm4,   8 * SIZE(BB)
	movaps	%xmm5,  12 * SIZE(BB)

	pshufd	$0x00, %xmm3, %xmm0
	pshufd	$0x55, %xmm3, %xmm1
	pshufd	$0xaa, %xmm3, %xmm4
	pshufd	$0xff, %xmm3, %xmm5

	movaps	%xmm0,  16 * SIZE(BB)
	movaps	%xmm1,  20 * SIZE(BB)
	movaps	%xmm4,  24 * SIZE(BB)
	movaps	%xmm5,  28 * SIZE(BB)

	movlps	%xmm2,   0 * SIZE(CO1)
	movlps	%xmm3,   2 * SIZE(CO1)
	movhps	%xmm2,   0 * SIZE(CO1, LDC)
	movhps	%xmm3,   2 * SIZE(CO1, LDC)
#else
	movaps	%xmm1,   0 * SIZE(AA)
	movaps	%xmm5,   4 * SIZE(AA)

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
	sall	$1 + ZBASE_SHIFT, %eax
	addl	%eax, AA
#ifdef LT
	addl	$8 * SIZE, B
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

	decl	%ebx
	jg	.L10
	ALIGN_4

.L30:
	movl	M,  %ebx
	andl	$1, %ebx
	jle	.L99
	ALIGN_4

.L40:
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

	leal	BUFFER, BB	# boffset1 = boffset

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$3 + ZBASE_SHIFT, %eax
	addl	%eax, BB
#endif

	xorps	%xmm4, %xmm4
	xorps	%xmm5, %xmm5
	xorps	%xmm6, %xmm6
	xorps	%xmm7, %xmm7

#ifdef	movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	 0 * SIZE(AA), %xmm0
#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	 8 * SIZE(AA), %xmm1
	movaps	 0 * SIZE(BB), %xmm2
	movaps	16 * SIZE(BB), %xmm3

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
	mulps	%xmm0, %xmm2
	prefetcht1	(PREFETCHSIZE +  0) * SIZE(AA)
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	mulps	12 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm6
	movaps	32 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movsd	 2 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm4
	movaps	20 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm5
	movaps	24 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	mulps	28 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm6
	movaps	48 * SIZE(BB), %xmm3
	addps	%xmm0, %xmm7
	movsd	 4 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	36 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm5
	movaps	40 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	mulps	44 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm6
	movaps	64 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movsd	 6 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm4
	movaps	52 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm5
	movaps	56 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	mulps	60 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm6
	movaps	80 * SIZE(BB), %xmm3
	addps	%xmm0, %xmm7
	movsd	16 * SIZE(AA), %xmm0
	mulps	%xmm1, %xmm2
#if defined(OPTERON) || defined(BARCELONA) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
	prefetcht1     (PREFETCHSIZE + 16) * SIZE(AA)
#endif
	addps	%xmm2, %xmm4
	movaps	68 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm5
	movaps	72 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	mulps	76 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm6
	movaps	96 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm7
	movsd	10 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movaps	84 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm5
	movaps	88 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	mulps	92 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm6
	movaps	112 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movsd	12 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm4
	movaps	100 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm5
	movaps	104 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	mulps	108 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm6
	movaps	128 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm7
	movsd	14 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movaps	116 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm5
	movaps	120 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	mulps	124 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm6
	movaps	144 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movsd	 24 * SIZE(AA), %xmm1
	addl	$ 16 * SIZE, AA
	addl	$128 * SIZE, BB
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
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	mulps	12 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm6
	movaps	16 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movsd	 2 * SIZE(AA), %xmm0

	addl	$ 2 * SIZE, AA
	addl	$16 * SIZE, BB
	decl	%eax
	jg	.L43
	ALIGN_4

.L44:
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
	subl	$1, %eax
#else
	subl	$2, %eax
#endif

	movl	AORIG, AA
	movl	BORIG, B
	leal	BUFFER, BB

	sall	$ZBASE_SHIFT, %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 2), B
	leal	(BB, %eax, 8), BB
#endif

#if defined(LN) || defined(LT)
	unpcklpd %xmm6, %xmm4

	movaps	 0 * SIZE(B), %xmm2

	subps	%xmm4,  %xmm2
#else
#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	 0 * SIZE(AA), %xmm1
#ifdef	movsd
	xorps	%xmm5, %xmm5
#endif
	movsd	 2 * SIZE(AA), %xmm5

	subps	%xmm4,  %xmm1
	subps	%xmm6,  %xmm5
#endif

#if defined(LN) || defined(LT)
	movaps	 0 * SIZE(AA), %xmm5

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
	movaps	 0 * SIZE(B), %xmm4

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

	movaps	 4 * SIZE(B), %xmm4

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
	movaps	 4 * SIZE(B), %xmm4

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

	movaps	 0 * SIZE(B), %xmm4

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
	movaps	%xmm2,   0 * SIZE(B)

	pshufd	$0x00, %xmm2, %xmm0
	pshufd	$0x55, %xmm2, %xmm1
	pshufd	$0xaa, %xmm2, %xmm4
	pshufd	$0xff, %xmm2, %xmm5

	movaps	%xmm0,   0 * SIZE(BB)
	movaps	%xmm1,   4 * SIZE(BB)
	movaps	%xmm4,   8 * SIZE(BB)
	movaps	%xmm5,  12 * SIZE(BB)

	movlps	%xmm2,   0 * SIZE(CO1)
	movhps	%xmm2,   0 * SIZE(CO1, LDC)
#else
	movlps	%xmm1,   0 * SIZE(AA)
	movlps	%xmm5,   2 * SIZE(AA)

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
	addl	%eax, AA
#ifdef LT
	addl	$4 * SIZE, B
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
       sall	$1 + ZBASE_SHIFT, %eax
       addl	%eax, B
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$1 + ZBASE_SHIFT, %eax
	addl	%eax, B
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
	ALIGN_4

.L101:
#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

	leal	BUFFER, %ecx

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
	jle	.L103
	ALIGN_4

.L102:
	movaps	 0 * SIZE(B), %xmm3
	movaps	 4 * SIZE(B), %xmm7

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

	movaps	%xmm0,  0 * SIZE(BB)
	movaps	%xmm1,  4 * SIZE(BB)
	movaps	%xmm2,  8 * SIZE(BB)
	movaps	%xmm3, 12 * SIZE(BB)

	pshufd	 $0x00, %xmm7, %xmm4
	pshufd	 $0x55, %xmm7, %xmm5
	pshufd	 $0xaa, %xmm7, %xmm6
	pshufd	 $0xff, %xmm7, %xmm7

	movaps	%xmm4, 16 * SIZE(BB)
	movaps	%xmm5, 20 * SIZE(BB)
	movaps	%xmm6, 24 * SIZE(BB)
	movaps	%xmm7, 28 * SIZE(BB)

	addl	$ 8 * SIZE, B
	addl	$32 * SIZE, BB
	decl	%eax
	jne	.L102
	ALIGN_4

.L103:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$3, %eax
	BRANCH
	jle	.L105
	ALIGN_4

.L104:
#ifdef	movsd
	xorps	%xmm3, %xmm3
#endif
	movsd	 0 * SIZE(B), %xmm3

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1

	movaps	%xmm0,  0 * SIZE(BB)
	movaps	%xmm1,  4 * SIZE(BB)

	addl	$ 2 * SIZE, %edi
	addl	$ 8 * SIZE, %ecx
	decl	%eax
	jne	.L104
	ALIGN_4

.L105:
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

	movl	C,  CO1

#ifndef RT
	addl	LDC, C
#endif

	movl	M,  %ebx
	sarl	$1, %ebx
	jle	.L130
	ALIGN_4

.L110:
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

	leal	BUFFER, BB	# boffset1 = boffset

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$2 + ZBASE_SHIFT, %eax
	addl	%eax, BB
#endif

	xorps	%xmm4, %xmm4
	xorps	%xmm5, %xmm5
	xorps	%xmm6, %xmm6
	xorps	%xmm7, %xmm7

	movaps	 0 * SIZE(AA), %xmm0
	movaps	16 * SIZE(AA), %xmm1
	movaps	 0 * SIZE(BB), %xmm2
	movaps	16 * SIZE(BB), %xmm3

	PREFETCHW   3 * SIZE(CO1)

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
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movaps	 4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm6
	movaps	12 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movaps	 8 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm7
	movaps	32 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm4
	movaps	20 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movaps	12  * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	movaps	24 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm6
	movaps	28 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movaps	32 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm7
	movaps	48 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm4
	movaps	36 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movaps	20 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm5
	movaps	40 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm6
	movaps	44 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movaps	24 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm7
	movaps	64 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movaps	52 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movaps	28 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movaps	56 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm6
	movaps	60 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movaps	48 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm7
	movaps	80 * SIZE(BB), %xmm3

	addl	$ 32 * SIZE, AA
	addl	$ 64 * SIZE, BB
	decl	%eax
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
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	 8 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	 4 * SIZE(AA), %xmm0

	addl	$ 4 * SIZE, AA
	addl	$ 8 * SIZE, BB
	decl	%eax
	jg	.L113
	ALIGN_4

.L114:
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
	movaps	%xmm4, %xmm5
	unpcklpd %xmm6, %xmm4
	unpckhpd %xmm6, %xmm5

#ifdef	movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	 0 * SIZE(B), %xmm2
#ifdef	movsd
	xorps	%xmm3, %xmm3
#endif
	movsd	 2 * SIZE(B), %xmm3

	subps	%xmm4,  %xmm2
	subps	%xmm5,  %xmm3
#else
	movaps	 0 * SIZE(AA), %xmm1

	subps	%xmm4,  %xmm1
#endif

#ifdef LN
	movaps	 4 * SIZE(AA), %xmm5

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

	movaps	 0 * SIZE(AA), %xmm5

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
	movaps	 0 * SIZE(AA), %xmm5

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

	movaps	 4 * SIZE(AA), %xmm5

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
	movaps	 0 * SIZE(B), %xmm4

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
	movlps	%xmm2,   0 * SIZE(B)
	movlps	%xmm3,   2 * SIZE(B)

	pshufd	$0x00, %xmm2, %xmm0
	pshufd	$0x55, %xmm2, %xmm1

	movaps	%xmm0,   0 * SIZE(BB)
	movaps	%xmm1,   4 * SIZE(BB)

	pshufd	$0x00, %xmm3, %xmm0
	pshufd	$0x55, %xmm3, %xmm1

	movaps	%xmm0,   8 * SIZE(BB)
	movaps	%xmm1,  12 * SIZE(BB)

	movlps	%xmm2,   0 * SIZE(CO1)
	movlps	%xmm3,   2 * SIZE(CO1)
#else
	movaps	%xmm1,   0 * SIZE(AA)

	movlps	%xmm1,   0 * SIZE(CO1)
	movhps	%xmm1,   2 * SIZE(CO1)
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
	jg	.L110
	ALIGN_4

.L130:
	movl	M,  %ebx
	andl	$1, %ebx
	jle	.L149

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

	leal	BUFFER, BB	# boffset1 = boffset

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$2 + ZBASE_SHIFT, %eax
	addl	%eax, BB
#endif

#ifdef	movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	 0 * SIZE(AA), %xmm0
	xorps	%xmm4, %xmm4
#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	 8 * SIZE(AA), %xmm1
	xorps	%xmm5, %xmm5
	movaps	 0 * SIZE(BB), %xmm2
	xorps	%xmm6, %xmm6
	movaps	16 * SIZE(BB), %xmm3
	xorps	%xmm7, %xmm7

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

	addl	$ 16 * SIZE, AA
	addl	$ 64 * SIZE, BB
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
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	 8 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movsd	 2 * SIZE(AA), %xmm0

	addl	$2 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L143
	ALIGN_4

.L144:
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
	subl	$1, %eax

	movl	AORIG, AA
	movl	BORIG, B
	leal	BUFFER, BB

	sall	$ZBASE_SHIFT, %eax
	addl	%eax, AA
	addl	%eax, B
	leal	(BB, %eax, 4), BB
#endif

#if defined(LN) || defined(LT)
#ifdef	movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	 0 * SIZE(B), %xmm2

	subps	%xmm4,  %xmm2
#else
#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	 0 * SIZE(AA), %xmm1

	subps	%xmm4,  %xmm1
#endif

#if defined(LN) || defined(LT)
	movaps	 0 * SIZE(AA), %xmm5

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
	movaps	 0 * SIZE(B), %xmm4

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
	movlps	%xmm2,   0 * SIZE(B)

	pshufd	$0x00, %xmm2, %xmm0
	pshufd	$0x55, %xmm2, %xmm1

	movaps	%xmm0,   0 * SIZE(BB)
	movaps	%xmm1,   4 * SIZE(BB)

	movlps	%xmm2,   0 * SIZE(CO1)
#else
	movlps	%xmm1,   0 * SIZE(AA)

	movlps	%xmm1,   0 * SIZE(CO1)
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

.L149:
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
	ALIGN_4

.L999:
	EMMS

	movl	OLD_STACK, %esp
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
