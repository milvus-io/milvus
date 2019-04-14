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

#define OLD_M	 4 + STACK(%esi)
#define OLD_N	 8 + STACK(%esi)
#define OLD_K	12 + STACK(%esi)
#define OLD_A	20 + STACK(%esi)
#define OLD_B	24 + STACK(%esi)
#define OLD_C	28 + STACK(%esi)
#define OLD_LDC	32 + STACK(%esi)
#define STACK_OFFT	36 + STACK(%esi)

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

#if defined(OPTERON) || defined(BARCELONA) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
#define PREFETCH     prefetch
#define PREFETCHW    prefetchw
#define PREFETCHSIZE (16 * 10 + 8)
#endif

#if defined(PENTIUM4) || defined(PENTIUMM)
#define PREFETCH     prefetcht0
#define PREFETCHW    prefetcht0
#define PREFETCHSIZE   96
#endif

#if defined(PENRYN) || defined(DUNNINGTON)
#define PREFETCH     prefetcht0
#define PREFETCHW    prefetcht0
#define PREFETCHSIZE   96
#endif

#define B	%edi
#define AA	%edx
#define	BB	%ecx
#define LDC	%ebp
#define CO1	%esi

#if defined(OPTERON) || !defined(HAVE_SSE2)
#define movsd	movlps
#endif

#ifdef HAVE_SSE2
#define	xorps	pxor
#endif

#define KERNEL1(address) \
	mulps	%xmm0, %xmm2; \
	addps	%xmm2, %xmm4; \
	movaps	 4 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	PREFETCH (PREFETCHSIZE +  0) * SIZE + (address) * 1 * SIZE(AA); \
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

	movl	%esp, %esi

	subl	$128 + LOCAL_BUFFER_SIZE, %esp
	andl	$-1024, %esp

	STACK_TOUCHING

	movl	OLD_M, %ebx
	movl	OLD_N, %eax
	movl	OLD_K, %ecx
	movl	OLD_A, %edx

	movl	%ebx, M
	movl	%eax, N
	movl	%ecx, K
	movl	%edx, A
	movl	%esi, OLD_STACK
	movss	STACK_OFFT, %xmm4

	movl	OLD_B, B
	movl	OLD_C, %ebx

	movl	%ebx, C
	movl	OLD_LDC, LDC

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

	movl	N, %eax
	sarl	$2, %eax
	movl	%eax, J
	jle	.L40

.L01:
#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

	leal	BUFFER, %ecx

#ifdef RT
       movl	K, %eax
       sall	$2 + BASE_SHIFT, %eax
       subl	%eax, B
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	movl	B, BORIG
        sall	$2 + BASE_SHIFT, %eax
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
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$1, %eax
	jle	.L05
	ALIGN_4

.L02:
	movaps	 0 * SIZE(B), %xmm3
	movaps	 4 * SIZE(B), %xmm7

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

	pshufd	 $0x00, %xmm7, %xmm4
	pshufd	 $0x55, %xmm7, %xmm5
	pshufd	 $0xaa, %xmm7, %xmm6
	pshufd	 $0xff, %xmm7, %xmm7

	movaps	%xmm0,  0 * SIZE(BB)
	movaps	%xmm1,  4 * SIZE(BB)
	movaps	%xmm2,  8 * SIZE(BB)
	movaps	%xmm3, 12 * SIZE(BB)
	movaps	%xmm4, 16 * SIZE(BB)
	movaps	%xmm5, 20 * SIZE(BB)
	movaps	%xmm6, 24 * SIZE(BB)
	movaps	%xmm7, 28 * SIZE(BB)

	addl	$ 8 * SIZE, B
	addl	$32 * SIZE, %ecx
	decl	%eax
	jne	.L02
	ALIGN_2

.L05:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$1, %eax
	BRANCH
	jle	.L10

	movaps	 0 * SIZE(B), %xmm3

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

	movaps	%xmm0,  0 * SIZE(BB)
	movaps	%xmm1,  4 * SIZE(BB)
	movaps	%xmm2,  8 * SIZE(BB)
	movaps	%xmm3, 12 * SIZE(BB)

	addl	$4 * SIZE, B
	ALIGN_4

.L10:
#if defined(LT) || defined(RN)
	movl	A, AA
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

	leal	(, LDC, 4), %eax

#ifdef RT
	subl	%eax, C
#endif
	movl	C, CO1
#ifndef RT
	addl	%eax, C
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

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$2 + BASE_SHIFT, %eax
	leal	(BB, %eax, 4), BB
#endif

	movss	 0 * SIZE(AA), %xmm0
	xorps	%xmm4, %xmm4
	movss	 4 * SIZE(AA), %xmm1
	xorps	%xmm5, %xmm5
	movss	 0 * SIZE(BB), %xmm2
	xorps	%xmm6, %xmm6
	movss	16 * SIZE(BB), %xmm3
	xorps	%xmm7, %xmm7

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
	mulss	%xmm0, %xmm2
	addss	%xmm2, %xmm4
#if defined(OPTERON) || defined(BARCELONA) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
	movss	 4 * SIZE(BB), %xmm2
	mulss	%xmm0, %xmm2
	addss	%xmm2, %xmm5
	movss	 8 * SIZE(BB), %xmm2
	mulss	%xmm0, %xmm2
	mulss	12 * SIZE(BB), %xmm0
	addss	%xmm2, %xmm6
	movss	32 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm7
	movss	 1 * SIZE(AA), %xmm0

	mulss	%xmm0, %xmm3
	addss	%xmm3, %xmm4
	movss	20 * SIZE(BB), %xmm3
	mulss	%xmm0, %xmm3
	addss	%xmm3, %xmm5
	movss	24 * SIZE(BB), %xmm3
	mulss	%xmm0, %xmm3
	mulss	28 * SIZE(BB), %xmm0
	addss	%xmm3, %xmm6
	movss	48 * SIZE(BB), %xmm3
	addss	%xmm0, %xmm7
	movss	 2 * SIZE(AA), %xmm0

	mulss	%xmm0, %xmm2
	addss	%xmm2, %xmm4
	movss	36 * SIZE(BB), %xmm2
	mulss	%xmm0, %xmm2
	addss	%xmm2, %xmm5
	movss	40 * SIZE(BB), %xmm2
	mulss	%xmm0, %xmm2
	mulss	44 * SIZE(BB), %xmm0
	addss	%xmm2, %xmm6
	movss	64 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm7
	movss	 3 * SIZE(AA), %xmm0

	mulss	%xmm0, %xmm3
	addss	%xmm3, %xmm4
	movss	52 * SIZE(BB), %xmm3
	mulss	%xmm0, %xmm3
	addss	%xmm3, %xmm5
	movss	56 * SIZE(BB), %xmm3
	mulss	%xmm0, %xmm3
	mulss	60 * SIZE(BB), %xmm0
	addss	%xmm3, %xmm6
	movss	80 * SIZE(BB), %xmm3
	addss	%xmm0, %xmm7
	movss	 8 * SIZE(AA), %xmm0

	mulss	%xmm1, %xmm2
	addss	%xmm2, %xmm4
	movss	68 * SIZE(BB), %xmm2
	mulss	%xmm1, %xmm2
	addss	%xmm2, %xmm5
	movss	72 * SIZE(BB), %xmm2
	mulss	%xmm1, %xmm2
	mulss	76 * SIZE(BB), %xmm1
	addss	%xmm2, %xmm6
	movss	96 * SIZE(BB), %xmm2
	addss	%xmm1, %xmm7
	movss	 5 * SIZE(AA), %xmm1

	mulss	%xmm1, %xmm3
	addss	%xmm3, %xmm4
	movss	84 * SIZE(BB), %xmm3
	mulss	%xmm1, %xmm3
	addss	%xmm3, %xmm5
	movss	88 * SIZE(BB), %xmm3
	mulss	%xmm1, %xmm3
	mulss	92 * SIZE(BB), %xmm1
	addss	%xmm3, %xmm6
	movss	112 * SIZE(BB), %xmm3
	addss	%xmm1, %xmm7
	movss	 6 * SIZE(AA), %xmm1

	mulss	%xmm1, %xmm2
	addss	%xmm2, %xmm4
	movss	100 * SIZE(BB), %xmm2
	mulss	%xmm1, %xmm2
	addss	%xmm2, %xmm5
	movss	104 * SIZE(BB), %xmm2
	mulss	%xmm1, %xmm2
	mulss	108 * SIZE(BB), %xmm1
	addss	%xmm2, %xmm6
	movss	128 * SIZE(BB), %xmm2
	addss	%xmm1, %xmm7
	movss	 7 * SIZE(AA), %xmm1

	mulss	%xmm1, %xmm3
	addss	%xmm3, %xmm4
	movss	116 * SIZE(BB), %xmm3
	mulss	%xmm1, %xmm3
	addss	%xmm3, %xmm5
	movss	120 * SIZE(BB), %xmm3
	mulss	%xmm1, %xmm3
	mulss	124 * SIZE(BB), %xmm1
	addss	%xmm3, %xmm6
	movss	144 * SIZE(BB), %xmm3
	addss	%xmm1, %xmm7
	movss	12 * SIZE(AA), %xmm1

	addl	$  8 * SIZE, AA
	addl	$128 * SIZE, BB
	decl   %eax
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
	mulss	%xmm0, %xmm2
	addss	%xmm2, %xmm4
	movss	 4 * SIZE(BB), %xmm2
	mulss	%xmm0, %xmm2
	addss	%xmm2, %xmm5
	movss	 8 * SIZE(BB), %xmm2
	mulss	%xmm0, %xmm2
	mulss	12 * SIZE(BB), %xmm0
	addss	%xmm2, %xmm6
	movss	16 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm7
	movss	 1 * SIZE(AA), %xmm0

	addl	$ 1 * SIZE, AA
	addl	$16 * SIZE, BB
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
	movl	BORIG, B
	leal	BUFFER, BB

	leal	(AA, %eax, SIZE), AA

	sall	$2 + BASE_SHIFT, %eax
	leal	(B,  %eax, 1), B
	leal	(BB, %eax, 4), BB
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm6, %xmm4
	unpcklps %xmm7, %xmm5
	unpcklps %xmm5, %xmm4

	movaps	 0 * SIZE(B), %xmm1

	subps	%xmm4,  %xmm1
#else
	movss	 0 * SIZE(AA), %xmm0
	movss	 1 * SIZE(AA), %xmm1
	movss	 2 * SIZE(AA), %xmm2
	movss	 3 * SIZE(AA), %xmm3

	subss	%xmm4, %xmm0
	subss	%xmm5, %xmm1
	subss	%xmm6, %xmm2
	subss	%xmm7, %xmm3
#endif

#if defined(LN) || defined(LT)
	movss	  0 * SIZE(AA), %xmm4
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm6, %xmm1
#endif

#ifdef RN
	movaps	 0 * SIZE(B), %xmm6
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

	movaps	 4 * SIZE(B), %xmm6
	pshufd	 $0x55, %xmm6, %xmm7
	mulss	 %xmm7, %xmm1
	pshufd	 $0xaa, %xmm6, %xmm7
	mulss	 %xmm1, %xmm7
	subss	 %xmm7, %xmm2
	pshufd	 $0xff, %xmm6, %xmm7
	mulss	 %xmm1, %xmm7
	subss	 %xmm7, %xmm3

	movaps	 8 * SIZE(B), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm7
	mulss	 %xmm7, %xmm2
	pshufd	 $0xff, %xmm6, %xmm7
	mulss	 %xmm2, %xmm7
	subss	 %xmm7, %xmm3

	movaps	 12 * SIZE(B), %xmm6
	pshufd	 $0xff, %xmm6, %xmm7
	mulss	 %xmm7, %xmm3
#endif

#ifdef RT
	movaps	 12 * SIZE(B), %xmm6
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

	movaps	  8 * SIZE(B), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm7
	mulss	 %xmm7, %xmm2
	pshufd	 $0x55, %xmm6, %xmm7
	mulss	 %xmm2, %xmm7
	subss	 %xmm7, %xmm1
	pshufd	 $0x00, %xmm6, %xmm7
	mulss	 %xmm2, %xmm7
	subss	 %xmm7, %xmm0

	movaps	  4 * SIZE(B), %xmm6
	pshufd	 $0x55, %xmm6, %xmm7
	mulss	 %xmm7, %xmm1
	pshufd	 $0x00, %xmm6, %xmm7
	mulss	 %xmm1, %xmm7
	subss	 %xmm7, %xmm0

	movaps	  0 * SIZE(B), %xmm6
	pshufd	 $0x00, %xmm6, %xmm7
	mulss	 %xmm7, %xmm0
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm1,   0 * SIZE(B)

	pshufd	$0x00, %xmm1, %xmm0
	pshufd	$0x55, %xmm1, %xmm2
	pshufd	$0xaa, %xmm1, %xmm4
	pshufd	$0xff, %xmm1, %xmm6
	movaps	%xmm0,   0 * SIZE(BB)
	movaps	%xmm2,   4 * SIZE(BB)
	movaps	%xmm4,   8 * SIZE(BB)
	movaps	%xmm6,  12 * SIZE(BB)
#else
	movss	%xmm0,   0 * SIZE(AA)
	movss	%xmm1,   1 * SIZE(AA)
	movss	%xmm2,   2 * SIZE(AA)
	movss	%xmm3,   3 * SIZE(AA)
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
	leal	(AA, %eax, SIZE), AA
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

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$2 + BASE_SHIFT, %eax
	leal	(BB, %eax, 4), BB
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
	je	.L25
	ALIGN_4

.L22:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
#if defined(OPTERON) || defined(BARCELONA) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm6
	movaps	12 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm7
	movaps	32 * SIZE(BB), %xmm2

	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm4
	movaps	20 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm5
	movaps	24 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm6
	movaps	28 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movsd	 4 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm7
	movaps	48 * SIZE(BB), %xmm3

	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	36 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm5
	movaps	40 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm6
	movaps	44 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 6 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm7
	movaps	64 * SIZE(BB), %xmm2

	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm4
	movaps	52 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm5
	movaps	56 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm6
	movaps	60 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movsd	 16 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm7
	movaps	80 * SIZE(BB), %xmm3

	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm4
	movaps	68 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm5
	movaps	72 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm6
	movaps	76 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movsd	10 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm7
	movaps	96 * SIZE(BB), %xmm2

	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movaps	84 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm5
	movaps	88 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm6
	movaps	92 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	12 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm7
	movaps	112 * SIZE(BB), %xmm3

	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm4
	movaps	100 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm5
	movaps	104 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm6
	movaps	108 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movsd	 14 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm7
	movaps	128 * SIZE(BB), %xmm2

	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movaps	116 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm5
	movaps	120 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm6
	movaps	124 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	 24 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm7
	movaps	144 * SIZE(BB), %xmm3

	addl	$ 16 * SIZE, AA
	addl	$128 * SIZE, BB
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
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L28
	ALIGN_4

.L26:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm6
	movaps	12 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm7
	movaps	16 * SIZE(BB), %xmm2

	addl	$ 2 * SIZE, AA
	addl	$16 * SIZE, BB
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
	movl	BORIG, B
	leal	BUFFER, BB

	sall	$1 + BASE_SHIFT, %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 2), B
	leal	(BB, %eax, 8), BB
#endif

#if defined(LN) || defined(LT)
	unpcklps %xmm6, %xmm4
	unpcklps %xmm7, %xmm5

	movaps	 %xmm4, %xmm6
	unpcklps %xmm5, %xmm4
	unpckhps %xmm5, %xmm6

	movaps	 0 * SIZE(B), %xmm1
	movaps	 4 * SIZE(B), %xmm3

	subps	%xmm4,  %xmm1
	subps	%xmm6,  %xmm3
#else
#ifdef	movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	 0 * SIZE(AA), %xmm0
#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	 2 * SIZE(AA), %xmm1
#ifdef	movsd
	xorps	%xmm2, %xmm2
#endif
	movsd	 4 * SIZE(AA), %xmm2
#ifdef	movsd
	xorps	%xmm3, %xmm3
#endif
	movsd	 6 * SIZE(AA), %xmm3

	subps	%xmm4, %xmm0
	subps	%xmm5, %xmm1
	subps	%xmm6, %xmm2
	subps	%xmm7, %xmm3
#endif

#ifdef LN
	movaps	  0 * SIZE(AA), %xmm4
	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm6, %xmm3
	pshufd	 $0xaa, %xmm4, %xmm6
	mulps	 %xmm3, %xmm6
	subps	 %xmm6, %xmm1

	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm6, %xmm1
#endif

#ifdef LT
	movaps	 0 * SIZE(AA), %xmm4
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm6, %xmm1

	pshufd	 $0x55, %xmm4, %xmm6
	mulps	 %xmm1, %xmm6
	subps	 %xmm6, %xmm3

	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm6, %xmm3
#endif

#ifdef RN
	movaps	 0 * SIZE(B), %xmm6
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

	movaps	 4 * SIZE(B), %xmm6
	pshufd	 $0x55, %xmm6, %xmm7
	mulps	 %xmm7, %xmm1
	pshufd	 $0xaa, %xmm6, %xmm7
	mulps	 %xmm1, %xmm7
	subps	 %xmm7, %xmm2
	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm1, %xmm7
	subps	 %xmm7, %xmm3

	movaps	 8 * SIZE(B), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm7
	mulps	 %xmm7, %xmm2
	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm2, %xmm7
	subps	 %xmm7, %xmm3

	movaps	 12 * SIZE(B), %xmm6
	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm7, %xmm3
#endif

#ifdef RT
	movaps	 12 * SIZE(B), %xmm6
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

	movaps	  8 * SIZE(B), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm7
	mulps	 %xmm7, %xmm2
	pshufd	 $0x55, %xmm6, %xmm7
	mulps	 %xmm2, %xmm7
	subps	 %xmm7, %xmm1
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm2, %xmm7
	subps	 %xmm7, %xmm0

	movaps	  4 * SIZE(B), %xmm6
	pshufd	 $0x55, %xmm6, %xmm7
	mulps	 %xmm7, %xmm1
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm1, %xmm7
	subps	 %xmm7, %xmm0

	movaps	  0 * SIZE(B), %xmm6
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm7, %xmm0
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm1,   0 * SIZE(B)
	movaps	%xmm3,   4 * SIZE(B)

	pshufd	$0x00, %xmm1, %xmm0
	pshufd	$0x55, %xmm1, %xmm2
	pshufd	$0xaa, %xmm1, %xmm4
	pshufd	$0xff, %xmm1, %xmm6
	movaps	%xmm0,   0 * SIZE(BB)
	movaps	%xmm2,   4 * SIZE(BB)
	movaps	%xmm4,   8 * SIZE(BB)
	movaps	%xmm6,  12 * SIZE(BB)

	pshufd	$0x00, %xmm3, %xmm0
	pshufd	$0x55, %xmm3, %xmm2
	pshufd	$0xaa, %xmm3, %xmm4
	pshufd	$0xff, %xmm3, %xmm6
	movaps	%xmm0,  16 * SIZE(BB)
	movaps	%xmm2,  20 * SIZE(BB)
	movaps	%xmm4,  24 * SIZE(BB)
	movaps	%xmm6,  28 * SIZE(BB)
#else
	movlps	%xmm0,   0 * SIZE(AA)
	movlps	%xmm1,   2 * SIZE(AA)
	movlps	%xmm2,   4 * SIZE(AA)
	movlps	%xmm3,   6 * SIZE(AA)
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

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$2 + BASE_SHIFT, %eax
	leal	(BB, %eax, 4), BB
#endif

	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm4, %xmm4
	movaps	16 * SIZE(AA), %xmm1
	xorps	%xmm5, %xmm5
	movaps	 0 * SIZE(BB), %xmm2
	xorps	%xmm6, %xmm6
	movaps	16 * SIZE(BB), %xmm3
	xorps	%xmm7, %xmm7

	leal	(LDC, LDC, 2), %eax

	PREFETCHW	-4 * SIZE(CO1)
	PREFETCHW	-4 * SIZE(CO1, LDC)
	PREFETCHW	-4 * SIZE(CO1, LDC, 2)
	PREFETCHW	-4 * SIZE(CO1, %eax)

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
	KERNEL1(0 * 16)
	KERNEL2(0 * 16)
	KERNEL3(0 * 16)
	KERNEL4(0 * 16)
	KERNEL5(0 * 16)
	KERNEL6(0 * 16)
	KERNEL7(0 * 16)
	KERNEL8(0 * 16)

	addl   $128 * SIZE, BB
	addl   $32 * SIZE, AA
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
	ALIGN_4

.L16:
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
	movl	BORIG, B
	leal	BUFFER, BB

	sall	$2 + BASE_SHIFT, %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 1), B
	leal	(BB, %eax, 4), BB
#endif

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

	movaps	 0 * SIZE(B), %xmm1
	movaps	 4 * SIZE(B), %xmm3
	movaps	 8 * SIZE(B), %xmm5
	movaps	12 * SIZE(B), %xmm7

	subps	%xmm4,  %xmm1
	subps	%xmm6,  %xmm3
	subps	%xmm0,  %xmm5
	subps	%xmm2,  %xmm7
#else
	movaps	 0 * SIZE(AA), %xmm0
	movaps	 4 * SIZE(AA), %xmm1
	movaps	 8 * SIZE(AA), %xmm2
	movaps	12 * SIZE(AA), %xmm3

	subps	%xmm4, %xmm0
	subps	%xmm5, %xmm1
	subps	%xmm6, %xmm2
	subps	%xmm7, %xmm3
#endif

#ifdef LN
	movaps	 12 * SIZE(AA), %xmm4
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

	movaps	  8 * SIZE(AA), %xmm4
	pshufd	 $0xaa, %xmm4, %xmm6
	mulps	 %xmm6, %xmm5
	pshufd	 $0x55, %xmm4, %xmm6
	mulps	 %xmm5, %xmm6
	subps	 %xmm6, %xmm3
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm5, %xmm6
	subps	 %xmm6, %xmm1

	movaps	  4 * SIZE(AA), %xmm4
	pshufd	 $0x55, %xmm4, %xmm6
	mulps	 %xmm6, %xmm3
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm3, %xmm6
	subps	 %xmm6, %xmm1

	movaps	  0 * SIZE(AA), %xmm4
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm6, %xmm1
#endif

#ifdef LT
	movaps	 0 * SIZE(AA), %xmm4
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

	movaps	 4 * SIZE(AA), %xmm4
	pshufd	 $0x55, %xmm4, %xmm6
	mulps	 %xmm6, %xmm3
	pshufd	 $0xaa, %xmm4, %xmm6
	mulps	 %xmm3, %xmm6
	subps	 %xmm6, %xmm5
	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm3, %xmm6
	subps	 %xmm6, %xmm7

	movaps	 8 * SIZE(AA), %xmm4
	pshufd	 $0xaa, %xmm4, %xmm6
	mulps	 %xmm6, %xmm5
	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm5, %xmm6
	subps	 %xmm6, %xmm7

	movaps	12 * SIZE(AA), %xmm4
	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm6, %xmm7
#endif

#ifdef RN
	movaps	 0 * SIZE(B), %xmm6
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

	movaps	 4 * SIZE(B), %xmm6
	pshufd	 $0x55, %xmm6, %xmm7
	mulps	 %xmm7, %xmm1
	pshufd	 $0xaa, %xmm6, %xmm7
	mulps	 %xmm1, %xmm7
	subps	 %xmm7, %xmm2
	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm1, %xmm7
	subps	 %xmm7, %xmm3

	movaps	 8 * SIZE(B), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm7
	mulps	 %xmm7, %xmm2
	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm2, %xmm7
	subps	 %xmm7, %xmm3

	movaps	 12 * SIZE(B), %xmm6
	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm7, %xmm3
#endif

#ifdef RT
	movaps	 12 * SIZE(B), %xmm6
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

	movaps	  8 * SIZE(B), %xmm6
	pshufd	 $0xaa, %xmm6, %xmm7
	mulps	 %xmm7, %xmm2
	pshufd	 $0x55, %xmm6, %xmm7
	mulps	 %xmm2, %xmm7
	subps	 %xmm7, %xmm1
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm2, %xmm7
	subps	 %xmm7, %xmm0

	movaps	  4 * SIZE(B), %xmm6
	pshufd	 $0x55, %xmm6, %xmm7
	mulps	 %xmm7, %xmm1
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm1, %xmm7
	subps	 %xmm7, %xmm0

	movaps	  0 * SIZE(B), %xmm6
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm7, %xmm0
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm1,   0 * SIZE(B)
	movaps	%xmm3,   4 * SIZE(B)
	movaps	%xmm5,   8 * SIZE(B)
	movaps	%xmm7,  12 * SIZE(B)

	pshufd	$0x00, %xmm1, %xmm0
	pshufd	$0x55, %xmm1, %xmm2
	pshufd	$0xaa, %xmm1, %xmm4
	pshufd	$0xff, %xmm1, %xmm6
	movaps	%xmm0,   0 * SIZE(BB)
	movaps	%xmm2,   4 * SIZE(BB)
	movaps	%xmm4,   8 * SIZE(BB)
	movaps	%xmm6,  12 * SIZE(BB)

	pshufd	$0x00, %xmm3, %xmm0
	pshufd	$0x55, %xmm3, %xmm2
	pshufd	$0xaa, %xmm3, %xmm4
	pshufd	$0xff, %xmm3, %xmm6
	movaps	%xmm0,  16 * SIZE(BB)
	movaps	%xmm2,  20 * SIZE(BB)
	movaps	%xmm4,  24 * SIZE(BB)
	movaps	%xmm6,  28 * SIZE(BB)

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm2
	pshufd	$0xaa, %xmm5, %xmm4
	pshufd	$0xff, %xmm5, %xmm6
	movaps	%xmm0,  32 * SIZE(BB)
	movaps	%xmm2,  36 * SIZE(BB)
	movaps	%xmm4,  40 * SIZE(BB)
	movaps	%xmm6,  44 * SIZE(BB)

	pshufd	$0x00, %xmm7, %xmm0
	pshufd	$0x55, %xmm7, %xmm2
	pshufd	$0xaa, %xmm7, %xmm4
	pshufd	$0xff, %xmm7, %xmm6
	movaps	%xmm0,  48 * SIZE(BB)
	movaps	%xmm2,  52 * SIZE(BB)
	movaps	%xmm4,  56 * SIZE(BB)
	movaps	%xmm6,  60 * SIZE(BB)
#else
	movaps	%xmm0,   0 * SIZE(AA)
	movaps	%xmm1,   4 * SIZE(AA)
	movaps	%xmm2,   8 * SIZE(AA)
	movaps	%xmm3,  12 * SIZE(AA)
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
#ifdef LT
	addl	$16 * SIZE, B
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
	jg	.L11
	ALIGN_4

.L39:
#ifdef LN
       movl	K, %eax
       leal	(, %eax, SIZE), %eax
       leal 	(B, %eax, 4), B
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(B,  %eax, 4), B
#endif

#ifdef RN
	addl	$4, KK
#endif

#ifdef RT
	subl	$4, KK
#endif

	decl	J			# j --
	jg	.L01
	ALIGN_4

.L40:
	testl	$2, N
	je	.L80

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
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$2, %eax
	jle	.L45
	ALIGN_4

.L42:
	movaps	 0 * SIZE(B), %xmm3
	movaps	 4 * SIZE(B), %xmm7

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

	pshufd	 $0x00, %xmm7, %xmm4
	pshufd	 $0x55, %xmm7, %xmm5
	pshufd	 $0xaa, %xmm7, %xmm6
	pshufd	 $0xff, %xmm7, %xmm7

	movaps	%xmm0,  0 * SIZE(BB)
	movaps	%xmm1,  4 * SIZE(BB)
	movaps	%xmm2,  8 * SIZE(BB)
	movaps	%xmm3, 12 * SIZE(BB)
	movaps	%xmm4, 16 * SIZE(BB)
	movaps	%xmm5, 20 * SIZE(BB)
	movaps	%xmm6, 24 * SIZE(BB)
	movaps	%xmm7, 28 * SIZE(BB)

	addl	$ 8 * SIZE, B
	addl	$32 * SIZE, %ecx
	decl	%eax
	jne	.L42
	ALIGN_4

.L45:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K, %eax
	subl	KK, %eax
#endif
	andl	$3, %eax
	BRANCH
	jle	.L50
	ALIGN_4

.L46:
#ifdef	movsd
	xorps	%xmm3, %xmm3
#endif
	movsd	 0 * SIZE(B), %xmm3

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1

	movaps	%xmm0,  0 * SIZE(BB)
	movaps	%xmm1,  4 * SIZE(BB)

	addl	$2 * SIZE, B
	addl	$8 * SIZE, %ecx
	decl	%eax
	jne	.L46
	ALIGN_4

.L50:
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

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + BASE_SHIFT, %eax
	leal	(BB, %eax, 4), BB
#endif

	xorps	%xmm4, %xmm4
	xorps	%xmm5, %xmm5
	xorps	%xmm6, %xmm6
	xorps	%xmm7, %xmm7

	movss	 0 * SIZE(AA), %xmm0
	movss	 4 * SIZE(AA), %xmm1
	movss	 0 * SIZE(BB), %xmm2
	movss	16 * SIZE(BB), %xmm3

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
	mulss	%xmm0, %xmm2
#if defined(OPTERON) || defined(BARCELONA) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
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

	addl	$ 8 * SIZE, AA
	addl	$64 * SIZE, BB
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
	ALIGN_4

.L76:
	mulss	%xmm0, %xmm2
	mulss	 4 * SIZE(BB), %xmm0
	addss	%xmm2, %xmm4
	movss	 8 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm5
	movss	 1 * SIZE(AA), %xmm0

	addl	$ 1 * SIZE, AA
	addl	$ 8 * SIZE, BB
	decl	%eax
	jg	.L76
	ALIGN_4

.L78:
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
	xorps	%xmm1, %xmm1
#endif
	movsd	 0 * SIZE(B), %xmm1

	subps	%xmm4,  %xmm1
#else
	movss	 0 * SIZE(AA), %xmm0
	movss	 1 * SIZE(AA), %xmm1

	subss	%xmm4, %xmm0
	subss	%xmm5, %xmm1
#endif

#if defined(LN) || defined(LT)
	movss	  0 * SIZE(AA), %xmm4
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm6, %xmm1
#endif

#ifdef RN
	movaps	 0 * SIZE(B), %xmm6
	pshufd	 $0x00, %xmm6, %xmm7
	mulss	 %xmm7, %xmm0
	pshufd	 $0x55, %xmm6, %xmm7
	mulss	 %xmm0, %xmm7
	subss	 %xmm7, %xmm1

	pshufd	 $0xff, %xmm6, %xmm7
	mulss	 %xmm7, %xmm1
#endif

#ifdef RT
	movaps	  0 * SIZE(B), %xmm6
	pshufd	 $0xff, %xmm6, %xmm7
	mulss	 %xmm7, %xmm1
	pshufd	 $0xaa, %xmm6, %xmm7
	mulss	 %xmm1, %xmm7
	subss	 %xmm7, %xmm0

	pshufd	 $0x00, %xmm6, %xmm7
	mulss	 %xmm7, %xmm0
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm1,   0 * SIZE(B)

	pshufd	$0x00, %xmm1, %xmm0
	pshufd	$0x55, %xmm1, %xmm2
	movaps	%xmm0,   0 * SIZE(BB)
	movaps	%xmm2,   4 * SIZE(BB)
#else
	movss	%xmm0,   0 * SIZE(AA)
	movss	%xmm1,   1 * SIZE(AA)
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
	leal	(AA, %eax, SIZE), AA
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

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + BASE_SHIFT, %eax
	leal	(BB, %eax, 4), BB
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
	je	.L65
	ALIGN_4

.L62:
#if defined(OPTERON) || defined(BARCELONA) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
#endif

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

	addl	$16 * SIZE, AA
	addl	$64 * SIZE, BB
	decl   %eax
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
	jg	.L66
	ALIGN_4

.L68:
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
	unpcklps %xmm6, %xmm4
	unpcklps %xmm7, %xmm5

	movaps	 %xmm4, %xmm6
	unpcklps %xmm5, %xmm4
	unpckhps %xmm5, %xmm6

#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	 0 * SIZE(B), %xmm1
#ifdef	movsd
	xorps	%xmm3, %xmm3
#endif
	movsd	 2 * SIZE(B), %xmm3

	subps	%xmm4,  %xmm1
	subps	%xmm6,  %xmm3
#else
#ifdef	movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	 0 * SIZE(AA), %xmm0
#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	 2 * SIZE(AA), %xmm1

	subps	%xmm4, %xmm0
	subps	%xmm5, %xmm1
#endif

#ifdef LN
	movaps	  0 * SIZE(AA), %xmm4
	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm6, %xmm3
	pshufd	 $0xaa, %xmm4, %xmm6
	mulps	 %xmm3, %xmm6
	subps	 %xmm6, %xmm1

	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm6, %xmm1
#endif

#ifdef LT
	movaps	 0 * SIZE(AA), %xmm4
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm6, %xmm1
	pshufd	 $0x55, %xmm4, %xmm6
	mulps	 %xmm1, %xmm6
	subps	 %xmm6, %xmm3

	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm6, %xmm3
#endif

#ifdef RN
	movaps	 0 * SIZE(B), %xmm6
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm7, %xmm0
	pshufd	 $0x55, %xmm6, %xmm7
	mulps	 %xmm0, %xmm7
	subps	 %xmm7, %xmm1

	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm7, %xmm1
#endif

#ifdef RT
	movaps	  0 * SIZE(B), %xmm6
	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm7, %xmm1
	pshufd	 $0xaa, %xmm6, %xmm7
	mulps	 %xmm1, %xmm7
	subps	 %xmm7, %xmm0

	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm7, %xmm0
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm1,   0 * SIZE(B)
	movlps	%xmm3,   2 * SIZE(B)

	pshufd	$0x00, %xmm1, %xmm0
	pshufd	$0x55, %xmm1, %xmm2
	movaps	%xmm0,   0 * SIZE(BB)
	movaps	%xmm2,   4 * SIZE(BB)

	pshufd	$0x00, %xmm3, %xmm0
	pshufd	$0x55, %xmm3, %xmm2
	movaps	%xmm0,   8 * SIZE(BB)
	movaps	%xmm2,  12 * SIZE(BB)
#else
	movlps	%xmm0,   0 * SIZE(AA)
	movlps	%xmm1,   2 * SIZE(AA)
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

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$1 + BASE_SHIFT, %eax
	leal	(BB, %eax, 4), BB
#endif

	xorps	%xmm4, %xmm4
	xorps	%xmm5, %xmm5
	xorps	%xmm6, %xmm6
	xorps	%xmm7, %xmm7

	movaps	 0 * SIZE(AA), %xmm0
	movaps	16 * SIZE(AA), %xmm1
	movaps	 0 * SIZE(BB), %xmm2
	movaps	16 * SIZE(BB), %xmm3

	PREFETCHW	-4 * SIZE(CO1)
	PREFETCHW	-4 * SIZE(CO1, LDC)

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
	mulps	%xmm0, %xmm2
#if defined(OPTERON) || defined(BARCELONA) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	 8 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	 4 * SIZE(AA), %xmm0

	mulps	%xmm0, %xmm2
	mulps	12 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	32 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	 8 * SIZE(AA), %xmm0

	mulps	%xmm0, %xmm3
	mulps	20 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm4
	movaps	24 * SIZE(BB), %xmm3
	addps	%xmm0, %xmm5
	movaps	12 * SIZE(AA), %xmm0

	mulps	%xmm0, %xmm3
	mulps	28 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm4
	movaps	48 * SIZE(BB), %xmm3
	addps	%xmm0, %xmm5
	movaps	32 * SIZE(AA), %xmm0

#if defined(OPTERON) || defined(BARCELONA) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
	prefetcht0 (PREFETCHSIZE  + 16) * SIZE(AA)
#endif
	mulps	%xmm1, %xmm2
	mulps	36 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm4
	movaps	40 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm5
	movaps	20 * SIZE(AA), %xmm1

	mulps	%xmm1, %xmm2
	mulps	44 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm4
	movaps	64 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm5
	movaps	24 * SIZE(AA), %xmm1

	mulps	%xmm1, %xmm3
	mulps	52 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm4
	movaps	56 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm5
	movaps	28 * SIZE(AA), %xmm1

	mulps	%xmm1, %xmm3
	mulps	60 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm4
	movaps	80 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm5
	movaps	48 * SIZE(AA), %xmm1

	addl	$32 * SIZE, AA
	addl	$64 * SIZE, BB
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
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L58
	ALIGN_4

.L56:
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	 8 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	 4 * SIZE(AA), %xmm0

	addl	$4 * SIZE, AA
	addl	$8 * SIZE, BB
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
	movl	BORIG, B
	leal	BUFFER, BB

	sall	$1 + BASE_SHIFT, %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 1), B
	leal	(BB, %eax, 4), BB
#endif

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

#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	 0 * SIZE(B), %xmm1
#ifdef	movsd
	xorps	%xmm3, %xmm3
#endif
	movsd	 2 * SIZE(B), %xmm3
#ifdef	movsd
	xorps	%xmm5, %xmm5
#endif
	movsd	 4 * SIZE(B), %xmm5
#ifdef	movsd
	xorps	%xmm7, %xmm7
#endif
	movsd	 6 * SIZE(B), %xmm7

	subps	%xmm4,  %xmm1
	subps	%xmm6,  %xmm3
	subps	%xmm0,  %xmm5
	subps	%xmm2,  %xmm7
#else
	movaps	 0 * SIZE(AA), %xmm0
	movaps	 4 * SIZE(AA), %xmm1

	subps	%xmm4, %xmm0
	subps	%xmm5, %xmm1
#endif

#ifdef LN
	movaps	 12 * SIZE(AA), %xmm4
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

	movaps	  8 * SIZE(AA), %xmm4
	pshufd	 $0xaa, %xmm4, %xmm6
	mulps	 %xmm6, %xmm5
	pshufd	 $0x55, %xmm4, %xmm6
	mulps	 %xmm5, %xmm6
	subps	 %xmm6, %xmm3
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm5, %xmm6
	subps	 %xmm6, %xmm1

	movaps	  4 * SIZE(AA), %xmm4
	pshufd	 $0x55, %xmm4, %xmm6
	mulps	 %xmm6, %xmm3
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm3, %xmm6
	subps	 %xmm6, %xmm1

	movaps	  0 * SIZE(AA), %xmm4
	pshufd	 $0x00, %xmm4, %xmm6
	mulps	 %xmm6, %xmm1
#endif

#ifdef LT
	movaps	 0 * SIZE(AA), %xmm4
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

	movaps	 4 * SIZE(AA), %xmm4
	pshufd	 $0x55, %xmm4, %xmm6
	mulps	 %xmm6, %xmm3
	pshufd	 $0xaa, %xmm4, %xmm6
	mulps	 %xmm3, %xmm6
	subps	 %xmm6, %xmm5
	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm3, %xmm6
	subps	 %xmm6, %xmm7

	movaps	 8 * SIZE(AA), %xmm4
	pshufd	 $0xaa, %xmm4, %xmm6
	mulps	 %xmm6, %xmm5
	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm5, %xmm6
	subps	 %xmm6, %xmm7

	movaps	12 * SIZE(AA), %xmm4
	pshufd	 $0xff, %xmm4, %xmm6
	mulps	 %xmm6, %xmm7
#endif

#ifdef RN
	movaps	 0 * SIZE(B), %xmm6
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm7, %xmm0
	pshufd	 $0x55, %xmm6, %xmm7
	mulps	 %xmm0, %xmm7
	subps	 %xmm7, %xmm1

	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm7, %xmm1
#endif

#ifdef RT
	movaps	  0 * SIZE(B), %xmm6
	pshufd	 $0xff, %xmm6, %xmm7
	mulps	 %xmm7, %xmm1
	pshufd	 $0xaa, %xmm6, %xmm7
	mulps	 %xmm1, %xmm7
	subps	 %xmm7, %xmm0

	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm7, %xmm0
#endif

#if defined(LN) || defined(LT)
	movlps	%xmm1,   0 * SIZE(B)
	movlps	%xmm3,   2 * SIZE(B)
	movlps	%xmm5,   4 * SIZE(B)
	movlps	%xmm7,   6 * SIZE(B)

	pshufd	$0x00, %xmm1, %xmm0
	pshufd	$0x55, %xmm1, %xmm2
	movaps	%xmm0,   0 * SIZE(BB)
	movaps	%xmm2,   4 * SIZE(BB)

	pshufd	$0x00, %xmm3, %xmm0
	pshufd	$0x55, %xmm3, %xmm2
	movaps	%xmm0,   8 * SIZE(BB)
	movaps	%xmm2,  12 * SIZE(BB)

	pshufd	$0x00, %xmm5, %xmm0
	pshufd	$0x55, %xmm5, %xmm2
	movaps	%xmm0,  16 * SIZE(BB)
	movaps	%xmm2,  20 * SIZE(BB)

	pshufd	$0x00, %xmm7, %xmm0
	pshufd	$0x55, %xmm7, %xmm2
	movaps	%xmm0,  24 * SIZE(BB)
	movaps	%xmm2,  28 * SIZE(BB)
#else
	movaps	%xmm0,   0 * SIZE(AA)
	movaps	%xmm1,   4 * SIZE(AA)
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
	jg	.L51
	ALIGN_4

.L79:
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
	ALIGN_4

.L80:
	testl	$1, N
	je	.L999

#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

	leal	BUFFER, %ecx

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
	movl	K, %eax
	subl	KK, %eax
#endif
	sarl	$3, %eax
	jle	.L85
	ALIGN_4

.L82:
	movsd	 0 * SIZE(B), %xmm3
	movhps	 2 * SIZE(B), %xmm3
	movsd	 4 * SIZE(B), %xmm7
	movhps	 6 * SIZE(B), %xmm7

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

	pshufd	 $0x00, %xmm7, %xmm4
	pshufd	 $0x55, %xmm7, %xmm5
	pshufd	 $0xaa, %xmm7, %xmm6
	pshufd	 $0xff, %xmm7, %xmm7

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
	jne	.L82
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
	jle	.L90
	ALIGN_4

.L86:
	movss	 0 * SIZE(B), %xmm3

	pshufd	 $0x00, %xmm3, %xmm0

	movaps	%xmm0,  0 * SIZE(BB)

	addl	$1 * SIZE, B
	addl	$4 * SIZE, BB
	decl	%eax
	jne	.L86
	ALIGN_4

.L90:
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

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$BASE_SHIFT, %eax
	leal	(BB, %eax, 4), BB
#endif

	xorps	%xmm4, %xmm4
	xorps	%xmm5, %xmm5
	xorps	%xmm6, %xmm6
	xorps	%xmm7, %xmm7

	movss	 0 * SIZE(AA), %xmm0
	movss	 4 * SIZE(AA), %xmm1
	movss	 0 * SIZE(BB), %xmm2
	movss	16 * SIZE(BB), %xmm3

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
	mulss	%xmm0, %xmm2
#if defined(OPTERON) || defined(BARCELONA) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
	movss	 1 * SIZE(AA), %xmm0
	addss	%xmm2, %xmm4
	movss	32 * SIZE(BB), %xmm2
	mulss	 4 * SIZE(BB), %xmm0
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
	movss	48 * SIZE(BB), %xmm3
	mulss	20 * SIZE(BB), %xmm1
	addss	%xmm1, %xmm5
	movss	 6 * SIZE(AA), %xmm1
	mulss	24 * SIZE(BB), %xmm1
	addss	%xmm1, %xmm6
	movss	 7 * SIZE(AA), %xmm1
	mulss	28 * SIZE(BB), %xmm1
	addss	%xmm1, %xmm7
	movss	12 * SIZE(AA), %xmm1

	addl	$ 8 * SIZE, AA
	addl	$32 * SIZE, BB
	decl   %eax
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
	mulss	%xmm0, %xmm2
	movss	 1 * SIZE(AA), %xmm0
	addss	%xmm2, %xmm4
	movss	 4 * SIZE(BB), %xmm2

	addl	$ 1 * SIZE, AA
	addl	$ 4 * SIZE, BB
	decl	%eax
	jg	.L116
	ALIGN_4

.L118:
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

	pshufd	$0x00, %xmm1, %xmm0
	movaps	%xmm0,   0 * SIZE(BB)
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

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$BASE_SHIFT, %eax
	leal	(BB, %eax, 4), BB
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
	je	.L105
	ALIGN_4

.L102:
	mulps	%xmm0, %xmm2
#if defined(OPTERON) || defined(BARCELONA) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
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

	addl	$16 * SIZE, AA
	addl	$32 * SIZE, BB
	decl   %eax
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
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movsd	 2 * SIZE(AA), %xmm0
	movaps	 4 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L106
	ALIGN_4

.L108:
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

	sall	$ BASE_SHIFT, %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 1), B
	leal	(BB, %eax, 4), BB
#endif

#if defined(LN) || defined(LT)
	pshufd	$1, %xmm4, %xmm6

	movss	 0 * SIZE(B), %xmm1
	movss	 1 * SIZE(B), %xmm3

	subss	%xmm4,  %xmm1
	subss	%xmm6,  %xmm3
#else
#ifdef	movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	 0 * SIZE(AA), %xmm0

	subps	%xmm4, %xmm0
#endif

#ifdef LN
	movaps	  0 * SIZE(AA), %xmm4
	pshufd	 $0xff, %xmm4, %xmm6
	mulss	 %xmm6, %xmm3
	pshufd	 $0xaa, %xmm4, %xmm6
	mulss	 %xmm3, %xmm6
	subss	 %xmm6, %xmm1

	pshufd	 $0x00, %xmm4, %xmm6
	mulss	 %xmm6, %xmm1
#endif

#ifdef LT
	movaps	 0 * SIZE(AA), %xmm4
	pshufd	 $0x00, %xmm4, %xmm6
	mulss	 %xmm6, %xmm1
	pshufd	 $0x55, %xmm4, %xmm6
	mulss	 %xmm1, %xmm6
	subss	 %xmm6, %xmm3

	pshufd	 $0xff, %xmm4, %xmm6
	mulss	 %xmm6, %xmm3
#endif

#if defined(RN) || defined(RT)
	movss	 0 * SIZE(B), %xmm6
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm7, %xmm0
#endif

#if defined(LN) || defined(LT)
	movss	%xmm1,   0 * SIZE(B)
	movss	%xmm3,   1 * SIZE(B)

	pshufd	$0x00, %xmm1, %xmm0
	movaps	%xmm0,   0 * SIZE(BB)
	pshufd	$0x00, %xmm3, %xmm0
	movaps	%xmm0,   4 * SIZE(BB)
#else
	movlps	%xmm0,   0 * SIZE(AA)
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

	leal	BUFFER, BB

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$BASE_SHIFT, %eax
	leal	(BB, %eax, 4), BB
#endif

	xorps	%xmm4, %xmm4
	xorps	%xmm5, %xmm5
	xorps	%xmm6, %xmm6
	xorps	%xmm7, %xmm7

	movaps	 0 * SIZE(AA), %xmm0
	movaps	16 * SIZE(AA), %xmm1
	movaps	 0 * SIZE(BB), %xmm2
	movaps	16 * SIZE(BB), %xmm3

	PREFETCHW	-4 * SIZE(CO1)

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
	mulps	%xmm0, %xmm2
#if defined(OPTERON) || defined(BARCELONA) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
	prefetcht0 (PREFETCHSIZE  + 0) * SIZE(AA)
#endif
	movaps	 4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	movaps	32 * SIZE(BB), %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm0, %xmm5
	movaps	 8 * SIZE(AA), %xmm0
	mulps	 8 * SIZE(BB), %xmm0
	addps	%xmm0, %xmm6
	movaps	12 * SIZE(AA), %xmm0
	mulps	12 * SIZE(BB), %xmm0
	addps	%xmm0, %xmm7
	movaps	32 * SIZE(AA), %xmm0
#if defined(OPTERON) || defined(BARCELONA) || defined(BOBCAT) || defined(BARCELONA_OPTIMIZATION)
	prefetcht0 (PREFETCHSIZE  + 16) * SIZE(AA)
#endif
	mulps	%xmm1, %xmm3
	movaps	20 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm4
	movaps	48 * SIZE(BB), %xmm3
	mulps	20 * SIZE(BB), %xmm1
	addps	%xmm1, %xmm5
	movaps	24 * SIZE(AA), %xmm1
	mulps	24 * SIZE(BB), %xmm1
	addps	%xmm1, %xmm6
	movaps	28 * SIZE(AA), %xmm1
	mulps	28 * SIZE(BB), %xmm1
	addps	%xmm1, %xmm7
	movaps	48 * SIZE(AA), %xmm1

	addl	$32 * SIZE, AA
	addl	$32 * SIZE, BB
	decl   %eax
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
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(AA), %xmm0
	movaps	 4 * SIZE(BB), %xmm2

	addl	$4 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L96
	ALIGN_4

.L98:
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

	sall	$ BASE_SHIFT, %eax
	leal	(AA, %eax, 4), AA
	leal	(B,  %eax, 1), B
	leal	(BB, %eax, 4), BB
#endif

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

	movss	 0 * SIZE(B), %xmm1
	movss	 1 * SIZE(B), %xmm3
	movss	 2 * SIZE(B), %xmm5
	movss	 3 * SIZE(B), %xmm7

	subss	%xmm4,  %xmm1
	subss	%xmm6,  %xmm3
	subss	%xmm0,  %xmm5
	subss	%xmm2,  %xmm7
#else
	movaps	 0 * SIZE(AA), %xmm0

	subps	%xmm4, %xmm0
#endif

#ifdef LN
	movaps	 12 * SIZE(AA), %xmm4
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

	movaps	  8 * SIZE(AA), %xmm4
	pshufd	 $0xaa, %xmm4, %xmm6
	mulss	 %xmm6, %xmm5
	pshufd	 $0x55, %xmm4, %xmm6
	mulss	 %xmm5, %xmm6
	subss	 %xmm6, %xmm3
	pshufd	 $0x00, %xmm4, %xmm6
	mulss	 %xmm5, %xmm6
	subss	 %xmm6, %xmm1

	movaps	  4 * SIZE(AA), %xmm4
	pshufd	 $0x55, %xmm4, %xmm6
	mulss	 %xmm6, %xmm3
	pshufd	 $0x00, %xmm4, %xmm6
	mulss	 %xmm3, %xmm6
	subss	 %xmm6, %xmm1

	movaps	  0 * SIZE(AA), %xmm4
	pshufd	 $0x00, %xmm4, %xmm6
	mulss	 %xmm6, %xmm1
#endif

#ifdef LT
	movaps	 0 * SIZE(AA), %xmm4
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

	movaps	 4 * SIZE(AA), %xmm4
	pshufd	 $0x55, %xmm4, %xmm6
	mulss	 %xmm6, %xmm3
	pshufd	 $0xaa, %xmm4, %xmm6
	mulss	 %xmm3, %xmm6
	subss	 %xmm6, %xmm5
	pshufd	 $0xff, %xmm4, %xmm6
	mulss	 %xmm3, %xmm6
	subss	 %xmm6, %xmm7

	movaps	 8 * SIZE(AA), %xmm4
	pshufd	 $0xaa, %xmm4, %xmm6
	mulss	 %xmm6, %xmm5
	pshufd	 $0xff, %xmm4, %xmm6
	mulss	 %xmm5, %xmm6
	subss	 %xmm6, %xmm7

	movaps	12 * SIZE(AA), %xmm4
	pshufd	 $0xff, %xmm4, %xmm6
	mulss	 %xmm6, %xmm7
#endif

#if defined(RN) || defined(RT)
	movss	 0 * SIZE(B), %xmm6
	pshufd	 $0x00, %xmm6, %xmm7
	mulps	 %xmm7, %xmm0
#endif

#if defined(LN) || defined(LT)
	movss	%xmm1,   0 * SIZE(B)
	movss	%xmm3,   1 * SIZE(B)
	movss	%xmm5,   2 * SIZE(B)
	movss	%xmm7,   3 * SIZE(B)

	pshufd	$0x00, %xmm1, %xmm0
	movaps	%xmm0,   0 * SIZE(BB)
	pshufd	$0x00, %xmm3, %xmm0
	movaps	%xmm0,   4 * SIZE(BB)

	pshufd	$0x00, %xmm5, %xmm0
	movaps	%xmm0,   8 * SIZE(BB)
	pshufd	$0x00, %xmm7, %xmm0
	movaps	%xmm0,  12 * SIZE(BB)
#else
	movss	%xmm0,   0 * SIZE(AA)
	movss	%xmm1,   1 * SIZE(AA)
	movss	%xmm2,   2 * SIZE(AA)
	movss	%xmm3,   3 * SIZE(AA)
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

	decl	%ebx			# i --
	jg	.L91
	ALIGN_4

.L119:
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
