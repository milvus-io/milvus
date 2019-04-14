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
#define ALPHA_I	20 + STACK + ARGS(%esp)
#define A	24 + STACK + ARGS(%esp)
#define ARG_B	28 + STACK + ARGS(%esp)
#define C	32 + STACK + ARGS(%esp)
#define ARG_LDC	36 + STACK + ARGS(%esp)
#define OFFSET	40 + STACK + ARGS(%esp)

#define J	 0 + STACK(%esp)
#define BX	 4 + STACK(%esp)
#define KK	 8 + STACK(%esp)
#define KKK	12 + STACK(%esp)

#ifdef NANO
#define PREFETCHSIZE  (16 * 3 + 8)
#define PREFETCHW     prefetcht0
#define PREFETCHB     prefetcht0
#endif

#if defined(NEHALEM) || defined(SANDYBRIDGE)
#define PREFETCHSIZE  (16 * 1 + 8)
#define PREFETCHW     prefetcht0
#define PREFETCHB     prefetcht0
#endif

#ifndef PREFETCH
#define PREFETCH      prefetcht0
#endif

#ifndef PREFETCHW
#define PREFETCHW     prefetcht0
#endif

#ifndef PREFETCHB
#define PREFETCHB     prefetcht0
#endif

#ifndef PREFETCHSIZE
#define PREFETCHSIZE  (16 * 13 + 8)
#endif

#define AA	%edx
#define BB	%ecx
#define LDC	%ebp
#define B	%edi
#define C1	%esi
#define I	%ebx

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define ADD1	  addps
#define ADD2	  addps
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define ADD1	  addps
#define ADD2	  addps
#elif  defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define ADD1	  addps
#define ADD2	  addps
#else
#define ADD1	  addps
#define ADD2	  subps
#endif

	PROLOGUE

	subl	$ARGS, %esp	# Generate Stack Frame

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	ARG_B,   B
	movl	ARG_LDC, LDC

#ifdef TRMMKERNEL
	movl	OFFSET, %eax
#ifndef LEFT
	negl	%eax
#endif
	movl	%eax, KK
#endif

	subl	$-32 * SIZE, A
	subl	$-32 * SIZE, B

	sall	$ZBASE_SHIFT, LDC

	movl	N,  %eax
	sarl	$1, %eax
	movl	%eax, J
	jle	.L30
	ALIGN_4

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	B, BX

	movl	C, C1
	movl	A, AA

	movl	M,  %ebx
	sarl	$1, %ebx
	jle	.L20
	ALIGN_4

.L10:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BB
#else
	movl	B, BB
	movl	KK, %eax
	leal	(, %eax,   SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 4), BB
#endif

	movl	BX, %eax
	PREFETCHB  -32 * SIZE(%eax)
	subl	$-16 * SIZE, %eax
	movl	%eax, BX

	movaps	-32 * SIZE(AA), %xmm0
	pxor	%xmm2, %xmm2
	movaps	-32 * SIZE(BB), %xmm1
	pxor	%xmm3, %xmm3

	xorps	%xmm4, %xmm4
	PREFETCHW	3 * SIZE(C1)
	xorps	%xmm5, %xmm5
	PREFETCHW	7 * SIZE(C1, LDC)
	xorps	%xmm6, %xmm6
	xorps	%xmm7, %xmm7

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$2, %eax
#else
	addl	$2, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L15
	ALIGN_4

.L12:
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

	ADD2	%xmm2, %xmm7
	PREFETCH (PREFETCHSIZE + 16) * SIZE(AA)
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
	jne    .L12
	ALIGN_4

.L15:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax
	BRANCH
	je .L18
	ALIGN_4

.L16:
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
	jg	.L16
	ALIGN_4

.L18:
	ADD2	%xmm2, %xmm7
	pcmpeqb	%xmm0, %xmm0
	ADD1	%xmm3, %xmm6
	psllq	$63,   %xmm0

	movsd	ALPHA_R, %xmm3

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
      defined(RR) || defined(RC) || defined(CR) || defined(CC)
	pxor	%xmm0, %xmm4
	pxor	%xmm0, %xmm6
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
	pshufd	$0xb1, %xmm0, %xmm0

	pxor	%xmm0, %xmm5
	pxor	%xmm0, %xmm7
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
	pxor	%xmm0, %xmm5
	pxor	%xmm0, %xmm7
#endif

	haddps	%xmm5, %xmm4
	haddps	%xmm7, %xmm6

	shufps	$0xd8, %xmm4, %xmm4
	shufps	$0xd8, %xmm6, %xmm6

	movaps	%xmm4, %xmm5
	shufps	$0xe4, %xmm6, %xmm4
	shufps	$0xe4, %xmm5, %xmm6

	pshufd	$0x00, %xmm3, %xmm2
	pshufd	$0x55, %xmm3, %xmm3

	pshufd	$0xb1, %xmm4, %xmm5
	pshufd	$0xb1, %xmm6, %xmm7

	mulps	%xmm2, %xmm4
	mulps	%xmm3, %xmm5

	mulps	%xmm2, %xmm6
	mulps	%xmm3, %xmm7

	addsubps	%xmm5, %xmm4
	addsubps	%xmm7, %xmm6

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(C1), %xmm2
	movhps	2 * SIZE(C1), %xmm2
	movsd	0 * SIZE(C1, LDC), %xmm3
	movhps	2 * SIZE(C1, LDC), %xmm3

	addps	%xmm2, %xmm4
	addps	%xmm3, %xmm6
#endif

	movsd	%xmm4, 0 * SIZE(C1)
	movhps	%xmm4, 2 * SIZE(C1)
	movsd	%xmm6, 0 * SIZE(C1, LDC)
	movhps	%xmm6, 2 * SIZE(C1, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax,    SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif

	addl	$4 * SIZE, C1
	decl	%ebx
	jg	.L10
	ALIGN_4

.L20:
	movl	M,  %ebx
	testl	$1, %ebx
	jle	.L29

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BB
#else
	movl	B, BB
	movl	KK, %eax
	leal	(, %eax,   SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB
#endif

	movsd	-32 * SIZE(AA), %xmm0
	pxor	%xmm2, %xmm2
	movaps	-32 * SIZE(BB), %xmm1
	pxor	%xmm3, %xmm3

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$1, %eax
#else
	addl	$2, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L25
	ALIGN_4

.L22:
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

	decl   %eax
	jne    .L22
	ALIGN_4

.L25:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax
	BRANCH
	je .L28
	ALIGN_4

.L26:
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
	jg	.L26
	ALIGN_4

.L28:
	addps	%xmm2, %xmm6
	addps	%xmm3, %xmm7

	movsd	ALPHA_R, %xmm3

	pshufd	 $0xb1, %xmm5, %xmm5
	pcmpeqb	%xmm0, %xmm0
	pshufd	 $0xb1, %xmm7, %xmm7
	psllq	$63,   %xmm0

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	pxor	%xmm0, %xmm5
	pxor	%xmm0, %xmm7

	subps	%xmm5, %xmm4
	subps	%xmm7, %xmm6
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
	pxor	%xmm0, %xmm5
	pxor	%xmm0, %xmm7

	addps	%xmm5, %xmm4
	addps	%xmm7, %xmm6
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
	pxor	%xmm0, %xmm4
	pxor	%xmm0, %xmm6

	addps	%xmm5, %xmm4
	addps	%xmm7, %xmm6
#else
	pxor	%xmm0, %xmm4
	pxor	%xmm0, %xmm6

	subps	%xmm5, %xmm4
	subps	%xmm7, %xmm6
#endif

	pshufd	$0x00, %xmm3, %xmm2
	pshufd	$0x55, %xmm3, %xmm3

	pshufd	$0xb1, %xmm4, %xmm5
	pshufd	$0xb1, %xmm6, %xmm7

	mulps	%xmm2, %xmm4
	mulps	%xmm3, %xmm5

	mulps	%xmm2, %xmm6
	mulps	%xmm3, %xmm7

	pxor	%xmm0, %xmm5
	pxor	%xmm0, %xmm7

	subps	%xmm5, %xmm4
	subps	%xmm7, %xmm6

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(C1), %xmm2
	movsd	0 * SIZE(C1, LDC), %xmm3

	addps	%xmm2, %xmm4
	addps	%xmm3, %xmm6
#endif

	movsd	%xmm4, 0 * SIZE(C1)
	movsd	%xmm6, 0 * SIZE(C1, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax,    SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif

	addl	$2 * SIZE, C1
	ALIGN_2

.L29:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif
	movl	BB, B

	leal	(, LDC, 2), %eax
	addl	%eax, C

	decl	J
	jg	.L01
	ALIGN_4

.L30:
	movl	N,  %eax
	testl	$1, %eax
	jle	.L999

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	C, C1
	movl	A, AA

	movl	M,  %ebx
	sarl	$1, %ebx
	jle	.L40
	ALIGN_4

.L31:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BB
#else
	movl	B, BB
	movl	KK, %eax
	leal	(, %eax,   SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 2), BB
#endif

	movaps	-32 * SIZE(AA), %xmm0
	pxor	%xmm2, %xmm2
	movaps	-32 * SIZE(BB), %xmm1
	pxor	%xmm3, %xmm3

	pxor	%xmm4, %xmm4
	prefetcht0	3 * SIZE(C1)
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$2, %eax
#else
	addl	$1, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L35
	ALIGN_4

.L32:
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
	jne    .L32
	ALIGN_4

.L35:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movsd	-32 * SIZE(BB), %xmm1
	andl	$7, %eax
	BRANCH
	je .L38
	ALIGN_4

.L36:
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
	jg	.L36
	ALIGN_4

.L38:
	addps	%xmm2, %xmm4
	addps	%xmm3, %xmm5

	movsd	ALPHA_R, %xmm3

	pshufd	 $0xb1, %xmm5, %xmm5
	pcmpeqb	%xmm0, %xmm0
	psllq	$63,   %xmm0

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	pxor	%xmm0, %xmm5
	subps	%xmm5, %xmm4
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
	pxor	%xmm0, %xmm5
	addps	%xmm5, %xmm4
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
	pxor	%xmm0, %xmm4
	addps	%xmm5, %xmm4
#else
	pxor	%xmm0, %xmm4
	subps	%xmm5, %xmm4
#endif

	pshufd	$0x00, %xmm3, %xmm2
	pshufd	$0x55, %xmm3, %xmm3

	pshufd	$0xb1, %xmm4, %xmm5

	mulps	%xmm2, %xmm4
	mulps	%xmm3, %xmm5

	pxor	%xmm0, %xmm5
	subps	%xmm5, %xmm4

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(C1), %xmm2
	movhps	2 * SIZE(C1), %xmm2

	addps	%xmm2, %xmm4
#endif

	movsd	%xmm4, 0 * SIZE(C1)
	movhps	%xmm4, 2 * SIZE(C1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax,    SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 2), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif

	addl	$4 * SIZE, C1
	decl	%ebx
	jg	.L31
	ALIGN_4

.L40:
	movl	M,  %ebx
	testl	$1, %ebx
	jle	.L999

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BB
#else
	movl	B, BB
	movl	KK, %eax
	leal	(, %eax,   SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 2), BB
#endif

	movsd	-32 * SIZE(AA), %xmm0
	pxor	%xmm2, %xmm2
	movsd	-32 * SIZE(BB), %xmm1
	pxor	%xmm3, %xmm3

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$1, %eax
#else
	addl	$1, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L45
	ALIGN_4

.L42:
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

	decl   %eax
	jne    .L42
	ALIGN_4

.L45:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax
	BRANCH
	je .L48
	ALIGN_4

.L46:
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
	jg	.L46
	ALIGN_4

.L48:
	addps	%xmm2, %xmm4
	addps	%xmm3, %xmm5

	movsd	ALPHA_R, %xmm3

	pshufd	 $0xb1, %xmm5, %xmm5
	pcmpeqb	%xmm0, %xmm0
	psllq	$63,   %xmm0

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	pxor	%xmm0, %xmm5
	subps	%xmm5, %xmm4
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
	pxor	%xmm0, %xmm5
	addps	%xmm5, %xmm4
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
	pxor	%xmm0, %xmm4
	addps	%xmm5, %xmm4
#else
	pxor	%xmm0, %xmm4
	subps	%xmm5, %xmm4
#endif

	pshufd	$0x00, %xmm3, %xmm2
	pshufd	$0x55, %xmm3, %xmm3

	pshufd	$0xb1, %xmm4, %xmm5

	mulps	%xmm2, %xmm4
	mulps	%xmm3, %xmm5

	pxor	%xmm0, %xmm5
	subps	%xmm5, %xmm4

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(C1), %xmm2
	addps	%xmm2, %xmm4
#endif

	movsd	%xmm4, 0 * SIZE(C1)
	ALIGN_4

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp

	addl	$ARGS, %esp
	ret

	EPILOGUE
