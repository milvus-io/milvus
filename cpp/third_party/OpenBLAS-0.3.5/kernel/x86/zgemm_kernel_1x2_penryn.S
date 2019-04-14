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
#define BX	 4 + STACK(%esp)
#define KK	 8 + STACK(%esp)
#define KKK	12 + STACK(%esp)

#ifdef NANO
#define PREFETCHSIZE  (8 * 3 + 4)
#define PREFETCHW     prefetcht0
#define PREFETCHB     prefetcht0
#endif

#if defined(NEHALEM) || defined(SANDYBRIDGE)
#define PREFETCHSIZE  (8 * 1 - 4)
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
#define PREFETCHSIZE  (8 * 13 + 4)
#endif

#define AA	%edx
#define BB	%ecx
#define LDC	%ebp
#define B	%edi
#define C1	%esi
#define I	%ebx

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define ADD1	  addpd
#define ADD2	  addpd
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define ADD1	  addpd
#define ADD2	  addpd
#elif  defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define ADD1	  addpd
#define ADD2	  addpd
#else
#define ADD1	  addpd
#define ADD2	  subpd
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

	movl	M,  %ebx
	testl	%ebx, %ebx
	jle	.L999

	subl	$-16 * SIZE, A
	subl	$-16 * SIZE, B

	sall	$ZBASE_SHIFT, LDC

	movl	N,  %eax
	sarl	$1, %eax
	movl	%eax, J
	jle	.L20
	ALIGN_2

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	B, BX

	movl	C, C1		# coffset = c
	movl	A, AA		# aoffset = a
	movl	M,  %ebx
	ALIGN_4

.L10:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BB
#else
	movl	B, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB
#endif

	movl	BX, %eax
	PREFETCHB  -16 * SIZE(%eax)
	subl	$-8 * SIZE, %eax
	movl	%eax, BX

	movaps	-16 * SIZE(AA), %xmm0
	pxor	%xmm2, %xmm2
	movaps	-16 * SIZE(BB), %xmm1
	pxor	%xmm3, %xmm3

	xorps	%xmm4, %xmm4
	PREFETCHW	1 * SIZE(C1)
	xorps	%xmm5, %xmm5
	PREFETCHW	3 * SIZE(C1, LDC)
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
	addl	$1, %eax
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

	ADD1	%xmm3, %xmm6
	PREFETCH (PREFETCHSIZE +  8) * SIZE(AA)
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
	subl   $-32 * SIZE, BB
	pshufd	$0x4e, %xmm3, %xmm2
	mulpd	%xmm0, %xmm3
	mulpd	%xmm0, %xmm2
	movaps	  0 * SIZE(AA), %xmm0

	subl   $-16 * SIZE, AA

	subl   $1, %eax
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
	ADD1	%xmm3, %xmm6
	pcmpeqb	%xmm0, %xmm0
	ADD2	%xmm2, %xmm7
	psllq	$63,   %xmm0

	movddup	ALPHA_R, %xmm2
	movddup	ALPHA_I, %xmm3

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
      defined(RR) || defined(RC) || defined(CR) || defined(CC)
	shufps	$0x40, %xmm0, %xmm0

	pxor	%xmm0, %xmm4
	pxor	%xmm0, %xmm6
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
	shufps	$0x04, %xmm0, %xmm0

	pxor	%xmm0, %xmm5
	pxor	%xmm0, %xmm7
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
	shufps	$0x40, %xmm0, %xmm0

	pxor	%xmm0, %xmm5
	pxor	%xmm0, %xmm7
#endif

#ifndef TRMMKERNEL
	movsd	0 * SIZE(C1), %xmm0
	movhpd	1 * SIZE(C1), %xmm0
	movsd	0 * SIZE(C1, LDC), %xmm1
	movhpd	1 * SIZE(C1, LDC), %xmm1
#endif

	haddpd	%xmm5, %xmm4
	haddpd	%xmm7, %xmm6

	pshufd	$0x4e, %xmm4, %xmm5
	pshufd	$0x4e, %xmm6, %xmm7

	mulpd	%xmm2, %xmm4
	mulpd	%xmm2, %xmm6

	mulpd	%xmm3, %xmm5
	mulpd	%xmm3, %xmm7

	addsubpd	%xmm5, %xmm4
	addsubpd	%xmm7, %xmm6

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	addpd	%xmm0, %xmm4
	addpd	%xmm1, %xmm6
#endif

	movsd	%xmm4, 0 * SIZE(C1)
	movhpd	%xmm4, 1 * SIZE(C1)
	movsd	%xmm6, 0 * SIZE(C1, LDC)
	movhpd	%xmm6, 1 * SIZE(C1, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif

	addl	$2 * SIZE, C1		# coffset += 4
	decl	%ebx			# i --
	jg	.L10

#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	movl	BB, B

	leal	(, LDC, 2), %eax
	addl	%eax, C			# c += ldc
	decl	J			# j --
	jg	.L01
	ALIGN_4

.L20:
	movl	N, %eax
	testl	$1, %eax
	jle	.L999

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	C, C1		# coffset = c
	movl	A, AA		# aoffset = a
	movl	M,  %ebx
	ALIGN_4

.L21:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BB
#else
	movl	B, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 2), BB
#endif

	movaps	-16 * SIZE(AA), %xmm0
	pxor	%xmm2, %xmm2
	movaps	-16 * SIZE(BB), %xmm1
	pxor	%xmm3, %xmm3

	pxor	%xmm4, %xmm4
	prefetcht0	1 * SIZE(C1)
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
	je	.L25
	ALIGN_4

.L22:
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
	jg	.L26
	ALIGN_4

.L28:
	addpd	%xmm6, %xmm4
	pcmpeqb	%xmm0, %xmm0
	addpd	%xmm7, %xmm5
	psllq	$63,   %xmm0

	movddup	ALPHA_R, %xmm2
	movddup	ALPHA_I, %xmm3

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
      defined(RR) || defined(RC) || defined(CR) || defined(CC)
	shufps	$0x40, %xmm0, %xmm0

	pxor	%xmm0, %xmm4
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
	shufps	$0x04, %xmm0, %xmm0

	pxor	%xmm0, %xmm5
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
	shufps	$0x40, %xmm0, %xmm0

	pxor	%xmm0, %xmm5
#endif

#ifndef TRMMKERNEL
	movsd	0 * SIZE(C1), %xmm0
	movhpd	1 * SIZE(C1), %xmm0
#endif

	haddpd	%xmm5, %xmm4

	pshufd	$0x4e, %xmm4, %xmm5
	mulpd	%xmm2, %xmm4
	mulpd	%xmm3, %xmm5
	addsubpd	%xmm5, %xmm4

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	addpd	%xmm0, %xmm4
#endif

	movsd	%xmm4, 0 * SIZE(C1)
	movhpd	%xmm4, 1 * SIZE(C1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 2), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif

	addl	$2 * SIZE, C1
	decl	%ebx			# i --
	jg	.L21
	ALIGN_4

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp

	addl	$ARGS, %esp
	ret

	EPILOGUE
