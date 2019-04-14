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
#define STACK_ALPHA_R	16 + STACK + ARGS(%esi)
#define STACK_ALPHA_I	20 + STACK + ARGS(%esi)
#define STACK_A	24 + STACK + ARGS(%esi)
#define STACK_B	28 + STACK + ARGS(%esi)
#define STACK_C	32 + STACK + ARGS(%esi)
#define STACK_LDC	36 + STACK + ARGS(%esi)
#define STACK_OFFT	40 + STACK + ARGS(%esi)

#define ALPHA_R	16(%esp)
#define ALPHA_I	32(%esp)
#define K	48(%esp)
#define N	52(%esp)
#define M	56(%esp)
#define A	60(%esp)
#define C	64(%esp)
#define J	68(%esp)
#define OLD_STACK 72(%esp)
#define TEMP    76(%esp)
#define OFFSET  80(%esp)
#define KK	84(%esp)
#define KKK	88(%esp)
#define BUFFER 128(%esp)

#define B	%edi
#define LDC	%ebp
#define C1	%esi

#define STACK_ALIGN	4096
#define STACK_OFFSET	1024

#define PREFETCH_R    (8 * 16 + 0)
#define PREFETCH_W    (PREFETCH_R * 2)

#define PREFETCHSIZE  (8 * 16 + 4)
#define PREFETCH     prefetcht0

#define AA	%edx
#define BB	%ecx

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define ADDSUB	addps
#else
#define ADDSUB	subps
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

	movd	STACK_M, %mm0
	movl	STACK_N, %eax
	movd	STACK_K, %mm1
	movd	STACK_A, %mm2
	movl	STACK_B, B
	movd	STACK_C, %mm3
	movl	STACK_LDC, LDC
#ifdef TRMMKERNEL
	movd	STACK_OFFT, %mm4
#endif

	movd	%mm1, K
	movd	%mm0, M
	movl	%eax, N
	movd	%mm2, A
	movd	%mm3, C
	movl	%esi, OLD_STACK
#ifdef TRMMKERNEL
	movd	%mm4, OFFSET
	movd	%mm4, KK
#ifndef LEFT
	negl	KK
#endif
#endif

	subl	$-32 * SIZE, A
	subl	$-32 * SIZE, B

	leal	(, LDC, SIZE * 2), LDC

	movss	STACK_ALPHA_R, %xmm0
	movss	STACK_ALPHA_I, %xmm1

	pcmpeqb	%xmm7, %xmm7
	pslld	$31, %xmm7	# Generate mask
	shufps	$0, %xmm0, %xmm0

	movaps	 %xmm0,  0 + ALPHA_R
	movss	 %xmm1,  4 + ALPHA_I
	movss	 %xmm1, 12 + ALPHA_I
	xorps	 %xmm7, %xmm1
	movss	 %xmm1,  0 + ALPHA_I
	movss	 %xmm1,  8 + ALPHA_I

	movl	%eax, J			# j = n
	testl	%eax, %eax
	jle	.L999

.L01:
 	leal	32 * SIZE + BUFFER, BB

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	K, %eax
	sarl	$2, %eax
	jle	.L03

.L02:
	prefetcht0	(PREFETCH_R + 0) * SIZE(B)

	movss	-32 * SIZE(B), %xmm0
	movss	-31 * SIZE(B), %xmm1
	movss	-30 * SIZE(B), %xmm2
	movss	-29 * SIZE(B), %xmm3
	movss	-28 * SIZE(B), %xmm4
	movss	-27 * SIZE(B), %xmm5
	movss	-26 * SIZE(B), %xmm6
	movss	-25 * SIZE(B), %xmm7

	prefetcht0	(PREFETCH_W +  0) * SIZE(BB)
	shufps	$0, %xmm0, %xmm0
	shufps	$0, %xmm1, %xmm1
	shufps	$0, %xmm2, %xmm2
	shufps	$0, %xmm3, %xmm3
	shufps	$0, %xmm4, %xmm4
	shufps	$0, %xmm5, %xmm5
	shufps	$0, %xmm6, %xmm6
	shufps	$0, %xmm7, %xmm7

	prefetcht0	(PREFETCH_W + 16) * SIZE(BB)
	movaps	%xmm0,  -32 * SIZE(BB)
	movaps	%xmm1,  -28 * SIZE(BB)
	movaps	%xmm2,  -24 * SIZE(BB)
	movaps	%xmm3,  -20 * SIZE(BB)
	movaps	%xmm4,  -16 * SIZE(BB)
	movaps	%xmm5,  -12 * SIZE(BB)
	movaps	%xmm6,   -8 * SIZE(BB)
	movaps	%xmm7,   -4 * SIZE(BB)

	addl	$  8 * SIZE, B
	subl	$-32 * SIZE, BB
	decl	%eax
	jne	.L02

.L03:
	movl	K, %eax
	andl	$3, %eax
	BRANCH
	jle	.L05

.L04:
	movss	-32 * SIZE(B), %xmm0
	movss	-31 * SIZE(B), %xmm1

	shufps	$0, %xmm0, %xmm0
	shufps	$0, %xmm1, %xmm1

	movaps	%xmm0,  -32 * SIZE(BB)
	movaps	%xmm1,  -28 * SIZE(BB)
	addl	$2 * SIZE, B
	addl	$8 * SIZE, BB
	decl	%eax
	jne	.L04
	ALIGN_4

.L05:
	movl	C, C1		# coffset = c
	movl	A, AA		# aoffset = a
	movl	M,  %ebx
	sarl	$2, %ebx	# i = (m >> 2)
	jle	.L20
	ALIGN_4

.L10:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	32 * SIZE + BUFFER, BB
#else
	leal	32 * SIZE + BUFFER, BB
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 4), BB /* because it's doubled */
#endif

	movaps	-32 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movaps	-32 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
	movapd	-16 * SIZE(AA), %xmm3
	pxor	%xmm6, %xmm6
	prefetcht0	7 * SIZE(C1)
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
	addl	$4, %eax
#else
	addl	$1, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L15
	ALIGN_4

.L12:
	movaps	%xmm1,  %xmm2
	mulps	%xmm0,  %xmm1
	addps	%xmm1,  %xmm4
	movaps	-28 * SIZE(BB), %xmm1
	mulps	%xmm1,  %xmm0
	ADDSUB	%xmm0,  %xmm5
	movaps	-28 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm2
	mulps	%xmm0,  %xmm1
	movaps	-24 * SIZE(AA), %xmm0
	addps	%xmm2,  %xmm6
	ADDSUB	%xmm1,  %xmm7

	movaps	-24 * SIZE(BB), %xmm1
	movaps	%xmm1,  %xmm2
	mulps	%xmm0,  %xmm1
	addps	%xmm1,  %xmm4
	movaps	-20 * SIZE(BB), %xmm1
	mulps	%xmm1,  %xmm0
	ADDSUB	%xmm0,  %xmm5
	movaps	-20 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm2
	mulps	%xmm0,  %xmm1
	movaps	  0 * SIZE(AA), %xmm0
	addps	%xmm2,  %xmm6
	ADDSUB	%xmm1,  %xmm7

	movaps	-16 * SIZE(BB), %xmm1
	movaps	%xmm1,  %xmm2
	mulps	%xmm3,  %xmm1
	addps	%xmm1,  %xmm4
	movaps	-12 * SIZE(BB), %xmm1
	mulps	%xmm1,  %xmm3
	ADDSUB	%xmm3,  %xmm5
	movaps	-12 * SIZE(AA), %xmm3
	mulps	%xmm3,  %xmm2
	mulps	%xmm3,  %xmm1
	movaps	 -8 * SIZE(AA), %xmm3
	addps	%xmm2,  %xmm6
	ADDSUB	%xmm1,  %xmm7

	movaps	 -8 * SIZE(BB), %xmm1
	movaps	%xmm1,  %xmm2
	mulps	%xmm3,  %xmm1
	addps	%xmm1,  %xmm4
	movaps	 -4 * SIZE(BB), %xmm1
	mulps	%xmm1,  %xmm3
	ADDSUB	%xmm3,  %xmm5
	movaps	 -4 * SIZE(AA), %xmm3
	mulps	%xmm3,  %xmm2
	mulps	%xmm3,  %xmm1
	movaps	 16 * SIZE(AA), %xmm3
	addps	%xmm2,  %xmm6
	ADDSUB	%xmm1,  %xmm7
	movaps	  0 * SIZE(BB), %xmm1

	movaps	%xmm1,  %xmm2
	mulps	%xmm0,  %xmm1
	addps	%xmm1,  %xmm4
	movaps	  4 * SIZE(BB), %xmm1
	mulps	%xmm1,  %xmm0
	ADDSUB	%xmm0,  %xmm5
	movaps	  4 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm2
	mulps	%xmm0,  %xmm1
	movaps	  8 * SIZE(AA), %xmm0
	addps	%xmm2,  %xmm6
	ADDSUB	%xmm1,  %xmm7

	movaps	  8 * SIZE(BB), %xmm1
	movaps	%xmm1,  %xmm2
	mulps	%xmm0,  %xmm1
	addps	%xmm1,  %xmm4
	movaps	 12 * SIZE(BB), %xmm1
	mulps	%xmm1,  %xmm0
	ADDSUB	%xmm0,  %xmm5
	movaps	 12 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm2
	mulps	%xmm0,  %xmm1
	movaps	 32 * SIZE(AA), %xmm0
	addps	%xmm2,  %xmm6
	ADDSUB	%xmm1,  %xmm7

	movaps	 16 * SIZE(BB), %xmm1
	movaps	%xmm1,  %xmm2
	mulps	%xmm3,  %xmm1
	addps	%xmm1,  %xmm4
	movaps	 20 * SIZE(BB), %xmm1
	mulps	%xmm1,  %xmm3
	ADDSUB	%xmm3,  %xmm5
	movaps	 20 * SIZE(AA), %xmm3
	mulps	%xmm3,  %xmm2
	mulps	%xmm3,  %xmm1
	addps	%xmm2,  %xmm6
	movaps	 24 * SIZE(AA), %xmm3
	ADDSUB	%xmm1,  %xmm7

	movaps	 24 * SIZE(BB), %xmm1
	movaps	%xmm1,  %xmm2
	mulps	%xmm3,  %xmm1
	addps	%xmm1,  %xmm4
	movaps	 28 * SIZE(BB), %xmm1
	mulps	%xmm1,  %xmm3
	ADDSUB	%xmm3,  %xmm5
	movaps	 28 * SIZE(AA), %xmm3
	mulps	%xmm3,  %xmm2
	mulps	%xmm3,  %xmm1
	subl   $-64 * SIZE, BB
	movaps	 48 * SIZE(AA), %xmm3
	subl   $-64 * SIZE, AA
	addps	%xmm2,  %xmm6
	ADDSUB	%xmm1,  %xmm7
	movaps	-32 * SIZE(BB), %xmm1


	decl   %eax
	jne    .L12
	ALIGN_4

.L15:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L18

.L16:
	movaps	%xmm1,  %xmm2
	mulps	%xmm0,  %xmm1
	addps	%xmm1,  %xmm4
	movaps	-28 * SIZE(BB), %xmm1
	mulps	%xmm1,  %xmm0
	ADDSUB	%xmm0,  %xmm5
	movaps	-28 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm2
	mulps	%xmm0,  %xmm1
	movaps	-24 * SIZE(AA), %xmm0
	addps	%xmm2,  %xmm6
	ADDSUB	%xmm1,  %xmm7
	movaps	-24 * SIZE(BB), %xmm1

	addl	$8 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L16

.L18:
	movaps	ALPHA_R, %xmm0
	movaps	ALPHA_I, %xmm1

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	shufps	$0xb1, %xmm5, %xmm5
	shufps	$0xb1, %xmm7, %xmm7

	addsubps	%xmm5, %xmm4
	addsubps	%xmm7, %xmm6

	movaps	%xmm4, %xmm5
	movaps	%xmm6, %xmm7

	shufps	$0xb1, %xmm4, %xmm4
	shufps	$0xb1, %xmm6, %xmm6
#else
	shufps	$0xb1, %xmm4, %xmm4
	shufps	$0xb1, %xmm6, %xmm6

	addsubps	%xmm4, %xmm5
	addsubps	%xmm6, %xmm7

	movaps	%xmm5, %xmm4
	movaps	%xmm7, %xmm6

	shufps	$0xb1, %xmm5, %xmm5
	shufps	$0xb1, %xmm7, %xmm7
#endif

	mulps	%xmm0, %xmm5
	mulps	%xmm1, %xmm4

	mulps	%xmm0, %xmm7
	mulps	%xmm1, %xmm6

	addps	%xmm5, %xmm4
	addps	%xmm7, %xmm6

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(C1), %xmm2
	movhps	2 * SIZE(C1), %xmm2
	movsd	4 * SIZE(C1), %xmm3
	movhps	6 * SIZE(C1), %xmm3

	addps	%xmm2, %xmm4
	addps	%xmm3, %xmm6
#endif

	movsd	%xmm4, 0 * SIZE(C1)
	movhps	%xmm4, 2 * SIZE(C1)
	movsd	%xmm6, 4 * SIZE(C1)
	movhps	%xmm6, 6 * SIZE(C1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax,    8), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$4, KK
#endif

	addl	$8 * SIZE, C1
	decl	%ebx
	jg	.L10
	ALIGN_2

.L20:
	movl	M,  %ebx
	testl	$2, %ebx
	jle	.L30

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	32 * SIZE + BUFFER, BB
#else

	leal	32 * SIZE + BUFFER, BB
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB /* because it's doubled */
#endif

	movaps	-32 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movaps	-32 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
	movaps	-16 * SIZE(AA), %xmm2
	pxor	%xmm6, %xmm6
	movaps	-16 * SIZE(BB), %xmm3
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
	je	.L25
	ALIGN_4

.L22:
	mulps	%xmm0,  %xmm1
	mulps	-28 * SIZE(BB), %xmm0
	addps	%xmm1, %xmm4
	movaps	-24 * SIZE(BB), %xmm1
	ADDSUB	%xmm0, %xmm5
	movaps	-28 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm1
	mulps	-20 * SIZE(BB), %xmm0
	addps	%xmm1, %xmm6
	movaps	  0 * SIZE(BB), %xmm1
	ADDSUB	%xmm0, %xmm7
	movaps	-24 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm3
	mulps	-12 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm4
	movaps	 -8 * SIZE(BB), %xmm3
	ADDSUB	%xmm0, %xmm5
	movaps	-20 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm3
	mulps	 -4 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm6
	movaps	 16 * SIZE(BB), %xmm3
	ADDSUB	%xmm0, %xmm7
	movaps	  0 * SIZE(AA), %xmm0
	mulps	%xmm2,  %xmm1
	mulps	  4 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm4
	movaps	  8 * SIZE(BB), %xmm1
	ADDSUB	%xmm2, %xmm5
	movaps	-12 * SIZE(AA), %xmm2
	mulps	%xmm2,  %xmm1
	mulps	 12 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm6
	movaps	 32 * SIZE(BB), %xmm1
	ADDSUB	%xmm2, %xmm7
	movaps	 -8 * SIZE(AA), %xmm2
	mulps	%xmm2,  %xmm3
	mulps	 20 * SIZE(BB), %xmm2
	addps	%xmm3, %xmm4
	movaps	 24 * SIZE(BB), %xmm3
	ADDSUB	%xmm2, %xmm5
	movaps	 -4 * SIZE(AA), %xmm2
	mulps	%xmm2,  %xmm3
	mulps	 28 * SIZE(BB), %xmm2
	addps	%xmm3, %xmm6
	movaps	 48 * SIZE(BB), %xmm3
	ADDSUB	%xmm2, %xmm7
	movaps	 16 * SIZE(AA), %xmm2

	subl   $-32 * SIZE, AA
	addl   $ 64 * SIZE, BB
	decl	%eax
	jne	.L22
	ALIGN_2

.L25:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L28

.L26:
	mulps	%xmm0,  %xmm1
	mulps	-28 * SIZE(BB), %xmm0
	addps	%xmm1, %xmm4
	movaps	-24 * SIZE(BB), %xmm1
	ADDSUB	%xmm0, %xmm5
	movaps	-28 * SIZE(AA), %xmm0

	addl	$4 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L26

.L28:
	addps	%xmm6, %xmm4
	addps	%xmm7, %xmm5

	movaps	ALPHA_R, %xmm0
	movaps	ALPHA_I, %xmm1

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	shufps	$0xb1, %xmm5, %xmm5
	addsubps	%xmm5, %xmm4
	movaps	%xmm4, %xmm5
	shufps	$0xb1, %xmm4, %xmm4
#else
	shufps	$0xb1, %xmm4, %xmm4
	addsubps	%xmm4, %xmm5
	movaps	%xmm5, %xmm4
	shufps	$0xb1, %xmm5, %xmm5
#endif

	mulps	%xmm0, %xmm5
	mulps	%xmm1, %xmm4

	addps	%xmm5, %xmm4

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
	leal	(,%eax,    8), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif
	addl	$4 * SIZE, C1
	ALIGN_2

.L30:
	testl	$1, %ebx
	jle	.L39

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	32 * SIZE + BUFFER, BB
#else

	leal	32 * SIZE + BUFFER, BB
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB /* because it's doubled */
#endif

	movsd	-32 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movsd	-32 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
	movsd	-24 * SIZE(AA), %xmm2
	pxor	%xmm6, %xmm6
	movsd	-16 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7


#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
	addl	$1, %eax
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L35
	ALIGN_4

.L32:
	mulps	%xmm0,  %xmm1
	mulps	-28 * SIZE(BB), %xmm0
	addps	%xmm1, %xmm4
 	movsd	-24 * SIZE(BB), %xmm1
	ADDSUB	%xmm0, %xmm5
	movsd	-30 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm1
	mulps	-20 * SIZE(BB), %xmm0
	addps	%xmm1, %xmm6
	movsd	  0 * SIZE(BB), %xmm1
	ADDSUB	%xmm0, %xmm7
	movsd	-28 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm3
	mulps	-12 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm4
	movsd	 -8 * SIZE(BB), %xmm3
	ADDSUB	%xmm0, %xmm5
	movsd	-26 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm3
	mulps	 -4 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm6
	movsd	 16 * SIZE(BB), %xmm3
	ADDSUB	%xmm0, %xmm7
	movsd	-16 * SIZE(AA), %xmm0
	mulps	%xmm2,  %xmm1
	mulps	  4 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm4
	movsd	  8 * SIZE(BB), %xmm1
	ADDSUB	%xmm2, %xmm5
	movsd	-22 * SIZE(AA), %xmm2
	mulps	%xmm2,  %xmm1
	mulps	 12 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm6
	movsd	 32 * SIZE(BB), %xmm1
	ADDSUB	%xmm2, %xmm7
	movsd	-20 * SIZE(AA), %xmm2
	mulps	%xmm2,  %xmm3
	mulps	 20 * SIZE(BB), %xmm2
	addps	%xmm3, %xmm4
	movsd	 24 * SIZE(BB), %xmm3
	ADDSUB	%xmm2, %xmm5
	movsd	-18 * SIZE(AA), %xmm2
	mulps	%xmm2,  %xmm3
	mulps	 28 * SIZE(BB), %xmm2
	addps	%xmm3, %xmm6
	movsd	 48 * SIZE(BB), %xmm3
	ADDSUB	%xmm2, %xmm7
	movsd	 -8 * SIZE(AA), %xmm2

	subl   $-16 * SIZE, AA
	addl   $ 64 * SIZE, BB

	decl	%eax
	jne	.L32
	ALIGN_2

.L35:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L38

.L36:
	mulps	%xmm0,  %xmm1
	mulps	-28 * SIZE(BB), %xmm0
	addps	%xmm1, %xmm4
	movsd	-24 * SIZE(BB), %xmm1
	ADDSUB	%xmm0, %xmm5
	movsd	-30 * SIZE(AA), %xmm0

	addl	$2 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L36

.L38:
	addps	%xmm6, %xmm4
	addps	%xmm7, %xmm5

	movaps	ALPHA_R, %xmm0
	movaps	ALPHA_I, %xmm1

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	shufps	$0xb1, %xmm5, %xmm5
	addsubps	%xmm5, %xmm4
	movaps	%xmm4, %xmm5
	shufps	$0xb1, %xmm4, %xmm4
#else
	shufps	$0xb1, %xmm4, %xmm4
	addsubps	%xmm4, %xmm5
	movaps	%xmm5, %xmm4
	shufps	$0xb1, %xmm5, %xmm5
#endif

	mulps	%xmm0, %xmm5
	mulps	%xmm1, %xmm4

	addps	%xmm5, %xmm4

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(C1), %xmm2
	addps	%xmm2, %xmm4
#endif

	movsd	%xmm4, 0 * SIZE(C1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax,    8), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif
	ALIGN_2

.L39:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$1, KK
#endif

	addl	LDC, C			# c += ldc
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
