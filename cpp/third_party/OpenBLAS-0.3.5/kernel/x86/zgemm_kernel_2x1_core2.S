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
#define STACK_ALPHA_R	16 + STACK + ARGS(%esi)
#define STACK_ALPHA_I	24 + STACK + ARGS(%esi)
#define STACK_A	32 + STACK + ARGS(%esi)
#define STACK_B	36 + STACK + ARGS(%esi)
#define STACK_C	40 + STACK + ARGS(%esi)
#define STACK_LDC	44 + STACK + ARGS(%esi)
#define STACK_OFFT	48 + STACK + ARGS(%esi)

#define ALPHA_R	16(%esp)
#define ALPHA_I	32(%esp)
#define K	48(%esp)
#define N	52(%esp)
#define M	56(%esp)
#define A	60(%esp)
#define C	64(%esp)
#define J	68(%esp)
#define BX	72(%esp)
#define OLD_STACK 76(%esp)
#define OFFSET  80(%esp)
#define KK	84(%esp)
#define KKK	88(%esp)
#define BUFFER 128(%esp)

#define STACK_ALIGN	4096
#define STACK_OFFSET	1024

#define PREFETCH_R    (8 * 16 + 0)
#define PREFETCH_W    (PREFETCH_R * 2)

#define PREFETCHSIZE  (8 * 16 + 4)
#define PREFETCH     prefetcht0

#define B	%edi
#define LDC	%ebp
#define AA	%edx
#define BB	%ecx
#define C1	%esi

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define ADD1	  addpd
#define ADD2	  addpd
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define ADD1	  addpd
#define ADD2	  subpd
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define ADD1	  subpd
#define ADD2	  addpd
#else
#define ADD1	  subpd
#define ADD2	  subpd
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

	movsd	STACK_ALPHA_R, %xmm0
	movsd	STACK_ALPHA_I, %xmm1

	movddup	%xmm0, %xmm0
	movddup	%xmm1, %xmm1

	movapd	 %xmm0, ALPHA_R
	movapd	 %xmm1, ALPHA_I

	movd	%mm1, K
	movl	%eax, N
	movd	%mm0, M
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

	subl	$-16 * SIZE, A
	subl	$-16 * SIZE, B
	sall	$ZBASE_SHIFT, LDC

	movl	%eax, J			# j = n
	testl	%eax, %eax
	jle	.L999
	ALIGN_2

.L01:
	leal	16 * SIZE + BUFFER, BB

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	K, %eax
	sarl	$2, %eax
	jle	.L03
	ALIGN_2

.L02:
	prefetcht0	(PREFETCH_R + 0) * SIZE(B)

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
	movl	K, %eax
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
	movl	B, BX

	movl	C, C1		# coffset = c
	movl	A, AA		# aoffset = a
	movl	M,  %ebx
	sarl	$1, %ebx	# i = (m >> 2)
	jle	.L20
	ALIGN_4

.L10:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	16 * SIZE + BUFFER, BB
#else

	leal	16 * SIZE + BUFFER, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 4), BB /* because it's doubled */

#endif

	movapd	-16 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movapd	-16 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
	movapd	 -8 * SIZE(AA), %xmm3
	pxor	%xmm6, %xmm6
	prefetcht0	3 * SIZE(C1)
	pxor	%xmm7, %xmm7
	movapd	%xmm1,  %xmm2

	movl	BX, %eax
	prefetcht0   (%eax)
	subl	$-8 * SIZE, %eax
	movl	%eax, BX

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
	je	.L15
	ALIGN_4

.L12:
	mulpd	%xmm0,  %xmm1
	ADD1	%xmm1,  %xmm4
	movapd	-14 * SIZE(BB), %xmm1
	mulpd	%xmm1,  %xmm0
	ADD2	%xmm0,  %xmm5
	movapd	-14 * SIZE(AA), %xmm0
	mulpd	%xmm0,  %xmm2
	ADD1	%xmm2,  %xmm6
	movapd	-12 * SIZE(BB), %xmm2
	mulpd	%xmm0,  %xmm1
	movapd	-12 * SIZE(AA), %xmm0
	ADD2	%xmm1,  %xmm7
	PADDING;
	movapd	%xmm2,  %xmm1

	mulpd	%xmm0,  %xmm2
	ADD1	%xmm2,  %xmm4
	movapd	-10 * SIZE(BB), %xmm2
	mulpd	%xmm2,  %xmm0
	ADD2	%xmm0,  %xmm5
	movapd	-10 * SIZE(AA), %xmm0
	mulpd	%xmm0,  %xmm1
	ADD1	%xmm1,  %xmm6
	movapd	 -8 * SIZE(BB), %xmm1
	mulpd	%xmm0,  %xmm2
	PADDING;
	movapd	  0 * SIZE(AA), %xmm0
	ADD2	%xmm2,  %xmm7
	PADDING;
	movapd	%xmm1,  %xmm2

	mulpd	%xmm3,  %xmm1
	ADD1	%xmm1,  %xmm4
	movapd	 -6 * SIZE(BB), %xmm1
	mulpd	%xmm1,  %xmm3
	ADD2	%xmm3,  %xmm5
	movapd	 -6 * SIZE(AA), %xmm3
	mulpd	%xmm3,  %xmm2
	ADD1	%xmm2,  %xmm6
	movapd	 -4 * SIZE(BB), %xmm2
	mulpd	%xmm3,  %xmm1
	movapd	 -4 * SIZE(AA), %xmm3
	ADD2	%xmm1,  %xmm7
	PADDING;
	movapd	%xmm2,  %xmm1

	mulpd	%xmm3,  %xmm2
	ADD1	%xmm2,  %xmm4
	movapd	 -2 * SIZE(BB), %xmm2
	mulpd	%xmm2,  %xmm3
	ADD2	%xmm3,  %xmm5
	movapd	 -2 * SIZE(AA), %xmm3
	mulpd	%xmm3,  %xmm1
	ADD1	%xmm1,  %xmm6
	PADDING;
	movapd	  0 * SIZE(BB), %xmm1
	mulpd	%xmm3,  %xmm2
	movapd	  8 * SIZE(AA), %xmm3
	ADD2	%xmm2,  %xmm7
	PADDING;
	movapd	%xmm1,  %xmm2

	mulpd	%xmm0,  %xmm1
	ADD1	%xmm1,  %xmm4
	movapd	  2 * SIZE(BB), %xmm1
	mulpd	%xmm1,  %xmm0
	ADD2	%xmm0,  %xmm5
	movapd	  2 * SIZE(AA), %xmm0
	mulpd	%xmm0,  %xmm2
	ADD1	%xmm2,  %xmm6
	movapd	  4 * SIZE(BB), %xmm2
	mulpd	%xmm0,  %xmm1
	movapd	  4 * SIZE(AA), %xmm0
	ADD2	%xmm1,  %xmm7
	PADDING;
	movapd	%xmm2,  %xmm1

	mulpd	%xmm0,  %xmm2
	ADD1	%xmm2,  %xmm4
	movapd	  6 * SIZE(BB), %xmm2
	mulpd	%xmm2,  %xmm0
	ADD2	%xmm0,  %xmm5
	movapd	  6 * SIZE(AA), %xmm0
	mulpd	%xmm0,  %xmm1
	ADD1	%xmm1,  %xmm6
	movapd	  8 * SIZE(BB), %xmm1
	mulpd	%xmm0,  %xmm2
	movapd	 16 * SIZE(AA), %xmm0
	ADD2	%xmm2,  %xmm7
	PADDING;
	movapd	%xmm1,  %xmm2

	mulpd	%xmm3,  %xmm1
	ADD1	%xmm1,  %xmm4
	movapd	 10 * SIZE(BB), %xmm1
	mulpd	%xmm1,  %xmm3
	ADD2	%xmm3,  %xmm5
	movapd	 10 * SIZE(AA), %xmm3
	mulpd	%xmm3,  %xmm2
	ADD1	%xmm2,  %xmm6
	movapd	 12 * SIZE(BB), %xmm2
	mulpd	%xmm3,  %xmm1
	movapd	 12 * SIZE(AA), %xmm3
	ADD2	%xmm1,  %xmm7
	PADDING;
	movapd	%xmm2,  %xmm1

	mulpd	%xmm3,  %xmm2
	ADD1	%xmm2,  %xmm4
	movapd	 14 * SIZE(BB), %xmm2
	mulpd	%xmm2,  %xmm3
	subl   $-32 * SIZE, BB
	ADD2	%xmm3,  %xmm5
	movapd	 14 * SIZE(AA), %xmm3
	mulpd	%xmm3,  %xmm1
	ADD1	%xmm1,  %xmm6
	movapd	-16 * SIZE(BB), %xmm1
	mulpd	%xmm3,  %xmm2
	movapd	 24 * SIZE(AA), %xmm3
	ADD2	%xmm2,  %xmm7
	PADDING;
	movapd	%xmm1,  %xmm2

	subl   $-32 * SIZE, AA
	decl   %eax
	BRANCH
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
	movapd	%xmm1, %xmm2

	addl	$4 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L16
	ALIGN_4

.L18:
	movapd	ALPHA_R, %xmm2
	movapd	ALPHA_I, %xmm3

	SHUFPD_1 %xmm5, %xmm5
	SHUFPD_1 %xmm7, %xmm7

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	addsubpd	%xmm5, %xmm4
	addsubpd	%xmm7, %xmm6

	movapd	%xmm4, %xmm5
	movapd	%xmm6, %xmm7
#else
	addsubpd	%xmm4, %xmm5
	addsubpd	%xmm6, %xmm7

	movapd		%xmm5, %xmm4
	movapd		%xmm7, %xmm6
#endif

#ifndef TRMMKERNEL
	movsd	0 * SIZE(C1), %xmm0
	movhpd	1 * SIZE(C1), %xmm0
	movsd	2 * SIZE(C1), %xmm1
	movhpd	3 * SIZE(C1), %xmm1
#endif

	SHUFPD_1 %xmm5, %xmm5
	SHUFPD_1 %xmm7, %xmm7

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
	movsd	%xmm6, 2 * SIZE(C1)
	movhpd	%xmm6, 3 * SIZE(C1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif

	addl	$4 * SIZE, C1		# coffset += 4
	decl	%ebx			# i --
	jg	.L10

.L20:
	movl	M,  %ebx
	testl	$1, %ebx
	je	.L29

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	16 * SIZE + BUFFER, %ecx
#else

	leal	16 * SIZE + BUFFER, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB /* because it's doubled */

#endif

	movapd	-16 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movapd	-16 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
	movapd	 -8 * SIZE(AA), %xmm2
	pxor	%xmm6, %xmm6
	movapd	 -8 * SIZE(BB), %xmm3
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
	jle	.L22

.L21:
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
	jg	.L21
	ALIGN_2

.L22:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax		# l = (k & 3)
	jle	.L24
	ALIGN_2

.L23:
	mulpd	%xmm0,  %xmm1
	mulpd	-14 * SIZE(BB), %xmm0
	ADD1	%xmm1, %xmm4
	movapd	-12 * SIZE(BB), %xmm1
	ADD2	%xmm0, %xmm5
	movapd	-14 * SIZE(AA), %xmm0

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax			# l--
	jg	.L23

.L24:
	addpd	%xmm6, %xmm4
	addpd	%xmm7, %xmm5

	movapd	ALPHA_R, %xmm2
	movapd	ALPHA_I, %xmm3

	SHUFPD_1 %xmm5, %xmm5

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	addsubpd	%xmm5, %xmm4
	movapd	%xmm4, %xmm5
#else
	addsubpd	%xmm4, %xmm5
	movapd		%xmm5, %xmm4
#endif

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	movsd	0 * SIZE(C1), %xmm0
	movhpd	1 * SIZE(C1), %xmm0
#endif

	SHUFPD_1 %xmm5, %xmm5

	mulpd	%xmm2, %xmm4

	mulpd	%xmm3, %xmm5

	addsubpd	%xmm5, %xmm4

#if! defined(TRMMKERNEL) && !defined(BETAZERO)
	addpd	%xmm0,  %xmm4
#endif

	movsd	%xmm4, 0 * SIZE(C1)
	movhpd	%xmm4, 1 * SIZE(C1)
	ALIGN_2

.L29:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$1, KK
#endif

	addl	LDC, C			# c += ldc
	decl	J			# j --
	jg	.L01

.L999:
	movl	OLD_STACK, %esp

	emms

	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret
	ALIGN_2

	EPILOGUE
