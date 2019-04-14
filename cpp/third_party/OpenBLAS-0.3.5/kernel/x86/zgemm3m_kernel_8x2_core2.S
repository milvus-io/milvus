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
#define STACK_ALPHA_I	20 + STACK + ARGS(%esi)
#define STACK_A	24 + STACK + ARGS(%esi)
#define STACK_B	28 + STACK + ARGS(%esi)
#define STACK_C	32 + STACK + ARGS(%esi)
#define STACK_LDC	36 + STACK + ARGS(%esi)

#define ALPHA	 0(%esp)
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
#define BUFFER 512(%esp)

#define PREFETCH_R    (8 * 16 + 0)
#define PREFETCH_W    (PREFETCH_R * 2)

#define PREFETCHSIZE  (8 * 16 + 4)
#define PREFETCH     prefetcht0

#define AA	%edx
#define BB	%ecx
#define LDC	%ebp
#define B	%edi
#define C1	%esi
#define I	%ebx

#ifdef OPTERON
#define MOVSD	movlps
#else
#define MOVSD	movsd
#endif

	PROLOGUE

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	%esp, %esi	# save old stack

	subl	$512 + LOCAL_BUFFER_SIZE, %esp
	andl	$-4096, %esp	# align stack

	STACK_TOUCHING

	movl	STACK_M, %ebx
	movl	STACK_N, %eax
	movl	STACK_K, %ecx
	movl	STACK_A, %edx
	movss	STACK_ALPHA_R,  %xmm0
	movss	STACK_ALPHA_I,  %xmm1

	movl	%ebx, M
	movl	%eax, N
	movl	%ecx, K
	movl	%edx, A
	movl	%esi, OLD_STACK

	movl	STACK_B, B
	movl	STACK_C, %ebx

	unpcklps %xmm1, %xmm0

	movlps	%xmm0, 0 + ALPHA
	movlps	%xmm0, 8 + ALPHA

	movl	%ebx, C
	movl	STACK_LDC, LDC

	subl	$-32 * SIZE, A
	subl	$-32 * SIZE, B

	sall	$ZBASE_SHIFT, LDC

	sarl	$1, %eax
	movl	%eax, J
	jle	.L50
	ALIGN_4

.L01:
	leal	32 * SIZE + BUFFER, BB

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	K, %eax
	sarl	$2, %eax
	jle	.L05
	ALIGN_4

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
	ALIGN_4

.L05:
	movl	K, %eax
	andl	$3, %eax
	BRANCH
	jle	.L10
	ALIGN_4

.L06:
	movss	-32 * SIZE(B), %xmm0
	movss	-31 * SIZE(B), %xmm1

	shufps	$0, %xmm0, %xmm0
	shufps	$0, %xmm1, %xmm1

	movaps	%xmm0,  -32 * SIZE(BB)
	movaps	%xmm1,  -28 * SIZE(BB)
	addl	$2 * SIZE, B
	addl	$8 * SIZE, BB
	decl	%eax
	jne	.L06
	ALIGN_4

.L10:
	movl	C, C1
	movl	A, AA
	movl	M,  I
	sarl	$3, I
	jle	.L20
	ALIGN_4

.L11:
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
	prefetcht0	7 * SIZE(C1, LDC)

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$8, %eax
#else
	addl	$2, %eax
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
	addps	%xmm0,  %xmm5
	movaps	-28 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm2
	mulps	%xmm0,  %xmm1
	movaps	-24 * SIZE(AA), %xmm0
	addps	%xmm2,  %xmm6
	addps	%xmm1,  %xmm7

	movaps	-24 * SIZE(BB), %xmm1
	movaps	%xmm1,  %xmm2
	mulps	%xmm0,  %xmm1
	addps	%xmm1,  %xmm4
	movaps	-20 * SIZE(BB), %xmm1
	mulps	%xmm1,  %xmm0
	addps	%xmm0,  %xmm5
	movaps	-20 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm2
	mulps	%xmm0,  %xmm1
	movaps	  0 * SIZE(AA), %xmm0
	addps	%xmm2,  %xmm6
	addps	%xmm1,  %xmm7

	movaps	-16 * SIZE(BB), %xmm1
	movaps	%xmm1,  %xmm2
	mulps	%xmm3,  %xmm1
	addps	%xmm1,  %xmm4
	movaps	-12 * SIZE(BB), %xmm1
	mulps	%xmm1,  %xmm3
	addps	%xmm3,  %xmm5
	movaps	-12 * SIZE(AA), %xmm3
	mulps	%xmm3,  %xmm2
	mulps	%xmm3,  %xmm1
	movaps	 -8 * SIZE(AA), %xmm3
	addps	%xmm2,  %xmm6
	addps	%xmm1,  %xmm7

	movaps	 -8 * SIZE(BB), %xmm1
	movaps	%xmm1,  %xmm2
	mulps	%xmm3,  %xmm1
	addps	%xmm1,  %xmm4
	movaps	 -4 * SIZE(BB), %xmm1
	mulps	%xmm1,  %xmm3
	addps	%xmm3,  %xmm5
	movaps	 -4 * SIZE(AA), %xmm3
	mulps	%xmm3,  %xmm2
	mulps	%xmm3,  %xmm1
	movaps	 16 * SIZE(AA), %xmm3
	addps	%xmm2,  %xmm6
	addps	%xmm1,  %xmm7
	movaps	  0 * SIZE(BB), %xmm1

	movaps	%xmm1,  %xmm2
	mulps	%xmm0,  %xmm1
	addps	%xmm1,  %xmm4
	movaps	  4 * SIZE(BB), %xmm1
	mulps	%xmm1,  %xmm0
	addps	%xmm0,  %xmm5
	movaps	  4 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm2
	mulps	%xmm0,  %xmm1
	movaps	  8 * SIZE(AA), %xmm0
	addps	%xmm2,  %xmm6
	addps	%xmm1,  %xmm7

	movaps	  8 * SIZE(BB), %xmm1
	movaps	%xmm1,  %xmm2
	mulps	%xmm0,  %xmm1
	addps	%xmm1,  %xmm4
	movaps	 12 * SIZE(BB), %xmm1
	mulps	%xmm1,  %xmm0
	addps	%xmm0,  %xmm5
	movaps	 12 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm2
	mulps	%xmm0,  %xmm1
	movaps	 32 * SIZE(AA), %xmm0
	addps	%xmm2,  %xmm6
	addps	%xmm1,  %xmm7

	movaps	 16 * SIZE(BB), %xmm1
	movaps	%xmm1,  %xmm2
	mulps	%xmm3,  %xmm1
	addps	%xmm1,  %xmm4
	movaps	 20 * SIZE(BB), %xmm1
	mulps	%xmm1,  %xmm3
	addps	%xmm3,  %xmm5
	movaps	 20 * SIZE(AA), %xmm3
	mulps	%xmm3,  %xmm2
	mulps	%xmm3,  %xmm1
	addps	%xmm2,  %xmm6
	movaps	 24 * SIZE(AA), %xmm3
	addps	%xmm1,  %xmm7

	movaps	 24 * SIZE(BB), %xmm1
	movaps	%xmm1,  %xmm2
	mulps	%xmm3,  %xmm1
	addps	%xmm1,  %xmm4
	movaps	 28 * SIZE(BB), %xmm1
	mulps	%xmm1,  %xmm3
	addps	%xmm3,  %xmm5
	movaps	 28 * SIZE(AA), %xmm3
	mulps	%xmm3,  %xmm2
	mulps	%xmm3,  %xmm1
	subl   $-64 * SIZE, BB
	movaps	 48 * SIZE(AA), %xmm3
	subl   $-64 * SIZE, AA
	addps	%xmm2,  %xmm6
	addps	%xmm1,  %xmm7
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
	andl	$7, %eax
	BRANCH
	je .L18
	ALIGN_4

.L16:
	movaps	%xmm1,  %xmm2
	mulps	%xmm0,  %xmm1
	addps	%xmm1,  %xmm4
	movaps	-28 * SIZE(BB), %xmm1
	mulps	%xmm1,  %xmm0
	addps	%xmm0,  %xmm5
	movaps	-28 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm2
	mulps	%xmm0,  %xmm1
	movaps	-24 * SIZE(AA), %xmm0
	addps	%xmm2,  %xmm6
	addps	%xmm1,  %xmm7
	movaps	-24 * SIZE(BB), %xmm1

	addl	$8 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L16
	ALIGN_4

.L18:
	movaps	ALPHA,  %xmm3

 	MOVSD	 0 * SIZE(%esi), %xmm0
	movhps	 2 * SIZE(%esi), %xmm0
	MOVSD	 4 * SIZE(%esi), %xmm1
	movhps	 6 * SIZE(%esi), %xmm1

	pshufd	$0x50, %xmm4,  %xmm2
	pshufd	$0xfa, %xmm4,  %xmm4

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm4

	addps	%xmm2, %xmm0
	addps	%xmm4, %xmm1

	movlps	%xmm0,   0 * SIZE(%esi)
	movhps	%xmm0,   2 * SIZE(%esi)
	movlps	%xmm1,   4 * SIZE(%esi)
	movhps	%xmm1,   6 * SIZE(%esi)

	MOVSD	 8 * SIZE(%esi), %xmm0
	movhps	10 * SIZE(%esi), %xmm0
	MOVSD	12 * SIZE(%esi), %xmm1
	movhps	14 * SIZE(%esi), %xmm1

	pshufd	$0x50, %xmm6,  %xmm2
	pshufd	$0xfa, %xmm6,  %xmm6

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm6

	addps	%xmm2, %xmm0
	addps	%xmm6, %xmm1

	movlps	%xmm0,   8 * SIZE(%esi)
	movhps	%xmm0,  10 * SIZE(%esi)
	movlps	%xmm1,  12 * SIZE(%esi)
	movhps	%xmm1,  14 * SIZE(%esi)

	MOVSD	 0 * SIZE(%esi, LDC), %xmm0
	movhps	 2 * SIZE(%esi, LDC), %xmm0
	MOVSD	 4 * SIZE(%esi, LDC), %xmm1
	movhps	 6 * SIZE(%esi, LDC), %xmm1

	pshufd	$0x50, %xmm5,  %xmm2
	pshufd	$0xfa, %xmm5,  %xmm5

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm5

	addps	%xmm2, %xmm0
	addps	%xmm5, %xmm1

	movlps	%xmm0,   0 * SIZE(%esi, LDC)
	movhps	%xmm0,   2 * SIZE(%esi, LDC)
	movlps	%xmm1,   4 * SIZE(%esi, LDC)
	movhps	%xmm1,   6 * SIZE(%esi, LDC)

	MOVSD	 8 * SIZE(%esi, LDC), %xmm0
	movhps	10 * SIZE(%esi, LDC), %xmm0
	MOVSD	12 * SIZE(%esi, LDC), %xmm1
	movhps	14 * SIZE(%esi, LDC), %xmm1

	pshufd	$0x50, %xmm7,  %xmm2
	pshufd	$0xfa, %xmm7,  %xmm7

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm7

	addps	%xmm2, %xmm0
	addps	%xmm7, %xmm1

	movlps	%xmm0,   8 * SIZE(%esi, LDC)
	movhps	%xmm0,  10 * SIZE(%esi, LDC)
	movlps	%xmm1,  12 * SIZE(%esi, LDC)
	movhps	%xmm1,  14 * SIZE(%esi, LDC)

	addl	$16 * SIZE, C1
	decl	I
	jg	.L11
	ALIGN_4

.L20:
	movl	M,  I
	testl	$4, I
	jle	.L30

.L21:
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
	addl	$4, %eax
#else
	addl	$2, %eax
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
	addps	%xmm0, %xmm5
	movaps	-28 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm1
	mulps	-20 * SIZE(BB), %xmm0
	addps	%xmm1, %xmm6
	movaps	  0 * SIZE(BB), %xmm1
	addps	%xmm0, %xmm7
	movaps	-24 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm3
	mulps	-12 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm4
	movaps	 -8 * SIZE(BB), %xmm3
	addps	%xmm0, %xmm5
	movaps	-20 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm3
	mulps	 -4 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm6
	movaps	 16 * SIZE(BB), %xmm3
	addps	%xmm0, %xmm7
	movaps	  0 * SIZE(AA), %xmm0
	mulps	%xmm2,  %xmm1
	mulps	  4 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm4
	movaps	  8 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm5
	movaps	-12 * SIZE(AA), %xmm2
	mulps	%xmm2,  %xmm1
	mulps	 12 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm6
	movaps	 32 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm7
	movaps	 -8 * SIZE(AA), %xmm2
	mulps	%xmm2,  %xmm3
	mulps	 20 * SIZE(BB), %xmm2
	addps	%xmm3, %xmm4
	movaps	 24 * SIZE(BB), %xmm3
	addps	%xmm2, %xmm5
	movaps	 -4 * SIZE(AA), %xmm2
	mulps	%xmm2,  %xmm3
	mulps	 28 * SIZE(BB), %xmm2
	addps	%xmm3, %xmm6
	movaps	 48 * SIZE(BB), %xmm3
	addps	%xmm2, %xmm7
	movaps	 16 * SIZE(AA), %xmm2

	subl   $-32 * SIZE, AA
	addl   $ 64 * SIZE, BB
	decl   %eax
	jne    .L22
	ALIGN_4

.L25:
	movaps	ALPHA,  %xmm3

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
	mulps	%xmm0,  %xmm1
	mulps	-28 * SIZE(BB), %xmm0
	addps	%xmm1, %xmm4
	movaps	-24 * SIZE(BB), %xmm1
	addps	%xmm0, %xmm5
	movaps	-28 * SIZE(AA), %xmm0

	addl	$4 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L26
	ALIGN_4

.L28:
	addps	%xmm6, %xmm4
	addps	%xmm7, %xmm5

	movsd	 0 * SIZE(%esi), %xmm0
	movhps	 2 * SIZE(%esi), %xmm0
	movsd	 4 * SIZE(%esi), %xmm1
	movhps	 6 * SIZE(%esi), %xmm1

	pshufd	$0x50, %xmm4,  %xmm2
	pshufd	$0xfa, %xmm4,  %xmm4

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm4

	addps	%xmm2, %xmm0
	addps	%xmm4, %xmm1

	movlps	%xmm0,   0 * SIZE(%esi)
	movhps	%xmm0,   2 * SIZE(%esi)
	movlps	%xmm1,   4 * SIZE(%esi)
	movhps	%xmm1,   6 * SIZE(%esi)

	movsd	 0 * SIZE(%esi, LDC), %xmm0
	movhps	 2 * SIZE(%esi, LDC), %xmm0
	movsd	 4 * SIZE(%esi, LDC), %xmm1
	movhps	 6 * SIZE(%esi, LDC), %xmm1

	pshufd	$0x50, %xmm5,  %xmm2
	pshufd	$0xfa, %xmm5,  %xmm5

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm5

	addps	%xmm2, %xmm0
	addps	%xmm5, %xmm1

	movlps	%xmm0,   0 * SIZE(%esi, LDC)
	movhps	%xmm0,   2 * SIZE(%esi, LDC)
	movlps	%xmm1,   4 * SIZE(%esi, LDC)
	movhps	%xmm1,   6 * SIZE(%esi, LDC)

	addl	$8 * SIZE, C1
	ALIGN_4

.L30:
	movl	M,  I
	testl	$2, I
	jle	.L40

.L31:
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
#ifdef LEFT
	addl	$2, %eax
#else
	addl	$2, %eax
#endif
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
	addps	%xmm0, %xmm5
	movsd	-30 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm1
	mulps	-20 * SIZE(BB), %xmm0
	addps	%xmm1, %xmm6
	movsd	  0 * SIZE(BB), %xmm1
	addps	%xmm0, %xmm7
	movsd	-28 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm3
	mulps	-12 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm4
	movsd	 -8 * SIZE(BB), %xmm3
	addps	%xmm0, %xmm5
	movsd	-26 * SIZE(AA), %xmm0
	mulps	%xmm0,  %xmm3
	mulps	 -4 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm6
	movsd	 16 * SIZE(BB), %xmm3
	addps	%xmm0, %xmm7
	movsd	-16 * SIZE(AA), %xmm0
	mulps	%xmm2,  %xmm1
	mulps	  4 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm4
	movsd	  8 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm5
	movsd	-22 * SIZE(AA), %xmm2
	mulps	%xmm2,  %xmm1
	mulps	 12 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm6
	movsd	 32 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm7
	movsd	-20 * SIZE(AA), %xmm2
	mulps	%xmm2,  %xmm3
	mulps	 20 * SIZE(BB), %xmm2
	addps	%xmm3, %xmm4
	movsd	 24 * SIZE(BB), %xmm3
	addps	%xmm2, %xmm5
	movsd	-18 * SIZE(AA), %xmm2
	mulps	%xmm2,  %xmm3
	mulps	 28 * SIZE(BB), %xmm2
	addps	%xmm3, %xmm6
	movsd	 48 * SIZE(BB), %xmm3
	addps	%xmm2, %xmm7
	movsd	 -8 * SIZE(AA), %xmm2

	subl   $-16 * SIZE, AA
	addl   $ 64 * SIZE, BB
	decl   %eax
	jne    .L32
	ALIGN_4

.L35:
	movaps	ALPHA,  %xmm3

#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax
	BRANCH
	je .L38
	ALIGN_4

.L36:
	mulps	%xmm0,  %xmm1
	mulps	-28 * SIZE(BB), %xmm0
	addps	%xmm1, %xmm4
	movsd	-24 * SIZE(BB), %xmm1
	addps	%xmm0, %xmm5
	movsd	-30 * SIZE(AA), %xmm0

	addl	$2 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L36
	ALIGN_4

.L38:
	addps	%xmm6, %xmm4
	addps	%xmm7, %xmm5

	movsd	 0 * SIZE(%esi), %xmm0
	movhps	 2 * SIZE(%esi), %xmm0

	pshufd	$0x50, %xmm4,  %xmm2
	mulps	%xmm3, %xmm2
	addps	%xmm2, %xmm0

	movlps	%xmm0,   0 * SIZE(%esi)
	movhps	%xmm0,   2 * SIZE(%esi)

	movsd	 0 * SIZE(%esi, LDC), %xmm0
	movhps	 2 * SIZE(%esi, LDC), %xmm0

	pshufd	$0x50, %xmm5,  %xmm2
	mulps	%xmm3, %xmm2
	addps	%xmm2, %xmm0

	movlps	%xmm0,   0 * SIZE(%esi, LDC)
	movhps	%xmm0,   2 * SIZE(%esi, LDC)

	addl	$4 * SIZE, %esi
	ALIGN_4

.L40:
	movl	M,  I
	testl	$1, I
	jle	.L49

.L41:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leal	32 * SIZE + BUFFER, BB
#else
	leal	32 * SIZE + BUFFER, BB
	movl	KK, %eax
	leal	(, %eax,   4), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 8), BB /* because it's doubled */
#endif

	movss	-32 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movss	-32 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
	movss	-28 * SIZE(AA), %xmm2
	pxor	%xmm6, %xmm6
	movss	-16 * SIZE(BB), %xmm3
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
	je	.L45
	ALIGN_4

.L42:
	mulss	%xmm0,  %xmm1
	mulss	-28 * SIZE(BB), %xmm0
	addss	%xmm1, %xmm4
 	movss	-24 * SIZE(BB), %xmm1
	addss	%xmm0, %xmm5
	movss	-31 * SIZE(AA), %xmm0
	mulss	%xmm0,  %xmm1
	mulss	-20 * SIZE(BB), %xmm0
	addss	%xmm1, %xmm6
	movss	  0 * SIZE(BB), %xmm1
	addss	%xmm0, %xmm7
	movss	-30 * SIZE(AA), %xmm0
	mulss	%xmm0,  %xmm3
	mulss	-12 * SIZE(BB), %xmm0
	addss	%xmm3, %xmm4
	movss	 -8 * SIZE(BB), %xmm3
	addss	%xmm0, %xmm5
	movss	-29 * SIZE(AA), %xmm0
	mulss	%xmm0,  %xmm3
	mulss	 -4 * SIZE(BB), %xmm0
	addss	%xmm3, %xmm6
	movss	 16 * SIZE(BB), %xmm3
	addss	%xmm0, %xmm7
	movss	-24 * SIZE(AA), %xmm0
	mulss	%xmm2,  %xmm1
	mulss	  4 * SIZE(BB), %xmm2
	addss	%xmm1, %xmm4
	movss	  8 * SIZE(BB), %xmm1
	addss	%xmm2, %xmm5
	movss	-27 * SIZE(AA), %xmm2
	mulss	%xmm2,  %xmm1
	mulss	 12 * SIZE(BB), %xmm2
	addss	%xmm1, %xmm6
	movss	 32 * SIZE(BB), %xmm1
	addss	%xmm2, %xmm7
	movss	-26 * SIZE(AA), %xmm2
	mulss	%xmm2,  %xmm3
	mulss	 20 * SIZE(BB), %xmm2
	addss	%xmm3, %xmm4
	movss	 24 * SIZE(BB), %xmm3
	addss	%xmm2, %xmm5
	movss	-25 * SIZE(AA), %xmm2
	mulss	%xmm2,  %xmm3
	mulss	 28 * SIZE(BB), %xmm2
	addss	%xmm3, %xmm6
	movss	 48 * SIZE(BB), %xmm3
	addss	%xmm2, %xmm7
	movss	-20 * SIZE(AA), %xmm2

	subl   $-8 * SIZE, AA
	addl   $64 * SIZE, BB
	decl   %eax
	jne    .L42
	ALIGN_4

.L45:
	movaps	ALPHA,  %xmm3

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
	mulss	%xmm0,  %xmm1
	mulss	-28 * SIZE(BB), %xmm0
	addss	%xmm1, %xmm4
	movss	-24 * SIZE(BB), %xmm1
	addss	%xmm0, %xmm5
	movss	-31 * SIZE(AA), %xmm0

	addl	$1 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L46
	ALIGN_4

.L48:
	addss	%xmm6, %xmm4
	addss	%xmm7, %xmm5

	movsd	 (%esi), %xmm0
	movhps	 (%esi, LDC), %xmm0

	shufps	 $0, %xmm5, %xmm4

	mulps	%xmm3, %xmm4
	addps	%xmm4, %xmm0

	movlps	%xmm0,   (%esi)
	movhps	%xmm0,   (%esi, LDC)
	ALIGN_4

.L49:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	leal	(, LDC, 2), %eax
	addl	%eax, C
	decl	J
	jg	.L01
	ALIGN_4

.L50:
	movl	N, %eax
	testl	$1, %eax
	jle	.L999
	ALIGN_4

.L51:
	leal	32 * SIZE + BUFFER, BB

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	K, %eax
	sarl	$3, %eax
	jle	.L55
	ALIGN_4

.L52:
	movss	-32 * SIZE(B), %xmm0
	movss	-31 * SIZE(B), %xmm1
	movss	-30 * SIZE(B), %xmm2
	movss	-29 * SIZE(B), %xmm3
	movss	-28 * SIZE(B), %xmm4
	movss	-27 * SIZE(B), %xmm5
	movss	-26 * SIZE(B), %xmm6
	movss	-25 * SIZE(B), %xmm7

	shufps	$0, %xmm0, %xmm0
	shufps	$0, %xmm1, %xmm1
	shufps	$0, %xmm2, %xmm2
	shufps	$0, %xmm3, %xmm3
	shufps	$0, %xmm4, %xmm4
	shufps	$0, %xmm5, %xmm5
	shufps	$0, %xmm6, %xmm6
	shufps	$0, %xmm7, %xmm7

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
	jne	.L52
	ALIGN_4

.L55:
	movl	K, %eax
	andl	$7, %eax
	BRANCH
	jle	.L60
	ALIGN_4

.L56:
	movss	-32 * SIZE(B), %xmm0
	shufps	$0, %xmm0, %xmm0
	movaps	%xmm0,  -32 * SIZE(BB)

	addl	$1 * SIZE, B
	addl	$4 * SIZE, BB
	decl	%eax
	jne	.L56
	ALIGN_4

.L60:
	movl	C, C1
	movl	A, AA
	movl	M,  I
	sarl	$3, I
	jle	.L70
	ALIGN_4

.L61:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leal	32 * SIZE + BUFFER, BB
#else
	leal	32 * SIZE + BUFFER, BB
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 2), BB /* because it's doubled */
#endif

	movaps	-32 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movaps	-32 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
	movaps	-16 * SIZE(AA), %xmm2
	pxor	%xmm6, %xmm6
	movaps	-16 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

	prefetcht0	3 * SIZE(C1)

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$8, %eax
#else
	addl	$1, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L65
	ALIGN_4

.L62:
	mulps	%xmm1, %xmm0
	mulps	-28 * SIZE(AA), %xmm1
	addps	%xmm0, %xmm4
	movaps	-24 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm6
	movaps	-28 * SIZE(BB), %xmm1
	mulps	%xmm1, %xmm0
	mulps	-20 * SIZE(AA), %xmm1
	addps	%xmm0, %xmm5
	movaps	  0 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm7
	movaps	-24 * SIZE(BB), %xmm1
	mulps	%xmm1, %xmm2
	mulps	-12 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm4
	movaps	 -8 * SIZE(AA), %xmm2
	addps	%xmm1, %xmm6
	movaps	-20 * SIZE(BB), %xmm1
	mulps	%xmm1, %xmm2
	mulps	 -4 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm5
	movaps	 16 * SIZE(AA), %xmm2
	addps	%xmm1, %xmm7
	movaps	  0 * SIZE(BB), %xmm1
	mulps	%xmm3, %xmm0
	mulps	  4 * SIZE(AA), %xmm3
	addps	%xmm0, %xmm4
	movaps	  8 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm6
	movaps	-12 * SIZE(BB), %xmm3
	mulps	%xmm3, %xmm0
	mulps	 12 * SIZE(AA), %xmm3
	addps	%xmm0, %xmm5
	movaps	 32 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm7
	movaps	 -8 * SIZE(BB), %xmm3
	mulps	%xmm3, %xmm2
	mulps	 20 * SIZE(AA), %xmm3
	addps	%xmm2, %xmm4
	movaps	 24 * SIZE(AA), %xmm2
	addps	%xmm3, %xmm6
	movaps	 -4 * SIZE(BB), %xmm3
	mulps	%xmm3, %xmm2
	mulps	 28 * SIZE(AA), %xmm3
	addps	%xmm2, %xmm5
	movaps	 48 * SIZE(AA), %xmm2
	addps	%xmm3, %xmm7
	movaps	 16 * SIZE(BB), %xmm3

	addl   $ 64 * SIZE, AA
	subl   $-32 * SIZE, BB
	decl   %eax
	jne    .L62
	ALIGN_4

.L65:
	movaps	ALPHA,  %xmm3

#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax
	BRANCH
	je .L68
	ALIGN_4

.L66:
	mulps	%xmm1, %xmm0
	mulps	-28 * SIZE(AA), %xmm1
	addps	%xmm0, %xmm4
	movaps	-24 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm6
	movaps	-28 * SIZE(BB), %xmm1

	addl	$8 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L66
	ALIGN_4

.L68:
	addps	%xmm5, %xmm4
	addps	%xmm7, %xmm6

	movsd	 0 * SIZE(%esi), %xmm0
	movhps	 2 * SIZE(%esi), %xmm0
	movsd	 4 * SIZE(%esi), %xmm1
	movhps	 6 * SIZE(%esi), %xmm1

	pshufd	$0x50, %xmm4,  %xmm2
	pshufd	$0xfa, %xmm4,  %xmm4

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm4

	addps	%xmm2, %xmm0
	addps	%xmm4, %xmm1

	movlps	%xmm0,   0 * SIZE(%esi)
	movhps	%xmm0,   2 * SIZE(%esi)
	movlps	%xmm1,   4 * SIZE(%esi)
	movhps	%xmm1,   6 * SIZE(%esi)

	movsd	 8 * SIZE(%esi), %xmm0
	movhps	10 * SIZE(%esi), %xmm0
	movsd	12 * SIZE(%esi), %xmm1
	movhps	14 * SIZE(%esi), %xmm1

	pshufd	$0x50, %xmm6,  %xmm2
	pshufd	$0xfa, %xmm6,  %xmm6

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm6

	addps	%xmm2, %xmm0
	addps	%xmm6, %xmm1

	movlps	%xmm0,   8 * SIZE(%esi)
	movhps	%xmm0,  10 * SIZE(%esi)
	movlps	%xmm1,  12 * SIZE(%esi)
	movhps	%xmm1,  14 * SIZE(%esi)

	addl	$16 * SIZE, C1
	decl	I
	jg	.L61
	ALIGN_4

.L70:
	movl	M,  I
	testl	$4, I
	jle	.L80

.L71:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leal	32 * SIZE + BUFFER, BB
#else
	leal	32 * SIZE + BUFFER, BB
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 2), BB /* because it's doubled */
#endif

	movaps	-32 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movaps	-32 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
	movaps	-16 * SIZE(AA), %xmm2
	movaps	-16 * SIZE(BB), %xmm3

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
	je	.L75
	ALIGN_4

.L72:
	mulps	%xmm0,  %xmm1
	movaps	-28 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm4
	movaps	-28 * SIZE(BB), %xmm1
	mulps	%xmm0,  %xmm1
	movaps	-24 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm5
	movaps	-24 * SIZE(BB), %xmm1
	mulps	%xmm0,  %xmm1
	movaps	-20 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm4
	movaps	-20 * SIZE(BB), %xmm1
	mulps	%xmm0,  %xmm1
	movaps	  0 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm5
	movaps	  0 * SIZE(BB), %xmm1
	mulps	%xmm2,  %xmm3
	movaps	-12 * SIZE(AA), %xmm2
	addps	%xmm3, %xmm4
	movaps	-12 * SIZE(BB), %xmm3
	mulps	%xmm2,  %xmm3
	movaps	 -8 * SIZE(AA), %xmm2
	addps	%xmm3, %xmm5
	movaps	 -8 * SIZE(BB), %xmm3
	mulps	%xmm2,  %xmm3
	movaps	 -4 * SIZE(AA), %xmm2
	addps	%xmm3, %xmm4
	movaps	 -4 * SIZE(BB), %xmm3
	mulps	%xmm2,  %xmm3
	movaps	 16 * SIZE(AA), %xmm2
	addps	%xmm3, %xmm5
	movaps	 16 * SIZE(BB), %xmm3

	subl   $-32 * SIZE, AA
	subl   $-32 * SIZE, BB
	decl   %eax
	jne    .L72
	ALIGN_4

.L75:
	movaps	ALPHA,  %xmm3

#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax
	BRANCH
	je .L78
	ALIGN_4

.L76:
	mulps	%xmm0,  %xmm1
	movaps	-28 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm4
	movaps	-28 * SIZE(BB), %xmm1

	addl	$4 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L76
	ALIGN_4

.L78:
	addps	%xmm5, %xmm4

	movsd	 0 * SIZE(%esi), %xmm0
	movhps	 2 * SIZE(%esi), %xmm0
	movsd	 4 * SIZE(%esi), %xmm1
	movhps	 6 * SIZE(%esi), %xmm1

	pshufd	$0x50, %xmm4,  %xmm2
	pshufd	$0xfa, %xmm4,  %xmm4

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm4

	addps	%xmm2, %xmm0
	addps	%xmm4, %xmm1

	movlps	%xmm0,   0 * SIZE(%esi)
	movhps	%xmm0,   2 * SIZE(%esi)
	movlps	%xmm1,   4 * SIZE(%esi)
	movhps	%xmm1,   6 * SIZE(%esi)

	addl	$8 * SIZE, %esi
	ALIGN_4

.L80:
	movl	M,  I
	testl	$2, I
	jle	.L90

.L81:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leal	32 * SIZE + BUFFER, BB
#else
	leal	32 * SIZE + BUFFER, BB
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 2), BB /* because it's doubled */
#endif

	movsd	-32 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movsd	-32 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
	movsd	-16 * SIZE(BB), %xmm3
	movsd	-24 * SIZE(AA), %xmm2

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
	je	.L85
	ALIGN_4

.L82:
	mulps	%xmm0,  %xmm1
	movsd	-30 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm4
	movsd	-28 * SIZE(BB), %xmm1
	mulps	%xmm0,  %xmm1
	movsd	-28 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm5
	movsd	-24 * SIZE(BB), %xmm1
	mulps	%xmm0,  %xmm1
	movsd	-26 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm4
	movsd	-20 * SIZE(BB), %xmm1
	mulps	%xmm0,  %xmm1
	movsd	-16 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm5
	movsd	 -0 * SIZE(BB), %xmm1
	mulps	%xmm2,  %xmm3
	movsd	-22 * SIZE(AA), %xmm2
	addps	%xmm3, %xmm4
	movsd	-12 * SIZE(BB), %xmm3
	mulps	%xmm2,  %xmm3
	movsd	-20 * SIZE(AA), %xmm2
	addps	%xmm3, %xmm5
	movsd	 -8 * SIZE(BB), %xmm3
	mulps	%xmm2,  %xmm3
	movsd	-18 * SIZE(AA), %xmm2
	addps	%xmm3, %xmm4
	movsd	 -4 * SIZE(BB), %xmm3
	mulps	%xmm2,  %xmm3
	movsd	 -8 * SIZE(AA), %xmm2
	addps	%xmm3, %xmm5
	movsd	 16 * SIZE(BB), %xmm3

	subl   $-16 * SIZE, AA
	subl   $-32 * SIZE, BB
	decl   %eax
	jne    .L82
	ALIGN_4

.L85:
	movaps	ALPHA,  %xmm3

#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax
	BRANCH
	je .L88
	ALIGN_4

.L86:
	mulps	%xmm0,  %xmm1
	movsd	-30 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm4
	movsd	-28 * SIZE(BB), %xmm1

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L86
	ALIGN_4

.L88:
	addps	%xmm5, %xmm4

	movsd	 0 * SIZE(%esi), %xmm0
	movhps	 2 * SIZE(%esi), %xmm0

	pshufd	$0x50, %xmm4,  %xmm2
	mulps	%xmm3, %xmm2
	addps	%xmm2, %xmm0

	movlps	%xmm0,   0 * SIZE(%esi)
	movhps	%xmm0,   2 * SIZE(%esi)

	addl	$4 * SIZE, %esi
	ALIGN_4

.L90:
	movl	M,  I
	testl	$1, I
	jle	.L99

.L91:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leal	32 * SIZE + BUFFER, BB
#else
	leal	32 * SIZE + BUFFER, BB
	movl	KK, %eax
	leal	(, %eax,   4), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB /* because it's doubled */
#endif

	movss	-32 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movss	-32 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
	movss	-16 * SIZE(BB), %xmm3
	movss	-28 * SIZE(AA), %xmm2

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
	je	.L95
	ALIGN_4

.L92:
	mulss	%xmm0,  %xmm1
	movss	-31 * SIZE(AA), %xmm0
	addss	%xmm1, %xmm4
	movss	-28 * SIZE(BB), %xmm1
	mulss	%xmm0,  %xmm1
	movss	-30 * SIZE(AA), %xmm0
	addss	%xmm1, %xmm5
	movss	-24 * SIZE(BB), %xmm1
	mulss	%xmm0,  %xmm1
	movss	-29 * SIZE(AA), %xmm0
	addss	%xmm1, %xmm4
	movss	-20 * SIZE(BB), %xmm1
	mulss	%xmm0,  %xmm1
	movss	-24 * SIZE(AA), %xmm0
	addss	%xmm1, %xmm5
	movss	 -0 * SIZE(BB), %xmm1
	mulss	%xmm2,  %xmm3
	movss	-27 * SIZE(AA), %xmm2
	addss	%xmm3, %xmm4
	movss	-12 * SIZE(BB), %xmm3
	mulss	%xmm2,  %xmm3
	movss	-26 * SIZE(AA), %xmm2
	addss	%xmm3, %xmm5
	movss	 -8 * SIZE(BB), %xmm3
	mulss	%xmm2,  %xmm3
	movss	-25 * SIZE(AA), %xmm2
	addss	%xmm3, %xmm4
	movss	 -4 * SIZE(BB), %xmm3
	mulss	%xmm2,  %xmm3
	movss	-20 * SIZE(AA), %xmm2
	addss	%xmm3, %xmm5
	movss	 16 * SIZE(BB), %xmm3

	subl   $ -8 * SIZE, AA
	subl   $-32 * SIZE, BB
	decl   %eax
	jne    .L92
	ALIGN_4

.L95:
	movaps	ALPHA,  %xmm3

#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax
	BRANCH
	je .L98
	ALIGN_4

.L96:
	mulss	%xmm0,  %xmm1
	movss	-31 * SIZE(AA), %xmm0
	addss	%xmm1, %xmm4
	movss	-28 * SIZE(BB), %xmm1

	addl	$1 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L96
	ALIGN_4

.L98:
	addss	%xmm5, %xmm4

	movsd	 0 * SIZE(%esi), %xmm0

	pshufd	$0x50, %xmm4,  %xmm2
	mulps	%xmm3, %xmm2
	addps	%xmm2, %xmm0

	movlps	%xmm0,   0 * SIZE(%esi)
	ALIGN_4

.L99:
	addl	LDC, C
	ALIGN_4


.L999:
	movl	OLD_STACK, %esp

	EMMS

	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
