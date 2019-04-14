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

#define J	 0 + STACK(%esp)
#define BX	 4 + STACK(%esp)
#define KK	 8 + STACK(%esp)
#define KKK	12 + STACK(%esp)

#define PREFETCH     prefetcht0
#define PREFETCHSIZE 84

#define AA	%edx
#define BB	%ecx
#define CO1	%esi
#define LDC	%ebp
#define B	%edi

	PROLOGUE

	subl	$ARGS, %esp

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

	sall	$ZBASE_SHIFT, LDC

	movl	N,  %eax
	sarl	$1, %eax
	movl	%eax, J
	jle	.L30
	ALIGN_2

.L10:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	K, %eax
	sall	$BASE_SHIFT + 1, %eax
	leal	(B, %eax), %eax
	movl	%eax, BX

	movl	C, CO1		# coffset = c
	leal	(, LDC, 2), %eax
	addl	%eax, C

	movl	A, AA		# aoffset = a

	movl	M,  %ebx
	sarl	$1, %ebx	# i = (m >> 2)
	jle	.L20
	ALIGN_4

.L11:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BB
#else
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 2), BB
#endif

	movl	BX, %eax
	prefetcht0  0 * SIZE(%eax)
	subl	$-8 * SIZE, BX

	movsd	 0 * SIZE(AA), %xmm0
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3

	xorps	%xmm4, %xmm4
	prefetcht0	3 * SIZE(CO1)
	xorps	%xmm5, %xmm5
	prefetcht0	3 * SIZE(CO1, LDC)
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
	sarl	$2, %eax
	je	.L15
	ALIGN_4

.L12:
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)

	addsd	%xmm2, %xmm6
	movsd	 1 * SIZE(AA), %xmm2
	movaps	%xmm0, %xmm1
	mulsd	 0 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm7
	mulsd	 1 * SIZE(BB), %xmm1

	addsd	%xmm0, %xmm4
	movsd	 2 * SIZE(AA), %xmm0
	movaps	%xmm2, %xmm3
	mulsd	 0 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm5
	mulsd	 1 * SIZE(BB), %xmm3

	addsd	%xmm2, %xmm6
	movsd	 3 * SIZE(AA), %xmm2
	movaps	%xmm0, %xmm1
	mulsd	 2 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm7
	mulsd	 3 * SIZE(BB), %xmm1

	addsd	%xmm0, %xmm4
	movsd	 4 * SIZE(AA), %xmm0
	movaps	%xmm2, %xmm3
	mulsd	 2 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm5
	mulsd	 3 * SIZE(BB), %xmm3

	addsd	%xmm2, %xmm6
	movsd	 5 * SIZE(AA), %xmm2
	movaps	%xmm0, %xmm1
	mulsd	 4 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm7
	mulsd	 5 * SIZE(BB), %xmm1

	addsd	%xmm0, %xmm4
	movsd	 6 * SIZE(AA), %xmm0
	movaps	%xmm2, %xmm3
	mulsd	 4 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm5
	mulsd	 5 * SIZE(BB), %xmm3

	addsd	%xmm2, %xmm6
	movsd	 7 * SIZE(AA), %xmm2
	movaps	%xmm0, %xmm1
	mulsd	 6 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm7
	mulsd	 7 * SIZE(BB), %xmm1

	addsd	%xmm0, %xmm4
	movsd	 8 * SIZE(AA), %xmm0
	movaps	%xmm2, %xmm3
	mulsd	 6 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm5
	mulsd	 7 * SIZE(BB), %xmm3

	addl   $8 * SIZE, BB
	addl   $8 * SIZE, AA
	decl   %eax
	jne    .L12
	ALIGN_4

.L15:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$3, %eax		# if (k & 1)
	BRANCH
	je .L18
	ALIGN_3

.L16:
	addsd	%xmm2, %xmm6
	movsd	 1 * SIZE(AA), %xmm2
	movaps	%xmm0, %xmm1
	mulsd	 0 * SIZE(BB), %xmm0
	addsd	%xmm3, %xmm7
	mulsd	 1 * SIZE(BB), %xmm1

	addsd	%xmm0, %xmm4
	movsd	 2 * SIZE(AA), %xmm0
	movaps	%xmm2, %xmm3
	mulsd	 0 * SIZE(BB), %xmm2
	addsd	%xmm1, %xmm5
	mulsd	 1 * SIZE(BB), %xmm3

	addl	$2 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L16
	ALIGN_4

.L18:
	movsd	ALPHA_R,  %xmm0
	movsd	ALPHA_I,  %xmm1

	addsd	%xmm2, %xmm6
	addsd	%xmm3, %xmm7

	movaps	%xmm4, %xmm2
	mulsd	%xmm0, %xmm4
	mulsd	%xmm1, %xmm2

	movaps	%xmm6, %xmm3
	mulsd	%xmm0, %xmm6
	mulsd	%xmm1, %xmm3

	addsd	0 * SIZE(CO1), %xmm4
	addsd	1 * SIZE(CO1), %xmm2
	addsd	2 * SIZE(CO1), %xmm6
	addsd	3 * SIZE(CO1), %xmm3

	movlps	%xmm4, 0 * SIZE(CO1)
	movlps	%xmm2, 1 * SIZE(CO1)
	movlps	%xmm6, 2 * SIZE(CO1)
	movlps	%xmm3, 3 * SIZE(CO1)

	movaps	%xmm5, %xmm2
	mulsd	%xmm0, %xmm5
	mulsd	%xmm1, %xmm2

	movaps	%xmm7, %xmm3
	mulsd	%xmm0, %xmm7
	mulsd	%xmm1, %xmm3

	addsd	0 * SIZE(CO1, LDC), %xmm5
	addsd	1 * SIZE(CO1, LDC), %xmm2
	addsd	2 * SIZE(CO1, LDC), %xmm7
	addsd	3 * SIZE(CO1, LDC), %xmm3

	movlps	%xmm5, 0 * SIZE(CO1, LDC)
	movlps	%xmm2, 1 * SIZE(CO1, LDC)
	movlps	%xmm7, 2 * SIZE(CO1, LDC)
	movlps	%xmm3, 3 * SIZE(CO1, LDC)

	addl	$4 * SIZE, CO1
	decl	%ebx
	jg	.L11
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
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 2), BB
#endif

	movsd	 0 * SIZE(AA), %xmm0
	xorps	%xmm2, %xmm2
	xorps	%xmm3, %xmm3
	xorps	%xmm4, %xmm4
	xorps	%xmm5, %xmm5

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
	sarl	$2, %eax
	je	.L25
	ALIGN_4

.L22:
	addsd	%xmm2, %xmm4
	movsd	 0 * SIZE(BB), %xmm2
	addsd	%xmm3, %xmm5
	movsd	 1 * SIZE(BB), %xmm3

	mulsd	%xmm0, %xmm2
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)
	mulsd	%xmm0, %xmm3
	movsd	 1 * SIZE(AA), %xmm0

	addsd	%xmm2, %xmm4
	movsd	 2 * SIZE(BB), %xmm2
	addsd	%xmm3, %xmm5
	movsd	 3 * SIZE(BB), %xmm3

	mulsd	%xmm0, %xmm2
	mulsd	%xmm0, %xmm3
	movsd	 2 * SIZE(AA), %xmm0

	addsd	%xmm2, %xmm4
	movsd	 4 * SIZE(BB), %xmm2
	addsd	%xmm3, %xmm5
	movsd	 5 * SIZE(BB), %xmm3

	mulsd	%xmm0, %xmm2
	mulsd	%xmm0, %xmm3
	movsd	 3 * SIZE(AA), %xmm0

	addsd	%xmm2, %xmm4
	movsd	 6 * SIZE(BB), %xmm2
	addsd	%xmm3, %xmm5
	movsd	 7 * SIZE(BB), %xmm3

	mulsd	%xmm0, %xmm2
	mulsd	%xmm0, %xmm3
	movsd	 4 * SIZE(AA), %xmm0

	addl   $4 * SIZE, AA
	addl   $8 * SIZE, BB
	decl   %eax
	jne    .L22
	ALIGN_4

.L25:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$3, %eax		# if (k & 1)
	BRANCH
	je .L28
	ALIGN_3

.L26:
	addsd	%xmm2, %xmm4
	movsd	 0 * SIZE(BB), %xmm2
	addsd	%xmm3, %xmm5
	movsd	 1 * SIZE(BB), %xmm3

	mulsd	%xmm0, %xmm2
	mulsd	%xmm0, %xmm3
	movsd	 1 * SIZE(AA), %xmm0

	addl	$1 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L26
	ALIGN_4

.L28:
	movsd	ALPHA_R,  %xmm0
	movsd	ALPHA_I,  %xmm1

	addsd	%xmm2, %xmm4
	addsd	%xmm3, %xmm5

	movaps	%xmm4, %xmm2
	mulsd	%xmm0, %xmm4
	mulsd	%xmm1, %xmm2

	movaps	%xmm5, %xmm3
	mulsd	%xmm0, %xmm5
	mulsd	%xmm1, %xmm3

	addsd	0 * SIZE(CO1), %xmm4
	addsd	1 * SIZE(CO1), %xmm2
	addsd	0 * SIZE(CO1, LDC), %xmm5
	addsd	1 * SIZE(CO1, LDC), %xmm3

	movlps	%xmm4, 0 * SIZE(CO1)
	movlps	%xmm2, 1 * SIZE(CO1)
	movlps	%xmm5, 0 * SIZE(CO1, LDC)
	movlps	%xmm3, 1 * SIZE(CO1, LDC)
	ALIGN_4

.L29:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	movl	BB, B
	decl	J
	jg	.L10
	ALIGN_4

.L30:
	testl	$1, N
	je	.L999

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	C, CO1
	addl	LDC, C

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
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(B,  %eax, 1), BB
#endif

	movsd	 0 * SIZE(BB), %xmm1
	xorps	%xmm0, %xmm0
	prefetcht0	3 * SIZE(CO1)
	xorps	%xmm2, %xmm2
	xorps	%xmm4, %xmm4
	xorps	%xmm6, %xmm6

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
	sarl	$2, %eax
	je	.L35
	ALIGN_4

.L32:
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)

	addsd	%xmm0, %xmm4
	movsd	 0 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm6
	movsd	 1 * SIZE(AA), %xmm2
	mulsd	 %xmm1, %xmm0
	mulsd	 %xmm1, %xmm2
	movsd	 1 * SIZE(BB), %xmm1

	addsd	%xmm0, %xmm4
	movsd	 2 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm6
	movsd	 3 * SIZE(AA), %xmm2
	mulsd	 %xmm1, %xmm0
	mulsd	 %xmm1, %xmm2
	movsd	 2 * SIZE(BB), %xmm1

	addsd	%xmm0, %xmm4
	movsd	 4 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm6
	movsd	 5 * SIZE(AA), %xmm2
	mulsd	 %xmm1, %xmm0
	mulsd	 %xmm1, %xmm2
	movsd	 3 * SIZE(BB), %xmm1

	addsd	%xmm0, %xmm4
	movsd	 6 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm6
	movsd	 7 * SIZE(AA), %xmm2
	mulsd	 %xmm1, %xmm0
	mulsd	 %xmm1, %xmm2
	movsd	 4 * SIZE(BB), %xmm1

	addl   $8 * SIZE, AA
	addl   $4 * SIZE, BB
	decl   %eax
	jne    .L32
	ALIGN_4

.L35:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$3, %eax		# if (k & 1)
	BRANCH
	je .L38
	ALIGN_3

.L36:
	addsd	%xmm0, %xmm4
	movsd	 0 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm6
	movsd	 1 * SIZE(AA), %xmm2
	mulsd	 %xmm1, %xmm0
	mulsd	 %xmm1, %xmm2
	movsd	 1 * SIZE(BB), %xmm1

	addl	$2 * SIZE, AA
	addl	$1 * SIZE, BB
	decl	%eax
	jg	.L36
	ALIGN_4

.L38:
	addsd	%xmm0, %xmm4
	addsd	%xmm2, %xmm6

	movsd	ALPHA_R,  %xmm0
	movsd	ALPHA_I,  %xmm1

	movaps	%xmm4, %xmm2
	mulsd	%xmm0, %xmm4
	mulsd	%xmm1, %xmm2

	movaps	%xmm6, %xmm3
	mulsd	%xmm0, %xmm6
	mulsd	%xmm1, %xmm3

	addsd	0 * SIZE(CO1), %xmm4
	addsd	1 * SIZE(CO1), %xmm2
	addsd	2 * SIZE(CO1), %xmm6
	addsd	3 * SIZE(CO1), %xmm3

	movlps	%xmm4, 0 * SIZE(CO1)
	movlps	%xmm2, 1 * SIZE(CO1)
	movlps	%xmm6, 2 * SIZE(CO1)
	movlps	%xmm3, 3 * SIZE(CO1)

	addl	$4 * SIZE, CO1
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
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 1), AA
	leal	(B,  %eax, 1), BB
#endif

	movsd	 0 * SIZE(AA), %xmm0
	xorps	%xmm4, %xmm4
	movsd	 0 * SIZE(BB), %xmm2
	xorps	%xmm5, %xmm5

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
	sarl	$2, %eax
	je	.L45
	ALIGN_4

.L42:
	mulsd	%xmm0, %xmm2
	movsd	 1 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm4
	movsd	 1 * SIZE(BB), %xmm2

	mulsd	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm5
	movsd	 2 * SIZE(BB), %xmm2

	mulsd	%xmm0, %xmm2
	movsd	 3 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm4
	movsd	 3 * SIZE(BB), %xmm2

	mulsd	%xmm0, %xmm2
	movsd	 4 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm5
	movsd	 4 * SIZE(BB), %xmm2

	addl   $4 * SIZE, AA
	addl   $4 * SIZE, BB
	decl   %eax
	jne    .L42
	ALIGN_4

.L45:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$3, %eax		# if (k & 1)
	BRANCH
	je .L48
	ALIGN_3

.L46:
	mulsd	%xmm0, %xmm2
	movsd	 1 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm4
	movsd	 1 * SIZE(BB), %xmm2

	addl	$1 * SIZE, AA
	addl	$1 * SIZE, BB
	decl	%eax
	jg	.L46
	ALIGN_4

.L48:
	addsd	%xmm5, %xmm4

	movsd	ALPHA_R,  %xmm0
	movsd	ALPHA_I,  %xmm1

	movaps	%xmm4, %xmm2
	mulsd	%xmm0, %xmm4
	mulsd	%xmm1, %xmm2

	addsd	0 * SIZE(CO1), %xmm4
	addsd	1 * SIZE(CO1), %xmm2

	movlps	%xmm4, 0 * SIZE(CO1)
	movlps	%xmm2, 1 * SIZE(CO1)
	ALIGN_4

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp

	addl	$ARGS, %esp
	ret

	EPILOGUE
