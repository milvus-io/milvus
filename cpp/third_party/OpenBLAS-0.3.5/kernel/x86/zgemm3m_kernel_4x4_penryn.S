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
#define ALPHA	16 + STACK + ARGS(%esp)
#define A	24 + STACK + ARGS(%esp)
#define ARG_B	28 + STACK + ARGS(%esp)
#define C	32 + STACK + ARGS(%esp)
#define ARG_LDC	36 + STACK + ARGS(%esp)

#define J	 0 + STACK(%esp)
#define BX	 4 + STACK(%esp)
#define KK	 8 + STACK(%esp)
#define KKK	12 + STACK(%esp)

#define PREFETCH_R   (8 * 4)

#define PREFETCHSIZE  (8 * 17 + 4)
#define PREFETCH     prefetcht0

#define AA	%edx
#define BB	%ecx
#define LDC	%ebp
#define B	%edi
#define C1	%esi
#define I	%ebx

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
	sarl	$2, %eax
	movl	%eax, J
	jle	.L40
	ALIGN_4

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	K, %eax
	sall	$BASE_SHIFT + 2, %eax
	leal	(B, %eax), %eax
	movl	%eax, BX

	movl	C, C1
	movl	A, AA

	movl	M,  I
	sarl	$2, I
	jle	.L20
	ALIGN_4

.L11:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BB
#else
	movl	B, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 4), BB
#endif

	movl	BX, %eax

	prefetcht2  -32 * SIZE(%eax)
	subl	$-16 * SIZE, BX

	leal		(C1, LDC, 2), %eax

	movaps	-32 * SIZE(AA), %xmm0
	pxor	%xmm2, %xmm2
	movaps	-32 * SIZE(BB), %xmm1
	pxor	%xmm3, %xmm3

	pxor	%xmm4, %xmm4
	prefetcht0	3 * SIZE(C1)
	pxor	%xmm5, %xmm5
	prefetcht0	3 * SIZE(C1, LDC)
	pxor	%xmm6, %xmm6
	prefetcht0	3 * SIZE(%eax)
	pxor	%xmm7, %xmm7
	prefetcht0	3 * SIZE(%eax, LDC)

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
	addl	$4, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L15
	ALIGN_4

.L12:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	addps	%xmm2, %xmm7
	pshufd	$0x93, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm6
	pshufd	$0x93, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm2, %xmm5
	pshufd	$0x93, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm1, %xmm4
	movaps	-28 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm7
	pshufd	$0x93, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm6
	pshufd	$0x93, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm2, %xmm5
	pshufd	$0x93, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm1, %xmm4
	movaps	-24 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-24 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm7
	pshufd	$0x93, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm6
	pshufd	$0x93, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm2, %xmm5
	pshufd	$0x93, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm1, %xmm4
	movaps	-20 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-20 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm7
	pshufd	$0x93, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm6
	pshufd	$0x93, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm2, %xmm5
	pshufd	$0x93, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm1, %xmm4
	movaps	-16 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-16 * SIZE(AA), %xmm0

	PREFETCH (PREFETCHSIZE + 16) * SIZE(AA)

	addps	%xmm2, %xmm7
	pshufd	$0x93, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm6
	pshufd	$0x93, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm2, %xmm5
	pshufd	$0x93, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm1, %xmm4
	movaps	-12 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-12 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm7
	pshufd	$0x93, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm6
	pshufd	$0x93, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm2, %xmm5
	pshufd	$0x93, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm1, %xmm4
	movaps	 -8 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	 -8 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm7
	pshufd	$0x93, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm6
	pshufd	$0x93, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm2, %xmm5
	pshufd	$0x93, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm1, %xmm4
	movaps	 -4 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	 -4 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm7
	subl   $-32 * SIZE, BB
	pshufd	$0x93, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm6
	pshufd	$0x93, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm2, %xmm5
	subl   $-32 * SIZE, AA
	pshufd	$0x93, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm1, %xmm4
	movaps	-32 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-32 * SIZE(AA), %xmm0

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
	addps	%xmm2, %xmm7
	pshufd	$0x93, %xmm1, %xmm2
	mulps	%xmm0, %xmm1
	addps	%xmm3, %xmm6
	pshufd	$0x93, %xmm2, %xmm3
	mulps	%xmm0, %xmm2

	addps	%xmm2, %xmm5
	pshufd	$0x93, %xmm3, %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm1, %xmm4
	movaps	-28 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(AA), %xmm0

	addl	$4 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L16
	ALIGN_4

.L18:
	addps	%xmm3, %xmm6
	addps	%xmm2, %xmm7

	movddup	ALPHA,  %xmm3

	pshufd	$0x39, %xmm5, %xmm2
	pshufd	$0x4e, %xmm6, %xmm0
	pshufd	$0x93, %xmm7, %xmm7

	movaps	 %xmm4, %xmm6
	unpcklps %xmm0, %xmm4
	unpckhps %xmm0, %xmm6

	movaps	 %xmm2, %xmm1
	unpcklps %xmm7, %xmm2
	unpckhps %xmm7, %xmm1

	movaps	 %xmm4, %xmm5
	unpcklps %xmm2, %xmm4
	unpckhps %xmm2, %xmm5

	movaps	 %xmm6, %xmm7
	unpcklps %xmm1, %xmm6
	unpckhps %xmm1, %xmm7

	pshufd	 $0x93, %xmm5, %xmm5
	pshufd	 $0x4e, %xmm6, %xmm6
	pshufd	 $0x39, %xmm7, %xmm7

	leal		(C1, LDC, 2), %eax

	movsd	 0 * SIZE(C1), %xmm0
	movhps	 2 * SIZE(C1), %xmm0
	movsd	 4 * SIZE(C1), %xmm1
	movhps	 6 * SIZE(C1), %xmm1

	pshufd	$0x50, %xmm4,  %xmm2
	pshufd	$0xfa, %xmm4,  %xmm4

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm4

	addps	%xmm2, %xmm0
	addps	%xmm4, %xmm1

	movlps	%xmm0,   0 * SIZE(C1)
	movhps	%xmm0,   2 * SIZE(C1)
	movlps	%xmm1,   4 * SIZE(C1)
	movhps	%xmm1,   6 * SIZE(C1)

	movsd	 0 * SIZE(C1, LDC), %xmm0
	movhps	 2 * SIZE(C1, LDC), %xmm0
	movsd	 4 * SIZE(C1, LDC), %xmm1
	movhps	 6 * SIZE(C1, LDC), %xmm1

	pshufd	$0x50, %xmm5,  %xmm2
	pshufd	$0xfa, %xmm5,  %xmm5

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm5

	addps	%xmm2, %xmm0
	addps	%xmm5, %xmm1

	movlps	%xmm0,   0 * SIZE(C1, LDC)
	movhps	%xmm0,   2 * SIZE(C1, LDC)
	movlps	%xmm1,   4 * SIZE(C1, LDC)
	movhps	%xmm1,   6 * SIZE(C1, LDC)

	movsd	 0 * SIZE(%eax), %xmm0
	movhps	 2 * SIZE(%eax), %xmm0
	movsd	 4 * SIZE(%eax), %xmm1
	movhps	 6 * SIZE(%eax), %xmm1

	pshufd	$0x50, %xmm6,  %xmm2
	pshufd	$0xfa, %xmm6,  %xmm6

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm6

	addps	%xmm2, %xmm0
	addps	%xmm6, %xmm1

	movlps	%xmm0,   0 * SIZE(%eax)
	movhps	%xmm0,   2 * SIZE(%eax)
	movlps	%xmm1,   4 * SIZE(%eax)
	movhps	%xmm1,   6 * SIZE(%eax)

	movsd	 0 * SIZE(%eax, LDC), %xmm0
	movhps	 2 * SIZE(%eax, LDC), %xmm0
	movsd	 4 * SIZE(%eax, LDC), %xmm1
	movhps	 6 * SIZE(%eax, LDC), %xmm1

	pshufd	$0x50, %xmm7,  %xmm2
	pshufd	$0xfa, %xmm7,  %xmm7

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm7

	addps	%xmm2, %xmm0
	addps	%xmm7, %xmm1

	movlps	%xmm0,   0 * SIZE(%eax, LDC)
	movhps	%xmm0,   2 * SIZE(%eax, LDC)
	movlps	%xmm1,   4 * SIZE(%eax, LDC)
	movhps	%xmm1,   6 * SIZE(%eax, LDC)

	addl	$8 * SIZE, C1
	decl	I
	jg	.L11
	ALIGN_4

.L20:
	movl	M,  I
	testl	$2, I
	jle	.L30

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

	pxor	%xmm4, %xmm4
	movaps	-32 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movaps	-32 * SIZE(BB), %xmm1
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
	addl	$4, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L25
	ALIGN_4

.L22:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	pshufd	$0x44, %xmm0, %xmm2
	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm4
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	-28 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm5

	pshufd	$0xee, %xmm0, %xmm2
	movaps	-28 * SIZE(AA), %xmm0

	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm6
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	-24 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm7

	pshufd	$0x44, %xmm0, %xmm2
	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm4
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	-20 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm5

	pshufd	$0xee, %xmm0, %xmm2
	movaps	-24 * SIZE(AA), %xmm0

	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm6
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	-16 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm7

	pshufd	$0x44, %xmm0, %xmm2
	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm4
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	-12 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm5

	pshufd	$0xee, %xmm0, %xmm2
	movaps	-20 * SIZE(AA), %xmm0

	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm6
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	 -8 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm7

	pshufd	$0x44, %xmm0, %xmm2
	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm4
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	 -4 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm5

	pshufd	$0xee, %xmm0, %xmm2
	movaps	-16 * SIZE(AA), %xmm0

	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm6
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	  0 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm7

	subl   $-16 * SIZE, AA
	subl   $-32 * SIZE, BB

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
	pshufd	$0x44, %xmm0, %xmm2
	movsd	-30 * SIZE(AA), %xmm0

	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm4
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	-28 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3
	addps	%xmm3, %xmm5

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L26
	ALIGN_4

.L28:
	movddup	ALPHA,  %xmm3

	addps	%xmm6, %xmm4
	addps	%xmm7, %xmm5

	leal		(C1, LDC, 2), %eax

	movsd	 0 * SIZE(C1), %xmm0
	movhps	 2 * SIZE(C1), %xmm0
	movsd	 0 * SIZE(C1, LDC), %xmm1
	movhps	 2 * SIZE(C1, LDC), %xmm1

	pshufd	$0x50, %xmm4,  %xmm2
	pshufd	$0xfa, %xmm4,  %xmm4

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm4

	addps	%xmm2, %xmm0
	addps	%xmm4, %xmm1

	movlps	%xmm0,   0 * SIZE(C1)
	movhps	%xmm0,   2 * SIZE(C1)
	movlps	%xmm1,   0 * SIZE(C1, LDC)
	movhps	%xmm1,   2 * SIZE(C1, LDC)

	movsd	 0 * SIZE(%eax), %xmm0
	movhps	 2 * SIZE(%eax), %xmm0
	movsd	 0 * SIZE(%eax, LDC), %xmm1
	movhps	 2 * SIZE(%eax, LDC), %xmm1

	pshufd	$0x50, %xmm5,  %xmm2
	pshufd	$0xfa, %xmm5,  %xmm5

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm5

	addps	%xmm2, %xmm0
	addps	%xmm5, %xmm1

	movlps	%xmm0,   0 * SIZE(%eax)
	movhps	%xmm0,   2 * SIZE(%eax)
	movlps	%xmm1,   0 * SIZE(%eax, LDC)
	movhps	%xmm1,   2 * SIZE(%eax, LDC)

	addl	$4 * SIZE, C1
	ALIGN_4

.L30:
	movl	M,  I
	testl	$1, I
	jle	.L39

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BB
#else
	movl	B, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	addl	%eax, AA
	leal	(BB, %eax, 4), BB
#endif

	pxor	%xmm4, %xmm4
	movsd	-32 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movaps	-32 * SIZE(BB), %xmm1
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
	addl	$4, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L35
	ALIGN_4

.L32:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	pshufd	$0x00, %xmm0, %xmm2
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movaps	-28 * SIZE(BB), %xmm1

	pshufd	$0x55, %xmm0, %xmm2
	movsd	-30 * SIZE(AA), %xmm0

	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movaps	-24 * SIZE(BB), %xmm1

	pshufd	$0x00, %xmm0, %xmm2
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movaps	-20 * SIZE(BB), %xmm1

	pshufd	$0x55, %xmm0, %xmm2
	movsd	-28 * SIZE(AA), %xmm0

	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movaps	-16 * SIZE(BB), %xmm1

	pshufd	$0x00, %xmm0, %xmm2
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movaps	-12 * SIZE(BB), %xmm1

	pshufd	$0x55, %xmm0, %xmm2
	movsd	-26 * SIZE(AA), %xmm0

	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movaps	 -8 * SIZE(BB), %xmm1

	pshufd	$0x00, %xmm0, %xmm2
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movaps	 -4 * SIZE(BB), %xmm1

	pshufd	$0x55, %xmm0, %xmm2
	movsd	-24 * SIZE(AA), %xmm0

	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movaps	  0 * SIZE(BB), %xmm1

	subl   $ -8 * SIZE, AA
	subl   $-32 * SIZE, BB

	subl   $1, %eax
	jne    .L32
	ALIGN_4

.L35:
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
	pshufd	$0x00, %xmm0, %xmm2
	movss	-31 * SIZE(AA), %xmm0

	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movaps	-28 * SIZE(BB), %xmm1

	addl	$1 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L36
	ALIGN_4

.L38:
	movddup	ALPHA,  %xmm3

	leal		(C1, LDC, 2), %eax

	movsd	 (C1), %xmm0
	movhps	 (C1, LDC), %xmm0
	movsd	 (%eax), %xmm1
	movhps	 (%eax, LDC), %xmm1

	pshufd	$0x50, %xmm4,  %xmm2
	pshufd	$0xfa, %xmm4,  %xmm4

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm4

	addps	%xmm2, %xmm0
	addps	%xmm4, %xmm1

	movlps	%xmm0,   (C1)
	movhps	%xmm0,   (C1, LDC)
	movlps	%xmm1,   (%eax)
	movhps	%xmm1,   (%eax, LDC)
	ALIGN_4

.L39:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$4, KK
#endif

	movl	BB, B

	leal	(, LDC, 4), %eax
	addl	%eax, C
	decl	J
	jg	.L01
	ALIGN_4

.L40:
	movl	N,  %eax
	testl	$2, %eax
	jle	.L70

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	C, C1
	movl	A, AA

	movl	M,  I
	sarl	$2, I
	jle	.L50
	ALIGN_4

.L41:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BB
#else
	movl	B, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
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
	prefetcht0	3 * SIZE(C1, LDC)
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
	addl	$4, %eax
#else
	addl	$2, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L45
	ALIGN_4

.L42:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm5
	pshufd	$0x55, %xmm1, %xmm3
	mulps	%xmm0, %xmm3
	movaps	-28 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm6
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm7
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

	addps	%xmm2, %xmm6
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm7
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

	addps	%xmm2, %xmm6
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm7
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

	addps	%xmm2, %xmm6
	pshufd	$0xaa, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm3, %xmm7
	pshufd	$0xff, %xmm1, %xmm3
	movaps	-16 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm3
	movaps	  0 * SIZE(AA), %xmm0

	subl   $-32 * SIZE, AA
	subl   $-16 * SIZE, BB

	subl   $1, %eax
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
	movaps	-28 * SIZE(AA), %xmm0

	addl	$4 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L46
	ALIGN_4

.L48:
	addps	%xmm2, %xmm4
	addps	%xmm3, %xmm5

	movddup	ALPHA,  %xmm3

	addps	%xmm6, %xmm4
	addps	%xmm7, %xmm5

	movsd	 0 * SIZE(C1), %xmm0
	movhps	 2 * SIZE(C1), %xmm0
	movsd	 4 * SIZE(C1), %xmm1
	movhps	 6 * SIZE(C1), %xmm1

	pshufd	$0x50, %xmm4,  %xmm2
	pshufd	$0xfa, %xmm4,  %xmm4

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm4

	addps	%xmm2, %xmm0
	addps	%xmm4, %xmm1

	movlps	%xmm0,   0 * SIZE(C1)
	movhps	%xmm0,   2 * SIZE(C1)
	movlps	%xmm1,   4 * SIZE(C1)
	movhps	%xmm1,   6 * SIZE(C1)

	movsd	 0 * SIZE(C1, LDC), %xmm0
	movhps	 2 * SIZE(C1, LDC), %xmm0
	movsd	 4 * SIZE(C1, LDC), %xmm1
	movhps	 6 * SIZE(C1, LDC), %xmm1

	pshufd	$0x50, %xmm5,  %xmm2
	pshufd	$0xfa, %xmm5,  %xmm5

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm5

	addps	%xmm2, %xmm0
	addps	%xmm5, %xmm1

	movlps	%xmm0,   0 * SIZE(C1, LDC)
	movhps	%xmm0,   2 * SIZE(C1, LDC)
	movlps	%xmm1,   4 * SIZE(C1, LDC)
	movhps	%xmm1,   6 * SIZE(C1, LDC)

	addl	$8 * SIZE, C1
	decl	I
	jg	.L41
	ALIGN_4

.L50:
	movl	M,  I
	testl	$2, I
	jle	.L60

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

	movaps	-32 * SIZE(AA), %xmm0
	pxor	%xmm3, %xmm3
	movaps	-32 * SIZE(BB), %xmm1
	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5

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
	je	.L55
	ALIGN_4

.L52:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	pshufd	$0x44, %xmm0, %xmm2
	addps	%xmm3, %xmm4
	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3

	pshufd	$0xee, %xmm0, %xmm2
	movaps	-28 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	-28 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3

	pshufd	$0x44, %xmm0, %xmm2
	addps	%xmm3, %xmm4
	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3

	pshufd	$0xee, %xmm0, %xmm2
	movaps	-24 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	-24 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3

	pshufd	$0x44, %xmm0, %xmm2
	addps	%xmm3, %xmm4
	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3

	pshufd	$0xee, %xmm0, %xmm2
	movaps	-20 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	-20 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3

	pshufd	$0x44, %xmm0, %xmm2
	addps	%xmm3, %xmm4
	pshufd	$0x50, %xmm1, %xmm3
	mulps	%xmm2, %xmm3

	pshufd	$0xee, %xmm0, %xmm2
	movaps	-16 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	pshufd	$0xfa, %xmm1, %xmm3
	movaps	-16 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3

	subl   $-16 * SIZE, AA
	subl   $-16 * SIZE, BB

	subl   $1, %eax
	jne    .L52
	ALIGN_4

.L55:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$7, %eax
	BRANCH
	je .L58
	ALIGN_4

.L56:
	pshufd	$0x44, %xmm0, %xmm2
	movsd	-30 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm4
	pshufd	$0x50, %xmm1, %xmm3
	movsd	-30 * SIZE(BB), %xmm1
	mulps	%xmm2, %xmm3

	addl	$2 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L56
	ALIGN_4

.L58:
	addps	%xmm3, %xmm4
	addps	%xmm5, %xmm4

	movddup	ALPHA,  %xmm3

	movsd	 0 * SIZE(C1), %xmm0
	movhps	 2 * SIZE(C1), %xmm0
	movsd	 0 * SIZE(C1, LDC), %xmm1
	movhps	 2 * SIZE(C1, LDC), %xmm1

	pshufd	$0x50, %xmm4,  %xmm2
	pshufd	$0xfa, %xmm4,  %xmm4

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm4

	addps	%xmm2, %xmm0
	addps	%xmm4, %xmm1

	movlps	%xmm0,   0 * SIZE(C1)
	movhps	%xmm0,   2 * SIZE(C1)
	movlps	%xmm1,   0 * SIZE(C1, LDC)
	movhps	%xmm1,   2 * SIZE(C1, LDC)

	addl	$4 * SIZE, C1
	ALIGN_4

.L60:
	movl	M,  I
	testl	$1, I
	jle	.L69

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BB
#else
	movl	B, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	addl	%eax, AA
	leal	(BB, %eax, 2), BB
#endif

	pxor	%xmm4, %xmm4
	movsd	-32 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movsd	-32 * SIZE(BB), %xmm1

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
	je	.L65
	ALIGN_4

.L62:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	pshufd	$0x00, %xmm0, %xmm2
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movsd	-30 * SIZE(BB), %xmm1

	pshufd	$0x55, %xmm0, %xmm2
	movsd	-30 * SIZE(AA), %xmm0
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm5
	movsd	-28 * SIZE(BB), %xmm1

	pshufd	$0x00, %xmm0, %xmm2
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movsd	-26 * SIZE(BB), %xmm1

	pshufd	$0x55, %xmm0, %xmm2
	movsd	-28 * SIZE(AA), %xmm0
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm5
	movsd	-24 * SIZE(BB), %xmm1

	pshufd	$0x00, %xmm0, %xmm2
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movsd	-22 * SIZE(BB), %xmm1

	pshufd	$0x55, %xmm0, %xmm2
	movsd	-26 * SIZE(AA), %xmm0
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm5
	movsd	-20 * SIZE(BB), %xmm1

	pshufd	$0x00, %xmm0, %xmm2
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movsd	-18 * SIZE(BB), %xmm1

	pshufd	$0x55, %xmm0, %xmm2
	movsd	-24 * SIZE(AA), %xmm0
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm5
	movsd	-16 * SIZE(BB), %xmm1

	subl   $ -8 * SIZE, AA
	subl   $-16 * SIZE, BB

	subl   $1, %eax
	jne    .L62
	ALIGN_4

.L65:
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
	pshufd	$0x00, %xmm0, %xmm2
	movss	-31 * SIZE(AA), %xmm0
	mulps	%xmm2, %xmm1
	addps	%xmm1, %xmm4
	movsd	-30 * SIZE(BB), %xmm1

	addl	$1 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L66
	ALIGN_4

.L68:
	movddup	ALPHA,  %xmm3

	addps	%xmm5, %xmm4

	movsd	 (C1), %xmm0
	movhps	 (C1, LDC), %xmm0

	pshufd	$0x50, %xmm4,  %xmm2
	mulps	%xmm3, %xmm2
	addps	%xmm2, %xmm0

	movlps	%xmm0,   (C1)
	movhps	%xmm0,   (C1, LDC)
	ALIGN_4

.L69:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	movl	BB, B

	leal	(, LDC, 2), %eax
	addl	%eax, C
	ALIGN_4

.L70:
	movl	N,  %eax
	testl	$1, %eax
	jle	.L999

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	C, C1
	movl	A, AA

	movl	M,  I
	sarl	$2, I
	jle	.L80
	ALIGN_4

.L71:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BB
#else
	movl	B, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 4), AA
	addl	%eax, BB
#endif

	movaps	-32 * SIZE(AA), %xmm0
	pxor	%xmm2, %xmm2
	movsd	-32 * SIZE(BB), %xmm1

	pxor	%xmm4, %xmm4
	prefetcht0	3 * SIZE(C1)
	pxor	%xmm5, %xmm5

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
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm5
	pshufd	$0x55, %xmm1, %xmm2
	movsd	-30 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-24 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	movaps	-20 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm5
	pshufd	$0x55, %xmm1, %xmm2
	movsd	-28 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-16 * SIZE(AA), %xmm0

	PREFETCH (PREFETCHSIZE + 16) * SIZE(AA)

	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	movaps	-12 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm5
	pshufd	$0x55, %xmm1, %xmm2
	movsd	-26 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	 -8 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	movaps	 -4 * SIZE(AA), %xmm0

	addps	%xmm2, %xmm5
	pshufd	$0x55, %xmm1, %xmm2
	movsd	-24 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	  0 * SIZE(AA), %xmm0

	subl   $-32 * SIZE, AA
	subl   $ -8 * SIZE, BB

	subl   $1, %eax
	jne    .L72
	ALIGN_4

.L75:
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
	addps	%xmm2, %xmm4
	pshufd	$0x00, %xmm1, %xmm2
	movss	-31 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movaps	-28 * SIZE(AA), %xmm0

	addl	$4 * SIZE, AA
	addl	$1 * SIZE, BB
	decl	%eax
	jg	.L76
	ALIGN_4

.L78:
	movddup	ALPHA,  %xmm3

	addps	%xmm2, %xmm4
	addps	%xmm5, %xmm4

	movsd	 0 * SIZE(C1), %xmm0
	movhps	 2 * SIZE(C1), %xmm0
	movsd	 4 * SIZE(C1), %xmm1
	movhps	 6 * SIZE(C1), %xmm1

	pshufd	$0x50, %xmm4,  %xmm2
	pshufd	$0xfa, %xmm4,  %xmm4

	mulps	%xmm3, %xmm2
	mulps	%xmm3, %xmm4

	addps	%xmm2, %xmm0
	addps	%xmm4, %xmm1

	movlps	%xmm0,   0 * SIZE(C1)
	movhps	%xmm0,   2 * SIZE(C1)
	movlps	%xmm1,   4 * SIZE(C1)
	movhps	%xmm1,   6 * SIZE(C1)

	addl	$8 * SIZE, C1
	decl	I
	jg	.L71
	ALIGN_4

.L80:
	movl	M,  I
	testl	$2, I
	jle	.L90

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BB
#else
	movl	B, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	addl	%eax, BB
#endif

	movsd	-32 * SIZE(AA), %xmm0
	pxor	%xmm3, %xmm3
	movsd	-32 * SIZE(BB), %xmm1
	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5

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
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	movsd	-30 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4

	pshufd	$0x55, %xmm1, %xmm2
	movsd	-30 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movsd	-28 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5

	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	movsd	-26 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4

	pshufd	$0x55, %xmm1, %xmm2
	movsd	-28 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movsd	-24 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5

	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	movsd	-22 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4

	pshufd	$0x55, %xmm1, %xmm2
	movsd	-26 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movsd	-20 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5

	pshufd	$0x00, %xmm1, %xmm2
	mulps	%xmm0, %xmm2
	movsd	-18 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4

	pshufd	$0x55, %xmm1, %xmm2
	movsd	-24 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movsd	-16 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5

	subl   $-16 * SIZE, AA
	subl   $ -8 * SIZE, BB

	subl   $1, %eax
	jne    .L82
	ALIGN_4

.L85:
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
	pshufd	$0x00, %xmm1, %xmm2
	movss	-31 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	movsd	-30 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4

	addl	$2 * SIZE, AA
	addl	$1 * SIZE, BB
	decl	%eax
	jg	.L86
	ALIGN_4

.L88:
	movddup	ALPHA,  %xmm3

	addps	%xmm5, %xmm4

	movsd	 0 * SIZE(C1), %xmm0
	movhps	 2 * SIZE(C1), %xmm0

	pshufd	$0x50, %xmm4,  %xmm2
	mulps	%xmm3, %xmm2
	addps	%xmm2, %xmm0

	movlps	%xmm0,   0 * SIZE(C1)
	movhps	%xmm0,   2 * SIZE(C1)

	addl	$4 * SIZE, C1
	ALIGN_4

.L90:
	movl	M,  I
	testl	$1, I
	jle	.L999

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	movl	B, BB
#else
	movl	B, BB
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	addl	%eax, AA
	addl	%eax, BB
#endif

	pxor	%xmm4, %xmm4
	movsd	-32 * SIZE(AA), %xmm0
	pxor	%xmm5, %xmm5
	movsd	-32 * SIZE(BB), %xmm1

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
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	mulps	%xmm0, %xmm1
	movsd	-30 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm4
	movsd	-30 * SIZE(BB), %xmm1

	mulps	%xmm0, %xmm1
	movsd	-28 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm4
	movsd	-28 * SIZE(BB), %xmm1

	mulps	%xmm0, %xmm1
	movsd	-26 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm4
	movsd	-26 * SIZE(BB), %xmm1

	mulps	%xmm0, %xmm1
	movsd	-24 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm4
	movsd	-24 * SIZE(BB), %xmm1

	subl   $-8 * SIZE, AA
	subl   $-8 * SIZE, BB

	subl   $1, %eax
	jne    .L92
	ALIGN_4

.L95:
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
	mulss	%xmm0, %xmm1
	movss	-31 * SIZE(AA), %xmm0
	addss	%xmm1, %xmm4
	movss	-31 * SIZE(BB), %xmm1

	addl	$1 * SIZE, AA
	addl	$1 * SIZE, BB
	decl	%eax
	jg	.L96
	ALIGN_4

.L98:
	movddup	ALPHA,  %xmm3

	haddps	%xmm4, %xmm4

	movsd	 0 * SIZE(C1), %xmm0

	pshufd	$0x50, %xmm4,  %xmm2
	mulps	%xmm3, %xmm2
	addps	%xmm2, %xmm0

	movlps	%xmm0,   0 * SIZE(C1)
	ALIGN_4

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp

	addl	$ARGS, %esp
	ret

	EPILOGUE
