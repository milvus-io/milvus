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
#define OFFSET	40 + STACK + ARGS(%esp)

#define J	 0 + STACK(%esp)
#define BX	 4 + STACK(%esp)
#define KK	 8 + STACK(%esp)
#define KKK	12 + STACK(%esp)

#define PREFETCH_R   (8 * 4)

#define PREFETCHSIZE  (8 * 21 + 4)
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

	subl	$-16 * SIZE, A
	subl	$-16 * SIZE, B

	leal	(, LDC, SIZE), LDC

	movl	N,  %eax
	sarl	$2, %eax
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

	movl	M,  I
	sarl	$1, I
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
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB
#endif

	movl	BX, %eax

	prefetcht2  (PREFETCH_R + 0) * SIZE(%eax)
	prefetcht2  (PREFETCH_R + 8) * SIZE(%eax)

	subl	$-8 * SIZE, BX

	leal		(C1, LDC, 2), %eax

	movaps	-16 * SIZE(AA), %xmm0
	pxor	%xmm2, %xmm2
	movaps	-16 * SIZE(BB), %xmm1
	pxor	%xmm3, %xmm3

	pxor	%xmm4, %xmm4
	prefetcht0	1 * SIZE(C1)
	pxor	%xmm5, %xmm5
	prefetcht0	1 * SIZE(C1, LDC)
	pxor	%xmm6, %xmm6
	prefetcht0	1 * SIZE(%eax)
	pxor	%xmm7, %xmm7
	prefetcht0	1 * SIZE(%eax, LDC)

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
	je	.L15
	ALIGN_4

.L12:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	addpd	%xmm2, %xmm6
	movapd	%xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	movaps	-14 * SIZE(BB), %xmm1
	addpd	%xmm3, %xmm7
	movapd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm1
//	SHUFPD_1 %xmm0, %xmm0
	pshufd	$0x4e, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	movaps	-14 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm5
	movaps	-12 * SIZE(BB), %xmm1

	addpd	%xmm2, %xmm6
	movapd	%xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	movaps	-10 * SIZE(BB), %xmm1
	addpd	%xmm3, %xmm7
	movapd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm1
//	SHUFPD_1 %xmm0, %xmm0
	pshufd	$0x4e, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	movaps	-12 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm5
	movaps	 -8 * SIZE(BB), %xmm1

	addpd	%xmm2, %xmm6
	movapd	%xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	movaps	 -6 * SIZE(BB), %xmm1
	addpd	%xmm3, %xmm7
	movapd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm1
//	SHUFPD_1 %xmm0, %xmm0
	pshufd	$0x4e, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	movaps	-10 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm5
	movaps	 -4 * SIZE(BB), %xmm1

	addpd	%xmm2, %xmm6
	movapd	%xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	movaps	 -2 * SIZE(BB), %xmm1
	addpd	%xmm3, %xmm7
	movapd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm1
//	SHUFPD_1 %xmm0, %xmm0
	pshufd	$0x4e, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	movaps	 -8 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm5
	movaps	  0 * SIZE(BB), %xmm1

	PREFETCH (PREFETCHSIZE +  8) * SIZE(AA)

	addpd	%xmm2, %xmm6
	movapd	%xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	movaps	  2 * SIZE(BB), %xmm1
	addpd	%xmm3, %xmm7
	movapd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm1
//	SHUFPD_1 %xmm0, %xmm0
	pshufd	$0x4e, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	movaps	 -6 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm5
	movaps	  4 * SIZE(BB), %xmm1

	addpd	%xmm2, %xmm6
	movapd	%xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	movaps	  6 * SIZE(BB), %xmm1
	addpd	%xmm3, %xmm7
	movapd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm1
//	SHUFPD_1 %xmm0, %xmm0
	pshufd	$0x4e, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	movaps	 -4 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm5
	movaps	  8 * SIZE(BB), %xmm1

	addpd	%xmm2, %xmm6
	movapd	%xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	movaps	 10 * SIZE(BB), %xmm1
	addpd	%xmm3, %xmm7
	movapd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm1
//	SHUFPD_1 %xmm0, %xmm0
	pshufd	$0x4e, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	movaps	 -2 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm5
	movaps	 12 * SIZE(BB), %xmm1

	addpd	%xmm2, %xmm6
	movapd	%xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	movaps	 14 * SIZE(BB), %xmm1
	addpd	%xmm3, %xmm7
	movapd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm1
//	SHUFPD_1 %xmm0, %xmm0
	pshufd	$0x4e, %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	movaps	  0 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm5
	movaps	 16 * SIZE(BB), %xmm1

	subl   $-32 * SIZE, BB
	subl   $-16 * SIZE, AA

	subl   $1, %eax
	BRANCH
	jne .L12
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
	addpd	%xmm2, %xmm6
	movapd	%xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm4
	movaps	-14 * SIZE(BB), %xmm1
	addpd	%xmm3, %xmm7
	movapd	%xmm1, %xmm3
	mulpd	%xmm0, %xmm1
	SHUFPD_1 %xmm0, %xmm0
	mulpd	%xmm0, %xmm2
	mulpd	%xmm0, %xmm3
	movaps	-14 * SIZE(AA), %xmm0
	addpd	%xmm1, %xmm5
	movaps	-12 * SIZE(BB), %xmm1

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L16
	ALIGN_4

.L18:
	addpd	%xmm2, %xmm6
	addpd	%xmm3, %xmm7


	movddup	ALPHA,  %xmm3

	movaps	 %xmm4, %xmm0
	unpcklpd %xmm6, %xmm4
	unpckhpd %xmm0, %xmm6

	movaps	 %xmm5, %xmm1
	unpcklpd %xmm7, %xmm5
	unpckhpd %xmm1, %xmm7

	mulpd	%xmm3, %xmm4
	mulpd	%xmm3, %xmm5
	mulpd	%xmm3, %xmm6
	mulpd	%xmm3, %xmm7

	leal		(C1, LDC, 2), %eax

#ifndef TRMMKERNEL
	movsd	0 * SIZE(C1), %xmm0
	movhpd	1 * SIZE(C1), %xmm0
	movsd	0 * SIZE(C1, LDC), %xmm1
	movhpd	1 * SIZE(C1, LDC), %xmm1

	movsd	0 * SIZE(%eax), %xmm2
	movhpd	1 * SIZE(%eax), %xmm2
	movsd	0 * SIZE(%eax, LDC), %xmm3
	movhpd	1 * SIZE(%eax, LDC), %xmm3

	addpd	%xmm0, %xmm4
	addpd	%xmm1, %xmm6
	addpd	%xmm2, %xmm5
	addpd	%xmm3, %xmm7
#endif

	movsd	%xmm4, 0 * SIZE(C1)
	movhpd	%xmm4, 1 * SIZE(C1)
	movsd	%xmm6, 0 * SIZE(C1, LDC)
	movhpd	%xmm6, 1 * SIZE(C1, LDC)

	movsd	%xmm5, 0 * SIZE(%eax)
	movhpd	%xmm5, 1 * SIZE(%eax)
	movsd	%xmm7, 0 * SIZE(%eax, LDC)
	movhpd	%xmm7, 1 * SIZE(%eax, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif

	addl	$2 * SIZE, C1
	decl	I
	jg	.L11
	ALIGN_4

.L20:
	movl	M,  I
	testl	$1, I
	jle	.L29

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

	movaps	-16 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movaps	-16 * SIZE(BB), %xmm2
	pxor	%xmm5, %xmm5
	movaps	-14 * SIZE(BB), %xmm3
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
	je	.L25
	ALIGN_4

.L22:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	pshufd	$0x44, %xmm0, %xmm1
	mulpd	%xmm1, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm2, %xmm4
	movaps	-12 * SIZE(BB), %xmm2
	addpd	%xmm3, %xmm5
	movaps	-10 * SIZE(BB), %xmm3

	pshufd	$0xee, %xmm0, %xmm1
	movaps	-14 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm2, %xmm6
	movaps	 -8 * SIZE(BB), %xmm2
	addpd	%xmm3, %xmm7
	movaps	 -6 * SIZE(BB), %xmm3

	pshufd	$0x44, %xmm0, %xmm1
	mulpd	%xmm1, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm2, %xmm4
	movaps	 -4 * SIZE(BB), %xmm2
	addpd	%xmm3, %xmm5
	movaps	 -2 * SIZE(BB), %xmm3

	pshufd	$0xee, %xmm0, %xmm1
	movaps	-12 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm2, %xmm6
	movaps	  0 * SIZE(BB), %xmm2
	addpd	%xmm3, %xmm7
	movaps	  2 * SIZE(BB), %xmm3

	pshufd	$0x44, %xmm0, %xmm1
	mulpd	%xmm1, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm2, %xmm4
	movaps	  4 * SIZE(BB), %xmm2
	addpd	%xmm3, %xmm5
	movaps	  6 * SIZE(BB), %xmm3

	pshufd	$0xee, %xmm0, %xmm1
	movaps	-10 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm2, %xmm6
	movaps	  8 * SIZE(BB), %xmm2
	addpd	%xmm3, %xmm7
	movaps	 10 * SIZE(BB), %xmm3

	pshufd	$0x44, %xmm0, %xmm1
	mulpd	%xmm1, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm2, %xmm4
	movaps	 12 * SIZE(BB), %xmm2
	addpd	%xmm3, %xmm5
	movaps	 14 * SIZE(BB), %xmm3

	pshufd	$0xee, %xmm0, %xmm1
	movaps	 -8 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm2, %xmm6
	movaps	 16 * SIZE(BB), %xmm2
	addpd	%xmm3, %xmm7
	movaps	 18 * SIZE(BB), %xmm3

	subl   $ -8 * SIZE, AA
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
	pshufd	$0x44, %xmm0, %xmm1
	movsd	-15 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2
	mulpd	%xmm1, %xmm3

	addpd	%xmm2, %xmm4
	movaps	-12 * SIZE(BB), %xmm2
	addpd	%xmm3, %xmm5
	movaps	-10 * SIZE(BB), %xmm3

	addl	$1 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L26
	ALIGN_4

.L28:
	movddup	ALPHA,  %xmm3

	addpd	%xmm6, %xmm4
	addpd	%xmm7, %xmm5

	leal		(C1, LDC, 2), %eax

#ifndef TRMMKERNEL
	movsd	0 * SIZE(C1), %xmm0
	movhpd	0 * SIZE(C1, LDC), %xmm0

	movsd	0 * SIZE(%eax), %xmm1
	movhpd	0 * SIZE(%eax, LDC), %xmm1
#endif

	mulpd	%xmm3, %xmm4
	mulpd	%xmm3, %xmm5

#ifndef TRMMKERNEL
	addpd	%xmm0, %xmm4
	addpd	%xmm1, %xmm5
#endif

	movsd	%xmm4, 0 * SIZE(C1)
	movhpd	%xmm4, 0 * SIZE(C1, LDC)

	movsd	%xmm5, 0 * SIZE(%eax)
	movhpd	%xmm5, 0 * SIZE(%eax, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	addl	%eax, AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif
	ALIGN_4

.L29:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$4, KK
#endif

	movl	BB, B

	leal	(, LDC, 4), %eax
	addl	%eax, C
	decl	J
	jg	.L01
	ALIGN_4

.L30:
	movl	N,  %eax
	testl	$2, %eax
	jle	.L50

#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	C, C1
	movl	A, AA

	movl	M,  I
	sarl	$1, I
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
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 2), BB
#endif

	movaps	-16 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movaps	-16 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
	prefetcht0	1 * SIZE(C1)
	pxor	%xmm6, %xmm6
	prefetcht0	1 * SIZE(C1, LDC)
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
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-14 * SIZE(AA), %xmm0

	addpd	%xmm1, %xmm5
	movaps	-14 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-12 * SIZE(AA), %xmm0

	addpd	%xmm1, %xmm7
	movaps	-12 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm6

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-10 * SIZE(AA), %xmm0

	addpd	%xmm1, %xmm5
	movaps	-10 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	 -8 * SIZE(AA), %xmm0

	addpd	%xmm1, %xmm7
	movaps	 -8 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm6

	PREFETCH (PREFETCHSIZE +  8) * SIZE(AA)

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	 -6 * SIZE(AA), %xmm0

	addpd	%xmm1, %xmm5
	movaps	 -6 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	 -4 * SIZE(AA), %xmm0

	addpd	%xmm1, %xmm7
	movaps	 -4 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm6

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	 -2 * SIZE(AA), %xmm0

	addpd	%xmm1, %xmm5
	movaps	 -2 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4

	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	  0 * SIZE(AA), %xmm0

	addpd	%xmm1, %xmm7
	movaps	  0 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm6

	subl   $-16 * SIZE, AA
	subl   $-16 * SIZE, BB

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
	pshufd	$0x4e, %xmm1, %xmm2
	mulpd	%xmm0, %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-14 * SIZE(AA), %xmm0

	addpd	%xmm1, %xmm5
	movaps	-14 * SIZE(BB), %xmm1
	addpd	%xmm2, %xmm4

	addl	$2 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L36
	ALIGN_4

.L38:
	movddup	ALPHA,  %xmm3

	addpd	%xmm6, %xmm4
	addpd	%xmm7, %xmm5

	movaps	 %xmm4, %xmm0
	movsd    %xmm5, %xmm4
	mulpd	%xmm3, %xmm4
	movsd    %xmm0, %xmm5
	mulpd	%xmm3, %xmm5

#ifndef TRMMKERNEL
	movsd	0 * SIZE(C1), %xmm0
	movhpd	1 * SIZE(C1), %xmm0
	movsd	0 * SIZE(C1, LDC), %xmm1
	movhpd	1 * SIZE(C1, LDC), %xmm1

	addpd	%xmm0, %xmm4
	addpd	%xmm1, %xmm5
#endif

	movsd	%xmm4, 0 * SIZE(C1)
	movhpd	%xmm4, 1 * SIZE(C1)
	movsd	%xmm5, 0 * SIZE(C1, LDC)
	movhpd	%xmm5, 1 * SIZE(C1, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 2), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif

	addl	$2 * SIZE, C1
	decl	I
	jg	.L31
	ALIGN_4

.L40:
	movl	M,  I
	testl	$1, I
	jle	.L49

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

	movaps	-16 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movaps	-16 * SIZE(BB), %xmm2
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
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	pshufd	$0x44, %xmm0, %xmm1
	mulpd	%xmm1, %xmm2

	addpd	%xmm2, %xmm4
	movaps	-14 * SIZE(BB), %xmm2

	pshufd	$0xee, %xmm0, %xmm1
	movaps	-14 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2

	addpd	%xmm2, %xmm5
	movaps	-12 * SIZE(BB), %xmm2

	pshufd	$0x44, %xmm0, %xmm1
	mulpd	%xmm1, %xmm2

	addpd	%xmm2, %xmm4
	movaps	-10 * SIZE(BB), %xmm2

	pshufd	$0xee, %xmm0, %xmm1
	movaps	-12 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2

	addpd	%xmm2, %xmm5
	movaps	 -8 * SIZE(BB), %xmm2

	pshufd	$0x44, %xmm0, %xmm1
	mulpd	%xmm1, %xmm2

	addpd	%xmm2, %xmm4
	movaps	 -6 * SIZE(BB), %xmm2

	pshufd	$0xee, %xmm0, %xmm1
	movaps	-10 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2

	addpd	%xmm2, %xmm5
	movaps	 -4 * SIZE(BB), %xmm2

	pshufd	$0x44, %xmm0, %xmm1
	mulpd	%xmm1, %xmm2

	addpd	%xmm2, %xmm4
	movaps	 -2 * SIZE(BB), %xmm2

	pshufd	$0xee, %xmm0, %xmm1
	movaps	 -8 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2

	addpd	%xmm2, %xmm5
	movaps	 0 * SIZE(BB), %xmm2

	subl   $ -8 * SIZE, AA
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
	pshufd	$0x44, %xmm0, %xmm1
	movsd	-15 * SIZE(AA), %xmm0
	mulpd	%xmm1, %xmm2

	addpd	%xmm2, %xmm4
	movaps	-14 * SIZE(BB), %xmm2

	addl	$1 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L46
	ALIGN_4

.L48:
	movddup	ALPHA,  %xmm3

	addpd	%xmm5, %xmm4

#ifndef TRMMKERNEL
	movsd	0 * SIZE(C1), %xmm0
	movhpd	0 * SIZE(C1, LDC), %xmm0
#endif

	mulpd	%xmm3, %xmm4

#ifndef TRMMKERNEL
	addpd	%xmm0, %xmm4
#endif

	movsd	%xmm4, 0 * SIZE(C1)
	movhpd	%xmm4, 0 * SIZE(C1, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, SIZE), %eax
	addl	%eax, AA
	leal	(BB, %eax, 2), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif
	ALIGN_4

.L49:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	movl	BB, B

	leal	(, LDC, 2), %eax
	addl	%eax, C
	ALIGN_4

.L50:
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
	sarl	$1, I
	jle	.L60
	ALIGN_4

.L51:
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

	movaps	-16 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movaps	-16 * SIZE(BB), %xmm1
	pxor	%xmm5, %xmm5
	prefetcht0	1 * SIZE(C1)

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
	je	.L55
	ALIGN_4

.L52:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	pshufd	$0x44, %xmm1, %xmm2
	mulpd	%xmm0, %xmm2
	movaps	-14 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm4

	pshufd	$0xee, %xmm1, %xmm2
	movaps	-14 * SIZE(BB), %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-12 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm5

	pshufd	$0x44, %xmm1, %xmm2
	mulpd	%xmm0, %xmm2
	movaps	-10 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm4

	pshufd	$0xee, %xmm1, %xmm2
	movaps	-12 * SIZE(BB), %xmm1
	mulpd	%xmm0, %xmm2
	movaps	 -8 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm5

	PREFETCH (PREFETCHSIZE +  8) * SIZE(AA)

	pshufd	$0x44, %xmm1, %xmm2
	mulpd	%xmm0, %xmm2
	movaps	 -6 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm4

	pshufd	$0xee, %xmm1, %xmm2
	movaps	-10 * SIZE(BB), %xmm1
	mulpd	%xmm0, %xmm2
	movaps	 -4 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm5

	pshufd	$0x44, %xmm1, %xmm2
	mulpd	%xmm0, %xmm2
	movaps	 -2 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm4

	pshufd	$0xee, %xmm1, %xmm2
	movaps	 -8 * SIZE(BB), %xmm1
	mulpd	%xmm0, %xmm2
	movaps	  0 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm5

	subl   $-16 * SIZE, AA
	subl   $ -8 * SIZE, BB

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
	pshufd	$0x44, %xmm1, %xmm2
	movsd	-15 * SIZE(BB), %xmm1
	mulpd	%xmm0, %xmm2
	movaps	-14 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm4

	addl	$2 * SIZE, AA
	addl	$1 * SIZE, BB
	decl	%eax
	jg	.L56
	ALIGN_4

.L58:
	movddup	ALPHA,  %xmm3

	addpd	%xmm5, %xmm4
	mulpd	%xmm3, %xmm4

#ifndef TRMMKERNEL
	movsd	0 * SIZE(C1), %xmm0
	movhpd	1 * SIZE(C1), %xmm0

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
	addl	%eax, BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif

	addl	$2 * SIZE, C1
	decl	I
	jg	.L51
	ALIGN_4

.L60:
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

	movaps	-16 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movaps	-16 * SIZE(BB), %xmm2
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
	addl	$1, %eax
#else
	addl	$1, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L65
	ALIGN_4

.L62:
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AA)

	mulpd	%xmm0, %xmm2
	movaps	-14 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm4
	movaps	-14 * SIZE(BB), %xmm2

	mulpd	%xmm0, %xmm2
	movaps	-12 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm5
	movaps	-12 * SIZE(BB), %xmm2

	mulpd	%xmm0, %xmm2
	movaps	-10 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm4
	movaps	-10 * SIZE(BB), %xmm2

	mulpd	%xmm0, %xmm2
	movaps	 -8 * SIZE(AA), %xmm0
	addpd	%xmm2, %xmm5
	movaps	 -8 * SIZE(BB), %xmm2

	subl   $-8 * SIZE, AA
	subl   $-8 * SIZE, BB

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
	mulsd	%xmm0, %xmm2
	movsd	-15 * SIZE(AA), %xmm0
	addsd	%xmm2, %xmm4
	movsd	-15 * SIZE(BB), %xmm2

	addl	$1 * SIZE, AA
	addl	$1 * SIZE, BB
	decl	%eax
	jg	.L66
	ALIGN_4

.L68:
	movddup	ALPHA,  %xmm3

	addpd	%xmm5, %xmm4

	haddpd	%xmm4, %xmm4

#ifndef TRMMKERNEL
	movsd	0 * SIZE(C1), %xmm0
#endif

	mulsd	%xmm3, %xmm4

#ifndef TRMMKERNEL
	addsd	%xmm0, %xmm4
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
