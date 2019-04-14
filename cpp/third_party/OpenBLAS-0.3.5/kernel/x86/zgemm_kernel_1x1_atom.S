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

#define PREFETCH	prefetcht0
#define PREFETCHSIZE 84

#define AA	%edx
#define BB	%ecx
#define CO1	%esi
#define LDC	%ebp
#define B	%edi

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define ADDSD1	addsd
#define ADDSD2	addsd
#define ADDSD3	addsd
#define ADDSD4	subsd
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define ADDSD1	addsd
#define ADDSD2	subsd
#define ADDSD3	addsd
#define ADDSD4	addsd
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define ADDSD1	addsd
#define ADDSD2	addsd
#define ADDSD3	subsd
#define ADDSD4	addsd
#else
#define ADDSD1	addsd
#define ADDSD2	subsd
#define ADDSD3	subsd
#define ADDSD4	subsd
#endif

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

	movl	M,    %ebx
	testl	%ebx, %ebx
	jle	.L999

	movl	N,    %eax
	testl	%eax, %eax
	movl	%eax, J
	jle	.L999
	ALIGN_4

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	B, BX

	movl	C, CO1
	addl	LDC, C

	movl	A, AA

	movl	M,    %ebx
	ALIGN_4

.L10:
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
	prefetcht0	1 * SIZE(CO1)
	xorps	%xmm5, %xmm5
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
	addl	$1, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$2, %eax
	je	.L15
	ALIGN_4

.L12:
	PREFETCH  (PREFETCHSIZE + 0) * SIZE(AA)

	ADDSD3	%xmm2, %xmm6
	movsd	 1 * SIZE(AA), %xmm2
	movaps	%xmm0, %xmm1
	mulsd	 0 * SIZE(BB), %xmm0
	ADDSD4	%xmm3, %xmm7
	mulsd	 1 * SIZE(BB), %xmm1

	ADDSD1	%xmm0, %xmm4
	movsd	 2 * SIZE(AA), %xmm0
	movaps	%xmm2, %xmm3
	mulsd	 0 * SIZE(BB), %xmm2
	ADDSD2	%xmm1, %xmm5
	mulsd	 1 * SIZE(BB), %xmm3

	ADDSD3	%xmm2, %xmm6
	movsd	 3 * SIZE(AA), %xmm2
	movaps	%xmm0, %xmm1
	mulsd	 2 * SIZE(BB), %xmm0
	ADDSD4	%xmm3, %xmm7
	mulsd	 3 * SIZE(BB), %xmm1

	ADDSD1	%xmm0, %xmm4
	movsd	 4 * SIZE(AA), %xmm0
	movaps	%xmm2, %xmm3
	mulsd	 2 * SIZE(BB), %xmm2
	ADDSD2	%xmm1, %xmm5
	mulsd	 3 * SIZE(BB), %xmm3

	ADDSD3	%xmm2, %xmm6
	movsd	 5 * SIZE(AA), %xmm2
	movaps	%xmm0, %xmm1
	mulsd	 4 * SIZE(BB), %xmm0
	ADDSD4	%xmm3, %xmm7
	mulsd	 5 * SIZE(BB), %xmm1

	ADDSD1	%xmm0, %xmm4
	movsd	 6 * SIZE(AA), %xmm0
	movaps	%xmm2, %xmm3
	mulsd	 4 * SIZE(BB), %xmm2
	ADDSD2	%xmm1, %xmm5
	mulsd	 5 * SIZE(BB), %xmm3

	ADDSD3	%xmm2, %xmm6
	movsd	 7 * SIZE(AA), %xmm2
	movaps	%xmm0, %xmm1
	mulsd	 6 * SIZE(BB), %xmm0
	ADDSD4	%xmm3, %xmm7
	mulsd	 7 * SIZE(BB), %xmm1

	ADDSD1	%xmm0, %xmm4
	movsd	 8 * SIZE(AA), %xmm0
	movaps	%xmm2, %xmm3
	mulsd	 6 * SIZE(BB), %xmm2
	ADDSD2	%xmm1, %xmm5
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
	ADDSD3	%xmm2, %xmm6
	movsd	 1 * SIZE(AA), %xmm2
	movaps	%xmm0, %xmm1
	mulsd	 0 * SIZE(BB), %xmm0
	ADDSD4	%xmm3, %xmm7
	mulsd	 1 * SIZE(BB), %xmm1

	ADDSD1	%xmm0, %xmm4
	movsd	 2 * SIZE(AA), %xmm0
	movaps	%xmm2, %xmm3
	mulsd	 0 * SIZE(BB), %xmm2
	ADDSD2	%xmm1, %xmm5
	mulsd	 1 * SIZE(BB), %xmm3

	addl	$2 * SIZE, AA
	addl	$2 * SIZE, BB
	decl	%eax
	jg	.L16
	ALIGN_4

.L18:
	movsd	ALPHA_R, %xmm0
	movsd	ALPHA_I, %xmm1

	ADDSD3	%xmm2, %xmm6
	ADDSD4	%xmm3, %xmm7

	addsd	%xmm7, %xmm4
	addsd	%xmm5, %xmm6

	movaps	%xmm4, %xmm5
	movaps	%xmm6, %xmm7

	mulsd	%xmm0, %xmm4
	mulsd	%xmm1, %xmm5
	mulsd	%xmm1, %xmm6
	mulsd	%xmm0, %xmm7

	subsd	%xmm6, %xmm4
	addsd	%xmm7, %xmm5

#ifndef TRMMKERNEL
	addsd	0 * SIZE(CO1), %xmm4
	addsd	1 * SIZE(CO1), %xmm5
#endif

	movsd	%xmm4, 0 * SIZE(CO1)
	movsd	%xmm5, 1 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(, %eax, SIZE), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 2), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif

	addl	$2 * SIZE, CO1
	decl	%ebx
	jg	.L10
	ALIGN_4

.L99:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$1, KK
#endif

	movl	BB, B
	decl	J
	jg	.L01
	ALIGN_4

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp

	addl	$ARGS, %esp
	ret

	EPILOGUE
