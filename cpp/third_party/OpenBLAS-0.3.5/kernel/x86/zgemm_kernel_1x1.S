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

#define BX	 0 + STACK(%esp)
#define KK	 4 + STACK(%esp)
#define KKK	 8 + STACK(%esp)

#define STACK_M	 4 + STACK + ARGS(%esp)
#define STACK_N	 8 + STACK + ARGS(%esp)
#define STACK_K	12 + STACK + ARGS(%esp)
#ifdef DOUBLE
#define ALPHA_R	16 + STACK + ARGS(%esp)
#define ALPHA_I	24 + STACK + ARGS(%esp)
#define STACK_A	32 + STACK + ARGS(%esp)
#define STACK_B	36 + STACK + ARGS(%esp)
#define STACK_C	40 + STACK + ARGS(%esp)
#define STACK_LDC	44 + STACK + ARGS(%esp)
#define OFFSET	48 + STACK + ARGS(%esp)
#else
#define ALPHA_R	16 + STACK + ARGS(%esp)
#define ALPHA_I	20 + STACK + ARGS(%esp)
#define STACK_A	24 + STACK + ARGS(%esp)
#define STACK_B	28 + STACK + ARGS(%esp)
#define STACK_C	32 + STACK + ARGS(%esp)
#define STACK_LDC	36 + STACK + ARGS(%esp)
#define OFFSET	40 + STACK + ARGS(%esp)
#endif

	PROLOGUE

	subl	$ARGS, %esp
	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

#define M	%esi
#define K	%edi

#define A	%ebx
#define B	%ecx
#define C	%edx
#define LDC	%ebp

#if defined(TRMMKERNEL) && !defined(LEFT)
	movl	OFFSET, %eax
	negl	%eax
	movl	%eax, KK
#endif

	movl	STACK_K,   K
	movl	STACK_LDC, LDC
	sall	$ZBASE_SHIFT,  LDC

	cmpl	$0, STACK_N
	jle	.L29
	cmpl	$0, STACK_M
	jle	.L29
	ALIGN_4

.L30:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	%ebx, BX

	movl	STACK_A, A
	movl	STACK_C, C
	movl	STACK_M, M
	ALIGN_4

.L34:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	STACK_B, B
#else
	movl	STACK_B, B
	movl	KK,   %eax
	leal	(, %eax, SIZE), %eax
	leal	(A, %eax, 2), A
	leal	(B, %eax, 2), B
#endif

#ifdef HAVE_SSE
	movl	BX, %eax

	prefetcht2  0 * SIZE(%eax)
	prefetcht2  4 * SIZE(%eax)

#if   L2_SIZE > 262144

	subl	$-8 * SIZE, BX

#elif L2_SIZE > 131072

	prefetcht2  8 * SIZE(%eax)
	prefetcht2 12 * SIZE(%eax)


	subl	$-16 * SIZE, BX
#else
	prefetcht2 16 * SIZE(%eax)
	prefetcht2 20 * SIZE(%eax)
	prefetcht2 24 * SIZE(%eax)
	prefetcht2 28 * SIZE(%eax)

	subl	$-32 * SIZE, BX
#endif
#endif

	fldz
	fldz
	fldz
	fldz

	FLD	4 * SIZE(B)		# B5
	FLD	4 * SIZE(A)		# A5
	FLD	0 * SIZE(B)		# B0
	FLD	0 * SIZE(A)		# A0

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

#ifdef HAVE_SSE
	prefetcht2	2 * SIZE(C)
#endif
	sarl	$2,   %eax
	je	.L37
	ALIGN_4

#define PREFETCH_OFFSET 40

.L38:
#ifdef HAVE_SSE
	prefetchnta	(PREFETCH_OFFSET) * SIZE(B)
#ifdef CORE_KATMAI
	prefetcht0	(PREFETCH_OFFSET) * SIZE(A)
#endif
#endif
	fmul	%st, %st(1)
	FMUL	 1 * SIZE(B)
	fxch	%st(1)
	faddp	%st, %st(5)
	FLD	 0 * SIZE(B)
	fxch	%st(1)
#if defined(NN) || defined(CN)
	faddp	%st, %st(4)
#else
	fsubrp	%st, %st(4)
#endif
	FLD	 1 * SIZE(A)
	fmul	%st, %st(1)
	FMUL	 1 * SIZE(B)
	fxch	%st(1)
#if defined(NN) || defined(NC)
	faddp	%st, %st(7)
#else
	fsubrp	%st, %st(7)
#endif
	FLD	 2 * SIZE(B)
	fxch	%st(1)
#if defined(NN) || defined(CC)
	fsubrp	%st, %st(6)
#else
	faddp	%st, %st(6)
#endif
	FLD	 2 * SIZE(A)

	fmul	%st, %st(1)
	FMUL	 3 * SIZE(B)
	fxch	%st(1)
	faddp	%st, %st(5)
	FLD	 2 * SIZE(B)
	fxch	%st(1)
#if defined(NN) || defined(CN)
	faddp	%st, %st(4)
#else
	fsubrp	%st, %st(4)
#endif
	FLD	 3 * SIZE(A)
	fmul	%st, %st(1)
	FMUL	 3 * SIZE(B)
	fxch	%st(1)
#if defined(NN) || defined(NC)
	faddp	%st, %st(7)
#else
	fsubrp	%st, %st(7)
#endif
	FLD	 8 * SIZE(B)
	fxch	%st(1)
#if defined(NN) || defined(CC)
	fsubrp	%st, %st(6)
#else
	faddp	%st, %st(6)
#endif
	FLD	 8 * SIZE(A)
	fxch	%st(2)

#ifdef HAVE_SSE
#ifdef DOUBLE
	prefetchnta	(PREFETCH_OFFSET + 4) * SIZE(B)
#ifdef CORE_KATMAI
	prefetcht0	(PREFETCH_OFFSET + 4) * SIZE(A)
#endif
#endif
#endif

	fmul	%st, %st(3)
	FMUL	 5 * SIZE(B)
	fxch	%st(3)
	faddp	%st, %st(5)
	FLD	 4 * SIZE(B)
	fxch	%st(3)
#if defined(NN) || defined(CN)
	faddp	%st, %st(4)
#else
	fsubrp	%st, %st(4)
#endif
	FLD	 5 * SIZE(A)
	fmul	%st, %st(3)
	FMUL	 5 * SIZE(B)
	fxch	%st(3)
#if defined(NN) || defined(NC)
	faddp	%st, %st(7)
#else
	fsubrp	%st, %st(7)
#endif
	FLD	 6 * SIZE(B)
	fxch	%st(3)
#if defined(NN) || defined(CC)
	fsubrp	%st, %st(6)
#else
	faddp	%st, %st(6)
#endif
	FLD	 6 * SIZE(A)

	fmul	%st, %st(3)
	FMUL	 7 * SIZE(B)
	fxch	%st(3)
	faddp	%st, %st(5)
	FLD	 6 * SIZE(B)
	fxch	%st(3)
#if defined(NN) || defined(CN)
	faddp	%st, %st(4)
#else
	fsubrp	%st, %st(4)
#endif
	FLD	 7 * SIZE(A)
	fmul	%st, %st(3)
	FMUL	 7 * SIZE(B)
	fxch	%st(3)
#if defined(NN) || defined(NC)
	faddp	%st, %st(7)
#else
	fsubrp	%st, %st(7)
#endif
	FLD	12 * SIZE(B)
	fxch	%st(3)
#if defined(NN) || defined(CC)
	fsubrp	%st, %st(6)
#else
	faddp	%st, %st(6)
#endif
	FLD	12 * SIZE(A)
	fxch	%st(2)

	subl	$-8 * SIZE, B
	subl	$-8 * SIZE, A
	decl	%eax
	jg	.L38
	ALIGN_4

.L37:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	andl	$3,  %eax
	jle	.L43
	ALIGN_2

.L54:
	fmul	%st, %st(1)
	FMUL	 1 * SIZE(B)
	fxch	%st(1)
	faddp	%st, %st(5)

	FLD	 0 * SIZE(B)
	fxch	%st(1)
#if defined(NN) || defined(CN)
	faddp	%st, %st(4)
#else
	fsubrp	%st, %st(4)
#endif

	FLD	 1 * SIZE(A)
	fmul	%st, %st(1)
	FMUL	 1 * SIZE(B)
	fxch	%st(1)
#if defined(NN) || defined(NC)
	faddp	%st, %st(7)
#else
	fsubrp	%st, %st(7)
#endif
	FLD	 2 * SIZE(B)
	fxch	%st(1)
#if defined(NN) || defined(CC)
	fsubrp	%st, %st(6)
#else
	faddp	%st, %st(6)
#endif
	FLD	 2 * SIZE(A)

	addl	$2 * SIZE, A
	addl	$2 * SIZE, B
	decl	%eax
	jg	.L54
	ALIGN_3

.L43:
	ffreep	%st(0)
	ffreep	%st(0)
	ffreep	%st(0)
	ffreep	%st(0)

	FLD	ALPHA_R
	fxch	%st(3)
	FLD	ALPHA_I
	fxch	%st(5)

	faddp	%st, %st(2)		# ctemp3 += ctemp4
	faddp	%st, %st(2)		# ctemp1 += ctemp2

	fld	%st(0)			# copy ctemp2
	fmul	%st(4), %st		# ctemp3 *= alpha_i
	fld	%st(2)			# copy ctemp1
	fmul	%st(4), %st		# ctemp1 *= alpha_r
	fsubp	%st, %st(1)		# ctemp2 -= ctemp4

#ifndef TRMMKERNEL
	FADD	0 * SIZE(C)
#endif
	FST	0 * SIZE(C)

	fmulp	%st, %st(2)		# ctemp3 *= alpha_i
	fmulp	%st, %st(2)		# ctemp1 *= alpha_r
	faddp	%st, %st(1)		# ctemp1 += ctemp3

#ifndef TRMMKERNEL
	FADD	1 * SIZE(C)
#endif
	FST	1 * SIZE(C)
	addl	$2 * SIZE, C

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK,  %eax
	leal	(,%eax, SIZE), %eax
	leal	(A, %eax, 2), A
	leal	(B, %eax, 2), B
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif
	decl	M
	jg	.L34
	ALIGN_2

.L33:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$1, KK
#endif

	movl	B, STACK_B
	addl	LDC, STACK_C
	decl	STACK_N
	jg	.L30
	ALIGN_2

.L29:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	addl	$ARGS, %esp
	ret

	EPILOGUE
