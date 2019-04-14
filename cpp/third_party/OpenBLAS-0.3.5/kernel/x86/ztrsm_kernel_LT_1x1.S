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

#define KK	 0 + STACK(%esp)
#define KKK	 4 + STACK(%esp)
#define AORIG	 8 + STACK(%esp)

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

	movl	STACK_K,   K
	movl	STACK_LDC, LDC
	sall	$ZBASE_SHIFT,  LDC

#ifdef LN
       movl	STACK_M, %eax
       sall	$ZBASE_SHIFT, %eax
       addl	%eax, STACK_C
       imull	K, %eax
       addl	%eax, STACK_A
#endif

#ifdef RT
       movl	STACK_N, %eax
       sall	$ZBASE_SHIFT, %eax
       imull	K, %eax
       addl	%eax, STACK_B

       movl	STACK_N, %eax
       imull	LDC, %eax
       addl	%eax, STACK_C
#endif

#ifdef RN
       movl	OFFSET, %eax
       negl	%eax
       movl	%eax, KK
#endif

#ifdef RT
       movl	STACK_N, %eax
       subl	OFFSET, %eax
       movl	%eax, KK
#endif

	cmpl	$0, STACK_N
	jle	.L29
	cmpl	$0, STACK_M
	jle	.L29
	ALIGN_4

.L30:
#if defined(LT) || defined(RN)
	movl	STACK_A, A
#else
	movl	STACK_A, %eax
	movl	%eax, AORIG
#endif

#ifdef RT
	movl	K, %eax
	sall	$ZBASE_SHIFT, %eax
	subl	%eax, STACK_B
#endif

#ifdef RT
	subl	LDC, STACK_C
#endif
	movl	STACK_C, C
#ifndef RT
	addl	LDC, STACK_C
#endif

	movl	STACK_M, M

#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

#ifdef LT
	movl	OFFSET, %eax
	movl	%eax, KK
#endif
	ALIGN_4

.L34:
#ifdef LN
       movl	K, %eax
       sall	$ZBASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
	movl	AORIG, A
	movl	STACK_B, B
	addl	%eax,  A
	addl	%eax,  B
#else
	movl	STACK_B, B
#endif

	fldz
	fldz
	fldz
	fldz

	FLD	4 * SIZE(B)		# B5
	FLD	4 * SIZE(A)		# A5
	FLD	0 * SIZE(B)		# B0
	FLD	0 * SIZE(A)		# A0

#ifdef HAVE_SSE
	prefetcht2	2 * SIZE(C)
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	sarl	$2,   %eax
	je	.L37
	ALIGN_4

#define PREFETCH_OFFSET 40

.L38:
#ifdef HAVE_SSE
	prefetchnta	(PREFETCH_OFFSET) * SIZE(B)
#if (L2_SIZE == 524288)
	prefetcht0	(PREFETCH_OFFSET) * SIZE(A)
#endif
#endif
	fmul	%st, %st(1)
	FMUL	 1 * SIZE(B)
	fxch	%st(1)
	faddp	%st, %st(5)
	FLD	 0 * SIZE(B)
	fxch	%st(1)
	faddp	%st, %st(4)
	FLD	 1 * SIZE(A)
	fmul	%st, %st(1)
	FMUL	 1 * SIZE(B)
	fxch	%st(1)
	faddp	%st, %st(7)
	FLD	 2 * SIZE(B)
	fxch	%st(1)
	faddp	%st, %st(6)
	FLD	 2 * SIZE(A)

	fmul	%st, %st(1)
	FMUL	 3 * SIZE(B)
	fxch	%st(1)
	faddp	%st, %st(5)
	FLD	 2 * SIZE(B)
	fxch	%st(1)
	faddp	%st, %st(4)
	FLD	 3 * SIZE(A)
	fmul	%st, %st(1)
	FMUL	 3 * SIZE(B)
	fxch	%st(1)
	faddp	%st, %st(7)
	FLD	 8 * SIZE(B)
	fxch	%st(1)
	faddp	%st, %st(6)
	FLD	 8 * SIZE(A)
	fxch	%st(2)

#ifdef HAVE_SSE
#ifdef DOUBLE
	prefetchnta	(PREFETCH_OFFSET + 4) * SIZE(B)
#if (L2_SIZE == 524288)
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
	faddp	%st, %st(4)
	FLD	 5 * SIZE(A)
	fmul	%st, %st(3)
	FMUL	 5 * SIZE(B)
	fxch	%st(3)
	faddp	%st, %st(7)
	FLD	 6 * SIZE(B)
	fxch	%st(3)
	faddp	%st, %st(6)
	FLD	 6 * SIZE(A)

	fmul	%st, %st(3)
	FMUL	 7 * SIZE(B)
	fxch	%st(3)
	faddp	%st, %st(5)
	FLD	 6 * SIZE(B)
	fxch	%st(3)
	faddp	%st, %st(4)
	FLD	 7 * SIZE(A)
	fmul	%st, %st(3)
	FMUL	 7 * SIZE(B)
	fxch	%st(3)
	faddp	%st, %st(7)
	FLD	12 * SIZE(B)
	fxch	%st(3)
	faddp	%st, %st(6)
	FLD	12 * SIZE(A)
	fxch	%st(2)

	subl	$-8 * SIZE, B
	subl	$-8 * SIZE, A
	decl	%eax
	jg	.L38
	ALIGN_4

.L37:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
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
	faddp	%st, %st(4)

	FLD	 1 * SIZE(A)
	fmul	%st, %st(1)
	FMUL	 1 * SIZE(B)
	fxch	%st(1)
	faddp	%st, %st(7)
	FLD	 2 * SIZE(B)
	fxch	%st(1)
	faddp	%st, %st(6)
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

#if defined(LN) || defined(LT)
#ifndef CONJ
	faddp	%st, %st(3)		# ctemp3 += ctemp4
	fsubp	%st, %st(1)		# ctemp1 += ctemp2
#else
	fsubp	%st, %st(3)		# ctemp1 += ctemp2
	faddp	%st, %st(1)		# ctemp3 += ctemp4
#endif
#endif

#if defined(RN) || defined(RT)
#ifndef CONJ
	faddp	%st, %st(3)		# ctemp3 += ctemp4
	fsubp	%st, %st(1)		# ctemp1 += ctemp2
#else
	fsubrp	%st, %st(3)		# ctemp1 += ctemp2
	faddp	%st, %st(1)		# ctemp3 += ctemp4
#endif
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	subl	$1, %eax
	sall	$ZBASE_SHIFT, %eax

	movl	AORIG, A
	movl	STACK_B, B
	addl	%eax, A
	addl	%eax, B
#endif

#if defined(LN) || defined(LT)
	FLD	0 * SIZE(B)
	fsubp	%st, %st(1)
	FLD	1 * SIZE(B)
	fsubp	%st, %st(2)
#else
	FLD	0 * SIZE(A)
	fsubp	%st, %st(1)
	FLD	1 * SIZE(A)
	fsubp	%st, %st(2)
#endif

#if defined(LN) || defined(LT)
	FLD	0 * SIZE(A)
	fmul	%st(1), %st
	FLD	0 * SIZE(A)
	fmul	%st(3), %st
	FLD	1 * SIZE(A)
	fmulp	%st, %st(3)
	FLD	1 * SIZE(A)
	fmulp	%st, %st(4)
#endif

#if defined(RN) || defined(RT)
	FLD	0 * SIZE(B)
	fmul	%st(1), %st
	FLD	0 * SIZE(B)
	fmul	%st(3), %st
	FLD	1 * SIZE(B)
	fmulp	%st, %st(3)
	FLD	1 * SIZE(B)
	fmulp	%st, %st(4)
#endif

#ifndef CONJ
	faddp	%st, %st(2)
	fsubp	%st, %st(2)
#else
	fsubp	%st, %st(2)
	faddp	%st, %st(2)
#endif

#ifdef LN
	subl	$2 * SIZE, C
#endif

#if defined(LN) || defined(LT)
	FSTU	1 * SIZE(B)
	fxch	%st(1)
	FSTU	0 * SIZE(B)
#else
	FSTU	1 * SIZE(A)
	fxch	%st(1)
	FSTU	0 * SIZE(A)
#endif
	FST	0 * SIZE(C)
	FST	1 * SIZE(C)

#ifndef LN
	addl	$2 * SIZE, C
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, A
	addl	%eax, B
#endif

#ifdef LN
	subl	$1, KK
#endif

#ifdef LT
	addl	$1, KK
#endif

#ifdef RT
       movl	K, %eax
       sall	$ZBASE_SHIFT, %eax
       addl	%eax, AORIG
#endif

	decl	M
	jg	.L34
	ALIGN_2

.L33:
#ifdef LN
	movl	K, %eax
	sall	$ZBASE_SHIFT, %eax
	addl	%eax, STACK_B
#endif
#if defined(LT) || defined(RN)
	movl	B, STACK_B
#endif

#ifdef RN
	addl	$1, KK
#endif

#ifdef RT
	subl	$1, KK
#endif

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
