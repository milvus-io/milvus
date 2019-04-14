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

#define J	 0 + STACK(%esp)
#define I	 4 + STACK(%esp)
#define KK	 8 + STACK(%esp)
#define KKK	12 + STACK(%esp)

#define M	 4 + STACK + ARGS(%esp)
#define N	 8 + STACK + ARGS(%esp)
#define K	12 + STACK + ARGS(%esp)
#ifdef DOUBLE
#define ALPHA_R	16 + STACK + ARGS(%esp)
#define ALPHA_I	24 + STACK + ARGS(%esp)
#define STACK_A	32 + STACK + ARGS(%esp)
#define STACK_B	36 + STACK + ARGS(%esp)
#define C	40 + STACK + ARGS(%esp)
#define STACK_LDC	44 + STACK + ARGS(%esp)
#else
#define ALPHA_R	16 + STACK + ARGS(%esp)
#define ALPHA_I	20 + STACK + ARGS(%esp)
#define STACK_A	24 + STACK + ARGS(%esp)
#define STACK_B	28 + STACK + ARGS(%esp)
#define C	32 + STACK + ARGS(%esp)
#define STACK_LDC	36 + STACK + ARGS(%esp)
#endif

#define A	%edx
#define B	%ecx
#define B_ORIG	%ebx
#define LDC	%ebp

#define PREFETCHSIZE (5 + 8 * 10)

/*

  A hint of scheduling is received from following URL

  http://www.netlib.org/atlas/atlas-comm/msg00260.html

  Julian's code is still faster than mine, since Athlon has big
  defect ... So this is a sample coding and please don't expect too
  much.

*/

	PROLOGUE

	subl	$ARGS, %esp	# Generate Stack Frame

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

#if defined(TRMMKERNEL) && !defined(LEFT)
	movl	OFFSET, %eax
	negl	%eax
	movl	%eax, KK
#endif

	movl	STACK_B, B_ORIG
	movl	STACK_LDC, LDC

	sall	$ZBASE_SHIFT, LDC

	subl	$-16 * SIZE, B_ORIG
	subl	$-16 * SIZE, STACK_A

	movl	M, %eax
	testl	%eax, %eax
	jle	.L999

	movl	N, %eax
	testl	%eax, %eax
	jle	.L999

	movl	K, %eax
	testl	%eax, %eax
	jle	.L999

	movl	N,   %eax
	sarl	$2,  %eax
	movl	%eax, J
	je	.L20
	ALIGN_3

.L11:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	STACK_A, A
	movl	C, %edi

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	B_ORIG, B
#else
	movl	KK,   %eax
	leal	(, %eax, SIZE), %eax
	leal	(B_ORIG, %eax, 4), B
#endif

#ifndef TRMMKERNEL
	movl	K,  %eax
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
	sarl	$4, %eax
	jle	.L13
	ALIGN_4

.L12:
	movl	 -16 * SIZE(B), %esi
	movl	  -8 * SIZE(B), %esi
	movl	   0 * SIZE(B), %esi
	movl	   8 * SIZE(B), %esi
	movl	  16 * SIZE(B), %esi
	movl	  24 * SIZE(B), %esi
	movl	  32 * SIZE(B), %esi
	movl	  40 * SIZE(B), %esi
	subl	 $-64 * SIZE, B
	decl	  %eax
	jne	  .L12
	ALIGN_3

.L13:
	movl	M, %esi
	movl	%esi, I
	ALIGN_3

.L14:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	B_ORIG, B
#else
	movl	KK,   %eax
	leal	(, %eax, SIZE), %eax
	leal	(A,      %eax, 1), A
	leal	(B_ORIG, %eax, 4), B
#endif

	leal	(%edi, LDC, 2), %eax

	fldz
	fldz
	fldz
	fldz

	FLD	  -8 * SIZE(A)
	FLD	 -16 * SIZE(A)
	FLD	 -16 * SIZE(B)

	movl	$32 * SIZE, %esi

#ifdef HAVE_3DNOW
	prefetchw	1 * SIZE(%edi)
	prefetchw	2 * SIZE(%edi, LDC)
	prefetchw	1 * SIZE(%eax)
	prefetchw	2 * SIZE(%eax, LDC)
#elif defined(HAVE_SSE)
	prefetcht0	1 * SIZE(%edi)
	prefetcht0	1 * SIZE(%edi, LDC)
	prefetcht0	1 * SIZE(%eax)
	prefetcht0	1 * SIZE(%eax, LDC)
#endif

#ifndef TRMMKERNEL
	movl	K,  %eax
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
 	je	.L16
	ALIGN_3

.L15:
	fmul	%st(1), %st
	faddp	%st, %st(3)
	PADDING
	FLD	-15 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(4)
	PADDING
	FLD	-14 * SIZE(B)

#if L1_DATA_LINESIZE == 32
#ifdef HAVE_3DNOW
	PADDING prefetch	(PREFETCHSIZE - 4) * SIZE(A)
#elif defined(HAVE_SSE)
	PADDING prefetcht0	(PREFETCHSIZE - 4) * SIZE(A)
#endif
#endif

	fmul	%st(1), %st
	faddp	%st, %st(5)
	PADDING
	FMUL	-13 * SIZE(B)

	faddp	%st, %st(5)
	FLD	-15 * SIZE(A)
	FLD	-12 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(3)
	PADDING
	FLD	-11 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(4)
	PADDING
	FLD	-10 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(5)
	PADDING
	FMUL	 -9 * SIZE(B)

	faddp	%st, %st(5)
	FLD	-14 * SIZE(A)
	FLD	 -8 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(3)
	PADDING
	FLD	 -7 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(4)
	PADDING
	FLD	 -6 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(5)
	PADDING
	FMUL	 -5 * SIZE(B)

	faddp	%st, %st(5)
	FLD	-13 * SIZE(A)
	FLD	 -4 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(3)
	PADDING
	FLD	 -3 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(4)
	PADDING
	FLD	 -2 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(5)
	PADDING
	FMUL	 -1 * SIZE(B)

	faddp	%st, %st(5)
	FLD	-12 * SIZE(A)
	FLD	  0 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(3)
	PADDING
	FLD	  1 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(4)
	PADDING
	FLD	  2 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(5)
	PADDING
	FMUL	  3 * SIZE(B)

	faddp	%st, %st(5)
	FLD	-11 * SIZE(A)
	FLD	  4 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(3)
	PADDING
	FLD	  5 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(4)
	PADDING
	FLD	  6 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(5)
	PADDING
	FMUL	  7 * SIZE(B)

	faddp	%st, %st(5)
	FLD	-10 * SIZE(A)
	FLD	  8 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(3)
	PADDING
	FLD	  9 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(4)
	PADDING
	FLD	 10 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(5)
	PADDING
	FMUL	 11 * SIZE(B)

	faddp	%st, %st(5)
	FLD	 -9 * SIZE(A)
	FLD	 12 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(3)
	PADDING
	FLD	 13 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(4)
	PADDING
	FLD	 14 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(5)
	PADDING
	FMUL	 15 * SIZE(B)

	faddp	%st, %st(5)
	FLD	  0 * SIZE(A)

#ifdef HAVE_3DNOW
	PADDING prefetch	PREFETCHSIZE * SIZE(A)
#elif defined(HAVE_SSE)
	PADDING prefetcht0	PREFETCHSIZE * SIZE(A)
#endif

	addl	$8 * SIZE, A
	fxch	%st(1)
	addl	$32 * SIZE, B

	FLD	-16 * SIZE(B)
	decl	%eax
	jne	.L15
	ALIGN_4

.L16:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	and	$7, %eax
	je	.L19
	ALIGN_4

.L17:
	fmul	%st(1), %st
	faddp	%st, %st(3)

	FLD	-15 * SIZE(B)
	fmul	%st(1), %st
	faddp	%st, %st(4)

	FLD	-14 * SIZE(B)
	fmul	%st(1), %st
	faddp	%st, %st(5)

	FMUL	-13 * SIZE(B)
	faddp	%st, %st(5)
	FLD	-15 * SIZE(A)
	FLD	-12 * SIZE(B)

	addl	$1 * SIZE,A
	addl	$4 * SIZE,B

	decl	%eax
	jne	 .L17
	ALIGN_4

.L19:
	ffreep	%st(0)
	ffreep	%st(0)
	ffreep	%st(0)

	leal	(%edi, LDC, 2), %eax

	FLD	ALPHA_I
	FLD	ALPHA_R

	fld	%st(2)
	fmul	%st(1), %st

	FLD	0 * SIZE(%edi)
	faddp	%st, %st(1)
	FST	0 * SIZE(%edi)

	fld	%st(3)
	fmul	%st(1), %st

	FLD	0 * SIZE(%edi, LDC)
	faddp	%st, %st(1)
	FST	0 * SIZE(%edi, LDC)

	fld	%st(4)
	fmul	%st(1), %st

	FLD	0 * SIZE(%eax)
	faddp	%st, %st(1)
	FST	0 * SIZE(%eax)

	fmul	%st(5), %st

	FLD	0 * SIZE(%eax, LDC)
	faddp	%st, %st(1)
	FST	0 * SIZE(%eax, LDC)

	fmul	%st, %st(1)
	fmul	%st, %st(2)
	fmul	%st, %st(3)
	fmulp	%st, %st(4)

	FLD	1 * SIZE(%edi)
	faddp	%st, %st(1)
	FST	1 * SIZE(%edi)

	FLD	1 * SIZE(%edi, LDC)
	faddp	%st, %st(1)
	FST	1 * SIZE(%edi, LDC)

	FLD	1 * SIZE(%eax)
	faddp	%st, %st(1)
	FST	1 * SIZE(%eax)

	FLD	1 * SIZE(%eax, LDC)
	faddp	%st, %st(1)
	FST	1 * SIZE(%eax, LDC)

	addl	$2 * SIZE, %edi
	decl	I
	jne	.L14

#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$4, KK
#endif

	leal	(, LDC, 4), %eax
	addl	%eax, C
	movl	B, B_ORIG
	decl	J
	jne	.L11
	ALIGN_4

.L20:
	movl	N,   %eax
	andl	$2,  %eax
	je	.L30
	ALIGN_3

.L21:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	STACK_A, A
	movl	C, %edi

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	B_ORIG, B
#else
	movl	KK,   %eax
	leal	(, %eax, SIZE), %eax
	leal	(B_ORIG, %eax, 2), B
#endif

#ifndef TRMMKERNEL
	movl	K,  %eax
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
	sarl	$4, %eax
	jle	.L23
	ALIGN_4

.L22:
	movl	 -16 * SIZE(B), %esi
	movl	  -8 * SIZE(B), %esi
	movl	   0 * SIZE(B), %esi
	movl	   8 * SIZE(B), %esi
	subl	 $-32 * SIZE, B
	decl	  %eax
	jne	  .L22
	ALIGN_3

.L23:
	movl	M, %esi
	movl	%esi, I
	ALIGN_3

.L24:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	B_ORIG, B
#else
	movl	KK,   %eax
	leal	(, %eax, SIZE), %eax
	leal	(A,      %eax, 1), A
	leal	(B_ORIG, %eax, 2), B
#endif

	fldz
	fldz
	fldz
	fldz

	FLD	 -16 * SIZE(A)
	FLD	 -16 * SIZE(B)

	prefetchw	1 * SIZE(%edi)
	prefetchw	1 * SIZE(%edi, LDC)

#ifndef TRMMKERNEL
	movl	K,  %eax
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
 	je	.L26
	ALIGN_3

.L25:
	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	-15 * SIZE(B)
	faddp	%st, %st(2)

	FLD	-15 * SIZE(A)
	FLD	-14 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(4)

	FMUL	-13 * SIZE(B)
	faddp	%st, %st(4)

	FLD	-14 * SIZE(A)
	FLD	-12 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	-11 * SIZE(B)
	faddp	%st, %st(2)

	FLD	-13 * SIZE(A)
	FLD	-10 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(4)

	FMUL	 -9 * SIZE(B)
	faddp	%st, %st(4)

	FLD	-12 * SIZE(A)
	FLD	 -8 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	 -7 * SIZE(B)
	faddp	%st, %st(2)

	FLD	-11 * SIZE(A)
	FLD	 -6 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(4)

	FMUL	 -5 * SIZE(B)
	faddp	%st, %st(4)

	FLD	-10 * SIZE(A)
	FLD	 -4 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	 -3 * SIZE(B)
	faddp	%st, %st(2)

	FLD	 -9 * SIZE(A)
	FLD	 -2 * SIZE(B)

	fmul	%st(1), %st
	faddp	%st, %st(4)

	FMUL	 -1 * SIZE(B)
	faddp	%st, %st(4)

	FLD	 -8 * SIZE(A)
	FLD	  0 * SIZE(B)

	addl	$  8 * SIZE, A
	subl	$-16 * SIZE, B

	decl	%eax
	jne	.L25
	ALIGN_4

.L26:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	and	$7, %eax
	je	.L29
	ALIGN_4

.L27:
	fmul	%st(1), %st
	faddp	%st, %st(2)

	FMUL	-15 * SIZE(B)
	faddp	%st, %st(2)

	FLD	 -15 * SIZE(A)
	FLD	 -14 * SIZE(B)

	addl	$1 * SIZE,A
	addl	$2 * SIZE,B

	decl	%eax
	jne	 .L27
	ALIGN_4

.L29:
	ffreep	%st(0)
	ffreep	%st(0)

	faddp	%st, %st(2)
	faddp	%st, %st(2)

	FLD	ALPHA_I
	FLD	ALPHA_R

	fld	%st(2)
	fmul	%st(1), %st

	FLD	0 * SIZE(%edi)
	faddp	%st, %st(1)
	FST	0 * SIZE(%edi)

	fmul	%st(3), %st

	FLD	0 * SIZE(%edi, LDC)
	faddp	%st, %st(1)
	FST	0 * SIZE(%edi, LDC)

	fmul	%st, %st(1)
	fmulp	%st, %st(2)

	FLD	1 * SIZE(%edi)
	faddp	%st, %st(1)
	FST	1 * SIZE(%edi)

	FLD	1 * SIZE(%edi, LDC)
	faddp	%st, %st(1)
	FST	1 * SIZE(%edi, LDC)

	addl	$2 * SIZE, %edi
	decl	I
	jne	.L24

#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	leal	(, LDC, 2), %eax
	addl	%eax, C
	movl	B, B_ORIG
	ALIGN_4

.L30:
	movl	N,   %eax
	andl	$1,  %eax
	je	.L999
	ALIGN_3

.L31:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	STACK_A, A
	movl	C, %edi

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	B_ORIG, B
#else
	movl	KK,   %eax
	leal	(, %eax, SIZE), %eax
	leal	(B_ORIG, %eax, 1), B
#endif

#ifndef TRMMKERNEL
	movl	K,  %eax
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
	sarl	$5, %eax
	jle	.L33
	ALIGN_4

.L32:
	movl	 -16 * SIZE(B), %esi
	movl	  -8 * SIZE(B), %esi
	movl	   0 * SIZE(B), %esi
	movl	   8 * SIZE(B), %esi
	subl	 $-32 * SIZE, B
	decl	  %eax
	jne	  .L32
	ALIGN_3

.L33:
	movl	M, %esi
	movl	%esi, I
	ALIGN_3

.L34:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	B_ORIG, B
#else
	movl	KK,   %eax
	leal	(, %eax, SIZE), %eax
	leal	(A,      %eax, 1), A
	leal	(B_ORIG, %eax, 1), B
#endif

	fldz
	fldz
	fldz
	fldz

	prefetchw	1 * SIZE(%edi)

#ifndef TRMMKERNEL
	movl	K,  %eax
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
 	je	.L36
	ALIGN_3

.L35:
	FLD	-16 * SIZE(A)
	FMUL	-16 * SIZE(B)
	faddp	%st, %st(1)

	FLD	-15 * SIZE(A)
	FMUL	-15 * SIZE(B)
	faddp	%st, %st(2)

	FLD	-14 * SIZE(A)
	FMUL	-14 * SIZE(B)
	faddp	%st, %st(3)

	FLD	-13 * SIZE(A)
	FMUL	-13 * SIZE(B)
	faddp	%st, %st(4)

	FLD	-12 * SIZE(A)
	FMUL	-12 * SIZE(B)
	faddp	%st, %st(1)

	FLD	-11 * SIZE(A)
	FMUL	-11 * SIZE(B)
	faddp	%st, %st(2)

	FLD	-10 * SIZE(A)
	FMUL	-10 * SIZE(B)
	faddp	%st, %st(3)

	FLD	 -9 * SIZE(A)
	FMUL	 -9 * SIZE(B)
	faddp	%st, %st(4)

	addl	$8 * SIZE, A
	addl	$8 * SIZE, B

	decl	%eax
	jne	.L35
	ALIGN_4

.L36:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	and	$7, %eax
	je	.L39
	ALIGN_4

.L37:
	FLD	-16 * SIZE(A)
	FMUL	-16 * SIZE(B)
	faddp	%st, %st(1)

	addl	$1 * SIZE,A
	addl	$1 * SIZE,B
	decl	%eax
	jne	 .L37
	ALIGN_4

.L39:
	faddp	%st, %st(2)
	faddp	%st, %st(2)
	faddp	%st, %st(1)

	FLD	ALPHA_I
	FLD	ALPHA_R

	fmul	%st(2), %st

	FLD	0 * SIZE(%edi)
	faddp	%st, %st(1)
	FST	0 * SIZE(%edi)

	fmulp	%st, %st(1)

	FLD	1 * SIZE(%edi)
	faddp	%st, %st(1)
	FST	1 * SIZE(%edi)

	addl	$2 * SIZE, %edi
	decl	I
	jne	.L34

#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$1, KK
#endif

	addl	LDC, C
	movl	B, B_ORIG
	ALIGN_4

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	addl	$ARGS, %esp
	ret

	EPILOGUE
