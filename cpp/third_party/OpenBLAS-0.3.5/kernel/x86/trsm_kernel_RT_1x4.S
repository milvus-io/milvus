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
#define ARGS	32

#define J	 0 + STACK(%esp)
#define I	 4 + STACK(%esp)
#define KK	 8 + STACK(%esp)
#define KKK	12 + STACK(%esp)
#define AORIG	16 + STACK(%esp)

#define M	 4 + STACK + ARGS(%esp)
#define N	 8 + STACK + ARGS(%esp)
#define K	12 + STACK + ARGS(%esp)
#define ALPHA	16 + STACK + ARGS(%esp)
#ifdef DOUBLE
#define STACK_A	24 + STACK + ARGS(%esp)
#define STACK_B	28 + STACK + ARGS(%esp)
#define C	32 + STACK + ARGS(%esp)
#define STACK_LDC	36 + STACK + ARGS(%esp)
#define OFFSET	40 + STACK + ARGS(%esp)
#else
#define STACK_A	20 + STACK + ARGS(%esp)
#define STACK_B	24 + STACK + ARGS(%esp)
#define C	28 + STACK + ARGS(%esp)
#define STACK_LDC	32 + STACK + ARGS(%esp)
#define OFFSET	36 + STACK + ARGS(%esp)
#endif

#define A	%edx
#define B	%ecx
#define B_ORIG	%ebx
#define LDC	%ebp

#define PREFETCHSIZE (5 + 8 * 10)

	PROLOGUE

	subl	$ARGS, %esp	# Generate Stack Frame

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	STACK_B, B_ORIG
	movl	STACK_LDC, LDC
	leal	(, LDC, SIZE), LDC

#ifdef LN
       movl	M, %eax
       leal	(, %eax, SIZE), %eax
       addl	%eax, C
       imull	K, %eax
       addl	%eax, STACK_A
#endif

#ifdef RT
       movl	N, %eax
       leal	(, %eax, SIZE), %eax
       imull	K, %eax
       addl	%eax, B_ORIG

       movl	N,   %eax
       imull	LDC, %eax
       addl	%eax, C
#endif

#ifdef RN
       movl	OFFSET, %eax
       negl	%eax
       movl	%eax, KK
#endif

#ifdef RT
       movl	N, %eax
       subl	OFFSET, %eax
       movl	%eax, KK
#endif

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
	andl	$1,  %eax
	je	.L20
	ALIGN_3

.L31:
#if defined(LT) || defined(RN)
	movl	STACK_A, A
#else
	movl	STACK_A, %eax
	movl	%eax, AORIG
#endif

#ifdef RT
	movl	K, %eax
	sall	$0 + BASE_SHIFT, %eax
	subl	%eax, B_ORIG
#endif

#ifdef RT
	subl	LDC, C
#endif
	movl	C, %edi
#ifndef RT
	addl	LDC, C
#endif

#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

#ifdef LT
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	B_ORIG, B

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
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
#ifdef LN
       movl	K, %eax
       sall	$BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	movl	AORIG, A
	leal	(A     , %eax, 1), A
	leal	(B_ORIG, %eax, 1), B
#else
	movl	B_ORIG, B
#endif

	fldz
	fldz
	fldz
	fldz

	prefetchw	1 * SIZE(%edi)

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
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
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
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

#if defined(LN) || defined(RT)
	movl	KK, %eax
	subl	$1, %eax

	movl	AORIG, A
	leal	(A,      %eax, SIZE), A
	leal	(B_ORIG, %eax, SIZE), B
#endif

#if defined(LN) || defined(LT)
	FLD	0 * SIZE - 16 * SIZE(B)
	fsubp	%st, %st(1)
#else
	FLD	0 * SIZE - 16 * SIZE(A)
	fsubp	%st, %st(1)
#endif

#if defined(LN) || defined(LT)
       FLD	0 * SIZE - 16 * SIZE(A)
       fmulp	%st, %st(1)
#endif

#if defined(RN) || defined(RT)
       FMUL	 0 * SIZE - 16 * SIZE(B)
#endif

#ifdef LN
	subl	$1 * SIZE, %edi
#endif

#if defined(LN) || defined(LT)
	FSTU	0 * SIZE - 16 * SIZE(B)
#else
	FSTU	0 * SIZE - 16 * SIZE(A)
#endif

	FST	0 * SIZE(%edi)

#ifndef LN
	addl	$1 * SIZE, %edi
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(A, %eax, SIZE), A
	leal	(B, %eax, SIZE), B
#endif

#ifdef LN
	subl	$1, KK
#endif

#ifdef LT
	addl	$1, KK
#endif

#ifdef RT
       movl	K, %eax
       sall	$BASE_SHIFT, %eax
       addl	%eax, AORIG
#endif
	decl	I
	jne	.L34

#ifdef LN
       movl	K, %eax
       leal	(    , %eax, SIZE), %eax
       leal	(B_ORIG, %eax, 1), B_ORIG
#endif
#if defined(LT) || defined(RN)
	movl	B, B_ORIG
#endif

#ifdef RN
	addl	$1, KK
#endif

#ifdef RT
	subl	$1, KK
#endif
	ALIGN_4

.L20:
	movl	N,   %eax
	andl	$2,  %eax
	je	.L30

#if defined(LT) || defined(RN)
	movl	STACK_A, A
#else
	movl	STACK_A, %eax
	movl	%eax, AORIG
#endif

#ifdef RT
	movl	K, %eax
	sall	$1 + BASE_SHIFT, %eax
	subl	%eax, B_ORIG
#endif

	leal	(, LDC, 2), %eax
#ifdef RT
	subl	%eax, C
#endif
	movl	C, %edi
#ifndef RT
	addl	%eax, C
#endif

#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

#ifdef LT
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	B_ORIG, B

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
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
#ifdef LN
       movl	K, %eax
       sall	$BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	movl	AORIG, A
	leal	(A     , %eax, 1), A
	leal	(B_ORIG, %eax, 2), B
#else
	movl	B_ORIG, B
#endif

	fldz
	fldz
	fldz
	fldz

	FLD	 -16 * SIZE(A)
	FLD	 -16 * SIZE(B)

	prefetchw	1 * SIZE(%edi)
	prefetchw	1 * SIZE(%edi, LDC)

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
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
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
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

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$2, %eax
#endif

	leal	(, %eax, SIZE), %eax

	movl	AORIG, A
	leal	(A,      %eax, 1), A
	leal	(B_ORIG, %eax, 2), B
#endif

#if defined(LN) || defined(LT)
	FLD	0 * SIZE - 16 * SIZE(B)
	fsubp	%st, %st(1)
	FLD	1 * SIZE - 16 * SIZE(B)
	fsubp	%st, %st(2)
#else
	FLD	0 * SIZE - 16 * SIZE(A)
	fsubp	%st, %st(1)
	FLD	1 * SIZE - 16 * SIZE(A)
	fsubp	%st, %st(2)
#endif

#ifdef LN
       FLD	0 * SIZE - 16 * SIZE(A)
       fmul	%st, %st(1)
       fmulp	%st, %st(2)
#endif

#ifdef LT
       FLD	0 * SIZE - 16 * SIZE(A)
       fmul	%st, %st(1)
       fmulp	%st, %st(2)
#endif

#ifdef RN
       FMUL	 0 * SIZE - 16 * SIZE(B)

       FLD	 1 * SIZE - 16 * SIZE(B)
       fmul	 %st(1), %st
       fsubrp	 %st, %st(2)

       FLD	 3 * SIZE - 16 * SIZE(B)
       fmulp	 %st, %st(2)
#endif

#ifdef RT
       FLD	 3 * SIZE - 16 * SIZE(B)
       fmulp	 %st, %st(2)
       FLD	 2 * SIZE - 16 * SIZE(B)
       fmul	 %st(2), %st
       fsubrp	 %st, %st(1)

       FLD	 0 * SIZE - 16 * SIZE(B)
       fmulp	 %st, %st(1)
#endif

#ifdef LN
	subl	$1 * SIZE, %edi
#endif

#if defined(LN) || defined(LT)
	FSTU	0 * SIZE - 16 * SIZE(B)
	fxch	%st(1)
	FSTU	1 * SIZE - 16 * SIZE(B)
#else
	FSTU	0 * SIZE - 16 * SIZE(A)
	fxch	%st(1)
	FSTU	1 * SIZE - 16 * SIZE(A)
#endif

	FST	0 * SIZE(%edi, LDC)
	FST	0 * SIZE(%edi)

#ifndef LN
	addl	$1 * SIZE, %edi
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(A, %eax, 1), A
	leal	(B, %eax, 2), B
#endif

#ifdef LN
	subl	$1, KK
#endif

#ifdef LT
	addl	$1, KK
#endif

#ifdef RT
       movl	K, %eax
       sall	$BASE_SHIFT, %eax
       addl	%eax, AORIG
#endif
	decl	I
	jne	.L24

#ifdef LN
       movl	K, %eax
       leal	(    , %eax, SIZE), %eax
       leal	(B_ORIG, %eax, 2), B_ORIG
#endif
#if defined(LT) || defined(RN)
	movl	B, B_ORIG
#endif

#ifdef RN
	addl	$2, KK
#endif

#ifdef RT
	subl	$2, KK
#endif
	ALIGN_4

.L30:
	movl	N,   %eax
	sarl	$2,  %eax
	movl	%eax, J
	je	.L999
	ALIGN_3

.L11:
#if defined(LT) || defined(RN)
	movl	STACK_A, A
#else
	movl	STACK_A, %eax
	movl	%eax, AORIG
#endif

#ifdef RT
	movl	K, %eax
	sall	$2 + BASE_SHIFT, %eax
	subl	%eax, B_ORIG
#endif

	leal	(, LDC, 4), %eax
#ifdef RT
	subl	%eax, C
#endif
	movl	C, %edi
#ifndef RT
	addl	%eax, C
#endif

#ifdef LN
	movl	OFFSET, %eax
	addl	M, %eax
	movl	%eax, KK
#endif

#ifdef LT
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

	movl	B_ORIG, B

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
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
#ifdef LN
       movl	K, %eax
       sall	$BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	leal	(, %eax, SIZE), %eax
	movl	AORIG, A
	leal	(A     , %eax, 1), A
	leal	(B_ORIG, %eax, 4), B
#else
	movl	B_ORIG, B
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

	prefetchw	1 * SIZE(%edi)
	prefetchw	1 * SIZE(%edi, LDC)
	prefetchw	1 * SIZE(%eax)
	prefetchw	1 * SIZE(%eax, LDC)

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
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

	PADDING prefetch	PREFETCHSIZE * SIZE(A)

	addl	$8 * SIZE, A
	fxch	%st(1)
	addl	$32 * SIZE, B

	FLD	-16 * SIZE(B)
	decl	%eax
	jne	.L15
	ALIGN_4

.L16:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
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

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$4, %eax
#endif

	leal	(, %eax, SIZE), %eax

	movl	AORIG, A
	leal	(A,      %eax, 1), A
	leal	(B_ORIG, %eax, 4), B
#endif

#if defined(LN) || defined(LT)
	FLD	0 * SIZE - 16 * SIZE(B)
	fsubp	%st, %st(1)
	FLD	1 * SIZE - 16 * SIZE(B)
	fsubp	%st, %st(2)
	FLD	2 * SIZE - 16 * SIZE(B)
	fsubp	%st, %st(3)
	FLD	3 * SIZE - 16 * SIZE(B)
	fsubp	%st, %st(4)
#else
	FLD	0 * SIZE - 16 * SIZE(A)
	fsubp	%st, %st(1)
	FLD	1 * SIZE - 16 * SIZE(A)
	fsubp	%st, %st(2)
	FLD	2 * SIZE - 16 * SIZE(A)
	fsubp	%st, %st(3)
	FLD	3 * SIZE - 16 * SIZE(A)
	fsubp	%st, %st(4)
#endif

#ifdef LN
       FLD	0 * SIZE - 16 * SIZE(A)
       fmul	%st, %st(1)
       fmul	%st, %st(2)
       fmul	%st, %st(3)
       fmulp	%st, %st(4)
#endif

#ifdef LT
       FLD	0 * SIZE - 16 * SIZE(A)
       fmul	%st, %st(1)
       fmul	%st, %st(2)
       fmul	%st, %st(3)
       fmulp	%st, %st(4)
#endif

#ifdef RN
       FMUL	 0 * SIZE - 16 * SIZE(B)

       FLD	 1 * SIZE - 16 * SIZE(B)
       fmul	 %st(1), %st
       fsubrp	 %st, %st(2)
       FLD	 2 * SIZE - 16 * SIZE(B)
       fmul	 %st(1), %st
       fsubrp	 %st, %st(3)
       FLD	 3 * SIZE - 16 * SIZE(B)
       fmul	 %st(1), %st
       fsubrp	 %st, %st(4)

       FLD	 5 * SIZE - 16 * SIZE(B)
       fmulp	 %st, %st(2)
       FLD	 6 * SIZE - 16 * SIZE(B)
       fmul	 %st(2), %st
       fsubrp	 %st, %st(3)
       FLD	 7 * SIZE - 16 * SIZE(B)
       fmul	 %st(2), %st
       fsubrp	 %st, %st(4)

       FLD	10 * SIZE - 16 * SIZE(B)
       fmulp	 %st, %st(3)
       FLD	11 * SIZE - 16 * SIZE(B)
       fmul	 %st(3), %st
       fsubrp	 %st, %st(4)

       FLD	15 * SIZE - 16 * SIZE(B)
       fmulp	 %st, %st(4)
#endif

#ifdef RT
       FLD	15 * SIZE - 16 * SIZE(B)
       fmulp	 %st, %st(4)

       FLD	14 * SIZE - 16 * SIZE(B)
       fmul	 %st(4), %st
       fsubrp	 %st, %st(3)
       FLD	13 * SIZE - 16 * SIZE(B)
       fmul	 %st(4), %st
       fsubrp	 %st, %st(2)
       FLD	12 * SIZE - 16 * SIZE(B)
       fmul	 %st(4), %st
       fsubrp	 %st, %st(1)

       FLD	10 * SIZE - 16 * SIZE(B)
       fmulp	 %st, %st(3)
       FLD	 9 * SIZE - 16 * SIZE(B)
       fmul	 %st(3), %st
       fsubrp	 %st, %st(2)
       FLD	 8 * SIZE - 16 * SIZE(B)
       fmul	 %st(3), %st
       fsubrp	 %st, %st(1)

       FLD	 5 * SIZE - 16 * SIZE(B)
       fmulp	 %st, %st(2)
       FLD	 4 * SIZE - 16 * SIZE(B)
       fmul	 %st(2), %st
       fsubrp	 %st, %st(1)

       FLD	 0 * SIZE - 16 * SIZE(B)
       fmulp	 %st, %st(1)
#endif

#ifdef LN
	subl	$1 * SIZE, %edi
#endif

#if defined(LN) || defined(LT)
	FSTU	0 * SIZE - 16 * SIZE(B)
	fxch	%st(1)
	FSTU	1 * SIZE - 16 * SIZE(B)
	fxch	%st(2)
	FSTU	2 * SIZE - 16 * SIZE(B)
	fxch	%st(3)
	FSTU	3 * SIZE - 16 * SIZE(B)
#else
	FSTU	0 * SIZE - 16 * SIZE(A)
	fxch	%st(1)
	FSTU	1 * SIZE - 16 * SIZE(A)
	fxch	%st(2)
	FSTU	2 * SIZE - 16 * SIZE(A)
	fxch	%st(3)
	FSTU	3 * SIZE - 16 * SIZE(A)
#endif

	leal	(%edi, LDC, 2), %eax

	FST	0 * SIZE(%eax, LDC)
	FST	0 * SIZE(%edi)
	FST	0 * SIZE(%edi, LDC)
	FST	0 * SIZE(%eax)

#ifndef LN
	addl	$1 * SIZE, %edi
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	leal	(,%eax, SIZE), %eax
	leal	(A, %eax, 1), A
	leal	(B, %eax, 4), B
#endif

#ifdef LN
	subl	$1, KK
#endif

#ifdef LT
	addl	$1, KK
#endif

#ifdef RT
       movl	K, %eax
       sall	$BASE_SHIFT, %eax
       addl	%eax, AORIG
#endif

	decl	I
	jne	.L14

#ifdef LN
       movl	K, %eax
       leal	(    , %eax, SIZE), %eax
       leal	(B_ORIG, %eax, 4), B_ORIG
#endif
#if defined(LT) || defined(RN)
	movl	B, B_ORIG
#endif

#ifdef RN
	addl	$4, KK
#endif

#ifdef RT
	subl	$4, KK
#endif

	decl	J
	jne	.L11
	ALIGN_4

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	addl	$ARGS, %esp
	ret

	EPILOGUE
