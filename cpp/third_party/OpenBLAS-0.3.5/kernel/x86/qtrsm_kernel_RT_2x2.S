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

#ifdef OPTERON
#define PREFETCH	prefetch
#define PREFETCHW	prefetchw
#else
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#endif

#define PREFETCHSIZE (5 + 4 * 10)
#define STACK	16
#define ARGS	16

#define J	 0 + STACK(%esp)
#define KK	 4 + STACK(%esp)
#define KKK	 8 + STACK(%esp)
#define AORIG	12 + STACK(%esp)

#define M	 4 + STACK + ARGS(%esp)
#define N	 8 + STACK + ARGS(%esp)
#define K	12 + STACK + ARGS(%esp)
#define ALPHA	16 + STACK + ARGS(%esp)
#define A	32 + STACK + ARGS(%esp)
#define ARG_B	36 + STACK + ARGS(%esp)
#define C	40 + STACK + ARGS(%esp)
#define ARG_LDC	44 + STACK + ARGS(%esp)
#define OFFSET	48 + STACK + ARGS(%esp)

#define I	%esi
#define B	%ebx
#define CO	%edi
#define AO	%edx
#define BO	%ecx
#define LDC	%ebp

#define PREFETCH_OFFSET 48

	PROLOGUE

	subl	$ARGS, %esp	# Generate Stack Frame

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	ARG_LDC, LDC
	movl	ARG_B,   B
	sall	$BASE_SHIFT, LDC

	addl	$8 * SIZE, A
	addl	$8 * SIZE, B


#ifdef LN
       movl	M, %eax
       sall	$BASE_SHIFT, %eax
       addl	%eax, C
       imull	K, %eax
       addl	%eax, A
#endif

#ifdef RT
       movl	N, %eax
       sall	$BASE_SHIFT, %eax
       imull	K, %eax
       addl	%eax, B

       movl	N,    %eax
       imull	%ebp, %eax
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

	movl	N,  %eax
	testl	$1, %eax
	je	.L30

#if defined(LT) || defined(RN)
	movl	A, AO
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

#ifdef RT
	movl	K, %eax
	sall	$0 + BASE_SHIFT, %eax
	subl	%eax, B
#endif

#ifdef RT
	subl	LDC, C
#endif
	movl	C, CO
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

	movl	M,  I
	sarl	$1, I
	je	.L40
	ALIGN_4

.L31:
#ifdef LN
       movl	K, %eax
       sall	$1 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$BASE_SHIFT, %eax
	movl	AORIG, AO
	leal	(AO, %eax, 2), AO
	leal	(B,  %eax, 1), BO
#else
	movl	B, BO
#endif

	fldz
	fldz

#if   defined(HAVE_3DNOW)
	prefetchw	2 * SIZE(CO)
#elif defined(HAVE_SSE)
	prefetchnta	2 * SIZE(CO)
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	sarl	$2, %eax
 	je	.L35
	ALIGN_4

.L32:
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(AO)

	FLD	 -8 * SIZE(BO)
	FLD	 -8 * SIZE(AO)
	fmul	 %st(1), %st
	faddp	 %st, %st(2)

	FLD	 -7 * SIZE(AO)
	fmulp	 %st, %st(1)
	faddp	 %st, %st(2)

	FLD	 -7 * SIZE(BO)
	FLD	 -6 * SIZE(AO)
	fmul	 %st(1), %st
	faddp	 %st, %st(2)

	FLD	 -5 * SIZE(AO)
	fmulp	 %st, %st(1)
	faddp	 %st, %st(2)

	FLD	 -6 * SIZE(BO)
	FLD	 -4 * SIZE(AO)
	fmul	 %st(1), %st
	faddp	 %st, %st(2)

	FLD	 -3 * SIZE(AO)
	fmulp	 %st, %st(1)
	faddp	 %st, %st(2)

	FLD	 -5 * SIZE(BO)
	FLD	 -2 * SIZE(AO)
	fmul	 %st(1), %st
	faddp	 %st, %st(2)

	FLD	 -1 * SIZE(AO)
	fmulp	 %st, %st(1)
	faddp	 %st, %st(2)

	addl	$8 * SIZE,AO
	addl	$4 * SIZE,BO

	decl	%eax
	jne	.L32
	ALIGN_4

.L35:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	and	$3,  %eax
	je	.L38
	ALIGN_4

.L36:
	FLD	 -8 * SIZE(BO)

	FLD	 -8 * SIZE(AO)
	fmul	 %st(1), %st
	faddp	 %st, %st(2)

	FLD	 -7 * SIZE(AO)
	fmulp	 %st, %st(1)
	faddp	 %st, %st(2)

	addl	$2 * SIZE,AO
	addl	$1 * SIZE,BO

	decl	%eax
	jne	 .L36
	ALIGN_4

.L38:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$2, %eax
#else
	subl	$1, %eax
#endif

	sall	$BASE_SHIFT, %eax

	movl	AORIG, AO
	leal	(AO, %eax, 2), AO
	leal	(B,  %eax, 1), BO
#endif

#if defined(LN) || defined(LT)
	FLD	-8 * SIZE(BO)
	fsubp	%st, %st(1)
	FLD	-7 * SIZE(BO)
	fsubp	%st, %st(2)
#else
	FLD	-8 * SIZE(AO)
	fsubp	%st, %st(1)
	FLD	-7 * SIZE(AO)
	fsubp	%st, %st(2)
#endif

#ifdef LN
       FLD	-5 * SIZE(AO)
       fmulp	%st, %st(2)

       FLD	-6 * SIZE(AO)
       fmul	%st(2), %st

       fsubrp	%st, %st(1)
       FLD	-8 * SIZE(AO)
       fmulp	%st, %st(1)
#endif

#ifdef LT
       FLD	-8 * SIZE(AO)
       fmulp	%st, %st(1)

       FLD	-7 * SIZE(AO)
       fmul	%st(1), %st

       fsubrp	%st, %st(2)

       FLD	-5 * SIZE(AO)
       fmulp	%st, %st(2)
#endif

#ifdef RN
       FLD	-8 * SIZE(BO)
       fmul	%st, %st(1)
       fmulp	%st, %st(2)
#endif

#ifdef RT
       FLD	-8 * SIZE(BO)
       fmul	%st, %st(1)
       fmulp	%st, %st(2)
#endif

#ifdef LN
	subl	$2 * SIZE, CO
#endif

#if defined(LN) || defined(LT)
	fld	%st
	FST	-8 * SIZE(BO)
	fxch	%st(1)
	fld	%st
	FST	-7 * SIZE(BO)
#else
	fld	%st
	FST	-8 * SIZE(AO)
	fxch	%st(1)
	fld	%st
	FST	-7 * SIZE(AO)
#endif

	FST	1 * SIZE(CO)
	FST	0 * SIZE(CO)

#ifndef LN
	addl	$2 * SIZE, CO
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$BASE_SHIFT, %eax
	leal	(AO, %eax, 2), AO
	leal	(BO, %eax, 1), BO
#endif

#ifdef LN
	subl	$2, KK
#endif

#ifdef LT
	addl	$2, KK
#endif

#ifdef RT
       movl	K, %eax
       sall	$1 + BASE_SHIFT, %eax
       addl	%eax, AORIG
#endif

	decl	I
	jne	.L31
	ALIGN_4

.L40:
	movl	 M, %eax
	andl	$1, %eax
	je	.L49
	ALIGN_4

.L41:
#ifdef LN
       movl	K, %eax
       sall	$0 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$BASE_SHIFT, %eax
	movl	AORIG, AO
	leal	(AO, %eax, 1), AO
	leal	(B,  %eax, 1), BO
#else
	movl	B, BO
#endif

	fldz

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	sarl	$2, %eax
 	je	.L45
	ALIGN_4

.L42:
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(AO)

	FLD	 -8 * SIZE(AO)
	FLD	 -8 * SIZE(BO)
	fmulp	 %st, %st(1)
	faddp	 %st, %st(1)

	FLD	 -7 * SIZE(AO)
	FLD	 -7 * SIZE(BO)
	fmulp	 %st, %st(1)
	faddp	 %st, %st(1)

	FLD	 -6 * SIZE(AO)
	FLD	 -6 * SIZE(BO)
	fmulp	 %st, %st(1)
	faddp	 %st, %st(1)

	FLD	 -5 * SIZE(AO)
	FLD	 -5 * SIZE(BO)
	fmulp	 %st, %st(1)
	faddp	 %st, %st(1)

	addl	$4 * SIZE,AO
	addl	$4 * SIZE,BO

	decl	%eax
	jne	.L42
	ALIGN_4

.L45:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	and	$3,  %eax
	je	.L48
	ALIGN_4

.L46:
	FLD	 -8 * SIZE(AO)

	FLD	 -8 * SIZE(BO)
	fmulp	 %st, %st(1)
	faddp	 %st, %st(1)

	addl	$1 * SIZE,AO
	addl	$1 * SIZE,BO

	decl	%eax
	jne	 .L46
	ALIGN_4

.L48:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$1, %eax
#endif

	sall	$BASE_SHIFT, %eax

	movl	AORIG, AO
	leal	(AO, %eax, 1), AO
	leal	(B,  %eax, 1), BO
#endif

#if defined(LN) || defined(LT)
	FLD	-8 * SIZE(BO)
	fsubp	%st, %st(1)
#else
	FLD	-8 * SIZE(AO)
	fsubp	%st, %st(1)
#endif

#ifdef LN
       FLD	-8 * SIZE(AO)
       fmulp	%st, %st(1)
#endif

#ifdef LT
       FLD	-8 * SIZE(AO)
       fmulp	%st, %st(1)
#endif

#ifdef RN
       FLD	-8 * SIZE(BO)
       fmulp	%st, %st(1)
#endif

#ifdef RT
       FLD	-8 * SIZE(BO)
       fmulp	%st, %st(1)
#endif

#ifdef LN
	subl	$1 * SIZE, CO
#endif

#if defined(LN) || defined(LT)
	fld	%st
	FST	-8 * SIZE(BO)
#else
	fld	%st
	FST	-8 * SIZE(AO)
#endif

	FST	0 * SIZE(CO)

#ifndef LN
	addl	$1 * SIZE, CO
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$BASE_SHIFT, %eax
	leal	(AO, %eax, 1), AO
	leal	(BO, %eax, 1), BO
#endif

#ifdef LN
	subl	$1, KK
#endif

#ifdef LT
	addl	$1, KK
#endif

#ifdef RT
       movl	K, %eax
       sall	$0 + BASE_SHIFT, %eax
       addl	%eax, AORIG
#endif
	ALIGN_4

.L49:
#ifdef LN
       movl	K, %eax
       sall	$BASE_SHIFT, %eax
       leal	(B, %eax, 1), B
#endif

#if defined(LT) || defined(RN)
	movl	BO, B
#endif

#ifdef RN
	addl	$1, KK
#endif

#ifdef RT
	subl	$1, KK
#endif
	ALIGN_4

.L30:
	movl	N,   %eax
	sarl	$1,  %eax
	movl	%eax, J
	je	.L999
	ALIGN_4

.L01:
#if defined(LT) || defined(RN)
	movl	A, AO
#else
	movl	A, %eax
	movl	%eax, AORIG
#endif

#ifdef RT
	movl	K, %eax
	sall	$1 + BASE_SHIFT, %eax
	subl	%eax, B
#endif

	lea	(, LDC, 2), %eax

#ifdef RT
	subl	%eax, C
#endif
	movl	C, CO
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

	movl	M,  I
	sarl	$1, I
	je	.L20
	ALIGN_4

.L11:
#ifdef LN
       movl	K, %eax
       sall	$1 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$BASE_SHIFT, %eax
	movl	AORIG, AO
	leal	(AO, %eax, 2), AO
	leal	(B,  %eax, 2), BO
#else
	movl	B, BO
#endif

	fldz
	fldz
	fldz
	fldz

#if   defined(HAVE_3DNOW)
	prefetchw	2 * SIZE(CO)
 	prefetchw	2 * SIZE(CO, LDC, 1)
#elif defined(HAVE_SSE)
	prefetchnta	2 * SIZE(CO)
 	prefetchnta	2 * SIZE(CO, LDC, 1)
#endif

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	sarl	$2, %eax
 	je	.L15
	ALIGN_4

.L12:
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(AO)

	FLD	 -8 * SIZE(AO)

	FLD	 -8 * SIZE(BO)
	fld	 %st(1)
	fmul	 %st(1), %st
	faddp	 %st, %st(3)

	FLD	 -7 * SIZE(BO)
	fmul	 %st, %st(2)

	FLD	 -7 * SIZE(AO)
	fmul	 %st, %st(2)
	fmulp	 %st, %st(1)

	faddp	 %st, %st(6)
	faddp	 %st, %st(4)
	faddp	 %st, %st(2)

	FLD	 -6 * SIZE(AO)

	FLD	 -6 * SIZE(BO)
	fld	 %st(1)
	fmul	 %st(1), %st
	faddp	 %st, %st(3)

	FLD	 -5 * SIZE(BO)
	fmul	 %st, %st(2)

	FLD	 -5 * SIZE(AO)
	fmul	 %st, %st(2)
	fmulp	 %st, %st(1)

	faddp	 %st, %st(6)
	faddp	 %st, %st(4)
	faddp	 %st, %st(2)

	PREFETCH	(PREFETCHSIZE + 4) * SIZE(AO)

	FLD	 -4 * SIZE(AO)

	FLD	 -4 * SIZE(BO)
	fld	 %st(1)
	fmul	 %st(1), %st
	faddp	 %st, %st(3)

	FLD	 -3 * SIZE(BO)
	fmul	 %st, %st(2)

	FLD	 -3 * SIZE(AO)
	fmul	 %st, %st(2)
	fmulp	 %st, %st(1)

	faddp	 %st, %st(6)
	faddp	 %st, %st(4)
	faddp	 %st, %st(2)

	FLD	 -2 * SIZE(AO)

	FLD	 -2 * SIZE(BO)
	fld	 %st(1)
	fmul	 %st(1), %st
	faddp	 %st, %st(3)

	FLD	 -1 * SIZE(BO)
	fmul	 %st, %st(2)

	FLD	 -1 * SIZE(AO)
	fmul	 %st, %st(2)
	fmulp	 %st, %st(1)

	faddp	 %st, %st(6)
	faddp	 %st, %st(4)
	faddp	 %st, %st(2)

	addl	$8 * SIZE,AO
	addl	$8 * SIZE,BO

	decl	%eax
	jne	.L12
	ALIGN_4

.L15:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	and	$3,  %eax
	je	.L18
	ALIGN_4

.L16:
	FLD	 -8 * SIZE(AO)

	FLD	 -8 * SIZE(BO)
	fld	 %st(1)
	fmul	 %st(1), %st
	faddp	 %st, %st(3)

	FLD	 -7 * SIZE(BO)
	fmul	 %st, %st(2)

	FLD	 -7 * SIZE(AO)
	fmul	 %st, %st(2)
	fmulp	 %st, %st(1)

	faddp	 %st, %st(6)
	faddp	 %st, %st(4)
	faddp	 %st, %st(2)

	addl	$2 * SIZE,AO
	addl	$2 * SIZE,BO

	decl	%eax
	jne	 .L16
	ALIGN_4

.L18:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$2, %eax
#else
	subl	$2, %eax
#endif

	sall	$BASE_SHIFT, %eax

	movl	AORIG, AO
	leal	(AO, %eax, 2), AO
	leal	(B,  %eax, 2), BO
#endif

#if defined(LN) || defined(LT)
	FLD	-8 * SIZE(BO)
	fsubp	%st, %st(1)
	FLD	-7 * SIZE(BO)
	fsubp	%st, %st(2)
	FLD	-6 * SIZE(BO)
	fsubp	%st, %st(3)
	FLD	-5 * SIZE(BO)
	fsubp	%st, %st(4)
#else
	FLD	-8 * SIZE(AO)
	fsubp	%st, %st(1)
	FLD	-7 * SIZE(AO)
	fsubp	%st, %st(3)
	FLD	-6 * SIZE(AO)
	fsubp	%st, %st(2)
	FLD	-5 * SIZE(AO)
	fsubp	%st, %st(4)
#endif

#ifdef LN
       FLD	-5 * SIZE(AO)
       fmul	%st, %st(3)
       fmulp	%st, %st(4)

       FLD	-6 * SIZE(AO)
       fmul	%st(3), %st
       FLD	-6 * SIZE(AO)
       fmul	%st(5), %st

       fsubrp	%st, %st(3)
       fsubrp	%st, %st(1)

       FLD	-8 * SIZE(AO)
       fmul	%st, %st(1)
       fmulp	%st, %st(2)
#endif

#ifdef LT
       FLD	-8 * SIZE(AO)
       fmul	%st, %st(1)
       fmulp	%st, %st(2)

       FLD	-7 * SIZE(AO)
       fmul	%st(1), %st
       FLD	-7 * SIZE(AO)
       fmul	%st(3), %st

       fsubrp	%st, %st(5)
       fsubrp	%st, %st(3)

       FLD	-5 * SIZE(AO)
       fmul	%st, %st(3)
       fmulp	%st, %st(4)
#endif

#ifdef RN
       FLD	-8 * SIZE(BO)
       fmul	%st, %st(1)
       fmulp	%st, %st(3)

       FLD	-7 * SIZE(BO)
       fmul	%st(1), %st
       FLD	-7 * SIZE(BO)
       fmul	%st(4), %st

       fsubrp	%st, %st(5)
       fsubrp	%st, %st(2)

       FLD	-5 * SIZE(BO)
       fmul	%st, %st(2)
       fmulp	%st, %st(4)
#endif

#ifdef RT
       FLD	-5 * SIZE(BO)
       fmul	%st, %st(2)
       fmulp	%st, %st(4)

       FLD	-6 * SIZE(BO)
       fmul	%st(2), %st
       FLD	-6 * SIZE(BO)
       fmul	%st(5), %st

       fsubrp	%st, %st(4)
       fsubrp	%st, %st(1)

       FLD	-8 * SIZE(BO)
       fmul	%st, %st(1)
       fmulp	%st, %st(3)
#endif

#ifdef LN
	subl	$2 * SIZE, CO
#endif

#if defined(LN) || defined(LT)
	fld	%st
	FST	-8 * SIZE(BO)
	fxch	%st(1)
	fld	%st
	FST	-7 * SIZE(BO)
	fxch	%st(2)
	fld	%st
	FST	-6 * SIZE(BO)
	fxch	%st(3)
	fld	%st
	FST	-5 * SIZE(BO)

	FST	1 * SIZE(CO, LDC)
	FST	0 * SIZE(CO)
	FST	0 * SIZE(CO, LDC)
	FST	1 * SIZE(CO)
#else
	fld	%st
	FST	-8 * SIZE(AO)
	fxch	%st(2)
	fld	%st
	FST	-7 * SIZE(AO)
	fxch	%st(1)
	fld	%st
	FST	-6 * SIZE(AO)
	fxch	%st(3)
	fld	%st
	FST	-5 * SIZE(AO)

	FST	1 * SIZE(CO, LDC)
	FST	1 * SIZE(CO)
	FST	0 * SIZE(CO)
	FST	0 * SIZE(CO, LDC)
#endif

#ifndef LN
	addl	$2 * SIZE, CO
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$BASE_SHIFT, %eax
	leal	(AO, %eax, 2), AO
	leal	(BO, %eax, 2), BO
#endif

#ifdef LN
	subl	$2, KK
#endif

#ifdef LT
	addl	$2, KK
#endif

#ifdef RT
       movl	K, %eax
       sall	$1 + BASE_SHIFT, %eax
       addl	%eax, AORIG
#endif

	decl	I
	jne	.L11
	ALIGN_4

.L20:
	movl	 M, %eax
	andl	$1, %eax
	je	.L29
	ALIGN_4

.L21:
#ifdef LN
       movl	K, %eax
       sall	$0 + BASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$BASE_SHIFT, %eax
	movl	AORIG, AO
	leal	(AO, %eax, 1), AO
	leal	(B,  %eax, 2), BO
#else
	movl	B, BO
#endif

	fldz
	fldz

#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	sarl	$2, %eax
 	je	.L25
	ALIGN_4

.L22:
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(AO)

	FLD	 -8 * SIZE(AO)

	FLD	 -8 * SIZE(BO)
	fmul	 %st(1), %st
	faddp	 %st, %st(2)

	FLD	 -7 * SIZE(BO)
	fmulp	 %st, %st(1)
	faddp	 %st, %st(2)

	FLD	 -7 * SIZE(AO)

	FLD	 -6 * SIZE(BO)
	fmul	 %st(1), %st
	faddp	 %st, %st(2)

	FLD	 -5 * SIZE(BO)
	fmulp	 %st, %st(1)
	faddp	 %st, %st(2)

	FLD	 -6 * SIZE(AO)

	FLD	 -4 * SIZE(BO)
	fmul	 %st(1), %st
	faddp	 %st, %st(2)

	FLD	 -3 * SIZE(BO)
	fmulp	 %st, %st(1)
	faddp	 %st, %st(2)

	FLD	 -5 * SIZE(AO)

	FLD	 -2 * SIZE(BO)
	fmul	 %st(1), %st
	faddp	 %st, %st(2)

	FLD	 -1 * SIZE(BO)
	fmulp	 %st, %st(1)
	faddp	 %st, %st(2)

	addl	$4 * SIZE,AO
	addl	$8 * SIZE,BO

	decl	%eax
	jne	.L22
	ALIGN_4

.L25:
#if defined(LT) || defined(RN)
	movl	KK, %eax
#else
	movl	K,  %eax
	subl	KK, %eax
#endif
	and	$3,  %eax
	je	.L28
	ALIGN_4

.L26:
	FLD	 -8 * SIZE(AO)

	FLD	 -8 * SIZE(BO)
	fmul	 %st(1), %st
	faddp	 %st, %st(2)

	FLD	 -7 * SIZE(BO)
	fmulp	 %st, %st(1)
	faddp	 %st, %st(2)

	addl	$1 * SIZE,AO
	addl	$2 * SIZE,BO

	decl	%eax
	jne	 .L26
	ALIGN_4

.L28:
#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$2, %eax
#endif

	sall	$BASE_SHIFT, %eax

	movl	AORIG, AO
	leal	(AO, %eax, 1), AO
	leal	(B,  %eax, 2), BO
#endif

#if defined(LN) || defined(LT)
	FLD	-8 * SIZE(BO)
	fsubp	%st, %st(1)
	FLD	-7 * SIZE(BO)
	fsubp	%st, %st(2)
#else
	FLD	-8 * SIZE(AO)
	fsubp	%st, %st(1)
	FLD	-7 * SIZE(AO)
	fsubp	%st, %st(2)
#endif

#if defined(LN) || defined(LT)
       FLD	-8 * SIZE(AO)
       fmul	%st, %st(1)
       fmulp	%st, %st(2)
#endif

#ifdef RN
       FLD	-8 * SIZE(BO)
       fmulp	%st, %st(1)

       FLD	-7 * SIZE(BO)
       fmul	%st(1), %st

       fsubrp	%st, %st(2)

       FLD	-5 * SIZE(BO)
       fmulp	%st, %st(2)
#endif

#ifdef RT
       FLD	-5 * SIZE(BO)
       fmulp	%st, %st(2)

       FLD	-6 * SIZE(BO)
       fmul	%st(2), %st

       fsubrp	%st, %st(1)

       FLD	-8 * SIZE(BO)
       fmulp	%st, %st(1)
#endif

#ifdef LN
	subl	$1 * SIZE, CO
#endif

#if defined(LN) || defined(LT)
	fld	%st
	FST	-8 * SIZE(BO)
	fxch	%st(1)
	fld	%st
	FST	-7 * SIZE(BO)
#else
	fld	%st
	FST	-8 * SIZE(AO)
	fxch	%st(1)
	fld	%st
	FST	-7 * SIZE(AO)
#endif

	FST	0 * SIZE(CO, LDC)
	FST	0 * SIZE(CO)

#ifndef LN
	addl	$1 * SIZE, CO
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$BASE_SHIFT, %eax
	leal	(AO, %eax, 1), AO
	leal	(BO, %eax, 2), BO
#endif

#ifdef LN
	subl	$1, KK
#endif

#ifdef LT
	addl	$1, KK
#endif

#ifdef RT
       movl	K, %eax
       sall	$0 + BASE_SHIFT, %eax
       addl	%eax, AORIG
#endif
	ALIGN_4

.L29:
#ifdef LN
       movl	K, %eax
       sall	$BASE_SHIFT, %eax
       leal	(B, %eax, 2), B
#endif

#if defined(LT) || defined(RN)
	movl	BO, B
#endif

#ifdef RN
	addl	$2, KK
#endif

#ifdef RT
	subl	$2, KK
#endif

	decl	J
	jne	.L01
	ALIGN_4

.L999:
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	addl	$ARGS, %esp
	ret

	EPILOGUE
