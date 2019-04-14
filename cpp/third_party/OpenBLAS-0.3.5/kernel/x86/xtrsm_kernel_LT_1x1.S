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
#define ALPHA_R	16 + STACK + ARGS(%esp)
#define ALPHA_I	32 + STACK + ARGS(%esp)
#define A	48 + STACK + ARGS(%esp)
#define ARG_B	52 + STACK + ARGS(%esp)
#define C	56 + STACK + ARGS(%esp)
#define ARG_LDC	60 + STACK + ARGS(%esp)
#define OFFSET	64 + STACK + ARGS(%esp)

#define I	%esi
#define B	%ebx
#define CO	%edi
#define AO	%edx
#define BO	%ecx
#define LDC	%ebp

#ifndef CONJ
#define ADD1	  faddp
#define ADD2	  fsubrp
#define ADD3	  faddp
#define ADD4	  faddp
#elif defined(LN) || defined(LT)
#define ADD1	  faddp
#define ADD2	  faddp
#define ADD3	  fsubrp
#define ADD4	  faddp
#else
#define ADD1	  faddp
#define ADD2	  faddp
#define ADD3	  faddp
#define ADD4	  fsubrp
#endif

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

	sall	$ZBASE_SHIFT, LDC

	addl	$8 * SIZE, A
	addl	$8 * SIZE, B

#ifdef LN
       movl	M, %eax
       sall	$ZBASE_SHIFT, %eax
       addl	%eax, C
       imull	K, %eax
       addl	%eax, A
#endif

#ifdef RT
       movl	N, %eax
       sall	$ZBASE_SHIFT, %eax
       imull	K, %eax
       addl	%eax, B

       movl	N, %eax
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

	cmpl	$0, M
	jle	.L999

	movl	N,   %eax
	movl	%eax, J
	testl	%eax, %eax
	jle	.L999
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
	sall	$ZBASE_SHIFT, %eax
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
	ALIGN_4

.L11:
#ifdef LN
       movl	K, %eax
       sall	$ZBASE_SHIFT, %eax
       subl	%eax, AORIG
#endif

#if defined(LN) || defined(RT)
	movl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
	movl	AORIG, AO
	leal	(AO, %eax, 1), AO
	leal	(B,  %eax, 1), BO
#else
	movl	B, BO
#endif

	fldz
	fldz
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
 	je	.L15
	ALIGN_4

.L12:
	PREFETCH	(PREFETCHSIZE + 0) * SIZE(AO)

	FLD	 -8 * SIZE(AO)

	FLD	 -8 * SIZE(BO)
	fld	 %st(1)
	fmul	 %st(1), %st
	ADD1	 %st, %st(3)

	FLD	 -7 * SIZE(BO)
	fmul	 %st, %st(2)

	FLD	 -7 * SIZE(AO)
	fmul	 %st, %st(2)
	fmulp	 %st, %st(1)

	ADD2	 %st, %st(6)
	ADD3	 %st, %st(3)
	ADD4	 %st, %st(3)

	FLD	 -6 * SIZE(AO)

	FLD	 -6 * SIZE(BO)
	fld	 %st(1)
	fmul	 %st(1), %st
	ADD1	 %st, %st(3)

	FLD	 -5 * SIZE(BO)
	fmul	 %st, %st(2)

	FLD	 -5 * SIZE(AO)
	fmul	 %st, %st(2)
	fmulp	 %st, %st(1)

	ADD2	 %st, %st(6)
	ADD3	 %st, %st(3)
	ADD4	 %st, %st(3)

	PREFETCH	(PREFETCHSIZE + 4) * SIZE(AO)

	FLD	 -4 * SIZE(AO)

	FLD	 -4 * SIZE(BO)
	fld	 %st(1)
	fmul	 %st(1), %st
	ADD1	 %st, %st(3)

	FLD	 -3 * SIZE(BO)
	fmul	 %st, %st(2)

	FLD	 -3 * SIZE(AO)
	fmul	 %st, %st(2)
	fmulp	 %st, %st(1)

	ADD2	 %st, %st(6)
	ADD3	 %st, %st(3)
	ADD4	 %st, %st(3)

	FLD	 -2 * SIZE(AO)

	FLD	 -2 * SIZE(BO)
	fld	 %st(1)
	fmul	 %st(1), %st
	ADD1	 %st, %st(3)

	FLD	 -1 * SIZE(BO)
	fmul	 %st, %st(2)

	FLD	 -1 * SIZE(AO)
	fmul	 %st, %st(2)
	fmulp	 %st, %st(1)

	ADD2	 %st, %st(6)
	ADD3	 %st, %st(3)
	ADD4	 %st, %st(3)

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
	ADD1	 %st, %st(3)

	FLD	 -7 * SIZE(BO)
	fmul	 %st, %st(2)

	FLD	 -7 * SIZE(AO)
	fmul	 %st, %st(2)
	fmulp	 %st, %st(1)

	ADD2	 %st, %st(6)
	ADD3	 %st, %st(3)
	ADD4	 %st, %st(3)

	addl	$2 * SIZE,AO
	addl	$2 * SIZE,BO

	decl	%eax
	jne	 .L16
	ALIGN_4

.L18:
	faddp	%st, %st(3)
	faddp	%st, %st(1)

	fxch	%st(1)

#if defined(LN) || defined(RT)
	movl	KK, %eax
#ifdef LN
	subl	$1, %eax
#else
	subl	$1, %eax
#endif

	sall	$ZBASE_SHIFT, %eax

	movl	AORIG, AO
	leal	(AO, %eax, 1), AO
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

#if defined(LN) || defined(LT)
	FLD	-8 * SIZE(AO)
	fmul	%st(1), %st
	FLD	-8 * SIZE(AO)
	fmul	%st(3), %st
	FLD	-7 * SIZE(AO)
	fmulp	%st, %st(3)
	FLD	-7 * SIZE(AO)
	fmulp	%st, %st(4)
#endif

#if defined(RN) || defined(RT)
	FLD	-8 * SIZE(BO)
	fmul	%st(1), %st
	FLD	-8 * SIZE(BO)
	fmul	%st(3), %st
	FLD	-7 * SIZE(BO)
	fmulp	%st, %st(3)
	FLD	-7 * SIZE(BO)
	fmulp	%st, %st(4)
#endif

#ifndef CONJ
	faddp	%st, %st(2)
	fsubp	%st, %st(2)
#else
	fsubp	%st, %st(2)
	faddp	%st, %st(2)
#endif

#if defined(LN) || defined(LT)
	fld	%st
	FST	-7 * SIZE(BO)
	fxch	%st(1)
	fld	%st
	FST	-8 * SIZE(BO)
#else
	fld	%st
	FST	-7 * SIZE(AO)
	fxch	%st(1)
	fld	%st
	FST	-8 * SIZE(AO)
#endif

#ifdef LN
	subl	$2 * SIZE, CO
#endif

	FST	0 * SIZE(CO)
	FST	1 * SIZE(CO)

#ifndef LN
	addl	$2 * SIZE, CO
#endif

#if defined(LT) || defined(RN)
	movl	K,  %eax
	subl	KK, %eax
	sall	$ZBASE_SHIFT, %eax
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
       sall	$ZBASE_SHIFT, %eax
       addl	%eax, AORIG
#endif

	decl	I
	jne	.L11

#ifdef LN
       movl	K, %eax
       sall	$ZBASE_SHIFT, %eax
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
