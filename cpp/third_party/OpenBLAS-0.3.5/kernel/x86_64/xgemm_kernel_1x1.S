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

#define M	ARG1
#define N	ARG2
#define K	ARG3
#define A	ARG4
#define B	ARG5
#define C	ARG6
#define LDC	%r10

#define I	%r12
#define J	%r13
#define AO	%r14
#define BO	%r15
#define	CO	%rbp

#define STACKSIZE 64

#define ALPHA_R	 8 + STACKSIZE(%rsp)
#define ALPHA_I	24 + STACKSIZE(%rsp)
#define OFFSET	48 + STACKSIZE(%rsp)

#define KK	%r11
#define KKK	 48(%rsp)

#ifdef OPTERON
#define PREFETCH	prefetch
#define PREFETCHW	prefetchw
#else
#define PREFETCH	prefetcht0
#define PREFETCHW	prefetcht0
#endif

#define PREFETCHSIZE (5 + 4 * 10)

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define ADD1	  faddp
#define ADD2	  fsubrp
#define ADD3	  faddp
#define ADD4	  faddp
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define ADD1	  faddp
#define ADD2	  faddp
#define ADD3	  fsubrp
#define ADD4	  faddp
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define ADD1	  faddp
#define ADD2	  faddp
#define ADD3	  faddp
#define ADD4	  fsubrp
#else
#define ADD1	  faddp
#define ADD2	  fsubrp
#define ADD3	  fsubrp
#define ADD4	  fsubrp
#endif

	PROLOGUE
	PROFCODE

	subq	$STACKSIZE, %rsp
	movq	%rbx,  0(%rsp)
	movq	%rbp,  8(%rsp)
	movq	%r12, 16(%rsp)
	movq	%r13, 24(%rsp)
	movq	%r14, 32(%rsp)
	movq	%r15, 40(%rsp)

	movq	40 + STACKSIZE(%rsp), LDC

#if defined(TRMMKERNEL) && !defined(LEFT)
	movq	OFFSET, %rax
	negq	%rax
	movq	%rax, KK
#endif

	addq	$8 * SIZE, A
	addq	$8 * SIZE, B

	salq	$ZBASE_SHIFT, LDC

	cmpq	$0, M
	jle	.L999

	movq	N,   %rax
	movq	%rax, J
	testq	%rax, %rax
	jle	.L999
	ALIGN_4

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	A, AO

	movq	C, CO
	addq	LDC, C

	movq	M,  I
	ALIGN_4

.L11:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	B, BO
#else
	movq	KK,   %rax
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B, %rax,  1), BO
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

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$1, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif
	sarq	$2, %rax
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

	addq	$8 * SIZE,AO
	addq	$8 * SIZE,BO

	decq	%rax
	jne	.L12
	ALIGN_4

.L15:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	and	$3,  %rax
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

	addq	$2 * SIZE,AO
	addq	$2 * SIZE,BO

	decq	%rax
	jne	 .L16
	ALIGN_4

.L18:
	faddp	%st, %st(3)
	faddp	%st, %st(1)

#ifndef TRMMKERNEL
	FLD	ALPHA_R
	fld	%st
	fmul	%st(2), %st
	fxch	%st(1)
	fmul	%st(3), %st

	FLD	ALPHA_I
	fmul	%st, %st(3)
	fmulp	%st, %st(4)

	fsubp	%st, %st(2)
	faddp	%st, %st(2)

	FLD	0 * SIZE(CO)
	faddp	%st, %st(1)
	FST	0 * SIZE(CO)

	FLD	1 * SIZE(CO)
	faddp	%st, %st(1)
	FST	1 * SIZE(CO)
#else
	FST	1 * SIZE(CO)
	FST	0 * SIZE(CO)
#endif

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	salq	$ZBASE_SHIFT, %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 1), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif

	addq	$2 * SIZE, CO
	decq	I
	jne	.L11

#if defined(TRMMKERNEL) && !defined(LEFT)
	addq	$1, KK
#endif

	movq	BO, B
	decq	J
	jne	.L01
	ALIGN_4

.L999:
	EMMS

	movq	  0(%rsp), %rbx
	movq	  8(%rsp), %rbp
	movq	 16(%rsp), %r12
	movq	 24(%rsp), %r13
	movq	 32(%rsp), %r14
	movq	 40(%rsp), %r15
	addq	$STACKSIZE, %rsp
	ret

	EPILOGUE
