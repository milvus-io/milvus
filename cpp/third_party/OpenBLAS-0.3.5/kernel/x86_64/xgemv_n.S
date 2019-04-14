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
#include "l2param.h"

#define P		32

#define STACKSIZE	80

#define ALPHA_R		 8 + STACKSIZE(%rsp)
#define ALPHA_I		24 + STACKSIZE(%rsp)
#define OLD_INCX	40 + STACKSIZE(%rsp)
#define OLD_Y		48 + STACKSIZE(%rsp)
#define OLD_INCY	56 + STACKSIZE(%rsp)
#define BUFFER		64 + STACKSIZE(%rsp)

#define PLDA_M	        56            (%rsp)
#define IS		64	      (%rsp)

#define M	  %rdi
#define N	  %rsi
#define A	  %rcx
#define LDA	  %r8
#define X	  %r9
#define INCX	  %rdx
#define Y	  %rbp
#define INCY	  %r10

#define TEMP	%rax
#define I	%rax
#define J	%r11
#define A1	%r12
#define X1	%r13
#define Y1	%r14
#define XP	%r15
#define MIN_N	%rbx


	PROLOGUE
	PROFCODE

	subq	$STACKSIZE, %rsp
	movq	%rbx,  0(%rsp)
	movq	%rbp,  8(%rsp)
	movq	%r12, 16(%rsp)
	movq	%r13, 24(%rsp)
	movq	%r14, 32(%rsp)
	movq	%r15, 40(%rsp)

	movq	OLD_INCX,   INCX
	movq	OLD_Y,      Y
	movq	OLD_INCY,   INCY

	FLD	ALPHA_I
	FLD	ALPHA_R

	salq	$ZBASE_SHIFT, INCX
	salq	$ZBASE_SHIFT, INCY

	movq	$0, IS

	test	M, M
	jle	.L79
	test	N, N
	jle	.L79

	movq	LDA, %rax
	imulq	$P,  %rax		# P * lda
	subq	M   ,%rax		# P * lda - m
	salq	$ZBASE_SHIFT, %rax
	movq	%rax, PLDA_M

	salq	$ZBASE_SHIFT, LDA
	ALIGN_2

.L32:
	movq	$P,  %rax
	movq	N,    MIN_N
	subq	IS,   MIN_N
	cmpq	%rax, MIN_N
	cmovg	%rax, MIN_N

	movq	IS, XP
	salq	$ZBASE_SHIFT, XP
	leaq	(X,XP, 1), XP

	cmpq	$2 * SIZE, INCX
	je	.L34

	movq	BUFFER, XP
	movq	XP,     X1

	movq	MIN_N, I
	sarq	$1,    I
	jle	.L35
	ALIGN_2

.L36:
	FLD	0 * SIZE(X)
	FLD	1 * SIZE(X)
	addq	INCX,X		# x += incx
	FLD	0 * SIZE(X)
	FLD	1 * SIZE(X)
	addq	INCX,X		# x += incx

	FST	3 * SIZE(X1)
	FST	2 * SIZE(X1)
	FST	1 * SIZE(X1)
	FST	0 * SIZE(X1)

	addq	$4 * SIZE, X1		# xp += 4
	decq	I
	jg	.L36
	ALIGN_3

.L35:
	movq	MIN_N, I
	andq	$1,    I
	jle	.L34

	FLD	0 * SIZE(X)
	FLD	1 * SIZE(X)
	addq	INCX,X		# x += incx
	FST	1 * SIZE(X1)
	FST	0 * SIZE(X1)
	ALIGN_3

/* Main Routine */
.L34:
	movq	 Y, Y1		# c_offset
	movq	 M, J		# j = m
	ALIGN_3

.L61:
	movq	A, A1			# a_offset = a
	addq	$2 * SIZE, A		# a++

	fldz
	fldz
	fldz
	fldz

	movq	XP, X1
	FLD	(X1)			#  bt1 = *(b_offset + 0)

	movq	MIN_N, I
	sarq	$1,    I
	jle	.L64
	ALIGN_3

.L65:
	FLD	 0 * SIZE(A1)		# at1  = *(a_offset + 0)
	fmul	%st(1)			# at1 *= bt1
	faddp	%st, %st(2)		# ct1 += at1

	FLD	 1 * SIZE(A1)		# bt1 *= *(a_offset + 1)
	fmulp	 %st, %st(1)
#ifndef CONJ
	faddp	%st, %st(2)		# ct2 += bt1
#else
	fsubrp	%st, %st(2)		# ct2 -= bt1
#endif
	FLD	 1 * SIZE(X1)		# bt1  = *(b_offset + 1)

	FLD	 0 * SIZE(A1)		# at1  = *(a_offset + 0)
	fmul	%st(1)			# at1 *= bt1
	faddp	%st, %st(4)		# ct3 += at1

	FLD	 1 * SIZE(A1)		# bt1 *= *(a_offset + 1)
	fmulp	 %st, %st(1)
	faddp	%st, %st(4)		# ct4 += bt1
	FLD	 2 * SIZE(X1)		# bt1  = *(b_offset + 2)

	addq	$2 * SIZE, X1		# b_offset += 2
	addq	LDA,  A1		# a_offset += lda

	FLD	 0 * SIZE(A1)		# at1  = *(a_offset + 0)
	fmul	%st(1)			# at1 *= bt1
	faddp	%st, %st(2)		# ct1 += at1

	FLD	 1 * SIZE(A1)		# bt1 *= *(a_offset + 1)
	fmulp	 %st, %st(1)
#ifndef CONJ
	faddp	%st, %st(2)		# ct2 += bt1
#else
	fsubrp	%st, %st(2)		# ct2 -= bt1
#endif
	FLD	 1 * SIZE(X1)		# bt1  = *(b_offset + 1)

	FLD	 0 * SIZE(A1)		# at1  = *(a_offset + 0)
	fmul	%st(1)			# at1 *= bt1
	faddp	%st, %st(4)		# ct3 += at1

	FLD	 1 * SIZE(A1)		# bt1 *= *(a_offset + 1)
	fmulp	 %st, %st(1)
	faddp	%st, %st(4)		# ct4 += bt1
	FLD	 2 * SIZE(X1)		# bt1  = *(b_offset + 2)

	addq	$2 * SIZE, X1		# b_offset += 2
	addq	LDA,  A1		# a_offset += lda

	decq	I
	jg	.L65

.L64:
	movq	MIN_N, I
	andq	$1, I
	jle	.L70
	ALIGN_2

.L71:
	FLD	 0 * SIZE(A1)		# at1  = *(a_offset + 0)
	fmul	%st(1)			# at1 *= bt1
	faddp	%st, %st(2)		# ct1 += at1

	FLD	 1 * SIZE(A1)		# bt1 *= *(a_offset + 1)
	fmulp	 %st, %st(1)
#ifndef CONJ
	faddp	%st, %st(2)		# ct2 += bt1
#else
	fsubrp	%st, %st(2)		# ct2 -= bt1
#endif
	FLD	 1 * SIZE(X1)		# bt1  = *(b_offset + 1)

	FLD	 0 * SIZE(A1)		# at1  = *(a_offset + 0)
	fmul	%st(1)			# at1 *= bt1
	faddp	%st, %st(4)		# ct3 += at1

	FLD	 1 * SIZE(A1)		# bt1 *= *(a_offset + 1)
	fmulp	 %st, %st(1)
	faddp	%st, %st(4)		# ct4 += bt1
	fldz
	ALIGN_2

.L70:
	ffreep	%st(0)

#ifndef XCONJ
#ifndef CONJ
	fsubp	%st, %st(3)
	faddp	%st, %st(1)
#else
	faddp	%st, %st(3)
	faddp	%st, %st(1)
#endif
#else
#ifndef CONJ
	faddp	%st, %st(3)
	fsubp	%st, %st(1)
#else
	fsubp	%st, %st(3)
	fsubp	%st, %st(1)
#endif
#endif

	fld	%st(0)		# ct4 = ct2
	fmul	%st(4)
	fld	%st(2)
	fmul	%st(4)
	fsubp	%st, %st(1)

	FLD	0 * SIZE(Y1)
	faddp	 %st, %st(1)
	FST	0 * SIZE(Y1)

	fmul	%st(2)
	fxch	%st(1)
	fmul	%st(3)
	faddp	%st, %st(1)

	FLD	1 * SIZE(Y1)
	faddp	 %st, %st(1)
	FST	1 * SIZE(Y1)

	addq	INCY, Y1
	decq	J
	jg	.L61

.L60:
	addq	PLDA_M, A
	addq	$P, IS
	cmpq	N,  IS
	jl	.L32

.L79:
	ffreep	%st
	ffreep	%st

	movq	  0(%rsp), %rbx
	movq	  8(%rsp), %rbp
	movq	 16(%rsp), %r12
	movq	 24(%rsp), %r13
	movq	 32(%rsp), %r14
	movq	 40(%rsp), %r15
	addq	$STACKSIZE, %rsp
	ret
	EPILOGUE
