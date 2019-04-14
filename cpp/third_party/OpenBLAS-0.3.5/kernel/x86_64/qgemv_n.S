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

#define ALPHA		 8 + STACKSIZE(%rsp)
#define OLD_INCX	24 + STACKSIZE(%rsp)
#define OLD_Y		32 + STACKSIZE(%rsp)
#define OLD_INCY	40 + STACKSIZE(%rsp)
#define BUFFER		48 + STACKSIZE(%rsp)

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
/* #define BUFFER	%r15 */
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

	FLD	ALPHA

	salq	$BASE_SHIFT, INCX
	salq	$BASE_SHIFT, INCY

	movq	$0, IS

	test	M, M
	jle	.L79			# goto END
	test	N, N
	jle	.L79			# goto END

	movq	LDA, %rax
	imulq	$P,  %rax		# P * lda
	subq	M   ,%rax		# P * lda - m
	salq	$BASE_SHIFT, %rax
	movq	%rax, PLDA_M

	salq	$BASE_SHIFT, LDA
	ALIGN_2

.L32:
	movq	$P,  %rax
	movq	N,    MIN_N
	subq	IS,   MIN_N
	cmpq	%rax, MIN_N
	cmovg	%rax, MIN_N

	movq	IS, XP
	salq	$BASE_SHIFT, XP
	leaq	(X,XP, 1), XP

	cmpq	$SIZE, INCX
	je	.L34			# if incx == 1 goto L34

	movq	BUFFER, XP
	movq	XP,     X1

	movq	MIN_N, I
	sarq	$2,I
	jle	.L35
	ALIGN_2

.L36:
	FLD	(X)
	addq	INCX,X
	FLD	(X)
	addq	INCX,X
	FLD	(X)
	addq	INCX,X
	FLD	(X)
	addq	INCX,X

	FST	3 * SIZE(X1)
	FST	2 * SIZE(X1)
	FST	1 * SIZE(X1)
	FST	0 * SIZE(X1)

	addq	$4 * SIZE, X1
	decq	I
	jg	.L36
	ALIGN_3

.L35:
	movq	MIN_N, I
	andq	$3,    I
	jle	.L34
	ALIGN_2

.L42:
	FLD	(X)
	addq	INCX,  X
	FST	(X1)
	addq	$SIZE, X1
	decq	I
	jg	.L42
	ALIGN_3

/* Main Routine */
.L34:
	movq	 Y, Y1
	movq	 M, J
	sarq	$2, J
	jle	.L47
	ALIGN_2

.L48:
	movq	A, A1			# a_offset = a
	fldz
	addq	$4 * SIZE, A		# a += 4
	fldz
	movq	XP, X1		# b_offset = xp
	fldz
	movq	MIN_N, I		# i = min_n
	fldz
	FLD	(X1)			# bt1 = b_offset
	sarq	$1,    I
	jle	.L51
	ALIGN_2

.L80:
	FLD	0 * SIZE(A1)		# at1  = *(a_offset + 0)
	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(2)		# ct1 += at1
	FLD	1 * SIZE(A1)		# at1  = *(a_offset + 1)

	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(3)		# ct2 += at1
	FLD	2 * SIZE(A1)		# at1  = *(a_offset + 2)

	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(4)		# ct3 += at1
	FLD	3 * SIZE(A1)		# bt1 *= *(a_offset + 3)

	fmulp	%st, %st(1)
	faddp	%st, %st(4)		# ct4 += at1
	FLD	1 * SIZE(X1)		# bt1 = b_offset

	addq	LDA, A1			# a_offset += lda
	FLD	0 * SIZE(A1)		# at1  = *(a_offset + 0)

	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(2)		# ct1 += at1
	FLD	1 * SIZE(A1)		# at1  = *(a_offset + 1)

	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(3)		# ct2 += at1
	FLD	2 * SIZE(A1)		# at1  = *(a_offset + 2)

	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(4)		# ct3 += at1
	FLD	3 * SIZE(A1)		# bt1 *= *(a_offset + 3)

	fmulp	%st, %st(1)
	addq	LDA, A1
	faddp	%st, %st(4)		# ct4 += at1

	FLD	2 * SIZE(X1)		# bt1 = b_offset
	addq	$2 * SIZE, X1		# b_offset += 2

	decq	I
	jg	.L80

.L51:
	movq	MIN_N, I
	andq	$1,  I
	je	.L57

	FLD	0 * SIZE(A1)		# at1  = *(a_offset + 0)
	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(2)		# ct1 += at1

	FLD	1 * SIZE(A1)		# at1  = *(a_offset + 1)
	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(3)		# ct2 += at1

	FLD	2 * SIZE(A1)		# at1  = *(a_offset + 2)
	fmul	%st(1), %st		# at1 *= bt1
	faddp	%st, %st(4)		# ct3 += at1

	FLD	3 * SIZE(A1)		# bt1 *= *(a_offset + 3)
	fmulp	%st, %st(1)
	faddp	%st, %st(4)		# ct4 += at1
	fldz
	ALIGN_2

.L57:
	ffreep	%st(0)

	fxch	%st(4)
	fmul	%st, %st(4)
	fmul	%st, %st(1)
	fmul	%st, %st(2)
	fmul	%st, %st(3)
	fxch	%st(4)

	FLD	(Y1)
	faddp	%st, %st(1)
	FST	(Y1)
	addq	INCY, Y1

	FLD	(Y1)
	faddp	%st, %st(1)
	FST	(Y1)
	addq	INCY, Y1

	FLD	(Y1)
	faddp	%st, %st(1)
	FST	(Y1)
	addq	INCY, Y1

	FLD	(Y1)
	faddp	%st, %st(1)
	FST	(Y1)
	addq	INCY, Y1

	decq	J		# j --
	jg	.L48
	ALIGN_3

.L47:
	movq	M,  J
	andq	$3, J		# j = (m & 3)
	jle	.L60
	ALIGN_2

.L61:
	movq	A, A1			# a_offset = a
	fldz
	addq	$SIZE, A		# a++
	fldz
	movq	XP, X1
	fldz
	fldz
	movq	MIN_N, I
	sarq	$3,  I
	jle	.L64
	ALIGN_2

.L65:
	FLD	0 * SIZE(X1)
	FLD	(A1)
	fmulp	%st, %st(1)
	faddp	%st, %st(1)
	addq	LDA, A1

	FLD	1 * SIZE(X1)
	FLD	(A1)
	fmulp	%st, %st(1)
	faddp	%st, %st(2)
	addq	LDA ,A1

	FLD	2 * SIZE(X1)
	FLD	(A1)
	fmulp	%st, %st(1)
	faddp	%st, %st(3)
	addq	LDA, A1

	FLD	3 * SIZE(X1)
	FLD	(A1)
	fmulp	%st, %st(1)
	faddp	%st, %st(4)
	addq	LDA, A1

	FLD	4 * SIZE(X1)
	FLD	(A1)
	fmulp	%st, %st(1)
	faddp	%st,%st(1)
	addq	LDA, A1

	FLD	5 * SIZE(X1)
	FLD	(A1)
	fmulp	%st, %st(1)
	faddp	%st, %st(2)
	addq	LDA, A1

	FLD	6 * SIZE(X1)
	FLD	(A1)
	fmulp	%st, %st(1)
	faddp	%st,%st(3)
	addq	LDA, A1

	FLD	7 * SIZE(X1)
	FLD	(A1)
	fmulp	%st, %st(1)
	faddp	%st,%st(4)
	addq	LDA, A1

	addq	$8 * SIZE, X1
	decq	I
	jg	.L65

.L64:
	movq	MIN_N,I
	andq	$7, I
	jle	.L70
	ALIGN_2

.L71:
	FLD	(X1)
	addq	$SIZE, X1
	FLD	(A1)
	fmulp	%st, %st(1)
	addq	LDA,  A1	 # a_offset += lda
	faddp	%st, %st(1)
	decq	I
	jg	.L71
	ALIGN_2

.L70:
	faddp	%st, %st(1)
	faddp	%st, %st(1)
	faddp	%st, %st(1)

	fmul	%st(1), %st
	FLD	(Y1)
	faddp	%st, %st(1)
	FST	(Y1)
	addq	INCY, Y1
	decq	J
	jg	.L61

.L60:
	addq	PLDA_M, A
	addq	$P, IS
	cmpq	N,  IS
	jl	.L32

.L79:
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
