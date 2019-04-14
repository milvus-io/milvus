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
#include "version.h"

#define STACKSIZE     64
#define PREFETCHSIZE  32

#define M	$16
#define N	$17
#define A	$21
#define	LDA	$18

#define X	$19
#define	INCX	$20
#define Y	$22
#define	INCY	$23

#define BUFFER	$24

#define I	$25
#define J	$27

#define Y1	$4
#define A1	$5
#define A2	$6

#define	alpha_r	$f19
#define	alpha_i	$f20

#define	alpha1	$f0
#define	alpha2	$f1
#define	alpha3	$f10
#define	alpha4	$f11

#define	y0	$f12
#define	y1	$f13
#define	y2	$f14
#define	y3	$f15

#define	y4	$f16
#define	y5	$f17
#define	y6	$f18
#define	y7	$f21

#define	a0	$f22
#define	a1	$f23
#define	a2	$f24
#define	a3	$f25
#define	a4	$f26
#define	a5	$f27
#define	a6	$f28
#define	a7	$f29

#define	t0	$f2
#define	t1	$f3
#define	t2	$f4
#define	t3	$f5

#if   !defined(CONJ) && !defined(XCONJ)
#define ADD1	ADD
#define ADD2	ADD
#define ADD3	SUB
#define ADD4	ADD
#elif  defined(CONJ) && !defined(XCONJ)
#define ADD1	ADD
#define ADD2	SUB
#define ADD3	ADD
#define ADD4	ADD
#elif !defined(CONJ) &&  defined(XCONJ)
#define ADD1	ADD
#define ADD2	ADD
#define ADD3	ADD
#define ADD4	SUB
#else
#define ADD1	ADD
#define ADD2	SUB
#define ADD3	SUB
#define ADD4	SUB
#endif

	PROLOGUE

	lda	$sp,  -STACKSIZE($sp)
	ldq	LDA,     0 + STACKSIZE($sp)
	ldq	X,       8 + STACKSIZE($sp)
	ldq	INCX,   16 + STACKSIZE($sp)
	ldq	Y,      24 + STACKSIZE($sp)
	ldq	INCY,   32 + STACKSIZE($sp)
	ldq	BUFFER, 40 + STACKSIZE($sp)

	stt	$f2,    0($sp)
	stt	$f3,    8($sp)
	stt	$f4,   16($sp)
	stt	$f5,   24($sp)
	stt	$f6,   32($sp)
	stt	$f7,   40($sp)
	stt	$f8,   48($sp)
	stt	$f9,   56($sp)

	PROFCODE

	cmple	M, 0, $0
	sll	INCX, ZBASE_SHIFT, INCX
	cmple	N, 0, $1
	sll	INCY, ZBASE_SHIFT, INCY

	or	$0, $1, $0
	bne	$0,  $L999

	cmpeq	INCY, 2 * SIZE, $0
	sll	LDA, ZBASE_SHIFT,LDA
	bne	$0, $L10

	mov	BUFFER, Y1

	mov	Y, BUFFER
	mov	Y1, Y

	sra	M, 2, I
	ble	I, $L05
	.align 4

$L02:
	ST	$f31,  0 * SIZE(Y1)
	ST	$f31,  1 * SIZE(Y1)
	ST	$f31,  2 * SIZE(Y1)
	ST	$f31,  3 * SIZE(Y1)
	ST	$f31,  4 * SIZE(Y1)
	ST	$f31,  5 * SIZE(Y1)
	ST	$f31,  6 * SIZE(Y1)
	ST	$f31,  7 * SIZE(Y1)

	lda	Y1,    8 * SIZE(Y1)
	lda	I, -1(I)
	bgt	I, $L02
	.align 4

$L05:
	and	M, 3, I
	ble	I, $L10
	.align 4

$L06:
	ST	$f31,  0 * SIZE(Y1)
	ST	$f31,  1 * SIZE(Y1)
	addq	Y1, 2 * SIZE, Y1

	lda	I, -1(I)
	bgt	I, $L06
	.align 4

$L10:
	sra	N, 1, J
	ble	J,  $L20
	.align 4

$L11:
	LD	alpha1,  0 * SIZE(X)
	LD	alpha2,  1 * SIZE(X)
	addq	X, INCX, X
	LD	alpha3,  0 * SIZE(X)
	LD	alpha4,  1 * SIZE(X)
	addq	X, INCX, X

	MUL	alpha_r, alpha1, y0
	MUL	alpha_r, alpha2, y1
	MUL	alpha_r, alpha3, y2
	MUL	alpha_r, alpha4, y3

	MUL	alpha_i, alpha2, t0
	mov	A, A1
	MUL	alpha_i, alpha1, t1
	addq	A,  LDA, A2
	MUL	alpha_i, alpha4, t2
	addq	A2, LDA, A
	MUL	alpha_i, alpha3, t3
	mov	Y, Y1

#ifndef XCONJ
	SUB	y0, t0, alpha1
	ADD	y1, t1, alpha2
	SUB	y2, t2, alpha3
	ADD	y3, t3, alpha4
#else
	ADD	y0, t0, alpha1
	SUB	y1, t1, alpha2
	ADD	y2, t2, alpha3
	SUB	y3, t3, alpha4
#endif

	ldl	$31, 4 * SIZE(X)

	sra	M,  2, I
	ble	I,  $L15

	LD	a0,   0 * SIZE(A1)
	LD	a1,   1 * SIZE(A1)
	LD	a2,   2 * SIZE(A1)
	LD	a3,   3 * SIZE(A1)

	LD	a4,   0 * SIZE(A2)
	LD	a5,   1 * SIZE(A2)
	LD	a6,   2 * SIZE(A2)
	LD	a7,   3 * SIZE(A2)

	MUL	alpha1, a0, t0
	LD	y0,   0 * SIZE(Y1)
	MUL	alpha1, a1, t1
	LD	y1,   1 * SIZE(Y1)

	MUL	alpha1, a2, t2
	LD	y2,   2 * SIZE(Y1)
	MUL	alpha1, a3, t3
	LD	y3,   3 * SIZE(Y1)

	ADD1	y0, t0, y0
	unop
	MUL	alpha3, a4, t0
	LD	y4,   4 * SIZE(Y1)

	ADD2	y1, t1, y1
	unop
	MUL	alpha3, a5, t1
	LD	y5,   5 * SIZE(Y1)

	ADD1	y2, t2, y2
	unop
	MUL	alpha3, a6, t2
	LD	y6,   6 * SIZE(Y1)

	ADD2	y3, t3, y3
	unop
	MUL	alpha3, a7, t3
	LD	y7,   7 * SIZE(Y1)

	ADD1	y0, t0, y0
	unop
	MUL	alpha2, a1, t0
	LD	a1,   5 * SIZE(A1)

	ADD2	y1, t1, y1
	unop
	MUL	alpha2, a0, t1
	LD	a0,   4 * SIZE(A1)

	ADD1	y2, t2, y2
	unop
	MUL	alpha2, a3, t2
	LD	a3,   7 * SIZE(A1)

	ADD2	y3, t3, y3
	unop
	MUL	alpha2, a2, t3
	LD	a2,   6 * SIZE(A1)

	ADD3	y0, t0, y0
	unop
	MUL	alpha4, a5, t0
	LD	a5,   5 * SIZE(A2)

	ADD4	y1, t1, y1
	unop
	MUL	alpha4, a4, t1
	LD	a4,   4 * SIZE(A2)

	ADD3	y2, t2, y2
	unop
	MUL	alpha4, a7, t2
	LD	a7,   7 * SIZE(A2)

	ADD4	y3, t3, y3
	unop
	MUL	alpha4, a6, t3
	LD	a6,   6 * SIZE(A2)

	ADD3	y0, t0, y0
	MUL	alpha1, a0, t0
	ADD4	y1, t1, y1
	MUL	alpha1, a1, t1

	ADD3	y2, t2, y2
	unop
	MUL	alpha1, a2, t2
	unop

	ADD4	y3, t3, y3
	lda	I,   -1(I)
	MUL	alpha1, a3, t3
	ble	I, $L13
	.align 4

$L12:
	ADD1	y4, t0, y4
	ST	y0,   0 * SIZE(Y1)
	MUL	alpha3, a4, t0
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(A1)

	ADD2	y5, t1, y5
	ST	y1,   1 * SIZE(Y1)
	MUL	alpha3, a5, t1
	lda	I,   -1(I)

	ADD1	y6, t2, y6
	ST	y2,   2 * SIZE(Y1)
	MUL	alpha3, a6, t2
	unop

	ADD2	y7, t3, y7
	ST	y3,   3 * SIZE(Y1)
	MUL	alpha3, a7, t3
	unop

	ADD1	y4, t0, y4
	unop
	MUL	alpha2, a1, t0
	LD	a1,   9 * SIZE(A1)

	ADD2	y5, t1, y5
	unop
	MUL	alpha2, a0, t1
	LD	a0,   8 * SIZE(A1)

	ADD1	y6, t2, y6
	unop
	MUL	alpha2, a3, t2
	LD	a3,  11 * SIZE(A1)

	ADD2	y7, t3, y7
	unop
	MUL	alpha2, a2, t3
	LD	a2,  10 * SIZE(A1)

	ADD3	y4, t0, y4
	lds	$f31, (PREFETCHSIZE + 0) * SIZE(Y1)
	MUL	alpha4, a5, t0
	LD	a5,   9 * SIZE(A2)

	ADD4	y5, t1, y5
	unop
	MUL	alpha4, a4, t1
	LD	a4,   8 * SIZE(A2)

	ADD3	y6, t2, y6
	unop
	MUL	alpha4, a7, t2
	LD	a7,  11 * SIZE(A2)

	ADD4	y7, t3, y7
	unop
	MUL	alpha4, a6, t3
	LD	a6,  10 * SIZE(A2)

	ADD3	y4, t0, y4
	unop
	MUL	alpha1, a0, t0
	LD	y0,   8 * SIZE(Y1)

	ADD4	y5, t1, y5
	unop
	MUL	alpha1, a1, t1
	LD	y1,   9 * SIZE(Y1)

	ADD3	y6, t2, y6
	unop
	MUL	alpha1, a2, t2
	LD	y2,  10 * SIZE(Y1)

	ADD4	y7, t3, y7
	unop
	MUL	alpha1, a3, t3
	LD	y3,  11 * SIZE(Y1)

	ADD1	y0, t0, y0
	ST	y4,   4 * SIZE(Y1)
	MUL	alpha3, a4, t0
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(A2)

	ADD2	y1, t1, y1
	ST	y5,   5 * SIZE(Y1)
	MUL	alpha3, a5, t1
	unop

	ADD1	y2, t2, y2
	ST	y6,   6 * SIZE(Y1)
	MUL	alpha3, a6, t2
	unop

	ADD2	y3, t3, y3
	ST	y7,   7 * SIZE(Y1)
	MUL	alpha3, a7, t3
	lda	Y1,   8 * SIZE(Y1)

	ADD1	y0, t0, y0
	unop
	MUL	alpha2, a1, t0
	LD	a1,  13 * SIZE(A1)

	ADD2	y1, t1, y1
	unop
	MUL	alpha2, a0, t1
	LD	a0,  12 * SIZE(A1)

	ADD1	y2, t2, y2
	unop
	MUL	alpha2, a3, t2
	LD	a3,  15 * SIZE(A1)

	ADD2	y3, t3, y3
	unop
	MUL	alpha2, a2, t3
	LD	a2,  14 * SIZE(A1)

	ADD3	y0, t0, y0
	unop
	MUL	alpha4, a5, t0
	LD	a5,  13 * SIZE(A2)

	ADD4	y1, t1, y1
	unop
	MUL	alpha4, a4, t1
	LD	a4,  12 * SIZE(A2)

	ADD3	y2, t2, y2
	unop
	MUL	alpha4, a7, t2
	LD	a7,  15 * SIZE(A2)

	ADD4	y3, t3, y3
	unop
	MUL	alpha4, a6, t3
	LD	a6,  14 * SIZE(A2)

	ADD3	y0, t0, y0
	unop
	MUL	alpha1, a0, t0
	LD	y4,   4 * SIZE(Y1)

	ADD4	y1, t1, y1
	lda	A2,   8 * SIZE(A2)
	MUL	alpha1, a1, t1
	LD	y5,   5 * SIZE(Y1)

	ADD3	y2, t2, y2
	lda	A1,   8 * SIZE(A1)
	MUL	alpha1, a2, t2
	LD	y6,   6 * SIZE(Y1)

	ADD4	y3, t3, y3
	MUL	alpha1, a3, t3
	LD	y7,   7 * SIZE(Y1)
	bgt	I, $L12
	.align 4

$L13:
	ADD1	y4, t0, y4
	ST	y0,   0 * SIZE(Y1)
	MUL	alpha3, a4, t0
	unop

	ADD2	y5, t1, y5
	ST	y1,   1 * SIZE(Y1)
	MUL	alpha3, a5, t1
	unop

	ADD1	y6, t2, y6
	ST	y2,   2 * SIZE(Y1)
	MUL	alpha3, a6, t2
	unop

	ADD2	y7, t3, y7
	ST	y3,   3 * SIZE(Y1)
	MUL	alpha3, a7, t3
	unop

	ADD1	y4, t0, y4
	MUL	alpha2, a1, t0
	ADD2	y5, t1, y5
	MUL	alpha2, a0, t1

	ADD1	y6, t2, y6
	MUL	alpha2, a3, t2
	ADD2	y7, t3, y7
	MUL	alpha2, a2, t3

	ADD3	y4, t0, y4
	MUL	alpha4, a5, t0
	ADD4	y5, t1, y5
	MUL	alpha4, a4, t1

	ADD3	y6, t2, y6
	MUL	alpha4, a7, t2
	ADD4	y7, t3, y7
	MUL	alpha4, a6, t3

	ADD3	y4, t0, y4
	ADD4	y5, t1, y5
	ADD3	y6, t2, y6
	ADD4	y7, t3, y7

	ST	y4,   4 * SIZE(Y1)
	lda	A1,   8 * SIZE(A1)
	ST	y5,   5 * SIZE(Y1)
	lda	A2,   8 * SIZE(A2)

	ST	y6,   6 * SIZE(Y1)
	unop
	ST	y7,   7 * SIZE(Y1)
	lda	Y1,   8 * SIZE(Y1)
	.align 4

$L15:
	and	M, 2, I
	ble	I, $L17

	LD	a0,   0 * SIZE(A1)
	LD	a1,   1 * SIZE(A1)
	LD	a2,   2 * SIZE(A1)
	LD	a3,   3 * SIZE(A1)

	LD	a4,   0 * SIZE(A2)
	LD	a5,   1 * SIZE(A2)
	LD	a6,   2 * SIZE(A2)
	LD	a7,   3 * SIZE(A2)

	MUL	alpha1, a0, t0
	LD	y0,   0 * SIZE(Y1)
	MUL	alpha1, a1, t1
	LD	y1,   1 * SIZE(Y1)
	MUL	alpha1, a2, t2
	LD	y2,   2 * SIZE(Y1)
	MUL	alpha1, a3, t3
	LD	y3,   3 * SIZE(Y1)

	ADD1	y0, t0, y0
	MUL	alpha3, a4, t0
	ADD2	y1, t1, y1
	MUL	alpha3, a5, t1
	ADD1	y2, t2, y2
	MUL	alpha3, a6, t2
	ADD2	y3, t3, y3
	MUL	alpha3, a7, t3

	ADD1	y0, t0, y0
	MUL	alpha2, a1, t0
	ADD2	y1, t1, y1
	MUL	alpha2, a0, t1

	ADD1	y2, t2, y2
	MUL	alpha2, a3, t2
	ADD2	y3, t3, y3
	MUL	alpha2, a2, t3

	ADD3	y0, t0, y0
	MUL	alpha4, a5, t0
	ADD4	y1, t1, y1
	MUL	alpha4, a4, t1

	ADD3	y2, t2, y2
	MUL	alpha4, a7, t2
	ADD4	y3, t3, y3
	MUL	alpha4, a6, t3

	ADD3	y0, t0, y0
	ADD4	y1, t1, y1
	ADD3	y2, t2, y2
	ADD4	y3, t3, y3

	ST	y0,   0 * SIZE(Y1)
	lda	A1,   4 * SIZE(A1)
	ST	y1,   1 * SIZE(Y1)
	lda	A2,   4 * SIZE(A2)

	ST	y2,   2 * SIZE(Y1)
	unop
	ST	y3,   3 * SIZE(Y1)
	lda	Y1,   4 * SIZE(Y1)
	.align 4

$L17:
	blbc	M, $L18

	LD	a0,   0 * SIZE(A1)
	LD	a1,   1 * SIZE(A1)
	LD	a2,   0 * SIZE(A2)
	LD	a3,   1 * SIZE(A2)

	LD	y0,   0 * SIZE(Y1)
	LD	y1,   1 * SIZE(Y1)

	MUL	alpha1, a0, t0
	MUL	alpha1, a1, t1

	ADD1	y0, t0, y0
	MUL	alpha3, a2, t0
	ADD2	y1, t1, y1
	MUL	alpha3, a3, t1

	ADD1	y0, t0, y0
	MUL	alpha2, a1, t0
	ADD2	y1, t1, y1
	MUL	alpha2, a0, t1

	ADD3	y0, t0, y0
	MUL	alpha4, a3, t0
	ADD4	y1, t1, y1
	MUL	alpha4, a2, t1

	ADD3	y0, t0, y0
	ADD4	y1, t1, y1

	ST	y0,   0 * SIZE(Y1)
	ST	y1,   1 * SIZE(Y1)
	.align 4

$L18:
	lda	J, -1(J)
	bgt	J,  $L11
	.align 4

$L20:
	blbc	N,  $L990

	LD	alpha1,  0 * SIZE(X)
	LD	alpha2,  1 * SIZE(X)

	MUL	alpha_r, alpha1, y0
	MUL	alpha_r, alpha2, y1

	MUL	alpha_i, alpha2, t0
	mov	A, A1
	MUL	alpha_i, alpha1, t1
	mov	Y, Y1

#ifndef XCONJ
	SUB	y0, t0, alpha1
	ADD	y1, t1, alpha2
#else
	ADD	y0, t0, alpha1
	SUB	y1, t1, alpha2
#endif

	sra	M,  2, I
	ble	I,  $L25

	LD	a0,   0 * SIZE(A1)
	LD	a1,   1 * SIZE(A1)
	LD	a2,   2 * SIZE(A1)
	LD	a3,   3 * SIZE(A1)

	LD	y0,   0 * SIZE(Y1)
	LD	y1,   1 * SIZE(Y1)
	LD	y2,   2 * SIZE(Y1)
	LD	y3,   3 * SIZE(Y1)

	MUL	alpha1, a0, t0
	LD	a4,   4 * SIZE(A1)
	MUL	alpha1, a1, t1
	LD	a5,   5 * SIZE(A1)
	MUL	alpha1, a2, t2
	LD	a6,   6 * SIZE(A1)
	MUL	alpha1, a3, t3
	LD	a7,   7 * SIZE(A1)

	ADD1	y0, t0, y0
	unop
	MUL	alpha2, a1, t0
	LD	a1,   9 * SIZE(A1)

	ADD2	y1, t1, y1
	unop
	MUL	alpha2, a0, t1
	LD	a0,   8 * SIZE(A1)

	ADD1	y2, t2, y2
	unop
	MUL	alpha2, a3, t2
	LD	a3,  11 * SIZE(A1)

	ADD2	y3, t3, y3
	unop
	MUL	alpha2, a2, t3
	LD	a2,  10 * SIZE(A1)

	ADD3	y0, t0, y0
	unop
	LD	y4,   4 * SIZE(Y1)
	MUL	alpha1, a4, t0

	ADD4	y1, t1, y1
	unop
	LD	y5,   5 * SIZE(Y1)
	MUL	alpha1, a5, t1

	ADD3	y2, t2, y2
	LD	y6,   6 * SIZE(Y1)
	MUL	alpha1, a6, t2
	lda	I,   -1(I)

	ADD4	y3, t3, y3
	LD	y7,   7 * SIZE(Y1)
	MUL	alpha1, a7, t3
	ble	I, $L23
	.align 4

$L22:
	ADD1	y4, t0, y4
	ST	y0,   0 * SIZE(Y1)
	MUL	alpha2, a5, t0
	LD	a5,  13 * SIZE(A1)

	ADD2	y5, t1, y5
	ST	y1,   1 * SIZE(Y1)
	MUL	alpha2, a4, t1
	LD	a4,  12 * SIZE(A1)

	ADD1	y6, t2, y6
	ST	y2,   2 * SIZE(Y1)
	MUL	alpha2, a7, t2
	LD	a7,  15 * SIZE(A1)

	ADD2	y7, t3, y7
	ST	y3,   3 * SIZE(Y1)
	MUL	alpha2, a6, t3
	LD	a6,  14 * SIZE(A1)

	ADD3	y4, t0, y4
	LD	y0,   8 * SIZE(Y1)
	MUL	alpha1, a0, t0
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(A1)

	ADD4	y5, t1, y5
	LD	y1,   9 * SIZE(Y1)
	MUL	alpha1, a1, t1
	lda	I,   -1(I)

	ADD3	y6, t2, y6
	LD	y2,  10 * SIZE(Y1)
	MUL	alpha1, a2, t2
	unop

	ADD4	y7, t3, y7
	LD	y3,  11 * SIZE(Y1)
	MUL	alpha1, a3, t3
	unop

	ADD1	y0, t0, y0
	ST	y4,   4 * SIZE(Y1)
	MUL	alpha2, a1, t0
	LD	a1,  17 * SIZE(A1)

	ADD2	y1, t1, y1
	ST	y5,   5 * SIZE(Y1)
	MUL	alpha2, a0, t1
	LD	a0,  16 * SIZE(A1)

	ADD1	y2, t2, y2
	ST	y6,   6 * SIZE(Y1)
	MUL	alpha2, a3, t2
	LD	a3,  19 * SIZE(A1)

	ADD2	y3, t3, y3
	ST	y7,   7 * SIZE(Y1)
	MUL	alpha2, a2, t3
	LD	a2,  18 * SIZE(A1)

	ADD3	y0, t0, y0
	LD	y4,  12 * SIZE(Y1)
	MUL	alpha1, a4, t0
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(Y1)

	ADD4	y1, t1, y1
	LD	y5,  13 * SIZE(Y1)
	MUL	alpha1, a5, t1
	lda	A1,   8 * SIZE(A1)

	ADD3	y2, t2, y2
	LD	y6,  14 * SIZE(Y1)
	MUL	alpha1, a6, t2
	lda	Y1,   8 * SIZE(Y1)

	ADD4	y3, t3, y3
	LD	y7,   7 * SIZE(Y1)
	MUL	alpha1, a7, t3
	bgt	I, $L22
	.align 4

$L23:
	ADD1	y4, t0, y4
	ST	y0,   0 * SIZE(Y1)
	MUL	alpha2, a5, t0
	unop

	ADD2	y5, t1, y5
	ST	y1,   1 * SIZE(Y1)
	MUL	alpha2, a4, t1
	unop

	ADD1	y6, t2, y6
	ST	y2,   2 * SIZE(Y1)
	MUL	alpha2, a7, t2
	unop

	ADD2	y7, t3, y7
	ST	y3,   3 * SIZE(Y1)
	MUL	alpha2, a6, t3
	unop

	ADD3	y4, t0, y4
	ADD4	y5, t1, y5
	ADD3	y6, t2, y6
	ADD4	y7, t3, y7

	ST	y4,   4 * SIZE(Y1)
	unop
	ST	y5,   5 * SIZE(Y1)
	unop

	ST	y6,   6 * SIZE(Y1)
	lda	A1,   8 * SIZE(A1)
	ST	y7,   7 * SIZE(Y1)
	lda	Y1,   8 * SIZE(Y1)
	.align 4

$L25:
	and	M, 2, I
	ble	I, $L27

	LD	a0,   0 * SIZE(A1)
	LD	a1,   1 * SIZE(A1)
	LD	a2,   2 * SIZE(A1)
	LD	a3,   3 * SIZE(A1)

	MUL	alpha1, a0, t0
	LD	y0,   0 * SIZE(Y1)
	MUL	alpha1, a1, t1
	LD	y1,   1 * SIZE(Y1)
	MUL	alpha1, a2, t2
	LD	y2,   2 * SIZE(Y1)
	MUL	alpha1, a3, t3
	LD	y3,   3 * SIZE(Y1)

	ADD1	y0, t0, y0
	MUL	alpha2, a1, t0
	ADD2	y1, t1, y1
	MUL	alpha2, a0, t1
	ADD1	y2, t2, y2
	MUL	alpha2, a3, t2
	ADD2	y3, t3, y3
	MUL	alpha2, a2, t3

	ADD3	y0, t0, y0
	ADD4	y1, t1, y1
	ADD3	y2, t2, y2
	ADD4	y3, t3, y3

	ST	y0,   0 * SIZE(Y1)
	ST	y1,   1 * SIZE(Y1)

	ST	y2,   2 * SIZE(Y1)
	lda	A1,   4 * SIZE(A1)
	ST	y3,   3 * SIZE(Y1)
	lda	Y1,   4 * SIZE(Y1)
	.align 4

$L27:
	blbc	M, $L990

	LD	a0,   0 * SIZE(A1)
	LD	a1,   1 * SIZE(A1)

	MUL	alpha1, a0, t0
	LD	y0,   0 * SIZE(Y1)
	MUL	alpha1, a1, t1
	LD	y1,   1 * SIZE(Y1)

	ADD1	y0, t0, y0
	MUL	alpha2, a1, t0
	ADD2	y1, t1, y1
	MUL	alpha2, a0, t1

	ADD3	y0, t0, y0
	ADD4	y1, t1, y1

	ST	y0,   0 * SIZE(Y1)
	ST	y1,   1 * SIZE(Y1)
	.align 4

$L990:
	cmpeq	INCY, 2 * SIZE, $0
	bne	$0, $L999

	mov	BUFFER, Y1

	sra	M, 2, I
	ble	I, $L995
	.align 4

$L992:
	LD	a0,  0 * SIZE(BUFFER)
	LD	a1,  1 * SIZE(BUFFER)
	addq	BUFFER, INCY, BUFFER
	LD	a2,  0 * SIZE(BUFFER)
	LD	a3,  1 * SIZE(BUFFER)
	addq	BUFFER, INCY, BUFFER

	LD	y0,  0 * SIZE(Y)
	LD	y1,  1 * SIZE(Y)
	LD	y2,  2 * SIZE(Y)
	LD	y3,  3 * SIZE(Y)

	LD	a4,  0 * SIZE(BUFFER)
	LD	a5,  1 * SIZE(BUFFER)
	addq	BUFFER, INCY, BUFFER
	LD	a6,  0 * SIZE(BUFFER)
	LD	a7,  1 * SIZE(BUFFER)
	addq	BUFFER, INCY, BUFFER

	LD	y4,  4 * SIZE(Y)
	LD	y5,  5 * SIZE(Y)
	LD	y6,  6 * SIZE(Y)
	LD	y7,  7 * SIZE(Y)

	ADD	a0, y0, a0
	ADD	a1, y1, a1
	ADD	a2, y2, a2
	ADD	a3, y3, a3

	ST	a0,  0 * SIZE(Y1)
	ADD	a4, y4, a4
	ST	a1,  1 * SIZE(Y1)
	ADD	a5, y5, a5
	addq	Y1, INCY, Y1

	ST	a2,  0 * SIZE(Y1)
	ADD	a6, y6, a6
	ST	a3,  1 * SIZE(Y1)
	ADD	a7, y7, a7
	addq	Y1, INCY, Y1

	ST	a4,  0 * SIZE(Y1)
	ST	a5,  1 * SIZE(Y1)
	addq	Y1, INCY, Y1
	ST	a6,  0 * SIZE(Y1)
	ST	a7,  1 * SIZE(Y1)
	addq	Y1, INCY, Y1

	lda	I, -1(I)
	lda	Y,   8 * SIZE(Y)
	bgt	I, $L992
	.align 4

$L995:
	and	M, 3, I
	ble	I, $L999
	.align 4

$L996:
	LD	a0,  0 * SIZE(BUFFER)
	LD	a1,  1 * SIZE(BUFFER)
	addq	BUFFER, INCY, BUFFER

	LD	y0,  0 * SIZE(Y)
	LD	y1,  1 * SIZE(Y)
	lda	Y,   2 * SIZE(Y)

	ADD	a0, y0, a0
	ADD	a1, y1, a1

	ST	a0,  0 * SIZE(Y1)
	ST	a1,  1 * SIZE(Y1)
	addq	Y1, INCY, Y1

	lda	I, -1(I)
	bgt	I, $L996
	.align 4

$L999:
	ldt	$f2,    0($sp)
	ldt	$f3,    8($sp)
	ldt	$f4,   16($sp)
	ldt	$f5,   24($sp)
	ldt	$f6,   32($sp)
	ldt	$f7,   40($sp)
	ldt	$f8,   48($sp)
	ldt	$f9,   56($sp)

	lda	$sp,  STACKSIZE($sp)
	ret
	EPILOGUE
