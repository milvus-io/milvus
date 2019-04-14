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
#define A	$20
#define	LDA	$21

#define X	$18
#define	INCX	$19
#define Y	$22
#define	INCY	$23

#define BUFFER	$24

#define I	$25
#define J	$27

#define	X1	$3
#define Y1	$4

#define A1	$5
#define A2	$6
#define A3	$7
#define A4	$8

#define	alpha	$f19

#define	s0	$f0
#define	s1	$f1
#define	s2	$f10
#define	s3	$f11

#define	t0	$f12
#define	t1	$f13
#define	t2	$f14
#define	t3	$f15

#define	x0	$f16
#define	x1	$f17
#define	x2	$f18
#define	x3	$f21

#define	a0	$f22
#define	a1	$f23
#define	a2	$f24
#define	a3	$f25
#define	a4	$f26
#define	a5	$f27
#define	a6	$f28
#define	a7	$f29

#define	a8	$f2
#define	a9	$f3
#define	a10	$f4
#define	a11	$f5
#define	a12	$f6
#define	a13	$f7
#define	a14	$f8
#define	a15	$f9

	PROLOGUE

	lda	$sp,  -STACKSIZE($sp)
	ldq	X,       0 + STACKSIZE($sp)
	ldq	INCX,    8 + STACKSIZE($sp)
	ldq	Y,      16 + STACKSIZE($sp)
	ldq	INCY,   24 + STACKSIZE($sp)
	ldq	BUFFER, 32 + STACKSIZE($sp)

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
	SXADDQ	INCX, 0, INCX
	cmple	N, 0, $1
	SXADDQ	INCY, 0, INCY

	or	$0, $1, $0
	bne	$0,  $L999

	cmpeq	INCX, SIZE, $0
	mov	X, X1
	SXADDQ	LDA,  0, LDA
	bne	$0, $L10

	sra	M, 3, I
	mov	BUFFER, Y1
	mov	BUFFER, X
	ble	I, $L05
	.align 4

$L02:
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(X1)
	lda	I, -1(I)

	LD	a0,  0 * SIZE(X1)
	addq	X1, INCX, X1
	LD	a1,  0 * SIZE(X1)
	addq	X1, INCX, X1
	LD	a2,  0 * SIZE(X1)
	addq	X1, INCX, X1
	LD	a3,  0 * SIZE(X1)
	addq	X1, INCX, X1

	ST	a0,  0 * SIZE(Y1)
	ST	a1,  1 * SIZE(Y1)
	ST	a2,  2 * SIZE(Y1)
	ST	a3,  3 * SIZE(Y1)

	LD	a4,  0 * SIZE(X1)
	addq	X1, INCX, X1
	LD	a5,  0 * SIZE(X1)
	addq	X1, INCX, X1
	LD	a6,  0 * SIZE(X1)
	addq	X1, INCX, X1
	LD	a7,  0 * SIZE(X1)
	addq	X1, INCX, X1

	ST	a4,  4 * SIZE(Y1)
	ST	a5,  5 * SIZE(Y1)
	ST	a6,  6 * SIZE(Y1)
	ST	a7,  7 * SIZE(Y1)

	lda	Y1,  8 * SIZE(Y1)
	bgt	I, $L02
	.align 4

$L05:
	and	M, 7, I
	ble	I, $L10
	.align 4

$L06:
	LD	a0,  0 * SIZE(X1)
	addq	X1, INCX, X1
	ST	a0,  0 * SIZE(Y1)
	addq	Y1, SIZE, Y1

	lda	I, -1(I)
	bgt	I, $L06
	.align 4

$L10:
	mov	Y, Y1
	fclr	t0
	unop
	fclr	t1

	sra	N, 2, J
	fclr	t2
	fclr	t3
	ble	J,  $L20
	.align 4

$L11:
	mov	A, A1
	fclr	s0
	addq	A,  LDA, A2
	fclr	s1

	addq	A2, LDA, A3
	fclr	s2
	addq	A3, LDA, A4
	fclr	s3

	s4addq	LDA, A, A
	unop
	mov	X, X1
	lds	$f31, 3 * SIZE(Y)

	sra	M,  3, I
	ble	I,  $L15

	LD	x0,   0 * SIZE(X1)
	LD	x1,   1 * SIZE(X1)
	LD	x2,   2 * SIZE(X1)

	LD	a0,   0 * SIZE(A1)
	LD	a1,   0 * SIZE(A2)
	LD	a2,   0 * SIZE(A3)
	LD	a3,   0 * SIZE(A4)
	LD	a4,   1 * SIZE(A1)
	LD	a5,   1 * SIZE(A2)
	LD	a6,   1 * SIZE(A3)
	LD	a7,   1 * SIZE(A4)
	LD	a8,   2 * SIZE(A1)
	LD	a9,   2 * SIZE(A2)
	LD	a10,  2 * SIZE(A3)
	LD	a11,  2 * SIZE(A4)
	LD	a12,  3 * SIZE(A1)
	LD	a13,  3 * SIZE(A2)
	LD	a14,  3 * SIZE(A3)
	LD	a15,  3 * SIZE(A4)

	lda	I,   -1(I)
	ble	I, $L13
	.align 4

$L12:
	ADD	s0, t0,  s0
	LD	x3,   3 * SIZE(X1)
	MUL	x0, a0,  t0
	LD	a0,   4 * SIZE(A1)

	ADD	s1, t1,  s1
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(A1)
	MUL	x0, a1,  t1
	LD	a1,   4 * SIZE(A2)

	ADD	s2, t2,  s2
	unop
	MUL	x0, a2,  t2
	LD	a2,   4 * SIZE(A3)

	ADD	s3, t3,  s3
	unop
	MUL	x0, a3,  t3
	LD	a3,   4 * SIZE(A4)

	ADD	s0, t0,  s0
	LD	x0,   4 * SIZE(X1)
	MUL	x1, a4,  t0
	LD	a4,   5 * SIZE(A1)

	ADD	s1, t1,  s1
	lda	A1,   8 * SIZE(A1)
	MUL	x1, a5,  t1
	LD	a5,   5 * SIZE(A2)

	ADD	s2, t2,  s2
	unop
	MUL	x1, a6,  t2
	LD	a6,   5 * SIZE(A3)

	ADD	s3, t3,  s3
	unop
	MUL	x1, a7,  t3
	LD	a7,   5 * SIZE(A4)

	ADD	s0, t0,  s0
	LD	x1,   5 * SIZE(X1)
	MUL	x2, a8,  t0
	LD	a8,  -2 * SIZE(A1)

	ADD	s1, t1,  s1
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(A2)
	MUL	x2, a9,  t1
	LD	a9,   6 * SIZE(A2)

	ADD	s2, t2,  s2
	lda	A2,   8 * SIZE(A2)
	MUL	x2, a10, t2
	LD	a10,  6 * SIZE(A3)

	ADD	s3, t3,  s3
	lda	A3,   8 * SIZE(A3)
	MUL	x2, a11, t3
	LD	a11,  6 * SIZE(A4)

	ADD	s0, t0,  s0
	LD	x2,   6 * SIZE(X1)
	MUL	x3, a12, t0
	LD	a12, -1 * SIZE(A1)

	ADD	s1, t1,  s1
	lda	A4,   8 * SIZE(A4)
	MUL	x3, a13, t1
	LD	a13, -1 * SIZE(A2)

	ADD	s2, t2,  s2
	unop
	MUL	x3, a14, t2
	LD	a14, -1 * SIZE(A3)

	ADD	s3, t3,  s3
	unop
	MUL	x3, a15, t3
	LD	a15, -1 * SIZE(A4)

	ADD	s0, t0,  s0
	LD	x3,   7 * SIZE(X1)
	MUL	x0, a0,  t0
	LD	a0,   0 * SIZE(A1)

	ADD	s1, t1,  s1
	ldl	$31, (PREFETCHSIZE - 8) * SIZE(A3)
	MUL	x0, a1,  t1
	LD	a1,   0 * SIZE(A2)

	ADD	s2, t2,  s2
	unop
	MUL	x0, a2,  t2
	LD	a2,   0 * SIZE(A3)

	ADD	s3, t3,  s3
	unop
	MUL	x0, a3,  t3
	LD	a3,   0 * SIZE(A4)

	ADD	s0, t0,  s0
	LD	x0,   8 * SIZE(X1)
	MUL	x1, a4,  t0
	LD	a4,   1 * SIZE(A1)

	ADD	s1, t1,  s1
	unop
	MUL	x1, a5,  t1
	LD	a5,   1 * SIZE(A2)

	ADD	s2, t2,  s2
	unop
	MUL	x1, a6,  t2
	LD	a6,   1 * SIZE(A3)

	ADD	s3, t3,  s3
	unop
	MUL	x1, a7,  t3
	LD	a7,   1 * SIZE(A4)

	ADD	s0, t0,  s0
	LD	x1,   9 * SIZE(X1)
	MUL	x2, a8,  t0
	LD	a8,   2 * SIZE(A1)

	ADD	s1, t1,  s1
	ldl	$31, (PREFETCHSIZE - 8) * SIZE(A4)
	MUL	x2, a9,  t1
	LD	a9,   2 * SIZE(A2)

	ADD	s2, t2,  s2
	lda	X1,   8 * SIZE(X1)
	MUL	x2, a10, t2
	LD	a10,  2 * SIZE(A3)

	ADD	s3, t3,  s3
	lda	I,   -1(I)
	MUL	x2, a11, t3
	LD	a11,  2 * SIZE(A4)

	ADD	s0, t0,  s0
	LD	x2,   2 * SIZE(X1)
	MUL	x3, a12, t0
	LD	a12,  3 * SIZE(A1)

	ADD	s1, t1,  s1
	ldl	$31, (PREFETCHSIZE - 8) * SIZE(X1)
	MUL	x3, a13, t1
	LD	a13,  3 * SIZE(A2)

	ADD	s2, t2,  s2
	unop
	MUL	x3, a14, t2
	LD	a14,  3 * SIZE(A3)

	ADD	s3, t3,  s3
	MUL	x3, a15, t3
	LD	a15, 3 * SIZE(A4)
	bgt	I, $L12
	.align 4

$L13:
	ADD	s0, t0,  s0
	LD	x3,   3 * SIZE(X1)
	MUL	x0, a0,  t0
	LD	a0,   4 * SIZE(A1)

	ADD	s1, t1,  s1
	unop
	MUL	x0, a1,  t1
	LD	a1,   4 * SIZE(A2)

	ADD	s2, t2,  s2
	unop
	MUL	x0, a2,  t2
	LD	a2,   4 * SIZE(A3)

	ADD	s3, t3,  s3
	unop
	MUL	x0, a3,  t3
	LD	a3,   4 * SIZE(A4)

	ADD	s0, t0,  s0
	LD	x0,   4 * SIZE(X1)
	MUL	x1, a4,  t0
	LD	a4,   5 * SIZE(A1)

	ADD	s1, t1,  s1
	unop
	MUL	x1, a5,  t1
	LD	a5,   5 * SIZE(A2)

	ADD	s2, t2,  s2
	unop
	MUL	x1, a6,  t2
	LD	a6,   5 * SIZE(A3)

	ADD	s3, t3,  s3
	unop
	MUL	x1, a7,  t3
	LD	a7,   5 * SIZE(A4)

	ADD	s0, t0,  s0
	LD	x1,   5 * SIZE(X1)
	MUL	x2, a8,  t0
	LD	a8,   6 * SIZE(A1)

	ADD	s1, t1,  s1
	unop
	MUL	x2, a9,  t1
	LD	a9,   6 * SIZE(A2)

	ADD	s2, t2,  s2
	unop
	MUL	x2, a10, t2
	LD	a10,  6 * SIZE(A3)

	ADD	s3, t3,  s3
	unop
	MUL	x2, a11, t3
	LD	a11,  6 * SIZE(A4)

	ADD	s0, t0,  s0
	LD	x2,   6 * SIZE(X1)
	MUL	x3, a12, t0
	LD	a12,  7 * SIZE(A1)

	ADD	s1, t1,  s1
	lda	A1,  8 * SIZE(A1)
	MUL	x3, a13, t1
	LD	a13,  7 * SIZE(A2)

	ADD	s2, t2,  s2
	lda	A2,  8 * SIZE(A2)
	MUL	x3, a14, t2
	LD	a14,  7 * SIZE(A3)

	ADD	s3, t3,  s3
	lda	A3,  8 * SIZE(A3)
	MUL	x3, a15, t3
	LD	a15,  7 * SIZE(A4)

	ADD	s0, t0,  s0
	LD	x3,   7 * SIZE(X1)
	MUL	x0, a0,  t0
	unop

	ADD	s1, t1,  s1
	lda	X1,  8 * SIZE(X1)
	MUL	x0, a1,  t1
	lda	A4,  8 * SIZE(A4)

	ADD	s2, t2,  s2
	MUL	x0, a2,  t2
	ADD	s3, t3,  s3
	MUL	x0, a3,  t3

	ADD	s0, t0,  s0
	MUL	x1, a4,  t0
	ADD	s1, t1,  s1
	MUL	x1, a5,  t1

	ADD	s2, t2,  s2
	MUL	x1, a6,  t2
	ADD	s3, t3,  s3
	MUL	x1, a7,  t3

	ADD	s0, t0,  s0
	MUL	x2, a8,  t0
	ADD	s1, t1,  s1
	MUL	x2, a9,  t1

	ADD	s2, t2,  s2
	MUL	x2, a10, t2
	ADD	s3, t3,  s3
	MUL	x2, a11, t3

	ADD	s0, t0,  s0
	MUL	x3, a12, t0
	ADD	s1, t1,  s1
	MUL	x3, a13, t1

	ADD	s2, t2,  s2
	MUL	x3, a14, t2
	ADD	s3, t3,  s3
	MUL	x3, a15, t3
	.align 4

$L15:
	and	M, 7, I
	ble	I,  $L18

	LD	x0,   0 * SIZE(X1)

	LD	a0,   0 * SIZE(A1)
	LD	a1,   0 * SIZE(A2)
	LD	a2,   0 * SIZE(A3)
	LD	a3,   0 * SIZE(A4)

	lda	I,   -1(I)
	ble	I, $L17
	.align 4

$L16:
	ADD	s0, t0, s0
	lda	A4,   1 * SIZE(A4)
	MUL	x0, a0, t0
	LD	a0,   1 * SIZE(A1)

	ADD	s1, t1, s1
	lda	A1,   1 * SIZE(A1)
	MUL	x0, a1, t1
	LD	a1,   1 * SIZE(A2)

	ADD	s2, t2, s2
	lda	A2,   1 * SIZE(A2)
	MUL	x0, a2, t2
	LD	a2,   1 * SIZE(A3)

	ADD	s3, t3, s3
	lda	A3,   1 * SIZE(A3)
	MUL	x0, a3, t3
	LD	a3,   0 * SIZE(A4)

	LD	x0,   1 * SIZE(X1)
	lda	X1,   1 * SIZE(X1)
	lda	I,   -1(I)
	bgt	I, $L16
	.align 4

$L17:
	ADD	s0, t0, s0
	MUL	x0, a0, t0
	ADD	s1, t1, s1
	MUL	x0, a1, t1

	ADD	s2, t2, s2
	MUL	x0, a2, t2
	ADD	s3, t3, s3
	MUL	x0, a3, t3
	.align 4

$L18:
	LD	a0,    0 * SIZE(Y)
	addq	Y, INCY, Y
	LD	a1,    0 * SIZE(Y)
	addq	Y, INCY, Y
	LD	a2,    0 * SIZE(Y)
	addq	Y, INCY, Y
	LD	a3,    0 * SIZE(Y)
	addq	Y, INCY, Y

	ADD	s0, t0, s0
	ADD	s1, t1, s1
	ADD	s2, t2, s2
	ADD	s3, t3, s3

	MUL	alpha, s0, s0
	MUL	alpha, s1, s1
	MUL	alpha, s2, s2
	MUL	alpha, s3, s3

	ADD	a0, s0, a0
	fclr	t0
	ADD	a1, s1, a1
	fclr	t1
	ADD	a2, s2, a2
	fclr	t2
	ADD	a3, s3, a3
	fclr	t3

	ST	a0,    0 * SIZE(Y1)
	addq	Y1, INCY, Y1
	ST	a1,    0 * SIZE(Y1)
	addq	Y1, INCY, Y1
	ST	a2,    0 * SIZE(Y1)
	addq	Y1, INCY, Y1
	ST	a3,    0 * SIZE(Y1)
	addq	Y1, INCY, Y1

	lda	J, -1(J)
	bgt	J,  $L11
	.align 4

$L20:
	and	N, 2, J
	ble	J,  $L30
	mov	A, A1
	addq	A,  LDA, A2

	addq	A2, LDA, A
	fclr	s0
	mov	X, X1
	fclr	s1

	sra	M,  3, I
	fclr	s2
	fclr	s3
	ble	I,  $L25

	LD	a0,   0 * SIZE(A1)
	LD	a1,   0 * SIZE(A2)
	LD	a2,   1 * SIZE(A1)
	LD	a3,   1 * SIZE(A2)
	LD	a4,   2 * SIZE(A1)
	LD	a5,   2 * SIZE(A2)
	LD	a6,   3 * SIZE(A1)
	LD	a7,   3 * SIZE(A2)

	LD	a8,   4 * SIZE(A1)
	LD	a9,   4 * SIZE(A2)
	LD	a10,  5 * SIZE(A1)
	LD	a11,  5 * SIZE(A2)
	LD	a12,  6 * SIZE(A1)
	LD	a13,  6 * SIZE(A2)
	LD	a14,  7 * SIZE(A1)
	LD	a15,  7 * SIZE(A2)

	LD	x0,   0 * SIZE(X1)
	LD	x1,   1 * SIZE(X1)
	LD	x2,   2 * SIZE(X1)

	lda	I,   -1(I)
	ble	I, $L23
	.align 4

$L22:
	ADD	s0, t0,  s0
	LD	x3,   3 * SIZE(X1)
	MUL	x0, a0,  t0
	LD	a0,   8 * SIZE(A1)

	ADD	s1, t1,  s1
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(A1)
	MUL	x0, a1,  t1
	LD	a1,   8 * SIZE(A2)

	ADD	s0, t2,  s0
	LD	x0,   4 * SIZE(X1)
	MUL	x1, a2,  t2
	LD	a2,   9 * SIZE(A1)

	ADD	s1, t3,  s1
	unop
	MUL	x1, a3,  t3
	LD	a3,   9 * SIZE(A2)

	ADD	s0, t0,  s0
	LD	x1,   5 * SIZE(X1)
	MUL	x2, a4,  t0
	LD	a4,  10 * SIZE(A1)

	ADD	s1, t1,  s1
	lda	I,   -1(I)
	MUL	x2, a5,  t1
	LD	a5,  10 * SIZE(A2)

	ADD	s0, t2,  s0
	LD	x2,   6 * SIZE(X1)
	MUL	x3, a6,  t2
	LD	a6,  11 * SIZE(A1)

	ADD	s1, t3,  s1
	lda	X1,   8 * SIZE(X1)
	MUL	x3, a7,  t3
	LD	a7,  11 * SIZE(A2)

	ADD	s0, t0,  s0
	LD	x3,  -1 * SIZE(X1)
	MUL	x0, a8,  t0
	LD	a8,  12 * SIZE(A1)

	ADD	s1, t1,  s1
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(A2)
	MUL	x0, a9,  t1
	LD	a9,  12 * SIZE(A2)

	ADD	s0, t0,  s0
	LD	x0,   0 * SIZE(X1)
	MUL	x1, a10, t0
	LD	a10, 13 * SIZE(A1)

	ADD	s1, t1,  s1
	lda	A1,   8 * SIZE(A1)
	MUL	x1, a11, t1
	LD	a11, 13 * SIZE(A2)

	ADD	s0, t0,  s0
	LD	x1,   1 * SIZE(X1)
	MUL	x2, a12, t0
	LD	a12,  6 * SIZE(A1)

	ADD	s1, t1,  s1
	MUL	x2, a13, t1
	LD	a13, 14 * SIZE(A2)
	lda	A2,   8 * SIZE(A2)

	ADD	s0, t0,  s0
	LD	x2,   2 * SIZE(X1)
	MUL	x3, a14, t0
	LD	a14,  7 * SIZE(A1)

	ADD	s1, t1,  s1
	MUL	x3, a15, t1
	LD	a15,  7 * SIZE(A2)
	bgt	I, $L22
	.align 4

$L23:
	ADD	s0, t0,  s0
	LD	x3,   3 * SIZE(X1)
	MUL	x0, a0,  t0
	lda	A1,   8 * SIZE(A1)

	ADD	s1, t1,  s1
	unop
	MUL	x0, a1,  t1
	unop

	ADD	s0, t2,  s0
	LD	x0,   4 * SIZE(X1)
	MUL	x1, a2,  t2
	lda	A2,   8 * SIZE(A2)

	ADD	s1, t3,  s1
	unop
	MUL	x1, a3,  t3
	unop

	ADD	s0, t0,  s0
	LD	x1,   5 * SIZE(X1)
	MUL	x2, a4,  t0
	unop

	ADD	s1, t1,  s1
	unop
	MUL	x2, a5,  t1
	unop

	ADD	s0, t2,  s0
	LD	x2,   6 * SIZE(X1)
	MUL	x3, a6,  t2
	unop

	ADD	s1, t3,  s1
	unop
	MUL	x3, a7,  t3
	unop

	ADD	s0, t0,  s0
	LD	x3,   7 * SIZE(X1)
	MUL	x0, a8,  t0
	lda	X1,   8 * SIZE(X1)

	ADD	s1, t1,  s1
	unop
	MUL	x0, a9,  t1
	unop

	ADD	s0, t0,  s0
	MUL	x1, a10, t0
	ADD	s1, t1,  s1
	MUL	x1, a11, t1

	ADD	s0, t0,  s0
	MUL	x2, a12, t0
	ADD	s1, t1,  s1
	MUL	x2, a13, t1

	ADD	s0, t0,  s0
	MUL	x3, a14, t0
	ADD	s1, t1,  s1
	MUL	x3, a15, t1
	.align 4

$L25:
	and	M, 7, I
	ble	I,  $L28

	LD	a0,   0 * SIZE(A1)
	LD	a1,   0 * SIZE(A2)
	LD	x0,   0 * SIZE(X1)

	lda	I,   -1(I)
	ble	I, $L27
	.align 4

$L26:
	ADD	s0, t0, s0
	lda	A2,   1 * SIZE(A2)
	MUL	x0, a0, t0
	LD	a0,   1 * SIZE(A1)

	ADD	s1, t1, s1
	lda	A1,   1 * SIZE(A1)
	MUL	x0, a1, t1
	LD	a1,   0 * SIZE(A2)

	LD	x0,   1 * SIZE(X1)
	lda	X1,   1 * SIZE(X1)
	lda	I,   -1(I)
	bgt	I, $L26
	.align 4

$L27:
	ADD	s0, t0, s0
	MUL	x0, a0, t0
	ADD	s1, t1, s1
	MUL	x0, a1, t1
	.align 4

$L28:
	LD	a0,    0 * SIZE(Y)
	addq	Y, INCY, Y
	LD	a1,    0 * SIZE(Y)
	addq	Y, INCY, Y

	ADD	s0, t0, s0
	ADD	s1, t1, s1
	ADD	s2, t2, s2
	ADD	s3, t3, s3

	ADD	s0, s2, s0
	ADD	s1, s3, s1

	MUL	alpha, s0, s0
	MUL	alpha, s1, s1

	ADD	a0, s0, a0
	ADD	a1, s1, a1

	ST	a0,    0 * SIZE(Y1)
	fclr	t0
	addq	Y1, INCY, Y1
	fclr	t1

	ST	a1,    0 * SIZE(Y1)
	fclr	t2
	addq	Y1, INCY, Y1
	fclr	t3
	.align 4

$L30:
	blbc	N,  $L999

	mov	A, A1
	fclr	s0
	mov	X, X1
	fclr	s1

	sra	M,  3, I
	fclr	s2
	fclr	s3
	ble	I,  $L35

	LD	a0,   0 * SIZE(A1)
	LD	a1,   1 * SIZE(A1)
	LD	a8,   0 * SIZE(X1)
	LD	a9,   1 * SIZE(X1)

	LD	a2,   2 * SIZE(A1)
	LD	a3,   3 * SIZE(A1)
	LD	a10,  2 * SIZE(X1)
	LD	a11,  3 * SIZE(X1)

	LD	a4,   4 * SIZE(A1)
	LD	a5,   5 * SIZE(A1)
	LD	a12,  4 * SIZE(X1)
	LD	a13,  5 * SIZE(X1)

	LD	a6,   6 * SIZE(A1)
	LD	a7,   7 * SIZE(A1)
	LD	a14,  6 * SIZE(X1)

	lda	I,   -1(I)
	ble	I, $L33
	.align 4

$L32:
	ADD	s0, t0,  s0
	LD	a15,  7 * SIZE(X1)
	MUL	a0, a8,  t0
	LD	a0,   8 * SIZE(A1)

	ADD	s1, t1,  s1
	LD	a8,   8 * SIZE(X1)
	MUL	a1, a9,  t1
	LD	a1,   9 * SIZE(A1)

	ADD	s2, t2,  s2
	LD	a9,   9 * SIZE(X1)
	MUL	a2, a10, t2
	LD	a2,  10 * SIZE(A1)

	ADD	s3, t3,  s3
	LD	a10, 10 * SIZE(X1)
	MUL	a3, a11, t3
	LD	a3,  11 * SIZE(A1)

	ADD	s0, t0,  s0
	LD	a11, 11 * SIZE(X1)
	MUL	a4, a12, t0
	LD	a4,  12 * SIZE(A1)

	ADD	s1, t1,  s1
	LD	a12, 12 * SIZE(X1)
	MUL	a5, a13, t1
	LD	a5,  13 * SIZE(A1)

	ADD	s2, t2,  s2
	LD	a13, 13 * SIZE(X1)
	MUL	a6, a14, t2
	LD	a6,  14 * SIZE(A1)

	ADD	s3, t3,  s3
	LD	a14, 14 * SIZE(X1)
	MUL	a7, a15, t3
	LD	a7,  15 * SIZE(A1)

	lda	A1,   8 * SIZE(A1)
	lda	I,   -1(I)
	lda	X1,   8 * SIZE(X1)
	bgt	I, $L32
	.align 4

$L33:
	ADD	s0, t0,  s0
	LD	a15,  7 * SIZE(X1)
	MUL	a0, a8,  t0
	lda	A1,   8 * SIZE(A1)

	ADD	s1, t1,  s1
	unop
	MUL	a1, a9,  t1
	lda	X1,   8 * SIZE(X1)

	ADD	s2, t2,  s2
	MUL	a2, a10, t2
	ADD	s3, t3,  s3
	MUL	a3, a11, t3

	ADD	s0, t0,  s0
	MUL	a4, a12, t0
	ADD	s1, t1,  s1
	MUL	a5, a13, t1

	ADD	s2, t2,  s2
	MUL	a6, a14, t2
	ADD	s3, t3,  s3
	MUL	a7, a15, t3
	.align 4

$L35:
	and	M, 7, I
	ble	I,  $L38

	LD	a0,   0 * SIZE(A1)
	LD	x0,   0 * SIZE(X1)

	lda	I,   -1(I)
	ble	I, $L37
	.align 4

$L36:
	ADD	s0, t0, s0
	MUL	x0, a0, t0
	LD	a0,   1 * SIZE(A1)
	LD	x0,   1 * SIZE(X1)

	lda	A1,   1 * SIZE(A1)
	lda	X1,   1 * SIZE(X1)
	lda	I,   -1(I)
	bgt	I, $L36
	.align 4

$L37:
	ADD	s0, t0, s0
	MUL	x0, a0, t0
	.align 4

$L38:
	LD	a0,    0 * SIZE(Y)

	ADD	s0, t0, s0
	ADD	s1, t1, s1
	ADD	s2, t2, s2
	ADD	s3, t3, s3

	ADD	s0, s2, s0
	ADD	s1, s3, s1
	ADD	s0, s1, s0

	MUL	alpha, s0, s0
	ADD	a0, s0, a0

	ST	a0,    0 * SIZE(Y1)
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
