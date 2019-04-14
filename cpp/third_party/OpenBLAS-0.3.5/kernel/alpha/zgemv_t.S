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

#define	X1	$3
#define Y1	$4
#define A1	$5
#define A2	$6

#define	alpha_r	$f19
#define	alpha_i	$f20

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

#if   !defined(CONJ) && !defined(XCONJ)
#define ADD1	ADD
#define ADD2	ADD
#define ADD3	SUB
#define ADD4	ADD
#elif !defined(CONJ) &&  defined(XCONJ)
#define ADD1	ADD
#define ADD2	ADD
#define ADD3	ADD
#define ADD4	SUB
#elif  defined(CONJ) && !defined(XCONJ)
#define ADD1	ADD
#define ADD2	SUB
#define ADD3	ADD
#define ADD4	ADD
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

	cmpeq	INCX, 2 * SIZE, $0
	mov	X, X1
	sll	LDA, ZBASE_SHIFT,LDA
	bne	$0, $L10

	sra	M, 2, I
	mov	BUFFER, Y1
	mov	BUFFER, X
	ble	I, $L05
	.align 4

$L02:
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(X1)
	lda	I, -1(I)

	LD	a0,  0 * SIZE(X1)
	LD	a1,  1 * SIZE(X1)
	addq	X1, INCX, X1
	LD	a2,  0 * SIZE(X1)
	LD	a3,  1 * SIZE(X1)
	addq	X1, INCX, X1

	ST	a0,  0 * SIZE(Y1)
	ST	a1,  1 * SIZE(Y1)
	ST	a2,  2 * SIZE(Y1)
	ST	a3,  3 * SIZE(Y1)

	LD	a4,  0 * SIZE(X1)
	LD	a5,  1 * SIZE(X1)
	addq	X1, INCX, X1
	LD	a6,  0 * SIZE(X1)
	LD	a7,  1 * SIZE(X1)
	addq	X1, INCX, X1

	ST	a4,  4 * SIZE(Y1)
	ST	a5,  5 * SIZE(Y1)
	ST	a6,  6 * SIZE(Y1)
	ST	a7,  7 * SIZE(Y1)

	lda	Y1,  8 * SIZE(Y1)
	bgt	I, $L02
	.align 4

$L05:
	and	M, 3, I
	ble	I, $L10
	.align 4

$L06:
	LD	a0,  0 * SIZE(X1)
	LD	a1,  1 * SIZE(X1)
	addq	X1, INCX, X1

	ST	a0,  0 * SIZE(Y1)
	ST	a1,  1 * SIZE(Y1)
	lda	Y1,  2 * SIZE(Y1)

	lda	I, -1(I)
	bgt	I, $L06
	.align 4

$L10:
	mov	Y, Y1
	fclr	t0
	unop
	fclr	t1

	sra	N, 1, J
	fclr	t2
	fclr	t3
	ble	J,  $L20
	.align 4

$L11:
	mov	A, A1
	fclr	s0
	addq	A,  LDA, A2
	fclr	s1

	addq	A2, LDA, A
	unop
	mov	X, X1
	lds	$f31, 3 * SIZE(Y)

	sra	M,  2, I
	fclr	s2
	fclr	s3
	ble	I,  $L15

	LD	a0,   0 * SIZE(A1)
	LD	a1,   1 * SIZE(A1)
	LD	a2,   0 * SIZE(A2)
	LD	a3,   1 * SIZE(A2)
	LD	a4,   2 * SIZE(A1)
	LD	a5,   3 * SIZE(A1)
	LD	a6,   2 * SIZE(A2)
	LD	a7,   3 * SIZE(A2)

	LD	a8,   4 * SIZE(A1)
	LD	a9,   5 * SIZE(A1)
	LD	a10,  4 * SIZE(A2)
	LD	a11,  5 * SIZE(A2)
	LD	a12,  6 * SIZE(A1)
	LD	a13,  7 * SIZE(A1)
	LD	a14,  6 * SIZE(A2)
	LD	a15,  7 * SIZE(A2)

	LD	x0,   0 * SIZE(X1)
	LD	x1,   1 * SIZE(X1)
	LD	x2,   2 * SIZE(X1)

	lda	I,   -1(I)
	ble	I, $L13
	.align 4

$L12:
	ADD3	s0, t0,  s0
	unop
	MUL	x0, a0,  t0
	LD	x3,   3 * SIZE(X1)

	ADD4	s1, t1,  s1
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(A1)
	MUL	x0, a1,  t1
	unop

	ADD3	s2, t2,  s2
	unop
	MUL	x0, a2,  t2
	unop

	ADD4	s3, t3,  s3
	unop
	MUL	x0, a3,  t3
	LD	x0,   4 * SIZE(X1)

	ADD1	s0, t0,  s0
	unop
	MUL	x1, a1,  t0
	LD	a1,   9 * SIZE(A1)

	ADD2	s1, t1,  s1
	unop
	MUL	x1, a0,  t1
	LD	a0,   8 * SIZE(A1)

	ADD1	s2, t2,  s2
	unop
	MUL	x1, a3,  t2
	LD	a3,   9 * SIZE(A2)

	ADD2	s3, t3,  s3
	unop
	MUL	x1, a2,  t3
	LD	a2,   8 * SIZE(A2)

	ADD3	s0, t0,  s0
	unop
	MUL	x2, a4,  t0
	LD	x1,   5 * SIZE(X1)

	ADD4	s1, t1,  s1
	MUL	x2, a5,  t1
	ADD3	s2, t2,  s2
	MUL	x2, a6,  t2

	ADD4	s3, t3,  s3
	unop
	MUL	x2, a7,  t3
	LD	x2,   6 * SIZE(X1)

	ADD1	s0, t0,  s0
	unop
	MUL	x3, a5,  t0
	LD	a5,  11 * SIZE(A1)

	ADD2	s1, t1,  s1
	unop
	MUL	x3, a4,  t1
	LD	a4,  10 * SIZE(A1)

	ADD1	s2, t2,  s2
	unop
	MUL	x3, a7,  t2
	LD	a7,  11 * SIZE(A2)

	ADD2	s3, t3,  s3
	unop
	MUL	x3, a6,  t3
	LD	a6,  10 * SIZE(A2)

	ADD3	s0, t0,  s0
	unop
	MUL	x0, a8,  t0
	LD	x3,   7 * SIZE(X1)

	ADD4	s1, t1,  s1
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(A2)
	MUL	x0, a9,  t1
	unop

	ADD3	s2, t2,  s2
	lda	I,   -1(I)
	MUL	x0, a10, t2
	unop

	ADD4	s3, t3,  s3
	unop
	MUL	x0, a11, t3
	LD	x0,   8 * SIZE(X1)

	ADD1	s0, t0,  s0
	unop
	MUL	x1, a9,  t0
	LD	a9,  13 * SIZE(A1)

	ADD2	s1, t1,  s1
	unop
	MUL	x1, a8,  t1
	LD	a8,  12 * SIZE(A1)

	ADD1	s2, t2,  s2
	lda	A1,   8 * SIZE(A1)
	MUL	x1, a11, t2
	LD	a11, 13 * SIZE(A2)

	ADD2	s3, t3,  s3
	unop
	MUL	x1, a10, t3
	LD	a10, 12 * SIZE(A2)

	ADD3	s0, t0,  s0
	unop
	MUL	x2, a12, t0
	LD	x1,   9 * SIZE(X1)

	ADD4	s1, t1,  s1
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(X1)
	MUL	x2, a13, t1
	lda	A2,   8 * SIZE(A2)

	ADD3	s2, t2,  s2
	unop
	MUL	x2, a14, t2
	unop

	ADD4	s3, t3,  s3
	unop
	MUL	x2, a15, t3
	LD	x2,  10 * SIZE(X1)

	ADD1	s0, t0,  s0
	unop
	MUL	x3, a13, t0
	LD	a13,  7 * SIZE(A1)

	ADD2	s1, t1,  s1
	lda	X1,   8 * SIZE(X1)
	MUL	x3, a12, t1
	LD	a12,  6 * SIZE(A1)

	ADD1	s2, t2,  s2
	unop
	MUL	x3, a15, t2
	LD	a15,  7 * SIZE(A2)

	ADD2	s3, t3,  s3
	MUL	x3, a14, t3
	LD	a14,  6 * SIZE(A2)
	bgt	I, $L12
	.align 4

$L13:
	ADD3	s0, t0,  s0
	unop
	MUL	x0, a0,  t0
	LD	x3,   3 * SIZE(X1)

	ADD4	s1, t1,  s1
	MUL	x0, a1,  t1
	ADD3	s2, t2,  s2
	MUL	x0, a2,  t2

	ADD4	s3, t3,  s3
	unop
	MUL	x0, a3,  t3
	LD	x0,   4 * SIZE(X1)

	ADD1	s0, t0,  s0
	MUL	x1, a1,  t0
	ADD2	s1, t1,  s1
	MUL	x1, a0,  t1

	ADD1	s2, t2,  s2
	unop
	MUL	x1, a3,  t2
	unop

	ADD2	s3, t3,  s3
	lda	A1,   8 * SIZE(A1)
	MUL	x1, a2,  t3
	LD	x1,   5 * SIZE(X1)

	ADD3	s0, t0,  s0
	MUL	x2, a4,  t0
	ADD4	s1, t1,  s1
	MUL	x2, a5,  t1

	ADD3	s2, t2,  s2
	unop
	MUL	x2, a6,  t2
	unop

	ADD4	s3, t3,  s3
	lda	A2,   8 * SIZE(A2)
	MUL	x2, a7,  t3
	LD	x2,   6 * SIZE(X1)

	ADD1	s0, t0,  s0
	MUL	x3, a5,  t0
	ADD2	s1, t1,  s1
	MUL	x3, a4,  t1

	ADD1	s2, t2,  s2
	unop
	MUL	x3, a7,  t2
	lda	X1,   8 * SIZE(X1)

	ADD2	s3, t3,  s3
	unop
	MUL	x3, a6,  t3
	LD	x3,  -1 * SIZE(X1)

	ADD3	s0, t0,  s0
	MUL	x0, a8,  t0
	ADD4	s1, t1,  s1
	MUL	x0, a9,  t1

	ADD3	s2, t2,  s2
	MUL	x0, a10, t2
	ADD4	s3, t3,  s3
	MUL	x0, a11, t3

	ADD1	s0, t0,  s0
	MUL	x1, a9,  t0
	ADD2	s1, t1,  s1
	MUL	x1, a8,  t1

	ADD1	s2, t2,  s2
	MUL	x1, a11, t2
	ADD2	s3, t3,  s3
	MUL	x1, a10, t3

	ADD3	s0, t0,  s0
	MUL	x2, a12, t0
	ADD4	s1, t1,  s1
	MUL	x2, a13, t1

	ADD3	s2, t2,  s2
	MUL	x2, a14, t2
	ADD4	s3, t3,  s3
	MUL	x2, a15, t3

	ADD1	s0, t0,  s0
	MUL	x3, a13, t0
	ADD2	s1, t1,  s1
	MUL	x3, a12, t1

	ADD1	s2, t2,  s2
	MUL	x3, a15, t2
	ADD2	s3, t3,  s3
	MUL	x3, a14, t3
	.align 4

$L15:
	and	M, 3, I
	ble	I,  $L18

	LD	a0,   0 * SIZE(A1)
	LD	a1,   1 * SIZE(A1)
	LD	a2,   0 * SIZE(A2)
	LD	a3,   1 * SIZE(A2)

	LD	x0,   0 * SIZE(X1)

	lda	I,   -1(I)
	ble	I, $L17
	.align 4

$L16:
	ADD3	s0, t0, s0
	lda	I,   -1(I)
	MUL	x0, a0, t0
	LD	x1,   1 * SIZE(X1)

	ADD4	s1, t1, s1
	MUL	x0, a1, t1
	ADD3	s2, t2, s2
	MUL	x0, a2, t2

	ADD4	s3, t3, s3
	unop
	MUL	x0, a3, t3
	LD	x0,   2 * SIZE(X1)

	ADD1	s0, t0, s0
	lda	A2,   2 * SIZE(A2)
	MUL	x1, a1, t0
	LD	a1,   3 * SIZE(A1)

	ADD2	s1, t1, s1
	lda	X1,   2 * SIZE(X1)
	MUL	x1, a0, t1
	LD	a0,   2 * SIZE(A1)

	ADD1	s2, t2, s2
	lda	A1,   2 * SIZE(A1)
	MUL	x1, a3, t2
	LD	a3,   1 * SIZE(A2)

	ADD2	s3, t3, s3
	MUL	x1, a2, t3
	LD	a2,   0 * SIZE(A2)
	bgt	I, $L16
	.align 4

$L17:
	ADD3	s0, t0, s0
	unop
	MUL	x0, a0, t0
	LD	x1,   1 * SIZE(X1)

	ADD4	s1, t1, s1
	unop
	MUL	x0, a1, t1
	unop

	ADD3	s2, t2, s2
	MUL	x0, a2, t2
	ADD4	s3, t3, s3
	MUL	x0, a3, t3

	ADD1	s0, t0, s0
	MUL	x1, a1, t0
	ADD2	s1, t1, s1
	MUL	x1, a0, t1

	ADD1	s2, t2, s2
	MUL	x1, a3, t2
	ADD2	s3, t3, s3
	MUL	x1, a2, t3
	.align 4

$L18:
	LD	a0,    0 * SIZE(Y)
	unop
	LD	a1,    1 * SIZE(Y)
	addq	Y, INCY, Y

	LD	a2,    0 * SIZE(Y)
	unop
	LD	a3,    1 * SIZE(Y)
	addq	Y, INCY, Y

	ADD3	s0, t0, s0
	ADD4	s1, t1, s1
	ADD3	s2, t2, s2
	ADD4	s3, t3, s3

	MUL	alpha_r, s0, t0
	MUL	alpha_r, s1, t1
	MUL	alpha_r, s2, t2
	MUL	alpha_r, s3, t3

	ADD	a0, t0, a0
	MUL	alpha_i, s1, t0
	ADD	a1, t1, a1
	MUL	alpha_i, s0, t1
	ADD	a2, t2, a2
	MUL	alpha_i, s3, t2
	ADD	a3, t3, a3
	MUL	alpha_i, s2, t3

	SUB	a0, t0, a0
	ADD	a1, t1, a1
	SUB	a2, t2, a2
	ADD	a3, t3, a3

	ST	a0,    0 * SIZE(Y1)
	fclr	t0
	ST	a1,    1 * SIZE(Y1)
	addq	Y1, INCY, Y1

	ST	a2,    0 * SIZE(Y1)
	fclr	t1
	ST	a3,    1 * SIZE(Y1)
	addq	Y1, INCY, Y1

	fclr	t2
	lda	J, -1(J)
	fclr	t3
	bgt	J,  $L11
	.align 4

$L20:
	blbc	N,  $L999

	mov	A, A1
	fclr	s0
	fclr	s1
	mov	X, X1

	sra	M,  2, I
	fclr	s2
	fclr	s3
	ble	I,  $L25

	LD	a0,   0 * SIZE(A1)
	LD	a1,   1 * SIZE(A1)
	LD	a4,   2 * SIZE(A1)
	LD	a5,   3 * SIZE(A1)
	LD	a8,   4 * SIZE(A1)
	LD	a9,   5 * SIZE(A1)
	LD	a12,  6 * SIZE(A1)
	LD	a13,  7 * SIZE(A1)

	LD	x0,   0 * SIZE(X1)
	LD	x1,   1 * SIZE(X1)
	LD	x2,   2 * SIZE(X1)

	lda	I,   -1(I)
	ble	I, $L23
	.align 4

$L22:
	ADD3	s0, t0,  s0
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(A1)
	MUL	x0, a0,  t0
	LD	x3,   3 * SIZE(X1)

	ADD4	s1, t1,  s1
	unop
	MUL	x0, a1,  t1
	LD	x0,   4 * SIZE(X1)

	ADD1	s2, t0,  s2
	lda	I,   -1(I)
	MUL	x1, a1,  t0
	LD	a1,   9 * SIZE(A1)

	ADD2	s3, t1,  s3
	unop
	MUL	x1, a0,  t1
	LD	a0,   8 * SIZE(A1)

	ADD3	s0, t0,  s0
	unop
	MUL	x2, a4,  t0
	LD	x1,   5 * SIZE(X1)

	ADD4	s1, t1,  s1
	unop
	MUL	x2, a5,  t1
	LD	x2,   6 * SIZE(X1)

	ADD1	s2, t0,  s2
	unop
	MUL	x3, a5,  t0
	LD	a5,  11 * SIZE(A1)

	ADD2	s3, t1,  s3
	unop
	MUL	x3, a4,  t1
	LD	a4,  10 * SIZE(A1)

	ADD3	s0, t0,  s0
	unop
	MUL	x0, a8,  t0
	LD	x3,   7 * SIZE(X1)

	ADD4	s1, t1,  s1
	unop
	MUL	x0, a9,  t1
	LD	x0,   8 * SIZE(X1)

	ADD1	s2, t0,  s2
	unop
	MUL	x1, a9,  t0
	LD	a9,  13 * SIZE(A1)

	ADD2	s3, t1,  s3
	unop
	MUL	x1, a8,  t1
	LD	a8,  12 * SIZE(A1)

	ADD3	s0, t0,  s0
	unop
	MUL	x2, a12, t0
	LD	x1,   9 * SIZE(X1)

	ADD4	s1, t1,  s1
	lda	A1,   8 * SIZE(A1)
	MUL	x2, a13, t1
	LD	x2,  10 * SIZE(X1)

	ADD1	s2, t0,  s2
	lda	X1,   8 * SIZE(X1)
	MUL	x3, a13, t0
	LD	a13,  7 * SIZE(A1)

	ADD2	s3, t1,  s3
	MUL	x3, a12, t1
	LD	a12,  6 * SIZE(A1)
	bgt	I, $L22
	.align 4

$L23:
	ADD3	s0, t0,  s0
	unop
	MUL	x0, a0,  t0
	LD	x3,   3 * SIZE(X1)

	ADD4	s1, t1,  s1
	unop
	MUL	x0, a1,  t1
	LD	x0,   4 * SIZE(X1)

	ADD1	s2, t0,  s2
	unop
	MUL	x1, a1,  t0
	lda	A1,   8 * SIZE(A1)

	ADD2	s3, t1,  s3
	unop
	MUL	x1, a0,  t1
	LD	x1,   5 * SIZE(X1)

	ADD3	s0, t0,  s0
	unop
	MUL	x2, a4,  t0
	unop

	ADD4	s1, t1,  s1
	unop
	MUL	x2, a5,  t1
	LD	x2,   6 * SIZE(X1)

	ADD1	s2, t0,  s2
	unop
	MUL	x3, a5,  t0
	lda	X1,   8 * SIZE(X1)

	ADD2	s3, t1,  s3
	unop
	MUL	x3, a4,  t1
	LD	x3,  -1 * SIZE(X1)

	ADD3	s0, t0,  s0
	MUL	x0, a8,  t0
	ADD4	s1, t1,  s1
	MUL	x0, a9,  t1

	ADD1	s2, t0,  s2
	MUL	x1, a9,  t0
	ADD2	s3, t1,  s3
	MUL	x1, a8,  t1

	ADD3	s0, t0,  s0
	MUL	x2, a12, t0
	ADD4	s1, t1,  s1
	MUL	x2, a13, t1

	ADD1	s2, t0,  s2
	MUL	x3, a13, t0
	ADD2	s3, t1,  s3
	MUL	x3, a12, t1
	.align 4

$L25:
	and	M, 3, I
	ble	I,  $L28

	LD	a0,   0 * SIZE(A1)
	LD	a1,   1 * SIZE(A1)

	LD	x0,   0 * SIZE(X1)

	lda	I,   -1(I)
	ble	I, $L27
	.align 4

$L26:
	ADD3	s0, t0, s0
	lda	A1,   2 * SIZE(A1)
	MUL	x0, a0, t0
	LD	x1,   1 * SIZE(X1)

	ADD4	s1, t1, s1
	lda	I,   -1(I)
	MUL	x0, a1, t1
	LD	x0,   2 * SIZE(X1)

	ADD1	s0, t0, s0
	lda	X1,   2 * SIZE(X1)
	MUL	x1, a1, t0
	LD	a1,   1 * SIZE(A1)

	ADD2	s1, t1, s1
	MUL	x1, a0, t1
	LD	a0,   0 * SIZE(A1)
	bgt	I, $L26
	.align 4

$L27:
	ADD3	s0, t0, s0
	unop
	MUL	x0, a0, t0
	LD	x1,   1 * SIZE(X1)

	ADD4	s1, t1, s1
	unop
	MUL	x0, a1, t1
	unop

	ADD1	s0, t0, s0
	MUL	x1, a1, t0
	ADD2	s1, t1, s1
	MUL	x1, a0, t1
	.align 4

$L28:
	LD	a0,    0 * SIZE(Y)
	LD	a1,    1 * SIZE(Y)

	ADD3	s0, t0, s0
	ADD4	s1, t1, s1
	ADD3	s2, t2, s2
	ADD4	s3, t3, s3

	ADD	s0, s2, s0
	ADD	s1, s3, s1

	MUL	alpha_r, s0, t0
	MUL	alpha_r, s1, t1

	ADD	a0, t0, a0
	MUL	alpha_i, s1, t0
	ADD	a1, t1, a1
	MUL	alpha_i, s0, t1

	SUB	a0, t0, a0
	ADD	a1, t1, a1

	ST	a0,    0 * SIZE(Y1)
	ST	a1,    1 * SIZE(Y1)
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
