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

#define Y1	$4

#define A1	$5
#define A2	$6
#define A3	$7
#define A4	$8

#define	alpha	$f19

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

	SXADDQ	LDA,  0, LDA

	cmpeq	INCY, SIZE, $0
	bne	$0, $L10

	mov	BUFFER, Y1

	mov	Y, BUFFER
	mov	Y1, Y

	sra	M, 3, I
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
	and	M, 7, I
	ble	I, $L10
	.align 4

$L06:
	ST	$f31,  0 * SIZE(Y1)
	addq	Y1, SIZE, Y1

	lda	I, -1(I)
	bgt	I, $L06
	.align 4

$L10:
	sra	N, 2, J
	ble	J,  $L20
	.align 4

$L11:
	LD	alpha1,  0 * SIZE(X)
	addq	X, INCX, X
	LD	alpha2,  0 * SIZE(X)
	addq	X, INCX, X
	LD	alpha3,  0 * SIZE(X)
	addq	X, INCX, X
	LD	alpha4,  0 * SIZE(X)
	addq	X, INCX, X

	MUL	alpha, alpha1, alpha1
	MUL	alpha, alpha2, alpha2
	MUL	alpha, alpha3, alpha3
	MUL	alpha, alpha4, alpha4

	mov	A, A1
	addq	A,  LDA, A2
	addq	A2, LDA, A3
	addq	A3, LDA, A4
	s4addq	LDA, A, A

	mov	Y, Y1
	ldl	$31, 4 * SIZE(X)

	sra	M,  3, I
	ble	I,  $L15

	LD	a0,  0 * SIZE(A1)
	LD	a1,  1 * SIZE(A1)
	LD	a2,  2 * SIZE(A1)
	LD	a3,  3 * SIZE(A1)

	LD	a4,  0 * SIZE(A2)
	LD	a5,  1 * SIZE(A2)
	LD	a6,  2 * SIZE(A2)
	LD	a7,  3 * SIZE(A2)

	LD	y0,  0 * SIZE(Y1)
	LD	y1,  1 * SIZE(Y1)
	LD	y2,  2 * SIZE(Y1)
	LD	y3,  3 * SIZE(Y1)

	LD	a8,  0 * SIZE(A3)
	LD	a9,  1 * SIZE(A3)
	LD	a10, 2 * SIZE(A3)
	LD	a11, 3 * SIZE(A3)

	LD	y4,  4 * SIZE(Y1)
	LD	y5,  5 * SIZE(Y1)
	LD	y6,  6 * SIZE(Y1)
	LD	y7,  7 * SIZE(Y1)

	MUL	alpha1, a0,  a0
	LD	a12, 0 * SIZE(A4)
	MUL	alpha1, a1,  a1
	LD	a13, 1 * SIZE(A4)
	MUL	alpha1, a2,  a2
	LD	a14, 2 * SIZE(A4)
	MUL	alpha1, a3,  a3
	LD	a15, 3 * SIZE(A4)

	ADD	y0, a0, y0
	LD	a0,   4 * SIZE(A1)
	MUL	alpha2, a4,  a4
	unop

	ADD	y1, a1, y1
	LD	a1,   5 * SIZE(A1)
	MUL	alpha2, a5,  a5
	unop

	ADD	y2, a2, y2
	LD	a2,   6 * SIZE(A1)
	MUL	alpha2, a6,  a6
	unop

	ADD	y3, a3, y3
	LD	a3,   7 * SIZE(A1)
	MUL	alpha2, a7,  a7
	unop

	ADD	y0, a4, y0
	LD	a4,   4 * SIZE(A2)
	MUL	alpha3, a8,  a8
	unop

	ADD	y1, a5, y1
	LD	a5,   5 * SIZE(A2)
	MUL	alpha3, a9,  a9
	lda	I,   -1(I)

	ADD	y2, a6, y2
	LD	a6,   6 * SIZE(A2)
	MUL	alpha3, a10, a10
	unop

	ADD	y3, a7, y3
	LD	a7,   7 * SIZE(A2)
	MUL	alpha3, a11, a11
	unop

	ADD	y0, a8,  y0
	LD	a8,   4 * SIZE(A3)
	MUL	alpha4, a12, a12
	ble	I, $L13
	.align 4

$L12:
	ADD	y1, a9,  y1
	LD	a9,   5 * SIZE(A3)
	MUL	alpha4, a13, a13
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(A1)

	ADD	y2, a10, y2
	LD	a10,  6 * SIZE(A3)
	MUL	alpha4, a14, a14
	unop

	ADD	y3, a11, y3
	LD	a11,  7 * SIZE(A3)
	MUL	alpha4, a15, a15
	lda	I,   -1(I)

	ADD	y0, a12, y0
	LD	a12,  4 * SIZE(A4)
	MUL	alpha1, a0,  a0
	lds	$f31, (PREFETCHSIZE + 0) * SIZE(Y1)

	ADD	y1, a13, y1
	LD	a13,  5 * SIZE(A4)
	MUL	alpha1, a1,  a1
	unop

	ADD	y2, a14, y2
	LD	a14,  6 * SIZE(A4)
	MUL	alpha1, a2,  a2
	unop

	ADD	y3, a15, y3
	LD	a15,  7 * SIZE(A4)
	MUL	alpha1, a3,  a3
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(A2)

	ADD	y4, a0, y4
	ST	y0,   0 * SIZE(Y1)
	MUL	alpha2, a4,  a4
	LD	a0,   8 * SIZE(A1)

	ADD	y5, a1, y5
	ST	y1,   1 * SIZE(Y1)
	MUL	alpha2, a5,  a5
	LD	a1,   9 * SIZE(A1)

	ADD	y6, a2, y6
	ST	y2,   2 * SIZE(Y1)
	MUL	alpha2, a6,  a6
	LD	a2,  10 * SIZE(A1)

	ADD	y7, a3, y7
	ST	y3,   3 * SIZE(Y1)
	MUL	alpha2, a7,  a7
	LD	a3,  11 * SIZE(A1)

	ADD	y4, a4, y4
	LD	a4,   8 * SIZE(A2)
	MUL	alpha3, a8,  a8
	LD	y0,   8 * SIZE(Y1)

	ADD	y5, a5, y5
	LD	a5,   9 * SIZE(A2)
	MUL	alpha3, a9,  a9
	LD	y1,   9 * SIZE(Y1)

	ADD	y6, a6, y6
	LD	a6,  10 * SIZE(A2)
	MUL	alpha3, a10, a10
	LD	y2,  10 * SIZE(Y1)

	ADD	y7, a7, y7
	LD	a7,  11 * SIZE(A2)
	MUL	alpha3, a11, a11
	LD	y3,  11 * SIZE(Y1)

	ADD	y4, a8,  y4
	LD	a8,   8 * SIZE(A3)
	MUL	alpha4, a12, a12
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(A3)

	ADD	y5, a9,  y5
	LD	a9,   9 * SIZE(A3)
	MUL	alpha4, a13, a13
	lda	A1,  8 * SIZE(A1)

	ADD	y6, a10, y6
	LD	a10, 10 * SIZE(A3)
	MUL	alpha4, a14, a14
	lda	A2,  8 * SIZE(A2)

	ADD	y7, a11, y7
	LD	a11, 11 * SIZE(A3)
	MUL	alpha4, a15, a15
	lda	Y1,  8 * SIZE(Y1)

	ADD	y4, a12, y4
	LD	a12,  8 * SIZE(A4)
	MUL	alpha1, a0,  a0
	unop

	ADD	y5, a13, y5
	LD	a13,  9 * SIZE(A4)
	MUL	alpha1, a1,  a1
	lda	A3,  8 * SIZE(A3)

	ADD	y6, a14, y6
	LD	a14, 10 * SIZE(A4)
	MUL	alpha1, a2,  a2
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(A4)

	ADD	y7, a15, y7
	LD	a15, 11 * SIZE(A4)
	MUL	alpha1, a3,  a3
	lda	A4,   8 * SIZE(A4)

	ADD	y0, a0, y0
	LD	a0,   4 * SIZE(A1)
	MUL	alpha2, a4,  a4
	ST	y4,  -4 * SIZE(Y1)

	ADD	y1, a1, y1
	LD	a1,   5 * SIZE(A1)
	MUL	alpha2, a5,  a5
	ST	y5,  -3 * SIZE(Y1)

	ADD	y2, a2, y2
	LD	a2,   6 * SIZE(A1)
	MUL	alpha2, a6,  a6
	ST	y6,  -2 * SIZE(Y1)

	ADD	y3, a3, y3
	LD	a3,   7 * SIZE(A1)
	MUL	alpha2, a7,  a7
	ST	y7,  -1 * SIZE(Y1)

	ADD	y0, a4, y0
	LD	a4,   4 * SIZE(A2)
	MUL	alpha3, a8,  a8
	LD	y4,   4 * SIZE(Y1)

	ADD	y1, a5, y1
	LD	a5,   5 * SIZE(A2)
	MUL	alpha3, a9,  a9
	LD	y5,   5 * SIZE(Y1)

	ADD	y2, a6, y2
	LD	a6,   6 * SIZE(A2)
	MUL	alpha3, a10, a10
	LD	y6,   6 * SIZE(Y1)

	ADD	y3, a7, y3
	LD	a7,   7 * SIZE(A2)
	MUL	alpha3, a11, a11
	LD	y7,   7 * SIZE(Y1)

	ADD	y0, a8,  y0
	LD	a8,   4 * SIZE(A3)
	MUL	alpha4, a12, a12
	bgt	I, $L12
	.align 4

$L13:
	ADD	y1, a9,  y1
	LD	a9,  5 * SIZE(A3)
	MUL	alpha4, a13, a13
	unop

	ADD	y2, a10, y2
	LD	a10, 6 * SIZE(A3)
	MUL	alpha4, a14, a14
	unop

	ADD	y3, a11, y3
	LD	a11, 7 * SIZE(A3)
	MUL	alpha4, a15, a15
	unop

	ADD	y0, a12, y0
	LD	a12, 4 * SIZE(A4)
	MUL	alpha1, a0,  a0
	unop

	ADD	y1, a13, y1
	LD	a13, 5 * SIZE(A4)
	MUL	alpha1, a1,  a1
	unop

	ADD	y2, a14, y2
	LD	a14, 6 * SIZE(A4)
	MUL	alpha1, a2,  a2
	unop

	ADD	y3, a15, y3
	LD	a15, 7 * SIZE(A4)
	MUL	alpha1, a3,  a3
	unop

	ST	y0,  0 * SIZE(Y1)
	ADD	y4, a0, y4
	unop
	MUL	alpha2, a4,  a4

	ST	y1,  1 * SIZE(Y1)
	ADD	y5, a1, y5
	unop
	MUL	alpha2, a5,  a5

	ST	y2,  2 * SIZE(Y1)
	ADD	y6, a2, y6
	unop
	MUL	alpha2, a6,  a6

	ST	y3,  3 * SIZE(Y1)
	ADD	y7, a3, y7
	lda	Y1,  8 * SIZE(Y1)
	MUL	alpha2, a7,  a7

	ADD	y4, a4, y4
	MUL	alpha3, a8,  a8
	ADD	y5, a5, y5
	MUL	alpha3, a9,  a9
	ADD	y6, a6, y6
	MUL	alpha3, a10, a10
	ADD	y7, a7, y7
	MUL	alpha3, a11, a11

	ADD	y4, a8,  y4
	MUL	alpha4, a12, a12
	ADD	y5, a9,  y5
	MUL	alpha4, a13, a13
	ADD	y6, a10, y6
	MUL	alpha4, a14, a14
	ADD	y7, a11, y7
	MUL	alpha4, a15, a15

	ADD	y4, a12, y4
	ADD	y5, a13, y5
	ADD	y6, a14, y6
	ADD	y7, a15, y7

	ST	y4, -4 * SIZE(Y1)
	lda	A1,  8 * SIZE(A1)
	ST	y5, -3 * SIZE(Y1)
	lda	A2,  8 * SIZE(A2)
	ST	y6, -2 * SIZE(Y1)
	lda	A3,  8 * SIZE(A3)
	ST	y7, -1 * SIZE(Y1)
	lda	A4,  8 * SIZE(A4)
	.align 4

$L15:
	and	M, 4, I
	ble	I, $L16

	LD	y0,  0 * SIZE(Y1)
	LD	y1,  1 * SIZE(Y1)
	LD	y2,  2 * SIZE(Y1)
	LD	y3,  3 * SIZE(Y1)

	LD	a0,  0 * SIZE(A1)
	LD	a1,  1 * SIZE(A1)
	LD	a2,  2 * SIZE(A1)
	LD	a3,  3 * SIZE(A1)

	LD	a4,  0 * SIZE(A2)
	LD	a5,  1 * SIZE(A2)
	LD	a6,  2 * SIZE(A2)
	LD	a7,  3 * SIZE(A2)

	LD	a8,  0 * SIZE(A3)
	LD	a9,  1 * SIZE(A3)
	LD	a10, 2 * SIZE(A3)
	LD	a11, 3 * SIZE(A3)

	MUL	alpha1, a0,  a0
	LD	a12, 0 * SIZE(A4)
	MUL	alpha1, a1,  a1
	LD	a13, 1 * SIZE(A4)
	MUL	alpha1, a2,  a2
	LD	a14, 2 * SIZE(A4)
	MUL	alpha1, a3,  a3
	LD	a15, 3 * SIZE(A4)

	ADD	y0, a0, y0
	MUL	alpha2, a4,  a4
	ADD	y1, a1, y1
	MUL	alpha2, a5,  a5
	ADD	y2, a2, y2
	MUL	alpha2, a6,  a6
	ADD	y3, a3, y3
	MUL	alpha2, a7,  a7

	ADD	y0, a4, y0
	MUL	alpha3, a8,  a8
	ADD	y1, a5, y1
	MUL	alpha3, a9,  a9
	ADD	y2, a6, y2
	MUL	alpha3, a10, a10
	ADD	y3, a7, y3
	MUL	alpha3, a11, a11

	ADD	y0, a8,  y0
	MUL	alpha4, a12, a12
	ADD	y1, a9,  y1
	MUL	alpha4, a13, a13
	ADD	y2, a10, y2
	MUL	alpha4, a14, a14
	ADD	y3, a11, y3
	MUL	alpha4, a15, a15

	ADD	y0, a12, y0
	lda	Y1,  4 * SIZE(Y1)
	ADD	y1, a13, y1
	unop

	ADD	y2, a14, y2
	unop
	ADD	y3, a15, y3
	unop

	ST	y0, -4 * SIZE(Y1)
	lda	A1,  4 * SIZE(A1)
	ST	y1, -3 * SIZE(Y1)
	lda	A2,  4 * SIZE(A2)
	ST	y2, -2 * SIZE(Y1)
	lda	A3,  4 * SIZE(A3)
	ST	y3, -1 * SIZE(Y1)
	lda	A4,  4 * SIZE(A4)
	.align 4

$L16:
	and	M, 2, I
	ble	I, $L17

	LD	a0,  0 * SIZE(A1)
	LD	a1,  1 * SIZE(A1)
	LD	a2,  0 * SIZE(A2)
	LD	a3,  1 * SIZE(A2)

	LD	y0,  0 * SIZE(Y1)
	LD	y1,  1 * SIZE(Y1)

	LD	a4,  0 * SIZE(A3)
	MUL	alpha1, a0, a0
	LD	a5,  1 * SIZE(A3)
	MUL	alpha1, a1, a1
	LD	a6,  0 * SIZE(A4)
	MUL	alpha2, a2, a2
	LD	a7,  1 * SIZE(A4)
	MUL	alpha2, a3, a3

	ADD	y0, a0, y0
	MUL	alpha3, a4, a4
	ADD	y1, a1, y1
	MUL	alpha3, a5, a5
	ADD	y0, a2, y0
	MUL	alpha4, a6, a6
	ADD	y1, a3, y1
	MUL	alpha4, a7, a7

	ADD	y0, a4, y0
	lda	A1,  2 * SIZE(A1)
	ADD	y1, a5, y1
	lda	A2,  2 * SIZE(A2)
	ADD	y0, a6, y0
	lda	A3,  2 * SIZE(A3)
	ADD	y1, a7, y1
	lda	A4,  2 * SIZE(A4)

	ST	y0,  0 * SIZE(Y1)
	unop
	ST	y1,  1 * SIZE(Y1)
	lda	Y1,  2 * SIZE(Y1)
	.align 4

$L17:
	blbc	M, $L18

	LD	y0,   0 * SIZE(Y1)

	LD	a0,   0 * SIZE(A1)
	LD	a1,   0 * SIZE(A2)
	LD	a2,   0 * SIZE(A3)
	LD	a3,   0 * SIZE(A4)

	MUL	alpha1, a0, a0
	MUL	alpha2, a1, a1
	MUL	alpha3, a2, a2
	MUL	alpha4, a3, a3

	ADD	y0, a0, y0
	ADD	y0, a1, y0
	ADD	y0, a2, y0
	ADD	y0, a3, y0

	ST	y0,   0 * SIZE(Y1)
	.align 4

$L18:
	lda	J, -1(J)
	bgt	J,  $L11
	.align 4

$L20:
	and	N, 2, J
	ble	J, $L30

	LD	alpha1,  0 * SIZE(X)
	addq	X, INCX, X
	LD	alpha2,  0 * SIZE(X)
	addq	X, INCX, X

	mov	A, A1
	MUL	alpha, alpha1, alpha1
	addq	A,  LDA, A2
	MUL	alpha, alpha2, alpha2

	addq	A2, LDA, A
	mov	Y, Y1

	sra	M,  3, I
	ble	I,  $L25

	LD	a0,  0 * SIZE(A1)
	LD	a1,  1 * SIZE(A1)
	LD	a2,  2 * SIZE(A1)
	LD	a3,  3 * SIZE(A1)

	LD	a4,  0 * SIZE(A2)
	LD	a5,  1 * SIZE(A2)
	LD	a6,  2 * SIZE(A2)
	LD	a7,  3 * SIZE(A2)

	LD	y0,  0 * SIZE(Y1)
	LD	y1,  1 * SIZE(Y1)
	LD	y2,  2 * SIZE(Y1)
	LD	y3,  3 * SIZE(Y1)

	MUL	alpha1, a0,  a0
	LD	y4,  4 * SIZE(Y1)
	MUL	alpha1, a1,  a1
	LD	y5,  5 * SIZE(Y1)
	MUL	alpha1, a2,  a2
	LD	y6,  6 * SIZE(Y1)
	MUL	alpha1, a3,  a3
	LD	y7,  7 * SIZE(Y1)

	ADD	y0, a0, y0
	LD	a0,  4 * SIZE(A1)
	MUL	alpha2, a4,  a4

	ADD	y1, a1, y1
	LD	a1,  5 * SIZE(A1)
	MUL	alpha2, a5,  a5

	ADD	y2, a2, y2
	LD	a2,  6 * SIZE(A1)
	MUL	alpha2, a6,  a6

	ADD	y3, a3, y3
	LD	a3,  7 * SIZE(A1)
	MUL	alpha2, a7,  a7

	ADD	y0, a4, y0
	LD	a4,  4 * SIZE(A2)
	MUL	alpha1, a0,  a0

	ADD	y1, a5, y1
	LD	a5,  5 * SIZE(A2)
	MUL	alpha1, a1,  a1

	ADD	y2, a6, y2
	LD	a6,  6 * SIZE(A2)
	MUL	alpha1, a2,  a2

	ADD	y3, a7, y3
	LD	a7,  7 * SIZE(A2)
	MUL	alpha1, a3,  a3

	lda	I,   -1(I)
	ble	I, $L23
	.align 4

$L22:
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(A1)
	lda	I,   -1(I)
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(A2)
	lda	A2,  8 * SIZE(A2)

	ADD	y4, a0, y4
	ST	y0,  0 * SIZE(Y1)
	MUL	alpha2, a4,  a4
	LD	a0,  8 * SIZE(A1)

	ADD	y5, a1, y5
	ST	y1,  1 * SIZE(Y1)
	MUL	alpha2, a5,  a5
	LD	a1,  9 * SIZE(A1)

	ADD	y6, a2, y6
	ST	y2,  2 * SIZE(Y1)
	MUL	alpha2, a6,  a6
	LD	a2, 10 * SIZE(A1)

	ADD	y7, a3, y7
	ST	y3,  3 * SIZE(Y1)
	MUL	alpha2, a7,  a7
	LD	a3, 11 * SIZE(A1)

	ADD	y4, a4, y4
	LD	a4,  0 * SIZE(A2)
	MUL	alpha1, a0,  a0
	LD	y0,  8 * SIZE(Y1)

	ADD	y5, a5, y5
	LD	a5,  1 * SIZE(A2)
	MUL	alpha1, a1,  a1
	LD	y1,  9 * SIZE(Y1)

	ADD	y6, a6, y6
	LD	a6,  2 * SIZE(A2)
	MUL	alpha1, a2,  a2
	LD	y2, 10 * SIZE(Y1)

	ADD	y7, a7, y7
	LD	a7,  3 * SIZE(A2)
	MUL	alpha1, a3,  a3
	LD	y3, 11 * SIZE(Y1)

	ADD	y0, a0, y0
	ST	y4,  4 * SIZE(Y1)
	MUL	alpha2, a4,  a4
	LD	a0, 12 * SIZE(A1)

	ADD	y1, a1, y1
	ST	y5,  5 * SIZE(Y1)
	MUL	alpha2, a5,  a5
	LD	a1, 13 * SIZE(A1)

	ADD	y2, a2, y2
	ST	y6,  6 * SIZE(Y1)
	MUL	alpha2, a6,  a6
	LD	a2, 14 * SIZE(A1)

	ADD	y3, a3, y3
	ST	y7,  7 * SIZE(Y1)
	MUL	alpha2, a7,  a7
	LD	a3, 15 * SIZE(A1)

	ADD	y0, a4, y0
	LD	a4,  4 * SIZE(A2)
	MUL	alpha1, a0,  a0
	LD	y4, 12 * SIZE(Y1)

	ADD	y1, a5, y1
	LD	a5,  5 * SIZE(A2)
	MUL	alpha1, a1,  a1
	LD	y5, 13 * SIZE(Y1)

	ADD	y2, a6, y2
	LD	a6,  6 * SIZE(A2)
	MUL	alpha1, a2,  a2
	LD	y6, 14 * SIZE(Y1)

	ADD	y3, a7, y3
	LD	a7,  7 * SIZE(A2)
	MUL	alpha1, a3,  a3
	LD	y7, 15 * SIZE(Y1)

	lds	$f31, (PREFETCHSIZE + 0) * SIZE(Y1)
	lda	A1,  8 * SIZE(A1)
	lda	Y1,  8 * SIZE(Y1)
	bgt	I, $L22
	.align 4

$L23:
	ADD	y4, a0, y4
	ST	y0,  0 * SIZE(Y1)
	MUL	alpha2, a4,  a4
	unop

	ADD	y5, a1, y5
	ST	y1,  1 * SIZE(Y1)
	MUL	alpha2, a5,  a5
	unop

	ADD	y6, a2, y6
	ST	y2,  2 * SIZE(Y1)
	MUL	alpha2, a6,  a6
	unop

	ADD	y7, a3, y7
	ST	y3,  3 * SIZE(Y1)
	MUL	alpha2, a7,  a7
	unop

	ADD	y4, a4, y4
	ADD	y5, a5, y5
	ADD	y6, a6, y6
	ADD	y7, a7, y7

	ST	y4,  4 * SIZE(Y1)
	lda	A1,  8 * SIZE(A1)
	ST	y5,  5 * SIZE(Y1)
	lda	A2,  8 * SIZE(A2)

	ST	y6,  6 * SIZE(Y1)
	unop
	ST	y7,  7 * SIZE(Y1)
	lda	Y1,  8 * SIZE(Y1)
	.align 4

$L25:
	and	M, 4, I
	ble	I, $L26

	LD	y0,  0 * SIZE(Y1)
	LD	y1,  1 * SIZE(Y1)
	LD	y2,  2 * SIZE(Y1)
	LD	y3,  3 * SIZE(Y1)

	LD	a0,  0 * SIZE(A1)
	LD	a1,  1 * SIZE(A1)
	LD	a2,  2 * SIZE(A1)
	LD	a3,  3 * SIZE(A1)

	MUL	alpha1, a0,  a0
	LD	a4,  0 * SIZE(A2)
	MUL	alpha1, a1,  a1
	LD	a5,  1 * SIZE(A2)
	MUL	alpha1, a2,  a2
	LD	a6,  2 * SIZE(A2)
	MUL	alpha1, a3,  a3
	LD	a7,  3 * SIZE(A2)

	ADD	y0, a0, y0
	MUL	alpha2, a4,  a4
	ADD	y1, a1, y1
	MUL	alpha2, a5,  a5
	ADD	y2, a2, y2
	MUL	alpha2, a6,  a6
	ADD	y3, a3, y3
	MUL	alpha2, a7,  a7

	ADD	y0, a4, y0
	lda	Y1,  4 * SIZE(Y1)
	ADD	y1, a5, y1
	unop
	ADD	y2, a6, y2
	unop
	ADD	y3, a7, y3
	unop

	ST	y0, -4 * SIZE(Y1)
	lda	A1,  4 * SIZE(A1)
	ST	y1, -3 * SIZE(Y1)
	lda	A2,  4 * SIZE(A2)
	ST	y2, -2 * SIZE(Y1)
	lda	A3,  4 * SIZE(A3)
	ST	y3, -1 * SIZE(Y1)
	lda	A4,  4 * SIZE(A4)
	.align 4

$L26:
	and	M, 2, I
	ble	I, $L27

	LD	a0,  0 * SIZE(A1)
	LD	a1,  1 * SIZE(A1)
	LD	a2,  0 * SIZE(A2)
	LD	a3,  1 * SIZE(A2)

	LD	y0,  0 * SIZE(Y1)
	LD	y1,  1 * SIZE(Y1)

	MUL	alpha1, a0, a0
	MUL	alpha1, a1, a1
	MUL	alpha2, a2, a2
	MUL	alpha2, a3, a3

	ADD	y0, a0, y0
	lda	A1,  2 * SIZE(A1)
	ADD	y1, a1, y1
	lda	A2,  2 * SIZE(A2)
	ADD	y0, a2, y0
	unop
	ADD	y1, a3, y1
	unop

	ST	y0,  0 * SIZE(Y1)
	unop
	ST	y1,  1 * SIZE(Y1)
	lda	Y1,  2 * SIZE(Y1)
	.align 4

$L27:
	blbc	M, $L30

	LD	y0,   0 * SIZE(Y1)

	LD	a0,   0 * SIZE(A1)
	LD	a1,   0 * SIZE(A2)

	MUL	alpha1, a0, a0
	MUL	alpha2, a1, a1

	ADD	y0, a0, y0
	ADD	y0, a1, y0

	ST	y0,   0 * SIZE(Y1)
	.align 4

$L30:
	blbc	N, $L990

	LD	alpha1,  0 * SIZE(X)
	mov	A, A1
	MUL	alpha, alpha1, alpha1
	mov	Y, Y1

	sra	M,  3, I
	ble	I,  $L35

	LD	a0,  0 * SIZE(A1)
	LD	a1,  1 * SIZE(A1)
	LD	a2,  2 * SIZE(A1)
	LD	a3,  3 * SIZE(A1)
	LD	a4,  4 * SIZE(A1)
	LD	a5,  5 * SIZE(A1)
	LD	a6,  6 * SIZE(A1)
	LD	a7,  7 * SIZE(A1)

	LD	y0,  0 * SIZE(Y1)
	LD	y1,  1 * SIZE(Y1)
	LD	y2,  2 * SIZE(Y1)
	LD	y3,  3 * SIZE(Y1)
	LD	y4,  4 * SIZE(Y1)
	LD	y5,  5 * SIZE(Y1)
	LD	y6,  6 * SIZE(Y1)
	LD	y7,  7 * SIZE(Y1)

	MUL	alpha1, a0,  a0
	MUL	alpha1, a1,  a1
	MUL	alpha1, a2,  a2
	MUL	alpha1, a3,  a3

	lda	I,   -1(I)
	ble	I, $L33
	.align 4

$L32:
	ADD	y0, a0, y0
	LD	y4,  4 * SIZE(Y1)
	MUL	alpha1, a4,  a4
	LD	a0,  8 * SIZE(A1)

	ADD	y1, a1, y1
	LD	y5,  5 * SIZE(Y1)
	MUL	alpha1, a5,  a5
	LD	a1,  9 * SIZE(A1)

	ADD	y2, a2, y2
	LD	y6,  6 * SIZE(Y1)
	MUL	alpha1, a6,  a6
	LD	a2, 10 * SIZE(A1)

	ADD	y3, a3, y3
	LD	y7,  7 * SIZE(Y1)
	MUL	alpha1, a7,  a7
	LD	a3, 11 * SIZE(A1)

	ST	y0,  0 * SIZE(Y1)
	ST	y1,  1 * SIZE(Y1)
	ST	y2,  2 * SIZE(Y1)
	ST	y3,  3 * SIZE(Y1)

	ADD	y4, a4, y4
	LD	y0,  8 * SIZE(Y1)
	MUL	alpha1, a0,  a0
	LD	a4, 12 * SIZE(A1)

	ADD	y5, a5, y5
	LD	y1,  9 * SIZE(Y1)
	MUL	alpha1, a1,  a1
	LD	a5, 13 * SIZE(A1)

	ADD	y6, a6, y6
	LD	y2, 10 * SIZE(Y1)
	MUL	alpha1, a2,  a2
	LD	a6, 14 * SIZE(A1)

	ADD	y7, a7, y7
	LD	y3, 11 * SIZE(Y1)
	MUL	alpha1, a3,  a3
	LD	a7, 15 * SIZE(A1)

	ST	y4,  4 * SIZE(Y1)
	lda	I,   -1(I)
	ST	y5,  5 * SIZE(Y1)
	lda	A1,  8 * SIZE(A1)

	ST	y6,  6 * SIZE(Y1)
	ldl	$31, (PREFETCHSIZE + 0) * SIZE(A1)
	ST	y7,  7 * SIZE(Y1)
	lds	$f31, (PREFETCHSIZE + 0) * SIZE(Y1)

	lda	Y1,  8 * SIZE(Y1)
	bgt	I, $L32
	.align 4

$L33:
	ADD	y0, a0, y0
	LD	y4,  4 * SIZE(Y1)
	MUL	alpha1, a4,  a4
	unop

	ADD	y1, a1, y1
	LD	y5,  5 * SIZE(Y1)
	MUL	alpha1, a5,  a5
	unop

	ADD	y2, a2, y2
	LD	y6,  6 * SIZE(Y1)
	MUL	alpha1, a6,  a6
	unop

	ADD	y3, a3, y3
	LD	y7,  7 * SIZE(Y1)
	MUL	alpha1, a7,  a7
	unop

	ADD	y4, a4, y4
	ST	y0,  0 * SIZE(Y1)
	ADD	y5, a5, y5
	ST	y1,  1 * SIZE(Y1)
	ADD	y6, a6, y6
	ST	y2,  2 * SIZE(Y1)
	ADD	y7, a7, y7
	ST	y3,  3 * SIZE(Y1)

	ST	y4,  4 * SIZE(Y1)
	unop
	ST	y5,  5 * SIZE(Y1)
	unop

	ST	y6,  6 * SIZE(Y1)
	lda	A1,  8 * SIZE(A1)
	ST	y7,  7 * SIZE(Y1)
	lda	Y1,  8 * SIZE(Y1)
	.align 4

$L35:
	and	M, 4, I
	ble	I, $L36

	LD	a0,  0 * SIZE(A1)
	LD	a1,  1 * SIZE(A1)
	LD	a2,  2 * SIZE(A1)
	LD	a3,  3 * SIZE(A1)

	MUL	alpha1, a0,  a0
	LD	y0,  0 * SIZE(Y1)
	MUL	alpha1, a1,  a1
	LD	y1,  1 * SIZE(Y1)
	MUL	alpha1, a2,  a2
	LD	y2,  2 * SIZE(Y1)
	MUL	alpha1, a3,  a3
	LD	y3,  3 * SIZE(Y1)

	ADD	y0, a0, y0
	ADD	y1, a1, y1
	ADD	y2, a2, y2
	ADD	y3, a3, y3

	ST	y0,  0 * SIZE(Y1)
	lda	A1,  4 * SIZE(A1)
	ST	y1,  1 * SIZE(Y1)
	lda	A2,  4 * SIZE(A2)
	ST	y2,  2 * SIZE(Y1)
	unop
	ST	y3,  3 * SIZE(Y1)
	lda	Y1,  4 * SIZE(Y1)
	.align 4

$L36:
	and	M, 2, I
	ble	I, $L37

	LD	a0,  0 * SIZE(A1)
	LD	a1,  1 * SIZE(A1)

	LD	y0,  0 * SIZE(Y1)
	MUL	alpha1, a0, a0
	LD	y1,  1 * SIZE(Y1)
	MUL	alpha1, a1, a1

	ADD	y0, a0, y0
	ADD	y1, a1, y1

	ST	y0,  0 * SIZE(Y1)
	lda	A1,  2 * SIZE(A1)
	ST	y1,  1 * SIZE(Y1)
	lda	Y1,  2 * SIZE(Y1)
	.align 4

$L37:
	blbc	M, $L990

	LD	y0,   0 * SIZE(Y1)
	LD	a0,   0 * SIZE(A1)

	MUL	alpha1, a0, a0

	ADD	y0, a0, y0
	ST	y0,   0 * SIZE(Y1)
	.align 4

$L990:
	cmpeq	INCY, SIZE, $0
	bne	$0, $L999

	mov	BUFFER, Y1

	sra	M, 3, I
	ble	I, $L995
	.align 4

$L992:
	LD	a0,  0 * SIZE(BUFFER)
	addq	BUFFER, INCY, BUFFER
	LD	a1,  0 * SIZE(BUFFER)
	addq	BUFFER, INCY, BUFFER
	LD	a2,  0 * SIZE(BUFFER)
	addq	BUFFER, INCY, BUFFER
	LD	a3,  0 * SIZE(BUFFER)
	addq	BUFFER, INCY, BUFFER

	LD	y0,  0 * SIZE(Y)
	LD	y1,  1 * SIZE(Y)
	LD	y2,  2 * SIZE(Y)
	LD	y3,  3 * SIZE(Y)

	LD	a4,  0 * SIZE(BUFFER)
	addq	BUFFER, INCY, BUFFER
	LD	a5,  0 * SIZE(BUFFER)
	addq	BUFFER, INCY, BUFFER
	LD	a6,  0 * SIZE(BUFFER)
	addq	BUFFER, INCY, BUFFER
	LD	a7,  0 * SIZE(BUFFER)
	addq	BUFFER, INCY, BUFFER

	LD	y4,  4 * SIZE(Y)
	LD	y5,  5 * SIZE(Y)
	LD	y6,  6 * SIZE(Y)
	LD	y7,  7 * SIZE(Y)

	ADD	a0, y0, a0
	ADD	a1, y1, a1
	ADD	a2, y2, a2
	ADD	a3, y3, a3
	ADD	a4, y4, a4
	ADD	a5, y5, a5
	ADD	a6, y6, a6
	ADD	a7, y7, a7

	ST	a0,  0 * SIZE(Y1)
	addq	Y1, INCY, Y1
	ST	a1,  0 * SIZE(Y1)
	addq	Y1, INCY, Y1
	ST	a2,  0 * SIZE(Y1)
	addq	Y1, INCY, Y1
	ST	a3,  0 * SIZE(Y1)
	addq	Y1, INCY, Y1

	ST	a4,  0 * SIZE(Y1)
	addq	Y1, INCY, Y1
	ST	a5,  0 * SIZE(Y1)
	addq	Y1, INCY, Y1
	ST	a6,  0 * SIZE(Y1)
	addq	Y1, INCY, Y1
	ST	a7,  0 * SIZE(Y1)
	addq	Y1, INCY, Y1

	lda	I, -1(I)
	lda	Y,   8 * SIZE(Y)
	bgt	I, $L992
	.align 4

$L995:
	and	M, 7, I
	ble	I, $L999
	.align 4

$L996:
	LD	a0,  0 * SIZE(BUFFER)
	addq	BUFFER, INCY, BUFFER

	LD	y0,  0 * SIZE(Y)
	lda	Y,   1 * SIZE(Y)

	ADD	a0, y0, a0

	ST	a0,  0 * SIZE(Y1)
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
