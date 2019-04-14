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

#define PREFETCHSIZE	88

#define N	$16
#define X	$17
#define INCX	$18
#define Y	$19
#define INCY	$20

#define I	$5

#define s0	$f0
#define s1	$f30
#define s2	$f1
#define s3	$f2

#define a0	$f10
#define a1	$f11
#define a2	$f12
#define a3	$f13
#define a4	$f14
#define a5	$f15
#define a6	$f16
#define a7	$f17

#define b0	$f18
#define b1	$f19
#define b2	$f20
#define b3	$f21
#define b4	$f22
#define b5	$f23
#define b6	$f24
#define b7	$f25

#define t0	$f26
#define t1	$f27
#define t2	$f28
#define t3	$f29


	PROLOGUE
	PROFCODE
	.frame	$sp, 16, $26, 0

	lda	$sp, -16($sp)
	fclr	s0
	stt	$f2,   0($sp)
	fclr	s1

	fclr	s2
	nop
	fclr	s3
	ble	N,  $L999

	fclr	t0
	cmpeq	INCX, 1,  $21
	fclr	t1
	cmpeq	INCY, 1,  $22
	fclr	t2
	and	$21, $22, $22
	fclr	t3
	beq	$22, $L20

#ifndef DOUBLE
	srl	N, 4, I
	ble	I, $L15

	LD	a0,  0 * SIZE(X)
	LD	a1,  1 * SIZE(X)
	LD	b0,  0 * SIZE(Y)
	LD	b1,  1 * SIZE(Y)

	LD	a2,  2 * SIZE(X)
	LD	a3,  3 * SIZE(X)
	LD	b2,  2 * SIZE(Y)
	LD	b3,  3 * SIZE(Y)

	LD	a4,  4 * SIZE(X)
	LD	a5,  5 * SIZE(X)
	LD	b4,  4 * SIZE(Y)
	LD	b5,  5 * SIZE(Y)

	LD	a6,  6 * SIZE(X)
	LD	a7,  7 * SIZE(X)
	addq	X, 16 * SIZE, X
	subq	I,   1, I

	addq	Y, 16 * SIZE, Y
	ble	I, $L13
	.align 4

$L12:
	ldl	$31, PREFETCHSIZE * 2 * SIZE(X)
	subq	I,   1, I
	ldl	$31, PREFETCHSIZE * 2 * SIZE(Y)
	addq	X, 16 * SIZE, X

	ADD	s0, t0, s0
	LD	b6, -10 * SIZE(Y)
	MUL	a0, b0, t0
	LD	b7,  -9 * SIZE(Y)

	ADD	s1, t1, s1
	LD	a0, -24 * SIZE(X)
	MUL	a1, b1, t1
	LD	a1, -23 * SIZE(X)

	ADD	s2, t2, s2
	LD	b0,  -8 * SIZE(Y)
	MUL	a2, b2, t2
	LD	b1,  -7 * SIZE(Y)

	ADD	s3, t3, s3
	LD	a2, -22 * SIZE(X)
	MUL	a3, b3, t3
	LD	a3, -21 * SIZE(X)

	ADD	s0, t0, s0
	LD	b2,  -6 * SIZE(Y)
	MUL	a4, b4, t0
	LD	b3,  -5 * SIZE(Y)

	ADD	s1, t1, s1
	LD	a4, -20 * SIZE(X)
	MUL	a5, b5, t1
	LD	a5, -19 * SIZE(X)

	ADD	s2, t2, s2
	LD	b4,  -4 * SIZE(Y)
	MUL	a6, b6, t2
	LD	b5,  -3 * SIZE(Y)

	ADD	s3, t3, s3
	LD	a6, -18 * SIZE(X)
	MUL	a7, b7, t3
	LD	a7, -17 * SIZE(X)

	ADD	s0, t0, s0
	LD	b6,  -2 * SIZE(Y)
	MUL	a0, b0, t0
	LD	b7,  -1 * SIZE(Y)

	ADD	s1, t1, s1
	LD	a0, -16 * SIZE(X)
	MUL	a1, b1, t1
	LD	a1, -15 * SIZE(X)

	ADD	s2, t2, s2
	LD	b0,   0 * SIZE(Y)
	MUL	a2, b2, t2
	LD	b1,   1 * SIZE(Y)

	ADD	s3, t3, s3
	LD	a2, -14 * SIZE(X)
	MUL	a3, b3, t3
	LD	a3, -13 * SIZE(X)

	ADD	s0, t0, s0
	LD	b2,   2 * SIZE(Y)
	MUL	a4, b4, t0
	LD	b3,   3 * SIZE(Y)

	ADD	s1, t1, s1
	LD	a4, -12 * SIZE(X)
	MUL	a5, b5, t1
	LD	a5, -11 * SIZE(X)

	ADD	s2, t2, s2
	LD	b4,   4 * SIZE(Y)
	MUL	a6, b6, t2
	LD	b5,   5 * SIZE(Y)

	ADD	s3, t3, s3
	LD	a6, -10 * SIZE(X)
	MUL	a7, b7, t3
	LD	a7,  -9 * SIZE(X)

	addq	Y, 16 * SIZE, Y
	bgt	I, $L12
	nop
	fnop
	.align 4

$L13:
	ADD	s0, t0, s0
	LD	b6,-10 * SIZE(Y)
	MUL	a0, b0, t0
	LD	b7, -9 * SIZE(Y)

	ADD	s1, t1, s1
	LD	a0, -8 * SIZE(X)
	MUL	a1, b1, t1
	LD	a1, -7 * SIZE(X)

	ADD	s2, t2, s2
	LD	b0, -8 * SIZE(Y)
	MUL	a2, b2, t2
	LD	b1, -7 * SIZE(Y)

	ADD	s3, t3, s3
	LD	a2, -6 * SIZE(X)
	MUL	a3, b3, t3
	LD	a3, -5 * SIZE(X)

	ADD	s0, t0, s0
	LD	b2, -6 * SIZE(Y)
	MUL	a4, b4, t0
	LD	b3, -5 * SIZE(Y)

	ADD	s1, t1, s1
	LD	a4, -4 * SIZE(X)
	MUL	a5, b5, t1
	LD	a5, -3 * SIZE(X)

	ADD	s2, t2, s2
	LD	b4, -4 * SIZE(Y)
	MUL	a6, b6, t2
	LD	b5, -3 * SIZE(Y)

	ADD	s3, t3, s3
	LD	a6, -2 * SIZE(X)
	MUL	a7, b7, t3
	LD	a7, -1 * SIZE(X)

	ADD	s0, t0, s0
	LD	b6, -2 * SIZE(Y)
	MUL	a0, b0, t0
	LD	b7, -1 * SIZE(Y)
	ADD	s1, t1, s1
	MUL	a1, b1, t1

	ADD	s2, t2, s2
	MUL	a2, b2, t2
	ADD	s3, t3, s3
	MUL	a3, b3, t3

	ADD	s0, t0, s0
	MUL	a4, b4, t0
	ADD	s1, t1, s1
	MUL	a5, b5, t1
	ADD	s2, t2, s2
	MUL	a6, b6, t2
	ADD	s3, t3, s3
	MUL	a7, b7, t3
	.align 4

$L15:
	ADD	s0, t0, s0
	and	N, 15, I
	ADD	s1, t1, s1
	ble	I, $L18
	.align 4

#else

	srl	N, 3, I
	ble	I, $L15

	LD	a0,  0 * SIZE(X)
	LD	a1,  1 * SIZE(X)
	LD	b0,  0 * SIZE(Y)
	LD	b1,  1 * SIZE(Y)

	LD	a2,  2 * SIZE(X)
	LD	a3,  3 * SIZE(X)
	LD	b2,  2 * SIZE(Y)
	LD	b3,  3 * SIZE(Y)

	LD	a4,  4 * SIZE(X)
	LD	a5,  5 * SIZE(X)
	LD	b4,  4 * SIZE(Y)
	LD	b5,  5 * SIZE(Y)

	LD	a6,  6 * SIZE(X)
	LD	a7,  7 * SIZE(X)
	addq	X, 8 * SIZE, X
	subq	I,   1, I

	addq	Y, 8 * SIZE, Y
	ble	I, $L13
	.align 4

$L12:
	ldl	$31, PREFETCHSIZE * SIZE(X)
	subq	I,   1, I
	ldl	$31, PREFETCHSIZE * SIZE(Y)
	addq	X, 8 * SIZE, X

	ADD	s0, t0, s0
	LD	b6, -2 * SIZE(Y)
	MUL	a0, b0, t0
	LD	b7, -1 * SIZE(Y)

	ADD	s1, t1, s1
	LD	a0, -8 * SIZE(X)
	MUL	a1, b1, t1
	LD	a1, -7 * SIZE(X)

	ADD	s2, t2, s2
	LD	b0,  0 * SIZE(Y)
	MUL	a2, b2, t2
	LD	b1,  1 * SIZE(Y)

	ADD	s3, t3, s3
	LD	a2, -6 * SIZE(X)
	MUL	a3, b3, t3
	LD	a3, -5 * SIZE(X)

	ADD	s0, t0, s0
	LD	b2,  2 * SIZE(Y)
	MUL	a4, b4, t0
	LD	b3,  3 * SIZE(Y)

	ADD	s1, t1, s1
	LD	a4, -4 * SIZE(X)
	MUL	a5, b5, t1
	LD	a5, -3 * SIZE(X)

	ADD	s2, t2, s2
	LD	b4,  4 * SIZE(Y)
	MUL	a6, b6, t2
	LD	b5,  5 * SIZE(Y)

	ADD	s3, t3, s3
	LD	a6, -2 * SIZE(X)
	MUL	a7, b7, t3
	LD	a7, -1 * SIZE(X)

	addq	Y, 8 * SIZE, Y
	bgt	I, $L12
	nop
	fnop
	.align 4

$L13:
	ADD	s0, t0, s0
	LD	b6, -2 * SIZE(Y)
	MUL	a0, b0, t0
	LD	b7, -1 * SIZE(Y)
	ADD	s1, t1, s1
	MUL	a1, b1, t1

	ADD	s2, t2, s2
	MUL	a2, b2, t2
	ADD	s3, t3, s3
	MUL	a3, b3, t3

	ADD	s0, t0, s0
	MUL	a4, b4, t0
	ADD	s1, t1, s1
	MUL	a5, b5, t1
	ADD	s2, t2, s2
	MUL	a6, b6, t2
	ADD	s3, t3, s3
	MUL	a7, b7, t3
	.align 4

$L15:
	ADD	s0, t0, s0
	and	N, 7, I
	ADD	s1, t1, s1
	ble	I, $L18
	.align 4

#endif

$L16:
	LD	a0, 0 * SIZE(X)
	addq	X, SIZE, X
	LD	b0, 0 * SIZE(Y)
	addq	Y, SIZE, Y

	ADD	s2, t2, s2
	MUL	a0, b0, t2
	subq	I, 1, I
	bgt	I, $L16
	.align 4

$L18:
	ADD	s2, t2, s2
	ADD	s3, t3, s3
	br	$L999
	.align 4

$L20:
	srl	N, 2, I
	ble	I, $L25

	LD	a0,  0 * SIZE(X)
	SXADDQ	INCX, X, X
	LD	b0,  0 * SIZE(Y)
	SXADDQ	INCY, Y, Y
	LD	a1,  0 * SIZE(X)
	SXADDQ	INCX, X, X
	LD	b1,  0 * SIZE(Y)
	SXADDQ	INCY, Y, Y

	LD	a2,  0 * SIZE(X)
	SXADDQ	INCX, X, X
	LD	b2,  0 * SIZE(Y)
	SXADDQ	INCY, Y, Y
	LD	a3,  0 * SIZE(X)
	SXADDQ	INCX, X, X
	LD	b3,  0 * SIZE(Y)
	subq	I,   1, I

	SXADDQ	INCY, Y, Y
	ble	I, $L23
	.align 4

$L22:
	ADD	s0, t0, s0
	MUL	a0, b0, t0
	ADD	s1, t1, s1
	MUL	a1, b1, t1
	ADD	s2, t2, s2
	MUL	a2, b2, t2
	ADD	s3, t3, s3
	MUL	a3, b3, t3

	LD	a0,  0 * SIZE(X)
	SXADDQ	INCX, X, X
	LD	b0,  0 * SIZE(Y)
	SXADDQ	INCY, Y, Y
	LD	a1,  0 * SIZE(X)
	SXADDQ	INCX, X, X
	LD	b1,  0 * SIZE(Y)
	SXADDQ	INCY, Y, Y

	LD	a2,  0 * SIZE(X)
	SXADDQ	INCX, X, X
	LD	b2,  0 * SIZE(Y)
	SXADDQ	INCY, Y, Y
	LD	a3,  0 * SIZE(X)
	SXADDQ	INCX, X, X
	LD	b3,  0 * SIZE(Y)
	SXADDQ	INCY, Y, Y

	subq	I,   1, I
	bgt	I, $L22
	nop
	fnop
	.align 4

$L23:
	ADD	s0, t0, s0
	MUL	a0, b0, t0
	ADD	s1, t1, s1
	MUL	a1, b1, t1
	ADD	s2, t2, s2
	MUL	a2, b2, t2
	ADD	s3, t3, s3
	MUL	a3, b3, t3
	.align 4

$L25:
	ADD	s0, t0, s0
	and	N, 3, I
	ADD	s1, t1, s1
	ble	I, $L28
	.align 4

$L26:
	LD	a0, 0 * SIZE(X)
	SXADDQ	INCX, X, X
	LD	b0, 0 * SIZE(Y)
	SXADDQ	INCY, Y, Y

	ADD	s2, t2, s2
	MUL	a0, b0, t2
	subq	I, 1, I
	bgt	I, $L26
	.align 4

$L28:
	ADD	s2, t2, s2
	ADD	s3, t3, s3
	.align 4

$L999:
	ADD	s2, s3, s2
	ldt	$f2,  0($sp)
	ADD	s0, s1, s0
	lda	$sp, 16($sp)

	ADD	s0, s2, s0
	ret

	EPILOGUE
