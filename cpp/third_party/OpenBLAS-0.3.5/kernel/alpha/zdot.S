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
#define XX	$21
#define YY	$23

#define I	$5

#define s0	$f0
#define s1	$f1
#define s2	$f2
#define s3	$f30

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
	addq	INCX, INCX, INCX
	fclr	s3
	ble	N,  $L999

	addq	INCY, INCY, INCY
	fclr	t0
	fclr	t1
	fclr	t2
	fclr	t3

	srl	N, 3, I
	ble	I, $L25

	LD	a0, 0 * SIZE(X)
	LD	a1, 1 * SIZE(X)
	LD	b0, 0 * SIZE(Y)
	LD	b1, 1 * SIZE(Y)

	SXADDQ	INCX, X, X
	SXADDQ	INCY, Y, Y

	LD	a2, 0 * SIZE(X)
	LD	a3, 1 * SIZE(X)
	LD	b2, 0 * SIZE(Y)
	LD	b3, 1 * SIZE(Y)

	SXADDQ	INCX, X, X
	SXADDQ	INCY, Y, Y

	LD	a4, 0 * SIZE(X)
	LD	a5, 1 * SIZE(X)
	LD	b4, 0 * SIZE(Y)
	LD	b5, 1 * SIZE(Y)

	SXADDQ	INCX, X, X
	SXADDQ	INCY, Y, Y

	LD	a6, 0 * SIZE(X)
	LD	b6, 0 * SIZE(Y)

	subq	I,   1, I
	ble	I, $L23
	.align 4

$L22:
	ADD	s0, t0, s0
	LD	a7, 1 * SIZE(X)
	MUL	a0, b0, t0
	LD	b7, 1 * SIZE(Y)

	ADD	s1, t1, s1
	ldl	$31, PREFETCHSIZE * SIZE(X)
	MUL	a0, b1, t1
	SXADDQ	INCX, X, X

	ADD	s2, t2, s2
	ldl	$31, PREFETCHSIZE * SIZE(Y)
	MUL	a1, b0, t2
	SXADDQ	INCY, Y, Y

	ADD	s3, t3, s3
	LD	a0, 0 * SIZE(X)
	MUL	a1, b1, t3
	LD	a1, 1 * SIZE(X)

	ADD	s0, t0, s0
	LD	b0, 0 * SIZE(Y)
	MUL	a2, b2, t0
	LD	b1, 1 * SIZE(Y)

	ADD	s1, t1, s1
	SXADDQ	INCX, X, X
	MUL	a2, b3, t1
	SXADDQ	INCY, Y, Y

	ADD	s2, t2, s2
	unop
	MUL	a3, b2, t2
	unop

	ADD	s3, t3, s3
	LD	a2, 0 * SIZE(X)
	MUL	a3, b3, t3
	LD	a3, 1 * SIZE(X)

	ADD	s0, t0, s0
	LD	b2, 0 * SIZE(Y)
	MUL	a4, b4, t0
	LD	b3, 1 * SIZE(Y)

	ADD	s1, t1, s1
	SXADDQ	INCX, X, X
	MUL	a4, b5, t1
	SXADDQ	INCY, Y, Y

	ADD	s2, t2, s2
	unop
	MUL	a5, b4, t2
	unop

	ADD	s3, t3, s3
	LD	a4, 0 * SIZE(X)
	MUL	a5, b5, t3
	LD	a5, 1 * SIZE(X)

	ADD	s0, t0, s0
	LD	b4, 0 * SIZE(Y)
	MUL	a6, b6, t0
	LD	b5, 1 * SIZE(Y)

	ADD	s1, t1, s1
	SXADDQ	INCX, X, X
	MUL	a6, b7, t1
	SXADDQ	INCY, Y, Y

	ADD	s2, t2, s2
	unop
	MUL	a7, b6, t2
	unop

	ADD	s3, t3, s3
	LD	a6, 0 * SIZE(X)
	MUL	a7, b7, t3
	LD	a7, 1 * SIZE(X)

	ADD	s0, t0, s0
	LD	b6, 0 * SIZE(Y)
	MUL	a0, b0, t0
	LD	b7, 1 * SIZE(Y)

	ADD	s1, t1, s1
	SXADDQ	INCX, X, X
	MUL	a0, b1, t1
	SXADDQ	INCY, Y, Y

	ADD	s2, t2, s2
	unop
	MUL	a1, b0, t2
	unop

	ADD	s3, t3, s3
	LD	a0, 0 * SIZE(X)
	MUL	a1, b1, t3
	LD	a1, 1 * SIZE(X)

	ADD	s0, t0, s0
	LD	b0, 0 * SIZE(Y)
	MUL	a2, b2, t0
	LD	b1, 1 * SIZE(Y)

	ADD	s1, t1, s1
	SXADDQ	INCX, X, X
	MUL	a2, b3, t1
	SXADDQ	INCY, Y, Y

	ADD	s2, t2, s2
	unop
	MUL	a3, b2, t2
	unop

	ADD	s3, t3, s3
	LD	a2, 0 * SIZE(X)
	MUL	a3, b3, t3
	LD	a3, 1 * SIZE(X)

	ADD	s0, t0, s0
	LD	b2, 0 * SIZE(Y)
	MUL	a4, b4, t0
	LD	b3, 1 * SIZE(Y)

	ADD	s1, t1, s1
	SXADDQ	INCX, X, X
	MUL	a4, b5, t1
	SXADDQ	INCY, Y, Y

	ADD	s2, t2, s2
	unop
	MUL	a5, b4, t2
	subq	I,   1, I

	ADD	s3, t3, s3
	LD	a4, 0 * SIZE(X)
	MUL	a5, b5, t3
	LD	a5, 1 * SIZE(X)

	ADD	s0, t0, s0
	LD	b4, 0 * SIZE(Y)
	MUL	a6, b6, t0
	LD	b5, 1 * SIZE(Y)

	ADD	s1, t1, s1
	SXADDQ	INCX, X, X
	MUL	a6, b7, t1
	SXADDQ	INCY, Y, Y

	ADD	s2, t2, s2
	LD	a6, 0 * SIZE(X)
	MUL	a7, b6, t2
	unop

	ADD	s3, t3, s3
	LD	b6, 0 * SIZE(Y)
	MUL	a7, b7, t3
	bgt	I, $L22
	.align 4

$L23:
	ADD	s0, t0, s0
	LD	a7, 1 * SIZE(X)
	MUL	a0, b0, t0
	LD	b7, 1 * SIZE(Y)

	ADD	s1, t1, s1
	SXADDQ	INCX, X, X
	MUL	a0, b1, t1
	SXADDQ	INCY, Y, Y

	ADD	s2, t2, s2
	unop
	MUL	a1, b0, t2
	unop

	ADD	s3, t3, s3
	LD	a0, 0 * SIZE(X)
	MUL	a1, b1, t3
	LD	a1, 1 * SIZE(X)

	ADD	s0, t0, s0
	LD	b0, 0 * SIZE(Y)
	MUL	a2, b2, t0
	LD	b1, 1 * SIZE(Y)

	ADD	s1, t1, s1
	SXADDQ	INCX, X, X
	MUL	a2, b3, t1
	SXADDQ	INCY, Y, Y

	ADD	s2, t2, s2
	unop
	MUL	a3, b2, t2
	unop

	ADD	s3, t3, s3
	LD	a2, 0 * SIZE(X)
	MUL	a3, b3, t3
	LD	a3, 1 * SIZE(X)

	ADD	s0, t0, s0
	LD	b2, 0 * SIZE(Y)
	MUL	a4, b4, t0
	LD	b3, 1 * SIZE(Y)

	ADD	s1, t1, s1
	SXADDQ	INCX, X, X
	MUL	a4, b5, t1
	SXADDQ	INCY, Y, Y

	ADD	s2, t2, s2
	unop
	MUL	a5, b4, t2
	unop

	ADD	s3, t3, s3
	LD	a4, 0 * SIZE(X)
	MUL	a5, b5, t3
	LD	a5, 1 * SIZE(X)

	ADD	s0, t0, s0
	LD	b4, 0 * SIZE(Y)
	MUL	a6, b6, t0
	LD	b5, 1 * SIZE(Y)

	ADD	s1, t1, s1
	SXADDQ	INCX, X, X
	MUL	a6, b7, t1
	SXADDQ	INCY, Y, Y

	ADD	s2, t2, s2
	unop
	MUL	a7, b6, t2
	unop

	ADD	s3, t3, s3
	LD	a6, 0 * SIZE(X)
	MUL	a7, b7, t3
	LD	a7, 1 * SIZE(X)

	ADD	s0, t0, s0
	LD	b6, 0 * SIZE(Y)
	MUL	a0, b0, t0
	LD	b7, 1 * SIZE(Y)

	ADD	s1, t1, s1
	SXADDQ	INCX, X, X
	MUL	a0, b1, t1
	SXADDQ	INCY, Y, Y

	ADD	s2, t2, s2
	MUL	a1, b0, t2
	ADD	s3, t3, s3
	MUL	a1, b1, t3

	ADD	s0, t0, s0
	MUL	a2, b2, t0
	ADD	s1, t1, s1
	MUL	a2, b3, t1

	ADD	s2, t2, s2
	MUL	a3, b2, t2
	ADD	s3, t3, s3
	MUL	a3, b3, t3

	ADD	s0, t0, s0
	MUL	a4, b4, t0
	ADD	s1, t1, s1
	MUL	a4, b5, t1

	ADD	s2, t2, s2
	MUL	a5, b4, t2
	ADD	s3, t3, s3
	MUL	a5, b5, t3

	ADD	s0, t0, s0
	MUL	a6, b6, t0
	ADD	s1, t1, s1
	MUL	a6, b7, t1

	ADD	s2, t2, s2
	MUL	a7, b6, t2
	ADD	s3, t3, s3
	MUL	a7, b7, t3
	.align 4

$L25:
	and	N, 7, I
	unop
	unop
	ble	I, $L998

	LD	a0, 0 * SIZE(X)
	LD	a1, 1 * SIZE(X)
	LD	b0, 0 * SIZE(Y)
	LD	b1, 1 * SIZE(Y)

	SXADDQ	INCX, X, X
	subq	I, 1, I
	SXADDQ	INCY, Y, Y
	ble	I, $L28
	.align 4

$L26:
	ADD	s0, t0, s0
	mov	X, XX
	MUL	a0, b0, t0
	mov	Y, YY

	ADD	s1, t1, s1
	SXADDQ	INCX, X, X
	MUL	a0, b1, t1
	SXADDQ	INCY, Y, Y

	ADD	s2, t2, s2
	LD	a0, 0 * SIZE(XX)
	MUL	a1, b0, t2
	LD	b0, 0 * SIZE(YY)

	ADD	s3, t3, s3
	subq	I, 1, I
	MUL	a1, b1, t3
	LD	a1, 1 * SIZE(XX)

	LD	b1, 1 * SIZE(YY)
	bgt	I, $L26
	.align 4

$L28:
	ADD	s0, t0, s0
	MUL	a0, b0, t0
	ADD	s1, t1, s1
	MUL	a0, b1, t1

	ADD	s2, t2, s2
	MUL	a1, b0, t2
	ADD	s3, t3, s3
	MUL	a1, b1, t3
	.align 4

$L998:
	ADD	s0, t0, s0
	ADD	s1, t1, s1
	ADD	s2, t2, s2
	ADD	s3, t3, s3

#ifndef CONJ
	SUB	s0, s3, s0
	ADD	s1, s2, s1
#else
	ADD	s0, s3, s0
	SUB	s1, s2, s1
#endif
	.align 4

$L999:
	ldt	$f2,  0($sp)
	lda	$sp, 16($sp)
	ret

	EPILOGUE
