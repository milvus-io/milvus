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
#define X	$20
#define INCX	$21

#define XX	$18
#define I	$19

#define ALPHA	$f19

#define s0	$f0
#define s1	$f1
#define s2	$f10
#define s3	$f11

#define a0	$f12
#define a1	$f13
#define a2	$f14
#define a3	$f15
#define a4	$f16
#define a5	$f17
#define a6	$f18
#define a7	$f21

#define t0	$f22
#define t1	$f23
#define t2	$f24
#define t3	$f25

	PROLOGUE
	PROFCODE

	mov	X, XX
	ble	N,  $L999

	cmpeq	INCX, 1, $0
	beq	$0, $L20

#ifndef DOUBLE
	sra	N, 4, I
	ble	I, $L15

	LD	a0,  0 * SIZE(X)
	LD	a1,  1 * SIZE(X)
	LD	a2,  2 * SIZE(X)
	LD	a3,  3 * SIZE(X)

	LD	a4,  4 * SIZE(X)
	MUL	a0, ALPHA, t0
	LD	a5,  5 * SIZE(X)
	MUL	a1, ALPHA, t1
	LD	a6,  6 * SIZE(X)
	MUL	a2, ALPHA, t2
	LD	a7,  7 * SIZE(X)
	MUL	a3, ALPHA, t3

	ST	t0,  0 * SIZE(X)
	MUL	a4, ALPHA, t0
	ST	t1,  1 * SIZE(X)
	MUL	a5, ALPHA, t1

	ST	t2,  2 * SIZE(X)
	MUL	a6, ALPHA, t2
	ST	t3,  3 * SIZE(X)
	MUL	a7, ALPHA, t3

	LD	a0,  8 * SIZE(X)
	LD	a1,  9 * SIZE(X)
	LD	a2, 10 * SIZE(X)
	LD	a3, 11 * SIZE(X)

	ST	t0,  4 * SIZE(X)
	MUL	a0, ALPHA, t0
	ST	t1,  5 * SIZE(X)
	MUL	a1, ALPHA, t1

	ST	t2,  6 * SIZE(X)
	MUL	a2, ALPHA, t2
	ST	t3,  7 * SIZE(X)
	MUL	a3, ALPHA, t3

	LD	a4, 12 * SIZE(X)
	LD	a5, 13 * SIZE(X)
	LD	a6, 14 * SIZE(X)
	LD	a7, 15 * SIZE(X)

	lda	I,  -1(I)
 	ble	I, $L13
	.align 4

$L12:
	ST	t0,  8 * SIZE(X)
	MUL	a4, ALPHA, t0
	ST	t1,  9 * SIZE(X)
	MUL	a5, ALPHA, t1

	ST	t2, 10 * SIZE(X)
	MUL	a6, ALPHA, t2
	ST	t3, 11 * SIZE(X)
	MUL	a7, ALPHA, t3

	LD	a0, 16 * SIZE(X)
	LD	a1, 17 * SIZE(X)
	LD	a2, 18 * SIZE(X)
	LD	a3, 19 * SIZE(X)

	ST	t0, 12 * SIZE(X)
	MUL	a0, ALPHA, t0
	ST	t1, 13 * SIZE(X)
	MUL	a1, ALPHA, t1

	ST	t2, 14 * SIZE(X)
	MUL	a2, ALPHA, t2
	ST	t3, 15 * SIZE(X)
	MUL	a3, ALPHA, t3

	LD	a4, 20 * SIZE(X)
	LD	a5, 21 * SIZE(X)
	LD	a6, 22 * SIZE(X)
	LD	a7, 23 * SIZE(X)

	ST	t0, 16 * SIZE(X)
	MUL	a4, ALPHA, t0
	ST	t1, 17 * SIZE(X)
	MUL	a5, ALPHA, t1

	ST	t2, 18 * SIZE(X)
	MUL	a6, ALPHA, t2
	ST	t3, 19 * SIZE(X)
	MUL	a7, ALPHA, t3

	LD	a0, 24 * SIZE(X)
	LD	a1, 25 * SIZE(X)
	LD	a2, 26 * SIZE(X)
	LD	a3, 27 * SIZE(X)

	ST	t0, 20 * SIZE(X)
	MUL	a0, ALPHA, t0
	ST	t1, 21 * SIZE(X)
	MUL	a1, ALPHA, t1

	ST	t2, 22 * SIZE(X)
	MUL	a2, ALPHA, t2
	ST	t3, 23 * SIZE(X)
	MUL	a3, ALPHA, t3

	LD	a4, 28 * SIZE(X)
	LD	a5, 29 * SIZE(X)
	LD	a6, 30 * SIZE(X)
	LD	a7, 31 * SIZE(X)

	lds	$f31, PREFETCHSIZE * SIZE(X)
	lda	I,  -1(I)
	addq	X, 16 * SIZE, X
	bne	I, $L12
	.align 4

$L13:
	ST	t0,  8 * SIZE(X)
	MUL	a4, ALPHA, t0
	ST	t1,  9 * SIZE(X)
	MUL	a5, ALPHA, t1

	ST	t2, 10 * SIZE(X)
	MUL	a6, ALPHA, t2
	ST	t3, 11 * SIZE(X)
	MUL	a7, ALPHA, t3

	ST	t0, 12 * SIZE(X)
	ST	t1, 13 * SIZE(X)
	ST	t2, 14 * SIZE(X)
	ST	t3, 15 * SIZE(X)
	addq	X, 16 * SIZE, X
	.align 4

$L15:
	and	N, 15, I

#else

	sra	N, 3, I
	ble	I, $L15

	LD	a0,  0 * SIZE(X)
	LD	a1,  1 * SIZE(X)
	LD	a2,  2 * SIZE(X)
	LD	a3,  3 * SIZE(X)

	LD	a4,  4 * SIZE(X)
	MUL	a0, ALPHA, t0
	LD	a5,  5 * SIZE(X)
	MUL	a1, ALPHA, t1

	LD	a6,  6 * SIZE(X)
	MUL	a2, ALPHA, t2
	LD	a7,  7 * SIZE(X)
	MUL	a3, ALPHA, t3

	lda	I,  -1(I)
	ble	I, $L13
	.align 4

$L12:
	ST	t0,  0 * SIZE(X)
	MUL	a4, ALPHA, t0
	ST	t1,  1 * SIZE(X)
	MUL	a5, ALPHA, t1

	ST	t2,  2 * SIZE(X)
	MUL	a6, ALPHA, t2
	ST	t3,  3 * SIZE(X)
	MUL	a7, ALPHA, t3

	LD	a0,  8 * SIZE(X)
	lda	I,  -1(I)
	LD	a1,  9 * SIZE(X)
	addq	X, 8 * SIZE, X

	LD	a2,  2 * SIZE(X)
	LD	a3,  3 * SIZE(X)

	ST	t0, -4 * SIZE(X)
	MUL	a0, ALPHA, t0
	ST	t1, -3 * SIZE(X)
	MUL	a1, ALPHA, t1

	ST	t2, -2 * SIZE(X)
	MUL	a2, ALPHA, t2
	ST	t3, -1 * SIZE(X)
	MUL	a3, ALPHA, t3

	LD	a4,  4 * SIZE(X)
	LD	a5,  5 * SIZE(X)

	LD	a6,  6 * SIZE(X)
	LD	a7,  7 * SIZE(X)
	lds	$f31, PREFETCHSIZE * SIZE(X)
	bne	I, $L12
	.align 4

$L13:
	ST	t0,  0 * SIZE(X)
	MUL	a4, ALPHA, t0
	ST	t1,  1 * SIZE(X)
	MUL	a5, ALPHA, t1

	ST	t2,  2 * SIZE(X)
	MUL	a6, ALPHA, t2
	ST	t3,  3 * SIZE(X)
	MUL	a7, ALPHA, t3

	ST	t0,  4 * SIZE(X)
	ST	t1,  5 * SIZE(X)
	ST	t2,  6 * SIZE(X)
	ST	t3,  7 * SIZE(X)
	addq	X, 8 * SIZE, X
	.align 4

$L15:
	and	N, 7, I

#endif

	unop
	unop
	ble	I, $L999
	.align 4

$L17:
	LD	a0,  0 * SIZE(X)

	MUL	a0, ALPHA, t0

	ST	t0,  0 * SIZE(X)

	addq	X, SIZE, X

	lda	I,  -1(I)
	bne	I, $L17
	ret
	.align 4

$L20:
	sra	N, 3, I
	ble	I, $L25

	LD	a0,  0 * SIZE(X)
	SXADDQ	INCX, X, X
	LD	a1,  0 * SIZE(X)
	SXADDQ	INCX, X, X
	LD	a2,  0 * SIZE(X)
	SXADDQ	INCX, X, X
	LD	a3,  0 * SIZE(X)
	SXADDQ	INCX, X, X

	LD	a4,  0 * SIZE(X)
	MUL	a0, ALPHA, t0
	lda	I,  -1(I)
	SXADDQ	INCX, X, X

	LD	a5,  0 * SIZE(X)
	MUL	a1, ALPHA, t1
	SXADDQ	INCX, X, X
	unop

	LD	a6,  0 * SIZE(X)
	MUL	a2, ALPHA, t2
	SXADDQ	INCX, X, X
	unop

	LD	a7,  0 * SIZE(X)
	MUL	a3, ALPHA, t3
	SXADDQ	INCX, X, X
	ble	I, $L23
	.align 4

$L22:
	ST	t0,  0 * SIZE(XX)
	MUL	a4, ALPHA, t0
	lds	$f31, PREFETCHSIZE * SIZE(X)
	SXADDQ	INCX, XX, XX

	LD	a0,  0 * SIZE(X)
	SXADDQ	INCX, X, X
	lda	I,  -1(I)
	unop

	ST	t1,  0 * SIZE(XX)
	MUL	a5, ALPHA, t1
	SXADDQ	INCX, XX, XX
	unop

	LD	a1,  0 * SIZE(X)
	SXADDQ	INCX, X, X

	ST	t2,  0 * SIZE(XX)
	MUL	a6, ALPHA, t2
	SXADDQ	INCX, XX, XX
	unop

	LD	a2,  0 * SIZE(X)
	SXADDQ	INCX, X, X

	ST	t3,  0 * SIZE(XX)
	MUL	a7, ALPHA, t3
	SXADDQ	INCX, XX, XX
	unop

	LD	a3,  0 * SIZE(X)
	SXADDQ	INCX, X, X

	ST	t0,  0 * SIZE(XX)
	MUL	a0, ALPHA, t0
	SXADDQ	INCX, XX, XX
	unop

	LD	a4,  0 * SIZE(X)
	SXADDQ	INCX, X, X

	ST	t1,  0 * SIZE(XX)
	MUL	a1, ALPHA, t1
	SXADDQ	INCX, XX, XX
	unop

	LD	a5,  0 * SIZE(X)
	SXADDQ	INCX, X, X

	ST	t2,  0 * SIZE(XX)
	MUL	a2, ALPHA, t2
	SXADDQ	INCX, XX, XX
	unop

	LD	a6,  0 * SIZE(X)
	SXADDQ	INCX, X, X

	ST	t3,  0 * SIZE(XX)
	MUL	a3, ALPHA, t3
	SXADDQ	INCX, XX, XX
	unop

	LD	a7,  0 * SIZE(X)
	SXADDQ	INCX, X, X
	unop
	bne	I, $L22
	.align 4

$L23:
	ST	t0,  0 * SIZE(XX)
	MUL	a4, ALPHA, t0
	SXADDQ	INCX, XX, XX

	ST	t1,  0 * SIZE(XX)
	MUL	a5, ALPHA, t1
	SXADDQ	INCX, XX, XX

	ST	t2,  0 * SIZE(XX)
	MUL	a6, ALPHA, t2
	SXADDQ	INCX, XX, XX

	ST	t3,  0 * SIZE(XX)
	MUL	a7, ALPHA, t3
	SXADDQ	INCX, XX, XX

	ST	t0,  0 * SIZE(XX)
	SXADDQ	INCX, XX, XX
	ST	t1,  0 * SIZE(XX)
	SXADDQ	INCX, XX, XX
	ST	t2,  0 * SIZE(XX)
	SXADDQ	INCX, XX, XX
	ST	t3,  0 * SIZE(XX)
	SXADDQ	INCX, XX, XX
	.align 4

$L25:
	and	N, 7, I
	unop
	unop
	ble	I, $L999
	.align 4

$L27:
	LD	a0,  0 * SIZE(X)

	MUL	a0, ALPHA, t0

	ST	t0,  0 * SIZE(XX)

	SXADDQ	INCX, X, X
	SXADDQ	INCX, XX, XX

	lda	I,  -1(I)
	bne	I, $L27
	.align 4

$L999:
	ret
	EPILOGUE
