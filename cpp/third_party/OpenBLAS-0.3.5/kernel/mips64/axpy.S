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

#define N	$4

#define X	$8
#define INCX	$9

#define Y	$10
#define INCY	$11

#define I	$2
#define TEMP	$3

#define YY	$5

#define ALPHA	$f15

#define a1	$f0
#define a2	$f1
#define a3	$f2
#define a4	$f3
#define a5	$f4
#define a6	$f5
#define a7	$f6
#define a8	$f7

#define b1	$f8
#define b2	$f9
#define b3	$f10
#define b4	$f11
#define b5	$f12
#define b6	$f13
#define b7	$f14
#define b8	$f17

#define t1	$f18
#define t2	$f19
#define t3	$f20
#define t4	$f21

	PROLOGUE

#ifndef __64BIT__
	daddiu	$sp, $sp, -16
	sdc1	$f20, 0($sp)
	sdc1	$f21, 8($sp)
#endif

	li	TEMP, SIZE

	blez	N, .L999
	dsll	INCX, INCX, BASE_SHIFT

	bne	INCX, TEMP, .L20
	dsll	INCY, INCY, BASE_SHIFT

	bne	INCY, TEMP, .L20
	dsra	I, N, 3

	blez	I, .L15
	daddiu	I, I, -1

	LD	a1,  0 * SIZE(X)
	LD	b1,  0 * SIZE(Y)
	LD	a2,  1 * SIZE(X)
	LD	b2,  1 * SIZE(Y)
	LD	a3,  2 * SIZE(X)
	LD	b3,  2 * SIZE(Y)
	LD	a4,  3 * SIZE(X)
	LD	b4,  3 * SIZE(Y)
	LD	a5,  4 * SIZE(X)
	LD	b5,  4 * SIZE(Y)
	LD	a6,  5 * SIZE(X)
	LD	b6,  5 * SIZE(Y)
	LD	a7,  6 * SIZE(X)
	LD	b7,  6 * SIZE(Y)
	LD	a8,  7 * SIZE(X)
	LD	b8,  7 * SIZE(Y)

	blez	I, .L13
	NOP
	.align 3

.L12:
	MADD	t1, b1, ALPHA, a1
	LD	a1,  8 * SIZE(X)
	LD	b1,  8 * SIZE(Y)

	MADD	t2, b2, ALPHA, a2
	LD	a2,  9 * SIZE(X)
	LD	b2,  9 * SIZE(Y)

	MADD	t3, b3, ALPHA, a3
	LD	a3, 10 * SIZE(X)
	LD	b3, 10 * SIZE(Y)

	MADD	t4, b4, ALPHA, a4
	LD	a4, 11 * SIZE(X)
	LD	b4, 11 * SIZE(Y)

	ST	t1,  0 * SIZE(Y)
	ST	t2,  1 * SIZE(Y)
	ST	t3,  2 * SIZE(Y)
	ST	t4,  3 * SIZE(Y)

	MADD	t1, b5, ALPHA, a5
	LD	a5, 12 * SIZE(X)
	LD	b5, 12 * SIZE(Y)

	MADD	t2, b6, ALPHA, a6
	LD	a6, 13 * SIZE(X)
	LD	b6, 13 * SIZE(Y)

	MADD	t3, b7, ALPHA, a7
	LD	a7, 14 * SIZE(X)
	LD	b7, 14 * SIZE(Y)

	MADD	t4, b8, ALPHA, a8
	LD	a8, 15 * SIZE(X)
	LD	b8, 15 * SIZE(Y)

	ST	t1,  4 * SIZE(Y)
	ST	t2,  5 * SIZE(Y)
	ST	t3,  6 * SIZE(Y)
	ST	t4,  7 * SIZE(Y)

	daddiu	I, I, -1
	daddiu	Y, Y, 8 * SIZE

	bgtz	I, .L12
	daddiu	X, X, 8 * SIZE
	.align 3

.L13:
	MADD	t1, b1, ALPHA, a1
	MADD	t2, b2, ALPHA, a2
	MADD	t3, b3, ALPHA, a3
	MADD	t4, b4, ALPHA, a4

	ST	t1,  0 * SIZE(Y)
	MADD	t1, b5, ALPHA, a5
	ST	t2,  1 * SIZE(Y)
	MADD	t2, b6, ALPHA, a6
	ST	t3,  2 * SIZE(Y)
	MADD	t3, b7, ALPHA, a7
	ST	t4,  3 * SIZE(Y)
	MADD	t4, b8, ALPHA, a8

	ST	t1,  4 * SIZE(Y)
	ST	t2,  5 * SIZE(Y)
	ST	t3,  6 * SIZE(Y)
	ST	t4,  7 * SIZE(Y)

	daddiu	X, X, 8 * SIZE
	daddiu	Y, Y, 8 * SIZE
	.align 3

.L15:
	andi	I,  N, 7

	blez	I, .L999
	NOP
	.align	3

.L16:
	LD	a1,  0 * SIZE(X)
	LD	b1,  0 * SIZE(Y)

	daddiu	X, X, SIZE
	daddiu	Y, Y, SIZE

	MADD	t1, b1, ALPHA, a1
	daddiu	I, I, -1

	bgtz	I, .L16
	ST	t1, -1 * SIZE(Y)

#ifndef __64BIT__
	ldc1	$f20, 0($sp)
	ldc1	$f21, 8($sp)
	daddiu	$sp, $sp, 16
#endif

	j	$31
	NOP
	.align 3

.L20:
	beqz	INCY, .L27
	dsra	I, N, 3

	move	YY, Y

	blez	I, .L25
	daddiu	I, I, -1

	LD	a1,  0 * SIZE(X)
	daddu	X, X, INCX
	LD	b1,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	LD	a2,  0 * SIZE(X)
	daddu	X, X, INCX
	LD	b2,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	LD	a3,  0 * SIZE(X)
	daddu	X, X, INCX
	LD	b3,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	LD	a4,  0 * SIZE(X)
	daddu	X, X, INCX
	LD	b4,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	LD	a5,  0 * SIZE(X)
	daddu	X, X, INCX
	LD	b5,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	LD	a6,  0 * SIZE(X)
	daddu	X, X, INCX
	LD	b6,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	LD	a7,  0 * SIZE(X)
	daddu	X, X, INCX
	LD	b7,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	LD	a8,  0 * SIZE(X)
	daddu	X, X, INCX
	LD	b8,  0 * SIZE(Y)
	daddu	Y, Y, INCY

	blez	I, .L23
	NOP
	.align 3

.L22:
	MADD	t1, b1, ALPHA, a1
	LD	a1,  0 * SIZE(X)
	LD	b1,  0 * SIZE(Y)
	daddu	X, X, INCX
	daddu	Y, Y, INCY

	MADD	t2, b2, ALPHA, a2
	LD	a2,  0 * SIZE(X)
	LD	b2,  0 * SIZE(Y)
	daddu	X, X, INCX
	daddu	Y, Y, INCY

	MADD	t3, b3, ALPHA, a3
	LD	a3,  0 * SIZE(X)
	LD	b3,  0 * SIZE(Y)
	daddu	X, X, INCX
	daddu	Y, Y, INCY

	MADD	t4, b4, ALPHA, a4
	LD	a4,  0 * SIZE(X)
	LD	b4,  0 * SIZE(Y)
	daddu	X, X, INCX
	daddu	Y, Y, INCY

	ST	t1,  0 * SIZE(YY)
	daddu	YY, YY, INCY
	MADD	t1, b5, ALPHA, a5

	LD	a5,  0 * SIZE(X)
	LD	b5,  0 * SIZE(Y)
	daddu	X, X, INCX
	daddu	Y, Y, INCY

	ST	t2,  0 * SIZE(YY)
	daddu	YY, YY, INCY
	MADD	t2, b6, ALPHA, a6

	LD	a6,  0 * SIZE(X)
	LD	b6,  0 * SIZE(Y)
	daddu	X, X, INCX
	daddu	Y, Y, INCY

	ST	t3,  0 * SIZE(YY)
	daddu	YY, YY, INCY
	MADD	t3, b7, ALPHA, a7

	LD	a7,  0 * SIZE(X)
	LD	b7,  0 * SIZE(Y)
	daddu	X, X, INCX
	daddu	Y, Y, INCY

	ST	t4,  0 * SIZE(YY)
	daddu	YY, YY, INCY
	MADD	t4, b8, ALPHA, a8

	LD	a8,  0 * SIZE(X)
	daddu	X, X, INCX

	LD	b8,  0 * SIZE(Y)
	daddu	Y, Y, INCY

	ST	t1,  0 * SIZE(YY)
	daddu	YY, YY, INCY
	ST	t2,  0 * SIZE(YY)
	daddu	YY, YY, INCY
	ST	t3,  0 * SIZE(YY)
	daddu	YY, YY, INCY
	ST	t4,  0 * SIZE(YY)
	daddiu	I, I, -1

	bgtz	I, .L22
	daddu	YY, YY, INCY
	.align 3

.L23:
	MADD	t1, b1, ALPHA, a1
	MADD	t2, b2, ALPHA, a2
	MADD	t3, b3, ALPHA, a3
	MADD	t4, b4, ALPHA, a4

	ST	t1,  0 * SIZE(YY)
	daddu	YY, YY, INCY
	MADD	t1, b5, ALPHA, a5

	ST	t2,  0 * SIZE(YY)
	daddu	YY, YY, INCY
	MADD	t2, b6, ALPHA, a6

	ST	t3,  0 * SIZE(YY)
	daddu	YY, YY, INCY
	MADD	t3, b7, ALPHA, a7

	ST	t4,  0 * SIZE(YY)
	daddu	YY, YY, INCY
	MADD	t4, b8, ALPHA, a8

	ST	t1,  0 * SIZE(YY)
	daddu	YY, YY, INCY
	ST	t2,  0 * SIZE(YY)
	daddu	YY, YY, INCY
	ST	t3,  0 * SIZE(YY)
	daddu	YY, YY, INCY
	ST	t4,  0 * SIZE(YY)
	daddu	YY, YY, INCY
	.align 3

.L25:
	andi	I,  N, 7

	blez	I, .L999
	NOP
	.align	3

.L26:
	LD	a1,  0 * SIZE(X)
	LD	b1,  0 * SIZE(Y)

	MADD	t1, b1, ALPHA, a1
	daddu	X, X, INCX

	ST	t1,  0 * SIZE(Y)
	daddiu	I, I, -1

	bgtz	I, .L26
	daddu	Y, Y, INCY
	.align 3

.L999:
#ifndef __64BIT__
	ldc1	$f20, 0($sp)
	ldc1	$f21, 8($sp)
	daddiu	$sp, $sp, 16
#endif

	j	$31
	NOP
	.align 3

.L27:
	LD	b1,  0 * SIZE(Y)

.L28:
	daddiu	N, N, -1
	LD	a1,  0 * SIZE(X)
	daddu	X, X, INCX
	bgtz	N, .L28
	MADD	b1, b1, ALPHA, a1

	j	.L999
	ST	b1,  0 * SIZE(Y)

	EPILOGUE
