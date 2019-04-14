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

#define X	$9
#define INCX	$10
#define Y	$11
#define INCY	$8

#define I	$2
#define TEMP	$3

#define YY	$5

#define ALPHA_R	$f15
#define ALPHA_I	$f16

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

#ifndef CONJ
#define MADD1	NMSUB
#define MADD2	MADD
#else
#define MADD1	MADD
#define MADD2	NMSUB
#endif

	PROLOGUE

	LDARG	INCY, 0($sp)
	li	TEMP, 2 * SIZE

#ifndef __64BIT__
	daddiu	$sp, $sp, -16
	sdc1	$f20, 0($sp)
	sdc1	$f21, 8($sp)
#endif

	blez	N, .L999
	dsll	INCX, INCX, ZBASE_SHIFT

	bne	INCX, TEMP, .L20
	dsll	INCY, INCY, ZBASE_SHIFT

	bne	INCY, TEMP, .L20
	dsra	I, N, 2

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
	MADD	t1, b1, ALPHA_R, a1
	LD	b1,  8 * SIZE(Y)
	MADD	t2, b2, ALPHA_I, a1
	LD	a1,  8 * SIZE(X)
	MADD	t3, b3, ALPHA_R, a3
	LD	b3, 10 * SIZE(Y)
	MADD	t4, b4, ALPHA_I, a3
	LD	a3, 10 * SIZE(X)

	MADD1	t1, t1, ALPHA_I, a2
	LD	b2,  9 * SIZE(Y)
	MADD2	t2, t2, ALPHA_R, a2
	LD	a2,  9 * SIZE(X)
	MADD1	t3, t3, ALPHA_I, a4
	LD	b4, 11 * SIZE(Y)
	MADD2	t4, t4, ALPHA_R, a4
	LD	a4, 11 * SIZE(X)

	ST	t1,  0 * SIZE(Y)
	ST	t2,  1 * SIZE(Y)
	ST	t3,  2 * SIZE(Y)
	ST	t4,  3 * SIZE(Y)

	MADD	t1, b5, ALPHA_R, a5
	LD	b5, 12 * SIZE(Y)
	MADD	t2, b6, ALPHA_I, a5
	LD	a5, 12 * SIZE(X)
	MADD	t3, b7, ALPHA_R, a7
	LD	b7, 14 * SIZE(Y)
	MADD	t4, b8, ALPHA_I, a7
	LD	a7, 14 * SIZE(X)

	MADD1	t1, t1, ALPHA_I, a6
	LD	b6, 13 * SIZE(Y)
	MADD2	t2, t2, ALPHA_R, a6
	LD	a6, 13 * SIZE(X)
	MADD1	t3, t3, ALPHA_I, a8
	LD	b8, 15 * SIZE(Y)
	MADD2	t4, t4, ALPHA_R, a8
	LD	a8, 15 * SIZE(X)

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
	MADD	t1, b1, ALPHA_R, a1
	MADD	t2, b2, ALPHA_I, a1
	MADD	t3, b3, ALPHA_R, a3
	MADD	t4, b4, ALPHA_I, a3

	MADD1	t1, t1, ALPHA_I, a2
	MADD2	t2, t2, ALPHA_R, a2
	MADD1	t3, t3, ALPHA_I, a4
	MADD2	t4, t4, ALPHA_R, a4

	ST	t1,  0 * SIZE(Y)
	MADD	t1, b5, ALPHA_R, a5
	ST	t2,  1 * SIZE(Y)
	MADD	t2, b6, ALPHA_I, a5
	ST	t3,  2 * SIZE(Y)
	MADD	t3, b7, ALPHA_R, a7
	ST	t4,  3 * SIZE(Y)
	MADD	t4, b8, ALPHA_I, a7

	MADD1	t1, t1, ALPHA_I, a6
	MADD2	t2, t2, ALPHA_R, a6
	MADD1	t3, t3, ALPHA_I, a8
	MADD2	t4, t4, ALPHA_R, a8

	ST	t1,  4 * SIZE(Y)
	ST	t2,  5 * SIZE(Y)
	ST	t3,  6 * SIZE(Y)
	ST	t4,  7 * SIZE(Y)

	daddiu	X, X, 8 * SIZE
	daddiu	Y, Y, 8 * SIZE
	.align 3

.L15:
	andi	I,  N, 3

	blez	I, .L999
	NOP
	.align	3

.L16:
 	LD	a1,  0 * SIZE(X)
 	LD	a2,  1 * SIZE(X)
	LD	b1,  0 * SIZE(Y)
	LD	b2,  1 * SIZE(Y)

	MADD	t1, b1, ALPHA_R, a1
	daddiu	X, X, 2 * SIZE
	MADD	t2, b2, ALPHA_I, a1

	MADD1	t1, t1, ALPHA_I, a2
	daddiu	I, I, -1
	MADD2	t2, t2, ALPHA_R, a2
	daddiu	Y, Y, 2 * SIZE

	ST	t1, -2 * SIZE(Y)

	bgtz	I, .L16
	ST	t2, -1 * SIZE(Y)

#ifndef __64BIT__
	ldc1	$f20, 0($sp)
	ldc1	$f21, 8($sp)
	daddiu	$sp, $sp,  16
#endif

	j	$31
	NOP
	.align 3

.L20:
	dsra	I, N, 2
	move	YY, Y

	blez	I, .L25
	daddiu	I, I, -1

	LD	a1,  0 * SIZE(X)
	LD	b1,  0 * SIZE(Y)
	LD	a2,  1 * SIZE(X)
	LD	b2,  1 * SIZE(Y)
	daddu	X, X, INCX
	daddu	Y, Y, INCY

	LD	a3,  0 * SIZE(X)
	LD	b3,  0 * SIZE(Y)
	LD	a4,  1 * SIZE(X)
	LD	b4,  1 * SIZE(Y)
	daddu	X, X, INCX
	daddu	Y, Y, INCY

	LD	a5,  0 * SIZE(X)
	LD	b5,  0 * SIZE(Y)
	LD	a6,  1 * SIZE(X)
	LD	b6,  1 * SIZE(Y)
	daddu	X, X, INCX
	daddu	Y, Y, INCY

	LD	a7,  0 * SIZE(X)
	blez	I, .L23
	LD	b7,  0 * SIZE(Y)
	.align 3

.L22:
	MADD	t1, b1, ALPHA_R, a1
	LD	b8,  1 * SIZE(Y)
	daddu	Y, Y, INCY
	MADD	t2, b2, ALPHA_I, a1
	LD	a8,  1 * SIZE(X)
	daddu	X, X, INCX

	MADD	t3, b3, ALPHA_R, a3
	LD	b1,  0 * SIZE(Y)
	MADD	t4, b4, ALPHA_I, a3
	LD	a1,  0 * SIZE(X)

	MADD1	t1, t1, ALPHA_I, a2
	LD	b2,  1 * SIZE(Y)
	daddu	Y, Y, INCY
	MADD2	t2, t2, ALPHA_R, a2
	LD	a2,  1 * SIZE(X)
	daddu	X, X, INCX

	MADD1	t3, t3, ALPHA_I, a4
	LD	a3,  0 * SIZE(X)
	MADD2	t4, t4, ALPHA_R, a4
	LD	b3,  0 * SIZE(Y)

	ST	t1,  0 * SIZE(YY)
	ST	t2,  1 * SIZE(YY)
	daddu	YY, YY, INCY
	ST	t3,  0 * SIZE(YY)
	ST	t4,  1 * SIZE(YY)
	daddu	YY, YY, INCY

	MADD	t1, b5, ALPHA_R, a5
	LD	a4,  1 * SIZE(X)
	daddu	X, X, INCX
	MADD	t2, b6, ALPHA_I, a5
	LD	b4,  1 * SIZE(Y)
	daddu	Y, Y, INCY

	MADD	t3, b7, ALPHA_R, a7
	LD	b5,  0 * SIZE(Y)
	MADD	t4, b8, ALPHA_I, a7
	LD	a5,  0 * SIZE(X)

	MADD1	t1, t1, ALPHA_I, a6
	LD	b6,  1 * SIZE(Y)
	daddu	Y, Y, INCY
	MADD2	t2, t2, ALPHA_R, a6
	LD	a6,  1 * SIZE(X)
	daddu	X, X, INCX

	MADD1	t3, t3, ALPHA_I, a8
	LD	b7,  0 * SIZE(Y)
	MADD2	t4, t4, ALPHA_R, a8
	LD	a7,  0 * SIZE(X)

	ST	t1,  0 * SIZE(YY)
	ST	t2,  1 * SIZE(YY)
	daddu	YY, YY, INCY
	ST	t3,  0 * SIZE(YY)
	ST	t4,  1 * SIZE(YY)
	daddu	YY, YY, INCY


	daddiu	I, I, -1

	bgtz	I, .L22
	NOP
	.align 3

.L23:
	MADD	t1, b1, ALPHA_R, a1
	LD	a8,  1 * SIZE(X)
	MADD	t2, b2, ALPHA_I, a1
	LD	b8,  1 * SIZE(Y)
	MADD	t3, b3, ALPHA_R, a3
	daddu	X, X, INCX
	MADD	t4, b4, ALPHA_I, a3
	daddu	Y, Y, INCY

	MADD1	t1, t1, ALPHA_I, a2
	MADD2	t2, t2, ALPHA_R, a2
	MADD1	t3, t3, ALPHA_I, a4
	MADD2	t4, t4, ALPHA_R, a4

	ST	t1,  0 * SIZE(YY)
	MADD	t1, b5, ALPHA_R, a5
	ST	t2,  1 * SIZE(YY)
	MADD	t2, b6, ALPHA_I, a5
	daddu	YY, YY, INCY

	ST	t3,  0 * SIZE(YY)
	MADD	t3, b7, ALPHA_R, a7
	ST	t4,  1 * SIZE(YY)
	MADD	t4, b8, ALPHA_I, a7
	daddu	YY, YY, INCY

	MADD1	t1, t1, ALPHA_I, a6
	MADD2	t2, t2, ALPHA_R, a6
	MADD1	t3, t3, ALPHA_I, a8
	MADD2	t4, t4, ALPHA_R, a8

	ST	t1,  0 * SIZE(YY)
	ST	t2,  1 * SIZE(YY)
	daddu	YY, YY, INCY
	ST	t3,  0 * SIZE(YY)
	ST	t4,  1 * SIZE(YY)
	daddu	YY, YY, INCY
	.align 3

.L25:
	andi	I,  N, 3

	blez	I, .L999
	NOP
	.align	3

.L26:
 	LD	a1,  0 * SIZE(X)
 	LD	a2,  1 * SIZE(X)
	LD	b1,  0 * SIZE(Y)
	LD	b2,  1 * SIZE(Y)

	MADD	t1, b1, ALPHA_R, a1
	MADD	t2, b2, ALPHA_I, a1
	daddu	X, X, INCX

	MADD1	t1, t1, ALPHA_I, a2
	MADD2	t2, t2, ALPHA_R, a2
	daddiu	I, I, -1

	ST	t1,  0 * SIZE(Y)
	ST	t2,  1 * SIZE(Y)

	bgtz	I, .L26
	daddu	Y, Y, INCY
	.align 3

.L999:
#ifndef __64BIT__
	ldc1	$f20, 0($sp)
	ldc1	$f21, 8($sp)
	daddiu	$sp, $sp,  16
#endif

	j	$31
	NOP

	EPILOGUE
