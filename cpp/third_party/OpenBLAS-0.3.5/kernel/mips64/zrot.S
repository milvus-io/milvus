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
#define	X	$5
#define INCX	$6
#define Y	$7
#define INCY	$8

#define XX	$9
#define YY	$10

#define C	$f17
#define S	$f18

#define I	$2
#define TEMP	$3

#define a1	$f4
#define a2	$f5
#define a3	$f6
#define a4	$f7

#define b1	$f8
#define b2	$f9
#define b3	$f10
#define b4	$f11

#define t1	$f0
#define t2	$f1
#define t3	$f2
#define t4	$f3

	PROLOGUE

	dsll	INCX, INCX, ZBASE_SHIFT
	li	TEMP, 2 * SIZE

	blez	N, .L999
	dsll	INCY, INCY, ZBASE_SHIFT

	bne	INCX, TEMP, .L20
	dsra	I, N, 1

	bne	INCY, TEMP, .L20
	NOP

	blez	I, .L15
	daddiu	I, I, -1

	LD	a1,  0 * SIZE(X)
	LD	b1,  0 * SIZE(Y)
	LD	a2,  1 * SIZE(X)
	LD	b2,  1 * SIZE(Y)

	LD	a3,  2 * SIZE(X)
	LD	b3,  2 * SIZE(Y)
	MUL	t1, S, b1

	LD	a4,  3 * SIZE(X)
	MUL	t2, C, b1
	LD	b4,  3 * SIZE(Y)
	MUL	t3, S, b2

	blez	I, .L13
	MUL	t4, C, b2
	.align 3

.L12:
	MADD	t1, t1, C, a1
	LD	b1,  4 * SIZE(Y)
	NMSUB	t2, t2, S, a1
	LD	a1,  4 * SIZE(X)
	MADD	t3, t3, C, a2
	LD	b2,  5 * SIZE(Y)
	NMSUB	t4, t4, S, a2
	LD	a2,  5 * SIZE(X)

	ST	t1,  0 * SIZE(X)
	MUL	t1, S, b3
	ST	t2,  0 * SIZE(Y)
	MUL	t2, C, b3
	ST	t3,  1 * SIZE(X)
	MUL	t3, S, b4
	ST	t4,  1 * SIZE(Y)
	MUL	t4, C, b4

	MADD	t1, t1, C, a3
	LD	b3,  6 * SIZE(Y)
	NMSUB	t2, t2, S, a3
	LD	a3,  6 * SIZE(X)
	MADD	t3, t3, C, a4
	LD	b4,  7 * SIZE(Y)
	NMSUB	t4, t4, S, a4
	LD	a4,  7 * SIZE(X)

	ST	t1,  2 * SIZE(X)
	MUL	t1, S, b1
	ST	t2,  2 * SIZE(Y)
	MUL	t2, C, b1
	ST	t3,  3 * SIZE(X)
	MUL	t3, S, b2
	ST	t4,  3 * SIZE(Y)
	MUL	t4, C, b2

	daddiu	I, I, -1
	daddiu	X, X, 4 * SIZE

	bgtz	I, .L12
	daddiu	Y, Y, 4 * SIZE
	.align 3

.L13:
	MADD	t1, t1, C, a1
	NMSUB	t2, t2, S, a1
	MADD	t3, t3, C, a2
	NMSUB	t4, t4, S, a2

	ST	t1,  0 * SIZE(X)
	MUL	t1, S, b3
	ST	t2,  0 * SIZE(Y)
	MUL	t2, C, b3
	ST	t3,  1 * SIZE(X)
	MUL	t3, S, b4
	ST	t4,  1 * SIZE(Y)
	MUL	t4, C, b4

	MADD	t1, t1, C, a3
	NMSUB	t2, t2, S, a3
	MADD	t3, t3, C, a4
	daddiu	X, X, 4 * SIZE
	NMSUB	t4, t4, S, a4
	daddiu	Y, Y, 4 * SIZE

	ST	t1, -2 * SIZE(X)
	ST	t2, -2 * SIZE(Y)
	ST	t3, -1 * SIZE(X)
	ST	t4, -1 * SIZE(Y)
	.align 3

.L15:
	andi	I,  N, 1

	blez	I, .L999
	NOP

	LD	a1,  0 * SIZE(X)
	LD	a2,  1 * SIZE(X)
	LD	b1,  0 * SIZE(Y)
	LD	b2,  1 * SIZE(Y)

	MUL	t1, S, b1
	MUL	t2, C, b1
	MUL	t3, S, b2
	MUL	t4, C, b2

	MADD	t1, t1, C, a1
	NMSUB	t2, t2, S, a1
	MADD	t3, t3, C, a2
	NMSUB	t4, t4, S, a2

	ST	t1,  0 * SIZE(X)
	ST	t2,  0 * SIZE(Y)
	ST	t3,  1 * SIZE(X)

	j	.L999
	ST	t4,  1 * SIZE(Y)
	.align 3

.L20:
	move	XX, X
	move	YY, Y

	blez	I, .L25
	daddiu	I, I, -1

	LD	a1,  0 * SIZE(X)
	LD	b1,  0 * SIZE(Y)

	LD	a2,  1 * SIZE(X)
	dadd	X, X, INCX
	LD	b2,  1 * SIZE(Y)
	dadd	Y, Y, INCY

	LD	a3,  0 * SIZE(X)
	LD	b3,  0 * SIZE(Y)

	LD	a4,  1 * SIZE(X)
	dadd	X, X, INCX
	MUL	t1, S, b1
	LD	b4,  1 * SIZE(Y)
	MUL	t2, C, b1
	dadd	Y, Y, INCY

	MUL	t3, S, b2
	blez	I, .L23
	MUL	t4, C, b2
	.align 3

.L22:
	MADD	t1, t1, C, a1
	LD	b1,  0 * SIZE(Y)
	NMSUB	t2, t2, S, a1
	LD	a1,  0 * SIZE(X)
	MADD	t3, t3, C, a2
	LD	b2,  1 * SIZE(Y)
	dadd	Y, Y, INCY
	NMSUB	t4, t4, S, a2
	LD	a2,  1 * SIZE(X)
	dadd	X, X, INCX

	ST	t1,  0 * SIZE(XX)
	MUL	t1, S, b3
	ST	t2,  0 * SIZE(YY)
	MUL	t2, C, b3
	ST	t3,  1 * SIZE(XX)
	dadd	XX, XX, INCX
	MUL	t3, S, b4
	ST	t4,  1 * SIZE(YY)
	dadd	YY, YY, INCY
	MUL	t4, C, b4

	MADD	t1, t1, C, a3
	LD	b3,  0 * SIZE(Y)
	NMSUB	t2, t2, S, a3
	LD	a3,  0 * SIZE(X)
	MADD	t3, t3, C, a4
	LD	b4,  1 * SIZE(Y)
	dadd	Y, Y, INCY
	NMSUB	t4, t4, S, a4
	LD	a4,  1 * SIZE(X)
	dadd	X, X, INCX

	ST	t1,  0 * SIZE(XX)
	MUL	t1, S, b1
	ST	t2,  0 * SIZE(YY)
	MUL	t2, C, b1
	ST	t3,  1 * SIZE(XX)
	dadd	XX, XX, INCX
	MUL	t3, S, b2
	ST	t4,  1 * SIZE(YY)
	MUL	t4, C, b2
	daddiu	I, I, -1

	bgtz	I, .L22
	dadd	YY, YY, INCY
	.align 3

.L23:
	MADD	t1, t1, C, a1
	NMSUB	t2, t2, S, a1
	MADD	t3, t3, C, a2
	NMSUB	t4, t4, S, a2

	ST	t1,  0 * SIZE(XX)
	MUL	t1, S, b3
	ST	t2,  0 * SIZE(YY)
	MUL	t2, C, b3
	ST	t3,  1 * SIZE(XX)
	dadd	XX, XX, INCX
	MUL	t3, S, b4
	ST	t4,  1 * SIZE(YY)
	dadd	YY, YY, INCY
	MUL	t4, C, b4

	MADD	t1, t1, C, a3
	NMSUB	t2, t2, S, a3
	MADD	t3, t3, C, a4
	NMSUB	t4, t4, S, a4

	ST	t1,  0 * SIZE(XX)
	ST	t2,  0 * SIZE(YY)
	ST	t3,  1 * SIZE(XX)
	dadd	XX, XX, INCX
	ST	t4,  1 * SIZE(YY)
	dadd	YY, YY, INCY
	.align 3

.L25:
	andi	I,  N, 1

	blez	I, .L999
	NOP
	.align	3

.L26:
	LD	a1,  0 * SIZE(X)
	LD	a2,  1 * SIZE(X)
	LD	b1,  0 * SIZE(Y)
	LD	b2,  1 * SIZE(Y)

	MUL	t1, S, b1
	MUL	t2, C, b1
	MUL	t3, S, b2
	MUL	t4, C, b2

	MADD	t1, t1, C, a1
	NMSUB	t2, t2, S, a1
	MADD	t3, t3, C, a2
	NMSUB	t4, t4, S, a2

	ST	t1,  0 * SIZE(X)
	ST	t2,  0 * SIZE(Y)
	ST	t3,  1 * SIZE(X)
	ST	t4,  1 * SIZE(Y)
	.align 3

.L999:
	j	$31
	NOP

	EPILOGUE
