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

#define I	$2
#define TEMP	$3

#define a1	$f0
#define a2	$f1
#define a3	$f2
#define a4	$f3
#define a5	$f4
#define a6	$f5
#define a7	$f6
#define a8	$f7

	PROLOGUE

#ifdef F_INTERFACE
	LDINT	N,     0(N)
	LDINT	INCX,  0(INCX)
	LDINT	INCY,  0(INCY)
#endif

	li	TEMP, SIZE
	NOP

	blez	N, .L999
	dsll	INCX, INCX, BASE_SHIFT

	bne	INCX, TEMP, .L20
	dsll	INCY, INCY, BASE_SHIFT

	bne	INCY, TEMP, .L20
	dsra	I, N, 3

	blez	I, .L15
	daddiu	I, I, -1

	LD	a1,  0 * SIZE(X)
	LD	a2,  1 * SIZE(X)
	LD	a3,  2 * SIZE(X)
	LD	a4,  3 * SIZE(X)
	LD	a5,  4 * SIZE(X)
	LD	a6,  5 * SIZE(X)
	LD	a7,  6 * SIZE(X)
	LD	a8,  7 * SIZE(X)

	blez	I, .L13
	NOP
	.align 3

.L12:
	ST	a1,  0 * SIZE(Y)
	LD	a1,  8 * SIZE(X)

	ST	a2,  1 * SIZE(Y)
	LD	a2,  9 * SIZE(X)

	ST	a3,  2 * SIZE(Y)
	LD	a3, 10 * SIZE(X)

	ST	a4,  3 * SIZE(Y)
	LD	a4, 11 * SIZE(X)

	ST	a5,  4 * SIZE(Y)
	LD	a5, 12 * SIZE(X)

	ST	a6,  5 * SIZE(Y)
	LD	a6, 13 * SIZE(X)

	ST	a7,  6 * SIZE(Y)
	LD	a7, 14 * SIZE(X)

	ST	a8,  7 * SIZE(Y)
	LD	a8, 15 * SIZE(X)

	daddiu	I, I, -1
	daddiu	X, X, 8 * SIZE

	bgtz	I, .L12
	daddiu	Y, Y, 8 * SIZE
	.align 3

.L13:
	ST	a1,  0 * SIZE(Y)
	ST	a2,  1 * SIZE(Y)
	ST	a3,  2 * SIZE(Y)
	ST	a4,  3 * SIZE(Y)
	ST	a5,  4 * SIZE(Y)
	ST	a6,  5 * SIZE(Y)
	ST	a7,  6 * SIZE(Y)
	ST	a8,  7 * SIZE(Y)

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
	daddiu	X, X, SIZE

	daddiu	I, I, -1
	daddiu	Y, Y, SIZE

	bgtz	I, .L16
	ST	a1, -1 * SIZE(Y)

	j	.L999
	NOP
	.align 3

.L20:
	dsra	I, N, 3

	blez	I, .L25
	daddiu	I, I, -1

	LD	a1,  0 * SIZE(X)
	daddu	X, X, INCX
	LD	a2,  0 * SIZE(X)
	daddu	X, X, INCX
	LD	a3,  0 * SIZE(X)
	daddu	X, X, INCX
	LD	a4,  0 * SIZE(X)
	daddu	X, X, INCX
	LD	a5,  0 * SIZE(X)
	daddu	X, X, INCX
	LD	a6,  0 * SIZE(X)
	daddu	X, X, INCX
	LD	a7,  0 * SIZE(X)
	daddu	X, X, INCX
	LD	a8,  0 * SIZE(X)
	daddu	X, X, INCX

	blez	I, .L23
	NOP
	.align 3

.L22:
	ST	a1,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	LD	a1,  0 * SIZE(X)
	daddu	X, X, INCX

	ST	a2,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	LD	a2,  0 * SIZE(X)
	daddu	X, X, INCX

	ST	a3,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	LD	a3,  0 * SIZE(X)
	daddu	X, X, INCX

	ST	a4,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	LD	a4,  0 * SIZE(X)
	daddu	X, X, INCX

	ST	a5,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	LD	a5,  0 * SIZE(X)
	daddu	X, X, INCX

	ST	a6,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	LD	a6,  0 * SIZE(X)
	daddu	X, X, INCX

	ST	a7,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	LD	a7,  0 * SIZE(X)
	daddu	X, X, INCX

	ST	a8,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	LD	a8,  0 * SIZE(X)

	daddiu	I, I, -1

	bgtz	I, .L22
	daddu	X, X, INCX
	.align 3

.L23:
	ST	a1,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	ST	a2,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	ST	a3,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	ST	a4,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	ST	a5,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	ST	a6,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	ST	a7,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	ST	a8,  0 * SIZE(Y)
	daddu	Y, Y, INCY
	.align 3

.L25:
	andi	I,  N, 7

	blez	I, .L999
	NOP
	.align	3

.L26:
	LD	a1,  0 * SIZE(X)
	daddu	X, X, INCX

	daddiu	I, I, -1
	ST	a1,  0 * SIZE(Y)

	bgtz	I, .L26
	daddu	Y, Y, INCY
	.align 3

.L999:
	j	$31
	NOP

	EPILOGUE
