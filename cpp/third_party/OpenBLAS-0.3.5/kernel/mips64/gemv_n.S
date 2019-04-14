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

#define M	$4
#define	N	$5
#define A	$8
#define LDA	$9
#define X	$10
#define INCX	$11
#define Y	$2
#define INCY	$6
#define BUFFER	$7

#define YORIG	$3
#define XX	$12
#define YY	$13

#define I	$14
#define J	$15

#define AO1	$16
#define AO2	$17

#define ALPHA	$f15

#define a1	$f0
#define a2	$f1
#define a3	$f2
#define a4	$f3
#define a5	$f4
#define a6	$f5
#define a7	$f6
#define a8	$f7

#define x1	$f8
#define x2	$f9

#define y1	$f10
#define y2	$f11
#define y3	$f12
#define y4	$f13
#define y5	$f14
#define y6	$f16
#define y7	$f17
#define y8	$f18

#define t1	$f19
#define t2	$f20
#define t3	$f21
#define t4	$f22


	PROLOGUE

	LDARG	Y,       0($sp)
	LDARG	INCY,    8($sp)
	LDARG	BUFFER, 16($sp)
#ifdef __64BIT__
	daddiu	$sp, $sp, -16
#else
	daddiu	$sp, $sp, -48
#endif

	SDARG	$16,   0($sp)

	SDARG	$17,   8($sp)
	dsll	LDA,  LDA,  BASE_SHIFT

#ifndef __64BIT__
	sdc1	$f20, 16($sp)
	sdc1	$f21, 24($sp)
	sdc1	$f22, 32($sp)
#endif

	blez	M, .L999
	dsll	INCX, INCX, BASE_SHIFT

	blez	N, .L999
	dsll	INCY, INCY, BASE_SHIFT

	li	YORIG, SIZE

	beq	INCY, YORIG, .L10
	move	YORIG, Y

	dsra	I,  M, 2
	move	YORIG, BUFFER

	move	XX, Y

	blez	I, .L05
	move	YY, BUFFER
	.align 3

.L02:
	LD	a1, 0 * SIZE(XX)
	daddu	XX, XX, INCY
	LD	a2, 0 * SIZE(XX)
	daddu	XX, XX, INCY
	LD	a3, 0 * SIZE(XX)
	daddu	XX, XX, INCY
	LD	a4, 0 * SIZE(XX)
	daddu	XX, XX, INCY

	ST	a1, 0 * SIZE(YY)
	ST	a2, 1 * SIZE(YY)
	ST	a3, 2 * SIZE(YY)
	ST	a4, 3 * SIZE(YY)
	daddiu	I, I, -1

	bgtz	I, .L02
	daddiu	YY, YY, 4 * SIZE
	.align 3

.L05:
	andi	I,  M, 3
	blez	I, .L10
	NOP
	.align 3

.L06:
	LD	a1, 0 * SIZE(XX)
	daddu	XX, XX, INCY

	ST	a1, 0 * SIZE(YY)
	daddiu	I, I, -1

	bgtz	I, .L06
	daddiu	YY, YY, 1 * SIZE
	.align 3

.L10:
	dsra	J,  N, 1
	blez	J, .L20
	NOP
	.align 3

.L11:
	LD	x1, 0 * SIZE(X)
	daddu	X, X, INCX
	LD	x2, 0 * SIZE(X)
	daddu	X, X, INCX

	move	AO1, A
	daddu	AO2, A,   LDA
	daddu	A,   AO2, LDA

	move	YY, YORIG
	MUL	x1, ALPHA, x1

	dsra	I,  M, 3
	blez	I, .L15
	MUL	x2, ALPHA, x2

	LD	a1, 0 * SIZE(AO1)
	LD	y1, 0 * SIZE(YY)
	LD	a2, 1 * SIZE(AO1)
	LD	y2, 1 * SIZE(YY)

	LD	a3, 2 * SIZE(AO1)
	LD	y3, 2 * SIZE(YY)
	LD	a4, 3 * SIZE(AO1)
	LD	y4, 3 * SIZE(YY)

	LD	a5, 0 * SIZE(AO2)
	LD	y5, 4 * SIZE(YY)
	LD	a6, 1 * SIZE(AO2)
	LD	y6, 5 * SIZE(YY)

	LD	a7, 2 * SIZE(AO2)
	LD	y7, 6 * SIZE(YY)
	LD	a8, 3 * SIZE(AO2)
	daddiu	I, I, -1

	blez	I, .L13
	LD	y8, 7 * SIZE(YY)
	.align	3

.L12:
	MADD	t1, y1, x1, a1
	LD	a1,  4 * SIZE(AO1)
	MADD	t2, y2, x1, a2
	LD	a2,  5 * SIZE(AO1)

	LD	y1,  8 * SIZE(YY)
	LD	y2,  9 * SIZE(YY)

	MADD	t3, y3, x1, a3
	LD	a3,  6 * SIZE(AO1)
	MADD	t4, y4, x1, a4
	LD	a4,  7 * SIZE(AO1)

	LD	y3, 10 * SIZE(YY)
	LD	y4, 11 * SIZE(YY)

	MADD	t1, t1, x2, a5
	LD	a5,  4 * SIZE(AO2)
	MADD	t2, t2, x2, a6
	LD	a6,  5 * SIZE(AO2)
	MADD	t3, t3, x2, a7
	LD	a7,  6 * SIZE(AO2)
	MADD	t4, t4, x2, a8
	LD	a8,  7 * SIZE(AO2)

	ST	t1,  0 * SIZE(YY)
	ST	t2,  1 * SIZE(YY)
	ST	t3,  2 * SIZE(YY)
	ST	t4,  3 * SIZE(YY)

	MADD	t1, y5, x1, a1
	LD	a1,  8 * SIZE(AO1)
	MADD	t2, y6, x1, a2
	LD	a2,  9 * SIZE(AO1)

	LD	y5, 12 * SIZE(YY)
	LD	y6, 13 * SIZE(YY)

	MADD	t3, y7, x1, a3
	LD	a3, 10 * SIZE(AO1)
	MADD	t4, y8, x1, a4
	LD	a4, 11 * SIZE(AO1)

	LD	y7, 14 * SIZE(YY)
	LD	y8, 15 * SIZE(YY)

	MADD	t1, t1, x2, a5
	LD	a5,  8 * SIZE(AO2)
	MADD	t2, t2, x2, a6
	LD	a6,  9 * SIZE(AO2)
	MADD	t3, t3, x2, a7
	LD	a7, 10 * SIZE(AO2)
	MADD	t4, t4, x2, a8
	LD	a8, 11 * SIZE(AO2)

	ST	t1, 4 * SIZE(YY)
	ST	t2, 5 * SIZE(YY)
	ST	t3, 6 * SIZE(YY)
	ST	t4, 7 * SIZE(YY)

	daddiu	I, I, -1
	daddiu	YY,  YY,   8 * SIZE

	daddiu	AO1, AO1,  8 * SIZE
	bgtz	I, .L12
	daddiu	AO2, AO2,  8 * SIZE
	.align 3

.L13:
	MADD	t1, y1, x1, a1
	LD	a1,  4 * SIZE(AO1)
	MADD	t2, y2, x1, a2
	LD	a2,  5 * SIZE(AO1)
	MADD	t3, y3, x1, a3
	LD	a3,  6 * SIZE(AO1)
	MADD	t4, y4, x1, a4
	LD	a4,  7 * SIZE(AO1)

	MADD	t1, t1, x2, a5
	LD	a5,  4 * SIZE(AO2)
	MADD	t2, t2, x2, a6
	LD	a6,  5 * SIZE(AO2)
	MADD	t3, t3, x2, a7
	LD	a7,  6 * SIZE(AO2)
	MADD	t4, t4, x2, a8
	LD	a8,  7 * SIZE(AO2)

	ST	t1,  0 * SIZE(YY)
	MADD	t1, y5, x1, a1
	ST	t2,  1 * SIZE(YY)
	MADD	t2, y6, x1, a2
	ST	t3,  2 * SIZE(YY)
	MADD	t3, y7, x1, a3
	ST	t4,  3 * SIZE(YY)
	MADD	t4, y8, x1, a4

	MADD	t1, t1, x2, a5
	daddiu	AO1, AO1,  8 * SIZE
	MADD	t2, t2, x2, a6
	daddiu	AO2, AO2,  8 * SIZE
	MADD	t3, t3, x2, a7
	daddiu	YY,  YY,   8 * SIZE
	MADD	t4, t4, x2, a8
	NOP

	ST	t1, -4 * SIZE(YY)
	ST	t2, -3 * SIZE(YY)
	ST	t3, -2 * SIZE(YY)
	ST	t4, -1 * SIZE(YY)
	.align 3

.L15:
	andi	I,  M, 4
	NOP
	blez	I, .L16
	NOP

	LD	a1, 0 * SIZE(AO1)
	LD	y1, 0 * SIZE(YY)
	LD	a2, 1 * SIZE(AO1)
	LD	y2, 1 * SIZE(YY)

	LD	a3, 2 * SIZE(AO1)
	LD	y3, 2 * SIZE(YY)
	LD	a4, 3 * SIZE(AO1)
	LD	y4, 3 * SIZE(YY)

	LD	a5, 0 * SIZE(AO2)
	MADD	y1, y1, x1, a1
	LD	a6, 1 * SIZE(AO2)
	MADD	y2, y2, x1, a2
	LD	a7, 2 * SIZE(AO2)
	MADD	y3, y3, x1, a3
	LD	a8, 3 * SIZE(AO2)
	MADD	y4, y4, x1, a4

	MADD	y1, y1, x2, a5
	daddiu	YY,  YY,   4 * SIZE
	MADD	y2, y2, x2, a6
	daddiu	AO1, AO1,  4 * SIZE
	MADD	y3, y3, x2, a7
	daddiu	AO2, AO2,  4 * SIZE
	MADD	y4, y4, x2, a8

	ST	y1, -4 * SIZE(YY)
	ST	y2, -3 * SIZE(YY)
	ST	y3, -2 * SIZE(YY)
	ST	y4, -1 * SIZE(YY)
	.align 3

.L16:
	andi	I,  M, 2
	NOP
	blez	I, .L17
	NOP

	LD	a1, 0 * SIZE(AO1)
	LD	y1, 0 * SIZE(YY)
	LD	a2, 1 * SIZE(AO1)
	LD	y2, 1 * SIZE(YY)

	LD	a5, 0 * SIZE(AO2)
	LD	a6, 1 * SIZE(AO2)

	MADD	y1, y1, x1, a1
	NOP
	MADD	y2, y2, x1, a2
	daddiu	YY,  YY,   2 * SIZE
	MADD	y1, y1, x2, a5
	daddiu	AO1, AO1,  2 * SIZE
	MADD	y2, y2, x2, a6
	daddiu	AO2, AO2,  2 * SIZE

	ST	y1, -2 * SIZE(YY)
	ST	y2, -1 * SIZE(YY)
	.align 3

.L17:
	andi	I,  M, 1
	NOP
	blez	I, .L19
	NOP

	LD	y1, 0 * SIZE(YY)
	LD	a1, 0 * SIZE(AO1)
	LD	a5, 0 * SIZE(AO2)

	MADD	y1, y1, x1, a1
	MADD	y1, y1, x2, a5

	ST	y1, 0 * SIZE(YY)
	.align 3


.L19:
	daddiu	J, J, -1

	bgtz	J, .L11
	NOP
	.align 3

.L20:
	andi	J,  N, 1
	blez	J, .L900
	NOP
	.align 3

.L21:
	LD	x1, 0 * SIZE(X)
	daddu	X, X, INCX

	move	YY, YORIG
	move	AO1, A

	dsra	I,  M, 3
	blez	I, .L25
	MUL	x1, ALPHA, x1


	LD	a1, 0 * SIZE(AO1)
	LD	y1, 0 * SIZE(YY)
	LD	a2, 1 * SIZE(AO1)
	LD	y2, 1 * SIZE(YY)

	LD	a3, 2 * SIZE(AO1)
	LD	y3, 2 * SIZE(YY)
	LD	a4, 3 * SIZE(AO1)
	LD	y4, 3 * SIZE(YY)

	LD	y5, 4 * SIZE(YY)
	LD	y6, 5 * SIZE(YY)

	LD	y7, 6 * SIZE(YY)
	daddiu	I, I, -1

	blez	I, .L23
	LD	y8, 7 * SIZE(YY)
	.align	3

.L22:
	MADD	t1, y1, x1, a1
	LD	a1,  4 * SIZE(AO1)
	MADD	t2, y2, x1, a2
	LD	a2,  5 * SIZE(AO1)

	LD	y1,  8 * SIZE(YY)
	LD	y2,  9 * SIZE(YY)

	MADD	t3, y3, x1, a3
	LD	a3,  6 * SIZE(AO1)
	MADD	t4, y4, x1, a4
	LD	a4,  7 * SIZE(AO1)

	LD	y3, 10 * SIZE(YY)
	LD	y4, 11 * SIZE(YY)

	ST	t1,  0 * SIZE(YY)
	ST	t2,  1 * SIZE(YY)
	ST	t3,  2 * SIZE(YY)
	ST	t4,  3 * SIZE(YY)

	MADD	t1, y5, x1, a1
	LD	a1,  8 * SIZE(AO1)
	MADD	t2, y6, x1, a2
	LD	a2,  9 * SIZE(AO1)

	LD	y5, 12 * SIZE(YY)
	LD	y6, 13 * SIZE(YY)

	MADD	t3, y7, x1, a3
	LD	a3, 10 * SIZE(AO1)
	MADD	t4, y8, x1, a4
	LD	a4, 11 * SIZE(AO1)

	LD	y7, 14 * SIZE(YY)
	LD	y8, 15 * SIZE(YY)

	ST	t1, 4 * SIZE(YY)
	ST	t2, 5 * SIZE(YY)
	ST	t3, 6 * SIZE(YY)
	ST	t4, 7 * SIZE(YY)

	daddiu	I, I, -1
	daddiu	YY,  YY,   8 * SIZE

	bgtz	I, .L22
	daddiu	AO1, AO1,  8 * SIZE
	.align 3

.L23:
	MADD	t1, y1, x1, a1
	LD	a1,  4 * SIZE(AO1)
	MADD	t2, y2, x1, a2
	LD	a2,  5 * SIZE(AO1)
	MADD	t3, y3, x1, a3
	LD	a3,  6 * SIZE(AO1)
	MADD	t4, y4, x1, a4
	LD	a4,  7 * SIZE(AO1)

	ST	t1,  0 * SIZE(YY)
	MADD	t1, y5, x1, a1
	ST	t2,  1 * SIZE(YY)
	MADD	t2, y6, x1, a2
	ST	t3,  2 * SIZE(YY)
	MADD	t3, y7, x1, a3
	ST	t4,  3 * SIZE(YY)
	MADD	t4, y8, x1, a4

	ST	t1,  4 * SIZE(YY)
	ST	t2,  5 * SIZE(YY)
	ST	t3,  6 * SIZE(YY)
	ST	t4,  7 * SIZE(YY)

	daddiu	AO1, AO1,  8 * SIZE
	daddiu	YY,  YY,   8 * SIZE
	.align 3

.L25:
	andi	I,  M, 4
	NOP
	blez	I, .L26
	NOP

	LD	a1, 0 * SIZE(AO1)
	LD	y1, 0 * SIZE(YY)
	LD	a2, 1 * SIZE(AO1)
	LD	y2, 1 * SIZE(YY)

	LD	a3, 2 * SIZE(AO1)
	LD	y3, 2 * SIZE(YY)
	LD	a4, 3 * SIZE(AO1)
	LD	y4, 3 * SIZE(YY)

	MADD	y1, y1, x1, a1
	MADD	y2, y2, x1, a2

	MADD	y3, y3, x1, a3
	daddiu	YY,  YY,   4 * SIZE
	MADD	y4, y4, x1, a4
	daddiu	AO1, AO1,  4 * SIZE

	ST	y1, -4 * SIZE(YY)
	ST	y2, -3 * SIZE(YY)
	ST	y3, -2 * SIZE(YY)
	ST	y4, -1 * SIZE(YY)
	.align 3

.L26:
	andi	I,  M, 2
	NOP
	blez	I, .L27
	NOP

	LD	a1, 0 * SIZE(AO1)
	LD	y1, 0 * SIZE(YY)
	LD	a2, 1 * SIZE(AO1)
	LD	y2, 1 * SIZE(YY)

	MADD	y1, y1, x1, a1
	daddiu	YY,  YY,   2 * SIZE
	MADD	y2, y2, x1, a2
	daddiu	AO1, AO1,  2 * SIZE

	ST	y1, -2 * SIZE(YY)
	ST	y2, -1 * SIZE(YY)
	.align 3

.L27:
	andi	I,  M, 1
	NOP
	blez	I, .L900
	NOP

	LD	y1, 0 * SIZE(YY)
	LD	a1, 0 * SIZE(AO1)

	MADD	y1, y1, x1, a1

	ST	y1, 0 * SIZE(YY)
	.align 3


.L900:
	li	YORIG, SIZE

	beq	INCY, YORIG, .L999
	dsra	I,  M, 2

	blez	I, .L905
	move	XX, BUFFER
	.align 3

.L902:
	LD	a1, 0 * SIZE(XX)
	LD	a2, 1 * SIZE(XX)
	LD	a3, 2 * SIZE(XX)
	LD	a4, 3 * SIZE(XX)

	ST	a1, 0 * SIZE(Y)
	daddu	Y, Y, INCY
	ST	a2, 0 * SIZE(Y)
	daddu	Y, Y, INCY
	ST	a3, 0 * SIZE(Y)
	daddu	Y, Y, INCY
	ST	a4, 0 * SIZE(Y)
	daddu	Y, Y, INCY

	daddiu	I, I, -1

	bgtz	I, .L902
	daddiu	XX, XX, 4 * SIZE
	.align 3

.L905:
	andi	I,  M, 3
	blez	I, .L999
	NOP
	.align 3

.L906:
	LD	a1, 0 * SIZE(XX)
	daddiu	XX, XX, 1 * SIZE

	ST	a1, 0 * SIZE(Y)
	daddiu	I, I, -1

	bgtz	I, .L906
	daddu	Y, Y, INCY
	.align 3

.L999:
	LDARG	$16,   0($sp)
	LDARG	$17,   8($sp)

#ifndef __64BIT__
	ldc1	$f20, 16($sp)
	ldc1	$f21, 24($sp)
	ldc1	$f22, 32($sp)
#endif

	j	$31
#ifdef __64BIT__
	daddiu	$sp, $sp, 16
#else
	daddiu	$sp, $sp, 48
#endif

	EPILOGUE
