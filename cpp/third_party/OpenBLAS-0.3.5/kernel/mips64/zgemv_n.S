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
#define A	$9
#define LDA	$10
#define X	$11
#define INCX	$2
#define Y	$6
#define INCY	$7
#define BUFFER	$8

#define YORIG	$3
#define XX	$12
#define YY	$13

#define I	$14
#define J	$15

#define AO1	$16
#define AO2	$17

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

#define x1	$f8
#define x2	$f9
#define x3	$f10
#define x4	$f11

#define y1	$f12
#define y2	$f13
#define y3	$f14
#define y4	$f17

#define t1	$f18
#define t2	$f19
#define t3	$f20
#define t4	$f21
#define t5	$f22
#define t6	$f23
#define t7	$f24
#define t8	$f25

#if !defined(CONJ) && !defined(XCONJ)
#define MADD1	   MADD
#define MADD2	   MADD
#define MADD3	   NMSUB
#define MADD4	   MADD
#endif

#if  defined(CONJ) && !defined(XCONJ)
#define MADD1	   MADD
#define MADD2	   MADD
#define MADD3	   MADD
#define MADD4	   NMSUB
#endif

#if  !defined(CONJ) && defined(XCONJ)
#define MADD1	   MADD
#define MADD2	   NMSUB
#define MADD3	   MADD
#define MADD4	   MADD
#endif

#if  defined(CONJ) && defined(XCONJ)
#define MADD1	   MADD
#define MADD2	   NMSUB
#define MADD3	   NMSUB
#define MADD4	   NMSUB
#endif

	PROLOGUE

	LDARG	INCX,    0($sp)
	LDARG	Y,       8($sp)
	LDARG	INCY,   16($sp)
	LDARG	BUFFER, 24($sp)
#ifndef __64BIT__
	daddiu	$sp, $sp, -64
#else
	daddiu	$sp, $sp, -32
#endif

	SDARG	$16,   0($sp)
	SDARG	$17,   8($sp)

	sdc1	$f24, 16($sp)
	sdc1	$f25, 24($sp)

#ifndef __64BIT__
	sdc1	$f20, 32($sp)
	sdc1	$f21, 40($sp)
	sdc1	$f22, 48($sp)
	sdc1	$f23, 56($sp)
#endif

	dsll	LDA,  LDA,  ZBASE_SHIFT

	blez	M, .L999
	dsll	INCX, INCX, ZBASE_SHIFT

	blez	N, .L999
	dsll	INCY, INCY, ZBASE_SHIFT

	li	YORIG, 2 * SIZE

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
	LD	a2, 1 * SIZE(XX)
	daddu	XX, XX, INCY
	LD	a3, 0 * SIZE(XX)
	LD	a4, 1 * SIZE(XX)
	daddu	XX, XX, INCY
	LD	a5, 0 * SIZE(XX)
	LD	a6, 1 * SIZE(XX)
	daddu	XX, XX, INCY
	LD	a7, 0 * SIZE(XX)
	LD	a8, 1 * SIZE(XX)
	daddu	XX, XX, INCY

	daddiu	I, I, -1
	daddiu	YY, YY, 8 * SIZE

	ST	a1, -8 * SIZE(YY)
	ST	a2, -7 * SIZE(YY)
	ST	a3, -6 * SIZE(YY)
	ST	a4, -5 * SIZE(YY)
	ST	a5, -4 * SIZE(YY)
	ST	a6, -3 * SIZE(YY)
	ST	a7, -2 * SIZE(YY)

	bgtz	I, .L02
	ST	a8, -1 * SIZE(YY)
	.align 3

.L05:
	andi	I,  M, 3
	blez	I, .L10
	NOP
	.align 3

.L06:
	LD	a1, 0 * SIZE(XX)
	LD	a2, 1 * SIZE(XX)
	daddu	XX, XX, INCY

	daddiu	I, I, -1

	ST	a1, 0 * SIZE(YY)
	ST	a2, 1 * SIZE(YY)

	bgtz	I, .L06
	daddiu	YY, YY, 2 * SIZE
	.align 3

.L10:
	dsra	J,  N, 1
	blez	J, .L20
	NOP
	.align 3

.L11:
	LD	x1, 0 * SIZE(X)
	LD	x2, 1 * SIZE(X)
	daddu	X, X, INCX
	LD	x3, 0 * SIZE(X)
	LD	x4, 1 * SIZE(X)
	daddu	X, X, INCX

	MUL	a1, ALPHA_R, x1
	move	AO1, A
	MUL	a2, ALPHA_I, x1
	daddu	AO2, A,   LDA
	MUL	a3, ALPHA_R, x3
	daddu	A,   AO2, LDA
	MUL	a4, ALPHA_I, x3

#ifndef XCONJ
	NMSUB	x1, a1, ALPHA_I, x2
	MADD	x2, a2, ALPHA_R, x2
	NMSUB	x3, a3, ALPHA_I, x4
	MADD	x4, a4, ALPHA_R, x4
#else
	MADD	x1, a1, ALPHA_I, x2
	MSUB	x2, a2, ALPHA_R, x2
	MADD	x3, a3, ALPHA_I, x4
	MSUB	x4, a4, ALPHA_R, x4
#endif

	dsra	I,  M, 2

	blez	I, .L15
	move	YY, YORIG

	LD	y1, 0 * SIZE(YY)
	LD	a1, 0 * SIZE(AO1)
	LD	y2, 1 * SIZE(YY)
	LD	a3, 2 * SIZE(AO1)
	LD	y3, 2 * SIZE(YY)
	LD	a2, 1 * SIZE(AO1)
	LD	y4, 3 * SIZE(YY)
	LD	a4, 3 * SIZE(AO1)

	LD	a5, 0 * SIZE(AO2)
	LD	a6, 1 * SIZE(AO2)
	LD	a7, 2 * SIZE(AO2)
	LD	a8, 3 * SIZE(AO2)

	MADD1	t1, y1, x1, a1
	LD	y1,  4 * SIZE(YY)
	MADD2	t2, y2, x2, a1
	LD	a1,  4 * SIZE(AO1)
	MADD1	t3, y3, x1, a3
	LD	y2,  5 * SIZE(YY)
	MADD2	t4, y4, x2, a3
	LD	a3,  6 * SIZE(AO1)

	MADD3	t1, t1, x2, a2
	LD	y3,  6 * SIZE(YY)
	MADD4	t2, t2, x1, a2
	LD	a2,  5 * SIZE(AO1)
	MADD3	t3, t3, x2, a4
	LD	y4,  7 * SIZE(YY)
	MADD4	t4, t4, x1, a4
	LD	a4,  7 * SIZE(AO1)

	MADD1	t1, t1, x3, a5
	NOP
	MADD2	t2, t2, x4, a5
	LD	a5,  4 * SIZE(AO2)
	MADD1	t3, t3, x3, a7
	NOP
	MADD2	t4, t4, x4, a7
	LD	a7,  6 * SIZE(AO2)

	MADD3	t1, t1, x4, a6
	NOP
	MADD4	t2, t2, x3, a6
	LD	a6,  5 * SIZE(AO2)
	MADD3	t3, t3, x4, a8
	daddiu	I, I, -1
	MADD4	t4, t4, x3, a8

	blez	I, .L13
	LD	a8,  7 * SIZE(AO2)
	.align	3

.L12:
	MADD1	t5, y1, x1, a1
	LD	y1,  8 * SIZE(YY)
	MADD2	t6, y2, x2, a1
	LD	a1,  8 * SIZE(AO1)
	MADD1	t7, y3, x1, a3
	LD	y2,  9 * SIZE(YY)
	MADD2	t8, y4, x2, a3
	LD	a3, 10 * SIZE(AO1)

	MADD3	t5, t5, x2, a2
	LD	y3, 10 * SIZE(YY)
	MADD4	t6, t6, x1, a2
	LD	a2,  9 * SIZE(AO1)
	MADD3	t7, t7, x2, a4
	LD	y4, 11 * SIZE(YY)
	MADD4	t8, t8, x1, a4
	LD	a4, 11 * SIZE(AO1)

	MADD1	t5, t5, x3, a5
	ST	t1,  0 * SIZE(YY)
	MADD2	t6, t6, x4, a5
	LD	a5,  8 * SIZE(AO2)
	MADD1	t7, t7, x3, a7
	ST	t2,  1 * SIZE(YY)
	MADD2	t8, t8, x4, a7
	LD	a7, 10 * SIZE(AO2)

	MADD3	t5, t5, x4, a6
	ST	t3,  2 * SIZE(YY)
	MADD4	t6, t6, x3, a6
	LD	a6,  9 * SIZE(AO2)
	MADD3	t7, t7, x4, a8
	ST	t4,  3 * SIZE(YY)
	MADD4	t8, t8, x3, a8
	LD	a8, 11 * SIZE(AO2)

	MADD1	t1, y1, x1, a1
	LD	y1, 12 * SIZE(YY)
	MADD2	t2, y2, x2, a1
	LD	a1, 12 * SIZE(AO1)
	MADD1	t3, y3, x1, a3
	LD	y2, 13 * SIZE(YY)
	MADD2	t4, y4, x2, a3
	LD	a3, 14 * SIZE(AO1)

	MADD3	t1, t1, x2, a2
	LD	y3, 14 * SIZE(YY)
	MADD4	t2, t2, x1, a2
	LD	a2, 13 * SIZE(AO1)
	MADD3	t3, t3, x2, a4
	LD	y4, 15 * SIZE(YY)
	MADD4	t4, t4, x1, a4
	LD	a4, 15 * SIZE(AO1)

	MADD1	t1, t1, x3, a5
	ST	t5,  4 * SIZE(YY)
	MADD2	t2, t2, x4, a5
	LD	a5, 12 * SIZE(AO2)
	MADD1	t3, t3, x3, a7
	ST	t6,  5 * SIZE(YY)
	MADD2	t4, t4, x4, a7
	LD	a7, 14 * SIZE(AO2)

	MADD3	t1, t1, x4, a6
	ST	t7,  6 * SIZE(YY)
	MADD4	t2, t2, x3, a6
	LD	a6, 13 * SIZE(AO2)
	MADD3	t3, t3, x4, a8
	ST	t8,  7 * SIZE(YY)
	MADD4	t4, t4, x3, a8
	LD	a8, 15 * SIZE(AO2)

	daddiu	I, I, -1
	daddiu	YY,  YY,   8 * SIZE

	daddiu	AO1, AO1,  8 * SIZE
	bgtz	I, .L12
	daddiu	AO2, AO2,  8 * SIZE
	.align 3

.L13:
	ST	t1,  0 * SIZE(YY)
	MADD1	t1, y1, x1, a1
	ST	t2,  1 * SIZE(YY)
	MADD2	t2, y2, x2, a1
	ST	t3,  2 * SIZE(YY)
	MADD1	t3, y3, x1, a3
	ST	t4,  3 * SIZE(YY)
	MADD2	t4, y4, x2, a3

	MADD3	t1, t1, x2, a2
	MADD4	t2, t2, x1, a2
	MADD3	t3, t3, x2, a4
	MADD4	t4, t4, x1, a4

	MADD1	t1, t1, x3, a5
	MADD2	t2, t2, x4, a5
	MADD1	t3, t3, x3, a7
	MADD2	t4, t4, x4, a7

	MADD3	t1, t1, x4, a6
	daddiu	AO1, AO1,  8 * SIZE
	MADD4	t2, t2, x3, a6
	daddiu	AO2, AO2,  8 * SIZE
	MADD3	t3, t3, x4, a8
	daddiu	YY,  YY,   8 * SIZE
	MADD4	t4, t4, x3, a8
	NOP

	ST	t1, -4 * SIZE(YY)
	ST	t2, -3 * SIZE(YY)
	ST	t3, -2 * SIZE(YY)
	ST	t4, -1 * SIZE(YY)
	.align 3

.L15:
	andi	I,  M, 2
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

	MADD1	t1, y1, x1, a1
	LD	a5, 0 * SIZE(AO2)
	MADD2	t2, y2, x2, a1
	LD	a6, 1 * SIZE(AO2)
	MADD1	t3, y3, x1, a3
	LD	a7, 2 * SIZE(AO2)
	MADD2	t4, y4, x2, a3
	LD	a8, 3 * SIZE(AO2)

	MADD3	t1, t1, x2, a2
	MADD4	t2, t2, x1, a2
	MADD3	t3, t3, x2, a4
	MADD4	t4, t4, x1, a4

	MADD1	t1, t1, x3, a5
	MADD2	t2, t2, x4, a5
	MADD1	t3, t3, x3, a7
	MADD2	t4, t4, x4, a7

	MADD3	t1, t1, x4, a6
	daddiu	YY,  YY,   4 * SIZE
	MADD4	t2, t2, x3, a6
	daddiu	AO1, AO1,  4 * SIZE
	MADD3	t3, t3, x4, a8
	daddiu	AO2, AO2,  4 * SIZE
	MADD4	t4, t4, x3, a8
	NOP

	ST	t1, -4 * SIZE(YY)
	ST	t2, -3 * SIZE(YY)
	ST	t3, -2 * SIZE(YY)
	ST	t4, -1 * SIZE(YY)
	.align 3

.L16:
	andi	I,  M, 1
	NOP
	blez	I, .L19
	NOP

	LD	y1, 0 * SIZE(YY)
	LD	y2, 1 * SIZE(YY)
	LD	a1, 0 * SIZE(AO1)
	LD	a2, 1 * SIZE(AO1)

	MADD1	t1, y1, x1, a1
	LD	a5, 0 * SIZE(AO2)
	MADD2	t2, y2, x2, a1
	LD	a6, 1 * SIZE(AO2)
	MADD3	t1, t1, x2, a2
	MADD4	t2, t2, x1, a2

	MADD1	t1, t1, x3, a5
	MADD2	t2, t2, x4, a5
	MADD3	t1, t1, x4, a6
	MADD4	t2, t2, x3, a6

	ST	t1,  0 * SIZE(YY)
	ST	t2,  1 * SIZE(YY)
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

	LD	x1, 0 * SIZE(X)
	LD	x2, 1 * SIZE(X)
	daddu	X, X, INCX

	MUL	a1, ALPHA_R, x1
	move	AO1, A
	MUL	a2, ALPHA_I, x1

#ifndef XCONJ
	NMSUB	x1, a1, ALPHA_I, x2
	MADD	x2, a2, ALPHA_R, x2
#else
	MADD	x1, a1, ALPHA_I, x2
	MSUB	x2, a2, ALPHA_R, x2
#endif

	dsra	I,  M, 2

	blez	I, .L25
	move	YY, YORIG

	LD	y1, 0 * SIZE(YY)
	LD	a1, 0 * SIZE(AO1)
	LD	y2, 1 * SIZE(YY)
	LD	a3, 2 * SIZE(AO1)
	LD	y3, 2 * SIZE(YY)
	LD	a2, 1 * SIZE(AO1)
	LD	y4, 3 * SIZE(YY)
	LD	a4, 3 * SIZE(AO1)

	MADD1	t1, y1, x1, a1
	LD	y1,  4 * SIZE(YY)
	MADD2	t2, y2, x2, a1
	LD	a1,  4 * SIZE(AO1)
	MADD1	t3, y3, x1, a3
	LD	y2,  5 * SIZE(YY)
	MADD2	t4, y4, x2, a3
	LD	a3,  6 * SIZE(AO1)

	MADD3	t1, t1, x2, a2
	LD	y3,  6 * SIZE(YY)
	MADD4	t2, t2, x1, a2
	LD	a2,  5 * SIZE(AO1)
	MADD3	t3, t3, x2, a4
	LD	y4,  7 * SIZE(YY)
	MADD4	t4, t4, x1, a4
	daddiu	I, I, -1

	blez	I, .L23
	LD	a4,  7 * SIZE(AO1)
	.align	3

.L22:
	MADD1	t5, y1, x1, a1
	LD	y1,  8 * SIZE(YY)
	MADD2	t6, y2, x2, a1
	LD	a1,  8 * SIZE(AO1)
	MADD1	t7, y3, x1, a3
	LD	y2,  9 * SIZE(YY)
	MADD2	t8, y4, x2, a3
	LD	a3, 10 * SIZE(AO1)

	MADD3	t5, t5, x2, a2
	LD	y3, 10 * SIZE(YY)
	MADD4	t6, t6, x1, a2
	LD	a2,  9 * SIZE(AO1)
	MADD3	t7, t7, x2, a4
	LD	y4, 11 * SIZE(YY)
	MADD4	t8, t8, x1, a4
	LD	a4, 11 * SIZE(AO1)

	ST	t1,  0 * SIZE(YY)
	ST	t2,  1 * SIZE(YY)
	ST	t3,  2 * SIZE(YY)
	ST	t4,  3 * SIZE(YY)

	MADD1	t1, y1, x1, a1
	LD	y1, 12 * SIZE(YY)
	MADD2	t2, y2, x2, a1
	LD	a1, 12 * SIZE(AO1)
	MADD1	t3, y3, x1, a3
	LD	y2, 13 * SIZE(YY)
	MADD2	t4, y4, x2, a3
	LD	a3, 14 * SIZE(AO1)

	MADD3	t1, t1, x2, a2
	LD	y3, 14 * SIZE(YY)
	MADD4	t2, t2, x1, a2
	LD	a2, 13 * SIZE(AO1)
	MADD3	t3, t3, x2, a4
	LD	y4, 15 * SIZE(YY)
	MADD4	t4, t4, x1, a4
	LD	a4, 15 * SIZE(AO1)

	ST	t5,  4 * SIZE(YY)
	ST	t6,  5 * SIZE(YY)
	ST	t7,  6 * SIZE(YY)
	ST	t8,  7 * SIZE(YY)

	daddiu	I, I, -1
	daddiu	YY,  YY,   8 * SIZE

	bgtz	I, .L22
	daddiu	AO1, AO1,  8 * SIZE
	.align 3

.L23:
	ST	t1,  0 * SIZE(YY)
	MADD1	t1, y1, x1, a1
	ST	t2,  1 * SIZE(YY)
	MADD2	t2, y2, x2, a1
	ST	t3,  2 * SIZE(YY)
	MADD1	t3, y3, x1, a3
	ST	t4,  3 * SIZE(YY)
	MADD2	t4, y4, x2, a3

	MADD3	t1, t1, x2, a2
	daddiu	AO1, AO1,  8 * SIZE
	MADD4	t2, t2, x1, a2
	daddiu	YY,  YY,   8 * SIZE
	MADD3	t3, t3, x2, a4
	MADD4	t4, t4, x1, a4

	ST	t1, -4 * SIZE(YY)
	ST	t2, -3 * SIZE(YY)
	ST	t3, -2 * SIZE(YY)
	ST	t4, -1 * SIZE(YY)
	.align 3

.L25:
	andi	I,  M, 2
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

	MADD1	t1, y1, x1, a1
	MADD2	t2, y2, x2, a1
	MADD1	t3, y3, x1, a3
	MADD2	t4, y4, x2, a3

	MADD3	t1, t1, x2, a2
	daddiu	YY,  YY,   4 * SIZE
	MADD4	t2, t2, x1, a2
	daddiu	AO1, AO1,  4 * SIZE
	MADD3	t3, t3, x2, a4
	MADD4	t4, t4, x1, a4

	ST	t1, -4 * SIZE(YY)
	ST	t2, -3 * SIZE(YY)
	ST	t3, -2 * SIZE(YY)
	ST	t4, -1 * SIZE(YY)
	.align 3

.L26:
	andi	I,  M, 1
	NOP
	blez	I, .L900
	NOP

	LD	y1, 0 * SIZE(YY)
	LD	y2, 1 * SIZE(YY)
	LD	a1, 0 * SIZE(AO1)
	LD	a2, 1 * SIZE(AO1)

	MADD1	t1, y1, x1, a1
	MADD2	t2, y2, x2, a1
	MADD3	t1, t1, x2, a2
	MADD4	t2, t2, x1, a2

	ST	t1,  0 * SIZE(YY)
	ST	t2,  1 * SIZE(YY)
	.align 3

.L900:
	li	YORIG, 2 * SIZE

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
	LD	a5, 4 * SIZE(XX)
	LD	a6, 5 * SIZE(XX)
	LD	a7, 6 * SIZE(XX)
	LD	a8, 7 * SIZE(XX)

	daddiu	I, I, -1

	ST	a1, 0 * SIZE(Y)
	ST	a2, 1 * SIZE(Y)
	daddu	Y, Y, INCY
	ST	a3, 0 * SIZE(Y)
	ST	a4, 1 * SIZE(Y)
	daddu	Y, Y, INCY
	ST	a5, 0 * SIZE(Y)
	ST	a6, 1 * SIZE(Y)
	daddu	Y, Y, INCY
	ST	a7, 0 * SIZE(Y)
	ST	a8, 1 * SIZE(Y)
	daddu	Y, Y, INCY

	bgtz	I, .L902
	daddiu	XX, XX, 8 * SIZE
	.align 3

.L905:
	andi	I,  M, 3
	blez	I, .L999
	NOP
	.align 3

.L906:
	LD	a1, 0 * SIZE(XX)
	LD	a2, 1 * SIZE(XX)
	daddiu	XX, XX, 2 * SIZE

	daddiu	I, I, -1

	ST	a1, 0 * SIZE(Y)
	ST	a2, 1 * SIZE(Y)

	bgtz	I, .L906
	daddu	Y, Y, INCY
	.align 3

.L999:
	LDARG	$16,   0($sp)
	LDARG	$17,   8($sp)
	ldc1	$f24, 16($sp)
	ldc1	$f25, 24($sp)

#ifndef __64BIT__
	ldc1	$f20, 32($sp)
	ldc1	$f21, 40($sp)
	ldc1	$f22, 48($sp)
	ldc1	$f23, 56($sp)
#endif

	j	$31
#ifdef __64BIT__
	daddiu	$sp, $sp, 32
#else
	daddiu	$sp, $sp, 64
#endif

	EPILOGUE
