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

#define XORIG	$3
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

#define y1	$f8
#define y2	$f9
#define y3	$f10
#define y4	$f11

#define x1	$f12
#define x2	$f13
#define x3	$f14
#define x4	$f17
#define x5	$f18
#define x6	$f19
#define x7	$f20
#define x8	$f21

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
#ifdef __64BIT__
	daddiu	$sp, $sp, -16
#else
	daddiu	$sp, $sp, -32
#endif

	MTC	$0, y1
	SDARG	$16,   0($sp)

	SDARG	$17,   8($sp)
	dsll	LDA,  LDA,  ZBASE_SHIFT

#ifndef __64BIT__
	sdc1	$f20, 16($sp)
	sdc1	$f21, 24($sp)
#endif

	blez	M, .L999
	dsll	INCX, INCX, ZBASE_SHIFT

	blez	N, .L999
	dsll	INCY, INCY, ZBASE_SHIFT

	li	XORIG, 2 * SIZE

	beq	INCX, XORIG, .L10
	move	XORIG, X

	dsra	I,  M, 2
	move	XORIG, BUFFER

	blez	I, .L05
	move	YY, BUFFER
	.align 3

.L02:
	LD	a1, 0 * SIZE(X)
	LD	a2, 1 * SIZE(X)
	daddu	X, X, INCX
	LD	a3, 0 * SIZE(X)
	LD	a4, 1 * SIZE(X)
	daddu	X, X, INCX
	LD	a5, 0 * SIZE(X)
	LD	a6, 1 * SIZE(X)
	daddu	X, X, INCX
	LD	a7, 0 * SIZE(X)
	LD	a8, 1 * SIZE(X)
	daddu	X, X, INCX

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
	LD	a1, 0 * SIZE(X)
	LD	a2, 1 * SIZE(X)
	daddu	X, X, INCX

	ST	a1, 0 * SIZE(YY)
	ST	a2, 1 * SIZE(YY)
	daddiu	I, I, -1

	bgtz	I, .L06
	daddiu	YY, YY, 2 * SIZE
	.align 3

.L10:
	dsra	J,  N, 1
	blez	J, .L20
	move	YY, Y
	.align 3

.L11:
	move	AO1, A
	MOV	y2, y1
	daddu	AO2, A,   LDA
	MOV	y3, y1
	daddu	A,   AO2, LDA
	MOV	y4, y1

	dsra	I,  M, 2
	blez	I, .L15
	move	XX, XORIG

	LD	x1, 0 * SIZE(XX)
	LD	x2, 1 * SIZE(XX)
	LD	x4, 3 * SIZE(XX)

	LD	a1, 0 * SIZE(AO1)
	LD	a3, 0 * SIZE(AO2)
	LD	a2, 1 * SIZE(AO1)
	LD	a4, 1 * SIZE(AO2)

	LD	a5, 2 * SIZE(AO1)
	LD	a7, 2 * SIZE(AO2)
	LD	a6, 3 * SIZE(AO1)
	LD	a8, 3 * SIZE(AO2)
	daddiu	I, I, -1

	blez	I, .L13
	NOP
	.align	3

.L12:
	MADD1	y1, y1, x1, a1
	LD	x3, 2 * SIZE(XX)
	MADD2	y2, y2, x2, a1
	LD	a1, 4 * SIZE(AO1)
	MADD1	y3, y3, x1, a3
	NOP
	MADD2	y4, y4, x2, a3
	LD	a3, 4 * SIZE(AO2)

	MADD3	y1, y1, x2, a2
	NOP
	MADD4	y2, y2, x1, a2
	LD	a2, 5 * SIZE(AO1)
	MADD3	y3, y3, x2, a4
	LD	x2, 5 * SIZE(XX)
	MADD4	y4, y4, x1, a4
	LD	a4, 5 * SIZE(AO2)

	MADD1	y1, y1, x3, a5
	LD	x1, 4 * SIZE(XX)
	MADD2	y2, y2, x4, a5
	LD	a5, 6 * SIZE(AO1)
	MADD1	y3, y3, x3, a7
	MADD2	y4, y4, x4, a7
	LD	a7, 6 * SIZE(AO2)

	MADD3	y1, y1, x4, a6
	daddiu	I, I, -1
	MADD4	y2, y2, x3, a6
	LD	a6, 7 * SIZE(AO1)
	MADD3	y3, y3, x4, a8
	LD	x4, 7 * SIZE(XX)
	MADD4	y4, y4, x3, a8
	LD	a8, 7 * SIZE(AO2)

	MADD1	y1, y1, x1, a1
	LD	x3, 6 * SIZE(XX)
	MADD2	y2, y2, x2, a1
	LD	a1,  8 * SIZE(AO1)
	MADD1	y3, y3, x1, a3
	MADD2	y4, y4, x2, a3
	LD	a3,  8 * SIZE(AO2)

	MADD3	y1, y1, x2, a2
	MADD4	y2, y2, x1, a2
	LD	a2,  9 * SIZE(AO1)
	MADD3	y3, y3, x2, a4
	LD	x2,  9 * SIZE(XX)
	MADD4	y4, y4, x1, a4
	LD	a4,  9 * SIZE(AO2)

	MADD1	y1, y1, x3, a5
	LD	x1,  8 * SIZE(XX)
	MADD2	y2, y2, x4, a5
	LD	a5, 10 * SIZE(AO1)
	MADD1	y3, y3, x3, a7
	daddiu	XX,  XX,   8 * SIZE
	MADD2	y4, y4, x4, a7
	LD	a7, 10 * SIZE(AO2)

	MADD3	y1, y1, x4, a6
	daddiu	AO2, AO2,  8 * SIZE
	MADD4	y2, y2, x3, a6
	LD	a6, 11 * SIZE(AO1)
	MADD3	y3, y3, x4, a8
	LD	x4,  3 * SIZE(XX)
	MADD4	y4, y4, x3, a8
	LD	a8,  3 * SIZE(AO2)

	bgtz	I, .L12
	daddiu	AO1, AO1,  8 * SIZE
	.align 3

.L13:
	MADD1	y1, y1, x1, a1
	LD	x3,  2 * SIZE(XX)
	MADD2	y2, y2, x2, a1
	LD	a1, 4 * SIZE(AO1)
	MADD1	y3, y3, x1, a3
	NOP
	MADD2	y4, y4, x2, a3
	LD	a3, 4 * SIZE(AO2)

	MADD3	y1, y1, x2, a2
	NOP
	MADD4	y2, y2, x1, a2
	LD	a2, 5 * SIZE(AO1)
	MADD3	y3, y3, x2, a4
	LD	x2, 5 * SIZE(XX)
	MADD4	y4, y4, x1, a4
	LD	a4, 5 * SIZE(AO2)

	MADD1	y1, y1, x3, a5
	LD	x1, 4 * SIZE(XX)
	MADD2	y2, y2, x4, a5
	LD	a5, 6 * SIZE(AO1)
	MADD1	y3, y3, x3, a7
	MADD2	y4, y4, x4, a7
	LD	a7, 6 * SIZE(AO2)

	MADD3	y1, y1, x4, a6
	NOP
	MADD4	y2, y2, x3, a6
	LD	a6, 7 * SIZE(AO1)
	MADD3	y3, y3, x4, a8
	LD	x4, 7 * SIZE(XX)
	MADD4	y4, y4, x3, a8
	LD	a8, 7 * SIZE(AO2)

	MADD1	y1, y1, x1, a1
	LD	x3, 6 * SIZE(XX)
	MADD2	y2, y2, x2, a1
	NOP
	MADD1	y3, y3, x1, a3
	MADD2	y4, y4, x2, a3

	MADD3	y1, y1, x2, a2
	MADD4	y2, y2, x1, a2
	MADD3	y3, y3, x2, a4
	MADD4	y4, y4, x1, a4

	MADD1	y1, y1, x3, a5
	MADD2	y2, y2, x4, a5
	MADD1	y3, y3, x3, a7
	MADD2	y4, y4, x4, a7

	MADD3	y1, y1, x4, a6
	daddiu	XX,  XX,   8 * SIZE
	MADD4	y2, y2, x3, a6
	daddiu	AO1, AO1,  8 * SIZE
	MADD3	y3, y3, x4, a8
	daddiu	AO2, AO2,  8 * SIZE
	MADD4	y4, y4, x3, a8
	NOP
	.align 3

.L15:
	andi	I,  M, 2
	NOP
	blez	I, .L17
	NOP

	LD	x1, 0 * SIZE(XX)
	LD	x2, 1 * SIZE(XX)
	LD	x3, 2 * SIZE(XX)
	LD	x4, 3 * SIZE(XX)

	LD	a1, 0 * SIZE(AO1)
	LD	a3, 0 * SIZE(AO2)
	LD	a2, 1 * SIZE(AO1)
	LD	a4, 1 * SIZE(AO2)

	LD	a5, 2 * SIZE(AO1)
	LD	a7, 2 * SIZE(AO2)
	LD	a6, 3 * SIZE(AO1)
	LD	a8, 3 * SIZE(AO2)

	MADD1	y1, y1, x1, a1
	MADD2	y2, y2, x2, a1
	MADD1	y3, y3, x1, a3
	MADD2	y4, y4, x2, a3

	MADD3	y1, y1, x2, a2
	MADD4	y2, y2, x1, a2
	MADD3	y3, y3, x2, a4
	MADD4	y4, y4, x1, a4

	MADD1	y1, y1, x3, a5
	MADD2	y2, y2, x4, a5
	MADD1	y3, y3, x3, a7
	MADD2	y4, y4, x4, a7

	MADD3	y1, y1, x4, a6
	daddiu	XX,  XX,   4 * SIZE
	MADD4	y2, y2, x3, a6
	daddiu	AO1, AO1,  4 * SIZE
	MADD3	y3, y3, x4, a8
	daddiu	AO2, AO2,  4 * SIZE
	MADD4	y4, y4, x3, a8
	NOP
	.align 3

.L17:
	andi	I,  M, 1
	blez	I, .L19
	.align	3

.L18:
	LD	x1, 0 * SIZE(XX)
	LD	x2, 1 * SIZE(XX)
	LD	a1, 0 * SIZE(AO1)
	LD	a3, 0 * SIZE(AO2)

	MADD1	y1, y1, x1, a1
	LD	a2, 1 * SIZE(AO1)
	MADD2	y2, y2, x2, a1
	LD	a4, 1 * SIZE(AO2)
	MADD1	y3, y3, x1, a3
	MADD2	y4, y4, x2, a3

	MADD3	y1, y1, x2, a2
	MADD4	y2, y2, x1, a2
	MADD3	y3, y3, x2, a4
	MADD4	y4, y4, x1, a4
	.align 3

.L19:
	LD	a1, 0 * SIZE(Y)
	LD	a2, 1 * SIZE(Y)
	daddu	Y, Y, INCY
	LD	a3, 0 * SIZE(Y)
	LD	a4, 1 * SIZE(Y)
	daddu	Y, Y, INCY

	MADD	a1, a1, ALPHA_R, y1
	MADD	a2, a2, ALPHA_I, y1
	MADD	a3, a3, ALPHA_R, y3
	MADD	a4, a4, ALPHA_I, y3

	NMSUB	a1, a1, ALPHA_I, y2
	MADD	a2, a2, ALPHA_R, y2
	NMSUB	a3, a3, ALPHA_I, y4
	MTC	$0, y1
	MADD	a4, a4, ALPHA_R, y4
	daddiu	J, J, -1

	ST	a1,  0 * SIZE(YY)
	ST	a2,  1 * SIZE(YY)
	daddu	YY, YY, INCY
	ST	a3,  0 * SIZE(YY)
	ST	a4,  1 * SIZE(YY)

	bgtz	J, .L11
	daddu	YY, YY, INCY
	.align 3

.L20:
	andi	J,  N, 1
	MOV	y2, y1
	blez	J, .L999
	dsra	I,  M, 2

	MOV	y3, y1
	move	AO1, A
	MOV	y4, y1

	blez	I, .L25
	move	XX, XORIG

	LD	a1, 0 * SIZE(AO1)
	LD	x1, 0 * SIZE(XX)
	LD	a2, 1 * SIZE(AO1)
	LD	x2, 1 * SIZE(XX)
	LD	a5, 2 * SIZE(AO1)
	LD	x4, 3 * SIZE(XX)
	daddiu	I, I, -1

	blez	I, .L23
	LD	a6, 3 * SIZE(AO1)
	.align	3

.L22:
	MADD1	y1, y1, x1, a1
	LD	x3, 2 * SIZE(XX)
	MADD2	y2, y2, x2, a1
	LD	a1, 4 * SIZE(AO1)

	MADD3	y3, y3, x2, a2
	LD	x2, 5 * SIZE(XX)
	MADD4	y4, y4, x1, a2
	LD	a2, 5 * SIZE(AO1)

	MADD1	y1, y1, x3, a5
	LD	x1, 4 * SIZE(XX)
	MADD2	y2, y2, x4, a5
	LD	a5, 6 * SIZE(AO1)

	MADD3	y3, y3, x4, a6
	LD	x4, 7 * SIZE(XX)
	MADD4	y4, y4, x3, a6
	LD	a6, 7 * SIZE(AO1)

	MADD1	y1, y1, x1, a1
	LD	x3, 6 * SIZE(XX)
	MADD2	y2, y2, x2, a1
	LD	a1,  8 * SIZE(AO1)

	MADD3	y3, y3, x2, a2
	LD	x2,  9 * SIZE(XX)
	MADD4	y4, y4, x1, a2
	LD	a2,  9 * SIZE(AO1)

	MADD1	y1, y1, x3, a5
	LD	x1,  8 * SIZE(XX)
	MADD2	y2, y2, x4, a5
	LD	a5, 10 * SIZE(AO1)

	MADD3	y3, y3, x4, a6
	LD	x4, 11 * SIZE(XX)
	MADD4	y4, y4, x3, a6
	LD	a6, 11 * SIZE(AO1)

	daddiu	I, I, -1
	daddiu	XX,  XX,   8 * SIZE

	bgtz	I, .L22
	daddiu	AO1, AO1,  8 * SIZE
	.align 3

.L23:
	MADD1	y1, y1, x1, a1
	LD	x3,  2 * SIZE(XX)
	MADD2	y2, y2, x2, a1
	LD	a1, 4 * SIZE(AO1)

	MADD3	y3, y3, x2, a2
	LD	x2, 5 * SIZE(XX)
	MADD4	y4, y4, x1, a2
	LD	a2, 5 * SIZE(AO1)

	MADD1	y1, y1, x3, a5
	LD	x1, 4 * SIZE(XX)
	MADD2	y2, y2, x4, a5
	LD	a5, 6 * SIZE(AO1)

	MADD3	y3, y3, x4, a6
	LD	x4, 7 * SIZE(XX)
	MADD4	y4, y4, x3, a6
	LD	a6, 7 * SIZE(AO1)

	MADD1	y1, y1, x1, a1
	LD	x3, 6 * SIZE(XX)
	MADD2	y2, y2, x2, a1
	NOP

	MADD3	y3, y3, x2, a2
	MADD4	y4, y4, x1, a2
	MADD1	y1, y1, x3, a5
	MADD2	y2, y2, x4, a5

	MADD3	y3, y3, x4, a6
	daddiu	XX,  XX,   8 * SIZE
	MADD4	y4, y4, x3, a6
	daddiu	AO1, AO1,  8 * SIZE
	NOP
	.align 3

.L25:
	andi	I,  M, 2
	NOP
	blez	I, .L27
	NOP

	LD	a1, 0 * SIZE(AO1)
	LD	x1, 0 * SIZE(XX)
	LD	a2, 1 * SIZE(AO1)
	LD	x2, 1 * SIZE(XX)

	LD	a5, 2 * SIZE(AO1)
	MADD1	y1, y1, x1, a1
	LD	x3, 2 * SIZE(XX)
	MADD2	y2, y2, x2, a1
	LD	a6, 3 * SIZE(AO1)
	MADD3	y3, y3, x2, a2
	LD	x4, 3 * SIZE(XX)
	MADD4	y4, y4, x1, a2

	MADD1	y1, y1, x3, a5
	MADD2	y2, y2, x4, a5

	MADD3	y3, y3, x4, a6
	daddiu	XX,  XX,   4 * SIZE
	MADD4	y4, y4, x3, a6
	daddiu	AO1, AO1,  4 * SIZE
	.align 3

.L27:
	andi	I,  M, 1
	blez	I, .L29
	.align	3

.L28:
	LD	a1, 0 * SIZE(AO1)
	LD	x1, 0 * SIZE(XX)
	LD	a2, 1 * SIZE(AO1)
	LD	x2, 1 * SIZE(XX)

	MADD1	y1, y1, x1, a1
	MADD2	y2, y2, x2, a1

	MADD3	y3, y3, x2, a2
	MADD4	y4, y4, x1, a2
	.align 3

.L29:
	LD	a1, 0 * SIZE(Y)
	LD	a2, 1 * SIZE(Y)

	ADD	y1, y1, y3
	ADD	y2, y2, y4

	MADD	a1, a1, ALPHA_R, y1
	MADD	a2, a2, ALPHA_I, y1
	NMSUB	a1, a1, ALPHA_I, y2
	MADD	a2, a2, ALPHA_R, y2

	ST	a1,  0 * SIZE(YY)
	ST	a2,  1 * SIZE(YY)
	.align 3

.L999:
	LDARG	$16,   0($sp)
	LDARG	$17,   8($sp)

#ifndef __64BIT__
	ldc1	$f20, 16($sp)
	ldc1	$f21, 24($sp)
#endif

	j	$31
#ifdef __64BIT__
	daddiu	$sp, $sp, 16
#else
	daddiu	$sp, $sp, 32
#endif

	EPILOGUE
