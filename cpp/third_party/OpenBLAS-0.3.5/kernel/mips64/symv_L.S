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
#define A	$6
#define LDA	$7
#define X	$8
#define INCX	$9
#define Y	$10
#define INCY	$11
#define BUFFER	$5

#define XX	$12
#define YY	$13

#define I	$14
#define IS	$15

#define AO1	$16
#define AO2	$17

#define Y1	$18
#define TEMP	$19

#define II	INCX

#define ALPHA	$f13

#define a1	$f0
#define a2	$f1
#define a3	$f2
#define a4	$f3
#define a5	$f4
#define a6	$f5
#define a7	$f6
#define a8	$f7

#define alpha1	$f8
#define alpha2	$f9

#define x1	$f10
#define x2	$f11
#define x3	$f12
#define x4	$f14

#define xsum1	$f15
#define xsum2	$f16

#define ysum1	$f17
#define ysum2	$f18
#define ysum3	$f19
#define ysum4	$f20


	PROLOGUE

	LDARG	BUFFER,  0($sp)
	daddiu	$sp, $sp, -32

	SDARG	$16,   0($sp)
	dsll	LDA,  LDA,  BASE_SHIFT
	SDARG	$17,   8($sp)
	dsll	INCX, INCX, BASE_SHIFT
	SDARG	$18,  16($sp)
	dsll	INCY, INCY, BASE_SHIFT
	SDARG	$19,  24($sp)
	nop

	blez	M, .L999
	li	IS, SIZE

	beq	IS, INCX, .L05
	move	Y1, Y

	dsra	I,  M, 2
	move	XX, X

	blez	I, .L02
	move	X,  BUFFER
	.align 3

.L01:
	LD	a1, 0 * SIZE(XX)
	daddu	XX, XX, INCX
	LD	a2, 0 * SIZE(XX)
	daddu	XX, XX, INCX
	LD	a3, 0 * SIZE(XX)
	daddu	XX, XX, INCX
	LD	a4, 0 * SIZE(XX)
	daddu	XX, XX, INCX

	ST	a1, 0 * SIZE(BUFFER)
	ST	a2, 1 * SIZE(BUFFER)
	ST	a3, 2 * SIZE(BUFFER)
	ST	a4, 3 * SIZE(BUFFER)
	daddiu	I, I, -1

	bgtz	I, .L01
	daddiu	BUFFER, BUFFER, 4 * SIZE
	.align 3

.L02:
	andi	I,  M, 3
	blez	I, .L05
	NOP
	.align 3

.L03:
	LD	a1, 0 * SIZE(XX)
	daddu	XX, XX, INCX

	ST	a1, 0 * SIZE(BUFFER)
	daddiu	I, I, -1

	bgtz	I, .L03
	daddiu	BUFFER, BUFFER, 1 * SIZE
	.align 3

.L05:
	beq	IS, INCY, .L10
	daddiu	BUFFER, BUFFER, 255

	li	TEMP, -256
	and	BUFFER, BUFFER, TEMP

	dsra	I,  M, 2
	move	Y1, BUFFER

	blez	I, .L07
	move	YY, Y
	.align 3

.L06:
	LD	a1, 0 * SIZE(YY)
	daddu	YY, YY, INCY
	LD	a2, 0 * SIZE(YY)
	daddu	YY, YY, INCY
	LD	a3, 0 * SIZE(YY)
	daddu	YY, YY, INCY
	LD	a4, 0 * SIZE(YY)
	daddu	YY, YY, INCY

	ST	a1, 0 * SIZE(BUFFER)
	ST	a2, 1 * SIZE(BUFFER)
	ST	a3, 2 * SIZE(BUFFER)
	ST	a4, 3 * SIZE(BUFFER)
	daddiu	I, I, -1

	bgtz	I, .L06
	daddiu	BUFFER, BUFFER, 4 * SIZE
	.align 3

.L07:
	andi	I,  M, 3
	blez	I, .L10
	NOP
	.align 3

.L08:
	LD	a1, 0 * SIZE(YY)
	daddu	YY, YY, INCY

	ST	a1, 0 * SIZE(BUFFER)
	daddiu	I, I, -1

	bgtz	I, .L08
	daddiu	BUFFER, BUFFER, 1 * SIZE
	.align 3

.L10:
	slti	TEMP, M, 2
	nop

	bgtz	TEMP, .L20
	li	IS, 0
	.align 3

.L11:
	dsll	TEMP,  IS,  BASE_SHIFT
	nop

	daddu	XX, X,  TEMP
	daddu	YY, Y1, TEMP

	LD	alpha1, 0 * SIZE(XX)
	move	AO1, A
	LD	alpha2, 1 * SIZE(XX)
	daddiu	XX, XX, 2 * SIZE

	LD	a1, 0 * SIZE(AO1)
	daddu	AO2, A,   LDA
	LD	a2, 1 * SIZE(AO1)
	daddiu	AO1, AO1,  2 * SIZE

	LD	a3, 0 * SIZE(AO2)
	daddu	A,   AO2, LDA
	LD	a4, 1 * SIZE(AO2)
	daddiu	AO2, AO2,  2 * SIZE

	MUL	xsum1, alpha1, a1
	daddiu	A, A, 2 * SIZE
	MUL	xsum2, alpha1, a2
	dsubu	II, M, IS

	MADD	xsum1, xsum1, alpha2, a2
	MADD	xsum2, xsum2, alpha2, a4
	daddiu	II, II, - 2

	MUL	alpha1, ALPHA, alpha1
	daddiu	YY, YY, 2 * SIZE
	MUL	alpha2, ALPHA, alpha2
	dsra	I,  II, 3

	blez	I, .L15
	daddiu	I, I, -1

	LD	x1, 0 * SIZE(XX)
	LD	x2, 1 * SIZE(XX)
	LD	x3, 2 * SIZE(XX)

	LD	a1, 0 * SIZE(AO1)
	LD	a2, 1 * SIZE(AO1)
	LD	a5, 2 * SIZE(AO1)
	LD	a6, 3 * SIZE(AO1)

	LD	a3, 0 * SIZE(AO2)
	LD	a4, 1 * SIZE(AO2)
	LD	a7, 2 * SIZE(AO2)
	LD	a8, 3 * SIZE(AO2)

	LD	ysum1, 0 * SIZE(YY)
	LD	ysum2, 1 * SIZE(YY)

	blez	I, .L13
	LD	ysum3, 2 * SIZE(YY)
	.align	3

.L12:
	MADD	ysum1, ysum1, alpha1, a1
	LD	ysum4, 3 * SIZE(YY)
	MADD	ysum2, ysum2, alpha1, a2
	LD	x4, 3 * SIZE(XX)
	MADD	xsum1, xsum1, x1, a1
	LD	a1, 4 * SIZE(AO1)
	MADD	xsum2, xsum2, x1, a3
	LD	x1, 4 * SIZE(XX)

	MADD	ysum1, ysum1, alpha2, a3
	LD	a3, 4 * SIZE(AO2)
	MADD	ysum2, ysum2, alpha2, a4
	daddiu	I, I, -1
	MADD	xsum1, xsum1, x2, a2
	LD	a2, 5 * SIZE(AO1)
	MADD	xsum2, xsum2, x2, a4
	LD	a4, 5 * SIZE(AO2)

	ST	ysum1, 0 * SIZE(YY)
	LD	ysum1, 4 * SIZE(YY)
	ST	ysum2, 1 * SIZE(YY)
	LD	ysum2, 5 * SIZE(YY)

	MADD	ysum3, ysum3, alpha1, a5
	nop
	MADD	ysum4, ysum4, alpha1, a6
	LD	x2, 5 * SIZE(XX)
	MADD	xsum1, xsum1, x3, a5
	LD	a5, 6 * SIZE(AO1)
	MADD	xsum2, xsum2, x3, a7
	LD	x3, 6 * SIZE(XX)

	MADD	ysum3, ysum3, alpha2, a7
	LD	a7, 6 * SIZE(AO2)
	MADD	ysum4, ysum4, alpha2, a8
	daddiu	XX, XX,  8 * SIZE
	MADD	xsum1, xsum1, x4, a6
	LD	a6, 7 * SIZE(AO1)
	MADD	xsum2, xsum2, x4, a8
	LD	a8, 7 * SIZE(AO2)

	ST	ysum3, 2 * SIZE(YY)
	LD	ysum3, 6 * SIZE(YY)
	ST	ysum4, 3 * SIZE(YY)
	LD	ysum4, 7 * SIZE(YY)

	MADD	ysum1, ysum1, alpha1, a1
	daddiu	AO2, AO2,  8 * SIZE
	MADD	ysum2, ysum2, alpha1, a2
	LD	x4,-1 * SIZE(XX)
	MADD	xsum1, xsum1, x1, a1
	LD	a1, 8 * SIZE(AO1)
	MADD	xsum2, xsum2, x1, a3
	LD	x1, 0 * SIZE(XX)

	MADD	ysum1, ysum1, alpha2, a3
	LD	a3, 0 * SIZE(AO2)
	MADD	ysum2, ysum2, alpha2, a4
	nop
	MADD	xsum1, xsum1, x2, a2
	LD	a2, 9 * SIZE(AO1)
	MADD	xsum2, xsum2, x2, a4
	LD	a4, 1 * SIZE(AO2)

	ST	ysum1, 4 * SIZE(YY)
	LD	ysum1, 8 * SIZE(YY)
	ST	ysum2, 5 * SIZE(YY)
	LD	ysum2, 9 * SIZE(YY)

	MADD	ysum3, ysum3, alpha1, a5
	daddiu	AO1, AO1,  8 * SIZE
	MADD	ysum4, ysum4, alpha1, a6
	LD	x2, 1 * SIZE(XX)
	MADD	xsum1, xsum1, x3, a5
	LD	a5, 2 * SIZE(AO1)
	MADD	xsum2, xsum2, x3, a7
	LD	x3, 2 * SIZE(XX)

	MADD	ysum3, ysum3, alpha2, a7
	LD	a7, 2 * SIZE(AO2)
	MADD	ysum4, ysum4, alpha2, a8
	daddiu	YY, YY,  8 * SIZE
	MADD	xsum1, xsum1, x4, a6
	LD	a6, 3 * SIZE(AO1)
	MADD	xsum2, xsum2, x4, a8
	LD	a8, 3 * SIZE(AO2)

	ST	ysum3,-2 * SIZE(YY)
	LD	ysum3, 2 * SIZE(YY)
	bgtz	I, .L12
	ST	ysum4,-1 * SIZE(YY)
	.align 3

.L13:
	MADD	ysum1, ysum1, alpha1, a1
	LD	ysum4, 3 * SIZE(YY)
	MADD	ysum2, ysum2, alpha1, a2
	LD	x4, 3 * SIZE(XX)
	MADD	xsum1, xsum1, x1, a1
	LD	a1, 4 * SIZE(AO1)
	MADD	xsum2, xsum2, x1, a3
	LD	x1, 4 * SIZE(XX)

	MADD	ysum1, ysum1, alpha2, a3
	LD	a3, 4 * SIZE(AO2)
	MADD	ysum2, ysum2, alpha2, a4
	MADD	xsum1, xsum1, x2, a2
	LD	a2, 5 * SIZE(AO1)
	MADD	xsum2, xsum2, x2, a4
	LD	a4, 5 * SIZE(AO2)
	LD	x2, 5 * SIZE(XX)

	ST	ysum1, 0 * SIZE(YY)
	ST	ysum2, 1 * SIZE(YY)
	LD	ysum1, 4 * SIZE(YY)
	LD	ysum2, 5 * SIZE(YY)

	MADD	ysum3, ysum3, alpha1, a5
	MADD	ysum4, ysum4, alpha1, a6
	MADD	xsum1, xsum1, x3, a5
	LD	a5, 6 * SIZE(AO1)
	MADD	xsum2, xsum2, x3, a7
	LD	x3, 6 * SIZE(XX)

	MADD	ysum3, ysum3, alpha2, a7
	LD	a7, 6 * SIZE(AO2)
	MADD	ysum4, ysum4, alpha2, a8
	MADD	xsum1, xsum1, x4, a6
	LD	a6, 7 * SIZE(AO1)
	MADD	xsum2, xsum2, x4, a8
	LD	a8, 7 * SIZE(AO2)
	LD	x4, 7 * SIZE(XX)

	ST	ysum3, 2 * SIZE(YY)
	ST	ysum4, 3 * SIZE(YY)
	LD	ysum3, 6 * SIZE(YY)
	LD	ysum4, 7 * SIZE(YY)

	MADD	ysum1, ysum1, alpha1, a1
	MADD	ysum2, ysum2, alpha1, a2
	MADD	xsum1, xsum1, x1, a1
	MADD	xsum2, xsum2, x1, a3

	MADD	ysum1, ysum1, alpha2, a3
	MADD	ysum2, ysum2, alpha2, a4
	MADD	xsum1, xsum1, x2, a2
	MADD	xsum2, xsum2, x2, a4

	MADD	ysum3, ysum3, alpha1, a5
	MADD	ysum4, ysum4, alpha1, a6
	MADD	xsum1, xsum1, x3, a5
	MADD	xsum2, xsum2, x3, a7

	MADD	ysum3, ysum3, alpha2, a7
	daddiu	XX, XX,  8 * SIZE
	MADD	ysum4, ysum4, alpha2, a8
	daddiu	AO1, AO1,  8 * SIZE
	MADD	xsum1, xsum1, x4, a6
	daddiu	AO2, AO2,  8 * SIZE
	MADD	xsum2, xsum2, x4, a8

	ST	ysum1, 4 * SIZE(YY)
	ST	ysum2, 5 * SIZE(YY)
	ST	ysum3, 6 * SIZE(YY)
	ST	ysum4, 7 * SIZE(YY)
	daddiu	YY, YY,  8 * SIZE
	.align 3

.L15:
	andi	I,  II, 4
	NOP
	blez	I, .L16
	NOP

	LD	x1, 0 * SIZE(XX)
	LD	x2, 1 * SIZE(XX)
	LD	x3, 2 * SIZE(XX)
	LD	x4, 3 * SIZE(XX)
	daddiu	XX, XX,  4 * SIZE

	LD	a1, 0 * SIZE(AO1)
	LD	a2, 1 * SIZE(AO1)
	LD	a5, 2 * SIZE(AO1)
	LD	a6, 3 * SIZE(AO1)
	daddiu	AO1, AO1,  4 * SIZE

	LD	a3, 0 * SIZE(AO2)
	LD	a4, 1 * SIZE(AO2)
	LD	a7, 2 * SIZE(AO2)
	LD	a8, 3 * SIZE(AO2)
	daddiu	AO2, AO2,  4 * SIZE

	LD	ysum1, 0 * SIZE(YY)
	LD	ysum2, 1 * SIZE(YY)
	LD	ysum3, 2 * SIZE(YY)
	LD	ysum4, 3 * SIZE(YY)

	MADD	ysum1, ysum1, alpha1, a1
	MADD	ysum2, ysum2, alpha1, a2
	MADD	xsum1, xsum1, x1, a1
	MADD	xsum2, xsum2, x1, a3

	MADD	ysum1, ysum1, alpha2, a3
	MADD	ysum2, ysum2, alpha2, a4
	MADD	xsum1, xsum1, x2, a2
	MADD	xsum2, xsum2, x2, a4

	MADD	ysum3, ysum3, alpha1, a5
	MADD	ysum4, ysum4, alpha1, a6
	MADD	xsum1, xsum1, x3, a5
	MADD	xsum2, xsum2, x3, a7

	MADD	ysum3, ysum3, alpha2, a7
	MADD	ysum4, ysum4, alpha2, a8
	MADD	xsum1, xsum1, x4, a6
	MADD	xsum2, xsum2, x4, a8

	ST	ysum1, 0 * SIZE(YY)
	ST	ysum2, 1 * SIZE(YY)
	ST	ysum3, 2 * SIZE(YY)
	ST	ysum4, 3 * SIZE(YY)

	daddiu	YY, YY,  4 * SIZE
	.align 3

.L16:
	andi	I,  II, 2
	NOP
	blez	I, .L17
	NOP

	LD	x1, 0 * SIZE(XX)
	LD	x2, 1 * SIZE(XX)
	daddiu	XX, XX,  2 * SIZE

	LD	a1, 0 * SIZE(AO1)
	LD	a2, 1 * SIZE(AO1)
	daddiu	AO1, AO1,  2 * SIZE

	LD	a3, 0 * SIZE(AO2)
	LD	a4, 1 * SIZE(AO2)
	daddiu	AO2, AO2,  2 * SIZE

	LD	ysum1, 0 * SIZE(YY)
	LD	ysum2, 1 * SIZE(YY)

	MADD	ysum1, ysum1, alpha1, a1
	MADD	ysum2, ysum2, alpha1, a2
	MADD	xsum1, xsum1, x1, a1
	MADD	xsum2, xsum2, x1, a3

	MADD	ysum1, ysum1, alpha2, a3
	MADD	ysum2, ysum2, alpha2, a4
	MADD	xsum1, xsum1, x2, a2
	MADD	xsum2, xsum2, x2, a4

	ST	ysum1, 0 * SIZE(YY)
	ST	ysum2, 1 * SIZE(YY)
	daddiu	YY, YY,  2 * SIZE
	.align 3

.L17:
	andi	I,  M, 1
	NOP
	blez	I, .L19
	NOP

	LD	x1, 0 * SIZE(XX)
	daddiu	XX, XX,  1 * SIZE
	LD	a1, 0 * SIZE(AO1)
	daddiu	AO1, AO1,  1 * SIZE

	LD	a3, 0 * SIZE(AO2)
	daddiu	AO2, AO2,  1 * SIZE
	LD	ysum1, 0 * SIZE(YY)

	MADD	ysum1, ysum1, alpha1, a1
	MADD	xsum1, xsum1, x1, a1
	MADD	ysum1, ysum1, alpha2, a3
	MADD	xsum2, xsum2, x1, a3

	ST	ysum1, 0 * SIZE(YY)
	.align 3

.L19:
	dsll	TEMP,  IS,  BASE_SHIFT
	daddu	TEMP, Y1, TEMP

	LD	ysum1, 0 * SIZE(TEMP)
	LD	ysum2, 1 * SIZE(TEMP)

	MADD	ysum1, ysum1, ALPHA, xsum1
	MADD	ysum2, ysum2, ALPHA, xsum2

	ST	ysum1, 0 * SIZE(TEMP)
	ST	ysum2, 1 * SIZE(TEMP)

	daddiu	TEMP, IS, 4
	slt	TEMP, M, TEMP

	beqz	TEMP, .L11
	daddiu	IS, IS, 2
	.align 3

.L20:
	andi	I, M, 1
	dsll	TEMP,  IS,  BASE_SHIFT
	blez	I, .L900
	daddu	XX, X,  TEMP

	daddu	YY, Y1, TEMP

	LD	x1,    0 * SIZE(XX)
	LD	ysum1, 0 * SIZE(YY)
	LD	a1,    0 * SIZE(A)

	MUL	xsum1, a1, x1

	MADD	ysum1, ysum1, ALPHA, xsum1

	ST	ysum1, 0 * SIZE(YY)
	.align 3

.L900:
	li	IS, SIZE

	beq	INCY, IS, .L999
	NOP

	dsra	I,  M, 2
	blez	I, .L905
	NOP
	.align 3

.L902:
	LD	a1, 0 * SIZE(Y1)
	LD	a2, 1 * SIZE(Y1)
	LD	a3, 2 * SIZE(Y1)
	LD	a4, 3 * SIZE(Y1)

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
	daddiu	Y1, Y1, 4 * SIZE
	.align 3

.L905:
	andi	I,  M, 3
	blez	I, .L999
	NOP
	.align 3

.L906:
	LD	a1, 0 * SIZE(Y1)
	daddiu	Y1, Y1, 1 * SIZE

	ST	a1, 0 * SIZE(Y)
	daddiu	I, I, -1

	bgtz	I, .L906
	daddu	Y, Y, INCY
	.align 3

.L999:
	LDARG	$16,   0($sp)
	LDARG	$17,   8($sp)
	LDARG	$18,  16($sp)
	LDARG	$19,  24($sp)

	j	$31
	daddiu	$sp, $sp, 32

	EPILOGUE
