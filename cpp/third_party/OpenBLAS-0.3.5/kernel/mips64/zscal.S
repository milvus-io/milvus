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

#define I	$2
#define TEMP	$3

#define XX	$5

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

#define t1	$f8
#define t2	$f9
#define t3	$f10
#define t4	$f11

	PROLOGUE

	li	TEMP, 2 * SIZE
	MTC	$0,  a1

	blez	N, .L999
	dsll	INCX, INCX, ZBASE_SHIFT

	CMPEQ	$fcc0, ALPHA_R, a1
	CMPEQ	$fcc1, ALPHA_I, a1

	bc1f	$fcc0, .L50
	NOP

	bc1f	$fcc1, .L50
	NOP

	bne	INCX, TEMP, .L20
	dsra	I, N, 2

	blez	I, .L15
	NOP
	.align 3

.L12:
	ST	a1,  0 * SIZE(X)
	ST	a1,  1 * SIZE(X)
	ST	a1,  2 * SIZE(X)
	ST	a1,  3 * SIZE(X)
	ST	a1,  4 * SIZE(X)
	ST	a1,  5 * SIZE(X)
	ST	a1,  6 * SIZE(X)
	ST	a1,  7 * SIZE(X)
	addiu	I, I, -1

	bgtz	I, .L12
	daddiu	X, X, 8 * SIZE
	.align 3

.L15:
	andi	I,  N, 3

	blez	I, .L999
	NOP
	.align	3

.L16:
	ST	a1,  0 * SIZE(X)
	ST	a1,  1 * SIZE(X)
	daddiu	I, I, -1

	bgtz	I, .L16
	daddiu	X, X, 2 * SIZE

	j	$31
	NOP
	.align 3

.L20:
	dsra	I, N, 2
	blez	I, .L25
	NOP
	.align 3

.L22:
	ST	a1,  0 * SIZE(X)
	ST	a1,  1 * SIZE(X)
	daddu	X, X, INCX
	ST	a1,  0 * SIZE(X)
	ST	a1,  1 * SIZE(X)
	daddu	X, X, INCX

	ST	a1,  0 * SIZE(X)
	ST	a1,  1 * SIZE(X)
	daddu	X, X, INCX
	ST	a1,  0 * SIZE(X)
	ST	a1,  1 * SIZE(X)
	daddiu	I, I, -1

	bgtz	I, .L22
	daddu	X, X, INCX
	.align 3

.L25:
	andi	I,  N, 3

	blez	I, .L999
	NOP
	.align	3

.L26:
	ST	a1,  0 * SIZE(X)
	daddiu	I, I, -1
	ST	a1,  1 * SIZE(X)

	bgtz	I, .L26
	daddu	X, X, INCX

	j	$31
	NOP
	.align 3

.L50:
	bne	INCX, TEMP, .L60
	dsra	I, N, 2

	blez	I, .L55
	daddiu	I, I, -1

	LD	a1,  0 * SIZE(X)
	LD	a2,  1 * SIZE(X)
	LD	a3,  2 * SIZE(X)
	LD	a4,  3 * SIZE(X)
	LD	a5,  4 * SIZE(X)
	LD	a6,  5 * SIZE(X)
	MUL	t1, ALPHA_R, a1
	LD	a7,  6 * SIZE(X)
	MUL	t2, ALPHA_I, a1
	LD	a8,  7 * SIZE(X)
	MUL	t3, ALPHA_R, a3

	blez	I, .L53
	MUL	t4, ALPHA_I, a3
	.align 3

.L52:
	NMSUB	t1, t1, ALPHA_I, a2
	LD	a1,  8 * SIZE(X)
	MADD	t2, t2, ALPHA_R, a2
	LD	a2,  9 * SIZE(X)

	NMSUB	t3, t3, ALPHA_I, a4
	LD	a3, 10 * SIZE(X)
	MADD	t4, t4, ALPHA_R, a4
	LD	a4, 11 * SIZE(X)

	ST	t1,  0 * SIZE(X)
	MUL	t1, ALPHA_R, a5
	ST	t2,  1 * SIZE(X)
	MUL	t2, ALPHA_I, a5

	ST	t3,  2 * SIZE(X)
	MUL	t3, ALPHA_R, a7
	ST	t4,  3 * SIZE(X)
	MUL	t4, ALPHA_I, a7

	NMSUB	t1, t1, ALPHA_I, a6
	LD	a5, 12 * SIZE(X)
	MADD	t2, t2, ALPHA_R, a6
	LD	a6, 13 * SIZE(X)

	NMSUB	t3, t3, ALPHA_I, a8
	LD	a7, 14 * SIZE(X)
	MADD	t4, t4, ALPHA_R, a8
	LD	a8, 15 * SIZE(X)

	ST	t1,  4 * SIZE(X)
	MUL	t1, ALPHA_R, a1
	ST	t2,  5 * SIZE(X)
	MUL	t2, ALPHA_I, a1
	ST	t3,  6 * SIZE(X)
	MUL	t3, ALPHA_R, a3
	ST	t4,  7 * SIZE(X)
	MUL	t4, ALPHA_I, a3

	daddiu	I, I, -1

	bgtz	I, .L52
	daddiu	X, X, 8 * SIZE
	.align 3

.L53:
	NMSUB	t1, t1, ALPHA_I, a2
	MADD	t2, t2, ALPHA_R, a2
	NMSUB	t3, t3, ALPHA_I, a4
	MADD	t4, t4, ALPHA_R, a4

	ST	t1,  0 * SIZE(X)
	MUL	t1, ALPHA_R, a5
	ST	t2,  1 * SIZE(X)
	MUL	t2, ALPHA_I, a5
	ST	t3,  2 * SIZE(X)
	MUL	t3, ALPHA_R, a7
	ST	t4,  3 * SIZE(X)
	MUL	t4, ALPHA_I, a7

	NMSUB	t1, t1, ALPHA_I, a6
	MADD	t2, t2, ALPHA_R, a6
	NMSUB	t3, t3, ALPHA_I, a8
	MADD	t4, t4, ALPHA_R, a8

	ST	t1,  4 * SIZE(X)
	ST	t2,  5 * SIZE(X)
	ST	t3,  6 * SIZE(X)
	ST	t4,  7 * SIZE(X)

	daddiu	X, X, 8 * SIZE
	.align 3

.L55:
	andi	I,  N, 3

	blez	I, .L999
	NOP
	.align	3

.L56:
	LD	a1,  0 * SIZE(X)
	LD	a2,  1 * SIZE(X)

	MUL	t1, ALPHA_R, a1
	MUL	t2, ALPHA_I, a1

	NMSUB	t1, t1, ALPHA_I, a2
	MADD	t2, t2, ALPHA_R, a2

	daddiu	X, X, 2 * SIZE
	daddiu	I, I, -1

	ST	t1, -2 * SIZE(X)
	bgtz	I, .L56
	ST	t2, -1 * SIZE(X)

	j	$31
	NOP
	.align 3

.L60:
	dsra	I, N, 2
	move	XX, X

	blez	I, .L65
	daddiu	I, I, -1

	LD	a1,  0 * SIZE(X)
	LD	a2,  1 * SIZE(X)
	daddu	X, X, INCX
	LD	a3,  0 * SIZE(X)
	LD	a4,  1 * SIZE(X)
	daddu	X, X, INCX
	LD	a5,  0 * SIZE(X)
	LD	a6,  1 * SIZE(X)
	daddu	X, X, INCX

	MUL	t1, ALPHA_R, a1
	LD	a7,  0 * SIZE(X)
	MUL	t2, ALPHA_I, a1
	LD	a8,  1 * SIZE(X)
	MUL	t3, ALPHA_R, a3
	daddu	X, X, INCX

	blez	I, .L63
	MUL	t4, ALPHA_I, a3
	.align 3

.L62:
	NMSUB	t1, t1, ALPHA_I, a2
	LD	a1,  0 * SIZE(X)
	MADD	t2, t2, ALPHA_R, a2
	LD	a2,  1 * SIZE(X)
	daddu	X, X, INCX

	NMSUB	t3, t3, ALPHA_I, a4
	LD	a3,  0 * SIZE(X)
	MADD	t4, t4, ALPHA_R, a4
	LD	a4,  1 * SIZE(X)
	daddu	X, X, INCX

	ST	t1,  0 * SIZE(XX)
	MUL	t1, ALPHA_R, a5

	ST	t2,  1 * SIZE(XX)
	MUL	t2, ALPHA_I, a5
	daddu	XX, XX, INCX

	ST	t3,  0 * SIZE(XX)
	MUL	t3, ALPHA_R, a7

	ST	t4,  1 * SIZE(XX)
	MUL	t4, ALPHA_I, a7
	daddu	XX, XX, INCX


	NMSUB	t1, t1, ALPHA_I, a6
	LD	a5,  0 * SIZE(X)
	MADD	t2, t2, ALPHA_R, a6
	LD	a6,  1 * SIZE(X)
	daddu	X, X, INCX

	NMSUB	t3, t3, ALPHA_I, a8
	LD	a7,  0 * SIZE(X)
	MADD	t4, t4, ALPHA_R, a8
	LD	a8,  1 * SIZE(X)
	daddu	X, X, INCX

	ST	t1,  0 * SIZE(XX)
	MUL	t1, ALPHA_R, a1
	ST	t2,  1 * SIZE(XX)
	MUL	t2, ALPHA_I, a1
	daddu	XX, XX, INCX

	ST	t3,  0 * SIZE(XX)
	MUL	t3, ALPHA_R, a3
	ST	t4,  1 * SIZE(XX)
	MUL	t4, ALPHA_I, a3

	daddiu	I, I, -1

	bgtz	I, .L62
	daddu	XX, XX, INCX
	.align 3

.L63:
	NMSUB	t1, t1, ALPHA_I, a2
	MADD	t2, t2, ALPHA_R, a2
	NMSUB	t3, t3, ALPHA_I, a4
	MADD	t4, t4, ALPHA_R, a4

	ST	t1,  0 * SIZE(XX)
	MUL	t1, ALPHA_R, a5
	ST	t2,  1 * SIZE(XX)
	MUL	t2, ALPHA_I, a5
	daddu	XX, XX, INCX

	ST	t3,  0 * SIZE(XX)
	MUL	t3, ALPHA_R, a7
	ST	t4,  1 * SIZE(XX)
	MUL	t4, ALPHA_I, a7
	daddu	XX, XX, INCX

	NMSUB	t1, t1, ALPHA_I, a6
	MADD	t2, t2, ALPHA_R, a6
	NMSUB	t3, t3, ALPHA_I, a8
	MADD	t4, t4, ALPHA_R, a8

	ST	t1,  0 * SIZE(XX)
	ST	t2,  1 * SIZE(XX)
	daddu	XX, XX, INCX
	ST	t3,  0 * SIZE(XX)
	ST	t4,  1 * SIZE(XX)
	daddu	XX, XX, INCX
	.align 3

.L65:
	andi	I,  N, 3

	blez	I, .L999
	NOP
	.align	3

.L66:
	LD	a1,  0 * SIZE(X)
	LD	a2,  1 * SIZE(X)

	MUL	t1, ALPHA_R, a1
	MUL	t2, ALPHA_I, a1

	NMSUB	t1, t1, ALPHA_I, a2
	MADD	t2, t2, ALPHA_R, a2
	daddiu	I, I, -1

	ST	t1,  0 * SIZE(X)
	ST	t2,  1 * SIZE(X)

	bgtz	I, .L66
	daddu	X, X, INCX
	.align 3

.L999:
	j	$31
	NOP

	EPILOGUE
