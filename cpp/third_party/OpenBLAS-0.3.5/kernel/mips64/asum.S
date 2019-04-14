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

#define I	$2
#define TEMP	$3

#define a1	$f2
#define a2	$f3
#define a3	$f4
#define a4	$f5
#define a5	$f6
#define a6	$f7
#define a7	$f8
#define a8	$f9

#define t1	$f10
#define t2	$f11
#define t3	$f12
#define t4	$f13

#define s1	$f0
#define s2	$f1

	PROLOGUE

#ifdef F_INTERFACE
	LDINT	N,     0(N)
	LDINT	INCX,  0(INCX)
#endif

	MTC	$0,  s1

	MTC	$0,  s2
	dsll	INCX, INCX, BASE_SHIFT

	blez	N, .L999
	li	TEMP, SIZE

	bne	INCX, TEMP, .L20
	dsra	I, N, 3

	blez	I, .L15
	NOP

	LD	a1,  0 * SIZE(X)
	LD	a2,  1 * SIZE(X)
	LD	a3,  2 * SIZE(X)
	LD	a4,  3 * SIZE(X)

	LD	a5,  4 * SIZE(X)
	FABS	t1, a1
	LD	a6,  5 * SIZE(X)
	FABS	t2, a2
	LD	a7,  6 * SIZE(X)
	FABS	t3, a3

	FABS	t4, a4
	daddiu	I, I, -1

	blez	I, .L13
	LD	a8,  7 * SIZE(X)
	.align 3

.L12:
	ADD	s1, s1, t1
	LD	a1,  8 * SIZE(X)

	FABS	t1, a5
	daddiu	I, I, -1

	ADD	s2, s2, t2
	LD	a2,  9 * SIZE(X)

	FABS	t2, a6
	NOP

	ADD	s1, s1, t3
	LD	a3, 10 * SIZE(X)

	FABS	t3, a7
	NOP

	ADD	s2, s2, t4
	LD	a4, 11 * SIZE(X)

	FABS	t4, a8
	daddiu	X, X, 8 * SIZE

	ADD	s1, s1, t1
	LD	a5,  4 * SIZE(X)

	FABS	t1, a1
	NOP

	ADD	s2, s2, t2
	LD	a6,  5 * SIZE(X)

	FABS	t2, a2
	NOP

	ADD	s1, s1, t3
	LD	a7,  6 * SIZE(X)

	FABS	t3, a3
	NOP

	ADD	s2, s2, t4
	LD	a8,  7 * SIZE(X)

	bgtz	I, .L12
	FABS	t4, a4
	.align 3

.L13:
	ADD	s1, s1, t1
	daddiu	X, X, 8 * SIZE

	FABS	t1, a5
	NOP

	ADD	s2, s2, t2
	FABS	t2, a6

	ADD	s1, s1, t3
	FABS	t3, a7

	ADD	s2, s2, t4
	FABS	t4, a8

	ADD	s1, s1, t1
	ADD	s2, s2, t2
	ADD	s1, s1, t3
	ADD	s2, s2, t4
	.align 3

.L15:
	andi	I,  N, 7

	blez	I, .L999
	NOP
	.align	3

.L16:
	LD	a1,  0 * SIZE(X)
	daddiu	I, I, -1

	FABS	t1, a1

	ADD	s1, s1, t1

	bgtz	I, .L16
	daddiu	X, X, SIZE

	j	.L999
	NOP
	.align 3

.L20:
	blez	I, .L25
	NOP

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

	FABS	t1, a1
	LD	a7,  0 * SIZE(X)

	FABS	t2, a2
	daddu	X, X, INCX

	FABS	t3, a3
	LD	a8,  0 * SIZE(X)

	FABS	t4, a4
	daddiu	I, I, -1

	blez	I, .L24
	daddu	X, X, INCX
	.align 3

.L23:
	ADD	s1, s1, t1
	LD	a1,  0 * SIZE(X)

	FABS	t1, a5
	daddu	X, X, INCX

	ADD	s2, s2, t2
	LD	a2,  0 * SIZE(X)

	FABS	t2, a6
	daddu	X, X, INCX

	ADD	s1, s1, t3
	LD	a3,  0 * SIZE(X)

	FABS	t3, a7
	daddu	X, X, INCX

	ADD	s2, s2, t4
	LD	a4,  0 * SIZE(X)

	FABS	t4, a8
	daddu	X, X, INCX

	ADD	s1, s1, t1
	LD	a5,  0 * SIZE(X)

	FABS	t1, a1
	daddu	X, X, INCX

	ADD	s2, s2, t2
	LD	a6,  0 * SIZE(X)

	FABS	t2, a2
	daddu	X, X, INCX

	ADD	s1, s1, t3
	LD	a7,  0 * SIZE(X)

	FABS	t3, a3
	daddu	X, X, INCX

	ADD	s2, s2, t4
	LD	a8,  0 * SIZE(X)

	FABS	t4, a4
	daddiu	I, I, -1

	bgtz	I, .L23
	daddu	X, X, INCX
	.align 3

.L24:
	ADD	s1, s1, t1
	FABS	t1, a5

	ADD	s2, s2, t2
	FABS	t2, a6

	ADD	s1, s1, t3
	FABS	t3, a7

	ADD	s2, s2, t4
	FABS	t4, a8

	ADD	s1, s1, t1
	ADD	s2, s2, t2
	ADD	s1, s1, t3
	ADD	s2, s2, t4
	.align 3

.L25:
	andi	I,  N, 7

	blez	I, .L999
	NOP
	.align	3

.L26:
	LD	a1,  0 * SIZE(X)
	daddiu	I, I, -1

	FABS	t1, a1
	daddu	X, X, INCX

	bgtz	I, .L26
	ADD	s1, s1, t1
	.align 3

.L999:
	j	$31
	ADD	s1, s1, s2

	EPILOGUE
