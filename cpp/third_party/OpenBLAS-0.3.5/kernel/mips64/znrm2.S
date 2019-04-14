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
#define XX	$7

#define I	$2
#define TEMP	$3

#define a1	$f4
#define a2	$f5
#define a3	$f6
#define a4	$f7
#define a5	$f8
#define a6	$f9
#define a7	$f10
#define a8	$f11

#define t1	$f12
#define t2	$f13
#define t3	$f14
#define t4	$f15

#define s1	$f0
#define s2	$f1
#define s3	$f2
#define s4	$f3

#define ALPHA	$f16
#define max	$f17


	PROLOGUE

#ifdef F_INTERFACE
	LDINT	N,     0(N)
	LDINT	INCX,  0(INCX)
#endif

	blez	N, .L999
	MTC	$0,  s1

	blez	INCX, .L999
	dsll	INCX, INCX, ZBASE_SHIFT

	move	XX, X
	MOV	s2, s1

	dsra	I, N, 2
	MOV	s3, s1

	blez	I, .L15
	MOV	s4, s1

	LD	a1,  0 * SIZE(X)
	LD	a2,  1 * SIZE(X)
	daddu	X, X, INCX
	LD	a3,  0 * SIZE(X)
	LD	a4,  1 * SIZE(X)
	daddu	X, X, INCX
	LD	a5,  0 * SIZE(X)
	LD	a6,  1 * SIZE(X)
	daddu	X, X, INCX
	LD	a7,  0 * SIZE(X)
	LD	a8,  1 * SIZE(X)
	daddiu	I, I, -1

	blez	I, .L13
	daddu	X, X, INCX
	.align 3

.L12:
	FABS	t1, a1
	LD	a1,  0 * SIZE(X)
	FABS	t2, a2
	NOP

	FABS	t3, a3
	LD	a2,  1 * SIZE(X)
	FABS	t4, a4
	daddu	X, X, INCX

	CMPLT	$fcc0, s1, t1
	LD	a3,  0 * SIZE(X)
	CMPLT	$fcc1, s2, t2
	NOP

	CMPLT	$fcc2, s3, t3
	LD	a4,  1 * SIZE(X)
	CMPLT	$fcc3, s4, t4
	daddu	X, X, INCX

	CMOVT	s1, t1, $fcc0
	CMOVT	s2, t2, $fcc1
	CMOVT	s3, t3, $fcc2
	CMOVT	s4, t4, $fcc3

	FABS	t1, a5
	LD	a5,  0 * SIZE(X)
	FABS	t2, a6
	NOP

	FABS	t3, a7
	LD	a6,  1 * SIZE(X)
	FABS	t4, a8
	daddu	X, X, INCX

	CMPLT	$fcc0, s1, t1
	LD	a7,  0 * SIZE(X)
	CMPLT	$fcc1, s2, t2
	NOP

	CMPLT	$fcc2, s3, t3
	LD	a8,  1 * SIZE(X)
	CMPLT	$fcc3, s4, t4
	daddu	X, X, INCX

	CMOVT	s1, t1, $fcc0
	daddiu	I, I, -1

	CMOVT	s2, t2, $fcc1
	CMOVT	s3, t3, $fcc2

	bgtz	I, .L12
	CMOVT	s4, t4, $fcc3
	.align 3

.L13:
	FABS	t1, a1
	FABS	t2, a2
	FABS	t3, a3
	FABS	t4, a4

	CMPLT	$fcc0, s1, t1
	CMPLT	$fcc1, s2, t2
	CMPLT	$fcc2, s3, t3
	CMPLT	$fcc3, s4, t4

	CMOVT	s1, t1, $fcc0
	CMOVT	s2, t2, $fcc1
	CMOVT	s3, t3, $fcc2
	CMOVT	s4, t4, $fcc3

	FABS	t1, a5
	FABS	t2, a6
	FABS	t3, a7
	FABS	t4, a8

	CMPLT	$fcc0, s1, t1
	CMPLT	$fcc1, s2, t2
	CMPLT	$fcc2, s3, t3
	CMPLT	$fcc3, s4, t4

	CMOVT	s1, t1, $fcc0
	CMOVT	s2, t2, $fcc1
	CMOVT	s3, t3, $fcc2
	CMOVT	s4, t4, $fcc3
	.align 3

.L15:
	andi	I,  N, 3

	blez	I, .L100
	NOP
	.align	3

.L16:
	LD	a1,  0 * SIZE(X)
	LD	a2,  1 * SIZE(X)
	daddiu	I, I, -1

	FABS	t1, a1
	FABS	t2, a2

	CMPLT	$fcc0, s1, t1
	CMPLT	$fcc1, s2, t2

	CMOVT	s1, t1, $fcc0
	CMOVT	s2, t2, $fcc1

	bgtz	I, .L16
	daddu	X, X, INCX
	.align 3

.L100:
	CMPLT	$fcc0, s1, s2
	CMPLT	$fcc1, s3, s4

	CMOVT	s1, s2, $fcc0
	CMOVT	s3, s4, $fcc1

	CMPLT	$fcc0, s1, s3
	CMOVT	s1, s3, $fcc0

	lui	TEMP, 0x3f80
	dmtc1	$0,   a1

	mtc1	TEMP, ALPHA
	CMPEQ	$fcc0, s1, a1

	bc1t	$fcc0, .L999
	cvt.d.s	ALPHA, ALPHA

	div.d	ALPHA, ALPHA, s1
	MOV	max, s1

	MOV	s1, a1
	MOV	s2, a1
	MOV	s3, a1
	MOV	s4, a1

	dsra	I, N, 2
	blez	I, .L105
	NOP

	LD	a1,  0 * SIZE(XX)
	LD	a2,  1 * SIZE(XX)
	daddu	XX, XX, INCX

	LD	a3,  0 * SIZE(XX)
	LD	a4,  1 * SIZE(XX)
	daddu	XX, XX, INCX

	LD	a5,  0 * SIZE(XX)
	LD	a6,  1 * SIZE(XX)
	daddu	XX, XX, INCX

	LD	a7,  0 * SIZE(XX)
	LD	a8,  1 * SIZE(XX)
	daddiu	I, I, -1

	blez	I, .L104
	daddu	XX, XX, INCX
	.align 3

.L103:
	MUL	t1, ALPHA, a1
	LD	a1,  0 * SIZE(XX)
	MUL	t2, ALPHA, a2
	daddiu	I, I, -1

	MUL	t3, ALPHA, a3
	LD	a2,  1 * SIZE(XX)
	MUL	t4, ALPHA, a4
	daddu	XX, XX, INCX

	MADD	s1, s1, t1, t1
	LD	a3,  0 * SIZE(XX)
	MADD	s2, s2, t2, t2
	NOP

	MADD	s3, s3, t3, t3
	LD	a4,  1 * SIZE(XX)
	MADD	s4, s4, t4, t4
	daddu	XX, XX, INCX

	MUL	t1, ALPHA, a5
	LD	a5,  0 * SIZE(XX)
	MUL	t2, ALPHA, a6
	NOP

	MUL	t3, ALPHA, a7
	LD	a6,  1 * SIZE(XX)
	MUL	t4, ALPHA, a8
	daddu	XX, XX, INCX

	MADD	s1, s1, t1, t1
	LD	a7,  0 * SIZE(XX)
	MADD	s2, s2, t2, t2
	LD	a8,  1 * SIZE(XX)

	MADD	s3, s3, t3, t3
	daddu	XX, XX, INCX
	bgtz	I, .L103
	MADD	s4, s4, t4, t4
	.align 3

.L104:
	MUL	t1, ALPHA, a1
	MUL	t2, ALPHA, a2
	MUL	t3, ALPHA, a3
	MUL	t4, ALPHA, a4

	MADD	s1, s1, t1, t1
	MADD	s2, s2, t2, t2
	MADD	s3, s3, t3, t3
	MADD	s4, s4, t4, t4

	MUL	t1, ALPHA, a5
	MUL	t2, ALPHA, a6
	MUL	t3, ALPHA, a7
	MUL	t4, ALPHA, a8

	MADD	s1, s1, t1, t1
	MADD	s2, s2, t2, t2
	MADD	s3, s3, t3, t3
	MADD	s4, s4, t4, t4
	.align 3

.L105:
	andi	I,  N, 3

	blez	I, .L998
	NOP
	.align	3

.L106:
	LD	a1,  0 * SIZE(XX)
	LD	a2,  1 * SIZE(XX)
	daddiu	I, I, -1

	MUL	t1, ALPHA, a1
	MUL	t2, ALPHA, a2

	MADD	s1, s1, t1, t1
	daddu	XX, XX, INCX

	bgtz	I, .L106
	MADD	s2, s2, t2, t2
	.align 3

.L998:
	ADD	s1, s1, s2
	ADD	s3, s3, s4

	ADD	s1, s1, s3

	sqrt.d	s1, s1

	j	$31
	MUL	s1, max, s1
	.align 3

.L999:
	j	$31
	NOP

	EPILOGUE
