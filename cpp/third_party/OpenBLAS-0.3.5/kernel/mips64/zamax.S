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
#define t5	$f16
#define t6	$f17
#define t7	$f18
#define t8	$f19

#define s1	$f0
#define s2	$f1
#define s3	$f2
#define s4	$f3

	PROLOGUE

#ifdef F_INTERFACE
	LDINT	N,     0(N)
	LDINT	INCX,  0(INCX)
#endif

	blez	N, .L999
	MTC	$0,  s1

	blez	INCX, .L999
	dsll	INCX, INCX, ZBASE_SHIFT

	LD	a1,  0 * SIZE(X)
	daddiu	N, N, -1

	LD	a2,  1 * SIZE(X)
	daddu	X, X, INCX

	FABS	t1, a1
	FABS	t2, a2

	blez	N, .L999
	ADD	s1, t1, t2

	NOP
	ADD	s2, t1, t2

	dsra	I, N, 2
	ADD	s3, t1, t2

	blez	I, .L15
	ADD	s4, t1, t2

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
	LD	a2,  1 * SIZE(X)

	FABS	t3, a3
	daddu	X, X, INCX
	FABS	t4, a4
	NOP

	FABS	t5, a5
	LD	a3,  0 * SIZE(X)
	FABS	t6, a6
	LD	a4,  1 * SIZE(X)

	FABS	t7, a7
	daddu	X, X, INCX
	FABS	t8, a8
	NOP

	ADD	t1, t1, t2
	LD	a5,  0 * SIZE(X)
	ADD	t3, t3, t4
	LD	a6,  1 * SIZE(X)

	ADD	t5, t5, t6
	daddu	X, X, INCX
	ADD	t7, t7, t8
	NOP

	CMPLT	$fcc0, s1, t1
	LD	a7,  0 * SIZE(X)
	CMPLT	$fcc1, s2, t3
	LD	a8,  1 * SIZE(X)

	CMPLT	$fcc2, s3, t5
	daddu	X, X, INCX
	CMPLT	$fcc3, s4, t7
	NOP

	CMOVT	s1, t1, $fcc0
	daddiu	I, I, -1
	CMOVT	s2, t3, $fcc1
	NOP

	CMOVT	s3, t5, $fcc2
	bgtz	I, .L12

	CMOVT	s4, t7, $fcc3
	NOP
	.align 3

.L13:
	FABS	t1, a1
	FABS	t2, a2
	FABS	t3, a3
	FABS	t4, a4

	FABS	t5, a5
	FABS	t6, a6
	FABS	t7, a7
	FABS	t8, a8

	ADD	t1, t1, t2
	ADD	t3, t3, t4
	ADD	t5, t5, t6
	ADD	t7, t7, t8

	CMPLT	$fcc0, s1, t1
	CMPLT	$fcc1, s2, t3
	CMPLT	$fcc2, s3, t5
	CMPLT	$fcc3, s4, t7

	CMOVT	s1, t1, $fcc0
	CMOVT	s2, t3, $fcc1
	CMOVT	s3, t5, $fcc2
	CMOVT	s4, t7, $fcc3
	.align 3

.L15:
	andi	I,  N, 3

	blez	I, .L998
	NOP
	.align	3

.L16:
	LD	a1,  0 * SIZE(X)
	LD	a2,  1 * SIZE(X)

	daddiu	I, I, -1

	FABS	t1, a1
	FABS	t2, a2


	ADD	t1, t1, t2

	CMPLT	$fcc0, s1, t1

	CMOVT	s1, t1, $fcc0

	bgtz	I, .L16
	daddu	X, X, INCX
	.align 3

.L998:
	CMPLT	$fcc0, s1, s2
	CMPLT	$fcc1, s3, s4

	CMOVT	s1, s2, $fcc0
	CMOVT	s3, s4, $fcc1

	CMPLT	$fcc0, s1, s3
	CMOVT	s1, s3, $fcc0
	.align 3

.L999:
	j	$31
	NOP

	EPILOGUE
