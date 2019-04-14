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
	dsll	INCX, INCX, ZBASE_SHIFT

	blez	N, .L999
	dsra	I, N, 2

	blez	I, .L25
	NOP

	LD	a1,  0 * SIZE(X)
	LD	a2,  1 * SIZE(X)
	daddu	X, X, INCX

	LD	a3,  0 * SIZE(X)
	LD	a4,  1 * SIZE(X)
	daddu	X, X, INCX

	LD	a5,  0 * SIZE(X)
	LD	a6,  1 * SIZE(X)
	daddu	X, X, INCX

	FABS	t1, a1
	FABS	t2, a2

	LD	a7,  0 * SIZE(X)
	LD	a8,  1 * SIZE(X)

	FABS	t3, a3
	FABS	t4, a4
	daddiu	I, I, -1

	blez	I, .L24
	daddu	X, X, INCX
	.align 3

.L23:
	ADD	s1, s1, t1
	LD	a1,  0 * SIZE(X)

	FABS	t1, a5
	daddiu	I, I, -1

	ADD	s2, s2, t2
	LD	a2,  1 * SIZE(X)

	FABS	t2, a6
	daddu	X, X, INCX

	ADD	s1, s1, t3
	LD	a3,  0 * SIZE(X)

	FABS	t3, a7
	NOP

	ADD	s2, s2, t4
	LD	a4,  1 * SIZE(X)

	FABS	t4, a8
	daddu	X, X, INCX

	ADD	s1, s1, t1
	LD	a5,  0 * SIZE(X)

	FABS	t1, a1
	NOP

	ADD	s2, s2, t2
	LD	a6,  1 * SIZE(X)

	FABS	t2, a2
	daddu	X, X, INCX

	ADD	s1, s1, t3
	LD	a7,  0 * SIZE(X)

	FABS	t3, a3
	LD	a8,  1 * SIZE(X)

	ADD	s2, s2, t4
	daddu	X, X, INCX

	bgtz	I, .L23
	FABS	t4, a4
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
	andi	I,  N, 3

	blez	I, .L999
	NOP
	.align	3

.L26:
	LD	a1,  0 * SIZE(X)
	LD	a2,  1 * SIZE(X)

	FABS	t1, a1
	daddiu	I, I, -1
	FABS	t2, a2
	daddu	X, X, INCX

	ADD	s1, s1, t1
	bgtz	I, .L26
	ADD	s2, s2, t2
	.align 3

.L999:
	j	$31
	ADD	s1, s1, s2

	EPILOGUE
