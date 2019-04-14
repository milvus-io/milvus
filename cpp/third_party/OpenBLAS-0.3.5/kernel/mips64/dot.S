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

#define a1	$f2
#define a2	$f3
#define a3	$f4
#define a4	$f5
#define b1	$f6
#define b2	$f7
#define b3	$f8
#define b4	$f9

#define s1	$f0
#define s2	$f1

	PROLOGUE

#ifdef F_INTERFACE
	LDINT	N,     0(N)
	LDINT	INCX,  0(INCX)
	LDINT	INCY,  0(INCY)
#endif

	MTC	$0,  s1
	MTC	$0,  s2

	dsll	INCX, INCX, BASE_SHIFT
	li	TEMP, SIZE

	blez	N, .L999
	dsll	INCY, INCY, BASE_SHIFT

	bne	INCX, TEMP, .L20
	dsra	I, N, 3

	bne	INCY, TEMP, .L20
	NOP

	blez	I, .L15
	NOP

	LD	a1,  0 * SIZE(X)
	LD	b1,  0 * SIZE(Y)

	LD	a2,  1 * SIZE(X)
	LD	b2,  1 * SIZE(Y)

	LD	a3,  2 * SIZE(X)
	LD	b3,  2 * SIZE(Y)

	LD	a4,  3 * SIZE(X)
	daddiu	I, I, -1

	blez	I, .L13
	LD	b4,  3 * SIZE(Y)
	.align 3

.L12:
#ifdef DSDOT
cvt.d.s  a1, a1
cvt.d.s  b1, b1  
madd.d s1, s1, a1, b1
#else
	MADD	s1, s1, a1, b1
#endif
	LD	a1,  4 * SIZE(X)
	LD	b1,  4 * SIZE(Y)

#ifdef DSDOT
cvt.d.s  a2, a2
cvt.d.s  b2, b2
madd.d s2, s2, a2, b2
#else
	MADD	s2, s2, a2, b2
#endif
	LD	a2,  5 * SIZE(X)
	LD	b2,  5 * SIZE(Y)

#ifdef DSDOT
cvt.d.s  a3, a3
cvt.d.s  b3, b3
madd.d s1, s1, a3, b3
#else
	MADD	s1, s1, a3, b3
#endif
	LD	a3,  6 * SIZE(X)
	LD	b3,  6 * SIZE(Y)

#ifdef DSDOT
cvt.d.s  a4, a4
cvt.d.s  b4, b4
madd.d s2, s2, a4, b4
#else
	MADD	s2, s2, a4, b4
#endif
	LD	a4,  7 * SIZE(X)
	LD	b4,  7 * SIZE(Y)

#ifdef DSDOT
cvt.d.s  a1, a1
cvt.d.s  b1, b1
madd.d s1, s1, a1, b1
#else
	MADD	s1, s1, a1, b1
#endif
	LD	a1,  8 * SIZE(X)
	LD	b1,  8 * SIZE(Y)

#ifdef DSDOT
cvt.d.s  a2, a2
cvt.d.s  b2, b2
madd.d s2, s2, a2, b2
#else
	MADD	s2, s2, a2, b2
#endif
	LD	a2,  9 * SIZE(X)
	LD	b2,  9 * SIZE(Y)

#ifdef DSDOT
cvt.d.s  a3, a3
cvt.d.s  b3, b3
madd.d s1, s1, a3, b3
#else
	MADD	s1, s1, a3, b3
#endif
	LD	a3, 10 * SIZE(X)
	LD	b3, 10 * SIZE(Y)

#ifdef DSDOT
cvt.d.s  a4, a4
cvt.d.s  b4, b4
madd.d s2, s2, a4, b4
#else
	MADD	s2, s2, a4, b4
#endif
	LD	a4, 11 * SIZE(X)
	LD	b4, 11 * SIZE(Y)

	daddiu	I, I, -1
	daddiu	X, X, 8 * SIZE

	bgtz	I, .L12
	daddiu	Y, Y, 8 * SIZE
	.align 3

.L13:
#ifdef DSDOT
cvt.d.s  a1, a1
cvt.d.s  b1, b1
madd.d s1, s1, a1, b1
#else
	MADD	s1, s1, a1, b1
#endif
	LD	a1,  4 * SIZE(X)
	LD	b1,  4 * SIZE(Y)

#ifdef DSDOT
cvt.d.s a2, a2
cvt.d.s b2, b2
madd.d s2, s2, a2, b2
#else
	MADD	s2, s2, a2, b2
#endif
	LD	a2,  5 * SIZE(X)
	LD	b2,  5 * SIZE(Y)

#ifdef DSDOT
cvt.d.s a3, a3 
cvt.d.s b3, b3
madd.d s1, s1, a3, b3
#else
	MADD	s1, s1, a3, b3
#endif
	LD	a3,  6 * SIZE(X)
	LD	b3,  6 * SIZE(Y)

#ifdef DSDOT
cvt.d.s a4, a4
cvt.d.s b4, b4
madd.d s2, s2, a4, b4
#else
	MADD	s2, s2, a4, b4
#endif
	LD	a4,  7 * SIZE(X)
	LD	b4,  7 * SIZE(Y)

#ifdef DSDOT
cvt.d.s  a1, a1
cvt.d.s  b1, b1
madd.d s1, s1, a1, b1
#else
	MADD	s1, s1, a1, b1
#endif
	daddiu	X, X, 8 * SIZE
#ifdef DSDOT
cvt.d.s  a2, a2
cvt.d.s  b2, b2
madd.d s2, s2, a2, b2
#else
	MADD	s2, s2, a2, b2
#endif
	daddiu	Y, Y, 8 * SIZE

#ifdef DSDOT
cvt.d.s  a3, a3
cvt.d.s  b3, b3
madd.d s1, s1, a3, b3
#else
	MADD	s1, s1, a3, b3
#endif
#ifdef DSDOT
cvt.d.s  a4, a4
cvt.d.s  b4, b4
madd.d s2, s2, a4, b4
#else
	MADD	s2, s2, a4, b4
#endif
	.align 3

.L15:
	andi	I,  N, 7

	blez	I, .L999
	NOP
	.align	3

.L16:
	LD	a1,  0 * SIZE(X)
	LD	b1,  0 * SIZE(Y)

#ifdef DSDOT
cvt.d.s  a1, a1
cvt.d.s  b1, b1
madd.d s1, s1, a1, b1
#else
	MADD	s1, s1, a1, b1
#endif
	daddiu	I, I, -1

	daddiu	X, X, SIZE
	daddiu	Y, Y, SIZE

	bgtz	I, .L16
	NOP
	j	.L999
	NOP
	.align 3

.L20:
#ifdef F_INTERFACE
	bgez	INCX, .L21
	daddiu	TEMP, N, -1

	mult	TEMP, INCX

	mflo	TEMP
	dsub	X, X, TEMP
	.align 3

.L21:
	bgez	INCY, .L22
	daddiu	TEMP, N, -1

	mult	TEMP, INCY

	mflo	TEMP
	dsub	Y, Y, TEMP
	.align 3

.L22:
#endif
	blez	I, .L25
	NOP
	.align 3

.L23:
	LD	a1,  0 * SIZE(X)
	dadd	X, X, INCX
	LD	b1,  0 * SIZE(Y)
	dadd	Y, Y, INCY

#ifdef DSDOT
cvt.d.s  a1, a1
cvt.d.s  b1, b1
madd.d s1, s1, a1, b1
#else
	MADD	s1, s1, a1, b1
#endif
	LD	a1,  0 * SIZE(X)
	dadd	X, X, INCX
	LD	b1,  0 * SIZE(Y)
	dadd	Y, Y, INCY

#ifdef DSDOT
cvt.d.s  a1, a1
cvt.d.s  b1, b1
madd.d s2, s2, a1, b1
#else
	MADD	s2, s2, a1, b1
#endif
	LD	a1,  0 * SIZE(X)
	dadd	X, X, INCX
	LD	b1,  0 * SIZE(Y)
	dadd	Y, Y, INCY

#ifdef DSDOT
cvt.d.s  a1, a1
cvt.d.s  b1, b1
madd.d s1, s1, a1, b1
#else
	MADD	s1, s1, a1, b1
#endif
	LD	a1,  0 * SIZE(X)
	dadd	X, X, INCX
	LD	b1,  0 * SIZE(Y)
	dadd	Y, Y, INCY

#ifdef DSDOT
cvt.d.s  a1, a1
cvt.d.s  b1, b1
madd.d s2, s2, a1, b1
#else
	MADD	s2, s2, a1, b1
#endif
	LD	a1,  0 * SIZE(X)
	dadd	X, X, INCX
	LD	b1,  0 * SIZE(Y)
	dadd	Y, Y, INCY

#ifdef DSDOT
cvt.d.s  a1, a1
cvt.d.s  b1, b1
madd.d s1, s1, a1, b1
#else
	MADD	s1, s1, a1, b1
#endif
	LD	a1,  0 * SIZE(X)
	dadd	X, X, INCX
	LD	b1,  0 * SIZE(Y)
	dadd	Y, Y, INCY

#ifdef DSDOT
cvt.d.s  a1, a1
cvt.d.s  b1, b1
madd.d s2, s2, a1, b1
#else
	MADD	s2, s2, a1, b1
#endif
	LD	a1,  0 * SIZE(X)
	dadd	X, X, INCX
	LD	b1,  0 * SIZE(Y)
	dadd	Y, Y, INCY

#ifdef DSDOT
cvt.d.s  a1, a1
cvt.d.s  b1, b1
madd.d s1, s1, a1, b1
#else
	MADD	s1, s1, a1, b1
#endif
	LD	a1,  0 * SIZE(X)
	dadd	X, X, INCX
	LD	b1,  0 * SIZE(Y)
	dadd	Y, Y, INCY

	daddiu	I, I, -1

	bgtz	I, .L23
#ifdef DSDOT
cvt.d.s  a1, a1
cvt.d.s  b1, b1
madd.d s2, s2, a1, b1
#else
	MADD	s2, s2, a1, b1
#endif
	.align 3

.L25:
	andi	I,  N, 7

	blez	I, .L999
	NOP
	.align	3

.L26:
	LD	a1,  0 * SIZE(X)
	dadd	X, X, INCX
	LD	b1,  0 * SIZE(Y)
	dadd	Y, Y, INCY

	daddiu	I, I, -1

	bgtz	I, .L26
#ifdef DSDOT
cvt.d.s  a1, a1
cvt.d.s  b1, b1
madd.d s1, s1, a1, b1
#else
	MADD	s1, s1, a1, b1
#endif
	.align 3

.L999:
#ifdef DSDOT
	add.d s1, s1, s2
#else
	ADD	s1, s1, s2
#endif
	j	$31
	NOP

	EPILOGUE
