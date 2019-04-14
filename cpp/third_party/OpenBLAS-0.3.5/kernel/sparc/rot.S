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

#define N	%i0
#define X	%i1
#define INCX	%i2
#define Y	%i3
#define INCY	%i4
#define I	%i5

#define XX	%l0
#define YY	%l1

#ifdef DOUBLE
#define a1	%f4
#define a2	%f6
#define a3	%f8
#define a4	%f10
#define a5	%f12
#define a6	%f14
#define a7	%f16
#define a8	%f18
#define b1	%f20
#define b2	%f22
#define b3	%f24
#define b4	%f26
#define b5	%f28
#define b6	%f30
#define b7	%f32
#define b8	%f34

#define c1	%f36
#define c2	%f38
#define c3	%f40
#define c4	%f42
#define c5	%f44
#define c6	%f46
#define c7	%f48
#define c8	%f50

#define t1	%f52
#define t2	%f54
#define t3	%f56
#define t4	%f58
#else
#define a1	%f2
#define a2	%f3
#define a3	%f4
#define a4	%f5
#define a5	%f6
#define a6	%f7
#define a7	%f8
#define a8	%f9
#define b1	%f10
#define b2	%f11
#define b3	%f12
#define b4	%f13
#define b5	%f14
#define b6	%f15
#define b7	%f16
#define b8	%f17

#define c1	%f18
#define c2	%f19
#define c3	%f20
#define c4	%f21
#define c5	%f22
#define c6	%f23
#define c7	%f24
#define c8	%f25

#define t1	%f26
#define t2	%f27
#define t3	%f28
#define t4	%f29
#endif

#ifdef DOUBLE
#define C	%f0
#define S	%f2
#else
#define C	%f0
#define S	%f1
#endif

	PROLOGUE
	SAVESP

#ifndef __64BIT__

#ifdef DOUBLE
	st	%i5, [%sp + STACK_START + 24]

	LDF	[%sp + STACK_START + 24], C
	LDF	[%sp + STACK_START + 32], S
#else
	st	%i5, [%sp + STACK_START + 24]

	LDF	[%sp + STACK_START + 24], C
	LDF	[%sp + STACK_START + 28], S
#endif
#else
#ifdef DOUBLE
	FMOV	%f10, C
	FMOV	%f12, S
#else
	FMOV	%f11, C
	FMOV	%f13, S
#endif
#endif

	cmp	N, 0
	ble	.LL19
	nop

	sll	INCX, BASE_SHIFT, INCX
	sll	INCY, BASE_SHIFT, INCY

	cmp	INCX, SIZE
	bne	.LL50
	nop

	cmp	INCY, SIZE
	bne	.LL50
	nop

	sra	N, 3, I
	cmp	I, 0
	ble,pn	%icc, .LL15
	nop


	LDF	[X +  0 * SIZE], a1
	LDF	[Y +  0 * SIZE], b1
	LDF	[X +  1 * SIZE], a2
	LDF	[Y +  1 * SIZE], b2
	LDF	[X +  2 * SIZE], a3
	LDF	[Y +  2 * SIZE], b3
	LDF	[X +  3 * SIZE], a4
	LDF	[Y +  3 * SIZE], b4

	LDF	[X +  4 * SIZE], a5
	LDF	[Y +  4 * SIZE], b5
	LDF	[X +  5 * SIZE], a6
	LDF	[Y +  5 * SIZE], b6
	LDF	[X +  6 * SIZE], a7
	LDF	[Y +  6 * SIZE], b7
	LDF	[X +  7 * SIZE], a8
	LDF	[Y +  7 * SIZE], b8

	FMUL	C, a1, c1
	FMUL	S, b1, c2
	FMUL	C, b1, c3
	LDF	[Y +  8 * SIZE], b1
	FMUL	S, a1, c4
	LDF	[X +  8 * SIZE], a1

	FMUL	C, a2, c5
	FMUL	S, b2, c6
	FADD	c1, c2, t1

	FMUL	C, b2, c7
	LDF	[Y +  9 * SIZE], b2
	FMUL	S, a2, c8
	LDF	[X +  9 * SIZE], a2
	FSUB	c3, c4, t2

	addcc	I, -1, I
	ble,pt	%icc, .LL12
	nop

#define PREFETCHSIZE 64

.LL11:
	FMUL	C, a3, c1
	nop
	prefetch [Y  + PREFETCHSIZE * SIZE], 1
	nop

	FMUL	S, b3, c2
	STF	t1, [X +  0 * SIZE]
	FADD	c5, c6, t3
	nop

	FMUL	C, b3, c3
	LDF	[Y + 10 * SIZE], b3
	nop
	nop

	FMUL	S, a3, c4
	STF	t2, [Y +  0 * SIZE]
	FSUB	c7, c8, t4
	nop

	FMUL	C, a4, c5
	LDF	[X + 10 * SIZE], a3
	nop
	nop

	FMUL	S, b4, c6
	STF	t3, [X +  1 * SIZE]
	FADD	c1, c2, t1
	nop

	FMUL	C, b4, c7
	LDF	[Y + 11 * SIZE], b4
	nop
	nop

	FMUL	S, a4, c8
	STF	t4, [Y +  1 * SIZE]
	FSUB	c3, c4, t2
	nop

	FMUL	C, a5, c1
	LDF	[X + 11 * SIZE], a4
	nop
	nop

	FMUL	S, b5, c2
	STF	t1, [X +  2 * SIZE]
	FADD	c5, c6, t3
	nop

	FMUL	C, b5, c3
	LDF	[Y + 12 * SIZE], b5
	nop
	nop

	FMUL	S, a5, c4
	STF	t2, [Y +  2 * SIZE]
	FSUB	c7, c8, t4
	nop

	FMUL	C, a6, c5
	LDF	[X + 12 * SIZE], a5
	nop
	nop

	FMUL	S, b6, c6
	STF	t3, [X +  3 * SIZE]
	FADD	c1, c2, t1
	nop

	FMUL	C, b6, c7
	LDF	[Y + 13 * SIZE], b6
	nop
	nop

	FMUL	S, a6, c8
	STF	t4, [Y +  3 * SIZE]
	FSUB	c3, c4, t2
	nop

	FMUL	C, a7, c1
	LDF	[X + 13 * SIZE], a6
	nop
	nop

	FMUL	S, b7, c2
	STF	t1, [X +  4 * SIZE]
	FADD	c5, c6, t3
	nop

	FMUL	C, b7, c3
	LDF	[Y + 14 * SIZE], b7
	nop
	nop

	FMUL	S, a7, c4
	STF	t2, [Y +  4 * SIZE]
	FSUB	c7, c8, t4
	nop

	FMUL	C, a8, c5
	LDF	[X + 14 * SIZE], a7
	nop
	nop

	FMUL	S, b8, c6
	STF	t3, [X +  5 * SIZE]
	FADD	c1, c2, t1
	nop

	FMUL	C, b8, c7
	LDF	[Y + 15 * SIZE], b8
	nop
	nop

	FMUL	S, a8, c8
	STF	t4, [Y +  5 * SIZE]
	FSUB	c3, c4, t2
	nop

	FMUL	C, a1, c1
	LDF	[X + 15 * SIZE], a8
	addcc	I, -1, I
	nop

	FMUL	S, b1, c2
	STF	t1, [X +  6 * SIZE]
	FADD	c5, c6, t3
	nop

	FMUL	C, b1, c3
	LDF	[Y + 16 * SIZE], b1
	nop
	nop

	FMUL	S, a1, c4
	STF	t2, [Y +  6 * SIZE]
	FSUB	c7, c8, t4
	nop

	FMUL	C, a2, c5
	LDF	[X + 16 * SIZE], a1
	add	Y, 8 * SIZE, Y
	nop

	FMUL	S, b2, c6
	STF	t3, [X +  7 * SIZE]
	FADD	c1, c2, t1
	nop

	FMUL	C, b2, c7
	LDF	[Y +  9 * SIZE], b2
	add	X, 8 * SIZE, X
	nop

	FMUL	S, a2, c8
	STF	t4, [Y -  1 * SIZE]
	FSUB	c3, c4, t2
	nop

	bg,pt	%icc, .LL11
	LDF	[X +  9 * SIZE], a2


.LL12:
	FMUL	C, a3, c1
	FMUL	S, b3, c2
	STF	t1, [X +  0 * SIZE]
	FADD	c5, c6, t3

	FMUL	C, b3, c3
	FMUL	S, a3, c4
	STF	t2, [Y +  0 * SIZE]
	FSUB	c7, c8, t4


	FMUL	C, a4, c5
	FMUL	S, b4, c6
	STF	t3, [X +  1 * SIZE]
	FADD	c1, c2, t1

	FMUL	C, b4, c7
	FMUL	S, a4, c8
	STF	t4, [Y +  1 * SIZE]
	FSUB	c3, c4, t2


	FMUL	C, a5, c1
	FMUL	S, b5, c2
	STF	t1, [X +  2 * SIZE]
	FADD	c5, c6, t3

	FMUL	C, b5, c3
	FMUL	S, a5, c4
	STF	t2, [Y +  2 * SIZE]
	FSUB	c7, c8, t4

	FMUL	C, a6, c5
	FMUL	S, b6, c6
	STF	t3, [X +  3 * SIZE]
	FADD	c1, c2, t1

	FMUL	C, b6, c7
	FMUL	S, a6, c8
	STF	t4, [Y +  3 * SIZE]
	FSUB	c3, c4, t2

	FMUL	C, a7, c1
	FMUL	S, b7, c2
	STF	t1, [X +  4 * SIZE]
	FADD	c5, c6, t3

	FMUL	C, b7, c3
	FMUL	S, a7, c4
	STF	t2, [Y +  4 * SIZE]
	FSUB	c7, c8, t4

	FMUL	C, a8, c5
	FMUL	S, b8, c6
	STF	t3, [X +  5 * SIZE]
	FADD	c1, c2, t1

	FMUL	C, b8, c7
	FMUL	S, a8, c8
	STF	t4, [Y +  5 * SIZE]
	FSUB	c3, c4, t2

	FADD	c5, c6, t3
	STF	t1, [X +  6 * SIZE]

	FSUB	c7, c8, t4
	STF	t2, [Y +  6 * SIZE]

	STF	t3, [X +  7 * SIZE]
	STF	t4, [Y +  7 * SIZE]

	add	X, 8 * SIZE, X
	add	Y, 8 * SIZE, Y


.LL15:
	andcc	N, 7, I
	nop
	ble,a,pn %icc, .LL19
	nop

.LL16:
	LDF	[X + 0 * SIZE], a1
	add	X, 1 * SIZE, X
	LDF	[Y + 0 * SIZE], b1
	add	Y, 1 * SIZE, Y

	FMUL	C, a1, c1
	FMUL	S, b1, c2
	FMUL	C, b1, c3
	FMUL	S, a1, c4

	FADD	c1, c2, c2
	addcc	I, -1, I
	FSUB	c3, c4, c4
	nop

	STF	c2, [X - 1 * SIZE]
	STF	c4, [Y - 1 * SIZE]
	bg,pt	%icc, .LL16
	nop

.LL19:
	return	%i7 + 8
	nop

.LL50:
	mov	X, XX
	mov	Y, YY

	sra	N, 3, I
	cmp	I, 0
	ble,pn	%icc, .LL55
	nop

.LL51:
	LDF	[X +  0 * SIZE], a1
	add	X, INCX, X
	LDF	[Y +  0 * SIZE], b1
	add	Y, INCY, Y
	LDF	[X +  0 * SIZE], a2
	add	X, INCX, X
	LDF	[Y +  0 * SIZE], b2
	add	Y, INCY, Y
	LDF	[X +  0 * SIZE], a3
	add	X, INCX, X
	LDF	[Y +  0 * SIZE], b3
	add	Y, INCY, Y
	LDF	[X +  0 * SIZE], a4
	add	X, INCX, X
	LDF	[Y +  0 * SIZE], b4
	add	Y, INCY, Y

	LDF	[X +  0 * SIZE], a5
	add	X, INCX, X
	LDF	[Y +  0 * SIZE], b5
	add	Y, INCY, Y
	LDF	[X +  0 * SIZE], a6
	add	X, INCX, X
	LDF	[Y +  0 * SIZE], b6
	add	Y, INCY, Y
	LDF	[X +  0 * SIZE], a7
	add	X, INCX, X
	LDF	[Y +  0 * SIZE], b7
	add	Y, INCY, Y
	LDF	[X +  0 * SIZE], a8
	add	X, INCX, X
	LDF	[Y +  0 * SIZE], b8
	add	Y, INCY, Y

	FMUL	C, a1, c1
	FMUL	S, b1, c2
	FMUL	C, b1, c3
	FMUL	S, a1, c4

	FADD	c1, c2, t1
	FSUB	c3, c4, t2

	STF	t1, [XX +  0 * SIZE]
	add	XX, INCX, XX
	STF	t2, [YY +  0 * SIZE]
	add	YY, INCY, YY

	FMUL	C, a2, c5
	FMUL	S, b2, c6
	FMUL	C, b2, c7
	FMUL	S, a2, c8

	FADD	c5, c6, t3
	FSUB	c7, c8, t4

	STF	t3, [XX +  0 * SIZE]
	add	XX, INCX, XX
	STF	t4, [YY +  0 * SIZE]
	add	YY, INCY, YY

	FMUL	C, a3, c1
	FMUL	S, b3, c2
	FMUL	C, b3, c3
	FMUL	S, a3, c4

	FADD	c1, c2, t1
	FSUB	c3, c4, t2

	STF	t1, [XX +  0 * SIZE]
	add	XX, INCX, XX
	STF	t2, [YY +  0 * SIZE]
	add	YY, INCY, YY

	FMUL	C, a4, c5
	FMUL	S, b4, c6
	FMUL	C, b4, c7
	FMUL	S, a4, c8

	FADD	c5, c6, t3
	FSUB	c7, c8, t4

	STF	t3, [XX +  0 * SIZE]
	add	XX, INCX, XX
	STF	t4, [YY +  0 * SIZE]
	add	YY, INCY, YY

	FMUL	C, a5, c1
	FMUL	S, b5, c2
	FMUL	C, b5, c3
	FMUL	S, a5, c4

	FADD	c1, c2, t1
	FSUB	c3, c4, t2

	STF	t1, [XX +  0 * SIZE]
	add	XX, INCX, XX
	STF	t2, [YY +  0 * SIZE]
	add	YY, INCY, YY

	FMUL	C, a6, c5
	FMUL	S, b6, c6
	FMUL	C, b6, c7
	FMUL	S, a6, c8

	FADD	c5, c6, t3
	FSUB	c7, c8, t4

	STF	t3, [XX +  0 * SIZE]
	add	XX, INCX, XX
	STF	t4, [YY +  0 * SIZE]
	add	YY, INCY, YY

	FMUL	C, a7, c1
	FMUL	S, b7, c2
	FMUL	C, b7, c3
	FMUL	S, a7, c4

	FADD	c1, c2, t1
	FSUB	c3, c4, t2

	STF	t1, [XX +  0 * SIZE]
	add	XX, INCX, XX
	STF	t2, [YY +  0 * SIZE]
	add	YY, INCY, YY

	FMUL	C, a8, c5
	FMUL	S, b8, c6
	FMUL	C, b8, c7
	FMUL	S, a8, c8

	FADD	c5, c6, t3
	FSUB	c7, c8, t4

	STF	t3, [XX +  0 * SIZE]
	add	XX, INCX, XX
	STF	t4, [YY +  0 * SIZE]
	add	YY, INCY, YY

	addcc	I, -1, I
	bg,pt	%icc, .LL51
	nop


.LL55:
	andcc	N, 7, I
	nop
	ble	%icc, .LL59
	nop

.LL56:
	LDF	[X +  0 * SIZE], a1
	LDF	[Y +  0 * SIZE], b1

	FMUL	C, a1, c1
	FMUL	S, b1, c2
	FMUL	C, b1, c3
	FMUL	S, a1, c4

	FADD	c1, c2, c2
	FSUB	c3, c4, c4

	STF	c2, [X +  0 * SIZE]
	add	X, INCX, X
	STF	c4, [Y +  0 * SIZE]
	addcc	I, -1, I

	bg	%icc, .LL56
	add	Y, INCY, Y


.LL59:
	return	%i7 + 8
	nop

	EPILOGUE
