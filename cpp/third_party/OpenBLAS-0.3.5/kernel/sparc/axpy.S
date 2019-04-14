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

#if defined(DOUBLE) && !defined(__64BIT__)
#define N	%i0
#define X	%i5
#define INCX	%i1
#define Y	%i2
#define INCY	%i3
#define I	%i4
#else
#define N	%i0
#define X	%i4
#define INCX	%i5
#define Y	%i1
#define INCY	%i2
#define I	%i3
#endif

#define YY	%l1

#ifdef DOUBLE
#define a1	%f0
#define a2	%f2
#define a3	%f4
#define a4	%f6
#define a5	%f8
#define a6	%f10
#define a7	%f12
#define a8	%f14
#define b1	%f16
#define b2	%f18
#define b3	%f20
#define b4	%f22
#define b5	%f24
#define b6	%f26
#define b7	%f28
#define b8	%f30

#define t1	%f32
#define t2	%f34
#define t3	%f36
#define	t4	%f38
#define c1	%f40
#define c2	%f42
#define c3	%f44
#define c4	%f46

#define c5	%f48
#define c6	%f50
#define c7	%f52
#define c8	%f54

#define ALPHA	%f62
#else
#define a1	%f0
#define a2	%f1
#define a3	%f2
#define a4	%f3
#define a5	%f4
#define a6	%f5
#define a7	%f6
#define a8	%f7
#define b1	%f8
#define b2	%f9
#define b3	%f10
#define b4	%f11
#define b5	%f12
#define b6	%f13
#define b7	%f14
#define b8	%f15

#define t1	%f16
#define t2	%f17
#define t3	%f18
#define	t4	%f19
#define c1	%f20
#define c2	%f21
#define c3	%f22
#define c4	%f23

#define c5	%f24
#define c6	%f25
#define c7	%f26
#define c8	%f27

#define ALPHA	%f31
#endif

	PROLOGUE
	SAVESP

#ifndef __64BIT__

#ifdef DOUBLE
	st	%i3, [%sp + STACK_START + 16]
	st	%i4, [%sp + STACK_START + 20]

	ld	[%sp + STACK_START + 28], INCX
	ld	[%sp + STACK_START + 32], Y
	ld	[%sp + STACK_START + 36], INCY
#else
	st	%i3, [%sp + STACK_START + 16]
	ld	[%sp + STACK_START + 28], Y
	ld	[%sp + STACK_START + 32], INCY
#endif
	LDF	[%sp + STACK_START + 16], ALPHA
#else
	ldx	[%sp + STACK_START + 56], Y
	ldx	[%sp + STACK_START + 64], INCY
#ifdef DOUBLE
	FMOV	%f6, ALPHA
#else
	FMOV	%f7, ALPHA
#endif
#endif

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

	FMUL	ALPHA, a1, t1
	FMUL	ALPHA, a2, t2
	FMUL	ALPHA, a3, t3
	FMUL	ALPHA, a4, t4

	FADD	b1, t1, c1
	FMUL	ALPHA, a5, t1
	FADD	b2, t2, c2
	FMUL	ALPHA, a6, t2

	add	I, -1, I
	cmp	I, 0
	ble,pt	%icc, .LL12
	nop

#ifdef DOUBLE
#define PREFETCHSIZE  54
#else
#define PREFETCHSIZE 108
#endif

.LL11:
	prefetch [Y  + PREFETCHSIZE * SIZE], 0

	LDF	[X +  8 * SIZE], a1
	LDF	[X +  9 * SIZE], a2
	LDF	[X + 10 * SIZE], a3
	LDF	[X + 11 * SIZE], a4

	FADD	b3, t3, c3
	STF	c1, [Y +  0 * SIZE]
	FMUL	ALPHA, a7, t3

	FADD	b4, t4, c4
	STF	c2, [Y +  1 * SIZE]
	FMUL	ALPHA, a8, t4

	LDF	[Y +  8 * SIZE], b1
	LDF	[Y +  9 * SIZE], b2
	LDF	[Y + 10 * SIZE], b3
	LDF	[Y + 11 * SIZE], b4

	FADD	b5, t1, c5
	STF	c3, [Y +  2 * SIZE]
	FMUL	ALPHA, a1, t1

	FADD	b6, t2, c6
	STF	c4, [Y +  3 * SIZE]
	FMUL	ALPHA, a2, t2

	prefetch [X  + PREFETCHSIZE * SIZE], 0

	LDF	[X + 12 * SIZE], a5
	LDF	[X + 13 * SIZE], a6
	LDF	[X + 14 * SIZE], a7
	LDF	[X + 15 * SIZE], a8

	FADD	b7, t3, c7
	STF	c5, [Y +  4 * SIZE]
	FMUL	ALPHA, a3, t3

	FADD	b8, t4, c8
	STF	c6, [Y +  5 * SIZE]
	FMUL	ALPHA, a4, t4

	LDF	[Y + 12 * SIZE], b5
	LDF	[Y + 13 * SIZE], b6
	LDF	[Y + 14 * SIZE], b7
	LDF	[Y + 15 * SIZE], b8

	FADD	b1, t1, c1
	STF	c7, [Y +  6 * SIZE]
	FMUL	ALPHA, a5, t1
	deccc	I

	FADD	b2, t2, c2
	STF	c8, [Y +  7 * SIZE]
	FMUL	ALPHA, a6, t2
	add	Y, 8 * SIZE, Y

	bg,pt	%icc, .LL11
	add	X, 8 * SIZE, X

.LL12:
	FADD	b3, t3, c3
	FMUL	ALPHA, a7, t3
	FADD	b4, t4, c4
	FMUL	ALPHA, a8, t4

	FADD	b5, t1, c5
	FADD	b6, t2, c6
	FADD	b7, t3, c7
	FADD	b8, t4, c8

	STF	c1, [Y +  0 * SIZE]
	STF	c2, [Y +  1 * SIZE]
	STF	c3, [Y +  2 * SIZE]
	STF	c4, [Y +  3 * SIZE]

	STF	c5, [Y +  4 * SIZE]
	STF	c6, [Y +  5 * SIZE]
	STF	c7, [Y +  6 * SIZE]
	STF	c8, [Y +  7 * SIZE]

	add	Y, 8 * SIZE, Y
	add	X, 8 * SIZE, X


.LL15:
	and	N, 7, I
	cmp	I,  0
	ble,a,pn %icc, .LL19
	nop

.LL16:
	LDF	[X +  0 * SIZE], a1
	LDF	[Y +  0 * SIZE], b1

	FMUL	ALPHA, a1, t1
	FADD	b1, t1, c1

	add	I, -1, I
	cmp	I, 0
	STF	c1, [Y +  0 * SIZE]
	add	Y, 1 * SIZE, Y
	bg,pt	%icc, .LL16
	add	X, 1 * SIZE, X

.LL19:
	return	%i7 + 8
	clr	%g0

.LL50:
	sra	N, 3, I
	cmp	I, 0
	ble,pn	%icc, .LL55
	mov	Y, YY

	LDF	[X +  0 * SIZE], a1
	add	I, -1, I
	add	X, INCX, X
	LDF	[Y +  0 * SIZE], b1
	cmp	I, 0
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
	ble,pt	%icc, .LL52
	add	Y, INCY, Y


.LL51:
	FMUL	ALPHA, a1, t1
	LDF	[X +  0 * SIZE], a1
	add	X, INCX, X

	FMUL	ALPHA, a2, t2
	LDF	[X +  0 * SIZE], a2
	add	X, INCX, X

	FMUL	ALPHA, a3, t3
	LDF	[X +  0 * SIZE], a3
	add	X, INCX, X
	FMUL	ALPHA, a4, t4
	LDF	[X +  0 * SIZE], a4
	add	X, INCX, X

	FADD	b1, t1, c1
	LDF	[Y +  0 * SIZE], b1
	add	Y, INCY, Y

	FMUL	ALPHA, a5, t1
	LDF	[X +  0 * SIZE], a5
	add	X, INCX, X
	FADD	b2, t2, c2
	LDF	[Y +  0 * SIZE], b2
	add	Y, INCY, Y

	FMUL	ALPHA, a6, t2
	LDF	[X +  0 * SIZE], a6
	add	X, INCX, X
	FADD	b3, t3, c3
	LDF	[Y +  0 * SIZE], b3
	add	Y, INCY, Y

	FMUL	ALPHA, a7, t3
	LDF	[X +  0 * SIZE], a7
	add	X, INCX, X
	FADD	b4, t4, c4
	LDF	[Y +  0 * SIZE], b4
	add	Y, INCY, Y
	FMUL	ALPHA, a8, t4
	LDF	[X +  0 * SIZE], a8
	add	X, INCX, X

	STF	c1, [YY +  0 * SIZE]
	add	YY, INCY, YY
	FADD	b5, t1, c1
	STF	c2, [YY +  0 * SIZE]
	add	YY, INCY, YY
	FADD	b6, t2, c2
	STF	c3, [YY +  0 * SIZE]
	add	YY, INCY, YY
	FADD	b7, t3, c3
	STF	c4, [YY +  0 * SIZE]
	add	YY, INCY, YY
	FADD	b8, t4, c4

	LDF	[Y +  0 * SIZE], b5
	add	I, -1, I
	add	Y, INCY, Y
	LDF	[Y +  0 * SIZE], b6
	cmp	I, 0
	add	Y, INCY, Y
	LDF	[Y +  0 * SIZE], b7
	add	Y, INCY, Y
	LDF	[Y +  0 * SIZE], b8
	add	Y, INCY, Y

	STF	c1, [YY +  0 * SIZE]
	add	YY, INCY, YY
	STF	c2, [YY +  0 * SIZE]
	add	YY, INCY, YY
	STF	c3, [YY +  0 * SIZE]
	add	YY, INCY, YY
	STF	c4, [YY +  0 * SIZE]

	bg,pt	%icc, .LL51
	add	YY, INCY, YY

.LL52:
	FMUL	ALPHA, a1, t1
	FMUL	ALPHA, a2, t2
	FMUL	ALPHA, a3, t3
	FMUL	ALPHA, a4, t4

	FADD	b1, t1, c1
	FMUL	ALPHA, a5, t1
	FADD	b2, t2, c2
	FMUL	ALPHA, a6, t2
	FADD	b3, t3, c3
	FMUL	ALPHA, a7, t3
	FADD	b4, t4, c4
	FMUL	ALPHA, a8, t4

	STF	c1, [YY +  0 * SIZE]
	add	YY, INCY, YY
	FADD	b5, t1, c1
	STF	c2, [YY +  0 * SIZE]
	add	YY, INCY, YY
	FADD	b6, t2, c2
	STF	c3, [YY +  0 * SIZE]
	add	YY, INCY, YY
	FADD	b7, t3, c3
	STF	c4, [YY +  0 * SIZE]
	add	YY, INCY, YY
	FADD	b8, t4, c4

	STF	c1, [YY +  0 * SIZE]
	add	YY, INCY, YY
	STF	c2, [YY +  0 * SIZE]
	add	YY, INCY, YY
	STF	c3, [YY +  0 * SIZE]
	add	YY, INCY, YY
	STF	c4, [YY +  0 * SIZE]
	add	YY, INCY, YY

.LL55:
	and	N, 7, I
	cmp	I,  0
	ble,a,pn %icc, .LL59
	nop

.LL56:
	LDF	[X +  0 * SIZE], a1
	LDF	[Y +  0 * SIZE], b1

	FMUL	ALPHA, a1, t1
	FADD	b1, t1, c1

	add	I, -1, I
	cmp	I, 0
	STF	c1, [Y +  0 * SIZE]
	add	Y, INCY, Y
	bg,pt	%icc, .LL56
	add	X, INCX, X

.LL59:
	return	%i7 + 8
	clr	%o0

	EPILOGUE
