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

#define XX	%l0
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
#endif

#ifdef DOUBLE
#define PREFETCHSIZE 128
#else
#define PREFETCHSIZE 256
#endif

	PROLOGUE
	SAVESP

#ifndef __64BIT__
#ifdef DOUBLE
	ld	[%sp + STACK_START + 28], INCX
	ld	[%sp + STACK_START + 32], Y
	ld	[%sp + STACK_START + 36], INCY
#else
	ld	[%sp+  STACK_START + 28], Y
	ld	[%sp+  STACK_START + 32], INCY
#endif
#else
	ldx	[%sp+  STACK_START + 56], Y
	ldx	[%sp+  STACK_START + 64], INCY
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

	deccc	I
	ble,pn	%icc, .LL12
	nop

.LL11:
	prefetch [X  + PREFETCHSIZE * SIZE], 0
	deccc	I

	STF	a1, [Y +  0 * SIZE]
	LDF	[X +  8 * SIZE], a1
	STF	b1, [X +  0 * SIZE]
	LDF	[Y +  8 * SIZE], b1

	STF	a2, [Y +  1 * SIZE]
	LDF	[X +  9 * SIZE], a2
	STF	b2, [X +  1 * SIZE]
	LDF	[Y +  9 * SIZE], b2

	STF	a3, [Y +  2 * SIZE]
	LDF	[X + 10 * SIZE], a3
	STF	b3, [X +  2 * SIZE]
	LDF	[Y + 10 * SIZE], b3

	STF	a4, [Y +  3 * SIZE]
	LDF	[X + 11 * SIZE], a4
	STF	b4, [X +  3 * SIZE]
	LDF	[Y + 11 * SIZE], b4

	prefetch [Y  + PREFETCHSIZE * SIZE], 0
	add	X, 8 * SIZE, X

	STF	a5, [Y +  4 * SIZE]
	LDF	[X +  4 * SIZE], a5
	STF	b5, [X -  4 * SIZE]
	LDF	[Y + 12 * SIZE], b5

	STF	a6, [Y +  5 * SIZE]
	LDF	[X +  5 * SIZE], a6
	STF	b6, [X -  3 * SIZE]
	LDF	[Y + 13 * SIZE], b6

	STF	a7, [Y +  6 * SIZE]
	LDF	[X +  6 * SIZE], a7
	STF	b7, [X -  2 * SIZE]
	LDF	[Y + 14 * SIZE], b7

	STF	a8, [Y +  7 * SIZE]
	LDF	[X +  7 * SIZE], a8
	STF	b8, [X -  1 * SIZE]
	LDF	[Y + 15 * SIZE], b8

	bg,pt	%icc, .LL11
	add	Y, 8 * SIZE, Y

.LL12:
	STF	a1, [Y +  0 * SIZE]
	STF	b1, [X +  0 * SIZE]
	STF	a2, [Y +  1 * SIZE]
	STF	b2, [X +  1 * SIZE]
	STF	a3, [Y +  2 * SIZE]
	STF	b3, [X +  2 * SIZE]
	STF	a4, [Y +  3 * SIZE]
	STF	b4, [X +  3 * SIZE]
	STF	a5, [Y +  4 * SIZE]
	STF	b5, [X +  4 * SIZE]
	STF	a6, [Y +  5 * SIZE]
	STF	b6, [X +  5 * SIZE]
	STF	a7, [Y +  6 * SIZE]
	STF	b7, [X +  6 * SIZE]
	STF	a8, [Y +  7 * SIZE]
	STF	b8, [X +  7 * SIZE]
	add	X, 8 * SIZE, X
	add	Y, 8 * SIZE, Y

.LL15:
	and	N, 7, I
	cmp	I,  0
	ble,a,pn %icc, .LL19
	nop

.LL16:
	LDF	[X +  0 * SIZE], a1
	add	I, -1, I
	LDF	[Y +  0 * SIZE], b1
	cmp	I, 0
	STF	a1, [Y +  0 * SIZE]
	add	Y, 1 * SIZE, Y
	STF	b1, [X +  0 * SIZE]
	bg,pt	%icc, .LL16
	add	X, 1 * SIZE, X

.LL19:
	return	%i7 + 8
	clr	%g0

.LL50:
	sra	N, 3, I
	mov	X, XX
	cmp	I, 0
	ble,pn	%icc, .LL55
	mov	Y, YY

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

	STF	a1, [YY +  0 * SIZE]
	add	I, -1, I
	add	YY, INCY, YY
	STF	b1, [XX +  0 * SIZE]
	cmp	I, 0
	add	XX, INCX, XX
	STF	a2, [YY +  0 * SIZE]
	add	YY, INCY, YY
	STF	b2, [XX +  0 * SIZE]
	add	XX, INCX, XX
	STF	a3, [YY +  0 * SIZE]
	add	YY, INCY, YY
	STF	b3, [XX +  0 * SIZE]
	add	XX, INCX, XX
	STF	a4, [YY +  0 * SIZE]
	add	YY, INCY, YY
	STF	b4, [XX +  0 * SIZE]
	add	XX, INCX, XX
	STF	a5, [YY +  0 * SIZE]
	add	YY, INCY, YY
	STF	b5, [XX +  0 * SIZE]
	add	XX, INCX, XX
	STF	a6, [YY +  0 * SIZE]
	add	YY, INCY, YY
	STF	b6, [XX +  0 * SIZE]
	add	XX, INCX, XX
	STF	a7, [YY +  0 * SIZE]
	add	YY, INCY, YY
	STF	b7, [XX +  0 * SIZE]
	add	XX, INCX, XX
	STF	a8, [YY +  0 * SIZE]
	add	YY, INCY, YY
	STF	b8, [XX +  0 * SIZE]

	bg,pt	%icc, .LL51
	add	XX, INCX, XX

.LL55:
	and	N, 7, I
	cmp	I,  0
	ble,a,pn %icc, .LL59
	nop

.LL56:
	LDF	[X +  0 * SIZE], a1
	LDF	[Y +  0 * SIZE], b1
	add	I, -1, I
	cmp	I, 0
	STF	b1, [X +  0 * SIZE]
	add	X, INCX, X
	STF	a1, [Y +  0 * SIZE]
	bg,pt	%icc, .LL56
	add	Y, INCY, Y

.LL59:
	return	%i7 + 8
	clr	%o0

	EPILOGUE
