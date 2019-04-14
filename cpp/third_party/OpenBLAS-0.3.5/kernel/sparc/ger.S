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

#define M	%i0
#define N	%i1

#if defined(DOUBLE) && !defined(__64BIT__)
#define X	%i5
#define INCX	%i2
#define Y	%i3
#define INCY	%i4
#else
#define X	%i4
#define INCX	%i5
#define Y	%i2
#define INCY	%i3
#endif

#define A	%l0
#define LDA	%l1
#define BUFFER	%l2

#define I	%l3
#define J	%l4

#define A1	%o0
#define X1	%o2
#define XX	%o3

#ifdef DOUBLE
#define t1	%f0
#define	t2 	%f2
#define t3	%f4
#define	t4 	%f6

#define x1	%f8
#define x2	%f10
#define x3	%f12
#define x4	%f14
#define x5	%f16
#define x6	%f18
#define x7	%f20
#define x8	%f22

#define a1	%f24
#define a2	%f26
#define a3	%f28
#define a4	%f30
#define a5	%f32
#define a6	%f34
#define a7	%f36
#define a8	%f38

#define a9	%f40
#define a10	%f42
#define a11	%f44
#define a12	%f46
#define a13	%f48
#define a14	%f50
#define a15	%f52
#define a16	%f54

#define y1	%f56
#define y2	%f58

#define ALPHA	%f60

#else
#define t1	%f0
#define	t2 	%f1
#define t3	%f2
#define	t4 	%f3

#define x1	%f4
#define x2	%f5
#define x3	%f6
#define x4	%f7
#define x5	%f8
#define x6	%f9
#define x7	%f10
#define x8	%f11

#define a1	%f12
#define a2	%f13
#define a3	%f14
#define a4	%f15
#define a5	%f16
#define a6	%f17
#define a7	%f18
#define a8	%f19

#define a9	%f20
#define a10	%f21
#define a11	%f22
#define a12	%f23
#define a13	%f24
#define a14	%f25
#define a15	%f26
#define a16	%f27

#define y1	%f28
#define y2	%f29
#define ALPHA	%f30
#endif

#define PREFETCHSIZE 60

	PROLOGUE
	SAVESP
	nop

#ifndef __64BIT__

#ifdef DOUBLE
	st	%i3, [%sp + STACK_START + 16]
	st	%i4, [%sp + STACK_START + 20]

	ld	[%sp + STACK_START + 28], INCX
	ld	[%sp + STACK_START + 32], Y
	ld	[%sp + STACK_START + 36], INCY
	ld	[%sp + STACK_START + 40], A
	ld	[%sp + STACK_START + 44], LDA
	ld	[%sp + STACK_START + 48], BUFFER
#else
	st	%i3, [%sp + STACK_START + 16]

	ld	[%sp + STACK_START + 28], Y
	ld	[%sp + STACK_START + 32], INCY
	ld	[%sp + STACK_START + 36], A
	ld	[%sp + STACK_START + 40], LDA
	ld	[%sp + STACK_START + 44], BUFFER
#endif
	LDF	[%sp + STACK_START + 16], ALPHA
#else
	ldx	[%sp + STACK_START + 56], Y
	ldx	[%sp + STACK_START + 64], INCY
	ldx	[%sp + STACK_START + 72], A
	ldx	[%sp + STACK_START + 80], LDA
	ldx	[%sp + STACK_START + 88], BUFFER
#ifdef DOUBLE
	FMOV	%f6, ALPHA
#else
	FMOV	%f7, ALPHA
#endif
#endif

	sll	LDA, BASE_SHIFT, LDA

	cmp	M, 0
	ble	%icc, .LL999
	sll	INCX, BASE_SHIFT, INCX
	cmp	N, 0
	ble	%icc, .LL999
	sll	INCY, BASE_SHIFT, INCY

	cmp	INCX, SIZE
	be	%icc, .LL10
	mov	X, XX

	mov	BUFFER, XX
	mov	BUFFER, X1

	sra	M, 3, J
	cmp	J, 0
	ble,pn	%icc, .LL05
	nop

.LL01:
	LDF	[X], a1
	add	X,  INCX, X
	LDF	[X], a2
	add	X,  INCX, X
	LDF	[X], a3
	add	X,  INCX, X
	LDF	[X], a4
	add	X,  INCX, X
	LDF	[X], a5
	add	X,  INCX, X
	LDF	[X], a6
	add	X,  INCX, X
	LDF	[X], a7
	add	X,  INCX, X
	LDF	[X], a8
	add	X,  INCX, X

	STF	a1, [X1 +  0 * SIZE]
	STF	a2, [X1 +  1 * SIZE]
	STF	a3, [X1 +  2 * SIZE]
	STF	a4, [X1 +  3 * SIZE]
	STF	a5, [X1 +  4 * SIZE]
	STF	a6, [X1 +  5 * SIZE]
	STF	a7, [X1 +  6 * SIZE]
	STF	a8, [X1 +  7 * SIZE]

	add	X1, 8 * SIZE, X1

	deccc	J
	bg,pn	%icc, .LL01
	nop

.LL05:
	andcc	M, 7, J
	ble,pn	%icc, .LL10
	nop

.LL06:
	LDF	[X], a1
	add	X,  INCX, X

	STF	a1, [X1 +  0 * SIZE]
	add	X1, 1 * SIZE, X1

	deccc	J
	bg,pn	%icc, .LL06
	nop

.LL10:
	mov	N, J
	cmp	N, 0
	ble,pn	%icc, .LL999
	nop

.LL11:
	mov	XX, X1

	mov	A,  A1
	add	A, LDA, A

	LDF	[Y], y1
	add	Y, INCY, Y

	FMUL	ALPHA, y1, y1

	sra	M, 3, I
	cmp	I, 0
	ble,pn	%icc, .LL15
	nop

	LDF	[X1 + 0 * SIZE], x1
	LDF	[A1 + 0 * SIZE], a1
	LDF	[X1 + 1 * SIZE], x2
	LDF	[A1 + 1 * SIZE], a2
	LDF	[X1 + 2 * SIZE], x3
	LDF	[A1 + 2 * SIZE], a3
	LDF	[X1 + 3 * SIZE], x4
	LDF	[A1 + 3 * SIZE], a4

	LDF	[X1 + 4 * SIZE], x5
	LDF	[A1 + 4 * SIZE], a5
	LDF	[X1 + 5 * SIZE], x6
	LDF	[A1 + 5 * SIZE], a6
	LDF	[X1 + 6 * SIZE], x7
	LDF	[A1 + 6 * SIZE], a7
	LDF	[X1 + 7 * SIZE], x8
	LDF	[A1 + 7 * SIZE], a8

	FMUL	x1,  y1, t1
	FMUL	x2,  y1, t2
	FMUL	x3,  y1, t3
	FMUL	x4,  y1, t4

	FADD	a1,  t1, a1
	FMUL	x5,  y1, t1
	FADD	a2,  t2, a2
	FMUL	x6,  y1, t2

	deccc	I
	ble,pn	%icc, .LL13
	nop

.LL12:
	prefetch  [A1 +  PREFETCHSIZE * SIZE], 0

	FADD	a3,  t3, a3
	LDF	[X1 +  8 * SIZE], x1
	FMUL	x7,  y1, t3
	LDF	[X1 +  9 * SIZE], x2
	FADD	a4,  t4, a4
	LDF	[X1 + 10 * SIZE], x3
	FMUL	x8,  y1, t4
	LDF	[X1 + 11 * SIZE], x4

	FADD	a5,  t1, a5
	STF	a1,  [A1 + 0 * SIZE]
	LDF	[A1 +  8 * SIZE], a1
	FMUL	x1,  y1, t1
	STF	a2,  [A1 + 1 * SIZE]
	LDF	[A1 +  9 * SIZE], a2

	FADD	a6,  t2, a6
	STF	a3,  [A1 + 2 * SIZE]
	LDF	[A1 + 10 * SIZE], a3
	FMUL	x2,  y1, t2
	STF	a4,  [A1 + 3 * SIZE]
	LDF	[A1 + 11 * SIZE], a4

	FADD	a7,  t3, a7
	LDF	[X1 + 12 * SIZE], x5
	FMUL	x3,  y1, t3
	LDF	[X1 + 13 * SIZE], x6
	FADD	a8,  t4, a8
	LDF	[X1 + 14 * SIZE], x7
	FMUL	x4,  y1, t4
	LDF	[X1 + 15 * SIZE], x8

	FADD	a1,  t1, a1
	STF	a5,  [A1 + 4 * SIZE]
	deccc	I
	LDF	[A1 + 12 * SIZE], a5
	FMUL	x5,  y1, t1
	STF	a6,  [A1 + 5 * SIZE]
	LDF	[A1 + 13 * SIZE], a6
	FADD	a2,  t2, a2
	STF	a7,  [A1 + 6 * SIZE]
	LDF	[A1 + 14 * SIZE], a7
	FMUL	x6,  y1, t2
	STF	a8,  [A1 + 7 * SIZE]
	LDF	[A1 + 15 * SIZE], a8
	add	A1, 8 * SIZE, A1

	bg,pn	%icc, .LL12
	add	X1, 8 * SIZE, X1

.LL13:
	FADD	a3,  t3, a3
	FMUL	x7,  y1, t3
	FADD	a4,  t4, a4
	FMUL	x8,  y1, t4

	FADD	a5,  t1, a5
	FADD	a6,  t2, a6
	FADD	a7,  t3, a7
	FADD	a8,  t4, a8

	STF	a1,  [A1 + 0 * SIZE]
	STF	a2,  [A1 + 1 * SIZE]
	STF	a3,  [A1 + 2 * SIZE]
	STF	a4,  [A1 + 3 * SIZE]

	STF	a5,  [A1 + 4 * SIZE]
	STF	a6,  [A1 + 5 * SIZE]
	STF	a7,  [A1 + 6 * SIZE]
	STF	a8,  [A1 + 7 * SIZE]

	add	A1, 8 * SIZE, A1
	add	X1, 8 * SIZE, X1

.LL15:
	andcc	M, 4, I
	ble,pn	%icc, .LL16
	nop

	LDF	[X1 + 0 * SIZE], x1
	LDF	[A1 + 0 * SIZE], a1
	LDF	[X1 + 1 * SIZE], x2
	LDF	[A1 + 1 * SIZE], a2

	LDF	[X1 + 2 * SIZE], x3
	LDF	[A1 + 2 * SIZE], a3
	LDF	[X1 + 3 * SIZE], x4
	LDF	[A1 + 3 * SIZE], a4

	FMUL	x1,  y1, t1
	FMUL	x2,  y1, t2
	FMUL	x3,  y1, t3
	FMUL	x4,  y1, t4

	FADD	a1,  t1, a1
	FADD	a2,  t2, a2
	FADD	a3,  t3, a3
	FADD	a4,  t4, a4

	STF	a1,  [A1 + 0 * SIZE]
	STF	a2,  [A1 + 1 * SIZE]
	STF	a3,  [A1 + 2 * SIZE]
	add	X1, 4 * SIZE, X1
	STF	a4,  [A1 + 3 * SIZE]
	add	A1, 4 * SIZE, A1

.LL16:
	andcc	M, 2, I
	ble,pn	%icc, .LL17
	nop

	LDF	[X1 + 0 * SIZE], x1
	LDF	[X1 + 1 * SIZE], x2
	LDF	[A1 + 0 * SIZE], a1
	LDF	[A1 + 1 * SIZE], a2

	FMUL	x1,  y1, t1
	FMUL	x2,  y1, t2

	FADD	a1,  t1, a1
	FADD	a2,  t2, a2

	STF	a1, [A1 + 0 * SIZE]
	add	X1, 2 * SIZE, X1
	STF	a2, [A1 + 1 * SIZE]
	add	A1, 2 * SIZE, A1

.LL17:
	andcc	M, 1, I
	ble,pn	%icc, .LL19
	nop

	LDF	[X1 + 0 * SIZE], x1
	add	X1, 1 * SIZE, X1

	LDF	[A1 + 0 * SIZE], a1

	FMUL	x1,  y1, t1
	FADD	a1,  t1, a1

	STF	a1, [A1 + 0 * SIZE]
	add	A1, 1 * SIZE, A1

.LL19:
	deccc	J
	bg	%icc, .LL11
	nop

.LL999:
	return	%i7 + 8
	clr	%o0

	EPILOGUE
