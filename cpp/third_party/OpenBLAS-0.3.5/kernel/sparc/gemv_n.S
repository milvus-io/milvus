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
#define A	%i5
#define LDA	%i2
#define X	%i3
#define INCX	%i4
#else
#define A	%i4
#define LDA	%i5
#define X	%i2
#define INCX	%i3
#endif

#define Y	%l0
#define INCY	%l1
#define BUFFER	%l2

#define I	%l3
#define J	%l5

#define A1	%o0
#define A2	%o1
#define A3	%o2
#define A4	%o3

#define Y1	%l4
#define YY	%l6

#ifdef DOUBLE
#define t1	%f0
#define	t2 	%f2
#define t3	%f4
#define	t4 	%f6

#define y1	%f8
#define y2	%f10
#define y3	%f12
#define y4	%f14
#define y5	%f16
#define y6	%f18
#define y7	%f20
#define y8	%f22

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

#define x1	%f56
#define x2	%f58
#define x3	%f60
#define x4	%f62

#define FZERO	%f52
#define ALPHA	%f54
#else
#define t1	%f0
#define	t2 	%f1
#define t3	%f2
#define	t4 	%f3

#define y1	%f4
#define y2	%f5
#define y3	%f6
#define y4	%f7
#define y5	%f8
#define y6	%f9
#define y7	%f10
#define y8	%f11

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

#define x1	%f28
#define x2	%f29
#define x3	%f30
#define x4	%f31

#define FZERO	%f26
#define ALPHA	%f27
#endif

#ifndef __64BIT__
#define STACK_FZERO	[%sp + STACK_START +  8]
#define STACK_ALPHA	[%sp + STACK_START + 16]
#else
#define STACK_FZERO	[%sp + STACK_START + 32]
#define STACK_ALPHA	[%sp + STACK_START + 40]
#endif

	PROLOGUE
	SAVESP

#ifndef __64BIT__
#ifdef DOUBLE
	st	%i3, [%sp + STACK_START + 16]   /* ALPHA */
	st	%i4, [%sp + STACK_START + 20]

	ld	[%sp + STACK_START + 28], LDA
	ld	[%sp + STACK_START + 32], X
	ld	[%sp + STACK_START + 36], INCX
	ld	[%sp + STACK_START + 40], Y
	ld	[%sp + STACK_START + 44], INCY
	ld	[%sp + STACK_START + 48], BUFFER
#else
	st	%i3, [%sp + STACK_START + 16]   /* ALPHA */

	ld	[%sp + STACK_START + 28], X
	ld	[%sp + STACK_START + 32], INCX
	ld	[%sp + STACK_START + 36], Y
	ld	[%sp + STACK_START + 40], INCY
	ld	[%sp + STACK_START + 44], BUFFER
#endif
	LDF	[%sp + STACK_START + 16], ALPHA
#else
	ldx	[%sp + STACK_START + 56], X
	ldx	[%sp + STACK_START + 64], INCX
	ldx	[%sp + STACK_START + 72], Y
	ldx	[%sp + STACK_START + 80], INCY
	ldx	[%sp + STACK_START + 88], BUFFER
#ifdef DOUBLE
	FMOV	%f6, ALPHA
	STF	%f6, STACK_ALPHA
#else
	FMOV	%f7, ALPHA
	STF	%f7, STACK_ALPHA
#endif
#endif

	sll	LDA, BASE_SHIFT, LDA

	cmp	M, 0
	ble	%icc, .LL999
	sll	INCX, BASE_SHIFT, INCX
	cmp	N, 0
	ble	%icc, .LL999
	sll	INCY, BASE_SHIFT, INCY

#ifdef DOUBLE
	FCLR(21)
#else
	FCLR(26)
#endif

	cmp	INCY, SIZE
	be	%icc, .LL10
	mov	Y, YY

	add	M, 7, J
	sra	J, 3, J
	mov	BUFFER, YY
	mov	BUFFER, Y1

.LL01:
	STF	FZERO, [Y1 +  0 * SIZE]
	STF	FZERO, [Y1 +  1 * SIZE]
	STF	FZERO, [Y1 +  2 * SIZE]
	STF	FZERO, [Y1 +  3 * SIZE]
	STF	FZERO, [Y1 +  4 * SIZE]
	STF	FZERO, [Y1 +  5 * SIZE]
	STF	FZERO, [Y1 +  6 * SIZE]
	deccc	J
	STF	FZERO, [Y1 +  7 * SIZE]
	bg,pn	%icc, .LL01
	add	Y1, 8 * SIZE, Y1

.LL10:
	sra	N, 2, J
	cmp	J, 0
	ble,pn	%icc, .LL20
	nop

.LL11:
	mov	YY, Y1

	mov	A,  A1
	add	A,  LDA, A2
	add	A2, LDA, A3
	add	A3, LDA, A4
	add	A4, LDA, A

	LDF	STACK_ALPHA, ALPHA

	LDF	[X], x1
	add	X, INCX, X
	LDF	[X], x2
	add	X, INCX, X
	LDF	[X], x3
	add	X, INCX, X
	LDF	[X], x4
	add	X, INCX, X

	FMUL	ALPHA, x1, x1
	FMUL	ALPHA, x2, x2
	FMUL	ALPHA, x3, x3
	FMUL	ALPHA, x4, x4

	sra	M, 3, I
	cmp	I, 0
	ble,pn	%icc, .LL16
	nop

	LDF	[A1 + 0 * SIZE], a1
	LDF	[A1 + 1 * SIZE], a2
	LDF	[A1 + 2 * SIZE], a3
	LDF	[A1 + 3 * SIZE], a4
	LDF	[A1 + 4 * SIZE], a5
	LDF	[A1 + 5 * SIZE], a6
	LDF	[A1 + 6 * SIZE], a7
	LDF	[A1 + 7 * SIZE], a8

	LDF	[A2 + 0 * SIZE], a9
	LDF	[A2 + 1 * SIZE], a10
	LDF	[A2 + 2 * SIZE], a11
	LDF	[A2 + 3 * SIZE], a12
	LDF	[A2 + 4 * SIZE], a13
	LDF	[A2 + 5 * SIZE], a14
	LDF	[A2 + 6 * SIZE], a15
	LDF	[A2 + 7 * SIZE], a16

	FMUL	a1,  x1, t1
	LDF	[A3 + 0 * SIZE], a1
	FMUL	a2,  x1, t2
	LDF	[A3 + 1 * SIZE], a2
	FMUL	a3,  x1, t3
	LDF	[A3 + 2 * SIZE], a3
	FMUL	a4,  x1, t4
	LDF	[A3 + 3 * SIZE], a4

	deccc	I
	ble,pn	%icc, .LL13
	nop
	nop
	nop
	nop

#ifdef DOUBLE
#define PREFETCHSIZE 20
#else
#define PREFETCHSIZE 40
#endif

.LL12:
	LDF	[Y1 +  0 * SIZE], y1
	LDF	[Y1 +  1 * SIZE], y2
	LDF	[Y1 +  2 * SIZE], y3
	LDF	[Y1 +  3 * SIZE], y4
	LDF	[Y1 +  4 * SIZE], y5
	LDF	[Y1 +  5 * SIZE], y6
	LDF	[Y1 +  6 * SIZE], y7
	LDF	[Y1 +  7 * SIZE], y8

	FADD	y1,  t1, y1
	prefetch  [A1 +  PREFETCHSIZE * SIZE], 1
	FMUL	a5,  x1, t1
	LDF	[A3 + 4 * SIZE], a5

	FADD	y2,  t2, y2
	nop
	FMUL	a6,  x1, t2
	LDF	[A3 + 5 * SIZE], a6

	FADD	y3,  t3, y3
	nop
	FMUL	a7,  x1, t3
	LDF	[A3 + 6 * SIZE], a7

	FADD	y4,  t4, y4
	nop
	FMUL	a8,  x1, t4
	LDF	[A3 + 7 * SIZE], a8

	FADD	y5,  t1, y5
	nop
	FMUL	a9,  x2, t1
	LDF	[A4 +  0 * SIZE], a9

	FADD	y6,  t2, y6
	nop
	FMUL	a10, x2, t2
	LDF	[A4 +  1 * SIZE], a10

	FADD	y7,  t3, y7
	nop
	FMUL	a11, x2, t3
	LDF	[A4 +  2 * SIZE], a11

	FADD	y8,  t4, y8
	nop
	FMUL	a12, x2, t4
	LDF	[A4 +  3 * SIZE], a12

	FADD	y1,  t1, y1
	prefetch  [A2 +  PREFETCHSIZE * SIZE], 1
	FMUL	a13, x2, t1
	LDF	[A4 +  4 * SIZE], a13

	FADD	y2,  t2, y2
	nop
	FMUL	a14, x2, t2
	LDF	[A4 +  5 * SIZE], a14

	FADD	y3,  t3, y3
	nop
	FMUL	a15, x2, t3
	LDF	[A4 +  6 * SIZE], a15

	FADD	y4,  t4, y4
	nop
	FMUL	a16, x2, t4
	LDF	[A4 +  7 * SIZE], a16

	FADD	y5,  t1, y5
	nop
	FMUL	a1,  x3, t1
	LDF	[A1 +  8 * SIZE], a1

	FADD	y6,  t2, y6
	nop
	FMUL	a2,  x3, t2
	LDF	[A1 +  9 * SIZE], a2

	FADD	y7,  t3, y7
	nop
	FMUL	a3,  x3, t3
	LDF	[A1 + 10 * SIZE], a3

	FADD	y8,  t4, y8
	nop
	FMUL	a4,  x3, t4
	LDF	[A1 + 11 * SIZE], a4

	FADD	y1,  t1, y1
	prefetch  [A3 +  PREFETCHSIZE * SIZE], 1
	FMUL	a5,  x3, t1
	LDF	[A1 + 12 * SIZE], a5

	FADD	y2,  t2, y2
	nop
	FMUL	a6,  x3, t2
	LDF	[A1 + 13 * SIZE], a6

	FADD	y3,  t3, y3
	nop
	FMUL	a7,  x3, t3
	LDF	[A1 + 14 * SIZE], a7

	FADD	y4,  t4, y4
	nop
	FMUL	a8,  x3, t4
	LDF	[A1 + 15 * SIZE], a8

	FADD	y5,  t1, y5
	nop
	FMUL	a9,  x4, t1
	LDF	[A2 +  8 * SIZE], a9

	FADD	y6,  t2, y6
	nop
	FMUL	a10, x4, t2
	LDF	[A2 +  9 * SIZE], a10

	FADD	y7,  t3, y7
	nop
	FMUL	a11, x4, t3
	LDF	[A2 + 10 * SIZE], a11

	FADD	y8,  t4, y8
	nop
	FMUL	a12, x4, t4
	LDF	[A2 + 11 * SIZE], a12

	FADD	y1,  t1, y1
	prefetch  [A4 +  PREFETCHSIZE * SIZE], 1
	FMUL	a13, x4, t1
	LDF	[A2 + 12 * SIZE], a13

	FADD	y2,  t2, y2
	add	A3, 8 * SIZE, A3
	FMUL	a14, x4, t2
	LDF	[A2 + 13 * SIZE], a14

	FADD	y3,  t3, y3
	add	Y1, 8 * SIZE, Y1
	FMUL	a15, x4, t3
	LDF	[A2 + 14 * SIZE], a15

	FADD	y4,  t4, y4
	deccc	I
	FMUL	a16, x4, t4
	LDF	[A2 + 15 * SIZE], a16

	FADD	y5,  t1, y5
	add	A1, 8 * SIZE, A1
	FMUL	a1,  x1, t1
	LDF	[A3 +  0 * SIZE], a1

	FADD	y6,  t2, y6
	add	A2, 8 * SIZE, A2
	FMUL	a2,  x1, t2
	LDF	[A3 +  1 * SIZE], a2

	FADD	y7,  t3, y7
	add	A4, 8 * SIZE, A4
	FMUL	a3,  x1, t3
	LDF	[A3 +  2 * SIZE], a3

	FADD	y8,  t4, y8
	nop
	FMUL	a4,  x1, t4
	LDF	[A3 +  3 * SIZE], a4

	STF	y1, [Y1 - 8 * SIZE]
	STF	y2, [Y1 - 7 * SIZE]
	STF	y3, [Y1 - 6 * SIZE]
	STF	y4, [Y1 - 5 * SIZE]

	STF	y5, [Y1 - 4 * SIZE]
	STF	y6, [Y1 - 3 * SIZE]
	STF	y7, [Y1 - 2 * SIZE]

	bg,pn	%icc, .LL12
	STF	y8, [Y1 - 1 * SIZE]

.LL13:
	LDF	[Y1 +  0 * SIZE], y1
	LDF	[Y1 +  1 * SIZE], y2
	LDF	[Y1 +  2 * SIZE], y3
	LDF	[Y1 +  3 * SIZE], y4
	LDF	[Y1 +  4 * SIZE], y5
	LDF	[Y1 +  5 * SIZE], y6
	LDF	[Y1 +  6 * SIZE], y7
	LDF	[Y1 +  7 * SIZE], y8

	FADD	y1,  t1, y1
	FMUL	a5,  x1, t1
	LDF	[A3 + 0 * SIZE], a1
	FADD	y2,  t2, y2
	FMUL	a6,  x1, t2
	LDF	[A3 + 1 * SIZE], a2

	FADD	y3,  t3, y3
	FMUL	a7,  x1, t3
	LDF	[A3 + 2 * SIZE], a3
	FADD	y4,  t4, y4
	FMUL	a8,  x1, t4
	LDF	[A3 + 3 * SIZE], a4

	FADD	y5,  t1, y5
	FMUL	a9,  x2, t1
	LDF	[A3 + 4 * SIZE], a5
	FADD	y6,  t2, y6
	FMUL	a10, x2, t2
	LDF	[A3 + 5 * SIZE], a6

	FADD	y7,  t3, y7
	FMUL	a11, x2, t3
	LDF	[A3 + 6 * SIZE], a7
	FADD	y8,  t4, y8
	FMUL	a12, x2, t4
	LDF	[A3 + 7 * SIZE], a8

	FADD	y1,  t1, y1
	FMUL	a13, x2, t1
	LDF	[A4 + 0 * SIZE], a9
	FADD	y2,  t2, y2
	FMUL	a14, x2, t2
	LDF	[A4 + 1 * SIZE], a10

	FADD	y3,  t3, y3
	FMUL	a15, x2, t3
	LDF	[A4 + 2 * SIZE], a11
	FADD	y4,  t4, y4
	FMUL	a16, x2, t4
	LDF	[A4 + 3 * SIZE], a12

	FADD	y5,  t1, y5
	FMUL	a1,  x3, t1
	LDF	[A4 + 4 * SIZE], a13
	FADD	y6,  t2, y6
	FMUL	a2,  x3, t2
	LDF	[A4 + 5 * SIZE], a14

	FADD	y7,  t3, y7
	FMUL	a3,  x3, t3
	LDF	[A4 + 6 * SIZE], a15
	FADD	y8,  t4, y8
	FMUL	a4,  x3, t4
	LDF	[A4 + 7 * SIZE], a16

	FADD	y1,  t1, y1
	FMUL	a5,  x3, t1
	FADD	y2,  t2, y2
	FMUL	a6,  x3, t2

	FADD	y3,  t3, y3
	FMUL	a7,  x3, t3
	FADD	y4,  t4, y4
	FMUL	a8,  x3, t4

	FADD	y5,  t1, y5
	FMUL	a9,  x4, t1
	FADD	y6,  t2, y6
	FMUL	a10, x4, t2

	FADD	y7,  t3, y7
	FMUL	a11, x4, t3
	FADD	y8,  t4, y8
	FMUL	a12, x4, t4

	FADD	y1,  t1, y1
	FMUL	a13, x4, t1
	FADD	y2,  t2, y2
	FMUL	a14, x4, t2

	FADD	y3,  t3, y3
	FMUL	a15, x4, t3
	FADD	y4,  t4, y4
	FMUL	a16, x4, t4
	add	A4, 8 * SIZE, A4

	STF	y1, [Y1 + 0 * SIZE]
	FADD	y5,  t1, y5
	STF	y2, [Y1 + 1 * SIZE]
	FADD	y6,  t2, y6
	STF	y3, [Y1 + 2 * SIZE]
	FADD	y7,  t3, y7
	STF	y4, [Y1 + 3 * SIZE]
	FADD	y8,  t4, y8

	STF	y5, [Y1 + 4 * SIZE]
	add	A1, 8 * SIZE, A1
	STF	y6, [Y1 + 5 * SIZE]
	add	A2, 8 * SIZE, A2
	STF	y7, [Y1 + 6 * SIZE]
	add	A3, 8 * SIZE, A3
	STF	y8, [Y1 + 7 * SIZE]
	add	Y1, 8 * SIZE, Y1

.LL16:
	andcc	M, 4, I
	ble,pn	%icc, .LL17
	nop

	LDF	[A1 + 0 * SIZE], a1
	LDF	[A1 + 1 * SIZE], a2
	LDF	[A1 + 2 * SIZE], a3
	LDF	[A1 + 3 * SIZE], a4

	LDF	[A2 + 0 * SIZE], a5
	LDF	[A2 + 1 * SIZE], a6
	LDF	[A2 + 2 * SIZE], a7
	LDF	[A2 + 3 * SIZE], a8

	LDF	[A3 + 0 * SIZE], a9
	LDF	[A3 + 1 * SIZE], a10
	LDF	[A3 + 2 * SIZE], a11
	LDF	[A3 + 3 * SIZE], a12

	LDF	[A4 + 0 * SIZE], a13
	LDF	[A4 + 1 * SIZE], a14
	LDF	[A4 + 2 * SIZE], a15
	LDF	[A4 + 3 * SIZE], a16

	LDF	[Y1 + 0 * SIZE], y1
	add	A1, 4 * SIZE, A1
	LDF	[Y1 + 1 * SIZE], y2
	add	A2, 4 * SIZE, A2
	LDF	[Y1 + 2 * SIZE], y3
	add	A3, 4 * SIZE, A3
	LDF	[Y1 + 3 * SIZE], y4
	add	A4, 4 * SIZE, A4

	FMUL	a1,  x1, t1
	FMUL	a2,  x1, t2
	FMUL	a3,  x1, t3
	FMUL	a4,  x1, t4

	FADD	y1,  t1, y1
	FMUL	a5,  x2, t1
	FADD	y2,  t2, y2
	FMUL	a6,  x2, t2
	FADD	y3,  t3, y3
	FMUL	a7,  x2, t3
	FADD	y4,  t4, y4
	FMUL	a8,  x2, t4

	FADD	y1,  t1, y1
	FMUL	a9,  x3, t1
	FADD	y2,  t2, y2
	FMUL	a10, x3, t2

	FADD	y3,  t3, y3
	FMUL	a11, x3, t3
	FADD	y4,  t4, y4
	FMUL	a12, x3, t4

	FADD	y1,  t1, y1
	FMUL	a13, x4, t1
	FADD	y2,  t2, y2
	FMUL	a14, x4, t2

	FADD	y3,  t3, y3
	FMUL	a15, x4, t3
	FADD	y4,  t4, y4
	FMUL	a16, x4, t4

	FADD	y1,  t1, y1
	FADD	y2,  t2, y2
	FADD	y3,  t3, y3
	FADD	y4,  t4, y4

	STF	y1, [Y1 + 0 * SIZE]
	STF	y2, [Y1 + 1 * SIZE]
	STF	y3, [Y1 + 2 * SIZE]
	STF	y4, [Y1 + 3 * SIZE]

	add	Y1, 4 * SIZE, Y1

.LL17:
	andcc	M, 2, I
	ble,pn	%icc, .LL18
	nop

	LDF	[A1 + 0 * SIZE], a1
	LDF	[A2 + 0 * SIZE], a2
	LDF	[A3 + 0 * SIZE], a3
	LDF	[A4 + 0 * SIZE], a4
	LDF	[Y1 + 0 * SIZE], y1

	LDF	[A1 + 1 * SIZE], a5
	LDF	[A2 + 1 * SIZE], a6
	LDF	[A3 + 1 * SIZE], a7
	LDF	[A4 + 1 * SIZE], a8
	LDF	[Y1 + 1 * SIZE], y2

	add	A1, 2 * SIZE, A1
	add	A2, 2 * SIZE, A2
	add	A3, 2 * SIZE, A3
	add	A4, 2 * SIZE, A4

	FMUL	a1, x1, t1
	FMUL	a2, x2, t2
	FMUL	a3, x3, t3
	FMUL	a4, x4, t4

	FADD	y1, t1, y1
	FMUL	a5, x1, t1
	FADD	y1, t2, y1
	FMUL	a6, x2, t2
	FADD	y1, t3, y1
	FMUL	a7, x3, t3
	FADD	y1, t4, y1
	FMUL	a8, x4, t4

	FADD	y2, t1, y2
	FADD	y2, t2, y2
	FADD	y2, t3, y2
	FADD	y2, t4, y2

	STF	y1, [Y1 + 0 * SIZE]
	STF	y2, [Y1 + 1 * SIZE]
	add	Y1, 2 * SIZE, Y1

.LL18:
	andcc	M, 1, I
	ble,pn	%icc, .LL19
	nop

	LDF	[A1 + 0 * SIZE], a1
	LDF	[A2 + 0 * SIZE], a2
	LDF	[A3 + 0 * SIZE], a3
	LDF	[A4 + 0 * SIZE], a4
	LDF	[Y1 + 0 * SIZE], y1

	FMUL	a1, x1, t1
	FMUL	a2, x2, t2
	FMUL	a3, x3, t3
	FMUL	a4, x4, t4

	FADD	y1, t1, y1
	FADD	y1, t2, y1
	FADD	y1, t3, y1
	FADD	y1, t4, y1

	STF	y1, [Y1]

.LL19:
	deccc	J
	bg	%icc, .LL11
	nop

.LL20:
	andcc	N, 2, J
	ble,pn	%icc, .LL30
	nop

.LL21:
	mov	YY, Y1

	mov	A,  A1
	add	A,  LDA, A2
	add	A2, LDA, A

	LDF	STACK_ALPHA, ALPHA

	LDF	[X], x1
	add	X, INCX, X
	LDF	[X], x2
	add	X, INCX, X

	FMUL	ALPHA, x1, x1
	FMUL	ALPHA, x2, x2

	sra	M, 3, I
	cmp	I, 0
	ble,pn	%icc, .LL26
	nop

	LDF	[Y1 + 0 * SIZE], y1
	LDF	[Y1 + 1 * SIZE], y2
	LDF	[Y1 + 2 * SIZE], y3
	LDF	[Y1 + 3 * SIZE], y4
	LDF	[Y1 + 4 * SIZE], y5
	LDF	[Y1 + 5 * SIZE], y6
	LDF	[Y1 + 6 * SIZE], y7
	LDF	[Y1 + 7 * SIZE], y8

	LDF	[A1 + 0 * SIZE], a1
	LDF	[A1 + 1 * SIZE], a2
	LDF	[A1 + 2 * SIZE], a3
	LDF	[A1 + 3 * SIZE], a4
	LDF	[A1 + 4 * SIZE], a5
	LDF	[A1 + 5 * SIZE], a6
	LDF	[A1 + 6 * SIZE], a7
	LDF	[A1 + 7 * SIZE], a8

	LDF	[A2 + 0 * SIZE], a9
	LDF	[A2 + 1 * SIZE], a10
	LDF	[A2 + 2 * SIZE], a11
	LDF	[A2 + 3 * SIZE], a12
	LDF	[A2 + 4 * SIZE], a13
	LDF	[A2 + 5 * SIZE], a14
	LDF	[A2 + 6 * SIZE], a15
	LDF	[A2 + 7 * SIZE], a16

	FMUL	a1,  x1, t1
	deccc	I
	LDF	[A1 +  8 * SIZE], a1
	FMUL	a2,  x1, t2
	LDF	[A1 +  9 * SIZE], a2
	FMUL	a3,  x1, t3
	LDF	[A1 + 10 * SIZE], a3
	FMUL	a4,  x1, t4
	ble,pn	%icc, .LL23
	LDF	[A1 + 11 * SIZE], a4

.LL22:
	FADD	y1,  t1, y1
	prefetch  [A1 +  PREFETCHSIZE * SIZE], 1
	FMUL	a5,  x1, t1
	LDF	[A1 + 12 * SIZE], a5
	FADD	y2,  t2, y2
	FMUL	a6,  x1, t2
	LDF	[A1 + 13 * SIZE], a6

	FADD	y3,  t3, y3
	FMUL	a7,  x1, t3
	LDF	[A1 + 14 * SIZE], a7
	FADD	y4,  t4, y4
	FMUL	a8,  x1, t4
	LDF	[A1 + 15 * SIZE], a8

	FADD	y5,  t1, y5
	FMUL	a9,  x2, t1
	LDF	[A2 +  8 * SIZE], a9
	FADD	y6,  t2, y6
	FMUL	a10, x2, t2
	LDF	[A2 +  9 * SIZE], a10

	FADD	y7,  t3, y7
	FMUL	a11, x2, t3
	LDF	[A2 + 10 * SIZE], a11
	FADD	y8,  t4, y8
	FMUL	a12, x2, t4
	LDF	[A2 + 11 * SIZE], a12

	FADD	y1,  t1, y1
	prefetch  [A2 +  PREFETCHSIZE * SIZE], 1
	FMUL	a13, x2, t1
	LDF	[A2 + 12 * SIZE], a13
	FADD	y2,  t2, y2
	FMUL	a14, x2, t2
	LDF	[A2 + 13 * SIZE], a14

	FADD	y3,  t3, y3
	FMUL	a15, x2, t3
	LDF	[A2 + 14 * SIZE], a15
	FADD	y4,  t4, y4
	FMUL	a16, x2, t4
	LDF	[A2 + 15 * SIZE], a16

	FADD	y5,  t1, y5
	FMUL	a1,  x1, t1
	LDF	[A1 + 16 * SIZE], a1
	FADD	y6,  t2, y6
	FMUL	a2,  x1, t2
	LDF	[A1 + 17 * SIZE], a2

	FADD	y7,  t3, y7
	FMUL	a3,  x1, t3
	LDF	[A1 + 18 * SIZE], a3
	FADD	y8,  t4, y8
	FMUL	a4,  x1, t4
	LDF	[A1 + 19 * SIZE], a4

	STF	y1, [Y1 + 0 * SIZE]
	STF	y2, [Y1 + 1 * SIZE]
	STF	y3, [Y1 + 2 * SIZE]
	STF	y4, [Y1 + 3 * SIZE]
	STF	y5, [Y1 + 4 * SIZE]
	STF	y6, [Y1 + 5 * SIZE]
	STF	y7, [Y1 + 6 * SIZE]
	STF	y8, [Y1 + 7 * SIZE]

	LDF	[Y1 +  8 * SIZE], y1
	add	A1, 8 * SIZE, A1
	LDF	[Y1 +  9 * SIZE], y2
	add	A2, 8 * SIZE, A2
	LDF	[Y1 + 10 * SIZE], y3
	deccc	I
	LDF	[Y1 + 11 * SIZE], y4
	LDF	[Y1 + 12 * SIZE], y5
	LDF	[Y1 + 13 * SIZE], y6
	LDF	[Y1 + 14 * SIZE], y7
	LDF	[Y1 + 15 * SIZE], y8
	bg,pn	%icc, .LL22
	add	Y1, 8 * SIZE, Y1

.LL23:
	FADD	y1,  t1, y1
	FMUL	a5,  x1, t1
	FADD	y2,  t2, y2
	FMUL	a6,  x1, t2

	FADD	y3,  t3, y3
	FMUL	a7,  x1, t3
	FADD	y4,  t4, y4
	FMUL	a8,  x1, t4

	FADD	y5,  t1, y5
	FMUL	a9,  x2, t1
	FADD	y6,  t2, y6
	FMUL	a10, x2, t2

	FADD	y7,  t3, y7
	FMUL	a11, x2, t3
	FADD	y8,  t4, y8
	FMUL	a12, x2, t4

	FADD	y1,  t1, y1
	FMUL	a13, x2, t1
	FADD	y2,  t2, y2
	FMUL	a14, x2, t2

	FADD	y3,  t3, y3
	FMUL	a15, x2, t3
	FADD	y4,  t4, y4
	FMUL	a16, x2, t4

	STF	y1, [Y1 + 0 * SIZE]
	FADD	y5,  t1, y5
	STF	y2, [Y1 + 1 * SIZE]
	FADD	y6,  t2, y6
	STF	y3, [Y1 + 2 * SIZE]
	FADD	y7,  t3, y7
	STF	y4, [Y1 + 3 * SIZE]
	FADD	y8,  t4, y8

	STF	y5, [Y1 + 4 * SIZE]
	add	A1, 8 * SIZE, A1
	STF	y6, [Y1 + 5 * SIZE]
	add	A2, 8 * SIZE, A2
	STF	y7, [Y1 + 6 * SIZE]
	nop
	STF	y8, [Y1 + 7 * SIZE]
	add	Y1, 8 * SIZE, Y1

.LL26:
	andcc	M, 4, I
	ble,pn	%icc, .LL27
	nop

	LDF	[A1 + 0 * SIZE], a1
	LDF	[A1 + 1 * SIZE], a2
	LDF	[A1 + 2 * SIZE], a3
	LDF	[A1 + 3 * SIZE], a4

	LDF	[A2 + 0 * SIZE], a5
	LDF	[A2 + 1 * SIZE], a6
	LDF	[A2 + 2 * SIZE], a7
	LDF	[A2 + 3 * SIZE], a8

	LDF	[Y1 + 0 * SIZE], y1
	add	A1, 4 * SIZE, A1
	LDF	[Y1 + 1 * SIZE], y2
	add	A2, 4 * SIZE, A2
	LDF	[Y1 + 2 * SIZE], y3
	LDF	[Y1 + 3 * SIZE], y4

	FMUL	a1,  x1, t1
	FMUL	a2,  x1, t2
	FMUL	a3,  x1, t3
	FMUL	a4,  x1, t4

	FADD	y1,  t1, y1
	FMUL	a5,  x2, t1
	FADD	y2,  t2, y2
	FMUL	a6,  x2, t2
	FADD	y3,  t3, y3
	FMUL	a7,  x2, t3
	FADD	y4,  t4, y4
	FMUL	a8,  x2, t4

	FADD	y1,  t1, y1
	FADD	y2,  t2, y2
	FADD	y3,  t3, y3
	FADD	y4,  t4, y4

	STF	y1, [Y1 + 0 * SIZE]
	STF	y2, [Y1 + 1 * SIZE]
	STF	y3, [Y1 + 2 * SIZE]
	STF	y4, [Y1 + 3 * SIZE]

	add	Y1, 4 * SIZE, Y1

.LL27:
	andcc	M, 2, I
	ble,pn	%icc, .LL28
	nop

	LDF	[A1 + 0 * SIZE], a1
	LDF	[A2 + 0 * SIZE], a2
	LDF	[Y1 + 0 * SIZE], y1
	LDF	[A1 + 1 * SIZE], a5
	LDF	[A2 + 1 * SIZE], a6
	add	A1, 2 * SIZE, A1
	LDF	[Y1 + 1 * SIZE], y2
	add	A2, 2 * SIZE, A2

	FMUL	a1, x1, t1
	FMUL	a2, x2, t2

	FADD	y1, t1, y1
	FMUL	a5, x1, t1
	FADD	y1, t2, y1
	FMUL	a6, x2, t2

	FADD	y2, t1, y2
	FADD	y2, t2, y2

	STF	y1, [Y1 + 0 * SIZE]
	STF	y2, [Y1 + 1 * SIZE]
	add	Y1, 2 * SIZE, Y1

.LL28:
	andcc	M, 1, I
	ble,pn	%icc, .LL30
	nop

	LDF	[A1 + 0 * SIZE], a1
	LDF	[A2 + 0 * SIZE], a2
	LDF	[Y1 + 0 * SIZE], y1

	FMUL	a1, x1, t1
	FMUL	a2, x2, t2

	FADD	y1, t1, y1
	FADD	y1, t2, y1

	STF	y1, [Y1]

.LL30:
	andcc	N, 1, J
	ble,pn	%icc, .LL990
	nop

.LL31:
	mov	YY, Y1
	mov	A,  A1

	LDF	STACK_ALPHA, ALPHA

	LDF	[X], x1
	add	X, INCX, X

	FMUL	ALPHA, x1, x1

	sra	M, 3, I
	cmp	I, 0
	ble,pn	%icc, .LL36
	nop

	LDF	[Y1 + 0 * SIZE], y1
	LDF	[Y1 + 1 * SIZE], y2
	LDF	[Y1 + 2 * SIZE], y3
	LDF	[Y1 + 3 * SIZE], y4
	LDF	[Y1 + 4 * SIZE], y5
	LDF	[Y1 + 5 * SIZE], y6
	LDF	[Y1 + 6 * SIZE], y7
	LDF	[Y1 + 7 * SIZE], y8

	LDF	[A1 + 0 * SIZE], a1
	LDF	[A1 + 1 * SIZE], a2
	LDF	[A1 + 2 * SIZE], a3
	LDF	[A1 + 3 * SIZE], a4
	LDF	[A1 + 4 * SIZE], a5
	LDF	[A1 + 5 * SIZE], a6
	LDF	[A1 + 6 * SIZE], a7
	LDF	[A1 + 7 * SIZE], a8

	FMUL	a1,  x1, t1
	deccc	I
	LDF	[A1 +  8 * SIZE], a1
	FMUL	a2,  x1, t2
	LDF	[A1 +  9 * SIZE], a2
	FMUL	a3,  x1, t3
	LDF	[A1 + 10 * SIZE], a3
	FMUL	a4,  x1, t4
	ble,pn	%icc, .LL33
	LDF	[A1 + 11 * SIZE], a4

.LL32:
	FADD	y1,  t1, y1
	prefetch  [A1 +  PREFETCHSIZE * SIZE], 1
	FMUL	a5,  x1, t1
	LDF	[A1 + 12 * SIZE], a5
	FADD	y2,  t2, y2
	FMUL	a6,  x1, t2
	LDF	[A1 + 13 * SIZE], a6

	FADD	y3,  t3, y3
	FMUL	a7,  x1, t3
	LDF	[A1 + 14 * SIZE], a7
	FADD	y4,  t4, y4
	FMUL	a8,  x1, t4
	LDF	[A1 + 15 * SIZE], a8

	FADD	y5,  t1, y5
	FMUL	a1,  x1, t1
	LDF	[A1 + 16 * SIZE], a1
	FADD	y6,  t2, y6
	FMUL	a2,  x1, t2
	LDF	[A1 + 17 * SIZE], a2

	FADD	y7,  t3, y7
	FMUL	a3,  x1, t3
	LDF	[A1 + 18 * SIZE], a3
	FADD	y8,  t4, y8
	FMUL	a4,  x1, t4
	LDF	[A1 + 19 * SIZE], a4

	STF	y1, [Y1 + 0 * SIZE]
	STF	y2, [Y1 + 1 * SIZE]
	STF	y3, [Y1 + 2 * SIZE]
	STF	y4, [Y1 + 3 * SIZE]
	STF	y5, [Y1 + 4 * SIZE]
	STF	y6, [Y1 + 5 * SIZE]
	STF	y7, [Y1 + 6 * SIZE]
	STF	y8, [Y1 + 7 * SIZE]

	LDF	[Y1 +  8 * SIZE], y1
	LDF	[Y1 +  9 * SIZE], y2
	LDF	[Y1 + 10 * SIZE], y3
	LDF	[Y1 + 11 * SIZE], y4
	LDF	[Y1 + 12 * SIZE], y5
	deccc	I
	LDF	[Y1 + 13 * SIZE], y6
	add	A1, 8 * SIZE, A1
	LDF	[Y1 + 14 * SIZE], y7
	add	Y1, 8 * SIZE, Y1
	bg,pn	%icc, .LL32
	LDF	[Y1 +  7 * SIZE], y8

.LL33:
	FADD	y1,  t1, y1
	FMUL	a5,  x1, t1
	FADD	y2,  t2, y2
	FMUL	a6,  x1, t2

	FADD	y3,  t3, y3
	FMUL	a7,  x1, t3
	FADD	y4,  t4, y4
	FMUL	a8,  x1, t4

	STF	y1, [Y1 + 0 * SIZE]
	FADD	y5,  t1, y5
	STF	y2, [Y1 + 1 * SIZE]
	FADD	y6,  t2, y6
	STF	y3, [Y1 + 2 * SIZE]
	FADD	y7,  t3, y7
	STF	y4, [Y1 + 3 * SIZE]
	FADD	y8,  t4, y8

	STF	y5, [Y1 + 4 * SIZE]
	STF	y6, [Y1 + 5 * SIZE]
	STF	y7, [Y1 + 6 * SIZE]
	add	A1, 8 * SIZE, A1
	STF	y8, [Y1 + 7 * SIZE]
	add	Y1, 8 * SIZE, Y1

.LL36:
	andcc	M, 4, I
	ble,pn	%icc, .LL37
	nop

	LDF	[A1 + 0 * SIZE], a1
	LDF	[A1 + 1 * SIZE], a2
	LDF	[A1 + 2 * SIZE], a3
	LDF	[A1 + 3 * SIZE], a4

	LDF	[Y1 + 0 * SIZE], y1
	add	A1, 4 * SIZE, A1
	LDF	[Y1 + 1 * SIZE], y2
	LDF	[Y1 + 2 * SIZE], y3
	LDF	[Y1 + 3 * SIZE], y4

	FMUL	a1,  x1, t1
	FMUL	a2,  x1, t2
	FMUL	a3,  x1, t3
	FMUL	a4,  x1, t4

	FADD	y1,  t1, y1
	FADD	y2,  t2, y2
	FADD	y3,  t3, y3
	FADD	y4,  t4, y4

	STF	y1, [Y1 + 0 * SIZE]
	STF	y2, [Y1 + 1 * SIZE]
	STF	y3, [Y1 + 2 * SIZE]
	STF	y4, [Y1 + 3 * SIZE]

	add	Y1, 4 * SIZE, Y1

.LL37:
	andcc	M, 2, I
	ble,pn	%icc, .LL38
	nop

	LDF	[A1 + 0 * SIZE], a1
	LDF	[Y1 + 0 * SIZE], y1

	LDF	[A1 + 1 * SIZE], a5
	LDF	[Y1 + 1 * SIZE], y2
	add	A1, 2 * SIZE, A1

	FMUL	a1, x1, t1
	FADD	y1, t1, y1
	FMUL	a5, x1, t1
	FADD	y2, t1, y2

	STF	y1, [Y1 + 0 * SIZE]
	STF	y2, [Y1 + 1 * SIZE]
	add	Y1, 2 * SIZE, Y1

.LL38:
	andcc	M, 1, I
	ble,pn	%icc, .LL990
	nop

	LDF	[A1 + 0 * SIZE], a1
	LDF	[Y1 + 0 * SIZE], y1

	FMUL	a1, x1, t1
	FADD	y1, t1, y1

	STF	y1, [Y1]

.LL990:
	cmp	INCY, SIZE
	be	%icc, .LL999
	mov	Y, Y1

	sra	M, 3, I
	cmp	I, 0
	ble,pn	%icc, .LL995
	nop

.LL991:
	LDF	[BUFFER +  0 * SIZE], a1
	LDF	[Y], y1
	add	Y, INCY, Y

	LDF	[BUFFER +  1 * SIZE], a2
	LDF	[Y], y2
	add	Y, INCY, Y

	LDF	[BUFFER +  2 * SIZE], a3
	LDF	[Y], y3
	add	Y, INCY, Y

	LDF	[BUFFER +  3 * SIZE], a4
	LDF	[Y], y4
	add	Y, INCY, Y

	LDF	[BUFFER +  4 * SIZE], a5
	FADD	y1, a1, y1
	LDF	[Y], y5
	add	Y, INCY, Y

	LDF	[BUFFER +  5 * SIZE], a6
	FADD	y2, a2, y2
	LDF	[Y], y6
	add	Y, INCY, Y

	LDF	[BUFFER +  6 * SIZE], a7
	FADD	y3, a3, y3
	LDF	[Y], y7
	add	Y, INCY, Y

	LDF	[BUFFER +  7 * SIZE], a8
	FADD	y4, a4, y4
	LDF	[Y], y8
	add	Y, INCY, Y

	STF	y1, [Y1]
	FADD	y5, a5, y5
	add	Y1, INCY, Y1
	STF	y2, [Y1]
	FADD	y6, a6, y6
	add	Y1, INCY, Y1
	STF	y3, [Y1]
	FADD	y7, a7, y7
	add	Y1, INCY, Y1
	STF	y4, [Y1]
	FADD	y8, a8, y8
	add	Y1, INCY, Y1
	STF	y5, [Y1]
	add	Y1, INCY, Y1
	STF	y6, [Y1]
	add	Y1, INCY, Y1
	STF	y7, [Y1]
	add	Y1, INCY, Y1
	STF	y8, [Y1]
	add	Y1, INCY, Y1

	deccc	I
	bg,pn	%icc, .LL991
	add	BUFFER, 8 * SIZE, BUFFER

.LL995:
	andcc	M, 7, I
	ble,pn	%icc, .LL999
	nop

	andcc	M, 4, I
	ble,pn	%icc, .LL996
	nop

	LDF	[BUFFER +  0 * SIZE], a1
	LDF	[BUFFER +  1 * SIZE], a2
	LDF	[BUFFER +  2 * SIZE], a3
	LDF	[BUFFER +  3 * SIZE], a4
	add	BUFFER, 4 * SIZE, BUFFER

	LDF	[Y], y1
	add	Y, INCY, Y
	LDF	[Y], y2
	add	Y, INCY, Y
	LDF	[Y], y3
	add	Y, INCY, Y
	LDF	[Y], y4
	add	Y, INCY, Y

	FADD	y1, a1, y1
	FADD	y2, a2, y2
	FADD	y3, a3, y3
	FADD	y4, a4, y4

	STF	y1, [Y1]
	add	Y1, INCY, Y1
	STF	y2, [Y1]
	add	Y1, INCY, Y1
	STF	y3, [Y1]
	add	Y1, INCY, Y1
	STF	y4, [Y1]
	add	Y1, INCY, Y1

.LL996:
	andcc	M, 2, I
	ble,pn	%icc, .LL997
	nop

	LDF	[BUFFER +  0 * SIZE], a1
	LDF	[BUFFER +  1 * SIZE], a2
	add	BUFFER, 2 * SIZE, BUFFER

	LDF	[Y], y1
	add	Y, INCY, Y
	LDF	[Y], y2
	add	Y, INCY, Y

	FADD	y1, a1, y1
	FADD	y2, a2, y2

	STF	y1, [Y1]
	add	Y1, INCY, Y1
	STF	y2, [Y1]
	add	Y1, INCY, Y1

.LL997:
	andcc	M, 1, I
	ble,pn	%icc, .LL999
	nop

	LDF	[BUFFER +  0 * SIZE], a1

	LDF	[Y], y1

	FADD	y1, a1, y1

	STF	y1, [Y1]


.LL999:
	return	%i7 + 8
	clr	%o0

	EPILOGUE
