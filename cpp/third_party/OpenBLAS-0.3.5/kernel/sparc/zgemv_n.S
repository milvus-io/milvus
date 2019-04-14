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

#ifdef DOUBLE
#define PREFETCHSIZE 44
#else
#define PREFETCHSIZE 88
#endif

#define M	%i0
#define N	%i1
#define A	%i5
#define LDA	%i2
#define X	%i3
#define INCX	%i4

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

#define FZERO	%f50
#define ALPHA_R	%f52
#define ALPHA_I	%f54
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

#define FZERO	%f25
#define ALPHA_R	%f26
#define ALPHA_I	%f27
#endif

#ifndef __64BIT__
#define STACK_ALPHA_R	[%sp + STACK_START + 16]
#ifndef DOUBLE
#define STACK_ALPHA_I	[%sp + STACK_START + 20]
#else
#define STACK_ALPHA_I	[%sp + STACK_START + 24]
#endif
#else
#define STACK_ALPHA_R	[%sp + STACK_START + 32]
#define STACK_ALPHA_I	[%sp + STACK_START + 40]
#endif

#ifndef CONJ
#define	FSUBX	FSUB
#define FADDX	FADD
#else
#define	FSUBX	FADD
#define FADDX	FSUB
#endif

	PROLOGUE
	SAVESP

#ifndef __64BIT__
#ifdef DOUBLE
	st	%i3, [%sp + STACK_START + 16]   /* ALPHA_R */
	st	%i4, [%sp + STACK_START + 20]
	st	%i5, [%sp + STACK_START + 24]   /* ALPHA_I */

	ld	[%sp + STACK_START + 32], A
	ld	[%sp + STACK_START + 36], LDA
	ld	[%sp + STACK_START + 40], X
	ld	[%sp + STACK_START + 44], INCX
	ld	[%sp + STACK_START + 48], Y
	ld	[%sp + STACK_START + 52], INCY
	ld	[%sp + STACK_START + 56], BUFFER
#else
	st	%i3, [%sp + STACK_START + 16]   /* ALPHA_R */
	st	%i4, [%sp + STACK_START + 20]   /* ALPHA_I */

	ld	[%sp + STACK_START + 28], LDA
	ld	[%sp + STACK_START + 32], X
	ld	[%sp + STACK_START + 36], INCX
	ld	[%sp + STACK_START + 40], Y
	ld	[%sp + STACK_START + 44], INCY
	ld	[%sp + STACK_START + 48], BUFFER
#endif
#else
	ldx	[%sp + STACK_START + 56], LDA
	ldx	[%sp + STACK_START + 64], X
	ldx	[%sp + STACK_START + 72], INCX
	ldx	[%sp + STACK_START + 80], Y
	ldx	[%sp + STACK_START + 88], INCY
	ldx	[%sp + STACK_START + 96], BUFFER

#ifdef DOUBLE
	std	%f6, STACK_ALPHA_R
	std	%f8, STACK_ALPHA_I
#else
	st	%f7, STACK_ALPHA_R
	st	%f9, STACK_ALPHA_I
#endif
#endif

	sll	LDA, ZBASE_SHIFT, LDA

	cmp	M, 0
	ble	%icc, .LL999
	sll	INCX, ZBASE_SHIFT, INCX

	cmp	N, 0
	ble	%icc, .LL999
	sll	INCY, ZBASE_SHIFT, INCY

	cmp	INCY, 2 * SIZE
	be	%icc, .LL20
	mov	Y, YY

#ifdef DOUBLE
	FCLR(19)
#else
	FCLR(25)
#endif

	add	M, 3, J
	sra	J, 2, J
	mov	BUFFER, YY
	mov	BUFFER, Y1

.LL01:
	STF	FZERO, [Y1 +  0 * SIZE]
	nop
	STF	FZERO, [Y1 +  1 * SIZE]
	STF	FZERO, [Y1 +  2 * SIZE]
	STF	FZERO, [Y1 +  3 * SIZE]
	STF	FZERO, [Y1 +  4 * SIZE]
	nop
	STF	FZERO, [Y1 +  5 * SIZE]
	deccc	J
	STF	FZERO, [Y1 +  6 * SIZE]
	nop
	STF	FZERO, [Y1 +  7 * SIZE]
	bg,pn	%icc, .LL01
	add	Y1, 8 * SIZE, Y1

.LL20:
	sra	N, 1, J
	cmp	J, 0
	ble,pn	%icc, .LL30
	nop

.LL21:
	mov	YY, Y1
	mov	A,  A1
	LDF	STACK_ALPHA_R, ALPHA_R
	LDF	STACK_ALPHA_I, ALPHA_I

	add	A,  LDA, A2
	add	A2, LDA, A

	LDF	[X + 0 * SIZE], x1
	LDF	[X + 1 * SIZE], x2
	add	X, INCX, X
	LDF	[X + 0 * SIZE], x3
	LDF	[X + 1 * SIZE], x4
	add	X, INCX, X

	FMUL	ALPHA_R, x1, a1
	FMUL	ALPHA_I, x2, a4
	FMUL	ALPHA_I, x1, a2
	FMUL	ALPHA_R, x2, a3

	FMUL	ALPHA_R, x3, a5
	FMUL	ALPHA_I, x4, a8
	FMUL	ALPHA_I, x3, a6
	FMUL	ALPHA_R, x4, a7

#ifndef XCONJ
	FSUB	a1, a4, x1
	FADD	a2, a3, x2
	FSUB	a5, a8, x3
	FADD	a6, a7, x4
#else
	FADD	a1, a4, x1
	FSUB	a2, a3, x2
	FADD	a5, a8, x3
	FSUB	a6, a7, x4
#endif

	sra	M, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL27
	nop

	LDF	[A1 + 0 * SIZE], a1
	LDF	[A1 + 1 * SIZE], a2
	LDF	[A1 + 2 * SIZE], a3
	LDF	[A1 + 3 * SIZE], a4

	LDF	[A1 + 4 * SIZE], a9
	LDF	[A1 + 5 * SIZE], a10
	LDF	[A1 + 6 * SIZE], a11
	LDF	[A1 + 7 * SIZE], a12

	LDF	[A2 + 0 * SIZE], a5
	LDF	[A2 + 1 * SIZE], a6
	LDF	[A2 + 2 * SIZE], a7
	LDF	[A2 + 3 * SIZE], a8

	LDF	[A2 + 4 * SIZE], a13
	LDF	[A2 + 5 * SIZE], a14
	LDF	[A2 + 6 * SIZE], a15
	LDF	[A2 + 7 * SIZE], a16

	LDF	[Y1 + 0 * SIZE], y1
	LDF	[Y1 + 1 * SIZE], y2
	LDF	[Y1 + 2 * SIZE], y3


	FMUL	a1, x1, t1
	deccc	I
	FMUL	a1, x2, t2
	LDF	[A1 +  8 * SIZE], a1

	FMUL	a3, x1, t3
	FMUL	a3, x2, t4
	ble,pn	%icc, .LL26
	LDF	[A1 + 10 * SIZE], a3

	FADD	y1, t1, y1
	LDF	[Y1 + 3 * SIZE], y4
	FMUL	a2, x2, t1

	FADD	y2, t2, y2
	FMUL	a2, x1, t2
	LDF	[A1 +  9 * SIZE], a2

	FADD	y3, t3, y3
	LDF	[Y1 + 4 * SIZE], y5
	FMUL	a4, x2, t3

	FADD	y4, t4, y4
	FMUL	a4, x1, t4
	LDF	[A1 + 11 * SIZE], a4

	FSUBX	y1, t1, y1
	LDF	[Y1 + 5 * SIZE], y6
	FMUL	a5, x3, t1

	FADDX	y2, t2, y2
	FMUL	a5, x4, t2
	LDF	[A2 +  8 * SIZE], a5

	FSUBX	y3, t3, y3
	LDF	[Y1 + 6 * SIZE], y7
	FMUL	a7, x3, t3

	FADDX	y4, t4, y4
	FMUL	a7, x4, t4
	LDF	[A2 + 10 * SIZE], a7

	FADD	y1, t1, y1
	LDF	[Y1 + 7 * SIZE], y8
	FMUL	a6, x4, t1

	FADD	y2, t2, y2
	FMUL	a6, x3, t2
	LDF	[A2 +  9 * SIZE], a6

	FADD	y3, t3, y3
	FMUL	a8, x4, t3

	FADD	y4, t4, y4
	FMUL	a8, x3, t4
	LDF	[A2 + 11 * SIZE], a8

	FSUBX	y1, t1, y1
	FMUL	a9,  x1, t1

	FADDX	y2, t2, y2
	FMUL	a9,  x2, t2
	LDF	[A1 + 12 * SIZE], a9

	FSUBX	y3, t3, y3
	deccc	I
	FMUL	a11, x1, t3

	FADDX	y4, t4, y4
	FMUL	a11, x2, t4
	ble,pn	%icc, .LL23
	LDF	[A1 + 14 * SIZE], a11

.LL22:
	FADD	y5, t1, y5
	prefetch  [A1 +  PREFETCHSIZE * SIZE], 1
	FMUL	a10, x2, t1
	LDF	[Y1 + 7 * SIZE], y8

	FADD	y6, t2, y6
	FMUL	a10, x1, t2
	LDF	[A1 + 13 * SIZE], a10

	FADD	y7, t3, y7
	FMUL	a12, x2, t3
	STF	y1, [Y1 +  0 * SIZE]

	FADD	y8, t4, y8
	FMUL	a12, x1, t4
	LDF	[A1 + 15 * SIZE], a12

	FSUBX	y5, t1, y5
	FMUL	a13, x3, t1
	STF	y2, [Y1 +  1 * SIZE]

	FADDX	y6, t2, y6
	FMUL	a13, x4, t2
	LDF	[A2 + 12 * SIZE], a13

	FSUBX	y7, t3, y7
	FMUL	a15, x3, t3
	STF	y3, [Y1 +  2 * SIZE]

	FADDX	y8, t4, y8
	FMUL	a15, x4, t4
	LDF	[A2 + 14 * SIZE], a15

	FADD	y5, t1, y5
	FMUL	a14, x4, t1
	STF	y4, [Y1 +  3 * SIZE]

	FADD	y6, t2, y6
	FMUL	a14, x3, t2
	LDF	[A2 + 13 * SIZE], a14

	FADD	y7, t3, y7
	FMUL	a16, x4, t3
	LDF	[Y1 +  8 * SIZE], y1

	FADD	y8, t4, y8
	FMUL	a16, x3, t4
	LDF	[A2 + 15 * SIZE], a16

	FSUBX	y5, t1, y5
	FMUL	a1, x1, t1
	LDF	[Y1 +  9 * SIZE], y2

	FADDX	y6, t2, y6
	FMUL	a1, x2, t2
	LDF	[A1 + 16 * SIZE], a1

	FSUBX	y7, t3, y7
	FMUL	a3, x1, t3
	LDF	[Y1 + 10 * SIZE], y3

	FADDX	y8, t4, y8
	FMUL	a3, x2, t4
	LDF	[A1 + 18 * SIZE], a3

	FADD	y1, t1, y1
	prefetch  [A2 +  PREFETCHSIZE * SIZE], 1
	FMUL	a2, x2, t1
	LDF	[Y1 + 11 * SIZE], y4

	FADD	y2, t2, y2
	FMUL	a2, x1, t2
	LDF	[A1 + 17 * SIZE], a2

	FADD	y3, t3, y3
	FMUL	a4, x2, t3
	STF	y5, [Y1 +  4 * SIZE]

	FADD	y4, t4, y4
	FMUL	a4, x1, t4
	LDF	[A1 + 19 * SIZE], a4

	FSUBX	y1, t1, y1
	FMUL	a5, x3, t1
	STF	y6, [Y1 +  5 * SIZE]

	FADDX	y2, t2, y2
	FMUL	a5, x4, t2
	LDF	[A2 + 16 * SIZE], a5

	FSUBX	y3, t3, y3
	FMUL	a7, x3, t3
	STF	y7, [Y1 +  6 * SIZE]

	FADDX	y4, t4, y4
	deccc	I
	FMUL	a7, x4, t4
	LDF	[A2 + 18 * SIZE], a7

	FADD	y1, t1, y1
	FMUL	a6, x4, t1
	STF	y8, [Y1 +  7 * SIZE]

	FADD	y2, t2, y2
	FMUL	a6, x3, t2
	LDF	[A2 + 17 * SIZE], a6

	FADD	y3, t3, y3
	add	A1, 8 * SIZE, A1
	FMUL	a8, x4, t3
	LDF	[Y1 + 12 * SIZE], y5

	FADD	y4, t4, y4
	FMUL	a8, x3, t4
	LDF	[A2 + 19 * SIZE], a8

	FSUBX	y1, t1, y1
	add	A2, 8 * SIZE, A2
	FMUL	a9,  x1, t1
	LDF	[Y1 + 13 * SIZE], y6

	FADDX	y2, t2, y2
	add	Y1, 8 * SIZE, Y1
	FMUL	a9,  x2, t2
	LDF	[A1 + 12 * SIZE], a9

	FSUBX	y3, t3, y3
	FMUL	a11, x1, t3
	LDF	[Y1 +  6 * SIZE], y7

	FADDX	y4, t4, y4
	FMUL	a11, x2, t4
	bg,pn	%icc, .LL22
	LDF	[A1 + 14 * SIZE], a11

.LL23:
	FADD	y5, t1, y5
	FMUL	a10, x2, t1
	LDF	[Y1 + 7 * SIZE], y8

	FADD	y6, t2, y6
	FMUL	a10, x1, t2
	LDF	[A1 + 13 * SIZE], a10

	FADD	y7, t3, y7
	FMUL	a12, x2, t3
	STF	y1, [Y1 +  0 * SIZE]

	FADD	y8, t4, y8
	FMUL	a12, x1, t4
	LDF	[A1 + 15 * SIZE], a12

	FSUBX	y5, t1, y5
	FMUL	a13, x3, t1
	STF	y2, [Y1 +  1 * SIZE]

	FADDX	y6, t2, y6
	FMUL	a13, x4, t2
	LDF	[A2 + 12 * SIZE], a13

	FSUBX	y7, t3, y7
	FMUL	a15, x3, t3
	STF	y3, [Y1 +  2 * SIZE]
	FADDX	y8, t4, y8
	FMUL	a15, x4, t4
	LDF	[A2 + 14 * SIZE], a15

	FADD	y5, t1, y5
	FMUL	a14, x4, t1
	STF	y4, [Y1 +  3 * SIZE]
	FADD	y6, t2, y6
	FMUL	a14, x3, t2
	LDF	[A2 + 13 * SIZE], a14

	FADD	y7, t3, y7
	FMUL	a16, x4, t3
	LDF	[Y1 +  8 * SIZE], y1
	FADD	y8, t4, y8
	FMUL	a16, x3, t4
	LDF	[A2 + 15 * SIZE], a16

	FSUBX	y5, t1, y5
	add	A1, 8 * SIZE, A1
	FMUL	a1, x1, t1
	LDF	[Y1 +  9 * SIZE], y2

	FADDX	y6, t2, y6
	add	A2, 8 * SIZE, A2
	FMUL	a1, x2, t2
	LDF	[A1 +  8 * SIZE], a1

	FSUBX	y7, t3, y7
	FMUL	a3, x1, t3
	LDF	[Y1 + 10 * SIZE], y3

	FADDX	y8, t4, y8
	add	Y1, 8 * SIZE, Y1
	FMUL	a3, x2, t4
	LDF	[A1 + 10 * SIZE], a3

	STF	y5, [Y1 -  4 * SIZE]
	STF	y6, [Y1 -  3 * SIZE]
	STF	y7, [Y1 -  2 * SIZE]
	STF	y8, [Y1 -  1 * SIZE]

.LL26:
	FADD	y1, t1, y1
	LDF	[Y1 +  3 * SIZE], y4
	FMUL	a2, x2, t1
	FADD	y2, t2, y2
	FMUL	a2, x1, t2

	FADD	y3, t3, y3
	LDF	[Y1 +  4 * SIZE], y5
	FMUL	a4, x2, t3
	FADD	y4, t4, y4
	FMUL	a4, x1, t4

	FSUBX	y1, t1, y1
	LDF	[Y1 +  5 * SIZE], y6
	FMUL	a5, x3, t1
	FADDX	y2, t2, y2
	FMUL	a5, x4, t2

	FSUBX	y3, t3, y3
	LDF	[Y1 +  6 * SIZE], y7
	FADDX	y4, t4, y4
	FMUL	a7, x4, t4

	FADD	y1, t1, y1
	LDF	[Y1 +  7 * SIZE], y8
	FMUL	a7, x3, t3
	FMUL	a6, x4, t1
	FADD	y2, t2, y2
	FMUL	a6, x3, t2

	FADD	y3, t3, y3
	FMUL	a8, x4, t3
	FADD	y4, t4, y4
	FMUL	a8, x3, t4

	FSUBX	y1, t1, y1
	FMUL	a9,  x1, t1
	FADDX	y2, t2, y2
	FMUL	a9,  x2, t2

	FSUBX	y3, t3, y3
	FMUL	a11, x1, t3
	FADDX	y4, t4, y4
	FMUL	a11, x2, t4

	FADD	y5, t1, y5
	FMUL	a10, x2, t1
	FADD	y6, t2, y6
	FMUL	a10, x1, t2

	FADD	y7, t3, y7
	FMUL	a12, x2, t3
	FADD	y8, t4, y8
	FMUL	a12, x1, t4

	FSUBX	y5, t1, y5
	FMUL	a13, x3, t1
	FADDX	y6, t2, y6
	FMUL	a13, x4, t2

	FSUBX	y7, t3, y7
	FMUL	a15, x3, t3
	FADDX	y8, t4, y8
	FMUL	a15, x4, t4

	FADD	y5, t1, y5
	FMUL	a14, x4, t1
	FADD	y6, t2, y6
	FMUL	a14, x3, t2

	FADD	y7, t3, y7
	FMUL	a16, x4, t3
	FADD	y8, t4, y8
	FMUL	a16, x3, t4

	STF	y1, [Y1 + 0 * SIZE]
	FSUBX	y5, t1, y5
	STF	y2, [Y1 + 1 * SIZE]
	FADDX	y6, t2, y6
	STF	y3, [Y1 + 2 * SIZE]
	FSUBX	y7, t3, y7
	STF	y4, [Y1 + 3 * SIZE]
	FADDX	y8, t4, y8

	STF	y5, [Y1 + 4 * SIZE]
	add	A1, 8 * SIZE, A1
	STF	y6, [Y1 + 5 * SIZE]
	add	A2, 8 * SIZE, A2
	STF	y7, [Y1 + 6 * SIZE]
	STF	y8, [Y1 + 7 * SIZE]
	add	Y1, 8 * SIZE, Y1

.LL27:
	andcc	M, 2, I
	ble,pn	%icc, .LL28
	nop

	LDF	[A1 + 0 * SIZE], a1
	LDF	[A1 + 1 * SIZE], a2
	LDF	[A1 + 2 * SIZE], a3
	LDF	[A1 + 3 * SIZE], a4

	LDF	[Y1 + 0 * SIZE], y1
	LDF	[Y1 + 1 * SIZE], y2
	LDF	[Y1 + 2 * SIZE], y3
	LDF	[Y1 + 3 * SIZE], y4

	FMUL	a1, x1, t1
	LDF	[A2 + 0 * SIZE], a5
	FMUL	a1, x2, t2
	LDF	[A2 + 1 * SIZE], a6
	FMUL	a3, x1, t3
	LDF	[A2 + 2 * SIZE], a7
	FMUL	a3, x2, t4
	LDF	[A2 + 3 * SIZE], a8

	FADD	y1, t1, y1
	FMUL	a2, x2, t1
	FADD	y2, t2, y2
	FMUL	a2, x1, t2

	FADD	y3, t3, y3
	FMUL	a4, x2, t3
	FADD	y4, t4, y4
	FMUL	a4, x1, t4

	FSUBX	y1, t1, y1
	FMUL	a5, x3, t1
	FADDX	y2, t2, y2
	FMUL	a5, x4, t2

	FSUBX	y3, t3, y3
	FMUL	a7, x3, t3
	FADDX	y4, t4, y4
	FMUL	a7, x4, t4

	FADD	y1, t1, y1
	FMUL	a6, x4, t1
	FADD	y2, t2, y2
	FMUL	a6, x3, t2

	FADD	y3, t3, y3
	FMUL	a8, x4, t3
	FADD	y4, t4, y4
	FMUL	a8, x3, t4

	FSUBX	y1, t1, y1
	FADDX	y2, t2, y2
	FSUBX	y3, t3, y3
	FADDX	y4, t4, y4

	STF	y1, [Y1 + 0 * SIZE]
	add	A1, 4 * SIZE, A1
	STF	y2, [Y1 + 1 * SIZE]
	add	A2, 4 * SIZE, A2
	STF	y3, [Y1 + 2 * SIZE]
	nop
	STF	y4, [Y1 + 3 * SIZE]
	add	Y1, 4 * SIZE, Y1

.LL28:
	andcc	M, 1, I
	ble,pn	%icc, .LL29
	nop

	LDF	[A1 + 0 * SIZE], a1
	LDF	[A1 + 1 * SIZE], a2
	LDF	[A2 + 0 * SIZE], a3
	LDF	[A2 + 1 * SIZE], a4

	LDF	[Y1 + 0 * SIZE], y1
	LDF	[Y1 + 1 * SIZE], y2

	FMUL	a1, x1, t1
	FMUL	a1, x2, t2
	FMUL	a2, x2, t3
	FMUL	a2, x1, t4

	FADD	y1, t1, y1
	FMUL	a3, x3, t1
	FADD	y2, t2, y2
	FMUL	a3, x4, t2

	FSUBX	y1, t3, y1
	FMUL	a4, x4, t3
	FADDX	y2, t4, y2
	FMUL	a4, x3, t4

	FADD	y1, t1, y1
	FADD	y2, t2, y2
	FSUBX	y1, t3, y1
	FADDX	y2, t4, y2

	STF	y1, [Y1 + 0 * SIZE]
	STF	y2, [Y1 + 1 * SIZE]

.LL29:
	deccc	J
	bg	%icc, .LL21
	nop


.LL30:
	andcc	N, 1, J
	ble,pn	%icc, .LL990
	nop

.LL31:
	mov	YY, Y1
	mov	A,  A1

	LDF	STACK_ALPHA_R, ALPHA_R
	LDF	STACK_ALPHA_I, ALPHA_I

	LDF	[X + 0 * SIZE], x1
	LDF	[X + 1 * SIZE], x2

	FMUL	ALPHA_R, x1, a1		/* AC */
	FMUL	ALPHA_I, x1, a2		/* AD */
	FMUL	ALPHA_R, x2, a3		/* BC */
	FMUL	ALPHA_I, x2, a4		/* BD */

#ifndef XCONJ
	FSUB	a1, a4, x1
	FADD	a2, a3, x2
#else
	FADD	a1, a4, x1
	FSUB	a2, a3, x2
#endif

	sra	M, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL37
	nop

	LDF	[A1 + 0 * SIZE], a1
	LDF	[A1 + 1 * SIZE], a2
	LDF	[A1 + 2 * SIZE], a3
	LDF	[A1 + 3 * SIZE], a4

	LDF	[A1 + 4 * SIZE], a9
	LDF	[A1 + 5 * SIZE], a10
	LDF	[A1 + 6 * SIZE], a11
	LDF	[A1 + 7 * SIZE], a12

	LDF	[Y1 + 0 * SIZE], y1
	LDF	[Y1 + 1 * SIZE], y2
	LDF	[Y1 + 2 * SIZE], y3
	LDF	[Y1 + 3 * SIZE], y4

	LDF	[Y1 + 4 * SIZE], y5
	LDF	[Y1 + 5 * SIZE], y6
	LDF	[Y1 + 6 * SIZE], y7
	LDF	[Y1 + 7 * SIZE], y8

	FMUL	a1, x1, t1
	deccc	I
	FMUL	a1, x2, t2
	LDF	[A1 +  8 * SIZE], a1
	FMUL	a3, x1, t3
	FMUL	a3, x2, t4
	ble,pn	%icc, .LL33
	LDF	[A1 + 10 * SIZE], a3

.LL32:
	FADD	y1, t1, y1
	prefetch  [A1 +  PREFETCHSIZE * SIZE], 1
	FMUL	a2, x2, t1
	FADD	y2, t2, y2
	FMUL	a2, x1, t2
	LDF	[A1 +  9 * SIZE], a2

	FADD	y3, t3, y3
	FMUL	a4, x2, t3
	FADD	y4, t4, y4
	FMUL	a4, x1, t4
	LDF	[A1 + 11 * SIZE], a4

	FSUBX	y1, t1, y1
	FMUL	a9,  x1, t1
	FADDX	y2, t2, y2
	FMUL	a9,  x2, t2
	LDF	[A1 + 12 * SIZE], a9

	FSUBX	y3, t3, y3
	FMUL	a11, x1, t3
	FADDX	y4, t4, y4
	FMUL	a11, x2, t4
	LDF	[A1 + 14 * SIZE], a11

	STF	y1, [Y1 + 0 * SIZE]
	STF	y2, [Y1 + 1 * SIZE]
	STF	y3, [Y1 + 2 * SIZE]
	STF	y4, [Y1 + 3 * SIZE]

	FADD	y5, t1, y5
	FMUL	a10, x2, t1
	LDF	[Y1 +  8 * SIZE], y1
	FADD	y6, t2, y6
	FMUL	a10, x1, t2
	LDF	[A1 + 13 * SIZE], a10

	FADD	y7, t3, y7
	deccc	I
	FMUL	a12, x2, t3
	LDF	[Y1 +  9 * SIZE], y2
	FADD	y8, t4, y8
	FMUL	a12, x1, t4
	LDF	[A1 + 15 * SIZE], a12

	FSUBX	y5, t1, y5
	add	A1, 8 * SIZE, A1
	FMUL	a1, x1, t1
	LDF	[Y1 + 10 * SIZE], y3
	FADDX	y6, t2, y6
	FMUL	a1, x2, t2
	LDF	[A1 +  8 * SIZE], a1

	FSUBX	y7, t3, y7
	FMUL	a3, x1, t3
	LDF	[Y1 + 11 * SIZE], y4
	FADDX	y8, t4, y8
	FMUL	a3, x2, t4
	LDF	[A1 + 10 * SIZE], a3

	STF	y5, [Y1 + 4 * SIZE]
	STF	y6, [Y1 + 5 * SIZE]
	STF	y7, [Y1 + 6 * SIZE]
	STF	y8, [Y1 + 7 * SIZE]

	LDF	[Y1 + 12 * SIZE], y5
	LDF	[Y1 + 13 * SIZE], y6
	LDF	[Y1 + 14 * SIZE], y7
	add	Y1, 8 * SIZE, Y1
	bg,pn	%icc, .LL32
	LDF	[Y1 +  7 * SIZE], y8

.LL33:
	FADD	y1, t1, y1
	FMUL	a2, x2, t1
	FADD	y2, t2, y2
	FMUL	a2, x1, t2

	FADD	y3, t3, y3
	FMUL	a4, x2, t3
	FADD	y4, t4, y4
	FMUL	a4, x1, t4

	FSUBX	y1, t1, y1
	FMUL	a9,  x1, t1
	FADDX	y2, t2, y2
	FMUL	a9,  x2, t2

	FSUBX	y3, t3, y3
	FMUL	a11, x1, t3
	FADDX	y4, t4, y4
	FMUL	a11, x2, t4

	FADD	y5, t1, y5
	FMUL	a10, x2, t1
	FADD	y6, t2, y6
	FMUL	a10, x1, t2

	FADD	y7, t3, y7
	FMUL	a12, x2, t3
	FADD	y8, t4, y8
	FMUL	a12, x1, t4

	FSUBX	y5, t1, y5
	FADDX	y6, t2, y6
	FSUBX	y7, t3, y7
	FADDX	y8, t4, y8

	STF	y1, [Y1 + 0 * SIZE]
	STF	y2, [Y1 + 1 * SIZE]
	STF	y3, [Y1 + 2 * SIZE]
	STF	y4, [Y1 + 3 * SIZE]

	STF	y5, [Y1 + 4 * SIZE]
	STF	y6, [Y1 + 5 * SIZE]
	STF	y7, [Y1 + 6 * SIZE]
	STF	y8, [Y1 + 7 * SIZE]

	add	A1, 8 * SIZE, A1
	add	Y1, 8 * SIZE, Y1


.LL37:
	andcc	M, 2, I
	ble,pn	%icc, .LL38
	nop

	LDF	[A1 + 0 * SIZE], a1
	LDF	[A1 + 1 * SIZE], a2
	LDF	[A1 + 2 * SIZE], a3
	LDF	[A1 + 3 * SIZE], a4

	LDF	[Y1 + 0 * SIZE], y1
	FMUL	a1, x1, t1
	LDF	[Y1 + 1 * SIZE], y2
	FMUL	a1, x2, t2
	LDF	[Y1 + 2 * SIZE], y3
	FMUL	a3, x1, t3
	LDF	[Y1 + 3 * SIZE], y4
	FMUL	a3, x2, t4

	FADD	y1, t1, y1
	FMUL	a2, x2, t1
	FADD	y2, t2, y2
	FMUL	a2, x1, t2
	FADD	y3, t3, y3
	FMUL	a4, x2, t3
	FADD	y4, t4, y4
	FMUL	a4, x1, t4

	FSUBX	y1, t1, y1
	FADDX	y2, t2, y2
	FSUBX	y3, t3, y3
	FADDX	y4, t4, y4

	STF	y1, [Y1 + 0 * SIZE]
	STF	y2, [Y1 + 1 * SIZE]
	STF	y3, [Y1 + 2 * SIZE]
	STF	y4, [Y1 + 3 * SIZE]

	add	A1, 4 * SIZE, A1
	add	Y1, 4 * SIZE, Y1

.LL38:
	andcc	M, 1, I
	ble,pn	%icc, .LL990
	nop

	LDF	[A1 + 0 * SIZE], a1
	LDF	[A1 + 1 * SIZE], a2
	LDF	[Y1 + 0 * SIZE], y1
	LDF	[Y1 + 1 * SIZE], y2

	FMUL	a1, x1, t1
	FMUL	a1, x2, t2
	FMUL	a2, x2, t3
	FMUL	a2, x1, t4

	FADD	y1, t1, y1
	FADD	y2, t2, y2
	FSUBX	y1, t3, y1
	FADDX	y2, t4, y2

	STF	y1, [Y1 + 0 * SIZE]
	STF	y2, [Y1 + 1 * SIZE]

.LL990:
	cmp	INCY, 2 * SIZE
	be	%icc, .LL999
	mov	Y, Y1

	sra	M, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL995
	nop

.LL991:
	LDF	[BUFFER +  0 * SIZE], a1
	LDF	[BUFFER +  1 * SIZE], a2
	LDF	[Y + 0 * SIZE], y1
	LDF	[Y + 1 * SIZE], y2
	add	Y, INCY, Y

	LDF	[BUFFER +  2 * SIZE], a3
	LDF	[BUFFER +  3 * SIZE], a4
	LDF	[Y + 0 * SIZE], y3
	LDF	[Y + 1 * SIZE], y4
	add	Y, INCY, Y

	LDF	[BUFFER +  4 * SIZE], a5
	LDF	[BUFFER +  5 * SIZE], a6
	LDF	[Y + 0 * SIZE], y5
	LDF	[Y + 1 * SIZE], y6
	add	Y, INCY, Y

	LDF	[BUFFER +  6 * SIZE], a7
	LDF	[BUFFER +  7 * SIZE], a8
	LDF	[Y + 0 * SIZE], y7
	LDF	[Y + 1 * SIZE], y8
	add	Y, INCY, Y

	FADD	y1, a1, y1
	FADD	y2, a2, y2
	FADD	y3, a3, y3
	FADD	y4, a4, y4
	FADD	y5, a5, y5
	FADD	y6, a6, y6
	FADD	y7, a7, y7
	FADD	y8, a8, y8

	STF	y1, [Y1 + 0 * SIZE]
	STF	y2, [Y1 + 1 * SIZE]
	add	Y1, INCY, Y1
	STF	y3, [Y1 + 0 * SIZE]
	STF	y4, [Y1 + 1 * SIZE]
	add	Y1, INCY, Y1
	STF	y5, [Y1 + 0 * SIZE]
	STF	y6, [Y1 + 1 * SIZE]
	add	Y1, INCY, Y1
	STF	y7, [Y1 + 0 * SIZE]
	STF	y8, [Y1 + 1 * SIZE]
	add	Y1, INCY, Y1

	deccc	I
	bg,pn	%icc, .LL991
	add	BUFFER, 8 * SIZE, BUFFER

.LL995:
	andcc	M, 2, I
	ble,pn	%icc, .LL996
	nop

	LDF	[BUFFER +  0 * SIZE], a1
	LDF	[BUFFER +  1 * SIZE], a2
	LDF	[Y + 0 * SIZE], y1
	LDF	[Y + 1 * SIZE], y2
	add	Y, INCY, Y

	LDF	[BUFFER +  2 * SIZE], a3
	LDF	[BUFFER +  3 * SIZE], a4
	LDF	[Y + 0 * SIZE], y3
	LDF	[Y + 1 * SIZE], y4
	add	Y, INCY, Y

	FADD	y1, a1, y1
	FADD	y2, a2, y2
	FADD	y3, a3, y3
	FADD	y4, a4, y4

	STF	y1, [Y1 + 0 * SIZE]
	STF	y2, [Y1 + 1 * SIZE]
	add	Y1, INCY, Y1
	STF	y3, [Y1 + 0 * SIZE]
	STF	y4, [Y1 + 1 * SIZE]
	add	Y1, INCY, Y1

	add	BUFFER, 4 * SIZE, BUFFER

.LL996:
	andcc	M, 1, I
	ble,pn	%icc, .LL999
	nop

	LDF	[BUFFER +  0 * SIZE], a1
	LDF	[BUFFER +  1 * SIZE], a2
	LDF	[Y + 0 * SIZE], y1
	LDF	[Y + 1 * SIZE], y2

	FADD	y1, a1, y1
	FADD	y2, a2, y2

	STF	y1, [Y1 + 0 * SIZE]
	STF	y2, [Y1 + 1 * SIZE]

.LL999:
	return	%i7 + 8
	clr	%o0

	EPILOGUE
