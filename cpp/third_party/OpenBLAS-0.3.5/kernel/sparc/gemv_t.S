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

#define P 1020

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
#define IS	%l4
#define J	%l5
#define MIN_M	%l6
#define XP	%l7

#define A1	%o0
#define A2	%o1
#define A3	%o2
#define A4	%o3
#define X1	%o4
#define Y1	%o5
#define PNLDA	%g1
#define Y2	%o7	/* Danger? */

#ifdef DOUBLE
#define t1	%f0
#define	t2 	%f2
#define t3	%f4
#define	t4 	%f6

#define c1	%f8
#define c2	%f10
#define c3	%f12
#define c4	%f14

#define a1	%f16
#define a2	%f18
#define a3	%f20
#define a4	%f22
#define a5	%f24
#define a6	%f26
#define a7	%f28
#define a8	%f30

#define a9	%f32
#define a10	%f34
#define a11	%f36
#define a12	%f38
#define a13	%f40
#define a14	%f42
#define a15	%f44
#define a16	%f46

#define b1	%f48
#define b2	%f50
#define b3	%f52
#define b4	%f54
#define b5	%f56
#define b6	%f58
#define b7	%f60
#define b8	%f62

#define FZERO	%f60
#define ALPHA	%f62

#else
#define t1	%f0
#define	t2 	%f1
#define t3	%f2
#define	t4 	%f3

#define c1	%f4
#define c2	%f5
#define c3	%f6
#define c4	%f7

#define a1	%f8
#define a2	%f9
#define a3	%f10
#define a4	%f11
#define a5	%f12
#define a6	%f13
#define a7	%f14
#define a8	%f15

#define a9	%f16
#define a10	%f17
#define a11	%f18
#define a12	%f19
#define a13	%f20
#define a14	%f21
#define a15	%f22
#define a16	%f23

#define b1	%f24
#define b2	%f25
#define b3	%f26
#define b4	%f27
#define b5	%f28
#define b6	%f29
#define b7	%f30
#define b8	%f31

#define FZERO	%f30
#define ALPHA	%f31
#endif

#ifndef __64BIT__
#define STACK_FZERO	[%sp + STACK_START +  8]
#define STACK_ALPHA	[%sp + STACK_START + 16]
#else
#define STACK_FZERO	[%sp + STACK_START + 32]
#define STACK_ALPHA	[%sp + STACK_START + 40]
#endif

#ifdef DOUBLE
#define PREFETCHSIZE 36
#else
#define PREFETCHSIZE 72
#endif

	PROLOGUE
	SAVESP
	nop

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
	ldx	[%sp+  STACK_START + 56], X
	ldx	[%sp+  STACK_START + 64], INCX
	ldx	[%sp+  STACK_START + 72], Y
	ldx	[%sp+  STACK_START + 80], INCY
	ldx	[%sp+  STACK_START + 88], BUFFER
#ifdef DOUBLE
	FMOV	%f6, ALPHA
	STF	%f6, STACK_ALPHA
#else
	FMOV	%f7, ALPHA
	STF	%f7, STACK_ALPHA
#endif
#endif

#ifdef DOUBLE
	FCLR(29)
#else
	FCLR(30)
#endif

	clr	IS
	mov	P, I
	sll	LDA, BASE_SHIFT, LDA
	sll	I, BASE_SHIFT, I
	smul	LDA, N, PNLDA
	sll	INCX, BASE_SHIFT, INCX
	sll	INCY, BASE_SHIFT, INCY
	sub	I, PNLDA, PNLDA

.LL10:
	sll	IS, BASE_SHIFT, I
	sub	M, IS, MIN_M
	cmp	MIN_M, P
	nop
	movg	%icc, P, MIN_M
	nop
	cmp	INCX, SIZE
	beq	.LL100
	add	X, I, XP

	sra	MIN_M, 2, I
	mov	BUFFER, XP
	cmp	I, 0
	ble,pn	%icc, .LL15
	mov	BUFFER, Y1

.LL11:
	LDF	[X], a1
	add	X, INCX, X
	LDF	[X], a2
	add	X, INCX, X
	LDF	[X], a3
	add	X, INCX, X
	LDF	[X], a4
	add	X, INCX, X

	STF	a1, [Y1 + 0 * SIZE]
	add	I, -1, I
	STF	a2, [Y1 + 1 * SIZE]
	cmp	I, 0
	STF	a3, [Y1 + 2 * SIZE]
	STF	a4, [Y1 + 3 * SIZE]
	bg,pn	%icc, .LL11
	add	Y1, 4 * SIZE, Y1

.LL15:
	and	MIN_M, 3, I
	cmp	I, 0
	ble,pn	%icc, .LL100
	nop

.LL16:
	LDF	[X], a1
	add	X, INCX, X
	add	I, -1, I
	cmp	I, 0
	nop
	STF	a1, [Y1]
	bg,pn	%icc, .LL16
	add	Y1, 1 * SIZE, Y1

.LL100:
	sra	N, 1, J
	cmp	J, 0
	ble	%icc, .LL200
	mov	Y, Y1

.LL110:
#ifdef DOUBLE
	FCLR(29)
#else
	FCLR(30)
#endif

	FMOV	FZERO, c1
	FMOV	FZERO, c2
	FMOV	FZERO, c3
	FMOV	FZERO, c4

	FMOV	FZERO, t1
	FMOV	FZERO, t2
	FMOV	FZERO, t3
	FMOV	FZERO, t4

	mov	A,  A1
	add	A,  LDA, A2
	add	A2, LDA, A

	mov	XP, X1

	sra	MIN_M, 3, I
	cmp	I, 0
	ble	%icc, .LL115
	prefetch [Y1 + 2 * SIZE], 0

	LDF	[A1 +  0 * SIZE], a1
	deccc	I
	LDF	[A1 +  1 * SIZE], a2
	LDF	[A1 +  2 * SIZE], a3
	LDF	[A1 +  3 * SIZE], a4
	LDF	[A1 +  4 * SIZE], a5
	LDF	[A1 +  5 * SIZE], a6
	LDF	[A1 +  6 * SIZE], a7
	LDF	[A1 +  7 * SIZE], a8

	LDF	[A2 +  0 * SIZE], a9
	LDF	[A2 +  1 * SIZE], a10
	LDF	[A2 +  2 * SIZE], a11
	LDF	[A2 +  3 * SIZE], a12
	LDF	[A2 +  4 * SIZE], a13
	LDF	[A2 +  5 * SIZE], a14
	LDF	[A2 +  6 * SIZE], a15
	LDF	[A2 +  7 * SIZE], a16

	LDF	[X1 +  0 * SIZE], b1
	LDF	[X1 +  1 * SIZE], b2
	LDF	[X1 +  2 * SIZE], b3
	LDF	[X1 +  3 * SIZE], b4
	LDF	[X1 +  4 * SIZE], b5
	LDF	[X1 +  5 * SIZE], b6

	ble	%icc, .LL112
	LDF	[X1 +  6 * SIZE], b7

.LL111:
	FADD	c1,  t1,  c1
	prefetch  [A1 +  PREFETCHSIZE * SIZE], 1
	FMUL	a1,  b1,  t1
	LDF	[A1 +  8 * SIZE], a1

	FADD	c2,  t2,  c2
	LDF	[X1 +  7 * SIZE], b8
	FMUL	a9,  b1,  t2
	LDF	[A2 +  8 * SIZE], a9

 	FADD	c3,  t3,  c3
	LDF	[X1 +  8 * SIZE], b1
	FMUL	a2,  b2,  t3
	LDF	[A1 +  9 * SIZE], a2

	FADD	c4,  t4,  c4
	deccc	I
	FMUL	a10, b2,  t4
	LDF	[A2 +  9 * SIZE], a10

	FADD	c1,  t1,  c1
	LDF	[X1 +  9 * SIZE], b2
	FMUL	a3,  b3,  t1
	LDF	[A1 + 10 * SIZE], a3

	FADD	c2,  t2,  c2
	nop
	FMUL	a11, b3,  t2
	LDF	[A2 + 10 * SIZE], a11

	FADD	c3,  t3,  c3
	LDF	[X1 + 10 * SIZE], b3
	FMUL	a4,  b4,  t3
	LDF	[A1 + 11 * SIZE], a4

	FADD	c4,  t4,  c4
	nop
	FMUL	a12, b4,  t4
	LDF	[A2 + 11 * SIZE], a12

	FADD	c1,  t1,  c1
	LDF	[X1 + 11 * SIZE], b4
	FMUL	a5,  b5,  t1
	LDF	[A1 + 12 * SIZE], a5

	FADD	c2,  t2,  c2
	prefetch  [A2 +  (PREFETCHSIZE + 4) * SIZE], 1
	FMUL	a13, b5,  t2
	LDF	[A2 + 12 * SIZE], a13

	FADD	c3,  t3,  c3
	LDF	[X1 + 12 * SIZE], b5
	FMUL	a6,  b6,  t3
	LDF	[A1 + 13 * SIZE], a6

	FADD	c4,  t4,  c4
	FMUL	a14, b6,  t4
	LDF	[A2 + 13 * SIZE], a14

	FADD	c1,  t1,  c1
	LDF	[X1 + 13 * SIZE], b6
	FMUL	a7,  b7,  t1
	LDF	[A1 + 14 * SIZE], a7

	FADD	c2,  t2,  c2
	add	X1, 8 * SIZE, X1
	FMUL	a15, b7,  t2
	LDF	[A2 + 14 * SIZE], a15

	FADD	c3,  t3,  c3
	LDF	[X1 +  6 * SIZE], b7
	FMUL	a8,  b8,  t3
	LDF	[A1 + 15 * SIZE], a8

	FADD	c4,  t4,  c4
	add	A1, 8 * SIZE, A1
	FMUL	a16, b8,  t4
	LDF	[A2 + 15 * SIZE], a16

	bg,pn	%icc, .LL111
	add	A2, 8 * SIZE, A2

.LL112:
	FADD	c1,  t1,  c1
	LDF	[X1 + 7 * SIZE], b8
	FMUL	a1,  b1,  t1
	add	A1, 8 * SIZE, A1

	FADD	c2,  t2,  c2
	add	A2, 8 * SIZE, A2
	FMUL	a9,  b1,  t2
	add	X1, 8 * SIZE, X1

	FADD	c3,  t3,  c3
	FMUL	a2,  b2,  t3
	FADD	c4,  t4,  c4
	FMUL	a10, b2,  t4

	FADD	c1,  t1,  c1
	FMUL	a3,  b3,  t1
	FADD	c2,  t2,  c2
	FMUL	a11, b3,  t2

	FADD	c3,  t3,  c3
	FMUL	a4,  b4,  t3
	FADD	c4,  t4,  c4
	FMUL	a12, b4,  t4

	FADD	c1,  t1,  c1
	FMUL	a5,  b5,  t1
	FADD	c2,  t2,  c2
	FMUL	a13, b5,  t2

	FADD	c3,  t3,  c3
	FMUL	a6,  b6,  t3
	FADD	c4,  t4,  c4
	FMUL	a14, b6,  t4

	FADD	c1,  t1,  c1
	FMUL	a7,  b7,  t1
	FADD	c2,  t2,  c2
	FMUL	a15, b7,  t2

	FADD	c3,  t3,  c3
	FMUL	a8,  b8,  t3
	FADD	c4,  t4,  c4
	FMUL	a16, b8,  t4

.LL115:
	andcc	MIN_M, 7, I
	ble	%icc, .LL119
	mov	Y1, Y2

	LDF	[X1 + 0 * SIZE], b1
	deccc	I
	LDF	[A1 + 0 * SIZE], a1
	ble	%icc, .LL117
	LDF	[A2 + 0 * SIZE], a2

.LL116:
	FADD	c1, t1, c1
	add	X1, 1 * SIZE, X1
	FMUL	a1, b1, t1
	LDF	[A1 + 1 * SIZE], a1

	FADD	c2, t2, c2
	add	A1, 1 * SIZE, A1
	FMUL	a2, b1, t2
	LDF	[X1 + 0 * SIZE], b1

	add	A2, 1 * SIZE, A2
	deccc	I
	bg,pn	%icc, .LL116
	LDF	[A2 + 0 * SIZE], a2

.LL117:
	FADD	c1, t1, c1
	add	X1, 1 * SIZE, X1
	FADD	c2, t2, c2
	add	A1, 1 * SIZE, A1

	FMUL	a1, b1, t1
	add	A2, 1 * SIZE, A2
	FMUL	a2, b1, t2
	nop

.LL119:
	FADD	c1, t1, c1
	FADD	c2, t2, c2
	FADD	c3, t3, c3
	FADD	c4, t4, c4

	FADD	c1, c3, c1
	FADD	c2, c4, c2


	LDF	[Y1], a1
	LDF	[Y1 + INCY], a2

	add	Y1, INCY, Y1
	add	Y1, INCY, Y1

	LDF	STACK_ALPHA, ALPHA

	FMUL	ALPHA, c1, c1
	FMUL	ALPHA, c2, c2
	FADD	a1, c1, a1
	FADD	a2, c2, a2

	STF	a1, [Y2]
	STF	a2, [Y2 + INCY]

	deccc	J
	bg	%icc, .LL110
#ifdef DOUBLE
	FCLR(29)
#else
	FCLR(30)
#endif

.LL200:
	andcc	N, 1, J
	nop
	ble	%icc, .LL400
	FMOV	FZERO, c1

.LL310:
	FMOV	FZERO, t1
	sra	MIN_M, 3, I
	FMOV	FZERO, c2
	mov	A, A1
	FMOV	FZERO, t2
	add	A, LDA, A
	FMOV	FZERO, t3
	cmp	I, 0
	FMOV	FZERO, t4
	ble	%icc, .LL315
	mov	XP, X1

	LDF	[A1 + 0 * SIZE], a1
	LDF	[A1 + 1 * SIZE], a2
	LDF	[A1 + 2 * SIZE], a3
	LDF	[A1 + 3 * SIZE], a4
	LDF	[A1 + 4 * SIZE], a5
	LDF	[A1 + 5 * SIZE], a6
	LDF	[A1 + 6 * SIZE], a7
	LDF	[A1 + 7 * SIZE], a8
	add	A1, 8 * SIZE, A1

	LDF	[X1 + 0 * SIZE], a9
	add	I, -1, I
	LDF	[X1 + 1 * SIZE], a10
	cmp	I, 0
	LDF	[X1 + 2 * SIZE], a11
	LDF	[X1 + 3 * SIZE], a12
	LDF	[X1 + 4 * SIZE], a13
	LDF	[X1 + 5 * SIZE], a14
	LDF	[X1 + 6 * SIZE], a15
	LDF	[X1 + 7 * SIZE], a16
	ble	%icc, .LL312
	add	X1, 8 * SIZE, X1

.LL311:
	prefetch [A1 + PREFETCHSIZE * SIZE], 1

	FADD	c1, t1, c1
	FMUL	a1, a9, t1
	LDF	[A1 + 0 * SIZE], a1
	LDF	[X1 + 0 * SIZE], a9

	FADD	c2, t2, c2
	FMUL	a2, a10, t2
	LDF	[A1 + 1 * SIZE], a2
	LDF	[X1 + 1 * SIZE], a10

	FADD	c1, t3, c1
	add	I, -1, I
	FMUL	a3, a11, t3
	LDF	[A1 + 2 * SIZE], a3
	LDF	[X1 + 2 * SIZE], a11

	FADD	c2, t4, c2
	cmp	I, 0
	FMUL	a4, a12, t4
	LDF	[A1 + 3 * SIZE], a4
	LDF	[X1 + 3 * SIZE], a12

	FADD	c1, t1, c1
	nop
	FMUL	a5, a13, t1
	LDF	[A1 + 4 * SIZE], a5
	LDF	[X1 + 4 * SIZE], a13

	FADD	c2, t2, c2
	nop
	FMUL	a6, a14, t2
	LDF	[A1 + 5 * SIZE], a6
	LDF	[X1 + 5 * SIZE], a14

	FADD	c1, t3, c1
	FMUL	a7, a15, t3
	LDF	[A1 + 6 * SIZE], a7
	LDF	[X1 + 6 * SIZE], a15

	FADD	c2, t4, c2
	add	X1, 8 * SIZE, X1
	FMUL	a8, a16, t4
	LDF	[A1 + 7 * SIZE], a8
	add	A1, 8 * SIZE, A1
	bg,pn	%icc, .LL311
	LDF	[X1 - 1 * SIZE], a16

.LL312:
	FADD	c1, t1, c1
	FMUL	a1, a9, t1
	FADD	c2, t2, c2
	FMUL	a2, a10, t2
	FADD	c1, t3, c1
	FMUL	a3, a11, t3
	FADD	c2, t4, c2
	FMUL	a4, a12, t4

	FADD	c1, t1, c1
	FMUL	a5, a13, t1
	FADD	c2, t2, c2
	FMUL	a6, a14, t2
	FADD	c1, t3, c1
	FMUL	a7, a15, t3
	FADD	c2, t4, c2
	FMUL	a8, a16, t4

.LL315:
	and	MIN_M, 7, I
	cmp	I, 0
	ble	%icc, .LL319
	nop

.LL316:
	LDF	[A1 + 0 * SIZE], a1
	add	A1, 1 * SIZE, A1
	LDF	[X1 + 0 * SIZE], b1
	nop

	FADD	c1, t1, c1
	nop
	add	I, -1, I
	FMUL	a1, b1, t1
	nop
	cmp	I, 0
	bg,pn	%icc, .LL316
	add	X1, 1 * SIZE, X1

.LL319:
	FADD	c1, t1, c1
	nop
	FADD	c2, t2, c2
	nop
	FADD	c1, t3, c1
	FADD	c2, t4, c2

	FADD	c1, c2, c1

	FMUL	ALPHA, c1, c1
	LDF	[Y1 + 0 * SIZE], a1
	FADD	a1, c1, a1
	STF	a1, [Y1 + 0 * SIZE]
	add	Y1, INCY, Y1

.LL400:
	add	IS, P, IS
	cmp	IS, M
	bl	%icc, .LL10
	add	A, PNLDA, A

.LL999:
	return	%i7 + 8
	clr	%o0

	EPILOGUE
