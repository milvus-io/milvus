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

#define P 4000

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
#define c5	%f16
#define c6	%f18
#define c7	%f20
#define c8	%f22
#define c9	%f24
#define c10	%f26
#define c11	%f28
#define c12	%f30
#define c13	%f32
#define c14	%f34
#define c15	%f36
#define c16	%f38

#define a1	%f40
#define a2	%f42
#define a3	%f44
#define a4	%f46
#define a5	%f48
#define a6	%f50
#define a7	%f52
#define a8	%f54

#define b1	%f56
#define b2	%f58
#define b3	%f60
#define b4	%f62
#else
#define t1	%f0
#define	t2 	%f1
#define t3	%f2
#define	t4 	%f3

#define c1	%f4
#define c2	%f5
#define c3	%f6
#define c4	%f7
#define c5	%f8
#define c6	%f9
#define c7	%f10
#define c8	%f11
#define c9	%f12
#define c10	%f13
#define c11	%f14
#define c12	%f15
#define c13	%f16
#define c14	%f17
#define c15	%f18
#define c16	%f19

#define a1	%f20
#define a2	%f21
#define a3	%f22
#define a4	%f23
#define a5	%f24
#define a6	%f25
#define a7	%f26
#define a8	%f27

#define b1	%f28
#define b2	%f29
#define b3	%f30
#define b4	%f31
#endif

#ifndef __64BIT__
#define ALPHA_R	[%sp + STACK_START + 16]
#ifndef DOUBLE
#define ALPHA_I	[%sp + STACK_START + 20]
#else
#define ALPHA_I	[%sp + STACK_START + 24]
#endif
#else
#define ALPHA_R	[%sp + STACK_START + 32]
#define ALPHA_I	[%sp + STACK_START + 40]
#endif

#ifdef DOUBLE
#define PREFETCHSIZE 18
#else
#define PREFETCHSIZE 36
#endif

	PROLOGUE
	SAVESP
	nop

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
	std	%f6, ALPHA_R
	std	%f8, ALPHA_I
#else
	st	%f7, ALPHA_R
	st	%f9, ALPHA_I
#endif
#endif

	clr	IS
	mov	P, I
	sll	LDA, ZBASE_SHIFT, LDA
	sll	I, ZBASE_SHIFT, I
	smul	LDA, N, PNLDA
	sll	INCX, ZBASE_SHIFT, INCX
	sll	INCY, ZBASE_SHIFT, INCY
	sub	I, PNLDA, PNLDA

.LL10:
	sll	IS, ZBASE_SHIFT, I
	sub	M, IS, MIN_M
	mov	P, J

	cmp	MIN_M, J
	nop
	movg	%icc, J, MIN_M
	nop
	cmp	INCX, 2 * SIZE
	beq	.LL100
	add	X, I, XP

	sra	MIN_M, 2, I
	mov	BUFFER, XP
	cmp	I, 0
	ble,pn	%icc, .LL15
	mov	BUFFER, Y1

.LL11:
	LDF	[X + 0 * SIZE], a1
	LDF	[X + 1 * SIZE], a2
	add	X, INCX, X
	LDF	[X + 0 * SIZE], a3
	LDF	[X + 1 * SIZE], a4
	add	X, INCX, X
	LDF	[X + 0 * SIZE], a5
	LDF	[X + 1 * SIZE], a6
	add	X, INCX, X
	LDF	[X + 0 * SIZE], a7
	LDF	[X + 1 * SIZE], a8
	add	X, INCX, X

	STF	a1, [Y1 + 0 * SIZE]
	add	I, -1, I
	STF	a2, [Y1 + 1 * SIZE]
	cmp	I, 0
	STF	a3, [Y1 + 2 * SIZE]
	STF	a4, [Y1 + 3 * SIZE]
	STF	a5, [Y1 + 4 * SIZE]
	STF	a6, [Y1 + 5 * SIZE]
	STF	a7, [Y1 + 6 * SIZE]
	STF	a8, [Y1 + 7 * SIZE]
	bg,pn	%icc, .LL11
	add	Y1, 8 * SIZE, Y1

.LL15:
	and	MIN_M, 3, I
	cmp	I, 0
	ble,pn	%icc, .LL100
	nop

.LL16:
	LDF	[X + 0 * SIZE], a1
	LDF	[X + 1 * SIZE], a2
	add	X, INCX, X
	add	I, -1, I
	cmp	I, 0
	nop
	STF	a1, [Y1 + 0 * SIZE]
	STF	a2, [Y1 + 1 * SIZE]
	bg,pn	%icc, .LL16
	add	Y1, 2 * SIZE, Y1

.LL100:
	sra	N, 2, J
	cmp	J, 0
	ble	%icc, .LL200
	mov	Y, Y1

.LL110:
	FCLR(0)

	FMOV	t1, c1
	sra	MIN_M, 2, I
	FMOV	t1, c2
	add	A,  LDA, A2
	FMOV	t1, c3
	mov	A,  A1
	FMOV	t1, c4
	add	A2, LDA, A3

	FMOV	t1, c5
	FMOV	t1, c6
	FMOV	t1, c7
	FMOV	t1, c8
	FMOV	t1, c9
	FMOV	t1, c10
	FMOV	t1, c11
	FMOV	t1, c12
	FMOV	t1, c13
	FMOV	t1, c14
	FMOV	t1, c15
	FMOV	t1, c16

	add	A3, LDA, A4
	FMOV	t1, t2
	mov	XP, X1
	FMOV	t1, t3
	add	A4, LDA, A
	cmp	I, 0
	ble	%icc, .LL115
	FMOV	t1, t4

	LDF	[A1 + 0 * SIZE], a1
	nop
	LDF	[A1 + 1 * SIZE], a2
	add	A1, 2 * SIZE, A1
	LDF	[A2 + 0 * SIZE], a3
	LDF	[A2 + 1 * SIZE], a4
	add	A2, 2 * SIZE, A2
	LDF	[A3 + 0 * SIZE], a5
	LDF	[A3 + 1 * SIZE], a6
	add	A3, 2 * SIZE, A3
	LDF	[A4 + 0 * SIZE], a7
	LDF	[A4 + 1 * SIZE], a8
	add	A4, 2 * SIZE, A4

	LDF	[X1 + 0 * SIZE], b1
	nop
	LDF	[X1 + 1 * SIZE], b2
	nop
	LDF	[X1 + 2 * SIZE], b3
	add	X1, 4 * SIZE, X1

	deccc	 I
	ble	 .LL112
	prefetch [Y1 + 7 * SIZE], 2

#ifndef XCONJ
#define FADDX	FADD
#else
#define FADDX	FSUB
#endif

.LL111:
	FADD	c13, t1, c13
	prefetch [A1 + PREFETCHSIZE * SIZE], 1
	FMUL	a1, b1, t1
	nop

	FADDX	c14, t2, c14
	nop
	FMUL	a1, b2, t2
	LDF	[A1 + 0 * SIZE], a1

	FADD	c15, t3, c15
	nop
	FMUL	a2, b1, t3
	LDF	[X1 - 1 * SIZE], b4

	FADD	c16, t4, c16
	nop
	FMUL	a2, b2, t4
	LDF	[A1 + 1 * SIZE], a2

	FADD	c1, t1, c1
	nop
	FMUL	a3, b1, t1
	nop

	FADDX	c2, t2, c2
	nop
	FMUL	a3, b2, t2
	LDF	[A2 + 0 * SIZE], a3

	FADD	c3, t3, c3
	nop
	FMUL	a4, b1, t3
	nop

	FADD	c4, t4, c4
	nop
	FMUL	a4, b2, t4
	LDF	[A2 + 1 * SIZE], a4

	FADD	c5, t1, c5
	nop
	FMUL	a5, b1, t1
	nop

	FADDX	c6, t2, c6
	nop
	FMUL	a5, b2, t2
	LDF	[A3 + 0 * SIZE], a5

	FADD	c7, t3, c7
	nop
	FMUL	a6, b1, t3
	nop

	FADD	c8, t4, c8
	nop
	FMUL	a6, b2, t4
	LDF	[A3 + 1 * SIZE], a6

	FADD	c9, t1, c9
	nop
	FMUL	a7, b1, t1
	nop

	FADDX	c10, t2, c10
	nop
	FMUL	a7, b2, t2
	LDF	[A4 + 0 * SIZE], a7

	FADD	c11, t3, c11
	nop
	FMUL	a8, b1, t3
	LDF	[X1 + 0 * SIZE], b1

	FADD	c12, t4, c12
	nop
	FMUL	a8, b2, t4
	LDF	[A4 + 1 * SIZE], a8

	FADD	c13, t1, c13
	nop
	FMUL	a1, b3, t1
	prefetch [A2 + PREFETCHSIZE * SIZE], 1

	FADDX	c14, t2, c14
	nop
	FMUL	a1, b4, t2
	LDF	[A1 + 2 * SIZE], a1

	FADD	c15, t3, c15
	nop
	FMUL	a2, b3, t3
	LDF	[X1 + 1 * SIZE], b2

	FADD	c16, t4, c16
	nop
	FMUL	a2, b4, t4
	LDF	[A1 + 3 * SIZE], a2

	FADD	c1, t1, c1
	nop
	FMUL	a3, b3, t1
	nop

	FADDX	c2, t2, c2
	nop
	FMUL	a3, b4, t2
	LDF	[A2 + 2 * SIZE], a3

	FADD	c3, t3, c3
	nop
	FMUL	a4, b3, t3
	nop

	FADD	c4, t4, c4
	nop
	FMUL	a4, b4, t4
	LDF	[A2 + 3 * SIZE], a4

	FADD	c5, t1, c5
	nop
	FMUL	a5, b3, t1
	nop

	FADDX	c6, t2, c6
	nop
	FMUL	a5, b4, t2
	LDF	[A3 + 2 * SIZE], a5

	FADD	c7, t3, c7
	nop
	FMUL	a6, b3, t3
	nop

	FADD	c8, t4, c8
	nop
	FMUL	a6, b4, t4
	LDF	[A3 + 3 * SIZE], a6

	FADD	c9, t1, c9
	nop
	FMUL	a7, b3, t1
	nop

	FADDX	c10, t2, c10
	nop
	FMUL	a7, b4, t2
	LDF	[A4 + 2 * SIZE], a7

	FADD	c11, t3, c11
	nop
	FMUL	a8, b3, t3
	LDF	[X1 + 2 * SIZE], b3

	FADD	c12, t4, c12
	nop
	FMUL	a8, b4, t4
	LDF	[A4 + 3 * SIZE], a8

	FADD	c13, t1, c13
	prefetch [A3 + PREFETCHSIZE * SIZE], 1
	FMUL	a1, b1, t1
	nop

	FADDX	c14, t2, c14
	nop
	FMUL	a1, b2, t2
	LDF	[A1 + 4 * SIZE], a1

	FADD	c15, t3, c15
	nop
	FMUL	a2, b1, t3
	LDF	[X1 + 3 * SIZE], b4

	FADD	c16, t4, c16
	nop
	FMUL	a2, b2, t4
	LDF	[A1 + 5 * SIZE], a2

	FADD	c1, t1, c1
	nop
	FMUL	a3, b1, t1
	nop

	FADDX	c2, t2, c2
	nop
	FMUL	a3, b2, t2
	LDF	[A2 + 4 * SIZE], a3

	FADD	c3, t3, c3
	nop
	FMUL	a4, b1, t3
	nop

	FADD	c4, t4, c4
	nop
	FMUL	a4, b2, t4
	LDF	[A2 + 5 * SIZE], a4

	FADD	c5, t1, c5
	nop
	FMUL	a5, b1, t1
	nop

	FADDX	c6, t2, c6
	nop
	FMUL	a5, b2, t2
	LDF	[A3 + 4 * SIZE], a5

	FADD	c7, t3, c7
	deccc	I
	FMUL	a6, b1, t3
	nop

	FADD	c8, t4, c8
	nop
	FMUL	a6, b2, t4
	LDF	[A3 + 5 * SIZE], a6

	FADD	c9, t1, c9
	nop
	FMUL	a7, b1, t1
	nop

	FADDX	c10, t2, c10
	nop
	FMUL	a7, b2, t2
	LDF	[A4 + 4 * SIZE], a7

	FADD	c11, t3, c11
	nop
	FMUL	a8, b1, t3
	LDF	[X1 + 4 * SIZE], b1

	FADD	c12, t4, c12
	nop
	FMUL	a8, b2, t4
	LDF	[A4 + 5 * SIZE], a8

	FADD	c13, t1, c13
	prefetch [A4 + PREFETCHSIZE * SIZE], 1
	FMUL	a1, b3, t1
	nop

	FADDX	c14, t2, c14
	nop
	FMUL	a1, b4, t2
	LDF	[A1 + 6 * SIZE], a1

	FADD	c15, t3, c15
	nop
	FMUL	a2, b3, t3
	LDF	[X1 + 5 * SIZE], b2

	FADD	c16, t4, c16
	nop
	FMUL	a2, b4, t4
	LDF	[A1 + 7 * SIZE], a2

	FADD	c1, t1, c1
	add	A1, 8 * SIZE, A1
	FMUL	a3, b3, t1
	nop

	FADDX	c2, t2, c2
	nop
	FMUL	a3, b4, t2
	LDF	[A2 + 6 * SIZE], a3

	FADD	c3, t3, c3
	nop
	FMUL	a4, b3, t3
	nop

	FADD	c4, t4, c4
	nop
	FMUL	a4, b4, t4
	LDF	[A2 + 7 * SIZE], a4

	FADD	c5, t1, c5
	add	A2, 8 * SIZE, A2
	FMUL	a5, b3, t1
	nop

	FADDX	c6, t2, c6
	nop
	FMUL	a5, b4, t2
	LDF	[A3 + 6 * SIZE], a5

	FADD	c7, t3, c7
	add	A4, 8 * SIZE, A4
	FMUL	a6, b3, t3
	nop

	FADD	c8, t4, c8
	nop
	FMUL	a6, b4, t4
	LDF	[A3 + 7 * SIZE], a6

	FADD	c9, t1, c9
	add	A3, 8 * SIZE, A3
	FMUL	a7, b3, t1
	nop

	FADDX	c10, t2, c10
	add	X1, 8 * SIZE, X1
	FMUL	a7, b4, t2
	LDF	[A4 - 2 * SIZE], a7

	FADD	c11, t3, c11
	nop
	FMUL	a8, b3, t3
	LDF	[X1 - 2 * SIZE], b3

	FADD	c12, t4, c12
	FMUL	a8, b4, t4
	bg,pn	%icc, .LL111
	LDF	[A4 - 1 * SIZE], a8

.LL112:
	FADD	c13, t1, c13
	nop
	FMUL	a1, b1, t1
	LDF	[X1 - 1 * SIZE], b4

	FADDX	c14, t2, c14
	nop
	FMUL	a1, b2, t2
	LDF	[A1 + 0 * SIZE], a1

	FADD	c15, t3, c15
	nop
	FMUL	a2, b1, t3
	LDF	[X1 - 1 * SIZE], b4

	FADD	c16, t4, c16
	nop
	FMUL	a2, b2, t4
	LDF	[A1 + 1 * SIZE], a2

	FADD	c1, t1, c1
	nop
	FMUL	a3, b1, t1
	nop

	FADDX	c2, t2, c2
	nop
	FMUL	a3, b2, t2
	LDF	[A2 + 0 * SIZE], a3

	FADD	c3, t3, c3
	nop
	FMUL	a4, b1, t3
	nop

	FADD	c4, t4, c4
	nop
	FMUL	a4, b2, t4
	LDF	[A2 + 1 * SIZE], a4

	FADD	c5, t1, c5
	nop
	FMUL	a5, b1, t1
	nop

	FADDX	c6, t2, c6
	nop
	FMUL	a5, b2, t2
	LDF	[A3 + 0 * SIZE], a5

	FADD	c7, t3, c7
	nop
	FMUL	a6, b1, t3
	nop

	FADD	c8, t4, c8
	nop
	FMUL	a6, b2, t4
	LDF	[A3 + 1 * SIZE], a6

	FADD	c9, t1, c9
	nop
	FMUL	a7, b1, t1
	nop

	FADDX	c10, t2, c10
	nop
	FMUL	a7, b2, t2
	LDF	[A4 + 0 * SIZE], a7

	FADD	c11, t3, c11
	nop
	FMUL	a8, b1, t3
	LDF	[X1 + 0 * SIZE], b1

	FADD	c12, t4, c12
	nop
	FMUL	a8, b2, t4
	LDF	[A4 + 1 * SIZE], a8

	FADD	c13, t1, c13
	nop
	FMUL	a1, b3, t1
	LDF	[X1 + 1 * SIZE], b2

	FADDX	c14, t2, c14
	nop
	FMUL	a1, b4, t2
	LDF	[A1 + 2 * SIZE], a1

	FADD	c15, t3, c15
	nop
	FMUL	a2, b3, t3
	nop

	FADD	c16, t4, c16
	nop
	FMUL	a2, b4, t4
	LDF	[A1 + 3 * SIZE], a2

	FADD	c1, t1, c1
	nop
	FMUL	a3, b3, t1
	nop

	FADDX	c2, t2, c2
	nop
	FMUL	a3, b4, t2
	LDF	[A2 + 2 * SIZE], a3

	FADD	c3, t3, c3
	nop
	FMUL	a4, b3, t3
	nop

	FADD	c4, t4, c4
	nop
	FMUL	a4, b4, t4
	LDF	[A2 + 3 * SIZE], a4

	FADD	c5, t1, c5
	nop
	FMUL	a5, b3, t1
	nop

	FADDX	c6, t2, c6
	nop
	FMUL	a5, b4, t2
	LDF	[A3 + 2 * SIZE], a5

	FADD	c7, t3, c7
	nop
	FMUL	a6, b3, t3
	nop

	FADD	c8, t4, c8
	nop
	FMUL	a6, b4, t4
	LDF	[A3 + 3 * SIZE], a6

	FADD	c9, t1, c9
	nop
	FMUL	a7, b3, t1
	nop

	FADDX	c10, t2, c10
	nop
	FMUL	a7, b4, t2
	LDF	[A4 + 2 * SIZE], a7

	FADD	c11, t3, c11
	nop
	FMUL	a8, b3, t3
	LDF	[X1 + 2 * SIZE], b3

	FADD	c12, t4, c12
	nop
	FMUL	a8, b4, t4
	LDF	[A4 + 3 * SIZE], a8

	FADD	c13, t1, c13
	nop
	FMUL	a1, b1, t1
	LDF	[X1 + 3 * SIZE], b4

	FADDX	c14, t2, c14
	add	X1, 4 * SIZE, X1
	FMUL	a1, b2, t2
	LDF	[A1 + 4 * SIZE], a1

	FADD	c15, t3, c15
	nop
	FMUL	a2, b1, t3
	nop

	FADD	c16, t4, c16
	nop
	FMUL	a2, b2, t4
	LDF	[A1 + 5 * SIZE], a2

	FADD	c1, t1, c1
	add	A1, 6 * SIZE, A1
	FMUL	a3, b1, t1
	nop

	FADDX	c2, t2, c2
	nop
	FMUL	a3, b2, t2
	LDF	[A2 + 4 * SIZE], a3

	FADD	c3, t3, c3
	nop
	FMUL	a4, b1, t3
	nop

	FADD	c4, t4, c4
	nop
	FMUL	a4, b2, t4
	LDF	[A2 + 5 * SIZE], a4

	FADD	c5, t1, c5
	add	A2, 6 * SIZE, A2
	FMUL	a5, b1, t1
	nop

	FADDX	c6, t2, c6
	nop
	FMUL	a5, b2, t2
	LDF	[A3 + 4 * SIZE], a5

	FADD	c7, t3, c7
	nop
	FMUL	a6, b1, t3
	nop

	FADD	c8, t4, c8
	nop
	FMUL	a6, b2, t4
	LDF	[A3 + 5 * SIZE], a6

	FADD	c9, t1, c9
	add	A3, 6 * SIZE, A3
	FMUL	a7, b1, t1
	nop

	FADDX	c10, t2, c10
	nop
	FMUL	a7, b2, t2
	LDF	[A4 + 4 * SIZE], a7

	FADD	c11, t3, c11
	nop
	FMUL	a8, b1, t3
	nop

	FADD	c12, t4, c12
	nop
	FMUL	a8, b2, t4
	LDF	[A4 + 5 * SIZE], a8

	FADD	c13, t1, c13
	add	A4, 6 * SIZE, A4
	FMUL	a1, b3, t1
	nop

	FADDX	c14, t2, c14
	nop
	FMUL	a1, b4, t2
	nop

	FADD	c15, t3, c15
	FMUL	a2, b3, t3
	FADD	c16, t4, c16
	FMUL	a2, b4, t4

	FADD	c1, t1, c1
	FMUL	a3, b3, t1
	FADDX	c2, t2, c2
	FMUL	a3, b4, t2
	FADD	c3, t3, c3
	FMUL	a4, b3, t3
	FADD	c4, t4, c4
	FMUL	a4, b4, t4

	FADD	c5, t1, c5
	FMUL	a5, b3, t1
	FADDX	c6, t2, c6
	FMUL	a5, b4, t2
	FADD	c7, t3, c7
	FMUL	a6, b3, t3
	FADD	c8, t4, c8
	FMUL	a6, b4, t4

	FADD	c9, t1, c9
	FMUL	a7, b3, t1
	FADDX	c10, t2, c10
	FMUL	a7, b4, t2
	FADD	c11, t3, c11
	FMUL	a8, b3, t3
	FADD	c12, t4, c12
	FMUL	a8, b4, t4

.LL115:
	andcc	MIN_M, 3, I
	LDF	ALPHA_R, b3
	mov	Y1, Y2
	ble,pn	%icc, .LL119
	LDF	ALPHA_I, b4

.L116:
	LDF	[A1 + 0 * SIZE], a1
	LDF	[A1 + 1 * SIZE], a2
	add	A1, 2 * SIZE, A1
	LDF	[X1 + 0 * SIZE], b1
	LDF	[X1 + 1 * SIZE], b2
	add	X1, 2 * SIZE, X1
	LDF	[A2 + 0 * SIZE], a3
	LDF	[A2 + 1 * SIZE], a4
	add	A2, 2 * SIZE, A2
	LDF	[A3 + 0 * SIZE], a5
	LDF	[A3 + 1 * SIZE], a6
	add	A3, 2 * SIZE, A3
	LDF	[A4 + 0 * SIZE], a7
	LDF	[A4 + 1 * SIZE], a8
	add	A4, 2 * SIZE, A4

	FADD	c13, t1, c13
	FMUL	a1, b1, t1
	FADDX	c14, t2, c14
	FMUL	a1, b2, t2
	FADD	c15, t3, c15
	FMUL	a2, b1, t3
	FADD	c16, t4, c16
	FMUL	a2, b2, t4

	FADD	c1, t1, c1
	FMUL	a3, b1, t1
	FADDX	c2, t2, c2
	FMUL	a3, b2, t2
	FADD	c3, t3, c3
	FMUL	a4, b1, t3
	FADD	c4, t4, c4
	FMUL	a4, b2, t4

	FADD	c5, t1, c5
	FMUL	a5, b1, t1
	FADDX	c6, t2, c6
	FMUL	a5, b2, t2
	FADD	c7, t3, c7
	FMUL	a6, b1, t3
	FADD	c8, t4, c8
	FMUL	a6, b2, t4

	FADD	c9, t1, c9
	FMUL	a7, b1, t1
	FADDX	c10, t2, c10
	FMUL	a7, b2, t2
	FADD	c11, t3, c11
	FMUL	a8, b1, t3
	FADD	c12, t4, c12
	FMUL	a8, b2, t4

	deccc	I
	bg	%icc, .L116
	nop

.LL119:
	FADD	c13, t1, c13
	LDF	[Y1 + 0 * SIZE], a1
	FADDX	c14, t2, c14
	LDF	[Y1 + 1 * SIZE] ,a2
	add	Y1, INCY, Y1
	FADD	c15, t3, c15
	LDF	[Y1 + 0 * SIZE], a3
	FADD	c16, t4, c16
	LDF	[Y1 + 1 * SIZE] ,a4
	add	Y1, INCY, Y1

#if (!defined(XCONJ) && !defined(CONJ)) || (defined(XCONJ) && defined(CONJ))
	FSUB	c1,  c4,  c1
	LDF	[Y1 + 0 * SIZE], a5
	FSUB	c5,  c8,  c5
	LDF	[Y1 + 1 * SIZE] ,a6
	add	Y1, INCY, Y1
	FSUB	c9,  c12, c9
	LDF	[Y1 + 0 * SIZE], a7
	FSUB	c13, c16, c13
	LDF	[Y1 + 1 * SIZE] ,a8
	add	Y1, INCY, Y1
#else
	FADD	c1,  c4,  c1
	LDF	[Y1 + 0 * SIZE], a5
	FADD	c5,  c8,  c5
	LDF	[Y1 + 1 * SIZE] ,a6
	add	Y1, INCY, Y1
	FADD	c9,  c12, c9
	LDF	[Y1 + 0 * SIZE], a7
	FADD	c13, c16, c13
	LDF	[Y1 + 1 * SIZE] ,a8
	add	Y1, INCY, Y1
#endif

#ifndef CONJ
	FADD	c2,  c3,  c2
	FCLR(0)
	FADD	c6,  c7,  c6
	FADD	c10, c11, c10
	FADD	c14, c15, c14
#else
	FSUB	c2,  c3,  c2
	FCLR(0)
	FSUB	c6,  c7,  c6
	FSUB	c10, c11, c10
	FSUB	c14, c15, c14
#endif

	FMUL	b3, c1, c3
	FMOV	t1, t2
	FMUL	b4, c1, c4
	FMOV	t1, t3
	FMUL	b4, c2, c1
	FMOV	t1, t4
	FMUL	b3, c2, c2

	FMUL	b3, c5, c7
	FMUL	b4, c5, c8
	FMUL	b4, c6, c5
	FMUL	b3, c6, c6

	FMUL	b3, c9,  c11
	FMUL	b4, c9,  c12
	FMUL	b4, c10, c9
	FMUL	b3, c10, c10

	FMUL	b3, c13, c15
	FSUB	c3,  c1,  c1
	FMUL	b4, c13, c16
	FADD	c2,  c4,  c2
	FMUL	b4, c14, c13
	FSUB	c7,  c5,  c5
	FMUL	b3, c14, c14
	FADD	c6,  c8,  c6

	FSUB	c11, c9,  c9
	FADD	c10, c12, c10
	FSUB	c15, c13, c13
	FADD	c14, c16, c14

	FADD	a1, c1, a1
	FADD	a2, c2, a2
	FADD	a3, c5, a3
	FADD	a4, c6, a4

	STF	a1, [Y2 + 0 * SIZE]
	FADD	a5, c9,  a5
	STF	a2, [Y2 + 1 * SIZE]
	FADD	a6, c10, a6
	add	Y2, INCY, Y2
	STF	a3, [Y2 + 0 * SIZE]
	FADD	a7, c13, a7
	STF	a4, [Y2 + 1 * SIZE]
	FADD	a8, c14, a8
	add	Y2, INCY, Y2

	STF	a5, [Y2 + 0 * SIZE]
	FMOV	t1, c1
	add	J, -1, J
	STF	a6, [Y2 + 1 * SIZE]
	FMOV	t1, c2
	cmp	J, 0
	add	Y2, INCY, Y2
	STF	a7, [Y2 + 0 * SIZE]
	FMOV	t1, c3
	STF	a8, [Y2 + 1 * SIZE]
	FMOV	t1, c4
	add	Y2, INCY, Y2

	FMOV	t1, c5
	bg	%icc, .LL110
	FMOV	t1, c6

.LL200:
	FCLR(0)

	and	N, 2, J
	cmp	J, 0
	FMOV	t1, c1
	ble	%icc, .LL300

	FMOV	t1, c2
	sra	MIN_M, 2, I
	FMOV	t1, t2
	add	A,  LDA, A2
	FMOV	t1, c3
	mov	A,  A1
	FMOV	t1, t3
	cmp	I, 0
	FMOV	t1, c4

	FMOV	t1, c5
	FMOV	t1, c6
	FMOV	t1, c7
	FMOV	t1, c8

	add	A2, LDA, A
	FMOV	t1, t4
	ble	%icc, .LL215
	mov	XP, X1

	LDF	[A1 + 0 * SIZE], a1
	LDF	[A1 + 1 * SIZE], a2
	LDF	[A1 + 2 * SIZE], a5
	LDF	[A1 + 3 * SIZE], a6
	add	A1, 4 * SIZE, A1

	LDF	[A2 + 0 * SIZE], a3
	LDF	[A2 + 1 * SIZE], a4
	LDF	[A2 + 2 * SIZE], a7
	LDF	[A2 + 3 * SIZE], a8
	add	A2, 4 * SIZE, A2

	LDF	[X1 + 0 * SIZE], b1
	add	I, -1, I
	LDF	[X1 + 1 * SIZE], b2
	cmp	I, 0
	LDF	[X1 + 2 * SIZE], b3
	LDF	[X1 + 3 * SIZE], b4
	ble	%icc, .LL212
	add	X1, 4 * SIZE, X1

.LL211:
	prefetch [A1 + PREFETCHSIZE * SIZE], 1

	FADD	c5, t1, c5
	FMUL	a1, b1, t1
	FADDX	c6, t2, c6
	FMUL	a1, b2, t2
	LDF	[A1 + 0 * SIZE], a1
	FADD	c7, t3, c7
	FMUL	a2, b1, t3
	FADD	c8, t4, c8
	FMUL	a2, b2, t4
	LDF	[A1 + 1 * SIZE], a2

	FADD	c1, t1, c1
	FMUL	a3, b1, t1
	FADDX	c2, t2, c2
	FMUL	a3, b2, t2
	LDF	[A2 + 0 * SIZE], a3
	FADD	c3, t3, c3
	FMUL	a4, b1, t3
	LDF	[X1 + 0 * SIZE], b1
	FADD	c4, t4, c4
	FMUL	a4, b2, t4
	LDF	[A2 + 1 * SIZE], a4

	FADD	c5, t1, c5
	LDF	[X1 + 1 * SIZE], b2
	FMUL	a5, b3, t1
	FADDX	c6, t2, c6
	FMUL	a5, b4, t2
	LDF	[A1 + 2 * SIZE], a5
	FADD	c7, t3, c7
	add	I, -1, I
	FMUL	a6, b3, t3
	FADD	c8, t4, c8
	cmp	I, 0
	FMUL	a6, b4, t4
	LDF	[A1 + 3 * SIZE], a6

	FADD	c1, t1, c1
	FMUL	a7, b3, t1
	FADDX	c2, t2, c2
	FMUL	a7, b4, t2
	LDF	[A2 + 2 * SIZE], a7
	FADD	c3, t3, c3
	FMUL	a8, b3, t3
	LDF	[X1 + 2 * SIZE], b3
	FADD	c4, t4, c4
	FMUL	a8, b4, t4
	LDF	[A2 + 3 * SIZE], a8

	prefetch [A2 + PREFETCHSIZE * SIZE], 1
	FADD	c5, t1, c5
	LDF	[X1 + 3 * SIZE], b4
	FMUL	a1, b1, t1
	FADDX	c6, t2, c6
	FMUL	a1, b2, t2
	LDF	[A1 + 4 * SIZE], a1
	FADD	c7, t3, c7
	FMUL	a2, b1, t3
	FADD	c8, t4, c8
	FMUL	a2, b2, t4
	LDF	[A1 + 5 * SIZE], a2

	FADD	c1, t1, c1
	FMUL	a3, b1, t1
	FADDX	c2, t2, c2
	FMUL	a3, b2, t2
	LDF	[A2 + 4 * SIZE], a3
	FADD	c3, t3, c3
	FMUL	a4, b1, t3
	LDF	[X1 + 4 * SIZE], b1
	FADD	c4, t4, c4
	FMUL	a4, b2, t4
	LDF	[A2 + 5 * SIZE], a4

	FADD	c5, t1, c5
	LDF	[X1 + 5 * SIZE], b2
	FMUL	a5, b3, t1
	FADDX	c6, t2, c6
	FMUL	a5, b4, t2
	LDF	[A1 + 6 * SIZE], a5
	FADD	c7, t3, c7
	FMUL	a6, b3, t3
	FADD	c8, t4, c8
	FMUL	a6, b4, t4
	LDF	[A1 + 7 * SIZE], a6
	add	A1, 8 * SIZE, A1

	FADD	c1, t1, c1
	FMUL	a7, b3, t1
	FADDX	c2, t2, c2
	FMUL	a7, b4, t2
	LDF	[A2 + 6 * SIZE], a7
	FADD	c3, t3, c3
	FMUL	a8, b3, t3
	LDF	[X1 + 6 * SIZE], b3
	FADD	c4, t4, c4
	add	X1, 8 * SIZE, X1
	FMUL	a8, b4, t4
	LDF	[A2 + 7 * SIZE], a8
	add	A2, 8 * SIZE, A2
	bg,pn	%icc, .LL211
	LDF	[X1 - 1 * SIZE], b4

.LL212:
	FADD	c5, t1, c5
	FMUL	a1, b1, t1
	FADDX	c6, t2, c6
	FMUL	a1, b2, t2
	LDF	[A1 + 0 * SIZE], a1
	FADD	c7, t3, c7
	FMUL	a2, b1, t3
	FADD	c8, t4, c8
	FMUL	a2, b2, t4
	LDF	[A1 + 1 * SIZE], a2

	FADD	c1, t1, c1
	FMUL	a3, b1, t1
	FADDX	c2, t2, c2
	FMUL	a3, b2, t2
	LDF	[A2 + 0 * SIZE], a3
	FADD	c3, t3, c3
	FMUL	a4, b1, t3
	LDF	[X1 + 0 * SIZE], b1
	FADD	c4, t4, c4
	FMUL	a4, b2, t4
	LDF	[A2 + 1 * SIZE], a4

	FADD	c5, t1, c5
	LDF	[X1 + 1 * SIZE], b2
	FMUL	a5, b3, t1
	FADDX	c6, t2, c6
	FMUL	a5, b4, t2
	LDF	[A1 + 2 * SIZE], a5
	FADD	c7, t3, c7
	FMUL	a6, b3, t3
	FADD	c8, t4, c8
	FMUL	a6, b4, t4
	LDF	[A1 + 3 * SIZE], a6
	add	A1, 4 * SIZE, A1

	FADD	c1, t1, c1
	FMUL	a7, b3, t1
	FADDX	c2, t2, c2
	FMUL	a7, b4, t2
	LDF	[A2 + 2 * SIZE], a7
	FADD	c3, t3, c3
	FMUL	a8, b3, t3
	LDF	[X1 + 2 * SIZE], b3
	FADD	c4, t4, c4
	FMUL	a8, b4, t4
	LDF	[A2 + 3 * SIZE], a8
	add	A2, 4 * SIZE, A2

	FADD	c5, t1, c5
	LDF	[X1 + 3 * SIZE], b4
	add	X1, 4 * SIZE, X1
	FMUL	a1, b1, t1
	FADDX	c6, t2, c6
	FMUL	a1, b2, t2
	FADD	c7, t3, c7
	FMUL	a2, b1, t3
	FADD	c8, t4, c8
	FMUL	a2, b2, t4

	FADD	c1, t1, c1
	FMUL	a3, b1, t1
	FADDX	c2, t2, c2
	FMUL	a3, b2, t2
	FADD	c3, t3, c3
	FMUL	a4, b1, t3
	FADD	c4, t4, c4
	FMUL	a4, b2, t4

	FADD	c5, t1, c5
	FMUL	a5, b3, t1
	FADDX	c6, t2, c6
	FMUL	a5, b4, t2
	FADD	c7, t3, c7
	FMUL	a6, b3, t3
	FADD	c8, t4, c8
	FMUL	a6, b4, t4

	FADD	c1, t1, c1
	FMUL	a7, b3, t1
	FADDX	c2, t2, c2
	FMUL	a7, b4, t2
	FADD	c3, t3, c3
	FMUL	a8, b3, t3
	FADD	c4, t4, c4
	FMUL	a8, b4, t4

.LL215:
	andcc	MIN_M, 3, I
	LDF	ALPHA_R, b3
	mov	Y1, Y2
	ble	%icc, .LL219
	LDF	ALPHA_I, b4

	LDF	[A1 + 0 * SIZE], a1
	add	I, -1, I
	LDF	[A1 + 1 * SIZE], a2
	cmp	I, 0
	add	A1, 2 * SIZE, A1

	LDF	[A2 + 0 * SIZE], a3
	LDF	[A2 + 1 * SIZE], a4
	add	A2, 2 * SIZE, A2

	LDF	[X1 + 0 * SIZE], b1
	LDF	[X1 + 1 * SIZE], b2
	ble	%icc, .LL217
	add	X1, 2 * SIZE, X1

.LL216:
	FADD	c5, t1, c5
	FMUL	a1, b1, t1
	FADDX	c6, t2, c6
	FMUL	a1, b2, t2
	LDF	[A1 + 0 * SIZE], a1
	FADD	c7, t3, c7
	add	I, -1, I
	FMUL	a2, b1, t3
	FADD	c8, t4, c8
	cmp	I, 0
	FMUL	a2, b2, t4
	LDF	[A1 + 1 * SIZE], a2
	add	A1, 2 * SIZE, A1

	FADD	c1, t1, c1
	FMUL	a3, b1, t1
	FADDX	c2, t2, c2
	FMUL	a3, b2, t2
	LDF	[A2 + 0 * SIZE], a3
	FADD	c3, t3, c3
	FMUL	a4, b1, t3
	LDF	[X1 + 0 * SIZE], b1
	FADD	c4, t4, c4
	add	X1, 2 * SIZE, X1
	FMUL	a4, b2, t4
	LDF	[A2 + 1 * SIZE], a4
	add	A2, 2 * SIZE, A2
	bg,pn	%icc, .LL216
	LDF	[X1 - 1 * SIZE], b2

.LL217:
	FADD	c5, t1, c5
	FMUL	a1, b1, t1
	FADDX	c6, t2, c6
	FMUL	a1, b2, t2
	FADD	c7, t3, c7
	FMUL	a2, b1, t3
	FADD	c8, t4, c8
	FMUL	a2, b2, t4

	FADD	c1, t1, c1
	FMUL	a3, b1, t1
	FADDX	c2, t2, c2
	FMUL	a3, b2, t2
	FADD	c3, t3, c3
	FMUL	a4, b1, t3
	FADD	c4, t4, c4
	FMUL	a4, b2, t4

.LL219:
	FADD	c5, t1, c5
	LDF	[Y1 + 0 * SIZE], a1
	FADDX	c6, t2, c6
	LDF	[Y1 + 1 * SIZE] ,a2
	add	Y1, INCY, Y1
	FADD	c7, t3, c7
	LDF	[Y1 + 0 * SIZE], a3
	FADD	c8, t4, c8
	LDF	[Y1 + 1 * SIZE] ,a4
	add	Y1, INCY, Y1

#if (!defined(XCONJ) && !defined(CONJ)) || (defined(XCONJ) && defined(CONJ))
	FSUB	c1, c4, c1
	FSUB	c5, c8, c5
#else
	FADD	c1, c4, c1
	FADD	c5, c8, c5
#endif

#ifndef CONJ
	FADD	c2, c3, c2
	FADD	c6, c7, c6
#else
	FSUB	c2, c3, c2
	FSUB	c6, c7, c6
#endif

	FMUL	b3, c1, c3
	FMUL	b4, c1, c4
	FMUL	b4, c2, c1
	FMUL	b3, c2, c2

	FMUL	b3, c5, c7
	FMUL	b4, c5, c8
	FMUL	b4, c6, c5
	FMUL	b3, c6, c6

	FSUB	c3, c1, c1
	FADD	c2, c4, c2
	FSUB	c7, c5, c5
	FADD	c6, c8, c6

	FADD	a1, c1, a1
	FADD	a2, c2, a2
	FADD	a3, c5, a3
	FADD	a4, c6, a4

	STF	a1, [Y2 + 0 * SIZE]
	STF	a2, [Y2 + 1 * SIZE]
	add	Y2, INCY, Y2
	STF	a3, [Y2 + 0 * SIZE]
	STF	a4, [Y2 + 1 * SIZE]

.LL300:
	andcc	N, 1, J
	FCLR(0)
	ble	%icc, .LL400
	FMOV	t1, c1

.LL310:
	sra	MIN_M, 2, I
	FMOV	t1, c2
	FMOV	t1, c3
	FMOV	t1, c4
	mov	A, A1
	FMOV	t1, t2
	add	A, LDA, A
	FMOV	t1, t3
	cmp	I, 0
	FMOV	t1, t4
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

	LDF	[X1 + 0 * SIZE], c9
	add	I, -1, I
	LDF	[X1 + 1 * SIZE], c10
	cmp	I, 0
	LDF	[X1 + 2 * SIZE], c11
	LDF	[X1 + 3 * SIZE], c12
	LDF	[X1 + 4 * SIZE], c13
	LDF	[X1 + 5 * SIZE], c14
	LDF	[X1 + 6 * SIZE], c15
	LDF	[X1 + 7 * SIZE], c16
	ble	%icc, .LL312
	add	X1, 8 * SIZE, X1

.LL311:
	prefetch [A1 + PREFETCHSIZE * SIZE], 1

	FADD	c1, t1, c1
	FMUL	a1, c9,  t1
	FADDX	c2, t2, c2
	FMUL	a1, c10, t2
	LDF	[A1 + 0 * SIZE], a1
	FADD	c3, t3, c3
	FMUL	a2, c9,  t3
	LDF	[X1 + 0 * SIZE], c9
	FADD	c4, t4, c4
	FMUL	a2, c10, t4
	LDF	[A1 + 1 * SIZE], a2
	LDF	[X1 + 1 * SIZE], c10

	FADD	c1, t1, c1
	FMUL	a3, c11, t1
	FADDX	c2, t2, c2
	FMUL	a3, c12, t2
	LDF	[A1 + 2 * SIZE], a3
	FADD	c3, t3, c3
	add	I, -1, I
	FMUL	a4, c11, t3
	LDF	[X1 + 2 * SIZE], c11
	FADD	c4, t4, c4
	cmp	I, 0
	FMUL	a4, c12, t4
	LDF	[A1 + 3 * SIZE], a4
	LDF	[X1 + 3 * SIZE], c12

	FADD	c1, t1, c1
	FMUL	a5, c13, t1
	FADDX	c2, t2, c2
	FMUL	a5, c14, t2
	LDF	[A1 + 4 * SIZE], a5
	FADD	c3, t3, c3
	FMUL	a6, c13, t3
	LDF	[X1 + 4 * SIZE], c13
	FADD	c4, t4, c4
	FMUL	a6, c14, t4
	LDF	[A1 + 5 * SIZE], a6
	LDF	[X1 + 5 * SIZE], c14

	FADD	c1, t1, c1
	FMUL	a7, c15, t1
	FADDX	c2, t2, c2
	FMUL	a7, c16, t2
	LDF	[A1 + 6 * SIZE], a7

	FADD	c3, t3, c3
	FMUL	a8, c15, t3
	LDF	[X1 + 6 * SIZE], c15
	FADD	c4, t4, c4
	add	X1, 8 * SIZE, X1
	FMUL	a8, c16, t4
	LDF	[A1 + 7 * SIZE], a8
	add	A1, 8 * SIZE, A1
	bg,pn	%icc, .LL311
	LDF	[X1 - 1 * SIZE], c16

.LL312:
	FADD	c1, t1, c1
	FMUL	a1, c9,  t1
	FADDX	c2, t2, c2
	FMUL	a1, c10, t2
	FADD	c3, t3, c3
	FMUL	a2, c9,  t3
	FADD	c4, t4, c4
	FMUL	a2, c10, t4

	FADD	c1, t1, c1
	FMUL	a3, c11, t1
	FADDX	c2, t2, c2
	FMUL	a3, c12, t2
	FADD	c3, t3, c3
	FMUL	a4, c11, t3
	FADD	c4, t4, c4
	FMUL	a4, c12, t4

	FADD	c1, t1, c1
	FMUL	a5, c13, t1
	FADDX	c2, t2, c2
	FMUL	a5, c14, t2
	FADD	c3, t3, c3
	FMUL	a6, c13, t3
	FADD	c4, t4, c4
	FMUL	a6, c14, t4

	FADD	c1, t1, c1
	FMUL	a7, c15, t1
	FADDX	c2, t2, c2
	FMUL	a7, c16, t2
	FADD	c3, t3, c3
	FMUL	a8, c15, t3
	FADD	c4, t4, c4
	FMUL	a8, c16, t4

.LL315:
	andcc	MIN_M, 3, I
	LDF	ALPHA_R, b3
	mov	Y1, Y2
	ble	%icc, .LL319
	LDF	ALPHA_I, b4

	LDF	[A1 + 0 * SIZE], a1
	add	I, -1, I
	LDF	[A1 + 1 * SIZE], a2
	add	A1, 2 * SIZE, A1
	LDF	[X1 + 0 * SIZE], b1
	cmp	I, 0
	LDF	[X1 + 1 * SIZE], b2
	ble	%icc, .LL317
	add	X1, 2 * SIZE, X1

.LL316:
	FADD	c1, t1, c1
	add	I, -1, I
	FMUL	a1, b1, t1
	FADDX	c2, t2, c2
	FMUL	a1, b2, t2
	LDF	[A1 + 0 * SIZE], a1
	FADD	c3, t3, c3
	cmp	I, 0
	FMUL	a2, b1, t3
	LDF	[X1 + 0 * SIZE], b1
	FADD	c4, t4, c4
	add	X1, 2 * SIZE, X1
	FMUL	a2, b2, t4
	LDF	[A1 + 1 * SIZE], a2
	add	A1, 2 * SIZE, A1

	bg,pn	%icc, .LL316
	LDF	[X1 - 1 * SIZE], b2

.LL317:
	FADD	c1, t1, c1
	FMUL	a1, b1, t1
	FADDX	c2, t2, c2
	FMUL	a1, b2, t2
	FADD	c3, t3, c3
	FMUL	a2, b1, t3
	FADD	c4, t4, c4
	FMUL	a2, b2, t4

.LL319:
	FADD	c1, t1, c1
	LDF	[Y1 + 0 * SIZE], a1
	FADDX	c2, t2, c2
	LDF	[Y1 + 1 * SIZE] ,a2
	add	Y1, INCY, Y1
	FADD	c3, t3, c3
	FADD	c4, t4, c4

#if (!defined(XCONJ) && !defined(CONJ)) || (defined(XCONJ) && defined(CONJ))
	FSUB	c1, c4, c1
#else
	FADD	c1, c4, c1
#endif

#ifndef CONJ
	FADD	c2, c3, c2
#else
	FSUB	c2, c3, c2
#endif

	FMUL	b3, c1, c3
	FMUL	b4, c1, c4
	FMUL	b4, c2, c1
	FMUL	b3, c2, c2

	FSUB	c3, c1, c1
	FADD	c2, c4, c2
	FADD	a1, c1, a1
	FADD	a2, c2, a2

	STF	a1, [Y2 + 0 * SIZE]
	STF	a2, [Y2 + 1 * SIZE]

.LL400:
	mov	P, I
	add	IS, I, IS
	cmp	IS, M
	bl	%icc, .LL10
	add	A, PNLDA, A

.LL999:
	return	%i7 + 8
	clr	%o0

	EPILOGUE
