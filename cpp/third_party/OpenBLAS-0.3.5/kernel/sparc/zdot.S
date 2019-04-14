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

#if defined(F_INTERFACE) && defined(F_INTERFACE_F2C)
#define OUT	%i0
#define N	%i1
#define X	%i2
#define INCX	%i3
#define Y	%i4
#define INCY	%i5
#else
#define N	%i0
#define X	%i1
#define INCX	%i2
#define Y	%i3
#define INCY	%i4
#endif

#define I	%l0

#ifdef DOUBLE
#define c1	%f0
#define c2	%f2
#define c3	%f4
#define c4	%f6
#define t1	%f8
#define t2	%f10
#define t3	%f12
#define t4	%f14
#define a1	%f16
#define a2	%f18
#define a3	%f20
#define a4	%f22
#define a5	%f24
#define a6	%f26
#define a7	%f28
#define a8	%f30

#define b1	%f32
#define b2	%f34
#define b3	%f36
#define b4	%f38
#define b5	%f40
#define b6	%f42
#define b7	%f44
#define b8	%f46
#else
#define c1	%f0
#define c2	%f1
#define c3	%f2
#define c4	%f3
#define t1	%f4
#define t2	%f5
#define t3	%f6
#define t4	%f7

#define a1	%f8
#define a2	%f9
#define a3	%f10
#define a4	%f11
#define a5	%f12
#define a6	%f13
#define a7	%f14
#define a8	%f15

#define b1	%f16
#define b2	%f17
#define b3	%f18
#define b4	%f19
#define b5	%f20
#define b6	%f21
#define b7	%f22
#define b8	%f23
#endif

	PROLOGUE
	SAVESP

#ifdef DOUBLE
	FCLR(0)
	FCLR(2)
	FCLR(4)
	FCLR(6)
#else
	FCLR(0)
	FCLR(1)
	FCLR(2)
	FCLR(3)
#endif

	FMOV	c1, c4
	FMOV	c1, t1
	sll	INCX, ZBASE_SHIFT, INCX
	FMOV	c1, t2
	sll	INCY, ZBASE_SHIFT, INCY
	FMOV	c1, t3
	FMOV	c1, t4

	cmp	INCX, 2 * SIZE
	bne	.LL50
	nop
	cmp	INCY, 2 * SIZE
	bne	.LL50
	nop

	sra	N, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL15
	nop

	LDF	[X +  0 * SIZE], a1
	add	I, -1, I
	LDF	[Y +  0 * SIZE], b1
	cmp	I, 0
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
	add	X, 8 * SIZE, X
	LDF	[Y +  7 * SIZE], b8
	ble,pt	%icc, .LL12
	add	Y, 8 * SIZE, Y

#define PREFETCHSIZE 40

.LL11:
	prefetch [X  + PREFETCHSIZE * SIZE], 0
	FADD	c1, t1, c1
	prefetch [Y  + PREFETCHSIZE * SIZE], 0
	FMUL	a1, b1,	t1

	FADD	c2, t2, c2
	FMUL	a2, b1,	t2
	LDF	[Y +  0 * SIZE], b1

	FADD	c3, t3, c3
	FMUL	a1, b2,	t3
	LDF	[X +  0 * SIZE], a1

	FADD	c4, t4, c4
	FMUL	a2, b2,	t4
	LDF	[X +  1 * SIZE], a2

	FADD	c1, t1, c1
	FMUL	a3, b3,	t1
	LDF	[Y +  1 * SIZE], b2
	FADD	c2, t2, c2
	FMUL	a4, b3,	t2
	LDF	[Y +  2 * SIZE], b3

	FADD	c3, t3, c3
	FMUL	a3, b4,	t3
	LDF	[X +  2 * SIZE], a3
	FADD	c4, t4, c4
	FMUL	a4, b4,	t4
	LDF	[X +  3 * SIZE], a4

	FADD	c1, t1, c1
	FMUL	a5, b5,	t1
	LDF	[Y +  3 * SIZE], b4
	FADD	c2, t2, c2
	FMUL	a6, b5,	t2
	LDF	[Y +  4 * SIZE], b5

	FADD	c3, t3, c3
	FMUL	a5, b6,	t3
	LDF	[X +  4 * SIZE], a5
	FADD	c4, t4, c4
	FMUL	a6, b6,	t4
	LDF	[X +  5 * SIZE], a6

	FADD	c1, t1, c1
	add	I, -1, I
	FMUL	a7, b7,	t1
	LDF	[Y +  5 * SIZE], b6
	FADD	c2, t2, c2
	cmp	I, 0
	FMUL	a8, b7,	t2
	LDF	[Y +  6 * SIZE], b7

	FADD	c3, t3, c3
	add	Y, 8 * SIZE, Y
	FMUL	a7, b8,	t3
	LDF	[X +  6 * SIZE], a7
	FADD	c4, t4, c4
	FMUL	a8, b8,	t4
	LDF	[X +  7 * SIZE], a8

	add	X, 8 * SIZE, X
	bg,pt	%icc, .LL11
	LDF	[Y -  1 * SIZE], b8

.LL12:
	FADD	c1, t1, c1
	FMUL	a1, b1,	t1
	FADD	c2, t2, c2
	FMUL	a2, b1,	t2

	FADD	c3, t3, c3
	FMUL	a1, b2,	t3
	FADD	c4, t4, c4
	FMUL	a2, b2,	t4

	FADD	c1, t1, c1
	FMUL	a3, b3,	t1
	FADD	c2, t2, c2
	FMUL	a4, b3,	t2

	FADD	c3, t3, c3
	FMUL	a3, b4,	t3
	FADD	c4, t4, c4
	FMUL	a4, b4,	t4

	FADD	c1, t1, c1
	FMUL	a5, b5,	t1
	FADD	c2, t2, c2
	FMUL	a6, b5,	t2

	FADD	c3, t3, c3
	FMUL	a5, b6,	t3
	FADD	c4, t4, c4
	FMUL	a6, b6,	t4

	FADD	c1, t1, c1
	FMUL	a7, b7,	t1
	FADD	c2, t2, c2
	FMUL	a8, b7,	t2

	FADD	c3, t3, c3
	FMUL	a7, b8,	t3
	FADD	c4, t4, c4
	FMUL	a8, b8,	t4

.LL15:
	and	N, 3, I
	cmp	I,  0
	ble,a,pn %icc, .LL19
	nop

.LL16:
	LDF	[X +  0 * SIZE], a1
	LDF	[X +  1 * SIZE], a2
	add	X, 2 * SIZE, X
	LDF	[Y +  0 * SIZE], b1
	LDF	[Y +  1 * SIZE], b2
	add	Y, 2 * SIZE, Y

	FADD	c1, t1, c1
	FMUL	a1, b1,	t1
	FADD	c2, t2, c2
	FMUL	a2, b1,	t2
	FADD	c3, t3, c3
	FMUL	a1, b2,	t3
	FADD	c4, t4, c4
	FMUL	a2, b2,	t4

	add	I, -1, I
	cmp	I, 0
	bg,pt	%icc, .LL16
	nop

.LL19:
	FADD	c1, t1, c1
	FADD	c2, t2, c2
	FADD	c3, t3, c3
	FADD	c4, t4, c4

#ifndef CONJ
	FSUB	c1, c4, c1
	FADD	c2, c3, c2
#else
	FADD	c1, c4, c1
	FSUB	c3, c2, c2
#endif

#if defined(F_INTERFACE) && defined(F_INTERFACE_F2C)
	STF	c1, [OUT + 0 * SIZE]
	STF	c2, [OUT + 1 * SIZE]
#endif
	return	%i7 + 8
	clr	%g0
.LL50:
#ifdef F_INTERFACE
       cmp	INCX, 0
       bge	.LL41
       sub	N, 1, I

       smul	I, INCX, I
       sub	X, I, X

.LL41:
       cmp	INCY, 0
       bge	.LL42
       sub	N, 1, I

       smul	I, INCY, I
       sub	Y, I, Y

.LL42:
#endif
	sra	N, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL55
	nop

	LDF	[X +  0 * SIZE], a1
	LDF	[X +  1 * SIZE], a2
	add	X, INCX, X
	LDF	[X +  0 * SIZE], a3
	LDF	[X +  1 * SIZE], a4
	add	X, INCX, X
	LDF	[X +  0 * SIZE], a5
	LDF	[X +  1 * SIZE], a6
	add	X, INCX, X
	LDF	[X +  0 * SIZE], a7
	LDF	[X +  1 * SIZE], a8
	add	X, INCX, X

	LDF	[Y +  0 * SIZE], b1
	LDF	[Y +  1 * SIZE], b2
	add	Y, INCY, Y
	LDF	[Y +  0 * SIZE], b3
	LDF	[Y +  1 * SIZE], b4
	add	Y, INCY, Y
	LDF	[Y +  0 * SIZE], b5
	LDF	[Y +  1 * SIZE], b6
	add	Y, INCY, Y
	LDF	[Y +  0 * SIZE], b7
	LDF	[Y +  1 * SIZE], b8
	add	Y, INCY, Y

	add	I, -1, I
	cmp	I, 0
	ble,pt	%icc, .LL52

.LL51:
	FADD	c1, t1, c1
	prefetch [X  + PREFETCHSIZE * SIZE], 0
	add	I, -1, I
	FMUL	a1, b1,	t1
	prefetch [Y  + PREFETCHSIZE * SIZE], 0

	FADD	c2, t2, c2
	cmp	I, 0
	FMUL	a2, b1,	t2
	LDF	[Y +  0 * SIZE], b1

	FADD	c3, t3, c3
	FMUL	a1, b2,	t3
	LDF	[X +  0 * SIZE], a1
	FADD	c4, t4, c4
	FMUL	a2, b2,	t4
	LDF	[X +  1 * SIZE], a2
	add	X, INCX, X

	FADD	c1, t1, c1
	FMUL	a3, b3,	t1
	LDF	[Y +  1 * SIZE], b2
	add	Y, INCY, Y
	FADD	c2, t2, c2
	FMUL	a4, b3,	t2
	LDF	[Y +  0 * SIZE], b3

	FADD	c3, t3, c3
	FMUL	a3, b4,	t3
	LDF	[X +  0 * SIZE], a3
	FADD	c4, t4, c4
	FMUL	a4, b4,	t4
	LDF	[X +  1 * SIZE], a4
	add	X, INCX, X

	FADD	c1, t1, c1
	FMUL	a5, b5,	t1
	LDF	[Y +  1 * SIZE], b4
	add	Y, INCY, Y
	FADD	c2, t2, c2
	FMUL	a6, b5,	t2
	LDF	[Y +  0 * SIZE], b5

	FADD	c3, t3, c3
	FMUL	a5, b6,	t3
	LDF	[X +  0 * SIZE], a5
	FADD	c4, t4, c4
	FMUL	a6, b6,	t4
	LDF	[X +  1 * SIZE], a6
	add	X, INCX, X

	FADD	c1, t1, c1
	FMUL	a7, b7,	t1
	LDF	[Y +  1 * SIZE], b6
	add	Y, INCY, Y
	FADD	c2, t2, c2
	FMUL	a8, b7,	t2
	LDF	[Y +  0 * SIZE], b7

	FADD	c3, t3, c3
	FMUL	a7, b8,	t3
	LDF	[X +  0 * SIZE], a7
	FADD	c4, t4, c4
	FMUL	a8, b8,	t4
	LDF	[X +  1 * SIZE], a8
	add	X, INCX, X

	LDF	[Y +  1 * SIZE], b8
	bg,pt	%icc, .LL51
	add	Y, INCY, Y

.LL52:
	FADD	c1, t1, c1
	FMUL	a1, b1,	t1
	FADD	c2, t2, c2
	FMUL	a2, b1,	t2

	FADD	c3, t3, c3
	FMUL	a1, b2,	t3
	FADD	c4, t4, c4
	FMUL	a2, b2,	t4

	FADD	c1, t1, c1
	FMUL	a3, b3,	t1
	FADD	c2, t2, c2
	FMUL	a4, b3,	t2

	FADD	c3, t3, c3
	FMUL	a3, b4,	t3
	FADD	c4, t4, c4
	FMUL	a4, b4,	t4

	FADD	c1, t1, c1
	FMUL	a5, b5,	t1
	FADD	c2, t2, c2
	FMUL	a6, b5,	t2

	FADD	c3, t3, c3
	FMUL	a5, b6,	t3
	FADD	c4, t4, c4
	FMUL	a6, b6,	t4

	FADD	c1, t1, c1
	FMUL	a7, b7,	t1
	FADD	c2, t2, c2
	FMUL	a8, b7,	t2

	FADD	c3, t3, c3
	FMUL	a7, b8,	t3
	FADD	c4, t4, c4
	FMUL	a8, b8,	t4

.LL55:
	and	N, 3, I
	cmp	I,  0
	ble,a,pn %icc, .LL59
	nop

.LL56:
	LDF	[X +  0 * SIZE], a1
	LDF	[X +  1 * SIZE], a2
	add	X, INCX, X
	LDF	[Y +  0 * SIZE], b1
	LDF	[Y +  1 * SIZE], b2
	add	Y, INCY, Y

	FADD	c1, t1, c1
	FMUL	a1, b1,	t1
	FADD	c2, t2, c2
	FMUL	a2, b1,	t2
	FADD	c3, t3, c3
	FMUL	a1, b2,	t3
	FADD	c4, t4, c4
	FMUL	a2, b2,	t4

	add	I, -1, I
	cmp	I, 0
	bg,pt	%icc, .LL56
	nop

.LL59:
	FADD	c1, t1, c1
	FADD	c2, t2, c2
	FADD	c3, t3, c3
	FADD	c4, t4, c4

#ifndef CONJ
	FSUB	c1, c4, c1
	FADD	c2, c3, c2
#else
	FADD	c1, c4, c1
	FSUB	c3, c2, c2
#endif

#if defined(F_INTERFACE) && defined(F_INTERFACE_F2C)
	STF	c1, [OUT + 0 * SIZE]
	STF	c2, [OUT + 1 * SIZE]
#endif
	return	%i7 + 8
	clr	%o0

	EPILOGUE
