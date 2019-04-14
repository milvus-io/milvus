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
#if defined(DOUBLE) && !defined(__64BIT__)
#define X	%i5
#define INCX	%i1
#else
#define X	%i4
#define INCX	%i5
#endif

#define I	%i2
#define XX	%i3

#ifdef DOUBLE
#define c1	%f0
#define c2	%f2
#define c3	%f4
#define c4	%f6
#define c5	%f8
#define c6	%f10
#define c7	%f12
#define c8	%f14

#define t1	%f16
#define t2	%f18
#define t3	%f20
#define t4	%f22
#define t5	%f24
#define t6	%f26
#define t7	%f28
#define t8	%f30

#define FZERO	%f60
#define ALPHA	%f62
#else
#define c1	%f0
#define c2	%f1
#define c3	%f2
#define c4	%f3
#define c5	%f4
#define c6	%f5
#define c7	%f6
#define c8	%f7

#define t1	%f8
#define t2	%f9
#define t3	%f10
#define t4	%f11
#define t5	%f12
#define t6	%f13
#define t7	%f14
#define t8	%f15

#define FZERO	%f29
#define ALPHA	%f30
#endif

#define PREFETCHSIZE 168

	PROLOGUE
	SAVESP

#ifndef __64BIT__
#ifdef DOUBLE
	st	%i3, [%sp + STACK_START + 16]
	st	%i4, [%sp + STACK_START + 20]
	ld	[%sp + STACK_START + 28], INCX
#else
	st	%i3, [%sp + STACK_START + 16]
#endif

	LDF	[%sp + STACK_START + 16], ALPHA
#else
#ifdef DOUBLE
	FMOV	%f6, ALPHA
#else
	FMOV	%f7, ALPHA
#endif
#endif

	FCLR(29)

	FCMP	ALPHA, FZERO
	fbne	.LL100
	sll	INCX, BASE_SHIFT, INCX

	cmp	INCX, SIZE
	bne	.LL50
	nop

	sra	N, 3, I
	cmp	I, 0
	ble,pn	%icc, .LL15
	nop

.LL11:
	prefetch [X  + PREFETCHSIZE * SIZE], 0

	STF	FZERO, [X +  0 * SIZE]
	add	I, -1, I
	STF	FZERO, [X +  1 * SIZE]
	cmp	I, 0
	STF	FZERO, [X +  2 * SIZE]
	STF	FZERO, [X +  3 * SIZE]
	STF	FZERO, [X +  4 * SIZE]
	STF	FZERO, [X +  5 * SIZE]
	add	X, 8 * SIZE, X
	STF	FZERO, [X -  2 * SIZE]
	bg,pt	%icc, .LL11
	STF	FZERO, [X -  1 * SIZE]

.LL15:
	and	N, 7, I
	cmp	I,  0
	ble,a,pn %icc, .LL19
	nop

.LL16:
	STF	FZERO, [X +  0 * SIZE]
	add	I, -1, I
	cmp	I, 0
	bg,pt	%icc, .LL16
	add	X, 1 * SIZE, X

.LL19:
	return	%i7 + 8
	clr	%o0

.LL50:
	sra	N, 3, I
	cmp	I, 0
	ble,pn	%icc, .LL55
	nop

.LL51:
	STF	FZERO, [X +  0 * SIZE]
	add	X, INCX, X
	add	I, -1, I
	STF	FZERO, [X +  0 * SIZE]
	add	X, INCX, X
	cmp	I, 0
	STF	FZERO, [X +  0 * SIZE]
	add	X, INCX, X
	STF	FZERO, [X +  0 * SIZE]
	add	X, INCX, X
	STF	FZERO, [X +  0 * SIZE]
	add	X, INCX, X
	STF	FZERO, [X +  0 * SIZE]
	add	X, INCX, X
	STF	FZERO, [X +  0 * SIZE]
	add	X, INCX, X
	STF	FZERO, [X +  0 * SIZE]
	bg,pt	%icc, .LL51
	add	X, INCX, X

.LL55:
	and	N, 7, I
	cmp	I,  0
	ble,a,pn %icc, .LL59
	nop

.LL56:
	STF	FZERO, [X +  0 * SIZE]
	add	I, -1, I
	cmp	I, 0
	bg,pt	%icc, .LL56
	add	X, INCX, X

.LL59:
	return	%i7 + 8
	clr	%o0

.LL100:
	cmp	INCX, SIZE
	bne	.LL150
	sra	N, 3, I

	cmp	I, 0
	ble,pn	%icc, .LL115
	nop

	LDF	[X +  0 * SIZE], c1
	LDF	[X +  1 * SIZE], c2
	LDF	[X +  2 * SIZE], c3
	LDF	[X +  3 * SIZE], c4
	LDF	[X +  4 * SIZE], c5
	LDF	[X +  5 * SIZE], c6
	LDF	[X +  6 * SIZE], c7
	LDF	[X +  7 * SIZE], c8
	FMUL	ALPHA, c1, t1
	LDF	[X +  8 * SIZE], c1
	FMUL	ALPHA, c2, t2
	LDF	[X +  9 * SIZE], c2

	deccc	I
	ble,pt	%icc, .LL112
	nop

.LL111:
	prefetch [X  + PREFETCHSIZE * SIZE], 0
	deccc	I

	FMUL	ALPHA, c3, t3
	LDF	[X + 10 * SIZE], c3
	nop
	STF	t1, [X +  0 * SIZE]

	FMUL	ALPHA, c4, t4
	LDF	[X + 11 * SIZE], c4
	nop
	STF	t2, [X +  1 * SIZE]

	FMUL	ALPHA, c5, t5
	LDF	[X + 12 * SIZE], c5
	nop
	STF	t3, [X +  2 * SIZE]

	FMUL	ALPHA, c6, t6
	LDF	[X + 13 * SIZE], c6
	nop
	STF	t4, [X +  3 * SIZE]

	FMUL	ALPHA, c7, t7
	LDF	[X + 14 * SIZE], c7
	nop
	STF	t5, [X +  4 * SIZE]

	FMUL	ALPHA, c8, t8
	LDF	[X + 15 * SIZE], c8
	nop
	STF	t6, [X +  5 * SIZE]

	FMUL	ALPHA, c1, t1
	STF	t7, [X +  6 * SIZE]
	nop
	LDF	[X + 16 * SIZE], c1

	FMUL	ALPHA, c2, t2
	STF	t8, [X +  7 * SIZE]
	nop
	LDF	[X + 17 * SIZE], c2

	bg,pt	%icc, .LL111
	add	X, 8 * SIZE, X

.LL112:
	FMUL	ALPHA, c3, t3
	STF	t1, [X +  0 * SIZE]
	FMUL	ALPHA, c4, t4
	STF	t2, [X +  1 * SIZE]

	FMUL	ALPHA, c5, t5
	STF	t3, [X +  2 * SIZE]
	FMUL	ALPHA, c6, t6
	STF	t4, [X +  3 * SIZE]

	FMUL	ALPHA, c7, t7
	STF	t5, [X +  4 * SIZE]
	FMUL	ALPHA, c8, t8
	STF	t6, [X +  5 * SIZE]
	STF	t7, [X +  6 * SIZE]
	STF	t8, [X +  7 * SIZE]

	add	X, 8 * SIZE, X

.LL115:
	and	N, 7, I
	cmp	I,  0
	ble,a,pn %icc, .LL119
	nop

.LL116:
	LDF	[X +  0 * SIZE], c1
	add	I, -1, I
	FMUL	ALPHA, c1, c1
	cmp	I, 0
	STF	c1, [X +  0 * SIZE]
	bg,pt	%icc, .LL116
	add	X, 1 * SIZE, X

.LL119:
	return	%i7 + 8
	clr	%o0

.LL150:
	sra	N, 3, I
	cmp	I, 0
	ble,pn	%icc, .LL155
	mov	X, XX

.LL151:
	LDF	[X +  0 * SIZE], c1
	add	X, INCX, X
	LDF	[X +  0 * SIZE], c2
	add	X, INCX, X
	LDF	[X +  0 * SIZE], c3
	add	X, INCX, X
	LDF	[X +  0 * SIZE], c4
	add	X, INCX, X
	LDF	[X +  0 * SIZE], c5
	FMUL	ALPHA, c1, c1
	add	X, INCX, X
	LDF	[X +  0 * SIZE], c6
	FMUL	ALPHA, c2, c2
	add	X, INCX, X
	LDF	[X +  0 * SIZE], c7
	FMUL	ALPHA, c3, c3
	add	X, INCX, X
	LDF	[X +  0 * SIZE], c8
	FMUL	ALPHA, c4, c4
	add	X, INCX, X

	STF	c1, [XX +  0 * SIZE]
	FMUL	ALPHA, c5, c5
	add	XX, INCX, XX
	STF	c2, [XX +  0 * SIZE]
	FMUL	ALPHA, c6, c6
	add	XX, INCX, XX
	STF	c3, [XX +  0 * SIZE]
	FMUL	ALPHA, c7, c7
	add	XX, INCX, XX
	STF	c4, [XX +  0 * SIZE]
	FMUL	ALPHA, c8, c8
	add	XX, INCX, XX
	STF	c5, [XX +  0 * SIZE]
	add	XX, INCX, XX
	add	I, -1, I
	STF	c6, [XX +  0 * SIZE]
	add	XX, INCX, XX
	cmp	I, 0
	STF	c7, [XX +  0 * SIZE]
	add	XX, INCX, XX
	STF	c8, [XX +  0 * SIZE]

	bg,pt	%icc, .LL151
	add	XX, INCX, XX

.LL155:
	and	N, 7, I
	cmp	I,  0
	ble,a,pn %icc, .LL159
	nop

.LL156:
	LDF	[X +  0 * SIZE], c1
	add	I, -1, I
	FMUL	ALPHA, c1, c1
	cmp	I, 0
	STF	c1, [X +  0 * SIZE]
	bg,pt	%icc, .LL156
	add	X, INCX, X

.LL159:
	return	%i7 + 8
	clr	%o0


	EPILOGUE
