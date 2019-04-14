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
#define I	%i3
#define XX	%i4

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
#define fmax	%f32
#define fzero	%f34
#define fone	%f36
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
#define fmax	%f16
#define fzero	%f17
#define fone	%f18
#endif

	PROLOGUE
	SAVESP

#ifdef DOUBLE
	FCLR(3)
#else
	FCLR(17)
#endif

	mov	X, XX
	mov	0x3ff, %g1
	sll	%g1, 20, %g1

	cmp	N, 0
	ble	.LL99
	FMOV	fzero, c1

	cmp	INCX, 0
	ble	.LL99
	sll	INCX, ZBASE_SHIFT, INCX

	add	%sp, -8, %sp
	st	%g1, [%sp + STACK_START + 0]
	st	%g0, [%sp + STACK_START + 4]

	LDF	[%sp + STACK_START], fone
	add	%sp, 8, %sp

	FMOV	fzero, c2
	FMOV	fzero, c3
	FMOV	fzero, c4

	cmp	INCX, 2 * SIZE
	bne	.LL100
	nop

	sra	N, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL15
	nop

	LDF	[X +  0 * SIZE], a1
	LDF	[X +  1 * SIZE], a2
	LDF	[X +  2 * SIZE], a3
	LDF	[X +  3 * SIZE], a4

	LDF	[X +  4 * SIZE], a5
	add	I, -1, I
	LDF	[X +  5 * SIZE], a6
	cmp	I, 0
	LDF	[X +  6 * SIZE], a7
	LDF	[X +  7 * SIZE], a8

	ble,pt	%icc, .LL12
	add	X, 8 * SIZE, X

#define PREFETCHSIZE 40

.LL11:
	FABS	a1, t1
	prefetch [X + PREFETCHSIZE * SIZE], 0
	FABS	a2, t2
	LDF	[X +  0 * SIZE], a1
	FABS	a3, t3
	LDF	[X +  1 * SIZE], a2
	FABS	a4, t4
	LDF	[X +  2 * SIZE], a3

	FCMP	%fcc0, t1, c1
	LDF	[X +  3 * SIZE], a4
	FCMP	%fcc1, t2, c2
	FCMP	%fcc2, t3, c3
	FCMP	%fcc3, t4, c4

	FMOVG	%fcc0, t1, c1
	FMOVG	%fcc1, t2, c2
	FMOVG	%fcc2, t3, c3
	FMOVG	%fcc3, t4, c4

	FABS	a5, t1
	LDF	[X +  4 * SIZE], a5
	FABS	a6, t2
	LDF	[X +  5 * SIZE], a6
	FABS	a7, t3
	LDF	[X +  6 * SIZE], a7
	FABS	a8, t4
	LDF	[X +  7 * SIZE], a8

	FCMP	%fcc0, t1, c1
	FCMP	%fcc1, t2, c2
	FCMP	%fcc2, t3, c3
	FCMP	%fcc3, t4, c4

	FMOVG	%fcc0, t1, c1
	add	I, -1, I
	FMOVG	%fcc1, t2, c2
	cmp	I, 0
	FMOVG	%fcc2, t3, c3
	FMOVG	%fcc3, t4, c4

	bg,pt	%icc, .LL11
	add	X, 8 * SIZE, X

.LL12:
	FABS	a1, t1
	FABS	a2, t2
	FABS	a3, t3
	FABS	a4, t4

	FCMP	%fcc0, t1, c1
	FCMP	%fcc1, t2, c2
	FCMP	%fcc2, t3, c3
	FCMP	%fcc3, t4, c4

	FMOVG	%fcc0, t1, c1
	FMOVG	%fcc1, t2, c2
	FMOVG	%fcc2, t3, c3
	FMOVG	%fcc3, t4, c4

	FABS	a5, t1
	FABS	a6, t2
	FABS	a7, t3
	FABS	a8, t4

	FCMP	%fcc0, t1, c1
	FCMP	%fcc1, t2, c2
	FCMP	%fcc2, t3, c3
	FCMP	%fcc3, t4, c4

	FMOVG	%fcc0, t1, c1
	FMOVG	%fcc1, t2, c2
	FMOVG	%fcc2, t3, c3
	FMOVG	%fcc3, t4, c4

.LL15:
	and	N, 3, I
	cmp	I,  0
	ble,a,pn %icc, .LL19
	nop

.LL16:
	LDF	[X +  0 * SIZE], a1
	LDF	[X +  1 * SIZE], a2
	FABS	a1, t1
	FABS	a2, t2
	FCMP	%fcc0, t1, c1
	FCMP	%fcc1, t2, c2
	FMOVG	%fcc0, t1, c1
	FMOVG	%fcc1, t2, c2
	add	I, -1, I
	cmp	I, 0
	bg,pt	%icc, .LL16
	add	X, 2 * SIZE, X

.LL19:
	FCMP	%fcc0, c2, c1
	FCMP	%fcc1, c4, c3
	mov	XX, X
	FMOVG	%fcc0, c2, c1
	FMOVG	%fcc1, c4, c3
	FCMP	%fcc0, c3, c1
	FMOVG	%fcc0, c3, c1

	FCMP	c1, fzero
	fbe	.LL99
	nop

	FMOV	c1, fmax
	FDIV	fone, c1, fone

	FMOV	fzero, c1
	FMOV	fzero, c2
	FMOV	fzero, c3
	FMOV	fzero, c4

	sra	N, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL35
	nop

	LDF	[X +  0 * SIZE], a1
	LDF	[X +  1 * SIZE], a2
	LDF	[X +  2 * SIZE], a3
	LDF	[X +  3 * SIZE], a4

	LDF	[X +  4 * SIZE], a5
	add	I, -1, I
	LDF	[X +  5 * SIZE], a6
	cmp	I, 0
	LDF	[X +  6 * SIZE], a7
	LDF	[X +  7 * SIZE], a8

	ble,pt	%icc, .LL32
	add	X, 8 * SIZE, X

.LL31:
	FMUL	fone, a1, t1
	prefetch [X + PREFETCHSIZE * SIZE], 0
	FMUL	fone, a2, t2
	LDF	[X +  0 * SIZE], a1
	FMUL	fone, a3, t3
	LDF	[X +  1 * SIZE], a2
	FMUL	fone, a4, t4
	LDF	[X +  2 * SIZE], a3

	FMUL	t1, t1, t1
	LDF	[X +  3 * SIZE], a4
	FMUL	t2, t2, t2
	FMUL	t3, t3, t3
	FMUL	t4, t4, t4

	FADD	c1, t1, c1
	FMUL	fone, a5, t1
	LDF	[X +  4 * SIZE], a5
	FADD	c2, t2, c2
	FMUL	fone, a6, t2
	LDF	[X +  5 * SIZE], a6
	FADD	c3, t3, c3
	FMUL	fone, a7, t3
	LDF	[X +  6 * SIZE], a7
	FADD	c4, t4, c4
	FMUL	fone, a8, t4
	LDF	[X +  7 * SIZE], a8

	FMUL	t1, t1, t1
	FMUL	t2, t2, t2
	FMUL	t3, t3, t3
	FMUL	t4, t4, t4

	FADD	c1, t1, c1
	add	I, -1, I
	FADD	c2, t2, c2
	cmp	I, 0
	FADD	c3, t3, c3
	FADD	c4, t4, c4

	bg,pt	%icc, .LL31
	add	X, 8 * SIZE, X

.LL32:
	FMUL	fone, a1, t1
	FMUL	fone, a2, t2
	FMUL	fone, a3, t3
	FMUL	fone, a4, t4

	FMUL	t1, t1, t1
	FMUL	t2, t2, t2
	FMUL	t3, t3, t3
	FMUL	t4, t4, t4

	FADD	c1, t1, c1
	FMUL	fone, a5, t1
	FADD	c2, t2, c2
	FMUL	fone, a6, t2
	FADD	c3, t3, c3
	FMUL	fone, a7, t3
	FADD	c4, t4, c4
	FMUL	fone, a8, t4

	FMUL	t1, t1, t1
	FMUL	t2, t2, t2
	FMUL	t3, t3, t3
	FMUL	t4, t4, t4

	FADD	c1, t1, c1
	FADD	c2, t2, c2
	FADD	c3, t3, c3
	FADD	c4, t4, c4

.LL35:
	and	N, 3, I
	cmp	I,  0
	ble,a,pn %icc, .LL39
	nop

.LL36:
	LDF	[X +  0 * SIZE], a1
	LDF	[X +  1 * SIZE], a2
	FMUL	fone, a1, t1
	FMUL	fone, a2, t2
	FMUL	t1, t1, t1
	FMUL	t2, t2, t2
	FADD	c1, t1, c1
	FADD	c2, t2, c2
	add	I, -1, I
	cmp	I, 0
	bg,pt	%icc, .LL36
	add	X, 2 * SIZE, X

.LL39:
	FADD	c1, c2, c1
	FADD	c3, c4, c3
	FADD	c1, c3, c1

	FSQRT	c1, c1
	FMUL	fmax, c1, c1

.LL99:
	return	%i7 + 8
	clr	%g0

.LL100:
	sra	N, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL105
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
	add	I, -1, I
	LDF	[X +  0 * SIZE], a7
	cmp	I, 0
	LDF	[X +  1 * SIZE], a8
	ble,pt	%icc, .LL102
	add	X, INCX, X

.LL101:
	FABS	a1, t1
	LDF	[X +  0 * SIZE], a1
	FABS	a2, t2
	LDF	[X +  1 * SIZE], a2
	add	X, INCX, X
	FABS	a3, t3
	LDF	[X +  0 * SIZE], a3
	FABS	a4, t4
	LDF	[X +  1 * SIZE], a4
	add	X, INCX, X

	FCMP	%fcc0, t1, c1
	FCMP	%fcc1, t2, c2
	FCMP	%fcc2, t3, c3
	FCMP	%fcc3, t4, c4

	FMOVG	%fcc0, t1, c1
	FMOVG	%fcc1, t2, c2
	FMOVG	%fcc2, t3, c3
	FMOVG	%fcc3, t4, c4

	FABS	a5, t1
	LDF	[X +  0 * SIZE], a5
	FABS	a6, t2
	LDF	[X +  1 * SIZE], a6
	add	X, INCX, X
	FABS	a7, t3
	LDF	[X +  0 * SIZE], a7
	FABS	a8, t4
	LDF	[X +  1 * SIZE], a8

	FCMP	%fcc0, t1, c1
	FCMP	%fcc1, t2, c2
	FCMP	%fcc2, t3, c3
	FCMP	%fcc3, t4, c4

	FMOVG	%fcc0, t1, c1
	add	I, -1, I
	FMOVG	%fcc1, t2, c2
	cmp	I, 0
	FMOVG	%fcc2, t3, c3
	FMOVG	%fcc3, t4, c4

	bg,pt	%icc, .LL101
	add	X, INCX, X

.LL102:
	FABS	a1, t1
	FABS	a2, t2
	FABS	a3, t3
	FABS	a4, t4

	FCMP	%fcc0, t1, c1
	FCMP	%fcc1, t2, c2
	FCMP	%fcc2, t3, c3
	FCMP	%fcc3, t4, c4

	FMOVG	%fcc0, t1, c1
	FMOVG	%fcc1, t2, c2
	FMOVG	%fcc2, t3, c3
	FMOVG	%fcc3, t4, c4

	FABS	a5, t1
	FABS	a6, t2
	FABS	a7, t3
	FABS	a8, t4

	FCMP	%fcc0, t1, c1
	FCMP	%fcc1, t2, c2
	FCMP	%fcc2, t3, c3
	FCMP	%fcc3, t4, c4

	FMOVG	%fcc0, t1, c1
	FMOVG	%fcc1, t2, c2
	FMOVG	%fcc2, t3, c3
	FMOVG	%fcc3, t4, c4

.LL105:
	and	N, 3, I
	cmp	I,  0
	ble,a,pn %icc, .LL109
	nop

.LL106:
	LDF	[X +  0 * SIZE], a1
	LDF	[X +  1 * SIZE], a2
	FABS	a1, t1
	FABS	a2, t2
	FCMP	%fcc0, t1, c1
	FCMP	%fcc1, t2, c2
	FMOVG	%fcc0, t1, c1
	FMOVG	%fcc1, t2, c2
	add	I, -1, I
	cmp	I, 0
	bg,pt	%icc, .LL106
	add	X, INCX, X

.LL109:
	FCMP	%fcc0, c2, c1
	FCMP	%fcc1, c4, c3
	mov	XX, X
	FMOVG	%fcc0, c2, c1
	FMOVG	%fcc1, c4, c3
	FCMP	%fcc0, c3, c1
	FMOVG	%fcc0, c3, c1

	FCMP	c1, fzero
	fbe	.LL99
	nop

	FMOV	c1, fmax
	FDIV	fone, c1, fone

	FMOV	fzero, c1
	FMOV	fzero, c2
	FMOV	fzero, c3
	FMOV	fzero, c4

	sra	N, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL135
	nop

	LDF	[X +  0 * SIZE], a1
	LDF	[X +  1 * SIZE], a2
	add	X, INCX, X
	LDF	[X +  0 * SIZE], a3
	LDF	[X +  1 * SIZE], a4
	add	X, INCX, X

	LDF	[X +  0 * SIZE], a5
	add	I, -1, I
	LDF	[X +  1 * SIZE], a6
	add	X, INCX, X
	cmp	I, 0
	LDF	[X +  0 * SIZE], a7
	LDF	[X +  1 * SIZE], a8

	ble,pt	%icc, .LL132
	add	X, INCX, X

.LL131:
	FMUL	fone, a1, t1
	prefetch [X + PREFETCHSIZE * SIZE], 0
	FMUL	fone, a2, t2
	LDF	[X +  0 * SIZE], a1
	FMUL	fone, a3, t3
	LDF	[X +  1 * SIZE], a2
	add	X, INCX, X
	FMUL	fone, a4, t4
	LDF	[X +  0 * SIZE], a3

	FMUL	t1, t1, t1
	LDF	[X +  1 * SIZE], a4
	add	X, INCX, X
	FMUL	t2, t2, t2
	FMUL	t3, t3, t3
	FMUL	t4, t4, t4

	FADD	c1, t1, c1
	FMUL	fone, a5, t1
	LDF	[X +  0 * SIZE], a5
	FADD	c2, t2, c2
	FMUL	fone, a6, t2
	LDF	[X +  1 * SIZE], a6
	add	X, INCX, X
	FADD	c3, t3, c3
	FMUL	fone, a7, t3
	LDF	[X +  0 * SIZE], a7
	FADD	c4, t4, c4
	FMUL	fone, a8, t4
	LDF	[X +  1 * SIZE], a8

	FMUL	t1, t1, t1
	FMUL	t2, t2, t2
	FMUL	t3, t3, t3
	FMUL	t4, t4, t4

	FADD	c1, t1, c1
	add	I, -1, I
	FADD	c2, t2, c2
	cmp	I, 0
	FADD	c3, t3, c3
	FADD	c4, t4, c4

	bg,pt	%icc, .LL131
	add	X, INCX, X

.LL132:
	FMUL	fone, a1, t1
	FMUL	fone, a2, t2
	FMUL	fone, a3, t3
	FMUL	fone, a4, t4

	FMUL	t1, t1, t1
	FMUL	t2, t2, t2
	FMUL	t3, t3, t3
	FMUL	t4, t4, t4

	FADD	c1, t1, c1
	FMUL	fone, a5, t1
	FADD	c2, t2, c2
	FMUL	fone, a6, t2
	FADD	c3, t3, c3
	FMUL	fone, a7, t3
	FADD	c4, t4, c4
	FMUL	fone, a8, t4

	FMUL	t1, t1, t1
	FMUL	t2, t2, t2
	FMUL	t3, t3, t3
	FMUL	t4, t4, t4

	FADD	c1, t1, c1
	FADD	c2, t2, c2
	FADD	c3, t3, c3
	FADD	c4, t4, c4

.LL135:
	and	N, 3, I
	cmp	I,  0
	ble,a,pn %icc, .LL139
	nop

.LL136:
	LDF	[X +  0 * SIZE], a1
	LDF	[X +  1 * SIZE], a2
	FMUL	fone, a1, t1
	FMUL	fone, a2, t2
	FMUL	t1, t1, t1
	FMUL	t2, t2, t2
	FADD	c1, t1, c1
	FADD	c2, t2, c2
	add	I, -1, I
	cmp	I, 0
	bg,pt	%icc, .LL136
	add	X, INCX, X

.LL139:
	FADD	c1, c2, c1
	FADD	c3, c4, c3
	FADD	c1, c3, c1

	FSQRT	c1, c1
	FMUL	fmax, c1, c1

	return	%i7 + 8
	clr	%g0

	EPILOGUE
