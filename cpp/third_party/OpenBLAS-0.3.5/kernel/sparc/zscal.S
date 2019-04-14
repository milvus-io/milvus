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
#define X	%i3
#define INCX	%i4
#else
#define X	%i5
#define INCX	%i3
#endif

#define I	%i1
#define XX	%i2

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

#define c9	%f32
#define c10	%f34
#define c11	%f36
#define c12	%f38
#define c13	%f40
#define c14	%f42
#define c15	%f44
#define c16	%f46

#define s1	%f32
#define s2	%f34
#define s3	%f36
#define s4	%f38
#define s5	%f40
#define s6	%f42
#define s7	%f44
#define s8	%f46

#define FZERO	%f48
#define ALPHA_R	%f50
#define ALPHA_I	%f52
#else
#define c1	%f0
#define c2	%f1
#define c3	%f2
#define c4	%f3
#define c5	%f4
#define c6	%f5
#define c7	%f6
#define c8	%f7

#define c9	%f8
#define c10	%f9
#define c11	%f10
#define c12	%f11
#define c13	%f12
#define c14	%f13
#define c15	%f14
#define c16	%f15

#define s1	%f8
#define s2	%f9
#define s3	%f10
#define s4	%f11
#define s5	%f12
#define s6	%f13
#define s7	%f14
#define s8	%f15

#define t1	%f16
#define t2	%f17
#define t3	%f18
#define t4	%f19
#define t5	%f20
#define t6	%f21
#define t7	%f22
#define t8	%f23

#define FZERO	%f24
#define ALPHA_R	%f25
#define ALPHA_I	%f26
#endif

#define PREFETCHSIZE 128

	PROLOGUE
	SAVESP

#ifndef __64BIT__
#ifdef DOUBLE
	st	%i3, [%sp + STACK_START + 16]
	st	%i4, [%sp + STACK_START + 20]
	st	%i5, [%sp + STACK_START + 24]

	ld	[%sp+ STACK_START + 32], X
	ld	[%sp+ STACK_START + 36], INCX
#else
	st	%i3, [%sp + STACK_START + 16]
	st	%i4, [%sp + STACK_START + 24]
	ld	[%sp+  STACK_START + 28], INCX
#endif
	LDF	[%sp + STACK_START + 16], ALPHA_R
	LDF	[%sp + STACK_START + 24], ALPHA_I
#else
	ldx	[%sp + STACK_START + 56], INCX
#ifdef DOUBLE
	FMOV	%f6, ALPHA_R
	FMOV	%f8, ALPHA_I
#else
	FMOV	%f7, ALPHA_R
	FMOV	%f9, ALPHA_I
#endif
#endif

#ifdef DOUBLE
	FCLR(17)
#else
	FCLR(24)
#endif

	FCMP	ALPHA_R, FZERO
	fbne	.LL100
	sll	INCX, ZBASE_SHIFT, INCX

	FCMP	ALPHA_I, FZERO
	fbne	.LL100
	nop
	cmp	INCX, 2 * SIZE
	bne	.LL50
	nop
	sra	N, 2, I
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
	and	N, 3, I
	cmp	I,  0
	ble,a,pn %icc, .LL19
	nop

.LL16:
	STF	FZERO, [X +  0 * SIZE]
	STF	FZERO, [X +  1 * SIZE]
	add	I, -1, I
	cmp	I, 0
	bg,pt	%icc, .LL16
	add	X, 2 * SIZE, X

.LL19:
	return	%i7 + 8
	clr	%o0

.LL50:
	sra	N, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL55
	nop

.LL51:
	STF	FZERO, [X +  0 * SIZE]
	add	I, -1, I
	STF	FZERO, [X +  1 * SIZE]
	add	X, INCX, X
	STF	FZERO, [X +  0 * SIZE]
	cmp	I, 0
	STF	FZERO, [X +  1 * SIZE]
	add	X, INCX, X
	STF	FZERO, [X +  0 * SIZE]
	STF	FZERO, [X +  1 * SIZE]
	add	X, INCX, X
	STF	FZERO, [X +  0 * SIZE]
	STF	FZERO, [X +  1 * SIZE]
	bg,pt	%icc, .LL51
	add	X, INCX, X

.LL55:
	and	N, 3, I
	cmp	I,  0
	ble,a,pn %icc, .LL59
	nop

.LL56:
	STF	FZERO, [X +  0 * SIZE]
	add	I, -1, I
	STF	FZERO, [X +  1 * SIZE]
	cmp	I, 0
	bg,pt	%icc, .LL56
	add	X, INCX, X

.LL59:
	return	%i7 + 8
	clr	%o0

.LL100:
	cmp	INCX, 2 * SIZE
	bne	.LL150
	sra	N, 2, I

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

	FMUL	ALPHA_R, c1, t1
	FMUL	ALPHA_I, c2, t3

	FMUL	ALPHA_I, c1, t2
	LDF	[X +  8 * SIZE], c1
	FMUL	ALPHA_R, c2, t4
	LDF	[X +  9 * SIZE], c2

	FMUL	ALPHA_R, c3, t5
	deccc	I
	FMUL	ALPHA_I, c4, t7
	FSUB	t1,  t3,  s1

	FMUL	ALPHA_I, c3, t6
	LDF	[X + 10 * SIZE], c3
	FMUL	ALPHA_R, c4, t8
	LDF	[X + 11 * SIZE], c4
	FADD	t4,  t2,  s2

	ble,pn	%icc, .LL112
	nop

.LL111:
	prefetch [X  + PREFETCHSIZE * SIZE], 0

	FMUL	ALPHA_R, c5, t1
	FMUL	ALPHA_I, c6, t3
	FSUB	t5,  t7,  s3
	STF	s1, [X +  0 * SIZE]

	FMUL	ALPHA_I, c5, t2
	LDF	[X + 12 * SIZE], c5
	FMUL	ALPHA_R, c6, t4
	LDF	[X + 13 * SIZE], c6

	FADD	t8,  t6,  s4
	STF	s2, [X +  1 * SIZE]

	FMUL	ALPHA_R, c7, t5
	FMUL	ALPHA_I, c8, t7
	FSUB	t1,  t3,  s5
	STF	s3, [X +  2 * SIZE]

	FMUL	ALPHA_I, c7, t6
	LDF	[X + 14 * SIZE], c7
	FMUL	ALPHA_R, c8, t8
	LDF	[X + 15 * SIZE], c8

	FADD	t4,  t2,  s6
	STF	s4, [X +  3 * SIZE]

	FMUL	ALPHA_R, c1, t1
	FMUL	ALPHA_I, c2, t3
	FSUB	t5,  t7,  s7
	STF	s5, [X +  4 * SIZE]

	FMUL	ALPHA_I, c1, t2
	LDF	[X + 16 * SIZE], c1
	FMUL	ALPHA_R, c2, t4
	LDF	[X + 17 * SIZE], c2

	FADD	t8,  t6,  s8
	STF	s6, [X +  5 * SIZE]

	FMUL	ALPHA_R, c3, t5
	deccc	I
	FMUL	ALPHA_I, c4, t7
	FSUB	t1,  t3,  s1
	STF	s7, [X +  6 * SIZE]

	FMUL	ALPHA_I, c3, t6
	LDF	[X + 18 * SIZE], c3
	FMUL	ALPHA_R, c4, t8
	LDF	[X + 19 * SIZE], c4

	FADD	t4,  t2,  s2
	STF	s8, [X +  7 * SIZE]

	bg,pt	%icc, .LL111
	add	X, 8 * SIZE, X


.LL112:
	FMUL	ALPHA_R, c5, t1
	FMUL	ALPHA_I, c6, t3
	FSUB	t5,  t7,  s3
	STF	s1, [X +  0 * SIZE]

	FMUL	ALPHA_I, c5, t2
	FMUL	ALPHA_R, c6, t4
	FADD	t8,  t6,  s4
	STF	s2, [X +  1 * SIZE]

	FMUL	ALPHA_R, c7, t5
	FMUL	ALPHA_I, c8, t7
	FSUB	t1,  t3,  s5
	STF	s3, [X +  2 * SIZE]

	FMUL	ALPHA_I, c7, t6
	FMUL	ALPHA_R, c8, t8
	FADD	t4,  t2,  s6
	STF	s4, [X +  3 * SIZE]

	FSUB	t5,  t7,  s7
	FADD	t8,  t6,  s8

	STF	s5, [X +  4 * SIZE]
	STF	s6, [X +  5 * SIZE]
	STF	s7, [X +  6 * SIZE]
	STF	s8, [X +  7 * SIZE]
	add	X, 8 * SIZE, X

.LL115:
	and	N, 3, I
	cmp	I,  0
	ble,a,pn %icc, .LL119
	nop

.LL116:
	LDF	[X +  0 * SIZE], c1
	LDF	[X +  1 * SIZE], c2

	FMUL	ALPHA_R, c1, c3
	FMUL	ALPHA_I, c1, c4
	FMUL	ALPHA_I, c2, c1
	FMUL	ALPHA_R, c2, c2

	FSUB	c3, c1, c1
	FADD	c2, c4, c2

	STF	c1, [X +  0 * SIZE]
	STF	c2, [X +  1 * SIZE]

	add	I, -1, I
	cmp	I, 0
	bg,pt	%icc, .LL116
	add	X, 2 * SIZE, X

.LL119:
	return	%i7 + 8
	clr	%o0

.LL150:
	sra	N, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL155
	mov	X, XX

.LL151:
	LDF	[X +  0 * SIZE], c1
	LDF	[X +  1 * SIZE], c2
	add	X, INCX, X
	LDF	[X +  0 * SIZE], c3
	FMUL	ALPHA_R, c1, c9
	LDF	[X +  1 * SIZE], c4
	FMUL	ALPHA_I, c1, c10
	add	X, INCX, X
	LDF	[X +  0 * SIZE], c5
	FMUL	ALPHA_I, c2, c1
	LDF	[X +  1 * SIZE], c6
	FMUL	ALPHA_R, c2, c2
	add	X, INCX, X
	LDF	[X +  0 * SIZE], c7
	FMUL	ALPHA_R, c3, c11
	LDF	[X +  1 * SIZE], c8
	FMUL	ALPHA_I, c3, c12
	add	X, INCX, X

	FMUL	ALPHA_I, c4, c3
	FMUL	ALPHA_R, c4, c4

	FMUL	ALPHA_R, c5, c13
	FMUL	ALPHA_I, c5, c14
	FMUL	ALPHA_I, c6, c5
	FMUL	ALPHA_R, c6, c6

	FMUL	ALPHA_R, c7, c15
	FSUB	c9,  c1,  c1
	FMUL	ALPHA_I, c7, c16
	FADD	c2,  c10, c2
	FMUL	ALPHA_I, c8, c7
	FSUB	c11, c3,  c3
	FMUL	ALPHA_R, c8, c8
	FADD	c4,  c12, c4

	STF	c1, [XX +  0 * SIZE]
	FSUB	c13, c5,  c5
	add	I, -1, I
	STF	c2, [XX +  1 * SIZE]
	FADD	c6,  c14, c6
	add	XX, INCX, XX
	STF	c3, [XX +  0 * SIZE]
	FSUB	c15, c7,  c7
	cmp	I, 0
	STF	c4, [XX +  1 * SIZE]
	FADD	c8,  c16, c8
	add	XX, INCX, XX
	STF	c5, [XX +  0 * SIZE]
	STF	c6, [XX +  1 * SIZE]
	add	XX, INCX, XX
	STF	c7, [XX +  0 * SIZE]
	STF	c8, [XX +  1 * SIZE]
	bg,pt	%icc, .LL151
	add	XX, INCX, XX

.LL155:
	and	N, 3, I
	cmp	I,  0
	ble,a,pn %icc, .LL159
	nop

.LL156:
	LDF	[X +  0 * SIZE], c1
	LDF	[X +  1 * SIZE], c2

	FMUL	ALPHA_R, c1, c3
	FMUL	ALPHA_I, c1, c4
	FMUL	ALPHA_I, c2, c1
	FMUL	ALPHA_R, c2, c2

	FSUB	c3, c1, c1
	FADD	c2, c4, c2

	STF	c1, [X +  0 * SIZE]
	STF	c2, [X +  1 * SIZE]

	add	I, -1, I
	cmp	I, 0
	bg,pt	%icc, .LL156
	add	X, INCX, X

.LL159:
	return	%i7 + 8
	clr	%o0


	EPILOGUE
