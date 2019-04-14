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

#define v1	%o0
#define v2	%o1
#define v3	%o2
#define v4	%o3
#define count	%o4

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
#endif

#ifndef USE_MIN
#define FCMOV	FMOVG
#define CMOV	movg
#else
#define FCMOV	FMOVL
#define CMOV	movl
#endif

	PROLOGUE
	SAVESP

	FCLR(0)

	cmp	N, 0
	ble	.LL20
	clr	v1

	cmp	INCX, 0
	ble	.LL20
	sll	INCX, BASE_SHIFT, INCX

	mov	1, v1

	add	N, -1, N
	LDF	[X], c4
	add	X, INCX, X
	cmp	N, 0
	ble	.LL20
	FABS	c4, c1

	FABS	c4, c2
	mov	1, v2
	FABS	c4, c3
	mov	1, v3
	FABS	c4, c4
	mov	1, v4
	mov	2, count

	cmp	INCX, SIZE
	bne	.LL50
	nop

	sra	N, 3, I
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
	nop

	FCMP	%fcc2, t3, c3
	FCMP	%fcc3, t4, c4

	FCMOV	%fcc0, t1, c1
	CMOV	%fcc0, count, v1
	FCMOV	%fcc1, t2, c2
	CMOV	%fcc1, count, v2
	FCMOV	%fcc2, t3, c3
	CMOV	%fcc2, count, v3
	FCMOV	%fcc3, t4, c4
	CMOV	%fcc3, count, v4
	add	count, 4, count

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

	FCMOV	%fcc0, t1, c1
	nop
	CMOV	%fcc0, count, v1
	add	I, -1, I

	FCMOV	%fcc1, t2, c2
	cmp	I, 0
	CMOV	%fcc1, count, v2
	add	X, 8 * SIZE, X

	FCMOV	%fcc2, t3, c3
	CMOV	%fcc2, count, v3
	FCMOV	%fcc3, t4, c4
	CMOV	%fcc3, count, v4
	bg,pt	%icc, .LL11
	add	count, 4, count

.LL12:
	FABS	a1, t1
	FABS	a2, t2
	FABS	a3, t3
	FABS	a4, t4

	FCMP	%fcc0, t1, c1
	FCMP	%fcc1, t2, c2
	FCMP	%fcc2, t3, c3
	FCMP	%fcc3, t4, c4

	FCMOV	%fcc0, t1, c1
	CMOV	%fcc0, count, v1
	FCMOV	%fcc1, t2, c2
	CMOV	%fcc1, count, v2
	FCMOV	%fcc2, t3, c3
	CMOV	%fcc2, count, v3
	FCMOV	%fcc3, t4, c4
	CMOV	%fcc3, count, v4
	add	count, 4, count

	FABS	a5, t1
	FABS	a6, t2
	FABS	a7, t3
	FABS	a8, t4

	FCMP	%fcc0, t1, c1
	FCMP	%fcc1, t2, c2
	FCMP	%fcc2, t3, c3
	FCMP	%fcc3, t4, c4

	FCMOV	%fcc0, t1, c1
	CMOV	%fcc0, count, v1
	FCMOV	%fcc1, t2, c2
	CMOV	%fcc1, count, v2
	FCMOV	%fcc2, t3, c3
	CMOV	%fcc2, count, v3
	FCMOV	%fcc3, t4, c4
	CMOV	%fcc3, count, v4
	add	count, 4, count

.LL15:
	and	N, 7, I
	cmp	I,  0
	ble,a,pn %icc, .LL19
	nop

.LL16:
	LDF	[X +  0 * SIZE], a1
	FABS	a1, t1
	FCMP	%fcc0, t1, c1
	FCMOV	%fcc0, t1, c1
	CMOV	%fcc0, count, v1
	add	I, -1, I
	add	count, 1, count
	cmp	I, 0
	bg,pt	%icc, .LL16
	add	X, 1 * SIZE, X

.LL19:
	FCMP	%fcc0, c2, c1
	add	v2, 1, v2
	FCMP	%fcc1, c4, c3
	add	v3, 2, v3
	add	v4, 3, v4

	FCMOV	%fcc0, c2, c1
	CMOV	%fcc0, v2, v1
	FCMOV	%fcc1, c4, c3
	CMOV	%fcc1, v4, v3
	FCMP	%fcc0, c3, c1
	CMOV	%fcc0, v3, v1

.LL20:
	mov	v1, %i0
	return	%i7 + 8
	nop

.LL50:
	sra	N, 3, I
	cmp	I, 0
	ble,pn	%icc, .LL55
	nop

	LDF	[X +  0 * SIZE], a1
	add	X, INCX, X
	LDF	[X +  0 * SIZE], a2
	add	X, INCX, X
	LDF	[X +  0 * SIZE], a3
	add	X, INCX, X
	LDF	[X +  0 * SIZE], a4
	add	X, INCX, X
	LDF	[X +  0 * SIZE], a5
	add	X, INCX, X
	LDF	[X +  0 * SIZE], a6
	add	X, INCX, X
	add	I, -1, I
	LDF	[X +  0 * SIZE], a7
	cmp	I, 0
	add	X, INCX, X
	LDF	[X +  0 * SIZE], a8
	ble,pt	%icc, .LL52
	add	X, INCX, X

.LL51:
	FABS	a1, t1
	LDF	[X +  0 * SIZE], a1
	add	X, INCX, X
	FABS	a2, t2
	LDF	[X +  0 * SIZE], a2
	add	X, INCX, X
	FABS	a3, t3
	LDF	[X +  0 * SIZE], a3
	add	X, INCX, X
	FABS	a4, t4
	LDF	[X +  0 * SIZE], a4
	add	X, INCX, X

	FCMP	%fcc0, t1, c1
	FCMP	%fcc1, t2, c2
	FCMP	%fcc2, t3, c3
	FCMP	%fcc3, t4, c4

	FCMOV	%fcc0, t1, c1
	CMOV	%fcc0, count, v1
	FCMOV	%fcc1, t2, c2
	CMOV	%fcc1, count, v2
	FCMOV	%fcc2, t3, c3
	CMOV	%fcc2, count, v3
	FCMOV	%fcc3, t4, c4
	CMOV	%fcc3, count, v4
	add	count, 4, count

	FABS	a5, t1
	LDF	[X +  0 * SIZE], a5
	add	X, INCX, X
	FABS	a6, t2
	LDF	[X +  0 * SIZE], a6
	add	X, INCX, X
	FABS	a7, t3
	LDF	[X +  0 * SIZE], a7
	add	X, INCX, X
	FABS	a8, t4
	LDF	[X +  0 * SIZE], a8

	FCMP	%fcc0, t1, c1
	FCMP	%fcc1, t2, c2
	FCMP	%fcc2, t3, c3
	FCMP	%fcc3, t4, c4

	FCMOV	%fcc0, t1, c1
	CMOV	%fcc0, count, v1
	add	I, -1, I
	FCMOV	%fcc1, t2, c2
	CMOV	%fcc1, count, v2
	cmp	I, 0
	FCMOV	%fcc2, t3, c3
	CMOV	%fcc2, count, v3
	FCMOV	%fcc3, t4, c4
	CMOV	%fcc3, count, v4
	add	count, 4, count

	bg,pt	%icc, .LL51
	add	X, INCX, X

.LL52:
	FABS	a1, t1
	FABS	a2, t2
	FABS	a3, t3
	FABS	a4, t4

	FCMP	%fcc0, t1, c1
	FCMP	%fcc1, t2, c2
	FCMP	%fcc2, t3, c3
	FCMP	%fcc3, t4, c4

	FCMOV	%fcc0, t1, c1
	CMOV	%fcc0, count, v1
	FCMOV	%fcc1, t2, c2
	CMOV	%fcc1, count, v2
	FCMOV	%fcc2, t3, c3
	CMOV	%fcc2, count, v3
	FCMOV	%fcc3, t4, c4
	CMOV	%fcc3, count, v4
	add	count, 4, count

	FABS	a5, t1
	FABS	a6, t2
	FABS	a7, t3
	FABS	a8, t4

	FCMP	%fcc0, t1, c1
	FCMP	%fcc1, t2, c2
	FCMP	%fcc2, t3, c3
	FCMP	%fcc3, t4, c4

	FCMOV	%fcc0, t1, c1
	CMOV	%fcc0, count, v1
	FCMOV	%fcc1, t2, c2
	CMOV	%fcc1, count, v2
	FCMOV	%fcc2, t3, c3
	CMOV	%fcc2, count, v3
	FCMOV	%fcc3, t4, c4
	CMOV	%fcc3, count, v4
	add	count, 4, count

.LL55:
	and	N, 7, I
	cmp	I,  0
	ble,a,pn %icc, .LL59
	nop

.LL56:
	LDF	[X +  0 * SIZE], a1
	FABS	a1, t1
	FCMP	%fcc0, t1, c1
	FCMOV	%fcc0, t1, c1
	CMOV	%fcc0, count, v1
	add	I, -1, I
	add	count, 1, count
	cmp	I, 0
	bg,pt	%icc, .LL56
	add	X, INCX, X

.LL59:
	FCMP	%fcc0, c2, c1
	add	v2, 1, v2
	FCMP	%fcc1, c4, c3
	add	v3, 2, v3
	add	v4, 3, v4

	FCMOV	%fcc0, c2, c1
	CMOV	%fcc0, v2, v1
	FCMOV	%fcc1, c4, c3
	CMOV	%fcc1, v4, v3
	FCMP	%fcc0, c3, c1
	CMOV	%fcc0, v3, v1

	mov	v1, %i0
	return	%i7 + 8
	nop

	EPILOGUE
