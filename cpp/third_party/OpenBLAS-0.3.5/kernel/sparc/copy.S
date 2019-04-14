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
#define Y	%i3
#define INCY	%i4
#define I	%i5

#ifdef DOUBLE
#define a1	%f0
#define a2	%f2
#define a3	%f4
#define a4	%f6
#define a5	%f8
#define a6	%f10
#define a7	%f12
#define a8	%f14
#define a9	%f16
#define a10	%f18
#define a11	%f20
#define a12	%f22
#define a13	%f24
#define a14	%f26
#define a15	%f28
#define a16	%f30
#else
#define a1	%f0
#define a2	%f1
#define a3	%f2
#define a4	%f3
#define a5	%f4
#define a6	%f5
#define a7	%f6
#define a8	%f7
#define a9	%f8
#define a10	%f9
#define a11	%f10
#define a12	%f11
#define a13	%f12
#define a14	%f13
#define a15	%f14
#define a16	%f15
#endif

	PROLOGUE
	SAVESP

	sll	INCX, BASE_SHIFT, INCX
	sll	INCY, BASE_SHIFT, INCY

	cmp	INCX, SIZE
	bne	.LL50
	nop
	cmp	INCY, SIZE
	bne	.LL50
	nop

	sra	N, 3, I
	cmp	I, 0
	ble,pn	%icc, .LL15
	nop

#define PREFETCHSIZE 32

.LL11:
	LDF	[X +  0 * SIZE], a1
	prefetch [X  + PREFETCHSIZE * SIZE], 0
	LDF	[X +  1 * SIZE], a2
	LDF	[X +  2 * SIZE], a3
	LDF	[X +  3 * SIZE], a4
	LDF	[X +  4 * SIZE], a5
	LDF	[X +  5 * SIZE], a6
	LDF	[X +  6 * SIZE], a7
	LDF	[X +  7 * SIZE], a8

	STF	a1, [Y +  0 * SIZE]
	prefetch [Y  + PREFETCHSIZE * SIZE], 0
	STF	a2, [Y +  1 * SIZE]
	STF	a3, [Y +  2 * SIZE]
	STF	a4, [Y +  3 * SIZE]
	STF	a5, [Y +  4 * SIZE]
	STF	a6, [Y +  5 * SIZE]
	STF	a7, [Y +  6 * SIZE]
	STF	a8, [Y +  7 * SIZE]

	add	I, -1, I
	cmp	I, 0
	add	Y,  8 * SIZE, Y
	add	X,  8 * SIZE, X

	bg,pt	%icc, .LL11
	nop


.LL15:
	and	N,  7, I
	cmp	I,  0
	ble,a,pn %icc, .LL19
	nop

.LL16:
	LDF	[X +  0 * SIZE], a1
	add	I, -1, I
	cmp	I, 0
	add	X, 1 * SIZE, X
	STF	a1, [Y +  0 * SIZE]
	bg,pt	%icc, .LL16
	add	Y, 1 * SIZE, Y

.LL19:
	return	%i7 + 8
	clr	%g0

.LL50:
	sra	N, 3, I
	cmp	I, 0
	ble,pn	%icc, .LL55
	nop

.LL51:
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
	LDF	[X +  0 * SIZE], a7
	add	X, INCX, X
	LDF	[X +  0 * SIZE], a8
	add	X, INCX, X

	STF	a1, [Y +  0 * SIZE]
	add	Y, INCY, Y
	add	I, -1, I
	STF	a2, [Y +  0 * SIZE]
	add	Y, INCY, Y
	cmp	I, 0
	STF	a3, [Y +  0 * SIZE]
	add	Y, INCY, Y
	STF	a4, [Y +  0 * SIZE]
	add	Y, INCY, Y
	STF	a5, [Y +  0 * SIZE]
	add	Y, INCY, Y
	STF	a6, [Y +  0 * SIZE]
	add	Y, INCY, Y
	STF	a7, [Y +  0 * SIZE]
	add	Y, INCY, Y
	STF	a8, [Y +  0 * SIZE]

	bg,pt	%icc, .LL51
	add	Y, INCY, Y

.LL55:
	and	N, 7, I
	cmp	I,  0
	ble,a,pn %icc, .LL59
	nop

.LL56:
	LDF	[X +  0 * SIZE], a1
	add	I, -1, I
	cmp	I, 0
	add	X, INCX, X
	STF	a1, [Y +  0 * SIZE]
	bg,pt	%icc, .LL56
	add	Y, INCY, Y

.LL59:
	return	%i7 + 8
	clr	%o0

	EPILOGUE
