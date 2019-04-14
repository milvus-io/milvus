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

	PROLOGUE
	SAVESP

	FCLR(0)

	FMOV	c1, c2
	FMOV	c1, c3
	FMOV	c1, c4
	FMOV	c1, t1
	FMOV	c1, t2
	FMOV	c1, t3
	FMOV	c1, t4

	cmp	INCX, 0
	ble	.LL20
	sll	INCX, ZBASE_SHIFT, INCX

	cmp	N, 0
	ble	.LL20
	nop

	cmp	INCX, 2 * SIZE
	bne	.LL50
	nop

	sra	N, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL15
	nop

	ld	[X +  0 * SIZE], a1
	add	I, -1, I
	ld	[X +  1 * SIZE], a2
	cmp	I, 0
	ld	[X +  2 * SIZE], a3
	ld	[X +  3 * SIZE], a4
	ld	[X +  4 * SIZE], a5
	ld	[X +  5 * SIZE], a6
	ld	[X +  6 * SIZE], a7
	ld	[X +  7 * SIZE], a8

	ble,pt	%icc, .LL12
	add	X, 8 * SIZE, X

#define PREFETCHSIZE 40

.LL11:
	faddd	c1, t1, c1
	fsmuld	a1, a1, t1
	prefetch [X  + PREFETCHSIZE * SIZE], 0

	faddd	c2, t2, c2
	add	I, -1, I
	fsmuld	a2, a2, t2
	ld	[X +  0 * SIZE], a1

	faddd	c3, t3, c3
	cmp	I, 0
	fsmuld	a3, a3, t3
	ld	[X +  1 * SIZE], a2

	faddd	c4, t4, c4
	fsmuld	a4, a4, t4
	ld	[X +  2 * SIZE], a3

	faddd	c1, t1, c1
	fsmuld	a5, a5, t1
	ld	[X +  3 * SIZE], a4

	faddd	c2, t2, c2
	fsmuld	a6, a6, t2
	ld	[X +  4 * SIZE], a5

	faddd	c3, t3, c3
	fsmuld	a7, a7, t3
	ld	[X +  5 * SIZE], a6

	faddd	c4, t4, c4
	ld	[X +  6 * SIZE], a7
	fsmuld	a8, a8, t4
	add	X, 8 * SIZE, X

	bg,pt	%icc, .LL11
	ld	[X -  1 * SIZE], a8

.LL12:
	faddd	c1, t1, c1
	fsmuld	a1, a1, t1
	faddd	c2, t2, c2
	fsmuld	a2, a2, t2

	faddd	c3, t3, c3
	fsmuld	a3, a3, t3
	faddd	c4, t4, c4
	fsmuld	a4, a4, t4

	faddd	c1, t1, c1
	fsmuld	a5, a5, t1
	faddd	c2, t2, c2
	fsmuld	a6, a6, t2

	faddd	c3, t3, c3
	fsmuld	a7, a7, t3
	faddd	c4, t4, c4
	fsmuld	a8, a8, t4

.LL15:
	and	N, 3, I
	cmp	I,  0
	ble,a,pn %icc, .LL19
	nop

.LL16:
	ld	[X +  0 * SIZE], a1
	add	I, -1, I
	ld	[X +  1 * SIZE], a2
	cmp	I, 0
	faddd	c1, t1, c1
	faddd	c2, t2, c2
	fsmuld	a1, a1, t1
	fsmuld	a2, a2, t2
	bg,pt	%icc, .LL16
	add	X, 2 * SIZE, X

.LL19:
	faddd	c1, t1, c1
	faddd	c2, t2, c2
	faddd	c3, t3, c3
	faddd	c4, t4, c4

	faddd	c1, c2, c1
	faddd	c3, c4, c3
	faddd	c1, c3, c1

	fsqrtd	c1, c1

#if !defined(NEED_F2CCONV) || !defined(F_INTERFACE_F2C)
	fdtos	c1, c1
#endif
.LL20:
	return	%i7 + 8
	clr	%g0

.LL50:
	sra	N, 2, I
	cmp	I, 0
	ble,pn	%icc, .LL55
	nop

	ld	[X +  0 * SIZE], a1
	ld	[X +  1 * SIZE], a2
	add	X, INCX, X
	ld	[X +  0 * SIZE], a3
	ld	[X +  1 * SIZE], a4
	add	X, INCX, X
	ld	[X +  0 * SIZE], a5
	ld	[X +  1 * SIZE], a6
	add	X, INCX, X
	add	I, -1, I
	ld	[X +  0 * SIZE], a7
	cmp	I, 0
	ld	[X +  1 * SIZE], a8

	ble,pt	%icc, .LL52
	add	X, INCX, X

.LL51:
	faddd	c1, t1, c1
	add	I, -1, I
	fsmuld	a1, a1, t1
	ld	[X +  0 * SIZE], a1

	faddd	c2, t2, c2
	cmp	I, 0
	fsmuld	a2, a2, t2
	ld	[X +  1 * SIZE], a2
	add	X, INCX, X

	faddd	c3, t3, c3
	fsmuld	a3, a3, t3
	ld	[X +  0 * SIZE], a3

	faddd	c4, t4, c4
	fsmuld	a4, a4, t4
	ld	[X +  1 * SIZE], a4
	add	X, INCX, X

	faddd	c1, t1, c1
	fsmuld	a5, a5, t1
	ld	[X +  0 * SIZE], a5

	faddd	c2, t2, c2
	fsmuld	a6, a6, t2
	ld	[X +  1 * SIZE], a6
	add	X, INCX, X

	faddd	c3, t3, c3
	fsmuld	a7, a7, t3
	ld	[X +  0 * SIZE], a7

	faddd	c4, t4, c4
	fsmuld	a8, a8, t4
	ld	[X +  1 * SIZE], a8
	bg,pt	%icc, .LL51
	add	X, INCX, X

.LL52:
	faddd	c1, t1, c1
	fsmuld	a1, a1, t1
	faddd	c2, t2, c2
	fsmuld	a2, a2, t2

	faddd	c3, t3, c3
	fsmuld	a3, a3, t3
	faddd	c4, t4, c4
	fsmuld	a4, a4, t4

	faddd	c1, t1, c1
	fsmuld	a5, a5, t1
	faddd	c2, t2, c2
	fsmuld	a6, a6, t2

	faddd	c3, t3, c3
	fsmuld	a7, a7, t3
	faddd	c4, t4, c4
	fsmuld	a8, a8, t4

.LL55:
	and	N, 3, I
	cmp	I,  0
	ble,a,pn %icc, .LL59
	nop

.LL56:
	ld	[X +  0 * SIZE], a1
	add	I, -1, I
	ld	[X +  1 * SIZE], a2
	cmp	I, 0
	faddd	c1, t1, c1
	faddd	c2, t2, c2
	fsmuld	a1, a1, t1
	fsmuld	a2, a2, t2
	bg,pt	%icc, .LL56
	add	X, INCX, X

.LL59:
	faddd	c1, t1, c1
	faddd	c2, t2, c2
	faddd	c3, t3, c3
	faddd	c4, t4, c4

	faddd	c1, c2, c1
	faddd	c3, c4, c3
	faddd	c1, c3, c1

	fsqrtd	c1, c1

#if !defined(NEED_F2CCONV) || !defined(F_INTERFACE_F2C)
	fdtos	c1, c1
#endif

	return	%i7 + 8
	clr	%o0

	EPILOGUE
