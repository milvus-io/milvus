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

#ifdef linux
#ifndef __64BIT__
#define N	r3
#define X	r6
#define INCX	r7
#define Y	r8
#define INCY	r9
#define	YY	r4
#define PRE	r5
#else
#define N	r3
#define X	r8
#define INCX	r9
#define Y	r5
#define INCY	r4
#define	YY	r6
#define PRE	r7
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
#define N	r3
#define X	r10
#define INCX	r4
#define Y	r5
#define INCY	r6
#define	YY	r7
#define PRE	r8
#else
#define N	r3
#define X	r8
#define INCX	r9
#define Y	r10
#define INCY	r4
#define	YY	r5
#define PRE	r6
#endif
#endif

#define ALPHA_R	f24
#define ALPHA_I	f25

#ifndef CONJ
#define ADD1	FNMSUB
#define ADD2	FMADD
#else
#define ADD1	FMADD
#define ADD2	FNMSUB
#endif

#define STACKSIZE 96

	PROLOGUE
	PROFCODE

	subi	SP, SP, STACKSIZE

	stfd	f14,    0(SP)
	stfd	f15,    8(SP)
	stfd	f16,   16(SP)
	stfd	f17,   24(SP)

	stfd	f18,   32(SP)
	stfd	f19,   40(SP)
	stfd	f20,   48(SP)
	stfd	f21,   56(SP)

	stfd	f22,   64(SP)
	stfd	f23,   72(SP)
	stfd	f24,   80(SP)
	stfd	f25,   88(SP)

#if defined(linux) && defined(__64BIT__)
        ld	INCY, FRAMESLOT(0) + STACKSIZE(SP)
#endif

#if defined(_AIX) || defined(__APPLE__)
#ifdef __64BIT__
	ld	INCY, FRAMESLOT(0) + STACKSIZE(SP)
#else
#ifdef DOUBLE
	lwz	INCX, FRAMESLOT(0) + STACKSIZE(SP)
	lwz	Y,    FRAMESLOT(1) + STACKSIZE(SP)
	lwz	INCY, FRAMESLOT(2) + STACKSIZE(SP)
#else
	lwz	INCY, FRAMESLOT(0) + STACKSIZE(SP)
#endif
#endif
#endif

	fmr	ALPHA_R, f1
	slwi	INCX, INCX, ZBASE_SHIFT
	fmr	ALPHA_I, f2
	slwi	INCY, INCY, ZBASE_SHIFT

	subi	INCX, INCX, SIZE
	subi	INCY, INCY, SIZE

	li	PRE, 2 * 16 * SIZE

	cmpwi	cr0, N, 0
	ble-	LL(999)

	sub	X, X, INCX
	sub	Y, Y, INCY
	mr	YY, Y

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	ble-	LL(150)
	.align 4

	LFDUX	f0,    X, INCX
	LFDU	f1,    1 * SIZE(X)
	LFDUX	f2,    X, INCX
	LFDU	f3,    1 * SIZE(X)

	LFDUX	f8,    Y, INCY
	LFDU	f9,    1 * SIZE(Y)
	LFDUX	f10,   Y, INCY
	LFDU	f11,   1 * SIZE(Y)

	LFDUX	f4,    X, INCX
	LFDU	f5,    1 * SIZE(X)
	LFDUX	f6,    X, INCX
	LFDU	f7,    1 * SIZE(X)

	LFDUX	f12,   Y, INCY
	LFDU	f13,   1 * SIZE(Y)
	LFDUX	f14,   Y, INCY
	LFDU	f15,   1 * SIZE(Y)
	bdz	LL(120)
	.align 4

LL(110):
	FMADD	f16,  ALPHA_R, f0, f8
	LFDUX	f8,    Y, INCY
	FMADD	f17,  ALPHA_I, f0, f9
	LFDU	f9,    1 * SIZE(Y)
	FMADD	f18,  ALPHA_R, f2, f10
	LFDUX	f10,   Y, INCY
	FMADD	f19,  ALPHA_I, f2, f11
	LFDU	f11,   1 * SIZE(Y)
#ifdef PPCG4
	dcbt	X, PRE
#endif

	ADD1	f16,  ALPHA_I, f1, f16
	LFDUX	f0,    X, INCX
	ADD2	f17,  ALPHA_R, f1, f17
	LFDU	f1,    1 * SIZE(X)
	ADD1	f18,  ALPHA_I, f3, f18
	LFDUX	f2,    X, INCX
	ADD2	f19,  ALPHA_R, f3, f19
	LFDU	f3,    1 * SIZE(X)
#ifdef PPCG4
	dcbtst	Y, PRE
#endif

	FMADD	f20,  ALPHA_R, f4, f12
	LFDUX	f12,   Y, INCY
	FMADD	f21,  ALPHA_I, f4, f13
	LFDU	f13,   1 * SIZE(Y)
	FMADD	f22,  ALPHA_R, f6, f14
	LFDUX	f14,   Y, INCY
	FMADD	f23,  ALPHA_I, f6, f15
	LFDU	f15,   1 * SIZE(Y)
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	X, PRE
#endif

	ADD1	f20,  ALPHA_I, f5, f20
	LFDUX	f4,    X, INCX
	ADD2	f21,  ALPHA_R, f5, f21
	LFDU	f5,    1 * SIZE(X)
	ADD1	f22,  ALPHA_I, f7, f22
	LFDUX	f6,    X, INCX
	ADD2	f23,  ALPHA_R, f7, f23
	LFDU	f7,    1 * SIZE(X)
#if defined(PPCG4) && defined(DOUBLE)
	dcbtst	Y, PRE
#endif

	STFDUX	f16,   YY, INCY
	STFDU	f17,   1 * SIZE(YY)
	STFDUX	f18,   YY, INCY
	STFDU	f19,   1 * SIZE(YY)

	FMADD	f16,  ALPHA_R, f0, f8
	LFDUX	f8,    Y, INCY
	FMADD	f17,  ALPHA_I, f0, f9
	LFDU	f9,    1 * SIZE(Y)
	FMADD	f18,  ALPHA_R, f2, f10
	LFDUX	f10,   Y, INCY
	FMADD	f19,  ALPHA_I, f2, f11
	LFDU	f11,   1 * SIZE(Y)
#ifdef PPCG4
	dcbt	X, PRE
#endif

	ADD1	f16,  ALPHA_I, f1, f16
	LFDUX	f0,    X, INCX
	ADD2	f17,  ALPHA_R, f1, f17
	LFDU	f1,    1 * SIZE(X)
	ADD1	f18,  ALPHA_I, f3, f18
	LFDUX	f2,    X, INCX
	ADD2	f19,  ALPHA_R, f3, f19
	LFDU	f3,    1 * SIZE(X)
#ifdef PPCG4
	dcbtst	Y, PRE
#endif

	STFDUX	f20,   YY, INCY
	STFDU	f21,   1 * SIZE(YY)
	STFDUX	f22,   YY, INCY
	STFDU	f23,   1 * SIZE(YY)

	FMADD	f20,  ALPHA_R, f4, f12
	LFDUX	f12,   Y, INCY
	FMADD	f21,  ALPHA_I, f4, f13
	LFDU	f13,   1 * SIZE(Y)
	FMADD	f22,  ALPHA_R, f6, f14
	LFDUX	f14,   Y, INCY
	FMADD	f23,  ALPHA_I, f6, f15
	LFDU	f15,   1 * SIZE(Y)
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	X, PRE
#endif

	ADD1	f20,  ALPHA_I, f5, f20
	LFDUX	f4,    X, INCX
	ADD2	f21,  ALPHA_R, f5, f21
	LFDU	f5,    1 * SIZE(X)
	ADD1	f22,  ALPHA_I, f7, f22
	LFDUX	f6,    X, INCX
	ADD2	f23,  ALPHA_R, f7, f23
	LFDU	f7,    1 * SIZE(X)
#if defined(PPCG4) && defined(DOUBLE)
	dcbtst	Y, PRE
#endif

	STFDUX	f16,   YY, INCY
	STFDU	f17,   1 * SIZE(YY)
	STFDUX	f18,   YY, INCY
	STFDU	f19,   1 * SIZE(YY)

	STFDUX	f20,   YY, INCY
	STFDU	f21,   1 * SIZE(YY)
	STFDUX	f22,   YY, INCY
	STFDU	f23,   1 * SIZE(YY)
	bdnz	LL(110)
	.align 4

LL(120):
	FMADD	f16,  ALPHA_R, f0, f8
	LFDUX	f8,    Y, INCY
	FMADD	f17,  ALPHA_I, f0, f9
	LFDU	f9,    1 * SIZE(Y)
	FMADD	f18,  ALPHA_R, f2, f10
	LFDUX	f10,   Y, INCY
	FMADD	f19,  ALPHA_I, f2, f11
	LFDU	f11,   1 * SIZE(Y)

	ADD1	f16,  ALPHA_I, f1, f16
	LFDUX	f0,    X, INCX
	ADD2	f17,  ALPHA_R, f1, f17
	LFDU	f1,    1 * SIZE(X)
	ADD1	f18,  ALPHA_I, f3, f18
	LFDUX	f2,    X, INCX
	ADD2	f19,  ALPHA_R, f3, f19
	LFDU	f3,    1 * SIZE(X)

	FMADD	f20,  ALPHA_R, f4, f12
	LFDUX	f12,   Y, INCY
	FMADD	f21,  ALPHA_I, f4, f13
	LFDU	f13,   1 * SIZE(Y)
	FMADD	f22,  ALPHA_R, f6, f14
	LFDUX	f14,   Y, INCY
	FMADD	f23,  ALPHA_I, f6, f15
	LFDU	f15,   1 * SIZE(Y)

	ADD1	f20,  ALPHA_I, f5, f20
	LFDUX	f4,    X, INCX
	ADD2	f21,  ALPHA_R, f5, f21
	LFDU	f5,    1 * SIZE(X)
	ADD1	f22,  ALPHA_I, f7, f22
	LFDUX	f6,    X, INCX
	ADD2	f23,  ALPHA_R, f7, f23
	LFDU	f7,    1 * SIZE(X)

	STFDUX	f16,   YY, INCY
	FMADD	f16,  ALPHA_R, f0, f8
	STFDU	f17,   1 * SIZE(YY)
	FMADD	f17,  ALPHA_I, f0, f9
	STFDUX	f18,   YY, INCY
	FMADD	f18,  ALPHA_R, f2, f10
	STFDU	f19,   1 * SIZE(YY)
	FMADD	f19,  ALPHA_I, f2, f11

	ADD1	f16,  ALPHA_I, f1, f16
	ADD2	f17,  ALPHA_R, f1, f17
	ADD1	f18,  ALPHA_I, f3, f18
	ADD2	f19,  ALPHA_R, f3, f19

	STFDUX	f20,   YY, INCY
	FMADD	f20,  ALPHA_R, f4, f12
	STFDU	f21,   1 * SIZE(YY)
	FMADD	f21,  ALPHA_I, f4, f13
	STFDUX	f22,   YY, INCY
	FMADD	f22,  ALPHA_R, f6, f14
	STFDU	f23,   1 * SIZE(YY)
	FMADD	f23,  ALPHA_I, f6, f15

	ADD1	f20,  ALPHA_I, f5, f20
	STFDUX	f16,   YY, INCY
	ADD2	f21,  ALPHA_R, f5, f21
	STFDU	f17,   1 * SIZE(YY)
	ADD1	f22,  ALPHA_I, f7, f22
	STFDUX	f18,   YY, INCY
	ADD2	f23,  ALPHA_R, f7, f23
	STFDU	f19,   1 * SIZE(YY)

	STFDUX	f20,   YY, INCY
	STFDU	f21,   1 * SIZE(YY)
	STFDUX	f22,   YY, INCY
	STFDU	f23,   1 * SIZE(YY)
	.align 4

LL(150):
	andi.	r0,  N, 7
	mtspr	CTR, r0
	ble	LL(999)
	.align 4

LL(160):
	LFDUX	f0,   X, INCX
	LFDU	f1,   1 * SIZE(X)
	LFDUX	f8,   Y, INCY
	LFDU	f9,   1 * SIZE(Y)

	FMADD	f16,  ALPHA_R, f0, f8
	FMADD	f17,  ALPHA_I, f0, f9

	ADD1	f16,  ALPHA_I, f1, f16
	ADD2	f17,  ALPHA_R, f1, f17

	STFDUX	f16,  YY, INCY
	STFDU	f17,  1 * SIZE(YY)
	bdnz	LL(160)
	.align 4

LL(999):
	lfd	f14,    0(SP)
	lfd	f15,    8(SP)
	lfd	f16,   16(SP)
	lfd	f17,   24(SP)

	lfd	f18,   32(SP)
	lfd	f19,   40(SP)
	lfd	f20,   48(SP)
	lfd	f21,   56(SP)

	lfd	f22,   64(SP)
	lfd	f23,   72(SP)
	lfd	f24,   80(SP)
	lfd	f25,   88(SP)

	addi	SP, SP, STACKSIZE
	li	r0, 0
	blr
	EPILOGUE
