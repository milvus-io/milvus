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
#define PREA	r4
#define	XX	r10
#define	YY	r11
#else
#define N	r3
#define X	r7
#define INCX	r8
#define Y	r9
#define INCY	r10
#define PREA	r4
#define	XX	r5
#define	YY	r6
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
#define N	r3
#define X	r8
#define INCX	r9
#define Y	r10
#define INCY	r4
#define PREA	r5
#define	XX	r6
#define	YY	r11
#else
#define N	r3
#define X	r7
#define INCX	r8
#define Y	r9
#define INCY	r10
#define PREA	r4
#define	XX	r5
#define	YY	r6
#endif
#endif

#define STACKSIZE 160

	PROLOGUE
	PROFCODE

	addi	SP, SP, -STACKSIZE
	li	r0,   0

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

	stfd	f26,   96(SP)
	stfd	f27,  104(SP)
	stfd	f28,  112(SP)
	stfd	f29,  120(SP)

	stfd	f30,  128(SP)
	stfd	f31,  136(SP)

#if (defined(_AIX) || defined(__APPLE__)) && !defined(__64BIT__) && defined(DOUBLE)
	lwz	INCY, FRAMESLOT(0) + STACKSIZE(SP)
#endif

	slwi	INCX, INCX, BASE_SHIFT
	slwi	INCY, INCY, BASE_SHIFT

#ifdef L1_DUALFETCH
	li	PREA, (L1_PREFETCHSIZE) / 2
#else
	li	PREA, (L1_PREFETCHSIZE)
#endif

	cmpwi	cr0, N, 0
	ble-	LL(999)

	cmpwi	cr0, INCX, SIZE
	bne-	cr0, LL(100)
	cmpwi	cr0, INCY, SIZE
	bne-	cr0, LL(100)

	srawi.	r0, N, 4
	mtspr	CTR, r0
	beq-	cr0, LL(50)
	.align 4

LL(10):
	LFD	f0,    0 * SIZE(X)
	LFD	f1,    1 * SIZE(X)
	LFD	f2,    2 * SIZE(X)
	LFD	f3,    3 * SIZE(X)

	LFD	f16,   0 * SIZE(Y)
	LFD	f17,   1 * SIZE(Y)
	LFD	f18,   2 * SIZE(Y)
	LFD	f19,   3 * SIZE(Y)

	LFD	f4,    4 * SIZE(X)
	LFD	f5,    5 * SIZE(X)
	LFD	f6,    6 * SIZE(X)
	LFD	f7,    7 * SIZE(X)

	LFD	f20,   4 * SIZE(Y)
	LFD	f21,   5 * SIZE(Y)
	LFD	f22,   6 * SIZE(Y)
	LFD	f23,   7 * SIZE(Y)

	LFD	f8,    8 * SIZE(X)
	LFD	f9,    9 * SIZE(X)
	LFD	f10,  10 * SIZE(X)
	LFD	f11,  11 * SIZE(X)

	LFD	f24,   8 * SIZE(Y)
	LFD	f25,   9 * SIZE(Y)
	LFD	f26,  10 * SIZE(Y)
	LFD	f27,  11 * SIZE(Y)

	LFD	f12,  12 * SIZE(X)
	LFD	f13,  13 * SIZE(X)
	LFD	f14,  14 * SIZE(X)
	LFD	f15,  15 * SIZE(X)

	LFD	f28,  12 * SIZE(Y)
	LFD	f29,  13 * SIZE(Y)
	LFD	f30,  14 * SIZE(Y)
	LFD	f31,  15 * SIZE(Y)

	STFD	f16,   0 * SIZE(X)
	STFD	f17,   1 * SIZE(X)
	STFD	f18,   2 * SIZE(X)
	STFD	f19,   3 * SIZE(X)

	STFD	f0,    0 * SIZE(Y)
	STFD	f1,    1 * SIZE(Y)
	STFD	f2,    2 * SIZE(Y)
	STFD	f3,    3 * SIZE(Y)

	STFD	f20,   4 * SIZE(X)
	STFD	f21,   5 * SIZE(X)
	STFD	f22,   6 * SIZE(X)
	STFD	f23,   7 * SIZE(X)

	STFD	f4,    4 * SIZE(Y)
	STFD	f5,    5 * SIZE(Y)
	STFD	f6,    6 * SIZE(Y)
	STFD	f7,    7 * SIZE(Y)

	STFD	f24,   8 * SIZE(X)
	STFD	f25,   9 * SIZE(X)
	STFD	f26,  10 * SIZE(X)
	STFD	f27,  11 * SIZE(X)

	STFD	f8,    8 * SIZE(Y)
	STFD	f9,    9 * SIZE(Y)
	STFD	f10,  10 * SIZE(Y)
	STFD	f11,  11 * SIZE(Y)

	STFD	f28,  12 * SIZE(X)
	STFD	f29,  13 * SIZE(X)
	STFD	f30,  14 * SIZE(X)
	STFD	f31,  15 * SIZE(X)

	STFD	f12,  12 * SIZE(Y)
	STFD	f13,  13 * SIZE(Y)
	STFD	f14,  14 * SIZE(Y)
	STFD	f15,  15 * SIZE(Y)

	addi	X, X, 16 * SIZE
	addi	Y, Y, 16 * SIZE
	dcbtst	X, PREA
#ifdef	L1_DUALFETCH
	dcbtst	Y, PREA
#endif
	bdnz	LL(10)
	.align 4

LL(50):
	andi.	r0,  N, 15
	mtspr	CTR, r0
	beq	LL(999)
	.align 4

LL(60):
	LFD	f8,  0 * SIZE(X)
	LFD	f9,  0 * SIZE(Y)

	STFD	f9,  0 * SIZE(X)
	STFD	f8,  0 * SIZE(Y)

	addi	X, X,  1 * SIZE
	addi	Y, Y,  1 * SIZE
	bdnz	LL(60)
	b	LL(999)
	.align 4

LL(100):
	sub	X, X, INCX
	sub	Y, Y, INCY

	mr	XX, X
	mr	YY, Y

	srawi.	r0, N, 4
	mtspr	CTR,  r0
	beq-	LL(150)
	.align 4

LL(110):
	LFDUX	f0,    X, INCX
	LFDUX	f1,    X, INCX
	LFDUX	f2,    X, INCX
	LFDUX	f3,    X, INCX

	LFDUX	f16,   Y, INCY
	LFDUX	f17,   Y, INCY
	LFDUX	f18,   Y, INCY
	LFDUX	f19,   Y, INCY

	LFDUX	f4,    X, INCX
	LFDUX	f5,    X, INCX
	LFDUX	f6,    X, INCX
	LFDUX	f7,    X, INCX

	LFDUX	f20,   Y, INCY
	LFDUX	f21,   Y, INCY
	LFDUX	f22,   Y, INCY
	LFDUX	f23,   Y, INCY

	LFDUX	f8,    X, INCX
	LFDUX	f9,    X, INCX
	LFDUX	f10,   X, INCX
	LFDUX	f11,   X, INCX

	LFDUX	f24,   Y, INCY
	LFDUX	f25,   Y, INCY
	LFDUX	f26,   Y, INCY
	LFDUX	f27,   Y, INCY

	LFDUX	f12,   X, INCX
	LFDUX	f13,   X, INCX
	LFDUX	f14,   X, INCX
	LFDUX	f15,   X, INCX

	LFDUX	f28,   Y, INCY
	LFDUX	f29,   Y, INCY
	LFDUX	f30,   Y, INCY
	LFDUX	f31,   Y, INCY

	STFDUX	f16,   XX, INCX
	STFDUX	f17,   XX, INCX
	STFDUX	f18,   XX, INCX
	STFDUX	f19,   XX, INCX

	STFDUX	f0,    YY, INCY
	STFDUX	f1,    YY, INCY
	STFDUX	f2,    YY, INCY
	STFDUX	f3,    YY, INCY

	STFDUX	f20,   XX, INCX
	STFDUX	f21,   XX, INCX
	STFDUX	f22,   XX, INCX
	STFDUX	f23,   XX, INCX

	STFDUX	f4,    YY, INCY
	STFDUX	f5,    YY, INCY
	STFDUX	f6,    YY, INCY
	STFDUX	f7,    YY, INCY

	STFDUX	f24,   XX, INCX
	STFDUX	f25,   XX, INCX
	STFDUX	f26,   XX, INCX
	STFDUX	f27,   XX, INCX

	STFDUX	f8,    YY, INCY
	STFDUX	f9,    YY, INCY
	STFDUX	f10,   YY, INCY
	STFDUX	f11,   YY, INCY

	STFDUX	f28,   XX, INCX
	STFDUX	f29,   XX, INCX
	STFDUX	f30,   XX, INCX
	STFDUX	f31,   XX, INCX

	STFDUX	f12,   YY, INCY
	STFDUX	f13,   YY, INCY
	STFDUX	f14,   YY, INCY
	STFDUX	f15,   YY, INCY
	bdnz	LL(110)
	.align 4

LL(150):
	andi.	r0,  N, 15
	mtspr	CTR, r0
	beq	LL(999)
	.align 4

LL(160):
	LFDUX	f8,    X,  INCX
	LFDUX	f9,    Y,  INCY
	STFDUX	f9,    XX, INCX
	STFDUX	f8,    YY, INCY
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

	lfd	f26,   96(SP)
	lfd	f27,  104(SP)
	lfd	f28,  112(SP)
	lfd	f29,  120(SP)

	lfd	f30,  128(SP)
	lfd	f31,  136(SP)
	addi	SP, SP, STACKSIZE
	blr

	EPILOGUE
