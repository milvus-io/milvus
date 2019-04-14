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
#define	XX	r5
#define	YY	r10
#else
#define N	r3
#define X	r8
#define INCX	r9
#define Y	r10
#define INCY	r4
#define PREA	r5
#define	XX	r6
#define	YY	r7
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
#define N	r3
#define X	r10
#define INCX	r4
#define Y	r5
#define INCY	r6
#define PREA	r7
#define	XX	r8
#define	YY	r9
#else
#define N	r3
#define X	r8
#define INCX	r9
#define Y	r10
#define INCY	r4
#define PREA	r5
#define	XX	r6
#define	YY	r7
#endif
#endif

#define INCXM1	r11
#define INCYM1	r12

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

	slwi	INCX, INCX, ZBASE_SHIFT
	slwi	INCY, INCY, ZBASE_SHIFT
	subi	INCXM1, INCX, SIZE
	subi	INCYM1, INCY, SIZE

#ifdef L1_DUALFETCH
	li	PREA, (L1_PREFETCHSIZE) / 2
#else
	li	PREA, (L1_PREFETCHSIZE)
#endif

	cmpwi	cr0, N, 0
	ble-	LL(999)

	cmpwi	cr0, INCX, 2 * SIZE
	bne-	cr0, LL(100)
	cmpwi	cr0, INCY, 2 * SIZE
	bne-	cr0, LL(100)

	srawi.	r0, N, 3
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
	andi.	r0,  N, 7
	mtspr	CTR, r0
	beq	LL(999)
	.align 4

LL(60):
	LFD	f0,  0 * SIZE(X)
	LFD	f1,  1 * SIZE(X)
	LFD	f2,  0 * SIZE(Y)
	LFD	f3,  1 * SIZE(Y)

	STFD	f2,  0 * SIZE(X)
	STFD	f3,  1 * SIZE(X)
	STFD	f0,  0 * SIZE(Y)
	STFD	f1,  1 * SIZE(Y)

	addi	X, X,  2 * SIZE
	addi	Y, Y,  2 * SIZE
	bdnz	LL(60)
	b	LL(999)
	.align 4

LL(100):
	sub	X, X, INCXM1
	sub	Y, Y, INCYM1

	mr	XX, X
	mr	YY, Y

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(150)
	.align 4

LL(110):
	LFDX	f0,    X, INCXM1
	LFDUX	f1,    X, INCX
	LFDX	f2,    X, INCXM1
	LFDUX	f3,    X, INCX

	LFDX	f16,   Y, INCYM1
	LFDUX	f17,   Y, INCY
	LFDX	f18,   Y, INCYM1
	LFDUX	f19,   Y, INCY

	LFDX	f4,    X, INCXM1
	LFDUX	f5,    X, INCX
	LFDX	f6,    X, INCXM1
	LFDUX	f7,    X, INCX

	LFDX	f20,   Y, INCYM1
	LFDUX	f21,   Y, INCY
	LFDX	f22,   Y, INCYM1
	LFDUX	f23,   Y, INCY

	LFDX	f8,    X, INCXM1
	LFDUX	f9,    X, INCX
	LFDX	f10,   X, INCXM1
	LFDUX	f11,   X, INCX

	LFDX	f24,   Y, INCYM1
	LFDUX	f25,   Y, INCY
	LFDX	f26,   Y, INCYM1
	LFDUX	f27,   Y, INCY

	LFDX	f12,   X, INCXM1
	LFDUX	f13,   X, INCX
	LFDX	f14,   X, INCXM1
	LFDUX	f15,   X, INCX

	LFDX	f28,   Y, INCYM1
	LFDUX	f29,   Y, INCY
	LFDX	f30,   Y, INCYM1
	LFDUX	f31,   Y, INCY

	STFDX	f16,   XX, INCXM1
	STFDUX	f17,   XX, INCX
	STFDX	f18,   XX, INCXM1
	STFDUX	f19,   XX, INCX

	STFDX	f0,    YY, INCYM1
	STFDUX	f1,    YY, INCY
	STFDX	f2,    YY, INCYM1
	STFDUX	f3,    YY, INCY

	STFDX	f20,   XX, INCXM1
	STFDUX	f21,   XX, INCX
	STFDX	f22,   XX, INCXM1
	STFDUX	f23,   XX, INCX

	STFDX	f4,    YY, INCYM1
	STFDUX	f5,    YY, INCY
	STFDX	f6,    YY, INCYM1
	STFDUX	f7,    YY, INCY

	STFDX	f24,   XX, INCXM1
	STFDUX	f25,   XX, INCX
	STFDX	f26,   XX, INCXM1
	STFDUX	f27,   XX, INCX

	STFDX	f8,    YY, INCYM1
	STFDUX	f9,    YY, INCY
	STFDX	f10,   YY, INCYM1
	STFDUX	f11,   YY, INCY

	STFDX	f28,   XX, INCXM1
	STFDUX	f29,   XX, INCX
	STFDX	f30,   XX, INCXM1
	STFDUX	f31,   XX, INCX

	STFDX	f12,   YY, INCYM1
	STFDUX	f13,   YY, INCY
	STFDX	f14,   YY, INCYM1
	STFDUX	f15,   YY, INCY
	bdnz	LL(110)
	.align 4

LL(150):
	andi.	r0,  N, 7
	mtspr	CTR, r0
	beq	LL(999)
	.align 4

LL(160):
	LFDX	f0,    X,  INCXM1
	LFDUX	f1,    X,  INCX
	LFDX	f2,    Y,  INCYM1
	LFDUX	f3,    Y,  INCY
	STFDX	f2,    XX, INCXM1
	STFDUX	f3,    XX, INCX
	STFDX	f0,    YY, INCYM1
	STFDUX	f1,    YY, INCY
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
