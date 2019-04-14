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
#define INCXM1	r4
#define INCYM1	r5
#define PREA	r10
#define	YY	r11
#else
#define N	r3
#define X	r8
#define INCX	r9
#define Y	r10
#define INCY	r4
#define INCXM1	r5
#define INCYM1	r6
#define PREA	r7
#define	YY	r11
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
#define N	r3
#define X	r10
#define INCX	r4
#define Y	r5
#define INCY	r6
#define INCXM1	r7
#define INCYM1	r8
#define PREA	r9
#define	YY	r11
#else
#define N	r3
#define X	r8
#define INCX	r9
#define Y	r10
#define INCY	r4
#define INCXM1	r5
#define INCYM1	r6
#define PREA	r7
#define	YY	r11
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

#ifndef NEEDPARAM

#define STACKSIZE 96

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
	fmr	ALPHA_I, f2

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

	LFD	f0,    0 * SIZE(X)
	LFD	f1,    1 * SIZE(X)
	LFD	f2,    2 * SIZE(X)
	LFD	f3,    3 * SIZE(X)

	LFD	f8,    0 * SIZE(Y)
	LFD	f9,    1 * SIZE(Y)
	LFD	f10,   2 * SIZE(Y)
	LFD	f11,   3 * SIZE(Y)

	LFD	f4,    4 * SIZE(X)
	LFD	f5,    5 * SIZE(X)
	LFD	f6,    6 * SIZE(X)
	LFD	f7,    7 * SIZE(X)

	LFD	f12,   4 * SIZE(Y)
	LFD	f13,   5 * SIZE(Y)
	LFD	f14,   6 * SIZE(Y)
	LFD	f15,   7 * SIZE(Y)
	bdz	LL(20)
	.align 4

LL(10):
	FMADD	f16,  ALPHA_R, f0, f8
	FMADD	f17,  ALPHA_I, f0, f9
	FMADD	f18,  ALPHA_R, f2, f10
	FMADD	f19,  ALPHA_I, f2, f11

	ADD1	f16,  ALPHA_I, f1, f16
	ADD2	f17,  ALPHA_R, f1, f17
	ADD1	f18,  ALPHA_I, f3, f18
	ADD2	f19,  ALPHA_R, f3, f19

	LFD	f0,    8 * SIZE(X)
	LFD	f1,    9 * SIZE(X)
	LFD	f2,   10 * SIZE(X)
	LFD	f3,   11 * SIZE(X)

	LFD	f8,    8 * SIZE(Y)
	LFD	f9,    9 * SIZE(Y)
	LFD	f10,  10 * SIZE(Y)
	LFD	f11,  11 * SIZE(Y)

	STFD	f16,   0 * SIZE(Y)
	STFD	f17,   1 * SIZE(Y)
	STFD	f18,   2 * SIZE(Y)
	STFD	f19,   3 * SIZE(Y)

	FMADD	f20,  ALPHA_R, f4, f12
	FMADD	f21,  ALPHA_I, f4, f13
	FMADD	f22,  ALPHA_R, f6, f14
	FMADD	f23,  ALPHA_I, f6, f15

	ADD1	f20,  ALPHA_I, f5, f20
	ADD2	f21,  ALPHA_R, f5, f21
	ADD1	f22,  ALPHA_I, f7, f22
	ADD2	f23,  ALPHA_R, f7, f23

	LFD	f4,   12 * SIZE(X)
	LFD	f5,   13 * SIZE(X)
	LFD	f6,   14 * SIZE(X)
	LFD	f7,   15 * SIZE(X)

	LFD	f12,  12 * SIZE(Y)
	LFD	f13,  13 * SIZE(Y)
	LFD	f14,  14 * SIZE(Y)
	LFD	f15,  15 * SIZE(Y)

	STFD	f20,   4 * SIZE(Y)
	STFD	f21,   5 * SIZE(Y)
	STFD	f22,   6 * SIZE(Y)
	STFD	f23,   7 * SIZE(Y)

	FMADD	f16,  ALPHA_R, f0, f8
	FMADD	f17,  ALPHA_I, f0, f9
	FMADD	f18,  ALPHA_R, f2, f10
	FMADD	f19,  ALPHA_I, f2, f11

	ADD1	f16,  ALPHA_I, f1, f16
	ADD2	f17,  ALPHA_R, f1, f17
	ADD1	f18,  ALPHA_I, f3, f18
	ADD2	f19,  ALPHA_R, f3, f19

	LFD	f0,   16 * SIZE(X)
	LFD	f1,   17 * SIZE(X)
	LFD	f2,   18 * SIZE(X)
	LFD	f3,   19 * SIZE(X)

	LFD	f8,   16 * SIZE(Y)
	LFD	f9,   17 * SIZE(Y)
	LFD	f10,  18 * SIZE(Y)
	LFD	f11,  19 * SIZE(Y)

	STFD	f16,   8 * SIZE(Y)
	STFD	f17,   9 * SIZE(Y)
	STFD	f18,  10 * SIZE(Y)
	STFD	f19,  11 * SIZE(Y)

	FMADD	f20,  ALPHA_R, f4, f12
	FMADD	f21,  ALPHA_I, f4, f13
	FMADD	f22,  ALPHA_R, f6, f14
	FMADD	f23,  ALPHA_I, f6, f15

	ADD1	f20,  ALPHA_I, f5, f20
	ADD2	f21,  ALPHA_R, f5, f21
	ADD1	f22,  ALPHA_I, f7, f22
	ADD2	f23,  ALPHA_R, f7, f23

	LFD	f4,   20 * SIZE(X)
	LFD	f5,   21 * SIZE(X)
	LFD	f6,   22 * SIZE(X)
	LFD	f7,   23 * SIZE(X)

	LFD	f12,  20 * SIZE(Y)
	LFD	f13,  21 * SIZE(Y)
	LFD	f14,  22 * SIZE(Y)
	LFD	f15,  23 * SIZE(Y)

	STFD	f20,  12 * SIZE(Y)
	STFD	f21,  13 * SIZE(Y)
	STFD	f22,  14 * SIZE(Y)
	STFD	f23,  15 * SIZE(Y)

#ifndef POWER6
	dcbtst	Y, PREA
#ifdef	L1_DUALFETCH
	dcbt	X, PREA
#endif
#endif
	addi	X, X, 16 * SIZE
	addi	Y, Y, 16 * SIZE

#ifdef POWER6
	dcbtst	Y, PREA
	L1_PREFETCH	X, PREA
#endif

	bdnz	LL(10)
	.align 4

LL(20):
	FMADD	f16,  ALPHA_R, f0, f8
	FMADD	f17,  ALPHA_I, f0, f9
	FMADD	f18,  ALPHA_R, f2, f10
	FMADD	f19,  ALPHA_I, f2, f11

	ADD1	f16,  ALPHA_I, f1, f16
	ADD2	f17,  ALPHA_R, f1, f17
	ADD1	f18,  ALPHA_I, f3, f18
	ADD2	f19,  ALPHA_R, f3, f19

	LFD	f0,    8 * SIZE(X)
	LFD	f1,    9 * SIZE(X)
	LFD	f2,   10 * SIZE(X)
	LFD	f3,   11 * SIZE(X)

	LFD	f8,    8 * SIZE(Y)
	LFD	f9,    9 * SIZE(Y)
	LFD	f10,  10 * SIZE(Y)
	LFD	f11,  11 * SIZE(Y)

	FMADD	f20,  ALPHA_R, f4, f12
	FMADD	f21,  ALPHA_I, f4, f13
	FMADD	f22,  ALPHA_R, f6, f14
	FMADD	f23,  ALPHA_I, f6, f15

	ADD1	f20,  ALPHA_I, f5, f20
	ADD2	f21,  ALPHA_R, f5, f21
	ADD1	f22,  ALPHA_I, f7, f22
	ADD2	f23,  ALPHA_R, f7, f23

	LFD	f4,   12 * SIZE(X)
	LFD	f5,   13 * SIZE(X)
	LFD	f6,   14 * SIZE(X)
	LFD	f7,   15 * SIZE(X)

	LFD	f12,  12 * SIZE(Y)
	LFD	f13,  13 * SIZE(Y)
	LFD	f14,  14 * SIZE(Y)
	LFD	f15,  15 * SIZE(Y)

	STFD	f16,   0 * SIZE(Y)
	STFD	f17,   1 * SIZE(Y)
	STFD	f18,   2 * SIZE(Y)
	STFD	f19,   3 * SIZE(Y)

	FMADD	f16,  ALPHA_R, f0, f8
	FMADD	f17,  ALPHA_I, f0, f9
	FMADD	f18,  ALPHA_R, f2, f10
	FMADD	f19,  ALPHA_I, f2, f11

	ADD1	f16,  ALPHA_I, f1, f16
	ADD2	f17,  ALPHA_R, f1, f17
	ADD1	f18,  ALPHA_I, f3, f18
	ADD2	f19,  ALPHA_R, f3, f19

	STFD	f20,   4 * SIZE(Y)
	STFD	f21,   5 * SIZE(Y)
	STFD	f22,   6 * SIZE(Y)
	STFD	f23,   7 * SIZE(Y)

	FMADD	f20,  ALPHA_R, f4, f12
	FMADD	f21,  ALPHA_I, f4, f13
	FMADD	f22,  ALPHA_R, f6, f14
	FMADD	f23,  ALPHA_I, f6, f15

	ADD1	f20,  ALPHA_I, f5, f20
	ADD2	f21,  ALPHA_R, f5, f21
	ADD1	f22,  ALPHA_I, f7, f22
	ADD2	f23,  ALPHA_R, f7, f23

	STFD	f16,   8 * SIZE(Y)
	STFD	f17,   9 * SIZE(Y)
	STFD	f18,  10 * SIZE(Y)
	STFD	f19,  11 * SIZE(Y)

	STFD	f20,  12 * SIZE(Y)
	STFD	f21,  13 * SIZE(Y)
	STFD	f22,  14 * SIZE(Y)
	STFD	f23,  15 * SIZE(Y)

	addi	X, X, 16 * SIZE
	addi	Y, Y, 16 * SIZE
	.align 4

LL(50):
	andi.	r0,  N, 7
	mtspr	CTR, r0
	beq	LL(999)
	.align 4

LL(60):
	LFD	f0,   0 * SIZE(X)
	LFD	f1,   1 * SIZE(X)
	LFD	f8,   0 * SIZE(Y)
	LFD	f9,   1 * SIZE(Y)

	FMADD	f16,  ALPHA_R, f0, f8
	FMADD	f17,  ALPHA_I, f0, f9

	ADD1	f16,  ALPHA_I, f1, f16
	ADD2	f17,  ALPHA_R, f1, f17

	STFD	f16,  0 * SIZE(Y)
	STFD	f17,  1 * SIZE(Y)
	addi	X, X,  2 * SIZE
	addi	Y, Y,  2 * SIZE
	bdnz	LL(60)
	b	LL(999)
	.align 4

LL(100):
	sub	X, X, INCXM1
	sub	Y, Y, INCYM1
	mr	YY, Y

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(150)
	.align 4

	LFDX	f0,    X, INCXM1
	LFDUX	f1,    X, INCX
	LFDX	f2,    X, INCXM1
	LFDUX	f3,    X, INCX

	LFDX	f8,    Y, INCYM1
	LFDUX	f9,    Y, INCY
	LFDX	f10,   Y, INCYM1
	LFDUX	f11,   Y, INCY

	LFDX	f4,    X, INCXM1
	LFDUX	f5,    X, INCX
	LFDX	f6,    X, INCXM1
	LFDUX	f7,    X, INCX

	LFDX	f12,   Y, INCYM1
	LFDUX	f13,   Y, INCY
	LFDX	f14,   Y, INCYM1
	LFDUX	f15,   Y, INCY
	bdz	LL(120)
	.align 4

LL(110):
	FMADD	f16,  ALPHA_R, f0, f8
	FMADD	f17,  ALPHA_I, f0, f9
	FMADD	f18,  ALPHA_R, f2, f10
	FMADD	f19,  ALPHA_I, f2, f11

	ADD1	f16,  ALPHA_I, f1, f16
	ADD2	f17,  ALPHA_R, f1, f17
	ADD1	f18,  ALPHA_I, f3, f18
	ADD2	f19,  ALPHA_R, f3, f19

	LFDX	f0,    X, INCXM1
	LFDUX	f1,    X, INCX
	LFDX	f2,    X, INCXM1
	LFDUX	f3,    X, INCX

	LFDX	f8,    Y, INCYM1
	LFDUX	f9,    Y, INCY
	LFDX	f10,   Y, INCYM1
	LFDUX	f11,   Y, INCY

	FMADD	f20,  ALPHA_R, f4, f12
	FMADD	f21,  ALPHA_I, f4, f13
	FMADD	f22,  ALPHA_R, f6, f14
	FMADD	f23,  ALPHA_I, f6, f15

	ADD1	f20,  ALPHA_I, f5, f20
	ADD2	f21,  ALPHA_R, f5, f21
	ADD1	f22,  ALPHA_I, f7, f22
	ADD2	f23,  ALPHA_R, f7, f23

	LFDX	f4,    X, INCXM1
	LFDUX	f5,    X, INCX
	LFDX	f6,    X, INCXM1
	LFDUX	f7,    X, INCX

	LFDX	f12,   Y, INCYM1
	LFDUX	f13,   Y, INCY
	LFDX	f14,   Y, INCYM1
	LFDUX	f15,   Y, INCY

	STFDX	f16,   YY, INCYM1
	STFDUX	f17,   YY, INCY
	STFDX	f18,   YY, INCYM1
	STFDUX	f19,   YY, INCY

	FMADD	f16,  ALPHA_R, f0, f8
	FMADD	f17,  ALPHA_I, f0, f9
	FMADD	f18,  ALPHA_R, f2, f10
	FMADD	f19,  ALPHA_I, f2, f11

	ADD1	f16,  ALPHA_I, f1, f16
	ADD2	f17,  ALPHA_R, f1, f17
	ADD1	f18,  ALPHA_I, f3, f18
	ADD2	f19,  ALPHA_R, f3, f19

	LFDX	f0,    X, INCXM1
	LFDUX	f1,    X, INCX
	LFDX	f2,    X, INCXM1
	LFDUX	f3,    X, INCX

	LFDX	f8,    Y, INCYM1
	LFDUX	f9,    Y, INCY
	LFDX	f10,   Y, INCYM1
	LFDUX	f11,   Y, INCY

	STFDX	f20,   YY, INCYM1
	STFDUX	f21,   YY, INCY
	STFDX	f22,   YY, INCYM1
	STFDUX	f23,   YY, INCY

	FMADD	f20,  ALPHA_R, f4, f12
	FMADD	f21,  ALPHA_I, f4, f13
	FMADD	f22,  ALPHA_R, f6, f14
	FMADD	f23,  ALPHA_I, f6, f15

	ADD1	f20,  ALPHA_I, f5, f20
	ADD2	f21,  ALPHA_R, f5, f21
	ADD1	f22,  ALPHA_I, f7, f22
	ADD2	f23,  ALPHA_R, f7, f23

	LFDX	f4,    X, INCXM1
	LFDUX	f5,    X, INCX
	LFDX	f6,    X, INCXM1
	LFDUX	f7,    X, INCX

	LFDX	f12,   Y, INCYM1
	LFDUX	f13,   Y, INCY
	LFDX	f14,   Y, INCYM1
	LFDUX	f15,   Y, INCY

	STFDX	f16,   YY, INCYM1
	STFDUX	f17,   YY, INCY
	STFDX	f18,   YY, INCYM1
	STFDUX	f19,   YY, INCY

	STFDX	f20,   YY, INCYM1
	STFDUX	f21,   YY, INCY
	STFDX	f22,   YY, INCYM1
	STFDUX	f23,   YY, INCY
	bdnz	LL(110)
	.align 4

LL(120):
	FMADD	f16,  ALPHA_R, f0, f8
	FMADD	f17,  ALPHA_I, f0, f9
	FMADD	f18,  ALPHA_R, f2, f10
	FMADD	f19,  ALPHA_I, f2, f11

	ADD1	f16,  ALPHA_I, f1, f16
	ADD2	f17,  ALPHA_R, f1, f17
	ADD1	f18,  ALPHA_I, f3, f18
	ADD2	f19,  ALPHA_R, f3, f19

	LFDX	f0,    X, INCXM1
	LFDUX	f1,    X, INCX
	LFDX	f2,    X, INCXM1
	LFDUX	f3,    X, INCX

	LFDX	f8,    Y, INCYM1
	LFDUX	f9,    Y, INCY
	LFDX	f10,   Y, INCYM1
	LFDUX	f11,   Y, INCY

	FMADD	f20,  ALPHA_R, f4, f12
	FMADD	f21,  ALPHA_I, f4, f13
	FMADD	f22,  ALPHA_R, f6, f14
	FMADD	f23,  ALPHA_I, f6, f15

	ADD1	f20,  ALPHA_I, f5, f20
	ADD2	f21,  ALPHA_R, f5, f21
	ADD1	f22,  ALPHA_I, f7, f22
	ADD2	f23,  ALPHA_R, f7, f23

	LFDX	f4,    X, INCXM1
	LFDUX	f5,    X, INCX
	LFDX	f6,    X, INCXM1
	LFDUX	f7,    X, INCX

	LFDX	f12,   Y, INCYM1
	LFDUX	f13,   Y, INCY
	LFDX	f14,   Y, INCYM1
	LFDUX	f15,   Y, INCY

	STFDX	f16,   YY, INCYM1
	STFDUX	f17,   YY, INCY
	STFDX	f18,   YY, INCYM1
	STFDUX	f19,   YY, INCY

	FMADD	f16,  ALPHA_R, f0, f8
	FMADD	f17,  ALPHA_I, f0, f9
	FMADD	f18,  ALPHA_R, f2, f10
	FMADD	f19,  ALPHA_I, f2, f11

	ADD1	f16,  ALPHA_I, f1, f16
	ADD2	f17,  ALPHA_R, f1, f17
	ADD1	f18,  ALPHA_I, f3, f18
	ADD2	f19,  ALPHA_R, f3, f19

	STFDX	f20,   YY, INCYM1
	STFDUX	f21,   YY, INCY
	STFDX	f22,   YY, INCYM1
	STFDUX	f23,   YY, INCY

	FMADD	f20,  ALPHA_R, f4, f12
	FMADD	f21,  ALPHA_I, f4, f13
	FMADD	f22,  ALPHA_R, f6, f14
	FMADD	f23,  ALPHA_I, f6, f15

	ADD1	f20,  ALPHA_I, f5, f20
	ADD2	f21,  ALPHA_R, f5, f21
	ADD1	f22,  ALPHA_I, f7, f22
	ADD2	f23,  ALPHA_R, f7, f23

	STFDX	f16,   YY, INCYM1
	STFDUX	f17,   YY, INCY
	STFDX	f18,   YY, INCYM1
	STFDUX	f19,   YY, INCY

	STFDX	f20,   YY, INCYM1
	STFDUX	f21,   YY, INCY
	STFDX	f22,   YY, INCYM1
	STFDUX	f23,   YY, INCY
	.align 4

LL(150):
	andi.	r0,  N, 7
	mtspr	CTR, r0
	beq	LL(999)
	.align 4

LL(160):
	LFDX	f0,   X, INCXM1
	LFDUX	f1,   X, INCX
	LFDX	f8,   Y, INCYM1
	LFDUX	f9,   Y, INCY

	FMADD	f16,  ALPHA_R, f0, f8
	FMADD	f17,  ALPHA_I, f0, f9

	ADD1	f16,  ALPHA_I, f1, f16
	ADD2	f17,  ALPHA_R, f1, f17

	STFDX	f16,  YY, INCYM1
	STFDUX	f17,  YY, INCY
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
	blr

	EPILOGUE

#endif
