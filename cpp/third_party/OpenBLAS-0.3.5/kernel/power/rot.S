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

#define N	r3
#define X	r4
#define INCX	r5
#define Y	r6
#define INCY	r7
#define PREA	r8
#define	XX	r9
#define	YY	r10

#define C	f1
#define S	f2

#define STACKSIZE 32

#ifndef NEEDPARAM

	PROLOGUE
	PROFCODE

	addi	SP, SP, -STACKSIZE
	li	r0,   0

	stfd	f14,    0(SP)
	stfd	f15,    8(SP)
	stfd	f16,   16(SP)
	stfd	f17,   24(SP)

	slwi	INCX, INCX, BASE_SHIFT
	slwi	INCY, INCY, BASE_SHIFT

	li	PREA, L1_PREFETCHSIZE

	cmpwi	cr0, N, 0
	ble-	LL(999)

	cmpwi	cr0, INCX, SIZE
	bne-	cr0, LL(100)
	cmpwi	cr0, INCY, SIZE
	bne-	cr0, LL(100)

	srawi.	r0, N, 4
	mtspr	CTR, r0
	beq-	cr0, LL(50)

	LFD	f0,   0 * SIZE(X)
	LFD	f4,   1 * SIZE(X)
	LFD	f6,   2 * SIZE(X)
	LFD	f8,   3 * SIZE(X)

	LFD	f3,   0 * SIZE(Y)
	LFD	f5,   1 * SIZE(Y)
	LFD	f7,   2 * SIZE(Y)
	LFD	f9,   3 * SIZE(Y)
	bdz	LL(12)
	.align 4

LL(10):
	FMUL	f10, C, f0
	FMUL	f11, C, f3
	FMUL	f12, C, f4
	FMUL	f13, C, f5

	FMUL	f14, C, f6
	FMUL	f15, C, f7
	FMUL	f16, C, f8
	FMUL	f17, C, f9

	FMADD	f10, S, f3,  f10
	FNMSUB	f11, S, f0,  f11
	FMADD	f12, S, f5,  f12
	FNMSUB	f13, S, f4,  f13

	FMADD	f14, S, f7,  f14
	FNMSUB	f15, S, f6,  f15
	FMADD	f16, S, f9,  f16
	FNMSUB	f17, S, f8,  f17

	LFD	f0,   4 * SIZE(X)
	LFD	f4,   5 * SIZE(X)
	LFD	f6,   6 * SIZE(X)
	LFD	f8,   7 * SIZE(X)

	LFD	f3,   4 * SIZE(Y)
	LFD	f5,   5 * SIZE(Y)
	LFD	f7,   6 * SIZE(Y)
	LFD	f9,   7 * SIZE(Y)

	STFD	f10,  0 * SIZE(X)
	STFD	f12,  1 * SIZE(X)
	STFD	f14,  2 * SIZE(X)
	STFD	f16,  3 * SIZE(X)

	STFD	f11,  0 * SIZE(Y)
	STFD	f13,  1 * SIZE(Y)
	STFD	f15,  2 * SIZE(Y)
	STFD	f17,  3 * SIZE(Y)

	FMUL	f10, C, f0
	FMUL	f11, C, f3
	FMUL	f12, C, f4
	FMUL	f13, C, f5

	FMUL	f14, C, f6
	FMUL	f15, C, f7
	FMUL	f16, C, f8
	FMUL	f17, C, f9

	FMADD	f10, S, f3,  f10
	FNMSUB	f11, S, f0,  f11
	FMADD	f12, S, f5,  f12
	FNMSUB	f13, S, f4,  f13

	FMADD	f14, S, f7,  f14
	FNMSUB	f15, S, f6,  f15
	FMADD	f16, S, f9,  f16
	FNMSUB	f17, S, f8,  f17

	LFD	f0,   8 * SIZE(X)
	LFD	f4,   9 * SIZE(X)
	LFD	f6,  10 * SIZE(X)
	LFD	f8,  11 * SIZE(X)

	LFD	f3,   8 * SIZE(Y)
	LFD	f5,   9 * SIZE(Y)
	LFD	f7,  10 * SIZE(Y)
	LFD	f9,  11 * SIZE(Y)

	STFD	f10,  4 * SIZE(X)
	STFD	f12,  5 * SIZE(X)
	STFD	f14,  6 * SIZE(X)
	STFD	f16,  7 * SIZE(X)

	STFD	f11,  4 * SIZE(Y)
	STFD	f13,  5 * SIZE(Y)
	STFD	f15,  6 * SIZE(Y)
	STFD	f17,  7 * SIZE(Y)

	FMUL	f10, C, f0
	FMUL	f11, C, f3
	FMUL	f12, C, f4
	FMUL	f13, C, f5

	FMUL	f14, C, f6
	FMUL	f15, C, f7
	FMUL	f16, C, f8
	FMUL	f17, C, f9

	FMADD	f10, S, f3,  f10
	FNMSUB	f11, S, f0,  f11
	FMADD	f12, S, f5,  f12
	FNMSUB	f13, S, f4,  f13

	FMADD	f14, S, f7,  f14
	FNMSUB	f15, S, f6,  f15
	FMADD	f16, S, f9,  f16
	FNMSUB	f17, S, f8,  f17

	LFD	f0,  12 * SIZE(X)
	LFD	f4,  13 * SIZE(X)
	LFD	f6,  14 * SIZE(X)
	LFD	f8,  15 * SIZE(X)

	LFD	f3,  12 * SIZE(Y)
	LFD	f5,  13 * SIZE(Y)
	LFD	f7,  14 * SIZE(Y)
	LFD	f9,  15 * SIZE(Y)

	STFD	f10,  8 * SIZE(X)
	STFD	f12,  9 * SIZE(X)
	STFD	f14, 10 * SIZE(X)
	STFD	f16, 11 * SIZE(X)

	STFD	f11,  8 * SIZE(Y)
	STFD	f13,  9 * SIZE(Y)
	STFD	f15, 10 * SIZE(Y)
	STFD	f17, 11 * SIZE(Y)

	FMUL	f10, C, f0
	FMUL	f11, C, f3
	FMUL	f12, C, f4
	FMUL	f13, C, f5

	FMUL	f14, C, f6
	FMUL	f15, C, f7
	FMUL	f16, C, f8
	FMUL	f17, C, f9

	FMADD	f10, S, f3,  f10
	FNMSUB	f11, S, f0,  f11
	FMADD	f12, S, f5,  f12
	FNMSUB	f13, S, f4,  f13

	FMADD	f14, S, f7,  f14
	FNMSUB	f15, S, f6,  f15
	FMADD	f16, S, f9,  f16
	FNMSUB	f17, S, f8,  f17

	LFD	f0,  16 * SIZE(X)
	LFD	f4,  17 * SIZE(X)
	LFD	f6,  18 * SIZE(X)
	LFD	f8,  19 * SIZE(X)

	LFD	f3,  16 * SIZE(Y)
	LFD	f5,  17 * SIZE(Y)
	LFD	f7,  18 * SIZE(Y)
	LFD	f9,  19 * SIZE(Y)

	STFD	f10, 12 * SIZE(X)
	STFD	f12, 13 * SIZE(X)
	STFD	f14, 14 * SIZE(X)
	STFD	f16, 15 * SIZE(X)

	STFD	f11, 12 * SIZE(Y)
	STFD	f13, 13 * SIZE(Y)
	STFD	f15, 14 * SIZE(Y)
	STFD	f17, 15 * SIZE(Y)

#ifndef POWER6
	dcbtst	X, PREA
#endif
	addi	X, X, 16 * SIZE
	addi	Y, Y, 16 * SIZE

#ifdef POWER6
	dcbtst	X, PREA
	dcbtst	X, PREA
#endif
	bdnz	LL(10)
	.align 4

LL(12):
	FMUL	f10, C, f0
	FMUL	f11, C, f3
	FMUL	f12, C, f4
	FMUL	f13, C, f5

	FMUL	f14, C, f6
	FMUL	f15, C, f7
	FMUL	f16, C, f8
	FMUL	f17, C, f9

	FMADD	f10, S, f3,  f10
	FNMSUB	f11, S, f0,  f11
	FMADD	f12, S, f5,  f12
	FNMSUB	f13, S, f4,  f13

	FMADD	f14, S, f7,  f14
	FNMSUB	f15, S, f6,  f15
	FMADD	f16, S, f9,  f16
	FNMSUB	f17, S, f8,  f17

	STFD	f10,  0 * SIZE(X)
	STFD	f12,  1 * SIZE(X)
	STFD	f14,  2 * SIZE(X)
	STFD	f16,  3 * SIZE(X)

	STFD	f11,  0 * SIZE(Y)
	STFD	f13,  1 * SIZE(Y)
	STFD	f15,  2 * SIZE(Y)
	STFD	f17,  3 * SIZE(Y)

	LFD	f0,   4 * SIZE(X)
	LFD	f4,   5 * SIZE(X)
	LFD	f6,   6 * SIZE(X)
	LFD	f8,   7 * SIZE(X)

	LFD	f3,   4 * SIZE(Y)
	LFD	f5,   5 * SIZE(Y)
	LFD	f7,   6 * SIZE(Y)
	LFD	f9,   7 * SIZE(Y)

	FMUL	f10, C, f0
	FMUL	f11, C, f3
	FMUL	f12, C, f4
	FMUL	f13, C, f5

	FMUL	f14, C, f6
	FMUL	f15, C, f7
	FMUL	f16, C, f8
	FMUL	f17, C, f9

	FMADD	f10, S, f3,  f10
	FNMSUB	f11, S, f0,  f11
	FMADD	f12, S, f5,  f12
	FNMSUB	f13, S, f4,  f13

	FMADD	f14, S, f7,  f14
	FNMSUB	f15, S, f6,  f15
	FMADD	f16, S, f9,  f16
	FNMSUB	f17, S, f8,  f17

	STFD	f10,  4 * SIZE(X)
	STFD	f12,  5 * SIZE(X)
	STFD	f14,  6 * SIZE(X)
	STFD	f16,  7 * SIZE(X)

	STFD	f11,  4 * SIZE(Y)
	STFD	f13,  5 * SIZE(Y)
	STFD	f15,  6 * SIZE(Y)
	STFD	f17,  7 * SIZE(Y)

	LFD	f0,   8 * SIZE(X)
	LFD	f4,   9 * SIZE(X)
	LFD	f6,  10 * SIZE(X)
	LFD	f8,  11 * SIZE(X)

	LFD	f3,   8 * SIZE(Y)
	LFD	f5,   9 * SIZE(Y)
	LFD	f7,  10 * SIZE(Y)
	LFD	f9,  11 * SIZE(Y)

	FMUL	f10, C, f0
	FMUL	f11, C, f3
	FMUL	f12, C, f4
	FMUL	f13, C, f5

	FMUL	f14, C, f6
	FMUL	f15, C, f7
	FMUL	f16, C, f8
	FMUL	f17, C, f9

	FMADD	f10, S, f3,  f10
	FNMSUB	f11, S, f0,  f11
	FMADD	f12, S, f5,  f12
	FNMSUB	f13, S, f4,  f13

	FMADD	f14, S, f7,  f14
	FNMSUB	f15, S, f6,  f15
	FMADD	f16, S, f9,  f16
	FNMSUB	f17, S, f8,  f17

	STFD	f10,  8 * SIZE(X)
	STFD	f12,  9 * SIZE(X)
	STFD	f14, 10 * SIZE(X)
	STFD	f16, 11 * SIZE(X)

	STFD	f11,  8 * SIZE(Y)
	STFD	f13,  9 * SIZE(Y)
	STFD	f15, 10 * SIZE(Y)
	STFD	f17, 11 * SIZE(Y)

	LFD	f0,  12 * SIZE(X)
	LFD	f4,  13 * SIZE(X)
	LFD	f6,  14 * SIZE(X)
	LFD	f8,  15 * SIZE(X)

	LFD	f3,  12 * SIZE(Y)
	LFD	f5,  13 * SIZE(Y)
	LFD	f7,  14 * SIZE(Y)
	LFD	f9,  15 * SIZE(Y)

	FMUL	f10, C, f0
	FMUL	f11, C, f3
	FMUL	f12, C, f4
	FMUL	f13, C, f5

	FMUL	f14, C, f6
	FMUL	f15, C, f7
	FMUL	f16, C, f8
	FMUL	f17, C, f9

	FMADD	f10, S, f3,  f10
	FNMSUB	f11, S, f0,  f11
	FMADD	f12, S, f5,  f12
	FNMSUB	f13, S, f4,  f13

	FMADD	f14, S, f7,  f14
	FNMSUB	f15, S, f6,  f15
	FMADD	f16, S, f9,  f16
	FNMSUB	f17, S, f8,  f17

	STFD	f10, 12 * SIZE(X)
	STFD	f12, 13 * SIZE(X)
	STFD	f14, 14 * SIZE(X)
	STFD	f16, 15 * SIZE(X)

	STFD	f11, 12 * SIZE(Y)
	STFD	f13, 13 * SIZE(Y)
	STFD	f15, 14 * SIZE(Y)
	STFD	f17, 15 * SIZE(Y)

	addi	X, X, 16 * SIZE
	addi	Y, Y, 16 * SIZE
	.align 4

LL(50):
	andi.	r0,  N, 15
	mtspr	CTR, r0
	beq	LL(999)
	.align 4

LL(60):
	LFD	f3,  0 * SIZE(X)
	LFD	f4,  0 * SIZE(Y)

	FMUL	f10, C, f3
	FMUL	f11, C, f4

	FMADD	f10, S, f4, f10
	FNMSUB	f11, S, f3, f11

	STFD	f10,  0 * SIZE(X)
	STFD	f11,  0 * SIZE(Y)

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

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(150)
	.align 4

LL(110):
	LFDUX	f0,    X,  INCX
	LFDUX	f3,    Y,  INCY
	LFDUX	f4,    X,  INCX
	LFDUX	f5,    Y,  INCY
	LFDUX	f6,    X,  INCX
	LFDUX	f7,    Y,  INCY
	LFDUX	f8,    X,  INCX
	LFDUX	f9,    Y,  INCY

	FMUL	f10, C, f0
	FMUL	f11, C, f3
	FMUL	f12, C, f4
	FMUL	f13, C, f5
	FMUL	f14, C, f6
	FMUL	f15, C, f7
	FMUL	f16, C, f8
	FMUL	f17, C, f9

	FMADD	f10, S, f3, f10
	FNMSUB	f11, S, f0, f11
	FMADD	f12, S, f5, f12
	FNMSUB	f13, S, f4, f13
	FMADD	f14, S, f7, f14
	FNMSUB	f15, S, f6, f15
	FMADD	f16, S, f9, f16
	FNMSUB	f17, S, f8, f17

	STFDUX	f10,    XX, INCX
	STFDUX	f11,    YY, INCY
	STFDUX	f12,    XX, INCX
	STFDUX	f13,    YY, INCY
	STFDUX	f14,    XX, INCX
	STFDUX	f15,    YY, INCY
	STFDUX	f16,    XX, INCX
	STFDUX	f17,    YY, INCY

	LFDUX	f0,    X,  INCX
	LFDUX	f3,    Y,  INCY
	LFDUX	f4,    X,  INCX
	LFDUX	f5,    Y,  INCY
	LFDUX	f6,    X,  INCX
	LFDUX	f7,    Y,  INCY
	LFDUX	f8,    X,  INCX
	LFDUX	f9,    Y,  INCY

	FMUL	f10, C, f0
	FMUL	f11, C, f3
	FMUL	f12, C, f4
	FMUL	f13, C, f5
	FMUL	f14, C, f6
	FMUL	f15, C, f7
	FMUL	f16, C, f8
	FMUL	f17, C, f9

	FMADD	f10, S, f3, f10
	FNMSUB	f11, S, f0, f11
	FMADD	f12, S, f5, f12
	FNMSUB	f13, S, f4, f13
	FMADD	f14, S, f7, f14
	FNMSUB	f15, S, f6, f15
	FMADD	f16, S, f9, f16
	FNMSUB	f17, S, f8, f17

	STFDUX	f10,    XX, INCX
	STFDUX	f11,    YY, INCY
	STFDUX	f12,    XX, INCX
	STFDUX	f13,    YY, INCY
	STFDUX	f14,    XX, INCX
	STFDUX	f15,    YY, INCY
	STFDUX	f16,    XX, INCX
	STFDUX	f17,    YY, INCY

	bdnz	LL(110)
	.align 4

LL(150):
	andi.	r0,  N, 7
	mtspr	CTR, r0
	beq	LL(999)
	.align 4

LL(160):
	LFDUX	f0,    X,  INCX
	LFDUX	f3,    Y,  INCY

	FMUL	f10, C, f0
	FMUL	f11, C, f3

	FMADD	f10, S, f3, f10
	FNMSUB	f11, S, f0, f11

	STFDUX	f10,    XX, INCX
	STFDUX	f11,    YY, INCY
	bdnz	LL(160)
	.align 4

LL(999):
	lfd	f14,    0(SP)
	lfd	f15,    8(SP)
	lfd	f16,   16(SP)
	lfd	f17,   24(SP)

	addi	SP, SP, STACKSIZE
	blr

	EPILOGUE
#endif
