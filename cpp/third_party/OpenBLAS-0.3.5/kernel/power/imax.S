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

#define RET	r3
#define X	r4
#define INCX	r5

#define N	r6
#define NN	r7
#define XX	r8
#define PREA	r9

#define FZERO	f1

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

	stw	r0,   144(SP)
	lfs	FZERO,144(SP)

#ifdef F_INTERFACE
	LDINT	N,    0(r3)
	LDINT	INCX, 0(INCX)
#else
	mr	N, r3
#endif

	li	RET, 0
	mr	NN, N
	mr	XX, X

	slwi	INCX, INCX, BASE_SHIFT

	li	PREA, L1_PREFETCHSIZE

	cmpwi	cr0, N, 0
	ble-	LL(9999)
	cmpwi	cr0, INCX, 0
	ble-	LL(9999)

	LFD	f1, 0 * SIZE(X)
	add	X, X, INCX

	fmr	f0, f1
	fmr	f2, f1
	fmr	f3, f1
	fmr	f4, f1
	fmr	f5, f1
	fmr	f6, f1
	fmr	f7, f1

	subi	N, N, 1

	cmpwi	cr0, INCX, SIZE
	bne-	cr0, LL(100)

	srawi.	r0, N, 4
	mtspr	CTR, r0
	beq-	cr0, LL(50)

	LFD	f16,   0 * SIZE(X)
	LFD	f17,   1 * SIZE(X)
	LFD	f18,   2 * SIZE(X)
	LFD	f19,   3 * SIZE(X)
	LFD	f20,   4 * SIZE(X)
	LFD	f21,   5 * SIZE(X)
	LFD	f22,   6 * SIZE(X)
	LFD	f23,   7 * SIZE(X)

	LFD	f24,   8 * SIZE(X)
	LFD	f25,   9 * SIZE(X)
	LFD	f26,  10 * SIZE(X)
	LFD	f27,  11 * SIZE(X)
	LFD	f28,  12 * SIZE(X)
	LFD	f29,  13 * SIZE(X)
	LFD	f30,  14 * SIZE(X)
	LFD	f31,  15 * SIZE(X)

	fsub	f8,  f0,  f16
	fsub	f9,  f1,  f17
	fsub	f10, f2,  f18
	fsub	f11, f3,  f19
	fsub	f12, f4,  f20
	fsub	f13, f5,  f21
	fsub	f14, f6,  f22
	fsub	f15, f7,  f23
	bdz	LL(20)
	.align 4

LL(10):
	fsel	f0,  f8,  f0,  f16
	fsub	f8,  f0,  f24
	fsel	f1,  f9,  f1,  f17
	fsub	f9,  f1,  f25
	fsel	f2,  f10, f2,  f18
	fsub	f10, f2,  f26
	fsel	f3,  f11, f3,  f19
	fsub	f11, f3,  f27

	LFD	f16,  16 * SIZE(X)
	LFD	f17,  17 * SIZE(X)
	LFD	f18,  18 * SIZE(X)
	LFD	f19,  19 * SIZE(X)

	fsel	f4,  f12, f4,  f20
	fsub	f12, f4,  f28
	fsel	f5,  f13, f5,  f21
	fsub	f13, f5,  f29
	fsel	f6,  f14, f6,  f22
	fsub	f14, f6,  f30
	fsel	f7,  f15, f7,  f23
	fsub	f15, f7,  f31

	LFD	f20,  20 * SIZE(X)
	LFD	f21,  21 * SIZE(X)
	LFD	f22,  22 * SIZE(X)
	LFD	f23,  23 * SIZE(X)

	fsel	f0,  f8,  f0,  f24
	fsub	f8,  f0,  f16
	fsel	f1,  f9,  f1,  f25
	fsub	f9,  f1,  f17
	fsel	f2,  f10, f2,  f26
	fsub	f10, f2,  f18
	fsel	f3,  f11, f3,  f27
	fsub	f11, f3,  f19

	LFD	f24,  24 * SIZE(X)
	LFD	f25,  25 * SIZE(X)
	LFD	f26,  26 * SIZE(X)
	LFD	f27,  27 * SIZE(X)

	fsel	f4,  f12, f4,  f28
	fsub	f12, f4,  f20
	fsel	f5,  f13, f5,  f29
	fsub	f13, f5,  f21
	fsel	f6,  f14, f6,  f30
	fsub	f14, f6,  f22
	fsel	f7,  f15, f7,  f31
	fsub	f15, f7,  f23

	LFD	f28,  28 * SIZE(X)
	LFD	f29,  29 * SIZE(X)
	LFD	f30,  30 * SIZE(X)
	LFD	f31,  31 * SIZE(X)

#ifndef POWER6
	L1_PREFETCH	X, PREA
#endif
	addi	X, X, 16 * SIZE
#ifdef POWER6
	L1_PREFETCH	X, PREA
#endif

	bdnz	LL(10)
	.align 4

LL(20):
	fsel	f0,  f8,  f0,  f16
	fsub	f8,  f0,  f24
	fsel	f1,  f9,  f1,  f17
	fsub	f9,  f1,  f25
	fsel	f2,  f10, f2,  f18
	fsub	f10, f2,  f26
	fsel	f3,  f11, f3,  f19
	fsub	f11, f3,  f27

	fsel	f4,  f12, f4,  f20
	fsub	f12, f4,  f28
	fsel	f5,  f13, f5,  f21
	fsub	f13, f5,  f29
	fsel	f6,  f14, f6,  f22
	fsub	f14, f6,  f30
	fsel	f7,  f15, f7,  f23
	fsub	f15, f7,  f31

	fsel	f0,  f8,  f0,  f24
	fsel	f1,  f9,  f1,  f25
	fsel	f2,  f10, f2,  f26
	fsel	f3,  f11, f3,  f27
	fsel	f4,  f12, f4,  f28
	fsel	f5,  f13, f5,  f29
	fsel	f6,  f14, f6,  f30
	fsel	f7,  f15, f7,  f31

	addi	X, X, 16 * SIZE
	.align 4

LL(50):
	andi.	r0,  N, 15
	mtspr	CTR, r0
	beq	LL(999)
	.align 4

LL(60):
	LFD	f8,  0 * SIZE(X)
	addi	X, X,  1 * SIZE
	fsub	f16, f1, f8
	fsel	f1, f16, f1, f8
	bdnz	LL(60)
	b	LL(999)
	.align 4

LL(100):
	sub	X, X, INCX

	srawi.	r0, N, 4
	mtspr	CTR,  r0
	beq-	LL(150)

	LFDUX	f16,   X, INCX
	LFDUX	f17,   X, INCX
	LFDUX	f18,   X, INCX
	LFDUX	f19,   X, INCX
	LFDUX	f20,   X, INCX
	LFDUX	f21,   X, INCX
	LFDUX	f22,   X, INCX
	LFDUX	f23,   X, INCX

	LFDUX	f24,   X, INCX
	LFDUX	f25,   X, INCX
	LFDUX	f26,   X, INCX
	LFDUX	f27,   X, INCX
	LFDUX	f28,   X, INCX
	LFDUX	f29,   X, INCX
	LFDUX	f30,   X, INCX
	LFDUX	f31,   X, INCX

	fsub	f8,  f0,  f16
	fsub	f9,  f1,  f17
	fsub	f10, f2,  f18
	fsub	f11, f3,  f19
	fsub	f12, f4,  f20
	fsub	f13, f5,  f21
	fsub	f14, f6,  f22
	fsub	f15, f7,  f23
	bdz	LL(120)
	.align 4

LL(110):
	fsel	f0,  f8,  f0,  f16
	fsub	f8,  f0,  f24
	fsel	f1,  f9,  f1,  f17
	fsub	f9,  f1,  f25
	fsel	f2,  f10, f2,  f18
	fsub	f10, f2,  f26
	fsel	f3,  f11, f3,  f19
	fsub	f11, f3,  f27

	LFDUX	f16,   X, INCX
	LFDUX	f17,   X, INCX
	LFDUX	f18,   X, INCX
	LFDUX	f19,   X, INCX

	fsel	f4,  f12, f4,  f20
	fsub	f12, f4,  f28
	fsel	f5,  f13, f5,  f21
	fsub	f13, f5,  f29
	fsel	f6,  f14, f6,  f22
	fsub	f14, f6,  f30
	fsel	f7,  f15, f7,  f23
	fsub	f15, f7,  f31

	LFDUX	f20,   X, INCX
	LFDUX	f21,   X, INCX
	LFDUX	f22,   X, INCX
	LFDUX	f23,   X, INCX

	fsel	f0,  f8,  f0,  f24
	fsub	f8,  f0,  f16
	fsel	f1,  f9,  f1,  f25
	fsub	f9,  f1,  f17
	fsel	f2,  f10, f2,  f26
	fsub	f10, f2,  f18
	fsel	f3,  f11, f3,  f27
	fsub	f11, f3,  f19

	LFDUX	f24,   X, INCX
	LFDUX	f25,   X, INCX
	LFDUX	f26,   X, INCX
	LFDUX	f27,   X, INCX

	fsel	f4,  f12, f4,  f28
	fsub	f12, f4,  f20
	fsel	f5,  f13, f5,  f29
	fsub	f13, f5,  f21
	fsel	f6,  f14, f6,  f30
	fsub	f14, f6,  f22
	fsel	f7,  f15, f7,  f31
	fsub	f15, f7,  f23

	LFDUX	f28,   X, INCX
	LFDUX	f29,   X, INCX
	LFDUX	f30,   X, INCX
	LFDUX	f31,   X, INCX
	bdnz	LL(110)
	.align 4

LL(120):
	fsel	f0,  f8,  f0,  f16
	fsub	f8,  f0,  f24
	fsel	f1,  f9,  f1,  f17
	fsub	f9,  f1,  f25
	fsel	f2,  f10, f2,  f18
	fsub	f10, f2,  f26
	fsel	f3,  f11, f3,  f19
	fsub	f11, f3,  f27

	fsel	f4,  f12, f4,  f20
	fsub	f12, f4,  f28
	fsel	f5,  f13, f5,  f21
	fsub	f13, f5,  f29
	fsel	f6,  f14, f6,  f22
	fsub	f14, f6,  f30
	fsel	f7,  f15, f7,  f23
	fsub	f15, f7,  f31

	fsel	f0,  f8,  f0,  f24
	fsel	f1,  f9,  f1,  f25
	fsel	f2,  f10, f2,  f26
	fsel	f3,  f11, f3,  f27
	fsel	f4,  f12, f4,  f28
	fsel	f5,  f13, f5,  f29
	fsel	f6,  f14, f6,  f30
	fsel	f7,  f15, f7,  f31
	.align 4

LL(150):
	andi.	r0,  N, 15
	mtspr	CTR, r0
	beq	LL(999)
	.align 4

LL(160):
	LFDUX	f8,    X, INCX
	fsub	f16, f1, f8
	fsel	f1, f16, f1, f8
	bdnz	LL(160)
	.align 4

LL(999):
	fsub	f8,  f0,  f1
	fsub	f9,  f2,  f3
	fsub	f10, f4,  f5
	fsub	f11, f6,  f7

	fsel	f0,  f8,  f0,  f1
	fsel	f2,  f9,  f2,  f3
	fsel	f4,  f10, f4,  f5
	fsel	f6,  f11, f6,  f7

	fsub	f8,  f0,  f2
	fsub	f9,  f4,  f6
	fsel	f0,  f8,  f0,  f2
	fsel	f4,  f9,  f4,  f6

	fsub	f8,  f0,  f4
	fsel	f1,  f8,  f0,  f4
	.align 4

LL(1000):
	cmpwi	cr0, INCX, SIZE
	bne-	cr0, LL(1100)

	srawi.	r0, NN, 3
	mtspr	CTR, r0
	beq-	cr0, LL(1050)

	LFD	f8,   0 * SIZE(XX)
	LFD	f9,   1 * SIZE(XX)
	LFD	f10,  2 * SIZE(XX)
	LFD	f11,  3 * SIZE(XX)
	LFD	f12,  4 * SIZE(XX)
	LFD	f13,  5 * SIZE(XX)
	LFD	f14,  6 * SIZE(XX)
	LFD	f15,  7 * SIZE(XX)
	bdz	LL(1020)
	.align 4

LL(1010):
	addi	RET, RET, 1
	fcmpu	cr0, f1, f8
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f9
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f10
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f11
	beq	cr0, LL(9999)

	LFD	f8,    8 * SIZE(XX)
	LFD	f9,    9 * SIZE(XX)
	LFD	f10,  10 * SIZE(XX)
	LFD	f11,  11 * SIZE(XX)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f12
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f13
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f14
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f15
	beq	cr0, LL(9999)

	LFD	f12,  12 * SIZE(XX)
	LFD	f13,  13 * SIZE(XX)
	LFD	f14,  14 * SIZE(XX)
	LFD	f15,  15 * SIZE(XX)

	addi	XX, XX,  8 * SIZE
	bdnz	LL(1010)
	.align 4

LL(1020):
	addi	XX, XX,  8 * SIZE

	addi	RET, RET, 1
	fcmpu	cr0, f1, f8
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f9
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f10
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f11
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f12
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f13
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f14
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f15
	beq	cr0, LL(9999)
	.align 4

LL(1050):
	andi.	r0,  NN, 7
	mtspr	CTR, r0
	beq	LL(9999)
	.align 4

LL(1060):
	LFD	f8,  0 * SIZE(XX)
	addi	XX, XX,  1 * SIZE
	addi	RET, RET, 1
	fcmpu	cr0, f1, f8
	beq	cr0, LL(9999)
	bdnz	LL(1060)
	b	LL(9999)
	.align 4

LL(1100):
	sub	XX, XX, INCX

	srawi.	r0, NN, 3
	mtspr	CTR,  r0
	beq-	LL(1150)

	LFDUX	f8,   XX, INCX
	LFDUX	f9,   XX, INCX
	LFDUX	f10,   XX, INCX
	LFDUX	f11,   XX, INCX
	LFDUX	f12,   XX, INCX
	LFDUX	f13,   XX, INCX
	LFDUX	f14,   XX, INCX
	LFDUX	f15,   XX, INCX
	bdz	LL(1120)
	.align 4

LL(1110):
	addi	RET, RET, 1
	fcmpu	cr0, f1, f8
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f9
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f10
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f11
	beq	cr0, LL(9999)

	LFDUX	f8,   XX, INCX
	LFDUX	f9,   XX, INCX
	LFDUX	f10,  XX, INCX
	LFDUX	f11,  XX, INCX

	addi	RET, RET, 1
	fcmpu	cr0, f1, f12
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f13
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f14
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f15
	beq	cr0, LL(9999)

	LFDUX	f12,  XX, INCX
	LFDUX	f13,  XX, INCX
	LFDUX	f14,  XX, INCX
	LFDUX	f15,  XX, INCX

	bdnz	LL(1110)
	.align 4

LL(1120):
	addi	RET, RET, 1
	fcmpu	cr0, f1, f8
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f9
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f10
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f11
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f12
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f13
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f14
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f15
	beq	cr0, LL(9999)
	.align 4

LL(1150):
	andi.	r0,  NN, 7
	mtspr	CTR, r0
	beq	LL(9999)
	.align 4

LL(1160):
	LFDUX	f8,    XX, INCX
	addi	RET, RET, 1
	fcmpu	cr0, f1, f8
	beq	cr0, LL(9999)
	bdnz	LL(1160)
	.align 4

LL(9999):
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
