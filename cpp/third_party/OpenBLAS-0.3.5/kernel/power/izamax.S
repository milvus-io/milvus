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
#define	INCXM1	r10

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

	slwi	INCX, INCX, ZBASE_SHIFT
	subi	INCXM1, INCX, SIZE

	li	PREA, L1_PREFETCHSIZE

	cmpwi	cr0, N, 0
	ble-	LL(9999)
	cmpwi	cr0, INCX, 0
	ble-	LL(9999)

	LFD	f1, 0 * SIZE(X)
	LFD	f2, 1 * SIZE(X)
	add	X, X, INCX

	fabs	f1, f1
	fabs	f2, f2
	fadd	f1, f1, f2

	fmr	f0, f1
	fmr	f2, f1
	fmr	f3, f1

	subi	N, N, 1

	cmpwi	cr0, INCX, 2 * SIZE
	bne-	cr0, LL(100)

	srawi.	r0, N, 3
	mtspr	CTR, r0
	beq-	cr0, LL(50)
	.align 4

	LFD	f24,   0 * SIZE(X)
	LFD	f25,   1 * SIZE(X)
	LFD	f26,   2 * SIZE(X)
	LFD	f27,   3 * SIZE(X)
	LFD	f28,   4 * SIZE(X)
	LFD	f29,   5 * SIZE(X)
	LFD	f30,   6 * SIZE(X)
	LFD	f31,   7 * SIZE(X)

	fabs	f8,  f24
	fabs	f9,  f25
	fabs	f10, f26
	fabs	f11, f27
	fabs	f12, f28
	fabs	f13, f29
	fabs	f14, f30
	fabs	f15, f31

	LFD	f24,   8 * SIZE(X)
	LFD	f25,   9 * SIZE(X)
	LFD	f26,  10 * SIZE(X)
	LFD	f27,  11 * SIZE(X)

	LFD	f28,  12 * SIZE(X)
	LFD	f29,  13 * SIZE(X)
	LFD	f30,  14 * SIZE(X)
	LFD	f31,  15 * SIZE(X)
	bdz	LL(20)
	.align 4

LL(10):
	fadd	f4,  f8,  f9
	fadd	f5,  f10, f11
	fadd	f6,  f12, f13
	fadd	f7,  f14, f15

	fabs	f8,  f24
	fabs	f9,  f25
	fabs	f10, f26
	fabs	f11, f27

	LFD	f24,  16 * SIZE(X)
	LFD	f25,  17 * SIZE(X)
	LFD	f26,  18 * SIZE(X)
	LFD	f27,  19 * SIZE(X)

	fabs	f12, f28
	fabs	f13, f29
	fabs	f14, f30
	fabs	f15, f31

	LFD	f28,  20 * SIZE(X)
	LFD	f29,  21 * SIZE(X)
	LFD	f30,  22 * SIZE(X)
	LFD	f31,  23 * SIZE(X)

	fsub	f16, f0,  f4
	fsub	f17, f1,  f5
	fsub	f18, f2,  f6
	fsub	f19, f3,  f7

	fadd	f20, f8,  f9
	fadd	f21, f10, f11
	fadd	f22, f12, f13
	fadd	f23, f14, f15

	fabs	f8,  f24
	fabs	f9,  f25
	fabs	f10, f26
	fabs	f11, f27

	LFD	f24,  24 * SIZE(X)
	LFD	f25,  25 * SIZE(X)
	LFD	f26,  26 * SIZE(X)
	LFD	f27,  27 * SIZE(X)

	fsel	f0,  f16, f0,  f4
	fsel	f1,  f17, f1,  f5
	fsel	f2,  f18, f2,  f6
	fsel	f3,  f19, f3,  f7

	fabs	f12, f28
	fabs	f13, f29
	fabs	f14, f30
	fabs	f15, f31

	LFD	f28,  28 * SIZE(X)
	LFD	f29,  29 * SIZE(X)
	LFD	f30,  30 * SIZE(X)
	LFD	f31,  31 * SIZE(X)

	fsub	f16, f0,  f20
	fsub	f17, f1,  f21
	fsub	f18, f2,  f22
	fsub	f19, f3,  f23

	fsel	f0,  f16, f0,  f20
	fsel	f1,  f17, f1,  f21
	fsel	f2,  f18, f2,  f22
	fsel	f3,  f19, f3,  f23

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
	fadd	f4,  f8,  f9
	fadd	f5,  f10, f11
	fadd	f6,  f12, f13
	fadd	f7,  f14, f15

	fabs	f8,  f24
	fabs	f9,  f25
	fabs	f10, f26
	fabs	f11, f27

	fabs	f12, f28
	fabs	f13, f29
	fabs	f14, f30
	fabs	f15, f31

	fsub	f16, f0,  f4
	fsub	f17, f1,  f5
	fsub	f18, f2,  f6
	fsub	f19, f3,  f7

	fadd	f20, f8,  f9
	fadd	f21, f10, f11
	fadd	f22, f12, f13
	fadd	f23, f14, f15

	fsel	f0,  f16, f0,  f4
	fsel	f1,  f17, f1,  f5
	fsel	f2,  f18, f2,  f6
	fsel	f3,  f19, f3,  f7

	fsub	f16, f0,  f20
	fsub	f17, f1,  f21
	fsub	f18, f2,  f22
	fsub	f19, f3,  f23

	fsel	f0,  f16, f0,  f20
	fsel	f1,  f17, f1,  f21
	fsel	f2,  f18, f2,  f22
	fsel	f3,  f19, f3,  f23

	addi	X, X, 16 * SIZE
	.align 4

LL(50):
	andi.	r0,  N, 7
	mtspr	CTR, r0
	beq	LL(999)
	.align 4

LL(60):
	LFD	f8,  0 * SIZE(X)
	LFD	f9,  1 * SIZE(X)
	addi	X, X,  2 * SIZE

	fabs	f8, f8
	fabs	f9, f9
	fadd	f8, f8, f9
	fsub	f16, f1, f8
	fsel	f1, f16, f1, f8
	bdnz	LL(60)
	b	LL(999)
	.align 4

LL(100):
	sub	X, X, INCXM1

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(150)

	LFDX	f24,   X, INCXM1
	LFDUX	f25,   X, INCX
	LFDX	f26,   X, INCXM1
	LFDUX	f27,   X, INCX
	LFDX	f28,   X, INCXM1
	LFDUX	f29,   X, INCX
	LFDX	f30,   X, INCXM1
	LFDUX	f31,   X, INCX

	fabs	f8,  f24
	fabs	f9,  f25
	fabs	f10, f26
	fabs	f11, f27
	fabs	f12, f28
	fabs	f13, f29
	fabs	f14, f30
	fabs	f15, f31

	LFDX	f24,   X, INCXM1
	LFDUX	f25,   X, INCX
	LFDX	f26,   X, INCXM1
	LFDUX	f27,   X, INCX
	LFDX	f28,   X, INCXM1
	LFDUX	f29,   X, INCX
	LFDX	f30,   X, INCXM1
	LFDUX	f31,   X, INCX

	bdz	LL(120)
	.align 4

LL(110):
	fadd	f4,  f8,  f9
	fadd	f5,  f10, f11
	fadd	f6,  f12, f13
	fadd	f7,  f14, f15

	fabs	f8,  f24
	fabs	f9,  f25
	fabs	f10, f26
	fabs	f11, f27

	LFDX	f24,   X, INCXM1
	LFDUX	f25,   X, INCX
	LFDX	f26,   X, INCXM1
	LFDUX	f27,   X, INCX

	fabs	f12, f28
	fabs	f13, f29
	fabs	f14, f30
	fabs	f15, f31

	LFDX	f28,   X, INCXM1
	LFDUX	f29,   X, INCX
	LFDX	f30,   X, INCXM1
	LFDUX	f31,   X, INCX

	fsub	f16, f0,  f4
	fsub	f17, f1,  f5
	fsub	f18, f2,  f6
	fsub	f19, f3,  f7

	fadd	f20, f8,  f9
	fadd	f21, f10, f11
	fadd	f22, f12, f13
	fadd	f23, f14, f15

	fabs	f8,  f24
	fabs	f9,  f25
	fabs	f10, f26
	fabs	f11, f27

	LFDX	f24,   X, INCXM1
	LFDUX	f25,   X, INCX
	LFDX	f26,   X, INCXM1
	LFDUX	f27,   X, INCX

	fsel	f0,  f16, f0,  f4
	fsel	f1,  f17, f1,  f5
	fsel	f2,  f18, f2,  f6
	fsel	f3,  f19, f3,  f7

	fabs	f12, f28
	fabs	f13, f29
	fabs	f14, f30
	fabs	f15, f31

	LFDX	f28,   X, INCXM1
	LFDUX	f29,   X, INCX
	LFDX	f30,   X, INCXM1
	LFDUX	f31,   X, INCX

	fsub	f16, f0,  f20
	fsub	f17, f1,  f21
	fsub	f18, f2,  f22
	fsub	f19, f3,  f23

	fsel	f0,  f16, f0,  f20
	fsel	f1,  f17, f1,  f21
	fsel	f2,  f18, f2,  f22
	fsel	f3,  f19, f3,  f23
	bdnz	LL(110)
	.align 4

LL(120):
	fadd	f4,  f8,  f9
	fadd	f5,  f10, f11
	fadd	f6,  f12, f13
	fadd	f7,  f14, f15

	fabs	f8,  f24
	fabs	f9,  f25
	fabs	f10, f26
	fabs	f11, f27

	fabs	f12, f28
	fabs	f13, f29
	fabs	f14, f30
	fabs	f15, f31

	fsub	f16, f0,  f4
	fsub	f17, f1,  f5
	fsub	f18, f2,  f6
	fsub	f19, f3,  f7

	fadd	f20, f8,  f9
	fadd	f21, f10, f11
	fadd	f22, f12, f13
	fadd	f23, f14, f15

	fsel	f0,  f16, f0,  f4
	fsel	f1,  f17, f1,  f5
	fsel	f2,  f18, f2,  f6
	fsel	f3,  f19, f3,  f7

	fsub	f16, f0,  f20
	fsub	f17, f1,  f21
	fsub	f18, f2,  f22
	fsub	f19, f3,  f23

	fsel	f0,  f16, f0,  f20
	fsel	f1,  f17, f1,  f21
	fsel	f2,  f18, f2,  f22
	fsel	f3,  f19, f3,  f23
	.align 4

LL(150):
	andi.	r0,  N, 7
	mtspr	CTR, r0
	beq	LL(999)
	.align 4

LL(160):
	LFDX	f8,    X, INCXM1
	LFDUX	f9,    X, INCX

	fabs	f8, f8
	fabs	f9, f9
	fadd	f8, f8, f9
	fsub	f16, f1, f8
	fsel	f1, f16, f1, f8
	bdnz	LL(160)
	.align 4

LL(999):
	fsub	f8,  f0,  f1
	fsub	f9,  f2,  f3

	fsel	f0,  f8,  f0,  f1
	fsel	f2,  f9,  f2,  f3
	fsub	f8,  f0,  f2
	fsel	f1,  f8,  f0,  f2
	.align 4


LL(1000):
	cmpwi	cr0, INCX, SIZE * 2
	bne-	cr0, LL(1100)

	srawi.	r0, NN, 3
	mtspr	CTR, r0
	beq-	cr0, LL(1050)

	LFD	f24,   0 * SIZE(XX)
	LFD	f25,   1 * SIZE(XX)
	LFD	f26,   2 * SIZE(XX)
	LFD	f27,   3 * SIZE(XX)
	LFD	f28,   4 * SIZE(XX)
	LFD	f29,   5 * SIZE(XX)
	LFD	f30,   6 * SIZE(XX)
	LFD	f31,   7 * SIZE(XX)
	bdz	LL(1020)
	.align 4

LL(1010):
	fabs	f8,  f24
	fabs	f9,  f25
	fabs	f10, f26
	fabs	f11, f27

	LFD	f24,   8 * SIZE(XX)
	LFD	f25,   9 * SIZE(XX)
	LFD	f26,  10 * SIZE(XX)
	LFD	f27,  11 * SIZE(XX)

	fabs	f12, f28
	fabs	f13, f29
	fabs	f14, f30
	fabs	f15, f31

	LFD	f28,  12 * SIZE(XX)
	LFD	f29,  13 * SIZE(XX)
	LFD	f30,  14 * SIZE(XX)
	LFD	f31,  15 * SIZE(XX)

	fadd	f4, f8,  f9
	fadd	f5, f10, f11
	fadd	f6, f12, f13
	fadd	f7, f14, f15

	addi	RET, RET, 1
	fcmpu	cr0, f1, f4
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f5
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f6
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f7
	beq	cr0, LL(9999)


	fabs	f8,  f24
	fabs	f9,  f25
	fabs	f10, f26
	fabs	f11, f27

	LFD	f24,  16 * SIZE(XX)
	LFD	f25,  17 * SIZE(XX)
	LFD	f26,  18 * SIZE(XX)
	LFD	f27,  19 * SIZE(XX)

	fabs	f12, f28
	fabs	f13, f29
	fabs	f14, f30
	fabs	f15, f31

	LFD	f28,  20 * SIZE(XX)
	LFD	f29,  21 * SIZE(XX)
	LFD	f30,  22 * SIZE(XX)
	LFD	f31,  23 * SIZE(XX)

	fadd	f4, f8,  f9
	fadd	f5, f10, f11
	fadd	f6, f12, f13
	fadd	f7, f14, f15

	addi	RET, RET, 1
	fcmpu	cr0, f1, f4
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f5
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f6
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f7
	beq	cr0, LL(9999)

	addi	XX, XX, 16 * SIZE
	bdnz	LL(1010)
	.align 4

LL(1020):
	fabs	f8,  f24
	fabs	f9,  f25
	fabs	f10, f26
	fabs	f11, f27

	LFD	f24,   8 * SIZE(XX)
	LFD	f25,   9 * SIZE(XX)
	LFD	f26,  10 * SIZE(XX)
	LFD	f27,  11 * SIZE(XX)

	fabs	f12, f28
	fabs	f13, f29
	fabs	f14, f30
	fabs	f15, f31

	LFD	f28,  12 * SIZE(XX)
	LFD	f29,  13 * SIZE(XX)
	LFD	f30,  14 * SIZE(XX)
	LFD	f31,  15 * SIZE(XX)

	fadd	f4, f8,  f9
	fadd	f5, f10, f11
	fadd	f6, f12, f13
	fadd	f7, f14, f15

	addi	RET, RET, 1
	fcmpu	cr0, f1, f4
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f5
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f6
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f7
	beq	cr0, LL(9999)


	fabs	f8,  f24
	fabs	f9,  f25
	fabs	f10, f26
	fabs	f11, f27

	fabs	f12, f28
	fabs	f13, f29
	fabs	f14, f30
	fabs	f15, f31

	fadd	f4, f8,  f9
	fadd	f5, f10, f11
	fadd	f6, f12, f13
	fadd	f7, f14, f15

	addi	RET, RET, 1
	fcmpu	cr0, f1, f4
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f5
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f6
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f7
	beq	cr0, LL(9999)

	addi	XX, XX, 16 * SIZE
	.align 4

LL(1050):
	andi.	r0,  NN, 7
	mtspr	CTR, r0
	beq	LL(9999)
	.align 4

LL(1060):
	LFD	f8,  0 * SIZE(XX)
	LFD	f9,  1 * SIZE(XX)
	addi	XX, XX,  2 * SIZE

	fabs	f8, f8
	fabs	f9, f9
	fadd	f8, f8, f9

	addi	RET, RET, 1
	fcmpu	cr0, f1, f8
	beq	cr0, LL(9999)
	bdnz	LL(1060)
	b	LL(9999)
	.align 4

LL(1100):
	sub	XX, XX, INCXM1

	srawi.	r0, NN, 3
	mtspr	CTR,  r0
	beq-	LL(1150)

	LFDX	f24,   XX, INCXM1
	LFDUX	f25,   XX, INCX
	LFDX	f26,   XX, INCXM1
	LFDUX	f27,   XX, INCX
	LFDX	f28,   XX, INCXM1
	LFDUX	f29,   XX, INCX
	LFDX	f30,   XX, INCXM1
	LFDUX	f31,   XX, INCX
	bdz	LL(1120)
	.align 4

LL(1110):
	fabs	f8,  f24
	fabs	f9,  f25
	fabs	f10, f26
	fabs	f11, f27

	LFDX	f24,   XX, INCXM1
	LFDUX	f25,   XX, INCX
	LFDX	f26,   XX, INCXM1
	LFDUX	f27,   XX, INCX

	fabs	f12, f28
	fabs	f13, f29
	fabs	f14, f30
	fabs	f15, f31

	LFDX	f28,   XX, INCXM1
	LFDUX	f29,   XX, INCX
	LFDX	f30,   XX, INCXM1
	LFDUX	f31,   XX, INCX

	fadd	f4, f8,  f9
	fadd	f5, f10, f11
	fadd	f6, f12, f13
	fadd	f7, f14, f15

	addi	RET, RET, 1
	fcmpu	cr0, f1, f4
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f5
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f6
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f7
	beq	cr0, LL(9999)


	fabs	f8,  f24
	fabs	f9,  f25
	fabs	f10, f26
	fabs	f11, f27

	LFDX	f24,   XX, INCXM1
	LFDUX	f25,   XX, INCX
	LFDX	f26,   XX, INCXM1
	LFDUX	f27,   XX, INCX

	fabs	f12, f28
	fabs	f13, f29
	fabs	f14, f30
	fabs	f15, f31

	LFDX	f28,   XX, INCXM1
	LFDUX	f29,   XX, INCX
	LFDX	f30,   XX, INCXM1
	LFDUX	f31,   XX, INCX

	fadd	f4, f8,  f9
	fadd	f5, f10, f11
	fadd	f6, f12, f13
	fadd	f7, f14, f15

	addi	RET, RET, 1
	fcmpu	cr0, f1, f4
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f5
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f6
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f7
	beq	cr0, LL(9999)

	bdnz	LL(1110)
	.align 4

LL(1120):
	fabs	f8,  f24
	fabs	f9,  f25
	fabs	f10, f26
	fabs	f11, f27

	LFDX	f24,   XX, INCXM1
	LFDUX	f25,   XX, INCX
	LFDX	f26,   XX, INCXM1
	LFDUX	f27,   XX, INCX

	fabs	f12, f28
	fabs	f13, f29
	fabs	f14, f30
	fabs	f15, f31

	LFDX	f28,   XX, INCXM1
	LFDUX	f29,   XX, INCX
	LFDX	f30,   XX, INCXM1
	LFDUX	f31,   XX, INCX

	fadd	f4, f8,  f9
	fadd	f5, f10, f11
	fadd	f6, f12, f13
	fadd	f7, f14, f15

	addi	RET, RET, 1
	fcmpu	cr0, f1, f4
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f5
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f6
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f7
	beq	cr0, LL(9999)

	fabs	f8,  f24
	fabs	f9,  f25
	fabs	f10, f26
	fabs	f11, f27

	fabs	f12, f28
	fabs	f13, f29
	fabs	f14, f30
	fabs	f15, f31

	fadd	f4, f8,  f9
	fadd	f5, f10, f11
	fadd	f6, f12, f13
	fadd	f7, f14, f15

	addi	RET, RET, 1
	fcmpu	cr0, f1, f4
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f5
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f6
	beq	cr0, LL(9999)

	addi	RET, RET, 1
	fcmpu	cr0, f1, f7
	beq	cr0, LL(9999)
	.align 4

LL(1150):
	andi.	r0,  NN, 7
	mtspr	CTR, r0
	beq	LL(9999)
	.align 4

LL(1160):
	LFDX	f8,    XX, INCXM1
	LFDUX	f9,    XX, INCX

	fabs	f8, f8
	fabs	f9, f9
	fadd	f8, f8, f9

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
