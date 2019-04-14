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

#define NN	r6
#define XX	r7
#define PREA	r8

#define FZERO	144(SP)
#define FONE	148(SP)
#define FMAX	152(SP)

#define STACKSIZE 160

	PROLOGUE
	PROFCODE

	addi	SP, SP, -STACKSIZE
	li	r10,   0
	lis	r11,   0x3f80
	lis	r12,   0x5fe0

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

	stw	r10,  FZERO
	stw	r11,  FONE
	stw	r12,  FMAX
	stw	r10,  4 + FMAX

	lfs	f1,   FZERO

#ifdef F_INTERFACE
	LDINT	N,    0(N)
	LDINT	INCX, 0(INCX)
#endif

	slwi	INCX, INCX, BASE_SHIFT

	li	PREA, L1_PREFETCHSIZE

	cmpwi	cr0, N, 0
	ble-	LL(9999)
	cmpwi	cr0, INCX, 0
	ble-	LL(9999)

	mr	NN, N
	mr	XX, X

	LFD	f1, 0 * SIZE(X)
	add	X, X, INCX

	fabs	f0, f1
	fabs	f2, f1
	fabs	f3, f1
	fabs	f4, f1
	fabs	f5, f1
	fabs	f6, f1
	fabs	f7, f1
	fabs	f1, f1
	subi	N, N, 1

	cmpwi	cr0, N, 0
	ble-	LL(9999)

	cmpwi	cr0, INCX, SIZE
	bne-	cr0, LL(1000)

	srawi.	r0, N, 4
	mtspr	CTR, r0
	beq-	cr0, LL(50)

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

	LFD	f24,   8 * SIZE(X)
	LFD	f25,   9 * SIZE(X)
	LFD	f26,  10 * SIZE(X)
	LFD	f27,  11 * SIZE(X)

	fabs	f12, f28
	fabs	f13, f29
	fabs	f14, f30
	fabs	f15, f31

	LFD	f28,  12 * SIZE(X)
	LFD	f29,  13 * SIZE(X)
	LFD	f30,  14 * SIZE(X)
	LFD	f31,  15 * SIZE(X)
	bdz	LL(20)
	.align 4

LL(10):
	fsub	f16, f0,  f8
	fsub	f17, f1,  f9
	fsub	f18, f2,  f10
	fsub	f19, f3,  f11
	fsub	f20, f4,  f12
	fsub	f21, f5,  f13
	fsub	f22, f6,  f14
	fsub	f23, f7,  f15

	fsel	f0,  f16, f0,  f8
	fabs	f8,  f24
	fsel	f1,  f17, f1,  f9
	fabs	f9,  f25
	fsel	f2,  f18, f2,  f10
	fabs	f10, f26
	fsel	f3,  f19, f3,  f11
	fabs	f11, f27

	LFD	f24,  16 * SIZE(X)
	LFD	f25,  17 * SIZE(X)
	LFD	f26,  18 * SIZE(X)
	LFD	f27,  19 * SIZE(X)

	fsel	f4,  f20, f4,  f12
	fabs	f12, f28
	fsel	f5,  f21, f5,  f13
	fabs	f13, f29
	fsel	f6,  f22, f6,  f14
	fabs	f14, f30
	fsel	f7,  f23, f7,  f15
	fabs	f15, f31

	LFD	f28,  20 * SIZE(X)
	LFD	f29,  21 * SIZE(X)
	LFD	f30,  22 * SIZE(X)
	LFD	f31,  23 * SIZE(X)

	fsub	f16, f0,  f8
	fsub	f17, f1,  f9
	fsub	f18, f2,  f10
	fsub	f19, f3,  f11
	fsub	f20, f4,  f12
	fsub	f21, f5,  f13
	fsub	f22, f6,  f14
	fsub	f23, f7,  f15

	fsel	f0,  f16, f0,  f8
	fabs	f8,  f24
	fsel	f1,  f17, f1,  f9
	fabs	f9,  f25
	fsel	f2,  f18, f2,  f10
	fabs	f10, f26
	fsel	f3,  f19, f3,  f11
	fabs	f11, f27

	LFD	f24,  24 * SIZE(X)
	LFD	f25,  25 * SIZE(X)
	LFD	f26,  26 * SIZE(X)
	LFD	f27,  27 * SIZE(X)

	fsel	f4,  f20, f4,  f12
	fabs	f12, f28
	fsel	f5,  f21, f5,  f13
	fabs	f13, f29
	fsel	f6,  f22, f6,  f14
	fabs	f14, f30
	fsel	f7,  f23, f7,  f15
	fabs	f15, f31

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
	fsub	f16, f0,  f8
	fsub	f17, f1,  f9
	fsub	f18, f2,  f10
	fsub	f19, f3,  f11
	fsub	f20, f4,  f12
	fsub	f21, f5,  f13
	fsub	f22, f6,  f14
	fsub	f23, f7,  f15

	fsel	f0,  f16, f0,  f8
	fabs	f8,  f24
	fsel	f1,  f17, f1,  f9
	fabs	f9,  f25
	fsel	f2,  f18, f2,  f10
	fabs	f10, f26
	fsel	f3,  f19, f3,  f11
	fabs	f11, f27

	fsel	f4,  f20, f4,  f12
	fabs	f12, f28
	fsel	f5,  f21, f5,  f13
	fabs	f13, f29
	fsel	f6,  f22, f6,  f14
	fabs	f14, f30
	fsel	f7,  f23, f7,  f15
	fabs	f15, f31

	fsub	f16, f0,  f8
	fsub	f17, f1,  f9
	fsub	f18, f2,  f10
	fsub	f19, f3,  f11
	fsub	f20, f4,  f12
	fsub	f21, f5,  f13
	fsub	f22, f6,  f14
	fsub	f23, f7,  f15

	fsel	f0,  f16, f0,  f8
	fsel	f1,  f17, f1,  f9
	fsel	f2,  f18, f2,  f10
	fsel	f3,  f19, f3,  f11
	fsel	f4,  f20, f4,  f12
	fsel	f5,  f21, f5,  f13
	fsel	f6,  f22, f6,  f14
	fsel	f7,  f23, f7,  f15
	addi	X, X, 16 * SIZE
	.align 4

LL(50):
	andi.	r0,  N, 15
	mtspr	CTR, r0
	beq	LL(100)
	.align 4

LL(60):
	LFD	f8,  0 * SIZE(X)
	addi	X, X,  1 * SIZE
	fabs	f8, f8
	fsub	f16, f1, f8
	fsel	f1, f16, f1, f8
	bdnz	LL(60)
	.align 4

LL(100):
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
	fsel	f31, f8,  f0,  f4

	lfs	f1,  FZERO
	lfs	f0,  FONE

	fcmpu	cr0, f1, f31
	beq-	cr0, LL(9999)

	fdiv	f30, f0, f31

	fmr	f0, f1
	fmr	f2, f1
	fmr	f3, f1
	fmr	f4, f1
	fmr	f5, f1
	fmr	f6, f1
	fmr	f7, f1

	srawi.	r0, NN, 4
	mtspr	CTR, r0
	beq-	cr0, LL(250)

	LFD	f8,   0 * SIZE(XX)
	LFD	f9,   1 * SIZE(XX)
	LFD	f10,  2 * SIZE(XX)
	LFD	f11,  3 * SIZE(XX)
	LFD	f12,  4 * SIZE(XX)
	LFD	f13,  5 * SIZE(XX)
	LFD	f14,  6 * SIZE(XX)
	LFD	f15,  7 * SIZE(XX)

	fmul	f16, f30, f8
	fmul	f17, f30, f9
	fmul	f18, f30, f10
	fmul	f19, f30, f11

	LFD	f8,   8 * SIZE(XX)
	LFD	f9,   9 * SIZE(XX)
	LFD	f10, 10 * SIZE(XX)
	LFD	f11, 11 * SIZE(XX)

	fmul	f20, f30, f12
	fmul	f21, f30, f13
	fmul	f22, f30, f14
	fmul	f23, f30, f15

	LFD	f12, 12 * SIZE(XX)
	LFD	f13, 13 * SIZE(XX)
	LFD	f14, 14 * SIZE(XX)
	LFD	f15, 15 * SIZE(XX)
	bdz	LL(220)
	.align 4

LL(210):
	fmadd	f0,  f16, f16, f0
	fmul	f16, f30, f8
	fmadd	f1,  f17, f17, f1
	fmul	f17, f30, f9
	fmadd	f2,  f18, f18, f2
	fmul	f18, f30, f10
	fmadd	f3,  f19, f19, f3
	fmul	f19, f30, f11

	LFD	f8,  16 * SIZE(XX)
	LFD	f9,  17 * SIZE(XX)
	LFD	f10, 18 * SIZE(XX)
	LFD	f11, 19 * SIZE(XX)

	fmadd	f4,  f20, f20, f4
	fmul	f20, f30, f12
	fmadd	f5,  f21, f21, f5
	fmul	f21, f30, f13
	fmadd	f6,  f22, f22, f6
	fmul	f22, f30, f14
	fmadd	f7,  f23, f23, f7
	fmul	f23, f30, f15

	LFD	f12, 20 * SIZE(XX)
	LFD	f13, 21 * SIZE(XX)
	LFD	f14, 22 * SIZE(XX)
	LFD	f15, 23 * SIZE(XX)

	fmadd	f0,  f16, f16, f0
	fmul	f16, f30, f8
	fmadd	f1,  f17, f17, f1
	fmul	f17, f30, f9
	fmadd	f2,  f18, f18, f2
	fmul	f18, f30, f10
	fmadd	f3,  f19, f19, f3
	fmul	f19, f30, f11

	LFD	f8,  24 * SIZE(XX)
	LFD	f9,  25 * SIZE(XX)
	LFD	f10, 26 * SIZE(XX)
	LFD	f11, 27 * SIZE(XX)

	fmadd	f4,  f20, f20, f4
	fmul	f20, f30, f12
	fmadd	f5,  f21, f21, f5
	fmul	f21, f30, f13
	fmadd	f6,  f22, f22, f6
	fmul	f22, f30, f14
	fmadd	f7,  f23, f23, f7
	fmul	f23, f30, f15

	LFD	f12, 28 * SIZE(XX)
	LFD	f13, 29 * SIZE(XX)
	LFD	f14, 30 * SIZE(XX)
	LFD	f15, 31 * SIZE(XX)

#ifndef POWER6
	L1_PREFETCH	XX, PREA
#endif
	addi	XX, XX, 16 * SIZE
#ifdef POWER6
	L1_PREFETCH	XX, PREA
#endif

	bdnz	LL(210)
	.align 4

LL(220):
	fmadd	f0,  f16, f16, f0
	fmul	f16, f30, f8
	fmadd	f1,  f17, f17, f1
	fmul	f17, f30, f9
	fmadd	f2,  f18, f18, f2
	fmul	f18, f30, f10
	fmadd	f3,  f19, f19, f3
	fmul	f19, f30, f11

	fmadd	f4,  f20, f20, f4
	fmul	f20, f30, f12
	fmadd	f5,  f21, f21, f5
	fmul	f21, f30, f13
	fmadd	f6,  f22, f22, f6
	fmul	f22, f30, f14
	fmadd	f7,  f23, f23, f7
	fmul	f23, f30, f15

	fmadd	f0,  f16, f16, f0
	fmadd	f1,  f17, f17, f1
	fmadd	f2,  f18, f18, f2
	fmadd	f3,  f19, f19, f3
	fmadd	f4,  f20, f20, f4
	fmadd	f5,  f21, f21, f5
	fmadd	f6,  f22, f22, f6
	fmadd	f7,  f23, f23, f7

	addi	XX, XX,  16 * SIZE
	.align 4

LL(250):
	andi.	r0,  NN, 15
	mtspr	CTR, r0
	beq-	cr0, LL(270)
	.align 4

LL(260):
	LFD	f8,  0 * SIZE(XX)
	addi	XX, XX,  1 * SIZE

	fmul	f16, f30, f8
	fmadd	f0,  f16, f16, f0
	bdnz	LL(260)
	.align 4

LL(270):
	fadd   f0, f0, f1
	fadd   f2, f2, f3
	fadd   f4, f4, f5
	fadd   f6, f6, f7

	fadd   f0, f0, f2
	fadd   f4, f4, f6

	fadd   f0, f0, f4

	fsqrt  f0, f0
	fmul   f1, f31, f0
	b	LL(9999)
	.align 4

LL(1000):
	sub	X, X, INCX

	srawi.	r0, N, 4
	mtspr	CTR,  r0
	beq-	LL(1050)

	LFDUX	f24,   X, INCX
	LFDUX	f25,   X, INCX
	LFDUX	f26,   X, INCX
	LFDUX	f27,   X, INCX
	LFDUX	f28,   X, INCX
	LFDUX	f29,   X, INCX
	LFDUX	f30,   X, INCX
	LFDUX	f31,   X, INCX

	fabs	f8,  f24
	fabs	f9,  f25
	fabs	f10, f26
	fabs	f11, f27

	LFDUX	f24,   X, INCX
	LFDUX	f25,   X, INCX
	LFDUX	f26,   X, INCX
	LFDUX	f27,   X, INCX

	fabs	f12, f28
	fabs	f13, f29
	fabs	f14, f30
	fabs	f15, f31

	LFDUX	f28,   X, INCX
	LFDUX	f29,   X, INCX
	LFDUX	f30,   X, INCX
	LFDUX	f31,   X, INCX
	bdz	LL(1020)
	.align 4

LL(1010):
	fsub	f16, f0,  f8
	fsub	f17, f1,  f9
	fsub	f18, f2,  f10
	fsub	f19, f3,  f11
	fsub	f20, f4,  f12
	fsub	f21, f5,  f13
	fsub	f22, f6,  f14
	fsub	f23, f7,  f15

	fsel	f0,  f16, f0,  f8
	fabs	f8,  f24
	fsel	f1,  f17, f1,  f9
	fabs	f9,  f25
	fsel	f2,  f18, f2,  f10
	fabs	f10, f26
	fsel	f3,  f19, f3,  f11
	fabs	f11, f27

	LFDUX	f24,   X, INCX
	LFDUX	f25,   X, INCX
	LFDUX	f26,   X, INCX
	LFDUX	f27,   X, INCX

	fsel	f4,  f20, f4,  f12
	fabs	f12, f28
	fsel	f5,  f21, f5,  f13
	fabs	f13, f29
	fsel	f6,  f22, f6,  f14
	fabs	f14, f30
	fsel	f7,  f23, f7,  f15
	fabs	f15, f31

	LFDUX	f28,   X, INCX
	LFDUX	f29,   X, INCX
	LFDUX	f30,   X, INCX
	LFDUX	f31,   X, INCX

	fsub	f16, f0,  f8
	fsub	f17, f1,  f9
	fsub	f18, f2,  f10
	fsub	f19, f3,  f11
	fsub	f20, f4,  f12
	fsub	f21, f5,  f13
	fsub	f22, f6,  f14
	fsub	f23, f7,  f15

	fsel	f0,  f16, f0,  f8
	fabs	f8,  f24
	fsel	f1,  f17, f1,  f9
	fabs	f9,  f25
	fsel	f2,  f18, f2,  f10
	fabs	f10, f26
	fsel	f3,  f19, f3,  f11
	fabs	f11, f27

	LFDUX	f24,   X, INCX
	LFDUX	f25,   X, INCX
	LFDUX	f26,   X, INCX
	LFDUX	f27,   X, INCX

	fsel	f4,  f20, f4,  f12
	fabs	f12, f28
	fsel	f5,  f21, f5,  f13
	fabs	f13, f29
	fsel	f6,  f22, f6,  f14
	fabs	f14, f30
	fsel	f7,  f23, f7,  f15
	fabs	f15, f31

	LFDUX	f28,   X, INCX
	LFDUX	f29,   X, INCX
	LFDUX	f30,   X, INCX
	LFDUX	f31,   X, INCX
	bdnz	LL(1010)
	.align 4

LL(1020):
	fsub	f16, f0,  f8
	fsub	f17, f1,  f9
	fsub	f18, f2,  f10
	fsub	f19, f3,  f11
	fsub	f20, f4,  f12
	fsub	f21, f5,  f13
	fsub	f22, f6,  f14
	fsub	f23, f7,  f15

	fsel	f0,  f16, f0,  f8
	fabs	f8,  f24
	fsel	f1,  f17, f1,  f9
	fabs	f9,  f25
	fsel	f2,  f18, f2,  f10
	fabs	f10, f26
	fsel	f3,  f19, f3,  f11
	fabs	f11, f27

	fsel	f4,  f20, f4,  f12
	fabs	f12, f28
	fsel	f5,  f21, f5,  f13
	fabs	f13, f29
	fsel	f6,  f22, f6,  f14
	fabs	f14, f30
	fsel	f7,  f23, f7,  f15
	fabs	f15, f31

	fsub	f16, f0,  f8
	fsub	f17, f1,  f9
	fsub	f18, f2,  f10
	fsub	f19, f3,  f11
	fsub	f20, f4,  f12
	fsub	f21, f5,  f13
	fsub	f22, f6,  f14
	fsub	f23, f7,  f15

	fsel	f0,  f16, f0,  f8
	fsel	f1,  f17, f1,  f9
	fsel	f2,  f18, f2,  f10
	fsel	f3,  f19, f3,  f11
	fsel	f4,  f20, f4,  f12
	fsel	f5,  f21, f5,  f13
	fsel	f6,  f22, f6,  f14
	fsel	f7,  f23, f7,  f15
	.align 4

LL(1050):
	andi.	r0,  N, 15
	mtspr	CTR, r0
	beq	LL(1999)
	.align 4

LL(1060):
	LFDUX	f8,    X, INCX
	fabs	f8, f8
	fsub	f16, f1, f8
	fsel	f1, f16, f1, f8
	bdnz	LL(1060)
	.align 4

LL(1999):
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
	fsel	f31, f8,  f0,  f4

	lfs	f1,  FZERO
	lfs	f0,  FONE
	lfd	f2,  FMAX

	fcmpu	cr0, f1, f31
	beq-	cr0, LL(9999)

	fdiv	f30, f0, f31

	fmr	f0, f1
	fmr	f2, f1
	fmr	f3, f1
	fmr	f4, f1
	fmr	f5, f1
	fmr	f6, f1
	fmr	f7, f1

	sub	XX, XX, INCX

	srawi.	r0, NN, 4
	mtspr	CTR, r0
	beq-	cr0, LL(2150)

	LFDUX	f8,   XX, INCX
	LFDUX	f9,   XX, INCX
	LFDUX	f10,  XX, INCX
	LFDUX	f11,  XX, INCX
	LFDUX	f12,  XX, INCX
	LFDUX	f13,  XX, INCX
	LFDUX	f14,  XX, INCX
	LFDUX	f15,  XX, INCX

	fmul	f16, f30, f8
	fmul	f17, f30, f9
	fmul	f18, f30, f10
	fmul	f19, f30, f11

	LFDUX	f8,  XX, INCX
	LFDUX	f9,  XX, INCX
	LFDUX	f10, XX, INCX
	LFDUX	f11, XX, INCX

	fmul	f20, f30, f12
	fmul	f21, f30, f13
	fmul	f22, f30, f14
	fmul	f23, f30, f15

	LFDUX	f12, XX, INCX
	LFDUX	f13, XX, INCX
	LFDUX	f14, XX, INCX
	LFDUX	f15, XX, INCX
	bdz	LL(2120)
	.align 4

LL(2110):
	fmadd	f0,  f16, f16, f0
	fmul	f16, f30, f8
	fmadd	f1,  f17, f17, f1
	fmul	f17, f30, f9
	fmadd	f2,  f18, f18, f2
	fmul	f18, f30, f10
	fmadd	f3,  f19, f19, f3
	fmul	f19, f30, f11

	LFDUX	f8,   XX, INCX
	LFDUX	f9,   XX, INCX
	LFDUX	f10,  XX, INCX
	LFDUX	f11,  XX, INCX

	fmadd	f4,  f20, f20, f4
	fmul	f20, f30, f12
	fmadd	f5,  f21, f21, f5
	fmul	f21, f30, f13
	fmadd	f6,  f22, f22, f6
	fmul	f22, f30, f14
	fmadd	f7,  f23, f23, f7
	fmul	f23, f30, f15

	LFDUX	f12,  XX, INCX
	LFDUX	f13,  XX, INCX
	LFDUX	f14,  XX, INCX
	LFDUX	f15,  XX, INCX

	fmadd	f0,  f16, f16, f0
	fmul	f16, f30, f8
	fmadd	f1,  f17, f17, f1
	fmul	f17, f30, f9
	fmadd	f2,  f18, f18, f2
	fmul	f18, f30, f10
	fmadd	f3,  f19, f19, f3
	fmul	f19, f30, f11

	LFDUX	f8,   XX, INCX
	LFDUX	f9,   XX, INCX
	LFDUX	f10,  XX, INCX
	LFDUX	f11,  XX, INCX

	fmadd	f4,  f20, f20, f4
	fmul	f20, f30, f12
	fmadd	f5,  f21, f21, f5
	fmul	f21, f30, f13
	fmadd	f6,  f22, f22, f6
	fmul	f22, f30, f14
	fmadd	f7,  f23, f23, f7
	fmul	f23, f30, f15

	LFDUX	f12,  XX, INCX
	LFDUX	f13,  XX, INCX
	LFDUX	f14,  XX, INCX
	LFDUX	f15,  XX, INCX

	bdnz	LL(2110)
	.align 4

LL(2120):
	fmadd	f0,  f16, f16, f0
	fmul	f16, f30, f8
	fmadd	f1,  f17, f17, f1
	fmul	f17, f30, f9
	fmadd	f2,  f18, f18, f2
	fmul	f18, f30, f10
	fmadd	f3,  f19, f19, f3
	fmul	f19, f30, f11

	fmadd	f4,  f20, f20, f4
	fmul	f20, f30, f12
	fmadd	f5,  f21, f21, f5
	fmul	f21, f30, f13
	fmadd	f6,  f22, f22, f6
	fmul	f22, f30, f14
	fmadd	f7,  f23, f23, f7
	fmul	f23, f30, f15

	fmadd	f0,  f16, f16, f0
	fmadd	f1,  f17, f17, f1
	fmadd	f2,  f18, f18, f2
	fmadd	f3,  f19, f19, f3
	fmadd	f4,  f20, f20, f4
	fmadd	f5,  f21, f21, f5
	fmadd	f6,  f22, f22, f6
	fmadd	f7,  f23, f23, f7
	.align 4

LL(2150):
	andi.	r0,  NN, 15
	mtspr	CTR, r0
	beq-	cr0, LL(2170)
	.align 4

LL(2160):
	LFDUX	f8,  XX, INCX

	fmul	f16, f30, f8
	fmadd	f0,  f16, f16, f0
	bdnz	LL(2160)
	.align 4

LL(2170):
	fadd   f0, f0, f1
	fadd   f2, f2, f3
	fadd   f4, f4, f5
	fadd   f6, f6, f7

	fadd   f0, f0, f2
	fadd   f4, f4, f6

	fadd   f0, f0, f4

	fsqrt  f0, f0
	fmul   f1, f31, f0
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
