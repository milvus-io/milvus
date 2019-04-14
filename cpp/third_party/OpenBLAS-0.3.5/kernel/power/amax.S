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

#define PREA	r8

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
	LDINT	N,    0(N)
	LDINT	INCX, 0(INCX)
#endif

	slwi	INCX, INCX, BASE_SHIFT

	li	PREA, L1_PREFETCHSIZE

	cmpwi	cr0, N, 0
	ble-	LL(9999)
	cmpwi	cr0, INCX, 0
	ble-	LL(9999)

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

	cmpwi	cr0, INCX, SIZE
	bne-	cr0, LL(100)

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
	beq	LL(999)
	.align 4

LL(60):
	LFD	f8,  0 * SIZE(X)
	addi	X, X,  1 * SIZE
	fabs	f8, f8
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
	bdz	LL(120)
	.align 4

LL(110):
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
	bdnz	LL(110)
	.align 4

LL(120):
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

LL(150):
	andi.	r0,  N, 15
	mtspr	CTR, r0
	beq	LL(999)
	.align 4

LL(160):
	LFDUX	f8,    X, INCX
	fabs	f8, f8
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
