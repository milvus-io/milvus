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
#define INCXM1	r9

#define FZERO	144(SP)
#define FONE	148(SP)

#define STACKSIZE 160

	PROLOGUE
	PROFCODE

	addi	SP, SP, -STACKSIZE
	li	r10,   0
	lis	r11,   0x3f80

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

	lfs	f1,   FZERO

#ifdef F_INTERFACE
	LDINT	N,    0(N)
	LDINT	INCX, 0(INCX)
#endif

	slwi	INCX, INCX, ZBASE_SHIFT
	subi	INCXM1, INCX, SIZE

	li	PREA, 4 * 16 * SIZE

	cmpwi	cr0, N, 0
	ble-	LL(9999)
	cmpwi	cr0, INCX, 0
	ble-	LL(9999)

	fmr	f0,  f1
	fmr	f2,  f1
	fmr	f3,  f1
	fmr	f4,  f1
	fmr	f5,  f1
	fmr	f6,  f1
	fmr	f7,  f1
	fmr	f8,  f1
	fmr	f9,  f1
	fmr	f10, f1
	fmr	f11, f1
	fmr	f12, f1
	fmr	f13, f1
	fmr	f14, f1
	fmr	f15, f1

	cmpwi	cr0, INCX, 2 * SIZE
	bne-	cr0, LL(1000)

	srawi.	r0, N, 3
	mtspr	CTR, r0
	beq-	cr0, LL(150)

	LFD	f16,  0 * SIZE(X)
	LFD	f17,  1 * SIZE(X)
	LFD	f18,  2 * SIZE(X)
	LFD	f19,  3 * SIZE(X)
	LFD	f20,  4 * SIZE(X)
	LFD	f21,  5 * SIZE(X)
	LFD	f22,  6 * SIZE(X)
	LFD	f23,  7 * SIZE(X)

	LFD	f24,  8 * SIZE(X)
	LFD	f25,  9 * SIZE(X)
	LFD	f26, 10 * SIZE(X)
	LFD	f27, 11 * SIZE(X)
	LFD	f28, 12 * SIZE(X)
	LFD	f29, 13 * SIZE(X)
	LFD	f30, 14 * SIZE(X)
	LFD	f31, 15 * SIZE(X)

	bdz	LL(120)
	.align 4

LL(110):
	fmadd	f0,  f16, f16, f0
	fmadd	f1,  f17, f17, f1
	fmadd	f2,  f18, f18, f2
	fmadd	f3,  f19, f19, f3

	LFD	f16, 16 * SIZE(X)
	LFD	f17, 17 * SIZE(X)
	LFD	f18, 18 * SIZE(X)
	LFD	f19, 19 * SIZE(X)

	fmadd	f4,  f20, f20, f4
	fmadd	f5,  f21, f21, f5
	fmadd	f6,  f22, f22, f6
	fmadd	f7,  f23, f23, f7

	LFD	f20, 20 * SIZE(X)
	LFD	f21, 21 * SIZE(X)
	LFD	f22, 22 * SIZE(X)
	LFD	f23, 23 * SIZE(X)

	fmadd	f8,  f24, f24, f8
	fmadd	f9,  f25, f25, f9
	fmadd	f10, f26, f26, f10
	fmadd	f11, f27, f27, f11

	LFD	f24, 24 * SIZE(X)
	LFD	f25, 25 * SIZE(X)
	LFD	f26, 26 * SIZE(X)
	LFD	f27, 27 * SIZE(X)

	fmadd	f12, f28, f28, f12
	fmadd	f13, f29, f29, f13
	fmadd	f14, f30, f30, f14
	fmadd	f15, f31, f31, f15

	LFD	f28, 28 * SIZE(X)
	LFD	f29, 29 * SIZE(X)
	LFD	f30, 30 * SIZE(X)
	LFD	f31, 31 * SIZE(X)

#ifndef POWER6
	L1_PREFETCH	X, PREA
#endif
	addi	X, X, 16 * SIZE
#ifdef POWER6
	L1_PREFETCH	X, PREA
#endif

	bdnz	LL(110)
	.align 4

LL(120):
	fmadd	f0,  f16, f16, f0
	fmadd	f1,  f17, f17, f1
	fmadd	f2,  f18, f18, f2
	fmadd	f3,  f19, f19, f3
	fmadd	f4,  f20, f20, f4
	fmadd	f5,  f21, f21, f5
	fmadd	f6,  f22, f22, f6
	fmadd	f7,  f23, f23, f7
	fmadd	f8,  f24, f24, f8
	fmadd	f9,  f25, f25, f9
	fmadd	f10, f26, f26, f10
	fmadd	f11, f27, f27, f11
	fmadd	f12, f28, f28, f12
	fmadd	f13, f29, f29, f13
	fmadd	f14, f30, f30, f14
	fmadd	f15, f31, f31, f15
	addi	X, X,  16 * SIZE
	.align 4

LL(150):
	andi.	r0,  N, 7
	mtspr	CTR, r0
	beq-	cr0, LL(170)
	.align 4

LL(160):
	LFD	f16,  0 * SIZE(X)
	LFD	f17,  1 * SIZE(X)
	addi	X, X,  2 * SIZE
	fmadd	f0,  f16, f16, f0
	fmadd	f1,  f17, f17, f1
	bdnz	LL(160)
	.align 4

LL(170):
	fadd   f0,  f0,  f1
	fadd   f2,  f2,  f3
	fadd   f4,  f4,  f5
	fadd   f6,  f6,  f7

	fadd   f8,  f8,  f9
	fadd   f10, f10, f11
	fadd   f12, f12, f13
	fadd   f14, f14, f15

	fadd   f0,  f0,  f2
	fadd   f4,  f4,  f6
	fadd   f8,  f8,  f10
	fadd   f12, f12, f14

	fadd   f0,  f0,  f4
	fadd   f8,  f8,  f12

	fadd   f0,  f0,  f8

	fsqrt  f1, f0
	b	LL(9999)
	.align 4

LL(1000):
	sub	X, X, INCXM1

	srawi.	r0, N, 3
	mtspr	CTR, r0
	beq-	cr0, LL(1150)

	LFDX	f16, X, INCXM1
	LFDUX	f17, X, INCX
	LFDX	f18, X, INCXM1
	LFDUX	f19, X, INCX
	LFDX	f20, X, INCXM1
	LFDUX	f21, X, INCX
	LFDX	f22, X, INCXM1
	LFDUX	f23, X, INCX

	LFDX	f24, X, INCXM1
	LFDUX	f25, X, INCX
	LFDX	f26, X, INCXM1
	LFDUX	f27, X, INCX

	LFDX	f28, X, INCXM1
	LFDUX	f29, X, INCX
	LFDX	f30, X, INCXM1
	LFDUX	f31, X, INCX
	bdz	LL(1120)
	.align 4

LL(1110):
	fmadd	f0,  f16, f16, f0
	fmadd	f1,  f17, f17, f1
	fmadd	f2,  f18, f18, f2
	fmadd	f3,  f19, f19, f3

	LFDX	f16, X, INCXM1
	LFDUX	f17, X, INCX
	LFDX	f18, X, INCXM1
	LFDUX	f19, X, INCX

	fmadd	f4,  f20, f20, f4
	fmadd	f5,  f21, f21, f5
	fmadd	f6,  f22, f22, f6
	fmadd	f7,  f23, f23, f7

	LFDX	f20, X, INCXM1
	LFDUX	f21, X, INCX
	LFDX	f22, X, INCXM1
	LFDUX	f23, X, INCX

	fmadd	f8,  f24, f24, f8
	fmadd	f9,  f25, f25, f9
	fmadd	f10, f26, f26, f10
	fmadd	f11, f27, f27, f11

	LFDX	f24, X, INCXM1
	LFDUX	f25, X, INCX
	LFDX	f26, X, INCXM1
	LFDUX	f27, X, INCX

	fmadd	f12, f28, f28, f12
	fmadd	f13, f29, f29, f13
	fmadd	f14, f30, f30, f14
	fmadd	f15, f31, f31, f15

	LFDX	f28, X, INCXM1
	LFDUX	f29, X, INCX
	LFDX	f30, X, INCXM1
	LFDUX	f31, X, INCX
	bdnz	LL(1110)
	.align 4

LL(1120):
	fmadd	f0,  f16, f16, f0
	fmadd	f1,  f17, f17, f1
	fmadd	f2,  f18, f18, f2
	fmadd	f3,  f19, f19, f3

	fmadd	f4,  f20, f20, f4
	fmadd	f5,  f21, f21, f5
	fmadd	f6,  f22, f22, f6
	fmadd	f7,  f23, f23, f7

	fmadd	f8,  f24, f24, f8
	fmadd	f9,  f25, f25, f9
	fmadd	f10, f26, f26, f10
	fmadd	f11, f27, f27, f11

	fmadd	f12, f28, f28, f12
	fmadd	f13, f29, f29, f13
	fmadd	f14, f30, f30, f14
	fmadd	f15, f31, f31, f15
	.align 4

LL(1150):
	andi.	r0,  N, 7
	mtspr	CTR, r0
	beq-	cr0, LL(1170)
	.align 4

LL(1160):
	LFDX	f16,   X, INCXM1
	LFDUX	f17,   X, INCX
	fmadd	f0,  f16, f16, f0
	fmadd	f1,  f17, f17, f1
	bdnz	LL(1160)
	.align 4

LL(1170):
	fadd   f0,  f0,  f1
	fadd   f2,  f2,  f3
	fadd   f4,  f4,  f5
	fadd   f6,  f6,  f7

	fadd   f8,  f8,  f9
	fadd   f10, f10, f11
	fadd   f12, f12, f13
	fadd   f14, f14, f15

	fadd   f0,  f0,  f2
	fadd   f4,  f4,  f6
	fadd   f8,  f8,  f10
	fadd   f12, f12, f14

	fadd   f0,  f0,  f4
	fadd   f8,  f8,  f12

	fadd   f0,  f0,  f8

	fsqrt  f1, f0
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
