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

#define INCXM1	r9
#define PREA	r8

#define FZERO	f0

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

	slwi	INCX, INCX, ZBASE_SHIFT
	subi	INCXM1, INCX, SIZE

	fmr	f1,  FZERO
	fmr	f2,  FZERO
	fmr	f3,  FZERO
	fmr	f4,  FZERO
	fmr	f5,  FZERO
	fmr	f6,  FZERO
	fmr	f7,  FZERO

	li	PREA, L1_PREFETCHSIZE

	cmpwi	cr0, N, 0
	ble-	LL(999)

	cmpwi	cr0, INCX, 0
	ble-	LL(999)

	cmpwi	cr0, INCX, 2 * SIZE
	bne-	cr0, LL(100)

	srawi.	r0, N, 3
	mtspr	CTR, r0
	beq-	cr0, LL(50)
	.align 4

	LFD	f8,    0 * SIZE(X)
	LFD	f9,    1 * SIZE(X)
	LFD	f10,   2 * SIZE(X)
	LFD	f11,   3 * SIZE(X)
	LFD	f12,   4 * SIZE(X)
	LFD	f13,   5 * SIZE(X)
	LFD	f14,   6 * SIZE(X)
	LFD	f15,   7 * SIZE(X)

	LFD	f24,   8 * SIZE(X)
	LFD	f25,   9 * SIZE(X)
	LFD	f26,  10 * SIZE(X)
	LFD	f27,  11 * SIZE(X)
	LFD	f28,  12 * SIZE(X)
	LFD	f29,  13 * SIZE(X)
	LFD	f30,  14 * SIZE(X)
	LFD	f31,  15 * SIZE(X)

	fabs	f16, f8
	fabs	f17, f9
	fabs	f18, f10
	fabs	f19, f11

	fabs	f20, f12
	fabs	f21, f13
	fabs	f22, f14
	fabs	f23, f15
	bdz	LL(20)
	.align 4

LL(10):
	FADD	f0, f0, f16
	fabs	f16, f24
	FADD	f1, f1, f17
	fabs	f17, f25

	FADD	f2, f2, f18
	fabs	f18, f26
	FADD	f3, f3, f19
	fabs	f19, f27

	LFD	f8,   16 * SIZE(X)
	LFD	f9,   17 * SIZE(X)
	LFD	f10,  18 * SIZE(X)
	LFD	f11,  19 * SIZE(X)

	FADD	f4, f4, f20
	fabs	f20, f28
	FADD	f5, f5, f21
	fabs	f21, f29

	FADD	f6, f6, f22
	fabs	f22, f30
	FADD	f7, f7, f23
	fabs	f23, f31

	LFD	f12,  20 * SIZE(X)
	LFD	f13,  21 * SIZE(X)
	LFD	f14,  22 * SIZE(X)
	LFD	f15,  23 * SIZE(X)

	FADD	f0, f0, f16
	fabs	f16, f8
	FADD	f1, f1, f17
	fabs	f17, f9

	FADD	f2, f2, f18
	fabs	f18, f10
	FADD	f3, f3, f19
	fabs	f19, f11

	LFD	f24,  24 * SIZE(X)
	LFD	f25,  25 * SIZE(X)
	LFD	f26,  26 * SIZE(X)
	LFD	f27,  27 * SIZE(X)

	FADD	f4, f4, f20
	fabs	f20, f12
	FADD	f5, f5, f21
	fabs	f21, f13

	FADD	f6, f6, f22
	fabs	f22, f14
	FADD	f7, f7, f23
	fabs	f23, f15

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
	FADD	f0, f0, f16
	fabs	f16, f24
	FADD	f1, f1, f17
	fabs	f17, f25

	FADD	f2, f2, f18
	fabs	f18, f26
	FADD	f3, f3, f19
	fabs	f19, f27

	FADD	f4, f4, f20
	fabs	f20, f28
	FADD	f5, f5, f21
	fabs	f21, f29

	FADD	f6, f6, f22
	fabs	f22, f30
	FADD	f7, f7, f23
	fabs	f23, f31

	FADD	f0, f0, f16
	FADD	f1, f1, f17
	FADD	f2, f2, f18
	FADD	f3, f3, f19

	FADD	f4, f4, f20
	FADD	f5, f5, f21
	FADD	f6, f6, f22
	FADD	f7, f7, f23
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
	FADD	f0, f0,  f8
	FADD	f1, f1,  f9

	bdnz	LL(60)
	b	LL(999)
	.align 4

LL(100):
	sub	X, X, INCXM1

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(150)

	LFDX	f8,    X, INCXM1
	LFDUX	f9,    X, INCX
	LFDX	f10,   X, INCXM1
	LFDUX	f11,   X, INCX
	LFDX	f12,   X, INCXM1
	LFDUX	f13,   X, INCX
	LFDX	f14,   X, INCXM1
	LFDUX	f15,   X, INCX

	LFDX	f24,   X, INCXM1
	LFDUX	f25,   X, INCX
	LFDX	f26,   X, INCXM1
	LFDUX	f27,   X, INCX
	LFDX	f28,   X, INCXM1
	LFDUX	f29,   X, INCX
	LFDX	f30,   X, INCXM1
	LFDUX	f31,   X, INCX

	fabs	f16, f8
	fabs	f17, f9
	fabs	f18, f10
	fabs	f19, f11

	fabs	f20, f12
	fabs	f21, f13
	fabs	f22, f14
	fabs	f23, f15
	bdz	LL(120)
	.align 4

LL(110):
	FADD	f0, f0, f16
	fabs	f16, f24
	FADD	f1, f1, f17
	fabs	f17, f25

	FADD	f2, f2, f18
	fabs	f18, f26
	FADD	f3, f3, f19
	fabs	f19, f27

	LFDX	f8,    X, INCXM1
	LFDUX	f9,    X, INCX
	LFDX	f10,   X, INCXM1
	LFDUX	f11,   X, INCX

	FADD	f4, f4, f20
	fabs	f20, f28
	FADD	f5, f5, f21
	fabs	f21, f29

	FADD	f6, f6, f22
	fabs	f22, f30
	FADD	f7, f7, f23
	fabs	f23, f31

	LFDX	f12,   X, INCXM1
	LFDUX	f13,   X, INCX
	LFDX	f14,   X, INCXM1
	LFDUX	f15,   X, INCX

	FADD	f0, f0, f16
	fabs	f16, f8
	FADD	f1, f1, f17
	fabs	f17, f9

	FADD	f2, f2, f18
	fabs	f18, f10
	FADD	f3, f3, f19
	fabs	f19, f11

	LFDX	f24,   X, INCXM1
	LFDUX	f25,   X, INCX
	LFDX	f26,   X, INCXM1
	LFDUX	f27,   X, INCX

	FADD	f4, f4, f20
	fabs	f20, f12
	FADD	f5, f5, f21
	fabs	f21, f13

	FADD	f6, f6, f22
	fabs	f22, f14
	FADD	f7, f7, f23
	fabs	f23, f15

	LFDX	f28,   X, INCXM1
	LFDUX	f29,   X, INCX
	LFDX	f30,   X, INCXM1
	LFDUX	f31,   X, INCX
	bdnz	LL(110)
	.align 4

LL(120):
	FADD	f0, f0, f16
	fabs	f16, f24
	FADD	f1, f1, f17
	fabs	f17, f25

	FADD	f2, f2, f18
	fabs	f18, f26
	FADD	f3, f3, f19
	fabs	f19, f27

	FADD	f4, f4, f20
	fabs	f20, f28
	FADD	f5, f5, f21
	fabs	f21, f29

	FADD	f6, f6, f22
	fabs	f22, f30
	FADD	f7, f7, f23
	fabs	f23, f31

	FADD	f0, f0, f16
	FADD	f1, f1, f17
	FADD	f2, f2, f18
	FADD	f3, f3, f19

	FADD	f4, f4, f20
	FADD	f5, f5, f21
	FADD	f6, f6, f22
	FADD	f7, f7, f23
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
	FADD	f0,  f0, f8
	FADD	f1,  f1, f9
	bdnz	LL(160)
	.align 4

LL(999):
	FADD	f0,  f0,  f1
	FADD	f2,  f2,  f3
	FADD	f4,  f4,  f5
	FADD	f6,  f6,  f7

	FADD	f0,  f0,  f2
	FADD	f4,  f4,  f6
	FADD	f1,  f0,  f4

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
