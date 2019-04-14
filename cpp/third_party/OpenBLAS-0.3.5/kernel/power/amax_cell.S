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

	li	PREA, 10 * 16 * SIZE

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
	bne-	cr0, LL(20)

	srawi.	r0, N, 4
	mtspr	CTR, r0
	beq-	cr0, LL(15)

	LFD	f8,    0 * SIZE(X)
	LFD	f9,    1 * SIZE(X)

	bdz	LL(13)
	.align 4

LL(12):
	fabs	f8,  f8
	LFD	f10,   2 * SIZE(X)
	fabs	f9,  f9
	LFD	f11,   3 * SIZE(X)
	fabs	f10, f10
	LFD	f12,   4 * SIZE(X)
	fabs	f11, f11
	LFD	f13,   5 * SIZE(X)
	fabs	f12, f12
	LFD	f14,   6 * SIZE(X)
	fabs	f13, f13
	LFD	f15,   7 * SIZE(X)

	fabs	f14, f14
	dcbt	X, PREA
	fabs	f15, f15
	nop

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
	LFD	f8,    8 * SIZE(X)
	fsel	f7,  f23, f7,  f15
	LFD	f9,    9 * SIZE(X)

	fabs	f8,  f8
	LFD	f10,  10 * SIZE(X)
	fabs	f9,  f9
	LFD	f11,  11 * SIZE(X)
	fabs	f10, f10
	LFD	f12,  12 * SIZE(X)
	fabs	f11, f11
	LFD	f13,  13 * SIZE(X)
	fabs	f12, f12
	LFD	f14,  14 * SIZE(X)
	fabs	f13, f13
	LFD	f15,  15 * SIZE(X)

	fabs	f14, f14
	addi	X, X, 16 * SIZE
	fabs	f15, f15

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
	LFD	f8,    0 * SIZE(X)

	fsel	f6,  f22, f6,  f14
	LFD	f9,    1 * SIZE(X)
	fsel	f7,  f23, f7,  f15
	bdnz	LL(12)
	.align 4

LL(13):
	fabs	f8,  f8
	LFD	f10,   2 * SIZE(X)
	fabs	f9,  f9
	LFD	f11,   3 * SIZE(X)
	fabs	f10, f10
	LFD	f12,   4 * SIZE(X)
	fabs	f11, f11
	LFD	f13,   5 * SIZE(X)
	fabs	f12, f12
	LFD	f14,   6 * SIZE(X)
	fabs	f13, f13
	LFD	f15,   7 * SIZE(X)

	fabs	f14, f14
	fabs	f15, f15

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
	LFD	f8,    8 * SIZE(X)
	fsel	f7,  f23, f7,  f15
	LFD	f9,    9 * SIZE(X)

	fabs	f8,  f8
	LFD	f10,  10 * SIZE(X)
	fabs	f9,  f9
	LFD	f11,  11 * SIZE(X)
	fabs	f10, f10
	LFD	f12,  12 * SIZE(X)
	fabs	f11, f11
	LFD	f13,  13 * SIZE(X)
	fabs	f12, f12
	LFD	f14,  14 * SIZE(X)
	fabs	f13, f13
	LFD	f15,  15 * SIZE(X)

	fabs	f14, f14
	addi	X, X, 16 * SIZE
	fabs	f15, f15
	nop

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

LL(15):
	andi.	r0,  N, 15
	beq	LL(999)

	andi.	r0,  N, 8
	beq	LL(16)

	LFD	f8,    0 * SIZE(X)
	LFD	f9,    1 * SIZE(X)

	fabs	f8,  f8
	LFD	f10,   2 * SIZE(X)
	fabs	f9,  f9
	LFD	f11,   3 * SIZE(X)
	fabs	f10, f10
	LFD	f12,   4 * SIZE(X)
	fabs	f11, f11
	LFD	f13,   5 * SIZE(X)

	fabs	f12, f12
	LFD	f14,   6 * SIZE(X)
	fabs	f13, f13
	LFD	f15,   7 * SIZE(X)
	fabs	f14, f14
	fabs	f15, f15

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
	nop
	fsel	f7,  f23, f7,  f15
	addi	X, X, 8 * SIZE
	.align 4

LL(16):
	andi.	r0,  N, 4
	beq	LL(17)

	LFD	f8,    0 * SIZE(X)
	LFD	f9,    1 * SIZE(X)

	fabs	f8,  f8
	LFD	f10,   2 * SIZE(X)
	fabs	f9,  f9
	LFD	f11,   3 * SIZE(X)
	fabs	f10, f10
	fabs	f11, f11

	fsub	f16, f0,  f8
	fsub	f17, f1,  f9
	fsub	f18, f2,  f10
	fsub	f19, f3,  f11

	fsel	f0,  f16, f0,  f8
	fsel	f1,  f17, f1,  f9
	fsel	f2,  f18, f2,  f10
	nop
	fsel	f3,  f19, f3,  f11
	addi	X, X, 4 * SIZE
	.align 4

LL(17):
	andi.	r0,  N, 2
	beq	LL(18)

	LFD	f8,    0 * SIZE(X)
	LFD	f9,    1 * SIZE(X)

	fabs	f8,  f8
	fabs	f9,  f9
	fsub	f16, f0,  f8
	fsub	f17, f1,  f9

	fsel	f0,  f16, f0,  f8
	nop
	fsel	f1,  f17, f1,  f9
	addi	X, X, 2 * SIZE
	.align 4

LL(18):
	andi.	r0,  N, 1
	beq	LL(999)

	LFD	f8,    0 * SIZE(X)
	fabs	f8,  f8
	fsub	f16, f0,  f8
	fsel	f0,  f16, f0,  f8
	b	LL(999)
	.align 4

LL(20):
	sub	X, X, INCX

	srawi.	r0, N, 4
	mtspr	CTR, r0
	beq-	cr0, LL(25)

	LFDUX	f8,   X, INCX
	LFDUX	f9,   X, INCX

	bdz	LL(23)
	.align 4

LL(22):
	fabs	f8,  f8
	LFDUX	f10,  X, INCX
	fabs	f9,  f9
	LFDUX	f11,  X, INCX
	fabs	f10, f10
	LFDUX	f12,  X, INCX
	fabs	f11, f11
	LFDUX	f13,  X, INCX
	fabs	f12, f12
	LFDUX	f14,  X, INCX
	fabs	f13, f13
	LFDUX	f15,  X, INCX

	fabs	f14, f14
	fabs	f15, f15

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
	LFDUX	f8,   X, INCX
	fsel	f7,  f23, f7,  f15
	LFDUX	f9,   X, INCX

	fabs	f8,  f8
	LFDUX	f10,  X, INCX
	fabs	f9,  f9
	LFDUX	f11,  X, INCX
	fabs	f10, f10
	LFDUX	f12,  X, INCX
	fabs	f11, f11
	LFDUX	f13,  X, INCX
	fabs	f12, f12
	LFDUX	f14,  X, INCX
	fabs	f13, f13
	LFDUX	f15,  X, INCX

	fabs	f14, f14
	fabs	f15, f15

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
	LFDUX	f8,    X, INCX

	fsel	f6,  f22, f6,  f14
	LFDUX	f9,    X, INCX
	fsel	f7,  f23, f7,  f15
	bdnz	LL(22)
	.align 4

LL(23):
	fabs	f8,  f8
	LFDUX	f10,   X, INCX
	fabs	f9,  f9
	LFDUX	f11,   X, INCX
	fabs	f10, f10
	LFDUX	f12,   X, INCX
	fabs	f11, f11
	LFDUX	f13,   X, INCX
	fabs	f12, f12
	LFDUX	f14,   X, INCX
	fabs	f13, f13
	LFDUX	f15,   X, INCX

	fabs	f14, f14
	fabs	f15, f15

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
	LFDUX	f8,   X, INCX
	fsel	f7,  f23, f7,  f15
	LFDUX	f9,   X, INCX

	fabs	f8,  f8
	LFDUX	f10,  X, INCX
	fabs	f9,  f9
	LFDUX	f11,  X, INCX
	fabs	f10, f10
	LFDUX	f12,  X, INCX
	fabs	f11, f11
	LFDUX	f13,  X, INCX
	fabs	f12, f12
	LFDUX	f14,  X, INCX
	fabs	f13, f13
	LFDUX	f15,  X, INCX

	fabs	f14, f14
	fabs	f15, f15

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

LL(25):
	andi.	r0,  N, 15
	beq	LL(999)

	andi.	r0,  N, 8
	beq	LL(26)

	LFDUX	f8,    X, INCX
	LFDUX	f9,    X, INCX

	fabs	f8,  f8
	LFDUX	f10,   X, INCX
	fabs	f9,  f9
	LFDUX	f11,   X, INCX
	fabs	f10, f10
	LFDUX	f12,   X, INCX
	fabs	f11, f11
	LFDUX	f13,   X, INCX

	fabs	f12, f12
	LFDUX	f14,   X, INCX
	fabs	f13, f13
	LFDUX	f15,   X, INCX
	fabs	f14, f14
	fabs	f15, f15

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

LL(26):
	andi.	r0,  N, 4
	beq	LL(27)

	LFDUX	f8,    X, INCX
	LFDUX	f9,    X, INCX

	fabs	f8,  f8
	LFDUX	f10,   X, INCX
	fabs	f9,  f9
	LFDUX	f11,   X, INCX
	fabs	f10, f10
	fabs	f11, f11

	fsub	f16, f0,  f8
	fsub	f17, f1,  f9
	fsub	f18, f2,  f10
	fsub	f19, f3,  f11

	fsel	f0,  f16, f0,  f8
	fsel	f1,  f17, f1,  f9
	fsel	f2,  f18, f2,  f10
	fsel	f3,  f19, f3,  f11
	.align 4

LL(27):
	andi.	r0,  N, 2
	beq	LL(28)

	LFDUX	f8,    X, INCX
	LFDUX	f9,    X, INCX

	fabs	f8,  f8
	fabs	f9,  f9
	fsub	f16, f0,  f8
	fsub	f17, f1,  f9

	fsel	f0,  f16, f0,  f8
	fsel	f1,  f17, f1,  f9
	.align 4

LL(28):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	f8,    X, INCX
	fabs	f8,  f8
	fsub	f16, f0,  f8
	fsel	f0,  f16, f0,  f8
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
