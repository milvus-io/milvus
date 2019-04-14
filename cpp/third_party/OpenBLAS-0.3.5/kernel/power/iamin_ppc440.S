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
#define PRE	r9

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

	slwi	INCX, INCX, BASE_SHIFT
	sub	X, X, INCX
	li	PRE, 3 * 16 * SIZE

	mr	NN, N
	mr	XX, X

	cmpwi	cr0, N, 0
	ble-	LL(9999)
	cmpwi	cr0, INCX, 0
	ble-	LL(9999)

	LFDUX	f1, X, INCX

	fabs	f0, f1
	fabs	f2, f1
	fabs	f3, f1
	fabs	f4, f1
	fabs	f5, f1
	subi	N, N, 1
	fabs	f6, f1
	srawi.	r0, N, 4
	fabs	f7, f1
	mtspr	CTR,  r0
	fabs	f1, f1
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
	LFDUX	f24,   X, INCX
	fabs	f9,  f25
	LFDUX	f25,   X, INCX
	fabs	f10, f26
	LFDUX	f26,   X, INCX
	fabs	f11, f27
	LFDUX	f27,   X, INCX

	fabs	f12, f28
	LFDUX	f28,   X, INCX
	fabs	f13, f29
	LFDUX	f29,   X, INCX
	fabs	f14, f30
	LFDUX	f30,   X, INCX
	fabs	f15, f31
	LFDUX	f31,   X, INCX
	bdz	LL(120)
	.align 4

LL(110):
	fsub	f16, f0,  f8
#ifdef PPCG4
	dcbt	X, PRE
#endif
	fsub	f17, f1,  f9
	fsub	f18, f2,  f10
	fsub	f19, f3,  f11
	fsub	f20, f4,  f12
	fsub	f21, f5,  f13
	fsub	f22, f6,  f14
	fsub	f23, f7,  f15

	fsel	f0,  f16, f8,  f0
	fabs	f8,  f24
	LFDUX	f24,   X, INCX
	fsel	f1,  f17, f9,  f1
	fabs	f9,  f25
	LFDUX	f25,   X, INCX
	fsel	f2,  f18, f10, f2
	fabs	f10, f26
	LFDUX	f26,   X, INCX
	fsel	f3,  f19, f11, f3
	fabs	f11, f27
	LFDUX	f27,   X, INCX

	fsel	f4,  f20, f12, f4
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	X, PRE
#endif
	fabs	f12, f28
	LFDUX	f28,   X, INCX
	fsel	f5,  f21, f13, f5
	fabs	f13, f29
	LFDUX	f29,   X, INCX
	fsel	f6,  f22, f14, f6
	fabs	f14, f30
	LFDUX	f30,   X, INCX
	fsel	f7,  f23, f15, f7
	fabs	f15, f31
	LFDUX	f31,   X, INCX

	fsub	f16, f0,  f8
#ifdef PPCG4
	dcbt	X, PRE
#endif
	fsub	f17, f1,  f9
	fsub	f18, f2,  f10
	fsub	f19, f3,  f11
	fsub	f20, f4,  f12
	fsub	f21, f5,  f13
	fsub	f22, f6,  f14
	fsub	f23, f7,  f15

	fsel	f0,  f16, f8,  f0
	fabs	f8,  f24
	LFDUX	f24,   X, INCX
	fsel	f1,  f17, f9,  f1
	fabs	f9,  f25
	LFDUX	f25,   X, INCX
	fsel	f2,  f18, f10, f2
	fabs	f10, f26
	LFDUX	f26,   X, INCX
	fsel	f3,  f19, f11, f3
	fabs	f11, f27
	LFDUX	f27,   X, INCX

	fsel	f4,  f20, f12, f4
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	X, PRE
#endif
	fabs	f12, f28
	LFDUX	f28,   X, INCX
	fsel	f5,  f21, f13, f5
	fabs	f13, f29
	LFDUX	f29,   X, INCX
	fsel	f6,  f22, f14, f6
	fabs	f14, f30
	LFDUX	f30,   X, INCX
	fsel	f7,  f23, f15, f7
	fabs	f15, f31
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

	fsel	f0,  f16, f8,  f0
	fabs	f8,  f24
	fsel	f1,  f17, f9,  f1
	fabs	f9,  f25
	fsel	f2,  f18, f10, f2
	fabs	f10, f26
	fsel	f3,  f19, f11, f3
	fabs	f11, f27

	fsel	f4,  f20, f12, f4
	fabs	f12, f28
	fsel	f5,  f21, f13, f5
	fabs	f13, f29
	fsel	f6,  f22, f14, f6
	fabs	f14, f30
	fsel	f7,  f23, f15, f7
	fabs	f15, f31

	fsub	f16, f0,  f8
	fsub	f17, f1,  f9
	fsub	f18, f2,  f10
	fsub	f19, f3,  f11
	fsub	f20, f4,  f12
	fsub	f21, f5,  f13
	fsub	f22, f6,  f14
	fsub	f23, f7,  f15

	fsel	f0,  f16, f8,  f0
	fsel	f1,  f17, f9,  f1
	fsel	f2,  f18, f10, f2
	fsel	f3,  f19, f11, f3
	fsel	f4,  f20, f12, f4
	fsel	f5,  f21, f13, f5
	fsel	f6,  f22, f14, f6
	fsel	f7,  f23, f15, f7
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
	fsel	f1, f16, f8, f1
	bdnz	LL(160)
	.align 4

LL(999):
	fsub	f8,  f0,  f1
	fsub	f9,  f2,  f3
	fsub	f10, f4,  f5
	fsub	f11, f6,  f7

	fsel	f0,  f8,  f1, f0
	fsel	f2,  f9,  f3, f2
	fsel	f4,  f10, f5, f4
	fsel	f6,  f11, f7, f6

	fsub	f8,  f0,  f2
	fsub	f9,  f4,  f6
	fsel	f0,  f8,  f2, f0
	fsel	f4,  f9,  f6, f4

	fsub	f8,  f0,  f4
	fsel	f1,  f8,  f4, f0
	.align 4

LL(1000):
	srawi.	r0, NN, 3
	mtspr	CTR,  r0
	beq-	LL(1150)

	LFDUX	f24,   XX, INCX
	LFDUX	f25,   XX, INCX
	LFDUX	f26,   XX, INCX
	LFDUX	f27,   XX, INCX
	LFDUX	f28,   XX, INCX
	LFDUX	f29,   XX, INCX
	LFDUX	f30,   XX, INCX
	LFDUX	f31,   XX, INCX
	bdz	LL(1120)
	.align 4

LL(1110):
	fabs	f8,  f24
	LFDUX	f24,   XX, INCX
	fabs	f9,  f25
	LFDUX	f25,   XX, INCX
	fabs	f10, f26
	LFDUX	f26,   XX, INCX
	fabs	f11, f27
	LFDUX	f27,   XX, INCX

#ifdef PPCG4
	dcbt	XX, PRE
#endif

	fabs	f12, f28
	LFDUX	f28,   XX, INCX
	fabs	f13, f29
	LFDUX	f29,   XX, INCX

	fabs	f14, f30
	LFDUX	f30,   XX, INCX
	fabs	f15, f31
	LFDUX	f31,   XX, INCX

#if defined(PPCG4) && defined(DOUBLE)
	dcbt	XX, PRE
#endif

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

	bdnz	LL(1110)
	.align 4

LL(1120):
	fabs	f8,  f24
	fabs	f9,  f25
	fabs	f10, f26
	fabs	f11, f27

	fabs	f12, f28
	fabs	f13, f29
	fabs	f14, f30
	fabs	f15, f31

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
	fabs	f8, f8
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
