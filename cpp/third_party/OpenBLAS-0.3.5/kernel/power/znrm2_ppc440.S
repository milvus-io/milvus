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
#define INC1	r9
#define PRE	r10

#define FZERO	144(SP)
#define FONE	148(SP)
#define FMAX	152(SP)
#define C1	156(SP)
#define C2	160(SP)

#define STACKSIZE 168

	PROLOGUE
	PROFCODE

	addi	SP, SP, -STACKSIZE
	li	r10,   0
	lis	r11,   0x3f80
	lis	r12,   0x5fe0
	lis	r6,    0x3f00
	lis	r7,    0x4040

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
	stw	r6,   C1
	stw	r7,   C2

	lfs	f1,   FZERO

#ifdef F_INTERFACE
	LDINT	N,    0(N)
	LDINT	INCX, 0(INCX)
#endif

	slwi	INCX, INCX, ZBASE_SHIFT
	sub	X, X, INCX
	li	INC1, SIZE

	li	PRE, 3 * 16 * SIZE

	cmpwi	cr0, N, 0
	ble-	LL(999)
	cmpwi	cr0, INCX, 0
	ble-	LL(999)

	mr	NN, N
	mr	XX, X

	LFDUX	f0, X, INCX
	LFDX	f1, X, INC1

	fabs	f2, f0
	fabs	f3, f1
	fabs	f4, f0
	fabs	f5, f1
	fabs	f6, f0
	fabs	f7, f1
	fabs	f0, f0
	fabs	f1, f1

	subi	N, N, 1

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(50)

	LFDUX	f24,   X, INCX
	LFDX	f25,   X, INC1
	LFDUX	f26,   X, INCX
	LFDX	f27,   X, INC1
	LFDUX	f28,   X, INCX
	LFDX	f29,   X, INC1
	LFDUX	f30,   X, INCX
	LFDX	f31,   X, INC1

	fabs	f8,  f24
	LFDUX	f24,   X, INCX
	fabs	f9,  f25
	LFDX	f25,   X, INC1
	fabs	f10, f26
	LFDUX	f26,   X, INCX
	fabs	f11, f27
	LFDX	f27,   X, INC1

	fabs	f12, f28
	LFDUX	f28,   X, INCX
	fabs	f13, f29
	LFDX	f29,   X, INC1
	fabs	f14, f30
	LFDUX	f30,   X, INCX
	fabs	f15, f31
	LFDX	f31,   X, INC1
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
#ifdef PPCG4
	dcbt	X, PRE
#endif
	fabs	f8,  f24
	LFDUX	f24,   X, INCX
	fsel	f1,  f17, f1,  f9
	fabs	f9,  f25
	LFDX	f25,   X, INC1
	fsel	f2,  f18, f2,  f10
	fabs	f10, f26
	LFDUX	f26,   X, INCX
	fsel	f3,  f19, f3,  f11
	fabs	f11, f27
	LFDX	f27,   X, INC1

	fsel	f4,  f20, f4,  f12
#ifdef PPCG4
	dcbt	X, PRE
#endif
	fabs	f12, f28
	LFDUX	f28,   X, INCX
	fsel	f5,  f21, f5,  f13
	fabs	f13, f29
	LFDX	f29,   X, INC1
	fsel	f6,  f22, f6,  f14
	fabs	f14, f30
	LFDUX	f30,   X, INCX
	fsel	f7,  f23, f7,  f15
	fabs	f15, f31
	LFDX	f31,   X, INC1

	fsub	f16, f0,  f8
	fsub	f17, f1,  f9
	fsub	f18, f2,  f10
	fsub	f19, f3,  f11
	fsub	f20, f4,  f12
	fsub	f21, f5,  f13
	fsub	f22, f6,  f14
	fsub	f23, f7,  f15

	fsel	f0,  f16, f0,  f8
#ifdef PPCG4
	dcbt	X, PRE
#endif
	fabs	f8,  f24
	LFDUX	f24,   X, INCX
	fsel	f1,  f17, f1,  f9
	fabs	f9,  f25
	LFDX	f25,   X, INC1
	fsel	f2,  f18, f2,  f10
	fabs	f10, f26
	LFDUX	f26,   X, INCX
	fsel	f3,  f19, f3,  f11
	fabs	f11, f27
	LFDX	f27,   X, INC1

	fsel	f4,  f20, f4,  f12
#ifdef PPCG4
	dcbt	X, PRE
#endif
	fabs	f12, f28
	LFDUX	f28,   X, INCX
	fsel	f5,  f21, f5,  f13
	fabs	f13, f29
	LFDX	f29,   X, INC1
	fsel	f6,  f22, f6,  f14
	fabs	f14, f30
	LFDUX	f30,   X, INCX
	fsel	f7,  f23, f7,  f15
	fabs	f15, f31
	LFDX	f31,   X, INC1
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
	.align 4

LL(50):
	andi.	r0,  N, 7
	mtspr	CTR, r0
	beq	LL(99)
	.align 4

LL(60):
	LFDUX	f8,    X, INCX
	LFDX	f9,    X, INC1

	fabs	f8, f8
	fabs	f9, f9
	fsub	f16, f0, f8
	fsub	f17, f1, f9
	fsel	f0, f16, f0, f8
	fsel	f1, f17, f1, f9
	bdnz	LL(60)
	.align 4

LL(99):
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
	beq-	cr0, LL(999)

	fdiv	f30, f0, f31

	fmr	f0, f1
	fmr	f2, f1
	fmr	f3, f1
	fmr	f4, f1
	fmr	f5, f1
	fmr	f6, f1
	fmr	f7, f1

	srawi.	r0, NN, 3
	mtspr	CTR, r0
	beq-	cr0, LL(150)

	LFDUX	f8,   XX, INCX
	LFDX	f9,   XX, INC1
	LFDUX	f10,  XX, INCX
	LFDX	f11,  XX, INC1
	LFDUX	f12,  XX, INCX
	LFDX	f13,  XX, INC1
	LFDUX	f14,  XX, INCX
	LFDX	f15,  XX, INC1

	fmul	f16, f30, f8
	LFDUX	f8,  XX, INCX
	fmul	f17, f30, f9
	LFDX	f9,  XX, INC1
	fmul	f18, f30, f10
	LFDUX	f10, XX, INCX
	fmul	f19, f30, f11
	LFDX	f11, XX, INC1

	fmul	f20, f30, f12
	LFDUX	f12, XX, INCX
	fmul	f21, f30, f13
	LFDX	f13, XX, INC1
	fmul	f22, f30, f14
	LFDUX	f14, XX, INCX
	fmul	f23, f30, f15
	LFDX	f15, XX, INC1
	bdz	LL(120)
	.align 4

LL(110):
	fmadd	f0,  f16, f16, f0
#ifdef PPCG4
	dcbt	XX, PRE
#endif
	fmul	f16, f30, f8
	LFDUX	f8,   XX, INCX
	fmadd	f1,  f17, f17, f1
	fmul	f17, f30, f9
	LFDX	f9,   XX, INC1
	fmadd	f2,  f18, f18, f2
	fmul	f18, f30, f10
	LFDUX	f10,  XX, INCX
	fmadd	f3,  f19, f19, f3
	fmul	f19, f30, f11
	LFDX	f11,  XX, INC1

	fmadd	f4,  f20, f20, f4
#ifdef PPCG4
	dcbt	XX, PRE
#endif
	fmul	f20, f30, f12
	LFDUX	f12,  XX, INCX
	fmadd	f5,  f21, f21, f5
	fmul	f21, f30, f13
	LFDX	f13,  XX, INC1
	fmadd	f6,  f22, f22, f6
	fmul	f22, f30, f14
	LFDUX	f14,  XX, INCX
	fmadd	f7,  f23, f23, f7
	fmul	f23, f30, f15
	LFDX	f15,  XX, INC1

	fmadd	f0,  f16, f16, f0
#ifdef PPCG4
	dcbt	XX, PRE
#endif
	fmul	f16, f30, f8
	LFDUX	f8,   XX, INCX
	fmadd	f1,  f17, f17, f1
	fmul	f17, f30, f9
	LFDX	f9,   XX, INC1
	fmadd	f2,  f18, f18, f2
	fmul	f18, f30, f10
	LFDUX	f10,  XX, INCX
	fmadd	f3,  f19, f19, f3
	fmul	f19, f30, f11
	LFDX	f11,  XX, INC1

	fmadd	f4,  f20, f20, f4
#ifdef PPCG4
	dcbt	XX, PRE
#endif
	fmul	f20, f30, f12
	LFDUX	f12,  XX, INCX
	fmadd	f5,  f21, f21, f5
	fmul	f21, f30, f13
	LFDX	f13,  XX, INC1
	fmadd	f6,  f22, f22, f6
	fmul	f22, f30, f14
	LFDUX	f14,  XX, INCX
	fmadd	f7,  f23, f23, f7
	fmul	f23, f30, f15
	LFDX	f15,  XX, INC1
	bdnz	LL(110)
	.align 4

LL(120):
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

LL(150):
	andi.	r0,  NN, 7
	mtspr	CTR, r0
	beq-	cr0, LL(170)
	.align 4

LL(160):
	LFDUX	f8,  XX, INCX
	LFDX	f9,  XX, INC1

	fmul	f16, f30, f8
	fmul	f17, f30, f9
	fmadd	f0,  f16, f16, f0
	fmadd	f1,  f17, f17, f1
	bdnz	LL(160)
	.align 4

LL(170):
	fadd   f0, f0, f1
	fadd   f2, f2, f3
	fadd   f4, f4, f5
	fadd   f6, f6, f7

	fadd   f0, f0, f2
	fadd   f4, f4, f6

	fadd   f1, f0, f4

	frsqrte f0, f1
	lfs	f8, C1
	lfs	f9, C2

	fmul	f2, f1, f0
	fadd	f7, f8, f8
	fmul	f3, f0, f8
	fnmsub	f4, f2, f0, f9
	fmul	f0, f3, f4

	fmul	f2, f1, f0
	fmul	f3, f0, f8
	fnmsub	f4, f2, f0, f9
	fmul	f0, f3, f4

	fmul	f2, f1, f0
	fmul	f3, f0, f8
	fnmsub	f4, f2, f0, f9
	fmul	f0, f3, f4

	fmul	f5, f1, f0
	fmul	f2, f5, f8
	fnmsub	f3, f5, f0, f7
	fmadd	f1, f2, f3, f5
	fmul    f1,  f31, f1
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

	lfd	f26,   96(SP)
	lfd	f27,  104(SP)
	lfd	f28,  112(SP)
	lfd	f29,  120(SP)

	lfd	f30,  128(SP)
	lfd	f31,  136(SP)

	addi	SP, SP, STACKSIZE
	blr

	EPILOGUE
