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

#define INCX2	r6
#define X2	r7

#define XX	r8

#define C1	f1
#define C2	f0
#define C3	f2
#define C4	f3

#define ALPHA	f4
#define ALPHA_R	f5

#define A1	f6
#define A2	f7
#define A3	f8
#define A4	f9
#define A5	f10
#define A6	f11
#define A7	f12
#define A8	f13

#define F1	f14
#define F2	f15
#define F3	f16
#define F4	f17
#define F5	f18
#define F6	f19
#define F7	f20
#define F8	f21

#define T1	f22
#define T2	f23
#define T3	f24
#define T4	f25
#define T5	f26
#define T6	f27
#define T7	f28
#define T8	f29


	PROLOGUE
	PROFCODE

	li	r10, -16

	stfpdux	f14, SP, r10
	stfpdux	f15, SP, r10

	stfpdux	f16, SP, r10
	stfpdux	f17, SP, r10
	stfpdux	f18, SP, r10
	stfpdux	f19, SP, r10

	stfpdux	f20, SP, r10
	stfpdux	f21, SP, r10
	stfpdux	f22, SP, r10
	stfpdux	f23, SP, r10

	stfpdux	f24, SP, r10
	stfpdux	f25, SP, r10
	stfpdux	f26, SP, r10
	stfpdux	f27, SP, r10

	stfpdux	f28, SP, r10
	stfpdux	f29, SP, r10

	li	r10,   0
	lis	r11, 0x3f80
	stwu	r11,   -4(SP)
	stwu	r11,   -4(SP)
	stwu	r10,   -4(SP)
	stwu	r10,   -4(SP)

#ifdef F_INTERFACE
	LDINT	N,    0(N)
	LDINT	INCX, 0(INCX)
#endif

	lfpsx	C1, SP, r10		# Zero clear

	slwi	INCX,  INCX, BASE_SHIFT
	add	INCX2, INCX, INCX

	fpmr	C2, C1
	fpmr	C3, C1
	fpmr	C4, C1

	cmpwi	cr0, N, 0
	ble	LL(99)
	cmpwi	cr0, INCX, 0
	ble	LL(99)

	mr	XX, X

	andi.	r0, X, 2 * SIZE - 1
	bne	LL(100)

/* aligned */

	sub	X, X, INCX2

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(15)

	LFPDUX	A1,    X, INCX2
	LFPDUX	A2,    X, INCX2
	LFPDUX	A3,    X, INCX2
	LFPDUX	A4,    X, INCX2

	LFPDUX	A5,    X, INCX2
	fpabs	T1, A1
	LFPDUX	A6,    X, INCX2
	fpabs	T2, A2
	LFPDUX	A7,    X, INCX2
	fpabs	T3, A3
	LFPDUX	A8,    X, INCX2
	fpabs	T4, A4
	bdz	LL(13)
	.align 4

LL(12):
	fpsub	F1, C1, T1
	LFPDUX	A1,    X, INCX2
	fpsub	F2, C2, T2
	LFPDUX	A2,    X, INCX2
	fpsub	F3, C3, T3
	LFPDUX	A3,    X, INCX2
	fpsub	F4, C4, T4
	LFPDUX	A4,    X, INCX2

	fpabs	T5, A5
	fpabs	T6, A6
	fpabs	T7, A7
	fpabs	T8, A8

	fpsel	C1, F1, C1, T1
	LFPDUX	A5,    X, INCX2
	fpsel	C2, F2, C2, T2
	LFPDUX	A6,    X, INCX2
	fpsel	C3, F3, C3, T3
	LFPDUX	A7,    X, INCX2
	fpsel	C4, F4, C4, T4
	LFPDUX	A8,    X, INCX2

	fpsub	F5, C1, T5
	fpsub	F6, C2, T6
	fpsub	F7, C3, T7
	fpsub	F8, C4, T8

	fpabs	T1, A1
	fpabs	T2, A2
	fpabs	T3, A3
	fpabs	T4, A4

	fpsel	C1, F5, C1, T5
	fpsel	C2, F6, C2, T6
	fpsel	C3, F7, C3, T7
	fpsel	C4, F8, C4, T8
	bdnz	LL(12)
	.align 4

LL(13):
	fpabs	T5, A5
	fpabs	T6, A6
	fpabs	T7, A7
	fpabs	T8, A8

	fpsub	F1, C1, T1
	fpsub	F2, C2, T2
	fpsub	F3, C3, T3
	fpsub	F4, C4, T4

	fpsel	C1, F1, C1, T1
	fpsel	C2, F2, C2, T2
	fpsel	C3, F3, C3, T3
	fpsel	C4, F4, C4, T4

	fpsub	F5, C1, T5
	fpsub	F6, C2, T6
	fpsub	F7, C3, T7
	fpsub	F8, C4, T8

	fpsel	C1, F5, C1, T5
	fpsel	C2, F6, C2, T6
	fpsel	C3, F7, C3, T7
	fpsel	C4, F8, C4, T8
	.align 4

LL(15):
	andi.	r0,  N, 7
	beq	LL(20)

	andi.	r0,  N, 4
	beq	LL(16)

	LFPDUX	A1,    X, INCX2
	LFPDUX	A2,    X, INCX2
	LFPDUX	A3,    X, INCX2
	LFPDUX	A4,    X, INCX2

	fpabs	A1, A1
	fpabs	A2, A2
	fpabs	A3, A3
	fpabs	A4, A4

	fpsub	F1, C1, A1
	fpsub	F2, C2, A2
	fpsub	F3, C3, A3
	fpsub	F4, C4, A4

	fpsel	C1, F1, C1, A1
	fpsel	C2, F2, C2, A2
	fpsel	C3, F3, C3, A3
	fpsel	C4, F4, C4, A4
	.align 4

LL(16):
	andi.	r0,  N, 2
	beq	LL(17)

	LFPDUX	A1,    X, INCX2
	LFPDUX	A2,    X, INCX2

	fpabs	A1, A1
	fpabs	A2, A2

	fpsub	F1, C1, A1
	fpsub	F2, C2, A2

	fpsel	C1, F1, C1, A1
	fpsel	C2, F2, C2, A2
	.align 4

LL(17):
	andi.	r0,  N, 1
	beq	LL(20)

	LFPDUX	A1,    X, INCX2
	fpabs	A1, A1
	fpsub	F1, C1, A1
	fpsel	C1, F1, C1, A1
	.align 4

LL(20):
	fpsub	F1,  C1,  C2
	fpsub	F2,  C3,  C4

	fpsel	C1,  F1,  C1,  C2
	fpsel	C3,  F2,  C3,  C4

	fpsub	F1,  C1,  C3
	fpsel	C1,  F1,  C1,  C3

	fsmtp	C2, C1

	fsub	F1,  C1,  C2
	fsel	ALPHA,  F1,  C1,  C2

	li	r10, 0

	lfs	ALPHA_R,  8(SP)		# load 1.0
	fdiv	ALPHA_R, ALPHA_R, ALPHA

	lfpsx	C1, SP, r10		# Zero clear

	fpmr	C2, C1
	fpmr	C3, C1
	fpmr	C4, C1
	fsmfp	ALPHA_R, ALPHA_R

	andi.	r0, XX, 2 * SIZE - 1
	beq	LL(21)

	LFD	C1, 0 * SIZE(XX)
	add	XX, XX, INCX

	cmpwi	cr0, N, 0
	fmul	C1, ALPHA_R, C1
	fmul	C1, C1, C1
	ble	LL(98)
	.align 4

LL(21):
	sub	XX, XX, INCX2

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(25)

	LFPDUX	A1,    XX, INCX2
	LFPDUX	A2,    XX, INCX2
	LFPDUX	A3,    XX, INCX2
	LFPDUX	A4,    XX, INCX2

	LFPDUX	A5,    XX, INCX2
	LFPDUX	A6,    XX, INCX2
	LFPDUX	A7,    XX, INCX2
	LFPDUX	A8,    XX, INCX2

	fpmul	T1, ALPHA_R, A1
	fpmul	T2, ALPHA_R, A2
	fpmul	T3, ALPHA_R, A3
	fpmul	T4, ALPHA_R, A4

	bdz	LL(23)
	.align 4

LL(22):
	fpmadd	C1, T1, T1, C1
	LFPDUX	A1,    XX, INCX2
	fpmul	T1, ALPHA_R, A5
	LFPDUX	A2,    XX, INCX2

	fpmadd	C2, T2, T2, C2
	LFPDUX	A3,    XX, INCX2
	fpmul	T2, ALPHA_R, A6
	LFPDUX	A4,    XX, INCX2

	fpmadd	C3, T3, T3, C3
	fpmul	T3, ALPHA_R, A7
	fpmadd	C4, T4, T4, C4
	fpmul	T4, ALPHA_R, A8

	fpmadd	C1, T1, T1, C1
	LFPDUX	A5,    XX, INCX2
	fpmul	T1, ALPHA_R, A1
	LFPDUX	A6,    XX, INCX2

	fpmadd	C2, T2, T2, C2
	LFPDUX	A7,    XX, INCX2
	fpmul	T2, ALPHA_R, A2
	LFPDUX	A8,    XX, INCX2

	fpmadd	C3, T3, T3, C3
	fpmul	T3, ALPHA_R, A3
	fpmadd	C4, T4, T4, C4
	fpmul	T4, ALPHA_R, A4
	bdnz	LL(22)
	.align 4

LL(23):
	fpmadd	C1, T1, T1, C1
	fpmul	T1, ALPHA_R, A5
	fpmadd	C2, T2, T2, C2
	fpmul	T2, ALPHA_R, A6

	fpmadd	C3, T3, T3, C3
	fpmul	T3, ALPHA_R, A7
	fpmadd	C4, T4, T4, C4
	fpmul	T4, ALPHA_R, A8

	fpmadd	C1, T1, T1, C1
	fpmadd	C2, T2, T2, C2
	fpmadd	C3, T3, T3, C3
	fpmadd	C4, T4, T4, C4
	.align 4

LL(25):
	andi.	r0,  N, 7
	beq	LL(98)

	andi.	r0,  N, 4
	beq	LL(26)

	LFPDUX	A1,    XX, INCX2
	LFPDUX	A2,    XX, INCX2
	LFPDUX	A3,    XX, INCX2
	LFPDUX	A4,    XX, INCX2

	fpmul	A1, ALPHA_R, A1
	fpmul	A2, ALPHA_R, A2
	fpmul	A3, ALPHA_R, A3
	fpmul	A4, ALPHA_R, A4

	fpmadd	C1, A1, A1, C1
	fpmadd	C2, A2, A2, C2
	fpmadd	C3, A3, A3, C3
	fpmadd	C4, A4, A4, C4
	.align 4

LL(26):
	andi.	r0,  N, 2
	beq	LL(27)

	LFPDUX	A1,    XX, INCX2
	LFPDUX	A2,    XX, INCX2
	fpmul	A1, ALPHA_R, A1
	fpmul	A2, ALPHA_R, A2

	fpmadd	C1, A1, A1, C1
	fpmadd	C2, A2, A2, C2
	.align 4

LL(27):
	andi.	r0,  N, 1
	beq	LL(98)

	LFPDUX	A1,    XX, INCX2
	fpmul	A1, ALPHA_R, A1
	fpmadd	C1, A1, A1, C1
	.align 4

LL(98):
	fpadd	C1,  C1,  C2
	lis	r3, 0x3f00
	fpadd	C3,  C3,  C4
	lis	r4, 0x4040

	stw	r3, 4(SP)
	stw	r4, 8(SP)

	fpadd	C1,  C1,  C3
	lfs	f10, 0(SP)

	fsmtp	C2, C1
	lfs	f11, 4(SP)
	fadd	C1, C2, C1
 	lfs	f12, 8(SP)

	fcmpu	cr0, f10, C1
	beq	cr0, LL(99)

#ifndef HUMMER_EMULATOR
	frsqrte	f9, C1
	li	r10, 16

	fmul	f2,  f1,  f9
	lfpdux	f29, SP, r10
	fmul	f3,  f9,  f11
	lfpdux	f28, SP, r10
	fnmsub	f7,  f2,  f9, f12
	lfpdux	f27, SP, r10
	fmul	f9,  f3,  f7
	lfpdux	f26, SP, r10
	fadd	f13, f11, f11
	lfpdux	f25, SP, r10
	fmul	f12, f1,  f9
	lfpdux	f24, SP, r10
	fmul	f11, f12, f11
	lfpdux	f23, SP, r10
	lfpdux	f22, SP, r10
	lfpdux	f21, SP, r10
	fnmsub	f1,  f12, f9, f13

	lfpdux	f20, SP, r10
	lfpdux	f19, SP, r10
	lfpdux	f18, SP, r10
	fmadd	f1,  f11, f1, f12
	lfpdux	f17, SP, r10
	lfpdux	f16, SP, r10

	lfpdux	f15, SP, r10
	lfpdux	f14, SP, r10
	addi	SP, SP,  16
	fmul	C1, ALPHA, C1
	blr
#else
	fsqrt	C1, C1

	li	r10, 16
	lfpdux	f29, SP, r10
	lfpdux	f28, SP, r10
	lfpdux	f27, SP, r10
	lfpdux	f26, SP, r10
	lfpdux	f25, SP, r10
	lfpdux	f24, SP, r10
	lfpdux	f23, SP, r10
	lfpdux	f22, SP, r10
	lfpdux	f21, SP, r10
	lfpdux	f20, SP, r10
	lfpdux	f19, SP, r10
	lfpdux	f18, SP, r10
	lfpdux	f17, SP, r10
	lfpdux	f16, SP, r10

	lfpdux	f15, SP, r10
	lfpdux	f14, SP, r10

	fmul	C1, ALPHA, C1
	addi	SP, SP,  16
	blr
#endif
	.align 4

LL(99):
	li	r10, 16

	lfpdux	f29, SP, r10
	lfpdux	f28, SP, r10
	lfpdux	f27, SP, r10
	lfpdux	f26, SP, r10
	lfpdux	f25, SP, r10
	lfpdux	f24, SP, r10

	lfpdux	f23, SP, r10
	lfpdux	f22, SP, r10
	lfpdux	f21, SP, r10
	lfpdux	f20, SP, r10

	lfpdux	f19, SP, r10
	lfpdux	f18, SP, r10
	lfpdux	f17, SP, r10
	lfpdux	f16, SP, r10

	lfpdux	f15, SP, r10
	lfpdux	f14, SP, r10
	addi	SP, SP,  16
	blr
	.align 4

LL(100):
	sub	X,  X,  INCX2
	addi	X2, X, SIZE

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(105)

	LFDUX	A1,   X,  INCX2
	LFDUX	A2,   X2, INCX2
	LFDUX	A3,   X,  INCX2
	LFDUX	A4,   X2, INCX2
	LFSDUX	A1,   X,  INCX2
	LFSDUX	A2,   X2, INCX2
	LFSDUX	A3,   X,  INCX2
	LFSDUX	A4,   X2, INCX2

	LFDUX	A5,   X,  INCX2
	LFDUX	A6,   X2, INCX2
	LFDUX	A7,   X,  INCX2
	LFDUX	A8,   X2, INCX2
	LFSDUX	A5,   X,  INCX2
	fpabs	T1, A1
	LFSDUX	A6,   X2, INCX2
	fpabs	T2, A2
	LFSDUX	A7,   X,  INCX2
	fpabs	T3, A3
	LFSDUX	A8,   X2, INCX2
	fpabs	T4, A4
	bdz	LL(103)
	.align 4

LL(102):
	fpsub	F1, C1, T1
	LFDUX	A1,   X,  INCX2
	fpsub	F2, C2, T2
	LFDUX	A2,   X2, INCX2
	fpsub	F3, C3, T3
	LFDUX	A3,   X,  INCX2
	fpsub	F4, C4, T4
	LFDUX	A4,   X2, INCX2

	fpabs	T5, A5
	LFSDUX	A1,   X,  INCX2
	fpabs	T6, A6
	LFSDUX	A2,   X2, INCX2
	fpabs	T7, A7
	LFSDUX	A3,   X,  INCX2
	fpabs	T8, A8
	LFSDUX	A4,   X2, INCX2

	fpsel	C1, F1, C1, T1
	LFDUX	A5,   X,  INCX2
	fpsel	C2, F2, C2, T2
	LFDUX	A6,   X2, INCX2
	fpsel	C3, F3, C3, T3
	LFDUX	A7,   X,  INCX2
	fpsel	C4, F4, C4, T4
	LFDUX	A8,   X2, INCX2

	fpsub	F5, C1, T5
	LFSDUX	A5,   X,  INCX2
	fpsub	F6, C2, T6
	LFSDUX	A6,   X2, INCX2
	fpsub	F7, C3, T7
	LFSDUX	A7,   X,  INCX2
	fpsub	F8, C4, T8
	LFSDUX	A8,   X2, INCX2

	fpabs	T1, A1
	fpabs	T2, A2
	fpabs	T3, A3
	fpabs	T4, A4

	fpsel	C1, F5, C1, T5
	fpsel	C2, F6, C2, T6
	fpsel	C3, F7, C3, T7
	fpsel	C4, F8, C4, T8
	bdnz	LL(102)
	.align 4

LL(103):
	fpabs	T5, A5
	fpabs	T6, A6
	fpabs	T7, A7
	fpabs	T8, A8

	fpsub	F1, C1, T1
	fpsub	F2, C2, T2
	fpsub	F3, C3, T3
	fpsub	F4, C4, T4

	fpsel	C1, F1, C1, T1
	fpsel	C2, F2, C2, T2
	fpsel	C3, F3, C3, T3
	fpsel	C4, F4, C4, T4

	fpsub	F5, C1, T5
	fpsub	F6, C2, T6
	fpsub	F7, C3, T7
	fpsub	F8, C4, T8

	fpsel	C1, F5, C1, T5
	fpsel	C2, F6, C2, T6
	fpsel	C3, F7, C3, T7
	fpsel	C4, F8, C4, T8
	.align 4

LL(105):
	andi.	r0,  N, 7
	beq	LL(120)

	andi.	r0,  N, 4
	beq	LL(106)

	LFDUX	A1,   X,  INCX2
	LFDUX	A2,   X2, INCX2
	LFDUX	A3,   X,  INCX2
	LFDUX	A4,   X2, INCX2
	LFSDUX	A1,   X,  INCX2
	LFSDUX	A2,   X2, INCX2
	LFSDUX	A3,   X,  INCX2
	LFSDUX	A4,   X2, INCX2

	fpabs	A1, A1
	fpabs	A2, A2
	fpabs	A3, A3
	fpabs	A4, A4

	fpsub	F1, C1, A1
	fpsub	F2, C2, A2
	fpsub	F3, C3, A3
	fpsub	F4, C4, A4

	fpsel	C1, F1, C1, A1
	fpsel	C2, F2, C2, A2
	fpsel	C3, F3, C3, A3
	fpsel	C4, F4, C4, A4
	.align 4

LL(106):
	andi.	r0,  N, 2
	beq	LL(107)

	LFDUX	A1,   X,  INCX2
	LFDUX	A2,   X2, INCX2
	LFDUX	A3,   X,  INCX2
	LFDUX	A4,   X2, INCX2

	fabs	A1, A1
	fabs	A2, A2
	fabs	A3, A3
	fabs	A4, A4

	fsub	F1, C1, A1
	fsub	F2, C2, A2
	fsub	F3, C3, A3
	fsub	F4, C4, A4

	fsel	C1, F1, C1, A1
	fsel	C2, F2, C2, A2
	fsel	C3, F3, C3, A3
	fsel	C4, F4, C4, A4
	.align 4

LL(107):
	andi.	r0,  N, 1
	beq	LL(120)

	LFDUX	A1,   X,  INCX2
	LFDUX	A2,   X2, INCX2
	fabs	A1, A1
	fabs	A2, A2
	fsub	F1, C1, A1
	fsub	F2, C2, A2
	fsel	C1, F1, C1, A1
	fsel	C2, F2, C2, A2
	.align 4

LL(120):
	fpsub	F1,  C1,  C2
	fpsub	F2,  C3,  C4

	fpsel	C1,  F1,  C1,  C2
	fpsel	C3,  F2,  C3,  C4

	fpsub	F1,  C1,  C3
	fpsel	C1,  F1,  C1,  C3

	fsmtp	C2, C1

	fsub	F1,  C1,  C2
	fsel	ALPHA,  F1,  C1,  C2

	li	r10, 0

	lfs	ALPHA_R,  8(SP)		# load 1.0
	fdiv	ALPHA_R, ALPHA_R, ALPHA

	lfpsx	C1, SP, r10		# Zero clear

	fpmr	C2, C1
	fpmr	C3, C1
	fpmr	C4, C1
	fsmfp	ALPHA_R, ALPHA_R

	sub	XX, XX, INCX2
	addi	X2, XX, SIZE

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(125)

	LFDUX	A1,    XX, INCX2
	LFDUX	A2,    X2, INCX2
	LFDUX	A3,    XX, INCX2
	LFDUX	A4,    X2, INCX2
	LFSDUX	A1,    XX, INCX2
	LFSDUX	A2,    X2, INCX2
	LFSDUX	A3,    XX, INCX2
	LFSDUX	A4,    X2, INCX2

	LFDUX	A5,    XX, INCX2
	LFDUX	A6,    X2, INCX2
	LFDUX	A7,    XX, INCX2
	LFDUX	A8,    X2, INCX2
	LFSDUX	A5,    XX, INCX2
	fpmul	T1, ALPHA_R, A1
	LFSDUX	A6,    X2, INCX2
	fpmul	T2, ALPHA_R, A2
	LFSDUX	A7,    XX, INCX2
	fpmul	T3, ALPHA_R, A3
	LFSDUX	A8,    X2, INCX2
	fpmul	T4, ALPHA_R, A4
	bdz	LL(123)
	.align 4

LL(122):
	fpmadd	C1, T1, T1, C1
	LFDUX	A1,    XX, INCX2
	fpmul	T1, ALPHA_R, A5
	LFDUX	A2,    X2, INCX2

	fpmadd	C2, T2, T2, C2
	LFDUX	A3,    XX, INCX2
	fpmul	T2, ALPHA_R, A6
	LFDUX	A4,    X2, INCX2

	fpmadd	C3, T3, T3, C3
	LFSDUX	A1,    XX, INCX2
	fpmul	T3, ALPHA_R, A7
	LFSDUX	A2,    X2, INCX2

	fpmadd	C4, T4, T4, C4
	LFSDUX	A3,    XX, INCX2
	fpmul	T4, ALPHA_R, A8
	LFSDUX	A4,    X2, INCX2

	fpmadd	C1, T1, T1, C1
	LFDUX	A5,    XX, INCX2
	fpmul	T1, ALPHA_R, A1
	LFDUX	A6,    X2, INCX2

	fpmadd	C2, T2, T2, C2
	LFDUX	A7,    XX, INCX2
	fpmul	T2, ALPHA_R, A2
	LFDUX	A8,    X2, INCX2

	fpmadd	C3, T3, T3, C3
	LFSDUX	A5,    XX, INCX2
	fpmul	T3, ALPHA_R, A3
	LFSDUX	A6,    X2, INCX2
	fpmadd	C4, T4, T4, C4
	LFSDUX	A7,    XX, INCX2
	fpmul	T4, ALPHA_R, A4
	LFSDUX	A8,    X2, INCX2
	bdnz	LL(122)
	.align 4

LL(123):
	fpmadd	C1, T1, T1, C1
	fpmul	T1, ALPHA_R, A5
	fpmadd	C2, T2, T2, C2
	fpmul	T2, ALPHA_R, A6
	fpmadd	C3, T3, T3, C3
	fpmul	T3, ALPHA_R, A7
	fpmadd	C4, T4, T4, C4
	fpmul	T4, ALPHA_R, A8

	fpmadd	C1, T1, T1, C1
	fpmadd	C2, T2, T2, C2
	fpmadd	C3, T3, T3, C3
	fpmadd	C4, T4, T4, C4
	.align 4

LL(125):
	andi.	r0,  N, 7
	beq	LL(998)

	andi.	r0,  N, 4
	beq	LL(126)

	LFDUX	A1,    XX, INCX2
	LFDUX	A2,    X2, INCX2
	LFDUX	A3,    XX, INCX2
	LFDUX	A4,    X2, INCX2
	LFSDUX	A1,    XX, INCX2
	LFSDUX	A2,    X2, INCX2
	LFSDUX	A3,    XX, INCX2
	LFSDUX	A4,    X2, INCX2

	fpmul	A1, ALPHA_R, A1
	fpmul	A2, ALPHA_R, A2
	fpmul	A3, ALPHA_R, A3
	fpmul	A4, ALPHA_R, A4

	fpmadd	C1, A1, A1, C1
	fpmadd	C2, A2, A2, C2
	fpmadd	C3, A3, A3, C3
	fpmadd	C4, A4, A4, C4
	.align 4

LL(126):
	andi.	r0,  N, 2
	beq	LL(127)

	LFDUX	A1,    XX, INCX2
	LFDUX	A2,    X2, INCX2
	LFDUX	A3,    XX, INCX2
	LFDUX	A4,    X2, INCX2

	fmul	A1, ALPHA_R, A1
	fmul	A2, ALPHA_R, A2
	fmul	A3, ALPHA_R, A3
	fmul	A4, ALPHA_R, A4

	fmadd	C1, A1, A1, C1
	fmadd	C2, A2, A2, C2
	fmadd	C3, A3, A3, C3
	fmadd	C4, A4, A4, C4
	.align 4

LL(127):
	andi.	r0,  N, 1
	beq	LL(998)

	LFDUX	A1,    XX, INCX2
	LFDUX	A2,    X2, INCX2

	fmul	A1, ALPHA_R, A1
	fmul	A2, ALPHA_R, A2
	fmadd	C1, A1, A1, C1
	fmadd	C2, A2, A2, C2
	.align 4

LL(998):
	fpadd	C1,  C1,  C2
	lis	r3, 0x3f00
 	fpadd	C3,  C3,  C4
	lis	r4, 0x4040

	stw	r3, 4(SP)
	stw	r4, 8(SP)

	fpadd	C1,  C1,  C3
	lfs	f10, 0(SP)
	fsmtp	C2, C1
	lfs	f11, 4(SP)
	fadd	C1, C2, C1
 	lfs	f12, 8(SP)

	fcmpu	cr0, f10, C1
	beq	cr0, LL(99)

#ifndef HUMMER_EMULATOR
	frsqrte	f9, C1
	li	r10, 16

	fmul	f2,  f1,  f9
	lfpdux	f29, SP, r10
	fmul	f3,  f9,  f11
	lfpdux	f28, SP, r10
	fnmsub	f7,  f2,  f9, f12
	lfpdux	f27, SP, r10
	fmul	f9,  f3,  f7
	lfpdux	f26, SP, r10
	fadd	f13, f11, f11
	lfpdux	f25, SP, r10
	fmul	f12, f1,  f9
	lfpdux	f24, SP, r10
	fmul	f11, f12, f11
	lfpdux	f23, SP, r10
	lfpdux	f22, SP, r10
	fnmsub	f1,  f12, f9, f13
	lfpdux	f21, SP, r10

	lfpdux	f20, SP, r10
	lfpdux	f19, SP, r10
	lfpdux	f18, SP, r10
	fmadd	f1,  f11, f1, f12
	lfpdux	f17, SP, r10
	lfpdux	f16, SP, r10

	lfpdux	f15, SP, r10
	lfpdux	f14, SP, r10
	addi	SP, SP,  16
	fmul	C1, ALPHA, C1
	blr
#else
	fsqrt	C1, C1
	li	r10, 16

	lfpdux	f29, SP, r10
	lfpdux	f28, SP, r10
	lfpdux	f27, SP, r10
	lfpdux	f26, SP, r10
	lfpdux	f25, SP, r10
	lfpdux	f24, SP, r10
	lfpdux	f23, SP, r10
	lfpdux	f22, SP, r10
	lfpdux	f21, SP, r10

	lfpdux	f20, SP, r10
	lfpdux	f19, SP, r10
	lfpdux	f18, SP, r10
	lfpdux	f17, SP, r10
	lfpdux	f16, SP, r10

	lfpdux	f15, SP, r10
	lfpdux	f14, SP, r10
	fmul	C1, ALPHA, C1
	addi	SP, SP,  16
	blr
#endif
	.align 4

LL(999):
	li	r10, 16

	lfpdux	f29, SP, r10
	lfpdux	f28, SP, r10
	lfpdux	f27, SP, r10
	lfpdux	f26, SP, r10
	lfpdux	f25, SP, r10
	lfpdux	f24, SP, r10

	lfpdux	f23, SP, r10
	lfpdux	f22, SP, r10
	lfpdux	f21, SP, r10
	lfpdux	f20, SP, r10

	lfpdux	f19, SP, r10
	lfpdux	f18, SP, r10
	lfpdux	f17, SP, r10
	lfpdux	f16, SP, r10

	lfpdux	f15, SP, r10
	lfpdux	f14, SP, r10
	addi	SP, SP,  16
	blr
	EPILOGUE
