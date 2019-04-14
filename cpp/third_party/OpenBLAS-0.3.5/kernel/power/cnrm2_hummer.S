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

#define C1	f1
#define C2	f0
#define C3	f2
#define C4	f3
#define C5	f4
#define C6	f5
#define C7	f6
#define C8	f7

#define A1	f8
#define A2	f9
#define A3	f10
#define A4	f11
#define A5	f12
#define A6	f13
#define A7	f14
#define A8	f15

#define A9	f16
#define A10	f17
#define A11	f18
#define A12	f19
#define A13	f20
#define A14	f21
#define A15	f22
#define A16	f23

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

	li	r10,   0
	stwu	r10,   -4(SP)
	stwu	r10,   -4(SP)
	stwu	r10,   -4(SP)
	stwu	r10,   -4(SP)

#ifdef F_INTERFACE
	LDINT	N,    0(N)
	LDINT	INCX, 0(INCX)
#endif

	lfpdx	C1, SP, r10		# Zero clear

	slwi	INCX,  INCX, BASE_SHIFT
	add	INCX2, INCX, INCX

	fpmr	C2, C1
	fpmr	C3, C1
	fpmr	C4, C1

	fpmr	C5, C1
	fpmr	C6, C1
	fpmr	C7, C1
	fpmr	C8, C1

	cmpwi	cr0, N, 0
	ble	LL(99)
	cmpwi	cr0, INCX, 0
	ble	LL(99)

	andi.	r0, X, 2 * SIZE - 1
	bne	LL(100)

	srawi.	r0, N, 4
	sub	X, X, INCX2
	mtspr	CTR,  r0
	beq-	LL(15)

	LFPDUX	A1,    X, INCX2
	LFPDUX	A2,    X, INCX2
	LFPDUX	A3,    X, INCX2
	LFPDUX	A4,    X, INCX2
	LFPDUX	A5,    X, INCX2
	LFPDUX	A6,    X, INCX2
	LFPDUX	A7,    X, INCX2
	LFPDUX	A8,    X, INCX2

	LFPDUX	A9,    X, INCX2
	LFPDUX	A10,   X, INCX2
	LFPDUX	A11,   X, INCX2
	LFPDUX	A12,   X, INCX2
	LFPDUX	A13,   X, INCX2
	LFPDUX	A14,   X, INCX2
	LFPDUX	A15,   X, INCX2
	LFPDUX	A16,   X, INCX2
	bdz	LL(13)
	.align 4

LL(12):
	fpmadd	C1, A1,  A1,  C1
	LFPDUX	A1,    X, INCX2
	fpmadd	C2, A2,  A2,  C2
	LFPDUX	A2,    X, INCX2
	fpmadd	C3, A3,  A3,  C3
	LFPDUX	A3,    X, INCX2
	fpmadd	C4, A4,  A4,  C4
	LFPDUX	A4,    X, INCX2

	fpmadd	C5, A5,  A5,  C5
	LFPDUX	A5,    X, INCX2
	fpmadd	C6, A6,  A6,  C6
	LFPDUX	A6,    X, INCX2
	fpmadd	C7, A7,  A7,  C7
	LFPDUX	A7,    X, INCX2
	fpmadd	C8, A8,  A8,  C8
	LFPDUX	A8,    X, INCX2

	fpmadd	C1, A9,  A9,  C1
	LFPDUX	A9,    X, INCX2
	fpmadd	C2, A10, A10, C2
	LFPDUX	A10,   X, INCX2
	fpmadd	C3, A11, A11, C3
	LFPDUX	A11,   X, INCX2
	fpmadd	C4, A12, A12, C4
	LFPDUX	A12,   X, INCX2

	fpmadd	C5, A13, A13, C5
	LFPDUX	A13,   X, INCX2
	fpmadd	C6, A14, A14, C6
	LFPDUX	A14,   X, INCX2
	fpmadd	C7, A15, A15, C7
	LFPDUX	A15,   X, INCX2
	fpmadd	C8, A16, A16, C8
	LFPDUX	A16,   X, INCX2

	bdnz	LL(12)
	.align 4

LL(13):
	fpmadd	C1, A1,  A1,  C1
	fpmadd	C2, A2,  A2,  C2
	fpmadd	C3, A3,  A3,  C3
	fpmadd	C4, A4,  A4,  C4

	fpmadd	C5, A5,  A5,  C5
	fpmadd	C6, A6,  A6,  C6
	fpmadd	C7, A7,  A7,  C7
	fpmadd	C8, A8,  A8,  C8

	fpmadd	C1, A9,  A9,  C1
	fpmadd	C2, A10, A10, C2
	fpmadd	C3, A11, A11, C3
	fpmadd	C4, A12, A12, C4

	fpmadd	C5, A13, A13, C5
	fpmadd	C6, A14, A14, C6
	fpmadd	C7, A15, A15, C7
	fpmadd	C8, A16, A16, C8
	.align 4

LL(15):
	andi.	r0,  N, 15
	beq	LL(98)

	andi.	r0,  N, 8
	beq	LL(16)

	LFPDUX	A1,   X, INCX2
	LFPDUX	A2,   X, INCX2
	LFPDUX	A3,   X, INCX2
	LFPDUX	A4,   X, INCX2
	LFPDUX	A5,   X, INCX2
	LFPDUX	A6,   X, INCX2
	LFPDUX	A7,   X, INCX2
	LFPDUX	A8,   X, INCX2

	fpmadd	C1, A1, A1, C1
	fpmadd	C2, A2, A2, C2
	fpmadd	C3, A3, A3, C3
	fpmadd	C4, A4, A4, C4

	fpmadd	C5, A5, A5, C5
	fpmadd	C6, A6, A6, C6
	fpmadd	C7, A7, A7, C7
	fpmadd	C8, A8, A8, C8
	.align 4

LL(16):
	andi.	r0,  N, 4
	beq	LL(17)

	LFPDUX	A1,    X, INCX2
	LFPDUX	A2,    X, INCX2
	LFPDUX	A3,    X, INCX2
	LFPDUX	A4,    X, INCX2

	fpmadd	C1, A1, A1, C1
	fpmadd	C2, A2, A2, C2
	fpmadd	C3, A3, A3, C3
	fpmadd	C4, A4, A4, C4
	.align 4

LL(17):
	andi.	r0,  N, 2
	beq	LL(18)

	LFPDUX	A1,    X, INCX2
	LFPDUX	A2,    X, INCX2
	fpmadd	C1, A1, A1, C1
	fpmadd	C2, A2, A2, C2
	.align 4

LL(18):
	andi.	r0,  N, 1
	beq	LL(98)

	LFPDUX	A1,    X, INCX2
	fpmadd	C3, A1, A1, C3
	.align 4

LL(98):
	fpadd	C1,  C1,  C5
	lis	r3, 0x3f00
	fpadd	C2,  C2,  C6
	lis	r4, 0x4040
	fpadd	C3,  C3,  C7
	stw	r3, 4(SP)
	fpadd	C4,  C4,  C8
	stw	r4, 8(SP)

	fpadd	C1,  C1,  C2
	lfs	f10, 0(SP)
	fpadd	C3,  C3,  C4
	lfs	f11, 4(SP)

	fpadd	C1,  C1,  C3
	lfs	f12, 8(SP)

	fsmtp	C2, C1
	fadd	C1, C2, C1

	fcmpu	cr0, f10, C1
	beq	cr0, LL(99)

#ifndef HUMMER_EMULATOR
	frsqrte	f9, f1
	li	r10, 16

	fmul	f2,  f1,  f9
	lfpdux	f23, SP, r10
	fmul	f3,  f9,  f11
	lfpdux	f22, SP, r10
	fnmsub	f4,  f2,  f9, f12
	lfpdux	f21, SP, r10
	fmul	f9,  f3,  f4
	lfpdux	f20, SP, r10
	fadd	f13, f11, f11
	lfpdux	f19, SP, r10
	fmul	f12, f1,  f9
	lfpdux	f18, SP, r10
	fmul	f11, f12, f11
	lfpdux	f17, SP, r10
	fnmsub	f1,  f12, f9, f13
	lfpdux	f16, SP, r10
	lfpdux	f15, SP, r10
	lfpdux	f14, SP, r10
	addi	SP, SP,  16
	fmadd	f1,  f11, f1, f12
	blr
#else
	fsqrt	f1, f1

	li	r10, 16
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
#endif
	.align 4

LL(99):
	li	r10, 16

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
	cmpwi	cr0, INCX, SIZE
	bne	LL(200)

	LFD	C1, 0(X)
	addi	X, X, 1 * SIZE
	addi	N, N, -1
	cmpwi	cr0, N, 0
	fmul	C1, C1, C1
	sub	X, X, INCX2
	ble	LL(198)

	srawi.	r0, N, 4
	mtspr	CTR,  r0
	beq-	LL(115)

	LFPDUX	A1,    X, INCX2
	LFPDUX	A2,    X, INCX2
	LFPDUX	A3,    X, INCX2
	LFPDUX	A4,    X, INCX2
	LFPDUX	A5,    X, INCX2
	LFPDUX	A6,    X, INCX2
	LFPDUX	A7,    X, INCX2
	LFPDUX	A8,    X, INCX2

	LFPDUX	A9,    X, INCX2
	LFPDUX	A10,   X, INCX2
	LFPDUX	A11,   X, INCX2
	LFPDUX	A12,   X, INCX2
	LFPDUX	A13,   X, INCX2
	LFPDUX	A14,   X, INCX2
	LFPDUX	A15,   X, INCX2
	LFPDUX	A16,   X, INCX2
	bdz	LL(113)
	.align 4

LL(112):
	fpmadd	C1, A1,  A1,  C1
	LFPDUX	A1,    X, INCX2
	fpmadd	C2, A2,  A2,  C2
	LFPDUX	A2,    X, INCX2
	fpmadd	C3, A3,  A3,  C3
	LFPDUX	A3,    X, INCX2
	fpmadd	C4, A4,  A4,  C4
	LFPDUX	A4,    X, INCX2

	fpmadd	C5, A5,  A5,  C5
	LFPDUX	A5,    X, INCX2
	fpmadd	C6, A6,  A6,  C6
	LFPDUX	A6,    X, INCX2
	fpmadd	C7, A7,  A7,  C7
	LFPDUX	A7,    X, INCX2
	fpmadd	C8, A8,  A8,  C8
	LFPDUX	A8,    X, INCX2

	fpmadd	C1, A9,  A9,  C1
	LFPDUX	A9,    X, INCX2
	fpmadd	C2, A10, A10, C2
	LFPDUX	A10,   X, INCX2
	fpmadd	C3, A11, A11, C3
	LFPDUX	A11,   X, INCX2
	fpmadd	C4, A12, A12, C4
	LFPDUX	A12,   X, INCX2

	fpmadd	C5, A13, A13, C5
	LFPDUX	A13,   X, INCX2
	fpmadd	C6, A14, A14, C6
	LFPDUX	A14,   X, INCX2
	fpmadd	C7, A15, A15, C7
	LFPDUX	A15,   X, INCX2
	fpmadd	C8, A16, A16, C8
	LFPDUX	A16,   X, INCX2

	bdnz	LL(112)
	.align 4

LL(113):
	fpmadd	C1, A1,  A1,  C1
	fpmadd	C2, A2,  A2,  C2
	fpmadd	C3, A3,  A3,  C3
	fpmadd	C4, A4,  A4,  C4

	fpmadd	C5, A5,  A5,  C5
	fpmadd	C6, A6,  A6,  C6
	fpmadd	C7, A7,  A7,  C7
	fpmadd	C8, A8,  A8,  C8

	fpmadd	C1, A9,  A9,  C1
	fpmadd	C2, A10, A10, C2
	fpmadd	C3, A11, A11, C3
	fpmadd	C4, A12, A12, C4

	fpmadd	C5, A13, A13, C5
	fpmadd	C6, A14, A14, C6
	fpmadd	C7, A15, A15, C7
	fpmadd	C8, A16, A16, C8
	.align 4

LL(115):
	andi.	r0,  N, 15
	beq	LL(198)

	andi.	r0,  N, 8
	beq	LL(116)

	LFPDUX	A1,   X, INCX2
	LFPDUX	A2,   X, INCX2
	LFPDUX	A3,   X, INCX2
	LFPDUX	A4,   X, INCX2
	LFPDUX	A5,   X, INCX2
	LFPDUX	A6,   X, INCX2
	LFPDUX	A7,   X, INCX2
	LFPDUX	A8,   X, INCX2

	fpmadd	C1, A1, A1, C1
	fpmadd	C2, A2, A2, C2
	fpmadd	C3, A3, A3, C3
	fpmadd	C4, A4, A4, C4

	fpmadd	C5, A5, A5, C5
	fpmadd	C6, A6, A6, C6
	fpmadd	C7, A7, A7, C7
	fpmadd	C8, A8, A8, C8
	.align 4

LL(116):
	andi.	r0,  N, 4
	beq	LL(117)

	LFPDUX	A1,    X, INCX2
	LFPDUX	A2,    X, INCX2
	LFPDUX	A3,    X, INCX2
	LFPDUX	A4,    X, INCX2

	fpmadd	C1, A1, A1, C1
	fpmadd	C2, A2, A2, C2
	fpmadd	C3, A3, A3, C3
	fpmadd	C4, A4, A4, C4
	.align 4

LL(117):
	andi.	r0,  N, 2
	beq	LL(118)

	LFPDUX	A1,    X, INCX2
	LFPDUX	A2,    X, INCX2
	fpmadd	C1, A1, A1, C1
	fpmadd	C2, A2, A2, C2
	.align 4

LL(118):
	andi.	r0,  N, 1
	beq	LL(198)

	LFPDUX	A1,    X, INCX2
	fpmadd	C3, A1, A1, C3
	.align 4

LL(198):
	LFDX	A1,    X, INCX2
	fmadd	C4, A1, A1, C4

	fpadd	C1,  C1,  C5
	lis	r3, 0x3f00
	fpadd	C2,  C2,  C6
	lis	r4, 0x4040
	fpadd	C3,  C3,  C7
	stw	r3, 4(SP)
	fpadd	C4,  C4,  C8
	stw	r4, 8(SP)

	fpadd	C1,  C1,  C2
	lfs	f10, 0(SP)
	fpadd	C3,  C3,  C4
	lfs	f11, 4(SP)

	fpadd	C1,  C1,  C3
	lfs	f12, 8(SP)

	fsmtp	C2, C1
	fadd	C1, C2, C1

	fcmpu	cr0, f10, C1
	beq	cr0, LL(199)

#ifndef HUMMER_EMULATOR
	frsqrte	f9, f1
	li	r10, 16

	fmul	f2,  f1,  f9
	lfpdux	f23, SP, r10
	fmul	f3,  f9,  f11
	lfpdux	f22, SP, r10
	fnmsub	f4,  f2,  f9, f12
	lfpdux	f21, SP, r10
	fmul	f9,  f3,  f4
	lfpdux	f20, SP, r10
	fadd	f13, f11, f11
	lfpdux	f19, SP, r10
	fmul	f12, f1,  f9
	lfpdux	f18, SP, r10
	fmul	f11, f12, f11
	lfpdux	f17, SP, r10
	fnmsub	f1,  f12, f9, f13
	lfpdux	f16, SP, r10
	lfpdux	f15, SP, r10
	lfpdux	f14, SP, r10
	addi	SP, SP,  16
	fmadd	f1,  f11, f1, f12
	blr
#else
	fsqrt	f1, f1

	li	r10, 16
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
#endif
	.align 4

LL(199):
	li	r10, 16

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

LL(200):
	sub	X,  X, INCX2
	addi	X2, X, SIZE

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(215)


	LFDUX	A1,    X,  INCX2
	LFDUX	A2,    X2, INCX2
	LFDUX	A3,    X,  INCX2
	LFDUX	A4,    X2, INCX2

	LFDUX	A5,    X,  INCX2
	LFDUX	A6,    X2, INCX2
	LFDUX	A7,    X,  INCX2
	LFDUX	A8,    X2, INCX2

	LFDUX	A9,    X,  INCX2
	LFDUX	A10,   X2, INCX2
	LFDUX	A11,   X,  INCX2
	LFDUX	A12,   X2, INCX2

	LFDUX	A13,   X,  INCX2
	LFDUX	A14,   X2, INCX2
	LFDUX	A15,   X,  INCX2
	LFDUX	A16,   X2, INCX2
	bdz	LL(213)
	.align 4

LL(212):
	fmadd	C1, A1, A1, C1
	LFDUX	A1,    X,  INCX2
	fmadd	C2, A2, A2, C2
	LFDUX	A2,    X2, INCX2
	fmadd	C3, A3, A3, C3
	LFDUX	A3,    X,  INCX2
	fmadd	C4, A4, A4, C4
	LFDUX	A4,    X2, INCX2

	fmadd	C5, A5, A5, C5
	LFDUX	A5,    X,  INCX2
	fmadd	C6, A6, A6, C6
	LFDUX	A6,    X2, INCX2
	fmadd	C7, A7, A7, C7
	LFDUX	A7,    X,  INCX2
	fmadd	C8, A8, A8, C8
	LFDUX	A8,    X2, INCX2

	fmadd	C1, A9,  A9,  C1
	LFDUX	A9,    X,  INCX2
	fmadd	C2, A10, A10, C2
	LFDUX	A10,   X2, INCX2
	fmadd	C3, A11, A11, C3
	LFDUX	A11,   X,  INCX2
	fmadd	C4, A12, A12, C4
	LFDUX	A12,   X2, INCX2

	fmadd	C5, A13, A13, C5
	LFDUX	A13,   X,  INCX2
	fmadd	C6, A14, A14, C6
	LFDUX	A14,   X2, INCX2
	fmadd	C7, A15, A15, C7
	LFDUX	A15,   X,  INCX2
	fmadd	C8, A16, A16, C8
	LFDUX	A16,   X2, INCX2

	bdnz	LL(212)
	.align 4

LL(213):
	fmadd	C1, A1, A1, C1
	fmadd	C2, A2, A2, C2
	fmadd	C3, A3, A3, C3
	fmadd	C4, A4, A4, C4

	fmadd	C5, A5, A5, C5
	fmadd	C6, A6, A6, C6
	fmadd	C7, A7, A7, C7
	fmadd	C8, A8, A8, C8

	fmadd	C1, A9,  A9,  C1
	fmadd	C2, A10, A10, C2
	fmadd	C3, A11, A11, C3
	fmadd	C4, A12, A12, C4

	fmadd	C5, A13, A13, C5
	fmadd	C6, A14, A14, C6
	fmadd	C7, A15, A15, C7
	fmadd	C8, A16, A16, C8
	.align 4

LL(215):
	andi.	r0,  N, 7
	beq	LL(998)
	andi.	r0,  N, 4
	beq	LL(216)

	LFDUX	A1,    X,  INCX2
	LFDUX	A2,    X2, INCX2
	LFDUX	A3,    X,  INCX2
	LFDUX	A4,    X2, INCX2

	LFDUX	A5,    X,  INCX2
	LFDUX	A6,    X2, INCX2
	LFDUX	A7,    X,  INCX2
	LFDUX	A8,    X2, INCX2

	fmadd	C1, A1, A1, C1
	fmadd	C2, A2, A2, C2
	fmadd	C3, A3, A3, C3
	fmadd	C4, A4, A4, C4

	fmadd	C5, A5, A5, C5
	fmadd	C6, A6, A6, C6
	fmadd	C7, A7, A7, C7
	fmadd	C8, A8, A8, C8
	.align 4

LL(216):
	andi.	r0,  N, 2
	beq	LL(217)

	LFDUX	A1,    X,  INCX2
	LFDUX	A2,    X2, INCX2
	LFDUX	A3,    X,  INCX2
	LFDUX	A4,    X2, INCX2

	fmadd	C1, A1, A1, C1
	fmadd	C2, A2, A2, C2
	fmadd	C3, A3, A3, C3
	fmadd	C4, A4, A4, C4
	.align 4

LL(217):
	andi.	r0,  N, 1
	beq	LL(998)

	LFDUX	A1,    X,  INCX2
	LFDUX	A2,    X2, INCX2

	fmadd	C1, A1, A1, C1
	fmadd	C2, A2, A2, C2
	.align 4

LL(998):
	fadd	C1,  C1,  C5
	lis	r3, 0x3f00
	fadd	C2,  C2,  C6
	lis	r4, 0x4040
	fadd	C3,  C3,  C7
	stw	r3, 4(SP)
	fadd	C4,  C4,  C8
	stw	r4, 8(SP)

	fadd	C1,  C1,  C2
	lfs	f10, 0(SP)
	fadd	C3,  C3,  C4
	lfs	f11, 4(SP)
	fadd	C1,  C1,  C3
	lfs	f12, 8(SP)

	fcmpu	cr0, f10, C1
	beq	cr0, LL(99)

	frsqrte	f9, f1
	li	r10, 16

	fmul	f2,  f1,  f9
	lfpdux	f23, SP, r10
	fmul	f3,  f9,  f11
	lfpdux	f22, SP, r10
	fnmsub	f4,  f2,  f9, f12
	lfpdux	f21, SP, r10
	fmul	f9,  f3,  f4
	lfpdux	f20, SP, r10
	fadd	f13, f11, f11
	lfpdux	f19, SP, r10
	fmul	f12, f1,  f9
	lfpdux	f18, SP, r10
	fmul	f11, f12, f11
	lfpdux	f17, SP, r10
	fnmsub	f1,  f12, f9, f13
	lfpdux	f16, SP, r10
	lfpdux	f15, SP, r10
	lfpdux	f14, SP, r10
	addi	SP, SP,  16
	fmadd	f1,  f11, f1, f12
	blr

LL(999):
	li	r10, 16

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
