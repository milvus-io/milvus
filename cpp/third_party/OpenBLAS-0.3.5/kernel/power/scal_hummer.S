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
#define X	r6
#define INCX	r7

#define INCX2	r4
#define X2	r5

#define ALPHA	f1

#define A1	f0
#define A2	f16
#define A3	f2
#define A4	f3
#define A5	f4
#define A6	f5
#define A7	f6
#define A8	f7

#define B1	f8
#define B2	f9
#define B3	f10
#define B4	f11
#define B5	f12
#define B6	f13
#define B7	f14
#define B8	f15

	PROLOGUE
	PROFCODE

	li	r10, -16

	stfpdux	f14, SP, r10
	stfpdux	f15, SP, r10
	stfpdux	f16, SP, r10

	li	r10,   0
	stwu	r10,   -4(SP)
	stwu	r10,   -4(SP)
	stwu	r10,   -4(SP)
	stwu	r10,   -4(SP)

	lfpdx	A1, SP, r10		# Zero clear
	fsmfp	ALPHA, ALPHA

	slwi	INCX,  INCX, BASE_SHIFT
	add	INCX2, INCX, INCX

	cmpwi	cr0, N, 0
	ble	LL(999)

	cmpwi	cr0, INCX, SIZE
	bne	LL(100)

	fcmpu	cr7, ALPHA, A1
	bne	cr7, LL(50)

	sub	X,  X, INCX2

	andi.	r0, X, 2 * SIZE - 1
	beq	LL(11)

	STFDX	A1, X, INCX2
	addi	X, X, 1 * SIZE
	addi	N, N, -1
	cmpwi	cr0, N, 0
	ble	LL(999)
	.align 4

LL(11):
	srawi.	r0, N, 4
	mtspr	CTR,  r0
	beq-	LL(15)
	.align 4

LL(12):
	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	bdnz	LL(12)
	.align 4

LL(15):
	andi.	r0,  N, 15
	beq	LL(999)
	andi.	r0,  N, 8
	beq	LL(16)

	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	.align 4

LL(16):
	andi.	r0,  N, 4
	beq	LL(17)

	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	.align 4

LL(17):
	andi.	r0,  N, 2
	beq	LL(18)

	STFPDUX	A1,   X, INCX2
	.align 4

LL(18):
	andi.	r0,  N, 1
	beq	LL(999)
	STFDUX	A1,   X, INCX2
	b	LL(999)
	.align 4

LL(50):
	sub	X2, X, INCX2
	sub	X,  X, INCX2

	andi.	r0, X, 2 * SIZE - 1
	beq	LL(51)

	LFDX	A1, X,  INCX2
	addi	X,  X,  1 * SIZE

	fmul	B1, ALPHA, A1
	addi	N, N, -1
	cmpwi	cr0, N, 0

	STFDX	B1, X2, INCX2
	addi	X2, X2, 1 * SIZE
	ble	LL(999)
	.align 4

LL(51):
	srawi.	r0, N, 4
	mtspr	CTR,  r0
	beq-	LL(55)

	LFPDUX	A1, X,  INCX2
	LFPDUX	A2, X,  INCX2
	LFPDUX	A3, X,  INCX2
	LFPDUX	A4, X,  INCX2
	LFPDUX	A5, X,  INCX2
	LFPDUX	A6, X,  INCX2
	LFPDUX	A7, X,  INCX2
	LFPDUX	A8, X,  INCX2
	bdz	LL(53)
	.align 4

LL(52):
	fpmul	B1, ALPHA, A1
	LFPDUX	A1, X,  INCX2
	fpmul	B2, ALPHA, A2
	LFPDUX	A2, X,  INCX2
	fpmul	B3, ALPHA, A3
	LFPDUX	A3, X,  INCX2
	fpmul	B4, ALPHA, A4
	LFPDUX	A4, X,  INCX2
	fpmul	B5, ALPHA, A5
	LFPDUX	A5, X,  INCX2
	fpmul	B6, ALPHA, A6
	LFPDUX	A6, X,  INCX2
	fpmul	B7, ALPHA, A7
	LFPDUX	A7, X,  INCX2
	fpmul	B8, ALPHA, A8
	LFPDUX	A8, X,  INCX2

	STFPDUX	B1, X2, INCX2
	STFPDUX	B2, X2, INCX2
	STFPDUX	B3, X2, INCX2
	STFPDUX	B4, X2, INCX2
	STFPDUX	B5, X2, INCX2
	STFPDUX	B6, X2, INCX2
	STFPDUX	B7, X2, INCX2
	STFPDUX	B8, X2, INCX2
	bdnz	LL(52)
	.align 4

LL(53):
	fpmul	B1, ALPHA, A1
	fpmul	B2, ALPHA, A2
	fpmul	B3, ALPHA, A3
	fpmul	B4, ALPHA, A4
	fpmul	B5, ALPHA, A5
	fpmul	B6, ALPHA, A6
	STFPDUX	B1, X2, INCX2
	fpmul	B7, ALPHA, A7
	STFPDUX	B2, X2, INCX2
	fpmul	B8, ALPHA, A8
	STFPDUX	B3, X2, INCX2

	STFPDUX	B4, X2, INCX2
	STFPDUX	B5, X2, INCX2
	STFPDUX	B6, X2, INCX2
	STFPDUX	B7, X2, INCX2
	STFPDUX	B8, X2, INCX2
	.align 4

LL(55):
	andi.	r0,  N, 15
	beq	LL(999)
	andi.	r0,  N, 8
	beq	LL(56)

	LFPDUX	A1, X,  INCX2
	LFPDUX	A2, X,  INCX2
	LFPDUX	A3, X,  INCX2
	LFPDUX	A4, X,  INCX2

	fpmul	B1, ALPHA, A1
	fpmul	B2, ALPHA, A2
	fpmul	B3, ALPHA, A3
	fpmul	B4, ALPHA, A4

	STFPDUX	B1, X2, INCX2
	STFPDUX	B2, X2, INCX2
	STFPDUX	B3, X2, INCX2
	STFPDUX	B4, X2, INCX2
	.align 4

LL(56):
	andi.	r0,  N, 4
	beq	LL(57)

	LFPDUX	A1, X,  INCX2
	LFPDUX	A2, X,  INCX2
	fpmul	B1, ALPHA, A1
	fpmul	B2, ALPHA, A2
	STFPDUX	B1, X2, INCX2
	STFPDUX	B2, X2, INCX2
	.align 4

LL(57):
	andi.	r0,  N, 2
	beq	LL(58)

	LFPDUX	A1, X,  INCX2
	fpmul	B1, ALPHA, A1
	STFPDUX	B1, X2, INCX2
	.align 4

LL(58):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDX	A1, X,  INCX2
	fmul	B1, ALPHA, A1
	STFDX	B1, X2, INCX2
	b	LL(999)
	.align 4


LL(100):
	fcmpu	cr7, ALPHA, A1
	bne	cr7, LL(200)

	sub	X,  X, INCX

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(115)
	.align 4

LL(112):
	STFDUX	A1,   X, INCX
	STFDUX	A1,   X, INCX
	STFDUX	A1,   X, INCX
	STFDUX	A1,   X, INCX
	STFDUX	A1,   X, INCX
	STFDUX	A1,   X, INCX
	STFDUX	A1,   X, INCX
	STFDUX	A1,   X, INCX
	bdnz	LL(112)
	.align 4

LL(115):
	andi.	r0,  N, 7
	beq	LL(999)
	andi.	r0,  N, 4
	beq	LL(117)

	STFDUX	A1,   X, INCX
	STFDUX	A1,   X, INCX
	STFDUX	A1,   X, INCX
	STFDUX	A1,   X, INCX
	.align 4

LL(117):
	andi.	r0,  N, 2
	beq	LL(118)

	STFDUX	A1,   X, INCX
	STFDUX	A1,   X, INCX
	.align 4

LL(118):
	andi.	r0,  N, 1
	beq	LL(999)
	STFDUX	A1,   X, INCX
	b	LL(999)
	.align 4

LL(200):
	sub	X2, X, INCX
	sub	X,  X, INCX

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(215)

	LFDUX	A1,   X, INCX
	LFDUX	A2,   X, INCX
	LFDUX	A3,   X, INCX
	LFDUX	A4,   X, INCX
	LFDUX	A5,   X, INCX
	LFDUX	A6,   X, INCX
	LFDUX	A7,   X, INCX
	LFDUX	A8,   X, INCX
	bdz	LL(213)
	.align 4

LL(212):
	fmul	B1, ALPHA, A1
	LFDUX	A1,   X, INCX
	fmul	B2, ALPHA, A2
	LFDUX	A2,   X, INCX

	fmul	B3, ALPHA, A3
	LFDUX	A3,   X, INCX
	fmul	B4, ALPHA, A4
	LFDUX	A4,   X, INCX

	fmul	B5, ALPHA, A5
	LFDUX	A5,   X, INCX
	fmul	B6, ALPHA, A6
	LFDUX	A6,   X, INCX

	fmul	B7, ALPHA, A7
	LFDUX	A7,   X, INCX
	fmul	B8, ALPHA, A8
	LFDUX	A8,   X, INCX

	STFDUX	B1,   X2, INCX
	STFDUX	B2,   X2, INCX
	STFDUX	B3,   X2, INCX
	STFDUX	B4,   X2, INCX
	STFDUX	B5,   X2, INCX
	STFDUX	B6,   X2, INCX
	STFDUX	B7,   X2, INCX
	STFDUX	B8,   X2, INCX
	bdnz	LL(212)
	.align 4

LL(213):
	fmul	B1, ALPHA, A1
	fmul	B2, ALPHA, A2
	fmul	B3, ALPHA, A3
	fmul	B4, ALPHA, A4
	fmul	B5, ALPHA, A5

	fmul	B6, ALPHA, A6
	STFDUX	B1,   X2, INCX
	fmul	B7, ALPHA, A7
	STFDUX	B2,   X2, INCX
	fmul	B8, ALPHA, A8
	STFDUX	B3,   X2, INCX
	STFDUX	B4,   X2, INCX
	STFDUX	B5,   X2, INCX
	STFDUX	B6,   X2, INCX
	STFDUX	B7,   X2, INCX
	STFDUX	B8,   X2, INCX
	.align 4

LL(215):
	andi.	r0,  N, 7
	beq	LL(999)
	andi.	r0,  N, 4
	beq	LL(217)

	LFDUX	A1,   X, INCX
	LFDUX	A2,   X, INCX
	LFDUX	A3,   X, INCX
	LFDUX	A4,   X, INCX

	fmul	B1, ALPHA, A1
	fmul	B2, ALPHA, A2
	fmul	B3, ALPHA, A3
	fmul	B4, ALPHA, A4

	STFDUX	B1,   X2, INCX
	STFDUX	B2,   X2, INCX
	STFDUX	B3,   X2, INCX
	STFDUX	B4,   X2, INCX
	.align 4

LL(217):
	andi.	r0,  N, 2
	beq	LL(218)

	LFDUX	A1,   X, INCX
	LFDUX	A2,   X, INCX

	fmul	B1, ALPHA, A1
	fmul	B2, ALPHA, A2

	STFDUX	B1,   X2, INCX
	STFDUX	B2,   X2, INCX
	.align 4

LL(218):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	A1,   X, INCX
	fmul	B1, ALPHA, A1
	STFDUX	B1,   X2, INCX
	.align 4

LL(999):
	li	r10, 16

	lfpdux	f16, SP, r10
	lfpdux	f15, SP, r10
	lfpdux	f14, SP, r10

	addi	SP, SP,  16
	blr

	EPILOGUE
