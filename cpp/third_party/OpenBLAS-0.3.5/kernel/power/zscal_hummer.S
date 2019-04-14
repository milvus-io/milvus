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
#define XX	r5
#define Y	r8
#define YY	r9

#define ALPHA	f1
#define ALPHA_I	f2

#define A1	f0
#define A2	f16
#define A3	f17
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
	stfpdux	f17, SP, r10

	li	r10,   0
	stwu	r10,   -4(SP)
	stwu	r10,   -4(SP)
	stwu	r10,   -4(SP)
	stwu	r10,   -4(SP)

	lfpdx	A1, SP, r10		# Zero clear
	fsmfp	ALPHA, ALPHA_I

	slwi	INCX,  INCX, BASE_SHIFT
	add	INCX2, INCX, INCX

	cmpwi	cr0, N, 0
	ble	LL(999)

	cmpwi	cr0, INCX, SIZE
	bne	LL(100)

	fcmpu	cr7, ALPHA, A1
	bne	cr7, LL(50)

	fscmp	cr7, ALPHA, A1
	bne	cr7, LL(50)

	andi.	r0, X, 2 * SIZE - 1
	bne	LL(20)

	sub	X,  X, INCX2

	srawi.	r0, N, 2
	mtspr	CTR,  r0
	beq-	LL(15)
	.align 4

LL(12):
	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	bdnz	LL(12)
	.align 4

LL(15):
	andi.	r0,  N, 3
	beq	LL(999)
	andi.	r0,  N, 2
	beq	LL(17)

	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	.align 4

LL(17):
	andi.	r0,  N, 1
	beq	LL(999)

	STFPDUX	A1,   X, INCX2
	b	LL(999)
	.align 4

LL(20):
	sub	X,  X, INCX2

	STFDX	A1, X, INCX2
	addi	X, X, SIZE
	addi	N, N, -1
	cmpwi	cr0, N, 0
	ble	LL(29)

	srawi.	r0, N, 2
	mtspr	CTR,  r0
	beq-	LL(25)
	.align 4

LL(22):
	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	bdnz	LL(22)
	.align 4

LL(25):
	andi.	r0,  N, 3
	beq	LL(29)
	andi.	r0,  N, 2
	beq	LL(27)

	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	.align 4

LL(27):
	andi.	r0,  N, 1
	beq	LL(29)

	STFPDUX	A1,   X, INCX2
	.align 4

LL(29):
	STFDX	A1,   X, INCX2
	b	LL(999)
	.align 4

LL(50):
	sub	Y,  X, INCX2
	sub	X,  X, INCX2

	andi.	r0, X, 2 * SIZE - 1
	bne	LL(60)

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(55)

	LFPDUX	A1,  X,  INCX2
	LFPDUX	A2,  X,  INCX2
	LFPDUX	A3,  X,  INCX2
	LFPDUX	A4,  X,  INCX2

	LFPDUX	A5,  X,  INCX2
	fxpmul	B1,  ALPHA, A1
	LFPDUX	A6,  X,  INCX2
	fxpmul	B2,  ALPHA, A2
	LFPDUX	A7,  X,  INCX2
	fxpmul	B3,  ALPHA, A3
	LFPDUX	A8,  X,  INCX2
	fxpmul	B4,  ALPHA, A4
 	fxpmul	B5,  ALPHA, A5

	fxcxnpma B1, ALPHA, A1, B1
	fxcxnpma B2, ALPHA, A2, B2
	bdz	LL(53)
	.align 4

LL(52):
	fxcxnpma B3, ALPHA, A3, B3
	LFPDUX	A1,  X,  INCX2
	fxpmul	B6,  ALPHA, A6
	STFPDUX	B1,   Y,  INCX2

	fxcxnpma B4, ALPHA, A4, B4
	LFPDUX	A2,  X,  INCX2
	fxpmul	B7,  ALPHA, A7
	STFPDUX	B2,   Y,  INCX2

	fxcxnpma B5, ALPHA, A5, B5
	LFPDUX	A3,  X,  INCX2
	fxpmul	B8,  ALPHA, A8
	STFPDUX	B3,   Y,  INCX2

	fxcxnpma B6, ALPHA, A6, B6
	LFPDUX	A4,  X,  INCX2
	fxpmul	B1,  ALPHA, A1
	STFPDUX	B4,   Y,  INCX2

	fxcxnpma B7, ALPHA, A7, B7
 	LFPDUX	A5,  X,  INCX2
	fxpmul	B2,  ALPHA, A2
	STFPDUX	B5,   Y,  INCX2

	fxcxnpma B8, ALPHA, A8, B8
	LFPDUX	A6,  X,  INCX2
	fxpmul	B3,  ALPHA, A3
	STFPDUX	B6,   Y,  INCX2

	fxcxnpma B1, ALPHA, A1, B1
	LFPDUX	A7,  X,  INCX2
	fxpmul	B4,  ALPHA, A4
	STFPDUX	B7,   Y,  INCX2

	fxcxnpma B2, ALPHA, A2, B2
	LFPDUX	A8,  X,  INCX2
	fxpmul	B5,  ALPHA, A5
	STFPDUX	B8,   Y,  INCX2
	bdnz	LL(52)
	.align 4

LL(53):
	fxcxnpma B3, ALPHA, A3, B3
	fxpmul	B6,  ALPHA, A6
	STFPDUX	B1,   Y,  INCX2

	fxcxnpma B4, ALPHA, A4, B4
	fxpmul	B7,  ALPHA, A7
	STFPDUX	B2,   Y,  INCX2

	fxcxnpma B5, ALPHA, A5, B5
	fxpmul	B8,  ALPHA, A8
	STFPDUX	B3,   Y,  INCX2

	fxcxnpma B6, ALPHA, A6, B6
	STFPDUX	B4,   Y,  INCX2
	fxcxnpma B7, ALPHA, A7, B7
	STFPDUX	B5,   Y,  INCX2
	fxcxnpma B8, ALPHA, A8, B8
	STFPDUX	B6,   Y,  INCX2
	STFPDUX	B7,   Y,  INCX2
	STFPDUX	B8,   Y,  INCX2
	.align 4

LL(55):
	andi.	r0,  N, 7
	beq	LL(999)

	andi.	r0,  N, 4
	beq	LL(56)

	LFPDUX	A1,  X,  INCX2
	LFPDUX	A2,  X,  INCX2
	LFPDUX	A3,  X,  INCX2
	LFPDUX	A4,  X,  INCX2

	fxpmul	B1,  ALPHA, A1
	fxpmul	B2,  ALPHA, A2
	fxpmul	B3,  ALPHA, A3
	fxpmul	B4,  ALPHA, A4

	fxcxnpma B1, ALPHA, A1, B1
	fxcxnpma B2, ALPHA, A2, B2
	fxcxnpma B3, ALPHA, A3, B3
	fxcxnpma B4, ALPHA, A4, B4

	STFPDUX	B1,   Y,  INCX2
	STFPDUX	B2,   Y,  INCX2
	STFPDUX	B3,   Y,  INCX2
	STFPDUX	B4,   Y,  INCX2
	.align 4

LL(56):
	andi.	r0,  N, 2
	beq	LL(57)

	LFPDUX	A1,  X,  INCX2
	LFPDUX	A2,  X,  INCX2

	fxpmul	B1,  ALPHA, A1
	fxpmul	B2,  ALPHA, A2

	fxcxnpma B1, ALPHA, A1, B1
	fxcxnpma B2, ALPHA, A2, B2

	STFPDUX	B1,   Y,  INCX2
	STFPDUX	B2,   Y,  INCX2
	.align 4

LL(57):
	andi.	r0,  N, 1
	beq	LL(999)

	LFPDUX	A1,   X,  INCX2

	fxpmul	B1,  ALPHA, A1
	fxcxnpma B1, ALPHA, A1, B1

	STFPDUX	B1,   Y,  INCX2
	b	LL(999)
	.align 4

LL(60):
	addi	XX, X, SIZE
	addi	YY, Y, SIZE

	srawi.	r0, N, 2
	mtspr	CTR,  r0
	beq-	LL(65)

	LFDUX	A1,  X,  INCX2
	LFDUX	A2,  XX, INCX2
	LFDUX	A3,  X,  INCX2
	LFDUX	A4,  XX, INCX2

	LFDUX	A5,  X,  INCX2
	fmul	B1, ALPHA,   A1
	LFDUX	A6,  XX, INCX2
	fmul	B2, ALPHA_I, A1
	LFDUX	A7,  X,  INCX2
	fmul	B3, ALPHA,   A3
	LFDUX	A8,  XX, INCX2
	fmul	B4, ALPHA_I, A3

	fmul	B5, ALPHA,   A5
	fnmsub	B1, ALPHA_I, A2, B1
	fmadd	B2, ALPHA  , A2, B2
	bdz	LL(63)
	.align 4

LL(62):
	fnmsub	B3, ALPHA_I, A4, B3
 	LFDUX	A1,  X,  INCX2
	fmul	B6, ALPHA_I, A5
	STFDUX	B1,  Y,  INCX2

	fmadd	B4, ALPHA  , A4, B4
	LFDUX	A2,  XX, INCX2
	fmul	B7, ALPHA,   A7
	STFDUX	B2,  YY, INCX2

	fnmsub	B5, ALPHA_I, A6, B5
	LFDUX	A3,  X,  INCX2
	fmul	B8, ALPHA_I, A7
	STFDUX	B3,  Y,  INCX2

	fmadd	B6, ALPHA  , A6, B6
	LFDUX	A4,  XX, INCX2
	fmul	B1, ALPHA,   A1
	STFDUX	B4,  YY, INCX2

	fnmsub	B7, ALPHA_I, A8, B7
	LFDUX	A5,  X,  INCX2
	fmul	B2, ALPHA_I, A1
	STFDUX	B5,  Y,  INCX2

	fmadd	B8, ALPHA  , A8, B8
	LFDUX	A6,  XX, INCX2
	fmul	B3, ALPHA,   A3
	STFDUX	B6,  YY, INCX2

	fnmsub	B1, ALPHA_I, A2, B1
	LFDUX	A7,  X,  INCX2
	fmul	B4, ALPHA_I, A3
	STFDUX	B7,  Y,  INCX2

	fmadd	B2, ALPHA  , A2, B2
	LFDUX	A8,  XX, INCX2
	fmul	B5, ALPHA,   A5
	STFDUX	B8,  YY, INCX2
	bdnz	LL(62)
	.align 4

LL(63):
	fnmsub	B3, ALPHA_I, A4, B3
	fmul	B6, ALPHA_I, A5
	STFDUX	B1,  Y,  INCX2

	fmadd	B4, ALPHA  , A4, B4
	fmul	B7, ALPHA,   A7
	STFDUX	B2,  YY, INCX2

	fnmsub	B5, ALPHA_I, A6, B5
	fmul	B8, ALPHA_I, A7
	STFDUX	B3,  Y,  INCX2

	fmadd	B6, ALPHA  , A6, B6
	STFDUX	B4,  YY, INCX2
	fnmsub	B7, ALPHA_I, A8, B7
	STFDUX	B5,  Y,  INCX2
	fmadd	B8, ALPHA  , A8, B8
	STFDUX	B6,  YY, INCX2
	STFDUX	B7,  Y,  INCX2
	STFDUX	B8,  YY, INCX2
	.align 4

LL(65):
	andi.	r0,  N, 3
	beq	LL(999)
	andi.	r0,  N, 2
	beq	LL(67)

	LFDUX	A1,  X,  INCX2
	LFDUX	A2,  XX, INCX2
	LFDUX	A3,  X,  INCX2
	LFDUX	A4,  XX, INCX2

	fmul	B1, ALPHA, A1
	fmul	B2, ALPHA, A2
	fmul	B3, ALPHA, A3
	fmul	B4, ALPHA, A4

	fnmsub	B1, ALPHA_I, A2, B1
	fmadd	B2, ALPHA_I, A1, B2
	fnmsub	B3, ALPHA_I, A4, B3
	fmadd	B4, ALPHA_I, A3, B4

	STFDUX	B1,  Y,  INCX2
	STFDUX	B2,  YY, INCX2
	STFDUX	B3,  Y,  INCX2
	STFDUX	B4,  YY, INCX2
	.align 4

LL(67):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	A1,   X,  INCX2
	LFDUX	A2,   XX, INCX2

	fmul	B1, ALPHA, A1
	fmul	B2, ALPHA, A2
	fnmsub	B1, ALPHA_I, A2, B1
	fmadd	B2, ALPHA_I, A1, B2

	STFDUX	B1,   Y,  INCX2
	STFDUX	B2,   YY, INCX2
	b	LL(999)
	.align 4


LL(100):
	fcmpu	cr7, ALPHA, A1
	bne	cr7, LL(150)

	fscmp	cr7, ALPHA, A1
	bne	cr7, LL(150)

	andi.	r0, X, 2 * SIZE - 1
	bne	LL(120)

	sub	X,  X, INCX2

	srawi.	r0, N, 2
	mtspr	CTR,  r0
	beq-	LL(115)
	.align 4

LL(112):
	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	bdnz	LL(112)
	.align 4

LL(115):
	andi.	r0,  N, 3
	beq	LL(999)
	andi.	r0,  N, 2
	beq	LL(117)

	STFPDUX	A1,   X, INCX2
	STFPDUX	A1,   X, INCX2
	.align 4

LL(117):
	andi.	r0,  N, 1
	beq	LL(999)

	STFPDUX	A1,   X, INCX2
	b	LL(999)
	.align 4

LL(120):
	subi	INCX2, INCX2, SIZE
	li	INCX, SIZE

	sub	X,  X, INCX2

	srawi.	r0, N, 2
	mtspr	CTR,  r0
	beq-	LL(125)
	.align 4

LL(122):
	STFDUX	A1,   X, INCX2
	STFDUX	A1,   X, INCX
	STFDUX	A1,   X, INCX2
	STFDUX	A1,   X, INCX
	STFDUX	A1,   X, INCX2
	STFDUX	A1,   X, INCX
	STFDUX	A1,   X, INCX2
	STFDUX	A1,   X, INCX
	bdnz	LL(122)
	.align 4

LL(125):
	andi.	r0,  N, 3
	beq	LL(999)
	andi.	r0,  N, 2
	beq	LL(127)

	STFDUX	A1,   X, INCX2
	STFDUX	A1,   X, INCX
	STFDUX	A1,   X, INCX2
	STFDUX	A1,   X, INCX
	.align 4

LL(127):
	andi.	r0,  N, 1
	beq	LL(999)

	STFDUX	A1,   X, INCX2
	STFDUX	A1,   X, INCX
	b	LL(999)
	.align 4

LL(150):
	sub	Y,  X, INCX2
	sub	X,  X, INCX2

	andi.	r0, X, 2 * SIZE - 1
	bne	LL(160)

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(155)

	LFPDUX	A1,  X,  INCX2
	LFPDUX	A2,  X,  INCX2
	LFPDUX	A3,  X,  INCX2
	LFPDUX	A4,  X,  INCX2

	LFPDUX	A5,  X,  INCX2
	fxpmul	B1,  ALPHA, A1
	LFPDUX	A6,  X,  INCX2
	fxpmul	B2,  ALPHA, A2
	LFPDUX	A7,  X,  INCX2
	fxpmul	B3,  ALPHA, A3
	LFPDUX	A8,  X,  INCX2
	fxpmul	B4,  ALPHA, A4
 	fxpmul	B5,  ALPHA, A5

	fxcxnpma B1, ALPHA, A1, B1
	fxcxnpma B2, ALPHA, A2, B2
	bdz	LL(153)
	.align 4

LL(152):
	fxcxnpma B3, ALPHA, A3, B3
	LFPDUX	A1,  X,  INCX2
	fxpmul	B6,  ALPHA, A6
	STFPDUX	B1,   Y,  INCX2

	fxcxnpma B4, ALPHA, A4, B4
	LFPDUX	A2,  X,  INCX2
	fxpmul	B7,  ALPHA, A7
	STFPDUX	B2,   Y,  INCX2

	fxcxnpma B5, ALPHA, A5, B5
	LFPDUX	A3,  X,  INCX2
	fxpmul	B8,  ALPHA, A8
	STFPDUX	B3,   Y,  INCX2

	fxcxnpma B6, ALPHA, A6, B6
	LFPDUX	A4,  X,  INCX2
	fxpmul	B1,  ALPHA, A1
	STFPDUX	B4,   Y,  INCX2

	fxcxnpma B7, ALPHA, A7, B7
 	LFPDUX	A5,  X,  INCX2
	fxpmul	B2,  ALPHA, A2
	STFPDUX	B5,   Y,  INCX2

	fxcxnpma B8, ALPHA, A8, B8
	LFPDUX	A6,  X,  INCX2
	fxpmul	B3,  ALPHA, A3
	STFPDUX	B6,   Y,  INCX2

	fxcxnpma B1, ALPHA, A1, B1
	LFPDUX	A7,  X,  INCX2
	fxpmul	B4,  ALPHA, A4
	STFPDUX	B7,   Y,  INCX2

	fxcxnpma B2, ALPHA, A2, B2
	LFPDUX	A8,  X,  INCX2
	fxpmul	B5,  ALPHA, A5
	STFPDUX	B8,   Y,  INCX2
	bdnz	LL(152)
	.align 4

LL(153):
	fxcxnpma B3, ALPHA, A3, B3
	fxpmul	B6,  ALPHA, A6
	STFPDUX	B1,   Y,  INCX2

	fxcxnpma B4, ALPHA, A4, B4
	fxpmul	B7,  ALPHA, A7
	STFPDUX	B2,   Y,  INCX2

	fxcxnpma B5, ALPHA, A5, B5
	fxpmul	B8,  ALPHA, A8
	STFPDUX	B3,   Y,  INCX2

	fxcxnpma B6, ALPHA, A6, B6
	STFPDUX	B4,   Y,  INCX2
	fxcxnpma B7, ALPHA, A7, B7
	STFPDUX	B5,   Y,  INCX2
	fxcxnpma B8, ALPHA, A8, B8
	STFPDUX	B6,   Y,  INCX2
	STFPDUX	B7,   Y,  INCX2
	STFPDUX	B8,   Y,  INCX2
	.align 4

LL(155):
	andi.	r0,  N, 7
	beq	LL(999)

	andi.	r0,  N, 4
	beq	LL(156)

	LFPDUX	A1,  X,  INCX2
	LFPDUX	A2,  X,  INCX2
	LFPDUX	A3,  X,  INCX2
	LFPDUX	A4,  X,  INCX2

	fxpmul	B1,  ALPHA, A1
	fxpmul	B2,  ALPHA, A2
	fxpmul	B3,  ALPHA, A3
	fxpmul	B4,  ALPHA, A4

	fxcxnpma B1, ALPHA, A1, B1
	fxcxnpma B2, ALPHA, A2, B2
	fxcxnpma B3, ALPHA, A3, B3
	fxcxnpma B4, ALPHA, A4, B4

	STFPDUX	B1,   Y,  INCX2
	STFPDUX	B2,   Y,  INCX2
	STFPDUX	B3,   Y,  INCX2
	STFPDUX	B4,   Y,  INCX2
	.align 4

LL(156):
	andi.	r0,  N, 2
	beq	LL(157)

	LFPDUX	A1,  X,  INCX2
	LFPDUX	A2,  X,  INCX2

	fxpmul	B1,  ALPHA, A1
	fxpmul	B2,  ALPHA, A2

	fxcxnpma B1, ALPHA, A1, B1
	fxcxnpma B2, ALPHA, A2, B2

	STFPDUX	B1,   Y,  INCX2
	STFPDUX	B2,   Y,  INCX2
	.align 4

LL(157):
	andi.	r0,  N, 1
	beq	LL(999)

	LFPDUX	A1,   X,  INCX2

	fxpmul	B1,  ALPHA, A1
	fxcxnpma B1, ALPHA, A1, B1

	STFPDUX	B1,   Y,  INCX2
	b	LL(999)
	.align 4

LL(160):
	addi	XX, X, SIZE
	addi	YY, Y, SIZE

	srawi.	r0, N, 2
	mtspr	CTR,  r0
	beq-	LL(165)

	LFDUX	A1,  X,  INCX2
	LFDUX	A2,  XX, INCX2
	LFDUX	A3,  X,  INCX2
	LFDUX	A4,  XX, INCX2

	LFDUX	A5,  X,  INCX2
	fmul	B1, ALPHA,   A1
	LFDUX	A6,  XX, INCX2
	fmul	B2, ALPHA_I, A1
	LFDUX	A7,  X,  INCX2
	fmul	B3, ALPHA,   A3
	LFDUX	A8,  XX, INCX2
	fmul	B4, ALPHA_I, A3

	fmul	B5, ALPHA,   A5
	fnmsub	B1, ALPHA_I, A2, B1
	fmadd	B2, ALPHA  , A2, B2
	bdz	LL(163)

	.align 4

LL(162):
	fnmsub	B3, ALPHA_I, A4, B3
 	LFDUX	A1,  X,  INCX2
	fmul	B6, ALPHA_I, A5
	STFDUX	B1,  Y,  INCX2

	fmadd	B4, ALPHA  , A4, B4
	LFDUX	A2,  XX, INCX2
	fmul	B7, ALPHA,   A7
	STFDUX	B2,  YY, INCX2

	fnmsub	B5, ALPHA_I, A6, B5
	LFDUX	A3,  X,  INCX2
	fmul	B8, ALPHA_I, A7
	STFDUX	B3,  Y,  INCX2

	fmadd	B6, ALPHA  , A6, B6
	LFDUX	A4,  XX, INCX2
	fmul	B1, ALPHA,   A1
	STFDUX	B4,  YY, INCX2

	fnmsub	B7, ALPHA_I, A8, B7
	LFDUX	A5,  X,  INCX2
	fmul	B2, ALPHA_I, A1
	STFDUX	B5,  Y,  INCX2

	fmadd	B8, ALPHA  , A8, B8
	LFDUX	A6,  XX, INCX2
	fmul	B3, ALPHA,   A3
	STFDUX	B6,  YY, INCX2

	fnmsub	B1, ALPHA_I, A2, B1
	LFDUX	A7,  X,  INCX2
	fmul	B4, ALPHA_I, A3
	STFDUX	B7,  Y,  INCX2

	fmadd	B2, ALPHA  , A2, B2
	LFDUX	A8,  XX, INCX2
	fmul	B5, ALPHA,   A5
	STFDUX	B8,  YY, INCX2
	bdnz	LL(162)
	.align 4

LL(163):
	fnmsub	B3, ALPHA_I, A4, B3
	fmul	B6, ALPHA_I, A5
	STFDUX	B1,  Y,  INCX2

	fmadd	B4, ALPHA  , A4, B4
	fmul	B7, ALPHA,   A7
	STFDUX	B2,  YY, INCX2

	fnmsub	B5, ALPHA_I, A6, B5
	fmul	B8, ALPHA_I, A7
	STFDUX	B3,  Y,  INCX2

	fmadd	B6, ALPHA  , A6, B6
	STFDUX	B4,  YY, INCX2
	fnmsub	B7, ALPHA_I, A8, B7
	STFDUX	B5,  Y,  INCX2
	fmadd	B8, ALPHA  , A8, B8
	STFDUX	B6,  YY, INCX2
	STFDUX	B7,  Y,  INCX2
	STFDUX	B8,  YY, INCX2
	.align 4

LL(165):
	andi.	r0,  N, 3
	beq	LL(999)
	andi.	r0,  N, 2
	beq	LL(167)

	LFDUX	A1,  X,  INCX2
	LFDUX	A2,  XX, INCX2
	LFDUX	A3,  X,  INCX2
	LFDUX	A4,  XX, INCX2

	fmul	B1, ALPHA, A1
	fmul	B2, ALPHA, A2
	fmul	B3, ALPHA, A3
	fmul	B4, ALPHA, A4

	fnmsub	B1, ALPHA_I, A2, B1
	fmadd	B2, ALPHA_I, A1, B2
	fnmsub	B3, ALPHA_I, A4, B3
	fmadd	B4, ALPHA_I, A3, B4

	STFDUX	B1,  Y,  INCX2
	STFDUX	B2,  YY, INCX2
	STFDUX	B3,  Y,  INCX2
	STFDUX	B4,  YY, INCX2
	.align 4

LL(167):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	A1,   X,  INCX2
	LFDUX	A2,   XX, INCX2

	fmul	B1, ALPHA, A1
	fmul	B2, ALPHA, A2
	fnmsub	B1, ALPHA_I, A2, B1
	fmadd	B2, ALPHA_I, A1, B2

	STFDUX	B1,   Y,  INCX2
	STFDUX	B2,   YY, INCX2
	.align 4

LL(999):
	li	r10, 16

	lfpdux	f17, SP, r10
	lfpdux	f16, SP, r10
	lfpdux	f15, SP, r10
	lfpdux	f14, SP, r10

	addi	SP, SP,  16
	blr

	EPILOGUE
