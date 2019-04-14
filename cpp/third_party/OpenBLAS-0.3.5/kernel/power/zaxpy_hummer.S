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
#define Y	r8
#define INCY	r9

#define	YY	r4
#define INCX2	r5
#define INCY2	r10
#define X1	r11
#define Y1	INCX
#define YY1	INCY

#define ALPHA	f1

#define A1	f0
#define A2	f8
#define A3	f2
#define A4	f3
#define A5	f4
#define A6	f5
#define A7	f6
#define A8	f7
#define A9	f25

#define B1	f9
#define B2	f10
#define B3	f11
#define B4	f12
#define B5	f13
#define B6	f14
#define B7	f15
#define B8	f16

#define C1	f17
#define C2	f18
#define C3	f19
#define C4	f20
#define C5	f21
#define C6	f22
#define C7	f23
#define C8	f24

#define ALPHA_R ALPHA
#define ALPHA_I A9

#ifndef CONJ
#define ADD1	FNMSUB
#define ADD2	FMADD
#else
#define ADD1	FMADD
#define ADD2	FNMSUB
#endif

#ifndef CONJ
#define FXMADD1	fxcpmadd
#define FXMADD2	fxcxnpma
#else
#define FXMADD1	fxcpnsma
#define FXMADD2	fxcxma
#endif

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

	fsmfp	ALPHA, f2

	slwi	INCX,  INCX, BASE_SHIFT
	slwi	INCY,  INCY, BASE_SHIFT

	add	INCX2, INCX, INCX
	add	INCY2, INCY, INCY

	cmpwi	cr0, N, 0
	ble	LL(999)

	andi.	r0, X, 2 * SIZE - 1
	bne	LL(100)
	andi.	r0, Y, 2 * SIZE - 1
	bne	LL(100)

	sub	X,  X, INCX2
	sub	Y,  Y, INCY2
	mr	YY, Y

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(15)

	LFPDUX	A1,   X, INCX2
	LFPDUX	B1,   Y, INCY2
	LFPDUX	A2,   X, INCX2
	LFPDUX	B2,   Y, INCY2
	LFPDUX	A3,   X, INCX2
	LFPDUX	B3,   Y, INCY2
	LFPDUX	A4,   X, INCX2
	LFPDUX	B4,   Y, INCY2

	LFPDUX	A5,   X, INCX2
	LFPDUX	B5,   Y, INCY2
	LFPDUX	A6,   X, INCX2
	LFPDUX	B6,   Y, INCY2
	LFPDUX	A7,   X, INCX2
	LFPDUX	B7,   Y, INCY2
	LFPDUX	A8,   X, INCX2
	LFPDUX	B8,   Y, INCY2

	bdz	LL(13)
	.align 4

LL(12):
	FXMADD1 C1, ALPHA, A1, B1
	LFPDUX	B1,   Y, INCY2
	FXMADD1 C2, ALPHA, A2, B2
	LFPDUX	B2,   Y, INCY2
	FXMADD1 C3, ALPHA, A3, B3
	LFPDUX	B3,   Y, INCY2
	FXMADD1 C4, ALPHA, A4, B4
	LFPDUX	B4,   Y, INCY2

	FXMADD1 C5, ALPHA, A5, B5
	LFPDUX	B5,   Y, INCY2
	FXMADD1 C6, ALPHA, A6, B6
	LFPDUX	B6,   Y, INCY2
	FXMADD1 C7, ALPHA, A7, B7
	LFPDUX	B7,   Y, INCY2
	FXMADD1 C8, ALPHA, A8, B8
	LFPDUX	B8,   Y, INCY2

	FXMADD2 C1, ALPHA, A1, C1
	LFPDUX	A1,   X, INCX2
	FXMADD2 C2, ALPHA, A2, C2
	LFPDUX	A2,   X, INCX2
	FXMADD2 C3, ALPHA, A3, C3
	LFPDUX	A3,   X, INCX2
	FXMADD2 C4, ALPHA, A4, C4
	LFPDUX	A4,   X, INCX2

	FXMADD2 C5, ALPHA, A5, C5
	LFPDUX	A5,   X, INCX2
	FXMADD2 C6, ALPHA, A6, C6
	LFPDUX	A6,   X, INCX2
	FXMADD2 C7, ALPHA, A7, C7
	LFPDUX	A7,   X, INCX2
	FXMADD2 C8, ALPHA, A8, C8
	LFPDUX	A8,   X, INCX2

	STFPDUX	C1,  YY, INCY2
	STFPDUX	C2,  YY, INCY2
	STFPDUX	C3,  YY, INCY2
	STFPDUX	C4,  YY, INCY2

	STFPDUX	C5,  YY, INCY2
	STFPDUX	C6,  YY, INCY2
	STFPDUX	C7,  YY, INCY2
	STFPDUX	C8,  YY, INCY2
	bdnz	LL(12)
	.align 4

LL(13):
	FXMADD1 C1, ALPHA, A1, B1
	FXMADD1 C2, ALPHA, A2, B2
	FXMADD1 C3, ALPHA, A3, B3
	FXMADD1 C4, ALPHA, A4, B4

	FXMADD1 C5, ALPHA, A5, B5
	FXMADD1 C6, ALPHA, A6, B6
	FXMADD1 C7, ALPHA, A7, B7
	FXMADD1 C8, ALPHA, A8, B8

	FXMADD2 C1, ALPHA, A1, C1
	FXMADD2 C2, ALPHA, A2, C2
	FXMADD2 C3, ALPHA, A3, C3
	FXMADD2 C4, ALPHA, A4, C4

	FXMADD2 C5, ALPHA, A5, C5
	FXMADD2 C6, ALPHA, A6, C6
	STFPDUX	C1,  YY, INCY2
	FXMADD2 C7, ALPHA, A7, C7
	STFPDUX	C2,  YY, INCY2
	FXMADD2 C8, ALPHA, A8, C8
	STFPDUX	C3,  YY, INCY2
	STFPDUX	C4,  YY, INCY2

	STFPDUX	C5,  YY, INCY2
	STFPDUX	C6,  YY, INCY2
	STFPDUX	C7,  YY, INCY2
	STFPDUX	C8,  YY, INCY2
	.align 4

LL(15):
	andi.	r0,  N, 7
	beq	LL(999)

	andi.	r0,  N, 4
	beq	LL(16)

	LFPDUX	A1,   X, INCX2
	LFPDUX	B1,   Y, INCY2
	LFPDUX	A2,   X, INCX2
	LFPDUX	B2,   Y, INCY2
	LFPDUX	A3,   X, INCX2
	LFPDUX	B3,   Y, INCY2
	LFPDUX	A4,   X, INCX2
	LFPDUX	B4,   Y, INCY2

	FXMADD1 C1, ALPHA, A1, B1
	FXMADD1 C2, ALPHA, A2, B2
	FXMADD1 C3, ALPHA, A3, B3
	FXMADD1 C4, ALPHA, A4, B4

	FXMADD2 C1, ALPHA, A1, C1
	FXMADD2 C2, ALPHA, A2, C2
	FXMADD2 C3, ALPHA, A3, C3
	FXMADD2 C4, ALPHA, A4, C4

	STFPDUX	C1,  YY, INCY2
	STFPDUX	C2,  YY, INCY2
	STFPDUX	C3,  YY, INCY2
	STFPDUX	C4,  YY, INCY2
	.align 4

LL(16):
	andi.	r0,  N, 2
	beq	LL(17)

	LFPDUX	A1,   X, INCX2
	LFPDUX	B1,   Y, INCY2
	LFPDUX	A2,   X, INCX2
	LFPDUX	B2,   Y, INCY2

	FXMADD1 C1, ALPHA, A1, B1
	FXMADD1 C2, ALPHA, A2, B2
	FXMADD2 C1, ALPHA, A1, C1
	FXMADD2 C2, ALPHA, A2, C2

	STFPDUX	C1,  YY, INCY2
	STFPDUX	C2,  YY, INCY2
	.align 4

LL(17):
	andi.	r0,  N, 1
	beq	LL(999)

	LFPDUX	A1,   X, INCX2
	LFPDUX	B1,   Y, INCY2

	FXMADD1	C1, ALPHA, A1, B1
	FXMADD2	C1, ALPHA, A1, C1

	STFPDUX	C1,  YY, INCY2
	b	LL(999)
	.align 4

LL(100):
	fsmtp	ALPHA_I, ALPHA_R

	sub	X,  X, INCX2
	sub	Y,  Y, INCY2

	addi	X1, X, SIZE
	addi	Y1, Y, SIZE

	mr	YY,  Y
	mr	YY1, Y1

	srawi.	r0, N, 2
	mtspr	CTR,  r0
	beq-	LL(115)

	LFDUX	A1,   X,  INCX2
	LFDUX	A2,   X1, INCX2
	LFDUX	B1,   Y,  INCY2
	LFDUX	B2,   Y1, INCY2

	LFDUX	A3,   X,  INCX2
	LFDUX	A4,   X1, INCX2
	LFDUX	B3,   Y,  INCY2
	LFDUX	B4,   Y1, INCY2

	LFDUX	A5,   X,  INCX2
	LFDUX	A6,   X1, INCX2
	LFDUX	B5,   Y,  INCY2
	LFDUX	B6,   Y1, INCY2

	LFDUX	A7,   X,  INCX2
	LFDUX	A8,   X1, INCX2
	LFDUX	B7,   Y,  INCY2
	LFDUX	B8,   Y1, INCY2
	bdz	LL(113)
	.align 4

LL(112):
	FMADD	C1,  ALPHA_R, A1, B1
	LFDUX	B1,   Y,  INCY2
	FMADD	C2,  ALPHA_I, A1, B2
	LFDUX	A1,   X,  INCX2
	FMADD	C3,  ALPHA_R, A3, B3
	LFDUX	B3,   Y,  INCY2
	FMADD	C4,  ALPHA_I, A3, B4
	LFDUX	A3,   X,  INCX2

	FMADD	C5,  ALPHA_R, A5, B5
	LFDUX	B5,   Y,  INCY2
	FMADD	C6,  ALPHA_I, A5, B6
	LFDUX	A5,   X,  INCX2
	FMADD	C7,  ALPHA_R, A7, B7
	LFDUX	B7,   Y,  INCY2
	FMADD	C8,  ALPHA_I, A7, B8
	LFDUX	A7,   X,  INCX2

	ADD1	C1,  ALPHA_I, A2, C1
	LFDUX	B2,   Y1, INCY2
	ADD2	C2,  ALPHA_R, A2, C2
	LFDUX	A2,   X1, INCX2
	ADD1	C3,  ALPHA_I, A4, C3
	LFDUX	B4,   Y1, INCY2
	ADD2	C4,  ALPHA_R, A4, C4
	LFDUX	A4,   X1, INCX2

	ADD1	C5,  ALPHA_I, A6, C5
	LFDUX	B6,   Y1, INCY2
	ADD2	C6,  ALPHA_R, A6, C6
	LFDUX	A6,   X1, INCX2
	ADD1	C7,  ALPHA_I, A8, C7
	LFDUX	B8,   Y1, INCY2
	ADD2	C8,  ALPHA_R, A8, C8
	LFDUX	A8,   X1, INCX2

	STFDUX	C1,  YY,  INCY2
	STFDUX	C2,  YY1, INCY2
	STFDUX	C3,  YY,  INCY2
	STFDUX	C4,  YY1, INCY2

	STFDUX	C5,  YY,  INCY2
	STFDUX	C6,  YY1, INCY2
	STFDUX	C7,  YY,  INCY2
	STFDUX	C8,  YY1, INCY2
	bdnz	LL(112)
	.align 4

LL(113):
	FMADD	C1,  ALPHA_R, A1, B1
	FMADD	C2,  ALPHA_I, A1, B2
	FMADD	C3,  ALPHA_R, A3, B3
	FMADD	C4,  ALPHA_I, A3, B4

	FMADD	C5,  ALPHA_R, A5, B5
	FMADD	C6,  ALPHA_I, A5, B6
	FMADD	C7,  ALPHA_R, A7, B7
	FMADD	C8,  ALPHA_I, A7, B8

	ADD1	C1,  ALPHA_I, A2, C1
	ADD2	C2,  ALPHA_R, A2, C2
	ADD1	C3,  ALPHA_I, A4, C3
	ADD2	C4,  ALPHA_R, A4, C4

	ADD1	C5,  ALPHA_I, A6, C5
	ADD2	C6,  ALPHA_R, A6, C6
	STFDUX	C1,  YY,  INCY2
	ADD1	C7,  ALPHA_I, A8, C7
	STFDUX	C2,  YY1, INCY2
	ADD2	C8,  ALPHA_R, A8, C8
	STFDUX	C3,  YY,  INCY2
	STFDUX	C4,  YY1, INCY2

	STFDUX	C5,  YY,  INCY2
	STFDUX	C6,  YY1, INCY2
	STFDUX	C7,  YY,  INCY2
	STFDUX	C8,  YY1, INCY2
	.align 4

LL(115):
	andi.	r0,  N, 3
	beq	LL(999)

	andi.	r0,  N, 2
	beq	LL(117)

	LFDUX	A1,   X,  INCX2
	LFDUX	A2,   X1, INCX2
	LFDUX	B1,   Y,  INCY2
	LFDUX	B2,   Y1, INCY2

	LFDUX	A3,   X,  INCX2
	FMADD	C1,  ALPHA_R, A1, B1
	LFDUX	A4,   X1, INCX2
	FMADD	C2,  ALPHA_I, A1, B2
	LFDUX	B3,   Y,  INCY2
	FMADD	C3,  ALPHA_R, A3, B3
	LFDUX	B4,   Y1, INCY2
	FMADD	C4,  ALPHA_I, A3, B4

	ADD1	C1,  ALPHA_I, A2, C1
	ADD2	C2,  ALPHA_R, A2, C2
	STFDUX	C1,  YY,  INCY2
	ADD1	C3,  ALPHA_I, A4, C3
	STFDUX	C2,  YY1, INCY2
	ADD2	C4,  ALPHA_R, A4, C4
	STFDUX	C3,  YY,  INCY2
	STFDUX	C4,  YY1, INCY2
	.align 4

LL(117):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	A1,   X,  INCX2
	LFDUX	A2,   X1, INCX2
	LFDUX	B1,   Y,  INCY2
	LFDUX	B2,   Y1, INCY2

	FMADD	C1,  ALPHA_R, A1, B1
	FMADD	C2,  ALPHA_I, A1, B2

	ADD1	C1,  ALPHA_I, A2, C1
	ADD2	C2,  ALPHA_R, A2, C2

	STFDUX	C1,  YY,  INCY2
	STFDUX	C2,  YY1, INCY2
	.align 4

LL(999):
	li	r10, 16
	subi	SP, SP, 16

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
