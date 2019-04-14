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

	fsmfp	ALPHA, ALPHA

	slwi	INCX,  INCX, BASE_SHIFT
	slwi	INCY,  INCY, BASE_SHIFT

	add	INCX2, INCX, INCX
	add	INCY2, INCY, INCY

	cmpwi	cr0, N, 0
	ble	LL(999)

	cmpwi	cr0, INCX, SIZE
	bne	LL(100)
	cmpwi	cr0, INCY, SIZE
	bne	LL(100)

	andi.	r0, Y, 2 * SIZE - 1
	beq	LL(05)

	LFD	A1,   0 * SIZE(X)
	LFD	B1,   0 * SIZE(Y)

	addi	X, X, SIZE
	addi	Y, Y, SIZE

	fmadd	C1, ALPHA, A1, B1
	addi	N, N, -1
	STFD	C1,  -1 * SIZE(Y)

LL(05):
	andi.	r0, X, 2 * SIZE - 1
	bne	LL(20)

	sub	X,  X, INCX2
	sub	Y,  Y, INCY2
	mr	YY, Y

	srawi.	r0, N, 4
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
	fpmadd	C1, ALPHA, A1, B1
	LFPDUX	A1,   X, INCX2
	LFPDUX	B1,   Y, INCY2
	fpmadd	C2, ALPHA, A2, B2
	LFPDUX	A2,   X, INCX2
	LFPDUX	B2,   Y, INCY2

	fpmadd	C3, ALPHA, A3, B3
	LFPDUX	A3,   X, INCX2
	LFPDUX	B3,   Y, INCY2
	fpmadd	C4, ALPHA, A4, B4
	LFPDUX	A4,   X, INCX2
	LFPDUX	B4,   Y, INCY2

	fpmadd	C5, ALPHA, A5, B5
	LFPDUX	A5,   X, INCX2
	LFPDUX	B5,   Y, INCY2
	fpmadd	C6, ALPHA, A6, B6
	LFPDUX	A6,   X, INCX2
	LFPDUX	B6,   Y, INCY2

	fpmadd	C7, ALPHA, A7, B7
	LFPDUX	A7,   X, INCX2
	LFPDUX	B7,   Y, INCY2
	fpmadd	C8, ALPHA, A8, B8
	LFPDUX	A8,   X, INCX2
	LFPDUX	B8,   Y, INCY2

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
	fpmadd	C1, ALPHA, A1, B1
	fpmadd	C2, ALPHA, A2, B2
	fpmadd	C3, ALPHA, A3, B3
	fpmadd	C4, ALPHA, A4, B4

	fpmadd	C5, ALPHA, A5, B5
	fpmadd	C6, ALPHA, A6, B6
	STFPDUX	C1,  YY, INCY2
	fpmadd	C7, ALPHA, A7, B7
	STFPDUX	C2,  YY, INCY2
	fpmadd	C8, ALPHA, A8, B8
	STFPDUX	C3,  YY, INCY2
	STFPDUX	C4,  YY, INCY2

	STFPDUX	C5,  YY, INCY2
	STFPDUX	C6,  YY, INCY2
	STFPDUX	C7,  YY, INCY2
	STFPDUX	C8,  YY, INCY2
	.align 4

LL(15):
	andi.	r0,  N, 15
	beq	LL(999)

	andi.	r0,  N, 8
	beq	LL(16)

	LFPDUX	A1,   X, INCX2
	LFPDUX	B1,   Y, INCY2
	LFPDUX	A2,   X, INCX2
	LFPDUX	B2,   Y, INCY2
	LFPDUX	A3,   X, INCX2
	LFPDUX	B3,   Y, INCY2
	LFPDUX	A4,   X, INCX2
	LFPDUX	B4,   Y, INCY2

	fpmadd	C1, ALPHA, A1, B1
	fpmadd	C2, ALPHA, A2, B2
	fpmadd	C3, ALPHA, A3, B3
	fpmadd	C4, ALPHA, A4, B4

	STFPDUX	C1,  YY, INCY2
	STFPDUX	C2,  YY, INCY2
	STFPDUX	C3,  YY, INCY2
	STFPDUX	C4,  YY, INCY2
	.align 4

LL(16):
	andi.	r0,  N, 4
	beq	LL(17)

	LFPDUX	A1,   X, INCX2
	LFPDUX	B1,   Y, INCY2
	LFPDUX	A2,   X, INCX2
	LFPDUX	B2,   Y, INCY2

	fpmadd	C1, ALPHA, A1, B1
	fpmadd	C2, ALPHA, A2, B2

	STFPDUX	C1,  YY, INCY2
	STFPDUX	C2,  YY, INCY2
	.align 4

LL(17):
	andi.	r0,  N, 2
	beq	LL(18)

	LFPDUX	A1,   X, INCX2
	LFPDUX	B1,   Y, INCY2

	fpmadd	C1, ALPHA, A1, B1

	STFPDUX	C1,  YY, INCY2
	.align 4

LL(18):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	A1,   X, INCX2
	LFDUX	B1,   Y, INCY2

	fmadd	C1, ALPHA, A1, B1
	STFDUX	C1,  YY, INCY2
	b	LL(999)
	.align 4

/* X is unaliged */

LL(20):
	LFD	A1,  0 * SIZE(X)
	addi	X,  X, SIZE
	sub	X,  X, INCX2
	sub	Y,  Y, INCY2
	mr	YY, Y

	srawi.	r0, N, 4
	mtspr	CTR,  r0
	beq-	LL(25)

	LFXDUX	A2,   X, INCX2
	LFPDUX	B1,   Y, INCY2
	LFXDUX	A3,   X, INCX2
	LFPDUX	B2,   Y, INCY2
	LFXDUX	A4,   X, INCX2
	LFPDUX	B3,   Y, INCY2
	LFXDUX	A5,   X, INCX2
	LFPDUX	B4,   Y, INCY2

	LFXDUX	A6,   X, INCX2
	LFPDUX	B5,   Y, INCY2
	LFXDUX	A7,   X, INCX2
	LFPDUX	B6,   Y, INCY2
	fsmr	A1, A2
	LFXDUX	A8,   X, INCX2
  	fsmr	A2, A3
	LFPDUX	B7,   Y, INCY2
	fsmr	A3, A4
	LFXDUX	A9,   X, INCX2
	fsmr	A4, A5
	LFPDUX	B8,   Y, INCY2
	bdz	LL(23)
	.align 4

LL(22):
	fpmadd	C1, ALPHA, A1, B1
	fsmr	A5, A6
	LFPDUX	B1,   Y, INCY2
	fpmadd	C2, ALPHA, A2, B2
	LFXDUX	A2,   X, INCX2
	fsmr	A6, A7
	LFPDUX	B2,   Y, INCY2
	fpmadd	C3, ALPHA, A3, B3
	LFXDUX	A3,   X, INCX2
	fsmr	A7, A8
	LFPDUX	B3,   Y, INCY2
	fpmadd	C4, ALPHA, A4, B4
	LFXDUX	A4,   X, INCX2
	fsmr	A8, A9
	LFPDUX	B4,   Y, INCY2

	fpmadd	C5, ALPHA, A5, B5
	LFXDUX	A5,   X, INCX2
	LFPDUX	B5,   Y, INCY2
	fpmadd	C6, ALPHA, A6, B6
	LFXDUX	A6,   X, INCX2
	LFPDUX	B6,   Y, INCY2

	fpmadd	C7, ALPHA, A7, B7
	LFXDUX	A7,   X, INCX2
	LFPDUX	B7,   Y, INCY2
	fpmadd	C8, ALPHA, A8, B8
	LFXDUX	A8,   X, INCX2
	LFPDUX	B8,   Y, INCY2

	fpmr	A1, A9
	LFXDUX	A9,   X, INCX2

	STFPDUX	C1,  YY, INCY2
	STFPDUX	C2,  YY, INCY2
	STFPDUX	C3,  YY, INCY2
	STFPDUX	C4,  YY, INCY2
	fsmr	A1, A2

	STFPDUX	C5,  YY, INCY2
	fsmr	A2, A3
	STFPDUX	C6,  YY, INCY2
	fsmr	A3, A4
	STFPDUX	C7,  YY, INCY2
	fsmr	A4, A5
	STFPDUX	C8,  YY, INCY2
	bdnz	LL(22)
	.align 4

LL(23):
	fpmadd	C1, ALPHA, A1, B1
	fsmr	A5, A6
	fpmadd	C2, ALPHA, A2, B2
	fsmr	A6, A7
	fpmadd	C3, ALPHA, A3, B3
	fsmr	A7, A8
	fpmadd	C4, ALPHA, A4, B4
	fsmr	A8, A9

	fpmadd	C5, ALPHA, A5, B5
	fpmadd	C6, ALPHA, A6, B6
	fpmadd	C7, ALPHA, A7, B7
	fpmadd	C8, ALPHA, A8, B8
	fpmr	A1, A9

	STFPDUX	C1,  YY, INCY2
	STFPDUX	C2,  YY, INCY2
	STFPDUX	C3,  YY, INCY2
	STFPDUX	C4,  YY, INCY2

	STFPDUX	C5,  YY, INCY2
	STFPDUX	C6,  YY, INCY2
	STFPDUX	C7,  YY, INCY2
	STFPDUX	C8,  YY, INCY2
	.align 4

LL(25):
	andi.	r0,  N, 15
	beq	LL(999)

	andi.	r0,  N, 8
	beq	LL(26)

	LFXDUX	A2,   X, INCX2
	LFPDUX	B1,   Y, INCY2
	LFXDUX	A3,   X, INCX2
	LFPDUX	B2,   Y, INCY2
	LFXDUX	A4,   X, INCX2
	LFPDUX	B3,   Y, INCY2
	LFXDUX	A5,   X, INCX2
	LFPDUX	B4,   Y, INCY2

	fsmr	A1, A2
	fsmr	A2, A3
	fsmr	A3, A4
	fsmr	A4, A5

	fpmadd	C1, ALPHA, A1, B1
	fpmadd	C2, ALPHA, A2, B2
	fpmadd	C3, ALPHA, A3, B3
	fpmadd	C4, ALPHA, A4, B4
	fpmr	A1, A5

	STFPDUX	C1,  YY, INCY2
	STFPDUX	C2,  YY, INCY2
	STFPDUX	C3,  YY, INCY2
	STFPDUX	C4,  YY, INCY2
	.align 4

LL(26):
	andi.	r0,  N, 4
	beq	LL(27)

	LFXDUX	A2,   X, INCX2
	LFPDUX	B1,   Y, INCY2
	LFXDUX	A3,   X, INCX2
	LFPDUX	B2,   Y, INCY2

	fsmr	A1, A2
	fsmr	A2, A3
	fpmadd	C1, ALPHA, A1, B1
	fpmadd	C2, ALPHA, A2, B2
	fpmr	A1, A3

	STFPDUX	C1,  YY, INCY2
	STFPDUX	C2,  YY, INCY2
	.align 4

LL(27):
	andi.	r0,  N, 2
	beq	LL(28)

	LFXDUX	A2,   X, INCX2
	LFPDUX	B1,   Y, INCY2

	fsmr	A1, A2
	fpmadd	C1, ALPHA, A1, B1
	fpmr	A1, A2

	STFPDUX	C1,  YY, INCY2
	.align 4

LL(28):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	B1,   Y, INCY2

	fmadd	C1, ALPHA, A1, B1
	STFDUX	C1,  YY, INCY2
	b	LL(999)
	.align 4
####


LL(100):
	sub	X,  X, INCX
	sub	Y,  Y, INCY
	mr	YY, Y

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(115)

	LFDUX	A1,   X, INCX
	LFDUX	B1,   Y, INCY
	LFDUX	A2,   X, INCX
	LFDUX	B2,   Y, INCY

	LFDUX	A3,   X, INCX
	LFDUX	B3,   Y, INCY
	LFDUX	A4,   X, INCX
	LFDUX	B4,   Y, INCY

	LFDUX	A5,   X, INCX
	LFDUX	B5,   Y, INCY
	LFDUX	A6,   X, INCX
	LFDUX	B6,   Y, INCY

	LFDUX	A7,   X, INCX
	LFDUX	B7,   Y, INCY
	LFDUX	A8,   X, INCX
	LFDUX	B8,   Y, INCY
	bdz	LL(113)
	.align 4

LL(112):
	fmadd	C1, ALPHA, A1, B1
	LFDUX	A1,   X, INCX
	LFDUX	B1,   Y, INCY

	fmadd	C2, ALPHA, A2, B2
	LFDUX	A2,   X, INCX
	LFDUX	B2,   Y, INCY

	fmadd	C3, ALPHA, A3, B3
	LFDUX	A3,   X, INCX
	LFDUX	B3,   Y, INCY

	fmadd	C4, ALPHA, A4, B4
	LFDUX	A4,   X, INCX
	LFDUX	B4,   Y, INCY

	fmadd	C5, ALPHA, A5, B5
	LFDUX	A5,   X, INCX
	LFDUX	B5,   Y, INCY
	fmadd	C6, ALPHA, A6, B6
	LFDUX	A6,   X, INCX
	LFDUX	B6,   Y, INCY
	fmadd	C7, ALPHA, A7, B7
	LFDUX	A7,   X, INCX
	LFDUX	B7,   Y, INCY
	fmadd	C8, ALPHA, A8, B8
	LFDUX	A8,   X, INCX
	LFDUX	B8,   Y, INCY

	STFDUX	C1,  YY, INCY
	STFDUX	C2,  YY, INCY
	STFDUX	C3,  YY, INCY
	STFDUX	C4,  YY, INCY

	STFDUX	C5,  YY, INCY
	STFDUX	C6,  YY, INCY
	STFDUX	C7,  YY, INCY
	STFDUX	C8,  YY, INCY
	bdnz	LL(112)
	.align 4

LL(113):
	fmadd	C1, ALPHA, A1, B1
	fmadd	C2, ALPHA, A2, B2
	fmadd	C3, ALPHA, A3, B3
	fmadd	C4, ALPHA, A4, B4

	fmadd	C5, ALPHA, A5, B5
	fmadd	C6, ALPHA, A6, B6
	STFDUX	C1,  YY, INCY
	fmadd	C7, ALPHA, A7, B7
	STFDUX	C2,  YY, INCY
	fmadd	C8, ALPHA, A8, B8
	STFDUX	C3,  YY, INCY

	STFDUX	C4,  YY, INCY
	STFDUX	C5,  YY, INCY
	STFDUX	C6,  YY, INCY
	STFDUX	C7,  YY, INCY
	STFDUX	C8,  YY, INCY
	.align 4

LL(115):
	andi.	r0,  N, 7
	beq	LL(999)
	andi.	r0,  N, 4
	beq	LL(117)

	LFDUX	A1,   X, INCX
	LFDUX	B1,   Y, INCY
	LFDUX	A2,   X, INCX
	LFDUX	B2,   Y, INCY

	LFDUX	A3,   X, INCX
	LFDUX	B3,   Y, INCY
	LFDUX	A4,   X, INCX
	LFDUX	B4,   Y, INCY

	fmadd	C1, ALPHA, A1, B1
	fmadd	C2, ALPHA, A2, B2
	fmadd	C3, ALPHA, A3, B3
	fmadd	C4, ALPHA, A4, B4

	STFDUX	C1,  YY, INCY
	STFDUX	C2,  YY, INCY
	STFDUX	C3,  YY, INCY
	STFDUX	C4,  YY, INCY
	.align 4

LL(117):
	andi.	r0,  N, 2
	beq	LL(118)

	LFDUX	A1,   X, INCX
	LFDUX	B1,   Y, INCY
	LFDUX	A2,   X, INCX
	LFDUX	B2,   Y, INCY

	fmadd	C1, ALPHA, A1, B1
	fmadd	C2, ALPHA, A2, B2

	STFDUX	C1,  YY, INCY
	STFDUX	C2,  YY, INCY
	.align 4

LL(118):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	A1,   X, INCX
	LFDUX	B1,   Y, INCY

	fmadd	C1, ALPHA, A1, B1
	STFDUX	C1,  YY, INCY
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
