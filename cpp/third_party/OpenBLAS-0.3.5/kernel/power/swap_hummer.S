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

#define INCX2	r4
#define INCY2	r5
#define X2	r10
#define Y2	r11

#define A1	f0
#define A2	f1
#define A3	f2
#define A4	f3
#define A5	f4

#define B1	f5
#define B2	f6
#define B3	f7
#define B4	f8
#define B5	f9

#define T1	f10
#define T2	f11
#define T3	f12
#define T4	f13
#define T5	f14
#define T6	f15
#define T7	f16

	PROLOGUE
	PROFCODE

	li	r10, -16

	stfpdux	f14, SP, r10
	stfpdux	f15, SP, r10
	stfpdux	f16, SP, r10

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

	sub	X, X, INCX2
	sub	Y, Y, INCY2

	mr	X2, X
	mr	Y2, Y

	andi.	r0, X, 2 * SIZE - 1
	bne	LL(30)
	andi.	r0, Y, 2 * SIZE - 1
	bne	LL(20)
	.align 4

LL(10):  /* X : aligned     Y : aligned */
	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(15)

	LFPDUX	A1,    X,  INCX2
	LFPDUX	B1,    Y,  INCY2
	LFPDUX	A2,    X,  INCX2
	LFPDUX	B2,    Y,  INCY2
	LFPDUX	A3,    X,  INCX2
	LFPDUX	B3,    Y,  INCY2
	LFPDUX	A4,    X,  INCX2
	LFPDUX	B4,    Y,  INCY2
	bdz	LL(13)
	.align 4

LL(12):
	STFPDUX	B1,    X2, INCY2
	LFPDUX	B1,    Y,  INCY2
	STFPDUX	A1,    Y2, INCY2
	LFPDUX	A1,    X,  INCX2

	STFPDUX	B2,    X2, INCY2
	LFPDUX	B2,    Y,  INCY2
	STFPDUX	A2,    Y2, INCY2
	LFPDUX	A2,    X,  INCX2

	STFPDUX	B3,    X2, INCY2
	LFPDUX	B3,    Y,  INCY2
	STFPDUX	A3,    Y2, INCY2
	LFPDUX	A3,    X,  INCX2

	STFPDUX	B4,    X2, INCY2
	LFPDUX	B4,    Y,  INCY2
	STFPDUX	A4,    Y2, INCY2
	LFPDUX	A4,    X,  INCX2
	bdnz	LL(12)
	.align 4

LL(13):
	STFPDUX	B1,    X2, INCY2
	STFPDUX	A1,    Y2, INCY2
	STFPDUX	B2,    X2, INCY2
	STFPDUX	A2,    Y2, INCY2
	STFPDUX	B3,    X2, INCY2
	STFPDUX	A3,    Y2, INCY2
	STFPDUX	B4,    X2, INCY2
	STFPDUX	A4,    Y2, INCY2
	.align 4

LL(15):
	andi.	r0,  N, 7
	beq	LL(999)

	andi.	r0,  N, 4
	beq	LL(16)

	LFPDUX	A1,    X,  INCX2
	LFPDUX	B1,    Y,  INCY2
	LFPDUX	A2,    X,  INCX2
	LFPDUX	B2,    Y,  INCY2

	STFPDUX	B1,    X2, INCY2
	STFPDUX	A1,    Y2, INCY2
	STFPDUX	B2,    X2, INCY2
	STFPDUX	A2,    Y2, INCY2
	.align 4

LL(16):
	andi.	r0,  N, 2
	beq	LL(17)

	LFPDUX	A1,    X,  INCX2
	LFPDUX	B1,    Y,  INCY2

	STFPDUX	B1,    X2, INCY2
	STFPDUX	A1,    Y2, INCY2
	.align 4

LL(17):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	A1,    X,  INCX2
	LFDUX	B1,    Y,  INCY2

	STFDUX	B1,    X2, INCY2
	STFDUX	A1,    Y2, INCY2
	b  LL(999)
	.align 4

LL(20):  /* X : aligned     Y : unaligned */

	LFXDUX	A1, X, INCX2
	LFDX	B1, Y, INCY2

	STFSDX	A1, Y2, INCY2

	add	Y,  Y,  INCY
	add	Y2, Y2, INCY

	addi	N, N, -1
	cmpwi	cr0, N, 0
	ble	LL(29)
	.align 4

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(25)

	LFXDUX	T1,    X,  INCX2
	LFXDUX	T2,    Y,  INCY2
	LFXDUX	T3,    X,  INCX2
	LFXDUX	T4,    Y,  INCY2

	LFPDUX	A4,    X,  INCX2
	fsmr	A1, T1
	LFPDUX	B4,    Y,  INCY2
	fsmr	B1, T2
	LFPDUX	A5,    X,  INCX2
	fsmr	T1, T3
	LFPDUX	B5,    Y,  INCY2
	fsmr	T2, T4
	bdz	LL(23)
	.align 4

LL(22):
	fxmr	T5, A4
	STFPDUX	A1,    Y2, INCY2
	fxmr	T6, B4
	STFPDUX	B1,    X2, INCX2
	fxmr	A1, A5
	STFPDUX	T1,    Y2, INCY2
	fxmr	B1, B5
	STFPDUX	T2,    X2, INCX2

	fsmr	T3, T5
	LFPDUX	A2,    X,  INCX2
	fsmr	T4, T6
 	LFPDUX	B2,    Y,  INCY2
	fsmr	T5, A1
	LFPDUX	A3,    X,  INCX2
	fsmr	T6, B1
	LFPDUX	B3,    Y,  INCY2

	fxmr	T1, A2
	STFPDUX	T3,    Y2, INCY2
	fxmr	T2, B2
	STFPDUX	T4,    X2, INCX2
	fxmr	T3, A3
	STFPDUX	T5,    Y2, INCY2
	fxmr	T4, B3
	STFPDUX	T6,    X2, INCX2

	fsmr	A1, T1
	LFPDUX	A4,    X,  INCX2
	fsmr	B1, T2
	LFPDUX	B4,    Y,  INCY2
	fsmr	T1, T3
	LFPDUX	A5,    X,  INCX2
	fsmr	T2, T4
	LFPDUX	B5,    Y,  INCY2
	bdnz	LL(22)
	.align 4

LL(23):
	fxmr	T5, A4
	STFPDUX	A1,    Y2, INCY2
	fxmr	T6, B4
	STFPDUX	B1,    X2, INCX2
	fxmr	A1, A5
	STFPDUX	T1,    Y2, INCY2
	fxmr	B1, B5
	STFPDUX	T2,    X2, INCX2

	fsmr	T3, T5
	fsmr	T4, T6
	fsmr	T5, A1
	fsmr	T6, B1

	STFPDUX	T3,    Y2, INCY2
	STFPDUX	T4,    X2, INCX2
	STFPDUX	T5,    Y2, INCY2
	STFPDUX	T6,    X2, INCX2
	.align 4

LL(25):
	andi.	r0,  N, 7
	beq	LL(29)

	andi.	r0,  N, 4
	beq	LL(27)

	LFXDUX	A2,    X,  INCX2
	LFXDUX	B2,    Y,  INCY2
	LFXDUX	A3,    X,  INCX2
	LFXDUX	B3,    Y,  INCY2

	fsmr	A1, A2
	fsmr	B1, B2
	fsmr	A2, A3
	fsmr	B2, B3

	STFPDUX	A1,    Y2, INCY2
	STFPDUX	B1,    X2, INCX2
	STFPDUX	A2,    Y2, INCY2
	fpmr	A1, A3
	STFPDUX	B2,    X2, INCX2
	fpmr	B1, B3
	.align 4

LL(27):
	andi.	r0,  N, 2
	beq	LL(28)

	LFXDUX	A2,    X,  INCX2
	LFXDUX	B2,    Y,  INCY2
	fsmr	A1, A2
	fsmr	B1, B2
	STFPDUX	A1,    Y2, INCY2
	fpmr	A1, A2
	STFPDUX	B1,    X2, INCX2
	fpmr	B1, B2
	.align 4

LL(28):
	andi.	r0,  N, 1
	beq	LL(29)

	LFSDX	B1,    Y,  INCY2
	STFDX	A1,    Y2, INCY2
	STFDX	B1,    X2, INCX2
	add	X2, X2, INCX
	fsmtp	B1, B1
	.align 4

LL(29):
	STFDX	B1,    X2, INCX2
	b  LL(999)
	.align 4


LL(30):  /* X : unaligned   Y : aligned */

	andi.	r0, Y, 2 * SIZE - 1
	bne	LL(40)

	LFXDUX	A1, Y, INCY2
	LFDX	B1, X, INCX2

	STFSDX	A1, X2, INCX2

	add	X,  X,  INCX
	add	X2, X2, INCX

	addi	N, N, -1
	cmpwi	cr0, N, 0
	ble	LL(39)
	.align 4

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(35)

	LFXDUX	T1,    Y,  INCY2
	LFXDUX	T2,    X,  INCX2
	LFXDUX	T3,    Y,  INCY2
	LFXDUX	T4,    X,  INCX2

	LFPDUX	A4,    Y,  INCY2
	fsmr	A1, T1
	LFPDUX	B4,    X,  INCX2
	fsmr	B1, T2
	LFPDUX	A5,    Y,  INCY2
	fsmr	T1, T3
	LFPDUX	B5,    X,  INCX2
	fsmr	T2, T4
	bdz	LL(33)
	.align 4

LL(32):
	fxmr	T5, A4
	STFPDUX	A1,    X2, INCX2
	fxmr	T6, B4
	STFPDUX	B1,    Y2, INCY2
	fxmr	A1, A5
	STFPDUX	T1,    X2, INCX2
	fxmr	B1, B5
	STFPDUX	T2,    Y2, INCY2

	fsmr	T3, T5
	LFPDUX	A2,    Y,  INCY2
	fsmr	T4, T6
 	LFPDUX	B2,    X,  INCX2
	fsmr	T5, A1
	LFPDUX	A3,    Y,  INCY2
	fsmr	T6, B1
	LFPDUX	B3,    X,  INCX2

	fxmr	T1, A2
	STFPDUX	T3,    X2, INCX2
	fxmr	T2, B2
	STFPDUX	T4,    Y2, INCY2
	fxmr	T3, A3
	STFPDUX	T5,    X2, INCX2
	fxmr	T4, B3
	STFPDUX	T6,    Y2, INCY2

	fsmr	A1, T1
	LFPDUX	A4,    Y,  INCY2
	fsmr	B1, T2
	LFPDUX	B4,    X,  INCX2
	fsmr	T1, T3
	LFPDUX	A5,    Y,  INCY2
	fsmr	T2, T4
	LFPDUX	B5,    X,  INCX2
	bdnz	LL(32)
	.align 4

LL(33):
	fxmr	T5, A4
	STFPDUX	A1,    X2, INCX2
	fxmr	T6, B4
	STFPDUX	B1,    Y2, INCY2
	fxmr	A1, A5
	STFPDUX	T1,    X2, INCX2
	fxmr	B1, B5
	STFPDUX	T2,    Y2, INCY2

	fsmr	T3, T5
	fsmr	T4, T6
	fsmr	T5, A1
	fsmr	T6, B1

	STFPDUX	T3,    X2, INCX2
	STFPDUX	T4,    Y2, INCY2
	STFPDUX	T5,    X2, INCX2
	STFPDUX	T6,    Y2, INCY2
	.align 4

LL(35):
	andi.	r0,  N, 7
	beq	LL(39)

	andi.	r0,  N, 4
	beq	LL(37)

	LFXDUX	A2,    Y,  INCY2
	LFXDUX	B2,    X,  INCX2
	LFXDUX	A3,    Y,  INCY2
	LFXDUX	B3,    X,  INCX2

	fsmr	A1, A2
	fsmr	B1, B2
	fsmr	A2, A3
	fsmr	B2, B3

	STFPDUX	A1,    X2, INCX2
	STFPDUX	B1,    Y2, INCY2
	STFPDUX	A2,    X2, INCX2
	fpmr	A1, A3
	STFPDUX	B2,    Y2, INCY2
	fpmr	B1, B3
	.align 4

LL(37):
	andi.	r0,  N, 2
	beq	LL(38)

	LFXDUX	A2,    Y,  INCY2
	LFXDUX	B2,    X,  INCX2
	fsmr	A1, A2
	fsmr	B1, B2
	STFPDUX	A1,    X2, INCX2
	fpmr	A1, A2
	STFPDUX	B1,    Y2, INCY2
	fpmr	B1, B2
	.align 4

LL(38):
	andi.	r0,  N, 1
	beq	LL(39)

	LFSDX	B1,    X,  INCX2
	STFDX	A1,    X2, INCX2
	STFDX	B1,    Y2, INCY2
	add	Y2, Y2, INCY
	fsmtp	B1, B1
	.align 4

LL(39):
	STFDX	B1,    Y2, INCY2
	b  LL(999)
	.align 4

LL(40):  /* X : unaligned   Y : unaligned */

	LFDX	A1, Y, INCY2
	LFDX	B1, X, INCX2
	add	X,  X,  INCX
	add	Y,  Y,  INCY

	addi	N, N, -1
	cmpwi	cr0, N, 0

	STFDX	A1, X2, INCX2
	STFDX	B1, Y2, INCY2
	add	X2, X2, INCX
	add	Y2, Y2, INCY
	ble	LL(999)

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(45)

	LFPDUX	A1,    X,  INCX2
	LFPDUX	B1,    Y,  INCY2
	LFPDUX	A2,    X,  INCX2
	LFPDUX	B2,    Y,  INCY2
	LFPDUX	A3,    X,  INCX2
	LFPDUX	B3,    Y,  INCY2
	LFPDUX	A4,    X,  INCX2
	LFPDUX	B4,    Y,  INCY2
	bdz	LL(43)
	.align 4

LL(42):
	STFPDUX	B1,    X2, INCY2
	LFPDUX	B1,    Y,  INCY2
	STFPDUX	A1,    Y2, INCY2
	LFPDUX	A1,    X,  INCX2

	STFPDUX	B2,    X2, INCY2
	LFPDUX	B2,    Y,  INCY2
	STFPDUX	A2,    Y2, INCY2
	LFPDUX	A2,    X,  INCX2

	STFPDUX	B3,    X2, INCY2
	LFPDUX	B3,    Y,  INCY2
	STFPDUX	A3,    Y2, INCY2
	LFPDUX	A3,    X,  INCX2

	STFPDUX	B4,    X2, INCY2
	LFPDUX	B4,    Y,  INCY2
	STFPDUX	A4,    Y2, INCY2
	LFPDUX	A4,    X,  INCX2
	bdnz	LL(42)
	.align 4

LL(43):
	STFPDUX	B1,    X2, INCY2
	STFPDUX	A1,    Y2, INCY2
	STFPDUX	B2,    X2, INCY2
	STFPDUX	A2,    Y2, INCY2
	STFPDUX	B3,    X2, INCY2
	STFPDUX	A3,    Y2, INCY2
	STFPDUX	B4,    X2, INCY2
	STFPDUX	A4,    Y2, INCY2
	.align 4

LL(45):
	andi.	r0,  N, 7
	beq	LL(999)

	andi.	r0,  N, 4
	beq	LL(46)

	LFPDUX	A1,    X,  INCX2
	LFPDUX	B1,    Y,  INCY2
	LFPDUX	A2,    X,  INCX2
	LFPDUX	B2,    Y,  INCY2

	STFPDUX	B1,    X2, INCY2
	STFPDUX	A1,    Y2, INCY2
	STFPDUX	B2,    X2, INCY2
	STFPDUX	A2,    Y2, INCY2
	.align 4

LL(46):
	andi.	r0,  N, 2
	beq	LL(47)

	LFPDUX	A1,    X,  INCX2
	LFPDUX	B1,    Y,  INCY2

	STFPDUX	B1,    X2, INCY2
	STFPDUX	A1,    Y2, INCY2
	.align 4

LL(47):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	A1,    X,  INCX2
	LFDUX	B1,    Y,  INCY2

	STFDUX	B1,    X2, INCY2
	STFDUX	A1,    Y2, INCY2
	b  LL(999)
	.align 4

LL(100):
	sub	X, X, INCX
	sub	Y, Y, INCY

	mr	X2, X
	mr	Y2, Y

	srawi.	r0, N, 2
	mtspr	CTR,  r0
	beq-	LL(115)

	LFDUX	A1,   X,  INCX
	LFDUX	B1,   Y,  INCY
	LFDUX	A2,   X,  INCX
	LFDUX	B2,   Y,  INCY
	LFDUX	A3,   X,  INCX
	LFDUX	B3,   Y,  INCY
	LFDUX	A4,   X,  INCX
	LFDUX	B4,   Y,  INCY
	bdz	LL(113)
	.align 4

LL(112):
	STFDUX	B1,   X2, INCX
	LFDUX	B1,   Y,  INCY
	STFDUX	A1,   Y2, INCY
	LFDUX	A1,   X,  INCX

	STFDUX	B2,   X2, INCX
	LFDUX	B2,   Y,  INCY
	STFDUX	A2,   Y2, INCY
	LFDUX	A2,   X,  INCX

	STFDUX	B3,   X2, INCX
	LFDUX	B3,   Y,  INCY
	STFDUX	A3,   Y2, INCY
	LFDUX	A3,   X,  INCX

	STFDUX	B4,   X2, INCX
	LFDUX	B4,   Y,  INCY
	STFDUX	A4,   Y2, INCY
	LFDUX	A4,   X,  INCX
	bdnz	LL(112)
	.align 4

LL(113):
	STFDUX	B1,   X2, INCX
	STFDUX	A1,   Y2, INCY
	STFDUX	B2,   X2, INCX
	STFDUX	A2,   Y2, INCY

	STFDUX	B3,   X2, INCX
	STFDUX	A3,   Y2, INCY
	STFDUX	B4,   X2, INCX
	STFDUX	A4,   Y2, INCY
	.align 4

LL(115):
	andi.	r0,  N, 3
	beq	LL(999)
	andi.	r0,  N, 2
	beq	LL(117)

	LFDUX	A1,   X,  INCX
	LFDUX	A2,   X,  INCX
	LFDUX	B1,   Y,  INCY
	LFDUX	B2,   Y,  INCY

	STFDUX	B1,   X2, INCX
	STFDUX	B2,   X2, INCX
	STFDUX	A1,   Y2, INCY
	STFDUX	A2,   Y2, INCY
	.align 4

LL(117):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	A1,   X,  INCX
	LFDUX	B1,   Y,  INCY
	STFDUX	B1,   X2, INCX
	STFDUX	A1,   Y2, INCY
	.align 4

LL(999):
	li	r10, 16
	addi	SP, SP,  -16

	lfpdux	f16, SP, r10
	lfpdux	f15, SP, r10
	lfpdux	f14, SP, r10

	addi	SP, SP,  16
	blr

	EPILOGUE
