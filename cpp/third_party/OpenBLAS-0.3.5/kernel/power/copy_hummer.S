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
#define Y	r6
#define INCY	r7

#define INCX2	r8
#define INCY2	r9
#define X2	r10
#define Y2	r11

#define A1	f0
#define A2	f1
#define A3	f2
#define A4	f3
#define A5	f4
#define A6	f5
#define A7	f6
#define A8	f7
#define A9	f8

#define T1	f9
#define T2	f10
#define T3	f11
#define T4	f12
#define T5	f13
#define T6	f14
#define T7	f15

	PROLOGUE
	PROFCODE

	li	r10, -16

	stfpdux	f14, SP, r10
	stfpdux	f15, SP, r10

	slwi	INCX,  INCX, BASE_SHIFT
	slwi	INCY,  INCY, BASE_SHIFT
	add	INCX2, INCX, INCX
	add	INCY2, INCY, INCY

	cmpwi	cr0, N, 0
	ble	LL(999)

	cmpwi	cr0, INCY, SIZE
	bne	LL(60)

	cmpwi	cr0, INCX, SIZE
	bne	LL(50)

	sub	X, X, INCX2
	sub	Y, Y, INCY2

	andi.	r0, X, 2 * SIZE - 1
	bne	LL(30)
	andi.	r0, Y, 2 * SIZE - 1
	bne	LL(20)
	.align 4

LL(10):  /* X : aligned     Y : aligned */
	srawi.	r0, N, 4
	mtspr	CTR,  r0
	beq-	LL(15)

	LFPDUX	A1,   X, INCX2
	LFPDUX	A2,   X, INCX2
	LFPDUX	A3,   X, INCX2
	LFPDUX	A4,   X, INCX2
	LFPDUX	A5,   X, INCX2
	LFPDUX	A6,   X, INCX2
	LFPDUX	A7,   X, INCX2
	LFPDUX	A8,   X, INCX2
	bdz	LL(13)
	.align 4

LL(12):
	STFPDUX	A1,   Y, INCY2
	LFPDUX	A1,   X, INCX2
	STFPDUX	A2,   Y, INCY2
	LFPDUX	A2,   X, INCX2
	STFPDUX	A3,   Y, INCY2
	LFPDUX	A3,   X, INCX2
	STFPDUX	A4,   Y, INCY2
	LFPDUX	A4,   X, INCX2

	STFPDUX	A5,   Y, INCY2
	LFPDUX	A5,   X, INCX2
	STFPDUX	A6,   Y, INCY2
	LFPDUX	A6,   X, INCX2
	STFPDUX	A7,   Y, INCY2
	LFPDUX	A7,   X, INCX2
	STFPDUX	A8,   Y, INCY2
	LFPDUX	A8,   X, INCX2
	bdnz	LL(12)
	.align 4

LL(13):
	STFPDUX	A1,   Y, INCY2
	STFPDUX	A2,   Y, INCY2
	STFPDUX	A3,   Y, INCY2
	STFPDUX	A4,   Y, INCY2
	STFPDUX	A5,   Y, INCY2
	STFPDUX	A6,   Y, INCY2
	STFPDUX	A7,   Y, INCY2
	STFPDUX	A8,   Y, INCY2
	.align 4

LL(15):
	andi.	r0,  N, 15
	beq	LL(999)

	andi.	r0,  N, 8
	beq	LL(16)

	LFPDUX	A1,    X, INCX2
	LFPDUX	A2,    X, INCX2
	LFPDUX	A3,    X, INCX2
	LFPDUX	A4,    X, INCX2

	STFPDUX	A1,    Y, INCY2
	STFPDUX	A2,    Y, INCY2
	STFPDUX	A3,    Y, INCY2
	STFPDUX	A4,    Y, INCY2
	.align 4

LL(16):
	andi.	r0,  N, 4
	beq	LL(17)

	LFPDUX	A1,    X, INCX2
	LFPDUX	A2,    X, INCX2

	STFPDUX	A1,    Y, INCY2
	STFPDUX	A2,    Y, INCY2
	.align 4

LL(17):
	andi.	r0,  N, 2
	beq	LL(18)

	LFPDUX	A1,    X, INCX2
	STFPDUX	A1,    Y, INCY2
	.align 4

LL(18):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	A1,    X, INCX2
	STFDUX	A1,    Y, INCY2
	.align 4
	b  LL(999)
	.align 4

LL(20):  /* X ): aligned     Y ): unaligned */

	LFXDUX	A1, X, INCX2
	addi	N, N, -1
	cmpwi	cr0, N, 0
	STFSDX	A1, Y, INCY2
	add	Y, Y, INCY
	ble	LL(999)
	.align 4

	srawi.	r0, N, 4
	mtspr	CTR,  r0
	beq-	LL(25)

	LFXDUX	T1,   X, INCX2
	LFXDUX	T2,   X, INCX2
	LFXDUX	T3,   X, INCX2
	LFXDUX	T4,   X, INCX2

	LFPDUX	A6,   X, INCX2
	fsmr	A1, T1
	LFPDUX	A7,   X, INCX2
	fsmr	T1, T2
	LFPDUX	A8,   X, INCX2
	fsmr	T2, T3
	LFPDUX	A9,   X, INCX2
	fsmr	T3, T4
	bdz	LL(23)
	.align 4

LL(22):
	STFPDUX	A1,   Y, INCY2
	fxmr	T5, A6
	STFPDUX	T1,   Y, INCY2
	fxmr	T6, A7
	STFPDUX	T2,   Y, INCY2
	fxmr	T7, A8
	STFPDUX	T3,   Y, INCY2
	fxmr	A1, A9

	fsmr	T4, T5
	LFPDUX	A2,   X, INCX2
	fsmr	T5, T6
	LFPDUX	A3,   X, INCX2
	fsmr	T6, T7
	LFPDUX	A4,   X, INCX2
	fsmr	T7, A1
	LFPDUX	A5,   X, INCX2

	STFPDUX	T4,   Y, INCY2
	fxmr	T1, A2
	STFPDUX	T5,   Y, INCY2
	fxmr	T2, A3
	STFPDUX	T6,   Y, INCY2
	fxmr	T3, A4
	STFPDUX	T7,   Y, INCY2
	fxmr	T4, A5

	LFPDUX	A6,   X, INCX2
	fsmr	A1, T1
	LFPDUX	A7,   X, INCX2
	fsmr	T1, T2
	LFPDUX	A8,   X, INCX2
	fsmr	T2, T3
	LFPDUX	A9,   X, INCX2
	fsmr	T3, T4
	bdnz	LL(22)
	.align 4

LL(23):
	STFPDUX	A1,   Y, INCY2
	fxmr	T5, A6
	STFPDUX	T1,   Y, INCY2
	fxmr	T6, A7
	STFPDUX	T2,   Y, INCY2
	fxmr	T7, A8
	STFPDUX	T3,   Y, INCY2
	fxmr	A1, A9

	fsmr	T4, T5
	fsmr	T5, T6
	fsmr	T6, T7
	fsmr	T7, A1

	STFPDUX	T4,   Y, INCY2
	STFPDUX	T5,   Y, INCY2
	STFPDUX	T6,   Y, INCY2
	STFPDUX	T7,   Y, INCY2
	.align 4

LL(25):
	andi.	r0,  N, 15
	beq	LL(999)

	andi.	r0,  N, 8
	beq	LL(26)

	LFXDUX	A2,    X, INCX2
	LFXDUX	A3,    X, INCX2
	LFXDUX	A4,    X, INCX2
	LFXDUX	A5,    X, INCX2

	fsmr	A1, A2
	fsmr	A2, A3
	fsmr	A3, A4
	fsmr	A4, A5

	STFPDUX	A1,    Y, INCY2
	STFPDUX	A2,    Y, INCY2
	STFPDUX	A3,    Y, INCY2
	STFPDUX	A4,    Y, INCY2
	fpmr	A1, A5
	.align 4

LL(26):
	andi.	r0,  N, 4
	beq	LL(27)

	LFXDUX	A2,    X, INCX2
	LFXDUX	A3,    X, INCX2
	fsmr	A1, A2
	fsmr	A2, A3
	STFPDUX	A1,    Y, INCY2
	STFPDUX	A2,    Y, INCY2
	fpmr	A1, A3
	.align 4

LL(27):
	andi.	r0,  N, 2
	beq	LL(28)

	LFXDUX	A2,    X, INCX2
	fsmr	A1, A2
	STFPDUX	A1,    Y, INCY2
	fpmr	A1, A2
	.align 4

LL(28):
	andi.	r0,  N, 1
	beq	LL(999)

	STFDUX	A1,    Y, INCY2
	b  LL(999)
	.align 4

LL(30):  /* X : unaligned   Y : aligned */
	andi.	r0, Y, 2 * SIZE - 1
	bne	LL(40)

	LFDX	A1, X, INCX2
	add	X, X, INCX

	srawi.	r0, N, 4
	mtspr	CTR,  r0
	beq-	LL(35)

	LFXDUX	T1,   X, INCX2
	LFXDUX	T2,   X, INCX2
	LFXDUX	T3,   X, INCX2
	LFXDUX	T4,   X, INCX2

	LFPDUX	A6,   X, INCX2
	fsmr	A1, T1
	LFPDUX	A7,   X, INCX2
	fsmr	T1, T2
	LFPDUX	A8,   X, INCX2
	fsmr	T2, T3
	LFPDUX	A9,   X, INCX2
	fsmr	T3, T4
	bdz	LL(33)
	.align 4

LL(32):
	fxmr	T5, A6
	STFPDUX	A1,   Y, INCY2
	fxmr	T6, A7
	STFPDUX	T1,   Y, INCY2
	fxmr	T7, A8
	STFPDUX	T2,   Y, INCY2
	fxmr	A1, A9
	STFPDUX	T3,   Y, INCY2

	fsmr	T4, T5
	LFPDUX	A2,   X, INCX2
	fsmr	T5, T6
	LFPDUX	A3,   X, INCX2
	fsmr	T6, T7
	LFPDUX	A4,   X, INCX2
	fsmr	T7, A1
	LFPDUX	A5,   X, INCX2

	STFPDUX	T4,   Y, INCY2
	fxmr	T1, A2
	STFPDUX	T5,   Y, INCY2
	fxmr	T2, A3
	STFPDUX	T6,   Y, INCY2
	fxmr	T3, A4
	STFPDUX	T7,   Y, INCY2
	fxmr	T4, A5

	LFPDUX	A6,   X, INCX2
	fsmr	A1, T1
	LFPDUX	A7,   X, INCX2
	fsmr	T1, T2
	LFPDUX	A8,   X, INCX2
	fsmr	T2, T3
	LFPDUX	A9,   X, INCX2
	fsmr	T3, T4

	bdnz	LL(32)
	.align 4

LL(33):
	STFPDUX	A1,   Y, INCY2
	fxmr	T5, A6
	STFPDUX	T1,   Y, INCY2
	fxmr	T6, A7
	STFPDUX	T2,   Y, INCY2
	fxmr	T7, A8
	STFPDUX	T3,   Y, INCY2
	fxmr	A1, A9

	fsmr	T4, T5
	fsmr	T5, T6
	fsmr	T6, T7
	fsmr	T7, A1

	STFPDUX	T4,   Y, INCY2
	STFPDUX	T5,   Y, INCY2
	STFPDUX	T6,   Y, INCY2
	STFPDUX	T7,   Y, INCY2
	.align 4

LL(35):
	andi.	r0,  N, 15
	beq	LL(999)

	andi.	r0,  N, 8
	beq	LL(36)

	LFXDUX	A2,    X, INCX2
	LFXDUX	A3,    X, INCX2
	LFXDUX	A4,    X, INCX2
	LFXDUX	A5,    X, INCX2

	fsmr	A1, A2
	fsmr	A2, A3
	fsmr	A3, A4
	fsmr	A4, A5

	STFPDUX	A1,    Y, INCY2
	STFPDUX	A2,    Y, INCY2
	STFPDUX	A3,    Y, INCY2
	STFPDUX	A4,    Y, INCY2
	fpmr	A1, A5
	.align 4

LL(36):
	andi.	r0,  N, 4
	beq	LL(37)

	LFXDUX	A2,    X, INCX2
	LFXDUX	A3,    X, INCX2
	fsmr	A1, A2
	fsmr	A2, A3
	STFPDUX	A1,    Y, INCY2
	STFPDUX	A2,    Y, INCY2
	fpmr	A1, A3
	.align 4

LL(37):
	andi.	r0,  N, 2
	beq	LL(38)

	LFXDUX	A2,    X, INCX2
	fsmr	A1, A2
	STFPDUX	A1,    Y, INCY2
	fpmr	A1, A2
	.align 4

LL(38):
	andi.	r0,  N, 1
	beq	LL(999)

	STFDUX	A1,    Y, INCY2
	b  LL(999)
	.align 4

LL(40):  /* X : unaligned   Y : unaligned */

	LFDX	A1, X, INCX2
	add	X, X, INCX
	addi	N, N, -1
	cmpwi	cr0, N, 0
	STFDX	A1, Y, INCY2
	add	Y, Y, INCY
	ble	LL(999)

	srawi.	r0, N, 4
	mtspr	CTR,  r0
	beq-	LL(45)

	LFPDUX	A1,   X, INCX2
	LFPDUX	A2,   X, INCX2
	LFPDUX	A3,   X, INCX2
	LFPDUX	A4,   X, INCX2
	LFPDUX	A5,   X, INCX2
	LFPDUX	A6,   X, INCX2
	LFPDUX	A7,   X, INCX2
	LFPDUX	A8,   X, INCX2
	bdz	LL(43)
	.align 4

LL(42):
	STFPDUX	A1,   Y, INCY2
	LFPDUX	A1,   X, INCX2
	STFPDUX	A2,   Y, INCY2
	LFPDUX	A2,   X, INCX2
	STFPDUX	A3,   Y, INCY2
	LFPDUX	A3,   X, INCX2
	STFPDUX	A4,   Y, INCY2
	LFPDUX	A4,   X, INCX2

	STFPDUX	A5,   Y, INCY2
	LFPDUX	A5,   X, INCX2
	STFPDUX	A6,   Y, INCY2
	LFPDUX	A6,   X, INCX2
	STFPDUX	A7,   Y, INCY2
	LFPDUX	A7,   X, INCX2
	STFPDUX	A8,   Y, INCY2
	LFPDUX	A8,   X, INCX2
	bdnz	LL(42)
	.align 4

LL(43):
	STFPDUX	A1,   Y, INCY2
	STFPDUX	A2,   Y, INCY2
	STFPDUX	A3,   Y, INCY2
	STFPDUX	A4,   Y, INCY2
	STFPDUX	A5,   Y, INCY2
	STFPDUX	A6,   Y, INCY2
	STFPDUX	A7,   Y, INCY2
	STFPDUX	A8,   Y, INCY2
	.align 4

LL(45):
	andi.	r0,  N, 15
	beq	LL(999)

	andi.	r0,  N, 8
	beq	LL(46)

	LFPDUX	A1,    X, INCX2
	LFPDUX	A2,    X, INCX2
	LFPDUX	A3,    X, INCX2
	LFPDUX	A4,    X, INCX2

	STFPDUX	A1,    Y, INCY2
	STFPDUX	A2,    Y, INCY2
	STFPDUX	A3,    Y, INCY2
	STFPDUX	A4,    Y, INCY2
	.align 4

LL(46):
	andi.	r0,  N, 4
	beq	LL(47)

	LFPDUX	A1,    X, INCX2
	LFPDUX	A2,    X, INCX2

	STFPDUX	A1,    Y, INCY2
	STFPDUX	A2,    Y, INCY2
	.align 4

LL(47):
	andi.	r0,  N, 2
	beq	LL(48)

	LFPDUX	A1,    X, INCX2
	STFPDUX	A1,    Y, INCY2
	.align 4

LL(48):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	A1,    X, INCX2
	STFDUX	A1,    Y, INCY2
	.align 4
	b  LL(999)
	.align 4

# INCX != 1, INCY == 1
LL(50):
	andi.	r0, Y, 2 * SIZE - 1
	beq	LL(51)

	LFD	A1, 0 * SIZE(X)
	add	X, X, INCX
	STFD	A1, 0 * SIZE(Y)
	add	Y, Y, INCY

	addi	N, N, -1
	cmpwi	cr0, N, 0
	ble	LL(999)
	.align 4

LL(51):
	sub	X, X, INCX
	sub	Y, Y, INCY2

	srawi.	r0, N, 4
	mtspr	CTR,  r0
	beq-	LL(55)
	.align 4

LL(52):
	LFDUX	A1,   X, INCX
	LFDUX	A2,   X, INCX
	LFDUX	A3,   X, INCX
	LFDUX	A4,   X, INCX
	LFDUX	A5,   X, INCX
	LFDUX	A6,   X, INCX
	LFDUX	A7,   X, INCX
	LFDUX	A8,   X, INCX

	LFDUX	A9,   X, INCX
	LFDUX	T1,   X, INCX
	LFDUX	T2,   X, INCX
	LFDUX	T3,   X, INCX
	fsmfp	A1, A2
	LFDUX	T4,   X, INCX
	fsmfp	A3, A4
	LFDUX	T5,   X, INCX
	fsmfp	A5, A6
	LFDUX	T6,   X, INCX
	fsmfp	A7, A8
	LFDUX	T7,   X, INCX
	fsmfp	A9, T1

	STFPDUX	A1,   Y, INCY2
	fsmfp	T2, T3
	STFPDUX	A3,   Y, INCY2
	fsmfp	T4, T5
	STFPDUX	A5,   Y, INCY2
	fsmfp	T6, T7
	STFPDUX	A7,   Y, INCY2
	STFPDUX	A9,   Y, INCY2
	STFPDUX	T2,   Y, INCY2
	STFPDUX	T4,   Y, INCY2
	STFPDUX	T6,   Y, INCY2
	bdnz	LL(52)
	.align 4

LL(55):
	andi.	r0,  N, 15
	beq	LL(999)

	andi.	r0,  N, 8
	beq	LL(56)

	LFDUX	A1,   X, INCX
	LFDUX	A2,   X, INCX
	LFDUX	A3,   X, INCX
	LFDUX	A4,   X, INCX
	LFDUX	A5,   X, INCX
	LFDUX	A6,   X, INCX
	LFDUX	A7,   X, INCX
	LFDUX	A8,   X, INCX

	fsmfp	A1, A2
	fsmfp	A3, A4
	fsmfp	A5, A6
	fsmfp	A7, A8

	STFPDUX	A1,   Y, INCY2
	STFPDUX	A3,   Y, INCY2
	STFPDUX	A5,   Y, INCY2
	STFPDUX	A7,   Y, INCY2
	.align 4

LL(56):
	andi.	r0,  N, 4
	beq	LL(57)

	LFDUX	A1,   X, INCX
	LFDUX	A2,   X, INCX
	LFDUX	A3,   X, INCX
	LFDUX	A4,   X, INCX

	fsmfp	A1, A2
	fsmfp	A3, A4

	STFPDUX	A1,   Y, INCY2
	STFPDUX	A3,   Y, INCY2
	.align 4

LL(57):
	andi.	r0,  N, 2
	beq	LL(58)

	LFDUX	A1,   X, INCX
	LFDUX	A2,   X, INCX

	fsmfp	A1, A2

	STFPDUX	A1,   Y, INCY2
	.align 4

LL(58):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	A1,    X, INCX
	STFDUX	A1,    Y, INCY2
	b	LL(999)
	.align 4


# INCX == 1, INCY != 1
LL(60):
	cmpwi	cr0, INCY, SIZE
	bne	LL(100)

	andi.	r0, X, 2 * SIZE - 1
	beq	LL(61)

	LFD	A1, 0 * SIZE(X)
	add	X, X, INCX
	STFD	A1, 0 * SIZE(Y)
	add	Y, Y, INCY

	addi	N, N, -1
	cmpwi	cr0, N, 0
	ble	LL(999)
	.align 4

LL(61):
	sub	X, X, INCX2
	sub	Y, Y, INCY

	srawi.	r0, N, 4
	mtspr	CTR,  r0
	beq-	LL(65)

	LFPDUX	A1,   X, INCX2
	LFPDUX	A2,   X, INCX2
	LFPDUX	A3,   X, INCX2
	LFPDUX	A4,   X, INCX2
	LFPDUX	A5,   X, INCX2
	LFPDUX	A6,   X, INCX2
	LFPDUX	A7,   X, INCX2
	LFPDUX	A8,   X, INCX2
	bdz	LL(63)
	.align 4

LL(62):
	STFDUX	A1,   Y, INCY
	STFSDUX	A1,   Y, INCY
	LFPDUX	A1,   X, INCX2

	STFDUX	A2,   Y, INCY
	STFSDUX	A2,   Y, INCY
	LFPDUX	A2,   X, INCX2

	STFDUX	A3,   Y, INCY
	STFSDUX	A3,   Y, INCY
	LFPDUX	A3,   X, INCX2

	STFDUX	A4,   Y, INCY
	STFSDUX	A4,   Y, INCY
	LFPDUX	A4,   X, INCX2

	STFDUX	A5,   Y, INCY
	STFSDUX	A5,   Y, INCY
	LFPDUX	A5,   X, INCX2

	STFDUX	A6,   Y, INCY
	STFSDUX	A6,   Y, INCY
	LFPDUX	A6,   X, INCX2

	STFDUX	A7,   Y, INCY
	STFSDUX	A7,   Y, INCY
	LFPDUX	A7,   X, INCX2

	STFDUX	A8,   Y, INCY
	STFSDUX	A8,   Y, INCY
	LFPDUX	A8,   X, INCX2
	bdnz	LL(62)
	.align 4

LL(63):
	STFDUX	A1,   Y, INCY
	STFSDUX	A1,   Y, INCY
	STFDUX	A2,   Y, INCY
	STFSDUX	A2,   Y, INCY
	STFDUX	A3,   Y, INCY
	STFSDUX	A3,   Y, INCY
	STFDUX	A4,   Y, INCY
	STFSDUX	A4,   Y, INCY
	STFDUX	A5,   Y, INCY
	STFSDUX	A5,   Y, INCY
	STFDUX	A6,   Y, INCY
	STFSDUX	A6,   Y, INCY
	STFDUX	A7,   Y, INCY
	STFSDUX	A7,   Y, INCY
	STFDUX	A8,   Y, INCY
	STFSDUX	A8,   Y, INCY
	.align 4

LL(65):
	andi.	r0,  N, 15
	beq	LL(999)

	andi.	r0,  N, 8
	beq	LL(66)

	LFPDUX	A1,   X, INCX2
	LFPDUX	A2,   X, INCX2
	LFPDUX	A3,   X, INCX2
	LFPDUX	A4,   X, INCX2

	STFDUX	A1,   Y, INCY
	STFSDUX	A1,   Y, INCY
	STFDUX	A2,   Y, INCY
	STFSDUX	A2,   Y, INCY
	STFDUX	A3,   Y, INCY
	STFSDUX	A3,   Y, INCY
	STFDUX	A4,   Y, INCY
	STFSDUX	A4,   Y, INCY
	.align 4

LL(66):
	andi.	r0,  N, 4
	beq	LL(67)

	LFPDUX	A1,   X, INCX2
	LFPDUX	A2,   X, INCX2

	STFDUX	A1,   Y, INCY
	STFSDUX	A1,   Y, INCY
	STFDUX	A2,   Y, INCY
	STFSDUX	A2,   Y, INCY
	.align 4

LL(67):
	andi.	r0,  N, 2
	beq	LL(68)

	LFPDUX	A1,   X, INCX2

	STFDUX	A1,   Y, INCY
	STFSDUX	A1,   Y, INCY
	.align 4

LL(68):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	A1,    X, INCX2
	STFDUX	A1,    Y, INCY
	b	LL(999)
	.align 4

LL(100):
	sub	X, X, INCX
	sub	Y, Y, INCY

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(115)

	LFDUX	A1,   X, INCX
	LFDUX	A2,   X, INCX
	LFDUX	A3,   X, INCX
	LFDUX	A4,   X, INCX
	LFDUX	A5,   X, INCX
	LFDUX	A6,   X, INCX
	LFDUX	A7,   X, INCX
	LFDUX	A8,   X, INCX
	bdz	LL(113)
	.align 4

LL(112):
	STFDUX	A1,   Y, INCY
	LFDUX	A1,   X, INCX
	STFDUX	A2,   Y, INCY
	LFDUX	A2,   X, INCX
	STFDUX	A3,   Y, INCY
	LFDUX	A3,   X, INCX
	STFDUX	A4,   Y, INCY
	LFDUX	A4,   X, INCX

	STFDUX	A5,   Y, INCY
	LFDUX	A5,   X, INCX
	STFDUX	A6,   Y, INCY
	LFDUX	A6,   X, INCX
	STFDUX	A7,   Y, INCY
	LFDUX	A7,   X, INCX
	STFDUX	A8,   Y, INCY
	LFDUX	A8,   X, INCX
	bdnz	LL(112)
	.align 4

LL(113):
	STFDUX	A1,   Y, INCY
	STFDUX	A2,   Y, INCY
	STFDUX	A3,   Y, INCY
	STFDUX	A4,   Y, INCY
	STFDUX	A5,   Y, INCY
	STFDUX	A6,   Y, INCY
	STFDUX	A7,   Y, INCY
	STFDUX	A8,   Y, INCY
	.align 4

LL(115):
	andi.	r0,  N, 7
	beq	LL(999)
	andi.	r0,  N, 4
	beq	LL(117)

	LFDUX	A1,    X, INCX
	LFDUX	A2,    X, INCX
	LFDUX	A3,    X, INCX
	LFDUX	A4,    X, INCX

	STFDUX	A1,    Y, INCY
	STFDUX	A2,    Y, INCY
	STFDUX	A3,    Y, INCY
	STFDUX	A4,    Y, INCY
	.align 4

LL(117):
	andi.	r0,  N, 2
	beq	LL(118)

	LFDUX	A1,    X, INCX
	LFDUX	A2,    X, INCX

	STFDUX	A1,    Y, INCY
	STFDUX	A2,    Y, INCY
	.align 4

LL(118):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	A1,    X, INCX
	STFDUX	A1,    Y, INCY
	.align 4

LL(999):
	li	r10, 16
	addi	SP, SP,  -16

	lfpdux	f15, SP, r10
	lfpdux	f14, SP, r10

	addi	SP, SP,  16
	blr

	EPILOGUE
