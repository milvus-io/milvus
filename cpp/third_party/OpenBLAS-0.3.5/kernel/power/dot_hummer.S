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

#define C1	f1
#define C2	f0
#define C3	f2
#define C4	f3

#define A1	f4
#define A2	f5
#define A3	f6
#define A4	f7
#define A5	f8
#define A6	f9
#define A7	f10
#define A8	f11
#define A9	f20

#define B1	f12
#define B2	f13
#define B3	f14
#define B4	f15
#define B5	f16
#define B6	f17
#define B7	f18
#define B8	f19
#define	B9	f20


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

	li	r10,   0
	stwu	r10,   -4(SP)
	stwu	r10,   -4(SP)
	stwu	r10,   -4(SP)
	stwu	r10,   -4(SP)

#ifdef F_INTERFACE
	LDINT	N,    0(N)
	LDINT	INCX, 0(INCX)
	LDINT	INCY, 0(INCY)
#endif

	lfpdx	C1, SP, r10		# Zero clear

	slwi	INCX,  INCX, BASE_SHIFT
	add	INCX2, INCX, INCX
	fpmr	C2, C1

	slwi	INCY,  INCY, BASE_SHIFT
	fpmr	C3, C1
	add	INCY2, INCY, INCY
	fpmr	C4, C1

	cmpwi	cr0, N, 0
	ble	LL(999)

	cmpwi	cr0, INCX, SIZE
	bne	LL(100)
	cmpwi	cr0, INCY, SIZE
	bne	LL(100)


/* X is aligned, Y is aligned */
LL(10):
	andi.	r0, X, 2 * SIZE - 1
	bne	LL(30)

	andi.	r0, Y, 2 * SIZE - 1
	bne	LL(20)

	sub	X, X, INCX2
	sub	Y, Y, INCY2

	srawi.	r0, N, 4
	mtspr	CTR,  r0
	beq-	LL(15)

	LFPDUX	A1,    X, INCX2
	LFPDUX	B1,    Y, INCY2
	LFPDUX	A2,    X, INCX2
	LFPDUX	B2,    Y, INCY2

	LFPDUX	A3,    X, INCX2
	LFPDUX	B3,    Y, INCY2
	LFPDUX	A4,    X, INCX2
	LFPDUX	B4,    Y, INCY2

	LFPDUX	A5,    X, INCX2
	LFPDUX	B5,    Y, INCY2
	LFPDUX	A6,    X, INCX2
	LFPDUX	B6,    Y, INCY2

	LFPDUX	A7,    X, INCX2
	LFPDUX	B7,    Y, INCY2
	LFPDUX	A8,    X, INCX2
	LFPDUX	B8,    Y, INCY2
	bdz	LL(14)
	.align 4

LL(13):
	fpmadd	C1, A1, B1, C1
	LFPDUX	A1,    X, INCX2
	LFPDUX	B1,    Y, INCY2
	fpmadd	C2, A2, B2, C2
	LFPDUX	A2,    X, INCX2
	LFPDUX	B2,    Y, INCY2
	fpmadd	C3, A3, B3, C3
	LFPDUX	A3,    X, INCX2
	LFPDUX	B3,    Y, INCY2
	fpmadd	C4, A4, B4, C4
	LFPDUX	A4,    X, INCX2
	LFPDUX	B4,    Y, INCY2

	fpmadd	C1, A5, B5, C1
	LFPDUX	A5,    X, INCX2
	LFPDUX	B5,    Y, INCY2
	fpmadd	C2, A6, B6, C2
	LFPDUX	A6,    X, INCX2
	LFPDUX	B6,    Y, INCY2
	fpmadd	C3, A7, B7, C3
	LFPDUX	A7,    X, INCX2
	LFPDUX	B7,    Y, INCY2
	fpmadd	C4, A8, B8, C4
	LFPDUX	A8,    X, INCX2
	LFPDUX	B8,    Y, INCY2

	bdnz	LL(13)
	.align 4

LL(14):
	fpmadd	C1, A1, B1, C1
	fpmadd	C2, A2, B2, C2
	fpmadd	C3, A3, B3, C3
	fpmadd	C4, A4, B4, C4
	fpmadd	C1, A5, B5, C1
	fpmadd	C2, A6, B6, C2
	fpmadd	C3, A7, B7, C3
	fpmadd	C4, A8, B8, C4
	.align 4

LL(15):
	andi.	r0,  N, 15
	beq	LL(999)

	andi.	r0,  N, 8
	beq	LL(16)

	LFPDUX	A1,    X, INCX2
	LFPDUX	B1,    Y, INCY2
	LFPDUX	A2,    X, INCX2
	LFPDUX	B2,    Y, INCY2
	LFPDUX	A3,    X, INCX2
	LFPDUX	B3,    Y, INCY2
	LFPDUX	A4,    X, INCX2
	LFPDUX	B4,    Y, INCY2

	fpmadd	C1, A1, B1, C1
	fpmadd	C2, A2, B2, C2
	fpmadd	C3, A3, B3, C3
	fpmadd	C4, A4, B4, C4
	.align 4

LL(16):
	andi.	r0,  N, 4
	beq	LL(17)

	LFPDUX	A1,    X, INCX2
	LFPDUX	B1,    Y, INCY2
	LFPDUX	A2,    X, INCX2
	LFPDUX	B2,    Y, INCY2

	fpmadd	C1, A1, B1, C1
	fpmadd	C2, A2, B2, C2
	.align 4

LL(17):
	andi.	r0,  N, 2
	beq	LL(18)

	LFPDUX	A1,    X, INCX2
	LFPDUX	B1,    Y, INCY2

	fpmadd	C1, A1, B1, C1
	.align 4

LL(18):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	A1,    X, INCX2
	LFDUX	B1,    Y, INCY2

	fmadd	C1, A1, B1, C1
	b	LL(999)
	.align 4

/* X is aligned, Y is NOT aligned */

LL(20):
	LFD	B1, 0 * SIZE(Y)
	sub	X, X, INCX2
	sub	Y, Y, INCY

	srawi.	r0, N, 4
	mtspr	CTR,  r0
	beq-	LL(25)

	LFPDUX	A1,    X, INCX2
	LFXDUX	B2,    Y, INCY2
	LFPDUX	A2,    X, INCX2
	LFXDUX	B3,    Y, INCY2

	LFPDUX	A3,    X, INCX2
	LFXDUX	B4,    Y, INCY2
	LFPDUX	A4,    X, INCX2
	LFXDUX	B5,    Y, INCY2

	LFPDUX	A5,    X, INCX2
	LFXDUX	B6,    Y, INCY2
	LFPDUX	A6,    X, INCX2
	LFXDUX	B7,    Y, INCY2

	LFPDUX	A7,    X, INCX2
	fsmr	B1, B2
	LFXDUX	B8,    Y, INCY2
	fsmr	B2, B3
	LFPDUX	A8,    X, INCX2
	fsmr	B3, B4
	bdz	LL(24)
	.align 4

LL(23):
	fpmadd	C1, A1, B1, C1
	LFPDUX	A1,    X, INCX2
	fsmr	B4, B5
	LFXDUX	B9,    Y, INCY2

	fpmadd	C2, A2, B2, C2
	LFPDUX	A2,    X, INCX2
	fsmr	B5, B6
	LFXDUX	B2,    Y, INCY2

	fpmadd	C3, A3, B3, C3
	LFXDUX	B3,    Y, INCY2
	fsmr	B6, B7
	LFPDUX	A3,    X, INCX2

	fpmadd	C4, A4, B4, C4
	LFXDUX	B4,    Y, INCY2
	fsmr	B7, B8
	LFPDUX	A4,    X, INCX2

	fpmadd	C1, A5, B5, C1
	LFXDUX	B5,    Y, INCY2
	fsmr	B8, B9
	LFPDUX	A5,    X, INCX2

	fpmadd	C2, A6, B6, C2
	LFXDUX	B6,    Y, INCY2
	fpmr	B1, B9
	LFPDUX	A6,    X, INCX2

	fpmadd	C3, A7, B7, C3
	LFXDUX	B7,    Y, INCY2
	fsmr	B1, B2
	LFPDUX	A7,    X, INCX2

	fpmadd	C4, A8, B8, C4
	LFXDUX	B8,    Y, INCY2
	fsmr	B2, B3
	LFPDUX	A8,    X, INCX2

	fsmr	B3, B4
	bdnz	LL(23)
	.align 4

LL(24):
	LFXDUX	B9,    Y, INCY2
	fpmadd	C1, A1, B1, C1
	fsmr	B4, B5
	fpmadd	C2, A2, B2, C2
	fsmr	B5, B6
	fpmadd	C3, A3, B3, C3
	fsmr	B6, B7
	fpmadd	C4, A4, B4, C4
	fsmr	B7, B8
	fpmadd	C1, A5, B5, C1
	fsmr	B8, B9
	fpmadd	C2, A6, B6, C2
	fpmr	B1, B9
	fpmadd	C3, A7, B7, C3
	fpmadd	C4, A8, B8, C4
	.align 4

LL(25):
	andi.	r0,  N, 15
	beq	LL(999)

	andi.	r0,  N, 8
	beq	LL(26)

	LFPDUX	A1,    X, INCX2
	LFXDUX	B2,    Y, INCY2
	LFPDUX	A2,    X, INCX2
	LFXDUX	B3,    Y, INCY2
	LFPDUX	A3,    X, INCX2
	LFXDUX	B4,    Y, INCY2
	LFPDUX	A4,    X, INCX2
	LFXDUX	B5,    Y, INCY2

	fsmr	B1, B2
	fsmr	B2, B3
	fsmr	B3, B4
	fsmr	B4, B5

	fpmadd	C1, A1, B1, C1
	fpmadd	C2, A2, B2, C2
	fpmadd	C3, A3, B3, C3
	fpmadd	C4, A4, B4, C4
	fpmr	B1, B5
	.align 4

LL(26):
	andi.	r0,  N, 4
	beq	LL(27)

	LFPDUX	A1,    X, INCX2
	LFXDUX	B2,    Y, INCY2
	LFPDUX	A2,    X, INCX2
	LFXDUX	B3,    Y, INCY2

	fsmr	B1, B2
	fsmr	B2, B3
	fpmadd	C1, A1, B1, C1
	fpmr	B1, B3
	fpmadd	C2, A2, B2, C2
	.align 4

LL(27):
	andi.	r0,  N, 2
	beq	LL(28)

	LFPDUX	A1,    X, INCX2
	LFXDUX	B2,    Y, INCY2
	fsmr	B1, B2
	fpmadd	C1, A1, B1, C1
	fpmr	B1, B2
	.align 4

LL(28):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	A1,    X, INCX2
	fmadd	C1, A1, B1, C1
	b	LL(999)
	.align 4

/* X is not aligned, Y is aligned */
LL(30):
	andi.	r0, Y, 2 * SIZE - 1
	bne	LL(40)

	LFD	A1, 0 * SIZE(X)
	sub	X, X, INCX
	sub	Y, Y, INCY2

	srawi.	r0, N, 4
	mtspr	CTR,  r0
	beq-	LL(35)

	LFXDUX	A2,    X, INCX2
	LFPDUX	B1,    Y, INCY2
	LFXDUX	A3,    X, INCX2
	LFPDUX	B2,    Y, INCY2

	LFXDUX	A4,    X, INCX2
	LFPDUX	B3,    Y, INCY2
	LFXDUX	A5,    X, INCX2
	LFPDUX	B4,    Y, INCY2

	LFXDUX	A6,    X, INCX2
	LFPDUX	B5,    Y, INCY2
	LFXDUX	A7,    X, INCX2
	LFPDUX	B6,    Y, INCY2

	LFXDUX	A8,    X, INCX2
	fsmr	A1, A2
	LFPDUX	B7,    Y, INCY2
	fsmr	A2, A3
	LFPDUX	B8,    Y, INCY2
	fsmr	A3, A4
	bdz	LL(34)
	.align 4

LL(33):
	fpmadd	C1, A1, B1, C1
	LFXDUX	A9,    X, INCX2
	fsmr	A4, A5
	LFPDUX	B1,    Y, INCY2

	fpmadd	C2, A2, B2, C2
	LFXDUX	A2,    X, INCX2
	fsmr	A5, A6
	LFPDUX	B2,    Y, INCY2

	fpmadd	C3, A3, B3, C3
	LFXDUX	A3,    X, INCX2
	fsmr	A6, A7
	LFPDUX	B3,    Y, INCY2

	fpmadd	C4, A4, B4, C4
	LFXDUX	A4,    X, INCX2
	fsmr	A7, A8
	LFPDUX	B4,    Y, INCY2

	fpmadd	C1, A5, B5, C1
	LFXDUX	A5,    X, INCX2
	fsmr	A8, A9
	LFPDUX	B5,    Y, INCY2

	fpmadd	C2, A6, B6, C2
	LFXDUX	A6,    X, INCX2
	fpmr	A1, A9
	LFPDUX	B6,    Y, INCY2

	fpmadd	C3, A7, B7, C3
	LFXDUX	A7,    X, INCX2
	fsmr	A1, A2
	LFPDUX	B7,    Y, INCY2

	fpmadd	C4, A8, B8, C4
	LFXDUX	A8,    X, INCX2
	fsmr	A2, A3
	LFPDUX	B8,    Y, INCY2

	fsmr	A3, A4
	bdnz	LL(33)
	.align 4

LL(34):
	LFXDUX	A9,    X, INCX2
	fpmadd	C1, A1, B1, C1
	fsmr	A4, A5
	fpmadd	C2, A2, B2, C2
	fsmr	A5, A6
	fpmadd	C3, A3, B3, C3
	fsmr	A6, A7
	fpmadd	C4, A4, B4, C4
	fsmr	A7, A8
	fpmadd	C1, A5, B5, C1
	fsmr	A8, A9
	fpmadd	C2, A6, B6, C2
	fpmr	A1, A9
	fpmadd	C3, A7, B7, C3
	fpmadd	C4, A8, B8, C4
	.align 4

LL(35):
	andi.	r0,  N, 15
	beq	LL(999)

	andi.	r0,  N, 8
	beq	LL(36)

	LFXDUX	A2,    X, INCX2
	LFPDUX	B1,    Y, INCY2
	LFXDUX	A3,    X, INCX2
	LFPDUX	B2,    Y, INCY2
	LFXDUX	A4,    X, INCX2
	LFPDUX	B3,    Y, INCY2
	LFXDUX	A5,    X, INCX2
	LFPDUX	B4,    Y, INCY2

	fsmr	A1, A2
	fsmr	A2, A3
	fsmr	A3, A4
	fsmr	A4, A5

	fpmadd	C1, A1, B1, C1
	fpmr	A1, A5
	fpmadd	C2, A2, B2, C2
	fpmadd	C3, A3, B3, C3
	fpmadd	C4, A4, B4, C4
	.align 4

LL(36):
	andi.	r0,  N, 4
	beq	LL(37)

	LFXDUX	A2,    X, INCX2
	LFPDUX	B1,    Y, INCY2
	LFXDUX	A3,    X, INCX2
	LFPDUX	B2,    Y, INCY2

	fsmr	A1, A2
	fsmr	A2, A3
	fpmadd	C1, A1, B1, C1
	fpmr	A1, A3
	fpmadd	C2, A2, B2, C2
	.align 4

LL(37):
	andi.	r0,  N, 2
	beq	LL(38)

	LFXDUX	A2,    X, INCX2
	LFPDUX	B1,    Y, INCY2

	fsmr	A1, A2
	fpmadd	C1, A1, B1, C1
	fpmr	A1, A2
	.align 4

LL(38):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	B1,    Y, INCY2
	fmadd	C1, A1, B1, C1
	b	LL(999)
	.align 4

/* X is NOT  aligned, Y is NOT aligned */
LL(40):
	LFD	A1, 0 * SIZE(X)
	LFD	B1, 0 * SIZE(Y)

	sub	X, X, INCX
	sub	Y, Y, INCY

	addi	N, N, -1
	cmpwi	cr0, N, 0
	fmadd	C1, A1, B1, C1
	ble	LL(999)

	srawi.	r0, N, 4
	mtspr	CTR,  r0
	beq-	LL(45)

	LFPDUX	A1,    X, INCX2
	LFPDUX	B1,    Y, INCY2
	LFPDUX	A2,    X, INCX2
	LFPDUX	B2,    Y, INCY2

	LFPDUX	A3,    X, INCX2
	LFPDUX	B3,    Y, INCY2
	LFPDUX	A4,    X, INCX2
	LFPDUX	B4,    Y, INCY2

	LFPDUX	A5,    X, INCX2
	LFPDUX	B5,    Y, INCY2
	LFPDUX	A6,    X, INCX2
	LFPDUX	B6,    Y, INCY2

	LFPDUX	A7,    X, INCX2
	LFPDUX	B7,    Y, INCY2
	LFPDUX	A8,    X, INCX2
	LFPDUX	B8,    Y, INCY2
	bdz	LL(44)
	.align 4

LL(43):
	fpmadd	C1, A1, B1, C1
	LFPDUX	A1,    X, INCX2
	LFPDUX	B1,    Y, INCY2
	fpmadd	C2, A2, B2, C2
	LFPDUX	A2,    X, INCX2
	LFPDUX	B2,    Y, INCY2
	fpmadd	C3, A3, B3, C3
	LFPDUX	A3,    X, INCX2
	LFPDUX	B3,    Y, INCY2
	fpmadd	C4, A4, B4, C4
	LFPDUX	A4,    X, INCX2
	LFPDUX	B4,    Y, INCY2
	fpmadd	C1, A5, B5, C1
	LFPDUX	A5,    X, INCX2
	LFPDUX	B5,    Y, INCY2
	fpmadd	C2, A6, B6, C2
	LFPDUX	A6,    X, INCX2
	LFPDUX	B6,    Y, INCY2
	fpmadd	C3, A7, B7, C3
	LFPDUX	A7,    X, INCX2
	LFPDUX	B7,    Y, INCY2
	fpmadd	C4, A8, B8, C4
	LFPDUX	A8,    X, INCX2
	LFPDUX	B8,    Y, INCY2
	bdnz	LL(43)
	.align 4

LL(44):
	fpmadd	C1, A1, B1, C1
	fpmadd	C2, A2, B2, C2
	fpmadd	C3, A3, B3, C3
	fpmadd	C4, A4, B4, C4
	fpmadd	C1, A5, B5, C1
	fpmadd	C2, A6, B6, C2
	fpmadd	C3, A7, B7, C3
	fpmadd	C4, A8, B8, C4
	.align 4

LL(45):
	andi.	r0,  N, 15
	beq	LL(999)

	andi.	r0,  N, 8
	beq	LL(46)

	LFPDUX	A1,    X, INCX2
	LFPDUX	B1,    Y, INCY2
	LFPDUX	A2,    X, INCX2
	LFPDUX	B2,    Y, INCY2
	LFPDUX	A3,    X, INCX2
	LFPDUX	B3,    Y, INCY2
	LFPDUX	A4,    X, INCX2
	LFPDUX	B4,    Y, INCY2

	fpmadd	C1, A1, B1, C1
	fpmadd	C2, A2, B2, C2
	fpmadd	C3, A3, B3, C3
	fpmadd	C4, A4, B4, C4
	.align 4

LL(46):
	andi.	r0,  N, 4
	beq	LL(47)

	LFPDUX	A1,    X, INCX2
	LFPDUX	B1,    Y, INCY2
	LFPDUX	A2,    X, INCX2
	LFPDUX	B2,    Y, INCY2

	fpmadd	C1, A1, B1, C1
	fpmadd	C2, A2, B2, C2
	.align 4

LL(47):
	andi.	r0,  N, 2
	beq	LL(48)

	LFPDUX	A1,    X, INCX2
	LFPDUX	B1,    Y, INCY2

	fpmadd	C1, A1, B1, C1
	.align 4

LL(48):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	A1,    X, INCX2
	LFDUX	B1,    Y, INCY2

	fmadd	C1, A1, B1, C1
	b	LL(999)
	.align 4

LL(100):
#ifdef F_INTERFACE
	cmpwi	cr0, INCX, 0
	bge+	LL(101)

	subi	r0, N, 1
	mullw	r0, r0, INCX
	sub	X, X, r0
	.align 4

LL(101):
	cmpwi	cr0, INCY, 0
	bge+	LL(102)

	subi	r0, N, 1
	mullw	r0, r0, INCY
	sub	Y, Y, r0
	.align 4

LL(102):
#endif
	sub	X, X, INCX
	sub	Y, Y, INCY

	srawi.	r0, N, 3
	mtspr	CTR,  r0
	beq-	LL(105)

	LFDUX	A1,    X, INCX
	LFDUX	B1,    Y, INCY
	LFDUX	A2,    X, INCX
	LFDUX	B2,    Y, INCY

	LFDUX	A3,    X, INCX
	LFDUX	B3,    Y, INCY
	LFDUX	A4,    X, INCX
	LFDUX	B4,    Y, INCY

	LFDUX	A5,    X, INCX
	LFDUX	B5,    Y, INCY
	LFDUX	A6,    X, INCX
	LFDUX	B6,    Y, INCY

	LFDUX	A7,    X, INCX
	LFDUX	B7,    Y, INCY
	LFDUX	A8,    X, INCX
	LFDUX	B8,    Y, INCY
	bdz	LL(104)
	.align 4

LL(103):
	fmadd	C1, A1, B1, C1
	LFDUX	A1,    X, INCX
	LFDUX	B1,    Y, INCY
	fmadd	C2, A2, B2, C2
	LFDUX	A2,    X, INCX
	LFDUX	B2,    Y, INCY

	fmadd	C3, A3, B3, C3
	LFDUX	A3,    X, INCX
	LFDUX	B3,    Y, INCY
	fmadd	C4, A4, B4, C4
	LFDUX	A4,    X, INCX
	LFDUX	B4,    Y, INCY

	fmadd	C1, A5, B5, C1
	LFDUX	A5,    X, INCX
	LFDUX	B5,    Y, INCY
	fmadd	C2, A6, B6, C2
	LFDUX	A6,    X, INCX
	LFDUX	B6,    Y, INCY

	fmadd	C3, A7, B7, C3
	LFDUX	A7,    X, INCX
	LFDUX	B7,    Y, INCY
	fmadd	C4, A8, B8, C4
	LFDUX	A8,    X, INCX
	LFDUX	B8,    Y, INCY

	bdnz	LL(103)
	.align 4

LL(104):
	fmadd	C1, A1, B1, C1
	fmadd	C2, A2, B2, C2
	fmadd	C3, A3, B3, C3
	fmadd	C4, A4, B4, C4
	fmadd	C1, A5, B5, C1
	fmadd	C2, A6, B6, C2
	fmadd	C3, A7, B7, C3
	fmadd	C4, A8, B8, C4
	.align 4

LL(105):
	andi.	r0,  N, 7
	beq	LL(999)

	andi.	r0,  N, 4
	beq	LL(107)

	LFDUX	A1,    X, INCX
	LFDUX	B1,    Y, INCY
	LFDUX	A2,    X, INCX
	LFDUX	B2,    Y, INCY

	LFDUX	A3,    X, INCX
	LFDUX	B3,    Y, INCY
	LFDUX	A4,    X, INCX
	LFDUX	B4,    Y, INCY

	fmadd	C1, A1, B1, C1
	fmadd	C2, A2, B2, C2
	fmadd	C3, A3, B3, C3
	fmadd	C4, A4, B4, C4
	.align 4

LL(107):
	andi.	r0,  N, 2
	beq	LL(108)

	LFDUX	A1,    X, INCX
	LFDUX	B1,    Y, INCY

	LFDUX	A2,    X, INCX
	LFDUX	B2,    Y, INCY

	fmadd	C1, A1, B1, C1
	fmadd	C2, A2, B2, C2
	.align 4

LL(108):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	A1,    X, INCX
	LFDUX	B1,    Y, INCY

	fmadd	C1, A1, B1, C1
	.align 4

LL(999):
	li	r10, 16

	fpadd	C1, C1, C2
	fpadd	C3, C3, C4
	fpadd	C1, C1, C3
	lfpdux	f20, SP, r10

	lfpdux	f19, SP, r10
	lfpdux	f18, SP, r10
	lfpdux	f17, SP, r10
	fsmtp	C2, C1
	lfpdux	f16, SP, r10

	lfpdux	f15, SP, r10
	lfpdux	f14, SP, r10
	fadd	C1, C1, C2
	addi	SP, SP,  16
	blr

	EPILOGUE
