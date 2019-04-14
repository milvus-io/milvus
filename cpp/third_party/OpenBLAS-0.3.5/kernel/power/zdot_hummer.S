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

#if defined(F_INTERFACE) && defined(F_INTERFACE_F2C)
#define RESULT	r3
#define N	r4
#define X	r5
#define INCX	r6
#define Y	r7
#define INCY	r8
#else
#define N	r3
#define X	r4
#define INCX	r5
#define Y	r6
#define INCY	r7
#endif

#define INCX2	r9
#define INCY2	r10

#define C1	f1
#define C2	f2
#define C3	f0
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

#define B1	f16
#define B2	f17
#define B3	f18
#define B4	f19
#define B5	f20
#define B6	f21
#define B7	f22
#define B8	f23

#ifndef CONJ
#define FXCXNPMA fxcxnpma
#else
#define FXCXNPMA fxcxnsma
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

	fpmr	C5, C1
	fpmr	C6, C1
	fpmr	C7, C1
	fpmr	C8, C1

	cmpwi	cr0, N, 0
	ble	LL(99)

#ifdef F_INTERFACE
	cmpwi	cr0, INCX, 0
	bge+	LL(05)

	subi	r0, N, 1
	mullw	r0, r0, INCX2
	sub	X, X, r0
	.align 4

LL(05):
	cmpwi	cr0, INCY, 0
	bge+	LL(06)

	subi	r0, N, 1
	mullw	r0, r0, INCY2
	sub	Y, Y, r0
	.align 4

LL(06):
#endif

	andi.	r0, X, 2 * SIZE - 1
	bne	LL(100)
	andi.	r0, Y, 2 * SIZE - 1
	bne	LL(100)

/* X is aligned, Y is aligned */
LL(10):
	sub	X, X, INCX2
	sub	Y, Y, INCY2

	srawi.	r0, N, 3
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
	bdz	LL(14)
	.align 4

LL(13):
	fxcpmadd C1, A1, B1, C1
	LFPDUX	B8,    Y, INCY2
	FXCXNPMA C2, A1, B1, C2
	LFPDUX	A1,    X, INCX2
	fxcpmadd C3, A2, B2, C3
	LFPDUX	B1,    Y, INCY2
	FXCXNPMA C4, A2, B2, C4
	LFPDUX	A2,    X, INCX2

	fxcpmadd C5, A3, B3, C5
	LFPDUX	B2,    Y, INCY2
	FXCXNPMA C6, A3, B3, C6
	LFPDUX	A3,    X, INCX2
	fxcpmadd C7, A4, B4, C7
	LFPDUX	B3,    Y, INCY2
	FXCXNPMA C8, A4, B4, C8
	LFPDUX	A4,    X, INCX2

	fxcpmadd C1, A5, B5, C1
	LFPDUX	B4,    Y, INCY2
	FXCXNPMA C2, A5, B5, C2
	LFPDUX	A5,    X, INCX2
	fxcpmadd C3, A6, B6, C3
	LFPDUX	B5,    Y, INCY2
	FXCXNPMA C4, A6, B6, C4
	LFPDUX	A6,    X, INCX2

	fxcpmadd C5, A7, B7, C5
	LFPDUX	B6,    Y, INCY2
	FXCXNPMA C6, A7, B7, C6
	LFPDUX	A7,    X, INCX2
	fxcpmadd C7, A8, B8, C7
	LFPDUX	B7,    Y, INCY2
	FXCXNPMA C8, A8, B8, C8
	LFPDUX	A8,    X, INCX2
	bdnz	LL(13)
	.align 4

LL(14):
	LFPDUX	B8,    Y, INCY2
	fxcpmadd C1, A1, B1, C1
	FXCXNPMA C2, A1, B1, C2
	fxcpmadd C3, A2, B2, C3
	FXCXNPMA C4, A2, B2, C4

	fxcpmadd C5, A3, B3, C5
	FXCXNPMA C6, A3, B3, C6
	fxcpmadd C7, A4, B4, C7
	FXCXNPMA C8, A4, B4, C8

	fxcpmadd C1, A5, B5, C1
	FXCXNPMA C2, A5, B5, C2
	fxcpmadd C3, A6, B6, C3
	FXCXNPMA C4, A6, B6, C4

	fxcpmadd C5, A7, B7, C5
	FXCXNPMA C6, A7, B7, C6
	fxcpmadd C7, A8, B8, C7
	FXCXNPMA C8, A8, B8, C8
	.align 4

LL(15):
	andi.	r0,  N, 7
	beq	LL(99)

	andi.	r0,  N, 4
	beq	LL(16)

	LFPDUX	A1,    X, INCX2
	LFPDUX	B1,    Y, INCY2
	LFPDUX	A2,    X, INCX2
	LFPDUX	B2,    Y, INCY2
	LFPDUX	A3,    X, INCX2
	LFPDUX	B3,    Y, INCY2
	LFPDUX	A4,    X, INCX2
	LFPDUX	B4,    Y, INCY2

	fxcpmadd C1, A1, B1, C1
	FXCXNPMA C2, A1, B1, C2
	fxcpmadd C3, A2, B2, C3
	FXCXNPMA C4, A2, B2, C4

	fxcpmadd C5, A3, B3, C5
	FXCXNPMA C6, A3, B3, C6
	fxcpmadd C7, A4, B4, C7
	FXCXNPMA C8, A4, B4, C8
	.align 4

LL(16):
	andi.	r0,  N, 2
	beq	LL(17)

	LFPDUX	A1,    X, INCX2
	LFPDUX	B1,    Y, INCY2
	LFPDUX	A2,    X, INCX2
	LFPDUX	B2,    Y, INCY2

	fxcpmadd C1, A1, B1, C1
	FXCXNPMA C2, A1, B1, C2
	fxcpmadd C3, A2, B2, C3
	FXCXNPMA C4, A2, B2, C4
	.align 4

LL(17):
	andi.	r0,  N, 1
	beq	LL(99)

	LFPDUX	A1,    X, INCX2
	LFPDUX	B1,    Y, INCY2

	fxcpmadd C1, A1, B1, C1
	FXCXNPMA C2, A1, B1, C2
	.align 4

LL(99):
	li	r10, 16

	fpadd	C1, C1, C5
	lfpdux	f23, SP, r10
	fpadd	C2, C2, C6
	lfpdux	f22, SP, r10
	fpadd	C3, C3, C7
	lfpdux	f21, SP, r10
	fpadd	C4, C4, C8
	lfpdux	f20, SP, r10

	lfpdux	f19, SP, r10
	lfpdux	f18, SP, r10
	fpadd	C1, C1, C3
	lfpdux	f17, SP, r10
	fpadd	C2, C2, C4
	lfpdux	f16, SP, r10

	fpadd	C1, C1, C2
	lfpdux	f15, SP, r10
	lfpdux	f14, SP, r10
	fsmtp	C2, C1

#if defined(F_INTERFACE) && defined(F_INTERFACE_F2C)
	STFD	C1,    0 * SIZE(RESULT)
	STFD	C2,    1 * SIZE(RESULT)
#endif
	addi	SP, SP,  16
	blr
	.align 4


/* X is aligned, Y is NOT aligned */

LL(100):
	subi	INCX2, INCX2, SIZE
	subi	INCY2, INCY2, SIZE

	li	INCX, SIZE
	li	INCY, SIZE

	sub	X, X, INCX2
	sub	Y, Y, INCY2

	srawi.	r0, N, 2
	mtspr	CTR,  r0
	beq-	LL(105)

	LFDUX	A1,    X, INCX2
	LFDUX	B1,    Y, INCY2
	LFDUX	A2,    X, INCX
	LFDUX	B2,    Y, INCY

	LFDUX	A3,    X, INCX2
	LFDUX	B3,    Y, INCY2
	LFDUX	A4,    X, INCX
	LFDUX	B4,    Y, INCY

	LFDUX	A5,    X, INCX2
	LFDUX	B5,    Y, INCY2
	LFDUX	A6,    X, INCX
	LFDUX	B6,    Y, INCY

	LFDUX	A7,    X, INCX2
	LFDUX	B7,    Y, INCY2
	LFDUX	A8,    X, INCX
	bdz	LL(104)
	.align 4

LL(103):
	fmadd	C1, A1, B1, C1
	LFDUX	B8,    Y, INCY
	fmadd	C2, A1, B2, C2
	LFDUX	A1,    X, INCX2

	fmadd	C3, A2, B1, C3
	LFDUX	B1,    Y, INCY2
	fmadd	C4, A2, B2, C4
	LFDUX	A2,    X, INCX

	fmadd	C5, A3, B3, C5
	LFDUX	B2,    Y, INCY
	fmadd	C6, A3, B4, C6
	LFDUX	A3,    X, INCX2

	fmadd	C7, A4, B3, C7
	LFDUX	B3,    Y, INCY2
	fmadd	C8, A4, B4, C8
	LFDUX	A4,    X, INCX

	fmadd	C1, A5, B5, C1
	LFDUX	B4,    Y, INCY
	fmadd	C2, A5, B6, C2
	LFDUX	A5,    X, INCX2

	fmadd	C3, A6, B5, C3
	LFDUX	B5,    Y, INCY2
	fmadd	C4, A6, B6, C4
	LFDUX	A6,    X, INCX

	fmadd	C5, A7, B7, C5
	LFDUX	B6,    Y, INCY
	fmadd	C6, A7, B8, C6
	LFDUX	A7,    X, INCX2

	fmadd	C7, A8, B7, C7
	LFDUX	B7,    Y, INCY2
	fmadd	C8, A8, B8, C8
	LFDUX	A8,    X, INCX

	bdnz	LL(103)
	.align 4

LL(104):
	LFDUX	B8,    Y, INCY
	fmadd	C1, A1, B1, C1
	fmadd	C2, A1, B2, C2
	fmadd	C3, A2, B1, C3
	fmadd	C4, A2, B2, C4

	fmadd	C5, A3, B3, C5
	fmadd	C6, A3, B4, C6
	fmadd	C7, A4, B3, C7
	fmadd	C8, A4, B4, C8

	fmadd	C1, A5, B5, C1
	fmadd	C2, A5, B6, C2
	fmadd	C3, A6, B5, C3
	fmadd	C4, A6, B6, C4

	fmadd	C5, A7, B7, C5
	fmadd	C6, A7, B8, C6
	fmadd	C7, A8, B7, C7
	fmadd	C8, A8, B8, C8
	.align 4

LL(105):
	andi.	r0,  N, 3
	beq	LL(999)

	andi.	r0,  N, 2
	beq	LL(107)

	LFDUX	A1,    X, INCX2
	LFDUX	B1,    Y, INCY2
	LFDUX	A2,    X, INCX
	LFDUX	B2,    Y, INCY

	LFDUX	A3,    X, INCX2
	LFDUX	B3,    Y, INCY2
	LFDUX	A4,    X, INCX
	LFDUX	B4,    Y, INCY

	fmadd	C1, A1, B1, C1
	fmadd	C2, A1, B2, C2
	fmadd	C3, A2, B1, C3
	fmadd	C4, A2, B2, C4

	fmadd	C5, A3, B3, C5
	fmadd	C6, A3, B4, C6
	fmadd	C7, A4, B3, C7
	fmadd	C8, A4, B4, C8
	.align 4

LL(107):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDUX	A1,    X, INCX2
	LFDUX	B1,    Y, INCY2

	LFDUX	A2,    X, INCX
	LFDUX	B2,    Y, INCY

	fmadd	C1, A1, B1, C1
	fmadd	C2, A1, B2, C2
	fmadd	C3, A2, B1, C3
	fmadd	C4, A2, B2, C4
	.align 4

LL(999):
	li	r10, 16

	fadd	C1, C1, C5
	lfpdux	f23, SP, r10
	fadd	C2, C2, C6
	lfpdux	f22, SP, r10
	fadd	C3, C3, C7
	lfpdux	f21, SP, r10
	fadd	C4, C4, C8
	lfpdux	f20, SP, r10

	lfpdux	f19, SP, r10
	lfpdux	f18, SP, r10
	lfpdux	f17, SP, r10
	lfpdux	f16, SP, r10

#ifndef CONJ
	FSUB	C1,  C1,  C4
	FADD	C2,  C2,  C3
#else
	FADD	C1,  C1,  C4
	FSUB	C2,  C2,  C3
#endif

	lfpdux	f15, SP, r10
	lfpdux	f14, SP, r10

#if defined(F_INTERFACE) && defined(F_INTERFACE_F2C)
	STFD	C1,    0 * SIZE(RESULT)
	STFD	C2,    1 * SIZE(RESULT)
#endif
	addi	SP, SP,  16
	blr

	EPILOGUE
