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

#define A1	f4
#define A2	f5
#define A3	f6
#define A4	f7
#define A5	f8
#define A6	f9
#define A7	f10
#define A8	f11

#define T1	f12
#define T2	f13
#define T3	f14
#define T4	f15

	PROLOGUE
	PROFCODE

	li	r10, -16

	stfpdux	f14, SP, r10
	stfpdux	f15, SP, r10

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

	cmpwi	cr0, N, 0
	ble	LL(999)
	cmpwi	cr0, INCX, 0
	ble	LL(999)

	cmpwi	cr0, INCX, SIZE
	bne	LL(100)

	andi.	r0, X, 2 * SIZE - 1
	beq	LL(05)

	LFD	C1, 0(X)
	addi	X, X, 1 * SIZE
	addi	N, N, -1
	cmpwi	cr0, N, 0
	fabs	C1, C1
	ble	LL(999)
	.align 4

LL(05):
	srawi.	r0, N, 4
	sub	X, X, INCX2
	mtspr	CTR,  r0
	beq-	LL(15)

	LFPDUX	A1,   X, INCX2
	fpmr	T1, C2
	LFPDUX	A2,   X, INCX2
	fpmr	T2, C2
	LFPDUX	A3,   X, INCX2
	fpmr	T3, C2
	LFPDUX	A4,   X, INCX2
	fpmr	T4, C2
	LFPDUX	A5,   X, INCX2
	LFPDUX	A6,   X, INCX2
	LFPDUX	A7,   X, INCX2
	LFPDUX	A8,   X, INCX2
	bdz	LL(13)
	.align 4

LL(12):
	fpadd	C1, C1, T1
	nop
	fpabs	T1, A1
	LFPDUX	A1,   X, INCX2

	fpadd	C2, C2, T2
	nop
	fpabs	T2, A2
	LFPDUX	A2,   X, INCX2

	fpadd	C3, C3, T3
	nop
	fpabs	T3, A3
	LFPDUX	A3,   X, INCX2

	fpadd	C4, C4, T4
	nop
	fpabs	T4, A4
	LFPDUX	A4,   X, INCX2

	fpadd	C1, C1, T1
	nop
	fpabs	T1, A5
	LFPDUX	A5,   X, INCX2

	fpadd	C2, C2, T2
	nop
	fpabs	T2, A6
	LFPDUX	A6,   X, INCX2

	fpadd	C3, C3, T3
	nop
	fpabs	T3, A7
	LFPDUX	A7,   X, INCX2

	fpadd	C4, C4, T4
	fpabs	T4, A8
	LFPDUX	A8,   X, INCX2
	bdnz	LL(12)
	.align 4

LL(13):
	fpadd	C1, C1, T1
	fpabs	T1, A1
	fpadd	C2, C2, T2
	fpabs	T2, A2
	fpadd	C3, C3, T3
	fpabs	T3, A3
	fpadd	C4, C4, T4
	fpabs	T4, A4

	fpadd	C1, C1, T1
	fpabs	T1, A5
	fpadd	C2, C2, T2
	fpabs	T2, A6
	fpadd	C3, C3, T3
	fpabs	T3, A7
	fpadd	C4, C4, T4
	fpabs	T4, A8

	fpadd	C1, C1, T1
	fpadd	C2, C2, T2
	fpadd	C3, C3, T3
	fpadd	C4, C4, T4
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

	fpabs	T1, A1
	fpabs	T2, A2
	fpabs	T3, A3
	fpabs	T4, A4

	fpadd	C1, C1, T1
	fpadd	C2, C2, T2
	fpadd	C3, C3, T3
	fpadd	C4, C4, T4
	.align 4

LL(16):
	andi.	r0,  N, 4
	beq	LL(17)

	LFPDUX	A1,    X, INCX2
	LFPDUX	A2,    X, INCX2
	fpabs	T1, A1
	fpabs	T2, A2

	fpadd	C1, C1, T1
	fpadd	C2, C2, T2
	.align 4

LL(17):
	andi.	r0,  N, 2
	beq	LL(18)

	LFPDUX	A1,    X, INCX2
	fpabs	T1, A1
	fpadd	C1, C1, T1
	.align 4

LL(18):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDX	A1,    X, INCX2
	fabs	T1, A1
	fadd	C1, C1, T1
	b LL(999)
	.align 4

LL(100):
	sub	X2, X, INCX
	sub	X,  X, INCX2

	srawi.	r0, N, 4
	mtspr	CTR,  r0
	beq-	LL(115)


	LFDUX	A1,   X, INCX2
	fpmr	T1, C2
	LFDUX	A2,   X, INCX2
	fpmr	T2, C2
	LFDUX	A3,   X, INCX2
	fpmr	T3, C2
	LFDUX	A4,   X, INCX2
	fpmr	T4, C2

	LFDUX	A5,   X, INCX2
	LFSDUX	A1,   X2, INCX2

	LFDUX	A6,   X, INCX2
	LFSDUX	A2,   X2, INCX2

	LFDUX	A7,   X, INCX2
	LFSDUX	A3,   X2, INCX2

	LFDUX	A8,   X, INCX2
	LFSDUX	A4,   X2, INCX2
	bdz	LL(113)
	.align 4

LL(112):
	fpadd	C1, C1, T1
	LFSDUX	A5,   X2, INCX2
	fpabs	T1, A1
	LFDUX	A1,   X, INCX2

	fpadd	C2, C2, T2
	LFSDUX	A6,   X2, INCX2
	fpabs	T2, A2
	LFDUX	A2,   X, INCX2

	fpadd	C3, C3, T3
	LFSDUX	A7,   X2, INCX2
	fpabs	T3, A3
	LFDUX	A3,   X, INCX2

	fpadd	C4, C4, T4
	LFSDUX	A8,   X2, INCX2
	fpabs	T4, A4
	LFDUX	A4,   X, INCX2

	fpadd	C1, C1, T1
	LFSDUX	A1,   X2, INCX2
	fpabs	T1, A5
	LFDUX	A5,   X, INCX2
	fpadd	C2, C2, T2
	LFSDUX	A2,   X2, INCX2
	fpabs	T2, A6
	LFDUX	A6,   X, INCX2

	fpadd	C3, C3, T3
	LFSDUX	A3,   X2, INCX2
	fpabs	T3, A7
	LFDUX	A7,   X, INCX2
	fpadd	C4, C4, T4
	LFSDUX	A4,   X2, INCX2
	fpabs	T4, A8
	LFDUX	A8,   X, INCX2

	bdnz	LL(112)
	.align 4

LL(113):
	fpadd	C1, C1, T1
	nop
	fpabs	T1, A1
	LFSDUX	A5,   X2, INCX2
	fpadd	C2, C2, T2
	nop
	fpabs	T2, A2
	LFSDUX	A6,   X2, INCX2
	fpadd	C3, C3, T3

	nop
	fpabs	T3, A3
	LFSDUX	A7,   X2, INCX2
	fpadd	C4, C4, T4
	nop
	fpabs	T4, A4
	LFSDUX	A8,   X2, INCX2

	fpadd	C1, C1, T1
	fpabs	T1, A5
	fpadd	C2, C2, T2
	fpabs	T2, A6
	fpadd	C3, C3, T3
	fpabs	T3, A7
	fpadd	C4, C4, T4
	fpabs	T4, A8

	fpadd	C1, C1, T1
	fpadd	C2, C2, T2
	fpadd	C3, C3, T3
	fpadd	C4, C4, T4
	.align 4

LL(115):
	andi.	r0,  N, 15
	beq	LL(999)
	andi.	r0,  N, 8
	beq	LL(116)

	LFDUX	A1,    X,  INCX2
	LFDUX	A2,    X2, INCX2
	LFDUX	A3,    X,  INCX2
	LFDUX	A4,    X2, INCX2

	fabs	T1, A1
	LFDUX	A5,    X,  INCX2
	fabs	T2, A2
	LFDUX	A6,    X2, INCX2
	fabs	T3, A3
	LFDUX	A7,    X,  INCX2
	fabs	T4, A4
	LFDUX	A8,    X2, INCX2

	fadd	C1, C1, T1
	fabs	T1, A5
	fadd	C2, C2, T2
	fabs	T2, A6

	fadd	C3, C3, T3
	fabs	T3, A7
	fadd	C4, C4, T4
	fabs	T4, A8

	fadd	C1, C1, T1
	fadd	C2, C2, T2
	fadd	C3, C3, T3
	fadd	C4, C4, T4
	.align 4

LL(116):
	andi.	r0,  N, 4
	beq	LL(117)

	LFDUX	A1,    X,  INCX2
	LFDUX	A2,    X2, INCX2
	LFDUX	A3,    X,  INCX2
	LFDUX	A4,    X2, INCX2

	fabs	T1, A1
	fabs	T2, A2
	fabs	T3, A3
	fabs	T4, A4

	fadd	C1, C1, T1
	fadd	C2, C2, T2
	fadd	C3, C3, T3
	fadd	C4, C4, T4
	.align 4

LL(117):
	andi.	r0,  N, 2
	beq	LL(118)

	LFDUX	A1,    X,  INCX2
	LFDUX	A2,    X2, INCX2

	fabs	T1, A1
	fabs	T2, A2
	fadd	C1, C1, T1
	fadd	C2, C2, T2
	.align 4

LL(118):
	andi.	r0,  N, 1
	beq	LL(999)

	LFDX	A1,    X, INCX2
	fabs	T1, A1
	fadd	C1, C1, T1
	.align 4

LL(999):
	fpadd	C1,  C1,  C2
	li	r10, 16
	fpadd	C3,  C3,  C4
	fpadd	C1,  C1,  C3
	lfpdux	f15, SP, r10
	fsmtp	C2, C1
	lfpdux	f14, SP, r10
	addi	SP, SP,  16
	fadd	C1, C2, C1
	blr

	EPILOGUE
