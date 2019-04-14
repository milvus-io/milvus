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

#define	XX	r8
#define RET	r9
#define NN	r10

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

#define F1	f12
#define F2	f13
#define F3	f14
#define F4	f15

#define T1	f16
#define T2	f17
#define T3	f18
#define T4	f19

#define B1	f20
#define B2	f21
#define B3	f22
#define B4	f23
#define B5	f24
#define B6	f25
#define B7	f26
#define B8	f27


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
	stfpdux	f26, SP, r10
	stfpdux	f27, SP, r10

#ifdef F_INTERFACE
	LDINT	N,    0(N)
	LDINT	INCX, 0(INCX)
#endif

	slwi	INCX,  INCX, BASE_SHIFT
	add	INCX2, INCX, INCX

	li	RET, 0
	cmpwi	cr0, N, 0
	ble	LL(999)
	cmpwi	cr0, INCX, 0
	mr	NN, N
	ble	LL(999)

	mr	XX, X

	LFD	A1, 0 * SIZE(X)
	LFD	A2, 1 * SIZE(X)
	add	X, X, INCX2
	li	RET, 1

	fabs	A1, A1
	fabs	A2, A2

	subi	INCX2, INCX2, SIZE

	addi	N, N, -1
	cmpwi	cr0, N, 0
	fadd	C1, A1, A2
	ble	LL(999)

	fsmfp	C1, C1
	li	INCX, SIZE
	fpmr	C2, C1
	sub	X,  X, INCX2
	fpmr	C3, C1
	srawi.	r0, N, 3
	fpmr	C4, C1
	mtspr	CTR,  r0
	beq-	LL(105)

	LFDUX	A1,   X, INCX2
	LFDUX	A2,   X, INCX
	LFDUX	A3,   X, INCX2
	LFDUX	A4,   X, INCX

	LFSDUX	A1,   X, INCX2
	LFSDUX	A2,   X, INCX
	LFSDUX	A3,   X, INCX2
	LFSDUX	A4,   X, INCX

	LFDUX	A5,   X, INCX2
	LFDUX	A6,   X, INCX
	LFDUX	A7,   X, INCX2
	LFDUX	A8,   X, INCX

	LFSDUX	A5,   X, INCX2
	LFSDUX	A6,   X, INCX
	LFSDUX	A7,   X, INCX2
	LFSDUX	A8,   X, INCX
	bdz	LL(103)
	.align 4

LL(102):
	fpabs	B1, A1
	LFDUX	A1,   X, INCX2
	fpabs	B2, A2
	LFDUX	A2,   X, INCX
	fpabs	B3, A3
	LFDUX	A3,   X, INCX2
	fpabs	B4, A4
	LFDUX	A4,   X, INCX

	fpabs	B5, A5
	LFSDUX	A1,   X, INCX2
	fpabs	B6, A6
	LFSDUX	A2,   X, INCX
	fpabs	B7, A7
	LFSDUX	A3,   X, INCX2
	fpabs	B8, A8
	LFSDUX	A4,   X, INCX

	fpadd	T1, B1, B2
	LFDUX	A5,   X, INCX2
	fpadd	T2, B3, B4
	LFDUX	A6,   X, INCX
	fpadd	T3, B5, B6
	LFDUX	A7,   X, INCX2
	fpadd	T4, B7, B8
	LFDUX	A8,   X, INCX

	fpsub	F1, T1, C1
	LFSDUX	A5,   X, INCX2
	fpsub	F2, T2, C2
	LFSDUX	A6,   X, INCX
	fpsub	F3, T3, C3
	LFSDUX	A7,   X, INCX2
	fpsub	F4, T4, C4
	LFSDUX	A8,   X, INCX

	fpsel	C1, F1, C1, T1
	fpsel	C2, F2, C2, T2
	fpsel	C3, F3, C3, T3
	fpsel	C4, F4, C4, T4
	bdnz	LL(102)
	.align 4

LL(103):
	fpabs	B1, A1
	fpabs	B2, A2
	fpabs	B3, A3
	fpabs	B4, A4

	fpabs	B5, A5
	fpabs	B6, A6
	fpabs	B7, A7
	fpabs	B8, A8

	fpadd	T1, B1, B2
	fpadd	T2, B3, B4
	fpadd	T3, B5, B6
	fpadd	T4, B7, B8

	fpsub	F1, T1, C1
	fpsub	F2, T2, C2
	fpsub	F3, T3, C3
	fpsub	F4, T4, C4

	fpsel	C1, F1, C1, T1
	fpsel	C2, F2, C2, T2
	fpsel	C3, F3, C3, T3
	fpsel	C4, F4, C4, T4
	.align 4

LL(105):
	andi.	r0,  N, 7
	beq	LL(120)

	andi.	r0,  N, 4
	beq	LL(106)

	LFDUX	A1,   X, INCX2
	LFDUX	A2,   X, INCX
	LFDUX	A3,   X, INCX2
	LFDUX	A4,   X, INCX

	LFSDUX	A1,   X, INCX2
	LFSDUX	A2,   X, INCX
	LFSDUX	A3,   X, INCX2
	LFSDUX	A4,   X, INCX

	fpabs	A1, A1
	fpabs	A2, A2
	fpabs	A3, A3
	fpabs	A4, A4

	fpadd	A1, A1, A2
	fpadd	A3, A3, A4

	fpsub	F1, A1, C1
	fpsub	F2, A3, C2

	fpsel	C1, F1, C1, A1
	fpsel	C2, F2, C2, A3
	.align 4

LL(106):
	andi.	r0,  N, 2
	beq	LL(107)

	LFDUX	A1,   X, INCX2
	LFDUX	A2,   X, INCX
	LFSDUX	A1,   X, INCX2
	LFSDUX	A2,   X, INCX

	fpabs	A1, A1
	fpabs	A2, A2

	fpadd	A1, A1, A2

	fpsub	F1, A1, C1
	fpsel	C1, F1, C1, A1
	.align 4

LL(107):
	andi.	r0,  N, 1
	beq	LL(120)

	LFDUX	A1,   X, INCX2
	LFDUX	A2,   X, INCX

	fabs	A1, A1
	fabs	A2, A2

	fadd	A1, A1, A2

	fsub	F1, A1, C1
	fsel	C1, F1, C1, A1
	.align 4

LL(120):
	fpsub	F1,  C2, C1
	fpsub	F2,  C4, C3

	fpsel	C1,  F1,  C1,  C2
	fpsel	C3,  F2,  C3,  C4

	fpsub	F1,  C3, C1
	fpsel	C1,  F1,  C1,  C3

	fsmtp	C2, C1

	li	RET, 0
	fsub	F1,  C2, C1
	fsel	C1,  F1,  C1,  C2

	fsmfp	C1, C1

	sub	XX,  XX, INCX2

	srawi.	r0, NN, 3
	mtspr	CTR,  r0
	beq-	LL(125)

	LFDUX	A1,   XX, INCX2
	LFDUX	A2,   XX, INCX
	LFDUX	A3,   XX, INCX2
	LFDUX	A4,   XX, INCX

	LFSDUX	A1,   XX, INCX2
	LFSDUX	A2,   XX, INCX
	LFSDUX	A3,   XX, INCX2
	LFSDUX	A4,   XX, INCX

	LFDUX	A5,   XX, INCX2
	LFDUX	A6,   XX, INCX
	LFDUX	A7,   XX, INCX2
	LFDUX	A8,   XX, INCX

	LFSDUX	A5,   XX, INCX2
	LFSDUX	A6,   XX, INCX
	LFSDUX	A7,   XX, INCX2
	LFSDUX	A8,   XX, INCX

	fpabs	T1, A1
	fpabs	T2, A2
	fpabs	T3, A3
	fpabs	T4, A4

	fpadd	B1, T1, T2
	fpadd	B2, T3, T4

 	bdz	LL(123)
	.align 4

LL(122):
	LFDUX	A1,   XX, INCX2
	fpabs	T1, A5
	addi	RET, RET, 1
	fcmpu	cr0, C1, B1
	LFDUX	A2,   XX, INCX
	beq	cr0, LL(999)

	LFDUX	A3,   XX, INCX2
	fpabs	T2, A6
	addi	RET, RET, 1
	fcmpu	cr0, C1, B2
	LFDUX	A4,   XX, INCX
	beq	cr0, LL(999)

	LFSDUX	A1,   XX, INCX2
	fpabs	T3, A7
	addi	RET, RET, 1
	fscmp	cr0, C1, B1
	LFSDUX	A2,   XX, INCX
	beq	cr0, LL(999)

	LFSDUX	A3,   XX, INCX2
	fpabs	T4, A8
	addi	RET, RET, 1
	fscmp	cr0, C1, B2
	LFSDUX	A4,   XX, INCX
	beq	cr0, LL(999)

	fpadd	B3, T1, T2
	fpadd	B4, T3, T4

	LFDUX	A5,   XX, INCX2
	fpabs	T1, A1
	addi	RET, RET, 1
	fcmpu	cr0, C1, B3
	LFDUX	A6,   XX, INCX
	beq	cr0, LL(999)

	LFDUX	A7,   XX, INCX2
	fpabs	T2, A2
	addi	RET, RET, 1
	fcmpu	cr0, C1, B4
	LFDUX	A8,   XX, INCX
	beq	cr0, LL(999)

	LFSDUX	A5,   XX, INCX2
	fpabs	T3, A3
	addi	RET, RET, 1
	fscmp	cr0, C1, B3
	LFSDUX	A6,   XX, INCX
	beq	cr0, LL(999)

	LFSDUX	A7,   XX, INCX2
	fpabs	T4, A4
	addi	RET, RET, 1
	fscmp	cr0, C1, B4
	LFSDUX	A8,   XX, INCX
	beq	cr0, LL(999)

	fpadd	B1, T1, T2
	fpadd	B2, T3, T4
	bdnz	LL(122)
	.align 4

LL(123):
	fpabs	T1, A5
	addi	RET, RET, 1
	fcmpu	cr0, C1, B1
	beq	cr0, LL(999)

	fpabs	T2, A6
	addi	RET, RET, 1
	fcmpu	cr0, C1, B2
	beq	cr0, LL(999)

	fpabs	T3, A7
	addi	RET, RET, 1
	fscmp	cr0, C1, B1
	beq	cr0, LL(999)

	fpabs	T4, A8
	addi	RET, RET, 1
	fscmp	cr0, C1, B2
	beq	cr0, LL(999)

	fpadd	B3, T1, T2
	fpadd	B4, T3, T4

	addi	RET, RET, 1
	fcmpu	cr0, C1, B3
	beq	cr0, LL(999)

	addi	RET, RET, 1
	fcmpu	cr0, C1, B4
	beq	cr0, LL(999)

	addi	RET, RET, 1
	fscmp	cr0, C1, B3
	beq	cr0, LL(999)

	addi	RET, RET, 1
	fscmp	cr0, C1, B4
	beq	cr0, LL(999)
	.align 4

LL(125):
	andi.	r0,  NN, 4
	beq	LL(126)

	LFDUX	A1,   XX, INCX2
	LFDUX	A2,   XX, INCX
	LFDUX	A3,   XX, INCX2
	LFDUX	A4,   XX, INCX

	LFSDUX	A1,   XX, INCX2
	LFSDUX	A2,   XX, INCX
	LFSDUX	A3,   XX, INCX2
	LFSDUX	A4,   XX, INCX

	fpabs	A1, A1
	fpabs	A2, A2
	fpabs	A3, A3
	fpabs	A4, A4

	fpadd	A1, A1, A2
	fpadd	A3, A3, A4

	addi	RET, RET, 1
	fcmpu	cr0, C1, A1
	beq	cr0, LL(999)

	addi	RET, RET, 1
	fcmpu	cr0, C1, A3
	beq	cr0, LL(999)

	addi	RET, RET, 1
	fscmp	cr0, C1, A1
	beq	cr0, LL(999)

	addi	RET, RET, 1
	fscmp	cr0, C1, A3
	beq	cr0, LL(999)
	.align 4

LL(126):
	andi.	r0,  NN, 2
	beq	LL(127)

	LFDUX	A1,   XX, INCX2
	LFDUX	A2,   XX, INCX
	LFDUX	A3,   XX, INCX2
	LFDUX	A4,   XX, INCX

	fabs	A1, A1
	fabs	A2, A2
	fabs	A3, A3
	fabs	A4, A4

	fadd	A1, A1, A2
	fadd	A3, A3, A4

	addi	RET, RET, 1
	fcmpu	cr0, C1, A1
	beq	cr0, LL(999)

	addi	RET, RET, 1
	fcmpu	cr0, C1, A3
	beq	cr0, LL(999)
	.align 4

LL(127):
	addi	RET, RET, 1
	.align 4

LL(999):
	li	r10, 16
	addi	SP, SP, -16
	mr	r3, RET

	lfpdux	f27, SP, r10
	lfpdux	f26, SP, r10
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
