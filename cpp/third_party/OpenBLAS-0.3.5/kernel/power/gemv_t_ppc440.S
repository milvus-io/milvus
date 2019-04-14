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

#ifdef linux
#ifndef __64BIT__
#define M	r3
#define	N	r4
#define A	r6
#define LDA	r7
#define X	r8
#define	INCX	r9
#define	Y	r10
#define	INCY	r5
#else
#define M	r3
#define	N	r4
#define A	r7
#define LDA	r8
#define X	r9
#define	INCX	r10
#define	Y	r5
#define	INCY	r6
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
#define M	r3
#define	N	r4
#define A	r8
#define LDA	r9
#define X	r10
#define	INCX	r5
#define	Y	r6
#define	INCY	r7
#else
#define M	r3
#define	N	r4
#define A	r7
#define LDA	r8
#define X	r9
#define	INCX	r10
#define	Y	r5
#define	INCY	r6
#endif
#endif

#define	BUFFER	r11
#define	XP	r12
#define	AO1	r14
#define	AO2	r15
#define	AO3	r16
#define	AO4	r17
#define	J	r18
#define YY	r19
#define	PREA	r20
#define	PREC	r21
#define	X1	r22


#if defined(PPCG4)
#define PREFETCHSIZE_A  42
#define PREFETCHSIZE_C   7
#endif

#if defined(POWER6)
#define PREFETCHSIZE_A  42
#define PREFETCHSIZE_C   7
#endif

#define y01 f0
#define y02 f1
#define y03 f2
#define y04 f3
#define y05 f4
#define y06 f5
#define y07 f6
#define y08 f7

#define a1	f8
#define a2	f9
#define a3	f10
#define a4	f11
#define a5	f12
#define a6	f13
#define a7	f14
#define a8	f15

#define b1	f16
#define b2	f17
#define b3	f18
#define b4	f19
#define b5	f20
#define b6	f21
#define b7	f22
#define b8	f23

#define alpha	f23

#ifndef NEEDPARAM

#ifndef __64BIT__
#define STACKSIZE 224
#else
#define STACKSIZE 288
#endif

#define FZERO     144(SP)
#define ALPHA     152(SP)

	PROLOGUE
	PROFCODE

	addi	SP, SP,  -STACKSIZE
	li	r0,   0

	stfd	f14,     0(SP)
	stfd	f15,     8(SP)
	stfd	f16,    16(SP)
	stfd	f17,    24(SP)
	stfd	f18,    32(SP)
	stfd	f19,    40(SP)
	stfd	f20,    48(SP)
	stfd	f21,    56(SP)
	stfd	f22,    64(SP)
	stfd	f23,    72(SP)

#ifdef __64BIT__
	std	r0,    FZERO
	stfd	f1,    ALPHA
	std	r14,   160(SP)
	std	r15,   168(SP)
	std	r16,   176(SP)
	std	r17,   184(SP)
	std	r18,   192(SP)
	std	r19,   200(SP)
	std	r20,   208(SP)
	std	r21,   216(SP)
	std	r22,   224(SP)
#else
	stw	r0,    0 + FZERO
	stw	r0,    4 + FZERO
	stfd	f1,    ALPHA
	stw	r14,   160(SP)
	stw	r15,   164(SP)
	stw	r16,   168(SP)
	stw	r17,   172(SP)
	stw	r18,   176(SP)
	stw	r19,   180(SP)
	stw	r20,   184(SP)
	stw	r21,   188(SP)
	stw	r22,   192(SP)
#endif

#ifdef linux
#ifndef __64BIT__
	lwz	INCY,	 FRAMESLOT(0) + STACKSIZE(SP)
	lwz	BUFFER,  FRAMESLOT(1) + STACKSIZE(SP)
#else
	ld	Y,       FRAMESLOT(0) + STACKSIZE(SP)
	ld	INCY,    FRAMESLOT(1) + STACKSIZE(SP)
	ld	BUFFER,  FRAMESLOT(2) + STACKSIZE(SP)
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#ifndef __64BIT__
#ifdef DOUBLE
	lwz	INCX,    FRAMESLOT(0) + STACKSIZE(SP)
	lwz	Y,       FRAMESLOT(1) + STACKSIZE(SP)
	lwz	INCY,    FRAMESLOT(2) + STACKSIZE(SP)
	lwz	BUFFER,  FRAMESLOT(3) + STACKSIZE(SP)
#else
	lwz	Y,       FRAMESLOT(0) + STACKSIZE(SP)
	lwz	INCY,    FRAMESLOT(1) + STACKSIZE(SP)
	lwz	BUFFER,  FRAMESLOT(2) + STACKSIZE(SP)
#endif
#else
	ld	Y,       FRAMESLOT(0) + STACKSIZE(SP)
	ld	INCY,    FRAMESLOT(1) + STACKSIZE(SP)
	ld	BUFFER,  FRAMESLOT(2) + STACKSIZE(SP)
#endif
#endif

	slwi	LDA,  LDA,  BASE_SHIFT
	slwi	INCX, INCX, BASE_SHIFT
	slwi	INCY, INCY, BASE_SHIFT

	addi	A, A, -SIZE
	sub	X, X, INCX
	sub	Y, Y, INCY

	li	PREA, PREFETCHSIZE_A * SIZE
	li	PREC, PREFETCHSIZE_C * SIZE

	cmpi	cr0, 0, M, 0
	ble	LL(999)
	cmpi	cr0, 0, N, 0
	ble	LL(999)

	mr	XP, X

	cmpi	cr0, 0, INCX, SIZE
	beq	LL(10)

	addi	XP, BUFFER, -SIZE
	addi	X1, BUFFER, -SIZE

	srawi.	r0, M, 3
	mtspr	CTR, r0
	ble	LL(CopyRemain)
	.align 4

LL(CopyKernel):
	LFDUX	f0, X, INCX
	LFDUX	f1, X, INCX
	LFDUX	f2, X, INCX
	LFDUX	f3, X, INCX
	LFDUX	f4, X, INCX
	LFDUX	f5, X, INCX
	LFDUX	f6, X, INCX
	LFDUX	f7, X, INCX

	STFDU	f0,  1 * SIZE(X1)
	STFDU	f1,  1 * SIZE(X1)
	STFDU	f2,  1 * SIZE(X1)
	STFDU	f3,  1 * SIZE(X1)
	STFDU	f4,  1 * SIZE(X1)
	STFDU	f5,  1 * SIZE(X1)
	STFDU	f6,  1 * SIZE(X1)
	STFDU	f7,  1 * SIZE(X1)
	bdnz	LL(CopyKernel)
	.align 4

LL(CopyRemain):
	andi.	r0, M, 7
	mtspr	CTR, r0
	ble	LL(10)
	.align 4

LL(CopySub):
	LFDUX	f0, X,  INCX
	STFDU	f0,  1 * SIZE(X1)
	bdnz	LL(CopySub)
	.align 4

LL(10):
	mr	YY, Y

	srawi.	J, N, 2
	ble	LL(30)
	.align 4

LL(21):
	mr     AO1, A
	add    AO2, A,   LDA
	add    AO3, AO2, LDA
	add    AO4, AO3, LDA
	add    A,   AO4, LDA

	mr     X1, XP

	lfd	 y01, FZERO
	fmr	 y02, y01
	fmr	 y03, y01
	fmr	 y04, y01
	fmr	 y05, y01
	fmr	 y06, y01
	fmr	 y07, y01
	fmr	 y08, y01

	dcbtst	 Y, PREC

	srawi.	r0,  M, 3
	mtspr	CTR, r0
	ble	LL(24)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	a2,  1 * SIZE(AO2)
	LFDU	a3,  1 * SIZE(AO3)
	LFDU	a4,  1 * SIZE(AO4)

	LFDU	b1,  1 * SIZE(X1)
	LFDU	b2,  1 * SIZE(X1)

	LFDU	a5,  1 * SIZE(AO1)
	LFDU	a6,  1 * SIZE(AO2)
	LFDU	a7,  1 * SIZE(AO3)
	LFDU	a8,  1 * SIZE(AO4)

	LFDU	b3,  1 * SIZE(X1)
	LFDU	b4,  1 * SIZE(X1)
	bdz	LL(23)
	.align 4

LL(22):
#ifdef PPCG4
	dcbt	X1, PREA
#endif

	FMADD	y01, a1, b1, y01
	LFDU	a1,  1 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFDU	a2,  1 * SIZE(AO2)
	FMADD	y03, a3, b1, y03
	LFDU	a3,  1 * SIZE(AO3)
	FMADD	y04, a4, b1, y04
	LFDU	a4,  1 * SIZE(AO4)

	LFDU	b1,  1 * SIZE(X1)
#ifdef PPCG4
	dcbt	AO1, PREA
#endif

	FMADD	y05, a5, b2, y05
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y06, a6, b2, y06
	LFDU	a6,  1 * SIZE(AO2)
	FMADD	y07, a7, b2, y07
	LFDU	a7,  1 * SIZE(AO3)
	FMADD	y08, a8, b2, y08
	LFDU	a8,  1 * SIZE(AO4)

	LFDU	b2,  1 * SIZE(X1)
#ifdef PPCG4
	dcbt	AO2, PREA
#endif

	FMADD	y01, a1, b3, y01
	LFDU	a1,  1 * SIZE(AO1)
	FMADD	y02, a2, b3, y02
	LFDU	a2,  1 * SIZE(AO2)
	FMADD	y03, a3, b3, y03
	LFDU	a3,  1 * SIZE(AO3)
	FMADD	y04, a4, b3, y04
	LFDU	a4,  1 * SIZE(AO4)

	LFDU	b3,  1 * SIZE(X1)
#ifdef PPCG4
	dcbt	AO3, PREA
#endif

	FMADD	y05, a5, b4, y05
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y06, a6, b4, y06
	LFDU	a6,  1 * SIZE(AO2)
	FMADD	y07, a7, b4, y07
	LFDU	a7,  1 * SIZE(AO3)
	FMADD	y08, a8, b4, y08
	LFDU	a8,  1 * SIZE(AO4)

#ifdef PPCG4
	dcbt	AO4, PREA
#endif
	LFDU	b4,  1 * SIZE(X1)
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	X1, PREA
#endif

	FMADD	y01, a1, b1, y01
	LFDU	a1,  1 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFDU	a2,  1 * SIZE(AO2)
	FMADD	y03, a3, b1, y03
	LFDU	a3,  1 * SIZE(AO3)
	FMADD	y04, a4, b1, y04
	LFDU	a4,  1 * SIZE(AO4)

	LFDU	b1,  1 * SIZE(X1)
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO1, PREA
#endif

	FMADD	y05, a5, b2, y05
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y06, a6, b2, y06
	LFDU	a6,  1 * SIZE(AO2)
	FMADD	y07, a7, b2, y07
	LFDU	a7,  1 * SIZE(AO3)
	FMADD	y08, a8, b2, y08
	LFDU	a8,  1 * SIZE(AO4)

	LFDU	b2,  1 * SIZE(X1)
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO2, PREA
#endif

	FMADD	y01, a1, b3, y01
	LFDU	a1,  1 * SIZE(AO1)
	FMADD	y02, a2, b3, y02
	LFDU	a2,  1 * SIZE(AO2)
	FMADD	y03, a3, b3, y03
	LFDU	a3,  1 * SIZE(AO3)
	FMADD	y04, a4, b3, y04
	LFDU	a4,  1 * SIZE(AO4)

	LFDU	b3,  1 * SIZE(X1)
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO3, PREA
#endif

	FMADD	y05, a5, b4, y05
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y06, a6, b4, y06
	LFDU	a6,  1 * SIZE(AO2)
	FMADD	y07, a7, b4, y07
	LFDU	a7,  1 * SIZE(AO3)
	FMADD	y08, a8, b4, y08
	LFDU	a8,  1 * SIZE(AO4)

	LFDU	b4,  1 * SIZE(X1)
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO4, PREA
#endif

 	bdnz	LL(22)
	.align 4

LL(23):
	FMADD	y01, a1, b1, y01
	LFDU	a1,  1 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFDU	a2,  1 * SIZE(AO2)
	FMADD	y03, a3, b1, y03
	LFDU	a3,  1 * SIZE(AO3)
	FMADD	y04, a4, b1, y04
	LFDU	a4,  1 * SIZE(AO4)

	LFDU	b1,  1 * SIZE(X1)

	FMADD	y05, a5, b2, y05
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y06, a6, b2, y06
	LFDU	a6,  1 * SIZE(AO2)
	FMADD	y07, a7, b2, y07
	LFDU	a7,  1 * SIZE(AO3)
	FMADD	y08, a8, b2, y08
	LFDU	a8,  1 * SIZE(AO4)

	LFDU	b2,  1 * SIZE(X1)

	FMADD	y01, a1, b3, y01
	LFDU	a1,  1 * SIZE(AO1)
	FMADD	y02, a2, b3, y02
	LFDU	a2,  1 * SIZE(AO2)
	FMADD	y03, a3, b3, y03
	LFDU	a3,  1 * SIZE(AO3)
	FMADD	y04, a4, b3, y04
	LFDU	a4,  1 * SIZE(AO4)

	LFDU	b3,  1 * SIZE(X1)

	FMADD	y05, a5, b4, y05
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y06, a6, b4, y06
	LFDU	a6,  1 * SIZE(AO2)
	FMADD	y07, a7, b4, y07
	LFDU	a7,  1 * SIZE(AO3)
	FMADD	y08, a8, b4, y08
	LFDU	a8,  1 * SIZE(AO4)

	LFDU	b4,  1 * SIZE(X1)

	FMADD	y01, a1, b1, y01
	LFDU	a1,  1 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFDU	a2,  1 * SIZE(AO2)
	FMADD	y03, a3, b1, y03
	LFDU	a3,  1 * SIZE(AO3)
	FMADD	y04, a4, b1, y04
	LFDU	a4,  1 * SIZE(AO4)

	FMADD	y05, a5, b2, y05
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y06, a6, b2, y06
	LFDU	a6,  1 * SIZE(AO2)
	FMADD	y07, a7, b2, y07
	LFDU	a7,  1 * SIZE(AO3)
	FMADD	y08, a8, b2, y08
	LFDU	a8,  1 * SIZE(AO4)

	FMADD	y01, a1, b3, y01
	FMADD	y02, a2, b3, y02
	FMADD	y03, a3, b3, y03
	FMADD	y04, a4, b3, y04

	FMADD	y05, a5, b4, y05
	FMADD	y06, a6, b4, y06
	FMADD	y07, a7, b4, y07
	FMADD	y08, a8, b4, y08
	.align 4

LL(24):
	andi.	r0, M,  7
	ble	LL(28)

	andi.	r0, M,  4
	ble	LL(26)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	a2,  1 * SIZE(AO2)
	LFDU	b1,  1 * SIZE(X1)
	LFDU	a3,  1 * SIZE(AO3)
	LFDU	a4,  1 * SIZE(AO4)
	LFDU	b2,  1 * SIZE(X1)

	FMADD	y01, a1, b1, y01
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFDU	a6,  1 * SIZE(AO2)
	FMADD	y03, a3, b1, y03
	LFDU	a7,  1 * SIZE(AO3)
	FMADD	y04, a4, b1, y04
	LFDU	a8,  1 * SIZE(AO4)

	LFDU	b3,  1 * SIZE(X1)

	FMADD	y05, a5, b2, y05
	LFDU	a1,  1 * SIZE(AO1)
	FMADD	y06, a6, b2, y06
	LFDU	a2,  1 * SIZE(AO2)
	FMADD	y07, a7, b2, y07
	LFDU	a3,  1 * SIZE(AO3)
	FMADD	y08, a8, b2, y08
	LFDU	a4,  1 * SIZE(AO4)

	LFDU	b4,  1 * SIZE(X1)

	FMADD	y01, a1, b3, y01
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y02, a2, b3, y02
	LFDU	a6,  1 * SIZE(AO2)
	FMADD	y03, a3, b3, y03
	LFDU	a7,  1 * SIZE(AO3)
	FMADD	y04, a4, b3, y04
	LFDU	a8,  1 * SIZE(AO4)

	FMADD	y05, a5, b4, y05
	FMADD	y06, a6, b4, y06
	FMADD	y07, a7, b4, y07
	FMADD	y08, a8, b4, y08
	.align 4

LL(26):
	andi.	r0, M,  2
	ble	LL(27)

	LFDU	b1,  1 * SIZE(X1)
	LFDU	a1,  1 * SIZE(AO1)
	LFDU	a2,  1 * SIZE(AO2)
	LFDU	a3,  1 * SIZE(AO3)
	LFDU	a4,  1 * SIZE(AO4)
	LFDU	b2,  1 * SIZE(X1)

	FMADD	y01, a1, b1, y01
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFDU	a6,  1 * SIZE(AO2)
	FMADD	y03, a3, b1, y03
	LFDU	a7,  1 * SIZE(AO3)
	FMADD	y04, a4, b1, y04
	LFDU	a8,  1 * SIZE(AO4)

	FMADD	y05, a5, b2, y05
	FMADD	y06, a6, b2, y06
	FMADD	y07, a7, b2, y07
	FMADD	y08, a8, b2, y08
	.align 4

LL(27):
	andi.	r0, M,  1
	ble	LL(28)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	b1,  1 * SIZE(X1)

	LFDU	a2,  1 * SIZE(AO2)
	LFDU	a3,  1 * SIZE(AO3)
	LFDU	a4,  1 * SIZE(AO4)

	FMADD	y01, a1, b1, y01
	FMADD	y02, a2, b1, y02
	FMADD	y03, a3, b1, y03
	FMADD	y04, a4, b1, y04
	.align 4

LL(28):
	lfd	alpha, ALPHA

	LFDUX	a1, Y, INCY
	LFDUX	a2, Y, INCY
	LFDUX	a3, Y, INCY
	LFDUX	a4, Y, INCY

	FADD	y01, y05, y01
	FADD	y02, y06, y02
	FADD	y03, y07, y03
	FADD	y04, y08, y04

	FMADD	a1, alpha, f0, a1
	FMADD	a2, alpha, f1, a2
	FMADD	a3, alpha, f2, a3
	FMADD	a4, alpha, f3, a4

	STFDUX	a1, YY, INCY
	addi	J, J, -1
	STFDUX	a2, YY, INCY
	cmpi	cr0, 0, J, 0
	STFDUX	a3, YY, INCY
	STFDUX	a4, YY, INCY
	bgt	LL(21)
	.align 4

LL(30):
	andi.	J, N, 2
	ble	LL(40)

	mr     AO1, A
	add    AO2, A,   LDA
	add    A,   AO2, LDA

	mr     X1, XP

	lfd	 y01, FZERO
	fmr	 y02, y01
	fmr	 y03, y01
	fmr	 y04, y01

	srawi.	r0,  M, 3
	mtspr	CTR, r0
	ble	LL(34)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	a2,  1 * SIZE(AO2)
	LFDU	b1,  1 * SIZE(X1)
	LFDU	b2,  1 * SIZE(X1)

	LFDU	a5,  1 * SIZE(AO1)
	LFDU	a6,  1 * SIZE(AO2)
	LFDU	b3,  1 * SIZE(X1)
	LFDU	b4,  1 * SIZE(X1)
	bdz	LL(33)
	.align 4

LL(32):
#ifdef PPCG4
	dcbt	X1, PREA
#endif

	FMADD	y01, a1, b1, y01
	LFDU	a1,  1 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFDU	a2,  1 * SIZE(AO2)

	LFDU	b1,  1 * SIZE(X1)
#ifdef PPCG4
	dcbt	AO1, PREA
#endif

	FMADD	y03, a5, b2, y03
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y04, a6, b2, y04
	LFDU	a6,  1 * SIZE(AO2)

	LFDU	b2,  1 * SIZE(X1)

	FMADD	y01, a1, b3, y01
	LFDU	a1,  1 * SIZE(AO1)
	FMADD	y02, a2, b3, y02
	LFDU	a2,  1 * SIZE(AO2)

	LFDU	b3,  1 * SIZE(X1)
#ifdef PPCG4
	dcbt	AO2, PREA
#endif

	FMADD	y03, a5, b4, y03
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y04, a6, b4, y04
	LFDU	a6,  1 * SIZE(AO2)

	LFDU	b4,  1 * SIZE(X1)

	FMADD	y01, a1, b1, y01
	LFDU	a1,  1 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFDU	a2,  1 * SIZE(AO2)

#if defined(PPCG4) && defined(DOUBLE)
	dcbt	X1, PREA
#endif
	LFDU	b1,  1 * SIZE(X1)
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO1, PREA
#endif

	FMADD	y03, a5, b2, y03
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y04, a6, b2, y04
	LFDU	a6,  1 * SIZE(AO2)

	LFDU	b2,  1 * SIZE(X1)

	FMADD	y01, a1, b3, y01
	LFDU	a1,  1 * SIZE(AO1)
	FMADD	y02, a2, b3, y02
	LFDU	a2,  1 * SIZE(AO2)

	LFDU	b3,  1 * SIZE(X1)
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO2, PREA
#endif

	FMADD	y03, a5, b4, y03
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y04, a6, b4, y04
	LFDU	a6,  1 * SIZE(AO2)

	LFDU	b4,  1 * SIZE(X1)
 	bdnz	LL(32)
	.align 4

LL(33):
	FMADD	y01, a1, b1, y01
	LFDU	a1,  1 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFDU	a2,  1 * SIZE(AO2)

	LFDU	b1,  1 * SIZE(X1)

	FMADD	y03, a5, b2, y03
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y04, a6, b2, y04
	LFDU	a6,  1 * SIZE(AO2)

	LFDU	b2,  1 * SIZE(X1)

	FMADD	y01, a1, b3, y01
	LFDU	a1,  1 * SIZE(AO1)
	FMADD	y02, a2, b3, y02
	LFDU	a2,  1 * SIZE(AO2)

	LFDU	b3,  1 * SIZE(X1)

	FMADD	y03, a5, b4, y03
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y04, a6, b4, y04
	LFDU	a6,  1 * SIZE(AO2)

	LFDU	b4,  1 * SIZE(X1)

	FMADD	y01, a1, b1, y01
	LFDU	a1,  1 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFDU	a2,  1 * SIZE(AO2)

	FMADD	y03, a5, b2, y03
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y04, a6, b2, y04
	LFDU	a6,  1 * SIZE(AO2)

	FMADD	y01, a1, b3, y01
	FMADD	y02, a2, b3, y02

	FMADD	y03, a5, b4, y03
	FMADD	y04, a6, b4, y04
	.align 4

LL(34):
	andi.	r0, M,  7
	ble	LL(38)

	andi.	r0, M,  4
	ble	LL(36)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	a2,  1 * SIZE(AO2)
	LFDU	b1,  1 * SIZE(X1)

	LFDU	b2,  1 * SIZE(X1)
	FMADD	y01, a1, b1, y01
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFDU	a6,  1 * SIZE(AO2)

	LFDU	b3,  1 * SIZE(X1)
	FMADD	y03, a5, b2, y03
	LFDU	a1,  1 * SIZE(AO1)
	FMADD	y04, a6, b2, y04
	LFDU	a2,  1 * SIZE(AO2)

	LFDU	b4,  1 * SIZE(X1)
	FMADD	y01, a1, b3, y01
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y02, a2, b3, y02
	LFDU	a6,  1 * SIZE(AO2)

	FMADD	y03, a5, b4, y03
	FMADD	y04, a6, b4, y04
	.align 4

LL(36):
	andi.	r0, M,  2
	ble	LL(37)

	LFDU	b1,  1 * SIZE(X1)
	LFDU	a1,  1 * SIZE(AO1)
	LFDU	a2,  1 * SIZE(AO2)
	LFDU	b2,  1 * SIZE(X1)
	LFDU	a3,  1 * SIZE(AO1)
	LFDU	a4,  1 * SIZE(AO2)

	FMADD	y01, a1, b1, y01
	FMADD	y02, a2, b1, y02
	FMADD	y03, a3, b2, y03
	FMADD	y04, a4, b2, y04
	.align 4

LL(37):
	andi.	r0, M,  1
	ble	LL(38)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	b1,  1 * SIZE(X1)
	LFDU	a2,  1 * SIZE(AO2)

	FMADD	y01, a1, b1, y01
	FMADD	y02, a2, b1, y02
	.align 4

LL(38):
	lfd	alpha, ALPHA

	LFDUX	a1, Y, INCY
	LFDUX	a2, Y, INCY

	FADD	y01, y03, y01
	FADD	y02, y04, y02

	FMADD	a1, alpha, f0, a1
	FMADD	a2, alpha, f1, a2

	STFDUX	a1, YY, INCY
	STFDUX	a2, YY, INCY
	.align 4

LL(40):
	andi.	J, N, 1
	ble	LL(999)

	mr     AO1, A
	add    A,   A, LDA

	mr     X1, XP

	lfd	 y01, FZERO
	fmr	 y02, y01

	srawi.	r0,  M, 3
	mtspr	CTR, r0
	ble	LL(44)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	a3,  1 * SIZE(AO1)
	LFDU	a4,  1 * SIZE(AO1)

	LFDU	b1,  1 * SIZE(X1)
	LFDU	b2,  1 * SIZE(X1)
	LFDU	b3,  1 * SIZE(X1)
	LFDU	b4,  1 * SIZE(X1)
	bdz	LL(43)
	.align 4

LL(42):
	FMADD	y01, a1, b1, y01
	LFDU	a1,  1 * SIZE(AO1)
	LFDU	b1,  1 * SIZE(X1)

#ifdef PPCG4
	dcbt	X1, PREA
#endif

	FMADD	y02, a2, b2, y02
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	b2,  1 * SIZE(X1)

#ifdef PPCG4
	dcbt	AO1, PREA
#endif

	FMADD	y01, a3, b3, y01
	LFDU	a3,  1 * SIZE(AO1)
	LFDU	b3,  1 * SIZE(X1)

	FMADD	y02, a4, b4, y02
 	LFDU	a4,  1 * SIZE(AO1)
	LFDU	b4,  1 * SIZE(X1)

	FMADD	y01, a1, b1, y01
	LFDU	a1,  1 * SIZE(AO1)
	LFDU	b1,  1 * SIZE(X1)

	FMADD	y02, a2, b2, y02
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	b2,  1 * SIZE(X1)

#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO1, PREA
#endif

	FMADD	y01, a3, b3, y01
	LFDU	a3,  1 * SIZE(AO1)
	LFDU	b3,  1 * SIZE(X1)

#if defined(PPCG4) && defined(DOUBLE)
	dcbt	X1, PREA
#endif

	FMADD	y02, a4, b4, y02
 	LFDU	a4,  1 * SIZE(AO1)
	LFDU	b4,  1 * SIZE(X1)

 	bdnz	LL(42)
	.align 4

LL(43):
	FMADD	y01, a1, b1, y01
	LFDU	a1,  1 * SIZE(AO1)
	LFDU	b1,  1 * SIZE(X1)

	FMADD	y02, a2, b2, y02
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	b2,  1 * SIZE(X1)

	FMADD	y01, a3, b3, y01
	LFDU	a3,  1 * SIZE(AO1)
	LFDU	b3,  1 * SIZE(X1)

	FMADD	y02, a4, b4, y02
 	LFDU	a4,  1 * SIZE(AO1)
	LFDU	b4,  1 * SIZE(X1)

	FMADD	y01, a1, b1, y01
	FMADD	y02, a2, b2, y02
	FMADD	y01, a3, b3, y01
	FMADD	y02, a4, b4, y02
	.align 4

LL(44):
	andi.	r0, M,  7
	ble	LL(48)

	andi.	r0, M,  4
	ble	LL(46)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	b1,  1 * SIZE(X1)
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	b2,  1 * SIZE(X1)

	FMADD	y01, a1, b1, y01
	LFDU	a3,  1 * SIZE(AO1)
	LFDU	b3,  1 * SIZE(X1)

	FMADD	y02, a2, b2, y02
	LFDU	a4,  1 * SIZE(AO1)
	LFDU	b4,  1 * SIZE(X1)

	FMADD	y01, a3, b3, y01
	FMADD	y02, a4, b4, y02
	.align 4

LL(46):
	andi.	r0, M,  2
	ble	LL(47)

	LFDU	b1,  1 * SIZE(X1)
	LFDU	a1,  1 * SIZE(AO1)
	LFDU	b2,  1 * SIZE(X1)
	LFDU	a2,  1 * SIZE(AO1)

	FMADD	y01, a1, b1, y01
	FMADD	y02, a2, b2, y02
	.align 4

LL(47):
	andi.	r0, M,  1
	ble	LL(48)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	b1,  1 * SIZE(X1)

	FMADD	y01, a1, b1, y01
	.align 4

LL(48):
	lfd	alpha, ALPHA

	LFDUX	a1, Y, INCY

	FADD	y01, y02, y01

	FMADD	a1, alpha, f0, a1

	STFDUX	a1, YY, INCY
	.align 4

LL(999):
	li	r3, 0

	lfd	f14,     0(SP)
	lfd	f15,     8(SP)
	lfd	f16,    16(SP)
	lfd	f17,    24(SP)
	lfd	f18,    32(SP)
	lfd	f19,    40(SP)
	lfd	f20,    48(SP)
	lfd	f21,    56(SP)
	lfd	f22,    64(SP)
	lfd	f23,    72(SP)

#ifdef __64BIT__
	ld	r14,   160(SP)
	ld	r15,   168(SP)
	ld	r16,   176(SP)
	ld	r17,   184(SP)
	ld	r18,   192(SP)
	ld	r19,   200(SP)
	ld	r20,   208(SP)
	ld	r21,   216(SP)
	ld	r22,   224(SP)
#else
	lwz	r14,   160(SP)
	lwz	r15,   164(SP)
	lwz	r16,   168(SP)
	lwz	r17,   172(SP)
	lwz	r18,   176(SP)
	lwz	r19,   180(SP)
	lwz	r20,   184(SP)
	lwz	r21,   188(SP)
	lwz	r22,   192(SP)
#endif

	addi	SP, SP, STACKSIZE

	blr

	EPILOGUE

#endif
