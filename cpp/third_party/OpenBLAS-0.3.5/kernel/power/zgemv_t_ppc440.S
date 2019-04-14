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

#define P 1024

#ifndef __64BIT__
#define STACKSIZE 224
#else
#define STACKSIZE 304
#endif

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
#define A	r8
#define LDA	r9
#define X	r10
#define	INCX	r5
#define	Y	r6
#define	INCY	r7
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
#define M	r3
#define	N	r4
#define A	r10
#define LDA	r5
#define X	r6
#define	INCX	r7
#define	Y	r8
#define	INCY	r9
#else
#define M	r3
#define	N	r4
#define A	r8
#define LDA	r9
#define X	r10
#define	INCX	r5
#define	Y	r6
#define	INCY	r7
#endif
#endif

#define	BUFFER	r11
#define	XP	r12
#define	X1	r14
#define	J	r15
#define	AO1	r16
#define	AO2	r17
#define	AO3	r18
#define	AO4	r19
#define	PREA	r20
#define	PREC	r21
#define	YY	r22

#if defined(PPCG4)
#define PREFETCHSIZE_A  (3 * 8)
#define PREFETCHSIZE_C   7
#endif

#if defined(POWER6)
#define PREFETCHSIZE_A  (3 * 8)
#define PREFETCHSIZE_C   7
#endif

#if !(defined(CONJ) && defined(XCONJ))
#define FMADDR FMADD
#define FMSUBR FNMSUB
#else
#define FMADDR FNMSUB
#define FMSUBR FMADD
#endif

#ifndef NEEDPARAM

#ifndef __64BIT__
#define FZERO	200(SP)
#else
#define FZERO	256(SP)
#endif

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
	stfd	f24,    80(SP)
	stfd	f25,    88(SP)
	stfd	f26,    96(SP)
	stfd	f27,   104(SP)
	stfd	f28,   112(SP)
	stfd	f29,   120(SP)
	stfd	f30,   128(SP)
	stfd	f31,   136(SP)

#ifdef __64BIT__
	std	r14,   144(SP)
	std	r15,   152(SP)
	std	r16,   160(SP)
	std	r17,   168(SP)
	std	r18,   176(SP)
	std	r19,   184(SP)
	std	r20,   192(SP)
	std	r21,   200(SP)
	std	r22,   208(SP)
	std	r0,    FZERO
#else
	stw	r14,   144(SP)
	stw	r15,   148(SP)
	stw	r16,   152(SP)
	stw	r17,   156(SP)
	stw	r18,   160(SP)
	stw	r19,   164(SP)
	stw	r20,   168(SP)
	stw	r21,   172(SP)
	stw	r22,   176(SP)
	stw	r0,    FZERO
	stw	r0,    4 + FZERO
#endif

#ifdef linux
#ifndef __64BIT__
	lwz	INCY,	 FRAMESLOT(0) + STACKSIZE(SP)
	lwz	BUFFER,  FRAMESLOT(1) + STACKSIZE(SP)
#else
	ld	INCX,    FRAMESLOT(0) + STACKSIZE(SP)
	ld	Y,       FRAMESLOT(1) + STACKSIZE(SP)
	ld	INCY,    FRAMESLOT(2) + STACKSIZE(SP)
	ld	BUFFER,  FRAMESLOT(3) + STACKSIZE(SP)
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#ifndef __64BIT__
#ifdef DOUBLE
	lwz	LDA,     FRAMESLOT(0) + STACKSIZE(SP)
	lwz	X,       FRAMESLOT(1) + STACKSIZE(SP)
	lwz	INCX,    FRAMESLOT(2) + STACKSIZE(SP)
	lwz	Y,       FRAMESLOT(3) + STACKSIZE(SP)
	lwz	INCY,    FRAMESLOT(4) + STACKSIZE(SP)
	lwz	BUFFER,  FRAMESLOT(5) + STACKSIZE(SP)
#else
	lwz	INCX,    FRAMESLOT(0) + STACKSIZE(SP)
	lwz	Y,       FRAMESLOT(1) + STACKSIZE(SP)
	lwz	INCY,    FRAMESLOT(2) + STACKSIZE(SP)
	lwz	BUFFER,  FRAMESLOT(3) + STACKSIZE(SP)
#endif
#else
	ld	INCX,    FRAMESLOT(0) + STACKSIZE(SP)
	ld	Y,       FRAMESLOT(1) + STACKSIZE(SP)
	ld	INCY,    FRAMESLOT(2) + STACKSIZE(SP)
	ld	BUFFER,  FRAMESLOT(3) + STACKSIZE(SP)
#endif
#endif

#ifndef XCONJ
#ifndef CONJ
#define FMADD1	FMADD
#define FMADD2	FMADD
#define FMADD3	FNMSUB
#define FMADD4	FMADD
#else
#define FMADD1	FMADD
#define FMADD2	FMADD
#define FMADD3	FMADD
#define FMADD4	FNMSUB
#endif
#else
#ifndef CONJ
#define FMADD1	FMADD
#define FMADD2	FNMSUB
#define FMADD3	FMADD
#define FMADD4	FMADD
#else
#define FMADD1	FMADD
#define FMADD2	FMADD
#define FMADD3	FNMSUB
#define FMADD4	FMADD
#endif
#endif

#define y1 f0
#define y2 f1
#define y3 f2
#define y4 f3
#define y5 f4
#define y6 f5
#define y7 f6
#define y8 f7

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

#define alpha_r	f24
#define alpha_i	f25

	fmr	alpha_r, f1
	fmr	alpha_i, f2

	slwi	LDA,  LDA,  ZBASE_SHIFT
	slwi	INCX, INCX, ZBASE_SHIFT
	slwi	INCY, INCY, ZBASE_SHIFT

	li	PREA, PREFETCHSIZE_A * SIZE
	li	PREC, PREFETCHSIZE_C * SIZE

	addi	A, A, -SIZE
	addi	INCX, INCX, -SIZE
	addi	INCY, INCY, -SIZE

	sub	X, X, INCX
	sub	Y, Y, INCY

	mr	YY, Y

	cmpwi	cr0, M, 0
	ble	LL(999)
	cmpwi	cr0, N, 0
	ble	LL(999)

	mr	XP, X
	cmpwi	cr0, INCX, SIZE
	beq	LL(10)

	addi	XP, BUFFER, -SIZE
	addi	X1, BUFFER, -SIZE

	srawi.	r0, M, 2
	mtspr	CTR, r0
	ble	LL(05)
	.align 4

LL(02):
	LFDUX	f0, X, INCX
	LFDU	f1, 1 * SIZE(X)
	LFDUX	f2, X, INCX
	LFDU	f3, 1 * SIZE(X)
	LFDUX	f4, X, INCX
	LFDU	f5, 1 * SIZE(X)
	LFDUX	f6, X, INCX
	LFDU	f7, 1 * SIZE(X)

	STFDU	f0,  1 * SIZE(X1)
	STFDU	f1,  1 * SIZE(X1)
	STFDU	f2,  1 * SIZE(X1)
	STFDU	f3,  1 * SIZE(X1)
	STFDU	f4,  1 * SIZE(X1)
	STFDU	f5,  1 * SIZE(X1)
	STFDU	f6,  1 * SIZE(X1)
	STFDU	f7,  1 * SIZE(X1)
	bdnz	LL(02)
	.align 4

LL(05):
	andi.	r0, M, 3
	mtspr	CTR, r0
	ble	LL(10)
	.align 4

LL(06):
	LFDUX	f0, X, INCX
	LFDU	f1,  1 * SIZE(X)
	STFDU	f0,  1 * SIZE(X1)
	STFDU	f1,  1 * SIZE(X1)
	bdnz	LL(06)
	.align 4

LL(10):
	srawi.	J, N, 2
	ble	LL(20)
	.align 4

LL(11):
	lfd	 y1,  FZERO
	mr     AO1, A
	fmr	 y2,  y1
	mr     X1, XP
	fmr	 y3,  y1
	add    AO2, A,   LDA
	fmr	 y4,  y1
	add    AO3, AO2, LDA
	fmr	 y5,  y1
	add    AO4, AO3, LDA
	fmr	 y6,  y1
	add    A,   AO4, LDA
	fmr	 y7,  y1

	dcbtst	 PREC, Y
	fmr	 y8,  y1

	srawi.	r0,  M, 2
	mtspr	CTR, r0
	ble	LL(15)

	LFDU	a1, 1 * SIZE(AO1)
	LFDU	b1, 1 * SIZE(X1)
	LFDU	a2, 1 * SIZE(AO1)
	LFDU	b2, 1 * SIZE(X1)
	LFDU	a3, 1 * SIZE(AO2)
	LFDU	a4, 1 * SIZE(AO2)
	LFDU	a5, 1 * SIZE(AO3)
	LFDU	a6, 1 * SIZE(AO3)
	LFDU	a7, 1 * SIZE(AO4)
	bdz	LL(13)
	.align 5

LL(12):
	FMADD1	y1,  a1,  b1, y1
	LFDU	a8, 1 * SIZE(AO4)
	FMADD2	y2,  a1,  b2, y2
	LFDU	b3, 1 * SIZE(X1)
	FMADD1	y3,  a3,  b1, y3
	LFDU	b4, 1 * SIZE(X1)
	FMADD2	y4,  a3,  b2, y4

#ifdef PPCG4
	dcbt	AO1, PREA
#endif

	FMADD3	y1,  a2,  b2, y1
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y2,  a2,  b1, y2
	LFDU	a2, 1 * SIZE(AO1)
	FMADD3	y3,  a4,  b2, y3
	LFDU	a3, 1 * SIZE(AO2)
	FMADD4	y4,  a4,  b1, y4
	LFDU	a4, 1 * SIZE(AO2)

#ifdef PPCG4
	dcbt	X1, PREA
#endif

	FMADD1	y5,  a5,  b1, y5
	FMADD2	y6,  a5,  b2, y6
	FMADD1	y7,  a7,  b1, y7
	FMADD2	y8,  a7,  b2, y8

#ifdef PPCG4
	dcbt	AO2, PREA
#endif

	FMADD3	y5,  a6,  b2, y5
	LFDU	a5, 1 * SIZE(AO3)
	FMADD4	y6,  a6,  b1, y6
	LFDU	a6, 1 * SIZE(AO3)
	FMADD3	y7,  a8,  b2, y7
	LFDU	a7, 1 * SIZE(AO4)
	FMADD4	y8,  a8,  b1, y8
	LFDU	a8, 1 * SIZE(AO4)


	FMADD1	y1,  a1,  b3, y1
	LFDU	b1, 1 * SIZE(X1)
	FMADD2	y2,  a1,  b4, y2
	LFDU	b2, 1 * SIZE(X1)
	FMADD1	y3,  a3,  b3, y3
	FMADD2	y4,  a3,  b4, y4

#ifdef PPCG4
	dcbt	AO3, PREA
#endif

	FMADD3	y1,  a2,  b4, y1
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y2,  a2,  b3, y2
	LFDU	a2, 1 * SIZE(AO1)
	FMADD3	y3,  a4,  b4, y3
	LFDU	a3, 1 * SIZE(AO2)
	FMADD4	y4,  a4,  b3, y4
	LFDU	a4, 1 * SIZE(AO2)

	FMADD1	y5,  a5,  b3, y5
	FMADD2	y6,  a5,  b4, y6
	FMADD1	y7,  a7,  b3, y7
	FMADD2	y8,  a7,  b4, y8

#ifdef PPCG4
	dcbt	AO4, PREA
#endif

	FMADD3	y5,  a6,  b4, y5
	LFDU	a5, 1 * SIZE(AO3)
	FMADD4	y6,  a6,  b3, y6
	LFDU	a6, 1 * SIZE(AO3)
	FMADD3	y7,  a8,  b4, y7
	LFDU	a7, 1 * SIZE(AO4)
	FMADD4	y8,  a8,  b3, y8
	LFDU	a8, 1 * SIZE(AO4)

	FMADD1	y1,  a1,  b1, y1
	LFDU	b3, 1 * SIZE(X1)
	FMADD2	y2,  a1,  b2, y2
	LFDU	b4, 1 * SIZE(X1)
	FMADD1	y3,  a3,  b1, y3
	FMADD2	y4,  a3,  b2, y4

#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO1, PREA
#endif

	FMADD3	y1,  a2,  b2, y1
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y2,  a2,  b1, y2
	LFDU	a2, 1 * SIZE(AO1)
	FMADD3	y3,  a4,  b2, y3
	LFDU	a3, 1 * SIZE(AO2)
	FMADD4	y4,  a4,  b1, y4
	LFDU	a4, 1 * SIZE(AO2)

#if defined(PPCG4) && defined(DOUBLE)
	dcbt	X1, PREA
#endif

	FMADD1	y5,  a5,  b1, y5
	FMADD2	y6,  a5,  b2, y6
	FMADD1	y7,  a7,  b1, y7
	FMADD2	y8,  a7,  b2, y8

#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO2, PREA
#endif

	FMADD3	y5,  a6,  b2, y5
	LFDU	a5, 1 * SIZE(AO3)
	FMADD4	y6,  a6,  b1, y6
	LFDU	a6, 1 * SIZE(AO3)
	FMADD3	y7,  a8,  b2, y7
	LFDU	a7, 1 * SIZE(AO4)
	FMADD4	y8,  a8,  b1, y8
	LFDU	a8, 1 * SIZE(AO4)

	FMADD1	y1,  a1,  b3, y1
	FMADD2	y2,  a1,  b4, y2
	FMADD1	y3,  a3,  b3, y3
	FMADD2	y4,  a3,  b4, y4

#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO3, PREA
#endif

	FMADD3	y1,  a2,  b4, y1
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y2,  a2,  b3, y2
	LFDU	a2, 1 * SIZE(AO1)
	FMADD3	y3,  a4,  b4, y3
	LFDU	a3, 1 * SIZE(AO2)
	FMADD4	y4,  a4,  b3, y4
	LFDU	a4, 1 * SIZE(AO2)

	FMADD1	y5,  a5,  b3, y5
	LFDU	b1, 1 * SIZE(X1)
	FMADD2	y6,  a5,  b4, y6
	LFDU	b2, 1 * SIZE(X1)
	FMADD1	y7,  a7,  b3, y7
	FMADD2	y8,  a7,  b4, y8

#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO4, PREA
#endif

	FMADD3	y5,  a6,  b4, y5
	LFDU	a5, 1 * SIZE(AO3)
	FMADD4	y6,  a6,  b3, y6
	LFDU	a6, 1 * SIZE(AO3)
	FMADD3	y7,  a8,  b4, y7
	LFDU	a7, 1 * SIZE(AO4)
	FMADD4	y8,  a8,  b3, y8
 	bdnz	LL(12)
	.align 4

LL(13):
	FMADD1	y1,  a1,  b1, y1
	LFDU	a8, 1 * SIZE(AO4)
	FMADD2	y2,  a1,  b2, y2
	LFDU	b3, 1 * SIZE(X1)
	FMADD1	y3,  a3,  b1, y3
	LFDU	b4, 1 * SIZE(X1)
	FMADD2	y4,  a3,  b2, y4

	FMADD3	y1,  a2,  b2, y1
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y2,  a2,  b1, y2
	LFDU	a2, 1 * SIZE(AO1)
	FMADD3	y3,  a4,  b2, y3
	LFDU	a3, 1 * SIZE(AO2)
	FMADD4	y4,  a4,  b1, y4
	LFDU	a4, 1 * SIZE(AO2)

	FMADD1	y5,  a5,  b1, y5
	FMADD2	y6,  a5,  b2, y6
	FMADD1	y7,  a7,  b1, y7
	FMADD2	y8,  a7,  b2, y8

	FMADD3	y5,  a6,  b2, y5
	LFDU	a5, 1 * SIZE(AO3)
	FMADD4	y6,  a6,  b1, y6
	LFDU	a6, 1 * SIZE(AO3)
	FMADD3	y7,  a8,  b2, y7
	LFDU	a7, 1 * SIZE(AO4)
	FMADD4	y8,  a8,  b1, y8
	LFDU	a8, 1 * SIZE(AO4)

	FMADD1	y1,  a1,  b3, y1
	LFDU	b1, 1 * SIZE(X1)
	FMADD2	y2,  a1,  b4, y2
	LFDU	b2, 1 * SIZE(X1)
	FMADD1	y3,  a3,  b3, y3
	FMADD2	y4,  a3,  b4, y4

	FMADD3	y1,  a2,  b4, y1
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y2,  a2,  b3, y2
	LFDU	a2, 1 * SIZE(AO1)
	FMADD3	y3,  a4,  b4, y3
	LFDU	a3, 1 * SIZE(AO2)
	FMADD4	y4,  a4,  b3, y4
	LFDU	a4, 1 * SIZE(AO2)

	FMADD1	y5,  a5,  b3, y5
	FMADD2	y6,  a5,  b4, y6
	FMADD1	y7,  a7,  b3, y7
	FMADD2	y8,  a7,  b4, y8

	FMADD3	y5,  a6,  b4, y5
	LFDU	a5, 1 * SIZE(AO3)
	FMADD4	y6,  a6,  b3, y6
	LFDU	a6, 1 * SIZE(AO3)
	FMADD3	y7,  a8,  b4, y7
	LFDU	a7, 1 * SIZE(AO4)
	FMADD4	y8,  a8,  b3, y8
	LFDU	a8, 1 * SIZE(AO4)

	FMADD1	y1,  a1,  b1, y1
	LFDU	b3, 1 * SIZE(X1)
	FMADD2	y2,  a1,  b2, y2
	LFDU	b4, 1 * SIZE(X1)
	FMADD1	y3,  a3,  b1, y3
	FMADD2	y4,  a3,  b2, y4

	FMADD3	y1,  a2,  b2, y1
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y2,  a2,  b1, y2
	LFDU	a2, 1 * SIZE(AO1)
	FMADD3	y3,  a4,  b2, y3
	LFDU	a3, 1 * SIZE(AO2)
	FMADD4	y4,  a4,  b1, y4
	LFDU	a4, 1 * SIZE(AO2)

	FMADD1	y5,  a5,  b1, y5
	FMADD2	y6,  a5,  b2, y6
	FMADD1	y7,  a7,  b1, y7
	FMADD2	y8,  a7,  b2, y8

	FMADD3	y5,  a6,  b2, y5
	LFDU	a5, 1 * SIZE(AO3)
	FMADD4	y6,  a6,  b1, y6
	LFDU	a6, 1 * SIZE(AO3)
	FMADD3	y7,  a8,  b2, y7
	LFDU	a7, 1 * SIZE(AO4)
	FMADD4	y8,  a8,  b1, y8
	LFDU	a8, 1 * SIZE(AO4)

	FMADD1	y1,  a1,  b3, y1
	FMADD2	y2,  a1,  b4, y2
	FMADD1	y3,  a3,  b3, y3
	FMADD2	y4,  a3,  b4, y4

	FMADD3	y1,  a2,  b4, y1
	FMADD4	y2,  a2,  b3, y2
	FMADD3	y3,  a4,  b4, y3
	FMADD4	y4,  a4,  b3, y4

	FMADD1	y5,  a5,  b3, y5
	FMADD2	y6,  a5,  b4, y6
	FMADD1	y7,  a7,  b3, y7
	FMADD2	y8,  a7,  b4, y8

	FMADD3	y5,  a6,  b4, y5
	FMADD4	y6,  a6,  b3, y6
	FMADD3	y7,  a8,  b4, y7
	FMADD4	y8,  a8,  b3, y8
	.align 4

LL(15):
	andi.	r0, M, 2
	ble	LL(17)

	LFDU	a1, 1 * SIZE(AO1)
	LFDU	b1, 1 * SIZE(X1)
	LFDU	a2, 1 * SIZE(AO1)
	LFDU	b2, 1 * SIZE(X1)
	LFDU	a3, 1 * SIZE(AO2)
	LFDU	b3, 1 * SIZE(X1)
	LFDU	a4, 1 * SIZE(AO2)
	LFDU	b4, 1 * SIZE(X1)

	FMADD1	y1,  a1,  b1, y1
	LFDU	a5, 1 * SIZE(AO3)
	FMADD2	y2,  a1,  b2, y2
	LFDU	a6, 1 * SIZE(AO3)
	FMADD1	y3,  a3,  b1, y3
	LFDU	a7, 1 * SIZE(AO4)
	FMADD2	y4,  a3,  b2, y4
	LFDU	a8, 1 * SIZE(AO4)

	FMADD3	y1,  a2,  b2, y1
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y2,  a2,  b1, y2
	LFDU	a2, 1 * SIZE(AO1)
	FMADD3	y3,  a4,  b2, y3
	LFDU	a3, 1 * SIZE(AO2)
	FMADD4	y4,  a4,  b1, y4
	LFDU	a4, 1 * SIZE(AO2)

	FMADD1	y5,  a5,  b1, y5
	FMADD2	y6,  a5,  b2, y6
	FMADD1	y7,  a7,  b1, y7
	FMADD2	y8,  a7,  b2, y8

	FMADD3	y5,  a6,  b2, y5
	LFDU	a5, 1 * SIZE(AO3)
	FMADD4	y6,  a6,  b1, y6
	LFDU	a6, 1 * SIZE(AO3)
	FMADD3	y7,  a8,  b2, y7
	LFDU	a7, 1 * SIZE(AO4)
	FMADD4	y8,  a8,  b1, y8
	LFDU	a8, 1 * SIZE(AO4)

	FMADD1	y1,  a1,  b3, y1
	FMADD2	y2,  a1,  b4, y2
	FMADD1	y3,  a3,  b3, y3
	FMADD2	y4,  a3,  b4, y4

	FMADD3	y1,  a2,  b4, y1
	FMADD4	y2,  a2,  b3, y2
	FMADD3	y3,  a4,  b4, y3
	FMADD4	y4,  a4,  b3, y4

	FMADD1	y5,  a5,  b3, y5
	FMADD2	y6,  a5,  b4, y6
	FMADD1	y7,  a7,  b3, y7
	FMADD2	y8,  a7,  b4, y8

	FMADD3	y5,  a6,  b4, y5
	FMADD4	y6,  a6,  b3, y6
	FMADD3	y7,  a8,  b4, y7
	FMADD4	y8,  a8,  b3, y8
	.align 4

LL(17):
	andi.	r0, M, 1
	ble	LL(19)

	LFDU	a1, 1 * SIZE(AO1)
	LFDU	a2, 1 * SIZE(AO1)
	LFDU	a3, 1 * SIZE(AO2)
	LFDU	a4, 1 * SIZE(AO2)
	LFDU	a5, 1 * SIZE(AO3)
	LFDU	a6, 1 * SIZE(AO3)
	LFDU	a7, 1 * SIZE(AO4)
	LFDU	a8, 1 * SIZE(AO4)

	LFDU	b1, 1 * SIZE(X1)
	LFDU	b2, 1 * SIZE(X1)

	FMADD1	y1,  a1,  b1, y1
	FMADD2	y2,  a1,  b2, y2
	FMADD1	y3,  a3,  b1, y3
	FMADD2	y4,  a3,  b2, y4

	FMADD3	y1,  a2,  b2, y1
	FMADD4	y2,  a2,  b1, y2
	FMADD3	y3,  a4,  b2, y3
	FMADD4	y4,  a4,  b1, y4

	FMADD1	y5,  a5,  b1, y5
	FMADD2	y6,  a5,  b2, y6
	FMADD1	y7,  a7,  b1, y7
	FMADD2	y8,  a7,  b2, y8

	FMADD3	y5,  a6,  b2, y5
	FMADD4	y6,  a6,  b1, y6
	FMADD3	y7,  a8,  b2, y7
	FMADD4	y8,  a8,  b1, y8
	.align 4

LL(19):
	LFDUX	b1,  Y, INCY
	LFDU	b2,  1 * SIZE(Y)
	LFDUX	b3,  Y, INCY
	LFDU	b4,  1 * SIZE(Y)
	LFDUX	b5,  Y, INCY
	LFDU	b6,  1 * SIZE(Y)
	LFDUX	b7,  Y, INCY
	LFDU	b8,  1 * SIZE(Y)

	FMADD	b1, alpha_r, y1, b1
	FMADDR	b2, alpha_r, y2, b2
	FMADD	b3, alpha_r, y3, b3
	FMADDR	b4, alpha_r, y4, b4

	FMADD	b5, alpha_r, y5, b5
	FMADDR	b6, alpha_r, y6, b6
	FMADD	b7, alpha_r, y7, b7
	FMADDR	b8, alpha_r, y8, b8

	FMSUBR	b1, alpha_i, y2, b1
	FMADD	b2, alpha_i, y1, b2
	FMSUBR	b3, alpha_i, y4, b3
	FMADD	b4, alpha_i, y3, b4

	FMSUBR	b5, alpha_i, y6, b5
	FMADD	b6, alpha_i, y5, b6
	FMSUBR	b7, alpha_i, y8, b7
	FMADD	b8, alpha_i, y7, b8

	STFDUX	b1,  YY, INCY
	STFDU	b2,  1 * SIZE(YY)
	STFDUX	b3,  YY, INCY
	STFDU	b4,  1 * SIZE(YY)

	STFDUX	b5,  YY, INCY
	STFDU	b6,  1 * SIZE(YY)
	STFDUX	b7,  YY, INCY
	STFDU	b8,  1 * SIZE(YY)

	addi	J, J, -1
	cmpwi	cr0, J, 0
	bgt	LL(11)
	.align 4

LL(20):
	andi.	J, N, 2
	ble	LL(30)

	lfd	 y1,  FZERO
	mr     AO1, A
	fmr	 y2,  y1
	mr     X1, XP
	fmr	 y3,  y1
	add    AO2, A,   LDA
	fmr	 y4,  y1
	add    A,   AO2, LDA

	srawi.	r0,  M, 2
	mtspr	CTR, r0
	ble	LL(25)

	LFDU	a1, 1 * SIZE(AO1)
	LFDU	b1, 1 * SIZE(X1)
	LFDU	a2, 1 * SIZE(AO1)
	LFDU	b2, 1 * SIZE(X1)
	LFDU	a3, 1 * SIZE(AO2)
	bdz	LL(23)
	.align 5

LL(22):
	FMADD1	y1,  a1,  b1, y1
	LFDU	a4, 1 * SIZE(AO2)
	FMADD2	y2,  a1,  b2, y2
	LFDU	b3, 1 * SIZE(X1)
	FMADD1	y3,  a3,  b1, y3
	LFDU	b4, 1 * SIZE(X1)
	FMADD2	y4,  a3,  b2, y4

#ifdef PPCG4
	dcbt	AO1, PREA
#endif

	FMADD3	y1,  a2,  b2, y1
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y2,  a2,  b1, y2
	LFDU	a2, 1 * SIZE(AO1)
	FMADD3	y3,  a4,  b2, y3
	LFDU	a3, 1 * SIZE(AO2)
	FMADD4	y4,  a4,  b1, y4
	LFDU	a4, 1 * SIZE(AO2)

#ifdef PPCG4
	dcbt	AO2, PREA
#endif

	FMADD1	y1,  a1,  b3, y1
	LFDU	b1, 1 * SIZE(X1)
	FMADD2	y2,  a1,  b4, y2
	LFDU	b2, 1 * SIZE(X1)
	FMADD1	y3,  a3,  b3, y3
	FMADD2	y4,  a3,  b4, y4

#ifdef PPCG4
	dcbt	X1, PREA
#endif

	FMADD3	y1,  a2,  b4, y1
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y2,  a2,  b3, y2
	LFDU	a2, 1 * SIZE(AO1)
	FMADD3	y3,  a4,  b4, y3
	LFDU	a3, 1 * SIZE(AO2)
	FMADD4	y4,  a4,  b3, y4
	LFDU	a4, 1 * SIZE(AO2)

	FMADD1	y1,  a1,  b1, y1
	LFDU	b3, 1 * SIZE(X1)
	FMADD2	y2,  a1,  b2, y2
	LFDU	b4, 1 * SIZE(X1)
	FMADD1	y3,  a3,  b1, y3
	FMADD2	y4,  a3,  b2, y4

#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO1, PREA
#endif

	FMADD3	y1,  a2,  b2, y1
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y2,  a2,  b1, y2
	LFDU	a2, 1 * SIZE(AO1)
	FMADD3	y3,  a4,  b2, y3
	LFDU	a3, 1 * SIZE(AO2)
	FMADD4	y4,  a4,  b1, y4
	LFDU	a4, 1 * SIZE(AO2)

#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO2, PREA
#endif

	FMADD1	y1,  a1,  b3, y1
	LFDU	b1, 1 * SIZE(X1)
	FMADD2	y2,  a1,  b4, y2
	LFDU	b2, 1 * SIZE(X1)
	FMADD1	y3,  a3,  b3, y3
	FMADD2	y4,  a3,  b4, y4

#if defined(PPCG4) && defined(DOUBLE)
	dcbt	X1, PREA
#endif

	FMADD3	y1,  a2,  b4, y1
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y2,  a2,  b3, y2
	LFDU	a2, 1 * SIZE(AO1)
	FMADD3	y3,  a4,  b4, y3
	LFDU	a3, 1 * SIZE(AO2)
	FMADD4	y4,  a4,  b3, y4

 	bdnz	LL(22)
	.align 4

LL(23):
	FMADD1	y1,  a1,  b1, y1
	LFDU	a4, 1 * SIZE(AO2)
	FMADD2	y2,  a1,  b2, y2
	LFDU	b3, 1 * SIZE(X1)
	FMADD1	y3,  a3,  b1, y3
	LFDU	b4, 1 * SIZE(X1)
	FMADD2	y4,  a3,  b2, y4

	FMADD3	y1,  a2,  b2, y1
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y2,  a2,  b1, y2
	LFDU	a2, 1 * SIZE(AO1)
	FMADD3	y3,  a4,  b2, y3
	LFDU	a3, 1 * SIZE(AO2)
	FMADD4	y4,  a4,  b1, y4
	LFDU	a4, 1 * SIZE(AO2)

	FMADD1	y1,  a1,  b3, y1
	LFDU	b1, 1 * SIZE(X1)
	FMADD2	y2,  a1,  b4, y2
	LFDU	b2, 1 * SIZE(X1)
	FMADD1	y3,  a3,  b3, y3
	FMADD2	y4,  a3,  b4, y4

	FMADD3	y1,  a2,  b4, y1
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y2,  a2,  b3, y2
	LFDU	a2, 1 * SIZE(AO1)
	FMADD3	y3,  a4,  b4, y3
	LFDU	a3, 1 * SIZE(AO2)
	FMADD4	y4,  a4,  b3, y4
	LFDU	a4, 1 * SIZE(AO2)

	FMADD1	y1,  a1,  b1, y1
	LFDU	b3, 1 * SIZE(X1)
	FMADD2	y2,  a1,  b2, y2
	LFDU	b4, 1 * SIZE(X1)
	FMADD1	y3,  a3,  b1, y3
	FMADD2	y4,  a3,  b2, y4

	FMADD3	y1,  a2,  b2, y1
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y2,  a2,  b1, y2
	LFDU	a2, 1 * SIZE(AO1)
	FMADD3	y3,  a4,  b2, y3
	LFDU	a3, 1 * SIZE(AO2)
	FMADD4	y4,  a4,  b1, y4
	LFDU	a4, 1 * SIZE(AO2)

	FMADD1	y1,  a1,  b3, y1
	FMADD2	y2,  a1,  b4, y2
	FMADD1	y3,  a3,  b3, y3
	FMADD2	y4,  a3,  b4, y4

	FMADD3	y1,  a2,  b4, y1
	FMADD4	y2,  a2,  b3, y2
	FMADD3	y3,  a4,  b4, y3
	FMADD4	y4,  a4,  b3, y4
	.align 4

LL(25):
	andi.	r0, M, 2
	ble	LL(27)

	LFDU	a1, 1 * SIZE(AO1)
	LFDU	b1, 1 * SIZE(X1)
	LFDU	a2, 1 * SIZE(AO1)
	LFDU	b2, 1 * SIZE(X1)
	LFDU	a3, 1 * SIZE(AO2)
	LFDU	b3, 1 * SIZE(X1)
	LFDU	a4, 1 * SIZE(AO2)
	LFDU	b4, 1 * SIZE(X1)

	FMADD1	y1,  a1,  b1, y1
	FMADD2	y2,  a1,  b2, y2
	FMADD1	y3,  a3,  b1, y3
	FMADD2	y4,  a3,  b2, y4

	FMADD3	y1,  a2,  b2, y1
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y2,  a2,  b1, y2
	LFDU	a2, 1 * SIZE(AO1)
	FMADD3	y3,  a4,  b2, y3
	LFDU	a3, 1 * SIZE(AO2)
	FMADD4	y4,  a4,  b1, y4
	LFDU	a4, 1 * SIZE(AO2)

	FMADD1	y1,  a1,  b3, y1
	FMADD2	y2,  a1,  b4, y2
	FMADD1	y3,  a3,  b3, y3
	FMADD2	y4,  a3,  b4, y4

	FMADD3	y1,  a2,  b4, y1
	FMADD4	y2,  a2,  b3, y2
	FMADD3	y3,  a4,  b4, y3
	FMADD4	y4,  a4,  b3, y4
	.align 4

LL(27):
	andi.	r0, M, 1
	ble	LL(29)

	LFDU	a1, 1 * SIZE(AO1)
	LFDU	a2, 1 * SIZE(AO1)
	LFDU	a3, 1 * SIZE(AO2)
	LFDU	a4, 1 * SIZE(AO2)

	LFDU	b1, 1 * SIZE(X1)
	LFDU	b2, 1 * SIZE(X1)

	FMADD1	y1,  a1,  b1, y1
	FMADD2	y2,  a1,  b2, y2
	FMADD1	y3,  a3,  b1, y3
	FMADD2	y4,  a3,  b2, y4

	FMADD3	y1,  a2,  b2, y1
	FMADD4	y2,  a2,  b1, y2
	FMADD3	y3,  a4,  b2, y3
	FMADD4	y4,  a4,  b1, y4
	.align 4

LL(29):
	LFDUX	b1,  Y, INCY
	LFDU	b2,  1 * SIZE(Y)
	LFDUX	b3,  Y, INCY
	LFDU	b4,  1 * SIZE(Y)

	FMADD	b1, alpha_r, y1, b1
	FMADDR	b2, alpha_r, y2, b2
	FMADD	b3, alpha_r, y3, b3
	FMADDR	b4, alpha_r, y4, b4

	FMSUBR	b1, alpha_i, y2, b1
	FMADD	b2, alpha_i, y1, b2
	FMSUBR	b3, alpha_i, y4, b3
	FMADD	b4, alpha_i, y3, b4

	STFDUX	b1,  YY, INCY
	STFDU	b2,  1 * SIZE(YY)
	STFDUX	b3,  YY, INCY
	STFDU	b4,  1 * SIZE(YY)
	.align 4

LL(30):
	andi.	J, N, 1
	ble	LL(999)

	lfd	 y1,  FZERO
	mr     AO1, A
	fmr	 y2,  y1
	mr     X1, XP
	fmr	 y3,  y1
	fmr	 y4,  y1
	add    A,   A, LDA

	srawi.	r0,  M, 2
	mtspr	CTR, r0
	ble	LL(35)

	LFDU	a1, 1 * SIZE(AO1)
	LFDU	b1, 1 * SIZE(X1)
	LFDU	a2, 1 * SIZE(AO1)
	LFDU	b2, 1 * SIZE(X1)
	bdz	LL(33)
	.align 5

LL(32):
	FMADD1	y1,  a1,  b1, y1
	LFDU	b3, 1 * SIZE(X1)
	FMADD2	y2,  a1,  b2, y2
	LFDU	b4, 1 * SIZE(X1)

#ifdef PPCG4
	dcbt	AO1, PREA
#endif

	FMADD3	y3,  a2,  b2, y3
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y4,  a2,  b1, y4
	LFDU	a2, 1 * SIZE(AO1)

	FMADD1	y1,  a1,  b3, y1
	LFDU	b1, 1 * SIZE(X1)
	FMADD2	y2,  a1,  b4, y2
	LFDU	b2, 1 * SIZE(X1)

#ifdef PPCG4
	dcbt	X1, PREA
#endif

	FMADD3	y3,  a2,  b4, y3
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y4,  a2,  b3, y4
	LFDU	a2, 1 * SIZE(AO1)

	FMADD1	y1,  a1,  b1, y1
	LFDU	b3, 1 * SIZE(X1)
	FMADD2	y2,  a1,  b2, y2
	LFDU	b4, 1 * SIZE(X1)

#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO1, PREA
#endif

	FMADD3	y3,  a2,  b2, y3
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y4,  a2,  b1, y4
	LFDU	a2, 1 * SIZE(AO1)

	FMADD1	y1,  a1,  b3, y1
	LFDU	b1, 1 * SIZE(X1)
	FMADD2	y2,  a1,  b4, y2
	LFDU	b2, 1 * SIZE(X1)

#if defined(PPCG4) && defined(DOUBLE)
	dcbt	X1, PREA
#endif

	FMADD3	y3,  a2,  b4, y3
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y4,  a2,  b3, y4
	LFDU	a2, 1 * SIZE(AO1)

 	bdnz	LL(32)
	.align 4

LL(33):
	FMADD1	y1,  a1,  b1, y1
	LFDU	b3, 1 * SIZE(X1)
	FMADD2	y2,  a1,  b2, y2
	LFDU	b4, 1 * SIZE(X1)

	FMADD3	y3,  a2,  b2, y3
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y4,  a2,  b1, y4
	LFDU	a2, 1 * SIZE(AO1)

	FMADD1	y1,  a1,  b3, y1
	LFDU	b1, 1 * SIZE(X1)
	FMADD2	y2,  a1,  b4, y2
	LFDU	b2, 1 * SIZE(X1)

	FMADD3	y3,  a2,  b4, y3
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y4,  a2,  b3, y4
	LFDU	a2, 1 * SIZE(AO1)

	FMADD1	y1,  a1,  b1, y1
	LFDU	b3, 1 * SIZE(X1)
	FMADD2	y2,  a1,  b2, y2
	LFDU	b4, 1 * SIZE(X1)

	FMADD3	y3,  a2,  b2, y3
	LFDU	a1, 1 * SIZE(AO1)
	FMADD4	y4,  a2,  b1, y4
	LFDU	a2, 1 * SIZE(AO1)

	FMADD1	y1,  a1,  b3, y1
	FMADD2	y2,  a1,  b4, y2
	FMADD3	y3,  a2,  b4, y3
	FMADD4	y4,  a2,  b3, y4
	.align 4

LL(35):
	andi.	r0, M, 2
	ble	LL(37)

	LFDU	a1, 1 * SIZE(AO1)
	LFDU	b1, 1 * SIZE(X1)
	LFDU	a2, 1 * SIZE(AO1)
	LFDU	b2, 1 * SIZE(X1)

	FMADD1	y1,  a1,  b1, y1
	LFDU	b3, 1 * SIZE(X1)
	FMADD2	y2,  a1,  b2, y2
	LFDU	a3, 1 * SIZE(AO1)
	FMADD3	y3,  a2,  b2, y3
	LFDU	b4, 1 * SIZE(X1)
	FMADD4	y4,  a2,  b1, y4
	LFDU	a4, 1 * SIZE(AO1)

	FMADD1	y1,  a3,  b3, y1
	FMADD2	y2,  a3,  b4, y2
	FMADD3	y3,  a4,  b4, y3
	FMADD4	y4,  a4,  b3, y4
	.align 4

LL(37):
	andi.	r0, M, 1
	ble	LL(39)

	LFDU	a1, 1 * SIZE(AO1)
	LFDU	b1, 1 * SIZE(X1)
	LFDU	a2, 1 * SIZE(AO1)
	LFDU	b2, 1 * SIZE(X1)

	FMADD1	y1,  a1,  b1, y1
	FMADD2	y2,  a1,  b2, y2
	FMADD3	y3,  a2,  b2, y3
	FMADD4	y4,  a2,  b1, y4
	.align 4

LL(39):
	LFDUX	b1,  Y, INCY
	LFDU	b2,  1 * SIZE(Y)

	FADD	y1, y1, y3
	FADD	y2, y2, y4

	FMADD	b1, alpha_r, y1, b1
	FMADDR	b2, alpha_r, y2, b2
	FMSUBR	b1, alpha_i, y2, b1
	FMADD	b2, alpha_i, y1, b2

	STFDUX	b1,  YY, INCY
	STFDU	b2,  1 * SIZE(YY)
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
	lfd	f24,    80(SP)
	lfd	f25,    88(SP)
	lfd	f26,    96(SP)
	lfd	f27,   104(SP)
	lfd	f28,   112(SP)
	lfd	f29,   120(SP)
	lfd	f30,   128(SP)
	lfd	f31,   136(SP)

#ifdef __64BIT__
	ld	r14,   144(SP)
	ld	r15,   152(SP)
	ld	r16,   160(SP)
	ld	r17,   168(SP)
	ld	r18,   176(SP)
	ld	r19,   184(SP)
	ld	r20,   192(SP)
	ld	r21,   200(SP)
	ld	r22,   208(SP)
#else
	lwz	r14,   144(SP)
	lwz	r15,   148(SP)
	lwz	r16,   152(SP)
	lwz	r17,   156(SP)
	lwz	r18,   160(SP)
	lwz	r19,   164(SP)
	lwz	r20,   168(SP)
	lwz	r21,   172(SP)
	lwz	r22,   176(SP)
#endif

	addi	SP, SP, STACKSIZE

	blr

	EPILOGUE

#endif
