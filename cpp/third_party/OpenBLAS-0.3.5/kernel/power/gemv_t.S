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
#define AO5	r18
#define	AO6	r19
#define	AO7	r20
#define	AO8	r21
#define	MIN_N	r22
#define	J	r23
#define CO	r24
#define	PREA	r25
#define	PREC	r26
#define	BO	r27
#define	PLDA_M	r28
#define IS	r29

#define Y1	CO

#if defined(PPCG4)
#define PREFETCHSIZE_A  42
#define PREFETCHSIZE_C  16
#endif

#if defined(PPC440) || defined(PPC440FP2)
#define PREFETCHSIZE_A  42
#define PREFETCHSIZE_C  16
#endif

#ifdef PPC970
#define PREFETCHSIZE_A  42
#define PREFETCHSIZE_C  16
#endif

#ifdef CELL
#define PREFETCHSIZE_A  42
#define PREFETCHSIZE_C  16
#endif

#ifdef POWER4
#define PREFETCHSIZE_A  48
#define PREFETCHSIZE_C  16
#endif

#ifdef POWER5
#define PREFETCHSIZE_A  40
#define PREFETCHSIZE_C   8
#endif

#ifdef POWER6
#define PREFETCHSIZE_A  96
#define PREFETCHSIZE_C   8
#endif

#ifdef POWER8
#define PREFETCHSIZE_A  96
#define PREFETCHSIZE_C   8
#endif

#define y01 f0
#define y02 f1
#define y03 f2
#define y04 f3
#define y05 f4
#define y06 f5
#define y07 f6
#define y08 f7
#define y09 f8
#define y10 f9
#define y11 f10
#define y12 f11
#define y13 f12
#define y14 f13
#define y15 f14
#define y16 f15

#define a1	f16
#define a2	f17
#define a3	f18
#define a4	f19
#define a5	f20
#define a6	f21
#define a7	f22
#define a8	f23

#define b1	f24
#define b2	f25
#define b3	f26
#define b4	f27
#define b5	f28
#define b6	f29
#define b7	f30
#define b8	f31

#define alpha	f31

#ifndef NEEDPARAM

#define P 2048

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
	stfd	f24,    80(SP)
	stfd	f25,    88(SP)
	stfd	f26,    96(SP)
	stfd	f27,   104(SP)
	stfd	f28,   112(SP)
	stfd	f29,   120(SP)
	stfd	f30,   128(SP)
	stfd	f31,   136(SP)

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
	std	r23,   232(SP)
	std	r24,   240(SP)
	std	r25,   248(SP)
	std	r26,   256(SP)
	std	r27,   264(SP)
	std	r28,   272(SP)
	std	r29,   280(SP)
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
	stw	r23,   196(SP)
	stw	r24,   200(SP)
	stw	r25,   204(SP)
	stw	r26,   208(SP)
	stw	r27,   212(SP)
	stw	r28,   216(SP)
	stw	r29,   220(SP)
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

	mullw	PLDA_M, LDA, N
	li	XP,  P
	subf	PLDA_M, XP, PLDA_M
	slwi	PLDA_M, PLDA_M, BASE_SHIFT

	slwi	LDA,  LDA,  BASE_SHIFT
	slwi	INCX, INCX, BASE_SHIFT
	slwi	INCY, INCY, BASE_SHIFT

	subf	Y, INCY, Y

	li	IS,  0

	addi	A, A, -SIZE

	li	PREA, PREFETCHSIZE_A * SIZE
	li	PREC, PREFETCHSIZE_C * SIZE

	cmpi	cr0, 0, M, 0
	ble	LL(999)

	cmpi	cr0, 0, N, 0
	ble	LL(999)
	.align 4

LL(ISLoop):
	subf	MIN_N, IS, M
	slwi	r0, IS, BASE_SHIFT
	cmpi	cr0, 0, MIN_N, P
	ble+	LL(min_nP)
	li	MIN_N, P
LL(min_nP):
	add	XP, X,  r0
	cmpi	cr0, 0, INCX, SIZE
	beq	LL(10)

	mr	XP, BUFFER
	addi	CO, BUFFER, -SIZE

	srawi.	r0, MIN_N, 3
	mtspr	CTR, r0
	ble	LL(CopyRemain)
	.align 4

LL(CopyKernel):
	LFD	f0, 0 * SIZE(X)
	add	X, X, INCX
	LFD	f1, 0 * SIZE(X)
	add	X, X, INCX
	LFD	f2, 0 * SIZE(X)
	add	X, X, INCX
	LFD	f3, 0 * SIZE(X)
	add	X, X, INCX
	LFD	f4, 0 * SIZE(X)
	add	X, X, INCX
	LFD	f5, 0 * SIZE(X)
	add	X, X, INCX
	LFD	f6, 0 * SIZE(X)
	add	X, X, INCX
	LFD	f7, 0 * SIZE(X)
	add	X, X, INCX

	STFD	f0,  1 * SIZE(CO)
	STFD	f1,  2 * SIZE(CO)
	STFD	f2,  3 * SIZE(CO)
	STFD	f3,  4 * SIZE(CO)
	STFD	f4,  5 * SIZE(CO)
	STFD	f5,  6 * SIZE(CO)
	STFD	f6,  7 * SIZE(CO)
	STFDU	f7,  8 * SIZE(CO)
	bdnz	LL(CopyKernel)
	.align 4

LL(CopyRemain):
	andi.	r0, MIN_N, 7
	mtspr	CTR, r0
	ble	LL(10)
	.align 4

LL(CopySub):
	LFD	f0, 0 * SIZE(X)
	add	X, X, INCX
	STFDU	f0,  1 * SIZE(CO)
	bdnz	LL(CopySub)
	.align 4

LL(10):
	mr	CO, Y
	addi	XP, XP, -SIZE
	srawi.	J, N, 3
	ble	LL(20)
	.align 4

LL(11):
	mr     AO1, A
	add    AO2, A,   LDA
	add    AO3, AO2, LDA
	add    AO4, AO3, LDA
	add    AO5, AO4, LDA
	add    AO6, AO5, LDA
	add    AO7, AO6, LDA
	add    AO8, AO7, LDA
	add    A,   AO8, LDA

	mr     BO, XP

	lfd	 y01, FZERO
	fmr	 y02, y01
	fmr	 y03, y01
	fmr	 y04, y01
	fmr	 y05, y01
	fmr	 y06, y01
	fmr	 y07, y01
	fmr	 y08, y01
	fmr	 y09, y01
	fmr	 y10, y01
	fmr	 y11, y01
	fmr	 y12, y01
	fmr	 y13, y01
	fmr	 y14, y01
	fmr	 y15, y01
	fmr	 y16, y01

	DCBT(Y1, PREC)

	srawi.	r0,  MIN_N, 4
	mtspr	CTR, r0
	ble	LL(14)

	LFD	a1,  1 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO2)
	LFD	a3,  1 * SIZE(AO3)
	LFD	a4,  1 * SIZE(AO4)
	LFD	a5,  1 * SIZE(AO5)
	LFD	a6,  1 * SIZE(AO6)
	LFD	a7,  1 * SIZE(AO7)
	LFD	a8,  1 * SIZE(AO8)

	LFD	b1,  1 * SIZE(BO)
	LFD	b2,  2 * SIZE(BO)
	LFD	b3,  3 * SIZE(BO)
	LFD	b4,  4 * SIZE(BO)
	LFD	b5,  5 * SIZE(BO)
	LFD	b6,  6 * SIZE(BO)
	LFD	b7,  7 * SIZE(BO)
	LFD	b8,  8 * SIZE(BO)
	bdz	LL(13)
	.align 4

LL(12):
	FMADD	y01, a1, b1, y01
	LFD	a1,  2 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFD	a2,  2 * SIZE(AO2)

	FMADD	y03, a3, b1, y03
	LFD	a3,  2 * SIZE(AO3)
	FMADD	y04, a4, b1, y04
	LFD	a4,  2 * SIZE(AO4)

	FMADD	y05, a5, b1, y05
	LFD	a5,  2 * SIZE(AO5)
	FMADD	y06, a6, b1, y06
	LFD	a6,  2 * SIZE(AO6)

	FMADD	y07, a7, b1, y07
	LFD	a7,  2 * SIZE(AO7)
	FMADD	y08, a8, b1, y08
	LFD	a8,  2 * SIZE(AO8)

	FMADD	y09, a1, b2, y09
	LFD	a1,  3 * SIZE(AO1)
	FMADD	y10, a2, b2, y10
	LFD	a2,  3 * SIZE(AO2)

	FMADD	y11, a3, b2, y11
	LFD	a3,  3 * SIZE(AO3)
	FMADD	y12, a4, b2, y12
	LFD	a4,  3 * SIZE(AO4)

	FMADD	y13, a5, b2, y13
	LFD	a5,  3 * SIZE(AO5)
	FMADD	y14, a6, b2, y14
	LFD	a6,  3 * SIZE(AO6)

	FMADD	y15, a7, b2, y15
	LFD	a7,  3 * SIZE(AO7)
	FMADD	y16, a8, b2, y16
	LFD	a8,  3 * SIZE(AO8)

	FMADD	y01, a1, b3, y01
	LFD	a1,  4 * SIZE(AO1)
	FMADD	y02, a2, b3, y02
	LFD	a2,  4 * SIZE(AO2)

	FMADD	y03, a3, b3, y03
	LFD	a3,  4 * SIZE(AO3)
	FMADD	y04, a4, b3, y04
	LFD	a4,  4 * SIZE(AO4)

	FMADD	y05, a5, b3, y05
	LFD	a5,  4 * SIZE(AO5)
	FMADD	y06, a6, b3, y06
	LFD	a6,  4 * SIZE(AO6)

	FMADD	y07, a7, b3, y07
	LFD	a7,  4 * SIZE(AO7)
	FMADD	y08, a8, b3, y08
	LFD	a8,  4 * SIZE(AO8)

	FMADD	y09, a1, b4, y09
	LFD	a1,  5 * SIZE(AO1)
	FMADD	y10, a2, b4, y10
	LFD	a2,  5 * SIZE(AO2)

	FMADD	y11, a3, b4, y11
	LFD	a3,  5 * SIZE(AO3)
	FMADD	y12, a4, b4, y12
	LFD	a4,  5 * SIZE(AO4)

	FMADD	y13, a5, b4, y13
	LFD	a5,  5 * SIZE(AO5)
	FMADD	y14, a6, b4, y14
	LFD	a6,  5 * SIZE(AO6)

	FMADD	y15, a7, b4, y15
	LFD	a7,  5 * SIZE(AO7)
	FMADD	y16, a8, b4, y16
	LFD	a8,  5 * SIZE(AO8)

	LFD	b1,  9 * SIZE(BO)
	LFD	b2, 10 * SIZE(BO)
	LFD	b3, 11 * SIZE(BO)
	LFD	b4, 12 * SIZE(BO)

	FMADD	y01, a1, b5, y01
	LFD	a1,  6 * SIZE(AO1)
	FMADD	y02, a2, b5, y02
	LFD	a2,  6 * SIZE(AO2)

	FMADD	y03, a3, b5, y03
	LFD	a3,  6 * SIZE(AO3)
	FMADD	y04, a4, b5, y04
	LFD	a4,  6 * SIZE(AO4)

	FMADD	y05, a5, b5, y05
	LFD	a5,  6 * SIZE(AO5)
	FMADD	y06, a6, b5, y06
	LFD	a6,  6 * SIZE(AO6)

	FMADD	y07, a7, b5, y07
	LFD	a7,  6 * SIZE(AO7)
	FMADD	y08, a8, b5, y08
	LFD	a8,  6 * SIZE(AO8)

	FMADD	y09, a1, b6, y09
	LFD	a1,  7 * SIZE(AO1)
	FMADD	y10, a2, b6, y10
	LFD	a2,  7 * SIZE(AO2)

	FMADD	y11, a3, b6, y11
	LFD	a3,  7 * SIZE(AO3)
	FMADD	y12, a4, b6, y12
	LFD	a4,  7 * SIZE(AO4)

	FMADD	y13, a5, b6, y13
	LFD	a5,  7 * SIZE(AO5)
	FMADD	y14, a6, b6, y14
	LFD	a6,  7 * SIZE(AO6)

	FMADD	y15, a7, b6, y15
	LFD	a7,  7 * SIZE(AO7)
	FMADD	y16, a8, b6, y16
	LFD	a8,  7 * SIZE(AO8)

	FMADD	y01, a1, b7, y01
	LFD	a1,  8 * SIZE(AO1)
	FMADD	y02, a2, b7, y02
	LFD	a2,  8 * SIZE(AO2)

	FMADD	y03, a3, b7, y03
	LFD	a3,  8 * SIZE(AO3)
	FMADD	y04, a4, b7, y04
	LFD	a4,  8 * SIZE(AO4)

	FMADD	y05, a5, b7, y05
	LFD	a5,  8 * SIZE(AO5)
	FMADD	y06, a6, b7, y06
	LFD	a6,  8 * SIZE(AO6)

	FMADD	y07, a7, b7, y07
	LFD	a7,  8 * SIZE(AO7)
	FMADD	y08, a8, b7, y08
	LFD	a8,  8 * SIZE(AO8)

	FMADD	y09, a1, b8, y09
	LFD	a1,  9 * SIZE(AO1)
	FMADD	y10, a2, b8, y10
	LFD	a2,  9 * SIZE(AO2)

	FMADD	y11, a3, b8, y11
	LFD	a3,  9 * SIZE(AO3)
	FMADD	y12, a4, b8, y12
	LFD	a4,  9 * SIZE(AO4)

	FMADD	y13, a5, b8, y13
	LFD	a5,  9 * SIZE(AO5)
	FMADD	y14, a6, b8, y14
	LFD	a6,  9 * SIZE(AO6)

	FMADD	y15, a7, b8, y15
	LFD	a7,  9 * SIZE(AO7)
	FMADD	y16, a8, b8, y16
	LFD	a8,  9 * SIZE(AO8)

	LFD	b5, 13 * SIZE(BO)
	LFD	b6, 14 * SIZE(BO)
	LFD	b7, 15 * SIZE(BO)
	LFD	b8, 16 * SIZE(BO)

	DCBT(AO1, PREA)
	DCBT(AO2, PREA)
	DCBT(AO3, PREA)
	DCBT(AO4, PREA)

	FMADD	y01, a1, b1, y01
	LFD	a1, 10 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFD	a2, 10 * SIZE(AO2)

	FMADD	y03, a3, b1, y03
	LFD	a3, 10 * SIZE(AO3)
	FMADD	y04, a4, b1, y04
	LFD	a4, 10 * SIZE(AO4)

	FMADD	y05, a5, b1, y05
	LFD	a5, 10 * SIZE(AO5)
	FMADD	y06, a6, b1, y06
	LFD	a6, 10 * SIZE(AO6)

	FMADD	y07, a7, b1, y07
	LFD	a7, 10 * SIZE(AO7)
	FMADD	y08, a8, b1, y08
	LFD	a8, 10 * SIZE(AO8)

	FMADD	y09, a1, b2, y09
	LFD	a1, 11 * SIZE(AO1)
	FMADD	y10, a2, b2, y10
	LFD	a2, 11 * SIZE(AO2)

	FMADD	y11, a3, b2, y11
	LFD	a3, 11 * SIZE(AO3)
	FMADD	y12, a4, b2, y12
	LFD	a4, 11 * SIZE(AO4)

	FMADD	y13, a5, b2, y13
	LFD	a5, 11 * SIZE(AO5)
	FMADD	y14, a6, b2, y14
	LFD	a6, 11 * SIZE(AO6)

	FMADD	y15, a7, b2, y15
	LFD	a7, 11 * SIZE(AO7)
	FMADD	y16, a8, b2, y16
	LFD	a8, 11 * SIZE(AO8)

	FMADD	y01, a1, b3, y01
	LFD	a1, 12 * SIZE(AO1)
	FMADD	y02, a2, b3, y02
	LFD	a2, 12 * SIZE(AO2)

	FMADD	y03, a3, b3, y03
	LFD	a3, 12 * SIZE(AO3)
	FMADD	y04, a4, b3, y04
	LFD	a4, 12 * SIZE(AO4)

	FMADD	y05, a5, b3, y05
	LFD	a5, 12 * SIZE(AO5)
	FMADD	y06, a6, b3, y06
	LFD	a6, 12 * SIZE(AO6)

	FMADD	y07, a7, b3, y07
	LFD	a7, 12 * SIZE(AO7)
	FMADD	y08, a8, b3, y08
	LFD	a8, 12 * SIZE(AO8)

	FMADD	y09, a1, b4, y09
	LFD	a1, 13 * SIZE(AO1)
	FMADD	y10, a2, b4, y10
	LFD	a2, 13 * SIZE(AO2)

	FMADD	y11, a3, b4, y11
	LFD	a3, 13 * SIZE(AO3)
	FMADD	y12, a4, b4, y12
	LFD	a4, 13 * SIZE(AO4)

	FMADD	y13, a5, b4, y13
	LFD	a5, 13 * SIZE(AO5)
	FMADD	y14, a6, b4, y14
	LFD	a6, 13 * SIZE(AO6)

	FMADD	y15, a7, b4, y15
	LFD	a7, 13 * SIZE(AO7)
	FMADD	y16, a8, b4, y16
	LFD	a8, 13 * SIZE(AO8)

	LFD	b1, 17 * SIZE(BO)
	LFD	b2, 18 * SIZE(BO)
	LFD	b3, 19 * SIZE(BO)
	LFD	b4, 20 * SIZE(BO)

	FMADD	y01, a1, b5, y01
	LFD	a1, 14 * SIZE(AO1)
	FMADD	y02, a2, b5, y02
	LFD	a2, 14 * SIZE(AO2)

	FMADD	y03, a3, b5, y03
	LFD	a3, 14 * SIZE(AO3)
	FMADD	y04, a4, b5, y04
	LFD	a4, 14 * SIZE(AO4)

	FMADD	y05, a5, b5, y05
	LFD	a5, 14 * SIZE(AO5)
	FMADD	y06, a6, b5, y06
	LFD	a6, 14 * SIZE(AO6)

	FMADD	y07, a7, b5, y07
	LFD	a7, 14 * SIZE(AO7)
	FMADD	y08, a8, b5, y08
	LFD	a8, 14 * SIZE(AO8)

	FMADD	y09, a1, b6, y09
	LFD	a1, 15 * SIZE(AO1)
	FMADD	y10, a2, b6, y10
	LFD	a2, 15 * SIZE(AO2)

	FMADD	y11, a3, b6, y11
	LFD	a3, 15 * SIZE(AO3)
	FMADD	y12, a4, b6, y12
	LFD	a4, 15 * SIZE(AO4)

	FMADD	y13, a5, b6, y13
	LFD	a5, 15 * SIZE(AO5)
	FMADD	y14, a6, b6, y14
	LFD	a6, 15 * SIZE(AO6)

	FMADD	y15, a7, b6, y15
	LFD	a7, 15 * SIZE(AO7)
	FMADD	y16, a8, b6, y16
	LFD	a8, 15 * SIZE(AO8)

	FMADD	y01, a1, b7, y01
	LFD	a1, 16 * SIZE(AO1)
	FMADD	y02, a2, b7, y02
	LFD	a2, 16 * SIZE(AO2)

	FMADD	y03, a3, b7, y03
	LFD	a3, 16 * SIZE(AO3)
	FMADD	y04, a4, b7, y04
	LFD	a4, 16 * SIZE(AO4)

	FMADD	y05, a5, b7, y05
	LFD	a5, 16 * SIZE(AO5)
	FMADD	y06, a6, b7, y06
	LFD	a6, 16 * SIZE(AO6)

	FMADD	y07, a7, b7, y07
	LFD	a7, 16 * SIZE(AO7)
	FMADD	y08, a8, b7, y08
	LFD	a8, 16 * SIZE(AO8)

	FMADD	y09, a1, b8, y09
	LFD	a1, 17 * SIZE(AO1)
	FMADD	y10, a2, b8, y10
	LFD	a2, 17 * SIZE(AO2)

	FMADD	y11, a3, b8, y11
	LFD	a3, 17 * SIZE(AO3)
	FMADD	y12, a4, b8, y12
	LFD	a4, 17 * SIZE(AO4)

	addi	AO1, AO1, 16 * SIZE
	addi	AO2, AO2, 16 * SIZE
	addi	AO3, AO3, 16 * SIZE
	addi	AO4, AO4, 16 * SIZE

	FMADD	y13, a5, b8, y13
	LFD	a5, 17 * SIZE(AO5)
	FMADD	y14, a6, b8, y14
	LFD	a6, 17 * SIZE(AO6)

	FMADD	y15, a7, b8, y15
	LFD	a7, 17 * SIZE(AO7)
	FMADD	y16, a8, b8, y16
	LFD	a8, 17 * SIZE(AO8)

	LFD	b5, 21 * SIZE(BO)
	LFD	b6, 22 * SIZE(BO)
	LFD	b7, 23 * SIZE(BO)
	LFD	b8, 24 * SIZE(BO)

	addi	AO5, AO5, 16 * SIZE
	addi	AO6, AO6, 16 * SIZE
	DCBT(AO5, PREA)
	DCBT(AO6, PREA)

	addi	AO7, AO7, 16 * SIZE
	addi	AO8, AO8, 16 * SIZE
	DCBT(AO7, PREA)
	DCBT(AO8, PREA)

	addi	BO,  BO,  16 * SIZE
 	bdnz	LL(12)
	.align 4

LL(13):
	FMADD	y01, a1, b1, y01
	LFD	a1,  2 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFD	a2,  2 * SIZE(AO2)

	FMADD	y03, a3, b1, y03
	LFD	a3,  2 * SIZE(AO3)
	FMADD	y04, a4, b1, y04
	LFD	a4,  2 * SIZE(AO4)

	FMADD	y05, a5, b1, y05
	LFD	a5,  2 * SIZE(AO5)
	FMADD	y06, a6, b1, y06
	LFD	a6,  2 * SIZE(AO6)

	FMADD	y07, a7, b1, y07
	LFD	a7,  2 * SIZE(AO7)
	FMADD	y08, a8, b1, y08
	LFD	a8,  2 * SIZE(AO8)

	FMADD	y09, a1, b2, y09
	LFD	a1,  3 * SIZE(AO1)
	FMADD	y10, a2, b2, y10
	LFD	a2,  3 * SIZE(AO2)

	FMADD	y11, a3, b2, y11
	LFD	a3,  3 * SIZE(AO3)
	FMADD	y12, a4, b2, y12
	LFD	a4,  3 * SIZE(AO4)

	FMADD	y13, a5, b2, y13
	LFD	a5,  3 * SIZE(AO5)
	FMADD	y14, a6, b2, y14
	LFD	a6,  3 * SIZE(AO6)

	FMADD	y15, a7, b2, y15
	LFD	a7,  3 * SIZE(AO7)
	FMADD	y16, a8, b2, y16
	LFD	a8,  3 * SIZE(AO8)

	FMADD	y01, a1, b3, y01
	LFD	a1,  4 * SIZE(AO1)
	FMADD	y02, a2, b3, y02
	LFD	a2,  4 * SIZE(AO2)

	FMADD	y03, a3, b3, y03
	LFD	a3,  4 * SIZE(AO3)
	FMADD	y04, a4, b3, y04
	LFD	a4,  4 * SIZE(AO4)

	FMADD	y05, a5, b3, y05
	LFD	a5,  4 * SIZE(AO5)
	FMADD	y06, a6, b3, y06
	LFD	a6,  4 * SIZE(AO6)

	FMADD	y07, a7, b3, y07
	LFD	a7,  4 * SIZE(AO7)
	FMADD	y08, a8, b3, y08
	LFD	a8,  4 * SIZE(AO8)

	FMADD	y09, a1, b4, y09
	LFD	a1,  5 * SIZE(AO1)
	FMADD	y10, a2, b4, y10
	LFD	a2,  5 * SIZE(AO2)

	FMADD	y11, a3, b4, y11
	LFD	a3,  5 * SIZE(AO3)
	FMADD	y12, a4, b4, y12
	LFD	a4,  5 * SIZE(AO4)

	FMADD	y13, a5, b4, y13
	LFD	a5,  5 * SIZE(AO5)
	FMADD	y14, a6, b4, y14
	LFD	a6,  5 * SIZE(AO6)

	FMADD	y15, a7, b4, y15
	LFD	a7,  5 * SIZE(AO7)
	FMADD	y16, a8, b4, y16
	LFD	a8,  5 * SIZE(AO8)

	LFD	b1,  9 * SIZE(BO)
	LFD	b2, 10 * SIZE(BO)
	LFD	b3, 11 * SIZE(BO)
	LFD	b4, 12 * SIZE(BO)

	FMADD	y01, a1, b5, y01
	LFD	a1,  6 * SIZE(AO1)
	FMADD	y02, a2, b5, y02
	LFD	a2,  6 * SIZE(AO2)

	FMADD	y03, a3, b5, y03
	LFD	a3,  6 * SIZE(AO3)
	FMADD	y04, a4, b5, y04
	LFD	a4,  6 * SIZE(AO4)

	FMADD	y05, a5, b5, y05
	LFD	a5,  6 * SIZE(AO5)
	FMADD	y06, a6, b5, y06
	LFD	a6,  6 * SIZE(AO6)

	FMADD	y07, a7, b5, y07
	LFD	a7,  6 * SIZE(AO7)
	FMADD	y08, a8, b5, y08
	LFD	a8,  6 * SIZE(AO8)

	FMADD	y09, a1, b6, y09
	LFD	a1,  7 * SIZE(AO1)
	FMADD	y10, a2, b6, y10
	LFD	a2,  7 * SIZE(AO2)

	FMADD	y11, a3, b6, y11
	LFD	a3,  7 * SIZE(AO3)
	FMADD	y12, a4, b6, y12
	LFD	a4,  7 * SIZE(AO4)

	FMADD	y13, a5, b6, y13
	LFD	a5,  7 * SIZE(AO5)
	FMADD	y14, a6, b6, y14
	LFD	a6,  7 * SIZE(AO6)

	FMADD	y15, a7, b6, y15
	LFD	a7,  7 * SIZE(AO7)
	FMADD	y16, a8, b6, y16
	LFD	a8,  7 * SIZE(AO8)

	FMADD	y01, a1, b7, y01
	LFD	a1,  8 * SIZE(AO1)
	FMADD	y02, a2, b7, y02
	LFD	a2,  8 * SIZE(AO2)

	FMADD	y03, a3, b7, y03
	LFD	a3,  8 * SIZE(AO3)
	FMADD	y04, a4, b7, y04
	LFD	a4,  8 * SIZE(AO4)

	FMADD	y05, a5, b7, y05
	LFD	a5,  8 * SIZE(AO5)
	FMADD	y06, a6, b7, y06
	LFD	a6,  8 * SIZE(AO6)

	FMADD	y07, a7, b7, y07
	LFD	a7,  8 * SIZE(AO7)
	FMADD	y08, a8, b7, y08
	LFD	a8,  8 * SIZE(AO8)

	FMADD	y09, a1, b8, y09
	LFD	a1,  9 * SIZE(AO1)
	FMADD	y10, a2, b8, y10
	LFD	a2,  9 * SIZE(AO2)

	FMADD	y11, a3, b8, y11
	LFD	a3,  9 * SIZE(AO3)
	FMADD	y12, a4, b8, y12
	LFD	a4,  9 * SIZE(AO4)

	FMADD	y13, a5, b8, y13
	LFD	a5,  9 * SIZE(AO5)
	FMADD	y14, a6, b8, y14
	LFD	a6,  9 * SIZE(AO6)

	FMADD	y15, a7, b8, y15
	LFD	a7,  9 * SIZE(AO7)
	FMADD	y16, a8, b8, y16
	LFD	a8,  9 * SIZE(AO8)

	LFD	b5, 13 * SIZE(BO)
	LFD	b6, 14 * SIZE(BO)
	LFD	b7, 15 * SIZE(BO)
	LFD	b8, 16 * SIZE(BO)

	FMADD	y01, a1, b1, y01
	LFD	a1, 10 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFD	a2, 10 * SIZE(AO2)

	FMADD	y03, a3, b1, y03
	LFD	a3, 10 * SIZE(AO3)
	FMADD	y04, a4, b1, y04
	LFD	a4, 10 * SIZE(AO4)

	FMADD	y05, a5, b1, y05
	LFD	a5, 10 * SIZE(AO5)
	FMADD	y06, a6, b1, y06
	LFD	a6, 10 * SIZE(AO6)

	FMADD	y07, a7, b1, y07
	LFD	a7, 10 * SIZE(AO7)
	FMADD	y08, a8, b1, y08
	LFD	a8, 10 * SIZE(AO8)

	FMADD	y09, a1, b2, y09
	LFD	a1, 11 * SIZE(AO1)
	FMADD	y10, a2, b2, y10
	LFD	a2, 11 * SIZE(AO2)

	FMADD	y11, a3, b2, y11
	LFD	a3, 11 * SIZE(AO3)
	FMADD	y12, a4, b2, y12
	LFD	a4, 11 * SIZE(AO4)

	FMADD	y13, a5, b2, y13
	LFD	a5, 11 * SIZE(AO5)
	FMADD	y14, a6, b2, y14
	LFD	a6, 11 * SIZE(AO6)

	FMADD	y15, a7, b2, y15
	LFD	a7, 11 * SIZE(AO7)
	FMADD	y16, a8, b2, y16
	LFD	a8, 11 * SIZE(AO8)

	FMADD	y01, a1, b3, y01
	LFD	a1, 12 * SIZE(AO1)
	FMADD	y02, a2, b3, y02
	LFD	a2, 12 * SIZE(AO2)

	FMADD	y03, a3, b3, y03
	LFD	a3, 12 * SIZE(AO3)
	FMADD	y04, a4, b3, y04
	LFD	a4, 12 * SIZE(AO4)

	FMADD	y05, a5, b3, y05
	LFD	a5, 12 * SIZE(AO5)
	FMADD	y06, a6, b3, y06
	LFD	a6, 12 * SIZE(AO6)

	FMADD	y07, a7, b3, y07
	LFD	a7, 12 * SIZE(AO7)
	FMADD	y08, a8, b3, y08
	LFD	a8, 12 * SIZE(AO8)

	FMADD	y09, a1, b4, y09
	LFD	a1, 13 * SIZE(AO1)
	FMADD	y10, a2, b4, y10
	LFD	a2, 13 * SIZE(AO2)

	FMADD	y11, a3, b4, y11
	LFD	a3, 13 * SIZE(AO3)
	FMADD	y12, a4, b4, y12
	LFD	a4, 13 * SIZE(AO4)

	FMADD	y13, a5, b4, y13
	LFD	a5, 13 * SIZE(AO5)
	FMADD	y14, a6, b4, y14
	LFD	a6, 13 * SIZE(AO6)

	FMADD	y15, a7, b4, y15
	LFD	a7, 13 * SIZE(AO7)
	FMADD	y16, a8, b4, y16
	LFD	a8, 13 * SIZE(AO8)

	FMADD	y01, a1, b5, y01
	LFD	a1, 14 * SIZE(AO1)
	FMADD	y02, a2, b5, y02
	LFD	a2, 14 * SIZE(AO2)

	FMADD	y03, a3, b5, y03
	LFD	a3, 14 * SIZE(AO3)
	FMADD	y04, a4, b5, y04
	LFD	a4, 14 * SIZE(AO4)

	FMADD	y05, a5, b5, y05
	LFD	a5, 14 * SIZE(AO5)
	FMADD	y06, a6, b5, y06
	LFD	a6, 14 * SIZE(AO6)

	FMADD	y07, a7, b5, y07
	LFD	a7, 14 * SIZE(AO7)
	FMADD	y08, a8, b5, y08
	LFD	a8, 14 * SIZE(AO8)

	FMADD	y09, a1, b6, y09
	LFD	a1, 15 * SIZE(AO1)
	FMADD	y10, a2, b6, y10
	LFD	a2, 15 * SIZE(AO2)

	FMADD	y11, a3, b6, y11
	LFD	a3, 15 * SIZE(AO3)
	FMADD	y12, a4, b6, y12
	LFD	a4, 15 * SIZE(AO4)

	FMADD	y13, a5, b6, y13
	LFD	a5, 15 * SIZE(AO5)
	FMADD	y14, a6, b6, y14
	LFD	a6, 15 * SIZE(AO6)

	FMADD	y15, a7, b6, y15
	LFD	a7, 15 * SIZE(AO7)
	FMADD	y16, a8, b6, y16
	LFD	a8, 15 * SIZE(AO8)

	FMADD	y01, a1, b7, y01
	LFD	a1, 16 * SIZE(AO1)
	FMADD	y02, a2, b7, y02
	LFD	a2, 16 * SIZE(AO2)

	FMADD	y03, a3, b7, y03
	LFD	a3, 16 * SIZE(AO3)
	FMADD	y04, a4, b7, y04
	LFD	a4, 16 * SIZE(AO4)

	FMADD	y05, a5, b7, y05
	LFD	a5, 16 * SIZE(AO5)
	FMADD	y06, a6, b7, y06
	LFD	a6, 16 * SIZE(AO6)

	FMADD	y07, a7, b7, y07
	LFD	a7, 16 * SIZE(AO7)
	FMADD	y08, a8, b7, y08
	LFD	a8, 16 * SIZE(AO8)

	FMADD	y09, a1, b8, y09
	FMADD	y10, a2, b8, y10
	FMADD	y11, a3, b8, y11
	FMADD	y12, a4, b8, y12

	addi	AO1, AO1, 16 * SIZE
	addi	AO2, AO2, 16 * SIZE
	addi	AO3, AO3, 16 * SIZE
	addi	AO4, AO4, 16 * SIZE

	FMADD	y13, a5, b8, y13
	FMADD	y14, a6, b8, y14
	FMADD	y15, a7, b8, y15
	FMADD	y16, a8, b8, y16

	addi	AO5, AO5, 16 * SIZE
	addi	AO6, AO6, 16 * SIZE
	addi	AO7, AO7, 16 * SIZE
	addi	AO8, AO8, 16 * SIZE
	addi	BO,  BO,  16 * SIZE
	.align 4

LL(14):
	andi.	r0, MIN_N, 15
	ble	LL(18)

	andi.	r0, MIN_N,  8
	ble	LL(15)

	LFD	a1,  1 * SIZE(AO1)
	LFD	b1,  1 * SIZE(BO)
	LFD	a2,  1 * SIZE(AO2)
	LFD	a3,  1 * SIZE(AO3)
	LFD	a4,  1 * SIZE(AO4)
	LFD	a5,  1 * SIZE(AO5)
	LFD	a6,  1 * SIZE(AO6)
	LFD	a7,  1 * SIZE(AO7)
	LFD	a8,  1 * SIZE(AO8)

	LFD	b2,  2 * SIZE(BO)
	LFD	b3,  3 * SIZE(BO)
	LFD	b4,  4 * SIZE(BO)

	FMADD	y01, a1, b1, y01
	LFD	a1,  2 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFD	a2,  2 * SIZE(AO2)
	FMADD	y03, a3, b1, y03
	LFD	a3,  2 * SIZE(AO3)
	FMADD	y04, a4, b1, y04
	LFD	a4,  2 * SIZE(AO4)
	FMADD	y05, a5, b1, y05
	LFD	a5,  2 * SIZE(AO5)
	FMADD	y06, a6, b1, y06
	LFD	a6,  2 * SIZE(AO6)
	FMADD	y07, a7, b1, y07
	LFD	a7,  2 * SIZE(AO7)
	FMADD	y08, a8, b1, y08
	LFD	a8,  2 * SIZE(AO8)

	FMADD	y09, a1, b2, y09
	LFD	a1,  3 * SIZE(AO1)
	FMADD	y10, a2, b2, y10
	LFD	a2,  3 * SIZE(AO2)
	FMADD	y11, a3, b2, y11
	LFD	a3,  3 * SIZE(AO3)
	FMADD	y12, a4, b2, y12
	LFD	a4,  3 * SIZE(AO4)
	FMADD	y13, a5, b2, y13
	LFD	a5,  3 * SIZE(AO5)
	FMADD	y14, a6, b2, y14
	LFD	a6,  3 * SIZE(AO6)
	FMADD	y15, a7, b2, y15
	LFD	a7,  3 * SIZE(AO7)
	FMADD	y16, a8, b2, y16
	LFD	a8,  3 * SIZE(AO8)

	LFD	b5,  5 * SIZE(BO)
	LFD	b6,  6 * SIZE(BO)
	LFD	b7,  7 * SIZE(BO)
	LFD	b8,  8 * SIZE(BO)

	FMADD	y01, a1, b3, y01
	LFD	a1,  4 * SIZE(AO1)
	FMADD	y02, a2, b3, y02
	LFD	a2,  4 * SIZE(AO2)
	FMADD	y03, a3, b3, y03
	LFD	a3,  4 * SIZE(AO3)
	FMADD	y04, a4, b3, y04
	LFD	a4,  4 * SIZE(AO4)
	FMADD	y05, a5, b3, y05
	LFD	a5,  4 * SIZE(AO5)
	FMADD	y06, a6, b3, y06
	LFD	a6,  4 * SIZE(AO6)
	FMADD	y07, a7, b3, y07
	LFD	a7,  4 * SIZE(AO7)
	FMADD	y08, a8, b3, y08
	LFD	a8,  4 * SIZE(AO8)

	FMADD	y09, a1, b4, y09
	LFD	a1,  5 * SIZE(AO1)
	FMADD	y10, a2, b4, y10
	LFD	a2,  5 * SIZE(AO2)
	FMADD	y11, a3, b4, y11
	LFD	a3,  5 * SIZE(AO3)
	FMADD	y12, a4, b4, y12
	LFD	a4,  5 * SIZE(AO4)
	FMADD	y13, a5, b4, y13
	LFD	a5,  5 * SIZE(AO5)
	FMADD	y14, a6, b4, y14
	LFD	a6,  5 * SIZE(AO6)
	FMADD	y15, a7, b4, y15
	LFD	a7,  5 * SIZE(AO7)
	FMADD	y16, a8, b4, y16
	LFD	a8,  5 * SIZE(AO8)

	FMADD	y01, a1, b5, y01
	LFD	a1,  6 * SIZE(AO1)
	FMADD	y02, a2, b5, y02
	LFD	a2,  6 * SIZE(AO2)
	FMADD	y03, a3, b5, y03
	LFD	a3,  6 * SIZE(AO3)
	FMADD	y04, a4, b5, y04
	LFD	a4,  6 * SIZE(AO4)
	FMADD	y05, a5, b5, y05
	LFD	a5,  6 * SIZE(AO5)
	FMADD	y06, a6, b5, y06
	LFD	a6,  6 * SIZE(AO6)
	FMADD	y07, a7, b5, y07
	LFD	a7,  6 * SIZE(AO7)
	FMADD	y08, a8, b5, y08
	LFD	a8,  6 * SIZE(AO8)

	FMADD	y09, a1, b6, y09
	LFD	a1,  7 * SIZE(AO1)
	FMADD	y10, a2, b6, y10
	LFD	a2,  7 * SIZE(AO2)
	FMADD	y11, a3, b6, y11
	LFD	a3,  7 * SIZE(AO3)
	FMADD	y12, a4, b6, y12
	LFD	a4,  7 * SIZE(AO4)
	FMADD	y13, a5, b6, y13
	LFD	a5,  7 * SIZE(AO5)
	FMADD	y14, a6, b6, y14
	LFD	a6,  7 * SIZE(AO6)
	FMADD	y15, a7, b6, y15
	LFD	a7,  7 * SIZE(AO7)
	FMADD	y16, a8, b6, y16
	LFD	a8,  7 * SIZE(AO8)

	FMADD	y01, a1, b7, y01
	LFD	a1,  8 * SIZE(AO1)
	FMADD	y02, a2, b7, y02
	LFD	a2,  8 * SIZE(AO2)
	FMADD	y03, a3, b7, y03
	LFD	a3,  8 * SIZE(AO3)
	FMADD	y04, a4, b7, y04
	LFD	a4,  8 * SIZE(AO4)
	FMADD	y05, a5, b7, y05
	LFD	a5,  8 * SIZE(AO5)
	FMADD	y06, a6, b7, y06
	LFD	a6,  8 * SIZE(AO6)
	FMADD	y07, a7, b7, y07
	LFD	a7,  8 * SIZE(AO7)
	FMADD	y08, a8, b7, y08
	LFD	a8,  8 * SIZE(AO8)

	FMADD	y09, a1, b8, y09
	addi	AO1, AO1, 8 * SIZE
	FMADD	y10, a2, b8, y10
	addi	AO2, AO2, 8 * SIZE
	FMADD	y11, a3, b8, y11
	addi	AO3, AO3, 8 * SIZE
	FMADD	y12, a4, b8, y12
	addi	AO4, AO4, 8 * SIZE
	FMADD	y13, a5, b8, y13
	addi	AO5, AO5, 8 * SIZE
	FMADD	y14, a6, b8, y14
	addi	AO6, AO6, 8 * SIZE
	FMADD	y15, a7, b8, y15
	addi	AO7, AO7, 8 * SIZE
	FMADD	y16, a8, b8, y16
	addi	AO8, AO8, 8 * SIZE
	addi	BO,  BO,  8 * SIZE
	.align 4

LL(15):
	andi.	r0, MIN_N,  4
	ble	LL(16)

	LFD	a1,  1 * SIZE(AO1)
	LFD	b1,  1 * SIZE(BO)
	LFD	a2,  1 * SIZE(AO2)
	LFD	a3,  1 * SIZE(AO3)
	LFD	a4,  1 * SIZE(AO4)
	LFD	a5,  1 * SIZE(AO5)
	LFD	a6,  1 * SIZE(AO6)
	LFD	a7,  1 * SIZE(AO7)
	LFD	a8,  1 * SIZE(AO8)

	LFD	b2,  2 * SIZE(BO)
	LFD	b3,  3 * SIZE(BO)
	LFD	b4,  4 * SIZE(BO)

	FMADD	y01, a1, b1, y01
	LFD	a1,  2 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFD	a2,  2 * SIZE(AO2)
	FMADD	y03, a3, b1, y03
	LFD	a3,  2 * SIZE(AO3)
	FMADD	y04, a4, b1, y04
	LFD	a4,  2 * SIZE(AO4)
	FMADD	y05, a5, b1, y05
	LFD	a5,  2 * SIZE(AO5)
	FMADD	y06, a6, b1, y06
	LFD	a6,  2 * SIZE(AO6)
	FMADD	y07, a7, b1, y07
	LFD	a7,  2 * SIZE(AO7)
	FMADD	y08, a8, b1, y08
	LFD	a8,  2 * SIZE(AO8)

	FMADD	y09, a1, b2, y09
	LFD	a1,  3 * SIZE(AO1)
	FMADD	y10, a2, b2, y10
	LFD	a2,  3 * SIZE(AO2)
	FMADD	y11, a3, b2, y11
	LFD	a3,  3 * SIZE(AO3)
	FMADD	y12, a4, b2, y12
	LFD	a4,  3 * SIZE(AO4)
	FMADD	y13, a5, b2, y13
	LFD	a5,  3 * SIZE(AO5)
	FMADD	y14, a6, b2, y14
	LFD	a6,  3 * SIZE(AO6)
	FMADD	y15, a7, b2, y15
	LFD	a7,  3 * SIZE(AO7)
	FMADD	y16, a8, b2, y16
	LFD	a8,  3 * SIZE(AO8)

	FMADD	y01, a1, b3, y01
	LFD	a1,  4 * SIZE(AO1)
	FMADD	y02, a2, b3, y02
	LFD	a2,  4 * SIZE(AO2)
	FMADD	y03, a3, b3, y03
	LFD	a3,  4 * SIZE(AO3)
	FMADD	y04, a4, b3, y04
	LFD	a4,  4 * SIZE(AO4)

	FMADD	y05, a5, b3, y05
	LFD	a5,  4 * SIZE(AO5)
	FMADD	y06, a6, b3, y06
	LFD	a6,  4 * SIZE(AO6)
	FMADD	y07, a7, b3, y07
	LFD	a7,  4 * SIZE(AO7)
	FMADD	y08, a8, b3, y08
	LFD	a8,  4 * SIZE(AO8)

	FMADD	y09, a1, b4, y09
	addi	AO1, AO1, 4 * SIZE
	FMADD	y10, a2, b4, y10
	addi	AO2, AO2, 4 * SIZE
	FMADD	y11, a3, b4, y11
	addi	AO3, AO3, 4 * SIZE
	FMADD	y12, a4, b4, y12
	addi	AO4, AO4, 4 * SIZE
	FMADD	y13, a5, b4, y13
	addi	AO5, AO5, 4 * SIZE
	FMADD	y14, a6, b4, y14
	addi	AO6, AO6, 4 * SIZE
	FMADD	y15, a7, b4, y15
	addi	AO7, AO7, 4 * SIZE
	FMADD	y16, a8, b4, y16
	addi	AO8, AO8, 4 * SIZE
	addi	BO,  BO,  4 * SIZE
	.align 4

LL(16):
	andi.	r0, MIN_N,  2
	ble	LL(17)

	LFD	a1,  1 * SIZE(AO1)
	LFD	b1,  1 * SIZE(BO)
	LFD	a2,  1 * SIZE(AO2)
	LFD	a3,  1 * SIZE(AO3)
	LFD	a4,  1 * SIZE(AO4)
	LFD	a5,  1 * SIZE(AO5)
	LFD	a6,  1 * SIZE(AO6)
	LFD	a7,  1 * SIZE(AO7)
	LFD	a8,  1 * SIZE(AO8)

	LFD	b2,  2 * SIZE(BO)

	FMADD	y01, a1, b1, y01
	LFD	a1,  2 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFD	a2,  2 * SIZE(AO2)
	FMADD	y03, a3, b1, y03
	LFD	a3,  2 * SIZE(AO3)
	FMADD	y04, a4, b1, y04
	LFD	a4,  2 * SIZE(AO4)
	FMADD	y05, a5, b1, y05
	LFD	a5,  2 * SIZE(AO5)
	FMADD	y06, a6, b1, y06
	LFD	a6,  2 * SIZE(AO6)
	FMADD	y07, a7, b1, y07
	LFD	a7,  2 * SIZE(AO7)
	FMADD	y08, a8, b1, y08
	LFD	a8,  2 * SIZE(AO8)

	FMADD	y09, a1, b2, y09
	addi	AO1, AO1, 2 * SIZE
	addi	AO2, AO2, 2 * SIZE
	FMADD	y10, a2, b2, y10
	addi	AO3, AO3, 2 * SIZE
	addi	AO4, AO4, 2 * SIZE
	FMADD	y11, a3, b2, y11
	FMADD	y12, a4, b2, y12
	addi	AO5, AO5, 2 * SIZE
	addi	AO6, AO6, 2 * SIZE
	FMADD	y13, a5, b2, y13
	FMADD	y14, a6, b2, y14
	addi	AO7, AO7, 2 * SIZE
	addi	AO8, AO8, 2 * SIZE
	FMADD	y15, a7, b2, y15
	FMADD	y16, a8, b2, y16
	addi	BO,  BO,  2 * SIZE
	.align 4

LL(17):
	andi.	r0, MIN_N,  1
	ble	LL(18)

	LFD	a1,  1 * SIZE(AO1)
	LFD	b1,  1 * SIZE(BO)
	LFD	a2,  1 * SIZE(AO2)
	LFD	a3,  1 * SIZE(AO3)
	LFD	a4,  1 * SIZE(AO4)
	LFD	a5,  1 * SIZE(AO5)
	LFD	a6,  1 * SIZE(AO6)
	LFD	a7,  1 * SIZE(AO7)
	LFD	a8,  1 * SIZE(AO8)

	FMADD	y01, a1, b1, y01
	FMADD	y02, a2, b1, y02
	FMADD	y03, a3, b1, y03
	FMADD	y04, a4, b1, y04
	FMADD	y05, a5, b1, y05
	FMADD	y06, a6, b1, y06
	FMADD	y07, a7, b1, y07
	FMADD	y08, a8, b1, y08
	.align 4

LL(18):
	mr	BO, CO
	lfd	alpha, ALPHA
	cmpi	cr0, 0, INCY, SIZE
	bne	LL(19)

	LFD	a1,  1 * SIZE(CO)
	LFD	a2,  2 * SIZE(CO)
	LFD	a3,  3 * SIZE(CO)
	LFD	a4,  4 * SIZE(CO)
	LFD	a5,  5 * SIZE(CO)
	LFD	a6,  6 * SIZE(CO)
	LFD	a7,  7 * SIZE(CO)
	LFD	a8,  8 * SIZE(CO)

	FADD	y01, y09, y01
	FADD	y02, y10, y02
	FADD	y03, y11, y03
	FADD	y04, y12, y04
	FADD	y05, y13, y05
	FADD	y06, y14, y06
	FADD	y07, y15, y07
	FADD	y08, y16, y08

	FMADD	a1, alpha, y01, a1
	FMADD	a2, alpha, y02, a2
	FMADD	a3, alpha, y03, a3
	FMADD	a4, alpha, y04, a4
	FMADD	a5, alpha, y05, a5
	FMADD	a6, alpha, y06, a6
	FMADD	a7, alpha, y07, a7
	FMADD	a8, alpha, y08, a8

	STFD	a1,  1 * SIZE(CO)
	STFD	a2,  2 * SIZE(CO)
	STFD	a3,  3 * SIZE(CO)
	STFD	a4,  4 * SIZE(CO)
	STFD	a5,  5 * SIZE(CO)
	STFD	a6,  6 * SIZE(CO)
	STFD	a7,  7 * SIZE(CO)
	STFD	a8,  8 * SIZE(CO)

	addi	J, J, -1
	addi	CO, CO, 8 * SIZE
	cmpi	cr0, 0, J, 0
	bgt	LL(11)
	b	LL(20)
	.align 4

LL(19):
	LFDUX	a1, CO, INCY
	LFDUX	a2, CO, INCY
	LFDUX	a3, CO, INCY
	LFDUX	a4, CO, INCY
	LFDUX	a5, CO, INCY
	LFDUX	a6, CO, INCY
	LFDUX	a7, CO, INCY
	LFDUX	a8, CO, INCY

	FADD	y01, y09, y01
	FADD	y02, y10, y02
	FADD	y03, y11, y03
	FADD	y04, y12, y04
	FADD	y05, y13, y05
	FADD	y06, y14, y06
	FADD	y07, y15, y07
	FADD	y08, y16, y08

	FMADD	a1, alpha, f0, a1
	FMADD	a2, alpha, f1, a2
	FMADD	a3, alpha, f2, a3
	FMADD	a4, alpha, f3, a4
	FMADD	a5, alpha, f4, a5
	FMADD	a6, alpha, f5, a6
	FMADD	a7, alpha, f6, a7
	FMADD	a8, alpha, f7, a8

	STFDUX	a1, BO, INCY
	STFDUX	a2, BO, INCY
	STFDUX	a3, BO, INCY
	STFDUX	a4, BO, INCY
	STFDUX	a5, BO, INCY
	STFDUX	a6, BO, INCY
	STFDUX	a7, BO, INCY
	STFDUX	a8, BO, INCY

	addi	J, J, -1
	cmpi	cr0, 0, J, 0
	bgt	LL(11)
	.align 4

LL(20):
	andi.	J, N, 7
	ble	LL(99)
	andi.	J, N, 4
	ble	LL(30)

	mr     AO1, A
	add    AO2, A,   LDA
	add    AO3, AO2, LDA
	add    AO4, AO3, LDA
	add    A,   AO4, LDA

	mr     BO, XP

	lfd	 y01, FZERO
	fmr	 y02, y01
	fmr	 y03, y01
	fmr	 y04, y01
	fmr	 y09, y01
	fmr	 y10, y01
	fmr	 y11, y01
	fmr	 y12, y01

	DCBT(Y1, PREC)

	srawi.	r0,  MIN_N, 4
	mtspr	CTR, r0
	ble	LL(24)

	LFD	a1,  1 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO2)
	LFD	a3,  1 * SIZE(AO3)
	LFD	a4,  1 * SIZE(AO4)
	LFD	a5,  2 * SIZE(AO1)
	LFD	a6,  2 * SIZE(AO2)
	LFD	a7,  2 * SIZE(AO3)
	LFD	a8,  2 * SIZE(AO4)

	LFD	b1,  1 * SIZE(BO)
	LFD	b2,  2 * SIZE(BO)
	LFD	b3,  3 * SIZE(BO)
	LFD	b4,  4 * SIZE(BO)
	LFD	b5,  5 * SIZE(BO)
	LFD	b6,  6 * SIZE(BO)
	LFD	b7,  7 * SIZE(BO)
	LFD	b8,  8 * SIZE(BO)
	bdz	LL(23)
	.align 4

LL(22):
	FMADD	y01, a1, b1, y01
	LFD	a1,  3 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFD	a2,  3 * SIZE(AO2)
	FMADD	y03, a3, b1, y03
	LFD	a3,  3 * SIZE(AO3)
	FMADD	y04, a4, b1, y04
	LFD	a4,  3 * SIZE(AO4)

	FMADD	y09, a5, b2, y09
	LFD	a5,  4 * SIZE(AO1)
	FMADD	y10, a6, b2, y10
	LFD	a6,  4 * SIZE(AO2)
	FMADD	y11, a7, b2, y11
	LFD	a7,  4 * SIZE(AO3)
	FMADD	y12, a8, b2, y12
	LFD	a8,  4 * SIZE(AO4)

	FMADD	y01, a1, b3, y01
	LFD	a1,  5 * SIZE(AO1)
	FMADD	y02, a2, b3, y02
	LFD	a2,  5 * SIZE(AO2)
	FMADD	y03, a3, b3, y03
	LFD	a3,  5 * SIZE(AO3)
	FMADD	y04, a4, b3, y04
	LFD	a4,  5 * SIZE(AO4)

	FMADD	y09, a5, b4, y09
	LFD	a5,  6 * SIZE(AO1)
	FMADD	y10, a6, b4, y10
	LFD	a6,  6 * SIZE(AO2)
	FMADD	y11, a7, b4, y11
	LFD	a7,  6 * SIZE(AO3)
	FMADD	y12, a8, b4, y12
	LFD	a8,  6 * SIZE(AO4)

	LFD	b1,  9 * SIZE(BO)
	LFD	b2, 10 * SIZE(BO)
	LFD	b3, 11 * SIZE(BO)
	LFD	b4, 12 * SIZE(BO)

	FMADD	y01, a1, b5, y01
	LFD	a1,  7 * SIZE(AO1)
	FMADD	y02, a2, b5, y02
	LFD	a2,  7 * SIZE(AO2)
	FMADD	y03, a3, b5, y03
	LFD	a3,  7 * SIZE(AO3)
	FMADD	y04, a4, b5, y04
	LFD	a4,  7 * SIZE(AO4)

	FMADD	y09, a5, b6, y09
	LFD	a5,  8 * SIZE(AO1)
	FMADD	y10, a6, b6, y10
	LFD	a6,  8 * SIZE(AO2)
	FMADD	y11, a7, b6, y11
	LFD	a7,  8 * SIZE(AO3)
	FMADD	y12, a8, b6, y12
	LFD	a8,  8 * SIZE(AO4)

	FMADD	y01, a1, b7, y01
	LFD	a1,  9 * SIZE(AO1)
	FMADD	y02, a2, b7, y02
	LFD	a2,  9 * SIZE(AO2)
	FMADD	y03, a3, b7, y03
	LFD	a3,  9 * SIZE(AO3)
	FMADD	y04, a4, b7, y04
	LFD	a4,  9 * SIZE(AO4)

	FMADD	y09, a5, b8, y09
	LFD	a5, 10 * SIZE(AO1)
	FMADD	y10, a6, b8, y10
	LFD	a6, 10 * SIZE(AO2)
	FMADD	y11, a7, b8, y11
	LFD	a7, 10 * SIZE(AO3)
	FMADD	y12, a8, b8, y12
	LFD	a8, 10 * SIZE(AO4)

	LFD	b5, 13 * SIZE(BO)
	LFD	b6, 14 * SIZE(BO)
	LFD	b7, 15 * SIZE(BO)
	LFD	b8, 16 * SIZE(BO)

	FMADD	y01, a1, b1, y01
	LFD	a1, 11 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFD	a2, 11 * SIZE(AO2)
	FMADD	y03, a3, b1, y03
	LFD	a3, 11 * SIZE(AO3)
	FMADD	y04, a4, b1, y04
	LFD	a4, 11 * SIZE(AO4)

	FMADD	y09, a5, b2, y09
	LFD	a5, 12 * SIZE(AO1)
	FMADD	y10, a6, b2, y10
	LFD	a6, 12 * SIZE(AO2)
	FMADD	y11, a7, b2, y11
	LFD	a7, 12 * SIZE(AO3)
	FMADD	y12, a8, b2, y12
	LFD	a8, 12 * SIZE(AO4)

	FMADD	y01, a1, b3, y01
	LFD	a1, 13 * SIZE(AO1)
	FMADD	y02, a2, b3, y02
	LFD	a2, 13 * SIZE(AO2)
	FMADD	y03, a3, b3, y03
	LFD	a3, 13 * SIZE(AO3)
	FMADD	y04, a4, b3, y04
	LFD	a4, 13 * SIZE(AO4)

	FMADD	y09, a5, b4, y09
	LFD	a5, 14 * SIZE(AO1)
	FMADD	y10, a6, b4, y10
	LFD	a6, 14 * SIZE(AO2)
	FMADD	y11, a7, b4, y11
	LFD	a7, 14 * SIZE(AO3)
	FMADD	y12, a8, b4, y12
	LFD	a8, 14 * SIZE(AO4)

	LFD	b1, 17 * SIZE(BO)
	LFD	b2, 18 * SIZE(BO)
	LFD	b3, 19 * SIZE(BO)
	LFD	b4, 20 * SIZE(BO)

	FMADD	y01, a1, b5, y01
	LFD	a1, 15 * SIZE(AO1)
	FMADD	y02, a2, b5, y02
	LFD	a2, 15 * SIZE(AO2)
	FMADD	y03, a3, b5, y03
	LFD	a3, 15 * SIZE(AO3)
	FMADD	y04, a4, b5, y04
	LFD	a4, 15 * SIZE(AO4)

	FMADD	y09, a5, b6, y09
	LFD	a5, 16 * SIZE(AO1)
	FMADD	y10, a6, b6, y10
	LFD	a6, 16 * SIZE(AO2)
	FMADD	y11, a7, b6, y11
	LFD	a7, 16 * SIZE(AO3)
	FMADD	y12, a8, b6, y12
	LFD	a8, 16 * SIZE(AO4)

	FMADD	y01, a1, b7, y01
	LFD	a1, 17 * SIZE(AO1)
	FMADD	y02, a2, b7, y02
	LFD	a2, 17 * SIZE(AO2)
	FMADD	y03, a3, b7, y03
	LFD	a3, 17 * SIZE(AO3)
	FMADD	y04, a4, b7, y04
	LFD	a4, 17 * SIZE(AO4)

	FMADD	y09, a5, b8, y09
	LFD	a5, 18 * SIZE(AO1)
	FMADD	y10, a6, b8, y10
	LFD	a6, 18 * SIZE(AO2)
	FMADD	y11, a7, b8, y11
	LFD	a7, 18 * SIZE(AO3)
	FMADD	y12, a8, b8, y12
	LFD	a8, 18 * SIZE(AO4)

	LFD	b5, 21 * SIZE(BO)
	LFD	b6, 22 * SIZE(BO)
	LFD	b7, 23 * SIZE(BO)
	LFD	b8, 24 * SIZE(BO)

	addi	AO1, AO1, 16 * SIZE
	addi	AO2, AO2, 16 * SIZE
	DCBT(AO1, PREA)
	DCBT(AO2, PREA)

	addi	AO3, AO3, 16 * SIZE
	addi	AO4, AO4, 16 * SIZE
	DCBT(AO3, PREA)
	DCBT(AO4, PREA)

	addi	BO,  BO,  16 * SIZE
 	bdnz	LL(22)
	.align 4

LL(23):
	FMADD	y01, a1, b1, y01
	LFD	a1,  3 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFD	a2,  3 * SIZE(AO2)
	FMADD	y03, a3, b1, y03
	LFD	a3,  3 * SIZE(AO3)
	FMADD	y04, a4, b1, y04
	LFD	a4,  3 * SIZE(AO4)

	FMADD	y09, a5, b2, y09
	LFD	a5,  4 * SIZE(AO1)
	FMADD	y10, a6, b2, y10
	LFD	a6,  4 * SIZE(AO2)
	FMADD	y11, a7, b2, y11
	LFD	a7,  4 * SIZE(AO3)
	FMADD	y12, a8, b2, y12
	LFD	a8,  4 * SIZE(AO4)

	FMADD	y01, a1, b3, y01
	LFD	a1,  5 * SIZE(AO1)
	FMADD	y02, a2, b3, y02
	LFD	a2,  5 * SIZE(AO2)
	FMADD	y03, a3, b3, y03
	LFD	a3,  5 * SIZE(AO3)
	FMADD	y04, a4, b3, y04
	LFD	a4,  5 * SIZE(AO4)

	FMADD	y09, a5, b4, y09
	LFD	a5,  6 * SIZE(AO1)
	FMADD	y10, a6, b4, y10
	LFD	a6,  6 * SIZE(AO2)
	FMADD	y11, a7, b4, y11
	LFD	a7,  6 * SIZE(AO3)
	FMADD	y12, a8, b4, y12
	LFD	a8,  6 * SIZE(AO4)

	LFD	b1,  9 * SIZE(BO)
	LFD	b2, 10 * SIZE(BO)
	LFD	b3, 11 * SIZE(BO)
	LFD	b4, 12 * SIZE(BO)

	FMADD	y01, a1, b5, y01
	LFD	a1,  7 * SIZE(AO1)
	FMADD	y02, a2, b5, y02
	LFD	a2,  7 * SIZE(AO2)
	FMADD	y03, a3, b5, y03
	LFD	a3,  7 * SIZE(AO3)
	FMADD	y04, a4, b5, y04
	LFD	a4,  7 * SIZE(AO4)

	FMADD	y09, a5, b6, y09
	LFD	a5,  8 * SIZE(AO1)
	FMADD	y10, a6, b6, y10
	LFD	a6,  8 * SIZE(AO2)
	FMADD	y11, a7, b6, y11
	LFD	a7,  8 * SIZE(AO3)
	FMADD	y12, a8, b6, y12
	LFD	a8,  8 * SIZE(AO4)

	FMADD	y01, a1, b7, y01
	LFD	a1,  9 * SIZE(AO1)
	FMADD	y02, a2, b7, y02
	LFD	a2,  9 * SIZE(AO2)
	FMADD	y03, a3, b7, y03
	LFD	a3,  9 * SIZE(AO3)
	FMADD	y04, a4, b7, y04
	LFD	a4,  9 * SIZE(AO4)

	FMADD	y09, a5, b8, y09
	LFD	a5, 10 * SIZE(AO1)
	FMADD	y10, a6, b8, y10
	LFD	a6, 10 * SIZE(AO2)
	FMADD	y11, a7, b8, y11
	LFD	a7, 10 * SIZE(AO3)
	FMADD	y12, a8, b8, y12
	LFD	a8, 10 * SIZE(AO4)

	LFD	b5, 13 * SIZE(BO)
	LFD	b6, 14 * SIZE(BO)
	LFD	b7, 15 * SIZE(BO)
	LFD	b8, 16 * SIZE(BO)

	FMADD	y01, a1, b1, y01
	LFD	a1, 11 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFD	a2, 11 * SIZE(AO2)
	FMADD	y03, a3, b1, y03
	LFD	a3, 11 * SIZE(AO3)
	FMADD	y04, a4, b1, y04
	LFD	a4, 11 * SIZE(AO4)

	FMADD	y09, a5, b2, y09
	LFD	a5, 12 * SIZE(AO1)
	FMADD	y10, a6, b2, y10
	LFD	a6, 12 * SIZE(AO2)
	FMADD	y11, a7, b2, y11
	LFD	a7, 12 * SIZE(AO3)
	FMADD	y12, a8, b2, y12
	LFD	a8, 12 * SIZE(AO4)

	FMADD	y01, a1, b3, y01
	LFD	a1, 13 * SIZE(AO1)
	FMADD	y02, a2, b3, y02
	LFD	a2, 13 * SIZE(AO2)
	FMADD	y03, a3, b3, y03
	LFD	a3, 13 * SIZE(AO3)
	FMADD	y04, a4, b3, y04
	LFD	a4, 13 * SIZE(AO4)

	FMADD	y09, a5, b4, y09
	LFD	a5, 14 * SIZE(AO1)
	FMADD	y10, a6, b4, y10
	LFD	a6, 14 * SIZE(AO2)
	FMADD	y11, a7, b4, y11
	LFD	a7, 14 * SIZE(AO3)
	FMADD	y12, a8, b4, y12
	LFD	a8, 14 * SIZE(AO4)

	FMADD	y01, a1, b5, y01
	LFD	a1, 15 * SIZE(AO1)
	FMADD	y02, a2, b5, y02
	LFD	a2, 15 * SIZE(AO2)
	FMADD	y03, a3, b5, y03
	LFD	a3, 15 * SIZE(AO3)
	FMADD	y04, a4, b5, y04
	LFD	a4, 15 * SIZE(AO4)

	FMADD	y09, a5, b6, y09
	LFD	a5, 16 * SIZE(AO1)
	FMADD	y10, a6, b6, y10
	LFD	a6, 16 * SIZE(AO2)
	FMADD	y11, a7, b6, y11
	LFD	a7, 16 * SIZE(AO3)
	FMADD	y12, a8, b6, y12
	LFD	a8, 16 * SIZE(AO4)

	FMADD	y01, a1, b7, y01
	FMADD	y02, a2, b7, y02
	FMADD	y03, a3, b7, y03
	FMADD	y04, a4, b7, y04

	FMADD	y09, a5, b8, y09
	FMADD	y10, a6, b8, y10
	FMADD	y11, a7, b8, y11
	FMADD	y12, a8, b8, y12

	addi	AO1, AO1, 16 * SIZE
	addi	AO2, AO2, 16 * SIZE
	addi	AO3, AO3, 16 * SIZE
	addi	AO4, AO4, 16 * SIZE
	addi	BO,  BO,  16 * SIZE
	.align 4

LL(24):
	andi.	r0, MIN_N, 15
	ble	LL(28)

	andi.	r0, MIN_N,  8
	ble	LL(25)

	LFD	a1,  1 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO2)
	LFD	a3,  1 * SIZE(AO3)
	LFD	a4,  1 * SIZE(AO4)

	LFD	b1,  1 * SIZE(BO)
	LFD	b2,  2 * SIZE(BO)
	LFD	b3,  3 * SIZE(BO)
	LFD	b4,  4 * SIZE(BO)

	LFD	a5,  2 * SIZE(AO1)
	LFD	a6,  2 * SIZE(AO2)
	LFD	a7,  2 * SIZE(AO3)
	LFD	a8,  2 * SIZE(AO4)

	FMADD	y01, a1, b1, y01
	LFD	a1,  3 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFD	a2,  3 * SIZE(AO2)
	FMADD	y03, a3, b1, y03
	LFD	a3,  3 * SIZE(AO3)
	FMADD	y04, a4, b1, y04
	LFD	a4,  3 * SIZE(AO4)

	FMADD	y09, a5, b2, y09
	LFD	a5,  4 * SIZE(AO1)
	FMADD	y10, a6, b2, y10
	LFD	a6,  4 * SIZE(AO2)
	FMADD	y11, a7, b2, y11
	LFD	a7,  4 * SIZE(AO3)
	FMADD	y12, a8, b2, y12
	LFD	a8,  4 * SIZE(AO4)

	FMADD	y01, a1, b3, y01
	LFD	a1,  5 * SIZE(AO1)
	FMADD	y02, a2, b3, y02
	LFD	a2,  5 * SIZE(AO2)
	FMADD	y03, a3, b3, y03
	LFD	a3,  5 * SIZE(AO3)
	FMADD	y04, a4, b3, y04
	LFD	a4,  5 * SIZE(AO4)

	FMADD	y09, a5, b4, y09
	LFD	a5,  6 * SIZE(AO1)
	FMADD	y10, a6, b4, y10
	LFD	a6,  6 * SIZE(AO2)
	FMADD	y11, a7, b4, y11
	LFD	a7,  6 * SIZE(AO3)
	FMADD	y12, a8, b4, y12
	LFD	a8,  6 * SIZE(AO4)

	LFD	b1,  5 * SIZE(BO)
	LFD	b2,  6 * SIZE(BO)
	LFD	b3,  7 * SIZE(BO)
	LFD	b4,  8 * SIZE(BO)

	FMADD	y01, a1, b1, y01
	LFD	a1,  7 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFD	a2,  7 * SIZE(AO2)
	FMADD	y03, a3, b1, y03
	LFD	a3,  7 * SIZE(AO3)
	FMADD	y04, a4, b1, y04
	LFD	a4,  7 * SIZE(AO4)

	FMADD	y09, a5, b2, y09
	LFD	a5,  8 * SIZE(AO1)
	FMADD	y10, a6, b2, y10
	LFD	a6,  8 * SIZE(AO2)
	FMADD	y11, a7, b2, y11
	LFD	a7,  8 * SIZE(AO3)
	FMADD	y12, a8, b2, y12
	LFD	a8,  8 * SIZE(AO4)

	FMADD	y01, a1, b3, y01
	FMADD	y02, a2, b3, y02
	FMADD	y03, a3, b3, y03
	FMADD	y04, a4, b3, y04

	FMADD	y09, a5, b4, y09
	addi	AO1, AO1, 8 * SIZE
	FMADD	y10, a6, b4, y10
	addi	AO2, AO2, 8 * SIZE
	FMADD	y11, a7, b4, y11
	addi	AO3, AO3, 8 * SIZE
	FMADD	y12, a8, b4, y12
	addi	AO4, AO4, 8 * SIZE

	addi	BO,  BO,  8 * SIZE
	.align 4

LL(25):
	andi.	r0, MIN_N,  4
	ble	LL(26)

	LFD	a1,  1 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO2)
	LFD	a3,  1 * SIZE(AO3)
	LFD	a4,  1 * SIZE(AO4)

	LFD	b1,  1 * SIZE(BO)
	LFD	b2,  2 * SIZE(BO)
	LFD	b3,  3 * SIZE(BO)
	LFD	b4,  4 * SIZE(BO)

	LFD	a5,  2 * SIZE(AO1)
	LFD	a6,  2 * SIZE(AO2)
	LFD	a7,  2 * SIZE(AO3)
	LFD	a8,  2 * SIZE(AO4)

	FMADD	y01, a1, b1, y01
	LFD	a1,  3 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFD	a2,  3 * SIZE(AO2)
	FMADD	y03, a3, b1, y03
	LFD	a3,  3 * SIZE(AO3)
	FMADD	y04, a4, b1, y04
	LFD	a4,  3 * SIZE(AO4)

	FMADD	y09, a5, b2, y09
	LFD	a5,  4 * SIZE(AO1)
	FMADD	y10, a6, b2, y10
	LFD	a6,  4 * SIZE(AO2)
	FMADD	y11, a7, b2, y11
	LFD	a7,  4 * SIZE(AO3)
	FMADD	y12, a8, b2, y12
	LFD	a8,  4 * SIZE(AO4)

	FMADD	y01, a1, b3, y01
	FMADD	y02, a2, b3, y02
	FMADD	y03, a3, b3, y03
	FMADD	y04, a4, b3, y04

	FMADD	y09, a5, b4, y09
	addi	AO1, AO1, 4 * SIZE
	FMADD	y10, a6, b4, y10
	addi	AO2, AO2, 4 * SIZE
	FMADD	y11, a7, b4, y11
	addi	AO3, AO3, 4 * SIZE
	FMADD	y12, a8, b4, y12
	addi	AO4, AO4, 4 * SIZE
	addi	BO,  BO,  4 * SIZE
	.align 4

LL(26):
	andi.	r0, MIN_N,  2
	ble	LL(27)

	LFD	a1,  1 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO2)
	LFD	b1,  1 * SIZE(BO)
	LFD	b2,  2 * SIZE(BO)

	LFD	a3,  1 * SIZE(AO3)
	LFD	a4,  1 * SIZE(AO4)

	LFD	a5,  2 * SIZE(AO1)
	LFD	a6,  2 * SIZE(AO2)
	LFD	a7,  2 * SIZE(AO3)
	LFD	a8,  2 * SIZE(AO4)

	FMADD	y01, a1, b1, y01
	FMADD	y02, a2, b1, y02
	FMADD	y03, a3, b1, y03
	FMADD	y04, a4, b1, y04

	FMADD	y09, a5, b2, y09
	addi	AO1, AO1, 2 * SIZE
	FMADD	y10, a6, b2, y10
	addi	AO2, AO2, 2 * SIZE
	FMADD	y11, a7, b2, y11
	addi	AO3, AO3, 2 * SIZE
	FMADD	y12, a8, b2, y12
	addi	AO4, AO4, 2 * SIZE
	addi	BO,  BO,  2 * SIZE
	.align 4

LL(27):
	andi.	r0, MIN_N,  1
	ble	LL(28)

	LFD	a1,  1 * SIZE(AO1)
	LFD	b1,  1 * SIZE(BO)

	LFD	a2,  1 * SIZE(AO2)
	LFD	a3,  1 * SIZE(AO3)
	LFD	a4,  1 * SIZE(AO4)

	FMADD	y01, a1, b1, y01
	FMADD	y02, a2, b1, y02
	FMADD	y03, a3, b1, y03
	FMADD	y04, a4, b1, y04
	.align 4

LL(28):
	mr	BO, CO
	lfd	alpha, ALPHA
	cmpi	cr0, 0, INCY, SIZE
	bne	LL(29)

	LFD	a1,  1 * SIZE(CO)
	LFD	a2,  2 * SIZE(CO)
	LFD	a3,  3 * SIZE(CO)
	LFD	a4,  4 * SIZE(CO)

	FADD	y01, y09, y01
	FADD	y02, y10, y02
	FADD	y03, y11, y03
	FADD	y04, y12, y04

	FMADD	a1, alpha, y01, a1
	FMADD	a2, alpha, y02, a2
	FMADD	a3, alpha, y03, a3
	FMADD	a4, alpha, y04, a4

	STFD	a1,  1 * SIZE(CO)
	STFD	a2,  2 * SIZE(CO)
	STFD	a3,  3 * SIZE(CO)
	STFD	a4,  4 * SIZE(CO)

	addi	CO, CO, 4 * SIZE
	b	LL(30)
	.align 4

LL(29):
	LFDUX	a1, CO, INCY
	LFDUX	a2, CO, INCY
	LFDUX	a3, CO, INCY
	LFDUX	a4, CO, INCY

	FADD	y01, y09, y01
	FADD	y02, y10, y02
	FADD	y03, y11, y03
	FADD	y04, y12, y04

	FMADD	a1, alpha, f0, a1
	FMADD	a2, alpha, f1, a2
	FMADD	a3, alpha, f2, a3
	FMADD	a4, alpha, f3, a4

	STFDUX	a1, BO, INCY
	STFDUX	a2, BO, INCY
	STFDUX	a3, BO, INCY
	STFDUX	a4, BO, INCY
	.align 4

LL(30):
	andi.	J, N, 2
	ble	LL(40)

	mr     AO1, A
	add    AO2, A,   LDA
	add    A,   AO2, LDA

	mr     BO, XP

	lfd	 y01, FZERO
	fmr	 y02, y01
	fmr	 y03, y01
	fmr	 y04, y01
	fmr	 y09, y01
	fmr	 y10, y01
	fmr	 y11, y01
	fmr	 y12, y01

	DCBT(Y1, PREC)

	srawi.	r0,  MIN_N, 4
	mtspr	CTR, r0
	ble	LL(34)

	LFD	a1,  1 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO2)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  2 * SIZE(AO2)
	LFD	a5,  3 * SIZE(AO1)
	LFD	a6,  3 * SIZE(AO2)
	LFD	a7,  4 * SIZE(AO1)
	LFD	a8,  4 * SIZE(AO2)

	LFD	b1,  1 * SIZE(BO)
	LFD	b2,  2 * SIZE(BO)
	LFD	b3,  3 * SIZE(BO)
	LFD	b4,  4 * SIZE(BO)
	LFD	b5,  5 * SIZE(BO)
	LFD	b6,  6 * SIZE(BO)
	LFD	b7,  7 * SIZE(BO)
	LFD	b8,  8 * SIZE(BO)
	bdz	LL(33)
	.align 4

LL(32):
	FMADD	y01, a1, b1, y01
	LFD	a1,  5 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFD	a2,  5 * SIZE(AO2)
	FMADD	y03, a3, b2, y03
	LFD	a3,  6 * SIZE(AO1)
	FMADD	y04, a4, b2, y04
	LFD	a4,  6 * SIZE(AO2)

	FMADD	y09, a5, b3, y09
	LFD	a5,  7 * SIZE(AO1)
	FMADD	y10, a6, b3, y10
	LFD	a6,  7 * SIZE(AO2)
	FMADD	y11, a7, b4, y11
	LFD	a7,  8 * SIZE(AO1)
	FMADD	y12, a8, b4, y12
	LFD	a8,  8 * SIZE(AO2)

	LFD	b1,  9 * SIZE(BO)
	LFD	b2, 10 * SIZE(BO)
	LFD	b3, 11 * SIZE(BO)
	LFD	b4, 12 * SIZE(BO)

	FMADD	y01, a1, b5, y01
	LFD	a1,  9 * SIZE(AO1)
	FMADD	y02, a2, b5, y02
	LFD	a2,  9 * SIZE(AO2)
	FMADD	y03, a3, b6, y03
	LFD	a3, 10 * SIZE(AO1)
	FMADD	y04, a4, b6, y04
	LFD	a4, 10 * SIZE(AO2)

	FMADD	y09, a5, b7, y09
	LFD	a5, 11 * SIZE(AO1)
	FMADD	y10, a6, b7, y10
	LFD	a6, 11 * SIZE(AO2)
	FMADD	y11, a7, b8, y11
	LFD	a7, 12 * SIZE(AO1)
	FMADD	y12, a8, b8, y12
	LFD	a8, 12 * SIZE(AO2)

	LFD	b5, 13 * SIZE(BO)
	LFD	b6, 14 * SIZE(BO)
	LFD	b7, 15 * SIZE(BO)
	LFD	b8, 16 * SIZE(BO)

	FMADD	y01, a1, b1, y01
	LFD	a1, 13 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFD	a2, 13 * SIZE(AO2)
	FMADD	y03, a3, b2, y03
	LFD	a3, 14 * SIZE(AO1)
	FMADD	y04, a4, b2, y04
	LFD	a4, 14 * SIZE(AO2)

	FMADD	y09, a5, b3, y09
	LFD	a5, 15 * SIZE(AO1)
	FMADD	y10, a6, b3, y10
	LFD	a6, 15 * SIZE(AO2)
	FMADD	y11, a7, b4, y11
	LFD	a7, 16 * SIZE(AO1)
	FMADD	y12, a8, b4, y12
	LFD	a8, 16 * SIZE(AO2)

	LFD	b1, 17 * SIZE(BO)
	LFD	b2, 18 * SIZE(BO)
	LFD	b3, 19 * SIZE(BO)
	LFD	b4, 20 * SIZE(BO)

	FMADD	y01, a1, b5, y01
	LFD	a1, 17 * SIZE(AO1)
	FMADD	y02, a2, b5, y02
	LFD	a2, 17 * SIZE(AO2)
	FMADD	y03, a3, b6, y03
	LFD	a3, 18 * SIZE(AO1)
	FMADD	y04, a4, b6, y04
	LFD	a4, 18 * SIZE(AO2)

	FMADD	y09, a5, b7, y09
	LFD	a5, 19 * SIZE(AO1)
	FMADD	y10, a6, b7, y10
	LFD	a6, 19 * SIZE(AO2)
	FMADD	y11, a7, b8, y11
	LFD	a7, 20 * SIZE(AO1)
	FMADD	y12, a8, b8, y12
	LFD	a8, 20 * SIZE(AO2)

	LFD	b5, 21 * SIZE(BO)
	LFD	b6, 22 * SIZE(BO)
	LFD	b7, 23 * SIZE(BO)
	LFD	b8, 24 * SIZE(BO)

	addi	AO1, AO1, 16 * SIZE
	addi	AO2, AO2, 16 * SIZE
	DCBT(AO1, PREA)
	DCBT(AO2, PREA)

	addi	BO,  BO,  16 * SIZE
 	bdnz	LL(32)
	.align 4

LL(33):
	FMADD	y01, a1, b1, y01
	LFD	a1,  5 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFD	a2,  5 * SIZE(AO2)
	FMADD	y03, a3, b2, y03
	LFD	a3,  6 * SIZE(AO1)
	FMADD	y04, a4, b2, y04
	LFD	a4,  6 * SIZE(AO2)

	FMADD	y09, a5, b3, y09
	LFD	a5,  7 * SIZE(AO1)
	FMADD	y10, a6, b3, y10
	LFD	a6,  7 * SIZE(AO2)
	FMADD	y11, a7, b4, y11
	LFD	a7,  8 * SIZE(AO1)
	FMADD	y12, a8, b4, y12
	LFD	a8,  8 * SIZE(AO2)

	LFD	b1,  9 * SIZE(BO)
	LFD	b2, 10 * SIZE(BO)
	LFD	b3, 11 * SIZE(BO)
	LFD	b4, 12 * SIZE(BO)

	FMADD	y01, a1, b5, y01
	LFD	a1,  9 * SIZE(AO1)
	FMADD	y02, a2, b5, y02
	LFD	a2,  9 * SIZE(AO2)
	FMADD	y03, a3, b6, y03
	LFD	a3, 10 * SIZE(AO1)
	FMADD	y04, a4, b6, y04
	LFD	a4, 10 * SIZE(AO2)

	FMADD	y09, a5, b7, y09
	LFD	a5, 11 * SIZE(AO1)
	FMADD	y10, a6, b7, y10
	LFD	a6, 11 * SIZE(AO2)
	FMADD	y11, a7, b8, y11
	LFD	a7, 12 * SIZE(AO1)
	FMADD	y12, a8, b8, y12
	LFD	a8, 12 * SIZE(AO2)

	LFD	b5, 13 * SIZE(BO)
	LFD	b6, 14 * SIZE(BO)
	LFD	b7, 15 * SIZE(BO)
	LFD	b8, 16 * SIZE(BO)

	FMADD	y01, a1, b1, y01
	LFD	a1, 13 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFD	a2, 13 * SIZE(AO2)
	FMADD	y03, a3, b2, y03
	LFD	a3, 14 * SIZE(AO1)
	FMADD	y04, a4, b2, y04
	LFD	a4, 14 * SIZE(AO2)

	FMADD	y09, a5, b3, y09
	LFD	a5, 15 * SIZE(AO1)
	FMADD	y10, a6, b3, y10
	LFD	a6, 15 * SIZE(AO2)
	FMADD	y11, a7, b4, y11
	LFD	a7, 16 * SIZE(AO1)
	FMADD	y12, a8, b4, y12
	LFD	a8, 16 * SIZE(AO2)

	FMADD	y01, a1, b5, y01
	FMADD	y02, a2, b5, y02
	FMADD	y03, a3, b6, y03
	FMADD	y04, a4, b6, y04

	FMADD	y09, a5, b7, y09
	FMADD	y10, a6, b7, y10
	FMADD	y11, a7, b8, y11
	FMADD	y12, a8, b8, y12

	addi	AO1, AO1, 16 * SIZE
	addi	AO2, AO2, 16 * SIZE
	addi	BO,  BO,  16 * SIZE
	.align 4

LL(34):
	andi.	r0, MIN_N, 15
	ble	LL(38)
	andi.	r0, MIN_N,  8
	ble	LL(35)

	LFD	a1,  1 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO2)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  2 * SIZE(AO2)

	LFD	b1,  1 * SIZE(BO)
	LFD	b2,  2 * SIZE(BO)
	LFD	b3,  3 * SIZE(BO)
	LFD	b4,  4 * SIZE(BO)

	LFD	a5,  3 * SIZE(AO1)
	LFD	a6,  3 * SIZE(AO2)
	LFD	a7,  4 * SIZE(AO1)
	LFD	a8,  4 * SIZE(AO2)

	LFD	b5,  5 * SIZE(BO)
	LFD	b6,  6 * SIZE(BO)
	LFD	b7,  7 * SIZE(BO)
	LFD	b8,  8 * SIZE(BO)

	FMADD	y01, a1, b1, y01
	LFD	a1,  5 * SIZE(AO1)
	FMADD	y02, a2, b1, y02
	LFD	a2,  5 * SIZE(AO2)
	FMADD	y09, a3, b2, y09
	LFD	a3,  6 * SIZE(AO1)
	FMADD	y10, a4, b2, y10
	LFD	a4,  6 * SIZE(AO2)

	FMADD	y01, a5, b3, y01
	LFD	a5,  7 * SIZE(AO1)
	FMADD	y02, a6, b3, y02
	LFD	a6,  7 * SIZE(AO2)
	FMADD	y09, a7, b4, y09
	LFD	a7,  8 * SIZE(AO1)
	FMADD	y10, a8, b4, y10
	LFD	a8,  8 * SIZE(AO2)

	FMADD	y01, a1, b5, y01
	FMADD	y02, a2, b5, y02
	FMADD	y09, a3, b6, y09
	FMADD	y10, a4, b6, y10

	FMADD	y01, a5, b7, y01
	addi	AO1, AO1, 8 * SIZE
	FMADD	y02, a6, b7, y02
	addi	AO2, AO2, 8 * SIZE
	FMADD	y09, a7, b8, y09
	addi	BO,  BO,  8 * SIZE
	FMADD	y10, a8, b8, y10
	nop
	.align 4

LL(35):
	andi.	r0, MIN_N,  4
	ble	LL(36)

	LFD	a1,  1 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO2)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  2 * SIZE(AO2)

	LFD	a5,  3 * SIZE(AO1)
	LFD	a6,  3 * SIZE(AO2)
	LFD	a7,  4 * SIZE(AO1)
	LFD	a8,  4 * SIZE(AO2)

	LFD	b1,  1 * SIZE(BO)
	LFD	b2,  2 * SIZE(BO)
	LFD	b3,  3 * SIZE(BO)
	LFD	b4,  4 * SIZE(BO)

	FMADD	y01, a1, b1, y01
	FMADD	y02, a2, b1, y02
	FMADD	y09, a3, b2, y09
	FMADD	y10, a4, b2, y10

	FMADD	y01, a5, b3, y01
	addi	AO1, AO1, 4 * SIZE
	FMADD	y02, a6, b3, y02
	addi	AO2, AO2, 4 * SIZE

	FMADD	y09, a7, b4, y09
	addi	BO,  BO,  4 * SIZE
	FMADD	y10, a8, b4, y10
	.align 4

LL(36):
	andi.	r0, MIN_N,  2
	ble	LL(37)

	LFD	a1,  1 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO2)
	LFD	b1,  1 * SIZE(BO)
	LFD	b2,  2 * SIZE(BO)

	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  2 * SIZE(AO2)

	FMADD	y01, a1, b1, y01
	FMADD	y02, a2, b1, y02
	FMADD	y09, a3, b2, y09
	FMADD	y10, a4, b2, y10

	addi	AO1, AO1, 2 * SIZE
	addi	AO2, AO2, 2 * SIZE
	addi	BO,  BO,  2 * SIZE
	.align 4

LL(37):
	andi.	r0, MIN_N,  1
	ble	LL(38)

	LFD	a1,  1 * SIZE(AO1)
	LFD	b1,  1 * SIZE(BO)
	LFD	a2,  1 * SIZE(AO2)

	FMADD	y01, a1, b1, y01
	FMADD	y02, a2, b1, y02
	.align 4

LL(38):
	mr	BO, CO
	lfd	alpha, ALPHA
	cmpi	cr0, 0, INCY, SIZE
	bne	LL(39)

	LFD	a1,  1 * SIZE(CO)
	LFD	a2,  2 * SIZE(CO)

	FADD	y01, y03, y01
	FADD	y02, y04, y02
	FADD	y09, y11, y09
	FADD	y10, y12, y10

	FADD	y01, y09, y01
	FADD	y02, y10, y02

	FMADD	a1, alpha, y01, a1
	FMADD	a2, alpha, y02, a2

	STFD	a1,  1 * SIZE(CO)
	STFD	a2,  2 * SIZE(CO)

	addi	CO, CO, 2 * SIZE
	b	LL(40)
	.align 4

LL(39):
	LFDUX	a1, CO, INCY
	LFDUX	a2, CO, INCY

	FADD	y01, y03, y01
	FADD	y02, y04, y02
	FADD	y09, y11, y09
	FADD	y10, y12, y10

	FADD	y01, y09, y01
	FADD	y02, y10, y02

	FMADD	a1, alpha, f0, a1
	FMADD	a2, alpha, f1, a2

	STFDUX	a1, BO, INCY
	STFDUX	a2, BO, INCY
	.align 4

LL(40):
	andi.	J, N, 1
	ble	LL(99)

	mr     AO1, A
	add    A,   A, LDA
	mr     BO, XP

	lfd	 y01, FZERO
	fmr	 y02, y01
	fmr	 y03, y01
	fmr	 y04, y01
	fmr	 y09, y01
	fmr	 y10, y01
	fmr	 y11, y01
	fmr	 y12, y01

	DCBT(Y1, PREC)

	srawi.	r0,  MIN_N, 4
	mtspr	CTR, r0
	ble	LL(44)

	LFD	a1,  1 * SIZE(AO1)
	LFD	a2,  2 * SIZE(AO1)
	LFD	a3,  3 * SIZE(AO1)
	LFD	a4,  4 * SIZE(AO1)
	LFD	a5,  5 * SIZE(AO1)
	LFD	a6,  6 * SIZE(AO1)
	LFD	a7,  7 * SIZE(AO1)
	LFD	a8,  8 * SIZE(AO1)

	LFD	b1,  1 * SIZE(BO)
	LFD	b2,  2 * SIZE(BO)
	LFD	b3,  3 * SIZE(BO)
	LFD	b4,  4 * SIZE(BO)
	LFD	b5,  5 * SIZE(BO)
	LFD	b6,  6 * SIZE(BO)
	LFD	b7,  7 * SIZE(BO)
	LFD	b8,  8 * SIZE(BO)
	bdz	LL(43)
	.align 4

LL(42):
	FMADD	y01, a1, b1, y01
	nop
	LFD	a1,  9 * SIZE(AO1)
	LFD	b1,  9 * SIZE(BO)

	FMADD	y02, a2, b2, y02
	nop
	LFD	a2, 10 * SIZE(AO1)
	LFD	b2, 10 * SIZE(BO)

	FMADD	y03, a3, b3, y03
	nop
	LFD	a3, 11 * SIZE(AO1)
	LFD	b3, 11 * SIZE(BO)

	FMADD	y04, a4, b4, y04
	nop
	LFD	a4, 12 * SIZE(AO1)
	LFD	b4, 12 * SIZE(BO)

	FMADD	y01, a5, b5, y01
	nop
	LFD	a5, 13 * SIZE(AO1)
	LFD	b5, 13 * SIZE(BO)

	FMADD	y02, a6, b6, y02
	nop
	LFD	a6, 14 * SIZE(AO1)
	LFD	b6, 14 * SIZE(BO)

	FMADD	y03, a7, b7, y03
	nop
	LFD	a7, 15 * SIZE(AO1)
	LFD	b7, 15 * SIZE(BO)

	FMADD	y04, a8, b8, y04
	nop
	LFD	a8, 16 * SIZE(AO1)
	LFD	b8, 16 * SIZE(BO)

	FMADD	y01, a1, b1, y01
	nop
	LFD	a1, 17 * SIZE(AO1)
	LFD	b1, 17 * SIZE(BO)

	FMADD	y02, a2, b2, y02
	nop
	LFD	a2, 18 * SIZE(AO1)
	LFD	b2, 18 * SIZE(BO)

	FMADD	y03, a3, b3, y03
	nop
	LFD	a3, 19 * SIZE(AO1)
	LFD	b3, 19 * SIZE(BO)

	FMADD	y04, a4, b4, y04
	nop
	LFD	a4, 20 * SIZE(AO1)
	LFD	b4, 20 * SIZE(BO)

	FMADD	y01, a5, b5, y01
	nop
	LFD	a5, 21 * SIZE(AO1)
	LFD	b5, 21 * SIZE(BO)

	FMADD	y02, a6, b6, y02
	nop
	LFD	a6, 22 * SIZE(AO1)
	LFD	b6, 22 * SIZE(BO)

	FMADD	y03, a7, b7, y03
	nop
	LFD	a7, 23 * SIZE(AO1)
	LFD	b7, 23 * SIZE(BO)

	FMADD	y04, a8, b8, y04
	nop
	LFD	a8, 24 * SIZE(AO1)
	LFD	b8, 24 * SIZE(BO)

	addi	AO1, AO1, 16 * SIZE
	addi	BO,  BO,  16 * SIZE
	DCBT(AO1, PREA)
 	bdnz	LL(42)
	.align 4

LL(43):
	FMADD	y01, a1, b1, y01
	nop
	LFD	a1,  9 * SIZE(AO1)
	LFD	b1,  9 * SIZE(BO)

	FMADD	y02, a2, b2, y02
	nop
	LFD	a2, 10 * SIZE(AO1)
	LFD	b2, 10 * SIZE(BO)

	FMADD	y03, a3, b3, y03
	nop
	LFD	a3, 11 * SIZE(AO1)
	LFD	b3, 11 * SIZE(BO)

	FMADD	y04, a4, b4, y04
	nop
	LFD	a4, 12 * SIZE(AO1)
	LFD	b4, 12 * SIZE(BO)

	FMADD	y01, a5, b5, y01
	nop
	LFD	a5, 13 * SIZE(AO1)
	LFD	b5, 13 * SIZE(BO)

	FMADD	y02, a6, b6, y02
	nop
	LFD	a6, 14 * SIZE(AO1)
	LFD	b6, 14 * SIZE(BO)

	FMADD	y03, a7, b7, y03
	nop
	LFD	a7, 15 * SIZE(AO1)
	LFD	b7, 15 * SIZE(BO)

	FMADD	y04, a8, b8, y04
	nop
	LFD	a8, 16 * SIZE(AO1)
	LFD	b8, 16 * SIZE(BO)

	FMADD	y01, a1, b1, y01
	FMADD	y02, a2, b2, y02
	FMADD	y03, a3, b3, y03
	FMADD	y04, a4, b4, y04

	FMADD	y01, a5, b5, y01
	addi	AO1, AO1, 16 * SIZE
	FMADD	y02, a6, b6, y02
	addi	BO,  BO,  16 * SIZE

	FMADD	y03, a7, b7, y03
	nop
	FMADD	y04, a8, b8, y04
	nop
	.align 4

LL(44):
	andi.	r0, MIN_N, 15
	ble	LL(48)
	andi.	r0, MIN_N,  8
	ble	LL(45)

	LFD	a1,  1 * SIZE(AO1)
	LFD	a2,  2 * SIZE(AO1)
	LFD	a3,  3 * SIZE(AO1)
	LFD	a4,  4 * SIZE(AO1)

	LFD	b1,  1 * SIZE(BO)
	LFD	b2,  2 * SIZE(BO)
	LFD	b3,  3 * SIZE(BO)
	LFD	b4,  4 * SIZE(BO)

	LFD	a5,  5 * SIZE(AO1)
	LFD	a6,  6 * SIZE(AO1)
	LFD	a7,  7 * SIZE(AO1)
	LFD	a8,  8 * SIZE(AO1)

	LFD	b5,  5 * SIZE(BO)
	LFD	b6,  6 * SIZE(BO)
	LFD	b7,  7 * SIZE(BO)
	LFD	b8,  8 * SIZE(BO)

	FMADD	y01, a1, b1, y01
	FMADD	y02, a2, b2, y02
	FMADD	y03, a3, b3, y03
	FMADD	y04, a4, b4, y04

	FMADD	y01, a5, b5, y01
	addi	AO1, AO1, 8 * SIZE
	FMADD	y02, a6, b6, y02
	addi	BO,  BO,  8 * SIZE
	FMADD	y03, a7, b7, y03
	nop
	FMADD	y04, a8, b8, y04
	nop
	.align 4

LL(45):
	andi.	r0, MIN_N,  4
	ble	LL(46)

	LFD	a1,  1 * SIZE(AO1)
	LFD	b1,  1 * SIZE(BO)
	LFD	a2,  2 * SIZE(AO1)
	LFD	b2,  2 * SIZE(BO)

	LFD	a3,  3 * SIZE(AO1)
	LFD	b3,  3 * SIZE(BO)
	LFD	a4,  4 * SIZE(AO1)
	LFD	b4,  4 * SIZE(BO)

	FMADD	y01, a1, b1, y01
	addi	AO1, AO1, 4 * SIZE
	FMADD	y02, a2, b2, y02
	addi	AO2, AO2, 4 * SIZE

	FMADD	y03, a3, b3, y03
	addi	BO,  BO,  4 * SIZE
	FMADD	y04, a4, b4, y04
	nop
	.align 4

LL(46):
	andi.	r0, MIN_N,  2
	ble	LL(47)

	LFD	a1,  1 * SIZE(AO1)
	LFD	b1,  1 * SIZE(BO)
	LFD	a2,  2 * SIZE(AO1)
	LFD	b2,  2 * SIZE(BO)

	FMADD	y01, a1, b1, y01
	addi	AO1, AO1, 2 * SIZE
	FMADD	y02, a2, b2, y02
	addi	BO,  BO,  2 * SIZE
	.align 4

LL(47):
	andi.	r0, MIN_N,  1
	ble	LL(48)

	LFD	a1,  1 * SIZE(AO1)
	LFD	b1,  1 * SIZE(BO)
	FMADD	y01, a1, b1, y01
	.align 4

LL(48):
	mr	BO, CO
	lfd	alpha, ALPHA
	cmpi	cr0, 0, INCY, SIZE
	bne	LL(49)

	LFD	a1,  1 * SIZE(CO)

	FADD	y01, y02, y01
	FADD	y03, y04, y03
	FADD	y01, y03, y01

	FMADD	a1, alpha, y01, a1
	STFD	a1,  1 * SIZE(CO)
	b	LL(99)
	.align 4

LL(49):
	LFDUX	a1, CO, INCY
	FADD	y01, y02, y01
	FADD	y03, y04, y03
	FADD	y01, y03, y01
	FMADD	a1, alpha, f0, a1
	STFDUX	a1, BO, INCY
	.align 4

LL(99):
	subf	A, PLDA_M, A
	addi	IS, IS, P
	cmp	cr0, 0, IS, M
	blt	LL(ISLoop)
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
	ld	r14,   160(SP)
	ld	r15,   168(SP)
	ld	r16,   176(SP)
	ld	r17,   184(SP)
	ld	r18,   192(SP)
	ld	r19,   200(SP)
	ld	r20,   208(SP)
	ld	r21,   216(SP)
	ld	r22,   224(SP)
	ld	r23,   232(SP)
	ld	r24,   240(SP)
	ld	r25,   248(SP)
	ld	r26,   256(SP)
	ld	r27,   264(SP)
	ld	r28,   272(SP)
	ld	r29,   280(SP)
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
	lwz	r23,   196(SP)
	lwz	r24,   200(SP)
	lwz	r25,   204(SP)
	lwz	r26,   208(SP)
	lwz	r27,   212(SP)
	lwz	r28,   216(SP)
	lwz	r29,   220(SP)
#endif

	addi	SP, SP, STACKSIZE

	blr

	EPILOGUE

#endif
