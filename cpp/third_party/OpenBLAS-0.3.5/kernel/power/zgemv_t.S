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

#define P 2048

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
#define	MIN_N	r14
#define	J	r15
#define CO	r16
#define	BO	r17
#define	PLDA_M	r18
#define	AO1	r19
#define	AO2	r20
#define	AO3	r21
#define	AO4	r22
#define IS	r23
#define	PREA	r24
#define	PREC	r25

#define	Y1	r23   /* dummy; should be same as gemv_n.S */
#define Y2	r24   /* dummy; should be same as gemv_n.S */

#if defined(PPCG4)
#define PREFETCHSIZE_A  34
#define PREFETCHSIZE_C  16
#endif

#if defined(PPC440) || defined(PPC440FP2)
#define PREFETCHSIZE_A  34
#define PREFETCHSIZE_C  16
#endif

#ifdef PPC970
#define PREFETCHSIZE_A  56
#define PREFETCHSIZE_C  16
#endif

#ifdef CELL
#define PREFETCHSIZE_A  56
#define PREFETCHSIZE_C  16
#endif

#ifdef POWER4
#define PREFETCHSIZE_A  34
#define PREFETCHSIZE_C  16
#endif

#ifdef POWER5
#define PREFETCHSIZE_A  40
#define PREFETCHSIZE_C   8
#endif

#ifdef POWER6
#define PREFETCHSIZE_A  24
#define PREFETCHSIZE_C   8
#endif

#ifdef POWER8
#define PREFETCHSIZE_A  24
#define PREFETCHSIZE_C   8
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
#define ALPHA_R 208(SP)
#define ALPHA_I 216(SP)
#else
#define FZERO	256(SP)
#define ALPHA_R 264(SP)
#define ALPHA_I 272(SP)
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
	std	r23,   216(SP)
	std	r24,   224(SP)
	std	r25,   232(SP)
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
	stw	r23,   180(SP)
	stw	r24,   184(SP)
	stw	r25,   188(SP)
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

	stfd	f1, ALPHA_R
	stfd	f2, ALPHA_I

	mullw	PLDA_M, LDA, N
	li	XP,  P
	subf	PLDA_M, XP, PLDA_M
	slwi	PLDA_M, PLDA_M, ZBASE_SHIFT

	slwi	LDA,  LDA,  ZBASE_SHIFT
	slwi	INCX, INCX, ZBASE_SHIFT
	slwi	INCY, INCY, ZBASE_SHIFT

	li	IS,  0

	li	PREA, PREFETCHSIZE_A * SIZE
	li	PREC, PREFETCHSIZE_C * SIZE

	cmpwi	cr0, M, 0
	ble	LL(End)
	cmpwi	cr0, N, 0
	ble	LL(End)
	.align 4

LL(ISLoop):
	subf	MIN_N, IS, M
	slwi	r0, IS, ZBASE_SHIFT
	cmpi	cr0, 0, MIN_N, P
	ble+	LL(min_nP)
	li	MIN_N, P
LL(min_nP):
	add	XP, X,  r0
	cmpwi	cr0, INCX, 2 * SIZE
	beq	LL(Main)

	mr	XP, BUFFER
	addi	CO, BUFFER, -SIZE

	srawi.	r0, MIN_N, 2
	mtspr	CTR, r0
	ble	LL(CopyRemain)
	.align 4

LL(CopyKernel):
	LFD	f0, 0 * SIZE(X)
	LFD	f1, 1 * SIZE(X)
	add	X, X, INCX
	LFD	f2, 0 * SIZE(X)
	LFD	f3, 1 * SIZE(X)
	add	X, X, INCX
	LFD	f4, 0 * SIZE(X)
	LFD	f5, 1 * SIZE(X)
	add	X, X, INCX
	LFD	f6, 0 * SIZE(X)
	LFD	f7, 1 * SIZE(X)
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
	andi.	r0, MIN_N, 3
	mtspr	CTR, r0
	ble	LL(Main)
	.align 4

LL(CopySub):
	LFD	f0, 0 * SIZE(X)
	LFD	f1, 1 * SIZE(X)
	add	X, X, INCX
	STFD	f0,  1 * SIZE(CO)
	STFDU	f1,  2 * SIZE(CO)
	bdnz	LL(CopySub)
	.align 4

LL(Main):
	mr	CO, Y
	addi	XP, XP, -SIZE
	srawi.	J, N, 2
	ble	LL(Remain)
	.align 4

LL(MainHead):
	mr     AO1, A
	add    AO2, A,   LDA
	add    AO3, AO2, LDA
	add    AO4, AO3, LDA
	add    A,   AO4, LDA

	mr     BO, XP

	lfd	 f0,  FZERO
	fmr	 f1,  f0
	fmr	 f2,  f0
	fmr	 f3,  f0
	fmr	 f4,  f0
	fmr	 f5,  f0
	fmr	 f6,  f0
	fmr	 f7,  f0
	fmr	 f8,  f0
	fmr	 f9,  f0
	fmr	 f10, f0
	fmr	 f11, f0
	fmr	 f12, f0
	fmr	 f13, f0
	fmr	 f14, f0
	fmr	 f15, f0

	dcbtst	 PREC, CO
	srawi.	r0,  MIN_N, 3
	mtspr	CTR, r0
	ble	LL(MainN3)

	LFD	f16, 0 * SIZE(AO1)
	LFD	f17, 1 * SIZE(AO1)
	LFD	f18, 0 * SIZE(AO2)
	LFD	f19, 1 * SIZE(AO2)
	LFD	f20, 0 * SIZE(AO3)
	LFD	f21, 1 * SIZE(AO3)
	LFD	f22, 0 * SIZE(AO4)
	LFD	f23, 1 * SIZE(AO4)

	LFD	f24, 1 * SIZE(BO)
	LFD	f25, 2 * SIZE(BO)
	LFD	f26, 3 * SIZE(BO)
	LFD	f27, 4 * SIZE(BO)
	LFD	f28, 5 * SIZE(BO)
	LFD	f29, 6 * SIZE(BO)
	LFD	f30, 7 * SIZE(BO)
	LFD	f31, 8 * SIZE(BO)

	bdz	LL(MainKernelSkip)
	.align 5

LL(MainKernel):
	FMADD	f0,  f16,  f24, f0
	FMADD	f1,  f16,  f25, f1
	FMADD	f2,  f17,  f24, f2
	FMADD	f3,  f17,  f25, f3

	FMADD	f4,  f18,  f24, f4
	FMADD	f5,  f18,  f25, f5
	FMADD	f6,  f19,  f24, f6
	FMADD	f7,  f19,  f25, f7

	LFD	f16, 2 * SIZE(AO1)
	LFD	f17, 3 * SIZE(AO1)
	LFD	f18, 2 * SIZE(AO2)
	LFD	f19, 3 * SIZE(AO2)

	FMADD	f8,  f20,  f24, f8
	FMADD	f9,  f20,  f25, f9
	FMADD	f10, f21,  f24, f10
	FMADD	f11, f21,  f25, f11

	FMADD	f12, f22,  f24, f12
	FMADD	f13, f22,  f25, f13
	FMADD	f14, f23,  f24, f14
	FMADD	f15, f23,  f25, f15

	LFD	f20, 2 * SIZE(AO3)
	LFD	f21, 3 * SIZE(AO3)
	LFD	f22, 2 * SIZE(AO4)
	LFD	f23, 3 * SIZE(AO4)

	FMADD	f0,  f16,  f26, f0
	FMADD	f1,  f16,  f27, f1
	FMADD	f2,  f17,  f26, f2
	FMADD	f3,  f17,  f27, f3

	FMADD	f4,  f18,  f26, f4
	FMADD	f5,  f18,  f27, f5
	FMADD	f6,  f19,  f26, f6
	FMADD	f7,  f19,  f27, f7

	LFD	f16, 4 * SIZE(AO1)
	LFD	f17, 5 * SIZE(AO1)
	LFD	f18, 4 * SIZE(AO2)
	LFD	f19, 5 * SIZE(AO2)

	FMADD	f8,  f20,  f26, f8
	FMADD	f9,  f20,  f27, f9
	FMADD	f10, f21,  f26, f10
	FMADD	f11, f21,  f27, f11

	FMADD	f12, f22,  f26, f12
	FMADD	f13, f22,  f27, f13
	FMADD	f14, f23,  f26, f14
	FMADD	f15, f23,  f27, f15

	LFD	f20, 4 * SIZE(AO3)
	LFD	f21, 5 * SIZE(AO3)
	LFD	f22, 4 * SIZE(AO4)
	LFD	f23, 5 * SIZE(AO4)

	LFD	f24,  9 * SIZE(BO)
	LFD	f25, 10 * SIZE(BO)
	LFD	f26, 11 * SIZE(BO)
	LFD	f27, 12 * SIZE(BO)

	FMADD	f0,  f16,  f28, f0
	FMADD	f1,  f16,  f29, f1
	FMADD	f2,  f17,  f28, f2
	FMADD	f3,  f17,  f29, f3

	FMADD	f4,  f18,  f28, f4
	FMADD	f5,  f18,  f29, f5
	FMADD	f6,  f19,  f28, f6
	FMADD	f7,  f19,  f29, f7

	LFD	f16, 6 * SIZE(AO1)
	LFD	f17, 7 * SIZE(AO1)
	LFD	f18, 6 * SIZE(AO2)
	LFD	f19, 7 * SIZE(AO2)

	FMADD	f8,  f20,  f28, f8
	FMADD	f9,  f20,  f29, f9
	FMADD	f10, f21,  f28, f10
	FMADD	f11, f21,  f29, f11

	FMADD	f12, f22,  f28, f12
	FMADD	f13, f22,  f29, f13
	FMADD	f14, f23,  f28, f14
	FMADD	f15, f23,  f29, f15

	LFD	f20, 6 * SIZE(AO3)
	LFD	f21, 7 * SIZE(AO3)
	LFD	f22, 6 * SIZE(AO4)
	LFD	f23, 7 * SIZE(AO4)

	FMADD	f0,  f16,  f30, f0
	FMADD	f1,  f16,  f31, f1
	FMADD	f2,  f17,  f30, f2
	FMADD	f3,  f17,  f31, f3

	FMADD	f4,  f18,  f30, f4
	FMADD	f5,  f18,  f31, f5
	FMADD	f6,  f19,  f30, f6
	FMADD	f7,  f19,  f31, f7

	LFD	f16, 8 * SIZE(AO1)
	LFD	f17, 9 * SIZE(AO1)
	LFD	f18, 8 * SIZE(AO2)
	LFD	f19, 9 * SIZE(AO2)

	FMADD	f8,  f20,  f30, f8
	FMADD	f9,  f20,  f31, f9
	FMADD	f10, f21,  f30, f10
	FMADD	f11, f21,  f31, f11

	FMADD	f12, f22,  f30, f12
	FMADD	f13, f22,  f31, f13
	FMADD	f14, f23,  f30, f14
	FMADD	f15, f23,  f31, f15

	LFD	f20, 8 * SIZE(AO3)
	LFD	f21, 9 * SIZE(AO3)
	LFD	f22, 8 * SIZE(AO4)
	LFD	f23, 9 * SIZE(AO4)

	LFD	f28, 13 * SIZE(BO)
	LFD	f29, 14 * SIZE(BO)
	LFD	f30, 15 * SIZE(BO)
	LFD	f31, 16 * SIZE(BO)

	FMADD	f0,  f16,  f24, f0
	FMADD	f1,  f16,  f25, f1
	FMADD	f2,  f17,  f24, f2
	FMADD	f3,  f17,  f25, f3

	FMADD	f4,  f18,  f24, f4
	FMADD	f5,  f18,  f25, f5
	FMADD	f6,  f19,  f24, f6
	FMADD	f7,  f19,  f25, f7

	LFD	f16, 10 * SIZE(AO1)
	LFD	f17, 11 * SIZE(AO1)
	LFD	f18, 10 * SIZE(AO2)
	LFD	f19, 11 * SIZE(AO2)

	FMADD	f8,  f20,  f24, f8
	FMADD	f9,  f20,  f25, f9
	FMADD	f10, f21,  f24, f10
	FMADD	f11, f21,  f25, f11

	FMADD	f12, f22,  f24, f12
	FMADD	f13, f22,  f25, f13
	FMADD	f14, f23,  f24, f14
	FMADD	f15, f23,  f25, f15

	LFD	f20, 10 * SIZE(AO3)
	LFD	f21, 11 * SIZE(AO3)
	LFD	f22, 10 * SIZE(AO4)
	LFD	f23, 11 * SIZE(AO4)

	FMADD	f0,  f16,  f26, f0
	FMADD	f1,  f16,  f27, f1
	FMADD	f2,  f17,  f26, f2
	FMADD	f3,  f17,  f27, f3

	FMADD	f4,  f18,  f26, f4
	FMADD	f5,  f18,  f27, f5
	FMADD	f6,  f19,  f26, f6
	FMADD	f7,  f19,  f27, f7

	LFD	f16, 12 * SIZE(AO1)
	LFD	f17, 13 * SIZE(AO1)
	LFD	f18, 12 * SIZE(AO2)
	LFD	f19, 13 * SIZE(AO2)

	FMADD	f8,  f20,  f26, f8
	FMADD	f9,  f20,  f27, f9
	FMADD	f10, f21,  f26, f10
	FMADD	f11, f21,  f27, f11

	FMADD	f12, f22,  f26, f12
	FMADD	f13, f22,  f27, f13
	FMADD	f14, f23,  f26, f14
	FMADD	f15, f23,  f27, f15

	LFD	f20, 12 * SIZE(AO3)
	LFD	f21, 13 * SIZE(AO3)
	LFD	f22, 12 * SIZE(AO4)
	LFD	f23, 13 * SIZE(AO4)

	LFD	f24, 17 * SIZE(BO)
	LFD	f25, 18 * SIZE(BO)
	LFD	f26, 19 * SIZE(BO)
	LFD	f27, 20 * SIZE(BO)

	FMADD	f0,  f16,  f28, f0
	FMADD	f1,  f16,  f29, f1
	FMADD	f2,  f17,  f28, f2
	FMADD	f3,  f17,  f29, f3

	FMADD	f4,  f18,  f28, f4
	FMADD	f5,  f18,  f29, f5
	FMADD	f6,  f19,  f28, f6
	FMADD	f7,  f19,  f29, f7

	LFD	f16, 14 * SIZE(AO1)
	LFD	f17, 15 * SIZE(AO1)
	LFD	f18, 14 * SIZE(AO2)
	LFD	f19, 15 * SIZE(AO2)

	FMADD	f8,  f20,  f28, f8
	FMADD	f9,  f20,  f29, f9
	FMADD	f10, f21,  f28, f10
	FMADD	f11, f21,  f29, f11

	FMADD	f12, f22,  f28, f12
	FMADD	f13, f22,  f29, f13
	FMADD	f14, f23,  f28, f14
	FMADD	f15, f23,  f29, f15

	LFD	f20, 14 * SIZE(AO3)
	LFD	f21, 15 * SIZE(AO3)
	LFD	f22, 14 * SIZE(AO4)
	LFD	f23, 15 * SIZE(AO4)

	FMADD	f0,  f16,  f30, f0
	FMADD	f1,  f16,  f31, f1
	FMADD	f2,  f17,  f30, f2
	FMADD	f3,  f17,  f31, f3

	FMADD	f4,  f18,  f30, f4
	FMADD	f5,  f18,  f31, f5
	FMADD	f6,  f19,  f30, f6
	FMADD	f7,  f19,  f31, f7

	LFD	f16, 16 * SIZE(AO1)
	LFD	f17, 17 * SIZE(AO1)
	LFD	f18, 16 * SIZE(AO2)
	LFD	f19, 17 * SIZE(AO2)

	addi	AO1, AO1, 16 * SIZE
	addi	AO2, AO2, 16 * SIZE
	DCBT(AO1, PREA)
	DCBT(AO2, PREA)

	FMADD	f8,  f20,  f30, f8
	FMADD	f9,  f20,  f31, f9
	FMADD	f10, f21,  f30, f10
	FMADD	f11, f21,  f31, f11

	FMADD	f12, f22,  f30, f12
	FMADD	f13, f22,  f31, f13
	FMADD	f14, f23,  f30, f14
	FMADD	f15, f23,  f31, f15

	LFD	f20, 16 * SIZE(AO3)
	LFD	f21, 17 * SIZE(AO3)
	LFD	f22, 16 * SIZE(AO4)
	LFD	f23, 17 * SIZE(AO4)

	LFD	f28, 21 * SIZE(BO)
	LFD	f29, 22 * SIZE(BO)
	LFD	f30, 23 * SIZE(BO)
	LFD	f31, 24 * SIZE(BO)

	addi	AO3, AO3, 16 * SIZE
	addi	AO4, AO4, 16 * SIZE
	DCBT(AO3, PREA)
	DCBT(AO4, PREA)

	addi	BO, BO, 16 * SIZE
 	bdnz	LL(MainKernel)
	.align 4

LL(MainKernelSkip):
	FMADD	f0,  f16,  f24, f0
	FMADD	f1,  f16,  f25, f1
	FMADD	f2,  f17,  f24, f2
	FMADD	f3,  f17,  f25, f3

	FMADD	f4,  f18,  f24, f4
	FMADD	f5,  f18,  f25, f5
	FMADD	f6,  f19,  f24, f6
	FMADD	f7,  f19,  f25, f7

	LFD	f16, 2 * SIZE(AO1)
	LFD	f17, 3 * SIZE(AO1)
	LFD	f18, 2 * SIZE(AO2)
	LFD	f19, 3 * SIZE(AO2)

	FMADD	f8,  f20,  f24, f8
	FMADD	f9,  f20,  f25, f9
	FMADD	f10, f21,  f24, f10
	FMADD	f11, f21,  f25, f11

	FMADD	f12, f22,  f24, f12
	FMADD	f13, f22,  f25, f13
	FMADD	f14, f23,  f24, f14
	FMADD	f15, f23,  f25, f15

	LFD	f20, 2 * SIZE(AO3)
	LFD	f21, 3 * SIZE(AO3)
	LFD	f22, 2 * SIZE(AO4)
	LFD	f23, 3 * SIZE(AO4)

	FMADD	f0,  f16,  f26, f0
	FMADD	f1,  f16,  f27, f1
	FMADD	f2,  f17,  f26, f2
	FMADD	f3,  f17,  f27, f3

	FMADD	f4,  f18,  f26, f4
	FMADD	f5,  f18,  f27, f5
	FMADD	f6,  f19,  f26, f6
	FMADD	f7,  f19,  f27, f7

	LFD	f16, 4 * SIZE(AO1)
	LFD	f17, 5 * SIZE(AO1)
	LFD	f18, 4 * SIZE(AO2)
	LFD	f19, 5 * SIZE(AO2)

	FMADD	f8,  f20,  f26, f8
	FMADD	f9,  f20,  f27, f9
	FMADD	f10, f21,  f26, f10
	FMADD	f11, f21,  f27, f11

	FMADD	f12, f22,  f26, f12
	FMADD	f13, f22,  f27, f13
	FMADD	f14, f23,  f26, f14
	FMADD	f15, f23,  f27, f15

	LFD	f20, 4 * SIZE(AO3)
	LFD	f21, 5 * SIZE(AO3)
	LFD	f22, 4 * SIZE(AO4)
	LFD	f23, 5 * SIZE(AO4)

	FMADD	f0,  f16,  f28, f0
	FMADD	f1,  f16,  f29, f1
	FMADD	f2,  f17,  f28, f2
	FMADD	f3,  f17,  f29, f3

	FMADD	f4,  f18,  f28, f4
	FMADD	f5,  f18,  f29, f5
	FMADD	f6,  f19,  f28, f6
	FMADD	f7,  f19,  f29, f7

	LFD	f16, 6 * SIZE(AO1)
	LFD	f17, 7 * SIZE(AO1)
	LFD	f18, 6 * SIZE(AO2)
	LFD	f19, 7 * SIZE(AO2)

	FMADD	f8,  f20,  f28, f8
	FMADD	f9,  f20,  f29, f9
	FMADD	f10, f21,  f28, f10
	FMADD	f11, f21,  f29, f11

	FMADD	f12, f22,  f28, f12
	FMADD	f13, f22,  f29, f13
	FMADD	f14, f23,  f28, f14
	FMADD	f15, f23,  f29, f15

	LFD	f20, 6 * SIZE(AO3)
	LFD	f21, 7 * SIZE(AO3)
	LFD	f22, 6 * SIZE(AO4)
	LFD	f23, 7 * SIZE(AO4)

	FMADD	f0,  f16,  f30, f0
	FMADD	f1,  f16,  f31, f1
	FMADD	f2,  f17,  f30, f2
	FMADD	f3,  f17,  f31, f3

	FMADD	f4,  f18,  f30, f4
	FMADD	f5,  f18,  f31, f5
	FMADD	f6,  f19,  f30, f6
	FMADD	f7,  f19,  f31, f7

	LFD	f16, 8 * SIZE(AO1)
	LFD	f17, 9 * SIZE(AO1)
	LFD	f18, 8 * SIZE(AO2)
	LFD	f19, 9 * SIZE(AO2)

	FMADD	f8,  f20,  f30, f8
	FMADD	f9,  f20,  f31, f9
	FMADD	f10, f21,  f30, f10
	FMADD	f11, f21,  f31, f11

	FMADD	f12, f22,  f30, f12
	FMADD	f13, f22,  f31, f13
	FMADD	f14, f23,  f30, f14
	FMADD	f15, f23,  f31, f15

	LFD	f20, 8 * SIZE(AO3)
	LFD	f21, 9 * SIZE(AO3)
	LFD	f22, 8 * SIZE(AO4)
	LFD	f23, 9 * SIZE(AO4)

	LFD	f24,  9 * SIZE(BO)
	LFD	f25, 10 * SIZE(BO)
	LFD	f26, 11 * SIZE(BO)
	LFD	f27, 12 * SIZE(BO)

	LFD	f28, 13 * SIZE(BO)
	LFD	f29, 14 * SIZE(BO)
	LFD	f30, 15 * SIZE(BO)
	LFDU	f31, 16 * SIZE(BO)

	FMADD	f0,  f16,  f24, f0
	FMADD	f1,  f16,  f25, f1
	FMADD	f2,  f17,  f24, f2
	FMADD	f3,  f17,  f25, f3

	FMADD	f4,  f18,  f24, f4
	FMADD	f5,  f18,  f25, f5
	FMADD	f6,  f19,  f24, f6
	FMADD	f7,  f19,  f25, f7

	LFD	f16, 10 * SIZE(AO1)
	LFD	f17, 11 * SIZE(AO1)
	LFD	f18, 10 * SIZE(AO2)
	LFD	f19, 11 * SIZE(AO2)

	FMADD	f8,  f20,  f24, f8
	FMADD	f9,  f20,  f25, f9
	FMADD	f10, f21,  f24, f10
	FMADD	f11, f21,  f25, f11

	FMADD	f12, f22,  f24, f12
	FMADD	f13, f22,  f25, f13
	FMADD	f14, f23,  f24, f14
	FMADD	f15, f23,  f25, f15

	LFD	f20, 10 * SIZE(AO3)
	LFD	f21, 11 * SIZE(AO3)
	LFD	f22, 10 * SIZE(AO4)
	LFD	f23, 11 * SIZE(AO4)

	FMADD	f0,  f16,  f26, f0
	FMADD	f1,  f16,  f27, f1
	FMADD	f2,  f17,  f26, f2
	FMADD	f3,  f17,  f27, f3

	FMADD	f4,  f18,  f26, f4
	FMADD	f5,  f18,  f27, f5
	FMADD	f6,  f19,  f26, f6
	FMADD	f7,  f19,  f27, f7

	LFD	f16, 12 * SIZE(AO1)
	LFD	f17, 13 * SIZE(AO1)
	LFD	f18, 12 * SIZE(AO2)
	LFD	f19, 13 * SIZE(AO2)

	FMADD	f8,  f20,  f26, f8
	FMADD	f9,  f20,  f27, f9
	FMADD	f10, f21,  f26, f10
	FMADD	f11, f21,  f27, f11

	FMADD	f12, f22,  f26, f12
	FMADD	f13, f22,  f27, f13
	FMADD	f14, f23,  f26, f14
	FMADD	f15, f23,  f27, f15

	LFD	f20, 12 * SIZE(AO3)
	LFD	f21, 13 * SIZE(AO3)
	LFD	f22, 12 * SIZE(AO4)
	LFD	f23, 13 * SIZE(AO4)

	FMADD	f0,  f16,  f28, f0
	FMADD	f1,  f16,  f29, f1
	FMADD	f2,  f17,  f28, f2
	FMADD	f3,  f17,  f29, f3

	FMADD	f4,  f18,  f28, f4
	FMADD	f5,  f18,  f29, f5
	FMADD	f6,  f19,  f28, f6
	FMADD	f7,  f19,  f29, f7

	LFD	f16, 14 * SIZE(AO1)
	LFD	f17, 15 * SIZE(AO1)
	LFD	f18, 14 * SIZE(AO2)
	LFD	f19, 15 * SIZE(AO2)

	FMADD	f8,  f20,  f28, f8
	FMADD	f9,  f20,  f29, f9
	FMADD	f10, f21,  f28, f10
	FMADD	f11, f21,  f29, f11

	FMADD	f12, f22,  f28, f12
	FMADD	f13, f22,  f29, f13
	FMADD	f14, f23,  f28, f14
	FMADD	f15, f23,  f29, f15

	LFD	f20, 14 * SIZE(AO3)
	LFD	f21, 15 * SIZE(AO3)
	LFD	f22, 14 * SIZE(AO4)
	LFD	f23, 15 * SIZE(AO4)

	addi	AO1, AO1, 16 * SIZE
	addi	AO2, AO2, 16 * SIZE
	addi	AO3, AO3, 16 * SIZE
	addi	AO4, AO4, 16 * SIZE

	FMADD	f0,  f16,  f30, f0
	FMADD	f1,  f16,  f31, f1
	FMADD	f2,  f17,  f30, f2
	FMADD	f3,  f17,  f31, f3

	FMADD	f4,  f18,  f30, f4
	FMADD	f5,  f18,  f31, f5
	FMADD	f6,  f19,  f30, f6
	FMADD	f7,  f19,  f31, f7

	FMADD	f8,  f20,  f30, f8
	FMADD	f9,  f20,  f31, f9
	FMADD	f10, f21,  f30, f10
	FMADD	f11, f21,  f31, f11

	FMADD	f12, f22,  f30, f12
	FMADD	f13, f22,  f31, f13
	FMADD	f14, f23,  f30, f14
	FMADD	f15, f23,  f31, f15
	.align 4

LL(MainN3):
	andi.	r0, MIN_N, 7
	mtspr	CTR, r0
	ble	LL(MainFinish)
	.align 4

	LFD	f16, 0 * SIZE(AO1)
	LFD	f17, 1 * SIZE(AO1)
	LFD	f18, 0 * SIZE(AO2)
	LFD	f19, 1 * SIZE(AO2)
	LFD	f20, 0 * SIZE(AO3)
	LFD	f21, 1 * SIZE(AO3)
	LFD	f22, 0 * SIZE(AO4)
	LFD	f23, 1 * SIZE(AO4)

	LFD	f24, 1 * SIZE(BO)
	LFDU	f25, 2 * SIZE(BO)

	addi	AO1, AO1, 2 * SIZE
	addi	AO2, AO2, 2 * SIZE
	addi	AO3, AO3, 2 * SIZE
	addi	AO4, AO4, 2 * SIZE

	bdz	LL(MainN3KernelSkip)
	.align 4

LL(MainN3Kernel):
	FMADD	f0,  f16,  f24, f0
	FMADD	f1,  f16,  f25, f1
	FMADD	f2,  f17,  f24, f2
	FMADD	f3,  f17,  f25, f3

	FMADD	f4,  f18,  f24, f4
	FMADD	f5,  f18,  f25, f5
	FMADD	f6,  f19,  f24, f6
	FMADD	f7,  f19,  f25, f7

	LFD	f16, 0 * SIZE(AO1)
	LFD	f17, 1 * SIZE(AO1)
	LFD	f18, 0 * SIZE(AO2)
	LFD	f19, 1 * SIZE(AO2)

	FMADD	f8,  f20,  f24, f8
	FMADD	f9,  f20,  f25, f9
	FMADD	f10, f21,  f24, f10
	FMADD	f11, f21,  f25, f11

	FMADD	f12, f22,  f24, f12
	FMADD	f13, f22,  f25, f13
	FMADD	f14, f23,  f24, f14
	FMADD	f15, f23,  f25, f15

	LFD	f20, 0 * SIZE(AO3)
	LFD	f21, 1 * SIZE(AO3)
	LFD	f22, 0 * SIZE(AO4)
	LFD	f23, 1 * SIZE(AO4)

	LFD	f24, 1 * SIZE(BO)
	LFDU	f25, 2 * SIZE(BO)

	addi	AO1, AO1, 2 * SIZE
	addi	AO2, AO2, 2 * SIZE
	addi	AO3, AO3, 2 * SIZE
	addi	AO4, AO4, 2 * SIZE

	bdnz	LL(MainN3Kernel)
	.align 4

LL(MainN3KernelSkip):
	FMADD	f0,  f16,  f24, f0
	FMADD	f1,  f16,  f25, f1
	FMADD	f2,  f17,  f24, f2
	FMADD	f3,  f17,  f25, f3

	FMADD	f4,  f18,  f24, f4
	FMADD	f5,  f18,  f25, f5
	FMADD	f6,  f19,  f24, f6
	FMADD	f7,  f19,  f25, f7

	FMADD	f8,  f20,  f24, f8
	FMADD	f9,  f20,  f25, f9
	FMADD	f10, f21,  f24, f10
	FMADD	f11, f21,  f25, f11

	FMADD	f12, f22,  f24, f12
	FMADD	f13, f22,  f25, f13
	FMADD	f14, f23,  f24, f14
	FMADD	f15, f23,  f25, f15
	.align 4

LL(MainFinish):
	lfd	f30, ALPHA_R
	lfd	f31, ALPHA_I

#ifndef XCONJ
#ifndef CONJ
	FSUB	f0,  f0,  f3
	FADD	f1,  f1,  f2
	FSUB	f4,  f4,  f7
	FADD	f5,  f5,  f6
	FSUB	f8,  f8,  f11
	FADD	f9,  f9,  f10
	FSUB	f12, f12, f15
	FADD	f13, f13, f14
#else
	FADD	f0,  f0,  f3
	FSUB	f1,  f1,  f2
	FADD	f4,  f4,  f7
	FSUB	f5,  f5,  f6
	FADD	f8,  f8,  f11
	FSUB	f9,  f9,  f10
	FADD	f12, f12, f15
	FSUB	f13, f13, f14
#endif
#else
#ifndef CONJ
	FADD	f0,  f0,  f3
	FSUB	f1,  f2,  f1
	FADD	f4,  f4,  f7
	FSUB	f5,  f6,  f5
	FADD	f8,  f8,  f11
	FSUB	f9,  f10, f9
	FADD	f12, f12, f15
	FSUB	f13, f14, f13
#else
	FSUB	f0,  f0,  f3
	FADD	f1,  f1,  f2
	FSUB	f4,  f4,  f7
	FADD	f5,  f5,  f6
	FSUB	f8,  f8,  f11
	FADD	f9,  f9,  f10
	FSUB	f12, f12, f15
	FADD	f13, f13, f14
#endif
#endif

	mr	BO, CO
	cmpwi	cr0, INCY, 2 * SIZE
	bne	LL(FinishN1)

	LFD	f16,  0 * SIZE(CO)
	LFD	f17,  1 * SIZE(CO)
	LFD	f18,  2 * SIZE(CO)
	LFD	f19,  3 * SIZE(CO)
	LFD	f20,  4 * SIZE(CO)
	LFD	f21,  5 * SIZE(CO)
	LFD	f22,  6 * SIZE(CO)
	LFD	f23,  7 * SIZE(CO)

	FMADD	f16, f30, f0,  f16
	FMADDR	f17, f30, f1,  f17
	FMADD	f18, f30, f4,  f18
	FMADDR	f19, f30, f5,  f19

	FMADD	f20, f30, f8,  f20
	FMADDR	f21, f30, f9,  f21
	FMADD	f22, f30, f12, f22
	FMADDR	f23, f30, f13, f23

	FMSUBR	f16, f31, f1,  f16
	FMADD	f17, f31, f0,  f17
	FMSUBR	f18, f31, f5,  f18
	FMADD	f19, f31, f4,  f19

	FMSUBR	f20, f31, f9,  f20
	FMADD	f21, f31, f8,  f21
	FMSUBR	f22, f31, f13, f22
	FMADD	f23, f31, f12, f23

	STFD	f16,  0 * SIZE(CO)
	STFD	f17,  1 * SIZE(CO)
	STFD	f18,  2 * SIZE(CO)
	STFD	f19,  3 * SIZE(CO)

	STFD	f20,  4 * SIZE(CO)
	STFD	f21,  5 * SIZE(CO)
	STFD	f22,  6 * SIZE(CO)
	STFD	f23,  7 * SIZE(CO)

	addi	CO, CO, 8 * SIZE

	addi	J, J, -1
	cmpwi	cr0, J, 0
	bgt	LL(MainHead)
	b	LL(Remain)
	.align 4

LL(FinishN1):
	LFD	f16,  0 * SIZE(CO)
	LFD	f17,  1 * SIZE(CO)
	add	CO, CO, INCY

	LFD	f18,  0 * SIZE(CO)
	LFD	f19,  1 * SIZE(CO)
	add	CO, CO, INCY

	LFD	f20,  0 * SIZE(CO)
	LFD	f21,  1 * SIZE(CO)
	add	CO, CO, INCY

	LFD	f22,  0 * SIZE(CO)
	LFD	f23,  1 * SIZE(CO)
	add	CO, CO, INCY

	FMADD	f16, f30, f0,  f16
	FMADDR	f17, f30, f1,  f17
	FMADD	f18, f30, f4,  f18
	FMADDR	f19, f30, f5,  f19

	FMADD	f20, f30, f8,  f20
	FMADDR	f21, f30, f9,  f21
	FMADD	f22, f30, f12, f22
	FMADDR	f23, f30, f13, f23

	FMSUBR	f16, f31, f1,  f16
	FMADD	f17, f31, f0,  f17
	FMSUBR	f18, f31, f5,  f18
	FMADD	f19, f31, f4,  f19

	FMSUBR	f20, f31, f9,  f20
	FMADD	f21, f31, f8,  f21
	FMSUBR	f22, f31, f13, f22
	FMADD	f23, f31, f12, f23

	STFD	f16,  0 * SIZE(BO)
	STFD	f17,  1 * SIZE(BO)
	add	BO, BO, INCY
	STFD	f18,  0 * SIZE(BO)
	STFD	f19,  1 * SIZE(BO)
	add	BO, BO, INCY

	STFD	f20,  0 * SIZE(BO)
	STFD	f21,  1 * SIZE(BO)
	add	BO, BO, INCY
	STFD	f22,  0 * SIZE(BO)
	STFD	f23,  1 * SIZE(BO)

	addi	J, J, -1
	cmpwi	cr0, J, 0
	bgt	LL(MainHead)
	.align 4

LL(Remain):
	andi.	J, N, 3
	ble	LL(ISEnd)
	.align 4

LL(RemainHead):
	mr	AO1, A
	add	A,   A,  LDA
	mr	BO,  XP
	lfd	f0, FZERO

	fmr	 f1,  f0
	fmr	 f2,  f0
	fmr	 f3,  f0
	fmr	 f4,  f0
	fmr	 f5,  f0
	fmr	 f6,  f0
	fmr	 f7,  f0
	fmr	 f8,  f0
	fmr	 f9,  f0
	fmr	 f10, f0
	fmr	 f11, f0
	fmr	 f12, f0
	fmr	 f13, f0
	fmr	 f14, f0
	fmr	 f15, f0

	srawi.	r0 , MIN_N, 3
	mtspr	CTR, r0
	ble	LL(RemainN3)

	LFD	f16, 0 * SIZE(AO1)
	LFD	f17, 1 * SIZE(AO1)
	LFD	f18, 2 * SIZE(AO1)
	LFD	f19, 3 * SIZE(AO1)

	LFD	f20, 4 * SIZE(AO1)
	LFD	f21, 5 * SIZE(AO1)
	LFD	f22, 6 * SIZE(AO1)
	LFD	f23, 7 * SIZE(AO1)

	LFD	f24, 1 * SIZE(BO)
	LFD	f25, 2 * SIZE(BO)
	LFD	f26, 3 * SIZE(BO)
	LFD	f27, 4 * SIZE(BO)

	LFD	f28, 5 * SIZE(BO)
	LFD	f29, 6 * SIZE(BO)
	LFD	f30, 7 * SIZE(BO)
	LFD	f31, 8 * SIZE(BO)

	bdz	LL(RemainKernelSkip)
	.align 4

LL(RemainKernel):
	FMADD	f0,  f16,  f24, f0
	FMADD	f1,  f16,  f25, f1
	FMADD	f2,  f17,  f24, f2
	FMADD	f3,  f17,  f25, f3

	FMADD	f4,  f18,  f26, f4
	FMADD	f5,  f18,  f27, f5
	FMADD	f6,  f19,  f26, f6
	FMADD	f7,  f19,  f27, f7

	LFD	f16,  8 * SIZE(AO1)
	LFD	f17,  9 * SIZE(AO1)
	LFD	f18, 10 * SIZE(AO1)
	LFD	f19, 11 * SIZE(AO1)

	LFD	f24,  9 * SIZE(BO)
	LFD	f25, 10 * SIZE(BO)
	LFD	f26, 11 * SIZE(BO)
	LFD	f27, 12 * SIZE(BO)

	FMADD	f8,  f20,  f28, f8
	FMADD	f9,  f20,  f29, f9
	FMADD	f10, f21,  f28, f10
	FMADD	f11, f21,  f29, f11

	FMADD	f12, f22,  f30, f12
	FMADD	f13, f22,  f31, f13
	FMADD	f14, f23,  f30, f14
	FMADD	f15, f23,  f31, f15

	LFD	f20, 12 * SIZE(AO1)
	LFD	f21, 13 * SIZE(AO1)
	LFD	f22, 14 * SIZE(AO1)
	LFD	f23, 15 * SIZE(AO1)

	LFD	f28, 13 * SIZE(BO)
	LFD	f29, 14 * SIZE(BO)
	LFD	f30, 15 * SIZE(BO)
	LFD	f31, 16 * SIZE(BO)

	FMADD	f0,  f16,  f24, f0
	FMADD	f1,  f16,  f25, f1
	FMADD	f2,  f17,  f24, f2
	FMADD	f3,  f17,  f25, f3

	FMADD	f4,  f18,  f26, f4
	FMADD	f5,  f18,  f27, f5
	FMADD	f6,  f19,  f26, f6
	FMADD	f7,  f19,  f27, f7

	LFD	f16, 16 * SIZE(AO1)
	LFD	f17, 17 * SIZE(AO1)
	LFD	f18, 18 * SIZE(AO1)
	LFD	f19, 19 * SIZE(AO1)

	LFD	f24, 17 * SIZE(BO)
	LFD	f25, 18 * SIZE(BO)
	LFD	f26, 19 * SIZE(BO)
	LFD	f27, 20 * SIZE(BO)

	FMADD	f8,  f20,  f28, f8
	FMADD	f9,  f20,  f29, f9
	FMADD	f10, f21,  f28, f10
	FMADD	f11, f21,  f29, f11

	FMADD	f12, f22,  f30, f12
	FMADD	f13, f22,  f31, f13
	FMADD	f14, f23,  f30, f14
	FMADD	f15, f23,  f31, f15

	LFD	f20, 20 * SIZE(AO1)
	LFD	f21, 21 * SIZE(AO1)
	LFD	f22, 22 * SIZE(AO1)
	LFD	f23, 23 * SIZE(AO1)

	LFD	f28, 21 * SIZE(BO)
	LFD	f29, 22 * SIZE(BO)
	LFD	f30, 23 * SIZE(BO)
	LFD	f31, 24 * SIZE(BO)

	addi	AO1, AO1, 16 * SIZE
	addi	BO,  BO,  16 * SIZE

	DCBT(AO1, PREA)

	bdnz	LL(RemainKernel)
	.align 4

LL(RemainKernelSkip):
	FMADD	f0,  f16,  f24, f0
	FMADD	f1,  f16,  f25, f1
	FMADD	f2,  f17,  f24, f2
	FMADD	f3,  f17,  f25, f3

	FMADD	f4,  f18,  f26, f4
	FMADD	f5,  f18,  f27, f5
	FMADD	f6,  f19,  f26, f6
	FMADD	f7,  f19,  f27, f7

	LFD	f16,  8 * SIZE(AO1)
	LFD	f17,  9 * SIZE(AO1)
	LFD	f18, 10 * SIZE(AO1)
	LFD	f19, 11 * SIZE(AO1)

	LFD	f24,  9 * SIZE(BO)
	LFD	f25, 10 * SIZE(BO)
	LFD	f26, 11 * SIZE(BO)
	LFD	f27, 12 * SIZE(BO)

	FMADD	f8,  f20,  f28, f8
	FMADD	f9,  f20,  f29, f9
	FMADD	f10, f21,  f28, f10
	FMADD	f11, f21,  f29, f11

	FMADD	f12, f22,  f30, f12
	FMADD	f13, f22,  f31, f13
	FMADD	f14, f23,  f30, f14
	FMADD	f15, f23,  f31, f15

	LFD	f20, 12 * SIZE(AO1)
	LFD	f21, 13 * SIZE(AO1)
	LFD	f22, 14 * SIZE(AO1)
	LFD	f23, 15 * SIZE(AO1)

	LFD	f28, 13 * SIZE(BO)
	LFD	f29, 14 * SIZE(BO)
	LFD	f30, 15 * SIZE(BO)
	LFDU	f31, 16 * SIZE(BO)

	FMADD	f0,  f16,  f24, f0
	FMADD	f1,  f16,  f25, f1
	FMADD	f2,  f17,  f24, f2
	FMADD	f3,  f17,  f25, f3

	FMADD	f4,  f18,  f26, f4
	FMADD	f5,  f18,  f27, f5
	FMADD	f6,  f19,  f26, f6
	FMADD	f7,  f19,  f27, f7

	FMADD	f8,  f20,  f28, f8
	FMADD	f9,  f20,  f29, f9
	FMADD	f10, f21,  f28, f10
	FMADD	f11, f21,  f29, f11

	FMADD	f12, f22,  f30, f12
	FMADD	f13, f22,  f31, f13
	FMADD	f14, f23,  f30, f14
	FMADD	f15, f23,  f31, f15

	addi	AO1, AO1, 16 * SIZE
	.align 4

LL(RemainN3):
	andi.	r0,  MIN_N, 7
	mtspr	CTR, r0
	ble	LL(RemainFinish)
	.align 4

	LFD	f16, 0 * SIZE(AO1)
	LFD	f17, 1 * SIZE(AO1)
	LFD	f24, 1 * SIZE(BO)
	LFDU	f25, 2 * SIZE(BO)
	addi	AO1, AO1, 2 * SIZE
	bdz	LL(RemainN3KernelSkip)
	.align 4

LL(RemainN3Kernel):
	FMADD	f0,  f16,  f24, f0
	FMADD	f1,  f16,  f25, f1
	FMADD	f2,  f17,  f24, f2
	FMADD	f3,  f17,  f25, f3

	LFD	f16, 0 * SIZE(AO1)
	LFD	f17, 1 * SIZE(AO1)
	LFD	f24, 1 * SIZE(BO)
	LFDU	f25, 2 * SIZE(BO)
	addi	AO1, AO1, 2 * SIZE
	bdnz	LL(RemainN3Kernel)
	.align 4

LL(RemainN3KernelSkip):
	FMADD	f0,  f16,  f24, f0
	FMADD	f1,  f16,  f25, f1
	FMADD	f2,  f17,  f24, f2
	FMADD	f3,  f17,  f25, f3
	.align 4

LL(RemainFinish):
	lfd	f30, ALPHA_R
	lfd	f31, ALPHA_I
	LFD	f16,  0 * SIZE(CO)
	LFD	f17,  1 * SIZE(CO)

	FADD	f0, f0, f4
	FADD	f1, f1, f5
	FADD	f2, f2, f6
	FADD	f3, f3, f7

	FADD	f8,  f8,  f12
	FADD	f9,  f9,  f13
	FADD	f10, f10, f14
	FADD	f11, f11, f15

	FADD	f0, f0, f8
	FADD	f1, f1, f9
	FADD	f2, f2, f10
	FADD	f3, f3, f11

#ifndef XCONJ
#ifndef CONJ
	FSUB	f0,  f0,  f3
	FADD	f1,  f1,  f2
#else
	FADD	f0,  f0,  f3
	FSUB	f1,  f1,  f2
#endif
#else
#ifndef CONJ
	FADD	f0,  f0,  f3
	FSUB	f1,  f2,  f1
#else
	FSUB	f0,  f0,  f3
	FADD	f1,  f1,  f2
#endif
#endif

	FMADD	f16, f30, f0,  f16
	FMADDR	f17, f30, f1,  f17
	FMSUBR	f16, f31, f1,  f16
	FMADD	f17, f31, f0,  f17

	STFD	f16,  0 * SIZE(CO)
	STFD	f17,  1 * SIZE(CO)
	add	CO, CO, INCY

	addi	J, J, -1
	cmpi	cr0, 0, J, 0
	bgt	LL(RemainHead)
	.align 4

LL(ISEnd):
	subf	A, PLDA_M, A
	addi	IS, IS, P

	cmp	cr0, 0, IS, M
	blt	LL(ISLoop)
	.align 4

LL(End):
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
	ld	r23,   216(SP)
	ld	r24,   224(SP)
	ld	r25,   232(SP)
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
	lwz	r23,   180(SP)
	lwz	r24,   184(SP)
	lwz	r25,   188(SP)
#endif

	addi	SP, SP, STACKSIZE

	blr

	EPILOGUE

#endif
