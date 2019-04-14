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

#define I	r11
#define	J	r12

#define AO1	r14
#define AO2	r15
#define AO3	r16
#define AO4	r17
#define AO5	r18
#define AO6	r19
#define AO7	r20
#define AO8	r21
#define LDA8	r22

#define	Y1	r23
#define	PREA	r24
#define	PREC	r25
#define YY	r26
#define BUFFER	r27

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

#define alpha1 f16
#define alpha2 f17
#define alpha3 f18
#define alpha4 f19
#define alpha5 f20
#define alpha6 f21
#define alpha7 f22
#define alpha8 f23

#define a1     f24
#define a2     f25
#define a3     f26
#define a4     f27
#define a5     f28
#define a6     f29
#define a7     f30
#define a8     f31

#define alpha  f31

#if defined(PPCG4)
#define PREFETCHSIZE_A  24
#define PREFETCHSIZE_C  16
#endif

#if defined(PPC440) || defined(PPC440FP2)
#define PREFETCHSIZE_A  24
#define PREFETCHSIZE_C  16
#endif

#ifdef PPC970
#define PREFETCHSIZE_A  16
#define PREFETCHSIZE_C  16
#endif

#ifdef CELL
#define PREFETCHSIZE_A  16
#define PREFETCHSIZE_C  16
#endif

#ifdef POWER4
#define PREFETCHSIZE_A  16
#define PREFETCHSIZE_C  16
#endif

#ifdef POWER5
#define PREFETCHSIZE_A  40
#define PREFETCHSIZE_C  24
#endif

#ifdef POWER6
#define PREFETCHSIZE_A  96
#define PREFETCHSIZE_C  40
#endif

#ifdef POWER8
#define PREFETCHSIZE_A  96
#define PREFETCHSIZE_C  40
#endif


#ifndef NEEDPARAM

#ifndef __64BIT__
#define STACKSIZE 224
#define ALPHA     200(SP)
#define	FZERO	  208(SP)
#else
#define STACKSIZE 280
#define ALPHA     256(SP)
#define FZERO	  264(SP)
#endif

	PROLOGUE
	PROFCODE

	addi	SP,   SP, -STACKSIZE
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
	std	r26,   240(SP)
	std	r27,   248(SP)
#else
	stw	r0,    0 + FZERO
	stw	r0,    4 + FZERO
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
	stw	r26,   192(SP)
	stw	r27,   196(SP)
#endif

#ifdef linux
#ifndef __64BIT__
	lwz	INCY,    FRAMESLOT(0) + STACKSIZE(SP)
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

	stfd	f1, ALPHA
	fmr	alpha, f1

	slwi	LDA8, LDA,  BASE_SHIFT + 3
	slwi	LDA,  LDA,  BASE_SHIFT
	slwi	INCX, INCX, BASE_SHIFT
	slwi	INCY, INCY, BASE_SHIFT

	li	PREA, PREFETCHSIZE_A * SIZE
	li	PREC, PREFETCHSIZE_C * SIZE

	cmpwi	cr0, M, 0
	ble-	LL(999)

	cmpwi	cr0, N, 0
	ble-	LL(999)

	mr	YY, Y
	lfd	f0, FZERO

	cmpi	cr0, 0, INCY, SIZE
	beq	LL(10)

	mr	YY, BUFFER
	mr	Y1, BUFFER

	addi	r0, M,  7
	srawi.	r0, r0, 3
	mtspr	CTR, r0
	.align 4

LL(02):
	STFD	f0, 0 * SIZE(Y1)
	STFD	f0, 1 * SIZE(Y1)
	STFD	f0, 2 * SIZE(Y1)
	STFD	f0, 3 * SIZE(Y1)
	STFD	f0, 4 * SIZE(Y1)
	STFD	f0, 5 * SIZE(Y1)
	STFD	f0, 6 * SIZE(Y1)
	STFD	f0, 7 * SIZE(Y1)
	addi	Y1, Y1, 8 * SIZE
	bdnz	LL(02)
	.align 4

LL(10):
	srawi.	J, N, 3
	ble	LL(20)
	.align 4

LL(11):
	LFD	alpha1, 0 * SIZE(X)
	add	X, X, INCX
	LFD	alpha2, 0 * SIZE(X)
	add	X, X, INCX
	LFD	alpha3, 0 * SIZE(X)
	add	X, X, INCX
	LFD	alpha4, 0 * SIZE(X)
	add	X, X, INCX
	LFD	alpha5, 0 * SIZE(X)
	add	X, X, INCX
	LFD	alpha6, 0 * SIZE(X)
	add	X, X, INCX
	LFD	alpha7, 0 * SIZE(X)
	add	X, X, INCX
	LFD	alpha8, 0 * SIZE(X)
	add	X, X, INCX

	FMUL	alpha1, alpha, alpha1
	FMUL	alpha2, alpha, alpha2
	FMUL	alpha3, alpha, alpha3
	FMUL	alpha4, alpha, alpha4
	FMUL	alpha5, alpha, alpha5
	FMUL	alpha6, alpha, alpha6
	FMUL	alpha7, alpha, alpha7
	FMUL	alpha8, alpha, alpha8

	mr	AO1, A
	add	AO2, A,   LDA
	add	AO3, AO2, LDA
	add	AO4, AO3, LDA
	add	AO5, AO4, LDA
	add	AO6, AO5, LDA
	add	AO7, AO6, LDA
	add	AO8, AO7, LDA
	add	A,   AO8, LDA

	mr	Y1, YY

	srawi.	r0,  M, 4
	mtspr	CTR, r0
	ble	LL(15)

	LFD	y01,  0 * SIZE(Y1)
	LFD	y02,  1 * SIZE(Y1)
	LFD	y03,  2 * SIZE(Y1)
	LFD	y04,  3 * SIZE(Y1)
	LFD	y05,  4 * SIZE(Y1)
	LFD	y06,  5 * SIZE(Y1)
	LFD	y07,  6 * SIZE(Y1)
	LFD	y08,  7 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	LFD	y09,  8 * SIZE(Y1)
	LFD	y10,  9 * SIZE(Y1)
	LFD	y11, 10 * SIZE(Y1)
	LFD	y12, 11 * SIZE(Y1)
	LFD	y13, 12 * SIZE(Y1)
	LFD	y14, 13 * SIZE(Y1)
	LFD	y15, 14 * SIZE(Y1)
	LFD	y16, 15 * SIZE(Y1)

	FMADD	y01, alpha1, a1, y01
	FMADD	y02, alpha1, a2, y02
	FMADD	y03, alpha1, a3, y03
	FMADD	y04, alpha1, a4, y04

	LFD	a1,  8 * SIZE(AO1)
	LFD	a2,  9 * SIZE(AO1)
	LFD	a3, 10 * SIZE(AO1)
	LFD	a4, 11 * SIZE(AO1)

	FMADD	y05, alpha1, a5, y05
	FMADD	y06, alpha1, a6, y06
	FMADD	y07, alpha1, a7, y07
	FMADD	y08, alpha1, a8, y08

	LFD	a5, 12 * SIZE(AO1)
	LFD	a6, 13 * SIZE(AO1)
	LFD	a7, 14 * SIZE(AO1)
	LFD	a8, 15 * SIZE(AO1)

	addi	AO1, AO1, 16 * SIZE
	nop
	nop
	DCBT(AO1, PREA)

	FMADD	y09, alpha1, a1, y09
	FMADD	y10, alpha1, a2, y10
	FMADD	y11, alpha1, a3, y11
	FMADD	y12, alpha1, a4, y12

	LFD	a1,  0 * SIZE(AO2)
	LFD	a2,  1 * SIZE(AO2)
	LFD	a3,  2 * SIZE(AO2)
	LFD	a4,  3 * SIZE(AO2)

	FMADD	y13, alpha1, a5, y13
	FMADD	y14, alpha1, a6, y14
	FMADD	y15, alpha1, a7, y15
	FMADD	y16, alpha1, a8, y16

	LFD	a5,  4 * SIZE(AO2)
	LFD	a6,  5 * SIZE(AO2)
	LFD	a7,  6 * SIZE(AO2)
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2, a1, y01
	FMADD	y02, alpha2, a2, y02
	FMADD	y03, alpha2, a3, y03
	FMADD	y04, alpha2, a4, y04

	LFD	a1,  8 * SIZE(AO2)
	LFD	a2,  9 * SIZE(AO2)
	LFD	a3, 10 * SIZE(AO2)
	LFD	a4, 11 * SIZE(AO2)

	FMADD	y05, alpha2, a5, y05
	FMADD	y06, alpha2, a6, y06
	FMADD	y07, alpha2, a7, y07
	FMADD	y08, alpha2, a8, y08

	LFD	a5, 12 * SIZE(AO2)
	LFD	a6, 13 * SIZE(AO2)
	LFD	a7, 14 * SIZE(AO2)
	LFD	a8, 15 * SIZE(AO2)

	addi	AO2, AO2, 16 * SIZE
	nop
	nop
	DCBT(AO2, PREA)

	FMADD	y09, alpha2, a1, y09
	FMADD	y10, alpha2, a2, y10
	FMADD	y11, alpha2, a3, y11
	FMADD	y12, alpha2, a4, y12

	LFD	a1,  0 * SIZE(AO3)
	LFD	a2,  1 * SIZE(AO3)
	LFD	a3,  2 * SIZE(AO3)
	LFD	a4,  3 * SIZE(AO3)

	FMADD	y13, alpha2, a5, y13
	FMADD	y14, alpha2, a6, y14
	FMADD	y15, alpha2, a7, y15
	FMADD	y16, alpha2, a8, y16

	LFD	a5,  4 * SIZE(AO3)
	LFD	a6,  5 * SIZE(AO3)
	LFD	a7,  6 * SIZE(AO3)
	LFD	a8,  7 * SIZE(AO3)

	FMADD	y01, alpha3, a1, y01
	FMADD	y02, alpha3, a2, y02
	FMADD	y03, alpha3, a3, y03
	FMADD	y04, alpha3, a4, y04

	LFD	a1,  8 * SIZE(AO3)
	LFD	a2,  9 * SIZE(AO3)
	LFD	a3, 10 * SIZE(AO3)
	LFD	a4, 11 * SIZE(AO3)

	FMADD	y05, alpha3, a5, y05
	FMADD	y06, alpha3, a6, y06
	FMADD	y07, alpha3, a7, y07
	FMADD	y08, alpha3, a8, y08

	LFD	a5, 12 * SIZE(AO3)
	LFD	a6, 13 * SIZE(AO3)
	LFD	a7, 14 * SIZE(AO3)
	LFD	a8, 15 * SIZE(AO3)

	addi	AO3, AO3, 16 * SIZE
	nop
	nop
	DCBT(AO3, PREA)

	FMADD	y09, alpha3, a1, y09
	FMADD	y10, alpha3, a2, y10
	FMADD	y11, alpha3, a3, y11
	FMADD	y12, alpha3, a4, y12

	LFD	a1,  0 * SIZE(AO4)
	LFD	a2,  1 * SIZE(AO4)
	LFD	a3,  2 * SIZE(AO4)
	LFD	a4,  3 * SIZE(AO4)

	FMADD	y13, alpha3, a5, y13
	FMADD	y14, alpha3, a6, y14
	FMADD	y15, alpha3, a7, y15
	FMADD	y16, alpha3, a8, y16

	LFD	a5,  4 * SIZE(AO4)
	LFD	a6,  5 * SIZE(AO4)
	LFD	a7,  6 * SIZE(AO4)
	LFD	a8,  7 * SIZE(AO4)

	FMADD	y01, alpha4, a1, y01
	FMADD	y02, alpha4, a2, y02
	FMADD	y03, alpha4, a3, y03
	FMADD	y04, alpha4, a4, y04

	LFD	a1,  8 * SIZE(AO4)
	LFD	a2,  9 * SIZE(AO4)
	LFD	a3, 10 * SIZE(AO4)
	LFD	a4, 11 * SIZE(AO4)

	FMADD	y05, alpha4, a5, y05
	FMADD	y06, alpha4, a6, y06
	FMADD	y07, alpha4, a7, y07
	FMADD	y08, alpha4, a8, y08

	LFD	a5, 12 * SIZE(AO4)
	LFD	a6, 13 * SIZE(AO4)
	LFD	a7, 14 * SIZE(AO4)
	LFD	a8, 15 * SIZE(AO4)

	addi	AO4, AO4, 16 * SIZE
	nop
	nop
	DCBT(AO4, PREA)

	FMADD	y09, alpha4, a1, y09
	FMADD	y10, alpha4, a2, y10
	FMADD	y11, alpha4, a3, y11
	FMADD	y12, alpha4, a4, y12

	LFD	a1,  0 * SIZE(AO5)
	LFD	a2,  1 * SIZE(AO5)
	LFD	a3,  2 * SIZE(AO5)
	LFD	a4,  3 * SIZE(AO5)

	FMADD	y13, alpha4, a5, y13
	FMADD	y14, alpha4, a6, y14
	FMADD	y15, alpha4, a7, y15
	FMADD	y16, alpha4, a8, y16

	LFD	a5,  4 * SIZE(AO5)
	LFD	a6,  5 * SIZE(AO5)
	LFD	a7,  6 * SIZE(AO5)
	LFD	a8,  7 * SIZE(AO5)

	FMADD	y01, alpha5, a1, y01
	FMADD	y02, alpha5, a2, y02
	FMADD	y03, alpha5, a3, y03
	FMADD	y04, alpha5, a4, y04

	LFD	a1,  8 * SIZE(AO5)
	LFD	a2,  9 * SIZE(AO5)
	LFD	a3, 10 * SIZE(AO5)
	LFD	a4, 11 * SIZE(AO5)

	FMADD	y05, alpha5, a5, y05
	FMADD	y06, alpha5, a6, y06
	FMADD	y07, alpha5, a7, y07
	FMADD	y08, alpha5, a8, y08

	LFD	a5, 12 * SIZE(AO5)
	LFD	a6, 13 * SIZE(AO5)
	LFD	a7, 14 * SIZE(AO5)
	LFD	a8, 15 * SIZE(AO5)

	addi	AO5, AO5, 16 * SIZE
	nop
	nop
	DCBT(AO5, PREA)

	FMADD	y09, alpha5, a1, y09
	FMADD	y10, alpha5, a2, y10
	FMADD	y11, alpha5, a3, y11
	FMADD	y12, alpha5, a4, y12

	LFD	a1,  0 * SIZE(AO6)
	LFD	a2,  1 * SIZE(AO6)
	LFD	a3,  2 * SIZE(AO6)
	LFD	a4,  3 * SIZE(AO6)

	FMADD	y13, alpha5, a5, y13
	FMADD	y14, alpha5, a6, y14
	FMADD	y15, alpha5, a7, y15
	FMADD	y16, alpha5, a8, y16

	LFD	a5,  4 * SIZE(AO6)
	LFD	a6,  5 * SIZE(AO6)
	LFD	a7,  6 * SIZE(AO6)
	LFD	a8,  7 * SIZE(AO6)

	FMADD	y01, alpha6, a1, y01
	FMADD	y02, alpha6, a2, y02
	FMADD	y03, alpha6, a3, y03
	FMADD	y04, alpha6, a4, y04

	LFD	a1,  8 * SIZE(AO6)
	LFD	a2,  9 * SIZE(AO6)
	LFD	a3, 10 * SIZE(AO6)
	LFD	a4, 11 * SIZE(AO6)

	FMADD	y05, alpha6, a5, y05
	FMADD	y06, alpha6, a6, y06
	FMADD	y07, alpha6, a7, y07
	FMADD	y08, alpha6, a8, y08

	LFD	a5, 12 * SIZE(AO6)
	LFD	a6, 13 * SIZE(AO6)
	LFD	a7, 14 * SIZE(AO6)
	LFD	a8, 15 * SIZE(AO6)

	addi	AO6, AO6, 16 * SIZE
	nop
	nop
	DCBT(AO6, PREA)

	FMADD	y09, alpha6, a1, y09
	FMADD	y10, alpha6, a2, y10
	FMADD	y11, alpha6, a3, y11
	FMADD	y12, alpha6, a4, y12

	LFD	a1,  0 * SIZE(AO7)
	LFD	a2,  1 * SIZE(AO7)
	LFD	a3,  2 * SIZE(AO7)
	LFD	a4,  3 * SIZE(AO7)

	FMADD	y13, alpha6, a5, y13
	FMADD	y14, alpha6, a6, y14
	FMADD	y15, alpha6, a7, y15
	FMADD	y16, alpha6, a8, y16

	LFD	a5,  4 * SIZE(AO7)
	LFD	a6,  5 * SIZE(AO7)
	LFD	a7,  6 * SIZE(AO7)
	LFD	a8,  7 * SIZE(AO7)

	FMADD	y01, alpha7, a1, y01
	FMADD	y02, alpha7, a2, y02
	FMADD	y03, alpha7, a3, y03
	FMADD	y04, alpha7, a4, y04

	LFD	a1,  8 * SIZE(AO7)
	LFD	a2,  9 * SIZE(AO7)
	LFD	a3, 10 * SIZE(AO7)
	LFD	a4, 11 * SIZE(AO7)

	FMADD	y05, alpha7, a5, y05
	FMADD	y06, alpha7, a6, y06
	FMADD	y07, alpha7, a7, y07
	FMADD	y08, alpha7, a8, y08

	LFD	a5, 12 * SIZE(AO7)
	LFD	a6, 13 * SIZE(AO7)
	LFD	a7, 14 * SIZE(AO7)
	LFD	a8, 15 * SIZE(AO7)

	addi	AO7, AO7, 16 * SIZE
	nop
	nop
	DCBT(AO7, PREA)

	FMADD	y09, alpha7, a1, y09
	FMADD	y10, alpha7, a2, y10
	FMADD	y11, alpha7, a3, y11
	FMADD	y12, alpha7, a4, y12

	LFD	a1,  0 * SIZE(AO8)
	LFD	a2,  1 * SIZE(AO8)
	LFD	a3,  2 * SIZE(AO8)
	LFD	a4,  3 * SIZE(AO8)

	FMADD	y13, alpha7, a5, y13
	FMADD	y14, alpha7, a6, y14
	FMADD	y15, alpha7, a7, y15
	FMADD	y16, alpha7, a8, y16

	LFD	a5,  4 * SIZE(AO8)
	LFD	a6,  5 * SIZE(AO8)
	LFD	a7,  6 * SIZE(AO8)
	LFD	a8,  7 * SIZE(AO8)

	FMADD	y01, alpha8, a1, y01
	FMADD	y02, alpha8, a2, y02
	FMADD	y03, alpha8, a3, y03
	FMADD	y04, alpha8, a4, y04

	LFD	a1,  8 * SIZE(AO8)
	LFD	a2,  9 * SIZE(AO8)
	LFD	a3, 10 * SIZE(AO8)
	LFD	a4, 11 * SIZE(AO8)

	FMADD	y05, alpha8, a5, y05
	FMADD	y06, alpha8, a6, y06
	FMADD	y07, alpha8, a7, y07
	FMADD	y08, alpha8, a8, y08

	LFD	a5, 12 * SIZE(AO8)
	LFD	a6, 13 * SIZE(AO8)
	LFD	a7, 14 * SIZE(AO8)
	LFD	a8, 15 * SIZE(AO8)

	addi	AO8, AO8, 16 * SIZE
	nop
	nop
	DCBT(AO8, PREA)

	FMADD	y09, alpha8, a1, y09
	FMADD	y10, alpha8, a2, y10
	FMADD	y11, alpha8, a3, y11
	FMADD	y12, alpha8, a4, y12

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)

	FMADD	y13, alpha8, a5, y13
	FMADD	y14, alpha8, a6, y14
	FMADD	y15, alpha8, a7, y15
	FMADD	y16, alpha8, a8, y16

	LFD	a5,  4 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	STFD	y01,  0 * SIZE(Y1)
	STFD	y02,  1 * SIZE(Y1)
	STFD	y03,  2 * SIZE(Y1)
	STFD	y04,  3 * SIZE(Y1)

	LFD	y01, 16 * SIZE(Y1)
	LFD	y02, 17 * SIZE(Y1)
	LFD	y03, 18 * SIZE(Y1)
	LFD	y04, 19 * SIZE(Y1)

	DCBT(Y1, PREC)
	bdz	LL(13)
	.align 4

LL(12):
	FMADD	y01, alpha1, a1, y01
	FMADD	y02, alpha1, a2, y02
	FMADD	y03, alpha1, a3, y03
	FMADD	y04, alpha1, a4, y04

	LFD	a1,  8 * SIZE(AO1)
	LFD	a2,  9 * SIZE(AO1)
	LFD	a3, 10 * SIZE(AO1)
	LFD	a4, 11 * SIZE(AO1)

	STFD	y05,  4 * SIZE(Y1)
	STFD	y06,  5 * SIZE(Y1)
	STFD	y07,  6 * SIZE(Y1)
	STFD	y08,  7 * SIZE(Y1)

	LFD	y05, 20 * SIZE(Y1)
	LFD	y06, 21 * SIZE(Y1)
	LFD	y07, 22 * SIZE(Y1)
	LFD	y08, 23 * SIZE(Y1)

	FMADD	y05, alpha1, a5, y05
	FMADD	y06, alpha1, a6, y06
	FMADD	y07, alpha1, a7, y07
	FMADD	y08, alpha1, a8, y08

	LFD	a5, 12 * SIZE(AO1)
	LFD	a6, 13 * SIZE(AO1)
	LFD	a7, 14 * SIZE(AO1)
	LFD	a8, 15 * SIZE(AO1)

	STFD	y09,  8 * SIZE(Y1)
	STFD	y10,  9 * SIZE(Y1)
	STFD	y11, 10 * SIZE(Y1)
	STFD	y12, 11 * SIZE(Y1)

	LFD	y09, 24 * SIZE(Y1)
	LFD	y10, 25 * SIZE(Y1)
	LFD	y11, 26 * SIZE(Y1)
	LFD	y12, 27 * SIZE(Y1)

	FMADD	y09, alpha1, a1, y09
	FMADD	y10, alpha1, a2, y10
	FMADD	y11, alpha1, a3, y11
	FMADD	y12, alpha1, a4, y12

	LFD	a1,  0 * SIZE(AO2)
	LFD	a2,  1 * SIZE(AO2)
	LFD	a3,  2 * SIZE(AO2)
	LFD	a4,  3 * SIZE(AO2)

	STFD	y13, 12 * SIZE(Y1)
	STFD	y14, 13 * SIZE(Y1)
	STFD	y15, 14 * SIZE(Y1)
	STFD	y16, 15 * SIZE(Y1)

	LFD	y13, 28 * SIZE(Y1)
	LFD	y14, 29 * SIZE(Y1)
	LFD	y15, 30 * SIZE(Y1)
	LFD	y16, 31 * SIZE(Y1)

	FMADD	y13, alpha1, a5, y13
	FMADD	y14, alpha1, a6, y14
	FMADD	y15, alpha1, a7, y15
	FMADD	y16, alpha1, a8, y16

	LFD	a5,  4 * SIZE(AO2)
	LFD	a6,  5 * SIZE(AO2)
	LFD	a7,  6 * SIZE(AO2)
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2, a1, y01
	FMADD	y02, alpha2, a2, y02
	FMADD	y03, alpha2, a3, y03
	FMADD	y04, alpha2, a4, y04

	LFD	a1,  8 * SIZE(AO2)
	LFD	a2,  9 * SIZE(AO2)
	LFD	a3, 10 * SIZE(AO2)
	LFD	a4, 11 * SIZE(AO2)

	FMADD	y05, alpha2, a5, y05
	FMADD	y06, alpha2, a6, y06
	FMADD	y07, alpha2, a7, y07
	FMADD	y08, alpha2, a8, y08

	LFD	a5, 12 * SIZE(AO2)
	LFD	a6, 13 * SIZE(AO2)
	LFD	a7, 14 * SIZE(AO2)
	LFD	a8, 15 * SIZE(AO2)

	FMADD	y09, alpha2, a1, y09
	FMADD	y10, alpha2, a2, y10
	FMADD	y11, alpha2, a3, y11
	FMADD	y12, alpha2, a4, y12

	LFD	a1,  0 * SIZE(AO3)
	LFD	a2,  1 * SIZE(AO3)
	LFD	a3,  2 * SIZE(AO3)
	LFD	a4,  3 * SIZE(AO3)

	FMADD	y13, alpha2, a5, y13
	FMADD	y14, alpha2, a6, y14
	FMADD	y15, alpha2, a7, y15
	FMADD	y16, alpha2, a8, y16

	LFD	a5,  4 * SIZE(AO3)
	LFD	a6,  5 * SIZE(AO3)
	LFD	a7,  6 * SIZE(AO3)
	LFD	a8,  7 * SIZE(AO3)

	FMADD	y01, alpha3, a1, y01
	FMADD	y02, alpha3, a2, y02
	FMADD	y03, alpha3, a3, y03
	FMADD	y04, alpha3, a4, y04

	LFD	a1,  8 * SIZE(AO3)
	LFD	a2,  9 * SIZE(AO3)
	LFD	a3, 10 * SIZE(AO3)
	LFD	a4, 11 * SIZE(AO3)

	FMADD	y05, alpha3, a5, y05
	FMADD	y06, alpha3, a6, y06
	FMADD	y07, alpha3, a7, y07
	FMADD	y08, alpha3, a8, y08

	LFD	a5, 12 * SIZE(AO3)
	LFD	a6, 13 * SIZE(AO3)
	LFD	a7, 14 * SIZE(AO3)
	LFD	a8, 15 * SIZE(AO3)

	FMADD	y09, alpha3, a1, y09
	FMADD	y10, alpha3, a2, y10
	FMADD	y11, alpha3, a3, y11
	FMADD	y12, alpha3, a4, y12

	LFD	a1,  0 * SIZE(AO4)
	LFD	a2,  1 * SIZE(AO4)
	LFD	a3,  2 * SIZE(AO4)
	LFD	a4,  3 * SIZE(AO4)

	FMADD	y13, alpha3, a5, y13
	FMADD	y14, alpha3, a6, y14
	FMADD	y15, alpha3, a7, y15
	FMADD	y16, alpha3, a8, y16

	LFD	a5,  4 * SIZE(AO4)
	LFD	a6,  5 * SIZE(AO4)
	LFD	a7,  6 * SIZE(AO4)
	LFD	a8,  7 * SIZE(AO4)

	FMADD	y01, alpha4, a1, y01
	FMADD	y02, alpha4, a2, y02
	FMADD	y03, alpha4, a3, y03
	FMADD	y04, alpha4, a4, y04

	LFD	a1,  8 * SIZE(AO4)
	LFD	a2,  9 * SIZE(AO4)
	LFD	a3, 10 * SIZE(AO4)
	LFD	a4, 11 * SIZE(AO4)

	FMADD	y05, alpha4, a5, y05
	FMADD	y06, alpha4, a6, y06
	FMADD	y07, alpha4, a7, y07
	FMADD	y08, alpha4, a8, y08

	LFD	a5, 12 * SIZE(AO4)
	LFD	a6, 13 * SIZE(AO4)
	LFD	a7, 14 * SIZE(AO4)
	LFD	a8, 15 * SIZE(AO4)

	addi	AO1, AO1, 16 * SIZE
	addi	AO2, AO2, 16 * SIZE
	addi	AO3, AO3, 16 * SIZE
	addi	AO4, AO4, 16 * SIZE

	DCBT(AO1, PREA)
	DCBT(AO2, PREA)
	DCBT(AO3, PREA)
	DCBT(AO4, PREA)

	FMADD	y09, alpha4, a1, y09
	FMADD	y10, alpha4, a2, y10
	FMADD	y11, alpha4, a3, y11
	FMADD	y12, alpha4, a4, y12

	LFD	a1,  0 * SIZE(AO5)
	LFD	a2,  1 * SIZE(AO5)
	LFD	a3,  2 * SIZE(AO5)
	LFD	a4,  3 * SIZE(AO5)

	FMADD	y13, alpha4, a5, y13
	FMADD	y14, alpha4, a6, y14
	FMADD	y15, alpha4, a7, y15
	FMADD	y16, alpha4, a8, y16

	LFD	a5,  4 * SIZE(AO5)
	LFD	a6,  5 * SIZE(AO5)
	LFD	a7,  6 * SIZE(AO5)
	LFD	a8,  7 * SIZE(AO5)

	FMADD	y01, alpha5, a1, y01
	FMADD	y02, alpha5, a2, y02
	FMADD	y03, alpha5, a3, y03
	FMADD	y04, alpha5, a4, y04

	LFD	a1,  8 * SIZE(AO5)
	LFD	a2,  9 * SIZE(AO5)
	LFD	a3, 10 * SIZE(AO5)
	LFD	a4, 11 * SIZE(AO5)

	FMADD	y05, alpha5, a5, y05
	FMADD	y06, alpha5, a6, y06
	FMADD	y07, alpha5, a7, y07
	FMADD	y08, alpha5, a8, y08

	LFD	a5, 12 * SIZE(AO5)
	LFD	a6, 13 * SIZE(AO5)
	LFD	a7, 14 * SIZE(AO5)
	LFD	a8, 15 * SIZE(AO5)

	FMADD	y09, alpha5, a1, y09
	FMADD	y10, alpha5, a2, y10
	FMADD	y11, alpha5, a3, y11
	FMADD	y12, alpha5, a4, y12

	LFD	a1,  0 * SIZE(AO6)
	LFD	a2,  1 * SIZE(AO6)
	LFD	a3,  2 * SIZE(AO6)
	LFD	a4,  3 * SIZE(AO6)

	FMADD	y13, alpha5, a5, y13
	FMADD	y14, alpha5, a6, y14
	FMADD	y15, alpha5, a7, y15
	FMADD	y16, alpha5, a8, y16

	LFD	a5,  4 * SIZE(AO6)
	LFD	a6,  5 * SIZE(AO6)
	LFD	a7,  6 * SIZE(AO6)
	LFD	a8,  7 * SIZE(AO6)

	FMADD	y01, alpha6, a1, y01
	FMADD	y02, alpha6, a2, y02
	FMADD	y03, alpha6, a3, y03
	FMADD	y04, alpha6, a4, y04

	LFD	a1,  8 * SIZE(AO6)
	LFD	a2,  9 * SIZE(AO6)
	LFD	a3, 10 * SIZE(AO6)
	LFD	a4, 11 * SIZE(AO6)

	FMADD	y05, alpha6, a5, y05
	FMADD	y06, alpha6, a6, y06
	FMADD	y07, alpha6, a7, y07
	FMADD	y08, alpha6, a8, y08

	LFD	a5, 12 * SIZE(AO6)
	LFD	a6, 13 * SIZE(AO6)
	LFD	a7, 14 * SIZE(AO6)
	LFD	a8, 15 * SIZE(AO6)

	FMADD	y09, alpha6, a1, y09
	FMADD	y10, alpha6, a2, y10
	FMADD	y11, alpha6, a3, y11
	FMADD	y12, alpha6, a4, y12

	LFD	a1,  0 * SIZE(AO7)
	LFD	a2,  1 * SIZE(AO7)
	LFD	a3,  2 * SIZE(AO7)
	LFD	a4,  3 * SIZE(AO7)

	FMADD	y13, alpha6, a5, y13
	FMADD	y14, alpha6, a6, y14
	FMADD	y15, alpha6, a7, y15
	FMADD	y16, alpha6, a8, y16

	LFD	a5,  4 * SIZE(AO7)
	LFD	a6,  5 * SIZE(AO7)
	LFD	a7,  6 * SIZE(AO7)
	LFD	a8,  7 * SIZE(AO7)

	FMADD	y01, alpha7, a1, y01
	FMADD	y02, alpha7, a2, y02
	FMADD	y03, alpha7, a3, y03
	FMADD	y04, alpha7, a4, y04

	LFD	a1,  8 * SIZE(AO7)
	LFD	a2,  9 * SIZE(AO7)
	LFD	a3, 10 * SIZE(AO7)
	LFD	a4, 11 * SIZE(AO7)

	FMADD	y05, alpha7, a5, y05
	FMADD	y06, alpha7, a6, y06
	FMADD	y07, alpha7, a7, y07
	FMADD	y08, alpha7, a8, y08

	LFD	a5, 12 * SIZE(AO7)
	LFD	a6, 13 * SIZE(AO7)
	LFD	a7, 14 * SIZE(AO7)
	LFD	a8, 15 * SIZE(AO7)

	FMADD	y09, alpha7, a1, y09
	FMADD	y10, alpha7, a2, y10
	FMADD	y11, alpha7, a3, y11
	FMADD	y12, alpha7, a4, y12

	LFD	a1,  0 * SIZE(AO8)
	LFD	a2,  1 * SIZE(AO8)
	LFD	a3,  2 * SIZE(AO8)
	LFD	a4,  3 * SIZE(AO8)

	FMADD	y13, alpha7, a5, y13
	FMADD	y14, alpha7, a6, y14
	FMADD	y15, alpha7, a7, y15
	FMADD	y16, alpha7, a8, y16

	LFD	a5,  4 * SIZE(AO8)
	LFD	a6,  5 * SIZE(AO8)
	LFD	a7,  6 * SIZE(AO8)
	LFD	a8,  7 * SIZE(AO8)

	FMADD	y01, alpha8, a1, y01
	FMADD	y02, alpha8, a2, y02
	FMADD	y03, alpha8, a3, y03
	FMADD	y04, alpha8, a4, y04

	LFD	a1,  8 * SIZE(AO8)
	LFD	a2,  9 * SIZE(AO8)
	LFD	a3, 10 * SIZE(AO8)
	LFD	a4, 11 * SIZE(AO8)

	FMADD	y05, alpha8, a5, y05
	FMADD	y06, alpha8, a6, y06
	FMADD	y07, alpha8, a7, y07
	FMADD	y08, alpha8, a8, y08

	LFD	a5, 12 * SIZE(AO8)
	LFD	a6, 13 * SIZE(AO8)
	LFD	a7, 14 * SIZE(AO8)
	LFD	a8, 15 * SIZE(AO8)

	addi	AO5, AO5, 16 * SIZE
	addi	AO6, AO6, 16 * SIZE
	addi	AO7, AO7, 16 * SIZE
	addi	AO8, AO8, 16 * SIZE

	DCBT(AO5, PREA)
	DCBT(AO6, PREA)
	DCBT(AO7, PREA)
	DCBT(AO8, PREA)

	FMADD	y09, alpha8, a1, y09
	FMADD	y10, alpha8, a2, y10
	FMADD	y11, alpha8, a3, y11
	FMADD	y12, alpha8, a4, y12

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)

	FMADD	y13, alpha8, a5, y13
	FMADD	y14, alpha8, a6, y14
	FMADD	y15, alpha8, a7, y15
	FMADD	y16, alpha8, a8, y16

	LFD	a5,  4 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	STFD	y01, 16 * SIZE(Y1)
	STFD	y02, 17 * SIZE(Y1)
	STFD	y03, 18 * SIZE(Y1)
	STFD	y04, 19 * SIZE(Y1)

	LFD	y01, 32 * SIZE(Y1)
	LFD	y02, 33 * SIZE(Y1)
	LFD	y03, 34 * SIZE(Y1)
	LFD	y04, 35 * SIZE(Y1)

	DCBT(Y1, PREC)
	addi	Y1, Y1, 16 * SIZE
	bdnz	LL(12)
	.align 4

LL(13):
	STFD	y05,  4 * SIZE(Y1)
	STFD	y06,  5 * SIZE(Y1)
	STFD	y07,  6 * SIZE(Y1)
	STFD	y08,  7 * SIZE(Y1)

	STFD	y09,  8 * SIZE(Y1)
	STFD	y10,  9 * SIZE(Y1)
	STFD	y11, 10 * SIZE(Y1)
	STFD	y12, 11 * SIZE(Y1)

	STFD	y13, 12 * SIZE(Y1)
	STFD	y14, 13 * SIZE(Y1)
	STFD	y15, 14 * SIZE(Y1)
	STFD	y16, 15 * SIZE(Y1)

	addi	Y1, Y1, 16 * SIZE
	.align 4

LL(15):
	andi.	r0, M, 15
	ble	LL(19)

	andi.	r0, M, 8
	ble	LL(16)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	LFD	y03, 2 * SIZE(Y1)
	LFD	y04, 3 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)

	LFD	y05, 4 * SIZE(Y1)
	LFD	y06, 5 * SIZE(Y1)
	LFD	y07, 6 * SIZE(Y1)
	LFD	y08, 7 * SIZE(Y1)

	LFD	a5,  4 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	FMADD	y01, alpha1, a1, y01
	LFD	a1,  0 * SIZE(AO2)
	FMADD	y02, alpha1, a2, y02
	LFD	a2,  1 * SIZE(AO2)
	FMADD	y03, alpha1, a3, y03
	LFD	a3,  2 * SIZE(AO2)
	FMADD	y04, alpha1, a4, y04
	LFD	a4,  3 * SIZE(AO2)

	FMADD	y05, alpha1, a5, y05
	LFD	a5,  4 * SIZE(AO2)
	FMADD	y06, alpha1, a6, y06
	LFD	a6,  5 * SIZE(AO2)
	FMADD	y07, alpha1, a7, y07
	LFD	a7,  6 * SIZE(AO2)
	FMADD	y08, alpha1, a8, y08
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2, a1, y01
	LFD	a1,  0 * SIZE(AO3)
	FMADD	y02, alpha2, a2, y02
	LFD	a2,  1 * SIZE(AO3)

	FMADD	y03, alpha2, a3, y03
	LFD	a3,  2 * SIZE(AO3)
	FMADD	y04, alpha2, a4, y04
	LFD	a4,  3 * SIZE(AO3)

	FMADD	y05, alpha2, a5, y05
	LFD	a5,  4 * SIZE(AO3)
	FMADD	y06, alpha2, a6, y06
	LFD	a6,  5 * SIZE(AO3)

	FMADD	y07, alpha2, a7, y07
	LFD	a7,  6 * SIZE(AO3)
	FMADD	y08, alpha2, a8, y08
	LFD	a8,  7 * SIZE(AO3)

	FMADD	y01, alpha3, a1, y01
	LFD	a1,  0 * SIZE(AO4)
	FMADD	y02, alpha3, a2, y02
	LFD	a2,  1 * SIZE(AO4)

	FMADD	y03, alpha3, a3, y03
	LFD	a3,  2 * SIZE(AO4)
	FMADD	y04, alpha3, a4, y04
	LFD	a4,  3 * SIZE(AO4)

	FMADD	y05, alpha3, a5, y05
	LFD	a5,  4 * SIZE(AO4)
	FMADD	y06, alpha3, a6, y06
	LFD	a6,  5 * SIZE(AO4)

	FMADD	y07, alpha3, a7, y07
	LFD	a7,  6 * SIZE(AO4)
	FMADD	y08, alpha3, a8, y08
	LFD	a8,  7 * SIZE(AO4)

	FMADD	y01, alpha4, a1, y01
	LFD	a1,  0 * SIZE(AO5)
	FMADD	y02, alpha4, a2, y02
	LFD	a2,  1 * SIZE(AO5)

	FMADD	y03, alpha4, a3, y03
	LFD	a3,  2 * SIZE(AO5)
	FMADD	y04, alpha4, a4, y04
	LFD	a4,  3 * SIZE(AO5)

	FMADD	y05, alpha4, a5, y05
	LFD	a5,  4 * SIZE(AO5)
	FMADD	y06, alpha4, a6, y06
	LFD	a6,  5 * SIZE(AO5)

	FMADD	y07, alpha4, a7, y07
	LFD	a7,  6 * SIZE(AO5)
	FMADD	y08, alpha4, a8, y08
	LFD	a8,  7 * SIZE(AO5)

	FMADD	y01, alpha5, a1, y01
	LFD	a1,  0 * SIZE(AO6)
	FMADD	y02, alpha5, a2, y02
	LFD	a2,  1 * SIZE(AO6)

	FMADD	y03, alpha5, a3, y03
	LFD	a3,  2 * SIZE(AO6)
	FMADD	y04, alpha5, a4, y04
	LFD	a4,  3 * SIZE(AO6)

	FMADD	y05, alpha5, a5, y05
	LFD	a5,  4 * SIZE(AO6)
	FMADD	y06, alpha5, a6, y06
	LFD	a6,  5 * SIZE(AO6)

	FMADD	y07, alpha5, a7, y07
	LFD	a7,  6 * SIZE(AO6)
	FMADD	y08, alpha5, a8, y08
	LFD	a8,  7 * SIZE(AO6)

	FMADD	y01, alpha6, a1, y01
	LFD	a1,  0 * SIZE(AO7)
	FMADD	y02, alpha6, a2, y02
	LFD	a2,  1 * SIZE(AO7)

	FMADD	y03, alpha6, a3, y03
	LFD	a3,  2 * SIZE(AO7)
	FMADD	y04, alpha6, a4, y04
	LFD	a4,  3 * SIZE(AO7)

	FMADD	y05, alpha6, a5, y05
	LFD	a5,  4 * SIZE(AO7)
	FMADD	y06, alpha6, a6, y06
	LFD	a6,  5 * SIZE(AO7)

	FMADD	y07, alpha6, a7, y07
	LFD	a7,  6 * SIZE(AO7)
	FMADD	y08, alpha6, a8, y08
	LFD	a8,  7 * SIZE(AO7)

	FMADD	y01, alpha7, a1, y01
	LFD	a1,  0 * SIZE(AO8)
	FMADD	y02, alpha7, a2, y02
	LFD	a2,  1 * SIZE(AO8)

	FMADD	y03, alpha7, a3, y03
	LFD	a3,  2 * SIZE(AO8)
	FMADD	y04, alpha7, a4, y04
	LFD	a4,  3 * SIZE(AO8)

	FMADD	y05, alpha7, a5, y05
	LFD	a5,  4 * SIZE(AO8)
	FMADD	y06, alpha7, a6, y06
	LFD	a6,  5 * SIZE(AO8)

	FMADD	y07, alpha7, a7, y07
	LFD	a7,  6 * SIZE(AO8)
	FMADD	y08, alpha7, a8, y08
	LFD	a8,  7 * SIZE(AO8)

	FMADD	y01, alpha8, a1, y01
	addi	AO1, AO1, 8 * SIZE
	FMADD	y02, alpha8, a2, y02
	addi	AO2, AO2, 8 * SIZE
	FMADD	y03, alpha8, a3, y03
	addi	AO3, AO3, 8 * SIZE
	FMADD	y04, alpha8, a4, y04
	addi	AO4, AO4, 8 * SIZE

	STFD	y01, 0 * SIZE(Y1)
	STFD	y02, 1 * SIZE(Y1)
	STFD	y03, 2 * SIZE(Y1)
	STFD	y04, 3 * SIZE(Y1)

	FMADD	y05, alpha8, a5, y05
	addi	AO5, AO5, 8 * SIZE
	FMADD	y06, alpha8, a6, y06
	addi	AO6, AO6, 8 * SIZE
	FMADD	y07, alpha8, a7, y07
	addi	AO7, AO7, 8 * SIZE
	FMADD	y08, alpha8, a8, y08
	addi	AO8, AO8, 8 * SIZE

	STFD	y05, 4 * SIZE(Y1)
	STFD	y06, 5 * SIZE(Y1)
	STFD	y07, 6 * SIZE(Y1)
	STFD	y08, 7 * SIZE(Y1)
	addi	Y1, Y1, 8 * SIZE
	.align 4

LL(16):
	andi.	r0, M, 4
	ble	LL(17)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	LFD	y03, 2 * SIZE(Y1)
	LFD	y04, 3 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)

	LFD	a5,  0 * SIZE(AO2)
	LFD	a6,  1 * SIZE(AO2)
	LFD	a7,  2 * SIZE(AO2)
	LFD	a8,  3 * SIZE(AO2)

	FMADD	y01, alpha1, a1, y01
	LFD	a1,  0 * SIZE(AO3)
	FMADD	y02, alpha1, a2, y02
	LFD	a2,  1 * SIZE(AO3)
	FMADD	y03, alpha1, a3, y03
	LFD	a3,  2 * SIZE(AO3)
	FMADD	y04, alpha1, a4, y04
	LFD	a4,  3 * SIZE(AO3)

	FMADD	y01, alpha2, a5, y01
	LFD	a5,  0 * SIZE(AO4)
	FMADD	y02, alpha2, a6, y02
	LFD	a6,  1 * SIZE(AO4)
	FMADD	y03, alpha2, a7, y03
	LFD	a7,  2 * SIZE(AO4)
	FMADD	y04, alpha2, a8, y04
	LFD	a8,  3 * SIZE(AO4)

	FMADD	y01, alpha3, a1, y01
	LFD	a1,  0 * SIZE(AO5)
	FMADD	y02, alpha3, a2, y02
	LFD	a2,  1 * SIZE(AO5)
	FMADD	y03, alpha3, a3, y03
	LFD	a3,  2 * SIZE(AO5)
	FMADD	y04, alpha3, a4, y04
	LFD	a4,  3 * SIZE(AO5)

	FMADD	y01, alpha4, a5, y01
	LFD	a5,  0 * SIZE(AO6)
	FMADD	y02, alpha4, a6, y02
	LFD	a6,  1 * SIZE(AO6)
	FMADD	y03, alpha4, a7, y03
	LFD	a7,  2 * SIZE(AO6)
	FMADD	y04, alpha4, a8, y04
	LFD	a8,  3 * SIZE(AO6)

	FMADD	y01, alpha5, a1, y01
	LFD	a1,  0 * SIZE(AO7)
	FMADD	y02, alpha5, a2, y02
	LFD	a2,  1 * SIZE(AO7)
	FMADD	y03, alpha5, a3, y03
	LFD	a3,  2 * SIZE(AO7)
	FMADD	y04, alpha5, a4, y04
	LFD	a4,  3 * SIZE(AO7)

	FMADD	y01, alpha6, a5, y01
	LFD	a5,  0 * SIZE(AO8)
	FMADD	y02, alpha6, a6, y02
	LFD	a6,  1 * SIZE(AO8)
	FMADD	y03, alpha6, a7, y03
	LFD	a7,  2 * SIZE(AO8)
	FMADD	y04, alpha6, a8, y04
	LFD	a8,  3 * SIZE(AO8)

	FMADD	y01, alpha7, a1, y01
	addi	AO1, AO1, 4 * SIZE
	FMADD	y02, alpha7, a2, y02
	addi	AO2, AO2, 4 * SIZE
	FMADD	y03, alpha7, a3, y03
	addi	AO3, AO3, 4 * SIZE
	FMADD	y04, alpha7, a4, y04
	addi	AO4, AO4, 4 * SIZE

	FMADD	y01, alpha8, a5, y01
	addi	AO5, AO5, 4 * SIZE
	FMADD	y02, alpha8, a6, y02
	addi	AO6, AO6, 4 * SIZE
	FMADD	y03, alpha8, a7, y03
	addi	AO7, AO7, 4 * SIZE
	FMADD	y04, alpha8, a8, y04
	addi	AO8, AO8, 4 * SIZE

	STFD	y01, 0 * SIZE(Y1)
	STFD	y02, 1 * SIZE(Y1)
	STFD	y03, 2 * SIZE(Y1)
	STFD	y04, 3 * SIZE(Y1)
	addi	Y1, Y1, 4 * SIZE
	.align 4

LL(17):
	andi.	r0, M, 2
	ble	LL(18)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  0 * SIZE(AO2)
	LFD	a4,  1 * SIZE(AO2)

	LFD	a5,  0 * SIZE(AO3)
	LFD	a6,  1 * SIZE(AO3)
	LFD	a7,  0 * SIZE(AO4)
	LFD	a8,  1 * SIZE(AO4)

	FMADD	y01, alpha1, a1, y01
	LFD	a1,  0 * SIZE(AO5)
	FMADD	y02, alpha1, a2, y02
	LFD	a2,  1 * SIZE(AO5)
	FMADD	y01, alpha2, a3, y01
	LFD	a3,  0 * SIZE(AO6)
	FMADD	y02, alpha2, a4, y02
	LFD	a4,  1 * SIZE(AO6)

	FMADD	y01, alpha3, a5, y01
	LFD	a5,  0 * SIZE(AO7)
	FMADD	y02, alpha3, a6, y02
	LFD	a6,  1 * SIZE(AO7)
	FMADD	y01, alpha4, a7, y01
	LFD	a7,  0 * SIZE(AO8)
	FMADD	y02, alpha4, a8, y02
	LFD	a8,  1 * SIZE(AO8)

	FMADD	y01, alpha5, a1, y01
	addi	AO1, AO1, 2 * SIZE
	FMADD	y02, alpha5, a2, y02
	addi	AO2, AO2, 2 * SIZE
	FMADD	y01, alpha6, a3, y01
	addi	AO3, AO3, 2 * SIZE
	FMADD	y02, alpha6, a4, y02
	addi	AO4, AO4, 2 * SIZE

	FMADD	y01, alpha7, a5, y01
	addi	AO5, AO5, 2 * SIZE
	FMADD	y02, alpha7, a6, y02
	addi	AO6, AO6, 2 * SIZE
	FMADD	y01, alpha8, a7, y01
	addi	AO7, AO7, 2 * SIZE
	FMADD	y02, alpha8, a8, y02
	addi	AO8, AO8, 2 * SIZE

	STFD	y01, 0 * SIZE(Y1)
	STFD	y02, 1 * SIZE(Y1)
	addi	Y1, Y1, 2 * SIZE
	.align 4

LL(18):
	andi.	r0, M, 1
	ble	LL(19)

	LFD	y01, 0 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  0 * SIZE(AO2)
	LFD	a3,  0 * SIZE(AO3)
	LFD	a4,  0 * SIZE(AO4)
	LFD	a5,  0 * SIZE(AO5)
	LFD	a6,  0 * SIZE(AO6)
	LFD	a7,  0 * SIZE(AO7)
	LFD	a8,  0 * SIZE(AO8)

	FMADD	y01, alpha1, a1, y01
	FMADD	y01, alpha2, a2, y01
	FMADD	y01, alpha3, a3, y01
	FMADD	y01, alpha4, a4, y01

	FMADD	y01, alpha5, a5, y01
	FMADD	y01, alpha6, a6, y01
	FMADD	y01, alpha7, a7, y01
	FMADD	y01, alpha8, a8, y01

	STFD	y01, 0 * SIZE(Y1)
	.align 4

LL(19):
	addi	J, J, -1
	lfd	alpha, ALPHA
	cmpi	cr0, 0, J, 0
	bgt	LL(11)
	.align 4

LL(20):
	andi.	J, N, 4
	mr	AO1, A
	add	AO2, A,   LDA
	ble	LL(30)
	.align 4

	LFD	alpha1, 0 * SIZE(X)
	add	X, X, INCX
	LFD	alpha2, 0 * SIZE(X)
	add	X, X, INCX
	LFD	alpha3, 0 * SIZE(X)
	add	X, X, INCX
	LFD	alpha4, 0 * SIZE(X)
	add	X, X, INCX

	FMUL	alpha1, alpha, alpha1
	add	AO3, AO2, LDA
	FMUL	alpha2, alpha, alpha2
	add	AO4, AO3, LDA
	FMUL	alpha3, alpha, alpha3
	add	A,   AO4, LDA
	FMUL	alpha4, alpha, alpha4
	mr	Y1, YY

	srawi.	r0,  M, 4
	mtspr	CTR, r0
	ble	LL(25)

	LFD	y01,  0 * SIZE(Y1)
	LFD	y02,  1 * SIZE(Y1)
	LFD	y03,  2 * SIZE(Y1)
	LFD	y04,  3 * SIZE(Y1)
	LFD	y05,  4 * SIZE(Y1)
	LFD	y06,  5 * SIZE(Y1)
	LFD	y07,  6 * SIZE(Y1)
	LFD	y08,  7 * SIZE(Y1)
	LFD	y09,  8 * SIZE(Y1)
	LFD	y10,  9 * SIZE(Y1)
	LFD	y11, 10 * SIZE(Y1)
	LFD	y12, 11 * SIZE(Y1)
	LFD	y13, 12 * SIZE(Y1)
	LFD	y14, 13 * SIZE(Y1)
	LFD	y15, 14 * SIZE(Y1)
	LFD	y16, 15 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)
	bdz	LL(23)
	.align 4

LL(22):
	FMADD	y01, alpha1, a1, y01
	LFD	a1,  8 * SIZE(AO1)
	FMADD	y02, alpha1, a2, y02
	LFD	a2,  9 * SIZE(AO1)
	FMADD	y03, alpha1, a3, y03
	LFD	a3, 10 * SIZE(AO1)
	FMADD	y04, alpha1, a4, y04
	LFD	a4, 11 * SIZE(AO1)

	FMADD	y05, alpha1, a5, y05
	LFD	a5, 12 * SIZE(AO1)
	FMADD	y06, alpha1, a6, y06
	LFD	a6, 13 * SIZE(AO1)
	FMADD	y07, alpha1, a7, y07
	LFD	a7, 14 * SIZE(AO1)
	FMADD	y08, alpha1, a8, y08
	LFD	a8, 15 * SIZE(AO1)

	FMADD	y09, alpha1, a1, y09
	LFD	a1,  0 * SIZE(AO2)
	FMADD	y10, alpha1, a2, y10
	LFD	a2,  1 * SIZE(AO2)
	FMADD	y11, alpha1, a3, y11
	LFD	a3,  2 * SIZE(AO2)
	FMADD	y12, alpha1, a4, y12
	LFD	a4,  3 * SIZE(AO2)

	FMADD	y13, alpha1, a5, y13
	LFD	a5,  4 * SIZE(AO2)
	FMADD	y14, alpha1, a6, y14
	LFD	a6,  5 * SIZE(AO2)
	FMADD	y15, alpha1, a7, y15
	LFD	a7,  6 * SIZE(AO2)
	FMADD	y16, alpha1, a8, y16
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2, a1, y01
	LFD	a1,  8 * SIZE(AO2)
	FMADD	y02, alpha2, a2, y02
	LFD	a2,  9 * SIZE(AO2)
	FMADD	y03, alpha2, a3, y03
	LFD	a3, 10 * SIZE(AO2)
	FMADD	y04, alpha2, a4, y04
	LFD	a4, 11 * SIZE(AO2)

	FMADD	y05, alpha2, a5, y05
	LFD	a5, 12 * SIZE(AO2)
	FMADD	y06, alpha2, a6, y06
	LFD	a6, 13 * SIZE(AO2)
	FMADD	y07, alpha2, a7, y07
	LFD	a7, 14 * SIZE(AO2)
	FMADD	y08, alpha2, a8, y08
	LFD	a8, 15 * SIZE(AO2)

	addi	AO1, AO1, 16 * SIZE
	addi	AO2, AO2, 16 * SIZE
	DCBT(AO1, PREA)
	DCBT(AO2, PREA)

	FMADD	y09, alpha2, a1, y09
	LFD	a1,  0 * SIZE(AO3)
	FMADD	y10, alpha2, a2, y10
	LFD	a2,  1 * SIZE(AO3)
	FMADD	y11, alpha2, a3, y11
	LFD	a3,  2 * SIZE(AO3)
	FMADD	y12, alpha2, a4, y12
	LFD	a4,  3 * SIZE(AO3)

	FMADD	y13, alpha2, a5, y13
	LFD	a5,  4 * SIZE(AO3)
	FMADD	y14, alpha2, a6, y14
	LFD	a6,  5 * SIZE(AO3)
	FMADD	y15, alpha2, a7, y15
	LFD	a7,  6 * SIZE(AO3)
	FMADD	y16, alpha2, a8, y16
	LFD	a8,  7 * SIZE(AO3)

	FMADD	y01, alpha3, a1, y01
	LFD	a1,  8 * SIZE(AO3)
	FMADD	y02, alpha3, a2, y02
	LFD	a2,  9 * SIZE(AO3)
	FMADD	y03, alpha3, a3, y03
	LFD	a3, 10 * SIZE(AO3)
	FMADD	y04, alpha3, a4, y04
	LFD	a4, 11 * SIZE(AO3)

	FMADD	y05, alpha3, a5, y05
	LFD	a5, 12 * SIZE(AO3)
	FMADD	y06, alpha3, a6, y06
	LFD	a6, 13 * SIZE(AO3)
	FMADD	y07, alpha3, a7, y07
	LFD	a7, 14 * SIZE(AO3)
	FMADD	y08, alpha3, a8, y08
	LFD	a8, 15 * SIZE(AO3)

	FMADD	y09, alpha3, a1, y09
	LFD	a1,  0 * SIZE(AO4)
	FMADD	y10, alpha3, a2, y10
	LFD	a2,  1 * SIZE(AO4)
	FMADD	y11, alpha3, a3, y11
	LFD	a3,  2 * SIZE(AO4)
	FMADD	y12, alpha3, a4, y12
	LFD	a4,  3 * SIZE(AO4)

	FMADD	y13, alpha3, a5, y13
	LFD	a5,  4 * SIZE(AO4)
	FMADD	y14, alpha3, a6, y14
	LFD	a6,  5 * SIZE(AO4)
	FMADD	y15, alpha3, a7, y15
	LFD	a7,  6 * SIZE(AO4)
	FMADD	y16, alpha3, a8, y16
	LFD	a8,  7 * SIZE(AO4)

	FMADD	y01, alpha4, a1, y01
	LFD	a1,  8 * SIZE(AO4)
	FMADD	y02, alpha4, a2, y02
	LFD	a2,  9 * SIZE(AO4)
	FMADD	y03, alpha4, a3, y03
	LFD	a3, 10 * SIZE(AO4)
	FMADD	y04, alpha4, a4, y04
	LFD	a4, 11 * SIZE(AO4)

	STFD	y01,  0 * SIZE(Y1)
	STFD	y02,  1 * SIZE(Y1)
	STFD	y03,  2 * SIZE(Y1)
	STFD	y04,  3 * SIZE(Y1)

	LFD	y01, 16 * SIZE(Y1)
	LFD	y02, 17 * SIZE(Y1)
	LFD	y03, 18 * SIZE(Y1)
	LFD	y04, 19 * SIZE(Y1)

	FMADD	y05, alpha4, a5, y05
	LFD	a5, 12 * SIZE(AO4)
	FMADD	y06, alpha4, a6, y06
	LFD	a6, 13 * SIZE(AO4)
	FMADD	y07, alpha4, a7, y07
	LFD	a7, 14 * SIZE(AO4)
	FMADD	y08, alpha4, a8, y08
	LFD	a8, 15 * SIZE(AO4)

	STFD	y05,  4 * SIZE(Y1)
	STFD	y06,  5 * SIZE(Y1)
	STFD	y07,  6 * SIZE(Y1)
	STFD	y08,  7 * SIZE(Y1)

	LFD	y05, 20 * SIZE(Y1)
	LFD	y06, 21 * SIZE(Y1)
	LFD	y07, 22 * SIZE(Y1)
	LFD	y08, 23 * SIZE(Y1)

	addi	AO3, AO3, 16 * SIZE
	addi	AO4, AO4, 16 * SIZE
	DCBT(AO3, PREA)
	DCBT(AO4, PREA)

	FMADD	y09, alpha4, a1, y09
	LFD	a1,  0 * SIZE(AO1)
	FMADD	y10, alpha4, a2, y10
	LFD	a2,  1 * SIZE(AO1)
	FMADD	y11, alpha4, a3, y11
	LFD	a3,  2 * SIZE(AO1)
	FMADD	y12, alpha4, a4, y12
	LFD	a4,  3 * SIZE(AO1)

	STFD	y09,  8 * SIZE(Y1)
	STFD	y10,  9 * SIZE(Y1)
	STFD	y11, 10 * SIZE(Y1)
	STFD	y12, 11 * SIZE(Y1)

	LFD	y09, 24 * SIZE(Y1)
	LFD	y10, 25 * SIZE(Y1)
	LFD	y11, 26 * SIZE(Y1)
	LFD	y12, 27 * SIZE(Y1)

	FMADD	y13, alpha4, a5, y13
	LFD	a5,  4 * SIZE(AO1)
	FMADD	y14, alpha4, a6, y14
	LFD	a6,  5 * SIZE(AO1)
	FMADD	y15, alpha4, a7, y15
	LFD	a7,  6 * SIZE(AO1)
	FMADD	y16, alpha4, a8, y16
	LFD	a8,  7 * SIZE(AO1)

	STFD	y13, 12 * SIZE(Y1)
	STFD	y14, 13 * SIZE(Y1)
	STFD	y15, 14 * SIZE(Y1)
	STFD	y16, 15 * SIZE(Y1)

	LFD	y13, 28 * SIZE(Y1)
	LFD	y14, 29 * SIZE(Y1)
	LFD	y15, 30 * SIZE(Y1)
	LFD	y16, 31 * SIZE(Y1)

	addi	Y1, Y1, 16 * SIZE
	DCBT(Y1, PREC)
	bdnz	LL(22)
	.align 4

LL(23):
	FMADD	y01, alpha1, a1, y01
	LFD	a1,  8 * SIZE(AO1)
	FMADD	y02, alpha1, a2, y02
	LFD	a2,  9 * SIZE(AO1)
	FMADD	y03, alpha1, a3, y03
	LFD	a3, 10 * SIZE(AO1)
	FMADD	y04, alpha1, a4, y04
	LFD	a4, 11 * SIZE(AO1)

	FMADD	y05, alpha1, a5, y05
	LFD	a5, 12 * SIZE(AO1)
	FMADD	y06, alpha1, a6, y06
	LFD	a6, 13 * SIZE(AO1)
	FMADD	y07, alpha1, a7, y07
	LFD	a7, 14 * SIZE(AO1)
	FMADD	y08, alpha1, a8, y08
	LFD	a8, 15 * SIZE(AO1)

	FMADD	y09, alpha1, a1, y09
	LFD	a1,  0 * SIZE(AO2)
	FMADD	y10, alpha1, a2, y10
	LFD	a2,  1 * SIZE(AO2)
	FMADD	y11, alpha1, a3, y11
	LFD	a3,  2 * SIZE(AO2)
	FMADD	y12, alpha1, a4, y12
	LFD	a4,  3 * SIZE(AO2)

	FMADD	y13, alpha1, a5, y13
	LFD	a5,  4 * SIZE(AO2)
	FMADD	y14, alpha1, a6, y14
	LFD	a6,  5 * SIZE(AO2)
	FMADD	y15, alpha1, a7, y15
	LFD	a7,  6 * SIZE(AO2)
	FMADD	y16, alpha1, a8, y16
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2, a1, y01
	LFD	a1,  8 * SIZE(AO2)
	FMADD	y02, alpha2, a2, y02
	LFD	a2,  9 * SIZE(AO2)
	FMADD	y03, alpha2, a3, y03
	LFD	a3, 10 * SIZE(AO2)
	FMADD	y04, alpha2, a4, y04
	LFD	a4, 11 * SIZE(AO2)

	FMADD	y05, alpha2, a5, y05
	LFD	a5, 12 * SIZE(AO2)
	FMADD	y06, alpha2, a6, y06
	LFD	a6, 13 * SIZE(AO2)
	FMADD	y07, alpha2, a7, y07
	LFD	a7, 14 * SIZE(AO2)
	FMADD	y08, alpha2, a8, y08
	LFD	a8, 15 * SIZE(AO2)

	FMADD	y09, alpha2, a1, y09
	LFD	a1,  0 * SIZE(AO3)
	FMADD	y10, alpha2, a2, y10
	LFD	a2,  1 * SIZE(AO3)
	FMADD	y11, alpha2, a3, y11
	LFD	a3,  2 * SIZE(AO3)
	FMADD	y12, alpha2, a4, y12
	LFD	a4,  3 * SIZE(AO3)

	FMADD	y13, alpha2, a5, y13
	LFD	a5,  4 * SIZE(AO3)
	FMADD	y14, alpha2, a6, y14
	LFD	a6,  5 * SIZE(AO3)
	FMADD	y15, alpha2, a7, y15
	LFD	a7,  6 * SIZE(AO3)
	FMADD	y16, alpha2, a8, y16
	LFD	a8,  7 * SIZE(AO3)

	FMADD	y01, alpha3, a1, y01
	LFD	a1,  8 * SIZE(AO3)
	FMADD	y02, alpha3, a2, y02
	LFD	a2,  9 * SIZE(AO3)
	FMADD	y03, alpha3, a3, y03
	LFD	a3, 10 * SIZE(AO3)
	FMADD	y04, alpha3, a4, y04
	LFD	a4, 11 * SIZE(AO3)

	FMADD	y05, alpha3, a5, y05
	LFD	a5, 12 * SIZE(AO3)
	FMADD	y06, alpha3, a6, y06
	LFD	a6, 13 * SIZE(AO3)
	FMADD	y07, alpha3, a7, y07
	LFD	a7, 14 * SIZE(AO3)
	FMADD	y08, alpha3, a8, y08
	LFD	a8, 15 * SIZE(AO3)

	FMADD	y09, alpha3, a1, y09
	LFD	a1,  0 * SIZE(AO4)
	FMADD	y10, alpha3, a2, y10
	LFD	a2,  1 * SIZE(AO4)
	FMADD	y11, alpha3, a3, y11
	LFD	a3,  2 * SIZE(AO4)
	FMADD	y12, alpha3, a4, y12
	LFD	a4,  3 * SIZE(AO4)

	FMADD	y13, alpha3, a5, y13
	LFD	a5,  4 * SIZE(AO4)
	FMADD	y14, alpha3, a6, y14
	LFD	a6,  5 * SIZE(AO4)
	FMADD	y15, alpha3, a7, y15
	LFD	a7,  6 * SIZE(AO4)
	FMADD	y16, alpha3, a8, y16
	LFD	a8,  7 * SIZE(AO4)

	FMADD	y01, alpha4, a1, y01
	LFD	a1,  8 * SIZE(AO4)
	FMADD	y02, alpha4, a2, y02
	LFD	a2,  9 * SIZE(AO4)
	FMADD	y03, alpha4, a3, y03
	LFD	a3, 10 * SIZE(AO4)
	FMADD	y04, alpha4, a4, y04
	LFD	a4, 11 * SIZE(AO4)

	FMADD	y05, alpha4, a5, y05
	LFD	a5, 12 * SIZE(AO4)
	FMADD	y06, alpha4, a6, y06
	LFD	a6, 13 * SIZE(AO4)
	FMADD	y07, alpha4, a7, y07
	LFD	a7, 14 * SIZE(AO4)
	FMADD	y08, alpha4, a8, y08
	LFD	a8, 15 * SIZE(AO4)

	FMADD	y09, alpha4, a1, y09
	addi	AO1, AO1, 16 * SIZE
	FMADD	y10, alpha4, a2, y10
	addi	AO2, AO2, 16 * SIZE
	FMADD	y11, alpha4, a3, y11
	addi	AO3, AO3, 16 * SIZE
	FMADD	y12, alpha4, a4, y12
	addi	AO4, AO4, 16 * SIZE

	FMADD	y13, alpha4, a5, y13
	FMADD	y14, alpha4, a6, y14
	FMADD	y15, alpha4, a7, y15
	FMADD	y16, alpha4, a8, y16

	STFD	y01,  0 * SIZE(Y1)
	STFD	y02,  1 * SIZE(Y1)
	STFD	y03,  2 * SIZE(Y1)
	STFD	y04,  3 * SIZE(Y1)
	STFD	y05,  4 * SIZE(Y1)
	STFD	y06,  5 * SIZE(Y1)
	STFD	y07,  6 * SIZE(Y1)
	STFD	y08,  7 * SIZE(Y1)
	STFD	y09,  8 * SIZE(Y1)
	STFD	y10,  9 * SIZE(Y1)
	STFD	y11, 10 * SIZE(Y1)
	STFD	y12, 11 * SIZE(Y1)
	STFD	y13, 12 * SIZE(Y1)
	STFD	y14, 13 * SIZE(Y1)
	STFD	y15, 14 * SIZE(Y1)
	STFD	y16, 15 * SIZE(Y1)
	addi	Y1, Y1, 16 * SIZE
	.align 4

LL(25):
	andi.	r0, M, 15
	ble	LL(30)

	andi.	r0, M, 8
	ble	LL(26)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	LFD	y03, 2 * SIZE(Y1)
	LFD	y04, 3 * SIZE(Y1)
	LFD	y05, 4 * SIZE(Y1)
	LFD	y06, 5 * SIZE(Y1)
	LFD	y07, 6 * SIZE(Y1)
	LFD	y08, 7 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	FMADD	y01, alpha1, a1, y01
	LFD	a1,  0 * SIZE(AO2)
	FMADD	y02, alpha1, a2, y02
	LFD	a2,  1 * SIZE(AO2)
	FMADD	y03, alpha1, a3, y03
	LFD	a3,  2 * SIZE(AO2)
	FMADD	y04, alpha1, a4, y04
	LFD	a4,  3 * SIZE(AO2)

	FMADD	y05, alpha1, a5, y05
	LFD	a5,  4 * SIZE(AO2)
	FMADD	y06, alpha1, a6, y06
	LFD	a6,  5 * SIZE(AO2)
	FMADD	y07, alpha1, a7, y07
	LFD	a7,  6 * SIZE(AO2)
	FMADD	y08, alpha1, a8, y08
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2, a1, y01
	LFD	a1,  0 * SIZE(AO3)
	FMADD	y02, alpha2, a2, y02
	LFD	a2,  1 * SIZE(AO3)
	FMADD	y03, alpha2, a3, y03
	LFD	a3,  2 * SIZE(AO3)
	FMADD	y04, alpha2, a4, y04
	LFD	a4,  3 * SIZE(AO3)
	FMADD	y05, alpha2, a5, y05
	LFD	a5,  4 * SIZE(AO3)
	FMADD	y06, alpha2, a6, y06
	LFD	a6,  5 * SIZE(AO3)
	FMADD	y07, alpha2, a7, y07
	LFD	a7,  6 * SIZE(AO3)
	FMADD	y08, alpha2, a8, y08
	LFD	a8,  7 * SIZE(AO3)

	FMADD	y01, alpha3, a1, y01
	LFD	a1,  0 * SIZE(AO4)
	FMADD	y02, alpha3, a2, y02
	LFD	a2,  1 * SIZE(AO4)
	FMADD	y03, alpha3, a3, y03
	LFD	a3,  2 * SIZE(AO4)
	FMADD	y04, alpha3, a4, y04
	LFD	a4,  3 * SIZE(AO4)

	FMADD	y05, alpha3, a5, y05
	LFD	a5,  4 * SIZE(AO4)
	FMADD	y06, alpha3, a6, y06
	LFD	a6,  5 * SIZE(AO4)
	FMADD	y07, alpha3, a7, y07
	LFD	a7,  6 * SIZE(AO4)
	FMADD	y08, alpha3, a8, y08
	LFD	a8,  7 * SIZE(AO4)

	FMADD	y01, alpha4, a1, y01
	addi	AO1, AO1, 8 * SIZE
	FMADD	y02, alpha4, a2, y02
	addi	AO2, AO2, 8 * SIZE
	FMADD	y03, alpha4, a3, y03
	addi	AO3, AO3, 8 * SIZE
	FMADD	y04, alpha4, a4, y04
	addi	AO4, AO4, 8 * SIZE

	STFD	y01, 0 * SIZE(Y1)
	STFD	y02, 1 * SIZE(Y1)
	STFD	y03, 2 * SIZE(Y1)
	STFD	y04, 3 * SIZE(Y1)

	FMADD	y05, alpha4, a5, y05
	FMADD	y06, alpha4, a6, y06
	FMADD	y07, alpha4, a7, y07
	FMADD	y08, alpha4, a8, y08

	STFD	y05, 4 * SIZE(Y1)
	STFD	y06, 5 * SIZE(Y1)
	STFD	y07, 6 * SIZE(Y1)
	STFD	y08, 7 * SIZE(Y1)
	addi	Y1, Y1, 8 * SIZE
	.align 4

LL(26):
	andi.	r0, M, 4
	ble	LL(27)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	LFD	y03, 2 * SIZE(Y1)
	LFD	y04, 3 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)

	LFD	a5,  0 * SIZE(AO2)
	LFD	a6,  1 * SIZE(AO2)
	LFD	a7,  2 * SIZE(AO2)
	LFD	a8,  3 * SIZE(AO2)

	FMADD	y01, alpha1, a1, y01
	LFD	a1,  0 * SIZE(AO3)
	FMADD	y02, alpha1, a2, y02
	LFD	a2,  1 * SIZE(AO3)
	FMADD	y03, alpha1, a3, y03
	LFD	a3,  2 * SIZE(AO3)
	FMADD	y04, alpha1, a4, y04
	LFD	a4,  3 * SIZE(AO3)

	FMADD	y01, alpha2, a5, y01
	LFD	a5,  0 * SIZE(AO4)
	FMADD	y02, alpha2, a6, y02
	LFD	a6,  1 * SIZE(AO4)
	FMADD	y03, alpha2, a7, y03
	LFD	a7,  2 * SIZE(AO4)
	FMADD	y04, alpha2, a8, y04
	LFD	a8,  3 * SIZE(AO4)

	FMADD	y01, alpha3, a1, y01
	addi	AO1, AO1, 4 * SIZE
	FMADD	y02, alpha3, a2, y02
	addi	AO2, AO2, 4 * SIZE
	FMADD	y03, alpha3, a3, y03
	addi	AO3, AO3, 4 * SIZE
	FMADD	y04, alpha3, a4, y04
	addi	AO4, AO4, 4 * SIZE

	FMADD	y01, alpha4, a5, y01
	FMADD	y02, alpha4, a6, y02
	FMADD	y03, alpha4, a7, y03
	FMADD	y04, alpha4, a8, y04

	STFD	y01, 0 * SIZE(Y1)
	STFD	y02, 1 * SIZE(Y1)
	STFD	y03, 2 * SIZE(Y1)
	STFD	y04, 3 * SIZE(Y1)
	addi	Y1, Y1, 4 * SIZE
	.align 4

LL(27):
	andi.	r0, M, 2
	ble	LL(28)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  0 * SIZE(AO2)
	LFD	a4,  1 * SIZE(AO2)

	LFD	a5,  0 * SIZE(AO3)
	LFD	a6,  1 * SIZE(AO3)
	LFD	a7,  0 * SIZE(AO4)
	LFD	a8,  1 * SIZE(AO4)

	FMADD	y01, alpha1, a1, y01
	addi	AO1, AO1, 2 * SIZE
	FMADD	y02, alpha1, a2, y02
	addi	AO2, AO2, 2 * SIZE
	FMADD	y01, alpha2, a3, y01
	addi	AO3, AO3, 2 * SIZE
	FMADD	y02, alpha2, a4, y02
	addi	AO4, AO4, 2 * SIZE

	FMADD	y01, alpha3, a5, y01
	FMADD	y02, alpha3, a6, y02
	FMADD	y01, alpha4, a7, y01
	FMADD	y02, alpha4, a8, y02

	STFD	y01, 0 * SIZE(Y1)
	STFD	y02, 1 * SIZE(Y1)
	addi	Y1, Y1, 2 * SIZE
	.align 4

LL(28):
	andi.	r0, M, 1
	ble	LL(30)

	LFD	y01, 0 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  0 * SIZE(AO2)
	LFD	a3,  0 * SIZE(AO3)
	LFD	a4,  0 * SIZE(AO4)

	FMADD	y01, alpha1, a1, y01
	FMADD	y01, alpha2, a2, y01
	FMADD	y01, alpha3, a3, y01
	FMADD	y01, alpha4, a4, y01

	STFD	y01, 0 * SIZE(Y1)
	.align 4

LL(30):
	andi.	J, N, 2
	lfd	alpha, ALPHA
	ble	LL(40)
	.align 4

	LFD	alpha1, 0 * SIZE(X)
	add	X, X, INCX
	LFD	alpha2, 0 * SIZE(X)
	add	X, X, INCX

	FMUL	alpha1, alpha, alpha1
	FMUL	alpha2, alpha, alpha2

	mr	AO1, A
	add	AO2, A,   LDA
	add	A,   AO2, LDA

	mr	Y1, YY

	srawi.	r0,  M, 4
	mtspr	CTR, r0
	ble	LL(35)


	LFD	y01,  0 * SIZE(Y1)
	LFD	y02,  1 * SIZE(Y1)
	LFD	y03,  2 * SIZE(Y1)
	LFD	y04,  3 * SIZE(Y1)
	LFD	y05,  4 * SIZE(Y1)
	LFD	y06,  5 * SIZE(Y1)
	LFD	y07,  6 * SIZE(Y1)
	LFD	y08,  7 * SIZE(Y1)
	LFD	y09,  8 * SIZE(Y1)
	LFD	y10,  9 * SIZE(Y1)
	LFD	y11, 10 * SIZE(Y1)
	LFD	y12, 11 * SIZE(Y1)
	LFD	y13, 12 * SIZE(Y1)
	LFD	y14, 13 * SIZE(Y1)
	LFD	y15, 14 * SIZE(Y1)
	LFD	y16, 15 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)
	bdz	LL(33)
	.align 4

LL(32):
	FMADD	y01, alpha1, a1, y01
	LFD	a1,  8 * SIZE(AO1)
	FMADD	y02, alpha1, a2, y02
	LFD	a2,  9 * SIZE(AO1)
	FMADD	y03, alpha1, a3, y03
	LFD	a3, 10 * SIZE(AO1)
	FMADD	y04, alpha1, a4, y04
	LFD	a4, 11 * SIZE(AO1)

	FMADD	y05, alpha1, a5, y05
	LFD	a5, 12 * SIZE(AO1)
	FMADD	y06, alpha1, a6, y06
	LFD	a6, 13 * SIZE(AO1)
	FMADD	y07, alpha1, a7, y07
	LFD	a7, 14 * SIZE(AO1)
	FMADD	y08, alpha1, a8, y08
	LFD	a8, 15 * SIZE(AO1)

	FMADD	y09, alpha1, a1, y09
	LFD	a1,  0 * SIZE(AO2)
	FMADD	y10, alpha1, a2, y10
	LFD	a2,  1 * SIZE(AO2)
	FMADD	y11, alpha1, a3, y11
	LFD	a3,  2 * SIZE(AO2)
	FMADD	y12, alpha1, a4, y12
	LFD	a4,  3 * SIZE(AO2)

	FMADD	y13, alpha1, a5, y13
	LFD	a5,  4 * SIZE(AO2)
	FMADD	y14, alpha1, a6, y14
	LFD	a6,  5 * SIZE(AO2)
	FMADD	y15, alpha1, a7, y15
	LFD	a7,  6 * SIZE(AO2)
	FMADD	y16, alpha1, a8, y16
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2, a1, y01
	LFD	a1,  8 * SIZE(AO2)
	FMADD	y02, alpha2, a2, y02
	LFD	a2,  9 * SIZE(AO2)
	FMADD	y03, alpha2, a3, y03
	LFD	a3, 10 * SIZE(AO2)
	FMADD	y04, alpha2, a4, y04
	LFD	a4, 11 * SIZE(AO2)

	FMADD	y05, alpha2, a5, y05
	LFD	a5, 12 * SIZE(AO2)
	FMADD	y06, alpha2, a6, y06
	LFD	a6, 13 * SIZE(AO2)
	FMADD	y07, alpha2, a7, y07
	LFD	a7, 14 * SIZE(AO2)
	FMADD	y08, alpha2, a8, y08
	LFD	a8, 15 * SIZE(AO2)

	FMADD	y09, alpha2, a1, y09
	LFD	a1, 16 * SIZE(AO1)
	FMADD	y10, alpha2, a2, y10
	LFD	a2, 17 * SIZE(AO1)
	FMADD	y11, alpha2, a3, y11
	LFD	a3, 18 * SIZE(AO1)
	FMADD	y12, alpha2, a4, y12
	LFD	a4, 19 * SIZE(AO1)

	FMADD	y13, alpha2, a5, y13
	LFD	a5, 20 * SIZE(AO1)
	FMADD	y14, alpha2, a6, y14
	LFD	a6, 21 * SIZE(AO1)
	FMADD	y15, alpha2, a7, y15
	LFD	a7, 22 * SIZE(AO1)
	FMADD	y16, alpha2, a8, y16
	LFD	a8, 23 * SIZE(AO1)

	STFD	y01,  0 * SIZE(Y1)
	STFD	y02,  1 * SIZE(Y1)
	STFD	y03,  2 * SIZE(Y1)
	STFD	y04,  3 * SIZE(Y1)

	LFD	y01, 16 * SIZE(Y1)
	LFD	y02, 17 * SIZE(Y1)
	LFD	y03, 18 * SIZE(Y1)
	LFD	y04, 19 * SIZE(Y1)

	STFD	y05,  4 * SIZE(Y1)
	STFD	y06,  5 * SIZE(Y1)
	STFD	y07,  6 * SIZE(Y1)
	STFD	y08,  7 * SIZE(Y1)

	LFD	y05, 20 * SIZE(Y1)
	LFD	y06, 21 * SIZE(Y1)
	LFD	y07, 22 * SIZE(Y1)
	LFD	y08, 23 * SIZE(Y1)

	STFD	y09,  8 * SIZE(Y1)
	STFD	y10,  9 * SIZE(Y1)
	STFD	y11, 10 * SIZE(Y1)
	STFD	y12, 11 * SIZE(Y1)

	LFD	y09, 24 * SIZE(Y1)
	LFD	y10, 25 * SIZE(Y1)
	LFD	y11, 26 * SIZE(Y1)
	LFD	y12, 27 * SIZE(Y1)

	STFD	y13, 12 * SIZE(Y1)
	STFD	y14, 13 * SIZE(Y1)
	STFD	y15, 14 * SIZE(Y1)
	STFD	y16, 15 * SIZE(Y1)

	LFD	y13, 28 * SIZE(Y1)
	LFD	y14, 29 * SIZE(Y1)
	LFD	y15, 30 * SIZE(Y1)
	LFD	y16, 31 * SIZE(Y1)

	addi	AO1, AO1, 16 * SIZE
	addi	AO2, AO2, 16 * SIZE
	addi	Y1,  Y1,  16 * SIZE

	DCBT(AO1, PREA)
	DCBT(AO2, PREA)
	DCBT(Y1, PREC)

	bdnz	LL(32)
	.align 4

LL(33):
	FMADD	y01, alpha1, a1, y01
	LFD	a1,  8 * SIZE(AO1)
	FMADD	y02, alpha1, a2, y02
	LFD	a2,  9 * SIZE(AO1)
	FMADD	y03, alpha1, a3, y03
	LFD	a3, 10 * SIZE(AO1)
	FMADD	y04, alpha1, a4, y04
	LFD	a4, 11 * SIZE(AO1)

	FMADD	y05, alpha1, a5, y05
	LFD	a5, 12 * SIZE(AO1)
	FMADD	y06, alpha1, a6, y06
	LFD	a6, 13 * SIZE(AO1)
	FMADD	y07, alpha1, a7, y07
	LFD	a7, 14 * SIZE(AO1)
	FMADD	y08, alpha1, a8, y08
	LFD	a8, 15 * SIZE(AO1)

	FMADD	y09, alpha1, a1, y09
	LFD	a1,  0 * SIZE(AO2)
	FMADD	y10, alpha1, a2, y10
	LFD	a2,  1 * SIZE(AO2)
	FMADD	y11, alpha1, a3, y11
	LFD	a3,  2 * SIZE(AO2)
	FMADD	y12, alpha1, a4, y12
	LFD	a4,  3 * SIZE(AO2)

	FMADD	y13, alpha1, a5, y13
	LFD	a5,  4 * SIZE(AO2)
	FMADD	y14, alpha1, a6, y14
	LFD	a6,  5 * SIZE(AO2)
	FMADD	y15, alpha1, a7, y15
	LFD	a7,  6 * SIZE(AO2)
	FMADD	y16, alpha1, a8, y16
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2, a1, y01
	LFD	a1,  8 * SIZE(AO2)
	FMADD	y02, alpha2, a2, y02
	LFD	a2,  9 * SIZE(AO2)
	FMADD	y03, alpha2, a3, y03
	LFD	a3, 10 * SIZE(AO2)
	FMADD	y04, alpha2, a4, y04
	LFD	a4, 11 * SIZE(AO2)

	FMADD	y05, alpha2, a5, y05
	LFD	a5, 12 * SIZE(AO2)
	FMADD	y06, alpha2, a6, y06
	LFD	a6, 13 * SIZE(AO2)
	FMADD	y07, alpha2, a7, y07
	LFD	a7, 14 * SIZE(AO2)
	FMADD	y08, alpha2, a8, y08
	LFD	a8, 15 * SIZE(AO2)

	FMADD	y09, alpha2, a1, y09
	FMADD	y10, alpha2, a2, y10
	FMADD	y11, alpha2, a3, y11
	FMADD	y12, alpha2, a4, y12
	FMADD	y13, alpha2, a5, y13
	FMADD	y14, alpha2, a6, y14
	FMADD	y15, alpha2, a7, y15
	FMADD	y16, alpha2, a8, y16

	STFD	y01,  0 * SIZE(Y1)
	STFD	y02,  1 * SIZE(Y1)
	STFD	y03,  2 * SIZE(Y1)
	STFD	y04,  3 * SIZE(Y1)
	STFD	y05,  4 * SIZE(Y1)
	STFD	y06,  5 * SIZE(Y1)
	STFD	y07,  6 * SIZE(Y1)
	STFD	y08,  7 * SIZE(Y1)
	STFD	y09,  8 * SIZE(Y1)
	STFD	y10,  9 * SIZE(Y1)
	STFD	y11, 10 * SIZE(Y1)
	STFD	y12, 11 * SIZE(Y1)
	STFD	y13, 12 * SIZE(Y1)
	STFD	y14, 13 * SIZE(Y1)
	STFD	y15, 14 * SIZE(Y1)
	STFD	y16, 15 * SIZE(Y1)

	addi	AO1, AO1, 16 * SIZE
	addi	AO2, AO2, 16 * SIZE
	addi	Y1,  Y1,  16 * SIZE
	.align 4

LL(35):
	andi.	r0, M, 15
	ble	LL(40)

	andi.	r0, M, 8
	ble	LL(36)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	LFD	y03, 2 * SIZE(Y1)
	LFD	y04, 3 * SIZE(Y1)
	LFD	y05, 4 * SIZE(Y1)
	LFD	y06, 5 * SIZE(Y1)
	LFD	y07, 6 * SIZE(Y1)
	LFD	y08, 7 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	FMADD	y01, alpha1, a1, y01
	LFD	a1,  0 * SIZE(AO2)
	FMADD	y02, alpha1, a2, y02
	LFD	a2,  1 * SIZE(AO2)
	FMADD	y03, alpha1, a3, y03
	LFD	a3,  2 * SIZE(AO2)
	FMADD	y04, alpha1, a4, y04
	LFD	a4,  3 * SIZE(AO2)
	FMADD	y05, alpha1, a5, y05
	LFD	a5,  4 * SIZE(AO2)
	FMADD	y06, alpha1, a6, y06
	LFD	a6,  5 * SIZE(AO2)
	FMADD	y07, alpha1, a7, y07
	LFD	a7,  6 * SIZE(AO2)
	FMADD	y08, alpha1, a8, y08
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2, a1, y01
	FMADD	y02, alpha2, a2, y02
	FMADD	y03, alpha2, a3, y03
	FMADD	y04, alpha2, a4, y04
	FMADD	y05, alpha2, a5, y05
	FMADD	y06, alpha2, a6, y06
	FMADD	y07, alpha2, a7, y07
	FMADD	y08, alpha2, a8, y08

	STFD	y01, 0 * SIZE(Y1)
	STFD	y02, 1 * SIZE(Y1)
	STFD	y03, 2 * SIZE(Y1)
	STFD	y04, 3 * SIZE(Y1)
	STFD	y05, 4 * SIZE(Y1)
	STFD	y06, 5 * SIZE(Y1)
	STFD	y07, 6 * SIZE(Y1)
	STFD	y08, 7 * SIZE(Y1)

	addi	AO1, AO1, 8 * SIZE
	addi	AO2, AO2, 8 * SIZE
	addi	Y1,  Y1,  8 * SIZE
	.align 4

LL(36):
	andi.	r0, M, 4
	ble	LL(37)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	LFD	y03, 2 * SIZE(Y1)
	LFD	y04, 3 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)

	LFD	a5,  0 * SIZE(AO2)
	LFD	a6,  1 * SIZE(AO2)
	LFD	a7,  2 * SIZE(AO2)
	LFD	a8,  3 * SIZE(AO2)

	FMADD	y01, alpha1, a1, y01
	FMADD	y02, alpha1, a2, y02
	FMADD	y03, alpha1, a3, y03
	FMADD	y04, alpha1, a4, y04

	FMADD	y01, alpha2, a5, y01
	FMADD	y02, alpha2, a6, y02
	FMADD	y03, alpha2, a7, y03
	FMADD	y04, alpha2, a8, y04

	STFD	y01, 0 * SIZE(Y1)
	STFD	y02, 1 * SIZE(Y1)
	STFD	y03, 2 * SIZE(Y1)
	STFD	y04, 3 * SIZE(Y1)

	addi	AO1, AO1, 4 * SIZE
	addi	AO2, AO2, 4 * SIZE
	addi	Y1,  Y1,  4 * SIZE
	.align 4

LL(37):
	andi.	r0, M, 2
	ble	LL(38)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  0 * SIZE(AO2)
	LFD	a4,  1 * SIZE(AO2)

	FMADD	y01, alpha1, a1, y01
	FMADD	y02, alpha1, a2, y02
	FMADD	y01, alpha2, a3, y01
	FMADD	y02, alpha2, a4, y02

	STFD	y01, 0 * SIZE(Y1)
	STFD	y02, 1 * SIZE(Y1)

	addi	AO1, AO1, 2 * SIZE
	addi	AO2, AO2, 2 * SIZE
	addi	Y1,  Y1,  2 * SIZE
	.align 4

LL(38):
	andi.	r0, M, 1
	ble	LL(40)

	LFD	y01, 0 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  0 * SIZE(AO2)

	FMADD	y01, alpha1, a1, y01
	FMADD	y01, alpha2, a2, y01

	STFD	y01, 0 * SIZE(Y1)
	.align 4

LL(40):
	andi.	J, N, 1
	lfd	alpha, ALPHA
	ble	LL(990)
	.align 4

	LFD	alpha1, 0 * SIZE(X)
	FMUL	alpha1, alpha, alpha1

	mr	AO1, A
	mr	Y1, YY

	srawi.	r0,  M, 4
	mtspr	CTR, r0
	ble	LL(45)

	LFD	y01,  0 * SIZE(Y1)
	LFD	y02,  1 * SIZE(Y1)
	LFD	y03,  2 * SIZE(Y1)
	LFD	y04,  3 * SIZE(Y1)
	LFD	y05,  4 * SIZE(Y1)
	LFD	y06,  5 * SIZE(Y1)
	LFD	y07,  6 * SIZE(Y1)
	LFD	y08,  7 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	LFD	y09,  8 * SIZE(Y1)
	LFD	y10,  9 * SIZE(Y1)
	LFD	y11, 10 * SIZE(Y1)
	LFD	y12, 11 * SIZE(Y1)
	LFD	y13, 12 * SIZE(Y1)
	LFD	y14, 13 * SIZE(Y1)
	LFD	y15, 14 * SIZE(Y1)
	LFD	y16, 15 * SIZE(Y1)

	bdz	LL(43)
	.align 4

LL(42):
	FMADD	y01, alpha1, a1, y01
	LFD	a1,  8 * SIZE(AO1)
	FMADD	y02, alpha1, a2, y02
	LFD	a2,  9 * SIZE(AO1)
	FMADD	y03, alpha1, a3, y03
	LFD	a3, 10 * SIZE(AO1)
	FMADD	y04, alpha1, a4, y04
	LFD	a4, 11 * SIZE(AO1)

	FMADD	y05, alpha1, a5, y05
	LFD	a5, 12 * SIZE(AO1)
	FMADD	y06, alpha1, a6, y06
	LFD	a6, 13 * SIZE(AO1)
	FMADD	y07, alpha1, a7, y07
	LFD	a7, 14 * SIZE(AO1)
	FMADD	y08, alpha1, a8, y08
	LFD	a8, 15 * SIZE(AO1)

	FMADD	y09, alpha1, a1, y09
	LFD	a1, 16 * SIZE(AO1)
	FMADD	y10, alpha1, a2, y10
	LFD	a2, 17 * SIZE(AO1)
	FMADD	y11, alpha1, a3, y11
	LFD	a3, 18 * SIZE(AO1)
	FMADD	y12, alpha1, a4, y12
	LFD	a4, 19 * SIZE(AO1)

	FMADD	y13, alpha1, a5, y13
	LFD	a5, 20 * SIZE(AO1)
	FMADD	y14, alpha1, a6, y14
	LFD	a6, 21 * SIZE(AO1)
	FMADD	y15, alpha1, a7, y15
	LFD	a7, 22 * SIZE(AO1)
	FMADD	y16, alpha1, a8, y16
	LFD	a8, 23 * SIZE(AO1)

	STFD	y01,  0 * SIZE(Y1)
	LFD	y01, 16 * SIZE(Y1)
	STFD	y02,  1 * SIZE(Y1)
	LFD	y02, 17 * SIZE(Y1)

	STFD	y03,  2 * SIZE(Y1)
	LFD	y03, 18 * SIZE(Y1)
	STFD	y04,  3 * SIZE(Y1)
	LFD	y04, 19 * SIZE(Y1)

	STFD	y05,  4 * SIZE(Y1)
	LFD	y05, 20 * SIZE(Y1)
	STFD	y06,  5 * SIZE(Y1)
	LFD	y06, 21 * SIZE(Y1)

	STFD	y07,  6 * SIZE(Y1)
	LFD	y07, 22 * SIZE(Y1)
	STFD	y08,  7 * SIZE(Y1)
	LFD	y08, 23 * SIZE(Y1)

	STFD	y09,  8 * SIZE(Y1)
	LFD	y09, 24 * SIZE(Y1)
	STFD	y10,  9 * SIZE(Y1)
	LFD	y10, 25 * SIZE(Y1)

	STFD	y11, 10 * SIZE(Y1)
	LFD	y11, 26 * SIZE(Y1)
	STFD	y12, 11 * SIZE(Y1)
	LFD	y12, 27 * SIZE(Y1)

	STFD	y13, 12 * SIZE(Y1)
	LFD	y13, 28 * SIZE(Y1)
	STFD	y14, 13 * SIZE(Y1)
	LFD	y14, 29 * SIZE(Y1)

	STFD	y15, 14 * SIZE(Y1)
	LFD	y15, 30 * SIZE(Y1)
	STFD	y16, 15 * SIZE(Y1)
	LFD	y16, 31 * SIZE(Y1)

	addi	AO1, AO1, 16 * SIZE
	addi	Y1, Y1, 16 * SIZE

	DCBT(AO1, PREA)
	DCBT(Y1, PREC)

	bdnz	LL(42)
	.align 4

LL(43):
	FMADD	y01, alpha1, a1, y01
	LFD	a1,  8 * SIZE(AO1)
	FMADD	y02, alpha1, a2, y02
	LFD	a2,  9 * SIZE(AO1)
	FMADD	y03, alpha1, a3, y03
	LFD	a3, 10 * SIZE(AO1)
	FMADD	y04, alpha1, a4, y04
	LFD	a4, 11 * SIZE(AO1)

	FMADD	y05, alpha1, a5, y05
	LFD	a5, 12 * SIZE(AO1)
	FMADD	y06, alpha1, a6, y06
	LFD	a6, 13 * SIZE(AO1)
	FMADD	y07, alpha1, a7, y07
	LFD	a7, 14 * SIZE(AO1)
	FMADD	y08, alpha1, a8, y08
	LFD	a8, 15 * SIZE(AO1)

	FMADD	y09, alpha1, a1, y09
	FMADD	y10, alpha1, a2, y10
	FMADD	y11, alpha1, a3, y11
	FMADD	y12, alpha1, a4, y12
	FMADD	y13, alpha1, a5, y13
	FMADD	y14, alpha1, a6, y14
	FMADD	y15, alpha1, a7, y15
	FMADD	y16, alpha1, a8, y16

	STFD	y01,  0 * SIZE(Y1)
	STFD	y02,  1 * SIZE(Y1)
	STFD	y03,  2 * SIZE(Y1)
	STFD	y04,  3 * SIZE(Y1)
	STFD	y05,  4 * SIZE(Y1)
	STFD	y06,  5 * SIZE(Y1)
	STFD	y07,  6 * SIZE(Y1)
	STFD	y08,  7 * SIZE(Y1)

	STFD	y09,  8 * SIZE(Y1)
	STFD	y10,  9 * SIZE(Y1)
	STFD	y11, 10 * SIZE(Y1)
	STFD	y12, 11 * SIZE(Y1)
	STFD	y13, 12 * SIZE(Y1)
	STFD	y14, 13 * SIZE(Y1)
	STFD	y15, 14 * SIZE(Y1)
	STFD	y16, 15 * SIZE(Y1)

	addi	AO1, AO1, 16 * SIZE
	addi	Y1,  Y1,  16 * SIZE
	.align 4

LL(45):
	andi.	r0, M, 15
	ble	LL(990)

	andi.	r0, M, 8
	ble	LL(46)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	LFD	y03, 2 * SIZE(Y1)
	LFD	y04, 3 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)

	LFD	y05, 4 * SIZE(Y1)
	LFD	y06, 5 * SIZE(Y1)
	LFD	y07, 6 * SIZE(Y1)
	LFD	y08, 7 * SIZE(Y1)

	LFD	a5,  4 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	FMADD	y01, alpha1, a1, y01
	FMADD	y02, alpha1, a2, y02
	FMADD	y03, alpha1, a3, y03
	FMADD	y04, alpha1, a4, y04

	FMADD	y05, alpha1, a5, y05
	FMADD	y06, alpha1, a6, y06
	FMADD	y07, alpha1, a7, y07
	FMADD	y08, alpha1, a8, y08

	STFD	y01, 0 * SIZE(Y1)
	STFD	y02, 1 * SIZE(Y1)
	STFD	y03, 2 * SIZE(Y1)
	STFD	y04, 3 * SIZE(Y1)

	STFD	y05, 4 * SIZE(Y1)
	STFD	y06, 5 * SIZE(Y1)
	STFD	y07, 6 * SIZE(Y1)
	STFD	y08, 7 * SIZE(Y1)

	addi	AO1, AO1, 8 * SIZE
	addi	Y1,  Y1,  8 * SIZE
	.align 4

LL(46):
	andi.	r0, M, 4
	ble	LL(47)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	LFD	y03, 2 * SIZE(Y1)
	LFD	y04, 3 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)

	FMADD	y01, alpha1, a1, y01
	FMADD	y02, alpha1, a2, y02
	FMADD	y03, alpha1, a3, y03
	FMADD	y04, alpha1, a4, y04

	STFD	y01, 0 * SIZE(Y1)
	STFD	y02, 1 * SIZE(Y1)
	STFD	y03, 2 * SIZE(Y1)
	STFD	y04, 3 * SIZE(Y1)

	addi	AO1, AO1, 4 * SIZE
	addi	Y1,  Y1,  4 * SIZE
	.align 4

LL(47):
	andi.	r0, M, 2
	ble	LL(48)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)

	FMADD	y01, alpha1, a1, y01
	FMADD	y02, alpha1, a2, y02

	STFD	y01, 0 * SIZE(Y1)
	STFD	y02, 1 * SIZE(Y1)

	addi	AO1, AO1, 2 * SIZE
	addi	Y1,  Y1,  2 * SIZE
	.align 4

LL(48):
	andi.	r0, M, 1
	ble	LL(990)

	LFD	y01, 0 * SIZE(Y1)
	LFD	a1,  0 * SIZE(AO1)

	FMADD	y01, alpha1, a1, y01
	STFD	y01, 0 * SIZE(Y1)
	.align 4

LL(990):
	cmpi	cr0, 0, INCY, SIZE
	beq	LL(999)

	mr	YY, BUFFER
	mr	Y1, Y

	srawi.	r0, M, 3
	mtspr	CTR, r0
	ble	LL(995)
	.align 4

LL(991):
	LFD	f0,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f1,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f2,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f3,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f4,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f5,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f6,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f7,  0 * SIZE(Y)
	add	Y, Y, INCY

	LFD	f8,   0 * SIZE(YY)
	LFD	f9,   1 * SIZE(YY)
	LFD	f10,  2 * SIZE(YY)
	LFD	f11,  3 * SIZE(YY)
	LFD	f12,  4 * SIZE(YY)
	LFD	f13,  5 * SIZE(YY)
	LFD	f14,  6 * SIZE(YY)
	LFD	f15,  7 * SIZE(YY)
	addi	YY, YY, 8 * SIZE

	FADD	f8,  f8,  f0
	FADD	f9,  f9,  f1
	FADD	f10, f10, f2
	FADD	f11, f11, f3
	FADD	f12, f12, f4
	FADD	f13, f13, f5
	FADD	f14, f14, f6
	FADD	f15, f15, f7

	STFD	f8,  0 * SIZE(Y1)
	add	Y1, Y1, INCY
	STFD	f9,  0 * SIZE(Y1)
	add	Y1, Y1, INCY
	STFD	f10, 0 * SIZE(Y1)
	add	Y1, Y1, INCY
	STFD	f11, 0 * SIZE(Y1)
	add	Y1, Y1, INCY
	STFD	f12, 0 * SIZE(Y1)
	add	Y1, Y1, INCY
	STFD	f13, 0 * SIZE(Y1)
	add	Y1, Y1, INCY
	STFD	f14, 0 * SIZE(Y1)
	add	Y1, Y1, INCY
	STFD	f15, 0 * SIZE(Y1)
	add	Y1, Y1, INCY
	bdnz	LL(991)
	.align 4

LL(995):
	andi.	J, M, 4
	ble	LL(996)

	LFD	f0,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f1,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f2,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f3,  0 * SIZE(Y)
	add	Y, Y, INCY

	LFD	f8,   0 * SIZE(YY)
	LFD	f9,   1 * SIZE(YY)
	LFD	f10,  2 * SIZE(YY)
	LFD	f11,  3 * SIZE(YY)
	addi	YY, YY, 4 * SIZE

	FADD	f8,  f8,  f0
	FADD	f9,  f9,  f1
	FADD	f10, f10, f2
	FADD	f11, f11, f3

	STFD	f8,  0 * SIZE(Y1)
	add	Y1, Y1, INCY
	STFD	f9,  0 * SIZE(Y1)
	add	Y1, Y1, INCY
	STFD	f10, 0 * SIZE(Y1)
	add	Y1, Y1, INCY
	STFD	f11, 0 * SIZE(Y1)
	add	Y1, Y1, INCY
	.align 4

LL(996):
	andi.	J, M, 2
	ble	LL(997)

	LFD	f0,  0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	f1,  0 * SIZE(Y)
	add	Y, Y, INCY

	LFD	f8,   0 * SIZE(YY)
	LFD	f9,   1 * SIZE(YY)
	addi	YY, YY, 2 * SIZE

	FADD	f8,  f8,  f0
	FADD	f9,  f9,  f1

	STFD	f8,  0 * SIZE(Y1)
	add	Y1, Y1, INCY
	STFD	f9,  0 * SIZE(Y1)
	add	Y1, Y1, INCY
	.align 4

LL(997):
	andi.	J, M, 1
	ble	LL(999)

	LFD	f0,  0 * SIZE(Y)
	LFD	f8,   0 * SIZE(YY)

	FADD	f8,  f8,  f0

	STFD	f8,  0 * SIZE(Y1)
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
	ld	r23,   216(SP)
	ld	r24,   224(SP)
	ld	r25,   232(SP)
	ld	r26,   240(SP)
	ld	r27,   248(SP)
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
	lwz	r26,   192(SP)
	lwz	r27,   196(SP)
#endif

	addi	SP, SP, STACKSIZE
	blr

	EPILOGUE
#endif
