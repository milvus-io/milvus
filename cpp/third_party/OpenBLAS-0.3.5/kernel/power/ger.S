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

#ifndef NEEDPARAM
#ifndef DOUBLE
#include "sparam.h"
#else
#include "dparam.h"
#endif
#endif

#ifdef linux
#ifndef __64BIT__
#define M	r3
#define	N	r4
#define X	r6
#define INCX	r7
#define Y	r8
#define	INCY	r9
#define	A	r10
#define	LDA	r5
#else
#define M	r3
#define	N	r4
#define X	r7
#define INCX	r8
#define Y	r9
#define	INCY	r10
#define	A	r5
#define	LDA	r6
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
#define M	r3
#define	N	r4
#define X	r8
#define INCX	r9
#define Y	r10
#define	INCY	r5
#define	A	r6
#define	LDA	r7
#else
#define M	r3
#define	N	r4
#define X	r7
#define INCX	r8
#define Y	r9
#define	INCY	r10
#define	A	r5
#define	LDA	r6
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

#define	X1	r22
#define	PREA	r23
#define	PREC	r24
#define XX	r25
#define BUFFER	r26

#define y01 f0
#define y02 f1
#define y03 f2
#define y04 f3
#define y05 f4
#define y06 f5
#define y07 f6
#define y08 f7

#define alpha1 f8
#define alpha2 f9

#define a1     f12
#define a2     f13
#define a3     f14
#define a4     f15
#define a5     f16
#define a6     f17
#define a7     f18
#define a8     f19
#define a9     f20
#define a10    f21
#define a11    f22
#define a12    f23
#define a13    f24
#define a14    f25
#define a15    f26
#define a16    f27

#define alpha  f31

#if defined(PPC440) || defined(PPC440FP2)
#define PREFETCHSIZE_A  24
#define PREFETCHSIZE_C  16
#endif

#ifdef PPC970
#define PREFETCHSIZE_A  16
#define PREFETCHSIZE_C  16
#endif

#ifdef POWER4
#define PREFETCHSIZE_A  16
#define PREFETCHSIZE_C  16
#endif

#ifdef POWER5
#define PREFETCHSIZE_A  16
#define PREFETCHSIZE_C  16
#endif

#ifndef NEEDPARAM

#ifndef __64BIT__
#define STACKSIZE 224
#else
#define STACKSIZE 280
#endif

	PROLOGUE
	PROFCODE

	addi	SP,   SP, -STACKSIZE

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
	std	r26,   240(SP)
	std	r27,   248(SP)
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
	stw	r26,   192(SP)
	stw	r27,   196(SP)
#endif

#ifdef linux
#ifndef __64BIT__
	lwz	LDA,     FRAMESLOT(0) + STACKSIZE(SP)
	lwz	BUFFER,  FRAMESLOT(1) + STACKSIZE(SP)
#else
	ld	A,       FRAMESLOT(0) + STACKSIZE(SP)
	ld	LDA,     FRAMESLOT(1) + STACKSIZE(SP)
	ld	BUFFER,  FRAMESLOT(2) + STACKSIZE(SP)
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#ifndef __64BIT__
#ifdef DOUBLE
	lwz	INCY,    FRAMESLOT(0) + STACKSIZE(SP)
	lwz	A,       FRAMESLOT(1) + STACKSIZE(SP)
	lwz	LDA,     FRAMESLOT(2) + STACKSIZE(SP)
	lwz	BUFFER,  FRAMESLOT(3) + STACKSIZE(SP)
#else
	lwz	A,       FRAMESLOT(0) + STACKSIZE(SP)
	lwz	LDA,     FRAMESLOT(1) + STACKSIZE(SP)
	lwz	BUFFER,  FRAMESLOT(2) + STACKSIZE(SP)
#endif
#else
	ld	A,       FRAMESLOT(0) + STACKSIZE(SP)
	ld	LDA,     FRAMESLOT(1) + STACKSIZE(SP)
	ld	BUFFER,  FRAMESLOT(2) + STACKSIZE(SP)
#endif
#endif

	fmr	alpha, f1

	slwi	LDA,  LDA,  BASE_SHIFT
	slwi	INCX, INCX, BASE_SHIFT
	slwi	INCY, INCY, BASE_SHIFT

	li	PREA, PREFETCHSIZE_A * SIZE
	li	PREC, PREFETCHSIZE_C * SIZE

	cmpwi	cr0, M, 0
	ble-	LL(999)

	cmpwi	cr0, N, 0
	ble-	LL(999)

	mr	XX, X

	cmpi	cr0, 0, INCX, SIZE
	beq	LL(10)

	mr	XX, BUFFER
	mr	X1, BUFFER

	srawi.	r0,  M, 3
	mtspr	CTR, r0
	ble	LL(05)
	.align 4

LL(01):
	LFD	a1, 0 * SIZE(X)
	add	X, X, INCX
	LFD	a2, 0 * SIZE(X)
	add	X, X, INCX
	LFD	a3, 0 * SIZE(X)
	add	X, X, INCX
	LFD	a4, 0 * SIZE(X)
	add	X, X, INCX
	LFD	a5, 0 * SIZE(X)
	add	X, X, INCX
	LFD	a6, 0 * SIZE(X)
	add	X, X, INCX
	LFD	a7, 0 * SIZE(X)
	add	X, X, INCX
	LFD	a8, 0 * SIZE(X)
	add	X, X, INCX

	STFD	a1, 0 * SIZE(X1)
	STFD	a2, 1 * SIZE(X1)
	STFD	a3, 2 * SIZE(X1)
	STFD	a4, 3 * SIZE(X1)
	STFD	a5, 4 * SIZE(X1)
	STFD	a6, 5 * SIZE(X1)
	STFD	a7, 6 * SIZE(X1)
	STFD	a8, 7 * SIZE(X1)

	addi	X1, X1, 8 * SIZE
	bdnz+	LL(01)
	.align 4

LL(05):
	andi.	r0, M, 7
	mtspr	CTR, r0
	ble	LL(10)
	.align 4

LL(06):
	LFD	a1, 0 * SIZE(X)
	add	X, X, INCX
	STFD	a1, 0 * SIZE(X1)
	addi	X1, X1, SIZE
	bdnz+	LL(06)
	.align 4

LL(10):
	srawi.	J, N, 1
	ble	LL(20)
	.align 4

LL(11):
	LFD	alpha1, 0 * SIZE(Y)
	add	Y, Y, INCY
	LFD	alpha2, 0 * SIZE(Y)
	add	Y, Y, INCY

	FMUL	alpha1, alpha, alpha1
	FMUL	alpha2, alpha, alpha2

	mr	AO1, A
	add	AO2, A,   LDA
	add	A,   AO2, LDA

	mr	X1, XX

	srawi.	r0,  M, 4
	mtspr	CTR, r0
	ble	LL(15)

	LFD	a1,   0 * SIZE(AO1)
	LFD	a2,   1 * SIZE(AO1)
	LFD	a3,   2 * SIZE(AO1)
	LFD	a4,   3 * SIZE(AO1)

	LFD	a5,   4 * SIZE(AO1)
	LFD	a6,   5 * SIZE(AO1)
	LFD	a7,   6 * SIZE(AO1)
	LFD	a8,   7 * SIZE(AO1)

	LFD	y01,  0 * SIZE(X1)
	LFD	y02,  1 * SIZE(X1)
	LFD	y03,  2 * SIZE(X1)
	LFD	y04,  3 * SIZE(X1)

	LFD	y05,  4 * SIZE(X1)
	LFD	y06,  5 * SIZE(X1)
	LFD	y07,  6 * SIZE(X1)
	LFD	y08,  7 * SIZE(X1)

	LFD	a9,   0 * SIZE(AO2)
	LFD	a10,  1 * SIZE(AO2)
	LFD	a11,  2 * SIZE(AO2)
	LFD	a12,  3 * SIZE(AO2)

	LFD	a13,  4 * SIZE(AO2)
	LFD	a14,  5 * SIZE(AO2)
	LFD	a15,  6 * SIZE(AO2)
	LFD	a16,  7 * SIZE(AO2)
	bdz	LL(13)
	.align 4

LL(12):
	FMADD	a1,  alpha1, y01, a1
	FMADD	a2,  alpha1, y02, a2
	FMADD	a3,  alpha1, y03, a3
	FMADD	a4,  alpha1, y04, a4

	FMADD	a5,  alpha1, y05, a5
	FMADD	a6,  alpha1, y06, a6
	FMADD	a7,  alpha1, y07, a7
	FMADD	a8,  alpha1, y08, a8

	STFD	a1,   0 * SIZE(AO1)
	STFD	a2,   1 * SIZE(AO1)
	STFD	a3,   2 * SIZE(AO1)
	STFD	a4,   3 * SIZE(AO1)

	LFD	a1,   8 * SIZE(AO1)
	LFD	a2,   9 * SIZE(AO1)
	LFD	a3,  10 * SIZE(AO1)
	LFD	a4,  11 * SIZE(AO1)

	STFD	a5,   4 * SIZE(AO1)
	STFD	a6,   5 * SIZE(AO1)
	STFD	a7,   6 * SIZE(AO1)
	STFD	a8,   7 * SIZE(AO1)

	LFD	a5,  12 * SIZE(AO1)
	LFD	a6,  13 * SIZE(AO1)
	LFD	a7,  14 * SIZE(AO1)
	LFD	a8,  15 * SIZE(AO1)

	FMADD	a9,  alpha2, y01, a9
	FMADD	a10, alpha2, y02, a10
	FMADD	a11, alpha2, y03, a11
	FMADD	a12, alpha2, y04, a12

	LFD	y01,  8 * SIZE(X1)
	LFD	y02,  9 * SIZE(X1)
	LFD	y03, 10 * SIZE(X1)
	LFD	y04, 11 * SIZE(X1)

	FMADD	a13, alpha2, y05, a13
	FMADD	a14, alpha2, y06, a14
	FMADD	a15, alpha2, y07, a15
	FMADD	a16, alpha2, y08, a16

	LFD	y05, 12 * SIZE(X1)
	LFD	y06, 13 * SIZE(X1)
	LFD	y07, 14 * SIZE(X1)
	LFD	y08, 15 * SIZE(X1)

	STFD	a9,   0 * SIZE(AO2)
	STFD	a10,  1 * SIZE(AO2)
	STFD	a11,  2 * SIZE(AO2)
	STFD	a12,  3 * SIZE(AO2)

	LFD	a9,   8 * SIZE(AO2)
	LFD	a10,  9 * SIZE(AO2)
	LFD	a11, 10 * SIZE(AO2)
	LFD	a12, 11 * SIZE(AO2)

	STFD	a13,  4 * SIZE(AO2)
	STFD	a14,  5 * SIZE(AO2)
	STFD	a15,  6 * SIZE(AO2)
	STFD	a16,  7 * SIZE(AO2)

	LFD	a13, 12 * SIZE(AO2)
	LFD	a14, 13 * SIZE(AO2)
	LFD	a15, 14 * SIZE(AO2)
	LFD	a16, 15 * SIZE(AO2)

	FMADD	a1,  alpha1, y01, a1
	FMADD	a2,  alpha1, y02, a2
	FMADD	a3,  alpha1, y03, a3
	FMADD	a4,  alpha1, y04, a4

	FMADD	a5,  alpha1, y05, a5
	FMADD	a6,  alpha1, y06, a6
	FMADD	a7,  alpha1, y07, a7
	FMADD	a8,  alpha1, y08, a8

	STFD	a1,   8 * SIZE(AO1)
	STFD	a2,   9 * SIZE(AO1)
	STFD	a3,  10 * SIZE(AO1)
	STFD	a4,  11 * SIZE(AO1)

	LFD	a1,  16 * SIZE(AO1)
	LFD	a2,  17 * SIZE(AO1)
	LFD	a3,  18 * SIZE(AO1)
	LFD	a4,  19 * SIZE(AO1)

	STFD	a5,  12 * SIZE(AO1)
	STFD	a6,  13 * SIZE(AO1)
	STFD	a7,  14 * SIZE(AO1)
	STFD	a8,  15 * SIZE(AO1)

	LFD	a5,  20 * SIZE(AO1)
	LFD	a6,  21 * SIZE(AO1)
	LFD	a7,  22 * SIZE(AO1)
	LFD	a8,  23 * SIZE(AO1)

	FMADD	a9,  alpha2, y01, a9
	FMADD	a10, alpha2, y02, a10
	FMADD	a11, alpha2, y03, a11
	FMADD	a12, alpha2, y04, a12

	LFD	y01, 16 * SIZE(X1)
	LFD	y02, 17 * SIZE(X1)
	LFD	y03, 18 * SIZE(X1)
	LFD	y04, 19 * SIZE(X1)

	FMADD	a13, alpha2, y05, a13
	FMADD	a14, alpha2, y06, a14
	FMADD	a15, alpha2, y07, a15
	FMADD	a16, alpha2, y08, a16

	LFD	y05, 20 * SIZE(X1)
	LFD	y06, 21 * SIZE(X1)
	LFD	y07, 22 * SIZE(X1)
	LFD	y08, 23 * SIZE(X1)

	STFD	a9,   8 * SIZE(AO2)
	STFD	a10,  9 * SIZE(AO2)
	STFD	a11, 10 * SIZE(AO2)
	STFD	a12, 11 * SIZE(AO2)

	LFD	a9,  16 * SIZE(AO2)
	LFD	a10, 17 * SIZE(AO2)
	LFD	a11, 18 * SIZE(AO2)
	LFD	a12, 19 * SIZE(AO2)

	STFD	a13, 12 * SIZE(AO2)
	STFD	a14, 13 * SIZE(AO2)
	STFD	a15, 14 * SIZE(AO2)
	STFD	a16, 15 * SIZE(AO2)

	LFD	a13, 20 * SIZE(AO2)
	LFD	a14, 21 * SIZE(AO2)
	LFD	a15, 22 * SIZE(AO2)
	LFD	a16, 23 * SIZE(AO2)

	addi	AO1, AO1, 16 * SIZE
	addi	AO2, AO2, 16 * SIZE
	addi	X1, X1,   16 * SIZE

	DCBT(AO1, PREA)
	DCBT(AO2, PREA)
	DCBT(Y1, PREY)

	bdnz+	LL(12)
	.align 4

LL(13):
	FMADD	a1,  alpha1, y01, a1
	FMADD	a2,  alpha1, y02, a2
	FMADD	a3,  alpha1, y03, a3
	FMADD	a4,  alpha1, y04, a4

	FMADD	a5,  alpha1, y05, a5
	FMADD	a6,  alpha1, y06, a6
	FMADD	a7,  alpha1, y07, a7
	FMADD	a8,  alpha1, y08, a8

	STFD	a1,   0 * SIZE(AO1)
	STFD	a2,   1 * SIZE(AO1)
	STFD	a3,   2 * SIZE(AO1)
	STFD	a4,   3 * SIZE(AO1)

	LFD	a1,   8 * SIZE(AO1)
	LFD	a2,   9 * SIZE(AO1)
	LFD	a3,  10 * SIZE(AO1)
	LFD	a4,  11 * SIZE(AO1)

	STFD	a5,   4 * SIZE(AO1)
	STFD	a6,   5 * SIZE(AO1)
	STFD	a7,   6 * SIZE(AO1)
	STFD	a8,   7 * SIZE(AO1)

	LFD	a5,  12 * SIZE(AO1)
	LFD	a6,  13 * SIZE(AO1)
	LFD	a7,  14 * SIZE(AO1)
	LFD	a8,  15 * SIZE(AO1)

	FMADD	a9,  alpha2, y01, a9
	FMADD	a10, alpha2, y02, a10
	FMADD	a11, alpha2, y03, a11
	FMADD	a12, alpha2, y04, a12

	LFD	y01,  8 * SIZE(X1)
	LFD	y02,  9 * SIZE(X1)
	LFD	y03, 10 * SIZE(X1)
	LFD	y04, 11 * SIZE(X1)

	FMADD	a13, alpha2, y05, a13
	FMADD	a14, alpha2, y06, a14
	FMADD	a15, alpha2, y07, a15
	FMADD	a16, alpha2, y08, a16

	LFD	y05, 12 * SIZE(X1)
	LFD	y06, 13 * SIZE(X1)
	LFD	y07, 14 * SIZE(X1)
	LFD	y08, 15 * SIZE(X1)

	STFD	a9,   0 * SIZE(AO2)
	STFD	a10,  1 * SIZE(AO2)
	STFD	a11,  2 * SIZE(AO2)
	STFD	a12,  3 * SIZE(AO2)

	LFD	a9,   8 * SIZE(AO2)
	LFD	a10,  9 * SIZE(AO2)
	LFD	a11, 10 * SIZE(AO2)
	LFD	a12, 11 * SIZE(AO2)

	STFD	a13,  4 * SIZE(AO2)
	STFD	a14,  5 * SIZE(AO2)
	STFD	a15,  6 * SIZE(AO2)
	STFD	a16,  7 * SIZE(AO2)

	LFD	a13, 12 * SIZE(AO2)
	LFD	a14, 13 * SIZE(AO2)
	LFD	a15, 14 * SIZE(AO2)
	LFD	a16, 15 * SIZE(AO2)

	FMADD	a1,  alpha1, y01, a1
	FMADD	a2,  alpha1, y02, a2
	FMADD	a3,  alpha1, y03, a3
	FMADD	a4,  alpha1, y04, a4

	FMADD	a5,  alpha1, y05, a5
	FMADD	a6,  alpha1, y06, a6
	FMADD	a7,  alpha1, y07, a7
	FMADD	a8,  alpha1, y08, a8

	STFD	a1,   8 * SIZE(AO1)
	STFD	a2,   9 * SIZE(AO1)
	STFD	a3,  10 * SIZE(AO1)
	STFD	a4,  11 * SIZE(AO1)

	LFD	a1,  16 * SIZE(AO1)
	LFD	a2,  17 * SIZE(AO1)
	LFD	a3,  18 * SIZE(AO1)
	LFD	a4,  19 * SIZE(AO1)

	STFD	a5,  12 * SIZE(AO1)
	STFD	a6,  13 * SIZE(AO1)
	STFD	a7,  14 * SIZE(AO1)
	STFD	a8,  15 * SIZE(AO1)

	LFD	a5,  20 * SIZE(AO1)
	LFD	a6,  21 * SIZE(AO1)
	LFD	a7,  22 * SIZE(AO1)
	LFD	a8,  23 * SIZE(AO1)

	FMADD	a9,  alpha2, y01, a9
	FMADD	a10, alpha2, y02, a10
	FMADD	a11, alpha2, y03, a11
	FMADD	a12, alpha2, y04, a12

	FMADD	a13, alpha2, y05, a13
	FMADD	a14, alpha2, y06, a14
	FMADD	a15, alpha2, y07, a15
	FMADD	a16, alpha2, y08, a16

	STFD	a9,   8 * SIZE(AO2)
	STFD	a10,  9 * SIZE(AO2)
	STFD	a11, 10 * SIZE(AO2)
	STFD	a12, 11 * SIZE(AO2)

	STFD	a13, 12 * SIZE(AO2)
	STFD	a14, 13 * SIZE(AO2)
	STFD	a15, 14 * SIZE(AO2)
	STFD	a16, 15 * SIZE(AO2)

	addi	AO1, AO1, 16 * SIZE
	addi	AO2, AO2, 16 * SIZE
	addi	X1, X1,   16 * SIZE
	.align 4


LL(15):
	andi.	r0, M, 15
	ble	LL(19)

	andi.	r0, M, 8
	ble	LL(16)

	LFD	y01,  0 * SIZE(X1)
	LFD	y02,  1 * SIZE(X1)
	LFD	y03,  2 * SIZE(X1)
	LFD	y04,  3 * SIZE(X1)
	LFD	y05,  4 * SIZE(X1)
	LFD	y06,  5 * SIZE(X1)
	LFD	y07,  6 * SIZE(X1)
	LFD	y08,  7 * SIZE(X1)

	LFD	a1,   0 * SIZE(AO1)
	LFD	a2,   1 * SIZE(AO1)
	LFD	a3,   2 * SIZE(AO1)
	LFD	a4,   3 * SIZE(AO1)
	LFD	a5,   4 * SIZE(AO1)
	LFD	a6,   5 * SIZE(AO1)
	LFD	a7,   6 * SIZE(AO1)
	LFD	a8,   7 * SIZE(AO1)

	LFD	a9,   0 * SIZE(AO2)
	LFD	a10,  1 * SIZE(AO2)
	LFD	a11,  2 * SIZE(AO2)
	LFD	a12,  3 * SIZE(AO2)
	LFD	a13,  4 * SIZE(AO2)
	LFD	a14,  5 * SIZE(AO2)
	LFD	a15,  6 * SIZE(AO2)
	LFD	a16,  7 * SIZE(AO2)

	FMADD	a1,  alpha1, y01, a1
	FMADD	a2,  alpha1, y02, a2
	FMADD	a3,  alpha1, y03, a3
	FMADD	a4,  alpha1, y04, a4

	STFD	a1,   0 * SIZE(AO1)
	STFD	a2,   1 * SIZE(AO1)
	STFD	a3,   2 * SIZE(AO1)
	STFD	a4,   3 * SIZE(AO1)

	FMADD	a5,  alpha1, y05, a5
	FMADD	a6,  alpha1, y06, a6
	FMADD	a7,  alpha1, y07, a7
	FMADD	a8,  alpha1, y08, a8

	STFD	a5,   4 * SIZE(AO1)
	STFD	a6,   5 * SIZE(AO1)
	STFD	a7,   6 * SIZE(AO1)
	STFD	a8,   7 * SIZE(AO1)

	FMADD	a9,  alpha2, y01, a9
	FMADD	a10, alpha2, y02, a10
	FMADD	a11, alpha2, y03, a11
	FMADD	a12, alpha2, y04, a12

	STFD	a9,   0 * SIZE(AO2)
	STFD	a10,  1 * SIZE(AO2)
	STFD	a11,  2 * SIZE(AO2)
	STFD	a12,  3 * SIZE(AO2)

	FMADD	a13, alpha2, y05, a13
	FMADD	a14, alpha2, y06, a14
	FMADD	a15, alpha2, y07, a15
	FMADD	a16, alpha2, y08, a16

	STFD	a13,  4 * SIZE(AO2)
	STFD	a14,  5 * SIZE(AO2)
	STFD	a15,  6 * SIZE(AO2)
	STFD	a16,  7 * SIZE(AO2)

	addi	AO1, AO1, 8 * SIZE
	addi	AO2, AO2, 8 * SIZE
	addi	X1, X1, 8 * SIZE
	.align 4

LL(16):
	andi.	r0, M, 4
	ble	LL(17)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)

	LFD	y01, 0 * SIZE(X1)
	LFD	y02, 1 * SIZE(X1)
	LFD	y03, 2 * SIZE(X1)
	LFD	y04, 3 * SIZE(X1)

	LFD	a5,  0 * SIZE(AO2)
	LFD	a6,  1 * SIZE(AO2)
	LFD	a7,  2 * SIZE(AO2)
	LFD	a8,  3 * SIZE(AO2)

	FMADD	a1,  alpha1, y01, a1
	FMADD	a2,  alpha1, y02, a2
	FMADD	a3,  alpha1, y03, a3
	FMADD	a4,  alpha1, y04, a4

	STFD	a1,  0 * SIZE(AO1)
	STFD	a2,  1 * SIZE(AO1)
	STFD	a3,  2 * SIZE(AO1)
	STFD	a4,  3 * SIZE(AO1)

	FMADD	a5,  alpha2, y01, a5
	FMADD	a6,  alpha2, y02, a6
	FMADD	a7,  alpha2, y03, a7
	FMADD	a8,  alpha2, y04, a8

	STFD	a5,  0 * SIZE(AO2)
	STFD	a6,  1 * SIZE(AO2)
	STFD	a7,  2 * SIZE(AO2)
	STFD	a8,  3 * SIZE(AO2)

	addi	AO1, AO1, 4 * SIZE
	addi	AO2, AO2, 4 * SIZE
	addi	X1, X1, 4 * SIZE
	.align 4

LL(17):
	andi.	r0, M, 2
	ble	LL(18)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  0 * SIZE(AO2)
	LFD	a4,  1 * SIZE(AO2)

	LFD	y01, 0 * SIZE(X1)
	LFD	y02, 1 * SIZE(X1)

	FMADD	a1, alpha1, y01, a1
	FMADD	a2, alpha1, y02, a2
	FMADD	a3, alpha2, y01, a3
	FMADD	a4, alpha2, y02, a4

	STFD	a1,  0 * SIZE(AO1)
	STFD	a2,  1 * SIZE(AO1)
	STFD	a3,  0 * SIZE(AO2)
	STFD	a4,  1 * SIZE(AO2)

	addi	AO1, AO1, 2 * SIZE
	addi	AO2, AO2, 2 * SIZE

	addi	X1, X1, 2 * SIZE
	.align 4

LL(18):
	andi.	r0, M, 1
	ble	LL(19)

	LFD	y01, 0 * SIZE(X1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  0 * SIZE(AO2)

	FMADD	a1, alpha1, y01, a1
	FMADD	a2, alpha2, y01, a2

	STFD	a1,  0 * SIZE(AO1)
	STFD	a2,  0 * SIZE(AO2)
	.align 4

LL(19):
	addi	J, J, -1
	cmpi	cr0, 0, J, 0
	bgt	LL(11)
	.align 4

LL(20):
	andi.	J, N, 1
	ble	LL(999)
	.align 4

LL(21):
	LFD	alpha1, 0 * SIZE(Y)
	FMUL	alpha1, alpha, alpha1

	mr	AO1, A
	mr	X1, XX

	srawi.	r0,  M, 4
	mtspr	CTR, r0
	ble	LL(25)

	LFD	a1,   0 * SIZE(AO1)
	LFD	a2,   1 * SIZE(AO1)
	LFD	a3,   2 * SIZE(AO1)
	LFD	a4,   3 * SIZE(AO1)

	LFD	a5,   4 * SIZE(AO1)
	LFD	a6,   5 * SIZE(AO1)
	LFD	a7,   6 * SIZE(AO1)
	LFD	a8,   7 * SIZE(AO1)

	LFD	y01,  0 * SIZE(X1)
	LFD	y02,  1 * SIZE(X1)
	LFD	y03,  2 * SIZE(X1)
	LFD	y04,  3 * SIZE(X1)

	LFD	y05,  4 * SIZE(X1)
	LFD	y06,  5 * SIZE(X1)
	LFD	y07,  6 * SIZE(X1)
	LFD	y08,  7 * SIZE(X1)

	bdz	LL(23)
	.align 4

LL(22):
	FMADD	a1,  alpha1, y01, a1
	FMADD	a2,  alpha1, y02, a2
	FMADD	a3,  alpha1, y03, a3
	FMADD	a4,  alpha1, y04, a4

	FMADD	a5,  alpha1, y05, a5
	FMADD	a6,  alpha1, y06, a6
	FMADD	a7,  alpha1, y07, a7
	FMADD	a8,  alpha1, y08, a8

	STFD	a1,   0 * SIZE(AO1)
	STFD	a2,   1 * SIZE(AO1)
	STFD	a3,   2 * SIZE(AO1)
	STFD	a4,   3 * SIZE(AO1)

	LFD	a1,   8 * SIZE(AO1)
	LFD	a2,   9 * SIZE(AO1)
	LFD	a3,  10 * SIZE(AO1)
	LFD	a4,  11 * SIZE(AO1)

	STFD	a5,   4 * SIZE(AO1)
	STFD	a6,   5 * SIZE(AO1)
	STFD	a7,   6 * SIZE(AO1)
	STFD	a8,   7 * SIZE(AO1)

	LFD	a5,  12 * SIZE(AO1)
	LFD	a6,  13 * SIZE(AO1)
	LFD	a7,  14 * SIZE(AO1)
	LFD	a8,  15 * SIZE(AO1)

	LFD	y01,  8 * SIZE(X1)
	LFD	y02,  9 * SIZE(X1)
	LFD	y03, 10 * SIZE(X1)
	LFD	y04, 11 * SIZE(X1)

	LFD	y05, 12 * SIZE(X1)
	LFD	y06, 13 * SIZE(X1)
	LFD	y07, 14 * SIZE(X1)
	LFD	y08, 15 * SIZE(X1)

	FMADD	a1,  alpha1, y01, a1
	FMADD	a2,  alpha1, y02, a2
	FMADD	a3,  alpha1, y03, a3
	FMADD	a4,  alpha1, y04, a4

	FMADD	a5,  alpha1, y05, a5
	FMADD	a6,  alpha1, y06, a6
	FMADD	a7,  alpha1, y07, a7
	FMADD	a8,  alpha1, y08, a8

	STFD	a1,   8 * SIZE(AO1)
	STFD	a2,   9 * SIZE(AO1)
	STFD	a3,  10 * SIZE(AO1)
	STFD	a4,  11 * SIZE(AO1)

	LFD	a1,  16 * SIZE(AO1)
	LFD	a2,  17 * SIZE(AO1)
	LFD	a3,  18 * SIZE(AO1)
	LFD	a4,  19 * SIZE(AO1)

	STFD	a5,  12 * SIZE(AO1)
	STFD	a6,  13 * SIZE(AO1)
	STFD	a7,  14 * SIZE(AO1)
	STFD	a8,  15 * SIZE(AO1)

	LFD	a5,  20 * SIZE(AO1)
	LFD	a6,  21 * SIZE(AO1)
	LFD	a7,  22 * SIZE(AO1)
	LFD	a8,  23 * SIZE(AO1)

	LFD	y01, 16 * SIZE(X1)
	LFD	y02, 17 * SIZE(X1)
	LFD	y03, 18 * SIZE(X1)
	LFD	y04, 19 * SIZE(X1)

	LFD	y05, 20 * SIZE(X1)
	LFD	y06, 21 * SIZE(X1)
	LFD	y07, 22 * SIZE(X1)
	LFD	y08, 23 * SIZE(X1)

	addi	AO1, AO1, 16 * SIZE
	addi	X1, X1,   16 * SIZE

	DCBT(AO1, PREA)
	DCBT(Y1, PREY)

	bdnz+	LL(22)
	.align 4

LL(23):
	FMADD	a1,  alpha1, y01, a1
	FMADD	a2,  alpha1, y02, a2
	FMADD	a3,  alpha1, y03, a3
	FMADD	a4,  alpha1, y04, a4

	FMADD	a5,  alpha1, y05, a5
	FMADD	a6,  alpha1, y06, a6
	FMADD	a7,  alpha1, y07, a7
	FMADD	a8,  alpha1, y08, a8

	STFD	a1,   0 * SIZE(AO1)
	STFD	a2,   1 * SIZE(AO1)
	STFD	a3,   2 * SIZE(AO1)
	STFD	a4,   3 * SIZE(AO1)

	LFD	a1,   8 * SIZE(AO1)
	LFD	a2,   9 * SIZE(AO1)
	LFD	a3,  10 * SIZE(AO1)
	LFD	a4,  11 * SIZE(AO1)

	STFD	a5,   4 * SIZE(AO1)
	STFD	a6,   5 * SIZE(AO1)
	STFD	a7,   6 * SIZE(AO1)
	STFD	a8,   7 * SIZE(AO1)

	LFD	a5,  12 * SIZE(AO1)
	LFD	a6,  13 * SIZE(AO1)
	LFD	a7,  14 * SIZE(AO1)
	LFD	a8,  15 * SIZE(AO1)

	LFD	y01,  8 * SIZE(X1)
	LFD	y02,  9 * SIZE(X1)
	LFD	y03, 10 * SIZE(X1)
	LFD	y04, 11 * SIZE(X1)

	LFD	y05, 12 * SIZE(X1)
	LFD	y06, 13 * SIZE(X1)
	LFD	y07, 14 * SIZE(X1)
	LFD	y08, 15 * SIZE(X1)

	FMADD	a1,  alpha1, y01, a1
	FMADD	a2,  alpha1, y02, a2
	FMADD	a3,  alpha1, y03, a3
	FMADD	a4,  alpha1, y04, a4

	FMADD	a5,  alpha1, y05, a5
	FMADD	a6,  alpha1, y06, a6
	FMADD	a7,  alpha1, y07, a7
	FMADD	a8,  alpha1, y08, a8

	STFD	a1,   8 * SIZE(AO1)
	STFD	a2,   9 * SIZE(AO1)
	STFD	a3,  10 * SIZE(AO1)
	STFD	a4,  11 * SIZE(AO1)

	LFD	a1,  16 * SIZE(AO1)
	LFD	a2,  17 * SIZE(AO1)
	LFD	a3,  18 * SIZE(AO1)
	LFD	a4,  19 * SIZE(AO1)

	STFD	a5,  12 * SIZE(AO1)
	STFD	a6,  13 * SIZE(AO1)
	STFD	a7,  14 * SIZE(AO1)
	STFD	a8,  15 * SIZE(AO1)

	LFD	a5,  20 * SIZE(AO1)
	LFD	a6,  21 * SIZE(AO1)
	LFD	a7,  22 * SIZE(AO1)
	LFD	a8,  23 * SIZE(AO1)

	addi	AO1, AO1, 16 * SIZE
	addi	X1, X1,   16 * SIZE
	.align 4

LL(25):
	andi.	r0, M, 15
	ble	LL(999)

	andi.	r0, M, 8
	ble	LL(26)

	LFD	y01,  0 * SIZE(X1)
	LFD	y02,  1 * SIZE(X1)
	LFD	y03,  2 * SIZE(X1)
	LFD	y04,  3 * SIZE(X1)
	LFD	y05,  4 * SIZE(X1)
	LFD	y06,  5 * SIZE(X1)
	LFD	y07,  6 * SIZE(X1)
	LFD	y08,  7 * SIZE(X1)

	LFD	a1,   0 * SIZE(AO1)
	LFD	a2,   1 * SIZE(AO1)
	LFD	a3,   2 * SIZE(AO1)
	LFD	a4,   3 * SIZE(AO1)
	LFD	a5,   4 * SIZE(AO1)
	LFD	a6,   5 * SIZE(AO1)
	LFD	a7,   6 * SIZE(AO1)
	LFD	a8,   7 * SIZE(AO1)

	FMADD	a1,  alpha1, y01, a1
	FMADD	a2,  alpha1, y02, a2
	FMADD	a3,  alpha1, y03, a3
	FMADD	a4,  alpha1, y04, a4

	STFD	a1,   0 * SIZE(AO1)
	STFD	a2,   1 * SIZE(AO1)
	STFD	a3,   2 * SIZE(AO1)
	STFD	a4,   3 * SIZE(AO1)

	FMADD	a5,  alpha1, y05, a5
	FMADD	a6,  alpha1, y06, a6
	FMADD	a7,  alpha1, y07, a7
	FMADD	a8,  alpha1, y08, a8

	STFD	a5,   4 * SIZE(AO1)
	STFD	a6,   5 * SIZE(AO1)
	STFD	a7,   6 * SIZE(AO1)
	STFD	a8,   7 * SIZE(AO1)

	addi	AO1, AO1, 8 * SIZE
	addi	X1, X1, 8 * SIZE
	.align 4

LL(26):
	andi.	r0, M, 4
	ble	LL(27)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)

	LFD	y01, 0 * SIZE(X1)
	LFD	y02, 1 * SIZE(X1)
	LFD	y03, 2 * SIZE(X1)
	LFD	y04, 3 * SIZE(X1)

	FMADD	a1,  alpha1, y01, a1
	FMADD	a2,  alpha1, y02, a2
	FMADD	a3,  alpha1, y03, a3
	FMADD	a4,  alpha1, y04, a4

	STFD	a1,  0 * SIZE(AO1)
	STFD	a2,  1 * SIZE(AO1)
	STFD	a3,  2 * SIZE(AO1)
	STFD	a4,  3 * SIZE(AO1)

	addi	AO1, AO1, 4 * SIZE
	addi	X1, X1, 4 * SIZE
	.align 4

LL(27):
	andi.	r0, M, 2
	ble	LL(28)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)

	LFD	y01, 0 * SIZE(X1)
	LFD	y02, 1 * SIZE(X1)

	FMADD	a1, alpha1, y01, a1
	FMADD	a2, alpha1, y02, a2

	STFD	a1,  0 * SIZE(AO1)
	STFD	a2,  1 * SIZE(AO1)

	addi	AO1, AO1, 2 * SIZE
	addi	X1, X1, 2 * SIZE
	.align 4

LL(28):
	andi.	r0, M, 1
	ble	LL(999)

	LFD	y01, 0 * SIZE(X1)
	LFD	a1,  0 * SIZE(AO1)

	FMADD	a1, alpha1, y01, a1

	STFD	a1,  0 * SIZE(AO1)
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
