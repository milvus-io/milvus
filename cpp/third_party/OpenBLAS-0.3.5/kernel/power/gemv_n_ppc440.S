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
#define LDA8	r18

#define	Y1	r19
#define Y2	r20
#define	PREA	r21
#define YY	r22
#define BUFFER	r23

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

#define a1     f20
#define a2     f21
#define a3     f22
#define a4     f23
#define a5     f24
#define a6     f25
#define a7     f26
#define a8     f27

#define alpha  f27

#if defined(PPCG4)
#define PREFETCHSIZE_A  (3 * 4)
#endif

#if defined(POWER6)
#define PREFETCHSIZE_A  (3 * 4)
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

	slwi	LDA,  LDA,  BASE_SHIFT
	slwi	INCX, INCX, BASE_SHIFT
	slwi	INCY, INCY, BASE_SHIFT

	li	PREA, PREFETCHSIZE_A * SIZE

	cmpwi	cr0, M, 0
	ble-	LL(999)

	cmpwi	cr0, N, 0
	ble-	LL(999)

	addi	A, A, -SIZE
	sub	X, X, INCX
	sub	Y, Y, INCY

	mr	YY, Y
	lfd	f0, FZERO

	cmpi	cr0, 0, INCY, SIZE
	beq	LL(10)

	addi	YY, BUFFER, -SIZE
	addi	Y1, BUFFER, -SIZE

	addi	r0, M,  7
	srawi.	r0, r0, 3
	mtspr	CTR, r0
	.align 4

LL(02):
	STFDU	f0, 1 * SIZE(Y1)
	STFDU	f0, 1 * SIZE(Y1)
	STFDU	f0, 1 * SIZE(Y1)
	STFDU	f0, 1 * SIZE(Y1)
	STFDU	f0, 1 * SIZE(Y1)
	STFDU	f0, 1 * SIZE(Y1)
	STFDU	f0, 1 * SIZE(Y1)
	STFDU	f0, 1 * SIZE(Y1)
	bdnz	LL(02)
	.align 4

LL(10):
	srawi.	J, N, 2
	ble	LL(30)
	.align 4

LL(21):
	mr	AO1, A
	add	AO2, A,   LDA

	LFDUX	alpha1, X, INCX
	LFDUX	alpha2, X, INCX
	LFDUX	alpha3, X, INCX
	LFDUX	alpha4, X, INCX

	FMUL	alpha1, alpha, alpha1
	add	AO3, AO2, LDA
	FMUL	alpha2, alpha, alpha2
	add	AO4, AO3, LDA
	FMUL	alpha3, alpha, alpha3
	add	A,   AO4, LDA
	FMUL	alpha4, alpha, alpha4
	mr	Y1, YY
	mr	Y2, YY

	srawi.	r0,  M, 3
	mtspr	CTR, r0
	ble	LL(25)

	LFDU	y01, 1 * SIZE(Y1)
	LFDU	a1,  1 * SIZE(AO1)
	LFDU	y02, 1 * SIZE(Y1)
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	y03, 1 * SIZE(Y1)
	LFDU	a3,  1 * SIZE(AO1)
	LFDU	y04, 1 * SIZE(Y1)
	LFDU	a4,  1 * SIZE(AO1)
	LFDU	y05, 1 * SIZE(Y1)
	LFDU	a5,  1 * SIZE(AO1)
	LFDU	y06, 1 * SIZE(Y1)
	LFDU	a6,  1 * SIZE(AO1)
	LFDU	y07, 1 * SIZE(Y1)
	LFDU	a7,  1 * SIZE(AO1)
	LFDU	y08, 1 * SIZE(Y1)
	LFDU	a8,  1 * SIZE(AO1)
	bdz	LL(23)
	.align 4

LL(22):
#ifdef PPCG4
	dcbtst	Y1, PREA
#endif

	FMADD	y09, alpha1, a1, y01
	LFDU	a1,  1 * SIZE(AO2)
	FMADD	y10, alpha1, a2, y02
	LFDU	a2,  1 * SIZE(AO2)
	FMADD	y11, alpha1, a3, y03
	LFDU	a3,  1 * SIZE(AO2)
	FMADD	y12, alpha1, a4, y04
	LFDU	a4,  1 * SIZE(AO2)

	LFDU	y01, 1 * SIZE(Y1)
#ifdef PPCG4
	dcbt	AO2, PREA
#endif

	FMADD	y13, alpha1, a5, y05
	LFDU	a5,  1 * SIZE(AO2)
	FMADD	y14, alpha1, a6, y06
	LFDU	a6,  1 * SIZE(AO2)
	FMADD	y15, alpha1, a7, y07
	LFDU	a7,  1 * SIZE(AO2)
	FMADD	y16, alpha1, a8, y08
	LFDU	a8,  1 * SIZE(AO2)

	LFDU	y02, 1 * SIZE(Y1)
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO2, PREA
#endif

	FMADD	y09, alpha2, a1, y09
	LFDU	a1,  1 * SIZE(AO3)
	FMADD	y10, alpha2, a2, y10
	LFDU	a2,  1 * SIZE(AO3)
	FMADD	y11, alpha2, a3, y11
	LFDU	a3,  1 * SIZE(AO3)
	FMADD	y12, alpha2, a4, y12
	LFDU	a4,  1 * SIZE(AO3)

	LFDU	y03, 1 * SIZE(Y1)
#ifdef PPCG4
	dcbt	AO3, PREA
#endif

	FMADD	y13, alpha2, a5, y13
	LFDU	a5,  1 * SIZE(AO3)
	FMADD	y14, alpha2, a6, y14
	LFDU	a6,  1 * SIZE(AO3)
	FMADD	y15, alpha2, a7, y15
	LFDU	a7,  1 * SIZE(AO3)
	FMADD	y16, alpha2, a8, y16
	LFDU	a8,  1 * SIZE(AO3)

	LFDU	y04, 1 * SIZE(Y1)
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO3, PREA
#endif

	FMADD	y09, alpha3, a1, y09
	LFDU	a1,  1 * SIZE(AO4)
	FMADD	y10, alpha3, a2, y10
	LFDU	a2,  1 * SIZE(AO4)
	FMADD	y11, alpha3, a3, y11
	LFDU	a3,  1 * SIZE(AO4)
	FMADD	y12, alpha3, a4, y12
	LFDU	a4,  1 * SIZE(AO4)

#if defined(PPCG4) && defined(DOUBLE)
	dcbtst	Y1, PREA
#endif
	LFDU	y05, 1 * SIZE(Y1)
#ifdef PPCG4
	dcbt	AO4, PREA
#endif

	FMADD	y13, alpha3, a5, y13
	LFDU	a5,  1 * SIZE(AO4)
	FMADD	y14, alpha3, a6, y14
	LFDU	a6,  1 * SIZE(AO4)
	FMADD	y15, alpha3, a7, y15
	LFDU	a7,  1 * SIZE(AO4)
	FMADD	y16, alpha3, a8, y16
	LFDU	a8,  1 * SIZE(AO4)

	LFDU	y06, 1 * SIZE(Y1)
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO4, PREA
#endif

	FMADD	y09, alpha4, a1, y09
	LFDU	a1,  1 * SIZE(AO1)
	FMADD	y10, alpha4, a2, y10
	LFDU	a2,  1 * SIZE(AO1)
	FMADD	y11, alpha4, a3, y11
	LFDU	a3,  1 * SIZE(AO1)
	FMADD	y12, alpha4, a4, y12
	LFDU	a4,  1 * SIZE(AO1)

	LFDU	y07, 1 * SIZE(Y1)
#ifdef PPCG4
	dcbt	AO1, PREA
#endif

	STFDU	y09, 1 * SIZE(Y2)
	STFDU	y10, 1 * SIZE(Y2)
	STFDU	y11, 1 * SIZE(Y2)
	STFDU	y12, 1 * SIZE(Y2)

	FMADD	y13, alpha4, a5, y13
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y14, alpha4, a6, y14
	LFDU	a6,  1 * SIZE(AO1)
	FMADD	y15, alpha4, a7, y15
	LFDU	a7,  1 * SIZE(AO1)
	FMADD	y16, alpha4, a8, y16
	LFDU	a8,  1 * SIZE(AO1)

	LFDU	y08, 1 * SIZE(Y1)
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO1, PREA
#endif

	STFDU	y13, 1 * SIZE(Y2)
	STFDU	y14, 1 * SIZE(Y2)
	STFDU	y15, 1 * SIZE(Y2)
	STFDU	y16, 1 * SIZE(Y2)
	bdnz	LL(22)
	.align 4

LL(23):
	FMADD	y01, alpha1, a1, y01
	LFDU	a1,  1 * SIZE(AO2)
	FMADD	y02, alpha1, a2, y02
	LFDU	a2,  1 * SIZE(AO2)
	FMADD	y03, alpha1, a3, y03
	LFDU	a3,  1 * SIZE(AO2)
	FMADD	y04, alpha1, a4, y04
	LFDU	a4,  1 * SIZE(AO2)

	FMADD	y05, alpha1, a5, y05
	LFDU	a5,  1 * SIZE(AO2)
	FMADD	y06, alpha1, a6, y06
	LFDU	a6,  1 * SIZE(AO2)
	FMADD	y07, alpha1, a7, y07
	LFDU	a7,  1 * SIZE(AO2)
	FMADD	y08, alpha1, a8, y08
	LFDU	a8,  1 * SIZE(AO2)

	FMADD	y01, alpha2, a1, y01
	LFDU	a1,  1 * SIZE(AO3)
	FMADD	y02, alpha2, a2, y02
	LFDU	a2,  1 * SIZE(AO3)
	FMADD	y03, alpha2, a3, y03
	LFDU	a3,  1 * SIZE(AO3)
	FMADD	y04, alpha2, a4, y04
	LFDU	a4,  1 * SIZE(AO3)

	FMADD	y05, alpha2, a5, y05
	LFDU	a5,  1 * SIZE(AO3)
	FMADD	y06, alpha2, a6, y06
	LFDU	a6,  1 * SIZE(AO3)
	FMADD	y07, alpha2, a7, y07
	LFDU	a7,  1 * SIZE(AO3)
	FMADD	y08, alpha2, a8, y08
	LFDU	a8,  1 * SIZE(AO3)

	FMADD	y01, alpha3, a1, y01
	LFDU	a1,  1 * SIZE(AO4)
	FMADD	y02, alpha3, a2, y02
	LFDU	a2,  1 * SIZE(AO4)
	FMADD	y03, alpha3, a3, y03
	LFDU	a3,  1 * SIZE(AO4)
	FMADD	y04, alpha3, a4, y04
	LFDU	a4,  1 * SIZE(AO4)

	FMADD	y05, alpha3, a5, y05
	LFDU	a5,  1 * SIZE(AO4)
	FMADD	y06, alpha3, a6, y06
	LFDU	a6,  1 * SIZE(AO4)
	FMADD	y07, alpha3, a7, y07
	LFDU	a7,  1 * SIZE(AO4)
	FMADD	y08, alpha3, a8, y08
	LFDU	a8,  1 * SIZE(AO4)

	FMADD	y01, alpha4, a1, y01
	FMADD	y02, alpha4, a2, y02
	FMADD	y03, alpha4, a3, y03
	FMADD	y04, alpha4, a4, y04

	FMADD	y05, alpha4, a5, y05
	STFDU	y01, 1 * SIZE(Y2)
	FMADD	y06, alpha4, a6, y06
	STFDU	y02, 1 * SIZE(Y2)
	FMADD	y07, alpha4, a7, y07
	STFDU	y03, 1 * SIZE(Y2)
	FMADD	y08, alpha4, a8, y08
	STFDU	y04, 1 * SIZE(Y2)

	STFDU	y05, 1 * SIZE(Y2)
	STFDU	y06, 1 * SIZE(Y2)
	STFDU	y07, 1 * SIZE(Y2)
	STFDU	y08, 1 * SIZE(Y2)
	.align 4

LL(25):
	andi.	r0, M, 7
	ble	LL(29)

	andi.	r0, M, 4
	ble	LL(27)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	y01, 1 * SIZE(Y1)
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	y02, 1 * SIZE(Y1)
	LFDU	a3,  1 * SIZE(AO1)
	LFDU	y03, 1 * SIZE(Y1)
	LFDU	a4,  1 * SIZE(AO1)
	LFDU	y04, 1 * SIZE(Y1)

	FMADD	y01, alpha1, a1, y01
	LFDU	a5,  1 * SIZE(AO2)
	FMADD	y02, alpha1, a2, y02
	LFDU	a6,  1 * SIZE(AO2)
	FMADD	y03, alpha1, a3, y03
	LFDU	a7,  1 * SIZE(AO2)
	FMADD	y04, alpha1, a4, y04
	LFDU	a8,  1 * SIZE(AO2)

	FMADD	y01, alpha2, a5, y01
	LFDU	a1,  1 * SIZE(AO3)
	FMADD	y02, alpha2, a6, y02
	LFDU	a2,  1 * SIZE(AO3)
	FMADD	y03, alpha2, a7, y03
	LFDU	a3,  1 * SIZE(AO3)
	FMADD	y04, alpha2, a8, y04
	LFDU	a4,  1 * SIZE(AO3)

	FMADD	y01, alpha3, a1, y01
	LFDU	a5,  1 * SIZE(AO4)
	FMADD	y02, alpha3, a2, y02
	LFDU	a6,  1 * SIZE(AO4)
	FMADD	y03, alpha3, a3, y03
	LFDU	a7,  1 * SIZE(AO4)
	FMADD	y04, alpha3, a4, y04
	LFDU	a8,  1 * SIZE(AO4)

	FMADD	y01, alpha4, a5, y01
	FMADD	y02, alpha4, a6, y02
	FMADD	y03, alpha4, a7, y03
	FMADD	y04, alpha4, a8, y04

	STFDU	y01, 1 * SIZE(Y2)
	STFDU	y02, 1 * SIZE(Y2)
	STFDU	y03, 1 * SIZE(Y2)
	STFDU	y04, 1 * SIZE(Y2)
	.align 4

LL(27):
	andi.	r0, M, 2
	ble	LL(28)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	y01, 1 * SIZE(Y1)
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	y02, 1 * SIZE(Y1)
	LFDU	a3,  1 * SIZE(AO2)
	LFDU	a4,  1 * SIZE(AO2)

	FMADD	y01, alpha1, a1, y01
	LFDU	a5,  1 * SIZE(AO3)
	FMADD	y02, alpha1, a2, y02
	LFDU	a6,  1 * SIZE(AO3)
	FMADD	y01, alpha2, a3, y01
	LFDU	a7,  1 * SIZE(AO4)
	FMADD	y02, alpha2, a4, y02
	LFDU	a8,  1 * SIZE(AO4)

	FMADD	y01, alpha3, a5, y01
	FMADD	y02, alpha3, a6, y02
	FMADD	y01, alpha4, a7, y01
	FMADD	y02, alpha4, a8, y02

	STFDU	y01, 1 * SIZE(Y2)
	STFDU	y02, 1 * SIZE(Y2)
	.align 4

LL(28):
	andi.	r0, M, 1
	ble	LL(29)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	y01, 1 * SIZE(Y1)
	LFDU	a2,  1 * SIZE(AO2)
	LFDU	a3,  1 * SIZE(AO3)
	LFDU	a4,  1 * SIZE(AO4)

	FMADD	y01, alpha1, a1, y01
	FMADD	y01, alpha2, a2, y01
	FMADD	y01, alpha3, a3, y01
	FMADD	y01, alpha4, a4, y01

	STFDU	y01, 1 * SIZE(Y2)
	.align 4

LL(29):
	addi	J, J, -1
	lfd	alpha, ALPHA
	cmpi	cr0, 0, J, 0
	bgt	LL(21)
	.align 4

LL(30):
	andi.	J, N, 2
	ble	LL(40)

	LFDUX	alpha1, X, INCX
	LFDUX	alpha2, X, INCX

	mr	AO1, A
	add	AO2, A,   LDA
	add	A,   AO2, LDA

	FMUL	alpha1, alpha, alpha1
	mr	Y1, YY
	FMUL	alpha2, alpha, alpha2
	mr	Y2, YY

	srawi.	r0,  M, 3
	mtspr	CTR, r0
	ble	LL(35)

	LFDU	y01, 1 * SIZE(Y1)
	LFDU	a1,  1 * SIZE(AO1)
	LFDU	y02, 1 * SIZE(Y1)
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	y03, 1 * SIZE(Y1)
	LFDU	a3,  1 * SIZE(AO1)
	LFDU	y04, 1 * SIZE(Y1)
	LFDU	a4,  1 * SIZE(AO1)
	LFDU	y05, 1 * SIZE(Y1)
	LFDU	a5,  1 * SIZE(AO1)
	LFDU	y06, 1 * SIZE(Y1)
	LFDU	a6,  1 * SIZE(AO1)
	LFDU	y07, 1 * SIZE(Y1)
	LFDU	a7,  1 * SIZE(AO1)
	LFDU	y08, 1 * SIZE(Y1)
	LFDU	a8,  1 * SIZE(AO1)
	bdz	LL(33)
	.align 4

LL(32):
#ifdef PPCG4
	dcbtst	Y1, PREA
#endif

	FMADD	y09, alpha1, a1, y01
	LFDU	a1,  1 * SIZE(AO2)
	FMADD	y10, alpha1, a2, y02
	LFDU	a2,  1 * SIZE(AO2)
	FMADD	y11, alpha1, a3, y03
	LFDU	a3,  1 * SIZE(AO2)
	FMADD	y12, alpha1, a4, y04
	LFDU	a4,  1 * SIZE(AO2)

	LFDU	y01, 1 * SIZE(Y1)
	LFDU	y02, 1 * SIZE(Y1)
#ifdef PPCG4
	dcbt	AO2, PREA
#endif

	FMADD	y13, alpha1, a5, y05
	LFDU	a5,  1 * SIZE(AO2)
	FMADD	y14, alpha1, a6, y06
	LFDU	a6,  1 * SIZE(AO2)
	FMADD	y15, alpha1, a7, y07
	LFDU	a7,  1 * SIZE(AO2)
	FMADD	y16, alpha1, a8, y08
	LFDU	a8,  1 * SIZE(AO2)

	LFDU	y03, 1 * SIZE(Y1)
	LFDU	y04, 1 * SIZE(Y1)
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO2, PREA
#endif

	FMADD	y09, alpha2, a1, y09
	LFDU	a1,  1 * SIZE(AO1)
	FMADD	y10, alpha2, a2, y10
	LFDU	a2,  1 * SIZE(AO1)
	FMADD	y11, alpha2, a3, y11
	LFDU	a3,  1 * SIZE(AO1)
	FMADD	y12, alpha2, a4, y12
	LFDU	a4,  1 * SIZE(AO1)

#if defined(PPCG4) && defined(DOUBLE)
	dcbtst	Y1, PREA
#endif
	LFDU	y05, 1 * SIZE(Y1)
	LFDU	y06, 1 * SIZE(Y1)
#ifdef PPCG4
	dcbt	AO1, PREA
#endif

	FMADD	y13, alpha2, a5, y13
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y14, alpha2, a6, y14
	LFDU	a6,  1 * SIZE(AO1)
	FMADD	y15, alpha2, a7, y15
	LFDU	a7,  1 * SIZE(AO1)
	FMADD	y16, alpha2, a8, y16
	LFDU	a8,  1 * SIZE(AO1)

	LFDU	y07, 1 * SIZE(Y1)
	LFDU	y08, 1 * SIZE(Y1)
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO1, PREA
#endif

	STFDU	y09, 1 * SIZE(Y2)
	STFDU	y10, 1 * SIZE(Y2)
	STFDU	y11, 1 * SIZE(Y2)
	STFDU	y12, 1 * SIZE(Y2)

	STFDU	y13, 1 * SIZE(Y2)
	STFDU	y14, 1 * SIZE(Y2)
	STFDU	y15, 1 * SIZE(Y2)
	STFDU	y16, 1 * SIZE(Y2)
	bdnz	LL(32)
	.align 4

LL(33):
	FMADD	y01, alpha1, a1, y01
	LFDU	a1,  1 * SIZE(AO2)
	FMADD	y02, alpha1, a2, y02
	LFDU	a2,  1 * SIZE(AO2)
	FMADD	y03, alpha1, a3, y03
	LFDU	a3,  1 * SIZE(AO2)
	FMADD	y04, alpha1, a4, y04
	LFDU	a4,  1 * SIZE(AO2)

	FMADD	y05, alpha1, a5, y05
	LFDU	a5,  1 * SIZE(AO2)
	FMADD	y06, alpha1, a6, y06
	LFDU	a6,  1 * SIZE(AO2)
	FMADD	y07, alpha1, a7, y07
	LFDU	a7,  1 * SIZE(AO2)
	FMADD	y08, alpha1, a8, y08
	LFDU	a8,  1 * SIZE(AO2)

	FMADD	y01, alpha2, a1, y01
	FMADD	y02, alpha2, a2, y02
	FMADD	y03, alpha2, a3, y03
	FMADD	y04, alpha2, a4, y04

	FMADD	y05, alpha2, a5, y05
	STFDU	y01, 1 * SIZE(Y2)
	FMADD	y06, alpha2, a6, y06
	STFDU	y02, 1 * SIZE(Y2)
	FMADD	y07, alpha2, a7, y07
	STFDU	y03, 1 * SIZE(Y2)
	FMADD	y08, alpha2, a8, y08
	STFDU	y04, 1 * SIZE(Y2)

	STFDU	y05, 1 * SIZE(Y2)
	STFDU	y06, 1 * SIZE(Y2)
	STFDU	y07, 1 * SIZE(Y2)
	STFDU	y08, 1 * SIZE(Y2)
	.align 4

LL(35):
	andi.	r0, M, 7
	ble	LL(40)

	andi.	r0, M, 4
	ble	LL(37)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	y01, 1 * SIZE(Y1)
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	y02, 1 * SIZE(Y1)
	LFDU	a3,  1 * SIZE(AO1)
	LFDU	y03, 1 * SIZE(Y1)
	LFDU	a4,  1 * SIZE(AO1)
	LFDU	y04, 1 * SIZE(Y1)

	FMADD	y01, alpha1, a1, y01
	LFDU	a5,  1 * SIZE(AO2)
	FMADD	y02, alpha1, a2, y02
	LFDU	a6,  1 * SIZE(AO2)
	FMADD	y03, alpha1, a3, y03
	LFDU	a7,  1 * SIZE(AO2)
	FMADD	y04, alpha1, a4, y04
	LFDU	a8,  1 * SIZE(AO2)

	FMADD	y01, alpha2, a5, y01
	FMADD	y02, alpha2, a6, y02
	FMADD	y03, alpha2, a7, y03
	FMADD	y04, alpha2, a8, y04

	STFDU	y01, 1 * SIZE(Y2)
	STFDU	y02, 1 * SIZE(Y2)
	STFDU	y03, 1 * SIZE(Y2)
	STFDU	y04, 1 * SIZE(Y2)
	.align 4

LL(37):
	andi.	r0, M, 2
	ble	LL(38)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	y01, 1 * SIZE(Y1)
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	y02, 1 * SIZE(Y1)
	LFDU	a3,  1 * SIZE(AO2)
	LFDU	a4,  1 * SIZE(AO2)

	FMADD	y01, alpha1, a1, y01
	FMADD	y02, alpha1, a2, y02
	FMADD	y01, alpha2, a3, y01
	FMADD	y02, alpha2, a4, y02

	STFDU	y01, 1 * SIZE(Y2)
	STFDU	y02, 1 * SIZE(Y2)
	.align 4

LL(38):
	andi.	r0, M, 1
	ble	LL(40)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	y01, 1 * SIZE(Y1)
	LFDU	a2,  1 * SIZE(AO2)

	FMADD	y01, alpha1, a1, y01
	FMADD	y01, alpha2, a2, y01

	STFDU	y01, 1 * SIZE(Y2)
	.align 4

LL(40):
	andi.	J, N, 1
	lfd	alpha, ALPHA
	ble	LL(990)

	LFDUX	alpha1, X, INCX

	mr	AO1, A
	add	A,   A, LDA

	FMUL	alpha1, alpha, alpha1
	mr	Y1, YY
	mr	Y2, YY

	srawi.	r0,  M, 3
	mtspr	CTR, r0
	ble	LL(45)

	LFDU	y01, 1 * SIZE(Y1)
	LFDU	a1,  1 * SIZE(AO1)
	LFDU	y02, 1 * SIZE(Y1)
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	y03, 1 * SIZE(Y1)
	LFDU	a3,  1 * SIZE(AO1)
	LFDU	y04, 1 * SIZE(Y1)
	LFDU	a4,  1 * SIZE(AO1)
	LFDU	y05, 1 * SIZE(Y1)
	LFDU	a5,  1 * SIZE(AO1)
	LFDU	y06, 1 * SIZE(Y1)
	LFDU	a6,  1 * SIZE(AO1)
	LFDU	y07, 1 * SIZE(Y1)
	LFDU	a7,  1 * SIZE(AO1)
	LFDU	y08, 1 * SIZE(Y1)
	LFDU	a8,  1 * SIZE(AO1)
	bdz	LL(43)
	.align 4

LL(42):
#ifdef PPCG4
	dcbtst	Y1, PREA
#endif

	FMADD	y09, alpha1, a1, y01
	LFDU	a1,  1 * SIZE(AO1)
	FMADD	y10, alpha1, a2, y02
	LFDU	a2,  1 * SIZE(AO1)
	FMADD	y11, alpha1, a3, y03
	LFDU	a3,  1 * SIZE(AO1)
	FMADD	y12, alpha1, a4, y04
	LFDU	a4,  1 * SIZE(AO1)

	LFDU	y01, 1 * SIZE(Y1)
	LFDU	y02, 1 * SIZE(Y1)
	LFDU	y03, 1 * SIZE(Y1)
	LFDU	y04, 1 * SIZE(Y1)
#ifdef PPCG4
	dcbt	AO1, PREA
#endif

	FMADD	y13, alpha1, a5, y05
	LFDU	a5,  1 * SIZE(AO1)
	FMADD	y14, alpha1, a6, y06
	LFDU	a6,  1 * SIZE(AO1)
	FMADD	y15, alpha1, a7, y07
	LFDU	a7,  1 * SIZE(AO1)
	FMADD	y16, alpha1, a8, y08
	LFDU	a8,  1 * SIZE(AO1)

#if defined(PPCG4) && defined(DOUBLE)
	dcbtst	Y1, PREA
#endif
	LFDU	y05, 1 * SIZE(Y1)
	LFDU	y06, 1 * SIZE(Y1)
	LFDU	y07, 1 * SIZE(Y1)
	LFDU	y08, 1 * SIZE(Y1)
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO1, PREA
#endif

	STFDU	y09, 1 * SIZE(Y2)
	STFDU	y10, 1 * SIZE(Y2)
	STFDU	y11, 1 * SIZE(Y2)
	STFDU	y12, 1 * SIZE(Y2)

	STFDU	y13, 1 * SIZE(Y2)
	STFDU	y14, 1 * SIZE(Y2)
	STFDU	y15, 1 * SIZE(Y2)
	STFDU	y16, 1 * SIZE(Y2)
	bdnz	LL(42)
	.align 4

LL(43):
	FMADD	y01, alpha1, a1, y01
	FMADD	y02, alpha1, a2, y02
	FMADD	y03, alpha1, a3, y03
	FMADD	y04, alpha1, a4, y04

	FMADD	y05, alpha1, a5, y05
	STFDU	y01, 1 * SIZE(Y2)
	FMADD	y06, alpha1, a6, y06
	STFDU	y02, 1 * SIZE(Y2)
	FMADD	y07, alpha1, a7, y07
	STFDU	y03, 1 * SIZE(Y2)
	FMADD	y08, alpha1, a8, y08
	STFDU	y04, 1 * SIZE(Y2)

	STFDU	y05, 1 * SIZE(Y2)
	STFDU	y06, 1 * SIZE(Y2)
	STFDU	y07, 1 * SIZE(Y2)
	STFDU	y08, 1 * SIZE(Y2)
	.align 4

LL(45):
	andi.	r0, M, 7
	ble	LL(990)

	andi.	r0, M, 4
	ble	LL(47)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	y01, 1 * SIZE(Y1)
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	y02, 1 * SIZE(Y1)
	LFDU	a3,  1 * SIZE(AO1)
	LFDU	y03, 1 * SIZE(Y1)
	LFDU	a4,  1 * SIZE(AO1)
	LFDU	y04, 1 * SIZE(Y1)

	FMADD	y01, alpha1, a1, y01
	FMADD	y02, alpha1, a2, y02
	FMADD	y03, alpha1, a3, y03
	FMADD	y04, alpha1, a4, y04

	STFDU	y01, 1 * SIZE(Y2)
	STFDU	y02, 1 * SIZE(Y2)
	STFDU	y03, 1 * SIZE(Y2)
	STFDU	y04, 1 * SIZE(Y2)
	.align 4

LL(47):
	andi.	r0, M, 2
	ble	LL(48)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	y01, 1 * SIZE(Y1)
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	y02, 1 * SIZE(Y1)

	FMADD	y01, alpha1, a1, y01
	FMADD	y02, alpha1, a2, y02

	STFDU	y01, 1 * SIZE(Y2)
	STFDU	y02, 1 * SIZE(Y2)
	.align 4

LL(48):
	andi.	r0, M, 1
	ble	LL(990)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	y01, 1 * SIZE(Y1)

	FMADD	y01, alpha1, a1, y01

	STFDU	y01, 1 * SIZE(Y2)
	.align 4

LL(990):
	cmpi	cr0, 0, INCY, SIZE
	beq	LL(999)

	addi	YY, BUFFER, -SIZE
	mr	Y1, Y

	srawi.	r0, M, 3
	mtspr	CTR, r0
	ble	LL(995)
	.align 4

LL(991):
	LFDUX	f0, Y, INCY
	LFDUX	f1, Y, INCY
	LFDUX	f2, Y, INCY
	LFDUX	f3, Y, INCY
	LFDUX	f4, Y, INCY
	LFDUX	f5, Y, INCY
	LFDUX	f6, Y, INCY
	LFDUX	f7, Y, INCY

	LFDU	f8,   1 * SIZE(YY)
	LFDU	f9,   1 * SIZE(YY)
	LFDU	f10,  1 * SIZE(YY)
	LFDU	f11,  1 * SIZE(YY)
	LFDU	f12,  1 * SIZE(YY)
	LFDU	f13,  1 * SIZE(YY)
	LFDU	f14,  1 * SIZE(YY)
	LFDU	f15,  1 * SIZE(YY)

	FADD	f8,  f8,  f0
	FADD	f9,  f9,  f1
	FADD	f10, f10, f2
	FADD	f11, f11, f3
	FADD	f12, f12, f4
	FADD	f13, f13, f5
	FADD	f14, f14, f6
	FADD	f15, f15, f7

	STFDUX	f8,  Y1, INCY
	STFDUX	f9,  Y1, INCY
	STFDUX	f10, Y1, INCY
	STFDUX	f11, Y1, INCY
	STFDUX	f12, Y1, INCY
	STFDUX	f13, Y1, INCY
	STFDUX	f14, Y1, INCY
	STFDUX	f15, Y1, INCY
	bdnz	LL(991)
	.align 4

LL(995):
	andi.	J, M, 4
	ble	LL(996)

	LFDUX	f0, Y, INCY
	LFDUX	f1, Y, INCY
	LFDUX	f2, Y, INCY
	LFDUX	f3, Y, INCY

	LFDU	f8,   1 * SIZE(YY)
	LFDU	f9,   1 * SIZE(YY)
	LFDU	f10,  1 * SIZE(YY)
	LFDU	f11,  1 * SIZE(YY)

	FADD	f8,  f8,  f0
	FADD	f9,  f9,  f1
	FADD	f10, f10, f2
	FADD	f11, f11, f3

	STFDUX	f8,  Y1, INCY
	STFDUX	f9,  Y1, INCY
	STFDUX	f10, Y1, INCY
	STFDUX	f11, Y1, INCY
	.align 4

LL(996):
	andi.	J, M, 2
	ble	LL(997)

	LFDUX	f0, Y, INCY
	LFDUX	f1, Y, INCY

	LFDU	f8,   1 * SIZE(YY)
	LFDU	f9,   1 * SIZE(YY)

	FADD	f8,  f8,  f0
	FADD	f9,  f9,  f1

	STFDUX	f8,  Y1, INCY
	STFDUX	f9,  Y1, INCY
	.align 4

LL(997):
	andi.	J, M, 1
	ble	LL(999)

	LFDUX	f0, Y, INCY
	LFDU	f8,   1 * SIZE(YY)

	FADD	f8,  f8,  f0

	STFDUX	f8,  Y1, INCY
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
#endif

	addi	SP, SP, STACKSIZE
	blr

	EPILOGUE
#endif
