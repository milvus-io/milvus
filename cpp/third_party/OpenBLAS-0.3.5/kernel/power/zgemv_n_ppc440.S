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

#define I	r11
#define	J	r12

#define AO1	r14
#define AO2	r15
#define AO3	r16
#define AO4	r17

#define	Y1	r18
#define Y2	r19
#define	PREA	r20
#define YY	r21
#define BUFFER	r22

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

#define alpha1r f16
#define alpha1i f17
#define alpha2r f18
#define alpha2i f19
#define alpha3r f20
#define alpha3i f21
#define alpha4r f22
#define alpha4i f23

#define a1     f24
#define a2     f25
#define a3     f26
#define a4     f27
#define a5     f28
#define a6     f29
#define a7     f30
#define a8     f31

#define alpha_r f14
#define alpha_i f15

#if defined(PPCG4)
#define PREFETCHSIZE_A  (3 * 4)
#endif

#if defined(POWER6)
#define PREFETCHSIZE_A  (3 * 4)
#endif

#ifndef XCONJ
#define FMADDR FMADD
#define FMSUBR FNMSUB
#else
#define FMADDR FNMSUB
#define FMSUBR FMADD
#endif

#ifndef  CONJ
#define FMADDX FMADD
#define FMSUBX FNMSUB
#else
#define FMADDX FNMSUB
#define FMSUBX FMADD
#endif

#ifndef NEEDPARAM

#ifndef __64BIT__
#define STACKSIZE 232
#define ALPHA_R	  208(SP)
#define ALPHA_I	  216(SP)
#define	FZERO	  224(SP)
#else
#define STACKSIZE 280
#define ALPHA_R   256(SP)
#define	ALPHA_I	  264(SP)
#define	FZERO	  272(SP)
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

	slwi	LDA,  LDA,  ZBASE_SHIFT
	slwi	INCX, INCX, ZBASE_SHIFT
	slwi	INCY, INCY, ZBASE_SHIFT

	addi	INCX, INCX, -SIZE
	addi	INCY, INCY, -SIZE
	addi	A, A, -SIZE

	cmpwi	cr0, M, 0
	ble-	LL(999)

	sub	X, X, INCX
	cmpwi	cr0, N, 0
	sub	Y, Y, INCY
	ble-	LL(999)

	li	PREA, PREFETCHSIZE_A * SIZE

	mr	YY, Y
	lfd	f0, FZERO

	cmpi	cr0, 0, INCY, SIZE
	beq	LL(10)

	addi	YY, BUFFER, -SIZE
	addi	Y1, BUFFER, -SIZE

	addi	r0, M,  3
	srawi.	r0, r0, 2
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
	ble	LL(20)
	.align 4

LL(11):
	lfd	alpha_r, ALPHA_R
	lfd	alpha_i, ALPHA_I

	LFDUX	a1, X, INCX
	LFDU	a2, 1 * SIZE(X)
	LFDUX	a3, X, INCX
	LFDU	a4, 1 * SIZE(X)
	LFDUX	a5, X, INCX
	LFDU	a6, 1 * SIZE(X)
	LFDUX	a7, X, INCX
	LFDU	a8, 1 * SIZE(X)

	FMUL	alpha1r, alpha_r, a1
	FMUL	alpha1i, alpha_i, a1
	FMUL	alpha2r, alpha_r, a3
	FMUL	alpha2i, alpha_i, a3

	FMUL	alpha3r, alpha_r, a5
	mr	Y1, YY
	FMUL	alpha3i, alpha_i, a5
	mr	Y2, YY
	FMUL	alpha4r, alpha_r, a7
	mr	AO1, A
	FMUL	alpha4i, alpha_i, a7
	add	AO2, A,   LDA

	FMSUBR	alpha1r, alpha_i, a2, alpha1r
	add	AO3, AO2, LDA
	FMADDR	alpha1i, alpha_r, a2, alpha1i
	add	AO4, AO3, LDA
	FMSUBR	alpha2r, alpha_i, a4, alpha2r
	add	A,   AO4, LDA
	FMADDR	alpha2i, alpha_r, a4, alpha2i

	FMSUBR	alpha3r, alpha_i, a6, alpha3r
	srawi.	r0,  M, 2
	FMADDR	alpha3i, alpha_r, a6, alpha3i
	FMSUBR	alpha4r, alpha_i, a8, alpha4r
	mtspr	CTR, r0
	FMADDR	alpha4i, alpha_r, a8, alpha4i
	ble	LL(15)
	.align 4

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	y01, 1 * SIZE(Y1)
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	y02, 1 * SIZE(Y1)
	LFDU	a3,  1 * SIZE(AO1)
	LFDU	y03, 1 * SIZE(Y1)
	LFDU	a4,  1 * SIZE(AO1)
	LFDU	y04, 1 * SIZE(Y1)

	LFDU	a5,  1 * SIZE(AO1)
	LFDU	y05, 1 * SIZE(Y1)
	LFDU	a6,  1 * SIZE(AO1)
	LFDU	y06, 1 * SIZE(Y1)
	LFDU	a7,  1 * SIZE(AO1)
	LFDU	y07, 1 * SIZE(Y1)
	LFDU	a8,  1 * SIZE(AO1)
	LFDU	y08, 1 * SIZE(Y1)

	FMADD	y09, alpha1r, a1, y01
	FMADD	y10, alpha1i, a1, y02
	FMADD	y11, alpha1r, a3, y03
	FMADD	y12, alpha1i, a3, y04

	FMADD	y13, alpha1r, a5, y05
	FMADD	y14, alpha1i, a5, y06
	FMADD	y15, alpha1r, a7, y07
	FMADD	y16, alpha1i, a7, y08

	bdz	LL(13)
	.align 4

LL(12):
	FMSUBX	y09, alpha1i, a2, y09
	LFDU	a1,  1 * SIZE(AO2)
	FMADDX	y10, alpha1r, a2, y10
	LFDU	a2,  1 * SIZE(AO2)
	FMSUBX	y11, alpha1i, a4, y11
	LFDU	a3,  1 * SIZE(AO2)
	FMADDX	y12, alpha1r, a4, y12
	LFDU	a4,  1 * SIZE(AO2)
#ifdef PPCG4
	dcbt	AO2, PREA
#endif

	FMSUBX	y13, alpha1i, a6, y13
	LFDU	a5,  1 * SIZE(AO2)
	FMADDX	y14, alpha1r, a6, y14
	LFDU	a6,  1 * SIZE(AO2)
	FMSUBX	y15, alpha1i, a8, y15
	LFDU	a7,  1 * SIZE(AO2)
	FMADDX	y16, alpha1r, a8, y16
	LFDU	a8,  1 * SIZE(AO2)
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO2, PREA
#endif

	FMADD	y09, alpha2r, a1, y09
	LFDU	y01, 1 * SIZE(Y1)
	FMADD	y10, alpha2i, a1, y10
	LFDU	y02, 1 * SIZE(Y1)
	FMADD	y11, alpha2r, a3, y11
	LFDU	y03, 1 * SIZE(Y1)
	FMADD	y12, alpha2i, a3, y12
	LFDU	y04, 1 * SIZE(Y1)

#ifdef PPCG4
	dcbtst	Y1, PREA
#endif

	FMADD	y13, alpha2r, a5, y13
	FMADD	y14, alpha2i, a5, y14
	FMADD	y15, alpha2r, a7, y15
	FMADD	y16, alpha2i, a7, y16

	FMSUBX	y09, alpha2i, a2, y09
	LFDU	a1,  1 * SIZE(AO3)
	FMADDX	y10, alpha2r, a2, y10
	LFDU	a2,  1 * SIZE(AO3)
	FMSUBX	y11, alpha2i, a4, y11
	LFDU	a3,  1 * SIZE(AO3)
	FMADDX	y12, alpha2r, a4, y12
	LFDU	a4,  1 * SIZE(AO3)

#ifdef PPCG4
	dcbt	AO3, PREA
#endif

	FMSUBX	y13, alpha2i, a6, y13
	LFDU	a5,  1 * SIZE(AO3)
	FMADDX	y14, alpha2r, a6, y14
	LFDU	a6,  1 * SIZE(AO3)
	FMSUBX	y15, alpha2i, a8, y15
	LFDU	a7,  1 * SIZE(AO3)
	FMADDX	y16, alpha2r, a8, y16
	LFDU	a8,  1 * SIZE(AO3)

#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO3, PREA
#endif

	FMADD	y09, alpha3r, a1, y09
	LFDU	y05, 1 * SIZE(Y1)
	FMADD	y10, alpha3i, a1, y10
	LFDU	y06, 1 * SIZE(Y1)
	FMADD	y11, alpha3r, a3, y11
	LFDU	y07, 1 * SIZE(Y1)
	FMADD	y12, alpha3i, a3, y12
	LFDU	y08, 1 * SIZE(Y1)

#if defined(PPCG4) && defined(DOUBLE)
	dcbtst	Y1, PREA
#endif

	FMADD	y13, alpha3r, a5, y13
	FMADD	y14, alpha3i, a5, y14
	FMADD	y15, alpha3r, a7, y15
	FMADD	y16, alpha3i, a7, y16

	FMSUBX	y09, alpha3i, a2, y09
	LFDU	a1,  1 * SIZE(AO4)
	FMADDX	y10, alpha3r, a2, y10
	LFDU	a2,  1 * SIZE(AO4)
	FMSUBX	y11, alpha3i, a4, y11
	LFDU	a3,  1 * SIZE(AO4)
	FMADDX	y12, alpha3r, a4, y12
	LFDU	a4,  1 * SIZE(AO4)

#ifdef PPCG4
	dcbt	AO4, PREA
#endif

	FMSUBX	y13, alpha3i, a6, y13
	LFDU	a5,  1 * SIZE(AO4)
	FMADDX	y14, alpha3r, a6, y14
	LFDU	a6,  1 * SIZE(AO4)
	FMSUBX	y15, alpha3i, a8, y15
	LFDU	a7,  1 * SIZE(AO4)
	FMADDX	y16, alpha3r, a8, y16
	LFDU	a8,  1 * SIZE(AO4)

#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO4, PREA
#endif

	FMADD	y09, alpha4r, a1, y09
	FMADD	y10, alpha4i, a1, y10
	FMADD	y11, alpha4r, a3, y11
	FMADD	y12, alpha4i, a3, y12

	FMADD	y13, alpha4r, a5, y13
	FMADD	y14, alpha4i, a5, y14
	FMADD	y15, alpha4r, a7, y15
	FMADD	y16, alpha4i, a7, y16

	FMSUBX	y09, alpha4i, a2, y09
	LFDU	a1,  1 * SIZE(AO1)
	FMADDX	y10, alpha4r, a2, y10
	LFDU	a2,  1 * SIZE(AO1)
	FMSUBX	y11, alpha4i, a4, y11
	LFDU	a3,  1 * SIZE(AO1)
	FMADDX	y12, alpha4r, a4, y12
	LFDU	a4,  1 * SIZE(AO1)

#ifdef PPCG4
	dcbt	AO1, PREA
#endif

	FMSUBX	y13, alpha4i, a6, y13
	LFDU	a5,  1 * SIZE(AO1)
	FMADDX	y14, alpha4r, a6, y14
	LFDU	a6,  1 * SIZE(AO1)
	FMSUBX	y15, alpha4i, a8, y15
	LFDU	a7,  1 * SIZE(AO1)
	FMADDX	y16, alpha4r, a8, y16
	LFDU	a8,  1 * SIZE(AO1)

#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO1, PREA
#endif

	STFDU	y09, 1 * SIZE(Y2)
	FMADD	y09, alpha1r, a1, y01
	STFDU	y10, 1 * SIZE(Y2)
	FMADD	y10, alpha1i, a1, y02
	STFDU	y11, 1 * SIZE(Y2)
	FMADD	y11, alpha1r, a3, y03
	STFDU	y12, 1 * SIZE(Y2)
	FMADD	y12, alpha1i, a3, y04

	STFDU	y13, 1 * SIZE(Y2)
	FMADD	y13, alpha1r, a5, y05
	STFDU	y14, 1 * SIZE(Y2)
	FMADD	y14, alpha1i, a5, y06
	STFDU	y15, 1 * SIZE(Y2)
	FMADD	y15, alpha1r, a7, y07
	STFDU	y16, 1 * SIZE(Y2)
	FMADD	y16, alpha1i, a7, y08
	bdnz	LL(12)
	.align 4

LL(13):
	FMSUBX	y09, alpha1i, a2, y09
	LFDU	a1,  1 * SIZE(AO2)
	FMADDX	y10, alpha1r, a2, y10
	LFDU	a2,  1 * SIZE(AO2)
	FMSUBX	y11, alpha1i, a4, y11
	LFDU	a3,  1 * SIZE(AO2)
	FMADDX	y12, alpha1r, a4, y12
	LFDU	a4,  1 * SIZE(AO2)

	FMSUBX	y13, alpha1i, a6, y13
	LFDU	a5,  1 * SIZE(AO2)
	FMADDX	y14, alpha1r, a6, y14
	LFDU	a6,  1 * SIZE(AO2)
	FMSUBX	y15, alpha1i, a8, y15
	LFDU	a7,  1 * SIZE(AO2)
	FMADDX	y16, alpha1r, a8, y16
	LFDU	a8,  1 * SIZE(AO2)

	FMADD	y09, alpha2r, a1, y09
	FMADD	y10, alpha2i, a1, y10
	FMADD	y11, alpha2r, a3, y11
	FMADD	y12, alpha2i, a3, y12

	FMADD	y13, alpha2r, a5, y13
	FMADD	y14, alpha2i, a5, y14
	FMADD	y15, alpha2r, a7, y15
	FMADD	y16, alpha2i, a7, y16

	FMSUBX	y09, alpha2i, a2, y09
	LFDU	a1,  1 * SIZE(AO3)
	FMADDX	y10, alpha2r, a2, y10
	LFDU	a2,  1 * SIZE(AO3)
	FMSUBX	y11, alpha2i, a4, y11
	LFDU	a3,  1 * SIZE(AO3)
	FMADDX	y12, alpha2r, a4, y12
	LFDU	a4,  1 * SIZE(AO3)

	FMSUBX	y13, alpha2i, a6, y13
	LFDU	a5,  1 * SIZE(AO3)
	FMADDX	y14, alpha2r, a6, y14
	LFDU	a6,  1 * SIZE(AO3)
	FMSUBX	y15, alpha2i, a8, y15
	LFDU	a7,  1 * SIZE(AO3)
	FMADDX	y16, alpha2r, a8, y16
	LFDU	a8,  1 * SIZE(AO3)

	FMADD	y09, alpha3r, a1, y09
	FMADD	y10, alpha3i, a1, y10
	FMADD	y11, alpha3r, a3, y11
	FMADD	y12, alpha3i, a3, y12

	FMADD	y13, alpha3r, a5, y13
	FMADD	y14, alpha3i, a5, y14
	FMADD	y15, alpha3r, a7, y15
	FMADD	y16, alpha3i, a7, y16

	FMSUBX	y09, alpha3i, a2, y09
	LFDU	a1,  1 * SIZE(AO4)
	FMADDX	y10, alpha3r, a2, y10
	LFDU	a2,  1 * SIZE(AO4)
	FMSUBX	y11, alpha3i, a4, y11
	LFDU	a3,  1 * SIZE(AO4)
	FMADDX	y12, alpha3r, a4, y12
	LFDU	a4,  1 * SIZE(AO4)

	FMSUBX	y13, alpha3i, a6, y13
	LFDU	a5,  1 * SIZE(AO4)
	FMADDX	y14, alpha3r, a6, y14
	LFDU	a6,  1 * SIZE(AO4)
	FMSUBX	y15, alpha3i, a8, y15
	LFDU	a7,  1 * SIZE(AO4)
	FMADDX	y16, alpha3r, a8, y16
	LFDU	a8,  1 * SIZE(AO4)

	FMADD	y09, alpha4r, a1, y09
	FMADD	y10, alpha4i, a1, y10
	FMADD	y11, alpha4r, a3, y11
	FMADD	y12, alpha4i, a3, y12

	FMADD	y13, alpha4r, a5, y13
	FMADD	y14, alpha4i, a5, y14
	FMADD	y15, alpha4r, a7, y15
	FMADD	y16, alpha4i, a7, y16

	FMSUBX	y09, alpha4i, a2, y09
	FMADDX	y10, alpha4r, a2, y10
	FMSUBX	y11, alpha4i, a4, y11
	FMADDX	y12, alpha4r, a4, y12

	FMSUBX	y13, alpha4i, a6, y13
	STFDU	y09, 1 * SIZE(Y2)
	FMADDX	y14, alpha4r, a6, y14
	STFDU	y10, 1 * SIZE(Y2)
	FMSUBX	y15, alpha4i, a8, y15
	STFDU	y11, 1 * SIZE(Y2)
	FMADDX	y16, alpha4r, a8, y16
	STFDU	y12, 1 * SIZE(Y2)

	STFDU	y13, 1 * SIZE(Y2)
	STFDU	y14, 1 * SIZE(Y2)
	STFDU	y15, 1 * SIZE(Y2)
	STFDU	y16, 1 * SIZE(Y2)
	.align 4

LL(15):
	andi.	r0, M, 2
	ble	LL(17)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	y01, 1 * SIZE(Y1)
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	y02, 1 * SIZE(Y1)
	LFDU	a3,  1 * SIZE(AO1)
	LFDU	y03, 1 * SIZE(Y1)
	LFDU	a4,  1 * SIZE(AO1)
	LFDU	y04, 1 * SIZE(Y1)

	FMADD	y01, alpha1r, a1, y01
	LFDU	a5,  1 * SIZE(AO2)
	FMADD	y02, alpha1i, a1, y02
	LFDU	a6,  1 * SIZE(AO2)
	FMADD	y03, alpha1r, a3, y03
	LFDU	a7,  1 * SIZE(AO2)
	FMADD	y04, alpha1i, a3, y04
	LFDU	a8,  1 * SIZE(AO2)

	FMSUBX	y01, alpha1i, a2, y01
	LFDU	a1,  1 * SIZE(AO3)
	FMADDX	y02, alpha1r, a2, y02
	LFDU	a2,  1 * SIZE(AO3)
	FMSUBX	y03, alpha1i, a4, y03
	LFDU	a3,  1 * SIZE(AO3)
	FMADDX	y04, alpha1r, a4, y04
	LFDU	a4,  1 * SIZE(AO3)

	FMADD	y01, alpha2r, a5, y01
	FMADD	y02, alpha2i, a5, y02
	FMADD	y03, alpha2r, a7, y03
	FMADD	y04, alpha2i, a7, y04

	FMSUBX	y01, alpha2i, a6, y01
	LFDU	a5,  1 * SIZE(AO4)
	FMADDX	y02, alpha2r, a6, y02
	LFDU	a6,  1 * SIZE(AO4)
	FMSUBX	y03, alpha2i, a8, y03
	LFDU	a7,  1 * SIZE(AO4)
	FMADDX	y04, alpha2r, a8, y04
	LFDU	a8,  1 * SIZE(AO4)

	FMADD	y01, alpha3r, a1, y01
	FMADD	y02, alpha3i, a1, y02
	FMADD	y03, alpha3r, a3, y03
	FMADD	y04, alpha3i, a3, y04

	FMSUBX	y01, alpha3i, a2, y01
	FMADDX	y02, alpha3r, a2, y02
	FMSUBX	y03, alpha3i, a4, y03
	FMADDX	y04, alpha3r, a4, y04

	FMADD	y01, alpha4r, a5, y01
	FMADD	y02, alpha4i, a5, y02
	FMADD	y03, alpha4r, a7, y03
	FMADD	y04, alpha4i, a7, y04

	FMSUBX	y01, alpha4i, a6, y01
	FMADDX	y02, alpha4r, a6, y02
	FMSUBX	y03, alpha4i, a8, y03
	FMADDX	y04, alpha4r, a8, y04

	STFDU	y01, 1 * SIZE(Y2)
	STFDU	y02, 1 * SIZE(Y2)
	STFDU	y03, 1 * SIZE(Y2)
	STFDU	y04, 1 * SIZE(Y2)
	.align 4

LL(17):
	andi.	r0, M, 1
	ble	LL(19)

	LFDU	y01, 1 * SIZE(Y1)
	LFDU	y02, 1 * SIZE(Y1)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	a3,  1 * SIZE(AO2)
	LFDU	a4,  1 * SIZE(AO2)

	FMADD	y01, alpha1r, a1, y01
	LFDU	a5,  1 * SIZE(AO3)
	FMADD	y02, alpha1i, a1, y02
	LFDU	a6,  1 * SIZE(AO3)
	FMSUBX	y01, alpha1i, a2, y01
	LFDU	a7,  1 * SIZE(AO4)
	FMADDX	y02, alpha1r, a2, y02
	LFDU	a8,  1 * SIZE(AO4)

	FMADD	y01, alpha2r, a3, y01
	FMADD	y02, alpha2i, a3, y02
	FMSUBX	y01, alpha2i, a4, y01
	FMADDX	y02, alpha2r, a4, y02

	FMADD	y01, alpha3r, a5, y01
	FMADD	y02, alpha3i, a5, y02
	FMSUBX	y01, alpha3i, a6, y01
	FMADDX	y02, alpha3r, a6, y02

	FMADD	y01, alpha4r, a7, y01
	FMADD	y02, alpha4i, a7, y02
	FMSUBX	y01, alpha4i, a8, y01
	FMADDX	y02, alpha4r, a8, y02

	STFDU	y01, 1 * SIZE(Y2)
	STFDU	y02, 1 * SIZE(Y2)
	.align 4

LL(19):
	addi	J, J, -1
	cmpi	cr0, 0, J, 0
	bgt	LL(11)
	.align 4

LL(20):
	andi.	J, N, 2
	ble	LL(30)

	lfd	alpha_r, ALPHA_R
	lfd	alpha_i, ALPHA_I

	LFDUX	a1, X, INCX
	LFDU	a2, 1 * SIZE(X)
	LFDUX	a3, X, INCX
	LFDU	a4, 1 * SIZE(X)

	FMUL	alpha1r, alpha_r, a1
	mr	Y1, YY
	FMUL	alpha1i, alpha_i, a1
	mr	Y2, YY
	FMUL	alpha2r, alpha_r, a3
	mr	AO1, A
	FMUL	alpha2i, alpha_i, a3
	add	AO2, A,   LDA

	FMSUBR	alpha1r, alpha_i, a2, alpha1r
	add	A,   AO2, LDA
	FMADDR	alpha1i, alpha_r, a2, alpha1i
	srawi.	r0,  M, 2
	FMSUBR	alpha2r, alpha_i, a4, alpha2r
	mtspr	CTR, r0
	FMADDR	alpha2i, alpha_r, a4, alpha2i
	ble	LL(25)
	.align 4

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	y01, 1 * SIZE(Y1)
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	y02, 1 * SIZE(Y1)
	LFDU	a3,  1 * SIZE(AO1)
	LFDU	y03, 1 * SIZE(Y1)
	LFDU	a4,  1 * SIZE(AO1)
	LFDU	y04, 1 * SIZE(Y1)

	LFDU	a5,  1 * SIZE(AO1)
	LFDU	y05, 1 * SIZE(Y1)
	LFDU	a6,  1 * SIZE(AO1)
	LFDU	y06, 1 * SIZE(Y1)
	LFDU	a7,  1 * SIZE(AO1)
	LFDU	y07, 1 * SIZE(Y1)
	LFDU	a8,  1 * SIZE(AO1)
	LFDU	y08, 1 * SIZE(Y1)

	FMADD	y09, alpha1r, a1, y01
	FMADD	y10, alpha1i, a1, y02
	FMADD	y11, alpha1r, a3, y03
	FMADD	y12, alpha1i, a3, y04

	FMADD	y13, alpha1r, a5, y05
	FMADD	y14, alpha1i, a5, y06
	FMADD	y15, alpha1r, a7, y07
	FMADD	y16, alpha1i, a7, y08

	bdz	LL(23)
	.align 4

LL(22):
	FMSUBX	y09, alpha1i, a2, y09
	LFDU	a1,  1 * SIZE(AO2)
	FMADDX	y10, alpha1r, a2, y10
	LFDU	a2,  1 * SIZE(AO2)
	FMSUBX	y11, alpha1i, a4, y11
	LFDU	a3,  1 * SIZE(AO2)
	FMADDX	y12, alpha1r, a4, y12
	LFDU	a4,  1 * SIZE(AO2)
#ifdef PPCG4
	dcbt	AO2, PREA
#endif

	FMSUBX	y13, alpha1i, a6, y13
	LFDU	a5,  1 * SIZE(AO2)
	FMADDX	y14, alpha1r, a6, y14
	LFDU	a6,  1 * SIZE(AO2)
	FMSUBX	y15, alpha1i, a8, y15
	LFDU	a7,  1 * SIZE(AO2)
	FMADDX	y16, alpha1r, a8, y16
	LFDU	a8,  1 * SIZE(AO2)
#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO2, PREA
#endif

	FMADD	y09, alpha2r, a1, y09
	LFDU	y01, 1 * SIZE(Y1)
	FMADD	y10, alpha2i, a1, y10
	LFDU	y02, 1 * SIZE(Y1)
	FMADD	y11, alpha2r, a3, y11
	LFDU	y03, 1 * SIZE(Y1)
	FMADD	y12, alpha2i, a3, y12
	LFDU	y04, 1 * SIZE(Y1)

#ifdef PPCG4
	dcbtst	Y1, PREA
#endif

	FMADD	y13, alpha2r, a5, y13
	LFDU	y05, 1 * SIZE(Y1)
	FMADD	y14, alpha2i, a5, y14
	LFDU	y06, 1 * SIZE(Y1)
	FMADD	y15, alpha2r, a7, y15
	LFDU	y07, 1 * SIZE(Y1)
	FMADD	y16, alpha2i, a7, y16
	LFDU	y08, 1 * SIZE(Y1)

#if defined(PPCG4) && defined(DOUBLE)
	dcbtst	Y1, PREA
#endif

	FMSUBX	y09, alpha2i, a2, y09
	LFDU	a1,  1 * SIZE(AO1)
	FMADDX	y10, alpha2r, a2, y10
	LFDU	a2,  1 * SIZE(AO1)
	FMSUBX	y11, alpha2i, a4, y11
	LFDU	a3,  1 * SIZE(AO1)
	FMADDX	y12, alpha2r, a4, y12
	LFDU	a4,  1 * SIZE(AO1)

#ifdef PPCG4
	dcbt	AO1, PREA
#endif

	FMSUBX	y13, alpha2i, a6, y13
	LFDU	a5,  1 * SIZE(AO1)
	FMADDX	y14, alpha2r, a6, y14
	LFDU	a6,  1 * SIZE(AO1)
	FMSUBX	y15, alpha2i, a8, y15
	LFDU	a7,  1 * SIZE(AO1)
	FMADDX	y16, alpha2r, a8, y16
	LFDU	a8,  1 * SIZE(AO1)

#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO1, PREA
#endif

	STFDU	y09, 1 * SIZE(Y2)
	FMADD	y09, alpha1r, a1, y01
	STFDU	y10, 1 * SIZE(Y2)
	FMADD	y10, alpha1i, a1, y02
	STFDU	y11, 1 * SIZE(Y2)
	FMADD	y11, alpha1r, a3, y03
	STFDU	y12, 1 * SIZE(Y2)
	FMADD	y12, alpha1i, a3, y04

	STFDU	y13, 1 * SIZE(Y2)
	FMADD	y13, alpha1r, a5, y05
	STFDU	y14, 1 * SIZE(Y2)
	FMADD	y14, alpha1i, a5, y06
	STFDU	y15, 1 * SIZE(Y2)
	FMADD	y15, alpha1r, a7, y07
	STFDU	y16, 1 * SIZE(Y2)
	FMADD	y16, alpha1i, a7, y08
	bdnz	LL(22)
	.align 4

LL(23):
	FMSUBX	y09, alpha1i, a2, y09
	LFDU	a1,  1 * SIZE(AO2)
	FMADDX	y10, alpha1r, a2, y10
	LFDU	a2,  1 * SIZE(AO2)
	FMSUBX	y11, alpha1i, a4, y11
	LFDU	a3,  1 * SIZE(AO2)
	FMADDX	y12, alpha1r, a4, y12
	LFDU	a4,  1 * SIZE(AO2)

	FMSUBX	y13, alpha1i, a6, y13
	LFDU	a5,  1 * SIZE(AO2)
	FMADDX	y14, alpha1r, a6, y14
	LFDU	a6,  1 * SIZE(AO2)
	FMSUBX	y15, alpha1i, a8, y15
	LFDU	a7,  1 * SIZE(AO2)
	FMADDX	y16, alpha1r, a8, y16
	LFDU	a8,  1 * SIZE(AO2)

	FMADD	y09, alpha2r, a1, y09
	FMADD	y10, alpha2i, a1, y10
	FMADD	y11, alpha2r, a3, y11
	FMADD	y12, alpha2i, a3, y12

	FMADD	y13, alpha2r, a5, y13
	FMADD	y14, alpha2i, a5, y14
	FMADD	y15, alpha2r, a7, y15
	FMADD	y16, alpha2i, a7, y16

	FMSUBX	y09, alpha2i, a2, y09
	FMADDX	y10, alpha2r, a2, y10
	FMSUBX	y11, alpha2i, a4, y11
	FMADDX	y12, alpha2r, a4, y12

	FMSUBX	y13, alpha2i, a6, y13
	STFDU	y09, 1 * SIZE(Y2)
	FMADDX	y14, alpha2r, a6, y14
	STFDU	y10, 1 * SIZE(Y2)
	FMSUBX	y15, alpha2i, a8, y15
	STFDU	y11, 1 * SIZE(Y2)
	FMADDX	y16, alpha2r, a8, y16
	STFDU	y12, 1 * SIZE(Y2)

	STFDU	y13, 1 * SIZE(Y2)
	STFDU	y14, 1 * SIZE(Y2)
	STFDU	y15, 1 * SIZE(Y2)
	STFDU	y16, 1 * SIZE(Y2)
	.align 4

LL(25):
	andi.	r0, M, 2
	ble	LL(27)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	y01, 1 * SIZE(Y1)
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	y02, 1 * SIZE(Y1)
	LFDU	a3,  1 * SIZE(AO1)
	LFDU	y03, 1 * SIZE(Y1)
	LFDU	a4,  1 * SIZE(AO1)
	LFDU	y04, 1 * SIZE(Y1)

	FMADD	y01, alpha1r, a1, y01
	LFDU	a5,  1 * SIZE(AO2)
	FMADD	y02, alpha1i, a1, y02
	LFDU	a6,  1 * SIZE(AO2)
	FMADD	y03, alpha1r, a3, y03
	LFDU	a7,  1 * SIZE(AO2)
	FMADD	y04, alpha1i, a3, y04
	LFDU	a8,  1 * SIZE(AO2)

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	FMADD	y01, alpha2r, a5, y01
	FMADD	y02, alpha2i, a5, y02
	FMADD	y03, alpha2r, a7, y03
	FMADD	y04, alpha2i, a7, y04

	FMSUBX	y01, alpha2i, a6, y01
	FMADDX	y02, alpha2r, a6, y02
	FMSUBX	y03, alpha2i, a8, y03
	FMADDX	y04, alpha2r, a8, y04

	STFDU	y01, 1 * SIZE(Y2)
	STFDU	y02, 1 * SIZE(Y2)
	STFDU	y03, 1 * SIZE(Y2)
	STFDU	y04, 1 * SIZE(Y2)
	.align 4

LL(27):
	andi.	r0, M, 1
	ble	LL(30)

	LFDU	y01, 1 * SIZE(Y1)
	LFDU	y02, 1 * SIZE(Y1)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	a3,  1 * SIZE(AO2)
	LFDU	a4,  1 * SIZE(AO2)

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02

	FMADD	y01, alpha2r, a3, y01
	FMADD	y02, alpha2i, a3, y02
	FMSUBX	y01, alpha2i, a4, y01
	FMADDX	y02, alpha2r, a4, y02

	STFDU	y01, 1 * SIZE(Y2)
	STFDU	y02, 1 * SIZE(Y2)
	.align 4

LL(30):
	andi.	J, N, 1
	ble	LL(990)
	.align 4

	lfd	alpha_r, ALPHA_R
	lfd	alpha_i, ALPHA_I

	LFDUX	a1, X, INCX
	LFDU	a2, 1 * SIZE(X)

	FMUL	alpha1r, alpha_r, a1
	mr	Y1, YY
	mr	Y2, YY
	FMUL	alpha1i, alpha_i, a1
	mr	AO1, A
	add	A,   A,   LDA

	FMSUBR	alpha1r, alpha_i, a2, alpha1r
	srawi.	r0,  M, 2
	mtspr	CTR, r0
	FMADDR	alpha1i, alpha_r, a2, alpha1i
	ble	LL(35)
	.align 4

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	y01, 1 * SIZE(Y1)
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	y02, 1 * SIZE(Y1)
	LFDU	a3,  1 * SIZE(AO1)
	LFDU	y03, 1 * SIZE(Y1)
	LFDU	a4,  1 * SIZE(AO1)
	LFDU	y04, 1 * SIZE(Y1)

	LFDU	a5,  1 * SIZE(AO1)
	LFDU	y05, 1 * SIZE(Y1)
	LFDU	a6,  1 * SIZE(AO1)
	LFDU	y06, 1 * SIZE(Y1)
	LFDU	a7,  1 * SIZE(AO1)
	LFDU	y07, 1 * SIZE(Y1)
	LFDU	a8,  1 * SIZE(AO1)
	LFDU	y08, 1 * SIZE(Y1)

	FMADD	y09, alpha1r, a1, y01
	FMADD	y10, alpha1i, a1, y02
	FMADD	y11, alpha1r, a3, y03
	FMADD	y12, alpha1i, a3, y04

	FMADD	y13, alpha1r, a5, y05
	FMADD	y14, alpha1i, a5, y06
	FMADD	y15, alpha1r, a7, y07
	FMADD	y16, alpha1i, a7, y08

	bdz	LL(33)
	.align 4

LL(32):
	FMSUBX	y09, alpha1i, a2, y09
	LFDU	a1,  1 * SIZE(AO1)
	FMADDX	y10, alpha1r, a2, y10
	LFDU	a2,  1 * SIZE(AO1)
	FMSUBX	y11, alpha1i, a4, y11
	LFDU	a3,  1 * SIZE(AO1)
	FMADDX	y12, alpha1r, a4, y12
	LFDU	a4,  1 * SIZE(AO1)

#ifdef PPCG4
	dcbt	AO1, PREA
#endif

	LFDU	y01, 1 * SIZE(Y1)
	LFDU	y02, 1 * SIZE(Y1)
	LFDU	y03, 1 * SIZE(Y1)
	LFDU	y04, 1 * SIZE(Y1)

#ifdef PPCG4
	dcbtst	Y1, PREA
#endif

	FMSUBX	y13, alpha1i, a6, y13
	LFDU	a5,  1 * SIZE(AO1)
	FMADDX	y14, alpha1r, a6, y14
	LFDU	a6,  1 * SIZE(AO1)
	FMSUBX	y15, alpha1i, a8, y15
	LFDU	a7,  1 * SIZE(AO1)
	FMADDX	y16, alpha1r, a8, y16
	LFDU	a8,  1 * SIZE(AO1)

#if defined(PPCG4) && defined(DOUBLE)
	dcbt	AO1, PREA
#endif

	LFDU	y05, 1 * SIZE(Y1)
	LFDU	y06, 1 * SIZE(Y1)
	LFDU	y07, 1 * SIZE(Y1)
	LFDU	y08, 1 * SIZE(Y1)

#if defined(PPCG4) && defined(DOUBLE)
	dcbtst	Y1, PREA
#endif

	STFDU	y09, 1 * SIZE(Y2)
	FMADD	y09, alpha1r, a1, y01
	STFDU	y10, 1 * SIZE(Y2)
	FMADD	y10, alpha1i, a1, y02
	STFDU	y11, 1 * SIZE(Y2)
	FMADD	y11, alpha1r, a3, y03
	STFDU	y12, 1 * SIZE(Y2)
	FMADD	y12, alpha1i, a3, y04

	STFDU	y13, 1 * SIZE(Y2)
	FMADD	y13, alpha1r, a5, y05
	STFDU	y14, 1 * SIZE(Y2)
	FMADD	y14, alpha1i, a5, y06
	STFDU	y15, 1 * SIZE(Y2)
	FMADD	y15, alpha1r, a7, y07
	STFDU	y16, 1 * SIZE(Y2)
	FMADD	y16, alpha1i, a7, y08
	bdnz	LL(32)
	.align 4

LL(33):
	FMSUBX	y09, alpha1i, a2, y09
	FMADDX	y10, alpha1r, a2, y10
	FMSUBX	y11, alpha1i, a4, y11
	FMADDX	y12, alpha1r, a4, y12

	FMSUBX	y13, alpha1i, a6, y13
	STFDU	y09, 1 * SIZE(Y2)
	FMADDX	y14, alpha1r, a6, y14
	STFDU	y10, 1 * SIZE(Y2)
	FMSUBX	y15, alpha1i, a8, y15
	STFDU	y11, 1 * SIZE(Y2)
	FMADDX	y16, alpha1r, a8, y16
	STFDU	y12, 1 * SIZE(Y2)

	STFDU	y13, 1 * SIZE(Y2)
	STFDU	y14, 1 * SIZE(Y2)
	STFDU	y15, 1 * SIZE(Y2)
	STFDU	y16, 1 * SIZE(Y2)
	.align 4

LL(35):
	andi.	r0, M, 2
	ble	LL(37)

	LFDU	a1,  1 * SIZE(AO1)
	LFDU	y01, 1 * SIZE(Y1)
	LFDU	a2,  1 * SIZE(AO1)
	LFDU	y02, 1 * SIZE(Y1)
	LFDU	a3,  1 * SIZE(AO1)
	LFDU	y03, 1 * SIZE(Y1)
	LFDU	a4,  1 * SIZE(AO1)
	LFDU	y04, 1 * SIZE(Y1)

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	STFDU	y01, 1 * SIZE(Y2)
	STFDU	y02, 1 * SIZE(Y2)
	STFDU	y03, 1 * SIZE(Y2)
	STFDU	y04, 1 * SIZE(Y2)
	.align 4

LL(37):
	andi.	r0, M, 1
	ble	LL(990)

	LFDU	y01, 1 * SIZE(Y1)
	LFDU	a1,  1 * SIZE(AO1)
	LFDU	y02, 1 * SIZE(Y1)
	LFDU	a2,  1 * SIZE(AO1)

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02

	STFDU	y01, 1 * SIZE(Y2)
	STFDU	y02, 1 * SIZE(Y2)
	.align 4

LL(990):
	cmpi	cr0, 0, INCY, SIZE
	beq	LL(999)

	addi	YY, BUFFER, -SIZE
	mr	Y1, Y

	srawi.	r0, M, 2
	mtspr	CTR, r0
	ble	LL(995)
	.align 4

LL(991):
	LFDUX	f0, Y, INCY
	LFDU	f1,  1 * SIZE(Y)
	LFDUX	f2, Y, INCY
	LFDU	f3,  1 * SIZE(Y)
	LFDUX	f4, Y, INCY
	LFDU	f5,  1 * SIZE(Y)
	LFDUX	f6, Y, INCY
	LFDU	f7,  1 * SIZE(Y)

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
	STFDU	f9,  1 * SIZE(Y1)
	STFDUX	f10, Y1, INCY
	STFDU	f11, 1 * SIZE(Y1)
	STFDUX	f12, Y1, INCY
	STFDU	f13, 1 * SIZE(Y1)
	STFDUX	f14, Y1, INCY
	STFDU	f15, 1 * SIZE(Y1)
	bdnz	LL(991)
	.align 4

LL(995):
	andi.	J, M, 2
	ble	LL(996)

	LFDUX	f0, Y, INCY
	LFDU	f1, 1 * SIZE(Y)
	LFDUX	f2, Y, INCY
	LFDU	f3, 1 * SIZE(Y)

	LFDU	f8,   1 * SIZE(YY)
	LFDU	f9,   1 * SIZE(YY)
	LFDU	f10,  1 * SIZE(YY)
	LFDU	f11,  1 * SIZE(YY)

	FADD	f8,  f8,  f0
	FADD	f9,  f9,  f1
	FADD	f10, f10, f2
	FADD	f11, f11, f3

	STFDUX	f8,  Y1, INCY
	STFDU	f9,  1 * SIZE(Y1)
	STFDUX	f10, Y1, INCY
	STFDU	f11, 1 * SIZE(Y1)
	.align 4

LL(996):
	andi.	J, M, 1
	ble	LL(999)

	LFDUX	f0, Y, INCY
	LFDU	f1, 1 * SIZE(Y)

	LFDU	f8,   1 * SIZE(YY)
	LFDU	f9,   1 * SIZE(YY)

	FADD	f8,  f8,  f0
	FADD	f9,  f9,  f1

	STFDUX	f8,  Y1, INCY
	STFDU	f9,  1 * SIZE(Y1)
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
