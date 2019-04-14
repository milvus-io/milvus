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
#define LDA4	r18

#define	Y1	r19
#define Y2	r20
#define	PREA	r21
#define	PREC	r22

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
#define PREFETCHSIZE_C  24
#endif

#ifdef POWER6
#define PREFETCHSIZE_A  24
#define PREFETCHSIZE_C  24
#endif

#ifdef POWER8
#define PREFETCHSIZE_A  24
#define PREFETCHSIZE_C  24
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
#define STACKSIZE 224
#define ALPHA_R	  208(SP)
#define ALPHA_I	  216(SP)
#else
#define STACKSIZE 280
#define ALPHA_R   256(SP)
#define	ALPHA_I	  264(SP)
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
	lwz	INCY,  FRAMESLOT(0) + STACKSIZE(SP)
#else
	ld	INCX,  FRAMESLOT(0) + STACKSIZE(SP)
	ld	Y,     FRAMESLOT(1) + STACKSIZE(SP)
	ld	INCY,  FRAMESLOT(2) + STACKSIZE(SP)
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#ifndef __64BIT__
#ifdef DOUBLE
	lwz	LDA,   FRAMESLOT(0) + STACKSIZE(SP)
	lwz	X,     FRAMESLOT(1) + STACKSIZE(SP)
	lwz	INCX,  FRAMESLOT(2) + STACKSIZE(SP)
	lwz	Y,     FRAMESLOT(3) + STACKSIZE(SP)
	lwz	INCY,  FRAMESLOT(4) + STACKSIZE(SP)
#else
	lwz	INCX,  FRAMESLOT(0) + STACKSIZE(SP)
	lwz	Y,     FRAMESLOT(1) + STACKSIZE(SP)
	lwz	INCY,  FRAMESLOT(2) + STACKSIZE(SP)
#endif
#else
	ld	INCX,  FRAMESLOT(0) + STACKSIZE(SP)
	ld	Y,     FRAMESLOT(1) + STACKSIZE(SP)
	ld	INCY,  FRAMESLOT(2) + STACKSIZE(SP)
#endif
#endif

	stfd	f1, ALPHA_R
	stfd	f2, ALPHA_I

	slwi	LDA4, LDA,  ZBASE_SHIFT + 2
	slwi	LDA,  LDA,  ZBASE_SHIFT
	slwi	INCX, INCX, ZBASE_SHIFT
	slwi	INCY, INCY, ZBASE_SHIFT

	li	PREA, PREFETCHSIZE_A * SIZE
	li	PREC, PREFETCHSIZE_C * SIZE

	cmpwi	cr0, M, 0
	ble-	LL(999)

	cmpwi	cr0, N, 0
	ble-	LL(999)

	cmpi	cr0, 0, INCY, 2 * SIZE
	bne	LL(100)

	srawi.	J, N, 2
	ble	LL(20)
	.align 4

LL(11):
	lfd	alpha_r, ALPHA_R
	lfd	alpha_i, ALPHA_I

	LFD	a1, 0 * SIZE(X)
	LFD	a2, 1 * SIZE(X)
	add	X, X, INCX
	LFD	a3, 0 * SIZE(X)
	LFD	a4, 1 * SIZE(X)
	add	X, X, INCX
	LFD	a5, 0 * SIZE(X)
	LFD	a6, 1 * SIZE(X)
	add	X, X, INCX
	LFD	a7, 0 * SIZE(X)
	LFD	a8, 1 * SIZE(X)
	add	X, X, INCX

	FMUL	alpha1r, alpha_r, a1
	FMUL	alpha1i, alpha_i, a1
	FMUL	alpha2r, alpha_r, a3
	FMUL	alpha2i, alpha_i, a3
	FMUL	alpha3r, alpha_r, a5
	FMUL	alpha3i, alpha_i, a5
	FMUL	alpha4r, alpha_r, a7
	FMUL	alpha4i, alpha_i, a7

	FMSUBR	alpha1r, alpha_i, a2, alpha1r
	FMADDR	alpha1i, alpha_r, a2, alpha1i
	FMSUBR	alpha2r, alpha_i, a4, alpha2r
	FMADDR	alpha2i, alpha_r, a4, alpha2i
	FMSUBR	alpha3r, alpha_i, a6, alpha3r
	FMADDR	alpha3i, alpha_r, a6, alpha3i
	FMSUBR	alpha4r, alpha_i, a8, alpha4r
	FMADDR	alpha4i, alpha_r, a8, alpha4i

	mr	AO1, A
	add	AO2, A,   LDA
	add	AO3, AO2, LDA
	add	AO4, AO3, LDA
	add	A,   AO4, LDA

	mr	Y1, Y
	mr	Y2, Y

	srawi.	r0,  M, 3
	mtspr	CTR, r0
	ble	LL(15)
	.align 4

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)

	LFD	y01,  0 * SIZE(Y1)
	LFD	y02,  1 * SIZE(Y1)
	LFD	y03,  2 * SIZE(Y1)
	LFD	y04,  3 * SIZE(Y1)

	LFD	a5,  4 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

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
	addi	Y1, Y1, 16 * SIZE

	bdz	LL(13)
	.align 4

LL(12):
	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMADD	y05, alpha1r, a5, y05
	FMADD	y06, alpha1i, a5, y06
	FMADD	y07, alpha1r, a7, y07
	FMADD	y08, alpha1i, a7, y08

	LFD	a1,  8 * SIZE(AO1)
	LFD	a3, 10 * SIZE(AO1)
	LFD	a5, 12 * SIZE(AO1)
	LFD	a7, 14 * SIZE(AO1)

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	FMSUBX	y05, alpha1i, a6, y05
	FMADDX	y06, alpha1r, a6, y06
	FMSUBX	y07, alpha1i, a8, y07
	FMADDX	y08, alpha1r, a8, y08

	LFD	a2,  9 * SIZE(AO1)
	LFD	a4, 11 * SIZE(AO1)
	LFD	a6, 13 * SIZE(AO1)
	LFD	a8, 15 * SIZE(AO1)

	addi	AO1, AO1, 16 * SIZE
	nop
	DCBT(AO1, PREA)
	nop

	FMADD	y09, alpha1r, a1, y09
	FMADD	y10, alpha1i, a1, y10
	FMADD	y11, alpha1r, a3, y11
	FMADD	y12, alpha1i, a3, y12

	FMADD	y13, alpha1r, a5, y13
	FMADD	y14, alpha1i, a5, y14
	FMADD	y15, alpha1r, a7, y15
	FMADD	y16, alpha1i, a7, y16

	LFD	a1,  0 * SIZE(AO2)
	LFD	a3,  2 * SIZE(AO2)
	LFD	a5,  4 * SIZE(AO2)
	LFD	a7,  6 * SIZE(AO2)

	FMSUBX	y09, alpha1i, a2, y09
	FMADDX	y10, alpha1r, a2, y10
	FMSUBX	y11, alpha1i, a4, y11
	FMADDX	y12, alpha1r, a4, y12

	FMSUBX	y13, alpha1i, a6, y13
	FMADDX	y14, alpha1r, a6, y14
	FMSUBX	y15, alpha1i, a8, y15
	FMADDX	y16, alpha1r, a8, y16

	LFD	a2,  1 * SIZE(AO2)
	LFD	a4,  3 * SIZE(AO2)
	LFD	a6,  5 * SIZE(AO2)
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2r, a1, y01
	FMADD	y02, alpha2i, a1, y02
	FMADD	y03, alpha2r, a3, y03
	FMADD	y04, alpha2i, a3, y04

	FMADD	y05, alpha2r, a5, y05
	FMADD	y06, alpha2i, a5, y06
	FMADD	y07, alpha2r, a7, y07
	FMADD	y08, alpha2i, a7, y08

	LFD	a1,  8 * SIZE(AO2)
	LFD	a3, 10 * SIZE(AO2)
	LFD	a5, 12 * SIZE(AO2)
	LFD	a7, 14 * SIZE(AO2)

	FMSUBX	y01, alpha2i, a2, y01
	FMADDX	y02, alpha2r, a2, y02
	FMSUBX	y03, alpha2i, a4, y03
	FMADDX	y04, alpha2r, a4, y04

	FMSUBX	y05, alpha2i, a6, y05
	FMADDX	y06, alpha2r, a6, y06
	FMSUBX	y07, alpha2i, a8, y07
	FMADDX	y08, alpha2r, a8, y08

	LFD	a2,  9 * SIZE(AO2)
	LFD	a4, 11 * SIZE(AO2)
	LFD	a6, 13 * SIZE(AO2)
	LFD	a8, 15 * SIZE(AO2)

	addi	AO2, AO2, 16 * SIZE
	nop
	DCBT(AO2, PREA)
	nop

	FMADD	y09, alpha2r, a1, y09
	FMADD	y10, alpha2i, a1, y10
	FMADD	y11, alpha2r, a3, y11
	FMADD	y12, alpha2i, a3, y12

	FMADD	y13, alpha2r, a5, y13
	FMADD	y14, alpha2i, a5, y14
	FMADD	y15, alpha2r, a7, y15
	FMADD	y16, alpha2i, a7, y16

	LFD	a1,  0 * SIZE(AO3)
	LFD	a3,  2 * SIZE(AO3)
	LFD	a5,  4 * SIZE(AO3)
	LFD	a7,  6 * SIZE(AO3)

	FMSUBX	y09, alpha2i, a2, y09
	FMADDX	y10, alpha2r, a2, y10
	FMSUBX	y11, alpha2i, a4, y11
	FMADDX	y12, alpha2r, a4, y12

	FMSUBX	y13, alpha2i, a6, y13
	FMADDX	y14, alpha2r, a6, y14
	FMSUBX	y15, alpha2i, a8, y15
	FMADDX	y16, alpha2r, a8, y16

	LFD	a2,  1 * SIZE(AO3)
	LFD	a4,  3 * SIZE(AO3)
	LFD	a6,  5 * SIZE(AO3)
	LFD	a8,  7 * SIZE(AO3)

	FMADD	y01, alpha3r, a1, y01
	FMADD	y02, alpha3i, a1, y02
	FMADD	y03, alpha3r, a3, y03
	FMADD	y04, alpha3i, a3, y04

	FMADD	y05, alpha3r, a5, y05
	FMADD	y06, alpha3i, a5, y06
	FMADD	y07, alpha3r, a7, y07
	FMADD	y08, alpha3i, a7, y08

	LFD	a1,  8 * SIZE(AO3)
	LFD	a3, 10 * SIZE(AO3)
	LFD	a5, 12 * SIZE(AO3)
	LFD	a7, 14 * SIZE(AO3)

	FMSUBX	y01, alpha3i, a2, y01
	FMADDX	y02, alpha3r, a2, y02
	FMSUBX	y03, alpha3i, a4, y03
	FMADDX	y04, alpha3r, a4, y04

	FMSUBX	y05, alpha3i, a6, y05
	FMADDX	y06, alpha3r, a6, y06
	FMSUBX	y07, alpha3i, a8, y07
	FMADDX	y08, alpha3r, a8, y08

	LFD	a2,  9 * SIZE(AO3)
	LFD	a4, 11 * SIZE(AO3)
	LFD	a6, 13 * SIZE(AO3)
	LFD	a8, 15 * SIZE(AO3)

	addi	AO3, AO3, 16 * SIZE
	nop
	DCBT(AO3, PREA)
	nop

	FMADD	y09, alpha3r, a1, y09
	FMADD	y10, alpha3i, a1, y10
	FMADD	y11, alpha3r, a3, y11
	FMADD	y12, alpha3i, a3, y12

	FMADD	y13, alpha3r, a5, y13
	FMADD	y14, alpha3i, a5, y14
	FMADD	y15, alpha3r, a7, y15
	FMADD	y16, alpha3i, a7, y16

	LFD	a1,  0 * SIZE(AO4)
	LFD	a3,  2 * SIZE(AO4)
	LFD	a5,  4 * SIZE(AO4)
	LFD	a7,  6 * SIZE(AO4)

	FMSUBX	y09, alpha3i, a2, y09
	FMADDX	y10, alpha3r, a2, y10
	FMSUBX	y11, alpha3i, a4, y11
	FMADDX	y12, alpha3r, a4, y12

	FMSUBX	y13, alpha3i, a6, y13
	FMADDX	y14, alpha3r, a6, y14
	FMSUBX	y15, alpha3i, a8, y15
	FMADDX	y16, alpha3r, a8, y16

	LFD	a2,  1 * SIZE(AO4)
	LFD	a4,  3 * SIZE(AO4)
	LFD	a6,  5 * SIZE(AO4)
	LFD	a8,  7 * SIZE(AO4)

	FMADD	y01, alpha4r, a1, y01
	FMADD	y02, alpha4i, a1, y02
	FMADD	y03, alpha4r, a3, y03
	FMADD	y04, alpha4i, a3, y04

	FMADD	y05, alpha4r, a5, y05
	FMADD	y06, alpha4i, a5, y06
	FMADD	y07, alpha4r, a7, y07
	FMADD	y08, alpha4i, a7, y08

	LFD	a1,  8 * SIZE(AO4)
	LFD	a3, 10 * SIZE(AO4)
	LFD	a5, 12 * SIZE(AO4)
	LFD	a7, 14 * SIZE(AO4)

	FMSUBX	y01, alpha4i, a2, y01
	FMADDX	y02, alpha4r, a2, y02
	FMSUBX	y03, alpha4i, a4, y03
	FMADDX	y04, alpha4r, a4, y04

	STFD	y01,  0 * SIZE(Y2)
	STFD	y02,  1 * SIZE(Y2)
	STFD	y03,  2 * SIZE(Y2)
	STFD	y04,  3 * SIZE(Y2)

	LFD	y01,  0 * SIZE(Y1)
	LFD	y02,  1 * SIZE(Y1)
	LFD	y03,  2 * SIZE(Y1)
	LFD	y04,  3 * SIZE(Y1)

	FMSUBX	y05, alpha4i, a6, y05
	FMADDX	y06, alpha4r, a6, y06
	FMSUBX	y07, alpha4i, a8, y07
	FMADDX	y08, alpha4r, a8, y08

	LFD	a2,  9 * SIZE(AO4)
	LFD	a4, 11 * SIZE(AO4)
	LFD	a6, 13 * SIZE(AO4)
	LFD	a8, 15 * SIZE(AO4)

	addi	AO4, AO4, 16 * SIZE
	nop
	DCBT(AO4, PREA)
	nop

	STFD	y05,  4 * SIZE(Y2)
	STFD	y06,  5 * SIZE(Y2)
	STFD	y07,  6 * SIZE(Y2)
	STFD	y08,  7 * SIZE(Y2)

	LFD	y05,  4 * SIZE(Y1)
	LFD	y06,  5 * SIZE(Y1)
	LFD	y07,  6 * SIZE(Y1)
	LFD	y08,  7 * SIZE(Y1)

	FMADD	y09, alpha4r, a1, y09
	FMADD	y10, alpha4i, a1, y10
	FMADD	y11, alpha4r, a3, y11
	FMADD	y12, alpha4i, a3, y12

	FMADD	y13, alpha4r, a5, y13
	FMADD	y14, alpha4i, a5, y14
	FMADD	y15, alpha4r, a7, y15
	FMADD	y16, alpha4i, a7, y16

	LFD	a1,  0 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)

	FMSUBX	y09, alpha4i, a2, y09
	FMADDX	y10, alpha4r, a2, y10
	FMSUBX	y11, alpha4i, a4, y11
	FMADDX	y12, alpha4r, a4, y12

	STFD	y09,  8 * SIZE(Y2)
	STFD	y10,  9 * SIZE(Y2)
	STFD	y11, 10 * SIZE(Y2)
	STFD	y12, 11 * SIZE(Y2)

	LFD	y09,  8 * SIZE(Y1)
	LFD	y10,  9 * SIZE(Y1)
	LFD	y11, 10 * SIZE(Y1)
	LFD	y12, 11 * SIZE(Y1)

	FMSUBX	y13, alpha4i, a6, y13
	FMADDX	y14, alpha4r, a6, y14
	FMSUBX	y15, alpha4i, a8, y15
	FMADDX	y16, alpha4r, a8, y16

	LFD	a2,  1 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	STFD	y13, 12 * SIZE(Y2)
	STFD	y14, 13 * SIZE(Y2)
	STFD	y15, 14 * SIZE(Y2)
	STFD	y16, 15 * SIZE(Y2)

	LFD	y13, 12 * SIZE(Y1)
	LFD	y14, 13 * SIZE(Y1)
	LFD	y15, 14 * SIZE(Y1)
	LFD	y16, 15 * SIZE(Y1)

	addi	Y2, Y2, 16 * SIZE
	addi	Y1, Y1, 16 * SIZE
	DCBT(Y1, PREC)
	bdnz	LL(12)
	.align 4

LL(13):
	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMADD	y05, alpha1r, a5, y05
	FMADD	y06, alpha1i, a5, y06
	FMADD	y07, alpha1r, a7, y07
	FMADD	y08, alpha1i, a7, y08

	LFD	a1,  8 * SIZE(AO1)
	LFD	a3, 10 * SIZE(AO1)
	LFD	a5, 12 * SIZE(AO1)
	LFD	a7, 14 * SIZE(AO1)

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	FMSUBX	y05, alpha1i, a6, y05
	FMADDX	y06, alpha1r, a6, y06
	FMSUBX	y07, alpha1i, a8, y07
	FMADDX	y08, alpha1r, a8, y08

	LFD	a2,  9 * SIZE(AO1)
	LFD	a4, 11 * SIZE(AO1)
	LFD	a6, 13 * SIZE(AO1)
	LFD	a8, 15 * SIZE(AO1)

	FMADD	y09, alpha1r, a1, y09
	FMADD	y10, alpha1i, a1, y10
	FMADD	y11, alpha1r, a3, y11
	FMADD	y12, alpha1i, a3, y12

	FMADD	y13, alpha1r, a5, y13
	FMADD	y14, alpha1i, a5, y14
	FMADD	y15, alpha1r, a7, y15
	FMADD	y16, alpha1i, a7, y16

	LFD	a1,  0 * SIZE(AO2)
	LFD	a3,  2 * SIZE(AO2)
	LFD	a5,  4 * SIZE(AO2)
	LFD	a7,  6 * SIZE(AO2)

	FMSUBX	y09, alpha1i, a2, y09
	FMADDX	y10, alpha1r, a2, y10
	FMSUBX	y11, alpha1i, a4, y11
	FMADDX	y12, alpha1r, a4, y12

	FMSUBX	y13, alpha1i, a6, y13
	FMADDX	y14, alpha1r, a6, y14
	FMSUBX	y15, alpha1i, a8, y15
	FMADDX	y16, alpha1r, a8, y16

	LFD	a2,  1 * SIZE(AO2)
	LFD	a4,  3 * SIZE(AO2)
	LFD	a6,  5 * SIZE(AO2)
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2r, a1, y01
	FMADD	y02, alpha2i, a1, y02
	FMADD	y03, alpha2r, a3, y03
	FMADD	y04, alpha2i, a3, y04

	FMADD	y05, alpha2r, a5, y05
	FMADD	y06, alpha2i, a5, y06
	FMADD	y07, alpha2r, a7, y07
	FMADD	y08, alpha2i, a7, y08

	LFD	a1,  8 * SIZE(AO2)
	LFD	a3, 10 * SIZE(AO2)
	LFD	a5, 12 * SIZE(AO2)
	LFD	a7, 14 * SIZE(AO2)

	FMSUBX	y01, alpha2i, a2, y01
	FMADDX	y02, alpha2r, a2, y02
	FMSUBX	y03, alpha2i, a4, y03
	FMADDX	y04, alpha2r, a4, y04

	FMSUBX	y05, alpha2i, a6, y05
	FMADDX	y06, alpha2r, a6, y06
	FMSUBX	y07, alpha2i, a8, y07
	FMADDX	y08, alpha2r, a8, y08

	LFD	a2,  9 * SIZE(AO2)
	LFD	a4, 11 * SIZE(AO2)
	LFD	a6, 13 * SIZE(AO2)
	LFD	a8, 15 * SIZE(AO2)

	FMADD	y09, alpha2r, a1, y09
	FMADD	y10, alpha2i, a1, y10
	FMADD	y11, alpha2r, a3, y11
	FMADD	y12, alpha2i, a3, y12

	FMADD	y13, alpha2r, a5, y13
	FMADD	y14, alpha2i, a5, y14
	FMADD	y15, alpha2r, a7, y15
	FMADD	y16, alpha2i, a7, y16

	LFD	a1,  0 * SIZE(AO3)
	LFD	a3,  2 * SIZE(AO3)
	LFD	a5,  4 * SIZE(AO3)
	LFD	a7,  6 * SIZE(AO3)

	FMSUBX	y09, alpha2i, a2, y09
	FMADDX	y10, alpha2r, a2, y10
	FMSUBX	y11, alpha2i, a4, y11
	FMADDX	y12, alpha2r, a4, y12

	FMSUBX	y13, alpha2i, a6, y13
	FMADDX	y14, alpha2r, a6, y14
	FMSUBX	y15, alpha2i, a8, y15
	FMADDX	y16, alpha2r, a8, y16

	LFD	a2,  1 * SIZE(AO3)
	LFD	a4,  3 * SIZE(AO3)
	LFD	a6,  5 * SIZE(AO3)
	LFD	a8,  7 * SIZE(AO3)

	FMADD	y01, alpha3r, a1, y01
	FMADD	y02, alpha3i, a1, y02
	FMADD	y03, alpha3r, a3, y03
	FMADD	y04, alpha3i, a3, y04

	FMADD	y05, alpha3r, a5, y05
	FMADD	y06, alpha3i, a5, y06
	FMADD	y07, alpha3r, a7, y07
	FMADD	y08, alpha3i, a7, y08

	LFD	a1,  8 * SIZE(AO3)
	LFD	a3, 10 * SIZE(AO3)
	LFD	a5, 12 * SIZE(AO3)
	LFD	a7, 14 * SIZE(AO3)

	FMSUBX	y01, alpha3i, a2, y01
	FMADDX	y02, alpha3r, a2, y02
	FMSUBX	y03, alpha3i, a4, y03
	FMADDX	y04, alpha3r, a4, y04

	FMSUBX	y05, alpha3i, a6, y05
	FMADDX	y06, alpha3r, a6, y06
	FMSUBX	y07, alpha3i, a8, y07
	FMADDX	y08, alpha3r, a8, y08

	LFD	a2,  9 * SIZE(AO3)
	LFD	a4, 11 * SIZE(AO3)
	LFD	a6, 13 * SIZE(AO3)
	LFD	a8, 15 * SIZE(AO3)

	FMADD	y09, alpha3r, a1, y09
	FMADD	y10, alpha3i, a1, y10
	FMADD	y11, alpha3r, a3, y11
	FMADD	y12, alpha3i, a3, y12

	FMADD	y13, alpha3r, a5, y13
	FMADD	y14, alpha3i, a5, y14
	FMADD	y15, alpha3r, a7, y15
	FMADD	y16, alpha3i, a7, y16

	LFD	a1,  0 * SIZE(AO4)
	LFD	a3,  2 * SIZE(AO4)
	LFD	a5,  4 * SIZE(AO4)
	LFD	a7,  6 * SIZE(AO4)

	FMSUBX	y09, alpha3i, a2, y09
	FMADDX	y10, alpha3r, a2, y10
	FMSUBX	y11, alpha3i, a4, y11
	FMADDX	y12, alpha3r, a4, y12

	FMSUBX	y13, alpha3i, a6, y13
	FMADDX	y14, alpha3r, a6, y14
	FMSUBX	y15, alpha3i, a8, y15
	FMADDX	y16, alpha3r, a8, y16

	LFD	a2,  1 * SIZE(AO4)
	LFD	a4,  3 * SIZE(AO4)
	LFD	a6,  5 * SIZE(AO4)
	LFD	a8,  7 * SIZE(AO4)

	FMADD	y01, alpha4r, a1, y01
	FMADD	y02, alpha4i, a1, y02
	FMADD	y03, alpha4r, a3, y03
	FMADD	y04, alpha4i, a3, y04

	FMADD	y05, alpha4r, a5, y05
	FMADD	y06, alpha4i, a5, y06
	FMADD	y07, alpha4r, a7, y07
	FMADD	y08, alpha4i, a7, y08

	LFD	a1,  8 * SIZE(AO4)
	LFD	a3, 10 * SIZE(AO4)
	LFD	a5, 12 * SIZE(AO4)
	LFD	a7, 14 * SIZE(AO4)

	FMSUBX	y01, alpha4i, a2, y01
	FMADDX	y02, alpha4r, a2, y02
	FMSUBX	y03, alpha4i, a4, y03
	FMADDX	y04, alpha4r, a4, y04

	FMSUBX	y05, alpha4i, a6, y05
	FMADDX	y06, alpha4r, a6, y06
	FMSUBX	y07, alpha4i, a8, y07
	FMADDX	y08, alpha4r, a8, y08

	LFD	a2,  9 * SIZE(AO4)
	LFD	a4, 11 * SIZE(AO4)
	LFD	a6, 13 * SIZE(AO4)
	LFD	a8, 15 * SIZE(AO4)

	FMADD	y09, alpha4r, a1, y09
	FMADD	y10, alpha4i, a1, y10
	FMADD	y11, alpha4r, a3, y11
	FMADD	y12, alpha4i, a3, y12

	FMADD	y13, alpha4r, a5, y13
	FMADD	y14, alpha4i, a5, y14
	FMADD	y15, alpha4r, a7, y15
	FMADD	y16, alpha4i, a7, y16

	LFD	a1, 16 * SIZE(AO1)
	LFD	a3, 18 * SIZE(AO1)
	LFD	a5, 20 * SIZE(AO1)
	LFD	a7, 22 * SIZE(AO1)

	FMSUBX	y09, alpha4i, a2, y09
	FMADDX	y10, alpha4r, a2, y10
	FMSUBX	y11, alpha4i, a4, y11
	FMADDX	y12, alpha4r, a4, y12

	FMSUBX	y13, alpha4i, a6, y13
	FMADDX	y14, alpha4r, a6, y14
	FMSUBX	y15, alpha4i, a8, y15
	FMADDX	y16, alpha4r, a8, y16

	LFD	a2, 17 * SIZE(AO1)
	LFD	a4, 19 * SIZE(AO1)
	LFD	a6, 21 * SIZE(AO1)
	LFD	a8, 23 * SIZE(AO1)

	addi	AO1, AO1, 16 * SIZE
	addi	AO2, AO2, 16 * SIZE
	addi	AO3, AO3, 16 * SIZE
	addi	AO4, AO4, 16 * SIZE

	STFD	y01,  0 * SIZE(Y2)
	STFD	y02,  1 * SIZE(Y2)
	STFD	y03,  2 * SIZE(Y2)
	STFD	y04,  3 * SIZE(Y2)
	STFD	y05,  4 * SIZE(Y2)
	STFD	y06,  5 * SIZE(Y2)
	STFD	y07,  6 * SIZE(Y2)
	STFD	y08,  7 * SIZE(Y2)
	STFD	y09,  8 * SIZE(Y2)
	STFD	y10,  9 * SIZE(Y2)
	STFD	y11, 10 * SIZE(Y2)
	STFD	y12, 11 * SIZE(Y2)
	STFD	y13, 12 * SIZE(Y2)
	STFD	y14, 13 * SIZE(Y2)
	STFD	y15, 14 * SIZE(Y2)
	STFD	y16, 15 * SIZE(Y2)
	addi	Y2, Y2, 16 * SIZE
	.align 4

LL(15):
	andi.	r0, M, 7
	ble	LL(19)
	andi.	r0, M, 4
	ble	LL(16)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	LFD	y03, 2 * SIZE(Y1)
	LFD	y04, 3 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)

	LFD	y05, 4 * SIZE(Y1)
	LFD	y06, 5 * SIZE(Y1)
	LFD	y07, 6 * SIZE(Y1)
	LFD	y08, 7 * SIZE(Y1)

	LFD	a2,  1 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMADD	y05, alpha1r, a5, y05
	FMADD	y06, alpha1i, a5, y06
	FMADD	y07, alpha1r, a7, y07
	FMADD	y08, alpha1i, a7, y08

	LFD	a1,  0 * SIZE(AO2)
	LFD	a3,  2 * SIZE(AO2)
	LFD	a5,  4 * SIZE(AO2)
	LFD	a7,  6 * SIZE(AO2)

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	FMSUBX	y05, alpha1i, a6, y05
	FMADDX	y06, alpha1r, a6, y06
	FMSUBX	y07, alpha1i, a8, y07
	FMADDX	y08, alpha1r, a8, y08

	LFD	a2,  1 * SIZE(AO2)
	LFD	a4,  3 * SIZE(AO2)
	LFD	a6,  5 * SIZE(AO2)
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2r, a1, y01
	FMADD	y02, alpha2i, a1, y02
	FMADD	y03, alpha2r, a3, y03
	FMADD	y04, alpha2i, a3, y04

	FMADD	y05, alpha2r, a5, y05
	FMADD	y06, alpha2i, a5, y06
	FMADD	y07, alpha2r, a7, y07
	FMADD	y08, alpha2i, a7, y08

	LFD	a1,  0 * SIZE(AO3)
	LFD	a3,  2 * SIZE(AO3)
	LFD	a5,  4 * SIZE(AO3)
	LFD	a7,  6 * SIZE(AO3)

	FMSUBX	y01, alpha2i, a2, y01
	FMADDX	y02, alpha2r, a2, y02
	FMSUBX	y03, alpha2i, a4, y03
	FMADDX	y04, alpha2r, a4, y04

	FMSUBX	y05, alpha2i, a6, y05
	FMADDX	y06, alpha2r, a6, y06
	FMSUBX	y07, alpha2i, a8, y07
	FMADDX	y08, alpha2r, a8, y08

	LFD	a2,  1 * SIZE(AO3)
	LFD	a4,  3 * SIZE(AO3)
	LFD	a6,  5 * SIZE(AO3)
	LFD	a8,  7 * SIZE(AO3)

	FMADD	y01, alpha3r, a1, y01
	FMADD	y02, alpha3i, a1, y02
	FMADD	y03, alpha3r, a3, y03
	FMADD	y04, alpha3i, a3, y04

	FMADD	y05, alpha3r, a5, y05
	FMADD	y06, alpha3i, a5, y06
	FMADD	y07, alpha3r, a7, y07
	FMADD	y08, alpha3i, a7, y08

	LFD	a1,  0 * SIZE(AO4)
	LFD	a3,  2 * SIZE(AO4)
	LFD	a5,  4 * SIZE(AO4)
	LFD	a7,  6 * SIZE(AO4)

	FMSUBX	y01, alpha3i, a2, y01
	FMADDX	y02, alpha3r, a2, y02
	FMSUBX	y03, alpha3i, a4, y03
	FMADDX	y04, alpha3r, a4, y04

	FMSUBX	y05, alpha3i, a6, y05
	FMADDX	y06, alpha3r, a6, y06
	FMSUBX	y07, alpha3i, a8, y07
	FMADDX	y08, alpha3r, a8, y08

	LFD	a2,  1 * SIZE(AO4)
	LFD	a4,  3 * SIZE(AO4)
	LFD	a6,  5 * SIZE(AO4)
	LFD	a8,  7 * SIZE(AO4)

	FMADD	y01, alpha4r, a1, y01
	FMADD	y02, alpha4i, a1, y02
	FMADD	y03, alpha4r, a3, y03
	FMADD	y04, alpha4i, a3, y04

	FMADD	y05, alpha4r, a5, y05
	FMADD	y06, alpha4i, a5, y06
	FMADD	y07, alpha4r, a7, y07
	FMADD	y08, alpha4i, a7, y08

	FMSUBX	y01, alpha4i, a2, y01
	FMADDX	y02, alpha4r, a2, y02
	FMSUBX	y03, alpha4i, a4, y03
	FMADDX	y04, alpha4r, a4, y04

	STFD	y01, 0 * SIZE(Y2)
	STFD	y02, 1 * SIZE(Y2)
	STFD	y03, 2 * SIZE(Y2)
	STFD	y04, 3 * SIZE(Y2)

	FMSUBX	y05, alpha4i, a6, y05
	FMADDX	y06, alpha4r, a6, y06
	FMSUBX	y07, alpha4i, a8, y07
	FMADDX	y08, alpha4r, a8, y08

	STFD	y05, 4 * SIZE(Y2)
	STFD	y06, 5 * SIZE(Y2)
	STFD	y07, 6 * SIZE(Y2)
	STFD	y08, 7 * SIZE(Y2)

	addi	AO1, AO1, 8 * SIZE
	addi	AO2, AO2, 8 * SIZE
	addi	AO3, AO3, 8 * SIZE
	addi	AO4, AO4, 8 * SIZE

	addi	Y1, Y1, 8 * SIZE
	addi	Y2, Y2, 8 * SIZE
	.align 4

LL(16):
	andi.	r0, M, 2
	nop
	nop
	ble	LL(17)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	LFD	y03, 2 * SIZE(Y1)
	LFD	y04, 3 * SIZE(Y1)

	LFD	a5,  0 * SIZE(AO2)
	LFD	a6,  1 * SIZE(AO2)
	LFD	a7,  2 * SIZE(AO2)
	LFD	a8,  3 * SIZE(AO2)

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	LFD	a1,  0 * SIZE(AO3)
	LFD	a2,  1 * SIZE(AO3)
	LFD	a3,  2 * SIZE(AO3)
	LFD	a4,  3 * SIZE(AO3)

	FMADD	y01, alpha2r, a5, y01
	FMADD	y02, alpha2i, a5, y02
	FMADD	y03, alpha2r, a7, y03
	FMADD	y04, alpha2i, a7, y04

	FMSUBX	y01, alpha2i, a6, y01
	FMADDX	y02, alpha2r, a6, y02
	FMSUBX	y03, alpha2i, a8, y03
	FMADDX	y04, alpha2r, a8, y04

	LFD	a5,  0 * SIZE(AO4)
	LFD	a6,  1 * SIZE(AO4)
	LFD	a7,  2 * SIZE(AO4)
	LFD	a8,  3 * SIZE(AO4)

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

	STFD	y01, 0 * SIZE(Y2)
	STFD	y02, 1 * SIZE(Y2)
	STFD	y03, 2 * SIZE(Y2)
	STFD	y04, 3 * SIZE(Y2)

	addi	AO1, AO1, 4 * SIZE
	addi	AO2, AO2, 4 * SIZE
	addi	AO3, AO3, 4 * SIZE
	addi	AO4, AO4, 4 * SIZE

	addi	Y1, Y1, 4 * SIZE
	addi	Y2, Y2, 4 * SIZE
	.align 4

LL(17):
	andi.	r0, M, 1
	ble	LL(19)

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

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02

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

	STFD	y01, 0 * SIZE(Y2)
	STFD	y02, 1 * SIZE(Y2)

	add	Y1, Y1, INCY
	add	Y2, Y2, INCY
	.align 4

LL(19):
	addi	J, J, -1
	cmpi	cr0, 0, J, 0
	bgt	LL(11)
	.align 4

LL(20):
	andi.	J, N, 2
	ble	LL(30)
	.align 4

LL(21):
	lfd	alpha_r, ALPHA_R
	lfd	alpha_i, ALPHA_I

	LFD	a1, 0 * SIZE(X)
	LFD	a2, 1 * SIZE(X)
	add	X, X, INCX
	LFD	a3, 0 * SIZE(X)
	LFD	a4, 1 * SIZE(X)
	add	X, X, INCX

	FMUL	alpha1r, alpha_r, a1
	FMUL	alpha1i, alpha_i, a1
	FMUL	alpha2r, alpha_r, a3
	FMUL	alpha2i, alpha_i, a3

	FMSUBR	alpha1r, alpha_i, a2, alpha1r
	FMADDR	alpha1i, alpha_r, a2, alpha1i
	FMSUBR	alpha2r, alpha_i, a4, alpha2r
	FMADDR	alpha2i, alpha_r, a4, alpha2i

	mr	AO1, A
	add	AO2, A,   LDA
	add	A,   AO2, LDA

	mr	Y1, Y
	mr	Y2, Y

	srawi.	r0,  M, 3
	mtspr	CTR, r0
	ble	LL(25)
	.align 4

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)

	LFD	y01,  0 * SIZE(Y1)
	LFD	y02,  1 * SIZE(Y1)
	LFD	y03,  2 * SIZE(Y1)
	LFD	y04,  3 * SIZE(Y1)

	LFD	a5,  4 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

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
	addi	Y1, Y1, 16 * SIZE
	bdz	LL(23)
	.align 4

LL(22):
	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMADD	y05, alpha1r, a5, y05
	FMADD	y06, alpha1i, a5, y06
	FMADD	y07, alpha1r, a7, y07
	FMADD	y08, alpha1i, a7, y08

	LFD	a1,  8 * SIZE(AO1)
	LFD	a3, 10 * SIZE(AO1)
	LFD	a5, 12 * SIZE(AO1)
	LFD	a7, 14 * SIZE(AO1)

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	FMSUBX	y05, alpha1i, a6, y05
	FMADDX	y06, alpha1r, a6, y06
	FMSUBX	y07, alpha1i, a8, y07
	FMADDX	y08, alpha1r, a8, y08

	LFD	a2,  9 * SIZE(AO1)
	LFD	a4, 11 * SIZE(AO1)
	LFD	a6, 13 * SIZE(AO1)
	LFD	a8, 15 * SIZE(AO1)

	addi	AO1, AO1, 16 * SIZE
	nop
	DCBT(AO1, PREA)
	nop

	FMADD	y09, alpha1r, a1, y09
	FMADD	y10, alpha1i, a1, y10
	FMADD	y11, alpha1r, a3, y11
	FMADD	y12, alpha1i, a3, y12

	FMADD	y13, alpha1r, a5, y13
	FMADD	y14, alpha1i, a5, y14
	FMADD	y15, alpha1r, a7, y15
	FMADD	y16, alpha1i, a7, y16

	LFD	a1,  0 * SIZE(AO2)
	LFD	a3,  2 * SIZE(AO2)
	LFD	a5,  4 * SIZE(AO2)
	LFD	a7,  6 * SIZE(AO2)

	FMSUBX	y09, alpha1i, a2, y09
	FMADDX	y10, alpha1r, a2, y10
	FMSUBX	y11, alpha1i, a4, y11
	FMADDX	y12, alpha1r, a4, y12

	FMSUBX	y13, alpha1i, a6, y13
	FMADDX	y14, alpha1r, a6, y14
	FMSUBX	y15, alpha1i, a8, y15
	FMADDX	y16, alpha1r, a8, y16

	LFD	a2,  1 * SIZE(AO2)
	LFD	a4,  3 * SIZE(AO2)
	LFD	a6,  5 * SIZE(AO2)
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2r, a1, y01
	FMADD	y02, alpha2i, a1, y02
	FMADD	y03, alpha2r, a3, y03
	FMADD	y04, alpha2i, a3, y04

	FMADD	y05, alpha2r, a5, y05
	FMADD	y06, alpha2i, a5, y06
	FMADD	y07, alpha2r, a7, y07
	FMADD	y08, alpha2i, a7, y08

	LFD	a1,  8 * SIZE(AO2)
	LFD	a3, 10 * SIZE(AO2)
	LFD	a5, 12 * SIZE(AO2)
	LFD	a7, 14 * SIZE(AO2)

	FMSUBX	y01, alpha2i, a2, y01
	FMADDX	y02, alpha2r, a2, y02
	FMSUBX	y03, alpha2i, a4, y03
	FMADDX	y04, alpha2r, a4, y04

	STFD	y01,  0 * SIZE(Y2)
	STFD	y02,  1 * SIZE(Y2)
	STFD	y03,  2 * SIZE(Y2)
	STFD	y04,  3 * SIZE(Y2)

	LFD	y01,  0 * SIZE(Y1)
	LFD	y02,  1 * SIZE(Y1)
	LFD	y03,  2 * SIZE(Y1)
	LFD	y04,  3 * SIZE(Y1)

	FMSUBX	y05, alpha2i, a6, y05
	FMADDX	y06, alpha2r, a6, y06
	FMSUBX	y07, alpha2i, a8, y07
	FMADDX	y08, alpha2r, a8, y08

	LFD	a2,  9 * SIZE(AO2)
	LFD	a4, 11 * SIZE(AO2)
	LFD	a6, 13 * SIZE(AO2)
	LFD	a8, 15 * SIZE(AO2)

	STFD	y05,  4 * SIZE(Y2)
	STFD	y06,  5 * SIZE(Y2)
	STFD	y07,  6 * SIZE(Y2)
	STFD	y08,  7 * SIZE(Y2)

	LFD	y05,  4 * SIZE(Y1)
	LFD	y06,  5 * SIZE(Y1)
	LFD	y07,  6 * SIZE(Y1)
	LFD	y08,  7 * SIZE(Y1)

	addi	AO2, AO2, 16 * SIZE
	nop
	DCBT(AO2, PREA)
	nop

	FMADD	y09, alpha2r, a1, y09
	FMADD	y10, alpha2i, a1, y10
	FMADD	y11, alpha2r, a3, y11
	FMADD	y12, alpha2i, a3, y12

	FMADD	y13, alpha2r, a5, y13
	FMADD	y14, alpha2i, a5, y14
	FMADD	y15, alpha2r, a7, y15
	FMADD	y16, alpha2i, a7, y16

	LFD	a1,  0 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)

	FMSUBX	y09, alpha2i, a2, y09
	FMADDX	y10, alpha2r, a2, y10
	FMSUBX	y11, alpha2i, a4, y11
	FMADDX	y12, alpha2r, a4, y12

	STFD	y09,  8 * SIZE(Y2)
	STFD	y10,  9 * SIZE(Y2)
	STFD	y11, 10 * SIZE(Y2)
	STFD	y12, 11 * SIZE(Y2)

	LFD	y09,  8 * SIZE(Y1)
	LFD	y10,  9 * SIZE(Y1)
	LFD	y11, 10 * SIZE(Y1)
	LFD	y12, 11 * SIZE(Y1)

	FMSUBX	y13, alpha2i, a6, y13
	FMADDX	y14, alpha2r, a6, y14
	FMSUBX	y15, alpha2i, a8, y15
	FMADDX	y16, alpha2r, a8, y16

	LFD	a2,  1 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	STFD	y13, 12 * SIZE(Y2)
	STFD	y14, 13 * SIZE(Y2)
	STFD	y15, 14 * SIZE(Y2)
	STFD	y16, 15 * SIZE(Y2)

	LFD	y13, 12 * SIZE(Y1)
	LFD	y14, 13 * SIZE(Y1)
	LFD	y15, 14 * SIZE(Y1)
	LFD	y16, 15 * SIZE(Y1)

	addi	Y2, Y2, 16 * SIZE
	addi	Y1, Y1, 16 * SIZE
	DCBT(Y1, PREC)
	bdnz	LL(22)
	.align 4

LL(23):
	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMADD	y05, alpha1r, a5, y05
	FMADD	y06, alpha1i, a5, y06
	FMADD	y07, alpha1r, a7, y07
	FMADD	y08, alpha1i, a7, y08

	LFD	a1,  8 * SIZE(AO1)
	LFD	a3, 10 * SIZE(AO1)
	LFD	a5, 12 * SIZE(AO1)
	LFD	a7, 14 * SIZE(AO1)

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	FMSUBX	y05, alpha1i, a6, y05
	FMADDX	y06, alpha1r, a6, y06
	FMSUBX	y07, alpha1i, a8, y07
	FMADDX	y08, alpha1r, a8, y08

	LFD	a2,  9 * SIZE(AO1)
	LFD	a4, 11 * SIZE(AO1)
	LFD	a6, 13 * SIZE(AO1)
	LFD	a8, 15 * SIZE(AO1)

	FMADD	y09, alpha1r, a1, y09
	FMADD	y10, alpha1i, a1, y10
	FMADD	y11, alpha1r, a3, y11
	FMADD	y12, alpha1i, a3, y12

	FMADD	y13, alpha1r, a5, y13
	FMADD	y14, alpha1i, a5, y14
	FMADD	y15, alpha1r, a7, y15
	FMADD	y16, alpha1i, a7, y16

	LFD	a1,  0 * SIZE(AO2)
	LFD	a3,  2 * SIZE(AO2)
	LFD	a5,  4 * SIZE(AO2)
	LFD	a7,  6 * SIZE(AO2)

	FMSUBX	y09, alpha1i, a2, y09
	FMADDX	y10, alpha1r, a2, y10
	FMSUBX	y11, alpha1i, a4, y11
	FMADDX	y12, alpha1r, a4, y12

	FMSUBX	y13, alpha1i, a6, y13
	FMADDX	y14, alpha1r, a6, y14
	FMSUBX	y15, alpha1i, a8, y15
	FMADDX	y16, alpha1r, a8, y16

	LFD	a2,  1 * SIZE(AO2)
	LFD	a4,  3 * SIZE(AO2)
	LFD	a6,  5 * SIZE(AO2)
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2r, a1, y01
	FMADD	y02, alpha2i, a1, y02
	FMADD	y03, alpha2r, a3, y03
	FMADD	y04, alpha2i, a3, y04

	FMADD	y05, alpha2r, a5, y05
	FMADD	y06, alpha2i, a5, y06
	FMADD	y07, alpha2r, a7, y07
	FMADD	y08, alpha2i, a7, y08

	LFD	a1,  8 * SIZE(AO2)
	LFD	a3, 10 * SIZE(AO2)
	LFD	a5, 12 * SIZE(AO2)
	LFD	a7, 14 * SIZE(AO2)

	FMSUBX	y01, alpha2i, a2, y01
	FMADDX	y02, alpha2r, a2, y02
	FMSUBX	y03, alpha2i, a4, y03
	FMADDX	y04, alpha2r, a4, y04

	STFD	y01,  0 * SIZE(Y2)
	STFD	y02,  1 * SIZE(Y2)
	STFD	y03,  2 * SIZE(Y2)
	STFD	y04,  3 * SIZE(Y2)

	FMSUBX	y05, alpha2i, a6, y05
	FMADDX	y06, alpha2r, a6, y06
	FMSUBX	y07, alpha2i, a8, y07
	FMADDX	y08, alpha2r, a8, y08

	LFD	a2,  9 * SIZE(AO2)
	LFD	a4, 11 * SIZE(AO2)
	LFD	a6, 13 * SIZE(AO2)
	LFD	a8, 15 * SIZE(AO2)

	STFD	y05,  4 * SIZE(Y2)
	STFD	y06,  5 * SIZE(Y2)
	STFD	y07,  6 * SIZE(Y2)
	STFD	y08,  7 * SIZE(Y2)

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
	FMADDX	y14, alpha2r, a6, y14
	FMSUBX	y15, alpha2i, a8, y15
	FMADDX	y16, alpha2r, a8, y16

	STFD	y09,  8 * SIZE(Y2)
	STFD	y10,  9 * SIZE(Y2)
	STFD	y11, 10 * SIZE(Y2)
	STFD	y12, 11 * SIZE(Y2)

	STFD	y13, 12 * SIZE(Y2)
	STFD	y14, 13 * SIZE(Y2)
	STFD	y15, 14 * SIZE(Y2)
	STFD	y16, 15 * SIZE(Y2)

	addi	AO1, AO1, 16 * SIZE
	addi	AO2, AO2, 16 * SIZE
	addi	Y2, Y2, 16 * SIZE
	.align 4

LL(25):
	andi.	r0, M, 7
	ble	LL(30)
	andi.	r0, M, 4
	ble	LL(26)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	LFD	y03, 2 * SIZE(Y1)
	LFD	y04, 3 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)

	LFD	y05, 4 * SIZE(Y1)
	LFD	y06, 5 * SIZE(Y1)
	LFD	y07, 6 * SIZE(Y1)
	LFD	y08, 7 * SIZE(Y1)

	LFD	a2,  1 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMADD	y05, alpha1r, a5, y05
	FMADD	y06, alpha1i, a5, y06
	FMADD	y07, alpha1r, a7, y07
	FMADD	y08, alpha1i, a7, y08

	LFD	a1,  0 * SIZE(AO2)
	LFD	a3,  2 * SIZE(AO2)
	LFD	a5,  4 * SIZE(AO2)
	LFD	a7,  6 * SIZE(AO2)

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	FMSUBX	y05, alpha1i, a6, y05
	FMADDX	y06, alpha1r, a6, y06
	FMSUBX	y07, alpha1i, a8, y07
	FMADDX	y08, alpha1r, a8, y08

	LFD	a2,  1 * SIZE(AO2)
	LFD	a4,  3 * SIZE(AO2)
	LFD	a6,  5 * SIZE(AO2)
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2r, a1, y01
	FMADD	y02, alpha2i, a1, y02
	FMADD	y03, alpha2r, a3, y03
	FMADD	y04, alpha2i, a3, y04

	FMADD	y05, alpha2r, a5, y05
	FMADD	y06, alpha2i, a5, y06
	FMADD	y07, alpha2r, a7, y07
	FMADD	y08, alpha2i, a7, y08

	FMSUBX	y01, alpha2i, a2, y01
	FMADDX	y02, alpha2r, a2, y02
	FMSUBX	y03, alpha2i, a4, y03
	FMADDX	y04, alpha2r, a4, y04

	STFD	y01, 0 * SIZE(Y2)
	STFD	y02, 1 * SIZE(Y2)
	STFD	y03, 2 * SIZE(Y2)
	STFD	y04, 3 * SIZE(Y2)

	FMSUBX	y05, alpha2i, a6, y05
	FMADDX	y06, alpha2r, a6, y06
	FMSUBX	y07, alpha2i, a8, y07
	FMADDX	y08, alpha2r, a8, y08

	STFD	y05, 4 * SIZE(Y2)
	STFD	y06, 5 * SIZE(Y2)
	STFD	y07, 6 * SIZE(Y2)
	STFD	y08, 7 * SIZE(Y2)

	addi	AO1, AO1, 8 * SIZE
	addi	AO2, AO2, 8 * SIZE
	addi	Y1, Y1, 8 * SIZE
	addi	Y2, Y2, 8 * SIZE
	.align 4

LL(26):
	andi.	r0, M, 2
	ble	LL(27)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a5,  0 * SIZE(AO2)
	LFD	a7,  2 * SIZE(AO2)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	LFD	y03, 2 * SIZE(Y1)
	LFD	y04, 3 * SIZE(Y1)

	LFD	a2,  1 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a6,  1 * SIZE(AO2)
	LFD	a8,  3 * SIZE(AO2)

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

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

	STFD	y01, 0 * SIZE(Y2)
	STFD	y02, 1 * SIZE(Y2)
	STFD	y03, 2 * SIZE(Y2)
	STFD	y04, 3 * SIZE(Y2)

	addi	AO1, AO1, 4 * SIZE
	addi	AO2, AO2, 4 * SIZE
	addi	Y1, Y1, 4 * SIZE
	addi	Y2, Y2, 4 * SIZE
	.align 4

LL(27):
	andi.	r0, M, 1
	ble	LL(30)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  0 * SIZE(AO2)
	LFD	a4,  1 * SIZE(AO2)

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02

	FMADD	y01, alpha2r, a3, y01
	FMADD	y02, alpha2i, a3, y02
	FMSUBX	y01, alpha2i, a4, y01
	FMADDX	y02, alpha2r, a4, y02

	STFD	y01, 0 * SIZE(Y2)
	STFD	y02, 1 * SIZE(Y2)
	add	Y1, Y1, INCY
	add	Y2, Y2, INCY
	.align 4

LL(30):
	andi.	J, N, 1
	ble	LL(999)
	.align 4

LL(31):
	lfd	alpha_r, ALPHA_R
	lfd	alpha_i, ALPHA_I

	LFD	a1, 0 * SIZE(X)
	LFD	a2, 1 * SIZE(X)
	add	X, X, INCX

	FMUL	alpha1r, alpha_r, a1
	FMUL	alpha1i, alpha_i, a1

	FMSUBR	alpha1r, alpha_i, a2, alpha1r
	FMADDR	alpha1i, alpha_r, a2, alpha1i

	mr	AO1, A
	add	A,   AO1, LDA

	mr	Y1, Y
	mr	Y2, Y

	srawi.	r0,  M, 3
	mtspr	CTR, r0
	ble	LL(35)
	.align 4

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

	addi	Y1, Y1, 16 * SIZE
	bdz	LL(33)
	.align 4

LL(32):
	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMADD	y05, alpha1r, a5, y05
	FMADD	y06, alpha1i, a5, y06
	FMADD	y07, alpha1r, a7, y07
	FMADD	y08, alpha1i, a7, y08

	LFD	a1,  8 * SIZE(AO1)
	LFD	a3, 10 * SIZE(AO1)
	LFD	a5, 12 * SIZE(AO1)
	LFD	a7, 14 * SIZE(AO1)

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	STFD	y01,  0 * SIZE(Y2)
	STFD	y02,  1 * SIZE(Y2)
	STFD	y03,  2 * SIZE(Y2)
	STFD	y04,  3 * SIZE(Y2)

	LFD	y01,  0 * SIZE(Y1)
	LFD	y02,  1 * SIZE(Y1)
	LFD	y03,  2 * SIZE(Y1)
	LFD	y04,  3 * SIZE(Y1)

	FMSUBX	y05, alpha1i, a6, y05
	FMADDX	y06, alpha1r, a6, y06
	FMSUBX	y07, alpha1i, a8, y07
	FMADDX	y08, alpha1r, a8, y08

	LFD	a2,  9 * SIZE(AO1)
	LFD	a4, 11 * SIZE(AO1)
	LFD	a6, 13 * SIZE(AO1)
	LFD	a8, 15 * SIZE(AO1)

	addi	AO1, AO1, 16 * SIZE
	nop
	DCBT(AO1, PREA)
	nop

	STFD	y05,  4 * SIZE(Y2)
	STFD	y06,  5 * SIZE(Y2)
	STFD	y07,  6 * SIZE(Y2)
	STFD	y08,  7 * SIZE(Y2)

	LFD	y05,  4 * SIZE(Y1)
	LFD	y06,  5 * SIZE(Y1)
	LFD	y07,  6 * SIZE(Y1)
	LFD	y08,  7 * SIZE(Y1)

	FMADD	y09, alpha1r, a1, y09
	FMADD	y10, alpha1i, a1, y10
	FMADD	y11, alpha1r, a3, y11
	FMADD	y12, alpha1i, a3, y12

	FMADD	y13, alpha1r, a5, y13
	FMADD	y14, alpha1i, a5, y14
	FMADD	y15, alpha1r, a7, y15
	FMADD	y16, alpha1i, a7, y16

	LFD	a1,  0 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)

	FMSUBX	y09, alpha1i, a2, y09
	FMADDX	y10, alpha1r, a2, y10
	FMSUBX	y11, alpha1i, a4, y11
	FMADDX	y12, alpha1r, a4, y12

	STFD	y09,  8 * SIZE(Y2)
	STFD	y10,  9 * SIZE(Y2)
	STFD	y11, 10 * SIZE(Y2)
	STFD	y12, 11 * SIZE(Y2)

	LFD	y09,  8 * SIZE(Y1)
	LFD	y10,  9 * SIZE(Y1)
	LFD	y11, 10 * SIZE(Y1)
	LFD	y12, 11 * SIZE(Y1)

	FMSUBX	y13, alpha1i, a6, y13
	FMADDX	y14, alpha1r, a6, y14
	FMSUBX	y15, alpha1i, a8, y15
	FMADDX	y16, alpha1r, a8, y16

	LFD	a2,  1 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	STFD	y13, 12 * SIZE(Y2)
	STFD	y14, 13 * SIZE(Y2)
	STFD	y15, 14 * SIZE(Y2)
	STFD	y16, 15 * SIZE(Y2)

	LFD	y13, 12 * SIZE(Y1)
	LFD	y14, 13 * SIZE(Y1)
	LFD	y15, 14 * SIZE(Y1)
	LFD	y16, 15 * SIZE(Y1)

	addi	Y1, Y1, 16 * SIZE
	addi	Y2, Y2, 16 * SIZE
	DCBT(Y1, PREC)
	bdnz	LL(32)
	.align 4

LL(33):
	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMADD	y05, alpha1r, a5, y05
	FMADD	y06, alpha1i, a5, y06
	FMADD	y07, alpha1r, a7, y07
	FMADD	y08, alpha1i, a7, y08

	LFD	a1,  8 * SIZE(AO1)
	LFD	a3, 10 * SIZE(AO1)
	LFD	a5, 12 * SIZE(AO1)
	LFD	a7, 14 * SIZE(AO1)

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	STFD	y01,  0 * SIZE(Y2)
	STFD	y02,  1 * SIZE(Y2)
	STFD	y03,  2 * SIZE(Y2)
	STFD	y04,  3 * SIZE(Y2)

	FMSUBX	y05, alpha1i, a6, y05
	FMADDX	y06, alpha1r, a6, y06
	FMSUBX	y07, alpha1i, a8, y07
	FMADDX	y08, alpha1r, a8, y08

	LFD	a2,  9 * SIZE(AO1)
	LFD	a4, 11 * SIZE(AO1)
	LFD	a6, 13 * SIZE(AO1)
	LFD	a8, 15 * SIZE(AO1)

	STFD	y05,  4 * SIZE(Y2)
	STFD	y06,  5 * SIZE(Y2)
	STFD	y07,  6 * SIZE(Y2)
	STFD	y08,  7 * SIZE(Y2)

	FMADD	y09, alpha1r, a1, y09
	FMADD	y10, alpha1i, a1, y10
	FMADD	y11, alpha1r, a3, y11
	FMADD	y12, alpha1i, a3, y12

	FMADD	y13, alpha1r, a5, y13
	FMADD	y14, alpha1i, a5, y14
	FMADD	y15, alpha1r, a7, y15
	FMADD	y16, alpha1i, a7, y16

	FMSUBX	y09, alpha1i, a2, y09
	FMADDX	y10, alpha1r, a2, y10
	FMSUBX	y11, alpha1i, a4, y11
	FMADDX	y12, alpha1r, a4, y12

	STFD	y09,  8 * SIZE(Y2)
	STFD	y10,  9 * SIZE(Y2)
	STFD	y11, 10 * SIZE(Y2)
	STFD	y12, 11 * SIZE(Y2)

	FMSUBX	y13, alpha1i, a6, y13
	FMADDX	y14, alpha1r, a6, y14
	FMSUBX	y15, alpha1i, a8, y15
	FMADDX	y16, alpha1r, a8, y16

	STFD	y13, 12 * SIZE(Y2)
	STFD	y14, 13 * SIZE(Y2)
	STFD	y15, 14 * SIZE(Y2)
	STFD	y16, 15 * SIZE(Y2)

	addi	AO1, AO1, 16 * SIZE
	addi	Y2, Y2, 16 * SIZE
	.align 4

LL(35):
	andi.	r0, M, 7
	ble	LL(999)
	andi.	r0, M, 4
	ble	LL(36)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	LFD	y03, 2 * SIZE(Y1)
	LFD	y04, 3 * SIZE(Y1)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)

	LFD	y05, 4 * SIZE(Y1)
	LFD	y06, 5 * SIZE(Y1)
	LFD	y07, 6 * SIZE(Y1)
	LFD	y08, 7 * SIZE(Y1)

	LFD	a2,  1 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMADD	y05, alpha1r, a5, y05
	FMADD	y06, alpha1i, a5, y06
	FMADD	y07, alpha1r, a7, y07
	FMADD	y08, alpha1i, a7, y08

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	FMSUBX	y05, alpha1i, a6, y05
	FMADDX	y06, alpha1r, a6, y06
	FMSUBX	y07, alpha1i, a8, y07
	FMADDX	y08, alpha1r, a8, y08

	STFD	y01, 0 * SIZE(Y2)
	STFD	y02, 1 * SIZE(Y2)
	STFD	y03, 2 * SIZE(Y2)
	STFD	y04, 3 * SIZE(Y2)
	STFD	y05, 4 * SIZE(Y2)
	STFD	y06, 5 * SIZE(Y2)
	STFD	y07, 6 * SIZE(Y2)
	STFD	y08, 7 * SIZE(Y2)

	addi	AO1, AO1, 8 * SIZE
	addi	Y1, Y1, 8 * SIZE
	addi	Y2, Y2, 8 * SIZE
	.align 4

LL(36):
	andi.	r0, M, 2
	ble	LL(37)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	LFD	y03, 2 * SIZE(Y1)
	LFD	y04, 3 * SIZE(Y1)

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	STFD	y01, 0 * SIZE(Y2)
	STFD	y02, 1 * SIZE(Y2)
	STFD	y03, 2 * SIZE(Y2)
	STFD	y04, 3 * SIZE(Y2)

	addi	AO1, AO1, 4 * SIZE
	addi	Y1, Y1, 4 * SIZE
	addi	Y2, Y2, 4 * SIZE
	.align 4

LL(37):
	andi.	r0, M, 1
	ble	LL(999)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02

	STFD	y01, 0 * SIZE(Y2)
	STFD	y02, 1 * SIZE(Y2)
	add	Y1, Y1, INCY
	add	Y2, Y2, INCY
	b	LL(999)
	.align 4

LL(100):
	srawi.	J, N, 2
	ble	LL(120)
	.align 4

LL(111):
	lfd	alpha_r, ALPHA_R
	lfd	alpha_i, ALPHA_I

	LFD	a1, 0 * SIZE(X)
	LFD	a2, 1 * SIZE(X)
	add	X, X, INCX
	LFD	a3, 0 * SIZE(X)
	LFD	a4, 1 * SIZE(X)
	add	X, X, INCX
	LFD	a5, 0 * SIZE(X)
	LFD	a6, 1 * SIZE(X)
	add	X, X, INCX
	LFD	a7, 0 * SIZE(X)
	LFD	a8, 1 * SIZE(X)
	add	X, X, INCX

	FMUL	alpha1r, alpha_r, a1
	FMUL	alpha1i, alpha_i, a1
	FMUL	alpha2r, alpha_r, a3
	FMUL	alpha2i, alpha_i, a3
	FMUL	alpha3r, alpha_r, a5
	FMUL	alpha3i, alpha_i, a5
	FMUL	alpha4r, alpha_r, a7
	FMUL	alpha4i, alpha_i, a7

	FMSUBR	alpha1r, alpha_i, a2, alpha1r
	FMADDR	alpha1i, alpha_r, a2, alpha1i
	FMSUBR	alpha2r, alpha_i, a4, alpha2r
	FMADDR	alpha2i, alpha_r, a4, alpha2i
	FMSUBR	alpha3r, alpha_i, a6, alpha3r
	FMADDR	alpha3i, alpha_r, a6, alpha3i
	FMSUBR	alpha4r, alpha_i, a8, alpha4r
	FMADDR	alpha4i, alpha_r, a8, alpha4i

	mr	AO1, A
	add	AO2, A,   LDA
	add	AO3, AO2, LDA
	add	AO4, AO3, LDA
	add	A,   AO4, LDA

	mr	Y1, Y
	mr	Y2, Y

	srawi.	r0,  M, 3
	mtspr	CTR, r0
	ble	LL(115)
	.align 4

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y03, 0 * SIZE(Y1)
	LFD	y04, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y05, 0 * SIZE(Y1)
	LFD	y06, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y07, 0 * SIZE(Y1)
	LFD	y08, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y09, 0 * SIZE(Y1)
	LFD	y10, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y11, 0 * SIZE(Y1)
	LFD	y12, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y13, 0 * SIZE(Y1)
	LFD	y14, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y15, 0 * SIZE(Y1)
	LFD	y16, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)
	bdz	LL(113)
	.align 4

LL(112):
	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMADD	y05, alpha1r, a5, y05
	FMADD	y06, alpha1i, a5, y06
	FMADD	y07, alpha1r, a7, y07
	FMADD	y08, alpha1i, a7, y08

	LFD	a1,  8 * SIZE(AO1)
	LFD	a3, 10 * SIZE(AO1)
	LFD	a5, 12 * SIZE(AO1)
	LFD	a7, 14 * SIZE(AO1)

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	FMSUBX	y05, alpha1i, a6, y05
	FMADDX	y06, alpha1r, a6, y06
	FMSUBX	y07, alpha1i, a8, y07
	FMADDX	y08, alpha1r, a8, y08

	LFD	a2,  9 * SIZE(AO1)
	LFD	a4, 11 * SIZE(AO1)
	LFD	a6, 13 * SIZE(AO1)
	LFD	a8, 15 * SIZE(AO1)

	addi	AO1, AO1, 16 * SIZE
	nop
	DCBT(AO1, PREA)
	nop

	FMADD	y09, alpha1r, a1, y09
	FMADD	y10, alpha1i, a1, y10
	FMADD	y11, alpha1r, a3, y11
	FMADD	y12, alpha1i, a3, y12

	FMADD	y13, alpha1r, a5, y13
	FMADD	y14, alpha1i, a5, y14
	FMADD	y15, alpha1r, a7, y15
	FMADD	y16, alpha1i, a7, y16

	LFD	a1,  0 * SIZE(AO2)
	LFD	a3,  2 * SIZE(AO2)
	LFD	a5,  4 * SIZE(AO2)
	LFD	a7,  6 * SIZE(AO2)

	FMSUBX	y09, alpha1i, a2, y09
	FMADDX	y10, alpha1r, a2, y10
	FMSUBX	y11, alpha1i, a4, y11
	FMADDX	y12, alpha1r, a4, y12

	FMSUBX	y13, alpha1i, a6, y13
	FMADDX	y14, alpha1r, a6, y14
	FMSUBX	y15, alpha1i, a8, y15
	FMADDX	y16, alpha1r, a8, y16

	LFD	a2,  1 * SIZE(AO2)
	LFD	a4,  3 * SIZE(AO2)
	LFD	a6,  5 * SIZE(AO2)
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2r, a1, y01
	FMADD	y02, alpha2i, a1, y02
	FMADD	y03, alpha2r, a3, y03
	FMADD	y04, alpha2i, a3, y04

	FMADD	y05, alpha2r, a5, y05
	FMADD	y06, alpha2i, a5, y06
	FMADD	y07, alpha2r, a7, y07
	FMADD	y08, alpha2i, a7, y08

	LFD	a1,  8 * SIZE(AO2)
	LFD	a3, 10 * SIZE(AO2)
	LFD	a5, 12 * SIZE(AO2)
	LFD	a7, 14 * SIZE(AO2)

	FMSUBX	y01, alpha2i, a2, y01
	FMADDX	y02, alpha2r, a2, y02
	FMSUBX	y03, alpha2i, a4, y03
	FMADDX	y04, alpha2r, a4, y04

	FMSUBX	y05, alpha2i, a6, y05
	FMADDX	y06, alpha2r, a6, y06
	FMSUBX	y07, alpha2i, a8, y07
	FMADDX	y08, alpha2r, a8, y08

	LFD	a2,  9 * SIZE(AO2)
	LFD	a4, 11 * SIZE(AO2)
	LFD	a6, 13 * SIZE(AO2)
	LFD	a8, 15 * SIZE(AO2)

	addi	AO2, AO2, 16 * SIZE
	nop
	DCBT(AO2, PREA)
	nop

	FMADD	y09, alpha2r, a1, y09
	FMADD	y10, alpha2i, a1, y10
	FMADD	y11, alpha2r, a3, y11
	FMADD	y12, alpha2i, a3, y12

	FMADD	y13, alpha2r, a5, y13
	FMADD	y14, alpha2i, a5, y14
	FMADD	y15, alpha2r, a7, y15
	FMADD	y16, alpha2i, a7, y16

	LFD	a1,  0 * SIZE(AO3)
	LFD	a3,  2 * SIZE(AO3)
	LFD	a5,  4 * SIZE(AO3)
	LFD	a7,  6 * SIZE(AO3)

	FMSUBX	y09, alpha2i, a2, y09
	FMADDX	y10, alpha2r, a2, y10
	FMSUBX	y11, alpha2i, a4, y11
	FMADDX	y12, alpha2r, a4, y12

	FMSUBX	y13, alpha2i, a6, y13
	FMADDX	y14, alpha2r, a6, y14
	FMSUBX	y15, alpha2i, a8, y15
	FMADDX	y16, alpha2r, a8, y16

	LFD	a2,  1 * SIZE(AO3)
	LFD	a4,  3 * SIZE(AO3)
	LFD	a6,  5 * SIZE(AO3)
	LFD	a8,  7 * SIZE(AO3)

	FMADD	y01, alpha3r, a1, y01
	FMADD	y02, alpha3i, a1, y02
	FMADD	y03, alpha3r, a3, y03
	FMADD	y04, alpha3i, a3, y04

	FMADD	y05, alpha3r, a5, y05
	FMADD	y06, alpha3i, a5, y06
	FMADD	y07, alpha3r, a7, y07
	FMADD	y08, alpha3i, a7, y08

	LFD	a1,  8 * SIZE(AO3)
	LFD	a3, 10 * SIZE(AO3)
	LFD	a5, 12 * SIZE(AO3)
	LFD	a7, 14 * SIZE(AO3)

	FMSUBX	y01, alpha3i, a2, y01
	FMADDX	y02, alpha3r, a2, y02
	FMSUBX	y03, alpha3i, a4, y03
	FMADDX	y04, alpha3r, a4, y04

	FMSUBX	y05, alpha3i, a6, y05
	FMADDX	y06, alpha3r, a6, y06
	FMSUBX	y07, alpha3i, a8, y07
	FMADDX	y08, alpha3r, a8, y08

	LFD	a2,  9 * SIZE(AO3)
	LFD	a4, 11 * SIZE(AO3)
	LFD	a6, 13 * SIZE(AO3)
	LFD	a8, 15 * SIZE(AO3)

	addi	AO3, AO3, 16 * SIZE
	nop
	DCBT(AO3, PREA)
	nop

	FMADD	y09, alpha3r, a1, y09
	FMADD	y10, alpha3i, a1, y10
	FMADD	y11, alpha3r, a3, y11
	FMADD	y12, alpha3i, a3, y12

	FMADD	y13, alpha3r, a5, y13
	FMADD	y14, alpha3i, a5, y14
	FMADD	y15, alpha3r, a7, y15
	FMADD	y16, alpha3i, a7, y16

	LFD	a1,  0 * SIZE(AO4)
	LFD	a3,  2 * SIZE(AO4)
	LFD	a5,  4 * SIZE(AO4)
	LFD	a7,  6 * SIZE(AO4)

	FMSUBX	y09, alpha3i, a2, y09
	FMADDX	y10, alpha3r, a2, y10
	FMSUBX	y11, alpha3i, a4, y11
	FMADDX	y12, alpha3r, a4, y12

	FMSUBX	y13, alpha3i, a6, y13
	FMADDX	y14, alpha3r, a6, y14
	FMSUBX	y15, alpha3i, a8, y15
	FMADDX	y16, alpha3r, a8, y16

	LFD	a2,  1 * SIZE(AO4)
	LFD	a4,  3 * SIZE(AO4)
	LFD	a6,  5 * SIZE(AO4)
	LFD	a8,  7 * SIZE(AO4)

	FMADD	y01, alpha4r, a1, y01
	FMADD	y02, alpha4i, a1, y02
	FMADD	y03, alpha4r, a3, y03
	FMADD	y04, alpha4i, a3, y04

	FMADD	y05, alpha4r, a5, y05
	FMADD	y06, alpha4i, a5, y06
	FMADD	y07, alpha4r, a7, y07
	FMADD	y08, alpha4i, a7, y08

	LFD	a1,  8 * SIZE(AO4)
	LFD	a3, 10 * SIZE(AO4)
	LFD	a5, 12 * SIZE(AO4)
 	LFD	a7, 14 * SIZE(AO4)

	FMSUBX	y01, alpha4i, a2, y01
	FMADDX	y02, alpha4r, a2, y02
	FMSUBX	y03, alpha4i, a4, y03
	FMADDX	y04, alpha4r, a4, y04

	STFD	y01, 0 * SIZE(Y2)
	nop
	STFD	y02, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y01, 0 * SIZE(Y1)
	nop
	LFD	y02, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	STFD	y03, 0 * SIZE(Y2)
	nop
	STFD	y04, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y03, 0 * SIZE(Y1)
	nop
	LFD	y04, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	FMSUBX	y05, alpha4i, a6, y05
	FMADDX	y06, alpha4r, a6, y06
	FMSUBX	y07, alpha4i, a8, y07
	FMADDX	y08, alpha4r, a8, y08

	LFD	a2,  9 * SIZE(AO4)
	LFD	a4, 11 * SIZE(AO4)
	LFD	a6, 13 * SIZE(AO4)
	LFD	a8, 15 * SIZE(AO4)

	addi	AO4, AO4, 16 * SIZE
	nop
	DCBT(AO4, PREA)
	nop

	STFD	y05, 0 * SIZE(Y2)
	nop
	STFD	y06, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y05, 0 * SIZE(Y1)
	nop
	LFD	y06, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	STFD	y07, 0 * SIZE(Y2)
	nop
	STFD	y08, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y07, 0 * SIZE(Y1)
	nop
	LFD	y08, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	FMADD	y09, alpha4r, a1, y09
	FMADD	y10, alpha4i, a1, y10
	FMADD	y11, alpha4r, a3, y11
	FMADD	y12, alpha4i, a3, y12

	FMADD	y13, alpha4r, a5, y13
	FMADD	y14, alpha4i, a5, y14
	FMADD	y15, alpha4r, a7, y15
	FMADD	y16, alpha4i, a7, y16

	LFD	a1,  0 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)

	FMSUBX	y09, alpha4i, a2, y09
	FMADDX	y10, alpha4r, a2, y10
	FMSUBX	y11, alpha4i, a4, y11
	FMADDX	y12, alpha4r, a4, y12

	STFD	y09, 0 * SIZE(Y2)
	nop
	STFD	y10, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y09, 0 * SIZE(Y1)
	nop
	LFD	y10, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	STFD	y11, 0 * SIZE(Y2)
	nop
	STFD	y12, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y11, 0 * SIZE(Y1)
	nop
	LFD	y12, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	FMSUBX	y13, alpha4i, a6, y13
	FMADDX	y14, alpha4r, a6, y14
	FMSUBX	y15, alpha4i, a8, y15
	FMADDX	y16, alpha4r, a8, y16

	LFD	a2,  1 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	STFD	y13, 0 * SIZE(Y2)
	nop
	STFD	y14, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y13, 0 * SIZE(Y1)
	nop
	LFD	y14, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	STFD	y15, 0 * SIZE(Y2)
	nop
	STFD	y16, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y15, 0 * SIZE(Y1)
	nop
	LFD	y16, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	DCBT(Y1, PREC)
	bdnz	LL(112)
	.align 4

LL(113):
	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMADD	y05, alpha1r, a5, y05
	FMADD	y06, alpha1i, a5, y06
	FMADD	y07, alpha1r, a7, y07
	FMADD	y08, alpha1i, a7, y08

	LFD	a1,  8 * SIZE(AO1)
	LFD	a3, 10 * SIZE(AO1)
	LFD	a5, 12 * SIZE(AO1)
	LFD	a7, 14 * SIZE(AO1)

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	FMSUBX	y05, alpha1i, a6, y05
	FMADDX	y06, alpha1r, a6, y06
	FMSUBX	y07, alpha1i, a8, y07
	FMADDX	y08, alpha1r, a8, y08

	LFD	a2,  9 * SIZE(AO1)
	LFD	a4, 11 * SIZE(AO1)
	LFD	a6, 13 * SIZE(AO1)
	LFD	a8, 15 * SIZE(AO1)

	FMADD	y09, alpha1r, a1, y09
	FMADD	y10, alpha1i, a1, y10
	FMADD	y11, alpha1r, a3, y11
	FMADD	y12, alpha1i, a3, y12

	FMADD	y13, alpha1r, a5, y13
	FMADD	y14, alpha1i, a5, y14
	FMADD	y15, alpha1r, a7, y15
	FMADD	y16, alpha1i, a7, y16

	LFD	a1,  0 * SIZE(AO2)
	LFD	a3,  2 * SIZE(AO2)
	LFD	a5,  4 * SIZE(AO2)
	LFD	a7,  6 * SIZE(AO2)

	FMSUBX	y09, alpha1i, a2, y09
	FMADDX	y10, alpha1r, a2, y10
	FMSUBX	y11, alpha1i, a4, y11
	FMADDX	y12, alpha1r, a4, y12

	FMSUBX	y13, alpha1i, a6, y13
	FMADDX	y14, alpha1r, a6, y14
	FMSUBX	y15, alpha1i, a8, y15
	FMADDX	y16, alpha1r, a8, y16

	LFD	a2,  1 * SIZE(AO2)
	LFD	a4,  3 * SIZE(AO2)
	LFD	a6,  5 * SIZE(AO2)
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2r, a1, y01
	FMADD	y02, alpha2i, a1, y02
	FMADD	y03, alpha2r, a3, y03
	FMADD	y04, alpha2i, a3, y04

	FMADD	y05, alpha2r, a5, y05
	FMADD	y06, alpha2i, a5, y06
	FMADD	y07, alpha2r, a7, y07
	FMADD	y08, alpha2i, a7, y08

	LFD	a1,  8 * SIZE(AO2)
	LFD	a3, 10 * SIZE(AO2)
	LFD	a5, 12 * SIZE(AO2)
	LFD	a7, 14 * SIZE(AO2)

	FMSUBX	y01, alpha2i, a2, y01
	FMADDX	y02, alpha2r, a2, y02
	FMSUBX	y03, alpha2i, a4, y03
	FMADDX	y04, alpha2r, a4, y04

	FMSUBX	y05, alpha2i, a6, y05
	FMADDX	y06, alpha2r, a6, y06
	FMSUBX	y07, alpha2i, a8, y07
	FMADDX	y08, alpha2r, a8, y08

	LFD	a2,  9 * SIZE(AO2)
	LFD	a4, 11 * SIZE(AO2)
	LFD	a6, 13 * SIZE(AO2)
	LFD	a8, 15 * SIZE(AO2)

	FMADD	y09, alpha2r, a1, y09
	FMADD	y10, alpha2i, a1, y10
	FMADD	y11, alpha2r, a3, y11
	FMADD	y12, alpha2i, a3, y12

	FMADD	y13, alpha2r, a5, y13
	FMADD	y14, alpha2i, a5, y14
	FMADD	y15, alpha2r, a7, y15
	FMADD	y16, alpha2i, a7, y16

	LFD	a1,  0 * SIZE(AO3)
	LFD	a3,  2 * SIZE(AO3)
	LFD	a5,  4 * SIZE(AO3)
	LFD	a7,  6 * SIZE(AO3)

	FMSUBX	y09, alpha2i, a2, y09
	FMADDX	y10, alpha2r, a2, y10
	FMSUBX	y11, alpha2i, a4, y11
	FMADDX	y12, alpha2r, a4, y12

	FMSUBX	y13, alpha2i, a6, y13
	FMADDX	y14, alpha2r, a6, y14
	FMSUBX	y15, alpha2i, a8, y15
	FMADDX	y16, alpha2r, a8, y16

	LFD	a2,  1 * SIZE(AO3)
	LFD	a4,  3 * SIZE(AO3)
	LFD	a6,  5 * SIZE(AO3)
	LFD	a8,  7 * SIZE(AO3)

	FMADD	y01, alpha3r, a1, y01
	FMADD	y02, alpha3i, a1, y02
	FMADD	y03, alpha3r, a3, y03
	FMADD	y04, alpha3i, a3, y04

	FMADD	y05, alpha3r, a5, y05
	FMADD	y06, alpha3i, a5, y06
	FMADD	y07, alpha3r, a7, y07
	FMADD	y08, alpha3i, a7, y08

	LFD	a1,  8 * SIZE(AO3)
	LFD	a3, 10 * SIZE(AO3)
	LFD	a5, 12 * SIZE(AO3)
	LFD	a7, 14 * SIZE(AO3)

	FMSUBX	y01, alpha3i, a2, y01
	FMADDX	y02, alpha3r, a2, y02
	FMSUBX	y03, alpha3i, a4, y03
	FMADDX	y04, alpha3r, a4, y04

	FMSUBX	y05, alpha3i, a6, y05
	FMADDX	y06, alpha3r, a6, y06
	FMSUBX	y07, alpha3i, a8, y07
	FMADDX	y08, alpha3r, a8, y08

	LFD	a2,  9 * SIZE(AO3)
	LFD	a4, 11 * SIZE(AO3)
	LFD	a6, 13 * SIZE(AO3)
	LFD	a8, 15 * SIZE(AO3)

	FMADD	y09, alpha3r, a1, y09
	FMADD	y10, alpha3i, a1, y10
	FMADD	y11, alpha3r, a3, y11
	FMADD	y12, alpha3i, a3, y12

	FMADD	y13, alpha3r, a5, y13
	FMADD	y14, alpha3i, a5, y14
	FMADD	y15, alpha3r, a7, y15
	FMADD	y16, alpha3i, a7, y16

	LFD	a1,  0 * SIZE(AO4)
	LFD	a3,  2 * SIZE(AO4)
	LFD	a5,  4 * SIZE(AO4)
	LFD	a7,  6 * SIZE(AO4)

	FMSUBX	y09, alpha3i, a2, y09
	FMADDX	y10, alpha3r, a2, y10
	FMSUBX	y11, alpha3i, a4, y11
	FMADDX	y12, alpha3r, a4, y12

	FMSUBX	y13, alpha3i, a6, y13
	FMADDX	y14, alpha3r, a6, y14
	FMSUBX	y15, alpha3i, a8, y15
	FMADDX	y16, alpha3r, a8, y16

	LFD	a2,  1 * SIZE(AO4)
	LFD	a4,  3 * SIZE(AO4)
	LFD	a6,  5 * SIZE(AO4)
	LFD	a8,  7 * SIZE(AO4)

	FMADD	y01, alpha4r, a1, y01
	FMADD	y02, alpha4i, a1, y02
	FMADD	y03, alpha4r, a3, y03
	FMADD	y04, alpha4i, a3, y04

	FMADD	y05, alpha4r, a5, y05
	FMADD	y06, alpha4i, a5, y06
	FMADD	y07, alpha4r, a7, y07
	FMADD	y08, alpha4i, a7, y08

	LFD	a1,  8 * SIZE(AO4)
	LFD	a3, 10 * SIZE(AO4)
	LFD	a5, 12 * SIZE(AO4)
	LFD	a7, 14 * SIZE(AO4)

	FMSUBX	y01, alpha4i, a2, y01
	FMADDX	y02, alpha4r, a2, y02
	FMSUBX	y03, alpha4i, a4, y03
	FMADDX	y04, alpha4r, a4, y04

	STFD	y01, 0 * SIZE(Y2)
	nop
	STFD	y02, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y03, 0 * SIZE(Y2)
	nop
	STFD	y04, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	FMSUBX	y05, alpha4i, a6, y05
	FMADDX	y06, alpha4r, a6, y06
	FMSUBX	y07, alpha4i, a8, y07
	FMADDX	y08, alpha4r, a8, y08

	LFD	a2,  9 * SIZE(AO4)
	LFD	a4, 11 * SIZE(AO4)
	LFD	a6, 13 * SIZE(AO4)
	LFD	a8, 15 * SIZE(AO4)

	STFD	y05, 0 * SIZE(Y2)
	nop
	STFD	y06, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y07, 0 * SIZE(Y2)
	nop
	STFD	y08, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

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

	STFD	y09, 0 * SIZE(Y2)
	nop
	STFD	y10, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y11, 0 * SIZE(Y2)
	nop
	STFD	y12, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	FMSUBX	y13, alpha4i, a6, y13
	FMADDX	y14, alpha4r, a6, y14
	FMSUBX	y15, alpha4i, a8, y15
	FMADDX	y16, alpha4r, a8, y16

	STFD	y13, 0 * SIZE(Y2)
	nop
	STFD	y14, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y15, 0 * SIZE(Y2)
	nop
	STFD	y16, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	addi	AO1, AO1, 16 * SIZE
	addi	AO2, AO2, 16 * SIZE
	addi	AO3, AO3, 16 * SIZE
	addi	AO4, AO4, 16 * SIZE
	.align 4

LL(115):
	andi.	r0, M, 7
	ble	LL(119)
	andi.	r0, M, 4
	ble	LL(116)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y03, 0 * SIZE(Y1)
	LFD	y04, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	LFD	a1,  0 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)

	LFD	y05, 0 * SIZE(Y1)
	LFD	y06, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y07, 0 * SIZE(Y1)
	LFD	y08, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	LFD	a2,  1 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMADD	y05, alpha1r, a5, y05
	FMADD	y06, alpha1i, a5, y06
	FMADD	y07, alpha1r, a7, y07
	FMADD	y08, alpha1i, a7, y08

	LFD	a1,  0 * SIZE(AO2)
	LFD	a3,  2 * SIZE(AO2)
	LFD	a5,  4 * SIZE(AO2)
	LFD	a7,  6 * SIZE(AO2)

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	FMSUBX	y05, alpha1i, a6, y05
	FMADDX	y06, alpha1r, a6, y06
	FMSUBX	y07, alpha1i, a8, y07
	FMADDX	y08, alpha1r, a8, y08

	LFD	a2,  1 * SIZE(AO2)
	LFD	a4,  3 * SIZE(AO2)
	LFD	a6,  5 * SIZE(AO2)
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2r, a1, y01
	FMADD	y02, alpha2i, a1, y02
	FMADD	y03, alpha2r, a3, y03
	FMADD	y04, alpha2i, a3, y04

	FMADD	y05, alpha2r, a5, y05
	FMADD	y06, alpha2i, a5, y06
	FMADD	y07, alpha2r, a7, y07
	FMADD	y08, alpha2i, a7, y08

	LFD	a1,  0 * SIZE(AO3)
	LFD	a3,  2 * SIZE(AO3)
	LFD	a5,  4 * SIZE(AO3)
	LFD	a7,  6 * SIZE(AO3)

	FMSUBX	y01, alpha2i, a2, y01
	FMADDX	y02, alpha2r, a2, y02
	FMSUBX	y03, alpha2i, a4, y03
	FMADDX	y04, alpha2r, a4, y04

	FMSUBX	y05, alpha2i, a6, y05
	FMADDX	y06, alpha2r, a6, y06
	FMSUBX	y07, alpha2i, a8, y07
	FMADDX	y08, alpha2r, a8, y08

	LFD	a2,  1 * SIZE(AO3)
	LFD	a4,  3 * SIZE(AO3)
	LFD	a6,  5 * SIZE(AO3)
	LFD	a8,  7 * SIZE(AO3)

	FMADD	y01, alpha3r, a1, y01
	FMADD	y02, alpha3i, a1, y02
	FMADD	y03, alpha3r, a3, y03
	FMADD	y04, alpha3i, a3, y04

	FMADD	y05, alpha3r, a5, y05
	FMADD	y06, alpha3i, a5, y06
	FMADD	y07, alpha3r, a7, y07
	FMADD	y08, alpha3i, a7, y08

	LFD	a1,  0 * SIZE(AO4)
	LFD	a3,  2 * SIZE(AO4)
	LFD	a5,  4 * SIZE(AO4)
	LFD	a7,  6 * SIZE(AO4)

	FMSUBX	y01, alpha3i, a2, y01
	FMADDX	y02, alpha3r, a2, y02
	FMSUBX	y03, alpha3i, a4, y03
	FMADDX	y04, alpha3r, a4, y04

	FMSUBX	y05, alpha3i, a6, y05
	FMADDX	y06, alpha3r, a6, y06
	FMSUBX	y07, alpha3i, a8, y07
	FMADDX	y08, alpha3r, a8, y08

	LFD	a2,  1 * SIZE(AO4)
	LFD	a4,  3 * SIZE(AO4)
	LFD	a6,  5 * SIZE(AO4)
	LFD	a8,  7 * SIZE(AO4)

	FMADD	y01, alpha4r, a1, y01
	FMADD	y02, alpha4i, a1, y02
	FMADD	y03, alpha4r, a3, y03
	FMADD	y04, alpha4i, a3, y04

	FMADD	y05, alpha4r, a5, y05
	FMADD	y06, alpha4i, a5, y06
	FMADD	y07, alpha4r, a7, y07
	FMADD	y08, alpha4i, a7, y08

	FMSUBX	y01, alpha4i, a2, y01
	FMADDX	y02, alpha4r, a2, y02
	FMSUBX	y03, alpha4i, a4, y03
	FMADDX	y04, alpha4r, a4, y04

	FMSUBX	y05, alpha4i, a6, y05
	FMADDX	y06, alpha4r, a6, y06
	FMSUBX	y07, alpha4i, a8, y07
	FMADDX	y08, alpha4r, a8, y08

	STFD	y01, 0 * SIZE(Y2)
	addi	AO1, AO1, 8 * SIZE
	STFD	y02, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y03, 0 * SIZE(Y2)
	addi	AO2, AO2, 8 * SIZE
	STFD	y04, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y05, 0 * SIZE(Y2)
	addi	AO3, AO3, 8 * SIZE
	STFD	y06, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y07, 0 * SIZE(Y2)
	addi	AO4, AO4, 8 * SIZE
	STFD	y08, 1 * SIZE(Y2)
	add	Y2, Y2, INCY
	.align 4

LL(116):
	andi.	r0, M, 2
	ble	LL(117)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y03, 0 * SIZE(Y1)
	LFD	y04, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	LFD	a5,  0 * SIZE(AO2)
	LFD	a6,  1 * SIZE(AO2)
	LFD	a7,  2 * SIZE(AO2)
	LFD	a8,  3 * SIZE(AO2)

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	LFD	a1,  0 * SIZE(AO3)
	LFD	a2,  1 * SIZE(AO3)
	LFD	a3,  2 * SIZE(AO3)
	LFD	a4,  3 * SIZE(AO3)

	FMADD	y01, alpha2r, a5, y01
	FMADD	y02, alpha2i, a5, y02
	FMADD	y03, alpha2r, a7, y03
	FMADD	y04, alpha2i, a7, y04

	FMSUBX	y01, alpha2i, a6, y01
	FMADDX	y02, alpha2r, a6, y02
	FMSUBX	y03, alpha2i, a8, y03
	FMADDX	y04, alpha2r, a8, y04

	LFD	a5,  0 * SIZE(AO4)
	LFD	a6,  1 * SIZE(AO4)
	LFD	a7,  2 * SIZE(AO4)
	LFD	a8,  3 * SIZE(AO4)

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

	STFD	y01, 0 * SIZE(Y2)
	addi	AO1, AO1, 4 * SIZE
	STFD	y02, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y03, 0 * SIZE(Y2)
	addi	AO2, AO2, 4 * SIZE
	STFD	y04, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	addi	AO3, AO3, 4 * SIZE
	addi	AO4, AO4, 4 * SIZE
	.align 4

LL(117):
	andi.	r0, M, 1
	ble	LL(119)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  0 * SIZE(AO2)
	LFD	a4,  1 * SIZE(AO2)
	LFD	a5,  0 * SIZE(AO3)
	LFD	a6,  1 * SIZE(AO3)
	LFD	a7,  0 * SIZE(AO4)
	LFD	a8,  1 * SIZE(AO4)

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02

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

	STFD	y01, 0 * SIZE(Y2)
	STFD	y02, 1 * SIZE(Y2)
	add	Y2, Y2, INCY
	.align 4

LL(119):
	addi	J, J, -1
	cmpi	cr0, 0, J, 0
	bgt	LL(111)
	.align 4

LL(120):
	andi.	J, N, 2
	ble	LL(130)
	.align 4

LL(121):
	lfd	alpha_r, ALPHA_R
	lfd	alpha_i, ALPHA_I

	LFD	a1, 0 * SIZE(X)
	LFD	a2, 1 * SIZE(X)
	add	X, X, INCX
	LFD	a3, 0 * SIZE(X)
	LFD	a4, 1 * SIZE(X)
	add	X, X, INCX

	FMUL	alpha1r, alpha_r, a1
	FMUL	alpha1i, alpha_i, a1
	FMUL	alpha2r, alpha_r, a3
	FMUL	alpha2i, alpha_i, a3

	FMSUBR	alpha1r, alpha_i, a2, alpha1r
	FMADDR	alpha1i, alpha_r, a2, alpha1i
	FMSUBR	alpha2r, alpha_i, a4, alpha2r
	FMADDR	alpha2i, alpha_r, a4, alpha2i

	mr	AO1, A
	add	AO2, A,   LDA
	add	A,   AO2, LDA

	mr	Y1, Y
	mr	Y2, Y

	srawi.	r0,  M, 3
	mtspr	CTR, r0
	ble	LL(125)
	.align 4

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y03, 0 * SIZE(Y1)
	LFD	y04, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	LFD	a1,  0 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)

	LFD	y05, 0 * SIZE(Y1)
	LFD	y06, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y07, 0 * SIZE(Y1)
	LFD	y08, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	LFD	a2,  1 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	LFD	y09, 0 * SIZE(Y1)
	LFD	y10, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y11, 0 * SIZE(Y1)
	LFD	y12, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y13, 0 * SIZE(Y1)
	LFD	y14, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y15, 0 * SIZE(Y1)
	LFD	y16, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	bdz	LL(123)
	.align 4

LL(122):
	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMADD	y05, alpha1r, a5, y05
	FMADD	y06, alpha1i, a5, y06
	FMADD	y07, alpha1r, a7, y07
	FMADD	y08, alpha1i, a7, y08

	LFD	a1,  8 * SIZE(AO1)
	LFD	a3, 10 * SIZE(AO1)
	LFD	a5, 12 * SIZE(AO1)
	LFD	a7, 14 * SIZE(AO1)

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	FMSUBX	y05, alpha1i, a6, y05
	FMADDX	y06, alpha1r, a6, y06
	FMSUBX	y07, alpha1i, a8, y07
	FMADDX	y08, alpha1r, a8, y08

	LFD	a2,  9 * SIZE(AO1)
	LFD	a4, 11 * SIZE(AO1)
	LFD	a6, 13 * SIZE(AO1)
	LFD	a8, 15 * SIZE(AO1)

	addi	AO1, AO1, 16 * SIZE
	nop
	DCBT(AO1, PREA)
	nop

	FMADD	y09, alpha1r, a1, y09
	FMADD	y10, alpha1i, a1, y10
	FMADD	y11, alpha1r, a3, y11
	FMADD	y12, alpha1i, a3, y12

	FMADD	y13, alpha1r, a5, y13
	FMADD	y14, alpha1i, a5, y14
	FMADD	y15, alpha1r, a7, y15
	FMADD	y16, alpha1i, a7, y16

	LFD	a1,  0 * SIZE(AO2)
	LFD	a3,  2 * SIZE(AO2)
	LFD	a5,  4 * SIZE(AO2)
	LFD	a7,  6 * SIZE(AO2)

	FMSUBX	y09, alpha1i, a2, y09
	FMADDX	y10, alpha1r, a2, y10
	FMSUBX	y11, alpha1i, a4, y11
	FMADDX	y12, alpha1r, a4, y12

	FMSUBX	y13, alpha1i, a6, y13
	FMADDX	y14, alpha1r, a6, y14
	FMSUBX	y15, alpha1i, a8, y15
	FMADDX	y16, alpha1r, a8, y16

	LFD	a2,  1 * SIZE(AO2)
	LFD	a4,  3 * SIZE(AO2)
	LFD	a6,  5 * SIZE(AO2)
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2r, a1, y01
	FMADD	y02, alpha2i, a1, y02
	FMADD	y03, alpha2r, a3, y03
	FMADD	y04, alpha2i, a3, y04

	FMADD	y05, alpha2r, a5, y05
	FMADD	y06, alpha2i, a5, y06
	FMADD	y07, alpha2r, a7, y07
	FMADD	y08, alpha2i, a7, y08

	LFD	a1,  8 * SIZE(AO2)
	LFD	a3, 10 * SIZE(AO2)
	LFD	a5, 12 * SIZE(AO2)
	LFD	a7, 14 * SIZE(AO2)

	FMSUBX	y01, alpha2i, a2, y01
	FMADDX	y02, alpha2r, a2, y02
	FMSUBX	y03, alpha2i, a4, y03
	FMADDX	y04, alpha2r, a4, y04

	STFD	y01, 0 * SIZE(Y2)
	nop
	STFD	y02, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y01, 0 * SIZE(Y1)
	nop
	LFD	y02, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	STFD	y03, 0 * SIZE(Y2)
	nop
	STFD	y04, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y03, 0 * SIZE(Y1)
	nop
	LFD	y04, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	FMSUBX	y05, alpha2i, a6, y05
	FMADDX	y06, alpha2r, a6, y06
	FMSUBX	y07, alpha2i, a8, y07
	FMADDX	y08, alpha2r, a8, y08

	LFD	a2,  9 * SIZE(AO2)
	LFD	a4, 11 * SIZE(AO2)
	LFD	a6, 13 * SIZE(AO2)
	LFD	a8, 15 * SIZE(AO2)

	addi	AO2, AO2, 16 * SIZE
	nop
	DCBT(AO2, PREA)
	nop

	STFD	y05, 0 * SIZE(Y2)
	nop
	STFD	y06, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y05, 0 * SIZE(Y1)
	nop
	LFD	y06, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	STFD	y07, 0 * SIZE(Y2)
	nop
	STFD	y08, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y07, 0 * SIZE(Y1)
	nop
	LFD	y08, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	FMADD	y09, alpha2r, a1, y09
	FMADD	y10, alpha2i, a1, y10
	FMADD	y11, alpha2r, a3, y11
	FMADD	y12, alpha2i, a3, y12

	FMADD	y13, alpha2r, a5, y13
	FMADD	y14, alpha2i, a5, y14
	FMADD	y15, alpha2r, a7, y15
	FMADD	y16, alpha2i, a7, y16

	LFD	a1,  0 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)

	FMSUBX	y09, alpha2i, a2, y09
	FMADDX	y10, alpha2r, a2, y10
	FMSUBX	y11, alpha2i, a4, y11
	FMADDX	y12, alpha2r, a4, y12

	STFD	y09, 0 * SIZE(Y2)
	nop
	STFD	y10, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y09, 0 * SIZE(Y1)
	nop
	LFD	y10, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	STFD	y11, 0 * SIZE(Y2)
	nop
	STFD	y12, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y11, 0 * SIZE(Y1)
	nop
	LFD	y12, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	FMSUBX	y13, alpha2i, a6, y13
	FMADDX	y14, alpha2r, a6, y14
	FMSUBX	y15, alpha2i, a8, y15
	FMADDX	y16, alpha2r, a8, y16

	LFD	a2,  1 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	STFD	y13, 0 * SIZE(Y2)
	nop
	STFD	y14, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y15, 0 * SIZE(Y2)
	nop
	STFD	y16, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y13, 0 * SIZE(Y1)
	nop
	LFD	y14, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	LFD	y15, 0 * SIZE(Y1)
	nop
	LFD	y16, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	DCBT(Y1, PREC)
	bdnz	LL(122)
	.align 4

LL(123):
	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMADD	y05, alpha1r, a5, y05
	FMADD	y06, alpha1i, a5, y06
	FMADD	y07, alpha1r, a7, y07
	FMADD	y08, alpha1i, a7, y08

	LFD	a1,  8 * SIZE(AO1)
	LFD	a3, 10 * SIZE(AO1)
	LFD	a5, 12 * SIZE(AO1)
	LFD	a7, 14 * SIZE(AO1)

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	FMSUBX	y05, alpha1i, a6, y05
	FMADDX	y06, alpha1r, a6, y06
	FMSUBX	y07, alpha1i, a8, y07
	FMADDX	y08, alpha1r, a8, y08

	LFD	a2,  9 * SIZE(AO1)
	LFD	a4, 11 * SIZE(AO1)
	LFD	a6, 13 * SIZE(AO1)
	LFD	a8, 15 * SIZE(AO1)

	FMADD	y09, alpha1r, a1, y09
	FMADD	y10, alpha1i, a1, y10
	FMADD	y11, alpha1r, a3, y11
	FMADD	y12, alpha1i, a3, y12

	FMADD	y13, alpha1r, a5, y13
	FMADD	y14, alpha1i, a5, y14
	FMADD	y15, alpha1r, a7, y15
	FMADD	y16, alpha1i, a7, y16

	LFD	a1,  0 * SIZE(AO2)
	LFD	a3,  2 * SIZE(AO2)
	LFD	a5,  4 * SIZE(AO2)
	LFD	a7,  6 * SIZE(AO2)

	FMSUBX	y09, alpha1i, a2, y09
	FMADDX	y10, alpha1r, a2, y10
	FMSUBX	y11, alpha1i, a4, y11
	FMADDX	y12, alpha1r, a4, y12

	FMSUBX	y13, alpha1i, a6, y13
	FMADDX	y14, alpha1r, a6, y14
	FMSUBX	y15, alpha1i, a8, y15
	FMADDX	y16, alpha1r, a8, y16

	LFD	a2,  1 * SIZE(AO2)
	LFD	a4,  3 * SIZE(AO2)
	LFD	a6,  5 * SIZE(AO2)
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2r, a1, y01
	FMADD	y02, alpha2i, a1, y02
	FMADD	y03, alpha2r, a3, y03
	FMADD	y04, alpha2i, a3, y04

	FMADD	y05, alpha2r, a5, y05
	FMADD	y06, alpha2i, a5, y06
	FMADD	y07, alpha2r, a7, y07
	FMADD	y08, alpha2i, a7, y08

	LFD	a1,  8 * SIZE(AO2)
	LFD	a3, 10 * SIZE(AO2)
	LFD	a5, 12 * SIZE(AO2)
	LFD	a7, 14 * SIZE(AO2)

	FMSUBX	y01, alpha2i, a2, y01
	FMADDX	y02, alpha2r, a2, y02
	FMSUBX	y03, alpha2i, a4, y03
	FMADDX	y04, alpha2r, a4, y04

	STFD	y01, 0 * SIZE(Y2)
	addi	AO1, AO1, 16 * SIZE
	STFD	y02, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y03, 0 * SIZE(Y2)
	nop
	STFD	y04, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	FMSUBX	y05, alpha2i, a6, y05
	FMADDX	y06, alpha2r, a6, y06
	FMSUBX	y07, alpha2i, a8, y07
	FMADDX	y08, alpha2r, a8, y08

	LFD	a2,  9 * SIZE(AO2)
	LFD	a4, 11 * SIZE(AO2)
	LFD	a6, 13 * SIZE(AO2)
	LFD	a8, 15 * SIZE(AO2)

	STFD	y05, 0 * SIZE(Y2)
	addi	AO2, AO2, 16 * SIZE
	STFD	y06, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y07, 0 * SIZE(Y2)
	nop
	STFD	y08, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

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

	STFD	y09, 0 * SIZE(Y2)
	nop
	STFD	y10, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y11, 0 * SIZE(Y2)
	nop
	STFD	y12, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	FMSUBX	y13, alpha2i, a6, y13
	FMADDX	y14, alpha2r, a6, y14
	FMSUBX	y15, alpha2i, a8, y15
	FMADDX	y16, alpha2r, a8, y16

	STFD	y13, 0 * SIZE(Y2)
	nop
	STFD	y14, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y15, 0 * SIZE(Y2)
	nop
	STFD	y16, 1 * SIZE(Y2)
	add	Y2, Y2, INCY
	.align 4

LL(125):
	andi.	r0, M, 7
	ble	LL(130)
	andi.	r0, M, 4
	ble	LL(126)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y03, 0 * SIZE(Y1)
	LFD	y04, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	LFD	a1,  0 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)

	LFD	y05, 0 * SIZE(Y1)
	LFD	y06, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y07, 0 * SIZE(Y1)
	LFD	y08, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	LFD	a2,  1 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMADD	y05, alpha1r, a5, y05
	FMADD	y06, alpha1i, a5, y06
	FMADD	y07, alpha1r, a7, y07
	FMADD	y08, alpha1i, a7, y08

	LFD	a1,  0 * SIZE(AO2)
	LFD	a3,  2 * SIZE(AO2)
	LFD	a5,  4 * SIZE(AO2)
	LFD	a7,  6 * SIZE(AO2)

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	FMSUBX	y05, alpha1i, a6, y05
	FMADDX	y06, alpha1r, a6, y06
	FMSUBX	y07, alpha1i, a8, y07
	FMADDX	y08, alpha1r, a8, y08

	LFD	a2,  1 * SIZE(AO2)
	LFD	a4,  3 * SIZE(AO2)
	LFD	a6,  5 * SIZE(AO2)
	LFD	a8,  7 * SIZE(AO2)

	FMADD	y01, alpha2r, a1, y01
	FMADD	y02, alpha2i, a1, y02
	FMADD	y03, alpha2r, a3, y03
	FMADD	y04, alpha2i, a3, y04

	FMADD	y05, alpha2r, a5, y05
	FMADD	y06, alpha2i, a5, y06
	FMADD	y07, alpha2r, a7, y07
	FMADD	y08, alpha2i, a7, y08

	FMSUBX	y01, alpha2i, a2, y01
	FMADDX	y02, alpha2r, a2, y02
	FMSUBX	y03, alpha2i, a4, y03
	FMADDX	y04, alpha2r, a4, y04

	STFD	y01, 0 * SIZE(Y2)
	addi	AO1, AO1, 8 * SIZE
	STFD	y02, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y03, 0 * SIZE(Y2)
	addi	AO2, AO2, 8 * SIZE
	STFD	y04, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	FMSUBX	y05, alpha2i, a6, y05
	FMADDX	y06, alpha2r, a6, y06
	FMSUBX	y07, alpha2i, a8, y07
	FMADDX	y08, alpha2r, a8, y08

	STFD	y05, 0 * SIZE(Y2)
	nop
	STFD	y06, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y07, 0 * SIZE(Y2)
	nop
	STFD	y08, 1 * SIZE(Y2)
	add	Y2, Y2, INCY
	.align 4

LL(126):
	andi.	r0, M, 2
	ble	LL(127)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y03, 0 * SIZE(Y1)
	LFD	y04, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	LFD	a5,  0 * SIZE(AO2)
	LFD	a6,  1 * SIZE(AO2)
	LFD	a7,  2 * SIZE(AO2)
	LFD	a8,  3 * SIZE(AO2)

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

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

	STFD	y01, 0 * SIZE(Y2)
	addi	AO1, AO1, 4 * SIZE
	STFD	y02, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y03, 0 * SIZE(Y2)
	addi	AO2, AO2, 4 * SIZE
	STFD	y04, 1 * SIZE(Y2)
	add	Y2, Y2, INCY
	.align 4

LL(127):
	andi.	r0, M, 1
	ble	LL(130)

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  0 * SIZE(AO2)
	LFD	a4,  1 * SIZE(AO2)

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02

	FMADD	y01, alpha2r, a3, y01
	FMADD	y02, alpha2i, a3, y02
	FMSUBX	y01, alpha2i, a4, y01
	FMADDX	y02, alpha2r, a4, y02

	STFD	y01, 0 * SIZE(Y2)
	STFD	y02, 1 * SIZE(Y2)
	add	Y2, Y2, INCY
	.align 4

LL(130):
	andi.	J, N, 1
	ble	LL(999)
	.align 4

LL(131):
	lfd	alpha_r, ALPHA_R
	lfd	alpha_i, ALPHA_I

	LFD	a1, 0 * SIZE(X)
	LFD	a2, 1 * SIZE(X)
	add	X, X, INCX

	FMUL	alpha1r, alpha_r, a1
	FMUL	alpha1i, alpha_i, a1

	FMSUBR	alpha1r, alpha_i, a2, alpha1r
	FMADDR	alpha1i, alpha_r, a2, alpha1i

	mr	AO1, A
	add	A,   AO1, LDA

	mr	Y1, Y
	mr	Y2, Y

	srawi.	r0,  M, 3
	mtspr	CTR, r0
	ble	LL(135)
	.align 4

	LFD	y01, 0 * SIZE(Y1)
	LFD	y02, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y03, 0 * SIZE(Y1)
	LFD	y04, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	LFD	a1,  0 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)

	LFD	y05, 0 * SIZE(Y1)
	LFD	y06, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y07, 0 * SIZE(Y1)
	LFD	y08, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	LFD	a2,  1 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	LFD	y09, 0 * SIZE(Y1)
	LFD	y10, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y11, 0 * SIZE(Y1)
	LFD	y12, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y13, 0 * SIZE(Y1)
	LFD	y14, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y15, 0 * SIZE(Y1)
	LFD	y16, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	bdz	LL(133)
	.align 4

LL(132):
	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMADD	y05, alpha1r, a5, y05
	FMADD	y06, alpha1i, a5, y06
	FMADD	y07, alpha1r, a7, y07
	FMADD	y08, alpha1i, a7, y08

	LFD	a1,  8 * SIZE(AO1)
	LFD	a3, 10 * SIZE(AO1)
	LFD	a5, 12 * SIZE(AO1)
	LFD	a7, 14 * SIZE(AO1)

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	STFD	y01, 0 * SIZE(Y2)
	nop
	STFD	y02, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y01, 0 * SIZE(Y1)
	nop
	LFD	y02, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	STFD	y03, 0 * SIZE(Y2)
	nop
	STFD	y04, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y03, 0 * SIZE(Y1)
	nop
	LFD	y04, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	FMSUBX	y05, alpha1i, a6, y05
	FMADDX	y06, alpha1r, a6, y06
	FMSUBX	y07, alpha1i, a8, y07
	FMADDX	y08, alpha1r, a8, y08

	LFD	a2,  9 * SIZE(AO1)
	LFD	a4, 11 * SIZE(AO1)
	LFD	a6, 13 * SIZE(AO1)
	LFD	a8, 15 * SIZE(AO1)

	addi	AO1, AO1, 16 * SIZE
	nop
	DCBT(AO1, PREA)
	nop

	STFD	y05, 0 * SIZE(Y2)
	nop
	STFD	y06, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y05, 0 * SIZE(Y1)
	nop
	LFD	y06, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	STFD	y07, 0 * SIZE(Y2)
	nop
	STFD	y08, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y07, 0 * SIZE(Y1)
	nop
	LFD	y08, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	FMADD	y09, alpha1r, a1, y09
	FMADD	y10, alpha1i, a1, y10
	FMADD	y11, alpha1r, a3, y11
	FMADD	y12, alpha1i, a3, y12

	FMADD	y13, alpha1r, a5, y13
	FMADD	y14, alpha1i, a5, y14
	FMADD	y15, alpha1r, a7, y15
	FMADD	y16, alpha1i, a7, y16

	LFD	a1,  0 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
	LFD	a7,  6 * SIZE(AO1)

	FMSUBX	y09, alpha1i, a2, y09
	FMADDX	y10, alpha1r, a2, y10
	FMSUBX	y11, alpha1i, a4, y11
	FMADDX	y12, alpha1r, a4, y12

	STFD	y09, 0 * SIZE(Y2)
	nop
	STFD	y10, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y09, 0 * SIZE(Y1)
	nop
	LFD	y10, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	STFD	y11, 0 * SIZE(Y2)
	nop
	STFD	y12, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y11, 0 * SIZE(Y1)
	nop
	LFD	y12, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	FMSUBX	y13, alpha1i, a6, y13
	FMADDX	y14, alpha1r, a6, y14
	FMSUBX	y15, alpha1i, a8, y15
	FMADDX	y16, alpha1r, a8, y16

	LFD	a2,  1 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	STFD	y13, 0 * SIZE(Y2)
	nop
	STFD	y14, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y15, 0 * SIZE(Y2)
	nop
	STFD	y16, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	LFD	y13, 0 * SIZE(Y1)
	nop
	LFD	y14, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	LFD	y15, 0 * SIZE(Y1)
	nop
	LFD	y16, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	DCBT(Y1, PREC)
	bdnz	LL(132)
	.align 4

LL(133):
	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMADD	y05, alpha1r, a5, y05
	FMADD	y06, alpha1i, a5, y06
	FMADD	y07, alpha1r, a7, y07
	FMADD	y08, alpha1i, a7, y08

	LFD	a1,  8 * SIZE(AO1)
 	LFD	a3, 10 * SIZE(AO1)
	LFD	a5, 12 * SIZE(AO1)
	LFD	a7, 14 * SIZE(AO1)

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	FMSUBX	y05, alpha1i, a6, y05
	FMADDX	y06, alpha1r, a6, y06
	FMSUBX	y07, alpha1i, a8, y07
	FMADDX	y08, alpha1r, a8, y08

	LFD	a2,  9 * SIZE(AO1)
	LFD	a4, 11 * SIZE(AO1)
	LFD	a6, 13 * SIZE(AO1)
	LFD	a8, 15 * SIZE(AO1)

	FMADD	y09, alpha1r, a1, y09
	FMADD	y10, alpha1i, a1, y10
	FMADD	y11, alpha1r, a3, y11
	FMADD	y12, alpha1i, a3, y12

	FMADD	y13, alpha1r, a5, y13
	FMADD	y14, alpha1i, a5, y14
	FMADD	y15, alpha1r, a7, y15
	FMADD	y16, alpha1i, a7, y16

	FMSUBX	y09, alpha1i, a2, y09
	FMADDX	y10, alpha1r, a2, y10
	FMSUBX	y11, alpha1i, a4, y11
	FMADDX	y12, alpha1r, a4, y12

	FMSUBX	y13, alpha1i, a6, y13
	FMADDX	y14, alpha1r, a6, y14
	FMSUBX	y15, alpha1i, a8, y15
	FMADDX	y16, alpha1r, a8, y16

	STFD	y01, 0 * SIZE(Y2)
	addi	AO1, AO1, 16 * SIZE
	STFD	y02, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y03, 0 * SIZE(Y2)
	nop
	STFD	y04, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y05, 0 * SIZE(Y2)
	nop
	STFD	y06, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y07, 0 * SIZE(Y2)
	nop
	STFD	y08, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y09, 0 * SIZE(Y2)
	nop
	STFD	y10, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y11, 0 * SIZE(Y2)
	nop
	STFD	y12, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y13, 0 * SIZE(Y2)
	nop
	STFD	y14, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y15, 0 * SIZE(Y2)
	nop
	STFD	y16, 1 * SIZE(Y2)
	add	Y2, Y2, INCY
	.align 4

LL(135):
	andi.	r0, M, 7
	ble	LL(999)
	andi.	r0, M, 4
	ble	LL(136)

	LFD	y01, 0 * SIZE(Y1)
	nop
	LFD	y02, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	LFD	y03, 0 * SIZE(Y1)
	nop
	LFD	y04, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	LFD	y05, 0 * SIZE(Y1)
	nop
	LFD	y06, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	LFD	y07, 0 * SIZE(Y1)
	nop
	LFD	y08, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	LFD	a1,  0 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a5,  4 * SIZE(AO1)
 	LFD	a7,  6 * SIZE(AO1)

	LFD	a2,  1 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)
	LFD	a6,  5 * SIZE(AO1)
	LFD	a8,  7 * SIZE(AO1)

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMADD	y05, alpha1r, a5, y05
	FMADD	y06, alpha1i, a5, y06
	FMADD	y07, alpha1r, a7, y07
	FMADD	y08, alpha1i, a7, y08

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	FMSUBX	y05, alpha1i, a6, y05
	FMADDX	y06, alpha1r, a6, y06
	FMSUBX	y07, alpha1i, a8, y07
	FMADDX	y08, alpha1r, a8, y08

	STFD	y01, 0 * SIZE(Y2)
	addi	AO1, AO1, 8 * SIZE
	STFD	y02, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y03, 0 * SIZE(Y2)
	nop
	STFD	y04, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y05, 0 * SIZE(Y2)
	nop
	STFD	y06, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y07, 0 * SIZE(Y2)
	nop
	STFD	y08, 1 * SIZE(Y2)
	add	Y2, Y2, INCY
	.align 4

LL(136):
	andi.	r0, M, 2
	ble	LL(137)

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)
	LFD	a3,  2 * SIZE(AO1)
	LFD	a4,  3 * SIZE(AO1)

	LFD	y01, 0 * SIZE(Y1)
	nop
	LFD	y02, 1 * SIZE(Y1)
	add	Y1, Y1, INCY
	LFD	y03, 0 * SIZE(Y1)
	nop
	LFD	y04, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMADD	y03, alpha1r, a3, y03
	FMADD	y04, alpha1i, a3, y04

	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02
	FMSUBX	y03, alpha1i, a4, y03
	FMADDX	y04, alpha1r, a4, y04

	STFD	y01, 0 * SIZE(Y2)
	addi	AO1, AO1, 4 * SIZE
	STFD	y02, 1 * SIZE(Y2)
	add	Y2, Y2, INCY

	STFD	y03, 0 * SIZE(Y2)
	nop
	STFD	y04, 1 * SIZE(Y2)
	add	Y2, Y2, INCY
	.align 4

LL(137):
	andi.	r0, M, 1
	ble	LL(999)

	LFD	y01, 0 * SIZE(Y1)
	nop
	LFD	y02, 1 * SIZE(Y1)
	add	Y1, Y1, INCY

	LFD	a1,  0 * SIZE(AO1)
	LFD	a2,  1 * SIZE(AO1)

	FMADD	y01, alpha1r, a1, y01
	FMADD	y02, alpha1i, a1, y02
	FMSUBX	y01, alpha1i, a2, y01
	FMADDX	y02, alpha1r, a2, y02

	STFD	y01, 0 * SIZE(Y2)
	nop
	STFD	y02, 1 * SIZE(Y2)
	add	Y2, Y2, INCY
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
