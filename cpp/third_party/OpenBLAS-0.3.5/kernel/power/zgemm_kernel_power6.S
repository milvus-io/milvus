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

#ifndef __64BIT__
#define LOAD	lwz
#else
#define LOAD	ld
#endif

#ifdef __64BIT__
#define STACKSIZE 320
#define ALPHA_R 296(SP)
#define ALPHA_I 304(SP)
#define FZERO	312(SP)
#else
#define STACKSIZE 256
#define ALPHA_R 224(SP)
#define ALPHA_I 232(SP)
#define FZERO	240(SP)
#endif

#define	M	r3
#define	N	r4
#define	K	r5

#ifdef linux
#ifndef __64BIT__
#define A	r6
#define	B	r7
#define	C	r8
#define	LDC	r9
#define OFFSET	r10
#else
#define A	r8
#define	B	r9
#define	C	r10
#define	LDC	r6
#define OFFSET	r7
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
#define A	r10
#define	B	r6
#define	C	r7
#define	LDC	r8
#define OFFSET	r9
#else
#define A	r8
#define	B	r9
#define	C	r10
#define	LDC	r6
#define OFFSET	r7
#endif
#endif

#define TEMP	r19
#define KK	r20
#define	BB	r21
#define	I	r22
#define J	r23
#define AO	r24
#define	BO	r25
#define	CO1	r26
#define CO2	r27
#define	CO3	r28
#define CO4	r29

#define PREA	r30
#define PREC	r31

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define FMA1	FMADD
#define FMA2	FMADD
#define FMA3	FNMSUB
#define FMA4	FMADD
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define FMA1	FMADD
#define FMA2	FNMSUB
#define FMA3	FMADD
#define FMA4	FMADD
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define FMA1	FMADD
#define FMA2	FMADD
#define FMA3	FMADD
#define FMA4	FNMSUB
#else
#define FMA1	FMADD
#define FMA2	FNMSUB
#define FMA3	FNMSUB
#define FMA4	FNMSUB
#endif

#ifndef NEEDPARAM

	PROLOGUE
	PROFCODE

	addi	SP, SP, -STACKSIZE
	li	r0, 0

	stfd	f14,    0(SP)
	stfd	f15,    8(SP)
	stfd	f16,   16(SP)
	stfd	f17,   24(SP)

	stfd	f18,   32(SP)
	stfd	f19,   40(SP)
	stfd	f20,   48(SP)
	stfd	f21,   56(SP)

	stfd	f22,   64(SP)
	stfd	f23,   72(SP)
	stfd	f24,   80(SP)
	stfd	f25,   88(SP)

	stfd	f26,   96(SP)
	stfd	f27,  104(SP)
	stfd	f28,  112(SP)
	stfd	f29,  120(SP)

	stfd	f30,  128(SP)
	stfd	f31,  136(SP)

#ifdef __64BIT__
	std	r31,  144(SP)
	std	r30,  152(SP)
	std	r29,  160(SP)
	std	r28,  168(SP)
	std	r27,  176(SP)
	std	r26,  184(SP)
	std	r25,  192(SP)
	std	r24,  200(SP)
	std	r23,  208(SP)
	std	r22,  216(SP)
	std	r21,  224(SP)
#ifdef TRMMKERNEL
	std	r20,  232(SP)
	std	r19,  240(SP)
#endif
#else
	stw	r31,  144(SP)
	stw	r30,  148(SP)
	stw	r29,  152(SP)
	stw	r28,  156(SP)
	stw	r27,  160(SP)
	stw	r26,  164(SP)
	stw	r25,  168(SP)
	stw	r24,  172(SP)
	stw	r23,  176(SP)
	stw	r22,  180(SP)
	stw	r21,  184(SP)
#ifdef TRMMKERNEL
	stw	r20,  188(SP)
	stw	r19,  192(SP)
#endif
#endif

	stfd	f1,  ALPHA_R
	stfd	f2,  ALPHA_I
	stw	r0,  FZERO

#ifdef linux
#ifdef __64BIT__
	ld	LDC, FRAMESLOT(0) + STACKSIZE(SP)
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#ifdef __64BIT__
	ld	LDC, FRAMESLOT(0) + STACKSIZE(SP)
#else
#ifdef DOUBLE
	lwz	B,   FRAMESLOT(0) + STACKSIZE(SP)
	lwz	C,   FRAMESLOT(1) + STACKSIZE(SP)
	lwz	LDC, FRAMESLOT(2) + STACKSIZE(SP)
#else
	lwz	LDC, FRAMESLOT(0) + STACKSIZE(SP)
#endif
#endif
#endif

#ifdef TRMMKERNEL
#if defined(linux) && defined(__64BIT__)
	ld	OFFSET,  FRAMESLOT(1) + STACKSIZE(SP)
#endif

#if defined(_AIX) || defined(__APPLE__)
#ifdef __64BIT__
	ld	OFFSET,  FRAMESLOT(1) + STACKSIZE(SP)
#else
#ifdef DOUBLE
	lwz	OFFSET,  FRAMESLOT(3) + STACKSIZE(SP)
#else
	lwz	OFFSET,  FRAMESLOT(1) + STACKSIZE(SP)
#endif
#endif
#endif
#if defined(TRMMKERNEL) && !defined(LEFT)
	neg	KK, OFFSET
#endif
#endif

	slwi	LDC, LDC, ZBASE_SHIFT
	li	PREA,  (16 * 3) * SIZE
	li	PREC,   3 * SIZE

	cmpwi	cr0, M, 0
	ble	LL(999)
	cmpwi	cr0, N, 0
	ble	LL(999)
	cmpwi	cr0, K, 0
	ble	LL(999)

	srawi.	J, N,  2
	ble	LL(30)
	.align 4

LL(10):
	mr	CO1, C
	add	CO2, C,   LDC
	add	CO3, CO2, LDC
	add	CO4, CO3, LDC
	add	C,   CO4, LDC

#if defined(TRMMKERNEL) && defined(LEFT)
	mr	KK, OFFSET
#endif

	slwi	BB, K, ZBASE_SHIFT + 2
	mr	AO, A

	lfs	f0, FZERO
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0
	fmr	f4,  f0
	fmr	f5,  f0
	fmr	f6,  f0
	fmr	f7,  f0
	fmr	f8,  f0
	fmr	f9,  f0
	fmr	f10, f0
	fmr	f11, f0
	fmr	f12, f0
	fmr	f13, f0
	fmr	f14, f0
	fmr	f15, f0

	srawi.	I, M,  1
	ble	LL(20)
	.align 4

LL(11):
#ifndef TRMMKERNEL
	LFD	f16,  0 * SIZE(AO)
	LFD	f20,  0 * SIZE(B)
	LFD	f17,  1 * SIZE(AO)
	LFD	f21,  1 * SIZE(B)
	LFD	f18,  2 * SIZE(AO)
	LFD	f22,  2 * SIZE(B)
	LFD	f19,  3 * SIZE(AO)
	LFD	f23,  3 * SIZE(B)
	LFD	f24,  4 * SIZE(B)
	LFD	f25,  5 * SIZE(B)
	LFD	f26,  6 * SIZE(B)
	LFD	f27,  7 * SIZE(B)

	dcbtst	CO1, PREC
	dcbtst	CO2, PREC
	dcbtst	CO3, PREC
	dcbtst	CO4, PREC

	srawi.	r0,  K,  3
	mr	BO,  B
	mtspr	CTR, r0
	ble	LL(15)
#else
#if	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	LFD	f16,  0 * SIZE(AO)
	LFD	f20,  0 * SIZE(B)
	LFD	f17,  1 * SIZE(AO)
	LFD	f21,  1 * SIZE(B)
	LFD	f18,  2 * SIZE(AO)
	LFD	f22,  2 * SIZE(B)
	LFD	f19,  3 * SIZE(AO)
	LFD	f23,  3 * SIZE(B)
	LFD	f24,  4 * SIZE(B)
	LFD	f25,  5 * SIZE(B)
	LFD	f26,  6 * SIZE(B)
	LFD	f27,  7 * SIZE(B)

	mr	BO,  B
#else
	slwi	r0,   KK, 1 + ZBASE_SHIFT
	slwi	TEMP, KK, 2 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, B,  TEMP

	LFD	f16,  0 * SIZE(AO)
	LFD	f20,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f21,  1 * SIZE(BO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f22,  2 * SIZE(BO)
	LFD	f19,  3 * SIZE(AO)
	LFD	f23,  3 * SIZE(BO)
	LFD	f24,  4 * SIZE(BO)
	LFD	f25,  5 * SIZE(BO)
	LFD	f26,  6 * SIZE(BO)
	LFD	f27,  7 * SIZE(BO)
#endif

	dcbtst	CO1, PREC
	dcbtst	CO2, PREC
	dcbtst	CO3, PREC
	dcbtst	CO4, PREC

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 4
#endif
	srawi.	TEMP,  TEMP,  3
	mtspr	CTR, TEMP
	ble	LL(15)
#endif
	.align 4

LL(12):
	dcbt	AO, PREA
	FMA1	f0,  f16, f20, f0
	nop
	FMA1	f2,  f18, f20, f2

	FMA2	f1,  f16, f21, f1
	FMA2	f3,  f18, f21, f3
	LFD	f28,  4 * SIZE(AO)
	LFD	f29,  5 * SIZE(AO)

	LFD	f30,  6 * SIZE(AO)
	LFD	f31,  7 * SIZE(AO)
	FMA1	f4,  f16, f22, f4
	FMA1	f6,  f18, f22, f6

	FMA2	f5,  f16, f23, f5
	FMA2	f7,  f18, f23, f7

	FMA1	f8,  f16, f24, f8
	FMA1	f10, f18, f24, f10
	FMA2	f9,  f16, f25, f9
	FMA2	f11, f18, f25, f11

	FMA1	f12, f16, f26, f12
	FMA1	f14, f18, f26, f14
	FMA2	f13, f16, f27, f13
	FMA2	f15, f18, f27, f15

	FMA4	f1,  f17, f20, f1
	FMA4	f3,  f19, f20, f3
	FMA3	f0,  f17, f21, f0
	FMA3	f2,  f19, f21, f2

	FMA4	f5,  f17, f22, f5
	FMA4	f7,  f19, f22, f7
	FMA3	f4,  f17, f23, f4
	FMA3	f6,  f19, f23, f6

	LFD	f20,  8 * SIZE(BO)
	LFD	f21,  9 * SIZE(BO)
	LFD	f22, 10 * SIZE(BO)
	LFD	f23, 11 * SIZE(BO)

	FMA4	f9,  f17, f24, f9
	FMA4	f11, f19, f24, f11
	FMA3	f8,  f17, f25, f8
	FMA3	f10, f19, f25, f10

	FMA4	f13, f17, f26, f13
	FMA4	f15, f19, f26, f15
	FMA3	f12, f17, f27, f12
	FMA3	f14, f19, f27, f14

	LFD	f24, 12 * SIZE(BO)
	LFD	f25, 13 * SIZE(BO)
	LFD	f26, 14 * SIZE(BO)
	LFD	f27, 15 * SIZE(BO)

	FMA1	f0,  f28, f20, f0
	FMA1	f2,  f30, f20, f2
	FMA2	f1,  f28, f21, f1
	FMA2	f3,  f30, f21, f3

	LFD	f16,  8 * SIZE(AO)
	LFD	f17,  9 * SIZE(AO)
	LFD	f18, 10 * SIZE(AO)
	LFD	f19, 11 * SIZE(AO)

	FMA1	f4,  f28, f22, f4
	FMA1	f6,  f30, f22, f6
	FMA2	f5,  f28, f23, f5
	FMA2	f7,  f30, f23, f7

	FMA1	f8,  f28, f24, f8
	FMA1	f10, f30, f24, f10
	FMA2	f9,  f28, f25, f9
	FMA2	f11, f30, f25, f11

	FMA1	f12, f28, f26, f12
	FMA1	f14, f30, f26, f14
	FMA2	f13, f28, f27, f13
	FMA2	f15, f30, f27, f15

	FMA4	f1,  f29, f20, f1
	FMA4	f3,  f31, f20, f3
	FMA3	f0,  f29, f21, f0
	FMA3	f2,  f31, f21, f2

	FMA4	f5,  f29, f22, f5
	FMA4	f7,  f31, f22, f7
	FMA3	f4,  f29, f23, f4
	FMA3	f6,  f31, f23, f6

	LFD	f20, 16 * SIZE(BO)
	LFD	f21, 17 * SIZE(BO)
	LFD	f22, 18 * SIZE(BO)
	LFD	f23, 19 * SIZE(BO)

	FMA4	f9,  f29, f24, f9
	FMA4	f11, f31, f24, f11
	FMA3	f8,  f29, f25, f8
	FMA3	f10, f31, f25, f10

	FMA4	f13, f29, f26, f13
	FMA4	f15, f31, f26, f15
	FMA3	f12, f29, f27, f12
	FMA3	f14, f31, f27, f14

	LFD	f24, 20 * SIZE(BO)
	LFD	f25, 21 * SIZE(BO)
	LFD	f26, 22 * SIZE(BO)
	LFD	f27, 23 * SIZE(BO)

	FMA1	f0,  f16, f20, f0
	FMA1	f2,  f18, f20, f2
	FMA2	f1,  f16, f21, f1
	FMA2	f3,  f18, f21, f3

	LFD	f28, 12 * SIZE(AO)
	LFD	f29, 13 * SIZE(AO)
	LFD	f30, 14 * SIZE(AO)
	LFD	f31, 15 * SIZE(AO)

	FMA1	f4,  f16, f22, f4
	FMA1	f6,  f18, f22, f6
	FMA2	f5,  f16, f23, f5
	FMA2	f7,  f18, f23, f7

	FMA1	f8,  f16, f24, f8
	FMA1	f10, f18, f24, f10
	FMA2	f9,  f16, f25, f9
	FMA2	f11, f18, f25, f11

	FMA1	f12, f16, f26, f12
	FMA1	f14, f18, f26, f14
	FMA2	f13, f16, f27, f13
	FMA2	f15, f18, f27, f15

	FMA4	f1,  f17, f20, f1
	FMA4	f3,  f19, f20, f3
	FMA3	f0,  f17, f21, f0
	FMA3	f2,  f19, f21, f2

	FMA4	f5,  f17, f22, f5
	FMA4	f7,  f19, f22, f7
	FMA3	f4,  f17, f23, f4
	FMA3	f6,  f19, f23, f6

	LFD	f20, 24 * SIZE(BO)
	LFD	f21, 25 * SIZE(BO)
	LFD	f22, 26 * SIZE(BO)
	LFD	f23, 27 * SIZE(BO)

	FMA4	f9,  f17, f24, f9
	FMA4	f11, f19, f24, f11
	FMA3	f8,  f17, f25, f8
	FMA3	f10, f19, f25, f10

	FMA4	f13, f17, f26, f13
	FMA4	f15, f19, f26, f15
	FMA3	f12, f17, f27, f12
	FMA3	f14, f19, f27, f14

	LFD	f24, 28 * SIZE(BO)
	LFD	f25, 29 * SIZE(BO)
	LFD	f26, 30 * SIZE(BO)
	LFD	f27, 31 * SIZE(BO)

	FMA1	f0,  f28, f20, f0
	FMA1	f2,  f30, f20, f2
	FMA2	f1,  f28, f21, f1
	FMA2	f3,  f30, f21, f3

	LFD	f16, 16 * SIZE(AO)
	LFD	f17, 17 * SIZE(AO)
	LFD	f18, 18 * SIZE(AO)
	LFD	f19, 19 * SIZE(AO)

	FMA1	f4,  f28, f22, f4
	FMA1	f6,  f30, f22, f6
	FMA2	f5,  f28, f23, f5
	FMA2	f7,  f30, f23, f7

	FMA1	f8,  f28, f24, f8
	FMA1	f10, f30, f24, f10
	FMA2	f9,  f28, f25, f9
	FMA2	f11, f30, f25, f11

	FMA1	f12, f28, f26, f12
	FMA1	f14, f30, f26, f14
	FMA2	f13, f28, f27, f13
	FMA2	f15, f30, f27, f15

	FMA4	f1,  f29, f20, f1
	FMA4	f3,  f31, f20, f3
	FMA3	f0,  f29, f21, f0
	FMA3	f2,  f31, f21, f2

	FMA4	f5,  f29, f22, f5
	FMA4	f7,  f31, f22, f7
	FMA3	f4,  f29, f23, f4
	FMA3	f6,  f31, f23, f6

	LFD	f20, 32 * SIZE(BO)
	LFD	f21, 33 * SIZE(BO)
	LFD	f22, 34 * SIZE(BO)
	LFD	f23, 35 * SIZE(BO)

	FMA4	f9,  f29, f24, f9
	FMA4	f11, f31, f24, f11
	FMA3	f8,  f29, f25, f8
	FMA3	f10, f31, f25, f10

	FMA4	f13, f29, f26, f13
	FMA4	f15, f31, f26, f15
	FMA3	f12, f29, f27, f12
	FMA3	f14, f31, f27, f14

	LFD	f24, 36 * SIZE(BO)
	LFD	f25, 37 * SIZE(BO)
	LFD	f26, 38 * SIZE(BO)
	LFD	f27, 39 * SIZE(BO)

	FMA1	f0,  f16, f20, f0
	FMA1	f2,  f18, f20, f2
	FMA2	f1,  f16, f21, f1
	FMA2	f3,  f18, f21, f3

	LFD	f28, 20 * SIZE(AO)
	LFD	f29, 21 * SIZE(AO)
	LFD	f30, 22 * SIZE(AO)
	LFD	f31, 23 * SIZE(AO)

	FMA1	f4,  f16, f22, f4
	FMA1	f6,  f18, f22, f6
	FMA2	f5,  f16, f23, f5
	FMA2	f7,  f18, f23, f7

	FMA1	f8,  f16, f24, f8
	FMA1	f10, f18, f24, f10
	FMA2	f9,  f16, f25, f9
	FMA2	f11, f18, f25, f11

	FMA1	f12, f16, f26, f12
	FMA1	f14, f18, f26, f14
	FMA2	f13, f16, f27, f13
	FMA2	f15, f18, f27, f15

	FMA4	f1,  f17, f20, f1
	FMA4	f3,  f19, f20, f3
	FMA3	f0,  f17, f21, f0
	FMA3	f2,  f19, f21, f2

	FMA4	f5,  f17, f22, f5
	FMA4	f7,  f19, f22, f7
	FMA3	f4,  f17, f23, f4
	FMA3	f6,  f19, f23, f6

	LFD	f20, 40 * SIZE(BO)
	LFD	f21, 41 * SIZE(BO)
	LFD	f22, 42 * SIZE(BO)
	LFD	f23, 43 * SIZE(BO)

	FMA4	f9,  f17, f24, f9
	FMA4	f11, f19, f24, f11
	FMA3	f8,  f17, f25, f8
	FMA3	f10, f19, f25, f10

	FMA4	f13, f17, f26, f13
	FMA4	f15, f19, f26, f15
	FMA3	f12, f17, f27, f12
	FMA3	f14, f19, f27, f14

	LFD	f24, 44 * SIZE(BO)
	LFD	f25, 45 * SIZE(BO)
	LFD	f26, 46 * SIZE(BO)
	LFD	f27, 47 * SIZE(BO)

	FMA1	f0,  f28, f20, f0
	FMA1	f2,  f30, f20, f2
	FMA2	f1,  f28, f21, f1
	FMA2	f3,  f30, f21, f3

	LFD	f16, 24 * SIZE(AO)
	LFD	f17, 25 * SIZE(AO)
	LFD	f18, 26 * SIZE(AO)
	LFD	f19, 27 * SIZE(AO)

	FMA1	f4,  f28, f22, f4
	FMA1	f6,  f30, f22, f6
	FMA2	f5,  f28, f23, f5
	FMA2	f7,  f30, f23, f7

	FMA1	f8,  f28, f24, f8
	FMA1	f10, f30, f24, f10
	FMA2	f9,  f28, f25, f9
	FMA2	f11, f30, f25, f11

	FMA1	f12, f28, f26, f12
	FMA1	f14, f30, f26, f14
	FMA2	f13, f28, f27, f13
	FMA2	f15, f30, f27, f15

	FMA4	f1,  f29, f20, f1
	FMA4	f3,  f31, f20, f3
	FMA3	f0,  f29, f21, f0
	FMA3	f2,  f31, f21, f2

	FMA4	f5,  f29, f22, f5
	FMA4	f7,  f31, f22, f7
	FMA3	f4,  f29, f23, f4
	FMA3	f6,  f31, f23, f6

	LFD	f20, 48 * SIZE(BO)
	LFD	f21, 49 * SIZE(BO)
	LFD	f22, 50 * SIZE(BO)
	LFD	f23, 51 * SIZE(BO)

	FMA4	f9,  f29, f24, f9
	FMA4	f11, f31, f24, f11
	FMA3	f8,  f29, f25, f8
	FMA3	f10, f31, f25, f10

	FMA4	f13, f29, f26, f13
	FMA4	f15, f31, f26, f15
	FMA3	f12, f29, f27, f12
	FMA3	f14, f31, f27, f14

	LFD	f24, 52 * SIZE(BO)
	LFD	f25, 53 * SIZE(BO)
	LFD	f26, 54 * SIZE(BO)
	LFD	f27, 55 * SIZE(BO)

	FMA1	f0,  f16, f20, f0
	FMA1	f2,  f18, f20, f2
	FMA2	f1,  f16, f21, f1
	FMA2	f3,  f18, f21, f3

	LFD	f28, 28 * SIZE(AO)
	LFD	f29, 29 * SIZE(AO)
	LFD	f30, 30 * SIZE(AO)
	LFD	f31, 31 * SIZE(AO)

	FMA1	f4,  f16, f22, f4
	FMA1	f6,  f18, f22, f6
	FMA2	f5,  f16, f23, f5
	FMA2	f7,  f18, f23, f7

	FMA1	f8,  f16, f24, f8
	FMA1	f10, f18, f24, f10
	FMA2	f9,  f16, f25, f9
	FMA2	f11, f18, f25, f11

	FMA1	f12, f16, f26, f12
	FMA1	f14, f18, f26, f14
	FMA2	f13, f16, f27, f13
	FMA2	f15, f18, f27, f15

	FMA4	f1,  f17, f20, f1
	FMA4	f3,  f19, f20, f3
	FMA3	f0,  f17, f21, f0
	FMA3	f2,  f19, f21, f2

	FMA4	f5,  f17, f22, f5
	FMA4	f7,  f19, f22, f7
	FMA3	f4,  f17, f23, f4
	FMA3	f6,  f19, f23, f6

	LFD	f20, 56 * SIZE(BO)
	LFD	f21, 57 * SIZE(BO)
	LFD	f22, 58 * SIZE(BO)
	LFD	f23, 59 * SIZE(BO)

	FMA4	f9,  f17, f24, f9
	FMA4	f11, f19, f24, f11
	FMA3	f8,  f17, f25, f8
	FMA3	f10, f19, f25, f10

	FMA4	f13, f17, f26, f13
	FMA4	f15, f19, f26, f15
	FMA3	f12, f17, f27, f12
	FMA3	f14, f19, f27, f14

	LFD	f24, 60 * SIZE(BO)
	LFD	f25, 61 * SIZE(BO)
	LFD	f26, 62 * SIZE(BO)
	LFD	f27, 63 * SIZE(BO)

	FMA1	f0,  f28, f20, f0
	FMA1	f2,  f30, f20, f2
	FMA2	f1,  f28, f21, f1
	FMA2	f3,  f30, f21, f3

	LFD	f16, 32 * SIZE(AO)
	LFD	f17, 33 * SIZE(AO)
	LFD	f18, 34 * SIZE(AO)
	LFD	f19, 35 * SIZE(AO)

	FMA1	f4,  f28, f22, f4
	FMA1	f6,  f30, f22, f6
	FMA2	f5,  f28, f23, f5
	FMA2	f7,  f30, f23, f7

	FMA1	f8,  f28, f24, f8
	FMA1	f10, f30, f24, f10
	FMA2	f9,  f28, f25, f9
	FMA2	f11, f30, f25, f11

	FMA1	f12, f28, f26, f12
	FMA1	f14, f30, f26, f14
	FMA2	f13, f28, f27, f13
	FMA2	f15, f30, f27, f15

	FMA4	f1,  f29, f20, f1
	FMA4	f3,  f31, f20, f3
	FMA3	f0,  f29, f21, f0
	FMA3	f2,  f31, f21, f2

	FMA4	f5,  f29, f22, f5
	FMA4	f7,  f31, f22, f7
	FMA3	f4,  f29, f23, f4
	FMA3	f6,  f31, f23, f6

	LFD	f20, 64 * SIZE(BO)
	LFD	f21, 65 * SIZE(BO)
	LFD	f22, 66 * SIZE(BO)
	LFD	f23, 67 * SIZE(BO)

	FMA4	f9,  f29, f24, f9
	FMA4	f11, f31, f24, f11
	FMA3	f8,  f29, f25, f8
	FMA3	f10, f31, f25, f10

	FMA4	f13, f29, f26, f13
	FMA4	f15, f31, f26, f15
	FMA3	f12, f29, f27, f12
	FMA3	f14, f31, f27, f14

	LFD	f24, 68 * SIZE(BO)
	LFD	f25, 69 * SIZE(BO)
	LFD	f26, 70 * SIZE(BO)
	LFD	f27, 71 * SIZE(BO)

	addi	AO, AO, 32 * SIZE
	addi	BO, BO, 64 * SIZE
	bdnz	LL(12)
	.align 4

LL(15):
	lfd	f30,  ALPHA_R
	lfd	f31,  ALPHA_I

	dcbtst	B, BB
	addi	BB, BB, 16 * SIZE
	dcbtst	B, BB
	addi	BB, BB, 16 * SIZE

#ifndef TRMMKERNEL
	andi.	r0,  K,  7
	mtspr	CTR, r0
	ble	LL(18)
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 4
#endif
	andi.	TEMP,  TEMP,  7
	mtspr	CTR, TEMP
	ble	LL(18)
#endif
	.align 4

LL(16):
	FMA1	f0,  f16, f20, f0
	FMA1	f2,  f18, f20, f2
	FMA2	f1,  f16, f21, f1
	FMA2	f3,  f18, f21, f3

	FMA1	f4,  f16, f22, f4
	FMA1	f6,  f18, f22, f6
	FMA2	f5,  f16, f23, f5
	FMA2	f7,  f18, f23, f7

	FMA1	f8,  f16, f24, f8
	FMA1	f10, f18, f24, f10
	FMA2	f9,  f16, f25, f9
	FMA2	f11, f18, f25, f11

	FMA1	f12, f16, f26, f12
	FMA1	f14, f18, f26, f14
	FMA2	f13, f16, f27, f13
	FMA2	f15, f18, f27, f15

	FMA4	f1,  f17, f20, f1
	FMA4	f3,  f19, f20, f3
	FMA3	f0,  f17, f21, f0
	FMA3	f2,  f19, f21, f2

	FMA4	f5,  f17, f22, f5
	FMA4	f7,  f19, f22, f7
	FMA3	f4,  f17, f23, f4
	FMA3	f6,  f19, f23, f6

	FMA4	f9,  f17, f24, f9
	FMA4	f11, f19, f24, f11
	FMA3	f8,  f17, f25, f8
	FMA3	f10, f19, f25, f10

	FMA4	f13, f17, f26, f13
	FMA4	f15, f19, f26, f15
	FMA3	f12, f17, f27, f12
	FMA3	f14, f19, f27, f14

	LFD	f16,  4 * SIZE(AO)
	LFD	f17,  5 * SIZE(AO)
	LFD	f18,  6 * SIZE(AO)
	LFD	f19,  7 * SIZE(AO)

	LFD	f20,  8 * SIZE(BO)
	LFD	f21,  9 * SIZE(BO)
	LFD	f22, 10 * SIZE(BO)
	LFD	f23, 11 * SIZE(BO)
	LFD	f24, 12 * SIZE(BO)
	LFD	f25, 13 * SIZE(BO)
	LFD	f26, 14 * SIZE(BO)
	LFD	f27, 15 * SIZE(BO)

	addi	AO, AO,  4 * SIZE
	addi	BO, BO,  8 * SIZE
	bdnz	LL(16)
	.align 4

LL(18):
#ifndef TRMMKERNEL

	LFD	f16, 0 * SIZE(CO1)
	LFD	f17, 1 * SIZE(CO1)
	LFD	f18, 2 * SIZE(CO1)
	LFD	f19, 3 * SIZE(CO1)

	LFD	f20, 0 * SIZE(CO2)
	LFD	f21, 1 * SIZE(CO2)
	LFD	f22, 2 * SIZE(CO2)
	LFD	f23, 3 * SIZE(CO2)

	FNMSUB	f24, f31, f1, f16
	FMADD	f25, f31, f0, f17
	FNMSUB	f26, f31, f3, f18
	FMADD	f27, f31, f2, f19

	LFD	f16, 0 * SIZE(CO3)
	LFD	f17, 1 * SIZE(CO3)
	LFD	f18, 2 * SIZE(CO3)
	LFD	f19, 3 * SIZE(CO3)

	FMADD	f0,  f30, f0, f24
	FMADD	f1,  f30, f1, f25
	FMADD	f2,  f30, f2, f26
	FMADD	f3,  f30, f3, f27

	FNMSUB	f24, f31, f5, f20
	FMADD	f25, f31, f4, f21
	FNMSUB	f26, f31, f7, f22
	FMADD	f27, f31, f6, f23

	LFD	f20, 0 * SIZE(CO4)
	LFD	f21, 1 * SIZE(CO4)
	LFD	f22, 2 * SIZE(CO4)
	LFD	f23, 3 * SIZE(CO4)

	FMADD	f4,  f30, f4,  f24
	FMADD	f5,  f30, f5,  f25
	FMADD	f6,  f30, f6,  f26
	FMADD	f7,  f30, f7,  f27

	FNMSUB	f24, f31, f9,  f16
	FMADD	f25, f31, f8,  f17
	FNMSUB	f26, f31, f11, f18
	FMADD	f27, f31, f10, f19

	FMADD	f8,  f30, f8,  f24
	FMADD	f9,  f30, f9,  f25
	FMADD	f10, f30, f10, f26
	FMADD	f11, f30, f11, f27

	FNMSUB	f24, f31, f13, f20
	FMADD	f25, f31, f12, f21
	FNMSUB	f26, f31, f15, f22
	FMADD	f27, f31, f14, f23

	FMADD	f12,  f30, f12, f24
	FMADD	f13,  f30, f13, f25
	FMADD	f14,  f30, f14, f26
	FMADD	f15,  f30, f15, f27

#else

	FMUL	f16, f31, f1
	FMUL	f17, f31, f0
	FMUL	f18, f31, f3
	FMUL	f19, f31, f2

	FMUL	f20, f31, f5
	FMUL	f21, f31, f4
	FMUL	f22, f31, f7
	FMUL	f23, f31, f6

	FMSUB	f0,  f30, f0, f16
	FMADD	f1,  f30, f1, f17
	FMADD	f2,  f30, f2, f18
	FMADD	f3,  f30, f3, f19

	FMSUB	f4,  f30, f4, f20
	FMADD	f5,  f30, f5, f21
	FMADD	f6,  f30, f6, f22
	FMADD	f7,  f30, f7, f23

	FMUL	f16, f31, f9
	FMUL	f17, f31, f8
	FMUL	f18, f31, f11
	FMUL	f19, f31, f10

	FMUL	f20, f31, f13
	FMUL	f21, f31, f12
	FMUL	f22, f31, f15
	FMUL	f23, f31, f14

	FMSUB	f8,  f30, f8,  f16
	FMADD	f9,  f30, f9,  f17
	FMADD	f10, f30, f10, f18
	FMADD	f11, f30, f11, f19

	FMSUB	f12, f30, f12, f20
	FMADD	f13, f30, f13, f21
	FMADD	f14, f30, f14, f22
	FMADD	f15, f30, f15, f23
#endif

	STFD	f0,  0 * SIZE(CO1)
	STFD	f1,  1 * SIZE(CO1)
	STFD	f2,  2 * SIZE(CO1)
	STFD	f3,  3 * SIZE(CO1)

	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

	STFD	f4,  0 * SIZE(CO2)
	STFD	f5,  1 * SIZE(CO2)
	STFD	f6,  2 * SIZE(CO2)
	STFD	f7,  3 * SIZE(CO2)

	fmr	f4,  f0
	fmr	f5,  f0
	fmr	f6,  f0
	fmr	f7,  f0

	STFD	f8,  0 * SIZE(CO3)
	STFD	f9,  1 * SIZE(CO3)
	STFD	f10, 2 * SIZE(CO3)
	STFD	f11, 3 * SIZE(CO3)

	fmr	f8,  f0
	fmr	f9,  f0
	fmr	f10, f0
	fmr	f11, f0

	STFD	f12, 0 * SIZE(CO4)
	STFD	f13, 1 * SIZE(CO4)
	STFD	f14, 2 * SIZE(CO4)
	STFD	f15, 3 * SIZE(CO4)

	fmr	f12, f0
	fmr	f13, f0
	fmr	f14, f0
	fmr	f15, f0

	addi	CO1, CO1, 4 * SIZE
	addi	CO2, CO2, 4 * SIZE
	addi	CO3, CO3, 4 * SIZE
	addi	CO4, CO4, 4 * SIZE

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -2
#else
	addi	TEMP, TEMP, -4
#endif
	slwi	r0,   TEMP, 1 + ZBASE_SHIFT
	slwi	TEMP, TEMP, 2 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 2
#endif
#endif

	addic.	I, I, -1
	bgt	LL(11)
	.align 4

LL(20):
	andi.	I,  M,  1
	ble	LL(29)

#ifndef TRMMKERNEL
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(B)
	LFD	f21,  1 * SIZE(B)
	LFD	f22,  2 * SIZE(B)
	LFD	f23,  3 * SIZE(B)
	LFD	f24,  4 * SIZE(B)
	LFD	f25,  5 * SIZE(B)
	LFD	f26,  6 * SIZE(B)
	LFD	f27,  7 * SIZE(B)

	srawi.	r0,  K,  2
	mr	BO,  B
	mtspr	CTR, r0
	ble	LL(25)
#else
#if	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(B)
	LFD	f21,  1 * SIZE(B)
	LFD	f22,  2 * SIZE(B)
	LFD	f23,  3 * SIZE(B)
	LFD	f24,  4 * SIZE(B)
	LFD	f25,  5 * SIZE(B)
	LFD	f26,  6 * SIZE(B)
	LFD	f27,  7 * SIZE(B)

	mr	BO,  B
#else
	slwi	r0,   KK, 0 + ZBASE_SHIFT
	slwi	TEMP, KK, 2 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, B,  TEMP

	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(BO)
	LFD	f21,  1 * SIZE(BO)
	LFD	f22,  2 * SIZE(BO)
	LFD	f23,  3 * SIZE(BO)
	LFD	f24,  4 * SIZE(BO)
	LFD	f25,  5 * SIZE(BO)
	LFD	f26,  6 * SIZE(BO)
	LFD	f27,  7 * SIZE(BO)
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 4
#endif
	srawi.	TEMP,  TEMP,  2
	mtspr	CTR, TEMP
	ble	LL(25)
#endif
	.align 4

LL(22):
	FMA1	f0,  f16, f20, f0
	FMA4	f3,  f17, f20, f3
	FMA2	f1,  f16, f21, f1
	FMA3	f2,  f17, f21, f2

	LFD	f28,  4 * SIZE(AO)
	LFD	f29,  5 * SIZE(AO)
	LFD	f30,  6 * SIZE(AO)
	LFD	f31,  7 * SIZE(AO)

	FMA1	f4,  f16, f22, f4
	FMA4	f7,  f17, f22, f7
	FMA2	f5,  f16, f23, f5
	FMA3	f6,  f17, f23, f6

	LFD	f20,  8 * SIZE(BO)
	LFD	f21,  9 * SIZE(BO)
	LFD	f22, 10 * SIZE(BO)
	LFD	f23, 11 * SIZE(BO)

	FMA1	f8,  f16, f24, f8
	FMA4	f11, f17, f24, f11
	FMA2	f9,  f16, f25, f9
	FMA3	f10, f17, f25, f10

	FMA1	f12, f16, f26, f12
	FMA4	f15, f17, f26, f15
	FMA2	f13, f16, f27, f13
	FMA3	f14, f17, f27, f14

	LFD	f24, 12 * SIZE(BO)
	LFD	f25, 13 * SIZE(BO)
	LFD	f26, 14 * SIZE(BO)
	LFD	f27, 15 * SIZE(BO)

	FMA1	f0,  f18, f20, f0
	FMA4	f3,  f19, f20, f3
	FMA2	f1,  f18, f21, f1
	FMA3	f2,  f19, f21, f2

	FMA1	f4,  f18, f22, f4
	FMA4	f7,  f19, f22, f7
	FMA2	f5,  f18, f23, f5
	FMA3	f6,  f19, f23, f6

	LFD	f20, 16 * SIZE(BO)
	LFD	f21, 17 * SIZE(BO)
	LFD	f22, 18 * SIZE(BO)
	LFD	f23, 19 * SIZE(BO)

	FMA1	f8,  f18, f24, f8
	FMA4	f11, f19, f24, f11
	FMA2	f9,  f18, f25, f9
	FMA3	f10, f19, f25, f10

	FMA1	f12, f18, f26, f12
	FMA4	f15, f19, f26, f15
	FMA2	f13, f18, f27, f13
	FMA3	f14, f19, f27, f14

	LFD	f24, 20 * SIZE(BO)
	LFD	f25, 21 * SIZE(BO)
	LFD	f26, 22 * SIZE(BO)
	LFD	f27, 23 * SIZE(BO)

	FMA1	f0,  f28, f20, f0
	FMA4	f3,  f29, f20, f3
	FMA2	f1,  f28, f21, f1
	FMA3	f2,  f29, f21, f2

	LFD	f16,  8 * SIZE(AO)
	LFD	f17,  9 * SIZE(AO)
	LFD	f18, 10 * SIZE(AO)
	LFD	f19, 11 * SIZE(AO)

	FMA1	f4,  f28, f22, f4
	FMA4	f7,  f29, f22, f7
	FMA2	f5,  f28, f23, f5
	FMA3	f6,  f29, f23, f6

	LFD	f20, 24 * SIZE(BO)
	LFD	f21, 25 * SIZE(BO)
	LFD	f22, 26 * SIZE(BO)
	LFD	f23, 27 * SIZE(BO)

	FMA1	f8,  f28, f24, f8
	FMA4	f11, f29, f24, f11
	FMA2	f9,  f28, f25, f9
	FMA3	f10, f29, f25, f10

	FMA1	f12, f28, f26, f12
	FMA4	f15, f29, f26, f15
	FMA2	f13, f28, f27, f13
	FMA3	f14, f29, f27, f14

	LFD	f24, 28 * SIZE(BO)
	LFD	f25, 29 * SIZE(BO)
	LFD	f26, 30 * SIZE(BO)
	LFD	f27, 31 * SIZE(BO)

	FMA1	f0,  f30, f20, f0
	FMA4	f3,  f31, f20, f3
	FMA2	f1,  f30, f21, f1
	FMA3	f2,  f31, f21, f2

	FMA1	f4,  f30, f22, f4
	FMA4	f7,  f31, f22, f7
	FMA2	f5,  f30, f23, f5
	FMA3	f6,  f31, f23, f6

	LFD	f20, 32 * SIZE(BO)
	LFD	f21, 33 * SIZE(BO)
	LFD	f22, 34 * SIZE(BO)
	LFD	f23, 35 * SIZE(BO)

	FMA1	f8,  f30, f24, f8
	FMA4	f11, f31, f24, f11
	FMA2	f9,  f30, f25, f9
	FMA3	f10, f31, f25, f10

	FMA1	f12, f30, f26, f12
	FMA4	f15, f31, f26, f15
	FMA2	f13, f30, f27, f13
	FMA3	f14, f31, f27, f14

	LFD	f24, 36 * SIZE(BO)
	LFD	f25, 37 * SIZE(BO)
	LFD	f26, 38 * SIZE(BO)
	LFD	f27, 39 * SIZE(BO)

	addi	AO, AO,  8 * SIZE
	addi	BO, BO, 32 * SIZE

	bdnz	LL(22)
	.align 4

LL(25):
	lfd	f30,  ALPHA_R
	lfd	f31,  ALPHA_I

#ifndef TRMMKERNEL
	andi.	r0,  K,  3
	mtspr	CTR, r0
	ble	LL(28)
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 4
#endif
	andi.	TEMP,  TEMP,  3
	mtspr	CTR, TEMP
	ble	LL(28)
#endif
	.align 4

LL(26):
	FMA1	f0,  f16, f20, f0
	FMA4	f3,  f17, f20, f3
	FMA2	f1,  f16, f21, f1
	FMA3	f2,  f17, f21, f2

	FMA1	f4,  f16, f22, f4
	FMA4	f7,  f17, f22, f7
	FMA2	f5,  f16, f23, f5
	FMA3	f6,  f17, f23, f6

	LFD	f20,  8 * SIZE(BO)
	LFD	f21,  9 * SIZE(BO)
	LFD	f22, 10 * SIZE(BO)
	LFD	f23, 11 * SIZE(BO)

	FMA1	f8,  f16, f24, f8
	FMA4	f11, f17, f24, f11
	FMA2	f9,  f16, f25, f9
	FMA3	f10, f17, f25, f10

	FMA1	f12, f16, f26, f12
	FMA4	f15, f17, f26, f15
	FMA2	f13, f16, f27, f13
	FMA3	f14, f17, f27, f14

	LFD	f16,  2 * SIZE(AO)
	LFD	f17,  3 * SIZE(AO)

	LFD	f24, 12 * SIZE(BO)
	LFD	f25, 13 * SIZE(BO)
	LFD	f26, 14 * SIZE(BO)
	LFD	f27, 15 * SIZE(BO)

	addi	AO, AO,  2 * SIZE
	addi	BO, BO,  8 * SIZE
	bdnz	LL(26)
	.align 4

LL(28):
#ifndef TRMMKERNEL

	LFD	f16, 0 * SIZE(CO1)
	LFD	f17, 1 * SIZE(CO1)
	LFD	f18, 0 * SIZE(CO2)
	LFD	f19, 1 * SIZE(CO2)

	FADD	f0,  f0,  f2
	FADD	f1,  f1,  f3
	FADD	f4,  f4,  f6
	FADD	f5,  f5,  f7

	LFD	f20, 0 * SIZE(CO3)
	LFD	f21, 1 * SIZE(CO3)
	LFD	f22, 0 * SIZE(CO4)
	LFD	f23, 1 * SIZE(CO4)

	FADD	f8,  f8,  f10
	FADD	f9,  f9,  f11
	FADD	f12, f12, f14
	FADD	f13, f13, f15

	FNMSUB	f24, f31, f1, f16
	FMADD	f25, f31, f0, f17
	FNMSUB	f26, f31, f5, f18
	FMADD	f27, f31, f4, f19

	FMADD	f0,  f30, f0, f24
	FMADD	f1,  f30, f1, f25
	FMADD	f4,  f30, f4,  f26
	FMADD	f5,  f30, f5,  f27

	FNMSUB	f24, f31, f9,  f20
	FMADD	f25, f31, f8,  f21
	FNMSUB	f26, f31, f13, f22
	FMADD	f27, f31, f12, f23

	FMADD	f8,  f30, f8,  f24
	FMADD	f9,  f30, f9,  f25
	FMADD	f12,  f30, f12, f26
	FMADD	f13,  f30, f13, f27

#else
	FADD	f0,  f0,  f2
	FADD	f1,  f1,  f3
	FADD	f4,  f4,  f6
	FADD	f5,  f5,  f7

	FMUL	f16, f31, f1
	FMUL	f17, f31, f0
	FMUL	f18, f31, f5
	FMUL	f19, f31, f4

	FMSUB	f0,  f30, f0, f16
	FMADD	f1,  f30, f1, f17
	FMSUB	f4,  f30, f4, f18
	FMADD	f5,  f30, f5, f19

	FADD	f8,  f8,  f10
	FADD	f9,  f9,  f11
	FADD	f12, f12, f14
	FADD	f13, f13, f15

	FMUL	f20, f31, f9
	FMUL	f21, f31, f8
	FMUL	f22, f31, f13
	FMUL	f23, f31, f12

	FMSUB	f8,  f30, f8,  f20
	FMADD	f9,  f30, f9,  f21
	FMSUB	f12, f30, f12, f22
	FMADD	f13, f30, f13, f23

#endif

	STFD	f0,  0 * SIZE(CO1)
	STFD	f1,  1 * SIZE(CO1)
	STFD	f4,  0 * SIZE(CO2)
	STFD	f5,  1 * SIZE(CO2)

	STFD	f8,  0 * SIZE(CO3)
	STFD	f9,  1 * SIZE(CO3)
	STFD	f12, 0 * SIZE(CO4)
	STFD	f13, 1 * SIZE(CO4)

	addi	CO1, CO1, 2 * SIZE
	addi	CO2, CO2, 2 * SIZE
	addi	CO3, CO3, 2 * SIZE
	addi	CO4, CO4, 2 * SIZE

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -1
#else
	addi	TEMP, TEMP, -4
#endif
	slwi	r0,   TEMP, 0 + ZBASE_SHIFT
	slwi	TEMP, TEMP, 2 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 1
#endif
#endif
	.align 4

LL(29):
#if defined(TRMMKERNEL) && !defined(LEFT)
	addi	KK, KK, 4
#endif

	mr	B,  BO

	addic.	J, J, -1
	bgt	LL(10)
	.align 4

LL(30):
	andi.	J, N,  2
	ble	LL(50)

	mr	CO1, C
	add	CO2, C,   LDC
	add	C,   CO2, LDC

#if defined(TRMMKERNEL) && defined(LEFT)
	mr	KK, OFFSET
#endif

	slwi	BB, K, ZBASE_SHIFT + 1
	mr	AO, A

	lfs	f0, FZERO
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0
	fmr	f4,  f0
	fmr	f5,  f0
	fmr	f6,  f0
	fmr	f7,  f0
	fmr	f8,  f0
	fmr	f9,  f0
	fmr	f10, f0
	fmr	f11, f0
	fmr	f12, f0
	fmr	f13, f0
	fmr	f14, f0
	fmr	f15, f0

	srawi.	I, M,  1
	ble	LL(40)
	.align 4

LL(31):
#ifndef TRMMKERNEL
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(B)
	LFD	f21,  1 * SIZE(B)
	LFD	f22,  2 * SIZE(B)
	LFD	f23,  3 * SIZE(B)

	LFD	f24,  4 * SIZE(B)
	LFD	f25,  5 * SIZE(B)
	LFD	f26,  6 * SIZE(B)
	LFD	f27,  7 * SIZE(B)

	dcbtst	CO1, PREC
	dcbtst	CO2, PREC

	srawi.	r0,  K,  3
	mr	BO,  B
	mtspr	CTR, r0
	ble	LL(35)
#else
#if	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(B)
	LFD	f21,  1 * SIZE(B)
	LFD	f22,  2 * SIZE(B)
	LFD	f23,  3 * SIZE(B)

	LFD	f24,  4 * SIZE(B)
	LFD	f25,  5 * SIZE(B)
	LFD	f26,  6 * SIZE(B)
	LFD	f27,  7 * SIZE(B)

	mr	BO,  B
#else
	slwi	r0,   KK, 1 + ZBASE_SHIFT
	slwi	TEMP, KK, 1 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, B,  TEMP

	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(BO)
	LFD	f21,  1 * SIZE(BO)
	LFD	f22,  2 * SIZE(BO)
	LFD	f23,  3 * SIZE(BO)

	LFD	f24,  4 * SIZE(BO)
	LFD	f25,  5 * SIZE(BO)
	LFD	f26,  6 * SIZE(BO)
	LFD	f27,  7 * SIZE(BO)

#endif

	dcbtst	CO1, PREC
	dcbtst	CO2, PREC

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 2
#endif
	srawi.	TEMP,  TEMP,  3
	mtspr	CTR, TEMP
	ble	LL(35)
#endif
	.align 4

LL(32):
	dcbt	AO, PREA
	dcbtst	BO, PREA

	FMA1	f0,  f16, f20, f0
	FMA1	f2,  f18, f20, f2
	FMA2	f1,  f16, f21, f1
	FMA2	f3,  f18, f21, f3

	LFD	f28,  4 * SIZE(AO)
	LFD	f29,  5 * SIZE(AO)
	LFD	f30,  6 * SIZE(AO)
	LFD	f31,  7 * SIZE(AO)

	FMA1	f4,  f16, f22, f4
	FMA1	f6,  f18, f22, f6
	FMA2	f5,  f16, f23, f5
	FMA2	f7,  f18, f23, f7

	FMA4	f9,  f17, f20, f9
	FMA4	f11, f19, f20, f11
	FMA3	f8,  f17, f21, f8
	FMA3	f10, f19, f21, f10

	FMA4	f13, f17, f22, f13
	FMA4	f15, f19, f22, f15
	FMA3	f12, f17, f23, f12
	FMA3	f14, f19, f23, f14

	LFD	f20,  8 * SIZE(BO)
	LFD	f21,  9 * SIZE(BO)
	LFD	f22, 10 * SIZE(BO)
	LFD	f23, 11 * SIZE(BO)

	FMA1	f0,  f28, f24, f0
	FMA1	f2,  f30, f24, f2
	FMA2	f1,  f28, f25, f1
	FMA2	f3,  f30, f25, f3

	LFD	f16,  8 * SIZE(AO)
	LFD	f17,  9 * SIZE(AO)
	LFD	f18, 10 * SIZE(AO)
	LFD	f19, 11 * SIZE(AO)

	FMA1	f4,  f28, f26, f4
	FMA1	f6,  f30, f26, f6
	FMA2	f5,  f28, f27, f5
	FMA2	f7,  f30, f27, f7

	FMA4	f9,  f29, f24, f9
	FMA4	f11, f31, f24, f11
	FMA3	f8,  f29, f25, f8
	FMA3	f10, f31, f25, f10

	FMA4	f13, f29, f26, f13
	FMA4	f15, f31, f26, f15
	FMA3	f12, f29, f27, f12
	FMA3	f14, f31, f27, f14

	LFD	f24, 12 * SIZE(BO)
	LFD	f25, 13 * SIZE(BO)
	LFD	f26, 14 * SIZE(BO)
	LFD	f27, 15 * SIZE(BO)

	FMA1	f0,  f16, f20, f0
	FMA1	f2,  f18, f20, f2
	FMA2	f1,  f16, f21, f1
	FMA2	f3,  f18, f21, f3

	LFD	f28, 12 * SIZE(AO)
	LFD	f29, 13 * SIZE(AO)
	LFD	f30, 14 * SIZE(AO)
	LFD	f31, 15 * SIZE(AO)

	FMA1	f4,  f16, f22, f4
	FMA1	f6,  f18, f22, f6
	FMA2	f5,  f16, f23, f5
	FMA2	f7,  f18, f23, f7

	FMA4	f9,  f17, f20, f9
	FMA4	f11, f19, f20, f11
	FMA3	f8,  f17, f21, f8
	FMA3	f10, f19, f21, f10

	FMA4	f13, f17, f22, f13
	FMA4	f15, f19, f22, f15
	FMA3	f12, f17, f23, f12
	FMA3	f14, f19, f23, f14

	LFD	f20, 16 * SIZE(BO)
	LFD	f21, 17 * SIZE(BO)
	LFD	f22, 18 * SIZE(BO)
	LFD	f23, 19 * SIZE(BO)

	FMA1	f0,  f28, f24, f0
	FMA1	f2,  f30, f24, f2
	FMA2	f1,  f28, f25, f1
	FMA2	f3,  f30, f25, f3

	LFD	f16, 16 * SIZE(AO)
	LFD	f17, 17 * SIZE(AO)
	LFD	f18, 18 * SIZE(AO)
	LFD	f19, 19 * SIZE(AO)

	FMA1	f4,  f28, f26, f4
	FMA1	f6,  f30, f26, f6
	FMA2	f5,  f28, f27, f5
	FMA2	f7,  f30, f27, f7

	FMA4	f9,  f29, f24, f9
	FMA4	f11, f31, f24, f11
	FMA3	f8,  f29, f25, f8
	FMA3	f10, f31, f25, f10

	FMA4	f13, f29, f26, f13
	FMA4	f15, f31, f26, f15
	FMA3	f12, f29, f27, f12
	FMA3	f14, f31, f27, f14

	LFD	f24, 20 * SIZE(BO)
	LFD	f25, 21 * SIZE(BO)
	LFD	f26, 22 * SIZE(BO)
	LFD	f27, 23 * SIZE(BO)

	FMA1	f0,  f16, f20, f0
	FMA1	f2,  f18, f20, f2
	FMA2	f1,  f16, f21, f1
	FMA2	f3,  f18, f21, f3

	LFD	f28, 20 * SIZE(AO)
	LFD	f29, 21 * SIZE(AO)
	LFD	f30, 22 * SIZE(AO)
	LFD	f31, 23 * SIZE(AO)

	FMA1	f4,  f16, f22, f4
	FMA1	f6,  f18, f22, f6
	FMA2	f5,  f16, f23, f5
	FMA2	f7,  f18, f23, f7

	FMA4	f9,  f17, f20, f9
	FMA4	f11, f19, f20, f11
	FMA3	f8,  f17, f21, f8
	FMA3	f10, f19, f21, f10

	FMA4	f13, f17, f22, f13
	FMA4	f15, f19, f22, f15
	FMA3	f12, f17, f23, f12
	FMA3	f14, f19, f23, f14

	LFD	f20, 24 * SIZE(BO)
	LFD	f21, 25 * SIZE(BO)
	LFD	f22, 26 * SIZE(BO)
	LFD	f23, 27 * SIZE(BO)

	FMA1	f0,  f28, f24, f0
	FMA1	f2,  f30, f24, f2
	FMA2	f1,  f28, f25, f1
	FMA2	f3,  f30, f25, f3

	LFD	f16, 24 * SIZE(AO)
	LFD	f17, 25 * SIZE(AO)
	LFD	f18, 26 * SIZE(AO)
	LFD	f19, 27 * SIZE(AO)

	FMA1	f4,  f28, f26, f4
	FMA1	f6,  f30, f26, f6
	FMA2	f5,  f28, f27, f5
	FMA2	f7,  f30, f27, f7

	FMA4	f9,  f29, f24, f9
	FMA4	f11, f31, f24, f11
	FMA3	f8,  f29, f25, f8
	FMA3	f10, f31, f25, f10

	FMA4	f13, f29, f26, f13
	FMA4	f15, f31, f26, f15
	FMA3	f12, f29, f27, f12
	FMA3	f14, f31, f27, f14

	LFD	f24, 28 * SIZE(BO)
	LFD	f25, 29 * SIZE(BO)
	LFD	f26, 30 * SIZE(BO)
	LFD	f27, 31 * SIZE(BO)

	FMA1	f0,  f16, f20, f0
	FMA1	f2,  f18, f20, f2
	FMA2	f1,  f16, f21, f1
	FMA2	f3,  f18, f21, f3

	LFD	f28, 28 * SIZE(AO)
	LFD	f29, 29 * SIZE(AO)
	LFD	f30, 30 * SIZE(AO)
	LFD	f31, 31 * SIZE(AO)

	FMA1	f4,  f16, f22, f4
	FMA1	f6,  f18, f22, f6
	FMA2	f5,  f16, f23, f5
	FMA2	f7,  f18, f23, f7

	FMA4	f9,  f17, f20, f9
	FMA4	f11, f19, f20, f11
	FMA3	f8,  f17, f21, f8
	FMA3	f10, f19, f21, f10

	FMA4	f13, f17, f22, f13
	FMA4	f15, f19, f22, f15
	FMA3	f12, f17, f23, f12
	FMA3	f14, f19, f23, f14

	LFD	f20, 32 * SIZE(BO)
	LFD	f21, 33 * SIZE(BO)
	LFD	f22, 34 * SIZE(BO)
	LFD	f23, 35 * SIZE(BO)

	FMA1	f0,  f28, f24, f0
	FMA1	f2,  f30, f24, f2
	FMA2	f1,  f28, f25, f1
	FMA2	f3,  f30, f25, f3

	LFD	f16, 32 * SIZE(AO)
	LFD	f17, 33 * SIZE(AO)
	LFD	f18, 34 * SIZE(AO)
	LFD	f19, 35 * SIZE(AO)

	FMA1	f4,  f28, f26, f4
	FMA1	f6,  f30, f26, f6
	FMA2	f5,  f28, f27, f5
	FMA2	f7,  f30, f27, f7

	FMA4	f9,  f29, f24, f9
	FMA4	f11, f31, f24, f11
	FMA3	f8,  f29, f25, f8
	FMA3	f10, f31, f25, f10

	FMA4	f13, f29, f26, f13
	FMA4	f15, f31, f26, f15
	FMA3	f12, f29, f27, f12
	FMA3	f14, f31, f27, f14

	LFD	f24, 36 * SIZE(BO)
	LFD	f25, 37 * SIZE(BO)
	LFD	f26, 38 * SIZE(BO)
	LFD	f27, 39 * SIZE(BO)

	addi	AO, AO, 32 * SIZE
	addi	BO, BO, 32 * SIZE

	bdnz	LL(32)
	.align 4

LL(35):
	lfd	f30,  ALPHA_R
	lfd	f31,  ALPHA_I

	dcbtst	B, BB
	addi	BB, BB, 16 * SIZE

#ifndef TRMMKERNEL
	andi.	r0,  K,  7
	mtspr	CTR, r0
	ble	LL(38)
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 2
#endif
	andi.	TEMP,  TEMP,  7
	mtspr	CTR, TEMP
	ble	LL(38)
#endif
	.align 4

LL(36):
	FMA1	f0,  f16, f20, f0
	FMA1	f2,  f18, f20, f2
	FMA2	f1,  f16, f21, f1
	FMA2	f3,  f18, f21, f3

	FMA1	f4,  f16, f22, f4
	FMA1	f6,  f18, f22, f6
	FMA2	f5,  f16, f23, f5
	FMA2	f7,  f18, f23, f7

	FMA4	f9,  f17, f20, f9
	FMA4	f11, f19, f20, f11
	FMA3	f8,  f17, f21, f8
	FMA3	f10, f19, f21, f10

	LFD	f16,  4 * SIZE(AO)
	LFD	f18,  6 * SIZE(AO)
	LFD	f20,  4 * SIZE(BO)
	LFD	f21,  5 * SIZE(BO)

	FMA4	f13, f17, f22, f13
	FMA4	f15, f19, f22, f15
	FMA3	f12, f17, f23, f12
	FMA3	f14, f19, f23, f14

	LFD	f17,  5 * SIZE(AO)
	LFD	f19,  7 * SIZE(AO)
	LFD	f22,  6 * SIZE(BO)
	LFD	f23,  7 * SIZE(BO)

	addi	AO, AO,  4 * SIZE
	addi	BO, BO,  4 * SIZE
	bdnz	LL(36)
	.align 4

LL(38):
#ifndef TRMMKERNEL

	LFD	f16, 0 * SIZE(CO1)
	LFD	f17, 1 * SIZE(CO1)
	LFD	f18, 2 * SIZE(CO1)
	LFD	f19, 3 * SIZE(CO1)

	FADD	f0, f0, f8
	FADD	f1, f1, f9
	FADD	f2, f2, f10
	FADD	f3, f3, f11

	LFD	f20, 0 * SIZE(CO2)
	LFD	f21, 1 * SIZE(CO2)
	LFD	f22, 2 * SIZE(CO2)
	LFD	f23, 3 * SIZE(CO2)

	FADD	f4, f4, f12
	FADD	f5, f5, f13
	FADD	f6, f6, f14
	FADD	f7, f7, f15

	FNMSUB	f24, f31, f1, f16
	FMADD	f25, f31, f0, f17
	FNMSUB	f26, f31, f3, f18
	FMADD	f27, f31, f2, f19

	FMADD	f0,  f30, f0, f24
	FMADD	f1,  f30, f1, f25
	FMADD	f2,  f30, f2, f26
	FMADD	f3,  f30, f3, f27

	FNMSUB	f24, f31, f5, f20
	FMADD	f25, f31, f4, f21
	FNMSUB	f26, f31, f7, f22
	FMADD	f27, f31, f6, f23

	FMADD	f4,  f30, f4,  f24
	FMADD	f5,  f30, f5,  f25
	FMADD	f6,  f30, f6,  f26
	FMADD	f7,  f30, f7,  f27

#else
	FADD	f0, f0, f8
	FADD	f1, f1, f9
	FADD	f2, f2, f10
	FADD	f3, f3, f11

	FADD	f4, f4, f12
	FADD	f5, f5, f13
	FADD	f6, f6, f14
	FADD	f7, f7, f15

	FMUL	f16, f31, f1
	FMUL	f17, f31, f0
	FMUL	f18, f31, f3
	FMUL	f19, f31, f2

	FMUL	f20, f31, f5
	FMUL	f21, f31, f4
	FMUL	f22, f31, f7
	FMUL	f23, f31, f6

	FMSUB	f0,  f30, f0, f16
	FMADD	f1,  f30, f1, f17
	FMADD	f2,  f30, f2, f18
	FMADD	f3,  f30, f3, f19

	FMSUB	f4,  f30, f4, f20
	FMADD	f5,  f30, f5, f21
	FMADD	f6,  f30, f6, f22
	FMADD	f7,  f30, f7, f23

#endif

	STFD	f0,  0 * SIZE(CO1)
	STFD	f1,  1 * SIZE(CO1)
	STFD	f2,  2 * SIZE(CO1)
	STFD	f3,  3 * SIZE(CO1)

	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

	STFD	f4,  0 * SIZE(CO2)
	STFD	f5,  1 * SIZE(CO2)
	STFD	f6,  2 * SIZE(CO2)
	STFD	f7,  3 * SIZE(CO2)

	fmr	f4,  f0
	fmr	f5,  f0
	fmr	f6,  f0
	fmr	f7,  f0

	fmr	f8,  f0
	fmr	f9,  f0
	fmr	f10, f0
	fmr	f11, f0

	fmr	f12, f0
	fmr	f13, f0
	fmr	f14, f0
	fmr	f15, f0

	addi	CO1, CO1, 4 * SIZE
	addi	CO2, CO2, 4 * SIZE

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -2
#else
	addi	TEMP, TEMP, -2
#endif
	slwi	r0,   TEMP, 1 + ZBASE_SHIFT
	slwi	TEMP, TEMP, 1 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 2
#endif
#endif

	addic.	I, I, -1
	bgt	LL(31)
	.align 4

LL(40):
	andi.	I,  M,  1
	ble	LL(49)

#ifndef TRMMKERNEL
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)

	LFD	f20,  0 * SIZE(B)
	LFD	f21,  1 * SIZE(B)
	LFD	f22,  2 * SIZE(B)
	LFD	f23,  3 * SIZE(B)

	srawi.	r0,  K,  2
	mr	BO,  B
	mtspr	CTR, r0
	ble	LL(45)
#else
#if	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)

	LFD	f20,  0 * SIZE(B)
	LFD	f21,  1 * SIZE(B)
	LFD	f22,  2 * SIZE(B)
	LFD	f23,  3 * SIZE(B)

	mr	BO,  B
#else
	slwi	r0,   KK, 0 + ZBASE_SHIFT
	slwi	TEMP, KK, 1 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, B,  TEMP

	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)

	LFD	f20,  0 * SIZE(BO)
	LFD	f21,  1 * SIZE(BO)
	LFD	f22,  2 * SIZE(BO)
	LFD	f23,  3 * SIZE(BO)
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 2
#endif
	srawi.	TEMP,  TEMP,  2
	mtspr	CTR, TEMP
	ble	LL(45)
#endif
	.align 4

LL(42):
	FMA1	f0,  f16, f20, f0
	FMA4	f3,  f17, f20, f3
	FMA2	f1,  f16, f21, f1
	FMA3	f2,  f17, f21, f2

	FMA1	f4,  f16, f22, f4
	FMA4	f7,  f17, f22, f7
	FMA2	f5,  f16, f23, f5
	FMA3	f6,  f17, f23, f6

	LFD	f16,  2 * SIZE(AO)
	LFD	f17,  3 * SIZE(AO)

	LFD	f20,  4 * SIZE(BO)
	LFD	f21,  5 * SIZE(BO)
	LFD	f22,  6 * SIZE(BO)
	LFD	f23,  7 * SIZE(BO)

	FMA1	f0,  f16, f20, f0
	FMA4	f3,  f17, f20, f3
	FMA2	f1,  f16, f21, f1
	FMA3	f2,  f17, f21, f2

	FMA1	f4,  f16, f22, f4
	FMA4	f7,  f17, f22, f7
	FMA2	f5,  f16, f23, f5
	FMA3	f6,  f17, f23, f6

	LFD	f16,  4 * SIZE(AO)
	LFD	f17,  5 * SIZE(AO)

	LFD	f20,  8 * SIZE(BO)
	LFD	f21,  9 * SIZE(BO)
	LFD	f22, 10 * SIZE(BO)
	LFD	f23, 11 * SIZE(BO)

	FMA1	f0,  f16, f20, f0
	FMA4	f3,  f17, f20, f3
	FMA2	f1,  f16, f21, f1
	FMA3	f2,  f17, f21, f2

	FMA1	f4,  f16, f22, f4
	FMA4	f7,  f17, f22, f7
	FMA2	f5,  f16, f23, f5
	FMA3	f6,  f17, f23, f6

	LFD	f16,  6 * SIZE(AO)
	LFD	f17,  7 * SIZE(AO)

	LFD	f20, 12 * SIZE(BO)
	LFD	f21, 13 * SIZE(BO)
	LFD	f22, 14 * SIZE(BO)
	LFD	f23, 15 * SIZE(BO)

	FMA1	f0,  f16, f20, f0
	FMA4	f3,  f17, f20, f3
	FMA2	f1,  f16, f21, f1
	FMA3	f2,  f17, f21, f2

	FMA1	f4,  f16, f22, f4
	FMA4	f7,  f17, f22, f7
	FMA2	f5,  f16, f23, f5
	FMA3	f6,  f17, f23, f6

	LFD	f16,  8 * SIZE(AO)
	LFD	f17,  9 * SIZE(AO)

	LFD	f20, 16 * SIZE(BO)
	LFD	f21, 17 * SIZE(BO)
	LFD	f22, 18 * SIZE(BO)
	LFD	f23, 19 * SIZE(BO)

	addi	AO, AO,  8 * SIZE
	addi	BO, BO, 16 * SIZE

	bdnz	LL(42)
	.align 4

LL(45):
	lfd	f30,  ALPHA_R
	lfd	f31,  ALPHA_I

#ifndef TRMMKERNEL
	andi.	r0,  K,  3
	mtspr	CTR, r0
	ble	LL(48)
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 2
#endif
	andi.	TEMP,  TEMP,  3
	mtspr	CTR, TEMP
	ble	LL(48)
#endif
	.align 4

LL(46):
	FMA1	f0,  f16, f20, f0
	FMA4	f3,  f17, f20, f3
	FMA2	f1,  f16, f21, f1
	FMA3	f2,  f17, f21, f2

	FMA1	f4,  f16, f22, f4
	FMA4	f7,  f17, f22, f7
	FMA2	f5,  f16, f23, f5
	FMA3	f6,  f17, f23, f6

	LFD	f16,  2 * SIZE(AO)
	LFD	f17,  3 * SIZE(AO)

	LFD	f20,  4 * SIZE(BO)
	LFD	f21,  5 * SIZE(BO)
	LFD	f22,  6 * SIZE(BO)
	LFD	f23,  7 * SIZE(BO)

	addi	AO, AO,  2 * SIZE
	addi	BO, BO,  4 * SIZE
	bdnz	LL(46)
	.align 4

LL(48):
#ifndef TRMMKERNEL
	LFD	f16, 0 * SIZE(CO1)
	LFD	f17, 1 * SIZE(CO1)
	LFD	f20, 0 * SIZE(CO2)
	LFD	f21, 1 * SIZE(CO2)

	FADD	f0,  f0,  f2
	FADD	f1,  f1,  f3
	FADD	f4,  f4,  f6
	FADD	f5,  f5,  f7

	FNMSUB	f24, f31, f1, f16
	FMADD	f25, f31, f0, f17
	FNMSUB	f26, f31, f5, f20
	FMADD	f27, f31, f4, f21

	FMADD	f0,  f30, f0, f24
	FMADD	f1,  f30, f1, f25
	FMADD	f4,  f30, f4,  f26
	FMADD	f5,  f30, f5,  f27

#else
	FADD	f0,  f0,  f2
	FADD	f1,  f1,  f3
	FADD	f4,  f4,  f6
	FADD	f5,  f5,  f7

	FMUL	f16, f31, f1
	FMUL	f17, f31, f0
	FMUL	f18, f31, f5
	FMUL	f19, f31, f4

	FMSUB	f0,  f30, f0, f16
	FMADD	f1,  f30, f1, f17
	FMSUB	f4,  f30, f4, f18
	FMADD	f5,  f30, f5, f19

#endif

	STFD	f0,  0 * SIZE(CO1)
	STFD	f1,  1 * SIZE(CO1)
	STFD	f4,  0 * SIZE(CO2)
	STFD	f5,  1 * SIZE(CO2)

	addi	CO1, CO1, 2 * SIZE
	addi	CO2, CO2, 2 * SIZE

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -1
#else
	addi	TEMP, TEMP, -2
#endif
	slwi	r0,   TEMP, 0 + ZBASE_SHIFT
	slwi	TEMP, TEMP, 1 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 1
#endif
#endif
	.align 4

LL(49):
#if defined(TRMMKERNEL) && !defined(LEFT)
	addi	KK, KK, 2
#endif

	mr	B,  BO
	.align 4

LL(50):
	andi.	J, N,  1
	ble	LL(999)

	mr	CO1, C
	add	C,   CO1, LDC

#if defined(TRMMKERNEL) && defined(LEFT)
	mr	KK, OFFSET
#endif

	mr	AO, A

	lfs	f0, FZERO
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0
	fmr	f8,  f0
	fmr	f9,  f0
	fmr	f10, f0
	fmr	f11, f0

	srawi.	I, M,  1
	ble	LL(60)
	.align 4

LL(51):
#ifndef TRMMKERNEL
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(B)
	LFD	f21,  1 * SIZE(B)
	LFD	f22,  2 * SIZE(B)
	LFD	f23,  3 * SIZE(B)

	dcbtst	CO1, PREC

	srawi.	r0,  K,  3
	mr	BO,  B
	mtspr	CTR, r0
	ble	LL(55)
#else
#if	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(B)
	LFD	f21,  1 * SIZE(B)
	LFD	f22,  2 * SIZE(B)
	LFD	f23,  3 * SIZE(B)

	mr	BO,  B
#else
	slwi	r0,   KK, 1 + ZBASE_SHIFT
	slwi	TEMP, KK, 0 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, B,  TEMP

	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(BO)
	LFD	f21,  1 * SIZE(BO)
	LFD	f22,  2 * SIZE(BO)
	LFD	f23,  3 * SIZE(BO)
#endif

	dcbtst	CO1, PREC

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 1
#endif
	srawi.	TEMP,  TEMP,  3
	mtspr	CTR, TEMP
	ble	LL(55)
#endif
	.align 4

LL(52):
	dcbt	AO, PREA
	dcbtst	BO, PREA

	FMA1	f0,  f16, f20, f0
	FMA1	f2,  f18, f20, f2
	FMA2	f1,  f16, f21, f1
	FMA2	f3,  f18, f21, f3

	FMA4	f9,  f17, f20, f9
	FMA4	f11, f19, f20, f11
	FMA3	f8,  f17, f21, f8
	FMA3	f10, f19, f21, f10

	LFD	f16,  4 * SIZE(AO)
	LFD	f17,  5 * SIZE(AO)
	LFD	f18,  6 * SIZE(AO)
	LFD	f19,  7 * SIZE(AO)

	FMA1	f0,  f16, f22, f0
	FMA1	f2,  f18, f22, f2
	FMA2	f1,  f16, f23, f1
	FMA2	f3,  f18, f23, f3

	FMA4	f9,  f17, f22, f9
	FMA4	f11, f19, f22, f11
	FMA3	f8,  f17, f23, f8
	FMA3	f10, f19, f23, f10

	LFD	f16,  8 * SIZE(AO)
	LFD	f17,  9 * SIZE(AO)
	LFD	f18, 10 * SIZE(AO)
	LFD	f19, 11 * SIZE(AO)

	LFD	f20,  4 * SIZE(BO)
	LFD	f21,  5 * SIZE(BO)
	LFD	f22,  6 * SIZE(BO)
	LFD	f23,  7 * SIZE(BO)

	FMA1	f0,  f16, f20, f0
	FMA1	f2,  f18, f20, f2
	FMA2	f1,  f16, f21, f1
	FMA2	f3,  f18, f21, f3

	FMA4	f9,  f17, f20, f9
	FMA4	f11, f19, f20, f11
	FMA3	f8,  f17, f21, f8
	FMA3	f10, f19, f21, f10

	LFD	f16, 12 * SIZE(AO)
	LFD	f17, 13 * SIZE(AO)
	LFD	f18, 14 * SIZE(AO)
	LFD	f19, 15 * SIZE(AO)

	FMA1	f0,  f16, f22, f0
	FMA1	f2,  f18, f22, f2
	FMA2	f1,  f16, f23, f1
	FMA2	f3,  f18, f23, f3

	FMA4	f9,  f17, f22, f9
	FMA4	f11, f19, f22, f11
	FMA3	f8,  f17, f23, f8
	FMA3	f10, f19, f23, f10

	LFD	f16, 16 * SIZE(AO)
	LFD	f17, 17 * SIZE(AO)
	LFD	f18, 18 * SIZE(AO)
	LFD	f19, 19 * SIZE(AO)

	LFD	f20,  8 * SIZE(BO)
	LFD	f21,  9 * SIZE(BO)
	LFD	f22, 10 * SIZE(BO)
	LFD	f23, 11 * SIZE(BO)

	FMA1	f0,  f16, f20, f0
	FMA1	f2,  f18, f20, f2
	FMA2	f1,  f16, f21, f1
	FMA2	f3,  f18, f21, f3

	FMA4	f9,  f17, f20, f9
	FMA4	f11, f19, f20, f11
	FMA3	f8,  f17, f21, f8
	FMA3	f10, f19, f21, f10

	LFD	f16, 20 * SIZE(AO)
	LFD	f17, 21 * SIZE(AO)
	LFD	f18, 22 * SIZE(AO)
	LFD	f19, 23 * SIZE(AO)

	FMA1	f0,  f16, f22, f0
	FMA1	f2,  f18, f22, f2
	FMA2	f1,  f16, f23, f1
	FMA2	f3,  f18, f23, f3

	FMA4	f9,  f17, f22, f9
	FMA4	f11, f19, f22, f11
	FMA3	f8,  f17, f23, f8
	FMA3	f10, f19, f23, f10

	LFD	f16, 24 * SIZE(AO)
	LFD	f17, 25 * SIZE(AO)
	LFD	f18, 26 * SIZE(AO)
	LFD	f19, 27 * SIZE(AO)

	LFD	f20, 12 * SIZE(BO)
	LFD	f21, 13 * SIZE(BO)
	LFD	f22, 14 * SIZE(BO)
	LFD	f23, 15 * SIZE(BO)

	FMA1	f0,  f16, f20, f0
	FMA1	f2,  f18, f20, f2
	FMA2	f1,  f16, f21, f1
	FMA2	f3,  f18, f21, f3

	FMA4	f9,  f17, f20, f9
	FMA4	f11, f19, f20, f11
	FMA3	f8,  f17, f21, f8
	FMA3	f10, f19, f21, f10

	LFD	f16, 28 * SIZE(AO)
	LFD	f17, 29 * SIZE(AO)
	LFD	f18, 30 * SIZE(AO)
	LFD	f19, 31 * SIZE(AO)

	FMA1	f0,  f16, f22, f0
	FMA1	f2,  f18, f22, f2
	FMA2	f1,  f16, f23, f1
	FMA2	f3,  f18, f23, f3

	FMA4	f9,  f17, f22, f9
	FMA4	f11, f19, f22, f11
	FMA3	f8,  f17, f23, f8
	FMA3	f10, f19, f23, f10

	LFD	f16, 32 * SIZE(AO)
	LFD	f17, 33 * SIZE(AO)
	LFD	f18, 34 * SIZE(AO)
	LFD	f19, 35 * SIZE(AO)

	LFD	f20, 16 * SIZE(BO)
	LFD	f21, 17 * SIZE(BO)
	LFD	f22, 18 * SIZE(BO)
	LFD	f23, 19 * SIZE(BO)

	addi	AO, AO, 32 * SIZE
	addi	BO, BO, 16 * SIZE

	bdnz	LL(52)
	.align 4

LL(55):
	lfd	f30,  ALPHA_R
	lfd	f31,  ALPHA_I

#ifndef TRMMKERNEL
	andi.	r0,  K,  7
	mtspr	CTR, r0
	ble	LL(58)
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 1
#endif
	andi.	TEMP,  TEMP,  7
	mtspr	CTR, TEMP
	ble	LL(58)
#endif
	.align 4

LL(56):
	FMA1	f0,  f16, f20, f0
	FMA1	f2,  f18, f20, f2
	FMA2	f1,  f16, f21, f1
	FMA2	f3,  f18, f21, f3

	LFD	f16,  4 * SIZE(AO)
	LFD	f18,  6 * SIZE(AO)

	FMA4	f9,  f17, f20, f9
	FMA4	f11, f19, f20, f11
	FMA3	f8,  f17, f21, f8
	FMA3	f10, f19, f21, f10

	LFD	f17,  5 * SIZE(AO)
	LFD	f19,  7 * SIZE(AO)
	LFD	f20,  2 * SIZE(BO)
	LFD	f21,  3 * SIZE(BO)

	addi	AO, AO,  4 * SIZE
	addi	BO, BO,  2 * SIZE
	bdnz	LL(56)
	.align 4

LL(58):
#ifndef TRMMKERNEL
	LFD	f16, 0 * SIZE(CO1)
	LFD	f17, 1 * SIZE(CO1)
	LFD	f18, 2 * SIZE(CO1)
	LFD	f19, 3 * SIZE(CO1)

	FADD	f0, f0, f8
	FADD	f1, f1, f9
	FADD	f2, f2, f10
	FADD	f3, f3, f11

	FNMSUB	f24, f31, f1, f16
	FMADD	f25, f31, f0, f17
	FNMSUB	f26, f31, f3, f18
	FMADD	f27, f31, f2, f19

	FMADD	f0,  f30, f0, f24
	FMADD	f1,  f30, f1, f25
	FMADD	f2,  f30, f2, f26
	FMADD	f3,  f30, f3, f27

#else
	FADD	f0, f0, f8
	FADD	f1, f1, f9
	FADD	f2, f2, f10
	FADD	f3, f3, f11

	FMUL	f16, f31, f1
	FMUL	f17, f31, f0
	FMUL	f18, f31, f3
	FMUL	f19, f31, f2

	FMSUB	f0,  f30, f0, f16
	FMADD	f1,  f30, f1, f17
	FMADD	f2,  f30, f2, f18
	FMADD	f3,  f30, f3, f19

#endif

	STFD	f0,  0 * SIZE(CO1)
	STFD	f1,  1 * SIZE(CO1)
	STFD	f2,  2 * SIZE(CO1)
	STFD	f3,  3 * SIZE(CO1)

	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

	fmr	f8,  f0
	fmr	f9,  f0
	fmr	f10, f0
	fmr	f11, f0

	addi	CO1, CO1, 4 * SIZE

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -2
#else
	addi	TEMP, TEMP, -1
#endif
	slwi	r0,   TEMP, 1 + ZBASE_SHIFT
	slwi	TEMP, TEMP, 0 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 2
#endif
#endif

	addic.	I, I, -1
	bgt	LL(51)
	.align 4

LL(60):
	andi.	I,  M,  1
	ble	LL(999)

#ifndef TRMMKERNEL
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(B)
	LFD	f21,  1 * SIZE(B)
	LFD	f22,  2 * SIZE(B)
	LFD	f23,  3 * SIZE(B)

	srawi.	r0,  K,  2
	mr	BO,  B
	mtspr	CTR, r0
	ble	LL(65)
#else
#if	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(B)
	LFD	f21,  1 * SIZE(B)
	LFD	f22,  2 * SIZE(B)
	LFD	f23,  3 * SIZE(B)

	mr	BO,  B
#else
	slwi	r0,   KK, 0 + ZBASE_SHIFT
	slwi	TEMP, KK, 0 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, B,  TEMP

	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(BO)
	LFD	f21,  1 * SIZE(BO)
	LFD	f22,  2 * SIZE(BO)
	LFD	f23,  3 * SIZE(BO)
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 1
#endif
	srawi.	TEMP,  TEMP,  2
	mtspr	CTR, TEMP
	ble	LL(65)
#endif
	.align 4

LL(62):
	FMA1	f0,  f16, f20, f0
	FMA4	f3,  f17, f20, f3
	FMA2	f1,  f16, f21, f1
	FMA3	f2,  f17, f21, f2

	LFD	f16,  4 * SIZE(AO)
	LFD	f17,  5 * SIZE(AO)
	LFD	f20,  4 * SIZE(BO)
	LFD	f21,  5 * SIZE(BO)

	FMA1	f0,  f18, f22, f0
	FMA4	f3,  f19, f22, f3
	FMA2	f1,  f18, f23, f1
	FMA3	f2,  f19, f23, f2

	LFD	f18,  6 * SIZE(AO)
	LFD	f19,  7 * SIZE(AO)
	LFD	f22,  6 * SIZE(BO)
	LFD	f23,  7 * SIZE(BO)

	FMA1	f0,  f16, f20, f0
	FMA4	f3,  f17, f20, f3
	FMA2	f1,  f16, f21, f1
	FMA3	f2,  f17, f21, f2

	LFD	f16,  8 * SIZE(AO)
	LFD	f17,  9 * SIZE(AO)
	LFD	f20,  8 * SIZE(BO)
	LFD	f21,  9 * SIZE(BO)

	FMA1	f0,  f18, f22, f0
	FMA4	f3,  f19, f22, f3
	FMA2	f1,  f18, f23, f1
	FMA3	f2,  f19, f23, f2

	LFD	f18, 10 * SIZE(AO)
	LFD	f19, 11 * SIZE(AO)
	LFD	f22, 10 * SIZE(BO)
	LFD	f23, 11 * SIZE(BO)

	addi	AO, AO,  8 * SIZE
	addi	BO, BO,  8 * SIZE
	bdnz	LL(62)
	.align 4

LL(65):
	lfd	f30,  ALPHA_R
	lfd	f31,  ALPHA_I

#ifndef TRMMKERNEL
	andi.	r0,  K,  3
	mtspr	CTR, r0
	ble	LL(68)
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 1
#endif
	andi.	TEMP,  TEMP,  3
	mtspr	CTR, TEMP
	ble	LL(68)
#endif
	.align 4

LL(66):
	FMA1	f0,  f16, f20, f0
	FMA4	f3,  f17, f20, f3
	LFD	f20,  2 * SIZE(BO)
	FMA2	f1,  f16, f21, f1
	LFD	f16,  2 * SIZE(AO)
	FMA3	f2,  f17, f21, f2
	LFD	f17,  3 * SIZE(AO)

	LFD	f21,  3 * SIZE(BO)
	addi	AO, AO,  2 * SIZE
	addi	BO, BO,  2 * SIZE
	bdnz	LL(66)
	.align 4

LL(68):
#ifndef TRMMKERNEL
	LFD	f16, 0 * SIZE(CO1)
	LFD	f17, 1 * SIZE(CO1)

	FADD	f0,  f0,  f2
	FADD	f1,  f1,  f3

	FNMSUB	f24, f31, f1, f16
	FMADD	f25, f31, f0, f17

	FMADD	f0,  f30, f0, f24
	FMADD	f1,  f30, f1, f25

#else

	FADD	f0,  f0,  f2
	FADD	f1,  f1,  f3

	FMUL	f16, f31, f1
	FMUL	f17, f31, f0

	FMSUB	f0,  f30, f0, f16
	FMADD	f1,  f30, f1, f17

#endif

	STFD	f0,  0 * SIZE(CO1)
	STFD	f1,  1 * SIZE(CO1)

	addi	CO1, CO1, 2 * SIZE

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -1
#else
	addi	TEMP, TEMP, -1
#endif
	slwi	r0,   TEMP, 0 + ZBASE_SHIFT
	slwi	TEMP, TEMP, 0 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 1
#endif
#endif
	.align 4

LL(999):
	addi	r3, 0, 0

	lfd	f14,    0(SP)
	lfd	f15,    8(SP)
	lfd	f16,   16(SP)
	lfd	f17,   24(SP)

	lfd	f18,   32(SP)
	lfd	f19,   40(SP)
	lfd	f20,   48(SP)
	lfd	f21,   56(SP)

	lfd	f22,   64(SP)
	lfd	f23,   72(SP)
	lfd	f24,   80(SP)
	lfd	f25,   88(SP)

	lfd	f26,   96(SP)
	lfd	f27,  104(SP)
	lfd	f28,  112(SP)
	lfd	f29,  120(SP)

	lfd	f30,  128(SP)
	lfd	f31,  136(SP)

#ifdef __64BIT__
	ld	r31,  144(SP)
	ld	r30,  152(SP)
	ld	r29,  160(SP)
	ld	r28,  168(SP)
	ld	r27,  176(SP)
	ld	r26,  184(SP)
	ld	r25,  192(SP)
	ld	r24,  200(SP)
	ld	r23,  208(SP)
	ld	r22,  216(SP)
	ld	r21,  224(SP)
#ifdef TRMMKERNEL
	ld	r20,  232(SP)
	ld	r19,  240(SP)
#endif
#else
	lwz	r31,  144(SP)
	lwz	r30,  148(SP)
	lwz	r29,  152(SP)
	lwz	r28,  156(SP)
	lwz	r27,  160(SP)
	lwz	r26,  164(SP)
	lwz	r25,  168(SP)
	lwz	r24,  172(SP)
	lwz	r23,  176(SP)
	lwz	r22,  180(SP)
	lwz	r21,  184(SP)
#ifdef TRMMKERNEL
	lwz	r20,  188(SP)
	lwz	r19,  192(SP)
#endif
#endif

	addi	SP, SP, STACKSIZE

	blr

	EPILOGUE
#endif
