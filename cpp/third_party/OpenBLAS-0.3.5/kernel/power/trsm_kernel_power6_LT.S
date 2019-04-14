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
#define ALPHA   296(SP)
#define FZERO	304(SP)
#else
#define STACKSIZE 240
#define ALPHA   224(SP)
#define FZERO	232(SP)
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
#define A	r7
#define	B	r8
#define	C	r9
#define	LDC	r10
#define OFFSET	r6
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
#define A	r8
#define	B	r9
#define	C	r10
#define	LDC	r7
#define OFFSET	r6
#else
#define A	r7
#define	B	r8
#define	C	r9
#define	LDC	r10
#define OFFSET	r6
#endif
#endif

#define AORIG	r18
#define TEMP	r19
#define BB	r20
#define KK	r21
#define	I	r22
#define J	r23
#define AO	r24
#define	BO	r25
#define	CO1	r26
#define CO2	r27
#define	CO3	r28
#define	CO4	r29

#define PREA	r30
#define PREC	r31

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
	std	r20,  232(SP)
	std	r19,  240(SP)
	std	r18,  248(SP)
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
	stw	r20,  188(SP)
	stw	r19,  192(SP)
	stw	r18,  196(SP)
#endif

	stw	r0,  FZERO

#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
	lwz	LDC, FRAMESLOT(0) + STACKSIZE(SP)
#endif
#endif

	slwi	LDC, LDC, BASE_SHIFT

#if defined(linux) && defined(__64BIT__)
	ld	OFFSET,  FRAMESLOT(0) + STACKSIZE(SP)
#endif

#if defined(_AIX) || defined(__APPLE__)
#ifdef __64BIT__
	ld	OFFSET,  FRAMESLOT(0) + STACKSIZE(SP)
#else
#ifdef DOUBLE
	lwz	OFFSET,  FRAMESLOT(1) + STACKSIZE(SP)
#else
	lwz	OFFSET,  FRAMESLOT(0) + STACKSIZE(SP)
#endif
#endif
#endif

#ifdef LN
	mullw	r0, M, K
	slwi	r0, r0, BASE_SHIFT
	add	A, A, r0

	slwi	r0, M, BASE_SHIFT
	add	C, C, r0
#endif

#ifdef RN
	neg	KK, OFFSET
#endif

#ifdef RT
	mullw	r0, N, K
	slwi	r0, r0, BASE_SHIFT
	add	B, B, r0

	mullw	r0, N, LDC
	add	C, C, r0

	sub	KK, N, OFFSET
#endif

	cmpwi	cr0, M, 0
	ble	LL(999)
	cmpwi	cr0, N, 0
	ble	LL(999)
	cmpwi	cr0, K, 0
	ble	LL(999)

	li	PREA,   (16 *  3 * SIZE)
	li	PREC,   4 * SIZE
	lfs	f0, FZERO

	srawi.	J, N,  2
	ble	LL(40)
	.align 4

LL(10):

#ifdef RT
	slwi	r0, K, 2 + BASE_SHIFT
	sub	B, B, r0

	slwi	r0, LDC, 2
	sub	C, C, r0
#endif

	mr	CO1, C
	add	CO2, C,  LDC
	add	CO3, CO2, LDC
	add	CO4, CO3, LDC

#ifdef LN
	add	KK, M, OFFSET
#endif

#ifdef LT
	mr	KK, OFFSET
#endif

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

	srawi.	I, M,  2

#if defined(LN) || defined(RT)
	mr	AORIG, A
#else
	mr	AO, A
#endif
#ifndef RT
	add	C,  CO4, LDC
#endif
	ble	LL(20)
	.align 4

LL(11):
#if defined(LT) || defined(RN)
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(B)
	LFD	f21,  1 * SIZE(B)
	LFD	f22,  2 * SIZE(B)
	LFD	f23,  3 * SIZE(B)

	dcbtst	CO1, PREC
	dcbtst	CO2, PREC
	dcbtst	CO3, PREC
	dcbtst	CO4, PREC

	srawi.	r0, KK,  3
	mtspr	CTR, r0
	mr	BO,  B
#else

#ifdef LN
	slwi	r0,   K,  2 + BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	TEMP, KK, 2 + BASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO, B,     TEMP

	sub	TEMP, K, KK

	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(BO)
	LFD	f21,  1 * SIZE(BO)
	LFD	f22,  2 * SIZE(BO)
	LFD	f23,  3 * SIZE(BO)

	dcbtst	CO1, PREC
	dcbtst	CO2, PREC
	dcbtst	CO3, PREC
	dcbtst	CO4, PREC

	srawi.	r0, TEMP,  3
	mtspr	CTR, r0
#endif
	ble	LL(15)
	.align 4

LL(12):
	dcbt	AO, PREA
	dcbtst	BO, PREA

	FMADD	f0,  f16, f20, f0
	FMADD	f4,  f16, f21, f4
	FMADD	f8,  f16, f22, f8
	FMADD	f12, f16, f23, f12

	LFD	f24,  4 * SIZE(AO)
	LFD	f28,  4 * SIZE(BO)
	LFD	f25,  5 * SIZE(AO)
	LFD	f29,  5 * SIZE(BO)

	FMADD	f1,  f17, f20, f1
	FMADD	f5,  f17, f21, f5
	FMADD	f9,  f17, f22, f9
	FMADD	f13, f17, f23, f13

	FMADD	f2,  f18, f20, f2
	FMADD	f6,  f18, f21, f6
	FMADD	f10, f18, f22, f10
	FMADD	f14, f18, f23, f14

	LFD	f26,  6 * SIZE(AO)
	LFD	f30,  6 * SIZE(BO)
	LFD	f27,  7 * SIZE(AO)
	LFD	f31,  7 * SIZE(BO)

	FMADD	f3,  f19, f20, f3
	FMADD	f7,  f19, f21, f7
	FMADD	f11, f19, f22, f11
	FMADD	f15, f19, f23, f15

	FMADD	f0,  f24, f28, f0
	FMADD	f4,  f24, f29, f4
	FMADD	f8,  f24, f30, f8
	FMADD	f12, f24, f31, f12

	LFD	f16,  8 * SIZE(AO)
 	LFD	f20,  8 * SIZE(BO)
	LFD	f17,  9 * SIZE(AO)
	LFD	f21,  9 * SIZE(BO)

	FMADD	f1,  f25, f28, f1
	FMADD	f5,  f25, f29, f5
	FMADD	f9,  f25, f30, f9
	FMADD	f13, f25, f31, f13

	FMADD	f2,  f26, f28, f2
	FMADD	f6,  f26, f29, f6
	FMADD	f10, f26, f30, f10
	FMADD	f14, f26, f31, f14

	LFD	f18, 10 * SIZE(AO)
	LFD	f22, 10 * SIZE(BO)
	LFD	f19, 11 * SIZE(AO)
	LFD	f23, 11 * SIZE(BO)

	FMADD	f3,  f27, f28, f3
	FMADD	f7,  f27, f29, f7
	FMADD	f11, f27, f30, f11
	FMADD	f15, f27, f31, f15

	FMADD	f0,  f16, f20, f0
	FMADD	f4,  f16, f21, f4
	FMADD	f8,  f16, f22, f8
	FMADD	f12, f16, f23, f12

	LFD	f24, 12 * SIZE(AO)
	LFD	f28, 12 * SIZE(BO)
	LFD	f25, 13 * SIZE(AO)
	LFD	f29, 13 * SIZE(BO)

	FMADD	f1,  f17, f20, f1
	FMADD	f5,  f17, f21, f5
	FMADD	f9,  f17, f22, f9
	FMADD	f13, f17, f23, f13

	FMADD	f2,  f18, f20, f2
	FMADD	f6,  f18, f21, f6
	FMADD	f10, f18, f22, f10
	FMADD	f14, f18, f23, f14

	LFD	f26, 14 * SIZE(AO)
	LFD	f30, 14 * SIZE(BO)
	LFD	f27, 15 * SIZE(AO)
	LFD	f31, 15 * SIZE(BO)

	FMADD	f3,  f19, f20, f3
	FMADD	f7,  f19, f21, f7
	FMADD	f11, f19, f22, f11
	FMADD	f15, f19, f23, f15

	FMADD	f0,  f24, f28, f0
	FMADD	f4,  f24, f29, f4
	FMADD	f8,  f24, f30, f8
	FMADD	f12, f24, f31, f12

	LFD	f16, 16 * SIZE(AO)
	LFD	f20, 16 * SIZE(BO)
	LFD	f17, 17 * SIZE(AO)
	LFD	f21, 17 * SIZE(BO)

	FMADD	f1,  f25, f28, f1
	FMADD	f5,  f25, f29, f5
	FMADD	f9,  f25, f30, f9
	FMADD	f13, f25, f31, f13

	FMADD	f2,  f26, f28, f2
	FMADD	f6,  f26, f29, f6
	FMADD	f10, f26, f30, f10
	FMADD	f14, f26, f31, f14

	LFD	f18, 18 * SIZE(AO)
	LFD	f22, 18 * SIZE(BO)
	LFD	f19, 19 * SIZE(AO)
	LFD	f23, 19 * SIZE(BO)

	FMADD	f3,  f27, f28, f3
	FMADD	f7,  f27, f29, f7
	FMADD	f11, f27, f30, f11
	FMADD	f15, f27, f31, f15

	FMADD	f0,  f16, f20, f0
	FMADD	f4,  f16, f21, f4
	FMADD	f8,  f16, f22, f8
	FMADD	f12, f16, f23, f12

	LFD	f24, 20 * SIZE(AO)
	LFD	f28, 20 * SIZE(BO)
	LFD	f25, 21 * SIZE(AO)
	LFD	f29, 21 * SIZE(BO)

	FMADD	f1,  f17, f20, f1
	FMADD	f5,  f17, f21, f5
	FMADD	f9,  f17, f22, f9
	FMADD	f13, f17, f23, f13

	FMADD	f2,  f18, f20, f2
	FMADD	f6,  f18, f21, f6
	FMADD	f10, f18, f22, f10
	FMADD	f14, f18, f23, f14

	LFD	f26, 22 * SIZE(AO)
	LFD	f30, 22 * SIZE(BO)
	LFD	f27, 23 * SIZE(AO)
	LFD	f31, 23 * SIZE(BO)

	FMADD	f3,  f19, f20, f3
	FMADD	f7,  f19, f21, f7
	FMADD	f11, f19, f22, f11
	FMADD	f15, f19, f23, f15

	FMADD	f0,  f24, f28, f0
	FMADD	f4,  f24, f29, f4
	FMADD	f8,  f24, f30, f8
	FMADD	f12, f24, f31, f12

	LFD	f16, 24 * SIZE(AO)
 	LFD	f20, 24 * SIZE(BO)
	LFD	f17, 25 * SIZE(AO)
	LFD	f21, 25 * SIZE(BO)

	FMADD	f1,  f25, f28, f1
	FMADD	f5,  f25, f29, f5
	FMADD	f9,  f25, f30, f9
	FMADD	f13, f25, f31, f13

	FMADD	f2,  f26, f28, f2
	FMADD	f6,  f26, f29, f6
	FMADD	f10, f26, f30, f10
	FMADD	f14, f26, f31, f14

	LFD	f18, 26 * SIZE(AO)
	LFD	f22, 26 * SIZE(BO)
	LFD	f19, 27 * SIZE(AO)
	LFD	f23, 27 * SIZE(BO)

	FMADD	f3,  f27, f28, f3
	FMADD	f7,  f27, f29, f7
	FMADD	f11, f27, f30, f11
	FMADD	f15, f27, f31, f15

	FMADD	f0,  f16, f20, f0
	FMADD	f4,  f16, f21, f4
	FMADD	f8,  f16, f22, f8
	FMADD	f12, f16, f23, f12

	LFD	f24, 28 * SIZE(AO)
	LFD	f28, 28 * SIZE(BO)
	LFD	f25, 29 * SIZE(AO)
	LFD	f29, 29 * SIZE(BO)

	FMADD	f1,  f17, f20, f1
	FMADD	f5,  f17, f21, f5
	FMADD	f9,  f17, f22, f9
	FMADD	f13, f17, f23, f13

	FMADD	f2,  f18, f20, f2
	FMADD	f6,  f18, f21, f6
	FMADD	f10, f18, f22, f10
	FMADD	f14, f18, f23, f14

	LFD	f26, 30 * SIZE(AO)
	LFD	f30, 30 * SIZE(BO)
	LFD	f27, 31 * SIZE(AO)
	LFD	f31, 31 * SIZE(BO)

	FMADD	f3,  f19, f20, f3
	FMADD	f7,  f19, f21, f7
	FMADD	f11, f19, f22, f11
	FMADD	f15, f19, f23, f15

	FMADD	f0,  f24, f28, f0
	FMADD	f4,  f24, f29, f4
	FMADD	f8,  f24, f30, f8
	FMADD	f12, f24, f31, f12

	LFD	f16, 32 * SIZE(AO)
	LFD	f20, 32 * SIZE(BO)
	LFD	f17, 33 * SIZE(AO)
	LFD	f21, 33 * SIZE(BO)

	FMADD	f1,  f25, f28, f1
	FMADD	f5,  f25, f29, f5
	FMADD	f9,  f25, f30, f9
	FMADD	f13, f25, f31, f13

	FMADD	f2,  f26, f28, f2
	FMADD	f6,  f26, f29, f6
	FMADD	f10, f26, f30, f10
	FMADD	f14, f26, f31, f14

	LFD	f18, 34 * SIZE(AO)
	LFD	f22, 34 * SIZE(BO)
	LFD	f19, 35 * SIZE(AO)
	LFD	f23, 35 * SIZE(BO)

	addi	AO, AO, 32 * SIZE
	addi	BO, BO, 32 * SIZE

	FMADD	f3,  f27, f28, f3
	FMADD	f7,  f27, f29, f7
	FMADD	f11, f27, f30, f11
	FMADD	f15, f27, f31, f15

	bdnz	LL(12)
	.align 4

LL(15):
#if defined(LT) || defined(RN)
	andi.	r0, KK,  7
#else
	andi.	r0, TEMP, 7
#endif
	mtspr	CTR, r0
	ble+	LL(18)
	.align 4

LL(16):
	FMADD	f0,  f16, f20, f0
	FMADD	f4,  f16, f21, f4
	FMADD	f8,  f16, f22, f8
	FMADD	f12, f16, f23, f12

	FMADD	f1,  f17, f20, f1
	FMADD	f5,  f17, f21, f5
	FMADD	f9,  f17, f22, f9
	FMADD	f13, f17, f23, f13

	FMADD	f2,  f18, f20, f2
	FMADD	f6,  f18, f21, f6
	FMADD	f10, f18, f22, f10
	FMADD	f14, f18, f23, f14

	FMADD	f3,  f19, f20, f3
	FMADD	f7,  f19, f21, f7
	FMADD	f11, f19, f22, f11
	FMADD	f15, f19, f23, f15

	LFD	f16,  4 * SIZE(AO)
	LFD	f17,  5 * SIZE(AO)
	LFD	f18,  6 * SIZE(AO)
	LFD	f19,  7 * SIZE(AO)

	LFD	f20,  4 * SIZE(BO)
	LFD	f21,  5 * SIZE(BO)
	LFD	f22,  6 * SIZE(BO)
	LFD	f23,  7 * SIZE(BO)

	addi	BO, BO,  4 * SIZE
	addi	AO, AO,  4 * SIZE
	bdnz	LL(16)
	.align 4

LL(18):
#if defined(LN) || defined(RT)
	subi	r0, KK, 4
	slwi	r0, r0, 2 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     r0
#endif

#if defined(LN) || defined(LT)
	LFD	f16,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(BO)
	LFD	f18,  2 * SIZE(BO)
	LFD	f19,  3 * SIZE(BO)

	LFD	f20,  4 * SIZE(BO)
 	LFD	f21,  5 * SIZE(BO)
	LFD	f22,  6 * SIZE(BO)
	LFD	f23,  7 * SIZE(BO)

	LFD	f24,  8 * SIZE(BO)
	LFD	f25,  9 * SIZE(BO)
	LFD	f26, 10 * SIZE(BO)
	LFD	f27, 11 * SIZE(BO)

	LFD	f28, 12 * SIZE(BO)
	LFD	f29, 13 * SIZE(BO)
	LFD	f30, 14 * SIZE(BO)
	LFD	f31, 15 * SIZE(BO)

	FSUB	f0,  f16, f0
	FSUB	f4,  f17, f4
	FSUB	f8,  f18, f8
	FSUB	f12, f19, f12

	FSUB	f1,  f20, f1
	FSUB	f5,  f21, f5
	FSUB	f9,  f22, f9
	FSUB	f13, f23, f13

	FSUB	f2,  f24, f2
	FSUB	f6,  f25, f6
	FSUB	f10, f26, f10
	FSUB	f14, f27, f14

	FSUB	f3,  f28, f3
	FSUB	f7,  f29, f7
	FSUB	f11, f30, f11
	FSUB	f15, f31, f15
#else
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  4 * SIZE(AO)
 	LFD	f21,  5 * SIZE(AO)
	LFD	f22,  6 * SIZE(AO)
	LFD	f23,  7 * SIZE(AO)

	LFD	f24,  8 * SIZE(AO)
	LFD	f25,  9 * SIZE(AO)
	LFD	f26, 10 * SIZE(AO)
	LFD	f27, 11 * SIZE(AO)

	LFD	f28, 12 * SIZE(AO)
	LFD	f29, 13 * SIZE(AO)
	LFD	f30, 14 * SIZE(AO)
	LFD	f31, 15 * SIZE(AO)

	FSUB	f0,  f16, f0
	FSUB	f1,  f17, f1
	FSUB	f2,  f18, f2
	FSUB	f3,  f19, f3

	FSUB	f4,  f20, f4
	FSUB	f5,  f21, f5
	FSUB	f6,  f22, f6
	FSUB	f7,  f23, f7

	FSUB	f8,  f24, f8
	FSUB	f9,  f25, f9
	FSUB	f10, f26, f10
	FSUB	f11, f27, f11

	FSUB	f12, f28, f12
	FSUB	f13, f29, f13
	FSUB	f14, f30, f14
	FSUB	f15, f31, f15
#endif

#ifdef LN
	LFD	f16, 15 * SIZE(AO)
	LFD	f17, 14 * SIZE(AO)
	LFD	f18, 13 * SIZE(AO)
	LFD	f19, 12 * SIZE(AO)

	FMUL	f3,  f16, f3
	FMUL	f7,  f16, f7
	FMUL	f11, f16, f11
	FMUL	f15, f16, f15

	FNMSUB	f2,  f17, f3,  f2
	FNMSUB	f6,  f17, f7,  f6
	FNMSUB	f10, f17, f11, f10
	FNMSUB	f14, f17, f15, f14

	FNMSUB	f1,  f18, f3,  f1
	FNMSUB	f5,  f18, f7,  f5
	FNMSUB	f9,  f18, f11, f9
	FNMSUB	f13, f18, f15, f13

	FNMSUB	f0,  f19, f3,  f0
	FNMSUB	f4,  f19, f7,  f4
	FNMSUB	f8,  f19, f11, f8
	FNMSUB	f12, f19, f15, f12

	LFD	f16, 10 * SIZE(AO)
	LFD	f17,  9 * SIZE(AO)
	LFD	f18,  8 * SIZE(AO)
	LFD	f19,  5 * SIZE(AO)

	FMUL	f2,  f16, f2
	FMUL	f6,  f16, f6
	FMUL	f10, f16, f10
	FMUL	f14, f16, f14

	LFD	f20,  4 * SIZE(AO)
	LFD	f21,  0 * SIZE(AO)

	FNMSUB	f1,  f17, f2,  f1
	FNMSUB	f5,  f17, f6,  f5
	FNMSUB	f9,  f17, f10, f9
	FNMSUB	f13, f17, f14,  f13

	FNMSUB	f0,  f18, f2,  f0
	FNMSUB	f4,  f18, f6,  f4
	FNMSUB	f8,  f18, f10, f8
	FNMSUB	f12, f18, f14, f12

	FMUL	f1,  f19, f1
	FMUL	f5,  f19, f5
	FMUL	f9,  f19, f9
	FMUL	f13, f19, f13

	FNMSUB	f0,  f20, f1,  f0
	FNMSUB	f4,  f20, f5,  f4
	FNMSUB	f8,  f20, f9,  f8
	FNMSUB	f12, f20, f13, f12

	FMUL	f0,  f21, f0
	FMUL	f4,  f21, f4
	FMUL	f8,  f21, f8
	FMUL	f12, f21, f12
#endif

#ifdef LT
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	FMUL	f0,  f16, f0
	FMUL	f4,  f16, f4
	FMUL	f8,  f16, f8
	FMUL	f12, f16, f12

	FNMSUB	f1,  f17, f0,  f1
	FNMSUB	f5,  f17, f4,  f5
	FNMSUB	f9,  f17, f8,  f9
	FNMSUB	f13, f17, f12, f13

	FNMSUB	f2,  f18, f0,  f2
	FNMSUB	f6,  f18, f4,  f6
	FNMSUB	f10, f18, f8,  f10
	FNMSUB	f14, f18, f12, f14

	FNMSUB	f3,  f19, f0,  f3
	FNMSUB	f7,  f19, f4,  f7
	FNMSUB	f11, f19, f8,  f11
	FNMSUB	f15, f19, f12, f15

	LFD	f16,  5 * SIZE(AO)
	LFD	f17,  6 * SIZE(AO)
	LFD	f18,  7 * SIZE(AO)
	LFD	f19, 10 * SIZE(AO)

	FMUL	f1,  f16, f1
	FMUL	f5,  f16, f5
	FMUL	f9,  f16, f9
	FMUL	f13, f16, f13

	LFD	f20, 11 * SIZE(AO)
	LFD	f21, 15 * SIZE(AO)

	FNMSUB	f2,  f17, f1,  f2
	FNMSUB	f6,  f17, f5,  f6
	FNMSUB	f10, f17, f9,  f10
	FNMSUB	f14, f17, f13, f14

	FNMSUB	f3,  f18, f1,  f3
	FNMSUB	f7,  f18, f5,  f7
	FNMSUB	f11, f18, f9,  f11
	FNMSUB	f15, f18, f13, f15

	FMUL	f2,  f19, f2
	FMUL	f6,  f19, f6
	FMUL	f10, f19, f10
	FMUL	f14, f19, f14

	FNMSUB	f3,  f20, f2,  f3
	FNMSUB	f7,  f20, f6,  f7
	FNMSUB	f11, f20, f10, f11
	FNMSUB	f15, f20, f14, f15

	FMUL	f3,  f21, f3
	FMUL	f7,  f21, f7
	FMUL	f11, f21, f11
	FMUL	f15, f21, f15
#endif

#ifdef RN
	LFD	f16,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(BO)
	LFD	f18,  2 * SIZE(BO)
	LFD	f19,  3 * SIZE(BO)

	FMUL	f0,  f16, f0
	FMUL	f1,  f16, f1
	FMUL	f2,  f16, f2
	FMUL	f3,  f16, f3

	FNMSUB	f4,  f17, f0,  f4
	FNMSUB	f5,  f17, f1,  f5
	FNMSUB	f6,  f17, f2,  f6
	FNMSUB	f7,  f17, f3,  f7

	FNMSUB	f8,  f18, f0,  f8
	FNMSUB	f9,  f18, f1,  f9
	FNMSUB	f10, f18, f2,  f10
	FNMSUB	f11, f18, f3,  f11

	FNMSUB	f12, f19, f0,  f12
	FNMSUB	f13, f19, f1,  f13
	FNMSUB	f14, f19, f2,  f14
	FNMSUB	f15, f19, f3,  f15

	LFD	f16,  5 * SIZE(BO)
	LFD	f17,  6 * SIZE(BO)
	LFD	f18,  7 * SIZE(BO)
	LFD	f19, 10 * SIZE(BO)

	FMUL	f4,  f16, f4
	FMUL	f5,  f16, f5
	FMUL	f6,  f16, f6
	FMUL	f7,  f16, f7

	LFD	f20, 11 * SIZE(BO)
	LFD	f21, 15 * SIZE(BO)

	FNMSUB	f8,  f17, f4,  f8
	FNMSUB	f9,  f17, f5,  f9
	FNMSUB	f10, f17, f6,  f10
	FNMSUB	f11, f17, f7,  f11

	FNMSUB	f12, f18, f4,  f12
	FNMSUB	f13, f18, f5,  f13
	FNMSUB	f14, f18, f6,  f14
	FNMSUB	f15, f18, f7,  f15

	FMUL	f8,  f19, f8
	FMUL	f9,  f19, f9
	FMUL	f10, f19, f10
	FMUL	f11, f19, f11

	FNMSUB	f12, f20, f8,  f12
	FNMSUB	f13, f20, f9,  f13
	FNMSUB	f14, f20, f10, f14
	FNMSUB	f15, f20, f11, f15

	FMUL	f12, f21, f12
	FMUL	f13, f21, f13
	FMUL	f14, f21, f14
	FMUL	f15, f21, f15
#endif

#ifdef RT
	LFD	f16, 15 * SIZE(BO)
	LFD	f17, 14 * SIZE(BO)
	LFD	f18, 13 * SIZE(BO)
	LFD	f19, 12 * SIZE(BO)

	FMUL	f12, f16, f12
	FMUL	f13, f16, f13
	FMUL	f14, f16, f14
	FMUL	f15, f16, f15

	FNMSUB	f8,  f17, f12, f8
	FNMSUB	f9,  f17, f13, f9
	FNMSUB	f10, f17, f14, f10
	FNMSUB	f11, f17, f15, f11

	FNMSUB	f4,  f18, f12, f4
	FNMSUB	f5,  f18, f13, f5
	FNMSUB	f6,  f18, f14, f6
	FNMSUB	f7,  f18, f15, f7

	FNMSUB	f0,  f19, f12, f0
	FNMSUB	f1,  f19, f13, f1
	FNMSUB	f2,  f19, f14, f2
	FNMSUB	f3,  f19, f15, f3

	LFD	f16, 10 * SIZE(BO)
	LFD	f17,  9 * SIZE(BO)
	LFD	f18,  8 * SIZE(BO)
	LFD	f19,  5 * SIZE(BO)

	FMUL	f8,  f16, f8
	FMUL	f9,  f16, f9
	FMUL	f10, f16, f10
	FMUL	f11, f16, f11

	LFD	f20,  4 * SIZE(BO)
	LFD	f21,  0 * SIZE(BO)

	FNMSUB	f4,  f17, f8,  f4
	FNMSUB	f5,  f17, f9,  f5
	FNMSUB	f6,  f17, f10, f6
	FNMSUB	f7,  f17, f11, f7

	FNMSUB	f0,  f18, f8,  f0
	FNMSUB	f1,  f18, f9,  f1
	FNMSUB	f2,  f18, f10, f2
	FNMSUB	f3,  f18, f11, f3

	FMUL	f4,  f19, f4
	FMUL	f5,  f19, f5
	FMUL	f6,  f19, f6
	FMUL	f7,  f19, f7

	FNMSUB	f0,  f20, f4,  f0
	FNMSUB	f1,  f20, f5,  f1
	FNMSUB	f2,  f20, f6,  f2
	FNMSUB	f3,  f20, f7,  f3

	FMUL	f0,  f21, f0
	FMUL	f1,  f21, f1
	FMUL	f2,  f21, f2
	FMUL	f3,  f21, f3
#endif

#ifdef LN
	subi	CO1, CO1, 4 * SIZE
	subi	CO2, CO2, 4 * SIZE
	subi	CO3, CO3, 4 * SIZE
	subi	CO4, CO4, 4 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFD	f0,   0 * SIZE(BO)
	STFD	f4,   1 * SIZE(BO)
	STFD	f8,   2 * SIZE(BO)
	STFD	f12,  3 * SIZE(BO)

	STFD	f1,   4 * SIZE(BO)
	STFD	f5,   5 * SIZE(BO)
	STFD	f9,   6 * SIZE(BO)
	STFD	f13,  7 * SIZE(BO)

	STFD	f2,   8 * SIZE(BO)
	STFD	f6,   9 * SIZE(BO)
	STFD	f10, 10 * SIZE(BO)
	STFD	f14, 11 * SIZE(BO)

	STFD	f3,  12 * SIZE(BO)
	STFD	f7,  13 * SIZE(BO)
	STFD	f11, 14 * SIZE(BO)
	STFD	f15, 15 * SIZE(BO)
#else
	STFD	f0,   0 * SIZE(AO)
	STFD	f1,   1 * SIZE(AO)
	STFD	f2,   2 * SIZE(AO)
	STFD	f3,   3 * SIZE(AO)

	STFD	f4,   4 * SIZE(AO)
	STFD	f5,   5 * SIZE(AO)
	STFD	f6,   6 * SIZE(AO)
	STFD	f7,   7 * SIZE(AO)

	STFD	f8,   8 * SIZE(AO)
	STFD	f9,   9 * SIZE(AO)
	STFD	f10, 10 * SIZE(AO)
	STFD	f11, 11 * SIZE(AO)

	STFD	f12, 12 * SIZE(AO)
	STFD	f13, 13 * SIZE(AO)
	STFD	f14, 14 * SIZE(AO)
	STFD	f15, 15 * SIZE(AO)
#endif

	STFD	f0,   0 * SIZE(CO1)
	STFD	f1,   1 * SIZE(CO1)
	STFD	f2,   2 * SIZE(CO1)
	STFD	f3,   3 * SIZE(CO1)

	STFD	f4,   0 * SIZE(CO2)
	STFD	f5,   1 * SIZE(CO2)
	STFD	f6,   2 * SIZE(CO2)
	STFD	f7,   3 * SIZE(CO2)

	STFD	f8,   0 * SIZE(CO3)
	STFD	f9,   1 * SIZE(CO3)
	STFD	f10,  2 * SIZE(CO3)
	STFD	f11,  3 * SIZE(CO3)

	STFD	f12,  0 * SIZE(CO4)
	STFD	f13,  1 * SIZE(CO4)
	STFD	f14,  2 * SIZE(CO4)
	STFD	f15,  3 * SIZE(CO4)

	lfs	f0,  FZERO
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

#ifndef LN
	addi	CO1, CO1, 4 * SIZE
	addi	CO2, CO2, 4 * SIZE
	addi	CO3, CO3, 4 * SIZE
	addi	CO4, CO4, 4 * SIZE
#endif

#ifdef RT
	slwi	r0, K, 2 + BASE_SHIFT
	add	AORIG, AORIG, r0
#endif

#if defined(LT) || defined(RN)
	sub	TEMP, K, KK
	slwi	TEMP, TEMP, 2 + BASE_SHIFT
	add	AO, AO, TEMP
	add	BO, BO, TEMP
#endif

#ifdef LT
	addi	KK, KK, 4
#endif

#ifdef LN
	subi	KK, KK, 4
#endif

	addic.	I, I, -1
	bgt+	LL(11)
	.align 4

LL(20):
	andi.	I,  M,  2
	ble	LL(30)

#if defined(LT) || defined(RN)
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

	srawi.	r0, KK,  2
	mtspr	CTR, r0
	mr	BO,  B
#else

#ifdef LN
	slwi	r0,   K,  1 + BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0,   KK, 1 + BASE_SHIFT
	slwi	TEMP, KK, 2 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

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

	srawi.	r0, TEMP,  2
	mtspr	CTR, r0
#endif
	ble	LL(25)
	.align 5

LL(22):
	FMADD	f0,  f16, f20, f0
	FMADD	f1,  f17, f20, f1
	FMADD	f4,  f16, f21, f4
	FMADD	f5,  f17, f21, f5

	FMADD	f8,  f16, f22, f8
	FMADD	f9,  f17, f22, f9
	FMADD	f12, f16, f23, f12
	FMADD	f13, f17, f23, f13

	LFD	f20,  8 * SIZE(BO)
	LFD	f21,  9 * SIZE(BO)
	LFD	f22, 10 * SIZE(BO)
	LFD	f23, 11 * SIZE(BO)

	FMADD	f2,  f18, f24, f2
	FMADD	f3,  f19, f24, f3
	FMADD	f6,  f18, f25, f6
	FMADD	f7,  f19, f25, f7

	FMADD	f10, f18, f26, f10
	FMADD	f11, f19, f26, f11
	FMADD	f14, f18, f27, f14
	FMADD	f15, f19, f27, f15

	LFD	f16,  4 * SIZE(AO)
	LFD	f17,  5 * SIZE(AO)
	LFD	f18,  6 * SIZE(AO)
	LFD	f19,  7 * SIZE(AO)

	FMADD	f0,  f16, f20, f0
	FMADD	f1,  f17, f20, f1
	FMADD	f4,  f16, f21, f4
	FMADD	f5,  f17, f21, f5

	LFD	f24, 12 * SIZE(BO)
	LFD	f25, 13 * SIZE(BO)
	LFD	f26, 14 * SIZE(BO)
	LFD	f27, 15 * SIZE(BO)

	FMADD	f8,  f16, f22, f8
	FMADD	f9,  f17, f22, f9
	FMADD	f12, f16, f23, f12
	FMADD	f13, f17, f23, f13

	LFD	f20, 16 * SIZE(BO)
	LFD	f21, 17 * SIZE(BO)
	LFD	f22, 18 * SIZE(BO)
	LFD	f23, 19 * SIZE(BO)

	FMADD	f2,  f18, f24, f2
	FMADD	f3,  f19, f24, f3
	FMADD	f6,  f18, f25, f6
	FMADD	f7,  f19, f25, f7

	FMADD	f10, f18, f26, f10
	FMADD	f11, f19, f26, f11
	FMADD	f14, f18, f27, f14
	FMADD	f15, f19, f27, f15

	LFD	f16,  8 * SIZE(AO)
	LFD	f17,  9 * SIZE(AO)
	LFD	f18, 10 * SIZE(AO)
	LFD	f19, 11 * SIZE(AO)

	LFD	f24, 20 * SIZE(BO)
	LFD	f25, 21 * SIZE(BO)
	LFD	f26, 22 * SIZE(BO)
	LFD	f27, 23 * SIZE(BO)

	addi	AO, AO,  8 * SIZE
	addi	BO, BO, 16 * SIZE
	dcbtst	AO, PREA
	bdnz	LL(22)

	fadd	f0,  f2,  f0
	fadd	f1,  f3,  f1
	fadd	f4,  f6,  f4
	fadd	f5,  f7,  f5
	fadd	f8,  f10, f8
	fadd	f9,  f11, f9
	fadd	f12, f14, f12
	fadd	f13, f15, f13
	.align 4

LL(25):
#if defined(LT) || defined(RN)
	andi.	r0, KK,  3
#else
	andi.	r0, TEMP, 3
#endif
	mtspr	CTR, r0
	ble+	LL(28)
	.align 4

LL(26):
	FMADD	f0,  f16, f20, f0
	FMADD	f1,  f17, f20, f1
	FMADD	f4,  f16, f21, f4
	FMADD	f5,  f17, f21, f5

	FMADD	f8,  f16, f22, f8
	FMADD	f9,  f17, f22, f9
	FMADD	f12, f16, f23, f12
	FMADD	f13, f17, f23, f13

	LFD	f16,  2 * SIZE(AO)
	LFD	f17,  3 * SIZE(AO)

	LFD	f20,  4 * SIZE(BO)
	LFD	f21,  5 * SIZE(BO)
	LFD	f22,  6 * SIZE(BO)
	LFD	f23,  7 * SIZE(BO)

	addi	BO, BO,  4 * SIZE
	addi	AO, AO,  2 * SIZE
	bdnz	LL(26)
	.align 4

LL(28):
#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 2
#else
	subi	r0, KK, 4
#endif
	slwi	TEMP, r0, 1 + BASE_SHIFT
	slwi	r0,   r0, 2 + BASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO, B,     r0
#endif

#if defined(LN) || defined(LT)
	LFD	f16,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(BO)
	LFD	f18,  2 * SIZE(BO)
	LFD	f19,  3 * SIZE(BO)

	LFD	f20,  4 * SIZE(BO)
 	LFD	f21,  5 * SIZE(BO)
	LFD	f22,  6 * SIZE(BO)
	LFD	f23,  7 * SIZE(BO)

	FSUB	f0,  f16, f0
	FSUB	f4,  f17, f4
	FSUB	f8,  f18, f8
	FSUB	f12, f19, f12

	FSUB	f1,  f20, f1
	FSUB	f5,  f21, f5
	FSUB	f9,  f22, f9
	FSUB	f13, f23, f13
#else
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f20,  2 * SIZE(AO)
 	LFD	f21,  3 * SIZE(AO)

	LFD	f24,  4 * SIZE(AO)
	LFD	f25,  5 * SIZE(AO)
	LFD	f28,  6 * SIZE(AO)
	LFD	f29,  7 * SIZE(AO)

	FSUB	f0,  f16, f0
	FSUB	f1,  f17, f1
	FSUB	f4,  f20, f4
	FSUB	f5,  f21, f5

	FSUB	f8,  f24, f8
	FSUB	f9,  f25, f9
	FSUB	f12, f28, f12
	FSUB	f13, f29, f13
#endif

#ifdef LN
	LFD	f19,  3 * SIZE(AO)
	LFD	f20,  2 * SIZE(AO)
	LFD	f21,  0 * SIZE(AO)

	FMUL	f1,  f19, f1
	FMUL	f5,  f19, f5
	FMUL	f9,  f19, f9
	FMUL	f13, f19, f13

	FNMSUB	f0,  f20, f1,  f0
	FNMSUB	f4,  f20, f5,  f4
	FNMSUB	f8,  f20, f9,  f8
	FNMSUB	f12, f20, f13, f12

	FMUL	f0,  f21, f0
	FMUL	f4,  f21, f4
	FMUL	f8,  f21, f8
	FMUL	f12, f21, f12
#endif

#ifdef LT
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)

	FMUL	f0,  f16, f0
	FMUL	f4,  f16, f4
	FMUL	f8,  f16, f8
	FMUL	f12, f16, f12

	FNMSUB	f1,  f17, f0,  f1
	FNMSUB	f5,  f17, f4,  f5
	FNMSUB	f9,  f17, f8,  f9
	FNMSUB	f13, f17, f12, f13

	LFD	f17,  3 * SIZE(AO)

	FMUL	f1,  f17, f1
	FMUL	f5,  f17, f5
	FMUL	f9,  f17, f9
	FMUL	f13, f17, f13
#endif

#ifdef RN
	LFD	f16,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(BO)
	LFD	f18,  2 * SIZE(BO)
	LFD	f19,  3 * SIZE(BO)

	FMUL	f0,  f16, f0
	FMUL	f1,  f16, f1
	FNMSUB	f4,  f17, f0,  f4
	FNMSUB	f5,  f17, f1,  f5
	FNMSUB	f8,  f18, f0,  f8
	FNMSUB	f9,  f18, f1,  f9
	FNMSUB	f12, f19, f0,  f12
	FNMSUB	f13, f19, f1,  f13

	LFD	f16,  5 * SIZE(BO)
	LFD	f17,  6 * SIZE(BO)
	LFD	f18,  7 * SIZE(BO)
	LFD	f19, 10 * SIZE(BO)

	LFD	f20, 11 * SIZE(BO)
	LFD	f21, 15 * SIZE(BO)

	FMUL	f4,  f16, f4
	FMUL	f5,  f16, f5
	FNMSUB	f8,  f17, f4,  f8
	FNMSUB	f9,  f17, f5,  f9
	FNMSUB	f12, f18, f4,  f12
	FNMSUB	f13, f18, f5,  f13

	FMUL	f8,  f19, f8
	FMUL	f9,  f19, f9
	FNMSUB	f12, f20, f8,  f12
	FNMSUB	f13, f20, f9,  f13
	FMUL	f12, f21, f12
	FMUL	f13, f21, f13
#endif

#ifdef RT
	LFD	f16, 15 * SIZE(BO)
	LFD	f17, 14 * SIZE(BO)
	LFD	f18, 13 * SIZE(BO)
	LFD	f19, 12 * SIZE(BO)

	FMUL	f12, f16, f12
	FMUL	f13, f16, f13
	FNMSUB	f8,  f17, f12, f8
	FNMSUB	f9,  f17, f13, f9
	FNMSUB	f4,  f18, f12, f4
	FNMSUB	f5,  f18, f13, f5
	FNMSUB	f0,  f19, f12, f0
	FNMSUB	f1,  f19, f13, f1

	LFD	f16, 10 * SIZE(BO)
	LFD	f17,  9 * SIZE(BO)
	LFD	f18,  8 * SIZE(BO)
	LFD	f19,  5 * SIZE(BO)
	LFD	f20,  4 * SIZE(BO)
	LFD	f21,  0 * SIZE(BO)

	FMUL	f8,  f16, f8
	FMUL	f9,  f16, f9
	FNMSUB	f4,  f17, f8,  f4
	FNMSUB	f5,  f17, f9,  f5
	FNMSUB	f0,  f18, f8,  f0
	FNMSUB	f1,  f18, f9,  f1

	FMUL	f4,  f19, f4
	FMUL	f5,  f19, f5
	FNMSUB	f0,  f20, f4,  f0
	FNMSUB	f1,  f20, f5,  f1

	FMUL	f0,  f21, f0
	FMUL	f1,  f21, f1
#endif

#ifdef LN
	subi	CO1, CO1, 2 * SIZE
	subi	CO2, CO2, 2 * SIZE
	subi	CO3, CO3, 2 * SIZE
	subi	CO4, CO4, 2 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFD	f0,   0 * SIZE(BO)
	STFD	f4,   1 * SIZE(BO)
	STFD	f8,   2 * SIZE(BO)
	STFD	f12,  3 * SIZE(BO)

	STFD	f1,   4 * SIZE(BO)
	STFD	f5,   5 * SIZE(BO)
	STFD	f9,   6 * SIZE(BO)
	STFD	f13,  7 * SIZE(BO)
#else
	STFD	f0,   0 * SIZE(AO)
	STFD	f1,   1 * SIZE(AO)
	STFD	f4,   2 * SIZE(AO)
	STFD	f5,   3 * SIZE(AO)

	STFD	f8,   4 * SIZE(AO)
	STFD	f9,   5 * SIZE(AO)
	STFD	f12,  6 * SIZE(AO)
	STFD	f13,  7 * SIZE(AO)
#endif

	STFD	f0,   0 * SIZE(CO1)
	STFD	f1,   1 * SIZE(CO1)
	STFD	f4,   0 * SIZE(CO2)
	STFD	f5,   1 * SIZE(CO2)

	STFD	f8,   0 * SIZE(CO3)
	STFD	f9,   1 * SIZE(CO3)
	STFD	f12,  0 * SIZE(CO4)
	STFD	f13,  1 * SIZE(CO4)

	lfs	f0,  FZERO
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

#ifndef LN
	addi	CO1, CO1, 2 * SIZE
	addi	CO2, CO2, 2 * SIZE
	addi	CO3, CO3, 2 * SIZE
	addi	CO4, CO4, 2 * SIZE
#endif

#ifdef RT
	slwi	r0, K, 1 + BASE_SHIFT
	add	AORIG, AORIG, r0
#endif

#if defined(LT) || defined(RN)
	sub	TEMP, K, KK
	slwi	r0,   TEMP, 1 + BASE_SHIFT
	slwi	TEMP, TEMP, 2 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LN
	subi	KK, KK, 2
#endif

#ifdef LT
	addi	KK, KK, 2
#endif
	.align 4

LL(30):
	andi.	I,  M,  1
	ble	LL(39)

#if defined(LT) || defined(RN)
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

	srawi.	r0, KK,  2
	mtspr	CTR, r0
	mr	BO,  B
#else

#ifdef LN
	slwi	r0,   K,  BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0,   KK, 0 + BASE_SHIFT
	slwi	TEMP, KK, 2 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

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

	srawi.	r0, TEMP,  2
	mtspr	CTR, r0
#endif
	ble	LL(35)
	.align 5

LL(32):
	FMADD	f0,  f16, f20, f0
	FMADD	f4,  f16, f21, f4
	FMADD	f8,  f16, f22, f8
	FMADD	f12, f16, f23, f12

	LFD	f20,  8 * SIZE(BO)
	LFD	f21,  9 * SIZE(BO)
	LFD	f22, 10 * SIZE(BO)
	LFD	f23, 11 * SIZE(BO)

	FMADD	f1,  f17, f24, f1
	FMADD	f5,  f17, f25, f5
	FMADD	f9,  f17, f26, f9
	FMADD	f13, f17, f27, f13

	LFD	f24, 12 * SIZE(BO)
	LFD	f25, 13 * SIZE(BO)
	LFD	f26, 14 * SIZE(BO)
	LFD	f27, 15 * SIZE(BO)

	FMADD	f0,  f18, f20, f0
	FMADD	f4,  f18, f21, f4
	FMADD	f8,  f18, f22, f8
	FMADD	f12, f18, f23, f12

	LFD	f20, 16 * SIZE(BO)
	LFD	f21, 17 * SIZE(BO)
	LFD	f22, 18 * SIZE(BO)
	LFD	f23, 19 * SIZE(BO)

	FMADD	f1,  f19, f24, f1
	FMADD	f5,  f19, f25, f5
	FMADD	f9,  f19, f26, f9
	FMADD	f13, f19, f27, f13

	LFD	f16,  4 * SIZE(AO)
	LFD	f17,  5 * SIZE(AO)
	LFD	f18,  6 * SIZE(AO)
	LFD	f19,  7 * SIZE(AO)

	LFD	f24, 20 * SIZE(BO)
	LFD	f25, 21 * SIZE(BO)
	LFD	f26, 22 * SIZE(BO)
	LFD	f27, 23 * SIZE(BO)

	addi	AO, AO,  4 * SIZE
	addi	BO, BO, 16 * SIZE
	dcbtst	AO, PREA
	bdnz	LL(32)

	fadd	f0,  f1,   f0
	fadd	f4,  f5,   f4
	fadd	f8,  f9,   f8
	fadd	f12, f13, f12
	.align 4

LL(35):
#if defined(LT) || defined(RN)
	andi.	r0, KK,  3
#else
	andi.	r0, TEMP, 3
#endif
	mtspr	CTR, r0
	ble+	LL(38)
	.align 4

LL(36):
	FMADD	f0,  f16, f20, f0
	FMADD	f4,  f16, f21, f4
	FMADD	f8,  f16, f22, f8
	FMADD	f12, f16, f23, f12

	LFD	f16,  1 * SIZE(AO)

	LFD	f20,  4 * SIZE(BO)
	LFD	f21,  5 * SIZE(BO)
	LFD	f22,  6 * SIZE(BO)
	LFD	f23,  7 * SIZE(BO)

	addi	BO, BO,  4 * SIZE
	addi	AO, AO,  1 * SIZE
	bdnz	LL(36)
	.align 4

LL(38):
#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 1
#else
	subi	r0, KK, 4
#endif
	slwi	TEMP, r0, 0 + BASE_SHIFT
	slwi	r0,   r0, 2 + BASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO, B,     r0
#endif

#if defined(LN) || defined(LT)
	LFD	f16,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(BO)
	LFD	f18,  2 * SIZE(BO)
	LFD	f19,  3 * SIZE(BO)

	FSUB	f0,  f16, f0
	FSUB	f4,  f17, f4
	FSUB	f8,  f18, f8
	FSUB	f12, f19, f12
#else
	LFD	f16,  0 * SIZE(AO)
	LFD	f20,  1 * SIZE(AO)
	LFD	f24,  2 * SIZE(AO)
	LFD	f28,  3 * SIZE(AO)

	FSUB	f0,  f16, f0
	FSUB	f4,  f20, f4
	FSUB	f8,  f24, f8
	FSUB	f12, f28, f12
#endif

#ifdef LN
	LFD	f21,  0 * SIZE(AO)

	FMUL	f0,  f21, f0
	FMUL	f4,  f21, f4
	FMUL	f8,  f21, f8
	FMUL	f12, f21, f12
#endif

#ifdef LT
	LFD	f16,  0 * SIZE(AO)

	FMUL	f0,  f16, f0
	FMUL	f4,  f16, f4
	FMUL	f8,  f16, f8
	FMUL	f12, f16, f12
#endif

#ifdef RN
	LFD	f16,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(BO)
	LFD	f18,  2 * SIZE(BO)
	LFD	f19,  3 * SIZE(BO)

	FMUL	f0,  f16, f0
	FNMSUB	f4,  f17, f0,  f4
	FNMSUB	f8,  f18, f0,  f8
	FNMSUB	f12, f19, f0,  f12

	LFD	f16,  5 * SIZE(BO)
	LFD	f17,  6 * SIZE(BO)
	LFD	f18,  7 * SIZE(BO)
	LFD	f19, 10 * SIZE(BO)

	LFD	f20, 11 * SIZE(BO)
	LFD	f21, 15 * SIZE(BO)

	FMUL	f4,  f16, f4
	FNMSUB	f8,  f17, f4,  f8
	FNMSUB	f12, f18, f4,  f12
	FMUL	f8,  f19, f8
	FNMSUB	f12, f20, f8,  f12
	FMUL	f12, f21, f12
#endif

#ifdef RT
	LFD	f16, 15 * SIZE(BO)
	LFD	f17, 14 * SIZE(BO)
	LFD	f18, 13 * SIZE(BO)
	LFD	f19, 12 * SIZE(BO)

	FMUL	f12, f16, f12
	FNMSUB	f8,  f17, f12, f8
	FNMSUB	f4,  f18, f12, f4
	FNMSUB	f0,  f19, f12, f0

	LFD	f16, 10 * SIZE(BO)
	LFD	f17,  9 * SIZE(BO)
	LFD	f18,  8 * SIZE(BO)
	LFD	f19,  5 * SIZE(BO)

	FMUL	f8,  f16, f8

	LFD	f20,  4 * SIZE(BO)
	LFD	f21,  0 * SIZE(BO)

	FNMSUB	f4,  f17, f8,  f4
	FNMSUB	f0,  f18, f8,  f0

	FMUL	f4,  f19, f4
	FNMSUB	f0,  f20, f4,  f0
	FMUL	f0,  f21, f0
#endif

#ifdef LN
	subi	CO1, CO1, 1 * SIZE
	subi	CO2, CO2, 1 * SIZE
	subi	CO3, CO3, 1 * SIZE
	subi	CO4, CO4, 1 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFD	f0,   0 * SIZE(BO)
	STFD	f4,   1 * SIZE(BO)
	STFD	f8,   2 * SIZE(BO)
	STFD	f12,  3 * SIZE(BO)
#else
	STFD	f0,   0 * SIZE(AO)
	STFD	f4,   1 * SIZE(AO)
	STFD	f8,   2 * SIZE(AO)
	STFD	f12,  3 * SIZE(AO)
#endif

	STFD	f0,   0 * SIZE(CO1)
	STFD	f4,   0 * SIZE(CO2)
	STFD	f8,   0 * SIZE(CO3)
	STFD	f12,  0 * SIZE(CO4)

	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f4,  f0
	fmr	f5,  f0

	fmr	f8,  f0
	fmr	f9,  f0
	fmr	f12, f0
	fmr	f13, f0

#ifndef LN
	addi	CO1, CO1, 1 * SIZE
	addi	CO2, CO2, 1 * SIZE
	addi	CO3, CO3, 1 * SIZE
	addi	CO4, CO4, 1 * SIZE
#endif

#ifdef RT
	slwi	r0, K, 0 + BASE_SHIFT
	add	AORIG, AORIG, r0
#endif

#if defined(LT) || defined(RN)
	sub	TEMP, K, KK
	slwi	r0,   TEMP, 0 + BASE_SHIFT
	slwi	TEMP, TEMP, 2 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LN
	subi	KK, KK, 1
#endif

#ifdef LT
	addi	KK, KK, 1
#endif
	.align 4

LL(39):
#ifdef LN
	slwi	r0, K, 2 + BASE_SHIFT
	add	B, B, r0
#endif

#if defined(LT) || defined(RN)
	mr	B,  BO
#endif

#ifdef RN
	addi	KK, KK, 4
#endif

#ifdef RT
	subi	KK, KK, 4
#endif

	addic.	J, J, -1
	lfs	f0, FZERO
	bgt	LL(10)
	.align 4

LL(40):
	andi.	J, N,  2
	ble	LL(70)

#ifdef RT
	slwi	r0, K, 1 + BASE_SHIFT
	sub	B, B, r0

	slwi	r0, LDC, 1
	sub	C, C, r0
#endif

	mr	CO1, C
	add	CO2, C,  LDC

#ifdef LN
	add	KK, M, OFFSET
#endif

#ifdef LT
	mr	KK, OFFSET
#endif

 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0
	fmr	f4,  f0
	fmr	f5,  f0
	fmr	f6,  f0
	fmr	f7,  f0

	srawi.	I, M,  2
#if defined(LN) || defined(RT)
	mr	AORIG, A
#else
	mr	AO, A
#endif
#ifndef RT
	add	C,  CO2, LDC
#endif
	ble	LL(50)
	.align 4

LL(41):
#if defined(LT) || defined(RN)
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(B)
	LFD	f21,  1 * SIZE(B)
	LFD	f22,  2 * SIZE(B)
	LFD	f23,  3 * SIZE(B)

	dcbt	CO1, PREC
	dcbt	CO2, PREC

	srawi.	r0, KK,  2
	mtspr	CTR, r0
	mr	BO,  B
#else

#ifdef LN
	slwi	r0,   K,  2 + BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0,   KK, 2 + BASE_SHIFT
	slwi	TEMP, KK, 1 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(BO)
	LFD	f21,  1 * SIZE(BO)
	LFD	f22,  2 * SIZE(BO)
	LFD	f23,  3 * SIZE(BO)

	dcbt	CO1, PREC
	dcbt	CO2, PREC

	srawi.	r0, TEMP,  2
	mtspr	CTR, r0
#endif
	ble	LL(45)
	.align 5

LL(42):
	FMADD	f0,  f16, f20, f0
	FMADD	f1,  f17, f20, f1
	FMADD	f2,  f18, f20, f2
	FMADD	f3,  f19, f20, f3

	FMADD	f4,  f16, f21, f4
	FMADD	f5,  f17, f21, f5
	FMADD	f6,  f18, f21, f6
	FMADD	f7,  f19, f21, f7

	LFD	f16,  4 * SIZE(AO)
	LFD	f17,  5 * SIZE(AO)
	LFD	f18,  6 * SIZE(AO)
	LFD	f19,  7 * SIZE(AO)

	FMADD	f0,  f16, f22, f0
	FMADD	f1,  f17, f22, f1
	FMADD	f2,  f18, f22, f2
	FMADD	f3,  f19, f22, f3

	FMADD	f4,  f16, f23, f4
	FMADD	f5,  f17, f23, f5
	FMADD	f6,  f18, f23, f6
	FMADD	f7,  f19, f23, f7

	LFD	f16,  8 * SIZE(AO)
	LFD	f17,  9 * SIZE(AO)
	LFD	f18, 10 * SIZE(AO)
	LFD	f19, 11 * SIZE(AO)

	LFD	f20,  4 * SIZE(BO)
	LFD	f21,  5 * SIZE(BO)
	LFD	f22,  6 * SIZE(BO)
	LFD	f23,  7 * SIZE(BO)

	FMADD	f0,  f16, f20, f0
	FMADD	f1,  f17, f20, f1
	FMADD	f2,  f18, f20, f2
	FMADD	f3,  f19, f20, f3

	FMADD	f4,  f16, f21, f4
	FMADD	f5,  f17, f21, f5
	FMADD	f6,  f18, f21, f6
	FMADD	f7,  f19, f21, f7

	LFD	f16, 12 * SIZE(AO)
	LFD	f17, 13 * SIZE(AO)
	LFD	f18, 14 * SIZE(AO)
	LFD	f19, 15 * SIZE(AO)

	FMADD	f0,  f16, f22, f0
	FMADD	f1,  f17, f22, f1
	FMADD	f2,  f18, f22, f2
	FMADD	f3,  f19, f22, f3

	FMADD	f4,  f16, f23, f4
	FMADD	f5,  f17, f23, f5
	FMADD	f6,  f18, f23, f6
	FMADD	f7,  f19, f23, f7

	LFD	f16, 16 * SIZE(AO)
	LFD	f17, 17 * SIZE(AO)
	LFD	f18, 18 * SIZE(AO)
	LFD	f19, 19 * SIZE(AO)

	LFD	f20,  8 * SIZE(BO)
	LFD	f21,  9 * SIZE(BO)
	LFD	f22, 10 * SIZE(BO)
	LFD	f23, 11 * SIZE(BO)

	addi	AO, AO, 16 * SIZE
	addi	BO, BO,  8 * SIZE
	dcbtst	AO, PREA
	bdnz	LL(42)
	.align 4

LL(45):
#if defined(LT) || defined(RN)
	andi.	r0, KK,  3
#else
	andi.	r0, TEMP, 3
#endif
	mtspr	CTR, r0
	ble+	LL(48)
	.align 4

LL(46):
	FMADD	f0,  f16, f20, f0
	FMADD	f1,  f17, f20, f1
	FMADD	f2,  f18, f20, f2
	FMADD	f3,  f19, f20, f3

	FMADD	f4,  f16, f21, f4
	FMADD	f5,  f17, f21, f5
	FMADD	f6,  f18, f21, f6
	FMADD	f7,  f19, f21, f7

	LFD	f16,  4 * SIZE(AO)
	LFD	f17,  5 * SIZE(AO)
	LFD	f18,  6 * SIZE(AO)
	LFD	f19,  7 * SIZE(AO)

	LFD	f20,  2 * SIZE(BO)
	LFD	f21,  3 * SIZE(BO)

	addi	BO, BO,  2 * SIZE
	addi	AO, AO,  4 * SIZE
	bdnz	LL(46)
	.align 4

LL(48):
#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 4
#else
	subi	r0, KK, 2
#endif
	slwi	TEMP, r0, 2 + BASE_SHIFT
	slwi	r0,   r0, 1 + BASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO, B,     r0
#endif

#if defined(LN) || defined(LT)
	LFD	f16,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(BO)
	LFD	f20,  2 * SIZE(BO)
 	LFD	f21,  3 * SIZE(BO)

	LFD	f24,  4 * SIZE(BO)
	LFD	f25,  5 * SIZE(BO)
	LFD	f28,  6 * SIZE(BO)
	LFD	f29,  7 * SIZE(BO)

	FSUB	f0,  f16, f0
	FSUB	f4,  f17, f4
	FSUB	f1,  f20, f1
	FSUB	f5,  f21, f5

	FSUB	f2,  f24, f2
	FSUB	f6,  f25, f6
	FSUB	f3,  f28, f3
	FSUB	f7,  f29, f7
#else
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  4 * SIZE(AO)
 	LFD	f21,  5 * SIZE(AO)
	LFD	f22,  6 * SIZE(AO)
	LFD	f23,  7 * SIZE(AO)

	FSUB	f0,  f16, f0
	FSUB	f1,  f17, f1
	FSUB	f2,  f18, f2
	FSUB	f3,  f19, f3

	FSUB	f4,  f20, f4
	FSUB	f5,  f21, f5
	FSUB	f6,  f22, f6
	FSUB	f7,  f23, f7
#endif

#ifdef LN
	LFD	f16, 15 * SIZE(AO)
	LFD	f17, 14 * SIZE(AO)
	LFD	f18, 13 * SIZE(AO)
	LFD	f19, 12 * SIZE(AO)

	FMUL	f3,  f16, f3
	FMUL	f7,  f16, f7
	FNMSUB	f2,  f17, f3,  f2
	FNMSUB	f6,  f17, f7,  f6
	FNMSUB	f1,  f18, f3,  f1
	FNMSUB	f5,  f18, f7,  f5
	FNMSUB	f0,  f19, f3,  f0
	FNMSUB	f4,  f19, f7,  f4

	LFD	f16, 10 * SIZE(AO)
	LFD	f17,  9 * SIZE(AO)
	LFD	f18,  8 * SIZE(AO)
	LFD	f19,  5 * SIZE(AO)

	LFD	f20,  4 * SIZE(AO)
	LFD	f21,  0 * SIZE(AO)

	FMUL	f2,  f16, f2
	FMUL	f6,  f16, f6
	FNMSUB	f1,  f17, f2,  f1
	FNMSUB	f5,  f17, f6,  f5
	FNMSUB	f0,  f18, f2,  f0
	FNMSUB	f4,  f18, f6,  f4

	FMUL	f1,  f19, f1
	FMUL	f5,  f19, f5
	FNMSUB	f0,  f20, f1,  f0
	FNMSUB	f4,  f20, f5,  f4
	FMUL	f0,  f21, f0
	FMUL	f4,  f21, f4
#endif

#ifdef LT
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	FMUL	f0,  f16, f0
	FMUL	f4,  f16, f4
	FNMSUB	f1,  f17, f0,  f1
	FNMSUB	f5,  f17, f4,  f5

	FNMSUB	f2,  f18, f0,  f2
	FNMSUB	f6,  f18, f4,  f6
	FNMSUB	f3,  f19, f0,  f3
	FNMSUB	f7,  f19, f4,  f7

	LFD	f17,  5 * SIZE(AO)
	LFD	f18,  6 * SIZE(AO)
	LFD	f19,  7 * SIZE(AO)

	FMUL	f1,  f17, f1
	FMUL	f5,  f17, f5

	FNMSUB	f2,  f18, f1,  f2
	FNMSUB	f6,  f18, f5,  f6

	FNMSUB	f3,  f19, f1,  f3
	FNMSUB	f7,  f19, f5,  f7

	LFD	f18, 10 * SIZE(AO)
	LFD	f19, 11 * SIZE(AO)

	FMUL	f2,  f18, f2
	FMUL	f6,  f18, f6

	FNMSUB	f3,  f19, f2,  f3
	FNMSUB	f7,  f19, f6,  f7

	LFD	f19, 15 * SIZE(AO)

	FMUL	f3,  f19, f3
	FMUL	f7,  f19, f7
#endif

#ifdef RN
	LFD	f16,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(BO)
	LFD	f18,  3 * SIZE(BO)

	FMUL	f0,  f16, f0
	FMUL	f1,  f16, f1
	FMUL	f2,  f16, f2
	FMUL	f3,  f16, f3

	FNMSUB	f4,  f17, f0,  f4
	FNMSUB	f5,  f17, f1,  f5
	FNMSUB	f6,  f17, f2,  f6
	FNMSUB	f7,  f17, f3,  f7

	FMUL	f4,  f18, f4
	FMUL	f5,  f18, f5
	FMUL	f6,  f18, f6
	FMUL	f7,  f18, f7
#endif

#ifdef RT
	LFD	f19,  3 * SIZE(BO)
	LFD	f20,  2 * SIZE(BO)
	LFD	f21,  0 * SIZE(BO)

	FMUL	f4,  f19, f4
	FMUL	f5,  f19, f5
	FMUL	f6,  f19, f6
	FMUL	f7,  f19, f7

	FNMSUB	f0,  f20, f4,  f0
	FNMSUB	f1,  f20, f5,  f1
	FNMSUB	f2,  f20, f6,  f2
	FNMSUB	f3,  f20, f7,  f3

	FMUL	f0,  f21, f0
	FMUL	f1,  f21, f1
	FMUL	f2,  f21, f2
	FMUL	f3,  f21, f3
#endif

#ifdef LN
	subi	CO1, CO1, 4 * SIZE
	subi	CO2, CO2, 4 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFD	f0,   0 * SIZE(BO)
	STFD	f4,   1 * SIZE(BO)
	STFD	f1,   2 * SIZE(BO)
	STFD	f5,   3 * SIZE(BO)

	STFD	f2,   4 * SIZE(BO)
	STFD	f6,   5 * SIZE(BO)
	STFD	f3,   6 * SIZE(BO)
	STFD	f7,   7 * SIZE(BO)
#else
	STFD	f0,   0 * SIZE(AO)
	STFD	f1,   1 * SIZE(AO)
	STFD	f2,   2 * SIZE(AO)
	STFD	f3,   3 * SIZE(AO)

	STFD	f4,   4 * SIZE(AO)
	STFD	f5,   5 * SIZE(AO)
	STFD	f6,   6 * SIZE(AO)
	STFD	f7,   7 * SIZE(AO)
#endif

	STFD	f0,   0 * SIZE(CO1)
	STFD	f1,   1 * SIZE(CO1)
	STFD	f2,   2 * SIZE(CO1)
	STFD	f3,   3 * SIZE(CO1)

	STFD	f4,   0 * SIZE(CO2)
	STFD	f5,   1 * SIZE(CO2)
	STFD	f6,   2 * SIZE(CO2)
	STFD	f7,   3 * SIZE(CO2)

	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

	fmr	f4,  f0
	fmr	f5,  f0
	fmr	f6,  f0
	fmr	f7,  f0

#ifndef LN
	addi	CO1, CO1, 4 * SIZE
	addi	CO2, CO2, 4 * SIZE
#endif

#ifdef RT
	slwi	r0, K, 2 + BASE_SHIFT
	add	AORIG, AORIG, r0
#endif

#if defined(LT) || defined(RN)
	sub	TEMP, K, KK
	slwi	r0,   TEMP, 2 + BASE_SHIFT
	slwi	TEMP, TEMP, 1 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LN
	subi	KK, KK, 4
#endif

#ifdef LT
	addi	KK, KK, 4
#endif

	addic.	I, I, -1
	bgt+	LL(41)
	.align 4

LL(50):
	andi.	I,  M,  2
	ble	LL(60)

#if defined(LT) || defined(RN)
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

	srawi.	r0, KK,  2
	mtspr	CTR, r0
	mr	BO,  B
#else

#ifdef LN
	slwi	r0,   K,  1 + BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0,   KK, 1 + BASE_SHIFT
	slwi	TEMP, KK, 1 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

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

	srawi.	r0, TEMP,  2
	mtspr	CTR, r0
#endif
	ble	LL(55)
	.align 5

LL(52):
	FMADD	f0,  f16, f20, f0
	FMADD	f1,  f17, f20, f1
	FMADD	f2,  f16, f21, f2
	FMADD	f3,  f17, f21, f3

	FMADD	f4,  f18, f22, f4
	FMADD	f5,  f19, f22, f5
	FMADD	f6,  f18, f23, f6
	FMADD	f7,  f19, f23, f7

	LFD	f16,  4 * SIZE(AO)
	LFD	f17,  5 * SIZE(AO)
	LFD	f18,  6 * SIZE(AO)
	LFD	f19,  7 * SIZE(AO)

	LFD	f20,  8 * SIZE(BO)
	LFD	f21,  9 * SIZE(BO)
	LFD	f22, 10 * SIZE(BO)
	LFD	f23, 11 * SIZE(BO)

	FMADD	f0,  f16, f24, f0
	FMADD	f1,  f17, f24, f1
	FMADD	f2,  f16, f25, f2
	FMADD	f3,  f17, f25, f3

	FMADD	f4,  f18, f26, f4
	FMADD	f5,  f19, f26, f5
	FMADD	f6,  f18, f27, f6
	FMADD	f7,  f19, f27, f7

	LFD	f16,  8 * SIZE(AO)
	LFD	f17,  9 * SIZE(AO)
	LFD	f18, 10 * SIZE(AO)
	LFD	f19, 11 * SIZE(AO)

	LFD	f24, 12 * SIZE(BO)
	LFD	f25, 13 * SIZE(BO)
	LFD	f26, 14 * SIZE(BO)
	LFD	f27, 15 * SIZE(BO)

	addi	AO, AO,  8 * SIZE
	addi	BO, BO,  8 * SIZE
	dcbtst	AO, PREA
	bdnz	LL(52)
	.align 4

LL(55):
#if defined(LT) || defined(RN)
	andi.	r0, KK,  3
#else
	andi.	r0, TEMP, 3
#endif
	mtspr	CTR, r0
	ble+	LL(58)
	.align 4

LL(56):
	FMADD	f0,  f16, f20, f0
	FMADD	f1,  f17, f20, f1
	FMADD	f2,  f16, f21, f2
	FMADD	f3,  f17, f21, f3

	LFD	f16,  2 * SIZE(AO)
	LFD	f17,  3 * SIZE(AO)
	LFD	f20,  2 * SIZE(BO)
	LFD	f21,  3 * SIZE(BO)

	addi	BO, BO,  2 * SIZE
	addi	AO, AO,  2 * SIZE
	bdnz	LL(56)
	.align 4

LL(58):
	FADD	f0, f4,  f0
	FADD	f1, f5,  f1
	FADD	f2, f6,  f2
	FADD	f3, f7,  f3

#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 2
#else
	subi	r0, KK, 2
#endif
	slwi	TEMP, r0, 1 + BASE_SHIFT
	slwi	r0,   r0, 1 + BASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO, B,     r0
#endif

#if defined(LN) || defined(LT)
	LFD	f16,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(BO)
	LFD	f20,  2 * SIZE(BO)
 	LFD	f21,  3 * SIZE(BO)

	FSUB	f0,  f16, f0
	FSUB	f2,  f17, f2
	FSUB	f1,  f20, f1
	FSUB	f3,  f21, f3
#else
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f20,  2 * SIZE(AO)
 	LFD	f21,  3 * SIZE(AO)

	FSUB	f0,  f16, f0
	FSUB	f1,  f17, f1
	FSUB	f2,  f20, f2
	FSUB	f3,  f21, f3
#endif

#ifdef LN
	LFD	f19,  3 * SIZE(AO)
	LFD	f20,  2 * SIZE(AO)
	LFD	f21,  0 * SIZE(AO)

	FMUL	f1,  f19, f1
	FMUL	f3,  f19, f3

	FNMSUB	f0,  f20, f1,  f0
	FNMSUB	f2,  f20, f3,  f2

	FMUL	f0,  f21, f0
	FMUL	f2,  f21, f2
#endif

#ifdef LT
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)

	FMUL	f0,  f16, f0
	FMUL	f2,  f16, f2
	FNMSUB	f1,  f17, f0,  f1
	FNMSUB	f3,  f17, f2,  f3

	LFD	f17,  3 * SIZE(AO)

	FMUL	f1,  f17, f1
	FMUL	f3,  f17, f3
#endif

#ifdef RN
	LFD	f16,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(BO)
	LFD	f18,  3 * SIZE(BO)

	FMUL	f0,  f16, f0
	FMUL	f1,  f16, f1

	FNMSUB	f2,  f17, f0,  f2
	FNMSUB	f3,  f17, f1,  f3
	FMUL	f2,  f18, f2
	FMUL	f3,  f18, f3
#endif

#ifdef RT
	LFD	f19,  3 * SIZE(BO)
	LFD	f20,  2 * SIZE(BO)
	LFD	f21,  0 * SIZE(BO)

	FMUL	f2,  f19, f2
	FMUL	f3,  f19, f3
	FNMSUB	f0,  f20, f2,  f0
	FNMSUB	f1,  f20, f3,  f1
	FMUL	f0,  f21, f0
	FMUL	f1,  f21, f1
#endif

#ifdef LN
	subi	CO1, CO1, 2 * SIZE
	subi	CO2, CO2, 2 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFD	f0,   0 * SIZE(BO)
	STFD	f2,   1 * SIZE(BO)
	STFD	f1,   2 * SIZE(BO)
	STFD	f3,   3 * SIZE(BO)
#else
	STFD	f0,   0 * SIZE(AO)
	STFD	f1,   1 * SIZE(AO)
	STFD	f2,   2 * SIZE(AO)
	STFD	f3,   3 * SIZE(AO)
#endif

	STFD	f0,   0 * SIZE(CO1)
	STFD	f1,   1 * SIZE(CO1)
	STFD	f2,   0 * SIZE(CO2)
	STFD	f3,   1 * SIZE(CO2)

	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

	fmr	f4,  f0
	fmr	f5,  f0
	fmr	f6,  f0
	fmr	f7,  f0

#ifndef LN
	addi	CO1, CO1, 2 * SIZE
	addi	CO2, CO2, 2 * SIZE
#endif

#ifdef RT
	slwi	r0, K, 1 + BASE_SHIFT
	add	AORIG, AORIG, r0
#endif

#if defined(LT) || defined(RN)
	sub	TEMP, K, KK
	slwi	r0,   TEMP, 1 + BASE_SHIFT
	slwi	TEMP, TEMP, 1 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LN
	subi	KK, KK, 2
#endif

#ifdef LT
	addi	KK, KK, 2
#endif
	.align 4

LL(60):
	andi.	I,  M,  1
	ble	LL(69)

#if defined(LT) || defined(RN)
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

	srawi.	r0, KK,  2
	mtspr	CTR, r0
	mr	BO,  B
#else

#ifdef LN
	slwi	r0,   K,  BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0,   KK, 0 + BASE_SHIFT
	slwi	TEMP, KK, 1 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

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

	srawi.	r0, TEMP,  2
	mtspr	CTR, r0
#endif
	ble	LL(65)
	.align 5

LL(62):
	FMADD	f0,  f16, f20, f0
	FMADD	f1,  f16, f21, f1
	FMADD	f2,  f17, f22, f2
	FMADD	f3,  f17, f23, f3

	LFD	f20,  8 * SIZE(BO)
	LFD	f21,  9 * SIZE(BO)
	LFD	f22, 10 * SIZE(BO)
	LFD	f23, 11 * SIZE(BO)

	FMADD	f0,  f18, f24, f0
	FMADD	f1,  f18, f25, f1
	FMADD	f2,  f19, f26, f2
	FMADD	f3,  f19, f27, f3

	LFD	f16,  4 * SIZE(AO)
	LFD	f17,  5 * SIZE(AO)
	LFD	f18,  6 * SIZE(AO)
	LFD	f19,  7 * SIZE(AO)

	LFD	f24, 12 * SIZE(BO)
	LFD	f25, 13 * SIZE(BO)
	LFD	f26, 14 * SIZE(BO)
	LFD	f27, 15 * SIZE(BO)

	addi	AO, AO,  4 * SIZE
	addi	BO, BO,  8 * SIZE
	bdnz	LL(62)
	.align 4

LL(65):
#if defined(LT) || defined(RN)
	andi.	r0, KK,  3
#else
	andi.	r0, TEMP, 3
#endif
	mtspr	CTR, r0
	ble+	LL(68)
	.align 4

LL(66):
	FMADD	f0,  f16, f20, f0
	FMADD	f1,  f16, f21, f1

	LFD	f16,  1 * SIZE(AO)

	LFD	f20,  2 * SIZE(BO)
	LFD	f21,  3 * SIZE(BO)

	addi	BO, BO,  2 * SIZE
	addi	AO, AO,  1 * SIZE
	bdnz	LL(66)
	.align 4

LL(68):
	FADD	f0, f2, f0
	FADD	f1, f3, f1

#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 1
#else
	subi	r0, KK, 2
#endif
	slwi	TEMP, r0, 0 + BASE_SHIFT
	slwi	r0,   r0, 1 + BASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO, B,     r0
#endif

#if defined(LN) || defined(LT)
	LFD	f16,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(BO)

	FSUB	f0,  f16, f0
	FSUB	f1,  f17, f1
#else
	LFD	f16,  0 * SIZE(AO)
	LFD	f20,  1 * SIZE(AO)

	FSUB	f0,  f16, f0
	FSUB	f1,  f20, f1
#endif

#ifdef LN
	LFD	f21,  0 * SIZE(AO)

	FMUL	f0,  f21, f0
	FMUL	f1,  f21, f1
#endif

#ifdef LT
	LFD	f16,  0 * SIZE(AO)

	FMUL	f0,  f16, f0
	FMUL	f1,  f16, f1
#endif

#ifdef RN
	LFD	f16,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(BO)
	LFD	f18,  3 * SIZE(BO)

	FMUL	f0,  f16, f0
	FNMSUB	f1,  f17, f0,  f1
	FMUL	f1,  f18, f1
#endif

#ifdef RT
	LFD	f19,  3 * SIZE(BO)
	LFD	f20,  2 * SIZE(BO)
	LFD	f21,  0 * SIZE(BO)

	FMUL	f1,  f19, f1
	FNMSUB	f0,  f20, f1,  f0
	FMUL	f0,  f21, f0
#endif

#ifdef LN
	subi	CO1, CO1, 1 * SIZE
	subi	CO2, CO2, 1 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFD	f0,   0 * SIZE(BO)
	STFD	f1,   1 * SIZE(BO)
#else
	STFD	f0,   0 * SIZE(AO)
	STFD	f1,   1 * SIZE(AO)
#endif

	STFD	f0,   0 * SIZE(CO1)
	STFD	f1,   0 * SIZE(CO2)

	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f4,  f0
	fmr	f5,  f0

#ifndef LN
	addi	CO1, CO1, 1 * SIZE
	addi	CO2, CO2, 1 * SIZE
#endif

#ifdef RT
	slwi	r0, K, 0 + BASE_SHIFT
	add	AORIG, AORIG, r0
#endif

#if defined(LT) || defined(RN)
	sub	TEMP, K, KK
	slwi	r0,   TEMP, 0 + BASE_SHIFT
	slwi	TEMP, TEMP, 1 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LN
	subi	KK, KK, 1
#endif

#ifdef LT
	addi	KK, KK, 1
#endif
	.align 4

LL(69):
#ifdef LN
	slwi	r0, K, 1 + BASE_SHIFT
	add	B, B, r0
#endif

#if defined(LT) || defined(RN)
	mr	B,  BO
#endif

#ifdef RN
	addi	KK, KK, 2
#endif

#ifdef RT
	subi	KK, KK, 2
#endif
	lfs	f0, FZERO
	.align 4

LL(70):
	andi.	J, N,  1
	ble	LL(999)

#ifdef RT
	slwi	r0, K, 0 + BASE_SHIFT
	sub	B, B, r0

	sub	C, C, LDC
#endif

	mr	CO1, C

#ifdef LN
	add	KK, M, OFFSET
#endif

#ifdef LT
	mr	KK, OFFSET
#endif

 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

	srawi.	I, M,  2
#if defined(LN) || defined(RT)
	mr	AORIG, A
#else
	mr	AO, A
#endif
#ifndef RT
	add	C,  CO1, LDC
#endif
	ble	LL(80)
	.align 4

LL(71):
#if defined(LT) || defined(RN)
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(B)
	LFD	f21,  1 * SIZE(B)
	LFD	f22,  2 * SIZE(B)
	LFD	f23,  3 * SIZE(B)

	dcbt	CO1, PREC

	srawi.	r0, KK,  2
	mtspr	CTR, r0
	mr	BO,  B
#else

#ifdef LN
	slwi	r0,   K,  2 + BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0,   KK, 2 + BASE_SHIFT
	slwi	TEMP, KK, 0 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(BO)
	LFD	f21,  1 * SIZE(BO)
	LFD	f22,  2 * SIZE(BO)
	LFD	f23,  3 * SIZE(BO)

	dcbt	CO1, PREC

	srawi.	r0, TEMP,  2
	mtspr	CTR, r0
#endif
	ble	LL(75)
	.align 5

LL(72):
	FMADD	f0,  f16, f20, f0
	FMADD	f1,  f17, f20, f1
	FMADD	f2,  f18, f20, f2
	FMADD	f3,  f19, f20, f3

	LFD	f16,  4 * SIZE(AO)
	LFD	f17,  5 * SIZE(AO)
	LFD	f18,  6 * SIZE(AO)
	LFD	f19,  7 * SIZE(AO)

	FMADD	f0,  f16, f21, f0
	FMADD	f1,  f17, f21, f1
	FMADD	f2,  f18, f21, f2
	FMADD	f3,  f19, f21, f3

	LFD	f16,  8 * SIZE(AO)
	LFD	f17,  9 * SIZE(AO)
	LFD	f18, 10 * SIZE(AO)
	LFD	f19, 11 * SIZE(AO)

	FMADD	f0,  f16, f22, f0
	FMADD	f1,  f17, f22, f1
	FMADD	f2,  f18, f22, f2
	FMADD	f3,  f19, f22, f3

	LFD	f16, 12 * SIZE(AO)
	LFD	f17, 13 * SIZE(AO)
	LFD	f18, 14 * SIZE(AO)
	LFD	f19, 15 * SIZE(AO)

	FMADD	f0,  f16, f23, f0
	FMADD	f1,  f17, f23, f1
	FMADD	f2,  f18, f23, f2
	FMADD	f3,  f19, f23, f3

	LFD	f16, 16 * SIZE(AO)
	LFD	f17, 17 * SIZE(AO)
	LFD	f18, 18 * SIZE(AO)
	LFD	f19, 19 * SIZE(AO)

	LFD	f20,  4 * SIZE(BO)
	LFD	f21,  5 * SIZE(BO)
	LFD	f22,  6 * SIZE(BO)
	LFD	f23,  7 * SIZE(BO)

	addi	AO, AO, 16 * SIZE
	addi	BO, BO,  4 * SIZE
	dcbtst	AO, PREA
	bdnz	LL(72)
	.align 4

LL(75):
#if defined(LT) || defined(RN)
	andi.	r0, KK,  3
#else
	andi.	r0, TEMP, 3
#endif
	mtspr	CTR, r0
	ble+	LL(78)
	.align 4

LL(76):
	FMADD	f0,  f16, f20, f0
	FMADD	f1,  f17, f20, f1
	FMADD	f2,  f18, f20, f2
	FMADD	f3,  f19, f20, f3

	LFD	f16,  4 * SIZE(AO)
	LFD	f17,  5 * SIZE(AO)
	LFD	f18,  6 * SIZE(AO)
	LFD	f19,  7 * SIZE(AO)

	LFD	f20,  1 * SIZE(BO)

	addi	BO, BO,  1 * SIZE
	addi	AO, AO,  4 * SIZE
	bdnz	LL(76)
	.align 4

LL(78):
#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 4
#else
	subi	r0, KK, 1
#endif
	slwi	TEMP, r0, 2 + BASE_SHIFT
	slwi	r0,   r0, 0 + BASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO, B,     r0
#endif

#if defined(LN) || defined(LT)
	LFD	f16,  0 * SIZE(BO)
	LFD	f20,  1 * SIZE(BO)
	LFD	f24,  2 * SIZE(BO)
	LFD	f28,  3 * SIZE(BO)

	FSUB	f0,  f16, f0
	FSUB	f1,  f20, f1
	FSUB	f2,  f24, f2
	FSUB	f3,  f28, f3
#else
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	FSUB	f0,  f16, f0
	FSUB	f1,  f17, f1
	FSUB	f2,  f18, f2
	FSUB	f3,  f19, f3
#endif

#ifdef LN
	LFD	f16, 15 * SIZE(AO)
	LFD	f17, 14 * SIZE(AO)
	LFD	f18, 13 * SIZE(AO)
	LFD	f19, 12 * SIZE(AO)

	FMUL	f3,  f16, f3
	FNMSUB	f2,  f17, f3,  f2
	FNMSUB	f1,  f18, f3,  f1
	FNMSUB	f0,  f19, f3,  f0

	LFD	f16, 10 * SIZE(AO)
	LFD	f17,  9 * SIZE(AO)
	LFD	f18,  8 * SIZE(AO)
	LFD	f19,  5 * SIZE(AO)

	LFD	f20,  4 * SIZE(AO)
	LFD	f21,  0 * SIZE(AO)

	FMUL	f2,  f16, f2
	FNMSUB	f1,  f17, f2,  f1
	FNMSUB	f0,  f18, f2,  f0

	FMUL	f1,  f19, f1
	FNMSUB	f0,  f20, f1,  f0
	FMUL	f0,  f21, f0
#endif

#ifdef LT
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	FMUL	f0,  f16, f0
	FNMSUB	f1,  f17, f0,  f1
	FNMSUB	f2,  f18, f0,  f2
	FNMSUB	f3,  f19, f0,  f3

	LFD	f17,  5 * SIZE(AO)
	LFD	f18,  6 * SIZE(AO)
	LFD	f19,  7 * SIZE(AO)

	FMUL	f1,  f17, f1
	FNMSUB	f2,  f18, f1,  f2
	FNMSUB	f3,  f19, f1,  f3

	LFD	f18, 10 * SIZE(AO)
	LFD	f19, 11 * SIZE(AO)

	FMUL	f2,  f18, f2
	FNMSUB	f3,  f19, f2,  f3

	LFD	f19, 15 * SIZE(AO)

	FMUL	f3,  f19, f3
#endif

#ifdef RN
	LFD	f16,  0 * SIZE(BO)

	FMUL	f0,  f16, f0
	FMUL	f1,  f16, f1
	FMUL	f2,  f16, f2
	FMUL	f3,  f16, f3
#endif

#ifdef RT
	LFD	f21,  0 * SIZE(BO)

	FMUL	f0,  f21, f0
	FMUL	f1,  f21, f1
	FMUL	f2,  f21, f2
	FMUL	f3,  f21, f3
#endif

#ifdef LN
	subi	CO1, CO1, 4 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFD	f0,   0 * SIZE(BO)
	STFD	f1,   1 * SIZE(BO)
	STFD	f2,   2 * SIZE(BO)
	STFD	f3,   3 * SIZE(BO)
#else
	STFD	f0,   0 * SIZE(AO)
	STFD	f1,   1 * SIZE(AO)
	STFD	f2,   2 * SIZE(AO)
	STFD	f3,   3 * SIZE(AO)
#endif

	STFD	f0,   0 * SIZE(CO1)
	STFD	f1,   1 * SIZE(CO1)
	STFD	f2,   2 * SIZE(CO1)
	STFD	f3,   3 * SIZE(CO1)

	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

#ifndef LN
	addi	CO1, CO1, 4 * SIZE
#endif

#ifdef RT
	slwi	r0, K, 2 + BASE_SHIFT
	add	AORIG, AORIG, r0
#endif

#if defined(LT) || defined(RN)
	sub	TEMP, K, KK
	slwi	r0,   TEMP, 2 + BASE_SHIFT
	slwi	TEMP, TEMP, 0 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LN
	subi	KK, KK, 4
#endif

#ifdef LT
	addi	KK, KK, 4
#endif

	addic.	I, I, -1
	bgt+	LL(71)
	.align 4

LL(80):
	andi.	I,  M,  2
	ble	LL(90)

#if defined(LT) || defined(RN)
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(B)
	LFD	f21,  1 * SIZE(B)
	LFD	f22,  2 * SIZE(B)
	LFD	f23,  3 * SIZE(B)

	srawi.	r0, KK,  2
	mtspr	CTR, r0
	mr	BO,  B
#else

#ifdef LN
	slwi	r0,   K,  1 + BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0,   KK, 1 + BASE_SHIFT
	slwi	TEMP, KK, 0 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(BO)
	LFD	f21,  1 * SIZE(BO)
	LFD	f22,  2 * SIZE(BO)
	LFD	f23,  3 * SIZE(BO)

	srawi.	r0, TEMP,  2
	mtspr	CTR, r0
#endif
	ble	LL(85)
	.align 5

LL(82):
	FMADD	f0,  f16, f20, f0
	FMADD	f1,  f17, f20, f1
	FMADD	f2,  f18, f21, f2
	FMADD	f3,  f19, f21, f3

	LFD	f16,  4 * SIZE(AO)
	LFD	f17,  5 * SIZE(AO)
	LFD	f18,  6 * SIZE(AO)
	LFD	f19,  7 * SIZE(AO)

	FMADD	f0,  f16, f22, f0
	FMADD	f1,  f17, f22, f1
	FMADD	f2,  f18, f23, f2
	FMADD	f3,  f19, f23, f3

	LFD	f16,  8 * SIZE(AO)
	LFD	f17,  9 * SIZE(AO)
	LFD	f18, 10 * SIZE(AO)
	LFD	f19, 11 * SIZE(AO)

	LFD	f20,  4 * SIZE(BO)
	LFD	f21,  5 * SIZE(BO)
	LFD	f22,  6 * SIZE(BO)
	LFD	f23,  7 * SIZE(BO)

	addi	AO, AO,  8 * SIZE
	addi	BO, BO,  4 * SIZE
	dcbtst	AO, PREA
	bdnz	LL(82)
	.align 4

LL(85):
#if defined(LT) || defined(RN)
	andi.	r0, KK,  3
#else
	andi.	r0, TEMP, 3
#endif
	mtspr	CTR, r0
	ble+	LL(88)
	.align 4

LL(86):
	FMADD	f0,  f16, f20, f0
	FMADD	f1,  f17, f20, f1

	LFD	f16,  2 * SIZE(AO)
	LFD	f17,  3 * SIZE(AO)
	LFD	f20,  1 * SIZE(BO)

	addi	BO, BO,  1 * SIZE
	addi	AO, AO,  2 * SIZE
	bdnz	LL(86)
	.align 4

LL(88):
	FADD	f0, f2, f0
	FADD	f1, f3, f1

#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 2
#else
	subi	r0, KK, 1
#endif
	slwi	TEMP, r0, 1 + BASE_SHIFT
	slwi	r0,   r0, 0 + BASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO, B,     r0
#endif

#if defined(LN) || defined(LT)
	LFD	f16,  0 * SIZE(BO)
	LFD	f20,  1 * SIZE(BO)

	FSUB	f0,  f16, f0
	FSUB	f1,  f20, f1
#else
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)

	FSUB	f0,  f16, f0
	FSUB	f1,  f17, f1
#endif

#ifdef LN
	LFD	f19,  3 * SIZE(AO)
	LFD	f20,  2 * SIZE(AO)
	LFD	f21,  0 * SIZE(AO)

	FMUL	f1,  f19, f1
	FNMSUB	f0,  f20, f1,  f0
	FMUL	f0,  f21, f0
#endif

#ifdef LT
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)

	FMUL	f0,  f16, f0
	FNMSUB	f1,  f17, f0,  f1

	LFD	f17,  3 * SIZE(AO)
	FMUL	f1,  f17, f1
#endif

#ifdef RN
	LFD	f16,  0 * SIZE(BO)

	FMUL	f0,  f16, f0
	FMUL	f1,  f16, f1
#endif

#ifdef RT
	LFD	f21,  0 * SIZE(BO)

	FMUL	f0,  f21, f0
	FMUL	f1,  f21, f1
#endif

#ifdef LN
	subi	CO1, CO1, 2 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFD	f0,   0 * SIZE(BO)
	STFD	f1,   1 * SIZE(BO)
#else
	STFD	f0,   0 * SIZE(AO)
	STFD	f1,   1 * SIZE(AO)
#endif

	STFD	f0,   0 * SIZE(CO1)
	STFD	f1,   1 * SIZE(CO1)

	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

#ifndef LN
	addi	CO1, CO1, 2 * SIZE
#endif

#ifdef RT
	slwi	r0, K, 1 + BASE_SHIFT
	add	AORIG, AORIG, r0
#endif

#if defined(LT) || defined(RN)
	sub	TEMP, K, KK
	slwi	r0,   TEMP, 1 + BASE_SHIFT
	slwi	TEMP, TEMP, 0 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LN
	subi	KK, KK, 2
#endif

#ifdef LT
	addi	KK, KK, 2
#endif
	.align 4

LL(90):
	andi.	I,  M,  1
	ble	LL(999)

#if defined(LT) || defined(RN)
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(B)
	LFD	f21,  1 * SIZE(B)
	LFD	f22,  2 * SIZE(B)
	LFD	f23,  3 * SIZE(B)

	srawi.	r0, KK,  3
	mtspr	CTR, r0
	mr	BO,  B
#else

#ifdef LN
	slwi	r0,   K,  BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0,   KK, 0 + BASE_SHIFT
	slwi	TEMP, KK, 0 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(BO)
	LFD	f21,  1 * SIZE(BO)
	LFD	f22,  2 * SIZE(BO)
	LFD	f23,  3 * SIZE(BO)

	srawi.	r0, TEMP,  3
	mtspr	CTR, r0
#endif
	ble	LL(95)
	.align 5

LL(92):
	FMADD	f0,  f16, f20, f0
	FMADD	f1,  f17, f21, f1
	FMADD	f2,  f18, f22, f2
	FMADD	f3,  f19, f23, f3

	LFD	f16,  4 * SIZE(AO)
	LFD	f17,  5 * SIZE(AO)
	LFD	f18,  6 * SIZE(AO)
	LFD	f19,  7 * SIZE(AO)

	LFD	f20,  4 * SIZE(BO)
	LFD	f21,  5 * SIZE(BO)
	LFD	f22,  6 * SIZE(BO)
	LFD	f23,  7 * SIZE(BO)

	FMADD	f0,  f16, f20, f0
	FMADD	f1,  f17, f21, f1
	FMADD	f2,  f18, f22, f2
	FMADD	f3,  f19, f23, f3

	LFD	f16,  8 * SIZE(AO)
	LFD	f17,  9 * SIZE(AO)
	LFD	f18, 10 * SIZE(AO)
	LFD	f19, 11 * SIZE(AO)

	LFD	f20,  8 * SIZE(BO)
	LFD	f21,  9 * SIZE(BO)
	LFD	f22, 10 * SIZE(BO)
	LFD	f23, 11 * SIZE(BO)

	addi	AO, AO,  8 * SIZE
	addi	BO, BO,  8 * SIZE
	bdnz	LL(92)
	.align 4

LL(95):
#if defined(LT) || defined(RN)
	andi.	r0, KK,  7
#else
	andi.	r0, TEMP, 7
#endif
	mtspr	CTR, r0
	ble+	LL(98)
	.align 4

LL(96):
	FMADD	f0,  f16, f20, f0
	LFD	f16,  1 * SIZE(AO)
	LFD	f20,  1 * SIZE(BO)
	addi	BO, BO,  1 * SIZE
	addi	AO, AO,  1 * SIZE
	bdnz	LL(96)
	.align 4

LL(98):
	FADD	f0, f1, f0
	FADD	f2, f3, f2
	FADD	f0, f2, f0

#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 1
#else
	subi	r0, KK, 1
#endif
	slwi	TEMP, r0, 0 + BASE_SHIFT
	slwi	r0,   r0, 0 + BASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO, B,     r0
#endif

#if defined(LN) || defined(LT)
	LFD	f16,  0 * SIZE(BO)
	FSUB	f0,  f16, f0
#else
	LFD	f16,  0 * SIZE(AO)
	FSUB	f0,  f16, f0
#endif

#ifdef LN
	LFD	f21,  0 * SIZE(AO)
	FMUL	f0,  f21, f0
#endif

#ifdef LT
	LFD	f16,  0 * SIZE(AO)
	FMUL	f0,  f16, f0
#endif

#ifdef RN
	LFD	f16,  0 * SIZE(BO)
	FMUL	f0,  f16, f0
#endif

#ifdef RT
	LFD	f21,  0 * SIZE(BO)
	FMUL	f0,  f21, f0
#endif

#ifdef LN
	subi	CO1, CO1, 1 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFD	f0,   0 * SIZE(BO)
#else
	STFD	f0,   0 * SIZE(AO)
#endif

	STFD	f0,   0 * SIZE(CO1)

#ifndef LN
	addi	CO1, CO1, 1 * SIZE
#endif

#ifdef RT
	slwi	r0, K, 0 + BASE_SHIFT
	add	AORIG, AORIG, r0
#endif

#if defined(LT) || defined(RN)
	sub	TEMP, K, KK
	slwi	r0,   TEMP, 0 + BASE_SHIFT
	slwi	TEMP, TEMP, 0 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LN
	subi	KK, KK, 1
#endif

#ifdef LT
	addi	KK, KK, 1
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
	ld	r20,  232(SP)
	ld	r19,  240(SP)
	ld	r18,  248(SP)
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
	lwz	r20,  188(SP)
	lwz	r19,  192(SP)
	lwz	r18,  196(SP)
#endif

	addi	SP, SP, STACKSIZE

	blr

	EPILOGUE
#endif
