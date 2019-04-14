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

#define AORIG	r21
#define TEMP	r22
#define KK	r23
#define	I	r24
#define J	r25
#define AO	r26
#define	BO	r27
#define	CO1	r28
#define CO2	r29

#define PREA	r30
#define PREC	r31
#define PREB	PREA

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
#endif

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

	slwi	LDC, LDC, ZBASE_SHIFT

#ifdef LN
	mullw	r0, M, K
	slwi	r0, r0, ZBASE_SHIFT
	add	A, A, r0

	slwi	r0, M, ZBASE_SHIFT
	add	C, C, r0
#endif

#ifdef RN
	neg	KK, OFFSET
#endif

#ifdef RT
	mullw	r0, N, K
	slwi	r0, r0, ZBASE_SHIFT
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

#ifndef PREFETCHTEST
#ifdef PPC970
	li	PREC,   4 * SIZE
#endif
#ifdef POWER4
	li	PREC,   4 * SIZE   /* is 12 best? */
#endif
#ifdef POWER5
	li	PREC,   4 * SIZE   /* is 12 best? */
#endif
#else

#ifdef linux
#ifndef __64BIT__
	lwz	PREA,  FRAMESLOT(2) + STACKSIZE(SP)
	lwz	PREC,  FRAMESLOT(3) + STACKSIZE(SP)
#else
	ld	PREA,  FRAMESLOT(3) + STACKSIZE(SP)
	ld	PREC,  FRAMESLOT(4) + STACKSIZE(SP)
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#ifdef __64BIT__
	ld	PREA,  FRAMESLOT(3) + STACKSIZE(SP)
	ld	PREC,  FRAMESLOT(4) + STACKSIZE(SP)
#else
#ifdef DOUBLE
	lwz	PREA,  FRAMESLOT(4) + STACKSIZE(SP)
	lwz	PREC,  FRAMESLOT(5) + STACKSIZE(SP)
#else
	lwz	PREA,  FRAMESLOT(3) + STACKSIZE(SP)
	lwz	PREC,  FRAMESLOT(4) + STACKSIZE(SP)
#endif
#endif
#endif

#endif

#ifndef PREFETCHTEST
#ifdef PPC970
#ifdef ALLOC_HUGETLB
	li	PREA,   (16 *  5 * SIZE + 16)
#else
	li	PREA,   (16 *  9 * SIZE + 16)
#endif
#endif
#ifdef POWER4
#ifdef ALLOC_HUGETLB
	li	PREA,   (16 *  1 * SIZE + 16)
#else
	li	PREA,   (16 *  2 * SIZE + 16)
#endif
#endif
#ifdef POWER5
#ifdef ALLOC_HUGETLB
	li	PREA,   (16 *  7 * SIZE | 1)
	li	PREB,   (16 *  7 * SIZE | 3)
#else
	li	PREA,   (16 * 12 * SIZE | 1)
	li	PREB,   (16 *  6 * SIZE | 3)
#endif
#endif
#endif

	andi.	J, N,  1
	ble	LL(30)

#ifdef RT
	slwi	r0, K, 0 + ZBASE_SHIFT
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

	srawi.	I,  M,  1
#if defined(LN) || defined(RT)
	mr	AORIG, A
#else
	mr	AO, A
#endif
#ifndef RT
	add	C, C, LDC
#endif
	ble	LL(40)
	.align 4

LL(31):
#if defined(LT) || defined(RN)
	LFD	f20,  0 * SIZE(AO)
	LFD	f21,  1 * SIZE(AO)
	LFD	f22,  2 * SIZE(AO)
	LFD	f23,  3 * SIZE(AO)

	LFD	f24,  4 * SIZE(AO)
	LFD	f25,  5 * SIZE(AO)
	LFD	f26,  6 * SIZE(AO)
	LFD	f27,  7 * SIZE(AO)

	LFD	f16, 0 * SIZE(B)
	LFD	f17, 1 * SIZE(B)
	LFD	f18, 2 * SIZE(B)
	LFD	f19, 3 * SIZE(B)

	lfs	f0, FZERO
	fmr	f1, f0
	fmr	f2, f0
	fmr	f3, f0
	fmr	f4, f0
	fmr	f5, f0
	fmr	f6, f0
	fmr	f7, f0

	dcbt	CO1, PREC

	srawi.	r0, KK,  2
	mr	BO,  B
	mtspr	CTR, r0
#else

#ifdef LN
	slwi	r0,   K,  1 + ZBASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0,   KK, 1 + ZBASE_SHIFT
	slwi	TEMP, KK, 0 + ZBASE_SHIFT

	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

	LFD	f20,  0 * SIZE(AO)
	LFD	f21,  1 * SIZE(AO)
	LFD	f22,  2 * SIZE(AO)
	LFD	f23,  3 * SIZE(AO)

	LFD	f24,  4 * SIZE(AO)
	LFD	f25,  5 * SIZE(AO)
	LFD	f26,  6 * SIZE(AO)
	LFD	f27,  7 * SIZE(AO)

	LFD	f16, 0 * SIZE(BO)
	LFD	f17, 1 * SIZE(BO)
	LFD	f18, 2 * SIZE(BO)
	LFD	f19, 3 * SIZE(BO)

	lfs	f0, FZERO
	fmr	f1, f0
	fmr	f2, f0
	fmr	f3, f0
	fmr	f4, f0
	fmr	f5, f0
	fmr	f6, f0
	fmr	f7, f0

	srawi.	r0, TEMP,  2
	mtspr	CTR, r0
#endif
	ble	LL(35)
	.align 4

LL(32):
	fmadd	f0,  f16, f20, f0
	fmadd	f1,  f16, f21, f1
	fmadd	f2,  f16, f22, f2
	fmadd	f3,  f16, f23, f3

	fmadd	f4,  f17, f20, f4
	fmadd	f5,  f17, f21, f5
	fmadd	f6,  f17, f22, f6
	fmadd	f7,  f17, f23, f7

	LFD	f20,  8 * SIZE(AO)
	LFD	f21,  9 * SIZE(AO)
	LFD	f22, 10 * SIZE(AO)
	LFD	f23, 11 * SIZE(AO)

	fmadd	f0,  f18, f24, f0
	fmadd	f1,  f18, f25, f1
	fmadd	f2,  f18, f26, f2
	fmadd	f3,  f18, f27, f3

	fmadd	f4,  f19, f24, f4
	fmadd	f5,  f19, f25, f5
	fmadd	f6,  f19, f26, f6
	fmadd	f7,  f19, f27, f7

	LFD	f24, 12 * SIZE(AO)
	LFD	f25, 13 * SIZE(AO)
	LFD	f26, 14 * SIZE(AO)
	LFD	f27, 15 * SIZE(AO)

	LFD	f16,  4 * SIZE(BO)
	LFD	f17,  5 * SIZE(BO)
	LFD	f18,  6 * SIZE(BO)
	LFD	f19,  7 * SIZE(BO)

	fmadd	f0,  f16, f20, f0
	fmadd	f1,  f16, f21, f1
	fmadd	f2,  f16, f22, f2
	fmadd	f3,  f16, f23, f3

	fmadd	f4,  f17, f20, f4
	fmadd	f5,  f17, f21, f5
	fmadd	f6,  f17, f22, f6
	fmadd	f7,  f17, f23, f7

	LFD	f20, 16 * SIZE(AO)
	LFD	f21, 17 * SIZE(AO)
	LFD	f22, 18 * SIZE(AO)
	LFD	f23, 19 * SIZE(AO)

	fmadd	f0,  f18, f24, f0
	fmadd	f1,  f18, f25, f1
	fmadd	f2,  f18, f26, f2
	fmadd	f3,  f18, f27, f3

	fmadd	f4,  f19, f24, f4
	fmadd	f5,  f19, f25, f5
	fmadd	f6,  f19, f26, f6
	fmadd	f7,  f19, f27, f7

	LFD	f24, 20 * SIZE(AO)
	LFD	f25, 21 * SIZE(AO)
	LFD	f26, 22 * SIZE(AO)
	LFD	f27, 23 * SIZE(AO)

	LFD	f16,  8 * SIZE(BO)
	LFD	f17,  9 * SIZE(BO)
	LFD	f18, 10 * SIZE(BO)
	LFD	f19, 11 * SIZE(BO)

	addi	AO, AO, 16 * SIZE
	addi	BO, BO,  8 * SIZE
 	dcbt	PREA, AO
 	dcbt	PREA, BO
	bdnz	LL(32)
	.align 4

LL(35):
#if defined(LT) || defined(RN)
	andi.	r0, KK,  3
#else
	andi.	r0, TEMP, 3
#endif
	mtspr	CTR, r0
	ble	LL(37)
	.align 4

LL(36):
	fmadd	f0,  f16, f20, f0
	fmadd	f1,  f16, f21, f1
	fmadd	f2,  f16, f22, f2
	fmadd	f3,  f16, f23, f3

	fmadd	f4,  f17, f20, f4
	fmadd	f5,  f17, f21, f5
	fmadd	f6,  f17, f22, f6
	fmadd	f7,  f17, f23, f7

	LFD	f20, 4 * SIZE(AO)
	LFD	f21, 5 * SIZE(AO)
	LFD	f22, 6 * SIZE(AO)
	LFD	f23, 7 * SIZE(AO)

	LFD	f16, 2 * SIZE(BO)
	LFD	f17, 3 * SIZE(BO)

	addi	BO,  BO, 2 * SIZE
	addi	AO,  AO, 4 * SIZE
	bdnz	LL(36)
	.align 4

LL(37):
#ifndef CONJ
	FSUB	  f0,  f0,  f5
	FADD	  f1,  f1,  f4
	FSUB	  f2,  f2,  f7
	FADD	  f3,  f3,  f6
#else
	FADD	  f0,  f0,  f5
	FSUB	  f1,  f4,  f1
	FADD	  f2,  f2,  f7
	FSUB	  f3,  f6,  f3
#endif

#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 2
#else
	subi	r0, KK, 1
#endif
	slwi	TEMP, r0, 1 + ZBASE_SHIFT
	slwi	r0,   r0, 0 + ZBASE_SHIFT

	add	AO, AORIG, TEMP
	add	BO, B,     r0
#endif

#if defined(LN) || defined(LT)
	LFD	f16,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(BO)
	LFD	f18,  2 * SIZE(BO)
	LFD	f19,  3 * SIZE(BO)

	FSUB	f0,  f16, f0
	FSUB	f1,  f17, f1
	FSUB	f2,  f18, f2
	FSUB	f3,  f19, f3
#else
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

#ifndef CONJ
	FSUB	f0,  f16, f0
	FSUB	f1,  f17, f1
	FSUB	f2,  f18, f2
	FSUB	f3,  f19, f3
#else
	FSUB	f0,  f16, f0
	FADD	f1,  f17, f1
	FSUB	f2,  f18, f2
	FADD	f3,  f19, f3
#endif
#endif

#ifdef LN
	LFD	f16,  6 * SIZE(AO)
	LFD	f17,  7 * SIZE(AO)
	LFD	f18,  4 * SIZE(AO)
	LFD	f19,  5 * SIZE(AO)
	LFD	f20,  0 * SIZE(AO)
	LFD	f21,  1 * SIZE(AO)

	FMUL	f6,  f17, f3
	FMUL	f7,  f17, f2

#ifndef CONJ
	FMSUB	f2,  f16, f2,  f6
	FMADD	f3,  f16, f3,  f7

	FMADD	f0,  f19, f3,  f0
	FNMSUB	f1,  f19, f2,  f1
	FNMSUB	f0,  f18, f2,  f0
	FNMSUB	f1,  f18, f3,  f1

	FMUL	f4,  f21, f1
	FMUL	f5,  f21, f0
	FMSUB	f0,  f20, f0,  f4
	FMADD	f1,  f20, f1,  f5
#else
	FMADD	f2,  f16, f2,  f6
	FMSUB	f3,  f16, f3,  f7

	FMSUB	f0,  f19, f3,  f0
	FNMADD	f1,  f19, f2,  f1
	FNMADD	f0,  f18, f2,  f0
	FNMADD	f1,  f18, f3,  f1

	FMUL	f4,  f21, f1
	FMUL	f5,  f21, f0
	FMADD	f0,  f20, f0,  f4
	FMSUB	f1,  f20, f1,  f5
#endif
#endif

#ifdef LT
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)
	LFD	f20,  6 * SIZE(AO)
	LFD	f21,  7 * SIZE(AO)

	FMUL	f4,  f17, f1
	FMUL	f5,  f17, f0

#ifndef CONJ
	FMSUB	f0,  f16, f0,  f4
	FMADD	f1,  f16, f1,  f5

	FMADD	f2,  f19, f1, f2
	FNMSUB	f3,  f19, f0, f3
	FNMSUB	f2,  f18, f0,  f2
	FNMSUB	f3,  f18, f1,  f3

	FMUL	f4,  f21, f3
	FMUL	f5,  f21, f2
	FMSUB	f2,  f20, f2,  f4
	FMADD	f3,  f20, f3,  f5
#else
	FMADD	f0,  f16, f0,  f4
	FMSUB	f1,  f16, f1,  f5

	FMSUB	f2,  f19, f1, f2
	FNMADD	f3,  f19, f0, f3
	FNMADD	f2,  f18, f0,  f2
	FNMADD	f3,  f18, f1,  f3

	FMUL	f4,  f21, f3
	FMUL	f5,  f21, f2
	FMADD	f2,  f20, f2,  f4
	FMSUB	f3,  f20, f3,  f5
#endif
#endif

#ifdef RN
	LFD	f16,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(BO)

	FMUL	f4,  f17, f1
	FMUL	f5,  f17, f0
	FMUL	f6,  f17, f3
	FMUL	f7,  f17, f2

#ifndef CONJ
	FMSUB	f0,  f16, f0,  f4
	FMADD	f1,  f16, f1,  f5
	FMSUB	f2,  f16, f2,  f6
	FMADD	f3,  f16, f3,  f7
#else
	FMADD	f0,  f16, f0,  f4
	FMSUB	f1,  f16, f1,  f5
	FMADD	f2,  f16, f2,  f6
	FMSUB	f3,  f16, f3,  f7
#endif
#endif

#ifdef RT
	LFD	f20,  0 * SIZE(BO)
	LFD	f21,  1 * SIZE(BO)

	FMUL	f4,  f21, f1
	FMUL	f5,  f21, f0
	FMUL	f6,  f21, f3
	FMUL	f7,  f21, f2

#ifndef CONJ
	FMSUB	f0,  f20, f0,  f4
	FMADD	f1,  f20, f1,  f5
	FMSUB	f2,  f20, f2,  f6
	FMADD	f3,  f20, f3,  f7

#else
	FMADD	f0,  f20, f0,  f4
	FMSUB	f1,  f20, f1,  f5
	FMADD	f2,  f20, f2,  f6
	FMSUB	f3,  f20, f3,  f7
#endif
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

#ifndef LN
	addi	CO1, CO1, 4 * SIZE
#endif

#ifdef RT
	slwi	r0, K, 1 + ZBASE_SHIFT
	add	AORIG, AORIG, r0
#endif

#if defined(LT) || defined(RN)
	sub	TEMP, K, KK
	slwi	r0,   TEMP, 1 + ZBASE_SHIFT
	slwi	TEMP, TEMP, 0 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LT
	addi	KK, KK, 2
#endif

#ifdef LN
	subi	KK, KK, 2
#endif

	addic.	I, I, -1
	bgt	LL(31)
	.align 4

LL(40):
	andi.	I,  M,  1
	ble	LL(49)

#if defined(LT) || defined(RN)
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(B)
	LFD	f21,  1 * SIZE(B)
	LFD	f22,  2 * SIZE(B)
	LFD	f23,  3 * SIZE(B)

	lfs	f0, FZERO
	fmr	f1, f0
	fmr	f2, f0
	fmr	f3, f0
	fmr	f4, f0
	fmr	f5, f0
	fmr	f6, f0
	fmr	f7, f0

	srawi.	r0, KK,  2
	mr	BO,  B
	mtspr	CTR, r0
#else

#ifdef LN
	slwi	r0,   K,  0 + ZBASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0,   KK, 0 + ZBASE_SHIFT

	add	AO, AORIG, r0
	add	BO, B,     r0

	sub	TEMP, K, KK

	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(BO)
	LFD	f21,  1 * SIZE(BO)
	LFD	f22,  2 * SIZE(BO)
	LFD	f23,  3 * SIZE(BO)

	lfs	f0, FZERO
	fmr	f1, f0
	fmr	f2, f0
	fmr	f3, f0
	fmr	f4, f0
	fmr	f5, f0
	fmr	f6, f0
	fmr	f7, f0

	srawi.	r0, TEMP,  2
	mtspr	CTR, r0
#endif
	ble	LL(45)
	.align 4

LL(42):
	fmadd	f0,  f16,  f20,  f0
	fmadd	f1,  f17,  f21,  f1
	fmadd	f2,  f17,  f20,  f2
	fmadd	f3,  f16,  f21,  f3

	LFD	f16,  4 * SIZE(AO)
	LFD	f17,  5 * SIZE(AO)
	LFD	f20,  4 * SIZE(BO)
	LFD	f21,  5 * SIZE(BO)

	fmadd	f4,  f18,  f22,  f4
	fmadd	f5,  f19,  f23,  f5
	fmadd	f6,  f19,  f22,  f6
	fmadd	f7,  f18,  f23,  f7

	LFD	f18,  6 * SIZE(AO)
	LFD	f19,  7 * SIZE(AO)
	LFD	f22,  6 * SIZE(BO)
	LFD	f23,  7 * SIZE(BO)

	fmadd	f0,  f16,  f20,  f0
	fmadd	f1,  f17,  f21,  f1
	fmadd	f2,  f17,  f20,  f2
	fmadd	f3,  f16,  f21,  f3

	LFD	f16,  8 * SIZE(AO)
	LFD	f17,  9 * SIZE(AO)
	LFD	f20,  8 * SIZE(BO)
	LFD	f21,  9 * SIZE(BO)

	fmadd	f4,  f18,  f22,  f4
	fmadd	f5,  f19,  f23,  f5
	fmadd	f6,  f19,  f22,  f6
	fmadd	f7,  f18,  f23,  f7

	LFD	f18, 10 * SIZE(AO)
	LFD	f19, 11 * SIZE(AO)
	LFD	f22, 10 * SIZE(BO)
	LFD	f23, 11 * SIZE(BO)

	addi	AO, AO,  8 * SIZE
	addi	BO, BO,  8 * SIZE
	bdnz	LL(42)
	.align 4

LL(45):
	fadd	f0, f0, f4
	fadd	f1, f1, f5
	fadd	f2, f2, f6
	fadd	f3, f3, f7

#if defined(LT) || defined(RN)
	andi.	r0, KK,  3
#else
	andi.	r0, TEMP, 3
#endif
	mtspr	CTR,r0
	ble	LL(47)
	.align 4

LL(46):
	fmadd	f0,  f16,  f20,  f0
	fmadd	f1,  f17,  f21,  f1
	fmadd	f2,  f17,  f20,  f2
	fmadd	f3,  f16,  f21,  f3

	LFD	f16, 2 * SIZE(AO)
	LFD	f17, 3 * SIZE(AO)
	LFD	f20, 2 * SIZE(BO)
	LFD	f21, 3 * SIZE(BO)

	addi	AO, AO, 2 * SIZE
	addi	BO, BO, 2 * SIZE

	bdnz	LL(46)
	.align 4

LL(47):
#ifndef CONJ
	FSUB	  f0,  f0,  f1
	FADD	  f1,  f2,  f3
#else
	FADD	  f0,  f0,  f1
	FSUB	  f1,  f3,  f2
#endif

#if defined(LN) || defined(RT)
	subi	r0, KK, 1
	slwi	r0,   r0, 0 + ZBASE_SHIFT

	add	AO, AORIG, r0
	add	BO, B,     r0
#endif

#if defined(LN) || defined(LT)
	LFD	f16,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(BO)
	FSUB	f0,  f16, f0
	FSUB	f1,  f17, f1
#else
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
#ifndef CONJ
	FSUB	f0,  f16, f0
	FSUB	f1,  f17, f1
#else
	FSUB	f0,  f16, f0
	FADD	f1,  f17, f1
#endif
#endif

#ifdef LN
	LFD	f20,  0 * SIZE(AO)
	LFD	f21,  1 * SIZE(AO)

	FMUL	f4,  f21, f1
	FMUL	f5,  f21, f0
#ifndef CONJ
	FMSUB	f0,  f20, f0,  f4
	FMADD	f1,  f20, f1,  f5
#else
	FMADD	f0,  f20, f0,  f4
	FMSUB	f1,  f20, f1,  f5
#endif
#endif

#ifdef LT
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)

	FMUL	f4,  f17, f1
	FMUL	f5,  f17, f0

#ifndef CONJ
	FMSUB	f0,  f16, f0,  f4
	FMADD	f1,  f16, f1,  f5
#else
	FMADD	f0,  f16, f0,  f4
	FMSUB	f1,  f16, f1,  f5
#endif
#endif

#ifdef RN
	LFD	f16,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(BO)

	FMUL	f4,  f17, f1
	FMUL	f5,  f17, f0
#ifndef CONJ
	FMSUB	f0,  f16, f0,  f4
	FMADD	f1,  f16, f1,  f5
#else
	FMADD	f0,  f16, f0,  f4
	FMSUB	f1,  f16, f1,  f5
#endif
#endif

#ifdef RT
	LFD	f20,  0 * SIZE(BO)
	LFD	f21,  1 * SIZE(BO)

	FMUL	f4,  f21, f1
	FMUL	f5,  f21, f0
#ifndef CONJ
	FMSUB	f0,  f20, f0,  f4
	FMADD	f1,  f20, f1,  f5
#else
	FMADD	f0,  f20, f0,  f4
	FMSUB	f1,  f20, f1,  f5
#endif
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

#ifndef LN
	addi	CO1, CO1, 2 * SIZE
#endif

#ifdef RT
	slwi	r0, K, 0 + ZBASE_SHIFT
	add	AORIG, AORIG, r0
#endif

#if defined(LT) || defined(RN)
	sub	TEMP, K, KK
	slwi	TEMP, TEMP, 0 + ZBASE_SHIFT
	add	AO, AO, TEMP
	add	BO, BO, TEMP
#endif

#ifdef LT
	addi	KK, KK, 1
#endif

#ifdef LN
	subi	KK, KK, 1
#endif
	.align 4

LL(49):
#ifdef LN
	slwi	r0, K, 0 + ZBASE_SHIFT
	add	B, B, r0
#endif

#if defined(LT) || defined(RN)
	mr	B,  BO
#endif

#ifdef RN
	addi	KK, KK, 1
#endif

#ifdef RT
	subi	KK, KK, 1
#endif
	.align 4


LL(30):
	srawi.	J, N,  1
	ble	LL(999)
	.align 4

LL(10):
#ifdef RT
	slwi	r0, K, 1 + ZBASE_SHIFT
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

	srawi.	I, M,  1
#if defined(LN) || defined(RT)
	mr	AORIG, A
#else
	mr	AO, A
#endif
#ifndef RT
	add	C,  CO2, LDC
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

	dcbt	CO1, PREC
	dcbt	CO2, PREC

	srawi.	r0, KK,  2
	mtspr	CTR, r0
	mr	BO,  B
#else

#ifdef LN
	slwi	r0,   K,  1 + ZBASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	TEMP, KK, 1 + ZBASE_SHIFT
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

	dcbt	CO1, PREC
	dcbt	CO2, PREC

	srawi.	r0, TEMP,  2
	mtspr	CTR, r0
#endif
	ble	LL(15)
	.align 4

LL(12):
	fmadd	f0,  f16, f20, f0
	fmadd	f5,  f17, f21, f5
	fmadd	f10, f18, f22, f10
	fmadd	f15, f19, f23, f15

	LFD	f28,  4 * SIZE(BO)
	LFD	f29,  5 * SIZE(BO)
	LFD	f30,  6 * SIZE(BO)
	LFD	f31,  7 * SIZE(BO)

	fmadd	f1,  f17, f20, f1
	fmadd	f2,  f18, f20, f2
	fmadd	f3,  f19, f20, f3
	fmadd	f4,  f16, f21, f4

	LFD	f24,  4 * SIZE(AO)
	LFD	f25,  5 * SIZE(AO)
	LFD	f26,  6 * SIZE(AO)
	LFD	f27,  7 * SIZE(AO)

	fmadd	f6,  f18, f21, f6
	fmadd	f7,  f19, f21, f7
	fmadd	f8,  f16, f22, f8
	fmadd	f9,  f17, f22, f9

	fmadd	f11, f19, f22, f11
	fmadd	f12, f16, f23, f12
	fmadd	f13, f17, f23, f13
	fmadd	f14, f18, f23, f14

	LFD	f20,  8 * SIZE(BO)
	LFD	f21,  9 * SIZE(BO)
	LFD	f22, 10 * SIZE(BO)
	LFD	f23, 11 * SIZE(BO)

	fmadd	f0,  f24, f28, f0
	fmadd	f5,  f25, f29, f5
	fmadd	f10, f26, f30, f10
	fmadd	f15, f27, f31, f15

	LFD	f16,  8 * SIZE(AO)
	LFD	f17,  9 * SIZE(AO)
	LFD	f18, 10 * SIZE(AO)
	LFD	f19, 11 * SIZE(AO)

	fmadd	f1,  f25, f28, f1
	fmadd	f2,  f26, f28, f2
	fmadd	f3,  f27, f28, f3
	fmadd	f4,  f24, f29, f4

	fmadd	f6,  f26, f29, f6
	fmadd	f7,  f27, f29, f7
	fmadd	f8,  f24, f30, f8
	fmadd	f9,  f25, f30, f9

	fmadd	f11, f27, f30, f11
	fmadd	f12, f24, f31, f12
	fmadd	f13, f25, f31, f13
	fmadd	f14, f26, f31, f14

	LFD	f28, 12 * SIZE(BO)
	LFD	f29, 13 * SIZE(BO)
	LFD	f30, 14 * SIZE(BO)
	LFD	f31, 15 * SIZE(BO)

	fmadd	f0,  f16, f20, f0
	fmadd	f5,  f17, f21, f5
	fmadd	f10, f18, f22, f10
	fmadd	f15, f19, f23, f15

	LFD	f24, 12 * SIZE(AO)
	LFD	f25, 13 * SIZE(AO)
	LFD	f26, 14 * SIZE(AO)
	LFD	f27, 15 * SIZE(AO)

	fmadd	f1,  f17, f20, f1
	fmadd	f2,  f18, f20, f2
	fmadd	f3,  f19, f20, f3
	fmadd	f4,  f16, f21, f4

	fmadd	f6,  f18, f21, f6
	fmadd	f7,  f19, f21, f7
	fmadd	f8,  f16, f22, f8
	fmadd	f9,  f17, f22, f9

	fmadd	f11, f19, f22, f11
	fmadd	f12, f16, f23, f12
	fmadd	f13, f17, f23, f13
	fmadd	f14, f18, f23, f14

	LFD	f16, 16 * SIZE(AO)
	LFD	f17, 17 * SIZE(AO)
	LFD	f18, 18 * SIZE(AO)
	LFD	f19, 19 * SIZE(AO)

	fmadd	f0,  f24, f28, f0
	fmadd	f5,  f25, f29, f5
	fmadd	f10, f26, f30, f10
	fmadd	f15, f27, f31, f15

	LFD	f20, 16 * SIZE(BO)
	LFD	f21, 17 * SIZE(BO)
	LFD	f22, 18 * SIZE(BO)
	LFD	f23, 19 * SIZE(BO)

	fmadd	f1,  f25, f28, f1
	fmadd	f2,  f26, f28, f2
	fmadd	f3,  f27, f28, f3
	fmadd	f4,  f24, f29, f4

	fmadd	f6,  f26, f29, f6
	fmadd	f7,  f27, f29, f7
	fmadd	f8,  f24, f30, f8
	fmadd	f9,  f25, f30, f9

	fmadd	f11, f27, f30, f11
	fmadd	f12, f24, f31, f12
	fmadd	f13, f25, f31, f13
	fmadd	f14, f26, f31, f14

	addi	AO, AO, 16 * SIZE
	addi	BO, BO, 16 * SIZE
#ifdef PPC970
#ifndef ALLOC_HUGETLB
	DCBT(AO, PREA)
#endif
	DCBT(BO, PREB)
#endif

#ifdef POWER4
#ifndef ALLOC_HUGETLB
	DCBT(AO, PREA)
#endif
	DCBT(BO, PREB)
#endif

#ifdef POWER5
	DCBT(AO, PREA)
	DCBT(BO, PREB)
#endif
	bdnz	LL(12)
	.align 4

LL(15):
#if defined(LT) || defined(RN)
	andi.	r0, KK,  3
#else
	andi.	r0, TEMP, 3
#endif
	mtspr	CTR, r0
	ble	LL(KERNEL_MainFinish)
	.align 4

LL(16):
	fmadd	f0,  f16, f20, f0
	fmadd	f5,  f17, f21, f5
	fmadd	f10, f18, f22, f10
	fmadd	f15, f19, f23, f15

	fmadd	f1,  f17, f20, f1
	fmadd	f2,  f18, f20, f2
	fmadd	f3,  f19, f20, f3
	fmadd	f4,  f16, f21, f4

	fmadd	f6,  f18, f21, f6
	fmadd	f7,  f19, f21, f7
	fmadd	f8,  f16, f22, f8
	fmadd	f9,  f17, f22, f9

	fmadd	f11, f19, f22, f11
	fmadd	f12, f16, f23, f12
	fmadd	f13, f17, f23, f13
	fmadd	f14, f18, f23, f14

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

LL(KERNEL_MainFinish):
#ifndef CONJ
	FSUB	  f0,  f0,  f5
	FADD	  f1,  f1,  f4
	FSUB	  f2,  f2,  f7
	FADD	  f3,  f3,  f6

	FSUB	  f8,  f8,  f13
	FADD	  f9,  f9,  f12
	FSUB	  f10, f10, f15
	FADD	  f11, f11, f14

#else
	FADD	  f0,  f0,  f5
	FSUB	  f1,  f4,  f1
	FADD	  f2,  f2,  f7
	FSUB	  f3,  f6,  f3

	FADD	  f8,  f8,  f13
	FSUB	  f9,  f12, f9
	FADD	  f10, f10, f15
	FSUB	  f11, f14, f11
#endif

#if defined(LN) || defined(RT)
	subi	r0, KK, 2
	slwi	r0, r0, 1 + ZBASE_SHIFT
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

	FSUB	f0,  f16, f0
	FSUB	f1,  f17, f1
	FSUB	f8,  f18, f8
	FSUB	f9,  f19, f9

	FSUB	f2,  f20, f2
	FSUB	f3,  f21, f3
	FSUB	f10, f22, f10
	FSUB	f11, f23, f11
#else
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  4 * SIZE(AO)
 	LFD	f21,  5 * SIZE(AO)
	LFD	f22,  6 * SIZE(AO)
	LFD	f23,  7 * SIZE(AO)

#ifndef CONJ
	FSUB	f0,  f16, f0
	FSUB	f1,  f17, f1
	FSUB	f2,  f18, f2
	FSUB	f3,  f19, f3

	FSUB	f8,  f20, f8
	FSUB	f9,  f21, f9
	FSUB	f10, f22, f10
	FSUB	f11, f23, f11
#else
	FSUB	f0,  f16, f0
	FADD	f1,  f17, f1
	FSUB	f2,  f18, f2
	FADD	f3,  f19, f3

	FSUB	f8,  f20, f8
	FADD	f9,  f21, f9
	FSUB	f10, f22, f10
	FADD	f11, f23, f11
#endif
#endif

#ifdef LN
	LFD	f16,  6 * SIZE(AO)
	LFD	f17,  7 * SIZE(AO)
	LFD	f18,  4 * SIZE(AO)
	LFD	f19,  5 * SIZE(AO)
	LFD	f20,  0 * SIZE(AO)
	LFD	f21,  1 * SIZE(AO)

	FMUL	f6,  f17, f3
	FMUL	f7,  f17, f2
	FMUL	f14, f17, f11
	FMUL	f15, f17, f10

#ifndef CONJ
	FMSUB	f2,  f16, f2,  f6
	FMADD	f3,  f16, f3,  f7
	FMSUB	f10, f16, f10, f14
	FMADD	f11, f16, f11, f15

	FMADD	f0,  f19, f3,  f0
	FNMSUB	f1,  f19, f2,  f1
	FMADD	f8,  f19, f11, f8
	FNMSUB	f9,  f19, f10, f9

	FNMSUB	f0,  f18, f2,  f0
	FNMSUB	f1,  f18, f3,  f1
	FNMSUB	f8,  f18, f10, f8
	FNMSUB	f9,  f18, f11, f9

	FMUL	f4,  f21, f1
	FMUL	f5,  f21, f0
	FMUL	f12, f21, f9
	FMUL	f13, f21, f8

	FMSUB	f0,  f20, f0,  f4
	FMADD	f1,  f20, f1,  f5
	FMSUB	f8,  f20, f8,  f12
	FMADD	f9,  f20, f9,  f13

#else
	FMADD	f2,  f16, f2,  f6
	FMSUB	f3,  f16, f3,  f7
	FMADD	f10, f16, f10, f14
	FMSUB	f11, f16, f11, f15

	FMSUB	f0,  f19, f3,  f0
	FNMADD	f1,  f19, f2,  f1
	FMSUB	f8,  f19, f11, f8
	FNMADD	f9,  f19, f10, f9

	FNMADD	f0,  f18, f2,  f0
	FNMADD	f1,  f18, f3,  f1
	FNMADD	f8,  f18, f10, f8
	FNMADD	f9,  f18, f11, f9

	FMUL	f4,  f21, f1
	FMUL	f5,  f21, f0
	FMUL	f12, f21, f9
	FMUL	f13, f21, f8

	FMADD	f0,  f20, f0,  f4
	FMSUB	f1,  f20, f1,  f5
	FMADD	f8,  f20, f8,  f12
	FMSUB	f9,  f20, f9,  f13
#endif
#endif

#ifdef LT
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)
	LFD	f20,  6 * SIZE(AO)
	LFD	f21,  7 * SIZE(AO)

	FMUL	f4,  f17, f1
	FMUL	f5,  f17, f0
	FMUL	f12, f17, f9
	FMUL	f13, f17, f8

#ifndef CONJ
	FMSUB	f0,  f16, f0,  f4
	FMADD	f1,  f16, f1,  f5
	FMSUB	f8,  f16, f8,  f12
	FMADD	f9,  f16, f9,  f13

	FMADD	f2,  f19, f1, f2
	FNMSUB	f3,  f19, f0, f3
	FMADD	f10, f19, f9, f10
	FNMSUB	f11, f19, f8, f11

	FNMSUB	f2,  f18, f0,  f2
	FNMSUB	f3,  f18, f1,  f3
	FNMSUB	f10, f18, f8,  f10
	FNMSUB	f11, f18, f9,  f11

	FMUL	f4,  f21, f3
	FMUL	f5,  f21, f2
	FMUL	f12, f21, f11
	FMUL	f13, f21, f10

	FMSUB	f2,  f20, f2,  f4
	FMADD	f3,  f20, f3,  f5
	FMSUB	f10, f20, f10, f12
	FMADD	f11, f20, f11, f13

#else
	FMADD	f0,  f16, f0,  f4
	FMSUB	f1,  f16, f1,  f5
	FMADD	f8,  f16, f8,  f12
	FMSUB	f9,  f16, f9,  f13

	FMSUB	f2,  f19, f1, f2
	FNMADD	f3,  f19, f0, f3
	FMSUB	f10, f19, f9, f10
	FNMADD	f11, f19, f8, f11

	FNMADD	f2,  f18, f0,  f2
	FNMADD	f3,  f18, f1,  f3
	FNMADD	f10, f18, f8,  f10
	FNMADD	f11, f18, f9,  f11

	FMUL	f4,  f21, f3
	FMUL	f5,  f21, f2
	FMUL	f12, f21, f11
	FMUL	f13, f21, f10

	FMADD	f2,  f20, f2,  f4
	FMSUB	f3,  f20, f3,  f5
	FMADD	f10, f20, f10, f12
	FMSUB	f11, f20, f11, f13
#endif
#endif

#ifdef RN
	LFD	f16,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(BO)
	LFD	f18,  2 * SIZE(BO)
	LFD	f19,  3 * SIZE(BO)
	LFD	f20,  6 * SIZE(BO)
	LFD	f21,  7 * SIZE(BO)

	FMUL	f4,  f17, f1
	FMUL	f5,  f17, f0
	FMUL	f6,  f17, f3
	FMUL	f7,  f17, f2

#ifndef CONJ
	FMSUB	f0,  f16, f0,  f4
	FMADD	f1,  f16, f1,  f5
	FMSUB	f2,  f16, f2,  f6
	FMADD	f3,  f16, f3,  f7

	FMADD	f8,  f19, f1, f8
	FNMSUB	f9,  f19, f0, f9
	FMADD	f10, f19, f3, f10
	FNMSUB	f11, f19, f2, f11

	FNMSUB	f8,  f18, f0,  f8
	FNMSUB	f9,  f18, f1,  f9
	FNMSUB	f10, f18, f2,  f10
	FNMSUB	f11, f18, f3,  f11

	FMUL	f4,  f21, f9
	FMUL	f5,  f21, f8
	FMUL	f6,  f21, f11
	FMUL	f7,  f21, f10

	FMSUB	f8,  f20, f8,  f4
	FMADD	f9,  f20, f9,  f5
	FMSUB	f10, f20, f10, f6
	FMADD	f11, f20, f11, f7
#else
	FMADD	f0,  f16, f0,  f4
	FMSUB	f1,  f16, f1,  f5
	FMADD	f2,  f16, f2,  f6
	FMSUB	f3,  f16, f3,  f7

	FMSUB	f8,  f19, f1, f8
	FNMADD	f9,  f19, f0, f9
	FMSUB	f10, f19, f3, f10
	FNMADD	f11, f19, f2, f11

	FNMADD	f8,  f18, f0,  f8
	FNMADD	f9,  f18, f1,  f9
	FNMADD	f10, f18, f2,  f10
	FNMADD	f11, f18, f3,  f11

	FMUL	f4,  f21, f9
	FMUL	f5,  f21, f8
	FMUL	f6,  f21, f11
	FMUL	f7,  f21, f10

	FMADD	f8,  f20, f8,  f4
	FMSUB	f9,  f20, f9,  f5
	FMADD	f10, f20, f10, f6
	FMSUB	f11, f20, f11, f7
#endif
#endif

#ifdef RT
	LFD	f16,  6 * SIZE(BO)
	LFD	f17,  7 * SIZE(BO)
	LFD	f18,  4 * SIZE(BO)
	LFD	f19,  5 * SIZE(BO)
	LFD	f20,  0 * SIZE(BO)
	LFD	f21,  1 * SIZE(BO)

	FMUL	f12, f17, f9
	FMUL	f13, f17, f8
	FMUL	f14, f17, f11
	FMUL	f15, f17, f10

#ifndef CONJ
	FMSUB	f8,  f16, f8,  f12
	FMADD	f9,  f16, f9,  f13
	FMSUB	f10, f16, f10, f14
	FMADD	f11, f16, f11, f15

	FMADD	f0,  f19, f9,  f0
	FNMSUB	f1,  f19, f8,  f1
	FMADD	f2,  f19, f11, f2
	FNMSUB	f3,  f19, f10, f3

	FNMSUB	f0,  f18, f8,  f0
	FNMSUB	f1,  f18, f9,  f1
	FNMSUB	f2,  f18, f10, f2
	FNMSUB	f3,  f18, f11, f3

	FMUL	f4,  f21, f1
	FMUL	f5,  f21, f0
	FMUL	f6,  f21, f3
	FMUL	f7,  f21, f2

	FMSUB	f0,  f20, f0,  f4
	FMADD	f1,  f20, f1,  f5
	FMSUB	f2,  f20, f2,  f6
	FMADD	f3,  f20, f3,  f7

#else
	FMADD	f8,  f16, f8,  f12
	FMSUB	f9,  f16, f9,  f13
	FMADD	f10, f16, f10, f14
	FMSUB	f11, f16, f11, f15

	FMSUB	f0,  f19, f9,  f0
	FNMADD	f1,  f19, f8,  f1
	FMSUB	f2,  f19, f11, f2
	FNMADD	f3,  f19, f10, f3

	FNMADD	f0,  f18, f8,  f0
	FNMADD	f1,  f18, f9,  f1
	FNMADD	f2,  f18, f10, f2
	FNMADD	f3,  f18, f11, f3

	FMUL	f4,  f21, f1
	FMUL	f5,  f21, f0
	FMUL	f6,  f21, f3
	FMUL	f7,  f21, f2

	FMADD	f0,  f20, f0,  f4
	FMSUB	f1,  f20, f1,  f5
	FMADD	f2,  f20, f2,  f6
	FMSUB	f3,  f20, f3,  f7
#endif
#endif

#ifdef LN
	subi	CO1, CO1, 4 * SIZE
	subi	CO2, CO2, 4 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFD	f0,   0 * SIZE(BO)
	STFD	f1,   1 * SIZE(BO)
	STFD	f8,   2 * SIZE(BO)
	STFD	f9,   3 * SIZE(BO)

	STFD	f2,   4 * SIZE(BO)
	STFD	f3,   5 * SIZE(BO)
	STFD	f10,  6 * SIZE(BO)
	STFD	f11,  7 * SIZE(BO)
#else
	STFD	f0,   0 * SIZE(AO)
	STFD	f1,   1 * SIZE(AO)
	STFD	f2,   2 * SIZE(AO)
	STFD	f3,   3 * SIZE(AO)

	STFD	f8,   4 * SIZE(AO)
	STFD	f9,   5 * SIZE(AO)
	STFD	f10,  6 * SIZE(AO)
	STFD	f11,  7 * SIZE(AO)
#endif

	STFD	f0,   0 * SIZE(CO1)
	STFD	f1,   1 * SIZE(CO1)
	STFD	f2,   2 * SIZE(CO1)
	STFD	f3,   3 * SIZE(CO1)

	STFD	f8,   0 * SIZE(CO2)
	STFD	f9,   1 * SIZE(CO2)
	STFD	f10,  2 * SIZE(CO2)
	STFD	f11,  3 * SIZE(CO2)

#ifndef LN
	addi	CO1, CO1, 4 * SIZE
	addi	CO2, CO2, 4 * SIZE
#endif

#ifdef RT
	slwi	r0, K, 1 + ZBASE_SHIFT
	add	AORIG, AORIG, r0
#endif

#if defined(LT) || defined(RN)
	sub	TEMP, K, KK
	slwi	TEMP, TEMP, 1 + ZBASE_SHIFT
	add	AO, AO, TEMP
	add	BO, BO, TEMP
#endif

#ifdef LT
	addi	KK, KK, 2
#endif

#ifdef LN
	subi	KK, KK, 2
#endif

	addic.	I, I, -1
	bgt	LL(11)
	.align 4

LL(20):
	andi.	I,  M,  1
	ble	LL(29)

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

	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

	fmr	f4,  f0
	fmr	f5,  f0
	fmr	f6,  f0
	fmr	f7,  f0

	srawi.	r0, KK,  2
	mr	BO,  B
	mtspr	CTR, r0
#else

#ifdef LN
	slwi	r0,   K,  0 + ZBASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0,   KK, 0 + ZBASE_SHIFT
	slwi	TEMP, KK, 1 + ZBASE_SHIFT
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

	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

	fmr	f4,  f0
	fmr	f5,  f0
	fmr	f6,  f0
	fmr	f7,  f0

	srawi.	r0, TEMP,  2
	mtspr	CTR, r0
#endif
	ble	LL(25)
	.align 4

LL(22):
	fmadd	f0,  f16, f20, f0
	fmadd	f1,  f16, f21, f1
	fmadd	f2,  f16, f22, f2
	fmadd	f3,  f16, f23, f3

	fmadd	f4,  f17, f20, f4
	fmadd	f5,  f17, f21, f5
	fmadd	f6,  f17, f22, f6
	fmadd	f7,  f17, f23, f7

 	LFD	f20,  8 * SIZE(BO)
	LFD	f21,  9 * SIZE(BO)
	LFD	f22, 10 * SIZE(BO)
	LFD	f23, 11 * SIZE(BO)

	fmadd	f0,  f18, f24, f0
	fmadd	f1,  f18, f25, f1
	fmadd	f2,  f18, f26, f2
	fmadd	f3,  f18, f27, f3

	fmadd	f4,  f19, f24, f4
	fmadd	f5,  f19, f25, f5
	fmadd	f6,  f19, f26, f6
	fmadd	f7,  f19, f27, f7

 	LFD	f24, 12 * SIZE(BO)
	LFD	f25, 13 * SIZE(BO)
	LFD	f26, 14 * SIZE(BO)
	LFD	f27, 15 * SIZE(BO)

	LFD	f16,  4 * SIZE(AO)
	LFD	f17,  5 * SIZE(AO)
	LFD	f18,  6 * SIZE(AO)
	LFD	f19,  7 * SIZE(AO)

	fmadd	f0,  f16, f20, f0
	fmadd	f1,  f16, f21, f1
	fmadd	f2,  f16, f22, f2
	fmadd	f3,  f16, f23, f3

	fmadd	f4,  f17, f20, f4
	fmadd	f5,  f17, f21, f5
	fmadd	f6,  f17, f22, f6
	fmadd	f7,  f17, f23, f7

 	LFD	f20, 16 * SIZE(BO)
	LFD	f21, 17 * SIZE(BO)
	LFD	f22, 18 * SIZE(BO)
	LFD	f23, 19 * SIZE(BO)

	fmadd	f0,  f18, f24, f0
	fmadd	f1,  f18, f25, f1
	fmadd	f2,  f18, f26, f2
	fmadd	f3,  f18, f27, f3

	fmadd	f4,  f19, f24, f4
	fmadd	f5,  f19, f25, f5
	fmadd	f6,  f19, f26, f6
	fmadd	f7,  f19, f27, f7

	LFD	f16,  8 * SIZE(AO)
	LFD	f17,  9 * SIZE(AO)
	LFD	f18, 10 * SIZE(AO)
	LFD	f19, 11 * SIZE(AO)

 	LFD	f24, 20 * SIZE(BO)
	LFD	f25, 21 * SIZE(BO)
	LFD	f26, 22 * SIZE(BO)
	LFD	f27, 23 * SIZE(BO)

	addi	BO,  BO, 16 * SIZE
	addi	AO,  AO,  8 * SIZE
	bdnz	LL(22)
	.align 4

LL(25):
#if defined(LT) || defined(RN)
	andi.	r0, KK,  3
#else
	andi.	r0, TEMP, 3
#endif
	mtspr	CTR, r0
	ble	LL(27)
	.align 4

LL(26):
	fmadd	f0,  f16, f20, f0
	fmadd	f1,  f16, f21, f1
	fmadd	f2,  f16, f22, f2
	fmadd	f3,  f16, f23, f3

	fmadd	f4,  f17, f20, f4
	fmadd	f5,  f17, f21, f5
	fmadd	f6,  f17, f22, f6
	fmadd	f7,  f17, f23, f7

 	LFD	f20,  4 * SIZE(BO)
	LFD	f21,  5 * SIZE(BO)
	LFD	f22,  6 * SIZE(BO)
	LFD	f23,  7 * SIZE(BO)

	LFD	f16,  2 * SIZE(AO)
	LFD	f17,  3 * SIZE(AO)
	addi	AO, AO,  2 * SIZE
	addi	BO, BO,  4 * SIZE
	bdnz	LL(26)
	.align 4

LL(27):
#ifndef CONJ
	FSUB	  f0,  f0,  f5
	FADD	  f1,  f1,  f4
	FSUB	  f2,  f2,  f7
	FADD	  f3,  f3,  f6
#else
#if defined(LN) || defined(LT)
	FADD	  f0,  f0,  f5
	FSUB	  f1,  f1,  f4
	FADD	  f2,  f2,  f7
	FSUB	  f3,  f3,  f6
#else
	FADD	  f0,  f0,  f5
	FSUB	  f1,  f4,  f1
	FADD	  f2,  f2,  f7
	FSUB	  f3,  f6,  f3
#endif
#endif

#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 1
#else
	subi	r0, KK, 2
#endif
	slwi	TEMP, r0, 0 + ZBASE_SHIFT
	slwi	r0,   r0, 1 + ZBASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO, B,     r0
#endif


#if defined(LN) || defined(LT)
	LFD	f16,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(BO)
	LFD	f18,  2 * SIZE(BO)
	LFD	f19,  3 * SIZE(BO)

	FSUB	f0,  f16, f0
	FSUB	f1,  f17, f1
	FSUB	f2,  f18, f2
	FSUB	f3,  f19, f3
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
	LFD	f20,  0 * SIZE(AO)
	LFD	f21,  1 * SIZE(AO)

	FMUL	f4,  f21, f1
	FMUL	f5,  f21, f0
	FMUL	f12, f21, f3
	FMUL	f13, f21, f2

#ifndef CONJ
	FMSUB	f0,  f20, f0,  f4
	FMADD	f1,  f20, f1,  f5
	FMSUB	f2,  f20, f2,  f12
	FMADD	f3,  f20, f3,  f13
#else
	FMADD	f0,  f20, f0,  f4
	FMSUB	f1,  f20, f1,  f5
	FMADD	f2,  f20, f2,  f12
	FMSUB	f3,  f20, f3,  f13
#endif
#endif

#ifdef LT
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)

	FMUL	f4,  f17, f1
	FMUL	f5,  f17, f0
	FMUL	f12, f17, f3
	FMUL	f13, f17, f2

#ifndef CONJ
	FMSUB	f0,  f16, f0,  f4
	FMADD	f1,  f16, f1,  f5
	FMSUB	f2,  f16, f2,  f12
	FMADD	f3,  f16, f3,  f13
#else
	FMADD	f0,  f16, f0,  f4
	FMSUB	f1,  f16, f1,  f5
	FMADD	f2,  f16, f2,  f12
	FMSUB	f3,  f16, f3,  f13
#endif
#endif

#ifdef RN
	LFD	f16,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(BO)
	LFD	f18,  2 * SIZE(BO)
	LFD	f19,  3 * SIZE(BO)
	LFD	f20,  6 * SIZE(BO)
	LFD	f21,  7 * SIZE(BO)

	FMUL	f4,  f17, f1
	FMUL	f5,  f17, f0

#ifndef CONJ
	FMSUB	f0,  f16, f0,  f4
	FMADD	f1,  f16, f1,  f5

	FMADD	f2,  f19, f1, f2
	FNMSUB	f3,  f19, f0, f3

	FNMSUB	f2,  f18, f0,  f2
	FNMSUB	f3,  f18, f1,  f3

	FMUL	f4,  f21, f3
	FMUL	f5,  f21, f2

	FMSUB	f2,  f20, f2,  f4
	FMADD	f3,  f20, f3,  f5
#else
	FMADD	f0,  f16, f0,  f4
	FMSUB	f1,  f16, f1,  f5

	FMSUB	f2,  f19, f1, f2
	FNMADD	f3,  f19, f0, f3

	FNMADD	f2,  f18, f0,  f2
	FNMADD	f3,  f18, f1,  f3

	FMUL	f4,  f21, f3
	FMUL	f5,  f21, f2

	FMADD	f2,  f20, f2,  f4
	FMSUB	f3,  f20, f3,  f5
#endif
#endif

#ifdef RT
	LFD	f16,  6 * SIZE(BO)
	LFD	f17,  7 * SIZE(BO)
	LFD	f18,  4 * SIZE(BO)
	LFD	f19,  5 * SIZE(BO)
	LFD	f20,  0 * SIZE(BO)
	LFD	f21,  1 * SIZE(BO)

	FMUL	f12, f17, f3
	FMUL	f13, f17, f2

#ifndef CONJ
	FMSUB	f2,  f16, f2,  f12
	FMADD	f3,  f16, f3,  f13

	FMADD	f0,  f19, f3,  f0
	FNMSUB	f1,  f19, f2,  f1

	FNMSUB	f0,  f18, f2,  f0
	FNMSUB	f1,  f18, f3,  f1

	FMUL	f4,  f21, f1
	FMUL	f5,  f21, f0

	FMSUB	f0,  f20, f0,  f4
	FMADD	f1,  f20, f1,  f5
#else
	FMADD	f2,  f16, f2,  f12
	FMSUB	f3,  f16, f3,  f13

	FMSUB	f0,  f19, f3,  f0
	FNMADD	f1,  f19, f2,  f1

	FNMADD	f0,  f18, f2,  f0
	FNMADD	f1,  f18, f3,  f1

	FMUL	f4,  f21, f1
	FMUL	f5,  f21, f0

	FMADD	f0,  f20, f0,  f4
	FMSUB	f1,  f20, f1,  f5
#endif
#endif

#ifdef LN
	subi	CO1, CO1, 2 * SIZE
	subi	CO2, CO2, 2 * SIZE
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
	STFD	f2,   0 * SIZE(CO2)
	STFD	f3,   1 * SIZE(CO2)

#ifndef LN
	addi	CO1, CO1, 2 * SIZE
	addi	CO2, CO2, 2 * SIZE
#endif

#ifdef RT
	slwi	r0, K, 0 + ZBASE_SHIFT
	add	AORIG, AORIG, r0
#endif

#if defined(LT) || defined(RN)
	sub	TEMP, K, KK
	slwi	r0,   TEMP, 0 + ZBASE_SHIFT
	slwi	TEMP, TEMP, 1 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LT
	addi	KK, KK, 1
#endif

#ifdef LN
	subi	KK, KK, 1
#endif
	.align 4

LL(29):
#ifdef LN
	slwi	r0, K, 1 + ZBASE_SHIFT
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

	addic.	J, J, -1
	bgt	LL(10)
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
#endif

	addi	SP, SP, STACKSIZE

	blr

	EPILOGUE
#endif
