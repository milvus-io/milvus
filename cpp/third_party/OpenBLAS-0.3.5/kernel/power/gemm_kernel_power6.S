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

#define TEMP	r19
#define KK	r20
#define BB	r21
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

	stfd	f1,  ALPHA
	stw	r0,  FZERO

#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
	lwz	LDC, FRAMESLOT(0) + STACKSIZE(SP)
#endif
#endif

	slwi	LDC, LDC, BASE_SHIFT

#if defined(TRMMKERNEL)
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
#endif

#if defined(TRMMKERNEL) && !defined(LEFT)
	neg	KK, OFFSET
#endif

	cmpwi	cr0, M, 0
	ble	LL(999)
	cmpwi	cr0, N, 0
	ble	LL(999)
	cmpwi	cr0, K, 0
	ble	LL(999)

	lfs	f0,  FZERO
	li	PREA,  (16 * 3) * SIZE
	srawi.	J, N,  2

	li	PREC,   3 * SIZE
	ble	LL(40)
	.align 4

LL(10):
	mr	CO1, C
 	fmr	f1,  f0
	add	CO2, C,  LDC
	fmr	f2,  f0
	add	CO3, CO2, LDC
	fmr	f3,  f0
	add	CO4, CO3, LDC
	fmr	f4,  f0

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mr	KK, OFFSET
#endif

	slwi	BB, K, BASE_SHIFT + 2
	fmr	f5,  f0

	srawi.	I, M,  2
	fmr	f6,  f0

	mr	AO, A
	fmr	f7,  f0
	add	C,  CO4, LDC
	fmr	f8,  f0
	fmr	f9,  f0
	fmr	f10, f0
	fmr	f11, f0
	fmr	f12, f0
	fmr	f13, f0
	fmr	f14, f0
	fmr	f15, f0

	ble	LL(20)
	.align 4

LL(11):
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	LFD	f16,  0 * SIZE(AO)
	LFD	f20,  0 * SIZE(B)
	LFD	f17,  1 * SIZE(AO)
	LFD	f21,  1 * SIZE(B)

	LFD	f18,  2 * SIZE(AO)
	LFD	f22,  2 * SIZE(B)
	LFD	f19,  3 * SIZE(AO)
	LFD	f23,  3 * SIZE(B)

	mr	BO,  B
#else
	slwi	r0, KK, 2 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, B,  r0

	LFD	f16,  0 * SIZE(AO)
	LFD	f20,  0 * SIZE(BO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f21,  1 * SIZE(BO)

	LFD	f18,  2 * SIZE(AO)
	LFD	f22,  2 * SIZE(BO)
	LFD	f19,  3 * SIZE(AO)
	LFD	f23,  3 * SIZE(BO)
#endif

	dcbtst	CO1, PREC
	dcbtst	CO2, PREC
	dcbtst	CO3, PREC
	dcbtst	CO4, PREC

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 4
#endif
	srawi.	TEMP,  TEMP,  3
	mtspr	CTR, TEMP
	ble	LL(15)

#else

	LFD	f16,  0 * SIZE(AO)
	LFD	f20,  0 * SIZE(B)
	LFD	f17,  1 * SIZE(AO)
	LFD	f21,  1 * SIZE(B)

	LFD	f18,  2 * SIZE(AO)
	LFD	f22,  2 * SIZE(B)
	LFD	f19,  3 * SIZE(AO)
	LFD	f23,  3 * SIZE(B)

	dcbtst	CO1, PREC
	dcbtst	CO2, PREC
	dcbtst	CO3, PREC
	dcbtst	CO4, PREC

	srawi.	r0,  K,  3
	mtctr	r0
	mr	BO,  B
	ble	LL(15)
#endif
	.align 4

LL(12):
	dcbt	AO, PREA
	FMADD	f0,  f16, f20, f0
	nop
	FMADD	f4,  f16, f21, f4

	FMADD	f8,  f16, f22, f8
	FMADD	f12, f16, f23, f12
	LFD	f24,  4 * SIZE(AO)
	LFD	f25,  5 * SIZE(AO)

	FMADD	f1,  f17, f20, f1
	FMADD	f5,  f17, f21, f5
	LFD	f28,  4 * SIZE(BO)
	LFD	f29,  5 * SIZE(BO)

	FMADD	f9,  f17, f22, f9
	FMADD	f13, f17, f23, f13
	FMADD	f2,  f18, f20, f2
	FMADD	f6,  f18, f21, f6

	FMADD	f10, f18, f22, f10
	FMADD	f14, f18, f23, f14
	LFD	f26,  6 * SIZE(AO)
	LFD	f27,  7 * SIZE(AO)

	FMADD	f3,  f19, f20, f3
	FMADD	f7,  f19, f21, f7
	LFD	f30,  6 * SIZE(BO)
	LFD	f31,  7 * SIZE(BO)

	FMADD	f11, f19, f22, f11
	FMADD	f15, f19, f23, f15
	FMADD	f0,  f24, f28, f0
	FMADD	f4,  f24, f29, f4

	FMADD	f8,  f24, f30, f8
	FMADD	f12, f24, f31, f12
	LFD	f16,  8 * SIZE(AO)
	LFD	f17,  9 * SIZE(AO)

	FMADD	f1,  f25, f28, f1
	FMADD	f5,  f25, f29, f5
 	LFD	f20,  8 * SIZE(BO)
	LFD	f21,  9 * SIZE(BO)

	FMADD	f9,  f25, f30, f9
	FMADD	f13, f25, f31, f13
	FMADD	f2,  f26, f28, f2
	FMADD	f6,  f26, f29, f6

	FMADD	f10, f26, f30, f10
	FMADD	f14, f26, f31, f14
	LFD	f18, 10 * SIZE(AO)
	LFD	f19, 11 * SIZE(AO)

	FMADD	f3,  f27, f28, f3
	FMADD	f7,  f27, f29, f7
	LFD	f22, 10 * SIZE(BO)
	LFD	f23, 11 * SIZE(BO)

	FMADD	f11, f27, f30, f11
	FMADD	f15, f27, f31, f15
	FMADD	f0,  f16, f20, f0
	FMADD	f4,  f16, f21, f4

	FMADD	f8,  f16, f22, f8
	FMADD	f12, f16, f23, f12
	LFD	f24, 12 * SIZE(AO)
	LFD	f25, 13 * SIZE(AO)

	FMADD	f1,  f17, f20, f1
	FMADD	f5,  f17, f21, f5
	LFD	f28, 12 * SIZE(BO)
	LFD	f29, 13 * SIZE(BO)

	FMADD	f9,  f17, f22, f9
	FMADD	f13, f17, f23, f13
	FMADD	f2,  f18, f20, f2
	FMADD	f6,  f18, f21, f6

	FMADD	f10, f18, f22, f10
	FMADD	f14, f18, f23, f14
	LFD	f26, 14 * SIZE(AO)
	LFD	f27, 15 * SIZE(AO)

	FMADD	f3,  f19, f20, f3
	FMADD	f7,  f19, f21, f7
	LFD	f30, 14 * SIZE(BO)
	LFD	f31, 15 * SIZE(BO)

	FMADD	f11, f19, f22, f11
	FMADD	f15, f19, f23, f15
	FMADD	f0,  f24, f28, f0
	FMADD	f4,  f24, f29, f4

	FMADD	f8,  f24, f30, f8
	FMADD	f12, f24, f31, f12
	LFD	f16, 16 * SIZE(AO)
	LFD	f17, 17 * SIZE(AO)

	FMADD	f1,  f25, f28, f1
	FMADD	f5,  f25, f29, f5
	LFD	f20, 16 * SIZE(BO)
	LFD	f21, 17 * SIZE(BO)

	FMADD	f9,  f25, f30, f9
	FMADD	f13, f25, f31, f13
	FMADD	f2,  f26, f28, f2
	FMADD	f6,  f26, f29, f6

	FMADD	f10, f26, f30, f10
	FMADD	f14, f26, f31, f14
	LFD	f18, 18 * SIZE(AO)
	LFD	f19, 19 * SIZE(AO)

	FMADD	f3,  f27, f28, f3
	FMADD	f7,  f27, f29, f7
	LFD	f22, 18 * SIZE(BO)
	LFD	f23, 19 * SIZE(BO)

	FMADD	f11, f27, f30, f11
	FMADD	f15, f27, f31, f15
	FMADD	f0,  f16, f20, f0
	FMADD	f4,  f16, f21, f4

	FMADD	f8,  f16, f22, f8
	FMADD	f12, f16, f23, f12
	LFD	f24, 20 * SIZE(AO)
	LFD	f25, 21 * SIZE(AO)

	FMADD	f1,  f17, f20, f1
	FMADD	f5,  f17, f21, f5
	LFD	f28, 20 * SIZE(BO)
	LFD	f29, 21 * SIZE(BO)

	FMADD	f9,  f17, f22, f9
	FMADD	f13, f17, f23, f13
	FMADD	f2,  f18, f20, f2
	FMADD	f6,  f18, f21, f6

	FMADD	f10, f18, f22, f10
	FMADD	f14, f18, f23, f14
	LFD	f26, 22 * SIZE(AO)
	LFD	f27, 23 * SIZE(AO)

	FMADD	f3,  f19, f20, f3
	FMADD	f7,  f19, f21, f7
	LFD	f30, 22 * SIZE(BO)
	LFD	f31, 23 * SIZE(BO)

	FMADD	f11, f19, f22, f11
	FMADD	f15, f19, f23, f15
	FMADD	f0,  f24, f28, f0
	FMADD	f4,  f24, f29, f4

	FMADD	f8,  f24, f30, f8
	FMADD	f12, f24, f31, f12
	LFD	f16, 24 * SIZE(AO)
	LFD	f17, 25 * SIZE(AO)

	FMADD	f1,  f25, f28, f1
	FMADD	f5,  f25, f29, f5
 	LFD	f20, 24 * SIZE(BO)
	LFD	f21, 25 * SIZE(BO)

	FMADD	f9,  f25, f30, f9
	FMADD	f13, f25, f31, f13
	FMADD	f2,  f26, f28, f2
	FMADD	f6,  f26, f29, f6

	FMADD	f10, f26, f30, f10
	FMADD	f14, f26, f31, f14
	LFD	f18, 26 * SIZE(AO)
	LFD	f19, 27 * SIZE(AO)

	FMADD	f3,  f27, f28, f3
	FMADD	f7,  f27, f29, f7
	LFD	f22, 26 * SIZE(BO)
	LFD	f23, 27 * SIZE(BO)

	FMADD	f11, f27, f30, f11
	FMADD	f15, f27, f31, f15
	FMADD	f0,  f16, f20, f0
	FMADD	f4,  f16, f21, f4

	FMADD	f8,  f16, f22, f8
	FMADD	f12, f16, f23, f12
	LFD	f24, 28 * SIZE(AO)
	LFD	f25, 29 * SIZE(AO)

	FMADD	f1,  f17, f20, f1
	FMADD	f5,  f17, f21, f5
	LFD	f28, 28 * SIZE(BO)
	LFD	f29, 29 * SIZE(BO)

	FMADD	f9,  f17, f22, f9
	FMADD	f13, f17, f23, f13
	FMADD	f2,  f18, f20, f2
	FMADD	f6,  f18, f21, f6

	FMADD	f10, f18, f22, f10
	FMADD	f14, f18, f23, f14
	LFD	f26, 30 * SIZE(AO)
	LFD	f27, 31 * SIZE(AO)

	FMADD	f3,  f19, f20, f3
	FMADD	f7,  f19, f21, f7
	LFD	f30, 30 * SIZE(BO)
	LFD	f31, 31 * SIZE(BO)

	FMADD	f11, f19, f22, f11
	FMADD	f15, f19, f23, f15
	FMADD	f0,  f24, f28, f0
	FMADD	f4,  f24, f29, f4

	FMADD	f8,  f24, f30, f8
	FMADD	f12, f24, f31, f12
	LFD	f16, 32 * SIZE(AO)
	LFD	f17, 33 * SIZE(AO)

	FMADD	f1,  f25, f28, f1
	FMADD	f5,  f25, f29, f5
	LFD	f20, 32 * SIZE(BO)
	LFD	f21, 33 * SIZE(BO)

	FMADD	f9,  f25, f30, f9
	FMADD	f13, f25, f31, f13
	LFD	f18, 34 * SIZE(AO)
	LFD	f19, 35 * SIZE(AO)

	FMADD	f2,  f26, f28, f2
	FMADD	f6,  f26, f29, f6
	LFD	f22, 34 * SIZE(BO)
	LFD	f23, 35 * SIZE(BO)

	FMADD	f10, f26, f30, f10
	FMADD	f14, f26, f31, f14
	FMADD	f3,  f27, f28, f3
	FMADD	f7,  f27, f29, f7

	FMADD	f11, f27, f30, f11
	FMADD	f15, f27, f31, f15
	addi	AO, AO, 32 * SIZE
	addi	BO, BO, 32 * SIZE
	bdnz	LL(12)
	.align 4

LL(15):
	lfd	f30,  ALPHA

	dcbtst	B, BB
	addi	BB, BB, 16 * SIZE
	dcbtst	B, BB
	addi	BB, BB, 16 * SIZE

#if defined(TRMMKERNEL)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 4
#endif

	andi.	TEMP,  TEMP,  7
	mtspr	CTR, TEMP
#else

	andi.	r0,  K,  7
	mtspr	CTR, r0

#endif
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
	nop
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

	FMADD	f0,  f0, f30, f16
	FMADD	f1,  f1, f30, f17
	FMADD	f2,  f2, f30, f18
	FMADD	f3,  f3, f30, f19

	FMADD	f4,  f4, f30, f20
	FMADD	f5,  f5, f30, f21
	FMADD	f6,  f6, f30, f22
	FMADD	f7,  f7, f30, f23

	LFD	f16, 0 * SIZE(CO3)
	LFD	f17, 1 * SIZE(CO3)
	LFD	f18, 2 * SIZE(CO3)
	LFD	f19, 3 * SIZE(CO3)

	LFD	f20, 0 * SIZE(CO4)
	LFD	f21, 1 * SIZE(CO4)
	LFD	f22, 2 * SIZE(CO4)
	LFD	f23, 3 * SIZE(CO4)

	FMADD	f8,  f8,  f30, f16
	FMADD	f9,  f9,  f30, f17
	FMADD	f10, f10, f30, f18
	FMADD	f11, f11, f30, f19

	FMADD	f12, f12, f30, f20
	FMADD	f13, f13, f30, f21
	FMADD	f14, f14, f30, f22
	FMADD	f15, f15, f30, f23

#else
	FMUL	f0,  f0, f30
	FMUL	f1,  f1, f30
	FMUL	f2,  f2, f30
	FMUL	f3,  f3, f30

	FMUL	f4,  f4, f30
	FMUL	f5,  f5, f30
	FMUL	f6,  f6, f30
	FMUL	f7,  f7, f30

	FMUL	f8,  f8,  f30
	FMUL	f9,  f9,  f30
	FMUL	f10, f10, f30
	FMUL	f11, f11, f30

	FMUL	f12, f12, f30
	FMUL	f13, f13, f30
	FMUL	f14, f14, f30
	FMUL	f15, f15, f30
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
	addi	TEMP, TEMP, -4
#else
	addi	TEMP, TEMP, -4
#endif
	slwi	TEMP, TEMP, 2 + BASE_SHIFT
	add	AO, AO, TEMP
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 4
#endif
#endif

	addic.	I, I, -1
	bgt+	LL(11)
	.align 4

LL(20):
	andi.	I,  M,  2
	ble	LL(30)

#if defined(TRMMKERNEL)
#if ( defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))

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
	slwi	r0,   KK, 1 + BASE_SHIFT
	slwi	TEMP, KK, 2 + BASE_SHIFT
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
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 4
#endif
	srawi.	TEMP,  TEMP,  2
	mtspr	CTR, TEMP

#else
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
	mtspr	CTR, r0
	mr	BO,  B
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
	lfd	f30,  ALPHA

#if   defined(TRMMKERNEL)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 4
#endif
	andi.	TEMP,  TEMP,  3
	mtspr	CTR, TEMP

#else

	andi.	r0,  K,  3
	mtspr	CTR, r0

#endif
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
#ifndef TRMMKERNEL
	LFD	f16, 0 * SIZE(CO1)
	LFD	f17, 1 * SIZE(CO1)
	LFD	f18, 0 * SIZE(CO2)
	LFD	f19, 1 * SIZE(CO2)

	FMADD	f0,  f0, f30, f16
	FMADD	f1,  f1, f30, f17
	FMADD	f4,  f4, f30, f18
	FMADD	f5,  f5, f30, f19

	LFD	f20, 0 * SIZE(CO3)
	LFD	f21, 1 * SIZE(CO3)
	LFD	f22, 0 * SIZE(CO4)
	LFD	f23, 1 * SIZE(CO4)

	FMADD	f8,  f8,  f30, f20
	FMADD	f9,  f9,  f30, f21
	FMADD	f12, f12, f30, f22
	FMADD	f13, f13, f30, f23
#else
	FMUL	f0,  f0, f30
	FMUL	f1,  f1, f30
	FMUL	f4,  f4, f30
	FMUL	f5,  f5, f30

	FMUL	f8,  f8,  f30
	FMUL	f9,  f9,  f30
	FMUL	f12, f12, f30
	FMUL	f13, f13, f30
#endif

	STFD	f0,  0 * SIZE(CO1)
	STFD	f1,  1 * SIZE(CO1)
	STFD	f4,  0 * SIZE(CO2)
	STFD	f5,  1 * SIZE(CO2)

	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

	STFD	f8,  0 * SIZE(CO3)
	STFD	f9,  1 * SIZE(CO3)
	STFD	f12, 0 * SIZE(CO4)
	STFD	f13, 1 * SIZE(CO4)

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

	addi	CO1, CO1, 2 * SIZE
	addi	CO2, CO2, 2 * SIZE
	addi	CO3, CO3, 2 * SIZE
	addi	CO4, CO4, 2 * SIZE

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -2
#else
	addi	TEMP, TEMP, -4
#endif
	slwi	r0,   TEMP, 1 + BASE_SHIFT
	slwi	TEMP, TEMP, 2 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 2
#endif
#endif
	.align 4

LL(30):
	andi.	I,  M,  1
	ble	LL(39)

#if   defined(TRMMKERNEL)

#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))

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
	slwi	r0,   KK, 0 + BASE_SHIFT
	slwi	TEMP, KK, 2 + BASE_SHIFT
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

#else
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
	mtspr	CTR, r0
	mr	BO,  B
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
	bdnz	LL(32)

	fadd	f0,  f1,   f0
	fadd	f4,  f5,   f4
	fadd	f8,  f9,   f8
	fadd	f12, f13, f12
	.align 4

LL(35):
	lfd	f30,  ALPHA
#if  defined(TRMMKERNEL)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 4
#endif
	andi.	TEMP,  TEMP,  3
	mtspr	CTR, TEMP

#else
	andi.	r0,  K,  3
	mtspr	CTR, r0

#endif
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
#ifndef TRMMKERNEL
	LFD	f16, 0 * SIZE(CO1)
	LFD	f18, 0 * SIZE(CO2)
	LFD	f20, 0 * SIZE(CO3)
	LFD	f22, 0 * SIZE(CO4)

	FMADD	f0,  f0,  f30, f16
	FMADD	f4,  f4,  f30, f18
	FMADD	f8,  f8,  f30, f20
	FMADD	f12, f12, f30, f22
#else
	FMUL	f0,  f0,  f30
	FMUL	f4,  f4,  f30
	FMUL	f8,  f8,  f30
	FMUL	f12, f12, f30
#endif

	STFD	f0,  0 * SIZE(CO1)
	STFD	f4,  0 * SIZE(CO2)
	STFD	f8,  0 * SIZE(CO3)
	STFD	f12, 0 * SIZE(CO4)

	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f4,  f0
	fmr	f5,  f0

	fmr	f8,  f0
	fmr	f9,  f0
	fmr	f12, f0
	fmr	f13, f0

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -1
#else
	addi	TEMP, TEMP, -4
#endif
	slwi	r0,   TEMP, 0 + BASE_SHIFT
	slwi	TEMP, TEMP, 2 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 2
#endif
#endif
	.align 4


LL(39):
#if defined(TRMMKERNEL) && !defined(LEFT)
	addi	KK, KK, 4
#endif

	lfs	f0,  FZERO

	mr	B,  BO
	addic.	J, J, -1
	bgt	LL(10)
	.align 4

LL(40):
	mr	CO1, C
	add	CO2, C,  LDC
	andi.	J, N,  2
	ble	LL(70)

#if defined(TRMMKERNEL) && defined(LEFT)
	mr	KK, OFFSET
#endif


	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0
	fmr	f4,  f0
	fmr	f5,  f0
	fmr	f6,  f0
	fmr	f7,  f0

	srawi.	I, M,  2
	add	C,  CO2, LDC
	mr	AO, A
	ble	LL(50)
	.align 4

LL(41):
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))

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
	slwi	r0,   KK, 2 + BASE_SHIFT
	slwi	TEMP, KK, 1 + BASE_SHIFT
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
	dcbtst	CO2, PREC

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 2
#endif
	srawi.	TEMP,  TEMP,  2
	mtspr	CTR, TEMP

#else

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

	srawi.	r0,  K,  2
	mtspr	CTR, r0
	mr	BO,  B
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
	bdnz	LL(42)
	.align 4

LL(45):
	lfd	f30,  ALPHA
#if defined(TRMMKERNEL)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 2
#endif
	andi.	TEMP,  TEMP,  3
	mtspr	CTR, TEMP
#else
	andi.	r0,  K,  3
	mtspr	CTR, r0
#endif
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
#ifndef TRMMKERNEL
	LFD	f16, 0 * SIZE(CO1)
	LFD	f17, 1 * SIZE(CO1)
	LFD	f18, 2 * SIZE(CO1)
	LFD	f19, 3 * SIZE(CO1)

	LFD	f20, 0 * SIZE(CO2)
	LFD	f21, 1 * SIZE(CO2)
	LFD	f22, 2 * SIZE(CO2)
	LFD	f23, 3 * SIZE(CO2)

	FMADD	f0,  f0, f30, f16
	FMADD	f1,  f1, f30, f17
	FMADD	f2,  f2, f30, f18
	FMADD	f3,  f3, f30, f19

	FMADD	f4,  f4, f30, f20
	FMADD	f5,  f5, f30, f21
	FMADD	f6,  f6, f30, f22
	FMADD	f7,  f7, f30, f23
#else
	FMUL	f0,  f0, f30
	FMUL	f1,  f1, f30
	FMUL	f2,  f2, f30
	FMUL	f3,  f3, f30

	FMUL	f4,  f4, f30
	FMUL	f5,  f5, f30
	FMUL	f6,  f6, f30
	FMUL	f7,  f7, f30
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

	addi	CO1, CO1, 4 * SIZE
	addi	CO2, CO2, 4 * SIZE

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -4
#else
	addi	TEMP, TEMP, -2
#endif
	slwi	r0,   TEMP, 2 + BASE_SHIFT
	slwi	TEMP, TEMP, 1 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 4
#endif
#endif

	addic.	I, I, -1
	bgt+	LL(41)
	.align 4

LL(50):
	andi.	I,  M,  2
	ble	LL(60)

#if defined(TRMMKERNEL)

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))

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
	slwi	r0,   KK, 1 + BASE_SHIFT
	slwi	TEMP, KK, 1 + BASE_SHIFT
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
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 2
#endif
	srawi.	TEMP,  TEMP,  2
	mtspr	CTR, TEMP

#else
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
	mtspr	CTR, r0
	mr	BO,  B
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
	bdnz	LL(52)
	.align 4

LL(55):
	lfd	f30,  ALPHA
#if defined(TRMMKERNEL)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 2
#endif
	andi.	TEMP,  TEMP,  3
	mtspr	CTR, TEMP

#else
	andi.	r0,  K,  3
	mtspr	CTR, r0
#endif
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
#ifndef TRMMKERNEL
	LFD	f16, 0 * SIZE(CO1)
	LFD	f17, 1 * SIZE(CO1)
	LFD	f18, 0 * SIZE(CO2)
	LFD	f19, 1 * SIZE(CO2)

	FADD	f0, f4,  f0
	FADD	f1, f5,  f1
	FADD	f2, f6,  f2
	FADD	f3, f7,  f3

	FMADD	f0,  f0, f30, f16
	FMADD	f1,  f1, f30, f17
	FMADD	f2,  f2, f30, f18
	FMADD	f3,  f3, f30, f19
#else
	FADD	f0, f4,  f0
	FADD	f1, f5,  f1
	FADD	f2, f6,  f2
	FADD	f3, f7,  f3

	FMUL	f0,  f0, f30
	FMUL	f1,  f1, f30
	FMUL	f2,  f2, f30
	FMUL	f3,  f3, f30
#endif

	STFD	f0,  0 * SIZE(CO1)
	STFD	f1,  1 * SIZE(CO1)
	STFD	f2,  0 * SIZE(CO2)
	STFD	f3,  1 * SIZE(CO2)

	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

	fmr	f4,  f0
	fmr	f5,  f0
	fmr	f6,  f0
	fmr	f7,  f0

	addi	CO1, CO1, 2 * SIZE
	addi	CO2, CO2, 2 * SIZE

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -2
#else
	addi	TEMP, TEMP, -2
#endif
	slwi	r0,   TEMP, 1 + BASE_SHIFT
	slwi	TEMP, TEMP, 1 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 2
#endif
#endif
	.align 4

LL(60):
	andi.	I,  M,  1
	ble	LL(69)

#if defined(TRMMKERNEL)

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))

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
	slwi	r0,   KK, 0 + BASE_SHIFT
	slwi	TEMP, KK, 1 + BASE_SHIFT
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
	addi	TEMP, KK, 2
#endif
	srawi.	TEMP,  TEMP,  2
	mtspr	CTR, TEMP
#else
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
	mtspr	CTR, r0
	mr	BO,  B
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
	lfd	f30,  ALPHA

#if defined(TRMMKERNEL)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 2
#endif
	andi.	TEMP,  TEMP,  3
	mtspr	CTR, TEMP

#else
	andi.	r0,  K,  3
	mtspr	CTR, r0

#endif
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
#ifndef TRMMKERNEL
	LFD	f16, 0 * SIZE(CO1)
	LFD	f18, 0 * SIZE(CO2)

	FADD	f0, f2, f0
	FADD	f1, f3, f1

	FMADD	f0,  f0,  f30, f16
	FMADD	f1,  f1,  f30, f18
#else
	FADD	f0, f2, f0
	FADD	f1, f3, f1

	FMUL	f0,  f0,  f30
	FMUL	f1,  f1,  f30
#endif

	STFD	f0,  0 * SIZE(CO1)
	STFD	f1,  0 * SIZE(CO2)

	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f4,  f0
	fmr	f5,  f0


#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -1
#else
	addi	TEMP, TEMP, -2
#endif
	slwi	r0,   TEMP, 0 + BASE_SHIFT
	slwi	TEMP, TEMP, 1 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 1
#endif
#endif
	.align 4

LL(69):
#if defined(TRMMKERNEL) && !defined(LEFT)
	addi	KK, KK, 2
#endif

	mr	B,  BO
	.align 4

LL(70):
	mr	CO1, C
	andi.	J, N,  1
	ble	LL(999)

#if defined(TRMMKERNEL) && defined(LEFT)
	mr	KK, OFFSET
#endif

	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

	srawi.	I, M,  2
	mr	AO, A
	ble	LL(80)
	.align 4

LL(71):
#if defined(TRMMKERNEL)

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))

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
	slwi	r0,   KK, 2 + BASE_SHIFT
	slwi	TEMP, KK, 0 + BASE_SHIFT
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
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 1
#endif
	srawi.	TEMP,  TEMP,  2
	mtspr	CTR, TEMP
#else
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(B)
	LFD	f21,  1 * SIZE(B)
	LFD	f22,  2 * SIZE(B)
	LFD	f23,  3 * SIZE(B)

	dcbtst	CO1, PREC

	srawi.	r0,  K,  2
	mtspr	CTR, r0
	mr	BO,  B
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
	bdnz	LL(72)
	.align 4

LL(75):
	lfd	f30,  ALPHA
#if defined(TRMMKERNEL)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 1
#endif
	andi.	TEMP,  TEMP,  3
	mtspr	CTR, TEMP

#else
	andi.	r0,  K,  3
	mtspr	CTR, r0

#endif
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
#ifndef TRMMKERNEL
	LFD	f16, 0 * SIZE(CO1)
	LFD	f17, 1 * SIZE(CO1)
	LFD	f18, 2 * SIZE(CO1)
	LFD	f19, 3 * SIZE(CO1)

	FMADD	f0,  f0, f30, f16
	FMADD	f1,  f1, f30, f17
	FMADD	f2,  f2, f30, f18
	FMADD	f3,  f3, f30, f19
#else
	FMUL	f0,  f0, f30
	FMUL	f1,  f1, f30
	FMUL	f2,  f2, f30
	FMUL	f3,  f3, f30
#endif

	STFD	f0,  0 * SIZE(CO1)
	STFD	f1,  1 * SIZE(CO1)
	STFD	f2,  2 * SIZE(CO1)
	STFD	f3,  3 * SIZE(CO1)

	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -4
#else
	addi	TEMP, TEMP, -1
#endif
	slwi	r0  , TEMP, 2 + BASE_SHIFT
	slwi	TEMP, TEMP, 0 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 4
#endif
#endif

	addi	CO1, CO1, 4 * SIZE
	addic.	I, I, -1
	bgt+	LL(71)
	.align 4

LL(80):
	andi.	I,  M,  2
	ble	LL(90)

#if defined(TRMMKERNEL)

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))

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
	slwi	r0,   KK, 1 + BASE_SHIFT
	slwi	TEMP, KK, 0 + BASE_SHIFT
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
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 1
#endif
	srawi.	TEMP,  TEMP,  2
	mtspr	CTR, TEMP

#else
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(B)
	LFD	f21,  1 * SIZE(B)
	LFD	f22,  2 * SIZE(B)
	LFD	f23,  3 * SIZE(B)

	srawi.	r0,  K,  2
	mtspr	CTR, r0
	mr	BO,  B

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
	bdnz	LL(82)
	.align 4

LL(85):
	lfd	f30,  ALPHA
#if defined(TRMMKERNEL)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 1
#endif
	andi.	TEMP,  TEMP,  3
	mtspr	CTR, TEMP

#else

	andi.	r0,  K,  3
	mtspr	CTR, r0

#endif
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
#ifndef TRMMKERNEL
	LFD	f16, 0 * SIZE(CO1)
	LFD	f17, 1 * SIZE(CO1)

	FADD	f0, f2, f0
	FADD	f1, f3, f1

	FMADD	f0,  f0, f30, f16
	FMADD	f1,  f1, f30, f17
#else
	FADD	f0, f2, f0
	FADD	f1, f3, f1

	FMUL	f0,  f0, f30
	FMUL	f1,  f1, f30
#endif

	STFD	f0,  0 * SIZE(CO1)
	STFD	f1,  1 * SIZE(CO1)

	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

	addi	CO1, CO1, 2 * SIZE

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -2
#else
	addi	TEMP, TEMP, -1
#endif
	slwi	r0  , TEMP, 1 + BASE_SHIFT
	slwi	TEMP, TEMP, 0 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 2
#endif
#endif
	.align 4

LL(90):
	andi.	I,  M,  1
	ble	LL(999)


#if defined(TRMMKERNEL)

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))

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
	slwi	r0,   KK, 0 + BASE_SHIFT
	slwi	TEMP, KK, 0 + BASE_SHIFT
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
	srawi.	TEMP,  TEMP,  3
	mtspr	CTR, TEMP

#else
	LFD	f16,  0 * SIZE(AO)
	LFD	f17,  1 * SIZE(AO)
	LFD	f18,  2 * SIZE(AO)
	LFD	f19,  3 * SIZE(AO)

	LFD	f20,  0 * SIZE(B)
	LFD	f21,  1 * SIZE(B)
	LFD	f22,  2 * SIZE(B)
	LFD	f23,  3 * SIZE(B)

	srawi.	r0,  K,  3
	mtspr	CTR, r0
	mr	BO,  B
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
	lfd	f30,  ALPHA

#if defined(TRMMKERNEL)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 1
#endif
	andi.	TEMP,  TEMP,  7
	mtspr	CTR, TEMP

#else

	andi.	r0,  K,  7
	mtspr	CTR, r0

#endif
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
#ifndef TRMMKERNEL
	LFD	f16, 0 * SIZE(CO1)

	FADD	f0, f1, f0
	FADD	f2, f3, f2
	FADD	f0, f2, f0

	FMADD	f0,  f0,  f30, f16
#else
	FADD	f0, f1, f0
	FADD	f2, f3, f2
	FADD	f0, f2, f0

	FMUL	f0,  f0,  f30
#endif

	STFD	f0,  0 * SIZE(CO1)
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
