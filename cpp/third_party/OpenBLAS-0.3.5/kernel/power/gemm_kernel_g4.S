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
#define KK	r20
#define	I	r21
#define J	r22
#define AO	r23
#define	BO	r24
#define	CO1	r25
#define CO2	r26
#define	CO3	r27
#define	CO4	r28

#define PREA	r29
#define PREC	r30


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
#if defined(TRMMKERNEL)
	std	r19,  240(SP)
	std	r18,  248(SP)
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
	stw	r20,  188(SP)
#if defined(TRMMKERNEL)
	stw	r19,  192(SP)
	stw	r18,  196(SP)
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
	ld	OFFSET, FRAMESLOT(0) + STACKSIZE(SP)
#endif

#if defined(_AIX) || defined(__APPLE__)
#ifdef __64BIT__
	ld	OFFSET, FRAMESLOT(0) + STACKSIZE(SP)
#else
#ifdef DOUBLE
	lwz	OFFSET, FRAMESLOT(1) + STACKSIZE(SP)
#else
	lwz	OFFSET, FRAMESLOT(0) + STACKSIZE(SP)
#endif
#endif
#endif
#endif

#if defined(TRMMKERNEL) && !defined(LEFT)
	neg	KK, OFFSET
#endif

	li	PREA, 8 * 8 * SIZE
	li	PREC, 3 * SIZE

	cmpwi	cr0, M, 0
	ble	.L999
	cmpwi	cr0, N, 0
	ble	.L999
	cmpwi	cr0, K, 0
	ble	.L999

	srawi.	J, N,  2
	ble	.L40
	.align 4

#define A1	f16
#define A2	f17
#define A3	f18
#define A4	f19
#define A5	f20
#define A6	f21
#define B1	f22
#define B2	f23
#define B3	f24
#define B4	f25
#define B5	f26
#define B6	f27
#define B7	f28
#define B8	f29
#define B9	f30
#define B10	f31


.L10:
	mr	CO1, C
	add	CO2, C,  LDC
	add	CO3, CO2, LDC
	add	CO4, CO3, LDC

#if defined(TRMMKERNEL) &&  defined(LEFT)
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
	fmr	f8,  f0
	fmr	f9,  f0
	fmr	f10, f0
	fmr	f11, f0
	fmr	f12, f0
	fmr	f13, f0
	fmr	f14, f0
	fmr	f15, f0

	srawi.	I, M,  2
	mr	AO, A
	add	C,  CO4, LDC
	ble	.L20
	.align 4

.L11:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))

	LFD	A1,  0 * SIZE(AO)
	LFD	A2,  1 * SIZE(AO)
	LFD	A3,  2 * SIZE(AO)
	LFDU	A5,  4 * SIZE(AO)

	LFD	B1,  0 * SIZE(B)
	LFD	B2,  1 * SIZE(B)
	LFD	B3,  2 * SIZE(B)
	LFD	B4,  3 * SIZE(B)
	mr	BO,  B
#else
	slwi	r0, KK, 2 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, B,  r0

	LFD	A1,  0 * SIZE(AO)
	LFD	A2,  1 * SIZE(AO)
	LFD	A3,  2 * SIZE(AO)
	LFDU	A5,  4 * SIZE(AO)

	LFD	B1,  0 * SIZE(BO)
	LFD	B2,  1 * SIZE(BO)
	LFD	B3,  2 * SIZE(BO)
	LFD	B4,  3 * SIZE(BO)
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
	srawi.	TEMP,  TEMP,  1
	mtspr	CTR, TEMP
	ble	.L15

#else
	LFD	A1,  0 * SIZE(AO)
	LFD	A2,  1 * SIZE(AO)
	LFD	A3,  2 * SIZE(AO)
	LFDU	A5,  4 * SIZE(AO)

	LFD	B1,  0 * SIZE(B)
	LFD	B2,  1 * SIZE(B)
	LFD	B3,  2 * SIZE(B)
	LFD	B4,  3 * SIZE(B)

	dcbtst	CO1, PREC
	dcbtst	CO2, PREC
	dcbtst	CO3, PREC
	dcbtst	CO4, PREC

	srawi.	r0,  K,  1
	mtspr	CTR, r0
	mr	BO,  B
	ble	.L15
#endif
	.align 4

.L12:
	FMADD	f0,  A1, B1, f0
	LFDU	B5,  4 * SIZE(BO)
	FMADD	f4,  A1, B2, f4
	dcbt	AO, PREA

	FMADD	f8,  A1, B3, f8
	LFD	A4, -1 * SIZE(AO)
	FMADD	f12, A1, B4, f12
	dcbt	BO, PREA

	FMADD	f1,  A2, B1, f1
	LFD	B6,  1 * SIZE(BO)
	FMADD	f5,  A2, B2, f5
	nop

	FMADD	f9,  A2, B3, f9
	LFDU	A1,  4 * SIZE(AO)
	FMADD	f13, A2, B4, f13
	nop

	FMADD	f2,  A3, B1, f2
	LFD	B7,  2 * SIZE(BO)
	FMADD	f6,  A3, B2, f6
	nop

	FMADD	f10, A3, B3, f10
	LFD	A2, -3 * SIZE(AO)
	FMADD	f14, A3, B4, f14
	nop

	FMADD	f3,  A4, B1, f3
	LFD	B8,  3 * SIZE(BO)
	FMADD	f7,  A4, B2, f7
	nop

	FMADD	f11, A4, B3, f11
	LFD	A3, -2 * SIZE(AO)
	FMADD	f15, A4, B4, f15
	nop

	FMADD	f0,  A5, B5, f0
	LFDU	B1,  4 * SIZE(BO)
	FMADD	f4,  A5, B6, f4
	nop

	FMADD	f8,  A5, B7, f8
	LFD	A4, -1 * SIZE(AO)
	FMADD	f12, A5, B8, f12
#ifdef DOUBLE
	dcbt	BO, PREA
#else
	nop
#endif

	FMADD	f1,  A2, B5, f1
	LFD	B2,  1 * SIZE(BO)
	FMADD	f5,  A2, B6, f5
	nop

	FMADD	f9,  A2, B7, f9
	LFDU	A5,  4 * SIZE(AO)
	FMADD	f13, A2, B8, f13
#ifdef DOUBLE
	dcbt	AO, PREA
#else
	nop
#endif

	FMADD	f2,  A3, B5, f2
	LFD	B3,  2 * SIZE(BO)
	FMADD	f6,  A3, B6, f6
	nop

	FMADD	f10, A3, B7, f10
	LFD	A2, -3 * SIZE(AO)
	FMADD	f14, A3, B8, f14
	nop

	FMADD	f3,  A4, B5, f3
	LFD	B4,  3 * SIZE(BO)
	FMADD	f7,  A4, B6, f7
	nop

	FMADD	f11, A4, B7, f11
	LFD	A3, -2 * SIZE(AO)
	FMADD	f15, A4, B8, f15
	bdnz	.L12
	.align 4

.L15:
	addi	AO, AO, -4 * SIZE
	lfd	f30,  ALPHA

#if defined(TRMMKERNEL)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 4
#endif

	andi.	TEMP,  TEMP,  1
#else

	andi.	r0,  K,  1
#endif
	ble+	.L18

.L16:
	LFD	A4,  3 * SIZE(AO)

	FMADD	f0,  A1, B1, f0
	FMADD	f4,  A1, B2, f4
	FMADD	f8,  A1, B3, f8
	FMADD	f12, A1, B4, f12

	FMADD	f1,  A2, B1, f1
	FMADD	f5,  A2, B2, f5
	FMADD	f9,  A2, B3, f9
	FMADD	f13, A2, B4, f13

	FMADD	f2,  A3, B1, f2
	FMADD	f6,  A3, B2, f6
	FMADD	f10, A3, B3, f10
	FMADD	f14, A3, B4, f14

	FMADD	f3,  A4, B1, f3
	FMADD	f7,  A4, B2, f7
	FMADD	f11, A4, B3, f11
	FMADD	f15, A4, B4, f15
	addi	AO, AO, 4 * SIZE
	addi	BO, BO, 4 * SIZE

	.align 4

.L18:
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
	LFD	f16, 0 * SIZE(CO3)
	FMADD	f1,  f1, f30, f17
	LFD	f17, 1 * SIZE(CO3)
	FMADD	f2,  f2, f30, f18
	LFD	f18, 2 * SIZE(CO3)
	FMADD	f3,  f3, f30, f19
	LFD	f19, 3 * SIZE(CO3)

	FMADD	f4,  f4, f30, f20
	LFD	f20, 0 * SIZE(CO4)
	FMADD	f5,  f5, f30, f21
	LFD	f21, 1 * SIZE(CO4)
	FMADD	f6,  f6, f30, f22
	LFD	f22, 2 * SIZE(CO4)
	FMADD	f7,  f7, f30, f23
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
	fmr	f4,  f0
	STFD	f5,  1 * SIZE(CO2)
	fmr	f5,  f0
	STFD	f6,  2 * SIZE(CO2)
	fmr	f6,  f0
	STFD	f7,  3 * SIZE(CO2)
	fmr	f7,  f0

	STFD	f8,  0 * SIZE(CO3)
	fmr	f8,  f0
	STFD	f9,  1 * SIZE(CO3)
	fmr	f9,  f0
	STFD	f10, 2 * SIZE(CO3)
	fmr	f10, f0
	STFD	f11, 3 * SIZE(CO3)
	fmr	f11, f0

	STFD	f12, 0 * SIZE(CO4)
	fmr	f12, f0
	STFD	f13, 1 * SIZE(CO4)
	fmr	f13, f0
	STFD	f14, 2 * SIZE(CO4)
	fmr	f14, f0
	STFD	f15, 3 * SIZE(CO4)
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
	bgt+	.L11
	.align 4

.L20:
	andi.	I,  M,  2
	ble	.L30

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
	ble	.L25
	.align 5

.L22:
	FMADD	f0,  f16, f20, f0
	nop
	FMADD	f1,  f17, f20, f1
	LFD	f20,  8 * SIZE(BO)
	FMADD	f4,  f16, f21, f4
	nop
	FMADD	f5,  f17, f21, f5
	LFD	f21,  9 * SIZE(BO)

	FMADD	f8,  f16, f22, f8
	nop
	FMADD	f9,  f17, f22, f9
	LFD	f22, 10 * SIZE(BO)
	FMADD	f12, f16, f23, f12
	LFD	f16,  4 * SIZE(AO)
	FMADD	f13, f17, f23, f13
	LFD	f23, 11 * SIZE(BO)

	FMADD	f2,  f18, f24, f2
	LFD	f17,  5 * SIZE(AO)
	FMADD	f3,  f19, f24, f3
	LFD	f24, 12 * SIZE(BO)
	FMADD	f6,  f18, f25, f6
	nop
	FMADD	f7,  f19, f25, f7
	LFD	f25, 13 * SIZE(BO)

	FMADD	f10, f18, f26, f10
	nop
	FMADD	f11, f19, f26, f11
	LFD	f26, 14 * SIZE(BO)
	FMADD	f14, f18, f27, f14
	LFD	f18,  6 * SIZE(AO)
	FMADD	f15, f19, f27, f15
	LFD	f27, 15 * SIZE(BO)

	FMADD	f0,  f16, f20, f0
	LFD	f19,  7 * SIZE(AO)
	FMADD	f1,  f17, f20, f1
	LFDU	f20, 16 * SIZE(BO)
	FMADD	f4,  f16, f21, f4
	nop
	FMADD	f5,  f17, f21, f5
	LFD	f21,  1 * SIZE(BO)

	FMADD	f8,  f16, f22, f8
	nop
	FMADD	f9,  f17, f22, f9
	LFD	f22,  2 * SIZE(BO)
	FMADD	f12, f16, f23, f12
	LFDU	f16,  8 * SIZE(AO)
	FMADD	f13, f17, f23, f13
	LFD	f23,  3 * SIZE(BO)

	FMADD	f2,  f18, f24, f2
	LFD	f17,  1 * SIZE(AO)
	FMADD	f3,  f19, f24, f3
	LFD	f24,  4 * SIZE(BO)
	FMADD	f6,  f18, f25, f6
	nop
	FMADD	f7,  f19, f25, f7
	LFD	f25,  5 * SIZE(BO)

	FMADD	f10, f18, f26, f10
	nop
	FMADD	f11, f19, f26, f11
	LFD	f26,  6 * SIZE(BO)
	FMADD	f14, f18, f27, f14
	LFD	f18,  2 * SIZE(AO)
	FMADD	f15, f19, f27, f15
	LFD	f19,  3 * SIZE(AO)
	LFD	f27,  7 * SIZE(BO)
	bdnz	.L22

	fadd	f0,  f2,  f0
	fadd	f1,  f3,  f1
	fadd	f4,  f6,  f4
	fadd	f5,  f7,  f5
	fadd	f8,  f10, f8
	fadd	f9,  f11, f9
	fadd	f12, f14, f12
	fadd	f13, f15, f13
	.align 4

.L25:
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
	ble+	.L28
	.align 4

.L26:
	FMADD	f0,  f16, f20, f0
	nop
	FMADD	f1,  f17, f20, f1
	LFDU	f20,  4 * SIZE(BO)
	FMADD	f4,  f16, f21, f4
	nop
	FMADD	f5,  f17, f21, f5
	LFD	f21,  1 * SIZE(BO)

	FMADD	f8,  f16, f22, f8
	nop
	FMADD	f9,  f17, f22, f9
	LFD	f22,  2 * SIZE(BO)
	FMADD	f12, f16, f23, f12
	LFDU	f16,  2 * SIZE(AO)
	FMADD	f13, f17, f23, f13
	LFD	f17,  1 * SIZE(AO)
	LFD	f23,  3 * SIZE(BO)
	bdnz	.L26
	.align 4

.L28:
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

.L30:
	andi.	I,  M,  1
	ble	.L39

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
	ble	.L35
	.align 5

.L32:
	FMADD	f0,  f16, f20, f0
	LFD	f20,  8 * SIZE(BO)
	FMADD	f4,  f16, f21, f4
	LFD	f21,  9 * SIZE(BO)
	FMADD	f8,  f16, f22, f8
	LFD	f22, 10 * SIZE(BO)
	FMADD	f12, f16, f23, f12
	LFD	f23, 11 * SIZE(BO)
	LFDU	f16,  4 * SIZE(AO)

	FMADD	f1,  f17, f24, f1
	LFD	f24, 12 * SIZE(BO)
	FMADD	f5,  f17, f25, f5
	LFD	f25, 13 * SIZE(BO)
	FMADD	f9,  f17, f26, f9
	LFD	f26, 14 * SIZE(BO)
	FMADD	f13, f17, f27, f13
	LFD	f27, 15 * SIZE(BO)
	LFD	f17,  1 * SIZE(AO)

	FMADD	f0,  f18, f20, f0
	LFDU	f20, 16 * SIZE(BO)
	FMADD	f4,  f18, f21, f4
	LFD	f21,  1 * SIZE(BO)
	FMADD	f8,  f18, f22, f8
	LFD	f22,  2 * SIZE(BO)
	FMADD	f12, f18, f23, f12
	LFD	f23,  3 * SIZE(BO)
	LFD	f18,  2 * SIZE(AO)

	FMADD	f1,  f19, f24, f1
	LFD	f24,  4 * SIZE(BO)
	FMADD	f5,  f19, f25, f5
	LFD	f25,  5 * SIZE(BO)
	FMADD	f9,  f19, f26, f9
	LFD	f26,  6 * SIZE(BO)
	FMADD	f13, f19, f27, f13
	LFD	f27,  7 * SIZE(BO)
	LFD	f19,  3 * SIZE(AO)
	bdnz	.L32

	fadd	f0,  f1,   f0
	fadd	f4,  f5,   f4
	fadd	f8,  f9,   f8
	fadd	f12, f13, f12
	.align 4

.L35:
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
	ble+	.L38
	.align 4

.L36:
	FMADD	f0,  f16, f20, f0
	LFDU	f20,  4 * SIZE(BO)
	FMADD	f4,  f16, f21, f4
	LFD	f21,  1 * SIZE(BO)
	FMADD	f8,  f16, f22, f8
	LFD	f22,  2 * SIZE(BO)
	FMADD	f12, f16, f23, f12
	LFDU	f16,  1 * SIZE(AO)
	LFD	f23,  3 * SIZE(BO)
	bdnz	.L36
	.align 4

.L38:
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


.L39:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addi	KK, KK, 4
#endif

	mr	B,  BO
	addic.	J, J, -1
	bgt	.L10
	.align 4

.L40:
	mr	CO1, C
	add	CO2, C,  LDC
	andi.	J, N,  2
	ble	.L70

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
	ble	.L50
	.align 4

.L41:
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

	srawi.	r0,  K,  2
	mtspr	CTR, r0
	mr	BO,  B
#endif
	ble	.L45
	.align 5

.L42:
	FMADD	f0,  f16, f20, f0
	FMADD	f1,  f17, f20, f1
	FMADD	f2,  f18, f20, f2
	FMADD	f3,  f19, f20, f3
	LFD	f20,  4 * SIZE(BO)

	FMADD	f4,  f16, f21, f4
	LFD	f16,  4 * SIZE(AO)
	FMADD	f5,  f17, f21, f5
	LFD	f17,  5 * SIZE(AO)
	FMADD	f6,  f18, f21, f6
	LFD	f18,  6 * SIZE(AO)
	FMADD	f7,  f19, f21, f7
	LFD	f19,  7 * SIZE(AO)

	FMADD	f0,  f16, f22, f0
	LFD	f21,  5 * SIZE(BO)
	FMADD	f1,  f17, f22, f1
	FMADD	f2,  f18, f22, f2
	FMADD	f3,  f19, f22, f3
	LFD	f22,  6 * SIZE(BO)

	FMADD	f4,  f16, f23, f4
	LFD	f16,  8 * SIZE(AO)
	FMADD	f5,  f17, f23, f5
	LFD	f17,  9 * SIZE(AO)
	FMADD	f6,  f18, f23, f6
	LFD	f18, 10 * SIZE(AO)
	FMADD	f7,  f19, f23, f7
	LFD	f19, 11 * SIZE(AO)

	FMADD	f0,  f16, f20, f0
	LFD	f23,  7 * SIZE(BO)
	FMADD	f1,  f17, f20, f1
	FMADD	f2,  f18, f20, f2
	FMADD	f3,  f19, f20, f3
	LFDU	f20,  8 * SIZE(BO)

	FMADD	f4,  f16, f21, f4
	LFD	f16, 12 * SIZE(AO)
	FMADD	f5,  f17, f21, f5
	LFD	f17, 13 * SIZE(AO)
	FMADD	f6,  f18, f21, f6
	LFD	f18, 14 * SIZE(AO)
	FMADD	f7,  f19, f21, f7
	LFD	f19, 15 * SIZE(AO)

	FMADD	f0,  f16, f22, f0
	LFD	f21,  1 * SIZE(BO)
	FMADD	f1,  f17, f22, f1
	FMADD	f2,  f18, f22, f2
	FMADD	f3,  f19, f22, f3
	LFD	f22,  2 * SIZE(BO)

	FMADD	f4,  f16, f23, f4
	LFDU	f16, 16 * SIZE(AO)
	FMADD	f5,  f17, f23, f5
	LFD	f17,  1 * SIZE(AO)
	FMADD	f6,  f18, f23, f6
	LFD	f18,  2 * SIZE(AO)
	FMADD	f7,  f19, f23, f7
	LFD	f19,  3 * SIZE(AO)

	LFD	f23,  3 * SIZE(BO)
	bdnz	.L42
	.align 4

.L45:
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
	ble+	.L48
	.align 4

.L46:
	FMADD	f0,  f16, f20, f0
	FMADD	f1,  f17, f20, f1
	FMADD	f2,  f18, f20, f2
	FMADD	f3,  f19, f20, f3
	LFDU	f20,  2 * SIZE(BO)

	FMADD	f4,  f16, f21, f4
	LFDU	f16,  4 * SIZE(AO)
	FMADD	f5,  f17, f21, f5
	LFD	f17,  1 * SIZE(AO)
	FMADD	f6,  f18, f21, f6
	LFD	f18,  2 * SIZE(AO)
	FMADD	f7,  f19, f21, f7
	LFD	f19,  3 * SIZE(AO)
	LFD	f21,  1 * SIZE(BO)
	bdnz	.L46
	.align 4

.L48:
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
	bgt+	.L41
	.align 4

.L50:
	andi.	I,  M,  2
	ble	.L60

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
	ble	.L55
	.align 5

.L52:
	FMADD	f0,  f16, f20, f0
	FMADD	f1,  f17, f20, f1
	LFDU	f20,  8 * SIZE(BO)
	FMADD	f2,  f16, f21, f2
	LFD	f16,  4 * SIZE(AO)
	FMADD	f3,  f17, f21, f3
	LFD	f17,  5 * SIZE(AO)

	FMADD	f4,  f18, f22, f4
	LFD	f21,  1 * SIZE(BO)
	FMADD	f5,  f19, f22, f5
	LFD	f22,  2 * SIZE(BO)
	FMADD	f6,  f18, f23, f6
	LFD	f18,  6 * SIZE(AO)
	FMADD	f7,  f19, f23, f7
	LFD	f19,  7 * SIZE(AO)

	FMADD	f0,  f16, f24, f0
	LFD	f23,  3 * SIZE(BO)
	FMADD	f1,  f17, f24, f1
	LFD	f24,  4 * SIZE(BO)
	FMADD	f2,  f16, f25, f2
	LFDU	f16,  8 * SIZE(AO)
	FMADD	f3,  f17, f25, f3
	LFD	f17,  1 * SIZE(AO)

	FMADD	f4,  f18, f26, f4
	LFD	f25,  5 * SIZE(BO)
	FMADD	f5,  f19, f26, f5
	LFD	f26,  6 * SIZE(BO)
	FMADD	f6,  f18, f27, f6
	LFD	f18,  2 * SIZE(AO)
	FMADD	f7,  f19, f27, f7
	LFD	f19,  3 * SIZE(AO)

	LFD	f27,  7 * SIZE(BO)
	bdnz	.L52
	.align 4

.L55:
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
	ble+	.L58
	.align 4

.L56:
	FMADD	f0,  f16, f20, f0
	FMADD	f1,  f17, f20, f1
	LFDU	f20,  2 * SIZE(BO)
	FMADD	f2,  f16, f21, f2
	LFDU	f16,  2 * SIZE(AO)
	FMADD	f3,  f17, f21, f3
	LFD	f17,  1 * SIZE(AO)
	LFD	f21,  1 * SIZE(BO)
	bdnz	.L56
	.align 4

.L58:
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

.L60:
	andi.	I,  M,  1
	ble	.L69

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
	ble	.L65
	.align 5

.L62:
	FMADD	f0,  f16, f20, f0
	LFDU	f20,  8 * SIZE(BO)
	FMADD	f1,  f16, f21, f1
	LFDU	f16,  4 * SIZE(AO)
	LFD	f21,  1 * SIZE(BO)
	FMADD	f2,  f17, f22, f2
	LFD	f22,  2 * SIZE(BO)
	FMADD	f3,  f17, f23, f3
	LFD	f17,  1 * SIZE(AO)
	LFD	f23,  3 * SIZE(BO)

	FMADD	f0,  f18, f24, f0
	LFD	f24,  4 * SIZE(BO)
	FMADD	f1,  f18, f25, f1
	LFD	f18,  2 * SIZE(AO)
	LFD	f25,  5 * SIZE(BO)
	FMADD	f2,  f19, f26, f2
	LFD	f26,  6 * SIZE(BO)
	FMADD	f3,  f19, f27, f3
	LFD	f19,  3 * SIZE(AO)
	LFD	f27,  7 * SIZE(BO)
	bdnz	.L62
	.align 4

.L65:
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
	ble+	.L68
	.align 4

.L66:
	FMADD	f0,  f16, f20, f0
	LFDU	f20,  2 * SIZE(BO)
	FMADD	f1,  f16, f21, f1
	LFDU	f16,  1 * SIZE(AO)
	LFD	f21,  1 * SIZE(BO)
	bdnz	.L66
	.align 4

.L68:
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

.L69:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addi	KK, KK, 2
#endif

	mr	B,  BO
	.align 4

.L70:
	mr	CO1, C
	andi.	J, N,  1
	ble	.L999

#if defined(TRMMKERNEL) && defined(LEFT)
	mr	KK, OFFSET
#endif

	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

	srawi.	I, M,  2
	mr	AO, A
	ble	.L80
	.align 4

.L71:
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

	srawi.	r0,  K,  2
	mtspr	CTR, r0
	mr	BO,  B
	ble	.L75

#endif
	ble	.L75
	.align 5

.L72:
	FMADD	f0,  f16, f20, f0
	LFD	f16,  4 * SIZE(AO)
	FMADD	f1,  f17, f20, f1
	LFD	f17,  5 * SIZE(AO)
	FMADD	f2,  f18, f20, f2
	LFD	f18,  6 * SIZE(AO)
	FMADD	f3,  f19, f20, f3
	LFD	f19,  7 * SIZE(AO)
	LFDU	f20,  4 * SIZE(BO)

	FMADD	f0,  f16, f21, f0
	LFD	f16,  8 * SIZE(AO)
	FMADD	f1,  f17, f21, f1
	LFD	f17,  9 * SIZE(AO)
	FMADD	f2,  f18, f21, f2
	LFD	f18, 10 * SIZE(AO)
	FMADD	f3,  f19, f21, f3
	LFD	f19, 11 * SIZE(AO)
	LFD	f21,  1 * SIZE(BO)

	FMADD	f0,  f16, f22, f0
	LFD	f16, 12 * SIZE(AO)
	FMADD	f1,  f17, f22, f1
	LFD	f17, 13 * SIZE(AO)
	FMADD	f2,  f18, f22, f2
	LFD	f18, 14 * SIZE(AO)
	FMADD	f3,  f19, f22, f3
	LFD	f19, 15 * SIZE(AO)
	LFD	f22,  2 * SIZE(BO)

	FMADD	f0,  f16, f23, f0
	LFDU	f16, 16 * SIZE(AO)
	FMADD	f1,  f17, f23, f1
	LFD	f17,  1 * SIZE(AO)
	FMADD	f2,  f18, f23, f2
	LFD	f18,  2 * SIZE(AO)
	FMADD	f3,  f19, f23, f3
	LFD	f19,  3 * SIZE(AO)
	LFD	f23,  3 * SIZE(BO)
	bdnz	.L72
	.align 4

.L75:
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
	ble+	.L78
	.align 4

.L76:
	FMADD	f0,  f16, f20, f0
	LFDU	f16,  4 * SIZE(AO)
	FMADD	f1,  f17, f20, f1
	LFD	f17,  1 * SIZE(AO)
	FMADD	f2,  f18, f20, f2
	LFD	f18,  2 * SIZE(AO)
	FMADD	f3,  f19, f20, f3
	LFDU	f20,  1 * SIZE(BO)
	LFD	f19,  3 * SIZE(AO)
	bdnz	.L76
	.align 4

.L78:
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
	bgt+	.L71
	.align 4

.L80:
	andi.	I,  M,  2
	ble	.L90

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
	ble	.L85
	.align 5

.L82:
	FMADD	f0,  f16, f20, f0
	LFD	f16,  4 * SIZE(AO)
	FMADD	f1,  f17, f20, f1
	LFDU	f20,  4 * SIZE(BO)
	LFD	f17,  5 * SIZE(AO)
	FMADD	f2,  f18, f21, f2
	LFD	f18,  6 * SIZE(AO)
	FMADD	f3,  f19, f21, f3
	LFD	f21,  1 * SIZE(BO)
	LFD	f19,  7 * SIZE(AO)

	FMADD	f0,  f16, f22, f0
	LFDU	f16,  8 * SIZE(AO)
	FMADD	f1,  f17, f22, f1
	LFD	f22,  2 * SIZE(BO)
	LFD	f17,  1 * SIZE(AO)
	FMADD	f2,  f18, f23, f2
	LFD	f18,  2 * SIZE(AO)
	FMADD	f3,  f19, f23, f3
	LFD	f23,  3 * SIZE(BO)
	LFD	f19,  3 * SIZE(AO)
	bdnz	.L82
	.align 4

.L85:
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
	ble+	.L88
	.align 4

.L86:
	FMADD	f0,  f16, f20, f0
	LFDU	f16,  2 * SIZE(AO)
	FMADD	f1,  f17, f20, f1
	LFDU	f20,  1 * SIZE(BO)
	LFD	f17,  1 * SIZE(AO)
	bdnz	.L86
	.align 4

.L88:
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

.L90:
	andi.	I,  M,  1
	ble	.L999


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
	ble	.L95
	.align 5

.L92:
	FMADD	f0,  f16, f20, f0
	LFD	f16,  4 * SIZE(AO)
	LFD	f20,  4 * SIZE(BO)
	FMADD	f1,  f17, f21, f1
	LFD	f17,  5 * SIZE(AO)
	LFD	f21,  5 * SIZE(BO)
	FMADD	f2,  f18, f22, f2
	LFD	f18,  6 * SIZE(AO)
	LFD	f22,  6 * SIZE(BO)
	FMADD	f3,  f19, f23, f3
	LFD	f19,  7 * SIZE(AO)
	LFD	f23,  7 * SIZE(BO)

	FMADD	f0,  f16, f20, f0
	LFDU	f16,  8 * SIZE(AO)
	LFDU	f20,  8 * SIZE(BO)
	FMADD	f1,  f17, f21, f1
	LFD	f17,  1 * SIZE(AO)
	LFD	f21,  1 * SIZE(BO)
	FMADD	f2,  f18, f22, f2
	LFD	f18,  2 * SIZE(AO)
	LFD	f22,  2 * SIZE(BO)
	FMADD	f3,  f19, f23, f3
	LFD	f19,  3 * SIZE(AO)
	LFD	f23,  3 * SIZE(BO)
	bdnz	.L92
	.align 4

.L95:
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
	ble+	.L98
	.align 4

.L96:
	FMADD	f0,  f16, f20, f0
	LFDU	f16,  1 * SIZE(AO)
	LFDU	f20,  1 * SIZE(BO)
	bdnz	.L96
	.align 4

.L98:
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

.L999:
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
#if defined(TRMMKERNEL) || defined(TRSMKERNEL)
	ld	r19,  240(SP)
	ld	r18,  248(SP)
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
	lwz	r20,  188(SP)
#if defined(TRMMKERNEL) || defined(TRSMKERNEL)
	lwz	r19,  192(SP)
	lwz	r18,  196(SP)
#endif
#endif

	addi	SP, SP, STACKSIZE

	blr

	EPILOGUE
