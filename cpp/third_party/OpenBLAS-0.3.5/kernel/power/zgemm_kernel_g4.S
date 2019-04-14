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
#ifdef TRMMKERNEL
	std	r23,  208(SP)
	std	r22,  216(SP)
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
#ifdef TRMMKERNEL
	stw	r23,  176(SP)
	stw	r22,  180(SP)
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
	li	PREA, 8 * 8 * SIZE
	li	PREC, 3 * SIZE

	cmpwi	cr0, M, 0
	ble	.L999
	cmpwi	cr0, N, 0
	ble	.L999
	cmpwi	cr0, K, 0
	ble	.L999

	lfs	f0, FZERO

	srawi.	J, N,  1
	ble	.L30
	.align 4

.L10:
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

	mr	CO1, C
	add	CO2, C,  LDC
	add	C,  CO2, LDC

#if defined(TRMMKERNEL) && defined(LEFT)
	mr	KK, OFFSET
#endif

	srawi.	I, M,  1
	mr	AO, A
	ble	.L20
	.align 4

.L11:
#ifndef TRMMKERNEL
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

	srawi.	r0,  K,  1
	mr	BO,  B
	mtspr	CTR, r0
	ble	.L15
#else
#if	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

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
	slwi	r0, KK, 1 + ZBASE_SHIFT
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

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 2
#endif
	srawi.	TEMP,  TEMP,  1
	mtspr	CTR, TEMP
	ble	.L15
#endif
	.align 4

.L12:
	FMADD	f0,  A1, B1, f0
	dcbt	AO, PREA
	FMADD	f4,  A1, B2, f4
	LFDU	B5,  4 * SIZE(BO)

	FMADD	f8,  A1, B3, f8
	dcbt	BO, PREA
	FMADD	f12, A1, B4, f12
	LFD	A4, -1 * SIZE(AO)

	FMADD	f1,  A2, B1, f1
	nop
	FMADD	f5,  A2, B2, f5
	LFD	B6,  1 * SIZE(BO)

	FMADD	f9,  A2, B3, f9
	LFDU	A1,  4 * SIZE(AO)
	FMADD	f13, A2, B4, f13
	nop

	FMADD	f2,  A3, B1, f2
	nop
	FMADD	f6,  A3, B2, f6
	LFD	B7,  2 * SIZE(BO)

	FMADD	f10, A3, B3, f10
	LFD	A2, -3 * SIZE(AO)
	FMADD	f14, A3, B4, f14
	nop

	FMADD	f3,  A4, B1, f3
	nop
	FMADD	f7,  A4, B2, f7
	LFD	B8,  3 * SIZE(BO)

	FMADD	f11, A4, B3, f11
	LFD	A3, -2 * SIZE(AO)
	FMADD	f15, A4, B4, f15
	nop

	FMADD	f0,  A5, B5, f0
#ifdef DOUBLE
	dcbt	AO, PREA
#else
	nop
#endif
	FMADD	f4,  A5, B6, f4
	LFDU	B1,  4 * SIZE(BO)

	FMADD	f8,  A5, B7, f8
#ifdef DOUBLE
	dcbt	BO, PREA
#else
	nop
#endif
	FMADD	f12, A5, B8, f12
	LFD	A4, -1 * SIZE(AO)

	FMADD	f1,  A2, B5, f1
	nop
	FMADD	f5,  A2, B6, f5
	LFD	B2,  1 * SIZE(BO)

	FMADD	f9,  A2, B7, f9
	LFDU	A5,  4 * SIZE(AO)
	FMADD	f13, A2, B8, f13
	nop

	FMADD	f2,  A3, B5, f2
	nop
	FMADD	f6,  A3, B6, f6
	LFD	B3,  2 * SIZE(BO)

	FMADD	f10, A3, B7, f10
	LFD	A2, -3 * SIZE(AO)
	FMADD	f14, A3, B8, f14
	nop

	FMADD	f3,  A4, B5, f3
	nop
	FMADD	f7,  A4, B6, f7
	LFD	B4,  3 * SIZE(BO)

	FMADD	f11, A4, B7, f11
	LFD	A3, -2 * SIZE(AO)
	FMADD	f15, A4, B8, f15
	bdnz	.L12
	.align 4
	.align 4

.L15:
	addi	AO, AO, -4 * SIZE

#ifndef TRMMKERNEL
	andi.	r0,  K,  1
	lfd	f30,  ALPHA_R
	lfd	f31,  ALPHA_I
	ble	.LKERNEL_MainFinish
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 2
#endif
	andi.	TEMP,  TEMP,  1
	lfd	f30,  ALPHA_R
	lfd	f31,  ALPHA_I
	ble	.LKERNEL_MainFinish
#endif
	.align 4

.L16:
	FMADD	f0,  A1, B1, f0
	LFD	A4,  3 * SIZE(AO)
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
	addi	AO, AO, 4 * SIZE
	FMADD	f15, A4, B4, f15
	addi	BO, BO, 4 * SIZE
	.align 4

.LKERNEL_MainFinish:
#ifndef TRMMKERNEL
	LFD	f16, 0 * SIZE(CO1)
	LFD	f17, 1 * SIZE(CO1)
	LFD	f18, 2 * SIZE(CO1)
	LFD	f19, 3 * SIZE(CO1)
#endif

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
      defined(CC) || defined(CR) || defined(RC) || defined(RR)

	FSUB	  f0,  f0,  f5
	FADD	  f1,  f1,  f4
	FSUB	  f2,  f2,  f7
	FADD	  f3,  f3,  f6

#ifndef TRMMKERNEL
	LFD	f20, 0 * SIZE(CO2)
	LFD	f21, 1 * SIZE(CO2)
	LFD	f22, 2 * SIZE(CO2)
	LFD	f23, 3 * SIZE(CO2)
#endif

	FSUB	  f8,  f8,  f13
	FADD	  f9,  f9,  f12
	FSUB	  f10, f10, f15
	FADD	  f11, f11, f14

#elif defined(CN) || defined(CT) || defined(RN) || defined(RT)

	FADD	  f0,  f0,  f5
	FSUB	  f1,  f1,  f4
	FADD	  f2,  f2,  f7
	FSUB	  f3,  f3,  f6

#ifndef TRMMKERNEL
	LFD	f20, 0 * SIZE(CO2)
	LFD	f21, 1 * SIZE(CO2)
	LFD	f22, 2 * SIZE(CO2)
	LFD	f23, 3 * SIZE(CO2)
#endif

	FADD	  f8,  f8,  f13
	FSUB	  f9,  f9,  f12
	FADD	  f10, f10, f15
	FSUB	  f11, f11, f14

#else /* defined(NC) || defined(TC) || defined(NR) || defined(TR) */

	FADD	  f0,  f0,  f5
	FSUB	  f1,  f4,  f1
	FADD	  f2,  f2,  f7
	FSUB	  f3,  f6,  f3

#ifndef TRMMKERNEL
	LFD	f20, 0 * SIZE(CO2)
	LFD	f21, 1 * SIZE(CO2)
	LFD	f22, 2 * SIZE(CO2)
	LFD	f23, 3 * SIZE(CO2)
#endif

	FADD	  f8,  f8,  f13
	FSUB	  f9,  f12, f9
	FADD	  f10, f10, f15
	FSUB	  f11, f14, f11

#endif

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)

#ifndef TRMMKERNEL
	FMADD	f16, f30, f0,  f16
	FMADD	f17, f30, f1,  f17
	FMADD	f18, f30, f2,  f18
	FMADD	f19, f30, f3,  f19

	FMADD	f20, f30, f8,  f20
	FMADD	f21, f30, f9,  f21
	FMADD	f22, f30, f10, f22
	FMADD	f23, f30, f11, f23
#else
	FMUL	f16, f30, f0
	FMUL	f17, f30, f1
	FMUL	f18, f30, f2
	FMUL	f19, f30, f3

	FMUL	f20, f30, f8
	FMUL	f21, f30, f9
	FMUL	f22, f30, f10
	FMUL	f23, f30, f11
#endif

	FNMSUB	f16, f31, f1,  f16
	FMADD	f17, f31, f0,  f17
	FNMSUB	f18, f31, f3,  f18
	FMADD	f19, f31, f2,  f19

	FNMSUB	f20, f31, f9,  f20
	FMADD	f21, f31, f8,  f21
	FNMSUB	f22, f31, f11, f22
	FMADD	f23, f31, f10, f23

#else /* defined(CN)||defined(CT)||defined(NC)||defined(TC)||defined(CC) */
      /* defined(RN)||defined(RT)||defined(NR)||defined(TR)||defined(CR) */
      /* defined(RC)|| defined(RR) */

#ifndef TRMMKERNEL
	FMADD	f16, f30, f0,  f16
	FNMSUB	f17, f30, f1,  f17
	FMADD	f18, f30, f2,  f18
	FNMSUB	f19, f30, f3,  f19

	FMADD	f20, f30, f8,  f20
	FNMSUB	f21, f30, f9,  f21
	FMADD	f22, f30, f10, f22
	FNMSUB	f23, f30, f11, f23

	FMADD	f16, f31, f1,  f16
	FMADD	f17, f31, f0,  f17
	FMADD	f18, f31, f3,  f18
	FMADD	f19, f31, f2,  f19

	FMADD	f20, f31, f9,  f20
	FMADD	f21, f31, f8,  f21
	FMADD	f22, f31, f11, f22
	FMADD	f23, f31, f10, f23
#else
	FMUL	f16, f30, f0
	FMUL	f17, f30, f1
	FMUL	f18, f30, f2
	FMUL	f19, f30, f3

	FMUL	f20, f30, f8
	FMUL	f21, f30, f9
	FMUL	f22, f30, f10
	FMUL	f23, f30, f11

	FMADD	f16, f31, f1,  f16
	FNMADD	f17, f31, f0,  f17
	FMADD	f18, f31, f3,  f18
	FNMADD	f19, f31, f2,  f19

	FMADD	f20, f31, f9,  f20
	FNMADD	f21, f31, f8,  f21
	FMADD	f22, f31, f11, f22
	FNMADD	f23, f31, f10, f23
#endif
#endif

	STFD	f16,  0 * SIZE(CO1)
	STFD	f17,  1 * SIZE(CO1)
	STFD	f18,  2 * SIZE(CO1)
	STFD	f19,  3 * SIZE(CO1)

	lfs	f0,  FZERO
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

	STFD	f20,  0 * SIZE(CO2)
	STFD	f21,  1 * SIZE(CO2)
	STFD	f22,  2 * SIZE(CO2)
	STFD	f23,  3 * SIZE(CO2)

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
	slwi	TEMP, TEMP, 1 + ZBASE_SHIFT
	add	AO, AO, TEMP
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 2
#endif
#endif

	addic.	I, I, -1
	bgt	.L11
	.align 4

.L20:
	andi.	I,  M,  1
	ble	.L29

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

	lfs	f0, FZERO
	fmr	f1, f0
	fmr	f2, f0
	fmr	f3, f0
	fmr	f4, f0
	fmr	f5, f0
	fmr	f6, f0
	fmr	f7, f0

	srawi.	r0,  K,  2
	mr	BO,  B
	mtspr	CTR, r0
	ble	.L25
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

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 2
#endif
	srawi.	TEMP,  TEMP,  2
	mtspr	CTR, TEMP
	ble	.L25
#endif
	.align 4

.L22:
	fmadd	f0,  f16, f20, f0
	LFD	f27,  7 * SIZE(BO)
	fmadd	f1,  f16, f21, f1
	LFD	f19,  3 * SIZE(AO)
	fmadd	f2,  f16, f22, f2
	nop
	fmadd	f3,  f16, f23, f3
	LFD	f16,  4 * SIZE(AO)

	fmadd	f4,  f17, f20, f4
 	LFD	f20,  8 * SIZE(BO)
	fmadd	f5,  f17, f21, f5
	LFD	f21,  9 * SIZE(BO)
	fmadd	f6,  f17, f22, f6
	LFD	f22, 10 * SIZE(BO)
	fmadd	f7,  f17, f23, f7
	LFD	f23, 11 * SIZE(BO)

	fmadd	f0,  f18, f24, f0
	LFD	f17,  5 * SIZE(AO)
	fmadd	f1,  f18, f25, f1
	nop
	fmadd	f2,  f18, f26, f2
	nop
	fmadd	f3,  f18, f27, f3
	LFD	f18,  6 * SIZE(AO)

	fmadd	f4,  f19, f24, f4
 	LFD	f24, 12 * SIZE(BO)
	fmadd	f5,  f19, f25, f5
	LFD	f25, 13 * SIZE(BO)
	fmadd	f6,  f19, f26, f6
	LFD	f26, 14 * SIZE(BO)
	fmadd	f7,  f19, f27, f7
	LFD	f27, 15 * SIZE(BO)

	fmadd	f0,  f16, f20, f0
	LFD	f19,  7 * SIZE(AO)
	fmadd	f1,  f16, f21, f1
	nop
	fmadd	f2,  f16, f22, f2
	nop
	fmadd	f3,  f16, f23, f3
	LFDU	f16,  8 * SIZE(AO)

	fmadd	f4,  f17, f20, f4
 	LFDU	f20, 16 * SIZE(BO)
	fmadd	f5,  f17, f21, f5
	LFD	f21,  1 * SIZE(BO)
	fmadd	f6,  f17, f22, f6
	LFD	f22,  2 * SIZE(BO)
	fmadd	f7,  f17, f23, f7
	LFD	f23,  3 * SIZE(BO)

	fmadd	f0,  f18, f24, f0
	LFD	f17,  1 * SIZE(AO)
	fmadd	f1,  f18, f25, f1
	nop
	fmadd	f2,  f18, f26, f2
	nop
	fmadd	f3,  f18, f27, f3
	LFD	f18,  2 * SIZE(AO)

	fmadd	f4,  f19, f24, f4
 	LFD	f24,  4 * SIZE(BO)
	fmadd	f5,  f19, f25, f5
	LFD	f25,  5 * SIZE(BO)
	fmadd	f6,  f19, f26, f6
	LFD	f26,  6 * SIZE(BO)
	fmadd	f7,  f19, f27, f7
	bdnz	.L22
	.align 4

.L25:
#ifndef TRMMKERNEL
	andi.	r0,  K,  3
	lfd	f30, ALPHA_R
	lfd	f31, ALPHA_I
	mtspr	CTR, r0
	ble	.L27
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 2
#endif
	andi.	TEMP,  TEMP,  3
	lfd	f30, ALPHA_R
	lfd	f31, ALPHA_I
	mtspr	CTR, TEMP
	ble	.L27
#endif
	.align 4

.L26:
	fmadd	f0,  f16, f20, f0
	fmadd	f1,  f16, f21, f1
	fmadd	f2,  f16, f22, f2
	fmadd	f3,  f16, f23, f3
	LFDU	f16,  2 * SIZE(AO)

	fmadd	f4,  f17, f20, f4
 	LFDU	f20,  4 * SIZE(BO)
	fmadd	f5,  f17, f21, f5
	LFD	f21,  1 * SIZE(BO)
	fmadd	f6,  f17, f22, f6
	LFD	f22,  2 * SIZE(BO)
	fmadd	f7,  f17, f23, f7
	LFD	f23,  3 * SIZE(BO)
	LFD	f17,  1 * SIZE(AO)
	bdnz	.L26
	.align 4

.L27:
#if   defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
      defined(CC) || defined(CR) || defined(RC) || defined(RR)

	FSUB	  f0,  f0,  f5
	FADD	  f1,  f1,  f4
	FSUB	  f2,  f2,  f7
	FADD	  f3,  f3,  f6

#elif defined(CN) || defined(CT) || defined(RN) || defined(RT)

	FADD	  f0,  f0,  f5
	FSUB	  f1,  f4,  f1
	FADD	  f2,  f2,  f7
	FSUB	  f3,  f6,  f3

#else /* defined(NC) || defined(TC) || defined(NR) || defined(TR) */

	FADD	  f0,  f0,  f5
	FSUB	  f1,  f1,  f4
	FADD	  f2,  f2,  f7
	FSUB	  f3,  f3,  f6

#endif

#ifndef TRMMKERNEL
	LFD	f16, 0 * SIZE(CO1)
	LFD	f17, 1 * SIZE(CO1)

	LFD	f18, 0 * SIZE(CO2)
	LFD	f19, 1 * SIZE(CO2)
#endif

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)

#ifndef TRMMKERNEL
	FMADD	f16, f30, f0,  f16
	FMADD	f17, f30, f1,  f17
	FMADD	f18, f30, f2,  f18
	FMADD	f19, f30, f3,  f19
#else
	FMUL	f16, f30, f0
	FMUL	f17, f30, f1
	FMUL	f18, f30, f2
	FMUL	f19, f30, f3
#endif

	FNMSUB	f16, f31, f1,  f16
	FMADD	f17, f31, f0,  f17
	FNMSUB	f18, f31, f3,  f18
	FMADD	f19, f31, f2,  f19


#else /* defined(CN)||defined(CT)||defined(NC)||defined(TC)||defined(CC) */
      /* defined(RN)||defined(RT)||defined(NR)||defined(TR)||defined(CR) */
      /* defined(RC)|| defined(RR) */

#ifndef TRMMKERNEL
	FMADD	f16, f30, f0,  f16
	FNMSUB	f17, f30, f1,  f17
	FMADD	f18, f30, f2,  f18
	FNMSUB	f19, f30, f3,  f19

	FMADD	f16, f31, f1,  f16
	FMADD	f17, f31, f0,  f17
	FMADD	f18, f31, f3,  f18
	FMADD	f19, f31, f2,  f19
#else
	FMUL	f16, f30, f0
	FMUL	f17, f30, f1
	FMUL	f18, f30, f2
	FMUL	f19, f30, f3

	FMADD	f16, f31, f1,  f16
	FNMADD	f17, f31, f0,  f17
	FMADD	f18, f31, f3,  f18
	FNMADD	f19, f31, f2,  f19
#endif
#endif

	STFD	f16, 0 * SIZE(CO1)
	STFD	f17, 1 * SIZE(CO1)
	STFD	f18, 0 * SIZE(CO2)
	STFD	f19, 1 * SIZE(CO2)

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

.L29:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addi	KK, KK, 2
#endif

	mr	B,  BO
	addic.	J, J, -1
	lfs	f0, FZERO
	bgt	.L10
	.align 4

.L30:
	andi.	J, N,  1
	ble	.L999

#if defined(TRMMKERNEL) && defined(LEFT)
	mr	KK, OFFSET
#endif

	srawi.	I,  M,  1
	mr	CO1, C
	add	C, C, LDC
	mr	AO, A
	ble	.L40
	.align 4

.L31:
#ifndef TRMMKERNEL
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

	srawi.	r0,  K,  2
	mr	BO, B
	mtspr	CTR, r0
	ble	.L35
#else
#if	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

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

	mr	BO,  B
#else
	slwi	r0,   KK, 1 + ZBASE_SHIFT
	slwi	TEMP, KK, 0 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, B,  TEMP

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
	ble	.L35
#endif
	.align 4

.L32:
	fmadd	f0,  f16, f20, f0
	LFD	f27,  7 * SIZE(AO)
	fmadd	f1,  f16, f21, f1
	LFD	f19,  3 * SIZE(BO)
	fmadd	f2,  f16, f22, f2
	nop
	fmadd	f3,  f16, f23, f3
	LFD	f16,  4 * SIZE(BO)

	fmadd	f4,  f17, f20, f4
	LFD	f20,  8 * SIZE(AO)
	fmadd	f5,  f17, f21, f5
	LFD	f21,  9 * SIZE(AO)
	fmadd	f6,  f17, f22, f6
	LFD	f22, 10 * SIZE(AO)
	fmadd	f7,  f17, f23, f7
	LFD	f23, 11 * SIZE(AO)

	fmadd	f0,  f18, f24, f0
	LFD	f17,  5 * SIZE(BO)
	fmadd	f1,  f18, f25, f1
	nop
	fmadd	f2,  f18, f26, f2
	nop
	fmadd	f3,  f18, f27, f3
	LFD	f18,  6 * SIZE(BO)

	fmadd	f4,  f19, f24, f4
	LFD	f24, 12 * SIZE(AO)
	fmadd	f5,  f19, f25, f5
	LFD	f25, 13 * SIZE(AO)
	fmadd	f6,  f19, f26, f6
	LFD	f26, 14 * SIZE(AO)
	fmadd	f7,  f19, f27, f7
	LFD	f27, 15 * SIZE(AO)

	fmadd	f0,  f16, f20, f0
	LFD	f19,  7 * SIZE(BO)
	fmadd	f1,  f16, f21, f1
	nop
	fmadd	f2,  f16, f22, f2
	nop
	fmadd	f3,  f16, f23, f3
	LFDU	f16,  8 * SIZE(BO)

	fmadd	f4,  f17, f20, f4
	LFDU	f20, 16 * SIZE(AO)
	fmadd	f5,  f17, f21, f5
	LFD	f21,  1 * SIZE(AO)
	fmadd	f6,  f17, f22, f6
	LFD	f22,  2 * SIZE(AO)
	fmadd	f7,  f17, f23, f7
	LFD	f23,  3 * SIZE(AO)

	fmadd	f0,  f18, f24, f0
	LFD	f17,  1 * SIZE(BO)
	fmadd	f1,  f18, f25, f1
	nop
	fmadd	f2,  f18, f26, f2
	nop
	fmadd	f3,  f18, f27, f3
	LFD	f18,  2 * SIZE(BO)

	fmadd	f4,  f19, f24, f4
	LFD	f24,  4 * SIZE(AO)
	fmadd	f5,  f19, f25, f5
	LFD	f25,  5 * SIZE(AO)
	fmadd	f6,  f19, f26, f6
	LFD	f26,  6 * SIZE(AO)
	fmadd	f7,  f19, f27, f7
	bdnz	.L32
	.align 4

.L35:
#ifndef TRMMKERNEL
	andi.	r0,  K,  3
	lfd	f30, ALPHA_R
	lfd	f31, ALPHA_I
	mtspr	CTR, r0
	ble	.L37
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 1
#endif
	andi.	TEMP,  TEMP,  3
	lfd	f30, ALPHA_R
	lfd	f31, ALPHA_I
	mtspr	CTR, TEMP
	ble	.L37
#endif
	.align 4

.L36:
	fmadd	f0,  f16, f20, f0
	fmadd	f1,  f16, f21, f1
	fmadd	f2,  f16, f22, f2
	fmadd	f3,  f16, f23, f3
	LFDU	f16, 2 * SIZE(BO)

	fmadd	f4,  f17, f20, f4
	LFDU	f20, 4 * SIZE(AO)
	fmadd	f5,  f17, f21, f5
	LFD	f21, 1 * SIZE(AO)
	fmadd	f6,  f17, f22, f6
	LFD	f22, 2 * SIZE(AO)
	fmadd	f7,  f17, f23, f7
	LFD	f23, 3 * SIZE(AO)
	LFD	f17, 1 * SIZE(BO)
	bdnz	.L36
	.align 4

.L37:
#if   defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
      defined(CC) || defined(CR) || defined(RC) || defined(RR)

	FSUB	  f0,  f0,  f5
	FADD	  f1,  f1,  f4
	FSUB	  f2,  f2,  f7
	FADD	  f3,  f3,  f6

#elif defined(CN) || defined(CT) || defined(RN) || defined(RT)

	FADD	  f0,  f0,  f5
	FSUB	  f1,  f1,  f4
	FADD	  f2,  f2,  f7
	FSUB	  f3,  f3,  f6

#else /* defined(NC) || defined(TC) || defined(NR) || defined(TR) */

	FADD	  f0,  f0,  f5
	FSUB	  f1,  f4,  f1
	FADD	  f2,  f2,  f7
	FSUB	  f3,  f6,  f3

#endif

#ifndef TRMMKERNEL
	LFD	f16, 0 * SIZE(CO1)
	LFD	f17, 1 * SIZE(CO1)
	LFD	f18, 2 * SIZE(CO1)
	LFD	f19, 3 * SIZE(CO1)
#endif

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)

#ifndef TRMMKERNEL
	FMADD	f16, f30, f0,  f16
	FMADD	f17, f30, f1,  f17
	FMADD	f18, f30, f2,  f18
	FMADD	f19, f30, f3,  f19
#else
	FMUL	f16, f30, f0
	FMUL	f17, f30, f1
	FMUL	f18, f30, f2
	FMUL	f19, f30, f3
#endif

	FNMSUB	f16, f31, f1,  f16
	FMADD	f17, f31, f0,  f17
	FNMSUB	f18, f31, f3,  f18
	FMADD	f19, f31, f2,  f19

#else /* defined(CN)||defined(CT)||defined(NC)||defined(TC)||defined(CC) */
      /* defined(RN)||defined(RT)||defined(NR)||defined(TR)||defined(CR) */
      /* defined(RC)|| defined(RR) */

#ifndef TRMMKERNEL
	FMADD	f16, f30, f0,  f16
	FNMSUB	f17, f30, f1,  f17
	FMADD	f18, f30, f2,  f18
	FNMSUB	f19, f30, f3,  f19

	FMADD	f16, f31, f1,  f16
	FMADD	f17, f31, f0,  f17
	FMADD	f18, f31, f3,  f18
	FMADD	f19, f31, f2,  f19
#else
	FMUL	f16, f30, f0
	FMUL	f17, f30, f1
	FMUL	f18, f30, f2
	FMUL	f19, f30, f3

	FMADD	f16, f31, f1,  f16
	FNMADD	f17, f31, f0,  f17
	FMADD	f18, f31, f3,  f18
	FNMADD	f19, f31, f2,  f19
#endif

#endif

	STFD	f16, 0 * SIZE(CO1)
	STFD	f17, 1 * SIZE(CO1)
	STFD	f18, 2 * SIZE(CO1)
	STFD	f19, 3 * SIZE(CO1)

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
	bgt	.L31
	.align 4

.L40:
	andi.	I,  M,  1
	ble	.L999

#ifndef TRMMKERNEL
	LFD	f16, 0 * SIZE(AO)
	LFD	f17, 1 * SIZE(AO)
	LFD	f18, 2 * SIZE(AO)
	LFD	f19, 3 * SIZE(AO)

	LFD	f20, 0 * SIZE(B)
	LFD	f21, 1 * SIZE(B)
	LFD	f22, 2 * SIZE(B)
	LFD	f23, 3 * SIZE(B)

	lfs	f0, FZERO
	fmr	f1, f0
	fmr	f2, f0
	fmr	f3, f0
	fmr	f4, f0
	fmr	f5, f0
	fmr	f6, f0
	fmr	f7, f0

	srawi.	r0,  K,  2
	mr	BO, B
	mtspr	CTR, r0
	ble	.L45
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

	lfs	f0, FZERO
	fmr	f1, f0
	fmr	f2, f0
	fmr	f3, f0
	fmr	f4, f0
	fmr	f5, f0
	fmr	f6, f0
	fmr	f7, f0

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

	lfs	f0, FZERO
	fmr	f1, f0
	fmr	f2, f0
	fmr	f3, f0
	fmr	f4, f0
	fmr	f5, f0
	fmr	f6, f0
	fmr	f7, f0
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
	ble	.L45
#endif
	.align 4

.L42:
	fmadd	f0,  f16,  f20,  f0
	LFD	f23,  3 * SIZE(BO)
	fmadd	f3,  f16,  f21,  f3
	LFD	f16,  4 * SIZE(AO)
	fmadd	f2,  f17,  f20,  f2
	LFD	f20,  4 * SIZE(BO)
	fmadd	f1,  f17,  f21,  f1
	LFD	f17,  5 * SIZE(AO)

	fmadd	f4,  f18,  f22,  f4
	LFD	f21,  5 * SIZE(BO)
	fmadd	f7,  f18,  f23,  f7
	LFD	f18,  6 * SIZE(AO)
	fmadd	f6,  f19,  f22,  f6
	LFD	f22,  6 * SIZE(BO)
	fmadd	f5,  f19,  f23,  f5
	LFD	f19,  7 * SIZE(AO)

	fmadd	f0,  f16,  f20,  f0
	LFD	f23,  7 * SIZE(BO)
	fmadd	f3,  f16,  f21,  f3
	LFDU	f16,  8 * SIZE(AO)
	fmadd	f2,  f17,  f20,  f2
	LFDU	f20,  8 * SIZE(BO)
	fmadd	f1,  f17,  f21,  f1
	LFD	f17,  1 * SIZE(AO)

	fmadd	f4,  f18,  f22,  f4
	LFD	f21,  1 * SIZE(BO)
	fmadd	f7,  f18,  f23,  f7
	LFD	f18,  2 * SIZE(AO)
	fmadd	f6,  f19,  f22,  f6
	LFD	f22,  2 * SIZE(BO)
	fmadd	f5,  f19,  f23,  f5
	LFD	f19,  3 * SIZE(AO)
	bdnz	.L42
	.align 4

.L45:
	fadd	f0, f0, f4
	fadd	f1, f1, f5
	fadd	f2, f2, f6
	fadd	f3, f3, f7

#ifndef TRMMKERNEL
	andi.	r0,  K,  3
	lfd	f30, ALPHA_R
	lfd	f31, ALPHA_I
	mtspr	CTR,r0
	ble	.L47
#else
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 1
#endif
	andi.	TEMP,  TEMP,  3
	lfd	f30, ALPHA_R
	lfd	f31, ALPHA_I
	mtspr	CTR,TEMP
	ble	.L47
#endif
	.align 4

.L46:
	fmadd	f0,  f16,  f20,  f0
	fmadd	f3,  f16,  f21,  f3
	LFDU	f16, 2 * SIZE(AO)
	fmadd	f2,  f17,  f20,  f2
	LFDU	f20, 2 * SIZE(BO)
	fmadd	f1,  f17,  f21,  f1
	LFD	f17, 1 * SIZE(AO)
	LFD	f21, 1 * SIZE(BO)
	bdnz	.L46
	.align 4

.L47:
#if   defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
      defined(CC) || defined(CR) || defined(RC) || defined(RR)
	fsub	f0, f0, f1
	fadd	f2, f2, f3
#elif defined(CN) || defined(CT) || defined(RN) || defined(RT)
	fadd	f0, f0, f1
	fsub	f2, f2, f3
#else
	fadd	f0, f0, f1
	fsub	f2, f3, f2
#endif

#ifndef TRMMKERNEL
	LFD	f16,  0 * SIZE(CO1)
	LFD	f17,  1 * SIZE(CO1)
#endif

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#ifndef TRMMKERNEL
	FMADD	f16, f30, f0, f16
	FMADD	f17, f30, f2, f17
#else
	FMUL	f16, f30, f0
	FMUL	f17, f30, f2
#endif

	FNMSUB	f16, f31, f2, f16
	FMADD	f17, f31, f0, f17

#else /* defined(CN)||defined(CT)||defined(NC)||defined(TC)||defined(CC) */
      /* defined(RN)||defined(RT)||defined(NR)||defined(TR)||defined(CR) */
      /* defined(RC) || defined(RR) */

#ifndef TRMMKERNEL
	FMADD	f16, f30, f0, f16
	FNMSUB	f17, f30, f2, f17

	FMADD	f16, f31, f2, f16
	FMADD	f17, f31, f0, f17
#else
	FMUL	f16, f30, f0
	FMUL	f17, f30, f2

	FMADD	f16, f31, f2, f16
	FNMADD	f17, f31, f0, f17
#endif

#endif
	STFD	f16,  0 * SIZE(CO1)
	STFD	f17,  1 * SIZE(CO1)
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
#ifdef TRMMKERNEL
	ld	r23,  208(SP)
	ld	r22,  216(SP)
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
#ifdef TRMMKERNEL
	lwz	r23,  176(SP)
	lwz	r22,  180(SP)
#endif
#endif

	addi	SP, SP, STACKSIZE

	blr

	EPILOGUE
#endif
