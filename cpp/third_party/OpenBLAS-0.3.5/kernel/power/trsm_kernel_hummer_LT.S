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

#define ALPHA    0
#define FZERO	 8

#define	M	r3
#define	N	r4
#define	K	r5

#ifdef linux
#define A	r6
#define	B	r7
#define	C	r8
#define	LDC	r9
#define OFFSET	r10
#endif

#define TEMP	r11
#define AORIG	r12
#define KK	r14
#define INCM1	r15
#define INCM4	r16
#define INCM2	r17
#define INC2	r19
#define INC	r20
#define INC4	r21

#define	I	r22
#define J	r23
#define AO	r24
#define BO	r25
#define AO2	r26
#define	BO2	r27

#define	CO1	r28
#define CO2	r29
#define	CO3	r30
#define	CO4	r31

#ifndef NEEDPARAM

#define A1	f16
#define A2	f17
#define A3	f18
#define A4	f19
#define A5	f20
#define A6	f21
#define A7	f22
#define A8	f23
#define A9	f24
#define A10	f25

#define B1	f26
#define B2	f27
#define B3	f28
#define B4	f29
#define B5	f30
#define B6	f31

#define AP	B6


	PROLOGUE
	PROFCODE

	li	r0, -16

	stfpdux	f14, SP, r0
	stfpdux	f15, SP, r0
	stfpdux	f16, SP, r0
	stfpdux	f17, SP, r0
	stfpdux	f18, SP, r0
	stfpdux	f19, SP, r0
	stfpdux	f20, SP, r0
	stfpdux	f21, SP, r0
	stfpdux	f22, SP, r0
	stfpdux	f23, SP, r0
	stfpdux	f24, SP, r0
	stfpdux	f25, SP, r0
	stfpdux	f26, SP, r0
	stfpdux	f27, SP, r0
	stfpdux	f28, SP, r0
	stfpdux	f29, SP, r0
	stfpdux	f30, SP, r0
	stfpdux	f31, SP, r0

	stwu	r31,  -4(SP)
	stwu	r30,  -4(SP)
	stwu	r29,  -4(SP)
	stwu	r28,  -4(SP)

	stwu	r27,  -4(SP)
	stwu	r26,  -4(SP)
	stwu	r25,  -4(SP)
	stwu	r24,  -4(SP)

	stwu	r23,  -4(SP)
	stwu	r22,  -4(SP)
	stwu	r21,  -4(SP)
	stwu	r20,  -4(SP)

	stwu	r19,  -4(SP)
	stwu	r18,  -4(SP)
	stwu	r17,  -4(SP)
	stwu	r16,  -4(SP)

	stwu	r15,  -4(SP)
	stwu	r14,  -4(SP)	# dummy

	li	r0,   0

	stwu	r0,   -4(SP)
	stwu	r0,   -4(SP)
	stfdu	f1,   -8(SP)

	slwi	LDC, LDC, BASE_SHIFT

	cmpwi	cr0, M, 0
	ble	.L999
	cmpwi	cr0, N, 0
	ble	.L999
	cmpwi	cr0, K, 0
	ble	.L999

	li	INC,    1 * SIZE
	li	INC2,   2 * SIZE
	li	INC4,   4 * SIZE

	li	INCM1, -1 * SIZE
	li	INCM2, -2 * SIZE
	li	INCM4, -4 * SIZE

	addi	C, C, - 1 * SIZE

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

	srawi.	J, N,  2
	ble	.L50
	.align 4

.L10:
#ifdef RT
	slwi	r0, K, 2 + BASE_SHIFT
	sub	B, B, r0

	slwi	r0, LDC, 2
	sub	C, C, r0
#endif

	mr	CO1, C
	add	CO2, C,   LDC
	add	CO3, CO2, LDC
	add	CO4, CO3, LDC

#ifdef LN
	add	KK, M, OFFSET
#endif

#ifdef LT
	mr	KK, OFFSET
#endif


#if defined(LN) || defined(RT)
	addi	AORIG, A, -4 * SIZE
#else
	addi	AO, A, -4 * SIZE
#endif
#ifndef RT
	add	C,  CO4, LDC
#endif

	li	r0, FZERO
	lfpsx	f0, SP, r0

	srawi.	I, M,  3
	ble	.L20
	.align 4

.L11:
#if defined(LT) || defined(RN)
	addi	AO2, AO,   2 * SIZE
	fpmr	f4,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f12, f0

	fpmr	f5,  f0
	fpmr	f9,  f0
	fpmr	f13, f0
	fpmr	f2,  f0

	fpmr	f6,  f0
	fpmr	f10, f0
	fpmr	f14, f0
	fpmr	f3,  f0

	fpmr	f7,  f0
	fpmr	f11, f0
	fpmr	f15, f0
	nop

	srawi.	r0,  KK,  2
 	fpmr	f1,  f0
	mtspr	CTR, r0
	ble	.L14
#else

#ifdef LN
	slwi	r0,   K,  3 + BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0  , KK, 3 + BASE_SHIFT
	slwi	TEMP, KK, 2 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

	addi	AO2, AO,   2 * SIZE
	fpmr	f4,  f0
	addi	BO,  BO, - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, BO,   2 * SIZE
	fpmr	f12, f0

	fpmr	f5,  f0
	fpmr	f9,  f0
	fpmr	f13, f0
	fpmr	f2,  f0

	fpmr	f6,  f0
	fpmr	f10, f0
	fpmr	f14, f0
	fpmr	f3,  f0

	fpmr	f7,  f0
	fpmr	f11, f0
	fpmr	f15, f0
	nop

	srawi.	r0,  TEMP,  2
 	fpmr	f1,  f0
	mtspr	CTR, r0
	ble	.L14
#endif

	LFPDUX	A1,  AO, INC4
	fpmr	f5,  f0
	LFPDUX	A3,  AO, INC4
	fpmr	f9,  f0
	LFPDUX	B1,  BO, INC4
	fpmr	f13, f0

	LFPDUX	A5,  AO, INC4
	fpmr	f2,  f0
	LFPDUX	A6,  AO, INC4
	fpmr	f6,  f0
	LFPDUX	B3,  BO, INC4
	fpmr	f10, f0
	LFPDUX	A7,  AO, INC4
	fpmr	f14, f0

	LFPDUX	A8,  AO, INC4
	fpmr	f3,  f0
	LFPDUX	B5,  BO, INC4
	fpmr	f7,  f0
	LFPDUX	A9,  AO, INC4
	fpmr	f11, f0
	LFPDUX	A2, AO2, INC4
	fpmr	f15, f0
	LFPDUX	B2, BO2, INC4
	bdz-	.L13
	.align 4

.L12:

## 1 ##
	fxcpmadd	f0,  B1, A1, f0
	nop
	fxcsmadd	f4,  B1, A1, f4
	nop
	fxcpmadd	f8,  B2, A1, f8
	LFPDUX	B4, BO2, INC4
	fxcsmadd	f12, B2, A1, f12
	LFPDUX	B6,  BO, INC4

	fxcpmadd	f1,  B1, A2, f1
	nop
	fxcsmadd	f5,  B1, A2, f5
	LFPDUX	A4, AO2, INC4
	fxcpmadd	f9,  B2, A2, f9
	LFPDUX	A10, AO, INC4
	fxcsmadd	f13, B2, A2, f13
	nop

	fxcpmadd	f2,  B1, A3, f2
	nop
	fxcsmadd	f6,  B1, A3, f6
	nop
	fxcpmadd	f10, B2, A3, f10
	nop
	fxcsmadd	f14, B2, A3, f14
	nop

	fxcpmadd	f3,  B1, A4, f3
	nop
	fxcsmadd	f7,  B1, A4, f7
	LFPDUX	A2, AO2, INC4
	fxcpmadd	f11, B2, A4, f11
	LFPDUX	A1,  AO, INC4
	fxcsmadd	f15, B2, A4, f15
	nop

## 2 ##

	fxcpmadd	f0,  B3, A5, f0
	nop
	fxcsmadd	f4,  B3, A5, f4
	nop
	fxcpmadd	f8,  B4, A5, f8
	LFPDUX	B2, BO2, INC4
	fxcsmadd	f12, B4, A5, f12
	LFPDUX	B1,  BO, INC4

	fxcpmadd	f1,  B3, A2, f1
	nop
	fxcsmadd	f5,  B3, A2, f5
	LFPDUX	A4, AO2, INC4
	fxcpmadd	f9,  B4, A2, f9
	LFPDUX	A3,  AO, INC4
	fxcsmadd	f13, B4, A2, f13
	nop

	fxcpmadd	f2,  B3, A6, f2
	nop
	fxcsmadd	f6,  B3, A6, f6
	nop
	fxcpmadd	f10, B4, A6, f10
	nop
	fxcsmadd	f14, B4, A6, f14
	nop

	fxcpmadd	f3,  B3, A4, f3
	nop
	fxcsmadd	f7,  B3, A4, f7
	LFPDUX	A2, AO2, INC4
	fxcpmadd	f11, B4, A4, f11
	LFPDUX	A5,  AO, INC4
	fxcsmadd	f15, B4, A4, f15
	nop

## 3 ##

	fxcpmadd	f0,  B5, A7, f0
	nop
	fxcsmadd	f4,  B5, A7, f4
	nop
	fxcpmadd	f8,  B2, A7, f8
	LFPDUX	B4, BO2, INC4
	fxcsmadd	f12, B2, A7, f12
	LFPDUX	B3,  BO, INC4

	fxcpmadd	f1,  B5, A2, f1
	nop
	fxcsmadd	f5,  B5, A2, f5
	LFPDUX	A4, AO2, INC4
	fxcpmadd	f9,  B2, A2, f9
	LFPDUX	A6,  AO, INC4
	fxcsmadd	f13, B2, A2, f13
	nop

	fxcpmadd	f2,  B5, A8, f2
	nop
	fxcsmadd	f6,  B5, A8, f6
	nop
	fxcpmadd	f10, B2, A8, f10
	nop
	fxcsmadd	f14, B2, A8, f14
	nop

	fxcpmadd	f3,  B5, A4, f3
	nop
	fxcsmadd	f7,  B5, A4, f7
	LFPDUX	A2, AO2, INC4
	fxcpmadd	f11, B2, A4, f11
	LFPDUX	A7,  AO, INC4
	fxcsmadd	f15, B2, A4, f15
	nop

## 4 ##
	fxcpmadd	f0,  B6, A9, f0
	nop
	fxcsmadd	f4,  B6, A9, f4
	nop
	fxcpmadd	f8,  B4, A9, f8
	LFPDUX	B2, BO2, INC4
	fxcsmadd	f12, B4, A9, f12
	LFPDUX	B5,  BO, INC4

	fxcpmadd	f1,  B6, A2, f1
	nop
	fxcsmadd	f5,  B6, A2, f5
	LFPDUX	A4, AO2, INC4
	fxcpmadd	f9,  B4, A2, f9
	LFPDUX	A8,  AO, INC4
	fxcsmadd	f13, B4, A2, f13
	nop

	fxcpmadd	f2,  B6, A10, f2
	nop
	fxcsmadd	f6,  B6, A10, f6
	nop
	fxcpmadd	f10, B4, A10, f10
	nop
	fxcsmadd	f14, B4, A10, f14
	nop

	fxcpmadd	f3,  B6, A4, f3
	LFPDUX	A2, AO2, INC4
	fxcsmadd	f7,  B6, A4, f7
	LFPDUX	A9,  AO, INC4
	fxcpmadd	f11, B4, A4, f11
	nop
	fxcsmadd	f15, B4, A4, f15
	bdnz+	.L12
	.align 4

.L13:
## 1 ##

	fxcpmadd	f0,  B1, A1, f0
	nop
	fxcsmadd	f4,  B1, A1, f4
	nop
	fxcpmadd	f8,  B2, A1, f8
	LFPDUX	B4, BO2, INC4
	fxcsmadd	f12, B2, A1, f12
	LFPDUX	B6,  BO, INC4

	fxcpmadd	f1,  B1, A2, f1
	nop
	fxcsmadd	f5,  B1, A2, f5
	LFPDUX	A4, AO2, INC4
	fxcpmadd	f9,  B2, A2, f9
	LFPDUX	A10, AO, INC4
	fxcsmadd	f13, B2, A2, f13
	nop

	fxcpmadd	f2,  B1, A3, f2
	nop
	fxcsmadd	f6,  B1, A3, f6
	nop
	fxcpmadd	f10, B2, A3, f10
	nop
	fxcsmadd	f14, B2, A3, f14
	nop

	fxcpmadd	f3,  B1, A4, f3
	nop
	fxcsmadd	f7,  B1, A4, f7
	LFPDUX	A2, AO2, INC4
	fxcpmadd	f11, B2, A4, f11
	nop
	fxcsmadd	f15, B2, A4, f15
	nop

## 2 ##

	fxcpmadd	f0,  B3, A5, f0
	nop
	fxcsmadd	f4,  B3, A5, f4
	nop
	fxcpmadd	f8,  B4, A5, f8
	LFPDUX	B2, BO2, INC4
	fxcsmadd	f12, B4, A5, f12
	nop

	fxcpmadd	f1,  B3, A2, f1
	nop
	fxcsmadd	f5,  B3, A2, f5
	LFPDUX	A4, AO2, INC4
	fxcpmadd	f9,  B4, A2, f9
	nop
	fxcsmadd	f13, B4, A2, f13
	nop

	fxcpmadd	f2,  B3, A6, f2
	nop
	fxcsmadd	f6,  B3, A6, f6
	nop
	fxcpmadd	f10, B4, A6, f10
	nop
	fxcsmadd	f14, B4, A6, f14
	nop

	fxcpmadd	f3,  B3, A4, f3
	nop
	fxcsmadd	f7,  B3, A4, f7
	LFPDUX	A2, AO2, INC4
	fxcpmadd	f11, B4, A4, f11
	nop
	fxcsmadd	f15, B4, A4, f15
	nop

## 3 ##

	fxcpmadd	f0,  B5, A7, f0
	nop
	fxcsmadd	f4,  B5, A7, f4
	nop
	fxcpmadd	f8,  B2, A7, f8
	LFPDUX	B4, BO2, INC4
	fxcsmadd	f12, B2, A7, f12
	nop

	fxcpmadd	f1,  B5, A2, f1
	nop
	fxcsmadd	f5,  B5, A2, f5
	LFPDUX	A4, AO2, INC4
	fxcpmadd	f9,  B2, A2, f9
	nop

	fxcsmadd	f13, B2, A2, f13

	fxcpmadd	f2,  B5, A8, f2
	nop
	fxcsmadd	f6,  B5, A8, f6
	nop
	fxcpmadd	f10, B2, A8, f10
	nop
	fxcsmadd	f14, B2, A8, f14
	nop

	fxcpmadd	f3,  B5, A4, f3
	nop
	fxcsmadd	f7,  B5, A4, f7
	LFPDUX	A2, AO2, INC4
	fxcpmadd	f11, B2, A4, f11
	nop
	fxcsmadd	f15, B2, A4, f15
	nop

## 4 ##

	fxcpmadd	f0,  B6, A9, f0
	nop
	fxcsmadd	f4,  B6, A9, f4
	nop
	fxcpmadd	f8,  B4, A9, f8
	nop
	fxcsmadd	f12, B4, A9, f12
	nop

	fxcpmadd	f1,  B6, A2, f1
	nop
	fxcsmadd	f5,  B6, A2, f5
	LFPDUX	A4, AO2, INC4
	fxcpmadd	f9,  B4, A2, f9
	nop
	fxcsmadd	f13, B4, A2, f13
	nop

	fxcpmadd	f2,  B6, A10, f2
	nop
	fxcsmadd	f6,  B6, A10, f6
	nop
	fxcpmadd	f10, B4, A10, f10
	nop
	fxcsmadd	f14, B4, A10, f14
	nop

	fxcpmadd	f3,  B6, A4, f3
	nop
	fxcsmadd	f7,  B6, A4, f7
	nop
	fxcpmadd	f11, B4, A4, f11
	nop
	fxcsmadd	f15, B4, A4, f15
	nop
	.align 4

.L14:
#if defined(LT) || defined(RN)
	andi.	r0,  KK,  3
	mtspr	CTR, r0
	ble+	.L18
#else
	andi.	r0, TEMP, 3
	mtspr	CTR, r0
	ble+	.L18
#endif
	.align 4

.L15:
	LFPDUX	A2,  AO,  INC4
	LFPDUX	A4,  AO2, INC4
	LFPDUX	A10, BO,  INC4
	LFPDUX	B4,  BO2, INC4
	bdz-	.L17
	.align 4

.L16:
	fxcpmadd	f0,  A10, A2, f0
	fxcsmadd	f4,  A10, A2, f4
	fxcpmadd	f8,  B4, A2, f8
	fxcsmadd	f12, B4, A2, f12
	LFPDUX	A2, AO,  INC4

	fxcpmadd	f1,  A10, A4, f1
	fxcsmadd	f5,  A10, A4, f5
	fxcpmadd	f9,  B4, A4, f9
	fxcsmadd	f13, B4, A4, f13
	LFPDUX	A4, AO2, INC4

	fxcpmadd	f2,  A10, A2, f2
	fxcsmadd	f6,  A10, A2, f6
	fxcpmadd	f10, B4, A2, f10
	fxcsmadd	f14, B4, A2, f14
	LFPDUX	A2, AO,  INC4

	fxcpmadd	f3,  A10, A4, f3
	fxcsmadd	f7,  A10, A4, f7
	LFPDUX	A10, BO,  INC4
	fxcpmadd	f11, B4, A4, f11
	fxcsmadd	f15, B4, A4, f15
	LFPDUX	A4, AO2, INC4
	LFPDUX	B4, BO2, INC4
	bdnz+	.L16
	.align 4

.L17:
	fxcpmadd	f0,  A10, A2, f0
	fxcsmadd	f4,  A10, A2, f4
	fxcpmadd	f8,  B4, A2, f8
	fxcsmadd	f12, B4, A2, f12
	LFPDUX	A2, AO,  INC4

	fxcpmadd	f1,  A10, A4, f1
	fxcsmadd	f5,  A10, A4, f5
	fxcpmadd	f9,  B4, A4, f9
	fxcsmadd	f13, B4, A4, f13
	LFPDUX	A4, AO2, INC4

	fxcpmadd	f2,  A10, A2, f2
	fxcsmadd	f6,  A10, A2, f6
	fxcpmadd	f10, B4, A2, f10
	fxcsmadd	f14, B4, A2, f14

	fxcpmadd	f3,  A10, A4, f3
	fxcsmadd	f7,  A10, A4, f7
	fxcpmadd	f11, B4, A4, f11
	fxcsmadd	f15, B4, A4, f15
	.align 4

.L18:
#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 8
#else
	subi	r0, KK, 4
#endif
	slwi	TEMP, r0, 3 + BASE_SHIFT
	slwi	r0,   r0, 2 + BASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO, B,     r0
	addi	AO2, AO,   2 * SIZE
	addi	BO,  BO, - 4 * SIZE
	addi	BO2, BO,   2 * SIZE
#endif

#if defined(LN) || defined(LT)
	fpmr	f24, f0
	LFPDUX	f16, BO,  INC4
	fpmr	f25, f1
	nop
	fpmr	f26, f2
	LFPDUX	f17, BO2, INC4
	fpmr	f27, f3
	nop

	fpmr	f28, f8
	LFPDUX	f18, BO,  INC4
	fpmr	f29, f9
	nop
	fpmr	f30, f10
	LFPDUX	f19, BO2, INC4
	fpmr	f31, f11
	nop

	fsmfp	f0,  f4
 	LFPDUX	f20, BO,  INC4
	fsmfp	f1,  f5
	nop
	fsmfp	f2,  f6
	LFPDUX	f21, BO2, INC4
	fsmfp	f3,  f7
	nop

	fsmfp	f8,  f12
	LFPDUX	f22, BO,  INC4
	fsmfp	f9,  f13
	nop
	fsmfp	f10, f14
	LFPDUX	f23, BO2, INC4
	fsmfp	f11, f15
	nop

	fsmtp	f4,  f24
	LFPDUX	f24, BO,  INC4
	fsmtp	f5,  f25
	nop
	fsmtp	f6,  f26
	LFPDUX	f25, BO2, INC4
	fsmtp	f7,  f27
	nop

	fsmtp	f12, f28
	LFPDUX	f26, BO,  INC4
	fsmtp	f13, f29
	nop
	fsmtp	f14, f30
	LFPDUX	f27, BO2, INC4
	fsmtp	f15, f31
	nop

	fpsub	f0,  f16,  f0
	LFPDUX	f28, BO,  INC4
	fpsub	f8,  f17,  f8
	nop
	fpsub	f4,  f18,  f4
	LFPDUX	f29, BO2, INC4
	fpsub	f12, f19,  f12
	nop

	fpsub	f1,  f20,  f1
	LFPDUX	f30, BO,  INC4
	fpsub	f9,  f21,  f9
	subi	BO,  BO,  32 * SIZE
	fpsub	f5,  f22,  f5
	LFPDUX	f31, BO2, INC4
	fpsub	f13, f23,  f13
	subi	BO2, BO2, 32 * SIZE

	fpsub	f2,  f24,  f2
	fpsub	f10, f25,  f10
	fpsub	f6,  f26,  f6
	fpsub	f14, f27,  f14
	fpsub	f3,  f28,  f3
	fpsub	f11, f29,  f11
	fpsub	f7,  f30,  f7
	fpsub	f15, f31,  f15

#else
	LFPDUX	f16, AO,  INC4
	LFPDUX	f17, AO2, INC4
	LFPDUX	f18, AO,  INC4
	LFPDUX	f19, AO2, INC4
	LFPDUX	f20, AO,  INC4
	LFPDUX	f21, AO2, INC4
	LFPDUX	f22, AO,  INC4
	LFPDUX	f23, AO2, INC4

	fpsub	f0,  f16,  f0
	LFPDUX	f24, AO,  INC4
	fpsub	f1,  f17,  f1
	LFPDUX	f25, AO2, INC4
	fpsub	f2,  f18,  f2
	LFPDUX	f26, AO,  INC4
	fpsub	f3,  f19,  f3
	LFPDUX	f27, AO2, INC4
	fpsub	f4,  f20,  f4
	LFPDUX	f28, AO,  INC4
	fpsub	f5,  f21,  f5
	LFPDUX	f29, AO2, INC4
	fpsub	f6,  f22,  f6
	LFPDUX	f30, AO,  INC4
	fpsub	f7,  f23,  f7
	LFPDUX	f31, AO2, INC4

	fpsub	f8,  f24,  f8
	subi	AO,  AO,  32 * SIZE
	fpsub	f9,  f25,  f9
	subi	AO2, AO2, 32 * SIZE
	fpsub	f10, f26,  f10
	fpsub	f11, f27,  f11
	fpsub	f12, f28,  f12
	fpsub	f13, f29,  f13
	fpsub	f14, f30,  f14
	fpsub	f15, f31,  f15
#endif

#ifdef LN
       addi	AO,  AO,  68 * SIZE
       addi	AO2, AO2, 68 * SIZE

	LFPDUX	A1,  AO2, INCM4
	LFPDUX	A2,  AO,  INCM4
	LFPDUX	A3,  AO2, INCM4
	LFPDUX	A4,  AO,  INCM4
	LFPDUX	A5,  AO2, INCM4
	LFPDUX	A6,  AO,  INCM4
	LFPDUX	A7,  AO2, INCM4
	LFPDUX	A8,  AO,  INCM4

	fxsmul	f7,  A1, f7
	fxsmul	f15, A1, f15

	fxcpnmsub  f3,  A1, f7,  f3
	fxcpnmsub  f11, A1, f15, f11

	fxcsnmsub  f6,  A2, f7,  f6
	fxcsnmsub  f14, A2, f15, f14

	fxcpnmsub  f2,  A2, f7,  f2
	fxcpnmsub  f10, A2, f15, f10

	fxcsnmsub  f5,  A3, f7,  f5
	fxcsnmsub  f13, A3, f15, f13

	fxcpnmsub  f1,  A3, f7,  f1
	fxcpnmsub  f9,  A3, f15, f9

	fxcsnmsub  f4,  A4, f7,  f4
	fxcsnmsub  f12, A4, f15, f12

	fxcpnmsub  f0,  A4, f7,  f0
	fxcpnmsub  f8,  A4, f15, f8

	fxpmul	f3,  A5, f3
	fxpmul	f11, A5, f11

	fxcsnmsub  f6,  A6, f3,  f6
	fxcsnmsub  f14, A6, f11, f14

	fxcpnmsub  f2,  A6, f3,  f2
	fxcpnmsub  f10, A6, f11, f10

	fxcsnmsub  f5,  A7, f3,  f5
	fxcsnmsub  f13, A7, f11, f13

	fxcpnmsub  f1,  A7, f3,  f1
	fxcpnmsub  f9,  A7, f11, f9

	fxcsnmsub  f4,  A8, f3,  f4
	fxcsnmsub  f12, A8, f11, f12

	fxcpnmsub  f0,  A8, f3,  f0
	fxcpnmsub  f8,  A8, f11, f8

	add	AO2, AO2, INCM4
	LFPDUX	A1,  AO,  INCM4
	LFPDUX	A2,  AO2, INCM4
	LFPDUX	A3,  AO,  INCM4

	add	AO2, AO2, INCM4
	LFPDUX	A4,  AO,  INCM4
	LFPDUX	A5,  AO2, INCM4
	LFPDUX	A6,  AO,  INCM4

	add	AO2, AO2, INCM4
	add	AO,  AO,  INCM4
	LFPDUX	A7,  AO2, INCM4
	LFPDUX	A8,  AO,  INCM4


	fxsmul	f6,  A1, f6
	fxsmul	f14, A1, f14

	fxcpnmsub  f2,  A1, f6,  f2
	fxcpnmsub  f10, A1, f14, f10

	fxcsnmsub  f5,  A2, f6,  f5
	fxcsnmsub  f13, A2, f14, f13

	fxcpnmsub  f1,  A2, f6,  f1
	fxcpnmsub  f9,  A2, f14, f9

	fxcsnmsub  f4,  A3, f6,  f4
	fxcsnmsub  f12, A3, f14, f12

	fxcpnmsub  f0,  A3, f6,  f0
	fxcpnmsub  f8,  A3, f14, f8

	fxpmul	f2,  A4, f2
	fxpmul	f10, A4, f10

	fxcsnmsub  f5,  A5, f2,  f5
	fxcsnmsub  f13, A5, f10, f13

	fxcpnmsub  f1,  A5, f2,  f1
	fxcpnmsub  f9,  A5, f10, f9

	fxcsnmsub  f4,  A6, f2,  f4
	fxcsnmsub  f12, A6, f10, f12

	fxcpnmsub  f0,  A6, f2,  f0
	fxcpnmsub  f8,  A6, f10, f8

	fxsmul	f5,  A7, f5
	fxsmul	f13, A7, f13

	fxcpnmsub  f1,  A7, f5,  f1
	fxcpnmsub  f9,  A7, f13, f9

	fxcsnmsub  f4,  A8, f5,  f4
	fxcsnmsub  f12, A8, f13, f12

	fxcpnmsub  f0,  A8, f5,  f0
	fxcpnmsub  f8,  A8, f13, f8

	add	AO2, AO2, INCM4
	add	AO,  AO,  INCM4
	LFPDUX	A1,  AO2, INCM4
	LFPDUX	A2,  AO,  INCM4

	subi	AO2, AO2, 8 * SIZE
	add	AO,  AO,  INCM4
	LFPDUX	A3,  AO,  INCM4

	subi	AO2, AO2, 8 * SIZE
	add	AO,  AO,  INCM4
	LFPDUX	A4,  AO,  INCM4

	addi	AO,  AO,  -4 * SIZE
	addi	AO2, AO2, -4 * SIZE

	fxpmul	f1,  A1, f1
	fxpmul	f9,  A1, f9

	fxcsnmsub  f4,  A2, f1,  f4
	fxcsnmsub  f12, A2, f9,  f12

	fxcpnmsub  f0,  A2, f1,  f0
	fxcpnmsub  f8,  A2, f9,  f8

	fxsmul	f4,  A3, f4
	fxsmul	f12, A3, f12

	fxcpnmsub  f0,  A3, f4,  f0
	fxcpnmsub  f8,  A3, f12, f8

	fxpmul	f0,  A4, f0
	fxpmul	f8,  A4, f8

#endif

#ifdef LT
	LFPDUX	A1,  AO,  INC4
	LFPDUX	A2,  AO2, INC4
	LFPDUX	A3,  AO,  INC4
	LFPDUX	A4,  AO2, INC4

	LFPDUX	A5,  AO,  INC4
	LFPDUX	A6,  AO2, INC4
	LFPDUX	A7,  AO,  INC4
	LFPDUX	A8,  AO2, INC4

	fxpmul	f0,  A1,  f0
	fxpmul	f8,  A1,  f8

	fxcsnmsub  f4,  A1, f0, f4
	fxcsnmsub  f12, A1, f8, f12

	fxcpnmsub  f1,  A2, f0, f1
	fxcpnmsub  f9,  A2, f8, f9

	fxcsnmsub  f5,  A2, f0, f5
	fxcsnmsub  f13, A2, f8, f13

	fxcpnmsub  f2,  A3, f0, f2
	fxcpnmsub  f10, A3, f8, f10

	fxcsnmsub  f6,  A3, f0, f6
	fxcsnmsub  f14, A3, f8, f14

	fxcpnmsub  f3,  A4, f0, f3
	fxcpnmsub  f11, A4, f8, f11

	fxcsnmsub  f7,  A4, f0, f7
	fxcsnmsub  f15, A4, f8, f15

	fxsmul	f4,  A5,  f4
	fxsmul	f12, A5,  f12

	fxcpnmsub  f1,  A6, f4,  f1
	fxcpnmsub  f9,  A6, f12, f9

	fxcsnmsub  f5,  A6, f4,  f5
	fxcsnmsub  f13, A6, f12, f13

	fxcpnmsub  f2,  A7, f4,  f2
	fxcpnmsub  f10, A7, f12, f10

	fxcsnmsub  f6,  A7, f4,  f6
	fxcsnmsub  f14, A7, f12, f14

	fxcpnmsub  f3,  A8, f4,  f3
	fxcpnmsub  f11, A8, f12, f11

	fxcsnmsub  f7,  A8, f4,  f7
	fxcsnmsub  f15, A8, f12, f15

	add	AO,  AO,  INC4
	LFPDUX	A1,  AO2, INC4
	LFPDUX	A2,  AO,  INC4
	LFPDUX	A3,  AO2, INC4

	add	AO,  AO,  INC4
	LFPDUX	A4,  AO2, INC4
	LFPDUX	A5,  AO,  INC4
	LFPDUX	A6,  AO2, INC4

	add	AO,  AO,  INC4
	add	AO2, AO2, INC4
	LFPDUX	A7,  AO,  INC4
	LFPDUX	A8,  AO2, INC4

	fxpmul	f1,  A1,  f1
	fxpmul	f9,  A1,  f9

	fxcsnmsub  f5,  A1, f1, f5
	fxcsnmsub  f13, A1, f9, f13

	fxcpnmsub  f2,  A2, f1, f2
	fxcpnmsub  f10, A2, f9, f10

	fxcsnmsub  f6,  A2, f1, f6
	fxcsnmsub  f14, A2, f9, f14

	fxcpnmsub  f3,  A3, f1, f3
	fxcpnmsub  f11, A3, f9, f11

	fxcsnmsub  f7,  A3, f1, f7
	fxcsnmsub  f15, A3, f9, f15

	fxsmul	f5,  A4,  f5
	fxsmul	f13, A4,  f13

	fxcpnmsub  f2,  A5, f5,  f2
	fxcpnmsub  f10, A5, f13, f10

	fxcsnmsub  f6,  A5, f5,  f6
	fxcsnmsub  f14, A5, f13, f14

	fxcpnmsub  f3,  A6, f5,  f3
	fxcpnmsub  f11, A6, f13, f11

	fxcsnmsub  f7,  A6, f5,  f7
	fxcsnmsub  f15, A6, f13, f15

	fxpmul	f2,  A7,  f2
	fxpmul	f10, A7,  f10

	fxcsnmsub  f6,  A7, f2,  f6
	fxcsnmsub  f14, A7, f10, f14

	fxcpnmsub  f3,  A8, f2,  f3
	fxcpnmsub  f11, A8, f10, f11

	fxcsnmsub  f7,  A8, f2,  f7
	fxcsnmsub  f15, A8, f10, f15

	add	AO,  AO,  INC4
	add	AO2, AO2, INC4
	LFPDUX	A1,  AO,  INC4
	LFPDUX	A2,  AO2, INC4

	addi	AO,  AO,  8 * SIZE
	addi	AO2, AO2, 4 * SIZE
	LFPDUX	A3,  AO2, INC4

	addi	AO,  AO,  8 * SIZE
	addi	AO2, AO2, 4 * SIZE
	LFPDUX	A4,  AO2, INC4

	subi	AO,  AO,  64 * SIZE
	subi	AO2, AO2, 64 * SIZE

	fxsmul	f6,  A1,  f6
	fxsmul	f14, A1,  f14

	fxcpnmsub  f3,  A2, f6,  f3
	fxcpnmsub  f11, A2, f14, f11

	fxcsnmsub  f7,  A2, f6,  f7
	fxcsnmsub  f15, A2, f14, f15

	fxpmul	f3,  A3,  f3
	fxpmul	f11, A3,  f11

	fxcsnmsub  f7,  A3, f3,  f7
	fxcsnmsub  f15, A3, f11, f15

	fxsmul	f7,  A4,  f7
	fxsmul	f15, A4,  f15
#endif

#ifdef RN
	LFPDUX	A1,  BO,  INC4
	LFPDUX	A2,  BO2, INC4
	LFPDUX	A3,  BO,  INC4
	LFPDUX	A4,  BO2, INC4

	add	BO,  BO,  INC4
	LFPDUX	A5,  BO2, INC4

	add	BO,  BO,  INC4
	LFPDUX	A6,  BO2, INC4
	subi	BO,  BO,  16 * SIZE
	subi	BO2, BO2, 16 * SIZE

	fxpmul	f0,  A1,  f0
	fxpmul	f1,  A1,  f1
	fxpmul	f2,  A1,  f2
	fxpmul	f3,  A1,  f3

	fxcsnmsub    f4,  A1, f0, f4
	fxcsnmsub    f5,  A1, f1, f5
	fxcsnmsub    f6,  A1, f2, f6
	fxcsnmsub    f7,  A1, f3, f7

	fxcpnmsub    f8,  A2, f0, f8
	fxcpnmsub    f9,  A2, f1, f9
	fxcpnmsub    f10, A2, f2, f10
	fxcpnmsub    f11, A2, f3, f11

	fxcsnmsub    f12, A2, f0, f12
	fxcsnmsub    f13, A2, f1, f13
	fxcsnmsub    f14, A2, f2, f14
	fxcsnmsub    f15, A2, f3, f15

	fxsmul	f4,  A3,  f4
	fxsmul	f5,  A3,  f5
	fxsmul	f6,  A3,  f6
	fxsmul	f7,  A3,  f7

	fxcpnmsub    f8,  A4, f4, f8
	fxcpnmsub    f9,  A4, f5, f9
	fxcpnmsub    f10, A4, f6, f10
	fxcpnmsub    f11, A4, f7, f11

	fxcsnmsub    f12, A4, f4, f12
	fxcsnmsub    f13, A4, f5, f13
	fxcsnmsub    f14, A4, f6, f14
	fxcsnmsub    f15, A4, f7, f15

	fxpmul	f8,  A5,  f8
	fxpmul	f9,  A5,  f9
	fxpmul	f10, A5,  f10
	fxpmul	f11, A5,  f11

	fxcsnmsub    f12, A5, f8,  f12
	fxcsnmsub    f13, A5, f9,  f13
	fxcsnmsub    f14, A5, f10, f14
	fxcsnmsub    f15, A5, f11, f15

	fxsmul	f12,  A6,  f12
	fxsmul	f13,  A6,  f13
	fxsmul	f14,  A6,  f14
	fxsmul	f15,  A6,  f15

#endif

#ifdef RT
	addi	BO,  BO,  20 * SIZE
	addi	BO2, BO2, 20 * SIZE

	LFPDUX	A1,  BO2, INCM4
	LFPDUX	A2,  BO,  INCM4

	LFPDUX	A3,  BO2, INCM4
	LFPDUX	A4,  BO,  INCM4

	add	BO2, BO2, INCM4
	LFPDUX	A5,  BO,  INCM4

	add	BO2, BO2, INCM4
	LFPDUX	A6,  BO,  INCM4
	subi	BO,  BO,  4 * SIZE
	subi	BO2, BO2, 4 * SIZE

	fxsmul	f12, A1,  f12
	fxsmul	f13, A1,  f13
	fxsmul	f14, A1,  f14
	fxsmul	f15, A1,  f15

	fxcpnmsub    f8,  A1, f12, f8
	fxcpnmsub    f9,  A1, f13, f9
	fxcpnmsub    f10, A1, f14, f10
	fxcpnmsub    f11, A1, f15, f11

	fxcsnmsub    f4,  A2, f12, f4
	fxcsnmsub    f5,  A2, f13, f5
	fxcsnmsub    f6,  A2, f14, f6
	fxcsnmsub    f7,  A2, f15, f7

	fxcpnmsub    f0,  A2, f12, f0
	fxcpnmsub    f1,  A2, f13, f1
	fxcpnmsub    f2,  A2, f14, f2
	fxcpnmsub    f3,  A2, f15, f3

	fxpmul	f8,  A3,  f8
	fxpmul	f9,  A3,  f9
	fxpmul	f10, A3,  f10
	fxpmul	f11, A3,  f11

	fxcsnmsub    f4,  A4, f8,  f4
	fxcsnmsub    f5,  A4, f9,  f5
	fxcsnmsub    f6,  A4, f10, f6
	fxcsnmsub    f7,  A4, f11, f7

	fxcpnmsub    f0,  A4, f8,  f0
	fxcpnmsub    f1,  A4, f9,  f1
	fxcpnmsub    f2,  A4, f10, f2
	fxcpnmsub    f3,  A4, f11, f3

	fxsmul	f4,  A5,  f4
	fxsmul	f5,  A5,  f5
	fxsmul	f6,  A5,  f6
	fxsmul	f7,  A5,  f7

	fxcpnmsub    f0,  A5, f4,  f0
	fxcpnmsub    f1,  A5, f5,  f1
	fxcpnmsub    f2,  A5, f6,  f2
	fxcpnmsub    f3,  A5, f7,  f3

	fxpmul	f0,  A6,  f0
	fxpmul	f1,  A6,  f1
	fxpmul	f2,  A6,  f2
	fxpmul	f3,  A6,  f3

#endif

#ifdef LN
	subi	CO1, CO1, 8 * SIZE
	subi	CO2, CO2, 8 * SIZE
	subi	CO3, CO3, 8 * SIZE
	subi	CO4, CO4, 8 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFPDUX	f0,  BO,  INC4
	STFPDUX	f8,  BO2, INC4
	STFPDUX	f4,  BO,  INC4
	STFPDUX	f12, BO2, INC4
	STFPDUX	f1,  BO,  INC4
	STFPDUX	f9,  BO2, INC4
	STFPDUX	f5,  BO,  INC4
	STFPDUX	f13, BO2, INC4
	STFPDUX	f2,  BO,  INC4
	STFPDUX	f10, BO2, INC4
	STFPDUX	f6,  BO,  INC4
	STFPDUX	f14, BO2, INC4
	STFPDUX	f3,  BO,  INC4
	STFPDUX	f11, BO2, INC4
	STFPDUX	f7,  BO,  INC4
	STFPDUX	f15, BO2, INC4

	subi	BO,  BO,  32 * SIZE
	subi	BO2, BO2, 32 * SIZE

	STFDUX	f0,  CO1, INC
	STFDUX	f4,  CO1, INC
	STFDUX	f1,  CO1, INC
	STFDUX	f5,  CO1, INC
	STFDUX	f2,  CO1, INC
	STFDUX	f6,  CO1, INC
	STFDUX	f3,  CO1, INC
	STFDUX	f7,  CO1, INC

	STFSDUX	f0,  CO2, INC
	STFSDUX	f4,  CO2, INC
	STFSDUX	f1,  CO2, INC
	STFSDUX	f5,  CO2, INC
	STFSDUX	f2,  CO2, INC
	STFSDUX	f6,  CO2, INC
	STFSDUX	f3,  CO2, INC
	STFSDUX	f7,  CO2, INC

	STFDUX	f8,  CO3, INC
	STFDUX	f12, CO3, INC
	STFDUX	f9,  CO3, INC
	STFDUX	f13, CO3, INC
	STFDUX	f10, CO3, INC
	STFDUX	f14, CO3, INC
	STFDUX	f11, CO3, INC
	STFDUX	f15, CO3, INC

	STFSDUX	f8,  CO4, INC
	STFSDUX	f12, CO4, INC
	STFSDUX	f9,  CO4, INC
	STFSDUX	f13, CO4, INC
	STFSDUX	f10, CO4, INC
	STFSDUX	f14, CO4, INC
	STFSDUX	f11, CO4, INC
	STFSDUX	f15, CO4, INC

#else
	STFPDUX	f0,  AO,  INC4
	STFPDUX	f1,  AO2, INC4
	STFPDUX	f2,  AO,  INC4
	STFPDUX	f3,  AO2, INC4
	STFPDUX	f4,  AO,  INC4
	STFPDUX	f5,  AO2, INC4
	STFPDUX	f6,  AO,  INC4
	STFPDUX	f7,  AO2, INC4
	STFPDUX	f8,  AO,  INC4
	STFPDUX	f9,  AO2, INC4
	STFPDUX	f10, AO,  INC4
	STFPDUX	f11, AO2, INC4
	STFPDUX	f12, AO,  INC4
	STFPDUX	f13, AO2, INC4
	STFPDUX	f14, AO,  INC4
	STFPDUX	f15, AO2, INC4

	subi	AO,  AO,  32 * SIZE
	subi	AO2, AO2, 32 * SIZE

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC
	STFDUX	f2,  CO1, INC
	STFSDUX	f2,  CO1, INC
	STFDUX	f3,  CO1, INC
	STFSDUX	f3,  CO1, INC

	STFDUX	f4,  CO2, INC
	STFSDUX	f4,  CO2, INC
	STFDUX	f5,  CO2, INC
	STFSDUX	f5,  CO2, INC
	STFDUX	f6,  CO2, INC
	STFSDUX	f6,  CO2, INC
	STFDUX	f7,  CO2, INC
	STFSDUX	f7,  CO2, INC

	STFDUX	f8,  CO3, INC
	STFSDUX	f8,  CO3, INC
	STFDUX	f9,  CO3, INC
	STFSDUX	f9,  CO3, INC
	STFDUX	f10, CO3, INC
	STFSDUX	f10, CO3, INC
	STFDUX	f11, CO3, INC
	STFSDUX	f11, CO3, INC

	STFDUX	f12, CO4, INC
	STFSDUX	f12, CO4, INC
	STFDUX	f13, CO4, INC
	STFSDUX	f13, CO4, INC
	STFDUX	f14, CO4, INC
	STFSDUX	f14, CO4, INC
	STFDUX	f15, CO4, INC
	STFSDUX	f15, CO4, INC
#endif

#ifdef LN
	subi	CO1, CO1, 8 * SIZE
	subi	CO2, CO2, 8 * SIZE
	subi	CO3, CO3, 8 * SIZE
	subi	CO4, CO4, 8 * SIZE
#endif

#ifdef RT
	slwi	r0, K, 3 + BASE_SHIFT
	add	AORIG, AORIG, r0
#endif

#if defined(LT) || defined(RN)
	sub	TEMP, K, KK
	slwi	r0,   TEMP, 3 + BASE_SHIFT
	slwi	TEMP, TEMP, 2 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LT
	addi	KK, KK, 8
#endif

#ifdef LN
	subi	KK, KK, 8
#endif

	addic.	I, I, -1
	li	r0, FZERO

	lfpsx	f0, SP, r0
	bgt+	.L11
	.align 4

.L20:
	andi.	I, M,  4
	beq	.L30

#if defined(LT) || defined(RN)
	addi	AO2, AO,   2 * SIZE
	fpmr	f4,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f12, f0

	srawi.	r0,  KK,  2
 	fpmr	f1,  f0
	fpmr	f5,  f0
	fpmr	f9,  f0
	mtspr	CTR, r0
	fpmr	f13, f0
	ble	.L24
#else

#ifdef LN
	slwi	r0,   K,  2 + BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0  , KK, 2 + BASE_SHIFT
	slwi	TEMP, KK, 2 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

	addi	AO2, AO,   2 * SIZE
	fpmr	f4,  f0
	addi	BO,  BO,  - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, BO,    2 * SIZE
	fpmr	f12, f0

	srawi.	r0,  TEMP,  2
 	fpmr	f1,  f0
	fpmr	f5,  f0
	fpmr	f9,  f0
	mtspr	CTR, r0
	fpmr	f13, f0
	ble	.L24
#endif

	LFPDUX	A1,   AO, INC4
	LFPDUX	B1,   BO, INC4
	LFPDUX	A2,  AO2, INC4
	LFPDUX	B2,  BO2, INC4
	LFPDUX	A3,   AO, INC4
	LFPDUX	B3,   BO, INC4
	LFPDUX	A4,  AO2, INC4
	LFPDUX	B4,  BO2, INC4

	LFPDUX	A5,   AO, INC4
	LFPDUX	B5,   BO, INC4
	LFPDUX	A6,  AO2, INC4
	LFPDUX	B6,  BO2, INC4
	LFPDUX	A7,   AO, INC4
	LFPDUX	A9,   BO, INC4
	LFPDUX	A10, BO2, INC4
	bdz-	.L23
	.align 4

.L22:
	fxcpmadd	f0,  B1, A1, f0
	nop
	fxcsmadd	f4,  B1, A1, f4
	LFPDUX	A8,  AO2, INC4
	fxcpmadd	f8,  B2, A1, f8
	nop
	fxcsmadd	f12, B2, A1, f12
	LFPDUX	A1,   AO, INC4

	fxcpmadd	f1,  B1, A2, f1
	nop
	fxcsmadd	f5,  B1, A2, f5
	LFPDUX	B1,   BO, INC4
	fxcpmadd	f9,  B2, A2, f9
	nop
	fxcsmadd	f13, B2, A2, f13
	LFPDUX	B2,  BO2, INC4

	fxcpmadd	f0,  B3, A3, f0
	nop
	fxcsmadd	f4,  B3, A3, f4
	LFPDUX	A2,  AO2, INC4
	fxcpmadd	f8,  B4, A3, f8
	nop
	fxcsmadd	f12, B4, A3, f12
	LFPDUX	A3,   AO, INC4

	fxcpmadd	f1,  B3, A4, f1
	nop
	fxcsmadd	f5,  B3, A4, f5
	LFPDUX	B3,   BO, INC4
	fxcpmadd	f9,  B4, A4, f9
	nop
	fxcsmadd	f13, B4, A4, f13
	LFPDUX	B4,  BO2, INC4

	fxcpmadd	f0,  B5, A5, f0
	nop
	fxcsmadd	f4,  B5, A5, f4
	LFPDUX	A4,  AO2, INC4
	fxcpmadd	f8,  B6, A5, f8
	nop
	fxcsmadd	f12, B6, A5, f12
	LFPDUX	A5,   AO, INC4

	fxcpmadd	f1,  B5, A6, f1
	nop
	fxcsmadd	f5,  B5, A6, f5
	LFPDUX	B5,   BO, INC4
	fxcpmadd	f9,  B6, A6, f9
	nop
	fxcsmadd	f13, B6, A6, f13
	LFPDUX	B6,  BO2, INC4

	fxcpmadd	f0,  A9,  A7, f0
	nop
	fxcsmadd	f4,  A9,  A7, f4
	LFPDUX	A6,  AO2, INC4
	fxcpmadd	f8,  A10, A7, f8
	nop
	fxcsmadd	f12, A10, A7, f12
	LFPDUX	A7,   AO, INC4

	fxcpmadd	f1,  A9,  A8, f1
	nop
	fxcsmadd	f5,  A9,  A8, f5
	LFPDUX	A9,   BO, INC4
	fxcpmadd	f9,  A10, A8, f9
	nop
	fxcsmadd	f13, A10, A8, f13
	LFPDUX	A10, BO2, INC4
	bdnz+	.L22
	.align 4

.L23:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f4,  B1, A1, f4
	LFPDUX	A8,  AO2, INC4
	fxcpmadd	f8,  B2, A1, f8
	fxcsmadd	f12, B2, A1, f12

	fxcpmadd	f1,  B1, A2, f1
	fxcsmadd	f5,  B1, A2, f5
	fxcpmadd	f9,  B2, A2, f9
	fxcsmadd	f13, B2, A2, f13

	fxcpmadd	f0,  B3, A3, f0
	fxcsmadd	f4,  B3, A3, f4
	fxcpmadd	f8,  B4, A3, f8
	fxcsmadd	f12, B4, A3, f12

	fxcpmadd	f1,  B3, A4, f1
	fxcsmadd	f5,  B3, A4, f5
	fxcpmadd	f9,  B4, A4, f9
	fxcsmadd	f13, B4, A4, f13

	fxcpmadd	f0,  B5, A5, f0
	fxcsmadd	f4,  B5, A5, f4
	fxcpmadd	f8,  B6, A5, f8
	fxcsmadd	f12, B6, A5, f12

	fxcpmadd	f1,  B5, A6, f1
	fxcsmadd	f5,  B5, A6, f5
	fxcpmadd	f9,  B6, A6, f9
	fxcsmadd	f13, B6, A6, f13

	fxcpmadd	f0,  A9, A7, f0
	fxcsmadd	f4,  A9, A7, f4
	fxcpmadd	f8,  A10, A7, f8
	fxcsmadd	f12, A10, A7, f12

	fxcpmadd	f1,  A9, A8, f1
	fxcsmadd	f5,  A9, A8, f5
	fxcpmadd	f9,  A10, A8, f9
	fxcsmadd	f13, A10, A8, f13
	.align 4

.L24:
#if defined(LT) || defined(RN)
	andi.	r0,  KK,  3
	mtspr	CTR, r0
	ble+	.L28
#else
	andi.	r0, TEMP, 3
	mtspr	CTR, r0
	ble+	.L28
#endif

	LFPDUX	A1,  AO,  INC4
	LFPDUX	A2,  AO2, INC4
	LFPDUX	B1,  BO,  INC4
	LFPDUX	B2,  BO2, INC4
	bdz-	.L27
	.align 4

.L26:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f4,  B1, A1, f4
	fxcpmadd	f8,  B2, A1, f8
	fxcsmadd	f12, B2, A1, f12
	LFPDUX	A1,  AO,  INC4

	fxcpmadd	f1,  B1, A2, f1
	fxcsmadd	f5,  B1, A2, f5
	LFPDUX	B1,  BO,  INC4
	fxcpmadd	f9,  B2, A2, f9
	fxcsmadd	f13, B2, A2, f13
	LFPDUX	A2,  AO2, INC4
	LFPDUX	B2,  BO2, INC4
	bdnz+	.L26
	.align 4

.L27:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f4,  B1, A1, f4
	fxcpmadd	f8,  B2, A1, f8
	fxcsmadd	f12, B2, A1, f12

	fxcpmadd	f1,  B1, A2, f1
	fxcsmadd	f5,  B1, A2, f5
	fxcpmadd	f9,  B2, A2, f9
	fxcsmadd	f13, B2, A2, f13
	.align 4

.L28:
#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 4
#else
	subi	r0, KK, 4
#endif
	slwi	TEMP, r0, 2 + BASE_SHIFT
	slwi	r0,   r0, 2 + BASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO, B,     r0
	addi	AO2, AO,   2 * SIZE
	addi	BO,  BO, - 4 * SIZE
	addi	BO2, BO,   2 * SIZE
#endif

#if defined(LN) || defined(LT)
	fpmr	f24, f0
	fpmr	f25, f1
	fpmr	f28, f8
	fpmr	f29, f9

	fsmfp	f0,  f4
	fsmfp	f1,  f5
	fsmfp	f8,  f12
	fsmfp	f9,  f13

	fsmtp	f4,  f24
	fsmtp	f5,  f25
	fsmtp	f12, f28
	fsmtp	f13, f29

	LFPDUX	f16, BO,  INC4
	LFPDUX	f17, BO2, INC4
	LFPDUX	f18, BO,  INC4
	LFPDUX	f19, BO2, INC4

 	LFPDUX	f20, BO,  INC4
	LFPDUX	f21, BO2, INC4
	LFPDUX	f22, BO,  INC4
	LFPDUX	f23, BO2, INC4

	subi	BO,  BO,  16 * SIZE
	subi	BO2, BO2, 16 * SIZE

	fpsub	f0,  f16,  f0
	fpsub	f8,  f17,  f8
	fpsub	f4,  f18,  f4
	fpsub	f12, f19,  f12

	fpsub	f1,  f20,  f1
	fpsub	f9,  f21,  f9
	fpsub	f5,  f22,  f5
	fpsub	f13, f23,  f13
#else
	LFPDUX	f16, AO,  INC4
	LFPDUX	f17, AO2, INC4
	LFPDUX	f18, AO,  INC4
	LFPDUX	f19, AO2, INC4
	LFPDUX	f20, AO,  INC4
	LFPDUX	f21, AO2, INC4
	LFPDUX	f22, AO,  INC4
	LFPDUX	f23, AO2, INC4

	subi	AO,  AO,  16 * SIZE
	subi	AO2, AO2, 16 * SIZE

	fpsub	f0,  f16,  f0
	fpsub	f1,  f17,  f1
	fpsub	f4,  f18,  f4
	fpsub	f5,  f19,  f5

	fpsub	f8,  f20,  f8
	fpsub	f9,  f21,  f9
	fpsub	f12, f22,  f12
	fpsub	f13, f23,  f13
#endif

#ifdef LN
       addi	AO,  AO,  20 * SIZE
       addi	AO2, AO2, 20 * SIZE

	LFPDUX	A1,  AO2, INCM4
	LFPDUX	A2,  AO,  INCM4
	LFPDUX	A3,  AO2, INCM4
	LFPDUX	A4,  AO,  INCM4

	add	AO2, AO2, INCM4
	LFPDUX	A5,  AO,  INCM4
	add	AO2, AO2, INCM4
	LFPDUX	A6,  AO,  INCM4

	addi	AO,  AO,  -4 * SIZE
	addi	AO2, AO2, -4 * SIZE

	fxsmul	f5,  A1, f5
	fxsmul	f13, A1, f13

	fxcpnmsub  f1,  A1, f5,  f1
	fxcpnmsub  f9,  A1, f13, f9

	fxcsnmsub  f4,  A2, f5,  f4
	fxcsnmsub  f12, A2, f13, f12

	fxcpnmsub  f0,  A2, f5,  f0
	fxcpnmsub  f8,  A2, f13, f8

	fxpmul	f1,  A3, f1
	fxpmul	f9,  A3, f9

	fxcsnmsub  f4,  A4, f1,  f4
	fxcsnmsub  f12, A4, f9,  f12

	fxcpnmsub  f0,  A4, f1,  f0
	fxcpnmsub  f8,  A4, f9,  f8

	fxsmul	f4,  A5, f4
	fxsmul	f12, A5, f12

	fxcpnmsub  f0,  A5, f4,  f0
	fxcpnmsub  f8,  A5, f12, f8

	fxpmul	f0,  A6, f0
	fxpmul	f8,  A6, f8
#endif

#ifdef LT
	LFPDUX	A1,  AO,  INC4
	LFPDUX	A2,  AO2, INC4
	LFPDUX	A3,  AO,  INC4
	LFPDUX	A4,  AO2, INC4

	add	AO,  AO,  INC4
	LFPDUX	A5,  AO2, INC4
	add	AO,  AO,  INC4
	LFPDUX	A6,  AO2, INC4

	subi	AO,  AO,  16 * SIZE
	subi	AO2, AO2, 16 * SIZE

	fxpmul	f0,  A1,  f0
	fxpmul	f8,  A1,  f8

	fxcsnmsub  f4,  A1, f0, f4
	fxcsnmsub  f12, A1, f8, f12

	fxcpnmsub  f1,  A2, f0, f1
	fxcpnmsub  f9,  A2, f8, f9

	fxcsnmsub  f5,  A2, f0, f5
	fxcsnmsub  f13, A2, f8, f13

	fxsmul	f4,  A3,  f4
	fxsmul	f12, A3,  f12

	fxcpnmsub  f1,  A4, f4,  f1
	fxcpnmsub  f9,  A4, f12, f9

	fxcsnmsub  f5,  A4, f4,  f5
	fxcsnmsub  f13, A4, f12, f13

	fxpmul	f1,  A5,  f1
	fxpmul	f9,  A5,  f9

	fxcsnmsub  f5,  A5, f1, f5
	fxcsnmsub  f13, A5, f9, f13

	fxsmul	f5,  A6,  f5
	fxsmul	f13, A6,  f13
#endif

#ifdef RN
	LFPDUX	A1,  BO,  INC4
	LFPDUX	A2,  BO2, INC4
	LFPDUX	A3,  BO,  INC4
	LFPDUX	A4,  BO2, INC4

	add	BO,  BO,  INC4
	LFPDUX	A5,  BO2, INC4

	add	BO,  BO,  INC4
	LFPDUX	A6,  BO2, INC4

	subi	BO,  BO,  16 * SIZE
	subi	BO2, BO2, 16 * SIZE

	fxpmul	f0,  A1,  f0
	fxpmul	f1,  A1,  f1
	fxcsnmsub    f4,  A1, f0, f4
	fxcsnmsub    f5,  A1, f1, f5

	fxcpnmsub    f8,  A2, f0, f8
	fxcpnmsub    f9,  A2, f1, f9
	fxcsnmsub    f12, A2, f0, f12
	fxcsnmsub    f13, A2, f1, f13

	fxsmul	f4,  A3,  f4
	fxsmul	f5,  A3,  f5
	fxcpnmsub    f8,  A4, f4, f8
	fxcpnmsub    f9,  A4, f5, f9

	fxcsnmsub    f12, A4, f4, f12
	fxcsnmsub    f13, A4, f5, f13

	fxpmul	f8,  A5,  f8
	fxpmul	f9,  A5,  f9
	fxcsnmsub    f12, A5, f8,  f12
	fxcsnmsub    f13, A5, f9,  f13

	fxsmul	f12,  A6,  f12
	fxsmul	f13,  A6,  f13
#endif

#ifdef RT
	addi	BO,  BO,  20 * SIZE
	addi	BO2, BO2, 20 * SIZE

	LFPDUX	A1,  BO2, INCM4
	LFPDUX	A2,  BO,  INCM4

	LFPDUX	A3,  BO2, INCM4
	LFPDUX	A4,  BO,  INCM4

	add	BO2, BO2, INCM4
	LFPDUX	A5,  BO,  INCM4

	add	BO2, BO2, INCM4
	LFPDUX	A6,  BO,  INCM4
	subi	BO,  BO,  4 * SIZE
	subi	BO2, BO2, 4 * SIZE

	fxsmul	f12, A1,  f12
	fxsmul	f13, A1,  f13
	fxcpnmsub    f8,  A1, f12, f8
	fxcpnmsub    f9,  A1, f13, f9

	fxcsnmsub    f4,  A2, f12, f4
	fxcsnmsub    f5,  A2, f13, f5
	fxcpnmsub    f0,  A2, f12, f0
	fxcpnmsub    f1,  A2, f13, f1

	fxpmul	f8,  A3,  f8
	fxpmul	f9,  A3,  f9
	fxcsnmsub    f4,  A4, f8,  f4
	fxcsnmsub    f5,  A4, f9,  f5

	fxcpnmsub    f0,  A4, f8,  f0
	fxcpnmsub    f1,  A4, f9,  f1

	fxsmul	f4,  A5,  f4
	fxsmul	f5,  A5,  f5
	fxcpnmsub    f0,  A5, f4,  f0
	fxcpnmsub    f1,  A5, f5,  f1

	fxpmul	f0,  A6,  f0
	fxpmul	f1,  A6,  f1
#endif

#ifdef LN
	subi	CO1, CO1, 4 * SIZE
	subi	CO2, CO2, 4 * SIZE
	subi	CO3, CO3, 4 * SIZE
	subi	CO4, CO4, 4 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFPDUX	f0,  BO,  INC4
	STFPDUX	f8,  BO2, INC4
	STFPDUX	f4,  BO,  INC4
	STFPDUX	f12, BO2, INC4
	STFPDUX	f1,  BO,  INC4
	STFPDUX	f9,  BO2, INC4
	STFPDUX	f5,  BO,  INC4
	STFPDUX	f13, BO2, INC4

	subi	BO,  BO,  16 * SIZE
	subi	BO2, BO2, 16 * SIZE

	STFDUX	f0,  CO1, INC
	STFDUX	f4,  CO1, INC
	STFDUX	f1,  CO1, INC
	STFDUX	f5,  CO1, INC

	STFSDUX	f0,  CO2, INC
	STFSDUX	f4,  CO2, INC
	STFSDUX	f1,  CO2, INC
	STFSDUX	f5,  CO2, INC

	STFDUX	f8,  CO3, INC
	STFDUX	f12, CO3, INC
	STFDUX	f9,  CO3, INC
	STFDUX	f13, CO3, INC

	STFSDUX	f8,  CO4, INC
	STFSDUX	f12, CO4, INC
	STFSDUX	f9,  CO4, INC
	STFSDUX	f13, CO4, INC
#else
	STFPDUX	f0,  AO,  INC4
	STFPDUX	f1,  AO2, INC4
	STFPDUX	f4,  AO,  INC4
	STFPDUX	f5,  AO2, INC4
	STFPDUX	f8,  AO,  INC4
	STFPDUX	f9,  AO2, INC4
	STFPDUX	f12, AO,  INC4
	STFPDUX	f13, AO2, INC4

	subi	AO,  AO,  16 * SIZE
	subi	AO2, AO2, 16 * SIZE

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC
	STFDUX	f4,  CO2, INC
	STFSDUX	f4,  CO2, INC
	STFDUX	f5,  CO2, INC
	STFSDUX	f5,  CO2, INC

	STFDUX	f8,  CO3, INC
	STFSDUX	f8,  CO3, INC
	STFDUX	f9,  CO3, INC
	STFSDUX	f9,  CO3, INC
	STFDUX	f12, CO4, INC
	STFSDUX	f12, CO4, INC
	STFDUX	f13, CO4, INC
	STFSDUX	f13, CO4, INC
#endif

#ifdef LN
	subi	CO1, CO1, 4 * SIZE
	subi	CO2, CO2, 4 * SIZE
	subi	CO3, CO3, 4 * SIZE
	subi	CO4, CO4, 4 * SIZE
#endif

#ifdef RT
	slwi	r0, K, 2 + BASE_SHIFT
	add	AORIG, AORIG, r0
#endif

#if defined(LT) || defined(RN)
	sub	TEMP, K, KK
	slwi	r0,   TEMP, 2 + BASE_SHIFT
	slwi	TEMP, TEMP, 2 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LT
	addi	KK, KK, 4
#endif

#ifdef LN
	subi	KK, KK, 4
#endif

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L30:
	andi.	I, M,  2
	beq	.L40

#if defined(LT) || defined(RN)
	addi	AO2, AO,   2 * SIZE
	fpmr	f4,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f12, f0

	srawi.	r0,  KK,  2
	mtspr	CTR, r0
	ble	.L34
#else

#ifdef LN
	slwi	r0,   K,  1 + BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0  , KK, 1 + BASE_SHIFT
	slwi	TEMP, KK, 2 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

	addi	AO2, AO,    2 * SIZE
	fpmr	f4,  f0
	addi	BO,  BO,  - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, BO,    2 * SIZE
	fpmr	f12, f0

	srawi.	r0,  TEMP,  2
	mtspr	CTR, r0
	ble	.L34
#endif

	LFPDUX	A1,  AO, INC4
	LFPDUX	B1,  BO, INC4
	LFPDUX	B2, BO2, INC4
	LFPDUX	A2, AO2, INC4
	LFPDUX	B3,  BO, INC4
	LFPDUX	B4, BO2, INC4

	LFPDUX	A3,  AO, INC4
	LFPDUX	A5,  BO, INC4
	LFPDUX	A6, BO2, INC4
	LFPDUX	A4, AO2, INC4
	LFPDUX	A7,  BO, INC4
	LFPDUX	A8, BO2, INC4
	bdz-	.L33
	.align 4

.L32:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f4,  B1, A1, f4
	LFPDUX	B1,  BO, INC4
	fxcpmadd	f8,  B2, A1, f8
	fxcsmadd	f12, B2, A1, f12
	LFPDUX	B2, BO2, INC4
	LFPDUX	A1,  AO, INC4

	fxcpmadd	f0,  B3, A2, f0
	fxcsmadd	f4,  B3, A2, f4
	LFPDUX	B3,  BO, INC4
	fxcpmadd	f8,  B4, A2, f8
	fxcsmadd	f12, B4, A2, f12
	LFPDUX	B4, BO2, INC4
	LFPDUX	A2, AO2, INC4

	fxcpmadd	f0,  A5, A3, f0
	fxcsmadd	f4,  A5, A3, f4
	LFPDUX	A5,  BO, INC4
	fxcpmadd	f8,  A6, A3, f8
	fxcsmadd	f12, A6, A3, f12
	LFPDUX	A6, BO2, INC4
	LFPDUX	A3,  AO, INC4

	fxcpmadd	f0,  A7, A4, f0
	fxcsmadd	f4,  A7, A4, f4
	LFPDUX	A7,  BO, INC4
	fxcpmadd	f8,  A8, A4, f8
	fxcsmadd	f12, A8, A4, f12
	LFPDUX	A8, BO2, INC4
	LFPDUX	A4, AO2, INC4
	bdnz+	.L32
	.align 4

.L33:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f4,  B1, A1, f4
	fxcpmadd	f8,  B2, A1, f8
	fxcsmadd	f12, B2, A1, f12

	fxcpmadd	f0,  B3, A2, f0
	fxcsmadd	f4,  B3, A2, f4
	fxcpmadd	f8,  B4, A2, f8
	fxcsmadd	f12, B4, A2, f12

	fxcpmadd	f0,  A5, A3, f0
	fxcsmadd	f4,  A5, A3, f4
	fxcpmadd	f8,  A6, A3, f8
	fxcsmadd	f12, A6, A3, f12

	fxcpmadd	f0,  A7, A4, f0
	fxcsmadd	f4,  A7, A4, f4
	fxcpmadd	f8,  A8, A4, f8
	fxcsmadd	f12, A8, A4, f12
	.align 4

.L34:
#if defined(LT) || defined(RN)
	andi.	r0,  KK,  3
	mtspr	CTR, r0
	ble+	.L38
#else
	andi.	r0, TEMP, 3
	mtspr	CTR, r0
	ble+	.L38
#endif

	LFPDX	A1,  AO,  INC4
	LFPDUX	B1,  BO,  INC4
	LFPDUX	B2,  BO2, INC4
	add	AO, AO, INC2
	bdz-	.L37
	.align 4

.L36:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f4,  B1, A1, f4
	LFPDUX	B1,  BO,  INC4
	fxcpmadd	f8,  B2, A1, f8
	fxcsmadd	f12, B2, A1, f12
	LFPDX	A1,  AO,  INC4
	LFPDUX	B2,  BO2, INC4
	add	AO, AO, INC2
	bdnz+	.L36
	.align 4

.L37:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f4,  B1, A1, f4
	fxcpmadd	f8,  B2, A1, f8
	fxcsmadd	f12, B2, A1, f12
	.align 4

.L38:
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
	addi	AO2, AO,   2 * SIZE
	addi	BO,  BO, - 4 * SIZE
	addi	BO2, BO,   2 * SIZE
#endif

#if defined(LN) || defined(LT)
	fpmr	f24, f0
	fpmr	f28, f8

	fsmfp	f0,  f4
	fsmfp	f8,  f12
	fsmtp	f4,  f24
	fsmtp	f12, f28

	LFPDUX	f16, BO,  INC4
	LFPDUX	f17, BO2, INC4
	LFPDUX	f18, BO,  INC4
	LFPDUX	f19, BO2, INC4

	subi	BO,  BO,   8 * SIZE
	subi	BO2, BO2,  8 * SIZE

	fpsub	f0,  f16,  f0
	fpsub	f8,  f17,  f8
	fpsub	f4,  f18,  f4
	fpsub	f12, f19,  f12
#else
	LFPDUX	f16, AO,  INC4
	LFPDUX	f17, AO2, INC4
	LFPDUX	f18, AO,  INC4
	LFPDUX	f19, AO2, INC4

	subi	AO,  AO,   8 * SIZE
	subi	AO2, AO2,  8 * SIZE

	fpsub	f0,  f16,  f0
	fpsub	f4,  f17,  f4
	fpsub	f8,  f18,  f8
	fpsub	f12, f19,  f12
#endif

#ifdef LN
	addi	AO,  AO,   8 * SIZE
	addi	AO2, AO2,  8 * SIZE

	LFPDUX	A1,  AO2, INCM4
	LFPDUX	A2,  AO,  INCM4

	addi	AO,  AO,  -4 * SIZE
	addi	AO2, AO2, -4 * SIZE

	fxsmul	f4,  A1, f4
	fxsmul	f12, A1, f12

	fxcpnmsub  f0,  A1, f4,  f0
	fxcpnmsub  f8,  A1, f12, f8

	fxpmul	f0,  A2, f0
	fxpmul	f8,  A2, f8
#endif

#ifdef LT
	LFPDUX	A1,  AO,  INC4
	LFPDUX	A2,  AO2, INC4

	subi	AO,  AO,   4 * SIZE
	subi	AO2, AO2,  4 * SIZE

	fxpmul	f0,  A1,  f0
	fxpmul	f8,  A1,  f8

	fxcsnmsub  f4,  A1, f0, f4
	fxcsnmsub  f12, A1, f8, f12

	fxsmul	f4,  A2,  f4
	fxsmul	f12, A2,  f12
#endif

#ifdef RN
	LFPDUX	A1,  BO,  INC4
	LFPDUX	A2,  BO2, INC4
	LFPDUX	A3,  BO,  INC4
	LFPDUX	A4,  BO2, INC4

	add	BO,  BO,  INC4
	LFPDUX	A5,  BO2, INC4

	add	BO,  BO,  INC4
	LFPDUX	A6,  BO2, INC4

	subi	BO,  BO,  16 * SIZE
	subi	BO2, BO2, 16 * SIZE

	fxpmul	     f0,  A1,  f0
	fxcsnmsub    f4,  A1, f0, f4
	fxcpnmsub    f8,  A2, f0, f8
	fxcsnmsub    f12, A2, f0, f12

	fxsmul	     f4,  A3,  f4
	fxcpnmsub    f8,  A4, f4, f8
	fxcsnmsub    f12, A4, f4, f12

	fxpmul	     f8,  A5,  f8
	fxcsnmsub    f12, A5, f8,  f12
	fxsmul	     f12, A6,  f12
#endif

#ifdef RT
	addi	BO,  BO,  20 * SIZE
	addi	BO2, BO2, 20 * SIZE

	LFPDUX	A1,  BO2, INCM4
	LFPDUX	A2,  BO,  INCM4

	LFPDUX	A3,  BO2, INCM4
	LFPDUX	A4,  BO,  INCM4

	add	BO2, BO2, INCM4
	LFPDUX	A5,  BO,  INCM4

	add	BO2, BO2, INCM4
	LFPDUX	A6,  BO,  INCM4
	subi	BO,  BO,  4 * SIZE
	subi	BO2, BO2, 4 * SIZE

	fxsmul	     f12, A1,  f12
	fxcpnmsub    f8,  A1, f12, f8
	fxcsnmsub    f4,  A2, f12, f4
	fxcpnmsub    f0,  A2, f12, f0

	fxpmul	     f8,  A3,  f8
	fxcsnmsub    f4,  A4, f8,  f4
	fxcpnmsub    f0,  A4, f8,  f0

	fxsmul	     f4,  A5,  f4
	fxcpnmsub    f0,  A5, f4,  f0
	fxpmul	     f0,  A6,  f0
#endif

#ifdef LN
	subi	CO1, CO1, 2 * SIZE
	subi	CO2, CO2, 2 * SIZE
	subi	CO3, CO3, 2 * SIZE
	subi	CO4, CO4, 2 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFPDUX	f0,  BO,  INC4
	STFPDUX	f8,  BO2, INC4
	STFPDUX	f4,  BO,  INC4
	STFPDUX	f12, BO2, INC4

	subi	BO,  BO,   8 * SIZE
	subi	BO2, BO2,  8 * SIZE

	STFDUX	f0,  CO1, INC
	STFDUX	f4,  CO1, INC
	STFSDUX	f0,  CO2, INC
	STFSDUX	f4,  CO2, INC

	STFDUX	f8,  CO3, INC
	STFDUX	f12, CO3, INC
	STFSDUX	f8,  CO4, INC
	STFSDUX	f12, CO4, INC

#else
	STFPDUX	f0,  AO,  INC4
	STFPDUX	f4,  AO2, INC4
	STFPDUX	f8,  AO,  INC4
	STFPDUX	f12, AO2, INC4

	subi	AO,  AO,   8 * SIZE
	subi	AO2, AO2,  8 * SIZE

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
	STFDUX	f4,  CO2, INC
	STFSDUX	f4,  CO2, INC

	STFDUX	f8,  CO3, INC
	STFSDUX	f8,  CO3, INC
	STFDUX	f12, CO4, INC
	STFSDUX	f12, CO4, INC
#endif

#ifdef LN
	subi	CO1, CO1, 2 * SIZE
	subi	CO2, CO2, 2 * SIZE
	subi	CO3, CO3, 2 * SIZE
	subi	CO4, CO4, 2 * SIZE
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

#ifdef LT
	addi	KK, KK, 2
#endif

#ifdef LN
	subi	KK, KK, 2
#endif

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L40:
	andi.	I, M,  1
	beq	.L49

#if defined(LT) || defined(RN)
	addi	AO2, AO,   2 * SIZE
	fpmr	f1,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f2,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f3,  f0

	srawi.	r0,  KK,  3
	mtspr	CTR, r0
	ble	.L44
#else

#ifdef LN
	slwi	r0,   K,  0 + BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0  , KK, 0 + BASE_SHIFT
	slwi	TEMP, KK, 2 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

	addi	AO2, AO,   2 * SIZE
	fpmr	f1,  f0
	addi	BO,  BO, - 4 * SIZE
	fpmr	f2,  f0
	addi	BO2, BO,   2 * SIZE
	fpmr	f3,  f0

	srawi.	r0,  TEMP,  3
	mtspr	CTR, r0
	ble	.L44
#endif

	LFPDUX	A1,  AO,  INC4
	LFPDUX	B1,  BO,  INC4
	LFPDUX	B2,  BO2, INC4
	LFPDUX	A2, AO2,  INC4
	LFPDUX	B3,  BO,  INC4
	LFPDUX	B4,  BO2, INC4

	LFPDUX	A3,  AO,  INC4
	LFPDUX	A5,  BO,  INC4
	LFPDUX	A6,  BO2, INC4
	LFPDUX	A4, AO2,  INC4
	LFPDUX	A7,  BO,  INC4
	LFPDUX	A8,  BO2, INC4
	bdz-	.L43
	.align 4

.L42:
	fxcpmadd	f0,  A1, B1, f0
	LFPDUX	B1,  BO,  INC4
	fxcpmadd	f1,  A1, B2, f1
	LFPDUX	B2,  BO2, INC4
	fxcsmadd	f2,  A1, B3, f2
	LFPDUX	B3,  BO,  INC4
	fxcsmadd	f3,  A1, B4, f3
	LFPDUX	B4,  BO2, INC4
	LFPDUX	A1,  AO,  INC4

	fxcpmadd	f0,  A2, A5, f0
	LFPDUX	A5,  BO,  INC4
	fxcpmadd	f1,  A2, A6, f1
	LFPDUX	A6,  BO2, INC4
	fxcsmadd	f2,  A2, A7, f2
	LFPDUX	A7,  BO,  INC4
	fxcsmadd	f3,  A2, A8, f3
	LFPDUX	A8,  BO2, INC4
	LFPDUX	A2, AO2,  INC4

	fxcpmadd	f0,  A3, B1, f0
	LFPDUX	B1,  BO,  INC4
	fxcpmadd	f1,  A3, B2, f1
	LFPDUX	B2,  BO2, INC4
	fxcsmadd	f2,  A3, B3, f2
	LFPDUX	B3,  BO,  INC4
	fxcsmadd	f3,  A3, B4, f3
	LFPDUX	B4,  BO2, INC4
	LFPDUX	A3,  AO,  INC4

	fxcpmadd	f0,  A4, A5, f0
	LFPDUX	A5,  BO,  INC4
	fxcpmadd	f1,  A4, A6, f1
	LFPDUX	A6,  BO2, INC4
	fxcsmadd	f2,  A4, A7, f2
	LFPDUX	A7,  BO,  INC4
	fxcsmadd	f3,  A4, A8, f3
	LFPDUX	A8,  BO2, INC4
	LFPDUX	A4, AO2,  INC4
	bdnz+	.L42
	.align 4

.L43:
	fxcpmadd	f0,  A1, B1, f0
	LFPDUX	B1,  BO,  INC4
	fxcpmadd	f1,  A1, B2, f1
	LFPDUX	B2,  BO2, INC4
	fxcsmadd	f2,  A1, B3, f2
	LFPDUX	B3,  BO,  INC4
	fxcsmadd	f3,  A1, B4, f3
	LFPDUX	B4,  BO2, INC4

	fxcpmadd	f0,  A2, A5, f0
	LFPDUX	A5,  BO,  INC4
	fxcpmadd	f1,  A2, A6, f1
	LFPDUX	A6,  BO2, INC4
	fxcsmadd	f2,  A2, A7, f2
	LFPDUX	A7,  BO,  INC4
	fxcsmadd	f3,  A2, A8, f3
	LFPDUX	A8,  BO2, INC4

	fxcpmadd	f0,  A3, B1, f0
	fxcpmadd	f1,  A3, B2, f1
	fxcsmadd	f2,  A3, B3, f2
	fxcsmadd	f3,  A3, B4, f3

	fxcpmadd	f0,  A4, A5, f0
	fxcpmadd	f1,  A4, A6, f1
	fxcsmadd	f2,  A4, A7, f2
	fxcsmadd	f3,  A4, A8, f3
	.align 4

.L44:
#if defined(LT) || defined(RN)
	andi.	r0,  KK,  7
	mtspr	CTR, r0
	ble+	.L48
#else
	andi.	r0, TEMP, 7
	mtspr	CTR, r0
	ble+	.L48
#endif

	LFDX	A1,  AO,  INC4
	LFPDUX	B1,  BO,  INC4
	LFPDUX	B2,  BO2, INC4
	add	AO, AO, INC
	bdz-	.L47
	.align 4

.L46:
	fxcpmadd	f0,  A1, B1, f0
	LFPDUX	B1,  BO,  INC4
	fxcpmadd	f1,  A1, B2, f1
	LFDX	A1,  AO,  INC4
	LFPDUX	B2,  BO2, INC4
	add	AO, AO, INC
	bdnz+	.L46
	.align 4

.L47:
	fxcpmadd	f0,  A1, B1, f0
	fxcpmadd	f1,  A1, B2, f1
	addi	AO2, AO,   2 * SIZE
	.align 4

.L48:
	fpadd	f0, f0, f2
	fpadd	f1, f1, f3

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
	addi	AO2, AO,   2 * SIZE
	addi	BO,  BO, - 4 * SIZE
	addi	BO2, BO,   2 * SIZE
#endif

#if defined(LN) || defined(LT)
	LFPDX	f16, BO,  INC4
	LFPDX	f17, BO2, INC4

	fpsub	f0,  f16,  f0
	fpsub	f1,  f17,  f1
#else
	LFPDX	f16, AO,  INC4
	LFPDX	f17, AO2, INC4

	fpsub	f0,  f16,  f0
	fpsub	f1,  f17,  f1
#endif

#if defined(LN) || defined(LT)
	LFPDX	A1,  AO, INC4

	fxpmul	f0,  A1, f0
	fxpmul	f1,  A1, f1
#endif

#ifdef RN
	LFD	A1,  (4 +  0) * SIZE(BO)
	LFD	A2,  (4 +  1) * SIZE(BO)
	LFD	A3,  (4 +  2) * SIZE(BO)
	LFD	A4,  (4 +  3) * SIZE(BO)

	LFD	A5,  (4 +  5) * SIZE(BO)
	LFD	A6,  (4 +  6) * SIZE(BO)
	LFD	A7,  (4 +  7) * SIZE(BO)
	LFD	A8,  (4 + 10) * SIZE(BO)

	LFD	A9,  (4 + 11) * SIZE(BO)
	LFD	A10, (4 + 15) * SIZE(BO)

	fsmtp	     f2, f0
	fsmtp	     f3, f1

	fmul	     f0,  A1, f0
	fnmsub	     f2,  A2, f0, f2
	fnmsub	     f1,  A3, f0, f1
	fnmsub	     f3,  A4, f0, f3

	fmul	     f2,  A5, f2
	fnmsub	     f1,  A6, f2, f1
	fnmsub	     f3,  A7, f2, f3

	fmul	     f1,  A8, f1
	fnmsub	     f3,  A9, f1, f3

	fmul	     f3,  A10, f3

	fsmfp	     f0, f2
	fsmfp	     f1, f3
#endif

#ifdef RT
	LFD	A1,  (4 + 15) * SIZE(BO)
	LFD	A2,  (4 + 14) * SIZE(BO)
	LFD	A3,  (4 + 13) * SIZE(BO)
	LFD	A4,  (4 + 12) * SIZE(BO)

	LFD	A5,  (4 + 10) * SIZE(BO)
	LFD	A6,  (4 +  9) * SIZE(BO)
	LFD	A7,  (4 +  8) * SIZE(BO)
	LFD	A8,  (4 +  5) * SIZE(BO)

	LFD	A9,  (4 +  4) * SIZE(BO)
	LFD	A10, (4 +  0) * SIZE(BO)

	fsmtp	     f2, f0
	fsmtp	     f3, f1

	fmul	     f3,  A1, f3
	fnmsub	     f1,  A2, f3, f1
	fnmsub	     f2,  A3, f3, f2
	fnmsub	     f0,  A4, f3, f0

	fmul	     f1,  A5, f1
	fnmsub	     f2,  A6, f1, f2
	fnmsub	     f0,  A7, f1, f0

	fmul	     f2,  A8, f2
	fnmsub	     f0,  A9, f2, f0

	fmul	     f0,  A10, f0

	fsmfp	     f0, f2
	fsmfp	     f1, f3
#endif

#if defined(LN) || defined(LT)
	STFPDX	f0,  BO,  INC4
	STFPDX	f1,  BO2, INC4
#else
	STFPDX	f0,  AO,  INC4
	STFPDX	f1,  AO2, INC4
#endif

#ifdef LN
	subi	CO1, CO1, 1 * SIZE
	subi	CO2, CO2, 1 * SIZE
	subi	CO3, CO3, 1 * SIZE
	subi	CO4, CO4, 1 * SIZE
#endif

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO2, INC
	STFDUX	f1,  CO3, INC
	STFSDUX	f1,  CO4, INC

#ifdef LN
	subi	CO1, CO1, 1 * SIZE
	subi	CO2, CO2, 1 * SIZE
	subi	CO3, CO3, 1 * SIZE
	subi	CO4, CO4, 1 * SIZE
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

#ifdef LT
	addi	KK, KK, 1
#endif

#ifdef LN
	subi	KK, KK, 1
#endif
	.align 4

.L49:
#ifdef LN
	slwi	r0, K, 2 + BASE_SHIFT
	add	B, B, r0
#endif

#if defined(LT) || defined(RN)
	addi	B,  BO, 4 * SIZE
#endif

#ifdef RN
	addi	KK, KK, 4
#endif

#ifdef RT
	subi	KK, KK, 4
#endif

	addic.	J, J, -1
	bgt+	.L10
	.align 4

.L50:
	andi.	J, N,  2
	beq	.L90

#ifdef RT
	slwi	r0, K, 1 + BASE_SHIFT
	sub	B, B, r0

	slwi	r0, LDC, 1
	sub	C, C, r0
#endif

	mr	CO1, C
	add	CO2, C,   LDC

#ifdef LN
	add	KK, M, OFFSET
#endif

#ifdef LT
	mr	KK, OFFSET
#endif


#if defined(LN) || defined(RT)
	addi	AORIG, A, -2 * SIZE
#else
	addi	AO, A, -2 * SIZE
#endif
#ifndef RT
	add	C,  CO2, LDC
#endif

	li	r0, FZERO
	lfpsx	f0, SP, r0

	srawi.	I, M,  3
	ble	.L60
	.align 4

.L51:
#if defined(LT) || defined(RN)
	fpmr	f4,  f0
	addi	BO,   B,  - 2 * SIZE
 	fpmr	f1,  f0
	fpmr	f5,  f0
	fpmr	f2,  f0
	fpmr	f6,  f0

	srawi.	r0,  KK,  2
	fpmr	f3,  f0
	mtspr	CTR, r0
	fpmr	f7,  f0
	ble	.L54
#else

#ifdef LN
	slwi	r0,   K,  3 + BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0  , KK, 3 + BASE_SHIFT
	slwi	TEMP, KK, 1 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

	fpmr	f4,  f0
	addi	BO,  BO,  - 2 * SIZE
 	fpmr	f1,  f0
	fpmr	f5,  f0
	fpmr	f2,  f0
	fpmr	f6,  f0

	srawi.	r0,  TEMP,  2
	fpmr	f3,  f0
	mtspr	CTR, r0
	fpmr	f7,  f0
	ble	.L54
#endif

	LFPDUX	B1,  BO,  INC2
	LFPDUX	A1,  AO,  INC2
	LFPDUX	A2,  AO,  INC2
	LFPDUX	B2,  BO,  INC2
	LFPDUX	A3,  AO,  INC2
	LFPDUX	A4,  AO,  INC2

	LFPDUX	B3,  BO,  INC2
	LFPDUX	A5,  AO,  INC2
	LFPDUX	A6,  AO,  INC2
	LFPDUX	A7,  AO,  INC2
	LFPDUX	A8,  AO,  INC2
	bdz-	.L53
	.align 4

.L52:
	fxcpmadd	f0,  B1, A1, f0
	LFPDUX	B4,  BO,  INC2
	fxcsmadd	f4,  B1, A1, f4
	LFPDUX	A1,  AO,  INC2
	fxcpmadd	f1,  B1, A2, f1
	nop
	fxcsmadd	f5,  B1, A2, f5
	LFPDUX	A2,  AO,  INC2

	fxcpmadd	f2,  B1, A3, f2
	nop
	fxcsmadd	f6,  B1, A3, f6
	LFPDUX	A3,  AO,  INC2
	fxcpmadd	f3,  B1, A4, f3
	nop
	fxcsmadd	f7,  B1, A4, f7
	LFPDUX	A4,  AO,  INC2

	fxcpmadd	f0,  B2, A5, f0
	LFPDUX	B1,  BO,  INC2
	fxcsmadd	f4,  B2, A5, f4
	LFPDUX	A5,  AO,  INC2
	fxcpmadd	f1,  B2, A6, f1
	nop
	fxcsmadd	f5,  B2, A6, f5
	LFPDUX	A6,  AO,  INC2

	fxcpmadd	f2,  B2, A7, f2
	nop
	fxcsmadd	f6,  B2, A7, f6
	LFPDUX	A7,  AO,  INC2
	fxcpmadd	f3,  B2, A8, f3
	nop
	fxcsmadd	f7,  B2, A8, f7
	LFPDUX	A8,  AO,  INC2

	fxcpmadd	f0,  B3, A1, f0
	LFPDUX	B2,  BO,  INC2
	fxcsmadd	f4,  B3, A1, f4
	LFPDUX	A1,  AO,  INC2
	fxcpmadd	f1,  B3, A2, f1
	nop
	fxcsmadd	f5,  B3, A2, f5
	LFPDUX	A2,  AO,  INC2

	fxcpmadd	f2,  B3, A3, f2
	nop
	fxcsmadd	f6,  B3, A3, f6
	LFPDUX	A3,  AO,  INC2
	fxcpmadd	f3,  B3, A4, f3
	nop
	fxcsmadd	f7,  B3, A4, f7
	LFPDUX	A4,  AO,  INC2

	fxcpmadd	f0,  B4, A5, f0
	LFPDUX	B3,  BO,  INC2
	fxcsmadd	f4,  B4, A5, f4
	LFPDUX	A5,  AO,  INC2
	fxcpmadd	f1,  B4, A6, f1
	nop
	fxcsmadd	f5,  B4, A6, f5
	LFPDUX	A6,  AO,  INC2

	fxcpmadd	f2,  B4, A7, f2
	nop
	fxcsmadd	f6,  B4, A7, f6
	LFPDUX	A7,  AO,  INC2
	fxcpmadd	f3,  B4, A8, f3
	nop
	fxcsmadd	f7,  B4, A8, f7
	LFPDUX	A8,  AO,  INC2
	bdnz+	.L52
	.align 4

.L53:
	fxcpmadd	f0,  B1, A1, f0
	LFPDUX	B4,  BO,  INC2
	fxcsmadd	f4,  B1, A1, f4
	LFPDUX	A1,  AO,  INC2
	fxcpmadd	f1,  B1, A2, f1
	nop
	fxcsmadd	f5,  B1, A2, f5
	LFPDUX	A2,  AO,  INC2

	fxcpmadd	f2,  B1, A3, f2
	nop
	fxcsmadd	f6,  B1, A3, f6
	LFPDUX	A3,  AO,  INC2
	fxcpmadd	f3,  B1, A4, f3
	nop
	fxcsmadd	f7,  B1, A4, f7
	LFPDUX	A4,  AO,  INC2

	fxcpmadd	f0,  B2, A5, f0
	nop
	fxcsmadd	f4,  B2, A5, f4
	LFPDUX	A5,  AO,  INC2
	fxcpmadd	f1,  B2, A6, f1
	nop
	fxcsmadd	f5,  B2, A6, f5
	LFPDUX	A6,  AO,  INC2

	fxcpmadd	f2,  B2, A7, f2
	nop
	fxcsmadd	f6,  B2, A7, f6
	LFPDUX	A7,  AO,  INC2
	fxcpmadd	f3,  B2, A8, f3
	nop
	fxcsmadd	f7,  B2, A8, f7
	LFPDUX	A8,  AO,  INC2

	fxcpmadd	f0,  B3, A1, f0
	fxcsmadd	f4,  B3, A1, f4
	fxcpmadd	f1,  B3, A2, f1
	fxcsmadd	f5,  B3, A2, f5

	fxcpmadd	f2,  B3, A3, f2
	fxcsmadd	f6,  B3, A3, f6
	fxcpmadd	f3,  B3, A4, f3
	fxcsmadd	f7,  B3, A4, f7

	fxcpmadd	f0,  B4, A5, f0
	fxcsmadd	f4,  B4, A5, f4
	fxcpmadd	f1,  B4, A6, f1
	fxcsmadd	f5,  B4, A6, f5

	fxcpmadd	f2,  B4, A7, f2
	fxcsmadd	f6,  B4, A7, f6
	fxcpmadd	f3,  B4, A8, f3
	fxcsmadd	f7,  B4, A8, f7
	.align 4

.L54:
#if defined(LT) || defined(RN)
	andi.	r0,  KK,  3
	mtspr	CTR, r0
	ble+	.L58
#else
	andi.	r0, TEMP, 3
	mtspr	CTR, r0
	ble+	.L58
#endif

	LFPDUX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	LFPDUX	A2,  AO,  INC2
	LFPDUX	A3,  AO,  INC2
	LFPDUX	A4,  AO,  INC2
	bdz-	.L57
	.align 4

.L56:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f4,  B1, A1, f4
	LFPDUX	A1,  AO,  INC2
	fxcpmadd	f1,  B1, A2, f1
	fxcsmadd	f5,  B1, A2, f5
	LFPDUX	A2,  AO,  INC2

	fxcpmadd	f2,  B1, A3, f2
	fxcsmadd	f6,  B1, A3, f6
	LFPDUX	A3,  AO,  INC2
	fxcpmadd	f3,  B1, A4, f3
	fxcsmadd	f7,  B1, A4, f7
	LFPDUX	A4,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	bdnz+	.L56
	.align 4

.L57:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f4,  B1, A1, f4
	fxcpmadd	f1,  B1, A2, f1
	fxcsmadd	f5,  B1, A2, f5

	fxcpmadd	f2,  B1, A3, f2
	fxcsmadd	f6,  B1, A3, f6
	fxcpmadd	f3,  B1, A4, f3
	fxcsmadd	f7,  B1, A4, f7
	.align 4

.L58:
#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 8
#else
	subi	r0, KK, 2
#endif
	slwi	TEMP, r0, 3 + BASE_SHIFT
	slwi	r0,   r0, 1 + BASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO,  B,     r0
	addi	BO,  BO, - 2 * SIZE
#endif

#if defined(LN) || defined(LT)
	fpmr	f24, f0
	fpmr	f25, f1
	fpmr	f26, f2
	fpmr	f27, f3

	fsmfp	f0,  f4
	fsmfp	f1,  f5
	fsmfp	f2,  f6
	fsmfp	f3,  f7

	fsmtp	f4,  f24
	fsmtp	f5,  f25
	fsmtp	f6,  f26
	fsmtp	f7,  f27

	LFPDUX	f16, BO,  INC2
	LFPDUX	f17, BO,  INC2
	LFPDUX	f18, BO,  INC2
	LFPDUX	f19, BO,  INC2

 	LFPDUX	f20, BO,  INC2
	LFPDUX	f21, BO,  INC2
	LFPDUX	f22, BO,  INC2
	LFPDUX	f23, BO,  INC2

	subi	BO,  BO,  16 * SIZE

	fpsub	f0,  f16,  f0
	fpsub	f4,  f17,  f4
	fpsub	f1,  f18,  f1
	fpsub	f5,  f19,  f5

	fpsub	f2,  f20,  f2
	fpsub	f6,  f21,  f6
	fpsub	f3,  f22,  f3
	fpsub	f7,  f23,  f7

#else
	LFPDUX	f16, AO,  INC2
	LFPDUX	f17, AO,  INC2
	LFPDUX	f18, AO,  INC2
	LFPDUX	f19, AO,  INC2

	LFPDUX	f20, AO,  INC2
	LFPDUX	f21, AO,  INC2
	LFPDUX	f22, AO,  INC2
	LFPDUX	f23, AO,  INC2

	subi	AO,  AO,  16 * SIZE

	fpsub	f0,  f16,  f0
	fpsub	f1,  f17,  f1
	fpsub	f2,  f18,  f2
	fpsub	f3,  f19,  f3
	fpsub	f4,  f20,  f4
	fpsub	f5,  f21,  f5
	fpsub	f6,  f22,  f6
	fpsub	f7,  f23,  f7
#endif

#ifdef LN
       addi	AO,  AO,  66 * SIZE

	LFPDUX	A1,  AO,  INCM2
	LFPDUX	A2,  AO,  INCM2
	LFPDUX	A3,  AO,  INCM2
	LFPDUX	A4,  AO,  INCM2
	LFPDUX	A5,  AO,  INCM2
	LFPDUX	A6,  AO,  INCM2
	LFPDUX	A7,  AO,  INCM2
	LFPDUX	A8,  AO,  INCM2

	fxsmul	   f7,  A1, f7
	fxcpnmsub  f3,  A1, f7,  f3
	fxcsnmsub  f6,  A2, f7,  f6
	fxcpnmsub  f2,  A2, f7,  f2

	fxcsnmsub  f5,  A3, f7,  f5
	fxcpnmsub  f1,  A3, f7,  f1
	fxcsnmsub  f4,  A4, f7,  f4
	fxcpnmsub  f0,  A4, f7,  f0

	fxpmul	   f3,  A5, f3
	fxcsnmsub  f6,  A6, f3,  f6
	fxcpnmsub  f2,  A6, f3,  f2

	fxcsnmsub  f5,  A7, f3,  f5
	fxcpnmsub  f1,  A7, f3,  f1
	fxcsnmsub  f4,  A8, f3,  f4
	fxcpnmsub  f0,  A8, f3,  f0

	add	AO,  AO,  INCM2
	LFPDUX	A1,  AO,  INCM2
	LFPDUX	A2,  AO,  INCM2
	LFPDUX	A3,  AO,  INCM2

	add	AO,  AO,  INCM2
	LFPDUX	A4,  AO,  INCM2
	LFPDUX	A5,  AO,  INCM2
	LFPDUX	A6,  AO,  INCM2

	add	AO,  AO,  INCM2
	add	AO,  AO,  INCM2
	LFPDUX	A7,  AO,  INCM2
	LFPDUX	A8,  AO,  INCM2

	fxsmul	   f6,  A1, f6
	fxcpnmsub  f2,  A1, f6,  f2
	fxcsnmsub  f5,  A2, f6,  f5
	fxcpnmsub  f1,  A2, f6,  f1
	fxcsnmsub  f4,  A3, f6,  f4
	fxcpnmsub  f0,  A3, f6,  f0

	fxpmul	   f2,  A4, f2
	fxcsnmsub  f5,  A5, f2,  f5
	fxcpnmsub  f1,  A5, f2,  f1
	fxcsnmsub  f4,  A6, f2,  f4
	fxcpnmsub  f0,  A6, f2,  f0

	fxsmul	   f5,  A7, f5
	fxcpnmsub  f1,  A7, f5,  f1
	fxcsnmsub  f4,  A8, f5,  f4
	fxcpnmsub  f0,  A8, f5,  f0

	add	AO,  AO,  INCM2
	add	AO,  AO,  INCM2
	LFPDUX	A1,  AO,  INCM2
	LFPDUX	A2,  AO,  INCM2

	subi	AO,  AO, 6 * SIZE
	LFPDUX	A3,  AO,  INCM2
	subi	AO,  AO, 6 * SIZE
	LFPDUX	A4,  AO,  INCM2

	addi	AO,  AO,  -2 * SIZE

	fxpmul	   f1,  A1, f1
	fxcsnmsub  f4,  A2, f1,  f4
	fxcpnmsub  f0,  A2, f1,  f0

	fxsmul	   f4,  A3, f4
	fxcpnmsub  f0,  A3, f4,  f0

	fxpmul	f0,  A4, f0
#endif

#ifdef LT
	LFPDUX	A1,  AO,  INC2
	LFPDUX	A2,  AO,  INC2
	LFPDUX	A3,  AO,  INC2
	LFPDUX	A4,  AO,  INC2

	LFPDUX	A5,  AO,  INC2
	LFPDUX	A6,  AO,  INC2
	LFPDUX	A7,  AO,  INC2
	LFPDUX	A8,  AO,  INC2

	fxpmul	   f0,  A1,  f0
	fxcsnmsub  f4,  A1, f0, f4
	fxcpnmsub  f1,  A2, f0, f1
	fxcsnmsub  f5,  A2, f0, f5
	fxcpnmsub  f2,  A3, f0, f2
	fxcsnmsub  f6,  A3, f0, f6
	fxcpnmsub  f3,  A4, f0, f3
	fxcsnmsub  f7,  A4, f0, f7

	fxsmul	   f4,  A5,  f4
	fxcpnmsub  f1,  A6, f4,  f1
	fxcsnmsub  f5,  A6, f4,  f5
	fxcpnmsub  f2,  A7, f4,  f2
	fxcsnmsub  f6,  A7, f4,  f6
	fxcpnmsub  f3,  A8, f4,  f3
	fxcsnmsub  f7,  A8, f4,  f7

	add	AO,  AO,  INC2
	LFPDUX	A1,  AO,  INC2
	LFPDUX	A2,  AO,  INC2
	LFPDUX	A3,  AO,  INC2

	add	AO,  AO,  INC2
	LFPDUX	A4,  AO,  INC2
	LFPDUX	A5,  AO,  INC2
	LFPDUX	A6,  AO,  INC2

	add	AO,  AO,  INC2
	add	AO,  AO,  INC2
	LFPDUX	A7,  AO,  INC2
	LFPDUX	A8,  AO,  INC2

	fxpmul	   f1,  A1,  f1
	fxcsnmsub  f5,  A1, f1, f5
	fxcpnmsub  f2,  A2, f1, f2
	fxcsnmsub  f6,  A2, f1, f6
	fxcpnmsub  f3,  A3, f1, f3
	fxcsnmsub  f7,  A3, f1, f7

	fxsmul	   f5,  A4,  f5
	fxcpnmsub  f2,  A5, f5,  f2
	fxcsnmsub  f6,  A5, f5,  f6
	fxcpnmsub  f3,  A6, f5,  f3
	fxcsnmsub  f7,  A6, f5,  f7

	fxpmul	   f2,  A7,  f2
	fxcsnmsub  f6,  A7, f2,  f6
	fxcpnmsub  f3,  A8, f2,  f3
	fxcsnmsub  f7,  A8, f2,  f7

	add	AO,  AO,  INC2
	add	AO,  AO,  INC2
	LFPDUX	A1,  AO,  INC2
	LFPDUX	A2,  AO,  INC2

	addi	AO,  AO,  6 * SIZE
	LFPDUX	A3,  AO,  INC2
	addi	AO,  AO,  6 * SIZE
	LFPDUX	A4,  AO,  INC2

	subi	AO,  AO,  64 * SIZE

	fxsmul	   f6,  A1,  f6
	fxcpnmsub  f3,  A2, f6,  f3
	fxcsnmsub  f7,  A2, f6,  f7

	fxpmul	   f3,  A3,  f3
	fxcsnmsub  f7,  A3, f3,  f7

	fxsmul	   f7,  A4,  f7
#endif

#ifdef RN
	LFPDUX	A1,  BO,  INC2
	LFPDUX	A2,  BO,  INC2

	subi	BO, BO, 4 * SIZE

	fxpmul	f0,  A1,  f0
	fxpmul	f1,  A1,  f1
	fxpmul	f2,  A1,  f2
	fxpmul	f3,  A1,  f3

	fxcsnmsub    f4,  A1, f0, f4
	fxcsnmsub    f5,  A1, f1, f5
	fxcsnmsub    f6,  A1, f2, f6
	fxcsnmsub    f7,  A1, f3, f7

	fxsmul	f4,  A2,  f4
	fxsmul	f5,  A2,  f5
	fxsmul	f6,  A2,  f6
	fxsmul	f7,  A2,  f7
#endif

#ifdef RT
	LFPDUX	A2,  BO,  INC2
	LFPDUX	A1,  BO,  INC2

	subi	BO, BO, 4 * SIZE

	fxsmul	f4,  A1,  f4
	fxsmul	f5,  A1,  f5
	fxsmul	f6,  A1,  f6
	fxsmul	f7,  A1,  f7

	fxcpnmsub    f0,  A1, f4,  f0
	fxcpnmsub    f1,  A1, f5,  f1
	fxcpnmsub    f2,  A1, f6,  f2
	fxcpnmsub    f3,  A1, f7,  f3

	fxpmul	f0,  A2,  f0
	fxpmul	f1,  A2,  f1
	fxpmul	f2,  A2,  f2
	fxpmul	f3,  A2,  f3

#endif

#ifdef LN
	subi	CO1, CO1, 8 * SIZE
	subi	CO2, CO2, 8 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFPDUX	f0,  BO,  INC2
	STFPDUX	f4,  BO,  INC2
	STFPDUX	f1,  BO,  INC2
	STFPDUX	f5,  BO,  INC2
	STFPDUX	f2,  BO,  INC2
	STFPDUX	f6,  BO,  INC2
	STFPDUX	f3,  BO,  INC2
	STFPDUX	f7,  BO,  INC2

	subi	BO,  BO,  16 * SIZE

	STFDUX	f0,  CO1, INC
	STFDUX	f4,  CO1, INC
	STFDUX	f1,  CO1, INC
	STFDUX	f5,  CO1, INC
	STFDUX	f2,  CO1, INC
	STFDUX	f6,  CO1, INC
	STFDUX	f3,  CO1, INC
	STFDUX	f7,  CO1, INC

	STFSDUX	f0,  CO2, INC
	STFSDUX	f4,  CO2, INC
	STFSDUX	f1,  CO2, INC
	STFSDUX	f5,  CO2, INC
	STFSDUX	f2,  CO2, INC
	STFSDUX	f6,  CO2, INC
	STFSDUX	f3,  CO2, INC
	STFSDUX	f7,  CO2, INC
#else
	STFPDUX	f0,  AO,  INC2
	STFPDUX	f1,  AO,  INC2
	STFPDUX	f2,  AO,  INC2
	STFPDUX	f3,  AO,  INC2
	STFPDUX	f4,  AO,  INC2
	STFPDUX	f5,  AO,  INC2
	STFPDUX	f6,  AO,  INC2
	STFPDUX	f7,  AO,  INC2

	subi	AO,  AO,  16 * SIZE

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC
	STFDUX	f2,  CO1, INC
	STFSDUX	f2,  CO1, INC
	STFDUX	f3,  CO1, INC
	STFSDUX	f3,  CO1, INC

	STFDUX	f4,  CO2, INC
	STFSDUX	f4,  CO2, INC
	STFDUX	f5,  CO2, INC
	STFSDUX	f5,  CO2, INC
	STFDUX	f6,  CO2, INC
	STFSDUX	f6,  CO2, INC
	STFDUX	f7,  CO2, INC
	STFSDUX	f7,  CO2, INC
#endif

#ifdef LN
	subi	CO1, CO1, 8 * SIZE
	subi	CO2, CO2, 8 * SIZE
#endif

#ifdef RT
	slwi	r0, K, 3 + BASE_SHIFT
	add	AORIG, AORIG, r0
#endif

#if defined(LT) || defined(RN)
	sub	TEMP, K, KK
	slwi	r0,   TEMP, 3 + BASE_SHIFT
	slwi	TEMP, TEMP, 1 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LT
	addi	KK, KK, 8
#endif

#ifdef LN
	subi	KK, KK, 8
#endif

	addic.	I, I, -1
	li	r0, FZERO

	lfpsx	f0, SP, r0
	bgt+	.L51
	.align 4

.L60:
	andi.	I, M,  4
	beq	.L70

#if defined(LT) || defined(RN)
 	fpmr	f1,  f0
	addi	BO,  B,  - 2 * SIZE
	fpmr	f2,  f0
	fpmr	f3,  f0

	srawi.	r0,  KK,  2
	mtspr	CTR, r0
	ble	.L64
#else

#ifdef LN
	slwi	r0,   K,  2 + BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0  , KK, 2 + BASE_SHIFT
	slwi	TEMP, KK, 1 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

 	fpmr	f1,  f0
	addi	BO,  B,  - 2 * SIZE
	fpmr	f2,  f0
	fpmr	f3,  f0

	srawi.	r0,  TEMP,  2
	mtspr	CTR, r0
	ble	.L64
#endif

	LFPDUX	B1,  BO, INC2
	LFPDUX	A1,  AO, INC2
	LFPDUX	A2,  AO, INC2
	LFPDUX	B2,  BO, INC2
	LFPDUX	A3,  AO, INC2
	LFPDUX	A4,  AO, INC2

	LFPDUX	B3,  BO, INC2
	LFPDUX	A5,  AO, INC2
	LFPDUX	A6,  AO, INC2
	LFPDUX	B4,  BO, INC2
	LFPDUX	A7,  AO, INC2
	LFPDUX	A8,  AO, INC2
	bdz-	.L63
	.align 4

.L62:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f2,  B1, A1, f2
	LFPDUX	A1,  AO, INC2
	fxcpmadd	f1,  B1, A2, f1
	fxcsmadd	f3,  B1, A2, f3
	LFPDUX	A2,  AO, INC2
	LFPDUX	B1,  BO, INC2

	fxcpmadd	f0,  B2, A3, f0
	fxcsmadd	f2,  B2, A3, f2
	LFPDUX	A3,  AO, INC2
	fxcpmadd	f1,  B2, A4, f1
	fxcsmadd	f3,  B2, A4, f3
	LFPDUX	A4,  AO, INC2
	LFPDUX	B2,  BO, INC2

	fxcpmadd	f0,  B3, A5, f0
	fxcsmadd	f2,  B3, A5, f2
	LFPDUX	A5,  AO, INC2
	fxcpmadd	f1,  B3, A6, f1
	fxcsmadd	f3,  B3, A6, f3
	LFPDUX	A6,  AO, INC2
	LFPDUX	B3,  BO, INC2

	fxcpmadd	f0,  B4, A7, f0
	fxcsmadd	f2,  B4, A7, f2
	LFPDUX	A7,  AO, INC2
	fxcpmadd	f1,  B4, A8, f1
	fxcsmadd	f3,  B4, A8, f3
	LFPDUX	A8,  AO, INC2
	LFPDUX	B4,  BO, INC2
	bdnz+	.L62
	.align 4

.L63:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f2,  B1, A1, f2
	fxcpmadd	f1,  B1, A2, f1
	fxcsmadd	f3,  B1, A2, f3

	fxcpmadd	f0,  B2, A3, f0
	fxcsmadd	f2,  B2, A3, f2
	fxcpmadd	f1,  B2, A4, f1
	fxcsmadd	f3,  B2, A4, f3

	fxcpmadd	f0,  B3, A5, f0
	fxcsmadd	f2,  B3, A5, f2
	fxcpmadd	f1,  B3, A6, f1
	fxcsmadd	f3,  B3, A6, f3

	fxcpmadd	f0,  B4, A7, f0
	fxcsmadd	f2,  B4, A7, f2
	fxcpmadd	f1,  B4, A8, f1
	fxcsmadd	f3,  B4, A8, f3
	.align 4

.L64:
#if defined(LT) || defined(RN)
	andi.	r0,  KK,  3
	mtspr	CTR, r0
	ble+	.L68
#else
	andi.	r0, TEMP, 3
	mtspr	CTR, r0
	ble+	.L68
#endif

	LFPDUX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	LFPDUX	A2,  AO,  INC2
	bdz-	.L67
	.align 4

.L66:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f2,  B1, A1, f2
	LFPDUX	A1,  AO,  INC2
	fxcpmadd	f1,  B1, A2, f1
	fxcsmadd	f3,  B1, A2, f3
	LFPDUX	B1,  BO,  INC2
	LFPDUX	A2,  AO,  INC2
	bdnz+	.L66
	.align 4

.L67:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f2,  B1, A1, f2
	fxcpmadd	f1,  B1, A2, f1
	fxcsmadd	f3,  B1, A2, f3
	.align 4

.L68:
#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 4
#else
	subi	r0, KK, 2
#endif
	slwi	TEMP, r0, 2 + BASE_SHIFT
	slwi	r0,   r0, 1 + BASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO,  B,     r0
	addi	BO,  BO, - 2 * SIZE
#endif

#if defined(LN) || defined(LT)
	fpmr	f24, f0
	fpmr	f25, f1

	fsmfp	f0,  f2
	fsmfp	f1,  f3
	fsmtp	f2,  f24
	fsmtp	f3,  f25

	LFPDUX	f16, BO,  INC2
	LFPDUX	f17, BO,  INC2
	LFPDUX	f18, BO,  INC2
	LFPDUX	f19, BO,  INC2

	subi	BO,  BO,   8 * SIZE

	fpsub	f0,  f16,  f0
	fpsub	f2,  f17,  f2
	fpsub	f1,  f18,  f1
	fpsub	f3,  f19,  f3
#else
	LFPDUX	f16, AO,  INC2
	LFPDUX	f17, AO,  INC2
	LFPDUX	f18, AO,  INC2
	LFPDUX	f19, AO,  INC2

	subi	AO,  AO,   8 * SIZE

	fpsub	f0,  f16,  f0
	fpsub	f1,  f17,  f1
	fpsub	f2,  f18,  f2
	fpsub	f3,  f19,  f3
#endif

#ifdef LN
	addi	AO,  AO,  18 * SIZE

	LFPDUX	A1,  AO,  INCM2
	LFPDUX	A2,  AO,  INCM2
	LFPDUX	A3,  AO,  INCM2
	LFPDUX	A4,  AO,  INCM2
	add	AO,  AO,  INCM2
	LFPDUX	A5,  AO,  INCM2
	add	AO,  AO,  INCM2
	LFPDUX	A6,  AO,  INCM2

	subi	AO,  AO,  2 * SIZE

	fxsmul	   f3,  A1, f3
	fxcpnmsub  f1,  A1, f3,  f1
	fxcsnmsub  f2,  A2, f3,  f2
	fxcpnmsub  f0,  A2, f3,  f0

	fxpmul	   f1,  A3, f1
	fxcsnmsub  f2,  A4, f1,  f2
	fxcpnmsub  f0,  A4, f1,  f0

	fxsmul	   f2,  A5, f2
	fxcpnmsub  f0,  A5, f2,  f0

	fxpmul	   f0,  A6, f0
#endif

#ifdef LT
	LFPDUX	A1,  AO,  INC2
	LFPDUX	A2,  AO,  INC2
	LFPDUX	A3,  AO,  INC2
	LFPDUX	A4,  AO,  INC2

	add	AO,  AO,  INC2
	LFPDUX	A5,  AO,  INC2
	add	AO,  AO,  INC2
	LFPDUX	A6,  AO,  INC2

	subi	AO,  AO,  16 * SIZE

	fxpmul	   f0,  A1,  f0
	fxcsnmsub  f2,  A1, f0, f2
	fxcpnmsub  f1,  A2, f0, f1
	fxcsnmsub  f3,  A2, f0, f3

	fxsmul	   f2,  A3,  f2
	fxcpnmsub  f1,  A4,  f2,  f1
	fxcsnmsub  f3,  A4,  f2,  f3

	fxpmul	   f1,  A5,  f1
	fxcsnmsub  f3,  A5,  f1, f3

	fxsmul	   f3,  A6,  f3
#endif

#ifdef RN
	LFPDUX	A1,  BO,  INC2
	LFPDUX	A2,  BO,  INC2

	subi	BO, BO, 4 * SIZE

	fxpmul	f0,  A1,  f0
	fxpmul	f1,  A1,  f1

	fxcsnmsub    f2,  A1, f0, f2
	fxcsnmsub    f3,  A1, f1, f3

	fxsmul	f2,  A2,  f2
	fxsmul	f3,  A2,  f3
#endif

#ifdef RT
	LFPDUX	A2,  BO,  INC2
	LFPDUX	A1,  BO,  INC2

	subi	BO, BO, 4 * SIZE

	fxsmul	f2,  A1,  f2
	fxsmul	f3,  A1,  f3

	fxcpnmsub    f0,  A1, f2,  f0
	fxcpnmsub    f1,  A1, f3,  f1

	fxpmul	f0,  A2,  f0
	fxpmul	f1,  A2,  f1
#endif

#ifdef LN
	subi	CO1, CO1, 4 * SIZE
	subi	CO2, CO2, 4 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFPDUX	f0,  BO,  INC2
	STFPDUX	f2,  BO,  INC2
	STFPDUX	f1,  BO,  INC2
	STFPDUX	f3,  BO,  INC2

	subi	BO,  BO,   8 * SIZE

	STFDUX	f0,  CO1, INC
	STFDUX	f2,  CO1, INC
	STFDUX	f1,  CO1, INC
	STFDUX	f3,  CO1, INC

	STFSDUX	f0,  CO2, INC
	STFSDUX	f2,  CO2, INC
	STFSDUX	f1,  CO2, INC
	STFSDUX	f3,  CO2, INC
#else
	STFPDUX	f0,  AO,  INC2
	STFPDUX	f1,  AO,  INC2
	STFPDUX	f2,  AO,  INC2
	STFPDUX	f3,  AO,  INC2

	subi	AO,  AO,   8 * SIZE

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC

	STFDUX	f2,  CO2, INC
	STFSDUX	f2,  CO2, INC
	STFDUX	f3,  CO2, INC
	STFSDUX	f3,  CO2, INC
#endif

#ifdef LN
	subi	CO1, CO1, 4 * SIZE
	subi	CO2, CO2, 4 * SIZE
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

#ifdef LT
	addi	KK, KK, 4
#endif

#ifdef LN
	subi	KK, KK, 4
#endif

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L70:
	andi.	I, M,  2
	beq	.L80

#if defined(LT) || defined(RN)
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0

	fpmr	f2,  f0
	fpmr	f3, f0

	srawi.	r0,  KK,  3
	mtspr	CTR, r0
	ble	.L74
#else

#ifdef LN
	slwi	r0,   K,  1 + BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0  , KK, 1 + BASE_SHIFT
	slwi	TEMP, KK, 1 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0

	fpmr	f2,  f0
	fpmr	f3, f0

	srawi.	r0,  TEMP,  3
	mtspr	CTR, r0
	ble	.L74
#endif

	LFPDUX	A1,  AO, INC2
	LFPDUX	B1,  BO, INC2
	LFPDUX	A2,  AO, INC2
	LFPDUX	B2,  BO, INC2
	LFPDUX	A3,  AO, INC2
	LFPDUX	B3,  BO, INC2
	LFPDUX	A4,  AO, INC2
	LFPDUX	B4,  BO, INC2

	LFPDUX	A5,  AO, INC2
	LFPDUX	B5,  BO, INC2
	LFPDUX	A6,  AO, INC2
	LFPDUX	B6,  BO, INC2
	LFPDUX	A7,  AO, INC2
	LFPDUX	A9,  BO, INC2
	LFPDUX	A8,  AO, INC2
	LFPDUX	A10, BO, INC2
	bdz-	.L73
	.align 4

.L72:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f1,  B1, A1, f1
	LFPDUX	A1,  AO, INC2
	LFPDUX	B1,  BO, INC2
	fxcpmadd	f2,  B2, A2, f2
	fxcsmadd	f3,  B2, A2, f3
	LFPDUX	A2,  AO, INC2
	LFPDUX	B2,  BO, INC2

	fxcpmadd	f0,  B3, A3, f0
	fxcsmadd	f1,  B3, A3, f1
	LFPDUX	A3,  AO, INC2
	LFPDUX	B3,  BO, INC2
	fxcpmadd	f2,  B4, A4, f2
	fxcsmadd	f3,  B4, A4, f3
	LFPDUX	A4,  AO, INC2
	LFPDUX	B4,  BO, INC2

	fxcpmadd	f0,  B5, A5, f0
	fxcsmadd	f1,  B5, A5, f1
	LFPDUX	A5,  AO, INC2
	LFPDUX	B5,  BO, INC2
	fxcpmadd	f2,  B6, A6, f2
	fxcsmadd	f3,  B6, A6, f3
	LFPDUX	A6,  AO, INC2
	LFPDUX	B6,  BO, INC2

	fxcpmadd	f0,  A9,  A7, f0
	fxcsmadd	f1,  A9,  A7, f1
	LFPDUX	A7,  AO, INC2
	LFPDUX	A9,  BO, INC2
	fxcpmadd	f2,  A10, A8, f2
	fxcsmadd	f3,  A10, A8, f3
	LFPDUX	A8,  AO, INC2
	LFPDUX	A10, BO, INC2
	bdnz+	.L72
	.align 4

.L73:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f1,  B1, A1, f1
	fxcpmadd	f2,  B2, A2, f2
	fxcsmadd	f3,  B2, A2, f3

	fxcpmadd	f0,  B3, A3, f0
	fxcsmadd	f1,  B3, A3, f1
	fxcpmadd	f2,  B4, A4, f2
	fxcsmadd	f3,  B4, A4, f3

	fxcpmadd	f0,  B5, A5, f0
	fxcsmadd	f1,  B5, A5, f1
	fxcpmadd	f2,  B6, A6, f2
	fxcsmadd	f3,  B6, A6, f3

	fxcpmadd	f0,  A9,  A7, f0
	fxcsmadd	f1,  A9,  A7, f1
	fxcpmadd	f2,  A10, A8, f2
	fxcsmadd	f3,  A10, A8, f3
	.align 4

.L74:
#if defined(LT) || defined(RN)
	andi.	r0,  KK,  7
	mtspr	CTR, r0
	ble+	.L78
#else
	andi.	r0, TEMP, 7
	mtspr	CTR, r0
	ble+	.L78
#endif

	LFPDUX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	bdz-	.L77
	.align 4

.L76:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f1,  B1, A1, f1
	LFPDUX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	bdnz+	.L76
	.align 4

.L77:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f1,  B1, A1, f1
	.align 4

.L78:
	fpadd	f0, f0, f2
	fpadd	f1, f1, f3

#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 2
#else
	subi	r0, KK, 2
#endif
	slwi	TEMP, r0, 1 + BASE_SHIFT
	slwi	r0,   r0, 1 + BASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO,  B,     r0
	addi	BO,  BO, - 2 * SIZE
#endif

#if defined(LN) || defined(LT)
	fpmr	f24, f0
	fsmfp	f0,  f1
	fsmtp	f1,  f24

	LFPDUX	f16, BO,  INC2
	LFPDUX	f17, BO,  INC2

	subi	BO,  BO,   4 * SIZE

	fpsub	f0,  f16,  f0
	fpsub	f1,  f17,  f1
#else
	LFPDUX	f16, AO,  INC2
	LFPDUX	f17, AO,  INC2

	subi	AO,  AO,   4 * SIZE

	fpsub	f0,  f16,  f0
	fpsub	f1,  f17,  f1
#endif

#ifdef LN
	LFPDUX	A1,  AO,  INC2
	LFPDUX	A2,  AO,  INC2

	addi	AO,  AO,  -4 * SIZE

	fxsmul	   f1,  A2, f1
	fxcpnmsub  f0,  A2, f1,  f0
	fxpmul	   f0,  A1, f0
#endif

#ifdef LT
	LFPDUX	A1,  AO,  INC2
	LFPDUX	A2,  AO,  INC2

	addi	AO,  AO,  -4 * SIZE

	fxpmul	   f0,  A1, f0
	fxcsnmsub  f1,  A1, f0, f1

	fxsmul	   f1,  A2,  f1
#endif

#ifdef RN
	LFPDUX	A1,  BO,  INC2
	LFPDUX	A2,  BO,  INC2

	subi	BO, BO, 4 * SIZE

	fxpmul	     f0,  A1, f0
	fxcsnmsub    f1,  A1, f0, f1

	fxsmul	     f1,  A2,  f1
#endif

#ifdef RT
	LFPDUX	A2,  BO,  INC2
	LFPDUX	A1,  BO,  INC2

	subi	BO, BO, 4 * SIZE

	fxsmul	     f1,  A1, f1
	fxcpnmsub    f0,  A1, f1,  f0
	fxpmul	     f0,  A2,  f0
#endif

#ifdef LN
	subi	CO1, CO1, 2 * SIZE
	subi	CO2, CO2, 2 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFPDUX	f0,  BO,  INC2
	STFPDUX	f1,  BO,  INC2

	subi	BO,  BO,   4 * SIZE

	STFDUX	f0,  CO1, INC
	STFDUX	f1,  CO1, INC

	STFSDUX	f0,  CO2, INC
	STFSDUX	f1,  CO2, INC
#else
	STFPDUX	f0,  AO,  INC2
	STFPDUX	f1,  AO,  INC2

	subi	AO,  AO,   4 * SIZE

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC

	STFDUX	f1,  CO2, INC
	STFSDUX	f1,  CO2, INC
#endif

#ifdef LN
	subi	CO1, CO1, 2 * SIZE
	subi	CO2, CO2, 2 * SIZE
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

#ifdef LT
	addi	KK, KK, 2
#endif

#ifdef LN
	subi	KK, KK, 2
#endif

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L80:
	andi.	I, M,  1
	beq	.L89

#if defined(LT) || defined(RN)
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0

	srawi.	r0,  KK,  3
	mtspr	CTR, r0
	ble	.L84
#else

#ifdef LN
	slwi	r0,   K,  0 + BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0  , KK, 0 + BASE_SHIFT
	slwi	TEMP, KK, 1 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0

	srawi.	r0,  TEMP,  3
	mtspr	CTR, r0
	ble	.L84

#endif

	LFPDUX	B1,  BO,  INC2
	LFPDUX	A1,  AO,  INC2
	LFPDUX	A2,  AO,  INC2

	LFPDUX	B2,  BO,  INC2
	LFPDUX	A3,  AO,  INC2
	LFPDUX	A4,  AO,  INC2

	LFPDUX	B3,  BO,  INC2
	LFPDUX	B4,  BO,  INC2
	bdz-	.L83
	.align 4

.L82:
	fxcpmadd	f0,  A1, B1, f0
	LFPDUX	B1,  BO,  INC2
	fxcsmadd	f1,  A1, B2, f1
	LFPDUX	B2,  BO,  INC2
	LFPDUX	A1,  AO,  INC2
	fxcpmadd	f2,  A2, B3, f2
	LFPDUX	B3,  BO,  INC2
	fxcsmadd	f3,  A2, B4, f3
	LFPDUX	B4,  BO,  INC2
	LFPDUX	A2,  AO,  INC2

	fxcpmadd	f0,  A3, B1, f0
	LFPDUX	B1,  BO,  INC2
	fxcsmadd	f1,  A3, B2, f1
	LFPDUX	B2,  BO,  INC2
	LFPDUX	A3,  AO,  INC2
	fxcpmadd	f2,  A4, B3, f2
	LFPDUX	B3,  BO,  INC2
	fxcsmadd	f3,  A4, B4, f3
	LFPDUX	B4,  BO,  INC2
	LFPDUX	A4,  AO,  INC2
	bdnz+	.L82
	.align 4

.L83:
	fxcpmadd	f0,  A1, B1, f0
	LFPDUX	B1,  BO,  INC2
	fxcsmadd	f1,  A1, B2, f1
	LFPDUX	B2,  BO,  INC2
	fxcpmadd	f2,  A2, B3, f2
	LFPDUX	B3,  BO,  INC2
	fxcsmadd	f3,  A2, B4, f3
	LFPDUX	B4,  BO,  INC2

	fxcpmadd	f0,  A3, B1, f0
	fxcsmadd	f1,  A3, B2, f1
	fxcpmadd	f2,  A4, B3, f2
	fxcsmadd	f3,  A4, B4, f3
	.align 4

.L84:
#if defined(LT) || defined(RN)
	andi.	r0,  KK,  7
	mtspr	CTR, r0
	ble+	.L88
#else
	andi.	r0, TEMP, 7
	mtspr	CTR, r0
	ble+	.L88
#endif

	LFDX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	add	AO, AO, INC
	bdz-	.L87
	.align 4

.L86:
	fxcpmadd	f0,  A1, B1, f0
	LFDX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	add	AO, AO, INC
	bdnz+	.L86
	.align 4

.L87:
	fxcpmadd	f0,  A1, B1, f0
	.align 4

.L88:
	fpadd	f0, f0, f1
	fpadd	f2, f2, f3
	fpadd	f0, f0, f2

#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 1
#else
	subi	r0, KK, 2
#endif
	slwi	TEMP, r0, 0 + BASE_SHIFT
	slwi	r0,   r0, 1 + BASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO,  B,     r0
	addi	BO,  BO, - 2 * SIZE
#endif

#if defined(LN) || defined(LT)
	LFPDX	f16, BO,  INC2

	fpsub	f0,  f16,  f0
#else
	LFPDX	f16, AO,  INC2

	fpsub	f0,  f16,  f0
#endif

#ifdef LN
	LFPDX	A1,  AO,  INC2

	fxpmul	   f0,  A1, f0
#endif

#ifdef LT
	LFPDX	A1,  AO,  INC2

	fxpmul	   f0,  A1, f0
#endif

#ifdef RN
	LFD	A1,  (2 + 0) * SIZE(BO)
	LFD	A2,  (2 + 1) * SIZE(BO)
	LFD	A3,  (2 + 3) * SIZE(BO)

	fsmtp	     f1, f0

	fmul	     f0,  A1, f0
	fnmsub	     f1,  A2, f0, f1

	fmul	     f1,  A3, f1
	fsmfp	     f0, f1
#endif

#ifdef RT
	LFD	A1,  (2 + 3) * SIZE(BO)
	LFD	A2,  (2 + 2) * SIZE(BO)
	LFD	A3,  (2 + 0) * SIZE(BO)

	fsmtp	     f1, f0

	fmul	     f1,  A1, f1
	fnmsub	     f0,  A2, f1, f0

	fmul	     f0,  A3, f0
	fsmfp	     f0, f1
#endif

#ifdef LN
	subi	CO1, CO1, 1 * SIZE
	subi	CO2, CO2, 1 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFPDX	f0,  BO,  INC2

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO2, INC
#else
	STFPDX	f0,  AO,  INC2

	STFDUX	f0,  CO1, INC
	STFDUX	f1,  CO2, INC
#endif

#ifdef LN
	subi	CO1, CO1, 1 * SIZE
	subi	CO2, CO2, 1 * SIZE
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

#ifdef LT
	addi	KK, KK, 1
#endif

#ifdef LN
	subi	KK, KK, 1
#endif
	.align 4

.L89:
#ifdef LN
	slwi	r0, K, 1 + BASE_SHIFT
	add	B, B, r0
#endif

#if defined(LT) || defined(RN)
	addi	B,  BO, 2 * SIZE
#endif

#ifdef RN
	addi	KK, KK, 2
#endif

#ifdef RT
	subi	KK, KK, 2
#endif
	.align 4

.L90:
	andi.	J, N,  1
	beq	.L999

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

#if defined(LN) || defined(RT)
	addi	AORIG, A, -2 * SIZE
#else
	addi	AO, A, -2 * SIZE
#endif
#ifndef RT
	add	C,  CO1, LDC
#endif

	li	r0, FZERO
	lfpsx	f0, SP, r0

	srawi.	I, M,  3
	ble	.L100
	.align 4

.L91:
#if defined(LT) || defined(RN)
 	fpmr	f1,  f0
	addi	BO,  B,  - 2 * SIZE
	fpmr	f2,  f0
	fpmr	f3,  f0

	srawi.	r0,  KK,  2
	mtspr	CTR, r0
	ble	.L94
#else

#ifdef LN
	slwi	r0,   K,  3 + BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0  , KK, 3 + BASE_SHIFT
	slwi	TEMP, KK, 0 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

 	fpmr	f1,  f0
	addi	BO,  B,  - 2 * SIZE
	fpmr	f2,  f0
	fpmr	f3,  f0

	srawi.	r0,  TEMP,  2
	mtspr	CTR, r0
	ble	.L94
#endif

	LFPDUX	B1,  BO,  INC2
	LFPDUX	A1,  AO,  INC2
	LFPDUX	A2,  AO,  INC2
	LFPDUX	A3,  AO,  INC2
	LFPDUX	A4,  AO,  INC2
	LFPDUX	B2,  BO,  INC2
	LFPDUX	A5,  AO,  INC2
	LFPDUX	A6,  AO,  INC2
	LFPDUX	A7,  AO,  INC2
	LFPDUX	A8,  AO,  INC2
	bdz-	.L93
	.align 4

.L92:
	fxcpmadd	f0,  B1, A1, f0
	LFPDUX	A1,  AO,  INC2
	fxcpmadd	f1,  B1, A2, f1
	LFPDUX	A2,  AO,  INC2
	fxcpmadd	f2,  B1, A3, f2
	LFPDUX	A3,  AO,  INC2
	fxcpmadd	f3,  B1, A4, f3
	LFPDUX	A4,  AO,  INC2

	fxcsmadd	f0,  B1, A5, f0
	LFPDUX	A5,  AO,  INC2
	fxcsmadd	f1,  B1, A6, f1
	LFPDUX	A6,  AO,  INC2
	fxcsmadd	f2,  B1, A7, f2
	LFPDUX	A7,  AO,  INC2
	fxcsmadd	f3,  B1, A8, f3
	LFPDUX	A8,  AO,  INC2
	LFPDUX	B1,  BO,  INC2

	fxcpmadd	f0,  B2, A1, f0
	LFPDUX	A1,  AO,  INC2
	fxcpmadd	f1,  B2, A2, f1
	LFPDUX	A2,  AO,  INC2
	fxcpmadd	f2,  B2, A3, f2
	LFPDUX	A3,  AO,  INC2
	fxcpmadd	f3,  B2, A4, f3
	LFPDUX	A4,  AO,  INC2

	fxcsmadd	f0,  B2, A5, f0
	LFPDUX	A5,  AO,  INC2
	fxcsmadd	f1,  B2, A6, f1
	LFPDUX	A6,  AO,  INC2
	fxcsmadd	f2,  B2, A7, f2
	LFPDUX	A7,  AO,  INC2
	fxcsmadd	f3,  B2, A8, f3
	LFPDUX	A8,  AO,  INC2
	LFPDUX	B2,  BO,  INC2
	bdnz+	.L92
	.align 4

.L93:
	fxcpmadd	f0,  B1, A1, f0
	LFPDUX	A1,  AO,  INC2
	fxcpmadd	f1,  B1, A2, f1
	LFPDUX	A2,  AO,  INC2
	fxcpmadd	f2,  B1, A3, f2
	LFPDUX	A3,  AO,  INC2
	fxcpmadd	f3,  B1, A4, f3
	LFPDUX	A4,  AO,  INC2

	fxcsmadd	f0,  B1, A5, f0
	LFPDUX	A5,  AO,  INC2
	fxcsmadd	f1,  B1, A6, f1
	LFPDUX	A6,  AO,  INC2
	fxcsmadd	f2,  B1, A7, f2
	LFPDUX	A7,  AO,  INC2
	fxcsmadd	f3,  B1, A8, f3
	LFPDUX	A8,  AO,  INC2

	fxcpmadd	f0,  B2, A1, f0
	fxcpmadd	f1,  B2, A2, f1
	fxcpmadd	f2,  B2, A3, f2
	fxcpmadd	f3,  B2, A4, f3

	fxcsmadd	f0,  B2, A5, f0
	fxcsmadd	f1,  B2, A6, f1
	fxcsmadd	f2,  B2, A7, f2
	fxcsmadd	f3,  B2, A8, f3
	.align 4

.L94:
#if defined(LT) || defined(RN)
	andi.	r0,  KK,  3
	mtspr	CTR, r0
	ble+	.L98
#else
	andi.	r0, TEMP, 3
	mtspr	CTR, r0
	ble+	.L98
#endif

	LFDX	B1,  BO,  INC2
	LFPDUX	A1,  AO,  INC2
	LFPDUX	A2,  AO,  INC2
	LFPDUX	A3,  AO,  INC2
	LFPDUX	A4,  AO,  INC2
	add	BO, BO, INC
	bdz-	.L97
	.align 4

.L96:
	fxcpmadd	f0,  B1, A1, f0
	LFPDUX	A1,  AO,  INC2
	fxcpmadd	f1,  B1, A2, f1
	LFPDUX	A2,  AO,  INC2
	fxcpmadd	f2,  B1, A3, f2
	LFPDUX	A3,  AO,  INC2
	fxcpmadd	f3,  B1, A4, f3
	LFDX	B1,  BO,  INC2
	LFPDUX	A4,  AO,  INC2
	add	BO, BO, INC
	bdnz+	.L96
	.align 4

.L97:
	fxcpmadd	f0,  B1, A1, f0
	fxcpmadd	f1,  B1, A2, f1
	fxcpmadd	f2,  B1, A3, f2
	fxcpmadd	f3,  B1, A4, f3
	.align 4

.L98:
#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 8
#else
	subi	r0, KK, 1
#endif
	slwi	TEMP, r0, 3 + BASE_SHIFT
	slwi	r0,   r0, 0 + BASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO,  B,     r0
	addi	BO,  BO, - 2 * SIZE
#endif

#if defined(LN) || defined(LT)
	LFPDUX	f16, BO,  INC2
	LFPDUX	f17, BO,  INC2
	LFPDUX	f18, BO,  INC2
	LFPDUX	f19, BO,  INC2

	subi	BO,  BO,   8 * SIZE

	fpsub	f0,  f16,  f0
	fpsub	f1,  f17,  f1
	fpsub	f2,  f18,  f2
	fpsub	f3,  f19,  f3
#else
	LFPDUX	f16, AO,  INC2
	LFPDUX	f17, AO,  INC2
	LFPDUX	f18, AO,  INC2
	LFPDUX	f19, AO,  INC2

	subi	AO,  AO,   8 * SIZE

	fpsub	f0,  f16,  f0
	fpsub	f1,  f17,  f1
	fpsub	f2,  f18,  f2
	fpsub	f3,  f19,  f3
#endif

#ifdef LN
	fsmtp	f4, f0
	fsmtp	f5, f1
	fsmtp	f6, f2
	fsmtp	f7, f3

	LFD	A1, (2 + 63) * SIZE(AO)
	LFD	A2, (2 + 62) * SIZE(AO)
	LFD	A3, (2 + 61) * SIZE(AO)
	LFD	A4, (2 + 60) * SIZE(AO)
	LFD	A5, (2 + 59) * SIZE(AO)
	LFD	A6, (2 + 58) * SIZE(AO)
	LFD	A7, (2 + 57) * SIZE(AO)
	LFD	A8, (2 + 56) * SIZE(AO)

	fmul	f7, A1, f7
	fnmsub	f3, A2, f7, f3
	fnmsub	f6, A3, f7, f6
	fnmsub	f2, A4, f7, f2
	fnmsub	f5, A5, f7, f5
	fnmsub	f1, A6, f7, f1
	fnmsub	f4, A7, f7, f4
	fnmsub	f0, A8, f7, f0

	LFD	A1, (2 + 54) * SIZE(AO)
	LFD	A2, (2 + 53) * SIZE(AO)
	LFD	A3, (2 + 52) * SIZE(AO)
	LFD	A4, (2 + 51) * SIZE(AO)
	LFD	A5, (2 + 50) * SIZE(AO)
	LFD	A6, (2 + 49) * SIZE(AO)
	LFD	A7, (2 + 48) * SIZE(AO)

	fmul	f3, A1, f3
	fnmsub	f6, A2, f3, f6
	fnmsub	f2, A3, f3, f2
	fnmsub	f5, A4, f3, f5
	fnmsub	f1, A5, f3, f1
	fnmsub	f4, A6, f3, f4
	fnmsub	f0, A7, f3, f0

	LFD	A1, (2 + 45) * SIZE(AO)
	LFD	A2, (2 + 44) * SIZE(AO)
	LFD	A3, (2 + 43) * SIZE(AO)
	LFD	A4, (2 + 42) * SIZE(AO)
	LFD	A5, (2 + 41) * SIZE(AO)
	LFD	A6, (2 + 40) * SIZE(AO)

	fmul	f6, A1, f6
	fnmsub	f2, A2, f6, f2
	fnmsub	f5, A3, f6, f5
	fnmsub	f1, A4, f6, f1
	fnmsub	f4, A5, f6, f4
	fnmsub	f0, A6, f6, f0

	LFD	A1, (2 + 36) * SIZE(AO)
	LFD	A2, (2 + 35) * SIZE(AO)
	LFD	A3, (2 + 34) * SIZE(AO)
	LFD	A4, (2 + 33) * SIZE(AO)
	LFD	A5, (2 + 32) * SIZE(AO)

	fmul	f2, A1, f2
	fnmsub	f5, A2, f2, f5
	fnmsub	f1, A3, f2, f1
	fnmsub	f4, A4, f2, f4
	fnmsub	f0, A5, f2, f0

	LFD	A1, (2 + 27) * SIZE(AO)
	LFD	A2, (2 + 26) * SIZE(AO)
	LFD	A3, (2 + 25) * SIZE(AO)
	LFD	A4, (2 + 24) * SIZE(AO)

	fmul	f5, A1, f5
	fnmsub	f1, A2, f5, f1
	fnmsub	f4, A3, f5, f4
	fnmsub	f0, A4, f5, f0

	LFD	A1, (2 + 18) * SIZE(AO)
	LFD	A2, (2 + 17) * SIZE(AO)
	LFD	A3, (2 + 16) * SIZE(AO)

	fmul	f1, A1, f1
	fnmsub	f4, A2, f1, f4
	fnmsub	f0, A3, f1, f0

	LFD	A1, (2 +  9) * SIZE(AO)
	LFD	A2, (2 +  8) * SIZE(AO)

	fmul	f4, A1, f4
	fnmsub	f0, A2, f4, f0

	LFD	A1, (2 +  0) * SIZE(AO)

	fmul	f0, A1, f0

	fsmfp	f0, f4
	fsmfp	f1, f5
	fsmfp	f2, f6
	fsmfp	f3, f7
#endif

#ifdef LT
	fsmtp	f4, f0
	fsmtp	f5, f1
	fsmtp	f6, f2
	fsmtp	f7, f3

	LFD	A1, (2 +  0) * SIZE(AO)
	LFD	A2, (2 +  1) * SIZE(AO)
	LFD	A3, (2 +  2) * SIZE(AO)
	LFD	A4, (2 +  3) * SIZE(AO)
	LFD	A5, (2 +  4) * SIZE(AO)
	LFD	A6, (2 +  5) * SIZE(AO)
	LFD	A7, (2 +  6) * SIZE(AO)
	LFD	A8, (2 +  7) * SIZE(AO)

	fmul	f0, A1, f0
	fnmsub	f4, A2, f0, f4
	fnmsub	f1, A3, f0, f1
	fnmsub	f5, A4, f0, f5
	fnmsub	f2, A5, f0, f2
	fnmsub	f6, A6, f0, f6
	fnmsub	f3, A7, f0, f3
	fnmsub	f7, A8, f0, f7

	LFD	A1, (2 +  9) * SIZE(AO)
	LFD	A2, (2 + 10) * SIZE(AO)
	LFD	A3, (2 + 11) * SIZE(AO)
	LFD	A4, (2 + 12) * SIZE(AO)
	LFD	A5, (2 + 13) * SIZE(AO)
	LFD	A6, (2 + 14) * SIZE(AO)
	LFD	A7, (2 + 15) * SIZE(AO)

	fmul	f4, A1, f4
	fnmsub	f1, A2, f4, f1
	fnmsub	f5, A3, f4, f5
	fnmsub	f2, A4, f4, f2
	fnmsub	f6, A5, f4, f6
	fnmsub	f3, A6, f4, f3
	fnmsub	f7, A7, f4, f7

	LFD	A1, (2 + 18) * SIZE(AO)
	LFD	A2, (2 + 19) * SIZE(AO)
	LFD	A3, (2 + 20) * SIZE(AO)
	LFD	A4, (2 + 21) * SIZE(AO)
	LFD	A5, (2 + 22) * SIZE(AO)
	LFD	A6, (2 + 23) * SIZE(AO)

	fmul	f1, A1, f1
	fnmsub	f5, A2, f1, f5
	fnmsub	f2, A3, f1, f2
	fnmsub	f6, A4, f1, f6
	fnmsub	f3, A5, f1, f3
	fnmsub	f7, A6, f1, f7

	LFD	A1, (2 + 27) * SIZE(AO)
	LFD	A2, (2 + 28) * SIZE(AO)
	LFD	A3, (2 + 29) * SIZE(AO)
	LFD	A4, (2 + 30) * SIZE(AO)
	LFD	A5, (2 + 31) * SIZE(AO)

	fmul	f5, A1, f5
	fnmsub	f2, A2, f5, f2
	fnmsub	f6, A3, f5, f6
	fnmsub	f3, A4, f5, f3
	fnmsub	f7, A5, f5, f7

	LFD	A1, (2 + 36) * SIZE(AO)
	LFD	A2, (2 + 37) * SIZE(AO)
	LFD	A3, (2 + 38) * SIZE(AO)
	LFD	A4, (2 + 39) * SIZE(AO)

	fmul	f2, A1, f2
	fnmsub	f6, A2, f2, f6
	fnmsub	f3, A3, f2, f3
	fnmsub	f7, A4, f2, f7

	LFD	A1, (2 + 45) * SIZE(AO)
	LFD	A2, (2 + 46) * SIZE(AO)
	LFD	A3, (2 + 47) * SIZE(AO)

	fmul	f6, A1, f6
	fnmsub	f3, A2, f6, f3
	fnmsub	f7, A3, f6, f7

	LFD	A1, (2 + 54) * SIZE(AO)
	LFD	A2, (2 + 55) * SIZE(AO)

	fmul	f3, A1, f3
	fnmsub	f7, A2, f3, f7

	LFD	A1, (2 + 63) * SIZE(AO)

	fmul	f7, A1, f7

	fsmfp	f0, f4
	fsmfp	f1, f5
	fsmfp	f2, f6
	fsmfp	f3, f7
#endif

#ifdef RN
	LFPDX	A1,  BO,  INC2

	fxpmul	f0,  A1,  f0
	fxpmul	f1,  A1,  f1
	fxpmul	f2,  A1,  f2
	fxpmul	f3,  A1,  f3
#endif

#ifdef RT
	LFPDX	A1,  BO,  INC2

	fxpmul	f0,  A1,  f0
	fxpmul	f1,  A1,  f1
	fxpmul	f2,  A1,  f2
	fxpmul	f3,  A1,  f3

#endif

#ifdef LN
	subi	CO1, CO1, 8 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFPDUX	f0,  BO,  INC2
	STFPDUX	f1,  BO,  INC2
	STFPDUX	f2,  BO,  INC2
	STFPDUX	f3,  BO,  INC2

	subi	BO,  BO,   8 * SIZE

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC
	STFDUX	f2,  CO1, INC
	STFSDUX	f2,  CO1, INC
	STFDUX	f3,  CO1, INC
	STFSDUX	f3,  CO1, INC
#else
	STFPDUX	f0,  AO,  INC2
	STFPDUX	f1,  AO,  INC2
	STFPDUX	f2,  AO,  INC2
	STFPDUX	f3,  AO,  INC2

	subi	AO,  AO,   8 * SIZE

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC
	STFDUX	f2,  CO1, INC
	STFSDUX	f2,  CO1, INC
	STFDUX	f3,  CO1, INC
	STFSDUX	f3,  CO1, INC
#endif

#ifdef LN
	subi	CO1, CO1, 8 * SIZE
#endif

#ifdef RT
	slwi	r0, K, 3 + BASE_SHIFT
	add	AORIG, AORIG, r0
#endif

#if defined(LT) || defined(RN)
	sub	TEMP, K, KK
	slwi	r0,   TEMP, 3 + BASE_SHIFT
	slwi	TEMP, TEMP, 0 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LT
	addi	KK, KK, 8
#endif

#ifdef LN
	subi	KK, KK, 8
#endif

	addic.	I, I, -1
	li	r0, FZERO

	lfpsx	f0, SP, r0
	bgt+	.L91
	.align 4

.L100:
	andi.	I, M,  4
	beq	.L110

#if defined(LT) || defined(RN)
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3, f0

	srawi.	r0,  KK,  3
	mtspr	CTR, r0
	ble	.L104
#else

#ifdef LN
	slwi	r0,   K,  2 + BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0  , KK, 2 + BASE_SHIFT
	slwi	TEMP, KK, 0 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3, f0

	srawi.	r0,  TEMP,  3
	mtspr	CTR, r0
	ble	.L104
#endif

	LFPDUX	B1,  BO,  INC2
	LFPDUX	A1,  AO,  INC2
	LFPDUX	A2,  AO,  INC2
	LFPDUX	A3,  AO,  INC2
	LFPDUX	A4,  AO,  INC2
	LFPDUX	B2,  BO,  INC2
	LFPDUX	A5,  AO,  INC2
	LFPDUX	A6,  AO,  INC2
	LFPDUX	A7,  AO,  INC2
	LFPDUX	A8,  AO,  INC2
	LFPDUX	B3,  BO,  INC2
	LFPDUX	B4,  BO,  INC2

	bdz-	.L103
	.align 4

.L102:
	fxcpmadd	f0,  B1, A1, f0
	LFPDUX	A1,  AO,  INC2
	fxcpmadd	f1,  B1, A2, f1
	LFPDUX	A2,  AO,  INC2
	fxcsmadd	f2,  B1, A3, f2
	LFPDUX	A3,  AO,  INC2
	fxcsmadd	f3,  B1, A4, f3
	LFPDUX	A4,  AO,  INC2
	LFPDUX	B1,  BO,  INC2

	fxcpmadd	f0,  B2, A5, f0
	LFPDUX	A5,  AO,  INC2
	fxcpmadd	f1,  B2, A6, f1
	LFPDUX	A6,  AO,  INC2
	fxcsmadd	f2,  B2, A7, f2
	LFPDUX	A7,  AO,  INC2
	fxcsmadd	f3,  B2, A8, f3
	LFPDUX	A8,  AO,  INC2
	LFPDUX	B2,  BO,  INC2

	fxcpmadd	f0,  B3, A1, f0
	LFPDUX	A1,  AO,  INC2
	fxcpmadd	f1,  B3, A2, f1
	LFPDUX	A2,  AO,  INC2
	fxcsmadd	f2,  B3, A3, f2
	LFPDUX	A3,  AO,  INC2
	fxcsmadd	f3,  B3, A4, f3
	LFPDUX	A4,  AO,  INC2
	LFPDUX	B3,  BO,  INC2

	fxcpmadd	f0,  B4, A5, f0
	LFPDUX	A5,  AO,  INC2
	fxcpmadd	f1,  B4, A6, f1
	LFPDUX	A6,  AO,  INC2
	fxcsmadd	f2,  B4, A7, f2
	LFPDUX	A7,  AO,  INC2
	fxcsmadd	f3,  B4, A8, f3
	LFPDUX	A8,  AO,  INC2
	LFPDUX	B4,  BO,  INC2
	bdnz+	.L102
	.align 4

.L103:
	fxcpmadd	f0,  B1, A1, f0
	LFPDUX	A1,  AO,  INC2
	fxcpmadd	f1,  B1, A2, f1
	LFPDUX	A2,  AO,  INC2
	fxcsmadd	f2,  B1, A3, f2
	LFPDUX	A3,  AO,  INC2
	fxcsmadd	f3,  B1, A4, f3
	LFPDUX	A4,  AO,  INC2

	fxcpmadd	f0,  B2, A5, f0
	LFPDUX	A5,  AO,  INC2
	fxcpmadd	f1,  B2, A6, f1
	LFPDUX	A6,  AO,  INC2
	fxcsmadd	f2,  B2, A7, f2
	LFPDUX	A7,  AO,  INC2
	fxcsmadd	f3,  B2, A8, f3
	LFPDUX	A8,  AO,  INC2

	fxcpmadd	f0,  B3, A1, f0
	fxcpmadd	f1,  B3, A2, f1
	fxcsmadd	f2,  B3, A3, f2
	fxcsmadd	f3,  B3, A4, f3

	fxcpmadd	f0,  B4, A5, f0
	fxcpmadd	f1,  B4, A6, f1
	fxcsmadd	f2,  B4, A7, f2
	fxcsmadd	f3,  B4, A8, f3
	.align 4

.L104:
#if defined(LT) || defined(RN)
	andi.	r0,  KK,  7
	mtspr	CTR, r0
	ble+	.L108
#else
	andi.	r0, TEMP, 7
	mtspr	CTR, r0
	ble+	.L108
#endif

	LFPDUX	A1,  AO,  INC2
	LFDX	B1,  BO,  INC2
	LFPDUX	A2,  AO,  INC2
	add	BO, BO, INC
	bdz-	.L107
	.align 4

.L106:
	fxcpmadd	f0,  B1, A1, f0
	LFPDUX	A1,  AO,  INC2
	fxcpmadd	f1,  B1, A2, f1
	LFDX	B1,  BO,  INC2
	LFPDUX	A2,  AO,  INC2
	add	BO, BO, INC
	bdnz+	.L106
	.align 4

.L107:
	fxcpmadd	f0,  B1, A1, f0
	fxcpmadd	f1,  B1, A2, f1
	.align 4

.L108:
	fpadd	f0, f0, f2
	fpadd	f1, f1, f3

#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 4
#else
	subi	r0, KK, 1
#endif
	slwi	TEMP, r0, 2 + BASE_SHIFT
	slwi	r0,   r0, 0 + BASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO,  B,     r0
	addi	BO,  BO, - 2 * SIZE
#endif

#if defined(LN) || defined(LT)
	LFPDUX	f16, BO,  INC2
	LFPDUX	f17, BO,  INC2

	subi	BO,  BO,   4 * SIZE

	fpsub	f0,  f16,  f0
	fpsub	f1,  f17,  f1
#else
	LFPDUX	f16, AO,  INC2
	LFPDUX	f17, AO,  INC2

	subi	AO,  AO,   4 * SIZE

	fpsub	f0,  f16,  f0
	fpsub	f1,  f17,  f1
#endif

#ifdef LN
	fsmtp	f4, f0
	fsmtp	f5, f1

	LFD	A1, (2 + 15) * SIZE(AO)
	LFD	A2, (2 + 14) * SIZE(AO)
	LFD	A3, (2 + 13) * SIZE(AO)
	LFD	A4, (2 + 12) * SIZE(AO)

	fmul	f5, A1, f5
	fnmsub	f1, A2, f5, f1
	fnmsub	f4, A3, f5, f4
	fnmsub	f0, A4, f5, f0

	LFD	A1, (2 + 10) * SIZE(AO)
	LFD	A2, (2 +  9) * SIZE(AO)
	LFD	A3, (2 +  8) * SIZE(AO)

	fmul	f1, A1, f1
	fnmsub	f4, A2, f1, f4
	fnmsub	f0, A3, f1, f0

	LFD	A1, (2 +  5) * SIZE(AO)
	LFD	A2, (2 +  4) * SIZE(AO)

	fmul	f4, A1, f4
	fnmsub	f0, A2, f4, f0

	LFD	A1, (2 +  0) * SIZE(AO)

	fmul	f0, A1, f0

	fsmfp	f0, f4
	fsmfp	f1, f5
#endif

#ifdef LT
	fsmtp	f4, f0
	fsmtp	f5, f1

	LFD	A1, (2 +  0) * SIZE(AO)
	LFD	A2, (2 +  1) * SIZE(AO)
	LFD	A3, (2 +  2) * SIZE(AO)
	LFD	A4, (2 +  3) * SIZE(AO)

	fmul	f0, A1, f0
	fnmsub	f4, A2, f0, f4
	fnmsub	f1, A3, f0, f1
	fnmsub	f5, A4, f0, f5

	LFD	A1, (2 +  5) * SIZE(AO)
	LFD	A2, (2 +  6) * SIZE(AO)
	LFD	A3, (2 +  7) * SIZE(AO)

	fmul	f4, A1, f4
	fnmsub	f1, A2, f4, f1
	fnmsub	f5, A3, f4, f5

	LFD	A1, (2 + 10) * SIZE(AO)
	LFD	A2, (2 + 11) * SIZE(AO)

	fmul	f1, A1, f1
	fnmsub	f5, A2, f1, f5

	LFD	A1, (2 + 15) * SIZE(AO)

	fmul	f5, A1, f5

	fsmfp	f0, f4
	fsmfp	f1, f5
#endif

#ifdef RN
	LFPDX	A1,  BO,  INC2

	fxpmul	f0,  A1,  f0
	fxpmul	f1,  A1,  f1
#endif

#ifdef RT
	LFPDX	A1,  BO,  INC2

	fxpmul	f0,  A1,  f0
	fxpmul	f1,  A1,  f1
#endif

#ifdef LN
	subi	CO1, CO1, 4 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFPDUX	f0,  BO,  INC2
	STFPDUX	f1,  BO,  INC2

	subi	BO,  BO,   4 * SIZE

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC
#else
	STFPDUX	f0,  AO,  INC2
	STFPDUX	f1,  AO,  INC2

	subi	AO,  AO,   4 * SIZE

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC
#endif

#ifdef LN
	subi	CO1, CO1, 4 * SIZE
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

#ifdef LT
	addi	KK, KK, 4
#endif

#ifdef LN
	subi	KK, KK, 4
#endif

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L110:
	andi.	I, M,  2
	beq	.L120

#if defined(LT) || defined(RN)
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0

	srawi.	r0,  KK,  3
	mtspr	CTR, r0
	ble	.L114
#else

#ifdef LN
	slwi	r0,   K,  1 + BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0  , KK, 1 + BASE_SHIFT
	slwi	TEMP, KK, 0 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0

	srawi.	r0, TEMP,  3
	mtspr	CTR, r0
	ble	.L114
#endif

	LFPDUX	A1,  AO,  INC2
	LFPDUX	A2,  AO,  INC2
	LFPDUX	B1,  BO,  INC2

	LFPDUX	A3,  AO,  INC2
	LFPDUX	A4,  AO,  INC2
	LFPDUX	B2,  BO,  INC2

	LFPDUX	A5,  AO,  INC2
	LFPDUX	A6,  AO,  INC2
	LFPDUX	B3,  BO,  INC2

	LFPDUX	A7,  AO,  INC2
	LFPDUX	A8,  AO,  INC2
	LFPDUX	B4,  BO,  INC2
	bdz-	.L113
	.align 4

.L112:
	fxcpmadd	f0,  B1, A1, f0
	LFPDUX	A1,  AO,  INC2
	fxcsmadd	f1,  B1, A2, f1
	LFPDUX	A2,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	fxcpmadd	f2,  B2, A3, f2
	LFPDUX	A3,  AO,  INC2
	fxcsmadd	f3,  B2, A4, f3
	LFPDUX	A4,  AO,  INC2
	LFPDUX	B2,  BO,  INC2
	fxcpmadd	f0,  B3, A5, f0
	LFPDUX	A5,  AO,  INC2
	fxcsmadd	f1,  B3, A6, f1
	LFPDUX	A6,  AO,  INC2
	LFPDUX	B3,  BO,  INC2
	fxcpmadd	f2,  B4, A7, f2
	LFPDUX	A7,  AO,  INC2
	fxcsmadd	f3,  B4, A8, f3
	LFPDUX	A8,  AO,  INC2
	LFPDUX	B4,  BO,  INC2
	bdnz+	.L112
	.align 4

.L113:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f1,  B1, A2, f1
	fxcpmadd	f2,  B2, A3, f2
	fxcsmadd	f3,  B2, A4, f3
	fxcpmadd	f0,  B3, A5, f0
	fxcsmadd	f1,  B3, A6, f1
	fxcpmadd	f2,  B4, A7, f2
	fxcsmadd	f3,  B4, A8, f3
	.align 4

.L114:
#if defined(LT) || defined(RN)
	andi.	r0,  KK,  7
	mtspr	CTR, r0
	ble+	.L118
#else
	andi.	r0, TEMP, 7
	mtspr	CTR, r0
	ble+	.L118
#endif

	LFPDUX	A1,  AO,  INC2
	LFDX	B1,  BO,  INC2
	add	BO, BO, INC
	bdz-	.L117
	.align 4

.L116:
	fxcpmadd	f0,  B1, A1, f0
	LFPDUX	A1,  AO,  INC2
	LFDX	B1,  BO,  INC2
	add	BO, BO, INC
	bdnz+	.L116
	.align 4

.L117:
	fxcpmadd	f0,  B1, A1, f0
	.align 4

.L118:
	fpadd	f0, f0, f1
	fpadd	f2, f3, f2
	fpadd	f0, f0, f2

#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 2
#else
	subi	r0, KK, 1
#endif
	slwi	TEMP, r0, 1 + BASE_SHIFT
	slwi	r0,   r0, 0 + BASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO,  B,     r0
	addi	BO,  BO, - 2 * SIZE
#endif

#if defined(LN) || defined(LT)
	LFPDX	f16, BO,  INC2

	fpsub	f0,  f16,  f0
#else
	LFPDX	f16, AO,  INC2

	fpsub	f0,  f16,  f0
#endif

#ifdef LN
	fsmtp	f4, f0

	LFD	A1, (2 +  3) * SIZE(AO)
	LFD	A2, (2 +  2) * SIZE(AO)
	LFD	A3, (2 +  0) * SIZE(AO)

	fmul	f4, A1, f4
	fnmsub	f0, A2, f4, f0
	fmul	f0, A3, f0
	fsmfp	f0, f4
#endif

#ifdef LT
	fsmtp	f4, f0

	LFD	A1, (2 +  0) * SIZE(AO)
	LFD	A2, (2 +  1) * SIZE(AO)
	LFD	A3, (2 +  3) * SIZE(AO)

	fmul	f0, A1, f0
	fnmsub	f4, A2, f0, f4
	fmul	f4, A3, f4

	fsmfp	f0, f4
#endif

#ifdef RN
	LFPDX	A1,  BO,  INC2

	fxpmul	f0,  A1,  f0
#endif

#ifdef RT
	LFPDX	A1,  BO,  INC2

	fxpmul	f0,  A1,  f0
#endif

#ifdef LN
	subi	CO1, CO1, 2 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFPDX	f0,  BO,  INC2

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
#else
	STFPDX	f0,  AO,  INC2

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
#endif

#ifdef LN
	subi	CO1, CO1, 2 * SIZE
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

#ifdef LT
	addi	KK, KK, 2
#endif

#ifdef LN
	subi	KK, KK, 2
#endif

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L120:
	andi.	I, M,  1
	beq	.L129

#if defined(LT) || defined(RN)
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0

	srawi.	r0,  KK,  3
	mtspr	CTR, r0
	ble	.L124
#else

#ifdef LN
	slwi	r0,   K,  0 + BASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0  , KK, 0 + BASE_SHIFT
	slwi	TEMP, KK, 0 + BASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0

	srawi.	r0,  TEMP,  3
	mtspr	CTR, r0
	ble	.L124
#endif

	LFPDUX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	LFPDUX	A2,  AO,  INC2
	LFPDUX	B2,  BO,  INC2
	LFPDUX	A3,  AO,  INC2
	LFPDUX	B3,  BO,  INC2
	LFPDUX	A4,  AO,  INC2
	LFPDUX	B4,  BO,  INC2
	bdz-	.L123
	.align 4

.L122:
	fpmadd	f0,  A1, B1, f0
	LFPDUX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	fpmadd	f1,  A2, B2, f1
	LFPDUX	A2,  AO,  INC2
	LFPDUX	B2,  BO,  INC2
	fpmadd	f2,  A3, B3, f2
	LFPDUX	A3,  AO,  INC2
	LFPDUX	B3,  BO,  INC2
	fpmadd	f3,  A4, B4, f3
	LFPDUX	A4,  AO,  INC2
	LFPDUX	B4,  BO,  INC2
	bdnz+	.L122
	.align 4

.L123:
	fpmadd	f0,  A1, B1, f0
	fpmadd	f1,  A2, B2, f1
	fpmadd	f2,  A3, B3, f2
	fpmadd	f3,  A4, B4, f3
	.align 4

.L124:
#if defined(LT) || defined(RN)
	andi.	r0,  KK,  7
	mtspr	CTR, r0
	ble+	.L128
#else
	andi.	r0, TEMP, 7
	mtspr	CTR, r0
	ble+	.L128
#endif

	LFDX	A1,  AO,  INC2
	LFDX	B1,  BO,  INC2
	add	AO, AO, INC
	add	BO, BO, INC
	bdz-	.L127
	.align 4

.L126:
	fmadd	f0,  A1, B1, f0
	LFDX	A1,  AO,  INC2
	LFDX	B1,  BO,  INC2
	add	AO, AO, INC
	add	BO, BO, INC
	bdnz+	.L126
	.align 4

.L127:
	fmadd	f0,  A1, B1, f0
	.align 4

.L128:
	fpadd	f0, f0, f1
	fpadd	f2, f2, f3
	fpadd	f0, f0, f2
	fsmtp	f1, f0

	fadd	f0, f0, f1

#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 1
#else
	subi	r0, KK, 1
#endif
	slwi	TEMP, r0, 0 + BASE_SHIFT
	slwi	r0,   r0, 0 + BASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO,  B,     r0
	addi	BO,  BO, - 2 * SIZE
#endif

#if defined(LN) || defined(LT)
	LFDX	f16, BO,  INC2

	fsub	f0,  f16,  f0
#else
	LFDX	f16, AO,  INC2

	fsub	f0,  f16,  f0
#endif

#ifdef LN
	LFD	A1, (2 +  0) * SIZE(AO)

	fmul	f0, A1, f0
#endif

#ifdef LT
	LFD	A1, (2 +  0) * SIZE(AO)

	fmul	f0, A1, f0
#endif

#ifdef RN
	LFDX	A1,  BO,  INC2

	fmul	f0,  A1,  f0
#endif

#ifdef RT
	LFDX	A1,  BO,  INC2

	fmul	f0,  A1,  f0
#endif

#ifdef LN
	subi	CO1, CO1, 1 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFDX	f0,  BO,  INC2

	STFDUX	f0,  CO1, INC
#else
	STFDX	f0,  AO,  INC2

	STFDUX	f0,  CO1, INC
#endif

#ifdef LN
	subi	CO1, CO1, 1 * SIZE
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

#ifdef LT
	addi	KK, KK, 1
#endif

#ifdef LN
	subi	KK, KK, 1
#endif
	.align 4

.L129:
#ifdef LN
	slwi	r0, K, 0 + BASE_SHIFT
	add	B, B, r0
#endif

#if defined(LT) || defined(RN)
	addi	B,  BO, 2 * SIZE
#endif

#ifdef RN
	addi	KK, KK, 1
#endif

#ifdef RT
	subi	KK, KK, 1
#endif
	.align 4


.L999:
	addi	SP, SP, 12

	lwzu	r14,   4(SP)
	lwzu	r15,   4(SP)

	lwzu	r16,   4(SP)
	lwzu	r17,   4(SP)
	lwzu	r18,   4(SP)
	lwzu	r19,   4(SP)

	lwzu	r20,   4(SP)
	lwzu	r21,   4(SP)
	lwzu	r22,   4(SP)
	lwzu	r23,   4(SP)

	lwzu	r24,   4(SP)
	lwzu	r25,   4(SP)
	lwzu	r26,   4(SP)
	lwzu	r27,   4(SP)

	lwzu	r28,   4(SP)
	lwzu	r29,   4(SP)
	lwzu	r30,   4(SP)
	lwzu	r31,   4(SP)

	subi	SP, SP, 12
	li	r0, 16

	lfpdux	f31, SP, r0
	lfpdux	f30, SP, r0
	lfpdux	f29, SP, r0
	lfpdux	f28, SP, r0
	lfpdux	f27, SP, r0
	lfpdux	f26, SP, r0
	lfpdux	f25, SP, r0
	lfpdux	f24, SP, r0
	lfpdux	f23, SP, r0
	lfpdux	f22, SP, r0
	lfpdux	f21, SP, r0
	lfpdux	f20, SP, r0
	lfpdux	f19, SP, r0
	lfpdux	f18, SP, r0
	lfpdux	f17, SP, r0
	lfpdux	f16, SP, r0
	lfpdux	f15, SP, r0
	lfpdux	f14, SP, r0
	addi	SP, SP, 16
	blr


	EPILOGUE
#endif
