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

#undef ZERO

#define ALPHA    0
#define FZERO	16

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
#define KK	r14
#define INCM1	r15
#define INCM3	r16
#define INCM5	r17
#define INCM7	r18
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
#define	ZERO	r31

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

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define FXCPMADD fxcpmadd
#define FXCSMADD fxcxnpma
#else
#define FXCPMADD fxcpnsma
#define FXCSMADD fxcxma
#endif

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
	stwu	r14,  -4(SP)

	li	r0,   0
	stwu	r0,   -4(SP)
	stwu	r0,   -4(SP)

	stfdu	f2,   -8(SP)
	stfdu	f1,   -8(SP)

	slwi	LDC, LDC, ZBASE_SHIFT

	cmpwi	cr0, M, 0
	ble	.L999
	cmpwi	cr0, N, 0
	ble	.L999
	cmpwi	cr0, K, 0
	ble	.L999

#if defined(TRMMKERNEL) && !defined(LEFT)
	neg	KK, OFFSET
#endif

	andi.	r0, C,   2 * SIZE - 1
	bne	.L1000

	li	INC,    1 * SIZE
	li	INC2,   2 * SIZE
	li	INC4,   4 * SIZE
	li	INCM1, -1 * SIZE
	li	INCM3, -2 * SIZE
	li	INCM5, -4 * SIZE
	li	INCM7, -6 * SIZE

	addi	C, C, - 2 * SIZE
	srawi.	J, N,  1
	ble	.L50
	.align 4

.L10:
	mr	CO1, C
	add	CO2, C,   LDC
	add	C,   CO2, LDC

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mr	KK, OFFSET
#endif

	addi	AO, A, -4 * SIZE

	li	r0, FZERO
	lfpsx	f0, SP, r0

	srawi.	I, M,  2
	ble	.L20
	.align 4

.L11:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	AO2, AO,   2 * SIZE
	fpmr	f4,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f12, f0
#else
	slwi	TEMP, KK, 2 + ZBASE_SHIFT
	slwi	r0,   KK, 1 + ZBASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	AO2, AO,   2 * SIZE
	fpmr	f4,  f0
	addi	BO,  BO, - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, BO,   2 * SIZE
	fpmr	f12, f0
#endif
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 2
#endif
	srawi.	r0,  TEMP,  2
 	fpmr	f1,  f0
	mtspr	CTR, r0
	ble	.L14
#else
	addi	AO2, AO,   2 * SIZE
	fpmr	f4,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f12, f0

	srawi.	r0,  K,  2
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
	FXCPMADD	f0,  B1, A1, f0
	nop
	FXCSMADD	f4,  B1, A1, f4
	nop
	FXCPMADD	f8,  B2, A1, f8
	LFPDUX	B4, BO2, INC4
	FXCSMADD	f12, B2, A1, f12
	LFPDUX	B6,  BO, INC4

	FXCPMADD	f1,  B1, A2, f1
	nop
	FXCSMADD	f5,  B1, A2, f5
	LFPDUX	A4, AO2, INC4
	FXCPMADD	f9,  B2, A2, f9
	LFPDUX	A10, AO, INC4
	FXCSMADD	f13, B2, A2, f13
	nop

	FXCPMADD	f2,  B1, A3, f2
	nop
	FXCSMADD	f6,  B1, A3, f6
	nop
	FXCPMADD	f10, B2, A3, f10
	nop
	FXCSMADD	f14, B2, A3, f14
	nop

	FXCPMADD	f3,  B1, A4, f3
	nop
	FXCSMADD	f7,  B1, A4, f7
	LFPDUX	A2, AO2, INC4
	FXCPMADD	f11, B2, A4, f11
	LFPDUX	A1,  AO, INC4
	FXCSMADD	f15, B2, A4, f15
	nop

## 2 ##

	FXCPMADD	f0,  B3, A5, f0
	nop
	FXCSMADD	f4,  B3, A5, f4
	nop
	FXCPMADD	f8,  B4, A5, f8
	LFPDUX	B2, BO2, INC4
	FXCSMADD	f12, B4, A5, f12
	LFPDUX	B1,  BO, INC4

	FXCPMADD	f1,  B3, A2, f1
	nop
	FXCSMADD	f5,  B3, A2, f5
	LFPDUX	A4, AO2, INC4
	FXCPMADD	f9,  B4, A2, f9
	LFPDUX	A3,  AO, INC4
	FXCSMADD	f13, B4, A2, f13
	nop

	FXCPMADD	f2,  B3, A6, f2
	nop
	FXCSMADD	f6,  B3, A6, f6
	nop
	FXCPMADD	f10, B4, A6, f10
	nop
	FXCSMADD	f14, B4, A6, f14
	nop

	FXCPMADD	f3,  B3, A4, f3
	nop
	FXCSMADD	f7,  B3, A4, f7
	LFPDUX	A2, AO2, INC4
	FXCPMADD	f11, B4, A4, f11
	LFPDUX	A5,  AO, INC4
	FXCSMADD	f15, B4, A4, f15
	nop

## 3 ##

	FXCPMADD	f0,  B5, A7, f0
	nop
	FXCSMADD	f4,  B5, A7, f4
	nop
	FXCPMADD	f8,  B2, A7, f8
	LFPDUX	B4, BO2, INC4
	FXCSMADD	f12, B2, A7, f12
	LFPDUX	B3,  BO, INC4

	FXCPMADD	f1,  B5, A2, f1
	nop
	FXCSMADD	f5,  B5, A2, f5
	LFPDUX	A4, AO2, INC4
	FXCPMADD	f9,  B2, A2, f9
	LFPDUX	A6,  AO, INC4
	FXCSMADD	f13, B2, A2, f13
	nop

	FXCPMADD	f2,  B5, A8, f2
	nop
	FXCSMADD	f6,  B5, A8, f6
	nop
	FXCPMADD	f10, B2, A8, f10
	nop
	FXCSMADD	f14, B2, A8, f14
	nop

	FXCPMADD	f3,  B5, A4, f3
	nop
	FXCSMADD	f7,  B5, A4, f7
	LFPDUX	A2, AO2, INC4
	FXCPMADD	f11, B2, A4, f11
	LFPDUX	A7,  AO, INC4
	FXCSMADD	f15, B2, A4, f15
	nop

## 4 ##
	FXCPMADD	f0,  B6, A9, f0
	nop
	FXCSMADD	f4,  B6, A9, f4
	nop
	FXCPMADD	f8,  B4, A9, f8
	LFPDUX	B2, BO2, INC4
	FXCSMADD	f12, B4, A9, f12
	LFPDUX	B5,  BO, INC4

	FXCPMADD	f1,  B6, A2, f1
	nop
	FXCSMADD	f5,  B6, A2, f5
	LFPDUX	A4, AO2, INC4
	FXCPMADD	f9,  B4, A2, f9
	LFPDUX	A8,  AO, INC4
	FXCSMADD	f13, B4, A2, f13
	nop

	FXCPMADD	f2,  B6, A10, f2
	nop
	FXCSMADD	f6,  B6, A10, f6
	nop
	FXCPMADD	f10, B4, A10, f10
	nop
	FXCSMADD	f14, B4, A10, f14
	nop

	FXCPMADD	f3,  B6, A4, f3
	LFPDUX	A2, AO2, INC4
	FXCSMADD	f7,  B6, A4, f7
	LFPDUX	A9,  AO, INC4
	FXCPMADD	f11, B4, A4, f11
	nop
	FXCSMADD	f15, B4, A4, f15
	bdnz+	.L12
	.align 4

.L13:
## 1 ##

	FXCPMADD	f0,  B1, A1, f0
	nop
	FXCSMADD	f4,  B1, A1, f4
	nop
	FXCPMADD	f8,  B2, A1, f8
	LFPDUX	B4, BO2, INC4
	FXCSMADD	f12, B2, A1, f12
	LFPDUX	B6,  BO, INC4

	FXCPMADD	f1,  B1, A2, f1
	nop
	FXCSMADD	f5,  B1, A2, f5
	LFPDUX	A4, AO2, INC4
	FXCPMADD	f9,  B2, A2, f9
	LFPDUX	A10, AO, INC4
	FXCSMADD	f13, B2, A2, f13
	nop

	FXCPMADD	f2,  B1, A3, f2
	nop
	FXCSMADD	f6,  B1, A3, f6
	nop
	FXCPMADD	f10, B2, A3, f10
	nop
	FXCSMADD	f14, B2, A3, f14
	nop

	FXCPMADD	f3,  B1, A4, f3
	nop
	FXCSMADD	f7,  B1, A4, f7
	LFPDUX	A2, AO2, INC4
	FXCPMADD	f11, B2, A4, f11
#ifndef TRMMKERNEL
	LFPDUX	A1, CO1, INC2
#else
	nop
#endif
	FXCSMADD	f15, B2, A4, f15
	nop

## 2 ##

	FXCPMADD	f0,  B3, A5, f0
	nop
	FXCSMADD	f4,  B3, A5, f4
	nop
	FXCPMADD	f8,  B4, A5, f8
	LFPDUX	B2, BO2, INC4
	FXCSMADD	f12, B4, A5, f12
#ifndef TRMMKERNEL
	LFPDUX	B1, CO1, INC2
#else
	nop
#endif

	FXCPMADD	f1,  B3, A2, f1
	nop
	FXCSMADD	f5,  B3, A2, f5
	LFPDUX	A4, AO2, INC4
	FXCPMADD	f9,  B4, A2, f9
#ifndef TRMMKERNEL
	LFPDUX	A3, CO1, INC2
#else
	nop
#endif
	FXCSMADD	f13, B4, A2, f13
	nop

	FXCPMADD	f2,  B3, A6, f2
	nop
	FXCSMADD	f6,  B3, A6, f6
	nop
	FXCPMADD	f10, B4, A6, f10
	nop
	FXCSMADD	f14, B4, A6, f14
	nop

	FXCPMADD	f3,  B3, A4, f3
	nop
	FXCSMADD	f7,  B3, A4, f7
	LFPDUX	A2, AO2, INC4
	FXCPMADD	f11, B4, A4, f11
#ifndef TRMMKERNEL
   	LFPDUX	A5, CO1, INC2
#else
	nop
#endif
	FXCSMADD	f15, B4, A4, f15
	nop

## 3 ##

	FXCPMADD	f0,  B5, A7, f0
	nop
	FXCSMADD	f4,  B5, A7, f4
	nop
	FXCPMADD	f8,  B2, A7, f8
	LFPDUX	B4, BO2, INC4
	FXCSMADD	f12, B2, A7, f12
#ifndef TRMMKERNEL
	LFPDUX	B3, CO2, INC2
#else
	nop
#endif

	FXCPMADD	f1,  B5, A2, f1
	nop
	FXCSMADD	f5,  B5, A2, f5
	LFPDUX	A4, AO2, INC4
	FXCPMADD	f9,  B2, A2, f9
#ifndef TRMMKERNEL
	LFPDUX	A6, CO2, INC2
#else
	nop
#endif
	FXCSMADD	f13, B2, A2, f13

	FXCPMADD	f2,  B5, A8, f2
	nop
	FXCSMADD	f6,  B5, A8, f6
	nop
	FXCPMADD	f10, B2, A8, f10
	nop
	FXCSMADD	f14, B2, A8, f14
	nop

	FXCPMADD	f3,  B5, A4, f3
	nop
	FXCSMADD	f7,  B5, A4, f7
	LFPDUX	A2, AO2, INC4
	FXCPMADD	f11, B2, A4, f11
#ifndef TRMMKERNEL
	LFPDUX	A7, CO2, INC2
#else
	nop
#endif
	FXCSMADD	f15, B2, A4, f15
	nop

## 4 ##

	FXCPMADD	f0,  B6, A9, f0
	nop
	FXCSMADD	f4,  B6, A9, f4
	nop
	FXCPMADD	f8,  B4, A9, f8
#ifndef TRMMKERNEL
	LFPDUX	B2, CO2, INC2
#else
	nop
#endif

	FXCSMADD	f12, B4, A9, f12

	FXCPMADD	f1,  B6, A2, f1
	nop
	FXCSMADD	f5,  B6, A2, f5
	LFPDUX	A4, AO2, INC4
	FXCPMADD	f9,  B4, A2, f9
	nop
	FXCSMADD	f13, B4, A2, f13
	nop

	FXCPMADD	f2,  B6, A10, f2
	FXCSMADD	f6,  B6, A10, f6
	FXCPMADD	f10, B4, A10, f10
	FXCSMADD	f14, B4, A10, f14

	FXCPMADD	f3,  B6, A4, f3
	FXCSMADD	f7,  B6, A4, f7
	FXCPMADD	f11, B4, A4, f11
	FXCSMADD	f15, B4, A4, f15
	.align 4

.L14:
	li	r0, ALPHA
	lfpdx	AP,  SP, r0
#ifdef TRMMKERNEL
	li	r0, FZERO
	lfpsx	f30, SP, r0
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 2
#endif
	andi.	r0,  TEMP,  3
	mtspr	CTR, r0
	ble+	.L18

	cmpwi	cr0, TEMP, 3
	bgt+	.L15
#else
	andi.	r0,  K,  3
	mtspr	CTR, r0
	ble+	.L18

	cmpwi	cr0, K, 3
	bgt+	.L15
#endif

#ifndef TRMMKERNEL
	LFPDUX	A1, CO1, INC2
	fpmr	f5,  f0
	LFPDUX	B1, CO1, INC2
	fpmr	f9,  f0
	LFPDUX	A3, CO1, INC2
	fpmr	f13, f0
   	LFPDUX	A5, CO1, INC2
	fpmr	f2,  f0

	LFPDUX	B3, CO2, INC2
	fpmr	f6,  f0
	LFPDUX	A6, CO2, INC2
	fpmr	f10, f0
	LFPDUX	A7, CO2, INC2
	fpmr	f14, f0
	LFPDUX	B2, CO2, INC2
	fpmr	f3,  f0
#else
	fpmr	f5,  f0
	fpmr	f9,  f0
	fpmr	f13, f0
	fpmr	f2,  f0

	fpmr	f6,  f0
	fpmr	f10, f0
	fpmr	f14, f0
	fpmr	f3,  f0
#endif

	fpmr	f7,  f0
	fpmr	f11, f0
	fpmr	f15, f0
	.align 4

.L15:
	LFPDUX	A2,  AO,  INC4
	LFPDUX	A4,  AO2, INC4
	LFPDUX	A10, BO,  INC4
	LFPDUX	B4,  BO2, INC4
	bdz-	.L17
	.align 4

.L16:
	FXCPMADD	f0,  A10, A2, f0
	FXCSMADD	f4,  A10, A2, f4
	FXCPMADD	f8,  B4, A2, f8
	FXCSMADD	f12, B4, A2, f12
	LFPDUX	A2, AO,  INC4

	FXCPMADD	f1,  A10, A4, f1
	FXCSMADD	f5,  A10, A4, f5
	FXCPMADD	f9,  B4, A4, f9
	FXCSMADD	f13, B4, A4, f13
	LFPDUX	A4, AO2, INC4

	FXCPMADD	f2,  A10, A2, f2
	FXCSMADD	f6,  A10, A2, f6
	FXCPMADD	f10, B4, A2, f10
	FXCSMADD	f14, B4, A2, f14
	LFPDUX	A2, AO,  INC4

	FXCPMADD	f3,  A10, A4, f3
	FXCSMADD	f7,  A10, A4, f7
	LFPDUX	A10, BO,  INC4
	FXCPMADD	f11, B4, A4, f11
	FXCSMADD	f15, B4, A4, f15
	LFPDUX	A4, AO2, INC4
	LFPDUX	B4, BO2, INC4
	bdnz+	.L16
	.align 4

.L17:
	FXCPMADD	f0,  A10, A2, f0
	FXCSMADD	f4,  A10, A2, f4
	FXCPMADD	f8,  B4, A2, f8
	FXCSMADD	f12, B4, A2, f12
	LFPDUX	A2, AO,  INC4

	FXCPMADD	f1,  A10, A4, f1
	FXCSMADD	f5,  A10, A4, f5
	FXCPMADD	f9,  B4, A4, f9
	FXCSMADD	f13, B4, A4, f13
	LFPDUX	A4, AO2, INC4

	FXCPMADD	f2,  A10, A2, f2
	FXCSMADD	f6,  A10, A2, f6
	FXCPMADD	f10, B4, A2, f10
	FXCSMADD	f14, B4, A2, f14

	FXCPMADD	f3,  A10, A4, f3
	FXCSMADD	f7,  A10, A4, f7
	FXCPMADD	f11, B4, A4, f11
	FXCSMADD	f15, B4, A4, f15
	.align 4

.L18:
#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RN) || defined(RT) || defined(CN) || defined(CT)
	fpadd	f0, f0, f4
	fpadd	f8, f8, f12
	fpadd	f1, f1, f5
	fpadd	f9, f9, f13

	fpadd	f2,  f2,  f6
	fpadd	f10, f10, f14
	fpadd	f3,  f3,  f7
	fpadd	f11, f11, f15
#else
	fpsub	f0, f0, f4
	fpsub	f8, f8, f12
	fpsub	f1, f1, f5
	fpsub	f9, f9, f13

	fpsub	f2,  f2,  f6
	fpsub	f10, f10, f14
	fpsub	f3,  f3,  f7
	fpsub	f11, f11, f15
#endif

#ifndef TRMMKERNEL
	fxcpmadd A1,  f0, AP,  A1
	fxcpmadd B1,  f1, AP,  B1
	fxcpmadd A3,  f2, AP,  A3
	fxcpmadd A5,  f3, AP,  A5

	fxcxnpma f0,  f0, AP,  A1
	fxcpmadd B3,  f8,  AP,  B3
	fxcxnpma f1,  f1, AP,  B1
	fxcpmadd A6,  f9,  AP,  A6
	fxcxnpma f2,  f2, AP,  A3
	fxcpmadd A7,  f10, AP,  A7

	fxcxnpma f3,  f3, AP,  A5
	fxcpmadd B2,  f11, AP,  B2
	fxcxnpma f8,  f8,  AP,  B3
	STFPDUX	f0,  CO1, INCM7
	fxcxnpma f9,  f9,  AP,  A6
	STFPDUX	f1,  CO1, INC2
	fxcxnpma f10, f10, AP,  A7
	STFPDUX	f2,  CO1, INC2

	fxcxnpma f11, f11, AP,  B2
	STFPDUX	f3,  CO1, INC2
	STFPDUX	f8,  CO2, INCM7
	STFPDUX	f9,  CO2, INC2
	STFPDUX	f10, CO2, INC2
	STFPDUX	f11, CO2, INC2
#else
	fxcpmadd f12, f0,  AP,  f30
	fxcpmadd f13, f1,  AP,  f30
	fxcpmadd f14, f2,  AP,  f30
	fxcpmadd f15, f3,  AP,  f30

	fxcxnpma f0,  f0,  AP,  f12
	fxcxnpma f1,  f1,  AP,  f13
	fxcxnpma f2,  f2,  AP,  f14
	fxcxnpma f3,  f3,  AP,  f15

	fxcpmadd f16, f8,  AP,  f30
	fxcpmadd f17, f9,  AP,  f30
	fxcpmadd f18, f10, AP,  f30
	fxcpmadd f19, f11, AP,  f30

	fxcxnpma f8,  f8,  AP,  f16
	fxcxnpma f9,  f9,  AP,  f17
	fxcxnpma f10, f10, AP,  f18
	fxcxnpma f11, f11, AP,  f19

	STFPDUX	f0,  CO1, INC2
	STFPDUX	f1,  CO1, INC2
	STFPDUX	f2,  CO1, INC2
	STFPDUX	f3,  CO1, INC2

	STFPDUX	f8,  CO2, INC2
	STFPDUX	f9,  CO2, INC2
	STFPDUX	f10, CO2, INC2
	STFPDUX	f11, CO2, INC2

#endif


#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -4
#else
	addi	TEMP, TEMP, -2
#endif
	slwi	r0,   TEMP, 2 + ZBASE_SHIFT
	slwi	TEMP, TEMP, 1 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 4
#endif
#endif

	addic.	I, I, -1
	li	r0, FZERO

	lfpsx	f0, SP, r0
	bgt+	.L11
	.align 4

.L20:
	andi.	I, M,  2
	beq	.L30

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	AO2, AO,   2 * SIZE
	fpmr	f4,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f12, f0
#else
	slwi	TEMP, KK, 1 + ZBASE_SHIFT
	slwi	r0,   KK, 1 + ZBASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	AO2, AO,   2 * SIZE
	fpmr	f4,  f0
	addi	BO,  BO, - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, BO,   2 * SIZE
	fpmr	f12, f0

#endif
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 2
#endif
	srawi.	r0,  TEMP,  2
 	fpmr	f1,  f0
	fpmr	f5,  f0
	fpmr	f9,  f0
	mtspr	CTR, r0
	fpmr	f13, f0
	ble	.L24
#else
	addi	AO2, AO,   2 * SIZE
	fpmr	f4,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f12, f0

	srawi.	r0,  K,  2
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
	FXCPMADD	f0,  B1, A1, f0
	nop
	FXCSMADD	f4,  B1, A1, f4
	LFPDUX	A8,  AO2, INC4
	FXCPMADD	f8,  B2, A1, f8
	nop
	FXCSMADD	f12, B2, A1, f12
	LFPDUX	A1,   AO, INC4

	FXCPMADD	f1,  B1, A2, f1
	nop
	FXCSMADD	f5,  B1, A2, f5
	LFPDUX	B1,   BO, INC4
	FXCPMADD	f9,  B2, A2, f9
	nop
	FXCSMADD	f13, B2, A2, f13
	LFPDUX	B2,  BO2, INC4

	FXCPMADD	f0,  B3, A3, f0
	nop
	FXCSMADD	f4,  B3, A3, f4
	LFPDUX	A2,  AO2, INC4
	FXCPMADD	f8,  B4, A3, f8
	nop
	FXCSMADD	f12, B4, A3, f12
	LFPDUX	A3,   AO, INC4

	FXCPMADD	f1,  B3, A4, f1
	nop
	FXCSMADD	f5,  B3, A4, f5
	LFPDUX	B3,   BO, INC4
	FXCPMADD	f9,  B4, A4, f9
	nop
	FXCSMADD	f13, B4, A4, f13
	LFPDUX	B4,  BO2, INC4

	FXCPMADD	f0,  B5, A5, f0
	nop
	FXCSMADD	f4,  B5, A5, f4
	LFPDUX	A4,  AO2, INC4
	FXCPMADD	f8,  B6, A5, f8
	nop
	FXCSMADD	f12, B6, A5, f12
	LFPDUX	A5,   AO, INC4

	FXCPMADD	f1,  B5, A6, f1
	nop
	FXCSMADD	f5,  B5, A6, f5
	LFPDUX	B5,   BO, INC4
	FXCPMADD	f9,  B6, A6, f9
	nop
	FXCSMADD	f13, B6, A6, f13
	LFPDUX	B6,  BO2, INC4

	FXCPMADD	f0,  A9,  A7, f0
	nop
	FXCSMADD	f4,  A9,  A7, f4
	LFPDUX	A6,  AO2, INC4
	FXCPMADD	f8,  A10, A7, f8
	nop
	FXCSMADD	f12, A10, A7, f12
	LFPDUX	A7,   AO, INC4

	FXCPMADD	f1,  A9,  A8, f1
	nop
	FXCSMADD	f5,  A9,  A8, f5
	LFPDUX	A9,   BO, INC4
	FXCPMADD	f9,  A10, A8, f9
	nop
	FXCSMADD	f13, A10, A8, f13
	LFPDUX	A10, BO2, INC4
	bdnz+	.L22
	.align 4

.L23:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f4,  B1, A1, f4
	LFPDUX	A8,  AO2, INC4
	FXCPMADD	f8,  B2, A1, f8
	FXCSMADD	f12, B2, A1, f12

	FXCPMADD	f1,  B1, A2, f1
	FXCSMADD	f5,  B1, A2, f5
	FXCPMADD	f9,  B2, A2, f9
	FXCSMADD	f13, B2, A2, f13

	FXCPMADD	f0,  B3, A3, f0
	FXCSMADD	f4,  B3, A3, f4
	FXCPMADD	f8,  B4, A3, f8
	FXCSMADD	f12, B4, A3, f12

	FXCPMADD	f1,  B3, A4, f1
	FXCSMADD	f5,  B3, A4, f5
	FXCPMADD	f9,  B4, A4, f9
	FXCSMADD	f13, B4, A4, f13

	FXCPMADD	f0,  B5, A5, f0
	FXCSMADD	f4,  B5, A5, f4
	FXCPMADD	f8,  B6, A5, f8
	FXCSMADD	f12, B6, A5, f12

	FXCPMADD	f1,  B5, A6, f1
	FXCSMADD	f5,  B5, A6, f5
	FXCPMADD	f9,  B6, A6, f9
	FXCSMADD	f13, B6, A6, f13

	FXCPMADD	f0,  A9, A7, f0
	FXCSMADD	f4,  A9, A7, f4
	FXCPMADD	f8,  A10, A7, f8
	FXCSMADD	f12, A10, A7, f12

	FXCPMADD	f1,  A9, A8, f1
	FXCSMADD	f5,  A9, A8, f5
	FXCPMADD	f9,  A10, A8, f9
	FXCSMADD	f13, A10, A8, f13
	.align 4

.L24:
	li	r0, ALPHA
	lfpdx	AP,  SP, r0
#ifdef TRMMKERNEL
	li	r0, FZERO
	lfpsx	f30, SP, r0
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 2
#endif
	andi.	r0,  TEMP,  3
	mtspr	CTR, r0
#else
	andi.	r0,  K,  3
	mtspr	CTR, r0
#endif
	ble+	.L28

	LFPDUX	A1,  AO,  INC4
	LFPDUX	A2,  AO2, INC4
	LFPDUX	B1,  BO,  INC4
	LFPDUX	B2,  BO2, INC4
	bdz-	.L27
	.align 4

.L26:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f4,  B1, A1, f4
	FXCPMADD	f8,  B2, A1, f8
	FXCSMADD	f12, B2, A1, f12
	LFPDUX	A1,  AO,  INC4

	FXCPMADD	f1,  B1, A2, f1
	FXCSMADD	f5,  B1, A2, f5
	LFPDUX	B1,  BO,  INC4
	FXCPMADD	f9,  B2, A2, f9
	FXCSMADD	f13, B2, A2, f13
	LFPDUX	A2,  AO2, INC4
	LFPDUX	B2,  BO2, INC4
	bdnz+	.L26
	.align 4

.L27:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f4,  B1, A1, f4
	FXCPMADD	f8,  B2, A1, f8
	FXCSMADD	f12, B2, A1, f12

	FXCPMADD	f1,  B1, A2, f1
	FXCSMADD	f5,  B1, A2, f5
	FXCPMADD	f9,  B2, A2, f9
	FXCSMADD	f13, B2, A2, f13
	.align 4

.L28:
#ifndef TRMMKERNEL
	LFPDUX	A1, CO1, INC2
	LFPDUX	A2, CO1, INC2
	LFPDUX	A3, CO2, INC2
	LFPDUX	A4, CO2, INC2
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RN) || defined(RT) || defined(CN) || defined(CT)
	fpadd	f0, f0, f4
	fpadd	f8, f8, f12
	fpadd	f1, f1, f5
	fpadd	f9, f9, f13
#else
	fpsub	f0, f0, f4
	fpsub	f8, f8, f12
	fpsub	f1, f1, f5
	fpsub	f9, f9, f13
#endif

#ifndef TRMMKERNEL
	fxcpmadd A1,  f0, AP,  A1
	fxcpmadd A2,  f1, AP,  A2
	fxcpmadd A3,  f8, AP,  A3
	fxcpmadd A4,  f9, AP,  A4

	fxcxnpma f0,  f0, AP,  A1
	fxcxnpma f1,  f1, AP,  A2
	fxcxnpma f8,  f8, AP,  A3
	fxcxnpma f9,  f9, AP,  A4

	STFPDUX	f0,  CO1, INCM3
	STFPDUX	f1,  CO1, INC2

	STFPDUX	f8,  CO2, INCM3
	STFPDUX	f9,  CO2, INC2
#else
	fxcpmadd f12,  f0, AP,  f30
	fxcpmadd f13,  f1, AP,  f30
	fxcpmadd f14,  f8, AP,  f30
	fxcpmadd f15,  f9, AP,  f30

	fxcxnpma f0,  f0, AP,  f12
	fxcxnpma f1,  f1, AP,  f13
	fxcxnpma f8,  f8, AP,  f14
	fxcxnpma f9,  f9, AP,  f15

	STFPDUX	f0,  CO1, INC2
	STFPDUX	f1,  CO1, INC2

	STFPDUX	f8,  CO2, INC2
	STFPDUX	f9,  CO2, INC2
#endif


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

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L30:
	andi.	I, M,  1
	beq	.L49

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	AO2, AO,   2 * SIZE
	fpmr	f1,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f2,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f3, f0
#else
	slwi	TEMP, KK, 0 + ZBASE_SHIFT
	slwi	r0,   KK, 1 + ZBASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	AO2, AO,   2 * SIZE
	fpmr	f1,  f0
	addi	BO,  BO,  - 4 * SIZE
	fpmr	f2,  f0
	addi	BO2, BO,    2 * SIZE
	fpmr	f3, f0
#endif
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 2
#endif
	srawi.	r0,  TEMP,  2
	mtspr	CTR, r0
	ble	.L34
#else
	addi	AO2, AO,   2 * SIZE
	fpmr	f1,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f2,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f3, f0

	srawi.	r0,  K,  2
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
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f1,  B1, A1, f1
	LFPDUX	B1,  BO, INC4
	FXCPMADD	f2,  B2, A1, f2
	FXCSMADD	f3,  B2, A1, f3
	LFPDUX	B2, BO2, INC4
	LFPDUX	A1,  AO, INC4

	FXCPMADD	f0,  B3, A2, f0
	FXCSMADD	f1,  B3, A2, f1
	LFPDUX	B3,  BO, INC4
	FXCPMADD	f2,  B4, A2, f2
	FXCSMADD	f3,  B4, A2, f3
	LFPDUX	B4, BO2, INC4
	LFPDUX	A2, AO2, INC4

	FXCPMADD	f0,  A5, A3, f0
	FXCSMADD	f1,  A5, A3, f1
	LFPDUX	A5,  BO, INC4
	FXCPMADD	f2,  A6, A3, f2
	FXCSMADD	f3,  A6, A3, f3
	LFPDUX	A6, BO2, INC4
	LFPDUX	A3,  AO, INC4

	FXCPMADD	f0,  A7, A4, f0
	FXCSMADD	f1,  A7, A4, f1
	LFPDUX	A7,  BO, INC4
	FXCPMADD	f2,  A8, A4, f2
	FXCSMADD	f3,  A8, A4, f3
	LFPDUX	A8, BO2, INC4
	LFPDUX	A4, AO2, INC4
	bdnz+	.L32
	.align 4

.L33:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f1,  B1, A1, f1
	FXCPMADD	f2,  B2, A1, f2
	FXCSMADD	f3,  B2, A1, f3

	FXCPMADD	f0,  B3, A2, f0
	FXCSMADD	f1,  B3, A2, f1
	FXCPMADD	f2,  B4, A2, f2
	FXCSMADD	f3,  B4, A2, f3

	FXCPMADD	f0,  A5, A3, f0
	FXCSMADD	f1,  A5, A3, f1
	FXCPMADD	f2,  A6, A3, f2
	FXCSMADD	f3,  A6, A3, f3

	FXCPMADD	f0,  A7, A4, f0
	FXCSMADD	f1,  A7, A4, f1
	FXCPMADD	f2,  A8, A4, f2
	FXCSMADD	f3,  A8, A4, f3
	.align 4

.L34:
	li	r0, ALPHA
	lfpdx	AP,  SP, r0
#ifdef TRMMKERNEL
	li	r0, FZERO
	lfpsx	f30, SP, r0
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 2
#endif
	andi.	r0,  TEMP,  3
	mtspr	CTR, r0
#else
	andi.	r0,  K,  3
	mtspr	CTR, r0
#endif
	ble+	.L38

	LFPDX	A1,  AO,  INC4
	LFPDUX	B1,  BO,  INC4
	LFPDUX	B2,  BO2, INC4
	add	AO, AO, INC2
	bdz-	.L37
	.align 4

.L36:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f1,  B1, A1, f1
	LFPDUX	B1,  BO,  INC4
	FXCPMADD	f2,  B2, A1, f2
	FXCSMADD	f3,  B2, A1, f3
	LFPDX	A1,  AO,  INC4
	LFPDUX	B2,  BO2, INC4
	add	AO, AO, INC2
	bdnz+	.L36
	.align 4

.L37:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f1,  B1, A1, f1
	FXCPMADD	f2,  B2, A1, f2
	FXCSMADD	f3,  B2, A1, f3
	.align 4

.L38:
#ifndef TRMMKERNEL
	LFPDX	A1, CO1, INC2
	LFPDX	A2, CO2, INC2
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RN) || defined(RT) || defined(CN) || defined(CT)
	fpadd	f0, f0, f1
	fpadd	f2, f2, f3
#else
	fpsub	f0, f0, f1
	fpsub	f2, f2, f3
#endif

#ifndef TRMMKERNEL
	fxcpmadd A1,  f0, AP,  A1
	fxcpmadd A2,  f2, AP,  A2
	fxcxnpma f0,  f0, AP,  A1
	fxcxnpma f2,  f2, AP,  A2
#else
	fxcpmadd f12, f0, AP,  f30
	fxcpmadd f13, f2, AP,  f30
	fxcxnpma f0,  f0, AP,  f12
	fxcxnpma f2,  f2, AP,  f13
#endif

	STFPDUX	f0,  CO1, INC2
	STFPDUX	f2,  CO2, INC2

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

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L49:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addi	KK, KK, 2
#endif

	addi	B,  BO, 4 * SIZE

	addic.	J, J, -1
	bgt+	.L10
	.align 4

.L50:
	andi.	J, N,  1
	beq	.L999

	mr	CO1, C

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mr	KK, OFFSET
#endif

	addi	AO, A, -2 * SIZE

	li	r0, FZERO
	lfpsx	f0, SP, r0

	srawi.	I, M,  2
	ble	.L60
	.align 4

.L51:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	fpmr	f4,  f0
	addi	BO,  B,  - 2 * SIZE
 	fpmr	f1,  f0
	fpmr	f5,  f0
	fpmr	f2,  f0
	fpmr	f6,  f0
#else
	slwi	TEMP, KK, 2 + ZBASE_SHIFT
	slwi	r0,   KK, 0 + ZBASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	fpmr	f4,  f0
	addi	BO,  BO,  - 2 * SIZE
 	fpmr	f1,  f0
	fpmr	f5,  f0
	fpmr	f2,  f0
	fpmr	f6,  f0
#endif
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 1
#endif
	srawi.	r0,  TEMP,  2
	fpmr	f3,  f0
	mtspr	CTR, r0
	fpmr	f7,  f0
	ble	.L54
#else
	srawi.	r0,  K,  2
	fpmr	f4,  f0
	addi	BO,  B,  - 2 * SIZE
 	fpmr	f1,  f0
	fpmr	f5,  f0
	fpmr	f2,  f0
	fpmr	f6,  f0
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
	FXCPMADD	f0,  B1, A1, f0
	LFPDUX	B4,  BO,  INC2
	FXCSMADD	f4,  B1, A1, f4
	LFPDUX	A1,  AO,  INC2
	FXCPMADD	f1,  B1, A2, f1
	nop
	FXCSMADD	f5,  B1, A2, f5
	LFPDUX	A2,  AO,  INC2

	FXCPMADD	f2,  B1, A3, f2
	nop
	FXCSMADD	f6,  B1, A3, f6
	LFPDUX	A3,  AO,  INC2
	FXCPMADD	f3,  B1, A4, f3
	nop
	FXCSMADD	f7,  B1, A4, f7
	LFPDUX	A4,  AO,  INC2

	FXCPMADD	f0,  B2, A5, f0
	LFPDUX	B1,  BO,  INC2
	FXCSMADD	f4,  B2, A5, f4
	LFPDUX	A5,  AO,  INC2
	FXCPMADD	f1,  B2, A6, f1
	nop
	FXCSMADD	f5,  B2, A6, f5
	LFPDUX	A6,  AO,  INC2

	FXCPMADD	f2,  B2, A7, f2
	nop
	FXCSMADD	f6,  B2, A7, f6
	LFPDUX	A7,  AO,  INC2
	FXCPMADD	f3,  B2, A8, f3
	nop
	FXCSMADD	f7,  B2, A8, f7
	LFPDUX	A8,  AO,  INC2

	FXCPMADD	f0,  B3, A1, f0
	LFPDUX	B2,  BO,  INC2
	FXCSMADD	f4,  B3, A1, f4
	LFPDUX	A1,  AO,  INC2
	FXCPMADD	f1,  B3, A2, f1
	nop
	FXCSMADD	f5,  B3, A2, f5
	LFPDUX	A2,  AO,  INC2

	FXCPMADD	f2,  B3, A3, f2
	nop
	FXCSMADD	f6,  B3, A3, f6
	LFPDUX	A3,  AO,  INC2
	FXCPMADD	f3,  B3, A4, f3
	nop
	FXCSMADD	f7,  B3, A4, f7
	LFPDUX	A4,  AO,  INC2

	FXCPMADD	f0,  B4, A5, f0
	LFPDUX	B3,  BO,  INC2
	FXCSMADD	f4,  B4, A5, f4
	LFPDUX	A5,  AO,  INC2
	FXCPMADD	f1,  B4, A6, f1
	nop
	FXCSMADD	f5,  B4, A6, f5
	LFPDUX	A6,  AO,  INC2

	FXCPMADD	f2,  B4, A7, f2
	nop
	FXCSMADD	f6,  B4, A7, f6
	LFPDUX	A7,  AO,  INC2
	FXCPMADD	f3,  B4, A8, f3
	nop
	FXCSMADD	f7,  B4, A8, f7
	LFPDUX	A8,  AO,  INC2
	bdnz+	.L52
	.align 4

.L53:
	FXCPMADD	f0,  B1, A1, f0
	LFPDUX	B4,  BO,  INC2
	FXCSMADD	f4,  B1, A1, f4
	LFPDUX	A1,  AO,  INC2
	FXCPMADD	f1,  B1, A2, f1
	nop
	FXCSMADD	f5,  B1, A2, f5
	LFPDUX	A2,  AO,  INC2

	FXCPMADD	f2,  B1, A3, f2
	nop
	FXCSMADD	f6,  B1, A3, f6
	LFPDUX	A3,  AO,  INC2
	FXCPMADD	f3,  B1, A4, f3
	nop
	FXCSMADD	f7,  B1, A4, f7
	LFPDUX	A4,  AO,  INC2

	FXCPMADD	f0,  B2, A5, f0
	nop
	FXCSMADD	f4,  B2, A5, f4
	LFPDUX	A5,  AO,  INC2
	FXCPMADD	f1,  B2, A6, f1
	nop
	FXCSMADD	f5,  B2, A6, f5
	LFPDUX	A6,  AO,  INC2

	FXCPMADD	f2,  B2, A7, f2
	nop
	FXCSMADD	f6,  B2, A7, f6
	LFPDUX	A7,  AO,  INC2
	FXCPMADD	f3,  B2, A8, f3
	nop
	FXCSMADD	f7,  B2, A8, f7
	LFPDUX	A8,  AO,  INC2

	FXCPMADD	f0,  B3, A1, f0
	FXCSMADD	f4,  B3, A1, f4
	FXCPMADD	f1,  B3, A2, f1
	FXCSMADD	f5,  B3, A2, f5

	FXCPMADD	f2,  B3, A3, f2
	FXCSMADD	f6,  B3, A3, f6
	FXCPMADD	f3,  B3, A4, f3
	FXCSMADD	f7,  B3, A4, f7

	FXCPMADD	f0,  B4, A5, f0
	FXCSMADD	f4,  B4, A5, f4
	FXCPMADD	f1,  B4, A6, f1
	FXCSMADD	f5,  B4, A6, f5

	FXCPMADD	f2,  B4, A7, f2
	FXCSMADD	f6,  B4, A7, f6
	FXCPMADD	f3,  B4, A8, f3
	FXCSMADD	f7,  B4, A8, f7
	.align 4

.L54:
	li	r0, ALPHA
	lfpdx	AP,  SP, r0
#ifdef TRMMKERNEL
	li	r0, FZERO
	lfpsx	f30, SP, r0
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 1
#endif
	andi.	r0,  TEMP,  3
	mtspr	CTR, r0
#else
	andi.	r0,  K,  3
	mtspr	CTR, r0
#endif
	ble+	.L58

	LFPDUX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	LFPDUX	A2,  AO,  INC2
	LFPDUX	A3,  AO,  INC2
	LFPDUX	A4,  AO,  INC2
	bdz-	.L57
	.align 4

.L56:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f4,  B1, A1, f4
	LFPDUX	A1,  AO,  INC2
	FXCPMADD	f1,  B1, A2, f1
	FXCSMADD	f5,  B1, A2, f5
	LFPDUX	A2,  AO,  INC2

	FXCPMADD	f2,  B1, A3, f2
	FXCSMADD	f6,  B1, A3, f6
	LFPDUX	A3,  AO,  INC2
	FXCPMADD	f3,  B1, A4, f3
	FXCSMADD	f7,  B1, A4, f7
	LFPDUX	A4,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	bdnz+	.L56
	.align 4

.L57:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f4,  B1, A1, f4
	FXCPMADD	f1,  B1, A2, f1
	FXCSMADD	f5,  B1, A2, f5

	FXCPMADD	f2,  B1, A3, f2
	FXCSMADD	f6,  B1, A3, f6
	FXCPMADD	f3,  B1, A4, f3
	FXCSMADD	f7,  B1, A4, f7
	.align 4

.L58:
#ifndef TRMMKERNEL
	LFPDUX	A1, CO1, INC2
	LFPDUX	A2, CO1, INC2
	LFPDUX	A3, CO1, INC2
   	LFPDUX	A4, CO1, INC2
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RN) || defined(RT) || defined(CN) || defined(CT)
	fpadd	f0, f0, f4
	fpadd	f1, f1, f5
	fpadd	f2, f2, f6
	fpadd	f3, f3, f7
#else
	fpsub	f0, f0, f4
	fpsub	f1, f1, f5
	fpsub	f2, f2, f6
	fpsub	f3, f3, f7
#endif

#ifndef TRMMKERNEL
	fxcpmadd A1,  f0, AP,  A1
	fxcpmadd A2,  f1, AP,  A2
	fxcpmadd A3,  f2, AP,  A3
	fxcpmadd A4,  f3, AP,  A4

	fxcxnpma f0,  f0, AP,  A1
	fxcxnpma f1,  f1, AP,  A2
	fxcxnpma f2,  f2, AP,  A3
	fxcxnpma f3,  f3, AP,  A4

	STFPDUX	f0,  CO1, INCM7
	STFPDUX	f1,  CO1, INC2
	STFPDUX	f2,  CO1, INC2
	STFPDUX	f3,  CO1, INC2
#else
	fxcpmadd f12, f0, AP,  f30
	fxcpmadd f13, f1, AP,  f30
	fxcpmadd f14, f2, AP,  f30
	fxcpmadd f15, f3, AP,  f30

	fxcxnpma f0,  f0, AP,  f12
	fxcxnpma f1,  f1, AP,  f13
	fxcxnpma f2,  f2, AP,  f14
	fxcxnpma f3,  f3, AP,  f15

	STFPDUX	f0,  CO1, INC2
	STFPDUX	f1,  CO1, INC2
	STFPDUX	f2,  CO1, INC2
	STFPDUX	f3,  CO1, INC2
#endif


#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -4
#else
	addi	TEMP, TEMP, -1
#endif
	slwi	r0,   TEMP, 2 + ZBASE_SHIFT
	slwi	TEMP, TEMP, 0 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 4
#endif
#endif

	addic.	I, I, -1
	li	r0, FZERO

	lfpsx	f0, SP, r0
	bgt+	.L51
	.align 4

.L60:
	andi.	I, M,  2
	beq	.L70

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	BO,  B,  - 2 * SIZE
 	fpmr	f1,  f0
#else
	slwi	TEMP, KK, 1 + ZBASE_SHIFT
	slwi	r0,   KK, 0 + ZBASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	BO,  BO,  - 2 * SIZE
 	fpmr	f1,  f0
#endif
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 1
#endif

	srawi.	r0,  TEMP,  2
	fpmr	f2,  f0
	mtspr	CTR, r0
	fpmr	f3,  f0
	ble	.L64

#else
	srawi.	r0,  K,  2
 	fpmr	f1,  f0
	addi	BO,  B,  - 2 * SIZE
	fpmr	f2,  f0
	mtspr	CTR, r0
	fpmr	f3,  f0
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
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f2,  B1, A1, f2
	LFPDUX	A1,  AO, INC2
	FXCPMADD	f1,  B1, A2, f1
	FXCSMADD	f3,  B1, A2, f3
	LFPDUX	A2,  AO, INC2
	LFPDUX	B1,  BO, INC2

	FXCPMADD	f0,  B2, A3, f0
	FXCSMADD	f2,  B2, A3, f2
	LFPDUX	A3,  AO, INC2
	FXCPMADD	f1,  B2, A4, f1
	FXCSMADD	f3,  B2, A4, f3
	LFPDUX	A4,  AO, INC2
	LFPDUX	B2,  BO, INC2

	FXCPMADD	f0,  B3, A5, f0
	FXCSMADD	f2,  B3, A5, f2
	LFPDUX	A5,  AO, INC2
	FXCPMADD	f1,  B3, A6, f1
	FXCSMADD	f3,  B3, A6, f3
	LFPDUX	A6,  AO, INC2
	LFPDUX	B3,  BO, INC2

	FXCPMADD	f0,  B4, A7, f0
	FXCSMADD	f2,  B4, A7, f2
	LFPDUX	A7,  AO, INC2
	FXCPMADD	f1,  B4, A8, f1
	FXCSMADD	f3,  B4, A8, f3
	LFPDUX	A8,  AO, INC2
	LFPDUX	B4,  BO, INC2
	bdnz+	.L62
	.align 4

.L63:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f2,  B1, A1, f2
	FXCPMADD	f1,  B1, A2, f1
	FXCSMADD	f3,  B1, A2, f3

	FXCPMADD	f0,  B2, A3, f0
	FXCSMADD	f2,  B2, A3, f2
	FXCPMADD	f1,  B2, A4, f1
	FXCSMADD	f3,  B2, A4, f3

	FXCPMADD	f0,  B3, A5, f0
	FXCSMADD	f2,  B3, A5, f2
	FXCPMADD	f1,  B3, A6, f1
	FXCSMADD	f3,  B3, A6, f3

	FXCPMADD	f0,  B4, A7, f0
	FXCSMADD	f2,  B4, A7, f2
	FXCPMADD	f1,  B4, A8, f1
	FXCSMADD	f3,  B4, A8, f3
	.align 4

.L64:
	li	r0, ALPHA
	lfpdx	AP,  SP, r0
#ifdef TRMMKERNEL
	li	r0, FZERO
	lfpsx	f30, SP, r0
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 1
#endif
	andi.	r0,  TEMP,  3
	mtspr	CTR, r0
#else
	andi.	r0,  K,  3
	mtspr	CTR, r0
#endif
	ble+	.L68

	LFPDUX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	LFPDUX	A2,  AO,  INC2
	bdz-	.L67
	.align 4

.L66:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f2,  B1, A1, f2
	LFPDUX	A1,  AO,  INC2
	FXCPMADD	f1,  B1, A2, f1
	FXCSMADD	f3,  B1, A2, f3
	LFPDUX	B1,  BO,  INC2
	LFPDUX	A2,  AO,  INC2
	bdnz+	.L66
	.align 4

.L67:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f2,  B1, A1, f2
	FXCPMADD	f1,  B1, A2, f1
	FXCSMADD	f3,  B1, A2, f3
	.align 4

.L68:
#ifndef TRMMKERNEL
	LFPDUX	A1, CO1, INC2
	LFPDUX	A2, CO1, INC2
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RN) || defined(RT) || defined(CN) || defined(CT)
	fpadd	f0, f0, f2
	fpadd	f1, f1, f3
#else
	fpsub	f0, f0, f2
	fpsub	f1, f1, f3
#endif

#ifndef TRMMKERNEL
	fxcpmadd A1,  f0, AP,  A1
	fxcpmadd A2,  f1, AP,  A2
	fxcxnpma f0,  f0, AP,  A1
	fxcxnpma f1,  f1, AP,  A2

	STFPDUX	f0,  CO1, INCM3
	STFPDUX	f1,  CO1, INC2
#else
	fxcpmadd f12, f0, AP,  f30
	fxcpmadd f13, f1, AP,  f30
	fxcxnpma f0,  f0, AP,  f12
	fxcxnpma f1,  f1, AP,  f13

	STFPDUX	f0,  CO1, INC2
	STFPDUX	f1,  CO1, INC2
#endif

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

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L70:
	andi.	I, M,  1
	beq	.L89

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
#else
	slwi	TEMP, KK, 0 + ZBASE_SHIFT
	slwi	r0,   KK, 0 + ZBASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	BO,  BO,  - 2 * SIZE
	fpmr	f1,  f0
#endif
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 1
#endif
	srawi.	r0,  TEMP,  3
	fpmr	f2,  f0
	mtspr	CTR, r0
	fpmr	f3,  f0
	ble	.L74
#else
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	srawi.	r0,  K,  3
	fpmr	f2,  f0
	mtspr	CTR, r0
	fpmr	f3,  f0
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
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f1,  B1, A1, f1
	LFPDUX	A1,  AO, INC2
	LFPDUX	B1,  BO, INC2
	FXCPMADD	f2,  B2, A2, f2
	FXCSMADD	f3,  B2, A2, f3
	LFPDUX	A2,  AO, INC2
	LFPDUX	B2,  BO, INC2

	FXCPMADD	f0,  B3, A3, f0
	FXCSMADD	f1,  B3, A3, f1
	LFPDUX	A3,  AO, INC2
	LFPDUX	B3,  BO, INC2
	FXCPMADD	f2,  B4, A4, f2
	FXCSMADD	f3,  B4, A4, f3
	LFPDUX	A4,  AO, INC2
	LFPDUX	B4,  BO, INC2

	FXCPMADD	f0,  B5, A5, f0
	FXCSMADD	f1,  B5, A5, f1
	LFPDUX	A5,  AO, INC2
	LFPDUX	B5,  BO, INC2
	FXCPMADD	f2,  B6, A6, f2
	FXCSMADD	f3,  B6, A6, f3
	LFPDUX	A6,  AO, INC2
	LFPDUX	B6,  BO, INC2

	FXCPMADD	f0,  A9,  A7, f0
	FXCSMADD	f1,  A9,  A7, f1
	LFPDUX	A7,  AO, INC2
	LFPDUX	A9,  BO, INC2
	FXCPMADD	f2,  A10, A8, f2
	FXCSMADD	f3,  A10, A8, f3
	LFPDUX	A8,  AO, INC2
	LFPDUX	A10, BO, INC2

	bdnz+	.L72
	.align 4

.L73:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f1,  B1, A1, f1
	FXCPMADD	f2,  B2, A2, f2
	FXCSMADD	f3,  B2, A2, f3

	FXCPMADD	f0,  B3, A3, f0
	FXCSMADD	f1,  B3, A3, f1
	FXCPMADD	f2,  B4, A4, f2
	FXCSMADD	f3,  B4, A4, f3

	FXCPMADD	f0,  B5, A5, f0
	FXCSMADD	f1,  B5, A5, f1
	FXCPMADD	f2,  B6, A6, f2
	FXCSMADD	f3,  B6, A6, f3

	FXCPMADD	f0,  A9,  A7, f0
	FXCSMADD	f1,  A9,  A7, f1
	FXCPMADD	f2,  A10, A8, f2
	FXCSMADD	f3,  A10, A8, f3
	.align 4

.L74:
	li	r0, ALPHA
	lfpdx	AP,  SP, r0
#ifdef TRMMKERNEL
	li	r0, FZERO
	lfpsx	f30, SP, r0
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 1
#endif
	andi.	r0,  TEMP,  7
	mtspr	CTR, r0
#else
	andi.	r0,  K,  7
	mtspr	CTR, r0
#endif
	ble+	.L78

	LFPDUX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	bdz-	.L77
	.align 4

.L76:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f1,  B1, A1, f1
	LFPDUX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	bdnz+	.L76
	.align 4

.L77:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f1,  B1, A1, f1
	.align 4

.L78:
#ifndef TRMMKERNEL
	LFPDX	A1, CO1, INC2
#endif

	fpadd	f0, f0, f2
	fpadd	f1, f1, f3

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RN) || defined(RT) || defined(CN) || defined(CT)
	fpadd	f0, f0, f1
#else
	fpsub	f0, f0, f1
#endif

#ifndef TRMMKERNEL
	fxcpmadd A1,  f0, AP,  A1
	fxcxnpma f0,  f0, AP,  A1
#else
	fxcpmadd f12, f0, AP,  f30
	fxcxnpma f0,  f0, AP,  f12
#endif

	STFPDUX	f0,  CO1, INC2

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L89:
	addi	B,  BO, 2 * SIZE
	.align 4

.L999:
	addi	SP, SP, 20

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
	.align 4


.L1000:
	li	INC,    1 * SIZE
	li	INC2,   2 * SIZE
	li	INC4,   4 * SIZE
	li	INCM1, -1 * SIZE
	li	INCM3, -3 * SIZE
	li	INCM5, -5 * SIZE
	li	INCM7, -7 * SIZE

	addi	C, C, - 1 * SIZE
	srawi.	J, N,  1
	ble	.L1050
	.align 4

.L1010:
	mr	CO1, C
	add	CO2, C,   LDC
	add	C,   CO2, LDC

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mr	KK, OFFSET
#endif

	addi	AO, A, -4 * SIZE

	li	r0, FZERO
	lfpsx	f0, SP, r0

	srawi.	I, M,  2
	ble	.L1020
	.align 4

.L1011:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	AO2, AO,   2 * SIZE
	fpmr	f4,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f12, f0
#else
	slwi	TEMP, KK, 2 + ZBASE_SHIFT
	slwi	r0,   KK, 1 + ZBASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	AO2, AO,   2 * SIZE
	fpmr	f4,  f0
	addi	BO,  BO, - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, BO,   2 * SIZE
	fpmr	f12, f0
#endif
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 2
#endif
	srawi.	r0,  TEMP,  2
 	fpmr	f1,  f0
	mtspr	CTR, r0
	ble	.L1014
#else
	addi	AO2, AO,   2 * SIZE
	fpmr	f4,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f12, f0

	srawi.	r0,  K,  2
 	fpmr	f1,  f0
	mtspr	CTR, r0
	ble	.L1014
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
	bdz-	.L1013
	.align 4

.L1012:

## 1 ##
	FXCPMADD	f0,  B1, A1, f0
	nop
	FXCSMADD	f4,  B1, A1, f4
	nop
	FXCPMADD	f8,  B2, A1, f8
	LFPDUX	B4, BO2, INC4
	FXCSMADD	f12, B2, A1, f12
	LFPDUX	B6,  BO, INC4

	FXCPMADD	f1,  B1, A2, f1
	nop
	FXCSMADD	f5,  B1, A2, f5
	LFPDUX	A4, AO2, INC4
	FXCPMADD	f9,  B2, A2, f9
	LFPDUX	A10, AO, INC4
	FXCSMADD	f13, B2, A2, f13
	nop

	FXCPMADD	f2,  B1, A3, f2
	nop
	FXCSMADD	f6,  B1, A3, f6
	nop
	FXCPMADD	f10, B2, A3, f10
	nop
	FXCSMADD	f14, B2, A3, f14
	nop

	FXCPMADD	f3,  B1, A4, f3
	nop
	FXCSMADD	f7,  B1, A4, f7
	LFPDUX	A2, AO2, INC4
	FXCPMADD	f11, B2, A4, f11
	LFPDUX	A1,  AO, INC4
	FXCSMADD	f15, B2, A4, f15
	nop

## 2 ##

	FXCPMADD	f0,  B3, A5, f0
	nop
	FXCSMADD	f4,  B3, A5, f4
	nop
	FXCPMADD	f8,  B4, A5, f8
	LFPDUX	B2, BO2, INC4
	FXCSMADD	f12, B4, A5, f12
	LFPDUX	B1,  BO, INC4

	FXCPMADD	f1,  B3, A2, f1
	nop
	FXCSMADD	f5,  B3, A2, f5
	LFPDUX	A4, AO2, INC4
	FXCPMADD	f9,  B4, A2, f9
	LFPDUX	A3,  AO, INC4
	FXCSMADD	f13, B4, A2, f13
	nop

	FXCPMADD	f2,  B3, A6, f2
	nop
	FXCSMADD	f6,  B3, A6, f6
	nop
	FXCPMADD	f10, B4, A6, f10
	nop
	FXCSMADD	f14, B4, A6, f14
	nop

	FXCPMADD	f3,  B3, A4, f3
	nop
	FXCSMADD	f7,  B3, A4, f7
	LFPDUX	A2, AO2, INC4
	FXCPMADD	f11, B4, A4, f11
	LFPDUX	A5,  AO, INC4
	FXCSMADD	f15, B4, A4, f15
	nop

## 3 ##

	FXCPMADD	f0,  B5, A7, f0
	nop
	FXCSMADD	f4,  B5, A7, f4
	nop
	FXCPMADD	f8,  B2, A7, f8
	LFPDUX	B4, BO2, INC4
	FXCSMADD	f12, B2, A7, f12
	LFPDUX	B3,  BO, INC4

	FXCPMADD	f1,  B5, A2, f1
	nop
	FXCSMADD	f5,  B5, A2, f5
	LFPDUX	A4, AO2, INC4
	FXCPMADD	f9,  B2, A2, f9
	LFPDUX	A6,  AO, INC4
	FXCSMADD	f13, B2, A2, f13
	nop

	FXCPMADD	f2,  B5, A8, f2
	nop
	FXCSMADD	f6,  B5, A8, f6
	nop
	FXCPMADD	f10, B2, A8, f10
	nop
	FXCSMADD	f14, B2, A8, f14
	nop

	FXCPMADD	f3,  B5, A4, f3
	nop
	FXCSMADD	f7,  B5, A4, f7
	LFPDUX	A2, AO2, INC4
	FXCPMADD	f11, B2, A4, f11
	LFPDUX	A7,  AO, INC4
	FXCSMADD	f15, B2, A4, f15
	nop

## 4 ##
	FXCPMADD	f0,  B6, A9, f0
	nop
	FXCSMADD	f4,  B6, A9, f4
	nop
	FXCPMADD	f8,  B4, A9, f8
	LFPDUX	B2, BO2, INC4
	FXCSMADD	f12, B4, A9, f12
	LFPDUX	B5,  BO, INC4

	FXCPMADD	f1,  B6, A2, f1
	nop
	FXCSMADD	f5,  B6, A2, f5
	LFPDUX	A4, AO2, INC4
	FXCPMADD	f9,  B4, A2, f9
	LFPDUX	A8,  AO, INC4
	FXCSMADD	f13, B4, A2, f13
	nop

	FXCPMADD	f2,  B6, A10, f2
	nop
	FXCSMADD	f6,  B6, A10, f6
	nop
	FXCPMADD	f10, B4, A10, f10
	nop
	FXCSMADD	f14, B4, A10, f14
	nop

	FXCPMADD	f3,  B6, A4, f3
	LFPDUX	A2, AO2, INC4
	FXCSMADD	f7,  B6, A4, f7
	LFPDUX	A9,  AO, INC4
	FXCPMADD	f11, B4, A4, f11
	nop
	FXCSMADD	f15, B4, A4, f15
	bdnz+	.L1012
	.align 4

.L1013:
## 1 ##

	FXCPMADD	f0,  B1, A1, f0
	nop
	FXCSMADD	f4,  B1, A1, f4
	nop
	FXCPMADD	f8,  B2, A1, f8
	LFPDUX	B4, BO2, INC4
	FXCSMADD	f12, B2, A1, f12
	LFPDUX	B6,  BO, INC4

	FXCPMADD	f1,  B1, A2, f1
	nop
	FXCSMADD	f5,  B1, A2, f5
	LFPDUX	A4, AO2, INC4
	FXCPMADD	f9,  B2, A2, f9
	LFPDUX	A10, AO, INC4
	FXCSMADD	f13, B2, A2, f13
	nop

	FXCPMADD	f2,  B1, A3, f2
	nop
	FXCSMADD	f6,  B1, A3, f6
	nop
	FXCPMADD	f10, B2, A3, f10
	nop
	FXCSMADD	f14, B2, A3, f14
	nop

	FXCPMADD	f3,  B1, A4, f3
	nop
	FXCSMADD	f7,  B1, A4, f7
	LFPDUX	A2, AO2, INC4
	FXCPMADD	f11, B2, A4, f11
#ifndef TRMMKERNEL
	LFDUX	A1, CO1, INC
#else
	nop
#endif
	FXCSMADD	f15, B2, A4, f15
	nop

## 2 ##

	FXCPMADD	f0,  B3, A5, f0
	nop
	FXCSMADD	f4,  B3, A5, f4
	nop
	FXCPMADD	f8,  B4, A5, f8
	LFPDUX	B2, BO2, INC4
	FXCSMADD	f12, B4, A5, f12
#ifndef TRMMKERNEL
	LFDUX	B1, CO1, INC2
#else
	nop
#endif

	FXCPMADD	f1,  B3, A2, f1
	nop
	FXCSMADD	f5,  B3, A2, f5
	LFPDUX	A4, AO2, INC4
	FXCPMADD	f9,  B4, A2, f9
#ifndef TRMMKERNEL
	LFDUX	A3, CO1, INC2
#else
	nop
#endif
	FXCSMADD	f13, B4, A2, f13
	nop

	FXCPMADD	f2,  B3, A6, f2
	nop
	FXCSMADD	f6,  B3, A6, f6
	nop
	FXCPMADD	f10, B4, A6, f10
	nop
	FXCSMADD	f14, B4, A6, f14
	nop

	FXCPMADD	f3,  B3, A4, f3
	nop
	FXCSMADD	f7,  B3, A4, f7
	LFPDUX	A2, AO2, INC4
	FXCPMADD	f11, B4, A4, f11
#ifndef TRMMKERNEL
   	LFDUX	A5, CO1, INC2
#else
	nop
#endif
	FXCSMADD	f15, B4, A4, f15
	nop

## 3 ##

	FXCPMADD	f0,  B5, A7, f0
	nop
	FXCSMADD	f4,  B5, A7, f4
	nop
	FXCPMADD	f8,  B2, A7, f8
	LFPDUX	B4, BO2, INC4
	FXCSMADD	f12, B2, A7, f12
#ifndef TRMMKERNEL
	LFSDUX	A1, CO1, INCM5
#else
	nop
#endif

	FXCPMADD	f1,  B5, A2, f1
	nop
	FXCSMADD	f5,  B5, A2, f5
	LFPDUX	A4, AO2, INC4
	FXCPMADD	f9,  B2, A2, f9
#ifndef TRMMKERNEL
	LFSDUX	B1, CO1, INC2
#else
	nop
#endif
	FXCSMADD	f13, B2, A2, f13
	nop

	FXCPMADD	f2,  B5, A8, f2
	nop
	FXCSMADD	f6,  B5, A8, f6
	nop
	FXCPMADD	f10, B2, A8, f10
	nop
	FXCSMADD	f14, B2, A8, f14
	nop

	FXCPMADD	f3,  B5, A4, f3
	nop
	FXCSMADD	f7,  B5, A4, f7
	LFPDUX	A2, AO2, INC4
	FXCPMADD	f11, B2, A4, f11
#ifndef TRMMKERNEL
	LFSDUX	A3, CO1, INC2
#else
	nop
#endif
	FXCSMADD	f15, B2, A4, f15
	nop

## 4 ##

	FXCPMADD	f0,  B6, A9, f0
	nop
	FXCSMADD	f4,  B6, A9, f4
	nop
	FXCPMADD	f8,  B4, A9, f8
#ifndef TRMMKERNEL
	LFSDUX	A5, CO1, INC2
#else
	nop
#endif
	FXCSMADD	f12, B4, A9, f12
#ifndef TRMMKERNEL
	LFDUX	B3, CO2, INC
#else
	nop
#endif

	FXCPMADD	f1,  B6, A2, f1
	nop
	FXCSMADD	f5,  B6, A2, f5
	LFPDUX	A4, AO2, INC4
	FXCPMADD	f9,  B4, A2, f9
#ifndef TRMMKERNEL
	LFDUX	A6, CO2, INC2
#else
	nop
#endif
	FXCSMADD	f13, B4, A2, f13
	nop

	FXCPMADD	f2,  B6, A10, f2
	nop
	FXCSMADD	f6,  B6, A10, f6
	nop
	FXCPMADD	f10, B4, A10, f10
	nop
	FXCSMADD	f14, B4, A10, f14
#ifndef TRMMKERNEL
	LFDUX	A7, CO2, INC2
#else
	nop
#endif

	FXCPMADD	f3,  B6, A4, f3
	nop
	FXCSMADD	f7,  B6, A4, f7
	nop
	FXCPMADD	f11, B4, A4, f11
	nop
	FXCSMADD	f15, B4, A4, f15
#ifndef TRMMKERNEL
	LFDUX	B2, CO2, INC2
#else
	nop
#endif
	.align 4

.L1014:
	li	r0, ALPHA
	lfpdx	AP,  SP, r0
#ifdef TRMMKERNEL
	li	r0, FZERO
	lfpsx	f30, SP, r0
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 2
#endif
	andi.	r0,  TEMP,  3
	mtspr	CTR, r0
	ble+	.L1018

	cmpwi	cr0, TEMP, 3
	bgt+	.L1015
#else
	andi.	r0,  K,  3
	mtspr	CTR, r0
	ble+	.L1018

	cmpwi	cr0, K, 3
	bgt+	.L1015
#endif

#ifndef TRMMKERNEL
	LFDUX	A1, CO1, INC
	fpmr	f5,  f0
	LFDUX	B1, CO1, INC2
	fpmr	f9,  f0
	LFDUX	A3, CO1, INC2
	fpmr	f13, f0
   	LFDUX	A5, CO1, INC2
	fpmr	f2,  f0

	LFSDUX	A1, CO1, INCM5
	fpmr	f6,  f0
	LFSDUX	B1, CO1, INC2
	fpmr	f10, f0
	LFSDUX	A3, CO1, INC2
	fpmr	f14, f0
	LFSDUX	A5, CO1, INC2
	fpmr	f3,  f0

	LFDUX	B3, CO2, INC
	fpmr	f7,  f0
	LFDUX	A6, CO2, INC2
	fpmr	f11, f0
	LFDUX	A7, CO2, INC2
	fpmr	f15, f0
	LFDUX	B2, CO2, INC2
#else
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
#endif
	.align 4

.L1015:
	LFPDUX	A2,  AO,  INC4
	LFPDUX	A4,  AO2, INC4
	LFPDUX	A10, BO,  INC4
	LFPDUX	B4,  BO2, INC4
	bdz-	.L1017
	.align 4

.L1016:
	FXCPMADD	f0,  A10, A2, f0
	FXCSMADD	f4,  A10, A2, f4
	FXCPMADD	f8,  B4, A2, f8
	FXCSMADD	f12, B4, A2, f12
	LFPDUX	A2, AO,  INC4

	FXCPMADD	f1,  A10, A4, f1
	FXCSMADD	f5,  A10, A4, f5
	FXCPMADD	f9,  B4, A4, f9
	FXCSMADD	f13, B4, A4, f13
	LFPDUX	A4, AO2, INC4

	FXCPMADD	f2,  A10, A2, f2
	FXCSMADD	f6,  A10, A2, f6
	FXCPMADD	f10, B4, A2, f10
	FXCSMADD	f14, B4, A2, f14
	LFPDUX	A2, AO,  INC4

	FXCPMADD	f3,  A10, A4, f3
	FXCSMADD	f7,  A10, A4, f7
	LFPDUX	A10, BO,  INC4
	FXCPMADD	f11, B4, A4, f11
	FXCSMADD	f15, B4, A4, f15
	LFPDUX	A4, AO2, INC4
	LFPDUX	B4, BO2, INC4
	bdnz+	.L1016
	.align 4

.L1017:
	FXCPMADD	f0,  A10, A2, f0
	FXCSMADD	f4,  A10, A2, f4
	FXCPMADD	f8,  B4, A2, f8
	FXCSMADD	f12, B4, A2, f12
	LFPDUX	A2, AO,  INC4

	FXCPMADD	f1,  A10, A4, f1
	FXCSMADD	f5,  A10, A4, f5
	FXCPMADD	f9,  B4, A4, f9
	FXCSMADD	f13, B4, A4, f13
	LFPDUX	A4, AO2, INC4

	FXCPMADD	f2,  A10, A2, f2
	FXCSMADD	f6,  A10, A2, f6
	FXCPMADD	f10, B4, A2, f10
	FXCSMADD	f14, B4, A2, f14

	FXCPMADD	f3,  A10, A4, f3
	FXCSMADD	f7,  A10, A4, f7
	FXCPMADD	f11, B4, A4, f11
	FXCSMADD	f15, B4, A4, f15
	.align 4

.L1018:
#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RN) || defined(RT) || defined(CN) || defined(CT)
	fpadd	f0, f0, f4
	fpadd	f8, f8, f12
	fpadd	f1, f1, f5
	fpadd	f9, f9, f13

	fpadd	f2,  f2,  f6
	fpadd	f10, f10, f14
	fpadd	f3,  f3,  f7
	fpadd	f11, f11, f15
#else
	fpsub	f0, f0, f4
	fpsub	f8, f8, f12
	fpsub	f1, f1, f5
	fpsub	f9, f9, f13

	fpsub	f2,  f2,  f6
	fpsub	f10, f10, f14
	fpsub	f3,  f3,  f7
	fpsub	f11, f11, f15
#endif

#ifndef TRMMKERNEL
	fxcpmadd A1,  f0, AP,  A1
	LFSDUX	B3, CO2, INCM5
	fxcpmadd B1,  f1, AP,  B1
	LFSDUX	A6, CO2, INC2
	fxcpmadd A3,  f2, AP,  A3
	LFSDUX	A7, CO2, INC2
	fxcpmadd A5,  f3, AP,  A5
	LFSDUX	B2, CO2, INC2

	fxcxnpma f0,  f0, AP,  A1
	fxcpmadd B3,  f8,  AP,  B3
	fxcxnpma f1,  f1, AP,  B1
	fxcpmadd A6,  f9,  AP,  A6
	fxcxnpma f2,  f2, AP,  A3
	fxcpmadd A7,  f10, AP,  A7

	fxcxnpma f3,  f3, AP,  A5
	STFDUX	f0,  CO1, INCM7
	fxcpmadd B2,  f11, AP,  B2
	STFSDUX	f0,  CO1, INC
	fxcxnpma f8,  f8,  AP,  B3
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC
	fxcxnpma f9,  f9,  AP,  A6
	STFDUX	f2,  CO1, INC
	STFSDUX	f2,  CO1, INC
	fxcxnpma f10, f10, AP,  A7
	STFDUX	f3,  CO1, INC
	STFSDUX	f3,  CO1, INC

	fxcxnpma f11, f11, AP,  B2
	STFDUX	f8,  CO2, INCM7
#else
	fxcpmadd f12, f0, AP,  f30
	fxcpmadd f13, f1, AP,  f30
	fxcpmadd f14, f2, AP,  f30
	fxcpmadd f15, f3, AP,  f30

	fxcxnpma f0,  f0, AP,  f12
	fxcxnpma f1,  f1, AP,  f13
	fxcxnpma f2,  f2, AP,  f14
	fxcxnpma f3,  f3, AP,  f15

	fxcpmadd f16, f8,  AP, f30
	fxcpmadd f17, f9,  AP, f30
	fxcpmadd f18, f10, AP, f30
	fxcpmadd f19, f11, AP, f30

	fxcxnpma f8,  f8,  AP, f16
	fxcxnpma f9,  f9,  AP, f17
	fxcxnpma f10, f10, AP, f18
	fxcxnpma f11, f11, AP, f19

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC
	STFDUX	f2,  CO1, INC
	STFSDUX	f2,  CO1, INC
	STFDUX	f3,  CO1, INC
	STFSDUX	f3,  CO1, INC
	STFDUX	f8,  CO2, INC
#endif
	STFSDUX	f8,  CO2, INC
	STFDUX	f9,  CO2, INC
	STFSDUX	f9,  CO2, INC
	STFDUX	f10, CO2, INC
	STFSDUX	f10, CO2, INC

	STFDUX	f11, CO2, INC
	STFSDUX	f11, CO2, INC



#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -4
#else
	addi	TEMP, TEMP, -2
#endif
	slwi	r0,   TEMP, 2 + ZBASE_SHIFT
	slwi	TEMP, TEMP, 1 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 4
#endif
#endif

	addic.	I, I, -1
	li	r0, FZERO

	lfpsx	f0, SP, r0
	bgt+	.L1011
	.align 4

.L1020:
	andi.	I, M,  2
	beq	.L1030

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	AO2, AO,   2 * SIZE
	fpmr	f4,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f12, f0
#else
	slwi	TEMP, KK, 1 + ZBASE_SHIFT
	slwi	r0,   KK, 1 + ZBASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	AO2, AO,   2 * SIZE
	fpmr	f4,  f0
	addi	BO,  BO, - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, BO,   2 * SIZE
	fpmr	f12, f0

#endif
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 2
#endif
	srawi.	r0,  TEMP,  2
 	fpmr	f1,  f0
	fpmr	f5,  f0
	fpmr	f9,  f0
	mtspr	CTR, r0
	fpmr	f13, f0
	ble	.L1024
#else
	addi	AO2, AO,   2 * SIZE
	fpmr	f4,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f12, f0

	srawi.	r0,  K,  2
 	fpmr	f1,  f0
	fpmr	f5,  f0
	fpmr	f9,  f0
	mtspr	CTR, r0
	fpmr	f13, f0
	ble	.L1024
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
	bdz-	.L1023
	.align 4

.L1022:
	FXCPMADD	f0,  B1, A1, f0
	nop
	FXCSMADD	f4,  B1, A1, f4
	LFPDUX	A8,  AO2, INC4
	FXCPMADD	f8,  B2, A1, f8
	nop
	FXCSMADD	f12, B2, A1, f12
	LFPDUX	A1,   AO, INC4

	FXCPMADD	f1,  B1, A2, f1
	nop
	FXCSMADD	f5,  B1, A2, f5
	LFPDUX	B1,   BO, INC4
	FXCPMADD	f9,  B2, A2, f9
	nop
	FXCSMADD	f13, B2, A2, f13
	LFPDUX	B2,  BO2, INC4

	FXCPMADD	f0,  B3, A3, f0
	nop
	FXCSMADD	f4,  B3, A3, f4
	LFPDUX	A2,  AO2, INC4
	FXCPMADD	f8,  B4, A3, f8
	nop
	FXCSMADD	f12, B4, A3, f12
	LFPDUX	A3,   AO, INC4

	FXCPMADD	f1,  B3, A4, f1
	nop
	FXCSMADD	f5,  B3, A4, f5
	LFPDUX	B3,   BO, INC4
	FXCPMADD	f9,  B4, A4, f9
	nop
	FXCSMADD	f13, B4, A4, f13
	LFPDUX	B4,  BO2, INC4

	FXCPMADD	f0,  B5, A5, f0
	nop
	FXCSMADD	f4,  B5, A5, f4
	LFPDUX	A4,  AO2, INC4
	FXCPMADD	f8,  B6, A5, f8
	nop
	FXCSMADD	f12, B6, A5, f12
	LFPDUX	A5,   AO, INC4

	FXCPMADD	f1,  B5, A6, f1
	nop
	FXCSMADD	f5,  B5, A6, f5
	LFPDUX	B5,   BO, INC4
	FXCPMADD	f9,  B6, A6, f9
	nop
	FXCSMADD	f13, B6, A6, f13
	LFPDUX	B6,  BO2, INC4

	FXCPMADD	f0,  A9,  A7, f0
	nop
	FXCSMADD	f4,  A9,  A7, f4
	LFPDUX	A6,  AO2, INC4
	FXCPMADD	f8,  A10, A7, f8
	nop
	FXCSMADD	f12, A10, A7, f12
	LFPDUX	A7,   AO, INC4

	FXCPMADD	f1,  A9,  A8, f1
	nop
	FXCSMADD	f5,  A9,  A8, f5
	LFPDUX	A9,   BO, INC4
	FXCPMADD	f9,  A10, A8, f9
	nop
	FXCSMADD	f13, A10, A8, f13
	LFPDUX	A10, BO2, INC4
	bdnz+	.L1022
	.align 4

.L1023:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f4,  B1, A1, f4
	LFPDUX	A8,  AO2, INC4
	FXCPMADD	f8,  B2, A1, f8
	FXCSMADD	f12, B2, A1, f12

	FXCPMADD	f1,  B1, A2, f1
	FXCSMADD	f5,  B1, A2, f5
	FXCPMADD	f9,  B2, A2, f9
	FXCSMADD	f13, B2, A2, f13

	FXCPMADD	f0,  B3, A3, f0
	FXCSMADD	f4,  B3, A3, f4
	FXCPMADD	f8,  B4, A3, f8
	FXCSMADD	f12, B4, A3, f12

	FXCPMADD	f1,  B3, A4, f1
	FXCSMADD	f5,  B3, A4, f5
	FXCPMADD	f9,  B4, A4, f9
	FXCSMADD	f13, B4, A4, f13

	FXCPMADD	f0,  B5, A5, f0
	FXCSMADD	f4,  B5, A5, f4
	FXCPMADD	f8,  B6, A5, f8
	FXCSMADD	f12, B6, A5, f12

	FXCPMADD	f1,  B5, A6, f1
	FXCSMADD	f5,  B5, A6, f5
	FXCPMADD	f9,  B6, A6, f9
	FXCSMADD	f13, B6, A6, f13

	FXCPMADD	f0,  A9, A7, f0
	FXCSMADD	f4,  A9, A7, f4
	FXCPMADD	f8,  A10, A7, f8
	FXCSMADD	f12, A10, A7, f12

	FXCPMADD	f1,  A9, A8, f1
	FXCSMADD	f5,  A9, A8, f5
	FXCPMADD	f9,  A10, A8, f9
	FXCSMADD	f13, A10, A8, f13
	.align 4

.L1024:
	li	r0, ALPHA
	lfpdx	AP,  SP, r0
#ifdef TRMMKERNEL
	li	r0, FZERO
	lfpsx	f30, SP, r0
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 2
#endif
	andi.	r0,  TEMP,  3
	mtspr	CTR, r0
#else
	andi.	r0,  K,  3
	mtspr	CTR, r0
#endif
	ble+	.L1028

	LFPDUX	A1,  AO,  INC4
	LFPDUX	A2,  AO2, INC4
	LFPDUX	B1,  BO,  INC4
	LFPDUX	B2,  BO2, INC4
	bdz-	.L1027
	.align 4

.L1026:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f4,  B1, A1, f4
	FXCPMADD	f8,  B2, A1, f8
	FXCSMADD	f12, B2, A1, f12
	LFPDUX	A1,  AO,  INC4

	FXCPMADD	f1,  B1, A2, f1
	FXCSMADD	f5,  B1, A2, f5
	LFPDUX	B1,  BO,  INC4
	FXCPMADD	f9,  B2, A2, f9
	FXCSMADD	f13, B2, A2, f13
	LFPDUX	A2,  AO2, INC4
	LFPDUX	B2,  BO2, INC4
	bdnz+	.L1026
	.align 4

.L1027:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f4,  B1, A1, f4
	FXCPMADD	f8,  B2, A1, f8
	FXCSMADD	f12, B2, A1, f12

	FXCPMADD	f1,  B1, A2, f1
	FXCSMADD	f5,  B1, A2, f5
	FXCPMADD	f9,  B2, A2, f9
	FXCSMADD	f13, B2, A2, f13
	.align 4

.L1028:
#ifndef TRMMKERNEL
	LFDUX	A1, CO1, INC
	LFDUX	A2, CO1, INC2
	LFDUX	A3, CO2, INC
	LFDUX	A4, CO2, INC2

	LFSDUX	A1, CO1, INCM1
	LFSDUX	A2, CO1, INC2
	LFSDUX	A3, CO2, INCM1
	LFSDUX	A4, CO2, INC2
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RN) || defined(RT) || defined(CN) || defined(CT)
	fpadd	f0, f0, f4
	fpadd	f8, f8, f12
	fpadd	f1, f1, f5
	fpadd	f9, f9, f13
#else
	fpsub	f0, f0, f4
	fpsub	f8, f8, f12
	fpsub	f1, f1, f5
	fpsub	f9, f9, f13
#endif

#ifndef TRMMKERNEL
	fxcpmadd A1,  f0, AP,  A1
	fxcpmadd A2,  f1, AP,  A2
	fxcpmadd A3,  f8, AP,  A3
	fxcpmadd A4,  f9, AP,  A4

	fxcxnpma f0,  f0, AP,  A1
	fxcxnpma f1,  f1, AP,  A2
	fxcxnpma f8,  f8, AP,  A3
	fxcxnpma f9,  f9, AP,  A4

	STFDUX	f0,  CO1, INCM3
	STFSDUX	f0,  CO1, INC
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC

	STFDUX	f8,  CO2, INCM3
	STFSDUX	f8,  CO2, INC
	STFDUX	f9,  CO2, INC
	STFSDUX	f9,  CO2, INC
#else
	fxcpmadd f12, f0, AP,  f30
	fxcpmadd f13, f1, AP,  f30
	fxcpmadd f14, f8, AP,  f30
	fxcpmadd f15, f9, AP,  f30

	fxcxnpma f0,  f0, AP,  f12
	fxcxnpma f1,  f1, AP,  f13
	fxcxnpma f8,  f8, AP,  f14
	fxcxnpma f9,  f9, AP,  f15

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC

	STFDUX	f8,  CO2, INC
	STFSDUX	f8,  CO2, INC
	STFDUX	f9,  CO2, INC
	STFSDUX	f9,  CO2, INC
#endif

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

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L1030:
	andi.	I, M,  1
	beq	.L1049

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	AO2, AO,   2 * SIZE
	fpmr	f1,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f2,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f3, f0
#else
	slwi	TEMP, KK, 0 + ZBASE_SHIFT
	slwi	r0,   KK, 1 + ZBASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	AO2, AO,   2 * SIZE
	fpmr	f1,  f0
	addi	BO,  BO,  - 4 * SIZE
	fpmr	f2,  f0
	addi	BO2, BO,    2 * SIZE
	fpmr	f3, f0
#endif
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 2
#endif
	srawi.	r0,  TEMP,  2
	mtspr	CTR, r0
	ble	.L1034
#else
	addi	AO2, AO,   2 * SIZE
	fpmr	f1,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f2,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f3, f0

	srawi.	r0,  K,  2
	mtspr	CTR, r0
	ble	.L1034
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
	bdz-	.L1033
	.align 4

.L1032:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f1,  B1, A1, f1
	LFPDUX	B1,  BO, INC4
	FXCPMADD	f2,  B2, A1, f2
	FXCSMADD	f3,  B2, A1, f3
	LFPDUX	B2, BO2, INC4
	LFPDUX	A1,  AO, INC4

	FXCPMADD	f0,  B3, A2, f0
	FXCSMADD	f1,  B3, A2, f1
	LFPDUX	B3,  BO, INC4
	FXCPMADD	f2,  B4, A2, f2
	FXCSMADD	f3,  B4, A2, f3
	LFPDUX	B4, BO2, INC4
	LFPDUX	A2, AO2, INC4

	FXCPMADD	f0,  A5, A3, f0
	FXCSMADD	f1,  A5, A3, f1
	LFPDUX	A5,  BO, INC4
	FXCPMADD	f2,  A6, A3, f2
	FXCSMADD	f3,  A6, A3, f3
	LFPDUX	A6, BO2, INC4
	LFPDUX	A3,  AO, INC4

	FXCPMADD	f0,  A7, A4, f0
	FXCSMADD	f1,  A7, A4, f1
	LFPDUX	A7,  BO, INC4
	FXCPMADD	f2,  A8, A4, f2
	FXCSMADD	f3,  A8, A4, f3
	LFPDUX	A8, BO2, INC4
	LFPDUX	A4, AO2, INC4
	bdnz+	.L1032
	.align 4

.L1033:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f1,  B1, A1, f1
	FXCPMADD	f2,  B2, A1, f2
	FXCSMADD	f3,  B2, A1, f3

	FXCPMADD	f0,  B3, A2, f0
	FXCSMADD	f1,  B3, A2, f1
	FXCPMADD	f2,  B4, A2, f2
	FXCSMADD	f3,  B4, A2, f3

	FXCPMADD	f0,  A5, A3, f0
	FXCSMADD	f1,  A5, A3, f1
	FXCPMADD	f2,  A6, A3, f2
	FXCSMADD	f3,  A6, A3, f3

	FXCPMADD	f0,  A7, A4, f0
	FXCSMADD	f1,  A7, A4, f1
	FXCPMADD	f2,  A8, A4, f2
	FXCSMADD	f3,  A8, A4, f3
	.align 4

.L1034:
	li	r0, ALPHA
	lfpdx	AP,  SP, r0
#ifdef TRMMKERNEL
	li	r0, FZERO
	lfpsx	f30, SP, r0
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 2
#endif
	andi.	r0,  TEMP,  3
	mtspr	CTR, r0
#else
	andi.	r0,  K,  3
	mtspr	CTR, r0
#endif
	ble+	.L1038

	LFPDX	A1,  AO,  INC4
	LFPDUX	B1,  BO,  INC4
	LFPDUX	B2,  BO2, INC4
	add	AO, AO, INC2
	bdz-	.L1037
	.align 4

.L1036:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f1,  B1, A1, f1
	LFPDUX	B1,  BO,  INC4
	FXCPMADD	f2,  B2, A1, f2
	FXCSMADD	f3,  B2, A1, f3
	LFPDX	A1,  AO,  INC4
	LFPDUX	B2,  BO2, INC4
	add	AO, AO, INC2
	bdnz+	.L1036
	.align 4

.L1037:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f1,  B1, A1, f1
	FXCPMADD	f2,  B2, A1, f2
	FXCSMADD	f3,  B2, A1, f3
	.align 4

.L1038:
#ifndef TRMMKERNEL
	LFDUX	A1, CO1, INC
	LFDUX	A2, CO2, INC
	LFSDUX	A1, CO1, INC
	LFSDUX	A2, CO2, INC
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RN) || defined(RT) || defined(CN) || defined(CT)
	fpadd	f0, f0, f1
	fpadd	f2, f2, f3
#else
	fpsub	f0, f0, f1
	fpsub	f2, f2, f3
#endif

#ifndef TRMMKERNEL
	fxcpmadd A1,  f0, AP,  A1
	fxcpmadd A2,  f2, AP,  A2
	fxcxnpma f0,  f0, AP,  A1
	fxcxnpma f2,  f2, AP,  A2

	STFDUX	f0,  CO1, INCM1
	STFSDUX	f0,  CO1, INC

	STFDUX	f2,  CO2, INCM1
	STFSDUX	f2,  CO2, INC
#else
	fxcpmadd f12, f0, AP,  f30
	fxcpmadd f13, f2, AP,  f30
	fxcxnpma f0,  f0, AP,  f12
	fxcxnpma f2,  f2, AP,  f13

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC

	STFDUX	f2,  CO2, INC
	STFSDUX	f2,  CO2, INC
#endif

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

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L1049:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addi	KK, KK, 2
#endif

	addi	B,  BO, 4 * SIZE

	addic.	J, J, -1
	bgt+	.L1010
	.align 4

.L1050:
	andi.	J, N,  1
	beq	.L10999

	mr	CO1, C

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mr	KK, OFFSET
#endif

	addi	AO, A, -2 * SIZE

	li	r0, FZERO
	lfpsx	f0, SP, r0

	srawi.	I, M,  2
	ble	.L1060
	.align 4

.L1051:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	fpmr	f4,  f0
	addi	BO,  B,  - 2 * SIZE
 	fpmr	f1,  f0
	fpmr	f5,  f0
	fpmr	f2,  f0
	fpmr	f6,  f0
#else
	slwi	TEMP, KK, 2 + ZBASE_SHIFT
	slwi	r0,   KK, 0 + ZBASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	fpmr	f4,  f0
	addi	BO,  BO,  - 2 * SIZE
 	fpmr	f1,  f0
	fpmr	f5,  f0
	fpmr	f2,  f0
	fpmr	f6,  f0
#endif
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 1
#endif
	srawi.	r0,  TEMP,  2
	fpmr	f3,  f0
	mtspr	CTR, r0
	fpmr	f7,  f0
	ble	.L1054
#else
	srawi.	r0,  K,  2
	fpmr	f4,  f0
	addi	BO,  B,  - 2 * SIZE
 	fpmr	f1,  f0
	fpmr	f5,  f0
	fpmr	f2,  f0
	fpmr	f6,  f0
	fpmr	f3,  f0
	mtspr	CTR, r0
	fpmr	f7,  f0
	ble	.L1054
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
	bdz-	.L1053
	.align 4

.L1052:
	FXCPMADD	f0,  B1, A1, f0
	LFPDUX	B4,  BO,  INC2
	FXCSMADD	f4,  B1, A1, f4
	LFPDUX	A1,  AO,  INC2
	FXCPMADD	f1,  B1, A2, f1
	nop
	FXCSMADD	f5,  B1, A2, f5
	LFPDUX	A2,  AO,  INC2

	FXCPMADD	f2,  B1, A3, f2
	nop
	FXCSMADD	f6,  B1, A3, f6
	LFPDUX	A3,  AO,  INC2
	FXCPMADD	f3,  B1, A4, f3
	nop
	FXCSMADD	f7,  B1, A4, f7
	LFPDUX	A4,  AO,  INC2

	FXCPMADD	f0,  B2, A5, f0
	LFPDUX	B1,  BO,  INC2
	FXCSMADD	f4,  B2, A5, f4
	LFPDUX	A5,  AO,  INC2
	FXCPMADD	f1,  B2, A6, f1
	nop
	FXCSMADD	f5,  B2, A6, f5
	LFPDUX	A6,  AO,  INC2

	FXCPMADD	f2,  B2, A7, f2
	nop
	FXCSMADD	f6,  B2, A7, f6
	LFPDUX	A7,  AO,  INC2
	FXCPMADD	f3,  B2, A8, f3
	nop
	FXCSMADD	f7,  B2, A8, f7
	LFPDUX	A8,  AO,  INC2

	FXCPMADD	f0,  B3, A1, f0
	LFPDUX	B2,  BO,  INC2
	FXCSMADD	f4,  B3, A1, f4
	LFPDUX	A1,  AO,  INC2
	FXCPMADD	f1,  B3, A2, f1
	nop
	FXCSMADD	f5,  B3, A2, f5
	LFPDUX	A2,  AO,  INC2

	FXCPMADD	f2,  B3, A3, f2
	nop
	FXCSMADD	f6,  B3, A3, f6
	LFPDUX	A3,  AO,  INC2
	FXCPMADD	f3,  B3, A4, f3
	nop
	FXCSMADD	f7,  B3, A4, f7
	LFPDUX	A4,  AO,  INC2

	FXCPMADD	f0,  B4, A5, f0
	LFPDUX	B3,  BO,  INC2
	FXCSMADD	f4,  B4, A5, f4
	LFPDUX	A5,  AO,  INC2
	FXCPMADD	f1,  B4, A6, f1
	nop
	FXCSMADD	f5,  B4, A6, f5
	LFPDUX	A6,  AO,  INC2

	FXCPMADD	f2,  B4, A7, f2
	nop
	FXCSMADD	f6,  B4, A7, f6
	LFPDUX	A7,  AO,  INC2
	FXCPMADD	f3,  B4, A8, f3
	nop
	FXCSMADD	f7,  B4, A8, f7
	LFPDUX	A8,  AO,  INC2
	bdnz+	.L1052
	.align 4

.L1053:
	FXCPMADD	f0,  B1, A1, f0
	LFPDUX	B4,  BO,  INC2
	FXCSMADD	f4,  B1, A1, f4
	LFPDUX	A1,  AO,  INC2
	FXCPMADD	f1,  B1, A2, f1
	nop
	FXCSMADD	f5,  B1, A2, f5
	LFPDUX	A2,  AO,  INC2

	FXCPMADD	f2,  B1, A3, f2
	nop
	FXCSMADD	f6,  B1, A3, f6
	LFPDUX	A3,  AO,  INC2
	FXCPMADD	f3,  B1, A4, f3
	nop
	FXCSMADD	f7,  B1, A4, f7
	LFPDUX	A4,  AO,  INC2

	FXCPMADD	f0,  B2, A5, f0
	nop
	FXCSMADD	f4,  B2, A5, f4
	LFPDUX	A5,  AO,  INC2
	FXCPMADD	f1,  B2, A6, f1
	nop
	FXCSMADD	f5,  B2, A6, f5
	LFPDUX	A6,  AO,  INC2

	FXCPMADD	f2,  B2, A7, f2
	nop
	FXCSMADD	f6,  B2, A7, f6
	LFPDUX	A7,  AO,  INC2
	FXCPMADD	f3,  B2, A8, f3
	nop
	FXCSMADD	f7,  B2, A8, f7
	LFPDUX	A8,  AO,  INC2

	FXCPMADD	f0,  B3, A1, f0
	FXCSMADD	f4,  B3, A1, f4
	FXCPMADD	f1,  B3, A2, f1
	FXCSMADD	f5,  B3, A2, f5

	FXCPMADD	f2,  B3, A3, f2
	FXCSMADD	f6,  B3, A3, f6
	FXCPMADD	f3,  B3, A4, f3
	FXCSMADD	f7,  B3, A4, f7

	FXCPMADD	f0,  B4, A5, f0
	FXCSMADD	f4,  B4, A5, f4
	FXCPMADD	f1,  B4, A6, f1
	FXCSMADD	f5,  B4, A6, f5

	FXCPMADD	f2,  B4, A7, f2
	FXCSMADD	f6,  B4, A7, f6
	FXCPMADD	f3,  B4, A8, f3
	FXCSMADD	f7,  B4, A8, f7
	.align 4

.L1054:
	li	r0, ALPHA
	lfpdx	AP,  SP, r0
#ifdef TRMMKERNEL
	li	r0, FZERO
	lfpsx	f30, SP, r0
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 1
#endif
	andi.	r0,  TEMP,  3
	mtspr	CTR, r0
#else
	andi.	r0,  K,  3
	mtspr	CTR, r0
#endif
	ble+	.L1058

	LFPDUX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	LFPDUX	A2,  AO,  INC2
	LFPDUX	A3,  AO,  INC2
	LFPDUX	A4,  AO,  INC2
	bdz-	.L1057
	.align 4

.L1056:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f4,  B1, A1, f4
	LFPDUX	A1,  AO,  INC2
	FXCPMADD	f1,  B1, A2, f1
	FXCSMADD	f5,  B1, A2, f5
	LFPDUX	A2,  AO,  INC2

	FXCPMADD	f2,  B1, A3, f2
	FXCSMADD	f6,  B1, A3, f6
	LFPDUX	A3,  AO,  INC2
	FXCPMADD	f3,  B1, A4, f3
	FXCSMADD	f7,  B1, A4, f7
	LFPDUX	A4,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	bdnz+	.L1056
	.align 4

.L1057:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f4,  B1, A1, f4
	FXCPMADD	f1,  B1, A2, f1
	FXCSMADD	f5,  B1, A2, f5

	FXCPMADD	f2,  B1, A3, f2
	FXCSMADD	f6,  B1, A3, f6
	FXCPMADD	f3,  B1, A4, f3
	FXCSMADD	f7,  B1, A4, f7
	.align 4

.L1058:
#ifndef TRMMKERNEL
	LFDUX	A1, CO1, INC
	LFDUX	A2, CO1, INC2
	LFDUX	A3, CO1, INC2
   	LFDUX	A4, CO1, INC2

	LFSDUX	A1, CO1, INCM5
	LFSDUX	A2, CO1, INC2
	LFSDUX	A3, CO1, INC2
	LFSDUX	A4, CO1, INC2
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RN) || defined(RT) || defined(CN) || defined(CT)
	fpadd	f0, f0, f4
	fpadd	f1, f1, f5
	fpadd	f2, f2, f6
	fpadd	f3, f3, f7
#else
	fpsub	f0, f0, f4
	fpsub	f1, f1, f5
	fpsub	f2, f2, f6
	fpsub	f3, f3, f7
#endif

#ifndef TRMMKERNEL
	fxcpmadd A1,  f0, AP,  A1
	fxcpmadd A2,  f1, AP,  A2
	fxcpmadd A3,  f2, AP,  A3
	fxcpmadd A4,  f3, AP,  A4

	fxcxnpma f0,  f0, AP,  A1
	fxcxnpma f1,  f1, AP,  A2
	fxcxnpma f2,  f2, AP,  A3
	fxcxnpma f3,  f3, AP,  A4

	STFDUX	f0,  CO1, INCM7
	STFSDUX	f0,  CO1, INC

	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC

	STFDUX	f2,  CO1, INC
	STFSDUX	f2,  CO1, INC

	STFDUX	f3,  CO1, INC
	STFSDUX	f3,  CO1, INC
#else
	fxcpmadd f12, f0, AP,  f30
	fxcpmadd f13, f1, AP,  f30
	fxcpmadd f14, f2, AP,  f30
	fxcpmadd f15, f3, AP,  f30

	fxcxnpma f0,  f0, AP,  f12
	fxcxnpma f1,  f1, AP,  f13
	fxcxnpma f2,  f2, AP,  f14
	fxcxnpma f3,  f3, AP,  f15

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC

	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC

	STFDUX	f2,  CO1, INC
	STFSDUX	f2,  CO1, INC

	STFDUX	f3,  CO1, INC
	STFSDUX	f3,  CO1, INC
#endif


#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -4
#else
	addi	TEMP, TEMP, -1
#endif
	slwi	r0,   TEMP, 2 + ZBASE_SHIFT
	slwi	TEMP, TEMP, 0 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 4
#endif
#endif

	addic.	I, I, -1
	li	r0, FZERO

	lfpsx	f0, SP, r0
	bgt+	.L1051
	.align 4

.L1060:
	andi.	I, M,  2
	beq	.L1070

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	BO,  B,  - 2 * SIZE
 	fpmr	f1,  f0
#else
	slwi	TEMP, KK, 1 + ZBASE_SHIFT
	slwi	r0,   KK, 0 + ZBASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	BO,  BO,  - 2 * SIZE
 	fpmr	f1,  f0
#endif
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 1
#endif

	srawi.	r0,  TEMP,  2
	fpmr	f2,  f0
	mtspr	CTR, r0
	fpmr	f3,  f0
	ble	.L1064

#else
	srawi.	r0,  K,  2
 	fpmr	f1,  f0
	addi	BO,  B,  - 2 * SIZE
	fpmr	f2,  f0
	mtspr	CTR, r0
	fpmr	f3,  f0
	ble	.L1064
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
	bdz-	.L1063
	.align 4

.L1062:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f2,  B1, A1, f2
	LFPDUX	A1,  AO, INC2
	FXCPMADD	f1,  B1, A2, f1
	FXCSMADD	f3,  B1, A2, f3
	LFPDUX	A2,  AO, INC2
	LFPDUX	B1,  BO, INC2

	FXCPMADD	f0,  B2, A3, f0
	FXCSMADD	f2,  B2, A3, f2
	LFPDUX	A3,  AO, INC2
	FXCPMADD	f1,  B2, A4, f1
	FXCSMADD	f3,  B2, A4, f3
	LFPDUX	A4,  AO, INC2
	LFPDUX	B2,  BO, INC2

	FXCPMADD	f0,  B3, A5, f0
	FXCSMADD	f2,  B3, A5, f2
	LFPDUX	A5,  AO, INC2
	FXCPMADD	f1,  B3, A6, f1
	FXCSMADD	f3,  B3, A6, f3
	LFPDUX	A6,  AO, INC2
	LFPDUX	B3,  BO, INC2

	FXCPMADD	f0,  B4, A7, f0
	FXCSMADD	f2,  B4, A7, f2
	LFPDUX	A7,  AO, INC2
	FXCPMADD	f1,  B4, A8, f1
	FXCSMADD	f3,  B4, A8, f3
	LFPDUX	A8,  AO, INC2
	LFPDUX	B4,  BO, INC2
	bdnz+	.L1062
	.align 4

.L1063:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f2,  B1, A1, f2
	FXCPMADD	f1,  B1, A2, f1
	FXCSMADD	f3,  B1, A2, f3

	FXCPMADD	f0,  B2, A3, f0
	FXCSMADD	f2,  B2, A3, f2
	FXCPMADD	f1,  B2, A4, f1
	FXCSMADD	f3,  B2, A4, f3

	FXCPMADD	f0,  B3, A5, f0
	FXCSMADD	f2,  B3, A5, f2
	FXCPMADD	f1,  B3, A6, f1
	FXCSMADD	f3,  B3, A6, f3

	FXCPMADD	f0,  B4, A7, f0
	FXCSMADD	f2,  B4, A7, f2
	FXCPMADD	f1,  B4, A8, f1
	FXCSMADD	f3,  B4, A8, f3
	.align 4

.L1064:
	li	r0, ALPHA
	lfpdx	AP,  SP, r0
#ifdef TRMMKERNEL
	li	r0, FZERO
	lfpsx	f30, SP, r0
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 1
#endif
	andi.	r0,  TEMP,  3
	mtspr	CTR, r0
#else
	andi.	r0,  K,  3
	mtspr	CTR, r0
#endif
	ble+	.L1068

	LFPDUX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	LFPDUX	A2,  AO,  INC2
	bdz-	.L1067
	.align 4

.L1066:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f2,  B1, A1, f2
	LFPDUX	A1,  AO,  INC2
	FXCPMADD	f1,  B1, A2, f1
	FXCSMADD	f3,  B1, A2, f3
	LFPDUX	B1,  BO,  INC2
	LFPDUX	A2,  AO,  INC2
	bdnz+	.L1066
	.align 4

.L1067:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f2,  B1, A1, f2
	FXCPMADD	f1,  B1, A2, f1
	FXCSMADD	f3,  B1, A2, f3
	.align 4

.L1068:
#ifndef TRMMKERNEL
	LFDUX	A1, CO1, INC
	LFDUX	A2, CO1, INC2
	LFSDUX	A1, CO1, INCM1
	LFSDUX	A2, CO1, INC2
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RN) || defined(RT) || defined(CN) || defined(CT)
	fpadd	f0, f0, f2
	fpadd	f1, f1, f3
#else
	fpsub	f0, f0, f2
	fpsub	f1, f1, f3
#endif

#ifndef TRMMKERNEL
	fxcpmadd A1,  f0, AP,  A1
	fxcpmadd A2,  f1, AP,  A2
	fxcxnpma f0,  f0, AP,  A1
	fxcxnpma f1,  f1, AP,  A2

	STFDUX	f0,  CO1, INCM3
	STFSDUX	f0,  CO1, INC

	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC
#else
	fxcpmadd f12, f0, AP,  f30
	fxcpmadd f13, f1, AP,  f30
	fxcxnpma f0,  f0, AP,  f12
	fxcxnpma f1,  f1, AP,  f13

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC

	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC
#endif

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

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L1070:
	andi.	I, M,  1
	beq	.L1089

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
#else
	slwi	TEMP, KK, 0 + ZBASE_SHIFT
	slwi	r0,   KK, 0 + ZBASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	BO,  BO,  - 2 * SIZE
	fpmr	f1,  f0
#endif
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 1
#endif
	srawi.	r0,  TEMP,  3
	fpmr	f2,  f0
	mtspr	CTR, r0
	fpmr	f3,  f0
	ble	.L1074
#else
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	srawi.	r0,  K,  3
	fpmr	f2,  f0
	mtspr	CTR, r0
	fpmr	f3,  f0
	ble	.L1074
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
	bdz-	.L1073
	.align 4

.L1072:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f1,  B1, A1, f1
	LFPDUX	A1,  AO, INC2
	LFPDUX	B1,  BO, INC2
	FXCPMADD	f2,  B2, A2, f2
	FXCSMADD	f3,  B2, A2, f3
	LFPDUX	A2,  AO, INC2
	LFPDUX	B2,  BO, INC2

	FXCPMADD	f0,  B3, A3, f0
	FXCSMADD	f1,  B3, A3, f1
	LFPDUX	A3,  AO, INC2
	LFPDUX	B3,  BO, INC2
	FXCPMADD	f2,  B4, A4, f2
	FXCSMADD	f3,  B4, A4, f3
	LFPDUX	A4,  AO, INC2
	LFPDUX	B4,  BO, INC2

	FXCPMADD	f0,  B5, A5, f0
	FXCSMADD	f1,  B5, A5, f1
	LFPDUX	A5,  AO, INC2
	LFPDUX	B5,  BO, INC2
	FXCPMADD	f2,  B6, A6, f2
	FXCSMADD	f3,  B6, A6, f3
	LFPDUX	A6,  AO, INC2
	LFPDUX	B6,  BO, INC2

	FXCPMADD	f0,  A9,  A7, f0
	FXCSMADD	f1,  A9,  A7, f1
	LFPDUX	A7,  AO, INC2
	LFPDUX	A9,  BO, INC2
	FXCPMADD	f2,  A10, A8, f2
	FXCSMADD	f3,  A10, A8, f3
	LFPDUX	A8,  AO, INC2
	LFPDUX	A10, BO, INC2

	bdnz+	.L1072
	.align 4

.L1073:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f1,  B1, A1, f1
	FXCPMADD	f2,  B2, A2, f2
	FXCSMADD	f3,  B2, A2, f3

	FXCPMADD	f0,  B3, A3, f0
	FXCSMADD	f1,  B3, A3, f1
	FXCPMADD	f2,  B4, A4, f2
	FXCSMADD	f3,  B4, A4, f3

	FXCPMADD	f0,  B5, A5, f0
	FXCSMADD	f1,  B5, A5, f1
	FXCPMADD	f2,  B6, A6, f2
	FXCSMADD	f3,  B6, A6, f3

	FXCPMADD	f0,  A9,  A7, f0
	FXCSMADD	f1,  A9,  A7, f1
	FXCPMADD	f2,  A10, A8, f2
	FXCSMADD	f3,  A10, A8, f3
	.align 4

.L1074:
	li	r0, ALPHA
	lfpdx	AP,  SP, r0
#ifdef TRMMKERNEL
	li	r0, FZERO
	lfpsx	f30, SP, r0
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 1
#endif
	andi.	r0,  TEMP,  7
	mtspr	CTR, r0
#else
	andi.	r0,  K,  7
	mtspr	CTR, r0
#endif
	ble+	.L1078

	LFPDUX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	bdz-	.L1077
	.align 4

.L1076:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f1,  B1, A1, f1
	LFPDUX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	bdnz+	.L1076
	.align 4

.L1077:
	FXCPMADD	f0,  B1, A1, f0
	FXCSMADD	f1,  B1, A1, f1
	.align 4

.L1078:
#ifndef TRMMKERNEL
	LFDUX	A1, CO1, INC
	LFDUX	A2, CO1, INC
#endif

	fpadd	f0, f0, f2
	fpadd	f1, f1, f3

	fsmfp	A1, A2

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RN) || defined(RT) || defined(CN) || defined(CT)
	fpadd	f0, f0, f1
#else
	fpsub	f0, f0, f1
#endif

#ifndef TRMMKERNEL
	fxcpmadd A1,  f0, AP,  A1
	fxcxnpma f0,  f0, AP,  A1

	STFDUX	f0,  CO1, INCM1
	STFSDUX	f0,  CO1, INC
#else
	fxcpmadd f12, f0, AP,  f30
	fxcxnpma f0,  f0, AP,  f12

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
#endif

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L1089:
	addi	B,  BO, 2 * SIZE
	.align 4

.L10999:
	addi	SP, SP, 20

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
	.align 4


	EPILOGUE
#endif
