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

#if defined(TRMMKERNEL) && !defined(LEFT)
	neg	KK, OFFSET
#endif

	andi.	r0, C,   2 * SIZE - 1
	bne	.L1000
	andi.	r0, LDC, 2 * SIZE - 1
	bne	.L1000

/* High performance version */

	li	INCM3, -2 * SIZE
	li	INCM5, -5 * SIZE
	li	INCM7, -6 * SIZE

	addi	C, C, - 2 * SIZE
	srawi.	J, N,  2
	ble	.L50
	.align 4

.L10:
	mr	CO1, C
	add	CO2, C,   LDC
	add	CO3, CO2, LDC
	add	CO4, CO3, LDC
	add	C,   CO4, LDC

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mr	KK, OFFSET
#endif

	addi	AO, A, -4 * SIZE

	li	r0, FZERO
	lfpsx	f0, SP, r0

	srawi.	I, M,  3
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
	slwi	TEMP, KK, 3 + BASE_SHIFT
	slwi	r0,   KK, 2 + BASE_SHIFT
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
	addi	TEMP, KK, 8
#else
	addi	TEMP, KK, 4
#endif
	srawi.	TEMP,  TEMP,  2
 	fpmr	f1,  f0
	mtspr	CTR, TEMP
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
#ifndef TRMMKERNEL
	LFPDUX	A1, CO1, INC2
#else
	nop
#endif
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
#ifndef TRMMKERNEL
	LFPDUX	B1, CO1, INC4
#else
	nop
#endif

	fxcpmadd	f1,  B3, A2, f1
	nop
	fxcsmadd	f5,  B3, A2, f5
	LFPDUX	A4, AO2, INC4
	fxcpmadd	f9,  B4, A2, f9
#ifndef TRMMKERNEL
 	LFPDUX	A3, CO2, INC2
#else
	nop
#endif
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
#ifndef TRMMKERNEL
	LFPDUX	A5, CO2, INC4
#else
	nop
#endif
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
#ifndef TRMMKERNEL
	LFPDUX	B3, CO3, INC2
#else
	nop
#endif

	fxcpmadd	f1,  B5, A2, f1
	nop
	fxcsmadd	f5,  B5, A2, f5
	LFPDUX	A4, AO2, INC4
	fxcpmadd	f9,  B2, A2, f9
#ifndef TRMMKERNEL
	LFPDUX	A6, CO3, INC4
#else
	nop
#endif
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
#ifndef TRMMKERNEL
	LFPDUX	A7, CO4, INC2
#else
	nop
#endif
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
#ifndef TRMMKERNEL
	LFPDUX	B2, CO4, INC4
#else
	nop
#endif

	fxcpmadd	f1,  B6, A2, f1
	nop
	fxcsmadd	f5,  B6, A2, f5
	LFPDUX	A4, AO2, INC4
	fxcpmadd	f9,  B4, A2, f9
#ifndef TRMMKERNEL
	LFPDUX	B5, CO1, INCM3
#else
	nop
#endif
	fxcsmadd	f13, B4, A2, f13
	nop

	fxcpmadd	f2,  B6, A10, f2
	nop
	fxcsmadd	f6,  B6, A10, f6
	nop
	fxcpmadd	f10, B4, A10, f10
	nop
	fxcsmadd	f14, B4, A10, f14
#ifndef TRMMKERNEL
    	LFPDUX	A8, CO1, INC4
#else
	nop
#endif

	fxcpmadd	f3,  B6, A4, f3
	nop
	fxcsmadd	f7,  B6, A4, f7
	nop
	fxcpmadd	f11, B4, A4, f11
	nop
	fxcsmadd	f15, B4, A4, f15
#ifndef TRMMKERNEL
	LFPDUX	A9, CO2, INCM3
#else
	nop
#endif
	.align 4

.L14:
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 8
#else
	addi	TEMP, KK, 4
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
	LFPDUX	B1, CO1, INC4
	fpmr	f9,  f0
 	LFPDUX	A3, CO2, INC2
	fpmr	f13, f0
	LFPDUX	A5, CO2, INC4
	fpmr	f2,  f0

	LFPDUX	B3, CO3, INC2
	fpmr	f6,  f0
	LFPDUX	A6, CO3, INC4
	fpmr	f10, f0
	LFPDUX	A7,  CO4, INC2
	fpmr	f14, f0
	LFPDUX	B2,  CO4, INC4
	fpmr	f3,  f0

	LFPDUX	B5, CO1, INCM3
	fpmr	f7,  f0
    	LFPDUX	A8, CO1, INC4
	fpmr	f11, f0
	LFPDUX	A9, CO2, INCM3
	fpmr	f15, f0
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
	nop
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
#ifndef TRMMKERNEL
	fxcpmadd	f0,  AP, f0,  A1
	LFPDUX	B4, CO2, INC4
	fxcpmadd	f1,  AP, f1,  B5
	LFPDUX	A2, CO3, INCM3

	fxcpmadd	f2,  AP, f2,  B1
 	LFPDUX	A4, CO3, INC4
	fxcpmadd	f3,  AP, f3,  A8
	LFPDUX	A10,  CO4, INCM3

	fxcpmadd	f4,  AP, f4,  A3
	LFPDUX	A1,  CO4, INC4
	fxcpmadd	f5,  AP, f5,  A9
	STFPDUX	f0,  CO1, INCM7

	fxcpmadd	f6,  AP, f6,  A5
 	STFPDUX	f1,  CO1, INC2
	fxcpmadd	f7,  AP, f7,  B4
	STFPDUX	f2,  CO1, INC2

	fxcpmadd	f8,  AP, f8,  B3
	STFPDUX	f3,  CO1, INC2
	fxcpmadd	f9,  AP, f9,  A2
	STFPDUX	f4,  CO2, INCM7

	fxcpmadd	f10, AP, f10, A6
	STFPDUX	f5,  CO2, INC2
	fxcpmadd	f11, AP, f11, A4
	STFPDUX	f6,  CO2, INC2

	fxcpmadd	f12, AP, f12, A7
	STFPDUX	f7,  CO2, INC2
	fxcpmadd	f13, AP, f13, A10
	STFPDUX	f8,  CO3, INCM7

	fxcpmadd	f14, AP, f14, B2
	STFPDUX	f9,  CO3, INC2
	fxcpmadd	f15, AP, f15, A1
	STFPDUX	f10, CO3, INC2

	STFPDUX	f11, CO3, INC2
	STFPDUX	f12, CO4, INCM7
	STFPDUX	f13, CO4, INC2
	STFPDUX	f14, CO4, INC2
	STFPDUX	f15, CO4, INC2
#else
	fpmul	f0,  AP, f0
	fpmul	f1,  AP, f1
	fpmul	f2,  AP, f2
	fpmul	f3,  AP, f3

	fpmul	f4,  AP, f4
	fpmul	f5,  AP, f5
	STFPDUX	f0,  CO1, INC2

	fpmul	f6,  AP, f6
 	STFPDUX	f1,  CO1, INC2
	fpmul	f7,  AP, f7
	STFPDUX	f2,  CO1, INC2

	fpmul	f8,  AP, f8
	STFPDUX	f3,  CO1, INC2
	fpmul	f9,  AP, f9
	STFPDUX	f4,  CO2, INC2

	fpmul	f10, AP, f10
	STFPDUX	f5,  CO2, INC2
	fpmul	f11, AP, f11
	STFPDUX	f6,  CO2, INC2

	fpmul	f12, AP, f12
	STFPDUX	f7,  CO2, INC2
	fpmul	f13, AP, f13
	STFPDUX	f8,  CO3, INC2

	fpmul	f14, AP, f14
	STFPDUX	f9,  CO3, INC2
	fpmul	f15, AP, f15
	STFPDUX	f10, CO3, INC2

	STFPDUX	f11, CO3, INC2
	STFPDUX	f12, CO4, INC2
	STFPDUX	f13, CO4, INC2
	STFPDUX	f14, CO4, INC2
	STFPDUX	f15, CO4, INC2
#endif

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -8
#else
	addi	TEMP, TEMP, -4
#endif
	slwi	r0,   TEMP, 3 + BASE_SHIFT
	slwi	TEMP, TEMP, 2 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 8
#endif
#endif

	addic.	I, I, -1
	li	r0, FZERO

	lfpsx	f0, SP, r0
	bgt+	.L11
	.align 4

.L20:
	andi.	I, M,  4
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
	slwi	TEMP, KK, 2 + BASE_SHIFT
	slwi	r0,   KK, 2 + BASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	AO2, AO,   2 * SIZE
	fpmr	f4,  f0
	addi	BO,  BO,  - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, BO,    2 * SIZE
	fpmr	f12, f0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 4
#endif

	srawi.	TEMP,  TEMP,  2
 	fpmr	f1,  f0
	fpmr	f5,  f0
	fpmr	f9,  f0
	mtspr	CTR, TEMP
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
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
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
#ifndef TRMMKERNEL
	LFPDUX	A1, CO1, INC2
	LFPDUX	B1, CO1, INC2
	LFPDUX	B3, CO2, INC2
	LFPDUX	A6, CO2, INC2

	LFPDUX	B5, CO3, INC2
	LFPDUX	A8, CO3, INC2
	LFPDUX	A2, CO4, INC2
	LFPDUX	A4, CO4, INC2

	fxcpmadd	f0,  AP, f0,  A1
	fxcpmadd	f1,  AP, f1,  B1
	fxcpmadd	f4,  AP, f4,  B3
	fxcpmadd	f5,  AP, f5,  A6

	fxcpmadd	f8,  AP, f8,  B5
	fxcpmadd	f9,  AP, f9,  A8
	STFPDUX	f0,  CO1, INCM3
	fxcpmadd	f12, AP, f12, A2
 	STFPDUX	f1,  CO1, INC2
	fxcpmadd	f13, AP, f13, A4
	STFPDUX	f4,  CO2, INCM3

	STFPDUX	f5,  CO2, INC2
	STFPDUX	f8,  CO3, INCM3
	STFPDUX	f9,  CO3, INC2
	STFPDUX	f12, CO4, INCM3
	STFPDUX	f13, CO4, INC2
#else
	fpmul	f0,  AP, f0
	fpmul	f1,  AP, f1
	fpmul	f4,  AP, f4
	fpmul	f5,  AP, f5

	fpmul	f8,  AP, f8
	fpmul	f9,  AP, f9
	STFPDUX	f0,  CO1, INC2
	fpmul	f12, AP, f12
 	STFPDUX	f1,  CO1, INC2
	fpmul	f13, AP, f13
	STFPDUX	f4,  CO2, INC2

	STFPDUX	f5,  CO2, INC2
	STFPDUX	f8,  CO3, INC2
	STFPDUX	f9,  CO3, INC2
	STFPDUX	f12, CO4, INC2
	STFPDUX	f13, CO4, INC2
#endif


#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -4
#else
	addi	TEMP, TEMP, -4
#endif
	slwi	r0,   TEMP, 2 + BASE_SHIFT
	slwi	TEMP, TEMP, 2 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 4
#endif
#endif

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L30:
	andi.	I, M,  2
	beq	.L40

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	AO2, AO,   2 * SIZE
	fpmr	f1,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f2,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f3, f0
#else
	slwi	TEMP, KK, 1 + BASE_SHIFT
	slwi	r0,   KK, 2 + BASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	AO2, AO,   2 * SIZE
	fpmr	f1,  f0
	addi	BO,  BO, - 4 * SIZE
	fpmr	f2,  f0
	addi	BO2, BO,   2 * SIZE
	fpmr	f3, f0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 4
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
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f1,  B1, A1, f1
	LFPDUX	B1,  BO, INC4
	fxcpmadd	f2,  B2, A1, f2
	fxcsmadd	f3,  B2, A1, f3
	LFPDUX	B2, BO2, INC4
	LFPDUX	A1,  AO, INC4

	fxcpmadd	f0,  B3, A2, f0
	fxcsmadd	f1,  B3, A2, f1
	LFPDUX	B3,  BO, INC4
	fxcpmadd	f2,  B4, A2, f2
	fxcsmadd	f3,  B4, A2, f3
	LFPDUX	B4, BO2, INC4
	LFPDUX	A2, AO2, INC4

	fxcpmadd	f0,  A5, A3, f0
	fxcsmadd	f1,  A5, A3, f1
	LFPDUX	A5,  BO, INC4
	fxcpmadd	f2,  A6, A3, f2
	fxcsmadd	f3,  A6, A3, f3
	LFPDUX	A6, BO2, INC4
	LFPDUX	A3,  AO, INC4

	fxcpmadd	f0,  A7, A4, f0
	fxcsmadd	f1,  A7, A4, f1
	LFPDUX	A7,  BO, INC4
	fxcpmadd	f2,  A8, A4, f2
	fxcsmadd	f3,  A8, A4, f3
	LFPDUX	A8, BO2, INC4
	LFPDUX	A4, AO2, INC4
	bdnz+	.L32
	.align 4

.L33:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f1,  B1, A1, f1
	fxcpmadd	f2,  B2, A1, f2
	fxcsmadd	f3,  B2, A1, f3

	fxcpmadd	f0,  B3, A2, f0
	fxcsmadd	f1,  B3, A2, f1
	fxcpmadd	f2,  B4, A2, f2
	fxcsmadd	f3,  B4, A2, f3

	fxcpmadd	f0,  A5, A3, f0
	fxcsmadd	f1,  A5, A3, f1
	fxcpmadd	f2,  A6, A3, f2
	fxcsmadd	f3,  A6, A3, f3

	fxcpmadd	f0,  A7, A4, f0
	fxcsmadd	f1,  A7, A4, f1
	fxcpmadd	f2,  A8, A4, f2
	fxcsmadd	f3,  A8, A4, f3
	.align 4

.L34:
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

#if defined(TRMMKERNEL)
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
	ble+	.L38

	LFPDX	A1,  AO,  INC4
	LFPDUX	B1,  BO,  INC4
	LFPDUX	B2,  BO2, INC4
	add	AO, AO, INC2
	bdz-	.L37
	.align 4

.L36:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f1,  B1, A1, f1
	LFPDUX	B1,  BO,  INC4
	fxcpmadd	f2,  B2, A1, f2
	fxcsmadd	f3,  B2, A1, f3
	LFPDX	A1,  AO,  INC4
	LFPDUX	B2,  BO2, INC4
	add	AO, AO, INC2
	bdnz+	.L36
	.align 4

.L37:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f1,  B1, A1, f1
	fxcpmadd	f2,  B2, A1, f2
	fxcsmadd	f3,  B2, A1, f3
	.align 4

.L38:
#ifndef TRMMKERNEL
	LFPDX	A1, CO1, INC2
	LFPDX	A2, CO2, INC2
	LFPDX	A3, CO3, INC2
	LFPDX	A4, CO4, INC2

	fxcpmadd	f0, AP, f0, A1
	fxcpmadd	f1, AP, f1, A2
	fxcpmadd	f2, AP, f2, A3
	fxcpmadd	f3, AP, f3, A4
#else
	fpmul	f0, AP, f0
	fpmul	f1, AP, f1
	fpmul	f2, AP, f2
	fpmul	f3, AP, f3
#endif

	STFPDUX	f0,  CO1, INC2
	STFPDUX	f1,  CO2, INC2
	STFPDUX	f2,  CO3, INC2
	STFPDUX	f3,  CO4, INC2

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

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L40:
	andi.	I, M,  1
	beq	.L49

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	AO2, AO,   2 * SIZE
	fpmr	f1,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f2,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f3,  f0
#else
	slwi	TEMP, KK, 0 + BASE_SHIFT
	slwi	r0,   KK, 2 + BASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	AO2, AO,   2 * SIZE
	fpmr	f1,  f0
	addi	BO,  BO, - 4 * SIZE
	fpmr	f2,  f0
	addi	BO2, BO,   2 * SIZE
	fpmr	f3,  f0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 4
#endif
	srawi.	r0,  TEMP,  3
	mtspr	CTR, r0
	ble	.L44

#else
	addi	AO2, AO,   2 * SIZE
	fpmr	f1,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f2,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f3,  f0

	srawi.	r0,  K,  3
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
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 4
#endif
	andi.	TEMP,  TEMP,  7
	mtspr	CTR, TEMP
#else
	andi.	r0,  K,  7
	mtspr	CTR, r0
#endif
	ble+	.L48

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
	.align 4

.L48:
#ifndef TRMMKERNEL
	LFDX	A1, CO1, INC2
	LFDX	A2, CO2, INC2
	LFDX	A3, CO3, INC2
	LFDX	A4, CO4, INC2

	fpadd	f0, f0, f2
	fpadd	f1, f1, f3

	fsmfp	A1, A2
	fsmfp	A3, A4

	fxcpmadd	f0,  AP, f0,  A1
	fxcpmadd	f1,  AP, f1,  A3
#else
	fpadd	f0, f0, f2
	fpadd	f1, f1, f3

	fpmul	f0,  AP, f0
	fpmul	f1,  AP, f1
#endif

	STFDX	f0,  CO1, INC2
	STFSDX	f0,  CO2, INC2
	STFDX	f1,  CO3, INC2
	STFSDX	f1,  CO4, INC2

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
	addi	KK, KK, 1
#endif
#endif
	.align 4

.L49:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addi	KK, KK, 4
#endif

	addi	B,  BO, 4 * SIZE

	addic.	J, J, -1
	bgt+	.L10
	.align 4

.L50:
	andi.	J, N,  2
	beq	.L90

	mr	CO1, C
	add	CO2, C,   LDC
	add	C,   CO2, LDC

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mr	KK, OFFSET
#endif

	addi	AO, A, -2 * SIZE

	li	r0, FZERO
	lfpsx	f0, SP, r0

	srawi.	I, M,  3
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
	slwi	TEMP, KK, 3 + BASE_SHIFT
	slwi	r0,   KK, 1 + BASE_SHIFT
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
	addi	TEMP, KK, 8
#else
	addi	TEMP, KK, 2
#endif
	srawi.	r0,  TEMP,  2
	fpmr	f3,  f0
	mtspr	CTR, r0
	fpmr	f7,  f0
	ble	.L54
#else
	fpmr	f4,  f0
	addi	BO,  B,  - 2 * SIZE
 	fpmr	f1,  f0
	fpmr	f5,  f0
	fpmr	f2,  f0
	fpmr	f6,  f0

	srawi.	r0,  K,  2
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
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 8
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
#ifndef TRMMKERNEL
	LFPDUX	A1, CO1, INC2
	LFPDUX	B1, CO1, INC2
	LFPDUX	A3, CO1, INC2
   	LFPDUX	A5, CO1, INC2

 	LFPDUX	B3, CO2, INC2
	LFPDUX	A6, CO2, INC2
	LFPDUX	A7, CO2, INC2
	LFPDUX	B2, CO2, INC2

	fxcpmadd	f0,  AP, f0,  A1
	fxcpmadd	f1,  AP, f1,  B1
	fxcpmadd	f2,  AP, f2,  A3
	fxcpmadd	f3,  AP, f3,  A5

	fxcpmadd	f4,  AP, f4,  B3
	fxcpmadd	f5,  AP, f5,  A6
	STFPDUX	f0,  CO1, INCM7
	fxcpmadd	f6,  AP, f6,  A7
	STFPDUX	f1,  CO1, INC2
	fxcpmadd	f7,  AP, f7,  B2
	STFPDUX	f2,  CO1, INC2
	STFPDUX	f3,  CO1, INC2
	STFPDUX	f4,  CO2, INCM7

	STFPDUX	f5,  CO2, INC2
	STFPDUX	f6,  CO2, INC2
	STFPDUX	f7,  CO2, INC2
#else
	fpmul	f0,  AP, f0
	fpmul	f1,  AP, f1
	fpmul	f2,  AP, f2
	fpmul	f3,  AP, f3

	fpmul	f4,  AP, f4
	fpmul	f5,  AP, f5
	STFPDUX	f0,  CO1, INC2
	fpmul	f6,  AP, f6
	STFPDUX	f1,  CO1, INC2
	fpmul	f7,  AP, f7
	STFPDUX	f2,  CO1, INC2
	STFPDUX	f3,  CO1, INC2
	STFPDUX	f4,  CO2, INC2

	STFPDUX	f5,  CO2, INC2
	STFPDUX	f6,  CO2, INC2
	STFPDUX	f7,  CO2, INC2
#endif


#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -8
#else
	addi	TEMP, TEMP, -2
#endif
	slwi	r0,   TEMP, 3 + BASE_SHIFT
	slwi	TEMP, TEMP, 1 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 8
#endif
#endif

	addic.	I, I, -1
	li	r0, FZERO

	lfpsx	f0, SP, r0
	bgt+	.L51
	.align 4

.L60:
	andi.	I, M,  4
	beq	.L70

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	BO,  B,  - 2 * SIZE
 	fpmr	f1,  f0
#else
	slwi	TEMP, KK, 2 + BASE_SHIFT
	slwi	r0,   KK, 1 + BASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	BO,  BO,  - 2 * SIZE
 	fpmr	f1,  f0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 2
#endif
	fpmr	f2,  f0
	srawi.	r0,  TEMP,  2
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
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

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
	ble+	.L68

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
#ifndef TRMMKERNEL
	LFPDUX	A1, CO1, INC2
	LFPDUX	A2, CO1, INC2
	LFPDUX	A3, CO2, INC2
	LFPDUX	A4, CO2, INC2

	fxcpmadd	f0,  AP, f0,  A1
	fxcpmadd	f1,  AP, f1,  A2
	fxcpmadd	f2,  AP, f2,  A3
 	fxcpmadd	f3,  AP, f3,  A4

	STFPDUX	f0,  CO1, INCM3
	STFPDUX	f1,  CO1, INC2
	STFPDUX	f2,  CO2, INCM3
	STFPDUX	f3,  CO2, INC2
#else
	fpmul	f0,  AP, f0
	fpmul	f1,  AP, f1
	fpmul	f2,  AP, f2
 	fpmul	f3,  AP, f3

	STFPDUX	f0,  CO1, INC2
	STFPDUX	f1,  CO1, INC2
	STFPDUX	f2,  CO2, INC2
	STFPDUX	f3,  CO2, INC2
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
	slwi	r0,   TEMP, 2 + BASE_SHIFT
	slwi	TEMP, TEMP, 1 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 4
#endif
#endif

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L70:
	andi.	I, M,  2
	beq	.L80

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
#else
	slwi	TEMP, KK, 1 + BASE_SHIFT
	slwi	r0,   KK, 1 + BASE_SHIFT
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
	addi	TEMP, KK, 2
#endif
	srawi.	r0,  TEMP,  3
	fpmr	f2,  f0
	mtspr	CTR, r0
	fpmr	f3, f0
	ble	.L74
#else
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0

	srawi.	r0,  K,  3
	fpmr	f2,  f0
	mtspr	CTR, r0
	fpmr	f3, f0
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
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 2
#endif
	andi.	TEMP,  TEMP,  7
	mtspr	CTR, TEMP
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
#ifndef TRMMKERNEL
	LFPDX	A1, CO1, INC2
	LFPDX	B3, CO2, INC2

	fpadd	f0, f0, f2
	fpadd	f1, f1, f3

	fxcpmadd	f0,  AP, f0,  A1
	fxcpmadd	f1,  AP, f1,  B3
#else
	fpadd	f0, f0, f2
	fpadd	f1, f1, f3

	fpmul	f0,  AP, f0
	fpmul	f1,  AP, f1
#endif

	STFPDUX	f0,  CO1, INC2
	STFPDUX	f1,  CO2, INC2

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

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L80:
	andi.	I, M,  1
	beq	.L89

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0
#else
	slwi	TEMP, KK, 0 + BASE_SHIFT
	slwi	r0,   KK, 1 + BASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	BO,  BO,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 2
#endif
	srawi.	r0,  TEMP,  3
	mtspr	CTR, r0
	ble	.L84
#else
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0

	srawi.	r0,  K,  3
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
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 2
#endif
	andi.	TEMP,  TEMP,  7
	mtspr	CTR, TEMP
#else
	andi.	r0,  K,  7
	mtspr	CTR, r0
#endif
	ble+	.L88

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
#ifndef TRMMKERNEL
	LFDX	A1, CO1, INC2
	LFDX	A2, CO2, INC2

	fpadd	f0, f0, f1
	fpadd	f2, f2, f3
	fsmfp	A1, A2
	fpadd	f0, f0, f2
	fxcpmadd	f0,  AP, f0,  A1
#else
	fpadd	f0, f0, f1
	fpadd	f2, f2, f3
	fsmfp	A1, A2
	fpadd	f0, f0, f2
	fpmul	f0,  AP, f0
#endif

	STFDX	f0,  CO1, INC2
	STFSDX	f0,  CO2, INC2

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

.L89:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addi	KK, KK, 2
#endif

	addi	B,  BO, 2 * SIZE
	.align 4

.L90:
	andi.	J, N,  1
	beq	.L999

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mr	KK, OFFSET
#endif

	mr	CO1, C
	addi	AO, A, -2 * SIZE

	li	r0, FZERO
	lfpsx	f0, SP, r0

	srawi.	I, M,  3
	ble	.L100
	.align 4

.L91:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	BO,  B,  - 2 * SIZE
 	fpmr	f1,  f0
#else
	slwi	TEMP, KK, 3 + BASE_SHIFT
	slwi	r0,   KK, 0 + BASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	BO,  BO,  - 2 * SIZE
 	fpmr	f1,  f0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 8
#else
	addi	TEMP, KK, 1
#endif
	fpmr	f2,  f0
	srawi.	r0,  TEMP,  2
	fpmr	f3,  f0
	mtspr	CTR, r0
	ble	.L94

#else
	srawi.	r0,  K,  2
 	fpmr	f1,  f0
	addi	BO,  B,  - 2 * SIZE
	fpmr	f2,  f0
	fpmr	f3,  f0
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
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 8
#else
	addi	TEMP, KK, 1
#endif
	andi.	TEMP,  TEMP,  3
	mtspr	CTR, TEMP
#else
	andi.	r0,  K,  3
	mtspr	CTR, r0
#endif
	ble+	.L98

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
#ifndef TRMMKERNEL
	LFPDUX	A1, CO1, INC2
	LFPDUX	B1, CO1, INC2
	LFPDUX	A3, CO1, INC2
   	LFPDUX	A5, CO1, INC2

	fxcpmadd	f0,  AP, f0,  A1
	fxcpmadd	f1,  AP, f1,  B1
	fxcpmadd	f2,  AP, f2,  A3
	fxcpmadd	f3,  AP, f3,  A5

	STFPDUX	f0,  CO1, INCM7
	STFPDUX	f1,  CO1, INC2
	STFPDUX	f2,  CO1, INC2
	STFPDUX	f3,  CO1, INC2
#else
	fpmul	f0,  AP, f0
	fpmul	f1,  AP, f1
	fpmul	f2,  AP, f2
	fpmul	f3,  AP, f3

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
	addi	TEMP, TEMP, -8
#else
	addi	TEMP, TEMP, -1
#endif
	slwi	r0,   TEMP, 3 + BASE_SHIFT
	slwi	TEMP, TEMP, 0 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 8
#endif
#endif

	addic.	I, I, -1
	li	r0, FZERO

	lfpsx	f0, SP, r0
	bgt+	.L91
	.align 4

.L100:
	andi.	I, M,  4
	beq	.L110

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3, f0
#else
	slwi	TEMP, KK, 2 + BASE_SHIFT
	slwi	r0,   KK, 0 + BASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	fpmr	f1,  f0
	addi	BO,  BO,  - 2 * SIZE
	fpmr	f2,  f0
	fpmr	f3, f0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 1
#endif
	srawi.	r0,  TEMP,  3
	mtspr	CTR, r0
	ble	.L104
#else
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3, f0

	srawi.	r0,  K,  3
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
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 1
#endif
	andi.	TEMP,  TEMP,  7
	mtspr	CTR, TEMP
#else
	andi.	r0,  K,  7
	mtspr	CTR, r0
#endif
	ble+	.L108

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
#ifndef TRMMKERNEL
	LFPDUX	A1, CO1, INC2
	LFPDUX	B1, CO1, INC2

	fpadd	f0, f0, f2
	fpadd	f1, f1, f3

	fxcpmadd	f0,  AP, f0,  A1
	fxcpmadd	f1,  AP, f1,  B1

	STFPDUX	f0,  CO1, INCM3
	STFPDUX	f1,  CO1, INC2
#else
	fpadd	f0, f0, f2
	fpadd	f1, f1, f3

	fpmul	f0,  AP, f0
	fpmul	f1,  AP, f1

	STFPDUX	f0,  CO1, INC2
	STFPDUX	f1,  CO1, INC2
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
	slwi	r0,   TEMP, 2 + BASE_SHIFT
	slwi	TEMP, TEMP, 0 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 4
#endif
#endif

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L110:
	andi.	I, M,  2
	beq	.L120

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0
#else
	slwi	TEMP, KK, 1 + BASE_SHIFT
	slwi	r0,   KK, 0 + BASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	fpmr	f1,  f0
	addi	BO,  BO,  - 2 * SIZE
	fpmr	f2,  f0
	fpmr	f3,  f0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 1
#endif
	srawi.	r0,  TEMP,  3
	mtspr	CTR, r0
	ble	.L114
#else
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0

	srawi.	r0,  K,  3
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
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 1
#endif
	andi.	TEMP,  TEMP,  7
	mtspr	CTR, TEMP
#else
	andi.	r0,  K,  7
	mtspr	CTR, r0
#endif
	ble+	.L118

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
#ifndef TRMMKERNEL
	LFPDX	A1, CO1, INC2

	fpadd	f0, f0, f1
	fpadd	f2, f3, f2
	fpadd	f0, f0, f2
	fxcpmadd	f1,  AP, f0,  A1

	li	r0, FZERO
	lfpsx	f0, SP, r0

	STFPDUX	f1,  CO1, INC2
#else
	fpadd	f0, f0, f1
	fpadd	f2, f3, f2
	fpadd	f0, f0, f2
	fpmul	f1,  AP, f0

	li	r0, FZERO
	lfpsx	f0, SP, r0

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
	slwi	r0,   TEMP, 1 + BASE_SHIFT
	slwi	TEMP, TEMP, 0 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 2
#endif
#endif
	.align 4

.L120:
	andi.	I, M,  1
	beq	.L999

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0
#else
	slwi	TEMP, KK, 0 + BASE_SHIFT
	slwi	r0,   KK, 0 + BASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	fpmr	f1,  f0
	addi	BO,  BO,  - 2 * SIZE
	fpmr	f2,  f0
	fpmr	f3,  f0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 1
#endif
	srawi.	r0,  TEMP,  3
	mtspr	CTR, r0
	ble	.L124
#else
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0

	srawi.	r0,  K,  3
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
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

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
	ble+	.L128

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
#ifndef TRMMKERNEL
	LFDX	A1, CO1, INC2
	fpadd	f0, f0, f1
	fpadd	f2, f2, f3
	fpadd	f0, f0, f2
	fsmtp	f1, f0
	fadd	f0, f0, f1
	fmadd	f0,  AP, f0,  A1
#else
	fpadd	f0, f0, f1
	fpadd	f2, f2, f3
	fpadd	f0, f0, f2
	fsmtp	f1, f0
	fadd	f0, f0, f1
	fpmul	f0,  AP, f0
#endif
	STFDUX	f0,  CO1, INC2
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
	.align 4

.L1000:
	li	INCM1, -1 * SIZE
	li	INCM3, -3 * SIZE
	li	INCM5, -5 * SIZE
	li	INCM7, -7 * SIZE

	addi	C, C, - 1 * SIZE
	srawi.	J, N,  2
	ble	.L1050
	.align 4

.L1010:
	mr	CO1, C
	add	CO2, C,   LDC
	add	CO3, CO2, LDC
	add	CO4, CO3, LDC
	add	C,   CO4, LDC

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mr	KK, OFFSET
#endif

	addi	AO, A, -4 * SIZE

	li	r0, FZERO
	lfpsx	f0, SP, r0

	srawi.	I, M,  3
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
	slwi	TEMP, KK, 3 + BASE_SHIFT
	slwi	r0,   KK, 2 + BASE_SHIFT
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
	addi	TEMP, KK, 8
#else
	addi	TEMP, KK, 4
#endif
	srawi.	TEMP,  TEMP,  2
 	fpmr	f1,  f0
	mtspr	CTR, TEMP
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
	bdnz+	.L1012
	.align 4

.L1013:
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
#ifndef TRMMKERNEL
	LFDUX	A1, CO1, INC
#else
	nop
#endif
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
#ifndef TRMMKERNEL
	LFDUX	B1, CO1, INC2
#else
	nop
#endif

	fxcpmadd	f1,  B3, A2, f1
	nop
	fxcsmadd	f5,  B3, A2, f5
	LFPDUX	A4, AO2, INC4
	fxcpmadd	f9,  B4, A2, f9
#ifndef TRMMKERNEL
	LFDUX	A3, CO1, INC2
#else
	nop
#endif
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
#ifndef TRMMKERNEL
   	LFDUX	A5, CO1, INC2
#else
	nop
#endif
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
#ifndef TRMMKERNEL
	LFSDUX	A1, CO1, INCM5
#else
	nop
#endif

	fxcpmadd	f1,  B5, A2, f1
	nop
	fxcsmadd	f5,  B5, A2, f5
	LFPDUX	A4, AO2, INC4
	fxcpmadd	f9,  B2, A2, f9
#ifndef TRMMKERNEL
	LFSDUX	B1, CO1, INC2
#else
	nop
#endif
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
#ifndef TRMMKERNEL
	LFSDUX	A3, CO1, INC2
#else
	nop
#endif
	fxcsmadd	f15, B2, A4, f15
	nop

## 4 ##

	fxcpmadd	f0,  B6, A9, f0
	nop
	fxcsmadd	f4,  B6, A9, f4
	nop
	fxcpmadd	f8,  B4, A9, f8
#ifndef TRMMKERNEL
	LFSDUX	A5, CO1, INC2
#else
	nop
#endif
	fxcsmadd	f12, B4, A9, f12
#ifndef TRMMKERNEL
	LFDUX	B3, CO2, INC
#else
	nop
#endif

	fxcpmadd	f1,  B6, A2, f1
	nop
	fxcsmadd	f5,  B6, A2, f5
	LFPDUX	A4, AO2, INC4
	fxcpmadd	f9,  B4, A2, f9
#ifndef TRMMKERNEL
	LFDUX	A6, CO2, INC2
#else
	nop
#endif
	fxcsmadd	f13, B4, A2, f13
	nop

	fxcpmadd	f2,  B6, A10, f2
	nop
	fxcsmadd	f6,  B6, A10, f6
	nop
	fxcpmadd	f10, B4, A10, f10
	nop
	fxcsmadd	f14, B4, A10, f14
#ifndef TRMMKERNEL
	LFDUX	A7, CO2, INC2
#else
	nop
#endif

	fxcpmadd	f3,  B6, A4, f3
	nop
	fxcsmadd	f7,  B6, A4, f7
	nop
	fxcpmadd	f11, B4, A4, f11
	nop
	fxcsmadd	f15, B4, A4, f15
#ifndef TRMMKERNEL
	LFDUX	B2, CO2, INC2
#else
	nop
#endif
	.align 4

.L1014:
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 8
#else
	addi	TEMP, KK, 4
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
	nop
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
	bdnz+	.L1016
	.align 4

.L1017:
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

.L1018:
#ifndef TRMMKERNEL
	LFSDUX	B3, CO2, INCM5
	LFSDUX	A6, CO2, INC2
	LFSDUX	A7, CO2, INC2
	LFSDUX	B2, CO2, INC2

	LFDUX	B5, CO3, INC
	LFDUX	A8, CO3, INC2
	LFDUX	A9, CO3, INC2
 	LFDUX	B4, CO3, INC2

	LFSDUX	B5, CO3, INCM5
	LFSDUX	A8, CO3, INC2
	LFSDUX	A9, CO3, INC2
	LFSDUX	B4, CO3, INC2

	LFDUX	A2,  CO4, INC
	LFDUX	A4,  CO4, INC2

	fxcpmadd	f0,  AP, f0,  A1
	LFDUX	A10, CO4, INC2
	LFDUX	A1,  CO4, INC2

	fxcpmadd	f1,  AP, f1,  B1
	LFSDUX	A2,  CO4, INCM5
	LFSDUX	A4,  CO4, INC2

	fxcpmadd	f2,  AP, f2,  A3
	LFSDUX	A10, CO4, INC2
	LFSDUX	A1,  CO4, INC2

	fxcpmadd	f3,  AP, f3,  A5
	STFDUX	f0,  CO1, INCM7
	STFSDUX	f0,  CO1, INC

	fxcpmadd	f4,  AP, f4,  B3
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC

	fxcpmadd	f5,  AP, f5,  A6
	STFDUX	f2,  CO1, INC
	STFSDUX	f2,  CO1, INC

	fxcpmadd	f6,  AP, f6,  A7
	STFDUX	f3,  CO1, INC
	STFSDUX	f3,  CO1, INC

	fxcpmadd	f7,  AP, f7,  B2
	STFDUX	f4,  CO2, INCM7
	STFSDUX	f4,  CO2, INC

	fxcpmadd	f8,  AP, f8,  B5
	STFDUX	f5,  CO2, INC
	STFSDUX	f5,  CO2, INC

	fxcpmadd	f9,  AP, f9,  A8
	STFDUX	f6,  CO2, INC
	STFSDUX	f6,  CO2, INC

	fxcpmadd	f10, AP, f10, A9
	STFDUX	f7,  CO2, INC
	STFSDUX	f7,  CO2, INC

	fxcpmadd	f11, AP, f11, B4
	STFDUX	f8,  CO3, INCM7
	STFSDUX	f8,  CO3, INC

	fxcpmadd	f12, AP, f12, A2
	STFDUX	f9,  CO3, INC
	STFSDUX	f9,  CO3, INC

	fxcpmadd	f13, AP, f13, A4
	STFDUX	f10, CO3, INC
	STFSDUX	f10, CO3, INC

	fxcpmadd	f14, AP, f14, A10
	STFDUX	f11, CO3, INC
	STFSDUX	f11, CO3, INC

	fxcpmadd	f15, AP, f15, A1
	STFDUX	f12, CO4, INCM7
#else
	fpmul	f0,  AP, f0
	fpmul	f1,  AP, f1
	fpmul	f2,  AP, f2
	fpmul	f3,  AP, f3

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC

	fpmul	f4,  AP, f4
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC

	fpmul	f5,  AP, f5
	STFDUX	f2,  CO1, INC
	STFSDUX	f2,  CO1, INC

	fpmul	f6,  AP, f6
	STFDUX	f3,  CO1, INC
	STFSDUX	f3,  CO1, INC

	fpmul	f7,  AP, f7
	STFDUX	f4,  CO2, INC
	STFSDUX	f4,  CO2, INC

	fpmul	f8,  AP, f8
	STFDUX	f5,  CO2, INC
	STFSDUX	f5,  CO2, INC

	fpmul	f9,  AP, f9
	STFDUX	f6,  CO2, INC
	STFSDUX	f6,  CO2, INC

	fpmul	f10, AP, f10
	STFDUX	f7,  CO2, INC
	STFSDUX	f7,  CO2, INC

	fpmul	f11, AP, f11
	STFDUX	f8,  CO3, INC
	STFSDUX	f8,  CO3, INC

	fpmul	f12, AP, f12
	STFDUX	f9,  CO3, INC
	STFSDUX	f9,  CO3, INC

	fpmul	f13, AP, f13
	STFDUX	f10, CO3, INC
	STFSDUX	f10, CO3, INC

	fpmul	f14, AP, f14
	STFDUX	f11, CO3, INC
	STFSDUX	f11, CO3, INC

	fpmul	f15, AP, f15
	STFDUX	f12, CO4, INC
#endif

	STFSDUX	f12, CO4, INC
	STFDUX	f13, CO4, INC
	STFSDUX	f13, CO4, INC
	STFDUX	f14, CO4, INC
	STFSDUX	f14, CO4, INC
	STFDUX	f15, CO4, INC
	STFSDUX	f15, CO4, INC

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -8
#else
	addi	TEMP, TEMP, -4
#endif
	slwi	r0,   TEMP, 3 + BASE_SHIFT
	slwi	TEMP, TEMP, 2 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 8
#endif
#endif

	addic.	I, I, -1
	li	r0, FZERO

	lfpsx	f0, SP, r0
	bgt+	.L1011
	.align 4

.L1020:
	andi.	I, M,  4
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
	slwi	TEMP, KK, 2 + BASE_SHIFT
	slwi	r0,   KK, 2 + BASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	AO2, AO,   2 * SIZE
	fpmr	f4,  f0
	addi	BO,  BO,  - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, BO,    2 * SIZE
	fpmr	f12, f0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 4
#endif

	srawi.	TEMP,  TEMP,  2
 	fpmr	f1,  f0
	fpmr	f5,  f0
	fpmr	f9,  f0
	mtspr	CTR, TEMP
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
	bdnz+	.L1022
	.align 4

.L1023:
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

.L1024:
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 4
#endif
	andi.	TEMP,  TEMP,  3
	mtspr	CTR, TEMP
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
	bdnz+	.L1026
	.align 4

.L1027:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f4,  B1, A1, f4
	fxcpmadd	f8,  B2, A1, f8
	fxcsmadd	f12, B2, A1, f12

	fxcpmadd	f1,  B1, A2, f1
	fxcsmadd	f5,  B1, A2, f5
	fxcpmadd	f9,  B2, A2, f9
	fxcsmadd	f13, B2, A2, f13
	.align 4

.L1028:
#ifndef TRMMKERNEL
	LFDUX	A1, CO1, INC
	LFDUX	B1, CO1, INC2
	LFDUX	B3, CO2, INC
	LFDUX	A6, CO2, INC2

	LFSDUX	A1, CO1, INCM1
	LFSDUX	B1, CO1, INC2
	LFSDUX	B3, CO2, INCM1
	LFSDUX	A6, CO2, INC2

	LFDUX	B5, CO3, INC
	LFDUX	A8, CO3, INC2
	LFDUX	A2, CO4, INC
	LFDUX	A4, CO4, INC2

	fxcpmadd	f0,  AP, f0,  A1
	LFSDUX	B5, CO3, INCM1
	LFSDUX	A8, CO3, INC2

	fxcpmadd	f1,  AP, f1,  B1
	LFSDUX	A2,  CO4, INCM1
	LFSDUX	A4,  CO4, INC2

	fxcpmadd	f4,  AP, f4,  B3
	STFDUX	f0,  CO1, INCM3
	STFSDUX	f0,  CO1, INC

	fxcpmadd	f5,  AP, f5,  A6
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC

	fxcpmadd	f8,  AP, f8,  B5
	STFDUX	f4,  CO2, INCM3
	STFSDUX	f4,  CO2, INC

	fxcpmadd	f9,  AP, f9,  A8
	STFDUX	f5,  CO2, INC
	STFSDUX	f5,  CO2, INC

	fxcpmadd	f12, AP, f12, A2
	STFDUX	f8,  CO3, INCM3
	STFSDUX	f8,  CO3, INC

	fxcpmadd	f13, AP, f13, A4
	STFDUX	f9,  CO3, INC
	STFSDUX	f9,  CO3, INC

	STFDUX	f12, CO4, INCM3
	STFSDUX	f12, CO4, INC

	STFDUX	f13, CO4, INC
	STFSDUX	f13, CO4, INC
#else
	fpmul	f0,  AP, f0
	fpmul	f1,  AP, f1

	fpmul	f4,  AP, f4
	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC

	fpmul	f5,  AP, f5
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC

	fpmul	f8,  AP, f8
	STFDUX	f4,  CO2, INC
	STFSDUX	f4,  CO2, INC

	fpmul	f9,  AP, f9
	STFDUX	f5,  CO2, INC
	STFSDUX	f5,  CO2, INC

	fpmul	f12, AP, f12
	STFDUX	f8,  CO3, INC
	STFSDUX	f8,  CO3, INC

	fpmul	f13, AP, f13
	STFDUX	f9,  CO3, INC
	STFSDUX	f9,  CO3, INC

	STFDUX	f12, CO4, INC
	STFSDUX	f12, CO4, INC

	STFDUX	f13, CO4, INC
	STFSDUX	f13, CO4, INC
#endif


#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -4
#else
	addi	TEMP, TEMP, -4
#endif
	slwi	r0,   TEMP, 2 + BASE_SHIFT
	slwi	TEMP, TEMP, 2 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 4
#endif
#endif

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L1030:
	andi.	I, M,  2
	beq	.L1040

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	AO2, AO,   2 * SIZE
	fpmr	f1,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f2,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f3, f0
#else
	slwi	TEMP, KK, 1 + BASE_SHIFT
	slwi	r0,   KK, 2 + BASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	AO2, AO,   2 * SIZE
	fpmr	f1,  f0
	addi	BO,  BO, - 4 * SIZE
	fpmr	f2,  f0
	addi	BO2, BO,   2 * SIZE
	fpmr	f3, f0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 4
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
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f1,  B1, A1, f1
	LFPDUX	B1,  BO, INC4
	fxcpmadd	f2,  B2, A1, f2
	fxcsmadd	f3,  B2, A1, f3
	LFPDUX	B2, BO2, INC4
	LFPDUX	A1,  AO, INC4

	fxcpmadd	f0,  B3, A2, f0
	fxcsmadd	f1,  B3, A2, f1
	LFPDUX	B3,  BO, INC4
	fxcpmadd	f2,  B4, A2, f2
	fxcsmadd	f3,  B4, A2, f3
	LFPDUX	B4, BO2, INC4
	LFPDUX	A2, AO2, INC4

	fxcpmadd	f0,  A5, A3, f0
	fxcsmadd	f1,  A5, A3, f1
	LFPDUX	A5,  BO, INC4
	fxcpmadd	f2,  A6, A3, f2
	fxcsmadd	f3,  A6, A3, f3
	LFPDUX	A6, BO2, INC4
	LFPDUX	A3,  AO, INC4

	fxcpmadd	f0,  A7, A4, f0
	fxcsmadd	f1,  A7, A4, f1
	LFPDUX	A7,  BO, INC4
	fxcpmadd	f2,  A8, A4, f2
	fxcsmadd	f3,  A8, A4, f3
	LFPDUX	A8, BO2, INC4
	LFPDUX	A4, AO2, INC4
	bdnz+	.L1032
	.align 4

.L1033:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f1,  B1, A1, f1
	fxcpmadd	f2,  B2, A1, f2
	fxcsmadd	f3,  B2, A1, f3

	fxcpmadd	f0,  B3, A2, f0
	fxcsmadd	f1,  B3, A2, f1
	fxcpmadd	f2,  B4, A2, f2
	fxcsmadd	f3,  B4, A2, f3

	fxcpmadd	f0,  A5, A3, f0
	fxcsmadd	f1,  A5, A3, f1
	fxcpmadd	f2,  A6, A3, f2
	fxcsmadd	f3,  A6, A3, f3

	fxcpmadd	f0,  A7, A4, f0
	fxcsmadd	f1,  A7, A4, f1
	fxcpmadd	f2,  A8, A4, f2
	fxcsmadd	f3,  A8, A4, f3
	.align 4

.L1034:
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

#if defined(TRMMKERNEL)
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
	ble+	.L1038

	LFPDX	A1,  AO,  INC4
	LFPDUX	B1,  BO,  INC4
	LFPDUX	B2,  BO2, INC4
	add	AO, AO, INC2
	bdz-	.L1037
	.align 4

.L1036:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f1,  B1, A1, f1
	LFPDUX	B1,  BO,  INC4
	fxcpmadd	f2,  B2, A1, f2
	fxcsmadd	f3,  B2, A1, f3
	LFPDX	A1,  AO,  INC4
	LFPDUX	B2,  BO2, INC4
	add	AO, AO, INC2
	bdnz+	.L1036
	.align 4

.L1037:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f1,  B1, A1, f1
	fxcpmadd	f2,  B2, A1, f2
	fxcsmadd	f3,  B2, A1, f3
	.align 4

.L1038:
#ifndef TRMMKERNEL
	LFDUX	A1, CO1, INC
	LFDUX	A2, CO2, INC
	LFDUX	A3, CO3, INC
	LFDUX	A4, CO4, INC

	LFSDUX	A1, CO1, INC
	LFSDUX	A2, CO2, INC
	LFSDUX	A3, CO3, INC
	LFSDUX	A4, CO4, INC

	fxcpmadd	f0, AP, f0, A1
	fxcpmadd	f1, AP, f1, A2
	fxcpmadd	f2, AP, f2, A3
	fxcpmadd	f3, AP, f3, A4

	STFDUX	f0,  CO1, INCM1
	STFSDUX	f0,  CO1, INC

	STFDUX	f1,  CO2, INCM1
	STFSDUX	f1,  CO2, INC

	STFDUX	f2,  CO3, INCM1
	STFSDUX	f2,  CO3, INC

	STFDUX	f3,  CO4, INCM1
	STFSDUX	f3,  CO4, INC
#else
	fpmul	f0, AP, f0
	fpmul	f1, AP, f1
	fpmul	f2, AP, f2
	fpmul	f3, AP, f3

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC

	STFDUX	f1,  CO2, INC
	STFSDUX	f1,  CO2, INC

	STFDUX	f2,  CO3, INC
	STFSDUX	f2,  CO3, INC

	STFDUX	f3,  CO4, INC
	STFSDUX	f3,  CO4, INC
#endif


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

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L1040:
	andi.	I, M,  1
	beq	.L1049

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	AO2, AO,   2 * SIZE
	fpmr	f1,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f2,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f3,  f0
#else
	slwi	TEMP, KK, 0 + BASE_SHIFT
	slwi	r0,   KK, 2 + BASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	AO2, AO,   2 * SIZE
	fpmr	f1,  f0
	addi	BO,  BO, - 4 * SIZE
	fpmr	f2,  f0
	addi	BO2, BO,   2 * SIZE
	fpmr	f3,  f0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 4
#endif
	srawi.	r0,  TEMP,  3
	mtspr	CTR, r0
	ble	.L1044

#else
	addi	AO2, AO,   2 * SIZE
	fpmr	f1,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f2,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f3,  f0

	srawi.	r0,  K,  3
	mtspr	CTR, r0
	ble	.L1044
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
	bdz-	.L1043
	.align 4

.L1042:
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
	bdnz+	.L1042
	.align 4

.L1043:
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

.L1044:
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 4
#endif
	andi.	TEMP,  TEMP,  7
	mtspr	CTR, TEMP
#else
	andi.	r0,  K,  7
	mtspr	CTR, r0
#endif
	ble+	.L1048

	LFDX	A1,  AO,  INC4
	LFPDUX	B1,  BO,  INC4
	LFPDUX	B2,  BO2, INC4
	add	AO, AO, INC
	bdz-	.L1047
	.align 4

.L1046:
	fxcpmadd	f0,  A1, B1, f0
	LFPDUX	B1,  BO,  INC4
	fxcpmadd	f1,  A1, B2, f1
	LFDX	A1,  AO,  INC4
	LFPDUX	B2,  BO2, INC4
	add	AO, AO, INC
	bdnz+	.L1046
	.align 4

.L1047:
	fxcpmadd	f0,  A1, B1, f0
	fxcpmadd	f1,  A1, B2, f1
	.align 4

.L1048:
#ifndef TRMMKERNEL
	LFDX	A1, CO1, INC
	LFDX	B3, CO3, INC
	LFSDX	A1, CO2, INC
	LFSDX	B3, CO4, INC

	fpadd	f0, f0, f2
	fpadd	f1, f1, f3

	fxcpmadd	f0,  AP, f0,  A1
	fxcpmadd	f1,  AP, f1,  B3
#else
	fpadd	f0, f0, f2
	fpadd	f1, f1, f3

	fpmul	f0,  AP, f0
	fpmul	f1,  AP, f1
#endif

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO2, INC
	STFDUX	f1,  CO3, INC
	STFSDUX	f1,  CO4, INC

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
	addi	KK, KK, 1
#endif
#endif
	.align 4

.L1049:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addi	KK, KK, 4
#endif

	addi	B,  BO, 4 * SIZE

	addic.	J, J, -1
	bgt+	.L1010
	.align 4

.L1050:
	andi.	J, N,  2
	beq	.L1090

	mr	CO1, C
	add	CO2, C,   LDC
	add	C,   CO2, LDC

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mr	KK, OFFSET
#endif

	addi	AO, A, -2 * SIZE

	li	r0, FZERO
	lfpsx	f0, SP, r0

	srawi.	I, M,  3
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
	slwi	TEMP, KK, 3 + BASE_SHIFT
	slwi	r0,   KK, 1 + BASE_SHIFT
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
	addi	TEMP, KK, 8
#else
	addi	TEMP, KK, 2
#endif
	srawi.	r0,  TEMP,  2
	fpmr	f3,  f0
	mtspr	CTR, r0
	fpmr	f7,  f0
	ble	.L1054
#else
	fpmr	f4,  f0
	addi	BO,  B,  - 2 * SIZE
 	fpmr	f1,  f0
	fpmr	f5,  f0
	fpmr	f2,  f0
	fpmr	f6,  f0

	srawi.	r0,  K,  2
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
	bdnz+	.L1052
	.align 4

.L1053:
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

.L1054:
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 8
#else
	addi	TEMP, KK, 2
#endif
	andi.	TEMP,  TEMP,  3
	mtspr	CTR, TEMP
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
	bdnz+	.L1056
	.align 4

.L1057:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f4,  B1, A1, f4
	fxcpmadd	f1,  B1, A2, f1
	fxcsmadd	f5,  B1, A2, f5

	fxcpmadd	f2,  B1, A3, f2
	fxcsmadd	f6,  B1, A3, f6
	fxcpmadd	f3,  B1, A4, f3
	fxcsmadd	f7,  B1, A4, f7
	.align 4

.L1058:
#ifndef TRMMKERNEL
	LFDUX	A1, CO1, INC
	LFDUX	B1, CO1, INC2
	LFDUX	A3, CO1, INC2
   	LFDUX	A5, CO1, INC2

	LFSDUX	A1, CO1, INCM5
	LFSDUX	B1, CO1, INC2
	LFSDUX	A3, CO1, INC2
	LFSDUX	A5, CO1, INC2

 	LFDUX	B3, CO2, INC
	LFDUX	A6, CO2, INC2
	LFDUX	A7, CO2, INC2
	LFDUX	B2, CO2, INC2

	fxcpmadd	f0,  AP, f0,  A1
	LFSDUX	B3, CO2, INCM5
	LFSDUX	A6, CO2, INC2
	fxcpmadd	f1,  AP, f1,  B1
	LFSDUX	A7, CO2, INC2
	LFSDUX	B2, CO2, INC2

	fxcpmadd	f2,  AP, f2,  A3
	STFDUX	f0,  CO1, INCM7
	STFSDUX	f0,  CO1, INC

	fxcpmadd	f3,  AP, f3,  A5
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC

	fxcpmadd	f4,  AP, f4,  B3
	STFDUX	f2,  CO1, INC
	STFSDUX	f2,  CO1, INC

	fxcpmadd	f5,  AP, f5,  A6
	STFDUX	f3,  CO1, INC
	STFSDUX	f3,  CO1, INC

	fxcpmadd	f6,  AP, f6,  A7
	STFDUX	f4,  CO2, INCM7
	STFSDUX	f4,  CO2, INC

	fxcpmadd	f7,  AP, f7,  B2
	STFDUX	f5,  CO2, INC
	STFSDUX	f5,  CO2, INC

	STFDUX	f6,  CO2, INC
	STFSDUX	f6,  CO2, INC

	STFDUX	f7,  CO2, INC
	STFSDUX	f7,  CO2, INC
#else
	fpmul	f0,  AP, f0
	fpmul	f1,  AP, f1

	fpmul	f2,  AP, f2
	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC

	fpmul	f3,  AP, f3
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC

	fpmul	f4,  AP, f4
	STFDUX	f2,  CO1, INC
	STFSDUX	f2,  CO1, INC

	fpmul	f5,  AP, f5
	STFDUX	f3,  CO1, INC
	STFSDUX	f3,  CO1, INC

	fpmul	f6,  AP, f6
	STFDUX	f4,  CO2, INC
	STFSDUX	f4,  CO2, INC

	fpmul	f7,  AP, f7
	STFDUX	f5,  CO2, INC
	STFSDUX	f5,  CO2, INC

	STFDUX	f6,  CO2, INC
	STFSDUX	f6,  CO2, INC

	STFDUX	f7,  CO2, INC
	STFSDUX	f7,  CO2, INC
#endif


#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -8
#else
	addi	TEMP, TEMP, -2
#endif
	slwi	r0,   TEMP, 3 + BASE_SHIFT
	slwi	TEMP, TEMP, 1 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 8
#endif
#endif

	addic.	I, I, -1
	li	r0, FZERO

	lfpsx	f0, SP, r0
	bgt+	.L1051
	.align 4

.L1060:
	andi.	I, M,  4
	beq	.L1070

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	BO,  B,  - 2 * SIZE
 	fpmr	f1,  f0
#else
	slwi	TEMP, KK, 2 + BASE_SHIFT
	slwi	r0,   KK, 1 + BASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	BO,  BO,  - 2 * SIZE
 	fpmr	f1,  f0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 2
#endif
	fpmr	f2,  f0
	srawi.	r0,  TEMP,  2
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
	bdnz+	.L1062
	.align 4

.L1063:
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

.L1064:
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

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
	ble+	.L1068

	LFPDUX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	LFPDUX	A2,  AO,  INC2
	bdz-	.L1067
	.align 4

.L1066:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f2,  B1, A1, f2
	LFPDUX	A1,  AO,  INC2
	fxcpmadd	f1,  B1, A2, f1
	fxcsmadd	f3,  B1, A2, f3
	LFPDUX	B1,  BO,  INC2
	LFPDUX	A2,  AO,  INC2
	bdnz+	.L1066
	.align 4

.L1067:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f2,  B1, A1, f2
	fxcpmadd	f1,  B1, A2, f1
	fxcsmadd	f3,  B1, A2, f3
	.align 4

.L1068:
#ifndef TRMMKERNEL
	LFDUX	A1, CO1, INC
	LFDUX	A2, CO1, INC2
	LFDUX	A3, CO2, INC
	LFDUX	A4, CO2, INC2

	LFSDUX	A1, CO1, INCM1
	LFSDUX	A2, CO1, INC2
	LFSDUX	A3, CO2, INCM1
	LFSDUX	A4, CO2, INC2

	fxcpmadd	f0,  AP, f0,  A1
	fxcpmadd	f1,  AP, f1,  A2
	fxcpmadd	f2,  AP, f2,  A3
	STFDUX	f0,  CO1, INCM3
	STFSDUX	f0,  CO1, INC

 	fxcpmadd	f3,  AP, f3,  A4
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC

	STFDUX	f2,  CO2, INCM3
	STFSDUX	f2,  CO2, INC

	STFDUX	f3,  CO2, INC
	STFSDUX	f3,  CO2, INC
#else
	fpmul	f0,  AP, f0
	fpmul	f1,  AP, f1
	fpmul	f2,  AP, f2
	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC

 	fpmul	f3,  AP, f3
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC

	STFDUX	f2,  CO2, INC
	STFSDUX	f2,  CO2, INC

	STFDUX	f3,  CO2, INC
	STFSDUX	f3,  CO2, INC
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
	slwi	r0,   TEMP, 2 + BASE_SHIFT
	slwi	TEMP, TEMP, 1 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 4
#endif
#endif

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L1070:
	andi.	I, M,  2
	beq	.L1080

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
#else
	slwi	TEMP, KK, 1 + BASE_SHIFT
	slwi	r0,   KK, 1 + BASE_SHIFT
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
	addi	TEMP, KK, 2
#endif
	srawi.	r0,  TEMP,  3
	fpmr	f2,  f0
	mtspr	CTR, r0
	fpmr	f3, f0
	ble	.L1074
#else
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0

	srawi.	r0,  K,  3
	fpmr	f2,  f0
	mtspr	CTR, r0
	fpmr	f3, f0
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
	bdnz+	.L1072
	.align 4

.L1073:
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

.L1074:
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 2
#endif
	andi.	TEMP,  TEMP,  7
	mtspr	CTR, TEMP
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
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f1,  B1, A1, f1
	LFPDUX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	bdnz+	.L1076
	.align 4

.L1077:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f1,  B1, A1, f1
	.align 4

.L1078:
#ifndef TRMMKERNEL
	LFDUX	A1, CO1, INC
	LFDUX	B3, CO2, INC
	LFSDUX	A1, CO1, INC
	LFSDUX	B3, CO2, INC

	fpadd	f0, f0, f2
	fpadd	f1, f1, f3

	fxcpmadd	f0,  AP, f0,  A1
	fxcpmadd	f1,  AP, f1,  B3

	STFDUX	f0,  CO1, INCM1
	STFSDUX	f0,  CO1, INC
	STFDUX	f1,  CO2, INCM1
	STFSDUX	f1,  CO2, INC
#else
	fpadd	f0, f0, f2
	fpadd	f1, f1, f3

	fpmul	f0,  AP, f0
	fpmul	f1,  AP, f1

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
	STFDUX	f1,  CO2, INC
	STFSDUX	f1,  CO2, INC
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
	slwi	r0,   TEMP, 1 + BASE_SHIFT
	slwi	TEMP, TEMP, 1 + BASE_SHIFT
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

.L1080:
	andi.	I, M,  1
	beq	.L1089

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0
#else
	slwi	TEMP, KK, 0 + BASE_SHIFT
	slwi	r0,   KK, 1 + BASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	BO,  BO,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 2
#endif
	srawi.	r0,  TEMP,  3
	mtspr	CTR, r0
	ble	.L1084
#else
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0

	srawi.	r0,  K,  3
	mtspr	CTR, r0
	ble	.L1084
#endif

	LFPDUX	B1,  BO,  INC2
	LFPDUX	A1,  AO,  INC2
	LFPDUX	A2,  AO,  INC2

	LFPDUX	B2,  BO,  INC2
	LFPDUX	A3,  AO,  INC2
	LFPDUX	A4,  AO,  INC2

	LFPDUX	B3,  BO,  INC2
	LFPDUX	B4,  BO,  INC2
	bdz-	.L1083
	.align 4

.L1082:
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
	bdnz+	.L1082
	.align 4

.L1083:
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

.L1084:
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 2
#endif
	andi.	TEMP,  TEMP,  7
	mtspr	CTR, TEMP
#else
	andi.	r0,  K,  7
	mtspr	CTR, r0
#endif
	ble+	.L1088

	LFDX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	add	AO, AO, INC
	bdz-	.L1087
	.align 4

.L1086:
	fxcpmadd	f0,  A1, B1, f0
	LFDX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	add	AO, AO, INC
	bdnz+	.L1086
	.align 4

.L1087:
	fxcpmadd	f0,  A1, B1, f0
	.align 4

.L1088:
#ifndef TRMMKERNEL
	LFDX	A1, CO1, INC
	LFDX	A2, CO2, INC

	fpadd	f0, f0, f1
	fpadd	f2, f2, f3
	fsmfp	A1, A2
	fpadd	f0, f0, f2
	fxcpmadd	f0,  AP, f0,  A1
#else
	fpadd	f0, f0, f1
	fpadd	f2, f2, f3
	fsmfp	A1, A2
	fpadd	f0, f0, f2
	fpmul	f0,  AP, f0
#endif

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO2, INC

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

.L1089:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addi	KK, KK, 2
#endif

	addi	B,  BO, 2 * SIZE
	.align 4

.L1090:
	andi.	J, N,  1
	beq	.L10999

#if defined(TRMMKERNEL) &&  defined(LEFT)
	mr	KK, OFFSET
#endif

	mr	CO1, C
	addi	AO, A, -2 * SIZE

	li	r0, FZERO
	lfpsx	f0, SP, r0

	srawi.	I, M,  3
	ble	.L10100
	.align 4

.L1091:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	BO,  B,  - 2 * SIZE
 	fpmr	f1,  f0
#else
	slwi	TEMP, KK, 3 + BASE_SHIFT
	slwi	r0,   KK, 0 + BASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	addi	BO,  BO,  - 2 * SIZE
 	fpmr	f1,  f0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 8
#else
	addi	TEMP, KK, 1
#endif
	fpmr	f2,  f0
	srawi.	r0,  TEMP,  2
	fpmr	f3,  f0
	mtspr	CTR, r0
	ble	.L1094

#else
	srawi.	r0,  K,  2
 	fpmr	f1,  f0
	addi	BO,  B,  - 2 * SIZE
	fpmr	f2,  f0
	fpmr	f3,  f0
	mtspr	CTR, r0
	ble	.L1094
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
	bdz-	.L1093
	.align 4

.L1092:
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
	bdnz+	.L1092
	.align 4

.L1093:
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

.L1094:
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 8
#else
	addi	TEMP, KK, 1
#endif
	andi.	TEMP,  TEMP,  3
	mtspr	CTR, TEMP
#else
	andi.	r0,  K,  3
	mtspr	CTR, r0
#endif
	ble+	.L1098

	LFDX	B1,  BO,  INC2
	LFPDUX	A1,  AO,  INC2
	LFPDUX	A2,  AO,  INC2
	LFPDUX	A3,  AO,  INC2
	LFPDUX	A4,  AO,  INC2
	add	BO, BO, INC
	bdz-	.L1097
	.align 4

.L1096:
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
	bdnz+	.L1096
	.align 4

.L1097:
	fxcpmadd	f0,  B1, A1, f0
	fxcpmadd	f1,  B1, A2, f1
	fxcpmadd	f2,  B1, A3, f2
	fxcpmadd	f3,  B1, A4, f3
	.align 4

.L1098:
#ifndef TRMMKERNEL
	LFDUX	A1, CO1, INC
	LFDUX	B1, CO1, INC2
	LFDUX	A3, CO1, INC2
   	LFDUX	A5, CO1, INC2

	LFSDUX	A1, CO1, INCM5
	LFSDUX	B1, CO1, INC2
	LFSDUX	A3, CO1, INC2
	LFSDUX	A5, CO1, INC2

	fxcpmadd	f0,  AP, f0,  A1
	fxcpmadd	f1,  AP, f1,  B1
	fxcpmadd	f2,  AP, f2,  A3
	STFDUX	f0,  CO1, INCM7
	STFSDUX	f0,  CO1, INC

	fxcpmadd	f3,  AP, f3,  A5
#else
	fpmul	f0,  AP, f0
	fpmul	f1,  AP, f1
	fpmul	f2,  AP, f2
	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC

	fpmul	f3,  AP, f3
#endif

	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC

	STFDUX	f2,  CO1, INC
	STFSDUX	f2,  CO1, INC

	STFDUX	f3,  CO1, INC
	STFSDUX	f3,  CO1, INC

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -8
#else
	addi	TEMP, TEMP, -1
#endif
	slwi	r0,   TEMP, 3 + BASE_SHIFT
	slwi	TEMP, TEMP, 0 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 8
#endif
#endif

	addic.	I, I, -1
	li	r0, FZERO

	lfpsx	f0, SP, r0
	bgt+	.L1091
	.align 4

.L10100:
	andi.	I, M,  4
	beq	.L10110

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3, f0
#else
	slwi	TEMP, KK, 2 + BASE_SHIFT
	slwi	r0,   KK, 0 + BASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	fpmr	f1,  f0
	addi	BO,  BO,  - 2 * SIZE
	fpmr	f2,  f0
	fpmr	f3, f0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 1
#endif
	srawi.	r0,  TEMP,  3
	mtspr	CTR, r0
	ble	.L10104
#else
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3, f0

	srawi.	r0,  K,  3
	mtspr	CTR, r0
	ble	.L10104
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

	bdz-	.L10103
	.align 4

.L10102:
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
	bdnz+	.L10102
	.align 4

.L10103:
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

.L10104:
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 4
#else
	addi	TEMP, KK, 1
#endif
	andi.	TEMP,  TEMP,  7
	mtspr	CTR, TEMP
#else
	andi.	r0,  K,  7
	mtspr	CTR, r0
#endif
	ble+	.L10108

	LFPDUX	A1,  AO,  INC2
	LFDX	B1,  BO,  INC2
	LFPDUX	A2,  AO,  INC2
	add	BO, BO, INC
	bdz-	.L10107
	.align 4

.L10106:
	fxcpmadd	f0,  B1, A1, f0
	LFPDUX	A1,  AO,  INC2
	fxcpmadd	f1,  B1, A2, f1
	LFDX	B1,  BO,  INC2
	LFPDUX	A2,  AO,  INC2
	add	BO, BO, INC
	bdnz+	.L10106
	.align 4

.L10107:
	fxcpmadd	f0,  B1, A1, f0
	fxcpmadd	f1,  B1, A2, f1
	.align 4

.L10108:
#ifndef TRMMKERNEL
	LFDUX	A1, CO1, INC
	LFDUX	B1, CO1, INC2
	LFSDUX	A1, CO1, INCM1
	LFSDUX	B1, CO1, INC2

	fpadd	f0, f0, f2
	fpadd	f1, f1, f3

	fxcpmadd	f0,  AP, f0,  A1
	fxcpmadd	f1,  AP, f1,  B1

	STFDUX	f0,  CO1, INCM3
	STFSDUX	f0,  CO1, INC
#else
	fpadd	f0, f0, f2
	fpadd	f1, f1, f3

	fpmul	f0,  AP, f0
	fpmul	f1,  AP, f1

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
#endif

	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC

#ifdef TRMMKERNEL
#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	sub	TEMP, K, KK
#ifdef LEFT
	addi	TEMP, TEMP, -4
#else
	addi	TEMP, TEMP, -1
#endif
	slwi	r0,   TEMP, 2 + BASE_SHIFT
	slwi	TEMP, TEMP, 0 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 4
#endif
#endif

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L10110:
	andi.	I, M,  2
	beq	.L10120

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0
#else
	slwi	TEMP, KK, 1 + BASE_SHIFT
	slwi	r0,   KK, 0 + BASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	fpmr	f1,  f0
	addi	BO,  BO,  - 2 * SIZE
	fpmr	f2,  f0
	fpmr	f3,  f0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 1
#endif
	srawi.	r0,  TEMP,  3
	mtspr	CTR, r0
	ble	.L10114
#else
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0

	srawi.	r0,  K,  3
	mtspr	CTR, r0
	ble	.L10114
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
	bdz-	.L10113
	.align 4

.L10112:
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
	bdnz+	.L10112
	.align 4

.L10113:
	fxcpmadd	f0,  B1, A1, f0
	fxcsmadd	f1,  B1, A2, f1
	fxcpmadd	f2,  B2, A3, f2
	fxcsmadd	f3,  B2, A4, f3
	fxcpmadd	f0,  B3, A5, f0
	fxcsmadd	f1,  B3, A6, f1
	fxcpmadd	f2,  B4, A7, f2
	fxcsmadd	f3,  B4, A8, f3
	.align 4

.L10114:
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 2
#else
	addi	TEMP, KK, 1
#endif
	andi.	TEMP,  TEMP,  7
	mtspr	CTR, TEMP
#else
	andi.	r0,  K,  7
	mtspr	CTR, r0
#endif
	ble+	.L10118

	LFPDUX	A1,  AO,  INC2
	LFDX	B1,  BO,  INC2
	add	BO, BO, INC
	bdz-	.L10117
	.align 4

.L10116:
	fxcpmadd	f0,  B1, A1, f0
	LFPDUX	A1,  AO,  INC2
	LFDX	B1,  BO,  INC2
	add	BO, BO, INC
	bdnz+	.L10116
	.align 4

.L10117:
	fxcpmadd	f0,  B1, A1, f0
	.align 4

.L10118:
#ifndef TRMMKERNEL
	LFDUX	A1, CO1, INC
	LFDUX	A2, CO1, INC

	fpadd	f0, f0, f1
	fpadd	f2, f3, f2
	fsmfp	A1, A2
	fpadd	f0, f0, f2
	fxcpmadd	f1,  AP, f0,  A1

	li	r0, FZERO
	lfpsx	f0, SP, r0

	STFDUX	f1,  CO1, INCM1
	STFSDUX	f1,  CO1, INC
#else
	fpadd	f0, f0, f1
	fpadd	f2, f3, f2
	fsmfp	A1, A2
	fpadd	f0, f0, f2
	fpmul	f1,  AP, f0

	li	r0, FZERO
	lfpsx	f0, SP, r0

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
	slwi	r0,   TEMP, 1 + BASE_SHIFT
	slwi	TEMP, TEMP, 0 + BASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LEFT
	addi	KK, KK, 2
#endif
#endif
	.align 4

.L10120:
	andi.	I, M,  1
	beq	.L10999

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0
#else
	slwi	TEMP, KK, 0 + BASE_SHIFT
	slwi	r0,   KK, 0 + BASE_SHIFT
	add	AO, AO, TEMP
	add	BO, B,  r0

	fpmr	f1,  f0
	addi	BO,  BO,  - 2 * SIZE
	fpmr	f2,  f0
	fpmr	f3,  f0
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	TEMP, K, KK
#elif defined(LEFT)
	addi	TEMP, KK, 1
#else
	addi	TEMP, KK, 1
#endif
	srawi.	r0,  TEMP,  3
	mtspr	CTR, r0
	ble	.L10124
#else
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0

	srawi.	r0,  K,  3
	mtspr	CTR, r0
	ble	.L10124
#endif

	LFPDUX	A1,  AO,  INC2
	LFPDUX	B1,  BO,  INC2
	LFPDUX	A2,  AO,  INC2
	LFPDUX	B2,  BO,  INC2
	LFPDUX	A3,  AO,  INC2
	LFPDUX	B3,  BO,  INC2
	LFPDUX	A4,  AO,  INC2
	LFPDUX	B4,  BO,  INC2
	bdz-	.L10123
	.align 4

.L10122:
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
	bdnz+	.L10122
	.align 4

.L10123:
	fpmadd	f0,  A1, B1, f0
	fpmadd	f1,  A2, B2, f1
	fpmadd	f2,  A3, B3, f2
	fpmadd	f3,  A4, B4, f3
	.align 4

.L10124:
	lfd	AP,  ALPHA(SP)
#ifdef TRMMKERNEL
       fsmfp	AP, AP
#endif

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
	ble+	.L10128

	LFDX	A1,  AO,  INC2
	LFDX	B1,  BO,  INC2
	add	AO, AO, INC
	add	BO, BO, INC
	bdz-	.L10127
	.align 4

.L10126:
	fmadd	f0,  A1, B1, f0
	LFDX	A1,  AO,  INC2
	LFDX	B1,  BO,  INC2
	add	AO, AO, INC
	add	BO, BO, INC
	bdnz+	.L10126
	.align 4

.L10127:
	fmadd	f0,  A1, B1, f0
	.align 4

.L10128:
#ifndef TRMMKERNEL
	LFDX	A1, CO1, INC
	fpadd	f0, f0, f1
	fpadd	f2, f2, f3
	fpadd	f0, f0, f2
	fsmtp	f1, f0
	fadd	f0, f0, f1
	fmadd	f0,  AP, f0,  A1
	STFDUX	f0,  CO1, INC
#else
	fpadd	f0, f0, f1
	fpadd	f2, f2, f3
	fpadd	f0, f0, f2
	fsmtp	f1, f0
	fadd	f0, f0, f1
	fmul	f0,  AP, f0
	STFDUX	f0,  CO1, INC
#endif
	.align 4

.L10999:
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
