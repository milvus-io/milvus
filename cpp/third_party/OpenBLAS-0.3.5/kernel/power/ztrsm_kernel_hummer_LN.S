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
#define AORIG	r12
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

#ifndef CONJ
#define FXCPMADD	fxcpmadd
#define FXCSMADD	fxcxnpma
#else
#if defined(LN) || defined(LT)
#define FXCPMADD	fxcpnsma
#define FXCSMADD	fxcxma
#else
#define FXCPMADD	fxcpmadd
#define FXCSMADD	fxcxnsma
#endif
#endif

#ifndef CONJ
#define FXCXNPMA	fxcxnpma
#define FXCXNSMA	fxcxnsma
#else
#define FXCXNPMA	fxcxnsma
#define FXCXNSMA	fxcxnpma
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

	li	INC,    1 * SIZE
	li	INC2,   2 * SIZE
	li	INC4,   4 * SIZE
	li	INCM1, -1 * SIZE
	li	INCM3, -3 * SIZE
	li	INCM5, -5 * SIZE
	li	INCM7, -7 * SIZE

	addi	C, C, - 1 * SIZE

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

	srawi.	J, N,  1
	ble	.L50
	.align 4

.L10:
#ifdef RT
	slwi	r0, K, 1 + ZBASE_SHIFT
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
	addi	AORIG, A, -4 * SIZE
#else
	addi	AO, A, -4 * SIZE
#endif
#ifndef RT
	add	C,  CO2, LDC
#endif

	li	r0, FZERO
	lfpsx	f0, SP, r0


	andi.	I, M,  1
	beq	.L20

#if defined(LT) || defined(RN)
	addi	AO2, AO,   2 * SIZE
	fpmr	f1,  f0
	addi	BO,  B,  - 4 * SIZE
	fpmr	f2,  f0
	addi	BO2, B,  - 2 * SIZE
	fpmr	f3, f0

	srawi.	r0,  KK,  2
	mtspr	CTR, r0
	ble	.L34
#else
#ifdef LN
	slwi	r0,   K,  0 + ZBASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0  , KK, 0 + ZBASE_SHIFT
	slwi	TEMP, KK, 1 + ZBASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

	addi	AO2, AO,   2 * SIZE
	fpmr	f1,  f0
	addi	BO,  BO,  - 4 * SIZE
	fpmr	f2,  f0
	addi	BO2, BO,    2 * SIZE
	fpmr	f3, f0

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
	fpadd	f0, f0, f1
	fpadd	f2, f2, f3

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
	addi	BO,  BO, - 4 * SIZE
#endif

	addi	AO2, AO,   2 * SIZE
	addi	BO2, BO,   2 * SIZE

#if defined(LN) || defined(LT)
	LFPDX	f16, BO,  INC4
	LFPDX	f17, BO2, INC4
#else
	LFPDX	f16, AO,  INC4
	LFPDX	f17, AO2, INC4
#endif

	fpsub	f0,  f16,  f0
	fpsub	f2,  f17,  f2

#ifdef LN
	LFPDX	A1,  AO,  INC4

	fxpmul	  f4,  A1, f0
	fxpmul	  f5,  A1, f2
	FXCXNPMA  f0,  A1, f0,  f4
	FXCXNPMA  f2,  A1, f2,  f5
#endif

#ifdef LT
	LFPDX	A1,  AO,  INC4

	fxpmul	  f4,  A1, f0
	fxpmul	  f5,  A1, f2
	FXCXNPMA  f0,  A1, f0, f4
	FXCXNPMA  f2,  A1, f2, f5
#endif

#ifdef RN
	LFPDUX	A1,  BO,  INC4
	LFPDUX	A2,  BO2, INC4
	add	BO,  BO,  INC4
	LFPDUX	A3,  BO2, INC4

	subi	BO,  BO,   8 * SIZE
	subi	BO2, BO2,  8 * SIZE

	fxpmul	  f4,  A1, f0
	FXCXNPMA  f0,  A1, f0, f4

	fxcpnmsub f2,  A2, f0, f2
	FXCXNSMA  f2,  A2, f0, f2

	fxpmul	  f4,  A3, f2
	FXCXNPMA  f2,  A3, f2,  f4
#endif

#ifdef RT
	LFPDUX	A1,  BO,  INC4
	add	BO2, BO2, INC4
	LFPDUX	A2,  BO,  INC4
	LFPDUX	A3,  BO2, INC4

	subi	BO,  BO,   8 * SIZE
	subi	BO2, BO2,  8 * SIZE

	fxpmul	  f4,  A3, f2
	FXCXNPMA  f2,  A3, f2,  f4

	fxcpnmsub f0,  A2, f2,  f0
	FXCXNSMA  f0,  A2, f2,  f0

	fxpmul	  f4,  A1, f0
	FXCXNPMA  f0,  A1, f0,  f4
#endif

#ifdef LN
	subi	CO1, CO1, 2 * SIZE
	subi	CO2, CO2, 2 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFPDX	f0,  BO,  INC4
	STFPDX	f2,  BO2, INC4
#else
	STFPDX	f0,  AO,  INC4
	STFPDX	f2,  AO2, INC4
#endif

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
	STFDUX	f2,  CO2, INC
	STFSDUX	f2,  CO2, INC

#ifdef LN
	subi	CO1, CO1, 2 * SIZE
	subi	CO2, CO2, 2 * SIZE
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

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L20:
	andi.	I, M,  2
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
	slwi	r0,   K,  1 + ZBASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0  , KK, 1 + ZBASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     r0

	sub	TEMP, K, KK

	addi	AO2, AO,   2 * SIZE
	fpmr	f4,  f0
	addi	BO,  BO,  - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, BO,    2 * SIZE
	fpmr	f12, f0

 	fpmr	f1,  f0
	fpmr	f5,  f0
	fpmr	f9,  f0
	fpmr	f13, f0
	srawi.	r0,  TEMP,  2
	mtspr	CTR, r0
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
	fpadd	f0, f0, f4
	fpadd	f8, f8, f12
	fpadd	f1, f1, f5
	fpadd	f9, f9, f13

#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 2
#else
	subi	r0, KK, 2
#endif
	slwi	r0,   r0, 1 + ZBASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     r0
	addi	AO2, AO,   2 * SIZE
	addi	BO,  BO, - 4 * SIZE
	addi	BO2, BO,   2 * SIZE
#endif

#if defined(LN) || defined(LT)
	LFPDUX	f16, BO,  INC4
	LFPDUX	f18, BO2, INC4
	LFPDUX	f17, BO,  INC4
	LFPDUX	f19, BO2, INC4

	subi	BO,  BO,   8 * SIZE
	subi	BO2, BO2,  8 * SIZE
#else
	LFPDUX	f16, AO,  INC4
	LFPDUX	f17, AO2, INC4
	LFPDUX	f18, AO,  INC4
	LFPDUX	f19, AO2, INC4

	subi	AO,  AO,   8 * SIZE
	subi	AO2, AO2,  8 * SIZE
#endif

	fpsub	f0,  f16,  f0
	fpsub	f1,  f17,  f1
	fpsub	f8,  f18,  f8
	fpsub	f9,  f19,  f9

#ifdef LN
	LFPDUX	A1,  AO,  INC4
	add	AO2, AO2, INC4
	LFPDUX	A2,  AO,  INC4
	LFPDUX	A3,  AO2, INC4

	subi	AO,  AO,   8 * SIZE
	subi	AO2, AO2,  8 * SIZE

	fxpmul	  f4,  A3, f1
	fxpmul	  f5,  A3, f9
	FXCXNPMA  f1,  A3, f1,  f4
	FXCXNPMA  f9,  A3, f9,  f5

	fxcpnmsub f0,  A2, f1,  f0
	fxcpnmsub f8,  A2, f9,  f8
	FXCXNSMA  f0,  A2, f1,  f0
	FXCXNSMA  f8,  A2, f9,  f8

	fxpmul	  f4,  A1, f0
	fxpmul	  f5,  A1, f8
	FXCXNPMA  f0,  A1, f0,  f4
	FXCXNPMA  f8,  A1, f8,  f5
#endif

#ifdef LT
	LFPDUX	A1,  AO,  INC4
	LFPDUX	A2,  AO2, INC4
	add	AO,  AO,  INC4
	LFPDUX	A3,  AO2, INC4

	subi	AO,  AO,   8 * SIZE
	subi	AO2, AO2,  8 * SIZE

	fxpmul	  f4,  A1, f0
	fxpmul	  f5,  A1, f8
	FXCXNPMA  f0,  A1, f0, f4
	FXCXNPMA  f8,  A1, f8, f5

	fxcpnmsub f1,  A2, f0, f1
	fxcpnmsub f9,  A2, f8, f9
	FXCXNSMA  f1,  A2, f0, f1
	FXCXNSMA  f9,  A2, f8, f9

	fxpmul	  f6,  A3, f1
	fxpmul	  f7,  A3, f9
	FXCXNPMA  f1,  A3, f1, f6
	FXCXNPMA  f9,  A3, f9, f7
#endif

#ifdef RN
	LFPDUX	A1,  BO,  INC4
	LFPDUX	A2,  BO2, INC4
	add	BO,  BO,  INC4
	LFPDUX	A3,  BO2, INC4

	subi	BO,  BO,   8 * SIZE
	subi	BO2, BO2,  8 * SIZE

	fxpmul	  f4,  A1, f0
	fxpmul	  f5,  A1, f1

	FXCXNPMA  f0,  A1, f0, f4
	FXCXNPMA  f1,  A1, f1, f5

	fxcpnmsub f8,  A2, f0, f8
	fxcpnmsub f9,  A2, f1, f9

	FXCXNSMA  f8,  A2, f0, f8
	FXCXNSMA  f9,  A2, f1, f9

	fxpmul	  f4,  A3, f8
	fxpmul	  f5,  A3, f9

	FXCXNPMA  f8,  A3, f8,  f4
	FXCXNPMA  f9,  A3, f9,  f5
#endif

#ifdef RT
	LFPDUX	A1,  BO,  INC4
	add	BO2, BO2, INC4
	LFPDUX	A2,  BO,  INC4
	LFPDUX	A3,  BO2, INC4

	subi	BO,  BO,   8 * SIZE
	subi	BO2, BO2,  8 * SIZE

	fxpmul	  f4,  A3, f8
	fxpmul	  f5,  A3, f9

	FXCXNPMA  f8,  A3, f8,  f4
	FXCXNPMA  f9,  A3, f9,  f5

	fxcpnmsub f0,  A2, f8,  f0
	fxcpnmsub f1,  A2, f9,  f1

	FXCXNSMA  f0,  A2, f8,  f0
	FXCXNSMA  f1,  A2, f9,  f1

	fxpmul	  f4,  A1, f0
	fxpmul	  f5,  A1, f1

	FXCXNPMA  f0,  A1, f0,  f4
	FXCXNPMA  f1,  A1, f1,  f5
#endif

#ifdef LN
	subi	CO1, CO1, 4 * SIZE
	subi	CO2, CO2, 4 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFPDUX	f0,  BO,  INC4
	STFPDUX	f8,  BO2, INC4
	STFPDUX	f1,  BO,  INC4
	STFPDUX	f9,  BO2, INC4

	subi	BO,  BO,   8 * SIZE
	subi	BO2, BO2,  8 * SIZE
#else
	STFPDUX	f0,  AO,  INC4
	STFPDUX	f1,  AO2, INC4
	STFPDUX	f8,  AO,  INC4
	STFPDUX	f9,  AO2, INC4

	subi	AO,  AO,   8 * SIZE
	subi	AO2, AO2,  8 * SIZE
#endif

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC

	STFDUX	f8,  CO2, INC
	STFSDUX	f8,  CO2, INC
	STFDUX	f9,  CO2, INC
	STFSDUX	f9,  CO2, INC

#ifdef LN
	subi	CO1, CO1, 4 * SIZE
	subi	CO2, CO2, 4 * SIZE
#endif

#ifdef RT
	slwi	r0, K, 1 + ZBASE_SHIFT
	add	AORIG, AORIG, r0
#endif

#if defined(LT) || defined(RN)
	sub	TEMP, K, KK
	slwi	r0,   TEMP, 1 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, r0
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

.L30:
	srawi.	I, M,  2
	ble	.L49
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

	srawi.	r0,  KK,  2
 	fpmr	f1,  f0
	mtspr	CTR, r0
	ble	.L14
#else

#ifdef LN
	slwi	r0,   K,  2 + ZBASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0  , KK, 2 + ZBASE_SHIFT
	slwi	TEMP, KK, 1 + ZBASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

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

	addi	AO2, AO,    2 * SIZE
	fpmr	f4,  f0
	addi	BO,  BO,  - 4 * SIZE
	fpmr	f8,  f0
	addi	BO2, BO,    2 * SIZE
	fpmr	f12, f0

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
	nop
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
	nop

	FXCPMADD	f1,  B3, A2, f1
	nop
	FXCSMADD	f5,  B3, A2, f5
	LFPDUX	A4, AO2, INC4
	FXCPMADD	f9,  B4, A2, f9
	nop
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
	nop
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
	nop

	FXCPMADD	f1,  B5, A2, f1
	nop
	FXCSMADD	f5,  B5, A2, f5
	LFPDUX	A4, AO2, INC4
	FXCPMADD	f9,  B2, A2, f9
	nop
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
	nop
	FXCSMADD	f15, B2, A4, f15
	nop

## 4 ##

	FXCPMADD	f0,  B6, A9, f0
	nop
	FXCSMADD	f4,  B6, A9, f4
	nop
	FXCPMADD	f8,  B4, A9, f8
	nop
	FXCSMADD	f12, B4, A9, f12
	nop

	FXCPMADD	f1,  B6, A2, f1
	nop
	FXCSMADD	f5,  B6, A2, f5
	LFPDUX	A4, AO2, INC4
	FXCPMADD	f9,  B4, A2, f9
	nop
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
	nop
	FXCSMADD	f7,  B6, A4, f7
	nop
	FXCPMADD	f11, B4, A4, f11
	nop
	FXCSMADD	f15, B4, A4, f15
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
	fpadd	f0,  f0,  f4
	fpadd	f8,  f8,  f12
	fpadd	f1,  f1,  f5
	fpadd	f9,  f9,  f13

	fpadd	f2,  f2,  f6
	fpadd	f10, f10, f14
	fpadd	f3,  f3,  f7
	fpadd	f11, f11, f15

#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 4
#else
	subi	r0, KK, 2
#endif
	slwi	TEMP, r0, 2 + ZBASE_SHIFT
	slwi	r0,   r0, 1 + ZBASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO, B,     r0
	addi	AO2, AO,   2 * SIZE
	addi	BO,  BO, - 4 * SIZE
	addi	BO2, BO,   2 * SIZE
#endif

#if defined(LN) || defined(LT)
	LFPDUX	f16, BO,  INC4
	LFPDUX	f20, BO2, INC4
	LFPDUX	f17, BO,  INC4
	LFPDUX	f21, BO2, INC4
	LFPDUX	f18, BO,  INC4
	LFPDUX	f22, BO2, INC4
	LFPDUX	f19, BO,  INC4
	LFPDUX	f23, BO2, INC4

	subi	BO,  BO,  16 * SIZE
	subi	BO2, BO2, 16 * SIZE
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
#endif

	fpsub	f0,  f16,  f0
	fpsub	f1,  f17,  f1
	fpsub	f2,  f18,  f2
	fpsub	f3,  f19,  f3

	fpsub	f8,  f20,  f8
	fpsub	f9,  f21,  f9
	fpsub	f10, f22,  f10
	fpsub	f11, f23,  f11

#ifdef LN
	LFPDUX	A1,  AO,  INC4
	add	AO2, AO2, INC4
	add	AO,  AO,  INC4
	add	AO2, AO2, INC4

	LFPDUX	A2,  AO,  INC4
	LFPDUX	A3,  AO2, INC4
	add	AO,  AO,  INC4
	add	AO2, AO2, INC4

	LFPDUX	A4,  AO,  INC4
	LFPDUX	A5,  AO2, INC4
	LFPDUX	A6,  AO,  INC4
	add	AO2, AO2, INC4

	LFPDUX	A7,  AO,  INC4
	LFPDUX	A8,  AO2, INC4
	LFPDUX	A9,  AO,  INC4
	LFPDUX	A10, AO2, INC4

	subi	AO,  AO,  32 * SIZE
	subi	AO2, AO2, 32 * SIZE

	fxpmul	  f4,  A10, f3
	fxpmul	  f5,  A10, f11
	FXCXNPMA  f3,  A10, f3,  f4
	FXCXNPMA  f11, A10, f11, f5

	fxcpnmsub f2,  A9, f3,  f2
	fxcpnmsub f10, A9, f11, f10
	FXCXNSMA  f2,  A9, f3,  f2
	FXCXNSMA  f10, A9, f11, f10

	fxcpnmsub f1,  A8, f3,  f1
	fxcpnmsub f9,  A8, f11, f9
	FXCXNSMA  f1,  A8, f3,  f1
	FXCXNSMA  f9,  A8, f11, f9

	fxcpnmsub f0,  A7, f3,  f0
	fxcpnmsub f8,  A7, f11, f8
	FXCXNSMA  f0,  A7, f3,  f0
	FXCXNSMA  f8,  A7, f11, f8

	fxpmul	  f4,  A6, f2
	fxpmul	  f5,  A6, f10
	FXCXNPMA  f2,  A6, f2,  f4
	FXCXNPMA  f10, A6, f10, f5

	fxcpnmsub f1,  A5, f2,  f1
	fxcpnmsub f9,  A5, f10, f9
	FXCXNSMA  f1,  A5, f2,  f1
	FXCXNSMA  f9,  A5, f10, f9

	fxcpnmsub f0,  A4, f2,  f0
	fxcpnmsub f8,  A4, f10, f8
	FXCXNSMA  f0,  A4, f2,  f0
	FXCXNSMA  f8,  A4, f10, f8

	fxpmul	  f4,  A3, f1
	fxpmul	  f5,  A3, f9
	FXCXNPMA  f1,  A3, f1,  f4
	FXCXNPMA  f9,  A3, f9,  f5

	fxcpnmsub f0,  A2, f1,  f0
	fxcpnmsub f8,  A2, f9,  f8
	FXCXNSMA  f0,  A2, f1,  f0
	FXCXNSMA  f8,  A2, f9,  f8

	fxpmul	  f4,  A1, f0
	fxpmul	  f5,  A1, f8
	FXCXNPMA  f0,  A1, f0,  f4
	FXCXNPMA  f8,  A1, f8,  f5
#endif

#ifdef LT
	LFPDUX	A1,  AO,  INC4
	LFPDUX	A2,  AO2, INC4
	LFPDUX	A3,  AO,  INC4
	LFPDUX	A4,  AO2, INC4

	add	AO,  AO,  INC4
	LFPDUX	A5,  AO2, INC4
	LFPDUX	A6,  AO,  INC4
	LFPDUX	A7,  AO2, INC4

	add	AO,  AO,  INC4
	add	AO2, AO2, INC4
	LFPDUX	A8,  AO,  INC4
	LFPDUX	A9,  AO2, INC4

	add	AO,  AO,  INC4
	add	AO2, AO2, INC4
	add	AO,  AO,  INC4
	LFPDUX	A10, AO2, INC4

	subi	AO,  AO,  32 * SIZE
	subi	AO2, AO2, 32 * SIZE

	fxpmul	  f4,  A1, f0
	fxpmul	  f5,  A1, f8
	FXCXNPMA  f0,  A1, f0, f4
	FXCXNPMA  f8,  A1, f8, f5

	fxcpnmsub f1,  A2, f0, f1
	fxcpnmsub f9,  A2, f8, f9
	FXCXNSMA  f1,  A2, f0, f1
	FXCXNSMA  f9,  A2, f8, f9

	fxcpnmsub f2,  A3, f0, f2
	fxcpnmsub f10, A3, f8, f10
	FXCXNSMA  f2,  A3, f0, f2
	FXCXNSMA  f10, A3, f8, f10

	fxcpnmsub f3,  A4, f0, f3
	fxcpnmsub f11, A4, f8, f11
	FXCXNSMA  f3,  A4, f0, f3
	FXCXNSMA  f11, A4, f8, f11

	fxpmul	  f6,  A5, f1
	fxpmul	  f7,  A5, f9
	FXCXNPMA  f1,  A5, f1, f6
	FXCXNPMA  f9,  A5, f9, f7

	fxcpnmsub f2,  A6, f1, f2
	fxcpnmsub f10, A6, f9, f10
	FXCXNSMA  f2,  A6, f1, f2
	FXCXNSMA  f10, A6, f9, f10

	fxcpnmsub f3,  A7, f1, f3
	fxcpnmsub f11, A7, f9, f11
	FXCXNSMA  f3,  A7, f1, f3
	FXCXNSMA  f11, A7, f9, f11

	fxpmul	  f4,  A8, f2
	fxpmul	  f5,  A8, f10
	FXCXNPMA  f2,  A8, f2,  f4
	FXCXNPMA  f10, A8, f10, f5

	fxcpnmsub f3,  A9, f2,  f3
	fxcpnmsub f11, A9, f10, f11
	FXCXNSMA  f3,  A9, f2,  f3
	FXCXNSMA  f11, A9, f10, f11

	fxpmul	  f6,  A10, f3
	fxpmul	  f7,  A10, f11
	FXCXNPMA  f3,  A10, f3,  f6
	FXCXNPMA  f11, A10, f11, f7
#endif

#ifdef RN
	LFPDUX	A1,  BO,  INC4
	LFPDUX	A2,  BO2, INC4
	add	BO,  BO,  INC4
	LFPDUX	A3,  BO2, INC4

	subi	BO,  BO,   8 * SIZE
	subi	BO2, BO2,  8 * SIZE

	fxpmul	  f4,  A1, f0
	fxpmul	  f5,  A1, f1
	fxpmul	  f6,  A1, f2
	fxpmul	  f7,  A1, f3

	FXCXNPMA  f0,  A1, f0, f4
	FXCXNPMA  f1,  A1, f1, f5
	FXCXNPMA  f2,  A1, f2, f6
	FXCXNPMA  f3,  A1, f3, f7

	fxcpnmsub f8,  A2, f0, f8
	fxcpnmsub f9,  A2, f1, f9
	fxcpnmsub f10, A2, f2, f10
	fxcpnmsub f11, A2, f3, f11

	FXCXNSMA  f8,  A2, f0, f8
	FXCXNSMA  f9,  A2, f1, f9
	FXCXNSMA  f10, A2, f2, f10
	FXCXNSMA  f11, A2, f3, f11

	fxpmul	  f4,  A3, f8
	fxpmul	  f5,  A3, f9
	fxpmul	  f6,  A3, f10
	fxpmul	  f7,  A3, f11

	FXCXNPMA  f8,  A3, f8,  f4
	FXCXNPMA  f9,  A3, f9,  f5
	FXCXNPMA  f10, A3, f10, f6
	FXCXNPMA  f11, A3, f11, f7
#endif

#ifdef RT
	LFPDUX	A1,  BO,  INC4
	add	BO2, BO2, INC4
	LFPDUX	A2,  BO,  INC4
	LFPDUX	A3,  BO2, INC4

	subi	BO,  BO,   8 * SIZE
	subi	BO2, BO2,  8 * SIZE

	fxpmul	  f4,  A3, f8
	fxpmul	  f5,  A3, f9
	fxpmul	  f6,  A3, f10
	fxpmul	  f7,  A3, f11

	FXCXNPMA  f8,  A3, f8,  f4
	FXCXNPMA  f9,  A3, f9,  f5
	FXCXNPMA  f10, A3, f10, f6
	FXCXNPMA  f11, A3, f11, f7

	fxcpnmsub f0,  A2, f8,  f0
	fxcpnmsub f1,  A2, f9,  f1
	fxcpnmsub f2,  A2, f10, f2
	fxcpnmsub f3,  A2, f11, f3

	FXCXNSMA  f0,  A2, f8,  f0
	FXCXNSMA  f1,  A2, f9,  f1
	FXCXNSMA  f2,  A2, f10, f2
	FXCXNSMA  f3,  A2, f11, f3

	fxpmul	  f4,  A1, f0
	fxpmul	  f5,  A1, f1
	fxpmul	  f6,  A1, f2
	fxpmul	  f7,  A1, f3

	FXCXNPMA  f0,  A1, f0,  f4
	FXCXNPMA  f1,  A1, f1,  f5
	FXCXNPMA  f2,  A1, f2,  f6
	FXCXNPMA  f3,  A1, f3,  f7
#endif

#ifdef LN
	subi	CO1, CO1, 8 * SIZE
	subi	CO2, CO2, 8 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFPDUX	f0,  BO,  INC4
	STFPDUX	f8,  BO2, INC4
	STFPDUX	f1,  BO,  INC4
	STFPDUX	f9,  BO2, INC4
	STFPDUX	f2,  BO,  INC4
	STFPDUX	f10, BO2, INC4
	STFPDUX	f3,  BO,  INC4
	STFPDUX	f11, BO2, INC4

	subi	BO,  BO,  16 * SIZE
	subi	BO2, BO2, 16 * SIZE
#else
	STFPDUX	f0,  AO,  INC4
	STFPDUX	f1,  AO2, INC4
	STFPDUX	f2,  AO,  INC4
	STFPDUX	f3,  AO2, INC4
	STFPDUX	f8,  AO,  INC4
	STFPDUX	f9,  AO2, INC4
	STFPDUX	f10, AO,  INC4
	STFPDUX	f11, AO2, INC4

	subi	AO,  AO,  16 * SIZE
	subi	AO2, AO2, 16 * SIZE
#endif

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC
	STFDUX	f2,  CO1, INC
	STFSDUX	f2,  CO1, INC
	STFDUX	f3,  CO1, INC
	STFSDUX	f3,  CO1, INC

	STFDUX	f8,  CO2, INC
	STFSDUX	f8,  CO2, INC
	STFDUX	f9,  CO2, INC
	STFSDUX	f9,  CO2, INC
	STFDUX	f10, CO2, INC
	STFSDUX	f10, CO2, INC
	STFDUX	f11, CO2, INC
	STFSDUX	f11, CO2, INC

#ifdef LN
	subi	CO1, CO1, 8 * SIZE
	subi	CO2, CO2, 8 * SIZE
#endif

#ifdef RT
	slwi	r0, K, 2 + ZBASE_SHIFT
	add	AORIG, AORIG, r0
#endif

#if defined(LT) || defined(RN)
	sub	TEMP, K, KK
	slwi	r0,   TEMP, 2 + ZBASE_SHIFT
	slwi	TEMP, TEMP, 1 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LT
	addi	KK, KK, 4
#endif

#ifdef LN
	subi	KK, KK, 4
#endif

	addic.	I, I, -1
	li	r0, FZERO

	lfpsx	f0, SP, r0
	bgt+	.L11
	.align 4

.L49:
#ifdef LN
	slwi	r0, K, 1 + ZBASE_SHIFT
	add	B, B, r0
#endif

#if defined(LT) || defined(RN)
	addi	B,  BO, 4 * SIZE
#endif

#ifdef RN
	addi	KK, KK, 2
#endif

#ifdef RT
	subi	KK, KK, 2
#endif

	addic.	J, J, -1
	bgt+	.L10
	.align 4

.L50:
	andi.	J, N,  1
	beq	.L999

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

	andi.	I, M,  1
	beq	.L60

#if defined(LT) || defined(RN)
	addi	BO,  B,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0
	srawi.	r0, KK,  3
	mtspr	CTR, r0
	ble	.L74
#else
#ifdef LN
	slwi	r0,   K,  0 + ZBASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	TEMP, KK, 0 + ZBASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO, B,     TEMP

	sub	TEMP, K, KK

	addi	BO,  BO,  - 2 * SIZE
	fpmr	f1,  f0
	fpmr	f2,  f0
	fpmr	f3,  f0
	srawi.	r0, TEMP,  3
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
	fpadd	f0, f0, f2
	fpadd	f1, f1, f3

	fpadd	f0, f0, f1

#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 1
#else
	subi	r0, KK, 1
#endif
	slwi	TEMP, r0, 0 + ZBASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO, B,     TEMP
	addi	BO,  BO, - 2 * SIZE
#endif

#if defined(LN) || defined(LT)
	LFPDX	f16, BO,  INC2
#else
	LFPDX	f16, AO,  INC2
#endif

	fpsub	f0,  f16,  f0

#ifdef LN
	LFPDX	A1,  AO,  INC2

	fxpmul	  f4,  A1, f0
	FXCXNPMA  f0,  A1, f0,  f4
#endif

#ifdef LT
	LFPDX	A1,  AO,  INC2

	fxpmul	  f4,  A1, f0
	FXCXNPMA  f0,  A1, f0, f4
#endif

#ifdef RN
	LFPDX	A1,  BO,  INC2

	fxpmul	  f4,  A1, f0
	FXCXNPMA  f0,  A1, f0, f4
#endif

#ifdef RT
	LFPDX	A1,  BO,  INC2

	fxpmul	  f4,  A1, f0
	FXCXNPMA  f0,  A1, f0,  f4
#endif

#ifdef LN
	subi	CO1, CO1, 2 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFPDX	f0,  BO,  INC2
#else
	STFPDX	f0,  AO,  INC2
#endif

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC

#ifdef LN
	subi	CO1, CO1, 2 * SIZE
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

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L60:
	andi.	I, M,  2
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
	slwi	r0,   K,  1 + ZBASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0  , KK, 1 + ZBASE_SHIFT
	slwi	TEMP, KK, 0 + ZBASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

 	fpmr	f1,  f0
	addi	BO,  BO,  - 2 * SIZE
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
	fpadd	f0, f0, f2
	fpadd	f1, f1, f3

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
	addi	BO,  BO, - 2 * SIZE
#endif

#if defined(LN) || defined(LT)
	LFPDUX	f16, BO,  INC2
	LFPDUX	f17, BO,  INC2

	subi	BO,  BO,   4 * SIZE
#else
	LFPDUX	f16, AO,  INC2
	LFPDUX	f17, AO,  INC2

	subi	AO,  AO,   4 * SIZE
#endif

	fpsub	f0,  f16,  f0
	fpsub	f1,  f17,  f1

#ifdef LN
	LFPDUX	A1,  AO,  INC2
	add	AO,  AO,  INC2
	LFPDUX	A2,  AO,  INC2
	LFPDUX	A3,  AO,  INC2

	subi	AO,  AO,   8 * SIZE

	fxpmul	  f4,  A3, f1
	FXCXNPMA  f1,  A3, f1,  f4

	fxcpnmsub f0,  A2, f1,  f0
	FXCXNSMA  f0,  A2, f1,  f0

	fxpmul	  f4,  A1, f0
	FXCXNPMA  f0,  A1, f0,  f4
#endif

#ifdef LT
	LFPDUX	A1,  AO,  INC2
	LFPDUX	A2,  AO,  INC2
	add	AO,  AO,  INC2
	LFPDUX	A3,  AO,  INC2

	subi	AO,  AO,   8 * SIZE

	fxpmul	  f4,  A1, f0
	FXCXNPMA  f0,  A1, f0, f4

	fxcpnmsub f1,  A2, f0, f1
	FXCXNSMA  f1,  A2, f0, f1

	fxpmul	  f6,  A3, f1
	FXCXNPMA  f1,  A3, f1, f6
#endif

#ifdef RN
	LFPDX	A1,  BO,  INC2

	fxpmul	  f4,  A1, f0
	fxpmul	  f5,  A1, f1

	FXCXNPMA  f0,  A1, f0, f4
	FXCXNPMA  f1,  A1, f1, f5
#endif

#ifdef RT
	LFPDX	A1,  BO,  INC2

	fxpmul	  f4,  A1, f0
	fxpmul	  f5,  A1, f1

	FXCXNPMA  f0,  A1, f0,  f4
	FXCXNPMA  f1,  A1, f1,  f5
#endif

#ifdef LN
	subi	CO1, CO1, 4 * SIZE
#endif

#if defined(LN) || defined(LT)
	STFPDUX	f0,  BO,  INC2
	STFPDUX	f1,  BO,  INC2

	subi	BO,  BO,   4 * SIZE
#else
	STFPDUX	f0,  AO,  INC2
	STFPDUX	f1,  AO,  INC2

	subi	AO,  AO,   4 * SIZE
#endif

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC

#ifdef LN
	subi	CO1, CO1, 4 * SIZE
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

	li	r0, FZERO
	lfpsx	f0, SP, r0
	.align 4

.L70:
	srawi.	I, M,  2
	ble	.L89
	.align 4

.L51:
#if defined(LT) || defined(RN)
	fpmr	f4,  f0
	addi	BO,  B,  - 2 * SIZE
 	fpmr	f1,  f0
	fpmr	f5,  f0
	fpmr	f2,  f0
	fpmr	f6,  f0
	fpmr	f3,  f0
	fpmr	f7,  f0
	srawi.	r0,  KK,  2
	mtspr	CTR, r0
	ble	.L54
#else

#ifdef LN
	slwi	r0,   K,  2 + ZBASE_SHIFT
	sub	AORIG, AORIG, r0
#endif

	slwi	r0  , KK, 2 + ZBASE_SHIFT
	slwi	TEMP, KK, 0 + ZBASE_SHIFT
	add	AO, AORIG, r0
	add	BO, B,     TEMP

	sub	TEMP, K, KK

	fpmr	f4,  f0
	addi	BO,  BO,  - 2 * SIZE
 	fpmr	f1,  f0
	fpmr	f5,  f0
	fpmr	f2,  f0
	fpmr	f6,  f0
	fpmr	f3,  f0
	fpmr	f7,  f0
	srawi.	r0, TEMP,  2
	mtspr	CTR, r0
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
	fpadd	f0,  f0,  f4
	fpadd	f1,  f1,  f5
	fpadd	f2,  f2,  f6
	fpadd	f3,  f3,  f7

#if defined(LN) || defined(RT)
#ifdef LN
	subi	r0, KK, 4
#else
	subi	r0, KK, 1
#endif
	slwi	TEMP, r0, 2 + ZBASE_SHIFT
	slwi	r0,   r0, 0 + ZBASE_SHIFT
	add	AO, AORIG, TEMP
	add	BO, B,     r0
	addi	BO,  BO, - 2 * SIZE
#endif

#if defined(LN) || defined(LT)
	LFPDUX	f16, BO,  INC2
	LFPDUX	f17, BO,  INC2
	LFPDUX	f18, BO,  INC2
	LFPDUX	f19, BO,  INC2

	subi	BO,  BO,   8 * SIZE
#else
	LFPDUX	f16, AO,  INC2
	LFPDUX	f17, AO,  INC2
	LFPDUX	f18, AO,  INC2
	LFPDUX	f19, AO,  INC2

	subi	AO,  AO,   8 * SIZE
#endif

	fpsub	f0,  f16,  f0
	fpsub	f1,  f17,  f1
	fpsub	f2,  f18,  f2
	fpsub	f3,  f19,  f3

#ifdef LN
	LFPDUX	A1,  AO,  INC2
	add	AO,  AO,  INC2
	add	AO,  AO,  INC2
	add	AO,  AO,  INC2

	LFPDUX	A2,  AO,  INC2
	LFPDUX	A3,  AO,  INC2
	add	AO,  AO,  INC2
	add	AO,  AO,  INC2

	LFPDUX	A4,  AO,  INC2
	LFPDUX	A5,  AO,  INC2
	LFPDUX	A6,  AO,  INC2
	add	AO,  AO,  INC2

	LFPDUX	A7,  AO,  INC2
	LFPDUX	A8,  AO,  INC2
	LFPDUX	A9,  AO,  INC2
	LFPDUX	A10, AO,  INC2

	subi	AO,  AO,  32 * SIZE

	fxpmul	  f4,  A10, f3
	FXCXNPMA  f3,  A10, f3,  f4

	fxcpnmsub f2,  A9, f3,  f2
	FXCXNSMA  f2,  A9, f3,  f2

	fxcpnmsub f1,  A8, f3,  f1
	FXCXNSMA  f1,  A8, f3,  f1

	fxcpnmsub f0,  A7, f3,  f0
	FXCXNSMA  f0,  A7, f3,  f0

	fxpmul	  f4,  A6, f2
	FXCXNPMA  f2,  A6, f2,  f4

	fxcpnmsub f1,  A5, f2,  f1
	FXCXNSMA  f1,  A5, f2,  f1

	fxcpnmsub f0,  A4, f2,  f0
	FXCXNSMA  f0,  A4, f2,  f0

	fxpmul	  f4,  A3, f1
	FXCXNPMA  f1,  A3, f1,  f4

	fxcpnmsub f0,  A2, f1,  f0
	FXCXNSMA  f0,  A2, f1,  f0

	fxpmul	  f4,  A1, f0
	FXCXNPMA  f0,  A1, f0,  f4
#endif

#ifdef LT
	LFPDUX	A1,  AO,  INC2
	LFPDUX	A2,  AO,  INC2
	LFPDUX	A3,  AO,  INC2
	LFPDUX	A4,  AO,  INC2

	add	AO,  AO,  INC2
	LFPDUX	A5,  AO,  INC2
	LFPDUX	A6,  AO,  INC2
	LFPDUX	A7,  AO,  INC2

	add	AO,  AO,  INC2
	add	AO,  AO,  INC2
	LFPDUX	A8,  AO,  INC2
	LFPDUX	A9,  AO,  INC2

	add	AO,  AO,  INC2
	add	AO,  AO,  INC2
	add	AO,  AO,  INC2
	LFPDUX	A10, AO,  INC2

	subi	AO,  AO,  32 * SIZE

	fxpmul	  f4,  A1, f0
	FXCXNPMA  f0,  A1, f0, f4

	fxcpnmsub f1,  A2, f0, f1
	FXCXNSMA  f1,  A2, f0, f1

	fxcpnmsub f2,  A3, f0, f2
	FXCXNSMA  f2,  A3, f0, f2

	fxcpnmsub f3,  A4, f0, f3
	FXCXNSMA  f3,  A4, f0, f3

	fxpmul	  f6,  A5, f1
	FXCXNPMA  f1,  A5, f1, f6

	fxcpnmsub f2,  A6, f1, f2
	FXCXNSMA  f2,  A6, f1, f2

	fxcpnmsub f3,  A7, f1, f3
	FXCXNSMA  f3,  A7, f1, f3

	fxpmul	  f4,  A8, f2
	FXCXNPMA  f2,  A8, f2,  f4

	fxcpnmsub f3,  A9, f2,  f3
	FXCXNSMA  f3,  A9, f2,  f3

	fxpmul	  f6,  A10, f3
	FXCXNPMA  f3,  A10, f3,  f6
#endif

#ifdef RN
	LFPDX	A1,  BO,  INC2

	fxpmul	  f4,  A1, f0
	fxpmul	  f5,  A1, f1
	fxpmul	  f6,  A1, f2
	fxpmul	  f7,  A1, f3

	FXCXNPMA  f0,  A1, f0, f4
	FXCXNPMA  f1,  A1, f1, f5
	FXCXNPMA  f2,  A1, f2, f6
	FXCXNPMA  f3,  A1, f3, f7
#endif

#ifdef RT
	LFPDX	A1,  BO,  INC2

	fxpmul	  f4,  A1, f0
	fxpmul	  f5,  A1, f1
	fxpmul	  f6,  A1, f2
	fxpmul	  f7,  A1, f3

	FXCXNPMA  f0,  A1, f0,  f4
	FXCXNPMA  f1,  A1, f1,  f5
	FXCXNPMA  f2,  A1, f2,  f6
	FXCXNPMA  f3,  A1, f3,  f7
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
#else
	STFPDUX	f0,  AO,  INC2
	STFPDUX	f1,  AO,  INC2
	STFPDUX	f2,  AO,  INC2
	STFPDUX	f3,  AO,  INC2

	subi	AO,  AO,   8 * SIZE
#endif

	STFDUX	f0,  CO1, INC
	STFSDUX	f0,  CO1, INC
	STFDUX	f1,  CO1, INC
	STFSDUX	f1,  CO1, INC
	STFDUX	f2,  CO1, INC
	STFSDUX	f2,  CO1, INC
	STFDUX	f3,  CO1, INC
	STFSDUX	f3,  CO1, INC

#ifdef LN
	subi	CO1, CO1, 8 * SIZE
#endif

#ifdef RT
	slwi	r0, K, 2 + ZBASE_SHIFT
	add	AORIG, AORIG, r0
#endif

#if defined(LT) || defined(RN)
	sub	TEMP, K, KK
	slwi	r0,   TEMP, 2 + ZBASE_SHIFT
	slwi	TEMP, TEMP, 0 + ZBASE_SHIFT
	add	AO, AO, r0
	add	BO, BO, TEMP
#endif

#ifdef LT
	addi	KK, KK, 4
#endif

#ifdef LN
	subi	KK, KK, 4
#endif

	addic.	I, I, -1
	li	r0, FZERO

	lfpsx	f0, SP, r0
	bgt+	.L51
	.align 4

.L89:
#ifdef LN
	slwi	r0, K, 0 + ZBASE_SHIFT
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
