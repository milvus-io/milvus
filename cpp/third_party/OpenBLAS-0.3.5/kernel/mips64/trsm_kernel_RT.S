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

#define M	$4
#define	N	$5
#define	K	$6
#define A	$8
#define B	$9
#define C	$10
#define LDC	$11

#define AO	$12
#define BO	$13

#define I	$2
#define J	$3
#define L	$7

#define CO1	$14
#define CO2	$15
#define CO3	$16
#define CO4	$17
#define CO5	$18
#define CO6	$19
#define CO7	$20
#define CO8	$21

#define OFFSET	$22
#define KK	$23
#define TEMP	$24
#define AORIG	$25

#define a1	$f0
#define a2	$f1
#define a3	$f27
#define a4	$f28

#define b1	$f2
#define b2	$f3
#define b3	$f4
#define b4	$f5
#define b5	$f6
#define b6	$f7
#define b7	$f8
#define b8	$f9

#define a5	b8

#define c11	$f10
#define c12	$f11
#define c21	$f12
#define c22	$f13
#define c31	$f14
#define c32	$f16
#define c41	$f17
#define c42	$f18
#define c51	$f19
#define c52	$f20
#define c61	$f21
#define c62	$f22
#define c71	$f23
#define c72	$f24
#define c81	$f25
#define c82	$f26

#define ALPHA	$f15

	PROLOGUE

	daddiu	$sp, $sp, -144

	SDARG	$16,   0($sp)
	SDARG	$17,   8($sp)
	SDARG	$18,  16($sp)
	SDARG	$19,  24($sp)
	SDARG	$20,  32($sp)
	SDARG	$21,  40($sp)
	sdc1	$f24, 48($sp)
	sdc1	$f25, 56($sp)
	sdc1	$f26, 64($sp)
	sdc1	$f27, 72($sp)
	sdc1	$f28, 80($sp)

	SDARG	$22,  88($sp)
	SDARG	$23,  96($sp)
	SDARG	$24, 104($sp)
	SDARG	$25, 112($sp)

#ifndef __64BIT__
	sdc1	$f20,112($sp)
	sdc1	$f21,120($sp)
	sdc1	$f22,128($sp)
	sdc1	$f23,136($sp)
#endif

	LDARG	OFFSET, 144($sp)

	dsll	LDC, LDC, BASE_SHIFT

#ifdef LN
	mult	M, K
	mflo	TEMP

	dsll	TEMP, TEMP, BASE_SHIFT
	daddu	A, A, TEMP

	dsll	TEMP, M, BASE_SHIFT
	daddu	C, C, TEMP
#endif

#ifdef RN
	neg	KK, OFFSET
#endif

#ifdef RT
	mult	N, K
	mflo	TEMP

	dsll	TEMP, TEMP, BASE_SHIFT
	daddu	B, B, TEMP

	mult	N, LDC
	mflo	TEMP
	daddu	C, C, TEMP

	dsubu	KK, N, OFFSET
#endif

	andi	J,  N, 1
	blez	J, .L30
	NOP

#ifdef RT
	dsll	TEMP, K, BASE_SHIFT
	dsubu	B, B, TEMP

	dsubu	C, C, LDC
#endif

	move	AO, A
	move	CO1, C

#ifdef LN
	daddu	KK, M, OFFSET
#endif

#ifdef LT
	move	KK, OFFSET
#endif

#if defined(LN) || defined(RT)
	move	AORIG, A
#else
	move	AO, A
#endif
#ifndef RT
	daddu	C,  CO1, LDC
#endif

	dsra	I,  M, 1
	blez	I, .L80
	NOP

.L71:
#if defined(LT) || defined(RN)
	LD	a1,  0 * SIZE(AO)
	MTC	$0,  c11
	LD	a2,  1 * SIZE(AO)
	MOV	c21, c11
	LD	a5,  4 * SIZE(AO)

	LD	b1,  0 * SIZE(B)
	MOV	c12, c11
	LD	b2,  1 * SIZE(B)
	MOV	c22, c11
	LD	b3,  2 * SIZE(B)
	LD	b5,  4 * SIZE(B)
	dsra	L,  KK, 2
	LD	b6,  8 * SIZE(B)
	LD	b7, 12 * SIZE(B)

	blez	L, .L75
	move	BO,  B
#else
#ifdef LN
	dsll	TEMP,   K,  1 + BASE_SHIFT
	dsubu	AORIG, AORIG, TEMP
#endif

	dsll	L,    KK, 1 + BASE_SHIFT
	dsll	TEMP, KK, 0 + BASE_SHIFT

	daddu	AO, AORIG, L
	daddu	BO, B,     TEMP

	dsubu	TEMP, K, KK

	LD	a1,  0 * SIZE(AO)
	MTC	$0,  c11
	LD	a2,  1 * SIZE(AO)
	MOV	c21, c11
	LD	a5,  4 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	MOV	c12, c11
	LD	b2,  1 * SIZE(BO)
	MOV	c22, c11
	LD	b3,  2 * SIZE(BO)
	LD	b5,  4 * SIZE(BO)
	dsra	L,  TEMP, 2
	LD	b6,  8 * SIZE(BO)
	LD	b7, 12 * SIZE(BO)

	blez	L, .L75
	NOP
#endif
	.align	3

.L72:
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)

	MADD	c11, c11, a1, b1
	MADD	c12, c12, a2, b1

	LD	a1,  2 * SIZE(AO)
	LD	a2,  3 * SIZE(AO)
	LD	b1,  1 * SIZE(BO)

	MADD	c11, c11, a1, b1
	MADD	c12, c12, a2, b1

	LD	a1,  4 * SIZE(AO)
	LD	a2,  5 * SIZE(AO)
	LD	b1,  2 * SIZE(BO)

	MADD	c11, c11, a1, b1
	MADD	c12, c12, a2, b1

	LD	a1,  6 * SIZE(AO)
	LD	a2,  7 * SIZE(AO)
	LD	b1,  3 * SIZE(BO)

	MADD	c11, c11, a1, b1
	MADD	c12, c12, a2, b1

	daddiu	L, L, -1
	daddiu	AO, AO,  8 * SIZE
	bgtz	L, .L72
	daddiu	BO, BO,  4 * SIZE
	.align 3

.L75:
#if defined(LT) || defined(RN)
	andi	L, KK,  3
#else
	andi	L, TEMP, 3
#endif
	NOP
	blez	L, .L78
	NOP
	.align	3

.L76:
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)

	MADD	c11, c11, a1, b1
	MADD	c12, c12, a2, b1

	daddiu	L, L, -1
	daddiu	AO, AO,  2 * SIZE
	bgtz	L, .L76
	daddiu	BO, BO,  1 * SIZE

.L78:
	ADD	c11, c11, c21
	ADD	c12, c12, c22

#if defined(LN) || defined(RT)
#ifdef LN
	daddiu	TEMP, KK, -2
#else
	daddiu	TEMP, KK, -1
#endif

	dsll	L,    TEMP, 1 + BASE_SHIFT
	dsll	TEMP, TEMP, 0 + BASE_SHIFT
	daddu	AO, AORIG, L
	daddu	BO, B,     TEMP
#endif


#if defined(LN) || defined(LT)
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	SUB	c11, b1, c11
	SUB	c12, b2, c12
#else
	LD	b1,  0 * SIZE(AO)
	LD	b2,  1 * SIZE(AO)

	SUB	c11, b1, c11
	SUB	c12, b2, c12
#endif

#ifdef LN
	LD	b1,  3 * SIZE(AO)
	LD	b2,  2 * SIZE(AO)
	LD	b3,  0 * SIZE(AO)

	MUL	c12, b1, c12
	NMSUB	c11, c11, b2, c12
	MUL	c11, b3, c11
#endif

#ifdef LT
	LD	b1,  0 * SIZE(AO)
	LD	b2,  1 * SIZE(AO)
	LD	b3,  3 * SIZE(AO)

	MUL	c11, b1, c11
	NMSUB	c12, c12, b2, c11
	MUL	c12, b3, c12
#endif

#if defined(RN) || defined(RT)
	LD	b1,  0 * SIZE(BO)

	MUL	c11, b1, c11
	MUL	c12, b1, c12
#endif

#ifdef LN
	daddiu	CO1, CO1, -2 * SIZE
#endif

#if defined(LN) || defined(LT)
	ST	c11,  0 * SIZE(BO)
	ST	c12,  1 * SIZE(BO)
#else
	ST	c11,  0 * SIZE(AO)
	ST	c12,  1 * SIZE(AO)
#endif

	ST	c11,  0 * SIZE(CO1)
	ST	c12,  1 * SIZE(CO1)

#ifndef LN
	daddiu	CO1, CO1, 2 * SIZE
#endif

#ifdef RT
	dsll	TEMP, K, 1 + BASE_SHIFT
	daddu	AORIG, AORIG, TEMP
#endif

#if defined(LT) || defined(RN)
	dsubu	TEMP, K, KK
	dsll	L,    TEMP, 1 + BASE_SHIFT
	dsll	TEMP, TEMP, 0 + BASE_SHIFT
	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LT
	daddiu	KK, KK, 2
#endif

#ifdef LN
	daddiu	KK, KK, -2
#endif

	daddiu	I, I, -1

	bgtz	I, .L71
	NOP
	.align 3

.L80:
	andi	I,  M, 1
	blez	I, .L89
	NOP

#if defined(LT) || defined(RN)
	LD	a1,  0 * SIZE(AO)
	MTC	$0,  c11
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(B)
	LD	b2,  1 * SIZE(B)
	MOV	c21, c11
	LD	b3,  2 * SIZE(B)
	LD	b4,  3 * SIZE(B)
	LD	b5,  4 * SIZE(B)
	LD	b6,  8 * SIZE(B)
	LD	b7, 12 * SIZE(B)

	dsra	L,  KK, 2
	blez	L, .L85
	move	BO,  B
#else
#ifdef LN
	dsll	TEMP,   K,  BASE_SHIFT
	dsubu	AORIG, AORIG, TEMP
#endif

	dsll	TEMP, KK, BASE_SHIFT

	daddu	AO, AORIG, TEMP
	daddu	BO, B,     TEMP

	dsubu	TEMP, K, KK

	LD	a1,  0 * SIZE(AO)
	MTC	$0,  c11
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)
	MOV	c21, c11
	LD	b5,  4 * SIZE(BO)
	LD	b6,  8 * SIZE(BO)
	LD	b7, 12 * SIZE(BO)

	dsra	L,  TEMP, 2
	blez	L, .L85
	NOP
#endif
	.align	3

.L82:
	LD	a1,  0 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)

	MADD	c11, c11, a1, b1

	LD	a1,  1 * SIZE(AO)
	LD	b1,  1 * SIZE(BO)

	MADD	c21, c21, a1, b1

	LD	a1,  2 * SIZE(AO)
	LD	b1,  2 * SIZE(BO)

	MADD	c11, c11, a1, b1

	LD	a1,  3 * SIZE(AO)
	LD	b1,  3 * SIZE(BO)

	MADD	c21, c21, a1, b1

	daddiu	L, L, -1
	daddiu	AO, AO,  4 * SIZE
	bgtz	L, .L82
	daddiu	BO, BO,  4 * SIZE
	.align 3

.L85:
#if defined(LT) || defined(RN)
	andi	L, KK,  3
#else
	andi	L, TEMP, 3
#endif
	NOP
	blez	L, .L88
	NOP
	.align	3

.L86:
	LD	a1,  0 * SIZE(AO)
	LD	b1,  0 * SIZE(BO)

	MADD	c11, c11, a1, b1

	daddiu	L, L, -1
	daddiu	AO, AO,  1 * SIZE
	bgtz	L, .L86
	daddiu	BO, BO,  1 * SIZE


.L88:
	ADD	c11, c11, c21

#if defined(LN) || defined(RT)
#ifdef LN
	daddiu	TEMP, KK, -1
#else
	daddiu	TEMP, KK, -1
#endif

	dsll	TEMP, TEMP, 0 + BASE_SHIFT
	daddu	AO, AORIG, TEMP
	daddu	BO, B,     TEMP
#endif


#if defined(LN) || defined(LT)
	LD	b1,  0 * SIZE(BO)

	SUB	c11, b1, c11
#else
	LD	b1,  0 * SIZE(AO)

	SUB	c11, b1, c11
#endif

#if defined(LN) || defined(LT)
	LD	b1,  0 * SIZE(AO)

	MUL	c11, b1, c11
#endif

#if defined(RN) || defined(RT)
	LD	b1,  0 * SIZE(BO)

	MUL	c11, b1, c11
#endif

#ifdef LN
	daddiu	CO1, CO1, -1 * SIZE
#endif

#if defined(LN) || defined(LT)
	ST	c11,  0 * SIZE(BO)
#else
	ST	c11,  0 * SIZE(AO)
#endif

	ST	c11,  0 * SIZE(CO1)

#ifndef LN
	daddiu	CO1, CO1, 1 * SIZE
#endif

#ifdef RT
	dsll	TEMP, K, BASE_SHIFT
	daddu	AORIG, AORIG, TEMP
#endif

#if defined(LT) || defined(RN)
	dsubu	TEMP, K, KK
	dsll	TEMP, TEMP, 0 + BASE_SHIFT
	daddu	AO, AO, TEMP
	daddu	BO, BO, TEMP
#endif

#ifdef LT
	daddiu	KK, KK, 1
#endif

#ifdef LN
	daddiu	KK, KK, -1
#endif
	.align 3

.L89:
#ifdef LN
	dsll	TEMP, K, BASE_SHIFT
	daddu	B, B, TEMP
#endif

#if defined(LT) || defined(RN)
	move	B,  BO
#endif

#ifdef RN
	daddiu	KK, KK,  1
#endif

#ifdef RT
	daddiu	KK, KK, -1
#endif
	.align 3

.L30:
	andi	J,  N, 2
	blez	J, .L50
	NOP

#ifdef RT
	dsll	TEMP, K, 1 + BASE_SHIFT
	dsubu	B, B, TEMP

	dsll	TEMP, LDC, 1
	dsubu	C, C, TEMP
#endif

	move	AO, A
	move	CO1, C
	daddu	CO2, C,   LDC

#ifdef LN
	daddu	KK, M, OFFSET
#endif

#ifdef LT
	move	KK, OFFSET
#endif

#if defined(LN) || defined(RT)
	move	AORIG, A
#else
	move	AO, A
#endif
#ifndef RT
	daddu	C,  CO2, LDC
#endif

	dsra	I,  M, 1
	blez	I, .L60
	NOP

.L51:
#if defined(LT) || defined(RN)
	LD	a1,  0 * SIZE(AO)
	MTC	$0,  c11
	LD	a2,  1 * SIZE(AO)
	MOV	c21, c11
	LD	a5,  4 * SIZE(AO)

	LD	b1,  0 * SIZE(B)
	MOV	c12, c11
	LD	b2,  1 * SIZE(B)
	MOV	c22, c11
	LD	b3,  2 * SIZE(B)
	LD	b5,  4 * SIZE(B)
	dsra	L,  KK, 2
	LD	b6,  8 * SIZE(B)
	LD	b7, 12 * SIZE(B)

	blez	L, .L55
	move	BO,  B

#else
#ifdef LN
	dsll	TEMP,   K,  1 + BASE_SHIFT
	dsubu	AORIG, AORIG, TEMP
#endif

	dsll	L,    KK, 1 + BASE_SHIFT
	dsll	TEMP, KK, 1 + BASE_SHIFT

	daddu	AO, AORIG, L
	daddu	BO, B,     TEMP

	dsubu	TEMP, K, KK

	LD	a1,  0 * SIZE(AO)
	MTC	$0,  c11
	LD	a2,  1 * SIZE(AO)
	MOV	c21, c11
	LD	a5,  4 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	MOV	c12, c11
	LD	b2,  1 * SIZE(BO)
	MOV	c22, c11
	LD	b3,  2 * SIZE(BO)
	LD	b5,  4 * SIZE(BO)
	dsra	L,  TEMP, 2
	LD	b6,  8 * SIZE(BO)
	LD	b7, 12 * SIZE(BO)

	blez	L, .L55
	NOP
#endif
	.align	3

.L52:
	MADD	c11, c11, a1, b1
	LD	a3,  2 * SIZE(AO)
	MADD	c21, c21, a1, b2
	LD	b4,  3 * SIZE(BO)
	MADD	c12, c12, a2, b1
	LD	a4,  3 * SIZE(AO)
	MADD	c22, c22, a2, b2
	LD	b1,  8 * SIZE(BO)

	MADD	c11, c11, a3, b3
	LD	a1,  8 * SIZE(AO)
	MADD	c21, c21, a3, b4
	LD	b2,  5 * SIZE(BO)
	MADD	c12, c12, a4, b3
	LD	a2,  5 * SIZE(AO)
	MADD	c22, c22, a4, b4
	LD	b3,  6 * SIZE(BO)

	MADD	c11, c11, a5, b5
	LD	a3,  6 * SIZE(AO)
	MADD	c21, c21, a5, b2
	LD	b4,  7 * SIZE(BO)
	MADD	c12, c12, a2, b5
	LD	a4,  7 * SIZE(AO)
	MADD	c22, c22, a2, b2
	LD	b5, 12 * SIZE(BO)

	MADD	c11, c11, a3, b3
	LD	a5, 12 * SIZE(AO)
	MADD	c21, c21, a3, b4
	LD	b2,  9 * SIZE(BO)
	MADD	c12, c12, a4, b3
	LD	a2,  9 * SIZE(AO)
	MADD	c22, c22, a4, b4
	LD	b3, 10 * SIZE(BO)

	daddiu	AO, AO,  8 * SIZE
	daddiu	L, L, -1
	bgtz	L, .L52
	daddiu	BO, BO,  8 * SIZE
	.align 3

.L55:
#if defined(LT) || defined(RN)
	andi	L, KK,  3
#else
	andi	L, TEMP, 3
#endif
	NOP
	blez	L, .L58
	NOP
	.align	3

.L56:
	MADD	c11, c11, a1, b1
	LD	a2,  1 * SIZE(AO)
	MADD	c21, c21, a1, b2
	LD	a1,  2 * SIZE(AO)

	MADD	c12, c12, a2, b1
	LD	b1,  2 * SIZE(BO)
	MADD	c22, c22, a2, b2
	LD	b2,  3 * SIZE(BO)

	daddiu	L, L, -1
	daddiu	AO, AO,  2 * SIZE
	bgtz	L, .L56
	daddiu	BO, BO,  2 * SIZE

.L58:
#if defined(LN) || defined(RT)
#ifdef LN
	daddiu	TEMP, KK, -2
#else
	daddiu	TEMP, KK, -2
#endif

	dsll	L,    TEMP, 1 + BASE_SHIFT
	dsll	TEMP, TEMP, 1 + BASE_SHIFT
	daddu	AO, AORIG, L
	daddu	BO, B,     TEMP
#endif


#if defined(LN) || defined(LT)
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	SUB	c11, b1, c11
	SUB	c21, b2, c21
	SUB	c12, b3, c12
	SUB	c22, b4, c22
#else
	LD	b1,  0 * SIZE(AO)
	LD	b2,  1 * SIZE(AO)
	LD	b3,  2 * SIZE(AO)
	LD	b4,  3 * SIZE(AO)

	SUB	c11, b1, c11
	SUB	c12, b2, c12
	SUB	c21, b3, c21
	SUB	c22, b4, c22
#endif

#ifdef LN
	LD	b1,  3 * SIZE(AO)
	LD	b2,  2 * SIZE(AO)
	LD	b3,  0 * SIZE(AO)

	MUL	c12, b1, c12
	MUL	c22, b1, c22

	NMSUB	c11, c11, b2, c12
	NMSUB	c21, c21, b2, c22

	MUL	c11, b3, c11
	MUL	c21, b3, c21
#endif

#ifdef LT
	LD	b1,  0 * SIZE(AO)
	LD	b2,  1 * SIZE(AO)
	LD	b3,  3 * SIZE(AO)

	MUL	c11, b1, c11
	MUL	c21, b1, c21

	NMSUB	c12, c12, b2, c11
	NMSUB	c22, c22, b2, c21

	MUL	c12, b3, c12
	MUL	c22, b3, c22
#endif

#ifdef RN
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  3 * SIZE(BO)

	MUL	c11, b1, c11
	MUL	c12, b1, c12

	NMSUB	c21, c21, b2, c11
	NMSUB	c22, c22, b2, c12

	MUL	c21, b3, c21
	MUL	c22, b3, c22
#endif

#ifdef RT
	LD	b1,  3 * SIZE(BO)
	LD	b2,  2 * SIZE(BO)
	LD	b3,  0 * SIZE(BO)

	MUL	c21, b1, c21
	MUL	c22, b1, c22

	NMSUB	c11, c11, b2, c21
	NMSUB	c12, c12, b2, c22

	MUL	c11, b3, c11
	MUL	c12, b3, c12
#endif

#ifdef LN
	daddiu	CO1, CO1, -2 * SIZE
	daddiu	CO2, CO2, -2 * SIZE
#endif

#if defined(LN) || defined(LT)
	ST	c11,  0 * SIZE(BO)
	ST	c21,  1 * SIZE(BO)
	ST	c12,  2 * SIZE(BO)
	ST	c22,  3 * SIZE(BO)
#else
	ST	c11,  0 * SIZE(AO)
	ST	c12,  1 * SIZE(AO)
	ST	c21,  2 * SIZE(AO)
	ST	c22,  3 * SIZE(AO)
#endif

	ST	c11,  0 * SIZE(CO1)
	ST	c12,  1 * SIZE(CO1)
	ST	c21,  0 * SIZE(CO2)
	ST	c22,  1 * SIZE(CO2)

#ifndef LN
	daddiu	CO1, CO1, 2 * SIZE
	daddiu	CO2, CO2, 2 * SIZE
#endif

#ifdef RT
	dsll	TEMP, K, 1 + BASE_SHIFT
	daddu	AORIG, AORIG, TEMP
#endif

#if defined(LT) || defined(RN)
	dsubu	TEMP, K, KK
	dsll	TEMP, TEMP, 1 + BASE_SHIFT
	daddu	AO, AO, TEMP
	daddu	BO, BO, TEMP
#endif

#ifdef LT
	daddiu	KK, KK, 2
#endif

#ifdef LN
	daddiu	KK, KK, -2
#endif

	MTC	$0,  a1

	MOV	c11, a1
	MOV	c21, a1
	MOV	c31, a1

	daddiu	I, I, -1

	bgtz	I, .L51
	MOV	c41, c11
	.align 3

.L60:
	andi	I,  M, 1
	blez	I, .L69
	NOP

#if defined(LT) || defined(RN)
	dsra	L,  KK, 2
	LD	a1,  0 * SIZE(AO)
	MTC	$0,  c11
	LD	a2,  1 * SIZE(AO)
	MOV	c21, c11
	LD	a3,  2 * SIZE(AO)
	MOV	c31, c11
	LD	a4,  3 * SIZE(AO)
	MOV	c41, c11

	LD	b1,  0 * SIZE(B)
	LD	b2,  1 * SIZE(B)
	LD	b3,  2 * SIZE(B)
	LD	b4,  3 * SIZE(B)
	LD	b5,  4 * SIZE(B)
	LD	b6,  8 * SIZE(B)
	LD	b7, 12 * SIZE(B)

	blez	L, .L65
	move	BO,  B
#else
#ifdef LN
	dsll	TEMP,   K,  BASE_SHIFT
	dsubu	AORIG, AORIG, TEMP
#endif

	dsll	L,    KK, 0 + BASE_SHIFT
	dsll	TEMP, KK, 1 + BASE_SHIFT

	daddu	AO, AORIG, L
	daddu	BO, B,     TEMP

	dsubu	TEMP, K, KK

	dsra	L,  TEMP, 2
	LD	a1,  0 * SIZE(AO)
	MTC	$0,  c11
	LD	a2,  1 * SIZE(AO)
	MOV	c21, c11
	LD	a3,  2 * SIZE(AO)
	MOV	c31, c11
	LD	a4,  3 * SIZE(AO)
	MOV	c41, c11

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)
	LD	b5,  4 * SIZE(BO)
	LD	b6,  8 * SIZE(BO)
	LD	b7, 12 * SIZE(BO)

	blez	L, .L65
	NOP
#endif
	.align	3

.L62:
	MADD	c11, c11, a1, b1
	LD	b1,  4 * SIZE(BO)
	MADD	c21, c21, a1, b2
	LD	b2,  5 * SIZE(BO)
	MADD	c31, c31, a2, b3
	LD	b3,  6 * SIZE(BO)
	MADD	c41, c41, a2, b4
	LD	b4,  7 * SIZE(BO)

	LD	a1,  4 * SIZE(AO)
	LD	a2,  5 * SIZE(AO)

	MADD	c11, c11, a3, b1
	LD	b1,  8 * SIZE(BO)
	MADD	c21, c21, a3, b2
	LD	b2,  9 * SIZE(BO)
	MADD	c31, c31, a4, b3
	LD	b3, 10 * SIZE(BO)
	MADD	c41, c41, a4, b4
	LD	b4, 11 * SIZE(BO)

	LD	a3,  6 * SIZE(AO)
	LD	a4,  7 * SIZE(AO)

	daddiu	L, L, -1
	daddiu	AO, AO,  4 * SIZE

	bgtz	L, .L62
	daddiu	BO, BO,  8 * SIZE
	.align 3

.L65:
#if defined(LT) || defined(RN)
	andi	L, KK,  3
#else
	andi	L, TEMP, 3
#endif
	NOP
	blez	L, .L68
	NOP
	.align	3

.L66:
	MADD	c11, c11, a1, b1
	LD	b1,  2 * SIZE(BO)
	MADD	c21, c21, a1, b2
	LD	b2,  3 * SIZE(BO)

	LD	a1,  1 * SIZE(AO)
	daddiu	L, L, -1

	daddiu	AO, AO,  1 * SIZE
	bgtz	L, .L66
	daddiu	BO, BO,  2 * SIZE


.L68:
	ADD	c11, c11, c31
	ADD	c21, c21, c41

#if defined(LN) || defined(RT)
#ifdef LN
	daddiu	TEMP, KK, -1
#else
	daddiu	TEMP, KK, -2
#endif

	dsll	L,    TEMP, 0 + BASE_SHIFT
	dsll	TEMP, TEMP, 1 + BASE_SHIFT
	daddu	AO, AORIG, L
	daddu	BO, B,     TEMP
#endif


#if defined(LN) || defined(LT)
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	SUB	c11, b1, c11
	SUB	c21, b2, c21
#else
	LD	b1,  0 * SIZE(AO)
	LD	b2,  1 * SIZE(AO)

	SUB	c11, b1, c11
	SUB	c21, b2, c21
#endif

#if defined(LN) || defined(LT)
	LD	b3,  0 * SIZE(AO)

	MUL	c11, b3, c11
	MUL	c21, b3, c21
#endif

#ifdef RN
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  3 * SIZE(BO)

	MUL	c11, b1, c11

	NMSUB	c21, c21, b2, c11

	MUL	c21, b3, c21
#endif

#ifdef RT
	LD	b1,  3 * SIZE(BO)
	LD	b2,  2 * SIZE(BO)
	LD	b3,  0 * SIZE(BO)

	MUL	c21, b1, c21

	NMSUB	c11, c11, b2, c21

	MUL	c11, b3, c11
#endif

#ifdef LN
	daddiu	CO1, CO1, -1 * SIZE
	daddiu	CO2, CO2, -1 * SIZE
#endif

#if defined(LN) || defined(LT)
	ST	c11,  0 * SIZE(BO)
	ST	c21,  1 * SIZE(BO)
#else
	ST	c11,  0 * SIZE(AO)
	ST	c21,  1 * SIZE(AO)
#endif

	ST	c11,  0 * SIZE(CO1)
	ST	c21,  0 * SIZE(CO2)

#ifndef LN
	daddiu	CO1, CO1, 1 * SIZE
	daddiu	CO2, CO2, 1 * SIZE
#endif

#ifdef RT
	dsll	TEMP, K, 0 + BASE_SHIFT
	daddu	AORIG, AORIG, TEMP
#endif

#if defined(LT) || defined(RN)
	dsubu	TEMP, K, KK
	dsll	L,    TEMP, 0 + BASE_SHIFT
	dsll	TEMP, TEMP, 1 + BASE_SHIFT
	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LT
	daddiu	KK, KK, 1
#endif

#ifdef LN
	daddiu	KK, KK, -1
#endif
	.align 3

.L69:
#ifdef LN
	dsll	TEMP, K, 1 + BASE_SHIFT
	daddu	B, B, TEMP
#endif

#if defined(LT) || defined(RN)
	move	B,  BO
#endif

#ifdef RN
	daddiu	KK, KK,  2
#endif

#ifdef RT
	daddiu	KK, KK, -2
#endif
	.align 3

.L50:
	andi	J,  N, 4
	blez	J, .L70
	move	AO, A

#ifdef RT
	dsll	TEMP, K, 2 + BASE_SHIFT
	dsubu	B, B, TEMP

	dsll	TEMP, LDC, 2
	dsubu	C, C, TEMP
#endif

	move	CO1, C
	MTC	$0,  c11
	daddu	CO2, C,   LDC
	daddu	CO3, CO2, LDC
	daddu	CO4, CO3, LDC
	MOV	c21, c11
	dsra	I,  M, 1
	MOV	c31, c11

#ifdef LN
	daddu	KK, M, OFFSET
#endif

#ifdef LT
	move	KK, OFFSET
#endif

#if defined(LN) || defined(RT)
	move	AORIG, A
#else
	move	AO, A
#endif
#ifndef RT
	daddu	C,  CO4, LDC
#endif

	blez	I, .L40
	MOV	c41, c11

.L31:
#if defined(LT) || defined(RN)
	LD	a1,  0 * SIZE(AO)
	LD	a3,  4 * SIZE(AO)

	LD	b1,  0 * SIZE(B)
	MOV	c12, c11
	LD	b2,  1 * SIZE(B)
	MOV	c22, c11
	LD	b3,  2 * SIZE(B)
	MOV	c32, c11
	LD	b4,  3 * SIZE(B)
	MOV	c42, c11

	LD	b5,  4 * SIZE(B)
	dsra	L,  KK, 2
	LD	b6,  8 * SIZE(B)
	LD	b7, 12 * SIZE(B)

	blez	L, .L35
	move	BO,  B
#else
#ifdef LN
	dsll	TEMP,   K,  1 + BASE_SHIFT
	dsubu	AORIG, AORIG, TEMP
#endif

	dsll	L,    KK, 1 + BASE_SHIFT
	dsll	TEMP, KK, 2 + BASE_SHIFT

	daddu	AO, AORIG, L
	daddu	BO, B,     TEMP

	dsubu	TEMP, K, KK

	LD	a1,  0 * SIZE(AO)
	LD	a3,  4 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	MOV	c12, c11
	LD	b2,  1 * SIZE(BO)
	MOV	c22, c11
	LD	b3,  2 * SIZE(BO)
	MOV	c32, c11
	LD	b4,  3 * SIZE(BO)
	MOV	c42, c11

	LD	b5,  4 * SIZE(BO)
	dsra	L,  TEMP, 2
	LD	b6,  8 * SIZE(BO)
	LD	b7, 12 * SIZE(BO)

	blez	L, .L35
	NOP
#endif
	.align	3

.L32:
	MADD	c11, c11, a1, b1
	LD	a2,  1 * SIZE(AO)
	MADD	c21, c21, a1, b2
	daddiu	L, L, -1
	MADD	c31, c31, a1, b3
	NOP
	MADD	c41, c41, a1, b4
	LD	a1,  2 * SIZE(AO)

	MADD	c12, c12, a2, b1
	LD	b1, 16 * SIZE(BO)
	MADD	c22, c22, a2, b2
	LD	b2,  5 * SIZE(BO)
	MADD	c32, c32, a2, b3
	LD	b3,  6 * SIZE(BO)
	MADD	c42, c42, a2, b4
	LD	b4,  7 * SIZE(BO)

	MADD	c11, c11, a1, b5
	LD	a2,  3 * SIZE(AO)
	MADD	c21, c21, a1, b2
	NOP
	MADD	c31, c31, a1, b3
	NOP
	MADD	c41, c41, a1, b4
	LD	a1,  8 * SIZE(AO)

	MADD	c12, c12, a2, b5
	LD	b5, 20 * SIZE(BO)
	MADD	c22, c22, a2, b2
	LD	b2,  9 * SIZE(BO)
	MADD	c32, c32, a2, b3
	LD	b3, 10 * SIZE(BO)
	MADD	c42, c42, a2, b4
	LD	b4, 11 * SIZE(BO)

	MADD	c11, c11, a3, b6
	LD	a2,  5 * SIZE(AO)
	MADD	c21, c21, a3, b2
	NOP
	MADD	c31, c31, a3, b3
	NOP
	MADD	c41, c41, a3, b4
	LD	a3,  6 * SIZE(AO)

	MADD	c12, c12, a2, b6
	LD	b6, 24 * SIZE(BO)
	MADD	c22, c22, a2, b2
	LD	b2, 13 * SIZE(BO)
	MADD	c32, c32, a2, b3
	LD	b3, 14 * SIZE(BO)
	MADD	c42, c42, a2, b4
	LD	b4, 15 * SIZE(BO)

	MADD	c11, c11, a3, b7
	LD	a2,  7 * SIZE(AO)
	MADD	c21, c21, a3, b2
	daddiu	AO, AO,  8 * SIZE
	MADD	c31, c31, a3, b3
	daddiu	BO, BO, 16 * SIZE
	MADD	c41, c41, a3, b4
	LD	a3,  4 * SIZE(AO)

	MADD	c12, c12, a2, b7
	LD	b7, 12 * SIZE(BO)
	MADD	c22, c22, a2, b2
	LD	b2,  1 * SIZE(BO)
	MADD	c32, c32, a2, b3
	LD	b3,  2 * SIZE(BO)
	MADD	c42, c42, a2, b4
	NOP

	bgtz	L, .L32
	LD	b4,  3 * SIZE(BO)
	.align 3

.L35:
#if defined(LT) || defined(RN)
	andi	L, KK,  3
#else
	andi	L, TEMP, 3
#endif
	NOP
	blez	L, .L38
	NOP
	.align	3

.L36:
	MADD	c11, c11, a1, b1
	LD	a2,  1 * SIZE(AO)
	MADD	c21, c21, a1, b2
	daddiu	L, L, -1
	MADD	c31, c31, a1, b3
	daddiu	AO, AO,  2 * SIZE
	MADD	c41, c41, a1, b4
	LD	a1,  0 * SIZE(AO)

	MADD	c12, c12, a2, b1
	LD	b1,  4 * SIZE(BO)
	MADD	c22, c22, a2, b2
	LD	b2,  5 * SIZE(BO)
	MADD	c32, c32, a2, b3
	LD	b3,  6 * SIZE(BO)
	MADD	c42, c42, a2, b4
	LD	b4,  7 * SIZE(BO)

	bgtz	L, .L36
	daddiu	BO, BO,  4 * SIZE

.L38:
#if defined(LN) || defined(RT)
#ifdef LN
	daddiu	TEMP, KK, -2
#else
	daddiu	TEMP, KK, -4
#endif

	dsll	L,    TEMP, 1 + BASE_SHIFT
	dsll	TEMP, TEMP, 2 + BASE_SHIFT
	daddu	AO, AORIG, L
	daddu	BO, B,     TEMP
#endif


#if defined(LN) || defined(LT)
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)
	LD	b5,  4 * SIZE(BO)
	LD	b6,  5 * SIZE(BO)
	LD	b7,  6 * SIZE(BO)
	LD	b8,  7 * SIZE(BO)

	SUB	c11, b1, c11
	SUB	c21, b2, c21
	SUB	c31, b3, c31
	SUB	c41, b4, c41
	SUB	c12, b5, c12
	SUB	c22, b6, c22
	SUB	c32, b7, c32
	SUB	c42, b8, c42
#else
	LD	b1,  0 * SIZE(AO)
	LD	b2,  1 * SIZE(AO)
	LD	b3,  2 * SIZE(AO)
	LD	b4,  3 * SIZE(AO)
	LD	b5,  4 * SIZE(AO)
	LD	b6,  5 * SIZE(AO)
	LD	b7,  6 * SIZE(AO)
	LD	b8,  7 * SIZE(AO)

	SUB	c11, b1, c11
	SUB	c12, b2, c12
	SUB	c21, b3, c21
	SUB	c22, b4, c22
	SUB	c31, b5, c31
	SUB	c32, b6, c32
	SUB	c41, b7, c41
	SUB	c42, b8, c42
#endif

#ifdef LN
	LD	b1,  3 * SIZE(AO)
	LD	b2,  2 * SIZE(AO)
	LD	b3,  0 * SIZE(AO)

	MUL	c12, b1, c12
	MUL	c22, b1, c22
	MUL	c32, b1, c32
	MUL	c42, b1, c42

	NMSUB	c11, c11, b2, c12
	NMSUB	c21, c21, b2, c22
	NMSUB	c31, c31, b2, c32
	NMSUB	c41, c41, b2, c42

	MUL	c11, b3, c11
	MUL	c21, b3, c21
	MUL	c31, b3, c31
	MUL	c41, b3, c41
#endif

#ifdef LT
	LD	b1,  0 * SIZE(AO)
	LD	b2,  1 * SIZE(AO)
	LD	b3,  3 * SIZE(AO)

	MUL	c11, b1, c11
	MUL	c21, b1, c21
	MUL	c31, b1, c31
	MUL	c41, b1, c41

	NMSUB	c12, c12, b2, c11
	NMSUB	c22, c22, b2, c21
	NMSUB	c32, c32, b2, c31
	NMSUB	c42, c42, b2, c41

	MUL	c12, b3, c12
	MUL	c22, b3, c22
	MUL	c32, b3, c32
	MUL	c42, b3, c42
#endif

#ifdef RN
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	MUL	c11, b1, c11
	MUL	c12, b1, c12

	NMSUB	c21, c21, b2, c11
	NMSUB	c22, c22, b2, c12
	NMSUB	c31, c31, b3, c11
	NMSUB	c32, c32, b3, c12
	NMSUB	c41, c41, b4, c11
	NMSUB	c42, c42, b4, c12

	LD	b2,  5 * SIZE(BO)
	LD	b3,  6 * SIZE(BO)
	LD	b4,  7 * SIZE(BO)

	MUL	c21, b2, c21
	MUL	c22, b2, c22

	NMSUB	c31, c31, b3, c21
	NMSUB	c32, c32, b3, c22
	NMSUB	c41, c41, b4, c21
	NMSUB	c42, c42, b4, c22

	LD	b3, 10 * SIZE(BO)
	LD	b4, 11 * SIZE(BO)

	MUL	c31, b3, c31
	MUL	c32, b3, c32

	NMSUB	c41, c41, b4, c31
	NMSUB	c42, c42, b4, c32

	LD	b4, 15 * SIZE(BO)

	MUL	c41, b4, c41
	MUL	c42, b4, c42
#endif

#ifdef RT
	LD	b5, 15 * SIZE(BO)
	LD	b6, 14 * SIZE(BO)
	LD	b7, 13 * SIZE(BO)
	LD	b8, 12 * SIZE(BO)

	MUL	c41, b5, c41
	MUL	c42, b5, c42

	NMSUB	c31, c31, b6, c41
	NMSUB	c32, c32, b6, c42
	NMSUB	c21, c21, b7, c41
	NMSUB	c22, c22, b7, c42
	NMSUB	c11, c11, b8, c41
	NMSUB	c12, c12, b8, c42

	LD	b6, 10 * SIZE(BO)
	LD	b7,  9 * SIZE(BO)
	LD	b8,  8 * SIZE(BO)

	MUL	c31, b6, c31
	MUL	c32, b6, c32

	NMSUB	c21, c21, b7, c31
	NMSUB	c22, c22, b7, c32
	NMSUB	c11, c11, b8, c31
	NMSUB	c12, c12, b8, c32

	LD	b7,  5 * SIZE(BO)
	LD	b8,  4 * SIZE(BO)

	MUL	c21, b7, c21
	MUL	c22, b7, c22

	NMSUB	c11, c11, b8, c21
	NMSUB	c12, c12, b8, c22

	LD	b8,  0 * SIZE(BO)

	MUL	c11, b8, c11
	MUL	c12, b8, c12
#endif

#ifdef LN
	daddiu	CO1, CO1, -2 * SIZE
	daddiu	CO2, CO2, -2 * SIZE
	daddiu	CO3, CO3, -2 * SIZE
	daddiu	CO4, CO4, -2 * SIZE
#endif

#if defined(LN) || defined(LT)
	ST	c11,  0 * SIZE(BO)
	ST	c21,  1 * SIZE(BO)
	ST	c31,  2 * SIZE(BO)
	ST	c41,  3 * SIZE(BO)
	ST	c12,  4 * SIZE(BO)
	ST	c22,  5 * SIZE(BO)
	ST	c32,  6 * SIZE(BO)
	ST	c42,  7 * SIZE(BO)
#else
	ST	c11,  0 * SIZE(AO)
	ST	c12,  1 * SIZE(AO)
	ST	c21,  2 * SIZE(AO)
	ST	c22,  3 * SIZE(AO)
	ST	c31,  4 * SIZE(AO)
	ST	c32,  5 * SIZE(AO)
	ST	c41,  6 * SIZE(AO)
	ST	c42,  7 * SIZE(AO)
#endif

	ST	c11,  0 * SIZE(CO1)
	ST	c12,  1 * SIZE(CO1)
	ST	c21,  0 * SIZE(CO2)
	ST	c22,  1 * SIZE(CO2)
	ST	c31,  0 * SIZE(CO3)
	ST	c32,  1 * SIZE(CO3)
	ST	c41,  0 * SIZE(CO4)
	ST	c42,  1 * SIZE(CO4)

#ifndef LN
	daddiu	CO1, CO1, 2 * SIZE
	daddiu	CO2, CO2, 2 * SIZE
	daddiu	CO3, CO3, 2 * SIZE
	daddiu	CO4, CO4, 2 * SIZE
#endif

#ifdef RT
	dsll	TEMP, K, 1 + BASE_SHIFT
	daddu	AORIG, AORIG, TEMP
#endif

#if defined(LT) || defined(RN)
	dsubu	TEMP, K, KK
	dsll	L,    TEMP, 1 + BASE_SHIFT
	dsll	TEMP, TEMP, 2 + BASE_SHIFT
	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LT
	daddiu	KK, KK, 2
#endif

#ifdef LN
	daddiu	KK, KK, -2
#endif

	MTC	$0,  a1

	MOV	c11, a1
	MOV	c21, a1
	MOV	c31, a1

	daddiu	I, I, -1

	bgtz	I, .L31
	MOV	c41, c11
	.align 3

.L40:
	andi	I,  M, 1
	blez	I, .L49
	MOV	c61, c11

#if defined(LT) || defined(RN)
	LD	a1,  0 * SIZE(AO)
	MOV	c71, c11
	LD	a2,  1 * SIZE(AO)
	MOV	c81, c11

	LD	b1,  0 * SIZE(B)
	LD	b2,  1 * SIZE(B)
	LD	b3,  2 * SIZE(B)
	LD	b4,  3 * SIZE(B)
	LD	b5,  4 * SIZE(B)
	LD	b6,  8 * SIZE(B)
	LD	b7, 12 * SIZE(B)

	dsra	L,  KK, 2

	blez	L, .L45
	move	BO,  B
#else
#ifdef LN
	dsll	TEMP,   K,  BASE_SHIFT
	dsubu	AORIG, AORIG, TEMP
#endif

	dsll	L,    KK, 0 + BASE_SHIFT
	dsll	TEMP, KK, 2 + BASE_SHIFT

	daddu	AO, AORIG, L
	daddu	BO, B,     TEMP

	dsubu	TEMP, K, KK

	LD	a1,  0 * SIZE(AO)
	MOV	c71, c11
	LD	a2,  1 * SIZE(AO)
	MOV	c81, c11

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)
	LD	b5,  4 * SIZE(BO)
	LD	b6,  8 * SIZE(BO)
	LD	b7, 12 * SIZE(BO)

	dsra	L,  TEMP, 2

	blez	L, .L45
	NOP
#endif
	.align	3

.L42:
	MADD	c11, c11, a1, b1
	LD	b1, 16 * SIZE(BO)
	MADD	c21, c21, a1, b2
	LD	b2,  5 * SIZE(BO)
	MADD	c31, c31, a1, b3
	LD	b3,  6 * SIZE(BO)
	MADD	c41, c41, a1, b4
	LD	b4,  7 * SIZE(BO)

	LD	a1,  4 * SIZE(AO)
	daddiu	L, L, -1

	MADD	c11, c11, a2, b5
	LD	b5, 20 * SIZE(BO)
	MADD	c21, c21, a2, b2
	LD	b2,  9 * SIZE(BO)
	MADD	c31, c31, a2, b3
	LD	b3, 10 * SIZE(BO)
	MADD	c41, c41, a2, b4
	LD	b4, 11 * SIZE(BO)

	LD	a2,  2 * SIZE(AO)
	daddiu	AO, AO,  4 * SIZE

	MADD	c11, c11, a2, b6
	LD	b6, 24 * SIZE(BO)
	MADD	c21, c21, a2, b2
	LD	b2, 13 * SIZE(BO)
	MADD	c31, c31, a2, b3
	LD	b3, 14 * SIZE(BO)
	MADD	c41, c41, a2, b4
	LD	b4, 15 * SIZE(BO)

	LD	a2, -1 * SIZE(AO)
	daddiu	BO, BO, 16 * SIZE

	MADD	c11, c11, a2, b7
	LD	b7, 12 * SIZE(BO)
	MADD	c21, c21, a2, b2
	LD	b2,  1 * SIZE(BO)
	MADD	c31, c31, a2, b3
	LD	b3,  2 * SIZE(BO)
	MADD	c41, c41, a2, b4
	LD	b4,  3 * SIZE(BO)

	bgtz	L, .L42
	LD	a2,  1 * SIZE(AO)
	.align 3

.L45:
#if defined(LT) || defined(RN)
	andi	L, KK,  3
#else
	andi	L, TEMP, 3
#endif
	NOP
	blez	L, .L48
	NOP
	.align	3

.L46:
	MADD	c11, c11, a1, b1
	LD	b1,  4 * SIZE(BO)
	MADD	c21, c21, a1, b2
	LD	b2,  5 * SIZE(BO)
	MADD	c31, c31, a1, b3
	LD	b3,  6 * SIZE(BO)
	MADD	c41, c41, a1, b4
	LD	a1,  1 * SIZE(AO)

	LD	b4,  7 * SIZE(BO)
	daddiu	L, L, -1

	daddiu	AO, AO,  1 * SIZE
	MOV	a2, a2
	bgtz	L, .L46
	daddiu	BO, BO,  4 * SIZE


.L48:
#if defined(LN) || defined(RT)
#ifdef LN
	daddiu	TEMP, KK, -1
#else
	daddiu	TEMP, KK, -4
#endif

	dsll	L,    TEMP, 0 + BASE_SHIFT
	dsll	TEMP, TEMP, 2 + BASE_SHIFT
	daddu	AO, AORIG, L
	daddu	BO, B,     TEMP
#endif


#if defined(LN) || defined(LT)
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	SUB	c11, b1, c11
	SUB	c21, b2, c21
	SUB	c31, b3, c31
	SUB	c41, b4, c41
#else
	LD	b1,  0 * SIZE(AO)
	LD	b2,  1 * SIZE(AO)
	LD	b3,  2 * SIZE(AO)
	LD	b4,  3 * SIZE(AO)

	SUB	c11, b1, c11
	SUB	c21, b2, c21
	SUB	c31, b3, c31
	SUB	c41, b4, c41
#endif

#if defined(LN) || defined(LT)
	LD	b1,  0 * SIZE(AO)

	MUL	c11, b1, c11
	MUL	c21, b1, c21
	MUL	c31, b1, c31
	MUL	c41, b1, c41
#endif

#ifdef RN
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	MUL	c11, b1, c11

	NMSUB	c21, c21, b2, c11
	NMSUB	c31, c31, b3, c11
	NMSUB	c41, c41, b4, c11

	LD	b2,  5 * SIZE(BO)
	LD	b3,  6 * SIZE(BO)
	LD	b4,  7 * SIZE(BO)

	MUL	c21, b2, c21

	NMSUB	c31, c31, b3, c21
	NMSUB	c41, c41, b4, c21

	LD	b3, 10 * SIZE(BO)
	LD	b4, 11 * SIZE(BO)

	MUL	c31, b3, c31

	NMSUB	c41, c41, b4, c31

	LD	b4, 15 * SIZE(BO)

	MUL	c41, b4, c41
#endif

#ifdef RT
	LD	b5, 15 * SIZE(BO)
	LD	b6, 14 * SIZE(BO)
	LD	b7, 13 * SIZE(BO)
	LD	b8, 12 * SIZE(BO)

	MUL	c41, b5, c41

	NMSUB	c31, c31, b6, c41
	NMSUB	c21, c21, b7, c41
	NMSUB	c11, c11, b8, c41

	LD	b6, 10 * SIZE(BO)
	LD	b7,  9 * SIZE(BO)
	LD	b8,  8 * SIZE(BO)

	MUL	c31, b6, c31

	NMSUB	c21, c21, b7, c31
	NMSUB	c11, c11, b8, c31

	LD	b7,  5 * SIZE(BO)
	LD	b8,  4 * SIZE(BO)

	MUL	c21, b7, c21

	NMSUB	c11, c11, b8, c21

	LD	b8,  0 * SIZE(BO)

	MUL	c11, b8, c11
#endif

#ifdef LN
	daddiu	CO1, CO1, -1 * SIZE
	daddiu	CO2, CO2, -1 * SIZE
	daddiu	CO3, CO3, -1 * SIZE
	daddiu	CO4, CO4, -1 * SIZE
#endif

#if defined(LN) || defined(LT)
	ST	c11,  0 * SIZE(BO)
	ST	c21,  1 * SIZE(BO)
	ST	c31,  2 * SIZE(BO)
	ST	c41,  3 * SIZE(BO)
#else
	ST	c11,  0 * SIZE(AO)
	ST	c21,  1 * SIZE(AO)
	ST	c31,  2 * SIZE(AO)
	ST	c41,  3 * SIZE(AO)
#endif

	ST	c11,  0 * SIZE(CO1)
	ST	c21,  0 * SIZE(CO2)
	ST	c31,  0 * SIZE(CO3)
	ST	c41,  0 * SIZE(CO4)

#ifndef LN
	daddiu	CO1, CO1, 1 * SIZE
	daddiu	CO2, CO2, 1 * SIZE
	daddiu	CO3, CO3, 1 * SIZE
	daddiu	CO4, CO4, 1 * SIZE
#endif

#ifdef RT
	dsll	TEMP, K, BASE_SHIFT
	daddu	AORIG, AORIG, TEMP
#endif

#if defined(LT) || defined(RN)
	dsubu	TEMP, K, KK
	dsll	L,    TEMP, 0 + BASE_SHIFT
	dsll	TEMP, TEMP, 2 + BASE_SHIFT
	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LT
	daddiu	KK, KK, 1
#endif

#ifdef LN
	daddiu	KK, KK, -1
#endif
	.align 3

.L49:
#ifdef LN
	dsll	TEMP, K, 2 + BASE_SHIFT
	daddu	B, B, TEMP
#endif

#if defined(LT) || defined(RN)
	move	B,  BO
#endif

#ifdef RN
	daddiu	KK, KK,  4
#endif

#ifdef RT
	daddiu	KK, KK, -4
#endif
	.align 3

.L70:
	dsra	J,  N, 3
	blez	J, .L999
	nop

.L10:
#ifdef RT
	dsll	TEMP, K, 3 + BASE_SHIFT
	dsubu	B, B, TEMP

	dsll	TEMP, LDC, 3
	dsubu	C, C, TEMP
#endif

	move	CO1, C
	MTC	$0,  c11
	daddu	CO2, C,   LDC
	daddu	CO3, CO2, LDC
	daddiu	J, J, -1
	daddu	CO4, CO3, LDC
	MOV	c21, c11
	daddu	CO5, CO4, LDC
	MOV	c31, c11
	daddu	CO6, CO5, LDC
	MOV	c41, c11
	daddu	CO7, CO6, LDC
	MOV	c51, c11
	daddu	CO8, CO7, LDC
	dsra	I,  M, 1

#ifdef LN
	daddu	KK, M, OFFSET
#endif

#ifdef LT
	move	KK, OFFSET
#endif

#if defined(LN) || defined(RT)
	move	AORIG, A
#else
	move	AO, A
#endif
#ifndef RT
	daddu	C,  CO8, LDC
#endif

	blez	I, .L20
	MOV	c61, c11

.L11:
#if defined(LT) || defined(RN)
	LD	a1,  0 * SIZE(AO)
	MOV	c71, c11
	LD	b1,  0 * SIZE(B)
	MOV	c81, c11

	LD	a3,  4 * SIZE(AO)
	MOV	c12, c11
	LD	b2,  1 * SIZE(B)
	MOV	c22, c11

	dsra	L,  KK, 2
	MOV	c32, c11
	LD	b3,  2 * SIZE(B)
	MOV	c42, c11

	LD	b4,  3 * SIZE(B)
	MOV	c52, c11
	LD	b5,  4 * SIZE(B)
	MOV	c62, c11

	LD	b6,  8 * SIZE(B)
	MOV	c72, c11
	LD	b7, 12 * SIZE(B)
	MOV	c82, c11

	blez	L, .L15
	move	BO,  B
#else

#ifdef LN
	dsll	TEMP,   K,  1 + BASE_SHIFT
	dsubu	AORIG, AORIG, TEMP
#endif

	dsll	L,    KK, 1 + BASE_SHIFT
	dsll	TEMP, KK, 3 + BASE_SHIFT

	daddu	AO, AORIG, L
	daddu	BO, B,     TEMP

	dsubu	TEMP, K, KK

	LD	a1,  0 * SIZE(AO)
	MOV	c71, c11
	LD	b1,  0 * SIZE(BO)
	MOV	c81, c11

	LD	a3,  4 * SIZE(AO)
	MOV	c12, c11
	LD	b2,  1 * SIZE(BO)
	MOV	c22, c11

	MOV	c32, c11
	LD	b3,  2 * SIZE(BO)
	MOV	c42, c11

	LD	b4,  3 * SIZE(BO)
	MOV	c52, c11
	LD	b5,  4 * SIZE(BO)
	MOV	c62, c11

	LD	b6,  8 * SIZE(BO)
	MOV	c72, c11
	LD	b7, 12 * SIZE(BO)
	MOV	c82, c11

	dsra	L,  TEMP, 2
	blez	L, .L15
	NOP
#endif

	MADD	c11, c11, a1, b1
	LD	a2,  1 * SIZE(AO)
	MADD	c21, c21, a1, b2
	daddiu	L, L, -1
	MADD	c31, c31, a1, b3
	blez	L, .L13
	MADD	c41, c41, a1, b4
	NOP
	.align	3

.L12:
	MADD	c12, c12, a2, b1
	LD	b1, 16 * SIZE(BO)
	MADD	c22, c22, a2, b2
	LD	b2,  5 * SIZE(BO)
	MADD	c32, c32, a2, b3
	LD	b3,  6 * SIZE(BO)
	MADD	c42, c42, a2, b4
	LD	b4,  7 * SIZE(BO)

	MADD	c51, c51, a1, b5
	NOP
	MADD	c61, c61, a1, b2
	LD	a4,  2 * SIZE(AO)
	MADD	c71, c71, a1, b3
	NOP
	MADD	c81, c81, a1, b4
	LD	a1,  8 * SIZE(AO)

	MADD	c52, c52, a2, b5
	LD	b5, 20 * SIZE(BO)
	MADD	c62, c62, a2, b2
	LD	b2,  9 * SIZE(BO)
	MADD	c72, c72, a2, b3
	LD	b3, 10 * SIZE(BO)
	MADD	c82, c82, a2, b4
	LD	b4, 11 * SIZE(BO)

	MADD	c11, c11, a4, b6
	LD	a2,  3 * SIZE(AO)
	MADD	c21, c21, a4, b2
	NOP
	MADD	c31, c31, a4, b3
	NOP
	MADD	c41, c41, a4, b4
	NOP

	MADD	c12, c12, a2, b6
	LD	b6, 24 * SIZE(BO)
	MADD	c22, c22, a2, b2
	LD	b2, 13 * SIZE(BO)
	MADD	c32, c32, a2, b3
	LD	b3, 14 * SIZE(BO)
	MADD	c42, c42, a2, b4
	LD	b4, 15 * SIZE(BO)

	MADD	c51, c51, a4, b7
	NOP
	MADD	c61, c61, a4, b2
	NOP
	MADD	c71, c71, a4, b3
	NOP
	MADD	c81, c81, a4, b4
	NOP

	MADD	c52, c52, a2, b7
	LD	b7, 28 * SIZE(BO)
	MADD	c62, c62, a2, b2
	LD	b2, 17 * SIZE(BO)
	MADD	c72, c72, a2, b3
	LD	b3, 18 * SIZE(BO)
	MADD	c82, c82, a2, b4
	LD	b4, 19 * SIZE(BO)

	MADD	c11, c11, a3, b1
	LD	a2,  5 * SIZE(AO)
	MADD	c21, c21, a3, b2
	NOP
	MADD	c31, c31, a3, b3
	NOP
	MADD	c41, c41, a3, b4
	NOP

	MADD	c12, c12, a2, b1
	LD	b1, 32 * SIZE(BO)
	MADD	c22, c22, a2, b2
	LD	b2, 21 * SIZE(BO)
	MADD	c32, c32, a2, b3
	LD	b3, 22 * SIZE(BO)
	MADD	c42, c42, a2, b4
	LD	b4, 23 * SIZE(BO)

	MADD	c51, c51, a3, b5
	NOP
	MADD	c61, c61, a3, b2
	LD	a4,  6 * SIZE(AO)
	MADD	c71, c71, a3, b3
	NOP
	MADD	c81, c81, a3, b4
	LD	a3, 12 * SIZE(AO)

	MADD	c52, c52, a2, b5
	LD	b5, 36 * SIZE(BO)
	MADD	c62, c62, a2, b2
	LD	b2, 25 * SIZE(BO)
	MADD	c72, c72, a2, b3
	LD	b3, 26 * SIZE(BO)
	MADD	c82, c82, a2, b4
	LD	b4, 27 * SIZE(BO)

	MADD	c11, c11, a4, b6
	LD	a2,  7 * SIZE(AO)
	MADD	c21, c21, a4, b2
	NOP
	MADD	c31, c31, a4, b3
	NOP
	MADD	c41, c41, a4, b4
	daddiu	L, L, -1

	MADD	c12, c12, a2, b6
	LD	b6, 40 * SIZE(BO)
	MADD	c22, c22, a2, b2
	LD	b2, 29 * SIZE(BO)
	MADD	c32, c32, a2, b3
	LD	b3, 30 * SIZE(BO)
	MADD	c42, c42, a2, b4
	LD	b4, 31 * SIZE(BO)

	MADD	c51, c51, a4, b7
	daddiu	BO, BO, 32 * SIZE
	MADD	c61, c61, a4, b2
	daddiu	AO, AO,  8 * SIZE
	MADD	c71, c71, a4, b3
	NOP
	MADD	c81, c81, a4, b4
	NOP

	MADD	c52, c52, a2, b7
	LD	b7, 12 * SIZE(BO)
	MADD	c62, c62, a2, b2
	LD	b2,  1 * SIZE(BO)
	MADD	c72, c72, a2, b3
	LD	b3,  2 * SIZE(BO)
	MADD	c82, c82, a2, b4
	LD	b4,  3 * SIZE(BO)

	MADD	c11, c11, a1, b1
	LD	a2,  1 * SIZE(AO)
	MADD	c21, c21, a1, b2
	NOP
	MADD	c31, c31, a1, b3
	bgtz	L, .L12
	MADD	c41, c41, a1, b4
	NOP
	.align 3

.L13:
	MADD	c12, c12, a2, b1
	LD	b1, 16 * SIZE(BO)
	MADD	c22, c22, a2, b2
	LD	b2,  5 * SIZE(BO)
	MADD	c32, c32, a2, b3
	LD	b3,  6 * SIZE(BO)
	MADD	c42, c42, a2, b4
	LD	b4,  7 * SIZE(BO)

	MADD	c51, c51, a1, b5
	NOP
	MADD	c61, c61, a1, b2
	LD	a4,  2 * SIZE(AO)
	MADD	c71, c71, a1, b3
	NOP
	MADD	c81, c81, a1, b4
	LD	a1,  8 * SIZE(AO)

	MADD	c52, c52, a2, b5
	LD	b5, 20 * SIZE(BO)
	MADD	c62, c62, a2, b2
	LD	b2,  9 * SIZE(BO)
	MADD	c72, c72, a2, b3
	LD	b3, 10 * SIZE(BO)
	MADD	c82, c82, a2, b4
	LD	b4, 11 * SIZE(BO)

	MADD	c11, c11, a4, b6
	LD	a2,  3 * SIZE(AO)
	MADD	c21, c21, a4, b2
	NOP
	MADD	c31, c31, a4, b3
	NOP
	MADD	c41, c41, a4, b4
	NOP

	MADD	c12, c12, a2, b6
	LD	b6, 24 * SIZE(BO)
	MADD	c22, c22, a2, b2
	LD	b2, 13 * SIZE(BO)
	MADD	c32, c32, a2, b3
	LD	b3, 14 * SIZE(BO)
	MADD	c42, c42, a2, b4
	LD	b4, 15 * SIZE(BO)

	MADD	c51, c51, a4, b7
	NOP
	MADD	c61, c61, a4, b2
	NOP
	MADD	c71, c71, a4, b3
	NOP
	MADD	c81, c81, a4, b4
	NOP

	MADD	c52, c52, a2, b7
	LD	b7, 28 * SIZE(BO)
	MADD	c62, c62, a2, b2
	LD	b2, 17 * SIZE(BO)
	MADD	c72, c72, a2, b3
	LD	b3, 18 * SIZE(BO)
	MADD	c82, c82, a2, b4
	LD	b4, 19 * SIZE(BO)

	MADD	c11, c11, a3, b1
	LD	a2,  5 * SIZE(AO)
	MADD	c21, c21, a3, b2
	NOP
	MADD	c31, c31, a3, b3
	NOP
	MADD	c41, c41, a3, b4
	NOP

	MADD	c12, c12, a2, b1
	LD	b1, 32 * SIZE(BO)
	MADD	c22, c22, a2, b2
	LD	b2, 21 * SIZE(BO)
	MADD	c32, c32, a2, b3
	LD	b3, 22 * SIZE(BO)
	MADD	c42, c42, a2, b4
	LD	b4, 23 * SIZE(BO)

	MADD	c51, c51, a3, b5
	NOP
	MADD	c61, c61, a3, b2
	LD	a4,  6 * SIZE(AO)
	MADD	c71, c71, a3, b3
	NOP
	MADD	c81, c81, a3, b4
	LD	a3, 12 * SIZE(AO)

	MADD	c52, c52, a2, b5
	LD	b5, 36 * SIZE(BO)
	MADD	c62, c62, a2, b2
	LD	b2, 25 * SIZE(BO)
	MADD	c72, c72, a2, b3
	LD	b3, 26 * SIZE(BO)
	MADD	c82, c82, a2, b4
	LD	b4, 27 * SIZE(BO)

	MADD	c11, c11, a4, b6
	LD	a2,  7 * SIZE(AO)
	MADD	c21, c21, a4, b2
	NOP
	MADD	c31, c31, a4, b3
	NOP
	MADD	c41, c41, a4, b4
	NOP

	MADD	c12, c12, a2, b6
	LD	b6, 40 * SIZE(BO)
	MADD	c22, c22, a2, b2
	LD	b2, 29 * SIZE(BO)
	MADD	c32, c32, a2, b3
	LD	b3, 30 * SIZE(BO)
	MADD	c42, c42, a2, b4
	LD	b4, 31 * SIZE(BO)

	MADD	c51, c51, a4, b7
	daddiu	BO, BO, 32 * SIZE
	MADD	c61, c61, a4, b2
	daddiu	AO, AO,  8 * SIZE
	MADD	c71, c71, a4, b3
	NOP
	MADD	c81, c81, a4, b4
	NOP

	MADD	c52, c52, a2, b7
	LD	b7, 12 * SIZE(BO)
	MADD	c62, c62, a2, b2
	LD	b2,  1 * SIZE(BO)
	MADD	c72, c72, a2, b3
	LD	b3,  2 * SIZE(BO)
	MADD	c82, c82, a2, b4
	LD	b4,  3 * SIZE(BO)
	.align 3

.L15:
#if defined(LT) || defined(RN)
	andi	L, KK,  3
#else
	andi	L, TEMP, 3
#endif
	blez	L, .L18
	NOP
	.align	3

.L16:
	MADD	c11, c11, a1, b1
	LD	a2,  1 * SIZE(AO)
	MADD	c21, c21, a1, b2
	NOP
	MADD	c31, c31, a1, b3
	NOP
	MADD	c41, c41, a1, b4
	NOP

	MADD	c12, c12, a2, b1
	LD	b1,  8 * SIZE(BO)
	MADD	c22, c22, a2, b2
	LD	b2,  5 * SIZE(BO)
	MADD	c32, c32, a2, b3
	LD	b3,  6 * SIZE(BO)
	MADD	c42, c42, a2, b4
	LD	b4,  7 * SIZE(BO)

	MADD	c51, c51, a1, b5
	daddiu	L, L, -1
	MADD	c61, c61, a1, b2
	daddiu	AO, AO,  2 * SIZE
	MADD	c71, c71, a1, b3
	daddiu	BO, BO,  8 * SIZE
	MADD	c81, c81, a1, b4
	LD	a1,  0 * SIZE(AO)

	MADD	c52, c52, a2, b5
	LD	b5,  4 * SIZE(BO)
	MADD	c62, c62, a2, b2
	LD	b2,  1 * SIZE(BO)
	MADD	c72, c72, a2, b3
	LD	b3,  2 * SIZE(BO)
	MADD	c82, c82, a2, b4
	bgtz	L, .L16
	LD	b4,  3 * SIZE(BO)

.L18:
#if defined(LN) || defined(RT)
#ifdef LN
	daddiu	TEMP, KK, -2
#else
	daddiu	TEMP, KK, -8
#endif

	dsll	L,    TEMP, 1 + BASE_SHIFT
	dsll	TEMP, TEMP, 3 + BASE_SHIFT
	daddu	AO, AORIG, L
	daddu	BO, B,     TEMP
#endif

#if defined(LN) || defined(LT)
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	SUB	c11, b1, c11
	LD	b5,  4 * SIZE(BO)
	SUB	c21, b2, c21
	LD	b6,  5 * SIZE(BO)
	SUB	c31, b3, c31
	LD	b7,  6 * SIZE(BO)
	SUB	c41, b4, c41
	LD	b8,  7 * SIZE(BO)

	SUB	c51, b5, c51
	LD	b1,  8 * SIZE(BO)
	SUB	c61, b6, c61
	LD	b2,  9 * SIZE(BO)
	SUB	c71, b7, c71
	LD	b3, 10 * SIZE(BO)
	SUB	c81, b8, c81
	LD	b4, 11 * SIZE(BO)

	SUB	c12, b1, c12
	LD	b5, 12 * SIZE(BO)
	SUB	c22, b2, c22
	LD	b6, 13 * SIZE(BO)
	SUB	c32, b3, c32
	LD	b7, 14 * SIZE(BO)
	SUB	c42, b4, c42
	LD	b8, 15 * SIZE(BO)

	SUB	c52, b5, c52
#ifdef LN
	LD	b1,  3 * SIZE(AO)
#else
	LD	b1,  0 * SIZE(AO)
#endif
	SUB	c62, b6, c62
	SUB	c72, b7, c72
	SUB	c82, b8, c82
#else
	LD	b1,  0 * SIZE(AO)
	LD	b2,  1 * SIZE(AO)
	LD	b3,  2 * SIZE(AO)
	LD	b4,  3 * SIZE(AO)

	SUB	c11, b1, c11
	LD	b5,  4 * SIZE(AO)
	SUB	c12, b2, c12
	LD	b6,  5 * SIZE(AO)
	SUB	c21, b3, c21
	LD	b7,  6 * SIZE(AO)
	SUB	c22, b4, c22
	LD	b8,  7 * SIZE(AO)

	SUB	c31, b5, c31
	LD	b1,  8 * SIZE(AO)
	SUB	c32, b6, c32
	LD	b2,  9 * SIZE(AO)
	SUB	c41, b7, c41
	LD	b3, 10 * SIZE(AO)
	SUB	c42, b8, c42
	LD	b4, 11 * SIZE(AO)

	LD	b5, 12 * SIZE(AO)
	SUB	c51, b1, c51
	LD	b6, 13 * SIZE(AO)
	SUB	c52, b2, c52
	LD	b7, 14 * SIZE(AO)
	SUB	c61, b3, c61
	LD	b8, 15 * SIZE(AO)
	SUB	c62, b4, c62

	SUB	c71, b5, c71
	SUB	c72, b6, c72
	SUB	c81, b7, c81
	SUB	c82, b8, c82
#endif

#ifdef LN
	MUL	c12, b1, c12
	LD	b2,  2 * SIZE(AO)
	MUL	c22, b1, c22
	MUL	c32, b1, c32
	MUL	c42, b1, c42
	MUL	c52, b1, c52
	MUL	c62, b1, c62
	MUL	c72, b1, c72
	MUL	c82, b1, c82

	NMSUB	c11, c11, b2, c12
	LD	b3,  0 * SIZE(AO)
	NMSUB	c21, c21, b2, c22
	NMSUB	c31, c31, b2, c32
	NMSUB	c41, c41, b2, c42
	NMSUB	c51, c51, b2, c52
	NMSUB	c61, c61, b2, c62
	NMSUB	c71, c71, b2, c72
	NMSUB	c81, c81, b2, c82

	MUL	c11, b3, c11
	daddiu	CO1, CO1, -2 * SIZE
	MUL	c21, b3, c21
	daddiu	CO2, CO2, -2 * SIZE
	MUL	c31, b3, c31
	daddiu	CO3, CO3, -2 * SIZE
	MUL	c41, b3, c41
	daddiu	CO4, CO4, -2 * SIZE
	MUL	c51, b3, c51
	daddiu	CO5, CO5, -2 * SIZE
	MUL	c61, b3, c61
	daddiu	CO6, CO6, -2 * SIZE
	MUL	c71, b3, c71
	daddiu	CO7, CO7, -2 * SIZE
	MUL	c81, b3, c81
	daddiu	CO8, CO8, -2 * SIZE
#endif

#ifdef LT
	MUL	c11, b1, c11
	LD	b2,  1 * SIZE(AO)
	MUL	c21, b1, c21
	MUL	c31, b1, c31
	MUL	c41, b1, c41
	MUL	c51, b1, c51
	MUL	c61, b1, c61
	MUL	c71, b1, c71
	MUL	c81, b1, c81

	NMSUB	c12, c12, b2, c11
	LD	b3,  3 * SIZE(AO)
	NMSUB	c22, c22, b2, c21
	NMSUB	c32, c32, b2, c31
	NMSUB	c42, c42, b2, c41
	NMSUB	c52, c52, b2, c51
	NMSUB	c62, c62, b2, c61
	NMSUB	c72, c72, b2, c71
	NMSUB	c82, c82, b2, c81

	MUL	c12, b3, c12
	MUL	c22, b3, c22
	MUL	c32, b3, c32
	MUL	c42, b3, c42
	MUL	c52, b3, c52
	MUL	c62, b3, c62
	MUL	c72, b3, c72
	MUL	c82, b3, c82
#endif

#ifdef RN
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	MUL	c11, b1, c11
	MUL	c12, b1, c12
	LD	b5,  4 * SIZE(BO)

	NMSUB	c21, c21, b2, c11
	NMSUB	c22, c22, b2, c12
	LD	b6,  5 * SIZE(BO)
	NMSUB	c31, c31, b3, c11
	NMSUB	c32, c32, b3, c12
	LD	b7,  6 * SIZE(BO)
	NMSUB	c41, c41, b4, c11
	NMSUB	c42, c42, b4, c12
	LD	b8,  7 * SIZE(BO)

	NMSUB	c51, c51, b5, c11
	NMSUB	c52, c52, b5, c12
	LD	b2,  9 * SIZE(BO)
	NMSUB	c61, c61, b6, c11
	NMSUB	c62, c62, b6, c12
	LD	b3, 10 * SIZE(BO)
	NMSUB	c71, c71, b7, c11
	NMSUB	c72, c72, b7, c12
	LD	b4, 11 * SIZE(BO)
	NMSUB	c81, c81, b8, c11
	NMSUB	c82, c82, b8, c12
	LD	b5, 12 * SIZE(BO)

	MUL	c21, b2, c21
	MUL	c22, b2, c22
	LD	b6, 13 * SIZE(BO)

	NMSUB	c31, c31, b3, c21
	NMSUB	c32, c32, b3, c22
	LD	b7, 14 * SIZE(BO)
	NMSUB	c41, c41, b4, c21
	NMSUB	c42, c42, b4, c22
	LD	b8, 15 * SIZE(BO)
	NMSUB	c51, c51, b5, c21
	NMSUB	c52, c52, b5, c22
	LD	b3, 18 * SIZE(BO)
	NMSUB	c61, c61, b6, c21
	NMSUB	c62, c62, b6, c22
	LD	b4, 19 * SIZE(BO)
	NMSUB	c71, c71, b7, c21
	NMSUB	c72, c72, b7, c22
	LD	b5, 20 * SIZE(BO)
	NMSUB	c81, c81, b8, c21
	NMSUB	c82, c82, b8, c22
	LD	b6, 21 * SIZE(BO)

	MUL	c31, b3, c31
	MUL	c32, b3, c32
	LD	b7, 22 * SIZE(BO)

	NMSUB	c41, c41, b4, c31
	NMSUB	c42, c42, b4, c32
	LD	b8, 23 * SIZE(BO)
	NMSUB	c51, c51, b5, c31
	NMSUB	c52, c52, b5, c32
	LD	b4, 27 * SIZE(BO)
	NMSUB	c61, c61, b6, c31
	NMSUB	c62, c62, b6, c32
	LD	b5, 28 * SIZE(BO)
	NMSUB	c71, c71, b7, c31
	NMSUB	c72, c72, b7, c32
	LD	b6, 29 * SIZE(BO)
	NMSUB	c81, c81, b8, c31
	NMSUB	c82, c82, b8, c32
	LD	b7, 30 * SIZE(BO)

	MUL	c41, b4, c41
	MUL	c42, b4, c42
	LD	b8, 31 * SIZE(BO)

	NMSUB	c51, c51, b5, c41
	NMSUB	c52, c52, b5, c42
	LD	b5, 36 * SIZE(BO)
	NMSUB	c61, c61, b6, c41
	NMSUB	c62, c62, b6, c42
	LD	b6, 37 * SIZE(BO)
	NMSUB	c71, c71, b7, c41
	NMSUB	c72, c72, b7, c42
	LD	b7, 38 * SIZE(BO)
	NMSUB	c81, c81, b8, c41
	NMSUB	c82, c82, b8, c42
	LD	b8, 39 * SIZE(BO)

	MUL	c51, b5, c51
	MUL	c52, b5, c52

	NMSUB	c61, c61, b6, c51
	NMSUB	c62, c62, b6, c52
	LD	b6, 45 * SIZE(BO)
	NMSUB	c71, c71, b7, c51
	NMSUB	c72, c72, b7, c52
	LD	b7, 46 * SIZE(BO)
	NMSUB	c81, c81, b8, c51
	NMSUB	c82, c82, b8, c52
	LD	b8, 47 * SIZE(BO)

	MUL	c61, b6, c61
	MUL	c62, b6, c62

	NMSUB	c71, c71, b7, c61
	NMSUB	c72, c72, b7, c62
	LD	b7, 54 * SIZE(BO)
	NMSUB	c81, c81, b8, c61
	NMSUB	c82, c82, b8, c62
	LD	b8, 55 * SIZE(BO)

	MUL	c71, b7, c71
	MUL	c72, b7, c72

	NMSUB	c81, c81, b8, c71
	NMSUB	c82, c82, b8, c72
	LD	b8, 63 * SIZE(BO)

	MUL	c81, b8, c81
	MUL	c82, b8, c82
#endif

#ifdef RT
	LD	b1, 63 * SIZE(BO)
	LD	b2, 62 * SIZE(BO)
	LD	b3, 61 * SIZE(BO)
	LD	b4, 60 * SIZE(BO)

	MUL	c81, b1, c81
	MUL	c82, b1, c82
	LD	b5, 59 * SIZE(BO)

	NMSUB	c71, c71, b2, c81
	NMSUB	c72, c72, b2, c82
	LD	b6, 58 * SIZE(BO)
	NMSUB	c61, c61, b3, c81
	NMSUB	c62, c62, b3, c82
	LD	b7, 57 * SIZE(BO)
	NMSUB	c51, c51, b4, c81
	NMSUB	c52, c52, b4, c82
	LD	b8, 56 * SIZE(BO)

	NMSUB	c41, c41, b5, c81
	NMSUB	c42, c42, b5, c82
	LD	b2, 54 * SIZE(BO)
	NMSUB	c31, c31, b6, c81
	NMSUB	c32, c32, b6, c82
	LD	b3, 53 * SIZE(BO)
	NMSUB	c21, c21, b7, c81
	NMSUB	c22, c22, b7, c82
	LD	b4, 52 * SIZE(BO)
	NMSUB	c11, c11, b8, c81
	NMSUB	c12, c12, b8, c82
	LD	b5, 51 * SIZE(BO)

	MUL	c71, b2, c71
	MUL	c72, b2, c72
	LD	b6, 50 * SIZE(BO)

	NMSUB	c61, c61, b3, c71
	NMSUB	c62, c62, b3, c72
	LD	b7, 49 * SIZE(BO)
	NMSUB	c51, c51, b4, c71
	NMSUB	c52, c52, b4, c72
	LD	b8, 48 * SIZE(BO)
	NMSUB	c41, c41, b5, c71
	NMSUB	c42, c42, b5, c72
	LD	b3, 45 * SIZE(BO)
	NMSUB	c31, c31, b6, c71
	NMSUB	c32, c32, b6, c72
	LD	b4, 44 * SIZE(BO)
	NMSUB	c21, c21, b7, c71
	NMSUB	c22, c22, b7, c72
	LD	b5, 43 * SIZE(BO)
	NMSUB	c11, c11, b8, c71
	NMSUB	c12, c12, b8, c72
	LD	b6, 42 * SIZE(BO)

	MUL	c61, b3, c61
	MUL	c62, b3, c62
	LD	b7, 41 * SIZE(BO)

	NMSUB	c51, c51, b4, c61
	NMSUB	c52, c52, b4, c62
	LD	b8, 40 * SIZE(BO)
	NMSUB	c41, c41, b5, c61
	NMSUB	c42, c42, b5, c62
	LD	b4, 36 * SIZE(BO)
	NMSUB	c31, c31, b6, c61
	NMSUB	c32, c32, b6, c62
	LD	b5, 35 * SIZE(BO)
	NMSUB	c21, c21, b7, c61
	NMSUB	c22, c22, b7, c62
	LD	b6, 34 * SIZE(BO)
	NMSUB	c11, c11, b8, c61
	NMSUB	c12, c12, b8, c62
	LD	b7, 33 * SIZE(BO)

	MUL	c51, b4, c51
	MUL	c52, b4, c52
	LD	b8, 32 * SIZE(BO)

	NMSUB	c41, c41, b5, c51
	NMSUB	c42, c42, b5, c52
	LD	b5, 27 * SIZE(BO)
	NMSUB	c31, c31, b6, c51
	NMSUB	c32, c32, b6, c52
	LD	b6, 26 * SIZE(BO)
	NMSUB	c21, c21, b7, c51
	NMSUB	c22, c22, b7, c52
	LD	b7, 25 * SIZE(BO)
	NMSUB	c11, c11, b8, c51
	NMSUB	c12, c12, b8, c52
	LD	b8, 24 * SIZE(BO)

	MUL	c41, b5, c41
	MUL	c42, b5, c42

	NMSUB	c31, c31, b6, c41
	NMSUB	c32, c32, b6, c42
	LD	b6, 18 * SIZE(BO)
	NMSUB	c21, c21, b7, c41
	NMSUB	c22, c22, b7, c42
	LD	b7, 17 * SIZE(BO)
	NMSUB	c11, c11, b8, c41
	NMSUB	c12, c12, b8, c42
	LD	b8, 16 * SIZE(BO)

	MUL	c31, b6, c31
	MUL	c32, b6, c32

	NMSUB	c21, c21, b7, c31
	NMSUB	c22, c22, b7, c32
	LD	b7,  9 * SIZE(BO)
	NMSUB	c11, c11, b8, c31
	NMSUB	c12, c12, b8, c32
	LD	b8,  8 * SIZE(BO)

	MUL	c21, b7, c21
	MUL	c22, b7, c22

	NMSUB	c11, c11, b8, c21
	NMSUB	c12, c12, b8, c22
	LD	b8,  0 * SIZE(BO)

	MUL	c11, b8, c11
	MUL	c12, b8, c12
#endif

#if defined(LN) || defined(LT)
	ST	c11,  0 * SIZE(BO)
	ST	c21,  1 * SIZE(BO)
	ST	c31,  2 * SIZE(BO)
	ST	c41,  3 * SIZE(BO)
	ST	c51,  4 * SIZE(BO)
	ST	c61,  5 * SIZE(BO)
	ST	c71,  6 * SIZE(BO)
	ST	c81,  7 * SIZE(BO)

	ST	c12,  8 * SIZE(BO)
	ST	c22,  9 * SIZE(BO)
	ST	c32, 10 * SIZE(BO)
	ST	c42, 11 * SIZE(BO)
	ST	c52, 12 * SIZE(BO)
	ST	c62, 13 * SIZE(BO)
	ST	c72, 14 * SIZE(BO)
	ST	c82, 15 * SIZE(BO)
#else
	ST	c11,  0 * SIZE(AO)
	ST	c12,  1 * SIZE(AO)
	ST	c21,  2 * SIZE(AO)
	ST	c22,  3 * SIZE(AO)
	ST	c31,  4 * SIZE(AO)
	ST	c32,  5 * SIZE(AO)
	ST	c41,  6 * SIZE(AO)
	ST	c42,  7 * SIZE(AO)

	ST	c51,  8 * SIZE(AO)
	ST	c52,  9 * SIZE(AO)
	ST	c61, 10 * SIZE(AO)
	ST	c62, 11 * SIZE(AO)
	ST	c71, 12 * SIZE(AO)
	ST	c72, 13 * SIZE(AO)
	ST	c81, 14 * SIZE(AO)
	ST	c82, 15 * SIZE(AO)
#endif

	ST	c11,  0 * SIZE(CO1)
	ST	c12,  1 * SIZE(CO1)
	ST	c21,  0 * SIZE(CO2)
	ST	c22,  1 * SIZE(CO2)
	ST	c31,  0 * SIZE(CO3)
	ST	c32,  1 * SIZE(CO3)
	ST	c41,  0 * SIZE(CO4)
	ST	c42,  1 * SIZE(CO4)
	ST	c51,  0 * SIZE(CO5)
	ST	c52,  1 * SIZE(CO5)
	ST	c61,  0 * SIZE(CO6)
	ST	c62,  1 * SIZE(CO6)
	ST	c71,  0 * SIZE(CO7)
	ST	c72,  1 * SIZE(CO7)
	ST	c81,  0 * SIZE(CO8)
	ST	c82,  1 * SIZE(CO8)

	MTC	$0,  a1

#ifndef LN
	daddiu	CO1, CO1, 2 * SIZE
	daddiu	CO2, CO2, 2 * SIZE
	daddiu	CO3, CO3, 2 * SIZE
	daddiu	CO4, CO4, 2 * SIZE
	daddiu	CO5, CO5, 2 * SIZE
	daddiu	CO6, CO6, 2 * SIZE
	daddiu	CO7, CO7, 2 * SIZE
	daddiu	CO8, CO8, 2 * SIZE
#endif

	MOV	c11, a1
	MOV	c21, a1

#ifdef RT
	dsll	TEMP, K, 1 + BASE_SHIFT
	daddu	AORIG, AORIG, TEMP
#endif

	MOV	c31, a1
	MOV	c41, a1

#if defined(LT) || defined(RN)
	dsubu	TEMP, K, KK
	dsll	L,    TEMP, 1 + BASE_SHIFT
	dsll	TEMP, TEMP, 3 + BASE_SHIFT
	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LT
	daddiu	KK, KK, 2
#endif

#ifdef LN
	daddiu	KK, KK, -2
#endif

	daddiu	I, I, -1
	MOV	c51, a1

	bgtz	I, .L11
	MOV	c61, a1
	.align 3

.L20:
	andi	I,  M, 1
	MOV	c61, c11
	blez	I, .L29
	MOV	c71, c11

#if defined(LT) || defined(RN)
	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(B)
	LD	b2,  1 * SIZE(B)
	LD	b3,  2 * SIZE(B)
	LD	b4,  3 * SIZE(B)
	LD	b5,  4 * SIZE(B)
	LD	b6,  8 * SIZE(B)
	LD	b7, 12 * SIZE(B)

	dsra	L,  KK, 2
	MOV	c81, c11

	blez	L, .L25
	move	BO,  B
#else

#ifdef LN
	dsll	TEMP,   K,  0 + BASE_SHIFT
	dsubu	AORIG, AORIG, TEMP
#endif

	dsll	L,    KK, 0 + BASE_SHIFT
	dsll	TEMP, KK, 3 + BASE_SHIFT

	daddu	AO, AORIG, L
	daddu	BO, B,     TEMP

	dsubu	TEMP, K, KK

	LD	a1,  0 * SIZE(AO)
	LD	a2,  1 * SIZE(AO)
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)
	LD	b5,  4 * SIZE(BO)
	LD	b6,  8 * SIZE(BO)
	LD	b7, 12 * SIZE(BO)

	dsra	L,  TEMP, 2
	MOV	c81, c11

	blez	L, .L25
	NOP
#endif
	.align	3

.L22:
	MADD	c11, c11, a1, b1
	LD	b1, 16 * SIZE(BO)
	MADD	c21, c21, a1, b2
	LD	b2,  5 * SIZE(BO)
	MADD	c31, c31, a1, b3
	LD	b3,  6 * SIZE(BO)
	MADD	c41, c41, a1, b4
	LD	b4,  7 * SIZE(BO)

	MADD	c51, c51, a1, b5
	LD	b5, 20 * SIZE(BO)
	MADD	c61, c61, a1, b2
	LD	b2,  9 * SIZE(BO)
	MADD	c71, c71, a1, b3
	LD	b3, 10 * SIZE(BO)
	MADD	c81, c81, a1, b4
	LD	b4, 11 * SIZE(BO)

	LD	a1,  4 * SIZE(AO)
	daddiu	L, L, -1

	MADD	c11, c11, a2, b6
	LD	b6, 24 * SIZE(BO)
	MADD	c21, c21, a2, b2
	LD	b2, 13 * SIZE(BO)
	MADD	c31, c31, a2, b3
	LD	b3, 14 * SIZE(BO)
	MADD	c41, c41, a2, b4
	LD	b4, 15 * SIZE(BO)

	MADD	c51, c51, a2, b7
	LD	b7, 28 * SIZE(BO)
	MADD	c61, c61, a2, b2
	LD	b2, 17 * SIZE(BO)
	MADD	c71, c71, a2, b3
	LD	b3, 18 * SIZE(BO)
	MADD	c81, c81, a2, b4
	LD	b4, 19 * SIZE(BO)

	LD	a2,  5 * SIZE(AO)
	daddiu	AO, AO,  4 * SIZE

	MADD	c11, c11, a3, b1
	LD	b1, 32 * SIZE(BO)
	MADD	c21, c21, a3, b2
	LD	b2, 21 * SIZE(BO)
	MADD	c31, c31, a3, b3
	LD	b3, 22 * SIZE(BO)
	MADD	c41, c41, a3, b4
	LD	b4, 23 * SIZE(BO)

	MADD	c51, c51, a3, b5
	LD	b5, 36 * SIZE(BO)
	MADD	c61, c61, a3, b2
	LD	b2, 25 * SIZE(BO)
	MADD	c71, c71, a3, b3
	LD	b3, 26 * SIZE(BO)
	MADD	c81, c81, a3, b4
	LD	b4, 27 * SIZE(BO)

	LD	a3,  2 * SIZE(AO)
	daddiu	BO, BO, 32 * SIZE

	MADD	c11, c11, a4, b6
	LD	b6,  8 * SIZE(BO)
	MADD	c21, c21, a4, b2
	LD	b2, -3 * SIZE(BO)
	MADD	c31, c31, a4, b3
	LD	b3, -2 * SIZE(BO)
	MADD	c41, c41, a4, b4
	LD	b4, -1 * SIZE(BO)

	MADD	c51, c51, a4, b7
	LD	b7, 12 * SIZE(BO)
	MADD	c61, c61, a4, b2
	LD	b2,  1 * SIZE(BO)
	MADD	c71, c71, a4, b3
	LD	b3,  2 * SIZE(BO)
	MADD	c81, c81, a4, b4
	LD	b4,  3 * SIZE(BO)
	bgtz	L, .L22
	LD	a4,  3 * SIZE(AO)
	.align 3

.L25:
#if defined(LT) || defined(RN)
	andi	L, KK,  3
#else
	andi	L, TEMP, 3
#endif
	NOP
	blez	L, .L28
	NOP
	.align	3

.L26:
	MADD	c11, c11, a1, b1
	LD	b1,  8 * SIZE(BO)
	MADD	c21, c21, a1, b2
	LD	b2,  5 * SIZE(BO)
	MADD	c31, c31, a1, b3
	LD	b3,  6 * SIZE(BO)
	MADD	c41, c41, a1, b4
	LD	b4,  7 * SIZE(BO)

	daddiu	L, L, -1
	MOV	a2, a2
	daddiu	AO, AO,  1 * SIZE
	daddiu	BO, BO,  8 * SIZE

	MADD	c51, c51, a1, b5
	LD	b5,  4 * SIZE(BO)
	MADD	c61, c61, a1, b2
	LD	b2,  1 * SIZE(BO)
	MADD	c71, c71, a1, b3
	LD	b3,  2 * SIZE(BO)
	MADD	c81, c81, a1, b4
	LD	a1,  0 * SIZE(AO)

	bgtz	L, .L26
	LD	b4,  3 * SIZE(BO)

.L28:
#if defined(LN) || defined(RT)
#ifdef LN
	daddiu	TEMP, KK, -1
#else
	daddiu	TEMP, KK, -8
#endif

	dsll	L,    TEMP, 0 + BASE_SHIFT
	dsll	TEMP, TEMP, 3 + BASE_SHIFT
	daddu	AO, AORIG, L
	daddu	BO, B,     TEMP
#endif


#if defined(LN) || defined(LT)
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)
	LD	b5,  4 * SIZE(BO)
	LD	b6,  5 * SIZE(BO)
	LD	b7,  6 * SIZE(BO)
	LD	b8,  7 * SIZE(BO)

	SUB	c11, b1, c11
	SUB	c21, b2, c21
	SUB	c31, b3, c31
	SUB	c41, b4, c41
	SUB	c51, b5, c51
	SUB	c61, b6, c61
	SUB	c71, b7, c71
	SUB	c81, b8, c81
#else
	LD	b1,  0 * SIZE(AO)
	LD	b2,  1 * SIZE(AO)
	LD	b3,  2 * SIZE(AO)
	LD	b4,  3 * SIZE(AO)
	LD	b5,  4 * SIZE(AO)
	LD	b6,  5 * SIZE(AO)
	LD	b7,  6 * SIZE(AO)
	LD	b8,  7 * SIZE(AO)

	SUB	c11, b1, c11
	SUB	c21, b2, c21
	SUB	c31, b3, c31
	SUB	c41, b4, c41
	SUB	c51, b5, c51
	SUB	c61, b6, c61
	SUB	c71, b7, c71
	SUB	c81, b8, c81
#endif

#if defined(LN) || defined(LT)
	LD	b1,  0 * SIZE(AO)

	MUL	c11, b1, c11
	MUL	c21, b1, c21
	MUL	c31, b1, c31
	MUL	c41, b1, c41
	MUL	c51, b1, c51
	MUL	c61, b1, c61
	MUL	c71, b1, c71
	MUL	c81, b1, c81
#endif

#ifdef RN
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)
	LD	b5,  4 * SIZE(BO)
	LD	b6,  5 * SIZE(BO)
	LD	b7,  6 * SIZE(BO)
	LD	b8,  7 * SIZE(BO)

	MUL	c11, b1, c11

	NMSUB	c21, c21, b2, c11
	NMSUB	c31, c31, b3, c11
	NMSUB	c41, c41, b4, c11
	NMSUB	c51, c51, b5, c11
	NMSUB	c61, c61, b6, c11
	NMSUB	c71, c71, b7, c11
	NMSUB	c81, c81, b8, c11

	LD	b2,  9 * SIZE(BO)
	LD	b3, 10 * SIZE(BO)
	LD	b4, 11 * SIZE(BO)
	LD	b5, 12 * SIZE(BO)
	LD	b6, 13 * SIZE(BO)
	LD	b7, 14 * SIZE(BO)
	LD	b8, 15 * SIZE(BO)

	MUL	c21, b2, c21

	NMSUB	c31, c31, b3, c21
	NMSUB	c41, c41, b4, c21
	NMSUB	c51, c51, b5, c21
	NMSUB	c61, c61, b6, c21
	NMSUB	c71, c71, b7, c21
	NMSUB	c81, c81, b8, c21

	LD	b3, 18 * SIZE(BO)
	LD	b4, 19 * SIZE(BO)
	LD	b5, 20 * SIZE(BO)
	LD	b6, 21 * SIZE(BO)
	LD	b7, 22 * SIZE(BO)
	LD	b8, 23 * SIZE(BO)

	MUL	c31, b3, c31

	NMSUB	c41, c41, b4, c31
	NMSUB	c51, c51, b5, c31
	NMSUB	c61, c61, b6, c31
	NMSUB	c71, c71, b7, c31
	NMSUB	c81, c81, b8, c31

	LD	b4, 27 * SIZE(BO)
	LD	b5, 28 * SIZE(BO)
	LD	b6, 29 * SIZE(BO)
	LD	b7, 30 * SIZE(BO)
	LD	b8, 31 * SIZE(BO)

	MUL	c41, b4, c41

	NMSUB	c51, c51, b5, c41
	NMSUB	c61, c61, b6, c41
	NMSUB	c71, c71, b7, c41
	NMSUB	c81, c81, b8, c41

	LD	b5, 36 * SIZE(BO)
	LD	b6, 37 * SIZE(BO)
	LD	b7, 38 * SIZE(BO)
	LD	b8, 39 * SIZE(BO)

	MUL	c51, b5, c51

	NMSUB	c61, c61, b6, c51
	NMSUB	c71, c71, b7, c51
	NMSUB	c81, c81, b8, c51

	LD	b6, 45 * SIZE(BO)
	LD	b7, 46 * SIZE(BO)
	LD	b8, 47 * SIZE(BO)

	MUL	c61, b6, c61

	NMSUB	c71, c71, b7, c61
	NMSUB	c81, c81, b8, c61

	LD	b7, 54 * SIZE(BO)
	LD	b8, 55 * SIZE(BO)

	MUL	c71, b7, c71

	NMSUB	c81, c81, b8, c71

	LD	b8, 63 * SIZE(BO)

	MUL	c81, b8, c81
#endif

#ifdef RT
	LD	b1, 63 * SIZE(BO)
	LD	b2, 62 * SIZE(BO)
	LD	b3, 61 * SIZE(BO)
	LD	b4, 60 * SIZE(BO)
	LD	b5, 59 * SIZE(BO)
	LD	b6, 58 * SIZE(BO)
	LD	b7, 57 * SIZE(BO)
	LD	b8, 56 * SIZE(BO)

	MUL	c81, b1, c81

	NMSUB	c71, c71, b2, c81
	NMSUB	c61, c61, b3, c81
	NMSUB	c51, c51, b4, c81
	NMSUB	c41, c41, b5, c81
	NMSUB	c31, c31, b6, c81
	NMSUB	c21, c21, b7, c81
	NMSUB	c11, c11, b8, c81

	LD	b2, 54 * SIZE(BO)
	LD	b3, 53 * SIZE(BO)
	LD	b4, 52 * SIZE(BO)
	LD	b5, 51 * SIZE(BO)
	LD	b6, 50 * SIZE(BO)
	LD	b7, 49 * SIZE(BO)
	LD	b8, 48 * SIZE(BO)

	MUL	c71, b2, c71

	NMSUB	c61, c61, b3, c71
	NMSUB	c51, c51, b4, c71
	NMSUB	c41, c41, b5, c71
	NMSUB	c31, c31, b6, c71
	NMSUB	c21, c21, b7, c71
	NMSUB	c11, c11, b8, c71

	LD	b3, 45 * SIZE(BO)
	LD	b4, 44 * SIZE(BO)
	LD	b5, 43 * SIZE(BO)
	LD	b6, 42 * SIZE(BO)
	LD	b7, 41 * SIZE(BO)
	LD	b8, 40 * SIZE(BO)

	MUL	c61, b3, c61

	NMSUB	c51, c51, b4, c61
	NMSUB	c41, c41, b5, c61
	NMSUB	c31, c31, b6, c61
	NMSUB	c21, c21, b7, c61
	NMSUB	c11, c11, b8, c61

	LD	b4, 36 * SIZE(BO)
	LD	b5, 35 * SIZE(BO)
	LD	b6, 34 * SIZE(BO)
	LD	b7, 33 * SIZE(BO)
	LD	b8, 32 * SIZE(BO)

	MUL	c51, b4, c51

	NMSUB	c41, c41, b5, c51
	NMSUB	c31, c31, b6, c51
	NMSUB	c21, c21, b7, c51
	NMSUB	c11, c11, b8, c51

	LD	b5, 27 * SIZE(BO)
	LD	b6, 26 * SIZE(BO)
	LD	b7, 25 * SIZE(BO)
	LD	b8, 24 * SIZE(BO)

	MUL	c41, b5, c41

	NMSUB	c31, c31, b6, c41
	NMSUB	c21, c21, b7, c41
	NMSUB	c11, c11, b8, c41

	LD	b6, 18 * SIZE(BO)
	LD	b7, 17 * SIZE(BO)
	LD	b8, 16 * SIZE(BO)

	MUL	c31, b6, c31

	NMSUB	c21, c21, b7, c31
	NMSUB	c11, c11, b8, c31

	LD	b7,  9 * SIZE(BO)
	LD	b8,  8 * SIZE(BO)

	MUL	c21, b7, c21

	NMSUB	c11, c11, b8, c21

	LD	b8,  0 * SIZE(BO)

	MUL	c11, b8, c11
#endif

#ifdef LN
	daddiu	CO1, CO1, -1 * SIZE
	daddiu	CO2, CO2, -1 * SIZE
	daddiu	CO3, CO3, -1 * SIZE
	daddiu	CO4, CO4, -1 * SIZE
	daddiu	CO5, CO5, -1 * SIZE
	daddiu	CO6, CO6, -1 * SIZE
	daddiu	CO7, CO7, -1 * SIZE
	daddiu	CO8, CO8, -1 * SIZE
#endif

#if defined(LN) || defined(LT)
	ST	c11,  0 * SIZE(BO)
	ST	c21,  1 * SIZE(BO)
	ST	c31,  2 * SIZE(BO)
	ST	c41,  3 * SIZE(BO)
	ST	c51,  4 * SIZE(BO)
	ST	c61,  5 * SIZE(BO)
	ST	c71,  6 * SIZE(BO)
	ST	c81,  7 * SIZE(BO)
#else
	ST	c11,  0 * SIZE(AO)
	ST	c21,  1 * SIZE(AO)
	ST	c31,  2 * SIZE(AO)
	ST	c41,  3 * SIZE(AO)
	ST	c51,  4 * SIZE(AO)
	ST	c61,  5 * SIZE(AO)
	ST	c71,  6 * SIZE(AO)
	ST	c81,  7 * SIZE(AO)
#endif

	ST	c11,  0 * SIZE(CO1)
	ST	c21,  0 * SIZE(CO2)
	ST	c31,  0 * SIZE(CO3)
	ST	c41,  0 * SIZE(CO4)
	ST	c51,  0 * SIZE(CO5)
	ST	c61,  0 * SIZE(CO6)
	ST	c71,  0 * SIZE(CO7)
	ST	c81,  0 * SIZE(CO8)

#ifndef LN
	daddiu	CO1, CO1, 1 * SIZE
	daddiu	CO2, CO2, 1 * SIZE
	daddiu	CO3, CO3, 1 * SIZE
	daddiu	CO4, CO4, 1 * SIZE
	daddiu	CO5, CO5, 1 * SIZE
	daddiu	CO6, CO6, 1 * SIZE
	daddiu	CO7, CO7, 1 * SIZE
	daddiu	CO8, CO8, 1 * SIZE
#endif

#ifdef RT
	dsll	TEMP, K, BASE_SHIFT
	daddu	AORIG, AORIG, TEMP
#endif

#if defined(LT) || defined(RN)
	dsubu	TEMP, K, KK
	dsll	L,    TEMP, 0 + BASE_SHIFT
	dsll	TEMP, TEMP, 3 + BASE_SHIFT
	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LT
	daddiu	KK, KK, 1
#endif

#ifdef LN
	daddiu	KK, KK, -1
#endif
	.align 3

.L29:
#ifdef LN
	dsll	TEMP, K, 3 + BASE_SHIFT
	daddu	B, B, TEMP
#endif

#if defined(LT) || defined(RN)
	move	B,  BO
#endif

#ifdef RN
	daddiu	KK, KK,  8
#endif

#ifdef RT
	daddiu	KK, KK, -8
#endif

	bgtz	J, .L10
	NOP
	.align 3



.L999:
	LDARG	$16,   0($sp)
	LDARG	$17,   8($sp)
	LDARG	$18,  16($sp)
	LDARG	$19,  24($sp)
	LDARG	$20,  32($sp)
	LDARG	$21,  40($sp)
	ldc1	$f24, 48($sp)
	ldc1	$f25, 56($sp)
	ldc1	$f26, 64($sp)
	ldc1	$f27, 72($sp)
	ldc1	$f28, 80($sp)

	LDARG	$22,  88($sp)
	LDARG	$23,  96($sp)
	LDARG	$24, 104($sp)
	LDARG	$25, 112($sp)

#ifndef __64BIT__
	ldc1	$f20,112($sp)
	ldc1	$f21,120($sp)
	ldc1	$f22,128($sp)
	ldc1	$f23,136($sp)
#endif

	j	$31
	daddiu	$sp, $sp, 144

	EPILOGUE
