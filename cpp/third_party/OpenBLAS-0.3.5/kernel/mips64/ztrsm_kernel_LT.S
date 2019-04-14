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
#define A	$9
#define B	$10
#define C	$11
#define LDC	$8

#define AO	$12
#define BO	$13

#define I	$2
#define J	$3
#define L	$7

#define CO1	$14
#define CO2	$15
#define CO3	$16
#define CO4	$17

#define OFFSET	$18
#define KK	$19
#define TEMP	$20
#define AORIG	$21

#define a1	$f0
#define a2	$f1
#define a3	$f26
#define a4	$f27

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
#define c32	$f15
#define c41	$f16
#define c42	$f17
#define c51	$f18
#define c52	$f19
#define c61	$f20
#define c62	$f21
#define c71	$f22
#define c72	$f23
#define c81	$f24
#define c82	$f25

#ifndef CONJ
#define MADD1	  MADD
#define MADD2	  MADD
#define MADD3	  MADD
#define MADD4	  NMSUB
#define MADD5	  MSUB
#define MADD6	  MADD
#define MADD7	  NMSUB
#define MADD8	  MADD
#else
#if defined(LN) || defined(LT)
#define MADD1	  MADD
#define MADD2	  NMSUB
#define MADD3	  MADD
#define MADD4	  MADD
#else
#define MADD1	  MADD
#define MADD2	  MADD
#define MADD3	  NMSUB
#define MADD4	  MADD
#endif
#define MADD5	  MADD
#define MADD6	  MSUB
#define MADD7	  MADD
#define MADD8	  NMSUB
#endif

	PROLOGUE

	daddiu	$sp, $sp, -128

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

#ifndef __64BIT__
	sdc1	$f20, 88($sp)
	sdc1	$f21, 96($sp)
	sdc1	$f22,104($sp)
	sdc1	$f23,112($sp)
#endif

	LDARG	LDC,    128 + 0($sp)
	LDARG	OFFSET, 128 + 8($sp)

	dsll	LDC, LDC, ZBASE_SHIFT

#ifdef LN
	mult	M, K
	mflo	TEMP

	dsll	TEMP, TEMP, ZBASE_SHIFT
	daddu	A, A, TEMP

	dsll	TEMP, M, ZBASE_SHIFT
	daddu	C, C, TEMP
#endif

#ifdef RN
	neg	KK, OFFSET
#endif

#ifdef RT
	mult	N, K
	mflo	TEMP

	dsll	TEMP, TEMP, ZBASE_SHIFT
	daddu	B, B, TEMP

	mult	N, LDC
	mflo	TEMP
	daddu	C, C, TEMP

	dsubu	KK, N, OFFSET
#endif

	dsra	J,  N, 2
	blez	J, .L20
	nop

.L10:
#ifdef RT
	dsll	TEMP, K, 2 + ZBASE_SHIFT
	dsubu	B, B, TEMP

	dsll	TEMP, LDC, 2
	dsubu	C, C, TEMP
#endif

	move	CO1, C
	MTC	$0,  c11
	daddu	CO2, C,   LDC
	daddu	CO3, CO2, LDC
	daddiu	J, J, -1
	daddu	CO4, CO3, LDC
	MOV	c21, c11
	MOV	c31, c11
	MOV	c41, c11
	MOV	c51, c11
	move	I,  M

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

	blez	I, .L19
	MOV	c61, c11
	.align 3

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
	dsll	TEMP,   K,  ZBASE_SHIFT
	dsubu	AORIG, AORIG, TEMP
#endif

	dsll	L,    KK, ZBASE_SHIFT
	dsll	TEMP, KK, 2 + ZBASE_SHIFT

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

	dsra	L,  TEMP, 2
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

	blez	L, .L15
	NOP
#endif

	MADD1	c11, c11, a1, b1
	LD	a2,  1 * SIZE(AO)
	MADD3	c21, c21, a1, b2
	daddiu	L, L, -1
	MADD1	c31, c31, a1, b3
	NOP
	blez	L, .L13
	MADD3	c41, c41, a1, b4
	.align	3

.L12:
	MADD2	c12, c12, a2, b1
	LD	b1, 16 * SIZE(BO)
	MADD4	c22, c22, a2, b2
	LD	b2,  5 * SIZE(BO)
	MADD2	c32, c32, a2, b3
	LD	b3,  6 * SIZE(BO)
	MADD4	c42, c42, a2, b4
	LD	b4,  7 * SIZE(BO)

	MADD1	c51, c51, a1, b5
	NOP
	MADD3	c61, c61, a1, b2
	LD	a4,  2 * SIZE(AO)
	MADD1	c71, c71, a1, b3
	NOP
	MADD3	c81, c81, a1, b4
	LD	a1,  8 * SIZE(AO)

	MADD2	c52, c52, a2, b5
	LD	b5, 20 * SIZE(BO)
	MADD4	c62, c62, a2, b2
	LD	b2,  9 * SIZE(BO)
	MADD2	c72, c72, a2, b3
	LD	b3, 10 * SIZE(BO)
	MADD4	c82, c82, a2, b4
	LD	b4, 11 * SIZE(BO)

	MADD1	c11, c11, a4, b6
	LD	a2,  3 * SIZE(AO)
	MADD3	c21, c21, a4, b2
	NOP
	MADD1	c31, c31, a4, b3
	NOP
	MADD3	c41, c41, a4, b4
	NOP

	MADD2	c12, c12, a2, b6
	LD	b6, 24 * SIZE(BO)
	MADD4	c22, c22, a2, b2
	LD	b2, 13 * SIZE(BO)
	MADD2	c32, c32, a2, b3
	LD	b3, 14 * SIZE(BO)
	MADD4	c42, c42, a2, b4
	LD	b4, 15 * SIZE(BO)

	MADD1	c51, c51, a4, b7
	NOP
	MADD3	c61, c61, a4, b2
	NOP
	MADD1	c71, c71, a4, b3
	NOP
	MADD3	c81, c81, a4, b4
	NOP

	MADD2	c52, c52, a2, b7
	LD	b7, 28 * SIZE(BO)
	MADD4	c62, c62, a2, b2
	LD	b2, 17 * SIZE(BO)
	MADD2	c72, c72, a2, b3
	LD	b3, 18 * SIZE(BO)
	MADD4	c82, c82, a2, b4
	LD	b4, 19 * SIZE(BO)

	MADD1	c11, c11, a3, b1
	LD	a2,  5 * SIZE(AO)
	MADD3	c21, c21, a3, b2
	NOP
	MADD1	c31, c31, a3, b3
	NOP
	MADD3	c41, c41, a3, b4
	NOP

	MADD2	c12, c12, a2, b1
	LD	b1, 32 * SIZE(BO)
	MADD4	c22, c22, a2, b2
	LD	b2, 21 * SIZE(BO)
	MADD2	c32, c32, a2, b3
	LD	b3, 22 * SIZE(BO)
	MADD4	c42, c42, a2, b4
	LD	b4, 23 * SIZE(BO)

	MADD1	c51, c51, a3, b5
	NOP
	MADD3	c61, c61, a3, b2
	LD	a4,  6 * SIZE(AO)
	MADD1	c71, c71, a3, b3
	NOP
	MADD3	c81, c81, a3, b4
	LD	a3, 12 * SIZE(AO)

	MADD2	c52, c52, a2, b5
	LD	b5, 36 * SIZE(BO)
	MADD4	c62, c62, a2, b2
	LD	b2, 25 * SIZE(BO)
	MADD2	c72, c72, a2, b3
	LD	b3, 26 * SIZE(BO)
	MADD4	c82, c82, a2, b4
	LD	b4, 27 * SIZE(BO)

	MADD1	c11, c11, a4, b6
	LD	a2,  7 * SIZE(AO)
	MADD3	c21, c21, a4, b2
	NOP
	MADD1	c31, c31, a4, b3
	NOP
	MADD3	c41, c41, a4, b4
	daddiu	L, L, -1

	MADD2	c12, c12, a2, b6
	LD	b6, 40 * SIZE(BO)
	MADD4	c22, c22, a2, b2
	LD	b2, 29 * SIZE(BO)
	MADD2	c32, c32, a2, b3
	LD	b3, 30 * SIZE(BO)
	MADD4	c42, c42, a2, b4
	LD	b4, 31 * SIZE(BO)

	MADD1	c51, c51, a4, b7
	daddiu	BO, BO, 32 * SIZE
	MADD3	c61, c61, a4, b2
	daddiu	AO, AO,  8 * SIZE
	MADD1	c71, c71, a4, b3
	NOP
	MADD3	c81, c81, a4, b4
	NOP

	MADD2	c52, c52, a2, b7
	LD	b7, 12 * SIZE(BO)
	MADD4	c62, c62, a2, b2
	LD	b2,  1 * SIZE(BO)
	MADD2	c72, c72, a2, b3
	LD	b3,  2 * SIZE(BO)
	MADD4	c82, c82, a2, b4
	LD	b4,  3 * SIZE(BO)

	MADD1	c11, c11, a1, b1
	LD	a2,  1 * SIZE(AO)
	MADD3	c21, c21, a1, b2
	NOP
	MADD1	c31, c31, a1, b3
	NOP
	bgtz	L, .L12
	MADD3	c41, c41, a1, b4
	.align 3

.L13:
	MADD2	c12, c12, a2, b1
	LD	b1, 16 * SIZE(BO)
	MADD4	c22, c22, a2, b2
	LD	b2,  5 * SIZE(BO)
	MADD2	c32, c32, a2, b3
	LD	b3,  6 * SIZE(BO)
	MADD4	c42, c42, a2, b4
	LD	b4,  7 * SIZE(BO)

	MADD1	c51, c51, a1, b5
	NOP
	MADD3	c61, c61, a1, b2
	LD	a4,  2 * SIZE(AO)
	MADD1	c71, c71, a1, b3
	NOP
	MADD3	c81, c81, a1, b4
	LD	a1,  8 * SIZE(AO)

	MADD2	c52, c52, a2, b5
	LD	b5, 20 * SIZE(BO)
	MADD4	c62, c62, a2, b2
	LD	b2,  9 * SIZE(BO)
	MADD2	c72, c72, a2, b3
	LD	b3, 10 * SIZE(BO)
	MADD4	c82, c82, a2, b4
	LD	b4, 11 * SIZE(BO)

	MADD1	c11, c11, a4, b6
	LD	a2,  3 * SIZE(AO)
	MADD3	c21, c21, a4, b2
	NOP
	MADD1	c31, c31, a4, b3
	NOP
	MADD3	c41, c41, a4, b4
	NOP

	MADD2	c12, c12, a2, b6
	LD	b6, 24 * SIZE(BO)
	MADD4	c22, c22, a2, b2
	LD	b2, 13 * SIZE(BO)
	MADD2	c32, c32, a2, b3
	LD	b3, 14 * SIZE(BO)
	MADD4	c42, c42, a2, b4
	LD	b4, 15 * SIZE(BO)

	MADD1	c51, c51, a4, b7
	NOP
	MADD3	c61, c61, a4, b2
	NOP
	MADD1	c71, c71, a4, b3
	NOP
	MADD3	c81, c81, a4, b4
	NOP

	MADD2	c52, c52, a2, b7
	LD	b7, 28 * SIZE(BO)
	MADD4	c62, c62, a2, b2
	LD	b2, 17 * SIZE(BO)
	MADD2	c72, c72, a2, b3
	LD	b3, 18 * SIZE(BO)
	MADD4	c82, c82, a2, b4
	LD	b4, 19 * SIZE(BO)

	MADD1	c11, c11, a3, b1
	LD	a2,  5 * SIZE(AO)
	MADD3	c21, c21, a3, b2
	NOP
	MADD1	c31, c31, a3, b3
	NOP
	MADD3	c41, c41, a3, b4
	NOP

	MADD2	c12, c12, a2, b1
	LD	b1, 32 * SIZE(BO)
	MADD4	c22, c22, a2, b2
	LD	b2, 21 * SIZE(BO)
	MADD2	c32, c32, a2, b3
	LD	b3, 22 * SIZE(BO)
	MADD4	c42, c42, a2, b4
	LD	b4, 23 * SIZE(BO)

	MADD1	c51, c51, a3, b5
	NOP
	MADD3	c61, c61, a3, b2
	LD	a4,  6 * SIZE(AO)
	MADD1	c71, c71, a3, b3
	NOP
	MADD3	c81, c81, a3, b4
	LD	a3, 12 * SIZE(AO)

	MADD2	c52, c52, a2, b5
	LD	b5, 36 * SIZE(BO)
	MADD4	c62, c62, a2, b2
	LD	b2, 25 * SIZE(BO)
	MADD2	c72, c72, a2, b3
	LD	b3, 26 * SIZE(BO)
	MADD4	c82, c82, a2, b4
	LD	b4, 27 * SIZE(BO)

	MADD1	c11, c11, a4, b6
	LD	a2,  7 * SIZE(AO)
	MADD3	c21, c21, a4, b2
	NOP
	MADD1	c31, c31, a4, b3
	NOP
	MADD3	c41, c41, a4, b4
	NOP

	MADD2	c12, c12, a2, b6
	LD	b6, 40 * SIZE(BO)
	MADD4	c22, c22, a2, b2
	LD	b2, 29 * SIZE(BO)
	MADD2	c32, c32, a2, b3
	LD	b3, 30 * SIZE(BO)
	MADD4	c42, c42, a2, b4
	LD	b4, 31 * SIZE(BO)

	MADD1	c51, c51, a4, b7
	daddiu	BO, BO, 32 * SIZE
	MADD3	c61, c61, a4, b2
	daddiu	AO, AO,  8 * SIZE
	MADD1	c71, c71, a4, b3
	NOP
	MADD3	c81, c81, a4, b4
	NOP

	MADD2	c52, c52, a2, b7
	LD	b7, 12 * SIZE(BO)
	MADD4	c62, c62, a2, b2
	LD	b2,  1 * SIZE(BO)
	MADD2	c72, c72, a2, b3
	LD	b3,  2 * SIZE(BO)
	MADD4	c82, c82, a2, b4
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
	MADD1	c11, c11, a1, b1
	LD	a2,  1 * SIZE(AO)
	MADD3	c21, c21, a1, b2
	NOP
	MADD1	c31, c31, a1, b3
	NOP
	MADD3	c41, c41, a1, b4
	NOP

	MADD2	c12, c12, a2, b1
	LD	b1,  8 * SIZE(BO)
	MADD4	c22, c22, a2, b2
	LD	b2,  5 * SIZE(BO)
	MADD2	c32, c32, a2, b3
	LD	b3,  6 * SIZE(BO)
	MADD4	c42, c42, a2, b4
	LD	b4,  7 * SIZE(BO)

	MADD1	c51, c51, a1, b5
	daddiu	L, L, -1
	MADD3	c61, c61, a1, b2
	daddiu	AO, AO,  2 * SIZE
	MADD1	c71, c71, a1, b3
	daddiu	BO, BO,  8 * SIZE
	MADD3	c81, c81, a1, b4
	LD	a1,  0 * SIZE(AO)

	MADD2	c52, c52, a2, b5
	LD	b5,  4 * SIZE(BO)
	MADD4	c62, c62, a2, b2
	LD	b2,  1 * SIZE(BO)
	MADD2	c72, c72, a2, b3
	LD	b3,  2 * SIZE(BO)
	MADD4	c82, c82, a2, b4
	bgtz	L, .L16
	LD	b4,  3 * SIZE(BO)

.L18:
 	ADD	c11, c11, c22
	ADD	c12, c12, c21
	ADD	c31, c31, c42
	ADD	c32, c32, c41

	ADD	c51, c51, c62
	ADD	c52, c52, c61
	ADD	c71, c71, c82
	ADD	c72, c72, c81

#if defined(LN) || defined(RT)
#ifdef LN
	daddiu	TEMP, KK, -1
#else
	daddiu	TEMP, KK, -4
#endif

	dsll	L,    TEMP, ZBASE_SHIFT
	dsll	TEMP, TEMP, 2 + ZBASE_SHIFT
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
	SUB	c12, b2, c12
	SUB	c31, b3, c31
	SUB	c32, b4, c32
	SUB	c51, b5, c51
	SUB	c52, b6, c52
 	SUB	c71, b7, c71
	SUB	c72, b8, c72

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
 	SUB	c31, b3, c31
	SUB	c32, b4, c32
	SUB	c51, b5, c51
	SUB	c52, b6, c52
	SUB	c71, b7, c71
	SUB	c72, b8, c72
#endif

#if defined(LN) || defined(LT)
	LD	b1,  0 * SIZE(AO)
	LD	b2,  1 * SIZE(AO)

	MUL	a1, b2, c12
	MUL	a2, b2, c11
	MUL	a3, b2, c32
	MUL	a4, b2, c31

	MADD5	c11, a1, b1, c11
	MADD6	c12, a2, b1, c12
	MADD5	c31, a3, b1, c31
	MADD6	c32, a4, b1, c32

	MUL	a1, b2, c52
	MUL	a2, b2, c51
	MUL	a3, b2, c72
	MUL	a4, b2, c71

	MADD5	c51, a1, b1, c51
	MADD6	c52, a2, b1, c52
	MADD5	c71, a3, b1, c71
	MADD6	c72, a4, b1, c72
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

	MUL	a1, b2, c12
	MUL	a2, b2, c11

	MADD5	c11, a1, b1, c11
	MADD6	c12, a2, b1, c12

	NMSUB	c31, c31, b3, c11
	MADD7	c32, c32, b4, c11
	NMSUB	c51, c51, b5, c11
	MADD7	c52, c52, b6, c11
	NMSUB	c71, c71, b7, c11
	MADD7	c72, c72, b8, c11

	MADD8	c31, c31, b4, c12
	NMSUB	c32, c32, b3, c12
	MADD8	c51, c51, b6, c12
	NMSUB	c52, c52, b5, c12
	MADD8	c71, c71, b8, c12
	NMSUB	c72, c72, b7, c12

	LD	b3, 10 * SIZE(BO)
	LD	b4, 11 * SIZE(BO)
	LD	b5, 12 * SIZE(BO)
	LD	b6, 13 * SIZE(BO)
	LD	b7, 14 * SIZE(BO)
	LD	b8, 15 * SIZE(BO)

	MUL	a1, b4, c32
	MUL	a2, b4, c31

	MADD5	c31, a1, b3, c31
	MADD6	c32, a2, b3, c32

	NMSUB	c51, c51, b5, c31
	MADD7	c52, c52, b6, c31
	NMSUB	c71, c71, b7, c31
	MADD7	c72, c72, b8, c31

	MADD8	c51, c51, b6, c32
	NMSUB	c52, c52, b5, c32
	MADD8	c71, c71, b8, c32
	NMSUB	c72, c72, b7, c32

	LD	b5, 20 * SIZE(BO)
	LD	b6, 21 * SIZE(BO)
	LD	b7, 22 * SIZE(BO)
	LD	b8, 23 * SIZE(BO)

	MUL	a1, b6, c52
	MUL	a2, b6, c51

	MADD5	c51, a1, b5, c51
	MADD6	c52, a2, b5, c52

	NMSUB	c71, c71, b7, c51
	MADD7	c72, c72, b8, c51

	MADD8	c71, c71, b8, c52
	NMSUB	c72, c72, b7, c52

	LD	b7, 30 * SIZE(BO)
	LD	b8, 31 * SIZE(BO)

	MUL	a1, b8, c72
	MUL	a2, b8, c71

	MADD5	c71, a1, b7, c71
	MADD6	c72, a2, b7, c72
#endif

#ifdef RT
	LD	b1, 30 * SIZE(BO)
	LD	b2, 31 * SIZE(BO)
	LD	b3, 28 * SIZE(BO)
	LD	b4, 29 * SIZE(BO)
	LD	b5, 26 * SIZE(BO)
	LD	b6, 27 * SIZE(BO)
	LD	b7, 24 * SIZE(BO)
	LD	b8, 25 * SIZE(BO)

	MUL	a1, b2, c72
	MUL	a2, b2, c71

	MADD5	c71, a1, b1, c71
	MADD6	c72, a2, b1, c72

	NMSUB	c51, c51, b3, c71
	MADD7	c52, c52, b4, c71
	NMSUB	c31, c31, b5, c71
	MADD7	c32, c32, b6, c71
	NMSUB	c11, c11, b7, c71
	MADD7	c12, c12, b8, c71

	MADD8	c51, c51, b4, c72
	NMSUB	c52, c52, b3, c72
	MADD8	c31, c31, b6, c72
	NMSUB	c32, c32, b5, c72
	MADD8	c11, c11, b8, c72
	NMSUB	c12, c12, b7, c72

	LD	b3, 20 * SIZE(BO)
	LD	b4, 21 * SIZE(BO)
	LD	b5, 18 * SIZE(BO)
	LD	b6, 19 * SIZE(BO)
	LD	b7, 16 * SIZE(BO)
	LD	b8, 17 * SIZE(BO)

	MUL	a1, b4, c52
	MUL	a2, b4, c51

	MADD5	c51, a1, b3, c51
	MADD6	c52, a2, b3, c52

	NMSUB	c31, c31, b5, c51
	MADD7	c32, c32, b6, c51
	NMSUB	c11, c11, b7, c51
	MADD7	c12, c12, b8, c51

	MADD8	c31, c31, b6, c52
	NMSUB	c32, c32, b5, c52
	MADD8	c11, c11, b8, c52
	NMSUB	c12, c12, b7, c52

	LD	b5, 10 * SIZE(BO)
	LD	b6, 11 * SIZE(BO)
	LD	b7,  8 * SIZE(BO)
	LD	b8,  9 * SIZE(BO)

	MUL	a1, b6, c32
	MUL	a2, b6, c31

	MADD5	c31, a1, b5, c31
	MADD6	c32, a2, b5, c32

	NMSUB	c11, c11, b7, c31
	MADD7	c12, c12, b8, c31

	MADD8	c11, c11, b8, c32
	NMSUB	c12, c12, b7, c32

	LD	b7,  0 * SIZE(BO)
	LD	b8,  1 * SIZE(BO)

	MUL	a1, b8, c12
	MUL	a2, b8, c11

	MADD5	c11, a1, b7, c11
	MADD6	c12, a2, b7, c12
#endif

#if defined(LN) || defined(LT)
	ST	c11,  0 * SIZE(BO)
	ST	c12,  1 * SIZE(BO)
	ST	c31,  2 * SIZE(BO)
	ST	c32,  3 * SIZE(BO)
	ST	c51,  4 * SIZE(BO)
	ST	c52,  5 * SIZE(BO)
	ST	c71,  6 * SIZE(BO)
	ST	c72,  7 * SIZE(BO)
#else
	ST	c11,  0 * SIZE(AO)
	ST	c12,  1 * SIZE(AO)
	ST	c31,  2 * SIZE(AO)
	ST	c32,  3 * SIZE(AO)
	ST	c51,  4 * SIZE(AO)
	ST	c52,  5 * SIZE(AO)
	ST	c71,  6 * SIZE(AO)
	ST	c72,  7 * SIZE(AO)
#endif

#ifdef LN
	daddiu	CO1,CO1, -2 * SIZE
	daddiu	CO2,CO2, -2 * SIZE
	daddiu	CO3,CO3, -2 * SIZE
	daddiu	CO4,CO4, -2 * SIZE
#endif

	ST	c11,  0 * SIZE(CO1)
	ST	c12,  1 * SIZE(CO1)
	ST	c31,  0 * SIZE(CO2)
	ST	c32,  1 * SIZE(CO2)
	ST	c51,  0 * SIZE(CO3)
	ST	c52,  1 * SIZE(CO3)
	ST	c71,  0 * SIZE(CO4)
	ST	c72,  1 * SIZE(CO4)

#ifndef LN
	daddiu	CO1,CO1, 2 * SIZE
	daddiu	CO2,CO2, 2 * SIZE
	daddiu	CO3,CO3, 2 * SIZE
	daddiu	CO4,CO4, 2 * SIZE
#endif


#ifdef RT
	dsll	TEMP, K, ZBASE_SHIFT
	daddu	AORIG, AORIG, TEMP
#endif

#if defined(LT) || defined(RN)
	dsubu	TEMP, K, KK
	dsll	L,    TEMP, ZBASE_SHIFT
	dsll	TEMP, TEMP, 2 + ZBASE_SHIFT
	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LT
	daddiu	KK, KK, 1
#endif

#ifdef LN
	daddiu	KK, KK, -1
#endif

	MTC	$0,  c11

	daddiu	I, I, -1


	MOV	c21, c11
	MOV	c31, c11
	MOV	c41, c11
	MOV	c51, c11

	bgtz	I, .L11
	MOV	c61, c11
	.align 3

.L19:
#ifdef LN
	dsll	TEMP, K, 2 + ZBASE_SHIFT
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

	bgtz	J, .L10
	NOP
	.align 3

.L20:
	andi	J,  N, 2
	blez	J, .L30
	NOP

#ifdef RT
	dsll	TEMP, K, 1 + ZBASE_SHIFT
	dsubu	B, B, TEMP

	dsll	TEMP, LDC, 1
	dsubu	C, C, TEMP
#endif

	MTC	$0,  c11

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

	move	I,  M
	blez	I, .L29
	NOP
	.align 3

.L21:
#if defined(LT) || defined(RN)
	LD	a1,  0 * SIZE(AO)
	MOV	c21, c11
	LD	b1,  0 * SIZE(B)
	MOV	c31, c11
	LD	a3,  4 * SIZE(AO)
	MOV	c41, c11
	LD	b2,  1 * SIZE(B)
	dsra	L,  KK, 2

	LD	b3,  2 * SIZE(B)
	MOV	c12, c11
	LD	b4,  3 * SIZE(B)
	MOV	c22, c11
	LD	b5,  4 * SIZE(B)
	MOV	c32, c11

	NOP
	MOV	c42, c11
	blez	L, .L25
	move	BO,  B
#else
#ifdef LN
	dsll	TEMP,   K,  ZBASE_SHIFT
	dsubu	AORIG, AORIG, TEMP
#endif

	dsll	L,    KK, ZBASE_SHIFT
	dsll	TEMP, KK, 1 + ZBASE_SHIFT

	daddu	AO, AORIG, L
	daddu	BO, B,     TEMP

	dsubu	TEMP, K, KK

	LD	a1,  0 * SIZE(AO)
	MOV	c21, c11
	LD	b1,  0 * SIZE(BO)
	MOV	c31, c11
	LD	a3,  4 * SIZE(AO)
	MOV	c41, c11
	LD	b2,  1 * SIZE(BO)
	dsra	L,  TEMP, 2

	LD	b3,  2 * SIZE(BO)
	MOV	c12, c11
	LD	b4,  3 * SIZE(BO)
	MOV	c22, c11
	LD	b5,  4 * SIZE(BO)
	MOV	c32, c11

	blez	L, .L25
	MOV	c42, c11
#endif
	.align	3

.L22:
	MADD1	c11, c11, a1, b1
	LD	a2,  1 * SIZE(AO)
	MADD3	c21, c21, a1, b2
	daddiu	L, L, -1
	MADD1	c31, c31, a1, b3
	NOP
	MADD3	c41, c41, a1, b4
	LD	a1,  2 * SIZE(AO)

	MADD2	c12, c12, a2, b1
	LD	b1,  8 * SIZE(BO)
	MADD4	c22, c22, a2, b2
	LD	b2,  5 * SIZE(BO)
	MADD2	c32, c32, a2, b3
	LD	b3,  6 * SIZE(BO)
	MADD4	c42, c42, a2, b4
	LD	b4,  7 * SIZE(BO)

	MADD1	c11, c11, a1, b5
	LD	a2,  3 * SIZE(AO)
	MADD3	c21, c21, a1, b2
	NOP
	MADD1	c31, c31, a1, b3
	NOP
	MADD3	c41, c41, a1, b4
	LD	a1,  8 * SIZE(AO)

	MADD2	c12, c12, a2, b5
	LD	b5, 12 * SIZE(BO)
	MADD4	c22, c22, a2, b2
	LD	b2,  9 * SIZE(BO)
	MADD2	c32, c32, a2, b3
	LD	b3, 10 * SIZE(BO)
	MADD4	c42, c42, a2, b4
	LD	b4, 11 * SIZE(BO)

	MADD1	c11, c11, a3, b1
	LD	a2,  5 * SIZE(AO)
	MADD3	c21, c21, a3, b2
	NOP
	MADD1	c31, c31, a3, b3
	NOP
	MADD3	c41, c41, a3, b4
	LD	a3,  6 * SIZE(AO)

	MADD2	c12, c12, a2, b1
	LD	b1, 16 * SIZE(BO)
	MADD4	c22, c22, a2, b2
	LD	b2, 13 * SIZE(BO)
	MADD2	c32, c32, a2, b3
	LD	b3, 14 * SIZE(BO)
	MADD4	c42, c42, a2, b4
	LD	b4, 15 * SIZE(BO)

	MADD1	c11, c11, a3, b5
	LD	a2,  7 * SIZE(AO)
	MADD3	c21, c21, a3, b2
	daddiu	AO, AO,  8 * SIZE
	MADD1	c31, c31, a3, b3
	NOP
	MADD3	c41, c41, a3, b4
	LD	a3,  4 * SIZE(AO)

	MADD2	c12, c12, a2, b5
	LD	b5, 20 * SIZE(BO)
	MADD4	c22, c22, a2, b2
	LD	b2, 17 * SIZE(BO)
	MADD2	c32, c32, a2, b3
	LD	b3, 18 * SIZE(BO)
	MADD4	c42, c42, a2, b4
	LD	b4, 19 * SIZE(BO)

	bgtz	L, .L22
	daddiu	BO, BO, 16 * SIZE
	.align 3

.L25:
#if defined(LT) || defined(RN)
	andi	L, KK,  3
#else
	andi	L, TEMP, 3
#endif
	blez	L, .L28
	NOP
	.align	3

.L26:
	MADD1	c11, c11, a1, b1
	LD	a2,  1 * SIZE(AO)
	MADD3	c21, c21, a1, b2
	daddiu	L, L, -1
	MADD1	c31, c31, a1, b3
	daddiu	BO, BO,  4 * SIZE
	MADD3	c41, c41, a1, b4
	LD	a1,  2 * SIZE(AO)

	MADD2	c12, c12, a2, b1
	LD	b1,  0 * SIZE(BO)
	MADD4	c22, c22, a2, b2
	LD	b2,  1 * SIZE(BO)
	MADD2	c32, c32, a2, b3
	LD	b3,  2 * SIZE(BO)
	MADD4	c42, c42, a2, b4
	LD	b4,  3 * SIZE(BO)

	bgtz	L, .L26
	daddiu	AO, AO,  2 * SIZE

.L28:
 	ADD	c11, c11, c22
	ADD	c12, c12, c21
	ADD	c31, c31, c42
	ADD	c32, c32, c41

#if defined(LN) || defined(RT)
#ifdef LN
	daddiu	TEMP, KK, -1
#else
	daddiu	TEMP, KK, -2
#endif

	dsll	L,    TEMP, ZBASE_SHIFT
	dsll	TEMP, TEMP, 1 + ZBASE_SHIFT
	daddu	AO, AORIG, L
	daddu	BO, B,     TEMP
#endif

#if defined(LN) || defined(LT)
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	SUB	c11, b1, c11
	SUB	c12, b2, c12
	SUB	c31, b3, c31
	SUB	c32, b4, c32
#else
	LD	b1,  0 * SIZE(AO)
	LD	b2,  1 * SIZE(AO)
	LD	b3,  2 * SIZE(AO)
	LD	b4,  3 * SIZE(AO)

	SUB	c11, b1, c11
	SUB	c12, b2, c12
 	SUB	c31, b3, c31
	SUB	c32, b4, c32
#endif

#if defined(LN) || defined(LT)
	LD	b1,  0 * SIZE(AO)
	LD	b2,  1 * SIZE(AO)

	MUL	a1, b2, c12
	MUL	a2, b2, c11
	MUL	a3, b2, c32
	MUL	a4, b2, c31

	MADD5	c11, a1, b1, c11
	MADD6	c12, a2, b1, c12
	MADD5	c31, a3, b1, c31
	MADD6	c32, a4, b1, c32
#endif

#ifdef RN
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)

	MUL	a1, b2, c12
	MUL	a2, b2, c11

	MADD5	c11, a1, b1, c11
	MADD6	c12, a2, b1, c12

	NMSUB	c31, c31, b3, c11
	MADD7	c32, c32, b4, c11

	MADD8	c31, c31, b4, c12
	NMSUB	c32, c32, b3, c12

	LD	b3,  6 * SIZE(BO)
	LD	b4,  7 * SIZE(BO)

	MUL	a1, b4, c32
	MUL	a2, b4, c31

	MADD5	c31, a1, b3, c31
	MADD6	c32, a2, b3, c32
#endif

#ifdef RT
	LD	b5,  6 * SIZE(BO)
	LD	b6,  7 * SIZE(BO)
	LD	b7,  4 * SIZE(BO)
	LD	b8,  5 * SIZE(BO)

	MUL	a1, b6, c32
	MUL	a2, b6, c31

	MADD5	c31, a1, b5, c31
	MADD6	c32, a2, b5, c32

	NMSUB	c11, c11, b7, c31
	MADD7	c12, c12, b8, c31

	MADD8	c11, c11, b8, c32
	NMSUB	c12, c12, b7, c32

	LD	b7,  0 * SIZE(BO)
	LD	b8,  1 * SIZE(BO)

	MUL	a1, b8, c12
	MUL	a2, b8, c11

	MADD5	c11, a1, b7, c11
	MADD6	c12, a2, b7, c12
#endif

#if defined(LN) || defined(LT)
	ST	c11,  0 * SIZE(BO)
	ST	c12,  1 * SIZE(BO)
	ST	c31,  2 * SIZE(BO)
	ST	c32,  3 * SIZE(BO)
#else
	ST	c11,  0 * SIZE(AO)
	ST	c12,  1 * SIZE(AO)
	ST	c31,  2 * SIZE(AO)
	ST	c32,  3 * SIZE(AO)
#endif

#ifdef LN
	daddiu	CO1,CO1, -2 * SIZE
	daddiu	CO2,CO2, -2 * SIZE
#endif

	ST	c11,  0 * SIZE(CO1)
	ST	c12,  1 * SIZE(CO1)
	ST	c31,  0 * SIZE(CO2)
	ST	c32,  1 * SIZE(CO2)

#ifndef LN
	daddiu	CO1,CO1, 2 * SIZE
	daddiu	CO2,CO2, 2 * SIZE
#endif

	MTC	$0,  c11

#ifdef RT
	dsll	TEMP, K, ZBASE_SHIFT
	daddu	AORIG, AORIG, TEMP
#endif

#if defined(LT) || defined(RN)
	dsubu	TEMP, K, KK
	dsll	L,    TEMP, ZBASE_SHIFT
	dsll	TEMP, TEMP, 1 + ZBASE_SHIFT
	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LT
	daddiu	KK, KK, 1
#endif

#ifdef LN
	daddiu	KK, KK, -1
#endif

	daddiu	I, I, -1

	bgtz	I, .L21
	NOP
	.align 3

.L29:
#ifdef LN
	dsll	TEMP, K, 1 + ZBASE_SHIFT
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

.L30:
	andi	J,  N, 1
	blez	J, .L999
	NOP


#ifdef RT
	dsll	TEMP, K, ZBASE_SHIFT
	dsubu	B, B, TEMP

	dsubu	C, C, LDC
#endif

	MTC	$0,  c11

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

	move	I,  M
	blez	I, .L39
	NOP
	.align 3

.L31:
#if defined(LT) || defined(RN)
	LD	a1,  0 * SIZE(AO)
	MOV	c21, c11
	LD	b1,  0 * SIZE(B)
	MOV	c31, c11
	LD	a2,  1 * SIZE(AO)

	MOV	c41, c11
	LD	b2,  1 * SIZE(B)
	MOV	c12, c11
	dsra	L,  KK, 2

	MOV	c22, c11
	LD	a3,  4 * SIZE(AO)
	MOV	c32, c11
	LD	b3,  4 * SIZE(B)

	NOP
	MOV	c42, c11
	blez	L, .L35
	move	BO,  B
#else
#ifdef LN
	dsll	TEMP,   K,  ZBASE_SHIFT
	dsubu	AORIG, AORIG, TEMP
#endif
	dsll	TEMP, KK, ZBASE_SHIFT

	daddu	AO, AORIG, TEMP
	daddu	BO, B,     TEMP

	dsubu	TEMP, K, KK

	LD	a1,  0 * SIZE(AO)
	MOV	c21, c11
	LD	b1,  0 * SIZE(BO)
	MOV	c31, c11
	LD	a2,  1 * SIZE(AO)

	MOV	c41, c11
	LD	b2,  1 * SIZE(BO)
	MOV	c12, c11
	dsra	L, TEMP, 2

	MOV	c22, c11
	LD	a3,  4 * SIZE(AO)
	MOV	c32, c11
	LD	b3,  4 * SIZE(BO)

	blez	L, .L35
	MOV	c42, c11
#endif
	.align	3

.L32:
	MADD1	c11, c11, a1, b1
	LD	b4,  3 * SIZE(BO)
	MADD3	c21, c21, a1, b2
	LD	a1,  2 * SIZE(AO)
	MADD2	c12, c12, a2, b1
	LD	b1,  2 * SIZE(BO)
	MADD4	c22, c22, a2, b2
	LD	a2,  3 * SIZE(AO)

	MADD1	c11, c11, a1, b1
	LD	b2,  5 * SIZE(BO)
	MADD3	c21, c21, a1, b4
	LD	a1,  8 * SIZE(AO)
	MADD2	c12, c12, a2, b1
	LD	b1,  8 * SIZE(BO)
	MADD4	c22, c22, a2, b4
	LD	a2,  5 * SIZE(AO)

	MADD1	c11, c11, a3, b3
	LD	b4,  7 * SIZE(BO)
	MADD3	c21, c21, a3, b2
	LD	a3,  6 * SIZE(AO)
	MADD2	c12, c12, a2, b3
	LD	b3,  6 * SIZE(BO)
	MADD4	c22, c22, a2, b2
	LD	a2,  7 * SIZE(AO)

	MADD1	c11, c11, a3, b3
	LD	b2,  9 * SIZE(BO)
	MADD3	c21, c21, a3, b4
	LD	a3, 12 * SIZE(AO)
	MADD2	c12, c12, a2, b3
	LD	b3, 12 * SIZE(BO)
	MADD4	c22, c22, a2, b4
	LD	a2,  9 * SIZE(AO)

	daddiu	AO, AO,  8 * SIZE
	daddiu	L, L, -1

	bgtz	L, .L32
	daddiu	BO, BO,  8 * SIZE
	.align 3

.L35:
#if defined(LT) || defined(RN)
	andi	L, KK,  3
#else
	andi	L, TEMP, 3
#endif
	blez	L, .L38
	NOP
	.align	3

.L36:
	MADD1	c11, c11, a1, b1
	daddiu	L, L, -1
	MADD3	c21, c21, a1, b2
	LD	a1,  2 * SIZE(AO)
	MADD2	c12, c12, a2, b1
	LD	b1,  2 * SIZE(BO)
	MADD4	c22, c22, a2, b2
	LD	a2,  3 * SIZE(AO)

	LD	b2,  3 * SIZE(BO)
	daddiu	BO, BO,  2 * SIZE
	bgtz	L, .L36
	daddiu	AO, AO,  2 * SIZE

.L38:
 	ADD	c11, c11, c22
	ADD	c12, c12, c21

#if defined(LN) || defined(RT)
	daddiu	TEMP, KK, -1

	dsll	TEMP, TEMP, ZBASE_SHIFT
	daddu	AO, AORIG, TEMP
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

#if defined(LN) || defined(LT)
	LD	b1,  0 * SIZE(AO)
	LD	b2,  1 * SIZE(AO)

	MUL	a1, b2, c12
	MUL	a2, b2, c11

	MADD5	c11, a1, b1, c11
	MADD6	c12, a2, b1, c12
#endif

#if defined(RN) || defined(RT)
	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)

	MUL	a1, b2, c12
	MUL	a2, b2, c11

	MADD5	c11, a1, b1, c11
	MADD6	c12, a2, b1, c12
#endif

#if defined(LN) || defined(LT)
	ST	c11,  0 * SIZE(BO)
	ST	c12,  1 * SIZE(BO)
#else
	ST	c11,  0 * SIZE(AO)
	ST	c12,  1 * SIZE(AO)
#endif

#ifdef LN
	daddiu	CO1,CO1, -2 * SIZE
#endif

	ST	c11,  0 * SIZE(CO1)
	ST	c12,  1 * SIZE(CO1)

#ifndef LN
	daddiu	CO1,CO1, 2 * SIZE
#endif

	MTC	$0,  c11

#ifdef RT
	dsll	TEMP, K, ZBASE_SHIFT
	daddu	AORIG, AORIG, TEMP
#endif

#if defined(LT) || defined(RN)
	dsubu	TEMP, K, KK
	dsll	TEMP, TEMP, ZBASE_SHIFT
	daddu	AO, AO, TEMP
	daddu	BO, BO, TEMP
#endif

#ifdef LT
	daddiu	KK, KK, 1
#endif

#ifdef LN
	daddiu	KK, KK, -1
#endif

	daddiu	I, I, -1

	bgtz	I, .L31
	NOP
	.align 3

.L39:
#ifdef LN
	dsll	TEMP, K, ZBASE_SHIFT
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

#ifndef __64BIT__
	ldc1	$f20, 88($sp)
	ldc1	$f21, 96($sp)
	ldc1	$f22,104($sp)
	ldc1	$f23,112($sp)
#endif

	j	$31
	daddiu	$sp, $sp, 128

	EPILOGUE
