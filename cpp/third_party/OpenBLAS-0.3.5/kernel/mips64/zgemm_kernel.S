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

#if defined(TRMMKERNEL)
#define OFFSET	$18
#define KK	$19
#define TEMP	$20
#endif

#define a1	$f0
#define a2	$f1
#define a3	$f28
#define a4	$f29

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
#define c32	$f17
#define c41	$f18
#define c42	$f19
#define c51	$f20
#define c52	$f21
#define c61	$f22
#define c62	$f23
#define c71	$f24
#define c72	$f25
#define c81	$f26
#define c82	$f27

#define ALPHA_R	$f15
#define ALPHA_I	$f16

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define MADD1	  MADD
#define MADD2	  MADD
#define MADD3	  MADD
#define MADD4	  NMSUB
#endif

#if   defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define MADD1	  MADD
#define MADD2	  MADD
#define MADD3	  NMSUB
#define MADD4	  MADD
#endif

#if   defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define MADD1	  MADD
#define MADD2	  NMSUB
#define MADD3	  MADD
#define MADD4	  MADD
#endif

#if   defined(RR) || defined(RC) || defined(CR) || defined(CC)
#define MADD1	  MADD
#define MADD2	  NMSUB
#define MADD3	  NMSUB
#define MADD4	  NMSUB
#endif

	PROLOGUE

	LDARG	LDC,   0($sp)
	daddiu	$sp, $sp, -128

	SDARG	$16,   0($sp)
	SDARG	$17,   8($sp)
	sdc1	$f24, 16($sp)
	sdc1	$f25, 24($sp)
	sdc1	$f26, 32($sp)
	sdc1	$f27, 40($sp)
	sdc1	$f28, 48($sp)
	sdc1	$f29, 56($sp)

#if defined(TRMMKERNEL)
	SDARG	$18,  64($sp)
	SDARG	$19,  72($sp)
	SDARG	$20,  80($sp)

	LDARG	OFFSET, 128 + 8($sp)
#endif

#ifndef __64BIT__
	sdc1	$f20, 88($sp)
	sdc1	$f21, 96($sp)
	sdc1	$f22,104($sp)
	sdc1	$f23,112($sp)
#endif

	dsll	LDC, LDC, ZBASE_SHIFT

#if defined(TRMMKERNEL) && !defined(LEFT)
	neg	KK, OFFSET
#endif

	dsra	J,  N, 2
	blez	J, .L20
	nop

.L10:
	move	CO1, C
	MTC	$0,  c11
	daddu	CO2, C,   LDC
	move	AO, A
	daddu	CO3, CO2, LDC
	daddiu	J, J, -1
	daddu	CO4, CO3, LDC
	MOV	c21, c11
	MOV	c31, c11
#if defined(TRMMKERNEL) &&  defined(LEFT)
	move	KK, OFFSET
#endif
	MOV	c41, c11
	MOV	c51, c11
	move	I,  M
	daddu	C,   CO4, LDC

	blez	I, .L19
	MOV	c61, c11

.L11:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	L,    KK,  ZBASE_SHIFT
	dsll	TEMP, KK, 2 + ZBASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, B,  TEMP
#endif

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

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, K, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 1
#else
	daddiu	TEMP, KK, 4
#endif
	dsra	L,  TEMP, 2

	blez	L, .L15
	NOP
#else
	LD	a1,  0 * SIZE(AO)
	MOV	c71, c11
	LD	b1,  0 * SIZE(B)
	MOV	c81, c11

	LD	a3,  4 * SIZE(AO)
	MOV	c12, c11
	LD	b2,  1 * SIZE(B)
	MOV	c22, c11

	dsra	L,  K, 2
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
#ifndef TRMMKERNEL
	andi	L,  K, 3
#else
	andi	L,  TEMP, 3
#endif
	NOP
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
#ifndef TRMMKERNEL
	LD	b1,  0 * SIZE(CO1)
 	ADD	c11, c11, c22
	LD	b2,  1 * SIZE(CO1)
	ADD	c12, c12, c21
	LD	b3,  0 * SIZE(CO2)
	ADD	c31, c31, c42
	LD	b4,  1 * SIZE(CO2)
	ADD	c32, c32, c41

	LD	b5,  0 * SIZE(CO3)
	ADD	c51, c51, c62
	LD	b6,  1 * SIZE(CO3)
	ADD	c52, c52, c61
	LD	b7,  0 * SIZE(CO4)
	ADD	c71, c71, c82
	LD	b8,  1 * SIZE(CO4)
	ADD	c72, c72, c81

	MADD	b1, b1, ALPHA_R, c11
	daddiu	CO1,CO1, 2 * SIZE
	MADD	b2, b2, ALPHA_R, c12
	daddiu	CO2,CO2, 2 * SIZE
	MADD	b3, b3, ALPHA_R, c31
	daddiu	CO3,CO3, 2 * SIZE
	MADD	b4, b4, ALPHA_R, c32
	daddiu	CO4,CO4, 2 * SIZE

	MADD	b5, b5, ALPHA_R, c51
	daddiu	I, I, -1
	MADD	b6, b6, ALPHA_R, c52
	NOP
	MADD	b7, b7, ALPHA_R, c71
	NOP
	MADD	b8, b8, ALPHA_R, c72
	NOP

	NMSUB	b1, b1, ALPHA_I, c12
	NOP
	MADD	b2, b2, ALPHA_I, c11
	MTC	$0,  c11
	NMSUB	b3, b3, ALPHA_I, c32
	NOP
	MADD	b4, b4, ALPHA_I, c31
	NOP

	ST	b1, -2 * SIZE(CO1)
	NMSUB	b5, b5, ALPHA_I, c52
	ST	b2, -1 * SIZE(CO1)
	MADD	b6, b6, ALPHA_I, c51
	ST	b3, -2 * SIZE(CO2)
	NMSUB	b7, b7, ALPHA_I, c72
	ST	b4, -1 * SIZE(CO2)
	MADD	b8, b8, ALPHA_I, c71

	ST	b5, -2 * SIZE(CO3)
	MOV	c21, c11
	ST	b6, -1 * SIZE(CO3)
	MOV	c31, c11
	ST	b7, -2 * SIZE(CO4)
	MOV	c41, c11
	ST	b8, -1 * SIZE(CO4)
	MOV	c51, c11

#else

 	ADD	c11, c11, c22
	daddiu	CO1,CO1, 2 * SIZE
	ADD	c12, c12, c21
	daddiu	CO2,CO2, 2 * SIZE
	ADD	c31, c31, c42
	daddiu	CO3,CO3, 2 * SIZE
	ADD	c32, c32, c41
	daddiu	CO4,CO4, 2 * SIZE

	ADD	c51, c51, c62
	daddiu	I, I, -1
	ADD	c52, c52, c61
	ADD	c71, c71, c82
	ADD	c72, c72, c81

	MUL	b1, ALPHA_R, c11
	MUL	b2, ALPHA_R, c12
	MUL	b3, ALPHA_R, c31
	MUL	b4, ALPHA_R, c32

	MUL	b5, ALPHA_R, c51
	MUL	b6, ALPHA_R, c52
	MUL	b7, ALPHA_R, c71
	MUL	b8, ALPHA_R, c72

	NMSUB	b1, b1, ALPHA_I, c12
	NOP
	MADD	b2, b2, ALPHA_I, c11
	MTC	$0,  c11
	NMSUB	b3, b3, ALPHA_I, c32
	NOP
	MADD	b4, b4, ALPHA_I, c31
	NOP

	ST	b1, -2 * SIZE(CO1)
	NMSUB	b5, b5, ALPHA_I, c52
	ST	b2, -1 * SIZE(CO1)
	MADD	b6, b6, ALPHA_I, c51
	ST	b3, -2 * SIZE(CO2)
	NMSUB	b7, b7, ALPHA_I, c72
	ST	b4, -1 * SIZE(CO2)
	MADD	b8, b8, ALPHA_I, c71

	ST	b5, -2 * SIZE(CO3)
	MOV	c21, c11
	ST	b6, -1 * SIZE(CO3)
	MOV	c31, c11
	ST	b7, -2 * SIZE(CO4)
	MOV	c41, c11
	ST	b8, -1 * SIZE(CO4)
	MOV	c51, c11

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, K, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -1
#else
	daddiu	TEMP, TEMP, -4
#endif

	dsll	L,    TEMP, ZBASE_SHIFT
	dsll	TEMP, TEMP, 2 + ZBASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 1
#endif
#endif
	bgtz	I, .L11
	MOV	c61, c11
	.align 3

.L19:
#if defined(TRMMKERNEL) && !defined(LEFT)
	daddiu	KK, KK, 4
#endif

	bgtz	J, .L10
	move	B, BO
	.align 3

.L20:
	andi	J,  N, 2
	MTC	$0,  c11
	blez	J, .L30
	move	CO1, C

	daddu	CO2, C,   LDC
	daddu	C,   CO2, LDC

#if defined(TRMMKERNEL) &&  defined(LEFT)
	move	KK, OFFSET
#endif

	move	I,  M
	blez	I, .L29
	move	AO, A
	.align 3

.L21:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	L,    KK,  ZBASE_SHIFT
	dsll	TEMP, KK, 1 + ZBASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, B,  TEMP
#endif

	LD	a1,  0 * SIZE(AO)
	MOV	c21, c11
	LD	b1,  0 * SIZE(BO)
	MOV	c31, c11
	LD	a3,  4 * SIZE(AO)
	MOV	c41, c11
	LD	b2,  1 * SIZE(BO)

	LD	b3,  2 * SIZE(BO)
	MOV	c12, c11
	LD	b4,  3 * SIZE(BO)
	MOV	c22, c11
	LD	b5,  4 * SIZE(BO)
	MOV	c32, c11

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, K, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 1
#else
	daddiu	TEMP, KK, 2
#endif
	dsra	L,  TEMP, 2
	blez	L, .L25
	MOV	c42, c11

#else
	LD	a1,  0 * SIZE(AO)
	MOV	c21, c11
	LD	b1,  0 * SIZE(B)
	MOV	c31, c11
	LD	a3,  4 * SIZE(AO)
	MOV	c41, c11
	LD	b2,  1 * SIZE(B)
	dsra	L,  K, 2

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
#ifndef TRMMKERNEL
	andi	L,  K, 3
#else
	andi	L,  TEMP, 3
#endif
	NOP
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
#ifndef TRMMKERNEL
	LD	b1,  0 * SIZE(CO1)
 	ADD	c11, c11, c22
	LD	b2,  1 * SIZE(CO1)
	ADD	c12, c12, c21
	LD	b3,  0 * SIZE(CO2)
	ADD	c31, c31, c42
	LD	b4,  1 * SIZE(CO2)
	ADD	c32, c32, c41

	MADD	b1, b1, ALPHA_R, c11
	daddiu	CO1,CO1, 2 * SIZE
	MADD	b2, b2, ALPHA_R, c12
	daddiu	CO2,CO2, 2 * SIZE
	MADD	b3, b3, ALPHA_R, c31
	daddiu	I, I, -1
	MADD	b4, b4, ALPHA_R, c32

	NMSUB	b1, b1, ALPHA_I, c12
	NOP
	MADD	b2, b2, ALPHA_I, c11
	MTC	$0,  c11
	NMSUB	b3, b3, ALPHA_I, c32
	NOP
	MADD	b4, b4, ALPHA_I, c31
	NOP

	ST	b1, -2 * SIZE(CO1)
	ST	b2, -1 * SIZE(CO1)
	ST	b3, -2 * SIZE(CO2)
#else
 	ADD	c11, c11, c22
	ADD	c12, c12, c21
	ADD	c31, c31, c42
	ADD	c32, c32, c41

	MUL	b1, ALPHA_R, c11
	daddiu	CO1,CO1, 2 * SIZE
	MUL	b2, ALPHA_R, c12
	daddiu	CO2,CO2, 2 * SIZE
	MUL	b3, ALPHA_R, c31
	daddiu	I, I, -1
	MUL	b4, ALPHA_R, c32

	NMSUB	b1, b1, ALPHA_I, c12
	NOP
	MADD	b2, b2, ALPHA_I, c11
	MTC	$0,  c11
	NMSUB	b3, b3, ALPHA_I, c32
	NOP
	MADD	b4, b4, ALPHA_I, c31
	NOP

	ST	b1, -2 * SIZE(CO1)
	ST	b2, -1 * SIZE(CO1)
	ST	b3, -2 * SIZE(CO2)

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, K, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -1
#else
	daddiu	TEMP, TEMP, -2
#endif

	dsll	L,    TEMP, ZBASE_SHIFT
	dsll	TEMP, TEMP, 1 + ZBASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 1
#endif
#endif
	bgtz	I, .L21
	ST	b4, -1 * SIZE(CO2)
	.align 3

.L29:
#if defined(TRMMKERNEL) && !defined(LEFT)
	daddiu	KK, KK, 2
#endif

	move	B, BO
	.align 3

.L30:
	andi	J,  N, 1
	MTC	$0,  c11
	blez	J, .L999
	move	CO1, C

#if defined(TRMMKERNEL) &&  defined(LEFT)
	move	KK, OFFSET
#endif

	move	I,  M
	daddu	C,   CO1, LDC
	blez	I, .L39
	move	AO, A
	.align 3

.L31:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	TEMP, KK,  ZBASE_SHIFT

	daddu	AO, AO, TEMP
	daddu	BO, B,  TEMP
#endif

	LD	a1,  0 * SIZE(AO)
	MOV	c21, c11
	LD	b1,  0 * SIZE(BO)
	MOV	c31, c11
	LD	a2,  1 * SIZE(AO)

	MOV	c41, c11
	LD	b2,  1 * SIZE(BO)
	MOV	c12, c11
	NOP

	MOV	c22, c11
	LD	a3,  4 * SIZE(AO)
	MOV	c32, c11
	LD	b3,  4 * SIZE(BO)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, K, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 1
#else
	daddiu	TEMP, KK, 1
#endif
	dsra	L,  TEMP, 2

	blez	L, .L35
	MOV	c42, c11
#else
	LD	a1,  0 * SIZE(AO)
	MOV	c21, c11
	LD	b1,  0 * SIZE(B)
	MOV	c31, c11
	LD	a2,  1 * SIZE(AO)

	MOV	c41, c11
	LD	b2,  1 * SIZE(B)
	MOV	c12, c11
	dsra	L,  K, 2

	MOV	c22, c11
	LD	a3,  4 * SIZE(AO)
	MOV	c32, c11
	LD	b3,  4 * SIZE(B)

	NOP
	MOV	c42, c11
	blez	L, .L35
	move	BO,  B
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
#ifndef TRMMKERNEL
	andi	L,  K, 3
#else
	andi	L,  TEMP, 3
#endif
	NOP
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
#ifndef TRMMKERNEL
	LD	b1,  0 * SIZE(CO1)
 	ADD	c11, c11, c22
	LD	b2,  1 * SIZE(CO1)
	ADD	c12, c12, c21

	MADD	b1, b1, ALPHA_R, c11
	daddiu	CO1,CO1, 2 * SIZE
	MADD	b2, b2, ALPHA_R, c12
	daddiu	I, I, -1

	NMSUB	b1, b1, ALPHA_I, c12
	NOP
	MADD	b2, b2, ALPHA_I, c11
	MTC	$0,  c11

	ST	b1, -2 * SIZE(CO1)
	NOP
	bgtz	I, .L31
	ST	b2, -1 * SIZE(CO1)
#else
 	ADD	c11, c11, c22
	ADD	c12, c12, c21

	MUL	b1, ALPHA_R, c11
	daddiu	CO1,CO1, 2 * SIZE
	MUL	b2, ALPHA_R, c12
	daddiu	I, I, -1

	NMSUB	b1, b1, ALPHA_I, c12
	NOP
	MADD	b2, b2, ALPHA_I, c11
	MTC	$0,  c11

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, K, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -1
#else
	daddiu	TEMP, TEMP, -1
#endif

	dsll	TEMP, TEMP, ZBASE_SHIFT

	daddu	AO, AO, TEMP
	daddu	BO, BO, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 1
#endif

	ST	b1, -2 * SIZE(CO1)
	NOP
	bgtz	I, .L31
	ST	b2, -1 * SIZE(CO1)
#endif
	.align 3

.L39:
#if defined(TRMMKERNEL) && !defined(LEFT)
	daddiu	KK, KK, 1
#endif
	move	B, BO
	.align 3


.L999:
	LDARG	$16,   0($sp)
	LDARG	$17,   8($sp)
	ldc1	$f24, 16($sp)
	ldc1	$f25, 24($sp)
	ldc1	$f26, 32($sp)
	ldc1	$f27, 40($sp)
	ldc1	$f28, 48($sp)
	ldc1	$f29, 56($sp)

#if defined(TRMMKERNEL)
	LDARG	$18,  64($sp)
	LDARG	$19,  72($sp)
	LDARG	$20,  80($sp)
#endif

#ifndef __64BIT__
	ldc1	$f20, 88($sp)
	ldc1	$f21, 96($sp)
	ldc1	$f22,104($sp)
	ldc1	$f23,112($sp)
#endif

	j	$31
	daddiu	$sp, $sp, 128

	EPILOGUE
