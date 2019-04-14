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

#define PREFETCHSIZE (4 * 10)

#define CO1	$14
#define CO2	$15
#define CO3	$16
#define CO4	$17
#define CO5	$18
#define CO6	$19
#define CO7	$20
#define CO8	$21

#define BB	$22

#if defined(TRMMKERNEL)
#define OFFSET	$23
#define KK	$24
#define TEMP	$25
#endif

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

	daddiu	$sp, $sp, -160

	SDARG	$16,   0($sp)
	SDARG	$17,   8($sp)
	SDARG	$18,  16($sp)
	SDARG	$19,  24($sp)
	SDARG	$20,  32($sp)
	SDARG	$21,  40($sp)
	SDARG	$22,  48($sp)

	sdc1	$f24, 56($sp)
	sdc1	$f25, 64($sp)
	sdc1	$f26, 72($sp)
	sdc1	$f27, 80($sp)
	sdc1	$f28, 88($sp)

#if defined(TRMMKERNEL)
	SDARG	$23,  96($sp)
	SDARG	$24, 104($sp)
	SDARG	$25, 112($sp)

	LDARG	OFFSET, 160($sp)
#endif

#ifndef __64BIT__
	sdc1	$f20,120($sp)
	sdc1	$f21,128($sp)
	sdc1	$f22,136($sp)
	sdc1	$f23,144($sp)
#endif

	dsll	LDC, LDC, BASE_SHIFT

#if defined(TRMMKERNEL) && !defined(LEFT)
	neg	KK, OFFSET
#endif

	dsra	J,  N, 3
	blez	J, .L30
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
	daddu	CO5, CO4, LDC
	MOV	c31, c11
	daddu	CO6, CO5, LDC
	MOV	c41, c11
	daddu	CO7, CO6, LDC
	MOV	c51, c11
	daddu	CO8, CO7, LDC
	dsra	I,  M, 1
	daddu	C,   CO8, LDC

	dsll	BB, K, 2 + BASE_SHIFT
	daddu	BB, B, BB

#if defined(TRMMKERNEL) &&  defined(LEFT)
	move	KK, OFFSET
#endif

	blez	I, .L20
	MOV	c61, c11

.L11:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	L,    KK, 1 + BASE_SHIFT
	dsll	TEMP, KK, 3 + BASE_SHIFT

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
	daddiu	TEMP, KK, 2
#else
	daddiu	TEMP, KK, 8
#endif
	dsra	L,  TEMP, 2

	blez	L, .L15
	NOP
#else
	LD	a1,  0 * SIZE(AO)
	MOV	c71, c11
	LD	b1,  0 * SIZE(B)
	MOV	c81, c11

	pref	1, 3 * SIZE(CO1)
	pref	1, 3 * SIZE(CO2)

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

	MADD	c11, c11, a1, b1
	LD	a2,  1 * SIZE(AO)
	MADD	c21, c21, a1, b2
	daddiu	L, L, -1
	MADD	c31, c31, a1, b3
	blez	L, .L13
	MADD	c41, c41, a1, b4
	pref	1, 2 * SIZE(CO3)
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
	LD	a4,  2 * SIZE(AO)
	MADD	c61, c61, a1, b2
	NOP
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
	LD	a4,  6 * SIZE(AO)
	MADD	c61, c61, a3, b2
	NOP
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
	pref	1, 3 * SIZE(CO4)
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
	pref	1, 3 * SIZE(CO5)
	MADD	c61, c61, a4, b2
	NOP
	MADD	c71, c71, a4, b3
	pref	1, 3 * SIZE(CO6)
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
	pref	1, 3 * SIZE(CO7)
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
	NOP

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
#ifndef TRMMKERNEL
	andi	L,  K, 3
#else
	andi	L,  TEMP, 3
#endif
	NOP
	blez	L, .L18
	pref	1, 3 * SIZE(CO8)
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
#ifndef TRMMKERNEL
	LD	$f0, 0 * SIZE(CO1)
	daddiu	CO3,CO3, 2 * SIZE
	LD	$f1, 1 * SIZE(CO1)
	daddiu	CO1,CO1, 2 * SIZE
	LD	$f2, 0 * SIZE(CO2)
	daddiu	CO4,CO4, 2 * SIZE
	LD	$f3, 1 * SIZE(CO2)
	daddiu	CO2,CO2, 2 * SIZE

	LD	$f4, -2 * SIZE(CO3)
	daddiu	CO5,CO5, 2 * SIZE
	LD	$f5, -1 * SIZE(CO3)
	daddiu	CO6,CO6, 2 * SIZE
	LD	$f6, -2 * SIZE(CO4)
	daddiu	CO7,CO7, 2 * SIZE
	LD	$f7, -1 * SIZE(CO4)
	daddiu	I, I, -1

	MADD	c11, $f0, ALPHA, c11
	LD	$f0,-2 * SIZE(CO5)
	MADD	c12, $f1, ALPHA, c12
	LD	$f1,-1 * SIZE(CO5)
	MADD	c21, $f2, ALPHA, c21
	LD	$f2,-2 * SIZE(CO6)
	MADD	c22, $f3, ALPHA, c22
	LD	$f3,-1 * SIZE(CO6)

	MADD	c31, $f4, ALPHA, c31
	LD	$f4,-2 * SIZE(CO7)
	MADD	c32, $f5, ALPHA, c32
	LD	$f5,-1 * SIZE(CO7)
	MADD	c41, $f6, ALPHA, c41
	LD	$f6, 0 * SIZE(CO8)
	MADD	c42, $f7, ALPHA, c42
	LD	$f7, 1 * SIZE(CO8)

	pref	0, 0 * SIZE(BB)
	pref	0, 8 * SIZE(BB)

	ST	c11, -2 * SIZE(CO1)
	MTC	$0,  c11
	ST	c12, -1 * SIZE(CO1)
	daddiu	CO8,CO8, 2 * SIZE
	ST	c21, -2 * SIZE(CO2)
	MOV	c21, c11
	ST	c22, -1 * SIZE(CO2)
	daddiu	BB, BB, 16 * SIZE

	MADD	c51, $f0, ALPHA, c51
	ST	c31, -2 * SIZE(CO3)
	MADD	c52, $f1, ALPHA, c52
	ST	c32, -1 * SIZE(CO3)
	MADD	c61, $f2, ALPHA, c61
	ST	c41, -2 * SIZE(CO4)
	MADD	c62, $f3, ALPHA, c62
	ST	c42, -1 * SIZE(CO4)

	MADD	c71, $f4, ALPHA, c71
	ST	c51, -2 * SIZE(CO5)
	MADD	c72, $f5, ALPHA, c72
	ST	c52, -1 * SIZE(CO5)
	MADD	c81, $f6, ALPHA, c81
	ST	c61, -2 * SIZE(CO6)
	MADD	c82, $f7, ALPHA, c82
	ST	c62, -1 * SIZE(CO6)

	ST	c71, -2 * SIZE(CO7)
	MOV	c31, c11
	ST	c72, -1 * SIZE(CO7)
	MOV	c41, c11

	ST	c81, -2 * SIZE(CO8)
	MOV	c51, c11
	ST	c82, -1 * SIZE(CO8)
	bgtz	I, .L11
	MOV	c61, c11
#else
	daddiu	CO4,CO4, 2 * SIZE
	daddiu	CO5,CO5, 2 * SIZE
	daddiu	CO6,CO6, 2 * SIZE
	daddiu	CO7,CO7, 2 * SIZE

	pref	0, 0 * SIZE(BB)
	pref	0, 8 * SIZE(BB)

	MUL	c11, ALPHA, c11
	daddiu	CO1,CO1, 2 * SIZE
	MUL	c12, ALPHA, c12
	MTC	$0,  a1
	MUL	c21, ALPHA, c21
	daddiu	CO2,CO2, 2 * SIZE
	MUL	c22, ALPHA, c22
	daddiu	CO3,CO3, 2 * SIZE

	ST	c11, -2 * SIZE(CO1)
	MUL	c31, ALPHA, c31
	ST	c12, -1 * SIZE(CO1)
	MUL	c32, ALPHA, c32
	ST	c21, -2 * SIZE(CO2)
	MUL	c41, ALPHA, c41
	ST	c22, -1 * SIZE(CO2)
	MUL	c42, ALPHA, c42

	ST	c31, -2 * SIZE(CO3)
	MUL	c51, ALPHA, c51
	ST	c32, -1 * SIZE(CO3)
	MUL	c52, ALPHA, c52
	ST	c41, -2 * SIZE(CO4)
	MUL	c61, ALPHA, c61
	ST	c42, -1 * SIZE(CO4)
	MUL	c62, ALPHA, c62

	ST	c51, -2 * SIZE(CO5)
	MUL	c71, ALPHA, c71
	ST	c52, -1 * SIZE(CO5)
	MUL	c72, ALPHA, c72
	ST	c61, -2 * SIZE(CO6)
	MUL	c81, ALPHA, c81
	ST	c62, -1 * SIZE(CO6)
	MUL	c82, ALPHA, c82

	ST	c71, -2 * SIZE(CO7)
	MOV	c11, a1
	ST	c72, -1 * SIZE(CO7)
	MOV	c21, a1

	daddiu	CO8,CO8, 2 * SIZE
	daddiu	BB, BB, 16 * SIZE

	ST	c81, -2 * SIZE(CO8)
	MOV	c31, a1
	ST	c82, -1 * SIZE(CO8)
	MOV	c41, a1

	daddiu	I, I, -1
	MOV	c51, a1

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, K, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -2
#else
	daddiu	TEMP, TEMP, -8
#endif

	dsll	L,    TEMP, 1 + BASE_SHIFT
	dsll	TEMP, TEMP, 3 + BASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 2
#endif

	bgtz	I, .L11
	MOV	c61, a1
#endif
	.align 3

.L20:
	andi	I,  M, 1
	MOV	c61, c11
	blez	I, .L29
	MOV	c71, c11

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	L,    KK, 0 + BASE_SHIFT
	dsll	TEMP, KK, 3 + BASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, B,  TEMP
#endif

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

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, K, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 1
#else
	daddiu	TEMP, KK, 8
#endif
	dsra	L,  TEMP, 2

	blez	L, .L25
	MOV	c81, c11
#else
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

	dsra	L,  K, 2
	MOV	c81, c11

	blez	L, .L25
	move	BO,  B
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
#ifndef TRMMKERNEL
	LD	$f0, 0 * SIZE(CO1)
	LD	$f1, 0 * SIZE(CO2)
	LD	$f2, 0 * SIZE(CO3)
	LD	$f3, 0 * SIZE(CO4)
	MADD	c11, $f0, ALPHA, c11
	LD	$f4, 0 * SIZE(CO5)
	MADD	c21, $f1, ALPHA, c21
	LD	$f5, 0 * SIZE(CO6)
	MADD	c31, $f2, ALPHA, c31
	LD	$f6, 0 * SIZE(CO7)
	MADD	c41, $f3, ALPHA, c41
	LD	$f7, 0 * SIZE(CO8)
	MADD	c51, $f4, ALPHA, c51
	ST	c11,  0 * SIZE(CO1)
	MADD	c61, $f5, ALPHA, c61
	ST	c21,  0 * SIZE(CO2)
	MADD	c71, $f6, ALPHA, c71
	ST	c31,  0 * SIZE(CO3)
	MADD	c81, $f7, ALPHA, c81
	ST	c41,  0 * SIZE(CO4)
	ST	c51,  0 * SIZE(CO5)
	ST	c61,  0 * SIZE(CO6)
	ST	c71,  0 * SIZE(CO7)
	ST	c81,  0 * SIZE(CO8)
#else
	MUL	c11, ALPHA, c11
	MUL	c21, ALPHA, c21
	MUL	c31, ALPHA, c31
	MUL	c41, ALPHA, c41

	ST	c11,  0 * SIZE(CO1)
	MUL	c51, ALPHA, c51
	ST	c21,  0 * SIZE(CO2)
	MUL	c61, ALPHA, c61
	ST	c31,  0 * SIZE(CO3)
	MUL	c71, ALPHA, c71
	ST	c41,  0 * SIZE(CO4)
	MUL	c81, ALPHA, c81

	ST	c51,  0 * SIZE(CO5)
	ST	c61,  0 * SIZE(CO6)
	ST	c71,  0 * SIZE(CO7)
	ST	c81,  0 * SIZE(CO8)

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, K, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -1
#else
	daddiu	TEMP, TEMP, -8
#endif

	dsll	L,    TEMP, 0 + BASE_SHIFT
	dsll	TEMP, TEMP, 3 + BASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 1
#endif
#endif
	.align 3

.L29:
#if defined(TRMMKERNEL) && !defined(LEFT)
	daddiu	KK, KK, 8
#endif

	bgtz	J, .L10
	move	B, BO
	.align 3

.L30:
	andi	J,  N, 4
	blez	J, .L50
	move	AO, A

	move	CO1, C
	MTC	$0,  c11
	daddu	CO2, C,   LDC
	daddu	CO3, CO2, LDC
	daddu	CO4, CO3, LDC
	MOV	c21, c11
	daddu	C,   CO4, LDC
	MOV	c31, c11

#if defined(TRMMKERNEL) &&  defined(LEFT)
	move	KK, OFFSET
#endif

	dsra	I,  M, 1
	blez	I, .L40
	MOV	c41, c11

.L31:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	L,    KK, 1 + BASE_SHIFT
	dsll	TEMP, KK, 2 + BASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, B,  TEMP
#endif

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
	LD	b6,  8 * SIZE(BO)
	LD	b7, 12 * SIZE(BO)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, K, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 2
#else
	daddiu	TEMP, KK, 4
#endif
	dsra	L,  TEMP, 2
	blez	L, .L35
	NOP
#else
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
	dsra	L,  K, 2
	LD	b6,  8 * SIZE(B)
	LD	b7, 12 * SIZE(B)

	blez	L, .L35
	move	BO,  B
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
#ifndef TRMMKERNEL
	LD	$f0, 0 * SIZE(CO1)
	daddiu	CO3,CO3, 2 * SIZE
	LD	$f1, 1 * SIZE(CO1)
	daddiu	CO1,CO1, 2 * SIZE
	LD	$f2, 0 * SIZE(CO2)
	daddiu	CO4,CO4, 2 * SIZE
	LD	$f3, 1 * SIZE(CO2)
	daddiu	CO2,CO2, 2 * SIZE

	LD	$f4, -2 * SIZE(CO3)
	MADD	c11, $f0, ALPHA, c11
	LD	$f5, -1 * SIZE(CO3)
	MADD	c12, $f1, ALPHA, c12
	LD	$f6, -2 * SIZE(CO4)
	MADD	c21, $f2, ALPHA, c21
	LD	$f7, -1 * SIZE(CO4)
	MADD	c22, $f3, ALPHA, c22

	MADD	c31, $f4, ALPHA, c31
	ST	c11, -2 * SIZE(CO1)
	MADD	c32, $f5, ALPHA, c32
	ST	c12, -1 * SIZE(CO1)
	MADD	c41, $f6, ALPHA, c41
	ST	c21, -2 * SIZE(CO2)
	MADD	c42, $f7, ALPHA, c42
	ST	c22, -1 * SIZE(CO2)

	ST	c31, -2 * SIZE(CO3)
	MTC	$0,  c11
	ST	c32, -1 * SIZE(CO3)
	daddiu	I, I, -1
	ST	c41, -2 * SIZE(CO4)
	MOV	c21, c11
	ST	c42, -1 * SIZE(CO4)
	MOV	c31, c11
#else
	MUL	c11, ALPHA, c11
	daddiu	CO3,CO3, 2 * SIZE
	MUL	c12, ALPHA, c12
	daddiu	CO1,CO1, 2 * SIZE
	MUL	c21, ALPHA, c21
	daddiu	CO4,CO4, 2 * SIZE
	MUL	c22, ALPHA, c22
	daddiu	CO2,CO2, 2 * SIZE

	ST	c11, -2 * SIZE(CO1)
	MUL	c31, ALPHA, c31
	ST	c12, -1 * SIZE(CO1)
	MUL	c32, ALPHA, c32
	ST	c21, -2 * SIZE(CO2)
	MUL	c41, ALPHA, c41
	ST	c22, -1 * SIZE(CO2)
	MUL	c42, ALPHA, c42

	ST	c31, -2 * SIZE(CO3)
	MTC	$0,  c11
	ST	c32, -1 * SIZE(CO3)
	daddiu	I, I, -1
	ST	c41, -2 * SIZE(CO4)
	MOV	c21, c11
	ST	c42, -1 * SIZE(CO4)
	MOV	c31, c11

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, K, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -2
#else
	daddiu	TEMP, TEMP, -4
#endif

	dsll	L,    TEMP, 1 + BASE_SHIFT
	dsll	TEMP, TEMP, 2 + BASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 2
#endif
#endif

	bgtz	I, .L31
	MOV	c41, c11
	.align 3

.L40:
	andi	I,  M, 1
	blez	I, .L49
	MOV	c61, c11

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	L,    KK, 0 + BASE_SHIFT
	dsll	TEMP, KK, 2 + BASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, B,  TEMP
#endif

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

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, K, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 1
#else
	daddiu	TEMP, KK, 4
#endif
	dsra	L,  TEMP, 2

	blez	L, .L45
	NOP
#else
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

	dsra	L,  K, 2

	blez	L, .L45
	move	BO,  B
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
#ifndef TRMMKERNEL
	andi	L,  K, 3
#else
	andi	L,  TEMP, 3
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
#ifndef TRMMKERNEL
	LD	$f0, 0 * SIZE(CO1)
	LD	$f1, 0 * SIZE(CO2)
	LD	$f2, 0 * SIZE(CO3)
	LD	$f3, 0 * SIZE(CO4)

	MADD	c11, $f0, ALPHA, c11
	MADD	c21, $f1, ALPHA, c21
	MADD	c31, $f2, ALPHA, c31
	MADD	c41, $f3, ALPHA, c41

	ST	c11,  0 * SIZE(CO1)
	ST	c21,  0 * SIZE(CO2)
	ST	c31,  0 * SIZE(CO3)
	ST	c41,  0 * SIZE(CO4)
#else
	MUL	c11, ALPHA, c11
	MUL	c21, ALPHA, c21
	MUL	c31, ALPHA, c31
	MUL	c41, ALPHA, c41

	ST	c11,  0 * SIZE(CO1)
	ST	c21,  0 * SIZE(CO2)
	ST	c31,  0 * SIZE(CO3)
	ST	c41,  0 * SIZE(CO4)

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, K, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -1
#else
	daddiu	TEMP, TEMP, -4
#endif

	dsll	L,    TEMP, 0 + BASE_SHIFT
	dsll	TEMP, TEMP, 2 + BASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 1
#endif
#endif
	.align 3

.L49:
#if defined(TRMMKERNEL) && !defined(LEFT)
	daddiu	KK, KK, 4
#endif
	move	B, BO
	.align 3

.L50:
	andi	J,  N, 2
	blez	J, .L70

	move	AO, A
	move	CO1, C
	daddu	CO2, C,   LDC

#if defined(TRMMKERNEL) &&  defined(LEFT)
	move	KK, OFFSET
#endif

	dsra	I,  M, 1
	blez	I, .L60
	daddu	C,   CO2, LDC

.L51:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	L,    KK, 1 + BASE_SHIFT
	dsll	TEMP, KK, 1 + BASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, B,  TEMP
#endif

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
	LD	b6,  8 * SIZE(BO)
	LD	b7, 12 * SIZE(BO)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, K, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 2
#else
	daddiu	TEMP, KK, 2
#endif
	dsra	L,  TEMP, 2
	blez	L, .L55
	NOP
#else
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
	dsra	L,  K, 2
	LD	b6,  8 * SIZE(B)
	LD	b7, 12 * SIZE(B)

	blez	L, .L55
	move	BO,  B
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
#ifndef TRMMKERNEL
	andi	L,  K, 3
#else
	andi	L,  TEMP, 3
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
#ifndef TRMMKERNEL
	LD	$f0, 0 * SIZE(CO1)
	daddiu	I, I, -1
	LD	$f1, 1 * SIZE(CO1)
	daddiu	CO1,CO1, 2 * SIZE
	LD	$f2, 0 * SIZE(CO2)
	NOP
	LD	$f3, 1 * SIZE(CO2)
	daddiu	CO2,CO2, 2 * SIZE

	MADD	c11, $f0, ALPHA, c11
	MADD	c12, $f1, ALPHA, c12
	MADD	c21, $f2, ALPHA, c21
	MADD	c22, $f3, ALPHA, c22

	ST	c11, -2 * SIZE(CO1)
	ST	c12, -1 * SIZE(CO1)
	ST	c21, -2 * SIZE(CO2)
	NOP
	bgtz	I, .L51
	ST	c22, -1 * SIZE(CO2)
#else
	daddiu	I, I, -1

	daddiu	CO1,CO1, 2 * SIZE
	daddiu	CO2,CO2, 2 * SIZE

	MUL	c11, ALPHA, c11
	MUL	c12, ALPHA, c12
	MUL	c21, ALPHA, c21
	MUL	c22, ALPHA, c22

	ST	c11, -2 * SIZE(CO1)
	ST	c12, -1 * SIZE(CO1)
	ST	c21, -2 * SIZE(CO2)
	ST	c22, -1 * SIZE(CO2)

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, K, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -2
#else
	daddiu	TEMP, TEMP, -2
#endif

	dsll	L,    TEMP, 1 + BASE_SHIFT
	dsll	TEMP, TEMP, 1 + BASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 2
#endif

	bgtz	I, .L51
	NOP
#endif
	.align 3

.L60:
	andi	I,  M, 1
	blez	I, .L69
	NOP

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	L,    KK, 0 + BASE_SHIFT
	dsll	TEMP, KK, 1 + BASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, B,  TEMP
#endif

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

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, K, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 1
#else
	daddiu	TEMP, KK, 2
#endif
	dsra	L,  TEMP, 2
	blez	L, .L65
	NOP
#else
	dsra	L,  K, 2
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
#ifndef TRMMKERNEL
	andi	L,  K, 3
#else
	andi	L,  TEMP, 3
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
#ifndef TRMMKERNEL
	LD	$f0, 0 * SIZE(CO1)
	LD	$f1, 0 * SIZE(CO2)

	ADD	c11, c11, c31
	ADD	c21, c21, c41

	MADD	c11, $f0, ALPHA, c11
	MADD	c21, $f1, ALPHA, c21

	ST	c11,  0 * SIZE(CO1)
	ST	c21,  0 * SIZE(CO2)
#else
	ADD	c11, c11, c31
	ADD	c21, c21, c41

	MUL	c11, ALPHA, c11
	MUL	c21, ALPHA, c21

	ST	c11,  0 * SIZE(CO1)
	ST	c21,  0 * SIZE(CO2)

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, K, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -1
#else
	daddiu	TEMP, TEMP, -2
#endif

	dsll	L,    TEMP, 0 + BASE_SHIFT
	dsll	TEMP, TEMP, 1 + BASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 1
#endif
#endif
	.align 3

.L69:
#if defined(TRMMKERNEL) && !defined(LEFT)
	daddiu	KK, KK, 2
#endif
	move	B, BO
	.align 3

.L70:
	andi	J,  N, 1
	blez	J, .L999

	move	AO, A
	move	CO1, C

#if defined(TRMMKERNEL) &&  defined(LEFT)
	move	KK, OFFSET
#endif

	dsra	I,  M, 1
	blez	I, .L80
	daddu	C,   CO1, LDC

.L71:
#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	L,    KK, 1 + BASE_SHIFT
	dsll	TEMP, KK, 0 + BASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, B,  TEMP
#endif

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
	LD	b6,  8 * SIZE(BO)
	LD	b7, 12 * SIZE(BO)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, K, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 2
#else
	daddiu	TEMP, KK, 1
#endif
	dsra	L,  TEMP, 2
	blez	L, .L75
	NOP
#else
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
	dsra	L,  K, 2
	LD	b6,  8 * SIZE(B)
	LD	b7, 12 * SIZE(B)

	blez	L, .L75
	move	BO,  B
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
#ifndef TRMMKERNEL
	andi	L,  K, 3
#else
	andi	L,  TEMP, 3
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
#ifndef TRMMKERNEL
	LD	$f0, 0 * SIZE(CO1)
	daddiu	I, I, -1
	LD	$f1, 1 * SIZE(CO1)
	daddiu	CO1,CO1, 2 * SIZE

	ADD	c11, c11, c21
	ADD	c12, c12, c22

	MADD	c11, $f0, ALPHA, c11
	MADD	c12, $f1, ALPHA, c12

	ST	c11, -2 * SIZE(CO1)
	bgtz	I, .L71
	ST	c12, -1 * SIZE(CO1)
#else
	ADD	c11, c11, c21
	daddiu	I, I, -1
	ADD	c12, c12, c22
	daddiu	CO1,CO1, 2 * SIZE

	MUL	c11, ALPHA, c11
	MUL	c12, ALPHA, c12

	ST	c11, -2 * SIZE(CO1)
	ST	c12, -1 * SIZE(CO1)

#if ( defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
	dsubu	TEMP, K, KK
#ifdef LEFT
	daddiu	TEMP, TEMP, -2
#else
	daddiu	TEMP, TEMP, -1
#endif

	dsll	L,    TEMP, 1 + BASE_SHIFT
	dsll	TEMP, TEMP, 0 + BASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, BO, TEMP
#endif

#ifdef LEFT
	daddiu	KK, KK, 2
#endif

	bgtz	I, .L71
	NOP
#endif
	.align 3

.L80:
	andi	I,  M, 1
	blez	I, .L89
	NOP

#if defined(TRMMKERNEL)
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	move	BO,  B
#else
	dsll	L,    KK, 0 + BASE_SHIFT
	dsll	TEMP, KK, 0 + BASE_SHIFT

	daddu	AO, AO, L
	daddu	BO, B,  TEMP
#endif

	LD	a1,  0 * SIZE(AO)
	MTC	$0,  c11
	LD	a2,  1 * SIZE(AO)
	MOV	c21, c11
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(BO)
	LD	b2,  1 * SIZE(BO)
	LD	b3,  2 * SIZE(BO)
	LD	b4,  3 * SIZE(BO)
	LD	b5,  4 * SIZE(BO)
	LD	b6,  8 * SIZE(BO)
	LD	b7, 12 * SIZE(BO)

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	dsubu	TEMP, K, KK
#elif defined(LEFT)
	daddiu	TEMP, KK, 1
#else
	daddiu	TEMP, KK, 1
#endif
	dsra	L,  TEMP, 2
	blez	L, .L85
	NOP
#else
	LD	a1,  0 * SIZE(AO)
	MTC	$0,  c11
	LD	a2,  1 * SIZE(AO)
	MOV	c21, c11
	LD	a3,  2 * SIZE(AO)
	LD	a4,  3 * SIZE(AO)

	LD	b1,  0 * SIZE(B)
	LD	b2,  1 * SIZE(B)
	LD	b3,  2 * SIZE(B)
	LD	b4,  3 * SIZE(B)
	LD	b5,  4 * SIZE(B)
	LD	b6,  8 * SIZE(B)
	LD	b7, 12 * SIZE(B)

	dsra	L,  K, 2
	blez	L, .L85
	move	BO,  B
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
#ifndef TRMMKERNEL
	andi	L,  K, 3
#else
	andi	L,  TEMP, 3
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
#ifndef TRMMKERNEL
	LD	$f0, 0 * SIZE(CO1)

	ADD	c11, c11, c21
	MADD	c11, $f0, ALPHA, c11

	ST	c11,  0 * SIZE(CO1)
#else
	ADD	c11, c11, c21
	MUL	c11, ALPHA, c11

	ST	c11,  0 * SIZE(CO1)
#endif
	.align 3

.L89:
#if defined(TRMMKERNEL) && !defined(LEFT)
	daddiu	KK, KK, 1
#endif
	move	B, BO
	.align 3


.L999:
	LDARG	$16,   0($sp)
	LDARG	$17,   8($sp)
	LDARG	$18,  16($sp)
	LDARG	$19,  24($sp)
	LDARG	$20,  32($sp)
	LDARG	$21,  40($sp)
	LDARG	$22,  48($sp)

	ldc1	$f24, 56($sp)
	ldc1	$f25, 64($sp)
	ldc1	$f26, 72($sp)
	ldc1	$f27, 80($sp)
	ldc1	$f28, 88($sp)

#if defined(TRMMKERNEL)
	LDARG	$23,  96($sp)
	LDARG	$24, 104($sp)
	LDARG	$25, 112($sp)
#endif

#ifndef __64BIT__
	ldc1	$f20,120($sp)
	ldc1	$f21,128($sp)
	ldc1	$f22,136($sp)
	ldc1	$f23,144($sp)
#endif

	j	$31
	daddiu	$sp, $sp, 160

	EPILOGUE
