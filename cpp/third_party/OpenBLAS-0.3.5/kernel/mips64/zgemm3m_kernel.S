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
#define CO5	$18
#define CO6	$19
#define CO7	$20
#define CO8	$21

#if defined(TRMMKERNEL)
#define OFFSET	$22
#define KK	$23
#define TEMP	$24
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
	sdc1	$f28, 80($sp)
	sdc1	$f29, 88($sp)

	LDARG	LDC,  128($sp)

	dsll	LDC, LDC, ZBASE_SHIFT

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

	blez	I, .L20
	MOV	c61, c11

.L11:
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
	andi	L,  K, 3
	NOP
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
	LD	$f0, 0 * SIZE(CO1)
	LD	$f1, 1 * SIZE(CO1)
	LD	$f2, 2 * SIZE(CO1)
	LD	$f3, 3 * SIZE(CO1)

	LD	$f4, 0 * SIZE(CO2)
	MADD	$f0, $f0, ALPHA_R, c11
	LD	$f5, 1 * SIZE(CO2)
	MADD	$f1, $f1, ALPHA_I, c11
	LD	$f6, 2 * SIZE(CO2)
	MADD	$f2, $f2, ALPHA_R, c12
	LD	$f7, 3 * SIZE(CO2)
	MADD	$f3, $f3, ALPHA_I, c12

	MADD	$f4, $f4, ALPHA_R, c21
	ST	$f0,  0 * SIZE(CO1)
	MADD	$f5, $f5, ALPHA_I, c21
	ST	$f1,  1 * SIZE(CO1)
	MADD	$f6, $f6, ALPHA_R, c22
	ST	$f2,  2 * SIZE(CO1)
	MADD	$f7, $f7, ALPHA_I, c22
	ST	$f3,  3 * SIZE(CO1)

	LD	$f0, 0 * SIZE(CO3)
	LD	$f1, 1 * SIZE(CO3)
	LD	$f2, 2 * SIZE(CO3)
	LD	$f3, 3 * SIZE(CO3)

	ST	$f4,  0 * SIZE(CO2)
	ST	$f5,  1 * SIZE(CO2)
	ST	$f6,  2 * SIZE(CO2)
	ST	$f7,  3 * SIZE(CO2)

	LD	$f4, 0 * SIZE(CO4)
	LD	$f5, 1 * SIZE(CO4)
	LD	$f6, 2 * SIZE(CO4)
	LD	$f7, 3 * SIZE(CO4)

	MADD	$f0, $f0, ALPHA_R, c31
	MADD	$f1, $f1, ALPHA_I, c31
	MADD	$f2, $f2, ALPHA_R, c32
	MADD	$f3, $f3, ALPHA_I, c32

	MADD	$f4, $f4, ALPHA_R, c41
	ST	$f0,  0 * SIZE(CO3)
	MADD	$f5, $f5, ALPHA_I, c41
	ST	$f1,  1 * SIZE(CO3)
	MADD	$f6, $f6, ALPHA_R, c42
	ST	$f2,  2 * SIZE(CO3)
	MADD	$f7, $f7, ALPHA_I, c42
	ST	$f3,  3 * SIZE(CO3)

	LD	$f0, 0 * SIZE(CO5)
	LD	$f1, 1 * SIZE(CO5)
	LD	$f2, 2 * SIZE(CO5)
	LD	$f3, 3 * SIZE(CO5)

	ST	$f4,  0 * SIZE(CO4)
	ST	$f5,  1 * SIZE(CO4)
	ST	$f6,  2 * SIZE(CO4)
	ST	$f7,  3 * SIZE(CO4)

	LD	$f4, 0 * SIZE(CO6)
	LD	$f5, 1 * SIZE(CO6)
	LD	$f6, 2 * SIZE(CO6)
	LD	$f7, 3 * SIZE(CO6)

	MADD	$f0, $f0, ALPHA_R, c51
	daddiu	CO1,CO1, 4 * SIZE
	MADD	$f1, $f1, ALPHA_I, c51
	daddiu	CO2,CO2, 4 * SIZE
	MADD	$f2, $f2, ALPHA_R, c52
	daddiu	CO3,CO3, 4 * SIZE
	MADD	$f3, $f3, ALPHA_I, c52
	daddiu	CO4,CO4, 4 * SIZE

	MADD	$f4, $f4, ALPHA_R, c61
	ST	$f0,  0 * SIZE(CO5)
	MADD	$f5, $f5, ALPHA_I, c61
	ST	$f1,  1 * SIZE(CO5)
	MADD	$f6, $f6, ALPHA_R, c62
	ST	$f2,  2 * SIZE(CO5)
	MADD	$f7, $f7, ALPHA_I, c62
	ST	$f3,  3 * SIZE(CO5)

	LD	$f0, 0 * SIZE(CO7)
	LD	$f1, 1 * SIZE(CO7)
	LD	$f2, 2 * SIZE(CO7)
	LD	$f3, 3 * SIZE(CO7)

	ST	$f4,  0 * SIZE(CO6)
	ST	$f5,  1 * SIZE(CO6)
	ST	$f6,  2 * SIZE(CO6)
	ST	$f7,  3 * SIZE(CO6)

	LD	$f4, 0 * SIZE(CO8)
	daddiu	I, I, -1
	LD	$f5, 1 * SIZE(CO8)
	MTC	$0,  c11
	LD	$f6, 2 * SIZE(CO8)
	LD	$f7, 3 * SIZE(CO8)

	MADD	$f0, $f0, ALPHA_R, c71
	daddiu	CO5,CO5, 4 * SIZE
	MADD	$f1, $f1, ALPHA_I, c71
	daddiu	CO6,CO6, 4 * SIZE
	MADD	$f2, $f2, ALPHA_R, c72
	daddiu	CO7,CO7, 4 * SIZE
	MADD	$f3, $f3, ALPHA_I, c72
	daddiu	CO8,CO8, 4 * SIZE

	MADD	$f4, $f4, ALPHA_R, c81
	ST	$f0, -4 * SIZE(CO7)
	MADD	$f5, $f5, ALPHA_I, c81
	ST	$f1, -3 * SIZE(CO7)
	MADD	$f6, $f6, ALPHA_R, c82
	ST	$f2, -2 * SIZE(CO7)
	MADD	$f7, $f7, ALPHA_I, c82
	ST	$f3, -1 * SIZE(CO7)

	ST	$f4, -4 * SIZE(CO8)
	MOV	c21, c11
	ST	$f5, -3 * SIZE(CO8)
	MOV	c31, c11
	ST	$f6, -2 * SIZE(CO8)
	MOV	c41, c11
	ST	$f7, -1 * SIZE(CO8)
	MOV	c51, c11
	bgtz	I, .L11
	MOV	c61, c11
	.align 3

.L20:
	andi	I,  M, 1
	MOV	c61, c11
	blez	I, .L29
	MOV	c71, c11

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
	andi	L,  K, 3
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
	LD	$f0, 0 * SIZE(CO1)
	LD	$f1, 1 * SIZE(CO1)
	LD	$f2, 0 * SIZE(CO2)
	LD	$f3, 1 * SIZE(CO2)

	LD	$f4, 0 * SIZE(CO3)
	MADD	$f0, $f0, ALPHA_R, c11
	LD	$f5, 1 * SIZE(CO3)
	MADD	$f1, $f1, ALPHA_I, c11
	LD	$f6, 0 * SIZE(CO4)
	MADD	$f2, $f2, ALPHA_R, c21
	LD	$f7, 1 * SIZE(CO4)
	MADD	$f3, $f3, ALPHA_I, c21

	MADD	$f4, $f4, ALPHA_R, c31
	ST	$f0,  0 * SIZE(CO1)
	MADD	$f5, $f5, ALPHA_I, c31
	ST	$f1,  1 * SIZE(CO1)
	MADD	$f6, $f6, ALPHA_R, c41
	ST	$f2,  0 * SIZE(CO2)
	MADD	$f7, $f7, ALPHA_I, c41
	ST	$f3,  1 * SIZE(CO2)

	LD	$f0, 0 * SIZE(CO5)
	LD	$f1, 1 * SIZE(CO5)
	LD	$f2, 0 * SIZE(CO6)
	LD	$f3, 1 * SIZE(CO6)

	ST	$f4,  0 * SIZE(CO3)
	ST	$f5,  1 * SIZE(CO3)
	ST	$f6,  0 * SIZE(CO4)
	ST	$f7,  1 * SIZE(CO4)

	LD	$f4, 0 * SIZE(CO7)
	MADD	$f0, $f0, ALPHA_R, c51
	LD	$f5, 1 * SIZE(CO7)
	MADD	$f1, $f1, ALPHA_I, c51
	LD	$f6, 0 * SIZE(CO8)
	MADD	$f2, $f2, ALPHA_R, c61
	LD	$f7, 1 * SIZE(CO8)
	MADD	$f3, $f3, ALPHA_I, c61

	MADD	$f4, $f4, ALPHA_R, c71
	ST	$f0,  0 * SIZE(CO5)
	MADD	$f5, $f5, ALPHA_I, c71
	ST	$f1,  1 * SIZE(CO5)
	MADD	$f6, $f6, ALPHA_R, c81
	ST	$f2,  0 * SIZE(CO6)
	MADD	$f7, $f7, ALPHA_I, c81
	ST	$f3,  1 * SIZE(CO6)

	ST	$f4,  0 * SIZE(CO7)
	ST	$f5,  1 * SIZE(CO7)
	ST	$f6,  0 * SIZE(CO8)
	ST	$f7,  1 * SIZE(CO8)
	.align 3

.L29:
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

	dsra	I,  M, 1
	blez	I, .L40
	MOV	c41, c11

.L31:
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
	andi	L,  K, 3
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
	LD	$f0, 0 * SIZE(CO1)
	LD	$f1, 1 * SIZE(CO1)
	LD	$f2, 2 * SIZE(CO1)
	LD	$f3, 3 * SIZE(CO1)

	LD	$f4, 0 * SIZE(CO2)
	LD	$f5, 1 * SIZE(CO2)
	LD	$f6, 2 * SIZE(CO2)
	LD	$f7, 3 * SIZE(CO2)

	MADD	$f0, $f0, ALPHA_R, c11
	MADD	$f1, $f1, ALPHA_I, c11
	MADD	$f2, $f2, ALPHA_R, c12
	MADD	$f3, $f3, ALPHA_I, c12

	MADD	$f4, $f4, ALPHA_R, c21
	ST	$f0,  0 * SIZE(CO1)
	MADD	$f5, $f5, ALPHA_I, c21
	ST	$f1,  1 * SIZE(CO1)
	MADD	$f6, $f6, ALPHA_R, c22
	ST	$f2,  2 * SIZE(CO1)
	MADD	$f7, $f7, ALPHA_I, c22
	ST	$f3,  3 * SIZE(CO1)

	LD	$f0, 0 * SIZE(CO3)
	LD	$f1, 1 * SIZE(CO3)
	LD	$f2, 2 * SIZE(CO3)
	LD	$f3, 3 * SIZE(CO3)

	ST	$f4,  0 * SIZE(CO2)
	MADD	$f0, $f0, ALPHA_R, c31
	ST	$f5,  1 * SIZE(CO2)
	MADD	$f1, $f1, ALPHA_I, c31
	ST	$f6,  2 * SIZE(CO2)
	MADD	$f2, $f2, ALPHA_R, c32
	ST	$f7,  3 * SIZE(CO2)
	MADD	$f3, $f3, ALPHA_I, c32

	LD	$f4, 0 * SIZE(CO4)
	LD	$f5, 1 * SIZE(CO4)
	LD	$f6, 2 * SIZE(CO4)
	LD	$f7, 3 * SIZE(CO4)

	MADD	$f4, $f4, ALPHA_R, c41
	daddiu	CO1,CO1, 4 * SIZE
	MADD	$f5, $f5, ALPHA_I, c41
	daddiu	CO2,CO2, 4 * SIZE
	MADD	$f6, $f6, ALPHA_R, c42
	daddiu	CO3,CO3, 4 * SIZE
	MADD	$f7, $f7, ALPHA_I, c42
	daddiu	CO4,CO4, 4 * SIZE

	ST	$f0, -4 * SIZE(CO3)
	daddiu	I, I, -1
	ST	$f1, -3 * SIZE(CO3)
	ST	$f2, -2 * SIZE(CO3)
	ST	$f3, -1 * SIZE(CO3)

	ST	$f4, -4 * SIZE(CO4)
	MTC	$0,  c11
	ST	$f5, -3 * SIZE(CO4)
	MOV	c21, c11
	ST	$f6, -2 * SIZE(CO4)
	MOV	c31, c11
	ST	$f7, -1 * SIZE(CO4)
	bgtz	I, .L31
	MOV	c41, c11
	.align 3

.L40:
	andi	I,  M, 1
	blez	I, .L49
	MOV	c61, c11

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
	andi	L,  K, 3
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
	LD	$f0, 0 * SIZE(CO1)
	LD	$f1, 1 * SIZE(CO1)
	LD	$f2, 0 * SIZE(CO2)
	LD	$f3, 1 * SIZE(CO2)

	LD	$f4, 0 * SIZE(CO3)
	MADD	$f0, $f0, ALPHA_R, c11
	LD	$f5, 1 * SIZE(CO3)
	MADD	$f1, $f1, ALPHA_I, c11
	LD	$f6, 0 * SIZE(CO4)
	MADD	$f2, $f2, ALPHA_R, c21
	LD	$f7, 1 * SIZE(CO4)
	MADD	$f3, $f3, ALPHA_I, c21

	MADD	$f4, $f4, ALPHA_R, c31
	ST	$f0,  0 * SIZE(CO1)
	MADD	$f5, $f5, ALPHA_I, c31
	ST	$f1,  1 * SIZE(CO1)
	MADD	$f6, $f6, ALPHA_R, c41
	ST	$f2,  0 * SIZE(CO2)
	MADD	$f7, $f7, ALPHA_I, c41
	ST	$f3,  1 * SIZE(CO2)

	ST	$f4,  0 * SIZE(CO3)
	ST	$f5,  1 * SIZE(CO3)
	ST	$f6,  0 * SIZE(CO4)
	ST	$f7,  1 * SIZE(CO4)
	.align 3

.L49:
	move	B, BO
	.align 3

.L50:
	andi	J,  N, 2
	blez	J, .L70

	move	AO, A
	move	CO1, C
	daddu	CO2, C,   LDC

	dsra	I,  M, 1
	blez	I, .L60
	daddu	C,   CO2, LDC

.L51:
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
	andi	L,  K, 3
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
	LD	$f0, 0 * SIZE(CO1)
	LD	$f1, 1 * SIZE(CO1)
	LD	$f2, 2 * SIZE(CO1)
	LD	$f3, 3 * SIZE(CO1)

	LD	$f4, 0 * SIZE(CO2)
	LD	$f5, 1 * SIZE(CO2)
	LD	$f6, 2 * SIZE(CO2)
	LD	$f7, 3 * SIZE(CO2)

	MADD	$f0, $f0, ALPHA_R, c11
	daddiu	I, I, -1
	MADD	$f1, $f1, ALPHA_I, c11
	daddiu	CO1,CO1, 4 * SIZE
	MADD	$f2, $f2, ALPHA_R, c12
	daddiu	CO2,CO2, 4 * SIZE
	MADD	$f3, $f3, ALPHA_I, c12
	MADD	$f4, $f4, ALPHA_R, c21
	MADD	$f5, $f5, ALPHA_I, c21
	MADD	$f6, $f6, ALPHA_R, c22
	MADD	$f7, $f7, ALPHA_I, c22

	ST	$f0, -4 * SIZE(CO1)
	ST	$f1, -3 * SIZE(CO1)
	ST	$f2, -2 * SIZE(CO1)
	ST	$f3, -1 * SIZE(CO1)

	ST	$f4, -4 * SIZE(CO2)
	ST	$f5, -3 * SIZE(CO2)
	ST	$f6, -2 * SIZE(CO2)
	bgtz	I, .L51
	ST	$f7, -1 * SIZE(CO2)
	.align 3

.L60:
	andi	I,  M, 1
	blez	I, .L69
	NOP

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
	andi	L,  K, 3
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
	LD	$f0, 0 * SIZE(CO1)
	LD	$f1, 1 * SIZE(CO1)
	LD	$f2, 0 * SIZE(CO2)
	LD	$f3, 1 * SIZE(CO2)

	ADD	c11, c11, c31
	ADD	c21, c21, c41

	MADD	$f0, $f0, ALPHA_R, c11
	MADD	$f1, $f1, ALPHA_I, c11
	MADD	$f2, $f2, ALPHA_R, c21
	MADD	$f3, $f3, ALPHA_I, c21

	ST	$f0,  0 * SIZE(CO1)
	ST	$f1,  1 * SIZE(CO1)
	ST	$f2,  0 * SIZE(CO2)
	ST	$f3,  1 * SIZE(CO2)
	.align 3

.L69:
	move	B, BO
	.align 3

.L70:
	andi	J,  N, 1
	blez	J, .L999

	move	AO, A
	move	CO1, C

	dsra	I,  M, 1
	blez	I, .L80
	daddu	C,   CO1, LDC

.L71:
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
	andi	L,  K, 3
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
	LD	$f0, 0 * SIZE(CO1)
	LD	$f1, 1 * SIZE(CO1)
	LD	$f2, 2 * SIZE(CO1)
	LD	$f3, 3 * SIZE(CO1)

	ADD	c11, c11, c21
	daddiu	I, I, -1
	ADD	c12, c12, c22
	daddiu	CO1,CO1, 4 * SIZE

	MADD	$f0, $f0, ALPHA_R, c11
	MADD	$f1, $f1, ALPHA_I, c11
	MADD	$f2, $f2, ALPHA_R, c12
	MADD	$f3, $f3, ALPHA_I, c12

	ST	$f0, -4 * SIZE(CO1)
	ST	$f1, -3 * SIZE(CO1)
	ST	$f2, -2 * SIZE(CO1)

	bgtz	I, .L71
	ST	$f3, -1 * SIZE(CO1)
	.align 3

.L80:
	andi	I,  M, 1
	blez	I, .L89
	NOP

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
	andi	L,  K, 3
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
	LD	$f0, 0 * SIZE(CO1)
	LD	$f1, 1 * SIZE(CO1)

	ADD	c11, c11, c21
	MADD	$f0, $f0, ALPHA_R, c11
	MADD	$f1, $f1, ALPHA_I, c11

	ST	$f0,  0 * SIZE(CO1)
	ST	$f1,  1 * SIZE(CO1)
	.align 3

.L89:
	move	B, BO
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
	ldc1	$f29, 88($sp)

	j	$31
	daddiu	$sp, $sp, 128

	EPILOGUE
