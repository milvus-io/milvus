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

#define	M	r3
#define	N	r4
#define	A	r5
#define	LDA	r6
#define B	r7

#define AO1	r8
#define AO2	r9
#define AO3	r10
#define AO4	r11

#define J	r12

#define B1	r21
#define B2	r22
#define B3	r23
#define B4	r24
#define M8	r25

#define AO5	r26
#define AO6	r27
#define AO7	r28
#define AO8	r29
#define INC	r30
#define INC2	r31

#define c01	f0
#define c02	f1
#define c03	f2
#define c04	f3
#define c05	f4
#define c06	f5
#define c07	f6
#define c08	f7
#define c09	f8
#define c10	f9
#define c11	f10
#define c12	f11
#define c13	f12
#define c14	f13
#define c15	f14
#define c16	f15

#define c17	f16
#define c18	f17
#define c19	f18
#define c20	f19
#define c21	f20
#define c22	f21
#define c23	f22
#define c24	f23
#define c25	f24
#define c26	f25
#define c27	f26
#define c28	f27
#define c29	f28
#define c30	f29
#define c31	f30
#define c32	f31

#define STACKSIZE 64

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

	slwi	LDA, LDA, BASE_SHIFT
	slwi	M8, M, 3 + BASE_SHIFT

	li	r8,  -8
	li	r9,  -4
	li	r10, -2

	and	B2, N, r8
	and	B3, N, r9
	and	B4, N, r10

	mullw	B2, B2, M
	mullw	B3, B3, M
	mullw	B4, B4, M

	slwi	B2, B2, BASE_SHIFT
	slwi	B3, B3, BASE_SHIFT
	slwi	B4, B4, BASE_SHIFT

	add	B2, B2, B
	add	B3, B3, B
	add	B4, B4, B

	cmpwi	cr0, M, 0
	ble-	.L999
	cmpwi	cr0, N, 0
	ble-	.L999

	subi	B2, B2, 2 * SIZE
	subi	B3, B3, 2 * SIZE
	subi	B4, B4, 2 * SIZE

	subi	M8, M8, 62 * SIZE

	li	INC,  1 * SIZE
	li	INC2, 2 * SIZE

	andi.	r0, A,   2 * SIZE - 1
	bne	.L100
	andi.	r0, LDA, 2 * SIZE - 1
	bne	.L100

	subi	A, A, 2 * SIZE
	srawi.	J,  M,  3
	ble	.L20
	.align 4

.L10:
	mr	AO1, A
	add	AO2, A,   LDA
	add	AO3, AO2, LDA
	add	AO4, AO3, LDA
	add	AO5, AO4, LDA
	add	AO6, AO5, LDA
	add	AO7, AO6, LDA
	add	AO8, AO7, LDA
	add	A,   AO8, LDA

	sub	B1, B, M8
	addi	B, B, 64 * SIZE

	srawi.	r0,  N,  3
	mtspr	CTR, r0
	ble	.L15
	.align 4

.L12:
	LFPDUX	c01,   AO1, INC2
	LFPDUX	c02,   AO1, INC2
	LFPDUX	c03,   AO1, INC2
	LFPDUX	c04,   AO1, INC2

	LFPDUX	c05,   AO2, INC2
	LFPDUX	c06,   AO2, INC2
	LFPDUX	c07,   AO2, INC2
	LFPDUX	c08,   AO2, INC2

 	LFPDUX	c09,   AO3, INC2
	LFPDUX	c10,   AO3, INC2
 	LFPDUX	c11,   AO3, INC2
	LFPDUX	c12,   AO3, INC2

	LFPDUX	c13,   AO4, INC2
	LFPDUX	c14,   AO4, INC2
	LFPDUX	c15,   AO4, INC2
	LFPDUX	c16,   AO4, INC2

	LFPDUX	c17,   AO5, INC2
	LFPDUX	c18,   AO5, INC2
	LFPDUX	c19,   AO5, INC2
	LFPDUX	c20,   AO5, INC2

	LFPDUX	c21,   AO6, INC2
	LFPDUX	c22,   AO6, INC2
	LFPDUX	c23,   AO6, INC2
	LFPDUX	c24,   AO6, INC2

 	LFPDUX	c25,   AO7, INC2
	LFPDUX	c26,   AO7, INC2
 	LFPDUX	c27,   AO7, INC2
	LFPDUX	c28,   AO7, INC2

	LFPDUX	c29,   AO8, INC2
	LFPDUX	c30,   AO8, INC2
	LFPDUX	c31,   AO8, INC2
	LFPDUX	c32,   AO8, INC2

	STFPDUX	c01,   B1, M8
	STFPDUX	c02,   B1, INC2
	STFPDUX	c03,   B1, INC2
	STFPDUX	c04,   B1, INC2
	STFPDUX	c05,   B1, INC2
	STFPDUX	c06,   B1, INC2
	STFPDUX	c07,   B1, INC2
	STFPDUX	c08,   B1, INC2

	STFPDUX	c09,   B1, INC2
	STFPDUX	c10,   B1, INC2
	STFPDUX	c11,   B1, INC2
	STFPDUX	c12,   B1, INC2
	STFPDUX	c13,   B1, INC2
	STFPDUX	c14,   B1, INC2
	STFPDUX	c15,   B1, INC2
	STFPDUX	c16,   B1, INC2

	STFPDUX	c17,   B1, INC2
	STFPDUX	c18,   B1, INC2
	STFPDUX	c19,   B1, INC2
	STFPDUX	c20,   B1, INC2
	STFPDUX	c21,   B1, INC2
	STFPDUX	c22,   B1, INC2
	STFPDUX	c23,   B1, INC2
	STFPDUX	c24,   B1, INC2

	STFPDUX	c25,   B1, INC2
	STFPDUX	c26,   B1, INC2
	STFPDUX	c27,   B1, INC2
	STFPDUX	c28,   B1, INC2
	STFPDUX	c29,   B1, INC2
	STFPDUX	c30,   B1, INC2
	STFPDUX	c31,   B1, INC2
	STFPDUX	c32,   B1, INC2
	bdnz	.L12
	.align 4

.L15:
	andi.	r0,  N,  7
	ble	.L19

	andi.	r0,  N,  4
	ble	.L16

	LFPDUX	c01,   AO1, INC2
	LFPDUX	c02,   AO1, INC2
	LFPDUX	c03,   AO2, INC2
	LFPDUX	c04,   AO2, INC2

 	LFPDUX	c05,   AO3, INC2
	LFPDUX	c06,   AO3, INC2
	LFPDUX	c07,   AO4, INC2
	LFPDUX	c08,   AO4, INC2

	LFPDUX	c09,   AO5, INC2
	LFPDUX	c10,   AO5, INC2
	LFPDUX	c11,   AO6, INC2
	LFPDUX	c12,   AO6, INC2

 	LFPDUX	c13,   AO7, INC2
	LFPDUX	c14,   AO7, INC2
	LFPDUX	c15,   AO8, INC2
	LFPDUX	c16,   AO8, INC2

	STFPDUX	c01,   B2, INC2
	STFPDUX	c02,   B2, INC2
	STFPDUX	c03,   B2, INC2
	STFPDUX	c04,   B2, INC2
	STFPDUX	c05,   B2, INC2
	STFPDUX	c06,   B2, INC2
	STFPDUX	c07,   B2, INC2
	STFPDUX	c08,   B2, INC2
	STFPDUX	c09,   B2, INC2
	STFPDUX	c10,   B2, INC2
	STFPDUX	c11,   B2, INC2
	STFPDUX	c12,   B2, INC2
	STFPDUX	c13,   B2, INC2
	STFPDUX	c14,   B2, INC2
	STFPDUX	c15,   B2, INC2
	STFPDUX	c16,   B2, INC2
	.align 4

.L16:
	andi.	r0,  N,  2
	ble	.L17

	LFPDUX	c01,   AO1, INC2
	LFPDUX	c03,   AO2, INC2
 	LFPDUX	c05,   AO3, INC2
	LFPDUX	c07,   AO4, INC2

	LFPDUX	c09,   AO5, INC2
	LFPDUX	c11,   AO6, INC2
 	LFPDUX	c13,   AO7, INC2
	LFPDUX	c15,   AO8, INC2

	STFPDUX	c01,   B3, INC2
	STFPDUX	c03,   B3, INC2
	STFPDUX	c05,   B3, INC2
	STFPDUX	c07,   B3, INC2
	STFPDUX	c09,   B3, INC2
	STFPDUX	c11,   B3, INC2
	STFPDUX	c13,   B3, INC2
	STFPDUX	c15,   B3, INC2
	.align 4

.L17:
	andi.	r0,  N,  1
	ble	.L19

	LFDUX	c01,   AO1, INC2
 	LFDUX	c02,   AO3, INC2
	LFDUX	c03,   AO5, INC2
 	LFDUX	c04,   AO7, INC2

	LFSDUX	c01,   AO2, INC2
	LFSDUX	c02,   AO4, INC2
	LFSDUX	c03,   AO6, INC2
	LFSDUX	c04,   AO8, INC2

	STFPDUX	c01,   B4, INC2
	STFPDUX	c02,   B4, INC2
	STFPDUX	c03,   B4, INC2
	STFPDUX	c04,   B4, INC2
	.align 4

.L19:
	addic.	J, J, -1
	bgt	.L10
	.align 4

.L20:
	andi.	J,  M,  4
	addi	M8, M8, 32 * SIZE
	ble	.L30

	mr	AO1, A
	add	AO2, A,   LDA
	add	AO3, AO2, LDA
	add	AO4, AO3, LDA
	add	A,   AO4, LDA

	sub	B1, B, M8
	addi	B, B, 32 * SIZE

	srawi.	r0,  N,  3
	mtspr	CTR, r0
	ble	.L25
	.align 4

.L22:
	LFPDUX	c01,   AO1, INC2
	LFPDUX	c02,   AO1, INC2
	LFPDUX	c03,   AO1, INC2
	LFPDUX	c04,   AO1, INC2

	LFPDUX	c05,   AO2, INC2
	LFPDUX	c06,   AO2, INC2
	LFPDUX	c07,   AO2, INC2
	LFPDUX	c08,   AO2, INC2

 	LFPDUX	c09,   AO3, INC2
	LFPDUX	c10,   AO3, INC2
 	LFPDUX	c11,   AO3, INC2
	LFPDUX	c12,   AO3, INC2

	LFPDUX	c13,   AO4, INC2
	LFPDUX	c14,   AO4, INC2
	LFPDUX	c15,   AO4, INC2
	LFPDUX	c16,   AO4, INC2

	STFPDUX	c01,   B1, M8
	STFPDUX	c02,   B1, INC2
	STFPDUX	c03,   B1, INC2
	STFPDUX	c04,   B1, INC2
	STFPDUX	c05,   B1, INC2
	STFPDUX	c06,   B1, INC2
	STFPDUX	c07,   B1, INC2
	STFPDUX	c08,   B1, INC2

	STFPDUX	c09,   B1, INC2
	STFPDUX	c10,   B1, INC2
	STFPDUX	c11,   B1, INC2
	STFPDUX	c12,   B1, INC2
	STFPDUX	c13,   B1, INC2
	STFPDUX	c14,   B1, INC2
	STFPDUX	c15,   B1, INC2
	STFPDUX	c16,   B1, INC2
	bdnz	.L22
	.align 4

.L25:
	andi.	r0,  N,  7
	ble	.L30

	andi.	r0,  N,  4
	ble	.L26

	LFPDUX	c01,   AO1, INC2
	LFPDUX	c02,   AO1, INC2
	LFPDUX	c03,   AO2, INC2
	LFPDUX	c04,   AO2, INC2

 	LFPDUX	c05,   AO3, INC2
	LFPDUX	c06,   AO3, INC2
	LFPDUX	c07,   AO4, INC2
	LFPDUX	c08,   AO4, INC2

	STFPDUX	c01,   B2, INC2
	STFPDUX	c02,   B2, INC2
	STFPDUX	c03,   B2, INC2
	STFPDUX	c04,   B2, INC2
	STFPDUX	c05,   B2, INC2
	STFPDUX	c06,   B2, INC2
	STFPDUX	c07,   B2, INC2
	STFPDUX	c08,   B2, INC2
	.align 4

.L26:
	andi.	r0,  N,  2
	ble	.L27

	LFPDUX	c01,   AO1, INC2
	LFPDUX	c03,   AO2, INC2
 	LFPDUX	c05,   AO3, INC2
	LFPDUX	c07,   AO4, INC2

	STFPDUX	c01,   B3, INC2
	STFPDUX	c03,   B3, INC2
	STFPDUX	c05,   B3, INC2
	STFPDUX	c07,   B3, INC2
	.align 4

.L27:
	andi.	r0,  N,  1
	ble	.L30

	LFDUX	c01,   AO1, INC2
	LFDUX	c02,   AO2, INC2
 	LFDUX	c03,   AO3, INC2
	LFDUX	c04,   AO4, INC2

	fsmfp	c01, c02
	fsmfp	c03, c04

	STFPDUX	c01,   B4, INC2
	STFPDUX	c03,   B4, INC2
	.align 4

.L30:
	andi.	J,  M,  2
	addi	M8, M8, 16 * SIZE
	ble	.L40

	mr	AO1, A
	add	AO2, A,   LDA
	add	A,   AO2, LDA

	sub	B1, B, M8
	addi	B, B, 16 * SIZE

	srawi.	r0,  N,  3
	mtspr	CTR, r0
	ble	.L35
	.align 4

.L32:
	LFPDUX	c01,   AO1, INC2
	LFPDUX	c02,   AO1, INC2
	LFPDUX	c03,   AO1, INC2
	LFPDUX	c04,   AO1, INC2

	LFPDUX	c05,   AO2, INC2
	LFPDUX	c06,   AO2, INC2
	LFPDUX	c07,   AO2, INC2
	LFPDUX	c08,   AO2, INC2

	STFPDUX	c01,   B1, M8
	STFPDUX	c02,   B1, INC2
	STFPDUX	c03,   B1, INC2
	STFPDUX	c04,   B1, INC2
	STFPDUX	c05,   B1, INC2
	STFPDUX	c06,   B1, INC2
	STFPDUX	c07,   B1, INC2
	STFPDUX	c08,   B1, INC2
	bdnz	.L32
	.align 4

.L35:
	andi.	r0,  N,  7
	ble	.L40

	andi.	r0,  N,  4
	ble	.L36

	LFPDUX	c01,   AO1, INC2
	LFPDUX	c02,   AO1, INC2
	LFPDUX	c03,   AO2, INC2
	LFPDUX	c04,   AO2, INC2

	STFPDUX	c01,   B2, INC2
	STFPDUX	c02,   B2, INC2
	STFPDUX	c03,   B2, INC2
	STFPDUX	c04,   B2, INC2
	.align 4

.L36:
	andi.	r0,  N,  2
	ble	.L37

	LFPDUX	c01,   AO1, INC2
	LFPDUX	c02,   AO2, INC2

	STFPDUX	c01,   B3, INC2
	STFPDUX	c02,   B3, INC2
	.align 4

.L37:
	andi.	r0,  N,  1
	ble	.L40

	LFDUX	c01,   AO1, INC2
	LFDUX	c02,   AO2, INC2

	fsmfp	c01, c02
	STFPDUX	c01,   B4, INC2
	.align 4

.L40:
	andi.	J,  M,  1
	addi	M8, M8, 8 * SIZE
	ble	.L999

	mr	AO1, A

	sub	B1, B, M8

	srawi.	r0,  N,  3
	mtspr	CTR, r0
	ble	.L45
	.align 4

.L42:
	LFPDUX	c01,   AO1, INC2
	LFPDUX	c02,   AO1, INC2
	LFPDUX	c03,   AO1, INC2
	LFPDUX	c04,   AO1, INC2

	STFPDUX	c01,   B1, M8
	STFPDUX	c02,   B1, INC2
	STFPDUX	c03,   B1, INC2
	STFPDUX	c04,   B1, INC2
	bdnz	.L42
	.align 4

.L45:
	andi.	r0,  N,  7
	ble	.L999

	andi.	r0,  N,  4
	ble	.L46

	LFPDUX	c01,   AO1, INC2
	LFPDUX	c02,   AO1, INC2

	STFPDUX	c01,   B2, INC2
	STFPDUX	c02,   B2, INC2
	.align 4

.L46:
	andi.	r0,  N,  2
	ble	.L47

	LFPDUX	c01,   AO1, INC2
	STFPDUX	c01,   B3, INC2
	.align 4

.L47:
	andi.	r0,  N,  1
	ble	.L999

	LFDX	c01,   AO1, INC2
	STFDX	c01,   B4, INC2
	b	.L999
	.align 4


.L100:
	subi	A, A, SIZE
	srawi.	J,  M,  3
	ble	.L120
	.align 4

.L110:
	mr	AO1, A
	add	AO2, A,   LDA
	add	AO3, AO2, LDA
	add	AO4, AO3, LDA
	add	AO5, AO4, LDA
	add	AO6, AO5, LDA
	add	AO7, AO6, LDA
	add	AO8, AO7, LDA
	add	A,   AO8, LDA

	sub	B1, B, M8
	addi	B, B, 64 * SIZE

	srawi.	r0,  N,  3
	mtspr	CTR, r0
	ble	.L115
	.align 4

.L112:
	LFDUX	c01,   AO1, INC
	LFDUX	c05,   AO2, INC
 	LFDUX	c09,   AO3, INC
	LFDUX	c13,   AO4, INC

	LFSDUX	c01,   AO1, INC
	LFSDUX	c05,   AO2, INC
	LFSDUX	c09,   AO3, INC
	LFSDUX	c13,   AO4, INC

	LFDUX	c02,   AO1, INC
	LFDUX	c06,   AO2, INC
	LFDUX	c10,   AO3, INC
	LFDUX	c14,   AO4, INC

	LFSDUX	c02,   AO1, INC
	LFSDUX	c06,   AO2, INC
	LFSDUX	c10,   AO3, INC
	LFSDUX	c14,   AO4, INC

	LFDUX	c03,   AO1, INC
	LFDUX	c07,   AO2, INC
 	LFDUX	c11,   AO3, INC
	LFDUX	c15,   AO4, INC

	LFSDUX	c03,   AO1, INC
	LFSDUX	c07,   AO2, INC
	LFSDUX	c11,   AO3, INC
	LFSDUX	c15,   AO4, INC

	LFDUX	c04,   AO1, INC
	LFDUX	c08,   AO2, INC
	LFDUX	c12,   AO3, INC
	LFDUX	c16,   AO4, INC

	LFSDUX	c04,   AO1, INC
	LFSDUX	c08,   AO2, INC
	LFSDUX	c12,   AO3, INC
	LFSDUX	c16,   AO4, INC


	LFDUX	c17,   AO5, INC
	LFDUX	c21,   AO6, INC
 	LFDUX	c25,   AO7, INC
	LFDUX	c29,   AO8, INC

	LFSDUX	c17,   AO5, INC
	LFSDUX	c21,   AO6, INC
	LFSDUX	c25,   AO7, INC
	LFSDUX	c29,   AO8, INC

	LFDUX	c18,   AO5, INC
	LFDUX	c22,   AO6, INC
	LFDUX	c26,   AO7, INC
	LFDUX	c30,   AO8, INC

	LFSDUX	c18,   AO5, INC
	LFSDUX	c22,   AO6, INC
	LFSDUX	c26,   AO7, INC
	LFSDUX	c30,   AO8, INC

	LFDUX	c19,   AO5, INC
	LFDUX	c23,   AO6, INC
 	LFDUX	c27,   AO7, INC
	LFDUX	c31,   AO8, INC

	LFSDUX	c19,   AO5, INC
	LFSDUX	c23,   AO6, INC
	LFSDUX	c27,   AO7, INC
	LFSDUX	c31,   AO8, INC

	LFDUX	c20,   AO5, INC
	LFDUX	c24,   AO6, INC
	LFDUX	c28,   AO7, INC
	LFDUX	c32,   AO8, INC

	LFSDUX	c20,   AO5, INC
	LFSDUX	c24,   AO6, INC
	LFSDUX	c28,   AO7, INC
	LFSDUX	c32,   AO8, INC

	STFPDUX	c01,   B1, M8
	STFPDUX	c02,   B1, INC2
	STFPDUX	c03,   B1, INC2
	STFPDUX	c04,   B1, INC2
	STFPDUX	c05,   B1, INC2
	STFPDUX	c06,   B1, INC2
	STFPDUX	c07,   B1, INC2
	STFPDUX	c08,   B1, INC2

	STFPDUX	c09,   B1, INC2
	STFPDUX	c10,   B1, INC2
	STFPDUX	c11,   B1, INC2
	STFPDUX	c12,   B1, INC2
	STFPDUX	c13,   B1, INC2
	STFPDUX	c14,   B1, INC2
	STFPDUX	c15,   B1, INC2
	STFPDUX	c16,   B1, INC2

	STFPDUX	c17,   B1, INC2
	STFPDUX	c18,   B1, INC2
	STFPDUX	c19,   B1, INC2
	STFPDUX	c20,   B1, INC2
	STFPDUX	c21,   B1, INC2
	STFPDUX	c22,   B1, INC2
	STFPDUX	c23,   B1, INC2
	STFPDUX	c24,   B1, INC2

	STFPDUX	c25,   B1, INC2
	STFPDUX	c26,   B1, INC2
	STFPDUX	c27,   B1, INC2
	STFPDUX	c28,   B1, INC2
	STFPDUX	c29,   B1, INC2
	STFPDUX	c30,   B1, INC2
	STFPDUX	c31,   B1, INC2
	STFPDUX	c32,   B1, INC2
	bdnz	.L112
	.align 4

.L115:
	andi.	r0,  N,  7
	ble	.L119

	andi.	r0,  N,  4
	ble	.L116

	LFDUX	c01,   AO1, INC
	LFDUX	c03,   AO2, INC
 	LFDUX	c05,   AO3, INC
	LFDUX	c07,   AO4, INC

	LFSDUX	c01,   AO1, INC
	LFSDUX	c03,   AO2, INC
	LFSDUX	c05,   AO3, INC
	LFSDUX	c07,   AO4, INC

	LFDUX	c02,   AO1, INC
	LFDUX	c04,   AO2, INC
	LFDUX	c06,   AO3, INC
	LFDUX	c08,   AO4, INC

	LFSDUX	c02,   AO1, INC
	LFSDUX	c04,   AO2, INC
	LFSDUX	c06,   AO3, INC
	LFSDUX	c08,   AO4, INC

	LFDUX	c09,   AO5, INC
	LFDUX	c11,   AO6, INC
 	LFDUX	c13,   AO7, INC
	LFDUX	c15,   AO8, INC

	LFSDUX	c09,   AO5, INC
	LFSDUX	c11,   AO6, INC
	LFSDUX	c13,   AO7, INC
	LFSDUX	c15,   AO8, INC

	LFDUX	c10,   AO5, INC
	LFDUX	c12,   AO6, INC
	LFDUX	c14,   AO7, INC
	LFDUX	c16,   AO8, INC

	LFSDUX	c10,   AO5, INC
	LFSDUX	c12,   AO6, INC
	LFSDUX	c14,   AO7, INC
	LFSDUX	c16,   AO8, INC

	STFPDUX	c01,   B2, INC2
	STFPDUX	c02,   B2, INC2
	STFPDUX	c03,   B2, INC2
	STFPDUX	c04,   B2, INC2
	STFPDUX	c05,   B2, INC2
	STFPDUX	c06,   B2, INC2
	STFPDUX	c07,   B2, INC2
	STFPDUX	c08,   B2, INC2
	STFPDUX	c09,   B2, INC2
	STFPDUX	c10,   B2, INC2
	STFPDUX	c11,   B2, INC2
	STFPDUX	c12,   B2, INC2
	STFPDUX	c13,   B2, INC2
	STFPDUX	c14,   B2, INC2
	STFPDUX	c15,   B2, INC2
	STFPDUX	c16,   B2, INC2
	.align 4

.L116:
	andi.	r0,  N,  2
	ble	.L117

	LFDUX	c01,   AO1, INC
	LFDUX	c03,   AO2, INC
 	LFDUX	c05,   AO3, INC
	LFDUX	c07,   AO4, INC

	LFSDUX	c01,   AO1, INC
	LFSDUX	c03,   AO2, INC
	LFSDUX	c05,   AO3, INC
	LFSDUX	c07,   AO4, INC

	LFDUX	c09,   AO5, INC
	LFDUX	c11,   AO6, INC
 	LFDUX	c13,   AO7, INC
	LFDUX	c15,   AO8, INC

	LFSDUX	c09,   AO5, INC
	LFSDUX	c11,   AO6, INC
	LFSDUX	c13,   AO7, INC
	LFSDUX	c15,   AO8, INC

	STFPDUX	c01,   B3, INC2
	STFPDUX	c03,   B3, INC2
	STFPDUX	c05,   B3, INC2
	STFPDUX	c07,   B3, INC2
	STFPDUX	c09,   B3, INC2
	STFPDUX	c11,   B3, INC2
	STFPDUX	c13,   B3, INC2
	STFPDUX	c15,   B3, INC2
	.align 4

.L117:
	andi.	r0,  N,  1
	ble	.L119

	LFDUX	c01,   AO1, INC
 	LFDUX	c02,   AO3, INC
	LFDUX	c03,   AO5, INC
 	LFDUX	c04,   AO7, INC

	LFSDUX	c01,   AO2, INC
	LFSDUX	c02,   AO4, INC
	LFSDUX	c03,   AO6, INC
	LFSDUX	c04,   AO8, INC

	STFPDUX	c01,   B4, INC2
	STFPDUX	c02,   B4, INC2
	STFPDUX	c03,   B4, INC2
	STFPDUX	c04,   B4, INC2
	.align 4

.L119:
	addic.	J, J, -1
	bgt	.L110
	.align 4

.L120:
	andi.	J,  M,  4
	addi	M8, M8, 32 * SIZE
	ble	.L130

	mr	AO1, A
	add	AO2, A,   LDA
	add	AO3, AO2, LDA
	add	AO4, AO3, LDA
	add	A,   AO4, LDA

	sub	B1, B, M8
	addi	B, B, 32 * SIZE

	srawi.	r0,  N,  3
	mtspr	CTR, r0
	ble	.L125
	.align 4

.L122:
	LFDUX	c01,   AO1, INC
	LFDUX	c05,   AO2, INC
 	LFDUX	c09,   AO3, INC
	LFDUX	c13,   AO4, INC

	LFSDUX	c01,   AO1, INC
	LFSDUX	c05,   AO2, INC
	LFSDUX	c09,   AO3, INC
	LFSDUX	c13,   AO4, INC

	LFDUX	c02,   AO1, INC
	LFDUX	c06,   AO2, INC
	LFDUX	c10,   AO3, INC
	LFDUX	c14,   AO4, INC

	LFSDUX	c02,   AO1, INC
	LFSDUX	c06,   AO2, INC
	LFSDUX	c10,   AO3, INC
	LFSDUX	c14,   AO4, INC

	LFDUX	c03,   AO1, INC
	LFDUX	c07,   AO2, INC
 	LFDUX	c11,   AO3, INC
	LFDUX	c15,   AO4, INC

	LFSDUX	c03,   AO1, INC
	LFSDUX	c07,   AO2, INC
	LFSDUX	c11,   AO3, INC
	LFSDUX	c15,   AO4, INC

	LFDUX	c04,   AO1, INC
	LFDUX	c08,   AO2, INC
	LFDUX	c12,   AO3, INC
	LFDUX	c16,   AO4, INC

	LFSDUX	c04,   AO1, INC
	LFSDUX	c08,   AO2, INC
	LFSDUX	c12,   AO3, INC
	LFSDUX	c16,   AO4, INC

	STFPDUX	c01,   B1, M8
	STFPDUX	c02,   B1, INC2
	STFPDUX	c03,   B1, INC2
	STFPDUX	c04,   B1, INC2
	STFPDUX	c05,   B1, INC2
	STFPDUX	c06,   B1, INC2
	STFPDUX	c07,   B1, INC2
	STFPDUX	c08,   B1, INC2

	STFPDUX	c09,   B1, INC2
	STFPDUX	c10,   B1, INC2
	STFPDUX	c11,   B1, INC2
	STFPDUX	c12,   B1, INC2
	STFPDUX	c13,   B1, INC2
	STFPDUX	c14,   B1, INC2
	STFPDUX	c15,   B1, INC2
	STFPDUX	c16,   B1, INC2
	bdnz	.L122
	.align 4

.L125:
	andi.	r0,  N,  7
	ble	.L130

	andi.	r0,  N,  4
	ble	.L126

	LFDUX	c01,   AO1, INC
	LFDUX	c03,   AO2, INC
 	LFDUX	c05,   AO3, INC
	LFDUX	c07,   AO4, INC

	LFSDUX	c01,   AO1, INC
	LFSDUX	c03,   AO2, INC
	LFSDUX	c05,   AO3, INC
	LFSDUX	c07,   AO4, INC

	LFDUX	c02,   AO1, INC
	LFDUX	c04,   AO2, INC
	LFDUX	c06,   AO3, INC
	LFDUX	c08,   AO4, INC

	LFSDUX	c02,   AO1, INC
	LFSDUX	c04,   AO2, INC
	LFSDUX	c06,   AO3, INC
	LFSDUX	c08,   AO4, INC

	STFPDUX	c01,   B2, INC2
	STFPDUX	c02,   B2, INC2
	STFPDUX	c03,   B2, INC2
	STFPDUX	c04,   B2, INC2
	STFPDUX	c05,   B2, INC2
	STFPDUX	c06,   B2, INC2
	STFPDUX	c07,   B2, INC2
	STFPDUX	c08,   B2, INC2
	.align 4

.L126:
	andi.	r0,  N,  2
	ble	.L127

	LFDUX	c01,   AO1, INC
	LFDUX	c03,   AO2, INC
 	LFDUX	c05,   AO3, INC
	LFDUX	c07,   AO4, INC

	LFSDUX	c01,   AO1, INC
	LFSDUX	c03,   AO2, INC
	LFSDUX	c05,   AO3, INC
	LFSDUX	c07,   AO4, INC

	STFPDUX	c01,   B3, INC2
	STFPDUX	c03,   B3, INC2
	STFPDUX	c05,   B3, INC2
	STFPDUX	c07,   B3, INC2
	.align 4

.L127:
	andi.	r0,  N,  1
	ble	.L130

	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO2, INC
 	LFDUX	c03,   AO3, INC
	LFDUX	c04,   AO4, INC

	fsmfp	c01, c02
	fsmfp	c03, c04

	STFPDUX	c01,   B4, INC2
	STFPDUX	c03,   B4, INC2
	.align 4

.L130:
	andi.	J,  M,  2
	addi	M8, M8, 16 * SIZE
	ble	.L140

	mr	AO1, A
	add	AO2, A,   LDA
	add	A,   AO2, LDA

	sub	B1, B, M8
	addi	B, B, 16 * SIZE

	srawi.	r0,  N,  3
	mtspr	CTR, r0
	ble	.L135
	.align 4

.L132:
	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO1, INC
	LFDUX	c03,   AO1, INC
	LFDUX	c04,   AO1, INC
	LFDUX	c05,   AO1, INC
	LFDUX	c06,   AO1, INC
	LFDUX	c07,   AO1, INC
	LFDUX	c08,   AO1, INC

	LFDUX	c09,   AO2, INC
	LFDUX	c10,   AO2, INC
	LFDUX	c11,   AO2, INC
	LFDUX	c12,   AO2, INC
	LFDUX	c13,   AO2, INC
	LFDUX	c14,   AO2, INC
	LFDUX	c15,   AO2, INC
	LFDUX	c16,   AO2, INC

	fsmfp	c01, c02
	fsmfp	c03, c04
	fsmfp	c05, c06
	fsmfp	c07, c08
	fsmfp	c09, c10
	fsmfp	c11, c12
	fsmfp	c13, c14
	fsmfp	c15, c16

	STFPDUX	c01,   B1, M8
	STFPDUX	c03,   B1, INC2
	STFPDUX	c05,   B1, INC2
	STFPDUX	c07,   B1, INC2
	STFPDUX	c09,   B1, INC2
	STFPDUX	c11,   B1, INC2
	STFPDUX	c13,   B1, INC2
	STFPDUX	c15,   B1, INC2
	bdnz	.L132
	.align 4

.L135:
	andi.	r0,  N,  7
	ble	.L140

	andi.	r0,  N,  4
	ble	.L136

	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO1, INC
	LFDUX	c03,   AO1, INC
	LFDUX	c04,   AO1, INC

	LFDUX	c09,   AO2, INC
	LFDUX	c10,   AO2, INC
	LFDUX	c11,   AO2, INC
	LFDUX	c12,   AO2, INC

	fsmfp	c01, c02
	fsmfp	c03, c04
	fsmfp	c09, c10
	fsmfp	c11, c12

	STFPDUX	c01,   B2, INC2
	STFPDUX	c03,   B2, INC2
	STFPDUX	c09,   B2, INC2
	STFPDUX	c11,   B2, INC2
	.align 4

.L136:
	andi.	r0,  N,  2
	ble	.L137

	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO1, INC
	LFDUX	c09,   AO2, INC
	LFDUX	c10,   AO2, INC

	fsmfp	c01, c02
	fsmfp	c09, c10

	STFPDUX	c01,   B3, INC2
	STFPDUX	c09,   B3, INC2
	.align 4

.L137:
	andi.	r0,  N,  1
	ble	.L140

	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO2, INC

	fsmfp	c01, c02
	STFPDUX	c01,   B4, INC2
	.align 4

.L140:
	andi.	J,  M,  1
	addi	M8, M8, 8 * SIZE
	ble	.L999

	mr	AO1, A

	sub	B1, B, M8

	srawi.	r0,  N,  3
	mtspr	CTR, r0
	ble	.L145
	.align 4

.L142:
	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO1, INC
	LFDUX	c03,   AO1, INC
	LFDUX	c04,   AO1, INC
	LFDUX	c05,   AO1, INC
	LFDUX	c06,   AO1, INC
	LFDUX	c07,   AO1, INC
	LFDUX	c08,   AO1, INC

	fsmfp	c01, c02
	fsmfp	c03, c04
	fsmfp	c05, c06
	fsmfp	c07, c08

	STFPDUX	c01,   B1, M8
	STFPDUX	c03,   B1, INC2
	STFPDUX	c05,   B1, INC2
	STFPDUX	c07,   B1, INC2
	bdnz	.L142
	.align 4

.L145:
	andi.	r0,  N,  7
	ble	.L999

	andi.	r0,  N,  4
	ble	.L146

	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO1, INC
	LFDUX	c03,   AO1, INC
	LFDUX	c04,   AO1, INC

	fsmfp	c01, c02
	fsmfp	c03, c04

	STFPDUX	c01,   B2, INC2
	STFPDUX	c03,   B2, INC2
	.align 4

.L146:
	andi.	r0,  N,  2
	ble	.L147

	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO1, INC

	fsmfp	c01, c02

	STFPDUX	c01,   B3, INC2
	.align 4

.L147:
	andi.	r0,  N,  1
	ble	.L999

	LFDX	c01,   AO1, INC
	STFDX	c01,   B4, INC2
	.align 4

.L999:
	addi	SP, SP, -4

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
