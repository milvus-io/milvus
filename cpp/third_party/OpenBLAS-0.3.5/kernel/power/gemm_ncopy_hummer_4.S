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

#define	sel_p	f16
#define	sel_s	f17

#define c17	f18
#define c18	f19


	PROLOGUE
	PROFCODE

	li	r0, -16

	stfpdux	f14, SP, r0
	stfpdux	f15, SP, r0

	stfpdux	f16, SP, r0
	stfpdux	f17, SP, r0
	stfpdux	f18, SP, r0
	stfpdux	f19, SP, r0

	stwu	r31,  -4(SP)
	stwu	r30,  -4(SP)

	lis	r9,   0x3f80
	lis	r10,  0xbf80

	stwu	r9,    -4(SP)
	stwu	r10,   -4(SP)
	stwu	r10,   -4(SP)
	stwu	r9,    -4(SP)

	slwi	LDA, LDA, BASE_SHIFT

	cmpwi	cr0, M, 0
	ble-	.L99
	cmpwi	cr0, N, 0
	ble-	.L99

	andi.	r0, A,   2 * SIZE - 1
	bne	.L100
	andi.	r0, LDA, 2 * SIZE - 1
	bne	.L100

	li	r0, 8
	addi	SP, SP, -8

	lfpsux	sel_p, SP, r0
	lfpsux	sel_s, SP, r0

	li	INC,  1 * SIZE
	li	INC2, 2 * SIZE

	subi	A, A, 2 * SIZE
	subi	B, B, 2 * SIZE

	srawi.	J,  N,  2
	ble	.L20
	.align 4
.L11:
	mr	AO1, A
	add	AO2, A,   LDA
	add	AO3, AO2, LDA
	add	AO4, AO3, LDA
	add	A,   AO4, LDA

	srawi.	r0,  M,  3
	mtspr	CTR, r0
	ble	.L15
	.align 4

.L12:
	LFPDUX	c01,   AO1, INC2
	LFXDUX	c05,   AO2, INC2
	LFPDUX	c09,   AO3, INC2
	LFXDUX	c13,   AO4, INC2

	LFPDUX	c02,   AO1, INC2
	LFXDUX	c06,   AO2, INC2
	LFPDUX	c10,   AO3, INC2
	LFXDUX	c14,   AO4, INC2

	LFPDUX	c03,   AO1, INC2
	LFXDUX	c07,   AO2, INC2
	LFPDUX	c11,   AO3, INC2
	LFXDUX	c15,   AO4, INC2

	LFPDUX	c04,   AO1, INC2
	LFXDUX	c08,   AO2, INC2
	LFPDUX	c12,   AO3, INC2
	LFXDUX	c16,   AO4, INC2

	fpsel	c17, sel_p, c01, c05
	fpsel	c18, sel_p, c09, c13
	fpsel	c01, sel_s, c01, c05
	fpsel	c05, sel_s, c09, c13

	fpsel	c09, sel_p, c02, c06
	fpsel	c13, sel_p, c10, c14
	STFPDUX	c17,   B, INC2
	fpsel	c02, sel_s, c02, c06
 	STFPDUX	c18,   B, INC2
	fpsel	c06, sel_s, c10, c14
	STFXDUX	c01,   B, INC2

	fpsel	c10, sel_p, c03, c07
	STFXDUX	c05,   B, INC2
	fpsel	c14, sel_p, c11, c15
	STFPDUX	c09,   B, INC2
	fpsel	c03, sel_s, c03, c07
	STFPDUX	c13,   B, INC2
	fpsel	c07, sel_s, c11, c15
	STFXDUX	c02,   B, INC2

	fpsel	c11, sel_p, c04, c08
	STFXDUX	c06,   B, INC2
	fpsel	c15, sel_p, c12, c16
	STFPDUX	c10,   B, INC2
	fpsel	c04, sel_s, c04, c08
	STFPDUX	c14,   B, INC2
	fpsel	c08, sel_s, c12, c16
	STFXDUX	c03,   B, INC2

	STFXDUX	c07,   B, INC2
	STFPDUX	c11,   B, INC2
	STFPDUX	c15,   B, INC2
	STFXDUX	c04,   B, INC2
	STFXDUX	c08,   B, INC2
	bdnz	.L12
	.align 4

.L15:
	andi.	r0,  M,  7
	ble	.L19

	andi.	r0,  M,  4
	beq	.L16

	LFPDUX	c01,   AO1, INC2
	LFXDUX	c05,   AO2, INC2
	LFPDUX	c09,   AO3, INC2
	LFXDUX	c13,   AO4, INC2

	LFPDUX	c02,   AO1, INC2
	LFXDUX	c06,   AO2, INC2
	LFPDUX	c10,   AO3, INC2
	LFXDUX	c14,   AO4, INC2

	fpsel	c17, sel_p, c01, c05
	fpsel	c18, sel_p, c09, c13
	fpsel	c01, sel_s, c01, c05
	fpsel	c05, sel_s, c09, c13

	fpsel	c09, sel_p, c02, c06
	fpsel	c13, sel_p, c10, c14
	STFPDUX	c17,   B, INC2
	fpsel	c02, sel_s, c02, c06
	STFPDUX	c18,   B, INC2
	fpsel	c06, sel_s, c10, c14
	STFXDUX	c01,   B, INC2
	STFXDUX	c05,   B, INC2
	STFPDUX	c09,   B, INC2
	STFPDUX	c13,   B, INC2
	STFXDUX	c02,   B, INC2
	STFXDUX	c06,   B, INC2
	.align 4

.L16:
	andi.	r0,  M,  2
	beq	.L17

	LFPDUX	c01,   AO1, INC2
	LFXDUX	c05,   AO2, INC2
	LFPDUX	c09,   AO3, INC2
	LFXDUX	c13,   AO4, INC2

	fpsel	c17, sel_p, c01, c05
	fpsel	c18, sel_p, c09, c13
	fpsel	c01, sel_s, c01, c05
	fpsel	c05, sel_s, c09, c13

	STFPDUX	c17,   B, INC2
	STFPDUX	c18,   B, INC2
	STFXDUX	c01,   B, INC2
	STFXDUX	c05,   B, INC2
	.align 4

.L17:
	andi.	r0,  M,  1
	beq	.L19

	LFDUX	c01,   AO1, INC2
	LFDUX	c02,   AO2, INC2
	LFDUX	c03,   AO3, INC2
	LFDUX	c04,   AO4, INC2

	fsmfp	c01, c02
	fsmfp	c03, c04

	STFPDUX	c01,   B, INC2
	STFPDUX	c03,   B, INC2
	.align 4

.L19:
	addic.	J, J, -1
	bgt	.L11
	.align 4

.L20:
	andi.	J,  N,  2
	ble	.L30

	mr	AO1, A
	add	AO2, A,   LDA
	add	A,   AO2, LDA

	srawi.	r0,  M,  3
	mtspr	CTR, r0
	ble	.L25
	.align 4

.L22:
	LFPDUX	c01,   AO1, INC2
	LFXDUX	c05,   AO2, INC2
	LFPDUX	c02,   AO1, INC2
	LFXDUX	c06,   AO2, INC2

	LFPDUX	c03,   AO1, INC2
	LFXDUX	c07,   AO2, INC2
	LFPDUX	c04,   AO1, INC2
	LFXDUX	c08,   AO2, INC2

	fpsel	c17, sel_p, c01, c05
	fpsel	c01, sel_s, c01, c05
	fpsel	c09, sel_p, c02, c06
	fpsel	c02, sel_s, c02, c06

	fpsel	c10, sel_p, c03, c07
	fpsel	c03, sel_s, c03, c07
	STFPDUX	c17,   B, INC2
	fpsel	c11, sel_p, c04, c08
	STFXDUX	c01,   B, INC2
	fpsel	c04, sel_s, c04, c08
	STFPDUX	c09,   B, INC2

	STFXDUX	c02,   B, INC2
	STFPDUX	c10,   B, INC2
	STFXDUX	c03,   B, INC2
	STFPDUX	c11,   B, INC2
	STFXDUX	c04,   B, INC2
	bdnz	.L22
	.align 4

.L25:
	andi.	r0,  M,  7
	ble	.L30

	andi.	r0,  M,  4
	beq	.L26

	LFPDUX	c01,   AO1, INC2
	LFXDUX	c05,   AO2, INC2
	LFPDUX	c02,   AO1, INC2
	LFXDUX	c06,   AO2, INC2

	fpsel	c17, sel_p, c01, c05
	fpsel	c01, sel_s, c01, c05
	fpsel	c09, sel_p, c02, c06
	fpsel	c02, sel_s, c02, c06

	STFPDUX	c17,   B, INC2
	STFXDUX	c01,   B, INC2
	STFPDUX	c09,   B, INC2
	STFXDUX	c02,   B, INC2
	.align 4

.L26:
	andi.	r0,  M,  2
	beq	.L27

	LFPDUX	c01,   AO1, INC2
	LFXDUX	c05,   AO2, INC2

	fpsel	c17, sel_p, c01, c05
	fpsel	c01, sel_s, c01, c05

	STFPDUX	c17,   B, INC2
	STFXDUX	c01,   B, INC2
	.align 4

.L27:
	andi.	r0,  M,  1
	beq	.L30

	LFDUX	c01,   AO1, INC2
	LFDUX	c02,   AO2, INC2

	fsmfp	c01, c02
	STFPDUX	c01,   B, INC2
	.align 4

.L30:
	andi.	J,  N,  1
	ble	.L99

	mr	AO1, A

	srawi.	r0,  M,  3
	mtspr	CTR, r0
	ble	.L35
	.align 4

.L32:
	LFPDUX	c01,   AO1, INC2
	LFPDUX	c02,   AO1, INC2
	LFPDUX	c03,   AO1, INC2
	LFPDUX	c04,   AO1, INC2

	STFPDUX	c01,   B, INC2
	STFPDUX	c02,   B, INC2
	STFPDUX	c03,   B, INC2
	STFPDUX	c04,   B, INC2
	bdnz	.L32
	.align 4

.L35:
	andi.	r0,  M,  7
	ble	.L99

	andi.	r0,  M,  4
	beq	.L36

	LFPDUX	c01,   AO1, INC2
	LFPDUX	c02,   AO1, INC2

	STFPDUX	c01,   B, INC2
	STFPDUX	c02,   B, INC2
	.align 4

.L36:
	andi.	r0,  M,  2
	beq	.L37

	LFPDUX	c01,   AO1, INC2

	STFPDUX	c01,   B, INC2
	.align 4

.L37:
	andi.	r0,  M,  1
	beq	.L99

	LFDX	c01,   AO1, INC2
	STFDX	c01,   B,  INC2
	.align 4

.L99:
	addi	SP, SP, 4

	lwzu	r30,   4(SP)
	lwzu	r31,   4(SP)

	subi	SP, SP, 12
	li	r0, 16

	lfpdux	f19, SP, r0
	lfpdux	f18, SP, r0
	lfpdux	f17, SP, r0
	lfpdux	f16, SP, r0

	lfpdux	f15, SP, r0
	lfpdux	f14, SP, r0
	addi	SP, SP, 16
	blr
	.align 4

.L100:
	li	INC,  1 * SIZE
	li	INC2, 2 * SIZE

	subi	A, A, 1 * SIZE
	subi	B, B, 2 * SIZE

	srawi.	J,  N,  2
	ble	.L120
	.align 4
.L111:
	mr	AO1, A
	add	AO2, A,   LDA
	add	AO3, AO2, LDA
	add	AO4, AO3, LDA
	add	A,   AO4, LDA

	srawi.	r0,  M,  3
	mtspr	CTR, r0
	ble	.L115
	.align 4

.L112:
	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO1, INC
	LFDUX	c03,   AO1, INC
	LFDUX	c04,   AO1, INC

	LFDUX	c09,   AO1, INC
	LFDUX	c10,   AO1, INC
	LFDUX	c11,   AO1, INC
	LFDUX	c12,   AO1, INC

	LFSDUX	c01,   AO2, INC
	LFSDUX	c02,   AO2, INC
	LFSDUX	c03,   AO2, INC
	LFSDUX	c04,   AO2, INC

	LFSDUX	c09,   AO2, INC
	LFSDUX	c10,   AO2, INC
	LFSDUX	c11,   AO2, INC
	LFSDUX	c12,   AO2, INC

	LFDUX	c05,   AO3, INC
	LFDUX	c06,   AO3, INC
	LFDUX	c07,   AO3, INC
	LFDUX	c08,   AO3, INC

	LFDUX	c13,   AO3, INC
	LFDUX	c14,   AO3, INC
	LFDUX	c15,   AO3, INC
	LFDUX	c16,   AO3, INC

	LFSDUX	c05,   AO4, INC
	LFSDUX	c06,   AO4, INC
	LFSDUX	c07,   AO4, INC
	LFSDUX	c08,   AO4, INC

	LFSDUX	c13,   AO4, INC
	LFSDUX	c14,   AO4, INC
	LFSDUX	c15,   AO4, INC
	LFSDUX	c16,   AO4, INC

	STFPDUX	c01,   B, INC2
	STFPDUX	c05,   B, INC2
	STFPDUX	c02,   B, INC2
	STFPDUX	c06,   B, INC2
	STFPDUX	c03,   B, INC2
	STFPDUX	c07,   B, INC2
	STFPDUX	c04,   B, INC2
	STFPDUX	c08,   B, INC2

	STFPDUX	c09,   B, INC2
	STFPDUX	c13,   B, INC2
	STFPDUX	c10,   B, INC2
	STFPDUX	c14,   B, INC2
	STFPDUX	c11,   B, INC2
	STFPDUX	c15,   B, INC2
	STFPDUX	c12,   B, INC2
	STFPDUX	c16,   B, INC2
	bdnz	.L112
	.align 4

.L115:
	andi.	r0,  M,  7
	ble	.L119

	andi.	r0,  M,  4
	beq	.L116

	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO1, INC
	LFDUX	c03,   AO1, INC
	LFDUX	c04,   AO1, INC

	LFSDUX	c01,   AO2, INC
	LFSDUX	c02,   AO2, INC
	LFSDUX	c03,   AO2, INC
	LFSDUX	c04,   AO2, INC

	LFDUX	c05,   AO3, INC
	LFDUX	c06,   AO3, INC
	LFDUX	c07,   AO3, INC
	LFDUX	c08,   AO3, INC

	LFSDUX	c05,   AO4, INC
	LFSDUX	c06,   AO4, INC
	LFSDUX	c07,   AO4, INC
	LFSDUX	c08,   AO4, INC

	STFPDUX	c01,   B, INC2
	STFPDUX	c05,   B, INC2
	STFPDUX	c02,   B, INC2
	STFPDUX	c06,   B, INC2
	STFPDUX	c03,   B, INC2
	STFPDUX	c07,   B, INC2
	STFPDUX	c04,   B, INC2
	STFPDUX	c08,   B, INC2
	.align 4

.L116:
	andi.	r0,  M,  2
	beq	.L117

	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO1, INC

	LFSDUX	c01,   AO2, INC
	LFSDUX	c02,   AO2, INC

	LFDUX	c05,   AO3, INC
	LFDUX	c06,   AO3, INC

	LFSDUX	c05,   AO4, INC
	LFSDUX	c06,   AO4, INC

	STFPDUX	c01,   B, INC2
	STFPDUX	c05,   B, INC2
	STFPDUX	c02,   B, INC2
	STFPDUX	c06,   B, INC2
	.align 4

.L117:
	andi.	r0,  M,  1
	beq	.L119

	LFDUX	c01,   AO1, INC
	LFDUX	c05,   AO3, INC

	nop
	nop

	LFSDUX	c01,   AO2, INC
	LFSDUX	c05,   AO4, INC

	STFPDUX	c01,   B, INC2
	STFPDUX	c05,   B, INC2
	.align 4

.L119:
	addic.	J, J, -1
	bgt	.L111
	.align 4

.L120:
	andi.	J,  N,  2
	ble	.L130

	mr	AO1, A
	add	AO2, A,   LDA
	add	A,   AO2, LDA

	srawi.	r0,  M,  3
	mtspr	CTR, r0
	ble	.L125
	.align 4

.L122:
	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO1, INC
	LFDUX	c03,   AO1, INC
	LFDUX	c04,   AO1, INC

	LFDUX	c09,   AO1, INC
	LFDUX	c10,   AO1, INC
	LFDUX	c11,   AO1, INC
	LFDUX	c12,   AO1, INC

	LFSDUX	c01,   AO2, INC
	LFSDUX	c02,   AO2, INC
	LFSDUX	c03,   AO2, INC
	LFSDUX	c04,   AO2, INC

	LFSDUX	c09,   AO2, INC
	LFSDUX	c10,   AO2, INC
	LFSDUX	c11,   AO2, INC
	LFSDUX	c12,   AO2, INC

	STFPDUX	c01,   B, INC2
	STFPDUX	c02,   B, INC2
	STFPDUX	c03,   B, INC2
	STFPDUX	c04,   B, INC2

	STFPDUX	c09,   B, INC2
	STFPDUX	c10,   B, INC2
	STFPDUX	c11,   B, INC2
	STFPDUX	c12,   B, INC2
	bdnz	.L122
	.align 4

.L125:
	andi.	r0,  M,  7
	ble	.L130

	andi.	r0,  M,  4
	beq	.L126

	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO1, INC
	LFDUX	c03,   AO1, INC
	LFDUX	c04,   AO1, INC

	LFSDUX	c01,   AO2, INC
	LFSDUX	c02,   AO2, INC
	LFSDUX	c03,   AO2, INC
	LFSDUX	c04,   AO2, INC

	STFPDUX	c01,   B, INC2
	STFPDUX	c02,   B, INC2
	STFPDUX	c03,   B, INC2
	STFPDUX	c04,   B, INC2
	.align 4

.L126:
	andi.	r0,  M,  2
	beq	.L127

	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO1, INC

	LFSDUX	c01,   AO2, INC
	LFSDUX	c02,   AO2, INC

	STFPDUX	c01,   B, INC2
	STFPDUX	c02,   B, INC2
	.align 4

.L127:
	andi.	r0,  M,  1
	beq	.L130

	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO2, INC

	fsmfp	c01, c02
	STFPDUX	c01,   B, INC2
	.align 4

.L130:
	andi.	J,  N,  1
	ble	.L999

	mr	AO1, A

	srawi.	r0,  M,  3
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

	fsmfp	c01, c02
	fsmfp	c03, c04
	fsmfp	c05, c06
	fsmfp	c07, c08

	STFPDUX	c01,   B, INC2
	STFPDUX	c03,   B, INC2
	STFPDUX	c05,   B, INC2
	STFPDUX	c07,   B, INC2
	bdnz	.L132
	.align 4

.L135:
	andi.	r0,  M,  7
	ble	.L999

	andi.	r0,  M,  4
	beq	.L136

	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO1, INC
	LFDUX	c03,   AO1, INC
	LFDUX	c04,   AO1, INC

	fsmfp	c01, c02
	fsmfp	c03, c04

	STFPDUX	c01,   B, INC2
	STFPDUX	c03,   B, INC2
	.align 4

.L136:
	andi.	r0,  M,  2
	beq	.L137

	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO1, INC

	fsmfp	c01, c02
	STFPDUX	c01,   B, INC2
	.align 4

.L137:
	andi.	r0,  M,  1
	beq	.L999

	LFDX	c01,   AO1, INC
	STFDX	c01,   B,  INC2
	.align 4

.L999:
	addi	SP, SP, 12

	lwzu	r30,   4(SP)
	lwzu	r31,   4(SP)

	subi	SP, SP, 12
	li	r0, 16

	lfpdux	f19, SP, r0
	lfpdux	f18, SP, r0
	lfpdux	f17, SP, r0
	lfpdux	f16, SP, r0

	lfpdux	f15, SP, r0
	lfpdux	f14, SP, r0
	addi	SP, SP, 16
	blr
	EPILOGUE
