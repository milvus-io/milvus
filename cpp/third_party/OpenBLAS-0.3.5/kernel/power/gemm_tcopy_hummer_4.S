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

#define J	r25
#define B1	r26
#define B2	r27
#define B3	r28
#define M4	r29
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

	PROLOGUE
	PROFCODE

	stwu	r31,  -4(SP)
	stwu	r30,  -4(SP)
	stwu	r29,  -4(SP)
	stwu	r28,  -4(SP)

	stwu	r27,  -4(SP)
	stwu	r26,  -4(SP)
	stwu	r25,  -4(SP)

	slwi	LDA, LDA, BASE_SHIFT
	slwi	M4, M, 2 + BASE_SHIFT

	li	r8, -4
	li	r9, -2

	and	B2, N, r8
	and	B3, N, r9

	mullw	B2, B2, M
	mullw	B3, B3, M

	slwi	B2, B2, BASE_SHIFT
	slwi	B3, B3, BASE_SHIFT

	add	B2, B2, B
	add	B3, B3, B

	cmpwi	cr0, M, 0
	ble-	.L99
	cmpwi	cr0, N, 0
	ble-	.L99

	subi	B2, B2, 2 * SIZE
	subi	B3, B3, 2 * SIZE
	subi	M4, M4, 14 * SIZE

	li	INC,  1 * SIZE
	li	INC2, 2 * SIZE

	andi.	r0, A,   2 * SIZE - 1
	bne	.L100
	andi.	r0, LDA, 2 * SIZE - 1
	bne	.L100

	subi	A, A, 2 * SIZE
	srawi.	J,  M,  2
	ble	.L20
	.align 4

.L10:
	mr	AO1, A
	add	AO2, A,   LDA
	add	AO3, AO2, LDA
	add	AO4, AO3, LDA
	add	A,   AO4, LDA

	sub	B1, B, M4
	addi	B, B, 16 * SIZE

	srawi.	r0,  N,  2
	mtspr	CTR, r0
	ble	.L15
	.align 4

.L12:
	LFPDUX	c01,   AO1, INC2
	LFPDUX	c02,   AO1, INC2
	LFPDUX	c03,   AO2, INC2
	LFPDUX	c04,   AO2, INC2
 	LFPDUX	c05,   AO3, INC2
	LFPDUX	c06,   AO3, INC2
	LFPDUX	c07,   AO4, INC2
	LFPDUX	c08,   AO4, INC2

	STFPDUX	c01,   B1, M4
	STFPDUX	c02,   B1, INC2
	STFPDUX	c03,   B1, INC2
	STFPDUX	c04,   B1, INC2
	STFPDUX	c05,   B1, INC2
	STFPDUX	c06,   B1, INC2
	STFPDUX	c07,   B1, INC2
	STFPDUX	c08,   B1, INC2
	bdnz	.L12
	.align 4

.L15:
	andi.	r0,  N,  3
	ble	.L19

	andi.	r0,  N,  2
	ble	.L17

	LFPDUX	c01,   AO1, INC2
	LFPDUX	c03,   AO2, INC2
	LFPDUX	c05,   AO3, INC2
	LFPDUX	c07,   AO4, INC2

	STFPDUX	c01,   B2, INC2
	STFPDUX	c03,   B2, INC2
	STFPDUX	c05,   B2, INC2
	STFPDUX	c07,   B2, INC2
	.align 4

.L17:
	andi.	r0,  N,  1
	ble	.L19

	LFDUX	c01,   AO1, INC2
	LFDUX	c02,   AO2, INC2
	LFDUX	c03,   AO3, INC2
	LFDUX	c04,   AO4, INC2

	fsmfp	c01, c02
	fsmfp	c03, c04

	STFPDUX	c01,   B3, INC2
	STFPDUX	c03,   B3, INC2
	.align 4

.L19:
	addic.	J, J, -1
	bgt	.L10
	.align 4

.L20:
	andi.	J,  M,  2
	addi	M4, M4, 8 * SIZE

	ble	.L30

	mr	AO1, A
	add	AO2, A,   LDA
	add	A,   AO2, LDA

	sub	B1, B, M4
	addi	B, B, 8 * SIZE

	srawi.	r0,  N,  2
	mtspr	CTR, r0
	ble	.L23
	.align 4

.L22:
	LFPDUX	c01,   AO1, INC2
	LFPDUX	c02,   AO1, INC2
	LFPDUX	c03,   AO2, INC2
	LFPDUX	c04,   AO2, INC2

	STFPDUX	c01,   B1, M4
	STFPDUX	c02,   B1, INC2
	STFPDUX	c03,   B1, INC2
	STFPDUX	c04,   B1, INC2
	bdnz	.L22
	.align 4

.L23:
	andi.	r0,  N,  2
	ble	.L24

	LFPDUX	c01,   AO1, INC2
	LFPDUX	c03,   AO2, INC2

	STFPDUX	c01,   B2, INC2
	STFPDUX	c03,   B2, INC2
	.align 4

.L24:
	andi.	r0,  N,  1
	ble	.L30

	LFDUX	c01,   AO1, INC2
	LFDUX	c02,   AO2, INC2

	fsmfp	c01, c02
	STFPDUX	c01,   B3, INC2
	.align 4

.L30:
	andi.	J,  M,  1
	addi	M4, M4, 4 * SIZE
	ble	.L99

	mr	AO1, A
	sub	B1, B, M4

	srawi.	r0,  N,  2
	mtspr	CTR, r0
	ble	.L33
	.align 4

.L32:
	LFPDUX	c01,   AO1, INC2
	LFPDUX	c02,   AO1, INC2

	STFPDUX	c01,   B1, M4
	STFPDUX	c02,   B1, INC2
	bdnz	.L32
	.align 4

.L33:
	andi.	r0,  N,  2
	ble	.L34

	LFPDUX	c01,   AO1, INC2

	STFPDUX	c01,   B2, INC2
	.align 4

.L34:
	andi.	r0,  N,  1
	ble	.L99

	LFDX	c01,   AO1, INC2
	STFDX	c01,   B3, INC2
	.align 4

.L99:
	addi	SP, SP, -4

	lwzu	r25,   4(SP)
	lwzu	r26,   4(SP)
	lwzu	r27,   4(SP)

	lwzu	r28,   4(SP)
	lwzu	r29,   4(SP)
	lwzu	r30,   4(SP)
	lwzu	r31,   4(SP)

	addi	SP, SP, 4
	blr

.L100:
	subi	A, A, SIZE
	srawi.	J,  M,  2
	ble	.L120
	.align 4

.L110:
	mr	AO1, A
	add	AO2, A,   LDA
	add	AO3, AO2, LDA
	add	AO4, AO3, LDA
	add	A,   AO4, LDA

	sub	B1, B, M4
	addi	B, B, 16 * SIZE

	srawi.	r0,  N,  2
	mtspr	CTR, r0
	ble	.L115
	.align 4

.L112:
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

	STFPDUX	c01,   B1, M4
	STFPDUX	c02,   B1, INC2
	STFPDUX	c03,   B1, INC2
	STFPDUX	c04,   B1, INC2
	STFPDUX	c05,   B1, INC2
	STFPDUX	c06,   B1, INC2
	STFPDUX	c07,   B1, INC2
	STFPDUX	c08,   B1, INC2
	bdnz	.L112
	.align 4

.L115:
	andi.	r0,  N,  3
	ble	.L119

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

	STFPDUX	c01,   B2, INC2
	STFPDUX	c03,   B2, INC2
	STFPDUX	c05,   B2, INC2
	STFPDUX	c07,   B2, INC2
	.align 4

.L117:
	andi.	r0,  N,  1
	ble	.L119

	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO2, INC
	LFDUX	c03,   AO3, INC
	LFDUX	c04,   AO4, INC

	fsmfp	c01, c02
	fsmfp	c03, c04

	STFPDUX	c01,   B3, INC2
	STFPDUX	c03,   B3, INC2
	.align 4

.L119:
	addic.	J, J, -1
	bgt	.L110
	.align 4

.L120:
	andi.	J,  M,  2
	addi	M4, M4, 8 * SIZE

	ble	.L130

	mr	AO1, A
	add	AO2, A,   LDA
	add	A,   AO2, LDA

	sub	B1, B, M4
	addi	B, B, 8 * SIZE

	srawi.	r0,  N,  2
	mtspr	CTR, r0
	ble	.L123
	.align 4

.L122:
	LFDUX	c01,   AO1, INC
	LFDUX	c03,   AO2, INC
	LFSDUX	c01,   AO1, INC
	LFSDUX	c03,   AO2, INC

	LFDUX	c02,   AO1, INC
	LFDUX	c04,   AO2, INC
	LFSDUX	c02,   AO1, INC
	LFSDUX	c04,   AO2, INC

	STFPDUX	c01,   B1, M4
	STFPDUX	c02,   B1, INC2
	STFPDUX	c03,   B1, INC2
	STFPDUX	c04,   B1, INC2
	bdnz	.L122
	.align 4

.L123:
	andi.	r0,  N,  2
	ble	.L124

	LFDUX	c01,   AO1, INC
	LFDUX	c03,   AO2, INC
	LFSDUX	c01,   AO1, INC
	LFSDUX	c03,   AO2, INC

	STFPDUX	c01,   B2, INC2
	STFPDUX	c03,   B2, INC2
	.align 4

.L124:
	andi.	r0,  N,  1
	ble	.L130

	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO2, INC

	fsmfp	c01, c02
	STFPDUX	c01,   B3, INC2
	.align 4

.L130:
	andi.	J,  M,  1
	addi	M4, M4, 4 * SIZE
	ble	.L999

	mr	AO1, A
	sub	B1, B, M4

	srawi.	r0,  N,  2
	mtspr	CTR, r0
	ble	.L133
	.align 4

.L132:
	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO1, INC
	LFDUX	c03,   AO1, INC
	LFDUX	c04,   AO1, INC

	fsmfp	c01, c02
	fsmfp	c03, c04

	STFPDUX	c01,   B1, M4
	STFPDUX	c03,   B1, INC2
	bdnz	.L132
	.align 4

.L133:
	andi.	r0,  N,  2
	ble	.L134

	LFDUX	c01,   AO1, INC
	LFDUX	c02,   AO1, INC

	fsmfp	c01, c02
	STFPDUX	c01,   B2, INC2
	.align 4

.L134:
	andi.	r0,  N,  1
	ble	.L999

	LFDX	c01,   AO1, INC
	STFDX	c01,   B3, INC2
	.align 4

.L999:
	addi	SP, SP, -4

	lwzu	r25,   4(SP)
	lwzu	r26,   4(SP)
	lwzu	r27,   4(SP)

	lwzu	r28,   4(SP)
	lwzu	r29,   4(SP)
	lwzu	r30,   4(SP)
	lwzu	r31,   4(SP)

	addi	SP, SP, 4
	blr
	EPILOGUE
