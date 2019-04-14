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

#ifndef __64BIT__
#define LOAD	lwz
#else
#define LOAD	ld
#endif

#ifdef __64BIT__
#define STACKSIZE 360
#else
#define STACKSIZE 272
#endif

#define ALPHA		  0
#define FZERO		 16

#define	M	r3
#define	N	r4
#define	K	r5

#ifdef linux
#ifndef __64BIT__
#define A	r6
#define	B	r7
#define	C	r8
#define	LDC	r9
#else
#define A	r7
#define	B	r8
#define	C	r9
#define	LDC	r10
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
#define A	r8
#define	B	r9
#define	C	r10
#define	LDC	r7
#else
#define A	r7
#define	B	r8
#define	C	r9
#define	LDC	r10
#endif
#endif

#define STACK	r11

#define	I	r21
#define J	r22
#define AO	r23
#define	BO	r24
#define	CO1	r25
#define CO2	r26
#define	CO3	r27
#define	CO4	r28

#define PREA	r29
#define PREB	r29
#define PREC	r30
#define VREG	r31

#define LOAD_A	lvx
#define LOAD_B	lvx

#define OFFSET_0	  0
#define OFFSET_1	r14
#define OFFSET_2	r15
#define OFFSET_3	r16
#define OFFSET_4	r17
#define OFFSET_5	r18
#define OFFSET_6	r19
#define OFFSET_7	r20

#define	c01	v0
#define	c02	v1
#define	c03	v2
#define	c04	v3
#define	c05	v4
#define	c06	v5
#define	c07	v6
#define	c08	v7
#define	c09	v8
#define	c10	v9
#define	c11	v10
#define	c12	v11
#define	c13	v12
#define	c14	v13
#define	c15	v14
#define	c16	v15

#define	a1	v16
#define	a2	v17
#define	a3	v18
#define	a4	v19
#define	a5	v20
#define	a6	v21
#define	a7	v22
#define	a8	v23

#define	b1	v24
#define	b2	v25
#define	bp1	v26
#define	bp2	v27

#define C1	v16
#define C2	v17
#define C3	v18
#define C4	v19
#define C5	v20
#define C6	v21
#define C7	v22
#define C8	v23
#define C9	v24

#define c00	v25

#define PERMRSHIFT1	 v26
#define PERMRSHIFT2	 v27
#define PERMRSHIFT3	 v28
#define PERMRSHIFT4	 v29

#define VZERO	v30
#define alpha	v31

#ifndef NEEDPARAM

	PROLOGUE
	PROFCODE

	addi	SP, SP, -STACKSIZE
	mr	STACK, SP

	li	r0,  0 * 16
	stvx	v20, SP, r0
	li	r0,  1 * 16
	stvx	v21, SP, r0
	li	r0,  2 * 16
	stvx	v22, SP, r0
	li	r0,  3 * 16
	stvx	v23, SP, r0
	li	r0,  4 * 16
	stvx	v24, SP, r0
	li	r0,  5 * 16
	stvx	v25, SP, r0
	li	r0,  6 * 16
	stvx	v26, SP, r0
	li	r0,  7 * 16
	stvx	v27, SP, r0
	li	r0,  8 * 16
	stvx	v28, SP, r0
	li	r0,  9 * 16
	stvx	v29, SP, r0
	li	r0, 10 * 16
	stvx	v30, SP, r0
	li	r0, 11 * 16
	stvx	v31, SP, r0

#ifdef __64BIT__
	std	r31,  192(SP)
	std	r30,  200(SP)
	std	r29,  208(SP)
	std	r28,  216(SP)
	std	r27,  224(SP)
	std	r26,  232(SP)
	std	r25,  240(SP)
	std	r24,  248(SP)
	std	r23,  256(SP)
	std	r22,  264(SP)
	std	r21,  272(SP)
	std	r20,  280(SP)
	std	r19,  288(SP)
	std	r18,  296(SP)
	std	r17,  304(SP)
	std	r16,  312(SP)
	std	r15,  320(SP)
	std	r14,  328(SP)
#else
	stw	r31,  192(SP)
	stw	r30,  196(SP)
	stw	r29,  200(SP)
	stw	r28,  204(SP)
	stw	r27,  208(SP)
	stw	r26,  212(SP)
	stw	r25,  216(SP)
	stw	r24,  220(SP)
	stw	r23,  224(SP)
	stw	r22,  228(SP)
	stw	r21,  232(SP)
	stw	r20,  236(SP)
	stw	r19,  240(SP)
	stw	r18,  244(SP)
	stw	r17,  248(SP)
	stw	r16,  252(SP)
	stw	r15,  256(SP)
	stw	r14,  260(SP)
#endif


#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
	lwz	LDC, FRAMESLOT(0) + STACKSIZE(SP)
#endif
#endif

	li	r0, -1

	mfspr	VREG, VRsave
	mtspr	VRsave, r0

	addi	SP, SP, -128
	li	r0, -128
	and	SP, SP, r0

	li	OFFSET_1,  4 * SIZE
	li	OFFSET_2,  8 * SIZE
	li	OFFSET_3, 12 * SIZE
	li	OFFSET_4, 16 * SIZE
	li	OFFSET_5, 20 * SIZE
	li	OFFSET_6, 24 * SIZE
	li	OFFSET_7, 28 * SIZE

	stfs	f1,  ALPHA +  0(SP)
	stfs	f1,  ALPHA +  4(SP)
	stfs	f1,  ALPHA +  8(SP)
	stfs	f1,  ALPHA + 12(SP)

	li	r29, 0
	stw	r29, FZERO(SP)

	slwi	LDC, LDC, BASE_SHIFT

	li	PREC,   (15 * SIZE)
	li	PREB,   (25 * 8 * SIZE)

	cmpwi	cr0, M, 0
	ble	LL(999)
	cmpwi	cr0, N, 0
	ble	LL(999)
	cmpwi	cr0, K, 0
	ble	LL(999)

	srawi.	J, N,  2
	ble	LL(60)
	.align 4

LL(01):
	mr	CO1, C
	add	CO2, C,  LDC
	add	CO3, CO2, LDC
	add	CO4, CO3, LDC
	add	C,   CO4, LDC

	mr	AO, A
	srawi.	I, M,  4
	ble	LL(20)
	.align 4

LL(11):
	vxor	c01, c01, c01
	LOAD_B	b1, OFFSET_0, B
	vxor	c02, c02, c02
	LOAD_A	a1, OFFSET_0, AO
	vxor	c03, c03, c03
	LOAD_A	a2, OFFSET_1, AO
	vxor	c04, c04, c04
	LOAD_A	a3, OFFSET_2, AO
	vxor	c05, c05, c05
	LOAD_A	a4, OFFSET_3, AO
	vxor	c06, c06, c06
	LOAD_B	b2, OFFSET_2, B
	vxor	c07, c07, c07
	LOAD_A	a5, OFFSET_4, AO
	vxor	c08, c08, c08
	LOAD_A	a6, OFFSET_5, AO
	vxor	c09, c09, c09
	dcbtst	CO1, PREC
	vxor	c10, c10, c10
	dcbtst	CO2, PREC
	vxor	c11, c11, c11
	dcbtst	CO3, PREC
	vxor	c12, c12, c12
	dcbtst	CO4, PREC
	vxor	c13, c13, c13
	mr	BO, B
	vxor	c14, c14, c14
	srawi.	r0,  K,  2
	vxor	c15, c15, c15
	mtspr	CTR, r0
	vxor	c16, c16, c16
	vspltw	bp1, b1, 0
	ble	LL(15)
	.align 4

LL(12):
/* 1 */
	vmaddfp	c01, a1, bp1, c01
	vspltw	bp2, b1, 1
	vmaddfp	c02, a2, bp1, c02
	addi	AO, AO,  8 * SIZE
	vmaddfp	c03, a3, bp1, c03
	LOAD_A	a7, OFFSET_4, AO
	vmaddfp	c04, a4, bp1, c04
	LOAD_A	a8, OFFSET_5, AO

/* 2 */
	vmaddfp	c05, a1, bp2, c05
	vspltw	bp1, b1, 2
	vmaddfp	c06, a2, bp2, c06
	dcbt	BO, PREB
	vmaddfp	c07, a3, bp2, c07
	dcbt	AO, PREB
	vmaddfp	c08, a4, bp2, c08
	addi	AO, AO,  8 * SIZE

/* 3 */
	vmaddfp	c09, a1, bp1, c09
	vspltw	bp2, b1, 3
	vmaddfp	c10, a2, bp1, c10
	LOAD_B	b1, OFFSET_1, BO
	vmaddfp	c11, a3, bp1, c11
	dcbt	AO, PREB
	vmaddfp	c12, a4, bp1, c12
	addi	AO, AO, 8 * SIZE

/* 4 */
	vmaddfp	c13, a1, bp2, c13
	vspltw	bp1, b1, 0
	vmaddfp	c14, a2, bp2, c14
	LOAD_A	a1, OFFSET_2, AO
	vmaddfp	c15, a3, bp2, c15
	dcbt	AO, PREB
	vmaddfp	c16, a4, bp2, c16
	addi	AO, AO,  8 * SIZE

/* 5 */
	vmaddfp	c01, a5, bp1, c01
	vspltw	bp2, b1, 1
	vmaddfp	c02, a6, bp1, c02
	LOAD_A	a2, OFFSET_1, AO
	vmaddfp	c03, a7, bp1, c03
	LOAD_A	a3, OFFSET_2, AO
	vmaddfp	c04, a8, bp1, c04
	LOAD_A	a4, OFFSET_3, AO

/* 6 */
	vmaddfp	c05, a5, bp2, c05
	vspltw	bp1, b1, 2
	vmaddfp	c06, a6, bp2, c06
	nop
	vmaddfp	c07, a7, bp2, c07
	dcbt	AO, PREA
	vmaddfp	c08, a8, bp2, c08
	addi	AO, AO,  8 * SIZE

/* 7 */
	vmaddfp	c09, a5, bp1, c09
	vspltw	bp2, b1, 3
	vmaddfp	c10, a6, bp1, c10
	LOAD_B	b1, OFFSET_4, BO
	vmaddfp	c11, a7, bp1, c11
	nop
	vmaddfp	c12, a8, bp1, c12
	nop

/* 8 */
	vmaddfp	c13, a5, bp2, c13
	vspltw	bp1, b2, 0
	vmaddfp	c14, a6, bp2, c14
	LOAD_A	a5, OFFSET_2, AO
	vmaddfp	c15, a7, bp2, c15
	LOAD_A	a6, OFFSET_3, AO
	vmaddfp	c16, a8, bp2, c16
	LOAD_A	a7, OFFSET_4, AO

/* 9 */
	vmaddfp	c01, a1, bp1, c01
	vspltw	bp2, b2, 1
	vmaddfp	c02, a2, bp1, c02
	LOAD_A	a8, OFFSET_5, AO
	vmaddfp	c03, a3, bp1, c03
	addi	BO, BO,  8 * SIZE
	vmaddfp	c04, a4, bp1, c04
	nop

/* 10 */
	vmaddfp	c05, a1, bp2, c05
	vspltw	bp1, b2, 2
	vmaddfp	c06, a2, bp2, c06
	nop
	vmaddfp	c07, a3, bp2, c07
	nop
	vmaddfp	c08, a4, bp2, c08
	nop

/* 11 */
	vmaddfp	c09, a1, bp1, c09
	vspltw	bp2, b2, 3
	vmaddfp	c10, a2, bp1, c10
	LOAD_B	b2, OFFSET_1, BO
	vmaddfp	c11, a3, bp1, c11
	dcbt	AO, PREA
	vmaddfp	c12, a4, bp1, c12
	addi	AO, AO,  8 * SIZE

/* 12 */
	vmaddfp	c13, a1, bp2, c13
	vspltw	bp1, b2, 0
	vmaddfp	c14, a2, bp2, c14
	LOAD_A	a1, OFFSET_4, AO
	vmaddfp	c15, a3, bp2, c15
	LOAD_A	a2, OFFSET_5, AO
	vmaddfp	c16, a4, bp2, c16
	LOAD_A	a3, OFFSET_6, AO

/* 13 */
	vmaddfp	c01, a5, bp1, c01
	vspltw	bp2, b2, 1
	vmaddfp	c02, a6, bp1, c02
	LOAD_A	a4, OFFSET_7, AO
	vmaddfp	c03, a7, bp1, c03
	dcbt	AO, PREA
	vmaddfp	c04, a8, bp1, c04
	addi	AO, AO,  8 * SIZE

/* 14 */
	vmaddfp	c05, a5, bp2, c05
	vspltw	bp1, b2, 2
	vmaddfp	c06, a6, bp2, c06
	nop
	vmaddfp	c07, a7, bp2, c07
	dcbt	AO, PREA
	vmaddfp	c08, a8, bp2, c08
	addi	AO, AO,  8 * SIZE

/* 15 */
	vmaddfp	c09, a5, bp1, c09
	vspltw	bp2, b2, 3
	vmaddfp	c10, a6, bp1, c10
	LOAD_B	b2, OFFSET_4, BO
	vmaddfp	c11, a7, bp1, c11
	dcbt	AO, PREA
	vmaddfp	c12, a8, bp1, c12
	addi	BO, BO,  8 * SIZE

/* 16 */
	vmaddfp	c13, a5, bp2, c13
	vspltw	bp1, b1, 0
	vmaddfp	c14, a6, bp2, c14
	LOAD_A	a5, OFFSET_4, AO
	vmaddfp	c15, a7, bp2, c15
	LOAD_A	a6, OFFSET_5, AO
	vmaddfp	c16, a8, bp2, c16
	bdnz+	LL(12)
	.align 4

LL(15):
	andi.	r0,  K,  3
	lvx	alpha, OFFSET_0, SP
	vxor	VZERO, VZERO, VZERO
	mtspr	CTR, r0
	ble+	LL(18)
	.align 4

LL(16):
	vmaddfp	c01, a1, bp1, c01
	vspltw	bp2, b1, 1
	vmaddfp	c02, a2, bp1, c02
	nop
	vmaddfp	c03, a3, bp1, c03
	nop
	vmaddfp	c04, a4, bp1, c04
	nop

	vmaddfp	c05, a1, bp2, c05
	vspltw	bp1, b1, 2
	vmaddfp	c06, a2, bp2, c06
	nop
	vmaddfp	c07, a3, bp2, c07
	nop
	vmaddfp	c08, a4, bp2, c08
	nop

	vmaddfp	c09, a1, bp1, c09
	vspltw	bp2, b1, 3
	vmaddfp	c10, a2, bp1, c10
	LOAD_B	b1, OFFSET_1, BO
	vmaddfp	c11, a3, bp1, c11
	addi	AO, AO, 16 * SIZE
	vmaddfp	c12, a4, bp1, c12
	addi	BO, BO,  4 * SIZE

	vmaddfp	c13, a1, bp2, c13
	vspltw	bp1, b1, 0
	vmaddfp	c14, a2, bp2, c14
	LOAD_A	a1, OFFSET_0, AO
	vmaddfp	c15, a3, bp2, c15
	LOAD_A	a2, OFFSET_1, AO
	vmaddfp	c16, a4, bp2, c16
	LOAD_A	a3, OFFSET_2, AO

	LOAD_A	a4, OFFSET_3, AO
	bdnz+	LL(16)
	.align 4

LL(18):
	lvx	C1, OFFSET_0, CO1
	cmpwi	cr0, LDC, 32 * SIZE
	lvx	C2, OFFSET_1, CO1
	lvsr	PERMRSHIFT1, 0, CO1
	lvx	C3, OFFSET_2, CO1
	lvsr	PERMRSHIFT2, 0, CO2
	lvx	C4, OFFSET_3, CO1
	lvsr	PERMRSHIFT3, 0, CO3
	lvx	C5, OFFSET_4, CO1
	lvsr	PERMRSHIFT4, 0, CO4
	ble	LL(19)

	vperm	c00, VZERO, c01,   PERMRSHIFT1
	vperm	c01, c01,   c02,   PERMRSHIFT1
	vperm	c02, c02,   c03,   PERMRSHIFT1
	vperm	c03, c03,   c04,   PERMRSHIFT1
	vperm	c04, c04,   VZERO, PERMRSHIFT1

	vmaddfp	c00, alpha, c00, C1
	lvx	C1, OFFSET_0, CO2
	vmaddfp	c01, alpha, c01, C2
	lvx	C6, OFFSET_1, CO2
	vmaddfp	c02, alpha, c02, C3
	lvx	C7, OFFSET_2, CO2
	vmaddfp	c03, alpha, c03, C4
	lvx	C8, OFFSET_3, CO2
	vmaddfp	c04, alpha, c04, C5
	lvx	C9, OFFSET_4, CO2

	stvx	c00, OFFSET_0, CO1
	vperm	c00, VZERO, c05,   PERMRSHIFT2
	stvx	c01, OFFSET_1, CO1
	vperm	c05, c05,   c06,   PERMRSHIFT2
	stvx	c02, OFFSET_2, CO1
	vperm	c06, c06,   c07,   PERMRSHIFT2
	stvx	c03, OFFSET_3, CO1
	vperm	c07, c07,   c08,   PERMRSHIFT2
	stvx	c04, OFFSET_4, CO1
	vperm	c08, c08,   VZERO, PERMRSHIFT2

	vmaddfp	c00, alpha, c00, C1
	lvx	C1, OFFSET_0, CO3
	vmaddfp	c05, alpha, c05, C6
	lvx	C2, OFFSET_1, CO3
	vmaddfp	c06, alpha, c06, C7
	lvx	C3, OFFSET_2, CO3
	vmaddfp	c07, alpha, c07, C8
	lvx	C4, OFFSET_3, CO3
	vmaddfp	c08, alpha, c08, C9
	lvx	C5, OFFSET_4, CO3

	stvx	c00, OFFSET_0, CO2
	vperm	c00, VZERO, c09,   PERMRSHIFT3
	stvx	c05, OFFSET_1, CO2
	vperm	c09, c09,   c10,   PERMRSHIFT3
	stvx	c06, OFFSET_2, CO2
	vperm	c10, c10,   c11,   PERMRSHIFT3
	stvx	c07, OFFSET_3, CO2
	vperm	c11, c11,   c12,   PERMRSHIFT3
	stvx	c08, OFFSET_4, CO2
	vperm	c12, c12,   VZERO, PERMRSHIFT3

	vmaddfp	c00, alpha, c00, C1
	lvx	C9, OFFSET_4, CO4
	vmaddfp	c09, alpha, c09, C2
	lvx	C1, OFFSET_0, CO4
	vmaddfp	c10, alpha, c10, C3
	lvx	C6, OFFSET_1, CO4
	vmaddfp	c11, alpha, c11, C4
	lvx	C7, OFFSET_2, CO4
	vmaddfp	c12, alpha, c12, C5
	lvx	C8, OFFSET_3, CO4

	stvx	c00, OFFSET_0, CO3
	vperm	c00, VZERO, c13,   PERMRSHIFT4
	stvx	c09, OFFSET_1, CO3
	vperm	c13, c13,   c14,   PERMRSHIFT4
	stvx	c10, OFFSET_2, CO3
	vperm	c14, c14,   c15,   PERMRSHIFT4
	stvx	c11, OFFSET_3, CO3
	vperm	c15, c15,   c16,   PERMRSHIFT4
	stvx	c12, OFFSET_4, CO3
	vperm	c16, c16,   VZERO, PERMRSHIFT4

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c13, alpha, c13, C6
	vmaddfp	c14, alpha, c14, C7
	vmaddfp	c15, alpha, c15, C8
	vmaddfp	c16, alpha, c16, C9

	stvx	c00, OFFSET_0, CO4
	stvx	c13, OFFSET_1, CO4
	stvx	c14, OFFSET_2, CO4
	stvx	c15, OFFSET_3, CO4
	stvx	c16, OFFSET_4, CO4

	addi	CO1, CO1, 16 * SIZE
	addi	CO2, CO2, 16 * SIZE
	addi	CO3, CO3, 16 * SIZE
	addi	CO4, CO4, 16 * SIZE

	addic.	I, I, -1
	bgt+	LL(11)
	b	LL(20)
	.align 4

LL(19):
	lvx	C6, OFFSET_1, CO2
	lvx	C7, OFFSET_2, CO2
	lvx	C8, OFFSET_3, CO2
	lvx	C9, OFFSET_4, CO2

	vperm	c00, VZERO, c01,   PERMRSHIFT1
	vperm	c01, c01,   c02,   PERMRSHIFT1
	vperm	c02, c02,   c03,   PERMRSHIFT1
	vperm	c03, c03,   c04,   PERMRSHIFT1
	vperm	c04, c04,   VZERO, PERMRSHIFT1

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c01, alpha, c01, C2
	lvx	C2, OFFSET_1, CO3
	vmaddfp	c02, alpha, c02, C3
	lvx	C3, OFFSET_2, CO3
	vmaddfp	c03, alpha, c03, C4
	lvx	C4, OFFSET_3, CO3
	vmaddfp	c04, alpha, c04, C5
	lvx	C5, OFFSET_4, CO3

	stvx	c00, OFFSET_0, CO1
	stvx	c01, OFFSET_1, CO1
	stvx	c02, OFFSET_2, CO1
	stvx	c03, OFFSET_3, CO1
	stvx	c04, OFFSET_4, CO1

	lvx	C1, OFFSET_0, CO2

	vperm	c00, VZERO, c05,   PERMRSHIFT2
	vperm	c05, c05,   c06,   PERMRSHIFT2
	vperm	c06, c06,   c07,   PERMRSHIFT2
	vperm	c07, c07,   c08,   PERMRSHIFT2
	vperm	c08, c08,   VZERO, PERMRSHIFT2

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c05, alpha, c05, C6
	lvx	C6, OFFSET_1, CO4
	vmaddfp	c06, alpha, c06, C7
	lvx	C7, OFFSET_2, CO4
	vmaddfp	c07, alpha, c07, C8
	lvx	C8, OFFSET_3, CO4
	vmaddfp	c08, alpha, c08, C9
	lvx	C9, OFFSET_4, CO4

	stvx	c00, OFFSET_0, CO2
	stvx	c05, OFFSET_1, CO2
	stvx	c06, OFFSET_2, CO2
	stvx	c07, OFFSET_3, CO2
	stvx	c08, OFFSET_4, CO2

	lvx	C1, OFFSET_0, CO3

	vperm	c00, VZERO, c09,   PERMRSHIFT3
	vperm	c09, c09,   c10,   PERMRSHIFT3
	vperm	c10, c10,   c11,   PERMRSHIFT3
	vperm	c11, c11,   c12,   PERMRSHIFT3
	vperm	c12, c12,   VZERO, PERMRSHIFT3

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c09, alpha, c09, C2
	vmaddfp	c10, alpha, c10, C3
	vmaddfp	c11, alpha, c11, C4
	vmaddfp	c12, alpha, c12, C5

	stvx	c00, OFFSET_0, CO3
	stvx	c09, OFFSET_1, CO3
	stvx	c10, OFFSET_2, CO3
	stvx	c11, OFFSET_3, CO3
	stvx	c12, OFFSET_4, CO3

	lvx	C1, OFFSET_0, CO4

	vperm	c00, VZERO, c13,   PERMRSHIFT4
	vperm	c13, c13,   c14,   PERMRSHIFT4
	vperm	c14, c14,   c15,   PERMRSHIFT4
	vperm	c15, c15,   c16,   PERMRSHIFT4
	vperm	c16, c16,   VZERO, PERMRSHIFT4

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c13, alpha, c13, C6
	vmaddfp	c14, alpha, c14, C7
	vmaddfp	c15, alpha, c15, C8
	vmaddfp	c16, alpha, c16, C9

	stvx	c00, OFFSET_0, CO4
	stvx	c13, OFFSET_1, CO4
	stvx	c14, OFFSET_2, CO4
	stvx	c15, OFFSET_3, CO4
	stvx	c16, OFFSET_4, CO4

	addi	CO1, CO1, 16 * SIZE
	addi	CO2, CO2, 16 * SIZE
	addi	CO3, CO3, 16 * SIZE
	addi	CO4, CO4, 16 * SIZE

	addic.	I, I, -1
	bgt+	LL(11)
	.align 4

LL(20):
	andi.	I, M,  8
	ble	LL(30)

	vxor	c01, c01, c01
	LOAD_A	a1, OFFSET_0, AO
	vxor	c02, c02, c02
	LOAD_A	a2, OFFSET_1, AO
	vxor	c05, c05, c05
	LOAD_A	a3, OFFSET_2, AO
	vxor	c06, c06, c06
	LOAD_A	a4, OFFSET_3, AO
	vxor	c09, c09, c09
	LOAD_B	b1, OFFSET_0, B
	vxor	c10, c10, c10
	LOAD_B	b2, OFFSET_1, B
	vxor	c13, c13, c13
	vxor	c14, c14, c14
	mr	BO, B
	vspltw	bp1, b1, 0

	srawi.	r0,  K,  1
	mtspr	CTR, r0
	ble	LL(25)
	.align 4

LL(22):
	vmaddfp	c01, a1, bp1, c01
	vspltw	bp2, b1, 1
	addi	AO, AO, 16 * SIZE
	vmaddfp	c02, a2, bp1, c02
	addi	BO, BO,  8 * SIZE

	vmaddfp	c05, a1, bp2, c05
	vspltw	bp1, b1, 2
	vmaddfp	c06, a2, bp2, c06

	vmaddfp	c09, a1, bp1, c09
	vspltw	bp2, b1, 3
	LOAD_B	b1, OFFSET_0, BO
	vmaddfp	c10, a2, bp1, c10

	vmaddfp	c13, a1, bp2, c13
	LOAD_A	a1, OFFSET_0, AO
	vspltw	bp1, b2, 0
	vmaddfp	c14, a2, bp2, c14
	LOAD_A	a2, OFFSET_1, AO

	vmaddfp	c01, a3, bp1, c01
	vspltw	bp2, b2, 1
	vmaddfp	c02, a4, bp1, c02

	vmaddfp	c05, a3, bp2, c05
	vspltw	bp1, b2, 2
	vmaddfp	c06, a4, bp2, c06

	vmaddfp	c09, a3, bp1, c09
	vspltw	bp2, b2, 3
	LOAD_B	b2, OFFSET_1, BO
	vmaddfp	c10, a4, bp1, c10

	vmaddfp	c13, a3, bp2, c13
	LOAD_A	a3, OFFSET_2, AO
	vmaddfp	c14, a4, bp2, c14
	LOAD_A	a4, OFFSET_3, AO
	vspltw	bp1, b1, 0
	bdnz	LL(22)
	.align 4

LL(25):
	andi.	r0,  K,  1
	lvx	alpha, OFFSET_0, SP
	vxor	VZERO, VZERO, VZERO
	ble+	LL(28)
	.align 4

LL(26):
	vmaddfp	c01, a1, bp1, c01
	vspltw	bp2, b1, 1
	vmaddfp	c02, a2, bp1, c02
	nop

	vmaddfp	c05, a1, bp2, c05
	vspltw	bp1, b1, 2
	vmaddfp	c06, a2, bp2, c06
	nop

	vmaddfp	c09, a1, bp1, c09
	vspltw	bp2, b1, 3
	vmaddfp	c10, a2, bp1, c10
	addi	AO, AO,  8 * SIZE

	vmaddfp	c13, a1, bp2, c13
	addi	BO, BO,  4 * SIZE
	vmaddfp	c14, a2, bp2, c14
	nop
	.align 4

LL(28):
	lvx	C1, OFFSET_0, CO1
	lvx	C2, OFFSET_1, CO1
	lvx	C3, OFFSET_2, CO1

	lvsr	PERMRSHIFT1, 0, CO1
	lvsr	PERMRSHIFT2, 0, CO2
	lvsr	PERMRSHIFT3, 0, CO3
	lvsr	PERMRSHIFT4, 0, CO4

	vperm	c00, VZERO, c01,   PERMRSHIFT1
	vperm	c01, c01,   c02,   PERMRSHIFT1
	vperm	c02, c02,   VZERO, PERMRSHIFT1

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c01, alpha, c01, C2
	vmaddfp	c02, alpha, c02, C3

	stvx	c00, OFFSET_0, CO1
	stvx	c01, OFFSET_1, CO1
	stvx	c02, OFFSET_2, CO1

	lvx	C1, OFFSET_0, CO2
	lvx	C2, OFFSET_1, CO2
	lvx	C3, OFFSET_2, CO2

	vperm	c00, VZERO, c05,   PERMRSHIFT2
	vperm	c05, c05,   c06,   PERMRSHIFT2
	vperm	c06, c06,   VZERO, PERMRSHIFT2

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c05, alpha, c05, C2
	vmaddfp	c06, alpha, c06, C3

	stvx	c00, OFFSET_0, CO2
	stvx	c05, OFFSET_1, CO2
	stvx	c06, OFFSET_2, CO2

	lvx	C1, OFFSET_0, CO3
	lvx	C2, OFFSET_1, CO3
	lvx	C3, OFFSET_2, CO3

	vperm	c00, VZERO, c09,   PERMRSHIFT3
	vperm	c09, c09,   c10,   PERMRSHIFT3
	vperm	c10, c10,   VZERO, PERMRSHIFT3

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c09, alpha, c09, C2
	vmaddfp	c10, alpha, c10, C3

	stvx	c00, OFFSET_0, CO3
	stvx	c09, OFFSET_1, CO3
	stvx	c10, OFFSET_2, CO3

	lvx	C1, OFFSET_0, CO4
	lvx	C2, OFFSET_1, CO4
	lvx	C3, OFFSET_2, CO4

	vperm	c00, VZERO, c13,   PERMRSHIFT4
	vperm	c13, c13,   c14,   PERMRSHIFT4
	vperm	c14, c14,   VZERO, PERMRSHIFT4

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c13, alpha, c13, C2
	vmaddfp	c14, alpha, c14, C3

	stvx	c00, OFFSET_0, CO4
	stvx	c13, OFFSET_1, CO4
	stvx	c14, OFFSET_2, CO4

	addi	CO1, CO1, 8 * SIZE
	addi	CO2, CO2, 8 * SIZE
	addi	CO3, CO3, 8 * SIZE
	addi	CO4, CO4, 8 * SIZE
	.align 4

LL(30):
	andi.	I, M,  4
	ble	LL(40)

	vxor	c01, c01, c01
	LOAD_A	a1, OFFSET_0, AO
	vxor	c02, c02, c02
	LOAD_A	a2, OFFSET_1, AO
	vxor	c05, c05, c05
	LOAD_B	b1, OFFSET_0, B
	vxor	c06, c06, c06
	LOAD_B	b2, OFFSET_1, B
	vxor	c09, c09, c09
	vxor	c10, c10, c10
	vxor	c13, c13, c13
	vxor	c14, c14, c14

	vspltw	bp1, b1, 0
	mr	BO, B

	srawi.	r0,  K,  1
	mtspr	CTR, r0
	ble	LL(35)
	.align 4

LL(32):
	vmaddfp	c01, a1, bp1, c01
	addi	AO, AO,  8 * SIZE
	vspltw	bp2, b1, 1
	vmaddfp	c05, a1, bp2, c05
	addi	BO, BO,  8 * SIZE
	vspltw	bp1, b1, 2
	vmaddfp	c09, a1, bp1, c09
	vspltw	bp2, b1, 3
	vmaddfp	c13, a1, bp2, c13
	LOAD_A	a1, OFFSET_0, AO
	vspltw	bp1, b2, 0
	LOAD_B	b1, OFFSET_0, BO

	vmaddfp	c02, a2, bp1, c02
	vspltw	bp2, b2, 1
	vmaddfp	c06, a2, bp2, c06
	vspltw	bp1, b2, 2
	vmaddfp	c10, a2, bp1, c10
	vspltw	bp2, b2, 3
	LOAD_B	b2, OFFSET_1, BO
	vmaddfp	c14, a2, bp2, c14
	LOAD_A	a2, OFFSET_1, AO

	vspltw	bp1, b1, 0
	bdnz	LL(32)
	.align 4

LL(35):
	andi.	r0,  K,  1
	lvx	alpha, OFFSET_0, SP
	vxor	VZERO, VZERO, VZERO
	ble+	LL(38)
	.align 4

LL(36):
	vmaddfp	c01, a1, bp1, c01
	vspltw	bp2, b1, 1
	vmaddfp	c05, a1, bp2, c05
	vspltw	bp1, b1, 2
	vmaddfp	c09, a1, bp1, c09
	vspltw	bp2, b1, 3
	vmaddfp	c13, a1, bp2, c13
	addi	AO, AO,  4 * SIZE
	addi	BO, BO,  4 * SIZE
	.align 4

LL(38):
	vaddfp	c01, c01, c02
	vaddfp	c05, c05, c06
	vaddfp	c09, c09, c10
	vaddfp	c13, c13, c14

	lvx	C1, OFFSET_0, CO1
	lvx	C2, OFFSET_1, CO1

	lvsr	PERMRSHIFT1, 0, CO1
	lvsr	PERMRSHIFT2, 0, CO2
	lvsr	PERMRSHIFT3, 0, CO3
	lvsr	PERMRSHIFT4, 0, CO4

	vperm	c00, VZERO, c01,   PERMRSHIFT1
	vperm	c01, c01,   VZERO, PERMRSHIFT1

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c01, alpha, c01, C2

	stvx	c00, OFFSET_0, CO1
	stvx	c01, OFFSET_1, CO1

	lvx	C1, OFFSET_0, CO2
	lvx	C2, OFFSET_1, CO2

	vperm	c00, VZERO, c05,   PERMRSHIFT2
	vperm	c05, c05,   VZERO, PERMRSHIFT2

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c05, alpha, c05, C2

	stvx	c00, OFFSET_0, CO2
	stvx	c05, OFFSET_1, CO2

	lvx	C1, OFFSET_0, CO3
	lvx	C2, OFFSET_1, CO3

	vperm	c00, VZERO, c09,   PERMRSHIFT3
	vperm	c09, c09,   VZERO, PERMRSHIFT3

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c09, alpha, c09, C2

	stvx	c00, OFFSET_0, CO3
	stvx	c09, OFFSET_1, CO3

	lvx	C1, OFFSET_0, CO4
	lvx	C2, OFFSET_1, CO4

	vperm	c00, VZERO, c13,   PERMRSHIFT4
	vperm	c13, c13,   VZERO, PERMRSHIFT4

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c13, alpha, c13, C2

	stvx	c00, OFFSET_0, CO4
	stvx	c13, OFFSET_1, CO4

	addi	CO1, CO1, 4 * SIZE
	addi	CO2, CO2, 4 * SIZE
	addi	CO3, CO3, 4 * SIZE
	addi	CO4, CO4, 4 * SIZE
	.align 4

LL(40):
	andi.	I, M,  2
	ble	LL(50)

	mr	BO, B

	LFD	f8,   0 * SIZE(AO)
	LFD	f9,   1 * SIZE(AO)

	LFD	f10,  0 * SIZE(B)
	LFD	f11,  1 * SIZE(B)
	LFD	f12,  2 * SIZE(B)
	LFD	f13,  3 * SIZE(B)

	lfs	f0,  FZERO(SP)
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

	fmr	f4,  f0
	fmr	f5,  f0
	fmr	f6,  f0
	fmr	f7,  f0

	srawi.	r0,  K,  1
	mtspr	CTR, r0
	ble	LL(45)
	.align 4

LL(42):
	FMADD	f0,  f8, f10, f0
	FMADD	f2,  f8, f11, f2
	FMADD	f4,  f8, f12, f4
	FMADD	f6,  f8, f13, f6

	FMADD	f1,  f9, f10, f1
	FMADD	f3,  f9, f11, f3
	FMADD	f5,  f9, f12, f5
	FMADD	f7,  f9, f13, f7

	LFD	f8,   2 * SIZE(AO)
	LFD	f9,   3 * SIZE(AO)

	LFD	f10,  4 * SIZE(BO)
	LFD	f11,  5 * SIZE(BO)
	LFD	f12,  6 * SIZE(BO)
	LFD	f13,  7 * SIZE(BO)

	FMADD	f0,  f8, f10, f0
	FMADD	f2,  f8, f11, f2
	FMADD	f4,  f8, f12, f4
	FMADD	f6,  f8, f13, f6

	FMADD	f1,  f9, f10, f1
	FMADD	f3,  f9, f11, f3
	FMADD	f5,  f9, f12, f5
	FMADD	f7,  f9, f13, f7

	LFD	f8,   4 * SIZE(AO)
	LFD	f9,   5 * SIZE(AO)

	LFD	f10,  8 * SIZE(BO)
	LFD	f11,  9 * SIZE(BO)
	LFD	f12, 10 * SIZE(BO)
	LFD	f13, 11 * SIZE(BO)

	addi	AO, AO,  4 * SIZE
	addi	BO, BO,  8 * SIZE
	bdnz	LL(42)
	.align 4

LL(45):
	andi.	r0,  K,  1
	ble	LL(48)
	.align 4

LL(46):
	FMADD	f0,  f8, f10, f0
	FMADD	f2,  f8, f11, f2
	FMADD	f4,  f8, f12, f4
	FMADD	f6,  f8, f13, f6

	FMADD	f1,  f9, f10, f1
	FMADD	f3,  f9, f11, f3
	FMADD	f5,  f9, f12, f5
	FMADD	f7,  f9, f13, f7

	LFD	f8,   2 * SIZE(AO)
	LFD	f9,   3 * SIZE(AO)

	LFD	f10,  4 * SIZE(BO)
	LFD	f11,  5 * SIZE(BO)
	LFD	f12,  6 * SIZE(BO)
	LFD	f13,  7 * SIZE(BO)

	addi	AO, AO,  2 * SIZE
	addi	BO, BO,  4 * SIZE
	.align 4

LL(48):
	lfs	f13,  ALPHA(SP)

	LFD	f8,  0 * SIZE(CO1)
	LFD	f9,  1 * SIZE(CO1)
	LFD	f10, 0 * SIZE(CO2)
	LFD	f11, 1 * SIZE(CO2)

	FMADD	f0,  f0, f13, f8
	FMADD	f1,  f1, f13, f9
	FMADD	f2,  f2, f13, f10
	FMADD	f3,  f3, f13, f11

	LFD	f8,  0 * SIZE(CO3)
	LFD	f9,  1 * SIZE(CO3)
	LFD	f10, 0 * SIZE(CO4)
	LFD	f11, 1 * SIZE(CO4)

	FMADD	f4,  f4, f13, f8
	FMADD	f5,  f5, f13, f9
	FMADD	f6,  f6, f13, f10
	FMADD	f7,  f7, f13, f11

	STFD	f0,  0 * SIZE(CO1)
	STFD	f1,  1 * SIZE(CO1)
	STFD	f2,  0 * SIZE(CO2)
	STFD	f3,  1 * SIZE(CO2)

	STFD	f4,  0 * SIZE(CO3)
	STFD	f5,  1 * SIZE(CO3)
	STFD	f6,  0 * SIZE(CO4)
	STFD	f7,  1 * SIZE(CO4)

	addi	CO1, CO1, 2 * SIZE
	addi	CO2, CO2, 2 * SIZE
	addi	CO3, CO3, 2 * SIZE
	addi	CO4, CO4, 2 * SIZE
	.align 4

LL(50):
	andi.	I, M,  1
	ble	LL(59)

	mr	BO, B

	LFD	f8,   0 * SIZE(AO)
	LFD	f9,   1 * SIZE(AO)

	LFD	f10,  0 * SIZE(B)
	LFD	f11,  1 * SIZE(B)
	LFD	f12,  2 * SIZE(B)
	LFD	f13,  3 * SIZE(B)

	lfs	f0,  FZERO(SP)
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

	srawi.	r0,  K,  1
	mtspr	CTR, r0
	ble	LL(55)
	.align 4

LL(52):
	FMADD	f0,  f8, f10, f0
	FMADD	f1,  f8, f11, f1
	FMADD	f2,  f8, f12, f2
	FMADD	f3,  f8, f13, f3

	LFD	f8,   2 * SIZE(AO)

	LFD	f10,  4 * SIZE(BO)
	LFD	f11,  5 * SIZE(BO)
	LFD	f12,  6 * SIZE(BO)
	LFD	f13,  7 * SIZE(BO)

	FMADD	f0,  f9, f10, f0
	FMADD	f1,  f9, f11, f1
	FMADD	f2,  f9, f12, f2
	FMADD	f3,  f9, f13, f3

	LFD	f9,   3 * SIZE(AO)

	LFD	f10,  8 * SIZE(BO)
	LFD	f11,  9 * SIZE(BO)
	LFD	f12, 10 * SIZE(BO)
	LFD	f13, 11 * SIZE(BO)

	addi	AO, AO,  2 * SIZE
	addi	BO, BO,  8 * SIZE
	bdnz	LL(52)
	.align 4

LL(55):
	andi.	r0,  K,  1
	ble	LL(58)
	.align 4

LL(56):
	FMADD	f0,  f8, f10, f0
	FMADD	f1,  f8, f11, f1
	FMADD	f2,  f8, f12, f2
	FMADD	f3,  f8, f13, f3

	LFD	f8,   2 * SIZE(AO)

	LFD	f10,  4 * SIZE(BO)
	LFD	f11,  5 * SIZE(BO)
	LFD	f12,  6 * SIZE(BO)
	LFD	f13,  7 * SIZE(BO)

	addi	AO, AO,  1 * SIZE
	addi	BO, BO,  4 * SIZE
	.align 4

LL(58):
	lfs	f13,  ALPHA(SP)

	LFD	f8,  0 * SIZE(CO1)
	LFD	f9,  0 * SIZE(CO2)
	LFD	f10, 0 * SIZE(CO3)
	LFD	f11, 0 * SIZE(CO4)

	FMADD	f0,  f0, f13, f8
	FMADD	f1,  f1, f13, f9
	FMADD	f2,  f2, f13, f10
	FMADD	f3,  f3, f13, f11

	STFD	f0,  0 * SIZE(CO1)
	STFD	f1,  0 * SIZE(CO2)
	STFD	f2,  0 * SIZE(CO3)
	STFD	f3,  0 * SIZE(CO4)
	.align 4

LL(59):
	mr	B, BO

	addic.	J, J, -1
	bgt	LL(01)
	.align 4

LL(60):
	andi.	r0, N,  2
	ble	LL(120)

	mr	CO1, C
	add	CO2, C,  LDC
	add	C,  CO2, LDC

	mr	AO, A
	srawi.	I, M,  4
	ble	LL(80)
	.align 4

LL(71):
	vxor	c01, c01, c01
	LOAD_B	b1, OFFSET_0, B
	vxor	c02, c02, c02
	vxor	c03, c03, c03
	LOAD_A	a1, OFFSET_0, AO
	vxor	c04, c04, c04
	LOAD_A	a2, OFFSET_1, AO
	vxor	c05, c05, c05
	LOAD_A	a3, OFFSET_2, AO
	vxor	c06, c06, c06
	LOAD_A	a4, OFFSET_3, AO
	vxor	c07, c07, c07
	vxor	c08, c08, c08

	mr	BO, B
	dcbtst	CO1, PREC
	dcbtst	CO2, PREC

	vspltw	bp1, b1, 0

	srawi.	r0,  K,  1
	mtspr	CTR, r0
	ble	LL(75)
	.align 4

LL(72):
	LOAD_A	a5, OFFSET_4, AO
	LOAD_A	a6, OFFSET_5, AO
	LOAD_A	a7, OFFSET_6, AO
	LOAD_A	a8, OFFSET_7, AO

	vmaddfp	c01, a1, bp1, c01
	vspltw	bp2, b1, 1
	vmaddfp	c02, a2, bp1, c02
	vmaddfp	c03, a3, bp1, c03
	vmaddfp	c04, a4, bp1, c04

	vmaddfp	c05, a1, bp2, c05
	vspltw	bp1, b1, 2
	vmaddfp	c06, a2, bp2, c06
	vmaddfp	c07, a3, bp2, c07
	vmaddfp	c08, a4, bp2, c08

	vmaddfp	c01, a5, bp1, c01
	vspltw	bp2, b1, 3
	vmaddfp	c02, a6, bp1, c02
	vmaddfp	c03, a7, bp1, c03
	vmaddfp	c04, a8, bp1, c04

	LOAD_B	b1, OFFSET_1, BO
	vspltw	bp1, b1, 0

	vmaddfp	c05, a5, bp2, c05
	vmaddfp	c06, a6, bp2, c06
	vmaddfp	c07, a7, bp2, c07
	vmaddfp	c08, a8, bp2, c08

	addi	AO, AO, 32 * SIZE
	addi	BO, BO,  4 * SIZE

	LOAD_A	a1, OFFSET_0, AO
	LOAD_A	a2, OFFSET_1, AO
	LOAD_A	a3, OFFSET_2, AO
	LOAD_A	a4, OFFSET_3, AO
	bdnz	LL(72)
	.align 4

LL(75):
	andi.	r0,  K,  1
	lvx	alpha, OFFSET_0, SP
	vxor	VZERO, VZERO, VZERO
	ble+	LL(78)
	.align 4

LL(76):
	vmaddfp	c01, a1, bp1, c01
	vspltw	bp2, b1, 1
	vmaddfp	c02, a2, bp1, c02
	addi	AO, AO, 16 * SIZE
	vmaddfp	c03, a3, bp1, c03
	addi	BO, BO,  2 * SIZE
	vmaddfp	c04, a4, bp1, c04
	nop

	vmaddfp	c05, a1, bp2, c05
	vmaddfp	c06, a2, bp2, c06
	vmaddfp	c07, a3, bp2, c07
	vmaddfp	c08, a4, bp2, c08
	.align 4

LL(78):
	lvx	C1, OFFSET_0, CO1
	lvx	C2, OFFSET_1, CO1
	lvx	C3, OFFSET_2, CO1
	lvx	C4, OFFSET_3, CO1
	lvx	C5, OFFSET_4, CO1

	lvsr	PERMRSHIFT1, 0, CO1
	lvsr	PERMRSHIFT2, 0, CO2
	lvsr	PERMRSHIFT3, 0, CO3
	lvsr	PERMRSHIFT4, 0, CO4

	vperm	c00, VZERO, c01,   PERMRSHIFT1
	vperm	c01, c01,   c02,   PERMRSHIFT1
	vperm	c02, c02,   c03,   PERMRSHIFT1
	vperm	c03, c03,   c04,   PERMRSHIFT1
	vperm	c04, c04,   VZERO, PERMRSHIFT1

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c01, alpha, c01, C2
	vmaddfp	c02, alpha, c02, C3
	vmaddfp	c03, alpha, c03, C4
	vmaddfp	c04, alpha, c04, C5

	stvx	c00, OFFSET_0, CO1
	stvx	c01, OFFSET_1, CO1
	stvx	c02, OFFSET_2, CO1
	stvx	c03, OFFSET_3, CO1
	stvx	c04, OFFSET_4, CO1

	lvx	C1, OFFSET_0, CO2
	lvx	C2, OFFSET_1, CO2
	lvx	C3, OFFSET_2, CO2
	lvx	C4, OFFSET_3, CO2
	lvx	C5, OFFSET_4, CO2

	vperm	c00, VZERO, c05,   PERMRSHIFT2
	vperm	c05, c05,   c06,   PERMRSHIFT2
	vperm	c06, c06,   c07,   PERMRSHIFT2
	vperm	c07, c07,   c08,   PERMRSHIFT2
	vperm	c08, c08,   VZERO, PERMRSHIFT2

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c05, alpha, c05, C2
	vmaddfp	c06, alpha, c06, C3
	vmaddfp	c07, alpha, c07, C4
	vmaddfp	c08, alpha, c08, C5

	stvx	c00, OFFSET_0, CO2
	stvx	c05, OFFSET_1, CO2
	stvx	c06, OFFSET_2, CO2
	stvx	c07, OFFSET_3, CO2
	stvx	c08, OFFSET_4, CO2

	addi	CO1, CO1, 16 * SIZE
	addi	CO2, CO2, 16 * SIZE
	addic.	I, I, -1
	bgt+	LL(71)
	.align 4

LL(80):
	andi.	I, M,  8
	ble	LL(90)

	vxor	c01, c01, c01
	LOAD_B	b1, OFFSET_0, B
	vxor	c02, c02, c02
	vxor	c03, c03, c03
	LOAD_A	a1, OFFSET_0, AO
	vxor	c04, c04, c04
	LOAD_A	a2, OFFSET_1, AO
	vxor	c05, c05, c05
	LOAD_A	a3, OFFSET_2, AO
	vxor	c06, c06, c06
	LOAD_A	a4, OFFSET_3, AO
	vxor	c07, c07, c07
	vxor	c08, c08, c08

	mr	BO, B

	vspltw	bp1, b1, 0
	srawi.	r0,  K,  1
	mtspr	CTR, r0
	ble	LL(85)
	.align 4

LL(82):
	vmaddfp	c01, a1, bp1, c01
	vspltw	bp2, b1, 1
	vmaddfp	c02, a2, bp1, c02

	vmaddfp	c05, a1, bp2, c05
	vspltw	bp1, b1, 2
	vmaddfp	c06, a2, bp2, c06

	vmaddfp	c03, a3, bp1, c03
	vspltw	bp2, b1, 3
	vmaddfp	c04, a4, bp1, c04

	LOAD_B	b1, OFFSET_1, BO
	vspltw	bp1, b1, 0

	vmaddfp	c07, a3, bp2, c07
	vmaddfp	c08, a4, bp2, c08

	addi	AO, AO, 16 * SIZE
	addi	BO, BO,  4 * SIZE

	LOAD_A	a1, OFFSET_0, AO
	LOAD_A	a2, OFFSET_1, AO
	LOAD_A	a3, OFFSET_2, AO
	LOAD_A	a4, OFFSET_3, AO
	bdnz	LL(82)
	.align 4

LL(85):
	andi.	r0,  K,  1
	lvx	alpha, OFFSET_0, SP
	vxor	VZERO, VZERO, VZERO
	ble+	LL(88)
	.align 4

LL(86):
	vmaddfp	c01, a1, bp1, c01
	vspltw	bp2, b1, 1
	vmaddfp	c02, a2, bp1, c02
	addi	AO, AO,  8 * SIZE
	vmaddfp	c05, a1, bp2, c05
	addi	BO, BO,  2 * SIZE
	vmaddfp	c06, a2, bp2, c06
	.align 4

LL(88):
	lvx	C1, OFFSET_0, CO1
	lvx	C2, OFFSET_1, CO1
	lvx	C3, OFFSET_2, CO1

	vaddfp	c01, c01, c03
	vaddfp	c02, c02, c04
	vaddfp	c05, c05, c07
	vaddfp	c06, c06, c08

	lvsr	PERMRSHIFT1, 0, CO1
	lvsr	PERMRSHIFT2, 0, CO2
	lvsr	PERMRSHIFT3, 0, CO3
	lvsr	PERMRSHIFT4, 0, CO4

	vperm	c00, VZERO, c01,   PERMRSHIFT1
	vperm	c01, c01,   c02,   PERMRSHIFT1
	vperm	c02, c02,   VZERO, PERMRSHIFT1

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c01, alpha, c01, C2
	vmaddfp	c02, alpha, c02, C3

	stvx	c00, OFFSET_0, CO1
	stvx	c01, OFFSET_1, CO1
	stvx	c02, OFFSET_2, CO1

	lvx	C1, OFFSET_0, CO2
	lvx	C2, OFFSET_1, CO2
	lvx	C3, OFFSET_2, CO2

	vperm	c00, VZERO, c05,   PERMRSHIFT2
	vperm	c05, c05,   c06,   PERMRSHIFT2
	vperm	c06, c06,   VZERO, PERMRSHIFT2

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c05, alpha, c05, C2
	vmaddfp	c06, alpha, c06, C3

	stvx	c00, OFFSET_0, CO2
	stvx	c05, OFFSET_1, CO2
	stvx	c06, OFFSET_2, CO2

	addi	CO1, CO1, 8 * SIZE
	addi	CO2, CO2, 8 * SIZE
	.align 4

LL(90):
	andi.	I, M,  4
	ble	LL(100)

	vxor	c01, c01, c01
	LOAD_B	b1, OFFSET_0, B
	vxor	c02, c02, c02
	LOAD_A	a1, OFFSET_0, AO
	LOAD_A	a2, OFFSET_1, AO
	vxor	c05, c05, c05
	vxor	c06, c06, c06

	mr	BO, B

	vspltw	bp1, b1, 0

	srawi.	r0,  K,  1
	mtspr	CTR, r0
	ble	LL(95)
	.align 4

LL(92):
	vmaddfp	c01, a1, bp1, c01
	vspltw	bp2, b1, 1

	vmaddfp	c05, a1, bp2, c05
	vspltw	bp1, b1, 2

	vmaddfp	c02, a2, bp1, c02
	vspltw	bp2, b1, 3

	LOAD_B	b1, OFFSET_1, BO
	vspltw	bp1, b1, 0

	vmaddfp	c06, a2, bp2, c06

	addi	AO, AO,  8 * SIZE
	addi	BO, BO,  4 * SIZE

	LOAD_A	a1, OFFSET_0, AO
	LOAD_A	a2, OFFSET_1, AO
	bdnz	LL(92)
	.align 4

LL(95):
	andi.	r0,  K,  1
	lvx	alpha, OFFSET_0, SP
	vxor	VZERO, VZERO, VZERO
	ble+	LL(98)
	.align 4

LL(96):
	vspltw	bp2, b1, 1
	vmaddfp	c01, a1, bp1, c01
	vmaddfp	c05, a1, bp2, c05
	addi	AO, AO,  4 * SIZE
	addi	BO, BO,  2 * SIZE
	.align 4

LL(98):
	vaddfp	c01, c01, c02
	vaddfp	c05, c05, c06
	vaddfp	c09, c09, c10
	vaddfp	c13, c13, c14

	lvx	C1, OFFSET_0, CO1
	lvx	C2, OFFSET_1, CO1

	lvsr	PERMRSHIFT1, 0, CO1
	lvsr	PERMRSHIFT2, 0, CO2
	lvsr	PERMRSHIFT3, 0, CO3
	lvsr	PERMRSHIFT4, 0, CO4

	vperm	c00, VZERO, c01,   PERMRSHIFT1
	vperm	c01, c01,   VZERO, PERMRSHIFT1

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c01, alpha, c01, C2

	stvx	c00, OFFSET_0, CO1
	stvx	c01, OFFSET_1, CO1

	lvx	C1, OFFSET_0, CO2
	lvx	C2, OFFSET_1, CO2

	vperm	c00, VZERO, c05,   PERMRSHIFT2
	vperm	c05, c05,   VZERO, PERMRSHIFT2

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c05, alpha, c05, C2

	stvx	c00, OFFSET_0, CO2
	stvx	c05, OFFSET_1, CO2

	addi	CO1, CO1, 4 * SIZE
	addi	CO2, CO2, 4 * SIZE
	.align 4

LL(100):
	andi.	I, M,  2
	ble	LL(110)

	mr	BO, B

	LFD	f8,   0 * SIZE(AO)
	LFD	f9,   1 * SIZE(AO)

	LFD	f10,  0 * SIZE(B)
	LFD	f11,  1 * SIZE(B)
	LFD	f12,  2 * SIZE(B)
	LFD	f13,  3 * SIZE(B)

	lfs	f0,  FZERO(SP)
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

	fmr	f4,  f0
	fmr	f5,  f0
	fmr	f6,  f0
	fmr	f7,  f0

	srawi.	r0,  K,  1
	mtspr	CTR, r0
	ble	LL(105)
	.align 4

LL(102):
	FMADD	f0,  f8, f10, f0
	FMADD	f1,  f9, f10, f1
	FMADD	f2,  f8, f11, f2
	FMADD	f3,  f9, f11, f3

	LFD	f8,   2 * SIZE(AO)
	LFD	f9,   3 * SIZE(AO)

	FMADD	f4,  f8, f12, f4
	FMADD	f5,  f9, f12, f5
	FMADD	f6,  f8, f13, f6
	FMADD	f7,  f9, f13, f7

	LFD	f8,  4 * SIZE(AO)
	LFD	f9,  5 * SIZE(AO)

	LFD	f10,  4 * SIZE(BO)
	LFD	f11,  5 * SIZE(BO)
	LFD	f12,  6 * SIZE(BO)
	LFD	f13,  7 * SIZE(BO)

	addi	AO, AO,  4 * SIZE
	addi	BO, BO,  4 * SIZE
	bdnz	LL(102)
	.align 4

LL(105):
	andi.	r0,  K,  1
	lfs	f13,  ALPHA(SP)
	ble	LL(108)
	.align 4

LL(106):
	FMADD	f0,  f8, f10, f0
	FMADD	f1,  f9, f10, f1
	FMADD	f2,  f8, f11, f2
	FMADD	f3,  f9, f11, f3

	LFD	f8,   2 * SIZE(AO)
	LFD	f9,   3 * SIZE(AO)

	LFD	f10,  2 * SIZE(BO)
	LFD	f11,  3 * SIZE(BO)

	addi	AO, AO,  2 * SIZE
	addi	BO, BO,  2 * SIZE
	.align 4

LL(108):
	LFD	f8,  0 * SIZE(CO1)
	LFD	f9,  1 * SIZE(CO1)
	LFD	f10, 0 * SIZE(CO2)
	LFD	f11, 1 * SIZE(CO2)

	FADD	f0, f0, f4
	FADD	f1, f1, f5
	FADD	f2, f2, f6
	FADD	f3, f3, f7

	FMADD	f0,  f0, f13, f8
	FMADD	f1,  f1, f13, f9
	FMADD	f2,  f2, f13, f10
	FMADD	f3,  f3, f13, f11

	STFD	f0,  0 * SIZE(CO1)
	STFD	f1,  1 * SIZE(CO1)
	STFD	f2,  0 * SIZE(CO2)
	STFD	f3,  1 * SIZE(CO2)

	addi	CO1, CO1, 2 * SIZE
	addi	CO2, CO2, 2 * SIZE
	.align 4

LL(110):
	andi.	I, M,  1
	ble	LL(119)

	mr	BO, B

	LFD	f8,   0 * SIZE(AO)
	LFD	f9,   1 * SIZE(AO)

	LFD	f10,  0 * SIZE(B)
	LFD	f11,  1 * SIZE(B)
	LFD	f12,  2 * SIZE(B)
	LFD	f13,  3 * SIZE(B)

	lfs	f0,  FZERO(SP)
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

	srawi.	r0,  K,  1
	mtspr	CTR, r0
	ble	LL(115)
	.align 4

LL(112):
	FMADD	f0,  f8, f10, f0
	FMADD	f1,  f8, f11, f1
	FMADD	f2,  f9, f12, f2
	FMADD	f3,  f9, f13, f3

	LFD	f8,   2 * SIZE(AO)
	LFD	f9,   3 * SIZE(AO)

	LFD	f10,  4 * SIZE(BO)
	LFD	f11,  5 * SIZE(BO)
	LFD	f12,  6 * SIZE(BO)
	LFD	f13,  7 * SIZE(BO)

	addi	AO, AO,  2 * SIZE
	addi	BO, BO,  4 * SIZE
	bdnz	LL(112)
	.align 4

LL(115):
	andi.	r0,  K,  1
	lfs	f13,  ALPHA(SP)
	ble	LL(118)
	.align 4

LL(116):
	FMADD	f0,  f8, f10, f0
	FMADD	f1,  f8, f11, f1

	LFD	f8,   1 * SIZE(AO)

	LFD	f10,  2 * SIZE(BO)
	LFD	f11,  3 * SIZE(BO)

	addi	AO, AO,  1 * SIZE
	addi	BO, BO,  2 * SIZE
	.align 4

LL(118):
	LFD	f8,  0 * SIZE(CO1)
	LFD	f9,  0 * SIZE(CO2)

	FADD	f0, f0, f2
	FADD	f1, f1, f3

	FMADD	f0,  f0, f13, f8
	FMADD	f1,  f1, f13, f9

	STFD	f0,  0 * SIZE(CO1)
	STFD	f1,  0 * SIZE(CO2)
	.align 4

LL(119):
	mr	B, BO
	.align 4

LL(120):
	andi.	r0, N,  1
	ble	LL(999)

	mr	CO1, C
	mr	AO, A
	srawi.	I, M,  4
	ble	LL(140)
	.align 4

LL(130):
	vxor	c01, c01, c01
	vxor	c02, c02, c02
	vxor	c03, c03, c03
	vxor	c04, c04, c04

	mr	BO, B

	dcbtst	CO1, PREC

	mr	J, K

	andi.	r0,  B,  15
	ble+	LL(131)

	LOAD_A	a1, OFFSET_0, AO
	LOAD_A	a2, OFFSET_1, AO
	LOAD_A	a3, OFFSET_2, AO
	LOAD_A	a4, OFFSET_3, AO
	LOAD_B	b1, OFFSET_0, BO
	vspltw	bp1, b1,  2
	vspltw	bp2, b1,  3

	addi	AO, AO, 16 * SIZE
	addi	BO, BO, SIZE

	vmaddfp	c01, a1, bp1, c01
	vmaddfp	c02, a2, bp1, c02
	vmaddfp	c03, a3, bp1, c03
	vmaddfp	c04, a4, bp1, c04
	subi	J, J, 1
	cmpwi	cr0, J, 0
	ble	LL(138)

	LOAD_A	a1, OFFSET_0, AO
	LOAD_A	a2, OFFSET_1, AO
	LOAD_A	a3, OFFSET_2, AO
	LOAD_A	a4, OFFSET_3, AO

	addi	AO, AO, 16 * SIZE
	addi	BO, BO, SIZE

	vmaddfp	c01, a1, bp2, c01
	vmaddfp	c02, a2, bp2, c02
	vmaddfp	c03, a3, bp2, c03
	vmaddfp	c04, a4, bp2, c04
	subi	J, J, 1
	cmpwi	cr0, J, 0
	ble	LL(138)
	.align 4


LL(131):
	LOAD_A	a1, OFFSET_0, AO
	LOAD_A	a2, OFFSET_1, AO
	LOAD_A	a3, OFFSET_2, AO
	LOAD_A	a4, OFFSET_3, AO
	LOAD_A	a5, OFFSET_4, AO
	LOAD_A	a6, OFFSET_5, AO
	LOAD_A	a7, OFFSET_6, AO
	LOAD_A	a8, OFFSET_7, AO

	LOAD_B	b1, OFFSET_0, BO

	srawi.	r0,  J,  2
	mtspr	CTR, r0
	ble	LL(135)
	.align 4

LL(133):
	vspltw	bp1, b1,  0
	vmaddfp	c01, a1, bp1, c01
	vmaddfp	c02, a2, bp1, c02
	vmaddfp	c03, a3, bp1, c03
	vmaddfp	c04, a4, bp1, c04

	vspltw	bp2, b1,  1
	vmaddfp	c01, a5, bp2, c01
	vmaddfp	c02, a6, bp2, c02
	vmaddfp	c03, a7, bp2, c03
	vmaddfp	c04, a8, bp2, c04

	addi	AO, AO, 32 * SIZE

	LOAD_A	a1, OFFSET_0, AO
	LOAD_A	a2, OFFSET_1, AO
	LOAD_A	a3, OFFSET_2, AO
	LOAD_A	a4, OFFSET_3, AO

	vspltw	bp1, b1,  2
	vmaddfp	c01, a1, bp1, c01
	vmaddfp	c02, a2, bp1, c02
	vmaddfp	c03, a3, bp1, c03
	vmaddfp	c04, a4, bp1, c04

	LOAD_A	a5, OFFSET_4, AO
	LOAD_A	a6, OFFSET_5, AO
	LOAD_A	a7, OFFSET_6, AO
	LOAD_A	a8, OFFSET_7, AO

	vspltw	bp2, b1,  3
	vmaddfp	c01, a5, bp2, c01
	vmaddfp	c02, a6, bp2, c02
	vmaddfp	c03, a7, bp2, c03
	vmaddfp	c04, a8, bp2, c04

	addi	AO, AO, 32 * SIZE
	addi	BO, BO,  4 * SIZE

	LOAD_A	a1, OFFSET_0, AO
	LOAD_A	a2, OFFSET_1, AO
	LOAD_A	a3, OFFSET_2, AO
	LOAD_A	a4, OFFSET_3, AO

	LOAD_A	a5, OFFSET_4, AO
	LOAD_A	a6, OFFSET_5, AO
	LOAD_A	a7, OFFSET_6, AO
	LOAD_A	a8, OFFSET_7, AO

	LOAD_B	b1, OFFSET_0, BO

	bdnz	LL(133)
	.align 4

LL(135):
	andi.	r0,  J,  3
	ble+	LL(138)

	cmpwi	cr0, r0, 3
	bne	LL(136)

	vspltw	bp1, b1,  0
	vmaddfp	c01, a1, bp1, c01
	vmaddfp	c02, a2, bp1, c02
	vmaddfp	c03, a3, bp1, c03
	vmaddfp	c04, a4, bp1, c04

	addi	AO, AO, 16 * SIZE

	LOAD_A	a1, OFFSET_0, AO
	LOAD_A	a2, OFFSET_1, AO
	LOAD_A	a3, OFFSET_2, AO
	LOAD_A	a4, OFFSET_3, AO

	vspltw	bp2, b1,  1
	vmaddfp	c01, a1, bp2, c01
	vmaddfp	c02, a2, bp2, c02
	vmaddfp	c03, a3, bp2, c03
	vmaddfp	c04, a4, bp2, c04

	addi	AO, AO, 16 * SIZE

	LOAD_A	a1, OFFSET_0, AO
	LOAD_A	a2, OFFSET_1, AO
	LOAD_A	a3, OFFSET_2, AO
	LOAD_A	a4, OFFSET_3, AO

	vspltw	bp1, b1,  2
	vmaddfp	c01, a1, bp1, c01
	vmaddfp	c02, a2, bp1, c02
	vmaddfp	c03, a3, bp1, c03
	vmaddfp	c04, a4, bp1, c04

	addi	AO, AO, 16 * SIZE
	addi	BO, BO,  3 * SIZE
	b	LL(138)
	.align 4

LL(136):
	cmpwi	cr0, r0, 2
	bne	LL(137)

	vspltw	bp1, b1,  0
	vspltw	bp2, b1,  1

	vmaddfp	c01, a1, bp1, c01
	vmaddfp	c02, a2, bp1, c02
	vmaddfp	c03, a3, bp1, c03
	vmaddfp	c04, a4, bp1, c04

	LOAD_A	a1, OFFSET_4, AO
	LOAD_A	a2, OFFSET_5, AO
	LOAD_A	a3, OFFSET_6, AO
	LOAD_A	a4, OFFSET_7, AO

	vmaddfp	c01, a1, bp2, c01
	vmaddfp	c02, a2, bp2, c02
	vmaddfp	c03, a3, bp2, c03
	vmaddfp	c04, a4, bp2, c04

	addi	AO, AO, 32 * SIZE
	addi	BO, BO,  2 * SIZE
	b	LL(138)
	.align 4

LL(137):
	cmpwi	cr0, r0, 1
	bne	LL(138)

	vspltw	bp1, b1,  0

	vmaddfp	c01, a1, bp1, c01
	vmaddfp	c02, a2, bp1, c02
	vmaddfp	c03, a3, bp1, c03
	vmaddfp	c04, a4, bp1, c04

	addi	AO, AO, 16 * SIZE
	addi	BO, BO,  1 * SIZE
	.align 4

LL(138):
	lvx	alpha, OFFSET_0, SP
	vxor	VZERO, VZERO, VZERO

	lvx	C1, OFFSET_0, CO1
	lvx	C2, OFFSET_1, CO1
	lvx	C3, OFFSET_2, CO1
	lvx	C4, OFFSET_3, CO1
	lvx	C5, OFFSET_4, CO1

	lvsr	PERMRSHIFT1, 0, CO1

	vperm	c00, VZERO, c01,   PERMRSHIFT1
	vperm	c01, c01,   c02,   PERMRSHIFT1
	vperm	c02, c02,   c03,   PERMRSHIFT1
	vperm	c03, c03,   c04,   PERMRSHIFT1
	vperm	c04, c04,   VZERO, PERMRSHIFT1

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c01, alpha, c01, C2
	vmaddfp	c02, alpha, c02, C3
	vmaddfp	c03, alpha, c03, C4
	vmaddfp	c04, alpha, c04, C5

	stvx	c00, OFFSET_0, CO1
	stvx	c01, OFFSET_1, CO1
	stvx	c02, OFFSET_2, CO1
	stvx	c03, OFFSET_3, CO1
	stvx	c04, OFFSET_4, CO1

	addi	CO1, CO1, 16 * SIZE
	addic.	I, I, -1
	bgt+	LL(130)
	.align 4

LL(140):
	andi.	I, M,  8
	ble	LL(150)

	vxor	c01, c01, c01
	vxor	c02, c02, c02

	mr	BO, B

	mr	J, K

	andi.	r0,  B,  15
	ble+	LL(141)

	LOAD_A	a1, OFFSET_0, AO
	LOAD_A	a2, OFFSET_1, AO
	LOAD_B	b1, OFFSET_0, BO
	vspltw	bp1, b1,  2
	vspltw	bp2, b1,  3

	addi	AO, AO, 8 * SIZE
	addi	BO, BO, SIZE

	vmaddfp	c01, a1, bp1, c01
	vmaddfp	c02, a2, bp1, c02
	subi	J, J, 1
	cmpwi	cr0, J, 0
	ble	LL(148)

	LOAD_A	a1, OFFSET_0, AO
	LOAD_A	a2, OFFSET_1, AO

	addi	AO, AO, 8 * SIZE
	addi	BO, BO, SIZE

	vmaddfp	c01, a1, bp2, c01
	vmaddfp	c02, a2, bp2, c02
	subi	J, J, 1
	cmpwi	cr0, J, 0
	ble	LL(148)
	.align 4


LL(141):
	LOAD_A	a1, OFFSET_0, AO
	LOAD_A	a2, OFFSET_1, AO
	LOAD_A	a3, OFFSET_2, AO
	LOAD_A	a4, OFFSET_3, AO
	LOAD_A	a5, OFFSET_4, AO
	LOAD_A	a6, OFFSET_5, AO
	LOAD_A	a7, OFFSET_6, AO
	LOAD_A	a8, OFFSET_7, AO

	LOAD_B	b1, OFFSET_0, BO

	srawi.	r0,  J,  2
	mtspr	CTR, r0
	ble	LL(145)
	.align 4

LL(143):
	vspltw	bp1, b1,  0
	vmaddfp	c01, a1, bp1, c01
	vmaddfp	c02, a2, bp1, c02

	vspltw	bp2, b1,  1
	vmaddfp	c01, a3, bp2, c01
	vmaddfp	c02, a4, bp2, c02

	vspltw	bp1, b1,  2
	vmaddfp	c01, a5, bp1, c01
	vmaddfp	c02, a6, bp1, c02

	vspltw	bp2, b1,  3
	vmaddfp	c01, a7, bp2, c01
	vmaddfp	c02, a8, bp2, c02

	addi	AO, AO, 32 * SIZE
	addi	BO, BO,  4 * SIZE

	LOAD_A	a1, OFFSET_0, AO
	LOAD_A	a2, OFFSET_1, AO
	LOAD_A	a3, OFFSET_2, AO
	LOAD_A	a4, OFFSET_3, AO

	LOAD_A	a5, OFFSET_4, AO
	LOAD_A	a6, OFFSET_5, AO
	LOAD_A	a7, OFFSET_6, AO
	LOAD_A	a8, OFFSET_7, AO

	LOAD_B	b1, OFFSET_0, BO

	bdnz	LL(143)
	.align 4

LL(145):
	andi.	r0,  J,  3
	ble+	LL(148)

	cmpwi	cr0, r0, 3
	bne	LL(146)

	vspltw	bp1, b1,  0
	vmaddfp	c01, a1, bp1, c01
	vmaddfp	c02, a2, bp1, c02

	vspltw	bp2, b1,  1
	vmaddfp	c01, a3, bp2, c01
	vmaddfp	c02, a4, bp2, c02

	LOAD_A	a1, OFFSET_4, AO
	LOAD_A	a2, OFFSET_5, AO

	vspltw	bp1, b1,  2
	vmaddfp	c01, a1, bp1, c01
	vmaddfp	c02, a2, bp1, c02


	addi	AO, AO, 24 * SIZE
	addi	BO, BO,  3 * SIZE
	b	LL(148)
	.align 4

LL(146):
	cmpwi	cr0, r0, 2
	bne	LL(147)

	vspltw	bp1, b1,  0
	vspltw	bp2, b1,  1

	vmaddfp	c01, a1, bp1, c01
	vmaddfp	c02, a2, bp1, c02

	vmaddfp	c01, a3, bp2, c01
	vmaddfp	c02, a4, bp2, c02

	addi	AO, AO, 16 * SIZE
	addi	BO, BO,  2 * SIZE
	b	LL(148)
	.align 4

LL(147):
	cmpwi	cr0, r0, 1
	bne	LL(148)

	vspltw	bp1, b1,  0

	vmaddfp	c01, a1, bp1, c01
	vmaddfp	c02, a2, bp1, c02

	addi	AO, AO,  8 * SIZE
	addi	BO, BO,  1 * SIZE
	.align 4

LL(148):
	lvx	alpha, OFFSET_0, SP
	vxor	VZERO, VZERO, VZERO

	lvx	C1, OFFSET_0, CO1
	lvx	C2, OFFSET_1, CO1
	lvx	C3, OFFSET_2, CO1

	lvsr	PERMRSHIFT1, 0, CO1

	vperm	c00, VZERO, c01,   PERMRSHIFT1
	vperm	c01, c01,   c02,   PERMRSHIFT1
	vperm	c02, c02,   VZERO, PERMRSHIFT1

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c01, alpha, c01, C2
	vmaddfp	c02, alpha, c02, C3

	stvx	c00, OFFSET_0, CO1
	stvx	c01, OFFSET_1, CO1
	stvx	c02, OFFSET_2, CO1
	addi	CO1, CO1, 8 * SIZE
	.align 4

LL(150):
	andi.	I, M,  4
	ble	LL(160)

	vxor	c01, c01, c01

	mr	BO, B

	mr	J, K

	andi.	r0,  B,  15
	ble+	LL(151)

	LOAD_A	a1, OFFSET_0, AO
	LOAD_B	b1, OFFSET_0, BO
	vspltw	bp1, b1,  2
	vspltw	bp2, b1,  3

	addi	AO, AO, 4 * SIZE
	addi	BO, BO, SIZE

	vmaddfp	c01, a1, bp1, c01
	subi	J, J, 1
	cmpwi	cr0, J, 0
	ble	LL(158)

	LOAD_A	a1, OFFSET_0, AO
	addi	AO, AO, 4 * SIZE
	addi	BO, BO, SIZE

	vmaddfp	c01, a1, bp2, c01
	subi	J, J, 1
	cmpwi	cr0, J, 0
	ble	LL(158)
	.align 4


LL(151):
	LOAD_A	a1, OFFSET_0, AO
	LOAD_A	a2, OFFSET_1, AO
	LOAD_A	a3, OFFSET_2, AO
	LOAD_A	a4, OFFSET_3, AO
	LOAD_B	b1, OFFSET_0, BO

	srawi.	r0,  J,  2
	mtspr	CTR, r0
	ble	LL(155)
	.align 4

LL(153):
	vspltw	bp1, b1,  0
	vmaddfp	c01, a1, bp1, c01
	vspltw	bp2, b1,  1
	vmaddfp	c01, a2, bp2, c01
	vspltw	bp1, b1,  2
	vmaddfp	c01, a3, bp1, c01
	vspltw	bp2, b1,  3
	vmaddfp	c01, a4, bp2, c01

	addi	AO, AO, 16 * SIZE
	addi	BO, BO,  4 * SIZE

	LOAD_A	a1, OFFSET_0, AO
	LOAD_A	a2, OFFSET_1, AO
	LOAD_A	a3, OFFSET_2, AO
	LOAD_A	a4, OFFSET_3, AO

	LOAD_B	b1, OFFSET_0, BO

	bdnz	LL(153)
	.align 4

LL(155):
	andi.	r0,  J,  3
	ble+	LL(158)

	cmpwi	cr0, r0, 3
	bne	LL(156)

	vspltw	bp1, b1,  0
	vmaddfp	c01, a1, bp1, c01
	vspltw	bp2, b1,  1
	vmaddfp	c01, a2, bp2, c01
	vspltw	bp1, b1,  2
	vmaddfp	c01, a3, bp1, c01

	addi	AO, AO, 12 * SIZE
	addi	BO, BO,  3 * SIZE
	b	LL(158)
	.align 4

LL(156):
	cmpwi	cr0, r0, 2
	bne	LL(157)

	vspltw	bp1, b1,  0
	vspltw	bp2, b1,  1

	vmaddfp	c01, a1, bp1, c01
	vmaddfp	c01, a2, bp2, c01

	addi	AO, AO,  8 * SIZE
	addi	BO, BO,  2 * SIZE
	b	LL(158)
	.align 4

LL(157):
	cmpwi	cr0, r0, 1
	bne	LL(158)

	vspltw	bp1, b1,  0

	vmaddfp	c01, a1, bp1, c01

	addi	AO, AO,  4 * SIZE
	addi	BO, BO,  1 * SIZE
	.align 4

LL(158):
	lvx	alpha, OFFSET_0, SP
	vxor	VZERO, VZERO, VZERO

	lvx	C1, OFFSET_0, CO1
	lvx	C2, OFFSET_1, CO1

	lvsr	PERMRSHIFT1, 0, CO1

	vperm	c00, VZERO, c01,   PERMRSHIFT1
	vperm	c01, c01,   VZERO, PERMRSHIFT1

	vmaddfp	c00, alpha, c00, C1
	vmaddfp	c01, alpha, c01, C2

	stvx	c00, OFFSET_0, CO1
	stvx	c01, OFFSET_1, CO1
	addi	CO1, CO1, 4 * SIZE
	.align 4

LL(160):
	andi.	I, M,  2
	ble	LL(170)

	mr	BO, B

	LFD	f8,   0 * SIZE(AO)
	LFD	f9,   1 * SIZE(AO)
	LFD	f10,  2 * SIZE(AO)
	LFD	f11,  3 * SIZE(AO)

	LFD	f12,  0 * SIZE(B)
	LFD	f13,  1 * SIZE(B)

	lfs	f0,  FZERO(SP)
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

	srawi.	r0,  K,  1
	mtspr	CTR, r0
	ble	LL(165)
	.align 4

LL(162):
	FMADD	f0,  f8,  f12, f0
	FMADD	f1,  f9,  f12, f1
	FMADD	f2,  f10, f13, f2
	FMADD	f3,  f11, f13, f3

	LFD	f8,   4 * SIZE(AO)
	LFD	f9,   5 * SIZE(AO)
	LFD	f10,  6 * SIZE(AO)
	LFD	f11,  7 * SIZE(AO)

	LFD	f12,  2 * SIZE(BO)
	LFD	f13,  3 * SIZE(BO)

	addi	AO, AO,  4 * SIZE
	addi	BO, BO,  2 * SIZE
	bdnz	LL(162)
	.align 4

LL(165):
	andi.	r0,  K,  1
	lfs	f13,  ALPHA(SP)
	ble	LL(168)
	.align 4

LL(166):
	FMADD	f0,  f8, f12, f0
	FMADD	f1,  f9, f12, f1

	addi	AO, AO,  2 * SIZE
	addi	BO, BO,  1 * SIZE
	.align 4

LL(168):
	LFD	f8,  0 * SIZE(CO1)
	LFD	f9,  1 * SIZE(CO1)

	FADD	f0, f0, f2
	FADD	f1, f1, f3

	FMADD	f0,  f0, f13, f8
	FMADD	f1,  f1, f13, f9

	STFD	f0,  0 * SIZE(CO1)
	STFD	f1,  1 * SIZE(CO1)

	addi	CO1, CO1, 2 * SIZE
	.align 4

LL(170):
	andi.	I, M,  1
	ble	LL(999)

	mr	BO, B

	LFD	f8,   0 * SIZE(AO)
	LFD	f9,   1 * SIZE(AO)

	LFD	f10,  0 * SIZE(B)
	LFD	f11,  1 * SIZE(B)

	lfs	f0,  FZERO(SP)
 	fmr	f1,  f0

	srawi.	r0,  K,  1
	mtspr	CTR, r0
	ble	LL(175)
	.align 4

LL(172):
	FMADD	f0,  f8, f10, f0
	FMADD	f1,  f9, f11, f1

	LFD	f8,   2 * SIZE(AO)
	LFD	f9,   3 * SIZE(AO)
	LFD	f10,  2 * SIZE(BO)
	LFD	f11,  3 * SIZE(BO)

	addi	AO, AO,  2 * SIZE
	addi	BO, BO,  2 * SIZE
	bdnz	LL(172)
	.align 4

LL(175):
	andi.	r0,  K,  1
	lfs	f13,  ALPHA(SP)
	ble	LL(178)
	.align 4

LL(176):
	FMADD	f0,  f8, f10, f0

	addi	AO, AO,  1 * SIZE
	addi	BO, BO,  1 * SIZE
	.align 4

LL(178):
	LFD	f8,  0 * SIZE(CO1)

	FADD	f0, f0, f1

	FMADD	f0,  f0, f13, f8

	STFD	f0,  0 * SIZE(CO1)
	.align 4

LL(999):
	mr	SP, STACK

	li	r0,  0 * 16
	lvx	v20, SP, r0
	li	r0,  1 * 16
	lvx	v21, SP, r0
	li	r0,  2 * 16
	lvx	v22, SP, r0
	li	r0,  3 * 16
	lvx	v23, SP, r0
	li	r0,  4 * 16
	lvx	v24, SP, r0
	li	r0,  5 * 16
	lvx	v25, SP, r0
	li	r0,  6 * 16
	lvx	v26, SP, r0
	li	r0,  7 * 16
	lvx	v27, SP, r0
	li	r0,  8 * 16
	lvx	v28, SP, r0
	li	r0,  9 * 16
	lvx	v29, SP, r0
	li	r0, 10 * 16
	lvx	v30, SP, r0
	li	r0, 11 * 16
	lvx	v31, SP, r0

	mtspr	VRsave, VREG

#ifdef __64BIT__
	ld	r31,  192(SP)
	ld	r30,  200(SP)
	ld	r29,  208(SP)
	ld	r28,  216(SP)
	ld	r27,  224(SP)
	ld	r26,  232(SP)
	ld	r25,  240(SP)
	ld	r24,  248(SP)
	ld	r23,  256(SP)
	ld	r22,  264(SP)
	ld	r21,  272(SP)
	ld	r20,  280(SP)
	ld	r19,  288(SP)
	ld	r18,  296(SP)
	ld	r17,  304(SP)
	ld	r16,  312(SP)
	ld	r15,  320(SP)
	ld	r14,  328(SP)
#else
	lwz	r31,  192(SP)
	lwz	r30,  196(SP)
	lwz	r29,  200(SP)
	lwz	r28,  204(SP)
	lwz	r27,  208(SP)
	lwz	r26,  212(SP)
	lwz	r25,  216(SP)
	lwz	r24,  220(SP)
	lwz	r23,  224(SP)
	lwz	r22,  228(SP)
	lwz	r21,  232(SP)
	lwz	r20,  236(SP)
	lwz	r19,  240(SP)
	lwz	r18,  244(SP)
	lwz	r17,  248(SP)
	lwz	r16,  252(SP)
	lwz	r15,  256(SP)
	lwz	r14,  260(SP)
#endif

	addi	SP, SP, STACKSIZE

	blr

	EPILOGUE
#endif
