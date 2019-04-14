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

#define ALIGN_SIZE	0xffff
#define SWAP		  0
#define NEG		 16
#define ALPHA_R		 32
#define ALPHA_I		 48
#define FZERO		 64

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
#define A	r8
#define	B	r9
#define	C	r10
#define	LDC	r6
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#if !defined(__64BIT__) && defined(DOUBLE)
#define A	r10
#define	B	r6
#define	C	r7
#define	LDC	r8
#else
#define A	r8
#define	B	r9
#define	C	r10
#define	LDC	r6
#endif
#endif

#define STACK	r11

#define	I	r21
#define J	r22
#define AO	r23
#define	BO	r24
#define	CO1	r25
#define CO2	r26

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

#define c00	v24

#define VZERO		 v25
#define PERMRSHIFT1	 v26
#define PERMRSHIFT2	 v27

#define swap		 v28
#define neg		 v29
#define alpha_r		 v30
#define alpha_i		 v31

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


#ifdef linux
#ifdef __64BIT__
	ld	LDC,   FRAMESLOT(0) + STACKSIZE(SP)
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#ifdef __64BIT__
	ld	LDC,   FRAMESLOT(0) + STACKSIZE(SP)
#else
#ifdef DOUBLE
	lwz	B,     FRAMESLOT(0) + STACKSIZE(SP)
	lwz	C,     FRAMESLOT(1) + STACKSIZE(SP)
	lwz	LDC,   FRAMESLOT(2) + STACKSIZE(SP)
#else
	lwz	LDC,   FRAMESLOT(0) + STACKSIZE(SP)
#endif
#endif
#endif

#ifndef PREFETCHTEST
#ifdef PPC970
	li	PREC,   16 * SIZE
#endif
#else

#ifdef linux
#ifndef __64BIT__
	lwz	PREB,  FRAMESLOT(2) + STACKSIZE(SP)
	lwz	PREC,  FRAMESLOT(3) + STACKSIZE(SP)
#else
	ld	PREB,  FRAMESLOT(3) + STACKSIZE(SP)
	ld	PREC,  FRAMESLOT(4) + STACKSIZE(SP)
#endif
#endif

#if defined(_AIX) || defined(__APPLE__)
#ifdef __64BIT__
	ld	PREB,  FRAMESLOT(3) + STACKSIZE(SP)
	ld	PREC,  FRAMESLOT(4) + STACKSIZE(SP)
#else
#ifdef DOUBLE
	lwz	PREB,  FRAMESLOT(4) + STACKSIZE(SP)
	lwz	PREC,  FRAMESLOT(5) + STACKSIZE(SP)
#else
	lwz	PREB,  FRAMESLOT(3) + STACKSIZE(SP)
	lwz	PREC,  FRAMESLOT(4) + STACKSIZE(SP)
#endif
#endif
#endif

#endif

#ifndef PREFETCHTEST
#ifdef CELL
	li	PREB,   (3 * 32 * SIZE)
#else
	li	PREB,   (5 * 32 * SIZE)
#endif
#endif

	li	r0, -1
	mfspr	VREG, VRsave

	mtspr	VRsave, r0

	addi	SP, SP, -128
	li	r0, -8192

	and	SP, SP, r0

	fneg	f3, f1
	fneg	f4, f2

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
      defined(NC) || defined(TC) || defined(NR) || defined(TR)
	stfs	f1,  ALPHA_R +  0(SP)
	stfs	f1,  ALPHA_R +  4(SP)
	stfs	f1,  ALPHA_R +  8(SP)
	stfs	f1,  ALPHA_R + 12(SP)

	stfs	f4,  ALPHA_I +  0(SP)
	stfs	f2,  ALPHA_I +  4(SP)
	stfs	f4,  ALPHA_I +  8(SP)
	stfs	f2,  ALPHA_I + 12(SP)
#else
	stfs	f1,  ALPHA_R +  0(SP)
	stfs	f3,  ALPHA_R +  4(SP)
	stfs	f1,  ALPHA_R +  8(SP)
	stfs	f3,  ALPHA_R + 12(SP)

	stfs	f2,  ALPHA_I +  0(SP)
	stfs	f2,  ALPHA_I +  4(SP)
	stfs	f2,  ALPHA_I +  8(SP)
	stfs	f2,  ALPHA_I + 12(SP)
#endif

	li	I,    Address_L(0x04050607)
	addis	I, I, Address_H(0x04050607)
	stw	I, SWAP +  0(SP)
	li	I,    Address_L(0x00010203)
	addis	I, I, Address_H(0x00010203)
	stw	I, SWAP +  4(SP)
	li	I,    Address_L(0x0c0d0e0f)
	addis	I, I, Address_H(0x0c0d0e0f)
	stw	I, SWAP +  8(SP)
	li	I,    Address_L(0x08090a0b)
	addis	I, I, Address_H(0x08090a0b)
	stw	I, SWAP + 12(SP)

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
      defined(RR) || defined(RC) || defined(CR) || defined(CC)
	lis	I, 0x8000
	stw	I, NEG +  0(SP)
	stw	I, NEG +  8(SP)
	li	I, 0
	stw	I, NEG +  4(SP)
	stw	I, NEG + 12(SP)
#else
	li	I, 0
	stw	I, NEG +  0(SP)
	stw	I, NEG +  8(SP)
	lis	I, 0x8000
	stw	I, NEG +  4(SP)
	stw	I, NEG + 12(SP)
#endif

	li	r0, 0
	stw	r0, FZERO(SP)

	slwi	LDC, LDC, ZBASE_SHIFT

	li	OFFSET_1,  4 * SIZE
	li	OFFSET_2,  8 * SIZE
	li	OFFSET_3, 12 * SIZE
	li	OFFSET_4, 16 * SIZE
	li	OFFSET_5, 20 * SIZE
	li	OFFSET_6, 24 * SIZE
	li	OFFSET_7, 28 * SIZE

	cmpwi	cr0, M, 0
	ble	LL(999)
	cmpwi	cr0, N, 0
	ble	LL(999)
	cmpwi	cr0, K, 0
	ble	LL(999)

	srawi.	J, N,  1
	ble	LL(50)
	.align 4

LL(01):
	mr	CO1, C
	add	CO2, C,  LDC
	add	C,   CO2, LDC

	mr	AO, A
	srawi.	I, M,  3
	ble	LL(20)
	.align 4

LL(11):
	vxor	c01, c01, c01
	LOAD_B	b1, OFFSET_0, B
	vxor	c02, c02, c02
	LOAD_B	b2, OFFSET_1, B
	vxor	c03, c03, c03
	LOAD_A	a1, OFFSET_0, AO
	vxor	c04, c04, c04
	LOAD_A	a2, OFFSET_1, AO
	vxor	c05, c05, c05
	LOAD_A	a3, OFFSET_2, AO
	vxor	c06, c06, c06
	LOAD_A	a4, OFFSET_3, AO
	vxor	c07, c07, c07
	LOAD_A	a5, OFFSET_4, AO
	vxor	c08, c08, c08

	vxor	c09, c09, c09
	dcbtst	CO1, PREC
	vxor	c10, c10, c10
	dcbtst	CO2, PREC
	vxor	c11, c11, c11
	vxor	c12, c12, c12
	vxor	c13, c13, c13
	mr	BO, B
	vxor	c14, c14, c14
	srawi.	r0,  K,  1
	vxor	c15, c15, c15
	mtspr	CTR, r0
	vxor	c16, c16, c16
	vspltw	bp1, b1, 0
	ble	LL(15)
	.align 4

LL(12):
	vmaddfp	c01, a1, bp1, c01
	vspltw	bp2, b1, 1
	vmaddfp	c02, a2, bp1, c02
	DCBT(BO, PREB)
	vmaddfp	c03, a3, bp1, c03
	nop
	vmaddfp	c04, a4, bp1, c04
	LOAD_A	a6, OFFSET_5, AO

	vmaddfp	c05, a1, bp2, c05
	vspltw	bp1, b1, 2
	vmaddfp	c06, a2, bp2, c06
#ifdef CELL
	DCBT(AO, PREA)
#else
	nop
#endif
	vmaddfp	c07, a3, bp2, c07
	nop
	vmaddfp	c08, a4, bp2, c08
	LOAD_A	a7, OFFSET_6, AO

	vmaddfp	c09, a1, bp1, c09
	vspltw	bp2, b1, 3
	vmaddfp	c10, a2, bp1, c10
	LOAD_B	b1, OFFSET_2, BO
	vmaddfp	c11, a3, bp1, c11
	nop
	vmaddfp	c12, a4, bp1, c12
	LOAD_A	a8, OFFSET_7, AO

	vmaddfp	c13, a1, bp2, c13
	vspltw	bp1, b2, 0
	vmaddfp	c14, a2, bp2, c14
	addi	AO, AO, 32 * SIZE
	vmaddfp	c15, a3, bp2, c15
	nop
	vmaddfp	c16, a4, bp2, c16
	LOAD_A	a1, OFFSET_0, AO

	vmaddfp	c01, a5, bp1, c01
	vspltw	bp2, b2, 1
	vmaddfp	c02, a6, bp1, c02
	nop
	vmaddfp	c03, a7, bp1, c03
	nop
	vmaddfp	c04, a8, bp1, c04
	LOAD_A	a2, OFFSET_1, AO

	vmaddfp	c05, a5, bp2, c05
	vspltw	bp1, b2, 2
	vmaddfp	c06, a6, bp2, c06
	nop
	vmaddfp	c07, a7, bp2, c07
	nop
	vmaddfp	c08, a8, bp2, c08
	LOAD_A	a3, OFFSET_2, AO

	vmaddfp	c09, a5, bp1, c09
	vspltw	bp2, b2, 3
	vmaddfp	c10, a6, bp1, c10
	LOAD_B	b2, OFFSET_3, BO
	vmaddfp	c11, a7, bp1, c11
	nop
	vmaddfp	c12, a8, bp1, c12
	LOAD_A	a4, OFFSET_3, AO

	vmaddfp	c13, a5, bp2, c13
	vspltw	bp1, b1, 0
	vmaddfp	c14, a6, bp2, c14
	addi	BO, BO,  8 * SIZE
	vmaddfp	c15, a7, bp2, c15
	LOAD_A	a5, OFFSET_4, AO
	vmaddfp	c16, a8, bp2, c16
	bdnz+	LL(12)
	.align 4

LL(15):
	lvx	swap,    OFFSET_0, SP
	lvx	neg,     OFFSET_1, SP
	lvx	alpha_r, OFFSET_2, SP
	lvx	alpha_i, OFFSET_3, SP

	andi.	r0,  K,  1
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
	addi	AO, AO, 16 * SIZE
	vmaddfp	c11, a3, bp1, c11
	addi	BO, BO,  4 * SIZE
	vmaddfp	c12, a4, bp1, c12
	nop

	vmaddfp	c13, a1, bp2, c13
	vmaddfp	c14, a2, bp2, c14
	vmaddfp	c15, a3, bp2, c15
	vmaddfp	c16, a4, bp2, c16
	.align 4

LL(18):
	vxor	VZERO, VZERO, VZERO

	vperm	c05, c05, c05, swap
	vperm	c06, c06, c06, swap
	vperm	c07, c07, c07, swap
	vperm	c08, c08, c08, swap

	vperm	c13, c13, c13, swap
	vperm	c14, c14, c14, swap
	vperm	c15, c15, c15, swap
	vperm	c16, c16, c16, swap

	vxor	c05, c05, neg
	vxor	c06, c06, neg
	vxor	c07, c07, neg
	vxor	c08, c08, neg

	vxor	c13, c13, neg
	vxor	c14, c14, neg
	vxor	c15, c15, neg
	vxor	c16, c16, neg

	vaddfp	c01, c01, c05
	vaddfp	c02, c02, c06
	vaddfp	c03, c03, c07
	vaddfp	c04, c04, c08

	vaddfp	c09, c09, c13
	vaddfp	c10, c10, c14
	vaddfp	c11, c11, c15
	vaddfp	c12, c12, c16

	vperm	c05, c01, c01, swap
	vperm	c06, c02, c02, swap
	vperm	c07, c03, c03, swap
	vperm	c08, c04, c04, swap

	vperm	c13, c09, c09, swap
	vperm	c14, c10, c10, swap
	vperm	c15, c11, c11, swap
	vperm	c16, c12, c12, swap

	vmaddfp	c01, alpha_r, c01, VZERO
	vmaddfp	c02, alpha_r, c02, VZERO
	vmaddfp	c03, alpha_r, c03, VZERO
	vmaddfp	c04, alpha_r, c04, VZERO

	vmaddfp	c01, alpha_i, c05, c01
	vmaddfp	c02, alpha_i, c06, c02
	vmaddfp	c03, alpha_i, c07, c03
	vmaddfp	c04, alpha_i, c08, c04

	vmaddfp	c09, alpha_r, c09, VZERO
	vmaddfp	c10, alpha_r, c10, VZERO
	vmaddfp	c11, alpha_r, c11, VZERO
	vmaddfp	c12, alpha_r, c12, VZERO

	vmaddfp	c09, alpha_i, c13, c09
	vmaddfp	c10, alpha_i, c14, c10
	vmaddfp	c11, alpha_i, c15, c11
	vmaddfp	c12, alpha_i, c16, c12

	lvx	C1, OFFSET_0, CO1
	lvx	C2, OFFSET_1, CO1
	lvx	C3, OFFSET_2, CO1
	lvx	C4, OFFSET_3, CO1
	lvx	C5, OFFSET_4, CO1

	lvsr	PERMRSHIFT1, 0, CO1
	lvsr	PERMRSHIFT2, 0, CO2

	vperm	c00, VZERO, c01,   PERMRSHIFT1
	vperm	c01, c01,   c02,   PERMRSHIFT1
	vperm	c02, c02,   c03,   PERMRSHIFT1
	vperm	c03, c03,   c04,   PERMRSHIFT1
	vperm	c04, c04,   VZERO, PERMRSHIFT1

	vaddfp	c00, c00, C1
	vaddfp	c01, c01, C2
	vaddfp	c02, c02, C3
	vaddfp	c03, c03, C4
	vaddfp	c04, c04, C5

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

	vperm	c00, VZERO, c09,   PERMRSHIFT2
	vperm	c09, c09,   c10,   PERMRSHIFT2
	vperm	c10, c10,   c11,   PERMRSHIFT2
	vperm	c11, c11,   c12,   PERMRSHIFT2
	vperm	c12, c12,   VZERO, PERMRSHIFT2

	vaddfp	c00, c00, C1
	vaddfp	c09, c09, C2
	vaddfp	c10, c10, C3
	vaddfp	c11, c11, C4
	vaddfp	c12, c12, C5

	stvx	c00, OFFSET_0, CO2
	stvx	c09, OFFSET_1, CO2
	stvx	c10, OFFSET_2, CO2
	stvx	c11, OFFSET_3, CO2
	stvx	c12, OFFSET_4, CO2

	addi	CO1, CO1, 16 * SIZE
	addi	CO2, CO2, 16 * SIZE
	addic.	I, I, -1
	bgt+	LL(11)
	.align 4

LL(20):
	andi.	I, M,  4
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
	vxor	VZERO, VZERO, VZERO

	lvx	swap,    OFFSET_0, SP
	lvx	neg,     OFFSET_1, SP
	lvx	alpha_r, OFFSET_2, SP
	lvx	alpha_i, OFFSET_3, SP

	vperm	c05, c05, c05, swap
	vperm	c06, c06, c06, swap
	vperm	c13, c13, c13, swap
	vperm	c14, c14, c14, swap

	vxor	c05, c05, neg
	vxor	c06, c06, neg
	vxor	c13, c13, neg
	vxor	c14, c14, neg

	vaddfp	c01, c01, c05
	vaddfp	c02, c02, c06
	vaddfp	c09, c09, c13
	vaddfp	c10, c10, c14

	vperm	c05, c01, c01, swap
	vperm	c06, c02, c02, swap
	vperm	c13, c09, c09, swap
	vperm	c14, c10, c10, swap

	vmaddfp	c01, alpha_r, c01, VZERO
	vmaddfp	c02, alpha_r, c02, VZERO
	vmaddfp	c01, alpha_i, c05, c01
	vmaddfp	c02, alpha_i, c06, c02

	vmaddfp	c09, alpha_r, c09, VZERO
	vmaddfp	c10, alpha_r, c10, VZERO
	vmaddfp	c09, alpha_i, c13, c09
	vmaddfp	c10, alpha_i, c14, c10

	lvx	C1, OFFSET_0, CO1
	lvx	C2, OFFSET_1, CO1
	lvx	C3, OFFSET_2, CO1

	lvsr	PERMRSHIFT1, 0, CO1
	lvsr	PERMRSHIFT2, 0, CO2

	vperm	c00, VZERO, c01,   PERMRSHIFT1
	vperm	c01, c01,   c02,   PERMRSHIFT1
	vperm	c02, c02, VZERO,   PERMRSHIFT1

	vaddfp	c00, c00, C1
	vaddfp	c01, c01, C2
	vaddfp	c02, c02, C3

	stvx	c00, OFFSET_0, CO1
	stvx	c01, OFFSET_1, CO1
	stvx	c02, OFFSET_2, CO1

	lvx	C1, OFFSET_0, CO2
	lvx	C2, OFFSET_1, CO2
	lvx	C3, OFFSET_2, CO2

	vperm	c00, VZERO, c09,   PERMRSHIFT2
	vperm	c09, c09,   c10,   PERMRSHIFT2
	vperm	c10, c10,   VZERO, PERMRSHIFT2

	vaddfp	c00, c00, C1
	vaddfp	c09, c09, C2
	vaddfp	c10, c10, C3

	stvx	c00, OFFSET_0, CO2
	stvx	c09, OFFSET_1, CO2
	stvx	c10, OFFSET_2, CO2

	addi	CO1, CO1, 8 * SIZE
	addi	CO2, CO2, 8 * SIZE
	.align 4

LL(30):
	andi.	I, M,  2
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

	vxor	VZERO, VZERO, VZERO

	lvx	swap,    OFFSET_0, SP
	lvx	neg,     OFFSET_1, SP
	lvx	alpha_r, OFFSET_2, SP
	lvx	alpha_i, OFFSET_3, SP

	vperm	c05, c05, c05, swap
	vperm	c13, c13, c13, swap

	vxor	c05, c05, neg
	vxor	c13, c13, neg

	vaddfp	c01, c01, c05
	vaddfp	c09, c09, c13

	vperm	c05, c01, c01, swap
	vperm	c13, c09, c09, swap

	vmaddfp	c01, alpha_r, c01, VZERO
	vmaddfp	c01, alpha_i, c05, c01

	vmaddfp	c09, alpha_r, c09, VZERO
	vmaddfp	c09, alpha_i, c13, c09

	lvx	C1, OFFSET_0, CO1
	lvx	C2, OFFSET_1, CO1

	lvsr	PERMRSHIFT1, 0, CO1
	lvsr	PERMRSHIFT2, 0, CO2

	vperm	c00, VZERO, c01,   PERMRSHIFT1
	vperm	c01, c01, VZERO,   PERMRSHIFT1

	vaddfp	c00, c00, C1
	vaddfp	c01, c01, C2

	stvx	c00, OFFSET_0, CO1
	stvx	c01, OFFSET_1, CO1

	lvx	C1, OFFSET_0, CO2
	lvx	C2, OFFSET_1, CO2

	vperm	c00, VZERO, c09,   PERMRSHIFT2
	vperm	c09, c09,   VZERO, PERMRSHIFT2

	vaddfp	c00, c00, C1
	vaddfp	c09, c09, C2

	stvx	c00, OFFSET_0, CO2
	stvx	c09, OFFSET_1, CO2

	addi	CO1, CO1,  4 * SIZE
	addi	CO2, CO2,  4 * SIZE
	.align 4

LL(40):
	andi.	I, M,  1
	ble	LL(49)

	mr	BO, B

	LFD	f8,   0 * SIZE(AO)
	LFD	f9,   1 * SIZE(AO)

	LFD	f10,  0 * SIZE(BO)
	LFD	f11,  1 * SIZE(BO)
	LFD	f12,  2 * SIZE(BO)
	LFD	f13,  3 * SIZE(BO)

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
	fmadd	f0,  f8, f10, f0
	fmadd	f2,  f8, f11, f2
	fmadd	f4,  f8, f12, f4
	fmadd	f6,  f8, f13, f6

	fmadd	f1,  f9, f10, f1
	fmadd	f3,  f9, f11, f3
	fmadd	f5,  f9, f12, f5
	fmadd	f7,  f9, f13, f7

	LFD	f8,   2 * SIZE(AO)
	LFD	f9,   3 * SIZE(AO)

	LFD	f10,  4 * SIZE(BO)
	LFD	f11,  5 * SIZE(BO)
	LFD	f12,  6 * SIZE(BO)
	LFD	f13,  7 * SIZE(BO)

	fmadd	f0,  f8, f10, f0
	fmadd	f2,  f8, f11, f2
	fmadd	f4,  f8, f12, f4
	fmadd	f6,  f8, f13, f6

	fmadd	f1,  f9, f10, f1
	fmadd	f3,  f9, f11, f3
	fmadd	f5,  f9, f12, f5
	fmadd	f7,  f9, f13, f7

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
	fmadd	f0,  f8, f10, f0
	fmadd	f2,  f8, f11, f2
	fmadd	f4,  f8, f12, f4
	fmadd	f6,  f8, f13, f6

	fmadd	f1,  f9, f10, f1
	fmadd	f3,  f9, f11, f3
	fmadd	f5,  f9, f12, f5
	fmadd	f7,  f9, f13, f7

	addi	AO, AO,  2 * SIZE
	addi	BO, BO,  4 * SIZE
	.align 4

LL(48):
#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	fsub	f0, f0, f3
	fadd	f1, f1, f2
	fsub	f4, f4, f7
	fadd	f5, f5, f6
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
	fadd	f0, f0, f3
	fsub	f1, f1, f2
	fadd	f4, f4, f7
	fsub	f5, f5, f6
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
	fadd	f0, f0, f3
	fsub	f1, f2, f1
	fadd	f4, f4, f7
	fsub	f5, f6, f5
#else /* RR, RC, CR, CC */
	fsub	f0, f0, f3
	fadd	f1, f1, f2
	fsub	f4, f4, f7
	fadd	f5, f5, f6
#endif

	LFD	f8,  0 * SIZE(CO1)
	LFD	f9,  1 * SIZE(CO1)
	LFD	f10, 0 * SIZE(CO2)
	LFD	f11, 1 * SIZE(CO2)

	lfs	f12,  ALPHA_R + 0(SP)
	lfs	f13,  ALPHA_I + 4(SP)

#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
	fmadd	f8,  f12, f0, f8
	fnmsub	f9,  f12, f1, f9
	fmadd	f10, f12, f4, f10
	fnmsub	f11, f12, f5, f11

	fmadd	f8,  f13, f1, f8
	fmadd	f9,  f13, f0, f9
	fmadd	f10, f13, f5, f10
	fmadd	f11, f13, f4, f11
#else
	fmadd	f8,  f12, f0, f8
	fmadd	f9,  f12, f1, f9
	fmadd	f10, f12, f4, f10
	fmadd	f11, f12, f5, f11

	fnmsub	f8,  f13, f1, f8
	fmadd	f9,  f13, f0, f9
	fnmsub	f10, f13, f5, f10
	fmadd	f11, f13, f4, f11
#endif

	STFD	f8,  0 * SIZE(CO1)
	STFD	f9,  1 * SIZE(CO1)
	STFD	f10, 0 * SIZE(CO2)
	STFD	f11, 1 * SIZE(CO2)

LL(49):
	mr	B, BO

	addic.	J, J, -1
	bgt	LL(01)
	.align 4

LL(50):
	andi.	J, N,  1
	ble	LL(999)

	mr	CO1, C
	mr	AO, A

	srawi.	I, M,  3
	ble	LL(70)
	.align 4

LL(61):
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
	ble	LL(65)
	.align 4

LL(62):
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
	bdnz	LL(62)
	.align 4

LL(65):
	andi.	r0,  K,  1
	ble+	LL(68)
	.align 4

LL(66):
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

LL(68):
	vxor	VZERO, VZERO, VZERO

	lvx	swap,    OFFSET_0, SP
	lvx	neg,     OFFSET_1, SP
	lvx	alpha_r, OFFSET_2, SP
	lvx	alpha_i, OFFSET_3, SP

	vperm	c05, c05, c05, swap
	vperm	c06, c06, c06, swap
	vperm	c07, c07, c07, swap
	vperm	c08, c08, c08, swap

	vxor	c05, c05, neg
	vxor	c06, c06, neg
	vxor	c07, c07, neg
	vxor	c08, c08, neg

	vaddfp	c01, c01, c05
	vaddfp	c02, c02, c06
	vaddfp	c03, c03, c07
	vaddfp	c04, c04, c08

	vperm	c05, c01, c01, swap
	vperm	c06, c02, c02, swap
	vperm	c07, c03, c03, swap
	vperm	c08, c04, c04, swap

	vmaddfp	c01, alpha_r, c01, VZERO
	vmaddfp	c02, alpha_r, c02, VZERO
	vmaddfp	c03, alpha_r, c03, VZERO
	vmaddfp	c04, alpha_r, c04, VZERO

	vmaddfp	c01, alpha_i, c05, c01
	vmaddfp	c02, alpha_i, c06, c02
	vmaddfp	c03, alpha_i, c07, c03
	vmaddfp	c04, alpha_i, c08, c04

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

	vaddfp	c00, c00, C1
	vaddfp	c01, c01, C2
	vaddfp	c02, c02, C3
	vaddfp	c03, c03, C4
	vaddfp	c04, c04, C5

	stvx	c00, OFFSET_0, CO1
	stvx	c01, OFFSET_1, CO1
	stvx	c02, OFFSET_2, CO1
	stvx	c03, OFFSET_3, CO1
	stvx	c04, OFFSET_4, CO1

	addi	CO1, CO1, 16 * SIZE
	addic.	I, I, -1
	bgt+	LL(61)
	.align 4

LL(70):
	andi.	I, M,  4
	ble	LL(80)

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
	ble	LL(75)
	.align 4

LL(72):
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
	bdnz	LL(72)
	.align 4

LL(75):
	andi.	r0,  K,  1
	ble+	LL(78)
	.align 4

LL(76):
	vmaddfp	c01, a1, bp1, c01
	vspltw	bp2, b1, 1
	vmaddfp	c02, a2, bp1, c02
	addi	AO, AO,  8 * SIZE
	vmaddfp	c05, a1, bp2, c05
	addi	BO, BO,  2 * SIZE
	vmaddfp	c06, a2, bp2, c06
	.align 4

LL(78):
	vaddfp	c01, c01, c03
	vaddfp	c02, c02, c04
	vaddfp	c05, c05, c07
	vaddfp	c06, c06, c08

	vxor	VZERO, VZERO, VZERO

	lvx	swap,    OFFSET_0, SP
	lvx	neg,     OFFSET_1, SP
	lvx	alpha_r, OFFSET_2, SP
	lvx	alpha_i, OFFSET_3, SP

	vperm	c05, c05, c05, swap
	vperm	c06, c06, c06, swap

	vxor	c05, c05, neg
	vxor	c06, c06, neg

	vaddfp	c01, c01, c05
	vaddfp	c02, c02, c06

	vperm	c05, c01, c01, swap
	vperm	c06, c02, c02, swap

	vmaddfp	c01, alpha_r, c01, VZERO
	vmaddfp	c02, alpha_r, c02, VZERO
	vmaddfp	c01, alpha_i, c05, c01
	vmaddfp	c02, alpha_i, c06, c02

	lvx	C1, OFFSET_0, CO1
	lvx	C2, OFFSET_1, CO1
	lvx	C3, OFFSET_2, CO1

	lvsr	PERMRSHIFT1, 0, CO1

	vperm	c00, VZERO, c01,   PERMRSHIFT1
	vperm	c01, c01,   c02,   PERMRSHIFT1
	vperm	c02, c02, VZERO,   PERMRSHIFT1

	vaddfp	c00, c00, C1
	vaddfp	c01, c01, C2
	vaddfp	c02, c02, C3

	stvx	c00, OFFSET_0, CO1
	stvx	c01, OFFSET_1, CO1
	stvx	c02, OFFSET_2, CO1

	addi	CO1, CO1,  8 * SIZE
	.align 4

LL(80):
	andi.	I, M,  2
	ble	LL(90)

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
	ble	LL(85)
	.align 4

LL(82):
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
	bdnz	LL(82)
	.align 4

LL(85):
	andi.	r0,  K,  1
	ble+	LL(88)
	.align 4

LL(86):
	vspltw	bp2, b1, 1
	vmaddfp	c01, a1, bp1, c01
	vmaddfp	c05, a1, bp2, c05
	addi	AO, AO,  4 * SIZE
	addi	BO, BO,  2 * SIZE
	.align 4

LL(88):
	vaddfp	c01, c01, c02
	vaddfp	c05, c05, c06
	vaddfp	c09, c09, c10
	vaddfp	c13, c13, c14

	vxor	VZERO, VZERO, VZERO

	lvx	swap,    OFFSET_0, SP
	lvx	neg,     OFFSET_1, SP
	lvx	alpha_r, OFFSET_2, SP
	lvx	alpha_i, OFFSET_3, SP

	vperm	c05, c05, c05, swap

	vxor	c05, c05, neg

	vaddfp	c01, c01, c05

	vperm	c05, c01, c01, swap

	vmaddfp	c01, alpha_r, c01, VZERO
	vmaddfp	c01, alpha_i, c05, c01

	lvx	C1, OFFSET_0, CO1
	lvx	C2, OFFSET_1, CO1

	lvsr	PERMRSHIFT1, 0, CO1

	vperm	c00, VZERO, c01,   PERMRSHIFT1
	vperm	c01, c01, VZERO,   PERMRSHIFT1

	vaddfp	c00, c00, C1
	vaddfp	c01, c01, C2

	stvx	c00, OFFSET_0, CO1
	stvx	c01, OFFSET_1, CO1

	addi	CO1, CO1,  4 * SIZE
	.align 4

LL(90):
	andi.	I, M,  1
	ble	LL(999)

	mr	BO, B

	LFD	f8,   0 * SIZE(AO)
	LFD	f9,   1 * SIZE(AO)

	LFD	f10,  0 * SIZE(BO)
	LFD	f11,  1 * SIZE(BO)
	LFD	f12,  2 * SIZE(BO)
	LFD	f13,  3 * SIZE(BO)

	lfs	f0,  FZERO(SP)
 	fmr	f1,  f0
	fmr	f2,  f0
	fmr	f3,  f0

	srawi.	r0,  K,  1
	mtspr	CTR, r0
	ble	LL(95)
	.align 4

LL(92):
	fmadd	f0,  f8, f10, f0
	fmadd	f2,  f8, f11, f2
	fmadd	f1,  f9, f10, f1
	fmadd	f3,  f9, f11, f3

	LFD	f8,   2 * SIZE(AO)
	LFD	f9,   3 * SIZE(AO)
	LFD	f10,  4 * SIZE(BO)
	LFD	f11,  5 * SIZE(BO)

	fmadd	f0,  f8, f12, f0
	fmadd	f2,  f8, f13, f2
	fmadd	f1,  f9, f12, f1
	fmadd	f3,  f9, f13, f3

	LFD	f8,   4 * SIZE(AO)
	LFD	f9,   5 * SIZE(AO)
	LFD	f12,  6 * SIZE(BO)
	LFD	f13,  7 * SIZE(BO)

	addi	AO, AO,  4 * SIZE
	addi	BO, BO,  4 * SIZE
	bdnz	LL(92)
	.align 4

LL(95):
	andi.	r0,  K,  1
	ble	LL(98)
	.align 4

LL(96):
	fmadd	f0,  f8, f10, f0
	fmadd	f2,  f8, f11, f2
	fmadd	f1,  f9, f10, f1
	fmadd	f3,  f9, f11, f3
	.align 4

LL(98):
#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
	fsub	f0, f0, f3
	fadd	f1, f1, f2
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
	fadd	f0, f0, f3
	fsub	f1, f1, f2
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
	fadd	f0, f0, f3
	fsub	f1, f2, f1
#else /* RR, RC, CR, CC */
	fsub	f0, f0, f3
	fadd	f1, f1, f2
#endif

	LFD	f8,  0 * SIZE(CO1)
	LFD	f9,  1 * SIZE(CO1)

	lfs	f12,  ALPHA_R + 0(SP)
	lfs	f13,  ALPHA_I + 4(SP)

#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
	fmadd	f8,  f12, f0, f8
	fnmsub	f9,  f12, f1, f9

	fmadd	f8,  f13, f1, f8
	fmadd	f9,  f13, f0, f9
#else
	fmadd	f8,  f12, f0, f8
	fmadd	f9,  f12, f1, f9

	fnmsub	f8,  f13, f1, f8
	fmadd	f9,  f13, f0, f9
#endif

	STFD	f8,  0 * SIZE(CO1)
	STFD	f9,  1 * SIZE(CO1)
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
