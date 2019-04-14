/*******************************************************************************
Copyright (c) 2017, The OpenBLAS Project
All rights reserved.
Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
1. Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.
2. Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in
the documentation and/or other materials provided with the
distribution.
3. Neither the name of the OpenBLAS project nor the names of
its contributors may be used to endorse or promote products
derived from this software without specific prior written permission.
THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE OPENBLAS PROJECT OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*******************************************************************************/

#define ASSEMBLER
#include "common.h"

/*                   X0          X1          X2          s0        X3        x4       x5           x6  */
/*int CNAME(BLASLONG bm,BLASLONG bn,BLASLONG bk,FLOAT alpha,FLOAT* ba,FLOAT* bb,FLOAT* C,BLASLONG ldc) */

#define origM		x0
#define origN		x1
#define origK		x2
#define origPA		x3
#define origPB		x4
#define pC		x5
#define LDC		x6
#define temp		x7
#define counterL	x8
#define counterI	x9
#define counterJ	x10
#define pB		x11
#define pCRow0		x12
#define pCRow1		x13
#define pCRow2		x14
#define pCRow3		x15
#define pA		x16
#define alpha		w17

#define alpha0		s10
#define alphaV0		v10.s[0]

#define A_PRE_SIZE	2560
#define B_PRE_SIZE	224
#define C_PRE_SIZE	160


// 00 origM
// 01 origN
// 02 origK
// 03 origPA
// 04 origPB
// 05 pC
// 06 origLDC -> LDC
// 07 offset
// 08 counterL
// 09 counterI
// 10 counterJ
// 11 pB
// 12 pCRow0
// 13 pCRow1
// 14 pCRow2
// 15 pA
// 16 temp
// 17
// 18 must save
// 19 must save
// 20 must save
// 21 must save
// 22 must save
// 23 must save
// 24 must save
// 25 must save
// 26 must save
// 27 must save
// 28 must save
// 29 frame
// 30 link
// 31 sp

//v00 ALPHA -> pA0_00, pA0_01, pA0_02, pA0_03
//v01 pA0_04, pA0_05, pA0_06, pA0_07
//v02 pA0_08, pA0_09, pA0_10, pA0_11
//v03 pA0_12, pA0_13, pA0_14, pA0_15
//v04 pA1_00, pA1_01, pA1_02, pA1_03
//v05 pA1_04, pA1_05, pA1_06, pA1_07
//v06 pA1_08, pA1_09, pA1_10, pA1_11
//v07 pA1_12, pA1_13, pA1_14, pA1_15
//v08 must save pB00
//v09 must save pB01
//v10 must save pB02
//v11 must save pB03
//v12 must save pB10
//v13 must save pB11
//v14 must save pB12
//v15 must save pB13
//v16 must save C00, C01, C02, C03
//v17 must save C04, C05, C06, C07
//v18 C08, C09, C10, C11
//v19 C12, C13, C14, C15
//v20 C16, C17, C18, C19
//v21 C20, C21, C22, C23
//v22 C24, C25, C26, C27
//v23 C28, C29, C30, C31
//v24 C32, C33, C34, C35
//v25 C36, C37, C38, C39
//v26 C40, C41, C42, C43
//v27 C44, C45, C46, C47
//v28 C48, C49, C50, C51
//v29 C52, C53, C54, C55
//v30 C56, C57, C58, C59
//v31 C60, C61, C62, C63

/*******************************************************************************
* Macro definitions
*******************************************************************************/

.macro INIT16x4
	fmov		s16, wzr
	fmov		s17, wzr
	fmov		s18, s16
	fmov		s19, s17
	fmov		s20, wzr
	fmov		s21, s16
	fmov		s22, s17
	fmov		s23, s18
	fmov		s24, wzr
	fmov		s25, s16
	fmov		s26, s17
	fmov		s27, s18
	fmov		s28, wzr
	fmov		s29, s16
	fmov		s30, s17
	fmov		s31, s18
.endm

.macro KERNEL16x4_I
	ldur	q0, [pA]
	ldur	q1, [pA, #16]

	ldur	q8, [pB]

	fmul	v16.4s, v0.4s, v8.s[0]
	fmul	v20.4s, v0.4s, v8.s[1]

	fmul	v24.4s, v0.4s, v8.s[2]
	fmul	v28.4s, v0.4s, v8.s[3]

	ldur	q2, [pA, #32]
	ldur	q3, [pA, #48]

	fmul	v17.4s, v1.4s, v8.s[0]
	fmul	v21.4s, v1.4s, v8.s[1]

	ldur	q4, [pA, #64]
	ldur	q5, [pA, #80]

	fmul	v25.4s, v1.4s, v8.s[2]
	fmul	v29.4s, v1.4s, v8.s[3]

	ldur	q12, [pB, #16]

	fmul	v18.4s, v2.4s, v8.s[0]
	fmul	v22.4s, v2.4s, v8.s[1]

	fmul	v19.4s, v3.4s, v8.s[0]
	fmul	v23.4s, v3.4s, v8.s[1]

	ldur	q6, [pA, #96]
	ldur	q7, [pA, #112]

	add	pB, pB, #32
	add	pA, pA, #128

	fmul	v26.4s, v2.4s, v8.s[2]
	fmul	v30.4s, v2.4s, v8.s[3]

	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE]

	fmul	v27.4s, v3.4s, v8.s[2]
	fmul	v31.4s, v3.4s, v8.s[3]

	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE+64]
.endm

.macro KERNEL16x4_M1
	fmla	v16.4s, v0.4s, v8.s[0]
	fmla	v17.4s, v1.4s, v8.s[0]

	ldur	q4, [pA]
	ldur	q5, [pA, #16]

	fmla	v18.4s, v2.4s, v8.s[0]
	fmla	v19.4s, v3.4s, v8.s[0]

	fmla	v20.4s, v0.4s, v8.s[1]
	fmla	v21.4s, v1.4s, v8.s[1]

	ldur	q12, [pB]

	fmla	v22.4s, v2.4s, v8.s[1]
	fmla	v23.4s, v3.4s, v8.s[1]

	add	pB, pB, #16

	fmla	v24.4s, v0.4s, v8.s[2]
	fmla	v25.4s, v1.4s, v8.s[2]

	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE+64]

	fmla	v26.4s, v2.4s, v8.s[2]
	fmla	v27.4s, v3.4s, v8.s[2]

	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE]

	fmla	v28.4s, v0.4s, v8.s[3]
	fmla	v29.4s, v1.4s, v8.s[3]

	ldur	q6, [pA, #32]
	ldur	q7, [pA, #48]
	add	pA, pA, #64

	fmla	v30.4s, v2.4s, v8.s[3]
	fmla	v31.4s, v3.4s, v8.s[3]
.endm

.macro KERNEL16x4_M2
	fmla	v16.4s, v4.4s, v12.s[0]
	fmla	v17.4s, v5.4s, v12.s[0]

	ldur	q0, [pA]
	ldur	q1, [pA, #16]

	fmla	v18.4s, v6.4s, v12.s[0]
	fmla	v19.4s, v7.4s, v12.s[0]

	fmla	v20.4s, v4.4s, v12.s[1]
	fmla	v21.4s, v5.4s, v12.s[1]

	ldur	q8, [pB]

	fmla	v22.4s, v6.4s, v12.s[1]
	fmla	v23.4s, v7.4s, v12.s[1]

	add	pB, pB, #16

	fmla	v24.4s, v4.4s, v12.s[2]
	fmla	v25.4s, v5.4s, v12.s[2]

	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]

	fmla	v26.4s, v6.4s, v12.s[2]
	fmla	v27.4s, v7.4s, v12.s[2]

	ldur	q2, [pA, #32]
	ldur	q3, [pA, #48]
	add	pA, pA, #64

	fmla	v28.4s, v4.4s, v12.s[3]
	fmla	v29.4s, v5.4s, v12.s[3]

	fmla	v30.4s, v6.4s, v12.s[3]
	fmla	v31.4s, v7.4s, v12.s[3]
.endm

.macro KERNEL16x4_E
	fmla	v16.4s, v4.4s, v12.s[0]
	fmla	v20.4s, v4.4s, v12.s[1]
	fmla	v24.4s, v4.4s, v12.s[2]
	fmla	v28.4s, v4.4s, v12.s[3]

	fmla	v17.4s, v5.4s, v12.s[0]
	fmla	v21.4s, v5.4s, v12.s[1]
	fmla	v25.4s, v5.4s, v12.s[2]
	fmla	v29.4s, v5.4s, v12.s[3]

	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]

	fmla	v18.4s, v6.4s, v12.s[0]
	fmla	v22.4s, v6.4s, v12.s[1]
	fmla	v26.4s, v6.4s, v12.s[2]
	fmla	v30.4s, v6.4s, v12.s[3]

	fmla	v19.4s, v7.4s, v12.s[0]
	fmla	v23.4s, v7.4s, v12.s[1]
	fmla	v27.4s, v7.4s, v12.s[2]
	fmla	v31.4s, v7.4s, v12.s[3]
.endm

.macro KERNEL16x4_SUB
	ldur	q0, [pA]
	ldur	q1, [pA, #16]
	ldur	q8, [pB]

	fmla	v16.4s, v0.4s, v8.s[0]
	fmla	v20.4s, v0.4s, v8.s[1]

	add	pB, pB, #16

	fmla	v24.4s, v0.4s, v8.s[2]
	fmla	v28.4s, v0.4s, v8.s[3]

	ldur	q2, [pA, #32]
	ldur	q3, [pA, #48]
	add	pA, pA, #64

	fmla	v17.4s, v1.4s, v8.s[0]
	fmla	v21.4s, v1.4s, v8.s[1]

	fmla	v25.4s, v1.4s, v8.s[2]
	fmla	v29.4s, v1.4s, v8.s[3]

	fmla	v18.4s, v2.4s, v8.s[0]
	fmla	v22.4s, v2.4s, v8.s[1]

	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE]

	fmla	v19.4s, v3.4s, v8.s[0]
	fmla	v23.4s, v3.4s, v8.s[1]

	fmla	v26.4s, v2.4s, v8.s[2]
	fmla	v30.4s, v2.4s, v8.s[3]

	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]

	fmla	v27.4s, v3.4s, v8.s[2]
	fmla	v31.4s, v3.4s, v8.s[3]
.endm

.macro SAVE16x4
	fmov	alpha0, alpha

	prfm	PLDL2KEEP, [pCRow0, #C_PRE_SIZE]
	prfm	PLDL2KEEP, [pCRow1, #C_PRE_SIZE]
	prfm	PLDL2KEEP, [pCRow2, #C_PRE_SIZE]
	prfm	PLDL2KEEP, [pCRow3, #C_PRE_SIZE]

	ldur	q0, [pCRow0]
	ldur	q1, [pCRow0, #16]
	ldur	q2, [pCRow0, #32]
	ldur	q3, [pCRow0, #48]

	ldur	q4, [pCRow1]
	ldur	q5, [pCRow1, #16]
	ldur	q6, [pCRow1, #32]
	ldur	q7, [pCRow1, #48]

	fmla	v0.4s, v16.4s, alphaV0
	fmla	v1.4s, v17.4s, alphaV0
	stp 	q0, q1, [pCRow0]

	fmla	v2.4s, v18.4s, alphaV0
	fmla	v3.4s, v19.4s, alphaV0
	stp 	q2, q3, [pCRow0, #32]
	ldur	q0, [pCRow2]
	ldur	q1, [pCRow2, #16]


	fmla	v4.4s, v20.4s, alphaV0
	fmla	v5.4s, v21.4s, alphaV0
	stp 	q4, q5, [pCRow1]
	ldur	q2, [pCRow2, #32]
	ldur	q3, [pCRow2, #48]

	fmla	v6.4s, v22.4s, alphaV0
	fmla	v7.4s, v23.4s, alphaV0
	stp 	q6, q7, [pCRow1, #32]
	ldur	q4, [pCRow3]
	ldur	q5, [pCRow3, #16]

	fmla	v0.4s, v24.4s, alphaV0
	fmla	v1.4s, v25.4s, alphaV0
	stp 	q0, q1, [pCRow2]
	ldur	q6, [pCRow3, #32]
	ldur	q7, [pCRow3, #48]

	fmla	v2.4s, v26.4s, alphaV0
	fmla	v3.4s, v27.4s, alphaV0
	stp 	q2, q3, [pCRow2, #32]

	fmla	v4.4s, v28.4s, alphaV0
	fmla	v5.4s, v29.4s, alphaV0
	stp 	q4, q5, [pCRow3]

	fmla	v6.4s, v30.4s, alphaV0
	fmla	v7.4s, v31.4s, alphaV0
	stp 	q6, q7, [pCRow3, #32]

	add	pCRow0, pCRow0, #64
	add	pCRow1, pCRow1, #64
	add	pCRow2, pCRow2, #64
	add	pCRow3, pCRow3, #64
.endm

/******************************************************************************/

.macro INIT8x4
	fmov		s16, wzr
	fmov		s17, wzr
	fmov		s20, wzr
	fmov		s21, s16
	fmov		s24, wzr
	fmov		s25, s16
	fmov		s28, wzr
	fmov		s29, s16
.endm

.macro KERNEL8x4_I
	ldp	s8, s9, [pB], #8
	ldp	s10, s11, [pB], #8

	ldr	q0, [pA], #16
	ldr	q1, [pA], #16

	fmul	v16.4s, v0.4s, v8.s[0]
	fmul	v17.4s, v1.4s, v8.s[0]
	fmul	v20.4s, v0.4s, v9.s[0]
	fmul	v21.4s, v1.4s, v9.s[0]
	fmul	v24.4s, v0.4s, v10.s[0]
	fmul	v25.4s, v1.4s, v10.s[0]
	fmul	v28.4s, v0.4s, v11.s[0]
	fmul	v29.4s, v1.4s, v11.s[0]

	ldp	s12, s13, [pB], #8
	ldp	s14, s15, [pB], #8

	ldr	q4, [pA], #16
	ldr	q5, [pA], #16
.endm

.macro KERNEL8x4_M1
	fmla	v16.4s, v0.4s, v8.s[0]
	fmla	v17.4s, v1.4s, v8.s[0]
	fmla	v20.4s, v0.4s, v9.s[0]
	fmla	v21.4s, v1.4s, v9.s[0]
	fmla	v24.4s, v0.4s, v10.s[0]
	fmla	v25.4s, v1.4s, v10.s[0]
	fmla	v28.4s, v0.4s, v11.s[0]
	fmla	v29.4s, v1.4s, v11.s[0]

	ldp	s12, s13, [pB], #8
	ldp	s14, s15, [pB], #8

	ldr	q4, [pA], #16
	ldr	q5, [pA], #16
.endm

.macro KERNEL8x4_M2
	fmla	v16.4s, v4.4s, v12.s[0]
	fmla	v17.4s, v5.4s, v12.s[0]
	fmla	v20.4s, v4.4s, v13.s[0]
	fmla	v21.4s, v5.4s, v13.s[0]
	fmla	v24.4s, v4.4s, v14.s[0]
	fmla	v25.4s, v5.4s, v14.s[0]
	fmla	v28.4s, v4.4s, v15.s[0]
	fmla	v29.4s, v5.4s, v15.s[0]

	ldp	s8, s9, [pB], #8
	ldp	s10, s11, [pB], #8

	ldr	q0, [pA], #16
	ldr	q1, [pA], #16
.endm

.macro KERNEL8x4_E
	fmla	v16.4s, v4.4s, v12.s[0]
	fmla	v17.4s, v5.4s, v12.s[0]
	fmla	v20.4s, v4.4s, v13.s[0]
	fmla	v21.4s, v5.4s, v13.s[0]
	fmla	v24.4s, v4.4s, v14.s[0]
	fmla	v25.4s, v5.4s, v14.s[0]
	fmla	v28.4s, v4.4s, v15.s[0]
	fmla	v29.4s, v5.4s, v15.s[0]
.endm

.macro KERNEL8x4_SUB
	ldp	s8, s9, [pB], #8
	ldp	s10, s11, [pB], #8

	ldr	q0, [pA], #16
	ldr	q1, [pA], #16

	fmla	v16.4s, v0.4s, v8.s[0]
	fmla	v17.4s, v1.4s, v8.s[0]
	fmla	v20.4s, v0.4s, v9.s[0]
	fmla	v21.4s, v1.4s, v9.s[0]
	fmla	v24.4s, v0.4s, v10.s[0]
	fmla	v25.4s, v1.4s, v10.s[0]
	fmla	v28.4s, v0.4s, v11.s[0]
	fmla	v29.4s, v1.4s, v11.s[0]
.endm

.macro SAVE8x4
	fmov	alpha0, alpha

	ldp	q0, q1, [pCRow0]
	fmla	v0.4s, v16.4s, alphaV0
	fmla	v1.4s, v17.4s, alphaV0
	stp 	q0, q1, [pCRow0]

	add	pCRow0, pCRow0, #32

	ldp	q2, q3, [pCRow1]
	fmla	v2.4s, v20.4s, alphaV0
	fmla	v3.4s, v21.4s, alphaV0
	stp 	q2, q3, [pCRow1]

	add	pCRow1, pCRow1, #32

	ldp	q4, q5, [pCRow2]
	fmla	v4.4s, v24.4s, alphaV0
	fmla	v5.4s, v25.4s, alphaV0
	stp 	q4, q5, [pCRow2]

	add	pCRow2, pCRow2, #32

	ldp	q6, q7, [pCRow3]
	fmla	v6.4s, v28.4s, alphaV0
	fmla	v7.4s, v29.4s, alphaV0
	stp 	q6, q7, [pCRow3]

	add	pCRow3, pCRow3, #32
.endm

/******************************************************************************/

.macro INIT4x4
	fmov		s16, wzr
	fmov		s20, wzr
	fmov		s24, wzr
	fmov		s28, wzr
.endm

.macro KERNEL4x4_I
	ldp	s8, s9, [pB], #8
	ldp	s10, s11, [pB], #8

	ldr	q0, [pA], #16

	fmul	v16.4s, v0.4s, v8.s[0]
	fmul	v20.4s, v0.4s, v9.s[0]
	fmul	v24.4s, v0.4s, v10.s[0]
	fmul	v28.4s, v0.4s, v11.s[0]

	ldp	s12, s13, [pB], #8
	ldp	s14, s15, [pB], #8

	ldr	q1, [pA], #16
.endm

.macro KERNEL4x4_M1
	fmla	v16.4s, v0.4s, v8.s[0]
	fmla	v20.4s, v0.4s, v9.s[0]
	fmla	v24.4s, v0.4s, v10.s[0]
	fmla	v28.4s, v0.4s, v11.s[0]

	ldp	s12, s13, [pB], #8
	ldp	s14, s15, [pB], #8

	ldr	q1, [pA], #16
.endm

.macro KERNEL4x4_M2
	fmla	v16.4s, v1.4s, v12.s[0]
	fmla	v20.4s, v1.4s, v13.s[0]
	fmla	v24.4s, v1.4s, v14.s[0]
	fmla	v28.4s, v1.4s, v15.s[0]

	ldp	s8, s9, [pB], #8
	ldp	s10, s11, [pB], #8

	ldr	q0, [pA], #16
.endm

.macro KERNEL4x4_E
	fmla	v16.4s, v1.4s, v12.s[0]
	fmla	v20.4s, v1.4s, v13.s[0]
	fmla	v24.4s, v1.4s, v14.s[0]
	fmla	v28.4s, v1.4s, v15.s[0]
.endm

.macro KERNEL4x4_SUB
	ldp	s8, s9, [pB], #8
	ldp	s10, s11, [pB], #8

	ldr	q0, [pA], #16

	fmla	v16.4s, v0.4s, v8.s[0]
	fmla	v20.4s, v0.4s, v9.s[0]
	fmla	v24.4s, v0.4s, v10.s[0]
	fmla	v28.4s, v0.4s, v11.s[0]
.endm

.macro SAVE4x4
	fmov	alpha0, alpha

	ldr 	q0, [pCRow0]
	fmla	v0.4s, v16.4s, alphaV0
	str 	q0, [pCRow0]

	add	pCRow0, pCRow0, #16

	ldr 	q1, [pCRow1]
	fmla	v1.4s, v20.4s, alphaV0
	str 	q1, [pCRow1]

	add	pCRow1, pCRow1, #16

	ldr 	q2, [pCRow2]
	fmla	v2.4s, v24.4s, alphaV0
	str 	q2, [pCRow2]

	add	pCRow2, pCRow2, #16

	ldr 	q3, [pCRow3]
	fmla	v3.4s, v28.4s, alphaV0
	str 	q3, [pCRow3]

	add	pCRow3, pCRow3, #16
.endm

/******************************************************************************/

.macro INIT2x4
	fmov		s16, wzr
	fmov		s20, s16
	fmov		s24, s20
	fmov		s28, s16
.endm

.macro KERNEL2x4_SUB
	ldp	s8, s9, [pB], #8
	ldp	s10, s11, [pB], #8

	ldr	d0, [pA], #8

	fmla	v16.2s, v0.2s, v8.s[0]
	fmla	v20.2s, v0.2s, v9.s[0]
	fmla	v24.2s, v0.2s, v10.s[0]
	fmla	v28.2s, v0.2s, v11.s[0]
.endm

.macro SAVE2x4
	fmov	alpha0, alpha

	ldr	d0, [pCRow0]
	fmla	v0.2s, v16.2s, alphaV0
	str	d0, [pCRow0]

	add	pCRow0, pCRow0, #8

	ldr	d1, [pCRow1]
	fmla	v1.2s, v20.2s, alphaV0
	str	d1, [pCRow1]

	add	pCRow1, pCRow1, #8

	ldr	d0, [pCRow2]
	fmla	v0.2s, v24.2s, alphaV0
	str	d0, [pCRow2]

	add	pCRow2, pCRow2, #8

	ldr	d1, [pCRow3]
	fmla	v1.2s, v28.2s, alphaV0
	str	d1, [pCRow3]

	add	pCRow3, pCRow3, #8
.endm

/******************************************************************************/

.macro INIT1x4
	fmov		s16, wzr
	fmov		s20, s16
.endm

.macro KERNEL1x4_SUB
	ldr	s0, [pA]
	add	pA, pA, #4

	ld1	{v8.2s, v9.2s}, [pB]
	add	pB, pB, #16

	fmla	v16.2s, v8.2s, v0.s[0]
	fmla	v20.2s, v9.2s, v0.s[0]
.endm

.macro SAVE1x4
	fmov	alpha0, alpha

	ld1	{v8.s}[0], [pCRow0]
	ld1	{v8.s}[1], [pCRow1]
	fmla	v8.2s, v16.2s, alphaV0
	st1	{v8.s}[0], [pCRow0]
	st1	{v8.s}[1], [pCRow1]

	add	pCRow0, pCRow0, #4
	add	pCRow1, pCRow1, #4

	ld1	{v12.s}[0], [pCRow2]
	ld1	{v12.s}[1], [pCRow3]
	fmla	v12.2s, v20.2s, alphaV0
	st1	{v12.s}[0], [pCRow2]
	st1	{v12.s}[1], [pCRow3]

	add	pCRow2, pCRow2, #4
	add	pCRow3, pCRow3, #4
.endm

/******************************************************************************/

.macro INIT16x2
	fmov	s16, wzr
	fmov	s17, wzr
	fmov	s18, wzr
	fmov	s19, s16
	fmov	s20, wzr
	fmov	s21, s16
	fmov	s22, wzr
	fmov	s23, s16
.endm

.macro KERNEL16x2_SUB
	ld1	{v8.2s}, [pB]
	add	pB, pB, #8
	ld1	{v0.4s}, [pA]
	add	pA, pA, #16
	ld1	{v1.4s}, [pA]
	add	pA, pA, #16
	ld1	{v2.4s}, [pA]
	add	pA, pA, #16
	ld1	{v3.4s}, [pA]
	add	pA, pA, #16

	fmla	v16.4s, v0.4s, v8.s[0]
	fmla	v17.4s, v1.4s, v8.s[0]
	fmla	v18.4s, v2.4s, v8.s[0]
	fmla	v19.4s, v3.4s, v8.s[0]

	fmla	v20.4s, v0.4s, v8.s[1]
	fmla	v21.4s, v1.4s, v8.s[1]
	fmla	v22.4s, v2.4s, v8.s[1]
	fmla	v23.4s, v3.4s, v8.s[1]
.endm

.macro SAVE16x2
	fmov	alpha0, alpha

	add	pCRow1, pCRow0, LDC

	ld1	{v0.4s, v1.4s, v2.4s, v3.4s}, [pCRow0]
	fmla	v0.4s, v16.4s, alphaV0
	fmla	v1.4s, v17.4s, alphaV0
	fmla	v2.4s, v18.4s, alphaV0
	fmla	v3.4s, v19.4s, alphaV0
	st1 	{v0.4s, v1.4s, v2.4s, v3.4s}, [pCRow0]

	ld1	{v4.4s, v5.4s, v6.4s, v7.4s}, [pCRow1]
	fmla	v4.4s, v20.4s, alphaV0
	fmla	v5.4s, v21.4s, alphaV0
	fmla	v6.4s, v22.4s, alphaV0
	fmla	v7.4s, v23.4s, alphaV0
	st1 	{v4.4s, v5.4s, v6.4s, v7.4s}, [pCRow1]

	add	pCRow0, pCRow0, #64
.endm

/******************************************************************************/

.macro INIT8x2
	fmov	s16, wzr
	fmov	s17, s16
	fmov	s20, s17
	fmov	s21, s16
.endm

.macro KERNEL8x2_SUB
	ld1	{v8.2s}, [pB]
	add	pB, pB, #8
	ld1	{v0.4s}, [pA]
	add	pA, pA, #16
	ld1	{v1.4s}, [pA]
	add	pA, pA, #16

	fmla	v16.4s, v0.4s, v8.s[0]
	fmla	v17.4s, v1.4s, v8.s[0]

	fmla	v20.4s, v0.4s, v8.s[1]
	fmla	v21.4s, v1.4s, v8.s[1]
.endm

.macro SAVE8x2
	fmov	alpha0, alpha

	add	pCRow1, pCRow0, LDC

	ld1	{v0.4s, v1.4s}, [pCRow0]
	fmla	v0.4s, v16.4s, alphaV0
	fmla	v1.4s, v17.4s, alphaV0
	st1 	{v0.4s, v1.4s}, [pCRow0]

	add	pCRow2, pCRow1, LDC

	ld1	{v4.4s, v5.4s}, [pCRow1]
	fmla	v4.4s, v20.4s, alphaV0
	fmla	v5.4s, v21.4s, alphaV0
	st1 	{v4.4s, v5.4s}, [pCRow1]

	add	pCRow0, pCRow0, #32
.endm

/******************************************************************************/

.macro INIT4x2
	fmov	s16, wzr
	fmov	s17, s16
	fmov	s20, s17
	fmov	s21, s16
.endm

.macro KERNEL4x2_SUB
	ld1	{v8.2s}, [pB]
	add	pB, pB, #8
	ld1	{v0.2s, v1.2s}, [pA]
	add	pA, pA, #16

	fmla	v16.2s, v0.2s, v8.s[0]
	fmla	v17.2s, v1.2s, v8.s[0]
	fmla	v20.2s, v0.2s, v8.s[1]
	fmla	v21.2s, v1.2s, v8.s[1]
.endm

.macro SAVE4x2
	fmov	alpha0, alpha

	ld1	{v8.2s, v9.2s}, [pCRow0]
	fmla	v8.2s, v16.2s, alphaV0
	fmla	v9.2s, v17.2s, alphaV0
	st1	{v8.2s, v9.2s}, [pCRow0]

	add	pCRow1, pCRow0, LDC
	ld1	{v12.2s, v13.2s}, [pCRow1]
	fmla	v12.2s, v20.2s, alphaV0
	fmla	v13.2s, v21.2s, alphaV0
	st1	{v12.2s, v13.2s}, [pCRow1]

	add	pCRow0, pCRow0, #16
.endm

/******************************************************************************/

.macro INIT2x2
	fmov		s16, wzr
	fmov		s20, s16
.endm

.macro KERNEL2x2_SUB
	ld1	{v8.2s}, [pB]
	add	pB, pB, #8

	ld1	{v0.2s}, [pA]
	add	pA, pA, #8

	fmla	v16.2s, v0.2s, v8.s[0]
	fmla	v20.2s, v0.2s, v8.s[1]
.endm

.macro SAVE2x2
	fmov	alpha0, alpha

	ld1	{v8.2s}, [pCRow0]
	fmla	v8.2s, v16.2s, alphaV0
	st1	{v8.2s}, [pCRow0]

	add	pCRow1 , pCRow0, LDC
	ld1	{v12.2s}, [pCRow1]
	fmla	v12.2s, v20.2s, alphaV0
	st1	{v12.2s}, [pCRow1]

	add	pCRow0, pCRow0, #8
.endm

/******************************************************************************/

.macro INIT1x2
	fmov		s16, wzr
.endm

.macro KERNEL1x2_SUB
	ld1	{v8.2s} , [pB]
	add	pB , pB, #8

	ldr	s0 , [pA]
	add	pA, pA, #4

	fmla	v16.2s, v8.2s, v0.s[0]
.endm

.macro SAVE1x2
	fmov	alpha0, alpha

	add	pCRow1 , pCRow0, LDC
	ld1	{v8.s}[0], [pCRow0]
	ld1	{v8.s}[1], [pCRow1]
	fmla	v8.2s, v16.2s, alphaV0
	st1	{v8.s}[0], [pCRow0]
	st1	{v8.s}[1], [pCRow1]

	add	pCRow0, pCRow0, #4
.endm

/******************************************************************************/

.macro INIT16x1
	fmov	s16, wzr
	fmov	s17, wzr
	fmov	s18, wzr
	fmov	s19, s16
.endm

.macro KERNEL16x1_SUB
	ldr	s8, [pB]
	add	pB , pB, #4

	ld1	{v0.4s}, [pA]
	add	pA, pA, #16
	ld1	{v1.4s}, [pA]
	add	pA, pA, #16
	ld1	{v2.4s}, [pA]
	add	pA, pA, #16
	ld1	{v3.4s}, [pA]
	add	pA, pA, #16

	fmla	v16.4s, v0.4s, v8.s[0]
	fmla	v17.4s, v1.4s, v8.s[0]
	fmla	v18.4s, v2.4s, v8.s[0]
	fmla	v19.4s, v3.4s, v8.s[0]
.endm

.macro SAVE16x1
	fmov	alpha0, alpha

	ld1	{v0.4s, v1.4s, v2.4s, v3.4s}, [pCRow0]
	fmla	v0.4s, v16.4s, alphaV0
	fmla	v1.4s, v17.4s, alphaV0
	fmla	v2.4s, v18.4s, alphaV0
	fmla	v3.4s, v19.4s, alphaV0
	st1 	{v0.4s, v1.4s, v2.4s, v3.4s}, [pCRow0]

	add	pCRow0, pCRow0, #64
.endm

/******************************************************************************/

.macro INIT8x1
	fmov	s16, wzr
	fmov	s17, wzr
.endm

.macro KERNEL8x1_SUB
	ldr	s8, [pB]
	add	pB , pB, #4

	ld1	{v0.4s}, [pA]
	add	pA, pA, #16
	ld1	{v1.4s}, [pA]
	add	pA, pA, #16

	fmla	v16.4s, v0.4s, v8.s[0]
	fmla	v17.4s, v1.4s, v8.s[0]
.endm

.macro SAVE8x1
	fmov	alpha0, alpha

	ld1	{v0.4s, v1.4s}, [pCRow0]
	fmla	v0.4s, v16.4s, alphaV0
	fmla	v1.4s, v17.4s, alphaV0
	st1 	{v0.4s, v1.4s}, [pCRow0]

	add	pCRow0, pCRow0, #32
.endm

/******************************************************************************/

.macro INIT4x1
	fmov	s16, wzr
	fmov	s17, s16
.endm

.macro KERNEL4x1_SUB
	ldr	s8, [pB]
	add	pB , pB, #4

	ld1	{v0.2s, v1.2s}, [pA]
	add	pA , pA, #16

	fmla	v16.2s, v0.2s, v8.s[0]
	fmla	v17.2s, v1.2s, v8.s[0]
.endm

.macro SAVE4x1
	fmov	alpha0, alpha

	ld1	{v8.2s, v9.2s}, [pCRow0]
	fmla	v8.2s, v16.2s, alphaV0
	fmla	v9.2s, v17.2s, alphaV0
	st1	{v8.2s, v9.2s}, [pCRow0]

	add	pCRow0, pCRow0, #16
.endm

/******************************************************************************/

.macro INIT2x1
	fmov		s16, wzr
.endm

.macro KERNEL2x1_SUB
	ldr	s8, [pB]
	add	pB , pB, #4

	ld1	{v0.2s}, [pA]
	add	pA , pA, #8

	fmla	v16.2s, v0.2s, v8.s[0]
.endm

.macro SAVE2x1
	fmov	alpha0, alpha

	ld1	{v8.2s}, [pCRow0]
	fmla	v8.2s, v16.2s, alphaV0
	st1	{v8.2s}, [pCRow0]

	add	pCRow0, pCRow0, #8
.endm

/******************************************************************************/

.macro INIT1x1
	fmov	s16, wzr
.endm

.macro KERNEL1x1_SUB
	ldr	s8, [pB]
	add	pB , pB, #4

	ldr	s0, [pA]
	add	pA , pA, #4

	fmadd 	s16, s0, s8, s16  
.endm

.macro SAVE1x1
	fmov	alpha0, alpha

	ldr 	s8, [pCRow0]
	fmla	s8, s16, alphaV0
	str 	s8, [pCRow0]

	add	pCRow0, pCRow0, #4
.endm

.macro KERNEL16x4_M1_M2_x1
	KERNEL16x4_M1
	KERNEL16x4_M2
.endm

.macro KERNEL16x4_M1_M2_x2
	KERNEL16x4_M1_M2_x1
	KERNEL16x4_M1_M2_x1
.endm

.macro KERNEL16x4_M1_M2_x4
	KERNEL16x4_M1_M2_x2
	KERNEL16x4_M1_M2_x2
.endm

.macro KERNEL16x4_M1_M2_x8
	KERNEL16x4_M1_M2_x4
	KERNEL16x4_M1_M2_x4
.endm

.macro KERNEL16x4_M1_M2_x16
	KERNEL16x4_M1_M2_x8
	KERNEL16x4_M1_M2_x8
.endm

.macro KERNEL16x4_M1_M2_x32
	KERNEL16x4_M1_M2_x16
	KERNEL16x4_M1_M2_x16
.endm

.macro KERNEL16x4_M1_M2_x64
	KERNEL16x4_M1_M2_x32
	KERNEL16x4_M1_M2_x32
.endm

/*******************************************************************************
* End of macro definitions
*******************************************************************************/

	PROLOGUE

.Lsgemm_kernel_begin:

	.align 5
	add	sp, sp, #-(11 * 16)
	stp	d8, d9, [sp, #(0 * 16)]
	stp	d10, d11, [sp, #(1 * 16)]
	stp	d12, d13, [sp, #(2 * 16)]
	stp	d14, d15, [sp, #(3 * 16)]
	stp	d16, d17, [sp, #(4 * 16)]
	stp	x18, x19, [sp, #(5 * 16)]
	stp	x20, x21, [sp, #(6 * 16)]
	stp	x22, x23, [sp, #(7 * 16)]
	stp	x24, x25, [sp, #(8 * 16)]
	stp	x26, x27, [sp, #(9 * 16)]
	str	x28, [sp, #(10 * 16)]

	prfm	PLDL1KEEP, [origPB]
	prfm	PLDL1KEEP, [origPA]

	fmov	alpha, s0

	lsl	LDC, LDC, #2			// ldc = ldc * 4

	mov	pB, origPB

	mov	counterJ, origN
	asr 	counterJ, counterJ, #2		// J = J / 4
	cmp 	counterJ, #0
	ble	.Lsgemm_kernel_L2_BEGIN

/******************************************************************************/

.Lsgemm_kernel_L4_BEGIN:
	mov	pCRow0, pC
	add	pCRow1, pCRow0, LDC
	add	pCRow2, pCRow1, LDC
	add	pCRow3, pCRow2, LDC

	add	pC, pCRow3, LDC

	mov	pA, origPA			// pA = start of A array

.Lsgemm_kernel_L4_M16_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #4		// counterI = counterI / 16
	cmp 	counterI, #0
	ble	.Lsgemm_kernel_L4_M8_BEGIN

	.align 5
.Lsgemm_kernel_L4_M16_20:

	mov	pB, origPB

	asr 	counterL , origK, #4        // L = K / 16
	cmp	counterL , #2
	blt	.Lsgemm_kernel_L4_M16_32

	KERNEL16x4_I
	KERNEL16x4_M2
	KERNEL16x4_M1_M2_x4
	KERNEL16x4_M1_M2_x2
	KERNEL16x4_M1_M2_x1

	subs	counterL, counterL, #2
	ble	.Lsgemm_kernel_L4_M16_22a

	.align 5
.Lsgemm_kernel_L4_M16_22:

	KERNEL16x4_M1_M2_x8

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L4_M16_22

	.align 5
.Lsgemm_kernel_L4_M16_22a:

	KERNEL16x4_M1_M2_x4
	KERNEL16x4_M1_M2_x2
	KERNEL16x4_M1_M2_x1
	KERNEL16x4_M1
	KERNEL16x4_E

	b	 .Lsgemm_kernel_L4_M16_44

	.align 5
.Lsgemm_kernel_L4_M16_32:

	tst	counterL, #1
	ble	.Lsgemm_kernel_L4_M16_40

	KERNEL16x4_I
	KERNEL16x4_M2
	KERNEL16x4_M1_M2_x4
	KERNEL16x4_M1_M2_x2
	KERNEL16x4_M1
	KERNEL16x4_E

	b	.Lsgemm_kernel_L4_M16_44

.Lsgemm_kernel_L4_M16_40:

	INIT16x4

.Lsgemm_kernel_L4_M16_44:

	ands	counterL , origK, #15
	ble	.Lsgemm_kernel_L4_M16_100

	.align 5
.Lsgemm_kernel_L4_M16_46:

	KERNEL16x4_SUB
	subs	counterL, counterL, #1
	bne	.Lsgemm_kernel_L4_M16_46

.Lsgemm_kernel_L4_M16_100:
	prfm	PLDL1KEEP, [pA]
	prfm	PLDL1KEEP, [pA, #64]
	prfm	PLDL1KEEP, [origPB]

	SAVE16x4

.Lsgemm_kernel_L4_M16_END:
	subs	counterI, counterI, #1
	bne	.Lsgemm_kernel_L4_M16_20

//------------------------------------------------------------------------------

.Lsgemm_kernel_L4_M8_BEGIN:

	mov	counterI, origM
	tst	counterI , #15
	ble	.Lsgemm_kernel_L4_END

	tst	counterI, #8
	ble	.Lsgemm_kernel_L4_M4_BEGIN

.Lsgemm_kernel_L4_M8_20:

	mov	pB, origPB

	asr 	counterL , origK, #1		// L = K / 2
	cmp	counterL , #2			// is there at least 4 to do?
	blt	.Lsgemm_kernel_L4_M8_32

	KERNEL8x4_I				// do one in the K
	KERNEL8x4_M2				// do another in the K

	subs	counterL, counterL, #2
	ble	.Lsgemm_kernel_L4_M8_22a
	.align 5

.Lsgemm_kernel_L4_M8_22:

	KERNEL8x4_M1
	KERNEL8x4_M2

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L4_M8_22

.Lsgemm_kernel_L4_M8_22a:

	KERNEL8x4_M1
	KERNEL8x4_E

	b	 .Lsgemm_kernel_L4_M8_44

.Lsgemm_kernel_L4_M8_32:

	tst	counterL, #1
	ble	.Lsgemm_kernel_L4_M8_40

	KERNEL8x4_I
	KERNEL8x4_E

	b	.Lsgemm_kernel_L4_M8_44

.Lsgemm_kernel_L4_M8_40:

	INIT8x4

.Lsgemm_kernel_L4_M8_44:

	ands	counterL , origK, #1
	ble	.Lsgemm_kernel_L4_M8_100

.Lsgemm_kernel_L4_M8_46:

	KERNEL8x4_SUB

.Lsgemm_kernel_L4_M8_100:

	SAVE8x4

.Lsgemm_kernel_L4_M8_END:

//------------------------------------------------------------------------------

.Lsgemm_kernel_L4_M4_BEGIN:

	mov	counterI, origM
	tst	counterI , #7
	ble	.Lsgemm_kernel_L4_END

	tst	counterI, #4
	ble	.Lsgemm_kernel_L4_M2_BEGIN

.Lsgemm_kernel_L4_M4_20:

	mov	pB, origPB

	asr 	counterL , origK, #1		// L = K / 2
	cmp	counterL , #2			// is there at least 4 to do?
	blt	.Lsgemm_kernel_L4_M4_32

	KERNEL4x4_I				// do one in the K
	KERNEL4x4_M2				// do another in the K

	subs	counterL, counterL, #2
	ble	.Lsgemm_kernel_L4_M4_22a
	.align 5

.Lsgemm_kernel_L4_M4_22:

	KERNEL4x4_M1
	KERNEL4x4_M2

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L4_M4_22

.Lsgemm_kernel_L4_M4_22a:

	KERNEL4x4_M1
	KERNEL4x4_E

	b	 .Lsgemm_kernel_L4_M4_44

.Lsgemm_kernel_L4_M4_32:

	tst	counterL, #1
	ble	.Lsgemm_kernel_L4_M4_40

	KERNEL4x4_I
	KERNEL4x4_E

	b	.Lsgemm_kernel_L4_M4_44

.Lsgemm_kernel_L4_M4_40:

	INIT4x4

.Lsgemm_kernel_L4_M4_44:

	ands	counterL , origK, #1
	ble	.Lsgemm_kernel_L4_M4_100

.Lsgemm_kernel_L4_M4_46:

	KERNEL4x4_SUB

.Lsgemm_kernel_L4_M4_100:

	SAVE4x4

.Lsgemm_kernel_L4_M4_END:

//------------------------------------------------------------------------------

.Lsgemm_kernel_L4_M2_BEGIN:

	mov	counterI, origM
	tst	counterI , #3
	ble	.Lsgemm_kernel_L4_END

	tst	counterI, #2			// counterI = counterI / 2
	ble	.Lsgemm_kernel_L4_M1_BEGIN

.Lsgemm_kernel_L4_M2_20:

	INIT2x4

	mov	pB, origPB

	asr 	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Lsgemm_kernel_L4_M2_40

.Lsgemm_kernel_L4_M2_22:

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L4_M2_22


.Lsgemm_kernel_L4_M2_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lsgemm_kernel_L4_M2_100

.Lsgemm_kernel_L4_M2_42:

	KERNEL2x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L4_M2_42

.Lsgemm_kernel_L4_M2_100:

	SAVE2x4

.Lsgemm_kernel_L4_M2_END:


.Lsgemm_kernel_L4_M1_BEGIN:

	tst	counterI, #1			// counterI = counterI % 2
	ble	.Lsgemm_kernel_L4_END

.Lsgemm_kernel_L4_M1_20:

	INIT1x4

	mov	pB, origPB

	asr 	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Lsgemm_kernel_L4_M1_40

.Lsgemm_kernel_L4_M1_22:
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L4_M1_22


.Lsgemm_kernel_L4_M1_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lsgemm_kernel_L4_M1_100

.Lsgemm_kernel_L4_M1_42:

	KERNEL1x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L4_M1_42

.Lsgemm_kernel_L4_M1_100:

	SAVE1x4

.Lsgemm_kernel_L4_END:
	add	origPB, origPB, origK, lsl #4	// B = B + K * 4 * 4

	subs	counterJ, counterJ , #1		// j--
	bgt	.Lsgemm_kernel_L4_BEGIN


/******************************************************************************/

.Lsgemm_kernel_L2_BEGIN:   // less than 2 left in N direction

	mov	counterJ , origN
	tst	counterJ , #3
	ble	.Lsgemm_kernel_L999

	tst	counterJ , #2
	ble	.Lsgemm_kernel_L1_BEGIN

	mov	pCRow0, pC			// pCRow0 = pC

	add	pC,pC,LDC, lsl #1

	mov	pA, origPA			// pA = A

.Lsgemm_kernel_L2_M16_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #4		// counterI = counterI / 16
	cmp	counterI,#0
	ble	.Lsgemm_kernel_L2_M8_BEGIN

.Lsgemm_kernel_L2_M16_20:

	INIT16x2

	mov	pB, origPB

	asr	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL,#0
	ble	.Lsgemm_kernel_L2_M16_40
	.align 5

.Lsgemm_kernel_L2_M16_22:
	KERNEL16x2_SUB
	KERNEL16x2_SUB
	KERNEL16x2_SUB
	KERNEL16x2_SUB

	KERNEL16x2_SUB
	KERNEL16x2_SUB
	KERNEL16x2_SUB
	KERNEL16x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L2_M16_22


.Lsgemm_kernel_L2_M16_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lsgemm_kernel_L2_M16_100

.Lsgemm_kernel_L2_M16_42:

	KERNEL16x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L2_M16_42

.Lsgemm_kernel_L2_M16_100:

	SAVE16x2

.Lsgemm_kernel_L2_M16_END:

	subs	counterI, counterI, #1
	bgt	.Lsgemm_kernel_L2_M16_20

//------------------------------------------------------------------------------

.Lsgemm_kernel_L2_M8_BEGIN:
	mov	counterI, origM
	tst	counterI , #15
	ble	.Lsgemm_kernel_L2_END

	tst	counterI, #8
	ble	.Lsgemm_kernel_L2_M4_BEGIN

.Lsgemm_kernel_L2_M8_20:

	INIT8x2

	mov	pB, origPB

	asr	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL,#0
	ble	.Lsgemm_kernel_L2_M8_40
	.align 5

.Lsgemm_kernel_L2_M8_22:
	KERNEL8x2_SUB
	KERNEL8x2_SUB
	KERNEL8x2_SUB
	KERNEL8x2_SUB

	KERNEL8x2_SUB
	KERNEL8x2_SUB
	KERNEL8x2_SUB
	KERNEL8x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L2_M8_22


.Lsgemm_kernel_L2_M8_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lsgemm_kernel_L2_M8_100

.Lsgemm_kernel_L2_M8_42:

	KERNEL8x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L2_M8_42

.Lsgemm_kernel_L2_M8_100:

	SAVE8x2

.Lsgemm_kernel_L2_M8_END:

//------------------------------------------------------------------------------

.Lsgemm_kernel_L2_M4_BEGIN:
	mov	counterI, origM
	tst	counterI , #7
	ble	.Lsgemm_kernel_L2_END

	tst	counterI, #4
	ble	.Lsgemm_kernel_L2_M2_BEGIN

.Lsgemm_kernel_L2_M4_20:

	INIT4x2

	mov	pB, origPB

	asr	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL,#0
	ble	.Lsgemm_kernel_L2_M4_40
	.align 5

.Lsgemm_kernel_L2_M4_22:
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L2_M4_22


.Lsgemm_kernel_L2_M4_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lsgemm_kernel_L2_M4_100

.Lsgemm_kernel_L2_M4_42:

	KERNEL4x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L2_M4_42

.Lsgemm_kernel_L2_M4_100:

	SAVE4x2

.Lsgemm_kernel_L2_M4_END:

//------------------------------------------------------------------------------


.Lsgemm_kernel_L2_M2_BEGIN:

	mov	counterI, origM
	tst	counterI , #3
	ble	.Lsgemm_kernel_L2_END

	tst	counterI, #2			// counterI = counterI / 2
	ble	.Lsgemm_kernel_L2_M1_BEGIN

.Lsgemm_kernel_L2_M2_20:

	INIT2x2

	mov	pB, origPB

	asr	counterL , origK, #3		// counterL = counterL / 8
        cmp	counterL,#0
	ble	.Lsgemm_kernel_L2_M2_40

.Lsgemm_kernel_L2_M2_22:

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L2_M2_22


.Lsgemm_kernel_L2_M2_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lsgemm_kernel_L2_M2_100

.Lsgemm_kernel_L2_M2_42:

	KERNEL2x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L2_M2_42

.Lsgemm_kernel_L2_M2_100:

	SAVE2x2

.Lsgemm_kernel_L2_M2_END:


.Lsgemm_kernel_L2_M1_BEGIN:

	tst	counterI, #1			// counterI = counterI % 2
	ble	.Lsgemm_kernel_L2_END

.Lsgemm_kernel_L2_M1_20:

	INIT1x2

	mov	pB, origPB

	asr 	counterL , origK, #3		// counterL = counterL / 8
        cmp     counterL, #0
	ble	.Lsgemm_kernel_L2_M1_40

.Lsgemm_kernel_L2_M1_22:
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L2_M1_22


.Lsgemm_kernel_L2_M1_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lsgemm_kernel_L2_M1_100

.Lsgemm_kernel_L2_M1_42:

	KERNEL1x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L2_M1_42

.Lsgemm_kernel_L2_M1_100:

	SAVE1x2

.Lsgemm_kernel_L2_END:

	add	origPB, origPB, origK, lsl #3	// B = B + K * 2 * 4

/******************************************************************************/

.Lsgemm_kernel_L1_BEGIN:

	mov	counterJ , origN
	tst	counterJ , #1
	ble	.Lsgemm_kernel_L999 // done


	mov	pCRow0, pC			// pCRow0 = C
	add	pC , pC , LDC			// Update pC to point to next

	mov	pA, origPA			// pA = A

.Lsgemm_kernel_L1_M16_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #4		// counterI = counterI / 16
	cmp	counterI, #0
	ble	.Lsgemm_kernel_L1_M8_BEGIN

.Lsgemm_kernel_L1_M16_20:

	INIT16x1

	mov	pB, origPB

	asr	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Lsgemm_kernel_L1_M16_40
	.align 5

.Lsgemm_kernel_L1_M16_22:
	KERNEL16x1_SUB
	KERNEL16x1_SUB
	KERNEL16x1_SUB
	KERNEL16x1_SUB

	KERNEL16x1_SUB
	KERNEL16x1_SUB
	KERNEL16x1_SUB
	KERNEL16x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L1_M16_22


.Lsgemm_kernel_L1_M16_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lsgemm_kernel_L1_M16_100

.Lsgemm_kernel_L1_M16_42:

	KERNEL16x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L1_M16_42

.Lsgemm_kernel_L1_M16_100:

	SAVE16x1

.Lsgemm_kernel_L1_M16_END:

	subs	counterI, counterI, #1
	bgt	.Lsgemm_kernel_L1_M16_20

//------------------------------------------------------------------------------

.Lsgemm_kernel_L1_M8_BEGIN:

	mov	counterI, origM
	tst	counterI , #15
	ble	.Lsgemm_kernel_L1_END

	tst	counterI, #8
	ble	.Lsgemm_kernel_L1_M4_BEGIN

.Lsgemm_kernel_L1_M8_20:

	INIT8x1

	mov	pB, origPB

	asr	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Lsgemm_kernel_L1_M8_40
	.align 5

.Lsgemm_kernel_L1_M8_22:
	KERNEL8x1_SUB
	KERNEL8x1_SUB
	KERNEL8x1_SUB
	KERNEL8x1_SUB

	KERNEL8x1_SUB
	KERNEL8x1_SUB
	KERNEL8x1_SUB
	KERNEL8x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L1_M8_22


.Lsgemm_kernel_L1_M8_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lsgemm_kernel_L1_M8_100

.Lsgemm_kernel_L1_M8_42:

	KERNEL8x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L1_M8_42

.Lsgemm_kernel_L1_M8_100:

	SAVE8x1

.Lsgemm_kernel_L1_M8_END:

//------------------------------------------------------------------------------

.Lsgemm_kernel_L1_M4_BEGIN:
	mov	counterI, origM
	tst	counterI , #7
	ble	.Lsgemm_kernel_L1_END

	tst	counterI, #4
	ble	.Lsgemm_kernel_L1_M2_BEGIN

.Lsgemm_kernel_L1_M4_20:

	INIT4x1

	mov	pB, origPB

	asr	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Lsgemm_kernel_L1_M4_40
	.align 5

.Lsgemm_kernel_L1_M4_22:
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L1_M4_22


.Lsgemm_kernel_L1_M4_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lsgemm_kernel_L1_M4_100

.Lsgemm_kernel_L1_M4_42:

	KERNEL4x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L1_M4_42

.Lsgemm_kernel_L1_M4_100:

	SAVE4x1

.Lsgemm_kernel_L1_M4_END:

//------------------------------------------------------------------------------

.Lsgemm_kernel_L1_M2_BEGIN:

	mov	counterI, origM
	tst	counterI , #3
	ble	.Lsgemm_kernel_L1_END

	tst	counterI, #2			// counterI = counterI / 2
	ble	.Lsgemm_kernel_L1_M1_BEGIN

.Lsgemm_kernel_L1_M2_20:

	INIT2x1

	mov	pB, origPB

	asr 	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Lsgemm_kernel_L1_M2_40

.Lsgemm_kernel_L1_M2_22:

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L1_M2_22


.Lsgemm_kernel_L1_M2_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lsgemm_kernel_L1_M2_100

.Lsgemm_kernel_L1_M2_42:

	KERNEL2x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L1_M2_42

.Lsgemm_kernel_L1_M2_100:

	SAVE2x1

.Lsgemm_kernel_L1_M2_END:


.Lsgemm_kernel_L1_M1_BEGIN:

	tst	counterI, #1			// counterI = counterI % 2
	ble	.Lsgemm_kernel_L1_END

.Lsgemm_kernel_L1_M1_20:

	INIT1x1

	mov	pB, origPB

	asr 	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Lsgemm_kernel_L1_M1_40

.Lsgemm_kernel_L1_M1_22:
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L1_M1_22


.Lsgemm_kernel_L1_M1_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lsgemm_kernel_L1_M1_100

.Lsgemm_kernel_L1_M1_42:

	KERNEL1x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L1_M1_42

.Lsgemm_kernel_L1_M1_100:

	SAVE1x1

.Lsgemm_kernel_L1_END:

.Lsgemm_kernel_L999:
	mov	x0, #0				// set return value
	ldp	d8, d9, [sp, #(0 * 16)]
	ldp	d10, d11, [sp, #(1 * 16)]
	ldp	d12, d13, [sp, #(2 * 16)]
	ldp	d14, d15, [sp, #(3 * 16)]
	ldp	d16, d17, [sp, #(4 * 16)]
	ldp	x18, x19, [sp, #(5 * 16)]
	ldp	x20, x21, [sp, #(6 * 16)]
	ldp	x22, x23, [sp, #(7 * 16)]
	ldp	x24, x25, [sp, #(8 * 16)]
	ldp	x26, x27, [sp, #(9 * 16)]
	ldr	x28, [sp, #(10 * 16)]
	add	sp, sp, #(11*16)
	ret

	EPILOGUE

