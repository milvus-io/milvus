/*******************************************************************************
Copyright (c) 2015, The OpenBLAS Project
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

/*                   X0          X1          X2          s0        X3        x4       x5           x6 */
/*int CNAME(BLASLONG bm,BLASLONG bn,BLASLONG bk,FLOAT alpha0,FLOAT* ba,FLOAT* bb,FLOAT* C,BLASLONG ldc */

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
#define pA_0		x15
#define pA_1		x16
#define pA_2		x17
#define pA_3		x18


#define alpha0		s10
#define alphaV0		v10.s[0]
#define alpha1		s11
#define alphaV1		v11.s[0]
#define alpha2		s14
#define alphaV2		v14.s[0]
#define alpha3		s15
#define alphaV3		v15.s[0]

// 00 origM
// 01 origN
// 02 origK
// 03 origPA
// 04 origPB
// 05 pC
// 06 origLDC -> LDC
// 07 offset -> temp
// 08 counterL
// 09 counterI
// 10 counterJ
// 11 pB
// 12 pCRow0
// 13 pCRow1
// 14 pCRow2
// 15 pA_0
// 16 pA_1
// 17 pA_2
// 18 must save pA_3
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

/***************************** FOR 16x4 ***************************************/
//v00 ALPHA -> pA00_0, pA01_0, pA02_0, pA03_0
//v01 pA10_0, pA11_0, pA12_0, pA13_0
//v02 pA00_1, pA01_1, pA02_1, pA03_1
//v03 pA10_1, pA11_1, pA12_1, pA13_1
//v04 pA00_2, pA01_2, pA02_2, pA03_2
//v05 pA10_2, pA11_2, pA12_2, pA13_2
//v06 pA00_3, pA01_3, pA02_3, pA03_3
//v07 pA10_3, pA11_3, pA12_3, pA13_3
//v08 must save pB00, pB01, pB02, pB03
//v09 must save
//v10 must save ALPHA0
//v11 must save ALPHA1
//v12 must save pB10, pB11, pB12, pB13
//v13 must save
//v14 must save ALPHA2
//v15 must save ALPHA3
//v16 must save C00_0, C01_0, C02_0, C03_0
//v17 must save C10_0, C11_0, C12_0, C13_0
//v18 C20_0, C21_0, C22_0, C23_0
//v19 C30_0, C31_0, C32_0, C33_0
//v20 C00_1, C01_1, C02_1, C03_1
//v21 C10_1, C11_1, C12_1, C13_1
//v22 C20_1, C21_1, C22_1, C23_1
//v23 C30_1, C31_1, C32_1, C33_1
//v24 C00_2, C01_2, C02_2, C03_2
//v25 C10_2, C11_2, C12_2, C13_2
//v26 C20_2, C21_2, C22_2, C23_2
//v27 C30_2, C31_2, C32_2, C33_2
//v28 C00_3, C01_3, C02_3, C03_3
//v29 C10_3, C11_3, C12_3, C13_3
//v30 C20_3, C21_3, C22_3, C23_3
//v31 C30_3, C31_3, C32_3, C33_3

/***************************** EXCEPT FOR 16x4 ********************************/
//v00 ALPHA -> pA00, pA01
//v01 pA02, pA03
//v02 ppA00, ppA01
//v03 ppA02, ppA03
//v04 pA10, pA11
//v05 pA12, pA13
//v06 ppA10, ppA11
//v07 ppA12, ppA13
//v08 must save pB00, pB01
//v09 must save pB02, pB03
//v10 must save ALPHA0
//v11 must save ALPHA1
//v12 must save pB10, pB11
//v13 must save pB12, pB13
//v14 must save ALPHA2
//v15 must save ALPHA3
//v16 must save C00, C01
//v17 must save C02, C03
//v18 ppC00, ppC01
//v19 ppC02, ppC03
//v20 C10, C11
//v21 C12, C13
//v22 ppC10, ppC11
//v23 ppC12, ppC13
//v24 C20, C21
//v25 C22, C23
//v26 ppC20, ppC21
//v27 ppC22, ppC23
//v28 C30, C31
//v29 C32, C33
//v30 ppC30, ppC31
//v31 ppC32, ppC33

/*******************************************************************************
* Macro definitions
*******************************************************************************/

.macro INIT16x4
	fmov		s16, wzr
	fmov		s17, s16
	fmov		s18, s17
	fmov		s19, s16
	fmov		s20, s17
	fmov		s21, s16
	fmov		s22, s17
	fmov		s23, s16
	fmov		s24, s17
	fmov		s25, s16
	fmov		s26, s17
	fmov		s27, s16
	fmov		s28, s17
	fmov		s29, s16
	fmov		s30, s17
	fmov		s31, s16
.endm

.macro KERNEL16x4_I
	ld1	{v8.4s}, [pB]
	add	pB, pB, #16

	ld1	{v0.4s}, [pA_0]
	add	pA_0, pA_0, #16

	fmul	v16.4s, v0.4s, v8.s[0]
	fmul	v20.4s, v0.4s, v8.s[1]

	ld1	{v2.4s}, [pA_1]
	add	pA_1, pA_1, #16

	fmul	v24.4s, v0.4s, v8.s[2]
	fmul	v28.4s, v0.4s, v8.s[3]

	ld1	{v4.4s}, [pA_2]
	add	pA_2, pA_2, #16

	fmul	v17.4s, v2.4s, v8.s[0]
	fmul	v21.4s, v2.4s, v8.s[1]

	ld1	{v6.4s}, [pA_3]
	add	pA_3, pA_3, #16

	fmul	v25.4s, v2.4s, v8.s[2]
	fmul	v29.4s, v2.4s, v8.s[3]

	ld1	{v12.4s}, [pB]		// for next round
	add	pB, pB, #16

	fmul	v18.4s, v4.4s, v8.s[0]
	fmul	v19.4s, v6.4s, v8.s[0]

	ld1	{v1.4s}, [pA_0]		// for next round
	add	pA_0, pA_0, #16

	fmul	v22.4s, v4.4s, v8.s[1]
	fmul	v23.4s, v6.4s, v8.s[1]

	ld1	{v3.4s}, [pA_1]		// for next round
	add	pA_1, pA_1, #16

	fmul	v26.4s, v4.4s, v8.s[2]
	fmul	v27.4s, v6.4s, v8.s[2]

	ld1	{v5.4s}, [pA_2]		// for next round
	add	pA_2, pA_2, #16

	fmul	v30.4s, v4.4s, v8.s[3]
	fmul	v31.4s, v6.4s, v8.s[3]

	ld1	{v7.4s}, [pA_3]		// for next round
	add	pA_3, pA_3, #16
.endm

.macro KERNEL16x4_M2
	fmla	v16.4s, v1.4s, v12.s[0]
	fmla	v17.4s, v3.4s, v12.s[0]

	ld1	{v8.4s}, [pB]		// for next round
	add	pB, pB, #16

	fmla	v18.4s, v5.4s, v12.s[0]
	fmla	v19.4s, v7.4s, v12.s[0]

	ld1	{v0.4s}, [pA_0]		// for next round
	add	pA_0, pA_0, #16

	fmla	v20.4s, v1.4s, v12.s[1]
	fmla	v21.4s, v3.4s, v12.s[1]

	ld1	{v2.4s}, [pA_1]		// for next round
	add	pA_1, pA_1, #16

	fmla	v22.4s, v5.4s, v12.s[1]
	fmla	v23.4s, v7.4s, v12.s[1]

	ld1	{v4.4s}, [pA_2]		// for next round
	add	pA_2, pA_2, #16

	fmla	v24.4s, v1.4s, v12.s[2]
	fmla	v25.4s, v3.4s, v12.s[2]

	ld1	{v6.4s}, [pA_3]		// for next round
	add	pA_3, pA_3, #16

	fmla	v26.4s, v5.4s, v12.s[2]
	fmla	v27.4s, v7.4s, v12.s[2]

	prfm	PLDL1KEEP, [pA_2, #512]

	fmla	v28.4s, v1.4s, v12.s[3]
	fmla	v29.4s, v3.4s, v12.s[3]

	prfm	PLDL1KEEP, [pA_3, #512]

	fmla	v30.4s, v5.4s, v12.s[3]
	fmla	v31.4s, v7.4s, v12.s[3]

	prfm	PLDL1KEEP, [pB, #512]
.endm

.macro KERNEL16x4_M1
	fmla	v16.4s, v0.4s, v8.s[0]
	fmla	v17.4s, v2.4s, v8.s[0]

	ld1	{v12.4s}, [pB]		// for next round
	add	pB, pB, #16

	fmla	v18.4s, v4.4s, v8.s[0]
	fmla	v19.4s, v6.4s, v8.s[0]

	ld1	{v1.4s}, [pA_0]		// for next round
	add	pA_0, pA_0, #16

	fmla	v20.4s, v0.4s, v8.s[1]
	fmla	v21.4s, v2.4s, v8.s[1]

	ld1	{v3.4s}, [pA_1]		// for next round
	add	pA_1, pA_1, #16

	fmla	v22.4s, v4.4s, v8.s[1]
	fmla	v23.4s, v6.4s, v8.s[1]

	ld1	{v5.4s}, [pA_2]		// for next round
	add	pA_2, pA_2, #16

	fmla	v24.4s, v0.4s, v8.s[2]
	fmla	v25.4s, v2.4s, v8.s[2]

	ld1	{v7.4s}, [pA_3]		// for next round
	add	pA_3, pA_3, #16

	fmla	v26.4s, v4.4s, v8.s[2]
	fmla	v27.4s, v6.4s, v8.s[2]

	prfm	PLDL1KEEP, [pA_0, #512]

	fmla	v28.4s, v0.4s, v8.s[3]
	fmla	v29.4s, v2.4s, v8.s[3]

	prfm	PLDL1KEEP, [pA_1, #512]

	fmla	v30.4s, v4.4s, v8.s[3]
	fmla	v31.4s, v6.4s, v8.s[3]
.endm

.macro KERNEL16x4_E
	fmla	v16.4s, v1.4s, v12.s[0]
	fmla	v17.4s, v3.4s, v12.s[0]
	fmla	v18.4s, v5.4s, v12.s[0]
	fmla	v19.4s, v7.4s, v12.s[0]
	fmla	v20.4s, v1.4s, v12.s[1]
	fmla	v21.4s, v3.4s, v12.s[1]
	fmla	v22.4s, v5.4s, v12.s[1]
	fmla	v23.4s, v7.4s, v12.s[1]
	fmla	v24.4s, v1.4s, v12.s[2]
	fmla	v25.4s, v3.4s, v12.s[2]
	fmla	v26.4s, v5.4s, v12.s[2]
	fmla	v27.4s, v7.4s, v12.s[2]
	fmla	v28.4s, v1.4s, v12.s[3]
	fmla	v29.4s, v3.4s, v12.s[3]
	fmla	v30.4s, v5.4s, v12.s[3]
	fmla	v31.4s, v7.4s, v12.s[3]
.endm

.macro KERNEL16x4_SUB
	ld1	{v8.4s}, [pB]
	add	pB, pB, #16

	ld1	{v0.4s}, [pA_0]
	add	pA_0, pA_0, #16

	fmla	v16.4s, v0.4s, v8.s[0]
	fmla	v20.4s, v0.4s, v8.s[1]
	fmla	v24.4s, v0.4s, v8.s[2]
	fmla	v28.4s, v0.4s, v8.s[3]

	ld1	{v2.4s}, [pA_1]
	add	pA_1, pA_1, #16

	fmla	v17.4s, v2.4s, v8.s[0]
	fmla	v21.4s, v2.4s, v8.s[1]
	fmla	v25.4s, v2.4s, v8.s[2]
	fmla	v29.4s, v2.4s, v8.s[3]

	ld1	{v4.4s}, [pA_2]
	add	pA_2, pA_2, #16

	fmla	v18.4s, v4.4s, v8.s[0]
	fmla	v22.4s, v4.4s, v8.s[1]
	fmla	v26.4s, v4.4s, v8.s[2]
	fmla	v30.4s, v4.4s, v8.s[3]

	ld1	{v6.4s}, [pA_3]
	add	pA_3, pA_3, #16

	fmla	v19.4s, v6.4s, v8.s[0]
	fmla	v23.4s, v6.4s, v8.s[1]
	fmla	v27.4s, v6.4s, v8.s[2]
	fmla	v31.4s, v6.4s, v8.s[3]
.endm

.macro SAVE16x4
	mov	pCRow1, pCRow0

	ld1	{v0.4s, v1.4s, v2.4s, v3.4s}, [pCRow1]
	fmla	v0.4s, v16.4s, alphaV0
	fmla	v1.4s, v17.4s, alphaV1
	fmla	v2.4s, v18.4s, alphaV2
	fmla	v3.4s, v19.4s, alphaV3
	st1 	{v0.4s, v1.4s, v2.4s, v3.4s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld1	{v4.4s, v5.4s, v6.4s, v7.4s}, [pCRow1]
	fmla	v4.4s, v20.4s, alphaV0
	fmla	v5.4s, v21.4s, alphaV1
	fmla	v6.4s, v22.4s, alphaV2
	fmla	v7.4s, v23.4s, alphaV3
	st1 	{v4.4s, v5.4s, v6.4s, v7.4s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld1	{v0.4s, v1.4s, v2.4s, v3.4s}, [pCRow1]
	fmla	v0.4s, v24.4s, alphaV0
	fmla	v1.4s, v25.4s, alphaV1
	fmla	v2.4s, v26.4s, alphaV2
	fmla	v3.4s, v27.4s, alphaV3
	st1 	{v0.4s, v1.4s, v2.4s, v3.4s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld1	{v4.4s, v5.4s, v6.4s, v7.4s}, [pCRow1]
	fmla	v4.4s, v28.4s, alphaV0
	fmla	v5.4s, v29.4s, alphaV1
	fmla	v6.4s, v30.4s, alphaV2
	fmla	v7.4s, v31.4s, alphaV3
	st1 	{v4.4s, v5.4s, v6.4s, v7.4s}, [pCRow1]

	add	pCRow0, pCRow0, #64
.endm

/******************************************************************************/

.macro INIT8x4
	fmov		s16, wzr
	fmov		s17, s16
	fmov		s18, s17
	fmov		s19, s16
	fmov		s20, s17
	fmov		s21, s16
	fmov		s22, s17
	fmov		s23, s16
	fmov		s24, s17
	fmov		s25, s16
	fmov		s26, s17
	fmov		s27, s16
	fmov		s28, s17
	fmov		s29, s16
	fmov		s30, s17
	fmov		s31, s16
.endm

.macro KERNEL8x4_SUB
	ld1	{v8.2s, v9.2s}, [pB]
	add	pB, pB, #16
	ld1	{v0.2s, v1.2s}, [pA_0]
	add	pA_0, pA_0, #16

	fmla	v16.2s, v0.2s, v8.s[0]
	fmla	v29.2s, v1.2s, v9.s[1]
	fmla	v20.2s, v0.2s, v8.s[1]
	fmla	v25.2s, v1.2s, v9.s[0]

	ld1	{v2.2s, v3.2s}, [pA_1]
	add	pA_1, pA_1, #16

	fmla	v24.2s, v0.2s, v9.s[0]
	fmla	v21.2s, v1.2s, v8.s[1]
	fmla	v28.2s, v0.2s, v9.s[1]
	fmla	v17.2s, v1.2s, v8.s[0]

	fmla	v18.2s, v2.2s, v8.s[0]
	fmla	v31.2s, v3.2s, v9.s[1]
	fmla	v22.2s, v2.2s, v8.s[1]
	fmla	v27.2s, v3.2s, v9.s[0]

	fmla	v26.2s, v2.2s, v9.s[0]
	fmla	v23.2s, v3.2s, v8.s[1]
	fmla	v30.2s, v2.2s, v9.s[1]
	fmla	v19.2s, v3.2s, v8.s[0]
.endm

.macro SAVE8x4
	mov	pCRow1, pCRow0

	ld1	{v0.2s, v1.2s}, [pCRow1]
	fmla	v0.2s, v16.2s, alphaV0
	fmla	v1.2s, v17.2s, alphaV1
	st1 	{v0.2s, v1.2s}, [pCRow1]

	add	pCRow2, pCRow1, LDC
	add	pCRow1, pCRow1, #16

	ld1	{v2.2s, v3.2s}, [pCRow1]
	fmla	v2.2s, v18.2s, alphaV2
	fmla	v3.2s, v19.2s, alphaV3
	st1 	{v2.2s, v3.2s}, [pCRow1]

	ld1	{v4.2s, v5.2s}, [pCRow2]
	fmla	v4.2s, v20.2s, alphaV0
	fmla	v5.2s, v21.2s, alphaV1
	st1 	{v4.2s, v5.2s}, [pCRow2]

	add	pCRow1, pCRow2, LDC
	add	pCRow2, pCRow2, #16

	ld1	{v6.2s, v7.2s}, [pCRow2]
	fmla	v6.2s, v22.2s, alphaV2
	fmla	v7.2s, v23.2s, alphaV3
	st1 	{v6.2s, v7.2s}, [pCRow2]

	ld1	{v0.2s, v1.2s}, [pCRow1]
	fmla	v0.2s, v24.2s, alphaV0
	fmla	v1.2s, v25.2s, alphaV1
	st1 	{v0.2s, v1.2s}, [pCRow1]

	add	pCRow2, pCRow1, LDC
	add	pCRow1, pCRow1, #16

	ld1	{v2.2s, v3.2s}, [pCRow1]
	fmla	v2.2s, v26.2s, alphaV2
	fmla	v3.2s, v27.2s, alphaV3
	st1 	{v2.2s, v3.2s}, [pCRow1]

	ld1	{v4.2s, v5.2s}, [pCRow2]
	fmla	v4.2s, v28.2s, alphaV0
	fmla	v5.2s, v29.2s, alphaV1
	st1 	{v4.2s, v5.2s}, [pCRow2]

	add	pCRow2, pCRow2, #16

	ld1	{v6.2s, v7.2s}, [pCRow2]
	fmla	v6.2s, v30.2s, alphaV2
	fmla	v7.2s, v31.2s, alphaV3
	st1 	{v6.2s, v7.2s}, [pCRow2]

	add	pCRow0, pCRow0, #32
.endm

/******************************************************************************/

.macro INIT4x4
	fmov		s16, wzr
	fmov		s17, s16
	fmov		s20, s17
	fmov		s21, s16
	fmov		s24, s17
	fmov		s25, s16
	fmov		s28, s17
	fmov		s29, s16
.endm

.macro KERNEL4x4_SUB
	ld1	{v8.2s, v9.2s}, [pB]
	add	pB, pB, #16
	ld1	{v0.2s, v1.2s}, [pA_0]
	add	pA_0, pA_0, #16

	fmla	v16.2s, v0.2s, v8.s[0]
	fmla	v29.2s, v1.2s, v9.s[1]

	fmla	v20.2s, v0.2s, v8.s[1]
	fmla	v25.2s, v1.2s, v9.s[0]

	fmla	v24.2s, v0.2s, v9.s[0]
	fmla	v21.2s, v1.2s, v8.s[1]

	fmla	v28.2s, v0.2s, v9.s[1]
	fmla	v17.2s, v1.2s, v8.s[0]
.endm

.macro SAVE4x4
	ld1	{v8.2s, v9.2s}, [pCRow0]
	fmla	v8.2s, v16.2s, alphaV0
	fmla	v9.2s, v17.2s, alphaV1
	st1 	{v8.2s, v9.2s}, [pCRow0]

	add	pCRow1, pCRow0, LDC

	ld1	{v12.2s, v13.2s}, [pCRow1]
	fmla	v12.2s, v20.2s, alphaV2
	fmla	v13.2s, v21.2s, alphaV3
	st1 	{v12.2s, v13.2s}, [pCRow1]

	add	pCRow2, pCRow1, LDC

	ld1	{v8.2s, v9.2s}, [pCRow2]
	fmla	v8.2s, v24.2s, alphaV0
	fmla	v9.2s, v25.2s, alphaV1
	st1 	{v8.2s, v9.2s}, [pCRow2]

	add	pCRow1, pCRow2, LDC

	ld1	{v12.2s, v13.2s}, [pCRow1]
	fmla	v12.2s, v28.2s, alphaV2
	fmla	v13.2s, v29.2s, alphaV3
	st1 	{v12.2s, v13.2s}, [pCRow1]

	add	pCRow0, pCRow0, #16
.endm

/******************************************************************************/

.macro INIT2x4
	fmov		s16, wzr
	fmov		s20, s16
	fmov		s24, s20
	fmov		s28, s16
.endm

.macro KERNEL2x4_SUB
	ld1	{v8.2s, v9.2s}, [pB]
	add	pB, pB, #16
	ld1	{v0.2s}, [pA_0]
	add	pA_0, pA_0, #8

	fmla	v16.2s, v0.2s, v8.s[0]
	fmla	v20.2s, v0.2s, v8.s[1]
	fmla	v24.2s, v0.2s, v9.s[0]
	fmla	v28.2s, v0.2s, v9.s[1]
.endm

.macro SAVE2x4
	ld1	{v8.2s}, [pCRow0]
	fmla	v8.2s, v16.2s, alphaV0
	st1	{v8.2s}, [pCRow0]

	add	pCRow1, pCRow0, LDC

	ld1	{v12.2s}, [pCRow1]
	fmla	v12.2s, v20.2s, alphaV1
	st1	{v12.2s}, [pCRow1]

	add	pCRow2, pCRow1, LDC

	ld1	{v8.2s}, [pCRow2]
	fmla	v8.2s, v24.2s, alphaV2
	st1	{v8.2s}, [pCRow2]

	add	pCRow1, pCRow2, LDC

	ld1	{v12.2s}, [pCRow1]
	fmla	v12.2s, v28.2s, alphaV3
	st1	{v12.2s}, [pCRow1]

	add	pCRow0, pCRow0, #8
.endm

/******************************************************************************/

.macro INIT1x4
	fmov		s16, wzr
	fmov		s20, s16
.endm

.macro KERNEL1x4_SUB
	ldr	s0, [pA_0]
	add	pA_0, pA_0, #4

	ld1	{v8.2s, v9.2s}, [pB]
	add	pB, pB, #16

	fmla	v16.2s, v8.2s, v0.s[0]
	fmla	v20.2s, v9.2s, v0.s[0]
.endm

.macro SAVE1x4
	add	pCRow1, pCRow0, LDC

	ld1	{v8.s}[0], [pCRow0]
	ld1	{v8.s}[1], [pCRow1]
	fmla	v8.2s, v16.2s, alphaV0
	st1	{v8.s}[0], [pCRow0]
	st1	{v8.s}[1], [pCRow1]

	add	pCRow2, pCRow1, LDC
	add	pCRow1, pCRow2, LDC

	ld1	{v12.s}[0], [pCRow2]
	ld1	{v12.s}[1], [pCRow1]
	fmla	v12.2s, v20.2s, alphaV1
	st1	{v12.s}[0], [pCRow2]
	st1	{v12.s}[1], [pCRow1]

	add	pCRow0, pCRow0, #4
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
	ld1	{v0.2s, v1.2s}, [pA_0]
	add	pA_0, pA_0, #16

	fmla	v16.2s, v0.2s, v8.s[0]
	fmla	v17.2s, v1.2s, v8.s[0]
	fmla	v20.2s, v0.2s, v8.s[1]
	fmla	v21.2s, v1.2s, v8.s[1]
.endm

.macro SAVE4x2
	ld1	{v8.2s, v9.2s}, [pCRow0]
	fmla	v8.2s, v16.2s, alphaV0
	fmla	v9.2s, v17.2s, alphaV1
	st1	{v8.2s, v9.2s}, [pCRow0]

	add	pCRow1, pCRow0, LDC

	ld1	{v12.2s, v13.2s}, [pCRow1]
	fmla	v12.2s, v20.2s, alphaV2
	fmla	v13.2s, v21.2s, alphaV3
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

	ld1	{v0.2s}, [pA_0]
	add	pA_0, pA_0, #8

	fmla	v16.2s, v0.2s, v8.s[0]
	fmla	v20.2s, v0.2s, v8.s[1]
.endm

.macro SAVE2x2
	ld1	{v8.2s}, [pCRow0]
	fmla	v8.2s, v16.2s, alphaV0
	st1	{v8.2s}, [pCRow0]

	add	pCRow1 , pCRow0, LDC

	ld1	{v12.2s}, [pCRow1]
	fmla	v12.2s, v20.2s, alphaV1
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

	ldr	s0 , [pA_0]
	add	pA_0, pA_0, #4

	fmla	v16.2s, v8.2s, v0.s[0]
.endm

.macro SAVE1x2
	add	pCRow1 , pCRow0, LDC

	ld1	{v8.s}[0], [pCRow0]
	ld1	{v8.s}[1], [pCRow1]
	fmla	v8.2s, v16.2s, alphaV0
	st1	{v8.s}[0], [pCRow0]
	st1	{v8.s}[1], [pCRow1]

	add	pCRow0, pCRow0, #4
.endm

/******************************************************************************/

.macro INIT4x1
	fmov	s16, wzr
	fmov	s17, s16
.endm

.macro KERNEL4x1_SUB
	ldr	s8, [pB]
	add	pB , pB, #4

	ld1	{v0.2s, v1.2s}, [pA_0]
	add	pA_0 , pA_0, #16

	fmla	v16.2s, v0.2s, v8.s[0]
	fmla	v17.2s, v1.2s, v8.s[0]
.endm

.macro SAVE4x1
	ld1	{v8.2s, v9.2s}, [pCRow0]
	fmla	v8.2s, v16.2s, alphaV0
	fmla	v9.2s, v17.2s, alphaV1
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

	ld1	{v0.2s}, [pA_0]
	add	pA_0 , pA_0, #8

	fmla	v16.2s, v0.2s, v8.s[0]
.endm

.macro SAVE2x1
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

	ldr	s0, [pA_0]
	add	pA_0 , pA_0, #4

	fmadd 	s16, s0, s8, s16  
.endm

.macro SAVE1x1
	ldr	s8, [pCRow0]
	fmadd	s8, s16, alpha0, s8
	str 	s8, [pCRow0]

	add	pCRow0, pCRow0, #4
.endm

/*******************************************************************************
* End of macro definitions
*******************************************************************************/

	PROLOGUE

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

	fmov	alpha0, s0
	fmov	alpha1, s0
	fmov	alpha2, s0
	fmov	alpha3, s0

	lsl	LDC, LDC, #2			// ldc = ldc * 4

	mov	pB, origPB

	mov	counterJ, origN
	asr 	counterJ, counterJ, #2		// J = J / 4
	cmp 	counterJ, #0
	ble	.Lsgemm_kernel_L2_BEGIN

/******************************************************************************/

.Lsgemm_kernel_L4_BEGIN:
	mov	pCRow0, pC			// pCRow0 = C
	add	pC, pC, LDC, lsl #2

	lsl	temp, origK, #4			// k * 4 * 4
	mov	pA_0, origPA			// pA_0 = start of A array
	add	pA_1, temp, pA_0
	add	pA_2, temp, pA_1
	add	pA_3, temp, pA_2

.Lsgemm_kernel_L4_M16_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #4		// counterI = counterI / 16
	cmp 	counterI, #0
	ble	.Lsgemm_kernel_L4_M8_BEGIN

.Lsgemm_kernel_L4_M16_20:

	mov	pB, origPB
	asr 	counterL , origK, #1		// L = K / 2
	cmp	counterL , #2			// is there at least 4 to do?
	blt	.Lsgemm_kernel_L4_M16_32

	KERNEL16x4_I				// do one in the K
	KERNEL16x4_M2				// do another in the K

	subs	counterL, counterL, #2
	ble	.Lsgemm_kernel_L4_M16_22a
	.align 5

.Lsgemm_kernel_L4_M16_22:

	KERNEL16x4_M1
	KERNEL16x4_M2

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L4_M16_22


.Lsgemm_kernel_L4_M16_22a:

	KERNEL16x4_M1
	KERNEL16x4_E

	b	 .Lsgemm_kernel_L4_M16_44

.Lsgemm_kernel_L4_M16_32:

	tst	counterL, #1
	ble	.Lsgemm_kernel_L4_M16_40

	KERNEL16x4_I

	KERNEL16x4_E

	b	.Lsgemm_kernel_L4_M16_44


.Lsgemm_kernel_L4_M16_40:

	INIT16x4

.Lsgemm_kernel_L4_M16_44:

	ands	counterL , origK, #1
	ble	.Lsgemm_kernel_L4_M16_100

.Lsgemm_kernel_L4_M16_46:

	KERNEL16x4_SUB

.Lsgemm_kernel_L4_M16_100:

	SAVE16x4

.Lsgemm_kernel_L4_M16_END:
	lsl	temp, origK, #4			// k * 4 * 4 = Four rows of A
	add	pA_0, pA_0, temp
	add	pA_0, pA_0, temp
	add	pA_0, pA_0, temp
	add	pA_1, pA_0, temp
	add	pA_2, pA_1, temp
	add	pA_3, pA_2, temp
	subs	counterI, counterI, #1
	bne	.Lsgemm_kernel_L4_M16_20

.Lsgemm_kernel_L4_M8_BEGIN:
	mov	counterI, origM
	tst	counterI , #15
	ble	.Lsgemm_kernel_L4_END

	tst	counterI, #8
	ble	.Lsgemm_kernel_L4_M4_BEGIN

.Lsgemm_kernel_L4_M8_20:

	INIT8x4

	mov	pB, origPB
	asr 	counterL, origK, #3		// counterL = counterL / 8
	cmp	counterL, #0
	ble	.Lsgemm_kernel_L4_M8_40

.Lsgemm_kernel_L4_M8_22:

	KERNEL8x4_SUB
	KERNEL8x4_SUB
	KERNEL8x4_SUB
	KERNEL8x4_SUB

	KERNEL8x4_SUB
	KERNEL8x4_SUB
	KERNEL8x4_SUB
	KERNEL8x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L4_M8_22


.Lsgemm_kernel_L4_M8_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lsgemm_kernel_L4_M8_100

.Lsgemm_kernel_L4_M8_42:

	KERNEL8x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L4_M8_42

.Lsgemm_kernel_L4_M8_100:

	SAVE8x4

.Lsgemm_kernel_L4_M8_END:
	lsl	temp, origK, #4			// k * 4 * 4
	add	pA_0, pA_0, temp

.Lsgemm_kernel_L4_M4_BEGIN:
	mov	counterI, origM
	tst	counterI , #7
	ble	.Lsgemm_kernel_L4_END

	tst	counterI, #4
	ble	.Lsgemm_kernel_L4_M2_BEGIN

.Lsgemm_kernel_L4_M4_20:

	INIT4x4

	mov	pB, origPB
	asr 	counterL, origK, #3		// counterL = counterL / 8
	cmp	counterL, #0
	ble	.Lsgemm_kernel_L4_M4_40

.Lsgemm_kernel_L4_M4_22:

	KERNEL4x4_SUB
	KERNEL4x4_SUB
	KERNEL4x4_SUB
	KERNEL4x4_SUB

	KERNEL4x4_SUB
	KERNEL4x4_SUB
	KERNEL4x4_SUB
	KERNEL4x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L4_M4_22


.Lsgemm_kernel_L4_M4_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lsgemm_kernel_L4_M4_100

.Lsgemm_kernel_L4_M4_42:

	KERNEL4x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lsgemm_kernel_L4_M4_42

.Lsgemm_kernel_L4_M4_100:

	SAVE4x4

.Lsgemm_kernel_L4_M4_END:


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

	lsl	temp, origK, #4 
	add	origPB, origPB, temp		// B = B + K * 4 * 4

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

	mov	pA_0, origPA			// pA_0 = A



.Lsgemm_kernel_L2_M4_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #2		// counterI = counterI / 4
	cmp	counterI,#0
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

	subs	counterI, counterI, #1
	bgt	.Lsgemm_kernel_L2_M4_20


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

	mov	pA_0, origPA			// pA_0 = A



.Lsgemm_kernel_L1_M4_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #2		// counterI = counterI / 4
	cmp	counterI, #0
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

	subs	counterI, counterI, #1
	bgt	.Lsgemm_kernel_L1_M4_20


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

