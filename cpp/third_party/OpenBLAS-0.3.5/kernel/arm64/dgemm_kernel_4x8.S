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

/*                   X0          X1          X2          s0         X3        x4       x5           x6 */
/*int CNAME(BLASLONG bm,BLASLONG bn,BLASLONG bk,FLOAT alpha0,FLOAT* ba,FLOAT* bb,FLOAT* C,BLASLONG ldc )*/

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
#define pA		x15

#define alpha0		d2
#define alphaV0		v2.d[0]
#define alpha1		d3
#define alphaV1		v3.d[0]
#define alpha2		d6
#define alphaV2		v6.d[0]
#define alpha3		d7
#define alphaV3		v7.d[0]

// 00 origM
// 01 origN
// 02 origK
// 03 origPA
// 04 origPB
// 05 pC
// 06 origLDC -> LDC
// 07 temp
// 08 counterL
// 09 counterI
// 10 counterJ
// 11 pB
// 12 pCRow0
// 13 pCRow1
// 14 pCRow2
// 15 pA
// 16
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

//v00 ALPHA -> pA00, pA01
//v01 pA02, pA03
//v02 ALPHA0
//v03 ALPHA1
//v04 pA10, pA11
//v05 pA12, pA13
//v06 ALPHA2
//v07 ALPHA3
//v08 must save pB0_0, pB0_1
//v09 must save pB0_2, pB0_3
//v10 must save pB0_4, pB0_5
//v11 must save pB0_6, pB0_7
//v12 must save pB1_0, pB1_1
//v13 must save pB1_2, pB1_3
//v14 must save pB1_4, pB1_5
//v15 must save pB1_6, pB1_7
//v16 must save C00, C01
//v17 must save C02, C03
//v18 C04, C05
//v19 C06, C07
//v20 C10, C11
//v21 C12, C13
//v22 C14, C15
//v23 C16, C17
//v24 C20, C21
//v25 C22, C23
//v26 C24, C25
//v27 C26, C27
//v28 C30, C31
//v29 C32, C33
//v30 C34, C35
//v31 C36, C37

/*******************************************************************************
* Macro definitions
*******************************************************************************/

.macro INIT4x8
	fmov		d16, xzr
	fmov		d17, xzr
	fmov		d18, xzr
	fmov		d19, d16
	fmov		d20, xzr
	fmov		d21, d16
	fmov		d22, d17
	fmov		d23, d18
	fmov		d24, xzr
	fmov		d25, d16
	fmov		d26, d17
	fmov		d27, d18
	fmov		d28, xzr
	fmov		d29, d16
	fmov		d30, d17
	fmov		d31, d18
.endm

.macro KERNEL4x8_I
	ld1	{v8.2d, v9.2d}, [pB]
	add	pB, pB, #32
	ld1	{v0.2d, v1.2d}, [pA]
	add	pA, pA, #32
	ld1	{v10.2d, v11.2d}, [pB]
	add	pB, pB, #32

	fmul	v16.2d, v0.2d, v8.d[0]
	fmul	v17.2d, v1.2d, v8.d[0]
	fmul	v18.2d, v0.2d, v8.d[1]
	fmul	v19.2d, v1.2d, v8.d[1]

	fmul	v20.2d, v0.2d, v9.d[0]
	fmul	v21.2d, v1.2d, v9.d[0]
	fmul	v22.2d, v0.2d, v9.d[1]
	fmul	v23.2d, v1.2d, v9.d[1]

	fmul	v24.2d, v0.2d, v10.d[0]
	fmul	v25.2d, v1.2d, v10.d[0]
	fmul	v26.2d, v0.2d, v10.d[1]
	fmul	v27.2d, v1.2d, v10.d[1]

	fmul	v28.2d, v0.2d, v11.d[0]
	fmul	v29.2d, v1.2d, v11.d[0]
	fmul	v30.2d, v0.2d, v11.d[1]
	fmul	v31.2d, v1.2d, v11.d[1]

	ld1	{v12.2d, v13.2d}, [pB]
	add	pB, pB, #32
	ld1	{v4.2d, v5.2d}, [pA]
	add	pA, pA, #32
	ld1	{v14.2d, v15.2d}, [pB]
	add	pB, pB, #32
.endm

.macro KERNEL4x8_M1
	fmla	v16.2d, v0.2d, v8.d[0]
	fmla	v17.2d, v1.2d, v8.d[0]
	fmla	v18.2d, v0.2d, v8.d[1]
	fmla	v19.2d, v1.2d, v8.d[1]

	fmla	v20.2d, v0.2d, v9.d[0]
	fmla	v21.2d, v1.2d, v9.d[0]
	fmla	v22.2d, v0.2d, v9.d[1]
	fmla	v23.2d, v1.2d, v9.d[1]

	fmla	v24.2d, v0.2d, v10.d[0]
	fmla	v25.2d, v1.2d, v10.d[0]
	fmla	v26.2d, v0.2d, v10.d[1]
	fmla	v27.2d, v1.2d, v10.d[1]

	fmla	v28.2d, v0.2d, v11.d[0]
	fmla	v29.2d, v1.2d, v11.d[0]
	fmla	v30.2d, v0.2d, v11.d[1]
	fmla	v31.2d, v1.2d, v11.d[1]

	ld1	{v12.2d, v13.2d}, [pB]		// For next round
	add	pB, pB, #32
	ld1	{v4.2d, v5.2d}, [pA]		// For next round
	add	pA, pA, #32
	ld1	{v14.2d, v15.2d}, [pB]
	add	pB, pB, #32

	prfm	PLDL1KEEP, [pA, #512]
.endm

.macro KERNEL4x8_M2
	fmla	v16.2d, v4.2d, v12.d[0]
	fmla	v17.2d, v5.2d, v12.d[0]
	fmla	v18.2d, v4.2d, v12.d[1]
	fmla	v19.2d, v5.2d, v12.d[1]

	fmla	v20.2d, v4.2d, v13.d[0]
	fmla	v21.2d, v5.2d, v13.d[0]
	fmla	v22.2d, v4.2d, v13.d[1]
	fmla	v23.2d, v5.2d, v13.d[1]

	fmla	v24.2d, v4.2d, v14.d[0]
	fmla	v25.2d, v5.2d, v14.d[0]
	fmla	v26.2d, v4.2d, v14.d[1]
	fmla	v27.2d, v5.2d, v14.d[1]

	fmla	v28.2d, v4.2d, v15.d[0]
	fmla	v29.2d, v5.2d, v15.d[0]
	fmla	v30.2d, v4.2d, v15.d[1]
	fmla	v31.2d, v5.2d, v15.d[1]

	ld1	{v8.2d, v9.2d}, [pB]		// For next round
	add	pB, pB, #32
	ld1	{v0.2d, v1.2d}, [pA]		// For next round
	add	pA, pA, #32
	ld1	{v10.2d, v11.2d}, [pB]
	add	pB, pB, #32

	prfm	PLDL1KEEP, [pB, #512]
.endm

.macro KERNEL4x8_E
	fmla	v16.2d, v4.2d, v12.d[0]
	fmla	v17.2d, v5.2d, v12.d[0]
	fmla	v18.2d, v4.2d, v12.d[1]
	fmla	v19.2d, v5.2d, v12.d[1]

	fmla	v20.2d, v4.2d, v13.d[0]
	fmla	v21.2d, v5.2d, v13.d[0]
	fmla	v22.2d, v4.2d, v13.d[1]
	fmla	v23.2d, v5.2d, v13.d[1]

	fmla	v24.2d, v4.2d, v14.d[0]
	fmla	v25.2d, v5.2d, v14.d[0]
	fmla	v26.2d, v4.2d, v14.d[1]
	fmla	v27.2d, v5.2d, v14.d[1]

	fmla	v28.2d, v4.2d, v15.d[0]
	fmla	v29.2d, v5.2d, v15.d[0]
	fmla	v30.2d, v4.2d, v15.d[1]
	fmla	v31.2d, v5.2d, v15.d[1]
.endm

.macro KERNEL4x8_SUB
	ld1	{v8.2d, v9.2d}, [pB]		// For next round
	add	pB, pB, #32
	ld1	{v0.2d, v1.2d}, [pA]		// For next round
	add	pA, pA, #32
	ld1	{v10.2d, v11.2d}, [pB]
	add	pB, pB, #32

	fmla	v16.2d, v0.2d, v8.d[0]
	fmla	v17.2d, v1.2d, v8.d[0]
	fmla	v18.2d, v0.2d, v8.d[1]
	fmla	v19.2d, v1.2d, v8.d[1]

	fmla	v20.2d, v0.2d, v9.d[0]
	fmla	v21.2d, v1.2d, v9.d[0]
	fmla	v22.2d, v0.2d, v9.d[1]
	fmla	v23.2d, v1.2d, v9.d[1]

	fmla	v24.2d, v0.2d, v10.d[0]
	fmla	v25.2d, v1.2d, v10.d[0]
	fmla	v26.2d, v0.2d, v10.d[1]
	fmla	v27.2d, v1.2d, v10.d[1]

	fmla	v28.2d, v0.2d, v11.d[0]
	fmla	v29.2d, v1.2d, v11.d[0]
	fmla	v30.2d, v0.2d, v11.d[1]
	fmla	v31.2d, v1.2d, v11.d[1]
.endm

.macro SAVE4x8
	add	pCRow1, pCRow0, LDC

	ld1	{v8.2d, v9.2d}, [pCRow0]
	fmla	v8.2d, v16.2d, alphaV0
	fmla	v9.2d, v17.2d, alphaV1
	st1 	{v8.2d, v9.2d}, [pCRow0]

	add	pCRow2, pCRow1, LDC

	ld1	{v10.2d, v11.2d}, [pCRow1]
	fmla	v10.2d, v18.2d, alphaV2
	fmla	v11.2d, v19.2d, alphaV3
	st1 	{v10.2d, v11.2d}, [pCRow1]

	add	pCRow1, pCRow2, LDC

	ld1	{v12.2d, v13.2d}, [pCRow2]
	fmla	v12.2d, v20.2d, alphaV0
	fmla	v13.2d, v21.2d, alphaV1
	st1 	{v12.2d, v13.2d}, [pCRow2]

	add	pCRow2, pCRow1, LDC

	ld1	{v14.2d, v15.2d}, [pCRow1]
	fmla	v14.2d, v22.2d, alphaV2
	fmla	v15.2d, v23.2d, alphaV3
	st1 	{v14.2d, v15.2d}, [pCRow1]

	add	pCRow1, pCRow2, LDC

	ld1	{v8.2d, v9.2d}, [pCRow2]
	fmla	v8.2d, v24.2d, alphaV0
	fmla	v9.2d, v25.2d, alphaV1
	st1 	{v8.2d, v9.2d}, [pCRow2]

	add	pCRow2, pCRow1, LDC

	ld1	{v10.2d, v11.2d}, [pCRow1]
	fmla	v10.2d, v26.2d, alphaV2
	fmla	v11.2d, v27.2d, alphaV3
	st1 	{v10.2d, v11.2d}, [pCRow1]

	add	pCRow1, pCRow2, LDC

	ld1	{v12.2d, v13.2d}, [pCRow2]
	fmla	v12.2d, v28.2d, alphaV0
	fmla	v13.2d, v29.2d, alphaV1
	st1 	{v12.2d, v13.2d}, [pCRow2]

	ld1	{v14.2d, v15.2d}, [pCRow1]
	fmla	v14.2d, v30.2d, alphaV2
	fmla	v15.2d, v31.2d, alphaV3
	st1 	{v14.2d, v15.2d}, [pCRow1]

	add	pCRow0, pCRow0, #32
.endm

/******************************************************************************/

.macro INIT2x8
	fmov	d16, xzr
	fmov	d18, xzr
	fmov	d20, xzr
	fmov	d22, d16
	fmov	d24, xzr
	fmov	d26, d16
	fmov	d28, xzr
	fmov	d30, d16
.endm

.macro KERNEL2x8_SUB
	ld1	{v8.2d, v9.2d}, [pB]
	add	pB, pB, #32
	ld1	{v0.2d}, [pA]
	add	pA, pA, #16
	ld1	{v10.2d, v11.2d}, [pB]
	add	pB, pB, #32

	fmla	v16.2d, v0.2d, v8.d[0]
	fmla	v18.2d, v0.2d, v8.d[1]

	fmla	v20.2d, v0.2d, v9.d[0]
	fmla	v22.2d, v0.2d, v9.d[1]

	fmla	v24.2d, v0.2d, v10.d[0]
	fmla	v26.2d, v0.2d, v10.d[1]

	fmla	v28.2d, v0.2d, v11.d[0]
	fmla	v30.2d, v0.2d, v11.d[1]
.endm

.macro SAVE2x8
	add	pCRow1, pCRow0, LDC

	ld1	{v8.2d}, [pCRow0]
	fmla	v8.2d, v16.2d, alphaV0
	st1 	{v8.2d}, [pCRow0]

	add	pCRow2, pCRow1, LDC

	ld1	{v10.2d}, [pCRow1]
	fmla	v10.2d, v18.2d, alphaV2
	st1 	{v10.2d}, [pCRow1]

	add	pCRow1, pCRow2, LDC

	ld1	{v12.2d}, [pCRow2]
	fmla	v12.2d, v20.2d, alphaV0
	st1 	{v12.2d}, [pCRow2]

	add	pCRow2, pCRow1, LDC

	ld1	{v14.2d}, [pCRow1]
	fmla	v14.2d, v22.2d, alphaV2
	st1 	{v14.2d}, [pCRow1]

	add	pCRow1, pCRow2, LDC

	ld1	{v8.2d}, [pCRow2]
	fmla	v8.2d, v24.2d, alphaV0
	st1 	{v8.2d}, [pCRow2]

	add	pCRow2, pCRow1, LDC

	ld1	{v10.2d}, [pCRow1]
	fmla	v10.2d, v26.2d, alphaV2
	st1 	{v10.2d}, [pCRow1]

	add	pCRow1, pCRow2, LDC

	ld1	{v12.2d}, [pCRow2]
	fmla	v12.2d, v28.2d, alphaV0
	st1 	{v12.2d}, [pCRow2]

	add	pCRow2, pCRow1, LDC

	ld1	{v14.2d}, [pCRow1]
	fmla	v14.2d, v30.2d, alphaV2
	st1 	{v14.2d}, [pCRow1]

	add	pCRow0, pCRow0, #16
.endm

/******************************************************************************/

.macro INIT1x8
	fmov	d16, xzr
	fmov	d20, xzr
	fmov	d24, xzr
	fmov	d28, xzr
.endm

.macro KERNEL1x8_SUB
	ld1	{v8.2d, v9.2d}, [pB]
	add	pB, pB, #32
	ldr	d0, [pA]
	add	pA, pA, #8
	ld1	{v10.2d, v11.2d}, [pB]
	add	pB, pB, #32

	fmla	v16.2d, v8.2d, v0.d[0]
	fmla	v20.2d, v9.2d, v0.d[0]
	fmla	v24.2d, v10.2d, v0.d[0]
	fmla	v28.2d, v11.2d, v0.d[0]
.endm

.macro SAVE1x8
	add	pCRow1, pCRow0, LDC

	ld1	{v8.d}[0], [pCRow0]
	ld1	{v8.d}[1], [pCRow1]
	fmla	v8.2d, v16.2d, alphaV0
	st1	{v8.d}[0], [pCRow0]
	st1	{v8.d}[1], [pCRow1]

	add	pCRow2, pCRow1, LDC
	add	pCRow1, pCRow2, LDC

	ld1	{v10.d}[0], [pCRow2]
	ld1	{v10.d}[1], [pCRow1]
	fmla	v10.2d, v20.2d, alphaV1
	st1	{v10.d}[0], [pCRow2]
	st1	{v10.d}[1], [pCRow1]

	add	pCRow2, pCRow1, LDC
	add	pCRow1, pCRow2, LDC

	ld1	{v12.d}[0], [pCRow2]
	ld1	{v12.d}[1], [pCRow1]
	fmla	v12.2d, v24.2d, alphaV2
	st1	{v12.d}[0], [pCRow2]
	st1	{v12.d}[1], [pCRow1]

	add	pCRow2, pCRow1, LDC
	add	pCRow1, pCRow2, LDC

	ld1	{v14.d}[0], [pCRow2]
	ld1	{v14.d}[1], [pCRow1]
	fmla	v14.2d, v28.2d, alphaV3
	st1	{v14.d}[0], [pCRow2]
	st1	{v14.d}[1], [pCRow1]

	add	pCRow0, pCRow0, #8
.endm

/******************************************************************************/

.macro INIT4x4
	fmov		d16, xzr
	fmov		d17, d16
	fmov		d20, d17
	fmov		d21, d16
	fmov		d24, d17
	fmov		d25, d16
	fmov		d28, d17
	fmov		d29, d16
.endm

.macro KERNEL4x4_I
	ld1	{v8.2d, v9.2d}, [pB]
	add	pB, pB, #32
	ld1	{v0.2d, v1.2d}, [pA]
	add	pA, pA, #32

	fmul	v16.2d, v0.2d, v8.d[0]
	fmul	v29.2d, v1.2d, v9.d[1]

	fmul	v20.2d, v0.2d, v8.d[1]
	fmul	v25.2d, v1.2d, v9.d[0]

	fmul	v24.2d, v0.2d, v9.d[0]
	fmul	v21.2d, v1.2d, v8.d[1]

	fmul	v28.2d, v0.2d, v9.d[1]
	fmul	v17.2d, v1.2d, v8.d[0]

	ld1	{v12.2d, v13.2d}, [pB]
	add	pB, pB, #32
	ld1	{v4.2d, v5.2d}, [pA]
	add	pA, pA, #32
.endm

.macro KERNEL4x4_M1
	fmla	v16.2d, v0.2d, v8.d[0]
	fmla	v29.2d, v1.2d, v9.d[1]

	ld1	{v12.2d, v13.2d}, [pB]		// For next round
	add	pB, pB, #32

	fmla	v20.2d, v0.2d, v8.d[1]
	fmla	v25.2d, v1.2d, v9.d[0]

	ld1	{v4.2d, v5.2d}, [pA]		// For next round
	add	pA, pA, #32

	fmla	v24.2d, v0.2d, v9.d[0]
	fmla	v21.2d, v1.2d, v8.d[1]

	prfm	PLDL1KEEP, [pA, #512]

	fmla	v28.2d, v0.2d, v9.d[1]
	fmla	v17.2d, v1.2d, v8.d[0]
.endm

.macro KERNEL4x4_M2
	fmla	v16.2d, v4.2d, v12.d[0]
	fmla	v29.2d, v5.2d, v13.d[1]

	ld1	{v8.2d, v9.2d}, [pB]		// For next round
	add	pB, pB, #32

	fmla	v20.2d, v4.2d, v12.d[1]
	fmla	v25.2d, v5.2d, v13.d[0]

	ld1	{v0.2d, v1.2d}, [pA]		// For next round
	add	pA, pA, #32

	fmla	v24.2d, v4.2d, v13.d[0]
	fmla	v21.2d, v5.2d, v12.d[1]

	prfm	PLDL1KEEP, [pB, #512]

	fmla	v28.2d, v4.2d, v13.d[1]
	fmla	v17.2d, v5.2d, v12.d[0]
.endm

.macro KERNEL4x4_E
	fmla	v16.2d, v4.2d, v12.d[0]
	fmla	v29.2d, v5.2d, v13.d[1]

	fmla	v20.2d, v4.2d, v12.d[1]
	fmla	v25.2d, v5.2d, v13.d[0]

	fmla	v24.2d, v4.2d, v13.d[0]
	fmla	v21.2d, v5.2d, v12.d[1]

	fmla	v28.2d, v4.2d, v13.d[1]
	fmla	v17.2d, v5.2d, v12.d[0]
.endm

.macro KERNEL4x4_SUB
	ld1	{v8.2d, v9.2d}, [pB]
	add	pB, pB, #32
	ld1	{v0.2d, v1.2d}, [pA]
	add	pA, pA, #32

	fmla	v16.2d, v0.2d, v8.d[0]
	fmla	v29.2d, v1.2d, v9.d[1]

	fmla	v20.2d, v0.2d, v8.d[1]
	fmla	v25.2d, v1.2d, v9.d[0]

	fmla	v24.2d, v0.2d, v9.d[0]
	fmla	v21.2d, v1.2d, v8.d[1]

	fmla	v28.2d, v0.2d, v9.d[1]
	fmla	v17.2d, v1.2d, v8.d[0]
.endm

.macro SAVE4x4
	ld1	{v8.2d, v9.2d}, [pCRow0]
	fmla	v8.2d, v16.2d, alphaV0
	fmla	v9.2d, v17.2d, alphaV1
	st1 	{v8.2d, v9.2d}, [pCRow0]

	add	pCRow1, pCRow0, LDC

	ld1	{v12.2d, v13.2d}, [pCRow1]
	fmla	v12.2d, v20.2d, alphaV2
	fmla	v13.2d, v21.2d, alphaV3
	st1 	{v12.2d, v13.2d}, [pCRow1]

	add	pCRow2, pCRow1, LDC

	ld1	{v8.2d, v9.2d}, [pCRow2]
	fmla	v8.2d, v24.2d, alphaV0
	fmla	v9.2d, v25.2d, alphaV1
	st1 	{v8.2d, v9.2d}, [pCRow2]

	add	pCRow1, pCRow2, LDC

	ld1	{v12.2d, v13.2d}, [pCRow1]
	fmla	v12.2d, v28.2d, alphaV2
	fmla	v13.2d, v29.2d, alphaV3
	st1 	{v12.2d, v13.2d}, [pCRow1]

	add	pCRow0, pCRow0, #32
.endm

/******************************************************************************/

.macro INIT2x4
	fmov		d16, xzr
	fmov		d20, d16
	fmov		d24, d20
	fmov		d28, d16
.endm

.macro KERNEL2x4_SUB
	ld1	{v8.2d, v9.2d}, [pB]
	add	pB, pB, #32
	ld1	{v0.2d}, [pA]
	add	pA, pA, #16

	fmla	v16.2d, v0.2d, v8.d[0]
	fmla	v20.2d, v0.2d, v8.d[1]
	fmla	v24.2d, v0.2d, v9.d[0]
	fmla	v28.2d, v0.2d, v9.d[1]
.endm

.macro SAVE2x4
	ld1	{v8.2d}, [pCRow0]
	fmla	v8.2d, v16.2d, alphaV0
	st1	{v8.2d}, [pCRow0]

	add	pCRow1, pCRow0, LDC

	ld1	{v12.2d}, [pCRow1]
	fmla	v12.2d, v20.2d, alphaV1
	st1	{v12.2d}, [pCRow1]

	add	pCRow2, pCRow1, LDC

	ld1	{v8.2d}, [pCRow2]
	fmla	v8.2d, v24.2d, alphaV2
	st1	{v8.2d}, [pCRow2]

	add	pCRow1, pCRow2, LDC

	ld1	{v12.2d}, [pCRow1]
	fmla	v12.2d, v28.2d, alphaV3
	st1	{v12.2d}, [pCRow1]

	add	pCRow0, pCRow0, #16
.endm

/******************************************************************************/

.macro INIT1x4
	fmov		d16, xzr
	fmov		d20, d16
.endm

.macro KERNEL1x4_SUB
	ldr	d0, [pA]
	add	pA, pA, #8

	ld1	{v8.2d, v9.2d}, [pB]
	add	pB, pB, #32

	fmla	v16.2d, v8.2d, v0.d[0]
	fmla	v20.2d, v9.2d, v0.d[0]
.endm

.macro SAVE1x4
	add	pCRow1, pCRow0, LDC

	ld1	{v8.d}[0], [pCRow0]
	ld1	{v8.d}[1], [pCRow1]
	fmla	v8.2d, v16.2d, alphaV0
	st1	{v8.d}[0], [pCRow0]
	st1	{v8.d}[1], [pCRow1]

	add	pCRow2, pCRow1, LDC
	add	pCRow1, pCRow2, LDC

	ld1	{v12.d}[0], [pCRow2]
	ld1	{v12.d}[1], [pCRow1]
	fmla	v12.2d, v20.2d, alphaV1
	st1	{v12.d}[0], [pCRow2]
	st1	{v12.d}[1], [pCRow1]

	add	pCRow0, pCRow0, #8
.endm

/******************************************************************************/

.macro INIT4x2
	fmov	d16, xzr
	fmov	d17, d16
	fmov	d20, d17
	fmov	d21, d16
.endm

.macro KERNEL4x2_SUB
	ld1	{v8.2d}, [pB]
	add	pB, pB, #16
	ld1	{v0.2d, v1.2d}, [pA]
	add	pA, pA, #32

	fmla	v16.2d, v0.2d, v8.d[0]
	fmla	v17.2d, v1.2d, v8.d[0]
	fmla	v20.2d, v0.2d, v8.d[1]
	fmla	v21.2d, v1.2d, v8.d[1]
.endm

.macro SAVE4x2
	ld1	{v8.2d, v9.2d}, [pCRow0]
	fmla	v8.2d, v16.2d, alphaV0
	fmla	v9.2d, v17.2d, alphaV1
	st1	{v8.2d, v9.2d}, [pCRow0]

	add	pCRow1, pCRow0, LDC

	ld1	{v12.2d, v13.2d}, [pCRow1]
	fmla	v12.2d, v20.2d, alphaV2
	fmla	v13.2d, v21.2d, alphaV3
	st1	{v12.2d, v13.2d}, [pCRow1]

	add	pCRow0, pCRow0, #32
.endm

/******************************************************************************/

.macro INIT2x2
	fmov		d16, xzr
	fmov		d20, d16
.endm

.macro KERNEL2x2_SUB
	ld1	{v8.2d}, [pB]
	add	pB, pB, #16

	ld1	{v0.2d}, [pA]
	add	pA, pA, #16

	fmla	v16.2d, v0.2d, v8.d[0]
	fmla	v20.2d, v0.2d, v8.d[1]
.endm

.macro SAVE2x2
	ld1	{v8.2d}, [pCRow0]
	fmla	v8.2d, v16.2d, alphaV0
	st1	{v8.2d}, [pCRow0]

	add	pCRow1 , pCRow0, LDC

	ld1	{v12.2d}, [pCRow1]
	fmla	v12.2d, v20.2d, alphaV1
	st1	{v12.2d}, [pCRow1]

	add	pCRow0, pCRow0, #16
.endm

/******************************************************************************/

.macro INIT1x2
	fmov		d16, xzr
.endm

.macro KERNEL1x2_SUB
	ld1	{v8.2d} , [pB]
	add	pB , pB, #16

	ldr	d0 , [pA]
	add	pA, pA, #8

	fmla	v16.2d, v8.2d, v0.d[0]
.endm

.macro SAVE1x2
	add	pCRow1 , pCRow0, LDC

	ld1	{v8.d}[0], [pCRow0]
	ld1	{v8.d}[1], [pCRow1]
	fmla	v8.2d, v16.2d, alphaV0
	st1	{v8.d}[0], [pCRow0]
	st1	{v8.d}[1], [pCRow1]

	add	pCRow0, pCRow0, #8
.endm

/******************************************************************************/

.macro INIT4x1
	fmov	d16, xzr
	fmov	d17, d16
.endm

.macro KERNEL4x1_SUB
	ldr	d8, [pB]
	add	pB , pB, #8

	ld1	{v0.2d, v1.2d}, [pA]
	add	pA , pA, #32

	fmla	v16.2d, v0.2d, v8.d[0]
	fmla	v17.2d, v1.2d, v8.d[0]
.endm

.macro SAVE4x1
	ld1	{v8.2d, v9.2d}, [pCRow0]
	fmla	v8.2d, v16.2d, alphaV0
	fmla	v9.2d, v17.2d, alphaV1
	st1	{v8.2d, v9.2d}, [pCRow0]

	add	pCRow0, pCRow0, #32
.endm




/******************************************************************************/

.macro INIT2x1
	fmov		d16, xzr
.endm

.macro KERNEL2x1_SUB
	ldr	d8, [pB]
	add	pB , pB, #8

	ld1	{v0.2d}, [pA]
	add	pA , pA, #16

	fmla	v16.2d, v0.2d, v8.d[0]
.endm

.macro SAVE2x1
	ld1	{v8.2d}, [pCRow0]
	fmla	v8.2d, v16.2d, alphaV0
	st1	{v8.2d}, [pCRow0]

	add	pCRow0, pCRow0, #16
.endm

/******************************************************************************/

.macro INIT1x1
	fmov	d16, xzr
.endm

.macro KERNEL1x1_SUB
	ldr	d8, [pB]
	add	pB , pB, #8

	ldr	d0, [pA]
	add	pA , pA, #8

	fmadd 	d16, d0, d8, d16  
.endm

.macro SAVE1x1
	ldr	d8, [pCRow0]
	fmadd	d8, d16, alpha0, d8
	str 	d8, [pCRow0]

	add	pCRow0, pCRow0, #8
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

	fmov	alpha0, d0
	fmov	alpha1, d0
	fmov	alpha2, d0
	fmov	alpha3, d0

	lsl	LDC, LDC, #3			// ldc = ldc * 8

	mov	pB, origPB

	mov	counterJ, origN
	asr 	counterJ, counterJ, #3		// J = J / 8
	cmp 	counterJ, #0
	ble	.Ldgemm_kernel_L4_BEGIN

/******************************************************************************/

.Ldgemm_kernel_L8_BEGIN:

	mov	pCRow0, pC			// pCRow0 = C
	add	pC, pC, LDC, lsl #3

	mov	pA, origPA			// pA = start of A array

.Ldgemm_kernel_L8_M4_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #2		// counterI = counterI / 4
	cmp 	counterI, #0
	ble	.Ldgemm_kernel_L8_M2_BEGIN

.Ldgemm_kernel_L8_M4_20:

	mov	pB, origPB

	asr 	counterL , origK, #1		// L = K / 2
	cmp	counterL , #2			// is there at least 4 to do?
	blt	.Ldgemm_kernel_L8_M4_32

	KERNEL4x8_I				// do one in the K
	KERNEL4x8_M2				// do another in the K

	subs	counterL, counterL, #2
	ble	.Ldgemm_kernel_L8_M4_22a
	.align 5

.Ldgemm_kernel_L8_M4_22:

	KERNEL4x8_M1
	KERNEL4x8_M2

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L8_M4_22


.Ldgemm_kernel_L8_M4_22a:

	KERNEL4x8_M1
	KERNEL4x8_E

	b	 .Ldgemm_kernel_L8_M4_44

.Ldgemm_kernel_L8_M4_32:

	tst	counterL, #1
	ble	.Ldgemm_kernel_L8_M4_40

	KERNEL4x8_I

	KERNEL4x8_E

	b	.Ldgemm_kernel_L8_M4_44


.Ldgemm_kernel_L8_M4_40:

	INIT4x8

.Ldgemm_kernel_L8_M4_44:

	ands	counterL , origK, #1
	ble	.Ldgemm_kernel_L8_M4_100

.Ldgemm_kernel_L8_M4_46:

	KERNEL4x8_SUB

.Ldgemm_kernel_L8_M4_100:

	SAVE4x8

.Ldgemm_kernel_L8_M4_END:
	subs	counterI, counterI, #1
	bne	.Ldgemm_kernel_L8_M4_20

.Ldgemm_kernel_L8_M2_BEGIN:

	mov	counterI, origM
	tst	counterI , #3
	ble	.Ldgemm_kernel_L8_END

	tst	counterI, #2			// counterI = counterI / 2
	ble	.Ldgemm_kernel_L8_M1_BEGIN

.Ldgemm_kernel_L8_M2_20:

	INIT2x8

	mov	pB, origPB

	asr 	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Ldgemm_kernel_L8_M2_40

.Ldgemm_kernel_L8_M2_22:

	KERNEL2x8_SUB
	KERNEL2x8_SUB
	KERNEL2x8_SUB
	KERNEL2x8_SUB

	KERNEL2x8_SUB
	KERNEL2x8_SUB
	KERNEL2x8_SUB
	KERNEL2x8_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L8_M2_22


.Ldgemm_kernel_L8_M2_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Ldgemm_kernel_L8_M2_100

.Ldgemm_kernel_L8_M2_42:

	KERNEL2x8_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L8_M2_42

.Ldgemm_kernel_L8_M2_100:

	SAVE2x8

.Ldgemm_kernel_L8_M2_END:


.Ldgemm_kernel_L8_M1_BEGIN:

	tst	counterI, #1			// counterI = counterI % 2
	ble	.Ldgemm_kernel_L8_END

.Ldgemm_kernel_L8_M1_20:

	INIT1x8

	mov	pB, origPB

	asr 	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Ldgemm_kernel_L8_M1_40

.Ldgemm_kernel_L8_M1_22:
	KERNEL1x8_SUB
	KERNEL1x8_SUB
	KERNEL1x8_SUB
	KERNEL1x8_SUB

	KERNEL1x8_SUB
	KERNEL1x8_SUB
	KERNEL1x8_SUB
	KERNEL1x8_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L8_M1_22


.Ldgemm_kernel_L8_M1_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Ldgemm_kernel_L8_M1_100

.Ldgemm_kernel_L8_M1_42:

	KERNEL1x8_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L8_M1_42

.Ldgemm_kernel_L8_M1_100:

	SAVE1x8

.Ldgemm_kernel_L8_END:

	lsl	temp, origK, #6
	add	origPB, origPB, temp		// B = B + K * 8 * 8

	subs	counterJ, counterJ , #1		// j--
	bgt	.Ldgemm_kernel_L8_BEGIN


/******************************************************************************/

.Ldgemm_kernel_L4_BEGIN:

	mov	counterJ , origN
	tst	counterJ , #7
	ble	.Ldgemm_kernel_L999

	tst	counterJ , #4
	ble	.Ldgemm_kernel_L2_BEGIN

	mov	pCRow0, pC			// pCRow0 = C
	add	pC, pC, LDC, lsl #2

	mov	pA, origPA			// pA = start of A array

.Ldgemm_kernel_L4_M4_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #2		// counterI = counterI / 4
	cmp 	counterI, #0
	ble	.Ldgemm_kernel_L4_M2_BEGIN

.Ldgemm_kernel_L4_M4_20:

	mov	pB, origPB

	asr 	counterL , origK, #1		// L = K / 2
	cmp	counterL , #2			// is there at least 4 to do?
	blt	.Ldgemm_kernel_L4_M4_32

	KERNEL4x4_I				// do one in the K
	KERNEL4x4_M2				// do another in the K

	subs	counterL, counterL, #2
	ble	.Ldgemm_kernel_L4_M4_22a
	.align 5

.Ldgemm_kernel_L4_M4_22:

	KERNEL4x4_M1
	KERNEL4x4_M2

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L4_M4_22


.Ldgemm_kernel_L4_M4_22a:

	KERNEL4x4_M1
	KERNEL4x4_E

	b	 .Ldgemm_kernel_L4_M4_44

.Ldgemm_kernel_L4_M4_32:

	tst	counterL, #1
	ble	.Ldgemm_kernel_L4_M4_40

	KERNEL4x4_I

	KERNEL4x4_E

	b	.Ldgemm_kernel_L4_M4_44


.Ldgemm_kernel_L4_M4_40:

	INIT4x4

.Ldgemm_kernel_L4_M4_44:

	ands	counterL , origK, #1
	ble	.Ldgemm_kernel_L4_M4_100

.Ldgemm_kernel_L4_M4_46:

	KERNEL4x4_SUB

.Ldgemm_kernel_L4_M4_100:

	SAVE4x4

.Ldgemm_kernel_L4_M4_END:
	subs	counterI, counterI, #1
	bne	.Ldgemm_kernel_L4_M4_20

.Ldgemm_kernel_L4_M2_BEGIN:

	mov	counterI, origM
	tst	counterI , #3
	ble	.Ldgemm_kernel_L4_END

	tst	counterI, #2			// counterI = counterI / 2
	ble	.Ldgemm_kernel_L4_M1_BEGIN

.Ldgemm_kernel_L4_M2_20:

	INIT2x4

	mov	pB, origPB

	asr 	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Ldgemm_kernel_L4_M2_40

.Ldgemm_kernel_L4_M2_22:

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L4_M2_22


.Ldgemm_kernel_L4_M2_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Ldgemm_kernel_L4_M2_100

.Ldgemm_kernel_L4_M2_42:

	KERNEL2x4_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L4_M2_42

.Ldgemm_kernel_L4_M2_100:

	SAVE2x4

.Ldgemm_kernel_L4_M2_END:


.Ldgemm_kernel_L4_M1_BEGIN:

	tst	counterI, #1			// counterI = counterI % 2
	ble	.Ldgemm_kernel_L4_END

.Ldgemm_kernel_L4_M1_20:

	INIT1x4

	mov	pB, origPB

	asr 	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Ldgemm_kernel_L4_M1_40

.Ldgemm_kernel_L4_M1_22:
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L4_M1_22


.Ldgemm_kernel_L4_M1_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Ldgemm_kernel_L4_M1_100

.Ldgemm_kernel_L4_M1_42:

	KERNEL1x4_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L4_M1_42

.Ldgemm_kernel_L4_M1_100:

	SAVE1x4

.Ldgemm_kernel_L4_END:

	lsl	temp, origK, #5 
	add	origPB, origPB, temp		// B = B + K * 4 * 8

/******************************************************************************/

.Ldgemm_kernel_L2_BEGIN:   // less than 2 left in N direction

	mov	counterJ , origN
	tst	counterJ , #3
	ble	.Ldgemm_kernel_L999   // error, N was less than 4?

	tst	counterJ , #2
	ble	.Ldgemm_kernel_L1_BEGIN

	mov	pCRow0, pC			// pCRow0 = pC

	add	pC,pC,LDC, lsl #1

	mov	pA, origPA			// pA = A


.Ldgemm_kernel_L2_M4_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #2		// counterI = counterI / 4
	cmp	counterI,#0
	ble	.Ldgemm_kernel_L2_M2_BEGIN

.Ldgemm_kernel_L2_M4_20:

	INIT4x2

	mov	pB, origPB

	asr	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL,#0
	ble	.Ldgemm_kernel_L2_M4_40
	.align 5

.Ldgemm_kernel_L2_M4_22:
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L2_M4_22


.Ldgemm_kernel_L2_M4_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Ldgemm_kernel_L2_M4_100

.Ldgemm_kernel_L2_M4_42:

	KERNEL4x2_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L2_M4_42

.Ldgemm_kernel_L2_M4_100:

	SAVE4x2

.Ldgemm_kernel_L2_M4_END:

	subs	counterI, counterI, #1
	bgt	.Ldgemm_kernel_L2_M4_20


.Ldgemm_kernel_L2_M2_BEGIN:

	mov	counterI, origM
	tst	counterI , #3
	ble	.Ldgemm_kernel_L2_END

	tst	counterI, #2			// counterI = counterI / 2
	ble	.Ldgemm_kernel_L2_M1_BEGIN

.Ldgemm_kernel_L2_M2_20:

	INIT2x2

	mov	pB, origPB

	asr	counterL , origK, #3		// counterL = counterL / 8
        cmp	counterL,#0
	ble	.Ldgemm_kernel_L2_M2_40

.Ldgemm_kernel_L2_M2_22:

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L2_M2_22


.Ldgemm_kernel_L2_M2_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Ldgemm_kernel_L2_M2_100

.Ldgemm_kernel_L2_M2_42:

	KERNEL2x2_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L2_M2_42

.Ldgemm_kernel_L2_M2_100:

	SAVE2x2

.Ldgemm_kernel_L2_M2_END:


.Ldgemm_kernel_L2_M1_BEGIN:

	tst	counterI, #1			// counterI = counterI % 2
	ble	.Ldgemm_kernel_L2_END

.Ldgemm_kernel_L2_M1_20:

	INIT1x2

	mov	pB, origPB

	asr 	counterL , origK, #3		// counterL = counterL / 8
        cmp     counterL, #0
	ble	.Ldgemm_kernel_L2_M1_40

.Ldgemm_kernel_L2_M1_22:
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L2_M1_22


.Ldgemm_kernel_L2_M1_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Ldgemm_kernel_L2_M1_100

.Ldgemm_kernel_L2_M1_42:

	KERNEL1x2_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L2_M1_42

.Ldgemm_kernel_L2_M1_100:

	SAVE1x2

.Ldgemm_kernel_L2_END:
	add	origPB, origPB, origK, lsl #4	// B = B + K * 2 * 8

/******************************************************************************/

.Ldgemm_kernel_L1_BEGIN:

	mov	counterJ , origN
	tst	counterJ , #1
	ble	.Ldgemm_kernel_L999 // done


	mov	pCRow0, pC			// pCRow0 = C
	add	pC , pC , LDC			// Update pC to point to next

	mov	pA, origPA			// pA = A

.Ldgemm_kernel_L1_M4_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #2		// counterI = counterI / 4
	cmp	counterI, #0
	ble	.Ldgemm_kernel_L1_M2_BEGIN

.Ldgemm_kernel_L1_M4_20:

	INIT4x1

	mov	pB, origPB
	asr	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Ldgemm_kernel_L1_M4_40
	.align 5

.Ldgemm_kernel_L1_M4_22:
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L1_M4_22


.Ldgemm_kernel_L1_M4_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Ldgemm_kernel_L1_M4_100

.Ldgemm_kernel_L1_M4_42:

	KERNEL4x1_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L1_M4_42

.Ldgemm_kernel_L1_M4_100:

	SAVE4x1

.Ldgemm_kernel_L1_M4_END:

	subs	counterI, counterI, #1
	bgt	.Ldgemm_kernel_L1_M4_20


.Ldgemm_kernel_L1_M2_BEGIN:

	mov	counterI, origM
	tst	counterI , #3
	ble	.Ldgemm_kernel_L1_END

	tst	counterI, #2			// counterI = counterI / 2
	ble	.Ldgemm_kernel_L1_M1_BEGIN

.Ldgemm_kernel_L1_M2_20:

	INIT2x1

	mov	pB, origPB

	asr 	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Ldgemm_kernel_L1_M2_40

.Ldgemm_kernel_L1_M2_22:

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L1_M2_22


.Ldgemm_kernel_L1_M2_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Ldgemm_kernel_L1_M2_100

.Ldgemm_kernel_L1_M2_42:

	KERNEL2x1_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L1_M2_42

.Ldgemm_kernel_L1_M2_100:

	SAVE2x1

.Ldgemm_kernel_L1_M2_END:


.Ldgemm_kernel_L1_M1_BEGIN:

	tst	counterI, #1			// counterI = counterI % 2
	ble	.Ldgemm_kernel_L1_END

.Ldgemm_kernel_L1_M1_20:

	INIT1x1

	mov	pB, origPB

	asr 	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Ldgemm_kernel_L1_M1_40

.Ldgemm_kernel_L1_M1_22:
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L1_M1_22


.Ldgemm_kernel_L1_M1_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Ldgemm_kernel_L1_M1_100

.Ldgemm_kernel_L1_M1_42:

	KERNEL1x1_SUB

	subs	counterL, counterL, #1
	bgt	.Ldgemm_kernel_L1_M1_42

.Ldgemm_kernel_L1_M1_100:

	SAVE1x1


.Ldgemm_kernel_L1_END:


.Ldgemm_kernel_L999:
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

