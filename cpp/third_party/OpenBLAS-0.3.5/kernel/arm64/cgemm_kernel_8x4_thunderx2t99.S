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
#define pCRow3		x15
#define pA		x16
#define alphaR		w17
#define alphaI		w18

#define alpha0_R	s10
#define alphaV0_R	v10.s[0]
#define alpha0_I	s11
#define alphaV0_I	v11.s[0]

#define A_PRE_SIZE	2560
#define B_PRE_SIZE	448
#define C_PRE_SIZE	128

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define OP_rr		fmla
#define OP_ii		fmls
#define OP_ri		fmla
#define OP_ir		fmla
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define OP_rr		fmla
#define OP_ii		fmla
#define OP_ri		fmls
#define OP_ir		fmla
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define OP_rr		fmla
#define OP_ii		fmla
#define OP_ri		fmla
#define OP_ir		fmls
#elif defined(RR) || defined(RC) || defined(CR) || defined(CC)
#define OP_rr		fmla
#define OP_ii		fmls
#define OP_ri		fmls
#define OP_ir		fmls
#endif

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
// 15 pCRow3
// 16 pA
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

//v00 ALPHA_R -> pA0_00_R, pA0_01_R, pA0_02_R, pA0_03_R
//v01 ALPHA_I -> pA0_00_I, pA0_01_I, pA0_02_I, pA0_03_I
//v02 pA0_04_R, pA0_05_R, pA0_06_R, pA0_07_R
//v03 pA0_04_I, pA0_05_I, pA0_06_I, pA0_07_I
//v04 pA1_00_R, pA1_01_R, pA1_02_R, pA1_03_R
//v05 pA1_00_I, pA1_01_I, pA1_02_I, pA1_03_I
//v06 pA1_04_R, pA1_05_R, pA1_06_R, pA1_07_R
//v07 pA1_04_I, pA1_05_I, pA1_06_I, pA1_07_I
//v08 must save pB0_00_R, pB0_01_R
//v09 must save pB0_00_I, pB0_01_I
//v10 must save pB0_02_R, pB0_03_R --> ALPHA0_R
//v11 must save pB0_02_I, pB0_03_I --> ALPHA0_I
//v12 must save pB1_00_R, pB1_01_R
//v13 must save pB1_00_I, pB1_01_I
//v14 must save pB1_02_R, pB1_03_R
//v15 must save pB1_02_I, pB1_03_I
//v16 must save pC_00_R, pC_01_R, pC_02_R, pC_03_R
//v17 must save pC_00_I, pC_01_I, pC_02_I, pC_03_I
//v18 pC_04_R, pC_05_R, pC_06_R, pC_07_R
//v19 pC_04_I, pC_05_I, pC_06_I, pC_07_I
//v20 pC_08_R, pC_09_R, pC_10_R, pC_11_R
//v21 pC_08_I, pC_09_I, pC_10_I, pC_11_I
//v22 pC_12_R, pC_13_R, pC_14_R, pC_15_R
//v23 pC_12_I, pC_13_I, pC_14_I, pC_15_I
//v24 pC_16_R, pC_17_R, pC_18_R, pC_19_R
//v25 pC_16_I, pC_17_I, pC_18_I, pC_19_I
//v26 pC_20_R, pC_21_R, pC_22_R, pC_23_R
//v27 pC_20_I, pC_21_I, pC_22_I, pC_23_I
//v28 pC_24_R, pC_25_R, pC_26_R, pC_27_R
//v29 pC_24_I, pC_25_I, pC_26_I, pC_27_I
//v30 pC_28_R, pC_29_R, pC_30_R, pC_31_R
//v31 pC_28_I, pC_29_I, pC_30_I, pC_31_I


/*******************************************************************************
* Macro definitions
*******************************************************************************/

.macro INIT8x4
	fmov		s16, wzr
	fmov		s17, wzr
	fmov		s18, wzr
	fmov		s19, s16
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

.macro KERNEL8x4_I
	ldr	q8, [pB]
	add	pB, pB, #16

	ld2	{v0.4s, v1.4s}, [pA]
	add	pA, pA, #32
	ld2	{v2.4s, v3.4s}, [pA]
	add	pA, pA, #32

	fmul	v16.4s, v0.4s, v8.s[0]
	OP_ii	v16.4s, v1.4s, v8.s[1]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v17.16b, v17.16b, v17.16b
	fmls	v17.4s, v0.4s, v8.s[1]
#else
	fmul	v17.4s, v0.4s, v8.s[1]
#endif
	OP_ir	v17.4s, v1.4s, v8.s[0]

	ldr	q10, [pB]
	add	pB, pB, #16

	fmul	v18.4s, v2.4s, v8.s[0]
	OP_ii	v18.4s, v3.4s, v8.s[1]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v19.16b, v19.16b, v19.16b
	fmls	v19.4s, v2.4s, v8.s[1]
#else
	fmul	v19.4s, v2.4s, v8.s[1]
#endif
	OP_ir	v19.4s, v3.4s, v8.s[0]

	ldr	q12, [pB]
	add	pB, pB, #16

	fmul	v20.4s, v0.4s, v8.s[2]
	OP_ii	v20.4s, v1.4s, v8.s[3]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v21.16b, v21.16b, v21.16b
	fmls	v21.4s, v0.4s, v8.s[3]
#else
	fmul	v21.4s, v0.4s, v8.s[3]
#endif
	OP_ir	v21.4s, v1.4s, v8.s[2]

	ldr	q14, [pB]
	add	pB, pB, #16

	fmul	v22.4s, v2.4s, v8.s[2]
	OP_ii	v22.4s, v3.4s, v8.s[3]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v23.16b, v23.16b, v23.16b
	fmls	v23.4s, v2.4s, v8.s[3]
#else
	fmul	v23.4s, v2.4s, v8.s[3]
#endif
	OP_ir	v23.4s, v3.4s, v8.s[2]

	ld2	{v4.4s, v5.4s}, [pA]
	add	pA, pA, #32

	fmul	v24.4s, v0.4s, v10.s[0]
	OP_ii	v24.4s, v1.4s, v10.s[1]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v25.16b, v25.16b, v25.16b
	fmls	v25.4s, v0.4s, v10.s[1]
#else
	fmul	v25.4s, v0.4s, v10.s[1]
#endif
	OP_ir	v25.4s, v1.4s, v10.s[0]

	ld2	{v6.4s, v7.4s}, [pA]
	add	pA, pA, #32

	fmul	v26.4s, v2.4s, v10.s[0]
	OP_ii	v26.4s, v3.4s, v10.s[1]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v27.16b, v27.16b, v27.16b
	fmls	v27.4s, v2.4s, v10.s[1]
#else
	fmul	v27.4s, v2.4s, v10.s[1]
#endif
	OP_ir	v27.4s, v3.4s, v10.s[0]

	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE]

	fmul	v28.4s, v0.4s, v10.s[2]
	OP_ii	v28.4s, v1.4s, v10.s[3]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v29.16b, v29.16b, v29.16b
	fmls	v29.4s, v0.4s, v10.s[3]
#else
	fmul	v29.4s, v0.4s, v10.s[3]
#endif
	OP_ir	v29.4s, v1.4s, v10.s[2]

	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE+64]

	fmul	v30.4s, v2.4s, v10.s[2]
	OP_ii	v30.4s, v3.4s, v10.s[3]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v31.16b, v31.16b, v31.16b
	fmls	v31.4s, v2.4s, v10.s[3]
#else
	fmul	v31.4s, v2.4s, v10.s[3]
#endif
	OP_ir	v31.4s, v3.4s, v10.s[2]
.endm

.macro KERNEL8x4_M1
	OP_rr	v16.4s, v0.4s, v8.s[0]
	OP_ii	v16.4s, v1.4s, v8.s[1]
	OP_ri	v17.4s, v0.4s, v8.s[1]
	OP_ir	v17.4s, v1.4s, v8.s[0]

	ldr	q12, [pB]
	add	pB, pB, #16

	OP_rr	v18.4s, v2.4s, v8.s[0]
	OP_ii	v18.4s, v3.4s, v8.s[1]
	OP_ri	v19.4s, v2.4s, v8.s[1]
	OP_ir	v19.4s, v3.4s, v8.s[0]

	ld2	{v4.4s, v5.4s}, [pA]
	add	pA, pA, #32

	OP_rr	v20.4s, v0.4s, v8.s[2]
	OP_ii	v20.4s, v1.4s, v8.s[3]
	OP_ri	v21.4s, v0.4s, v8.s[3]
	OP_ir	v21.4s, v1.4s, v8.s[2]

	ld2	{v6.4s, v7.4s}, [pA]
	add	pA, pA, #32

	OP_rr	v22.4s, v2.4s, v8.s[2]
	OP_ii	v22.4s, v3.4s, v8.s[3]
	OP_ri	v23.4s, v2.4s, v8.s[3]
	OP_ir	v23.4s, v3.4s, v8.s[2]

	ldr	q14, [pB]
	add	pB, pB, #16

	OP_rr	v24.4s, v0.4s, v10.s[0]
	OP_ii	v24.4s, v1.4s, v10.s[1]
	OP_ri	v25.4s, v0.4s, v10.s[1]
	OP_ir	v25.4s, v1.4s, v10.s[0]

	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE]

	OP_rr	v26.4s, v2.4s, v10.s[0]
	OP_ii	v26.4s, v3.4s, v10.s[1]
	OP_ri	v27.4s, v2.4s, v10.s[1]
	OP_ir	v27.4s, v3.4s, v10.s[0]

	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE+64]

	OP_rr	v28.4s, v0.4s, v10.s[2]
	OP_ii	v28.4s, v1.4s, v10.s[3]
	OP_ri	v29.4s, v0.4s, v10.s[3]
	OP_ir	v29.4s, v1.4s, v10.s[2]

	OP_rr	v30.4s, v2.4s, v10.s[2]
	OP_ii	v30.4s, v3.4s, v10.s[3]
	OP_ri	v31.4s, v2.4s, v10.s[3]
	OP_ir	v31.4s, v3.4s, v10.s[2]
.endm

.macro KERNEL8x4_M2
	OP_rr	v16.4s, v4.4s, v12.s[0]
	OP_ii	v16.4s, v5.4s, v12.s[1]
	OP_ri	v17.4s, v4.4s, v12.s[1]
	OP_ir	v17.4s, v5.4s, v12.s[0]

	ldr	q8, [pB]
	add	pB, pB, #16

	OP_rr	v18.4s, v6.4s, v12.s[0]
	OP_ii	v18.4s, v7.4s, v12.s[1]
	OP_ri	v19.4s, v6.4s, v12.s[1]
	OP_ir	v19.4s, v7.4s, v12.s[0]

	ld2	{v0.4s, v1.4s}, [pA]
	add	pA, pA, #32

	OP_rr	v20.4s, v4.4s, v12.s[2]
	OP_ii	v20.4s, v5.4s, v12.s[3]
	OP_ri	v21.4s, v4.4s, v12.s[3]
	OP_ir	v21.4s, v5.4s, v12.s[2]

	ld2	{v2.4s, v3.4s}, [pA]
	add	pA, pA, #32

	OP_rr	v22.4s, v6.4s, v12.s[2]
	OP_ii	v22.4s, v7.4s, v12.s[3]
	OP_ri	v23.4s, v6.4s, v12.s[3]
	OP_ir	v23.4s, v7.4s, v12.s[2]

	ldr	q10, [pB]
	add	pB, pB, #16

	OP_rr	v24.4s, v4.4s, v14.s[0]
	OP_ii	v24.4s, v5.4s, v14.s[1]
	OP_ri	v25.4s, v4.4s, v14.s[1]
	OP_ir	v25.4s, v5.4s, v14.s[0]

	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]

	OP_rr	v26.4s, v6.4s, v14.s[0]
	OP_ii	v26.4s, v7.4s, v14.s[1]
	OP_ri	v27.4s, v6.4s, v14.s[1]
	OP_ir	v27.4s, v7.4s, v14.s[0]

	OP_rr	v28.4s, v4.4s, v14.s[2]
	OP_ii	v28.4s, v5.4s, v14.s[3]
	OP_ri	v29.4s, v4.4s, v14.s[3]
	OP_ir	v29.4s, v5.4s, v14.s[2]

	OP_rr	v30.4s, v6.4s, v14.s[2]
	OP_ii	v30.4s, v7.4s, v14.s[3]
	OP_ri	v31.4s, v6.4s, v14.s[3]
	OP_ir	v31.4s, v7.4s, v14.s[2]
.endm

.macro KERNEL8x4_E
	OP_rr	v16.4s, v4.4s, v12.s[0]
	OP_ii	v16.4s, v5.4s, v12.s[1]
	OP_ri	v17.4s, v4.4s, v12.s[1]
	OP_ir	v17.4s, v5.4s, v12.s[0]

	OP_rr	v18.4s, v6.4s, v12.s[0]
	OP_ii	v18.4s, v7.4s, v12.s[1]
	OP_ri	v19.4s, v6.4s, v12.s[1]
	OP_ir	v19.4s, v7.4s, v12.s[0]

	OP_rr	v20.4s, v4.4s, v12.s[2]
	OP_ii	v20.4s, v5.4s, v12.s[3]
	OP_ri	v21.4s, v4.4s, v12.s[3]
	OP_ir	v21.4s, v5.4s, v12.s[2]

	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]

	OP_rr	v22.4s, v6.4s, v12.s[2]
	OP_ii	v22.4s, v7.4s, v12.s[3]
	OP_ri	v23.4s, v6.4s, v12.s[3]
	OP_ir	v23.4s, v7.4s, v12.s[2]

	OP_rr	v24.4s, v4.4s, v14.s[0]
	OP_ii	v24.4s, v5.4s, v14.s[1]
	OP_ri	v25.4s, v4.4s, v14.s[1]
	OP_ir	v25.4s, v5.4s, v14.s[0]

	OP_rr	v26.4s, v6.4s, v14.s[0]
	OP_ii	v26.4s, v7.4s, v14.s[1]
	OP_ri	v27.4s, v6.4s, v14.s[1]
	OP_ir	v27.4s, v7.4s, v14.s[0]

	OP_rr	v28.4s, v4.4s, v14.s[2]
	OP_ii	v28.4s, v5.4s, v14.s[3]
	OP_ri	v29.4s, v4.4s, v14.s[3]
	OP_ir	v29.4s, v5.4s, v14.s[2]

	OP_rr	v30.4s, v6.4s, v14.s[2]
	OP_ii	v30.4s, v7.4s, v14.s[3]
	OP_ri	v31.4s, v6.4s, v14.s[3]
	OP_ir	v31.4s, v7.4s, v14.s[2]
.endm

.macro KERNEL8x4_SUB
	ldr	q8, [pB]
	add	pB, pB, #16

	ld2	{v0.4s, v1.4s}, [pA]
	add	pA, pA, #32

	OP_rr	v16.4s, v0.4s, v8.s[0]
	OP_ii	v16.4s, v1.4s, v8.s[1]
	OP_ri	v17.4s, v0.4s, v8.s[1]
	OP_ir	v17.4s, v1.4s, v8.s[0]

	ld2	{v2.4s, v3.4s}, [pA]
	add	pA, pA, #32

	OP_rr	v20.4s, v0.4s, v8.s[2]
	OP_ii	v20.4s, v1.4s, v8.s[3]
	OP_ri	v21.4s, v0.4s, v8.s[3]
	OP_ir	v21.4s, v1.4s, v8.s[2]

	ldr	q10, [pB]
	add	pB, pB, #16

	OP_rr	v18.4s, v2.4s, v8.s[0]
	OP_ii	v18.4s, v3.4s, v8.s[1]
	OP_ri	v19.4s, v2.4s, v8.s[1]
	OP_ir	v19.4s, v3.4s, v8.s[0]

	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE]

	OP_rr	v22.4s, v2.4s, v8.s[2]
	OP_ii	v22.4s, v3.4s, v8.s[3]
	OP_ri	v23.4s, v2.4s, v8.s[3]
	OP_ir	v23.4s, v3.4s, v8.s[2]

	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE+64]

	OP_rr	v24.4s, v0.4s, v10.s[0]
	OP_ii	v24.4s, v1.4s, v10.s[1]
	OP_ri	v25.4s, v0.4s, v10.s[1]
	OP_ir	v25.4s, v1.4s, v10.s[0]

	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]

	OP_rr	v26.4s, v2.4s, v10.s[0]
	OP_ii	v26.4s, v3.4s, v10.s[1]
	OP_ri	v27.4s, v2.4s, v10.s[1]
	OP_ir	v27.4s, v3.4s, v10.s[0]

	OP_rr	v28.4s, v0.4s, v10.s[2]
	OP_ii	v28.4s, v1.4s, v10.s[3]
	OP_ri	v29.4s, v0.4s, v10.s[3]
	OP_ir	v29.4s, v1.4s, v10.s[2]

	OP_rr	v30.4s, v2.4s, v10.s[2]
	OP_ii	v30.4s, v3.4s, v10.s[3]
	OP_ri	v31.4s, v2.4s, v10.s[3]
	OP_ir	v31.4s, v3.4s, v10.s[2]
.endm

.macro SAVE8x4
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	prfm	PLDL2KEEP, [pCRow0, #C_PRE_SIZE]

	ld2	{v0.4s, v1.4s}, [pCRow0]
	fmla	v0.4s, v16.4s, alphaV0_R
	fmls	v0.4s, v17.4s, alphaV0_I
	fmla	v1.4s, v16.4s, alphaV0_I
	fmla	v1.4s, v17.4s, alphaV0_R
	st2 	{v0.4s, v1.4s}, [pCRow0]

	add	pCRow0, pCRow0, #32

	ld2	{v2.4s, v3.4s}, [pCRow0]
	fmla	v2.4s, v18.4s, alphaV0_R
	fmls	v2.4s, v19.4s, alphaV0_I
	fmla	v3.4s, v18.4s, alphaV0_I
	fmla	v3.4s, v19.4s, alphaV0_R
	st2 	{v2.4s, v3.4s}, [pCRow0]

	add	pCRow0, pCRow0, #32
	prfm	PLDL2KEEP, [pCRow1, #C_PRE_SIZE]

	ld2	{v4.4s, v5.4s}, [pCRow1]
	fmla	v4.4s, v20.4s, alphaV0_R
	fmls	v4.4s, v21.4s, alphaV0_I
	fmla	v5.4s, v20.4s, alphaV0_I
	fmla	v5.4s, v21.4s, alphaV0_R
	st2 	{v4.4s, v5.4s}, [pCRow1]

	add	pCRow1, pCRow1, #32

	ld2	{v6.4s, v7.4s}, [pCRow1]
	fmla	v6.4s, v22.4s, alphaV0_R
	fmls	v6.4s, v23.4s, alphaV0_I
	fmla	v7.4s, v22.4s, alphaV0_I
	fmla	v7.4s, v23.4s, alphaV0_R
	st2 	{v6.4s, v7.4s}, [pCRow1]

	add	pCRow1, pCRow1, #32
	prfm	PLDL2KEEP, [pCRow2, #C_PRE_SIZE]

	ld2	{v0.4s, v1.4s}, [pCRow2]
	fmla	v0.4s, v24.4s, alphaV0_R
	fmls	v0.4s, v25.4s, alphaV0_I
	fmla	v1.4s, v24.4s, alphaV0_I
	fmla	v1.4s, v25.4s, alphaV0_R
	st2 	{v0.4s, v1.4s}, [pCRow2]

	add	pCRow2, pCRow2, #32

	ld2	{v2.4s, v3.4s}, [pCRow2]
	fmla	v2.4s, v26.4s, alphaV0_R
	fmls	v2.4s, v27.4s, alphaV0_I
	fmla	v3.4s, v26.4s, alphaV0_I
	fmla	v3.4s, v27.4s, alphaV0_R
	st2 	{v2.4s, v3.4s}, [pCRow2]

	add	pCRow2, pCRow2, #32
	prfm	PLDL2KEEP, [pCRow3, #C_PRE_SIZE]

	ld2	{v4.4s, v5.4s}, [pCRow3]
	fmla	v4.4s, v28.4s, alphaV0_R
	fmls	v4.4s, v29.4s, alphaV0_I
	fmla	v5.4s, v28.4s, alphaV0_I
	fmla	v5.4s, v29.4s, alphaV0_R
	st2 	{v4.4s, v5.4s}, [pCRow3]

	add	pCRow3, pCRow3, #32

	ld2	{v6.4s, v7.4s}, [pCRow3]
	fmla	v6.4s, v30.4s, alphaV0_R
	fmls	v6.4s, v31.4s, alphaV0_I
	fmla	v7.4s, v30.4s, alphaV0_I
	fmla	v7.4s, v31.4s, alphaV0_R
	st2 	{v6.4s, v7.4s}, [pCRow3]

	add	pCRow3, pCRow3, #32
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

.macro KERNEL4x4_I
	ld2	{v8.4s, v9.4s}, [pB]
	add	pB, pB, #32
	ld2	{v0.4s, v1.4s}, [pA]
	add	pA, pA, #32

	fmul	v16.4s, v0.4s, v8.s[0]
	OP_ii	v16.4s, v1.4s, v9.s[0]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v17.16b, v17.16b, v17.16b
	fmls	v17.4s, v0.4s, v9.s[0]
#else
	fmul	v17.4s, v0.4s, v9.s[0]
#endif
	OP_ir	v17.4s, v1.4s, v8.s[0]

	fmul	v20.4s, v0.4s, v8.s[1]
	OP_ii	v20.4s, v1.4s, v9.s[1]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v21.16b, v21.16b, v21.16b
	fmls	v21.4s, v0.4s, v9.s[1]
#else
	fmul	v21.4s, v0.4s, v9.s[1]
#endif
	OP_ir	v21.4s, v1.4s, v8.s[1]

	fmul	v24.4s, v0.4s, v8.s[2]
	OP_ii	v24.4s, v1.4s, v9.s[2]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v25.16b, v25.16b, v25.16b
	fmls	v25.4s, v0.4s, v9.s[2]
#else
	fmul	v25.4s, v0.4s, v9.s[2]
#endif
	OP_ir	v25.4s, v1.4s, v8.s[2]

	fmul	v28.4s, v0.4s, v8.s[3]
	OP_ii	v28.4s, v1.4s, v9.s[3]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v29.16b, v29.16b, v29.16b
	fmls	v29.4s, v0.4s, v9.s[3]
#else
	fmul	v29.4s, v0.4s, v9.s[3]
#endif
	OP_ir	v29.4s, v1.4s, v8.s[3]

	ld2	{v12.4s, v13.4s}, [pB]
	add	pB, pB, #32
	ld2	{v4.4s, v5.4s}, [pA]
	add	pA, pA, #32
.endm

.macro KERNEL4x4_M1
	OP_rr	v16.4s, v0.4s, v8.s[0]
	OP_ii	v16.4s, v1.4s, v9.s[0]
	OP_ri	v17.4s, v0.4s, v9.s[0]
	OP_ir	v17.4s, v1.4s, v8.s[0]

	ld2	{v12.4s, v13.4s}, [pB]		// For next round
	add	pB, pB, #32

	OP_rr	v20.4s, v0.4s, v8.s[1]
	OP_ii	v20.4s, v1.4s, v9.s[1]
	OP_ri	v21.4s, v0.4s, v9.s[1]
	OP_ir	v21.4s, v1.4s, v8.s[1]

	ld2	{v4.4s, v5.4s}, [pA]		// For next round
	add	pA, pA, #32

	OP_rr	v24.4s, v0.4s, v8.s[2]
	OP_ii	v24.4s, v1.4s, v9.s[2]
	OP_ri	v25.4s, v0.4s, v9.s[2]
	OP_ir	v25.4s, v1.4s, v8.s[2]

	prfm	PLDL1KEEP, [pA, #512]

	OP_rr	v28.4s, v0.4s, v8.s[3]
	OP_ii	v28.4s, v1.4s, v9.s[3]
	OP_ri	v29.4s, v0.4s, v9.s[3]
	OP_ir	v29.4s, v1.4s, v8.s[3]
.endm

.macro KERNEL4x4_M2
	OP_rr	v16.4s, v4.4s, v12.s[0]
	OP_ii	v16.4s, v5.4s, v13.s[0]
	OP_ri	v17.4s, v4.4s, v13.s[0]
	OP_ir	v17.4s, v5.4s, v12.s[0]

	ld2	{v8.4s, v9.4s}, [pB]		// For next round
	add	pB, pB, #32

	OP_rr	v20.4s, v4.4s, v12.s[1]
	OP_ii	v20.4s, v5.4s, v13.s[1]
	OP_ri	v21.4s, v4.4s, v13.s[1]
	OP_ir	v21.4s, v5.4s, v12.s[1]

	ld2	{v0.4s, v1.4s}, [pA]		// For next round
	add	pA, pA, #32

	OP_rr	v24.4s, v4.4s, v12.s[2]
	OP_ii	v24.4s, v5.4s, v13.s[2]
	OP_ri	v25.4s, v4.4s, v13.s[2]
	OP_ir	v25.4s, v5.4s, v12.s[2]

	prfm	PLDL1KEEP, [pB, #512]

	OP_rr	v28.4s, v4.4s, v12.s[3]
	OP_ii	v28.4s, v5.4s, v13.s[3]
	OP_ri	v29.4s, v4.4s, v13.s[3]
	OP_ir	v29.4s, v5.4s, v12.s[3]
.endm

.macro KERNEL4x4_E
	OP_rr	v16.4s, v4.4s, v12.s[0]
	OP_ii	v16.4s, v5.4s, v13.s[0]
	OP_ri	v17.4s, v4.4s, v13.s[0]
	OP_ir	v17.4s, v5.4s, v12.s[0]

	OP_rr	v20.4s, v4.4s, v12.s[1]
	OP_ii	v20.4s, v5.4s, v13.s[1]
	OP_ri	v21.4s, v4.4s, v13.s[1]
	OP_ir	v21.4s, v5.4s, v12.s[1]

	OP_rr	v24.4s, v4.4s, v12.s[2]
	OP_ii	v24.4s, v5.4s, v13.s[2]
	OP_ri	v25.4s, v4.4s, v13.s[2]
	OP_ir	v25.4s, v5.4s, v12.s[2]

	OP_rr	v28.4s, v4.4s, v12.s[3]
	OP_ii	v28.4s, v5.4s, v13.s[3]
	OP_ri	v29.4s, v4.4s, v13.s[3]
	OP_ir	v29.4s, v5.4s, v12.s[3]
.endm

.macro KERNEL4x4_SUB
	ld2	{v8.4s, v9.4s}, [pB]
	add	pB, pB, #32
	ld2	{v0.4s, v1.4s}, [pA]
	add	pA, pA, #32

	OP_rr	v16.4s, v0.4s, v8.s[0]
	OP_ii	v16.4s, v1.4s, v9.s[0]
	OP_ri	v17.4s, v0.4s, v9.s[0]
	OP_ir	v17.4s, v1.4s, v8.s[0]

	OP_rr	v20.4s, v0.4s, v8.s[1]
	OP_ii	v20.4s, v1.4s, v9.s[1]
	OP_ri	v21.4s, v0.4s, v9.s[1]
	OP_ir	v21.4s, v1.4s, v8.s[1]

	OP_rr	v24.4s, v0.4s, v8.s[2]
	OP_ii	v24.4s, v1.4s, v9.s[2]
	OP_ri	v25.4s, v0.4s, v9.s[2]
	OP_ir	v25.4s, v1.4s, v8.s[2]

	OP_rr	v28.4s, v0.4s, v8.s[3]
	OP_ii	v28.4s, v1.4s, v9.s[3]
	OP_ri	v29.4s, v0.4s, v9.s[3]
	OP_ir	v29.4s, v1.4s, v8.s[3]
.endm

.macro SAVE4x4
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	mov	pCRow1, pCRow0

	ld2	{v0.4s, v1.4s}, [pCRow1]
	fmla	v0.4s, v16.4s, alphaV0_R
	fmls	v0.4s, v17.4s, alphaV0_I
	fmla	v1.4s, v16.4s, alphaV0_I
	fmla	v1.4s, v17.4s, alphaV0_R
	st2 	{v0.4s, v1.4s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v4.4s, v5.4s}, [pCRow1]
	fmla	v4.4s, v20.4s, alphaV0_R
	fmls	v4.4s, v21.4s, alphaV0_I
	fmla	v5.4s, v20.4s, alphaV0_I
	fmla	v5.4s, v21.4s, alphaV0_R
	st2 	{v4.4s, v5.4s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v0.4s, v1.4s}, [pCRow1]
	fmla	v0.4s, v24.4s, alphaV0_R
	fmls	v0.4s, v25.4s, alphaV0_I
	fmla	v1.4s, v24.4s, alphaV0_I
	fmla	v1.4s, v25.4s, alphaV0_R
	st2 	{v0.4s, v1.4s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v4.4s, v5.4s}, [pCRow1]
	fmla	v4.4s, v28.4s, alphaV0_R
	fmls	v4.4s, v29.4s, alphaV0_I
	fmla	v5.4s, v28.4s, alphaV0_I
	fmla	v5.4s, v29.4s, alphaV0_R
	st2 	{v4.4s, v5.4s}, [pCRow1]

	add	pCRow0, pCRow0, #32
.endm

/******************************************************************************/

.macro INIT2x4
	fmov	s16, wzr
	fmov	s17, wzr
	fmov	s20, s16
	fmov	s21, s17
	fmov	s24, s16
	fmov	s25, s17
	fmov	s28, s16
	fmov	s29, s17
.endm

.macro KERNEL2x4_SUB
	ld2	{v8.4s, v9.4s}, [pB]
	add	pB, pB, #32
	ld2	{v0.2s, v1.2s}, [pA]
	add	pA, pA, #16

	OP_rr	v16.2s, v0.2s, v8.s[0]
	OP_ii	v16.2s, v1.2s, v9.s[0]
	OP_ri	v17.2s, v0.2s, v9.s[0]
	OP_ir	v17.2s, v1.2s, v8.s[0]

	OP_rr	v20.2s, v0.2s, v8.s[1]
	OP_ii	v20.2s, v1.2s, v9.s[1]
	OP_ri	v21.2s, v0.2s, v9.s[1]
	OP_ir	v21.2s, v1.2s, v8.s[1]

	OP_rr	v24.2s, v0.2s, v8.s[2]
	OP_ii	v24.2s, v1.2s, v9.s[2]
	OP_ri	v25.2s, v0.2s, v9.s[2]
	OP_ir	v25.2s, v1.2s, v8.s[2]

	OP_rr	v28.2s, v0.2s, v8.s[3]
	OP_ii	v28.2s, v1.2s, v9.s[3]
	OP_ri	v29.2s, v0.2s, v9.s[3]
	OP_ir	v29.2s, v1.2s, v8.s[3]
.endm

.macro SAVE2x4
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	mov	pCRow1, pCRow0

	ld2	{v0.2s, v1.2s}, [pCRow1]
	fmla	v0.2s, v16.2s, alphaV0_R
	fmls	v0.2s, v17.2s, alphaV0_I
	fmla	v1.2s, v16.2s, alphaV0_I
	fmla	v1.2s, v17.2s, alphaV0_R
	st2 	{v0.2s, v1.2s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v4.2s, v5.2s}, [pCRow1]
	fmla	v4.2s, v20.2s, alphaV0_R
	fmls	v4.2s, v21.2s, alphaV0_I
	fmla	v5.2s, v20.2s, alphaV0_I
	fmla	v5.2s, v21.2s, alphaV0_R
	st2 	{v4.2s, v5.2s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v0.2s, v1.2s}, [pCRow1]
	fmla	v0.2s, v24.2s, alphaV0_R
	fmls	v0.2s, v25.2s, alphaV0_I
	fmla	v1.2s, v24.2s, alphaV0_I
	fmla	v1.2s, v25.2s, alphaV0_R
	st2 	{v0.2s, v1.2s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v4.2s, v5.2s}, [pCRow1]
	fmla	v4.2s, v28.2s, alphaV0_R
	fmls	v4.2s, v29.2s, alphaV0_I
	fmla	v5.2s, v28.2s, alphaV0_I
	fmla	v5.2s, v29.2s, alphaV0_R
	st2 	{v4.2s, v5.2s}, [pCRow1]

	add	pCRow0, pCRow0, #16
.endm

/******************************************************************************/

.macro INIT1x4
	fmov	s16, wzr
	fmov	s17, wzr
	fmov	s20, s16
	fmov	s21, s17
	fmov	s24, s16
	fmov	s25, s17
	fmov	s28, s16
	fmov	s29, s17
.endm

.macro KERNEL1x4_SUB
	ld2	{v8.4s, v9.4s}, [pB]
	add	pB, pB, #32
	ld2	{v0.s, v1.s}[0], [pA]
	add	pA, pA, #8

	OP_rr	s16, s0, v8.s[0]
	OP_ii	s16, s1, v9.s[0]
	OP_ri	s17, s0, v9.s[0]
	OP_ir	s17, s1, v8.s[0]

	OP_rr	s20, s0, v8.s[1]
	OP_ii	s20, s1, v9.s[1]
	OP_ri	s21, s0, v9.s[1]
	OP_ir	s21, s1, v8.s[1]

	OP_rr	s24, s0, v8.s[2]
	OP_ii	s24, s1, v9.s[2]
	OP_ri	s25, s0, v9.s[2]
	OP_ir	s25, s1, v8.s[2]

	OP_rr	s28, s0, v8.s[3]
	OP_ii	s28, s1, v9.s[3]
	OP_ri	s29, s0, v9.s[3]
	OP_ir	s29, s1, v8.s[3]
.endm

.macro SAVE1x4
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	mov	pCRow1, pCRow0

	ld2	{v0.s, v1.s}[0], [pCRow1]
	fmla	s0, s16, alphaV0_R
	fmls	s0, s17, alphaV0_I
	fmla	s1, s16, alphaV0_I
	fmla	s1, s17, alphaV0_R
	st2	{v0.s, v1.s}[0], [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v4.s, v5.s}[0], [pCRow1]
	fmla	s4, s20, alphaV0_R
	fmls	s4, s21, alphaV0_I
	fmla	s5, s20, alphaV0_I
	fmla	s5, s21, alphaV0_R
	st2 	{v4.s, v5.s}[0], [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v0.s, v1.s}[0], [pCRow1]
	fmla	s0, s24, alphaV0_R
	fmls	s0, s25, alphaV0_I
	fmla	s1, s24, alphaV0_I
	fmla	s1, s25, alphaV0_R
	st2	{v0.s, v1.s}[0], [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v4.s, v5.s}[0], [pCRow1]
	fmla	s4, s28, alphaV0_R
	fmls	s4, s29, alphaV0_I
	fmla	s5, s28, alphaV0_I
	fmla	s5, s29, alphaV0_R
	st2	{v4.s, v5.s}[0], [pCRow1]

	add	pCRow0, pCRow0, #8
.endm

/******************************************************************************/

.macro INIT8x2
	fmov		s16, wzr
	fmov		s17, wzr
	fmov		s18, wzr
	fmov		s19, s16
	fmov		s20, wzr
	fmov		s21, s16
	fmov		s22, s17
	fmov		s23, s18
.endm

.macro KERNEL8x2_SUB
	ld2	{v8.2s, v9.2s}, [pB]
	add	pB, pB, #16
	ld2	{v0.4s, v1.4s}, [pA]
	add	pA, pA, #32
	ld2	{v2.4s, v3.4s}, [pA]
	add	pA, pA, #32

	OP_rr	v16.4s, v0.4s, v8.s[0]
	OP_ii	v16.4s, v1.4s, v9.s[0]
	OP_ri	v17.4s, v0.4s, v9.s[0]
	OP_ir	v17.4s, v1.4s, v8.s[0]

	OP_rr	v18.4s, v2.4s, v8.s[0]
	OP_ii	v18.4s, v3.4s, v9.s[0]
	OP_ri	v19.4s, v2.4s, v9.s[0]
	OP_ir	v19.4s, v3.4s, v8.s[0]

	OP_rr	v20.4s, v0.4s, v8.s[1]
	OP_ii	v20.4s, v1.4s, v9.s[1]
	OP_ri	v21.4s, v0.4s, v9.s[1]
	OP_ir	v21.4s, v1.4s, v8.s[1]

	OP_rr	v22.4s, v2.4s, v8.s[1]
	OP_ii	v22.4s, v3.4s, v9.s[1]
	OP_ri	v23.4s, v2.4s, v9.s[1]
	OP_ir	v23.4s, v3.4s, v8.s[1]
.endm

.macro SAVE8x2
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	mov	pCRow1, pCRow0

	ld2	{v0.4s, v1.4s}, [pCRow1]
	fmla	v0.4s, v16.4s, alphaV0_R
	fmls	v0.4s, v17.4s, alphaV0_I
	fmla	v1.4s, v16.4s, alphaV0_I
	fmla	v1.4s, v17.4s, alphaV0_R
	st2 	{v0.4s, v1.4s}, [pCRow1]

	add	pCRow2, pCRow1, #32

	ld2	{v2.4s, v3.4s}, [pCRow2]
	fmla	v2.4s, v18.4s, alphaV0_R
	fmls	v2.4s, v19.4s, alphaV0_I
	fmla	v3.4s, v18.4s, alphaV0_I
	fmla	v3.4s, v19.4s, alphaV0_R
	st2 	{v2.4s, v3.4s}, [pCRow2]

	add	pCRow1, pCRow1, LDC

	ld2	{v4.4s, v5.4s}, [pCRow1]
	fmla	v4.4s, v20.4s, alphaV0_R
	fmls	v4.4s, v21.4s, alphaV0_I
	fmla	v5.4s, v20.4s, alphaV0_I
	fmla	v5.4s, v21.4s, alphaV0_R
	st2 	{v4.4s, v5.4s}, [pCRow1]

	add	pCRow2, pCRow1, #32

	ld2	{v6.4s, v7.4s}, [pCRow2]
	fmla	v6.4s, v22.4s, alphaV0_R
	fmls	v6.4s, v23.4s, alphaV0_I
	fmla	v7.4s, v22.4s, alphaV0_I
	fmla	v7.4s, v23.4s, alphaV0_R
	st2 	{v6.4s, v7.4s}, [pCRow2]

	add	pCRow0, pCRow0, #64
.endm

/******************************************************************************/

.macro INIT4x2
	fmov	s16, wzr
	fmov	s17, wzr
	fmov	s20, s16
	fmov	s21, s17
.endm

.macro KERNEL4x2_SUB
	ld2	{v8.2s, v9.2s}, [pB]
	add	pB, pB, #16
	ld2	{v0.4s, v1.4s}, [pA]
	add	pA, pA, #32

	OP_rr	v16.4s, v0.4s, v8.s[0]
	OP_ii	v16.4s, v1.4s, v9.s[0]
	OP_ri	v17.4s, v0.4s, v9.s[0]
	OP_ir	v17.4s, v1.4s, v8.s[0]

	OP_rr	v20.4s, v0.4s, v8.s[1]
	OP_ii	v20.4s, v1.4s, v9.s[1]
	OP_ri	v21.4s, v0.4s, v9.s[1]
	OP_ir	v21.4s, v1.4s, v8.s[1]
.endm

.macro SAVE4x2
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	mov	pCRow1, pCRow0

	ld2	{v0.4s, v1.4s}, [pCRow1]
	fmla	v0.4s, v16.4s, alphaV0_R
	fmls	v0.4s, v17.4s, alphaV0_I
	fmla	v1.4s, v16.4s, alphaV0_I
	fmla	v1.4s, v17.4s, alphaV0_R
	st2 	{v0.4s, v1.4s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v4.4s, v5.4s}, [pCRow1]
	fmla	v4.4s, v20.4s, alphaV0_R
	fmls	v4.4s, v21.4s, alphaV0_I
	fmla	v5.4s, v20.4s, alphaV0_I
	fmla	v5.4s, v21.4s, alphaV0_R
	st2 	{v4.4s, v5.4s}, [pCRow1]

	add	pCRow0, pCRow0, #32
.endm

/******************************************************************************/

.macro INIT2x2
	fmov		s16, wzr
	fmov		s17, wzr
	fmov		s20, s16
	fmov		s21, s17
.endm

.macro KERNEL2x2_SUB
	ld2	{v8.2s, v9.2s}, [pB]
	add	pB, pB, #16
	ld2	{v0.2s, v1.2s}, [pA]
	add	pA, pA, #16

	OP_rr	v16.2s, v0.2s, v8.s[0]
	OP_ii	v16.2s, v1.2s, v9.s[0]
	OP_ri	v17.2s, v0.2s, v9.s[0]
	OP_ir	v17.2s, v1.2s, v8.s[0]

	OP_rr	v20.2s, v0.2s, v8.s[1]
	OP_ii	v20.2s, v1.2s, v9.s[1]
	OP_ri	v21.2s, v0.2s, v9.s[1]
	OP_ir	v21.2s, v1.2s, v8.s[1]
.endm

.macro SAVE2x2
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	mov	pCRow1, pCRow0

	ld2	{v0.2s, v1.2s}, [pCRow1]
	fmla	v0.2s, v16.2s, alphaV0_R
	fmls	v0.2s, v17.2s, alphaV0_I
	fmla	v1.2s, v16.2s, alphaV0_I
	fmla	v1.2s, v17.2s, alphaV0_R
	st2 	{v0.2s, v1.2s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v4.2s, v5.2s}, [pCRow1]
	fmla	v4.2s, v20.2s, alphaV0_R
	fmls	v4.2s, v21.2s, alphaV0_I
	fmla	v5.2s, v20.2s, alphaV0_I
	fmla	v5.2s, v21.2s, alphaV0_R
	st2 	{v4.2s, v5.2s}, [pCRow1]

	add	pCRow0, pCRow0, #16
.endm

/******************************************************************************/

.macro INIT1x2
	fmov		s16, wzr
	fmov		s17, wzr
	fmov		s20, wzr
	fmov		s21, wzr
.endm

.macro KERNEL1x2_SUB
	ld2	{v8.2s, v9.2s}, [pB]
	add	pB, pB, #16
	ld2	{v0.s, v1.s}[0], [pA]
	add	pA, pA, #8

	OP_rr	s16, s0, v8.s[0]
	OP_ii	s16, s1, v9.s[0]
	OP_ri	s17, s0, v9.s[0]
	OP_ir	s17, s1, v8.s[0]

	OP_rr	s20, s0, v8.s[1]
	OP_ii	s20, s1, v9.s[1]
	OP_ri	s21, s0, v9.s[1]
	OP_ir	s21, s1, v8.s[1]
.endm

.macro SAVE1x2
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	mov	pCRow1, pCRow0

	ld2	{v0.s, v1.s}[0], [pCRow1]
	fmla	s0, s16, alphaV0_R
	fmls	s0, s17, alphaV0_I
	fmla	s1, s16, alphaV0_I
	fmla	s1, s17, alphaV0_R
	st2	{v0.s, v1.s}[0], [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v4.s, v5.s}[0], [pCRow1]
	fmla	s4, s20, alphaV0_R
	fmls	s4, s21, alphaV0_I
	fmla	s5, s20, alphaV0_I
	fmla	s5, s21, alphaV0_R
	st2 	{v4.s, v5.s}[0], [pCRow1]

	add	pCRow0, pCRow0, #8
.endm

/******************************************************************************/

.macro INIT8x1
	fmov		s16, wzr
	fmov		s17, wzr
	fmov		s18, wzr
	fmov		s19, s16
.endm

.macro KERNEL8x1_SUB
	ld1	{v8.2s}, [pB]
	add	pB, pB, #8
	ld2	{v0.4s, v1.4s}, [pA]
	add	pA, pA, #32
	ld2	{v2.4s, v3.4s}, [pA]
	add	pA, pA, #32

	OP_rr	v16.4s, v0.4s, v8.s[0]
	OP_ii	v16.4s, v1.4s, v8.s[1]
	OP_ri	v17.4s, v0.4s, v8.s[1]
	OP_ir	v17.4s, v1.4s, v8.s[0]

	OP_rr	v18.4s, v2.4s, v8.s[0]
	OP_ii	v18.4s, v3.4s, v8.s[1]
	OP_ri	v19.4s, v2.4s, v8.s[1]
	OP_ir	v19.4s, v3.4s, v8.s[0]
.endm

.macro SAVE8x1
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	mov	pCRow1, pCRow0

	ld2	{v0.4s, v1.4s}, [pCRow1]
	fmla	v0.4s, v16.4s, alphaV0_R
	fmls	v0.4s, v17.4s, alphaV0_I
	fmla	v1.4s, v16.4s, alphaV0_I
	fmla	v1.4s, v17.4s, alphaV0_R
	st2 	{v0.4s, v1.4s}, [pCRow1]

	add	pCRow1, pCRow1, #32

	ld2	{v2.4s, v3.4s}, [pCRow1]
	fmla	v2.4s, v18.4s, alphaV0_R
	fmls	v2.4s, v19.4s, alphaV0_I
	fmla	v3.4s, v18.4s, alphaV0_I
	fmla	v3.4s, v19.4s, alphaV0_R
	st2 	{v2.4s, v3.4s}, [pCRow1]

	add	pCRow0, pCRow0, #64
.endm


/******************************************************************************/

.macro INIT4x1
	fmov	s16, wzr
	fmov	s17, s16
.endm

.macro KERNEL4x1_SUB
	ld2	{v8.s, v9.s}[0], [pB]
	add	pB, pB, #8
	ld2	{v0.4s, v1.4s}, [pA]
	add	pA, pA, #32

	OP_rr	v16.4s, v0.4s, v8.s[0]
	OP_ii	v16.4s, v1.4s, v9.s[0]
	OP_ri	v17.4s, v0.4s, v9.s[0]
	OP_ir	v17.4s, v1.4s, v8.s[0]
.endm

.macro SAVE4x1
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	mov	pCRow1, pCRow0

	ld2	{v0.4s, v1.4s}, [pCRow1]
	fmla	v0.4s, v16.4s, alphaV0_R
	fmls	v0.4s, v17.4s, alphaV0_I
	fmla	v1.4s, v16.4s, alphaV0_I
	fmla	v1.4s, v17.4s, alphaV0_R
	st2 	{v0.4s, v1.4s}, [pCRow1]

	add	pCRow0, pCRow0, #32
.endm

/******************************************************************************/

.macro INIT2x1
	fmov	s16, wzr
	fmov	s17, wzr
.endm

.macro KERNEL2x1_SUB
	ld2	{v8.s, v9.s}[0], [pB]
	add	pB, pB, #8
	ld2	{v0.2s, v1.2s}, [pA]
	add	pA, pA, #16

	OP_rr	v16.2s, v0.2s, v8.s[0]
	OP_ii	v16.2s, v1.2s, v9.s[0]
	OP_ri	v17.2s, v0.2s, v9.s[0]
	OP_ir	v17.2s, v1.2s, v8.s[0]
.endm

.macro SAVE2x1
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	mov	pCRow1, pCRow0

	ld2	{v0.2s, v1.2s}, [pCRow1]
	fmla	v0.2s, v16.2s, alphaV0_R
	fmls	v0.2s, v17.2s, alphaV0_I
	fmla	v1.2s, v16.2s, alphaV0_I
	fmla	v1.2s, v17.2s, alphaV0_R
	st2 	{v0.2s, v1.2s}, [pCRow1]

	add	pCRow0, pCRow0, #16

.endm

/******************************************************************************/

.macro INIT1x1
	fmov	s16, wzr
	fmov	s17, wzr
.endm

.macro KERNEL1x1_SUB
	ld2	{v8.s, v9.s}[0], [pB]
	add	pB, pB, #8
	ld2	{v0.s, v1.s}[0], [pA]
	add	pA, pA, #8

	OP_rr	s16, s0, v8.s[0]
	OP_ii	s16, s1, v9.s[0]
	OP_ri	s17, s0, v9.s[0]
	OP_ir	s17, s1, v8.s[0]
.endm

.macro SAVE1x1
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	mov	pCRow1, pCRow0

	ld2	{v0.s, v1.s}[0], [pCRow1]
	fmla	s0, s16, alphaV0_R
	fmls	s0, s17, alphaV0_I
	fmla	s1, s16, alphaV0_I
	fmla	s1, s17, alphaV0_R
	st2	{v0.s, v1.s}[0], [pCRow1]

	add	pCRow0, pCRow0, #8
.endm

.macro KERNEL8x4_M1_M2_x1
	KERNEL8x4_M1
	KERNEL8x4_M2
.endm

.macro KERNEL8x4_M1_M2_x2
	KERNEL8x4_M1_M2_x1
	KERNEL8x4_M1_M2_x1
.endm

.macro KERNEL8x4_M1_M2_x4
	KERNEL8x4_M1_M2_x2
	KERNEL8x4_M1_M2_x2
.endm

.macro KERNEL8x4_M1_M2_x8
	KERNEL8x4_M1_M2_x4
	KERNEL8x4_M1_M2_x4
.endm

.macro KERNEL8x4_M1_M2_x16
	KERNEL8x4_M1_M2_x8
	KERNEL8x4_M1_M2_x8
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

	prfm	PLDL1KEEP, [origPB]
	prfm	PLDL1KEEP, [origPA]

	fmov	alphaR, s0
	fmov	alphaI, s1

	lsl	LDC, LDC, #3			// ldc = ldc * 8

	mov	pB, origPB

	mov	counterJ, origN
	asr 	counterJ, counterJ, #2		// J = J / 4
	cmp 	counterJ, #0
	ble	.Lcgemm_kernel_L2_BEGIN

/******************************************************************************/

.Lcgemm_kernel_L4_BEGIN:
	mov	pCRow0, pC
	add	pCRow1, pCRow0, LDC
	add	pCRow2, pCRow1, LDC
	add	pCRow3, pCRow2, LDC

	add	pC, pCRow3, LDC

	mov	pA, origPA			// pA = start of A array

.Lcgemm_kernel_L4_M8_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #3		// counterI = counterI / 8
	cmp 	counterI, #0
	ble	.Lcgemm_kernel_L4_M4_BEGIN

	.align 5
.Lcgemm_kernel_L4_M8_20:

	mov	pB, origPB

	asr 	counterL , origK, #5		// origK / 32
	cmp	counterL , #2
	blt	.Lcgemm_kernel_L4_M8_32

	KERNEL8x4_I
	KERNEL8x4_M2
	KERNEL8x4_M1_M2_x1
	KERNEL8x4_M1_M2_x2
	KERNEL8x4_M1_M2_x4
	KERNEL8x4_M1_M2_x8

	subs	counterL, counterL, #2		// subtract 2
	ble	.Lcgemm_kernel_L4_M8_22a

	.align 5
.Lcgemm_kernel_L4_M8_22:

	KERNEL8x4_M1_M2_x16

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L4_M8_22

	.align 5
.Lcgemm_kernel_L4_M8_22a:

	KERNEL8x4_M1_M2_x8
	KERNEL8x4_M1_M2_x4
	KERNEL8x4_M1_M2_x2
	KERNEL8x4_M1_M2_x1
	KERNEL8x4_M1
	KERNEL8x4_E

	b	 .Lcgemm_kernel_L4_M8_44

	.align 5
.Lcgemm_kernel_L4_M8_32:

	tst	counterL, #1
	ble	.Lcgemm_kernel_L4_M8_40

	KERNEL8x4_I
	KERNEL8x4_M2
	KERNEL8x4_M1_M2_x8
	KERNEL8x4_M1_M2_x4
	KERNEL8x4_M1_M2_x2
	KERNEL8x4_M1
	KERNEL8x4_E

	b	.Lcgemm_kernel_L4_M8_44

.Lcgemm_kernel_L4_M8_40:

	INIT8x4

.Lcgemm_kernel_L4_M8_44:

	ands	counterL , origK, #31
	ble	.Lcgemm_kernel_L4_M8_100

	.align 5
.Lcgemm_kernel_L4_M8_46:

	KERNEL8x4_SUB

	subs	counterL, counterL, #1
	bne	.Lcgemm_kernel_L4_M8_46

.Lcgemm_kernel_L4_M8_100:
	prfm	PLDL1KEEP, [pA]
	prfm	PLDL1KEEP, [pA, #64]
	prfm	PLDL1KEEP, [origPB]

	SAVE8x4

.Lcgemm_kernel_L4_M8_END:
	subs	counterI, counterI, #1
	bne	.Lcgemm_kernel_L4_M8_20

.Lcgemm_kernel_L4_M4_BEGIN:

	mov	counterI, origM
	tst	counterI , #7
	ble	.Lcgemm_kernel_L4_END

	tst	counterI, #4
	ble	.Lcgemm_kernel_L4_M2_BEGIN


.Lcgemm_kernel_L4_M4_20:

	mov	pB, origPB
	
	asr 	counterL , origK, #1		// L = K / 2
	cmp	counterL , #2			// is there at least 4 to do?
	blt	.Lcgemm_kernel_L4_M4_32

	KERNEL4x4_I				// do one in the K
	KERNEL4x4_M2				// do another in the K

	subs	counterL, counterL, #2
	ble	.Lcgemm_kernel_L4_M4_22a
	.align 5


.Lcgemm_kernel_L4_M4_22:

	KERNEL4x4_M1
	KERNEL4x4_M2

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L4_M4_22

.Lcgemm_kernel_L4_M4_22a:
	KERNEL4x4_M1
	KERNEL4x4_E
	b	 .Lcgemm_kernel_L4_M4_44
.Lcgemm_kernel_L4_M4_32:
	tst	counterL, #1
	ble	.Lcgemm_kernel_L4_M4_40
	KERNEL4x4_I
	KERNEL4x4_E
	b	.Lcgemm_kernel_L4_M4_44
.Lcgemm_kernel_L4_M4_40:

	INIT4x4

.Lcgemm_kernel_L4_M4_44:
	ands	counterL , origK, #1
	ble	.Lcgemm_kernel_L4_M4_100

.Lcgemm_kernel_L4_M4_46:
	KERNEL4x4_SUB

.Lcgemm_kernel_L4_M4_100:

	SAVE4x4

.Lcgemm_kernel_L4_M4_END:

.Lcgemm_kernel_L4_M2_BEGIN:

	mov	counterI, origM
	tst	counterI , #3
	ble	.Lcgemm_kernel_L4_END

	tst	counterI, #2			// counterI = counterI / 2
	ble	.Lcgemm_kernel_L4_M1_BEGIN

.Lcgemm_kernel_L4_M2_20:

	INIT2x4

	mov	pB, origPB
	asr 	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Lcgemm_kernel_L4_M2_40

.Lcgemm_kernel_L4_M2_22:

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L4_M2_22


.Lcgemm_kernel_L4_M2_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lcgemm_kernel_L4_M2_100

.Lcgemm_kernel_L4_M2_42:

	KERNEL2x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L4_M2_42

.Lcgemm_kernel_L4_M2_100:

	SAVE2x4

.Lcgemm_kernel_L4_M2_END:


.Lcgemm_kernel_L4_M1_BEGIN:

	tst	counterI, #1			// counterI = counterI % 2
	ble	.Lcgemm_kernel_L4_END

.Lcgemm_kernel_L4_M1_20:

	INIT1x4

	mov	pB, origPB
	asr 	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Lcgemm_kernel_L4_M1_40

.Lcgemm_kernel_L4_M1_22:
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L4_M1_22


.Lcgemm_kernel_L4_M1_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lcgemm_kernel_L4_M1_100

.Lcgemm_kernel_L4_M1_42:

	KERNEL1x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L4_M1_42

.Lcgemm_kernel_L4_M1_100:

	SAVE1x4


.Lcgemm_kernel_L4_END:

	lsl	temp, origK, #5 
	add	origPB, origPB, temp		// B = B + K * 4 * 8

	subs	counterJ, counterJ , #1		// j--
	bgt	.Lcgemm_kernel_L4_BEGIN


/******************************************************************************/

.Lcgemm_kernel_L2_BEGIN:   // less than 2 left in N direction

	mov	counterJ , origN
	tst	counterJ , #3
	ble	.Lcgemm_kernel_L999   // error, N was less than 4?

	tst	counterJ , #2
	ble	.Lcgemm_kernel_L1_BEGIN

	mov	pCRow0, pC			// pCRow0 = pC

	add	pC,pC,LDC, lsl #1

	mov	pA, origPA			// pA = A


.Lcgemm_kernel_L2_M8_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #3		// counterI = counterI / 8
	cmp	counterI, #0
	ble	.Lcgemm_kernel_L2_M4_BEGIN

.Lcgemm_kernel_L2_M8_20:

	INIT8x2

	mov	pB, origPB

	asr	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL,#0
	ble	.Lcgemm_kernel_L2_M8_40
	.align 5

.Lcgemm_kernel_L2_M8_22:
	KERNEL8x2_SUB
	KERNEL8x2_SUB
	KERNEL8x2_SUB
	KERNEL8x2_SUB

	KERNEL8x2_SUB
	KERNEL8x2_SUB
	KERNEL8x2_SUB
	KERNEL8x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L2_M8_22


.Lcgemm_kernel_L2_M8_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lcgemm_kernel_L2_M8_100

.Lcgemm_kernel_L2_M8_42:

	KERNEL8x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L2_M8_42

.Lcgemm_kernel_L2_M8_100:

	SAVE8x2

.Lcgemm_kernel_L2_M8_END:

	subs	counterI, counterI, #1
	bgt	.Lcgemm_kernel_L2_M8_20

.Lcgemm_kernel_L2_M4_BEGIN:

	mov	counterI, origM
	tst	counterI , #7
	ble	.Lcgemm_kernel_L2_END

	tst	counterI, #4			// counterI = counterI / 2
	ble	.Lcgemm_kernel_L2_M2_BEGIN

.Lcgemm_kernel_L2_M4_20:

	INIT4x2

	mov	pB, origPB
	asr	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL,#0
	ble	.Lcgemm_kernel_L2_M4_40
	.align 5

.Lcgemm_kernel_L2_M4_22:
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L2_M4_22


.Lcgemm_kernel_L2_M4_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lcgemm_kernel_L2_M4_100

.Lcgemm_kernel_L2_M4_42:

	KERNEL4x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L2_M4_42

.Lcgemm_kernel_L2_M4_100:

	SAVE4x2

.Lcgemm_kernel_L2_M4_END:

.Lcgemm_kernel_L2_M2_BEGIN:

	mov	counterI, origM
	tst	counterI , #3
	ble	.Lcgemm_kernel_L2_END

	tst	counterI, #2			// counterI = counterI / 2
	ble	.Lcgemm_kernel_L2_M1_BEGIN

.Lcgemm_kernel_L2_M2_20:

	INIT2x2

	mov	pB, origPB
	asr	counterL , origK, #3		// counterL = counterL / 8
        cmp	counterL,#0
	ble	.Lcgemm_kernel_L2_M2_40

.Lcgemm_kernel_L2_M2_22:

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L2_M2_22


.Lcgemm_kernel_L2_M2_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lcgemm_kernel_L2_M2_100

.Lcgemm_kernel_L2_M2_42:

	KERNEL2x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L2_M2_42

.Lcgemm_kernel_L2_M2_100:

	SAVE2x2

.Lcgemm_kernel_L2_M2_END:


.Lcgemm_kernel_L2_M1_BEGIN:

	tst	counterI, #1			// counterI = counterI % 2
	ble	.Lcgemm_kernel_L2_END

.Lcgemm_kernel_L2_M1_20:

	INIT1x2

	mov	pB, origPB
	asr 	counterL , origK, #3		// counterL = counterL / 8
        cmp     counterL, #0
	ble	.Lcgemm_kernel_L2_M1_40

.Lcgemm_kernel_L2_M1_22:
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L2_M1_22


.Lcgemm_kernel_L2_M1_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lcgemm_kernel_L2_M1_100

.Lcgemm_kernel_L2_M1_42:

	KERNEL1x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L2_M1_42

.Lcgemm_kernel_L2_M1_100:

	SAVE1x2


.Lcgemm_kernel_L2_END:
	add	origPB, origPB, origK, lsl #4	// B = B + K * 2 * 8

/******************************************************************************/

.Lcgemm_kernel_L1_BEGIN:

	mov	counterJ , origN
	tst	counterJ , #1
	ble	.Lcgemm_kernel_L999 // done


	mov	pCRow0, pC			// pCRow0 = C
	add	pC , pC , LDC			// Update pC to point to next

	mov	pA, origPA			// pA = A


.Lcgemm_kernel_L1_M8_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #3		// counterI = counterI / 8
	cmp	counterI, #0
	ble	.Lcgemm_kernel_L1_M4_BEGIN

.Lcgemm_kernel_L1_M8_20:

	INIT8x1

	mov	pB, origPB
	asr	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Lcgemm_kernel_L1_M8_40
	.align 5

.Lcgemm_kernel_L1_M8_22:
	KERNEL8x1_SUB
	KERNEL8x1_SUB
	KERNEL8x1_SUB
	KERNEL8x1_SUB

	KERNEL8x1_SUB
	KERNEL8x1_SUB
	KERNEL8x1_SUB
	KERNEL8x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L1_M8_22


.Lcgemm_kernel_L1_M8_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lcgemm_kernel_L1_M8_100

.Lcgemm_kernel_L1_M8_42:

	KERNEL8x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L1_M8_42

.Lcgemm_kernel_L1_M8_100:

	SAVE8x1

.Lcgemm_kernel_L1_M8_END:

	subs	counterI, counterI, #1
	bgt	.Lcgemm_kernel_L1_M8_20

.Lcgemm_kernel_L1_M4_BEGIN:

	mov	counterI, origM
	tst	counterI , #7
	ble	.Lcgemm_kernel_L1_END

	tst	counterI, #4			// counterI = counterI / 2
	ble	.Lcgemm_kernel_L1_M2_BEGIN


.Lcgemm_kernel_L1_M4_20:

	INIT4x1

	mov	pB, origPB
	asr	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Lcgemm_kernel_L1_M4_40
	.align 5

.Lcgemm_kernel_L1_M4_22:
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L1_M4_22


.Lcgemm_kernel_L1_M4_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lcgemm_kernel_L1_M4_100

.Lcgemm_kernel_L1_M4_42:

	KERNEL4x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L1_M4_42

.Lcgemm_kernel_L1_M4_100:

	SAVE4x1

.Lcgemm_kernel_L1_M4_END:


.Lcgemm_kernel_L1_M2_BEGIN:

	mov	counterI, origM
	tst	counterI , #3
	ble	.Lcgemm_kernel_L1_END

	tst	counterI, #2			// counterI = counterI / 2
	ble	.Lcgemm_kernel_L1_M1_BEGIN

.Lcgemm_kernel_L1_M2_20:

	INIT2x1

	mov	pB, origPB
	asr 	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Lcgemm_kernel_L1_M2_40

.Lcgemm_kernel_L1_M2_22:

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L1_M2_22


.Lcgemm_kernel_L1_M2_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lcgemm_kernel_L1_M2_100

.Lcgemm_kernel_L1_M2_42:

	KERNEL2x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L1_M2_42

.Lcgemm_kernel_L1_M2_100:

	SAVE2x1

.Lcgemm_kernel_L1_M2_END:


.Lcgemm_kernel_L1_M1_BEGIN:

	tst	counterI, #1			// counterI = counterI % 2
	ble	.Lcgemm_kernel_L1_END

.Lcgemm_kernel_L1_M1_20:

	INIT1x1

	mov	pB, origPB
	asr 	counterL , origK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Lcgemm_kernel_L1_M1_40

.Lcgemm_kernel_L1_M1_22:
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L1_M1_22


.Lcgemm_kernel_L1_M1_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lcgemm_kernel_L1_M1_100

.Lcgemm_kernel_L1_M1_42:

	KERNEL1x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L1_M1_42

.Lcgemm_kernel_L1_M1_100:

	SAVE1x1


.Lcgemm_kernel_L1_END:


.Lcgemm_kernel_L999:
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

