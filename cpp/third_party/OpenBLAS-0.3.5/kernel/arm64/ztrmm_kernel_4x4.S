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

/*                   X0          X1          X2          s0           s1         X3        x4       x5           x6              x7  */
/*int CNAME(BLASLONG bm,BLASLONG bn,BLASLONG bk,FLOAT alpha0,FLOAT alpha1,FLOAT* ba,FLOAT* bb,FLOAT* C,BLASLONG ldc, BLASLONG offset */

#define origM		x0
#define origN		x1
#define origK		x2
#define origPA		x3
#define origPB		x4
#define pC		x5
#define LDC		x6
#define offset		x7
#define counterL	x8
#define counterI	x9
#define counterJ	x10
#define pB		x11
#define pCRow0		x12
#define pCRow1		x13
#define pCRow2		x14
#define pCRow3		x15
#define pA		x16
#define alphaR		x17
#define alphaI		x18
#define temp		x19
#define tempOffset	x20
#define tempK		x21

#define alpha0_R	d10
#define alphaV0_R	v10.d[0]
#define alpha0_I	d11
#define alphaV0_I	v11.d[0]

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
// 17 alpha_save_R
// 18 must save alpha_save_I
// 19 must save temp
// 20 must save tempOffset
// 21 must save tempK
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

//v00 ALPHA_R -> pA00_R, pA01_R
//v01 ALPHA_I -> pA00_I, pA01_I
//v02 pA02_R, pA03_R
//v03 pA02_I, pA03_I
//v04 pA10_R, pA11_R
//v05 pA10_I, pA11_I
//v06 pA12_R, pA13_R
//v07 pA12_I, pA13_I
//v08 must save pB00_R, pB01_R
//v09 must save pB00_I, pB01_I
//v10 must save pB02_R, pB03_R OR ALPHA0_R
//v11 must save pB02_I, pB03_I OR ALPHA0_I
//v12 must save pB10_R, pB11_R
//v13 must save pB10_I, pB11_I
//v14 must save pB12_R, pB13_R OR ALPHA1_R
//v15 must save pB12_I, pB13_I OR ALPHA1_R
//v16 must save pC00_R, pC01_R
//v17 must save pC00_I, pC01_I
//v18 pC02_R, pC03_R
//v19 pC02_I, pC03_I
//v20 pC10_R, pC11_R
//v21 pC10_I, pC11_I
//v22 pC12_R, pC13_R
//v23 pC12_I, pC13_I
//v24 pC20_R, pC21_R
//v25 pC20_I, pC21_I
//v26 pC22_R, pC23_R
//v27 pC22_I, pC23_I
//v28 pC30_R, pC31_R
//v29 pC30_I, pC31_I
//v30 pC32_R, pC33_R
//v31 pC32_I, pC33_I

/*******************************************************************************
* Macro definitions
*******************************************************************************/

.macro INIT4x4
	fmov		d16, xzr
	fmov		d17, d16
	fmov		d18, d17
	fmov		d19, d16
	fmov		d20, d17
	fmov		d21, d16
	fmov		d22, d17
	fmov		d23, d16
	fmov		d24, d17
	fmov		d25, d16
	fmov		d26, d17
	fmov		d27, d16
	fmov		d28, d17
	fmov		d29, d16
	fmov		d30, d17
	fmov		d31, d16
.endm

.macro KERNEL4x4_I
	ld2	{v8.2d, v9.2d}, [pB]
	add	pB, pB, #32
	ld2	{v0.2d, v1.2d}, [pA]
	add	pA, pA, #32

	fmul	v16.2d, v0.2d, v8.d[0]
	OP_ii	v16.2d, v1.2d, v9.d[0]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v17.16b, v17.16b, v17.16b
	fmls	v17.2d, v0.2d, v9.d[0]
#else
	fmul	v17.2d, v0.2d, v9.d[0]
#endif
	OP_ir	v17.2d, v1.2d, v8.d[0]

	ld2	{v2.2d, v3.2d}, [pA]
	add	pA, pA, #32

	fmul	v20.2d, v0.2d, v8.d[1]
	OP_ii	v20.2d, v1.2d, v9.d[1]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v21.16b, v21.16b, v21.16b
	fmls	v21.2d, v0.2d, v9.d[1]
#else
	fmul	v21.2d, v0.2d, v9.d[1]
#endif
	OP_ir	v21.2d, v1.2d, v8.d[1]

	ld2	{v10.2d, v11.2d}, [pB]
	add	pB, pB, #32

	fmul	v22.2d, v2.2d, v8.d[1]
	OP_ii	v22.2d, v3.2d, v9.d[1]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v23.16b, v23.16b, v23.16b
	fmls	v23.2d, v2.2d, v9.d[1]
#else
	fmul	v23.2d, v2.2d, v9.d[1]
#endif
	OP_ir	v23.2d, v3.2d, v8.d[1]

	ld2	{v12.2d, v13.2d}, [pB]
	add	pB, pB, #32

	fmul	v18.2d, v2.2d, v8.d[0]
	OP_ii	v18.2d, v3.2d, v9.d[0]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v19.16b, v19.16b, v19.16b
	fmls	v19.2d, v2.2d, v9.d[0]
#else
	fmul	v19.2d, v2.2d, v9.d[0]
#endif
	OP_ir	v19.2d, v3.2d, v8.d[0]

	ld2	{v4.2d, v5.2d} , [pA]
	add	pA, pA, #32

	fmul	v24.2d, v0.2d, v10.d[0]
	OP_ii	v24.2d, v1.2d, v11.d[0]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v25.16b, v25.16b, v25.16b
	fmls	v25.2d, v0.2d, v11.d[0]
#else
	fmul	v25.2d, v0.2d, v11.d[0]
#endif
	OP_ir	v25.2d, v1.2d, v10.d[0]

	ld2	{v6.2d, v7.2d} , [pA]
	add	pA, pA, #32

	fmul	v26.2d, v2.2d, v10.d[0]
	OP_ii	v26.2d, v3.2d, v11.d[0]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v27.16b, v27.16b, v27.16b
	fmls	v27.2d, v2.2d, v11.d[0]
#else
	fmul	v27.2d, v2.2d, v11.d[0]
#endif
	OP_ir	v27.2d, v3.2d, v10.d[0]

	ld2	{v14.2d, v15.2d}, [pB]
	add	pB, pB, #32

	fmul	v28.2d, v0.2d, v10.d[1]
	OP_ii	v28.2d, v1.2d, v11.d[1]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v29.16b, v29.16b, v29.16b
	fmls	v29.2d, v0.2d, v11.d[1]
#else
	fmul	v29.2d, v0.2d, v11.d[1]
#endif
	OP_ir	v29.2d, v1.2d, v10.d[1]

	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE]

	fmul	v30.2d, v2.2d, v10.d[1]
	OP_ii	v30.2d, v3.2d, v11.d[1]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v31.16b, v31.16b, v31.16b
	fmls	v31.2d, v2.2d, v11.d[1]
#else
	fmul	v31.2d, v2.2d, v11.d[1]
#endif
	OP_ir	v31.2d, v3.2d, v10.d[1]

	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE+64]
.endm

.macro KERNEL4x4_M1
	OP_rr	v16.2d, v0.2d, v8.d[0]
	OP_ii	v16.2d, v1.2d, v9.d[0]
	OP_ri	v17.2d, v0.2d, v9.d[0]
	OP_ir	v17.2d, v1.2d, v8.d[0]

	ld2	{v12.2d, v13.2d}, [pB]
	add	pB, pB, #32

	OP_rr	v18.2d, v2.2d, v8.d[0]
	OP_ii	v18.2d, v3.2d, v9.d[0]
	OP_ri	v19.2d, v2.2d, v9.d[0]
	OP_ir	v19.2d, v3.2d, v8.d[0]

	ld2	{v4.2d, v5.2d} , [pA]
	add	pA, pA, #32

	OP_rr	v20.2d, v0.2d, v8.d[1]
	OP_ii	v20.2d, v1.2d, v9.d[1]
	OP_ri	v21.2d, v0.2d, v9.d[1]
	OP_ir	v21.2d, v1.2d, v8.d[1]

	ld2	{v6.2d, v7.2d} , [pA]
	add	pA, pA, #32

	OP_rr	v22.2d, v2.2d, v8.d[1]
	OP_ii	v22.2d, v3.2d, v9.d[1]
	OP_ri	v23.2d, v2.2d, v9.d[1]
	OP_ir	v23.2d, v3.2d, v8.d[1]

	ld2	{v14.2d, v15.2d}, [pB]
	add	pB, pB, #32

	OP_rr	v24.2d, v0.2d, v10.d[0]
	OP_ii	v24.2d, v1.2d, v11.d[0]
	OP_ri	v25.2d, v0.2d, v11.d[0]
	OP_ir	v25.2d, v1.2d, v10.d[0]

	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE]

	OP_rr	v26.2d, v2.2d, v10.d[0]
	OP_ii	v26.2d, v3.2d, v11.d[0]
	OP_ri	v27.2d, v2.2d, v11.d[0]
	OP_ir	v27.2d, v3.2d, v10.d[0]

	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE+64]

	OP_rr	v28.2d, v0.2d, v10.d[1]
	OP_ii	v28.2d, v1.2d, v11.d[1]
	OP_ri	v29.2d, v0.2d, v11.d[1]
	OP_ir	v29.2d, v1.2d, v10.d[1]

	OP_rr	v30.2d, v2.2d, v10.d[1]
	OP_ii	v30.2d, v3.2d, v11.d[1]
	OP_ri	v31.2d, v2.2d, v11.d[1]
	OP_ir	v31.2d, v3.2d, v10.d[1]
.endm

.macro KERNEL4x4_M2
	OP_rr	v16.2d, v4.2d, v12.d[0]
	OP_ii	v16.2d, v5.2d, v13.d[0]
	OP_ri	v17.2d, v4.2d, v13.d[0]
	OP_ir	v17.2d, v5.2d, v12.d[0]

	ld2	{v8.2d, v9.2d}, [pB]
	add	pB, pB, #32

	OP_rr	v18.2d, v6.2d, v12.d[0]
	OP_ii	v18.2d, v7.2d, v13.d[0]
	OP_ri	v19.2d, v6.2d, v13.d[0]
	OP_ir	v19.2d, v7.2d, v12.d[0]

	ld2	{v0.2d, v1.2d}, [pA]
	add	pA, pA, #32

	OP_rr	v20.2d, v4.2d, v12.d[1]
	OP_ii	v20.2d, v5.2d, v13.d[1]
	OP_ri	v21.2d, v4.2d, v13.d[1]
	OP_ir	v21.2d, v5.2d, v12.d[1]

	ld2	{v2.2d, v3.2d}, [pA]
	add	pA, pA, #32

	OP_rr	v22.2d, v6.2d, v12.d[1]
	OP_ii	v22.2d, v7.2d, v13.d[1]
	OP_ri	v23.2d, v6.2d, v13.d[1]
	OP_ir	v23.2d, v7.2d, v12.d[1]

	ld2	{v10.2d, v11.2d}, [pB]
	add	pB, pB, #32

	OP_rr	v24.2d, v4.2d, v14.d[0]
	OP_ii	v24.2d, v5.2d, v15.d[0]
	OP_ri	v25.2d, v4.2d, v15.d[0]
	OP_ir	v25.2d, v5.2d, v14.d[0]

	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]

	OP_rr	v26.2d, v6.2d, v14.d[0]
	OP_ii	v26.2d, v7.2d, v15.d[0]
	OP_ri	v27.2d, v6.2d, v15.d[0]
	OP_ir	v27.2d, v7.2d, v14.d[0]

	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE+64]

	OP_rr	v28.2d, v4.2d, v14.d[1]
	OP_ii	v28.2d, v5.2d, v15.d[1]
	OP_ri	v29.2d, v4.2d, v15.d[1]
	OP_ir	v29.2d, v5.2d, v14.d[1]

	OP_rr	v30.2d, v6.2d, v14.d[1]
	OP_ii	v30.2d, v7.2d, v15.d[1]
	OP_ri	v31.2d, v6.2d, v15.d[1]
	OP_ir	v31.2d, v7.2d, v14.d[1]
.endm

.macro KERNEL4x4_E
	OP_rr	v16.2d, v4.2d, v12.d[0]
	OP_ii	v16.2d, v5.2d, v13.d[0]
	OP_ri	v17.2d, v4.2d, v13.d[0]
	OP_ir	v17.2d, v5.2d, v12.d[0]

	OP_rr	v18.2d, v6.2d, v12.d[0]
	OP_ii	v18.2d, v7.2d, v13.d[0]
	OP_ri	v19.2d, v6.2d, v13.d[0]
	OP_ir	v19.2d, v7.2d, v12.d[0]

	OP_rr	v20.2d, v4.2d, v12.d[1]
	OP_ii	v20.2d, v5.2d, v13.d[1]
	OP_ri	v21.2d, v4.2d, v13.d[1]
	OP_ir	v21.2d, v5.2d, v12.d[1]

	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]

	OP_rr	v22.2d, v6.2d, v12.d[1]
	OP_ii	v22.2d, v7.2d, v13.d[1]
	OP_ri	v23.2d, v6.2d, v13.d[1]
	OP_ir	v23.2d, v7.2d, v12.d[1]

	OP_rr	v24.2d, v4.2d, v14.d[0]
	OP_ii	v24.2d, v5.2d, v15.d[0]
	OP_ri	v25.2d, v4.2d, v15.d[0]
	OP_ir	v25.2d, v5.2d, v14.d[0]

	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE+64]

	OP_rr	v26.2d, v6.2d, v14.d[0]
	OP_ii	v26.2d, v7.2d, v15.d[0]
	OP_ri	v27.2d, v6.2d, v15.d[0]
	OP_ir	v27.2d, v7.2d, v14.d[0]

	OP_rr	v28.2d, v4.2d, v14.d[1]
	OP_ii	v28.2d, v5.2d, v15.d[1]
	OP_ri	v29.2d, v4.2d, v15.d[1]
	OP_ir	v29.2d, v5.2d, v14.d[1]

	OP_rr	v30.2d, v6.2d, v14.d[1]
	OP_ii	v30.2d, v7.2d, v15.d[1]
	OP_ri	v31.2d, v6.2d, v15.d[1]
	OP_ir	v31.2d, v7.2d, v14.d[1]
.endm

.macro KERNEL4x4_SUB
	ld2	{v8.2d, v9.2d}, [pB]
	add	pB, pB, #32

	ld2	{v0.2d, v1.2d}, [pA]
	add	pA, pA, #32

	OP_rr	v16.2d, v0.2d, v8.d[0]
	OP_ii	v16.2d, v1.2d, v9.d[0]
	OP_ri	v17.2d, v0.2d, v9.d[0]
	OP_ir	v17.2d, v1.2d, v8.d[0]

	ld2	{v2.2d, v3.2d}, [pA]
	add	pA, pA, #32

	OP_rr	v20.2d, v0.2d, v8.d[1]
	OP_ii	v20.2d, v1.2d, v9.d[1]
	OP_ri	v21.2d, v0.2d, v9.d[1]
	OP_ir	v21.2d, v1.2d, v8.d[1]

	ld2	{v10.2d, v11.2d}, [pB]
	add	pB, pB, #32

	OP_rr	v18.2d, v2.2d, v8.d[0]
	OP_ii	v18.2d, v3.2d, v9.d[0]
	OP_ri	v19.2d, v2.2d, v9.d[0]
	OP_ir	v19.2d, v3.2d, v8.d[0]

	prfm	PLDL1KEEP, [pB, #B_PRE_SIZE]

	OP_rr	v22.2d, v2.2d, v8.d[1]
	OP_ii	v22.2d, v3.2d, v9.d[1]
	OP_ri	v23.2d, v2.2d, v9.d[1]
	OP_ir	v23.2d, v3.2d, v8.d[1]

	prfm	PLDL1KEEP, [pA, #A_PRE_SIZE]

	OP_rr	v24.2d, v0.2d, v10.d[0]
	OP_ii	v24.2d, v1.2d, v11.d[0]
	OP_ri	v25.2d, v0.2d, v11.d[0]
	OP_ir	v25.2d, v1.2d, v10.d[0]

	OP_rr	v26.2d, v2.2d, v10.d[0]
	OP_ii	v26.2d, v3.2d, v11.d[0]
	OP_ri	v27.2d, v2.2d, v11.d[0]
	OP_ir	v27.2d, v3.2d, v10.d[0]

	OP_rr	v28.2d, v0.2d, v10.d[1]
	OP_ii	v28.2d, v1.2d, v11.d[1]
	OP_ri	v29.2d, v0.2d, v11.d[1]
	OP_ir	v29.2d, v1.2d, v10.d[1]

	OP_rr	v30.2d, v2.2d, v10.d[1]
	OP_ii	v30.2d, v3.2d, v11.d[1]
	OP_ri	v31.2d, v2.2d, v11.d[1]
	OP_ir	v31.2d, v3.2d, v10.d[1]
.endm

.macro SAVE4x4
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	prfm	PLDL2KEEP, [pCRow0, #C_PRE_SIZE]

	fmul	v0.2d, v16.2d, alphaV0_R
	fmls	v0.2d, v17.2d, alphaV0_I
	fmul	v1.2d, v16.2d, alphaV0_I
	fmla	v1.2d, v17.2d, alphaV0_R
	st2 	{v0.2d, v1.2d}, [pCRow0]

	add	pCRow0, pCRow0, #32

	fmul	v2.2d, v18.2d, alphaV0_R
	fmls	v2.2d, v19.2d, alphaV0_I
	fmul	v3.2d, v18.2d, alphaV0_I
	fmla	v3.2d, v19.2d, alphaV0_R
	st2 	{v2.2d, v3.2d}, [pCRow0]

	add	pCRow0, pCRow0, #32
	prfm	PLDL2KEEP, [pCRow1, #C_PRE_SIZE]

	fmul	v4.2d, v20.2d, alphaV0_R
	fmls	v4.2d, v21.2d, alphaV0_I
	fmul	v5.2d, v20.2d, alphaV0_I
	fmla	v5.2d, v21.2d, alphaV0_R
	st2 	{v4.2d, v5.2d}, [pCRow1]

	add	pCRow1, pCRow1, #32

	fmul	v6.2d, v22.2d, alphaV0_R
	fmls	v6.2d, v23.2d, alphaV0_I
	fmul	v7.2d, v22.2d, alphaV0_I
	fmla	v7.2d, v23.2d, alphaV0_R
	st2 	{v6.2d, v7.2d}, [pCRow1]

	add	pCRow1, pCRow1, #32
	prfm	PLDL2KEEP, [pCRow2, #C_PRE_SIZE]

	fmul	v0.2d, v24.2d, alphaV0_R
	fmls	v0.2d, v25.2d, alphaV0_I
	fmul	v1.2d, v24.2d, alphaV0_I
	fmla	v1.2d, v25.2d, alphaV0_R
	st2 	{v0.2d, v1.2d}, [pCRow2]

	add	pCRow2, pCRow2, #32

	fmul	v2.2d, v26.2d, alphaV0_R
	fmls	v2.2d, v27.2d, alphaV0_I
	fmul	v3.2d, v26.2d, alphaV0_I
	fmla	v3.2d, v27.2d, alphaV0_R
	st2 	{v2.2d, v3.2d}, [pCRow2]

	add	pCRow2, pCRow2, #32
	prfm	PLDL2KEEP, [pCRow3, #C_PRE_SIZE]

	fmul	v4.2d, v28.2d, alphaV0_R
	fmls	v4.2d, v29.2d, alphaV0_I
	fmul	v5.2d, v28.2d, alphaV0_I
	fmla	v5.2d, v29.2d, alphaV0_R
	st2 	{v4.2d, v5.2d}, [pCRow3]

	add	pCRow3, pCRow3, #32

	fmul	v6.2d, v30.2d, alphaV0_R
	fmls	v6.2d, v31.2d, alphaV0_I
	fmul	v7.2d, v30.2d, alphaV0_I
	fmla	v7.2d, v31.2d, alphaV0_R
	st2 	{v6.2d, v7.2d}, [pCRow3]

	add	pCRow3, pCRow3, #32
.endm

/******************************************************************************/

.macro INIT2x4
	fmov	d16, xzr
	fmov	d17, xzr
	fmov	d20, d16
	fmov	d21, d17
	fmov	d24, d16
	fmov	d25, d17
	fmov	d28, d16
	fmov	d29, d17
.endm

.macro KERNEL2x4_SUB
	ld2	{v8.2d, v9.2d}, [pB]
	add	pB, pB, #32
	ld2	{v10.2d, v11.2d}, [pB]
	add	pB, pB, #32

	ld2	{v0.2d, v1.2d}, [pA]
	add	pA, pA, #32

	OP_rr	v16.2d, v0.2d, v8.d[0]
	OP_ii	v16.2d, v1.2d, v9.d[0]
	OP_ri	v17.2d, v0.2d, v9.d[0]
	OP_ir	v17.2d, v1.2d, v8.d[0]

	OP_rr	v20.2d, v0.2d, v8.d[1]
	OP_ii	v20.2d, v1.2d, v9.d[1]
	OP_ri	v21.2d, v0.2d, v9.d[1]
	OP_ir	v21.2d, v1.2d, v8.d[1]

	OP_rr	v24.2d, v0.2d, v10.d[0]
	OP_ii	v24.2d, v1.2d, v11.d[0]
	OP_ri	v25.2d, v0.2d, v11.d[0]
	OP_ir	v25.2d, v1.2d, v10.d[0]

	OP_rr	v28.2d, v0.2d, v10.d[1]
	OP_ii	v28.2d, v1.2d, v11.d[1]
	OP_ri	v29.2d, v0.2d, v11.d[1]
	OP_ir	v29.2d, v1.2d, v10.d[1]
.endm

.macro SAVE2x4
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	mov	pCRow1, pCRow0

	fmul	v0.2d, v16.2d, alphaV0_R
	fmls	v0.2d, v17.2d, alphaV0_I
	fmul	v1.2d, v16.2d, alphaV0_I
	fmla	v1.2d, v17.2d, alphaV0_R
	st2 	{v0.2d, v1.2d}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	fmul	v4.2d, v20.2d, alphaV0_R
	fmls	v4.2d, v21.2d, alphaV0_I
	fmul	v5.2d, v20.2d, alphaV0_I
	fmla	v5.2d, v21.2d, alphaV0_R
	st2 	{v4.2d, v5.2d}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	fmul	v0.2d, v24.2d, alphaV0_R
	fmls	v0.2d, v25.2d, alphaV0_I
	fmul	v1.2d, v24.2d, alphaV0_I
	fmla	v1.2d, v25.2d, alphaV0_R
	st2 	{v0.2d, v1.2d}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	fmul	v4.2d, v28.2d, alphaV0_R
	fmls	v4.2d, v29.2d, alphaV0_I
	fmul	v5.2d, v28.2d, alphaV0_I
	fmla	v5.2d, v29.2d, alphaV0_R
	st2 	{v4.2d, v5.2d}, [pCRow1]

	add	pCRow0, pCRow0, #32
.endm

/******************************************************************************/

.macro INIT1x4
	fmov	d16, xzr
	fmov	d17, xzr
	fmov	d20, d16
	fmov	d21, d17
	fmov	d24, d16
	fmov	d25, d17
	fmov	d28, d16
	fmov	d29, d17
.endm

.macro KERNEL1x4_SUB
	ld2	{v8.2d, v9.2d}, [pB]
	add	pB, pB, #32
	ld2	{v10.2d, v11.2d}, [pB]
	add	pB, pB, #32
	ld2	{v0.d, v1.d}[0], [pA]
	add	pA, pA, #16

	OP_rr	d16, d0, v8.d[0]
	OP_ii	d16, d1, v9.d[0]
	OP_ri	d17, d0, v9.d[0]
	OP_ir	d17, d1, v8.d[0]

	OP_rr	d20, d0, v8.d[1]
	OP_ii	d20, d1, v9.d[1]
	OP_ri	d21, d0, v9.d[1]
	OP_ir	d21, d1, v8.d[1]

	OP_rr	d24, d0, v10.d[0]
	OP_ii	d24, d1, v11.d[0]
	OP_ri	d25, d0, v11.d[0]
	OP_ir	d25, d1, v10.d[0]

	OP_rr	d28, d0, v10.d[1]
	OP_ii	d28, d1, v11.d[1]
	OP_ri	d29, d0, v11.d[1]
	OP_ir	d29, d1, v10.d[1]
.endm

.macro SAVE1x4
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	mov	pCRow1, pCRow0

	fmul	d0, d16, alphaV0_R
	fmls	d0, d17, alphaV0_I
	fmul	d1, d16, alphaV0_I
	fmla	d1, d17, alphaV0_R
	st2	{v0.d, v1.d}[0], [pCRow1]

	add	pCRow1, pCRow1, LDC

	fmul	d4, d20, alphaV0_R
	fmls	d4, d21, alphaV0_I
	fmul	d5, d20, alphaV0_I
	fmla	d5, d21, alphaV0_R
	st2 	{v4.d, v5.d}[0], [pCRow1]

	add	pCRow1, pCRow1, LDC

	fmul	d0, d24, alphaV0_R
	fmls	d0, d25, alphaV0_I
	fmul	d1, d24, alphaV0_I
	fmla	d1, d25, alphaV0_R
	st2	{v0.d, v1.d}[0], [pCRow1]

	add	pCRow1, pCRow1, LDC

	fmul	d4, d28, alphaV0_R
	fmls	d4, d29, alphaV0_I
	fmul	d5, d28, alphaV0_I
	fmla	d5, d29, alphaV0_R
	st2	{v4.d, v5.d}[0], [pCRow1]

	add	pCRow0, pCRow0, #16
.endm

/******************************************************************************/

.macro INIT4x2
	fmov	d16, xzr
	fmov	d17, xzr
	fmov	d18, d16
	fmov	d19, d17
	fmov	d20, d16
	fmov	d21, d17
	fmov	d22, d16
	fmov	d23, d17
.endm

.macro KERNEL4x2_SUB
	ld2	{v8.2d, v9.2d}, [pB]
	add	pB, pB, #32
	ld2	{v0.2d, v1.2d}, [pA]
	add	pA, pA, #32
	ld2	{v2.2d, v3.2d}, [pA]
	add	pA, pA, #32

	OP_rr	v16.2d, v0.2d, v8.d[0]
	OP_ii	v16.2d, v1.2d, v9.d[0]
	OP_ri	v17.2d, v0.2d, v9.d[0]
	OP_ir	v17.2d, v1.2d, v8.d[0]

	OP_rr	v18.2d, v2.2d, v8.d[0]
	OP_ii	v18.2d, v3.2d, v9.d[0]
	OP_ri	v19.2d, v2.2d, v9.d[0]
	OP_ir	v19.2d, v3.2d, v8.d[0]

	OP_rr	v20.2d, v0.2d, v8.d[1]
	OP_ii	v20.2d, v1.2d, v9.d[1]
	OP_ri	v21.2d, v0.2d, v9.d[1]
	OP_ir	v21.2d, v1.2d, v8.d[1]

	OP_rr	v22.2d, v2.2d, v8.d[1]
	OP_ii	v22.2d, v3.2d, v9.d[1]
	OP_ri	v23.2d, v2.2d, v9.d[1]
	OP_ir	v23.2d, v3.2d, v8.d[1]
.endm

.macro SAVE4x2
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	mov	pCRow1, pCRow0

	fmul	v0.2d, v16.2d, alphaV0_R
	fmls	v0.2d, v17.2d, alphaV0_I
	fmul	v1.2d, v16.2d, alphaV0_I
	fmla	v1.2d, v17.2d, alphaV0_R
	st2 	{v0.2d, v1.2d}, [pCRow1]
	add	pCRow2, pCRow1, #32
	fmul	v2.2d, v18.2d, alphaV0_R
	fmls	v2.2d, v19.2d, alphaV0_I
	fmul	v3.2d, v18.2d, alphaV0_I
	fmla	v3.2d, v19.2d, alphaV0_R
	st2 	{v2.2d, v3.2d}, [pCRow2]

	add	pCRow1, pCRow1, LDC

	fmul	v4.2d, v20.2d, alphaV0_R
	fmls	v4.2d, v21.2d, alphaV0_I
	fmul	v5.2d, v20.2d, alphaV0_I
	fmla	v5.2d, v21.2d, alphaV0_R
	st2 	{v4.2d, v5.2d}, [pCRow1]
	add	pCRow2, pCRow1, #32
	fmul	v6.2d, v22.2d, alphaV0_R
	fmls	v6.2d, v23.2d, alphaV0_I
	fmul	v7.2d, v22.2d, alphaV0_I
	fmla	v7.2d, v23.2d, alphaV0_R
	st2 	{v6.2d, v7.2d}, [pCRow2]

	add	pCRow0, pCRow0, #64
.endm

/******************************************************************************/

.macro INIT2x2
	fmov		d16, xzr
	fmov		d17, xzr
	fmov		d20, d16
	fmov		d21, d17
.endm

.macro KERNEL2x2_SUB
	ld2	{v8.2d, v9.2d}, [pB]
	add	pB, pB, #32
	ld2	{v0.2d, v1.2d}, [pA]
	add	pA, pA, #32

	OP_rr	v16.2d, v0.2d, v8.d[0]
	OP_ii	v16.2d, v1.2d, v9.d[0]
	OP_ri	v17.2d, v0.2d, v9.d[0]
	OP_ir	v17.2d, v1.2d, v8.d[0]

	OP_rr	v20.2d, v0.2d, v8.d[1]
	OP_ii	v20.2d, v1.2d, v9.d[1]
	OP_ri	v21.2d, v0.2d, v9.d[1]
	OP_ir	v21.2d, v1.2d, v8.d[1]
.endm

.macro SAVE2x2
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	mov	pCRow1, pCRow0

	fmul	v0.2d, v16.2d, alphaV0_R
	fmls	v0.2d, v17.2d, alphaV0_I
	fmul	v1.2d, v16.2d, alphaV0_I
	fmla	v1.2d, v17.2d, alphaV0_R
	st2 	{v0.2d, v1.2d}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	fmul	v4.2d, v20.2d, alphaV0_R
	fmls	v4.2d, v21.2d, alphaV0_I
	fmul	v5.2d, v20.2d, alphaV0_I
	fmla	v5.2d, v21.2d, alphaV0_R
	st2 	{v4.2d, v5.2d}, [pCRow1]

	add	pCRow0, pCRow0, #32
.endm

/******************************************************************************/

.macro INIT1x2
	fmov		d16, xzr
	fmov		d17, xzr
	fmov		d20, xzr
	fmov		d21, xzr
.endm

.macro KERNEL1x2_SUB
	ld2	{v8.2d, v9.2d}, [pB]
	add	pB, pB, #32
	ld2	{v0.d, v1.d}[0], [pA]
	add	pA, pA, #16

	OP_rr	d16, d0, v8.d[0]
	OP_ii	d16, d1, v9.d[0]
	OP_ri	d17, d0, v9.d[0]
	OP_ir	d17, d1, v8.d[0]

	OP_rr	d20, d0, v8.d[1]
	OP_ii	d20, d1, v9.d[1]
	OP_ri	d21, d0, v9.d[1]
	OP_ir	d21, d1, v8.d[1]
.endm

.macro SAVE1x2
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	mov	pCRow1, pCRow0

	fmul	d0, d16, alphaV0_R
	fmls	d0, d17, alphaV0_I
	fmul	d1, d16, alphaV0_I
	fmla	d1, d17, alphaV0_R
	st2	{v0.d, v1.d}[0], [pCRow1]

	add	pCRow1, pCRow1, LDC

	fmul	d4, d20, alphaV0_R
	fmls	d4, d21, alphaV0_I
	fmul	d5, d20, alphaV0_I
	fmla	d5, d21, alphaV0_R
	st2 	{v4.d, v5.d}[0], [pCRow1]

	add	pCRow0, pCRow0, #16
.endm

/******************************************************************************/

.macro INIT4x1
	fmov	d16, xzr
	fmov	d17, d16
	fmov	d18, d16
	fmov	d19, d17
.endm

.macro KERNEL4x1_SUB
	ld2	{v8.d, v9.d}[0], [pB]
	add	pB, pB, #16
	ld2	{v0.2d, v1.2d}, [pA]
	add	pA, pA, #32
	ld2	{v2.2d, v3.2d}, [pA]
	add	pA, pA, #32

	OP_rr	v16.2d, v0.2d, v8.d[0]
	OP_ii	v16.2d, v1.2d, v9.d[0]
	OP_ri	v17.2d, v0.2d, v9.d[0]
	OP_ir	v17.2d, v1.2d, v8.d[0]

	OP_rr	v18.2d, v2.2d, v8.d[0]
	OP_ii	v18.2d, v3.2d, v9.d[0]
	OP_ri	v19.2d, v2.2d, v9.d[0]
	OP_ir	v19.2d, v3.2d, v8.d[0]
.endm

.macro SAVE4x1
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	mov	pCRow1, pCRow0

	fmul	v0.2d, v16.2d, alphaV0_R
	fmls	v0.2d, v17.2d, alphaV0_I
	fmul	v1.2d, v16.2d, alphaV0_I
	fmla	v1.2d, v17.2d, alphaV0_R
	st2 	{v0.2d, v1.2d}, [pCRow1]
	add	pCRow2, pCRow1, #32
	fmul	v2.2d, v18.2d, alphaV0_R
	fmls	v2.2d, v19.2d, alphaV0_I
	fmul	v3.2d, v18.2d, alphaV0_I
	fmla	v3.2d, v19.2d, alphaV0_R
	st2 	{v2.2d, v3.2d}, [pCRow2]

	add	pCRow0, pCRow0, #64
.endm

/******************************************************************************/

.macro INIT2x1
	fmov	d16, xzr
	fmov	d17, xzr
.endm

.macro KERNEL2x1_SUB
	ld2	{v8.d, v9.d}[0], [pB]
	add	pB, pB, #16
	ld2	{v0.2d, v1.2d}, [pA]
	add	pA, pA, #32

	OP_rr	v16.2d, v0.2d, v8.d[0]
	OP_ii	v16.2d, v1.2d, v9.d[0]
	OP_ri	v17.2d, v0.2d, v9.d[0]
	OP_ir	v17.2d, v1.2d, v8.d[0]
.endm

.macro SAVE2x1
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	mov	pCRow1, pCRow0

	fmul	v0.2d, v16.2d, alphaV0_R
	fmls	v0.2d, v17.2d, alphaV0_I
	fmul	v1.2d, v16.2d, alphaV0_I
	fmla	v1.2d, v17.2d, alphaV0_R
	st2 	{v0.2d, v1.2d}, [pCRow1]

	add	pCRow0, pCRow0, #32

.endm

/******************************************************************************/

.macro INIT1x1
	fmov	d16, xzr
	fmov	d17, xzr
.endm

.macro KERNEL1x1_SUB
	ld2	{v8.d, v9.d}[0], [pB]
	add	pB, pB, #16
	ld2	{v0.d, v1.d}[0], [pA]
	add	pA, pA, #16

	OP_rr	d16, d0, v8.d[0]
	OP_ii	d16, d1, v9.d[0]
	OP_ri	d17, d0, v9.d[0]
	OP_ir	d17, d1, v8.d[0]
.endm

.macro SAVE1x1
	fmov	alpha0_R, alphaR
	fmov	alpha0_I, alphaI

	mov	pCRow1, pCRow0

	fmul	d0, d16, alphaV0_R
	fmls	d0, d17, alphaV0_I
	fmul	d1, d16, alphaV0_I
	fmla	d1, d17, alphaV0_R
	st2	{v0.d, v1.d}[0], [pCRow1]

	add	pCRow0, pCRow0, #16
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

	fmov	alphaR, d0
	fmov	alphaI, d1

	lsl	LDC, LDC, #4			// ldc = ldc * 2 * 8

#if !defined(LEFT)
	neg	tempOffset, offset
#endif

	mov	pB, origPB

	mov	counterJ, origN
	asr 	counterJ, counterJ, #2		// J = J / 4
	cmp 	counterJ, #0
	ble	.Lztrmm_kernel_L2_BEGIN

.Lztrmm_kernel_L4_BEGIN:
	mov	pCRow0, pC
	add	pCRow1, pCRow0, LDC
	add	pCRow2, pCRow1, LDC
	add	pCRow3, pCRow2, LDC

	add	pC, pCRow3, LDC


#if defined(LEFT)
	mov	tempOffset, offset
#endif
	mov	pA, origPA			// pA = start of A array

.Lztrmm_kernel_L4_M4_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #2		// counterI = counterI / 4
	cmp 	counterI, #0
	ble	.Lztrmm_kernel_L4_M2_BEGIN

	.align 5
.Lztrmm_kernel_L4_M4_20:

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	pB, origPB
#else
	mov	pB, origPB
	lsl	temp, tempOffset, #6
	add	pB, pB, temp
	add	pA, pA, temp
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	tempK, origK, tempOffset
#elif defined(LEFT)
	add	tempK, tempOffset, #4
#else
	add	tempK, tempOffset, #4
#endif

	asr 	counterL , tempK, #3
	cmp	counterL , #2
	blt	.Lztrmm_kernel_L4_M4_32

	KERNEL4x4_I
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	subs	counterL, counterL, #2
	ble	.Lztrmm_kernel_L4_M4_22a

	.align 5
.Lztrmm_kernel_L4_M4_22:

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	subs	counterL, counterL, #1
	bgt	.Lztrmm_kernel_L4_M4_22

	.align 5
.Lztrmm_kernel_L4_M4_22a:

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_E

	b	 .Lztrmm_kernel_L4_M4_44

	.align 5
.Lztrmm_kernel_L4_M4_32:

	tst	counterL, #1
	ble	.Lztrmm_kernel_L4_M4_40

	KERNEL4x4_I
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_E

	b	.Lztrmm_kernel_L4_M4_44


.Lztrmm_kernel_L4_M4_40:

	INIT4x4

.Lztrmm_kernel_L4_M4_44:

	ands	counterL , tempK, #7
	ble	.Lztrmm_kernel_L4_M4_100

	.align 5
.Lztrmm_kernel_L4_M4_46:
	KERNEL4x4_SUB

	subs	counterL, counterL, #1
	bne	.Lztrmm_kernel_L4_M4_46

.Lztrmm_kernel_L4_M4_100:

	SAVE4x4

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub	tempK, origK, tempOffset
#if defined(LEFT)
	sub	tempK, tempK, #4
#else
	sub	tempK, tempK, #4
#endif
	lsl	temp, tempK, #6
	add	pA, pA, temp
	add	pB, pB, temp
#endif
#if defined(LEFT)
	add	tempOffset, tempOffset, #4
#endif

	prfm	PLDL1KEEP, [pA]
	prfm	PLDL1KEEP, [pA, #64]
	prfm	PLDL1KEEP, [origPB]

.Lztrmm_kernel_L4_M4_END:
	subs	counterI, counterI, #1
	bne	.Lztrmm_kernel_L4_M4_20

.Lztrmm_kernel_L4_M2_BEGIN:

	mov	counterI, origM
	tst	counterI , #3
	ble	.Lztrmm_kernel_L4_END

	tst	counterI, #2			// counterI = counterI / 2
	ble	.Lztrmm_kernel_L4_M1_BEGIN

.Lztrmm_kernel_L4_M2_20:

	INIT2x4

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	pB, origPB
#else
	mov	pB, origPB
	lsl	temp, tempOffset, #5
	add	pA, pA, temp
	lsl	temp, tempOffset, #6
	add	pB, pB, temp
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	tempK, origK, tempOffset
#elif defined(LEFT)
	add	tempK, tempOffset, #2
#else
	add	tempK, tempOffset, #4
#endif

	asr 	counterL , tempK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Lztrmm_kernel_L4_M2_40

.Lztrmm_kernel_L4_M2_22:

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lztrmm_kernel_L4_M2_22


.Lztrmm_kernel_L4_M2_40:

	ands	counterL , tempK, #7		// counterL = counterL % 8
	ble	.Lztrmm_kernel_L4_M2_100

.Lztrmm_kernel_L4_M2_42:

	KERNEL2x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lztrmm_kernel_L4_M2_42

.Lztrmm_kernel_L4_M2_100:

	SAVE2x4

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub	tempK, origK, tempOffset
#if defined(LEFT)
	sub	tempK, tempK, #2
#else
	sub	tempK, tempK, #4
#endif
	lsl	temp, tempK, #5
	add	pA, pA, temp
	lsl	temp, tempK, #6
	add	pB, pB, temp
#endif
#if defined(LEFT)
	add	tempOffset, tempOffset, #2
#endif

.Lztrmm_kernel_L4_M2_END:


.Lztrmm_kernel_L4_M1_BEGIN:

	tst	counterI, #1			// counterI = counterI % 2
	ble	.Lztrmm_kernel_L4_END

.Lztrmm_kernel_L4_M1_20:

	INIT1x4

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	pB, origPB
#else
	mov	pB, origPB
	lsl	temp, tempOffset, #6
	add	pB, pB, temp
	lsl	temp, tempOffset, #4
	add	pA, pA, temp
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	tempK, origK, tempOffset
#elif defined(LEFT)
	add	tempK, tempOffset, #1
#else
	add	tempK, tempOffset, #4
#endif

	asr 	counterL , tempK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Lztrmm_kernel_L4_M1_40

.Lztrmm_kernel_L4_M1_22:
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lztrmm_kernel_L4_M1_22


.Lztrmm_kernel_L4_M1_40:

	ands	counterL , tempK, #7		// counterL = counterL % 8
	ble	.Lztrmm_kernel_L4_M1_100

.Lztrmm_kernel_L4_M1_42:

	KERNEL1x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lztrmm_kernel_L4_M1_42

.Lztrmm_kernel_L4_M1_100:

	SAVE1x4

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub	tempK, origK, tempOffset
#if defined(LEFT)
	sub	tempK, tempK, #1
#else
	sub	tempK, tempK, #4
#endif
	lsl	temp, tempK, #4
	add	pA, pA, temp
	lsl	temp, tempK, #6
	add	pB, pB, temp
#endif
#if defined(LEFT)
	add	tempOffset, tempOffset, #1
#endif


.Lztrmm_kernel_L4_END:

	lsl	temp, origK, #6
	add	origPB, origPB, temp		// B = B + K * 4 * 8 * 2

#if !defined(LEFT)
	add	tempOffset, tempOffset, #4
#endif

	subs	counterJ, counterJ , #1		// j--
	bgt	.Lztrmm_kernel_L4_BEGIN


/******************************************************************************/

.Lztrmm_kernel_L2_BEGIN:   // less than 2 left in N direction

	mov	counterJ , origN
	tst	counterJ , #3
	ble	.Lztrmm_kernel_L999   // error, N was less than 4?

	tst	counterJ , #2
	ble	.Lztrmm_kernel_L1_BEGIN

	mov	pCRow0, pC			// pCRow0 = pC

	add	pC,pC,LDC, lsl #1

#if defined(LEFT)
	mov	tempOffset, offset
#endif

	mov	pA, origPA			// pA = A

.Lztrmm_kernel_L2_M4_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #2		// counterI = counterI / 4
	cmp	counterI,#0
	ble	.Lztrmm_kernel_L2_M2_BEGIN

.Lztrmm_kernel_L2_M4_20:

	INIT4x2

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	pB, origPB
#else
	mov	pB, origPB
	lsl	temp, tempOffset, #5
	add	pB, pB, temp
	lsl	temp, tempOffset, #6
	add	pA, pA, temp
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	tempK, origK, tempOffset
#elif defined(LEFT)
	add	tempK, tempOffset, #4
#else
	add	tempK, tempOffset, #2
#endif

	asr	counterL , tempK, #3		// counterL = counterL / 8
	cmp	counterL,#0
	ble	.Lztrmm_kernel_L2_M4_40
	.align 5

.Lztrmm_kernel_L2_M4_22:
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lztrmm_kernel_L2_M4_22


.Lztrmm_kernel_L2_M4_40:

	ands	counterL , tempK, #7		// counterL = counterL % 8
	ble	.Lztrmm_kernel_L2_M4_100

.Lztrmm_kernel_L2_M4_42:

	KERNEL4x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lztrmm_kernel_L2_M4_42

.Lztrmm_kernel_L2_M4_100:

	SAVE4x2

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub	tempK, origK, tempOffset
#if defined(LEFT)
	sub	tempK, tempK, #4
#else
	sub	tempK, tempK, #2
#endif
	lsl	temp, tempK, #6
	add	pA, pA, temp
	lsl	temp, tempK, #5
	add	pB, pB, temp
#endif
#if defined(LEFT)
	add	tempOffset, tempOffset, #4
#endif

.Lztrmm_kernel_L2_M4_END:

	subs	counterI, counterI, #1
	bgt	.Lztrmm_kernel_L2_M4_20


.Lztrmm_kernel_L2_M2_BEGIN:

	mov	counterI, origM
	tst	counterI , #3
	ble	.Lztrmm_kernel_L2_END

	tst	counterI, #2			// counterI = counterI / 2
	ble	.Lztrmm_kernel_L2_M1_BEGIN

.Lztrmm_kernel_L2_M2_20:

	INIT2x2

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	pB, origPB
#else
	mov	pB, origPB
	lsl	temp, tempOffset, #5
	add	pB, pB, temp
	lsl	temp, tempOffset, #5
	add	pA, pA, temp
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	tempK, origK, tempOffset
#elif defined(LEFT)
	add	tempK, tempOffset, #2
#else
	add	tempK, tempOffset, #2
#endif

	asr	counterL , tempK, #3		// counterL = counterL / 8
        cmp	counterL,#0
	ble	.Lztrmm_kernel_L2_M2_40

.Lztrmm_kernel_L2_M2_22:

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lztrmm_kernel_L2_M2_22


.Lztrmm_kernel_L2_M2_40:

	ands	counterL , tempK, #7		// counterL = counterL % 8
	ble	.Lztrmm_kernel_L2_M2_100

.Lztrmm_kernel_L2_M2_42:

	KERNEL2x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lztrmm_kernel_L2_M2_42

.Lztrmm_kernel_L2_M2_100:

	SAVE2x2

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub	tempK, origK, tempOffset
#if defined(LEFT)
	sub	tempK, tempK, #2
#else
	sub	tempK, tempK, #2
#endif
	lsl	temp, tempK, #5
	add	pA, pA, temp
	lsl	temp, tempK, #5
	add	pB, pB, temp
#endif
#if defined(LEFT)
	add	tempOffset, tempOffset, #2
#endif

.Lztrmm_kernel_L2_M2_END:


.Lztrmm_kernel_L2_M1_BEGIN:

	tst	counterI, #1			// counterI = counterI % 2
	ble	.Lztrmm_kernel_L2_END

.Lztrmm_kernel_L2_M1_20:

	INIT1x2

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	pB, origPB
#else
	mov	pB, origPB
	lsl	temp, tempOffset, #5
	add	pB, pB, temp
	lsl	temp, tempOffset, #4
	add	pA, pA, temp
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	tempK, origK, tempOffset
#elif defined(LEFT)
	add	tempK, tempOffset, #1
#else
	add	tempK, tempOffset, #2
#endif

	asr 	counterL , tempK, #3		// counterL = counterL / 8
        cmp     counterL, #0
	ble	.Lztrmm_kernel_L2_M1_40

.Lztrmm_kernel_L2_M1_22:
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lztrmm_kernel_L2_M1_22


.Lztrmm_kernel_L2_M1_40:

	ands	counterL , tempK, #7		// counterL = counterL % 8
	ble	.Lztrmm_kernel_L2_M1_100

.Lztrmm_kernel_L2_M1_42:

	KERNEL1x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lztrmm_kernel_L2_M1_42

.Lztrmm_kernel_L2_M1_100:

	SAVE1x2

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub	tempK, origK, tempOffset
#if defined(LEFT)
	sub	tempK, tempK, #1
#else
	sub	tempK, tempK, #2
#endif
	lsl	temp, tempK, #4
	add	pA, pA, temp
	lsl	temp, tempK, #5
	add	pB, pB, temp
#endif
#if defined(LEFT)
	add	tempOffset, tempOffset, #1
#endif


.Lztrmm_kernel_L2_END:
#if !defined(LEFT)
	add	tempOffset, tempOffset, #2
#endif

	lsl	temp, origK, #5
	add	origPB, origPB, temp // B = B + K * 2 * 8 * 2

/******************************************************************************/

.Lztrmm_kernel_L1_BEGIN:

	mov	counterJ , origN
	tst	counterJ , #1
	ble	.Lztrmm_kernel_L999 // done


	mov	pCRow0, pC			// pCRow0 = C
	add	pC , pC , LDC			// Update pC to point to next

#if defined(LEFT)
	mov	tempOffset, offset
#endif

	mov	pA, origPA			// pA = A



.Lztrmm_kernel_L1_M4_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #2		// counterI = counterI / 4
	cmp	counterI, #0
	ble	.Lztrmm_kernel_L1_M2_BEGIN

.Lztrmm_kernel_L1_M4_20:

	INIT4x1

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	pB, origPB
#else
	mov	pB, origPB
	lsl	temp, tempOffset, #4
	add	pB, pB, temp
	lsl	temp, tempOffset, #6
	add	pA, pA, temp
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	tempK, origK, tempOffset
#elif defined(LEFT)
	add	tempK, tempOffset, #4
#else
	add	tempK, tempOffset, #1
#endif

	asr	counterL , tempK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Lztrmm_kernel_L1_M4_40
	.align 5

.Lztrmm_kernel_L1_M4_22:
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lztrmm_kernel_L1_M4_22


.Lztrmm_kernel_L1_M4_40:

	ands	counterL , tempK, #7		// counterL = counterL % 8
	ble	.Lztrmm_kernel_L1_M4_100

.Lztrmm_kernel_L1_M4_42:

	KERNEL4x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lztrmm_kernel_L1_M4_42

.Lztrmm_kernel_L1_M4_100:

	SAVE4x1

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub	tempK, origK, tempOffset
#if defined(LEFT)
	sub	tempK, tempK, #4
#else
	sub	tempK, tempK, #1
#endif
	lsl	temp, tempK, #6
	add	pA, pA, temp
	lsl	temp, tempK, #4
	add	pB, pB, temp
#endif
#if defined(LEFT)
	add	tempOffset, tempOffset, #4
#endif

.Lztrmm_kernel_L1_M4_END:

	subs	counterI, counterI, #1
	bgt	.Lztrmm_kernel_L1_M4_20


.Lztrmm_kernel_L1_M2_BEGIN:

	mov	counterI, origM
	tst	counterI , #3
	ble	.Lztrmm_kernel_L1_END

	tst	counterI, #2			// counterI = counterI / 2
	ble	.Lztrmm_kernel_L1_M1_BEGIN

.Lztrmm_kernel_L1_M2_20:

	INIT2x1

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	pB, origPB
#else
	mov	pB, origPB
	lsl	temp, tempOffset, #4
	add	pB, pB, temp
	lsl	temp, tempOffset, #5
	add	pA, pA, temp
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	tempK, origK, tempOffset
#elif defined(LEFT)
	add	tempK, tempOffset, #2
#else
	add	tempK, tempOffset, #1
#endif

	asr 	counterL , tempK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Lztrmm_kernel_L1_M2_40

.Lztrmm_kernel_L1_M2_22:

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lztrmm_kernel_L1_M2_22


.Lztrmm_kernel_L1_M2_40:

	ands	counterL , tempK, #7		// counterL = counterL % 8
	ble	.Lztrmm_kernel_L1_M2_100

.Lztrmm_kernel_L1_M2_42:

	KERNEL2x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lztrmm_kernel_L1_M2_42

.Lztrmm_kernel_L1_M2_100:

	SAVE2x1

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub	tempK, origK, tempOffset
#if defined(LEFT)
	sub	tempK, tempK, #2
#else
	sub	tempK, tempK, #1
#endif
	lsl	temp, tempK, #5
	add	pA, pA, temp
	lsl	temp, tempK, #4
	add	pB, pB, temp
#endif
#if defined(LEFT)
	add	tempOffset, tempOffset, #2
#endif

.Lztrmm_kernel_L1_M2_END:


.Lztrmm_kernel_L1_M1_BEGIN:

	tst	counterI, #1			// counterI = counterI % 2
	ble	.Lztrmm_kernel_L1_END

.Lztrmm_kernel_L1_M1_20:

	INIT1x1

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	pB, origPB
#else
	mov	pB, origPB
	lsl	temp, tempOffset, #4
	add	pB, pB, temp
	lsl	temp, tempOffset, #4
	add	pA, pA, temp
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub	tempK, origK, tempOffset
#elif defined(LEFT)
	add	tempK, tempOffset, #1
#else
	add	tempK, tempOffset, #1
#endif

	asr 	counterL , tempK, #3		// counterL = counterL / 8
	cmp	counterL , #0
	ble	.Lztrmm_kernel_L1_M1_40

.Lztrmm_kernel_L1_M1_22:
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lztrmm_kernel_L1_M1_22


.Lztrmm_kernel_L1_M1_40:

	ands	counterL , tempK, #7		// counterL = counterL % 8
	ble	.Lztrmm_kernel_L1_M1_100

.Lztrmm_kernel_L1_M1_42:

	KERNEL1x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lztrmm_kernel_L1_M1_42

.Lztrmm_kernel_L1_M1_100:

	SAVE1x1


.Lztrmm_kernel_L1_END:


.Lztrmm_kernel_L999:
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

