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
#define pA		x15
#define ppC		x16
#define ppA		x17

#define alpha0_R	s10
#define alphaV0_R	v10.s[0]
#define alpha0_I	s11
#define alphaV0_I	v11.s[0]

#define alpha1_R	s14
#define alphaV1_R	v14.s[0]
#define alpha1_I	s15
#define alphaV1_I	v15.s[0]

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
// 15 pA
// 16 ppC
// 17 ppA
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

//v00 ALPHA_R -> pA00_R, pA01_R, pA02_R, pA03_R
//v01 ALPHA_I -> pA00_I, pA01_I, pA02_I, pA03_I
//v02 ppA00_R, ppA01_R, ppA02_R, ppA03_R
//v03 ppA00_I, ppA01_I, ppA02_I, ppA03_I
//v04 pA10_R, pA11_R, pA12_R, pA13_R
//v05 pA10_I, pA11_I, pA12_I, pA13_I
//v06 ppA10_R, ppA11_R, ppA12_R, ppA13_R
//v07 ppA10_I, ppA11_I, ppA12_I, ppA13_I
//v08 must save pB00_R, pB01_R, pB02_R, pB03_R
//v09 must save pB00_I, pB01_I, pB02_I, pB03_I
//v10 must save ALPHA0_R
//v11 must save ALPHA0_I
//v12 must save pB10_R, pB11_R, pB12_R, pB13_R
//v13 must save pB10_I, pB11_I, pB12_I, pB13_I
//v14 must save ALPHA1_R
//v15 must save ALPHA1_I
//v16 must save pC00_R, pC01_R, pC02_R, pC03_R
//v17 must save pC00_I, pC01_I, pC02_I, pC03_I
//v18 ppC00_R, ppC01_R, ppC02_R, ppC03_R
//v19 ppC00_I, ppC01_I, ppC02_I, ppC03_I
//v20 pC10_R, pC11_R, pC12_R, pC13_R
//v21 pC10_I, pC11_I, pC12_I, pC13_I
//v22 ppC10_R, ppC11_R, ppC12_R, ppC13_R
//v23 ppC10_I, ppC11_I, ppC12_I, ppC13_I
//v24 pC20_R, pC21_R, pC22_R, pC23_R
//v25 pC20_I, pC21_I, pC22_I, pC23_I
//v26 ppC20_R, ppC21_R, ppC22_R, ppC23_R
//v27 ppC20_I, ppC21_I, ppC22_I, ppC23_I
//v28 pC30_R, pC31_R, pC32_R, pC33_R
//v29 pC30_I, pC31_I, pC32_I, pC33_I
//v30 ppC30_R, ppC31_R, ppC32_R, ppC33_R
//v31 ppC30_I, ppC31_I, ppC32_I, ppC33_I

/*******************************************************************************
* Macro definitions
*******************************************************************************/

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

.macro KERNEL8x4_I
	ld2	{v8.4s, v9.4s}, [pB]
	add	pB, pB, #32
	ld2	{v0.4s, v1.4s}, [pA]
	add	pA, pA, #32
	ld2	{v2.4s, v3.4s}, [ppA]
	add	ppA, ppA, #32

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

	fmul	v18.4s, v2.4s, v8.s[0]
	OP_ii	v18.4s, v3.4s, v9.s[0]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v19.16b, v19.16b, v19.16b
	fmls	v19.4s, v2.4s, v9.s[0]
#else
	fmul	v19.4s, v2.4s, v9.s[0]
#endif
	OP_ir	v19.4s, v3.4s, v8.s[0]

	fmul	v22.4s, v2.4s, v8.s[1]
	OP_ii	v22.4s, v3.4s, v9.s[1]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v23.16b, v23.16b, v23.16b
	fmls	v23.4s, v2.4s, v9.s[1]
#else
	fmul	v23.4s, v2.4s, v9.s[1]
#endif
	OP_ir	v23.4s, v3.4s, v8.s[1]

	fmul	v26.4s, v2.4s, v8.s[2]
	OP_ii	v26.4s, v3.4s, v9.s[2]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v27.16b, v27.16b, v27.16b
	fmls	v27.4s, v2.4s, v9.s[2]
#else
	fmul	v27.4s, v2.4s, v9.s[2]
#endif
	OP_ir	v27.4s, v3.4s, v8.s[2]

	fmul	v30.4s, v2.4s, v8.s[3]
	OP_ii	v30.4s, v3.4s, v9.s[3]
#if defined(NR) || defined(NC) || defined(TR) || defined(TC) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	eor	v31.16b, v31.16b, v31.16b
	fmls	v31.4s, v2.4s, v9.s[3]
#else
	fmul	v31.4s, v2.4s, v9.s[3]
#endif
	OP_ir	v31.4s, v3.4s, v8.s[3]

	ld2	{v12.4s, v13.4s}, [pB]
	add	pB, pB, #32
	ld2	{v4.4s, v5.4s} , [pA]
	add	pA, pA, #32
	ld2	{v6.4s, v7.4s} , [ppA]
	add	ppA, ppA, #32
.endm

.macro KERNEL8x4_M1
	OP_rr	v16.4s, v0.4s, v8.s[0]
	OP_ii	v16.4s, v1.4s, v9.s[0]
	OP_ri	v17.4s, v0.4s, v9.s[0]
	OP_ir	v17.4s, v1.4s, v8.s[0]

	ld2	{v12.4s, v13.4s}, [pB]		// for next round
	add	pB, pB, #32

	OP_rr	v20.4s, v0.4s, v8.s[1]
	OP_ii	v20.4s, v1.4s, v9.s[1]
	OP_ri	v21.4s, v0.4s, v9.s[1]
	OP_ir	v21.4s, v1.4s, v8.s[1]

	prfm	PLDL1KEEP, [pB, #512]

	OP_rr	v24.4s, v0.4s, v8.s[2]
	OP_ii	v24.4s, v1.4s, v9.s[2]
	OP_ri	v25.4s, v0.4s, v9.s[2]
	OP_ir	v25.4s, v1.4s, v8.s[2]

	ld2	{v4.4s, v5.4s} , [pA]		// for next round
	add	pA, pA, #32

	OP_rr	v28.4s, v0.4s, v8.s[3]
	OP_ii	v28.4s, v1.4s, v9.s[3]
	OP_ri	v29.4s, v0.4s, v9.s[3]
	OP_ir	v29.4s, v1.4s, v8.s[3]

	prfm	PLDL1KEEP, [pA, #512]

	OP_rr	v18.4s, v2.4s, v8.s[0]
	OP_ii	v18.4s, v3.4s, v9.s[0]
	OP_ri	v19.4s, v2.4s, v9.s[0]
	OP_ir	v19.4s, v3.4s, v8.s[0]

	ld2	{v6.4s, v7.4s} , [ppA]		// for next round
	add	ppA, ppA, #32

	OP_rr	v22.4s, v2.4s, v8.s[1]
	OP_ii	v22.4s, v3.4s, v9.s[1]
	OP_ri	v23.4s, v2.4s, v9.s[1]
	OP_ir	v23.4s, v3.4s, v8.s[1]

	prfm	PLDL1KEEP, [ppA, #512]

	OP_rr	v26.4s, v2.4s, v8.s[2]
	OP_ii	v26.4s, v3.4s, v9.s[2]
	OP_ri	v27.4s, v2.4s, v9.s[2]
	OP_ir	v27.4s, v3.4s, v8.s[2]

	OP_rr	v30.4s, v2.4s, v8.s[3]
	OP_ii	v30.4s, v3.4s, v9.s[3]
	OP_ri	v31.4s, v2.4s, v9.s[3]
	OP_ir	v31.4s, v3.4s, v8.s[3]
.endm

.macro KERNEL8x4_M2
	OP_rr	v16.4s, v4.4s, v12.s[0]
	OP_ii	v16.4s, v5.4s, v13.s[0]
	OP_ri	v17.4s, v4.4s, v13.s[0]
	OP_ir	v17.4s, v5.4s, v12.s[0]

	ld2	{v8.4s, v9.4s}, [pB]		// for next round
	add	pB, pB, #32

	OP_rr	v20.4s, v4.4s, v12.s[1]
	OP_ii	v20.4s, v5.4s, v13.s[1]
	OP_ri	v21.4s, v4.4s, v13.s[1]
	OP_ir	v21.4s, v5.4s, v12.s[1]

	prfm	PLDL1KEEP, [pA, #512]

	OP_rr	v24.4s, v4.4s, v12.s[2]
	OP_ii	v24.4s, v5.4s, v13.s[2]
	OP_ri	v25.4s, v4.4s, v13.s[2]
	OP_ir	v25.4s, v5.4s, v12.s[2]

	ld2	{v0.4s, v1.4s}, [pA]		// for next round
	add	pA, pA, #32

	OP_rr	v28.4s, v4.4s, v12.s[3]
	OP_ii	v28.4s, v5.4s, v13.s[3]
	OP_ri	v29.4s, v4.4s, v13.s[3]
	OP_ir	v29.4s, v5.4s, v12.s[3]

	prfm	PLDL1KEEP, [ppA, #512]

	OP_rr	v18.4s, v6.4s, v12.s[0]
	OP_ii	v18.4s, v7.4s, v13.s[0]
	OP_ri	v19.4s, v6.4s, v13.s[0]
	OP_ir	v19.4s, v7.4s, v12.s[0]

	ld2	{v2.4s, v3.4s}, [ppA]		// for next round
	add	ppA, ppA, #32

	OP_rr	v22.4s, v6.4s, v12.s[1]
	OP_ii	v22.4s, v7.4s, v13.s[1]
	OP_ri	v23.4s, v6.4s, v13.s[1]
	OP_ir	v23.4s, v7.4s, v12.s[1]

	prfm	PLDL1KEEP, [pB, #512]

	OP_rr	v26.4s, v6.4s, v12.s[2]
	OP_ii	v26.4s, v7.4s, v13.s[2]
	OP_ri	v27.4s, v6.4s, v13.s[2]
	OP_ir	v27.4s, v7.4s, v12.s[2]

	OP_rr	v30.4s, v6.4s, v12.s[3]
	OP_ii	v30.4s, v7.4s, v13.s[3]
	OP_ri	v31.4s, v6.4s, v13.s[3]
	OP_ir	v31.4s, v7.4s, v12.s[3]
.endm

.macro KERNEL8x4_E
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

	OP_rr	v18.4s, v6.4s, v12.s[0]
	OP_ii	v18.4s, v7.4s, v13.s[0]
	OP_ri	v19.4s, v6.4s, v13.s[0]
	OP_ir	v19.4s, v7.4s, v12.s[0]

	OP_rr	v22.4s, v6.4s, v12.s[1]
	OP_ii	v22.4s, v7.4s, v13.s[1]
	OP_ri	v23.4s, v6.4s, v13.s[1]
	OP_ir	v23.4s, v7.4s, v12.s[1]

	OP_rr	v26.4s, v6.4s, v12.s[2]
	OP_ii	v26.4s, v7.4s, v13.s[2]
	OP_ri	v27.4s, v6.4s, v13.s[2]
	OP_ir	v27.4s, v7.4s, v12.s[2]

	OP_rr	v30.4s, v6.4s, v12.s[3]
	OP_ii	v30.4s, v7.4s, v13.s[3]
	OP_ri	v31.4s, v6.4s, v13.s[3]
	OP_ir	v31.4s, v7.4s, v12.s[3]
.endm

.macro KERNEL8x4_SUB
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

	ld2	{v2.4s, v3.4s}, [ppA]
	add	ppA, ppA, #32

	OP_rr	v24.4s, v0.4s, v8.s[2]
	OP_ii	v24.4s, v1.4s, v9.s[2]
	OP_ri	v25.4s, v0.4s, v9.s[2]
	OP_ir	v25.4s, v1.4s, v8.s[2]

	OP_rr	v28.4s, v0.4s, v8.s[3]
	OP_ii	v28.4s, v1.4s, v9.s[3]
	OP_ri	v29.4s, v0.4s, v9.s[3]
	OP_ir	v29.4s, v1.4s, v8.s[3]

	OP_rr	v18.4s, v2.4s, v8.s[0]
	OP_ii	v18.4s, v3.4s, v9.s[0]
	OP_ri	v19.4s, v2.4s, v9.s[0]
	OP_ir	v19.4s, v3.4s, v8.s[0]

	OP_rr	v22.4s, v2.4s, v8.s[1]
	OP_ii	v22.4s, v3.4s, v9.s[1]
	OP_ri	v23.4s, v2.4s, v9.s[1]
	OP_ir	v23.4s, v3.4s, v8.s[1]

	OP_rr	v26.4s, v2.4s, v8.s[2]
	OP_ii	v26.4s, v3.4s, v9.s[2]
	OP_ri	v27.4s, v2.4s, v9.s[2]
	OP_ir	v27.4s, v3.4s, v8.s[2]

	OP_rr	v30.4s, v2.4s, v8.s[3]
	OP_ii	v30.4s, v3.4s, v9.s[3]
	OP_ri	v31.4s, v2.4s, v9.s[3]
	OP_ir	v31.4s, v3.4s, v8.s[3]
.endm

.macro SAVE8x4
	mov	pCRow1, pCRow0

	add	pCRow2, pCRow1, #32

	ld2	{v0.4s, v1.4s}, [pCRow1]
	fmla	v0.4s, v16.4s, alphaV0_R
	fmls	v0.4s, v17.4s, alphaV0_I
	fmla	v1.4s, v16.4s, alphaV1_I
	fmla	v1.4s, v17.4s, alphaV1_R
	st2 	{v0.4s, v1.4s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v2.4s, v3.4s}, [pCRow2]
	fmla	v2.4s, v18.4s, alphaV0_R
	fmls	v2.4s, v19.4s, alphaV0_I
	fmla	v3.4s, v18.4s, alphaV1_I
	fmla	v3.4s, v19.4s, alphaV1_R
	st2 	{v2.4s, v3.4s}, [pCRow2]

	add	pCRow2, pCRow1, #32

	ld2	{v4.4s, v5.4s}, [pCRow1]
	fmla	v4.4s, v20.4s, alphaV0_R
	fmls	v4.4s, v21.4s, alphaV0_I
	fmla	v5.4s, v20.4s, alphaV1_I
	fmla	v5.4s, v21.4s, alphaV1_R
	st2 	{v4.4s, v5.4s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v6.4s, v7.4s}, [pCRow2]
	fmla	v6.4s, v22.4s, alphaV0_R
	fmls	v6.4s, v23.4s, alphaV0_I
	fmla	v7.4s, v22.4s, alphaV1_I
	fmla	v7.4s, v23.4s, alphaV1_R
	st2 	{v6.4s, v7.4s}, [pCRow2]

	add	pCRow2, pCRow1, #32

	ld2	{v0.4s, v1.4s}, [pCRow1]
	fmla	v0.4s, v24.4s, alphaV0_R
	fmls	v0.4s, v25.4s, alphaV0_I
	fmla	v1.4s, v24.4s, alphaV1_I
	fmla	v1.4s, v25.4s, alphaV1_R
	st2 	{v0.4s, v1.4s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v2.4s, v3.4s}, [pCRow2]
	fmla	v2.4s, v26.4s, alphaV0_R
	fmls	v2.4s, v27.4s, alphaV0_I
	fmla	v3.4s, v26.4s, alphaV1_I
	fmla	v3.4s, v27.4s, alphaV1_R
	st2 	{v2.4s, v3.4s}, [pCRow2]

	add	pCRow2, pCRow1, #32

	ld2	{v4.4s, v5.4s}, [pCRow1]
	fmla	v4.4s, v28.4s, alphaV0_R
	fmls	v4.4s, v29.4s, alphaV0_I
	fmla	v5.4s, v28.4s, alphaV1_I
	fmla	v5.4s, v29.4s, alphaV1_R
	st2 	{v4.4s, v5.4s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v6.4s, v7.4s}, [pCRow2]
	fmla	v6.4s, v30.4s, alphaV0_R
	fmls	v6.4s, v31.4s, alphaV0_I
	fmla	v7.4s, v30.4s, alphaV1_I
	fmla	v7.4s, v31.4s, alphaV1_R
	st2 	{v6.4s, v7.4s}, [pCRow2]

	add	pCRow0, pCRow0, #64
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
	mov	pCRow1, pCRow0

	ld2	{v0.4s, v1.4s}, [pCRow1]
	fmla	v0.4s, v16.4s, alphaV0_R
	fmls	v0.4s, v17.4s, alphaV0_I
	fmla	v1.4s, v16.4s, alphaV1_I
	fmla	v1.4s, v17.4s, alphaV1_R
	st2 	{v0.4s, v1.4s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v4.4s, v5.4s}, [pCRow1]
	fmla	v4.4s, v20.4s, alphaV0_R
	fmls	v4.4s, v21.4s, alphaV0_I
	fmla	v5.4s, v20.4s, alphaV1_I
	fmla	v5.4s, v21.4s, alphaV1_R
	st2 	{v4.4s, v5.4s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v0.4s, v1.4s}, [pCRow1]
	fmla	v0.4s, v24.4s, alphaV0_R
	fmls	v0.4s, v25.4s, alphaV0_I
	fmla	v1.4s, v24.4s, alphaV1_I
	fmla	v1.4s, v25.4s, alphaV1_R
	st2 	{v0.4s, v1.4s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v4.4s, v5.4s}, [pCRow1]
	fmla	v4.4s, v28.4s, alphaV0_R
	fmls	v4.4s, v29.4s, alphaV0_I
	fmla	v5.4s, v28.4s, alphaV1_I
	fmla	v5.4s, v29.4s, alphaV1_R
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
	mov	pCRow1, pCRow0

	ld2	{v0.2s, v1.2s}, [pCRow1]
	fmla	v0.2s, v16.2s, alphaV0_R
	fmls	v0.2s, v17.2s, alphaV0_I
	fmla	v1.2s, v16.2s, alphaV1_I
	fmla	v1.2s, v17.2s, alphaV1_R
	st2 	{v0.2s, v1.2s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v4.2s, v5.2s}, [pCRow1]
	fmla	v4.2s, v20.2s, alphaV0_R
	fmls	v4.2s, v21.2s, alphaV0_I
	fmla	v5.2s, v20.2s, alphaV1_I
	fmla	v5.2s, v21.2s, alphaV1_R
	st2 	{v4.2s, v5.2s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v0.2s, v1.2s}, [pCRow1]
	fmla	v0.2s, v24.2s, alphaV0_R
	fmls	v0.2s, v25.2s, alphaV0_I
	fmla	v1.2s, v24.2s, alphaV1_I
	fmla	v1.2s, v25.2s, alphaV1_R
	st2 	{v0.2s, v1.2s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v4.2s, v5.2s}, [pCRow1]
	fmla	v4.2s, v28.2s, alphaV0_R
	fmls	v4.2s, v29.2s, alphaV0_I
	fmla	v5.2s, v28.2s, alphaV1_I
	fmla	v5.2s, v29.2s, alphaV1_R
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
	mov	pCRow1, pCRow0

	ld2	{v0.s, v1.s}[0], [pCRow1]
	fmla	s0, s16, alphaV0_R
	fmls	s0, s17, alphaV0_I
	fmla	s1, s16, alphaV1_I
	fmla	s1, s17, alphaV1_R
	st2	{v0.s, v1.s}[0], [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v4.s, v5.s}[0], [pCRow1]
	fmla	s4, s20, alphaV0_R
	fmls	s4, s21, alphaV0_I
	fmla	s5, s20, alphaV1_I
	fmla	s5, s21, alphaV1_R
	st2 	{v4.s, v5.s}[0], [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v0.s, v1.s}[0], [pCRow1]
	fmla	s0, s24, alphaV0_R
	fmls	s0, s25, alphaV0_I
	fmla	s1, s24, alphaV1_I
	fmla	s1, s25, alphaV1_R
	st2	{v0.s, v1.s}[0], [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v4.s, v5.s}[0], [pCRow1]
	fmla	s4, s28, alphaV0_R
	fmls	s4, s29, alphaV0_I
	fmla	s5, s28, alphaV1_I
	fmla	s5, s29, alphaV1_R
	st2	{v4.s, v5.s}[0], [pCRow1]

	add	pCRow0, pCRow0, #8
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
	mov	pCRow1, pCRow0

	ld2	{v0.4s, v1.4s}, [pCRow1]
	fmla	v0.4s, v16.4s, alphaV0_R
	fmls	v0.4s, v17.4s, alphaV0_I
	fmla	v1.4s, v16.4s, alphaV1_I
	fmla	v1.4s, v17.4s, alphaV1_R
	st2 	{v0.4s, v1.4s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v4.4s, v5.4s}, [pCRow1]
	fmla	v4.4s, v20.4s, alphaV0_R
	fmls	v4.4s, v21.4s, alphaV0_I
	fmla	v5.4s, v20.4s, alphaV1_I
	fmla	v5.4s, v21.4s, alphaV1_R
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
	mov	pCRow1, pCRow0

	ld2	{v0.2s, v1.2s}, [pCRow1]
	fmla	v0.2s, v16.2s, alphaV0_R
	fmls	v0.2s, v17.2s, alphaV0_I
	fmla	v1.2s, v16.2s, alphaV1_I
	fmla	v1.2s, v17.2s, alphaV1_R
	st2 	{v0.2s, v1.2s}, [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v4.2s, v5.2s}, [pCRow1]
	fmla	v4.2s, v20.2s, alphaV0_R
	fmls	v4.2s, v21.2s, alphaV0_I
	fmla	v5.2s, v20.2s, alphaV1_I
	fmla	v5.2s, v21.2s, alphaV1_R
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
	mov	pCRow1, pCRow0

	ld2	{v0.s, v1.s}[0], [pCRow1]
	fmla	s0, s16, alphaV0_R
	fmls	s0, s17, alphaV0_I
	fmla	s1, s16, alphaV1_I
	fmla	s1, s17, alphaV1_R
	st2	{v0.s, v1.s}[0], [pCRow1]

	add	pCRow1, pCRow1, LDC

	ld2	{v4.s, v5.s}[0], [pCRow1]
	fmla	s4, s20, alphaV0_R
	fmls	s4, s21, alphaV0_I
	fmla	s5, s20, alphaV1_I
	fmla	s5, s21, alphaV1_R
	st2 	{v4.s, v5.s}[0], [pCRow1]

	add	pCRow0, pCRow0, #8
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
	mov	pCRow1, pCRow0

	ld2	{v0.4s, v1.4s}, [pCRow1]
	fmla	v0.4s, v16.4s, alphaV0_R
	fmls	v0.4s, v17.4s, alphaV0_I
	fmla	v1.4s, v16.4s, alphaV1_I
	fmla	v1.4s, v17.4s, alphaV1_R
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
	mov	pCRow1, pCRow0

	ld2	{v0.2s, v1.2s}, [pCRow1]
	fmla	v0.2s, v16.2s, alphaV0_R
	fmls	v0.2s, v17.2s, alphaV0_I
	fmla	v1.2s, v16.2s, alphaV1_I
	fmla	v1.2s, v17.2s, alphaV1_R
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
	mov	pCRow1, pCRow0

	ld2	{v0.s, v1.s}[0], [pCRow1]
	fmla	s0, s16, alphaV0_R
	fmls	s0, s17, alphaV0_I
	fmla	s1, s16, alphaV1_I
	fmla	s1, s17, alphaV1_R
	st2	{v0.s, v1.s}[0], [pCRow1]

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

	fmov	alpha0_R, s0
	fmov	alpha0_I, s1
	fmov	alpha1_R, s0
	fmov	alpha1_I, s1

	lsl	LDC, LDC, #3			// ldc = ldc * 8

	mov	pB, origPB

	mov	counterJ, origN
	asr 	counterJ, counterJ, #2		// J = J / 4
	cmp 	counterJ, #0
	ble	.Lcgemm_kernel_L2_BEGIN

/******************************************************************************/

.Lcgemm_kernel_L4_BEGIN:
	mov	pCRow0, pC			// pCRow0 = C
	add	pC, pC, LDC, lsl #2

	lsl	temp, origK, #5			// k * 4 * 8
	mov	pA, origPA			// pA = start of A array
	add	ppA, temp, pA

.Lcgemm_kernel_L4_M8_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #3		// counterI = counterI / 8
	cmp 	counterI, #0
	ble	.Lcgemm_kernel_L4_M4_BEGIN

.Lcgemm_kernel_L4_M8_20:

	mov	pB, origPB
	asr 	counterL , origK, #1		// L = K / 2
	cmp	counterL , #2			// is there at least 4 to do?
	blt	.Lcgemm_kernel_L4_M8_32

	KERNEL8x4_I				// do one in the K
	KERNEL8x4_M2				// do another in the K

	subs	counterL, counterL, #2		// subtract 2
	ble	.Lcgemm_kernel_L4_M8_22a
	.align 5

.Lcgemm_kernel_L4_M8_22:

	KERNEL8x4_M1
	KERNEL8x4_M2

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L4_M8_22


.Lcgemm_kernel_L4_M8_22a:

	KERNEL8x4_M1
	KERNEL8x4_E

	b	 .Lcgemm_kernel_L4_M8_44

.Lcgemm_kernel_L4_M8_32:

	tst	counterL, #1
	ble	.Lcgemm_kernel_L4_M8_40

	KERNEL8x4_I
	KERNEL8x4_E

	b	.Lcgemm_kernel_L4_M8_44


.Lcgemm_kernel_L4_M8_40:

	INIT8x4

.Lcgemm_kernel_L4_M8_44:

	ands	counterL , origK, #1
	ble	.Lcgemm_kernel_L4_M8_100

.Lcgemm_kernel_L4_M8_46:
	KERNEL8x4_SUB

.Lcgemm_kernel_L4_M8_100:

	SAVE8x4

.Lcgemm_kernel_L4_M8_END:
	lsl	temp, origK, #5			// k * 4 * 8
	add	pA, pA, temp
	add	ppA, ppA, temp
	subs	counterI, counterI, #1
	bne	.Lcgemm_kernel_L4_M8_20


.Lcgemm_kernel_L4_M4_BEGIN:
	mov	counterI, origM
	tst	counterI , #7
	ble	.Lcgemm_kernel_L4_END

	tst	counterI, #4
	ble	.Lcgemm_kernel_L4_M2_BEGIN

.Lcgemm_kernel_L4_M4_20:

	INIT4x4

	mov	pB, origPB
	asr 	counterL, origK, #3		// counterL = counterL / 8
	cmp	counterL, #0
	ble	.Lcgemm_kernel_L4_M4_40

.Lcgemm_kernel_L4_M4_22:

	KERNEL4x4_SUB
	KERNEL4x4_SUB
	KERNEL4x4_SUB
	KERNEL4x4_SUB

	KERNEL4x4_SUB
	KERNEL4x4_SUB
	KERNEL4x4_SUB
	KERNEL4x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L4_M4_22


.Lcgemm_kernel_L4_M4_40:

	ands	counterL , origK, #7		// counterL = counterL % 8
	ble	.Lcgemm_kernel_L4_M4_100

.Lcgemm_kernel_L4_M4_42:

	KERNEL4x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lcgemm_kernel_L4_M4_42

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



.Lcgemm_kernel_L2_M4_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #2		// counterI = counterI / 4
	cmp	counterI,#0
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

	subs	counterI, counterI, #1
	bgt	.Lcgemm_kernel_L2_M4_20


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



.Lcgemm_kernel_L1_M4_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #2		// counterI = counterI / 4
	cmp	counterI, #0
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

	subs	counterI, counterI, #1
	bgt	.Lcgemm_kernel_L1_M4_20


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

