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

/*                   X0          X1          X2          s0        X3        x4       x5           x6               x7 */
/*int CNAME(BLASLONG bm,BLASLONG bn,BLASLONG bk,FLOAT alpha,FLOAT* ba,FLOAT* bb,FLOAT* C,BLASLONG ldc, BLASLONG offset) */

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
#define pA		x15
#define temp		x16
#define tempOffset	x17
#define tempK		x18

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
// 17 tempOffset
// 18 must save tempK
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
//v02
//v03
//v04 pA10, pA11
//v05 pA12, pA13
//v06
//v07
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
//v18
//v19
//v20 C10, C11
//v21 C12, C13
//v22
//v23
//v24 C20, C21
//v25 C22, C23
//v26
//v27
//v28 C30, C31
//v29 C32, C33
//v30
//v31

/*******************************************************************************
* Macro definitions
*******************************************************************************/

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
	ld1	{v8.2s, v9.2s}, [pB]
	add	pB, pB, #16
	ld1	{v0.2s, v1.2s}, [pA]
	add	pA, pA, #16

	fmul	v16.2s, v0.2s, v8.s[0]
	fmul	v29.2s, v1.2s, v9.s[1]

	fmul	v20.2s, v0.2s, v8.s[1]
	fmul	v25.2s, v1.2s, v9.s[0]

	fmul	v24.2s, v0.2s, v9.s[0]
	fmul	v21.2s, v1.2s, v8.s[1]

	fmul	v28.2s, v0.2s, v9.s[1]
	fmul	v17.2s, v1.2s, v8.s[0]

	ld1	{v12.2s, v13.2s}, [pB]
	add	pB, pB, #16
	ld1	{v4.2s, v5.2s}, [pA]
	add	pA, pA, #16
.endm

.macro KERNEL4x4_M1
	fmla	v16.2s, v0.2s, v8.s[0]
	fmla	v29.2s, v1.2s, v9.s[1]

	ld1	{v12.2s, v13.2s}, [pB]		// For next round
	add	pB, pB, #16

	fmla	v20.2s, v0.2s, v8.s[1]
	fmla	v25.2s, v1.2s, v9.s[0]

	ld1	{v4.2s, v5.2s}, [pA]		// For next round
	add	pA, pA, #16

	fmla	v24.2s, v0.2s, v9.s[0]
	fmla	v21.2s, v1.2s, v8.s[1]

	prfm	PLDL1KEEP, [pB, #512]

	fmla	v28.2s, v0.2s, v9.s[1]
	fmla	v17.2s, v1.2s, v8.s[0]
.endm

.macro KERNEL4x4_M2
	fmla	v16.2s, v4.2s, v12.s[0]
	fmla	v29.2s, v5.2s, v13.s[1]

	ld1	{v8.2s, v9.2s}, [pB]		// For next round
	add	pB, pB, #16

	fmla	v20.2s, v4.2s, v12.s[1]
	fmla	v25.2s, v5.2s, v13.s[0]

	ld1	{v0.2s, v1.2s}, [pA]		// For next round
	add	pA, pA, #16

	fmla	v24.2s, v4.2s, v13.s[0]
	fmla	v21.2s, v5.2s, v12.s[1]

	prfm	PLDL1KEEP, [pA, #512]

	fmla	v28.2s, v4.2s, v13.s[1]
	fmla	v17.2s, v5.2s, v12.s[0]
.endm

.macro KERNEL4x4_E
	fmla	v16.2s, v4.2s, v12.s[0]
	fmla	v29.2s, v5.2s, v13.s[1]

	fmla	v20.2s, v4.2s, v12.s[1]
	fmla	v25.2s, v5.2s, v13.s[0]

	fmla	v24.2s, v4.2s, v13.s[0]
	fmla	v21.2s, v5.2s, v12.s[1]

	fmla	v28.2s, v4.2s, v13.s[1]
	fmla	v17.2s, v5.2s, v12.s[0]
.endm

.macro KERNEL4x4_SUB
	ld1	{v8.2s, v9.2s}, [pB]
	add	pB, pB, #16
	ld1	{v0.2s, v1.2s}, [pA]
	add	pA, pA, #16

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
	fmul	v8.2s, v16.2s, alphaV0
	fmul	v9.2s, v17.2s, alphaV1
	st1 	{v8.2s, v9.2s}, [pCRow0]

	add	pCRow1, pCRow0, LDC
	fmul	v12.2s, v20.2s, alphaV2
	fmul	v13.2s, v21.2s, alphaV3
	st1 	{v12.2s, v13.2s}, [pCRow1]

	add	pCRow2, pCRow1, LDC
	fmul	v8.2s, v24.2s, alphaV0
	fmul	v9.2s, v25.2s, alphaV1
	st1 	{v8.2s, v9.2s}, [pCRow2]

	add	pCRow1, pCRow2, LDC
	fmul	v12.2s, v28.2s, alphaV2
	fmul	v13.2s, v29.2s, alphaV3
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
	ld1	{v0.2s}, [pA]
	add	pA, pA, #8

	fmla	v16.2s, v0.2s, v8.s[0]
	fmla	v20.2s, v0.2s, v8.s[1]
	fmla	v24.2s, v0.2s, v9.s[0]
	fmla	v28.2s, v0.2s, v9.s[1]
.endm

.macro SAVE2x4
	fmul	v8.2s, v16.2s, alphaV0
	st1	{v8.2s}, [pCRow0]

	add	pCRow1, pCRow0, LDC
	fmul	v12.2s, v20.2s, alphaV1
	st1	{v12.2s}, [pCRow1]

	add	pCRow2, pCRow1, LDC
	fmul	v8.2s, v24.2s, alphaV2
	st1	{v8.2s}, [pCRow2]

	add	pCRow1, pCRow2, LDC
	fmul	v12.2s, v28.2s, alphaV3
	st1	{v12.2s}, [pCRow1]

	add	pCRow0, pCRow0, #8
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
	add	pCRow1, pCRow0, LDC
	fmul	v8.2s, v16.2s, alphaV0
	st1	{v8.s}[0], [pCRow0]
	st1	{v8.s}[1], [pCRow1]

	add	pCRow2, pCRow1, LDC
	add	pCRow1, pCRow2, LDC
	fmul	v12.2s, v20.2s, alphaV1
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
	ld1	{v0.2s, v1.2s}, [pA]
	add	pA, pA, #16

	fmla	v16.2s, v0.2s, v8.s[0]
	fmla	v17.2s, v1.2s, v8.s[0]
	fmla	v20.2s, v0.2s, v8.s[1]
	fmla	v21.2s, v1.2s, v8.s[1]
.endm

.macro SAVE4x2
	fmul	v8.2s, v16.2s, alphaV0
	fmul	v9.2s, v17.2s, alphaV1
	st1	{v8.2s, v9.2s}, [pCRow0]

	add	pCRow1, pCRow0, LDC
	fmul	v12.2s, v20.2s, alphaV2
	fmul	v13.2s, v21.2s, alphaV3
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
	fmul	v8.2s, v16.2s, alphaV0
	st1	{v8.2s}, [pCRow0]

	add	pCRow1 , pCRow0, LDC
	fmul	v12.2s, v20.2s, alphaV1
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
	add	pCRow1 , pCRow0, LDC
	fmul	v8.2s, v16.2s, alphaV0
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

	ld1	{v0.2s, v1.2s}, [pA]
	add	pA , pA, #16

	fmla	v16.2s, v0.2s, v8.s[0]
	fmla	v17.2s, v1.2s, v8.s[0]
.endm

.macro SAVE4x1
	fmul	v8.2s, v16.2s, alphaV0
	fmul	v9.2s, v17.2s, alphaV1
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
	fmul	v8.2s, v16.2s, alphaV0
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
	fmul	s8, s16, alpha0
	str 	s8, [pCRow0]

	add	pCRow0, pCRow0, #4
.endm

/*******************************************************************************
* End of macro definitions
*******************************************************************************/

	PROLOGUE

.Lstrmm_kernel_begin:

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

#if !defined(LEFT)
	neg	tempOffset, offset
#endif

	mov	pB, origPB

	mov	counterJ, origN
	asr 	counterJ, counterJ, #2		// J = J / 4
	cmp 	counterJ, #0
	ble	.Lstrmm_kernel_L2_BEGIN

/******************************************************************************/

.Lstrmm_kernel_L4_BEGIN:
	mov	pCRow0, pC			// pCRow0 = C
	add	pC, pC, LDC, lsl #2

#if defined(LEFT)
	mov	tempOffset, offset
#endif

	mov	pA, origPA			// pA = start of A array

.Lstrmm_kernel_L4_M4_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #2		// counterI = counterI / 4
	cmp 	counterI, #0
	ble	.Lstrmm_kernel_L4_M2_BEGIN

.Lstrmm_kernel_L4_M4_20:

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	pB, origPB
#else
	mov	pB, origPB
	lsl	temp, tempOffset, #4
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

	asr 	counterL , tempK, #1		// L = K / 2
	cmp	counterL , #2			// is there at least 4 to do?
	blt	.Lstrmm_kernel_L4_M4_32

	KERNEL4x4_I				// do one in the K
	KERNEL4x4_M2				// do another in the K

	subs	counterL, counterL, #2
	ble	.Lstrmm_kernel_L4_M4_22a
	.align 5

.Lstrmm_kernel_L4_M4_22:

	KERNEL4x4_M1
	KERNEL4x4_M2

	subs	counterL, counterL, #1
	bgt	.Lstrmm_kernel_L4_M4_22

.Lstrmm_kernel_L4_M4_22a:

	KERNEL4x4_M1
	KERNEL4x4_E

	b	 .Lstrmm_kernel_L4_M4_44

.Lstrmm_kernel_L4_M4_32:

	tst	counterL, #1
	ble	.Lstrmm_kernel_L4_M4_40

	KERNEL4x4_I
	KERNEL4x4_E

	b	.Lstrmm_kernel_L4_M4_44

.Lstrmm_kernel_L4_M4_40:

	INIT4x4

.Lstrmm_kernel_L4_M4_44:

	ands	counterL , tempK, #1
	ble	.Lstrmm_kernel_L4_M4_100

.Lstrmm_kernel_L4_M4_46:

	KERNEL4x4_SUB

.Lstrmm_kernel_L4_M4_100:

	SAVE4x4

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub	tempK, origK, tempOffset
#if defined(LEFT)
	sub	tempK, tempK, #4
#else
	sub	tempK, tempK, #4
#endif
	lsl	temp, tempK, #4
	add	pA, pA, temp
	add	pB, pB, temp
#endif
#if defined(LEFT)
	add	tempOffset, tempOffset, #4
#endif

.Lstrmm_kernel_L4_M4_END:
	subs	counterI, counterI, #1
	bne	.Lstrmm_kernel_L4_M4_20

.Lstrmm_kernel_L4_M2_BEGIN:

	mov	counterI, origM
	tst	counterI , #3
	ble	.Lstrmm_kernel_L4_END

	tst	counterI, #2			// counterI = counterI / 2
	ble	.Lstrmm_kernel_L4_M1_BEGIN

.Lstrmm_kernel_L4_M2_20:

	INIT2x4

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	pB, origPB
#else
	mov	pB, origPB
	lsl	temp, tempOffset, #3
	add	pA, pA, temp
	lsl	temp, tempOffset, #4
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
	ble	.Lstrmm_kernel_L4_M2_40

.Lstrmm_kernel_L4_M2_22:

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lstrmm_kernel_L4_M2_22


.Lstrmm_kernel_L4_M2_40:

	ands	counterL , tempK, #7		// counterL = counterL % 8
	ble	.Lstrmm_kernel_L4_M2_100

.Lstrmm_kernel_L4_M2_42:

	KERNEL2x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lstrmm_kernel_L4_M2_42

.Lstrmm_kernel_L4_M2_100:

	SAVE2x4

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub	tempK, origK, tempOffset
#if defined(LEFT)
	sub	tempK, tempK, #2
#else
	sub	tempK, tempK, #4
#endif
	lsl	temp, tempK, #3
	add	pA, pA, temp
	lsl	temp, tempK, #4
	add	pB, pB, temp
#endif
#if defined(LEFT)
	add	tempOffset, tempOffset, #2
#endif


.Lstrmm_kernel_L4_M2_END:


.Lstrmm_kernel_L4_M1_BEGIN:

	tst	counterI, #1			// counterI = counterI % 2
	ble	.Lstrmm_kernel_L4_END

.Lstrmm_kernel_L4_M1_20:

	INIT1x4

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	pB, origPB
#else
	mov	pB, origPB
	lsl	temp, tempOffset, #4
	add	pB, pB, temp
	lsl	temp, tempOffset, #2
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
	ble	.Lstrmm_kernel_L4_M1_40

.Lstrmm_kernel_L4_M1_22:
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lstrmm_kernel_L4_M1_22


.Lstrmm_kernel_L4_M1_40:

	ands	counterL , tempK, #7		// counterL = counterL % 8
	ble	.Lstrmm_kernel_L4_M1_100

.Lstrmm_kernel_L4_M1_42:

	KERNEL1x4_SUB

	subs	counterL, counterL, #1
	bgt	.Lstrmm_kernel_L4_M1_42

.Lstrmm_kernel_L4_M1_100:

	SAVE1x4

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub	tempK, origK, tempOffset
#if defined(LEFT)
	sub	tempK, tempK, #1
#else
	sub	tempK, tempK, #4
#endif
	lsl	temp, tempK, #2
	add	pA, pA, temp
	lsl	temp, tempK, #4
	add	pB, pB, temp
#endif
#if defined(LEFT)
	add	tempOffset, tempOffset, #1
#endif


.Lstrmm_kernel_L4_END:
	add	origPB, origPB, origK, lsl #4	// B = B + K * 4 * 4

#if !defined(LEFT)
	add	tempOffset, tempOffset, #4
#endif

	subs	counterJ, counterJ , #1		// j--
	bgt	.Lstrmm_kernel_L4_BEGIN


/******************************************************************************/

.Lstrmm_kernel_L2_BEGIN:   // less than 2 left in N direction

	mov	counterJ , origN
	tst	counterJ , #3
	ble	.Lstrmm_kernel_L999

	tst	counterJ , #2
	ble	.Lstrmm_kernel_L1_BEGIN

	mov	pCRow0, pC			// pCRow0 = pC

	add	pC,pC,LDC, lsl #1

#if defined(LEFT)
	mov	tempOffset, offset
#endif

	mov	pA, origPA			// pA = A

.Lstrmm_kernel_L2_M4_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #2		// counterI = counterI / 4
	cmp	counterI,#0
	ble	.Lstrmm_kernel_L2_M2_BEGIN

.Lstrmm_kernel_L2_M4_20:

	INIT4x2

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	pB, origPB
#else
	mov	pB, origPB
	lsl	temp, tempOffset, #3
	add	pB, pB, temp
	lsl	temp, tempOffset, #4
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
	ble	.Lstrmm_kernel_L2_M4_40
	.align 5

.Lstrmm_kernel_L2_M4_22:
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lstrmm_kernel_L2_M4_22


.Lstrmm_kernel_L2_M4_40:

	ands	counterL , tempK, #7		// counterL = counterL % 8
	ble	.Lstrmm_kernel_L2_M4_100

.Lstrmm_kernel_L2_M4_42:

	KERNEL4x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lstrmm_kernel_L2_M4_42

.Lstrmm_kernel_L2_M4_100:

	SAVE4x2

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub	tempK, origK, tempOffset
#if defined(LEFT)
	sub	tempK, tempK, #4
#else
	sub	tempK, tempK, #2
#endif
	lsl	temp, tempK, #4
	add	pA, pA, temp
	lsl	temp, tempK, #3
	add	pB, pB, temp
#endif
#if defined(LEFT)
	add	tempOffset, tempOffset, #4
#endif

.Lstrmm_kernel_L2_M4_END:

	subs	counterI, counterI, #1
	bgt	.Lstrmm_kernel_L2_M4_20


.Lstrmm_kernel_L2_M2_BEGIN:

	mov	counterI, origM
	tst	counterI , #3
	ble	.Lstrmm_kernel_L2_END

	tst	counterI, #2			// counterI = counterI / 2
	ble	.Lstrmm_kernel_L2_M1_BEGIN

.Lstrmm_kernel_L2_M2_20:

	INIT2x2

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	pB, origPB
#else
	mov	pB, origPB
	lsl	temp, tempOffset, #3
	add	pB, pB, temp
	lsl	temp, tempOffset, #3
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
	ble	.Lstrmm_kernel_L2_M2_40

.Lstrmm_kernel_L2_M2_22:

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lstrmm_kernel_L2_M2_22


.Lstrmm_kernel_L2_M2_40:

	ands	counterL , tempK, #7		// counterL = counterL % 8
	ble	.Lstrmm_kernel_L2_M2_100

.Lstrmm_kernel_L2_M2_42:

	KERNEL2x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lstrmm_kernel_L2_M2_42

.Lstrmm_kernel_L2_M2_100:

	SAVE2x2
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub	tempK, origK, tempOffset
#if defined(LEFT)
	sub	tempK, tempK, #2
#else
	sub	tempK, tempK, #2
#endif
	lsl	temp, tempK, #3
	add	pA, pA, temp
	lsl	temp, tempK, #3
	add	pB, pB, temp
#endif
#if defined(LEFT)
	add	tempOffset, tempOffset, #2
#endif

.Lstrmm_kernel_L2_M2_END:


.Lstrmm_kernel_L2_M1_BEGIN:

	tst	counterI, #1			// counterI = counterI % 2
	ble	.Lstrmm_kernel_L2_END

.Lstrmm_kernel_L2_M1_20:

	INIT1x2

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	pB, origPB
#else
	mov	pB, origPB
	lsl	temp, tempOffset, #3
	add	pB, pB, temp
	lsl	temp, tempOffset, #2
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
	ble	.Lstrmm_kernel_L2_M1_40

.Lstrmm_kernel_L2_M1_22:
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lstrmm_kernel_L2_M1_22


.Lstrmm_kernel_L2_M1_40:

	ands	counterL , tempK, #7		// counterL = counterL % 8
	ble	.Lstrmm_kernel_L2_M1_100

.Lstrmm_kernel_L2_M1_42:

	KERNEL1x2_SUB

	subs	counterL, counterL, #1
	bgt	.Lstrmm_kernel_L2_M1_42

.Lstrmm_kernel_L2_M1_100:

	SAVE1x2

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub	tempK, origK, tempOffset
#if defined(LEFT)
	sub	tempK, tempK, #1
#else
	sub	tempK, tempK, #2
#endif
	lsl	temp, tempK, #2
	add	pA, pA, temp
	lsl	temp, tempK, #3
	add	pB, pB, temp
#endif
#if defined(LEFT)
	add	tempOffset, tempOffset, #1
#endif

.Lstrmm_kernel_L2_END:
#if !defined(LEFT)
	add	tempOffset, tempOffset, #2
#endif
	add	origPB, origPB, origK, lsl #3	// B = B + K * 2 * 4

/******************************************************************************/

.Lstrmm_kernel_L1_BEGIN:

	mov	counterJ , origN
	tst	counterJ , #1
	ble	.Lstrmm_kernel_L999 // done


	mov	pCRow0, pC			// pCRow0 = C
	add	pC , pC , LDC			// Update pC to point to next

#if defined(LEFT)
	mov	tempOffset, offset
#endif

	mov	pA, origPA			// pA = A

.Lstrmm_kernel_L1_M4_BEGIN:

	mov	counterI, origM
	asr 	counterI, counterI, #2		// counterI = counterI / 4
	cmp	counterI, #0
	ble	.Lstrmm_kernel_L1_M2_BEGIN

.Lstrmm_kernel_L1_M4_20:

	INIT4x1

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	pB, origPB
#else
	mov	pB, origPB
	lsl	temp, tempOffset, #2
	add	pB, pB, temp
	lsl	temp, tempOffset, #4
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
	ble	.Lstrmm_kernel_L1_M4_40
	.align 5

.Lstrmm_kernel_L1_M4_22:
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lstrmm_kernel_L1_M4_22


.Lstrmm_kernel_L1_M4_40:

	ands	counterL , tempK, #7		// counterL = counterL % 8
	ble	.Lstrmm_kernel_L1_M4_100

.Lstrmm_kernel_L1_M4_42:

	KERNEL4x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lstrmm_kernel_L1_M4_42

.Lstrmm_kernel_L1_M4_100:

	SAVE4x1

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub	tempK, origK, tempOffset
#if defined(LEFT)
	sub	tempK, tempK, #4
#else
	sub	tempK, tempK, #1
#endif
	lsl	temp, tempK, #4
	add	pA, pA, temp
	lsl	temp, tempK, #2
	add	pB, pB, temp
#endif
#if defined(LEFT)
	add	tempOffset, tempOffset, #4
#endif

.Lstrmm_kernel_L1_M4_END:

	subs	counterI, counterI, #1
	bgt	.Lstrmm_kernel_L1_M4_20


.Lstrmm_kernel_L1_M2_BEGIN:

	mov	counterI, origM
	tst	counterI , #3
	ble	.Lstrmm_kernel_L1_END

	tst	counterI, #2			// counterI = counterI / 2
	ble	.Lstrmm_kernel_L1_M1_BEGIN

.Lstrmm_kernel_L1_M2_20:

	INIT2x1

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	pB, origPB
#else
	mov	pB, origPB
	lsl	temp, tempOffset, #2
	add	pB, pB, temp
	lsl	temp, tempOffset, #3
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
	ble	.Lstrmm_kernel_L1_M2_40

.Lstrmm_kernel_L1_M2_22:

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lstrmm_kernel_L1_M2_22


.Lstrmm_kernel_L1_M2_40:

	ands	counterL , tempK, #7		// counterL = counterL % 8
	ble	.Lstrmm_kernel_L1_M2_100

.Lstrmm_kernel_L1_M2_42:

	KERNEL2x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lstrmm_kernel_L1_M2_42

.Lstrmm_kernel_L1_M2_100:

	SAVE2x1

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub	tempK, origK, tempOffset
#if defined(LEFT)
	sub	tempK, tempK, #2
#else
	sub	tempK, tempK, #1
#endif
	lsl	temp, tempK, #3
	add	pA, pA, temp
	lsl	temp, tempK, #2
	add	pB, pB, temp
#endif
#if defined(LEFT)
	add	tempOffset, tempOffset, #2
#endif


.Lstrmm_kernel_L1_M2_END:


.Lstrmm_kernel_L1_M1_BEGIN:

	tst	counterI, #1			// counterI = counterI % 2
	ble	.Lstrmm_kernel_L1_END

.Lstrmm_kernel_L1_M1_20:

	INIT1x1

#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mov	pB, origPB
#else
	mov	pB, origPB
	lsl	temp, tempOffset, #2
	add	pB, pB, temp
	lsl	temp, tempOffset, #2
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
	ble	.Lstrmm_kernel_L1_M1_40

.Lstrmm_kernel_L1_M1_22:
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lstrmm_kernel_L1_M1_22


.Lstrmm_kernel_L1_M1_40:

	ands	counterL , tempK, #7		// counterL = counterL % 8
	ble	.Lstrmm_kernel_L1_M1_100

.Lstrmm_kernel_L1_M1_42:

	KERNEL1x1_SUB

	subs	counterL, counterL, #1
	bgt	.Lstrmm_kernel_L1_M1_42

.Lstrmm_kernel_L1_M1_100:

	SAVE1x1

#if 0
#if (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub	tempK, origK, tempOffset
#if defined(LEFT)
	sub	tempK, tempK, #1
#else
	sub	tempK, tempK, #1
#endif
	lsl	temp, tempK, #2
	add	pA, pA, temp
	lsl	temp, tempK, #2
	add	pB, pB, temp
#endif
#if defined(LEFT)
	add	tempOffset, tempOffset, #1
#endif
#endif

.Lstrmm_kernel_L1_END:

#if 0
#if !defined(LEFT)
	add	tempOffset, tempOffset, #1
#endif
#endif

.Lstrmm_kernel_L999:
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

