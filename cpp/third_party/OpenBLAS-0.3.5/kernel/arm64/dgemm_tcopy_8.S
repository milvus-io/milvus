/***************************************************************************
Copyright (c) 2016, The OpenBLAS Project
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
*****************************************************************************/

#define ASSEMBLER
#include "common.h"

#define	M		x0
#define	N		x1
#define	A		x2
#define	LDA		x3
#define	B		x4

#define M8		x5

#define	A01		x6
#define	A02		x7
#define	A03		x8
#define	A04		x9
#define	A05		x10
#define	A06		x11
#define	A07		x12
#define	A08		x13

#define	B01		x14
#define	B02		x15
#define	B03		x16
#define	B04		x17

#define I		x18
#define	J		x19

#define TEMP1		x20
#define TEMP2		x21

#define A_PREFETCH	2560
#define B_PREFETCH	256

/**************************************************************************************
* Macro definitions
**************************************************************************************/
.macro SAVE_REGS
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
.endm

.macro RESTORE_REGS
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
.endm

/*************************************************************************************************************************/

.macro COPY8x8
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A04, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A05, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A06, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A07, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A08, #A_PREFETCH]

	ldp	q0, q1, [A01], #32
	ldp	q2, q3, [A01], #32

	st1	{v0.2d, v1.2d, v2.2d, v3.2d}, [B01]
	add	TEMP1, B01, #64

	ldp	q4, q5, [A02], #32
	ldp	q6, q7, [A02], #32

	st1	{v4.2d, v5.2d, v6.2d, v7.2d}, [TEMP1]
	add	TEMP1, TEMP1, #64

	ldp	q8, q9, [A03], #32
	ldp	q10, q11, [A03], #32

	st1	{v8.2d, v9.2d, v10.2d, v11.2d}, [TEMP1]
	add	TEMP1, TEMP1, #64

	ldp	q12, q13, [A04], #32
	ldp	q14, q15, [A04], #32

	st1	{v12.2d, v13.2d, v14.2d, v15.2d}, [TEMP1]
	add	TEMP1, TEMP1, #64

	ldp	q16, q17, [A05], #32
	ldp	q18, q19, [A05], #32

	st1	{v16.2d, v17.2d, v18.2d, v19.2d}, [TEMP1]
	add	TEMP1, TEMP1, #64

	ldp	q20, q21, [A06], #32
	ldp	q22, q23, [A06], #32

	st1	{v20.2d, v21.2d, v22.2d, v23.2d}, [TEMP1]
	add	TEMP1, TEMP1, #64

	ldp	q24, q25, [A07], #32
	ldp	q26, q27, [A07], #32

	st1	{v24.2d, v25.2d, v26.2d, v27.2d}, [TEMP1]
	add	TEMP1, TEMP1, #64

	ldp	q28, q29, [A08], #32
	ldp	q30, q31, [A08], #32

	st1	{v28.2d, v29.2d, v30.2d, v31.2d}, [TEMP1]
	add	TEMP1, TEMP1, #64

	add	B01, B01, M8
.endm

.macro COPY4x8
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A04, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A05, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A06, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A07, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A08, #A_PREFETCH]

	ldp	q0, q1, [A01], #32
	ldp	q2, q3, [A02], #32

	st1	{v0.2d, v1.2d, v2.2d, v3.2d}, [B02]
	add	B02, B02, #64

	ldp	q4, q5, [A03], #32
	ldp	q6, q7, [A04], #32

	st1	{v4.2d, v5.2d, v6.2d, v7.2d}, [B02]
	add	B02, B02, #64

	ldp	q8, q9, [A05], #32
	ldp	q10, q11, [A06], #32

	st1	{v8.2d, v9.2d, v10.2d, v11.2d}, [B02]
	add	B02, B02, #64

	ldp	q12, q13, [A07], #32
	ldp	q14, q15, [A08], #32

	st1	{v12.2d, v13.2d, v14.2d, v15.2d}, [B02]
	add	B02, B02, #64
.endm

.macro COPY2x8
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A04, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A05, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A06, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A07, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A08, #A_PREFETCH]

	ldr	q0, [A01], #16
	ldr	q1, [A02], #16
	ldr	q2, [A03], #16
	ldr	q3, [A04], #16

	st1	{v0.2d, v1.2d, v2.2d, v3.2d}, [B03]
	add	B03, B03, #64

	ldr	q4, [A05], #16
	ldr	q5, [A06], #16
	ldr	q6, [A07], #16
	ldr	q7, [A08], #16

	st1	{v4.2d, v5.2d, v6.2d, v7.2d}, [B03]
	add	B03, B03, #64
.endm

.macro COPY1x8
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A04, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A05, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A06, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A07, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A08, #A_PREFETCH]

	ldr	d0, [A01], #8
	ldr	d1, [A02], #8
	ldr	d2, [A03], #8
	ldr	d3, [A04], #8

	st1	{v0.1d, v1.1d, v2.1d, v3.1d}, [B04]
	add	B04, B04, #32

	ldr	d4, [A05], #8
	ldr	d5, [A06], #8
	ldr	d6, [A07], #8
	ldr	d7, [A08], #8

	st1	{v4.1d, v5.1d, v6.1d, v7.1d}, [B04]

	add	B04, B04, #32
.endm

/*************************************************************************************************************************/

.macro COPY8x4
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A04, #A_PREFETCH]

	ldp	q0, q1, [A01], #32
	ldp	q2, q3, [A01], #32

	st1	{v0.2d, v1.2d, v2.2d, v3.2d}, [B01]
	add	TEMP1, B01, #64

	ldp	q4, q5, [A02], #32
	ldp	q6, q7, [A02], #32

	st1	{v4.2d, v5.2d, v6.2d, v7.2d}, [TEMP1]
	add	TEMP1, TEMP1, #64

	ldp	q8, q9, [A03], #32
	ldp	q10, q11, [A03], #32

	st1	{v8.2d, v9.2d, v10.2d, v11.2d}, [TEMP1]
	add	TEMP1, TEMP1, #64

	ldp	q12, q13, [A04], #32
	ldp	q14, q15, [A04], #32

	st1	{v12.2d, v13.2d, v14.2d, v15.2d}, [TEMP1]
	add	TEMP1, TEMP1, #64

	add	B01, B01, M8
.endm

.macro COPY4x4
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A04, #A_PREFETCH]

	ldp	q0, q1, [A01], #32
	ldp	q2, q3, [A02], #32

	st1	{v0.2d, v1.2d, v2.2d, v3.2d}, [B02]
	add	B02, B02, #64

	ldp	q4, q5, [A03], #32
	ldp	q6, q7, [A04], #32

	st1	{v4.2d, v5.2d, v6.2d, v7.2d}, [B02]
	add	B02, B02, #64
.endm

.macro COPY2x4
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A04, #A_PREFETCH]

	ldr	q0, [A01], #16
	ldr	q1, [A02], #16
	ldr	q2, [A03], #16
	ldr	q3, [A04], #16

	st1	{v0.2d, v1.2d, v2.2d, v3.2d}, [B03]

	add	B03, B03, #64
.endm

.macro COPY1x4
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A04, #A_PREFETCH]

	ldr	d0, [A01], #8
	ldr	d1, [A02], #8
	ldr	d2, [A03], #8
	ldr	d3, [A04], #8

	st1	{v0.1d, v1.1d, v2.1d, v3.1d}, [B04]

	add	B04, B04, #32
.endm

/*************************************************************************************************************************/

.macro COPY8x2
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ldp	q0, q1, [A01], #32
	ldp	q2, q3, [A01], #32
	ldp	q4, q5, [A02], #32
	ldp	q6, q7, [A02], #32

	st1	{v0.2d, v1.2d, v2.2d, v3.2d}, [B01]
	add	TEMP1, B01, #64
	st1	{v4.2d, v5.2d, v6.2d, v7.2d}, [TEMP1]
	add	B01, B01, M8
.endm

.macro COPY4x2
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ldp	q0, q1, [A01], #32
	ldp	q2, q3, [A02], #32

	st1	{v0.2d, v1.2d, v2.2d, v3.2d}, [B02]
	add	B02, B02, #64
.endm

.macro COPY2x2
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ldr	q0, [A01], #16
	ldr	q1, [A02], #16

	stp	q0, q1, [B03]

	add	B03, B03, #32
.endm

.macro COPY1x2
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ldr	d0, [A01], #8
	ldr	d1, [A02], #8

	stp	d0, d1, [B04]

	add	B04, B04, #16
.endm

/*************************************************************************************************************************/

.macro COPY8x1
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]

	ldp	q0, q1, [A01], #32
	ldp	q2, q3, [A01], #32

	stp	q0, q1, [B01]
	add	TEMP1, B01, #32
	stp	q2, q3, [TEMP1]
	add	B01, B01, M8
.endm

.macro COPY4x1
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]

	ldp	q0, q1, [A01], #32
	stp	q0, q1, [B02]

	add	B02, B02, #32
.endm

.macro COPY2x1
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]

	ldr	q0, [A01], #16
	str	q0, [B03]

	add	B03, B03, #16
.endm

.macro COPY1x1
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]

	ldr	d0, [A01], #8
	str	d0, [B04]

	add	B04, B04, #8
.endm



/**************************************************************************************
* End of macro definitions
**************************************************************************************/

	PROLOGUE

	.align 5

	SAVE_REGS

	lsl	LDA, LDA, #3					// LDA = LDA * SIZE

	lsl	TEMP1, M, #3					// TEMP1 = M * SIZE

	and	B02 , N , #-8
	and	B03 , N , #-4
	and	B04 , N , #-2

	mul	B02, B02, TEMP1
	mul	B03, B03, TEMP1
	mul	B04, B04, TEMP1

	add	B02 , B02, B
	add	B03 , B03, B
	add	B04 , B04, B

	lsl	M8, M, #6					// M8 = M * 8 * SIZE

.Ldgemm_tcopy_L8_BEGIN:
	asr 	J, M, #3					// J = M / 4
	cmp 	J, #0
	ble	.Ldgemm_tcopy_L4_BEGIN

	.align	5
.Ldgemm_tcopy_L8_M8_BEGIN:

	mov	A01, A
	add	A02, A01, LDA
	add	A03, A02, LDA
	add	A04, A03, LDA
	add	A05, A04, LDA
	add	A06, A05, LDA
	add	A07, A06, LDA
	add	A08, A07, LDA
	add	A, A08, LDA

	mov	B01, B
	add	B, B01, #512					// B = B + 64 * SIZE

	asr	I, N, #3					// I = N / 8
	cmp 	I, #0
	ble	.Ldgemm_tcopy_L8_M8_40

	.align	5
.Ldgemm_tcopy_L8_M8_20:

	COPY8x8

	subs	I , I , #1
	bne	.Ldgemm_tcopy_L8_M8_20

.Ldgemm_tcopy_L8_M8_40:
	tst	N , #4
	ble	.Ldgemm_tcopy_L8_M8_60

	COPY4x8

.Ldgemm_tcopy_L8_M8_60:

	tst	N , #2
	ble	.Ldgemm_tcopy_L8_M8_80

	COPY2x8


.Ldgemm_tcopy_L8_M8_80:

	tst	N, #1
	ble	.Ldgemm_tcopy_L8_M8_END

	COPY1x8


.Ldgemm_tcopy_L8_M8_END:

	subs	J , J, #1						// j--
	bne	.Ldgemm_tcopy_L8_M8_BEGIN

/*********************************************************************************************/

.Ldgemm_tcopy_L4_BEGIN:
	tst	M, #7
	ble	.Ldgemm_tcopy_L999

	tst	M, #4
	ble	.Ldgemm_tcopy_L2_BEGIN

.Ldgemm_tcopy_L4_M8_BEGIN:

	mov	A01, A
	add	A02, A01, LDA
	add	A03, A02, LDA
	add	A04, A03, LDA
	add	A, A04, LDA

	mov	B01, B
	add	B, B01, #256					// B = B + 32 * SIZE

	asr	I, N, #3					// I = N / 8
	cmp 	I, #0
	ble	.Ldgemm_tcopy_L4_M8_40

	.align	5
.Ldgemm_tcopy_L4_M8_20:

	COPY8x4

	subs	I , I , #1
	bne	.Ldgemm_tcopy_L4_M8_20

.Ldgemm_tcopy_L4_M8_40:
	tst	N , #4
	ble	.Ldgemm_tcopy_L4_M8_60

	COPY4x4

.Ldgemm_tcopy_L4_M8_60:

	tst	N , #2
	ble	.Ldgemm_tcopy_L4_M8_80

	COPY2x4


.Ldgemm_tcopy_L4_M8_80:

	tst	N, #1
	ble	.Ldgemm_tcopy_L4_M8_END

	COPY1x4


.Ldgemm_tcopy_L4_M8_END:

/*********************************************************************************************/

.Ldgemm_tcopy_L2_BEGIN:

	tst	M, #3
	ble	.Ldgemm_tcopy_L999

	tst	M, #2
	ble	.Ldgemm_tcopy_L1_BEGIN

.Ldgemm_tcopy_L2_M8_BEGIN:
	mov	A01, A
	add	A02, A01, LDA
	add	A, A02, LDA

	mov	B01, B
	add	B, B01, #128					// B = B + 16 * SIZE

	asr	I, N, #3					// I = N / 8
	cmp 	I, #0
	ble	.Ldgemm_tcopy_L2_M8_40

	.align	5
.Ldgemm_tcopy_L2_M8_20:

	COPY8x2

	subs	I , I , #1
	bne	.Ldgemm_tcopy_L2_M8_20

.Ldgemm_tcopy_L2_M8_40:
	tst	N , #4
	ble	.Ldgemm_tcopy_L2_M8_60

	COPY4x2

.Ldgemm_tcopy_L2_M8_60:

	tst	N , #2
	ble	.Ldgemm_tcopy_L2_M8_80

	COPY2x2

.Ldgemm_tcopy_L2_M8_80:

	tst	N , #1
	ble	.Ldgemm_tcopy_L2_M8_END

	COPY1x2


.Ldgemm_tcopy_L2_M8_END:


/*********************************************************************************************/

.Ldgemm_tcopy_L1_BEGIN:

	tst	M, #1
	ble	.Ldgemm_tcopy_L999


.Ldgemm_tcopy_L1_M8_BEGIN:

	mov	A01, A						// A01 = A
	mov	B01, B

	asr	I, N, #3					// I = M / 8
	cmp 	I, #0
	ble	.Ldgemm_tcopy_L1_M8_40

	.align	5
.Ldgemm_tcopy_L1_M8_20:

	COPY8x1

	subs	I , I , #1
	bne	.Ldgemm_tcopy_L1_M8_20

.Ldgemm_tcopy_L1_M8_40:
	tst	N , #4
	ble	.Ldgemm_tcopy_L1_M8_60

	COPY4x1

.Ldgemm_tcopy_L1_M8_60:

	tst	N , #2
	ble	.Ldgemm_tcopy_L1_M8_80

	COPY2x1

.Ldgemm_tcopy_L1_M8_80:

	tst	N , #1
	ble	.Ldgemm_tcopy_L1_M8_END

	COPY1x1


.Ldgemm_tcopy_L1_M8_END:


.Ldgemm_tcopy_L999:
	mov	x0, #0						// set return value
	RESTORE_REGS
	ret

	EPILOGUE

