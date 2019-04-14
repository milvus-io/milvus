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
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A00 PARTICULAR PURPOSE
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

#define	M	x0
#define	N	x1
#define	A00	x2
#define	LDA	x3
#define	B00	x4

#define	A01	x5
#define	A02	x6
#define	A03	x7
#define	A04	x8
#define	A05	x9
#define	A06	x10
#define	A07	x11
#define	A08	x12

#define I	x13
#define	J	x14

#define	TEMP1	x15
#define	TEMP2	x16

#define A_PREFETCH	2560

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

/*************************************************************************************/

.macro COPY8x8
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A04, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A05, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A06, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A07, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A08, #A_PREFETCH]

	COPY4x8
	COPY4x8
.endm

.macro COPY4x8
	ldp	q0, q1, [A01], #32
	ins	v16.d[0], v0.d[0]
	ins	v20.d[0], v0.d[1]
	ins	v24.d[0], v1.d[0]
	ins	v28.d[0], v1.d[1]

	ldp	q2, q3, [A02], #32
	ins	v16.d[1], v2.d[0]
	ins	v20.d[1], v2.d[1]
	ins	v24.d[1], v3.d[0]
	ins	v28.d[1], v3.d[1]

	ldp	q4, q5, [A03], #32
	ins	v17.d[0], v4.d[0]
	ins	v21.d[0], v4.d[1]
	ins	v25.d[0], v5.d[0]
	ins	v29.d[0], v5.d[1]

	ldp	q6, q7, [A04], #32
	ins	v17.d[1], v6.d[0]
	ins	v21.d[1], v6.d[1]
	ins	v25.d[1], v7.d[0]
	ins	v29.d[1], v7.d[1]

	ldp	q8, q9, [A05], #32
	ins	v18.d[0], v8.d[0]
	ins	v22.d[0], v8.d[1]
	ins	v26.d[0], v9.d[0]
	ins	v30.d[0], v9.d[1]

	ldp	q10, q11, [A06], #32
	ins	v18.d[1], v10.d[0]
	ins	v22.d[1], v10.d[1]
	ins	v26.d[1], v11.d[0]
	ins	v30.d[1], v11.d[1]

	ldp	q12, q13, [A07], #32
	ins	v19.d[0], v12.d[0]
	ins	v23.d[0], v12.d[1]
	ins	v27.d[0], v13.d[0]
	ins	v31.d[0], v13.d[1]

	ldp	q14, q15, [A08], #32
	ins	v19.d[1], v14.d[0]
	ins	v23.d[1], v14.d[1]
	ins	v27.d[1], v15.d[0]
	ins	v31.d[1], v15.d[1]

	st1	{v16.2d, v17.2d, v18.2d, v19.2d}, [B00]
	add	B00, B00, #64

	st1	{v20.2d, v21.2d, v22.2d, v23.2d}, [B00]
	add	B00, B00, #64

	st1	{v24.2d, v25.2d, v26.2d, v27.2d}, [B00]
	add	B00, B00, #64

	st1	{v28.2d, v29.2d, v30.2d, v31.2d}, [B00]
	add	B00, B00, #64
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
	ldr	d4, [A05], #8
	ldr	d5, [A06], #8
	ldr	d6, [A07], #8
	ldr	d7, [A08], #8

	st1	{v0.1d, v1.1d, v2.1d, v3.1d}, [B00]
	add	B00, B00, #32
	st1	{v4.1d, v5.1d, v6.1d, v7.1d}, [B00]
	add	B00, B00, #32

.endm


/*************************************************************************************/

.macro COPY8x4
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A04, #A_PREFETCH]

	ldp	q0, q1, [A01], #32
	ins	v8.d[0], v0.d[0]
	ins	v10.d[0], v0.d[1]
	ins	v12.d[0], v1.d[0]
	ins	v14.d[0], v1.d[1]

	ldp	q2, q3, [A02], #32
	ins	v8.d[1], v2.d[0]
	ins	v10.d[1], v2.d[1]
	ins	v12.d[1], v3.d[0]
	ins	v14.d[1], v3.d[1]

	ldp	q4, q5, [A03], #32
	ins	v9.d[0], v4.d[0]
	ins	v11.d[0], v4.d[1]
	ins	v13.d[0], v5.d[0]
	ins	v15.d[0], v5.d[1]

	ldp	q6, q7, [A04], #32
	ins	v9.d[1], v6.d[0]
	ins	v11.d[1], v6.d[1]
	ins	v13.d[1], v7.d[0]
	ins	v15.d[1], v7.d[1]

	st1	{v8.2d, v9.2d, v10.2d, v11.2d}, [B00]
	add	B00, B00, #64

	st1	{v12.2d, v13.2d, v14.2d, v15.2d}, [B00]
	add	B00, B00, #64

	ldp	q16, q17, [A01], #32
	ins	v24.d[0], v16.d[0]
	ins	v26.d[0], v16.d[1]
	ins	v28.d[0], v17.d[0]
	ins	v30.d[0], v17.d[1]

	ldp	q18, q19, [A02], #32
	ins	v24.d[1], v18.d[0]
	ins	v26.d[1], v18.d[1]
	ins	v28.d[1], v19.d[0]
	ins	v30.d[1], v19.d[1]

	ldp	q20, q21, [A03], #32
	ins	v25.d[0], v20.d[0]
	ins	v27.d[0], v20.d[1]
	ins	v29.d[0], v21.d[0]
	ins	v31.d[0], v21.d[1]

	ldp	q22, q23, [A04], #32
	ins	v25.d[1], v22.d[0]
	ins	v27.d[1], v22.d[1]
	ins	v29.d[1], v23.d[0]
	ins	v31.d[1], v23.d[1]

	st1	{v24.2d, v25.2d, v26.2d, v27.2d}, [B00]
	add	B00, B00, #64

	st1	{v28.2d, v29.2d, v30.2d, v31.2d}, [B00]
	add	B00, B00, #64
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

	st1	{v0.1d, v1.1d, v2.1d, v3.1d}, [B00]
	add	B00, B00, #32
.endm

/*************************************************************************************/

.macro COPY8x2
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ldp	q0, q1, [A01], #32
	ldp	q2, q3, [A01], #32

	ins	v8.d[0], v0.d[0]
	ins	v9.d[0], v0.d[1]
	ins	v10.d[0], v1.d[0]
	ins	v11.d[0], v1.d[1]
	ins	v12.d[0], v2.d[0]
	ins	v13.d[0], v2.d[1]
	ins	v14.d[0], v3.d[0]
	ins	v15.d[0], v3.d[1]

	ldp	q4, q5, [A02], #32
	ldp	q6, q7, [A02], #32

	ins	v8.d[1], v4.d[0]
	ins	v9.d[1], v4.d[1]
	ins	v10.d[1], v5.d[0]
	ins	v11.d[1], v5.d[1]
	ins	v12.d[1], v6.d[0]
	ins	v13.d[1], v6.d[1]
	ins	v14.d[1], v7.d[0]
	ins	v15.d[1], v7.d[1]

	st1	{v8.2d, v9.2d, v10.2d, v11.2d}, [B00]
	add	B00, B00, #64
	st1	{v12.2d, v13.2d, v14.2d, v15.2d}, [B00]
	add	B00, B00, #64
.endm


.macro COPY1x2
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ldr	d0, [A01], #8
	ldr	d1, [A02], #8

	stp	d0, d1, [B00]
	add	B00, B00, #16
.endm

/*************************************************************************************/

.macro COPY8x1
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]

	ldp	q0, q1, [A01], #32
	ldp	q2, q3, [A01], #32
	stp	q0, q1, [B00], #32
	stp	q2, q3, [B00], #32
.endm


.macro COPY1x1
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]

	ldr	d0, [A01], #8
	str	d0, [B00], #8
.endm

/**************************************************************************************
* End of macro definitions
**************************************************************************************/

	PROLOGUE

	.align 5

	SAVE_REGS

	lsl	LDA, LDA, #3					// LDA = LDA * SIZE

.Ldgemm_ncopy_L8_BEGIN:

	asr	J, N, #3					// J = N / 8
	cmp 	J, #0
	ble	.Ldgemm_ncopy_L4_BEGIN

.Ldgemm_ncopy_L8_M8_BEGIN:

	mov	A01, A00
	add	A02, A01, LDA
	add	A03, A02, LDA
	add	A04, A03, LDA
	add	A05, A04, LDA
	add	A06, A05, LDA
	add	A07, A06, LDA
	add	A08, A07, LDA
	add	A00, A08, LDA


	asr	I, M, #3					// I = M / 8
	cmp	I, #0
	ble	.Ldgemm_ncopy_L8_M8_40

.Ldgemm_ncopy_L8_M8_20:

	COPY8x8

	subs	I , I , #1
	bne	.Ldgemm_ncopy_L8_M8_20


.Ldgemm_ncopy_L8_M8_40:

	and	I, M , #7
	cmp	I, #0
	ble	.Ldgemm_ncopy_L8_M8_END

.Ldgemm_ncopy_L8_M8_60:

	COPY1x8

	subs	I , I , #1
	bne	.Ldgemm_ncopy_L8_M8_60


.Ldgemm_ncopy_L8_M8_END:

	subs	J , J, #1						// j--
	bne	.Ldgemm_ncopy_L8_M8_BEGIN

/*********************************************************************************************/

.Ldgemm_ncopy_L4_BEGIN:

	tst	N, #7
	ble	.Ldgemm_ncopy_L999

	tst	N, #4
	ble	.Ldgemm_ncopy_L2_BEGIN

.Ldgemm_ncopy_L4_M8_BEGIN:

	mov	A01, A00
	add	A02, A01, LDA
	add	A03, A02, LDA
	add	A04, A03, LDA
	add	A00, A04, LDA

	asr	I, M, #3					// I = M / 8
	cmp	I, #0
	ble	.Ldgemm_ncopy_L4_M8_40

.Ldgemm_ncopy_L4_M8_20:

	COPY8x4

	subs	I , I , #1
	bne	.Ldgemm_ncopy_L4_M8_20


.Ldgemm_ncopy_L4_M8_40:

	and	I, M , #7
	cmp	I, #0
	ble	.Ldgemm_ncopy_L4_M8_END

.Ldgemm_ncopy_L4_M8_60:

	COPY1x4

	subs	I , I , #1
	bne	.Ldgemm_ncopy_L4_M8_60


.Ldgemm_ncopy_L4_M8_END:


/*********************************************************************************************/

.Ldgemm_ncopy_L2_BEGIN:

	tst	N, #3
	ble	.Ldgemm_ncopy_L999

	tst	N, #2
	ble	.Ldgemm_ncopy_L1_BEGIN

.Ldgemm_ncopy_L2_M8_BEGIN:
	mov	A01, A00
	add	A02, A01, LDA
	add	A00, A02, LDA

	asr	I, M, #3					// I = M / 8
	cmp 	I, #0
	ble	.Ldgemm_ncopy_L2_M8_40

.Ldgemm_ncopy_L2_M8_20:

	COPY8x2

	subs	I , I , #1
	bne	.Ldgemm_ncopy_L2_M8_20


.Ldgemm_ncopy_L2_M8_40:

	and	I, M , #7
	cmp	I, #0
	ble	.Ldgemm_ncopy_L2_M8_END

.Ldgemm_ncopy_L2_M8_60:

	COPY1x2

	subs	I , I , #1
	bne	.Ldgemm_ncopy_L2_M8_60


.Ldgemm_ncopy_L2_M8_END:


/*********************************************************************************************/

.Ldgemm_ncopy_L1_BEGIN:

	tst	N, #1
	ble	.Ldgemm_ncopy_L999


.Ldgemm_ncopy_L1_M8_BEGIN:

	mov	A01, A00

	asr	I, M, #3					// I = M / 8
	cmp	I, #0
	ble	.Ldgemm_ncopy_L1_M8_40

.Ldgemm_ncopy_L1_M8_20:

	COPY8x1

	subs	I , I , #1
	bne	.Ldgemm_ncopy_L1_M8_20


.Ldgemm_ncopy_L1_M8_40:

	and	I, M , #7
	cmp	I, #0
	ble	.Ldgemm_ncopy_L1_M8_END

.Ldgemm_ncopy_L1_M8_60:

	COPY1x1

	subs	I , I , #1
	bne	.Ldgemm_ncopy_L1_M8_60


.Ldgemm_ncopy_L1_M8_END:

.Ldgemm_ncopy_L999:

	mov	x0, #0
	RESTORE_REGS
	ret

	EPILOGUE

