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

#define M4		x5

#define	A01		x6
#define	A02		x7
#define	A03		x8
#define	A04		x9

#define	B01		x10
#define	B02		x11
#define	B03		x12
#define	B04		x13

#define I		x14
#define	J		x15

#define TEMP1		x16
#define TEMP2		x17

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

.macro COPY4x4
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A03, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A04, #A_PREFETCH]

	ldp	q0, q1, [A01], #32
	ldp	q2, q3, [A02], #32

	////prfm	PLDL1KEEP, [B01, #B_PREFETCH]
	st1	{v0.2d, v1.2d, v2.2d, v3.2d}, [B01]
	add	TEMP1, B01, #64

	ldp	q4, q5, [A03], #32
	ldp	q6, q7, [A04], #32

	////prfm	PLDL1KEEP, [B01, #B_PREFETCH]
	st1	{v4.2d, v5.2d, v6.2d, v7.2d}, [TEMP1]

	add	B01, B01, M4
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

	////prfm	PLDL1KEEP, [B02, #B_PREFETCH]
	st1	{v0.2d, v1.2d, v2.2d, v3.2d}, [B02]

	add	B02, B02, #64
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

	////prfm	PLDL1KEEP, [B03, #B_PREFETCH]
	st1	{v0.1d, v1.1d, v2.1d, v3.1d}, [B03]

	add	B03, B03, #32
.endm

/*************************************************************************************************************************/

.macro COPY4x2
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ldp	q0, q1, [A01], #32
	ldp	q2, q3, [A02], #32

	////prfm	PLDL1KEEP, [B01, #B_PREFETCH]

	st1	{v0.2d, v1.2d, v2.2d, v3.2d}, [B01]
	add	B01, B01, M4
.endm

.macro COPY2x2
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ldr	q0, [A01], #16
	ldr	q1, [A02], #16

	////prfm	PLDL1KEEP, [B02, #B_PREFETCH]
	stp	q0, q1, [B02]

	add	B02, B02, #32
.endm

.macro COPY1x2
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ldr	d0, [A01], #8
	ldr	d1, [A02], #8

	////prfm	PLDL1KEEP, [B03, #B_PREFETCH]
	stp	d0, d1, [B03]

	add	B03, B03, #16
.endm

/*************************************************************************************************************************/

.macro COPY4x1
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]

	ldp	q0, q1, [A01], #32

	////prfm	PLDL1KEEP, [B01, #B_PREFETCH]
	stp	q0, q1, [B01]

	add	B01, B01, M4
.endm

.macro COPY2x1
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]

	ldr	q0, [A01], #16

	////prfm	PLDL1KEEP, [B02, #B_PREFETCH]
	str	q0, [B02]

	add	B02, B02, #16
.endm

.macro COPY1x1
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]

	ldr	d0, [A01], #8

	////prfm	PLDL1KEEP, [B03, #B_PREFETCH]
	str	d0, [B03]

	add	B03, B03, #8
.endm



/**************************************************************************************
* End of macro definitions
**************************************************************************************/

	PROLOGUE

	.align 5

	SAVE_REGS

	lsl	LDA, LDA, #3					// LDA = LDA * SIZE

	lsl	TEMP1, M, #3					// x12 = M * SIZE

	and	B02 , N , #-4
	and	B03 , N , #-2

	mul	B02, B02, TEMP1
	mul	B03, B03, TEMP1

	add	B02 , B02, B
	add	B03 , B03, B

	lsl	M4, M, #5					// M4 = M * 4 * SIZE

.Ldgemm_tcopy_L4_BEGIN:
	asr 	J, M, #2					// J = M / 4
	cmp 	J, #0
	ble	.Ldgemm_tcopy_L2_BEGIN

	.align	5
.Ldgemm_tcopy_L4_M4_BEGIN:

	mov	A01, A
	add	A02, A01, LDA
	add	A03, A02, LDA
	add	A04, A03, LDA
	add	A, A04, LDA

	mov	B01, B
	add	B, B01, #128					// B = B + 16 * SIZE

	asr	I, N, #2					// I = N / 4
	cmp 	I, #0
	ble	.Ldgemm_tcopy_L4_M4_40

	.align	5
.Ldgemm_tcopy_L4_M4_20:

	COPY4x4

	subs	I , I , #1
	bne	.Ldgemm_tcopy_L4_M4_20


.Ldgemm_tcopy_L4_M4_40:

	tst	N , #2
	ble	.Ldgemm_tcopy_L4_M4_60

	COPY2x4


.Ldgemm_tcopy_L4_M4_60:

	tst	N, #1
	ble	.Ldgemm_tcopy_L4_M4_END

	COPY1x4


.Ldgemm_tcopy_L4_M4_END:

	subs	J , J, #1						// j--
	bne	.Ldgemm_tcopy_L4_M4_BEGIN



/*********************************************************************************************/

.Ldgemm_tcopy_L2_BEGIN:

	tst	M, #3
	ble	.Ldgemm_tcopy_L999

	tst	M, #2
	ble	.Ldgemm_tcopy_L1_BEGIN

.Ldgemm_tcopy_L2_M4_BEGIN:
	mov	A01, A
	add	A02, A01, LDA
	add	A, A02, LDA

	mov	B01, B
	add	B, B01, #64					// B = B + 8 * SIZE

	asr	I, N, #2					// I = N / 4
	cmp 	I, #0
	ble	.Ldgemm_tcopy_L2_M4_40

	.align	5
.Ldgemm_tcopy_L2_M4_20:

	COPY4x2

	subs	I , I , #1
	bne	.Ldgemm_tcopy_L2_M4_20


.Ldgemm_tcopy_L2_M4_40:

	tst	N , #2
	ble	.Ldgemm_tcopy_L2_M4_60

	COPY2x2

.Ldgemm_tcopy_L2_M4_60:

	tst	N , #1
	ble	.Ldgemm_tcopy_L2_M4_END

	COPY1x2


.Ldgemm_tcopy_L2_M4_END:


/*********************************************************************************************/

.Ldgemm_tcopy_L1_BEGIN:

	tst	M, #1
	ble	.Ldgemm_tcopy_L999


.Ldgemm_tcopy_L1_M4_BEGIN:

	mov	A01, A						// A01 = A
	mov	B01, B

	asr	I, N, #2					// I = M / 4
	cmp 	I, #0
	ble	.Ldgemm_tcopy_L1_M4_40

	.align	5
.Ldgemm_tcopy_L1_M4_20:

	COPY4x1

	subs	I , I , #1
	bne	.Ldgemm_tcopy_L1_M4_20


.Ldgemm_tcopy_L1_M4_40:

	tst	N , #2
	ble	.Ldgemm_tcopy_L1_M4_60

	COPY2x1

.Ldgemm_tcopy_L1_M4_60:

	tst	N , #1
	ble	.Ldgemm_tcopy_L1_M4_END

	COPY1x1


.Ldgemm_tcopy_L1_M4_END:


.Ldgemm_tcopy_L999:
	mov	x0, #0						// set return value
	RESTORE_REGS
	ret

	EPILOGUE

