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

#define I	x9
#define	J	x10

#define	TEMP1	x11
#define	TEMP2	x12

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

.macro COPY4x4
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

.macro COPY4x2
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]
	//prfm	PLDL1KEEP, [A02, #A_PREFETCH]

	ldp	q0, q1, [A01], #32
	ins	v8.d[0], v0.d[0]
	ins	v9.d[0], v0.d[1]
	ins	v10.d[0], v1.d[0]
	ins	v11.d[0], v1.d[1]

	ldp	q2, q3, [A02], #32
	ins	v8.d[1], v2.d[0]
	ins	v9.d[1], v2.d[1]
	ins	v10.d[1], v3.d[0]
	ins	v11.d[1], v3.d[1]

	st1	{v8.2d, v9.2d, v10.2d, v11.2d}, [B00]
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

.macro COPY4x1
	//prfm	PLDL1KEEP, [A01, #A_PREFETCH]

	ldp	q0, q1, [A01], #32
	stp	q0, q1, [B00], #32
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

.Ldgemm_ncopy_L4_BEGIN:

	asr	J, N, #2					// J = N / 4
	cmp 	J, #0
	ble	.Ldgemm_ncopy_L2_BEGIN

	.align	5
.Ldgemm_ncopy_L4_M4_BEGIN:

	mov	A01, A00
	add	A02, A01, LDA
	add	A03, A02, LDA
	add	A04, A03, LDA
	add	A00, A04, LDA

	asr	I, M, #2					// I = M / 4
	cmp	I, #0
	ble	.Ldgemm_ncopy_L4_M4_40

	.align	5
.Ldgemm_ncopy_L4_M4_20:

	COPY4x4

	subs	I , I , #1
	bne	.Ldgemm_ncopy_L4_M4_20


.Ldgemm_ncopy_L4_M4_40:

	and	I, M , #3
	cmp	I, #0
	ble	.Ldgemm_ncopy_L4_M4_END

	.align	5
.Ldgemm_ncopy_L4_M4_60:

	COPY1x4

	subs	I , I , #1
	bne	.Ldgemm_ncopy_L4_M4_60


.Ldgemm_ncopy_L4_M4_END:

	subs	J , J, #1						// j--
	bne	.Ldgemm_ncopy_L4_M4_BEGIN



/*********************************************************************************************/

.Ldgemm_ncopy_L2_BEGIN:

	tst	N, #3
	ble	.Ldgemm_ncopy_L999

	tst	N, #2
	ble	.Ldgemm_ncopy_L1_BEGIN

.Ldgemm_ncopy_L2_M4_BEGIN:
	mov	A01, A00
	add	A02, A01, LDA
	add	A00, A02, LDA

	asr	I, M, #2					// I = M / 4
	cmp 	I, #0
	ble	.Ldgemm_ncopy_L2_M4_40

	.align	5
.Ldgemm_ncopy_L2_M4_20:

	COPY4x2

	subs	I , I , #1
	bne	.Ldgemm_ncopy_L2_M4_20


.Ldgemm_ncopy_L2_M4_40:

	and	I, M , #3
	cmp	I, #0
	ble	.Ldgemm_ncopy_L2_M4_END

	.align	5
.Ldgemm_ncopy_L2_M4_60:

	COPY1x2

	subs	I , I , #1
	bne	.Ldgemm_ncopy_L2_M4_60


.Ldgemm_ncopy_L2_M4_END:


/*********************************************************************************************/

.Ldgemm_ncopy_L1_BEGIN:

	tst	N, #1
	ble	.Ldgemm_ncopy_L999


.Ldgemm_ncopy_L1_M4_BEGIN:

	mov	A01, A00

	asr	I, M, #2					// I = M / 4
	cmp	I, #0
	ble	.Ldgemm_ncopy_L1_M4_40

	.align	5
.Ldgemm_ncopy_L1_M4_20:

	COPY4x1

	subs	I , I , #1
	bne	.Ldgemm_ncopy_L1_M4_20


.Ldgemm_ncopy_L1_M4_40:

	and	I, M , #3
	cmp	I, #0
	ble	.Ldgemm_ncopy_L1_M4_END

	.align	5
.Ldgemm_ncopy_L1_M4_60:

	COPY1x1

	subs	I , I , #1
	bne	.Ldgemm_ncopy_L1_M4_60


.Ldgemm_ncopy_L1_M4_END:

.Ldgemm_ncopy_L999:

	mov	x0, #0
	RESTORE_REGS
	ret

	EPILOGUE

