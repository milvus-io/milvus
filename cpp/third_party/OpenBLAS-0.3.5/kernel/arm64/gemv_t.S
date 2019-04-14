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

#define	M	x0	/* Y vector length */
#define	N	x1	/* X vector length */
#define	A	x3	/* A vector address */
#define	LDA	x4	/* A stride */
#define	X	x5	/* X vector address */
#define	INC_X	x6	/* X stride */
#define	Y	x7	/* Y vector address */
#define	INC_Y	x2	/* Y stride */
#define	A_PTR	x9	/* loop A vector address */
#define	X_PTR	x10	/* loop X vector address */
#define J	x11	/* loop variable */
#define I	x12	/* loop variable */

#define X_PREFETCH_SIZE	768
#define A_PREFETCH_SIZE	768

/*******************************************************************************
* Macro definitions
*******************************************************************************/

#if !defined(DOUBLE)
#define REG0	wzr
#define ALPHA	s0
#define TEMP	s1
#define TEMP1	s2
#define TEMP2	s3
#define TEMP3	s4
#define TEMPV	{v1.s}[0]
#define TMP1	s2
#define TMPV1	{v2.s}[0]
#define TMP2	s3
#define TMPV2	{v3.s}[0]
#define SZ	4
#define SHZ	2
#else
#define REG0	xzr
#define ALPHA	d0
#define TEMP	d1
#define TEMP1	d2
#define TEMP2	d3
#define TEMP3	d4
#define TEMPV	{v1.d}[0]
#define TMP1	d2
#define TMPV1	{v2.d}[0]
#define TMP2	d3
#define TMPV2	{v3.d}[0]
#define SZ	8
#define SHZ	3
#endif

/******************************************************************************/

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

.macro KERNEL_F32
#if !defined(DOUBLE)
	ld1	{v5.4s, v6.4s, v7.4s, v8.4s}, [A_PTR], #64
	ld1	{v9.4s, v10.4s, v11.4s, v12.4s}, [X_PTR], #64
	fmla	v1.4s, v5.4s, v9.4s
	prfm	PLDL1KEEP, [A_PTR, #A_PREFETCH_SIZE]
	fmla	v2.4s, v6.4s, v10.4s
	prfm	PLDL1KEEP, [X_PTR, #X_PREFETCH_SIZE]
	fmla	v3.4s, v7.4s, v11.4s
	ld1	{v13.4s, v14.4s, v15.4s, v16.4s}, [A_PTR], #64
	fmla	v4.4s, v8.4s, v12.4s

	ld1	{v17.4s, v18.4s, v19.4s, v20.4s}, [X_PTR], #64
	fmla	v1.4s, v13.4s, v17.4s
	prfm	PLDL1KEEP, [A_PTR, #A_PREFETCH_SIZE]
	fmla	v2.4s, v14.4s, v18.4s
	prfm	PLDL1KEEP, [X_PTR, #X_PREFETCH_SIZE]
	fmla	v3.4s, v15.4s, v19.4s
	fmla	v4.4s, v16.4s, v20.4s
#else
	ld1	{v5.2d, v6.2d, v7.2d, v8.2d}, [A_PTR], #64
	ld1	{v9.2d, v10.2d, v11.2d, v12.2d}, [X_PTR], #64
	fmla	v1.2d, v5.2d, v9.2d
	prfm	PLDL1KEEP, [A_PTR, #A_PREFETCH_SIZE]
	fmla	v2.2d, v6.2d, v10.2d
	prfm	PLDL1KEEP, [X_PTR, #X_PREFETCH_SIZE]
	fmla	v3.2d, v7.2d, v11.2d
	fmla	v4.2d, v8.2d, v12.2d

	ld1	{v13.2d, v14.2d, v15.2d, v16.2d}, [A_PTR], #64
	ld1	{v17.2d, v18.2d, v19.2d, v20.2d}, [X_PTR], #64
	fmla	v1.2d, v13.2d, v17.2d
	prfm	PLDL1KEEP, [A_PTR, #A_PREFETCH_SIZE]
	fmla	v2.2d, v14.2d, v18.2d
	prfm	PLDL1KEEP, [X_PTR, #X_PREFETCH_SIZE]
	fmla	v3.2d, v15.2d, v19.2d
	fmla	v4.2d, v16.2d, v20.2d

	ld1	{v5.2d, v6.2d, v7.2d, v8.2d}, [A_PTR], #64
	ld1	{v9.2d, v10.2d, v11.2d, v12.2d}, [X_PTR], #64
	fmla	v1.2d, v5.2d, v9.2d
	prfm	PLDL1KEEP, [A_PTR, #A_PREFETCH_SIZE]
	fmla	v2.2d, v6.2d, v10.2d
	prfm	PLDL1KEEP, [X_PTR, #X_PREFETCH_SIZE]
	fmla	v3.2d, v7.2d, v11.2d
	fmla	v4.2d, v8.2d, v12.2d

	ld1	{v13.2d, v14.2d, v15.2d, v16.2d}, [A_PTR], #64
	ld1	{v17.2d, v18.2d, v19.2d, v20.2d}, [X_PTR], #64
	fmla	v1.2d, v13.2d, v17.2d
	prfm	PLDL1KEEP, [A_PTR, #A_PREFETCH_SIZE]
	fmla	v2.2d, v14.2d, v18.2d
	prfm	PLDL1KEEP, [X_PTR, #X_PREFETCH_SIZE]
	fmla	v3.2d, v15.2d, v19.2d
	fmla	v4.2d, v16.2d, v20.2d
#endif
.endm

.macro KERNEL_F32_FINALIZE
#if !defined(DOUBLE)
	fadd	v1.4s, v1.4s, v2.4s
	fadd	v1.4s, v1.4s, v3.4s
	fadd	v1.4s, v1.4s, v4.4s
#else
	fadd	v1.2d, v1.2d, v2.2d
	fadd	v1.2d, v1.2d, v3.2d
	fadd	v1.2d, v1.2d, v4.2d
#endif
.endm

.macro KERNEL_F4
#if !defined(DOUBLE)
	ld1	{v2.4s}, [A_PTR], #16
	ld1	{v3.4s}, [X_PTR], #16
	fmla	v1.4s, v2.4s, v3.4s
#else
	ld1	{v2.2d}, [A_PTR], #16
	ld1	{v3.2d}, [X_PTR], #16
	fmla	v1.2d, v2.2d, v3.2d

	ld1	{v4.2d}, [A_PTR], #16
	ld1	{v5.2d}, [X_PTR], #16
	fmla	v1.2d, v4.2d, v5.2d
#endif
.endm

.macro KERNEL_F4_FINALIZE
#if !defined(DOUBLE)
	ext	v2.16b, v1.16b, v1.16b, #8
	fadd	v1.2s, v1.2s, v2.2s
	faddp	TEMP, v1.2s
#else
	faddp	TEMP, v1.2d
#endif
.endm

.macro KERNEL_F1
	ld1	TMPV1, [A_PTR], #SZ
	ld1	TMPV2, [X_PTR], #SZ
	fmadd	TEMP, TMP1, TMP2, TEMP
.endm

.macro INIT_S
	lsl	INC_X, INC_X, #SHZ
.endm

.macro KERNEL_S1
	ld1	TMPV1, [A_PTR], #SZ
	ld1	TMPV2, [X_PTR], INC_X
	fmadd	TEMP, TMP1, TMP2, TEMP
.endm

/*******************************************************************************
* End of macro definitions
*******************************************************************************/

	PROLOGUE

	ldr	INC_Y, [sp]

	SAVE_REGS

	cmp	N, xzr
	ble	.Lgemv_t_kernel_L999
	cmp	M, xzr
	ble	.Lgemv_t_kernel_L999

	lsl	LDA, LDA, #SHZ
	lsl	INC_Y, INC_Y, #SHZ
	mov	J, N

	cmp	INC_X, #1
	bne	.Lgemv_t_kernel_S_BEGIN

.Lgemv_t_kernel_F_LOOP:

	fmov	TEMP, REG0
	fmov	TEMP1, REG0
	fmov	TEMP2, REG0
	fmov	TEMP3, REG0

	mov	A_PTR, A
	mov	X_PTR, X

.Lgemv_t_kernel_F32:

	asr	I, M, #5
	cmp	I, xzr
	beq	.Lgemv_t_kernel_F4

.Lgemv_t_kernel_F320:

	KERNEL_F32

	subs	I, I, #1
	bne	.Lgemv_t_kernel_F320

	KERNEL_F32_FINALIZE

.Lgemv_t_kernel_F4:
	ands	I, M, #31
	asr	I, I, #2
	cmp	I, xzr
	beq	.Lgemv_t_kernel_F1

.Lgemv_t_kernel_F40:

	KERNEL_F4

	subs	I, I, #1
	bne	.Lgemv_t_kernel_F40

.Lgemv_t_kernel_F1:

	KERNEL_F4_FINALIZE

	ands	I, M, #3
	ble	.Lgemv_t_kernel_F_END

.Lgemv_t_kernel_F10:

	KERNEL_F1

	subs	I, I, #1
	bne	.Lgemv_t_kernel_F10

.Lgemv_t_kernel_F_END:

	ld1	TMPV1, [Y]
	add	A, A, LDA
	subs	J, J, #1
	fmadd	TMP1, ALPHA, TEMP, TMP1
	st1	TMPV1, [Y], INC_Y
	bne	.Lgemv_t_kernel_F_LOOP

	b	.Lgemv_t_kernel_L999

.Lgemv_t_kernel_S_BEGIN:

	INIT_S

.Lgemv_t_kernel_S_LOOP:

	fmov	TEMP, REG0
	mov	A_PTR, A
	mov	X_PTR, X

	asr	I, M, #2
	cmp	I, xzr
	ble	.Lgemv_t_kernel_S1

.Lgemv_t_kernel_S4:

	KERNEL_S1
	KERNEL_S1
	KERNEL_S1
	KERNEL_S1

	subs	I, I, #1
	bne	.Lgemv_t_kernel_S4

.Lgemv_t_kernel_S1:

	ands	I, M, #3
	ble	.Lgemv_t_kernel_S_END

.Lgemv_t_kernel_S10:

	KERNEL_S1

	subs	I, I, #1
	bne	.Lgemv_t_kernel_S10

.Lgemv_t_kernel_S_END:

	ld1	TMPV1, [Y]
	add	A, A, LDA
	subs    J, J, #1
	fmadd	TMP1, ALPHA, TEMP, TMP1
	st1	TMPV1, [Y], INC_Y
        bne     .Lgemv_t_kernel_S_LOOP

.Lgemv_t_kernel_L999:

	RESTORE_REGS

	mov	w0, wzr
	ret

	EPILOGUE
