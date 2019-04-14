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
#define	Y_IPTR	x10	/* loop Y vector address */
#define J	x11	/* loop variable */
#define I	x12	/* loop variable */
#define	Y_OPTR	x13	/* loop Y vector address */

/*******************************************************************************
* Macro definitions
*******************************************************************************/

#if !defined(DOUBLE)
#define ALPHA	s0
#define TEMP	s1
#define TEMPV	{v1.s}[0]
#define TMP1	s2
#define TMPV1	{v2.s}[0]
#define TMP2	s3
#define TMPV2	{v3.s}[0]
#define SZ	4
#define SHZ	2
#else
#define ALPHA	d0
#define TEMP	d1
#define TEMPV	{v1.d}[0]
#define TMP1	d2
#define TMPV1	{v2.d}[0]
#define TMP2	d3
#define TMPV2	{v3.d}[0]
#define SZ	8
#define SHZ	3
#endif

#define A_PRE_SIZE 768
#define Y_PRE_SIZE 768

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

.macro KERNEL_F16
#if !defined(DOUBLE)
	ld1	{v2.4s, v3.4s}, [A_PTR], #32
	ld1	{v4.4s, v5.4s}, [Y_IPTR], #32
	fmla	v4.4s, v1.4s, v2.4s
	prfm	PLDL1KEEP, [A_PTR, #A_PRE_SIZE]
	fmla	v5.4s, v1.4s, v3.4s
	st1	{v4.4s, v5.4s}, [Y_OPTR], #32

	ld1	{v6.4s, v7.4s}, [A_PTR], #32
	ld1	{v8.4s, v9.4s}, [Y_IPTR], #32
	fmla	v8.4s, v1.4s, v6.4s
	prfm	PLDL1KEEP, [Y_IPTR, #Y_PRE_SIZE]
	fmla	v9.4s, v1.4s, v7.4s
	st1	{v8.4s, v9.4s}, [Y_OPTR], #32
#else //DOUBLE
	ld1	{v2.2d, v3.2d}, [A_PTR], #32
	ld1	{v4.2d, v5.2d}, [Y_IPTR], #32
	fmla	v4.2d, v1.2d, v2.2d
	prfm	PLDL1KEEP, [A_PTR, #A_PRE_SIZE]
	fmla	v5.2d, v1.2d, v3.2d
	st1	{v4.2d, v5.2d}, [Y_OPTR], #32

	ld1	{v6.2d, v7.2d}, [A_PTR], #32
	ld1	{v8.2d, v9.2d}, [Y_IPTR], #32
	fmla	v8.2d, v1.2d, v6.2d
	prfm	PLDL1KEEP, [Y_IPTR, #Y_PRE_SIZE]
	fmla	v9.2d, v1.2d, v7.2d
	st1	{v8.2d, v9.2d}, [Y_OPTR], #32

	ld1	{v10.2d, v11.2d}, [A_PTR], #32
	ld1	{v12.2d, v13.2d}, [Y_IPTR], #32
	fmla	v12.2d, v1.2d, v10.2d
	prfm	PLDL1KEEP, [A_PTR, #A_PRE_SIZE]
	fmla	v13.2d, v1.2d, v11.2d
	st1	{v12.2d, v13.2d}, [Y_OPTR], #32

	ld1	{v14.2d, v15.2d}, [A_PTR], #32
	ld1	{v16.2d, v17.2d}, [Y_IPTR], #32
	fmla	v16.2d, v1.2d, v14.2d
	prfm	PLDL1KEEP, [Y_IPTR, #Y_PRE_SIZE]
	fmla	v17.2d, v1.2d, v15.2d
	st1	{v16.2d, v17.2d}, [Y_OPTR], #32
#endif
.endm

.macro KERNEL_F4
#if !defined(DOUBLE)
	ld1	{v2.4s}, [A_PTR], #16
	ld1	{v3.4s}, [Y_IPTR], #16
	fmla	v3.4s, v1.4s, v2.4s
	st1	{v3.4s}, [Y_OPTR], #16
#else
	ld1	{v2.2d}, [A_PTR], #16
	ld1	{v3.2d}, [Y_IPTR], #16
	fmla	v3.2d, v1.2d, v2.2d
	st1	{v3.2d}, [Y_OPTR], #16

	ld1	{v4.2d}, [A_PTR], #16
	ld1	{v5.2d}, [Y_IPTR], #16
	fmla	v5.2d, v1.2d, v4.2d
	st1	{v5.2d}, [Y_OPTR], #16
#endif
.endm

.macro KERNEL_F1

	ld1	TMPV1, [A_PTR], #SZ
	ld1	TMPV2, [Y_IPTR]
	fmadd	TMP2, TEMP, TMP1, TMP2
	st1	TMPV2, [Y_IPTR], #SZ

.endm

.macro INIT_S

	lsl	INC_Y, INC_Y, #SHZ

.endm

.macro KERNEL_S1

	ld1	TMPV1, [A_PTR], #SZ
	ld1	TMPV2, [Y_IPTR]
	fmadd	TMP2, TEMP, TMP1, TMP2
	st1	TMPV2, [Y_IPTR], INC_Y

.endm

/*******************************************************************************
* End of macro definitions
*******************************************************************************/

	PROLOGUE

	ldr	INC_Y, [sp]

	SAVE_REGS

	cmp	N, xzr
	ble	.Lgemv_n_kernel_L999
	cmp	M, xzr
	ble	.Lgemv_n_kernel_L999

	lsl	LDA, LDA, #SHZ
	lsl	INC_X, INC_X, #SHZ
	mov	J, N

	cmp	INC_Y, #1
	bne	.Lgemv_n_kernel_S_BEGIN

.Lgemv_n_kernel_F_LOOP:

	ld1	TEMPV, [X], INC_X
	fmul	TEMP, ALPHA, TEMP
#if !defined(DOUBLE)
	ins	v1.s[1], v1.s[0]
	ins	v1.s[2], v1.s[0]
	ins	v1.s[3], v1.s[0]
#else
	ins	v1.d[1], v1.d[0]
#endif
	mov	A_PTR, A
	mov	Y_IPTR, Y
	mov	Y_OPTR, Y

.Lgemv_n_kernel_F32:

	asr	I, M, #5
	cmp	I, xzr
	beq	.Lgemv_n_kernel_F4

.Lgemv_n_kernel_F320:

	KERNEL_F16
	KERNEL_F16

	subs	I, I, #1
	bne	.Lgemv_n_kernel_F320

.Lgemv_n_kernel_F4:
	ands	I, M, #31
	asr	I, I, #2
	cmp	I, xzr
	beq	.Lgemv_n_kernel_F1

.Lgemv_n_kernel_F40:

	KERNEL_F4

	subs	I, I, #1
	bne	.Lgemv_n_kernel_F40

.Lgemv_n_kernel_F1:
	ands	I, M, #3
	ble	.Lgemv_n_kernel_F_END

.Lgemv_n_kernel_F10:

	KERNEL_F1

	subs	I, I, #1
	bne	.Lgemv_n_kernel_F10

.Lgemv_n_kernel_F_END:

	add	A, A, LDA
	subs	J, J, #1
	bne	.Lgemv_n_kernel_F_LOOP

	b	.Lgemv_n_kernel_L999

.Lgemv_n_kernel_S_BEGIN:

	INIT_S

.Lgemv_n_kernel_S_LOOP:

	ld1	TEMPV, [X], INC_X
	fmul	TEMP, ALPHA, TEMP
	mov	A_PTR, A
	mov	Y_IPTR, Y

	asr	I, M, #2
	cmp	I, xzr
	ble	.Lgemv_n_kernel_S1

.Lgemv_n_kernel_S4:

	KERNEL_S1
	KERNEL_S1
	KERNEL_S1
	KERNEL_S1

	subs	I, I, #1
	bne	.Lgemv_n_kernel_S4

.Lgemv_n_kernel_S1:

	ands	I, M, #3
	ble	.Lgemv_n_kernel_S_END

.Lgemv_n_kernel_S10:

	KERNEL_S1

	subs	I, I, #1
	bne	.Lgemv_n_kernel_S10

.Lgemv_n_kernel_S_END:

	add	A, A, LDA
	subs	J, J, #1
	bne	.Lgemv_n_kernel_S_LOOP

.Lgemv_n_kernel_L999:

	mov	w0, wzr

	RESTORE_REGS

	ret

	EPILOGUE
