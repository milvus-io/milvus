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

#define	N	x0	/* vector length */
#define	X	x3	/* X vector address */
#define	INC_X	x4	/* X stride */
#define	Y	x5	/* Y vector address */
#define	INC_Y	x6	/* Y stride */
#define I	x1	/* loop variable */

/*******************************************************************************
* Macro definitions
*******************************************************************************/

#define	DA	d0	/* scale input value */
#define TMPX	d1
#define TMPVX	{v1.d}[0]
#define TMPY	d2
#define TMPVY	{v2.d}[0]
#define SZ	8

/******************************************************************************/

.macro KERNEL_F1
	ldr	TMPX, [X], #SZ
	ldr	TMPY, [Y]
	fmadd	TMPY, TMPX, DA, TMPY
	str	TMPY, [Y], #SZ
.endm

.macro KERNEL_F16
	ldp	q4, q5, [X]
	ldp	q16, q17, [Y]

	ldp	q6, q7, [X, #32]
	ldp	q18, q19, [Y, #32]

	fmla	v16.2d, v4.2d, v0.d[0]
	fmla	v17.2d, v5.2d, v0.d[0]

	PRFM	PLDL1KEEP, [X, #896]
	PRFM	PLDL1KEEP, [Y, #896]

	stp	q16, q17, [Y]

	ldp	q20, q21, [X, #64]
	ldp	q24, q25, [Y, #64]

	fmla	v18.2d, v6.2d, v0.d[0]
	fmla	v19.2d, v7.2d, v0.d[0]

	PRFM	PLDL1KEEP, [X, #896+64]
	PRFM	PLDL1KEEP, [Y, #896+64]

	stp	q18, q19, [Y, #32]

	ldp	q22, q23, [X, #96]
	ldp	q26, q27, [Y, #96]

	fmla	v24.2d, v20.2d, v0.d[0]
	fmla	v25.2d, v21.2d, v0.d[0]

	stp	q24, q25, [Y, #64]

	fmla	v26.2d, v22.2d, v0.d[0]
	fmla	v27.2d, v23.2d, v0.d[0]

	stp	q26, q27, [Y, #96]

	add	Y, Y, #128
	add	X, X, #128
.endm

.macro KERNEL_F32
	KERNEL_F16 
	KERNEL_F16 
.endm

.macro INIT_S
	lsl	INC_X, INC_X, #3
	lsl	INC_Y, INC_Y, #3
.endm

.macro KERNEL_S1
	ld1	TMPVX, [X], INC_X
	ldr	TMPY, [Y]
	fmadd	TMPY, TMPX, DA, TMPY
	st1	TMPVY, [Y], INC_Y
.endm

/*******************************************************************************
* End of macro definitions
*******************************************************************************/

	PROLOGUE

	cmp	N, xzr
	ble	.Ldaxpy_kernel_L999

	fcmp	DA, #0.0
	beq	.Ldaxpy_kernel_L999

	cmp	INC_X, #1
	bne	.Ldaxpy_kernel_S_BEGIN
	cmp	INC_Y, #1
	bne	.Ldaxpy_kernel_S_BEGIN

.Ldaxpy_kernel_F_BEGIN:

	asr	I, N, #5
	cmp	I, xzr
	beq	.Ldaxpy_kernel_F1

	.align 5
.Ldaxpy_kernel_F32:

	KERNEL_F32

	subs	I, I, #1
	bne	.Ldaxpy_kernel_F32

.Ldaxpy_kernel_F1:

	ands	I, N, #31
	ble	.Ldaxpy_kernel_L999

.Ldaxpy_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
        bne     .Ldaxpy_kernel_F10

	b	.Ldaxpy_kernel_L999

.Ldaxpy_kernel_S_BEGIN:

	INIT_S

	asr	I, N, #2
	cmp	I, xzr
	ble	.Ldaxpy_kernel_S1

.Ldaxpy_kernel_S4:

	KERNEL_S1
	KERNEL_S1
	KERNEL_S1
	KERNEL_S1

	subs	I, I, #1
	bne	.Ldaxpy_kernel_S4

.Ldaxpy_kernel_S1:

	ands	I, N, #3
	ble	.Ldaxpy_kernel_L999

.Ldaxpy_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     .Ldaxpy_kernel_S10

.Ldaxpy_kernel_L999:

	mov	w0, wzr
	ret
