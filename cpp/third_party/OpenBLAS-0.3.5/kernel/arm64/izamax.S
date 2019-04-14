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

#define	N	x0	/* vector length */
#define	X	x1	/* X vector address */
#define	INC_X	x2	/* X stride */
#define INDEX	x3	/* index of max/min value */
#define Z	x4	/* vector index */
#define I	x5	/* loop variable */

/*******************************************************************************
* Macro definitions
*******************************************************************************/

#if defined(USE_MIN)
#define COND	le
#else
#define COND	ge
#endif

#if !defined(DOUBLE)
#define MAXF	s0
#define TMPF	s1
#define TMPVF	{v1.s}[0]
#define SZ	4
#else
#define MAXF	d0
#define TMPF	d1
#define TMPVF	{v1.d}[0]
#define SZ	8
#endif

/******************************************************************************/

.macro INIT_S
#if !defined(DOUBLE)
	lsl	INC_X, INC_X, #3
	ld1	{v0.2s}, [X], INC_X
	mov	Z, #1
	mov	INDEX, Z
	fabs	v0.2s, v0.2s
	ext	v1.8b, v0.8b, v0.8b, #4
	fadd	MAXF, s0, s1
#else
	lsl	INC_X, INC_X, #4
	ld1	{v0.2d}, [X], INC_X
	mov	Z, #1
	mov	INDEX, Z
	fabs	v0.2d, v0.2d
	faddp	MAXF, v0.2d
#endif
.endm

.macro KERNEL_F8
#if !defined(DOUBLE)
	ldp	q2, q3, [X], #32
	ldp	q4, q5, [X], #32

	fabs	v2.4s, v2.4s
	fabs	v3.4s, v3.4s
	fabs	v4.4s, v4.4s
	fabs	v5.4s, v5.4s

	faddp	v2.4s, v2.4s, v3.4s
	faddp	v3.4s, v4.4s, v5.4s

	fmax	v2.4s, v2.4s, v3.4s
	fmaxv	TMPF, v2.4s
	fcmp	MAXF, TMPF
	fcsel	MAXF, MAXF, TMPF, COND
	csel	INDEX, INDEX, Z, COND
	add	Z, Z, #8
#else
	ldp	q2, q3, [X], #32
	ldp	q4, q5, [X], #32
	ldp	q16, q17, [X], #32
	ldp	q18, q19, [X], #32

	fabs	v2.2d, v2.2d
	fabs	v3.2d, v3.2d
	fabs	v4.2d, v4.2d
	fabs	v5.2d, v5.2d
	fabs	v16.2d, v16.2d
	fabs	v17.2d, v17.2d
	fabs	v18.2d, v18.2d
	fabs	v19.2d, v19.2d

	faddp	v2.2d, v2.2d, v3.2d
	faddp	v3.2d, v4.2d, v5.2d
	faddp	v4.2d, v16.2d, v17.2d
	faddp	v5.2d, v18.2d, v19.2d

	fmax	v2.2d, v2.2d, v3.2d
	fmax	v4.2d, v4.2d, v5.2d
	fmax	v2.2d, v2.2d, v4.2d
	fmaxp	TMPF, v2.2d

	fcmp	MAXF, TMPF
	fcsel	MAXF, MAXF, TMPF, COND
	csel	INDEX, INDEX, Z, COND
	add	Z, Z, #8
#endif
	PRFM	PLDL1KEEP, [X, #1024]
.endm

.macro KERNEL_F8_FINALIZE
	sub	x6, INDEX, #1
#if !defined(DOUBLE)
	lsl	x6, x6, #3
	add	x7, x7, x6

	ldp	q2, q3, [x7]
	ldp	q4, q5, [x7, #32]

	fabs	v2.4s, v2.4s
	fabs	v3.4s, v3.4s
	fabs	v4.4s, v4.4s
	fabs	v5.4s, v5.4s

	faddp	v2.4s, v2.4s, v3.4s
	faddp	v3.4s, v4.4s, v5.4s

	ins	v4.s[0], v3.s[3]
	add	x6, INDEX, #7
	fcmp	MAXF, s4
	csel	INDEX, x6, INDEX, eq

	ins	v4.s[0], v3.s[2]
	sub	x6, x6, #1
	fcmp	MAXF, s4
	csel	INDEX, x6, INDEX, eq

	ins	v4.s[0], v3.s[1]
	sub	x6, x6, #1
	fcmp	MAXF, s4
	csel	INDEX, x6, INDEX, eq

	ins	v4.s[0], v3.s[0]
	sub	x6, x6, #1
	fcmp	MAXF, s4
	csel	INDEX, x6, INDEX, eq

	ins	v4.s[0], v2.s[3]
	sub	x6, x6, #1
	fcmp	MAXF, s4
	csel	INDEX, x6, INDEX, eq

	ins	v4.s[0], v2.s[2]
	sub	x6, x6, #1
	fcmp	MAXF, s4
	csel	INDEX, x6, INDEX, eq

	ins	v4.s[0], v2.s[1]
	sub	x6, x6, #1
	fcmp	MAXF, s4
	csel	INDEX, x6, INDEX, eq

	ins	v4.s[0], v2.s[0]
	sub	x6, x6, #1
	fcmp	MAXF, s4
	csel	INDEX, x6, INDEX, eq
#else
	lsl	x6, x6, #4
	add	x7, x7, x6

	ldp	q2, q3, [x7]
	ldp	q4, q5, [x7, #32]
	ldp	q16, q17, [x7, #64]
	ldp	q18, q19, [x7, #96]

	fabs	v2.2d, v2.2d
	fabs	v3.2d, v3.2d
	fabs	v4.2d, v4.2d
	fabs	v5.2d, v5.2d
	fabs	v16.2d, v16.2d
	fabs	v17.2d, v17.2d
	fabs	v18.2d, v18.2d
	fabs	v19.2d, v19.2d

	faddp	v2.2d, v2.2d, v3.2d
	faddp	v3.2d, v4.2d, v5.2d
	faddp	v4.2d, v16.2d, v17.2d
	faddp	v5.2d, v18.2d, v19.2d

	ins	v7.d[0], v5.d[1]
	add	x6, INDEX, #7
	fcmp	MAXF, d7
	csel	INDEX, x6, INDEX, eq

	ins	v7.d[0], v5.d[0]
	sub	x6, x6, #1
	fcmp	MAXF, d7
	csel	INDEX, x6, INDEX, eq

	ins	v7.d[0], v4.d[1]
	sub	x6, x6, #1
	fcmp	MAXF, d7
	csel	INDEX, x6, INDEX, eq

	ins	v7.d[0], v4.d[0]
	sub	x6, x6, #1
	fcmp	MAXF, d7
	csel	INDEX, x6, INDEX, eq

	ins	v7.d[0], v3.d[1]
	sub	x6, x6, #1
	fcmp	MAXF, d7
	csel	INDEX, x6, INDEX, eq

	ins	v7.d[0], v3.d[0]
	sub	x6, x6, #1
	fcmp	MAXF, d7
	csel	INDEX, x6, INDEX, eq

	ins	v7.d[0], v2.d[1]
	sub	x6, x6, #1
	fcmp	MAXF, d7
	csel	INDEX, x6, INDEX, eq

	ins	v7.d[0], v2.d[0]
	sub	x6, x6, #1
	fcmp	MAXF, d7
	csel	INDEX, x6, INDEX, eq
#endif
.endm

.macro KERNEL_S1
#if !defined(DOUBLE)
	ld1	{v1.2s}, [X], INC_X
	add	Z, Z, #1
	fabs	v1.2s, v1.2s
	ext	v2.8b, v1.8b, v1.8b, #4
	fadd	TMPF, s1, s2
#else
	ld1	{v1.2d}, [X], INC_X
	add	Z, Z, #1
	fabs	v1.2d, v1.2d
	faddp	TMPF, v1.2d
#endif
	fcmp	MAXF, TMPF
	fcsel	MAXF, MAXF, TMPF, COND
	csel	INDEX, INDEX, Z, COND
.endm

/*******************************************************************************
* End of macro definitions
*******************************************************************************/

	PROLOGUE

	cmp	N, xzr
	ble	.Lizamax_kernel_zero
	cmp	INC_X, xzr
	ble	.Lizamax_kernel_zero

	cmp	INC_X, #1
	bne	.Lizamax_kernel_S_BEGIN
	mov	x7, X


.Lizamax_kernel_F_BEGIN:

	INIT_S

	subs	N, N, #1
	ble	.Lizamax_kernel_L999

	asr	I, N, #3
	cmp	I, xzr
	ble	.Lizamax_kernel_F1

	add	Z, Z, #1

.Lizamax_kernel_F8:

	KERNEL_F8

	subs	I, I, #1
	bne	.Lizamax_kernel_F8

	KERNEL_F8_FINALIZE

	sub	Z, Z, #1
.Lizamax_kernel_F1:

	ands	I, N, #7
	ble	.Lizamax_kernel_L999

.Lizamax_kernel_F10:

	KERNEL_S1

	subs    I, I, #1
        bne     .Lizamax_kernel_F10

	b	.Lizamax_kernel_L999

.Lizamax_kernel_S_BEGIN:

	INIT_S

	subs	N, N, #1
	ble	.Lizamax_kernel_L999

	asr	I, N, #2
	cmp	I, xzr
	ble	.Lizamax_kernel_S1

.Lizamax_kernel_S4:

	KERNEL_S1
	KERNEL_S1
	KERNEL_S1
	KERNEL_S1

	subs	I, I, #1
	bne	.Lizamax_kernel_S4

.Lizamax_kernel_S1:

	ands	I, N, #3
	ble	.Lizamax_kernel_L999

.Lizamax_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     .Lizamax_kernel_S10

.Lizamax_kernel_L999:

	mov	x0, INDEX
	ret

.Lizamax_kernel_zero:

	mov	x0, xzr
	ret

	EPILOGUE
