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
#define	Y	x3	/* Y vector address */
#define	INC_Y	x4	/* Y stride */
#define I	x5	/* loop variable */

/*******************************************************************************
* Macro definitions
*******************************************************************************/

#if !defined(DOUBLE)
#define	C	s0	/* scale input value */
#define	S	s1	/* scale input value */
#else
#define	C	d0	/* scale input value */
#define	S	d1	/* scale input value */
#endif

/******************************************************************************/

.macro INIT

#if !defined(DOUBLE)
	ins	v0.s[1], v0.s[0]		// [C, C]
	ins	v1.s[1], v1.s[0]		// [S, S]
#else
	ins	v0.d[1], v0.d[0]		// [C, C]
	ins	v1.d[1], v1.d[0]		// [S, S]
#endif

.endm

.macro KERNEL_F1

#if !defined(DOUBLE)
	ld1	{v2.2s}, [X]
	ld1	{v3.2s}, [Y]
	fmul	v4.2s, v0.2s, v2.2s		// [C*X1, C*X0]
	fmla	v4.2s, v1.2s, v3.2s		// [C*X1 + S*Y1, C*X0 + S*Y0]
	fmul	v5.2s, v0.2s, v3.2s		// [C*Y1, C*Y0]
	fmls	v5.2s, v1.2s, v2.2s		// [C*Y1 - S*X1, C*Y0 - S*X0]
	st1	{v4.2s}, [X], #8
	st1	{v5.2s}, [Y], #8
#else
	ld1	{v2.2d}, [X]
	ld1	{v3.2d}, [Y]
	fmul	v4.2d, v0.2d, v2.2d		// [C*X1, C*X0]
	fmla	v4.2d, v1.2d, v3.2d		// [C*X1 + S*Y1, C*X0 + S*Y0]
	fmul	v5.2d, v0.2d, v3.2d		// [C*Y1, C*Y0]
	fmls	v5.2d, v1.2d, v2.2d		// [C*Y1 - S*X1, C*Y0 - S*X0]
	st1	{v4.2d}, [X], #16
	st1	{v5.2d}, [Y], #16
#endif

.endm

.macro KERNEL_INIT_F4

#if !defined(DOUBLE)
	ins	v0.d[1], v0.d[0]		// [C, C, C, C]
	ins	v1.d[1], v1.d[0]		// [S, S, S, S]
#endif

.endm

.macro KERNEL_F4

#if !defined(DOUBLE)
	ld1	{v2.4s, v3.4s}, [X]
	ld1	{v4.4s, v5.4s}, [Y]
	fmul	v6.4s, v0.4s, v2.4s		// C*X3, C*X2, C*X1, C*X0
	fmul	v7.4s, v0.4s, v3.4s		// C*X7, C*X6, C*X5, C*X4
	fmla	v6.4s, v1.4s, v4.4s		// C*X3+S*Y3, ..., C*X0+S*Y0
	fmla	v7.4s, v1.4s, v5.4s		// C*X7+S*Y7, ..., C*X4+S*Y4
	fmul	v16.4s, v0.4s, v4.4s		// C*Y3, C*Y2, C*Y1, C*Y0
	fmul	v17.4s, v0.4s, v5.4s		// C*Y7, C*Y6, C*Y5, C*Y4
	fmls	v16.4s, v1.4s, v2.4s		// C*Y3-S*X3, ..., C*Y0-S*X0
	fmls	v17.4s, v1.4s, v3.4s		// C*Y7-S*X7, ..., C*Y4-S*X4
	st1	{v6.4s,v7.4s}, [X], #32
	st1	{v16.4s,v17.4s}, [Y], #32
#else // DOUBLE
	ld1	{v2.2d, v3.2d}, [X]
	ld1	{v4.2d, v5.2d}, [Y]
	fmul	v6.2d, v0.2d, v2.2d		// C*X3, C*X2, C*X1, C*X0
	fmul	v7.2d, v0.2d, v3.2d		// C*X7, C*X6, C*X5, C*X4
	fmla	v6.2d, v1.2d, v4.2d		// C*X3+S*Y3, ..., C*X0+S*Y0
	fmla	v7.2d, v1.2d, v5.2d		// C*X7+S*Y7, ..., C*X4+S*Y4
	fmul	v16.2d, v0.2d, v4.2d		// C*Y3, C*Y2, C*Y1, C*Y0
	fmul	v17.2d, v0.2d, v5.2d		// C*Y7, C*Y6, C*Y5, C*Y4
	fmls	v16.2d, v1.2d, v2.2d		// C*Y3-S*X3, ..., C*Y0-S*X0
	fmls	v17.2d, v1.2d, v3.2d		// C*Y7-S*X7, ..., C*Y4-S*X4
	st1	{v6.2d,v7.2d}, [X], #32
	st1	{v16.2d,v17.2d}, [Y], #32
	ld1	{v2.2d, v3.2d}, [X]
	ld1	{v4.2d, v5.2d}, [Y]
	fmul	v6.2d, v0.2d, v2.2d		// C*X3, C*X2, C*X1, C*X0
	fmul	v7.2d, v0.2d, v3.2d		// C*X7, C*X6, C*X5, C*X4
	fmla	v6.2d, v1.2d, v4.2d		// C*X3+S*Y3, ..., C*X0+S*Y0
	fmla	v7.2d, v1.2d, v5.2d		// C*X7+S*Y7, ..., C*X4+S*Y4
	fmul	v16.2d, v0.2d, v4.2d		// C*Y3, C*Y2, C*Y1, C*Y0
	fmul	v17.2d, v0.2d, v5.2d		// C*Y7, C*Y6, C*Y5, C*Y4
	fmls	v16.2d, v1.2d, v2.2d		// C*Y3-S*X3, ..., C*Y0-S*X0
	fmls	v17.2d, v1.2d, v3.2d		// C*Y7-S*X7, ..., C*Y4-S*X4
	st1	{v6.2d,v7.2d}, [X], #32
	st1	{v16.2d,v17.2d}, [Y], #32
#endif

.endm

.macro INIT_S

#if !defined(DOUBLE)
	lsl	INC_X, INC_X, #3
	lsl	INC_Y, INC_Y, #3
#else
	lsl	INC_X, INC_X, #4
	lsl	INC_Y, INC_Y, #4
#endif

.endm

.macro KERNEL_S1

#if !defined(DOUBLE)
	ld1	{v2.2s}, [X]
	ld1	{v3.2s}, [Y]
	fmul	v4.2s, v0.2s, v2.2s		// [C*X1, C*X0]
	fmla	v4.2s, v1.2s, v3.2s		// [C*X1 + S*Y1, C*X0 + S*Y0]
	fmul	v5.2s, v0.2s, v3.2s		// [C*Y1, C*Y0]
	fmls	v5.2s, v1.2s, v2.2s		// [C*Y1 - S*X1, C*Y0 - S*X0]
	st1	{v4.2s}, [X], INC_X
	st1	{v5.2s}, [Y], INC_Y
#else
	ld1	{v2.2d}, [X]
	ld1	{v3.2d}, [Y]
	fmul	v4.2d, v0.2d, v2.2d		// [C*X1, C*X0]
	fmla	v4.2d, v1.2d, v3.2d		// [C*X1 + S*Y1, C*X0 + S*Y0]
	fmul	v5.2d, v0.2d, v3.2d		// [C*Y1, C*Y0]
	fmls	v5.2d, v1.2d, v2.2d		// [C*Y1 - S*X1, C*Y0 - S*X0]
	st1	{v4.2d}, [X], INC_X
	st1	{v5.2d}, [Y], INC_Y
#endif

.endm

/*******************************************************************************
* End of macro definitions
*******************************************************************************/

	PROLOGUE

	cmp	N, xzr
	ble	.Lzrot_kernel_L999

	INIT

	cmp	INC_X, #1
	bne	.Lzrot_kernel_S_BEGIN
	cmp	INC_Y, #1
	bne	.Lzrot_kernel_S_BEGIN

.Lzrot_kernel_F_BEGIN:

	asr	I, N, #2
	cmp	I, xzr
	beq	.Lzrot_kernel_F1

	KERNEL_INIT_F4

.Lzrot_kernel_F4:

	KERNEL_F4

	subs	I, I, #1
	bne	.Lzrot_kernel_F4

.Lzrot_kernel_F1:

	ands	I, N, #3
	ble	.Lzrot_kernel_L999

.Lzrot_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
        bne     .Lzrot_kernel_F10

	mov	w0, wzr
	ret

.Lzrot_kernel_S_BEGIN:

	INIT_S

	asr	I, N, #2
	cmp	I, xzr
	ble	.Lzrot_kernel_S1

.Lzrot_kernel_S4:

	KERNEL_S1
	KERNEL_S1
	KERNEL_S1
	KERNEL_S1

	subs	I, I, #1
	bne	.Lzrot_kernel_S4

.Lzrot_kernel_S1:

	ands	I, N, #3
	ble	.Lzrot_kernel_L999

.Lzrot_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     .Lzrot_kernel_S10

.Lzrot_kernel_L999:

	mov	w0, wzr
	ret
