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
#define	X	x3	/* X vector address */
#define	INC_X	x4	/* X stride */
#define	Y	x5	/* Y vector address */
#define	INC_Y	x6	/* Y stride */
#define I	x1	/* loop variable */

/*******************************************************************************
* Macro definitions
*******************************************************************************/

#if !defined(DOUBLE)
#define	DA	s0	/* scale input value */
#define TMPX	s1
#define TMPVX	{v1.s}[0]
#define TMPY	s2
#define TMPVY	{v2.s}[0]
#define SZ	4
#else
#define	DA	d0	/* scale input value */
#define TMPX	d1
#define TMPVX	{v1.d}[0]
#define TMPY	d2
#define TMPVY	{v2.d}[0]
#define SZ	8
#endif

/******************************************************************************/

.macro KERNEL_F1

	ldr	TMPX, [X], #SZ
	ldr	TMPY, [Y]
	fmadd	TMPY, TMPX, DA, TMPY
	str	TMPY, [Y], #SZ

.endm

.macro KERNEL_F4

#if !defined(DOUBLE)
	ld1	{v1.4s}, [X], #16
	ld1	{v2.4s}, [Y]
	fmla	v2.4s, v1.4s, v0.s[0]
	st1	{v2.4s}, [Y], #16
#else // DOUBLE
	ld1	{v1.2d, v2.2d}, [X], #32
	ld1	{v3.2d, v4.2d}, [Y]
	fmla	v3.2d, v1.2d, v0.d[0]
	fmla	v4.2d, v2.2d, v0.d[0]
	st1	{v3.2d, v4.2d}, [Y], #32
#endif

.endm

.macro KERNEL_F8
#if !defined(DOUBLE)
	ld1	{v1.4s, v2.4s}, [X], #32
	ld1	{v3.4s, v4.4s}, [Y]

	fmla	v3.4s, v1.4s, v0.s[0]
	fmla	v4.4s, v2.4s, v0.s[0]

	st1	{v3.4s, v4.4s}, [Y], #32
#else // DOUBLE
	ld1	{v1.2d, v2.2d, v3.2d, v4.2d}, [X], #64
	ld1	{v16.2d, v17.2d, v18.2d, v19.2d}, [Y]

	fmla	v16.2d, v1.2d, v0.d[0]
	fmla	v17.2d, v2.2d, v0.d[0]
	fmla	v18.2d, v3.2d, v0.d[0]
	fmla	v19.2d, v4.2d, v0.d[0]

	st1	{v16.2d, v17.2d, v18.2d, v19.2d}, [Y], #64
#endif
	PRFM	PLDL1KEEP, [X, #512]
	PRFM	PLDL1KEEP, [Y, #512]
.endm

.macro INIT_S

#if !defined(DOUBLE)
	lsl	INC_X, INC_X, #2
	lsl	INC_Y, INC_Y, #2
#else
	lsl	INC_X, INC_X, #3
	lsl	INC_Y, INC_Y, #3
#endif

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
	ble	.Laxpy_kernel_L999

	fcmp	DA, #0.0
	beq	.Laxpy_kernel_L999

	cmp	INC_X, #1
	bne	.Laxpy_kernel_S_BEGIN
	cmp	INC_Y, #1
	bne	.Laxpy_kernel_S_BEGIN

.Laxpy_kernel_F_BEGIN:

	asr	I, N, #3
	cmp	I, xzr
	beq	.Laxpy_kernel_F1

.Laxpy_kernel_F8:

	KERNEL_F8

	subs	I, I, #1
	bne	.Laxpy_kernel_F8

.Laxpy_kernel_F1:

	ands	I, N, #7
	ble	.Laxpy_kernel_L999

.Laxpy_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
        bne     .Laxpy_kernel_F10

	mov	w0, wzr
	ret

.Laxpy_kernel_S_BEGIN:

	INIT_S

	asr	I, N, #2
	cmp	I, xzr
	ble	.Laxpy_kernel_S1

.Laxpy_kernel_S4:

	KERNEL_S1
	KERNEL_S1
	KERNEL_S1
	KERNEL_S1

	subs	I, I, #1
	bne	.Laxpy_kernel_S4

.Laxpy_kernel_S1:

	ands	I, N, #3
	ble	.Laxpy_kernel_L999

.Laxpy_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     .Laxpy_kernel_S10

.Laxpy_kernel_L999:

	mov	w0, wzr
	ret
