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
#define Y_COPY	x7	/* loop variable */

/*******************************************************************************
* Macro definitions
*******************************************************************************/

#if !defined(DOUBLE)
#define	DA_R	s0	/* scale input value */
#define	DA_I	s1	/* scale input value */
#define SZ	4
#else
#define	DA_R	d0	/* scale input value */
#define	DA_I	d1	/* scale input value */
#define SZ	8
#endif

/******************************************************************************/

.macro INIT

#if !defined(CONJ)
#if !defined(DOUBLE)
	ins	v0.s[1], v0.s[0]		// v0 = DA_R, DA_R 
	eor	v2.16b, v2.16b, v2.16b
	fsub	s2, s2, DA_I
	ins	v1.s[1], v2.s[0]		// v1 = -DA_I, DA_I 
	ext	v1.8b, v1.8b, v1.8b, #4		// v1 = DA_I, -DA_I
#else
	ins	v0.d[1], v0.d[0]		// v0 = DA_R, DA_R 
	eor	v2.16b, v2.16b, v2.16b
	fsub	d2, d2, DA_I
	ins	v1.d[1], v2.d[0]		// v1 = -DA_I, DA_I 
	ext	v1.16b, v1.16b, v1.16b, #8	// v1 = DA_I, -DA_I
#endif
#else
#if !defined(DOUBLE)
	eor	v2.16b, v2.16b, v2.16b
	fsub	s2, s2, DA_R
	ins	v0.s[1], v2.s[0]		// v0 = -DA_R, DA_R 
	ins	v1.s[1], v1.s[0]		// v1 = DA_I, DA_I 
#else
	eor	v2.16b, v2.16b, v2.16b
	fsub	d2, d2, DA_R
	ins	v0.d[1], v2.d[0]		// v0 = -DA_R, DA_R 
	ins	v1.d[1], v1.d[0]		// v1 = DA_I, DA_I 
#endif
#endif

.endm

.macro KERNEL_F1

#if !defined(DOUBLE)
	ld1	{v2.2s}, [X], #8		// V2 = X[ix+1], X[ix]; X += 2
	ld1	{v3.2s}, [Y]			// V3 = Y[iy+1], Y[iy]
	ext	v4.8b, v2.8b, v2.8b, #4		// V4 = X[ix], X[ix+1]
	fmla	v3.2s, v0.2s, v2.2s		// Y[iy]   += DA_R * X[ix]
						// Y[iy+1] += +-DA_R * X[ix+1]
	fmla	v3.2s, v1.2s, v4.2s		// Y[iy]   += +-DA_I * X[ix+1]
						// Y[iy+1] += DA_I * X[ix]
	st1	{v3.2s}, [Y], #8
#else
	ld1	{v2.2d}, [X], #16		// V2 = X[ix+1], X[ix]; X += 2
	ld1	{v3.2d}, [Y]			// V3 = Y[iy+1], Y[iy]
	ext	v4.16b, v2.16b, v2.16b, #8	// V4 = X[ix], X[ix+1]
	fmla	v3.2d, v0.2d, v2.2d		// Y[iy]   += DA_R * X[ix]
						// Y[iy+1] += +-DA_R * X[ix+1]
	fmla	v3.2d, v1.2d, v4.2d		// Y[iy]   += +-DA_I * X[ix+1]
						// Y[iy+1] += DA_I * X[ix]
	st1	{v3.2d}, [Y], #16
#endif

.endm

.macro KERNEL_INIT_F4

#if !defined(DOUBLE)
	ins	v16.s[0], v0.s[0]
	ins	v16.s[1], v16.s[0]
	ins	v16.d[1], v16.d[0]
#if !defined(CONJ)
	ins	v17.s[0], v1.s[1]
#else
	ins	v17.s[0], v1.s[0]
#endif
	ins	v17.s[1], v17.s[0]
	ins	v17.d[1], v17.d[0]
#else //DOUBLE
	ins	v16.d[0], v0.d[0]
	ins	v16.d[1], v16.d[0]
#if !defined(CONJ)
	ins	v17.d[0], v1.d[1]
#else
	ins	v17.d[0], v1.d[0]
#endif
	ins	v17.d[1], v17.d[0]
#endif

.endm

.macro KERNEL_F4

#if !defined(DOUBLE)
	ld2	{v2.4s, v3.4s}, [X], #32
	ld2	{v4.4s, v5.4s}, [Y_COPY], #32

	fmla	v4.4s, v2.4s, v16.4s
#if !defined(CONJ)
	fmls	v4.4s, v3.4s, v17.4s
#else
	fmla	v4.4s, v3.4s, v17.4s
#endif


	fmla	v5.4s, v2.4s, v17.4s
#if !defined(CONJ)
	fmla	v5.4s, v3.4s, v16.4s
#else
	fmls	v5.4s, v3.4s, v16.4s
#endif

	st2	{v4.4s, v5.4s}, [Y], #32
#else // DOUBLE
	ld2	{v2.2d, v3.2d}, [X], #32
	ld2	{v4.2d, v5.2d}, [Y_COPY], #32

	fmla	v4.2d, v2.2d, v16.2d
#if !defined(CONJ)
	fmls	v4.2d, v3.2d, v17.2d
#else
	fmla	v4.2d, v3.2d, v17.2d
#endif

	fmla	v5.2d, v2.2d, v17.2d
#if !defined(CONJ)
	fmla	v5.2d, v3.2d, v16.2d
#else
	fmls	v5.2d, v3.2d, v16.2d
#endif

	st2	{v4.2d, v5.2d}, [Y], #32

	ld2	{v18.2d, v19.2d}, [X], #32
	ld2	{v20.2d, v21.2d}, [Y_COPY], #32

	fmla	v20.2d, v18.2d, v16.2d
#if !defined(CONJ)
	fmls	v20.2d, v19.2d, v17.2d
#else
	fmla	v20.2d, v19.2d, v17.2d
#endif

	fmla	v21.2d, v18.2d, v17.2d
#if !defined(CONJ)
	fmla	v21.2d, v19.2d, v16.2d
#else
	fmls	v21.2d, v19.2d, v16.2d
#endif
	st2	{v20.2d, v21.2d}, [Y], #32
#endif
	PRFM	PLDL1KEEP, [X, #512]
	PRFM	PLDL1KEEP, [Y, #512]
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
	ld1	{v2.2s}, [X], INC_X		// V2 = X[ix+1], X[ix]; X += 2
	ld1	{v3.2s}, [Y]			// V3 = Y[iy+1], Y[iy]
	ext	v4.8b, v2.8b, v2.8b, #4		// V4 = X[ix], X[ix+1]
	fmla	v3.2s, v0.2s, v2.2s		// Y[iy]   += DA_R * X[ix]
						// Y[iy+1] += +-DA_R * X[ix+1]
	fmla	v3.2s, v1.2s, v4.2s		// Y[iy]   += +-DA_I * X[ix+1]
						// Y[iy+1] += DA_I * X[ix]
	st1	{v3.2s}, [Y], INC_Y
#else
	ld1	{v2.2d}, [X], INC_X		// V2 = X[ix+1], X[ix]; X += 2
	ld1	{v3.2d}, [Y]			// V3 = Y[iy+1], Y[iy]
	ext	v4.16b, v2.16b, v2.16b, #8	// V4 = X[ix], X[ix+1]
	fmla	v3.2d, v0.2d, v2.2d		// Y[iy]   += DA_R * X[ix]
						// Y[iy+1] += +-DA_R * X[ix+1]
	fmla	v3.2d, v1.2d, v4.2d		// Y[iy]   += +-DA_I * X[ix+1]
						// Y[iy+1] += DA_I * X[ix]
	st1	{v3.2d}, [Y], INC_Y
#endif

.endm

/*******************************************************************************
* End of macro definitions
*******************************************************************************/

	PROLOGUE

	cmp	N, xzr
	ble	.Lzaxpy_kernel_L999

	mov	Y_COPY, Y

	fcmp	DA_R, #0.0
	bne	.L1
	fcmp	DA_I, #0.0
	beq	.Lzaxpy_kernel_L999

.L1:
	INIT

	cmp	INC_X, #1
	bne	.Lzaxpy_kernel_S_BEGIN
	cmp	INC_Y, #1
	bne	.Lzaxpy_kernel_S_BEGIN

.Lzaxpy_kernel_F_BEGIN:

	asr	I, N, #2
	cmp	I, xzr
	beq	.Lzaxpy_kernel_F1

	KERNEL_INIT_F4

.Lzaxpy_kernel_F4:

	KERNEL_F4

	subs	I, I, #1
	bne	.Lzaxpy_kernel_F4

.Lzaxpy_kernel_F1:

	ands	I, N, #3
	ble	.Lzaxpy_kernel_L999

.Lzaxpy_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
        bne     .Lzaxpy_kernel_F10

	mov	w0, wzr
	ret

.Lzaxpy_kernel_S_BEGIN:

	INIT_S

	asr	I, N, #2
	cmp	I, xzr
	ble	.Lzaxpy_kernel_S1

.Lzaxpy_kernel_S4:

	KERNEL_S1
	KERNEL_S1
	KERNEL_S1
	KERNEL_S1

	subs	I, I, #1
	bne	.Lzaxpy_kernel_S4

.Lzaxpy_kernel_S1:

	ands	I, N, #3
	ble	.Lzaxpy_kernel_L999

.Lzaxpy_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     .Lzaxpy_kernel_S10

.Lzaxpy_kernel_L999:

	mov	w0, wzr
	ret
