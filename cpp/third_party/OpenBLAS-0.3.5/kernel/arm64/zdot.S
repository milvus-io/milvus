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
#if !defined(DSDOT)
#define REG0	wzr
#define DOTF	s0
#else // DSDOT
#define REG0	xzr
#define DOTF	d0
#endif
#define DOTI	s1
#define TMPX	s2
#define LD1VX	{v2.s}[0]
#define TMPY	s3
#define LD1VY	{v3.s}[0]
#define TMPVY	v3.s[0]
#define SZ	4
#else
#define REG0	xzr
#define DOTF	d0
#define DOTI	d1
#define TMPX	d2
#define LD1VX	{v2.d}[0]
#define TMPY	d3
#define LD1VY	{v3.d}[0]
#define TMPVY	v3.d[0]
#define SZ	8
#endif

/******************************************************************************/

.macro KERNEL_F1

#if !defined(DOUBLE)
	ld1	{v2.2s}, [X], #8		// V2 = X[ix+1], X[ix]; X += 2
	ld1	{v3.2s}, [Y], #8		// V3 = Y[iy+1], Y[iy]; Y += 2
	ins	v4.s[0], v2.s[1]		// V4 = X[ix+1]
#if !defined(CONJ)
	fmla	DOTF, s2, v3.s[0]		// dot[0] += X[ix] * Y[iy]
	fmls	DOTF, s4, v3.s[1]		// dot[0] -= X[ix+1] * Y[iy+1]
	fmla	DOTI, s4, v3.s[0]		// dot[1] += X[ix+1] * Y[iy]
	fmla	DOTI, s2, v3.s[1]		// dot[1] += X[ix] * Y[iy+1]
#else
	fmla	DOTF, s2, v3.s[0]		// dot[0] += X[ix] * Y[iy]
	fmla	DOTF, s4, v3.s[1]		// dot[0] += X[ix+1] * Y[iy+1]
	fmls	DOTI, s4, v3.s[0]		// dot[1] -= X[ix+1] * Y[iy]
	fmla	DOTI, s2, v3.s[1]		// dot[1] += X[ix] * Y[iy+1]
#endif
#else // DOUBLE
	ld1	{v2.2d}, [X], #16		// V2 = X[ix+1], X[ix]; X += 2
	ld1	{v3.2d}, [Y], #16		// V3 = Y[iy+1], Y[iy]; Y += 2
	ins	v4.d[0], v2.d[1]		// V4 = X[ix+1]
#if !defined(CONJ)
	fmla	DOTF, d2, v3.d[0]		// dot[0] += X[ix] * Y[iy]
	fmls	DOTF, d4, v3.d[1]		// dot[0] -= X[ix+1] * Y[iy+1]
	fmla	DOTI, d4, v3.d[0]		// dot[1] += X[ix+1] * Y[iy]
	fmla	DOTI, d2, v3.d[1]		// dot[1] += X[ix] * Y[iy+1]
#else
	fmla	DOTF, d2, v3.d[0]		// dot[0] += X[ix] * Y[iy]
	fmla	DOTF, d4, v3.d[1]		// dot[0] += X[ix+1] * Y[iy+1]
	fmls	DOTI, d4, v3.d[0]		// dot[1] -= X[ix+1] * Y[iy]
	fmla	DOTI, d2, v3.d[1]		// dot[1] += X[ix] * Y[iy+1]
#endif
#endif

.endm


.macro KERNEL_F4

#if !defined(DOUBLE)
	ld2	{v2.4s, v3.4s}, [X], #32	// V2 = X[ix+1], X[ix]; X += 2
	ld2	{v4.4s, v5.4s}, [Y], #32	// V2 = X[ix+1], X[ix]; X += 2

	fmla	v0.4s, v2.4s, v4.4s		// dot[0] += X[ix] * Y[iy]
	fmla	v1.4s, v2.4s, v5.4s		// dot[1] += X[ix] * Y[iy+1]
	PRFM	PLDL1KEEP, [X, #1024]
	PRFM	PLDL1KEEP, [Y, #1024]
#if !defined(CONJ)
	fmls	v0.4s, v3.4s, v5.4s		// dot[0] -= X[ix+1] * Y[iy+1]
	fmla	v1.4s, v3.4s, v4.4s		// dot[1] += X[ix+1] * Y[iy]
#else
	fmla	v0.4s, v3.4s, v5.4s		// dot[0] += X[ix+1] * Y[iy+1]
	fmls	v1.4s, v3.4s, v4.4s		// dot[1] -= X[ix+1] * Y[iy]
#endif
#else // DOUBLE
	ld2	{v2.2d, v3.2d}, [X], #32	// V2 = X[ix+1], X[ix]; X += 2
	ld2	{v16.2d, v17.2d}, [Y], #32

	fmla	v0.2d, v2.2d, v16.2d		// dot[0] += X[ix] * Y[iy]
	fmla	v1.2d, v2.2d, v17.2d		// dot[1] += X[ix] * Y[iy+1]
	ld2	{v4.2d, v5.2d}, [X], #32
	ld2	{v18.2d, v19.2d}, [Y], #32
	fmla	v0.2d, v4.2d, v18.2d		// dot[1] += X[ix] * Y[iy+1]
	fmla	v1.2d, v4.2d, v19.2d		// dot[1] += X[ix] * Y[iy+1]
	PRFM	PLDL1KEEP, [X, #1024]
	PRFM	PLDL1KEEP, [Y, #1024]
#if !defined(CONJ)
	fmls	v0.2d, v3.2d, v17.2d		// dot[0] -= X[ix+1] * Y[iy+1]
	fmls	v20.2d, v5.2d, v19.2d		// dot[0] -= X[ix+1] * Y[iy+1]
	fmla	v1.2d, v3.2d, v16.2d		// dot[1] += X[ix+1] * Y[iy]
	fmla	v21.2d, v5.2d, v18.2d		// dot[1] += X[ix+1] * Y[iy]
#else
	fmla	v0.2d, v3.2d, v17.2d		// dot[0] += X[ix+1] * Y[iy+1]
	fmla	v20.2d, v5.2d, v19.2d		// dot[0] += X[ix+1] * Y[iy+1]
	fmls	v1.2d, v3.2d, v16.2d		// dot[1] -= X[ix+1] * Y[iy]
	fmls	v21.2d, v5.2d, v18.2d		// dot[1] -= X[ix+1] * Y[iy]
#endif
#endif

.endm

.macro KERNEL_F4_FINALIZE
#if !defined(DOUBLE)
	ext	v2.16b, v0.16b, v0.16b, #8
	fadd	v0.2s, v0.2s, v2.2s
	faddp	DOTF, v0.2s
	ext	v3.16b, v1.16b, v1.16b, #8
	fadd	v1.2s, v1.2s, v3.2s
	faddp	DOTI, v1.2s
#else
	fadd	v0.2d, v0.2d, v20.2d
	faddp	DOTF, v0.2d
	fadd	v1.2d, v1.2d, v21.2d
	faddp	DOTI, v1.2d
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
	ld1	{v2.2s}, [X], INC_X		// V2 = X[ix+1], X[ix]; X += 2
	ld1	{v3.2s}, [Y], INC_Y		// V3 = Y[iy+1], Y[iy]; Y += 2
	ext	v4.8b, v2.8b, v2.8b, #4		// V4 = X[ix], X[ix+1]
#if !defined(CONJ)
	fmla	DOTF, s2, v3.s[0]		// dot[0] += X[ix] * Y[iy]
	fmls	DOTF, s4, v3.s[1]		// dot[0] -= X[ix+1] * Y[iy+1]
	fmla	DOTI, s4, v3.s[0]		// dot[1] += X[ix+1] * Y[iy]
	fmla	DOTI, s2, v3.s[1]		// dot[1] += X[ix] * Y[iy+1]
#else
	fmla	DOTF, s2, v3.s[0]		// dot[0] += X[ix] * Y[iy]
	fmla	DOTF, s4, v3.s[1]		// dot[0] += X[ix+1] * Y[iy+1]
	fmls	DOTI, s4, v3.s[0]		// dot[1] -= X[ix+1] * Y[iy]
	fmla	DOTI, s2, v3.s[1]		// dot[1] += X[ix] * Y[iy+1]
#endif
#else // DOUBLE
	ld1	{v2.2d}, [X], INC_X		// V2 = X[ix+1], X[ix]; X += 2
	ld1	{v3.2d}, [Y], INC_Y		// V3 = Y[iy+1], Y[iy]; Y += 2
	ext	v4.16b, v2.16b, v2.16b, #8	// V4 = X[ix], X[ix+1]
#if !defined(CONJ)
	fmla	DOTF, d2, v3.d[0]		// dot[0] += X[ix] * Y[iy]
	fmls	DOTF, d4, v3.d[1]		// dot[0] -= X[ix+1] * Y[iy+1]
	fmla	DOTI, d4, v3.d[0]		// dot[1] += X[ix+1] * Y[iy]
	fmla	DOTI, d2, v3.d[1]		// dot[1] += X[ix] * Y[iy+1]
#else
	fmla	DOTF, d2, v3.d[0]		// dot[0] += X[ix] * Y[iy]
	fmla	DOTF, d4, v3.d[1]		// dot[0] += X[ix+1] * Y[iy+1]
	fmls	DOTI, d4, v3.d[0]		// dot[1] -= X[ix+1] * Y[iy]
	fmla	DOTI, d2, v3.d[1]		// dot[1] += X[ix] * Y[iy+1]
#endif
#endif

.endm

/*******************************************************************************
* End of macro definitions
*******************************************************************************/

	PROLOGUE

	fmov	DOTF, REG0
	fmov	DOTI, DOTF
#if !defined(DOUBLE)
	fmov	s20, DOTF
	fmov	s21, DOTI
#else
	fmov	d20, DOTF
	fmov	d21, DOTI
#endif

	cmp	N, xzr
	ble	.Lzdot_kernel_L999

	cmp	INC_X, #1
	bne	.Lzdot_kernel_S_BEGIN
	cmp	INC_Y, #1
	bne	.Lzdot_kernel_S_BEGIN

.Lzdot_kernel_F_BEGIN:

	asr	I, N, #2
	cmp	I, xzr
	beq	.Lzdot_kernel_F1

.Lzdot_kernel_F4:

	KERNEL_F4

	subs	I, I, #1
	bne	.Lzdot_kernel_F4

	KERNEL_F4_FINALIZE

.Lzdot_kernel_F1:

	ands	I, N, #3
	ble	.Lzdot_kernel_L999

.Lzdot_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
        bne     .Lzdot_kernel_F10

	ret

.Lzdot_kernel_S_BEGIN:

	INIT_S

	asr	I, N, #2
	cmp	I, xzr
	ble	.Lzdot_kernel_S1

.Lzdot_kernel_S4:

	KERNEL_S1
	KERNEL_S1
	KERNEL_S1
	KERNEL_S1

	subs	I, I, #1
	bne	.Lzdot_kernel_S4

.Lzdot_kernel_S1:

	ands	I, N, #3
	ble	.Lzdot_kernel_L999

.Lzdot_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     .Lzdot_kernel_S10

.Lzdot_kernel_L999:

	ret

	EPILOGUE
