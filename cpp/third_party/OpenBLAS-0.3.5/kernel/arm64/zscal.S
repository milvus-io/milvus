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
#define I	x5	/* loop variable */
#define X_COPY	x6	/* Copy of X */

/*******************************************************************************
* Macro definitions
*******************************************************************************/

#if !defined(DOUBLE)
#define	DA_R	s0	/* real scale input value */
#define	DA_I	s1	/* imaginary scale input value */
#else
#define	DA_R	d0	/* real scale input value */
#define	DA_I	d1	/* imaginary scale input value */
#endif

/******************************************************************************/

.macro INIT

#if !defined(DOUBLE)
	ins	v0.s[1], v0.s[0]		// v0 = DA_R, DA_R
#else
	ins	v0.d[1], v0.d[0]		// v0 = DA_R, DA_R 
#endif

.endm

.macro KERNEL_F1
#if !defined(DOUBLE)
	ld1	{v2.2s}, [X]			// X1, X0
	fmul	s3, DA_R, v2.s[0]		// DA_R*X0
	fmul	s5, DA_I, v2.s[1]		// DA_I*X1
	fsub	s3, s3, s5			// DA_R*X0-DA_I*X1

	fmul	s4, DA_I, v2.s[0]		// DA_I*X0
	fmul	s5, DA_R, v2.s[1]		// DA_R*X1
	fadd	s4, s4, s5			// DA_I*X0+DA_R*X1

	ins	v3.s[1], v4.s[0]		// DA_R*X1+DA_I*X0, DA_R*X0-DA_I*X1
	st1	{v3.2s}, [X], #8
#else
	ld1	{v2.2d}, [X]			// X1, X0
	fmul	d3, DA_R, v2.d[0]		// DA_R*X0
	fmul	d5, DA_I, v2.d[1]		// DA_I*X1
	fsub	d3, d3, d5			// DA_R*X0-DA_I*X1

	fmul	d4, DA_I, v2.d[0]		// DA_I*X0
	fmul	d5, DA_R, v2.d[1]		// DA_R*X1
	fadd	d4, d4, d5			// DA_I*X0+DA_R*X1

	ins	v3.d[1], v4.d[0]		// DA_R*X1+DA_I*X0, DA_R*X0-DA_I*X1
	st1	{v3.2d}, [X], #16
#endif
.endm

.macro KERNEL_INIT_F4

#if !defined(DOUBLE)
	ins	v16.s[0], v0.s[0]
	ins	v16.s[1], v16.s[0]
	ins	v16.d[1], v16.d[0]
	ins	v17.s[0], v1.s[0]
	ins	v17.s[1], v17.s[0]
	ins	v17.d[1], v17.d[0]
#else //DOUBLE
	ins	v16.d[0], v0.d[0]
	ins	v16.d[1], v16.d[0]
	ins	v17.d[0], v1.d[0]
	ins	v17.d[1], v17.d[0]
#endif

.endm

.macro KERNEL_F4

#if !defined(DOUBLE)
	ld2	{v2.4s, v3.4s}, [X], #32

	fmul	v4.4s, v2.4s, v16.4s
	fmul	v6.4s, v3.4s, v17.4s
	fsub	v4.4s, v4.4s, v6.4s

	fmul	v5.4s, v2.4s, v17.4s
	fmul	v6.4s, v3.4s, v16.4s
	fadd	v5.4s, v5.4s, v6.4s

	st2	{v4.4s, v5.4s}, [X_COPY], #32
#else // DOUBLE
	ld2	{v2.2d, v3.2d}, [X], #32

	fmul	v4.2d, v2.2d, v16.2d
	fmul	v6.2d, v3.2d, v17.2d
	fsub	v4.2d, v4.2d, v6.2d
	fmul	v5.2d, v2.2d, v17.2d
	fmul	v6.2d, v3.2d, v16.2d
	fadd	v5.2d, v5.2d, v6.2d

	st2	{v4.2d, v5.2d}, [X_COPY], #32

	ld2	{v18.2d, v19.2d}, [X], #32

	fmul	v20.2d, v18.2d, v16.2d
	fmul	v6.2d, v19.2d, v17.2d
	fsub	v20.2d, v20.2d, v6.2d
	fmul	v21.2d, v18.2d, v17.2d
	fmul	v6.2d, v19.2d, v16.2d
	fadd	v21.2d, v21.2d, v6.2d

	st2	{v20.2d, v21.2d}, [X_COPY], #32
#endif
	PRFM	PLDL1KEEP, [X, #1024]
.endm

.macro INIT_S

#if !defined(DOUBLE)
	lsl	INC_X, INC_X, #3
#else
	lsl	INC_X, INC_X, #4
#endif

.endm

.macro KERNEL_S1
#if !defined(DOUBLE)
	ld1	{v2.2s}, [X]			// X1, X0
	fmul	s3, DA_R, v2.s[0]		// DA_R*X0
	fmul	s5, DA_I, v2.s[1]		// DA_I*X1
	fsub	s3, s3, s5			// DA_R*X0-DA_I*X1

	fmul	s4, DA_I, v2.s[0]		// DA_I*X0
	fmul	s5, DA_R, v2.s[1]		// DA_R*X1
	fadd	s4, s4, s5			// DA_I*X0+DA_R*X1

	ins	v3.s[1], v4.s[0]		// DA_R*X1+DA_I*X0, DA_R*X0-DA_I*X1
	st1	{v3.2s}, [X], INC_X
#else
	ld1	{v2.2d}, [X]			// X1, X0
	fmul	d3, DA_R, v2.d[0]		// DA_R*X0
	fmul	d5, DA_I, v2.d[1]		// DA_I*X1
	fsub	d3, d3, d5			// DA_R*X0-DA_I*X1

	fmul	d4, DA_I, v2.d[0]		// DA_I*X0
	fmul	d5, DA_R, v2.d[1]		// DA_R*X1
	fadd	d4, d4, d5			// DA_I*X0+DA_R*X1

	ins	v3.d[1], v4.d[0]		// DA_R*X1+DA_I*X0, DA_R*X0-DA_I*X1
	st1	{v3.2d}, [X], INC_X
#endif
.endm

/*******************************************************************************
* End of macro definitions
*******************************************************************************/

	PROLOGUE
	
	b	zscal_begin
data_ar:
	.word	0x3e44fae6
data_ai:
	.word	0x3d320fa2
data_xr:
	.word	0x3f4baff1
data_xi:
	.word	0xbe8ef0bd

zscal_begin:

	ldr	s20, data_ar
	ldr	s21, data_ai
	ldr	s22, data_xr
	ldr	s23, data_xi
	
	fmul	s24, s22, s21
	fmla	s24, s23, v20.s[0]

	fmul	s25, s22, s21
	fmul	s26, s23, s20
	fadd	s25, s25, s26

	mov	X_COPY, X

	cmp	N, xzr
	ble	.Lzscal_kernel_L999

	fcmp	DA_R, #0.0
	bne	.Lzscal_kernel_R_non_zero

	fcmp	DA_I, #0.0
	beq	.Lzscal_kernel_RI_zero

	b	.Lzscal_kernel_R_zero

.Lzscal_kernel_R_non_zero:

	fcmp	DA_I, #0.0
	beq	.Lzscal_kernel_I_zero

/*******************************************************************************
* A_R != 0 && A_I != 0
*******************************************************************************/

.Lzscal_kernel_RI_non_zero:

	INIT

	cmp	INC_X, #1
	bne	.Lzscal_kernel_S_BEGIN

.Lzscal_kernel_F_BEGIN:

	asr	I, N, #2
	cmp	I, xzr
	beq	.Lzscal_kernel_F1

	KERNEL_INIT_F4

.Lzscal_kernel_F4:

	KERNEL_F4

	subs	I, I, #1
	bne	.Lzscal_kernel_F4

.Lzscal_kernel_F1:

	ands	I, N, #3
	ble	.Lzscal_kernel_L999

.Lzscal_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
        bne     .Lzscal_kernel_F10

	mov	w0, wzr
	ret

.Lzscal_kernel_S_BEGIN:

	INIT_S

	asr	I, N, #2
	cmp	I, xzr
	ble	.Lzscal_kernel_S1

.Lzscal_kernel_S4:

	KERNEL_S1
	KERNEL_S1
	KERNEL_S1
	KERNEL_S1

	subs	I, I, #1
	bne	.Lzscal_kernel_S4

.Lzscal_kernel_S1:

	ands	I, N, #3
	ble	.Lzscal_kernel_L999

.Lzscal_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     .Lzscal_kernel_S10

.Lzscal_kernel_L999:

	mov	w0, wzr
	ret

/*******************************************************************************
* A_R == 0 && A_I != 0
*******************************************************************************/

.Lzscal_kernel_R_zero:
	INIT_S

#if !defined(DOUBLE)
	eor	v2.16b, v2.16b, v2.16b
	fsub	s2, s2, DA_I
	ins	v1.s[1], v2.s[0]		// v1 = -DA_I, DA_I 
#else
	eor	v2.16b, v2.16b, v2.16b
	fsub	d2, d2, DA_I
	ins	v1.d[1], v2.d[0]		// v1 = -DA_I, DA_I
#endif

.Lzscal_kernel_R_zero_1:
#if !defined(DOUBLE)
	ld1	{v2.2s}, [X]			// X1, X0
	fmul	v2.2s, v2.2s, v1.2s		// -DA_I*X1, DA_I*X0
	ext	v2.8b, v2.8b, v2.8b, #4		// DA_I*X0, -DA_I*X1
	st1	{v2.2s}, [X]
#else
	ld1	{v2.2d}, [X]			// X1, X0
	fmul	v2.2d, v2.2d, v1.2d		// -DA_I*X1, DA_I*X0
	ext	v2.16b, v2.16b, v2.16b, #8	// DA_I*X0, -DA_I*X1
	st1	{v2.2d}, [X]
#endif
	add	X, X, INC_X
	subs	N, N, #1
	bne	.Lzscal_kernel_R_zero_1

	mov	w0, wzr
	ret

/*******************************************************************************
* A_R != 0 && A_I == 0
*******************************************************************************/

.Lzscal_kernel_I_zero:
	INIT_S
#if !defined(DOUBLE)
	ins	v0.s[1], v0.s[0]		// v0 = DA_R, DA_R
#else
	ins	v0.d[1], v0.d[0]		// v0 = DA_R, DA_R 
#endif

.Lzscal_kernel_I_zero_1:
#if !defined(DOUBLE)
	ld1	{v2.2s}, [X]			// X1, X0
	fmul	v2.2s, v2.2s, v0.2s		// DA_R*X1, DA_R*X0
	st1	{v2.2s}, [X]
#else
	ld1	{v2.2d}, [X]			// X1, X0
	fmul	v2.2d, v2.2d, v0.2d		// DA_R*X1, DA_R*X0
	st1	{v2.2d}, [X]
#endif
	add	X, X, INC_X
	subs	N, N, #1
	bne	.Lzscal_kernel_I_zero_1

	mov	w0, wzr
	ret

/*******************************************************************************
* A_R == 0 && A_I == 0
*******************************************************************************/

.Lzscal_kernel_RI_zero:

	INIT_S

.Lzscal_kernel_RI_zero_1:

	stp	DA_R, DA_I, [X]
	add	X, X, INC_X
	subs	N, N, #1
	bne	.Lzscal_kernel_RI_zero_1

	mov	w0, wzr
	ret

	EPILOGUE
