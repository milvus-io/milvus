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
#define	X_PTR	x10	/* loop Y vector address */
#define J	x11	/* loop variable */
#define I	x12	/* loop variable */

#define A_PRE_SIZE	768
#define X_PRE_SIZE	768

/*******************************************************************************
* Macro definitions
*******************************************************************************/

#if !defined(DOUBLE)
#define ALPHA_R		s0
#define ALPHA_I		s1
#define ALPHA_R_COPY	s7
#define ALPHA_I_COPY	s8
#define SHZ		3
#else
#define ALPHA_R		d0
#define ALPHA_I		d1
#define ALPHA_R_COPY	d7
#define ALPHA_I_COPY	d8
#define SHZ		4
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

.macro INIT
#if !defined(XCONJ)
#if !defined(DOUBLE)
	ins	v0.s[1], v0.s[0]		// v0 = ALPHA_R, ALPHA_R 
	eor	v2.16b, v2.16b, v2.16b
	fsub	s2, s2, ALPHA_I
	ins	v1.s[1], v2.s[0]
	ext	v1.8b, v1.8b, v1.8b, #4		// v1 = ALPHA_I, -ALPHA_I 
#else
	ins	v0.d[1], v0.d[0]		// v0 = ALPHA_R, ALPHA_R 
	eor	v2.16b, v2.16b, v2.16b
	fsub	d2, d2, ALPHA_I
	ins	v1.d[1], v2.d[0]
	ext	v1.16b, v1.16b, v1.16b, #8	// v1 = ALPHA_I, -ALPHA_I 
#endif
#else // XCONJ
#if !defined(DOUBLE)
	eor	v2.16b, v2.16b, v2.16b
	fsub	s2, s2, ALPHA_R
	ins	v0.s[1], v2.s[0]		// v0 = -ALPHA_R, ALPHA_R 
	ins	v1.s[1], v1.s[0]		// v1 = ALPHA_I, ALPHA_I 
#else
	eor	v2.16b, v2.16b, v2.16b
	fsub	d2, d2, ALPHA_R
	ins	v0.d[1], v2.d[0]		// v0 = -ALPHA_R, ALPHA_R 
	ins	v1.d[1], v1.d[0]		// v1 = ALPHA_I, ALPHA_I 
#endif
#endif
.endm

.macro INIT_LOOP
	fmov	d9, xzr				// TEMP_R = [0, 0]
	fmov	d10, xzr			// TEMP_I = [0, 0]
#if !defined(DOUBLE)
#else
	fmov	d15, xzr			// TEMP_R = [0, 0]
	fmov	d16, xzr			// TEMP_I = [0, 0]
#endif

	fmov	d2, xzr				// TEMP = [0, 0]
.endm

.macro KERNEL_F4
#if !defined(DOUBLE)

	ld2	{v11.4s, v12.4s}, [X_PTR], #32
	ld2	{v13.4s, v14.4s}, [A_PTR], #32
	prfm    PLDL1STRM, [X_PTR, #X_PRE_SIZE]
	prfm    PLDL1STRM, [A_PTR, #A_PRE_SIZE]

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	fmla	v9.4s,  v11.4s, v13.4s		// [+ R(X) * A_R]
	fmls	v9.4s,  v12.4s, v14.4s		// [- I(X) * A_I]
	fmla	v10.4s, v11.4s, v14.4s		// [+ R(X) * A_I]
	fmla	v10.4s, v12.4s, v13.4s		// [+ I(X) * A_R]
#else
	fmla	v9.4s,  v11.4s, v13.4s		// [+ R(X) * A_R]
	fmla	v9.4s,  v12.4s, v14.4s		// [+ I(X) * A_I]
	fmls	v10.4s, v11.4s, v14.4s		// [- R(X) * A_I]
	fmla	v10.4s, v12.4s, v13.4s		// [+ I(X) * A_R]
#endif

#else // DOUBLE
	ld2	{v11.2d, v12.2d}, [X_PTR], #32
	ld2	{v13.2d, v14.2d}, [A_PTR], #32
	prfm    PLDL1STRM, [X_PTR, #X_PRE_SIZE]

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	fmla	v9.2d,  v11.2d, v13.2d		// [+ R(X) * A_R]
	fmls	v9.2d,  v12.2d, v14.2d		// [- I(X) * A_I]
	fmla	v10.2d, v11.2d, v14.2d		// [+ R(X) * A_I]
	fmla	v10.2d, v12.2d, v13.2d		// [+ I(X) * A_R]
#else
	fmla	v9.2d,  v11.2d, v13.2d		// [+ R(X) * A_R]
	fmla	v9.2d,  v12.2d, v14.2d		// [+ I(X) * A_I]
	fmls	v10.2d, v11.2d, v14.2d		// [- R(X) * A_I]
	fmla	v10.2d, v12.2d, v13.2d		// [+ I(X) * A_R]
#endif

	ld2	{v17.2d, v18.2d}, [X_PTR], #32
	ld2	{v19.2d, v20.2d}, [A_PTR], #32
	prfm    PLDL1STRM, [A_PTR, #A_PRE_SIZE]

#if (!defined(CONJ) && !defined(XCONJ)) || (defined(CONJ) && defined(XCONJ))
	fmla	v15.2d, v17.2d, v19.2d		// [+ R(X) * A_R]
	fmls	v15.2d, v18.2d, v20.2d		// [- I(X) * A_I]
	fmla	v16.2d, v17.2d, v20.2d		// [+ R(X) * A_I]
	fmla	v16.2d, v18.2d, v19.2d		// [+ I(X) * A_R]
#else
	fmla	v15.2d, v17.2d, v19.2d		// [+ R(X) * A_R]
	fmla	v15.2d, v18.2d, v20.2d		// [- I(X) * A_I]
	fmls	v16.2d, v17.2d, v20.2d		// [+ R(X) * A_I]
	fmla	v16.2d, v18.2d, v19.2d		// [+ I(X) * A_R]
#endif

#endif //DOUBLE
.endm

.macro KERNEL_F4_FINALIZE
#if !defined(DOUBLE)
	ext	v21.16b, v9.16b, v9.16b, #8
	fadd	v9.2s, v9.2s, v21.2s
	faddp	s9, v9.2s

	ext	v21.16b, v10.16b, v10.16b, #8
	fadd	v10.2s, v10.2s, v21.2s
	faddp	s10, v10.2s

	ins	v2.s[0], v9.s[0]
	ins	v2.s[1], v10.s[0]
#else
	fadd	v9.2d, v9.2d, v15.2d
	fadd	v10.2d, v10.2d, v16.2d

	faddp	d9, v9.2d
	faddp	d10, v10.2d

	ins	v2.d[0], v9.d[0]
	ins	v2.d[1], v10.d[0]
#endif
.endm


.macro KERNEL_F1
#if !defined(DOUBLE)
	ld1r	{v4.2s}, [A_PTR], #4		// [A0, A0]
	ld1	{v5.s}[0], [A_PTR], #4		// A1
	ld1	{v6.2s}, [X_PTR], #8		// [X1, X0]
	eor	v16.16b, v16.16b, v16.16b
	fsub	s16, s16, s5
	ins	v5.s[1], v16.s[0]		// [-A1, A1]
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
	ext	v5.8b, v5.8b, v5.8b, #4		// [A1, -A1]
#endif
	ext	v7.8b, v6.8b, v6.8b, #4		// [X0, X1]
	fmla	v2.2s, v4.2s, v6.2s
	fmla	v2.2s, v5.2s, v7.2s
#else // DOUBLE
	ld1r	{v4.2d}, [A_PTR], #8		// [A0, A0]
	ld1	{v5.d}[0], [A_PTR], #8		// A1
	ld1	{v6.2d}, [X_PTR], #16		// [X1, X0]
	eor	v16.16b, v16.16b, v16.16b
	fsub	d16, d16, d5
	ins	v5.d[1], v16.d[0]		// [-A1, A1]
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
	ext	v5.16b, v5.16b, v5.16b, #8	// [A1, -A1]
#endif
	ext	v7.16b, v6.16b, v6.16b, #8	// [X0, X1]
	fmla	v2.2d, v4.2d, v6.2d
	fmla	v2.2d, v5.2d, v7.2d
#endif
.endm

.macro INIT_S
	lsl	INC_X, INC_X, #SHZ
.endm

.macro KERNEL_S1
#if !defined(DOUBLE)
	ld1r	{v4.2s}, [A_PTR], #4		// [A0, A0]
	ld1	{v5.s}[0], [A_PTR], #4		// A1
	ld1	{v6.2s}, [X_PTR], INC_X		// [X1, X0]
	eor	v16.16b, v16.16b, v16.16b
	fsub	s16, s16, s5
	ins	v5.s[1], v16.s[0]		// [-A1, A1]
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
	ext	v5.8b, v5.8b, v5.8b, #4		// [A1, -A1]
#endif
	ext	v7.8b, v6.8b, v6.8b, #4		// [X0, X1]
	fmla	v2.2s, v4.2s, v6.2s
	fmla	v2.2s, v5.2s, v7.2s
#else // DOUBLE
	ld1r	{v4.2d}, [A_PTR], #8		// [A0, A0]
	ld1	{v5.d}[0], [A_PTR], #8		// A1
	ld1	{v6.2d}, [X_PTR], INC_X		// [X1, X0]
	eor	v16.16b, v16.16b, v16.16b
	fsub	d16, d16, d5
	ins	v5.d[1], v16.d[0]		// [-A1, A1]
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
	ext	v5.16b, v5.16b, v5.16b, #8	// [A1, -A1]
#endif
	ext	v7.16b, v6.16b, v6.16b, #8	// [X0, X1]
	fmla	v2.2d, v4.2d, v6.2d
	fmla	v2.2d, v5.2d, v7.2d
#endif
.endm

/*******************************************************************************
* End of macro definitions
*******************************************************************************/

	PROLOGUE

	ldr	INC_Y, [sp]
	SAVE_REGS

	cmp	N, xzr
	ble	.Lzgemv_t_kernel_L999
	cmp	M, xzr
	ble	.Lzgemv_t_kernel_L999

	lsl	LDA, LDA, #SHZ
	lsl	INC_Y, INC_Y, #SHZ
	mov	J, N

	INIT

	cmp	INC_X, #1
	bne	.Lzgemv_t_kernel_S_BEGIN

.Lzgemv_t_kernel_F_LOOP:

	mov	A_PTR, A
	mov	X_PTR, X

	INIT_LOOP

	asr	I, M, #2
	cmp	I, xzr
	beq	.Lzgemv_t_kernel_F1

.Lzgemv_t_kernel_F4:

	KERNEL_F4

	subs	I, I, #1
	bne	.Lzgemv_t_kernel_F4

	KERNEL_F4_FINALIZE

.Lzgemv_t_kernel_F1:

	ands	I, M, #3
	ble	.Lzgemv_t_kernel_F_END

.Lzgemv_t_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
        bne     .Lzgemv_t_kernel_F10

.Lzgemv_t_kernel_F_END:

#if !defined(DOUBLE)
	ld1	{v4.2s}, [Y]
	ext	v3.8b, v2.8b, v2.8b, #4		// [TEMP_R, TEMP_I]
	fmla	v4.2s, v0.2s, v2.2s
	fmla	v4.2s, v1.2s, v3.2s
	st1	{v4.2s}, [Y], INC_Y
#else // DOUBLE
	ld1	{v4.2d}, [Y]
	ext	v3.16b, v2.16b, v2.16b, #8	// [TEMP_R, TEMP_I]
	fmla	v4.2d, v0.2d, v2.2d
	fmla	v4.2d, v1.2d, v3.2d
	st1	{v4.2d}, [Y], INC_Y
#endif

	add	A, A, LDA
	subs    J, J, #1
        bne     .Lzgemv_t_kernel_F_LOOP

	b	.Lzgemv_t_kernel_L999

.Lzgemv_t_kernel_S_BEGIN:

	INIT_S

.Lzgemv_t_kernel_S_LOOP:

	mov	A_PTR, A
	mov	X_PTR, X
	INIT_LOOP

	asr	I, M, #2
	cmp	I, xzr
	ble	.Lzgemv_t_kernel_S1

.Lzgemv_t_kernel_S4:

	KERNEL_S1
	KERNEL_S1
	KERNEL_S1
	KERNEL_S1

	subs	I, I, #1
	bne	.Lzgemv_t_kernel_S4

.Lzgemv_t_kernel_S1:

	ands	I, M, #3
	ble	.Lzgemv_t_kernel_S_END

.Lzgemv_t_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     .Lzgemv_t_kernel_S10

.Lzgemv_t_kernel_S_END:

#if !defined(DOUBLE)
	ld1	{v4.2s}, [Y]
	ext	v3.8b, v2.8b, v2.8b, #4		// [TEMP_R, TEMP_I]
	fmla	v4.2s, v0.2s, v2.2s
	fmla	v4.2s, v1.2s, v3.2s
	st1	{v4.2s}, [Y], INC_Y
#else // DOUBLE
	ld1	{v4.2d}, [Y]
	ext	v3.16b, v2.16b, v2.16b, #8	// [TEMP_R, TEMP_I]
	fmla	v4.2d, v0.2d, v2.2d
	fmla	v4.2d, v1.2d, v3.2d
	st1	{v4.2d}, [Y], INC_Y
#endif

	add	A, A, LDA
	subs    J, J, #1
        bne     .Lzgemv_t_kernel_S_LOOP

.Lzgemv_t_kernel_L999:
	RESTORE_REGS
	mov	w0, wzr
	ret

	EPILOGUE
