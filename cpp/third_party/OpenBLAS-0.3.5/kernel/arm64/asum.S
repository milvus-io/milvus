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
#define I	x5	/* loop variable */

/*******************************************************************************
* Macro definitions
*******************************************************************************/

#if !defined(DOUBLE)
#define REG0	wzr
#define SUMF	s0
#define TMPF	s1
#define TMPVF	{v1.s}[0]
#define SZ	4
#else
#define REG0	xzr
#define SUMF	d0
#define TMPF	d1
#define TMPVF	{v1.d}[0]
#define SZ	8
#endif

/******************************************************************************/

.macro KERNEL_F1
	ldr	TMPF, [X], #SZ
	fabs	TMPF, TMPF
	fadd	SUMF, SUMF, TMPF
.endm

.macro KERNEL_F8
#if !defined(DOUBLE)
	ld1	{v1.4s, v2.4s}, [X], #32	// Load [X3, X2, X1, X0]
	fabs	v1.4s, v1.4s			// ABS() each value
	fabs	v2.4s, v2.4s			// ABS() each value
	fadd	v1.4s, v1.4s, v2.4s		// [X3+X1, X2+X0]
	fadd	v0.4s, v0.4s, v1.4s		// [X3+X1, X2+X0]
	PRFM	PLDL1KEEP, [X, #1024]
#else // DOUBLE
	ld1	{v2.2d, v3.2d, v4.2d, v5.2d}, [X]
	add	X, X, #64
	fabs	v2.2d, v2.2d
	fabs	v3.2d, v3.2d
	fabs	v4.2d, v4.2d
	fabs	v5.2d, v5.2d

	PRFM	PLDL1KEEP, [X, #1024]

	fadd	v2.2d, v2.2d, v3.2d
	fadd	v4.2d, v4.2d, v5.2d
	fadd	v0.2d, v0.2d, v2.2d
	fadd	v0.2d, v0.2d, v4.2d
#endif
.endm

.macro KERNEL_F8_FINALIZE
#if !defined(DOUBLE)
	ext	v1.16b, v0.16b, v0.16b, #8
	fadd	v0.2s, v0.2s, v1.2s
	faddp	SUMF, v0.2s
#else
	faddp	SUMF, v0.2d
#endif
.endm

.macro INIT_S
#if !defined(DOUBLE)
	lsl	INC_X, INC_X, #2
#else
	lsl	INC_X, INC_X, #3
#endif
.endm

.macro KERNEL_S1
	ld1	TMPVF, [X], INC_X
	fabs	TMPF, TMPF
	fadd	SUMF, SUMF, TMPF
.endm

/*******************************************************************************
* End of macro definitions
*******************************************************************************/

	PROLOGUE

	fmov	SUMF, REG0
#if !defined(DOUBLE)
	fmov	s1, SUMF
#else
	fmov	d1, SUMF
#endif

	cmp	N, xzr
	ble	.Lasum_kernel_L999
	cmp	INC_X, xzr
	ble	.Lasum_kernel_L999

	cmp	INC_X, #1
	bne	.Lasum_kernel_S_BEGIN

.Lasum_kernel_F_BEGIN:

	asr	I, N, #3
	cmp	I, xzr
	beq	.Lasum_kernel_F1

.Lasum_kernel_F8:

	KERNEL_F8

	subs	I, I, #1
	bne	.Lasum_kernel_F8

	KERNEL_F8_FINALIZE

.Lasum_kernel_F1:

	ands	I, N, #7
	ble	.Lasum_kernel_L999

.Lasum_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
        bne     .Lasum_kernel_F10

.Lasum_kernel_L999:
	ret

.Lasum_kernel_S_BEGIN:

	INIT_S

	asr	I, N, #2
	cmp	I, xzr
	ble	.Lasum_kernel_S1

.Lasum_kernel_S4:

	KERNEL_S1
	KERNEL_S1
	KERNEL_S1
	KERNEL_S1

	subs	I, I, #1
	bne	.Lasum_kernel_S4

.Lasum_kernel_S1:

	ands	I, N, #3
	ble	.Lasum_kernel_L999

.Lasum_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     .Lasum_kernel_S10

	ret

	EPILOGUE
