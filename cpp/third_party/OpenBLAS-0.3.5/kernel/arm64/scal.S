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
#define	X_COPY	x5	/* X vector address */
#define	INC_X	x4	/* X stride */
#define I	x1	/* loop variable */

/*******************************************************************************
* Macro definitions
*******************************************************************************/

#if !defined(DOUBLE)
#define	DA	s0	/* scale input value */
#define	DAV	{v0.s}[0]
#define TMPF	s1
#define TMPVF	{v1.s}[0]
#define SZ	4
#else
#define	DA	d0	/* scale input value */
#define	DAV	{v0.d}[0]
#define TMPF	d1
#define TMPVF	{v1.d}[0]
#define SZ	8
#endif

/******************************************************************************/

.macro KERNEL_F1

	ldr	TMPF, [X]
	fmul	TMPF, TMPF, DA
	str	TMPF, [X], #SZ

.endm

.macro KERNEL_INIT_F8

#if !defined(DOUBLE)
	ins	v0.s[1], v0.s[0]
	ins	v0.s[2], v0.s[0]
	ins	v0.s[3], v0.s[0]
#else
	ins	v0.d[1], v0.d[0]
#endif

.endm

.macro KERNEL_F8
#if !defined(DOUBLE)
	ld1	{v1.4s, v2.4s}, [X]
	fmul	v1.4s, v1.4s, v0.4s
	fmul	v2.4s, v2.4s, v0.4s
	st1	{v1.4s, v2.4s}, [X], #32
#else // DOUBLE
	ld1	{v1.2d, v2.2d, v3.2d, v4.2d}, [X]
	fmul	v1.2d, v1.2d, v0.2d
	fmul	v2.2d, v2.2d, v0.2d
	fmul	v3.2d, v3.2d, v0.2d
	fmul	v4.2d, v4.2d, v0.2d
	st1	{v1.2d, v2.2d, v3.2d, v4.2d}, [X], #64
#endif
	PRFM	PLDL1KEEP, [X, #1024]
.endm

.macro INIT_S

#if !defined(DOUBLE)
	lsl	INC_X, INC_X, #2
#else
	lsl	INC_X, INC_X, #3
#endif

.endm

.macro KERNEL_S1
	ldr	TMPF, [X]
	fmul	TMPF, TMPF, DA
	st1	TMPVF, [X], INC_X
.endm

.macro KERNEL_S4
#if !defined(DOUBLE)
	ldr	s1, [X]
	add	X, X, INC_X
	fmul	s1, s1, s0
	str	s1, [X_COPY]
	add	X_COPY, X_COPY, INC_X

	ldr	s2, [X]
	add	X, X, INC_X
	fmul	s2, s2, s0
	str	s2, [X_COPY]
	add	X_COPY, X_COPY, INC_X

	ldr	s3, [X]
	add	X, X, INC_X
	fmul	s3, s3, s0
	str	s3, [X_COPY]
	add	X_COPY, X_COPY, INC_X

	ldr	s4, [X]
	add	X, X, INC_X
	fmul	s4, s4, s0
	str	s4, [X_COPY]
	add	X_COPY, X_COPY, INC_X
#else
	ldr	d1, [X]
	add	X, X, INC_X
	fmul	d1, d1, d0
	str	d1, [X_COPY]
	add	X_COPY, X_COPY, INC_X

	ldr	d2, [X]
	add	X, X, INC_X
	fmul	d2, d2, d0
	str	d2, [X_COPY]
	add	X_COPY, X_COPY, INC_X

	ldr	d3, [X]
	add	X, X, INC_X
	fmul	d3, d3, d0
	str	d3, [X_COPY]
	add	X_COPY, X_COPY, INC_X

	ldr	d4, [X]
	add	X, X, INC_X
	fmul	d4, d4, d0
	str	d4, [X_COPY]
	add	X_COPY, X_COPY, INC_X
#endif
.endm

/*******************************************************************************
* End of macro definitions
*******************************************************************************/

	PROLOGUE

	cmp	N, xzr
	ble	.Lscal_kernel_L999

	fcmp	DA, #0.0
	beq	.Lscal_kernel_zero

	cmp	INC_X, #1
	bne	.Lscal_kernel_S_BEGIN

.Lscal_kernel_F_BEGIN:

	asr	I, N, #3
	cmp	I, xzr
	beq	.Lscal_kernel_F1

	KERNEL_INIT_F8

.Lscal_kernel_F8:

	KERNEL_F8

	subs	I, I, #1
	bne	.Lscal_kernel_F8

.Lscal_kernel_F1:

	ands	I, N, #7
	ble	.Lscal_kernel_L999

.Lscal_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
        bne     .Lscal_kernel_F10

	mov	w0, wzr
	ret

.Lscal_kernel_S_BEGIN:

	INIT_S
	mov	X_COPY, X

	asr	I, N, #2
	cmp	I, xzr
	ble	.Lscal_kernel_S1

.Lscal_kernel_S4:

	KERNEL_S4

	subs	I, I, #1
	bne	.Lscal_kernel_S4

.Lscal_kernel_S1:

	ands	I, N, #3
	ble	.Lscal_kernel_L999

.Lscal_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     .Lscal_kernel_S10

.Lscal_kernel_L999:

	mov	w0, wzr
	ret

.Lscal_kernel_zero:

	INIT_S

.Lscal_kernel_Z1:

	st1	DAV, [X], INC_X
	subs    N, N, #1
        bne     .Lscal_kernel_Z1

	mov	w0, wzr
	ret

	EPILOGUE
