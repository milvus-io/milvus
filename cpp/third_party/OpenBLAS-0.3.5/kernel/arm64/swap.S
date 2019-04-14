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
#define TMP0	s0
#define TMPV0	{v0.s}[0]
#define TMP1	s1
#define TMPV1	{v1.s}[0]
#define SZ	4
#else
#define TMP0	d0
#define TMPV0	{v0.d}[0]
#define TMP1	d1
#define TMPV1	{v1.d}[0]
#define SZ	8
#endif

/******************************************************************************/

.macro KERNEL_F1

#if !defined(COMPLEX)
	ldr	TMP0, [X]
	ldr	TMP1, [Y]
	str	TMP0, [Y], #SZ
	str	TMP1, [X], #SZ
#else
#if !defined(DOUBLE)
	ld1	{v0.2s}, [X]
	ld1	{v1.2s}, [Y]
	st1	{v0.2s}, [Y], #8
	st1	{v1.2s}, [X], #8
#else
	ld1	{v0.2d}, [X]
	ld1	{v1.2d}, [Y]
	st1	{v0.2d}, [Y], #16
	st1	{v1.2d}, [X], #16
#endif
#endif

.endm

.macro KERNEL_F8

#if !defined(COMPLEX)
#if !defined(DOUBLE)
	ld1	{v0.4s, v1.4s}, [X]
	ld1	{v2.4s, v3.4s}, [Y]
	st1	{v0.4s, v1.4s}, [Y], #32
	st1	{v2.4s, v3.4s}, [X], #32
#else // DOUBLE
	ld1	{v0.4s, v1.4s}, [X]
	ld1	{v2.4s, v3.4s}, [Y]
	st1	{v0.4s, v1.4s}, [Y], #32
	st1	{v2.4s, v3.4s}, [X], #32
	ld1	{v0.4s, v1.4s}, [X]
	ld1	{v2.4s, v3.4s}, [Y]
	st1	{v0.4s, v1.4s}, [Y], #32
	st1	{v2.4s, v3.4s}, [X], #32
#endif
#else // COMPLEX
#if !defined(DOUBLE)
	ld1	{v0.4s, v1.4s}, [X]
	ld1	{v2.4s, v3.4s}, [Y]
	st1	{v0.4s, v1.4s}, [Y], #32
	st1	{v2.4s, v3.4s}, [X], #32
	ld1	{v0.4s, v1.4s}, [X]
	ld1	{v2.4s, v3.4s}, [Y]
	st1	{v0.4s, v1.4s}, [Y], #32
	st1	{v2.4s, v3.4s}, [X], #32
#else // DOUBLE
	ld1	{v0.4s, v1.4s}, [X]
	ld1	{v2.4s, v3.4s}, [Y]
	st1	{v0.4s, v1.4s}, [Y], #32
	st1	{v2.4s, v3.4s}, [X], #32
	ld1	{v0.4s, v1.4s}, [X]
	ld1	{v2.4s, v3.4s}, [Y]
	st1	{v0.4s, v1.4s}, [Y], #32
	st1	{v2.4s, v3.4s}, [X], #32
	ld1	{v0.4s, v1.4s}, [X]
	ld1	{v2.4s, v3.4s}, [Y]
	st1	{v0.4s, v1.4s}, [Y], #32
	st1	{v2.4s, v3.4s}, [X], #32
	ld1	{v0.4s, v1.4s}, [X]
	ld1	{v2.4s, v3.4s}, [Y]
	st1	{v0.4s, v1.4s}, [Y], #32
	st1	{v2.4s, v3.4s}, [X], #32
#endif
#endif

.endm

.macro INIT_S

#if !defined(COMPLEX)
#if !defined(DOUBLE)
	lsl	INC_X, INC_X, #2
	lsl	INC_Y, INC_Y, #2
#else
	lsl	INC_X, INC_X, #3
	lsl	INC_Y, INC_Y, #3
#endif
#else
#if !defined(DOUBLE)
	lsl	INC_X, INC_X, #3
	lsl	INC_Y, INC_Y, #3
#else
	lsl	INC_X, INC_X, #4
	lsl	INC_Y, INC_Y, #4
#endif
#endif

.endm

.macro KERNEL_S1

#if !defined(COMPLEX)
#if !defined(DOUBLE)
	ldr	w10, [X]
	ldr	w11, [Y]
	str	w10, [Y]
	str	w11, [X]
#else
	ldr	x10, [X]
	ldr	x11, [Y]
	str	x10, [Y]
	str	x11, [X]
#endif
#else
#if !defined(DOUBLE)
	ldr	x10, [X]
	ldr	x11, [Y]
	str	x10, [Y]
	str	x11, [X]
#else
	ldr	x10, [X]
	ldr	x11, [Y]
	str	x10, [Y]
	str	x11, [X]

	ldr	x12, [X, #8]
	ldr	x13, [Y, #8]
	str	x12, [Y, #8]
	str	x13, [X, #8]
#endif
#endif
	add	Y, Y, INC_Y
	add	X, X, INC_X
.endm

/*******************************************************************************
* End of macro definitions
*******************************************************************************/

	PROLOGUE

	cmp	N, xzr
	ble	.Lswap_kernel_L999

	cmp	INC_X, #1
	bne	.Lswap_kernel_S_BEGIN
	cmp	INC_Y, #1
	bne	.Lswap_kernel_S_BEGIN

.Lswap_kernel_F_BEGIN:

	asr	I, N, #3
	cmp	I, xzr
	beq	.Lswap_kernel_F1

.Lswap_kernel_F8:

	KERNEL_F8

	subs	I, I, #1
	bne	.Lswap_kernel_F8

.Lswap_kernel_F1:

	ands	I, N, #7
	ble	.Lswap_kernel_L999

.Lswap_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
	bne     .Lswap_kernel_F10

	b	.Lswap_kernel_L999


.Lswap_kernel_S_BEGIN:

	INIT_S

	asr	I, N, #2
	cmp	I, xzr
	ble	.Lswap_kernel_S1

.Lswap_kernel_S4:

	KERNEL_S1
	KERNEL_S1
	KERNEL_S1
	KERNEL_S1

	subs	I, I, #1
	bne	.Lswap_kernel_S4

.Lswap_kernel_S1:

	ands	I, N, #3
	ble	.Lswap_kernel_L999

.Lswap_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     .Lswap_kernel_S10

.Lswap_kernel_L999:

	mov	w0, wzr
	ret

	EPILOGUE
