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

#if defined(USE_MIN)
#define COND	le
#else
#define COND	ge
#endif

#if !defined(DOUBLE)
#define REG0	wzr
#define MAXF	s0
#define TMPF	s1
#define TMPVF	{v1.s}[0]
#define SZ	4
#else
#define REG0	xzr
#define MAXF	d0
#define TMPF	d1
#define TMPVF	{v1.d}[0]
#define SZ	8
#endif

/******************************************************************************/

.macro INIT_F1
	ldr	MAXF, [X], #SZ
#if defined(USE_ABS)
	fabs	MAXF, MAXF
#endif
.endm

.macro KERNEL_F1
	ldr	TMPF, [X], #SZ
#if defined(USE_ABS)
	fabs	TMPF, TMPF
#endif
	fcmp	MAXF, TMPF
	fcsel	MAXF, MAXF, TMPF, COND
.endm

.macro INIT_F4
#if !defined(DOUBLE)
	ld1	{v0.4s}, [X], #16
#if defined(USE_ABS)
	fabs	v0.4s, v0.4s
#endif
#if defined(USE_MIN)
	fminv	MAXF, v0.4s
#else
	fmaxv	MAXF, v0.4s
#endif
#else // DOUBLE
	ld2	{v0.2d,v1.2d}, [X], #32
#if defined(USE_ABS)
	fabs	v0.2d, v0.2d
	fabs	v1.2d, v1.2d
#endif
#if defined(USE_MIN)
	fmin	v0.2d, v0.2d, v1.2d
	fminp	MAXF, v0.2d
#else
	fmax	v0.2d, v0.2d, v1.2d
	fmaxp	MAXF, v0.2d
#endif
#endif
.endm

.macro KERNEL_F4
#if !defined(DOUBLE)
	ld1	{v1.4s}, [X], #16
#if defined(USE_ABS)
	fabs	v1.4s, v1.4s
#endif
#if defined(USE_MIN)
	fminv	TMPF, v1.4s
#else
	fmaxv	TMPF, v1.4s
#endif
#else // DOUBLE
	ld2	{v1.2d,v2.2d}, [X], #32
#if defined(USE_ABS)
	fabs	v1.2d, v1.2d
	fabs	v2.2d, v2.2d
#endif
#if defined(USE_MIN)
	fmin	v1.2d, v1.2d, v2.2d
	fminp	TMPF, v1.2d
#else
	fmax	v1.2d, v1.2d, v2.2d
	fmaxp	TMPF, v1.2d
#endif
#endif
	fcmp	MAXF, TMPF
	fcsel	MAXF, MAXF, TMPF, COND
.endm

.macro INIT_S
#if !defined(DOUBLE)
	lsl	INC_X, INC_X, #2
	ld1	{v0.s}[0], [X], INC_X
#else
	lsl	INC_X, INC_X, #3
	ld1	{v0.d}[0], [X], INC_X
#endif
#if defined(USE_ABS)
	fabs	MAXF, MAXF
#endif
.endm

.macro KERNEL_S1
	ld1	TMPVF, [X], INC_X
#if defined(USE_ABS)
	fabs	TMPF, TMPF
#endif
	fcmp	MAXF, TMPF
	fcsel	MAXF, MAXF, TMPF, COND
.endm

/*******************************************************************************
* End of macro definitions
*******************************************************************************/

	PROLOGUE

	cmp	N, xzr
	ble	.Lamax_kernel_zero
	cmp	INC_X, xzr
	ble	.Lamax_kernel_zero

	cmp	INC_X, #1
	bne	.Lamax_kernel_S_BEGIN

.Lamax_kernel_F_BEGIN:

	asr	I, N, #2
	cmp	I, xzr
	beq	.Lamax_kernel_F1_INIT

	INIT_F4
	subs	I, I, #1
	beq	.Lamax_kernel_F1

.Lamax_kernel_F4:

	KERNEL_F4

	subs	I, I, #1
	bne	.Lamax_kernel_F4

.Lamax_kernel_F1:

	ands	I, N, #3
	ble	.Lamax_kernel_L999

.Lamax_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
        bne     .Lamax_kernel_F10

	ret

.Lamax_kernel_F1_INIT:

	INIT_F1
	subs	N, N, #1
	b	.Lamax_kernel_F1

.Lamax_kernel_S_BEGIN:

	INIT_S

	subs	N, N, #1
	ble	.Lamax_kernel_L999

	asr	I, N, #2
	cmp	I, xzr
	ble	.Lamax_kernel_S1

.Lamax_kernel_S4:

	KERNEL_S1
	KERNEL_S1
	KERNEL_S1
	KERNEL_S1

	subs	I, I, #1
	bne	.Lamax_kernel_S4

.Lamax_kernel_S1:

	ands	I, N, #3
	ble	.Lamax_kernel_L999

.Lamax_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     .Lamax_kernel_S10

.Lamax_kernel_L999:

	ret

.Lamax_kernel_zero:

	fmov	MAXF, REG0
	ret

	EPILOGUE
