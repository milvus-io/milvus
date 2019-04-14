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

#define REG0	xzr
#define SUMF	d0
#define TMPF	d1
#define TMPVF	{v1.d}[0]
#define SZ	8

/******************************************************************************/

.macro KERNEL_F1
	ld1	{v1.2d}, [X], #16
	fabs	v1.2d, v1.2d
	faddp	TMPF, v1.2d
	fadd	SUMF, SUMF, TMPF
.endm

.macro KERNEL_F4
	ld1	{v1.2d, v2.2d, v3.2d, v4.2d}, [X], #64
	fabs	v1.2d, v1.2d
	fabs	v2.2d, v2.2d
	fabs	v3.2d, v3.2d
	fabs	v4.2d, v4.2d

	fadd	v1.2d, v1.2d, v2.2d
	fadd	v3.2d, v3.2d, v4.2d

	fadd	v0.2d, v0.2d, v1.2d
	fadd	v0.2d, v0.2d, v3.2d

	PRFM	PLDL1KEEP, [X, #1024]
.endm

.macro KERNEL_F4_FINALIZE
	faddp	SUMF, v0.2d
.endm

.macro INIT_S
	lsl	INC_X, INC_X, #4
.endm

.macro KERNEL_S1
	ld1	{v1.2d}, [X], INC_X
	fabs	v1.2d, v1.2d
	faddp	TMPF, v1.2d
	fadd	SUMF, SUMF, TMPF
.endm

/*******************************************************************************
* End of macro definitions
*******************************************************************************/

	PROLOGUE

	fmov	SUMF, REG0

	cmp	N, xzr
	ble	.Lzasum_kernel_L999
	cmp	INC_X, xzr
	ble	.Lzasum_kernel_L999

	cmp	INC_X, #1
	bne	.Lzasum_kernel_S_BEGIN

.Lzasum_kernel_F_BEGIN:

	asr	I, N, #2
	cmp	I, xzr
	beq	.Lzasum_kernel_F1

.Lzasum_kernel_F4:

	KERNEL_F4

	subs	I, I, #1
	bne	.Lzasum_kernel_F4

	KERNEL_F4_FINALIZE

.Lzasum_kernel_F1:

	ands	I, N, #3
	ble	.Lzasum_kernel_L999

.Lzasum_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
        bne     .Lzasum_kernel_F10

.Lzasum_kernel_L999:
	ret

.Lzasum_kernel_S_BEGIN:

	INIT_S

	asr	I, N, #2
	cmp	I, xzr
	ble	.Lzasum_kernel_S1

.Lzasum_kernel_S4:

	KERNEL_S1
	KERNEL_S1
	KERNEL_S1
	KERNEL_S1

	subs	I, I, #1
	bne	.Lzasum_kernel_S4

.Lzasum_kernel_S1:

	ands	I, N, #3
	ble	.Lzasum_kernel_L999

.Lzasum_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     .Lzasum_kernel_S10

	ret

	EPILOGUE
