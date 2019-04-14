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

#define REG0	wzr
#define SUMF	s0
#define TMPF	s1
#define TMPVF	{v1.s}[0]
#define SZ	4

/******************************************************************************/

.macro KERNEL_F1
	ld1	{v1.2s}, [X], #8
	fabs	v1.2s, v1.2s
	ext	v2.8b, v1.8b, v1.8b, #4
	fadd	TMPF, TMPF, s2
	fadd	SUMF, SUMF, TMPF
.endm

.macro KERNEL_F8
	ld1	{v1.4s, v2.4s, v3.4s, v4.4s}, [X]
	add	X, X, #64
	fabs	v1.4s, v1.4s
	fabs	v2.4s, v2.4s
	fabs	v3.4s, v3.4s
	fabs	v4.4s, v4.4s

	PRFM	PLDL1KEEP, [X, #1024]

	fadd	v1.4s, v1.4s, v2.4s
	fadd	v3.4s, v3.4s, v4.4s
	fadd	v0.4s, v0.4s, v1.4s
	fadd	v0.4s, v0.4s, v3.4s
.endm

.macro KERNEL_F8_FINALIZE
	ext	v1.16b, v0.16b, v0.16b, #8
	fadd	v0.2s, v0.2s, v1.2s
	faddp	SUMF, v0.2s
.endm

.macro INIT_S
	lsl	INC_X, INC_X, #3
.endm

.macro KERNEL_S1
	ld1	{v1.2s}, [X], INC_X
	fabs	v1.2s, v1.2s
	ext	v2.8b, v1.8b, v1.8b, #4
	fadd	TMPF, TMPF, s2
	fadd	SUMF, SUMF, TMPF

.endm

/*******************************************************************************
* End of macro definitions
*******************************************************************************/

	PROLOGUE

	fmov	SUMF, REG0
	fmov	s1, SUMF

	cmp	N, xzr
	ble	.Lcasum_kernel_L999
	cmp	INC_X, xzr
	ble	.Lcasum_kernel_L999

	cmp	INC_X, #1
	bne	.Lcasum_kernel_S_BEGIN

.Lcasum_kernel_F_BEGIN:

	asr	I, N, #3
	cmp	I, xzr
	beq	.Lcasum_kernel_F1

.Lcasum_kernel_F8:

	KERNEL_F8

	subs	I, I, #1
	bne	.Lcasum_kernel_F8

	KERNEL_F8_FINALIZE

.Lcasum_kernel_F1:

	ands	I, N, #7
	ble	.Lcasum_kernel_L999

.Lcasum_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
        bne     .Lcasum_kernel_F10

.Lcasum_kernel_L999:
	ret

.Lcasum_kernel_S_BEGIN:

	INIT_S

	asr	I, N, #2
	cmp	I, xzr
	ble	.Lcasum_kernel_S1

.Lcasum_kernel_S4:

	KERNEL_S1
	KERNEL_S1
	KERNEL_S1
	KERNEL_S1

	subs	I, I, #1
	bne	.Lcasum_kernel_S4

.Lcasum_kernel_S1:

	ands	I, N, #3
	ble	.Lcasum_kernel_L999

.Lcasum_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     .Lcasum_kernel_S10

	ret

	EPILOGUE
