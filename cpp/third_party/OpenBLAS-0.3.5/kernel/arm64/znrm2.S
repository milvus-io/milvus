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

#define	N		x0
#define	X		x1
#define	INC_X		x2

#define I		x3

#if !defined(DOUBLE)
#define SSQ		s0
#define SCALE		s1
#define REGZERO		s6
#define REGONE		s7
#else
#define SSQ		d0
#define SCALE		d1
#define REGZERO		d6
#define REGONE		d7
#endif

/**************************************************************************************
* Macro definitions
**************************************************************************************/

.macro KERNEL_F1
#if !defined(DOUBLE)
	ldr	s4, [X], #4
	fcmp	s4, REGZERO
	beq	KERNEL_F1_NEXT_\@
	fabs	s4, s4
	fcmp	SCALE, s4
	bge	KERNEL_F1_SCALE_GE_XR_\@
	fdiv	s2, SCALE, s4
	fmul	s2, s2, s2
	fmul	s3, SSQ, s2
	fadd	SSQ, REGONE, s3
	fmov	SCALE, s4
	b	KERNEL_F1_NEXT_\@
KERNEL_F1_SCALE_GE_XR_\@:
	fdiv	s2, s4, SCALE
	fmla	SSQ, s2, v2.s[0]
KERNEL_F1_NEXT_\@:
	ldr	s5, [X], #4
	fcmp	s5, REGZERO
	beq	KERNEL_F1_END_\@
	fabs	s5, s5
	fcmp	SCALE, s5
	bge	KERNEL_F1_SCALE_GE_XI_\@
	fdiv	s2, SCALE, s5
	fmul	s2, s2, s2
	fmul	s3, SSQ, s2
	fadd	SSQ, REGONE, s3
	fmov	SCALE, s5
	b	KERNEL_F1_END_\@
KERNEL_F1_SCALE_GE_XI_\@:
	fdiv	s2, s5, SCALE
	fmla	SSQ, s2, v2.s[0]
#else
	ldr	d4, [X], #8
	fcmp	d4, REGZERO
	beq	KERNEL_F1_NEXT_\@
	fabs	d4, d4
	fcmp	SCALE, d4
	bge	KERNEL_F1_SCALE_GE_XR_\@
	fdiv	d2, SCALE, d4
	fmul	d2, d2, d2
	fmul	d3, SSQ, d2
	fadd	SSQ, REGONE, d3
	fmov	SCALE, d4
	b	KERNEL_F1_NEXT_\@
KERNEL_F1_SCALE_GE_XR_\@:
	fdiv	d2, d4, SCALE
	fmla	SSQ, d2, v2.d[0]
KERNEL_F1_NEXT_\@:
	ldr	d5, [X], #8
	fcmp	d5, REGZERO
	beq	KERNEL_F1_END_\@
	fabs	d5, d5
	fcmp	SCALE, d5
	bge	KERNEL_F1_SCALE_GE_XI_\@
	fdiv	d2, SCALE, d5
	fmul	d2, d2, d2
	fmul	d3, SSQ, d2
	fadd	SSQ, REGONE, d3
	fmov	SCALE, d5
	b	KERNEL_F1_END_\@
KERNEL_F1_SCALE_GE_XI_\@:
	fdiv	d2, d5, SCALE
	fmla	SSQ, d2, v2.d[0]
#endif
KERNEL_F1_END_\@:
.endm

.macro KERNEL_S1
#if !defined(DOUBLE)
	ldr	s4, [X]
	fcmp	s4, REGZERO
	beq	KERNEL_S1_NEXT_\@
	fabs	s4, s4
	fcmp	SCALE, s4
	bge	KERNEL_S1_SCALE_GE_XR_\@
	fdiv	s2, SCALE, s4
	fmul	s2, s2, s2
	fmul	s3, SSQ, s2
	fadd	SSQ, REGONE, s3
	fmov	SCALE, s4
	b	KERNEL_S1_NEXT_\@
KERNEL_S1_SCALE_GE_XR_\@:
	fdiv	s2, s4, SCALE
	fmla	SSQ, s2, v2.s[0]
KERNEL_S1_NEXT_\@:
	ldr	s5, [X, #4]
	fcmp	s5, REGZERO
	beq	KERNEL_S1_END_\@
	fabs	s5, s5
	fcmp	SCALE, s5
	bge	KERNEL_S1_SCALE_GE_XI_\@
	fdiv	s2, SCALE, s5
	fmul	s2, s2, s2
	fmul	s3, SSQ, s2
	fadd	SSQ, REGONE, s3
	fmov	SCALE, s5
	b	KERNEL_S1_END_\@
KERNEL_S1_SCALE_GE_XI_\@:
	fdiv	s2, s5, SCALE
	fmla	SSQ, s2, v2.s[0]
#else
	ldr	d4, [X]
	fcmp	d4, REGZERO
	beq	KERNEL_S1_NEXT_\@
	fabs	d4, d4
	fcmp	SCALE, d4
	bge	KERNEL_S1_SCALE_GE_XR_\@
	fdiv	d2, SCALE, d4
	fmul	d2, d2, d2
	fmul	d3, SSQ, d2
	fadd	SSQ, REGONE, d3
	fmov	SCALE, d4
	b	KERNEL_S1_NEXT_\@
KERNEL_S1_SCALE_GE_XR_\@:
	fdiv	d2, d4, SCALE
	fmla	SSQ, d2, v2.d[0]
KERNEL_S1_NEXT_\@:
	ldr	d5, [X, #8]
	fcmp	d5, REGZERO
	beq	KERNEL_S1_END_\@
	fabs	d5, d5
	fcmp	SCALE, d5
	bge	KERNEL_S1_SCALE_GE_XI_\@
	fdiv	d2, SCALE, d5
	fmul	d2, d2, d2
	fmul	d3, SSQ, d2
	fadd	SSQ, REGONE, d3
	fmov	SCALE, d5
	b	KERNEL_S1_END_\@
KERNEL_S1_SCALE_GE_XI_\@:
	fdiv	d2, d5, SCALE
	fmla	SSQ, d2, v2.d[0]
#endif
KERNEL_S1_END_\@:
	add	X, X, INC_X
.endm

.macro KERNEL_F8
	KERNEL_F1
	KERNEL_F1
	KERNEL_F1
	KERNEL_F1
	KERNEL_F1
	KERNEL_F1
	KERNEL_F1
	KERNEL_F1
.endm

.macro INIT_S
#if !defined(DOUBLE)
	lsl	INC_X, INC_X, #3		// INC_X * SIZE
#else
	lsl	INC_X, INC_X, #4		// INC_X * SIZE
#endif
.endm

.macro INIT
	eor	v1.16b, v1.16b, v1.16b		// scale=0.0
	fmov	SSQ, #1.0
	fmov	REGONE, SSQ
	fmov	REGZERO, SCALE
.endm

/**************************************************************************************
* End of macro definitions
**************************************************************************************/

	PROLOGUE

	.align 5

	INIT

	cmp	N, #0
	ble	.Lznrm2_kernel_L999

	cmp	INC_X, #0
	beq	.Lznrm2_kernel_L999

	cmp	INC_X, #1
	bne	.Lznrm2_kernel_S_BEGIN

.Lznrm2_kernel_F_BEGIN:

	asr	I, N, #3				// I = N / 8
	cmp	I, xzr
	ble	.Lznrm2_kernel_F1

.Lznrm2_kernel_F8:

	KERNEL_F8

	subs    I, I, #1
        bne     .Lznrm2_kernel_F8

.Lznrm2_kernel_F1:

	ands	I, N, #7
	ble	.Lznrm2_kernel_L999


.Lznrm2_kernel_F10:

	KERNEL_F1

	subs	I, I, #1
	bne	.Lznrm2_kernel_F10

	b	.Lznrm2_kernel_L999

.Lznrm2_kernel_S_BEGIN:

	INIT_S

	mov	I, N

	.align 5

.Lznrm2_kernel_S10:

	KERNEL_S1

	subs	I, I, #1
	bne	.Lznrm2_kernel_S10


.Lznrm2_kernel_L999:
	fsqrt	SSQ, SSQ
	fmul	SSQ, SCALE, SSQ

	ret

	EPILOGUE

