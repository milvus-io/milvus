/*******************************************************************************
Copyright (c) 2017, The OpenBLAS Project
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

#if !defined(COMPLEX)
#if !defined(DOUBLE)
#define TMPF0		s0
#define TMPF1		s1
#define INC_SHIFT	2
#define N_DIV_SHIFT	2
#define N_REM_MASK	3
#else
#define TMPF0		d0
#define TMPF1		d1
#define INC_SHIFT	3
#define N_DIV_SHIFT	1
#define N_REM_MASK	1
#endif
#else
#if !defined(DOUBLE)
#define TMPF0		d0
#define TMPF1		d1
#define INC_SHIFT	3
#define N_DIV_SHIFT	1
#define N_REM_MASK	1
#else
#define TMPF0		q0
#define TMPF1		q1
#define INC_SHIFT	4
#define N_DIV_SHIFT	0
#define N_REM_MASK	0
#endif
#endif

.macro KERNEL_F1
	ldr	TMPF0, [X]
	ldr	TMPF1, [Y]
	str	TMPF0, [Y]
	str	TMPF1, [X]
	add	X, X, INC_X
	add	Y, Y, INC_Y
.endm

.macro KERNEL_F
	ldr	q0, [X]
	ldr	q1, [Y]
	add	X, X, #16
	add	Y, Y, #16

	prfm	PLDL1STRM, [X, #1024]
	prfm	PLDL1STRM, [Y, #1024]

	str	q0, [Y, #-16]
	str	q1, [X, #-16]
.endm

.macro INIT
	lsl	INC_X, INC_X, #INC_SHIFT
	lsl	INC_Y, INC_Y, #INC_SHIFT
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
	INIT

	asr	I, N, #N_DIV_SHIFT
	cmp	I, xzr
	beq	.Lswap_kernel_F1

	.align 5
.Lswap_kernel_F:

	KERNEL_F

	subs	I, I, #1
	bne	.Lswap_kernel_F

.Lswap_kernel_F1:

#if defined(DOUBLE) && defined(COMPLEX)
	b	.Lswap_kernel_L999
#else
	ands	I, N, #N_REM_MASK
	ble	.Lswap_kernel_L999
#endif

.Lswap_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
	bne     .Lswap_kernel_F10

	b	.Lswap_kernel_L999


.Lswap_kernel_S_BEGIN:

	INIT

	asr	I, N, #2
	cmp	I, xzr
	ble	.Lswap_kernel_S1

.Lswap_kernel_S4:

	KERNEL_F1
	KERNEL_F1
	KERNEL_F1
	KERNEL_F1

	subs	I, I, #1
	bne	.Lswap_kernel_S4

.Lswap_kernel_S1:

	ands	I, N, #3
	ble	.Lswap_kernel_L999

.Lswap_kernel_S10:

	KERNEL_F1

	subs	I, I, #1
	bne	.Lswap_kernel_S10

.Lswap_kernel_L999:

	mov	w0, wzr
	ret

	EPILOGUE
