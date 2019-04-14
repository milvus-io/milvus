/***************************************************************************
Copyright (c) 2013, The OpenBLAS Project
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
*****************************************************************************/

/**************************************************************************************
* 2013/11/11 Saar
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*
**************************************************************************************/

#define ASSEMBLER
#include "common.h"

#define STACKSIZE 256

#define	N	r0
#define	X	r1
#define	INC_X	r2

/******************************************************
* [fp, #-128] - [fp, #-64] is reserved
* for store and restore of floating point
* registers
*******************************************************/

#if !defined(__ARM_PCS_VFP)
#define	OLD_RETURN_ADDR	r0
#define	OLD_N		r1
#define	OLD_X		r2
#define	OLD_INC_X	r3
#define	OLD_Y		[fp, #0 ]
#define	OLD_INC_Y	[fp, #4 ]
#define	RETURN_ADDR	r8
#else
#define	OLD_Y		r3
#define	OLD_INC_Y	[fp, #0 ]
#endif

#define I	r5
#define Y	r6
#define INC_Y	r7

#define X_PRE	512

/**************************************************************************************
* Macro definitions
**************************************************************************************/

.macro KERNEL_F4

	pld	[ X, #X_PRE  ]
	pld	[ Y, #X_PRE  ]

	vldmia.f64	X!, { d4 - d5 }
	vldmia.f64	Y!, { d8 - d9 }
	fmacd   d0  , d4,  d8
	fmacd   d1  , d4,  d9
	vldmia.f64	X!, { d6 - d7 }
	fmacd   d2  , d5,  d9
	fmacd   d3  , d5,  d8

	vldmia.f64	Y!, { d10 - d11 }
	fmacd   d0  , d6,  d10
	fmacd   d1  , d6,  d11
	pld	[ X, #X_PRE  ]
	fmacd   d2  , d7,  d11
	fmacd   d3  , d7,  d10

	pld	[ Y, #X_PRE  ]

	vldmia.f64	X!, { d4 - d5 }
	vldmia.f64	Y!, { d8 - d9 }
	fmacd   d0  , d4,  d8
	fmacd   d1  , d4,  d9
	vldmia.f64	X!, { d6 - d7 }
	fmacd   d2  , d5,  d9
	fmacd   d3  , d5,  d8

	vldmia.f64	Y!, { d10 - d11 }
	fmacd   d0  , d6,  d10
	fmacd   d1  , d6,  d11
	fmacd   d2  , d7,  d11
	fmacd   d3  , d7,  d10

.endm

.macro KERNEL_F1

	vldmia.f64	X!, { d4 - d5 }
	vldmia.f64	Y!, { d8 - d9 }
	fmacd   d0  , d4,  d8
	fmacd   d1  , d4,  d9
	fmacd   d2  , d5,  d9
	fmacd   d3  , d5,  d8

.endm


/*************************************************************************************************************************/

.macro KERNEL_S4

	nop

	vldmia.f64	X, { d4 - d5 }
	vldmia.f64	Y, { d8 - d9 }
	fmacd   d0  , d4,  d8
	fmacd   d1  , d4,  d9
	fmacd   d2  , d5,  d9
	fmacd   d3  , d5,  d8
	add	X, X, INC_X
	add	Y, Y, INC_Y

	vldmia.f64	X, { d4 - d5 }
	vldmia.f64	Y, { d8 - d9 }
	fmacd   d0  , d4,  d8
	fmacd   d1  , d4,  d9
	fmacd   d2  , d5,  d9
	fmacd   d3  , d5,  d8
	add	X, X, INC_X
	add	Y, Y, INC_Y

	vldmia.f64	X, { d4 - d5 }
	vldmia.f64	Y, { d8 - d9 }
	fmacd   d0  , d4,  d8
	fmacd   d1  , d4,  d9
	fmacd   d2  , d5,  d9
	fmacd   d3  , d5,  d8
	add	X, X, INC_X
	add	Y, Y, INC_Y

	vldmia.f64	X, { d4 - d5 }
	vldmia.f64	Y, { d8 - d9 }
	fmacd   d0  , d4,  d8
	fmacd   d1  , d4,  d9
	fmacd   d2  , d5,  d9
	fmacd   d3  , d5,  d8
	add	X, X, INC_X
	add	Y, Y, INC_Y

.endm


.macro KERNEL_S1

	vldmia.f64	X, { d4 - d5 }
	vldmia.f64	Y, { d8 - d9 }
	fmacd   d0  , d4,  d8
	fmacd   d1  , d4,  d9
	fmacd   d2  , d5,  d9
	fmacd   d3  , d5,  d8
	add	X, X, INC_X
	add	Y, Y, INC_Y

.endm



/**************************************************************************************
* End of macro definitions
**************************************************************************************/

	PROLOGUE

	.align 5

	push	{r4 - r9, fp}
	add	fp, sp, #28
	sub	sp, sp, #STACKSIZE				// reserve stack

	sub	r4, fp, #128
	vstm	r4, { d8 - d15} 				// store floating point registers

        movs    r4, #0                                          // clear floating point register
        vmov    s0, r4
        vcvt.f64.f32    d0, s0
        vcvt.f64.f32    d1, s0
        vcvt.f64.f32    d2, s0
        vcvt.f64.f32    d3, s0

#if !defined(__ARM_PCS_VFP)
	mov	RETURN_ADDR, OLD_RETURN_ADDR
	mov	N, OLD_N
	mov	X, OLD_X
	mov	INC_X, OLD_INC_X
	ldr	Y, OLD_Y
	ldr	INC_Y, OLD_INC_Y
#else
	mov	Y, OLD_Y
	ldr	INC_Y, OLD_INC_Y
#endif

	cmp	N, #0
	ble	zdot_kernel_L999

#	cmp	INC_X, #0
#	beq	zdot_kernel_L999

#	cmp	INC_Y, #0
#	beq	zdot_kernel_L999

	cmp	INC_X, #1
	bne	zdot_kernel_S_BEGIN

	cmp	INC_Y, #1
	bne	zdot_kernel_S_BEGIN

zdot_kernel_F_BEGIN:

	asrs	I, N, #2					// I = N / 4
	ble	zdot_kernel_F1

zdot_kernel_F4:

	KERNEL_F4

	subs	I, I, #1
	bne	zdot_kernel_F4

zdot_kernel_F1:

	ands	I, N, #3
	ble	zdot_kernel_L999

zdot_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
        bne     zdot_kernel_F10

	b	zdot_kernel_L999

zdot_kernel_S_BEGIN:

	lsl	INC_X, INC_X, #4				// INC_X * SIZE * 2
	lsl	INC_Y, INC_Y, #4				// INC_Y * SIZE * 2

	asrs	I, N, #2					// I = N / 4
	ble	zdot_kernel_S1

zdot_kernel_S4:

	KERNEL_S4

	subs	I, I, #1
	bne	zdot_kernel_S4

zdot_kernel_S1:

	ands	I, N, #3
	ble	zdot_kernel_L999

zdot_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     zdot_kernel_S10



zdot_kernel_L999:

	sub	r3, fp, #128
	vldm	r3, { d8 - d15}					// restore floating point registers

#if !defined(CONJ)
	vsub.f64	d0 , d0, d2
	vadd.f64	d1 , d1, d3
#else
	vadd.f64	d0 , d0, d2
	vsub.f64	d1 , d1, d3
#endif
#if !defined(__ARM_PCS_VFP)
	vstm	RETURN_ADDR, {d0 - d1}
#endif

	sub	sp, fp, #28
	pop	{r4 - r9, fp}
	bx	lr

	EPILOGUE

