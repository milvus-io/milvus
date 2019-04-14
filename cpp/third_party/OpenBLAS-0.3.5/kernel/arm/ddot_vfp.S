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
* 2016/01/23 Saar
*        Bugfix for Refs #750 and #740
**************************************************************************************/

#define ASSEMBLER
#include "common.h"

#define STACKSIZE 256

#define	N	r0
#define	X	r1
#define	INC_X	r2
#define	OLD_Y	r3


/******************************************************
* [fp, #-128] - [fp, #-64] is reserved
* for store and restore of floating point
* registers
*******************************************************/

#define OLD_INC_Y	[fp, #4 ]

#define I	r5
#define Y	r6
#define INC_Y	r7

#define X_PRE	512

/**************************************************************************************
* Macro definitions
**************************************************************************************/

.macro KERNEL_F4

	pld	[ X, #X_PRE  ]
	vldmia.f64	X!, { d8 }
	pld	[ Y, #X_PRE  ]
	vldmia.f64	Y!, { d4 }
	vldmia.f64	Y!, { d5 }
	fmacd   d0  , d4,  d8
	vldmia.f64	X!, { d9 }
	vldmia.f64	Y!, { d6 }
	fmacd   d1  , d5,  d9
	vldmia.f64	X!, { d10 }
	vldmia.f64	X!, { d11 }
	fmacd   d0  , d6,  d10
	vldmia.f64	Y!, { d7 }
	fmacd   d1  , d7,  d11

.endm

.macro KERNEL_F1

	vldmia.f64	X!, { d4 }
	vldmia.f64	Y!, { d8 }
	fmacd   d0  , d4,  d8

.endm


/*************************************************************************************************************************/

.macro KERNEL_S4

	nop
	vldmia.f64	X, { d4 }
	vldmia.f64	Y, { d8 }
	add	X, X, INC_X
	add	Y, Y, INC_Y
	fmacd   d0  , d4,  d8

	vldmia.f64	X, { d5 }
	vldmia.f64	Y, { d9 }
	add	X, X, INC_X
	add	Y, Y, INC_Y
	fmacd   d1  , d5,  d9

	vldmia.f64	X, { d6 }
	vldmia.f64	Y, { d10 }
	add	X, X, INC_X
	add	Y, Y, INC_Y
	fmacd   d0  , d6,  d10

	vldmia.f64	X, { d7 }
	vldmia.f64	Y, { d11 }
	add	X, X, INC_X
	add	Y, Y, INC_Y
	fmacd   d1  , d7,  d11

.endm


.macro KERNEL_S1

	vldmia.f64	X, { d4 }
	vldmia.f64	Y, { d8 }
	add	X, X, INC_X
	fmacd   d0  , d4,  d8
	add	Y, Y, INC_Y

.endm



/**************************************************************************************
* End of macro definitions
**************************************************************************************/

	PROLOGUE

	.align 5

	push	{r4 - r9, fp}
	add	fp, sp, #24
	sub	sp, sp, #STACKSIZE				// reserve stack

	sub	r4, fp, #128
	vstm	r4, { d8 - d15} 				// store floating point registers

	mov	Y, OLD_Y
	ldr	INC_Y, OLD_INC_Y

        movs    r4, #0                                          // clear floating point register
        vmov    s0, r4
        vmov    s1, r4
        vcvt.f64.f32    d0, s0
        vcvt.f64.f32    d1, s1


	cmp	N, #0
	ble	ddot_kernel_L999

#	cmp	INC_X, #0
#	beq	ddot_kernel_L999

#	cmp	INC_Y, #0
#	beq	ddot_kernel_L999

	cmp	INC_X, #1
	bne	ddot_kernel_S_BEGIN

	cmp	INC_Y, #1
	bne	ddot_kernel_S_BEGIN

ddot_kernel_F_BEGIN:

	asrs	I, N, #2					// I = N / 4
	ble	ddot_kernel_F1

ddot_kernel_F4:

	KERNEL_F4

	subs	I, I, #1
	ble	ddot_kernel_F1


	KERNEL_F4

	subs	I, I, #1
	bne	ddot_kernel_F4

ddot_kernel_F1:

	ands	I, N, #3
	ble	ddot_kernel_L999

ddot_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
        bne     ddot_kernel_F10

	b	ddot_kernel_L999

ddot_kernel_S_BEGIN:

	lsl	INC_X, INC_X, #3				// INC_X * SIZE
	lsl	INC_Y, INC_Y, #3				// INC_Y * SIZE

	asrs	I, N, #2					// I = N / 4
	ble	ddot_kernel_S1

ddot_kernel_S4:

	KERNEL_S4

	subs	I, I, #1
	bne	ddot_kernel_S4

ddot_kernel_S1:

	ands	I, N, #3
	ble	ddot_kernel_L999

ddot_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     ddot_kernel_S10






ddot_kernel_L999:

	sub	r3, fp, #128
	vldm	r3, { d8 - d15}					// restore floating point registers

	vadd.f64	d0 , d0, d1				// set return value
#if !defined(__ARM_PCS_VFP)
	vmov	r0, r1, d0
#endif
	sub	sp, fp, #24
	pop	{r4 - r9, fp}
	bx	lr

	EPILOGUE

