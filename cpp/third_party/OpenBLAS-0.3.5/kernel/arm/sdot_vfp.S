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
* 	 CTEST			: OK (no test for dsdot)
* 	 TEST			: OK (no test for dsdot)
*
* 2016/01/23 Saar
*	 Bugfix	for Refs #750 and #740
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

#if defined(DSDOT)

.macro KERNEL_F4

	vldmia.f32	X!, { s14 }
	vldmia.f32	Y!, { s15 }
	vmul.f32   s15, s14, s15
	vcvt.f64.f32	d4, s15
	vadd.f64   d0  , d0,  d4

	vldmia.f32	X!, { s14 }
	vldmia.f32	Y!, { s15 }
	vmul.f32   s15, s14, s15
	vcvt.f64.f32	d4, s15
	vadd.f64   d0  , d0,  d4

	vldmia.f32	X!, { s14 }
	vldmia.f32	Y!, { s15 }
	vmul.f32   s15, s14, s15
	vcvt.f64.f32	d4, s15
	vadd.f64   d0  , d0,  d4

	vldmia.f32	X!, { s14 }
	vldmia.f32	Y!, { s15 }
	vmul.f32   s15, s14, s15
	vcvt.f64.f32	d4, s15
	vadd.f64   d0  , d0,  d4

.endm

.macro KERNEL_F1

	vldmia.f32	X!, { s14 }
	vldmia.f32	Y!, { s15 }
	vmul.f32   s15, s14, s15
	vcvt.f64.f32	d4, s15
	vadd.f64   d0  , d0,  d4

.endm


.macro KERNEL_S4

	nop

	vldmia.f32	X, { s14 }
	vldmia.f32	Y, { s15 }
	vmul.f32   s15, s14, s15
	vcvt.f64.f32	d4, s15
	vadd.f64   d0  , d0,  d4
	add	X, X, INC_X
	add	Y, Y, INC_Y

	vldmia.f32	X, { s14 }
	vldmia.f32	Y, { s15 }
	vmul.f32   s15, s14, s15
	vcvt.f64.f32	d4, s15
	vadd.f64   d0  , d0,  d4
	add	X, X, INC_X
	add	Y, Y, INC_Y

	vldmia.f32	X, { s14 }
	vldmia.f32	Y, { s15 }
	vmul.f32   s15, s14, s15
	vcvt.f64.f32	d4, s15
	vadd.f64   d0  , d0,  d4
	add	X, X, INC_X
	add	Y, Y, INC_Y

	vldmia.f32	X, { s14 }
	vldmia.f32	Y, { s15 }
	vmul.f32   s15, s14, s15
	vcvt.f64.f32	d4, s15
	vadd.f64   d0  , d0,  d4
	add	X, X, INC_X
	add	Y, Y, INC_Y

.endm


.macro KERNEL_S1

	vldmia.f32	X, { s14 }
	vldmia.f32	Y, { s15 }
	vmul.f32   s15, s14, s15
	vcvt.f64.f32	d4, s15
	vadd.f64   d0  , d0,  d4
	add	X, X, INC_X
	add	Y, Y, INC_Y

.endm



#else

.macro KERNEL_F4

	vldmia.f32	X!, { s8 - s9 }
	vldmia.f32	Y!, { s4 - s5}
	fmacs   s0  , s4,  s8
	vldmia.f32	X!, { s10 - s11 }
	fmacs   s1  , s5,  s9
	vldmia.f32	Y!, { s6 - s7 }
	fmacs   s0  , s6,  s10
	fmacs   s1  , s7,  s11

.endm

.macro KERNEL_F1

	vldmia.f32	X!, { s4 }
	vldmia.f32	Y!, { s8 }
	fmacs   s0  , s4,  s8

.endm


.macro KERNEL_S4

	nop
	vldmia.f32	X, { s4 }
	vldmia.f32	Y, { s8 }
	add	X, X, INC_X
	add	Y, Y, INC_Y
	fmacs   s0  , s4,  s8

	vldmia.f32	X, { s5 }
	vldmia.f32	Y, { s9 }
	add	X, X, INC_X
	add	Y, Y, INC_Y
	fmacs   s1  , s5,  s9

	vldmia.f32	X, { s6 }
	vldmia.f32	Y, { s10 }
	add	X, X, INC_X
	add	Y, Y, INC_Y
	fmacs   s0  , s6,  s10

	vldmia.f32	X, { s7 }
	vldmia.f32	Y, { s11 }
	add	X, X, INC_X
	add	Y, Y, INC_Y
	fmacs   s1  , s7,  s11

.endm


.macro KERNEL_S1

	vldmia.f32	X, { s4 }
	vldmia.f32	Y, { s8 }
	add	X, X, INC_X
	fmacs   s0  , s4,  s8
	add	Y, Y, INC_Y

.endm

#endif

/**************************************************************************************
* End of macro definitions
**************************************************************************************/

	PROLOGUE

	.align 5

	push	{r4 - r9, fp}
	add	fp, sp, #24
	sub	sp, sp, #STACKSIZE				// reserve stack

	sub	r4, fp, #128
	vstm	r4, { s8 - s15 } 				// store floating point registers

	mov	Y, OLD_Y
	ldr	INC_Y, OLD_INC_Y

	movs	r4, #0						// clear floating point register
	vmov	s0, r4
	vmov	s1, r4
#if	defined(DSDOT)
	vcvt.f64.f32    d0, s0
	vcvt.f64.f32    d1, s1
#endif

	cmp	N, #0
	ble	sdot_kernel_L999

#	cmp	INC_X, #0
#	beq	sdot_kernel_L999

#	cmp	INC_Y, #0
#	beq	sdot_kernel_L999

	cmp	INC_X, #1
	bne	sdot_kernel_S_BEGIN

	cmp	INC_Y, #1
	bne	sdot_kernel_S_BEGIN

sdot_kernel_F_BEGIN:

	asrs	I, N, #2					// I = N / 4
	ble	sdot_kernel_F1

sdot_kernel_F4:

	KERNEL_F4

	subs	I, I, #1
	bne	sdot_kernel_F4

sdot_kernel_F1:

	ands	I, N, #3
	ble	sdot_kernel_L999

sdot_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
        bne     sdot_kernel_F10

	b	sdot_kernel_L999

sdot_kernel_S_BEGIN:

	lsl	INC_X, INC_X, #2				// INC_X * SIZE
	lsl	INC_Y, INC_Y, #2				// INC_Y * SIZE

	asrs	I, N, #2					// I = N / 4
	ble	sdot_kernel_S1

sdot_kernel_S4:

	KERNEL_S4

	subs	I, I, #1
	bne	sdot_kernel_S4

sdot_kernel_S1:

	ands	I, N, #3
	ble	sdot_kernel_L999

sdot_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     sdot_kernel_S10






sdot_kernel_L999:

	sub	r3, fp, #128
	vldm	r3, { s8 - s15}					// restore floating point registers

#if	defined(DSDOT)
	vadd.f64	d0 , d0, d1				// set return value
#else
	vadd.f32	s0 , s0, s1				// set return value
#endif

#if !defined(__ARM_PCS_VFP)
#if defined(DSDOT)
	vmov	r0, r1, d0
#else
	vmov	r0, s0
#endif
#endif

	sub	sp, fp, #24
	pop	{r4 - r9, fp}
	bx	lr

	EPILOGUE

