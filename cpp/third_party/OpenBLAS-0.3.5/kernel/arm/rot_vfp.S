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
* 2013/11/15 Saar
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*
**************************************************************************************/

#define ASSEMBLER
#include "common.h"

#define STACKSIZE 256

#define	OLD_INC_Y	[fp, #0 ]

#if !defined(__ARM_PCS_VFP)
#if !defined(DOUBLE)
#define	OLD_C		[fp, #4]
#define	OLD_S		[fp, #8]
#else
#define	OLD_C		[fp, #8]
#define	OLD_S		[fp, #16]
#endif
#endif

#define	N	r0
#define X	r1
#define	INC_X	r2
#define	Y	r3
#define INC_Y	r4

#define I	r12

#define X_PRE	512

/**************************************************************************************
* Macro definitions
**************************************************************************************/

/*****************************************************************************************/



#if	!defined(COMPLEX)

#if	defined(DOUBLE)

.macro KERNEL_F4

	pld	[ X, #X_PRE ]
	pld	[ Y, #X_PRE ]

	vldmia.f64	X,  { d4 }
	vldmia.f64	Y,  { d5 }
	vmul.f64    d2 , d0, d4
	fmacd       d2 , d1, d5
	vmul.f64    d3 , d0, d5
	vmls.f64    d3 , d1, d4
	vstmia.f64	X!, { d2 }
	vstmia.f64	Y!, { d3 }

	vldmia.f64	X,  { d4 }
	vldmia.f64	Y,  { d5 }
	vmul.f64    d2 , d0, d4
	fmacd       d2 , d1, d5
	vmul.f64    d3 , d0, d5
	vmls.f64    d3 , d1, d4
	vstmia.f64	X!, { d2 }
	vstmia.f64	Y!, { d3 }

	vldmia.f64	X,  { d4 }
	vldmia.f64	Y,  { d5 }
	vmul.f64    d2 , d0, d4
	fmacd       d2 , d1, d5
	vmul.f64    d3 , d0, d5
	vmls.f64    d3 , d1, d4
	vstmia.f64	X!, { d2 }
	vstmia.f64	Y!, { d3 }

	vldmia.f64	X,  { d4 }
	vldmia.f64	Y,  { d5 }
	vmul.f64    d2 , d0, d4
	fmacd       d2 , d1, d5
	vmul.f64    d3 , d0, d5
	vmls.f64    d3 , d1, d4
	vstmia.f64	X!, { d2 }
	vstmia.f64	Y!, { d3 }

.endm


.macro KERNEL_F1

	vldmia.f64	X,  { d4 }
	vldmia.f64	Y,  { d5 }
	vmul.f64    d2 , d0, d4
	fmacd       d2 , d1, d5
	vmul.f64    d3 , d0, d5
	vmls.f64    d3 , d1, d4
	vstmia.f64	X!, { d2 }
	vstmia.f64	Y!, { d3 }

.endm

.macro KERNEL_S1

	vldmia.f64	X,  { d4 }
	vldmia.f64	Y,  { d5 }
	vmul.f64    d2 , d0, d4
	fmacd       d2 , d1, d5
	vmul.f64    d3 , d0, d5
	vmls.f64    d3 , d1, d4
	vstmia.f64	X, { d2 }
	vstmia.f64	Y, { d3 }

	add	X, X, INC_X
	add	Y, Y, INC_Y

.endm

#else

.macro KERNEL_F4

	vldmia.f32	X,  { s4 }
	vldmia.f32	Y,  { s5 }
	vmul.f32    s2 , s0, s4
	fmacs       s2 , s1, s5
	vmul.f32    s3 , s0, s5
	vmls.f32    s3 , s1, s4
	vstmia.f32	X!, { s2 }
	vstmia.f32	Y!, { s3 }

	vldmia.f32	X,  { s4 }
	vldmia.f32	Y,  { s5 }
	vmul.f32    s2 , s0, s4
	fmacs       s2 , s1, s5
	vmul.f32    s3 , s0, s5
	vmls.f32    s3 , s1, s4
	vstmia.f32	X!, { s2 }
	vstmia.f32	Y!, { s3 }

	vldmia.f32	X,  { s4 }
	vldmia.f32	Y,  { s5 }
	vmul.f32    s2 , s0, s4
	fmacs       s2 , s1, s5
	vmul.f32    s3 , s0, s5
	vmls.f32    s3 , s1, s4
	vstmia.f32	X!, { s2 }
	vstmia.f32	Y!, { s3 }

	vldmia.f32	X,  { s4 }
	vldmia.f32	Y,  { s5 }
	vmul.f32    s2 , s0, s4
	fmacs       s2 , s1, s5
	vmul.f32    s3 , s0, s5
	vmls.f32    s3 , s1, s4
	vstmia.f32	X!, { s2 }
	vstmia.f32	Y!, { s3 }

.endm


.macro KERNEL_F1

	vldmia.f32	X,  { s4 }
	vldmia.f32	Y,  { s5 }
	vmul.f32    s2 , s0, s4
	fmacs       s2 , s1, s5
	vmul.f32    s3 , s0, s5
	vmls.f32    s3 , s1, s4
	vstmia.f32	X!, { s2 }
	vstmia.f32	Y!, { s3 }

.endm

.macro KERNEL_S1

	vldmia.f32	X,  { s4 }
	vldmia.f32	Y,  { s5 }
	vmul.f32    s2 , s0, s4
	fmacs       s2 , s1, s5
	vmul.f32    s3 , s0, s5
	vmls.f32    s3 , s1, s4
	vstmia.f32	X, { s2 }
	vstmia.f32	Y, { s3 }

	add	X, X, INC_X
	add	Y, Y, INC_Y

.endm



#endif

#else

#if	defined(DOUBLE)

.macro KERNEL_F4

	pld	[ X, #X_PRE ]
	pld	[ Y, #X_PRE ]

	vldmia.f64	X,  { d4 - d5 }
	vldmia.f64	Y,  { d6 - d7 }
	vmul.f64    d2 , d0, d4
	fmacd       d2 , d1, d6
	vmul.f64    d3 , d0, d6
	vmls.f64    d3 , d1, d4
	vstmia.f64	X!, { d2 }
	vstmia.f64	Y!, { d3 }
	vmul.f64    d2 , d0, d5
	fmacd       d2 , d1, d7
	vmul.f64    d3 , d0, d7
	vmls.f64    d3 , d1, d5
	vstmia.f64	X!, { d2 }
	vstmia.f64	Y!, { d3 }

	vldmia.f64	X,  { d4 - d5 }
	vldmia.f64	Y,  { d6 - d7 }
	vmul.f64    d2 , d0, d4
	fmacd       d2 , d1, d6
	vmul.f64    d3 , d0, d6
	vmls.f64    d3 , d1, d4
	vstmia.f64	X!, { d2 }
	vstmia.f64	Y!, { d3 }
	vmul.f64    d2 , d0, d5
	fmacd       d2 , d1, d7
	vmul.f64    d3 , d0, d7
	vmls.f64    d3 , d1, d5
	vstmia.f64	X!, { d2 }
	vstmia.f64	Y!, { d3 }

	pld	[ X, #X_PRE ]
	pld	[ Y, #X_PRE ]

	vldmia.f64	X,  { d4 - d5 }
	vldmia.f64	Y,  { d6 - d7 }
	vmul.f64    d2 , d0, d4
	fmacd       d2 , d1, d6
	vmul.f64    d3 , d0, d6
	vmls.f64    d3 , d1, d4
	vstmia.f64	X!, { d2 }
	vstmia.f64	Y!, { d3 }
	vmul.f64    d2 , d0, d5
	fmacd       d2 , d1, d7
	vmul.f64    d3 , d0, d7
	vmls.f64    d3 , d1, d5
	vstmia.f64	X!, { d2 }
	vstmia.f64	Y!, { d3 }

	vldmia.f64	X,  { d4 - d5 }
	vldmia.f64	Y,  { d6 - d7 }
	vmul.f64    d2 , d0, d4
	fmacd       d2 , d1, d6
	vmul.f64    d3 , d0, d6
	vmls.f64    d3 , d1, d4
	vstmia.f64	X!, { d2 }
	vstmia.f64	Y!, { d3 }
	vmul.f64    d2 , d0, d5
	fmacd       d2 , d1, d7
	vmul.f64    d3 , d0, d7
	vmls.f64    d3 , d1, d5
	vstmia.f64	X!, { d2 }
	vstmia.f64	Y!, { d3 }

.endm


.macro KERNEL_F1

	vldmia.f64	X,  { d4 - d5 }
	vldmia.f64	Y,  { d6 - d7 }
	vmul.f64    d2 , d0, d4
	fmacd       d2 , d1, d6
	vmul.f64    d3 , d0, d6
	vmls.f64    d3 , d1, d4
	vstmia.f64	X!, { d2 }
	vstmia.f64	Y!, { d3 }
	vmul.f64    d2 , d0, d5
	fmacd       d2 , d1, d7
	vmul.f64    d3 , d0, d7
	vmls.f64    d3 , d1, d5
	vstmia.f64	X!, { d2 }
	vstmia.f64	Y!, { d3 }


.endm

.macro KERNEL_S1

	vldmia.f64	X,  { d4 - d5 }
	vldmia.f64	Y,  { d6 - d7 }
	vmul.f64    d2 , d0, d4
	fmacd       d2 , d1, d6
	vmul.f64    d3 , d0, d6
	vmls.f64    d3 , d1, d4
	vstr	    d2 , [ X, #0 ]
	vstr	    d3 , [ Y, #0 ]
	vmul.f64    d2 , d0, d5
	fmacd       d2 , d1, d7
	vmul.f64    d3 , d0, d7
	vmls.f64    d3 , d1, d5
	vstr	    d2 , [ X, #8 ]
	vstr	    d3 , [ Y, #8 ]

	add	X, X, INC_X
	add	Y, Y, INC_Y

.endm



#else

.macro KERNEL_F4

	pld	[ X, #X_PRE ]
	pld	[ Y, #X_PRE ]

	vldmia.f32	X,  { s4 - s5 }
	vldmia.f32	Y,  { s6 - s7 }
	vmul.f32    s2 , s0, s4
	fmacs       s2 , s1, s6
	vmul.f32    s3 , s0, s6
	vmls.f32    s3 , s1, s4
	vstmia.f32	X!, { s2 }
	vstmia.f32	Y!, { s3 }
	vmul.f32    s2 , s0, s5
	fmacs       s2 , s1, s7
	vmul.f32    s3 , s0, s7
	vmls.f32    s3 , s1, s5
	vstmia.f32	X!, { s2 }
	vstmia.f32	Y!, { s3 }

	vldmia.f32	X,  { s4 - s5 }
	vldmia.f32	Y,  { s6 - s7 }
	vmul.f32    s2 , s0, s4
	fmacs       s2 , s1, s6
	vmul.f32    s3 , s0, s6
	vmls.f32    s3 , s1, s4
	vstmia.f32	X!, { s2 }
	vstmia.f32	Y!, { s3 }
	vmul.f32    s2 , s0, s5
	fmacs       s2 , s1, s7
	vmul.f32    s3 , s0, s7
	vmls.f32    s3 , s1, s5
	vstmia.f32	X!, { s2 }
	vstmia.f32	Y!, { s3 }

	pld	[ X, #X_PRE ]
	pld	[ Y, #X_PRE ]

	vldmia.f32	X,  { s4 - s5 }
	vldmia.f32	Y,  { s6 - s7 }
	vmul.f32    s2 , s0, s4
	fmacs       s2 , s1, s6
	vmul.f32    s3 , s0, s6
	vmls.f32    s3 , s1, s4
	vstmia.f32	X!, { s2 }
	vstmia.f32	Y!, { s3 }
	vmul.f32    s2 , s0, s5
	fmacs       s2 , s1, s7
	vmul.f32    s3 , s0, s7
	vmls.f32    s3 , s1, s5
	vstmia.f32	X!, { s2 }
	vstmia.f32	Y!, { s3 }

	vldmia.f32	X,  { s4 - s5 }
	vldmia.f32	Y,  { s6 - s7 }
	vmul.f32    s2 , s0, s4
	fmacs       s2 , s1, s6
	vmul.f32    s3 , s0, s6
	vmls.f32    s3 , s1, s4
	vstmia.f32	X!, { s2 }
	vstmia.f32	Y!, { s3 }
	vmul.f32    s2 , s0, s5
	fmacs       s2 , s1, s7
	vmul.f32    s3 , s0, s7
	vmls.f32    s3 , s1, s5
	vstmia.f32	X!, { s2 }
	vstmia.f32	Y!, { s3 }

.endm


.macro KERNEL_F1

	vldmia.f32	X,  { s4 - s5 }
	vldmia.f32	Y,  { s6 - s7 }
	vmul.f32    s2 , s0, s4
	fmacs       s2 , s1, s6
	vmul.f32    s3 , s0, s6
	vmls.f32    s3 , s1, s4
	vstmia.f32	X!, { s2 }
	vstmia.f32	Y!, { s3 }
	vmul.f32    s2 , s0, s5
	fmacs       s2 , s1, s7
	vmul.f32    s3 , s0, s7
	vmls.f32    s3 , s1, s5
	vstmia.f32	X!, { s2 }
	vstmia.f32	Y!, { s3 }


.endm

.macro KERNEL_S1

	vldmia.f32	X,  { s4 - s5 }
	vldmia.f32	Y,  { s6 - s7 }
	vmul.f32    s2 , s0, s4
	fmacs       s2 , s1, s6
	vmul.f32    s3 , s0, s6
	vmls.f32    s3 , s1, s4
	vstr	    s2 , [ X, #0 ]
	vstr	    s3 , [ Y, #0 ]
	vmul.f32    s2 , s0, s5
	fmacs       s2 , s1, s7
	vmul.f32    s3 , s0, s7
	vmls.f32    s3 , s1, s5
	vstr	    s2 , [ X, #4 ]
	vstr	    s3 , [ Y, #4 ]

	add	X, X, INC_X
	add	Y, Y, INC_Y

.endm


#endif

#endif

/**************************************************************************************
* End of macro definitions
**************************************************************************************/

	PROLOGUE

	.align 5
	push    {r4 , fp}
        add     fp, sp, #8

	ldr    INC_Y , OLD_INC_Y
#if !defined(__ARM_PCS_VFP)
#if !defined(DOUBLE)
	vldr	s0, OLD_C
	vldr	s1, OLD_S
#else
	vldr	d0, OLD_C
	vldr	d1, OLD_S
#endif
#endif

	cmp	N, #0
	ble	rot_kernel_L999
/*
	cmp	INC_X, #0
	beq	rot_kernel_L999

	cmp	INC_Y, #0
	beq	rot_kernel_L999
*/
	cmp	INC_X, #1
	bne	rot_kernel_S_BEGIN

	cmp	INC_Y, #1
	bne	rot_kernel_S_BEGIN


rot_kernel_F_BEGIN:


	asrs	I, N, #2					// I = N / 4
	ble	rot_kernel_F1

	.align 5

rot_kernel_F4:

#if !defined(COMPLEX) && !defined(DOUBLE)
	pld	[ X, #X_PRE ]
	pld	[ Y, #X_PRE ]
#endif

	KERNEL_F4

	subs	I, I, #1
	ble	rot_kernel_F1

	KERNEL_F4

	subs	I, I, #1
	bne	rot_kernel_F4

rot_kernel_F1:

	ands	I, N, #3
	ble	rot_kernel_L999

rot_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
        bne     rot_kernel_F10

	b	rot_kernel_L999

rot_kernel_S_BEGIN:

#if defined(COMPLEX)

#if defined(DOUBLE)
	lsl	INC_X, INC_X, #4				// INC_X * SIZE * 2
	lsl	INC_Y, INC_Y, #4				// INC_Y * SIZE * 2
#else
	lsl	INC_X, INC_X, #3				// INC_X * SIZE * 2
	lsl	INC_Y, INC_Y, #3				// INC_Y * SIZE * 2
#endif

#else

#if defined(DOUBLE)
	lsl	INC_X, INC_X, #3				// INC_X * SIZE
	lsl	INC_Y, INC_Y, #3				// INC_Y * SIZE
#else
	lsl	INC_X, INC_X, #2				// INC_X * SIZE
	lsl	INC_Y, INC_Y, #2				// INC_Y * SIZE
#endif

#endif


	asrs	I, N, #2					// I = N / 4
	ble	rot_kernel_S1

	.align 5

rot_kernel_S4:

	KERNEL_S1
	KERNEL_S1
	KERNEL_S1
	KERNEL_S1

	subs	I, I, #1
	bne	rot_kernel_S4

rot_kernel_S1:

	ands	I, N, #3
	ble	rot_kernel_L999

rot_kernel_S10:

	KERNEL_S1
	
	cmp	INC_X, #0
	beq	rot_kernel_L999

	cmp	INC_Y, #0
	beq	rot_kernel_L999

	subs    I, I, #1
        bne     rot_kernel_S10


rot_kernel_L999:

	mov	r0, #0		// set return value

	sub     sp, fp, #8
	pop     {r4,fp}
	bx	lr

	EPILOGUE

