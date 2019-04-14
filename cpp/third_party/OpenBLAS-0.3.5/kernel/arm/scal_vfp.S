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

#define	OLD_INC_X	[sp, #0 ]


#define	N	r0
#define	INC_X	r1
#define	X	r3

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
	vldmia.f64	X,  { d4 - d7 }
	vmul.f64    d4, d4, d0
	vmul.f64    d5, d5, d0
	vmul.f64    d6, d6, d0
	vstmia.f64	X!, { d4 - d5 }
	vmul.f64    d7, d7, d0
	vstmia.f64	X!, { d6 - d7 }

.endm


.macro KERNEL_F1

	vldmia.f64	X,  { d4 }
	vmul.f64    d4, d4, d0
	vstmia.f64	X!, { d4 }

.endm

.macro KERNEL_S1

	vldmia.f64	X,  { d4 }
	vmul.f64    d4, d4, d0
	vstmia.f64	X,  { d4 }
	add	X, X, INC_X

.endm

#else

.macro KERNEL_F4

	vldmia.f32	X,  { s4 - s7 }
	vmul.f32    s4, s4, s0
	vmul.f32    s5, s5, s0
	vmul.f32    s6, s6, s0
	vstmia.f32	X!, { s4 - s5 }
	vmul.f32    s7, s7, s0
	vstmia.f32	X!, { s6 - s7 }

.endm


.macro KERNEL_F1

	vldmia.f32	X,  { s4 }
	vmul.f32    s4, s4, s0
	vstmia.f32	X!, { s4 }

.endm

.macro KERNEL_S1

	vldmia.f32	X,  { s4 }
	vmul.f32    s4, s4, s0
	vstmia.f32	X,  { s4 }
	add	X, X, INC_X

.endm



#endif

#else

#if	defined(DOUBLE)

.macro KERNEL_F4

	pld	[ X, #X_PRE ]

	vldmia.f64	X,  { d4 - d5 }
	vmul.f64    d2, d0, d4
	vmls.f64    d2, d1, d5
	vmul.f64    d3, d0, d5
	fmacd      d3, d1, d4
	vstmia.f64	X!, { d2 - d3 }

	vldmia.f64	X,  { d4 - d5 }
	vmul.f64    d2, d0, d4
	vmls.f64    d2, d1, d5
	vmul.f64    d3, d0, d5
	fmacd      d3, d1, d4
	vstmia.f64	X!, { d2 - d3 }

	pld	[ X, #X_PRE ]

	vldmia.f64	X,  { d4 - d5 }
	vmul.f64    d2, d0, d4
	vmls.f64    d2, d1, d5
	vmul.f64    d3, d0, d5
	fmacd      d3, d1, d4
	vstmia.f64	X!, { d2 - d3 }

	vldmia.f64	X,  { d4 - d5 }
	vmul.f64    d2, d0, d4
	vmls.f64    d2, d1, d5
	vmul.f64    d3, d0, d5
	fmacd      d3, d1, d4
	vstmia.f64	X!, { d2 - d3 }

.endm


.macro KERNEL_F1

	vldmia.f64	X,  { d4 - d5 }
	vmul.f64    d2, d0, d4
	vmls.f64    d2, d1, d5
	vmul.f64    d3, d0, d5
	fmacd      d3, d1, d4
	vstmia.f64	X!, { d2 - d3 }

.endm

.macro KERNEL_S1

	vldmia.f64	X,  { d4 - d5 }
	vmul.f64    d2, d0, d4
	vmls.f64    d2, d1, d5
	vmul.f64    d3, d0, d5
	fmacd      d3, d1, d4
	vstmia.f64	X, { d2 - d3 }
	add	X, X, INC_X

.endm


#else

.macro KERNEL_F4

	pld	[ X, #X_PRE ]

	vldmia.f32	X,  { s4 - s5 }
	vmul.f32    s2, s0, s4
	vmls.f32    s2, s1, s5
	vmul.f32    s3, s0, s5
	fmacs      s3, s1, s4
	vstmia.f32	X!, { s2 - s3 }

	vldmia.f32	X,  { s4 - s5 }
	vmul.f32    s2, s0, s4
	vmls.f32    s2, s1, s5
	vmul.f32    s3, s0, s5
	fmacs      s3, s1, s4
	vstmia.f32	X!, { s2 - s3 }

	vldmia.f32	X,  { s4 - s5 }
	vmul.f32    s2, s0, s4
	vmls.f32    s2, s1, s5
	vmul.f32    s3, s0, s5
	fmacs      s3, s1, s4
	vstmia.f32	X!, { s2 - s3 }

	vldmia.f32	X,  { s4 - s5 }
	vmul.f32    s2, s0, s4
	vmls.f32    s2, s1, s5
	vmul.f32    s3, s0, s5
	fmacs      s3, s1, s4
	vstmia.f32	X!, { s2 - s3 }

.endm


.macro KERNEL_F1

	vldmia.f32	X,  { s4 - s5 }
	vmul.f32    s2, s0, s4
	vmls.f32    s2, s1, s5
	vmul.f32    s3, s0, s5
	fmacs      s3, s1, s4
	vstmia.f32	X!, { s2 - s3 }

.endm

.macro KERNEL_S1

	vldmia.f32	X,  { s4 - s5 }
	vmul.f32    s2, s0, s4
	vmls.f32    s2, s1, s5
	vmul.f32    s3, s0, s5
	fmacs      s3, s1, s4
	vstmia.f32	X, { s2 - s3 }
	add	X, X, INC_X

.endm



#endif

#endif

/**************************************************************************************
* End of macro definitions
**************************************************************************************/

	PROLOGUE

	.align 5

	ldr    INC_X , OLD_INC_X

	cmp	N, #0
	ble	scal_kernel_L999

	cmp	INC_X, #0
	ble	scal_kernel_L999

	cmp	INC_X, #1
	bne	scal_kernel_S_BEGIN


scal_kernel_F_BEGIN:


	asrs	I, N, #2					// I = N / 4
	ble	scal_kernel_F1

	.align 5

scal_kernel_F4:

#if !defined(COMPLEX) && !defined(DOUBLE)
	pld	[ X, #X_PRE ]
#endif

	KERNEL_F4

	subs	I, I, #1
	ble	scal_kernel_F1

	KERNEL_F4

	subs	I, I, #1
	bne	scal_kernel_F4

scal_kernel_F1:

	ands	I, N, #3
	ble	scal_kernel_L999

scal_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
        bne     scal_kernel_F10

	b	scal_kernel_L999

scal_kernel_S_BEGIN:

#if defined(COMPLEX)

#if defined(DOUBLE)
	lsl	INC_X, INC_X, #4				// INC_X * SIZE * 2
#else
	lsl	INC_X, INC_X, #3				// INC_X * SIZE * 2
#endif

#else

#if defined(DOUBLE)
	lsl	INC_X, INC_X, #3				// INC_X * SIZE
#else
	lsl	INC_X, INC_X, #2				// INC_X * SIZE
#endif

#endif


	asrs	I, N, #2					// I = N / 4
	ble	scal_kernel_S1

	.align 5

scal_kernel_S4:

	KERNEL_S1
	KERNEL_S1
	KERNEL_S1
	KERNEL_S1

	subs	I, I, #1
	bne	scal_kernel_S4

scal_kernel_S1:

	ands	I, N, #3
	ble	scal_kernel_L999

scal_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     scal_kernel_S10


scal_kernel_L999:

	mov	r0, #0		// set return value

	bx	lr

	EPILOGUE

