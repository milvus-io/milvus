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


#define I	r12

#define X_PRE	512

/**************************************************************************************
* Macro definitions
**************************************************************************************/

#if	!defined(COMPLEX)

#if	defined(DOUBLE)

.macro KERNEL_F4

	pld	[ X, #X_PRE  ]
	vldmia.f64	X!, { d4 - d5 }
	vabs.f64   d4,  d4
	vadd.f64   d0  , d0,  d4
	vabs.f64   d5,  d5
	vldmia.f64	X!, { d6 - d7 }
	vabs.f64   d6,  d6
	vadd.f64   d1  , d1,  d5
	vabs.f64   d7,  d7
	vadd.f64   d0  , d0,  d6
	vadd.f64   d1  , d1,  d7

.endm

.macro KERNEL_F1

	vldmia.f64	X!, { d4 }
	vabs.f64   d4,  d4
	vadd.f64   d0  , d0,  d4

.endm


.macro KERNEL_S4

	vldmia.f64	X, { d4 }
	vabs.f64   d4,  d4
	vadd.f64   d0  , d0,  d4
	add	X, X, INC_X

	vldmia.f64	X, { d4 }
	vabs.f64   d4,  d4
	vadd.f64   d0  , d0,  d4
	add	X, X, INC_X

	vldmia.f64	X, { d4 }
	vabs.f64   d4,  d4
	vadd.f64   d0  , d0,  d4
	add	X, X, INC_X

	vldmia.f64	X, { d4 }
	vabs.f64   d4,  d4
	vadd.f64   d0  , d0,  d4
	add	X, X, INC_X

.endm


.macro KERNEL_S1

	vldmia.f64	X, { d4 }
	vabs.f64   d4,  d4
	vadd.f64   d0  , d0,  d4
	add	X, X, INC_X

.endm

#else

.macro KERNEL_F4

	vldmia.f32	X!, { s4 - s5 }
	vabs.f32   s4,  s4
	vadd.f32   s0  , s0,  s4
	vabs.f32   s5,  s5
	vldmia.f32	X!, { s6 - s7 }
	vabs.f32   s6,  s6
	vadd.f32   s1  , s1,  s5
	vabs.f32   s7,  s7
	vadd.f32   s0  , s0,  s6
	vadd.f32   s1  , s1,  s7

.endm

.macro KERNEL_F1

	vldmia.f32	X!, { s4 }
	vabs.f32   s4,  s4
	vadd.f32   s0  , s0,  s4

.endm


.macro KERNEL_S4

	vldmia.f32	X, { s4 }
	vabs.f32   s4,  s4
	vadd.f32   s0  , s0,  s4
	add	X, X, INC_X

	vldmia.f32	X, { s4 }
	vabs.f32   s4,  s4
	vadd.f32   s0  , s0,  s4
	add	X, X, INC_X

	vldmia.f32	X, { s4 }
	vabs.f32   s4,  s4
	vadd.f32   s0  , s0,  s4
	add	X, X, INC_X

	vldmia.f32	X, { s4 }
	vabs.f32   s4,  s4
	vadd.f32   s0  , s0,  s4
	add	X, X, INC_X

.endm


.macro KERNEL_S1

	vldmia.f32	X, { s4 }
	vabs.f32   s4,  s4
	vadd.f32   s0  , s0,  s4
	add	X, X, INC_X

.endm


#endif

#else

#if	defined(DOUBLE)

.macro KERNEL_F4

	pld	[ X, #X_PRE  ]
	vldmia.f64	X!, { d4 - d5 }
	vabs.f64   d4,  d4
	vadd.f64   d0  , d0,  d4
	vabs.f64   d5,  d5
	vldmia.f64	X!, { d6 - d7 }
	vabs.f64   d6,  d6
	vadd.f64   d1  , d1,  d5
	vabs.f64   d7,  d7
	vadd.f64   d0  , d0,  d6
	vadd.f64   d1  , d1,  d7

	pld	[ X, #X_PRE  ]
	vldmia.f64	X!, { d4 - d5 }
	vabs.f64   d4,  d4
	vadd.f64   d0  , d0,  d4
	vabs.f64   d5,  d5
	vldmia.f64	X!, { d6 - d7 }
	vabs.f64   d6,  d6
	vadd.f64   d1  , d1,  d5
	vabs.f64   d7,  d7
	vadd.f64   d0  , d0,  d6
	vadd.f64   d1  , d1,  d7


.endm

.macro KERNEL_F1

	vldmia.f64	X!, { d4 }
	vabs.f64   d4,  d4
	vadd.f64   d0  , d0,  d4

	vldmia.f64	X!, { d4 }
	vabs.f64   d4,  d4
	vadd.f64   d0  , d0,  d4


.endm


.macro KERNEL_S4

	vldmia.f64	X, { d4 -d5 }
	vabs.f64   d4,  d4
	vadd.f64   d0  , d0,  d4
	vabs.f64   d5,  d5
	vadd.f64   d0  , d0,  d5
	add	X, X, INC_X

	vldmia.f64	X, { d4 -d5 }
	vabs.f64   d4,  d4
	vadd.f64   d0  , d0,  d4
	vabs.f64   d5,  d5
	vadd.f64   d0  , d0,  d5
	add	X, X, INC_X

	vldmia.f64	X, { d4 -d5 }
	vabs.f64   d4,  d4
	vadd.f64   d0  , d0,  d4
	vabs.f64   d5,  d5
	vadd.f64   d0  , d0,  d5
	add	X, X, INC_X

	vldmia.f64	X, { d4 -d5 }
	vabs.f64   d4,  d4
	vadd.f64   d0  , d0,  d4
	vabs.f64   d5,  d5
	vadd.f64   d0  , d0,  d5
	add	X, X, INC_X

.endm


.macro KERNEL_S1

	vldmia.f64	X, { d4 -d5 }
	vabs.f64   d4,  d4
	vadd.f64   d0  , d0,  d4
	vabs.f64   d5,  d5
	vadd.f64   d0  , d0,  d5
	add	X, X, INC_X

.endm

#else

.macro KERNEL_F4

	pld	[ X, #X_PRE  ]
	vldmia.f32	X!, { s4 - s5 }
	vabs.f32   s4,  s4
	vadd.f32   s0  , s0,  s4
	vabs.f32   s5,  s5
	vldmia.f32	X!, { s6 - s7 }
	vabs.f32   s6,  s6
	vadd.f32   s1  , s1,  s5
	vabs.f32   s7,  s7
	vadd.f32   s0  , s0,  s6
	vadd.f32   s1  , s1,  s7

	vldmia.f32	X!, { s4 - s5 }
	vabs.f32   s4,  s4
	vadd.f32   s0  , s0,  s4
	vabs.f32   s5,  s5
	vldmia.f32	X!, { s6 - s7 }
	vabs.f32   s6,  s6
	vadd.f32   s1  , s1,  s5
	vabs.f32   s7,  s7
	vadd.f32   s0  , s0,  s6
	vadd.f32   s1  , s1,  s7


.endm

.macro KERNEL_F1

	vldmia.f32	X!, { s4 }
	vabs.f32   s4,  s4
	vadd.f32   s0  , s0,  s4

	vldmia.f32	X!, { s4 }
	vabs.f32   s4,  s4
	vadd.f32   s0  , s0,  s4

.endm


.macro KERNEL_S4

	vldmia.f32	X, { s4 -s5 }
	vabs.f32   s4,  s4
	vadd.f32   s0  , s0,  s4
	vabs.f32   s5,  s5
	vadd.f32   s0  , s0,  s5
	add	X, X, INC_X

	vldmia.f32	X, { s4 -s5 }
	vabs.f32   s4,  s4
	vadd.f32   s0  , s0,  s4
	vabs.f32   s5,  s5
	vadd.f32   s0  , s0,  s5
	add	X, X, INC_X

	vldmia.f32	X, { s4 -s5 }
	vabs.f32   s4,  s4
	vadd.f32   s0  , s0,  s4
	vabs.f32   s5,  s5
	vadd.f32   s0  , s0,  s5
	add	X, X, INC_X

	vldmia.f32	X, { s4 -s5 }
	vabs.f32   s4,  s4
	vadd.f32   s0  , s0,  s4
	vabs.f32   s5,  s5
	vadd.f32   s0  , s0,  s5
	add	X, X, INC_X

.endm


.macro KERNEL_S1

	vldmia.f32	X, { s4 -s5 }
	vabs.f32   s4,  s4
	vadd.f32   s0  , s0,  s4
	vabs.f32   s5,  s5
	vadd.f32   s0  , s0,  s5
	add	X, X, INC_X

.endm

#endif

#endif

/**************************************************************************************
* End of macro definitions
**************************************************************************************/

	PROLOGUE

	.align 5

        movs    r12, #0                                          // clear floating point register
        vmov    s0, r12
        vmov    s1, r12
#if     defined(DOUBLE)
        vcvt.f64.f32    d0, s0
        vcvt.f64.f32    d1, s1
#endif

	cmp	N, #0
	ble	asum_kernel_L999

	cmp	INC_X, #0
	beq	asum_kernel_L999

	cmp	INC_X, #1
	bne	asum_kernel_S_BEGIN


asum_kernel_F_BEGIN:

	asrs	I, N, #2					// I = N / 4
	ble	asum_kernel_F1

	.align 5

asum_kernel_F4:

#if !defined(DOUBLE) && !defined(COMPLEX)
	pld	[ X, #X_PRE  ]
#endif
	KERNEL_F4

	subs	I, I, #1
	ble	asum_kernel_F1

	KERNEL_F4

	subs	I, I, #1
	bne	asum_kernel_F4

asum_kernel_F1:

	ands	I, N, #3
	ble	asum_kernel_L999

asum_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
        bne     asum_kernel_F10

	b	asum_kernel_L999

asum_kernel_S_BEGIN:

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
	ble	asum_kernel_S1

	.align 5

asum_kernel_S4:

	KERNEL_S4

	subs	I, I, #1
	bne	asum_kernel_S4

asum_kernel_S1:

	ands	I, N, #3
	ble	asum_kernel_L999

asum_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     asum_kernel_S10


asum_kernel_L999:


#if defined(DOUBLE)
	vadd.f64	d0 , d0, d1				// set return value
#else
	vadd.f32	s0 , s0, s1				// set return value
#endif

#if !defined(__ARM_PCS_VFP)
#if !defined(DOUBLE)
	vmov	r0, s0
#else
	vmov	r0, r1, d0
#endif
#endif

	bx	lr

	EPILOGUE

