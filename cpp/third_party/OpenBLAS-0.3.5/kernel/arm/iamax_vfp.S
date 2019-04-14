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
* 2013/11/14 Saar
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
#define INDEX	r3
#define Z	r4

#define I	r12

#define X_PRE	512

/**************************************************************************************
* Macro definitions
**************************************************************************************/

#if	defined(USE_ABS)

#if	defined(DOUBLE)

#define	VABS(x0,x1)	vabs.f64	x0, x1

#else

#define	VABS(x0,x1)	vabs.f32	x0, x1

#endif

#else

#define VABS(x0,x1)	nop

#endif

/*****************************************************************************************/

#if	defined(USE_MIN)

#define	MOVCOND		movlt

#if	defined(DOUBLE)

#define	VMOVCOND	vmovlt.f64

#else

#define	VMOVCOND	vmovlt.f32

#endif

#else

#define	MOVCOND		movgt

#if	defined(DOUBLE)

#define	VMOVCOND	vmovgt.f64

#else

#define	VMOVCOND	vmovgt.f32

#endif


#endif


/*****************************************************************************************/



#if	!defined(COMPLEX)

#if	defined(DOUBLE)

.macro INIT_F

	vldmia.f64	X!, { d0 }
	VABS(   d0,  d0 )
	mov	Z, #1
	mov	INDEX, Z

.endm

.macro KERNEL_F1

	vldmia.f64	X!, { d4 }
	add	Z, Z, #1
	VABS(   d4,  d4 )
	vcmpe.f64  	d4,  d0
	vmrs		APSR_nzcv, fpscr
	VMOVCOND	d0,  d4
	MOVCOND		INDEX, Z

.endm

.macro INIT_S

	vldmia.f64	X, { d0 }
	VABS(   d0,  d0 )
	mov	Z, #1
	mov	INDEX, Z
	add	X, X, INC_X

.endm


.macro KERNEL_S1

	vldmia.f64	X, { d4 }
	add	Z, Z, #1
	VABS(   d4,  d4 )
	vcmpe.f64  	d4,  d0
	vmrs		APSR_nzcv, fpscr
	VMOVCOND	d0,  d4
	MOVCOND		INDEX, Z
	add	X, X, INC_X

.endm

#else

.macro INIT_F

	vldmia.f32	X!, { s0 }
	VABS(   s0,  s0 )
	mov	Z, #1
	mov	INDEX, Z

.endm

.macro KERNEL_F1

	vldmia.f32	X!, { s4 }
	add	Z, Z, #1
	VABS(   s4,  s4 )
	vcmpe.f32  	s4,  s0
	vmrs		APSR_nzcv, fpscr
	VMOVCOND	s0,  s4
	MOVCOND		INDEX, Z

.endm

.macro INIT_S

	vldmia.f32	X, { s0 }
	VABS(   s0,  s0 )
	mov	Z, #1
	mov	INDEX, Z
	add	X, X, INC_X

.endm


.macro KERNEL_S1

	vldmia.f32	X, { s4 }
	add	Z, Z, #1
	VABS(   s4,  s4 )
	vcmpe.f32  	s4,  s0
	vmrs		APSR_nzcv, fpscr
	VMOVCOND	s0,  s4
	MOVCOND		INDEX, Z
	add	X, X, INC_X

.endm




#endif

#else

#if	defined(DOUBLE)

.macro INIT_F

	vldmia.f64	X!, { d0 -d1 }
	vabs.f64   d0,  d0
	vabs.f64   d1,  d1
	vadd.f64   d0  , d0,  d1
	mov	Z, #1
	mov	INDEX, Z

.endm


.macro KERNEL_F1

	vldmia.f64	X!, { d4 - d5 }
	add	Z, Z, #1
	vabs.f64   d4,  d4
	vabs.f64   d5,  d5
	vadd.f64   d4  , d4,  d5
	vcmpe.f64  	d4,  d0
	vmrs		APSR_nzcv, fpscr
	VMOVCOND	d0,  d4
	MOVCOND		INDEX, Z

.endm

.macro INIT_S

	vldmia.f64	X, { d0 -d1 }
	vabs.f64   d0,  d0
	vabs.f64   d1,  d1
	vadd.f64   d0  , d0,  d1
	mov	Z, #1
	mov	INDEX, Z
	add	X, X, INC_X

.endm



.macro KERNEL_S1

	vldmia.f64	X, { d4 - d5 }
	add	Z, Z, #1
	vabs.f64   d4,  d4
	vabs.f64   d5,  d5
	vadd.f64   d4  , d4,  d5
	vcmpe.f64  	d4,  d0
	vmrs		APSR_nzcv, fpscr
	VMOVCOND	d0,  d4
	MOVCOND		INDEX, Z
	add	X, X, INC_X

.endm

#else

.macro INIT_F

	vldmia.f32	X!, { s0 -s1 }
	vabs.f32   s0,  s0
	vabs.f32   s1,  s1
	vadd.f32   s0  , s0,  s1
	mov	Z, #1
	mov	INDEX, Z

.endm


.macro KERNEL_F1

	vldmia.f32	X!, { s4 - s5 }
	add	Z, Z, #1
	vabs.f32   s4,  s4
	vabs.f32   s5,  s5
	vadd.f32   s4  , s4,  s5
	vcmpe.f32  	s4,  s0
	vmrs		APSR_nzcv, fpscr
	VMOVCOND	s0,  s4
	MOVCOND		INDEX, Z

.endm

.macro INIT_S

	vldmia.f32	X, { s0 -s1 }
	vabs.f32   s0,  s0
	vabs.f32   s1,  s1
	vadd.f32   s0  , s0,  s1
	mov	Z, #1
	mov	INDEX, Z
	add	X, X, INC_X

.endm



.macro KERNEL_S1

	vldmia.f32	X, { s4 - s5 }
	add	Z, Z, #1
	vabs.f32   s4,  s4
	vabs.f32   s5,  s5
	vadd.f32   s4  , s4,  s5
	vcmpe.f32  	s4,  s0
	vmrs		APSR_nzcv, fpscr
	VMOVCOND	s0,  s4
	MOVCOND		INDEX, Z
	add	X, X, INC_X

.endm




#endif

#endif

/**************************************************************************************
* End of macro definitions
**************************************************************************************/

	PROLOGUE

	.align 5
	push    {r4}

        movs    r12, #0                                          // clear floating point register
        vmov    s0, r12
#if     defined(DOUBLE)
        vcvt.f64.f32    d0, s0
#endif

	mov	INDEX, #0

	cmp	N, #0
	ble	iamax_kernel_L999

	cmp	INC_X, #0
	beq	iamax_kernel_L999


	cmp	INC_X, #1
	bne	iamax_kernel_S_BEGIN


iamax_kernel_F_BEGIN:

	INIT_F

	subs	N, N , #1
	ble	iamax_kernel_L999

	asrs	I, N, #2					// I = N / 4
	ble	iamax_kernel_F1

	.align 5

iamax_kernel_F4:

	pld	[ X, #X_PRE ]
	KERNEL_F1
	KERNEL_F1
#if defined(COMPLEX) && defined(DOUBLE)
	pld	[ X, #X_PRE ]
#endif
	KERNEL_F1
	KERNEL_F1

	subs	I, I, #1
	ble	iamax_kernel_F1


#if defined(COMPLEX) || defined(DOUBLE)
	pld	[ X, #X_PRE ]
#endif
	KERNEL_F1
	KERNEL_F1
#if defined(COMPLEX) && defined(DOUBLE)
	pld	[ X, #X_PRE ]
#endif
	KERNEL_F1
	KERNEL_F1

	subs	I, I, #1
	bne	iamax_kernel_F4

iamax_kernel_F1:

	ands	I, N, #3
	ble	iamax_kernel_L999

iamax_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
        bne     iamax_kernel_F10

	b	iamax_kernel_L999

iamax_kernel_S_BEGIN:

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

	INIT_S

	subs	N, N , #1
	ble	iamax_kernel_L999

	asrs	I, N, #2					// I = N / 4
	ble	iamax_kernel_S1

	.align 5

iamax_kernel_S4:

	KERNEL_S1
	KERNEL_S1
	KERNEL_S1
	KERNEL_S1

	subs	I, I, #1
	bne	iamax_kernel_S4

iamax_kernel_S1:

	ands	I, N, #3
	ble	iamax_kernel_L999

iamax_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     iamax_kernel_S10


iamax_kernel_L999:

	mov	r0, INDEX		// set return value

	pop     {r4}
	bx	lr

	EPILOGUE

