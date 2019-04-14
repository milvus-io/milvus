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
* 2013/11/22 Saar
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


.macro KERNEL_F1

	vldmia.f64	X!, 	{ d4 }
	vcmpe.f64	d4, d6				// compare with 0.0
	vmrs		APSR_nzcv, fpscr
	beq		KERNEL_F1_NEXT_\@
	vabs.f64   	d4,  d4
	vcmpe.f64  	d0,  d4				// compare with scale
	vmrs		APSR_nzcv, fpscr
	vdivge.f64	d2 , d4, d0			// scale >= x ?	x / scale
	vmlage.f64	d1 , d2 , d2			// ssq += ( x/scale ) * ( x/scale )
	bge		KERNEL_F1_NEXT_\@
	vdiv.f64	d2 , d0, d4			// scale / x
	vmul.f64	d2 , d2, d2			// ( scale / x ) * ( scale / x )
	vmul.f64	d3 , d1, d2			// ssq * ( scale / x ) * ( scale / x )
	vadd.f64	d1 , d3, d7			// ssq = 1 + ssq * ( scale / x ) * ( scale / x )
	vmov.f64	d0 , d4				// scale = x

KERNEL_F1_NEXT_\@:

.endm

.macro KERNEL_F8

	pld	[ X, #X_PRE ]
	KERNEL_F1
	KERNEL_F1
	KERNEL_F1
	KERNEL_F1
	pld	[ X, #X_PRE ]
	KERNEL_F1
	KERNEL_F1
	KERNEL_F1
	KERNEL_F1

.endm

.macro KERNEL_S1

	vldmia.f64	X, 	{ d4 }
	vcmpe.f64	d4, d6				// compare with 0.0
	vmrs		APSR_nzcv, fpscr
	beq		KERNEL_S1_NEXT
	vabs.f64   	d4,  d4
	vcmpe.f64  	d0,  d4				// compare with scale
	vmrs		APSR_nzcv, fpscr
	vdivge.f64	d2 , d4, d0			// scale >= x ?	x / scale
	vmlage.f64	d1 , d2 , d2			// ssq += ( x/scale ) * ( x/scale )
	bge		KERNEL_S1_NEXT
	vdiv.f64	d2 , d0, d4			// scale / x
	vmul.f64	d2 , d2, d2			// ( scale / x ) * ( scale / x )
	vmul.f64	d3 , d1, d2			// ssq * ( scale / x ) * ( scale / x )
	vadd.f64	d1 , d3, d7			// ssq = 1 + ssq * ( scale / x ) * ( scale / x )
	vmov.f64	d0 , d4				// scale = x

KERNEL_S1_NEXT:

	add	X, X, INC_X

.endm

#else

.macro KERNEL_F1

	vldmia.f32	X!, 	{ s4 }
	vcmpe.f32	s4, s6				// compare with 0.0
	vmrs		APSR_nzcv, fpscr
	beq		KERNEL_F1_NEXT_\@
	vabs.f32   	s4,  s4
	vcmpe.f32  	s0,  s4				// compare with scale
	vmrs		APSR_nzcv, fpscr
	vdivge.f32	s2 , s4, s0			// scale >= x ?	x / scale
	vmlage.f32	s1 , s2 , s2			// ssq += ( x/scale ) * ( x/scale )
	bge		KERNEL_F1_NEXT_\@
	vdiv.f32	s2 , s0, s4			// scale / x
	vmul.f32	s2 , s2, s2			// ( scale / x ) * ( scale / x )
	vmul.f32	s3 , s1, s2			// ssq * ( scale / x ) * ( scale / x )
	vadd.f32	s1 , s3, s7			// ssq = 1 + ssq * ( scale / x ) * ( scale / x )
	vmov.f32	s0 , s4				// scale = x

KERNEL_F1_NEXT_\@:

.endm

.macro KERNEL_F8

	pld	[ X, #X_PRE ]
	KERNEL_F1
	KERNEL_F1
	KERNEL_F1
	KERNEL_F1

	KERNEL_F1
	KERNEL_F1
	KERNEL_F1
	KERNEL_F1

.endm

.macro KERNEL_S1

	vldmia.f32	X, 	{ s4 }
	vcmpe.f32	s4, s6				// compare with 0.0
	vmrs		APSR_nzcv, fpscr
	beq		KERNEL_S1_NEXT
	vabs.f32   	s4,  s4
	vcmpe.f32  	s0,  s4				// compare with scale
	vmrs		APSR_nzcv, fpscr
	vdivge.f32	s2 , s4, s0			// scale >= x ?	x / scale
	vmlage.f32	s1 , s2 , s2			// ssq += ( x/scale ) * ( x/scale )
	bge		KERNEL_S1_NEXT
	vdiv.f32	s2 , s0, s4			// scale / x
	vmul.f32	s2 , s2, s2			// ( scale / x ) * ( scale / x )
	vmul.f32	s3 , s1, s2			// ssq * ( scale / x ) * ( scale / x )
	vadd.f32	s1 , s3, s7			// ssq = 1 + ssq * ( scale / x ) * ( scale / x )
	vmov.f32	s0 , s4				// scale = x

KERNEL_S1_NEXT:

	add	X, X, INC_X

.endm




#endif

#else

#if	defined(DOUBLE)

.macro KERNEL_F1

	vldmia.f64	X!, 	{ d4 - d5 }

	vcmpe.f64	d4, d6				// compare with 0.0
	vmrs		APSR_nzcv, fpscr
	beq		KERNEL_F1_NEXT_\@
	vabs.f64   	d4,  d4
	vcmpe.f64  	d0,  d4				// compare with scale
	vmrs		APSR_nzcv, fpscr
	vdivge.f64	d2 , d4, d0			// scale >= x ?	x / scale
	vmlage.f64	d1 , d2 , d2			// ssq += ( x/scale ) * ( x/scale )
	bge		KERNEL_F1_NEXT_\@
	vdiv.f64	d2 , d0, d4			// scale / x
	vmul.f64	d2 , d2, d2			// ( scale / x ) * ( scale / x )
	vmul.f64	d3 , d1, d2			// ssq * ( scale / x ) * ( scale / x )
	vadd.f64	d1 , d3, d7			// ssq = 1 + ssq * ( scale / x ) * ( scale / x )
	vmov.f64	d0 , d4				// scale = x

KERNEL_F1_NEXT_\@:

	vcmpe.f64	d5, d6				// compare with 0.0
	vmrs		APSR_nzcv, fpscr
	beq		KERNEL_F1_END_\@
	vabs.f64   	d5,  d5
	vcmpe.f64  	d0,  d5				// compare with scale
	vmrs		APSR_nzcv, fpscr
	vdivge.f64	d2 , d5, d0			// scale >= x ?	x / scale
	vmlage.f64	d1 , d2 , d2			// ssq += ( x/scale ) * ( x/scale )
	bge		KERNEL_F1_END_\@
	vdiv.f64	d2 , d0, d5			// scale / x
	vmul.f64	d2 , d2, d2			// ( scale / x ) * ( scale / x )
	vmul.f64	d3 , d1, d2			// ssq * ( scale / x ) * ( scale / x )
	vadd.f64	d1 , d3, d7			// ssq = 1 + ssq * ( scale / x ) * ( scale / x )
	vmov.f64	d0 , d5				// scale = x

KERNEL_F1_END_\@:


.endm

.macro KERNEL_F8

	pld	[ X, #X_PRE ]
	KERNEL_F1
	KERNEL_F1
	pld	[ X, #X_PRE ]
	KERNEL_F1
	KERNEL_F1
	pld	[ X, #X_PRE ]
	KERNEL_F1
	KERNEL_F1
	pld	[ X, #X_PRE ]
	KERNEL_F1
	KERNEL_F1

.endm

.macro KERNEL_S1

	vldmia.f64	X, 	{ d4 - d5 }

	vcmpe.f64	d4, d6				// compare with 0.0
	vmrs		APSR_nzcv, fpscr
	beq		KERNEL_S1_NEXT_\@
	vabs.f64   	d4,  d4
	vcmpe.f64  	d0,  d4				// compare with scale
	vmrs		APSR_nzcv, fpscr
	vdivge.f64	d2 , d4, d0			// scale >= x ?	x / scale
	vmlage.f64	d1 , d2 , d2			// ssq += ( x/scale ) * ( x/scale )
	bge		KERNEL_S1_NEXT_\@
	vdiv.f64	d2 , d0, d4			// scale / x
	vmul.f64	d2 , d2, d2			// ( scale / x ) * ( scale / x )
	vmul.f64	d3 , d1, d2			// ssq * ( scale / x ) * ( scale / x )
	vadd.f64	d1 , d3, d7			// ssq = 1 + ssq * ( scale / x ) * ( scale / x )
	vmov.f64	d0 , d4				// scale = x

KERNEL_S1_NEXT_\@:

	vcmpe.f64	d5, d6				// compare with 0.0
	vmrs		APSR_nzcv, fpscr
	beq		KERNEL_S1_END_\@
	vabs.f64   	d5,  d5
	vcmpe.f64  	d0,  d5				// compare with scale
	vmrs		APSR_nzcv, fpscr
	vdivge.f64	d2 , d5, d0			// scale >= x ?	x / scale
	vmlage.f64	d1 , d2 , d2			// ssq += ( x/scale ) * ( x/scale )
	bge		KERNEL_S1_END_\@
	vdiv.f64	d2 , d0, d5			// scale / x
	vmul.f64	d2 , d2, d2			// ( scale / x ) * ( scale / x )
	vmul.f64	d3 , d1, d2			// ssq * ( scale / x ) * ( scale / x )
	vadd.f64	d1 , d3, d7			// ssq = 1 + ssq * ( scale / x ) * ( scale / x )
	vmov.f64	d0 , d5				// scale = x

KERNEL_S1_END_\@:

	add	X, X, INC_X

.endm


#else

.macro KERNEL_F1

	vldmia.f32	X!, 	{ s4 - s5 }

	vcmpe.f32	s4, s6				// compare with 0.0
	vmrs		APSR_nzcv, fpscr
	beq		KERNEL_F1_NEXT_\@
	vabs.f32   	s4,  s4
	vcmpe.f32  	s0,  s4				// compare with scale
	vmrs		APSR_nzcv, fpscr
	vdivge.f32	s2 , s4, s0			// scale >= x ?	x / scale
	vmlage.f32	s1 , s2 , s2			// ssq += ( x/scale ) * ( x/scale )
	bge		KERNEL_F1_NEXT_\@
	vdiv.f32	s2 , s0, s4			// scale / x
	vmul.f32	s2 , s2, s2			// ( scale / x ) * ( scale / x )
	vmul.f32	s3 , s1, s2			// ssq * ( scale / x ) * ( scale / x )
	vadd.f32	s1 , s3, s7			// ssq = 1 + ssq * ( scale / x ) * ( scale / x )
	vmov.f32	s0 , s4				// scale = x

KERNEL_F1_NEXT_\@:

	vcmpe.f32	s5, s6				// compare with 0.0
	vmrs		APSR_nzcv, fpscr
	beq		KERNEL_F1_END_\@
	vabs.f32   	s5,  s5
	vcmpe.f32  	s0,  s5				// compare with scale
	vmrs		APSR_nzcv, fpscr
	vdivge.f32	s2 , s5, s0			// scale >= x ?	x / scale
	vmlage.f32	s1 , s2 , s2			// ssq += ( x/scale ) * ( x/scale )
	bge		KERNEL_F1_END_\@
	vdiv.f32	s2 , s0, s5			// scale / x
	vmul.f32	s2 , s2, s2			// ( scale / x ) * ( scale / x )
	vmul.f32	s3 , s1, s2			// ssq * ( scale / x ) * ( scale / x )
	vadd.f32	s1 , s3, s7			// ssq = 1 + ssq * ( scale / x ) * ( scale / x )
	vmov.f32	s0 , s5				// scale = x

KERNEL_F1_END_\@:


.endm

.macro KERNEL_F8

	pld	[ X, #X_PRE ]
	KERNEL_F1
	KERNEL_F1
	KERNEL_F1
	KERNEL_F1
	pld	[ X, #X_PRE ]
	KERNEL_F1
	KERNEL_F1
	KERNEL_F1
	KERNEL_F1

.endm

.macro KERNEL_S1

	vldmia.f32	X, 	{ s4 - s5 }

	vcmpe.f32	s4, s6				// compare with 0.0
	vmrs		APSR_nzcv, fpscr
	beq		KERNEL_S1_NEXT_\@
	vabs.f32   	s4,  s4
	vcmpe.f32  	s0,  s4				// compare with scale
	vmrs		APSR_nzcv, fpscr
	vdivge.f32	s2 , s4, s0			// scale >= x ?	x / scale
	vmlage.f32	s1 , s2 , s2			// ssq += ( x/scale ) * ( x/scale )
	bge		KERNEL_S1_NEXT_\@
	vdiv.f32	s2 , s0, s4			// scale / x
	vmul.f32	s2 , s2, s2			// ( scale / x ) * ( scale / x )
	vmul.f32	s3 , s1, s2			// ssq * ( scale / x ) * ( scale / x )
	vadd.f32	s1 , s3, s7			// ssq = 1 + ssq * ( scale / x ) * ( scale / x )
	vmov.f32	s0 , s4				// scale = x

KERNEL_S1_NEXT_\@:

	vcmpe.f32	s5, s6				// compare with 0.0
	vmrs		APSR_nzcv, fpscr
	beq		KERNEL_S1_END_\@
	vabs.f32   	s5,  s5
	vcmpe.f32  	s0,  s5				// compare with scale
	vmrs		APSR_nzcv, fpscr
	vdivge.f32	s2 , s5, s0			// scale >= x ?	x / scale
	vmlage.f32	s1 , s2 , s2			// ssq += ( x/scale ) * ( x/scale )
	bge		KERNEL_S1_END_\@
	vdiv.f32	s2 , s0, s5			// scale / x
	vmul.f32	s2 , s2, s2			// ( scale / x ) * ( scale / x )
	vmul.f32	s3 , s1, s2			// ssq * ( scale / x ) * ( scale / x )
	vadd.f32	s1 , s3, s7			// ssq = 1 + ssq * ( scale / x ) * ( scale / x )
	vmov.f32	s0 , s5				// scale = x

KERNEL_S1_END_\@:

	add	X, X, INC_X

.endm



#endif

#endif

/**************************************************************************************
* End of macro definitions
**************************************************************************************/

	PROLOGUE

	b nrm2_begin


#if	defined(COMPLEX)

#if	defined(DOUBLE)

znrm2_zero:
	.word	0x00000000
	.word	0x00000000


znrm2_one:
	.word	0x00000000
	.word	0x3ff00000

#else

cnrm2_zero:
	.word	0x00000000

cnrm2_one:
	.word	0x3f800000

#endif

#else

#if	defined(DOUBLE)

dnrm2_zero:
	.word	0x00000000
	.word	0x00000000


dnrm2_one:
	.word	0x00000000
	.word	0x3ff00000

#else

snrm2_zero:
	.word	0x00000000

snrm2_one:
	.word	0x3f800000

#endif

#endif


	.align 5


nrm2_begin:

#if defined(COMPLEX)

#if defined(DOUBLE)
	vldr.64			d0 , znrm2_zero	
	vldr.64			d1 , znrm2_one		// ssq=1.0
	vmov.f64		d7 , d1			// value 1.0
	vmov.f64		d6 , d0			// value 0.0
#else
	vldr.32			s0 , cnrm2_zero
	vldr.32			s1 , cnrm2_one		// ssq=1.0
	vmov.f32		s7 , s1			// value 1.0
	vmov.f32		s6 , s0			// value 0.0
#endif

#else

#if defined(DOUBLE)
	vldr.64			d0 , dnrm2_zero	
	vldr.64			d1 , dnrm2_one		// ssq=1.0
	vmov.f64		d7 , d1			// value 1.0
	vmov.f64		d6 , d0			// value 0.0
#else
	vldr.32			s0 , snrm2_zero
	vldr.32			s1 , snrm2_one		// ssq=1.0
	vmov.f32		s7 , s1			// value 1.0
	vmov.f32		s6 , s0			// value 0.0
#endif


#endif


	cmp	N, #0
	ble	nrm2_kernel_L999

	cmp	INC_X, #0
	beq	nrm2_kernel_L999


	cmp	INC_X, #1
	bne	nrm2_kernel_S_BEGIN


nrm2_kernel_F_BEGIN:

	asrs	I, N, #3				// I = N / 8
	ble	nrm2_kernel_F1

nrm2_kernel_F8:

	KERNEL_F8

	subs    I, I, #1
        bne     nrm2_kernel_F8

nrm2_kernel_F1:

	ands    I, N, #7
        ble     nrm2_kernel_L999


nrm2_kernel_F10:

	KERNEL_F1

	subs    I, I, #1
        bne     nrm2_kernel_F10

	b	nrm2_kernel_L999

nrm2_kernel_S_BEGIN:

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



nrm2_kernel_S1:

	mov	I, N

	.align 5

nrm2_kernel_S10:

	KERNEL_S1

	subs    I, I, #1
        bne     nrm2_kernel_S10


nrm2_kernel_L999:

#if defined(DOUBLE)
	vsqrt.f64	d1, d1
	vmul.f64	d0, d0, d1
#else
	vsqrt.f32	s1, s1
	vmul.f32	s0, s0, s1
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

