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
* 2013/11/19 Saar
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*
**************************************************************************************/

#define ASSEMBLER
#include "common.h"

#define STACKSIZE 256

#if !defined(__ARM_PCS_VFP)

#if !defined(DOUBLE)
#define OLD_ALPHA	r3
#define OLD_A_SOFTFP	[fp, #0 ]
#define OLD_LDA		[fp, #4 ]
#define X		[fp, #8 ]
#define OLD_INC_X	[fp, #12 ]
#define Y		[fp, #16 ]
#define OLD_INC_Y	[fp, #20 ]
#else
#define OLD_ALPHA	[fp, #0 ]
#define OLD_A_SOFTFP	[fp, #8 ]
#define OLD_LDA		[fp, #12]
#define X		[fp, #16]
#define OLD_INC_X	[fp, #20]
#define Y		[fp, #24]
#define OLD_INC_Y	[fp, #28]
#endif

#else

#define OLD_LDA		[fp, #0 ]
#define X		[fp, #4 ]
#define OLD_INC_X	[fp, #8 ]
#define Y		[fp, #12 ]
#define OLD_INC_Y	[fp, #16 ]

#endif

#define OLD_A		r3
#define	OLD_M		r0

#define AO1	r0
#define N	r1
#define J	r2

#define AO2	r4
#define XO	r5
#define YO	r6
#define LDA	r7
#define INC_X	r8
#define INC_Y	r9

#define I	r12

#define M	[fp, #-252 ]
#define A	[fp, #-256 ]

#define FP_ZERO [fp, #-228]
#define FP_ZERO_0 [fp, #-228]
#define FP_ZERO_1 [fp, #-224]


#define X_PRE	64
#define Y_PRE	0
#define A_PRE	0

/**************************************************************************************
* Macro definitions
**************************************************************************************/


#if	defined(DOUBLE)

.macro INIT_F8

	pld     [ YO , #Y_PRE ]
	pld     [ YO , #Y_PRE+32 ]

	fldd		d24 , FP_ZERO
	vmov.f64	d25 , d24
	vmov.f64	d26 , d24
	vmov.f64	d27 , d24
	vmov.f64	d28 , d24
	vmov.f64	d29 , d24
	vmov.f64	d30 , d24
	vmov.f64	d31 , d24

.endm

.macro KERNEL_F8X8

	pld     [ XO , #X_PRE ]
	KERNEL_F8X1
	KERNEL_F8X1
	KERNEL_F8X1
	KERNEL_F8X1

	pld     [ XO , #X_PRE ]
	KERNEL_F8X1
	KERNEL_F8X1
	KERNEL_F8X1
	KERNEL_F8X1

.endm


.macro KERNEL_F8X1

	vldmia.f64	XO! ,  { d4 }
	vldmia.f64	AO1 ,  { d8 - d15 }

	vmla.f64	d24 , d4 , d8
	pld	[ AO2 , #A_PRE ]
	vmla.f64	d25 , d4 , d9
	pld	[ AO2 , #A_PRE+32 ]
	vmla.f64	d26 , d4 , d10
	vmla.f64	d27 , d4 , d11
	vmla.f64	d28 , d4 , d12
	vmla.f64	d29 , d4 , d13
	add		AO1, AO1, LDA
	vmla.f64	d30 , d4 , d14
	add		AO2, AO2, LDA
	vmla.f64	d31 , d4 , d15

.endm

.macro	SAVE_F8

	vldmia.f64	YO,  { d16 - d23 }

	vmla.f64	d16, d0, d24
	vmla.f64	d17, d0, d25
	vmla.f64	d18, d0, d26
	vmla.f64	d19, d0, d27
	vmla.f64	d20, d0, d28
	vmla.f64	d21, d0, d29
	vmla.f64	d22, d0, d30
	vmla.f64	d23, d0, d31

	vstmia.f64	YO!, { d16 - d23 }

.endm


.macro INIT_F1

	fldd		d24 , FP_ZERO

.endm



.macro KERNEL_F1X1

	vldmia.f64	XO! ,  { d4 }
	vldmia.f64	AO1 ,  { d8 }
	vmla.f64	d24 , d4 , d8
	add		AO1, AO1, LDA

.endm

.macro	SAVE_F1

	vldmia.f64	YO,  { d16 }
	vmla.f64	d16, d0, d24
	vstmia.f64	YO!, { d16 }

.endm

/*********************************************************************************************/


.macro INIT_S8

	fldd		d24 , FP_ZERO
	vmov.f64	d25 , d24
	vmov.f64	d26 , d24
	vmov.f64	d27 , d24
	vmov.f64	d28 , d24
	vmov.f64	d29 , d24
	vmov.f64	d30 , d24
	vmov.f64	d31 , d24

.endm

.macro KERNEL_S8X8

	KERNEL_S8X1
	KERNEL_S8X1
	KERNEL_S8X1
	KERNEL_S8X1

	KERNEL_S8X1
	KERNEL_S8X1
	KERNEL_S8X1
	KERNEL_S8X1

.endm


.macro KERNEL_S8X1

	pld	[ AO2 , #A_PRE ]
	pld	[ AO2 , #A_PRE+32 ]
	vldmia.f64	XO ,  { d4 }
	vldmia.f64	AO1 ,  { d8 - d15 }

	vmla.f64	d24 , d4 , d8
	vmla.f64	d25 , d4 , d9
	vmla.f64	d26 , d4 , d10
	vmla.f64	d27 , d4 , d11
	vmla.f64	d28 , d4 , d12
	vmla.f64	d29 , d4 , d13
	vmla.f64	d30 , d4 , d14
	vmla.f64	d31 , d4 , d15
	add		AO1, AO1, LDA
	add		AO2, AO2, LDA
	add		XO, XO, INC_X

.endm

.macro	SAVE_S8

	vldmia.f64	YO,  { d16 }
	vmla.f64	d16, d0, d24
	vstmia.f64	YO,  { d16 }
	add	YO, YO, INC_Y

	vldmia.f64	YO,  { d17 }
	vmla.f64	d17, d0, d25
	vstmia.f64	YO,  { d17 }
	add	YO, YO, INC_Y

	vldmia.f64	YO,  { d18 }
	vmla.f64	d18, d0, d26
	vstmia.f64	YO,  { d18 }
	add	YO, YO, INC_Y

	vldmia.f64	YO,  { d19 }
	vmla.f64	d19, d0, d27
	vstmia.f64	YO,  { d19 }
	add	YO, YO, INC_Y

	vldmia.f64	YO,  { d20 }
	vmla.f64	d20, d0, d28
	vstmia.f64	YO,  { d20 }
	add	YO, YO, INC_Y

	vldmia.f64	YO,  { d21 }
	vmla.f64	d21, d0, d29
	vstmia.f64	YO,  { d21 }
	add	YO, YO, INC_Y

	vldmia.f64	YO,  { d22 }
	vmla.f64	d22, d0, d30
	vstmia.f64	YO,  { d22 }
	add	YO, YO, INC_Y

	vldmia.f64	YO,  { d23 }
	vmla.f64	d23, d0, d31
	vstmia.f64	YO,  { d23 }
	add	YO, YO, INC_Y

.endm


.macro INIT_S1

	fldd		d24 , FP_ZERO

.endm



.macro KERNEL_S1X1

	vldmia.f64	XO  ,  { d4 }
	vldmia.f64	AO1 ,  { d8 }
	vmla.f64	d24 , d4 , d8
	add		AO1, AO1, LDA
	add	XO, XO, INC_X

.endm

.macro	SAVE_S1

	vldmia.f64	YO,  { d16 }
	vmla.f64	d16, d0, d24
	vstmia.f64	YO,  { d16 }
	add	YO, YO, INC_Y

.endm



#else	/************************* SINGLE PRECISION *****************************************/

.macro INIT_F8

	pld     [ YO , #Y_PRE ]

	flds		s24 , FP_ZERO
	vmov.f32	s25 , s24
	vmov.f32	s26 , s24
	vmov.f32	s27 , s24
	vmov.f32	s28 , s24
	vmov.f32	s29 , s24
	vmov.f32	s30 , s24
	vmov.f32	s31 , s24

.endm

.macro KERNEL_F8X8

	pld     [ XO , #X_PRE ]
	KERNEL_F8X1
	KERNEL_F8X1
	KERNEL_F8X1
	KERNEL_F8X1

	KERNEL_F8X1
	KERNEL_F8X1
	KERNEL_F8X1
	KERNEL_F8X1

.endm


.macro KERNEL_F8X1

	pld	[ AO2 , #A_PRE ]
	vldmia.f32	XO! ,  { s4 }
	vldmia.f32	AO1 ,  { s8 - s15 }

	vmla.f32	s24 , s4 , s8
	vmla.f32	s25 , s4 , s9
	vmla.f32	s26 , s4 , s10
	vmla.f32	s27 , s4 , s11
	vmla.f32	s28 , s4 , s12
	vmla.f32	s29 , s4 , s13
	vmla.f32	s30 , s4 , s14
	vmla.f32	s31 , s4 , s15
	add		AO1, AO1, LDA
	add		AO2, AO2, LDA

.endm

.macro	SAVE_F8

	vldmia.f32	YO,  { s16 - s23 }

	vmla.f32	s16, s0, s24
	vmla.f32	s17, s0, s25
	vmla.f32	s18, s0, s26
	vmla.f32	s19, s0, s27
	vmla.f32	s20, s0, s28
	vmla.f32	s21, s0, s29
	vmla.f32	s22, s0, s30
	vmla.f32	s23, s0, s31

	vstmia.f32	YO!, { s16 - s23 }

.endm


.macro INIT_F1

	flds		s24 , FP_ZERO

.endm



.macro KERNEL_F1X1

	vldmia.f32	XO! ,  { s4 }
	vldmia.f32	AO1 ,  { s8 }
	vmla.f32	s24 , s4 , s8
	add		AO1, AO1, LDA

.endm

.macro	SAVE_F1

	vldmia.f32	YO,  { s16 }
	vmla.f32	s16, s0, s24
	vstmia.f32	YO!, { s16 }

.endm

/*********************************************************************************************/


.macro INIT_S8

	flds		s24 , FP_ZERO
	vmov.f32	s25 , s24
	vmov.f32	s26 , s24
	vmov.f32	s27 , s24
	vmov.f32	s28 , s24
	vmov.f32	s29 , s24
	vmov.f32	s30 , s24
	vmov.f32	s31 , s24

.endm

.macro KERNEL_S8X8

	KERNEL_S8X1
	KERNEL_S8X1
	KERNEL_S8X1
	KERNEL_S8X1

	KERNEL_S8X1
	KERNEL_S8X1
	KERNEL_S8X1
	KERNEL_S8X1

.endm


.macro KERNEL_S8X1

	pld	[ AO2 , #A_PRE ]
	vldmia.f32	XO ,  { s4 }
	vldmia.f32	AO1 ,  { s8 - s15 }

	vmla.f32	s24 , s4 , s8
	vmla.f32	s25 , s4 , s9
	vmla.f32	s26 , s4 , s10
	vmla.f32	s27 , s4 , s11
	vmla.f32	s28 , s4 , s12
	vmla.f32	s29 , s4 , s13
	vmla.f32	s30 , s4 , s14
	vmla.f32	s31 , s4 , s15
	add		AO1, AO1, LDA
	add		AO2, AO2, LDA
	add		XO, XO, INC_X

.endm

.macro	SAVE_S8

	vldmia.f32	YO,  { s16 }
	vmla.f32	s16, s0, s24
	vstmia.f32	YO,  { s16 }
	add	YO, YO, INC_Y

	vldmia.f32	YO,  { s17 }
	vmla.f32	s17, s0, s25
	vstmia.f32	YO,  { s17 }
	add	YO, YO, INC_Y

	vldmia.f32	YO,  { s18 }
	vmla.f32	s18, s0, s26
	vstmia.f32	YO,  { s18 }
	add	YO, YO, INC_Y

	vldmia.f32	YO,  { s19 }
	vmla.f32	s19, s0, s27
	vstmia.f32	YO,  { s19 }
	add	YO, YO, INC_Y

	vldmia.f32	YO,  { s20 }
	vmla.f32	s20, s0, s28
	vstmia.f32	YO,  { s20 }
	add	YO, YO, INC_Y

	vldmia.f32	YO,  { s21 }
	vmla.f32	s21, s0, s29
	vstmia.f32	YO,  { s21 }
	add	YO, YO, INC_Y

	vldmia.f32	YO,  { s22 }
	vmla.f32	s22, s0, s30
	vstmia.f32	YO,  { s22 }
	add	YO, YO, INC_Y

	vldmia.f32	YO,  { s23 }
	vmla.f32	s23, s0, s31
	vstmia.f32	YO,  { s23 }
	add	YO, YO, INC_Y

.endm


.macro INIT_S1

	flds		s24 , FP_ZERO

.endm



.macro KERNEL_S1X1

	vldmia.f32	XO  ,  { s4 }
	vldmia.f32	AO1 ,  { s8 }
	vmla.f32	s24 , s4 , s8
	add		AO1, AO1, LDA
	add	XO, XO, INC_X

.endm

.macro	SAVE_S1

	vldmia.f32	YO,  { s16 }
	vmla.f32	s16, s0, s24
	vstmia.f32	YO,  { s16 }
	add	YO, YO, INC_Y

.endm




#endif

/**************************************************************************************
* End of macro definitions
**************************************************************************************/

	PROLOGUE

	.align 5
	push    {r4 - r9 , fp}
        add     fp, sp, #28
	sub     sp, sp, #STACKSIZE                              // reserve stack

        sub     r12, fp, #192

#if	defined(DOUBLE)
        vstm    r12, { d8 - d15 }                                 // store floating point registers
#else
        vstm    r12, { s8 - s31 }                                 // store floating point registers
#endif

        movs    r12, #0
        str     r12, FP_ZERO
        str     r12, FP_ZERO_1

	cmp	OLD_M, #0
	ble	gemvn_kernel_L999

	cmp	N, #0
	ble	gemvn_kernel_L999

#if !defined(__ARM_PCS_VFP)
#if !defined(DOUBLE)
	vmov	s0, OLD_ALPHA
#else
	vldr	d0, OLD_ALPHA
#endif
	ldr	OLD_A, OLD_A_SOFTFP
#endif

	str	OLD_A, A
	str	OLD_M, M

	ldr    INC_X , OLD_INC_X
	ldr    INC_Y , OLD_INC_Y

	cmp	INC_X, #0
	beq	gemvn_kernel_L999

	cmp	INC_Y, #0
	beq	gemvn_kernel_L999

	ldr	LDA, OLD_LDA


#if defined(DOUBLE)
	lsl	LDA, LDA, #3				// LDA * SIZE
#else
	lsl	LDA, LDA, #2				// LDA * SIZE
#endif

	cmp	INC_X, #1
	bne	gemvn_kernel_S8_BEGIN

	cmp	INC_Y, #1
	bne	gemvn_kernel_S8_BEGIN


gemvn_kernel_F8_BEGIN:

	ldr	YO , Y

	ldr	I, M
	asrs	I, I, #3					// I = M / 8
	ble	gemvn_kernel_F1_BEGIN

gemvn_kernel_F8X8:

	ldr	AO1, A
	add	AO2, AO1, LDA
	add	r3 , AO1, #8*SIZE
	str	r3 , A

	ldr	XO , X

	INIT_F8

	asrs	J, N, #3					// J = N / 8
	ble	gemvn_kernel_F8X1


gemvn_kernel_F8X8_10:

	KERNEL_F8X8

	subs	J, J, #1
	bne	gemvn_kernel_F8X8_10


gemvn_kernel_F8X1:

	ands	J, N , #7
	ble	gemvn_kernel_F8_END

gemvn_kernel_F8X1_10:

	KERNEL_F8X1

	subs	J, J, #1
	bne	gemvn_kernel_F8X1_10


gemvn_kernel_F8_END:

	SAVE_F8

	subs	I , I , #1
	bne	gemvn_kernel_F8X8


gemvn_kernel_F1_BEGIN:

	ldr	I, M
	ands	I,  I , #7
	ble	gemvn_kernel_L999

gemvn_kernel_F1X1:

	ldr	AO1, A
	add	r3, AO1, #SIZE
	str	r3, A

	ldr	XO , X

	INIT_F1

	mov	J, N


gemvn_kernel_F1X1_10:

	KERNEL_F1X1

	subs	J, J, #1
	bne	gemvn_kernel_F1X1_10


gemvn_kernel_F1_END:

	SAVE_F1

	subs	I , I , #1
	bne	gemvn_kernel_F1X1

	b	gemvn_kernel_L999



/*************************************************************************************************************/

gemvn_kernel_S8_BEGIN:

#if defined(DOUBLE)
	lsl	INC_X, INC_X, #3				// INC_X * SIZE
	lsl	INC_Y, INC_Y, #3				// INC_Y * SIZE
#else
	lsl	INC_X, INC_X, #2				// INC_X * SIZE
	lsl	INC_Y, INC_Y, #2				// INC_Y * SIZE
#endif

	ldr	YO , Y

	ldr	I, M
	asrs	I, I, #3					// I = M / 8
	ble	gemvn_kernel_S1_BEGIN

gemvn_kernel_S8X8:

	ldr	AO1, A
	add	AO2, AO1, LDA
	add	r3 , AO1, #8*SIZE
	str	r3 , A

	ldr	XO , X

	INIT_S8

	asrs	J, N, #3					// J = N / 8
	ble	gemvn_kernel_S8X1


gemvn_kernel_S8X8_10:

	KERNEL_S8X8

	subs	J, J, #1
	bne	gemvn_kernel_S8X8_10


gemvn_kernel_S8X1:

	ands	J, N , #7
	ble	gemvn_kernel_S8_END

gemvn_kernel_S8X1_10:

	KERNEL_S8X1

	subs	J, J, #1
	bne	gemvn_kernel_S8X1_10


gemvn_kernel_S8_END:

	SAVE_S8

	subs	I , I , #1
	bne	gemvn_kernel_S8X8


gemvn_kernel_S1_BEGIN:

	ldr	I, M
	ands	I,  I , #7
	ble	gemvn_kernel_L999

gemvn_kernel_S1X1:

	ldr	AO1, A
	add	r3, AO1, #SIZE
	str	r3, A

	ldr	XO , X

	INIT_S1

	mov	J, N


gemvn_kernel_S1X1_10:

	KERNEL_S1X1

	subs	J, J, #1
	bne	gemvn_kernel_S1X1_10


gemvn_kernel_S1_END:

	SAVE_S1

	subs	I , I , #1
	bne	gemvn_kernel_S1X1


/*************************************************************************************************************/

gemvn_kernel_L999:

        sub     r3, fp, #192

#if	defined(DOUBLE)
        vldm    r3, { d8 - d15 }                                 // restore floating point registers
#else
        vldm    r3, { s8 - s31 }                                 // restore floating point registers
#endif

	mov	r0, #0		// set return value

	sub     sp, fp, #28
	pop     {r4 -r9 ,fp}
	bx	lr

	EPILOGUE

