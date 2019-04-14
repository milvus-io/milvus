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
* 2013/11/18 Saar
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
#define	OLD_N		r1

#define M	r0
#define AO1	r1
#define J	r2

#define AO2	r4
#define XO	r5
#define YO	r6
#define LDA	r7
#define INC_X	r8
#define INC_Y	r9

#define I	r12

#define N	[fp, #-252 ]
#define A	[fp, #-256 ]


#define X_PRE	512
#define A_PRE	512

/**************************************************************************************
* Macro definitions
**************************************************************************************/


#if	defined(DOUBLE)

.macro INIT_F2

	vsub.f64	d4 , d4 , d4
	vsub.f64	d5 , d5 , d5

.endm

.macro KERNEL_F2X4

	pld	[ XO  , #X_PRE ]
	vldmia.f64	XO! ,  { d28 - d31 }
	pld	[ AO1 , #A_PRE ]
	vldmia.f64	AO1!,  { d8 - d9   }
	pld	[ AO2 , #A_PRE ]
	vldmia.f64	AO2!,  { d16 - d17 }
	vmla.f64	d4 , d28 , d8
	vmla.f64	d5 , d28 , d16
	vldmia.f64	AO1!,  { d10 - d11 }
	vmla.f64	d4 , d29 , d9
	vmla.f64	d5 , d29 , d17
	vldmia.f64	AO2!,  { d18 - d19 }
	vmla.f64	d4 , d30, d10
	vmla.f64	d5 , d30, d18
	vmla.f64	d4 , d31, d11
	vmla.f64	d5 , d31, d19

.endm


.macro KERNEL_F2X1

	vldmia.f64	XO! ,  { d2 }
	vldmia.f64	AO1!,  { d8 }
	vldmia.f64	AO2!,  { d16 }
	vmla.f64	d4 , d2 , d8
	vmla.f64	d5 , d2 , d16

.endm

.macro	SAVE_F2

	vldmia.f64	YO,  { d24 - d25 }
	vmla.f64	d24, d0, d4
	vmla.f64	d25, d0, d5
	vstmia.f64	YO!, { d24 - d25 }

.endm

.macro INIT_S2

	vsub.f64	d4 , d4 , d4
	vsub.f64	d5 , d5 , d5

.endm

.macro KERNEL_S2X4

	pld	[ AO1 , #A_PRE ]
	vldmia.f64	XO ,  { d28 }
	add	XO, XO, INC_X
	vldmia.f64	AO1!,  { d8 - d9   }
	pld	[ AO2 , #A_PRE ]
	vldmia.f64	AO2!,  { d16 - d17 }
	vmla.f64	d4 , d28 , d8
	vldmia.f64	XO ,  { d29 }
	add	XO, XO, INC_X
	vmla.f64	d5 , d28 , d16
	vldmia.f64	AO1!,  { d10 - d11 }
	vmla.f64	d4 , d29 , d9
	vldmia.f64	XO ,  { d30 }
	add	XO, XO, INC_X
	vmla.f64	d5 , d29 , d17
	vldmia.f64	AO2!,  { d18 - d19 }
	vmla.f64	d4 , d30, d10
	vldmia.f64	XO ,  { d31 }
	add	XO, XO, INC_X
	vmla.f64	d5 , d30, d18
	vmla.f64	d4 , d31, d11
	vmla.f64	d5 , d31, d19

.endm


.macro KERNEL_S2X1

	vldmia.f64	XO ,  { d2 }
	vldmia.f64	AO1!,  { d8 }
	add	XO, XO, INC_X
	vldmia.f64	AO2!,  { d16 }
	vmla.f64	d4 , d2 , d8
	vmla.f64	d5 , d2 , d16

.endm

.macro	SAVE_S2

	vldmia.f64	YO,  { d24 }
	vmla.f64	d24, d0, d4
	vstmia.f64	YO,  { d24 }
	add	YO, YO, INC_Y

	vldmia.f64	YO,  { d24 }
	vmla.f64	d24, d0, d5
	vstmia.f64	YO,  { d24 }
	add	YO, YO, INC_Y

.endm

.macro INIT_F1

	vsub.f64	d4 , d4 , d4

.endm

.macro KERNEL_F1X4

	pld	[ XO  , #X_PRE ]
	vldmia.f64	XO! ,  { d28 - d31 }
	pld	[ AO1 , #A_PRE ]
	vldmia.f64	AO1!,  { d8 - d9   }
	vmla.f64	d4 , d28 , d8
	vldmia.f64	AO1!,  { d10 - d11 }
	vmla.f64	d4 , d29 , d9
	vmla.f64	d4 , d30, d10
	vmla.f64	d4 , d31, d11

.endm


.macro KERNEL_F1X1

	vldmia.f64	XO! ,  { d2 }
	vldmia.f64	AO1!,  { d8 }
	vmla.f64	d4 , d2 , d8

.endm

.macro	SAVE_F1

	vldmia.f64	YO,  { d24 }
	vmla.f64	d24, d0, d4
	vstmia.f64	YO!, { d24 }

.endm

.macro INIT_S1

	vsub.f64	d4 , d4 , d4

.endm

.macro KERNEL_S1X4

	pld	[ AO1 , #A_PRE ]
	vldmia.f64	XO ,  { d28 }
	add	XO, XO, INC_X
	vldmia.f64	AO1!,  { d8 - d9   }
	vmla.f64	d4 , d28 , d8
	vldmia.f64	XO ,  { d29 }
	add	XO, XO, INC_X
	vldmia.f64	AO1!,  { d10 - d11 }
	vmla.f64	d4 , d29 , d9
	vldmia.f64	XO ,  { d30 }
	add	XO, XO, INC_X
	vmla.f64	d4 , d30, d10
	vldmia.f64	XO ,  { d31 }
	add	XO, XO, INC_X
	vmla.f64	d4 , d31, d11

.endm


.macro KERNEL_S1X1

	vldmia.f64	XO ,  { d2 }
	vldmia.f64	AO1!,  { d8 }
	add	XO, XO, INC_X
	vmla.f64	d4 , d2 , d8

.endm

.macro	SAVE_S1

	vldmia.f64	YO,  { d24 }
	vmla.f64	d24, d0, d4
	vstmia.f64	YO,  { d24 }
	add	YO, YO, INC_Y

.endm


#else	/************************* SINGLE PRECISION *****************************************/

.macro INIT_F2

	vsub.f32	s4 , s4 , s4
	vsub.f32	s5 , s5 , s5

.endm

.macro KERNEL_F2X4

	vldmia.f32	XO! ,  { s28 - s31 }
	vldmia.f32	AO1!,  { s8 - s9   }
	vldmia.f32	AO2!,  { s16 - s17 }
	vmla.f32	s4 , s28 , s8
	vmla.f32	s5 , s28 , s16
	vldmia.f32	AO1!,  { s10 - s11 }
	vmla.f32	s4 , s29 , s9
	vmla.f32	s5 , s29 , s17
	vldmia.f32	AO2!,  { s18 - s19 }
	vmla.f32	s4 , s30, s10
	vmla.f32	s5 , s30, s18
	vmla.f32	s4 , s31, s11
	vmla.f32	s5 , s31, s19

.endm


.macro KERNEL_F2X1

	vldmia.f32	XO! ,  { s2 }
	vldmia.f32	AO1!,  { s8 }
	vldmia.f32	AO2!,  { s16 }
	vmla.f32	s4 , s2 , s8
	vmla.f32	s5 , s2 , s16

.endm

.macro	SAVE_F2

	vldmia.f32	YO,  { s24 - s25 }
	vmla.f32	s24, s0, s4
	vmla.f32	s25, s0, s5
	vstmia.f32	YO!, { s24 - s25 }

.endm

.macro INIT_S2

	vsub.f32	s4 , s4 , s4
	vsub.f32	s5 , s5 , s5

.endm

.macro KERNEL_S2X4

	vldmia.f32	XO ,  { s28 }
	add	XO, XO, INC_X
	vldmia.f32	AO1!,  { s8 - s9   }
	vldmia.f32	AO2!,  { s16 - s17 }
	vmla.f32	s4 , s28 , s8
	vldmia.f32	XO ,  { s29 }
	add	XO, XO, INC_X
	vmla.f32	s5 , s28 , s16
	vldmia.f32	AO1!,  { s10 - s11 }
	vmla.f32	s4 , s29 , s9
	vldmia.f32	XO ,  { s30 }
	add	XO, XO, INC_X
	vmla.f32	s5 , s29 , s17
	vldmia.f32	AO2!,  { s18 - s19 }
	vmla.f32	s4 , s30, s10
	vldmia.f32	XO ,  { s31 }
	add	XO, XO, INC_X
	vmla.f32	s5 , s30, s18
	vmla.f32	s4 , s31, s11
	vmla.f32	s5 , s31, s19

.endm


.macro KERNEL_S2X1

	vldmia.f32	XO ,  { s2 }
	vldmia.f32	AO1!,  { s8 }
	add	XO, XO, INC_X
	vldmia.f32	AO2!,  { s16 }
	vmla.f32	s4 , s2 , s8
	vmla.f32	s5 , s2 , s16

.endm

.macro	SAVE_S2

	vldmia.f32	YO,  { s24 }
	vmla.f32	s24, s0, s4
	vstmia.f32	YO,  { s24 }
	add	YO, YO, INC_Y

	vldmia.f32	YO,  { s24 }
	vmla.f32	s24, s0, s5
	vstmia.f32	YO,  { s24 }
	add	YO, YO, INC_Y

.endm

.macro INIT_F1

	vsub.f32	s4 , s4 , s4

.endm

.macro KERNEL_F1X4

	vldmia.f32	XO! ,  { s28 - s31 }
	vldmia.f32	AO1!,  { s8 - s9   }
	vmla.f32	s4 , s28 , s8
	vldmia.f32	AO1!,  { s10 - s11 }
	vmla.f32	s4 , s29 , s9
	vmla.f32	s4 , s30, s10
	vmla.f32	s4 , s31, s11

.endm


.macro KERNEL_F1X1

	vldmia.f32	XO! ,  { s2 }
	vldmia.f32	AO1!,  { s8 }
	vmla.f32	s4 , s2 , s8

.endm

.macro	SAVE_F1

	vldmia.f32	YO,  { s24 }
	vmla.f32	s24, s0, s4
	vstmia.f32	YO!, { s24 }

.endm

.macro INIT_S1

	vsub.f32	s4 , s4 , s4

.endm

.macro KERNEL_S1X4

	vldmia.f32	XO ,  { s28 }
	add	XO, XO, INC_X
	vldmia.f32	AO1!,  { s8 - s9   }
	vmla.f32	s4 , s28 , s8
	vldmia.f32	XO ,  { s29 }
	add	XO, XO, INC_X
	vldmia.f32	AO1!,  { s10 - s11 }
	vmla.f32	s4 , s29 , s9
	vldmia.f32	XO ,  { s30 }
	add	XO, XO, INC_X
	vmla.f32	s4 , s30, s10
	vldmia.f32	XO ,  { s31 }
	add	XO, XO, INC_X
	vmla.f32	s4 , s31, s11

.endm


.macro KERNEL_S1X1

	vldmia.f32	XO ,  { s2 }
	vldmia.f32	AO1!,  { s8 }
	add	XO, XO, INC_X
	vmla.f32	s4 , s2 , s8

.endm

.macro	SAVE_S1

	vldmia.f32	YO,  { s24 }
	vmla.f32	s24, s0, s4
	vstmia.f32	YO,  { s24 }
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

	cmp	M, #0
	ble	gemvt_kernel_L999

	cmp	OLD_N, #0
	ble	gemvt_kernel_L999

#if !defined(__ARM_PCS_VFP)
#if !defined(DOUBLE)
	vmov	s0, OLD_ALPHA
#else
	vldr	d0, OLD_ALPHA
#endif
	ldr	OLD_A, OLD_A_SOFTFP
#endif

	str	OLD_A, A
	str	OLD_N, N

	ldr    INC_X , OLD_INC_X
	ldr    INC_Y , OLD_INC_Y

	cmp	INC_X, #0
	beq	gemvt_kernel_L999

	cmp	INC_Y, #0
	beq	gemvt_kernel_L999

	ldr	LDA, OLD_LDA


#if defined(DOUBLE)
	lsl	LDA, LDA, #3				// LDA * SIZE
#else
	lsl	LDA, LDA, #2				// LDA * SIZE
#endif

	cmp	INC_X, #1
	bne	gemvt_kernel_S2_BEGIN

	cmp	INC_Y, #1
	bne	gemvt_kernel_S2_BEGIN


gemvt_kernel_F2_BEGIN:

	ldr	YO , Y

	ldr	J, N
	asrs	J, J, #1					// J = N / 2
	ble	gemvt_kernel_F1_BEGIN

gemvt_kernel_F2X4:

	ldr	AO1, A
	add	AO2, AO1, LDA
	add	r3 , AO2, LDA
	str	r3 , A

	ldr	XO , X

	INIT_F2

	asrs	I, M, #2					// I = M / 4
	ble	gemvt_kernel_F2X1


gemvt_kernel_F2X4_10:

	KERNEL_F2X4

	subs	I, I, #1
	bne	gemvt_kernel_F2X4_10


gemvt_kernel_F2X1:

	ands	I, M , #3
	ble	gemvt_kernel_F2_END

gemvt_kernel_F2X1_10:

	KERNEL_F2X1

	subs	I, I, #1
	bne	gemvt_kernel_F2X1_10


gemvt_kernel_F2_END:

	SAVE_F2

	subs	J , J , #1
	bne	gemvt_kernel_F2X4


gemvt_kernel_F1_BEGIN:

	ldr	J, N
	ands	J, J, #1
	ble	gemvt_kernel_L999

gemvt_kernel_F1X4:

	ldr	AO1, A

	ldr	XO , X

	INIT_F1

	asrs	I, M, #2					// I = M / 4
	ble	gemvt_kernel_F1X1


gemvt_kernel_F1X4_10:

	KERNEL_F1X4

	subs	I, I, #1
	bne	gemvt_kernel_F1X4_10


gemvt_kernel_F1X1:

	ands	I, M , #3
	ble	gemvt_kernel_F1_END

gemvt_kernel_F1X1_10:

	KERNEL_F1X1

	subs	I, I, #1
	bne	gemvt_kernel_F1X1_10


gemvt_kernel_F1_END:

	SAVE_F1

	b	gemvt_kernel_L999



/*************************************************************************************************************/

gemvt_kernel_S2_BEGIN:

#if defined(DOUBLE)
	lsl	INC_X, INC_X, #3				// INC_X * SIZE
	lsl	INC_Y, INC_Y, #3				// INC_Y * SIZE
#else
	lsl	INC_X, INC_X, #2				// INC_X * SIZE
	lsl	INC_Y, INC_Y, #2				// INC_Y * SIZE
#endif

	ldr	YO , Y

	ldr	J, N
	asrs	J, J, #1					// J = N / 2
	ble	gemvt_kernel_S1_BEGIN

gemvt_kernel_S2X4:

	ldr	AO1, A
	add	AO2, AO1, LDA
	add	r3 , AO2, LDA
	str	r3 , A

	ldr	XO , X

	INIT_S2

	asrs	I, M, #2					// I = M / 4
	ble	gemvt_kernel_S2X1


gemvt_kernel_S2X4_10:

	KERNEL_S2X4

	subs	I, I, #1
	bne	gemvt_kernel_S2X4_10


gemvt_kernel_S2X1:

	ands	I, M , #3
	ble	gemvt_kernel_S2_END

gemvt_kernel_S2X1_10:

	KERNEL_S2X1

	subs	I, I, #1
	bne	gemvt_kernel_S2X1_10


gemvt_kernel_S2_END:

	SAVE_S2

	subs	J , J , #1
	bne	gemvt_kernel_S2X4


gemvt_kernel_S1_BEGIN:

	ldr	J, N
	ands	J, J, #1
	ble	gemvt_kernel_L999

gemvt_kernel_S1X4:

	ldr	AO1, A

	ldr	XO , X

	INIT_S1

	asrs	I, M, #2					// I = M / 4
	ble	gemvt_kernel_S1X1


gemvt_kernel_S1X4_10:

	KERNEL_S1X4

	subs	I, I, #1
	bne	gemvt_kernel_S1X4_10


gemvt_kernel_S1X1:

	ands	I, M , #3
	ble	gemvt_kernel_S1_END

gemvt_kernel_S1X1_10:

	KERNEL_S1X1

	subs	I, I, #1
	bne	gemvt_kernel_S1X1_10


gemvt_kernel_S1_END:

	SAVE_S1



/*************************************************************************************************************/

gemvt_kernel_L999:

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

