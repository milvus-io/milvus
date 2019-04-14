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
* 2013/11/29 Saar
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*
**************************************************************************************/

#define ASSEMBLER
#include "common.h"

#define STACKSIZE 256

#if !defined(__ARM_PCS_VFP)
#define OLD_ALPHAR	r3
#define OLD_ALPHAI	[fp, #0 ]
#define OLD_A_SOFTFP	[fp, #4 ]
#define OLD_LDA		[fp, #8 ]
#define X		[fp, #12 ]
#define OLD_INC_X	[fp, #16 ]
#define Y		[fp, #20 ]
#define OLD_INC_Y	[fp, #24 ]
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

#define FP_ZERO [fp, #-228]
#define FP_ZERO_0 [fp, #-228]
#define FP_ZERO_1 [fp, #-224]

#define ALPHA_I [fp, #-236]
#define ALPHA_R [fp, #-244]

#define M	[fp, #-252 ]
#define A	[fp, #-256 ]


#define X_PRE	64
#define Y_PRE	0
#define A_PRE	0

/**************************************************************************************/

#if !defined(CONJ) && !defined(XCONJ)

        #define KMAC_R  vmls.f32
        #define KMAC_I  fmacs

        #define FMAC_R1 fmacs
        #define FMAC_R2 vmls.f32
        #define FMAC_I1 fmacs
        #define FMAC_I2 fmacs

#elif defined(CONJ) && !defined(XCONJ)

        #define KMAC_R  fmacs
        #define KMAC_I  vmls.f32

        #define FMAC_R1 fmacs
        #define FMAC_R2 vmls.f32
        #define FMAC_I1 fmacs
        #define FMAC_I2 fmacs

#elif !defined(CONJ) && defined(XCONJ)

        #define KMAC_R  fmacs
        #define KMAC_I  vmls.f32

        #define FMAC_R1 fmacs
        #define FMAC_R2 fmacs
        #define FMAC_I1 vmls.f32
        #define FMAC_I2 fmacs

#else

        #define KMAC_R  vmls.f32
        #define KMAC_I  fmacs

        #define FMAC_R1 fmacs
        #define FMAC_R2 fmacs
        #define FMAC_I1 vmls.f32
        #define FMAC_I2 fmacs

#endif

.macro INIT_F4

	pld	[ YO, #Y_PRE ]
	flds			s8 , FP_ZERO
        vmov.f32                s9 , s8
        vmov.f32                s10, s8
        vmov.f32                s11, s8
        vmov.f32                s12, s8
        vmov.f32                s13, s8
        vmov.f32                s14, s8
        vmov.f32                s15, s8

.endm

.macro KERNEL_F4X4

	pld	[ XO, #X_PRE ]
	KERNEL_F4X1
	KERNEL_F4X1
	KERNEL_F4X1
	KERNEL_F4X1

.endm

.macro KERNEL_F4X1

	pld	[ AO2, #A_PRE ]
        flds    s0 , [ AO1 ]
        flds    s1 , [ AO1, #4  ]
        flds    s2 , [ AO1, #8 ]
        flds    s3 , [ AO1, #12 ]

        flds    s4 , [ XO ]
        flds    s5 , [ XO, #4 ]

        fmacs   s8  , s0,  s4
        fmacs   s9  , s0,  s5
        fmacs   s10 , s2,  s4
        fmacs   s11 , s2,  s5

        KMAC_R  s8  , s1,  s5
        KMAC_I  s9  , s1,  s4
        KMAC_R  s10 , s3,  s5
        KMAC_I  s11 , s3,  s4

        flds    s0 , [ AO1, #16 ]
        flds    s1 , [ AO1, #20 ]
        flds    s2 , [ AO1, #24 ]
        flds    s3 , [ AO1, #28 ]

        fmacs   s12 , s0,  s4
        fmacs   s13 , s0,  s5
        fmacs   s14 , s2,  s4
        fmacs   s15 , s2,  s5

        KMAC_R  s12 , s1,  s5
        KMAC_I  s13 , s1,  s4
        KMAC_R  s14 , s3,  s5
        KMAC_I  s15 , s3,  s4

        add     XO , XO, #8
        add     AO1 , AO1, LDA
        add     AO2 , AO2, LDA

.endm

.macro SAVE_F4

        flds            s0, ALPHA_R
        flds            s1, ALPHA_I

        vldmia.f32 YO, { s4 - s7 }

        FMAC_R1 s4 , s0 , s8
        FMAC_I1 s5 , s0 , s9
        FMAC_R2 s4 , s1 , s9
        FMAC_I2 s5 , s1 , s8

        FMAC_R1 s6 , s0 , s10
        FMAC_I1 s7 , s0 , s11
        FMAC_R2 s6 , s1 , s11
        FMAC_I2 s7 , s1 , s10

        vstmia.f32 YO!, { s4 - s7 }

        vldmia.f32 YO, { s4 - s7 }

        FMAC_R1 s4 , s0 , s12
        FMAC_I1 s5 , s0 , s13
        FMAC_R2 s4 , s1 , s13
        FMAC_I2 s5 , s1 , s12

        FMAC_R1 s6 , s0 , s14
        FMAC_I1 s7 , s0 , s15
        FMAC_R2 s6 , s1 , s15
        FMAC_I2 s7 , s1 , s14

        vstmia.f32 YO!, { s4 - s7 }

.endm




.macro INIT_F1

	flds			s8 , FP_ZERO
        vmov.f32                s9 , s8

.endm

.macro KERNEL_F1X1

        flds    s0 , [ AO1 ]
        flds    s1 , [ AO1, #4 ]

        flds    s4 , [ XO ]
        flds    s5 , [ XO, #4 ]

        fmacs   s8  , s0,  s4
        fmacs   s9  , s0,  s5

        KMAC_R  s8  , s1,  s5
        KMAC_I  s9  , s1,  s4

        add     XO , XO, #8
        add     AO1 , AO1, LDA


.endm

.macro SAVE_F1

        flds            s0, ALPHA_R
        flds            s1, ALPHA_I

        vldmia.f32 YO, { s4 - s5 }

        FMAC_R1 s4 , s0 , s8
        FMAC_I1 s5 , s0 , s9
        FMAC_R2 s4 , s1 , s9
        FMAC_I2 s5 , s1 , s8

        vstmia.f32 YO, { s4 - s5 }

        add     YO, YO, #8

.endm

/****************************************************************************************/

.macro INIT_S4

	flds			s8 , FP_ZERO
        vmov.f32                s9 , s8
        vmov.f32                s10, s8
        vmov.f32                s11, s8
        vmov.f32                s12, s8
        vmov.f32                s13, s8
        vmov.f32                s14, s8
        vmov.f32                s15, s8

.endm

.macro KERNEL_S4X4

	KERNEL_S4X1
	KERNEL_S4X1
	KERNEL_S4X1
	KERNEL_S4X1

.endm

.macro KERNEL_S4X1

        flds    s0 , [ AO1 ]
        flds    s1 , [ AO1, #4  ]
        flds    s2 , [ AO1, #8 ]
        flds    s3 , [ AO1, #12 ]

        flds    s4 , [ XO ]
        flds    s5 , [ XO, #4 ]

        fmacs   s8  , s0,  s4
        fmacs   s9  , s0,  s5
        fmacs   s10 , s2,  s4
        fmacs   s11 , s2,  s5

        KMAC_R  s8  , s1,  s5
        KMAC_I  s9  , s1,  s4
        KMAC_R  s10 , s3,  s5
        KMAC_I  s11 , s3,  s4

        flds    s0 , [ AO1, #16 ]
        flds    s1 , [ AO1, #20 ]
        flds    s2 , [ AO1, #24 ]
        flds    s3 , [ AO1, #28 ]

        fmacs   s12 , s0,  s4
        fmacs   s13 , s0,  s5
        fmacs   s14 , s2,  s4
        fmacs   s15 , s2,  s5

        KMAC_R  s12 , s1,  s5
        KMAC_I  s13 , s1,  s4
        KMAC_R  s14 , s3,  s5
        KMAC_I  s15 , s3,  s4

        add     XO , XO, INC_X
        add     AO1 , AO1, LDA
        add     AO2 , AO2, LDA

.endm

.macro SAVE_S4

        flds            s0, ALPHA_R
        flds            s1, ALPHA_I

        vldmia.f32 YO, { s4 - s5 }

        FMAC_R1 s4 , s0 , s8
        FMAC_I1 s5 , s0 , s9
        FMAC_R2 s4 , s1 , s9
        FMAC_I2 s5 , s1 , s8

        vstmia.f32 YO, { s4 - s5 }

	add	YO, YO, INC_Y

        vldmia.f32 YO, { s6 - s7 }

        FMAC_R1 s6 , s0 , s10
        FMAC_I1 s7 , s0 , s11
        FMAC_R2 s6 , s1 , s11
        FMAC_I2 s7 , s1 , s10

        vstmia.f32 YO, { s6 - s7 }

	add	YO, YO, INC_Y

        vldmia.f32 YO, { s4 - s5 }

        FMAC_R1 s4 , s0 , s12
        FMAC_I1 s5 , s0 , s13
        FMAC_R2 s4 , s1 , s13
        FMAC_I2 s5 , s1 , s12

        vstmia.f32 YO, { s4 - s5 }

	add	YO, YO, INC_Y

        vldmia.f32 YO, { s6 - s7 }

        FMAC_R1 s6 , s0 , s14
        FMAC_I1 s7 , s0 , s15
        FMAC_R2 s6 , s1 , s15
        FMAC_I2 s7 , s1 , s14

        vstmia.f32 YO, { s6 - s7 }

	add	YO, YO, INC_Y

.endm




.macro INIT_S1

	flds			s8 , FP_ZERO
        vmov.f32                s9 , s8

.endm

.macro KERNEL_S1X1

        flds    s0 , [ AO1 ]
        flds    s1 , [ AO1, #4 ]

        flds    s4 , [ XO ]
        flds    s5 , [ XO, #4 ]

        fmacs   s8  , s0,  s4
        fmacs   s9  , s0,  s5

        KMAC_R  s8  , s1,  s5
        KMAC_I  s9  , s1,  s4

        add     XO , XO, INC_X
        add     AO1 , AO1, LDA


.endm

.macro SAVE_S1

        flds            s0, ALPHA_R
        flds            s1, ALPHA_I

        vldmia.f32 YO, { s4 - s5 }

        FMAC_R1 s4 , s0 , s8
        FMAC_I1 s5 , s0 , s9
        FMAC_R2 s4 , s1 , s9
        FMAC_I2 s5 , s1 , s8

        vstmia.f32 YO, { s4 - s5 }

        add     YO, YO, INC_Y

.endm



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
        vstm    r12, { s8 - s15 }                                 // store floating point registers
#endif

        movs    r12, #0
        str     r12, FP_ZERO
        str     r12, FP_ZERO_1

	cmp	OLD_M, #0
	ble	cgemvn_kernel_L999

	cmp	N, #0
	ble	cgemvn_kernel_L999

#if !defined(__ARM_PCS_VFP)
	vmov	s0, OLD_ALPHAR
	vldr	s1, OLD_ALPHAI
	ldr	OLD_A, OLD_A_SOFTFP
#endif

	str	OLD_A, A
	str	OLD_M, M
	vstr    s0 , ALPHA_R
        vstr    s1 , ALPHA_I


	ldr    INC_X , OLD_INC_X
	ldr    INC_Y , OLD_INC_Y

	cmp	INC_X, #0
	beq	cgemvn_kernel_L999

	cmp	INC_Y, #0
	beq	cgemvn_kernel_L999

	ldr	LDA, OLD_LDA


#if defined(DOUBLE)
	lsl	LDA, LDA, #4				// LDA * SIZE * 2
#else
	lsl	LDA, LDA, #3				// LDA * SIZE * 2
#endif

	cmp	INC_X, #1
	bne	cgemvn_kernel_S4_BEGIN

	cmp	INC_Y, #1
	bne	cgemvn_kernel_S4_BEGIN


cgemvn_kernel_F4_BEGIN:

	ldr	YO , Y

	ldr	I, M
	asrs	I, I, #2					// I = M / 4
	ble	cgemvn_kernel_F1_BEGIN

cgemvn_kernel_F4X4:

	ldr	AO1, A
	add	AO2, AO1, LDA
	add	r3 , AO1, #32
	str	r3 , A

	add	AO2, AO2, LDA
	add	AO2, AO2, LDA

	ldr	XO , X

	INIT_F4

	asrs	J, N, #2					// J = N / 4
	ble	cgemvn_kernel_F4X1


cgemvn_kernel_F4X4_10:

	KERNEL_F4X4

	subs	J, J, #1
	bne	cgemvn_kernel_F4X4_10


cgemvn_kernel_F4X1:

	ands	J, N , #3
	ble	cgemvn_kernel_F4_END

cgemvn_kernel_F4X1_10:

	KERNEL_F4X1

	subs	J, J, #1
	bne	cgemvn_kernel_F4X1_10


cgemvn_kernel_F4_END:

	SAVE_F4

	subs	I , I , #1
	bne	cgemvn_kernel_F4X4


cgemvn_kernel_F1_BEGIN:

	ldr	I, M
	ands	I,  I , #3
	ble	cgemvn_kernel_L999

cgemvn_kernel_F1X1:

	ldr	AO1, A
	add	r3, AO1, #8
	str	r3, A

	ldr	XO , X

	INIT_F1

	mov	J, N


cgemvn_kernel_F1X1_10:

	KERNEL_F1X1

	subs	J, J, #1
	bne	cgemvn_kernel_F1X1_10


cgemvn_kernel_F1_END:

	SAVE_F1

	subs	I , I , #1
	bne	cgemvn_kernel_F1X1

	b	cgemvn_kernel_L999



/*************************************************************************************************************/

cgemvn_kernel_S4_BEGIN:

#if defined(DOUBLE)
	lsl	INC_X, INC_X, #4				// INC_X * SIZE * 2
	lsl	INC_Y, INC_Y, #4				// INC_Y * SIZE * 2
#else
	lsl	INC_X, INC_X, #3				// INC_X * SIZE * 2
	lsl	INC_Y, INC_Y, #3				// INC_Y * SIZE * 2
#endif

	ldr	YO , Y

	ldr	I, M
	asrs	I, I, #2					// I = M / 4
	ble	cgemvn_kernel_S1_BEGIN

cgemvn_kernel_S4X4:

	ldr	AO1, A
	add	AO2, AO1, LDA
	add	r3 , AO1, #32
	str	r3 , A

	ldr	XO , X

	INIT_S4

	asrs	J, N, #2					// J = N / 4
	ble	cgemvn_kernel_S4X1


cgemvn_kernel_S4X4_10:

	KERNEL_S4X4

	subs	J, J, #1
	bne	cgemvn_kernel_S4X4_10


cgemvn_kernel_S4X1:

	ands	J, N , #3
	ble	cgemvn_kernel_S4_END

cgemvn_kernel_S4X1_10:

	KERNEL_S4X1

	subs	J, J, #1
	bne	cgemvn_kernel_S4X1_10


cgemvn_kernel_S4_END:

	SAVE_S4

	subs	I , I , #1
	bne	cgemvn_kernel_S4X4


cgemvn_kernel_S1_BEGIN:

	ldr	I, M
	ands	I,  I , #3
	ble	cgemvn_kernel_L999

cgemvn_kernel_S1X1:

	ldr	AO1, A
	add	r3, AO1, #8
	str	r3, A

	ldr	XO , X

	INIT_S1

	mov	J, N


cgemvn_kernel_S1X1_10:

	KERNEL_S1X1

	subs	J, J, #1
	bne	cgemvn_kernel_S1X1_10


cgemvn_kernel_S1_END:

	SAVE_S1

	subs	I , I , #1
	bne	cgemvn_kernel_S1X1


/*************************************************************************************************************/

cgemvn_kernel_L999:

        sub     r3, fp, #192

#if	defined(DOUBLE)
        vldm    r3, { d8 - d15 }                                 // restore floating point registers
#else
        vldm    r3, { s8 - s15 }                                 // restore floating point registers
#endif

	mov	r0, #0		// set return value

	sub     sp, fp, #28
	pop     {r4 -r9 ,fp}
	bx	lr

	EPILOGUE

