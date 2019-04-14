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
#define OLD_ALPHAR	[fp, #0 ]
#define OLD_ALPHAI	[fp, #8 ]
#define OLD_A_SOFTFP	[fp, #16]
#define OLD_LDA		[fp, #20]
#define X		[fp, #24]
#define OLD_INC_X	[fp, #28]
#define Y		[fp, #32]
#define OLD_INC_Y	[fp, #36]
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

        #define KMAC_R  vmls.f64
        #define KMAC_I  fmacd

        #define FMAC_R1 fmacd
        #define FMAC_R2 vmls.f64
        #define FMAC_I1 fmacd
        #define FMAC_I2 fmacd

#elif defined(CONJ) && !defined(XCONJ)

        #define KMAC_R  fmacd
        #define KMAC_I  vmls.f64

        #define FMAC_R1 fmacd
        #define FMAC_R2 vmls.f64
        #define FMAC_I1 fmacd
        #define FMAC_I2 fmacd

#elif !defined(CONJ) && defined(XCONJ)

        #define KMAC_R  fmacd
        #define KMAC_I  vmls.f64

        #define FMAC_R1 fmacd
        #define FMAC_R2 fmacd
        #define FMAC_I1 vmls.f64
        #define FMAC_I2 fmacd

#else

        #define KMAC_R  vmls.f64
        #define KMAC_I  fmacd

        #define FMAC_R1 fmacd
        #define FMAC_R2 fmacd
        #define FMAC_I1 vmls.f64
        #define FMAC_I2 fmacd

#endif

.macro INIT_F4

	pld	[ YO, #Y_PRE ]
	fldd			d8, FP_ZERO
        vmov.f64                d9 , d8
        vmov.f64                d10, d8
        vmov.f64                d11, d8
        vmov.f64                d12, d8
        vmov.f64                d13, d8
        vmov.f64                d14, d8
        vmov.f64                d15, d8

.endm

.macro KERNEL_F4X4

	pld	[ XO, #X_PRE ]
	KERNEL_F4X1
	KERNEL_F4X1
	pld	[ XO, #X_PRE ]
	KERNEL_F4X1
	KERNEL_F4X1

.endm

.macro KERNEL_F4X1

        fldd    d0 , [ AO1 ]

        fldd    d4 , [ XO ]
        fldd    d5 , [ XO, #8 ]

	pld	[ AO2, #A_PRE ]

        fldd    d1 , [ AO1, #8  ]
        fmacd   d8  , d0,  d4
        fldd    d2 , [ AO1, #16 ]
        fmacd   d9  , d0,  d5
        fldd    d3 , [ AO1, #24 ]
        fmacd   d10 , d2,  d4
        fldd    d0 , [ AO1, #32 ]
        fmacd   d11 , d2,  d5

        KMAC_R  d8  , d1,  d5
        KMAC_I  d9  , d1,  d4
        KMAC_R  d10 , d3,  d5
        fldd    d1 , [ AO1, #40 ]
        KMAC_I  d11 , d3,  d4

        fldd    d2 , [ AO1, #48 ]

        fmacd   d12 , d0,  d4
        fldd    d3 , [ AO1, #56 ]
        fmacd   d13 , d0,  d5
	pld	[ AO2, #A_PRE+32 ]
        fmacd   d14 , d2,  d4
        fmacd   d15 , d2,  d5

        KMAC_R  d12 , d1,  d5
        add     XO , XO, #16
        KMAC_I  d13 , d1,  d4
        add     AO1 , AO1, LDA
        KMAC_R  d14 , d3,  d5
        add     AO2 , AO2, LDA
        KMAC_I  d15 , d3,  d4

.endm

.macro SAVE_F4

        fldd            d0, ALPHA_R
        fldd            d1, ALPHA_I

        vldmia.f64 YO, { d4 - d7 }

        FMAC_R1 d4 , d0 , d8
        FMAC_I1 d5 , d0 , d9
        FMAC_R2 d4 , d1 , d9
        FMAC_I2 d5 , d1 , d8

        FMAC_R1 d6 , d0 , d10
        FMAC_I1 d7 , d0 , d11
        FMAC_R2 d6 , d1 , d11
        FMAC_I2 d7 , d1 , d10

        vstmia.f64 YO!, { d4 - d7 }

        vldmia.f64 YO, { d4 - d7 }

        FMAC_R1 d4 , d0 , d12
        FMAC_I1 d5 , d0 , d13
        FMAC_R2 d4 , d1 , d13
        FMAC_I2 d5 , d1 , d12

        FMAC_R1 d6 , d0 , d14
        FMAC_I1 d7 , d0 , d15
        FMAC_R2 d6 , d1 , d15
        FMAC_I2 d7 , d1 , d14

        vstmia.f64 YO!, { d4 - d7 }

.endm




.macro INIT_F1

	fldd			d8, FP_ZERO
        vmov.f64                d9 , d8

.endm

.macro KERNEL_F1X1

        fldd    d0 , [ AO1 ]
        fldd    d1 , [ AO1, #8 ]

        fldd    d4 , [ XO ]
        fldd    d5 , [ XO, #8 ]

        fmacd   d8  , d0,  d4
        fmacd   d9  , d0,  d5

        KMAC_R  d8  , d1,  d5
        KMAC_I  d9  , d1,  d4

        add     XO , XO, #16
        add     AO1 , AO1, LDA


.endm

.macro SAVE_F1

        fldd            d0, ALPHA_R
        fldd            d1, ALPHA_I

        vldmia.f64 YO, { d4 - d5 }

        FMAC_R1 d4 , d0 , d8
        FMAC_I1 d5 , d0 , d9
        FMAC_R2 d4 , d1 , d9
        FMAC_I2 d5 , d1 , d8

        vstmia.f64 YO, { d4 - d5 }

        add     YO, YO, #16

.endm

/****************************************************************************************/

.macro INIT_S4

	fldd			d8, FP_ZERO
        vmov.f64                d9 , d8
        vmov.f64                d10, d8
        vmov.f64                d11, d8
        vmov.f64                d12, d8
        vmov.f64                d13, d8
        vmov.f64                d14, d8
        vmov.f64                d15, d8

.endm

.macro KERNEL_S4X4

	KERNEL_S4X1
	KERNEL_S4X1
	KERNEL_S4X1
	KERNEL_S4X1

.endm

.macro KERNEL_S4X1

        fldd    d0 , [ AO1 ]
        fldd    d1 , [ AO1, #8  ]
        fldd    d2 , [ AO1, #16 ]
        fldd    d3 , [ AO1, #24 ]

        fldd    d4 , [ XO ]
        fldd    d5 , [ XO, #8 ]

        fmacd   d8  , d0,  d4
        fmacd   d9  , d0,  d5
        fmacd   d10 , d2,  d4
        fmacd   d11 , d2,  d5

        KMAC_R  d8  , d1,  d5
        KMAC_I  d9  , d1,  d4
        KMAC_R  d10 , d3,  d5
        KMAC_I  d11 , d3,  d4

        fldd    d0 , [ AO1, #32 ]
        fldd    d1 , [ AO1, #40 ]
        fldd    d2 , [ AO1, #48 ]
        fldd    d3 , [ AO1, #56 ]

        fmacd   d12 , d0,  d4
        fmacd   d13 , d0,  d5
        fmacd   d14 , d2,  d4
        fmacd   d15 , d2,  d5

        KMAC_R  d12 , d1,  d5
        KMAC_I  d13 , d1,  d4
        KMAC_R  d14 , d3,  d5
        KMAC_I  d15 , d3,  d4

        add     XO , XO, INC_X
        add     AO1 , AO1, LDA
        add     AO2 , AO2, LDA

.endm

.macro SAVE_S4

        fldd            d0, ALPHA_R
        fldd            d1, ALPHA_I

        vldmia.f64 YO, { d4 - d5 }

        FMAC_R1 d4 , d0 , d8
        FMAC_I1 d5 , d0 , d9
        FMAC_R2 d4 , d1 , d9
        FMAC_I2 d5 , d1 , d8

        vstmia.f64 YO, { d4 - d5 }

	add	YO, YO, INC_Y

        vldmia.f64 YO, { d6 - d7 }

        FMAC_R1 d6 , d0 , d10
        FMAC_I1 d7 , d0 , d11
        FMAC_R2 d6 , d1 , d11
        FMAC_I2 d7 , d1 , d10

        vstmia.f64 YO, { d6 - d7 }

	add	YO, YO, INC_Y

        vldmia.f64 YO, { d4 - d5 }

        FMAC_R1 d4 , d0 , d12
        FMAC_I1 d5 , d0 , d13
        FMAC_R2 d4 , d1 , d13
        FMAC_I2 d5 , d1 , d12

        vstmia.f64 YO, { d4 - d5 }

	add	YO, YO, INC_Y

        vldmia.f64 YO, { d6 - d7 }

        FMAC_R1 d6 , d0 , d14
        FMAC_I1 d7 , d0 , d15
        FMAC_R2 d6 , d1 , d15
        FMAC_I2 d7 , d1 , d14

        vstmia.f64 YO, { d6 - d7 }

	add	YO, YO, INC_Y

.endm




.macro INIT_S1

	fldd			d8, FP_ZERO
        vmov.f64                d9 , d8

.endm

.macro KERNEL_S1X1

        fldd    d0 , [ AO1 ]
        fldd    d1 , [ AO1, #8 ]

        fldd    d4 , [ XO ]
        fldd    d5 , [ XO, #8 ]

        fmacd   d8  , d0,  d4
        fmacd   d9  , d0,  d5

        KMAC_R  d8  , d1,  d5
        KMAC_I  d9  , d1,  d4

        add     XO , XO, INC_X
        add     AO1 , AO1, LDA


.endm

.macro SAVE_S1

        fldd            d0, ALPHA_R
        fldd            d1, ALPHA_I

        vldmia.f64 YO, { d4 - d5 }

        FMAC_R1 d4 , d0 , d8
        FMAC_I1 d5 , d0 , d9
        FMAC_R2 d4 , d1 , d9
        FMAC_I2 d5 , d1 , d8

        vstmia.f64 YO, { d4 - d5 }

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
	ble	zgemvn_kernel_L999

	cmp	N, #0
	ble	zgemvn_kernel_L999

#if !defined(__ARM_PCS_VFP)
	vldr	d0, OLD_ALPHAR
	vldr	d1, OLD_ALPHAI
	ldr	OLD_A, OLD_A_SOFTFP
#endif

	str	OLD_A, A
	str	OLD_M, M
	vstr    d0 , ALPHA_R
        vstr    d1 , ALPHA_I


	ldr    INC_X , OLD_INC_X
	ldr    INC_Y , OLD_INC_Y

	cmp	INC_X, #0
	beq	zgemvn_kernel_L999

	cmp	INC_Y, #0
	beq	zgemvn_kernel_L999

	ldr	LDA, OLD_LDA


#if defined(DOUBLE)
	lsl	LDA, LDA, #4				// LDA * SIZE * 2
#else
	lsl	LDA, LDA, #3				// LDA * SIZE * 2
#endif

	cmp	INC_X, #1
	bne	zgemvn_kernel_S4_BEGIN

	cmp	INC_Y, #1
	bne	zgemvn_kernel_S4_BEGIN


zgemvn_kernel_F4_BEGIN:

	ldr	YO , Y

	ldr	I, M
	asrs	I, I, #2					// I = M / 4
	ble	zgemvn_kernel_F1_BEGIN

zgemvn_kernel_F4X4:

	ldr	AO1, A
	add	AO2, AO1, LDA
	add	r3 , AO1, #64
	str	r3 , A

	add	AO2, AO2, LDA
	add	AO2, AO2, LDA

	ldr	XO , X

	INIT_F4

	asrs	J, N, #2					// J = N / 4
	ble	zgemvn_kernel_F4X1


zgemvn_kernel_F4X4_10:

	KERNEL_F4X4

	subs	J, J, #1
	bne	zgemvn_kernel_F4X4_10


zgemvn_kernel_F4X1:

	ands	J, N , #3
	ble	zgemvn_kernel_F4_END

zgemvn_kernel_F4X1_10:

	KERNEL_F4X1

	subs	J, J, #1
	bne	zgemvn_kernel_F4X1_10


zgemvn_kernel_F4_END:

	SAVE_F4

	subs	I , I , #1
	bne	zgemvn_kernel_F4X4


zgemvn_kernel_F1_BEGIN:

	ldr	I, M
	ands	I,  I , #3
	ble	zgemvn_kernel_L999

zgemvn_kernel_F1X1:

	ldr	AO1, A
	add	r3, AO1, #16
	str	r3, A

	ldr	XO , X

	INIT_F1

	mov	J, N


zgemvn_kernel_F1X1_10:

	KERNEL_F1X1

	subs	J, J, #1
	bne	zgemvn_kernel_F1X1_10


zgemvn_kernel_F1_END:

	SAVE_F1

	subs	I , I , #1
	bne	zgemvn_kernel_F1X1

	b	zgemvn_kernel_L999



/*************************************************************************************************************/

zgemvn_kernel_S4_BEGIN:

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
	ble	zgemvn_kernel_S1_BEGIN

zgemvn_kernel_S4X4:

	ldr	AO1, A
	add	AO2, AO1, LDA
	add	r3 , AO1, #64
	str	r3 , A

	ldr	XO , X

	INIT_S4

	asrs	J, N, #2					// J = N / 4
	ble	zgemvn_kernel_S4X1


zgemvn_kernel_S4X4_10:

	KERNEL_S4X4

	subs	J, J, #1
	bne	zgemvn_kernel_S4X4_10


zgemvn_kernel_S4X1:

	ands	J, N , #3
	ble	zgemvn_kernel_S4_END

zgemvn_kernel_S4X1_10:

	KERNEL_S4X1

	subs	J, J, #1
	bne	zgemvn_kernel_S4X1_10


zgemvn_kernel_S4_END:

	SAVE_S4

	subs	I , I , #1
	bne	zgemvn_kernel_S4X4


zgemvn_kernel_S1_BEGIN:

	ldr	I, M
	ands	I,  I , #3
	ble	zgemvn_kernel_L999

zgemvn_kernel_S1X1:

	ldr	AO1, A
	add	r3, AO1, #16
	str	r3, A

	ldr	XO , X

	INIT_S1

	mov	J, N


zgemvn_kernel_S1X1_10:

	KERNEL_S1X1

	subs	J, J, #1
	bne	zgemvn_kernel_S1X1_10


zgemvn_kernel_S1_END:

	SAVE_S1

	subs	I , I , #1
	bne	zgemvn_kernel_S1X1


/*************************************************************************************************************/

zgemvn_kernel_L999:

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

