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

#define FP_ZERO [fp, #-228]
#define FP_ZERO_0 [fp, #-228]
#define FP_ZERO_1 [fp, #-224]

#define N	[fp, #-252 ]
#define A	[fp, #-256 ]


#define X_PRE	512
#define A_PRE	512
#define Y_PRE	32

/**************************************************************************************
* Macro definitions
**************************************************************************************/

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



.macro INIT_F2

	fldd		d12, FP_ZERO
	vmov.f64	d13, d12
	vmov.f64	d14, d12
	vmov.f64	d15, d12

.endm

.macro KERNEL_F2X4

	KERNEL_F2X1
	KERNEL_F2X1
	KERNEL_F2X1
	KERNEL_F2X1

.endm

.macro KERNEL_F2X1

	vldmia.f64	XO! ,  { d2 - d3 }
	vldmia.f64	AO1!,  { d4 - d5 }

	fmacd	d12 , d4 , d2
	fmacd	d13 , d4 , d3
	vldmia.f64	AO2!,  { d8 - d9   }
	KMAC_R  d12 , d5 , d3
        KMAC_I  d13 , d5 , d2

	fmacd	d14 , d8 , d2
	fmacd	d15 , d8 , d3
        KMAC_R  d14 , d9 , d3
        KMAC_I  d15 , d9 , d2

.endm

.macro	SAVE_F2

	vldmia.f64	YO,  { d4 - d7 }

	FMAC_R1 d4 , d0 , d12
        FMAC_I1 d5 , d0 , d13
        FMAC_R2 d4 , d1 , d13
        FMAC_I2 d5 , d1 , d12

        FMAC_R1 d6 , d0 , d14
        FMAC_I1 d7 , d0 , d15
        FMAC_R2 d6 , d1 , d15
        FMAC_I2 d7 , d1 , d14

	vstmia.f64	YO!, { d4 - d7 }

.endm

/************************************************************************************************/

.macro INIT_F1

	fldd		d12, FP_ZERO
	vmov.f64	d13, d12

.endm

.macro KERNEL_F1X4

	KERNEL_F1X1
	KERNEL_F1X1
	KERNEL_F1X1
	KERNEL_F1X1

.endm

.macro KERNEL_F1X1

	vldmia.f64	XO! ,  { d2 - d3 }
	vldmia.f64	AO1!,  { d4 - d5 }

	fmacd	d12 , d4 , d2
	fmacd	d13 , d4 , d3
	KMAC_R  d12 , d5 , d3
        KMAC_I  d13 , d5 , d2

.endm

.macro	SAVE_F1

	vldmia.f64	YO,  { d4 - d5 }

	FMAC_R1 d4 , d0 , d12
        FMAC_I1 d5 , d0 , d13
        FMAC_R2 d4 , d1 , d13
        FMAC_I2 d5 , d1 , d12

	vstmia.f64	YO!, { d4 - d5 }

.endm

/************************************************************************************************/

.macro INIT_S2

	fldd		d12, FP_ZERO
	vmov.f64	d13, d12
	vmov.f64	d14, d12
	vmov.f64	d15, d12

.endm

.macro KERNEL_S2X4

	KERNEL_S2X1
	KERNEL_S2X1
	KERNEL_S2X1
	KERNEL_S2X1

.endm

.macro KERNEL_S2X1

	vldmia.f64	XO  ,  { d2 - d3 }
	vldmia.f64	AO1!,  { d4 - d5 }
	vldmia.f64	AO2!,  { d8 - d9   }

	fmacd	d12 , d4 , d2
	fmacd	d13 , d4 , d3
	KMAC_R  d12 , d5 , d3
        KMAC_I  d13 , d5 , d2

	fmacd	d14 , d8 , d2
	fmacd	d15 , d8 , d3
        KMAC_R  d14 , d9 , d3
        KMAC_I  d15 , d9 , d2

	add	XO, XO, INC_X

.endm

.macro	SAVE_S2

	vldmia.f64	YO,  { d4 - d5 }

	FMAC_R1 d4 , d0 , d12
        FMAC_I1 d5 , d0 , d13
        FMAC_R2 d4 , d1 , d13
        FMAC_I2 d5 , d1 , d12

	vstmia.f64	YO,  { d4 - d5 }

	add	YO, YO, INC_Y

	vldmia.f64	YO,  { d6 - d7 }

        FMAC_R1 d6 , d0 , d14
        FMAC_I1 d7 , d0 , d15
        FMAC_R2 d6 , d1 , d15
        FMAC_I2 d7 , d1 , d14

	vstmia.f64	YO,  { d6 - d7 }

	add	YO, YO, INC_Y

.endm

/************************************************************************************************/

.macro INIT_S1

	fldd		d12, FP_ZERO
	vmov.f64	d13, d12

.endm

.macro KERNEL_S1X4

	KERNEL_S1X1
	KERNEL_S1X1
	KERNEL_S1X1
	KERNEL_S1X1

.endm

.macro KERNEL_S1X1

	vldmia.f64	XO  ,  { d2 - d3 }
	vldmia.f64	AO1!,  { d4 - d5 }

	fmacd	d12 , d4 , d2
	fmacd	d13 , d4 , d3
	KMAC_R  d12 , d5 , d3
        KMAC_I  d13 , d5 , d2

	add	XO, XO, INC_X

.endm

.macro	SAVE_S1

	vldmia.f64	YO,  { d4 - d5 }

	FMAC_R1 d4 , d0 , d12
        FMAC_I1 d5 , d0 , d13
        FMAC_R2 d4 , d1 , d13
        FMAC_I2 d5 , d1 , d12

	vstmia.f64	YO,  { d4 - d5 }

	add	YO, YO, INC_Y

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

	cmp	M, #0
	ble	zgemvt_kernel_L999

	cmp	OLD_N, #0
	ble	zgemvt_kernel_L999

#if !defined(__ARM_PCS_VFP)
	vldr	d0, OLD_ALPHAR
	vldr	d1, OLD_ALPHAI
	ldr	OLD_A, OLD_A_SOFTFP
#endif

	str	OLD_A, A
	str	OLD_N, N

	ldr    INC_X , OLD_INC_X
	ldr    INC_Y , OLD_INC_Y

	cmp	INC_X, #0
	beq	zgemvt_kernel_L999

	cmp	INC_Y, #0
	beq	zgemvt_kernel_L999

	ldr	LDA, OLD_LDA


#if defined(DOUBLE)
	lsl	LDA, LDA, #4				// LDA * SIZE
#else
	lsl	LDA, LDA, #3				// LDA * SIZE
#endif

	cmp	INC_X, #1
	bne	zgemvt_kernel_S2_BEGIN

	cmp	INC_Y, #1
	bne	zgemvt_kernel_S2_BEGIN


zgemvt_kernel_F2_BEGIN:

	ldr	YO , Y

	ldr	J, N
	asrs	J, J, #1					// J = N / 2
	ble	zgemvt_kernel_F1_BEGIN

zgemvt_kernel_F2X4:

	ldr	AO1, A
	add	AO2, AO1, LDA
	add	r3 , AO2, LDA
	str	r3 , A

	ldr	XO , X

	INIT_F2

	asrs	I, M, #2					// I = M / 4
	ble	zgemvt_kernel_F2X1


zgemvt_kernel_F2X4_10:

	KERNEL_F2X4

	subs	I, I, #1
	bne	zgemvt_kernel_F2X4_10


zgemvt_kernel_F2X1:

	ands	I, M , #3
	ble	zgemvt_kernel_F2_END

zgemvt_kernel_F2X1_10:

	KERNEL_F2X1

	subs	I, I, #1
	bne	zgemvt_kernel_F2X1_10


zgemvt_kernel_F2_END:

	SAVE_F2

	subs	J , J , #1
	bne	zgemvt_kernel_F2X4


zgemvt_kernel_F1_BEGIN:

	ldr	J, N
	ands	J, J, #1
	ble	zgemvt_kernel_L999

zgemvt_kernel_F1X4:

	ldr	AO1, A

	ldr	XO , X

	INIT_F1

	asrs	I, M, #2					// I = M / 4
	ble	zgemvt_kernel_F1X1


zgemvt_kernel_F1X4_10:

	KERNEL_F1X4

	subs	I, I, #1
	bne	zgemvt_kernel_F1X4_10


zgemvt_kernel_F1X1:

	ands	I, M , #3
	ble	zgemvt_kernel_F1_END

zgemvt_kernel_F1X1_10:

	KERNEL_F1X1

	subs	I, I, #1
	bne	zgemvt_kernel_F1X1_10


zgemvt_kernel_F1_END:

	SAVE_F1

	b	zgemvt_kernel_L999



/*************************************************************************************************************/

zgemvt_kernel_S2_BEGIN:

#if defined(DOUBLE)
	lsl	INC_X, INC_X, #4				// INC_X * SIZE
	lsl	INC_Y, INC_Y, #4				// INC_Y * SIZE
#else
	lsl	INC_X, INC_X, #3				// INC_X * SIZE
	lsl	INC_Y, INC_Y, #3				// INC_Y * SIZE
#endif

	ldr	YO , Y

	ldr	J, N
	asrs	J, J, #1					// J = N / 2
	ble	zgemvt_kernel_S1_BEGIN

zgemvt_kernel_S2X4:

	ldr	AO1, A
	add	AO2, AO1, LDA
	add	r3 , AO2, LDA
	str	r3 , A

	ldr	XO , X

	INIT_S2

	asrs	I, M, #2					// I = M / 4
	ble	zgemvt_kernel_S2X1


zgemvt_kernel_S2X4_10:

	KERNEL_S2X4

	subs	I, I, #1
	bne	zgemvt_kernel_S2X4_10


zgemvt_kernel_S2X1:

	ands	I, M , #3
	ble	zgemvt_kernel_S2_END

zgemvt_kernel_S2X1_10:

	KERNEL_S2X1

	subs	I, I, #1
	bne	zgemvt_kernel_S2X1_10


zgemvt_kernel_S2_END:

	SAVE_S2

	subs	J , J , #1
	bne	zgemvt_kernel_S2X4


zgemvt_kernel_S1_BEGIN:

	ldr	J, N
	ands	J, J, #1
	ble	zgemvt_kernel_L999

zgemvt_kernel_S1X4:

	ldr	AO1, A

	ldr	XO , X

	INIT_S1

	asrs	I, M, #2					// I = M / 4
	ble	zgemvt_kernel_S1X1


zgemvt_kernel_S1X4_10:

	KERNEL_S1X4

	subs	I, I, #1
	bne	zgemvt_kernel_S1X4_10


zgemvt_kernel_S1X1:

	ands	I, M , #3
	ble	zgemvt_kernel_S1_END

zgemvt_kernel_S1X1_10:

	KERNEL_S1X1

	subs	I, I, #1
	bne	zgemvt_kernel_S1X1_10


zgemvt_kernel_S1_END:

	SAVE_S1



/*************************************************************************************************************/

zgemvt_kernel_L999:

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

