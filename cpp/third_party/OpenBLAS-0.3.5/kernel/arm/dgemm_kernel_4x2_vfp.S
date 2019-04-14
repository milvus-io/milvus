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
* 2013/11/27 Saar
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*
**************************************************************************************/

#define ASSEMBLER
#include "common.h"

#define STACKSIZE 256

#define	OLD_M	r0
#define	OLD_N	r1
#define	OLD_K	r2
#define	OLD_A	r3
#define OLD_ALPHA d0

/******************************************************
* [fp, #-128] - [fp, #-64] is reserved
* for store and restore of floating point
* registers
*******************************************************/

#define LDC	[fp, #-252 ]
#define M	[fp, #-256 ]
#define N	[fp, #-260 ]
#define K	[fp, #-264 ]
#define A	[fp, #-268 ]

#define FP_ZERO [fp, #-240]
#define FP_ZERO_0 [fp, # -240]
#define FP_ZERO_1 [fp, # -236]

#define ALPHA	[fp, #-280]

#if !defined(__ARM_PCS_VFP)
#define OLD_ALPHA_SOFTFP	[fp, #4]
#define OLD_A_SOFTFP	[fp, #12 ]
#define B	[fp, #16 ]
#define C	[fp, #20 ]
#define OLD_LDC	[fp, #24 ]
#else
#define B	[fp, #4 ]
#define C	[fp, #8 ]
#define OLD_LDC	[fp, #12 ]
#endif

#define I	r0
#define J	r1
#define L	r2

#define	AO	r5
#define	BO	r6

#define	CO1	r8
#define	CO2	r9

#define K1	r7
#define BC	r12

#define A_PRE	96
#define B_PRE	96
#define C_PRE	32

/**************************************************************************************
* Macro definitions
**************************************************************************************/

.macro INIT4x2

	fldd			d8, FP_ZERO
	vmov.f64		d9, d8
	vmov.f64		d10, d8
	vmov.f64		d11, d8
	vmov.f64		d12, d8
	vmov.f64		d13, d8
	vmov.f64		d14, d8
	vmov.f64		d15, d8

.endm



.macro KERNEL4x2_SUB

	pld	[ AO, #A_PRE ]
	fldd	d4 , [ BO ]

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]

	fmacd	d8  , d0,  d4
	fldd	d2 , [ AO, #16 ]
	fmacd	d9  , d1,  d4
	fldd	d3 , [ AO, #24 ]
	fmacd	d10  , d2,  d4
	fldd	d5 , [ BO, #8 ]
	fmacd	d11  , d3,  d4

	fmacd	d12  , d0,  d5
	fmacd	d13  , d1,  d5
	add	AO , AO, #32
	fmacd	d14  , d2,  d5
	add	BO , BO, #16
	fmacd	d15  , d3,  d5


.endm

.macro SAVE4x2

	ldr	r3  , LDC
	add	CO2 , CO1, r3

	fldd		d0, ALPHA


	fldd	d4 , [CO1]
	fldd	d5 , [CO1, #8 ]

	pld	[ CO1, #C_PRE ]
	fmacd	d4 , d0 , d8
	fldd	d6 , [CO1, #16 ]
	fmacd	d5 , d0 , d9
	fldd	d7 , [CO1, #24 ]
	fmacd	d6 , d0 , d10
	fstd	d4 , [CO1]
	fmacd	d7 , d0 , d11

	fstd	d5 , [CO1, #8 ]
	fstd	d6 , [CO1, #16 ]
	fstd	d7 , [CO1, #24 ]

	fldd	d4 , [CO2]
	fldd	d5 , [CO2, #8 ]

	pld	[ CO2, #C_PRE ]
	fmacd	d4 , d0 , d12
	fldd	d6 , [CO2, #16 ]
	fmacd	d5 , d0 , d13
	fldd	d7 , [CO2, #24 ]
	fmacd	d6 , d0 , d14
	fstd	d4 , [CO2]
	fmacd	d7 , d0 , d15
	add	CO1, CO1, #32

	fstd	d5 , [CO2, #8 ]
	fstd	d6 , [CO2, #16 ]
	fstd	d7 , [CO2, #24 ]


.endm


/******************************************************************************/

.macro INIT2x2

	fldd			d8, FP_ZERO
	vmov.f64		d9, d8
	vmov.f64		d12, d8
	vmov.f64		d13, d8

.endm

.macro KERNEL2x2_SUB

	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]

	fmacd	d8  , d0,  d4
	fmacd	d9  , d1,  d4

	fmacd	d12  , d0,  d5
	fmacd	d13  , d1,  d5

	add	AO , AO, #16
	add	BO , BO, #16

.endm

.macro SAVE2x2

	ldr	r3  , LDC
	add	CO2 , CO1, r3

	fldd		d0, ALPHA

	fldd	d4 , [CO1]
	fldd	d5 , [CO1, #8 ]

	fmacd	d4 , d0 , d8
	fmacd	d5 , d0 , d9

	fstd	d4 , [CO1]
	fstd	d5 , [CO1, #8 ]

	fldd	d4 , [CO2]
	fldd	d5 , [CO2, #8 ]

	fmacd	d4 , d0 , d12
	fmacd	d5 , d0 , d13

	fstd	d4 , [CO2]
	fstd	d5 , [CO2, #8 ]

	add	CO1, CO1, #16

.endm


/******************************************************************************/

.macro INIT1x2

	fldd			d8, FP_ZERO
	vmov.f64		d12, d8

.endm

.macro KERNEL1x2_SUB

	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]

	fldd	d0 , [ AO ]

	fmacd	d8  , d0,  d4

	fmacd	d12  , d0,  d5

	add	AO , AO, #8
	add	BO , BO, #16

.endm

.macro SAVE1x2

	ldr	r3  , LDC
	add	CO2 , CO1, r3

	fldd		d0, ALPHA

	fldd	d4 , [CO1]

	fmacd	d4 , d0 , d8

	fstd	d4 , [CO1]

	fldd	d4 , [CO2]

	fmacd	d4 , d0 , d12

	fstd	d4 , [CO2]

	add	CO1, CO1, #8

.endm



/******************************************************************************/

.macro INIT4x1

	fldd			d8, FP_ZERO
	vmov.f64		d9, d8
	vmov.f64		d10, d8
	vmov.f64		d11, d8

.endm



.macro KERNEL4x1_SUB

	fldd	d4 , [ BO ]

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]
	fldd	d2 , [ AO, #16 ]
	fldd	d3 , [ AO, #24 ]

	fmacd	d8  , d0,  d4
	fmacd	d9  , d1,  d4
	fmacd	d10 , d2,  d4
	fmacd	d11 , d3,  d4

	add	AO , AO, #32
	add	BO , BO, #8

.endm

.macro SAVE4x1

	fldd		d0, ALPHA

	fldd	d4 , [CO1]
	fldd	d5 , [CO1, #8 ]
	fldd	d6 , [CO1, #16 ]
	fldd	d7 , [CO1, #24 ]

	fmacd	d4 , d0 , d8
	fmacd	d5 , d0 , d9
	fmacd	d6 , d0 , d10
	fmacd	d7 , d0 , d11

	fstd	d4 , [CO1]
	fstd	d5 , [CO1, #8 ]
	fstd	d6 , [CO1, #16 ]
	fstd	d7 , [CO1, #24 ]

	add	CO1, CO1, #32

.endm

/******************************************************************************/

.macro INIT2x1

	fldd			d8, FP_ZERO
	vmov.f64		d9 , d8

.endm

.macro KERNEL2x1_SUB

	fldd	d4 , [ BO ]

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]

	fmacd	d8  , d0,  d4
	fmacd	d9  , d1,  d4

	add	AO , AO, #16
	add	BO , BO, #8

.endm

.macro SAVE2x1

	fldd		d0, ALPHA

	fldd	d4 , [CO1]
	fldd	d5 , [CO1, #8 ]

	fmacd	d4 , d0 , d8
	fmacd	d5 , d0 , d9

	fstd	d4 , [CO1]
	fstd	d5 , [CO1, #8 ]

	add	CO1, CO1, #16

.endm


/******************************************************************************/

.macro INIT1x1

	fldd			d8, FP_ZERO

.endm

.macro KERNEL1x1_SUB

	fldd	d4 , [ BO ]

	fldd	d0 , [ AO ]

	fmacd	d8  , d0,  d4

	add	AO , AO, #8
	add	BO , BO, #8

.endm

.macro SAVE1x1

	fldd		d0, ALPHA

	fldd	d4 , [CO1]

	fmacd	d4 , d0 , d8

	fstd	d4 , [CO1]

	add	CO1, CO1, #8

.endm


/**************************************************************************************
* End of macro definitions
**************************************************************************************/

	PROLOGUE

	.align 5

	push	{r4 - r9, fp}
	add	fp, sp, #24
	sub	sp, sp, #STACKSIZE				// reserve stack

#if !defined(__ARM_PCS_VFP)
	vldr	OLD_ALPHA, OLD_ALPHA_SOFTFP
	ldr	OLD_A, OLD_A_SOFTFP
#endif
	str	OLD_M, M
	str	OLD_N, N
	str	OLD_K, K
	str	OLD_A, A
	vstr	OLD_ALPHA, ALPHA

	sub	r3, fp, #128
	vstm	r3, { d8 - d15} 				// store floating point registers

        movs    r4, #0
        str     r4, FP_ZERO
        str     r4, FP_ZERO_1

	ldr	r3, OLD_LDC
	lsl	r3, r3, #3					// ldc = ldc * 8
	str	r3, LDC

	ldr	K1, K
	ldr	BC, B

	ldr	J, N
	asrs	J, J, #1					// J = J / 2
	ble	dgemm_kernel_L1_BEGIN


/*********************************************************************************************/

dgemm_kernel_L2_BEGIN:

	ldr	CO1, C						// CO1 = C
	ldr	r4 , LDC
	lsl	r4 , r4 , #1					// LDC * 2
	add	r3 , r4, CO1
	str	r3 , C						// store C

	ldr	AO, A						// AO = A

dgemm_kernel_L2_M4_BEGIN:

	ldr	I, M
	asrs	I, I, #2					// I = I / 4
	ble	dgemm_kernel_L2_M2_BEGIN

dgemm_kernel_L2_M4_20:

	INIT4x2

	mov	BO, BC
	asrs	L , K1, #3					// L = L / 8
	ble	dgemm_kernel_L2_M4_40
	.align 5

dgemm_kernel_L2_M4_22:

	pld	[ BO, #B_PRE ]
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	pld	[ BO, #B_PRE ]
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	pld	[ BO, #B_PRE ]
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	pld	[ BO, #B_PRE ]
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	subs	L, L, #1
	bgt	dgemm_kernel_L2_M4_22


dgemm_kernel_L2_M4_40:

	ands	L , K1, #7					// L = L % 8
	ble	dgemm_kernel_L2_M4_100

dgemm_kernel_L2_M4_42:

	KERNEL4x2_SUB

	subs	L, L, #1
	bgt	dgemm_kernel_L2_M4_42

dgemm_kernel_L2_M4_100:

	SAVE4x2

dgemm_kernel_L2_M4_END:

	subs	I, I, #1
	bgt	dgemm_kernel_L2_M4_20


dgemm_kernel_L2_M2_BEGIN:

	ldr	I, M
	tst	I , #3
	ble	dgemm_kernel_L2_END

	tst	I, #2					// I = I / 2
	ble	dgemm_kernel_L2_M1_BEGIN

dgemm_kernel_L2_M2_20:

	INIT2x2

	mov	BO, BC
	asrs	L , K1, #3					// L = L / 8
	ble	dgemm_kernel_L2_M2_40

dgemm_kernel_L2_M2_22:

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	subs	L, L, #1
	bgt	dgemm_kernel_L2_M2_22


dgemm_kernel_L2_M2_40:

	ands	L , K1, #7					// L = L % 8
	ble	dgemm_kernel_L2_M2_100

dgemm_kernel_L2_M2_42:

	KERNEL2x2_SUB

	subs	L, L, #1
	bgt	dgemm_kernel_L2_M2_42

dgemm_kernel_L2_M2_100:

	SAVE2x2

dgemm_kernel_L2_M2_END:


dgemm_kernel_L2_M1_BEGIN:

	tst	I, #1					// I = I % 2
	ble	dgemm_kernel_L2_END

dgemm_kernel_L2_M1_20:

	INIT1x2

	mov	BO, BC
	asrs	L , K1, #3					// L = L / 8
	ble	dgemm_kernel_L2_M1_40

dgemm_kernel_L2_M1_22:
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	subs	L, L, #1
	bgt	dgemm_kernel_L2_M1_22


dgemm_kernel_L2_M1_40:

	ands	L , K1, #7					// L = L % 8
	ble	dgemm_kernel_L2_M1_100

dgemm_kernel_L2_M1_42:

	KERNEL1x2_SUB

	subs	L, L, #1
	bgt	dgemm_kernel_L2_M1_42

dgemm_kernel_L2_M1_100:

	SAVE1x2


dgemm_kernel_L2_END:

	mov	r3, BC
	mov	r4, K1
	lsl	r4, r4, #4					// k * 2 * 8
	add	r3, r3, r4					// B = B + K * 2 * 8
	mov	BC, r3

	subs	J , #1						// j--
	bgt	dgemm_kernel_L2_BEGIN

/*********************************************************************************************/

dgemm_kernel_L1_BEGIN:

	ldr	J , N
	tst	J , #1
	ble	dgemm_kernel_L999


	ldr	CO1, C						// CO1 = C
	ldr	r4 , LDC
	add	r3 , r4, CO1
	str	r3 , C						// store C

	ldr	AO, A						// AO = A



dgemm_kernel_L1_M4_BEGIN:

	ldr	I, M
	asrs	I, I, #2					// I = I / 4
	ble	dgemm_kernel_L1_M2_BEGIN

dgemm_kernel_L1_M4_20:

	INIT4x1

	mov	BO, BC
	asrs	L , K1, #3					// L = L / 8
	ble	dgemm_kernel_L1_M4_40
	.align 5

dgemm_kernel_L1_M4_22:
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	subs	L, L, #1
	bgt	dgemm_kernel_L1_M4_22


dgemm_kernel_L1_M4_40:

	ands	L , K1, #7					// L = L % 8
	ble	dgemm_kernel_L1_M4_100

dgemm_kernel_L1_M4_42:

	KERNEL4x1_SUB

	subs	L, L, #1
	bgt	dgemm_kernel_L1_M4_42

dgemm_kernel_L1_M4_100:

	SAVE4x1

dgemm_kernel_L1_M4_END:

	subs	I, I, #1
	bgt	dgemm_kernel_L1_M4_20


dgemm_kernel_L1_M2_BEGIN:

	ldr	I, M
	tst	I , #3
	ble	dgemm_kernel_L1_END

	tst	I, #2					// I = I / 2
	ble	dgemm_kernel_L1_M1_BEGIN

dgemm_kernel_L1_M2_20:

	INIT2x1

	mov	BO, BC
	asrs	L , K1, #3					// L = L / 8
	ble	dgemm_kernel_L1_M2_40

dgemm_kernel_L1_M2_22:

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	subs	L, L, #1
	bgt	dgemm_kernel_L1_M2_22


dgemm_kernel_L1_M2_40:

	ands	L , K1, #7					// L = L % 8
	ble	dgemm_kernel_L1_M2_100

dgemm_kernel_L1_M2_42:

	KERNEL2x1_SUB

	subs	L, L, #1
	bgt	dgemm_kernel_L1_M2_42

dgemm_kernel_L1_M2_100:

	SAVE2x1

dgemm_kernel_L1_M2_END:


dgemm_kernel_L1_M1_BEGIN:

	tst	I, #1					// I = I % 2
	ble	dgemm_kernel_L1_END

dgemm_kernel_L1_M1_20:

	INIT1x1

	mov	BO, BC
	asrs	L , K1, #3					// L = L / 8
	ble	dgemm_kernel_L1_M1_40

dgemm_kernel_L1_M1_22:
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	subs	L, L, #1
	bgt	dgemm_kernel_L1_M1_22


dgemm_kernel_L1_M1_40:

	ands	L , K1, #7					// L = L % 8
	ble	dgemm_kernel_L1_M1_100

dgemm_kernel_L1_M1_42:

	KERNEL1x1_SUB

	subs	L, L, #1
	bgt	dgemm_kernel_L1_M1_42

dgemm_kernel_L1_M1_100:

	SAVE1x1


dgemm_kernel_L1_END:


dgemm_kernel_L999:

	sub	r3, fp, #128
	vldm	r3, { d8 - d15}					// restore floating point registers

	movs	r0, #0						// set return value
	sub	sp, fp, #24
	pop	{r4 - r9, fp}
	bx	lr

	EPILOGUE

