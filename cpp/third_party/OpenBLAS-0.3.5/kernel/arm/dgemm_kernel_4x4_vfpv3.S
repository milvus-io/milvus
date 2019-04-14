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
* 2013/11/23 Saar
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*
*
* 2013/10/11 Saar
*	UNROLL_N		4
*	UNROLL_M		4
*	DGEMM_P			128
*	DGEMM_Q			96
*	DGEMM_R			512
*	A_PRE			96
*	B_PRE			96
*	C_PRE			64
*
* 	Performance on Odroid U2:
*
*		1 Core:		1.57 GFLOPS	ATLAS: 1.59	GFLOPS
*		2 Cores:	3.14 GFLOPS	ATLAS: 3.16	GFLOPS
*		3 Cores:	4.56 GFLOPS	ATLAS: 4.60	GFLOPS
*		4 Cores:	5.82 GFLOPS	ATLAS: 5.41	GFLOPS
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
#define C_PRE	64

/**************************************************************************************
* Macro definitions
**************************************************************************************/

.macro INIT4x4

	fldd			d16, FP_ZERO
	vmov.f64		d17, d16
	vmov.f64		d18, d16
	vmov.f64		d19, d16
	vmov.f64		d20, d16
	vmov.f64		d21, d16
	vmov.f64		d22, d16
	vmov.f64		d23, d16
	vmov.f64		d24, d16
	vmov.f64		d25, d16
	vmov.f64		d26, d16
	vmov.f64		d27, d16
	vmov.f64		d28, d16
	vmov.f64		d29, d16
	vmov.f64		d30, d16
	vmov.f64		d31, d16

.endm

.macro KERNEL4x4_I
	pld	[ BO , #B_PRE ]
	fldd	d8 , [ BO ]
	fldd	d0 , [ AO ]
	pld	[ AO , #A_PRE ]

	fldd	d1 , [ AO, #8 ]
	fmuld	d16  , d0,  d8
	fldd	d2 , [ AO, #16 ]
	fmuld	d17  , d1,  d8
	fldd	d3 , [ AO, #24 ]
	fmuld	d18  , d2,  d8
	fldd	d9 , [ BO, #8 ]
	fmuld	d19  , d3,  d8

	fldd	d10, [ BO, #16 ]
	fmuld	d20  , d0,  d9
	fldd	d11, [ BO, #24 ]
	fmuld	d21  , d1,  d9
	add	BO , BO, #32
	add	AO , AO, #32
	fmuld	d22  , d2,  d9

	pld	[ BO , #B_PRE ]
	fldd	d12, [ BO ]
	fmuld	d23  , d3,  d9

	pld	[ AO , #A_PRE ]
	fldd	d4 , [ AO, #0 ]
	fmuld	d24  , d0,  d10
	fldd	d5 , [ AO, #8 ]
	fmuld	d25  , d1,  d10
	fldd	d6 , [ AO, #16 ]
	fmuld	d26  , d2,  d10
	fldd	d7 , [ AO, #24 ]
	fmuld	d27  , d3,  d10

	fldd	d13, [ BO, #8 ]
	fmuld	d28  , d0,  d11
	fldd	d14, [ BO, #16 ]
	fmuld	d29  , d1,  d11
	fldd	d15, [ BO, #24 ]
	fmuld	d30  , d2,  d11
	fmuld	d31  , d3,  d11

.endm



.macro KERNEL4x4_M2

	fmacd	d16  , d4,  d12
	pld	[ AO , #A_PRE+32 ]
	fmacd	d17  , d5,  d12
	fldd	d0 , [ AO , #32 ]
	fmacd	d18  , d6,  d12
	pld	[ BO , #B_PRE+32 ]
	fmacd	d19  , d7,  d12

	fldd	d8 , [ BO , #32 ]
	fmacd	d20  , d4,  d13
	fldd	d1 , [ AO, #40 ]
	fmacd	d21  , d5,  d13
	fldd	d2 , [ AO, #48 ]
	fmacd	d22  , d6,  d13
	fldd	d3 , [ AO, #56 ]
	fmacd	d23  , d7,  d13

	fmacd	d24  , d4,  d14
	fmacd	d25  , d5,  d14
	fldd	d9 , [ BO, #40 ]
	fmacd	d26  , d6,  d14
	fldd	d10, [ BO, #48 ]
	fmacd	d27  , d7,  d14

	fldd	d11, [ BO, #56 ]
	fmacd	d28  , d4,  d15
	fmacd	d29  , d5,  d15
	add	AO , AO, #64
	fmacd	d30  , d6,  d15
	add	BO , BO, #64
	fmacd	d31  , d7,  d15

.endm


.macro KERNEL4x4_M1

	fmacd	d16  , d0,  d8
	pld	[ AO , #A_PRE ]
	fmacd	d17  , d1,  d8
	fldd	d4 , [ AO ]
	fmacd	d18  , d2,  d8
	pld	[ BO , #B_PRE ]
	fmacd	d19  , d3,  d8

	fldd	d12, [ BO ]
	fmacd	d20  , d0,  d9
	fldd	d5 , [ AO, #8 ]
	fmacd	d21  , d1,  d9
	fldd	d6 , [ AO, #16 ]
	fmacd	d22  , d2,  d9
	fldd	d7 , [ AO, #24 ]
	fmacd	d23  , d3,  d9

	fmacd	d24  , d0,  d10
	fmacd	d25  , d1,  d10
	fldd	d13, [ BO, #8 ]
	fmacd	d26  , d2,  d10
	fldd	d14, [ BO, #16 ]
	fmacd	d27  , d3,  d10

	fldd	d15, [ BO, #24 ]
	fmacd	d28  , d0,  d11
	fmacd	d29  , d1,  d11
	fmacd	d30  , d2,  d11
	fmacd	d31  , d3,  d11

.endm



.macro KERNEL4x4_E

	fmacd	d16  , d4,  d12
	fmacd	d17  , d5,  d12
	add	BO , BO, #32
	add	AO , AO, #32
	fmacd	d18  , d6,  d12
	fmacd	d19  , d7,  d12

	fmacd	d20  , d4,  d13
	fmacd	d21  , d5,  d13
	fmacd	d22  , d6,  d13
	fmacd	d23  , d7,  d13

	fmacd	d24  , d4,  d14
	fmacd	d25  , d5,  d14
	fmacd	d26  , d6,  d14
	fmacd	d27  , d7,  d14

	fmacd	d28  , d4,  d15
	fmacd	d29  , d5,  d15
	fmacd	d30  , d6,  d15
	fmacd	d31  , d7,  d15

.endm




.macro KERNEL4x4_SUB

	fldd	d8 , [ BO ]
	pld	[ BO , #B_PRE ]

	fldd	d0 , [ AO ]
	pld	[ AO , #A_PRE ]
	fldd	d1 , [ AO, #8 ]

	fmacd	d16  , d0,  d8
	fldd	d2 , [ AO, #16 ]
	fmacd	d17  , d1,  d8
	fldd	d3 , [ AO, #24 ]
	fmacd	d18  , d2,  d8
	fldd	d9 , [ BO, #8 ]
	fmacd	d19  , d3,  d8

	fldd	d10, [ BO, #16 ]
	fmacd	d20  , d0,  d9
	fldd	d11, [ BO, #24 ]
	fmacd	d21  , d1,  d9
	fmacd	d22  , d2,  d9
	fmacd	d23  , d3,  d9

	fmacd	d24  , d0,  d10
	fmacd	d25  , d1,  d10
	fmacd	d26  , d2,  d10
	fmacd	d27  , d3,  d10

	fmacd	d28  , d0,  d11
	fmacd	d29  , d1,  d11
	add	AO , AO, #32
	fmacd	d30  , d2,  d11
	add	BO , BO, #32
	fmacd	d31  , d3,  d11

.endm

.macro SAVE4x4
	pld	[ CO1 , #C_PRE ]

	ldr	r3  , LDC
	add	CO2 , CO1, r3
	fldd		d0, ALPHA
	add	r4  , CO2, r3
	pld	[ CO2 , #C_PRE ]

	vldmia.f64 CO1, { d8 - d11 }
	pld	[ r4 , #C_PRE ]

	fmacd	d8 , d0 , d16
	fldd	d12, [CO2]
	fmacd	d9 , d0 , d17
	fldd	d13, [CO2, #8 ]
	fmacd	d10, d0 , d18
	fldd	d14, [CO2, #16 ]
	fmacd	d11, d0 , d19
	fldd	d15, [CO2, #24 ]

	fmacd	d12, d0 , d20
	fstd	d8 , [CO1]
	fmacd	d13, d0 , d21
	fstd	d9 , [CO1, #8 ]
	fmacd	d14, d0 , d22
	fstd	d10, [CO1, #16 ]
	fmacd	d15, d0 , d23
	fstd	d11, [CO1, #24 ]

	vldmia.f64 r4, { d8 - d11 }

	fmacd	d8 , d0 , d24
	fstd	d12, [CO2]
	fmacd	d9 , d0 , d25
	fstd	d13, [CO2, #8 ]
	fmacd	d10, d0 , d26
	fstd	d14, [CO2, #16 ]
	fmacd	d11, d0 , d27
	fstd	d15, [CO2, #24 ]

	add	CO2, r4 , r3

	pld	[ CO2 , #C_PRE ]

	vldmia.f64 CO2, { d12 - d15 }

	fstd	d8 , [r4 ]
	fmacd	d12, d0 , d28
	fstd	d9 , [r4 , #8 ]
	fmacd	d13, d0 , d29
	fstd	d10, [r4 , #16 ]
	fmacd	d14, d0 , d30
	fstd	d11, [r4 , #24 ]
	fmacd	d15, d0 , d31

	vstmia.f64 CO2, { d12 - d15 }

	add	CO1, CO1, #32

.endm

/******************************************************************************/

.macro INIT2x4

	fldd			d16, FP_ZERO
	vmov.f64		d17, d16
	vmov.f64		d20, d16
	vmov.f64		d21, d16
	vmov.f64		d24, d16
	vmov.f64		d25, d16
	vmov.f64		d28, d16
	vmov.f64		d29, d16

.endm



.macro KERNEL2x4_SUB

	fldd	d8 , [ BO ]
	fldd	d9 , [ BO, #8 ]
	fldd	d10, [ BO, #16 ]
	fldd	d11, [ BO, #24 ]

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]

	fmacd	d16  , d0,  d8
	fmacd	d17  , d1,  d8

	fmacd	d20  , d0,  d9
	fmacd	d21  , d1,  d9

	fmacd	d24  , d0,  d10
	fmacd	d25  , d1,  d10

	fmacd	d28  , d0,  d11
	fmacd	d29  , d1,  d11
	add	AO , AO, #16
	add	BO , BO, #32

.endm

.macro SAVE2x4

	ldr	r3  , LDC
	add	CO2 , CO1, r3
	add	r4  , CO2, r3

	fldd		d0, ALPHA

	fldd	d8 , [CO1]
	fldd	d9 , [CO1, #8 ]

	fmacd	d8 , d0 , d16
	fmacd	d9 , d0 , d17

	fstd	d8 , [CO1]
	fstd	d9 , [CO1, #8 ]

	fldd	d12, [CO2]
	fldd	d13, [CO2, #8 ]

	fmacd	d12, d0 , d20
	fmacd	d13, d0 , d21

	fstd	d12, [CO2]
	fstd	d13, [CO2, #8 ]

	fldd	d8 , [r4 ]
	fldd	d9 , [r4 , #8 ]

	fmacd	d8 , d0 , d24
	fmacd	d9 , d0 , d25

	fstd	d8 , [r4 ]
	fstd	d9 , [r4 , #8 ]

	add	CO2, r4 , r3

	fldd	d12, [CO2]
	fldd	d13, [CO2, #8 ]

	fmacd	d12, d0 , d28
	fmacd	d13, d0 , d29

	fstd	d12, [CO2]
	fstd	d13, [CO2, #8 ]

	add	CO1, CO1, #16

.endm


/******************************************************************************/

.macro INIT1x4

	fldd			d16, FP_ZERO
	vmov.f64		d20, d16
	vmov.f64		d24, d16
	vmov.f64		d28, d16

.endm



.macro KERNEL1x4_SUB

	fldd	d8 , [ BO ]
	fldd	d9 , [ BO, #8 ]
	fldd	d10, [ BO, #16 ]
	fldd	d11, [ BO, #24 ]

	fldd	d0 , [ AO ]

	fmacd	d16  , d0,  d8
	fmacd	d20  , d0,  d9
	fmacd	d24  , d0,  d10
	fmacd	d28  , d0,  d11

	add	AO , AO, #8
	add	BO , BO, #32

.endm

.macro SAVE1x4

	ldr	r3  , LDC
	add	CO2 , CO1, r3
	add	r4  , CO2, r3

	fldd		d0, ALPHA

	fldd	d8 , [CO1]
	fmacd	d8 , d0 , d16
	fstd	d8 , [CO1]

	fldd	d12, [CO2]
	fmacd	d12, d0 , d20
	fstd	d12, [CO2]

	fldd	d8 , [r4 ]
	fmacd	d8 , d0 , d24
	fstd	d8 , [r4 ]

	add	CO2, r4 , r3

	fldd	d12, [CO2]
	fmacd	d12, d0 , d28
	fstd	d12, [CO2]

	add	CO1, CO1, #8

.endm

/******************************************************************************/
/******************************************************************************/

.macro INIT4x2

	fldd			d16, FP_ZERO
	vmov.f64		d17, d16
	vmov.f64		d18, d16
	vmov.f64		d19, d16
	vmov.f64		d20, d16
	vmov.f64		d21, d16
	vmov.f64		d22, d16
	vmov.f64		d23, d16

.endm



.macro KERNEL4x2_SUB

	fldd	d8 , [ BO ]
	fldd	d9 , [ BO, #8 ]

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]
	fldd	d2 , [ AO, #16 ]
	fldd	d3 , [ AO, #24 ]

	fmacd	d16  , d0,  d8
	fmacd	d17  , d1,  d8
	fmacd	d18  , d2,  d8
	fmacd	d19  , d3,  d8

	fmacd	d20  , d0,  d9
	fmacd	d21  , d1,  d9
	fmacd	d22  , d2,  d9
	fmacd	d23  , d3,  d9

	add	AO , AO, #32
	add	BO , BO, #16

.endm

.macro SAVE4x2

	ldr	r3  , LDC
	add	CO2 , CO1, r3

	fldd		d0, ALPHA

	fldd	d8 , [CO1]
	fldd	d9 , [CO1, #8 ]
	fldd	d10, [CO1, #16 ]
	fldd	d11, [CO1, #24 ]

	fmacd	d8 , d0 , d16
	fmacd	d9 , d0 , d17
	fmacd	d10, d0 , d18
	fmacd	d11, d0 , d19

	fstd	d8 , [CO1]
	fstd	d9 , [CO1, #8 ]
	fstd	d10, [CO1, #16 ]
	fstd	d11, [CO1, #24 ]

	fldd	d12, [CO2]
	fldd	d13, [CO2, #8 ]
	fldd	d14, [CO2, #16 ]
	fldd	d15, [CO2, #24 ]

	fmacd	d12, d0 , d20
	fmacd	d13, d0 , d21
	fmacd	d14, d0 , d22
	fmacd	d15, d0 , d23

	fstd	d12, [CO2]
	fstd	d13, [CO2, #8 ]
	fstd	d14, [CO2, #16 ]
	fstd	d15, [CO2, #24 ]

	add	CO1, CO1, #32

.endm


/******************************************************************************/

.macro INIT2x2

	fldd			d16, FP_ZERO
	vmov.f64		d17, d16
	vmov.f64		d20, d16
	vmov.f64		d21, d16

.endm



.macro KERNEL2x2_SUB

	fldd	d8 , [ BO ]
	fldd	d9 , [ BO, #8 ]

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]

	fmacd	d16  , d0,  d8
	fmacd	d17  , d1,  d8

	fmacd	d20  , d0,  d9
	fmacd	d21  , d1,  d9

	add	AO , AO, #16
	add	BO , BO, #16

.endm

.macro SAVE2x2

	ldr	r3  , LDC
	add	CO2 , CO1, r3

	fldd		d0, ALPHA

	fldd	d8 , [CO1]
	fldd	d9 , [CO1, #8 ]

	fmacd	d8 , d0 , d16
	fmacd	d9 , d0 , d17

	fstd	d8 , [CO1]
	fstd	d9 , [CO1, #8 ]

	fldd	d12, [CO2]
	fldd	d13, [CO2, #8 ]

	fmacd	d12, d0 , d20
	fmacd	d13, d0 , d21

	fstd	d12, [CO2]
	fstd	d13, [CO2, #8 ]

	add	CO1, CO1, #16

.endm

/******************************************************************************/

.macro INIT1x2

	fldd			d16, FP_ZERO
	vmov.f64		d20, d16

.endm



.macro KERNEL1x2_SUB

	fldd	d8 , [ BO ]
	fldd	d9 , [ BO, #8 ]

	fldd	d0 , [ AO ]
	fmacd	d16  , d0,  d8
	fmacd	d20  , d0,  d9

	add	AO , AO, #8
	add	BO , BO, #16

.endm

.macro SAVE1x2

	ldr	r3  , LDC
	add	CO2 , CO1, r3

	fldd		d0, ALPHA

	fldd	d8 , [CO1]
	fmacd	d8 , d0 , d16
	fstd	d8 , [CO1]

	fldd	d12, [CO2]
	fmacd	d12, d0 , d20
	fstd	d12, [CO2]

	add	CO1, CO1, #8

.endm

/******************************************************************************/
/******************************************************************************/

.macro INIT4x1

	fldd			d16, FP_ZERO
	vmov.f64		d17, d16
	vmov.f64		d18, d16
	vmov.f64		d19, d16

.endm



.macro KERNEL4x1_SUB

	fldd	d8 , [ BO ]

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]
	fldd	d2 , [ AO, #16 ]
	fldd	d3 , [ AO, #24 ]

	fmacd	d16  , d0,  d8
	fmacd	d17  , d1,  d8
	fmacd	d18  , d2,  d8
	fmacd	d19  , d3,  d8

	add	AO , AO, #32
	add	BO , BO, #8

.endm

.macro SAVE4x1


	fldd		d0, ALPHA

	fldd	d8 , [CO1]
	fldd	d9 , [CO1, #8 ]
	fldd	d10, [CO1, #16 ]
	fldd	d11, [CO1, #24 ]

	fmacd	d8 , d0 , d16
	fmacd	d9 , d0 , d17
	fmacd	d10, d0 , d18
	fmacd	d11, d0 , d19

	fstd	d8 , [CO1]
	fstd	d9 , [CO1, #8 ]
	fstd	d10, [CO1, #16 ]
	fstd	d11, [CO1, #24 ]

	add	CO1, CO1, #32

.endm




/******************************************************************************/

.macro INIT2x1

	fldd			d16, FP_ZERO
	vmov.f64		d17, d16

.endm



.macro KERNEL2x1_SUB

	fldd	d8 , [ BO ]

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]

	fmacd	d16  , d0,  d8
	fmacd	d17  , d1,  d8

	add	AO , AO, #16
	add	BO , BO, #8

.endm

.macro SAVE2x1


	fldd		d0, ALPHA

	fldd	d8 , [CO1]
	fldd	d9 , [CO1, #8 ]

	fmacd	d8 , d0 , d16
	fmacd	d9 , d0 , d17

	fstd	d8 , [CO1]
	fstd	d9 , [CO1, #8 ]

	add	CO1, CO1, #16

.endm

/******************************************************************************/

.macro INIT1x1

	fldd			d16, FP_ZERO

.endm



.macro KERNEL1x1_SUB

	fldd	d8 , [ BO ]

	fldd	d0 , [ AO ]

	fmacd	d16  , d0,  d8

	add	AO , AO, #8
	add	BO , BO, #8

.endm

.macro SAVE1x1


	fldd		d0, ALPHA

	fldd	d8 , [CO1]
	fmacd	d8 , d0 , d16
	fstd	d8 , [CO1]

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

	movs	r4, #0
	str	r4, FP_ZERO
	str	r4, FP_ZERO_1

	sub	r3, fp, #128
	vstm	r3, { d8 - d15} 				// store floating point registers

	ldr	r3, OLD_LDC
	lsl	r3, r3, #3					// ldc = ldc * 8
	str	r3, LDC

	ldr	K1, K
	ldr	BC, B

	ldr	J, N
	asrs	J, J, #2					// J = J / 4
	ble	dgemm_kernel_L2_BEGIN

dgemm_kernel_L4_BEGIN:

	ldr	CO1, C						// CO1 = C
	ldr	r4 , LDC
	lsl	r4 , r4 , #2					// LDC * 4
	add	r3 , r4, CO1
	str	r3 , C						// store C

	ldr	AO, A						// AO = A
        pld     [AO , #A_PRE-64]
        pld     [AO , #A_PRE-32]



dgemm_kernel_L4_M4_BEGIN:

	ldr	I, M
	asrs	I, I, #2					// I = I / 4
	ble	dgemm_kernel_L4_M2_BEGIN

dgemm_kernel_L4_M4_20:


	mov	BO, BC
	asrs	L , K1, #3					// L = L / 8
	cmp	L , #2
	blt	dgemm_kernel_L4_M4_32


	KERNEL4x4_I
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	subs	L, L, #2
	ble	dgemm_kernel_L4_M4_22a
	.align 5

dgemm_kernel_L4_M4_22:

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	subs	L, L, #1
	bgt	dgemm_kernel_L4_M4_22

dgemm_kernel_L4_M4_22a:

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_E

	b	 dgemm_kernel_L4_M4_44

dgemm_kernel_L4_M4_32:

	tst	L, #1
	ble	dgemm_kernel_L4_M4_40

	KERNEL4x4_I
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_E

	b	 dgemm_kernel_L4_M4_44


dgemm_kernel_L4_M4_40:

	INIT4x4


dgemm_kernel_L4_M4_44:

	ands	L , K1, #7					// L = L % 8
	ble	dgemm_kernel_L4_M4_100

dgemm_kernel_L4_M4_46:

	KERNEL4x4_SUB

	subs	L, L, #1
	bne	dgemm_kernel_L4_M4_46

dgemm_kernel_L4_M4_100:

	SAVE4x4

dgemm_kernel_L4_M4_END:

	subs	I, I, #1
	bne	dgemm_kernel_L4_M4_20


dgemm_kernel_L4_M2_BEGIN:

	ldr	I, M
	tst	I , #3
	ble	dgemm_kernel_L4_END

	tst	I, #2					// I = I / 2
	ble	dgemm_kernel_L4_M1_BEGIN

dgemm_kernel_L4_M2_20:

	INIT2x4

	mov	BO, BC
	asrs	L , K1, #3					// L = L / 8
	ble	dgemm_kernel_L4_M2_40

dgemm_kernel_L4_M2_22:

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	subs	L, L, #1
	bgt	dgemm_kernel_L4_M2_22


dgemm_kernel_L4_M2_40:

	ands	L , K1, #7					// L = L % 8
	ble	dgemm_kernel_L4_M2_100

dgemm_kernel_L4_M2_42:

	KERNEL2x4_SUB

	subs	L, L, #1
	bgt	dgemm_kernel_L4_M2_42

dgemm_kernel_L4_M2_100:

	SAVE2x4

dgemm_kernel_L4_M2_END:


dgemm_kernel_L4_M1_BEGIN:

	tst	I, #1					// I = I % 2
	ble	dgemm_kernel_L4_END

dgemm_kernel_L4_M1_20:

	INIT1x4

	mov	BO, BC
	asrs	L , K1, #3					// L = L / 8
	ble	dgemm_kernel_L4_M1_40

dgemm_kernel_L4_M1_22:
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	subs	L, L, #1
	bgt	dgemm_kernel_L4_M1_22


dgemm_kernel_L4_M1_40:

	ands	L , K1, #7					// L = L % 8
	ble	dgemm_kernel_L4_M1_100

dgemm_kernel_L4_M1_42:

	KERNEL1x4_SUB

	subs	L, L, #1
	bgt	dgemm_kernel_L4_M1_42

dgemm_kernel_L4_M1_100:

	SAVE1x4


dgemm_kernel_L4_END:

	mov	r3, BC
	mov	r4, K1
	lsl	r4, r4, #5					// k * 4 * 8
	add	r3, r3, r4					// B = B + K * 4 * 8
	mov	BC, r3

	subs	J , #1						// j--
	bgt	dgemm_kernel_L4_BEGIN



/*********************************************************************************************/

dgemm_kernel_L2_BEGIN:

	ldr	J , N
	tst	J , #3
	ble	dgemm_kernel_L999

	tst	J , #2
	ble	dgemm_kernel_L1_BEGIN

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
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	KERNEL4x2_SUB
	KERNEL4x2_SUB
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

