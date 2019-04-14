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
* 2013/11/05 Saar
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*
* 2013/11/02 Saar
*       UNROLL_N                2
*       UNROLL_M                2
*       ZGEMM_P                 64
*       ZGEMM_Q                 120
*       ZGEMM_R                 4096
*       A_PRE                   96
*       B_PRE                   96
*       C_PRE                   64
*
*       Performance on Odroid U2:
*
*               1 Core:         1.62 GFLOPS     ATLAS: 1.39     GFLOPS
*               2 Cores:        3.20 GFLOPS     ATLAS: 2.54     GFLOPS
*               3 Cores:        4.72 GFLOPS     ATLAS: 3.76     GFLOPS
*               4 Cores:        5.93 GFLOPS     ATLAS: 4.88     GFLOPS
**************************************************************************************/

#define ASSEMBLER
#include "common.h"

#define STACKSIZE 256

#define	OLD_M	r0
#define	OLD_N	r1
#define	OLD_K	r2
#define	OLD_A	r3
#define OLD_ALPHA_R d0
#define OLD_ALPHA_I d1

/******************************************************
* [fp, #-128] - [fp, #-64] is reserved
* for store and restore of floating point
* registers
*******************************************************/

#define A	[fp, #-248 ]
#define LDC	[fp, #-252 ]
#define M	[fp, #-256 ]
#define N	[fp, #-260 ]
#define K	[fp, #-264 ]

#define FP_ZERO [fp, #-240]
#define FP_ZERO_0 [fp, # -240]
#define FP_ZERO_1 [fp, # -236]

#define ALPHA_I	[fp, #-272]
#define ALPHA_R	[fp, #-280]

#if !defined(__ARM_PCS_VFP)
#define OLD_ALPHAR_SOFTFP	[fp, #4]
#define OLD_ALPHAI_SOFTFP	[fp, #12]
#define OLD_A_SOFTFP	[fp, #20 ]
#define B	[fp, #24 ]
#define C	[fp, #28 ]
#define OLD_LDC	[fp, #32 ]
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

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)

	#define	FADD_R	fsubd
	#define	FADD_I	faddd

	#define	FMAC_R1	vmls.f64
	#define	FMAC_R2	vmls.f64
	#define	FMAC_I1	fmacd
	#define	FMAC_I2	vmls.f64

#elif defined(CN) || defined(CT)

	#define	FADD_R	faddd
	#define	FADD_I	fsubd

	#define	FMAC_R1	fmacd
	#define	FMAC_R2	fmacd
	#define	FMAC_I1	vmls.f64
	#define	FMAC_I2	fmacd

#elif defined(NC) || defined(TC)

	#define	FADD_R	faddd
	#define	FADD_I	fsubd

	#define	FMAC_R1	fmacd
	#define	FMAC_R2	vmls.f64
	#define	FMAC_I1	fmacd
	#define	FMAC_I2	fmacd

#else

	#define	FADD_R  fsubd
	#define	FADD_I	faddd

	#define	FMAC_R1	vmls.f64
	#define	FMAC_R2	fmacd
	#define	FMAC_I1	vmls.f64
	#define	FMAC_I2	vmls.f64

#endif



/**************************************************************************************
* Macro definitions
**************************************************************************************/

.macro INIT2x2

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

.macro KERNEL2x2_I
	pld	[ AO , #A_PRE ]
	pld	[ BO , #B_PRE ]
	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]
	fldd	d8 , [ BO ]
	fldd	d9 , [ BO, #8 ]

	fmuld	d16  , d0,  d8
	fldd	d2 , [ AO, #16 ]
	fmuld	d24  , d1,  d9
	fldd	d3 , [ AO, #24 ]
	fmuld	d17  , d0,  d9
	fldd	d10, [ BO, #16 ]
	fmuld	d25  , d1,  d8

	fldd	d11, [ BO, #24 ]
	fmuld	d18  , d2,  d8
	add	BO , BO, #32
	fmuld	d26  , d3,  d9
	add	AO , AO, #32
	fmuld	d19  , d2,  d9
	pld	[ BO , #B_PRE ]
	fmuld	d27  , d3,  d8

	pld	[ AO , #A_PRE ]
	fmuld	d20  , d0,  d10
	fldd	d4 , [ AO, #0 ]
	fmuld	d28  , d1,  d11
	fldd	d5 , [ AO, #8 ]
	fmuld	d21  , d0,  d11
	fldd	d12, [ BO ]
	fmuld	d29  , d1,  d10

	fldd	d13, [ BO, #8 ]
	fmuld	d22  , d2,  d10
	fldd	d6 , [ AO, #16 ]
	fmuld	d30  , d3,  d11
	fldd	d7 , [ AO, #24 ]
	fmuld	d23  , d2,  d11
	fldd	d14, [ BO, #16 ]
	fmuld	d31  , d3,  d10
	fldd	d15, [ BO, #24 ]

	add	BO , BO, #32
	add	AO , AO, #32
.endm



.macro KERNEL2x2_M1
	pld	[ AO , #A_PRE ]

	fmacd	d16  , d0,  d8
	pld	[ BO , #B_PRE ]
	fmacd	d24  , d1,  d9
	fldd	d4 , [ AO, #0 ]
	fmacd	d17  , d0,  d9
	fldd	d5 , [ AO, #8 ]
	fmacd	d25  , d1,  d8

	fldd	d12, [ BO ]
	fmacd	d18  , d2,  d8
	fldd	d13, [ BO, #8 ]
	fmacd	d26  , d3,  d9
	fldd	d6 , [ AO, #16 ]
	fmacd	d19  , d2,  d9
	fldd	d7 , [ AO, #24 ]
	fmacd	d27  , d3,  d8

	fmacd	d20  , d0,  d10
	fldd	d14, [ BO, #16 ]
	fmacd	d28  , d1,  d11
	fmacd	d21  , d0,  d11
	fldd	d15, [ BO, #24 ]
	fmacd	d29  , d1,  d10

	fmacd	d22  , d2,  d10
	add	BO , BO, #32
	fmacd	d30  , d3,  d11
	fmacd	d23  , d2,  d11
	add	AO , AO, #32
	fmacd	d31  , d3,  d10

.endm

.macro KERNEL2x2_M2
	pld	[ AO , #A_PRE ]

	fmacd	d16  , d4,  d12
	pld	[ BO , #B_PRE ]
	fmacd	d24  , d5,  d13
	fldd	d0 , [ AO, #0 ]
	fmacd	d17  , d4,  d13
	fldd	d1 , [ AO, #8 ]
	fmacd	d25  , d5,  d12

	fmacd	d18  , d6,  d12
	fldd	d8 , [ BO ]
	fmacd	d26  , d7,  d13
	fldd	d9 , [ BO, #8 ]
	fmacd	d19  , d6,  d13
	fmacd	d27  , d7,  d12

	fldd	d2 , [ AO, #16 ]
	fmacd	d20  , d4,  d14
	fldd	d3 , [ AO, #24 ]
	fmacd	d28  , d5,  d15
	fmacd	d21  , d4,  d15
	fldd	d10, [ BO, #16 ]
	fmacd	d29  , d5,  d14

	fldd	d11, [ BO, #24 ]
	fmacd	d22  , d6,  d14
	fmacd	d30  , d7,  d15
	add	BO , BO, #32
	fmacd	d23  , d6,  d15
	add	AO , AO, #32
	fmacd	d31  , d7,  d14

.endm


.macro KERNEL2x2_E

	fmacd	d16  , d4,  d12
	fmacd	d24  , d5,  d13
	fmacd	d17  , d4,  d13
	fmacd	d25  , d5,  d12

	fmacd	d18  , d6,  d12
	fmacd	d26  , d7,  d13
	fmacd	d19  , d6,  d13
	fmacd	d27  , d7,  d12

	fmacd	d20  , d4,  d14
	fmacd	d28  , d5,  d15
	fmacd	d21  , d4,  d15
	fmacd	d29  , d5,  d14

	fmacd	d22  , d6,  d14
	fmacd	d30  , d7,  d15
	fmacd	d23  , d6,  d15
	fmacd	d31  , d7,  d14

.endm

.macro KERNEL2x2_SUB

	pld	[ AO , #A_PRE ]
	pld	[ BO , #B_PRE ]
	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]
	fldd	d8 , [ BO ]
	fldd	d9 , [ BO, #8 ]

	fmacd	d16  , d0,  d8
	fldd	d2 , [ AO, #16 ]
	fmacd	d24  , d1,  d9
	fldd	d3 , [ AO, #24 ]
	fmacd	d17  , d0,  d9
	fldd	d10, [ BO, #16 ]
	fmacd	d25  , d1,  d8

	fldd	d11, [ BO, #24 ]
	fmacd	d18  , d2,  d8
	fmacd	d26  , d3,  d9
	fmacd	d19  , d2,  d9
	fmacd	d27  , d3,  d8

	fmacd	d20  , d0,  d10
	fmacd	d28  , d1,  d11
	fmacd	d21  , d0,  d11
	fmacd	d29  , d1,  d10

	fmacd	d22  , d2,  d10
	add	BO , BO, #32
	fmacd	d30  , d3,  d11
	fmacd	d23  , d2,  d11
	add	AO , AO, #32
	fmacd	d31  , d3,  d10

.endm




.macro SAVE2x2
	pld	[ CO1 , #C_PRE ]

	ldr	r3  , LDC
	add	CO2 , CO1, r3
	fldd		d0, ALPHA_R
	fldd		d1, ALPHA_I

	vldmia.f64 CO1, { d4 - d7 }
	vldmia.f64 CO2, { d8 - d11 }

	FADD_R	d16, d24 , d16
	FADD_I  d17, d25 , d17
	FADD_R	d18, d26 , d18
	FADD_I  d19, d27 , d19
	FADD_R	d20, d28 , d20
	FADD_I  d21, d29 , d21
	FADD_R	d22, d30 , d22
	FADD_I  d23, d31 , d23

	FMAC_R1 d4 , d0 , d16
	FMAC_I1 d5 , d0 , d17
	FMAC_R2 d4 , d1 , d17
	FMAC_I2	d5 , d1 , d16

	FMAC_R1 d6 , d0 , d18
	FMAC_I1 d7 , d0 , d19
	FMAC_R2 d6 , d1 , d19
	FMAC_I2	d7 , d1 , d18

	FMAC_R1 d8 , d0 , d20
	FMAC_I1 d9 , d0 , d21
	FMAC_R2 d8 , d1 , d21
	FMAC_I2	d9 , d1 , d20

	FMAC_R1 d10, d0 , d22
	FMAC_I1 d11, d0 , d23
	FMAC_R2 d10, d1 , d23
	FMAC_I2	d11, d1 , d22

	vstmia.f64 CO1, { d4 - d7 }
	vstmia.f64 CO2, { d8 - d11 }

	add	CO1, CO1, #32

.endm

/******************************************************************************/

.macro INIT1x2

	fldd			d16, FP_ZERO
	vmov.f64		d17, d16
	vmov.f64		d20, d16
	vmov.f64		d21, d16
	vmov.f64		d24, d16
	vmov.f64		d25, d16
	vmov.f64		d28, d16
	vmov.f64		d29, d16

.endm

.macro KERNEL1x2_I
	pld	[ AO , #A_PRE ]
	pld	[ BO , #B_PRE ]
	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]
	fldd	d8 , [ BO ]
	fldd	d9 , [ BO, #8 ]
	fldd	d10, [ BO, #16 ]
	fldd	d11, [ BO, #24 ]

	fmuld	d16  , d0,  d8
	fmuld	d24  , d1,  d9
	fmuld	d17  , d0,  d9
	fmuld	d25  , d1,  d8

	fmuld	d20  , d0,  d10
	fmuld	d28  , d1,  d11
	fmuld	d21  , d0,  d11
	fmuld	d29  , d1,  d10

	add	BO , BO, #32
	add	AO , AO, #16

	pld	[ BO , #B_PRE ]

	fldd	d4 , [ AO, #0 ]
	fldd	d5 , [ AO, #8 ]

	fldd	d12, [ BO ]
	fldd	d13, [ BO, #8 ]
	fldd	d14, [ BO, #16 ]
	fldd	d15, [ BO, #24 ]

	add	BO , BO, #32
	add	AO , AO, #16
.endm



.macro KERNEL1x2_M1
	pld	[ BO , #B_PRE ]

	fmacd	d16  , d0,  d8
	fmacd	d24  , d1,  d9
	fmacd	d17  , d0,  d9
	fmacd	d25  , d1,  d8

	fmacd	d20  , d0,  d10
	fmacd	d28  , d1,  d11
	fmacd	d21  , d0,  d11
	fmacd	d29  , d1,  d10

	fldd	d4 , [ AO, #0 ]
	fldd	d5 , [ AO, #8 ]

	fldd	d12, [ BO ]
	fldd	d13, [ BO, #8 ]
	fldd	d14, [ BO, #16 ]
	fldd	d15, [ BO, #24 ]

	add	BO , BO, #32
	add	AO , AO, #16
.endm

.macro KERNEL1x2_M2
	pld	[ AO , #A_PRE ]
	pld	[ BO , #B_PRE ]

	fmacd	d16  , d4,  d12
	fmacd	d24  , d5,  d13
	fmacd	d17  , d4,  d13
	fmacd	d25  , d5,  d12

	fmacd	d20  , d4,  d14
	fmacd	d28  , d5,  d15
	fmacd	d21  , d4,  d15
	fmacd	d29  , d5,  d14

	fldd	d0 , [ AO, #0 ]
	fldd	d1 , [ AO, #8 ]

	fldd	d8 , [ BO ]
	fldd	d9 , [ BO, #8 ]
	fldd	d10, [ BO, #16 ]
	fldd	d11, [ BO, #24 ]

	add	BO , BO, #32
	add	AO , AO, #16
.endm


.macro KERNEL1x2_E

	fmacd	d16  , d4,  d12
	fmacd	d24  , d5,  d13
	fmacd	d17  , d4,  d13
	fmacd	d25  , d5,  d12

	fmacd	d20  , d4,  d14
	fmacd	d28  , d5,  d15
	fmacd	d21  , d4,  d15
	fmacd	d29  , d5,  d14

.endm

.macro KERNEL1x2_SUB

	pld	[ AO , #A_PRE ]
	pld	[ BO , #B_PRE ]
	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]
	fldd	d8 , [ BO ]
	fldd	d9 , [ BO, #8 ]
	fldd	d10, [ BO, #16 ]
	fldd	d11, [ BO, #24 ]

	fmacd	d16  , d0,  d8
	fmacd	d24  , d1,  d9
	fmacd	d17  , d0,  d9
	fmacd	d25  , d1,  d8

	fmacd	d20  , d0,  d10
	fmacd	d28  , d1,  d11
	fmacd	d21  , d0,  d11
	fmacd	d29  , d1,  d10

	add	BO , BO, #32
	add	AO , AO, #16

.endm




.macro SAVE1x2
	pld	[ CO1 , #C_PRE ]

	ldr	r3  , LDC
	add	CO2 , CO1, r3
	fldd		d0, ALPHA_R
	fldd		d1, ALPHA_I

	vldmia.f64 CO1, { d4 - d5 }
	vldmia.f64 CO2, { d8 - d9  }

	FADD_R	d16, d24 , d16
	FADD_I  d17, d25 , d17
	FADD_R	d20, d28 , d20
	FADD_I  d21, d29 , d21

	FMAC_R1 d4 , d0 , d16
	FMAC_I1 d5 , d0 , d17
	FMAC_R2 d4 , d1 , d17
	FMAC_I2	d5 , d1 , d16

	FMAC_R1 d8 , d0 , d20
	FMAC_I1 d9 , d0 , d21
	FMAC_R2 d8 , d1 , d21
	FMAC_I2	d9 , d1 , d20

	vstmia.f64 CO1, { d4 - d5 }
	vstmia.f64 CO2, { d8 - d9  }

	add	CO1, CO1, #16

.endm

/******************************************************************************/

.macro INIT2x1

	fldd			d16, FP_ZERO
	vmov.f64		d17, d16
	vmov.f64		d18, d16
	vmov.f64		d19, d16
	vmov.f64		d24, d16
	vmov.f64		d25, d16
	vmov.f64		d26, d16
	vmov.f64		d27, d16

.endm

.macro KERNEL2x1_I
	pld	[ AO , #A_PRE ]
	pld	[ BO , #B_PRE ]
	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]
	fldd	d2 , [ AO, #16 ]
	fldd	d3 , [ AO, #24 ]
	fldd	d8 , [ BO ]
	fldd	d9 , [ BO, #8 ]

	fmuld	d16  , d0,  d8
	fmuld	d24  , d1,  d9
	fmuld	d17  , d0,  d9
	fmuld	d25  , d1,  d8

	fmuld	d18  , d2,  d8
	fmuld	d26  , d3,  d9
	fmuld	d19  , d2,  d9
	fmuld	d27  , d3,  d8

	add	BO , BO, #16
	add	AO , AO, #32

	pld	[ BO , #B_PRE ]
	pld	[ AO , #A_PRE ]

	fldd	d4 , [ AO, #0 ]
	fldd	d5 , [ AO, #8 ]
	fldd	d6 , [ AO, #16 ]
	fldd	d7 , [ AO, #24 ]

	fldd	d12, [ BO ]
	fldd	d13, [ BO, #8 ]

	add	BO , BO, #16
	add	AO , AO, #32
.endm



.macro KERNEL2x1_M1
	pld	[ AO , #A_PRE ]
	pld	[ BO , #B_PRE ]

	fmacd	d16  , d0,  d8
	fmacd	d24  , d1,  d9
	fmacd	d17  , d0,  d9
	fmacd	d25  , d1,  d8

	fmacd	d18  , d2,  d8
	fmacd	d26  , d3,  d9
	fmacd	d19  , d2,  d9
	fmacd	d27  , d3,  d8

	fldd	d4 , [ AO, #0 ]
	fldd	d5 , [ AO, #8 ]
	fldd	d6 , [ AO, #16 ]
	fldd	d7 , [ AO, #24 ]

	fldd	d12, [ BO ]
	fldd	d13, [ BO, #8 ]

	add	BO , BO, #16
	add	AO , AO, #32
.endm

.macro KERNEL2x1_M2
	pld	[ AO , #A_PRE ]
	pld	[ BO , #B_PRE ]

	fmacd	d16  , d4,  d12
	fmacd	d24  , d5,  d13
	fmacd	d17  , d4,  d13
	fmacd	d25  , d5,  d12

	fmacd	d18  , d6,  d12
	fmacd	d26  , d7,  d13
	fmacd	d19  , d6,  d13
	fmacd	d27  , d7,  d12

	fldd	d0 , [ AO, #0 ]
	fldd	d1 , [ AO, #8 ]
	fldd	d2 , [ AO, #16 ]
	fldd	d3 , [ AO, #24 ]

	fldd	d8 , [ BO ]
	fldd	d9 , [ BO, #8 ]

	add	BO , BO, #16
	add	AO , AO, #32
.endm


.macro KERNEL2x1_E

	fmacd	d16  , d4,  d12
	fmacd	d24  , d5,  d13
	fmacd	d17  , d4,  d13
	fmacd	d25  , d5,  d12

	fmacd	d18  , d6,  d12
	fmacd	d26  , d7,  d13
	fmacd	d19  , d6,  d13
	fmacd	d27  , d7,  d12

.endm

.macro KERNEL2x1_SUB

	pld	[ AO , #A_PRE ]
	pld	[ BO , #B_PRE ]
	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]
	fldd	d2 , [ AO, #16 ]
	fldd	d3 , [ AO, #24 ]
	fldd	d8 , [ BO ]
	fldd	d9 , [ BO, #8 ]

	fmacd	d16  , d0,  d8
	fmacd	d24  , d1,  d9
	fmacd	d17  , d0,  d9
	fmacd	d25  , d1,  d8

	fmacd	d18  , d2,  d8
	fmacd	d26  , d3,  d9
	fmacd	d19  , d2,  d9
	fmacd	d27  , d3,  d8

	add	BO , BO, #16
	add	AO , AO, #32

.endm




.macro SAVE2x1
	pld	[ CO1 , #C_PRE ]

	fldd		d0, ALPHA_R
	fldd		d1, ALPHA_I

	vldmia.f64 CO1, { d4 - d7 }

	FADD_R	d16, d24 , d16
	FADD_I  d17, d25 , d17
	FADD_R	d18, d26 , d18
	FADD_I  d19, d27 , d19

	FMAC_R1 d4 , d0 , d16
	FMAC_I1 d5 , d0 , d17
	FMAC_R2 d4 , d1 , d17
	FMAC_I2	d5 , d1 , d16

	FMAC_R1 d6 , d0 , d18
	FMAC_I1 d7 , d0 , d19
	FMAC_R2 d6 , d1 , d19
	FMAC_I2	d7 , d1 , d18

	vstmia.f64 CO1, { d4 - d7 }

	add	CO1, CO1, #32

.endm

/******************************************************************************/

.macro INIT1x1

	fldd			d16, FP_ZERO
	vmov.f64		d17, d16
	vmov.f64		d24, d16
	vmov.f64		d25, d16

.endm

.macro KERNEL1x1_I
	pld	[ AO , #A_PRE ]
	pld	[ BO , #B_PRE ]
	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]
	fldd	d8 , [ BO ]
	fldd	d9 , [ BO, #8 ]

	fmuld	d16  , d0,  d8
	fmuld	d24  , d1,  d9
	fmuld	d17  , d0,  d9
	fmuld	d25  , d1,  d8

	add	BO , BO, #16
	add	AO , AO, #16

	pld	[ BO , #B_PRE ]
	pld	[ AO , #A_PRE ]

	fldd	d4 , [ AO, #0 ]
	fldd	d5 , [ AO, #8 ]

	fldd	d12, [ BO ]
	fldd	d13, [ BO, #8 ]

	add	BO , BO, #16
	add	AO , AO, #16
.endm



.macro KERNEL1x1_M1

	fmacd	d16  , d0,  d8
	fmacd	d24  , d1,  d9
	fmacd	d17  , d0,  d9
	fmacd	d25  , d1,  d8

	fldd	d4 , [ AO, #0 ]
	fldd	d5 , [ AO, #8 ]

	fldd	d12, [ BO ]
	fldd	d13, [ BO, #8 ]

	add	BO , BO, #16
	add	AO , AO, #16
.endm

.macro KERNEL1x1_M2

	fmacd	d16  , d4,  d12
	fmacd	d24  , d5,  d13
	fmacd	d17  , d4,  d13
	fmacd	d25  , d5,  d12

	fldd	d0 , [ AO, #0 ]
	fldd	d1 , [ AO, #8 ]

	fldd	d8 , [ BO ]
	fldd	d9 , [ BO, #8 ]

	add	BO , BO, #16
	add	AO , AO, #16
.endm


.macro KERNEL1x1_E

	fmacd	d16  , d4,  d12
	fmacd	d24  , d5,  d13
	fmacd	d17  , d4,  d13
	fmacd	d25  , d5,  d12

.endm

.macro KERNEL1x1_SUB

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]
	fldd	d8 , [ BO ]
	fldd	d9 , [ BO, #8 ]

	fmacd	d16  , d0,  d8
	fmacd	d24  , d1,  d9
	fmacd	d17  , d0,  d9
	fmacd	d25  , d1,  d8

	add	BO , BO, #16
	add	AO , AO, #16

.endm




.macro SAVE1x1
	pld	[ CO1 , #C_PRE ]

	fldd		d0, ALPHA_R
	fldd		d1, ALPHA_I

	vldmia.f64 CO1, { d4 - d5 }

	FADD_R	d16, d24 , d16
	FADD_I  d17, d25 , d17

	FMAC_R1 d4 , d0 , d16
	FMAC_I1 d5 , d0 , d17
	FMAC_R2 d4 , d1 , d17
	FMAC_I2	d5 , d1 , d16

	vstmia.f64 CO1, { d4 - d5 }

	add	CO1, CO1, #16

.endm

/******************************************************************************/


/**************************************************************************************
* End of macro definitions
**************************************************************************************/

	PROLOGUE

	.align 5

	push	{r4 - r9, fp}
	add	fp, sp, #24
	sub	sp, sp, #STACKSIZE				// reserve stack

#if !defined(__ARM_PCS_VFP)
	vldr	OLD_ALPHA_R, OLD_ALPHAR_SOFTFP
	vldr	OLD_ALPHA_I, OLD_ALPHAI_SOFTFP
	ldr	OLD_A, OLD_A_SOFTFP
#endif
	str	OLD_M, M
	str	OLD_N, N
	str	OLD_K, K
	str	OLD_A, A
	vstr	OLD_ALPHA_R, ALPHA_R
	vstr	OLD_ALPHA_I, ALPHA_I

	sub	r3, fp, #128
	vstm	r3, { d8 - d15} 				// store floating point registers

        movs    r4, #0
        str     r4, FP_ZERO
        str     r4, FP_ZERO_1

	ldr	r3, OLD_LDC
	lsl	r3, r3, #4					// ldc = ldc * 8 * 2
	str	r3, LDC

	ldr	K1, K
	ldr	BC, B

	ldr	J, N
	asrs	J, J, #1					// J = J / 2
	ble	zgemm_kernel_L1_BEGIN

zgemm_kernel_L2_BEGIN:

	ldr	CO1, C						// CO1 = C
	ldr	r4 , LDC
	lsl	r4 , r4 , #1					// LDC * 2
	add	r3 , r4, CO1
	str	r3 , C						// store C

	ldr	AO, A						// AO = A
        pld     [AO , #A_PRE-64]
        pld     [AO , #A_PRE-32]



zgemm_kernel_L2_M2_BEGIN:

	ldr	I, M
	asrs	I, I, #1					// I = I / 2
	ble	zgemm_kernel_L2_M1_BEGIN

zgemm_kernel_L2_M2_20:


	mov	BO, BC
	asrs	L , K1, #3					// L = L / 8
	cmp	L , #3
	blt	zgemm_kernel_L2_M2_30
	.align 5



	KERNEL2x2_I
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_M2

	KERNEL2x2_M1
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_M2

	sub	L, L, #2

zgemm_kernel_L2_M2_22:

	KERNEL2x2_M1
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_M2

	KERNEL2x2_M1
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_M2

	subs	L, L, #1
	bgt	zgemm_kernel_L2_M2_22

	KERNEL2x2_M1
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_M2

	KERNEL2x2_M1
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_E

	b	 zgemm_kernel_L2_M2_44


zgemm_kernel_L2_M2_30:
	tst	L, #3
	ble	zgemm_kernel_L2_M2_40

	tst	L, #2
	ble	zgemm_kernel_L2_M2_32

	KERNEL2x2_I
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_M2

	KERNEL2x2_M1
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_M2

	KERNEL2x2_M1
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_M2


	KERNEL2x2_M1
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_E

	b	 zgemm_kernel_L2_M2_44

zgemm_kernel_L2_M2_32:

	tst	L, #1
	ble	zgemm_kernel_L2_M2_40

	KERNEL2x2_I
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_M2

	KERNEL2x2_M1
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_E

	b	 zgemm_kernel_L2_M2_44


zgemm_kernel_L2_M2_40:

	INIT2x2


zgemm_kernel_L2_M2_44:

	ands	L , K1, #7					// L = L % 8
	ble	zgemm_kernel_L2_M2_100

zgemm_kernel_L2_M2_46:

	KERNEL2x2_SUB

	subs	L, L, #1
	bne	zgemm_kernel_L2_M2_46

zgemm_kernel_L2_M2_100:

	SAVE2x2

zgemm_kernel_L2_M2_END:

	subs	I, I, #1
	bne	zgemm_kernel_L2_M2_20


zgemm_kernel_L2_M1_BEGIN:

	ldr	I, M
	tst	I, #1					// I = I % 2
	ble	zgemm_kernel_L2_END

zgemm_kernel_L2_M1_20:

	INIT1x2

	mov	BO, BC
	asrs	L , K1, #3					// L = L / 8
	ble	zgemm_kernel_L2_M1_40

zgemm_kernel_L2_M1_22:

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	subs	L, L, #1
	bgt	zgemm_kernel_L2_M1_22


zgemm_kernel_L2_M1_40:

	ands	L , K1, #7					// L = L % 8
	ble	zgemm_kernel_L2_M1_100

zgemm_kernel_L2_M1_42:

	KERNEL1x2_SUB

	subs	L, L, #1
	bgt	zgemm_kernel_L2_M1_42

zgemm_kernel_L2_M1_100:

	SAVE1x2


zgemm_kernel_L2_END:

	mov	r3, BC
	mov	r4, K1
	lsl	r4, r4, #5					// k * 2 * 8 * 2
	add	r3, r3, r4					// B = B + K * 4 * 8
	mov	BC, r3

	subs	J , #1						// j--
	bgt	zgemm_kernel_L2_BEGIN



/*********************************************************************************************/

zgemm_kernel_L1_BEGIN:

	ldr	J , N
	tst	J , #1
	ble	zgemm_kernel_L999


	ldr	CO1, C						// CO1 = C
	ldr	r4 , LDC
	add	r3 , r4, CO1
	str	r3 , C						// store C

	ldr	AO, A						// AO = A

zgemm_kernel_L1_M2_BEGIN:

	ldr	I, M
	asrs	I, I, #1					// I = I / 2
	ble	zgemm_kernel_L1_M1_BEGIN

zgemm_kernel_L1_M2_20:


	mov	BO, BC
	asrs	L , K1, #3					// L = L / 8
	cmp	L , #3
	blt	zgemm_kernel_L1_M2_30
	.align 5



	KERNEL2x1_I
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_M2

	KERNEL2x1_M1
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_M2

	sub	L, L, #2

zgemm_kernel_L1_M2_22:

	KERNEL2x1_M1
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_M2

	KERNEL2x1_M1
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_M2

	subs	L, L, #1
	bgt	zgemm_kernel_L1_M2_22

	KERNEL2x1_M1
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_M2

	KERNEL2x1_M1
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_E

	b	 zgemm_kernel_L1_M2_44


zgemm_kernel_L1_M2_30:
	tst	L, #3
	ble	zgemm_kernel_L1_M2_40

	tst	L, #2
	ble	zgemm_kernel_L1_M2_32

	KERNEL2x1_I
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_M2

	KERNEL2x1_M1
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_M2

	KERNEL2x1_M1
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_M2


	KERNEL2x1_M1
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_E

	b	 zgemm_kernel_L1_M2_44

zgemm_kernel_L1_M2_32:

	tst	L, #1
	ble	zgemm_kernel_L1_M2_40

	KERNEL2x1_I
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_M2

	KERNEL2x1_M1
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_E

	b	 zgemm_kernel_L1_M2_44


zgemm_kernel_L1_M2_40:

	INIT2x1


zgemm_kernel_L1_M2_44:

	ands	L , K1, #7					// L = L % 8
	ble	zgemm_kernel_L1_M2_100

zgemm_kernel_L1_M2_46:

	KERNEL2x1_SUB

	subs	L, L, #1
	bne	zgemm_kernel_L1_M2_46

zgemm_kernel_L1_M2_100:

	SAVE2x1

zgemm_kernel_L1_M2_END:

	subs	I, I, #1
	bne	zgemm_kernel_L1_M2_20


zgemm_kernel_L1_M1_BEGIN:

	ldr	I, M
	tst	I, #1					// I = I % 2
	ble	zgemm_kernel_L1_END

zgemm_kernel_L1_M1_20:

	INIT1x1

	mov	BO, BC
	asrs	L , K1, #3					// L = L / 8
	ble	zgemm_kernel_L1_M1_40

zgemm_kernel_L1_M1_22:

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	subs	L, L, #1
	bgt	zgemm_kernel_L1_M1_22


zgemm_kernel_L1_M1_40:

	ands	L , K1, #7					// L = L % 8
	ble	zgemm_kernel_L1_M1_100

zgemm_kernel_L1_M1_42:

	KERNEL1x1_SUB

	subs	L, L, #1
	bgt	zgemm_kernel_L1_M1_42

zgemm_kernel_L1_M1_100:

	SAVE1x1


zgemm_kernel_L1_END:



zgemm_kernel_L999:

	sub	r3, fp, #128
	vldm	r3, { d8 - d15}					// restore floating point registers

	movs	r0, #0						// set return value
	sub	sp, fp, #24
	pop	{r4 - r9, fp}
	bx	lr

	EPILOGUE

