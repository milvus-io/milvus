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
* 2013/11/28 Saar
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
#define OLD_ALPHA_R d0
#define OLD_ALPHA_I d1

/******************************************************
* [fp, #-128] - [fp, #-64] is reserved
* for store and restore of floating point
* registers
*******************************************************/

#define KKK     [fp, #-240]
#define KK      [fp, #-244 ]
#define A	[fp, #-248 ]
#define LDC	[fp, #-252 ]
#define M	[fp, #-256 ]
#define N	[fp, #-260 ]
#define K	[fp, #-264 ]

#define FP_ZERO [fp, #-232]
#define FP_ZERO_0 [fp, #-232]
#define FP_ZERO_1 [fp, #-228]

#define ALPHA_I	[fp, #-272]
#define ALPHA_R	[fp, #-280]

#if !defined(__ARM_PCS_VFP)
#define OLD_ALPHAR_SOFTFP	[fp, #4]
#define OLD_ALPHAI_SOFTFP	[fp, #12]
#define OLD_A_SOFTFP	[fp, #20 ]
#define B	[fp, #24 ]
#define C	[fp, #28 ]
#define OLD_LDC	[fp, #32 ]
#define OFFSET  [fp, #36 ]
#else
#define B	[fp, #4 ]
#define C	[fp, #8 ]
#define OLD_LDC	[fp, #12 ]
#define OFFSET  [fp, #16 ]
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


#if defined(NN) || defined(NT) || defined(TN) || defined(TT)

	#define	KMAC_R	vmls.f64
	#define	KMAC_I	fmacd

	#define	FMAC_R1	fmacd
	#define	FMAC_R2	vmls.f64
	#define	FMAC_I1	fmacd
	#define	FMAC_I2	fmacd

#elif defined(CN) || defined(CT)

	#define	KMAC_R	fmacd
	#define	KMAC_I	vmls.f64

	#define	FMAC_R1	fmacd
	#define	FMAC_R2	vmls.f64
	#define	FMAC_I1	fmacd
	#define	FMAC_I2	fmacd

#elif defined(NC) || defined(TC)

	#define	KMAC_R	fmacd
	#define	KMAC_I	vmls.f64

	#define	FMAC_R1	fmacd
	#define	FMAC_R2	fmacd
	#define	FMAC_I1	vmls.f64
	#define	FMAC_I2	fmacd

#else

	#define	KMAC_R  vmls.f64
	#define	KMAC_I	fmacd

	#define	FMAC_R1	fmacd
	#define	FMAC_R2	fmacd
	#define	FMAC_I1	vmls.f64
	#define	FMAC_I2	fmacd

#endif



/**************************************************************************************
* Macro definitions
**************************************************************************************/

.macro INIT2x2

	fldd			d8 , FP_ZERO
	vmov.f64		d9 , d8
	vmov.f64		d10, d8
	vmov.f64		d11, d8
	vmov.f64		d12, d8
	vmov.f64		d13, d8
	vmov.f64		d14, d8
	vmov.f64		d15, d8

.endm

.macro KERNEL2x2_I
	pld	[ AO, #A_PRE ]
	pld	[ BO, #B_PRE ]

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]

	fldd	d2 , [ AO, #16 ]
	fldd	d3 , [ AO, #24 ]

	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]

	fldd	d6 , [ BO, #16 ]
	fldd	d7 , [ BO, #24 ]

	fmuld	d8  , d0,  d4
	KMAC_R	d8  , d1,  d5
	fmuld	d9  , d0,  d5
	KMAC_I	d9  , d1,  d4

	fmuld	d10 , d2,  d4
	KMAC_R	d10 , d3,  d5
	fmuld	d11 , d2,  d5
	KMAC_I	d11 , d3,  d4

	fmuld	d12 , d0,  d6
	KMAC_R	d12 , d1,  d7
	fmuld	d13 , d0,  d7
	KMAC_I	d13 , d1,  d6

	fmuld	d14 , d2,  d6
	KMAC_R	d14 , d3,  d7
	fmuld	d15 , d2,  d7
	KMAC_I	d15 , d3,  d6

	add	BO , BO, #32
	add	AO , AO, #32

.endm



.macro KERNEL2x2_M1


	fldd	d0 , [ AO ]
	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]

	fmacd	d8  , d0,  d4
	fldd	d1 , [ AO, #8 ]
	fmacd	d9  , d0,  d5
	fldd	d2 , [ AO, #16 ]
	KMAC_R	d8  , d1,  d5
	fldd	d3 , [ AO, #24 ]
	KMAC_I	d9  , d1,  d4

	fldd	d6 , [ BO, #16 ]
	fmacd	d10 , d2,  d4
	fldd	d7 , [ BO, #24 ]
	fmacd	d11 , d2,  d5
	KMAC_R	d10 , d3,  d5
	pld	[ AO, #A_PRE ]
	KMAC_I	d11 , d3,  d4

	pld	[ BO, #B_PRE ]
	fmacd	d12 , d0,  d6
	fmacd	d13 , d0,  d7
	KMAC_R	d12 , d1,  d7
	KMAC_I	d13 , d1,  d6

	fmacd	d14 , d2,  d6
	fmacd	d15 , d2,  d7
	add	BO , BO, #32
	KMAC_R	d14 , d3,  d7
	add	AO , AO, #32
	KMAC_I	d15 , d3,  d6


.endm

.macro KERNEL2x2_M2

	fldd	d0 , [ AO ]
	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]

	fmacd	d8  , d0,  d4
	fldd	d1 , [ AO, #8 ]
	fmacd	d9  , d0,  d5
	fldd	d2 , [ AO, #16 ]
	KMAC_R	d8  , d1,  d5
	fldd	d3 , [ AO, #24 ]
	KMAC_I	d9  , d1,  d4

	fldd	d6 , [ BO, #16 ]
	fmacd	d10 , d2,  d4
	fldd	d7 , [ BO, #24 ]
	fmacd	d11 , d2,  d5
	KMAC_R	d10 , d3,  d5
	pld	[ AO, #A_PRE ]
	KMAC_I	d11 , d3,  d4

	pld	[ BO, #B_PRE ]
	fmacd	d12 , d0,  d6
	fmacd	d13 , d0,  d7
	KMAC_R	d12 , d1,  d7
	KMAC_I	d13 , d1,  d6

	fmacd	d14 , d2,  d6
	fmacd	d15 , d2,  d7
	add	BO , BO, #32
	KMAC_R	d14 , d3,  d7
	add	AO , AO, #32
	KMAC_I	d15 , d3,  d6


.endm


.macro KERNEL2x2_E

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]
	fldd	d2 , [ AO, #16 ]
	fldd	d3 , [ AO, #24 ]

	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]
	fldd	d6 , [ BO, #16 ]
	fldd	d7 , [ BO, #24 ]

	fmacd	d8  , d0,  d4
	KMAC_R	d8  , d1,  d5
	fmacd	d9  , d0,  d5
	KMAC_I	d9  , d1,  d4

	fmacd	d10 , d2,  d4
	KMAC_R	d10 , d3,  d5
	fmacd	d11 , d2,  d5
	KMAC_I	d11 , d3,  d4

	fmacd	d12 , d0,  d6
	KMAC_R	d12 , d1,  d7
	fmacd	d13 , d0,  d7
	KMAC_I	d13 , d1,  d6

	fmacd	d14 , d2,  d6
	KMAC_R	d14 , d3,  d7
	fmacd	d15 , d2,  d7
	KMAC_I	d15 , d3,  d6

	add	BO , BO, #32
	add	AO , AO, #32

.endm

.macro KERNEL2x2_SUB

	fldd	d0 , [ AO ]
	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]

	fmacd	d8  , d0,  d4
	fldd	d1 , [ AO, #8 ]
	fmacd	d9  , d0,  d5
	fldd	d2 , [ AO, #16 ]
	KMAC_R	d8  , d1,  d5
	fldd	d3 , [ AO, #24 ]
	KMAC_I	d9  , d1,  d4

	fldd	d6 , [ BO, #16 ]
	fmacd	d10 , d2,  d4
	fldd	d7 , [ BO, #24 ]
	fmacd	d11 , d2,  d5
	KMAC_R	d10 , d3,  d5
	pld	[ AO, #A_PRE ]
	KMAC_I	d11 , d3,  d4

	pld	[ BO, #B_PRE ]
	fmacd	d12 , d0,  d6
	fmacd	d13 , d0,  d7
	KMAC_R	d12 , d1,  d7
	KMAC_I	d13 , d1,  d6

	fmacd	d14 , d2,  d6
	fmacd	d15 , d2,  d7
	add	BO , BO, #32
	KMAC_R	d14 , d3,  d7
	add	AO , AO, #32
	KMAC_I	d15 , d3,  d6


.endm


.macro SAVE2x2

	ldr	r3  , LDC
	add	CO2 , CO1, r3

	fldd		d0, ALPHA_R
	fldd		d1, ALPHA_I

	fldd		d4 , FP_ZERO
	vmov.f64	d5 , d4
	vmov.f64	d6 , d4
	vmov.f64	d7 , d4

	FMAC_R1 d4 , d0 , d8
	FMAC_I1 d5 , d0 , d9
	FMAC_R2 d4 , d1 , d9
	FMAC_I2	d5 , d1 , d8

	FMAC_R1 d6 , d0 , d10
	FMAC_I1 d7 , d0 , d11
	FMAC_R2 d6 , d1 , d11
	FMAC_I2	d7 , d1 , d10

	vstmia.f64 CO1, { d4 - d7 }

	fldd		d4 , FP_ZERO
	vmov.f64	d5 , d4
	vmov.f64	d6 , d4
	vmov.f64	d7 , d4

	FMAC_R1 d4 , d0 , d12
	FMAC_I1 d5 , d0 , d13
	FMAC_R2 d4 , d1 , d13
	FMAC_I2	d5 , d1 , d12

	FMAC_R1 d6 , d0 , d14
	FMAC_I1 d7 , d0 , d15
	FMAC_R2 d6 , d1 , d15
	FMAC_I2	d7 , d1 , d14

	vstmia.f64 CO2, { d4 - d7 }

	add	CO1, CO1, #32

.endm

/******************************************************************************/

.macro INIT1x2

	fldd			d8 , FP_ZERO
	vmov.f64		d9 , d8
	vmov.f64		d12, d8
	vmov.f64		d13, d8

.endm

.macro KERNEL1x2_I

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]

	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]

	fldd	d6 , [ BO, #16 ]
	fldd	d7 , [ BO, #24 ]

	fmuld	d8  , d0,  d4
	KMAC_R	d8  , d1,  d5
	fmuld	d9  , d0,  d5
	KMAC_I	d9  , d1,  d4

	fmuld	d12 , d0,  d6
	KMAC_R	d12 , d1,  d7
	fmuld	d13 , d0,  d7
	KMAC_I	d13 , d1,  d6

	add	BO , BO, #32
	add	AO , AO, #16

.endm



.macro KERNEL1x2_M1

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]

	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]
	fldd	d6 , [ BO, #16 ]
	fldd	d7 , [ BO, #24 ]

	fmacd	d8  , d0,  d4
	KMAC_R	d8  , d1,  d5
	fmacd	d9  , d0,  d5
	KMAC_I	d9  , d1,  d4

	fmacd	d12 , d0,  d6
	KMAC_R	d12 , d1,  d7
	fmacd	d13 , d0,  d7
	KMAC_I	d13 , d1,  d6

	add	BO , BO, #32
	add	AO , AO, #16

.endm

.macro KERNEL1x2_M2

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]

	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]
	fldd	d6 , [ BO, #16 ]
	fldd	d7 , [ BO, #24 ]

	fmacd	d8  , d0,  d4
	KMAC_R	d8  , d1,  d5
	fmacd	d9  , d0,  d5
	KMAC_I	d9  , d1,  d4

	fmacd	d12 , d0,  d6
	KMAC_R	d12 , d1,  d7
	fmacd	d13 , d0,  d7
	KMAC_I	d13 , d1,  d6

	add	BO , BO, #32
	add	AO , AO, #16


.endm


.macro KERNEL1x2_E

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]

	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]
	fldd	d6 , [ BO, #16 ]
	fldd	d7 , [ BO, #24 ]

	fmacd	d8  , d0,  d4
	KMAC_R	d8  , d1,  d5
	fmacd	d9  , d0,  d5
	KMAC_I	d9  , d1,  d4

	fmacd	d12 , d0,  d6
	KMAC_R	d12 , d1,  d7
	fmacd	d13 , d0,  d7
	KMAC_I	d13 , d1,  d6

	add	BO , BO, #32
	add	AO , AO, #16

.endm

.macro KERNEL1x2_SUB

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]

	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]
	fldd	d6 , [ BO, #16 ]
	fldd	d7 , [ BO, #24 ]

	fmacd	d8  , d0,  d4
	KMAC_R	d8  , d1,  d5
	fmacd	d9  , d0,  d5
	KMAC_I	d9  , d1,  d4

	fmacd	d12 , d0,  d6
	KMAC_R	d12 , d1,  d7
	fmacd	d13 , d0,  d7
	KMAC_I	d13 , d1,  d6

	add	BO , BO, #32
	add	AO , AO, #16


.endm


.macro SAVE1x2

	ldr	r3  , LDC
	add	CO2 , CO1, r3

	fldd		d0, ALPHA_R
	fldd		d1, ALPHA_I

	fldd		d4 , FP_ZERO
	vmov.f64	d5 , d4

	FMAC_R1 d4 , d0 , d8
	FMAC_I1 d5 , d0 , d9
	FMAC_R2 d4 , d1 , d9
	FMAC_I2	d5 , d1 , d8

	vstmia.f64 CO1, { d4 - d5 }

	fldd		d4 , FP_ZERO
	vmov.f64	d5 , d4

	FMAC_R1 d4 , d0 , d12
	FMAC_I1 d5 , d0 , d13
	FMAC_R2 d4 , d1 , d13
	FMAC_I2	d5 , d1 , d12

	vstmia.f64 CO2, { d4 - d5 }

	add	CO1, CO1, #16

.endm


/******************************************************************************/

.macro INIT2x1

	fldd			d8 , FP_ZERO
	vmov.f64		d9 , d8
	vmov.f64		d10, d8
	vmov.f64		d11, d8

.endm

.macro KERNEL2x1_I

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]

	fldd	d2 , [ AO, #16 ]
	fldd	d3 , [ AO, #24 ]

	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]

	fmuld	d8  , d0,  d4
	KMAC_R	d8  , d1,  d5
	fmuld	d9  , d0,  d5
	KMAC_I	d9  , d1,  d4

	fmuld	d10 , d2,  d4
	KMAC_R	d10 , d3,  d5
	fmuld	d11 , d2,  d5
	KMAC_I	d11 , d3,  d4

	add	BO , BO, #16
	add	AO , AO, #32

.endm



.macro KERNEL2x1_M1

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]
	fldd	d2 , [ AO, #16 ]
	fldd	d3 , [ AO, #24 ]

	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]

	fmacd	d8  , d0,  d4
	KMAC_R	d8  , d1,  d5
	fmacd	d9  , d0,  d5
	KMAC_I	d9  , d1,  d4

	fmacd	d10 , d2,  d4
	KMAC_R	d10 , d3,  d5
	fmacd	d11 , d2,  d5
	KMAC_I	d11 , d3,  d4

	add	BO , BO, #16
	add	AO , AO, #32

.endm

.macro KERNEL2x1_M2

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]
	fldd	d2 , [ AO, #16 ]
	fldd	d3 , [ AO, #24 ]

	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]

	fmacd	d8  , d0,  d4
	KMAC_R	d8  , d1,  d5
	fmacd	d9  , d0,  d5
	KMAC_I	d9  , d1,  d4

	fmacd	d10 , d2,  d4
	KMAC_R	d10 , d3,  d5
	fmacd	d11 , d2,  d5
	KMAC_I	d11 , d3,  d4

	add	BO , BO, #16
	add	AO , AO, #32


.endm


.macro KERNEL2x1_E

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]
	fldd	d2 , [ AO, #16 ]
	fldd	d3 , [ AO, #24 ]

	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]

	fmacd	d8  , d0,  d4
	KMAC_R	d8  , d1,  d5
	fmacd	d9  , d0,  d5
	KMAC_I	d9  , d1,  d4

	fmacd	d10 , d2,  d4
	KMAC_R	d10 , d3,  d5
	fmacd	d11 , d2,  d5
	KMAC_I	d11 , d3,  d4

	add	BO , BO, #16
	add	AO , AO, #32

.endm

.macro KERNEL2x1_SUB

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]
	fldd	d2 , [ AO, #16 ]
	fldd	d3 , [ AO, #24 ]

	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]

	fmacd	d8  , d0,  d4
	KMAC_R	d8  , d1,  d5
	fmacd	d9  , d0,  d5
	KMAC_I	d9  , d1,  d4

	fmacd	d10 , d2,  d4
	KMAC_R	d10 , d3,  d5
	fmacd	d11 , d2,  d5
	KMAC_I	d11 , d3,  d4

	add	BO , BO, #16
	add	AO , AO, #32


.endm


.macro SAVE2x1

	fldd		d0, ALPHA_R
	fldd		d1, ALPHA_I

	fldd		d4 , FP_ZERO
	vmov.f64	d5 , d4
	vmov.f64	d6 , d4
	vmov.f64	d7 , d4

	FMAC_R1 d4 , d0 , d8
	FMAC_I1 d5 , d0 , d9
	FMAC_R2 d4 , d1 , d9
	FMAC_I2	d5 , d1 , d8

	FMAC_R1 d6 , d0 , d10
	FMAC_I1 d7 , d0 , d11
	FMAC_R2 d6 , d1 , d11
	FMAC_I2	d7 , d1 , d10

	vstmia.f64 CO1, { d4 - d7 }

	add	CO1, CO1, #32

.endm


/******************************************************************************/

.macro INIT1x1

	fldd			d8 , FP_ZERO
	vmov.f64		d9 , d8

.endm

.macro KERNEL1x1_I

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]

	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]

	fmuld	d8  , d0,  d4
	KMAC_R	d8  , d1,  d5
	fmuld	d9  , d0,  d5
	KMAC_I	d9  , d1,  d4

	add	BO , BO, #16
	add	AO , AO, #16

.endm



.macro KERNEL1x1_M1

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]

	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]

	fmacd	d8  , d0,  d4
	KMAC_R	d8  , d1,  d5
	fmacd	d9  , d0,  d5
	KMAC_I	d9  , d1,  d4

	add	BO , BO, #16
	add	AO , AO, #16

.endm

.macro KERNEL1x1_M2

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]

	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]

	fmacd	d8  , d0,  d4
	KMAC_R	d8  , d1,  d5
	fmacd	d9  , d0,  d5
	KMAC_I	d9  , d1,  d4

	add	BO , BO, #16
	add	AO , AO, #16


.endm


.macro KERNEL1x1_E

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]

	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]

	fmacd	d8  , d0,  d4
	KMAC_R	d8  , d1,  d5
	fmacd	d9  , d0,  d5
	KMAC_I	d9  , d1,  d4

	add	BO , BO, #16
	add	AO , AO, #16

.endm

.macro KERNEL1x1_SUB

	fldd	d0 , [ AO ]
	fldd	d1 , [ AO, #8 ]

	fldd	d4 , [ BO ]
	fldd	d5 , [ BO, #8 ]

	fmacd	d8  , d0,  d4
	KMAC_R	d8  , d1,  d5
	fmacd	d9  , d0,  d5
	KMAC_I	d9  , d1,  d4

	add	BO , BO, #16
	add	AO , AO, #16


.endm


.macro SAVE1x1

	fldd		d0, ALPHA_R
	fldd		d1, ALPHA_I

	fldd		d4 , FP_ZERO
	vmov.f64	d5 , d4

	FMAC_R1 d4 , d0 , d8
	FMAC_I1 d5 , d0 , d9
	FMAC_R2 d4 , d1 , d9
	FMAC_I2	d5 , d1 , d8

	vstmia.f64 CO1, { d4 - d5 }

	add	CO1, CO1, #16

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

        ldr     r3, OFFSET
#ifndef LEFT
        neg     r3 , r3
#endif
        str     r3 , KK

	ldr	BC, B

	ldr	J, N
	asrs	J, J, #1					// J = J / 2
	ble	_L1_BEGIN

_L2_BEGIN:

	ldr	CO1, C						// CO1 = C
	ldr	r4 , LDC
	lsl	r4 , r4 , #1					// LDC * 2
	add	r3 , r4, CO1
	str	r3 , C						// store C

#if defined(LEFT)
        ldr     r3 , OFFSET
        str     r3 , KK
#endif


	ldr	AO, A						// AO = A
        pld     [AO , #A_PRE-64]
        pld     [AO , #A_PRE-32]



_L2_M2_BEGIN:

	ldr	I, M
	asrs	I, I, #1					// I = I / 2
	ble	_L2_M1_BEGIN

_L2_M2_20:

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))

        mov     BO, BC
#else
        mov     BO, BC
        ldr     r3 , KK
        lsls    r4 , r3 , #5                                    // 2 * 8 * 2 double values
        add     BO , BO , r4
        lsls    r4 , r3 , #5                                    // 2 * 8 * 2 double values
        add     AO , AO , r4

#endif

#ifndef TRMMKERNEL
        ldr     K1, K
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        ldr     K1, K
        ldr     r3, KK
        sub     K1, K1, r3
        str     K1, KKK
#else
        ldr     K1, KK
#ifdef LEFT
        add     K1, K1, #2        // number of values in AO
#else
        add     K1, K1, #2        // number of values in BO
#endif
        str     K1, KKK
#endif


	asrs	L , K1, #3					// L = L / 8
	cmp	L , #3
	blt	_L2_M2_30
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

_L2_M2_22:

	KERNEL2x2_M1
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_M2

	KERNEL2x2_M1
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_M2

	subs	L, L, #1
	bgt	_L2_M2_22

	KERNEL2x2_M1
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_M2

	KERNEL2x2_M1
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_E

	b	 _L2_M2_44


_L2_M2_30:
	tst	L, #3
	ble	_L2_M2_40

	tst	L, #2
	ble	_L2_M2_32

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

	b	 _L2_M2_44

_L2_M2_32:

	tst	L, #1
	ble	_L2_M2_40

	KERNEL2x2_I
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_M2

	KERNEL2x2_M1
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_E

	b	 _L2_M2_44


_L2_M2_40:

	INIT2x2


_L2_M2_44:

	ands	L , K1, #7					// L = L % 8
	ble	_L2_M2_100

_L2_M2_46:

	KERNEL2x2_SUB

	subs	L, L, #1
	bne	_L2_M2_46

_L2_M2_100:

	SAVE2x2

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
        ldr     r3 , K
        ldr     r4 , KKK
        sub     r3 , r3 , r4
        lsls    r4 , r3 , #5                    // 2 * 8 * 2 double values
        add     BO , BO , r4
        lsls    r4 , r3 , #5                    // 2 * 8 * 2 double values
        add     AO , AO , r4
#endif

#if defined(LEFT)
        ldr     r3 , KK
        add     r3 , r3 , #2                    // number of values in AO
        str     r3 , KK
#endif




_L2_M2_END:

	subs	I, I, #1
	bne	_L2_M2_20


_L2_M1_BEGIN:

	ldr	I, M
	tst	I, #1					// I = I % 2
	ble	_L2_END

_L2_M1_20:

	INIT1x2

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))

        mov     BO, BC
#else
        mov     BO, BC
        ldr     r3 , KK
        lsls    r4 , r3 , #5                                    // 2 * 8 * 2 double values
        add     BO , BO , r4
        lsls    r4 , r3 , #4                                    // 1 * 8 * 2 double values
        add     AO , AO , r4

#endif

#ifndef TRMMKERNEL
        ldr     K1, K
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        ldr     K1, K
        ldr     r3, KK
        sub     K1, K1, r3
        str     K1, KKK
#else
        ldr     K1, KK
#ifdef LEFT
        add     K1, K1, #1        // number of values in AO
#else
        add     K1, K1, #2        // number of values in BO
#endif
        str     K1, KKK
#endif

	asrs	L , K1, #3					// L = L / 8
	ble	_L2_M1_40

_L2_M1_22:

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	subs	L, L, #1
	bgt	_L2_M1_22


_L2_M1_40:

	ands	L , K1, #7					// L = L % 8
	ble	_L2_M1_100

_L2_M1_42:

	KERNEL1x2_SUB

	subs	L, L, #1
	bgt	_L2_M1_42

_L2_M1_100:

	SAVE1x2

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
        ldr     r3 , K
        ldr     r4 , KKK
        sub     r3 , r3 , r4
        lsls    r4 , r3 , #5                    // 2 * 8 * 2 double values
        add     BO , BO , r4
        lsls    r4 , r3 , #4                    // 1 * 8 * 2 double values
        add     AO , AO , r4
#endif

#if defined(LEFT)
        ldr     r3 , KK
        add     r3 , r3 , #1                    // number of values in AO
        str     r3 , KK
#endif



_L2_END:

	mov	r3, BC
	ldr	r4, K
	lsl	r4, r4, #5					// k * 2 * 8 * 2
	add	r3, r3, r4					// B = B + K * 4 * 8
	mov	BC, r3

#if !defined(LEFT)
        ldr     r3 , KK
        add     r3 , r3 , #2                                    // number of values in BO
        str     r3 , KK
#endif


	subs	J , #1						// j--
	bgt	_L2_BEGIN



/*********************************************************************************************/

_L1_BEGIN:

	ldr	J , N
	tst	J , #1
	ble	_L999


	ldr	CO1, C						// CO1 = C
	ldr	r4 , LDC
	add	r3 , r4, CO1
	str	r3 , C						// store C

#if defined(LEFT)
        ldr     r3 , OFFSET
        str     r3 , KK
#endif


	ldr	AO, A						// AO = A

_L1_M2_BEGIN:

	ldr	I, M
	asrs	I, I, #1					// I = I / 2
	ble	_L1_M1_BEGIN

_L1_M2_20:

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))

        mov     BO, BC
#else
        mov     BO, BC
        ldr     r3 , KK
        lsls    r4 , r3 , #4                                    // 1 * 8 * 2 double values
        add     BO , BO , r4
        lsls    r4 , r3 , #5                                    // 2 * 8 * 2 double values
        add     AO , AO , r4

#endif

#ifndef TRMMKERNEL
        ldr     K1, K
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        ldr     K1, K
        ldr     r3, KK
        sub     K1, K1, r3
        str     K1, KKK
#else
        ldr     K1, KK
#ifdef LEFT
        add     K1, K1, #2        // number of values in AO
#else
        add     K1, K1, #1        // number of values in BO
#endif
        str     K1, KKK
#endif

	asrs	L , K1, #3					// L = L / 8
	cmp	L , #3
	blt	_L1_M2_30
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

_L1_M2_22:

	KERNEL2x1_M1
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_M2

	KERNEL2x1_M1
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_M2

	subs	L, L, #1
	bgt	_L1_M2_22

	KERNEL2x1_M1
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_M2

	KERNEL2x1_M1
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_E

	b	 _L1_M2_44


_L1_M2_30:
	tst	L, #3
	ble	_L1_M2_40

	tst	L, #2
	ble	_L1_M2_32

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

	b	 _L1_M2_44

_L1_M2_32:

	tst	L, #1
	ble	_L1_M2_40

	KERNEL2x1_I
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_M2

	KERNEL2x1_M1
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_E

	b	 _L1_M2_44


_L1_M2_40:

	INIT2x1


_L1_M2_44:

	ands	L , K1, #7					// L = L % 8
	ble	_L1_M2_100

_L1_M2_46:

	KERNEL2x1_SUB

	subs	L, L, #1
	bne	_L1_M2_46

_L1_M2_100:

	SAVE2x1

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
        ldr     r3 , K
        ldr     r4 , KKK
        sub     r3 , r3 , r4
        lsls    r4 , r3 , #4                    // 1 * 8 * 2 double values
        add     BO , BO , r4
        lsls    r4 , r3 , #5                    // 2 * 8 * 2 double values
        add     AO , AO , r4
#endif

#if defined(LEFT)
        ldr     r3 , KK
        add     r3 , r3 , #2                    // number of values in AO
        str     r3 , KK
#endif



_L1_M2_END:

	subs	I, I, #1
	bne	_L1_M2_20


_L1_M1_BEGIN:

	ldr	I, M
	tst	I, #1					// I = I % 2
	ble	_L1_END

_L1_M1_20:

	INIT1x1

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))

        mov     BO, BC
#else
        mov     BO, BC
        ldr     r3 , KK
        lsls    r4 , r3 , #4                                    // 1 * 8 * 2 double values
        add     BO , BO , r4
        lsls    r4 , r3 , #4                                    // 1 * 8 * 2 double values
        add     AO , AO , r4

#endif

#ifndef TRMMKERNEL
        ldr     K1, K
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        ldr     K1, K
        ldr     r3, KK
        sub     K1, K1, r3
        str     K1, KKK
#else
        ldr     K1, KK
#ifdef LEFT
        add     K1, K1, #1        // number of values in AO
#else
        add     K1, K1, #1        // number of values in BO
#endif
        str     K1, KKK
#endif

	asrs	L , K1, #3					// L = L / 8
	ble	_L1_M1_40

_L1_M1_22:

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	subs	L, L, #1
	bgt	_L1_M1_22


_L1_M1_40:

	ands	L , K1, #7					// L = L % 8
	ble	_L1_M1_100

_L1_M1_42:

	KERNEL1x1_SUB

	subs	L, L, #1
	bgt	_L1_M1_42

_L1_M1_100:

	SAVE1x1


#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
        ldr     r3 , K
        ldr     r4 , KKK
        sub     r3 , r3 , r4
        lsls    r4 , r3 , #4                    // 1 * 8 * 2 double values
        add     BO , BO , r4
        lsls    r4 , r3 , #4                    // 1 * 8 * 2 double values
        add     AO , AO , r4
#endif

#if defined(LEFT)
        ldr     r3 , KK
        add     r3 , r3 , #1                    // number of values in AO
        str     r3 , KK
#endif



_L1_END:



_L999:

	sub	r3, fp, #128
	vldm	r3, { d8 - d15}					// restore floating point registers

	movs	r0, #0						// set return value
	sub	sp, fp, #24
	pop	{r4 - r9, fp}
	bx	lr

	EPILOGUE

