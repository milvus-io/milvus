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
#define OLD_ALPHA_R s0
#define OLD_ALPHA_I s1

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
#define OLD_ALPHAR_SOFTFP	r3
#define OLD_ALPHAI_SOFTFP	[fp, #4]
#define OLD_A_SOFTFP	[fp, #8 ]
#define B	[fp, #12 ]
#define C	[fp, #16 ]
#define OLD_LDC	[fp, #20 ]
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


#if defined(NN) || defined(NT) || defined(TN) || defined(TT)

	#define	KMAC_R	vmls.f32
	#define	KMAC_I	fmacs

	#define	FMAC_R1	fmacs
	#define	FMAC_R2	vmls.f32
	#define	FMAC_I1	fmacs
	#define	FMAC_I2	fmacs

#elif defined(CN) || defined(CT)

	#define	KMAC_R	fmacs
	#define	KMAC_I	vmls.f32

	#define	FMAC_R1	fmacs
	#define	FMAC_R2	vmls.f32
	#define	FMAC_I1	fmacs
	#define	FMAC_I2	fmacs

#elif defined(NC) || defined(TC)

	#define	KMAC_R	fmacs
	#define	KMAC_I	vmls.f32

	#define	FMAC_R1	fmacs
	#define	FMAC_R2	fmacs
	#define	FMAC_I1	vmls.f32
	#define	FMAC_I2	fmacs

#else

	#define	KMAC_R  vmls.f32
	#define	KMAC_I	fmacs

	#define	FMAC_R1	fmacs
	#define	FMAC_R2	fmacs
	#define	FMAC_I1	vmls.f32
	#define	FMAC_I2	fmacs

#endif



/**************************************************************************************
* Macro definitions
**************************************************************************************/

.macro INIT2x2

	flds			s8 , FP_ZERO
	vmov.f32		s9 , s8
	vmov.f32		s10, s8
	vmov.f32		s11, s8
	vmov.f32		s12, s8
	vmov.f32		s13, s8
	vmov.f32		s14, s8
	vmov.f32		s15, s8

.endm

.macro KERNEL2x2_I

	pld	[ AO, #A_PRE ]
	vldmia.f32	AO!, { s0 - s3 }
	pld	[ BO, #B_PRE ]
	vldmia.f32	BO!, { s4 - s7 }


	fmuls	s8  , s0,  s4
	fmuls	s9  , s0,  s5
	fmuls	s10 , s2,  s4
	fmuls	s11 , s2,  s5

	KMAC_R	s8  , s1,  s5
	KMAC_I	s9  , s1,  s4
	KMAC_R	s10 , s3,  s5
	KMAC_I	s11 , s3,  s4

	fmuls	s12 , s0,  s6
	fmuls	s13 , s0,  s7
	fmuls	s14 , s2,  s6
	fmuls	s15 , s2,  s7

	KMAC_R	s12 , s1,  s7
	KMAC_I	s13 , s1,  s6
	KMAC_R	s14 , s3,  s7
	KMAC_I	s15 , s3,  s6

.endm



.macro KERNEL2x2_M1

	pld	[ AO, #A_PRE ]
	vldmia.f32	AO!, { s0 - s3 }
	pld	[ BO, #B_PRE ]
	vldmia.f32	BO!, { s4 - s7 }

	fmacs	s8  , s0,  s4
	fmacs	s9  , s0,  s5
	fmacs	s10 , s2,  s4
	fmacs	s11 , s2,  s5

	KMAC_R	s8  , s1,  s5
	KMAC_I	s9  , s1,  s4
	KMAC_R	s10 , s3,  s5
	KMAC_I	s11 , s3,  s4

	fmacs	s12 , s0,  s6
	fmacs	s13 , s0,  s7
	fmacs	s14 , s2,  s6
	fmacs	s15 , s2,  s7

	KMAC_R	s12 , s1,  s7
	KMAC_I	s13 , s1,  s6
	KMAC_R	s14 , s3,  s7
	KMAC_I	s15 , s3,  s6

.endm

.macro KERNEL2x2_M2

	vldmia.f32	AO!, { s0 - s3 }
	vldmia.f32	BO!, { s4 - s7 }

	fmacs	s8  , s0,  s4
	fmacs	s9  , s0,  s5
	fmacs	s10 , s2,  s4
	fmacs	s11 , s2,  s5

	KMAC_R	s8  , s1,  s5
	KMAC_I	s9  , s1,  s4
	KMAC_R	s10 , s3,  s5
	KMAC_I	s11 , s3,  s4

	fmacs	s12 , s0,  s6
	fmacs	s13 , s0,  s7
	fmacs	s14 , s2,  s6
	fmacs	s15 , s2,  s7

	KMAC_R	s12 , s1,  s7
	KMAC_I	s13 , s1,  s6
	KMAC_R	s14 , s3,  s7
	KMAC_I	s15 , s3,  s6


.endm


.macro KERNEL2x2_E

	vldmia.f32	AO!, { s0 - s3 }
	vldmia.f32	BO!, { s4 - s7 }

	fmacs	s8  , s0,  s4
	fmacs	s9  , s0,  s5
	fmacs	s10 , s2,  s4
	fmacs	s11 , s2,  s5

	KMAC_R	s8  , s1,  s5
	KMAC_I	s9  , s1,  s4
	KMAC_R	s10 , s3,  s5
	KMAC_I	s11 , s3,  s4

	fmacs	s12 , s0,  s6
	fmacs	s13 , s0,  s7
	fmacs	s14 , s2,  s6
	fmacs	s15 , s2,  s7

	KMAC_R	s12 , s1,  s7
	KMAC_I	s13 , s1,  s6
	KMAC_R	s14 , s3,  s7
	KMAC_I	s15 , s3,  s6


.endm

.macro KERNEL2x2_SUB

	vldmia.f32	AO!, { s0 - s3 }
	vldmia.f32	BO!, { s4 - s7 }

	fmacs	s8  , s0,  s4
	fmacs	s9  , s0,  s5
	fmacs	s10 , s2,  s4
	fmacs	s11 , s2,  s5

	KMAC_R	s8  , s1,  s5
	KMAC_I	s9  , s1,  s4
	KMAC_R	s10 , s3,  s5
	KMAC_I	s11 , s3,  s4

	fmacs	s12 , s0,  s6
	fmacs	s13 , s0,  s7
	fmacs	s14 , s2,  s6
	fmacs	s15 , s2,  s7

	KMAC_R	s12 , s1,  s7
	KMAC_I	s13 , s1,  s6
	KMAC_R	s14 , s3,  s7
	KMAC_I	s15 , s3,  s6


.endm


.macro SAVE2x2

	ldr	r3  , LDC
	add	CO2 , CO1, r3

	flds		s0, ALPHA_R
	flds		s1, ALPHA_I

	vldmia.f32 CO1, { s4 - s7 }

	FMAC_R1 s4 , s0 , s8
	FMAC_I1 s5 , s0 , s9
	FMAC_R2 s4 , s1 , s9
	FMAC_I2	s5 , s1 , s8

	FMAC_R1 s6 , s0 , s10
	FMAC_I1 s7 , s0 , s11
	FMAC_R2 s6 , s1 , s11
	FMAC_I2	s7 , s1 , s10

	vstmia.f32 CO1, { s4 - s7 }

	vldmia.f32 CO2, { s4 - s7 }

	FMAC_R1 s4 , s0 , s12
	FMAC_I1 s5 , s0 , s13
	FMAC_R2 s4 , s1 , s13
	FMAC_I2	s5 , s1 , s12

	FMAC_R1 s6 , s0 , s14
	FMAC_I1 s7 , s0 , s15
	FMAC_R2 s6 , s1 , s15
	FMAC_I2	s7 , s1 , s14

	vstmia.f32 CO2, { s4 - s7 }

	add	CO1, CO1, #16

.endm

/******************************************************************************/

.macro INIT1x2

	flds			s8 , FP_ZERO
	vmov.f32		s9 , s8
	vmov.f32		s12, s8
	vmov.f32		s13, s8

.endm

.macro KERNEL1x2_I

	flds	s0 , [ AO ]
	flds	s1 , [ AO, #4 ]

	flds	s4 , [ BO ]
	flds	s5 , [ BO, #4 ]

	flds	s6 , [ BO, #8 ]
	flds	s7 , [ BO, #12 ]

	fmuls	s8  , s0,  s4
	KMAC_R	s8  , s1,  s5
	fmuls	s9  , s0,  s5
	KMAC_I	s9  , s1,  s4

	fmuls	s12 , s0,  s6
	KMAC_R	s12 , s1,  s7
	fmuls	s13 , s0,  s7
	KMAC_I	s13 , s1,  s6

	add	BO , BO, #16
	add	AO , AO, #8

.endm



.macro KERNEL1x2_M1

	flds	s0 , [ AO ]
	flds	s1 , [ AO, #4 ]

	flds	s4 , [ BO ]
	flds	s5 , [ BO, #4 ]
	flds	s6 , [ BO, #8 ]
	flds	s7 , [ BO, #12 ]

	fmacs	s8  , s0,  s4
	KMAC_R	s8  , s1,  s5
	fmacs	s9  , s0,  s5
	KMAC_I	s9  , s1,  s4

	fmacs	s12 , s0,  s6
	KMAC_R	s12 , s1,  s7
	fmacs	s13 , s0,  s7
	KMAC_I	s13 , s1,  s6

	add	BO , BO, #16
	add	AO , AO, #8

.endm

.macro KERNEL1x2_M2

	flds	s0 , [ AO ]
	flds	s1 , [ AO, #4 ]

	flds	s4 , [ BO ]
	flds	s5 , [ BO, #4 ]
	flds	s6 , [ BO, #8 ]
	flds	s7 , [ BO, #12 ]

	fmacs	s8  , s0,  s4
	KMAC_R	s8  , s1,  s5
	fmacs	s9  , s0,  s5
	KMAC_I	s9  , s1,  s4

	fmacs	s12 , s0,  s6
	KMAC_R	s12 , s1,  s7
	fmacs	s13 , s0,  s7
	KMAC_I	s13 , s1,  s6

	add	BO , BO, #16
	add	AO , AO, #8


.endm


.macro KERNEL1x2_E

	flds	s0 , [ AO ]
	flds	s1 , [ AO, #4 ]

	flds	s4 , [ BO ]
	flds	s5 , [ BO, #4 ]
	flds	s6 , [ BO, #8 ]
	flds	s7 , [ BO, #12 ]

	fmacs	s8  , s0,  s4
	KMAC_R	s8  , s1,  s5
	fmacs	s9  , s0,  s5
	KMAC_I	s9  , s1,  s4

	fmacs	s12 , s0,  s6
	KMAC_R	s12 , s1,  s7
	fmacs	s13 , s0,  s7
	KMAC_I	s13 , s1,  s6

	add	BO , BO, #16
	add	AO , AO, #8

.endm

.macro KERNEL1x2_SUB

	flds	s0 , [ AO ]
	flds	s1 , [ AO, #4 ]

	flds	s4 , [ BO ]
	flds	s5 , [ BO, #4 ]
	flds	s6 , [ BO, #8 ]
	flds	s7 , [ BO, #12 ]

	fmacs	s8  , s0,  s4
	KMAC_R	s8  , s1,  s5
	fmacs	s9  , s0,  s5
	KMAC_I	s9  , s1,  s4

	fmacs	s12 , s0,  s6
	KMAC_R	s12 , s1,  s7
	fmacs	s13 , s0,  s7
	KMAC_I	s13 , s1,  s6

	add	BO , BO, #16
	add	AO , AO, #8


.endm


.macro SAVE1x2

	ldr	r3  , LDC
	add	CO2 , CO1, r3

	flds		s0, ALPHA_R
	flds		s1, ALPHA_I

	vldmia.f32 CO1, { s4 - s5 }

	FMAC_R1 s4 , s0 , s8
	FMAC_I1 s5 , s0 , s9
	FMAC_R2 s4 , s1 , s9
	FMAC_I2	s5 , s1 , s8

	vstmia.f32 CO1, { s4 - s5 }

	vldmia.f32 CO2, { s4 - s5 }

	FMAC_R1 s4 , s0 , s12
	FMAC_I1 s5 , s0 , s13
	FMAC_R2 s4 , s1 , s13
	FMAC_I2	s5 , s1 , s12

	vstmia.f32 CO2, { s4 - s5 }

	add	CO1, CO1, #8

.endm


/******************************************************************************/

.macro INIT2x1

	flds			s8 , FP_ZERO
	vmov.f32		s9 , s8
	vmov.f32		s10, s8
	vmov.f32		s11, s8

.endm

.macro KERNEL2x1_I

	flds	s0 , [ AO ]
	flds	s1 , [ AO, #4 ]

	flds	s2 , [ AO, #8 ]
	flds	s3 , [ AO, #12 ]

	flds	s4 , [ BO ]
	flds	s5 , [ BO, #4 ]

	fmuls	s8  , s0,  s4
	KMAC_R	s8  , s1,  s5
	fmuls	s9  , s0,  s5
	KMAC_I	s9  , s1,  s4

	fmuls	s10 , s2,  s4
	KMAC_R	s10 , s3,  s5
	fmuls	s11 , s2,  s5
	KMAC_I	s11 , s3,  s4

	add	BO , BO, #8
	add	AO , AO, #16

.endm



.macro KERNEL2x1_M1

	flds	s0 , [ AO ]
	flds	s1 , [ AO, #4 ]
	flds	s2 , [ AO, #8 ]
	flds	s3 , [ AO, #12 ]

	flds	s4 , [ BO ]
	flds	s5 , [ BO, #4 ]

	fmacs	s8  , s0,  s4
	KMAC_R	s8  , s1,  s5
	fmacs	s9  , s0,  s5
	KMAC_I	s9  , s1,  s4

	fmacs	s10 , s2,  s4
	KMAC_R	s10 , s3,  s5
	fmacs	s11 , s2,  s5
	KMAC_I	s11 , s3,  s4

	add	BO , BO, #8
	add	AO , AO, #16

.endm

.macro KERNEL2x1_M2

	flds	s0 , [ AO ]
	flds	s1 , [ AO, #4 ]
	flds	s2 , [ AO, #8 ]
	flds	s3 , [ AO, #12 ]

	flds	s4 , [ BO ]
	flds	s5 , [ BO, #4 ]

	fmacs	s8  , s0,  s4
	KMAC_R	s8  , s1,  s5
	fmacs	s9  , s0,  s5
	KMAC_I	s9  , s1,  s4

	fmacs	s10 , s2,  s4
	KMAC_R	s10 , s3,  s5
	fmacs	s11 , s2,  s5
	KMAC_I	s11 , s3,  s4

	add	BO , BO, #8
	add	AO , AO, #16


.endm


.macro KERNEL2x1_E

	flds	s0 , [ AO ]
	flds	s1 , [ AO, #4 ]
	flds	s2 , [ AO, #8 ]
	flds	s3 , [ AO, #12 ]

	flds	s4 , [ BO ]
	flds	s5 , [ BO, #4 ]

	fmacs	s8  , s0,  s4
	KMAC_R	s8  , s1,  s5
	fmacs	s9  , s0,  s5
	KMAC_I	s9  , s1,  s4

	fmacs	s10 , s2,  s4
	KMAC_R	s10 , s3,  s5
	fmacs	s11 , s2,  s5
	KMAC_I	s11 , s3,  s4

	add	BO , BO, #8
	add	AO , AO, #16

.endm

.macro KERNEL2x1_SUB

	flds	s0 , [ AO ]
	flds	s1 , [ AO, #4 ]
	flds	s2 , [ AO, #8 ]
	flds	s3 , [ AO, #12 ]

	flds	s4 , [ BO ]
	flds	s5 , [ BO, #4 ]

	fmacs	s8  , s0,  s4
	KMAC_R	s8  , s1,  s5
	fmacs	s9  , s0,  s5
	KMAC_I	s9  , s1,  s4

	fmacs	s10 , s2,  s4
	KMAC_R	s10 , s3,  s5
	fmacs	s11 , s2,  s5
	KMAC_I	s11 , s3,  s4

	add	BO , BO, #8
	add	AO , AO, #16


.endm


.macro SAVE2x1

	flds		s0, ALPHA_R
	flds		s1, ALPHA_I

	vldmia.f32 CO1, { s4 - s7 }

	FMAC_R1 s4 , s0 , s8
	FMAC_I1 s5 , s0 , s9
	FMAC_R2 s4 , s1 , s9
	FMAC_I2	s5 , s1 , s8

	FMAC_R1 s6 , s0 , s10
	FMAC_I1 s7 , s0 , s11
	FMAC_R2 s6 , s1 , s11
	FMAC_I2	s7 , s1 , s10

	vstmia.f32 CO1, { s4 - s7 }

	add	CO1, CO1, #16

.endm


/******************************************************************************/

.macro INIT1x1

	flds			s8 , FP_ZERO
	vmov.f32		s9 , s8

.endm

.macro KERNEL1x1_I

	flds	s0 , [ AO ]
	flds	s1 , [ AO, #4 ]

	flds	s4 , [ BO ]
	flds	s5 , [ BO, #4 ]

	fmuls	s8  , s0,  s4
	KMAC_R	s8  , s1,  s5
	fmuls	s9  , s0,  s5
	KMAC_I	s9  , s1,  s4

	add	BO , BO, #8
	add	AO , AO, #8

.endm



.macro KERNEL1x1_M1

	flds	s0 , [ AO ]
	flds	s1 , [ AO, #4 ]

	flds	s4 , [ BO ]
	flds	s5 , [ BO, #4 ]

	fmacs	s8  , s0,  s4
	KMAC_R	s8  , s1,  s5
	fmacs	s9  , s0,  s5
	KMAC_I	s9  , s1,  s4

	add	BO , BO, #8
	add	AO , AO, #8

.endm

.macro KERNEL1x1_M2

	flds	s0 , [ AO ]
	flds	s1 , [ AO, #4 ]

	flds	s4 , [ BO ]
	flds	s5 , [ BO, #4 ]

	fmacs	s8  , s0,  s4
	KMAC_R	s8  , s1,  s5
	fmacs	s9  , s0,  s5
	KMAC_I	s9  , s1,  s4

	add	BO , BO, #8
	add	AO , AO, #8


.endm


.macro KERNEL1x1_E

	flds	s0 , [ AO ]
	flds	s1 , [ AO, #4 ]

	flds	s4 , [ BO ]
	flds	s5 , [ BO, #4 ]

	fmacs	s8  , s0,  s4
	KMAC_R	s8  , s1,  s5
	fmacs	s9  , s0,  s5
	KMAC_I	s9  , s1,  s4

	add	BO , BO, #8
	add	AO , AO, #8

.endm

.macro KERNEL1x1_SUB

	flds	s0 , [ AO ]
	flds	s1 , [ AO, #4 ]

	flds	s4 , [ BO ]
	flds	s5 , [ BO, #4 ]

	fmacs	s8  , s0,  s4
	KMAC_R	s8  , s1,  s5
	fmacs	s9  , s0,  s5
	KMAC_I	s9  , s1,  s4

	add	BO , BO, #8
	add	AO , AO, #8


.endm


.macro SAVE1x1

	flds		s0, ALPHA_R
	flds		s1, ALPHA_I

	vldmia.f32 CO1, { s4 - s5 }

	FMAC_R1 s4 , s0 , s8
	FMAC_I1 s5 , s0 , s9
	FMAC_R2 s4 , s1 , s9
	FMAC_I2	s5 , s1 , s8

	vstmia.f32 CO1, { s4 - s5 }

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
	vmov	OLD_ALPHA_R, OLD_ALPHAR_SOFTFP
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
	vstm	r3, { s8 - s15} 				// store floating point registers

        movs    r4, #0
        str     r4, FP_ZERO
        str     r4, FP_ZERO_1

	ldr	r3, OLD_LDC
	lsl	r3, r3, #3					// ldc = ldc * 4 * 2
	str	r3, LDC

	ldr	K1, K
	ldr	BC, B

	ldr	J, N
	asrs	J, J, #1					// J = J / 2
	ble	cgemm_kernel_L1_BEGIN

cgemm_kernel_L2_BEGIN:

	ldr	CO1, C						// CO1 = C
	ldr	r4 , LDC
	lsl	r4 , r4 , #1					// LDC * 2
	add	r3 , r4, CO1
	str	r3 , C						// store C

	ldr	AO, A						// AO = A
        pld     [AO , #A_PRE-64]
        pld     [AO , #A_PRE-32]



cgemm_kernel_L2_M2_BEGIN:

	ldr	I, M
	asrs	I, I, #1					// I = I / 2
	ble	cgemm_kernel_L2_M1_BEGIN

cgemm_kernel_L2_M2_20:


	mov	BO, BC
	asrs	L , K1, #3					// L = L / 8
	cmp	L , #3
	blt	cgemm_kernel_L2_M2_30
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

cgemm_kernel_L2_M2_22:

	KERNEL2x2_M1
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_M2

	KERNEL2x2_M1
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_M2

	subs	L, L, #1
	bgt	cgemm_kernel_L2_M2_22

	KERNEL2x2_M1
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_M2

	KERNEL2x2_M1
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_E

	b	 cgemm_kernel_L2_M2_44


cgemm_kernel_L2_M2_30:
	tst	L, #3
	ble	cgemm_kernel_L2_M2_40

	tst	L, #2
	ble	cgemm_kernel_L2_M2_32

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

	b	 cgemm_kernel_L2_M2_44

cgemm_kernel_L2_M2_32:

	tst	L, #1
	ble	cgemm_kernel_L2_M2_40

	KERNEL2x2_I
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_M2

	KERNEL2x2_M1
	KERNEL2x2_M2
	KERNEL2x2_M1
	KERNEL2x2_E

	b	 cgemm_kernel_L2_M2_44


cgemm_kernel_L2_M2_40:

	INIT2x2


cgemm_kernel_L2_M2_44:

	ands	L , K1, #7					// L = L % 8
	ble	cgemm_kernel_L2_M2_100

cgemm_kernel_L2_M2_46:

	KERNEL2x2_SUB

	subs	L, L, #1
	bne	cgemm_kernel_L2_M2_46

cgemm_kernel_L2_M2_100:

	SAVE2x2

cgemm_kernel_L2_M2_END:

	subs	I, I, #1
	bne	cgemm_kernel_L2_M2_20


cgemm_kernel_L2_M1_BEGIN:

	ldr	I, M
	tst	I, #1					// I = I % 2
	ble	cgemm_kernel_L2_END

cgemm_kernel_L2_M1_20:

	INIT1x2

	mov	BO, BC
	asrs	L , K1, #3					// L = L / 8
	ble	cgemm_kernel_L2_M1_40

cgemm_kernel_L2_M1_22:

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	subs	L, L, #1
	bgt	cgemm_kernel_L2_M1_22


cgemm_kernel_L2_M1_40:

	ands	L , K1, #7					// L = L % 8
	ble	cgemm_kernel_L2_M1_100

cgemm_kernel_L2_M1_42:

	KERNEL1x2_SUB

	subs	L, L, #1
	bgt	cgemm_kernel_L2_M1_42

cgemm_kernel_L2_M1_100:

	SAVE1x2


cgemm_kernel_L2_END:

	mov	r3, BC
	mov	r4, K1
	lsl	r4, r4, #4					// k * 2 * 4 * 2
	add	r3, r3, r4					// B = B + K * 2 * 8
	mov	BC, r3

	subs	J , #1						// j--
	bgt	cgemm_kernel_L2_BEGIN



/*********************************************************************************************/

cgemm_kernel_L1_BEGIN:

	ldr	J , N
	tst	J , #1
	ble	cgemm_kernel_L999


	ldr	CO1, C						// CO1 = C
	ldr	r4 , LDC
	add	r3 , r4, CO1
	str	r3 , C						// store C

	ldr	AO, A						// AO = A

cgemm_kernel_L1_M2_BEGIN:

	ldr	I, M
	asrs	I, I, #1					// I = I / 2
	ble	cgemm_kernel_L1_M1_BEGIN

cgemm_kernel_L1_M2_20:


	mov	BO, BC
	asrs	L , K1, #3					// L = L / 8
	cmp	L , #3
	blt	cgemm_kernel_L1_M2_30
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

cgemm_kernel_L1_M2_22:

	KERNEL2x1_M1
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_M2

	KERNEL2x1_M1
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_M2

	subs	L, L, #1
	bgt	cgemm_kernel_L1_M2_22

	KERNEL2x1_M1
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_M2

	KERNEL2x1_M1
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_E

	b	 cgemm_kernel_L1_M2_44


cgemm_kernel_L1_M2_30:
	tst	L, #3
	ble	cgemm_kernel_L1_M2_40

	tst	L, #2
	ble	cgemm_kernel_L1_M2_32

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

	b	 cgemm_kernel_L1_M2_44

cgemm_kernel_L1_M2_32:

	tst	L, #1
	ble	cgemm_kernel_L1_M2_40

	KERNEL2x1_I
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_M2

	KERNEL2x1_M1
	KERNEL2x1_M2
	KERNEL2x1_M1
	KERNEL2x1_E

	b	 cgemm_kernel_L1_M2_44


cgemm_kernel_L1_M2_40:

	INIT2x1


cgemm_kernel_L1_M2_44:

	ands	L , K1, #7					// L = L % 8
	ble	cgemm_kernel_L1_M2_100

cgemm_kernel_L1_M2_46:

	KERNEL2x1_SUB

	subs	L, L, #1
	bne	cgemm_kernel_L1_M2_46

cgemm_kernel_L1_M2_100:

	SAVE2x1

cgemm_kernel_L1_M2_END:

	subs	I, I, #1
	bne	cgemm_kernel_L1_M2_20


cgemm_kernel_L1_M1_BEGIN:

	ldr	I, M
	tst	I, #1					// I = I % 2
	ble	cgemm_kernel_L1_END

cgemm_kernel_L1_M1_20:

	INIT1x1

	mov	BO, BC
	asrs	L , K1, #3					// L = L / 8
	ble	cgemm_kernel_L1_M1_40

cgemm_kernel_L1_M1_22:

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	subs	L, L, #1
	bgt	cgemm_kernel_L1_M1_22


cgemm_kernel_L1_M1_40:

	ands	L , K1, #7					// L = L % 8
	ble	cgemm_kernel_L1_M1_100

cgemm_kernel_L1_M1_42:

	KERNEL1x1_SUB

	subs	L, L, #1
	bgt	cgemm_kernel_L1_M1_42

cgemm_kernel_L1_M1_100:

	SAVE1x1


cgemm_kernel_L1_END:



cgemm_kernel_L999:

	sub	r3, fp, #128
	vldm	r3, { s8 - s15}					// restore floating point registers

	movs	r0, #0						// set return value
	sub	sp, fp, #24
	pop	{r4 - r9, fp}
	bx	lr

	EPILOGUE

