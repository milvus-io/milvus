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
**************************************************************************************/

#define ASSEMBLER
#include "common.h"

#define STACKSIZE 256

#define	OLD_M	r0
#define	OLD_N	r1
#define	OLD_K	r2
#define	OLD_A	r3
#define OLD_ALPHA s0

/******************************************************
* [fp, #-128] - [fp, #-32] is reserved
* for store and restore of floating point
* registers
*******************************************************/

#define KK      [fp, #-244 ]
#define KKK     [fp, #-248]
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
#define OLD_ALPHA_SOFTFP	r3
#define OLD_A_SOFTFP	[fp, #4 ]
#define B	[fp, #8 ]
#define C	[fp, #12 ]
#define OLD_LDC	[fp, #16 ]
#define OFFSET  [fp, #20 ]
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

.macro INIT4x4

	flds			S16, FP_ZERO
	vmov.f32		s17, s16
	vmov.f32		s18, s16
	vmov.f32		s19, s16
	vmov.f32		s20, s16
	vmov.f32		s21, s16
	vmov.f32		s22, s16
	vmov.f32		s23, s16
	vmov.f32		s24, s16
	vmov.f32		s25, s16
	vmov.f32		s26, s16
	vmov.f32		s27, s16
	vmov.f32		s28, s16
	vmov.f32		s29, s16
	vmov.f32		s30, s16
	vmov.f32		s31, s16

.endm

.macro KERNEL4x4_I

	vldmia.f32 AO!, { s0 - s1 }
	pld	[ AO , #A_PRE-8 ]
	vldmia.f32 BO!, { s8 - s9 }
	pld	[ BO , #B_PRE-8 ]

	fmuls	s16  , s0,  s8
	vldmia.f32 AO!, { s2 - s3 }
	fmuls	s17  , s1,  s8
	fmuls	s18  , s2,  s8
	vldmia.f32 BO!, { s10 - s11 }
	fmuls	s19  , s3,  s8

	fmuls	s20  , s0,  s9
	vldmia.f32 AO!, { s4 - s5 }
	fmuls	s21  , s1,  s9
	fmuls	s22  , s2,  s9
	vldmia.f32 AO!, { s6 - s7 }
	fmuls	s23  , s3,  s9

	fmuls	s24  , s0,  s10
	vldmia.f32 BO!, { s12 - s13 }
	fmuls	s25  , s1,  s10
	fmuls	s26  , s2,  s10
	vldmia.f32 BO!, { s14 - s15 }
	fmuls	s27  , s3,  s10

	fmuls	s28  , s0,  s11
	fmuls	s29  , s1,  s11
	fmuls	s30  , s2,  s11
	fmuls	s31  , s3,  s11

.endm


.macro KERNEL4x4_M2

	pld	[ AO , #A_PRE ]
	fmacs	s16  , s4,  s12
	fmacs	s17  , s5,  s12
	vldmia.f32 AO!, { s0 - s1 }
	fmacs	s18  , s6,  s12
	pld	[ BO , #B_PRE ]
	fmacs	s19  , s7,  s12

	fmacs	s20  , s4,  s13
	vldmia.f32 AO!, { s2 - s3 }
	fmacs	s21  , s5,  s13
	fmacs	s22  , s6,  s13
	vldmia.f32 BO!, { s8 - s9 }
	fmacs	s23  , s7,  s13

	fmacs	s24  , s4,  s14
	vldmia.f32 BO!, { s10 - s11 }
	fmacs	s25  , s5,  s14
	fmacs	s26  , s6,  s14
	fmacs	s27  , s7,  s14

	fmacs	s28  , s4,  s15
	fmacs	s29  , s5,  s15
	fmacs	s30  , s6,  s15
	fmacs	s31  , s7,  s15

.endm


.macro KERNEL4x4_M1

	fmacs	s16  , s0,  s8
	vldmia.f32 AO!, { s4 - s5 }
	fmacs	s17  , s1,  s8
	fmacs	s18  , s2,  s8
	vldmia.f32 AO!, { s6 - s7 }
	fmacs	s19  , s3,  s8

	fmacs	s20  , s0,  s9
	vldmia.f32 BO!, { s12 - s13 }
	fmacs	s21  , s1,  s9
	fmacs	s22  , s2,  s9
	vldmia.f32 BO!, { s14 - s15 }
	fmacs	s23  , s3,  s9

	fmacs	s24  , s0,  s10
	fmacs	s25  , s1,  s10
	fmacs	s26  , s2,  s10
	fmacs	s27  , s3,  s10

	fmacs	s28  , s0,  s11
	fmacs	s29  , s1,  s11
	fmacs	s30  , s2,  s11
	fmacs	s31  , s3,  s11

.endm



.macro KERNEL4x4_E

	fmacs	s16  , s4,  s12
	fmacs	s17  , s5,  s12
	fmacs	s18  , s6,  s12
	fmacs	s19  , s7,  s12

	fmacs	s20  , s4,  s13
	fmacs	s21  , s5,  s13
	fmacs	s22  , s6,  s13
	fmacs	s23  , s7,  s13

	fmacs	s24  , s4,  s14
	fmacs	s25  , s5,  s14
	fmacs	s26  , s6,  s14
	fmacs	s27  , s7,  s14

	fmacs	s28  , s4,  s15
	fmacs	s29  , s5,  s15
	fmacs	s30  , s6,  s15
	fmacs	s31  , s7,  s15

.endm




.macro KERNEL4x4_SUB

	flds	s8 , [ BO ]
	pld	[ BO , #B_PRE ]

	flds	s0 , [ AO ]
	pld	[ AO , #A_PRE ]
	flds	s1 , [ AO, #4 ]

	fmacs	s16  , s0,  s8
	flds	s2 , [ AO, #8 ]
	fmacs	s17  , s1,  s8
	flds	s3 , [ AO, #12 ]
	fmacs	s18  , s2,  s8
	flds	s9 , [ BO, #4 ]
	fmacs	s19  , s3,  s8

	flds	s10, [ BO, #8 ]
	fmacs	s20  , s0,  s9
	flds	s11, [ BO, #12 ]
	fmacs	s21  , s1,  s9
	fmacs	s22  , s2,  s9
	fmacs	s23  , s3,  s9

	fmacs	s24  , s0,  s10
	fmacs	s25  , s1,  s10
	fmacs	s26  , s2,  s10
	fmacs	s27  , s3,  s10

	fmacs	s28  , s0,  s11
	fmacs	s29  , s1,  s11
	add	AO , AO, #16
	fmacs	s30  , s2,  s11
	add	BO , BO, #16
	fmacs	s31  , s3,  s11

.endm

.macro SAVE4x4

	ldr	r3  , LDC
	add	CO2 , CO1, r3
	flds		s0, ALPHA
	add	r4  , CO2, r3


	fmuls	s8 , s0 , s16
	fmuls	s9 , s0 , s17
	fmuls	s10, s0 , s18
	fmuls	s11, s0 , s19

	fmuls	s12, s0 , s20
	fsts	s8 , [CO1]
	fmuls	s13, s0 , s21
	fsts	s9 , [CO1, #4 ]
	fmuls	s14, s0 , s22
	fsts	s10, [CO1, #8 ]
	fmuls	s15, s0 , s23
	fsts	s11, [CO1, #12 ]


	fmuls	s8 , s0 , s24
	fsts	s12, [CO2]
	fmuls	s9 , s0 , s25
	fsts	s13, [CO2, #4 ]
	fmuls	s10, s0 , s26
	fsts	s14, [CO2, #8 ]
	fmuls	s11, s0 , s27
	fsts	s15, [CO2, #12 ]

	add	CO2, r4 , r3

	fsts	s8 , [r4 ]
	fmuls	s12, s0 , s28
	fsts	s9 , [r4 , #4 ]
	fmuls	s13, s0 , s29
	fsts	s10, [r4 , #8 ]
	fmuls	s14, s0 , s30
	fsts	s11, [r4 , #12 ]
	fmuls	s15, s0 , s31

	vstmia.f32 CO2, { s12 - s15 }

	add	CO1, CO1, #16

.endm

/******************************************************************************/

.macro INIT2x4

	flds			S16, FP_ZERO
	vmov.f32		s17, s16
	vmov.f32		s20, s16
	vmov.f32		s21, s16
	vmov.f32		s24, s16
	vmov.f32		s25, s16
	vmov.f32		s28, s16
	vmov.f32		s29, s16

.endm



.macro KERNEL2x4_SUB

	flds	s8 , [ BO ]
	flds	s9 , [ BO, #4 ]
	flds	s10, [ BO, #8 ]
	flds	s11, [ BO, #12 ]

	flds	s0 , [ AO ]
	flds	s1 , [ AO, #4 ]

	fmacs	s16  , s0,  s8
	fmacs	s17  , s1,  s8

	fmacs	s20  , s0,  s9
	fmacs	s21  , s1,  s9

	fmacs	s24  , s0,  s10
	fmacs	s25  , s1,  s10

	fmacs	s28  , s0,  s11
	fmacs	s29  , s1,  s11
	add	AO , AO, #8
	add	BO , BO, #16

.endm

.macro SAVE2x4

	ldr	r3  , LDC
	add	CO2 , CO1, r3
	add	r4  , CO2, r3

	flds		s0, ALPHA

	fmuls	s8 , s0 , s16
	fmuls	s9 , s0 , s17

	fsts	s8 , [CO1]
	fsts	s9 , [CO1, #4 ]

	fmuls	s12, s0 , s20
	fmuls	s13, s0 , s21

	fsts	s12, [CO2]
	fsts	s13, [CO2, #4 ]


	fmuls	s8 , s0 , s24
	fmuls	s9 , s0 , s25

	fsts	s8 , [r4 ]
	fsts	s9 , [r4 , #4 ]

	add	CO2, r4 , r3

	fmuls	s12, s0 , s28
	fmuls	s13, s0 , s29

	fsts	s12, [CO2]
	fsts	s13, [CO2, #4 ]

	add	CO1, CO1, #8

.endm


/******************************************************************************/

.macro INIT1x4

	flds			S16, FP_ZERO
	vmov.f32		s20, s16
	vmov.f32		s24, s16
	vmov.f32		s28, s16

.endm



.macro KERNEL1x4_SUB

	flds	s8 , [ BO ]
	flds	s9 , [ BO, #4 ]
	flds	s10, [ BO, #8 ]
	flds	s11, [ BO, #12 ]

	flds	s0 , [ AO ]

	fmacs	s16  , s0,  s8
	fmacs	s20  , s0,  s9
	fmacs	s24  , s0,  s10
	fmacs	s28  , s0,  s11

	add	AO , AO, #4
	add	BO , BO, #16

.endm

.macro SAVE1x4

	ldr	r3  , LDC
	add	CO2 , CO1, r3
	add	r4  , CO2, r3

	flds		s0, ALPHA

	fmuls	s8 , s0 , s16
	fsts	s8 , [CO1]

	fmuls	s12, s0 , s20
	fsts	s12, [CO2]

	fmuls	s8 , s0 , s24
	fsts	s8 , [r4 ]

	add	CO2, r4 , r3

	fmuls	s12, s0 , s28
	fsts	s12, [CO2]

	add	CO1, CO1, #4

.endm

/******************************************************************************/
/******************************************************************************/

.macro INIT4x2

	flds			S16, FP_ZERO
	vmov.f32		s17, s16
	vmov.f32		s18, s16
	vmov.f32		s19, s16
	vmov.f32		s20, s16
	vmov.f32		s21, s16
	vmov.f32		s22, s16
	vmov.f32		s23, s16

.endm



.macro KERNEL4x2_SUB

	flds	s8 , [ BO ]
	flds	s9 , [ BO, #4 ]

	flds	s0 , [ AO ]
	flds	s1 , [ AO, #4 ]
	flds	s2 , [ AO, #8 ]
	flds	s3 , [ AO, #12 ]

	fmacs	s16  , s0,  s8
	fmacs	s17  , s1,  s8
	fmacs	s18  , s2,  s8
	fmacs	s19  , s3,  s8

	fmacs	s20  , s0,  s9
	fmacs	s21  , s1,  s9
	fmacs	s22  , s2,  s9
	fmacs	s23  , s3,  s9

	add	AO , AO, #16
	add	BO , BO, #8

.endm

.macro SAVE4x2

	ldr	r3  , LDC
	add	CO2 , CO1, r3

	flds		s0, ALPHA

	fmuls	s8 , s0 , s16
	fmuls	s9 , s0 , s17
	fmuls	s10, s0 , s18
	fmuls	s11, s0 , s19

	fsts	s8 , [CO1]
	fsts	s9 , [CO1, #4 ]
	fsts	s10, [CO1, #8 ]
	fsts	s11, [CO1, #12 ]

	fmuls	s12, s0 , s20
	fmuls	s13, s0 , s21
	fmuls	s14, s0 , s22
	fmuls	s15, s0 , s23

	fsts	s12, [CO2]
	fsts	s13, [CO2, #4 ]
	fsts	s14, [CO2, #8 ]
	fsts	s15, [CO2, #12 ]

	add	CO1, CO1, #16

.endm


/******************************************************************************/

.macro INIT2x2

	flds			S16, FP_ZERO
	vmov.f32		s17, s16
	vmov.f32		s20, s16
	vmov.f32		s21, s16

.endm



.macro KERNEL2x2_SUB

	flds	s8 , [ BO ]
	flds	s9 , [ BO, #4 ]

	flds	s0 , [ AO ]
	flds	s1 , [ AO, #4 ]

	fmacs	s16  , s0,  s8
	fmacs	s17  , s1,  s8

	fmacs	s20  , s0,  s9
	fmacs	s21  , s1,  s9

	add	AO , AO, #8
	add	BO , BO, #8

.endm

.macro SAVE2x2

	ldr	r3  , LDC
	add	CO2 , CO1, r3

	flds		s0, ALPHA

	fmuls	s8 , s0 , s16
	fmuls	s9 , s0 , s17

	fsts	s8 , [CO1]
	fsts	s9 , [CO1, #4 ]

	fmuls	s12, s0 , s20
	fmuls	s13, s0 , s21

	fsts	s12, [CO2]
	fsts	s13, [CO2, #4 ]

	add	CO1, CO1, #8

.endm

/******************************************************************************/

.macro INIT1x2

	flds			S16, FP_ZERO
	vmov.f32		s20, s16

.endm



.macro KERNEL1x2_SUB

	flds	s8 , [ BO ]
	flds	s9 , [ BO, #4 ]

	flds	s0 , [ AO ]
	fmacs	s16  , s0,  s8
	fmacs	s20  , s0,  s9

	add	AO , AO, #4
	add	BO , BO, #8

.endm

.macro SAVE1x2

	ldr	r3  , LDC
	add	CO2 , CO1, r3

	flds		s0, ALPHA

	fmuls	s8 , s0 , s16
	fsts	s8 , [CO1]

	fmuls	s12, s0 , s20
	fsts	s12, [CO2]

	add	CO1, CO1, #4

.endm

/******************************************************************************/
/******************************************************************************/

.macro INIT4x1

	flds			S16, FP_ZERO
	vmov.f32		s17, s16
	vmov.f32		s18, s16
	vmov.f32		s19, s16

.endm



.macro KERNEL4x1_SUB

	flds	s8 , [ BO ]

	flds	s0 , [ AO ]
	flds	s1 , [ AO, #4 ]
	flds	s2 , [ AO, #8 ]
	flds	s3 , [ AO, #12 ]

	fmacs	s16  , s0,  s8
	fmacs	s17  , s1,  s8
	fmacs	s18  , s2,  s8
	fmacs	s19  , s3,  s8

	add	AO , AO, #16
	add	BO , BO, #4

.endm

.macro SAVE4x1


	flds		s0, ALPHA

	fmuls	s8 , s0 , s16
	fmuls	s9 , s0 , s17
	fmuls	s10, s0 , s18
	fmuls	s11, s0 , s19

	fsts	s8 , [CO1]
	fsts	s9 , [CO1, #4 ]
	fsts	s10, [CO1, #8 ]
	fsts	s11, [CO1, #12 ]

	add	CO1, CO1, #16

.endm




/******************************************************************************/

.macro INIT2x1

	flds			S16, FP_ZERO
	vmov.f32		s17, s16

.endm



.macro KERNEL2x1_SUB

	flds	s8 , [ BO ]

	flds	s0 , [ AO ]
	flds	s1 , [ AO, #4 ]

	fmacs	s16  , s0,  s8
	fmacs	s17  , s1,  s8

	add	AO , AO, #8
	add	BO , BO, #4

.endm

.macro SAVE2x1


	flds		s0, ALPHA

	fmuls	s8 , s0 , s16
	fmuls	s9 , s0 , s17

	fsts	s8 , [CO1]
	fsts	s9 , [CO1, #4 ]

	add	CO1, CO1, #8

.endm

/******************************************************************************/

.macro INIT1x1

	flds			S16, FP_ZERO

.endm



.macro KERNEL1x1_SUB

	flds	s8 , [ BO ]

	flds	s0 , [ AO ]

	fmacs	s16  , s0,  s8

	add	AO , AO, #4
	add	BO , BO, #4

.endm

.macro SAVE1x1


	flds		s0, ALPHA

	fmuls	s8 , s0 , s16
	fsts	s8 , [CO1]

	add	CO1, CO1, #4

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
	vmov	OLD_ALPHA, OLD_ALPHA_SOFTFP
	ldr	OLD_A, OLD_A_SOFTFP
#endif
	str	OLD_M, M
	str	OLD_N, N
	str	OLD_K, K
	str	OLD_A, A
	vstr	OLD_ALPHA, ALPHA

	sub	r3, fp, #128
	vstm	r3, { s8 - s31} 				// store floating point registers

        movs    r4, #0
        str     r4, FP_ZERO
        str     r4, FP_ZERO_1

	ldr	r3, OLD_LDC
	lsl	r3, r3, #2					// ldc = ldc * 4
	str	r3, LDC

        ldr     r3, OFFSET
#ifndef LEFT
        neg     r3 , r3
#endif
        str     r3 , KK

	ldr	BC, B

	ldr	J, N
	asrs	J, J, #2					// J = J / 4
	ble	_L2_BEGIN

_L4_BEGIN:

	ldr	CO1, C						// CO1 = C
	ldr	r4 , LDC
	lsl	r4 , r4 , #2					// LDC * 4
	add	r3 , r4, CO1
	str	r3 , C						// store C

#if defined(LEFT)
        ldr     r3 , OFFSET
        str     r3 , KK
#endif

	ldr	AO, A						// AO = A
        pld     [AO , #A_PRE-64]
        pld     [AO , #A_PRE-32]



_L4_M4_BEGIN:

	ldr	I, M
	asrs	I, I, #2					// I = I / 4
	ble	_L4_M2_BEGIN

_L4_M4_20:

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))

        mov     BO, BC
#else
        mov     BO, BC
        ldr     r3 , KK
        lsls    r4 , r3 , #4                                    // 4 float values
        add     BO , BO , r4
        lsls    r4 , r3 , #4                                    // 4 float values
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
        add     K1, K1, #4        // number of values in AO
#else
        add     K1, K1, #4        // number of values in BO
#endif
        str     K1, KKK
#endif

	asrs	L , K1, #3					// L = L / 8
	cmp	L , #3
	blt	_L4_M4_30
	.align 5



	KERNEL4x4_I
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	sub	L, L, #2

_L4_M4_22:

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	subs	L, L, #1
	bgt	_L4_M4_22

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_E

	b	 _L4_M4_44


_L4_M4_30:
	tst	L, #3
	ble	_L4_M4_40

	tst	L, #2
	ble	_L4_M4_32

	KERNEL4x4_I
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2


	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_E

	b	 _L4_M4_44

_L4_M4_32:

	tst	L, #1
	ble	_L4_M4_40

	KERNEL4x4_I
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_M2

	KERNEL4x4_M1
	KERNEL4x4_M2
	KERNEL4x4_M1
	KERNEL4x4_E

	b	 _L4_M4_44


_L4_M4_40:

	INIT4x4


_L4_M4_44:

	ands	L , K1, #7					// L = L % 8
	ble	_L4_M4_100

_L4_M4_46:

	KERNEL4x4_SUB

	subs	L, L, #1
	bne	_L4_M4_46

_L4_M4_100:

	SAVE4x4

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
        ldr     r3 , K
        ldr     r4 , KKK
        sub     r3 , r3 , r4
        lsls    r4 , r3 , #4                    // 4 float values
        add     BO , BO , r4
        lsls    r4 , r3 , #4                    // 4 float values
        add     AO , AO , r4
#endif

#if defined(LEFT)
        ldr     r3 , KK
        add     r3 , r3 , #4                    // number of values in AO
        str     r3 , KK
#endif




_L4_M4_END:

	subs	I, I, #1
	bne	_L4_M4_20


_L4_M2_BEGIN:

	ldr	I, M
	tst	I , #3
	ble	_L4_END

	tst	I, #2					// I = I / 2
	ble	_L4_M1_BEGIN

_L4_M2_20:

	INIT2x4

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))

        mov     BO, BC
#else
        mov     BO, BC
        ldr     r3 , KK
        lsls    r4 , r3 , #4                                    // 4 float values
        add     BO , BO , r4
        lsls    r4 , r3 , #3                                    // 2 float values
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
        add     K1, K1, #4        // number of values in BO
#endif
        str     K1, KKK
#endif


	asrs	L , K1, #3					// L = L / 8
	ble	_L4_M2_40

_L4_M2_22:

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	subs	L, L, #1
	bgt	_L4_M2_22


_L4_M2_40:

	ands	L , K1, #7					// L = L % 8
	ble	_L4_M2_100

_L4_M2_42:

	KERNEL2x4_SUB

	subs	L, L, #1
	bgt	_L4_M2_42

_L4_M2_100:

	SAVE2x4

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
        ldr     r3 , K
        ldr     r4 , KKK
        sub     r3 , r3 , r4
        lsls    r4 , r3 , #4                    // 4 float values
        add     BO , BO , r4
        lsls    r4 , r3 , #3                    // 2 float values
        add     AO , AO , r4
#endif

#if defined(LEFT)
        ldr     r3 , KK
        add     r3 , r3 , #2                    // number of values in AO
        str     r3 , KK
#endif



_L4_M2_END:


_L4_M1_BEGIN:

	tst	I, #1					// I = I % 2
	ble	_L4_END

_L4_M1_20:

	INIT1x4

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))

        mov     BO, BC
#else
        mov     BO, BC
        ldr     r3 , KK
        lsls    r4 , r3 , #4                                    // 4 float values
        add     BO , BO , r4
        lsls    r4 , r3 , #2                                    // 1 float value
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
        add     K1, K1, #4        // number of values in BO
#endif
        str     K1, KKK
#endif


	asrs	L , K1, #3					// L = L / 8
	ble	_L4_M1_40

_L4_M1_22:
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	subs	L, L, #1
	bgt	_L4_M1_22


_L4_M1_40:

	ands	L , K1, #7					// L = L % 8
	ble	_L4_M1_100

_L4_M1_42:

	KERNEL1x4_SUB

	subs	L, L, #1
	bgt	_L4_M1_42

_L4_M1_100:

	SAVE1x4

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
        ldr     r3 , K
        ldr     r4 , KKK
        sub     r3 , r3 , r4
        lsls    r4 , r3 , #4                    // 4 float values
        add     BO , BO , r4
        lsls    r4 , r3 , #2                    // 1 float value
        add     AO , AO , r4
#endif

#if defined(LEFT)
        ldr     r3 , KK
        add     r3 , r3 , #1                    // number of values in AO
        str     r3 , KK
#endif



_L4_END:

	mov	r3, BC
	ldr	r4, K
	lsl	r4, r4, #4					// k * 4 * 4
	add	r3, r3, r4					// B = B + K * 4 * 4
	mov	BC, r3

#if !defined(LEFT)
        ldr     r3 , KK
        add     r3 , r3 , #4                                    // number of values in BO
        str     r3 , KK
#endif

	subs	J , #1						// j--
	bgt	_L4_BEGIN



/*********************************************************************************************/

_L2_BEGIN:

	ldr	J , N
	tst	J , #3
	ble	_L999

	tst	J , #2
	ble	_L1_BEGIN

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
        //pld     [AO , #A_PRE-96]
        //pld     [AO , #A_PRE-64]
        //pld     [AO , #A_PRE-32]



_L2_M4_BEGIN:

	ldr	I, M
	asrs	I, I, #2					// I = I / 4
	ble	_L2_M2_BEGIN

_L2_M4_20:

	INIT4x2

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))

        mov     BO, BC
#else
        mov     BO, BC
        ldr     r3 , KK
        lsls    r4 , r3 , #3                                    // 2 float values
        add     BO , BO , r4
        lsls    r4 , r3 , #4                                    // 4 float values
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
        add     K1, K1, #4        // number of values in AO
#else
        add     K1, K1, #2        // number of values in BO
#endif
        str     K1, KKK
#endif


	asrs	L , K1, #3					// L = L / 8
	ble	_L2_M4_40
	.align 5

_L2_M4_22:
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	subs	L, L, #1
	bgt	_L2_M4_22


_L2_M4_40:

	ands	L , K1, #7					// L = L % 8
	ble	_L2_M4_100

_L2_M4_42:

	KERNEL4x2_SUB

	subs	L, L, #1
	bgt	_L2_M4_42

_L2_M4_100:

	SAVE4x2

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
        ldr     r3 , K
        ldr     r4 , KKK
        sub     r3 , r3 , r4
        lsls    r4 , r3 , #3                    // 2 float values
        add     BO , BO , r4
        lsls    r4 , r3 , #4                    // 4 float values
        add     AO , AO , r4
#endif

#if defined(LEFT)
        ldr     r3 , KK
        add     r3 , r3 , #4                    // number of values in AO
        str     r3 , KK
#endif



_L2_M4_END:

	subs	I, I, #1
	bgt	_L2_M4_20


_L2_M2_BEGIN:

	ldr	I, M
	tst	I , #3
	ble	_L2_END

	tst	I, #2					// I = I / 2
	ble	_L2_M1_BEGIN

_L2_M2_20:

	INIT2x2

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))

        mov     BO, BC
#else
        mov     BO, BC
        ldr     r3 , KK
        lsls    r4 , r3 , #3                                    // 2 float values
        add     BO , BO , r4
        lsls    r4 , r3 , #3                                    // 2 float values
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
	ble	_L2_M2_40

_L2_M2_22:

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	subs	L, L, #1
	bgt	_L2_M2_22


_L2_M2_40:

	ands	L , K1, #7					// L = L % 8
	ble	_L2_M2_100

_L2_M2_42:

	KERNEL2x2_SUB

	subs	L, L, #1
	bgt	_L2_M2_42

_L2_M2_100:

	SAVE2x2

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
        ldr     r3 , K
        ldr     r4 , KKK
        sub     r3 , r3 , r4
        lsls    r4 , r3 , #3                    // 2 float values
        add     BO , BO , r4
        lsls    r4 , r3 , #3                    // 2 float values
        add     AO , AO , r4
#endif

#if defined(LEFT)
        ldr     r3 , KK
        add     r3 , r3 , #2                    // number of values in AO
        str     r3 , KK
#endif



_L2_M2_END:


_L2_M1_BEGIN:

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
        lsls    r4 , r3 , #3                                    // 2 float values
        add     BO , BO , r4
        lsls    r4 , r3 , #2                                    // 1 float value
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
        lsls    r4 , r3 , #3                    // 2 float values
        add     BO , BO , r4
        lsls    r4 , r3 , #2                    // 1 float value
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
	lsl	r4, r4, #3					// k * 2 * 4
	add	r3, r3, r4					// B = B + K * 2 * 4
	mov	BC, r3

#if !defined(LEFT)
        ldr     r3 , KK
        add     r3 , r3 , #2                                    // number of values in BO
        str     r3 , KK
#endif


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
        //pld     [AO , #A_PRE-96]
        //pld     [AO , #A_PRE-64]
        //pld     [AO , #A_PRE-32]



_L1_M4_BEGIN:

	ldr	I, M
	asrs	I, I, #2					// I = I / 4
	ble	_L1_M2_BEGIN

_L1_M4_20:

	INIT4x1

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))

        mov     BO, BC
#else
        mov     BO, BC
        ldr     r3 , KK
        lsls    r4 , r3 , #2                                    // 1 float value
        add     BO , BO , r4
        lsls    r4 , r3 , #4                                    // 4 float values
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
        add     K1, K1, #4        // number of values in AO
#else
        add     K1, K1, #1        // number of values in BO
#endif
        str     K1, KKK
#endif


	asrs	L , K1, #3					// L = L / 8
	ble	_L1_M4_40
	.align 5

_L1_M4_22:
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	subs	L, L, #1
	bgt	_L1_M4_22


_L1_M4_40:

	ands	L , K1, #7					// L = L % 8
	ble	_L1_M4_100

_L1_M4_42:

	KERNEL4x1_SUB

	subs	L, L, #1
	bgt	_L1_M4_42

_L1_M4_100:

	SAVE4x1

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
        ldr     r3 , K
        ldr     r4 , KKK
        sub     r3 , r3 , r4
        lsls    r4 , r3 , #2                    // 1 float value
        add     BO , BO , r4
        lsls    r4 , r3 , #4                    // 4 float values
        add     AO , AO , r4
#endif

#if defined(LEFT)
        ldr     r3 , KK
        add     r3 , r3 , #4                    // number of values in AO
        str     r3 , KK
#endif




_L1_M4_END:

	subs	I, I, #1
	bgt	_L1_M4_20


_L1_M2_BEGIN:

	ldr	I, M
	tst	I , #3
	ble	_L1_END

	tst	I, #2					// I = I / 2
	ble	_L1_M1_BEGIN

_L1_M2_20:

	INIT2x1

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))

        mov     BO, BC
#else
        mov     BO, BC
        ldr     r3 , KK
        lsls    r4 , r3 , #2                                    // 1 float value
        add     BO , BO , r4
        lsls    r4 , r3 , #3                                    // 2 float values
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
	ble	_L1_M2_40

_L1_M2_22:

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	subs	L, L, #1
	bgt	_L1_M2_22


_L1_M2_40:

	ands	L , K1, #7					// L = L % 8
	ble	_L1_M2_100

_L1_M2_42:

	KERNEL2x1_SUB

	subs	L, L, #1
	bgt	_L1_M2_42

_L1_M2_100:

	SAVE2x1

#if  (defined(LEFT) &&  defined(TRANSA)) || \
    (!defined(LEFT) && !defined(TRANSA))
        ldr     r3 , K
        ldr     r4 , KKK
        sub     r3 , r3 , r4
        lsls    r4 , r3 , #2                    // 1 float value
        add     BO , BO , r4
        lsls    r4 , r3 , #3                    // 2 float values
        add     AO , AO , r4
#endif

#if defined(LEFT)
        ldr     r3 , KK
        add     r3 , r3 , #2                    // number of values in AO
        str     r3 , KK
#endif



_L1_M2_END:


_L1_M1_BEGIN:

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
        lsls    r4 , r3 , #2                                    // 1 float value
        add     BO , BO , r4
        lsls    r4 , r3 , #2                                    // 1 float value
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


_L1_END:


_L999:

	sub	r3, fp, #128
	vldm	r3, { s8 - s31}					// restore floating point registers

	movs	r0, #0						// set return value
	sub	sp, fp, #24
	pop	{r4 - r9, fp}
	bx	lr

	EPILOGUE

