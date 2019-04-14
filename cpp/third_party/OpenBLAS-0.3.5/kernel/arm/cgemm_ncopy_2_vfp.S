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
**************************************************************************************/

#define ASSEMBLER
#include "common.h"

#define STACKSIZE 256

#define	OLD_M	r0
#define	OLD_N	r1
#define	OLD_A	r2
#define	OLD_LDA	r3


/******************************************************
* [fp, #-128] - [fp, #-64] is reserved
* for store and restore of floating point
* registers
*******************************************************/

#define LDA	[fp, #-260 ]

#define B	[fp, #4 ]

#define M	r0
#define N	r1
#define A	r2

#define	BO	r5

#define	AO1	r6
#define	AO2	r7

#define I	r3
#define	J	r12

#define A_PRE	256

/**************************************************************************************
* Macro definitions
**************************************************************************************/

.macro COPY2x2

	flds	s0 , [ AO1, #0  ]
	flds	s1 , [ AO1, #4  ]
	flds	s4 , [ AO1, #8 ]
	flds	s5 , [ AO1, #12 ]

	flds	s2 , [ AO2, #0  ]
	flds	s3 , [ AO2, #4  ]
	add	AO1, AO1, #16
	flds	s6 , [ AO2, #8 ]
	flds	s7 , [ AO2, #12 ]

	vstmia.f32	BO!, { s0 - s7 }
	add	AO2, AO2, #16

.endm


.macro COPY1x2

	flds	s0 , [ AO1, #0  ]
	flds	s1 , [ AO1, #4  ]
	flds	s2 , [ AO2, #0  ]
	flds	s3 , [ AO2, #4  ]

	add	AO1, AO1, #8
	vstmia.f32	BO!, { s0 - s3 }
	add	AO2, AO2, #8

.endm

.macro COPY2x1

	flds	s0 , [ AO1, #0  ]
	flds	s1 , [ AO1, #4  ]
	flds	s2 , [ AO1, #8 ]
	flds	s3 , [ AO1, #12 ]

	vstmia.f32	BO!, { s0 - s3 }
	add	AO1, AO1, #16

.endm


.macro COPY1x1

	flds	s0 , [ AO1, #0  ]
	flds	s1 , [ AO1, #4  ]

	vstmia.f32	BO!, { s0 - s1 }
	add	AO1, AO1, #8

.endm





/**************************************************************************************
* End of macro definitions
**************************************************************************************/

	PROLOGUE

	.align 5

	push	{r4 - r9, fp}
	add	fp, sp, #24
	sub	sp, sp, #STACKSIZE				// reserve stack


	lsl	r3, r3, #3					// lda = lda * 4 * 2
	str	r3, LDA

	sub	r4, fp, #128
	vstm	r4, { s8 - s15} 				// store floating point registers

	ldr	BO, B

/*********************************************************************************************/

cgemm_ncopy_L2_BEGIN:

	asrs	J, N, #1					// J = N / 2
	ble	cgemm_ncopy_L1_BEGIN

cgemm_ncopy_L2_M2_BEGIN:

	mov	AO1, A						// AO1 = A
	ldr	r4 , LDA
	add	AO2, AO1, r4
	add	A  , AO2, r4 					// A = A + 2 * LDA

	asrs	I, M, #1					// I = M / 2
	ble	cgemm_ncopy_L2_M2_40

cgemm_ncopy_L2_M2_20:

	pld	[ AO1, #A_PRE ]
	pld	[ AO2, #A_PRE ]

	COPY2x2
	subs	I , I , #1
	ble	cgemm_ncopy_L2_M2_40

	COPY2x2
	subs	I , I , #1
	bne	cgemm_ncopy_L2_M2_20


cgemm_ncopy_L2_M2_40:

	ands	I, M , #1
	ble	cgemm_ncopy_L2_M2_END

cgemm_ncopy_L2_M2_60:

	COPY1x2

	subs	I , I , #1
	bne	cgemm_ncopy_L2_M2_60


cgemm_ncopy_L2_M2_END:

	subs    J , J, #1                                               // j--
        bne     cgemm_ncopy_L2_M2_BEGIN


/*********************************************************************************************/

cgemm_ncopy_L1_BEGIN:

	tst	N, #1
	ble	cgemm_ncopy_L999


cgemm_ncopy_L1_M2_BEGIN:

	mov	AO1, A						// AO1 = A
	ldr	r4 , LDA
	add	A  , AO1, r4 					// A = A + 1 * LDA

	asrs	I, M, #1					// I = M / 2
	ble	cgemm_ncopy_L1_M2_40

cgemm_ncopy_L1_M2_20:

	COPY2x1

	subs	I , I , #1
	bne	cgemm_ncopy_L1_M2_20


cgemm_ncopy_L1_M2_40:

	ands	I, M , #1
	ble	cgemm_ncopy_L1_M2_END

cgemm_ncopy_L1_M2_60:

	COPY1x1

	subs	I , I , #1
	bne	cgemm_ncopy_L1_M2_60


cgemm_ncopy_L1_M2_END:



cgemm_ncopy_L999:

	sub	r3, fp, #128
	vldm	r3, { s8 - s15}					// restore floating point registers

	movs	r0, #0						// set return value
	sub	sp, fp, #24
	pop	{r4 - r9, fp}
	bx	lr

	EPILOGUE

