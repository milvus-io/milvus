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
#define	AO3	r8
#define	AO4	r9

#define I	r3
#define	J	r12

#define A_PRE	256

/**************************************************************************************
* Macro definitions
**************************************************************************************/

.macro COPY4x4

	pld	[ AO1, #A_PRE  ]
	pld	[ AO2, #A_PRE  ]
	pld	[ AO3, #A_PRE  ]
	pld	[ AO4, #A_PRE  ]

	fldd	d0 , [ AO1, #0  ]
	fldd	d1 , [ AO2, #0  ]
	fldd	d2 , [ AO3, #0  ]
	fldd	d3 , [ AO4, #0  ]

	fldd	d4 , [ AO1, #8  ]
	fldd	d8 , [ AO1, #16 ]
	fldd	d12, [ AO1, #24 ]

	fldd	d5 , [ AO2, #8  ]
	add	AO1, AO1, #32
	fldd	d9 , [ AO2, #16 ]
	fldd	d13, [ AO2, #24 ]

	fldd	d6 , [ AO3, #8  ]
	add	AO2, AO2, #32
	fldd	d10, [ AO3, #16 ]
	fldd	d14, [ AO3, #24 ]

	fldd	d7 , [ AO4, #8  ]
	add	AO3, AO3, #32
	fldd	d11, [ AO4, #16 ]
	fldd	d15, [ AO4, #24 ]

	vstmia.f64	BO!, { d0 - d3 }
	add	AO4, AO4, #32
	vstmia.f64	BO!, { d4 - d7 }
	vstmia.f64	BO!, { d8 - d15 }

.endm

.macro COPY1x4

	fldd	d0 , [ AO1, #0  ]
	fldd	d1 , [ AO2, #0  ]
	add	AO1, AO1, #8
	fldd	d2 , [ AO3, #0  ]
	add	AO2, AO2, #8
	fldd	d3 , [ AO4, #0  ]

	add	AO3, AO3, #8
	vstmia.f64	BO!, { d0 - d3 }
	add	AO4, AO4, #8

.endm

.macro COPY4x2

	fldd	d0 , [ AO1, #0  ]
	fldd	d2 , [ AO1, #8  ]
	fldd	d4 , [ AO1, #16 ]
	fldd	d6 , [ AO1, #24 ]

	fldd	d1 , [ AO2, #0  ]
	fldd	d3 , [ AO2, #8  ]
	add	AO1, AO1, #32
	fldd	d5 , [ AO2, #16 ]
	fldd	d7 , [ AO2, #24 ]

	vstmia.f64	BO!, { d0 - d7 }
	add	AO2, AO2, #32

.endm


.macro COPY1x2

	fldd	d0 , [ AO1, #0  ]
	fldd	d1 , [ AO2, #0  ]
	add	AO1, AO1, #8

	vstmia.f64	BO!, { d0 - d1 }
	add	AO2, AO2, #8

.endm

.macro COPY4x1

	fldd	d0 , [ AO1, #0  ]
	fldd	d1 , [ AO1, #8  ]
	fldd	d2 , [ AO1, #16 ]
	fldd	d3 , [ AO1, #24 ]

	vstmia.f64	BO!, { d0 - d3 }
	add	AO1, AO1, #32

.endm


.macro COPY1x1

	fldd	d0 , [ AO1, #0  ]

	vstmia.f64	BO!, { d0 }
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


	lsl	r3, r3, #3					// lda = lda * 8
	str	r3, LDA

	sub	r4, fp, #128
	vstm	r4, { d8 - d15} 				// store floating point registers

	ldr	BO, B

dgemm_ncopy_L4_BEGIN:

	asrs	J, N, #2					// J = N / 4
	ble	dgemm_ncopy_L2_BEGIN

dgemm_ncopy_L4_M4_BEGIN:

	mov	AO1, A						// AO1 = A
	ldr	r4 , LDA
	add	AO2, AO1, r4
	add	AO3, AO2, r4
	add	AO4, AO3, r4
	add	A  , AO4, r4					// A = A + 4 * LDA

	asrs	I, M, #2					// I = M / 4
	ble	dgemm_ncopy_L4_M4_40

dgemm_ncopy_L4_M4_20:

	COPY4x4

	subs	I , I , #1
	bne	dgemm_ncopy_L4_M4_20


dgemm_ncopy_L4_M4_40:

	ands	I, M , #3
	ble	dgemm_ncopy_L4_M4_END

dgemm_ncopy_L4_M4_60:

	COPY1x4

	subs	I , I , #1
	bne	dgemm_ncopy_L4_M4_60


dgemm_ncopy_L4_M4_END:

	subs	J , J, #1						// j--
	bne	dgemm_ncopy_L4_M4_BEGIN



/*********************************************************************************************/

dgemm_ncopy_L2_BEGIN:

	tst	N, #3
	ble	dgemm_ncopy_L999

	tst	N, #2
	ble	dgemm_ncopy_L1_BEGIN

dgemm_ncopy_L2_M4_BEGIN:

	mov	AO1, A						// AO1 = A
	ldr	r4 , LDA
	add	AO2, AO1, r4
	add	A  , AO2, r4 					// A = A + 2 * LDA

	asrs	I, M, #2					// I = M / 4
	ble	dgemm_ncopy_L2_M4_40

dgemm_ncopy_L2_M4_20:

	COPY4x2

	subs	I , I , #1
	bne	dgemm_ncopy_L2_M4_20


dgemm_ncopy_L2_M4_40:

	ands	I, M , #3
	ble	dgemm_ncopy_L2_M4_END

dgemm_ncopy_L2_M4_60:

	COPY1x2

	subs	I , I , #1
	bne	dgemm_ncopy_L2_M4_60


dgemm_ncopy_L2_M4_END:


/*********************************************************************************************/

dgemm_ncopy_L1_BEGIN:

	tst	N, #1
	ble	dgemm_ncopy_L999


dgemm_ncopy_L1_M4_BEGIN:

	mov	AO1, A						// AO1 = A
	ldr	r4 , LDA
	add	A  , AO1, r4 					// A = A + 1 * LDA

	asrs	I, M, #2					// I = M / 4
	ble	dgemm_ncopy_L1_M4_40

dgemm_ncopy_L1_M4_20:

	COPY4x1

	subs	I , I , #1
	bne	dgemm_ncopy_L1_M4_20


dgemm_ncopy_L1_M4_40:

	ands	I, M , #3
	ble	dgemm_ncopy_L1_M4_END

dgemm_ncopy_L1_M4_60:

	COPY1x1

	subs	I , I , #1
	bne	dgemm_ncopy_L1_M4_60


dgemm_ncopy_L1_M4_END:



dgemm_ncopy_L999:

	sub	r3, fp, #128
	vldm	r3, { d8 - d15}					// restore floating point registers

	movs	r0, #0						// set return value
	sub	sp, fp, #24
	pop	{r4 - r9, fp}
	bx	lr

	EPILOGUE

