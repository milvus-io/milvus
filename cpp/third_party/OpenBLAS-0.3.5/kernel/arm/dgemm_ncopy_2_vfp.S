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
* 2013/11/24 Saar
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

#define B	[fp, #4 ]

#define M	r0
#define N	r1
#define A	r2

#define	BO	r5

#define	AO1	r6
#define	AO2	r7
#define	LDA	r8

#define I	r3
#define	J	r12

#define A_PRE	256

/**************************************************************************************
* Macro definitions
**************************************************************************************/

.macro COPY2x2

	fldd	d0 , [ AO1, #0  ]
	fldd	d2 , [ AO1, #8  ]

	fldd	d1 , [ AO2, #0  ]
	fldd	d3 , [ AO2, #8  ]

	add	AO1, AO1, #16
	vstmia.f64	BO!, { d0 - d3 }
	add	AO2, AO2, #16

.endm


.macro COPY1x2

	fldd	d0 , [ AO1, #0  ]
	fldd	d1 , [ AO2, #0  ]
	add	AO1, AO1, #8

	vstmia.f64	BO!, { d0 - d1 }
	add	AO2, AO2, #8

.endm

.macro COPY2x1

	fldd	d0 , [ AO1, #0  ]
	fldd	d1 , [ AO1, #8  ]

	vstmia.f64	BO!, { d0 - d1 }
	add	AO1, AO1, #16

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

	lsl	LDA, OLD_LDA, #3					// lda = lda * 8

	ldr	BO, B


/*********************************************************************************************/

dgemm_ncopy_L2_BEGIN:

	asrs	J, N, #1					// J = N / 2
	ble	dgemm_ncopy_L1_BEGIN

dgemm_ncopy_L2_M2_BEGIN:

	mov	AO1, A						// AO1 = A
	add	AO2, AO1, LDA
	add	A  , AO2, LDA 					// A = A + 2 * LDA

	asrs	I, M, #1					// I = M / 2
	ble	dgemm_ncopy_L2_M2_40

dgemm_ncopy_L2_M2_20:

	COPY2x2

	subs	I , I , #1
	bne	dgemm_ncopy_L2_M2_20


dgemm_ncopy_L2_M2_40:

	ands	I, M , #1
	ble	dgemm_ncopy_L2_M2_END

dgemm_ncopy_L2_M2_60:

	COPY1x2

	subs	I , I , #1
	bne	dgemm_ncopy_L2_M2_60


dgemm_ncopy_L2_M2_END:

	subs	J , J, #1						// j--
	bne	dgemm_ncopy_L2_M2_BEGIN

/*********************************************************************************************/

dgemm_ncopy_L1_BEGIN:

	tst	N, #1
	ble	dgemm_ncopy_L999


dgemm_ncopy_L1_M2_BEGIN:

	mov	AO1, A						// AO1 = A
	add	A  , AO1, LDA 					// A = A + 1 * LDA

	asrs	I, M, #1					// I = M / 2
	ble	dgemm_ncopy_L1_M2_40

dgemm_ncopy_L1_M2_20:

	COPY2x1

	subs	I , I , #1
	bne	dgemm_ncopy_L1_M2_20


dgemm_ncopy_L1_M2_40:

	ands	I, M , #1
	ble	dgemm_ncopy_L1_M2_END

dgemm_ncopy_L1_M2_60:

	COPY1x1

	subs	I , I , #1
	bne	dgemm_ncopy_L1_M2_60


dgemm_ncopy_L1_M2_END:



dgemm_ncopy_L999:


	movs	r0, #0						// set return value
	sub	sp, fp, #24
	pop	{r4 - r9, fp}
	bx	lr

	EPILOGUE

