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
* 2013/11/07 Saar
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

#define B	[fp, #4 ]
#define A	[fp, #-248 ]

#define M	r0
#define N	r1
#define M4	r2

#define	LDA	r5

#define	AO1	r6
#define	BO1	r7
#define	BO2	r8

#define I	r4
#define	J	r12

#define A_PRE	256

/**************************************************************************************
* Macro definitions
**************************************************************************************/
.macro COPY2x2

	vldmia.f32	AO1, { s0 - s3 }

	add	r3, AO1, LDA
	vldmia.f32	r3, { s4 - s7 }

	vstmia.f32	BO1, { s0 - s7 }
	add	AO1, AO1, #16
	add	BO1, BO1, M4

.endm

.macro COPY1x2

	vldmia.f32	AO1, { s0 -s1 }

	add	r3, AO1, LDA
	vldmia.f32	r3, { s2 - s3 }

	vstmia.f32	BO2, { s0 - s3 }
	add	AO1, AO1, #8
	add	BO2, BO2, #16

.endm

/*************************************************************************************************************************/
.macro COPY2x1

	vldmia.f32	AO1, { s0 - s3 }

	vstmia.f32	BO1, { s0 - s3 }
	add	AO1, AO1, #16
	add	BO1, BO1, M4

.endm

.macro COPY1x1

	vldmia.f32	AO1, { s0 - s1 }

	vstmia.f32	BO2, { s0 - s1 }
	add	AO1, AO1, #8
	add	BO2, BO2, #8

.endm



/**************************************************************************************
* End of macro definitions
**************************************************************************************/

	PROLOGUE

	.align 5

	push	{r4 - r9, fp}
	add	fp, sp, #24
	sub	sp, sp, #STACKSIZE				// reserve stack

	str	OLD_A, A					// store A

	lsl	LDA, OLD_LDA, #3				// lda = lda * SIZE * 2

	sub	r4, fp, #128
	vstm	r4, { s8 - s15} 				// store floating point registers

	lsl	r4 , M, #3					// M * SIZE * 2

	ldr	r3, B

	and	BO2 , N , #-2

	mul	BO2, BO2, r4

	add	BO2 , BO2, r3

	lsl	M4, M, #4					// M4 = M * 2 * SIZE * 2

cgemm_tcopy_L2_BEGIN:

	asrs	J, M, #1					// J = N / 2
	ble	cgemm_tcopy_L1_BEGIN

cgemm_tcopy_L2_M2_BEGIN:

	ldr	AO1, A						// AO1 = A
	lsl	r3, LDA, #1					// r3 = 2 * LDA
	add	r3, r3 , AO1					// A = A + 2 * LDA
	str	r3, A						// store A

	ldr	BO1, B
	add	r3, BO1, #32					// B = B + 4 * SIZE *2
	str	r3, B

	asrs	I, N, #1					// I = M / 2
	ble	cgemm_tcopy_L2_M2_60

cgemm_tcopy_L2_M2_40:

	COPY2x2
	subs I, I, #1
	bne	cgemm_tcopy_L2_M2_40

cgemm_tcopy_L2_M2_60:

	tst	N , #1
	ble	cgemm_tcopy_L2_M2_END

	COPY1x2


cgemm_tcopy_L2_M2_END:

	subs	J , J, #1						// j--
	bne	cgemm_tcopy_L2_M2_BEGIN

/*********************************************************************************************/

cgemm_tcopy_L1_BEGIN:

	tst	M, #1
	ble	cgemm_tcopy_L999


cgemm_tcopy_L1_M2_BEGIN:

	ldr	AO1, A						// AO1 = A
	add	r3, LDA , AO1					// A = A + 1 * LDA
	str	r3, A						// store A

	ldr	BO1, B
	add	r3, BO1, #16					// B = B + 2 * SIZE *2
	str	r3, B

	asrs	I, N, #1					// I = M / 2
	ble	cgemm_tcopy_L1_M2_60


cgemm_tcopy_L1_M2_40:

	COPY2x1
	subs I, I, #1
	bne	cgemm_tcopy_L1_M2_40

cgemm_tcopy_L1_M2_60:

	tst	N , #1
	ble	cgemm_tcopy_L1_M2_END

	COPY1x1


cgemm_tcopy_L1_M2_END:



cgemm_tcopy_L999:

	sub	r3, fp, #128
	vldm	r3, { s8 - s15}					// restore floating point registers

	mov	r0, #0						// set return value
	sub	sp, fp, #24
	pop	{r4 - r9, fp}
	bx	lr

	EPILOGUE

