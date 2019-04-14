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
* 2013/11/06 Saar
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
#define	BO3	r9

#define I	r4
#define	J	r12

#define A_PRE	256

/**************************************************************************************
* Macro definitions
**************************************************************************************/

.macro COPY4x4

	pld	[ AO1, #A_PRE  ]
	vldmia.f64	AO1, { d0 - d3 }

	add	r3, AO1, LDA
	pld	[ r3, #A_PRE  ]
	vldmia.f64	r3, { d4 - d7 }

	add	r3, r3, LDA
	pld	[ r3, #A_PRE  ]
	vldmia.f64	r3, { d8 - d11 }

	add	r3, r3, LDA
	pld	[ r3, #A_PRE  ]
	vldmia.f64	r3, { d12 - d15 }

	vstmia.f64	BO1, { d0 - d15 }
	add	AO1, AO1, #32
	add	BO1, BO1, M4

.endm

.macro COPY2x4

	vldmia.f64	AO1, { d0 - d1 }

	add	r3, AO1, LDA
	vldmia.f64	r3, { d2 - d3 }

	add	r3, r3, LDA
	vldmia.f64	r3, { d4 - d5 }

	add	r3, r3, LDA
	vldmia.f64	r3, { d6 - d7 }

	vstmia.f64	BO2, { d0 - d7 }
	add	AO1, AO1, #16
	add	BO2, BO2, #64

.endm

.macro COPY1x4

	vldmia.f64	AO1, { d0 }

	add	r3, AO1, LDA
	vldmia.f64	r3, { d1 }

	add	r3, r3, LDA
	vldmia.f64	r3, { d2 }

	add	r3, r3, LDA
	vldmia.f64	r3, { d3 }

	vstmia.f64	BO3, { d0 - d3 }
	add	AO1, AO1, #8
	add	BO3, BO3, #32

.endm

/*************************************************************************************************************************/

.macro COPY4x2

	pld	[ AO1, #A_PRE  ]
	vldmia.f64	AO1, { d0 - d3 }

	add	r3, AO1, LDA
	pld	[ r3, #A_PRE  ]
	vldmia.f64	r3, { d4 - d7 }

	vstmia.f64	BO1, { d0 - d7 }
	add	AO1, AO1, #32
	add	BO1, BO1, M4

.endm

.macro COPY2x2

	vldmia.f64	AO1, { d0 - d1 }

	add	r3, AO1, LDA
	vldmia.f64	r3, { d2 - d3 }

	vstmia.f64	BO2, { d0 - d3 }
	add	AO1, AO1, #16
	add	BO2, BO2, #32

.endm

.macro COPY1x2

	vldmia.f64	AO1, { d0 }

	add	r3, AO1, LDA
	vldmia.f64	r3, { d1 }

	vstmia.f64	BO3, { d0 - d1 }
	add	AO1, AO1, #8
	add	BO3, BO3, #16

.endm

/*************************************************************************************************************************/

.macro COPY4x1

	pld	[ AO1, #A_PRE  ]
	vldmia.f64	AO1, { d0 - d3 }

	vstmia.f64	BO1, { d0 - d3 }
	add	AO1, AO1, #32
	add	BO1, BO1, M4

.endm

.macro COPY2x1

	vldmia.f64	AO1, { d0 - d1 }

	vstmia.f64	BO2, { d0 - d1 }
	add	AO1, AO1, #16
	add	BO2, BO2, #16

.endm

.macro COPY1x1

	vldmia.f64	AO1, { d0 }

	vstmia.f64	BO3, { d0 }
	add	AO1, AO1, #8
	add	BO3, BO3, #8

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

	lsl	LDA, OLD_LDA, #3				// lda = lda * SIZE

	sub	r4, fp, #128
	vstm	r4, { d8 - d15} 				// store floating point registers

	lsl	r4 , M, #3					// M * SIZE

	ldr	r3, B

	and	BO2 , N , #-4
	and	BO3 , N , #-2

	mul	BO2, BO2, r4
	mul	BO3, BO3, r4

	add	BO2 , BO2, r3
	add	BO3 , BO3, r3

	lsl	M4, M, #5					// M4 = M * 4 * SIZE

dgemm_tcopy_L4_BEGIN:

	asrs	J, M, #2					// J = N / 4
	ble	dgemm_tcopy_L2_BEGIN

dgemm_tcopy_L4_M4_BEGIN:

	ldr	AO1, A						// AO1 = A
	lsl	r3, LDA, #2					// r3 = 4 * LDA
	add	r3, r3 , AO1					// A = A + 4 * LDA
	str	r3, A						// store A

	ldr	BO1, B
	add	r3, BO1, #128					// B = B + 16 * SIZE
	str	r3, B

	asrs	I, N, #2					// I = M / 4
	ble	dgemm_tcopy_L4_M4_40

dgemm_tcopy_L4_M4_20:

	COPY4x4

	subs	I , I , #1
	bne	dgemm_tcopy_L4_M4_20


dgemm_tcopy_L4_M4_40:

	tst	N , #2
	ble	dgemm_tcopy_L4_M4_60

	COPY2x4


dgemm_tcopy_L4_M4_60:

	tst	N, #1
	ble	dgemm_tcopy_L4_M4_END

	COPY1x4


dgemm_tcopy_L4_M4_END:

	subs	J , J, #1						// j--
	bne	dgemm_tcopy_L4_M4_BEGIN



/*********************************************************************************************/

dgemm_tcopy_L2_BEGIN:

	tst	M, #3
	ble	dgemm_tcopy_L999

	tst	M, #2
	ble	dgemm_tcopy_L1_BEGIN

dgemm_tcopy_L2_M4_BEGIN:

	ldr	AO1, A						// AO1 = A
	lsl	r3, LDA, #1					// r3 = 2 * LDA
	add	r3, r3 , AO1					// A = A + 2 * LDA
	str	r3, A						// store A

	ldr	BO1, B
	add	r3, BO1, #64					// B = B + 8 * SIZE
	str	r3, B

	asrs	I, N, #2					// I = M / 4
	ble	dgemm_tcopy_L2_M4_40

dgemm_tcopy_L2_M4_20:

	COPY4x2

	subs	I , I , #1
	bne	dgemm_tcopy_L2_M4_20


dgemm_tcopy_L2_M4_40:

	tst	N , #2
	ble	dgemm_tcopy_L2_M4_60

	COPY2x2

dgemm_tcopy_L2_M4_60:

	tst	N , #1
	ble	dgemm_tcopy_L2_M4_END

	COPY1x2


dgemm_tcopy_L2_M4_END:


/*********************************************************************************************/

dgemm_tcopy_L1_BEGIN:

	tst	M, #1
	ble	dgemm_tcopy_L999


dgemm_tcopy_L1_M4_BEGIN:

	ldr	AO1, A						// AO1 = A
	add	r3, LDA , AO1					// A = A + 1 * LDA
	str	r3, A						// store A

	ldr	BO1, B
	add	r3, BO1, #32					// B = B + 4 * SIZE
	str	r3, B

	asrs	I, N, #2					// I = M / 4
	ble	dgemm_tcopy_L1_M4_40

dgemm_tcopy_L1_M4_20:

	COPY4x1

	subs	I , I , #1
	bne	dgemm_tcopy_L1_M4_20


dgemm_tcopy_L1_M4_40:

	tst	N , #2
	ble	dgemm_tcopy_L1_M4_60

	COPY2x1

dgemm_tcopy_L1_M4_60:

	tst	N , #1
	ble	dgemm_tcopy_L1_M4_END

	COPY1x1


dgemm_tcopy_L1_M4_END:



dgemm_tcopy_L999:

	sub	r3, fp, #128
	vldm	r3, { d8 - d15}					// restore floating point registers

	mov	r0, #0						// set return value
	sub	sp, fp, #24
	pop	{r4 - r9, fp}
	bx	lr

	EPILOGUE

