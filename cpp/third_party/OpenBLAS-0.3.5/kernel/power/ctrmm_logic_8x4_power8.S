/***************************************************************************
Copyright (c) 2013-2016, The OpenBLAS Project
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
* 2016/04/04 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
* 	 LAPACK-TEST		: OK
**************************************************************************************/

	srawi.		J,	N,	2
	ble		CTRMM_L4_END

CTRMM_L4_BEGIN:

	mr		CO,	C
	mr		AO,	A
	slwi		T1,	LDC	,	2
	add		C,	C,	T1

#if defined(LEFT)
	mr		KK,	OFFSET		// OFFSET -> KK
#endif

	srawi.		I,	M,	3
	ble		CTRMM_L4x8_END

CTRMM_L4x8_BEGIN:


#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mr		BO,	B					// B -> BO
#else
	mr		BO,	B					// B -> BO
	slwi		T1,	KK,	5				// Number of values in B shifted
	slwi		T2,	KK,	6				// Number of values in A shifted
	add		BO,	BO,	T1				// Add values to BO
	add		AO,	AO,	T2				// Add values to AO
#endif

#if (defined(LEFT) &&  !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub		T1,	K,	KK				// K - KK -> TEMP1
#else
	mr		T1,	KK					// KK -> KTEMP
#ifdef LEFT
	addi		T1,	T1,	8				// KTEMP + Number of values in A -> KTEMP
#else
	addi		T1,	T1,	4				// KTEMP + Number of values in B -> KTEMP
#endif
#endif

	mr		KKK,	T1
	mr		K1,	T1
	srawi.		L,	K1,	3				// KTEMP / 8 -> L
	ble		CTRMM_L4x8_SUB0
	cmpwi		cr0,	L,	1
	ble		CTRMM_L4x8_SUB4

CTRMM_L4x8_LOOP_START:

	dcbt		AO, PRE
	dcbt		BO, PRE
	LOAD4x8_1
	KERNEL4x8_I1
	dcbt		AO, PRE
	KERNEL4x8_2
	KERNEL4x8_1
	dcbt		AO, PRE
	KERNEL4x8_2

	KERNEL4x8_1
	dcbt		AO, PRE
	KERNEL4x8_2
	KERNEL4x8_1
	dcbt		AO, PRE
	dcbt		BO, PRE
	KERNEL4x8_2

	addic.		L,	L,	-2
	ble		CTRMM_L4x8_LOOP_END

	.align 5

CTRMM_L4x8_LOOP:

	KERNEL4x8_1
	dcbt		AO, PRE
	KERNEL4x8_2
	KERNEL4x8_1
	dcbt		AO, PRE
	KERNEL4x8_2

	KERNEL4x8_1
	dcbt		AO, PRE
	KERNEL4x8_2
	KERNEL4x8_1
	dcbt		AO, PRE
	dcbt		BO, PRE
	KERNEL4x8_2

	addic.		L,	L,	-1
	bgt		CTRMM_L4x8_LOOP

CTRMM_L4x8_LOOP_END:

	KERNEL4x8_1
	dcbt		AO, PRE
	KERNEL4x8_2
	KERNEL4x8_1
	dcbt		AO, PRE
	KERNEL4x8_2

	KERNEL4x8_1
	KERNEL4x8_2
	KERNEL4x8_1
	KERNEL4x8_E2

	b		CTRMM_L4x8_SUB1

CTRMM_L4x8_SUB4:

	KERNEL4x8_SUBI1
	KERNEL4x8_SUB1
	KERNEL4x8_SUB1
	KERNEL4x8_SUB1

	KERNEL4x8_SUB1
	KERNEL4x8_SUB1
	KERNEL4x8_SUB1
	KERNEL4x8_SUB1

	b		CTRMM_L4x8_SUB1

CTRMM_L4x8_SUB0:

	andi.		L,	K1,	7						// K1 & 7 -> L

	KERNEL4x8_SUBI1

	addic.		L,	L,	-1
	ble		CTRMM_L4x8_SAVE
	b		CTRMM_L4x8_SUB2

CTRMM_L4x8_SUB1:

	andi.		L,	K1,	7						// K1 & 7 -> L
	ble		CTRMM_L4x8_SAVE

CTRMM_L4x8_SUB2:

	KERNEL4x8_SUB1

	addic.		L,	L,	-1
	bgt		CTRMM_L4x8_SUB2

CTRMM_L4x8_SAVE:

	SAVE4x8

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub		T1,	K,	KKK					// K - KKK -> TEMP1
	slwi		T2,	T1,	5			// TEMP1 * Number of values in B shifted -> TEMP2
	slwi		T1,	T1,	6			// TEMP1 * Number of values in A shifted -> TEMP1
	add		BO,	BO,	T2					// BO += TEMP2 * number of values in B shifted
	add		AO,	AO,	T1					// AO += TEMP1 * number of values in A shifted
#endif

#if defined(LEFT)
	addi		KK,	KK,	8				// KK += Number of values in A
#endif


	addic.		I,	I,	-1
	bgt		CTRMM_L4x8_BEGIN

CTRMM_L4x8_END:

CTRMM_L4x4_BEGIN:
	andi.		T2,	M,	7
	ble		CTRMM_L4x1_END

	andi.		T1,	M,	4
	ble		CTRMM_L4x4_END

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mr		BO,	B					// B -> BO
#else
	mr		BO,	B					// B -> BO
	slwi		T1,	KK,	5				// Number of values in B shifted
	slwi		T2,	KK,	5				// Number of values in A shifted
	add		BO,	BO,	T1				// Add values to BO
	add		AO,	AO,	T2				// Add values to AO
#endif

#if (defined(LEFT) &&  !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub		T1,	K,	KK				// K - KK -> TEMP1
#else
	mr		T1,	KK					// KK -> KTEMP
#ifdef LEFT
	addi		T1,	T1,	4				// KTEMP + Number of values in A -> KTEMP
#else
	addi		T1,	T1,	4				// KTEMP + Number of values in B -> KTEMP
#endif
#endif

	mr		KKK,	T1
	mr		K1,	T1
	srawi.		L,	K1,	3				// KTEMP / 8 -> L
	ble		CTRMM_L4x4_SUB0
	cmpwi		cr0,	L,	1
	ble		CTRMM_L4x4_SUB4

CTRMM_L4x4_LOOP_START:

	LOAD4x4_1
	KERNEL4x4_I1
	KERNEL4x4_2
	KERNEL4x4_1
	KERNEL4x4_2

	KERNEL4x4_1
	KERNEL4x4_2
	KERNEL4x4_1
	KERNEL4x4_2

	addic.		L,	L,	-2
	ble		CTRMM_L4x4_LOOP_END

	.align 5

CTRMM_L4x4_LOOP:

	KERNEL4x4_1
	KERNEL4x4_2
	KERNEL4x4_1
	KERNEL4x4_2

	KERNEL4x4_1
	KERNEL4x4_2
	KERNEL4x4_1
	KERNEL4x4_2

	addic.		L,	L,	-1
	bgt		CTRMM_L4x4_LOOP

CTRMM_L4x4_LOOP_END:

	KERNEL4x4_1
	KERNEL4x4_2
	KERNEL4x4_1
	KERNEL4x4_2

	KERNEL4x4_1
	KERNEL4x4_2
	KERNEL4x4_1
	KERNEL4x4_E2

	b		CTRMM_L4x4_SUB1

CTRMM_L4x4_SUB4:

	KERNEL4x4_SUBI1
	KERNEL4x4_SUB1
	KERNEL4x4_SUB1
	KERNEL4x4_SUB1

	KERNEL4x4_SUB1
	KERNEL4x4_SUB1
	KERNEL4x4_SUB1
	KERNEL4x4_SUB1

	b		CTRMM_L4x4_SUB1

CTRMM_L4x4_SUB0:

	andi.		L,	K1,	7						// K1 & 7 -> L

	KERNEL4x4_SUBI1

	addic.		L,	L,	-1
	ble		CTRMM_L4x4_SAVE
	b		CTRMM_L4x4_SUB2

CTRMM_L4x4_SUB1:

	andi.		L,	K1,	7						// K1 & 7 -> L
	ble		CTRMM_L4x4_SAVE

CTRMM_L4x4_SUB2:

	KERNEL4x4_SUB1

	addic.		L,	L,	-1
	bgt		CTRMM_L4x4_SUB2

CTRMM_L4x4_SAVE:

	SAVE4x4

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub		T1,	K,	KKK					// K - KKK -> TEMP1
	slwi		T2,	T1,	5			// TEMP1 * Number of values in B shifted -> TEMP2
	slwi		T1,	T1,	5			// TEMP1 * Number of values in A shifted -> TEMP1
	add		BO,	BO,	T2					// BO += TEMP2 * number of values in B shifted
	add		AO,	AO,	T1					// AO += TEMP1 * number of values in A shifted
#endif

#if defined(LEFT)
	addi		KK,	KK,	4				// KK += Number of values in A
#endif


CTRMM_L4x4_END:

CTRMM_L4x2_BEGIN:

	andi.		T1,	M,	2
	ble		CTRMM_L4x2_END

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mr		BO,	B					// B -> BO
#else
	mr		BO,	B					// B -> BO
	slwi		T1,	KK,	5				// Number of values in B shifted
	slwi		T2,	KK,	4				// Number of values in A shifted
	add		BO,	BO,	T1				// Add values to BO
	add		AO,	AO,	T2				// Add values to AO
#endif

#if (defined(LEFT) &&  !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub		T1,	K,	KK				// K - KK -> TEMP1
#else
	mr		T1,	KK					// KK -> KTEMP
#ifdef LEFT
	addi		T1,	T1,	2				// KTEMP + Number of values in A -> KTEMP
#else
	addi		T1,	T1,	4				// KTEMP + Number of values in B -> KTEMP
#endif
#endif

	mr		KKK,	T1
	mr		K1,	T1
	srawi.		L,	K1,	3				// KTEMP / 8 -> L
	ble		CTRMM_L4x2_SUB0
	cmpwi		cr0,	L,	1
	ble		CTRMM_L4x2_SUB4

CTRMM_L4x2_LOOP_START:

	LOAD4x2_1
	KERNEL4x2_I1
	KERNEL4x2_2
	KERNEL4x2_1
	KERNEL4x2_2

	KERNEL4x2_1
	KERNEL4x2_2
	KERNEL4x2_1
	KERNEL4x2_2

	addic.		L,	L,	-2
	ble		CTRMM_L4x2_LOOP_END

	.align 5

CTRMM_L4x2_LOOP:

	KERNEL4x2_1
	KERNEL4x2_2
	KERNEL4x2_1
	KERNEL4x2_2

	KERNEL4x2_1
	KERNEL4x2_2
	KERNEL4x2_1
	KERNEL4x2_2

	addic.		L,	L,	-1
	bgt		CTRMM_L4x2_LOOP

CTRMM_L4x2_LOOP_END:

	KERNEL4x2_1
	KERNEL4x2_2
	KERNEL4x2_1
	KERNEL4x2_2

	KERNEL4x2_1
	KERNEL4x2_2
	KERNEL4x2_1
	KERNEL4x2_E2

	b		CTRMM_L4x2_SUB1

CTRMM_L4x2_SUB4:

	KERNEL4x2_SUBI1
	KERNEL4x2_SUB1
	KERNEL4x2_SUB1
	KERNEL4x2_SUB1

	KERNEL4x2_SUB1
	KERNEL4x2_SUB1
	KERNEL4x2_SUB1
	KERNEL4x2_SUB1

	b		CTRMM_L4x2_SUB1

CTRMM_L4x2_SUB0:

	andi.		L,	K1,	7						// K1 & 7 -> L

	KERNEL4x2_SUBI1

	addic.		L,	L,	-1
	ble		CTRMM_L4x2_SAVE
	b		CTRMM_L4x2_SUB2

CTRMM_L4x2_SUB1:

	andi.		L,	K1,	7						// K1 & 7 -> L
	ble		CTRMM_L4x2_SAVE

CTRMM_L4x2_SUB2:

	KERNEL4x2_SUB1

	addic.		L,	L,	-1
	bgt		CTRMM_L4x2_SUB2

CTRMM_L4x2_SAVE:

	SAVE4x2

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub		T1,	K,	KKK					// K - KKK -> TEMP1
	slwi		T2,	T1,	5			// TEMP1 * Number of values in B shifted -> TEMP2
	slwi		T1,	T1,	4			// TEMP1 * Number of values in A shifted -> TEMP1
	add		BO,	BO,	T2					// BO += TEMP2 * number of values in B shifted
	add		AO,	AO,	T1					// AO += TEMP1 * number of values in A shifted
#endif

#if defined(LEFT)
	addi		KK,	KK,	2				// KK += Number of values in A
#endif


CTRMM_L4x2_END:

CTRMM_L4x1_BEGIN:

	andi.		T1,	M,	1
	ble		CTRMM_L4x1_END

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mr		BO,	B					// B -> BO
#else
	mr		BO,	B					// B -> BO
	slwi		T1,	KK,	5				// Number of values in B shifted
	slwi		T2,	KK,	3				// Number of values in A shifted
	add		BO,	BO,	T1				// Add values to BO
	add		AO,	AO,	T2				// Add values to AO
#endif

#if (defined(LEFT) &&  !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub		T1,	K,	KK				// K - KK -> TEMP1
#else
	mr		T1,	KK					// KK -> KTEMP
#ifdef LEFT
	addi		T1,	T1,	1				// KTEMP + Number of values in A -> KTEMP
#else
	addi		T1,	T1,	4				// KTEMP + Number of values in B -> KTEMP
#endif
#endif

	mr		KKK,	T1
	mr		K1,	T1
	srawi.		L,	K1,	3				// KTEMP / 8 -> L
	ble		CTRMM_L4x1_SUB0
	cmpwi		cr0,	L,	1
	ble		CTRMM_L4x1_SUB4

CTRMM_L4x1_LOOP_START:

	LOAD4x1_1
	KERNEL4x1_I1
	KERNEL4x1_2
	KERNEL4x1_1
	KERNEL4x1_2

	KERNEL4x1_1
	KERNEL4x1_2
	KERNEL4x1_1
	KERNEL4x1_2

	addic.		L,	L,	-2
	ble		CTRMM_L4x1_LOOP_END

	.align 5

CTRMM_L4x1_LOOP:

	KERNEL4x1_1
	KERNEL4x1_2
	KERNEL4x1_1
	KERNEL4x1_2

	KERNEL4x1_1
	KERNEL4x1_2
	KERNEL4x1_1
	KERNEL4x1_2

	addic.		L,	L,	-1
	bgt		CTRMM_L4x1_LOOP

CTRMM_L4x1_LOOP_END:

	KERNEL4x1_1
	KERNEL4x1_2
	KERNEL4x1_1
	KERNEL4x1_2

	KERNEL4x1_1
	KERNEL4x1_2
	KERNEL4x1_1
	KERNEL4x1_E2

	b		CTRMM_L4x1_SUB1

CTRMM_L4x1_SUB4:

	KERNEL4x1_SUBI1
	KERNEL4x1_SUB1
	KERNEL4x1_SUB1
	KERNEL4x1_SUB1

	KERNEL4x1_SUB1
	KERNEL4x1_SUB1
	KERNEL4x1_SUB1
	KERNEL4x1_SUB1

	b		CTRMM_L4x1_SUB1

CTRMM_L4x1_SUB0:

	andi.		L,	K1,	7						// K1 & 7 -> L

	KERNEL4x1_SUBI1

	addic.		L,	L,	-1
	ble		CTRMM_L4x1_SAVE
	b		CTRMM_L4x1_SUB2

CTRMM_L4x1_SUB1:

	andi.		L,	K1,	7						// K1 & 7 -> L
	ble		CTRMM_L4x1_SAVE

CTRMM_L4x1_SUB2:

	KERNEL4x1_SUB1

	addic.		L,	L,	-1
	bgt		CTRMM_L4x1_SUB2

CTRMM_L4x1_SAVE:

	SAVE4x1

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub		T1,	K,	KKK					// K - KKK -> TEMP1
	slwi		T2,	T1,	5			// TEMP1 * Number of values in B shifted -> TEMP2
	slwi		T1,	T1,	3			// TEMP1 * Number of values in A shifted -> TEMP1
	add		BO,	BO,	T2					// BO += TEMP2 * number of values in B shifted
	add		AO,	AO,	T1					// AO += TEMP1 * number of values in A shifted
#endif

#if defined(LEFT)
	addi		KK,	KK,	1				// KK += Number of values in A
#endif


CTRMM_L4x1_END:

	slwi		T1,	K,	5
	add		B,	B,	T1

#if !defined(LEFT)
	addi		KK,	KK,	4					// KK += Number of values in B
#endif


	addic.		J,	J,	-1
	bgt		CTRMM_L4_BEGIN

	andi.		T2,	N,	3
	ble		L999_H2

CTRMM_L4_END:

	b		CTRMM_L2_BEGIN

L999_H1:

	b		L999_H2

CTRMM_L2_BEGIN:

	andi.		T1,	N,	2
	ble		CTRMM_L2_END
	mr		CO,	C
	mr		AO,	A
	slwi		T1,	LDC	,	1
	add		C,	C,	T1

#if defined(LEFT)
	mr		KK,	OFFSET		// OFFSET -> KK
#endif

	srawi.		I,	M,	3
	ble		CTRMM_L2x8_END

CTRMM_L2x8_BEGIN:


#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mr		BO,	B					// B -> BO
#else
	mr		BO,	B					// B -> BO
	slwi		T1,	KK,	4				// Number of values in B shifted
	slwi		T2,	KK,	6				// Number of values in A shifted
	add		BO,	BO,	T1				// Add values to BO
	add		AO,	AO,	T2				// Add values to AO
#endif

#if (defined(LEFT) &&  !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub		T1,	K,	KK				// K - KK -> TEMP1
#else
	mr		T1,	KK					// KK -> KTEMP
#ifdef LEFT
	addi		T1,	T1,	8				// KTEMP + Number of values in A -> KTEMP
#else
	addi		T1,	T1,	2				// KTEMP + Number of values in B -> KTEMP
#endif
#endif

	mr		KKK,	T1
	mr		K1,	T1
	srawi.		L,	K1,	3				// KTEMP / 8 -> L
	ble		CTRMM_L2x8_SUB0
	cmpwi		cr0,	L,	1
	ble		CTRMM_L2x8_SUB4

CTRMM_L2x8_LOOP_START:

	LOAD2x8_1
	KERNEL2x8_I1
	KERNEL2x8_2
	KERNEL2x8_1
	KERNEL2x8_2

	KERNEL2x8_1
	KERNEL2x8_2
	KERNEL2x8_1
	KERNEL2x8_2

	addic.		L,	L,	-2
	ble		CTRMM_L2x8_LOOP_END

	.align 5

CTRMM_L2x8_LOOP:

	KERNEL2x8_1
	KERNEL2x8_2
	KERNEL2x8_1
	KERNEL2x8_2

	KERNEL2x8_1
	KERNEL2x8_2
	KERNEL2x8_1
	KERNEL2x8_2

	addic.		L,	L,	-1
	bgt		CTRMM_L2x8_LOOP

CTRMM_L2x8_LOOP_END:

	KERNEL2x8_1
	KERNEL2x8_2
	KERNEL2x8_1
	KERNEL2x8_2

	KERNEL2x8_1
	KERNEL2x8_2
	KERNEL2x8_1
	KERNEL2x8_E2

	b		CTRMM_L2x8_SUB1

CTRMM_L2x8_SUB4:

	KERNEL2x8_SUBI1
	KERNEL2x8_SUB1
	KERNEL2x8_SUB1
	KERNEL2x8_SUB1

	KERNEL2x8_SUB1
	KERNEL2x8_SUB1
	KERNEL2x8_SUB1
	KERNEL2x8_SUB1

	b		CTRMM_L2x8_SUB1

CTRMM_L2x8_SUB0:

	andi.		L,	K1,	7						// K1 & 7 -> L

	KERNEL2x8_SUBI1

	addic.		L,	L,	-1
	ble		CTRMM_L2x8_SAVE
	b		CTRMM_L2x8_SUB2

CTRMM_L2x8_SUB1:

	andi.		L,	K1,	7						// K1 & 7 -> L
	ble		CTRMM_L2x8_SAVE

CTRMM_L2x8_SUB2:

	KERNEL2x8_SUB1

	addic.		L,	L,	-1
	bgt		CTRMM_L2x8_SUB2

CTRMM_L2x8_SAVE:

	SAVE2x8

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub		T1,	K,	KKK					// K - KKK -> TEMP1
	slwi		T2,	T1,	4			// TEMP1 * Number of values in B shifted -> TEMP2
	slwi		T1,	T1,	6			// TEMP1 * Number of values in A shifted -> TEMP1
	add		BO,	BO,	T2					// BO += TEMP2 * number of values in B shifted
	add		AO,	AO,	T1					// AO += TEMP1 * number of values in A shifted
#endif

#if defined(LEFT)
	addi		KK,	KK,	8				// KK += Number of values in A
#endif


	addic.		I,	I,	-1
	bgt		CTRMM_L2x8_BEGIN

CTRMM_L2x8_END:

CTRMM_L2x4_BEGIN:
	andi.		T2,	M,	7
	ble		CTRMM_L2x1_END

	andi.		T1,	M,	4
	ble		CTRMM_L2x4_END

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mr		BO,	B					// B -> BO
#else
	mr		BO,	B					// B -> BO
	slwi		T1,	KK,	4				// Number of values in B shifted
	slwi		T2,	KK,	5				// Number of values in A shifted
	add		BO,	BO,	T1				// Add values to BO
	add		AO,	AO,	T2				// Add values to AO
#endif

#if (defined(LEFT) &&  !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub		T1,	K,	KK				// K - KK -> TEMP1
#else
	mr		T1,	KK					// KK -> KTEMP
#ifdef LEFT
	addi		T1,	T1,	4				// KTEMP + Number of values in A -> KTEMP
#else
	addi		T1,	T1,	2				// KTEMP + Number of values in B -> KTEMP
#endif
#endif

	mr		KKK,	T1
	mr		K1,	T1
	srawi.		L,	K1,	3				// KTEMP / 8 -> L
	ble		CTRMM_L2x4_SUB0
	cmpwi		cr0,	L,	1
	ble		CTRMM_L2x4_SUB4

CTRMM_L2x4_LOOP_START:

	LOAD2x4_1
	KERNEL2x4_I1
	KERNEL2x4_2
	KERNEL2x4_1
	KERNEL2x4_2

	KERNEL2x4_1
	KERNEL2x4_2
	KERNEL2x4_1
	KERNEL2x4_2

	addic.		L,	L,	-2
	ble		CTRMM_L2x4_LOOP_END

	.align 5

CTRMM_L2x4_LOOP:

	KERNEL2x4_1
	KERNEL2x4_2
	KERNEL2x4_1
	KERNEL2x4_2

	KERNEL2x4_1
	KERNEL2x4_2
	KERNEL2x4_1
	KERNEL2x4_2

	addic.		L,	L,	-1
	bgt		CTRMM_L2x4_LOOP

CTRMM_L2x4_LOOP_END:

	KERNEL2x4_1
	KERNEL2x4_2
	KERNEL2x4_1
	KERNEL2x4_2

	KERNEL2x4_1
	KERNEL2x4_2
	KERNEL2x4_1
	KERNEL2x4_E2

	b		CTRMM_L2x4_SUB1

CTRMM_L2x4_SUB4:

	KERNEL2x4_SUBI1
	KERNEL2x4_SUB1
	KERNEL2x4_SUB1
	KERNEL2x4_SUB1

	KERNEL2x4_SUB1
	KERNEL2x4_SUB1
	KERNEL2x4_SUB1
	KERNEL2x4_SUB1

	b		CTRMM_L2x4_SUB1

CTRMM_L2x4_SUB0:

	andi.		L,	K1,	7						// K1 & 7 -> L

	KERNEL2x4_SUBI1

	addic.		L,	L,	-1
	ble		CTRMM_L2x4_SAVE
	b		CTRMM_L2x4_SUB2

CTRMM_L2x4_SUB1:

	andi.		L,	K1,	7						// K1 & 7 -> L
	ble		CTRMM_L2x4_SAVE

CTRMM_L2x4_SUB2:

	KERNEL2x4_SUB1

	addic.		L,	L,	-1
	bgt		CTRMM_L2x4_SUB2

CTRMM_L2x4_SAVE:

	SAVE2x4

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub		T1,	K,	KKK					// K - KKK -> TEMP1
	slwi		T2,	T1,	4			// TEMP1 * Number of values in B shifted -> TEMP2
	slwi		T1,	T1,	5			// TEMP1 * Number of values in A shifted -> TEMP1
	add		BO,	BO,	T2					// BO += TEMP2 * number of values in B shifted
	add		AO,	AO,	T1					// AO += TEMP1 * number of values in A shifted
#endif

#if defined(LEFT)
	addi		KK,	KK,	4				// KK += Number of values in A
#endif


CTRMM_L2x4_END:

CTRMM_L2x2_BEGIN:

	andi.		T1,	M,	2
	ble		CTRMM_L2x2_END

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mr		BO,	B					// B -> BO
#else
	mr		BO,	B					// B -> BO
	slwi		T1,	KK,	4				// Number of values in B shifted
	slwi		T2,	KK,	4				// Number of values in A shifted
	add		BO,	BO,	T1				// Add values to BO
	add		AO,	AO,	T2				// Add values to AO
#endif

#if (defined(LEFT) &&  !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub		T1,	K,	KK				// K - KK -> TEMP1
#else
	mr		T1,	KK					// KK -> KTEMP
#ifdef LEFT
	addi		T1,	T1,	2				// KTEMP + Number of values in A -> KTEMP
#else
	addi		T1,	T1,	2				// KTEMP + Number of values in B -> KTEMP
#endif
#endif

	mr		KKK,	T1
	mr		K1,	T1
	srawi.		L,	K1,	3				// KTEMP / 8 -> L
	ble		CTRMM_L2x2_SUB0
	cmpwi		cr0,	L,	1
	ble		CTRMM_L2x2_SUB4

CTRMM_L2x2_LOOP_START:

	LOAD2x2_1
	KERNEL2x2_I1
	KERNEL2x2_2
	KERNEL2x2_1
	KERNEL2x2_2

	KERNEL2x2_1
	KERNEL2x2_2
	KERNEL2x2_1
	KERNEL2x2_2

	addic.		L,	L,	-2
	ble		CTRMM_L2x2_LOOP_END

	.align 5

CTRMM_L2x2_LOOP:

	KERNEL2x2_1
	KERNEL2x2_2
	KERNEL2x2_1
	KERNEL2x2_2

	KERNEL2x2_1
	KERNEL2x2_2
	KERNEL2x2_1
	KERNEL2x2_2

	addic.		L,	L,	-1
	bgt		CTRMM_L2x2_LOOP

CTRMM_L2x2_LOOP_END:

	KERNEL2x2_1
	KERNEL2x2_2
	KERNEL2x2_1
	KERNEL2x2_2

	KERNEL2x2_1
	KERNEL2x2_2
	KERNEL2x2_1
	KERNEL2x2_E2

	b		CTRMM_L2x2_SUB1

CTRMM_L2x2_SUB4:

	KERNEL2x2_SUBI1
	KERNEL2x2_SUB1
	KERNEL2x2_SUB1
	KERNEL2x2_SUB1

	KERNEL2x2_SUB1
	KERNEL2x2_SUB1
	KERNEL2x2_SUB1
	KERNEL2x2_SUB1

	b		CTRMM_L2x2_SUB1

CTRMM_L2x2_SUB0:

	andi.		L,	K1,	7						// K1 & 7 -> L

	KERNEL2x2_SUBI1

	addic.		L,	L,	-1
	ble		CTRMM_L2x2_SAVE
	b		CTRMM_L2x2_SUB2

CTRMM_L2x2_SUB1:

	andi.		L,	K1,	7						// K1 & 7 -> L
	ble		CTRMM_L2x2_SAVE

CTRMM_L2x2_SUB2:

	KERNEL2x2_SUB1

	addic.		L,	L,	-1
	bgt		CTRMM_L2x2_SUB2

CTRMM_L2x2_SAVE:

	SAVE2x2

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub		T1,	K,	KKK					// K - KKK -> TEMP1
	slwi		T2,	T1,	4			// TEMP1 * Number of values in B shifted -> TEMP2
	slwi		T1,	T1,	4			// TEMP1 * Number of values in A shifted -> TEMP1
	add		BO,	BO,	T2					// BO += TEMP2 * number of values in B shifted
	add		AO,	AO,	T1					// AO += TEMP1 * number of values in A shifted
#endif

#if defined(LEFT)
	addi		KK,	KK,	2				// KK += Number of values in A
#endif


CTRMM_L2x2_END:

CTRMM_L2x1_BEGIN:

	andi.		T1,	M,	1
	ble		CTRMM_L2x1_END

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mr		BO,	B					// B -> BO
#else
	mr		BO,	B					// B -> BO
	slwi		T1,	KK,	4				// Number of values in B shifted
	slwi		T2,	KK,	3				// Number of values in A shifted
	add		BO,	BO,	T1				// Add values to BO
	add		AO,	AO,	T2				// Add values to AO
#endif

#if (defined(LEFT) &&  !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub		T1,	K,	KK				// K - KK -> TEMP1
#else
	mr		T1,	KK					// KK -> KTEMP
#ifdef LEFT
	addi		T1,	T1,	1				// KTEMP + Number of values in A -> KTEMP
#else
	addi		T1,	T1,	2				// KTEMP + Number of values in B -> KTEMP
#endif
#endif

	mr		KKK,	T1
	mr		K1,	T1
	srawi.		L,	K1,	3				// KTEMP / 8 -> L
	ble		CTRMM_L2x1_SUB0
	cmpwi		cr0,	L,	1
	ble		CTRMM_L2x1_SUB4

CTRMM_L2x1_LOOP_START:

	LOAD2x1_1
	KERNEL2x1_I1
	KERNEL2x1_2
	KERNEL2x1_1
	KERNEL2x1_2

	KERNEL2x1_1
	KERNEL2x1_2
	KERNEL2x1_1
	KERNEL2x1_2

	addic.		L,	L,	-2
	ble		CTRMM_L2x1_LOOP_END

	.align 5

CTRMM_L2x1_LOOP:

	KERNEL2x1_1
	KERNEL2x1_2
	KERNEL2x1_1
	KERNEL2x1_2

	KERNEL2x1_1
	KERNEL2x1_2
	KERNEL2x1_1
	KERNEL2x1_2

	addic.		L,	L,	-1
	bgt		CTRMM_L2x1_LOOP

CTRMM_L2x1_LOOP_END:

	KERNEL2x1_1
	KERNEL2x1_2
	KERNEL2x1_1
	KERNEL2x1_2

	KERNEL2x1_1
	KERNEL2x1_2
	KERNEL2x1_1
	KERNEL2x1_E2

	b		CTRMM_L2x1_SUB1

CTRMM_L2x1_SUB4:

	KERNEL2x1_SUBI1
	KERNEL2x1_SUB1
	KERNEL2x1_SUB1
	KERNEL2x1_SUB1

	KERNEL2x1_SUB1
	KERNEL2x1_SUB1
	KERNEL2x1_SUB1
	KERNEL2x1_SUB1

	b		CTRMM_L2x1_SUB1

CTRMM_L2x1_SUB0:

	andi.		L,	K1,	7						// K1 & 7 -> L

	KERNEL2x1_SUBI1

	addic.		L,	L,	-1
	ble		CTRMM_L2x1_SAVE
	b		CTRMM_L2x1_SUB2

CTRMM_L2x1_SUB1:

	andi.		L,	K1,	7						// K1 & 7 -> L
	ble		CTRMM_L2x1_SAVE

CTRMM_L2x1_SUB2:

	KERNEL2x1_SUB1

	addic.		L,	L,	-1
	bgt		CTRMM_L2x1_SUB2

CTRMM_L2x1_SAVE:

	SAVE2x1

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub		T1,	K,	KKK					// K - KKK -> TEMP1
	slwi		T2,	T1,	4			// TEMP1 * Number of values in B shifted -> TEMP2
	slwi		T1,	T1,	3			// TEMP1 * Number of values in A shifted -> TEMP1
	add		BO,	BO,	T2					// BO += TEMP2 * number of values in B shifted
	add		AO,	AO,	T1					// AO += TEMP1 * number of values in A shifted
#endif

#if defined(LEFT)
	addi		KK,	KK,	1				// KK += Number of values in A
#endif


CTRMM_L2x1_END:

	slwi		T1,	K,	4
	add		B,	B,	T1

#if !defined(LEFT)
	addi		KK,	KK,	2					// KK += Number of values in B
#endif


CTRMM_L2_END:

	b		CTRMM_L1_BEGIN

L999_H2:

	b		L999

CTRMM_L1_BEGIN:

	andi.		T1,	N,	1
	ble		CTRMM_L1_END
	mr		CO,	C
	mr		AO,	A

#if defined(LEFT)
	mr		KK,	OFFSET		// OFFSET -> KK
#endif

	srawi.		I,	M,	3
	ble		CTRMM_L1x8_END

CTRMM_L1x8_BEGIN:


#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mr		BO,	B					// B -> BO
#else
	mr		BO,	B					// B -> BO
	slwi		T1,	KK,	3				// Number of values in B shifted
	slwi		T2,	KK,	6				// Number of values in A shifted
	add		BO,	BO,	T1				// Add values to BO
	add		AO,	AO,	T2				// Add values to AO
#endif

#if (defined(LEFT) &&  !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub		T1,	K,	KK				// K - KK -> TEMP1
#else
	mr		T1,	KK					// KK -> KTEMP
#ifdef LEFT
	addi		T1,	T1,	8				// KTEMP + Number of values in A -> KTEMP
#else
	addi		T1,	T1,	1				// KTEMP + Number of values in B -> KTEMP
#endif
#endif

	mr		KKK,	T1
	mr		K1,	T1
	srawi.		L,	K1,	3				// KTEMP / 8 -> L
	ble		CTRMM_L1x8_SUB0
	cmpwi		cr0,	L,	1
	ble		CTRMM_L1x8_SUB4

CTRMM_L1x8_LOOP_START:

	LOAD1x8_1
	KERNEL1x8_I1
	KERNEL1x8_2
	KERNEL1x8_1
	KERNEL1x8_2

	KERNEL1x8_1
	KERNEL1x8_2
	KERNEL1x8_1
	KERNEL1x8_2

	addic.		L,	L,	-2
	ble		CTRMM_L1x8_LOOP_END

	.align 5

CTRMM_L1x8_LOOP:

	KERNEL1x8_1
	KERNEL1x8_2
	KERNEL1x8_1
	KERNEL1x8_2

	KERNEL1x8_1
	KERNEL1x8_2
	KERNEL1x8_1
	KERNEL1x8_2

	addic.		L,	L,	-1
	bgt		CTRMM_L1x8_LOOP

CTRMM_L1x8_LOOP_END:

	KERNEL1x8_1
	KERNEL1x8_2
	KERNEL1x8_1
	KERNEL1x8_2

	KERNEL1x8_1
	KERNEL1x8_2
	KERNEL1x8_1
	KERNEL1x8_E2

	b		CTRMM_L1x8_SUB1

CTRMM_L1x8_SUB4:

	KERNEL1x8_SUBI1
	KERNEL1x8_SUB1
	KERNEL1x8_SUB1
	KERNEL1x8_SUB1

	KERNEL1x8_SUB1
	KERNEL1x8_SUB1
	KERNEL1x8_SUB1
	KERNEL1x8_SUB1

	b		CTRMM_L1x8_SUB1

CTRMM_L1x8_SUB0:

	andi.		L,	K1,	7						// K1 & 7 -> L

	KERNEL1x8_SUBI1

	addic.		L,	L,	-1
	ble		CTRMM_L1x8_SAVE
	b		CTRMM_L1x8_SUB2

CTRMM_L1x8_SUB1:

	andi.		L,	K1,	7						// K1 & 7 -> L
	ble		CTRMM_L1x8_SAVE

CTRMM_L1x8_SUB2:

	KERNEL1x8_SUB1

	addic.		L,	L,	-1
	bgt		CTRMM_L1x8_SUB2

CTRMM_L1x8_SAVE:

	SAVE1x8

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub		T1,	K,	KKK					// K - KKK -> TEMP1
	slwi		T2,	T1,	3			// TEMP1 * Number of values in B shifted -> TEMP2
	slwi		T1,	T1,	6			// TEMP1 * Number of values in A shifted -> TEMP1
	add		BO,	BO,	T2					// BO += TEMP2 * number of values in B shifted
	add		AO,	AO,	T1					// AO += TEMP1 * number of values in A shifted
#endif

#if defined(LEFT)
	addi		KK,	KK,	8				// KK += Number of values in A
#endif


	addic.		I,	I,	-1
	bgt		CTRMM_L1x8_BEGIN

CTRMM_L1x8_END:

CTRMM_L1x4_BEGIN:
	andi.		T2,	M,	7
	ble		CTRMM_L1x1_END

	andi.		T1,	M,	4
	ble		CTRMM_L1x4_END

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mr		BO,	B					// B -> BO
#else
	mr		BO,	B					// B -> BO
	slwi		T1,	KK,	3				// Number of values in B shifted
	slwi		T2,	KK,	5				// Number of values in A shifted
	add		BO,	BO,	T1				// Add values to BO
	add		AO,	AO,	T2				// Add values to AO
#endif

#if (defined(LEFT) &&  !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub		T1,	K,	KK				// K - KK -> TEMP1
#else
	mr		T1,	KK					// KK -> KTEMP
#ifdef LEFT
	addi		T1,	T1,	4				// KTEMP + Number of values in A -> KTEMP
#else
	addi		T1,	T1,	1				// KTEMP + Number of values in B -> KTEMP
#endif
#endif

	mr		KKK,	T1
	mr		K1,	T1
	srawi.		L,	K1,	3				// KTEMP / 8 -> L
	ble		CTRMM_L1x4_SUB0
	cmpwi		cr0,	L,	1
	ble		CTRMM_L1x4_SUB4

CTRMM_L1x4_LOOP_START:

	LOAD1x4_1
	KERNEL1x4_I1
	KERNEL1x4_2
	KERNEL1x4_1
	KERNEL1x4_2

	KERNEL1x4_1
	KERNEL1x4_2
	KERNEL1x4_1
	KERNEL1x4_2

	addic.		L,	L,	-2
	ble		CTRMM_L1x4_LOOP_END

	.align 5

CTRMM_L1x4_LOOP:

	KERNEL1x4_1
	KERNEL1x4_2
	KERNEL1x4_1
	KERNEL1x4_2

	KERNEL1x4_1
	KERNEL1x4_2
	KERNEL1x4_1
	KERNEL1x4_2

	addic.		L,	L,	-1
	bgt		CTRMM_L1x4_LOOP

CTRMM_L1x4_LOOP_END:

	KERNEL1x4_1
	KERNEL1x4_2
	KERNEL1x4_1
	KERNEL1x4_2

	KERNEL1x4_1
	KERNEL1x4_2
	KERNEL1x4_1
	KERNEL1x4_E2

	b		CTRMM_L1x4_SUB1

CTRMM_L1x4_SUB4:

	KERNEL1x4_SUBI1
	KERNEL1x4_SUB1
	KERNEL1x4_SUB1
	KERNEL1x4_SUB1

	KERNEL1x4_SUB1
	KERNEL1x4_SUB1
	KERNEL1x4_SUB1
	KERNEL1x4_SUB1

	b		CTRMM_L1x4_SUB1

CTRMM_L1x4_SUB0:

	andi.		L,	K1,	7						// K1 & 7 -> L

	KERNEL1x4_SUBI1

	addic.		L,	L,	-1
	ble		CTRMM_L1x4_SAVE
	b		CTRMM_L1x4_SUB2

CTRMM_L1x4_SUB1:

	andi.		L,	K1,	7						// K1 & 7 -> L
	ble		CTRMM_L1x4_SAVE

CTRMM_L1x4_SUB2:

	KERNEL1x4_SUB1

	addic.		L,	L,	-1
	bgt		CTRMM_L1x4_SUB2

CTRMM_L1x4_SAVE:

	SAVE1x4

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub		T1,	K,	KKK					// K - KKK -> TEMP1
	slwi		T2,	T1,	3			// TEMP1 * Number of values in B shifted -> TEMP2
	slwi		T1,	T1,	5			// TEMP1 * Number of values in A shifted -> TEMP1
	add		BO,	BO,	T2					// BO += TEMP2 * number of values in B shifted
	add		AO,	AO,	T1					// AO += TEMP1 * number of values in A shifted
#endif

#if defined(LEFT)
	addi		KK,	KK,	4				// KK += Number of values in A
#endif


CTRMM_L1x4_END:

CTRMM_L1x2_BEGIN:

	andi.		T1,	M,	2
	ble		CTRMM_L1x2_END

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mr		BO,	B					// B -> BO
#else
	mr		BO,	B					// B -> BO
	slwi		T1,	KK,	3				// Number of values in B shifted
	slwi		T2,	KK,	4				// Number of values in A shifted
	add		BO,	BO,	T1				// Add values to BO
	add		AO,	AO,	T2				// Add values to AO
#endif

#if (defined(LEFT) &&  !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub		T1,	K,	KK				// K - KK -> TEMP1
#else
	mr		T1,	KK					// KK -> KTEMP
#ifdef LEFT
	addi		T1,	T1,	2				// KTEMP + Number of values in A -> KTEMP
#else
	addi		T1,	T1,	1				// KTEMP + Number of values in B -> KTEMP
#endif
#endif

	mr		KKK,	T1
	mr		K1,	T1
	srawi.		L,	K1,	3				// KTEMP / 8 -> L
	ble		CTRMM_L1x2_SUB0
	cmpwi		cr0,	L,	1
	ble		CTRMM_L1x2_SUB4

CTRMM_L1x2_LOOP_START:

	LOAD1x2_1
	KERNEL1x2_I1
	KERNEL1x2_2
	KERNEL1x2_1
	KERNEL1x2_2

	KERNEL1x2_1
	KERNEL1x2_2
	KERNEL1x2_1
	KERNEL1x2_2

	addic.		L,	L,	-2
	ble		CTRMM_L1x2_LOOP_END

	.align 5

CTRMM_L1x2_LOOP:

	KERNEL1x2_1
	KERNEL1x2_2
	KERNEL1x2_1
	KERNEL1x2_2

	KERNEL1x2_1
	KERNEL1x2_2
	KERNEL1x2_1
	KERNEL1x2_2

	addic.		L,	L,	-1
	bgt		CTRMM_L1x2_LOOP

CTRMM_L1x2_LOOP_END:

	KERNEL1x2_1
	KERNEL1x2_2
	KERNEL1x2_1
	KERNEL1x2_2

	KERNEL1x2_1
	KERNEL1x2_2
	KERNEL1x2_1
	KERNEL1x2_E2

	b		CTRMM_L1x2_SUB1

CTRMM_L1x2_SUB4:

	KERNEL1x2_SUBI1
	KERNEL1x2_SUB1
	KERNEL1x2_SUB1
	KERNEL1x2_SUB1

	KERNEL1x2_SUB1
	KERNEL1x2_SUB1
	KERNEL1x2_SUB1
	KERNEL1x2_SUB1

	b		CTRMM_L1x2_SUB1

CTRMM_L1x2_SUB0:

	andi.		L,	K1,	7						// K1 & 7 -> L

	KERNEL1x2_SUBI1

	addic.		L,	L,	-1
	ble		CTRMM_L1x2_SAVE
	b		CTRMM_L1x2_SUB2

CTRMM_L1x2_SUB1:

	andi.		L,	K1,	7						// K1 & 7 -> L
	ble		CTRMM_L1x2_SAVE

CTRMM_L1x2_SUB2:

	KERNEL1x2_SUB1

	addic.		L,	L,	-1
	bgt		CTRMM_L1x2_SUB2

CTRMM_L1x2_SAVE:

	SAVE1x2

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub		T1,	K,	KKK					// K - KKK -> TEMP1
	slwi		T2,	T1,	3			// TEMP1 * Number of values in B shifted -> TEMP2
	slwi		T1,	T1,	4			// TEMP1 * Number of values in A shifted -> TEMP1
	add		BO,	BO,	T2					// BO += TEMP2 * number of values in B shifted
	add		AO,	AO,	T1					// AO += TEMP1 * number of values in A shifted
#endif

#if defined(LEFT)
	addi		KK,	KK,	2				// KK += Number of values in A
#endif


CTRMM_L1x2_END:

CTRMM_L1x1_BEGIN:

	andi.		T1,	M,	1
	ble		CTRMM_L1x1_END

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	mr		BO,	B					// B -> BO
#else
	mr		BO,	B					// B -> BO
	slwi		T1,	KK,	3				// Number of values in B shifted
	slwi		T2,	KK,	3				// Number of values in A shifted
	add		BO,	BO,	T1				// Add values to BO
	add		AO,	AO,	T2				// Add values to AO
#endif

#if (defined(LEFT) &&  !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	sub		T1,	K,	KK				// K - KK -> TEMP1
#else
	mr		T1,	KK					// KK -> KTEMP
#ifdef LEFT
	addi		T1,	T1,	1				// KTEMP + Number of values in A -> KTEMP
#else
	addi		T1,	T1,	1				// KTEMP + Number of values in B -> KTEMP
#endif
#endif

	mr		KKK,	T1
	mr		K1,	T1
	srawi.		L,	K1,	3				// KTEMP / 8 -> L
	ble		CTRMM_L1x1_SUB0
	cmpwi		cr0,	L,	1
	ble		CTRMM_L1x1_SUB4

CTRMM_L1x1_LOOP_START:

	LOAD1x1_1
	KERNEL1x1_I1
	KERNEL1x1_2
	KERNEL1x1_1
	KERNEL1x1_2

	KERNEL1x1_1
	KERNEL1x1_2
	KERNEL1x1_1
	KERNEL1x1_2

	addic.		L,	L,	-2
	ble		CTRMM_L1x1_LOOP_END

	.align 5

CTRMM_L1x1_LOOP:

	KERNEL1x1_1
	KERNEL1x1_2
	KERNEL1x1_1
	KERNEL1x1_2

	KERNEL1x1_1
	KERNEL1x1_2
	KERNEL1x1_1
	KERNEL1x1_2

	addic.		L,	L,	-1
	bgt		CTRMM_L1x1_LOOP

CTRMM_L1x1_LOOP_END:

	KERNEL1x1_1
	KERNEL1x1_2
	KERNEL1x1_1
	KERNEL1x1_2

	KERNEL1x1_1
	KERNEL1x1_2
	KERNEL1x1_1
	KERNEL1x1_E2

	b		CTRMM_L1x1_SUB1

CTRMM_L1x1_SUB4:

	KERNEL1x1_SUBI1
	KERNEL1x1_SUB1
	KERNEL1x1_SUB1
	KERNEL1x1_SUB1

	KERNEL1x1_SUB1
	KERNEL1x1_SUB1
	KERNEL1x1_SUB1
	KERNEL1x1_SUB1

	b		CTRMM_L1x1_SUB1

CTRMM_L1x1_SUB0:

	andi.		L,	K1,	7						// K1 & 7 -> L

	KERNEL1x1_SUBI1

	addic.		L,	L,	-1
	ble		CTRMM_L1x1_SAVE
	b		CTRMM_L1x1_SUB2

CTRMM_L1x1_SUB1:

	andi.		L,	K1,	7						// K1 & 7 -> L
	ble		CTRMM_L1x1_SAVE

CTRMM_L1x1_SUB2:

	KERNEL1x1_SUB1

	addic.		L,	L,	-1
	bgt		CTRMM_L1x1_SUB2

CTRMM_L1x1_SAVE:

	SAVE1x1

#if  (defined(LEFT) &&  defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
	sub		T1,	K,	KKK					// K - KKK -> TEMP1
	slwi		T2,	T1,	3			// TEMP1 * Number of values in B shifted -> TEMP2
	slwi		T1,	T1,	3			// TEMP1 * Number of values in A shifted -> TEMP1
	add		BO,	BO,	T2					// BO += TEMP2 * number of values in B shifted
	add		AO,	AO,	T1					// AO += TEMP1 * number of values in A shifted
#endif

#if defined(LEFT)
	addi		KK,	KK,	1				// KK += Number of values in A
#endif


CTRMM_L1x1_END:

#if !defined(LEFT)
	addi		KK,	KK,	1					// KK += Number of values in B
#endif


CTRMM_L1_END:
