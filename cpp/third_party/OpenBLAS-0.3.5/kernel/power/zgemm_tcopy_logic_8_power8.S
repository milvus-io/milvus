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
* 2016/04/22 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/


	srawi.		I,	M,	2
	ble		ZCOPYT_L2_BEGIN


ZCOPYT_L4_BEGIN:

	mr		A0,	A
	add		A1,	A0,	LDA
	add		A2,	A1,	LDA
	add		A3,	A2,	LDA
	add		A,	A3,	LDA
	mr		B8,	B
	addi		B,	B,	64*SIZE

	sradi.		J,	N,	3
	ble		ZCOPYT_L4x4_BEGIN

	mr		BO,	B8

	.align 5

ZCOPYT_L4x8_LOOP:

	addi    T1,     PREB,   128
        addi    T2,     PREB,   256
        dcbt    A0,     PREA
        dcbt    A1,     PREA
        dcbt    A2,     PREA
        dcbt    A3,     PREA
        dcbtst  BO,     M8
        dcbtst  BO,     PREB
        dcbtst  BO,     T1
        dcbtst  BO,     T2

	COPY_4x8

	add		BO,	BO,	M8

	addic.		J,	J,	-1
	bgt		ZCOPYT_L4x8_LOOP

ZCOPYT_L4x4_BEGIN:

	andi.		T1,	N,	4
	ble		ZCOPYT_L4x2_BEGIN

	mr		BO,	B4

	COPY_4x4


	addi		B4,	B4,	32*SIZE

ZCOPYT_L4x2_BEGIN:

	andi.		T1,	N,	2
	ble		ZCOPYT_L4x1_BEGIN

	mr		BO,	B2

	COPY_4x2


	addi		B2,	B2,	16*SIZE

ZCOPYT_L4x1_BEGIN:

	andi.		T1,	N,	1
	ble		ZCOPYT_L4_END

	mr		BO,	B1

	COPY_4x1


	addi		B1,	B1,	8*SIZE

ZCOPYT_L4_END:

	addic.		I,	I,	-1
	bgt		ZCOPYT_L4_BEGIN



ZCOPYT_L2_BEGIN:

	andi.		T1,	M,	2
	ble		ZCOPYT_L1_BEGIN

	mr		A0,	A
	add		A1,	A0,	LDA
	add		A,	A1,	LDA
	mr		B8,	B
	addi		B,	B,	32*SIZE

	sradi.		J,	N,	3
	ble		ZCOPYT_L2x4_BEGIN

	mr		BO,	B8

ZCOPYT_L2x8_LOOP:

	COPY_2x8

	add		BO,	BO,	M8

	addic.		J,	J,	-1
	bgt		ZCOPYT_L2x8_LOOP

ZCOPYT_L2x4_BEGIN:

	andi.		T1,	N,	4
	ble		ZCOPYT_L2x2_BEGIN

	mr		BO,	B4

	COPY_2x4


	addi		B4,	B4,	16*SIZE

ZCOPYT_L2x2_BEGIN:

	andi.		T1,	N,	2
	ble		ZCOPYT_L2x1_BEGIN

	mr		BO,	B2

	COPY_2x2


	addi		B2,	B2,	8*SIZE

ZCOPYT_L2x1_BEGIN:

	andi.		T1,	N,	1
	ble		ZCOPYT_L2_END

	mr		BO,	B1

	COPY_2x1


	addi		B1,	B1,	4*SIZE

ZCOPYT_L2_END:


ZCOPYT_L1_BEGIN:

	andi.		T1,	M,	1
	ble		L999

	mr		A0,	A
	add		A,	A0,	LDA
	mr		B8,	B
	addi		B,	B,	16*SIZE

	sradi.		J,	N,	3
	ble		ZCOPYT_L1x4_BEGIN

	mr		BO,	B8

ZCOPYT_L1x8_LOOP:

	COPY_1x8

	add		BO,	BO,	M8

	addic.		J,	J,	-1
	bgt		ZCOPYT_L1x8_LOOP

ZCOPYT_L1x4_BEGIN:

	andi.		T1,	N,	4
	ble		ZCOPYT_L1x2_BEGIN

	mr		BO,	B4

	COPY_1x4


	addi		B4,	B4,	8*SIZE

ZCOPYT_L1x2_BEGIN:

	andi.		T1,	N,	2
	ble		ZCOPYT_L1x1_BEGIN

	mr		BO,	B2

	COPY_1x2


	addi		B2,	B2,	4*SIZE

ZCOPYT_L1x1_BEGIN:

	andi.		T1,	N,	1
	ble		ZCOPYT_L1_END

	mr		BO,	B1

	COPY_1x1


	addi		B1,	B1,	2*SIZE

ZCOPYT_L1_END:

