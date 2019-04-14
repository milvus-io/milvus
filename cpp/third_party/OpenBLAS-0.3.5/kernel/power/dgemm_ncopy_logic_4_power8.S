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
* 2016/04/28 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/


	mr		BO,	B
	srawi.		I,	N,	2
	ble		DCOPYN_L2_BEGIN


DCOPYN_L4_BEGIN:


DCOPYN_L4_LOOP:

	mr		A0,	A
	add		A1,	A0,	LDA
	add		A2,	A1,	LDA
	add		A3,	A2,	LDA
	add		A,	A3,	LDA

DCOPYN_L4x16_BEGIN:

	srawi.		J,	M,	4
	ble		DCOPYN_L4x16_END

DCOPYN_L4x16_LOOP:

	dcbt		A0,	PREA
	dcbt		A1,	PREA
	dcbt		A2,	PREA
	dcbt		A3,	PREA
	COPY_4x16
	addic.		J,	J,	-1
	bgt		DCOPYN_L4x16_LOOP

DCOPYN_L4x16_END:


DCOPYN_L4x8_BEGIN:

	andi.		J,	M,	8
	ble		DCOPYN_L4x8_END
	COPY_4x8

DCOPYN_L4x8_END:


DCOPYN_L4x4_BEGIN:

	andi.		J,	M,	4
	ble		DCOPYN_L4x4_END
	COPY_4x4

DCOPYN_L4x4_END:


DCOPYN_L4x2_BEGIN:

	andi.		J,	M,	2
	ble		DCOPYN_L4x2_END
	COPY_4x2

DCOPYN_L4x2_END:


DCOPYN_L4x1_BEGIN:

	andi.		J,	M,	1
	ble		DCOPYN_L4x1_END
	COPY_4x1

DCOPYN_L4x1_END:


DCOPYN_L4_END:

	addic.		I,	I,	-1
	bgt		DCOPYN_L4_LOOP

DCOPYN_L2_BEGIN:

	andi.		T1,	4,	2
	ble		DCOPYN_L2_END

DCOPYN_L2_LOOP:

	mr		A0,	A
	add		A1,	A0,	LDA
	add		A,	A1,	LDA

DCOPYN_L2x16_BEGIN:

	srawi.		J,	M,	4
	ble		DCOPYN_L2x16_END

DCOPYN_L2x16_LOOP:

	COPY_2x16
	addic.		J,	J,	-1
	bgt		DCOPYN_L2x16_LOOP

DCOPYN_L2x16_END:


DCOPYN_L2x8_BEGIN:

	andi.		J,	M,	8
	ble		DCOPYN_L2x8_END
	COPY_2x8

DCOPYN_L2x8_END:


DCOPYN_L2x4_BEGIN:

	andi.		J,	M,	4
	ble		DCOPYN_L2x4_END
	COPY_2x4

DCOPYN_L2x4_END:


DCOPYN_L2x2_BEGIN:

	andi.		J,	M,	2
	ble		DCOPYN_L2x2_END
	COPY_2x2

DCOPYN_L2x2_END:


DCOPYN_L2x1_BEGIN:

	andi.		J,	M,	1
	ble		DCOPYN_L2x1_END
	COPY_2x1

DCOPYN_L2x1_END:


DCOPYN_L2_END:


DCOPYN_L1_BEGIN:

	andi.		T1,	4,	1
	ble		DCOPYN_L1_END

DCOPYN_L1_LOOP:

	mr		A0,	A
	add		A,	A0,	LDA

DCOPYN_L1x16_BEGIN:

	srawi.		J,	M,	4
	ble		DCOPYN_L1x16_END

DCOPYN_L1x16_LOOP:

	COPY_1x16
	addic.		J,	J,	-1
	bgt		DCOPYN_L1x16_LOOP

DCOPYN_L1x16_END:


DCOPYN_L1x8_BEGIN:

	andi.		J,	M,	8
	ble		DCOPYN_L1x8_END
	COPY_1x8

DCOPYN_L1x8_END:


DCOPYN_L1x4_BEGIN:

	andi.		J,	M,	4
	ble		DCOPYN_L1x4_END
	COPY_1x4

DCOPYN_L1x4_END:


DCOPYN_L1x2_BEGIN:

	andi.		J,	M,	2
	ble		DCOPYN_L1x2_END
	COPY_1x2

DCOPYN_L1x2_END:


DCOPYN_L1x1_BEGIN:

	andi.		J,	M,	1
	ble		DCOPYN_L1x1_END
	COPY_1x1

DCOPYN_L1x1_END:


DCOPYN_L1_END:

