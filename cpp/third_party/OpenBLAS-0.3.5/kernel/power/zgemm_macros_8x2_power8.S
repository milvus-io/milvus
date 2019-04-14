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

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)

	#define	XSFADD_R1	xsadddp
	#define	XSFADD_R2	xssubdp
	#define	XSFADD_I1	xsadddp
	#define	XSFADD_I2	xsadddp

#elif  defined(CN) || defined(CT) || defined(RN) || defined(RT)

	#define	XSFADD_R1	xsadddp
	#define	XSFADD_R2	xsadddp
	#define	XSFADD_I1	xssubdp
	#define	XSFADD_I2	xsadddp

#elif  defined(NC) || defined(TC) || defined(NR) || defined(TR)

	#define	XSFADD_R1	xsadddp
	#define	XSFADD_R2	xsadddp
	#define	XSFADD_I1	xsadddp
	#define	XSFADD_I2	xssubdp

#else		// CC || CR || RC || RR

	#define	XSFADD_R1	xsadddp
	#define	XSFADD_R2	xssubdp
	#define	XSFADD_I1	xssubdp
	#define	XSFADD_I2	xssubdp

#endif

/**********************************************************************************************
* Macros for N=2 and M=8
**********************************************************************************************/

.macro LOAD2x8_1

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B
	lxvd2x		vs18,	o32,	BO		// load real part from B
	lxvd2x		vs19,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A
	lxvd2x		vs2,	o32,	AO		// load real,imag from A
	lxvd2x		vs3,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs4,	o0,	AO		// load real,imag from A
	lxvd2x		vs5,	o16,	AO		// load real,imag from A
	lxvd2x		vs6,	o32,	AO		// load real,imag from A
	lxvd2x		vs7,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64


.endm

.macro KERNEL2x8_I1

	lxvd2x		vs8,	o0,	AO		// load real,imag from A
	lxvd2x		vs9,	o16,	AO		// load real,imag from A
	lxvd2x		vs10,	o32,	AO		// load real,imag from A
	lxvd2x		vs11,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs12,	o0,	AO		// load real,imag from A
	lxvd2x		vs13,	o16,	AO		// load real,imag from A
	lxvd2x		vs14,	o32,	AO		// load real,imag from A
	lxvd2x		vs15,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs20,	o0,	BO		// load real part from B
	lxvd2x		vs21,	o16,	BO		// load imag part from B
	lxvd2x		vs22,	o32,	BO		// load real part from B
	lxvd2x		vs23,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	xvmuldp		vs32,	vs0,	vs16		// real*real, imag*real
	xvmuldp		vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmuldp		vs34,	vs1,	vs16		// real*real, imag*real
	xvmuldp		vs35,	vs1,	vs17		// real*imag, imag*imag
	xvmuldp		vs36,	vs2,	vs16		// real*real, imag*real
	xvmuldp		vs37,	vs2,	vs17		// real*imag, imag*imag
	xvmuldp		vs38,	vs3,	vs16		// real*real, imag*real
	xvmuldp		vs39,	vs3,	vs17		// real*imag, imag*imag
	xvmuldp		vs40,	vs4,	vs16		// real*real, imag*real
	xvmuldp		vs41,	vs4,	vs17		// real*imag, imag*imag
	xvmuldp		vs42,	vs5,	vs16		// real*real, imag*real
	xvmuldp		vs43,	vs5,	vs17		// real*imag, imag*imag
	xvmuldp		vs44,	vs6,	vs16		// real*real, imag*real
	xvmuldp		vs45,	vs6,	vs17		// real*imag, imag*imag
	xvmuldp		vs46,	vs7,	vs16		// real*real, imag*real
	xvmuldp		vs47,	vs7,	vs17		// real*imag, imag*imag

	xvmuldp		vs48,	vs0,	vs18		// real*real, imag*real
	xvmuldp		vs49,	vs0,	vs19		// real*imag, imag*imag
	xvmuldp		vs50,	vs1,	vs18		// real*real, imag*real
	xvmuldp		vs51,	vs1,	vs19		// real*imag, imag*imag
	xvmuldp		vs52,	vs2,	vs18		// real*real, imag*real
	xvmuldp		vs53,	vs2,	vs19		// real*imag, imag*imag
	xvmuldp		vs54,	vs3,	vs18		// real*real, imag*real
	xvmuldp		vs55,	vs3,	vs19		// real*imag, imag*imag
	xvmuldp		vs56,	vs4,	vs18		// real*real, imag*real
	xvmuldp		vs57,	vs4,	vs19		// real*imag, imag*imag
	xvmuldp		vs58,	vs5,	vs18		// real*real, imag*real
	xvmuldp		vs59,	vs5,	vs19		// real*imag, imag*imag
	xvmuldp		vs60,	vs6,	vs18		// real*real, imag*real
	xvmuldp		vs61,	vs6,	vs19		// real*imag, imag*imag
	xvmuldp		vs62,	vs7,	vs18		// real*real, imag*real
	xvmuldp		vs63,	vs7,	vs19		// real*imag, imag*imag


.endm

.macro KERNEL2x8_1

	lxvd2x		vs8,	o0,	AO		// load real,imag from A
	lxvd2x		vs9,	o16,	AO		// load real,imag from A
	lxvd2x		vs10,	o32,	AO		// load real,imag from A
	lxvd2x		vs11,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs12,	o0,	AO		// load real,imag from A
	lxvd2x		vs13,	o16,	AO		// load real,imag from A
	lxvd2x		vs14,	o32,	AO		// load real,imag from A
	lxvd2x		vs15,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs20,	o0,	BO		// load real part from B
	lxvd2x		vs21,	o16,	BO		// load imag part from B
	lxvd2x		vs22,	o32,	BO		// load real part from B
	lxvd2x		vs23,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	xvmaddadp	vs32,	vs0,	vs16		// real*real, imag*real
	xvmaddadp	vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmaddadp	vs34,	vs1,	vs16		// real*real, imag*real
	xvmaddadp	vs35,	vs1,	vs17		// real*imag, imag*imag
	xvmaddadp	vs36,	vs2,	vs16		// real*real, imag*real
	xvmaddadp	vs37,	vs2,	vs17		// real*imag, imag*imag
	xvmaddadp	vs38,	vs3,	vs16		// real*real, imag*real
	xvmaddadp	vs39,	vs3,	vs17		// real*imag, imag*imag
	xvmaddadp	vs40,	vs4,	vs16		// real*real, imag*real
	xvmaddadp	vs41,	vs4,	vs17		// real*imag, imag*imag
	xvmaddadp	vs42,	vs5,	vs16		// real*real, imag*real
	xvmaddadp	vs43,	vs5,	vs17		// real*imag, imag*imag
	xvmaddadp	vs44,	vs6,	vs16		// real*real, imag*real
	xvmaddadp	vs45,	vs6,	vs17		// real*imag, imag*imag
	xvmaddadp	vs46,	vs7,	vs16		// real*real, imag*real
	xvmaddadp	vs47,	vs7,	vs17		// real*imag, imag*imag

	xvmaddadp	vs48,	vs0,	vs18		// real*real, imag*real
	xvmaddadp	vs49,	vs0,	vs19		// real*imag, imag*imag
	xvmaddadp	vs50,	vs1,	vs18		// real*real, imag*real
	xvmaddadp	vs51,	vs1,	vs19		// real*imag, imag*imag
	xvmaddadp	vs52,	vs2,	vs18		// real*real, imag*real
	xvmaddadp	vs53,	vs2,	vs19		// real*imag, imag*imag
	xvmaddadp	vs54,	vs3,	vs18		// real*real, imag*real
	xvmaddadp	vs55,	vs3,	vs19		// real*imag, imag*imag
	xvmaddadp	vs56,	vs4,	vs18		// real*real, imag*real
	xvmaddadp	vs57,	vs4,	vs19		// real*imag, imag*imag
	xvmaddadp	vs58,	vs5,	vs18		// real*real, imag*real
	xvmaddadp	vs59,	vs5,	vs19		// real*imag, imag*imag
	xvmaddadp	vs60,	vs6,	vs18		// real*real, imag*real
	xvmaddadp	vs61,	vs6,	vs19		// real*imag, imag*imag
	xvmaddadp	vs62,	vs7,	vs18		// real*real, imag*real
	xvmaddadp	vs63,	vs7,	vs19		// real*imag, imag*imag


.endm

.macro KERNEL2x8_2

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A
	lxvd2x		vs2,	o32,	AO		// load real,imag from A
	lxvd2x		vs3,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs4,	o0,	AO		// load real,imag from A
	lxvd2x		vs5,	o16,	AO		// load real,imag from A
	lxvd2x		vs6,	o32,	AO		// load real,imag from A
	lxvd2x		vs7,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B
	lxvd2x		vs18,	o32,	BO		// load real part from B
	lxvd2x		vs19,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	xvmaddadp	vs32,	vs8,	vs20		// real*real, imag*real
	xvmaddadp	vs33,	vs8,	vs21		// real*imag, imag*imag
	xvmaddadp	vs34,	vs9,	vs20		// real*real, imag*real
	xvmaddadp	vs35,	vs9,	vs21		// real*imag, imag*imag
	xvmaddadp	vs36,	vs10,	vs20		// real*real, imag*real
	xvmaddadp	vs37,	vs10,	vs21		// real*imag, imag*imag
	xvmaddadp	vs38,	vs11,	vs20		// real*real, imag*real
	xvmaddadp	vs39,	vs11,	vs21		// real*imag, imag*imag
	xvmaddadp	vs40,	vs12,	vs20		// real*real, imag*real
	xvmaddadp	vs41,	vs12,	vs21		// real*imag, imag*imag
	xvmaddadp	vs42,	vs13,	vs20		// real*real, imag*real
	xvmaddadp	vs43,	vs13,	vs21		// real*imag, imag*imag
	xvmaddadp	vs44,	vs14,	vs20		// real*real, imag*real
	xvmaddadp	vs45,	vs14,	vs21		// real*imag, imag*imag
	xvmaddadp	vs46,	vs15,	vs20		// real*real, imag*real
	xvmaddadp	vs47,	vs15,	vs21		// real*imag, imag*imag

	xvmaddadp	vs48,	vs8,	vs22		// real*real, imag*real
	xvmaddadp	vs49,	vs8,	vs23		// real*imag, imag*imag
	xvmaddadp	vs50,	vs9,	vs22		// real*real, imag*real
	xvmaddadp	vs51,	vs9,	vs23		// real*imag, imag*imag
	xvmaddadp	vs52,	vs10,	vs22		// real*real, imag*real
	xvmaddadp	vs53,	vs10,	vs23		// real*imag, imag*imag
	xvmaddadp	vs54,	vs11,	vs22		// real*real, imag*real
	xvmaddadp	vs55,	vs11,	vs23		// real*imag, imag*imag
	xvmaddadp	vs56,	vs12,	vs22		// real*real, imag*real
	xvmaddadp	vs57,	vs12,	vs23		// real*imag, imag*imag
	xvmaddadp	vs58,	vs13,	vs22		// real*real, imag*real
	xvmaddadp	vs59,	vs13,	vs23		// real*imag, imag*imag
	xvmaddadp	vs60,	vs14,	vs22		// real*real, imag*real
	xvmaddadp	vs61,	vs14,	vs23		// real*imag, imag*imag
	xvmaddadp	vs62,	vs15,	vs22		// real*real, imag*real
	xvmaddadp	vs63,	vs15,	vs23		// real*imag, imag*imag


.endm

.macro KERNEL2x8_E2


	xvmaddadp	vs32,	vs8,	vs20		// real*real, imag*real
	xvmaddadp	vs33,	vs8,	vs21		// real*imag, imag*imag
	xvmaddadp	vs34,	vs9,	vs20		// real*real, imag*real
	xvmaddadp	vs35,	vs9,	vs21		// real*imag, imag*imag
	xvmaddadp	vs36,	vs10,	vs20		// real*real, imag*real
	xvmaddadp	vs37,	vs10,	vs21		// real*imag, imag*imag
	xvmaddadp	vs38,	vs11,	vs20		// real*real, imag*real
	xvmaddadp	vs39,	vs11,	vs21		// real*imag, imag*imag
	xvmaddadp	vs40,	vs12,	vs20		// real*real, imag*real
	xvmaddadp	vs41,	vs12,	vs21		// real*imag, imag*imag
	xvmaddadp	vs42,	vs13,	vs20		// real*real, imag*real
	xvmaddadp	vs43,	vs13,	vs21		// real*imag, imag*imag
	xvmaddadp	vs44,	vs14,	vs20		// real*real, imag*real
	xvmaddadp	vs45,	vs14,	vs21		// real*imag, imag*imag
	xvmaddadp	vs46,	vs15,	vs20		// real*real, imag*real
	xvmaddadp	vs47,	vs15,	vs21		// real*imag, imag*imag

	xvmaddadp	vs48,	vs8,	vs22		// real*real, imag*real
	xvmaddadp	vs49,	vs8,	vs23		// real*imag, imag*imag
	xvmaddadp	vs50,	vs9,	vs22		// real*real, imag*real
	xvmaddadp	vs51,	vs9,	vs23		// real*imag, imag*imag
	xvmaddadp	vs52,	vs10,	vs22		// real*real, imag*real
	xvmaddadp	vs53,	vs10,	vs23		// real*imag, imag*imag
	xvmaddadp	vs54,	vs11,	vs22		// real*real, imag*real
	xvmaddadp	vs55,	vs11,	vs23		// real*imag, imag*imag
	xvmaddadp	vs56,	vs12,	vs22		// real*real, imag*real
	xvmaddadp	vs57,	vs12,	vs23		// real*imag, imag*imag
	xvmaddadp	vs58,	vs13,	vs22		// real*real, imag*real
	xvmaddadp	vs59,	vs13,	vs23		// real*imag, imag*imag
	xvmaddadp	vs60,	vs14,	vs22		// real*real, imag*real
	xvmaddadp	vs61,	vs14,	vs23		// real*imag, imag*imag
	xvmaddadp	vs62,	vs15,	vs22		// real*real, imag*real
	xvmaddadp	vs63,	vs15,	vs23		// real*imag, imag*imag


.endm

.macro KERNEL2x8_SUBI1

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A
	lxvd2x		vs2,	o32,	AO		// load real,imag from A
	lxvd2x		vs3,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs4,	o0,	AO		// load real,imag from A
	lxvd2x		vs5,	o16,	AO		// load real,imag from A
	lxvd2x		vs6,	o32,	AO		// load real,imag from A
	lxvd2x		vs7,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B
	lxvd2x		vs18,	o32,	BO		// load real part from B
	lxvd2x		vs19,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	xvmuldp		vs32,	vs0,	vs16		// real*real, imag*real
	xvmuldp		vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmuldp		vs34,	vs1,	vs16		// real*real, imag*real
	xvmuldp		vs35,	vs1,	vs17		// real*imag, imag*imag
	xvmuldp		vs36,	vs2,	vs16		// real*real, imag*real
	xvmuldp		vs37,	vs2,	vs17		// real*imag, imag*imag
	xvmuldp		vs38,	vs3,	vs16		// real*real, imag*real
	xvmuldp		vs39,	vs3,	vs17		// real*imag, imag*imag
	xvmuldp		vs40,	vs4,	vs16		// real*real, imag*real
	xvmuldp		vs41,	vs4,	vs17		// real*imag, imag*imag
	xvmuldp		vs42,	vs5,	vs16		// real*real, imag*real
	xvmuldp		vs43,	vs5,	vs17		// real*imag, imag*imag
	xvmuldp		vs44,	vs6,	vs16		// real*real, imag*real
	xvmuldp		vs45,	vs6,	vs17		// real*imag, imag*imag
	xvmuldp		vs46,	vs7,	vs16		// real*real, imag*real
	xvmuldp		vs47,	vs7,	vs17		// real*imag, imag*imag

	xvmuldp		vs48,	vs0,	vs18		// real*real, imag*real
	xvmuldp		vs49,	vs0,	vs19		// real*imag, imag*imag
	xvmuldp		vs50,	vs1,	vs18		// real*real, imag*real
	xvmuldp		vs51,	vs1,	vs19		// real*imag, imag*imag
	xvmuldp		vs52,	vs2,	vs18		// real*real, imag*real
	xvmuldp		vs53,	vs2,	vs19		// real*imag, imag*imag
	xvmuldp		vs54,	vs3,	vs18		// real*real, imag*real
	xvmuldp		vs55,	vs3,	vs19		// real*imag, imag*imag
	xvmuldp		vs56,	vs4,	vs18		// real*real, imag*real
	xvmuldp		vs57,	vs4,	vs19		// real*imag, imag*imag
	xvmuldp		vs58,	vs5,	vs18		// real*real, imag*real
	xvmuldp		vs59,	vs5,	vs19		// real*imag, imag*imag
	xvmuldp		vs60,	vs6,	vs18		// real*real, imag*real
	xvmuldp		vs61,	vs6,	vs19		// real*imag, imag*imag
	xvmuldp		vs62,	vs7,	vs18		// real*real, imag*real
	xvmuldp		vs63,	vs7,	vs19		// real*imag, imag*imag


.endm

.macro KERNEL2x8_SUB1

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A
	lxvd2x		vs2,	o32,	AO		// load real,imag from A
	lxvd2x		vs3,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs4,	o0,	AO		// load real,imag from A
	lxvd2x		vs5,	o16,	AO		// load real,imag from A
	lxvd2x		vs6,	o32,	AO		// load real,imag from A
	lxvd2x		vs7,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B
	lxvd2x		vs18,	o32,	BO		// load real part from B
	lxvd2x		vs19,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	xvmaddadp	vs32,	vs0,	vs16		// real*real, imag*real
	xvmaddadp	vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmaddadp	vs34,	vs1,	vs16		// real*real, imag*real
	xvmaddadp	vs35,	vs1,	vs17		// real*imag, imag*imag
	xvmaddadp	vs36,	vs2,	vs16		// real*real, imag*real
	xvmaddadp	vs37,	vs2,	vs17		// real*imag, imag*imag
	xvmaddadp	vs38,	vs3,	vs16		// real*real, imag*real
	xvmaddadp	vs39,	vs3,	vs17		// real*imag, imag*imag
	xvmaddadp	vs40,	vs4,	vs16		// real*real, imag*real
	xvmaddadp	vs41,	vs4,	vs17		// real*imag, imag*imag
	xvmaddadp	vs42,	vs5,	vs16		// real*real, imag*real
	xvmaddadp	vs43,	vs5,	vs17		// real*imag, imag*imag
	xvmaddadp	vs44,	vs6,	vs16		// real*real, imag*real
	xvmaddadp	vs45,	vs6,	vs17		// real*imag, imag*imag
	xvmaddadp	vs46,	vs7,	vs16		// real*real, imag*real
	xvmaddadp	vs47,	vs7,	vs17		// real*imag, imag*imag

	xvmaddadp	vs48,	vs0,	vs18		// real*real, imag*real
	xvmaddadp	vs49,	vs0,	vs19		// real*imag, imag*imag
	xvmaddadp	vs50,	vs1,	vs18		// real*real, imag*real
	xvmaddadp	vs51,	vs1,	vs19		// real*imag, imag*imag
	xvmaddadp	vs52,	vs2,	vs18		// real*real, imag*real
	xvmaddadp	vs53,	vs2,	vs19		// real*imag, imag*imag
	xvmaddadp	vs54,	vs3,	vs18		// real*real, imag*real
	xvmaddadp	vs55,	vs3,	vs19		// real*imag, imag*imag
	xvmaddadp	vs56,	vs4,	vs18		// real*real, imag*real
	xvmaddadp	vs57,	vs4,	vs19		// real*imag, imag*imag
	xvmaddadp	vs58,	vs5,	vs18		// real*real, imag*real
	xvmaddadp	vs59,	vs5,	vs19		// real*imag, imag*imag
	xvmaddadp	vs60,	vs6,	vs18		// real*real, imag*real
	xvmaddadp	vs61,	vs6,	vs19		// real*imag, imag*imag
	xvmaddadp	vs62,	vs7,	vs18		// real*real, imag*real
	xvmaddadp	vs63,	vs7,	vs19		// real*imag, imag*imag


.endm

.macro SAVE2x8


	mr		T1,	CO
	addi		T2,	T1,	64

#ifndef TRMMKERNEL

	lxvd2x		vs16,	o0,	T1
	lxvd2x		vs17,	o16,	T1
	lxvd2x		vs18,	o32,	T1
	lxvd2x		vs19,	o48,	T1
	lxvd2x		vs20,	o0,	T2
	lxvd2x		vs21,	o16,	T2
	lxvd2x		vs22,	o32,	T2
	lxvd2x		vs23,	o48,	T2

#endif


	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs33,	vs33			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs32		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs33		// imagA*imagB

	xxswapd		vs32,	vs32			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs33,	vs33			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs32		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs33		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs8,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs35,	vs35			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs34		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs35		// imagA*imagB

	xxswapd		vs34,	vs34			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs35,	vs35			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs34		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs35		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs9,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs37,	vs37			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs36		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs37		// imagA*imagB

	xxswapd		vs36,	vs36			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs37,	vs37			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs36		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs37		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs10,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs39,	vs39			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs38		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs39		// imagA*imagB

	xxswapd		vs38,	vs38			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs39,	vs39			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs38		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs39		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs11,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs41,	vs41			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs40		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs41		// imagA*imagB

	xxswapd		vs40,	vs40			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs41,	vs41			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs40		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs41		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs12,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs43,	vs43			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs42		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs43		// imagA*imagB

	xxswapd		vs42,	vs42			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs43,	vs43			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs42		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs43		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs13,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs45,	vs45			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs44		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs45		// imagA*imagB

	xxswapd		vs44,	vs44			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs45,	vs45			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs44		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs45		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs14,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs47,	vs47			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs46		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs47		// imagA*imagB

	xxswapd		vs46,	vs46			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs47,	vs47			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs46		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs47		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs15,	vs2,	vs3,	0	// merge real and imag part


#ifndef TRMMKERNEL

	xvadddp		vs8,	vs8,	vs16
	xvadddp		vs9,	vs9,	vs17
	xvadddp		vs10,	vs10,	vs18
	xvadddp		vs11,	vs11,	vs19
	xvadddp		vs12,	vs12,	vs20
	xvadddp		vs13,	vs13,	vs21
	xvadddp		vs14,	vs14,	vs22
	xvadddp		vs15,	vs15,	vs23

#endif

	stxvd2x		vs8,	o0,	T1
	stxvd2x		vs9,	o16,	T1
	stxvd2x		vs10,	o32,	T1
	stxvd2x		vs11,	o48,	T1
	stxvd2x		vs12,	o0,	T2
	stxvd2x		vs13,	o16,	T2
	stxvd2x		vs14,	o32,	T2
	stxvd2x		vs15,	o48,	T2

	add		T1,	T1,	LDC
	add		T2,	T2,	LDC

#ifndef TRMMKERNEL

	lxvd2x		vs16,	o0,	T1
	lxvd2x		vs17,	o16,	T1
	lxvd2x		vs18,	o32,	T1
	lxvd2x		vs19,	o48,	T1
	lxvd2x		vs20,	o0,	T2
	lxvd2x		vs21,	o16,	T2
	lxvd2x		vs22,	o32,	T2
	lxvd2x		vs23,	o48,	T2

#endif


	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs49,	vs49			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs48		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs49		// imagA*imagB

	xxswapd		vs48,	vs48			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs49,	vs49			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs48		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs49		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs8,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs51,	vs51			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs50		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs51		// imagA*imagB

	xxswapd		vs50,	vs50			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs51,	vs51			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs50		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs51		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs9,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs53,	vs53			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs52		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs53		// imagA*imagB

	xxswapd		vs52,	vs52			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs53,	vs53			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs52		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs53		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs10,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs55,	vs55			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs54		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs55		// imagA*imagB

	xxswapd		vs54,	vs54			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs55,	vs55			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs54		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs55		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs11,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs57,	vs57			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs56		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs57		// imagA*imagB

	xxswapd		vs56,	vs56			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs57,	vs57			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs56		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs57		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs12,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs59,	vs59			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs58		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs59		// imagA*imagB

	xxswapd		vs58,	vs58			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs59,	vs59			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs58		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs59		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs13,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs61,	vs61			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs60		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs61		// imagA*imagB

	xxswapd		vs60,	vs60			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs61,	vs61			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs60		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs61		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs14,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs63,	vs63			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs62		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs63		// imagA*imagB

	xxswapd		vs62,	vs62			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs63,	vs63			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs62		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs63		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs15,	vs2,	vs3,	0	// merge real and imag part


#ifndef TRMMKERNEL

	xvadddp		vs8,	vs8,	vs16
	xvadddp		vs9,	vs9,	vs17
	xvadddp		vs10,	vs10,	vs18
	xvadddp		vs11,	vs11,	vs19
	xvadddp		vs12,	vs12,	vs20
	xvadddp		vs13,	vs13,	vs21
	xvadddp		vs14,	vs14,	vs22
	xvadddp		vs15,	vs15,	vs23

#endif

	stxvd2x		vs8,	o0,	T1
	stxvd2x		vs9,	o16,	T1
	stxvd2x		vs10,	o32,	T1
	stxvd2x		vs11,	o48,	T1
	stxvd2x		vs12,	o0,	T2
	stxvd2x		vs13,	o16,	T2
	stxvd2x		vs14,	o32,	T2
	stxvd2x		vs15,	o48,	T2

	add		T1,	T1,	LDC
	add		T2,	T2,	LDC
	addi		CO,	CO,	128

.endm


/**********************************************************************************************
* Macros for N=2 and M=4
**********************************************************************************************/

.macro LOAD2x4_1

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B
	lxvd2x		vs18,	o32,	BO		// load real part from B
	lxvd2x		vs19,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A
	lxvd2x		vs2,	o32,	AO		// load real,imag from A
	lxvd2x		vs3,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64


.endm

.macro KERNEL2x4_I1

	lxvd2x		vs8,	o0,	AO		// load real,imag from A
	lxvd2x		vs9,	o16,	AO		// load real,imag from A
	lxvd2x		vs10,	o32,	AO		// load real,imag from A
	lxvd2x		vs11,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs20,	o0,	BO		// load real part from B
	lxvd2x		vs21,	o16,	BO		// load imag part from B
	lxvd2x		vs22,	o32,	BO		// load real part from B
	lxvd2x		vs23,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	xvmuldp		vs32,	vs0,	vs16		// real*real, imag*real
	xvmuldp		vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmuldp		vs34,	vs1,	vs16		// real*real, imag*real
	xvmuldp		vs35,	vs1,	vs17		// real*imag, imag*imag
	xvmuldp		vs36,	vs2,	vs16		// real*real, imag*real
	xvmuldp		vs37,	vs2,	vs17		// real*imag, imag*imag
	xvmuldp		vs38,	vs3,	vs16		// real*real, imag*real
	xvmuldp		vs39,	vs3,	vs17		// real*imag, imag*imag

	xvmuldp		vs40,	vs0,	vs18		// real*real, imag*real
	xvmuldp		vs41,	vs0,	vs19		// real*imag, imag*imag
	xvmuldp		vs42,	vs1,	vs18		// real*real, imag*real
	xvmuldp		vs43,	vs1,	vs19		// real*imag, imag*imag
	xvmuldp		vs44,	vs2,	vs18		// real*real, imag*real
	xvmuldp		vs45,	vs2,	vs19		// real*imag, imag*imag
	xvmuldp		vs46,	vs3,	vs18		// real*real, imag*real
	xvmuldp		vs47,	vs3,	vs19		// real*imag, imag*imag


.endm

.macro KERNEL2x4_1

	lxvd2x		vs8,	o0,	AO		// load real,imag from A
	lxvd2x		vs9,	o16,	AO		// load real,imag from A
	lxvd2x		vs10,	o32,	AO		// load real,imag from A
	lxvd2x		vs11,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs20,	o0,	BO		// load real part from B
	lxvd2x		vs21,	o16,	BO		// load imag part from B
	lxvd2x		vs22,	o32,	BO		// load real part from B
	lxvd2x		vs23,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	xvmaddadp	vs32,	vs0,	vs16		// real*real, imag*real
	xvmaddadp	vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmaddadp	vs34,	vs1,	vs16		// real*real, imag*real
	xvmaddadp	vs35,	vs1,	vs17		// real*imag, imag*imag
	xvmaddadp	vs36,	vs2,	vs16		// real*real, imag*real
	xvmaddadp	vs37,	vs2,	vs17		// real*imag, imag*imag
	xvmaddadp	vs38,	vs3,	vs16		// real*real, imag*real
	xvmaddadp	vs39,	vs3,	vs17		// real*imag, imag*imag

	xvmaddadp	vs40,	vs0,	vs18		// real*real, imag*real
	xvmaddadp	vs41,	vs0,	vs19		// real*imag, imag*imag
	xvmaddadp	vs42,	vs1,	vs18		// real*real, imag*real
	xvmaddadp	vs43,	vs1,	vs19		// real*imag, imag*imag
	xvmaddadp	vs44,	vs2,	vs18		// real*real, imag*real
	xvmaddadp	vs45,	vs2,	vs19		// real*imag, imag*imag
	xvmaddadp	vs46,	vs3,	vs18		// real*real, imag*real
	xvmaddadp	vs47,	vs3,	vs19		// real*imag, imag*imag


.endm

.macro KERNEL2x4_2

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A
	lxvd2x		vs2,	o32,	AO		// load real,imag from A
	lxvd2x		vs3,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B
	lxvd2x		vs18,	o32,	BO		// load real part from B
	lxvd2x		vs19,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	xvmaddadp	vs32,	vs8,	vs20		// real*real, imag*real
	xvmaddadp	vs33,	vs8,	vs21		// real*imag, imag*imag
	xvmaddadp	vs34,	vs9,	vs20		// real*real, imag*real
	xvmaddadp	vs35,	vs9,	vs21		// real*imag, imag*imag
	xvmaddadp	vs36,	vs10,	vs20		// real*real, imag*real
	xvmaddadp	vs37,	vs10,	vs21		// real*imag, imag*imag
	xvmaddadp	vs38,	vs11,	vs20		// real*real, imag*real
	xvmaddadp	vs39,	vs11,	vs21		// real*imag, imag*imag

	xvmaddadp	vs40,	vs8,	vs22		// real*real, imag*real
	xvmaddadp	vs41,	vs8,	vs23		// real*imag, imag*imag
	xvmaddadp	vs42,	vs9,	vs22		// real*real, imag*real
	xvmaddadp	vs43,	vs9,	vs23		// real*imag, imag*imag
	xvmaddadp	vs44,	vs10,	vs22		// real*real, imag*real
	xvmaddadp	vs45,	vs10,	vs23		// real*imag, imag*imag
	xvmaddadp	vs46,	vs11,	vs22		// real*real, imag*real
	xvmaddadp	vs47,	vs11,	vs23		// real*imag, imag*imag


.endm

.macro KERNEL2x4_E2


	xvmaddadp	vs32,	vs8,	vs20		// real*real, imag*real
	xvmaddadp	vs33,	vs8,	vs21		// real*imag, imag*imag
	xvmaddadp	vs34,	vs9,	vs20		// real*real, imag*real
	xvmaddadp	vs35,	vs9,	vs21		// real*imag, imag*imag
	xvmaddadp	vs36,	vs10,	vs20		// real*real, imag*real
	xvmaddadp	vs37,	vs10,	vs21		// real*imag, imag*imag
	xvmaddadp	vs38,	vs11,	vs20		// real*real, imag*real
	xvmaddadp	vs39,	vs11,	vs21		// real*imag, imag*imag

	xvmaddadp	vs40,	vs8,	vs22		// real*real, imag*real
	xvmaddadp	vs41,	vs8,	vs23		// real*imag, imag*imag
	xvmaddadp	vs42,	vs9,	vs22		// real*real, imag*real
	xvmaddadp	vs43,	vs9,	vs23		// real*imag, imag*imag
	xvmaddadp	vs44,	vs10,	vs22		// real*real, imag*real
	xvmaddadp	vs45,	vs10,	vs23		// real*imag, imag*imag
	xvmaddadp	vs46,	vs11,	vs22		// real*real, imag*real
	xvmaddadp	vs47,	vs11,	vs23		// real*imag, imag*imag


.endm

.macro KERNEL2x4_SUBI1

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A
	lxvd2x		vs2,	o32,	AO		// load real,imag from A
	lxvd2x		vs3,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B
	lxvd2x		vs18,	o32,	BO		// load real part from B
	lxvd2x		vs19,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	xvmuldp		vs32,	vs0,	vs16		// real*real, imag*real
	xvmuldp		vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmuldp		vs34,	vs1,	vs16		// real*real, imag*real
	xvmuldp		vs35,	vs1,	vs17		// real*imag, imag*imag
	xvmuldp		vs36,	vs2,	vs16		// real*real, imag*real
	xvmuldp		vs37,	vs2,	vs17		// real*imag, imag*imag
	xvmuldp		vs38,	vs3,	vs16		// real*real, imag*real
	xvmuldp		vs39,	vs3,	vs17		// real*imag, imag*imag

	xvmuldp		vs40,	vs0,	vs18		// real*real, imag*real
	xvmuldp		vs41,	vs0,	vs19		// real*imag, imag*imag
	xvmuldp		vs42,	vs1,	vs18		// real*real, imag*real
	xvmuldp		vs43,	vs1,	vs19		// real*imag, imag*imag
	xvmuldp		vs44,	vs2,	vs18		// real*real, imag*real
	xvmuldp		vs45,	vs2,	vs19		// real*imag, imag*imag
	xvmuldp		vs46,	vs3,	vs18		// real*real, imag*real
	xvmuldp		vs47,	vs3,	vs19		// real*imag, imag*imag


.endm

.macro KERNEL2x4_SUB1

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A
	lxvd2x		vs2,	o32,	AO		// load real,imag from A
	lxvd2x		vs3,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B
	lxvd2x		vs18,	o32,	BO		// load real part from B
	lxvd2x		vs19,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	xvmaddadp	vs32,	vs0,	vs16		// real*real, imag*real
	xvmaddadp	vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmaddadp	vs34,	vs1,	vs16		// real*real, imag*real
	xvmaddadp	vs35,	vs1,	vs17		// real*imag, imag*imag
	xvmaddadp	vs36,	vs2,	vs16		// real*real, imag*real
	xvmaddadp	vs37,	vs2,	vs17		// real*imag, imag*imag
	xvmaddadp	vs38,	vs3,	vs16		// real*real, imag*real
	xvmaddadp	vs39,	vs3,	vs17		// real*imag, imag*imag

	xvmaddadp	vs40,	vs0,	vs18		// real*real, imag*real
	xvmaddadp	vs41,	vs0,	vs19		// real*imag, imag*imag
	xvmaddadp	vs42,	vs1,	vs18		// real*real, imag*real
	xvmaddadp	vs43,	vs1,	vs19		// real*imag, imag*imag
	xvmaddadp	vs44,	vs2,	vs18		// real*real, imag*real
	xvmaddadp	vs45,	vs2,	vs19		// real*imag, imag*imag
	xvmaddadp	vs46,	vs3,	vs18		// real*real, imag*real
	xvmaddadp	vs47,	vs3,	vs19		// real*imag, imag*imag


.endm

.macro SAVE2x4


	mr		T1,	CO

#ifndef TRMMKERNEL

	lxvd2x		vs16,	o0,	T1
	lxvd2x		vs17,	o16,	T1
	lxvd2x		vs18,	o32,	T1
	lxvd2x		vs19,	o48,	T1

#endif


	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs33,	vs33			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs32		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs33		// imagA*imagB

	xxswapd		vs32,	vs32			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs33,	vs33			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs32		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs33		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs8,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs35,	vs35			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs34		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs35		// imagA*imagB

	xxswapd		vs34,	vs34			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs35,	vs35			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs34		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs35		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs9,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs37,	vs37			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs36		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs37		// imagA*imagB

	xxswapd		vs36,	vs36			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs37,	vs37			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs36		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs37		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs10,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs39,	vs39			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs38		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs39		// imagA*imagB

	xxswapd		vs38,	vs38			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs39,	vs39			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs38		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs39		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs11,	vs2,	vs3,	0	// merge real and imag part


#ifndef TRMMKERNEL

	xvadddp		vs8,	vs8,	vs16
	xvadddp		vs9,	vs9,	vs17
	xvadddp		vs10,	vs10,	vs18
	xvadddp		vs11,	vs11,	vs19

#endif

	stxvd2x		vs8,	o0,	T1
	stxvd2x		vs9,	o16,	T1
	stxvd2x		vs10,	o32,	T1
	stxvd2x		vs11,	o48,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL

	lxvd2x		vs16,	o0,	T1
	lxvd2x		vs17,	o16,	T1
	lxvd2x		vs18,	o32,	T1
	lxvd2x		vs19,	o48,	T1

#endif


	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs41,	vs41			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs40		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs41		// imagA*imagB

	xxswapd		vs40,	vs40			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs41,	vs41			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs40		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs41		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs8,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs43,	vs43			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs42		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs43		// imagA*imagB

	xxswapd		vs42,	vs42			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs43,	vs43			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs42		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs43		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs9,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs45,	vs45			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs44		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs45		// imagA*imagB

	xxswapd		vs44,	vs44			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs45,	vs45			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs44		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs45		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs10,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs47,	vs47			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs46		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs47		// imagA*imagB

	xxswapd		vs46,	vs46			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs47,	vs47			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs46		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs47		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs11,	vs2,	vs3,	0	// merge real and imag part


#ifndef TRMMKERNEL

	xvadddp		vs8,	vs8,	vs16
	xvadddp		vs9,	vs9,	vs17
	xvadddp		vs10,	vs10,	vs18
	xvadddp		vs11,	vs11,	vs19

#endif

	stxvd2x		vs8,	o0,	T1
	stxvd2x		vs9,	o16,	T1
	stxvd2x		vs10,	o32,	T1
	stxvd2x		vs11,	o48,	T1

	add		T1,	T1,	LDC
	addi		CO,	CO,	64

.endm


/**********************************************************************************************
* Macros for N=2 and M=2
**********************************************************************************************/

.macro LOAD2x2_1

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B
	lxvd2x		vs18,	o32,	BO		// load real part from B
	lxvd2x		vs19,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A

	addi		AO,	AO,	32


.endm

.macro KERNEL2x2_I1

	lxvd2x		vs8,	o0,	AO		// load real,imag from A
	lxvd2x		vs9,	o16,	AO		// load real,imag from A

	addi		AO,	AO,	32

	lxvd2x		vs20,	o0,	BO		// load real part from B
	lxvd2x		vs21,	o16,	BO		// load imag part from B
	lxvd2x		vs22,	o32,	BO		// load real part from B
	lxvd2x		vs23,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	xvmuldp		vs32,	vs0,	vs16		// real*real, imag*real
	xvmuldp		vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmuldp		vs34,	vs1,	vs16		// real*real, imag*real
	xvmuldp		vs35,	vs1,	vs17		// real*imag, imag*imag

	xvmuldp		vs36,	vs0,	vs18		// real*real, imag*real
	xvmuldp		vs37,	vs0,	vs19		// real*imag, imag*imag
	xvmuldp		vs38,	vs1,	vs18		// real*real, imag*real
	xvmuldp		vs39,	vs1,	vs19		// real*imag, imag*imag


.endm

.macro KERNEL2x2_1

	lxvd2x		vs8,	o0,	AO		// load real,imag from A
	lxvd2x		vs9,	o16,	AO		// load real,imag from A

	addi		AO,	AO,	32

	lxvd2x		vs20,	o0,	BO		// load real part from B
	lxvd2x		vs21,	o16,	BO		// load imag part from B
	lxvd2x		vs22,	o32,	BO		// load real part from B
	lxvd2x		vs23,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	xvmaddadp	vs32,	vs0,	vs16		// real*real, imag*real
	xvmaddadp	vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmaddadp	vs34,	vs1,	vs16		// real*real, imag*real
	xvmaddadp	vs35,	vs1,	vs17		// real*imag, imag*imag

	xvmaddadp	vs36,	vs0,	vs18		// real*real, imag*real
	xvmaddadp	vs37,	vs0,	vs19		// real*imag, imag*imag
	xvmaddadp	vs38,	vs1,	vs18		// real*real, imag*real
	xvmaddadp	vs39,	vs1,	vs19		// real*imag, imag*imag


.endm

.macro KERNEL2x2_2

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A

	addi		AO,	AO,	32

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B
	lxvd2x		vs18,	o32,	BO		// load real part from B
	lxvd2x		vs19,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	xvmaddadp	vs32,	vs8,	vs20		// real*real, imag*real
	xvmaddadp	vs33,	vs8,	vs21		// real*imag, imag*imag
	xvmaddadp	vs34,	vs9,	vs20		// real*real, imag*real
	xvmaddadp	vs35,	vs9,	vs21		// real*imag, imag*imag

	xvmaddadp	vs36,	vs8,	vs22		// real*real, imag*real
	xvmaddadp	vs37,	vs8,	vs23		// real*imag, imag*imag
	xvmaddadp	vs38,	vs9,	vs22		// real*real, imag*real
	xvmaddadp	vs39,	vs9,	vs23		// real*imag, imag*imag


.endm

.macro KERNEL2x2_E2


	xvmaddadp	vs32,	vs8,	vs20		// real*real, imag*real
	xvmaddadp	vs33,	vs8,	vs21		// real*imag, imag*imag
	xvmaddadp	vs34,	vs9,	vs20		// real*real, imag*real
	xvmaddadp	vs35,	vs9,	vs21		// real*imag, imag*imag

	xvmaddadp	vs36,	vs8,	vs22		// real*real, imag*real
	xvmaddadp	vs37,	vs8,	vs23		// real*imag, imag*imag
	xvmaddadp	vs38,	vs9,	vs22		// real*real, imag*real
	xvmaddadp	vs39,	vs9,	vs23		// real*imag, imag*imag


.endm

.macro KERNEL2x2_SUBI1

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A

	addi		AO,	AO,	32

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B
	lxvd2x		vs18,	o32,	BO		// load real part from B
	lxvd2x		vs19,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	xvmuldp		vs32,	vs0,	vs16		// real*real, imag*real
	xvmuldp		vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmuldp		vs34,	vs1,	vs16		// real*real, imag*real
	xvmuldp		vs35,	vs1,	vs17		// real*imag, imag*imag

	xvmuldp		vs36,	vs0,	vs18		// real*real, imag*real
	xvmuldp		vs37,	vs0,	vs19		// real*imag, imag*imag
	xvmuldp		vs38,	vs1,	vs18		// real*real, imag*real
	xvmuldp		vs39,	vs1,	vs19		// real*imag, imag*imag


.endm

.macro KERNEL2x2_SUB1

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A

	addi		AO,	AO,	32

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B
	lxvd2x		vs18,	o32,	BO		// load real part from B
	lxvd2x		vs19,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	xvmaddadp	vs32,	vs0,	vs16		// real*real, imag*real
	xvmaddadp	vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmaddadp	vs34,	vs1,	vs16		// real*real, imag*real
	xvmaddadp	vs35,	vs1,	vs17		// real*imag, imag*imag

	xvmaddadp	vs36,	vs0,	vs18		// real*real, imag*real
	xvmaddadp	vs37,	vs0,	vs19		// real*imag, imag*imag
	xvmaddadp	vs38,	vs1,	vs18		// real*real, imag*real
	xvmaddadp	vs39,	vs1,	vs19		// real*imag, imag*imag


.endm

.macro SAVE2x2


	mr		T1,	CO

#ifndef TRMMKERNEL

	lxvd2x		vs16,	o0,	T1
	lxvd2x		vs17,	o16,	T1

#endif


	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs33,	vs33			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs32		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs33		// imagA*imagB

	xxswapd		vs32,	vs32			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs33,	vs33			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs32		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs33		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs8,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs35,	vs35			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs34		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs35		// imagA*imagB

	xxswapd		vs34,	vs34			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs35,	vs35			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs34		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs35		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs9,	vs2,	vs3,	0	// merge real and imag part


#ifndef TRMMKERNEL

	xvadddp		vs8,	vs8,	vs16
	xvadddp		vs9,	vs9,	vs17

#endif

	stxvd2x		vs8,	o0,	T1
	stxvd2x		vs9,	o16,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL

	lxvd2x		vs16,	o0,	T1
	lxvd2x		vs17,	o16,	T1

#endif


	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs37,	vs37			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs36		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs37		// imagA*imagB

	xxswapd		vs36,	vs36			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs37,	vs37			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs36		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs37		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs8,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs39,	vs39			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs38		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs39		// imagA*imagB

	xxswapd		vs38,	vs38			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs39,	vs39			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs38		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs39		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs9,	vs2,	vs3,	0	// merge real and imag part


#ifndef TRMMKERNEL

	xvadddp		vs8,	vs8,	vs16
	xvadddp		vs9,	vs9,	vs17

#endif

	stxvd2x		vs8,	o0,	T1
	stxvd2x		vs9,	o16,	T1

	add		T1,	T1,	LDC
	addi		CO,	CO,	32

.endm


/**********************************************************************************************
* Macros for N=2 and M=1
**********************************************************************************************/

.macro LOAD2x1_1

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B
	lxvd2x		vs18,	o32,	BO		// load real part from B
	lxvd2x		vs19,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	lxvd2x		vs0,	o0,	AO		// load real,imag from A

	addi		AO,	AO,	16


.endm

.macro KERNEL2x1_I1

	lxvd2x		vs8,	o0,	AO		// load real,imag from A

	addi		AO,	AO,	16

	lxvd2x		vs20,	o0,	BO		// load real part from B
	lxvd2x		vs21,	o16,	BO		// load imag part from B
	lxvd2x		vs22,	o32,	BO		// load real part from B
	lxvd2x		vs23,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	xvmuldp		vs32,	vs0,	vs16		// real*real, imag*real
	xvmuldp		vs33,	vs0,	vs17		// real*imag, imag*imag

	xvmuldp		vs34,	vs0,	vs18		// real*real, imag*real
	xvmuldp		vs35,	vs0,	vs19		// real*imag, imag*imag


.endm

.macro KERNEL2x1_1

	lxvd2x		vs8,	o0,	AO		// load real,imag from A

	addi		AO,	AO,	16

	lxvd2x		vs20,	o0,	BO		// load real part from B
	lxvd2x		vs21,	o16,	BO		// load imag part from B
	lxvd2x		vs22,	o32,	BO		// load real part from B
	lxvd2x		vs23,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	xvmaddadp	vs32,	vs0,	vs16		// real*real, imag*real
	xvmaddadp	vs33,	vs0,	vs17		// real*imag, imag*imag

	xvmaddadp	vs34,	vs0,	vs18		// real*real, imag*real
	xvmaddadp	vs35,	vs0,	vs19		// real*imag, imag*imag


.endm

.macro KERNEL2x1_2

	lxvd2x		vs0,	o0,	AO		// load real,imag from A

	addi		AO,	AO,	16

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B
	lxvd2x		vs18,	o32,	BO		// load real part from B
	lxvd2x		vs19,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	xvmaddadp	vs32,	vs8,	vs20		// real*real, imag*real
	xvmaddadp	vs33,	vs8,	vs21		// real*imag, imag*imag

	xvmaddadp	vs34,	vs8,	vs22		// real*real, imag*real
	xvmaddadp	vs35,	vs8,	vs23		// real*imag, imag*imag


.endm

.macro KERNEL2x1_E2


	xvmaddadp	vs32,	vs8,	vs20		// real*real, imag*real
	xvmaddadp	vs33,	vs8,	vs21		// real*imag, imag*imag

	xvmaddadp	vs34,	vs8,	vs22		// real*real, imag*real
	xvmaddadp	vs35,	vs8,	vs23		// real*imag, imag*imag


.endm

.macro KERNEL2x1_SUBI1

	lxvd2x		vs0,	o0,	AO		// load real,imag from A

	addi		AO,	AO,	16

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B
	lxvd2x		vs18,	o32,	BO		// load real part from B
	lxvd2x		vs19,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	xvmuldp		vs32,	vs0,	vs16		// real*real, imag*real
	xvmuldp		vs33,	vs0,	vs17		// real*imag, imag*imag

	xvmuldp		vs34,	vs0,	vs18		// real*real, imag*real
	xvmuldp		vs35,	vs0,	vs19		// real*imag, imag*imag


.endm

.macro KERNEL2x1_SUB1

	lxvd2x		vs0,	o0,	AO		// load real,imag from A

	addi		AO,	AO,	16

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B
	lxvd2x		vs18,	o32,	BO		// load real part from B
	lxvd2x		vs19,	o48,	BO		// load imag part from B

	addi		BO,	BO,	64

	xvmaddadp	vs32,	vs0,	vs16		// real*real, imag*real
	xvmaddadp	vs33,	vs0,	vs17		// real*imag, imag*imag

	xvmaddadp	vs34,	vs0,	vs18		// real*real, imag*real
	xvmaddadp	vs35,	vs0,	vs19		// real*imag, imag*imag


.endm

.macro SAVE2x1


	mr		T1,	CO

#ifndef TRMMKERNEL

	lxvd2x		vs16,	o0,	T1

#endif


	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs33,	vs33			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs32		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs33		// imagA*imagB

	xxswapd		vs32,	vs32			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs33,	vs33			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs32		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs33		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs8,	vs2,	vs3,	0	// merge real and imag part


#ifndef TRMMKERNEL

	xvadddp		vs8,	vs8,	vs16

#endif

	stxvd2x		vs8,	o0,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL

	lxvd2x		vs16,	o0,	T1

#endif


	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs35,	vs35			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs34		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs35		// imagA*imagB

	xxswapd		vs34,	vs34			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs35,	vs35			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs34		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs35		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs8,	vs2,	vs3,	0	// merge real and imag part


#ifndef TRMMKERNEL

	xvadddp		vs8,	vs8,	vs16

#endif

	stxvd2x		vs8,	o0,	T1

	add		T1,	T1,	LDC
	addi		CO,	CO,	16

.endm


/**********************************************************************************************
* Macros for N=1 and M=8
**********************************************************************************************/

.macro LOAD1x8_1

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A
	lxvd2x		vs2,	o32,	AO		// load real,imag from A
	lxvd2x		vs3,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs4,	o0,	AO		// load real,imag from A
	lxvd2x		vs5,	o16,	AO		// load real,imag from A
	lxvd2x		vs6,	o32,	AO		// load real,imag from A
	lxvd2x		vs7,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64


.endm

.macro KERNEL1x8_I1

	lxvd2x		vs8,	o0,	AO		// load real,imag from A
	lxvd2x		vs9,	o16,	AO		// load real,imag from A
	lxvd2x		vs10,	o32,	AO		// load real,imag from A
	lxvd2x		vs11,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs12,	o0,	AO		// load real,imag from A
	lxvd2x		vs13,	o16,	AO		// load real,imag from A
	lxvd2x		vs14,	o32,	AO		// load real,imag from A
	lxvd2x		vs15,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs20,	o0,	BO		// load real part from B
	lxvd2x		vs21,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	xvmuldp		vs32,	vs0,	vs16		// real*real, imag*real
	xvmuldp		vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmuldp		vs34,	vs1,	vs16		// real*real, imag*real
	xvmuldp		vs35,	vs1,	vs17		// real*imag, imag*imag
	xvmuldp		vs36,	vs2,	vs16		// real*real, imag*real
	xvmuldp		vs37,	vs2,	vs17		// real*imag, imag*imag
	xvmuldp		vs38,	vs3,	vs16		// real*real, imag*real
	xvmuldp		vs39,	vs3,	vs17		// real*imag, imag*imag
	xvmuldp		vs40,	vs4,	vs16		// real*real, imag*real
	xvmuldp		vs41,	vs4,	vs17		// real*imag, imag*imag
	xvmuldp		vs42,	vs5,	vs16		// real*real, imag*real
	xvmuldp		vs43,	vs5,	vs17		// real*imag, imag*imag
	xvmuldp		vs44,	vs6,	vs16		// real*real, imag*real
	xvmuldp		vs45,	vs6,	vs17		// real*imag, imag*imag
	xvmuldp		vs46,	vs7,	vs16		// real*real, imag*real
	xvmuldp		vs47,	vs7,	vs17		// real*imag, imag*imag


.endm

.macro KERNEL1x8_1

	lxvd2x		vs8,	o0,	AO		// load real,imag from A
	lxvd2x		vs9,	o16,	AO		// load real,imag from A
	lxvd2x		vs10,	o32,	AO		// load real,imag from A
	lxvd2x		vs11,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs12,	o0,	AO		// load real,imag from A
	lxvd2x		vs13,	o16,	AO		// load real,imag from A
	lxvd2x		vs14,	o32,	AO		// load real,imag from A
	lxvd2x		vs15,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs20,	o0,	BO		// load real part from B
	lxvd2x		vs21,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	xvmaddadp	vs32,	vs0,	vs16		// real*real, imag*real
	xvmaddadp	vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmaddadp	vs34,	vs1,	vs16		// real*real, imag*real
	xvmaddadp	vs35,	vs1,	vs17		// real*imag, imag*imag
	xvmaddadp	vs36,	vs2,	vs16		// real*real, imag*real
	xvmaddadp	vs37,	vs2,	vs17		// real*imag, imag*imag
	xvmaddadp	vs38,	vs3,	vs16		// real*real, imag*real
	xvmaddadp	vs39,	vs3,	vs17		// real*imag, imag*imag
	xvmaddadp	vs40,	vs4,	vs16		// real*real, imag*real
	xvmaddadp	vs41,	vs4,	vs17		// real*imag, imag*imag
	xvmaddadp	vs42,	vs5,	vs16		// real*real, imag*real
	xvmaddadp	vs43,	vs5,	vs17		// real*imag, imag*imag
	xvmaddadp	vs44,	vs6,	vs16		// real*real, imag*real
	xvmaddadp	vs45,	vs6,	vs17		// real*imag, imag*imag
	xvmaddadp	vs46,	vs7,	vs16		// real*real, imag*real
	xvmaddadp	vs47,	vs7,	vs17		// real*imag, imag*imag


.endm

.macro KERNEL1x8_2

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A
	lxvd2x		vs2,	o32,	AO		// load real,imag from A
	lxvd2x		vs3,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs4,	o0,	AO		// load real,imag from A
	lxvd2x		vs5,	o16,	AO		// load real,imag from A
	lxvd2x		vs6,	o32,	AO		// load real,imag from A
	lxvd2x		vs7,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	xvmaddadp	vs32,	vs8,	vs20		// real*real, imag*real
	xvmaddadp	vs33,	vs8,	vs21		// real*imag, imag*imag
	xvmaddadp	vs34,	vs9,	vs20		// real*real, imag*real
	xvmaddadp	vs35,	vs9,	vs21		// real*imag, imag*imag
	xvmaddadp	vs36,	vs10,	vs20		// real*real, imag*real
	xvmaddadp	vs37,	vs10,	vs21		// real*imag, imag*imag
	xvmaddadp	vs38,	vs11,	vs20		// real*real, imag*real
	xvmaddadp	vs39,	vs11,	vs21		// real*imag, imag*imag
	xvmaddadp	vs40,	vs12,	vs20		// real*real, imag*real
	xvmaddadp	vs41,	vs12,	vs21		// real*imag, imag*imag
	xvmaddadp	vs42,	vs13,	vs20		// real*real, imag*real
	xvmaddadp	vs43,	vs13,	vs21		// real*imag, imag*imag
	xvmaddadp	vs44,	vs14,	vs20		// real*real, imag*real
	xvmaddadp	vs45,	vs14,	vs21		// real*imag, imag*imag
	xvmaddadp	vs46,	vs15,	vs20		// real*real, imag*real
	xvmaddadp	vs47,	vs15,	vs21		// real*imag, imag*imag


.endm

.macro KERNEL1x8_E2


	xvmaddadp	vs32,	vs8,	vs20		// real*real, imag*real
	xvmaddadp	vs33,	vs8,	vs21		// real*imag, imag*imag
	xvmaddadp	vs34,	vs9,	vs20		// real*real, imag*real
	xvmaddadp	vs35,	vs9,	vs21		// real*imag, imag*imag
	xvmaddadp	vs36,	vs10,	vs20		// real*real, imag*real
	xvmaddadp	vs37,	vs10,	vs21		// real*imag, imag*imag
	xvmaddadp	vs38,	vs11,	vs20		// real*real, imag*real
	xvmaddadp	vs39,	vs11,	vs21		// real*imag, imag*imag
	xvmaddadp	vs40,	vs12,	vs20		// real*real, imag*real
	xvmaddadp	vs41,	vs12,	vs21		// real*imag, imag*imag
	xvmaddadp	vs42,	vs13,	vs20		// real*real, imag*real
	xvmaddadp	vs43,	vs13,	vs21		// real*imag, imag*imag
	xvmaddadp	vs44,	vs14,	vs20		// real*real, imag*real
	xvmaddadp	vs45,	vs14,	vs21		// real*imag, imag*imag
	xvmaddadp	vs46,	vs15,	vs20		// real*real, imag*real
	xvmaddadp	vs47,	vs15,	vs21		// real*imag, imag*imag


.endm

.macro KERNEL1x8_SUBI1

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A
	lxvd2x		vs2,	o32,	AO		// load real,imag from A
	lxvd2x		vs3,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs4,	o0,	AO		// load real,imag from A
	lxvd2x		vs5,	o16,	AO		// load real,imag from A
	lxvd2x		vs6,	o32,	AO		// load real,imag from A
	lxvd2x		vs7,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	xvmuldp		vs32,	vs0,	vs16		// real*real, imag*real
	xvmuldp		vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmuldp		vs34,	vs1,	vs16		// real*real, imag*real
	xvmuldp		vs35,	vs1,	vs17		// real*imag, imag*imag
	xvmuldp		vs36,	vs2,	vs16		// real*real, imag*real
	xvmuldp		vs37,	vs2,	vs17		// real*imag, imag*imag
	xvmuldp		vs38,	vs3,	vs16		// real*real, imag*real
	xvmuldp		vs39,	vs3,	vs17		// real*imag, imag*imag
	xvmuldp		vs40,	vs4,	vs16		// real*real, imag*real
	xvmuldp		vs41,	vs4,	vs17		// real*imag, imag*imag
	xvmuldp		vs42,	vs5,	vs16		// real*real, imag*real
	xvmuldp		vs43,	vs5,	vs17		// real*imag, imag*imag
	xvmuldp		vs44,	vs6,	vs16		// real*real, imag*real
	xvmuldp		vs45,	vs6,	vs17		// real*imag, imag*imag
	xvmuldp		vs46,	vs7,	vs16		// real*real, imag*real
	xvmuldp		vs47,	vs7,	vs17		// real*imag, imag*imag


.endm

.macro KERNEL1x8_SUB1

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A
	lxvd2x		vs2,	o32,	AO		// load real,imag from A
	lxvd2x		vs3,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs4,	o0,	AO		// load real,imag from A
	lxvd2x		vs5,	o16,	AO		// load real,imag from A
	lxvd2x		vs6,	o32,	AO		// load real,imag from A
	lxvd2x		vs7,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	xvmaddadp	vs32,	vs0,	vs16		// real*real, imag*real
	xvmaddadp	vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmaddadp	vs34,	vs1,	vs16		// real*real, imag*real
	xvmaddadp	vs35,	vs1,	vs17		// real*imag, imag*imag
	xvmaddadp	vs36,	vs2,	vs16		// real*real, imag*real
	xvmaddadp	vs37,	vs2,	vs17		// real*imag, imag*imag
	xvmaddadp	vs38,	vs3,	vs16		// real*real, imag*real
	xvmaddadp	vs39,	vs3,	vs17		// real*imag, imag*imag
	xvmaddadp	vs40,	vs4,	vs16		// real*real, imag*real
	xvmaddadp	vs41,	vs4,	vs17		// real*imag, imag*imag
	xvmaddadp	vs42,	vs5,	vs16		// real*real, imag*real
	xvmaddadp	vs43,	vs5,	vs17		// real*imag, imag*imag
	xvmaddadp	vs44,	vs6,	vs16		// real*real, imag*real
	xvmaddadp	vs45,	vs6,	vs17		// real*imag, imag*imag
	xvmaddadp	vs46,	vs7,	vs16		// real*real, imag*real
	xvmaddadp	vs47,	vs7,	vs17		// real*imag, imag*imag


.endm

.macro SAVE1x8


	mr		T1,	CO
	addi		T2,	T1,	64

#ifndef TRMMKERNEL

	lxvd2x		vs16,	o0,	T1
	lxvd2x		vs17,	o16,	T1
	lxvd2x		vs18,	o32,	T1
	lxvd2x		vs19,	o48,	T1
	lxvd2x		vs20,	o0,	T2
	lxvd2x		vs21,	o16,	T2
	lxvd2x		vs22,	o32,	T2
	lxvd2x		vs23,	o48,	T2

#endif


	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs33,	vs33			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs32		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs33		// imagA*imagB

	xxswapd		vs32,	vs32			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs33,	vs33			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs32		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs33		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs8,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs35,	vs35			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs34		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs35		// imagA*imagB

	xxswapd		vs34,	vs34			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs35,	vs35			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs34		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs35		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs9,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs37,	vs37			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs36		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs37		// imagA*imagB

	xxswapd		vs36,	vs36			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs37,	vs37			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs36		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs37		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs10,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs39,	vs39			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs38		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs39		// imagA*imagB

	xxswapd		vs38,	vs38			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs39,	vs39			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs38		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs39		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs11,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs41,	vs41			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs40		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs41		// imagA*imagB

	xxswapd		vs40,	vs40			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs41,	vs41			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs40		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs41		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs12,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs43,	vs43			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs42		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs43		// imagA*imagB

	xxswapd		vs42,	vs42			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs43,	vs43			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs42		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs43		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs13,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs45,	vs45			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs44		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs45		// imagA*imagB

	xxswapd		vs44,	vs44			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs45,	vs45			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs44		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs45		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs14,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs47,	vs47			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs46		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs47		// imagA*imagB

	xxswapd		vs46,	vs46			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs47,	vs47			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs46		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs47		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs15,	vs2,	vs3,	0	// merge real and imag part


#ifndef TRMMKERNEL

	xvadddp		vs8,	vs8,	vs16
	xvadddp		vs9,	vs9,	vs17
	xvadddp		vs10,	vs10,	vs18
	xvadddp		vs11,	vs11,	vs19
	xvadddp		vs12,	vs12,	vs20
	xvadddp		vs13,	vs13,	vs21
	xvadddp		vs14,	vs14,	vs22
	xvadddp		vs15,	vs15,	vs23

#endif

	stxvd2x		vs8,	o0,	T1
	stxvd2x		vs9,	o16,	T1
	stxvd2x		vs10,	o32,	T1
	stxvd2x		vs11,	o48,	T1
	stxvd2x		vs12,	o0,	T2
	stxvd2x		vs13,	o16,	T2
	stxvd2x		vs14,	o32,	T2
	stxvd2x		vs15,	o48,	T2

	add		T1,	T1,	LDC
	add		T2,	T2,	LDC
	addi		CO,	CO,	128

.endm


/**********************************************************************************************
* Macros for N=1 and M=4
**********************************************************************************************/

.macro LOAD1x4_1

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A
	lxvd2x		vs2,	o32,	AO		// load real,imag from A
	lxvd2x		vs3,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64


.endm

.macro KERNEL1x4_I1

	lxvd2x		vs8,	o0,	AO		// load real,imag from A
	lxvd2x		vs9,	o16,	AO		// load real,imag from A
	lxvd2x		vs10,	o32,	AO		// load real,imag from A
	lxvd2x		vs11,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs20,	o0,	BO		// load real part from B
	lxvd2x		vs21,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	xvmuldp		vs32,	vs0,	vs16		// real*real, imag*real
	xvmuldp		vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmuldp		vs34,	vs1,	vs16		// real*real, imag*real
	xvmuldp		vs35,	vs1,	vs17		// real*imag, imag*imag
	xvmuldp		vs36,	vs2,	vs16		// real*real, imag*real
	xvmuldp		vs37,	vs2,	vs17		// real*imag, imag*imag
	xvmuldp		vs38,	vs3,	vs16		// real*real, imag*real
	xvmuldp		vs39,	vs3,	vs17		// real*imag, imag*imag


.endm

.macro KERNEL1x4_1

	lxvd2x		vs8,	o0,	AO		// load real,imag from A
	lxvd2x		vs9,	o16,	AO		// load real,imag from A
	lxvd2x		vs10,	o32,	AO		// load real,imag from A
	lxvd2x		vs11,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs20,	o0,	BO		// load real part from B
	lxvd2x		vs21,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	xvmaddadp	vs32,	vs0,	vs16		// real*real, imag*real
	xvmaddadp	vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmaddadp	vs34,	vs1,	vs16		// real*real, imag*real
	xvmaddadp	vs35,	vs1,	vs17		// real*imag, imag*imag
	xvmaddadp	vs36,	vs2,	vs16		// real*real, imag*real
	xvmaddadp	vs37,	vs2,	vs17		// real*imag, imag*imag
	xvmaddadp	vs38,	vs3,	vs16		// real*real, imag*real
	xvmaddadp	vs39,	vs3,	vs17		// real*imag, imag*imag


.endm

.macro KERNEL1x4_2

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A
	lxvd2x		vs2,	o32,	AO		// load real,imag from A
	lxvd2x		vs3,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	xvmaddadp	vs32,	vs8,	vs20		// real*real, imag*real
	xvmaddadp	vs33,	vs8,	vs21		// real*imag, imag*imag
	xvmaddadp	vs34,	vs9,	vs20		// real*real, imag*real
	xvmaddadp	vs35,	vs9,	vs21		// real*imag, imag*imag
	xvmaddadp	vs36,	vs10,	vs20		// real*real, imag*real
	xvmaddadp	vs37,	vs10,	vs21		// real*imag, imag*imag
	xvmaddadp	vs38,	vs11,	vs20		// real*real, imag*real
	xvmaddadp	vs39,	vs11,	vs21		// real*imag, imag*imag


.endm

.macro KERNEL1x4_E2


	xvmaddadp	vs32,	vs8,	vs20		// real*real, imag*real
	xvmaddadp	vs33,	vs8,	vs21		// real*imag, imag*imag
	xvmaddadp	vs34,	vs9,	vs20		// real*real, imag*real
	xvmaddadp	vs35,	vs9,	vs21		// real*imag, imag*imag
	xvmaddadp	vs36,	vs10,	vs20		// real*real, imag*real
	xvmaddadp	vs37,	vs10,	vs21		// real*imag, imag*imag
	xvmaddadp	vs38,	vs11,	vs20		// real*real, imag*real
	xvmaddadp	vs39,	vs11,	vs21		// real*imag, imag*imag


.endm

.macro KERNEL1x4_SUBI1

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A
	lxvd2x		vs2,	o32,	AO		// load real,imag from A
	lxvd2x		vs3,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	xvmuldp		vs32,	vs0,	vs16		// real*real, imag*real
	xvmuldp		vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmuldp		vs34,	vs1,	vs16		// real*real, imag*real
	xvmuldp		vs35,	vs1,	vs17		// real*imag, imag*imag
	xvmuldp		vs36,	vs2,	vs16		// real*real, imag*real
	xvmuldp		vs37,	vs2,	vs17		// real*imag, imag*imag
	xvmuldp		vs38,	vs3,	vs16		// real*real, imag*real
	xvmuldp		vs39,	vs3,	vs17		// real*imag, imag*imag


.endm

.macro KERNEL1x4_SUB1

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A
	lxvd2x		vs2,	o32,	AO		// load real,imag from A
	lxvd2x		vs3,	o48,	AO		// load real,imag from A

	addi		AO,	AO,	64

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	xvmaddadp	vs32,	vs0,	vs16		// real*real, imag*real
	xvmaddadp	vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmaddadp	vs34,	vs1,	vs16		// real*real, imag*real
	xvmaddadp	vs35,	vs1,	vs17		// real*imag, imag*imag
	xvmaddadp	vs36,	vs2,	vs16		// real*real, imag*real
	xvmaddadp	vs37,	vs2,	vs17		// real*imag, imag*imag
	xvmaddadp	vs38,	vs3,	vs16		// real*real, imag*real
	xvmaddadp	vs39,	vs3,	vs17		// real*imag, imag*imag


.endm

.macro SAVE1x4


	mr		T1,	CO

#ifndef TRMMKERNEL

	lxvd2x		vs16,	o0,	T1
	lxvd2x		vs17,	o16,	T1
	lxvd2x		vs18,	o32,	T1
	lxvd2x		vs19,	o48,	T1

#endif


	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs33,	vs33			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs32		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs33		// imagA*imagB

	xxswapd		vs32,	vs32			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs33,	vs33			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs32		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs33		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs8,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs35,	vs35			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs34		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs35		// imagA*imagB

	xxswapd		vs34,	vs34			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs35,	vs35			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs34		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs35		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs9,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs37,	vs37			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs36		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs37		// imagA*imagB

	xxswapd		vs36,	vs36			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs37,	vs37			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs36		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs37		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs10,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs39,	vs39			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs38		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs39		// imagA*imagB

	xxswapd		vs38,	vs38			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs39,	vs39			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs38		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs39		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs11,	vs2,	vs3,	0	// merge real and imag part


#ifndef TRMMKERNEL

	xvadddp		vs8,	vs8,	vs16
	xvadddp		vs9,	vs9,	vs17
	xvadddp		vs10,	vs10,	vs18
	xvadddp		vs11,	vs11,	vs19

#endif

	stxvd2x		vs8,	o0,	T1
	stxvd2x		vs9,	o16,	T1
	stxvd2x		vs10,	o32,	T1
	stxvd2x		vs11,	o48,	T1

	add		T1,	T1,	LDC
	addi		CO,	CO,	64

.endm


/**********************************************************************************************
* Macros for N=1 and M=2
**********************************************************************************************/

.macro LOAD1x2_1

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A

	addi		AO,	AO,	32


.endm

.macro KERNEL1x2_I1

	lxvd2x		vs8,	o0,	AO		// load real,imag from A
	lxvd2x		vs9,	o16,	AO		// load real,imag from A

	addi		AO,	AO,	32

	lxvd2x		vs20,	o0,	BO		// load real part from B
	lxvd2x		vs21,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	xvmuldp		vs32,	vs0,	vs16		// real*real, imag*real
	xvmuldp		vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmuldp		vs34,	vs1,	vs16		// real*real, imag*real
	xvmuldp		vs35,	vs1,	vs17		// real*imag, imag*imag


.endm

.macro KERNEL1x2_1

	lxvd2x		vs8,	o0,	AO		// load real,imag from A
	lxvd2x		vs9,	o16,	AO		// load real,imag from A

	addi		AO,	AO,	32

	lxvd2x		vs20,	o0,	BO		// load real part from B
	lxvd2x		vs21,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	xvmaddadp	vs32,	vs0,	vs16		// real*real, imag*real
	xvmaddadp	vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmaddadp	vs34,	vs1,	vs16		// real*real, imag*real
	xvmaddadp	vs35,	vs1,	vs17		// real*imag, imag*imag


.endm

.macro KERNEL1x2_2

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A

	addi		AO,	AO,	32

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	xvmaddadp	vs32,	vs8,	vs20		// real*real, imag*real
	xvmaddadp	vs33,	vs8,	vs21		// real*imag, imag*imag
	xvmaddadp	vs34,	vs9,	vs20		// real*real, imag*real
	xvmaddadp	vs35,	vs9,	vs21		// real*imag, imag*imag


.endm

.macro KERNEL1x2_E2


	xvmaddadp	vs32,	vs8,	vs20		// real*real, imag*real
	xvmaddadp	vs33,	vs8,	vs21		// real*imag, imag*imag
	xvmaddadp	vs34,	vs9,	vs20		// real*real, imag*real
	xvmaddadp	vs35,	vs9,	vs21		// real*imag, imag*imag


.endm

.macro KERNEL1x2_SUBI1

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A

	addi		AO,	AO,	32

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	xvmuldp		vs32,	vs0,	vs16		// real*real, imag*real
	xvmuldp		vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmuldp		vs34,	vs1,	vs16		// real*real, imag*real
	xvmuldp		vs35,	vs1,	vs17		// real*imag, imag*imag


.endm

.macro KERNEL1x2_SUB1

	lxvd2x		vs0,	o0,	AO		// load real,imag from A
	lxvd2x		vs1,	o16,	AO		// load real,imag from A

	addi		AO,	AO,	32

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	xvmaddadp	vs32,	vs0,	vs16		// real*real, imag*real
	xvmaddadp	vs33,	vs0,	vs17		// real*imag, imag*imag
	xvmaddadp	vs34,	vs1,	vs16		// real*real, imag*real
	xvmaddadp	vs35,	vs1,	vs17		// real*imag, imag*imag


.endm

.macro SAVE1x2


	mr		T1,	CO

#ifndef TRMMKERNEL

	lxvd2x		vs16,	o0,	T1
	lxvd2x		vs17,	o16,	T1

#endif


	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs33,	vs33			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs32		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs33		// imagA*imagB

	xxswapd		vs32,	vs32			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs33,	vs33			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs32		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs33		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs8,	vs2,	vs3,	0	// merge real and imag part



	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs35,	vs35			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs34		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs35		// imagA*imagB

	xxswapd		vs34,	vs34			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs35,	vs35			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs34		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs35		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs9,	vs2,	vs3,	0	// merge real and imag part


#ifndef TRMMKERNEL

	xvadddp		vs8,	vs8,	vs16
	xvadddp		vs9,	vs9,	vs17

#endif

	stxvd2x		vs8,	o0,	T1
	stxvd2x		vs9,	o16,	T1

	add		T1,	T1,	LDC
	addi		CO,	CO,	32

.endm


/**********************************************************************************************
* Macros for N=1 and M=1
**********************************************************************************************/

.macro LOAD1x1_1

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	lxvd2x		vs0,	o0,	AO		// load real,imag from A

	addi		AO,	AO,	16


.endm

.macro KERNEL1x1_I1

	lxvd2x		vs8,	o0,	AO		// load real,imag from A

	addi		AO,	AO,	16

	lxvd2x		vs20,	o0,	BO		// load real part from B
	lxvd2x		vs21,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	xvmuldp		vs32,	vs0,	vs16		// real*real, imag*real
	xvmuldp		vs33,	vs0,	vs17		// real*imag, imag*imag


.endm

.macro KERNEL1x1_1

	lxvd2x		vs8,	o0,	AO		// load real,imag from A

	addi		AO,	AO,	16

	lxvd2x		vs20,	o0,	BO		// load real part from B
	lxvd2x		vs21,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	xvmaddadp	vs32,	vs0,	vs16		// real*real, imag*real
	xvmaddadp	vs33,	vs0,	vs17		// real*imag, imag*imag


.endm

.macro KERNEL1x1_2

	lxvd2x		vs0,	o0,	AO		// load real,imag from A

	addi		AO,	AO,	16

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	xvmaddadp	vs32,	vs8,	vs20		// real*real, imag*real
	xvmaddadp	vs33,	vs8,	vs21		// real*imag, imag*imag


.endm

.macro KERNEL1x1_E2


	xvmaddadp	vs32,	vs8,	vs20		// real*real, imag*real
	xvmaddadp	vs33,	vs8,	vs21		// real*imag, imag*imag


.endm

.macro KERNEL1x1_SUBI1

	lxvd2x		vs0,	o0,	AO		// load real,imag from A

	addi		AO,	AO,	16

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	xvmuldp		vs32,	vs0,	vs16		// real*real, imag*real
	xvmuldp		vs33,	vs0,	vs17		// real*imag, imag*imag


.endm

.macro KERNEL1x1_SUB1

	lxvd2x		vs0,	o0,	AO		// load real,imag from A

	addi		AO,	AO,	16

	lxvd2x		vs16,	o0,	BO		// load real part from B
	lxvd2x		vs17,	o16,	BO		// load imag part from B

	addi		BO,	BO,	32

	xvmaddadp	vs32,	vs0,	vs16		// real*real, imag*real
	xvmaddadp	vs33,	vs0,	vs17		// real*imag, imag*imag


.endm

.macro SAVE1x1


	mr		T1,	CO

#ifndef TRMMKERNEL

	lxvd2x		vs16,	o0,	T1

#endif


	xxlxor		vs0,	vs0,	vs0
	xxlxor		vs1,	vs1,	vs1
	xxswapd		vs33,	vs33			// realA*imagB, imagA*imagB -> imagA*imagB, realA*imagB

	XSFADD_R1	vs0,	vs0,	vs32		// realA*realB
	XSFADD_R2	vs0,	vs0,	vs33		// imagA*imagB

	xxswapd		vs32,	vs32			// realA*realB, imagA*realB -> imagA*realB, realA*realB
	xxswapd		vs33,	vs33			// imagA*imagB, realA*imagB -> realA*imagB, imagA*imagB

	XSFADD_I1	vs1,	vs1,	vs32		// realA*imagB
	XSFADD_I2	vs1,	vs1,	vs33		// imagA*realB

	xsmuldp		vs4,	vs0,	alpha_r		// real*alpha_r
	xsmuldp		vs5,	vs1,	alpha_i		// imag*alpha_i
	xsmuldp		vs6,	vs0,	alpha_i		// real*alpha_i
	xsmuldp		vs7,	vs1,	alpha_r		// imag*alpha_r

	xssubdp		vs2,	vs4,	vs5		// real*alpha_r - imag*alpha_i
	xsadddp		vs3,	vs6,	vs7		// real*alpha_i + imag*alpha_r
	xxpermdi	vs8,	vs2,	vs3,	0	// merge real and imag part


#ifndef TRMMKERNEL

	xvadddp		vs8,	vs8,	vs16

#endif

	stxvd2x		vs8,	o0,	T1

	add		T1,	T1,	LDC
	addi		CO,	CO,	16

.endm



.macro ZCOPYB_1x1

        lxvdsx          vs4,    o0,     BO              // b0_r
        lxvdsx          vs5,    o8,     BO              // b0_i
        addi            BO,     BO,     16
        stxvd2x         vs4,    o0,     BBO
        stxvd2x         vs5,    o16,    BBO
        addi            BBO,    BBO,    32

.endm


.macro ZCOPYB_8x1

        lxvd2x          vs32,   o0,     BO
        lxvd2x          vs33,  o16,     BO
        lxvd2x          vs34,  o32,     BO
        lxvd2x          vs35,  o48,     BO
        addi            BO,     BO,     64

        lxvd2x          vs36,   o0,     BO
        lxvd2x          vs37,  o16,     BO
        lxvd2x          vs38,  o32,     BO
        lxvd2x          vs39,  o48,     BO
        addi            BO,     BO,     64

	xxspltd		vs40, vs32, 0
	xxspltd		vs41, vs32, 1
	xxspltd		vs42, vs33, 0
	xxspltd		vs43, vs33, 1
	xxspltd		vs44, vs34, 0
	xxspltd		vs45, vs34, 1
	xxspltd		vs46, vs35, 0
	xxspltd		vs47, vs35, 1

	xxspltd		vs48, vs36, 0
	xxspltd		vs49, vs36, 1
	xxspltd		vs50, vs37, 0
	xxspltd		vs51, vs37, 1
	xxspltd		vs52, vs38, 0
	xxspltd		vs53, vs38, 1
	xxspltd		vs54, vs39, 0
	xxspltd		vs55, vs39, 1

        stxvd2x         vs40,    o0,     BBO
        stxvd2x         vs41,   o16,     BBO
        stxvd2x         vs42,   o32,     BBO
        stxvd2x         vs43,   o48,     BBO
        addi            BBO,    BBO,    64

        stxvd2x         vs44,    o0,     BBO
        stxvd2x         vs45,   o16,     BBO
        stxvd2x         vs46,   o32,     BBO
        stxvd2x         vs47,   o48,     BBO
        addi            BBO,    BBO,    64

        stxvd2x         vs48,    o0,     BBO
        stxvd2x         vs49,   o16,     BBO
        stxvd2x         vs50,   o32,     BBO
        stxvd2x         vs51,   o48,     BBO
        addi            BBO,    BBO,    64

        stxvd2x         vs52,    o0,     BBO
        stxvd2x         vs53,   o16,     BBO
        stxvd2x         vs54,   o32,     BBO
        stxvd2x         vs55,   o48,     BBO
        addi            BBO,    BBO,    64

.endm


