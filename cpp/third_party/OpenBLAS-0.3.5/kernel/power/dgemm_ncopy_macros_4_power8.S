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


/**********************************************************************************************
* Macros for N=4 and M=16
**********************************************************************************************/

.macro COPY_4x16

	lxvd2x		vs0,	o0,	A0
	lxvd2x		vs1,	o0,	A1
	lxvd2x		vs2,	o0,	A2
	lxvd2x		vs3,	o0,	A3

	lxvd2x		vs4,	o16,	A0
	lxvd2x		vs5,	o16,	A1
	lxvd2x		vs6,	o16,	A2
	lxvd2x		vs7,	o16,	A3

	xxpermdi	vs32,	vs0,	vs1,	0
	xxpermdi	vs33,	vs2,	vs3,	0
	xxpermdi	vs34,	vs0,	vs1,	3
	xxpermdi	vs35,	vs2,	vs3,	3

	xxpermdi	vs36,	vs4,	vs5,	0
	xxpermdi	vs37,	vs6,	vs7,	0
	xxpermdi	vs38,	vs4,	vs5,	3
	xxpermdi	vs39,	vs6,	vs7,	3	

	lxvd2x		vs0,	o32,	A0
	lxvd2x		vs1,	o32,	A1
	lxvd2x		vs2,	o32,	A2
	lxvd2x		vs3,	o32,	A3

	lxvd2x		vs4,	o48,	A0
	lxvd2x		vs5,	o48,	A1
	lxvd2x		vs6,	o48,	A2
	lxvd2x		vs7,	o48,	A3


	xxpermdi	vs40,	vs0,	vs1,	0
	xxpermdi	vs41,	vs2,	vs3,	0
	xxpermdi	vs42,	vs0,	vs1,	3
	xxpermdi	vs43,	vs2,	vs3,	3

	xxpermdi	vs44,	vs4,	vs5,	0
	xxpermdi	vs45,	vs6,	vs7,	0
	xxpermdi	vs46,	vs4,	vs5,	3
	xxpermdi	vs47,	vs6,	vs7,	3	

	lxvd2x		vs0,	o64,	A0
	lxvd2x		vs1,	o64,	A1
	lxvd2x		vs2,	o64,	A2
	lxvd2x		vs3,	o64,	A3

	lxvd2x		vs4,	o80,	A0
	lxvd2x		vs5,	o80,	A1
	lxvd2x		vs6,	o80,	A2
	lxvd2x		vs7,	o80,	A3


	xxpermdi	vs48,	vs0,	vs1,	0
	xxpermdi	vs49,	vs2,	vs3,	0
	xxpermdi	vs50,	vs0,	vs1,	3
	xxpermdi	vs51,	vs2,	vs3,	3

	xxpermdi	vs8,	vs4,	vs5,	0
	xxpermdi	vs9,	vs6,	vs7,	0
	xxpermdi	vs10,	vs4,	vs5,	3
	xxpermdi	vs11,	vs6,	vs7,	3	

	lxvd2x		vs0,	o96,	A0
	lxvd2x		vs1,	o96,	A1
	lxvd2x		vs2,	o96,	A2
	lxvd2x		vs3,	o96,	A3

 
	lxvd2x		vs6,	o112,	A0
	lxvd2x		vs7,	o112,	A1
	lxvd2x		vs12,	o112,	A2
	lxvd2x		vs13,	o112,	A3


	xxpermdi	vs4,	vs0,	vs1,	0
	xxpermdi	vs5,	vs2,	vs3,	0
	xxpermdi	vs0,	vs0,	vs1,	3
	xxpermdi	vs2,	vs2,	vs3,	3


	addi		A0,	A0,	128
	addi		A1,	A1,	128

	xxpermdi	vs1,	vs6,	vs7,	0
	xxpermdi	vs3,	vs12,	vs13,	0
	xxpermdi	vs6,	vs6,	vs7,	3
	xxpermdi	vs12,	vs12,	vs13,	3	

	dcbt		BO,	PREB

	addi		A3,	A3,	128
	addi		A2,	A2,	128

	stxvd2x		vs32,	o0,	BO
	stxvd2x		vs33,	o16,	BO
	stxvd2x		vs34,	o32,	BO
	stxvd2x		vs35,	o48,	BO
	stxvd2x		vs36,	o64,	BO
	stxvd2x		vs37,	o80,	BO
	stxvd2x		vs38,	o96,	BO
	stxvd2x		vs39,	o112,	BO
	addi		BO,	BO,	128

	dcbt		BO,	PREB

	stxvd2x		vs40,	o0,	BO
	stxvd2x		vs41,	o16,	BO
	stxvd2x		vs42,	o32,	BO
	stxvd2x		vs43,	o48,	BO
	stxvd2x		vs44,	o64,	BO
	stxvd2x		vs45,	o80,	BO
	stxvd2x		vs46,	o96,	BO
	stxvd2x		vs47,	o112,	BO
	addi		BO,	BO,	128

	dcbt		BO,	PREB

	stxvd2x		vs48,	o0,	BO
	stxvd2x		vs49,	o16,	BO
	stxvd2x		vs50,	o32,	BO
	stxvd2x		vs51,	o48,	BO
	stxvd2x		vs8,	o64,	BO
	stxvd2x		vs9,	o80,	BO
	stxvd2x		vs10,	o96,	BO
	stxvd2x		vs11,	o112,	BO
	addi		BO,	BO,	128

	dcbt		BO,	PREB

	stxvd2x		vs4,	o0,	BO
	stxvd2x		vs5,	o16,	BO
	stxvd2x		vs0,	o32,	BO
	stxvd2x		vs2,	o48,	BO
	stxvd2x		vs1,	o64,	BO
	stxvd2x		vs3,	o80,	BO
	stxvd2x		vs6,	o96,	BO
	stxvd2x		vs12,	o112,	BO
	addi		BO,	BO,	128


.endm


/**********************************************************************************************
* Macros for N=4 and M=8
**********************************************************************************************/

.macro COPY_4x8

	lxvd2x		vs0,	o0,	A0
	lxvd2x		vs1,	o16,	A0
	lxvd2x		vs2,	o32,	A0
	lxvd2x		vs3,	o48,	A0
	addi		A0,	A0,	64


	lxvd2x		vs8,	o0,	A1
	lxvd2x		vs9,	o16,	A1
	lxvd2x		vs10,	o32,	A1
	lxvd2x		vs11,	o48,	A1
	addi		A1,	A1,	64


	lxvd2x		vs4,	o0,	A2
	lxvd2x		vs5,	o16,	A2
	lxvd2x		vs6,	o32,	A2
	lxvd2x		vs7,	o48,	A2
	addi		A2,	A2,	64


	lxvd2x		vs12,	o0,	A3
	lxvd2x		vs13,	o16,	A3
	lxvd2x		vs50,	o32,	A3
	lxvd2x		vs51,	o48,	A3
	addi		A3,	A3,	64


	xxpermdi	vs32,	vs0,	vs8,	0
	xxpermdi	vs33,	vs4,	vs12,	0
	xxpermdi	vs34,	vs0,	vs8,	3
	xxpermdi	vs35,	vs4,	vs12,	3

	xxpermdi	vs36,	vs1,	vs9,	0
	xxpermdi	vs37,	vs5,	vs13,	0
	xxpermdi	vs38,	vs1,	vs9,	3
	xxpermdi	vs39,	vs5,	vs13,	3

	xxpermdi	vs40,	vs2,	vs10,	0
	xxpermdi	vs41,	vs6,	vs50,	0
	xxpermdi	vs42,	vs2,	vs10,	3
	xxpermdi	vs43,	vs6,	vs50,	3

	xxpermdi	vs44,	vs3,	vs11,	0
	xxpermdi	vs45,	vs7,	vs51,	0
	xxpermdi	vs46,	vs3,	vs11,	3
	xxpermdi	vs47,	vs7,	vs51,	3


	stxvd2x		vs32,	o0,	BO
	stxvd2x		vs33,	o16,	BO
	stxvd2x		vs34,	o32,	BO
	stxvd2x		vs35,	o48,	BO
	stxvd2x		vs36,	o64,	BO
	stxvd2x		vs37,	o80,	BO
	stxvd2x		vs38,	o96,	BO
	stxvd2x		vs39,	o112,	BO
	addi		BO,	BO,	128

	stxvd2x		vs40,	o0,	BO
	stxvd2x		vs41,	o16,	BO
	stxvd2x		vs42,	o32,	BO
	stxvd2x		vs43,	o48,	BO
	stxvd2x		vs44,	o64,	BO
	stxvd2x		vs45,	o80,	BO
	stxvd2x		vs46,	o96,	BO
	stxvd2x		vs47,	o112,	BO
	addi		BO,	BO,	128


.endm


/**********************************************************************************************
* Macros for N=4 and M=4
**********************************************************************************************/

.macro COPY_4x4

	lxvd2x		vs0,	o0,	A0
	lxvd2x		vs1,	o16,	A0
	addi		A0,	A0,	32


	lxvd2x		vs8,	o0,	A1
	lxvd2x		vs9,	o16,	A1
	addi		A1,	A1,	32


	lxvd2x		vs10,	o0,	A2
	lxvd2x		vs11,	o16,	A2
	addi		A2,	A2,	32


	lxvd2x		vs12,	o0,	A3
	lxvd2x		vs13,	o16,	A3
	addi		A3,	A3,	32


	xxpermdi	vs32,	vs0,	vs8,	0
	xxpermdi	vs33,	vs10,	vs12,	0
	xxpermdi	vs34,	vs0,	vs8,	3
	xxpermdi	vs35,	vs10,	vs12,	3

	xxpermdi	vs36,	vs1,	vs9,	0
	xxpermdi	vs37,	vs11,	vs13,	0
	xxpermdi	vs38,	vs1,	vs9,	3
	xxpermdi	vs39,	vs11,	vs13,	3


	stxvd2x		vs32,	o0,	BO
	stxvd2x		vs33,	o16,	BO
	stxvd2x		vs34,	o32,	BO
	stxvd2x		vs35,	o48,	BO
	stxvd2x		vs36,	o64,	BO
	stxvd2x		vs37,	o80,	BO
	stxvd2x		vs38,	o96,	BO
	stxvd2x		vs39,	o112,	BO
	addi		BO,	BO,	128


.endm


/**********************************************************************************************
* Macros for N=4 and M=2
**********************************************************************************************/

.macro COPY_4x2

	lxvd2x		vs0,	o0,	A0
	addi		A0,	A0,	16


	lxvd2x		vs8,	o0,	A1
	addi		A1,	A1,	16


	lxvd2x		vs9,	o0,	A2
	addi		A2,	A2,	16


	lxvd2x		vs10,	o0,	A3
	addi		A3,	A3,	16


	xxpermdi	vs32,	vs0,	vs8,	0
	xxpermdi	vs33,	vs9,	vs10,	0
	xxpermdi	vs34,	vs0,	vs8,	3
	xxpermdi	vs35,	vs9,	vs10,	3


	stxvd2x		vs32,	o0,	BO
	stxvd2x		vs33,	o16,	BO
	stxvd2x		vs34,	o32,	BO
	stxvd2x		vs35,	o48,	BO
	addi		BO,	BO,	64


.endm


/**********************************************************************************************
* Macros for N=4 and M=1
**********************************************************************************************/

.macro COPY_4x1

	lxsdx		vs0,	o0,	A0
	addi		A0,	A0,	8


	lxsdx		vs8,	o0,	A1
	addi		A1,	A1,	8


	lxsdx		vs9,	o0,	A2
	addi		A2,	A2,	8


	lxsdx		vs10,	o0,	A3
	addi		A3,	A3,	8


	xxpermdi	vs32,	vs0,	vs8,	0
	xxpermdi	vs33,	vs9,	vs10,	0


	stxvd2x		vs32,	o0,	BO
	stxvd2x		vs33,	o16,	BO
	addi		BO,	BO,	32


.endm


/**********************************************************************************************
* Macros for N=2 and M=16
**********************************************************************************************/

.macro COPY_2x16

	lxvd2x		vs0,	o0,	A0
	lxvd2x		vs1,	o16,	A0
	lxvd2x		vs2,	o32,	A0
	lxvd2x		vs3,	o48,	A0
	lxvd2x		vs4,	o64,	A0
	lxvd2x		vs5,	o80,	A0
	lxvd2x		vs6,	o96,	A0
	lxvd2x		vs7,	o112,	A0
	addi		A0,	A0,	128


	lxvd2x		vs8,	o0,	A1
	lxvd2x		vs9,	o16,	A1
	lxvd2x		vs10,	o32,	A1
	lxvd2x		vs11,	o48,	A1
	lxvd2x		vs12,	o64,	A1
	lxvd2x		vs13,	o80,	A1
	lxvd2x		vs48,	o96,	A1
	lxvd2x		vs49,	o112,	A1
	addi		A1,	A1,	128


	xxpermdi	vs32,	vs0,	vs8,	0
	xxpermdi	vs33,	vs0,	vs8,	3

	xxpermdi	vs34,	vs1,	vs9,	0
	xxpermdi	vs35,	vs1,	vs9,	3

	xxpermdi	vs36,	vs2,	vs10,	0
	xxpermdi	vs37,	vs2,	vs10,	3

	xxpermdi	vs38,	vs3,	vs11,	0
	xxpermdi	vs39,	vs3,	vs11,	3

	xxpermdi	vs40,	vs4,	vs12,	0
	xxpermdi	vs41,	vs4,	vs12,	3

	xxpermdi	vs42,	vs5,	vs13,	0
	xxpermdi	vs43,	vs5,	vs13,	3

	xxpermdi	vs44,	vs6,	vs48,	0
	xxpermdi	vs45,	vs6,	vs48,	3

	xxpermdi	vs46,	vs7,	vs49,	0
	xxpermdi	vs47,	vs7,	vs49,	3


	stxvd2x		vs32,	o0,	BO
	stxvd2x		vs33,	o16,	BO
	stxvd2x		vs34,	o32,	BO
	stxvd2x		vs35,	o48,	BO
	stxvd2x		vs36,	o64,	BO
	stxvd2x		vs37,	o80,	BO
	stxvd2x		vs38,	o96,	BO
	stxvd2x		vs39,	o112,	BO
	addi		BO,	BO,	128

	stxvd2x		vs40,	o0,	BO
	stxvd2x		vs41,	o16,	BO
	stxvd2x		vs42,	o32,	BO
	stxvd2x		vs43,	o48,	BO
	stxvd2x		vs44,	o64,	BO
	stxvd2x		vs45,	o80,	BO
	stxvd2x		vs46,	o96,	BO
	stxvd2x		vs47,	o112,	BO
	addi		BO,	BO,	128


.endm


/**********************************************************************************************
* Macros for N=2 and M=8
**********************************************************************************************/

.macro COPY_2x8

	lxvd2x		vs0,	o0,	A0
	lxvd2x		vs1,	o16,	A0
	lxvd2x		vs2,	o32,	A0
	lxvd2x		vs3,	o48,	A0
	addi		A0,	A0,	64


	lxvd2x		vs8,	o0,	A1
	lxvd2x		vs9,	o16,	A1
	lxvd2x		vs10,	o32,	A1
	lxvd2x		vs11,	o48,	A1
	addi		A1,	A1,	64


	xxpermdi	vs32,	vs0,	vs8,	0
	xxpermdi	vs33,	vs0,	vs8,	3

	xxpermdi	vs34,	vs1,	vs9,	0
	xxpermdi	vs35,	vs1,	vs9,	3

	xxpermdi	vs36,	vs2,	vs10,	0
	xxpermdi	vs37,	vs2,	vs10,	3

	xxpermdi	vs38,	vs3,	vs11,	0
	xxpermdi	vs39,	vs3,	vs11,	3


	stxvd2x		vs32,	o0,	BO
	stxvd2x		vs33,	o16,	BO
	stxvd2x		vs34,	o32,	BO
	stxvd2x		vs35,	o48,	BO
	stxvd2x		vs36,	o64,	BO
	stxvd2x		vs37,	o80,	BO
	stxvd2x		vs38,	o96,	BO
	stxvd2x		vs39,	o112,	BO
	addi		BO,	BO,	128


.endm


/**********************************************************************************************
* Macros for N=2 and M=4
**********************************************************************************************/

.macro COPY_2x4

	lxvd2x		vs0,	o0,	A0
	lxvd2x		vs1,	o16,	A0
	addi		A0,	A0,	32


	lxvd2x		vs8,	o0,	A1
	lxvd2x		vs9,	o16,	A1
	addi		A1,	A1,	32


	xxpermdi	vs32,	vs0,	vs8,	0
	xxpermdi	vs33,	vs0,	vs8,	3

	xxpermdi	vs34,	vs1,	vs9,	0
	xxpermdi	vs35,	vs1,	vs9,	3


	stxvd2x		vs32,	o0,	BO
	stxvd2x		vs33,	o16,	BO
	stxvd2x		vs34,	o32,	BO
	stxvd2x		vs35,	o48,	BO
	addi		BO,	BO,	64


.endm


/**********************************************************************************************
* Macros for N=2 and M=2
**********************************************************************************************/

.macro COPY_2x2

	lxvd2x		vs0,	o0,	A0
	addi		A0,	A0,	16


	lxvd2x		vs8,	o0,	A1
	addi		A1,	A1,	16


	xxpermdi	vs32,	vs0,	vs8,	0
	xxpermdi	vs33,	vs0,	vs8,	3


	stxvd2x		vs32,	o0,	BO
	stxvd2x		vs33,	o16,	BO
	addi		BO,	BO,	32


.endm


/**********************************************************************************************
* Macros for N=2 and M=1
**********************************************************************************************/

.macro COPY_2x1

	lxsdx		vs0,	o0,	A0
	addi		A0,	A0,	8


	lxsdx		vs8,	o0,	A1
	addi		A1,	A1,	8


	xxpermdi	vs32,	vs0,	vs8,	0


	stxvd2x		vs32,	o0,	BO
	addi		BO,	BO,	16


.endm


/**********************************************************************************************
* Macros for N=1 and M=16
**********************************************************************************************/

.macro COPY_1x16

	lxvd2x		vs0,	o0,	A0
	lxvd2x		vs1,	o16,	A0
	lxvd2x		vs2,	o32,	A0
	lxvd2x		vs3,	o48,	A0
	lxvd2x		vs4,	o64,	A0
	lxvd2x		vs5,	o80,	A0
	lxvd2x		vs6,	o96,	A0
	lxvd2x		vs7,	o112,	A0
	addi		A0,	A0,	128


	stxvd2x		vs0,	o0,	BO
	stxvd2x		vs1,	o16,	BO
	stxvd2x		vs2,	o32,	BO
	stxvd2x		vs3,	o48,	BO
	addi		BO,	BO,	64

	stxvd2x		vs4,	o0,	BO
	stxvd2x		vs5,	o16,	BO
	stxvd2x		vs6,	o32,	BO
	stxvd2x		vs7,	o48,	BO
	addi		BO,	BO,	64


.endm


/**********************************************************************************************
* Macros for N=1 and M=8
**********************************************************************************************/

.macro COPY_1x8

	lxvd2x		vs0,	o0,	A0
	lxvd2x		vs1,	o16,	A0
	lxvd2x		vs2,	o32,	A0
	lxvd2x		vs3,	o48,	A0
	addi		A0,	A0,	64


	stxvd2x		vs0,	o0,	BO
	stxvd2x		vs1,	o16,	BO
	stxvd2x		vs2,	o32,	BO
	stxvd2x		vs3,	o48,	BO
	addi		BO,	BO,	64


.endm


/**********************************************************************************************
* Macros for N=1 and M=4
**********************************************************************************************/

.macro COPY_1x4

	lxvd2x		vs0,	o0,	A0
	lxvd2x		vs1,	o16,	A0
	addi		A0,	A0,	32


	stxvd2x		vs0,	o0,	BO
	stxvd2x		vs1,	o16,	BO
	addi		BO,	BO,	32


.endm


/**********************************************************************************************
* Macros for N=1 and M=2
**********************************************************************************************/

.macro COPY_1x2

	lxvd2x		vs0,	o0,	A0
	addi		A0,	A0,	16


	stxvd2x		vs0,	o0,	BO
	addi		BO,	BO,	16


.endm


/**********************************************************************************************
* Macros for N=1 and M=1
**********************************************************************************************/

.macro COPY_1x1

	lxsdx		vs0,	o0,	A0
	addi		A0,	A0,	8


	stxsdx		vs0,	o0,	BO
	addi		BO,	BO,	8


.endm

