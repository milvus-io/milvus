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
* 2016/04/21 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/


/**********************************************************************************************
* Macros for N=8 and M=16
**********************************************************************************************/

.macro LOAD8x16_1

	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO
	lxvw4x		vs2,	o32,	AO
	lxvw4x		vs3,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		T1,	T1,	64

	lxvw4x		vs12,	o0,	T1
	lxvw4x		vs13,	o16,	T1
	lxvw4x		vs14,	o32,	T1
	lxvw4x		vs15,	o48,	T1

	addi		BO,	BO,	128

.endm

.macro KERNEL8x16_I1


	lxvw4x		vs4,	o0,	AO
	lxvw4x		vs5,	o16,	AO
	lxvw4x		vs6,	o32,	AO
	lxvw4x		vs7,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1
	lxvw4x		vs17,	o16,	T1
	lxvw4x		vs18,	o32,	T1
	lxvw4x		vs19,	o48,	T1

	addi		T1,	T1,	64

	lxvw4x		vs20,	o0,	T1
	lxvw4x		vs21,	o16,	T1
	lxvw4x		vs22,	o32,	T1
	lxvw4x		vs23,	o48,	T1

	addi		BO,	BO,	128


	xvmulsp		vs32,	vs0,	vs8
	xvmulsp		vs33,	vs1,	vs8
	xvmulsp		vs34,	vs2,	vs8
	xvmulsp		vs35,	vs3,	vs8

	xvmulsp		vs36,	vs0,	vs9
	xvmulsp		vs37,	vs1,	vs9
	xvmulsp		vs38,	vs2,	vs9
	xvmulsp		vs39,	vs3,	vs9

	xvmulsp		vs40,	vs0,	vs10
	xvmulsp		vs41,	vs1,	vs10
	xvmulsp		vs42,	vs2,	vs10
	xvmulsp		vs43,	vs3,	vs10

	xvmulsp		vs44,	vs0,	vs11
	xvmulsp		vs45,	vs1,	vs11
	xvmulsp		vs46,	vs2,	vs11
	xvmulsp		vs47,	vs3,	vs11

	xvmulsp		vs48,	vs0,	vs12
	xvmulsp		vs49,	vs1,	vs12
	xvmulsp		vs50,	vs2,	vs12
	xvmulsp		vs51,	vs3,	vs12

	xvmulsp		vs52,	vs0,	vs13
	xvmulsp		vs53,	vs1,	vs13
	xvmulsp		vs54,	vs2,	vs13
	xvmulsp		vs55,	vs3,	vs13

	xvmulsp		vs56,	vs0,	vs14
	xvmulsp		vs57,	vs1,	vs14
	xvmulsp		vs58,	vs2,	vs14
	xvmulsp		vs59,	vs3,	vs14

	xvmulsp		vs60,	vs0,	vs15
	xvmulsp		vs61,	vs1,	vs15
	xvmulsp		vs62,	vs2,	vs15
	xvmulsp		vs63,	vs3,	vs15


.endm

.macro KERNEL8x16_1


	lxvw4x		vs4,	o0,	AO
	lxvw4x		vs5,	o16,	AO
	lxvw4x		vs6,	o32,	AO
	lxvw4x		vs7,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1
	lxvw4x		vs17,	o16,	T1
	lxvw4x		vs18,	o32,	T1
	lxvw4x		vs19,	o48,	T1

	addi		T1,	T1,	64

	lxvw4x		vs20,	o0,	T1
	lxvw4x		vs21,	o16,	T1
	lxvw4x		vs22,	o32,	T1
	lxvw4x		vs23,	o48,	T1

	addi		BO,	BO,	128


	xvmaddasp	vs32,	vs0,	vs8
	xvmaddasp	vs33,	vs1,	vs8
	xvmaddasp	vs34,	vs2,	vs8
	xvmaddasp	vs35,	vs3,	vs8

	xvmaddasp	vs36,	vs0,	vs9
	xvmaddasp	vs37,	vs1,	vs9
	xvmaddasp	vs38,	vs2,	vs9
	xvmaddasp	vs39,	vs3,	vs9

	xvmaddasp	vs40,	vs0,	vs10
	xvmaddasp	vs41,	vs1,	vs10
	xvmaddasp	vs42,	vs2,	vs10
	xvmaddasp	vs43,	vs3,	vs10

	xvmaddasp	vs44,	vs0,	vs11
	xvmaddasp	vs45,	vs1,	vs11
	xvmaddasp	vs46,	vs2,	vs11
	xvmaddasp	vs47,	vs3,	vs11

	xvmaddasp	vs48,	vs0,	vs12
	xvmaddasp	vs49,	vs1,	vs12
	xvmaddasp	vs50,	vs2,	vs12
	xvmaddasp	vs51,	vs3,	vs12

	xvmaddasp	vs52,	vs0,	vs13
	xvmaddasp	vs53,	vs1,	vs13
	xvmaddasp	vs54,	vs2,	vs13
	xvmaddasp	vs55,	vs3,	vs13

	xvmaddasp	vs56,	vs0,	vs14
	xvmaddasp	vs57,	vs1,	vs14
	xvmaddasp	vs58,	vs2,	vs14
	xvmaddasp	vs59,	vs3,	vs14

	xvmaddasp	vs60,	vs0,	vs15
	xvmaddasp	vs61,	vs1,	vs15
	xvmaddasp	vs62,	vs2,	vs15
	xvmaddasp	vs63,	vs3,	vs15


.endm

.macro KERNEL8x16_2


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO
	lxvw4x		vs2,	o32,	AO
	lxvw4x		vs3,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		T1,	T1,	64

	lxvw4x		vs12,	o0,	T1
	lxvw4x		vs13,	o16,	T1
	lxvw4x		vs14,	o32,	T1
	lxvw4x		vs15,	o48,	T1

	addi		BO,	BO,	128


	xvmaddasp	vs32,	vs4,	vs16
	xvmaddasp	vs33,	vs5,	vs16
	xvmaddasp	vs34,	vs6,	vs16
	xvmaddasp	vs35,	vs7,	vs16

	xvmaddasp	vs36,	vs4,	vs17
	xvmaddasp	vs37,	vs5,	vs17
	xvmaddasp	vs38,	vs6,	vs17
	xvmaddasp	vs39,	vs7,	vs17

	xvmaddasp	vs40,	vs4,	vs18
	xvmaddasp	vs41,	vs5,	vs18
	xvmaddasp	vs42,	vs6,	vs18
	xvmaddasp	vs43,	vs7,	vs18

	xvmaddasp	vs44,	vs4,	vs19
	xvmaddasp	vs45,	vs5,	vs19
	xvmaddasp	vs46,	vs6,	vs19
	xvmaddasp	vs47,	vs7,	vs19

	xvmaddasp	vs48,	vs4,	vs20
	xvmaddasp	vs49,	vs5,	vs20
	xvmaddasp	vs50,	vs6,	vs20
	xvmaddasp	vs51,	vs7,	vs20

	xvmaddasp	vs52,	vs4,	vs21
	xvmaddasp	vs53,	vs5,	vs21
	xvmaddasp	vs54,	vs6,	vs21
	xvmaddasp	vs55,	vs7,	vs21

	xvmaddasp	vs56,	vs4,	vs22
	xvmaddasp	vs57,	vs5,	vs22
	xvmaddasp	vs58,	vs6,	vs22
	xvmaddasp	vs59,	vs7,	vs22

	xvmaddasp	vs60,	vs4,	vs23
	xvmaddasp	vs61,	vs5,	vs23
	xvmaddasp	vs62,	vs6,	vs23
	xvmaddasp	vs63,	vs7,	vs23


.endm

.macro KERNEL8x16_E2


	xvmaddasp	vs32,	vs4,	vs16
	xvmaddasp	vs33,	vs5,	vs16
	xvmaddasp	vs34,	vs6,	vs16
	xvmaddasp	vs35,	vs7,	vs16

	xvmaddasp	vs36,	vs4,	vs17
	xvmaddasp	vs37,	vs5,	vs17
	xvmaddasp	vs38,	vs6,	vs17
	xvmaddasp	vs39,	vs7,	vs17

	xvmaddasp	vs40,	vs4,	vs18
	xvmaddasp	vs41,	vs5,	vs18
	xvmaddasp	vs42,	vs6,	vs18
	xvmaddasp	vs43,	vs7,	vs18

	xvmaddasp	vs44,	vs4,	vs19
	xvmaddasp	vs45,	vs5,	vs19
	xvmaddasp	vs46,	vs6,	vs19
	xvmaddasp	vs47,	vs7,	vs19

	xvmaddasp	vs48,	vs4,	vs20
	xvmaddasp	vs49,	vs5,	vs20
	xvmaddasp	vs50,	vs6,	vs20
	xvmaddasp	vs51,	vs7,	vs20

	xvmaddasp	vs52,	vs4,	vs21
	xvmaddasp	vs53,	vs5,	vs21
	xvmaddasp	vs54,	vs6,	vs21
	xvmaddasp	vs55,	vs7,	vs21

	xvmaddasp	vs56,	vs4,	vs22
	xvmaddasp	vs57,	vs5,	vs22
	xvmaddasp	vs58,	vs6,	vs22
	xvmaddasp	vs59,	vs7,	vs22

	xvmaddasp	vs60,	vs4,	vs23
	xvmaddasp	vs61,	vs5,	vs23
	xvmaddasp	vs62,	vs6,	vs23
	xvmaddasp	vs63,	vs7,	vs23


.endm

.macro KERNEL8x16_SUBI1


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO
	lxvw4x		vs2,	o32,	AO
	lxvw4x		vs3,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		T1,	T1,	64

	lxvw4x		vs12,	o0,	T1
	lxvw4x		vs13,	o16,	T1
	lxvw4x		vs14,	o32,	T1
	lxvw4x		vs15,	o48,	T1

	addi		BO,	BO,	128


	xvmulsp		vs32,	vs0,	vs8
	xvmulsp		vs33,	vs1,	vs8
	xvmulsp		vs34,	vs2,	vs8
	xvmulsp		vs35,	vs3,	vs8

	xvmulsp		vs36,	vs0,	vs9
	xvmulsp		vs37,	vs1,	vs9
	xvmulsp		vs38,	vs2,	vs9
	xvmulsp		vs39,	vs3,	vs9

	xvmulsp		vs40,	vs0,	vs10
	xvmulsp		vs41,	vs1,	vs10
	xvmulsp		vs42,	vs2,	vs10
	xvmulsp		vs43,	vs3,	vs10

	xvmulsp		vs44,	vs0,	vs11
	xvmulsp		vs45,	vs1,	vs11
	xvmulsp		vs46,	vs2,	vs11
	xvmulsp		vs47,	vs3,	vs11

	xvmulsp		vs48,	vs0,	vs12
	xvmulsp		vs49,	vs1,	vs12
	xvmulsp		vs50,	vs2,	vs12
	xvmulsp		vs51,	vs3,	vs12

	xvmulsp		vs52,	vs0,	vs13
	xvmulsp		vs53,	vs1,	vs13
	xvmulsp		vs54,	vs2,	vs13
	xvmulsp		vs55,	vs3,	vs13

	xvmulsp		vs56,	vs0,	vs14
	xvmulsp		vs57,	vs1,	vs14
	xvmulsp		vs58,	vs2,	vs14
	xvmulsp		vs59,	vs3,	vs14

	xvmulsp		vs60,	vs0,	vs15
	xvmulsp		vs61,	vs1,	vs15
	xvmulsp		vs62,	vs2,	vs15
	xvmulsp		vs63,	vs3,	vs15


.endm

.macro KERNEL8x16_SUB1


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO
	lxvw4x		vs2,	o32,	AO
	lxvw4x		vs3,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		T1,	T1,	64

	lxvw4x		vs12,	o0,	T1
	lxvw4x		vs13,	o16,	T1
	lxvw4x		vs14,	o32,	T1
	lxvw4x		vs15,	o48,	T1

	addi		BO,	BO,	128


	xvmaddasp	vs32,	vs0,	vs8
	xvmaddasp	vs33,	vs1,	vs8
	xvmaddasp	vs34,	vs2,	vs8
	xvmaddasp	vs35,	vs3,	vs8

	xvmaddasp	vs36,	vs0,	vs9
	xvmaddasp	vs37,	vs1,	vs9
	xvmaddasp	vs38,	vs2,	vs9
	xvmaddasp	vs39,	vs3,	vs9

	xvmaddasp	vs40,	vs0,	vs10
	xvmaddasp	vs41,	vs1,	vs10
	xvmaddasp	vs42,	vs2,	vs10
	xvmaddasp	vs43,	vs3,	vs10

	xvmaddasp	vs44,	vs0,	vs11
	xvmaddasp	vs45,	vs1,	vs11
	xvmaddasp	vs46,	vs2,	vs11
	xvmaddasp	vs47,	vs3,	vs11

	xvmaddasp	vs48,	vs0,	vs12
	xvmaddasp	vs49,	vs1,	vs12
	xvmaddasp	vs50,	vs2,	vs12
	xvmaddasp	vs51,	vs3,	vs12

	xvmaddasp	vs52,	vs0,	vs13
	xvmaddasp	vs53,	vs1,	vs13
	xvmaddasp	vs54,	vs2,	vs13
	xvmaddasp	vs55,	vs3,	vs13

	xvmaddasp	vs56,	vs0,	vs14
	xvmaddasp	vs57,	vs1,	vs14
	xvmaddasp	vs58,	vs2,	vs14
	xvmaddasp	vs59,	vs3,	vs14

	xvmaddasp	vs60,	vs0,	vs15
	xvmaddasp	vs61,	vs1,	vs15
	xvmaddasp	vs62,	vs2,	vs15
	xvmaddasp	vs63,	vs3,	vs15


.endm

.macro SAVE8x16

	mr		T1,	CO

#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1
	lxvw4x		vs2,	o32,	T1
	lxvw4x		vs3,	o48,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs32,	alpha_vr
	xvmulsp		vs1,	vs33,	alpha_vr
	xvmulsp		vs2,	vs34,	alpha_vr
	xvmulsp		vs3,	vs35,	alpha_vr
#else
	xvmaddasp	vs0,	vs32,	alpha_vr
	xvmaddasp	vs1,	vs33,	alpha_vr
	xvmaddasp	vs2,	vs34,	alpha_vr
	xvmaddasp	vs3,	vs35,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1
	stxvw4x		vs2,	o32,	T1
	stxvw4x		vs3,	o48,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1
	lxvw4x		vs2,	o32,	T1
	lxvw4x		vs3,	o48,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs36,	alpha_vr
	xvmulsp		vs1,	vs37,	alpha_vr
	xvmulsp		vs2,	vs38,	alpha_vr
	xvmulsp		vs3,	vs39,	alpha_vr
#else
	xvmaddasp	vs0,	vs36,	alpha_vr
	xvmaddasp	vs1,	vs37,	alpha_vr
	xvmaddasp	vs2,	vs38,	alpha_vr
	xvmaddasp	vs3,	vs39,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1
	stxvw4x		vs2,	o32,	T1
	stxvw4x		vs3,	o48,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1
	lxvw4x		vs2,	o32,	T1
	lxvw4x		vs3,	o48,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs40,	alpha_vr
	xvmulsp		vs1,	vs41,	alpha_vr
	xvmulsp		vs2,	vs42,	alpha_vr
	xvmulsp		vs3,	vs43,	alpha_vr
#else
	xvmaddasp	vs0,	vs40,	alpha_vr
	xvmaddasp	vs1,	vs41,	alpha_vr
	xvmaddasp	vs2,	vs42,	alpha_vr
	xvmaddasp	vs3,	vs43,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1
	stxvw4x		vs2,	o32,	T1
	stxvw4x		vs3,	o48,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1
	lxvw4x		vs2,	o32,	T1
	lxvw4x		vs3,	o48,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs44,	alpha_vr
	xvmulsp		vs1,	vs45,	alpha_vr
	xvmulsp		vs2,	vs46,	alpha_vr
	xvmulsp		vs3,	vs47,	alpha_vr
#else
	xvmaddasp	vs0,	vs44,	alpha_vr
	xvmaddasp	vs1,	vs45,	alpha_vr
	xvmaddasp	vs2,	vs46,	alpha_vr
	xvmaddasp	vs3,	vs47,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1
	stxvw4x		vs2,	o32,	T1
	stxvw4x		vs3,	o48,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1
	lxvw4x		vs2,	o32,	T1
	lxvw4x		vs3,	o48,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs48,	alpha_vr
	xvmulsp		vs1,	vs49,	alpha_vr
	xvmulsp		vs2,	vs50,	alpha_vr
	xvmulsp		vs3,	vs51,	alpha_vr
#else
	xvmaddasp	vs0,	vs48,	alpha_vr
	xvmaddasp	vs1,	vs49,	alpha_vr
	xvmaddasp	vs2,	vs50,	alpha_vr
	xvmaddasp	vs3,	vs51,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1
	stxvw4x		vs2,	o32,	T1
	stxvw4x		vs3,	o48,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1
	lxvw4x		vs2,	o32,	T1
	lxvw4x		vs3,	o48,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs52,	alpha_vr
	xvmulsp		vs1,	vs53,	alpha_vr
	xvmulsp		vs2,	vs54,	alpha_vr
	xvmulsp		vs3,	vs55,	alpha_vr
#else
	xvmaddasp	vs0,	vs52,	alpha_vr
	xvmaddasp	vs1,	vs53,	alpha_vr
	xvmaddasp	vs2,	vs54,	alpha_vr
	xvmaddasp	vs3,	vs55,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1
	stxvw4x		vs2,	o32,	T1
	stxvw4x		vs3,	o48,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1
	lxvw4x		vs2,	o32,	T1
	lxvw4x		vs3,	o48,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs56,	alpha_vr
	xvmulsp		vs1,	vs57,	alpha_vr
	xvmulsp		vs2,	vs58,	alpha_vr
	xvmulsp		vs3,	vs59,	alpha_vr
#else
	xvmaddasp	vs0,	vs56,	alpha_vr
	xvmaddasp	vs1,	vs57,	alpha_vr
	xvmaddasp	vs2,	vs58,	alpha_vr
	xvmaddasp	vs3,	vs59,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1
	stxvw4x		vs2,	o32,	T1
	stxvw4x		vs3,	o48,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1
	lxvw4x		vs2,	o32,	T1
	lxvw4x		vs3,	o48,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs60,	alpha_vr
	xvmulsp		vs1,	vs61,	alpha_vr
	xvmulsp		vs2,	vs62,	alpha_vr
	xvmulsp		vs3,	vs63,	alpha_vr
#else
	xvmaddasp	vs0,	vs60,	alpha_vr
	xvmaddasp	vs1,	vs61,	alpha_vr
	xvmaddasp	vs2,	vs62,	alpha_vr
	xvmaddasp	vs3,	vs63,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1
	stxvw4x		vs2,	o32,	T1
	stxvw4x		vs3,	o48,	T1

	add		T1,	T1,	LDC

	addi		CO,	CO,	64

.endm


/**********************************************************************************************
* Macros for N=8 and M=8
**********************************************************************************************/

.macro LOAD8x8_1

	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		T1,	T1,	64

	lxvw4x		vs12,	o0,	T1
	lxvw4x		vs13,	o16,	T1
	lxvw4x		vs14,	o32,	T1
	lxvw4x		vs15,	o48,	T1

	addi		BO,	BO,	128

.endm

.macro KERNEL8x8_I1


	lxvw4x		vs4,	o0,	AO
	lxvw4x		vs5,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1
	lxvw4x		vs17,	o16,	T1
	lxvw4x		vs18,	o32,	T1
	lxvw4x		vs19,	o48,	T1

	addi		T1,	T1,	64

	lxvw4x		vs20,	o0,	T1
	lxvw4x		vs21,	o16,	T1
	lxvw4x		vs22,	o32,	T1
	lxvw4x		vs23,	o48,	T1

	addi		BO,	BO,	128


	xvmulsp		vs32,	vs0,	vs8
	xvmulsp		vs33,	vs1,	vs8

	xvmulsp		vs34,	vs0,	vs9
	xvmulsp		vs35,	vs1,	vs9

	xvmulsp		vs36,	vs0,	vs10
	xvmulsp		vs37,	vs1,	vs10

	xvmulsp		vs38,	vs0,	vs11
	xvmulsp		vs39,	vs1,	vs11

	xvmulsp		vs40,	vs0,	vs12
	xvmulsp		vs41,	vs1,	vs12

	xvmulsp		vs42,	vs0,	vs13
	xvmulsp		vs43,	vs1,	vs13

	xvmulsp		vs44,	vs0,	vs14
	xvmulsp		vs45,	vs1,	vs14

	xvmulsp		vs46,	vs0,	vs15
	xvmulsp		vs47,	vs1,	vs15


.endm

.macro KERNEL8x8_1


	lxvw4x		vs4,	o0,	AO
	lxvw4x		vs5,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1
	lxvw4x		vs17,	o16,	T1
	lxvw4x		vs18,	o32,	T1
	lxvw4x		vs19,	o48,	T1

	addi		T1,	T1,	64

	lxvw4x		vs20,	o0,	T1
	lxvw4x		vs21,	o16,	T1
	lxvw4x		vs22,	o32,	T1
	lxvw4x		vs23,	o48,	T1

	addi		BO,	BO,	128


	xvmaddasp	vs32,	vs0,	vs8
	xvmaddasp	vs33,	vs1,	vs8

	xvmaddasp	vs34,	vs0,	vs9
	xvmaddasp	vs35,	vs1,	vs9

	xvmaddasp	vs36,	vs0,	vs10
	xvmaddasp	vs37,	vs1,	vs10

	xvmaddasp	vs38,	vs0,	vs11
	xvmaddasp	vs39,	vs1,	vs11

	xvmaddasp	vs40,	vs0,	vs12
	xvmaddasp	vs41,	vs1,	vs12

	xvmaddasp	vs42,	vs0,	vs13
	xvmaddasp	vs43,	vs1,	vs13

	xvmaddasp	vs44,	vs0,	vs14
	xvmaddasp	vs45,	vs1,	vs14

	xvmaddasp	vs46,	vs0,	vs15
	xvmaddasp	vs47,	vs1,	vs15


.endm

.macro KERNEL8x8_2


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		T1,	T1,	64

	lxvw4x		vs12,	o0,	T1
	lxvw4x		vs13,	o16,	T1
	lxvw4x		vs14,	o32,	T1
	lxvw4x		vs15,	o48,	T1

	addi		BO,	BO,	128


	xvmaddasp	vs32,	vs4,	vs16
	xvmaddasp	vs33,	vs5,	vs16

	xvmaddasp	vs34,	vs4,	vs17
	xvmaddasp	vs35,	vs5,	vs17

	xvmaddasp	vs36,	vs4,	vs18
	xvmaddasp	vs37,	vs5,	vs18

	xvmaddasp	vs38,	vs4,	vs19
	xvmaddasp	vs39,	vs5,	vs19

	xvmaddasp	vs40,	vs4,	vs20
	xvmaddasp	vs41,	vs5,	vs20

	xvmaddasp	vs42,	vs4,	vs21
	xvmaddasp	vs43,	vs5,	vs21

	xvmaddasp	vs44,	vs4,	vs22
	xvmaddasp	vs45,	vs5,	vs22

	xvmaddasp	vs46,	vs4,	vs23
	xvmaddasp	vs47,	vs5,	vs23


.endm

.macro KERNEL8x8_E2


	xvmaddasp	vs32,	vs4,	vs16
	xvmaddasp	vs33,	vs5,	vs16

	xvmaddasp	vs34,	vs4,	vs17
	xvmaddasp	vs35,	vs5,	vs17

	xvmaddasp	vs36,	vs4,	vs18
	xvmaddasp	vs37,	vs5,	vs18

	xvmaddasp	vs38,	vs4,	vs19
	xvmaddasp	vs39,	vs5,	vs19

	xvmaddasp	vs40,	vs4,	vs20
	xvmaddasp	vs41,	vs5,	vs20

	xvmaddasp	vs42,	vs4,	vs21
	xvmaddasp	vs43,	vs5,	vs21

	xvmaddasp	vs44,	vs4,	vs22
	xvmaddasp	vs45,	vs5,	vs22

	xvmaddasp	vs46,	vs4,	vs23
	xvmaddasp	vs47,	vs5,	vs23


.endm

.macro KERNEL8x8_SUBI1


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		T1,	T1,	64

	lxvw4x		vs12,	o0,	T1
	lxvw4x		vs13,	o16,	T1
	lxvw4x		vs14,	o32,	T1
	lxvw4x		vs15,	o48,	T1

	addi		BO,	BO,	128


	xvmulsp		vs32,	vs0,	vs8
	xvmulsp		vs33,	vs1,	vs8

	xvmulsp		vs34,	vs0,	vs9
	xvmulsp		vs35,	vs1,	vs9

	xvmulsp		vs36,	vs0,	vs10
	xvmulsp		vs37,	vs1,	vs10

	xvmulsp		vs38,	vs0,	vs11
	xvmulsp		vs39,	vs1,	vs11

	xvmulsp		vs40,	vs0,	vs12
	xvmulsp		vs41,	vs1,	vs12

	xvmulsp		vs42,	vs0,	vs13
	xvmulsp		vs43,	vs1,	vs13

	xvmulsp		vs44,	vs0,	vs14
	xvmulsp		vs45,	vs1,	vs14

	xvmulsp		vs46,	vs0,	vs15
	xvmulsp		vs47,	vs1,	vs15


.endm

.macro KERNEL8x8_SUB1


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		T1,	T1,	64

	lxvw4x		vs12,	o0,	T1
	lxvw4x		vs13,	o16,	T1
	lxvw4x		vs14,	o32,	T1
	lxvw4x		vs15,	o48,	T1

	addi		BO,	BO,	128


	xvmaddasp	vs32,	vs0,	vs8
	xvmaddasp	vs33,	vs1,	vs8

	xvmaddasp	vs34,	vs0,	vs9
	xvmaddasp	vs35,	vs1,	vs9

	xvmaddasp	vs36,	vs0,	vs10
	xvmaddasp	vs37,	vs1,	vs10

	xvmaddasp	vs38,	vs0,	vs11
	xvmaddasp	vs39,	vs1,	vs11

	xvmaddasp	vs40,	vs0,	vs12
	xvmaddasp	vs41,	vs1,	vs12

	xvmaddasp	vs42,	vs0,	vs13
	xvmaddasp	vs43,	vs1,	vs13

	xvmaddasp	vs44,	vs0,	vs14
	xvmaddasp	vs45,	vs1,	vs14

	xvmaddasp	vs46,	vs0,	vs15
	xvmaddasp	vs47,	vs1,	vs15


.endm

.macro SAVE8x8

	mr		T1,	CO

#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs32,	alpha_vr
	xvmulsp		vs1,	vs33,	alpha_vr
#else
	xvmaddasp	vs0,	vs32,	alpha_vr
	xvmaddasp	vs1,	vs33,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs34,	alpha_vr
	xvmulsp		vs1,	vs35,	alpha_vr
#else
	xvmaddasp	vs0,	vs34,	alpha_vr
	xvmaddasp	vs1,	vs35,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs36,	alpha_vr
	xvmulsp		vs1,	vs37,	alpha_vr
#else
	xvmaddasp	vs0,	vs36,	alpha_vr
	xvmaddasp	vs1,	vs37,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs38,	alpha_vr
	xvmulsp		vs1,	vs39,	alpha_vr
#else
	xvmaddasp	vs0,	vs38,	alpha_vr
	xvmaddasp	vs1,	vs39,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs40,	alpha_vr
	xvmulsp		vs1,	vs41,	alpha_vr
#else
	xvmaddasp	vs0,	vs40,	alpha_vr
	xvmaddasp	vs1,	vs41,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs42,	alpha_vr
	xvmulsp		vs1,	vs43,	alpha_vr
#else
	xvmaddasp	vs0,	vs42,	alpha_vr
	xvmaddasp	vs1,	vs43,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs44,	alpha_vr
	xvmulsp		vs1,	vs45,	alpha_vr
#else
	xvmaddasp	vs0,	vs44,	alpha_vr
	xvmaddasp	vs1,	vs45,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs46,	alpha_vr
	xvmulsp		vs1,	vs47,	alpha_vr
#else
	xvmaddasp	vs0,	vs46,	alpha_vr
	xvmaddasp	vs1,	vs47,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1

	add		T1,	T1,	LDC

	addi		CO,	CO,	32

.endm


/**********************************************************************************************
* Macros for N=8 and M=4
**********************************************************************************************/

.macro LOAD8x4_1

	lxvw4x		vs0,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		T1,	T1,	64

	lxvw4x		vs12,	o0,	T1
	lxvw4x		vs13,	o16,	T1
	lxvw4x		vs14,	o32,	T1
	lxvw4x		vs15,	o48,	T1

	addi		BO,	BO,	128

.endm

.macro KERNEL8x4_I1


	lxvw4x		vs4,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1
	lxvw4x		vs17,	o16,	T1
	lxvw4x		vs18,	o32,	T1
	lxvw4x		vs19,	o48,	T1

	addi		T1,	T1,	64

	lxvw4x		vs20,	o0,	T1
	lxvw4x		vs21,	o16,	T1
	lxvw4x		vs22,	o32,	T1
	lxvw4x		vs23,	o48,	T1

	addi		BO,	BO,	128


	xvmulsp		vs32,	vs0,	vs8

	xvmulsp		vs33,	vs0,	vs9

	xvmulsp		vs34,	vs0,	vs10

	xvmulsp		vs35,	vs0,	vs11

	xvmulsp		vs36,	vs0,	vs12

	xvmulsp		vs37,	vs0,	vs13

	xvmulsp		vs38,	vs0,	vs14

	xvmulsp		vs39,	vs0,	vs15


.endm

.macro KERNEL8x4_1


	lxvw4x		vs4,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1
	lxvw4x		vs17,	o16,	T1
	lxvw4x		vs18,	o32,	T1
	lxvw4x		vs19,	o48,	T1

	addi		T1,	T1,	64

	lxvw4x		vs20,	o0,	T1
	lxvw4x		vs21,	o16,	T1
	lxvw4x		vs22,	o32,	T1
	lxvw4x		vs23,	o48,	T1

	addi		BO,	BO,	128


	xvmaddasp	vs32,	vs0,	vs8

	xvmaddasp	vs33,	vs0,	vs9

	xvmaddasp	vs34,	vs0,	vs10

	xvmaddasp	vs35,	vs0,	vs11

	xvmaddasp	vs36,	vs0,	vs12

	xvmaddasp	vs37,	vs0,	vs13

	xvmaddasp	vs38,	vs0,	vs14

	xvmaddasp	vs39,	vs0,	vs15


.endm

.macro KERNEL8x4_2


	lxvw4x		vs0,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		T1,	T1,	64

	lxvw4x		vs12,	o0,	T1
	lxvw4x		vs13,	o16,	T1
	lxvw4x		vs14,	o32,	T1
	lxvw4x		vs15,	o48,	T1

	addi		BO,	BO,	128


	xvmaddasp	vs32,	vs4,	vs16

	xvmaddasp	vs33,	vs4,	vs17

	xvmaddasp	vs34,	vs4,	vs18

	xvmaddasp	vs35,	vs4,	vs19

	xvmaddasp	vs36,	vs4,	vs20

	xvmaddasp	vs37,	vs4,	vs21

	xvmaddasp	vs38,	vs4,	vs22

	xvmaddasp	vs39,	vs4,	vs23


.endm

.macro KERNEL8x4_E2


	xvmaddasp	vs32,	vs4,	vs16

	xvmaddasp	vs33,	vs4,	vs17

	xvmaddasp	vs34,	vs4,	vs18

	xvmaddasp	vs35,	vs4,	vs19

	xvmaddasp	vs36,	vs4,	vs20

	xvmaddasp	vs37,	vs4,	vs21

	xvmaddasp	vs38,	vs4,	vs22

	xvmaddasp	vs39,	vs4,	vs23


.endm

.macro KERNEL8x4_SUBI1


	lxvw4x		vs0,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		T1,	T1,	64

	lxvw4x		vs12,	o0,	T1
	lxvw4x		vs13,	o16,	T1
	lxvw4x		vs14,	o32,	T1
	lxvw4x		vs15,	o48,	T1

	addi		BO,	BO,	128


	xvmulsp		vs32,	vs0,	vs8

	xvmulsp		vs33,	vs0,	vs9

	xvmulsp		vs34,	vs0,	vs10

	xvmulsp		vs35,	vs0,	vs11

	xvmulsp		vs36,	vs0,	vs12

	xvmulsp		vs37,	vs0,	vs13

	xvmulsp		vs38,	vs0,	vs14

	xvmulsp		vs39,	vs0,	vs15


.endm

.macro KERNEL8x4_SUB1


	lxvw4x		vs0,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		T1,	T1,	64

	lxvw4x		vs12,	o0,	T1
	lxvw4x		vs13,	o16,	T1
	lxvw4x		vs14,	o32,	T1
	lxvw4x		vs15,	o48,	T1

	addi		BO,	BO,	128


	xvmaddasp	vs32,	vs0,	vs8

	xvmaddasp	vs33,	vs0,	vs9

	xvmaddasp	vs34,	vs0,	vs10

	xvmaddasp	vs35,	vs0,	vs11

	xvmaddasp	vs36,	vs0,	vs12

	xvmaddasp	vs37,	vs0,	vs13

	xvmaddasp	vs38,	vs0,	vs14

	xvmaddasp	vs39,	vs0,	vs15


.endm

.macro SAVE8x4

	mr		T1,	CO

#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs32,	alpha_vr
#else
	xvmaddasp	vs0,	vs32,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs33,	alpha_vr
#else
	xvmaddasp	vs0,	vs33,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs34,	alpha_vr
#else
	xvmaddasp	vs0,	vs34,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs35,	alpha_vr
#else
	xvmaddasp	vs0,	vs35,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs36,	alpha_vr
#else
	xvmaddasp	vs0,	vs36,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs37,	alpha_vr
#else
	xvmaddasp	vs0,	vs37,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs38,	alpha_vr
#else
	xvmaddasp	vs0,	vs38,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs39,	alpha_vr
#else
	xvmaddasp	vs0,	vs39,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1

	add		T1,	T1,	LDC

	addi		CO,	CO,	16

.endm


/**********************************************************************************************
* Macros for N=8 and M=2
**********************************************************************************************/

.macro LOAD8x2_1

	lxsspx		vs0,	o0,	AO
	lxsspx		vs1,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1
	lxsspx		vs10,	o32,	T1
	lxsspx		vs11,	o48,	T1

	addi		T1,	T1,	64

	lxsspx		vs12,	o0,	T1
	lxsspx		vs13,	o16,	T1
	lxsspx		vs14,	o32,	T1
	lxsspx		vs15,	o48,	T1


	addi		BO,	BO,	128

.endm

.macro KERNEL8x2_I1


	lxsspx		vs4,	o0,	AO
	lxsspx		vs5,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs16,	o0,	T1
	lxsspx		vs17,	o16,	T1
	lxsspx		vs18,	o32,	T1
	lxsspx		vs19,	o48,	T1

	addi		T1,	T1,	64

	lxsspx		vs20,	o0,	T1
	lxsspx		vs21,	o16,	T1
	lxsspx		vs22,	o32,	T1
	lxsspx		vs23,	o48,	T1


	addi		BO,	BO,	128


	xsmuldp		vs32,	vs0,	vs8
	xsmuldp		vs33,	vs1,	vs8

	xsmuldp		vs34,	vs0,	vs9
	xsmuldp		vs35,	vs1,	vs9

	xsmuldp		vs36,	vs0,	vs10
	xsmuldp		vs37,	vs1,	vs10

	xsmuldp		vs38,	vs0,	vs11
	xsmuldp		vs39,	vs1,	vs11

	xsmuldp		vs40,	vs0,	vs12
	xsmuldp		vs41,	vs1,	vs12

	xsmuldp		vs42,	vs0,	vs13
	xsmuldp		vs43,	vs1,	vs13

	xsmuldp		vs44,	vs0,	vs14
	xsmuldp		vs45,	vs1,	vs14

	xsmuldp		vs46,	vs0,	vs15
	xsmuldp		vs47,	vs1,	vs15


.endm

.macro KERNEL8x2_1


	lxsspx		vs4,	o0,	AO
	lxsspx		vs5,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs16,	o0,	T1
	lxsspx		vs17,	o16,	T1
	lxsspx		vs18,	o32,	T1
	lxsspx		vs19,	o48,	T1

	addi		T1,	T1,	64

	lxsspx		vs20,	o0,	T1
	lxsspx		vs21,	o16,	T1
	lxsspx		vs22,	o32,	T1
	lxsspx		vs23,	o48,	T1


	addi		BO,	BO,	128


	xsmaddadp	vs32,	vs0,	vs8
	xsmaddadp	vs33,	vs1,	vs8

	xsmaddadp	vs34,	vs0,	vs9
	xsmaddadp	vs35,	vs1,	vs9

	xsmaddadp	vs36,	vs0,	vs10
	xsmaddadp	vs37,	vs1,	vs10

	xsmaddadp	vs38,	vs0,	vs11
	xsmaddadp	vs39,	vs1,	vs11

	xsmaddadp	vs40,	vs0,	vs12
	xsmaddadp	vs41,	vs1,	vs12

	xsmaddadp	vs42,	vs0,	vs13
	xsmaddadp	vs43,	vs1,	vs13

	xsmaddadp	vs44,	vs0,	vs14
	xsmaddadp	vs45,	vs1,	vs14

	xsmaddadp	vs46,	vs0,	vs15
	xsmaddadp	vs47,	vs1,	vs15


.endm

.macro KERNEL8x2_2


	lxsspx		vs0,	o0,	AO
	lxsspx		vs1,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1
	lxsspx		vs10,	o32,	T1
	lxsspx		vs11,	o48,	T1

	addi		T1,	T1,	64

	lxsspx		vs12,	o0,	T1
	lxsspx		vs13,	o16,	T1
	lxsspx		vs14,	o32,	T1
	lxsspx		vs15,	o48,	T1


	addi		BO,	BO,	128


	xsmaddadp	vs32,	vs4,	vs16
	xsmaddadp	vs33,	vs5,	vs16

	xsmaddadp	vs34,	vs4,	vs17
	xsmaddadp	vs35,	vs5,	vs17

	xsmaddadp	vs36,	vs4,	vs18
	xsmaddadp	vs37,	vs5,	vs18

	xsmaddadp	vs38,	vs4,	vs19
	xsmaddadp	vs39,	vs5,	vs19

	xsmaddadp	vs40,	vs4,	vs20
	xsmaddadp	vs41,	vs5,	vs20

	xsmaddadp	vs42,	vs4,	vs21
	xsmaddadp	vs43,	vs5,	vs21

	xsmaddadp	vs44,	vs4,	vs22
	xsmaddadp	vs45,	vs5,	vs22

	xsmaddadp	vs46,	vs4,	vs23
	xsmaddadp	vs47,	vs5,	vs23


.endm

.macro KERNEL8x2_E2


	xsmaddadp	vs32,	vs4,	vs16
	xsmaddadp	vs33,	vs5,	vs16

	xsmaddadp	vs34,	vs4,	vs17
	xsmaddadp	vs35,	vs5,	vs17

	xsmaddadp	vs36,	vs4,	vs18
	xsmaddadp	vs37,	vs5,	vs18

	xsmaddadp	vs38,	vs4,	vs19
	xsmaddadp	vs39,	vs5,	vs19

	xsmaddadp	vs40,	vs4,	vs20
	xsmaddadp	vs41,	vs5,	vs20

	xsmaddadp	vs42,	vs4,	vs21
	xsmaddadp	vs43,	vs5,	vs21

	xsmaddadp	vs44,	vs4,	vs22
	xsmaddadp	vs45,	vs5,	vs22

	xsmaddadp	vs46,	vs4,	vs23
	xsmaddadp	vs47,	vs5,	vs23


.endm

.macro KERNEL8x2_SUBI1


	lxsspx		vs0,	o0,	AO
	lxsspx		vs1,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1
	lxsspx		vs10,	o32,	T1
	lxsspx		vs11,	o48,	T1

	addi		T1,	T1,	64

	lxsspx		vs12,	o0,	T1
	lxsspx		vs13,	o16,	T1
	lxsspx		vs14,	o32,	T1
	lxsspx		vs15,	o48,	T1


	addi		BO,	BO,	128


	xsmuldp		vs32,	vs0,	vs8
	xsmuldp		vs33,	vs1,	vs8

	xsmuldp		vs34,	vs0,	vs9
	xsmuldp		vs35,	vs1,	vs9

	xsmuldp		vs36,	vs0,	vs10
	xsmuldp		vs37,	vs1,	vs10

	xsmuldp		vs38,	vs0,	vs11
	xsmuldp		vs39,	vs1,	vs11

	xsmuldp		vs40,	vs0,	vs12
	xsmuldp		vs41,	vs1,	vs12

	xsmuldp		vs42,	vs0,	vs13
	xsmuldp		vs43,	vs1,	vs13

	xsmuldp		vs44,	vs0,	vs14
	xsmuldp		vs45,	vs1,	vs14

	xsmuldp		vs46,	vs0,	vs15
	xsmuldp		vs47,	vs1,	vs15


.endm

.macro KERNEL8x2_SUB1


	lxsspx		vs0,	o0,	AO
	lxsspx		vs1,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1
	lxsspx		vs10,	o32,	T1
	lxsspx		vs11,	o48,	T1

	addi		T1,	T1,	64

	lxsspx		vs12,	o0,	T1
	lxsspx		vs13,	o16,	T1
	lxsspx		vs14,	o32,	T1
	lxsspx		vs15,	o48,	T1


	addi		BO,	BO,	128


	xsmaddadp	vs32,	vs0,	vs8
	xsmaddadp	vs33,	vs1,	vs8

	xsmaddadp	vs34,	vs0,	vs9
	xsmaddadp	vs35,	vs1,	vs9

	xsmaddadp	vs36,	vs0,	vs10
	xsmaddadp	vs37,	vs1,	vs10

	xsmaddadp	vs38,	vs0,	vs11
	xsmaddadp	vs39,	vs1,	vs11

	xsmaddadp	vs40,	vs0,	vs12
	xsmaddadp	vs41,	vs1,	vs12

	xsmaddadp	vs42,	vs0,	vs13
	xsmaddadp	vs43,	vs1,	vs13

	xsmaddadp	vs44,	vs0,	vs14
	xsmaddadp	vs45,	vs1,	vs14

	xsmaddadp	vs46,	vs0,	vs15
	xsmaddadp	vs47,	vs1,	vs15


.endm

.macro SAVE8x2

	mr		T1,	CO

#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1
	lxsspx		vs1,	o4,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs32,	alpha_r
	xsmuldp		vs1,	vs33,	alpha_r
#else
	xsmaddadp	vs0,	vs32,	alpha_r
	xsmaddadp	vs1,	vs33,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1
	stxsspx		vs1,	o4,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1
	lxsspx		vs1,	o4,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs34,	alpha_r
	xsmuldp		vs1,	vs35,	alpha_r
#else
	xsmaddadp	vs0,	vs34,	alpha_r
	xsmaddadp	vs1,	vs35,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1
	stxsspx		vs1,	o4,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1
	lxsspx		vs1,	o4,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs36,	alpha_r
	xsmuldp		vs1,	vs37,	alpha_r
#else
	xsmaddadp	vs0,	vs36,	alpha_r
	xsmaddadp	vs1,	vs37,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1
	stxsspx		vs1,	o4,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1
	lxsspx		vs1,	o4,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs38,	alpha_r
	xsmuldp		vs1,	vs39,	alpha_r
#else
	xsmaddadp	vs0,	vs38,	alpha_r
	xsmaddadp	vs1,	vs39,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1
	stxsspx		vs1,	o4,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1
	lxsspx		vs1,	o4,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs40,	alpha_r
	xsmuldp		vs1,	vs41,	alpha_r
#else
	xsmaddadp	vs0,	vs40,	alpha_r
	xsmaddadp	vs1,	vs41,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1
	stxsspx		vs1,	o4,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1
	lxsspx		vs1,	o4,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs42,	alpha_r
	xsmuldp		vs1,	vs43,	alpha_r
#else
	xsmaddadp	vs0,	vs42,	alpha_r
	xsmaddadp	vs1,	vs43,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1
	stxsspx		vs1,	o4,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1
	lxsspx		vs1,	o4,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs44,	alpha_r
	xsmuldp		vs1,	vs45,	alpha_r
#else
	xsmaddadp	vs0,	vs44,	alpha_r
	xsmaddadp	vs1,	vs45,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1
	stxsspx		vs1,	o4,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1
	lxsspx		vs1,	o4,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs46,	alpha_r
	xsmuldp		vs1,	vs47,	alpha_r
#else
	xsmaddadp	vs0,	vs46,	alpha_r
	xsmaddadp	vs1,	vs47,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1
	stxsspx		vs1,	o4,	T1

	add		T1,	T1,	LDC

	addi		CO,	CO,	8

.endm


/**********************************************************************************************
* Macros for N=8 and M=1
**********************************************************************************************/

.macro LOAD8x1_1

	lxsspx		vs0,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1
	lxsspx		vs10,	o32,	T1
	lxsspx		vs11,	o48,	T1

	addi		T1,	T1,	64

	lxsspx		vs12,	o0,	T1
	lxsspx		vs13,	o16,	T1
	lxsspx		vs14,	o32,	T1
	lxsspx		vs15,	o48,	T1


	addi		BO,	BO,	128

.endm

.macro KERNEL8x1_I1


	lxsspx		vs4,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs16,	o0,	T1
	lxsspx		vs17,	o16,	T1
	lxsspx		vs18,	o32,	T1
	lxsspx		vs19,	o48,	T1

	addi		T1,	T1,	64

	lxsspx		vs20,	o0,	T1
	lxsspx		vs21,	o16,	T1
	lxsspx		vs22,	o32,	T1
	lxsspx		vs23,	o48,	T1


	addi		BO,	BO,	128


	xsmuldp		vs32,	vs0,	vs8

	xsmuldp		vs33,	vs0,	vs9

	xsmuldp		vs34,	vs0,	vs10

	xsmuldp		vs35,	vs0,	vs11

	xsmuldp		vs36,	vs0,	vs12

	xsmuldp		vs37,	vs0,	vs13

	xsmuldp		vs38,	vs0,	vs14

	xsmuldp		vs39,	vs0,	vs15


.endm

.macro KERNEL8x1_1


	lxsspx		vs4,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs16,	o0,	T1
	lxsspx		vs17,	o16,	T1
	lxsspx		vs18,	o32,	T1
	lxsspx		vs19,	o48,	T1

	addi		T1,	T1,	64

	lxsspx		vs20,	o0,	T1
	lxsspx		vs21,	o16,	T1
	lxsspx		vs22,	o32,	T1
	lxsspx		vs23,	o48,	T1


	addi		BO,	BO,	128


	xsmaddadp	vs32,	vs0,	vs8

	xsmaddadp	vs33,	vs0,	vs9

	xsmaddadp	vs34,	vs0,	vs10

	xsmaddadp	vs35,	vs0,	vs11

	xsmaddadp	vs36,	vs0,	vs12

	xsmaddadp	vs37,	vs0,	vs13

	xsmaddadp	vs38,	vs0,	vs14

	xsmaddadp	vs39,	vs0,	vs15


.endm

.macro KERNEL8x1_2


	lxsspx		vs0,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1
	lxsspx		vs10,	o32,	T1
	lxsspx		vs11,	o48,	T1

	addi		T1,	T1,	64

	lxsspx		vs12,	o0,	T1
	lxsspx		vs13,	o16,	T1
	lxsspx		vs14,	o32,	T1
	lxsspx		vs15,	o48,	T1


	addi		BO,	BO,	128


	xsmaddadp	vs32,	vs4,	vs16

	xsmaddadp	vs33,	vs4,	vs17

	xsmaddadp	vs34,	vs4,	vs18

	xsmaddadp	vs35,	vs4,	vs19

	xsmaddadp	vs36,	vs4,	vs20

	xsmaddadp	vs37,	vs4,	vs21

	xsmaddadp	vs38,	vs4,	vs22

	xsmaddadp	vs39,	vs4,	vs23


.endm

.macro KERNEL8x1_E2


	xsmaddadp	vs32,	vs4,	vs16

	xsmaddadp	vs33,	vs4,	vs17

	xsmaddadp	vs34,	vs4,	vs18

	xsmaddadp	vs35,	vs4,	vs19

	xsmaddadp	vs36,	vs4,	vs20

	xsmaddadp	vs37,	vs4,	vs21

	xsmaddadp	vs38,	vs4,	vs22

	xsmaddadp	vs39,	vs4,	vs23


.endm

.macro KERNEL8x1_SUBI1


	lxsspx		vs0,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1
	lxsspx		vs10,	o32,	T1
	lxsspx		vs11,	o48,	T1

	addi		T1,	T1,	64

	lxsspx		vs12,	o0,	T1
	lxsspx		vs13,	o16,	T1
	lxsspx		vs14,	o32,	T1
	lxsspx		vs15,	o48,	T1


	addi		BO,	BO,	128


	xsmuldp		vs32,	vs0,	vs8

	xsmuldp		vs33,	vs0,	vs9

	xsmuldp		vs34,	vs0,	vs10

	xsmuldp		vs35,	vs0,	vs11

	xsmuldp		vs36,	vs0,	vs12

	xsmuldp		vs37,	vs0,	vs13

	xsmuldp		vs38,	vs0,	vs14

	xsmuldp		vs39,	vs0,	vs15


.endm

.macro KERNEL8x1_SUB1


	lxsspx		vs0,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1
	lxsspx		vs10,	o32,	T1
	lxsspx		vs11,	o48,	T1

	addi		T1,	T1,	64

	lxsspx		vs12,	o0,	T1
	lxsspx		vs13,	o16,	T1
	lxsspx		vs14,	o32,	T1
	lxsspx		vs15,	o48,	T1


	addi		BO,	BO,	128


	xsmaddadp	vs32,	vs0,	vs8

	xsmaddadp	vs33,	vs0,	vs9

	xsmaddadp	vs34,	vs0,	vs10

	xsmaddadp	vs35,	vs0,	vs11

	xsmaddadp	vs36,	vs0,	vs12

	xsmaddadp	vs37,	vs0,	vs13

	xsmaddadp	vs38,	vs0,	vs14

	xsmaddadp	vs39,	vs0,	vs15


.endm

.macro SAVE8x1

	mr		T1,	CO

#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs32,	alpha_r
#else
	xsmaddadp	vs0,	vs32,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs33,	alpha_r
#else
	xsmaddadp	vs0,	vs33,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs34,	alpha_r
#else
	xsmaddadp	vs0,	vs34,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs35,	alpha_r
#else
	xsmaddadp	vs0,	vs35,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs36,	alpha_r
#else
	xsmaddadp	vs0,	vs36,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs37,	alpha_r
#else
	xsmaddadp	vs0,	vs37,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs38,	alpha_r
#else
	xsmaddadp	vs0,	vs38,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs39,	alpha_r
#else
	xsmaddadp	vs0,	vs39,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1

	add		T1,	T1,	LDC

	addi		CO,	CO,	4

.endm


/**********************************************************************************************
* Macros for N=4 and M=16
**********************************************************************************************/

.macro LOAD4x16_1

	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO
	lxvw4x		vs2,	o32,	AO
	lxvw4x		vs3,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		BO,	BO,	64

.endm

.macro KERNEL4x16_I1


	lxvw4x		vs4,	o0,	AO
	lxvw4x		vs5,	o16,	AO
	lxvw4x		vs6,	o32,	AO
	lxvw4x		vs7,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1
	lxvw4x		vs17,	o16,	T1
	lxvw4x		vs18,	o32,	T1
	lxvw4x		vs19,	o48,	T1

	addi		BO,	BO,	64


	xvmulsp		vs32,	vs0,	vs8
	xvmulsp		vs33,	vs1,	vs8
	xvmulsp		vs34,	vs2,	vs8
	xvmulsp		vs35,	vs3,	vs8

	xvmulsp		vs36,	vs0,	vs9
	xvmulsp		vs37,	vs1,	vs9
	xvmulsp		vs38,	vs2,	vs9
	xvmulsp		vs39,	vs3,	vs9

	xvmulsp		vs40,	vs0,	vs10
	xvmulsp		vs41,	vs1,	vs10
	xvmulsp		vs42,	vs2,	vs10
	xvmulsp		vs43,	vs3,	vs10

	xvmulsp		vs44,	vs0,	vs11
	xvmulsp		vs45,	vs1,	vs11
	xvmulsp		vs46,	vs2,	vs11
	xvmulsp		vs47,	vs3,	vs11


.endm

.macro KERNEL4x16_1


	lxvw4x		vs4,	o0,	AO
	lxvw4x		vs5,	o16,	AO
	lxvw4x		vs6,	o32,	AO
	lxvw4x		vs7,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1
	lxvw4x		vs17,	o16,	T1
	lxvw4x		vs18,	o32,	T1
	lxvw4x		vs19,	o48,	T1

	addi		BO,	BO,	64


	xvmaddasp	vs32,	vs0,	vs8
	xvmaddasp	vs33,	vs1,	vs8
	xvmaddasp	vs34,	vs2,	vs8
	xvmaddasp	vs35,	vs3,	vs8

	xvmaddasp	vs36,	vs0,	vs9
	xvmaddasp	vs37,	vs1,	vs9
	xvmaddasp	vs38,	vs2,	vs9
	xvmaddasp	vs39,	vs3,	vs9

	xvmaddasp	vs40,	vs0,	vs10
	xvmaddasp	vs41,	vs1,	vs10
	xvmaddasp	vs42,	vs2,	vs10
	xvmaddasp	vs43,	vs3,	vs10

	xvmaddasp	vs44,	vs0,	vs11
	xvmaddasp	vs45,	vs1,	vs11
	xvmaddasp	vs46,	vs2,	vs11
	xvmaddasp	vs47,	vs3,	vs11


.endm

.macro KERNEL4x16_2


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO
	lxvw4x		vs2,	o32,	AO
	lxvw4x		vs3,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		BO,	BO,	64


	xvmaddasp	vs32,	vs4,	vs16
	xvmaddasp	vs33,	vs5,	vs16
	xvmaddasp	vs34,	vs6,	vs16
	xvmaddasp	vs35,	vs7,	vs16

	xvmaddasp	vs36,	vs4,	vs17
	xvmaddasp	vs37,	vs5,	vs17
	xvmaddasp	vs38,	vs6,	vs17
	xvmaddasp	vs39,	vs7,	vs17

	xvmaddasp	vs40,	vs4,	vs18
	xvmaddasp	vs41,	vs5,	vs18
	xvmaddasp	vs42,	vs6,	vs18
	xvmaddasp	vs43,	vs7,	vs18

	xvmaddasp	vs44,	vs4,	vs19
	xvmaddasp	vs45,	vs5,	vs19
	xvmaddasp	vs46,	vs6,	vs19
	xvmaddasp	vs47,	vs7,	vs19


.endm

.macro KERNEL4x16_E2


	xvmaddasp	vs32,	vs4,	vs16
	xvmaddasp	vs33,	vs5,	vs16
	xvmaddasp	vs34,	vs6,	vs16
	xvmaddasp	vs35,	vs7,	vs16

	xvmaddasp	vs36,	vs4,	vs17
	xvmaddasp	vs37,	vs5,	vs17
	xvmaddasp	vs38,	vs6,	vs17
	xvmaddasp	vs39,	vs7,	vs17

	xvmaddasp	vs40,	vs4,	vs18
	xvmaddasp	vs41,	vs5,	vs18
	xvmaddasp	vs42,	vs6,	vs18
	xvmaddasp	vs43,	vs7,	vs18

	xvmaddasp	vs44,	vs4,	vs19
	xvmaddasp	vs45,	vs5,	vs19
	xvmaddasp	vs46,	vs6,	vs19
	xvmaddasp	vs47,	vs7,	vs19


.endm

.macro KERNEL4x16_SUBI1


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO
	lxvw4x		vs2,	o32,	AO
	lxvw4x		vs3,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		BO,	BO,	64


	xvmulsp		vs32,	vs0,	vs8
	xvmulsp		vs33,	vs1,	vs8
	xvmulsp		vs34,	vs2,	vs8
	xvmulsp		vs35,	vs3,	vs8

	xvmulsp		vs36,	vs0,	vs9
	xvmulsp		vs37,	vs1,	vs9
	xvmulsp		vs38,	vs2,	vs9
	xvmulsp		vs39,	vs3,	vs9

	xvmulsp		vs40,	vs0,	vs10
	xvmulsp		vs41,	vs1,	vs10
	xvmulsp		vs42,	vs2,	vs10
	xvmulsp		vs43,	vs3,	vs10

	xvmulsp		vs44,	vs0,	vs11
	xvmulsp		vs45,	vs1,	vs11
	xvmulsp		vs46,	vs2,	vs11
	xvmulsp		vs47,	vs3,	vs11


.endm

.macro KERNEL4x16_SUB1


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO
	lxvw4x		vs2,	o32,	AO
	lxvw4x		vs3,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		BO,	BO,	64


	xvmaddasp	vs32,	vs0,	vs8
	xvmaddasp	vs33,	vs1,	vs8
	xvmaddasp	vs34,	vs2,	vs8
	xvmaddasp	vs35,	vs3,	vs8

	xvmaddasp	vs36,	vs0,	vs9
	xvmaddasp	vs37,	vs1,	vs9
	xvmaddasp	vs38,	vs2,	vs9
	xvmaddasp	vs39,	vs3,	vs9

	xvmaddasp	vs40,	vs0,	vs10
	xvmaddasp	vs41,	vs1,	vs10
	xvmaddasp	vs42,	vs2,	vs10
	xvmaddasp	vs43,	vs3,	vs10

	xvmaddasp	vs44,	vs0,	vs11
	xvmaddasp	vs45,	vs1,	vs11
	xvmaddasp	vs46,	vs2,	vs11
	xvmaddasp	vs47,	vs3,	vs11


.endm

.macro SAVE4x16

	mr		T1,	CO

#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1
	lxvw4x		vs2,	o32,	T1
	lxvw4x		vs3,	o48,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs32,	alpha_vr
	xvmulsp		vs1,	vs33,	alpha_vr
	xvmulsp		vs2,	vs34,	alpha_vr
	xvmulsp		vs3,	vs35,	alpha_vr
#else
	xvmaddasp	vs0,	vs32,	alpha_vr
	xvmaddasp	vs1,	vs33,	alpha_vr
	xvmaddasp	vs2,	vs34,	alpha_vr
	xvmaddasp	vs3,	vs35,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1
	stxvw4x		vs2,	o32,	T1
	stxvw4x		vs3,	o48,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1
	lxvw4x		vs2,	o32,	T1
	lxvw4x		vs3,	o48,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs36,	alpha_vr
	xvmulsp		vs1,	vs37,	alpha_vr
	xvmulsp		vs2,	vs38,	alpha_vr
	xvmulsp		vs3,	vs39,	alpha_vr
#else
	xvmaddasp	vs0,	vs36,	alpha_vr
	xvmaddasp	vs1,	vs37,	alpha_vr
	xvmaddasp	vs2,	vs38,	alpha_vr
	xvmaddasp	vs3,	vs39,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1
	stxvw4x		vs2,	o32,	T1
	stxvw4x		vs3,	o48,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1
	lxvw4x		vs2,	o32,	T1
	lxvw4x		vs3,	o48,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs40,	alpha_vr
	xvmulsp		vs1,	vs41,	alpha_vr
	xvmulsp		vs2,	vs42,	alpha_vr
	xvmulsp		vs3,	vs43,	alpha_vr
#else
	xvmaddasp	vs0,	vs40,	alpha_vr
	xvmaddasp	vs1,	vs41,	alpha_vr
	xvmaddasp	vs2,	vs42,	alpha_vr
	xvmaddasp	vs3,	vs43,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1
	stxvw4x		vs2,	o32,	T1
	stxvw4x		vs3,	o48,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1
	lxvw4x		vs2,	o32,	T1
	lxvw4x		vs3,	o48,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs44,	alpha_vr
	xvmulsp		vs1,	vs45,	alpha_vr
	xvmulsp		vs2,	vs46,	alpha_vr
	xvmulsp		vs3,	vs47,	alpha_vr
#else
	xvmaddasp	vs0,	vs44,	alpha_vr
	xvmaddasp	vs1,	vs45,	alpha_vr
	xvmaddasp	vs2,	vs46,	alpha_vr
	xvmaddasp	vs3,	vs47,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1
	stxvw4x		vs2,	o32,	T1
	stxvw4x		vs3,	o48,	T1

	add		T1,	T1,	LDC

	addi		CO,	CO,	64

.endm


/**********************************************************************************************
* Macros for N=4 and M=8
**********************************************************************************************/

.macro LOAD4x8_1

	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		BO,	BO,	64

.endm

.macro KERNEL4x8_I1


	lxvw4x		vs4,	o0,	AO
	lxvw4x		vs5,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1
	lxvw4x		vs17,	o16,	T1
	lxvw4x		vs18,	o32,	T1
	lxvw4x		vs19,	o48,	T1

	addi		BO,	BO,	64


	xvmulsp		vs32,	vs0,	vs8
	xvmulsp		vs33,	vs1,	vs8

	xvmulsp		vs34,	vs0,	vs9
	xvmulsp		vs35,	vs1,	vs9

	xvmulsp		vs36,	vs0,	vs10
	xvmulsp		vs37,	vs1,	vs10

	xvmulsp		vs38,	vs0,	vs11
	xvmulsp		vs39,	vs1,	vs11


.endm

.macro KERNEL4x8_1


	lxvw4x		vs4,	o0,	AO
	lxvw4x		vs5,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1
	lxvw4x		vs17,	o16,	T1
	lxvw4x		vs18,	o32,	T1
	lxvw4x		vs19,	o48,	T1

	addi		BO,	BO,	64


	xvmaddasp	vs32,	vs0,	vs8
	xvmaddasp	vs33,	vs1,	vs8

	xvmaddasp	vs34,	vs0,	vs9
	xvmaddasp	vs35,	vs1,	vs9

	xvmaddasp	vs36,	vs0,	vs10
	xvmaddasp	vs37,	vs1,	vs10

	xvmaddasp	vs38,	vs0,	vs11
	xvmaddasp	vs39,	vs1,	vs11


.endm

.macro KERNEL4x8_2


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		BO,	BO,	64


	xvmaddasp	vs32,	vs4,	vs16
	xvmaddasp	vs33,	vs5,	vs16

	xvmaddasp	vs34,	vs4,	vs17
	xvmaddasp	vs35,	vs5,	vs17

	xvmaddasp	vs36,	vs4,	vs18
	xvmaddasp	vs37,	vs5,	vs18

	xvmaddasp	vs38,	vs4,	vs19
	xvmaddasp	vs39,	vs5,	vs19


.endm

.macro KERNEL4x8_E2


	xvmaddasp	vs32,	vs4,	vs16
	xvmaddasp	vs33,	vs5,	vs16

	xvmaddasp	vs34,	vs4,	vs17
	xvmaddasp	vs35,	vs5,	vs17

	xvmaddasp	vs36,	vs4,	vs18
	xvmaddasp	vs37,	vs5,	vs18

	xvmaddasp	vs38,	vs4,	vs19
	xvmaddasp	vs39,	vs5,	vs19


.endm

.macro KERNEL4x8_SUBI1


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		BO,	BO,	64


	xvmulsp		vs32,	vs0,	vs8
	xvmulsp		vs33,	vs1,	vs8

	xvmulsp		vs34,	vs0,	vs9
	xvmulsp		vs35,	vs1,	vs9

	xvmulsp		vs36,	vs0,	vs10
	xvmulsp		vs37,	vs1,	vs10

	xvmulsp		vs38,	vs0,	vs11
	xvmulsp		vs39,	vs1,	vs11


.endm

.macro KERNEL4x8_SUB1


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		BO,	BO,	64


	xvmaddasp	vs32,	vs0,	vs8
	xvmaddasp	vs33,	vs1,	vs8

	xvmaddasp	vs34,	vs0,	vs9
	xvmaddasp	vs35,	vs1,	vs9

	xvmaddasp	vs36,	vs0,	vs10
	xvmaddasp	vs37,	vs1,	vs10

	xvmaddasp	vs38,	vs0,	vs11
	xvmaddasp	vs39,	vs1,	vs11


.endm

.macro SAVE4x8

	mr		T1,	CO

#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs32,	alpha_vr
	xvmulsp		vs1,	vs33,	alpha_vr
#else
	xvmaddasp	vs0,	vs32,	alpha_vr
	xvmaddasp	vs1,	vs33,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs34,	alpha_vr
	xvmulsp		vs1,	vs35,	alpha_vr
#else
	xvmaddasp	vs0,	vs34,	alpha_vr
	xvmaddasp	vs1,	vs35,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs36,	alpha_vr
	xvmulsp		vs1,	vs37,	alpha_vr
#else
	xvmaddasp	vs0,	vs36,	alpha_vr
	xvmaddasp	vs1,	vs37,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs38,	alpha_vr
	xvmulsp		vs1,	vs39,	alpha_vr
#else
	xvmaddasp	vs0,	vs38,	alpha_vr
	xvmaddasp	vs1,	vs39,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1

	add		T1,	T1,	LDC

	addi		CO,	CO,	32

.endm


/**********************************************************************************************
* Macros for N=4 and M=4
**********************************************************************************************/

.macro LOAD4x4_1

	lxvw4x		vs0,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		BO,	BO,	64

.endm

.macro KERNEL4x4_I1


	lxvw4x		vs4,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1
	lxvw4x		vs17,	o16,	T1
	lxvw4x		vs18,	o32,	T1
	lxvw4x		vs19,	o48,	T1

	addi		BO,	BO,	64


	xvmulsp		vs32,	vs0,	vs8

	xvmulsp		vs33,	vs0,	vs9

	xvmulsp		vs34,	vs0,	vs10

	xvmulsp		vs35,	vs0,	vs11


.endm

.macro KERNEL4x4_1


	lxvw4x		vs4,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1
	lxvw4x		vs17,	o16,	T1
	lxvw4x		vs18,	o32,	T1
	lxvw4x		vs19,	o48,	T1

	addi		BO,	BO,	64


	xvmaddasp	vs32,	vs0,	vs8

	xvmaddasp	vs33,	vs0,	vs9

	xvmaddasp	vs34,	vs0,	vs10

	xvmaddasp	vs35,	vs0,	vs11


.endm

.macro KERNEL4x4_2


	lxvw4x		vs0,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		BO,	BO,	64


	xvmaddasp	vs32,	vs4,	vs16

	xvmaddasp	vs33,	vs4,	vs17

	xvmaddasp	vs34,	vs4,	vs18

	xvmaddasp	vs35,	vs4,	vs19


.endm

.macro KERNEL4x4_E2


	xvmaddasp	vs32,	vs4,	vs16

	xvmaddasp	vs33,	vs4,	vs17

	xvmaddasp	vs34,	vs4,	vs18

	xvmaddasp	vs35,	vs4,	vs19


.endm

.macro KERNEL4x4_SUBI1


	lxvw4x		vs0,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		BO,	BO,	64


	xvmulsp		vs32,	vs0,	vs8

	xvmulsp		vs33,	vs0,	vs9

	xvmulsp		vs34,	vs0,	vs10

	xvmulsp		vs35,	vs0,	vs11


.endm

.macro KERNEL4x4_SUB1


	lxvw4x		vs0,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1
	lxvw4x		vs10,	o32,	T1
	lxvw4x		vs11,	o48,	T1

	addi		BO,	BO,	64


	xvmaddasp	vs32,	vs0,	vs8

	xvmaddasp	vs33,	vs0,	vs9

	xvmaddasp	vs34,	vs0,	vs10

	xvmaddasp	vs35,	vs0,	vs11


.endm

.macro SAVE4x4

	mr		T1,	CO

#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs32,	alpha_vr
#else
	xvmaddasp	vs0,	vs32,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs33,	alpha_vr
#else
	xvmaddasp	vs0,	vs33,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs34,	alpha_vr
#else
	xvmaddasp	vs0,	vs34,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs35,	alpha_vr
#else
	xvmaddasp	vs0,	vs35,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1

	add		T1,	T1,	LDC

	addi		CO,	CO,	16

.endm


/**********************************************************************************************
* Macros for N=4 and M=2
**********************************************************************************************/

.macro LOAD4x2_1

	lxsspx		vs0,	o0,	AO
	lxsspx		vs1,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1
	lxsspx		vs10,	o32,	T1
	lxsspx		vs11,	o48,	T1


	addi		BO,	BO,	64

.endm

.macro KERNEL4x2_I1


	lxsspx		vs4,	o0,	AO
	lxsspx		vs5,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs16,	o0,	T1
	lxsspx		vs17,	o16,	T1
	lxsspx		vs18,	o32,	T1
	lxsspx		vs19,	o48,	T1


	addi		BO,	BO,	64


	xsmuldp		vs32,	vs0,	vs8
	xsmuldp		vs33,	vs1,	vs8

	xsmuldp		vs34,	vs0,	vs9
	xsmuldp		vs35,	vs1,	vs9

	xsmuldp		vs36,	vs0,	vs10
	xsmuldp		vs37,	vs1,	vs10

	xsmuldp		vs38,	vs0,	vs11
	xsmuldp		vs39,	vs1,	vs11


.endm

.macro KERNEL4x2_1


	lxsspx		vs4,	o0,	AO
	lxsspx		vs5,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs16,	o0,	T1
	lxsspx		vs17,	o16,	T1
	lxsspx		vs18,	o32,	T1
	lxsspx		vs19,	o48,	T1


	addi		BO,	BO,	64


	xsmaddadp	vs32,	vs0,	vs8
	xsmaddadp	vs33,	vs1,	vs8

	xsmaddadp	vs34,	vs0,	vs9
	xsmaddadp	vs35,	vs1,	vs9

	xsmaddadp	vs36,	vs0,	vs10
	xsmaddadp	vs37,	vs1,	vs10

	xsmaddadp	vs38,	vs0,	vs11
	xsmaddadp	vs39,	vs1,	vs11


.endm

.macro KERNEL4x2_2


	lxsspx		vs0,	o0,	AO
	lxsspx		vs1,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1
	lxsspx		vs10,	o32,	T1
	lxsspx		vs11,	o48,	T1


	addi		BO,	BO,	64


	xsmaddadp	vs32,	vs4,	vs16
	xsmaddadp	vs33,	vs5,	vs16

	xsmaddadp	vs34,	vs4,	vs17
	xsmaddadp	vs35,	vs5,	vs17

	xsmaddadp	vs36,	vs4,	vs18
	xsmaddadp	vs37,	vs5,	vs18

	xsmaddadp	vs38,	vs4,	vs19
	xsmaddadp	vs39,	vs5,	vs19


.endm

.macro KERNEL4x2_E2


	xsmaddadp	vs32,	vs4,	vs16
	xsmaddadp	vs33,	vs5,	vs16

	xsmaddadp	vs34,	vs4,	vs17
	xsmaddadp	vs35,	vs5,	vs17

	xsmaddadp	vs36,	vs4,	vs18
	xsmaddadp	vs37,	vs5,	vs18

	xsmaddadp	vs38,	vs4,	vs19
	xsmaddadp	vs39,	vs5,	vs19


.endm

.macro KERNEL4x2_SUBI1


	lxsspx		vs0,	o0,	AO
	lxsspx		vs1,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1
	lxsspx		vs10,	o32,	T1
	lxsspx		vs11,	o48,	T1


	addi		BO,	BO,	64


	xsmuldp		vs32,	vs0,	vs8
	xsmuldp		vs33,	vs1,	vs8

	xsmuldp		vs34,	vs0,	vs9
	xsmuldp		vs35,	vs1,	vs9

	xsmuldp		vs36,	vs0,	vs10
	xsmuldp		vs37,	vs1,	vs10

	xsmuldp		vs38,	vs0,	vs11
	xsmuldp		vs39,	vs1,	vs11


.endm

.macro KERNEL4x2_SUB1


	lxsspx		vs0,	o0,	AO
	lxsspx		vs1,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1
	lxsspx		vs10,	o32,	T1
	lxsspx		vs11,	o48,	T1


	addi		BO,	BO,	64


	xsmaddadp	vs32,	vs0,	vs8
	xsmaddadp	vs33,	vs1,	vs8

	xsmaddadp	vs34,	vs0,	vs9
	xsmaddadp	vs35,	vs1,	vs9

	xsmaddadp	vs36,	vs0,	vs10
	xsmaddadp	vs37,	vs1,	vs10

	xsmaddadp	vs38,	vs0,	vs11
	xsmaddadp	vs39,	vs1,	vs11


.endm

.macro SAVE4x2

	mr		T1,	CO

#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1
	lxsspx		vs1,	o4,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs32,	alpha_r
	xsmuldp		vs1,	vs33,	alpha_r
#else
	xsmaddadp	vs0,	vs32,	alpha_r
	xsmaddadp	vs1,	vs33,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1
	stxsspx		vs1,	o4,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1
	lxsspx		vs1,	o4,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs34,	alpha_r
	xsmuldp		vs1,	vs35,	alpha_r
#else
	xsmaddadp	vs0,	vs34,	alpha_r
	xsmaddadp	vs1,	vs35,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1
	stxsspx		vs1,	o4,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1
	lxsspx		vs1,	o4,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs36,	alpha_r
	xsmuldp		vs1,	vs37,	alpha_r
#else
	xsmaddadp	vs0,	vs36,	alpha_r
	xsmaddadp	vs1,	vs37,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1
	stxsspx		vs1,	o4,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1
	lxsspx		vs1,	o4,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs38,	alpha_r
	xsmuldp		vs1,	vs39,	alpha_r
#else
	xsmaddadp	vs0,	vs38,	alpha_r
	xsmaddadp	vs1,	vs39,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1
	stxsspx		vs1,	o4,	T1

	add		T1,	T1,	LDC

	addi		CO,	CO,	8

.endm


/**********************************************************************************************
* Macros for N=4 and M=1
**********************************************************************************************/

.macro LOAD4x1_1

	lxsspx		vs0,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1
	lxsspx		vs10,	o32,	T1
	lxsspx		vs11,	o48,	T1


	addi		BO,	BO,	64

.endm

.macro KERNEL4x1_I1


	lxsspx		vs4,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs16,	o0,	T1
	lxsspx		vs17,	o16,	T1
	lxsspx		vs18,	o32,	T1
	lxsspx		vs19,	o48,	T1


	addi		BO,	BO,	64


	xsmuldp		vs32,	vs0,	vs8

	xsmuldp		vs33,	vs0,	vs9

	xsmuldp		vs34,	vs0,	vs10

	xsmuldp		vs35,	vs0,	vs11


.endm

.macro KERNEL4x1_1


	lxsspx		vs4,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs16,	o0,	T1
	lxsspx		vs17,	o16,	T1
	lxsspx		vs18,	o32,	T1
	lxsspx		vs19,	o48,	T1


	addi		BO,	BO,	64


	xsmaddadp	vs32,	vs0,	vs8

	xsmaddadp	vs33,	vs0,	vs9

	xsmaddadp	vs34,	vs0,	vs10

	xsmaddadp	vs35,	vs0,	vs11


.endm

.macro KERNEL4x1_2


	lxsspx		vs0,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1
	lxsspx		vs10,	o32,	T1
	lxsspx		vs11,	o48,	T1


	addi		BO,	BO,	64


	xsmaddadp	vs32,	vs4,	vs16

	xsmaddadp	vs33,	vs4,	vs17

	xsmaddadp	vs34,	vs4,	vs18

	xsmaddadp	vs35,	vs4,	vs19


.endm

.macro KERNEL4x1_E2


	xsmaddadp	vs32,	vs4,	vs16

	xsmaddadp	vs33,	vs4,	vs17

	xsmaddadp	vs34,	vs4,	vs18

	xsmaddadp	vs35,	vs4,	vs19


.endm

.macro KERNEL4x1_SUBI1


	lxsspx		vs0,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1
	lxsspx		vs10,	o32,	T1
	lxsspx		vs11,	o48,	T1


	addi		BO,	BO,	64


	xsmuldp		vs32,	vs0,	vs8

	xsmuldp		vs33,	vs0,	vs9

	xsmuldp		vs34,	vs0,	vs10

	xsmuldp		vs35,	vs0,	vs11


.endm

.macro KERNEL4x1_SUB1


	lxsspx		vs0,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1
	lxsspx		vs10,	o32,	T1
	lxsspx		vs11,	o48,	T1


	addi		BO,	BO,	64


	xsmaddadp	vs32,	vs0,	vs8

	xsmaddadp	vs33,	vs0,	vs9

	xsmaddadp	vs34,	vs0,	vs10

	xsmaddadp	vs35,	vs0,	vs11


.endm

.macro SAVE4x1

	mr		T1,	CO

#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs32,	alpha_r
#else
	xsmaddadp	vs0,	vs32,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs33,	alpha_r
#else
	xsmaddadp	vs0,	vs33,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs34,	alpha_r
#else
	xsmaddadp	vs0,	vs34,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs35,	alpha_r
#else
	xsmaddadp	vs0,	vs35,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1

	add		T1,	T1,	LDC

	addi		CO,	CO,	4

.endm


/**********************************************************************************************
* Macros for N=2 and M=16
**********************************************************************************************/

.macro LOAD2x16_1

	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO
	lxvw4x		vs2,	o32,	AO
	lxvw4x		vs3,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1

	addi		BO,	BO,	32

.endm

.macro KERNEL2x16_I1


	lxvw4x		vs4,	o0,	AO
	lxvw4x		vs5,	o16,	AO
	lxvw4x		vs6,	o32,	AO
	lxvw4x		vs7,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1
	lxvw4x		vs17,	o16,	T1

	addi		BO,	BO,	32


	xvmulsp		vs32,	vs0,	vs8
	xvmulsp		vs33,	vs1,	vs8
	xvmulsp		vs34,	vs2,	vs8
	xvmulsp		vs35,	vs3,	vs8

	xvmulsp		vs36,	vs0,	vs9
	xvmulsp		vs37,	vs1,	vs9
	xvmulsp		vs38,	vs2,	vs9
	xvmulsp		vs39,	vs3,	vs9


.endm

.macro KERNEL2x16_1


	lxvw4x		vs4,	o0,	AO
	lxvw4x		vs5,	o16,	AO
	lxvw4x		vs6,	o32,	AO
	lxvw4x		vs7,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1
	lxvw4x		vs17,	o16,	T1

	addi		BO,	BO,	32


	xvmaddasp	vs32,	vs0,	vs8
	xvmaddasp	vs33,	vs1,	vs8
	xvmaddasp	vs34,	vs2,	vs8
	xvmaddasp	vs35,	vs3,	vs8

	xvmaddasp	vs36,	vs0,	vs9
	xvmaddasp	vs37,	vs1,	vs9
	xvmaddasp	vs38,	vs2,	vs9
	xvmaddasp	vs39,	vs3,	vs9


.endm

.macro KERNEL2x16_2


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO
	lxvw4x		vs2,	o32,	AO
	lxvw4x		vs3,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1

	addi		BO,	BO,	32


	xvmaddasp	vs32,	vs4,	vs16
	xvmaddasp	vs33,	vs5,	vs16
	xvmaddasp	vs34,	vs6,	vs16
	xvmaddasp	vs35,	vs7,	vs16

	xvmaddasp	vs36,	vs4,	vs17
	xvmaddasp	vs37,	vs5,	vs17
	xvmaddasp	vs38,	vs6,	vs17
	xvmaddasp	vs39,	vs7,	vs17


.endm

.macro KERNEL2x16_E2


	xvmaddasp	vs32,	vs4,	vs16
	xvmaddasp	vs33,	vs5,	vs16
	xvmaddasp	vs34,	vs6,	vs16
	xvmaddasp	vs35,	vs7,	vs16

	xvmaddasp	vs36,	vs4,	vs17
	xvmaddasp	vs37,	vs5,	vs17
	xvmaddasp	vs38,	vs6,	vs17
	xvmaddasp	vs39,	vs7,	vs17


.endm

.macro KERNEL2x16_SUBI1


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO
	lxvw4x		vs2,	o32,	AO
	lxvw4x		vs3,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1

	addi		BO,	BO,	32


	xvmulsp		vs32,	vs0,	vs8
	xvmulsp		vs33,	vs1,	vs8
	xvmulsp		vs34,	vs2,	vs8
	xvmulsp		vs35,	vs3,	vs8

	xvmulsp		vs36,	vs0,	vs9
	xvmulsp		vs37,	vs1,	vs9
	xvmulsp		vs38,	vs2,	vs9
	xvmulsp		vs39,	vs3,	vs9


.endm

.macro KERNEL2x16_SUB1


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO
	lxvw4x		vs2,	o32,	AO
	lxvw4x		vs3,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1

	addi		BO,	BO,	32


	xvmaddasp	vs32,	vs0,	vs8
	xvmaddasp	vs33,	vs1,	vs8
	xvmaddasp	vs34,	vs2,	vs8
	xvmaddasp	vs35,	vs3,	vs8

	xvmaddasp	vs36,	vs0,	vs9
	xvmaddasp	vs37,	vs1,	vs9
	xvmaddasp	vs38,	vs2,	vs9
	xvmaddasp	vs39,	vs3,	vs9


.endm

.macro SAVE2x16

	mr		T1,	CO

#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1
	lxvw4x		vs2,	o32,	T1
	lxvw4x		vs3,	o48,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs32,	alpha_vr
	xvmulsp		vs1,	vs33,	alpha_vr
	xvmulsp		vs2,	vs34,	alpha_vr
	xvmulsp		vs3,	vs35,	alpha_vr
#else
	xvmaddasp	vs0,	vs32,	alpha_vr
	xvmaddasp	vs1,	vs33,	alpha_vr
	xvmaddasp	vs2,	vs34,	alpha_vr
	xvmaddasp	vs3,	vs35,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1
	stxvw4x		vs2,	o32,	T1
	stxvw4x		vs3,	o48,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1
	lxvw4x		vs2,	o32,	T1
	lxvw4x		vs3,	o48,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs36,	alpha_vr
	xvmulsp		vs1,	vs37,	alpha_vr
	xvmulsp		vs2,	vs38,	alpha_vr
	xvmulsp		vs3,	vs39,	alpha_vr
#else
	xvmaddasp	vs0,	vs36,	alpha_vr
	xvmaddasp	vs1,	vs37,	alpha_vr
	xvmaddasp	vs2,	vs38,	alpha_vr
	xvmaddasp	vs3,	vs39,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1
	stxvw4x		vs2,	o32,	T1
	stxvw4x		vs3,	o48,	T1

	add		T1,	T1,	LDC

	addi		CO,	CO,	64

.endm


/**********************************************************************************************
* Macros for N=2 and M=8
**********************************************************************************************/

.macro LOAD2x8_1

	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1

	addi		BO,	BO,	32

.endm

.macro KERNEL2x8_I1


	lxvw4x		vs4,	o0,	AO
	lxvw4x		vs5,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1
	lxvw4x		vs17,	o16,	T1

	addi		BO,	BO,	32


	xvmulsp		vs32,	vs0,	vs8
	xvmulsp		vs33,	vs1,	vs8

	xvmulsp		vs34,	vs0,	vs9
	xvmulsp		vs35,	vs1,	vs9


.endm

.macro KERNEL2x8_1


	lxvw4x		vs4,	o0,	AO
	lxvw4x		vs5,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1
	lxvw4x		vs17,	o16,	T1

	addi		BO,	BO,	32


	xvmaddasp	vs32,	vs0,	vs8
	xvmaddasp	vs33,	vs1,	vs8

	xvmaddasp	vs34,	vs0,	vs9
	xvmaddasp	vs35,	vs1,	vs9


.endm

.macro KERNEL2x8_2


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1

	addi		BO,	BO,	32


	xvmaddasp	vs32,	vs4,	vs16
	xvmaddasp	vs33,	vs5,	vs16

	xvmaddasp	vs34,	vs4,	vs17
	xvmaddasp	vs35,	vs5,	vs17


.endm

.macro KERNEL2x8_E2


	xvmaddasp	vs32,	vs4,	vs16
	xvmaddasp	vs33,	vs5,	vs16

	xvmaddasp	vs34,	vs4,	vs17
	xvmaddasp	vs35,	vs5,	vs17


.endm

.macro KERNEL2x8_SUBI1


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1

	addi		BO,	BO,	32


	xvmulsp		vs32,	vs0,	vs8
	xvmulsp		vs33,	vs1,	vs8

	xvmulsp		vs34,	vs0,	vs9
	xvmulsp		vs35,	vs1,	vs9


.endm

.macro KERNEL2x8_SUB1


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1

	addi		BO,	BO,	32


	xvmaddasp	vs32,	vs0,	vs8
	xvmaddasp	vs33,	vs1,	vs8

	xvmaddasp	vs34,	vs0,	vs9
	xvmaddasp	vs35,	vs1,	vs9


.endm

.macro SAVE2x8

	mr		T1,	CO

#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs32,	alpha_vr
	xvmulsp		vs1,	vs33,	alpha_vr
#else
	xvmaddasp	vs0,	vs32,	alpha_vr
	xvmaddasp	vs1,	vs33,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs34,	alpha_vr
	xvmulsp		vs1,	vs35,	alpha_vr
#else
	xvmaddasp	vs0,	vs34,	alpha_vr
	xvmaddasp	vs1,	vs35,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1

	add		T1,	T1,	LDC

	addi		CO,	CO,	32

.endm


/**********************************************************************************************
* Macros for N=2 and M=4
**********************************************************************************************/

.macro LOAD2x4_1

	lxvw4x		vs0,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1

	addi		BO,	BO,	32

.endm

.macro KERNEL2x4_I1


	lxvw4x		vs4,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1
	lxvw4x		vs17,	o16,	T1

	addi		BO,	BO,	32


	xvmulsp		vs32,	vs0,	vs8

	xvmulsp		vs33,	vs0,	vs9


.endm

.macro KERNEL2x4_1


	lxvw4x		vs4,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1
	lxvw4x		vs17,	o16,	T1

	addi		BO,	BO,	32


	xvmaddasp	vs32,	vs0,	vs8

	xvmaddasp	vs33,	vs0,	vs9


.endm

.macro KERNEL2x4_2


	lxvw4x		vs0,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1

	addi		BO,	BO,	32


	xvmaddasp	vs32,	vs4,	vs16

	xvmaddasp	vs33,	vs4,	vs17


.endm

.macro KERNEL2x4_E2


	xvmaddasp	vs32,	vs4,	vs16

	xvmaddasp	vs33,	vs4,	vs17


.endm

.macro KERNEL2x4_SUBI1


	lxvw4x		vs0,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1

	addi		BO,	BO,	32


	xvmulsp		vs32,	vs0,	vs8

	xvmulsp		vs33,	vs0,	vs9


.endm

.macro KERNEL2x4_SUB1


	lxvw4x		vs0,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1
	lxvw4x		vs9,	o16,	T1

	addi		BO,	BO,	32


	xvmaddasp	vs32,	vs0,	vs8

	xvmaddasp	vs33,	vs0,	vs9


.endm

.macro SAVE2x4

	mr		T1,	CO

#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs32,	alpha_vr
#else
	xvmaddasp	vs0,	vs32,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs33,	alpha_vr
#else
	xvmaddasp	vs0,	vs33,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1

	add		T1,	T1,	LDC

	addi		CO,	CO,	16

.endm


/**********************************************************************************************
* Macros for N=2 and M=2
**********************************************************************************************/

.macro LOAD2x2_1

	lxsspx		vs0,	o0,	AO
	lxsspx		vs1,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1


	addi		BO,	BO,	32

.endm

.macro KERNEL2x2_I1


	lxsspx		vs4,	o0,	AO
	lxsspx		vs5,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs16,	o0,	T1
	lxsspx		vs17,	o16,	T1


	addi		BO,	BO,	32


	xsmuldp		vs32,	vs0,	vs8
	xsmuldp		vs33,	vs1,	vs8

	xsmuldp		vs34,	vs0,	vs9
	xsmuldp		vs35,	vs1,	vs9


.endm

.macro KERNEL2x2_1


	lxsspx		vs4,	o0,	AO
	lxsspx		vs5,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs16,	o0,	T1
	lxsspx		vs17,	o16,	T1


	addi		BO,	BO,	32


	xsmaddadp	vs32,	vs0,	vs8
	xsmaddadp	vs33,	vs1,	vs8

	xsmaddadp	vs34,	vs0,	vs9
	xsmaddadp	vs35,	vs1,	vs9


.endm

.macro KERNEL2x2_2


	lxsspx		vs0,	o0,	AO
	lxsspx		vs1,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1


	addi		BO,	BO,	32


	xsmaddadp	vs32,	vs4,	vs16
	xsmaddadp	vs33,	vs5,	vs16

	xsmaddadp	vs34,	vs4,	vs17
	xsmaddadp	vs35,	vs5,	vs17


.endm

.macro KERNEL2x2_E2


	xsmaddadp	vs32,	vs4,	vs16
	xsmaddadp	vs33,	vs5,	vs16

	xsmaddadp	vs34,	vs4,	vs17
	xsmaddadp	vs35,	vs5,	vs17


.endm

.macro KERNEL2x2_SUBI1


	lxsspx		vs0,	o0,	AO
	lxsspx		vs1,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1


	addi		BO,	BO,	32


	xsmuldp		vs32,	vs0,	vs8
	xsmuldp		vs33,	vs1,	vs8

	xsmuldp		vs34,	vs0,	vs9
	xsmuldp		vs35,	vs1,	vs9


.endm

.macro KERNEL2x2_SUB1


	lxsspx		vs0,	o0,	AO
	lxsspx		vs1,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1


	addi		BO,	BO,	32


	xsmaddadp	vs32,	vs0,	vs8
	xsmaddadp	vs33,	vs1,	vs8

	xsmaddadp	vs34,	vs0,	vs9
	xsmaddadp	vs35,	vs1,	vs9


.endm

.macro SAVE2x2

	mr		T1,	CO

#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1
	lxsspx		vs1,	o4,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs32,	alpha_r
	xsmuldp		vs1,	vs33,	alpha_r
#else
	xsmaddadp	vs0,	vs32,	alpha_r
	xsmaddadp	vs1,	vs33,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1
	stxsspx		vs1,	o4,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1
	lxsspx		vs1,	o4,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs34,	alpha_r
	xsmuldp		vs1,	vs35,	alpha_r
#else
	xsmaddadp	vs0,	vs34,	alpha_r
	xsmaddadp	vs1,	vs35,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1
	stxsspx		vs1,	o4,	T1

	add		T1,	T1,	LDC

	addi		CO,	CO,	8

.endm


/**********************************************************************************************
* Macros for N=2 and M=1
**********************************************************************************************/

.macro LOAD2x1_1

	lxsspx		vs0,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1


	addi		BO,	BO,	32

.endm

.macro KERNEL2x1_I1


	lxsspx		vs4,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs16,	o0,	T1
	lxsspx		vs17,	o16,	T1


	addi		BO,	BO,	32


	xsmuldp		vs32,	vs0,	vs8

	xsmuldp		vs33,	vs0,	vs9


.endm

.macro KERNEL2x1_1


	lxsspx		vs4,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs16,	o0,	T1
	lxsspx		vs17,	o16,	T1


	addi		BO,	BO,	32


	xsmaddadp	vs32,	vs0,	vs8

	xsmaddadp	vs33,	vs0,	vs9


.endm

.macro KERNEL2x1_2


	lxsspx		vs0,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1


	addi		BO,	BO,	32


	xsmaddadp	vs32,	vs4,	vs16

	xsmaddadp	vs33,	vs4,	vs17


.endm

.macro KERNEL2x1_E2


	xsmaddadp	vs32,	vs4,	vs16

	xsmaddadp	vs33,	vs4,	vs17


.endm

.macro KERNEL2x1_SUBI1


	lxsspx		vs0,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1


	addi		BO,	BO,	32


	xsmuldp		vs32,	vs0,	vs8

	xsmuldp		vs33,	vs0,	vs9


.endm

.macro KERNEL2x1_SUB1


	lxsspx		vs0,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1
	lxsspx		vs9,	o16,	T1


	addi		BO,	BO,	32


	xsmaddadp	vs32,	vs0,	vs8

	xsmaddadp	vs33,	vs0,	vs9


.endm

.macro SAVE2x1

	mr		T1,	CO

#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs32,	alpha_r
#else
	xsmaddadp	vs0,	vs32,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1

	add		T1,	T1,	LDC


#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs33,	alpha_r
#else
	xsmaddadp	vs0,	vs33,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1

	add		T1,	T1,	LDC

	addi		CO,	CO,	4

.endm


/**********************************************************************************************
* Macros for N=1 and M=16
**********************************************************************************************/

.macro LOAD1x16_1

	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO
	lxvw4x		vs2,	o32,	AO
	lxvw4x		vs3,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1

	addi		BO,	BO,	16

.endm

.macro KERNEL1x16_I1


	lxvw4x		vs4,	o0,	AO
	lxvw4x		vs5,	o16,	AO
	lxvw4x		vs6,	o32,	AO
	lxvw4x		vs7,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1

	addi		BO,	BO,	16


	xvmulsp		vs32,	vs0,	vs8
	xvmulsp		vs33,	vs1,	vs8
	xvmulsp		vs34,	vs2,	vs8
	xvmulsp		vs35,	vs3,	vs8


.endm

.macro KERNEL1x16_1


	lxvw4x		vs4,	o0,	AO
	lxvw4x		vs5,	o16,	AO
	lxvw4x		vs6,	o32,	AO
	lxvw4x		vs7,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1

	addi		BO,	BO,	16


	xvmaddasp	vs32,	vs0,	vs8
	xvmaddasp	vs33,	vs1,	vs8
	xvmaddasp	vs34,	vs2,	vs8
	xvmaddasp	vs35,	vs3,	vs8


.endm

.macro KERNEL1x16_2


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO
	lxvw4x		vs2,	o32,	AO
	lxvw4x		vs3,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1

	addi		BO,	BO,	16


	xvmaddasp	vs32,	vs4,	vs16
	xvmaddasp	vs33,	vs5,	vs16
	xvmaddasp	vs34,	vs6,	vs16
	xvmaddasp	vs35,	vs7,	vs16


.endm

.macro KERNEL1x16_E2


	xvmaddasp	vs32,	vs4,	vs16
	xvmaddasp	vs33,	vs5,	vs16
	xvmaddasp	vs34,	vs6,	vs16
	xvmaddasp	vs35,	vs7,	vs16


.endm

.macro KERNEL1x16_SUBI1


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO
	lxvw4x		vs2,	o32,	AO
	lxvw4x		vs3,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1

	addi		BO,	BO,	16


	xvmulsp		vs32,	vs0,	vs8
	xvmulsp		vs33,	vs1,	vs8
	xvmulsp		vs34,	vs2,	vs8
	xvmulsp		vs35,	vs3,	vs8


.endm

.macro KERNEL1x16_SUB1


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO
	lxvw4x		vs2,	o32,	AO
	lxvw4x		vs3,	o48,	AO

	addi		AO,	AO,	64

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1

	addi		BO,	BO,	16


	xvmaddasp	vs32,	vs0,	vs8
	xvmaddasp	vs33,	vs1,	vs8
	xvmaddasp	vs34,	vs2,	vs8
	xvmaddasp	vs35,	vs3,	vs8


.endm

.macro SAVE1x16

	mr		T1,	CO

#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1
	lxvw4x		vs2,	o32,	T1
	lxvw4x		vs3,	o48,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs32,	alpha_vr
	xvmulsp		vs1,	vs33,	alpha_vr
	xvmulsp		vs2,	vs34,	alpha_vr
	xvmulsp		vs3,	vs35,	alpha_vr
#else
	xvmaddasp	vs0,	vs32,	alpha_vr
	xvmaddasp	vs1,	vs33,	alpha_vr
	xvmaddasp	vs2,	vs34,	alpha_vr
	xvmaddasp	vs3,	vs35,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1
	stxvw4x		vs2,	o32,	T1
	stxvw4x		vs3,	o48,	T1

	add		T1,	T1,	LDC

	addi		CO,	CO,	64

.endm


/**********************************************************************************************
* Macros for N=1 and M=8
**********************************************************************************************/

.macro LOAD1x8_1

	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1

	addi		BO,	BO,	16

.endm

.macro KERNEL1x8_I1


	lxvw4x		vs4,	o0,	AO
	lxvw4x		vs5,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1

	addi		BO,	BO,	16


	xvmulsp		vs32,	vs0,	vs8
	xvmulsp		vs33,	vs1,	vs8


.endm

.macro KERNEL1x8_1


	lxvw4x		vs4,	o0,	AO
	lxvw4x		vs5,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1

	addi		BO,	BO,	16


	xvmaddasp	vs32,	vs0,	vs8
	xvmaddasp	vs33,	vs1,	vs8


.endm

.macro KERNEL1x8_2


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1

	addi		BO,	BO,	16


	xvmaddasp	vs32,	vs4,	vs16
	xvmaddasp	vs33,	vs5,	vs16


.endm

.macro KERNEL1x8_E2


	xvmaddasp	vs32,	vs4,	vs16
	xvmaddasp	vs33,	vs5,	vs16


.endm

.macro KERNEL1x8_SUBI1


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1

	addi		BO,	BO,	16


	xvmulsp		vs32,	vs0,	vs8
	xvmulsp		vs33,	vs1,	vs8


.endm

.macro KERNEL1x8_SUB1


	lxvw4x		vs0,	o0,	AO
	lxvw4x		vs1,	o16,	AO

	addi		AO,	AO,	32

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1

	addi		BO,	BO,	16


	xvmaddasp	vs32,	vs0,	vs8
	xvmaddasp	vs33,	vs1,	vs8


.endm

.macro SAVE1x8

	mr		T1,	CO

#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1
	lxvw4x		vs1,	o16,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs32,	alpha_vr
	xvmulsp		vs1,	vs33,	alpha_vr
#else
	xvmaddasp	vs0,	vs32,	alpha_vr
	xvmaddasp	vs1,	vs33,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1
	stxvw4x		vs1,	o16,	T1

	add		T1,	T1,	LDC

	addi		CO,	CO,	32

.endm


/**********************************************************************************************
* Macros for N=1 and M=4
**********************************************************************************************/

.macro LOAD1x4_1

	lxvw4x		vs0,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1

	addi		BO,	BO,	16

.endm

.macro KERNEL1x4_I1


	lxvw4x		vs4,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1

	addi		BO,	BO,	16


	xvmulsp		vs32,	vs0,	vs8


.endm

.macro KERNEL1x4_1


	lxvw4x		vs4,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs16,	o0,	T1

	addi		BO,	BO,	16


	xvmaddasp	vs32,	vs0,	vs8


.endm

.macro KERNEL1x4_2


	lxvw4x		vs0,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1

	addi		BO,	BO,	16


	xvmaddasp	vs32,	vs4,	vs16


.endm

.macro KERNEL1x4_E2


	xvmaddasp	vs32,	vs4,	vs16


.endm

.macro KERNEL1x4_SUBI1


	lxvw4x		vs0,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1

	addi		BO,	BO,	16


	xvmulsp		vs32,	vs0,	vs8


.endm

.macro KERNEL1x4_SUB1


	lxvw4x		vs0,	o0,	AO

	addi		AO,	AO,	16

	mr		T1,	BO

	lxvw4x		vs8,	o0,	T1

	addi		BO,	BO,	16


	xvmaddasp	vs32,	vs0,	vs8


.endm

.macro SAVE1x4

	mr		T1,	CO

#ifndef TRMMKERNEL

	lxvw4x		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xvmulsp		vs0,	vs32,	alpha_vr
#else
	xvmaddasp	vs0,	vs32,	alpha_vr
#endif

	stxvw4x		vs0,	o0,	T1

	add		T1,	T1,	LDC

	addi		CO,	CO,	16

.endm


/**********************************************************************************************
* Macros for N=1 and M=2
**********************************************************************************************/

.macro LOAD1x2_1

	lxsspx		vs0,	o0,	AO
	lxsspx		vs1,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1


	addi		BO,	BO,	16

.endm

.macro KERNEL1x2_I1


	lxsspx		vs4,	o0,	AO
	lxsspx		vs5,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs16,	o0,	T1


	addi		BO,	BO,	16


	xsmuldp		vs32,	vs0,	vs8
	xsmuldp		vs33,	vs1,	vs8


.endm

.macro KERNEL1x2_1


	lxsspx		vs4,	o0,	AO
	lxsspx		vs5,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs16,	o0,	T1


	addi		BO,	BO,	16


	xsmaddadp	vs32,	vs0,	vs8
	xsmaddadp	vs33,	vs1,	vs8


.endm

.macro KERNEL1x2_2


	lxsspx		vs0,	o0,	AO
	lxsspx		vs1,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1


	addi		BO,	BO,	16


	xsmaddadp	vs32,	vs4,	vs16
	xsmaddadp	vs33,	vs5,	vs16


.endm

.macro KERNEL1x2_E2


	xsmaddadp	vs32,	vs4,	vs16
	xsmaddadp	vs33,	vs5,	vs16


.endm

.macro KERNEL1x2_SUBI1


	lxsspx		vs0,	o0,	AO
	lxsspx		vs1,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1


	addi		BO,	BO,	16


	xsmuldp		vs32,	vs0,	vs8
	xsmuldp		vs33,	vs1,	vs8


.endm

.macro KERNEL1x2_SUB1


	lxsspx		vs0,	o0,	AO
	lxsspx		vs1,	o4,	AO

	addi		AO,	AO,	8

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1


	addi		BO,	BO,	16


	xsmaddadp	vs32,	vs0,	vs8
	xsmaddadp	vs33,	vs1,	vs8


.endm

.macro SAVE1x2

	mr		T1,	CO

#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1
	lxsspx		vs1,	o4,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs32,	alpha_r
	xsmuldp		vs1,	vs33,	alpha_r
#else
	xsmaddadp	vs0,	vs32,	alpha_r
	xsmaddadp	vs1,	vs33,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1
	stxsspx		vs1,	o4,	T1

	add		T1,	T1,	LDC

	addi		CO,	CO,	8

.endm


/**********************************************************************************************
* Macros for N=1 and M=1
**********************************************************************************************/

.macro LOAD1x1_1

	lxsspx		vs0,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1


	addi		BO,	BO,	16

.endm

.macro KERNEL1x1_I1


	lxsspx		vs4,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs16,	o0,	T1


	addi		BO,	BO,	16


	xsmuldp		vs32,	vs0,	vs8


.endm

.macro KERNEL1x1_1


	lxsspx		vs4,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs16,	o0,	T1


	addi		BO,	BO,	16


	xsmaddadp	vs32,	vs0,	vs8


.endm

.macro KERNEL1x1_2


	lxsspx		vs0,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1


	addi		BO,	BO,	16


	xsmaddadp	vs32,	vs4,	vs16


.endm

.macro KERNEL1x1_E2


	xsmaddadp	vs32,	vs4,	vs16


.endm

.macro KERNEL1x1_SUBI1


	lxsspx		vs0,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1


	addi		BO,	BO,	16


	xsmuldp		vs32,	vs0,	vs8


.endm

.macro KERNEL1x1_SUB1


	lxsspx		vs0,	o0,	AO

	addi		AO,	AO,	4

	mr		T1,	BO

	lxsspx		vs8,	o0,	T1


	addi		BO,	BO,	16


	xsmaddadp	vs32,	vs0,	vs8


.endm

.macro SAVE1x1

	mr		T1,	CO

#ifndef TRMMKERNEL

	lxsspx		vs0,	o0,	T1

#endif

#ifdef TRMMKERNEL
	xsmuldp		vs0,	vs32,	alpha_r
#else
	xsmaddadp	vs0,	vs32,	alpha_r
#endif

	stxsspx		vs0,	o0,	T1

	add		T1,	T1,	LDC

	addi		CO,	CO,	4

.endm





.macro COPYB_4x8


	lxvw4x		vs5,	o0,	BO
	xxspltw		vs6,	vs5,	0
	xxspltw		vs7,	vs5,	1
	xxspltw		vs8,	vs5,	2
	xxspltw		vs9,	vs5,	3

	lxvw4x		vs10,	o16,	BO
	xxspltw		vs11,	vs10,	0
	xxspltw		vs12,	vs10,	1
	xxspltw		vs13,	vs10,	2
	xxspltw		vs14,	vs10,	3

	lxvw4x		vs15,	o32,	BO
	xxspltw		vs16,	vs15,	0
	xxspltw		vs17,	vs15,	1
	xxspltw		vs18,	vs15,	2
	xxspltw		vs19,	vs15,	3

	lxvw4x		vs20,	o48,	BO
	xxspltw		vs21,	vs20,	0
	xxspltw		vs22,	vs20,	1
	xxspltw		vs23,	vs20,	2
	xxspltw		vs24,	vs20,	3

	addi		BO,	BO,	64
	lxvw4x		vs35,	o0,	BO
	xxspltw		vs36,	vs35,	0
	xxspltw		vs37,	vs35,	1
	xxspltw		vs38,	vs35,	2
	xxspltw		vs39,	vs35,	3

	lxvw4x		vs40,	o16,	BO
	xxspltw		vs41,	vs40,	0
	xxspltw		vs42,	vs40,	1
	xxspltw		vs43,	vs40,	2
	xxspltw		vs44,	vs40,	3

	lxvw4x		vs45,	o32,	BO
	xxspltw		vs46,	vs45,	0
	xxspltw		vs47,	vs45,	1
	xxspltw		vs48,	vs45,	2
	xxspltw		vs49,	vs45,	3

	lxvw4x		vs50,	o48,	BO
	xxspltw		vs51,	vs50,	0
	xxspltw		vs52,	vs50,	1
	xxspltw		vs53,	vs50,	2
	xxspltw		vs54,	vs50,	3

	addi		BO,	BO,	64


	stxvw4x		vs6,	o0,	BBO
	stxvw4x		vs7,	o16,	BBO
	stxvw4x		vs8,	o32,	BBO
	stxvw4x		vs9,	o48,	BBO

	addi		BBO,	BBO,	64
	stxvw4x		vs11,	o0,	BBO
	stxvw4x		vs12,	o16,	BBO
	stxvw4x		vs13,	o32,	BBO
	stxvw4x		vs14,	o48,	BBO

	addi		BBO,	BBO,	64
	stxvw4x		vs16,	o0,	BBO
	stxvw4x		vs17,	o16,	BBO
	stxvw4x		vs18,	o32,	BBO
	stxvw4x		vs19,	o48,	BBO

	addi		BBO,	BBO,	64
	stxvw4x		vs21,	o0,	BBO
	stxvw4x		vs22,	o16,	BBO
	stxvw4x		vs23,	o32,	BBO
	stxvw4x		vs24,	o48,	BBO

	addi		BBO,	BBO,	64
	stxvw4x		vs36,	o0,	BBO
	stxvw4x		vs37,	o16,	BBO
	stxvw4x		vs38,	o32,	BBO
	stxvw4x		vs39,	o48,	BBO

	addi		BBO,	BBO,	64
	stxvw4x		vs41,	o0,	BBO
	stxvw4x		vs42,	o16,	BBO
	stxvw4x		vs43,	o32,	BBO
	stxvw4x		vs44,	o48,	BBO

	addi		BBO,	BBO,	64
	stxvw4x		vs46,	o0,	BBO
	stxvw4x		vs47,	o16,	BBO
	stxvw4x		vs48,	o32,	BBO
	stxvw4x		vs49,	o48,	BBO

	addi		BBO,	BBO,	64
	stxvw4x		vs51,	o0,	BBO
	stxvw4x		vs52,	o16,	BBO
	stxvw4x		vs53,	o32,	BBO
	stxvw4x		vs54,	o48,	BBO

	addi		BBO,	BBO,	64
.endm


.macro COPYB_1x8


	lxvw4x		vs5,	o0,	BO
	xxspltw		vs6,	vs5,	0
	xxspltw		vs7,	vs5,	1
	xxspltw		vs8,	vs5,	2
	xxspltw		vs9,	vs5,	3

	lxvw4x		vs10,	o16,	BO
	xxspltw		vs11,	vs10,	0
	xxspltw		vs12,	vs10,	1
	xxspltw		vs13,	vs10,	2
	xxspltw		vs14,	vs10,	3


	addi		BO,	BO,	32

	stxvw4x		vs6,	o0,	BBO
	stxvw4x		vs7,	o16,	BBO
	stxvw4x		vs8,	o32,	BBO
	stxvw4x		vs9,	o48,	BBO

	addi		BBO,	BBO,	64
	stxvw4x		vs11,	o0,	BBO
	stxvw4x		vs12,	o16,	BBO
	stxvw4x		vs13,	o32,	BBO
	stxvw4x		vs14,	o48,	BBO

	addi		BBO,	BBO,	64
.endm

