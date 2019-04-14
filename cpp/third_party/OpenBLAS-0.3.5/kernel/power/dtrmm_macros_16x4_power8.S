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
* 2016/03/05 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/

/*********************************************************************
* Macros for N=4, M=16                                               *
*********************************************************************/

.macro LOAD4x16_1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 64

	lxvd2x	vs4,	0,	AO
	lxvd2x	vs5,	o16,	AO
	lxvd2x	vs6,	o32,	AO
	lxvd2x	vs7,	o48,	AO

	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 64
	addi		BO, BO, 32

.endm

.macro KERNEL4x16_I1

	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO

	xvmuldp			vs36,	vs4,	vs24
	xvmuldp			vs37,	vs5,	vs24
	xvmuldp			vs38,	vs6,	vs24
	xvmuldp			vs39,	vs7,	vs24

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25
	xvmuldp			vs42,	vs2,	vs25
	xvmuldp			vs43,	vs3,	vs25

	lxvd2x	vs10,	o32,	AO
	lxvd2x	vs11,	o48,	AO

	xvmuldp			vs44,	vs4,	vs25
	xvmuldp			vs45,	vs5,	vs25
	xvmuldp			vs46,	vs6,	vs25
	xvmuldp			vs47,	vs7,	vs25

	addi		AO, AO, 64

	xvmuldp			vs48,	vs0,	vs26
	xvmuldp			vs49,	vs1,	vs26
	xvmuldp			vs50,	vs2,	vs26
	xvmuldp			vs51,	vs3,	vs26

	lxvd2x	vs12,	0,	AO
	lxvd2x	vs13,	o16,	AO

	xvmuldp			vs52,	vs4,	vs26
	xvmuldp			vs53,	vs5,	vs26
	xvmuldp			vs54,	vs6,	vs26
	xvmuldp			vs55,	vs7,	vs26

	lxvd2x	vs14,	o32,	AO
	lxvd2x	vs15,	o48,	AO

	xvmuldp			vs56,	vs0,	vs27
	xvmuldp			vs57,	vs1,	vs27
	xvmuldp			vs58,	vs2,	vs27
	xvmuldp			vs59,	vs3,	vs27

	lxvdsx	vs30,	o16,	BO
	lxvdsx	vs31,	o24,	BO

	xvmuldp			vs60,	vs4,	vs27
	xvmuldp			vs61,	vs5,	vs27
	xvmuldp			vs62,	vs6,	vs27
	xvmuldp			vs63,	vs7,	vs27

	addi		AO, AO, 64
	addi		BO, BO, 32

.endm

.macro KERNEL4x16_1

	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO

	xvmaddadp		vs36,	vs4,	vs24
	xvmaddadp		vs37,	vs5,	vs24
	xvmaddadp		vs38,	vs6,	vs24
	xvmaddadp		vs39,	vs7,	vs24

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25
	xvmaddadp		vs42,	vs2,	vs25
	xvmaddadp		vs43,	vs3,	vs25

	lxvd2x	vs10,	o32,	AO
	lxvd2x	vs11,	o48,	AO

	xvmaddadp		vs44,	vs4,	vs25
	xvmaddadp		vs45,	vs5,	vs25
	xvmaddadp		vs46,	vs6,	vs25
	xvmaddadp		vs47,	vs7,	vs25

	addi		AO, AO, 64

	xvmaddadp		vs48,	vs0,	vs26
	xvmaddadp		vs49,	vs1,	vs26
	xvmaddadp		vs50,	vs2,	vs26
	xvmaddadp		vs51,	vs3,	vs26

	lxvd2x	vs12,	0,	AO
	lxvd2x	vs13,	o16,	AO

	xvmaddadp		vs52,	vs4,	vs26
	xvmaddadp		vs53,	vs5,	vs26
	xvmaddadp		vs54,	vs6,	vs26
	xvmaddadp		vs55,	vs7,	vs26

	lxvd2x	vs14,	o32,	AO
	lxvd2x	vs15,	o48,	AO

	xvmaddadp		vs56,	vs0,	vs27
	xvmaddadp		vs57,	vs1,	vs27
	xvmaddadp		vs58,	vs2,	vs27
	xvmaddadp		vs59,	vs3,	vs27


	lxvdsx	vs30,	o16,	BO
	lxvdsx	vs31,	o24,	BO

	xvmaddadp		vs60,	vs4,	vs27
	xvmaddadp		vs61,	vs5,	vs27
	xvmaddadp		vs62,	vs6,	vs27
	xvmaddadp		vs63,	vs7,	vs27

	addi		AO, AO, 64
	addi		BO, BO, 32

.endm

.macro KERNEL4x16_2

	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	xvmaddadp		vs36,	vs12,	vs28
	xvmaddadp		vs37,	vs13,	vs28
	xvmaddadp		vs38,	vs14,	vs28
	xvmaddadp		vs39,	vs15,	vs28

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29
	xvmaddadp		vs42,	vs10,	vs29
	xvmaddadp		vs43,	vs11,	vs29

	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	xvmaddadp		vs44,	vs12,	vs29
	xvmaddadp		vs45,	vs13,	vs29
	xvmaddadp		vs46,	vs14,	vs29
	xvmaddadp		vs47,	vs15,	vs29

	addi		AO, AO, 64

	xvmaddadp		vs48,	vs8,	vs30
	xvmaddadp		vs49,	vs9,	vs30
	xvmaddadp		vs50,	vs10,	vs30
	xvmaddadp		vs51,	vs11,	vs30

	lxvd2x	vs4,	0,	AO
	lxvd2x	vs5,	o16,	AO

	xvmaddadp		vs52,	vs12,	vs30
	xvmaddadp		vs53,	vs13,	vs30
	xvmaddadp		vs54,	vs14,	vs30
	xvmaddadp		vs55,	vs15,	vs30

	lxvd2x	vs6,	o32,	AO
	lxvd2x	vs7,	o48,	AO

	xvmaddadp		vs56,	vs8,	vs31
	xvmaddadp		vs57,	vs9,	vs31
	xvmaddadp		vs58,	vs10,	vs31
	xvmaddadp		vs59,	vs11,	vs31

	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	xvmaddadp		vs60,	vs12,	vs31
	xvmaddadp		vs61,	vs13,	vs31
	xvmaddadp		vs62,	vs14,	vs31
	xvmaddadp		vs63,	vs15,	vs31

	addi		AO, AO, 64
	addi		BO, BO, 32

.endm

.macro KERNEL4x16_E2


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28
	xvmaddadp		vs36,	vs12,	vs28
	xvmaddadp		vs37,	vs13,	vs28
	xvmaddadp		vs38,	vs14,	vs28
	xvmaddadp		vs39,	vs15,	vs28

	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29
	xvmaddadp		vs42,	vs10,	vs29
	xvmaddadp		vs43,	vs11,	vs29
	xvmaddadp		vs44,	vs12,	vs29
	xvmaddadp		vs45,	vs13,	vs29
	xvmaddadp		vs46,	vs14,	vs29
	xvmaddadp		vs47,	vs15,	vs29

	xvmaddadp		vs48,	vs8,	vs30
	xvmaddadp		vs49,	vs9,	vs30
	xvmaddadp		vs50,	vs10,	vs30
	xvmaddadp		vs51,	vs11,	vs30
	xvmaddadp		vs52,	vs12,	vs30
	xvmaddadp		vs53,	vs13,	vs30
	xvmaddadp		vs54,	vs14,	vs30
	xvmaddadp		vs55,	vs15,	vs30

	xvmaddadp		vs56,	vs8,	vs31
	xvmaddadp		vs57,	vs9,	vs31
	xvmaddadp		vs58,	vs10,	vs31
	xvmaddadp		vs59,	vs11,	vs31
	xvmaddadp		vs60,	vs12,	vs31
	xvmaddadp		vs61,	vs13,	vs31
	xvmaddadp		vs62,	vs14,	vs31
	xvmaddadp		vs63,	vs15,	vs31

.endm

.macro KERNEL4x16_SUBI1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO
	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 64
	addi		BO, BO, 32

	lxvd2x	vs4,	0,	AO
	lxvd2x	vs5,	o16,	AO
	lxvd2x	vs6,	o32,	AO
	lxvd2x	vs7,	o48,	AO

	addi		AO, AO, 64


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24
	xvmuldp			vs36,	vs4,	vs24
	xvmuldp			vs37,	vs5,	vs24
	xvmuldp			vs38,	vs6,	vs24
	xvmuldp			vs39,	vs7,	vs24

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25
	xvmuldp			vs42,	vs2,	vs25
	xvmuldp			vs43,	vs3,	vs25
	xvmuldp			vs44,	vs4,	vs25
	xvmuldp			vs45,	vs5,	vs25
	xvmuldp			vs46,	vs6,	vs25
	xvmuldp			vs47,	vs7,	vs25

	xvmuldp			vs48,	vs0,	vs26
	xvmuldp			vs49,	vs1,	vs26
	xvmuldp			vs50,	vs2,	vs26
	xvmuldp			vs51,	vs3,	vs26
	xvmuldp			vs52,	vs4,	vs26
	xvmuldp			vs53,	vs5,	vs26
	xvmuldp			vs54,	vs6,	vs26
	xvmuldp			vs55,	vs7,	vs26

	xvmuldp			vs56,	vs0,	vs27
	xvmuldp			vs57,	vs1,	vs27
	xvmuldp			vs58,	vs2,	vs27
	xvmuldp			vs59,	vs3,	vs27
	xvmuldp			vs60,	vs4,	vs27
	xvmuldp			vs61,	vs5,	vs27
	xvmuldp			vs62,	vs6,	vs27
	xvmuldp			vs63,	vs7,	vs27

.endm

.macro KERNEL4x16_SUB1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO
	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 64
	addi		BO, BO, 32

	lxvd2x	vs4,	0,	AO
	lxvd2x	vs5,	o16,	AO
	lxvd2x	vs6,	o32,	AO
	lxvd2x	vs7,	o48,	AO

	addi		AO, AO, 64


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24
	xvmaddadp		vs36,	vs4,	vs24
	xvmaddadp		vs37,	vs5,	vs24
	xvmaddadp		vs38,	vs6,	vs24
	xvmaddadp		vs39,	vs7,	vs24

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25
	xvmaddadp		vs42,	vs2,	vs25
	xvmaddadp		vs43,	vs3,	vs25
	xvmaddadp		vs44,	vs4,	vs25
	xvmaddadp		vs45,	vs5,	vs25
	xvmaddadp		vs46,	vs6,	vs25
	xvmaddadp		vs47,	vs7,	vs25

	xvmaddadp		vs48,	vs0,	vs26
	xvmaddadp		vs49,	vs1,	vs26
	xvmaddadp		vs50,	vs2,	vs26
	xvmaddadp		vs51,	vs3,	vs26
	xvmaddadp		vs52,	vs4,	vs26
	xvmaddadp		vs53,	vs5,	vs26
	xvmaddadp		vs54,	vs6,	vs26
	xvmaddadp		vs55,	vs7,	vs26

	xvmaddadp		vs56,	vs0,	vs27
	xvmaddadp		vs57,	vs1,	vs27
	xvmaddadp		vs58,	vs2,	vs27
	xvmaddadp		vs59,	vs3,	vs27
	xvmaddadp		vs60,	vs4,	vs27
	xvmaddadp		vs61,	vs5,	vs27
	xvmaddadp		vs62,	vs6,	vs27
	xvmaddadp		vs63,	vs7,	vs27

.endm

.macro SAVE4x16

	mr		T1,	CO
	addi		T2,	T1,	64

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
	lxvd2x		vs1,	o16,	T1
	lxvd2x		vs2,	o32,	T1
	lxvd2x		vs3,	o48,	T1

	lxvd2x		vs4,	0,	T2
	lxvd2x		vs5,	o16,	T2
	lxvd2x		vs6,	o32,	T2
	lxvd2x		vs7,	o48,	T2
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
	xvmaddadp	vs1,	vs33,	alpha_r
	xvmaddadp	vs2,	vs34,	alpha_r
	xvmaddadp	vs3,	vs35,	alpha_r
	xvmaddadp	vs4,	vs36,	alpha_r
	xvmaddadp	vs5,	vs37,	alpha_r
	xvmaddadp	vs6,	vs38,	alpha_r
	xvmaddadp	vs7,	vs39,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
	xvmuldp		vs1,	vs33,	alpha_r
	xvmuldp		vs2,	vs34,	alpha_r
	xvmuldp		vs3,	vs35,	alpha_r
	xvmuldp		vs4,	vs36,	alpha_r
	xvmuldp		vs5,	vs37,	alpha_r
	xvmuldp		vs6,	vs38,	alpha_r
	xvmuldp		vs7,	vs39,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1
	stxvd2x		vs1,	o16,	T1
	stxvd2x		vs2,	o32,	T1
	stxvd2x		vs3,	o48,	T1


	stxvd2x		vs4,	0,	T2
	stxvd2x		vs5,	o16,	T2
	stxvd2x		vs6,	o32,	T2
	stxvd2x		vs7,	o48,	T2

	add		T1,	T1,	LDC
	add		T2,	T2,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs8,	0,	T1
	lxvd2x		vs9,	o16,	T1
	lxvd2x		vs10,	o32,	T1
	lxvd2x		vs11,	o48,	T1

	lxvd2x		vs12,	0,	T2
	lxvd2x		vs13,	o16,	T2
	lxvd2x		vs14,	o32,	T2
	lxvd2x		vs15,	o48,	T2
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs8,	vs40,	alpha_r
	xvmaddadp	vs9,	vs41,	alpha_r
	xvmaddadp	vs10,	vs42,	alpha_r
	xvmaddadp	vs11,	vs43,	alpha_r
	xvmaddadp	vs12,	vs44,	alpha_r
	xvmaddadp	vs13,	vs45,	alpha_r
	xvmaddadp	vs14,	vs46,	alpha_r
	xvmaddadp	vs15,	vs47,	alpha_r
#else
	xvmuldp		vs8,	vs40,	alpha_r
	xvmuldp		vs9,	vs41,	alpha_r
	xvmuldp		vs10,	vs42,	alpha_r
	xvmuldp		vs11,	vs43,	alpha_r
	xvmuldp		vs12,	vs44,	alpha_r
	xvmuldp		vs13,	vs45,	alpha_r
	xvmuldp		vs14,	vs46,	alpha_r
	xvmuldp		vs15,	vs47,	alpha_r
#endif

	stxvd2x		vs8,	0,	T1
	stxvd2x		vs9,	o16,	T1
	stxvd2x		vs10,	o32,	T1
	stxvd2x		vs11,	o48,	T1


	stxvd2x		vs12,	0,	T2
	stxvd2x		vs13,	o16,	T2
	stxvd2x		vs14,	o32,	T2
	stxvd2x		vs15,	o48,	T2

	add		T1,	T1,	LDC
	add		T2,	T2,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
	lxvd2x		vs1,	o16,	T1
	lxvd2x		vs2,	o32,	T1
	lxvd2x		vs3,	o48,	T1

	lxvd2x		vs4,	0,	T2
	lxvd2x		vs5,	o16,	T2
	lxvd2x		vs6,	o32,	T2
	lxvd2x		vs7,	o48,	T2
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs48,	alpha_r
	xvmaddadp	vs1,	vs49,	alpha_r
	xvmaddadp	vs2,	vs50,	alpha_r
	xvmaddadp	vs3,	vs51,	alpha_r
	xvmaddadp	vs4,	vs52,	alpha_r
	xvmaddadp	vs5,	vs53,	alpha_r
	xvmaddadp	vs6,	vs54,	alpha_r
	xvmaddadp	vs7,	vs55,	alpha_r
#else
	xvmuldp		vs0,	vs48,	alpha_r
	xvmuldp		vs1,	vs49,	alpha_r
	xvmuldp		vs2,	vs50,	alpha_r
	xvmuldp		vs3,	vs51,	alpha_r
	xvmuldp		vs4,	vs52,	alpha_r
	xvmuldp		vs5,	vs53,	alpha_r
	xvmuldp		vs6,	vs54,	alpha_r
	xvmuldp		vs7,	vs55,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1
	stxvd2x		vs1,	o16,	T1
	stxvd2x		vs2,	o32,	T1
	stxvd2x		vs3,	o48,	T1


	stxvd2x		vs4,	0,	T2
	stxvd2x		vs5,	o16,	T2
	stxvd2x		vs6,	o32,	T2
	stxvd2x		vs7,	o48,	T2

	add		T1,	T1,	LDC
	add		T2,	T2,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs8,	0,	T1
	lxvd2x		vs9,	o16,	T1
	lxvd2x		vs10,	o32,	T1
	lxvd2x		vs11,	o48,	T1

	lxvd2x		vs12,	0,	T2
	lxvd2x		vs13,	o16,	T2
	lxvd2x		vs14,	o32,	T2
	lxvd2x		vs15,	o48,	T2
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs8,	vs56,	alpha_r
	xvmaddadp	vs9,	vs57,	alpha_r
	xvmaddadp	vs10,	vs58,	alpha_r
	xvmaddadp	vs11,	vs59,	alpha_r
	xvmaddadp	vs12,	vs60,	alpha_r
	xvmaddadp	vs13,	vs61,	alpha_r
	xvmaddadp	vs14,	vs62,	alpha_r
	xvmaddadp	vs15,	vs63,	alpha_r
#else
	xvmuldp		vs8,	vs56,	alpha_r
	xvmuldp		vs9,	vs57,	alpha_r
	xvmuldp		vs10,	vs58,	alpha_r
	xvmuldp		vs11,	vs59,	alpha_r
	xvmuldp		vs12,	vs60,	alpha_r
	xvmuldp		vs13,	vs61,	alpha_r
	xvmuldp		vs14,	vs62,	alpha_r
	xvmuldp		vs15,	vs63,	alpha_r
#endif

	stxvd2x		vs8,	0,	T1
	stxvd2x		vs9,	o16,	T1
	stxvd2x		vs10,	o32,	T1
	stxvd2x		vs11,	o48,	T1


	stxvd2x		vs12,	0,	T2
	stxvd2x		vs13,	o16,	T2
	stxvd2x		vs14,	o32,	T2
	stxvd2x		vs15,	o48,	T2

	addi		CO,	CO,	128

.endm

/*********************************************************************
* Macros for N=4, M=8                                                *
*********************************************************************/

.macro LOAD4x8_1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 64
	addi		BO, BO, 32

.endm

.macro KERNEL4x8_I1

	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO

	xvmuldp			vs42,	vs2,	vs25
	xvmuldp			vs43,	vs3,	vs25

	xvmuldp			vs48,	vs0,	vs26
	xvmuldp			vs49,	vs1,	vs26

	lxvd2x	vs10,	o32,	AO
	lxvd2x	vs11,	o48,	AO

	xvmuldp			vs50,	vs2,	vs26
	xvmuldp			vs51,	vs3,	vs26

	lxvdsx	vs30,	o16,	BO
	lxvdsx	vs31,	o24,	BO

	xvmuldp			vs56,	vs0,	vs27
	xvmuldp			vs57,	vs1,	vs27
	xvmuldp			vs58,	vs2,	vs27
	xvmuldp			vs59,	vs3,	vs27

	addi		AO, AO, 64
	addi		BO, BO, 32

.endm

.macro KERNEL4x8_1

	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25
	xvmaddadp		vs42,	vs2,	vs25
	xvmaddadp		vs43,	vs3,	vs25

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO

	xvmaddadp		vs48,	vs0,	vs26
	xvmaddadp		vs49,	vs1,	vs26

	lxvd2x	vs10,	o32,	AO
	lxvd2x	vs11,	o48,	AO

	xvmaddadp		vs50,	vs2,	vs26
	xvmaddadp		vs51,	vs3,	vs26

	lxvdsx	vs30,	o16,	BO
	lxvdsx	vs31,	o24,	BO

	xvmaddadp		vs56,	vs0,	vs27
	xvmaddadp		vs57,	vs1,	vs27
	xvmaddadp		vs58,	vs2,	vs27
	xvmaddadp		vs59,	vs3,	vs27

	addi		AO, AO, 64
	addi		BO, BO, 32

.endm

.macro KERNEL4x8_2

	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29
	xvmaddadp		vs42,	vs10,	vs29
	xvmaddadp		vs43,	vs11,	vs29

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	xvmaddadp		vs48,	vs8,	vs30
	xvmaddadp		vs49,	vs9,	vs30

	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	xvmaddadp		vs50,	vs10,	vs30
	xvmaddadp		vs51,	vs11,	vs30

	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	xvmaddadp		vs56,	vs8,	vs31
	xvmaddadp		vs57,	vs9,	vs31
	xvmaddadp		vs58,	vs10,	vs31
	xvmaddadp		vs59,	vs11,	vs31

	addi		AO, AO, 64
	addi		BO, BO, 32

.endm

.macro KERNEL4x8_E2


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28

	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29
	xvmaddadp		vs42,	vs10,	vs29
	xvmaddadp		vs43,	vs11,	vs29

	xvmaddadp		vs48,	vs8,	vs30
	xvmaddadp		vs49,	vs9,	vs30
	xvmaddadp		vs50,	vs10,	vs30
	xvmaddadp		vs51,	vs11,	vs30

	xvmaddadp		vs56,	vs8,	vs31
	xvmaddadp		vs57,	vs9,	vs31
	xvmaddadp		vs58,	vs10,	vs31
	xvmaddadp		vs59,	vs11,	vs31

.endm

.macro KERNEL4x8_SUBI1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO
	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 64
	addi		BO, BO, 32


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25
	xvmuldp			vs42,	vs2,	vs25
	xvmuldp			vs43,	vs3,	vs25

	xvmuldp			vs48,	vs0,	vs26
	xvmuldp			vs49,	vs1,	vs26
	xvmuldp			vs50,	vs2,	vs26
	xvmuldp			vs51,	vs3,	vs26

	xvmuldp			vs56,	vs0,	vs27
	xvmuldp			vs57,	vs1,	vs27
	xvmuldp			vs58,	vs2,	vs27
	xvmuldp			vs59,	vs3,	vs27

.endm

.macro KERNEL4x8_SUB1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO
	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 64
	addi		BO, BO, 32


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25
	xvmaddadp		vs42,	vs2,	vs25
	xvmaddadp		vs43,	vs3,	vs25

	xvmaddadp		vs48,	vs0,	vs26
	xvmaddadp		vs49,	vs1,	vs26
	xvmaddadp		vs50,	vs2,	vs26
	xvmaddadp		vs51,	vs3,	vs26

	xvmaddadp		vs56,	vs0,	vs27
	xvmaddadp		vs57,	vs1,	vs27
	xvmaddadp		vs58,	vs2,	vs27
	xvmaddadp		vs59,	vs3,	vs27

.endm

.macro SAVE4x8

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
	lxvd2x		vs1,	o16,	T1
	lxvd2x		vs2,	o32,	T1
	lxvd2x		vs3,	o48,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
	xvmaddadp	vs1,	vs33,	alpha_r
	xvmaddadp	vs2,	vs34,	alpha_r
	xvmaddadp	vs3,	vs35,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
	xvmuldp		vs1,	vs33,	alpha_r
	xvmuldp		vs2,	vs34,	alpha_r
	xvmuldp		vs3,	vs35,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1
	stxvd2x		vs1,	o16,	T1
	stxvd2x		vs2,	o32,	T1
	stxvd2x		vs3,	o48,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs8,	0,	T1
	lxvd2x		vs9,	o16,	T1
	lxvd2x		vs10,	o32,	T1
	lxvd2x		vs11,	o48,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs8,	vs40,	alpha_r
	xvmaddadp	vs9,	vs41,	alpha_r
	xvmaddadp	vs10,	vs42,	alpha_r
	xvmaddadp	vs11,	vs43,	alpha_r
#else
	xvmuldp		vs8,	vs40,	alpha_r
	xvmuldp		vs9,	vs41,	alpha_r
	xvmuldp		vs10,	vs42,	alpha_r
	xvmuldp		vs11,	vs43,	alpha_r
#endif

	stxvd2x		vs8,	0,	T1
	stxvd2x		vs9,	o16,	T1
	stxvd2x		vs10,	o32,	T1
	stxvd2x		vs11,	o48,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
	lxvd2x		vs1,	o16,	T1
	lxvd2x		vs2,	o32,	T1
	lxvd2x		vs3,	o48,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs48,	alpha_r
	xvmaddadp	vs1,	vs49,	alpha_r
	xvmaddadp	vs2,	vs50,	alpha_r
	xvmaddadp	vs3,	vs51,	alpha_r
#else
	xvmuldp		vs0,	vs48,	alpha_r
	xvmuldp		vs1,	vs49,	alpha_r
	xvmuldp		vs2,	vs50,	alpha_r
	xvmuldp		vs3,	vs51,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1
	stxvd2x		vs1,	o16,	T1
	stxvd2x		vs2,	o32,	T1
	stxvd2x		vs3,	o48,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs8,	0,	T1
	lxvd2x		vs9,	o16,	T1
	lxvd2x		vs10,	o32,	T1
	lxvd2x		vs11,	o48,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs8,	vs56,	alpha_r
	xvmaddadp	vs9,	vs57,	alpha_r
	xvmaddadp	vs10,	vs58,	alpha_r
	xvmaddadp	vs11,	vs59,	alpha_r
#else
	xvmuldp		vs8,	vs56,	alpha_r
	xvmuldp		vs9,	vs57,	alpha_r
	xvmuldp		vs10,	vs58,	alpha_r
	xvmuldp		vs11,	vs59,	alpha_r
#endif

	stxvd2x		vs8,	0,	T1
	stxvd2x		vs9,	o16,	T1
	stxvd2x		vs10,	o32,	T1
	stxvd2x		vs11,	o48,	T1

	addi		CO,	CO,	64

.endm

/*********************************************************************
* Macros for N=4, M=4                                                *
*********************************************************************/

.macro LOAD4x4_1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO
	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 32
	addi		BO, BO, 32

.endm

.macro KERNEL4x4_I1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO
	lxvdsx	vs30,	o16,	BO
	lxvdsx	vs31,	o24,	BO

	addi		AO, AO, 32
	addi		BO, BO, 32


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25

	xvmuldp			vs48,	vs0,	vs26
	xvmuldp			vs49,	vs1,	vs26

	xvmuldp			vs56,	vs0,	vs27
	xvmuldp			vs57,	vs1,	vs27

.endm

.macro KERNEL4x4_1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO
	lxvdsx	vs30,	o16,	BO
	lxvdsx	vs31,	o24,	BO

	addi		AO, AO, 32
	addi		BO, BO, 32


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25

	xvmaddadp		vs48,	vs0,	vs26
	xvmaddadp		vs49,	vs1,	vs26

	xvmaddadp		vs56,	vs0,	vs27
	xvmaddadp		vs57,	vs1,	vs27

.endm

.macro KERNEL4x4_2

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO
	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 32
	addi		BO, BO, 32


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28

	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29

	xvmaddadp		vs48,	vs8,	vs30
	xvmaddadp		vs49,	vs9,	vs30

	xvmaddadp		vs56,	vs8,	vs31
	xvmaddadp		vs57,	vs9,	vs31

.endm

.macro KERNEL4x4_E2


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28

	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29

	xvmaddadp		vs48,	vs8,	vs30
	xvmaddadp		vs49,	vs9,	vs30

	xvmaddadp		vs56,	vs8,	vs31
	xvmaddadp		vs57,	vs9,	vs31

.endm

.macro KERNEL4x4_SUBI1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO
	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 32
	addi		BO, BO, 32


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25

	xvmuldp			vs48,	vs0,	vs26
	xvmuldp			vs49,	vs1,	vs26

	xvmuldp			vs56,	vs0,	vs27
	xvmuldp			vs57,	vs1,	vs27

.endm

.macro KERNEL4x4_SUB1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO
	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 32
	addi		BO, BO, 32


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25

	xvmaddadp		vs48,	vs0,	vs26
	xvmaddadp		vs49,	vs1,	vs26

	xvmaddadp		vs56,	vs0,	vs27
	xvmaddadp		vs57,	vs1,	vs27

.endm

.macro SAVE4x4

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
	lxvd2x		vs1,	o16,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
	xvmaddadp	vs1,	vs33,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
	xvmuldp		vs1,	vs33,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1
	stxvd2x		vs1,	o16,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs8,	0,	T1
	lxvd2x		vs9,	o16,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs8,	vs40,	alpha_r
	xvmaddadp	vs9,	vs41,	alpha_r
#else
	xvmuldp		vs8,	vs40,	alpha_r
	xvmuldp		vs9,	vs41,	alpha_r
#endif

	stxvd2x		vs8,	0,	T1
	stxvd2x		vs9,	o16,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
	lxvd2x		vs1,	o16,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs48,	alpha_r
	xvmaddadp	vs1,	vs49,	alpha_r
#else
	xvmuldp		vs0,	vs48,	alpha_r
	xvmuldp		vs1,	vs49,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1
	stxvd2x		vs1,	o16,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs8,	0,	T1
	lxvd2x		vs9,	o16,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs8,	vs56,	alpha_r
	xvmaddadp	vs9,	vs57,	alpha_r
#else
	xvmuldp		vs8,	vs56,	alpha_r
	xvmuldp		vs9,	vs57,	alpha_r
#endif

	stxvd2x		vs8,	0,	T1
	stxvd2x		vs9,	o16,	T1

	addi		CO,	CO,	32

.endm

/*********************************************************************
* Macros for N=4, M=2                                                *
*********************************************************************/

.macro LOAD4x2_1

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO
	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 16
	addi		BO, BO, 32

.endm

.macro KERNEL4x2_I1

	lxvd2x	vs8,	0,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO
	lxvdsx	vs30,	o16,	BO
	lxvdsx	vs31,	o24,	BO

	addi		AO, AO, 16
	addi		BO, BO, 32


	xvmuldp			vs32,	vs0,	vs24

	xvmuldp			vs40,	vs0,	vs25

	xvmuldp			vs48,	vs0,	vs26

	xvmuldp			vs56,	vs0,	vs27

.endm

.macro KERNEL4x2_1

	lxvd2x	vs8,	0,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO
	lxvdsx	vs30,	o16,	BO
	lxvdsx	vs31,	o24,	BO

	addi		AO, AO, 16
	addi		BO, BO, 32


	xvmaddadp		vs32,	vs0,	vs24

	xvmaddadp		vs40,	vs0,	vs25

	xvmaddadp		vs48,	vs0,	vs26

	xvmaddadp		vs56,	vs0,	vs27

.endm

.macro KERNEL4x2_2

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO
	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 16
	addi		BO, BO, 32


	xvmaddadp		vs32,	vs8,	vs28

	xvmaddadp		vs40,	vs8,	vs29

	xvmaddadp		vs48,	vs8,	vs30

	xvmaddadp		vs56,	vs8,	vs31

.endm

.macro KERNEL4x2_E2


	xvmaddadp		vs32,	vs8,	vs28

	xvmaddadp		vs40,	vs8,	vs29

	xvmaddadp		vs48,	vs8,	vs30

	xvmaddadp		vs56,	vs8,	vs31

.endm

.macro KERNEL4x2_SUBI1

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO
	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 16
	addi		BO, BO, 32


	xvmuldp			vs32,	vs0,	vs24

	xvmuldp			vs40,	vs0,	vs25

	xvmuldp			vs48,	vs0,	vs26

	xvmuldp			vs56,	vs0,	vs27

.endm

.macro KERNEL4x2_SUB1

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO
	lxvdsx	vs26,	o16,	BO
	lxvdsx	vs27,	o24,	BO

	addi		AO, AO, 16
	addi		BO, BO, 32


	xvmaddadp		vs32,	vs0,	vs24

	xvmaddadp		vs40,	vs0,	vs25

	xvmaddadp		vs48,	vs0,	vs26

	xvmaddadp		vs56,	vs0,	vs27

.endm

.macro SAVE4x2

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs8,	0,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs8,	vs40,	alpha_r
#else
	xvmuldp		vs8,	vs40,	alpha_r
#endif

	stxvd2x		vs8,	0,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs48,	alpha_r
#else
	xvmuldp		vs0,	vs48,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs8,	0,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs8,	vs56,	alpha_r
#else
	xvmuldp		vs8,	vs56,	alpha_r
#endif

	stxvd2x		vs8,	0,	T1

	addi		CO,	CO,	16

.endm

/*********************************************************************
* Macros for N=4, M=1                                                *
*********************************************************************/

.macro LOAD4x1_1

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO
	lxsdx	vs25,	o8,	BO
	lxsdx	vs26,	o16,	BO
	lxsdx	vs27,	o24,	BO

	addi		AO, AO, 8
	addi		BO, BO, 32

.endm

.macro KERNEL4x1_I1

	lxsdx	vs8,	0,	AO

	lxsdx	vs28,	0,	BO
	lxsdx	vs29,	o8,	BO
	lxsdx	vs30,	o16,	BO
	lxsdx	vs31,	o24,	BO

	addi		AO, AO, 8
	addi		BO, BO, 32


	xsmuldp			vs32,	vs0,	vs24

	xsmuldp			vs40,	vs0,	vs25

	xsmuldp			vs48,	vs0,	vs26

	xsmuldp			vs56,	vs0,	vs27

.endm

.macro KERNEL4x1_1

	lxsdx	vs8,	0,	AO

	lxsdx	vs28,	0,	BO
	lxsdx	vs29,	o8,	BO
	lxsdx	vs30,	o16,	BO
	lxsdx	vs31,	o24,	BO

	addi		AO, AO, 8
	addi		BO, BO, 32


	xsmaddadp		vs32,	vs0,	vs24

	xsmaddadp		vs40,	vs0,	vs25

	xsmaddadp		vs48,	vs0,	vs26

	xsmaddadp		vs56,	vs0,	vs27

.endm

.macro KERNEL4x1_2

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO
	lxsdx	vs25,	o8,	BO
	lxsdx	vs26,	o16,	BO
	lxsdx	vs27,	o24,	BO

	addi		AO, AO, 8
	addi		BO, BO, 32


	xsmaddadp		vs32,	vs8,	vs28

	xsmaddadp		vs40,	vs8,	vs29

	xsmaddadp		vs48,	vs8,	vs30

	xsmaddadp		vs56,	vs8,	vs31

.endm

.macro KERNEL4x1_E2


	xsmaddadp		vs32,	vs8,	vs28

	xsmaddadp		vs40,	vs8,	vs29

	xsmaddadp		vs48,	vs8,	vs30

	xsmaddadp		vs56,	vs8,	vs31

.endm

.macro KERNEL4x1_SUBI1

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO
	lxsdx	vs25,	o8,	BO
	lxsdx	vs26,	o16,	BO
	lxsdx	vs27,	o24,	BO

	addi		AO, AO, 8
	addi		BO, BO, 32


	xsmuldp			vs32,	vs0,	vs24

	xsmuldp			vs40,	vs0,	vs25

	xsmuldp			vs48,	vs0,	vs26

	xsmuldp			vs56,	vs0,	vs27

.endm

.macro KERNEL4x1_SUB1

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO
	lxsdx	vs25,	o8,	BO
	lxsdx	vs26,	o16,	BO
	lxsdx	vs27,	o24,	BO

	addi		AO, AO, 8
	addi		BO, BO, 32


	xsmaddadp		vs32,	vs0,	vs24

	xsmaddadp		vs40,	vs0,	vs25

	xsmaddadp		vs48,	vs0,	vs26

	xsmaddadp		vs56,	vs0,	vs27

.endm

.macro SAVE4x1

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxsdx		vs0,	0,	T1
#endif

#ifndef TRMMKERNEL
	xsmaddadp	vs0,	vs32,	alpha_r
#else
	xsmuldp		vs0,	vs32,	alpha_r
#endif

	stxsdx		vs0,	0,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxsdx		vs8,	0,	T1
#endif

#ifndef TRMMKERNEL
	xsmaddadp	vs8,	vs40,	alpha_r
#else
	xsmuldp		vs8,	vs40,	alpha_r
#endif

	stxsdx		vs8,	0,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxsdx		vs0,	0,	T1
#endif

#ifndef TRMMKERNEL
	xsmaddadp	vs0,	vs48,	alpha_r
#else
	xsmuldp		vs0,	vs48,	alpha_r
#endif

	stxsdx		vs0,	0,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxsdx		vs8,	0,	T1
#endif

#ifndef TRMMKERNEL
	xsmaddadp	vs8,	vs56,	alpha_r
#else
	xsmuldp		vs8,	vs56,	alpha_r
#endif

	stxsdx		vs8,	0,	T1

	addi		CO,	CO,	8

.endm

/*********************************************************************
* Macros for N=2, M=16                                               *
*********************************************************************/

.macro LOAD2x16_1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16

	lxvd2x	vs4,	0,	AO
	lxvd2x	vs5,	o16,	AO
	lxvd2x	vs6,	o32,	AO
	lxvd2x	vs7,	o48,	AO

	addi		AO, AO, 64

.endm

.macro KERNEL2x16_I1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO
	lxvd2x	vs10,	o32,	AO
	lxvd2x	vs11,	o48,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16

	lxvd2x	vs12,	0,	AO
	lxvd2x	vs13,	o16,	AO
	lxvd2x	vs14,	o32,	AO
	lxvd2x	vs15,	o48,	AO

	addi		AO, AO, 64


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24
	xvmuldp			vs36,	vs4,	vs24
	xvmuldp			vs37,	vs5,	vs24
	xvmuldp			vs38,	vs6,	vs24
	xvmuldp			vs39,	vs7,	vs24

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25
	xvmuldp			vs42,	vs2,	vs25
	xvmuldp			vs43,	vs3,	vs25
	xvmuldp			vs44,	vs4,	vs25
	xvmuldp			vs45,	vs5,	vs25
	xvmuldp			vs46,	vs6,	vs25
	xvmuldp			vs47,	vs7,	vs25

.endm

.macro KERNEL2x16_1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO
	lxvd2x	vs10,	o32,	AO
	lxvd2x	vs11,	o48,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16

	lxvd2x	vs12,	0,	AO
	lxvd2x	vs13,	o16,	AO
	lxvd2x	vs14,	o32,	AO
	lxvd2x	vs15,	o48,	AO

	addi		AO, AO, 64


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24
	xvmaddadp		vs36,	vs4,	vs24
	xvmaddadp		vs37,	vs5,	vs24
	xvmaddadp		vs38,	vs6,	vs24
	xvmaddadp		vs39,	vs7,	vs24

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25
	xvmaddadp		vs42,	vs2,	vs25
	xvmaddadp		vs43,	vs3,	vs25
	xvmaddadp		vs44,	vs4,	vs25
	xvmaddadp		vs45,	vs5,	vs25
	xvmaddadp		vs46,	vs6,	vs25
	xvmaddadp		vs47,	vs7,	vs25

.endm

.macro KERNEL2x16_2

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16

	lxvd2x	vs4,	0,	AO
	lxvd2x	vs5,	o16,	AO
	lxvd2x	vs6,	o32,	AO
	lxvd2x	vs7,	o48,	AO

	addi		AO, AO, 64


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28
	xvmaddadp		vs36,	vs12,	vs28
	xvmaddadp		vs37,	vs13,	vs28
	xvmaddadp		vs38,	vs14,	vs28
	xvmaddadp		vs39,	vs15,	vs28

	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29
	xvmaddadp		vs42,	vs10,	vs29
	xvmaddadp		vs43,	vs11,	vs29
	xvmaddadp		vs44,	vs12,	vs29
	xvmaddadp		vs45,	vs13,	vs29
	xvmaddadp		vs46,	vs14,	vs29
	xvmaddadp		vs47,	vs15,	vs29

.endm

.macro KERNEL2x16_E2


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28
	xvmaddadp		vs36,	vs12,	vs28
	xvmaddadp		vs37,	vs13,	vs28
	xvmaddadp		vs38,	vs14,	vs28
	xvmaddadp		vs39,	vs15,	vs28

	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29
	xvmaddadp		vs42,	vs10,	vs29
	xvmaddadp		vs43,	vs11,	vs29
	xvmaddadp		vs44,	vs12,	vs29
	xvmaddadp		vs45,	vs13,	vs29
	xvmaddadp		vs46,	vs14,	vs29
	xvmaddadp		vs47,	vs15,	vs29

.endm

.macro KERNEL2x16_SUBI1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16

	lxvd2x	vs4,	0,	AO
	lxvd2x	vs5,	o16,	AO
	lxvd2x	vs6,	o32,	AO
	lxvd2x	vs7,	o48,	AO

	addi		AO, AO, 64


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24
	xvmuldp			vs36,	vs4,	vs24
	xvmuldp			vs37,	vs5,	vs24
	xvmuldp			vs38,	vs6,	vs24
	xvmuldp			vs39,	vs7,	vs24

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25
	xvmuldp			vs42,	vs2,	vs25
	xvmuldp			vs43,	vs3,	vs25
	xvmuldp			vs44,	vs4,	vs25
	xvmuldp			vs45,	vs5,	vs25
	xvmuldp			vs46,	vs6,	vs25
	xvmuldp			vs47,	vs7,	vs25

.endm

.macro KERNEL2x16_SUB1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16

	lxvd2x	vs4,	0,	AO
	lxvd2x	vs5,	o16,	AO
	lxvd2x	vs6,	o32,	AO
	lxvd2x	vs7,	o48,	AO

	addi		AO, AO, 64


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24
	xvmaddadp		vs36,	vs4,	vs24
	xvmaddadp		vs37,	vs5,	vs24
	xvmaddadp		vs38,	vs6,	vs24
	xvmaddadp		vs39,	vs7,	vs24

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25
	xvmaddadp		vs42,	vs2,	vs25
	xvmaddadp		vs43,	vs3,	vs25
	xvmaddadp		vs44,	vs4,	vs25
	xvmaddadp		vs45,	vs5,	vs25
	xvmaddadp		vs46,	vs6,	vs25
	xvmaddadp		vs47,	vs7,	vs25

.endm

.macro SAVE2x16

	mr		T1,	CO
	addi		T2,	T1,	64

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
	lxvd2x		vs1,	o16,	T1
	lxvd2x		vs2,	o32,	T1
	lxvd2x		vs3,	o48,	T1

	lxvd2x		vs4,	0,	T2
	lxvd2x		vs5,	o16,	T2
	lxvd2x		vs6,	o32,	T2
	lxvd2x		vs7,	o48,	T2
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
	xvmaddadp	vs1,	vs33,	alpha_r
	xvmaddadp	vs2,	vs34,	alpha_r
	xvmaddadp	vs3,	vs35,	alpha_r
	xvmaddadp	vs4,	vs36,	alpha_r
	xvmaddadp	vs5,	vs37,	alpha_r
	xvmaddadp	vs6,	vs38,	alpha_r
	xvmaddadp	vs7,	vs39,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
	xvmuldp		vs1,	vs33,	alpha_r
	xvmuldp		vs2,	vs34,	alpha_r
	xvmuldp		vs3,	vs35,	alpha_r
	xvmuldp		vs4,	vs36,	alpha_r
	xvmuldp		vs5,	vs37,	alpha_r
	xvmuldp		vs6,	vs38,	alpha_r
	xvmuldp		vs7,	vs39,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1
	stxvd2x		vs1,	o16,	T1
	stxvd2x		vs2,	o32,	T1
	stxvd2x		vs3,	o48,	T1

	stxvd2x		vs4,	0,	T2
	stxvd2x		vs5,	o16,	T2
	stxvd2x		vs6,	o32,	T2
	stxvd2x		vs7,	o48,	T2

	add		T1,	T1,	LDC
	add		T2,	T2,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs8,	0,	T1
	lxvd2x		vs9,	o16,	T1
	lxvd2x		vs10,	o32,	T1
	lxvd2x		vs11,	o48,	T1

	lxvd2x		vs12,	0,	T2
	lxvd2x		vs13,	o16,	T2
	lxvd2x		vs14,	o32,	T2
	lxvd2x		vs15,	o48,	T2
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs8,	vs40,	alpha_r
	xvmaddadp	vs9,	vs41,	alpha_r
	xvmaddadp	vs10,	vs42,	alpha_r
	xvmaddadp	vs11,	vs43,	alpha_r
	xvmaddadp	vs12,	vs44,	alpha_r
	xvmaddadp	vs13,	vs45,	alpha_r
	xvmaddadp	vs14,	vs46,	alpha_r
	xvmaddadp	vs15,	vs47,	alpha_r
#else
	xvmuldp		vs8,	vs40,	alpha_r
	xvmuldp		vs9,	vs41,	alpha_r
	xvmuldp		vs10,	vs42,	alpha_r
	xvmuldp		vs11,	vs43,	alpha_r
	xvmuldp		vs12,	vs44,	alpha_r
	xvmuldp		vs13,	vs45,	alpha_r
	xvmuldp		vs14,	vs46,	alpha_r
	xvmuldp		vs15,	vs47,	alpha_r
#endif

	stxvd2x		vs8,	0,	T1
	stxvd2x		vs9,	o16,	T1
	stxvd2x		vs10,	o32,	T1
	stxvd2x		vs11,	o48,	T1

	stxvd2x		vs12,	0,	T2
	stxvd2x		vs13,	o16,	T2
	stxvd2x		vs14,	o32,	T2
	stxvd2x		vs15,	o48,	T2

	addi		CO,	CO,	128

.endm

/*********************************************************************
* Macros for N=4, M=8                                                *
*********************************************************************/

.macro LOAD2x8_1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16

.endm

.macro KERNEL2x8_I1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO
	lxvd2x	vs10,	o32,	AO
	lxvd2x	vs11,	o48,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25
	xvmuldp			vs42,	vs2,	vs25
	xvmuldp			vs43,	vs3,	vs25

.endm

.macro KERNEL2x8_1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO
	lxvd2x	vs10,	o32,	AO
	lxvd2x	vs11,	o48,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25
	xvmaddadp		vs42,	vs2,	vs25
	xvmaddadp		vs43,	vs3,	vs25

.endm

.macro KERNEL2x8_2

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28

	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29
	xvmaddadp		vs42,	vs10,	vs29
	xvmaddadp		vs43,	vs11,	vs29

.endm

.macro KERNEL2x8_E2


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28

	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29
	xvmaddadp		vs42,	vs10,	vs29
	xvmaddadp		vs43,	vs11,	vs29

.endm

.macro KERNEL2x8_SUBI1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25
	xvmuldp			vs42,	vs2,	vs25
	xvmuldp			vs43,	vs3,	vs25

.endm

.macro KERNEL2x8_SUB1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 64
	addi		BO, BO, 16


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25
	xvmaddadp		vs42,	vs2,	vs25
	xvmaddadp		vs43,	vs3,	vs25

.endm

.macro SAVE2x8

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
	lxvd2x		vs1,	o16,	T1
	lxvd2x		vs2,	o32,	T1
	lxvd2x		vs3,	o48,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
	xvmaddadp	vs1,	vs33,	alpha_r
	xvmaddadp	vs2,	vs34,	alpha_r
	xvmaddadp	vs3,	vs35,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
	xvmuldp		vs1,	vs33,	alpha_r
	xvmuldp		vs2,	vs34,	alpha_r
	xvmuldp		vs3,	vs35,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1
	stxvd2x		vs1,	o16,	T1
	stxvd2x		vs2,	o32,	T1
	stxvd2x		vs3,	o48,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs8,	0,	T1
	lxvd2x		vs9,	o16,	T1
	lxvd2x		vs10,	o32,	T1
	lxvd2x		vs11,	o48,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs8,	vs40,	alpha_r
	xvmaddadp	vs9,	vs41,	alpha_r
	xvmaddadp	vs10,	vs42,	alpha_r
	xvmaddadp	vs11,	vs43,	alpha_r
#else
	xvmuldp		vs8,	vs40,	alpha_r
	xvmuldp		vs9,	vs41,	alpha_r
	xvmuldp		vs10,	vs42,	alpha_r
	xvmuldp		vs11,	vs43,	alpha_r
#endif

	stxvd2x		vs8,	0,	T1
	stxvd2x		vs9,	o16,	T1
	stxvd2x		vs10,	o32,	T1
	stxvd2x		vs11,	o48,	T1

	addi		CO,	CO,	64

.endm

/*********************************************************************
* Macros for N=2, M=4                                                *
*********************************************************************/

.macro LOAD2x4_1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 32
	addi		BO, BO, 16

.endm

.macro KERNEL2x4_I1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO

	addi		AO, AO, 32
	addi		BO, BO, 16


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25

.endm

.macro KERNEL2x4_1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO

	addi		AO, AO, 32
	addi		BO, BO, 16


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25

.endm

.macro KERNEL2x4_2

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 32
	addi		BO, BO, 16


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28

	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29

.endm

.macro KERNEL2x4_E2


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28

	xvmaddadp		vs40,	vs8,	vs29
	xvmaddadp		vs41,	vs9,	vs29

.endm

.macro KERNEL2x4_SUBI1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 32
	addi		BO, BO, 16


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24

	xvmuldp			vs40,	vs0,	vs25
	xvmuldp			vs41,	vs1,	vs25

.endm

.macro KERNEL2x4_SUB1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 32
	addi		BO, BO, 16


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24

	xvmaddadp		vs40,	vs0,	vs25
	xvmaddadp		vs41,	vs1,	vs25

.endm

.macro SAVE2x4

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
	lxvd2x		vs1,	o16,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
	xvmaddadp	vs1,	vs33,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
	xvmuldp		vs1,	vs33,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1
	stxvd2x		vs1,	o16,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs8,	0,	T1
	lxvd2x		vs9,	o16,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs8,	vs40,	alpha_r
	xvmaddadp	vs9,	vs41,	alpha_r
#else
	xvmuldp		vs8,	vs40,	alpha_r
	xvmuldp		vs9,	vs41,	alpha_r
#endif

	stxvd2x		vs8,	0,	T1
	stxvd2x		vs9,	o16,	T1

	addi		CO,	CO,	32

.endm

/*********************************************************************
* Macros for N=2, M=2                                                *
*********************************************************************/

.macro LOAD2x2_1

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 16
	addi		BO, BO, 16

.endm

.macro KERNEL2x2_I1

	lxvd2x	vs8,	0,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO

	addi		AO, AO, 16
	addi		BO, BO, 16


	xvmuldp			vs32,	vs0,	vs24

	xvmuldp			vs40,	vs0,	vs25

.endm

.macro KERNEL2x2_1

	lxvd2x	vs8,	0,	AO

	lxvdsx	vs28,	0,	BO
	lxvdsx	vs29,	o8,	BO

	addi		AO, AO, 16
	addi		BO, BO, 16


	xvmaddadp		vs32,	vs0,	vs24

	xvmaddadp		vs40,	vs0,	vs25

.endm

.macro KERNEL2x2_2

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 16
	addi		BO, BO, 16


	xvmaddadp		vs32,	vs8,	vs28

	xvmaddadp		vs40,	vs8,	vs29

.endm

.macro KERNEL2x2_E2


	xvmaddadp		vs32,	vs8,	vs28

	xvmaddadp		vs40,	vs8,	vs29

.endm

.macro KERNEL2x2_SUBI1

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 16
	addi		BO, BO, 16


	xvmuldp			vs32,	vs0,	vs24

	xvmuldp			vs40,	vs0,	vs25

.endm

.macro KERNEL2x2_SUB1

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO
	lxvdsx	vs25,	o8,	BO

	addi		AO, AO, 16
	addi		BO, BO, 16


	xvmaddadp		vs32,	vs0,	vs24

	xvmaddadp		vs40,	vs0,	vs25

.endm

.macro SAVE2x2

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxvd2x		vs8,	0,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs8,	vs40,	alpha_r
#else
	xvmuldp		vs8,	vs40,	alpha_r
#endif

	stxvd2x		vs8,	0,	T1

	addi		CO,	CO,	16

.endm

/*********************************************************************
* Macros for N=2, M=1                                                *
*********************************************************************/

.macro LOAD2x1_1

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO
	lxsdx	vs25,	o8,	BO

	addi		AO, AO, 8
	addi		BO, BO, 16

.endm

.macro KERNEL2x1_I1

	lxsdx	vs8,	0,	AO

	lxsdx	vs28,	0,	BO
	lxsdx	vs29,	o8,	BO

	addi		AO, AO, 8
	addi		BO, BO, 16


	xsmuldp			vs32,	vs0,	vs24

	xsmuldp			vs40,	vs0,	vs25

.endm

.macro KERNEL2x1_1

	lxsdx	vs8,	0,	AO

	lxsdx	vs28,	0,	BO
	lxsdx	vs29,	o8,	BO

	addi		AO, AO, 8
	addi		BO, BO, 16


	xsmaddadp		vs32,	vs0,	vs24

	xsmaddadp		vs40,	vs0,	vs25

.endm

.macro KERNEL2x1_2

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO
	lxsdx	vs25,	o8,	BO

	addi		AO, AO, 8
	addi		BO, BO, 16


	xsmaddadp		vs32,	vs8,	vs28

	xsmaddadp		vs40,	vs8,	vs29

.endm

.macro KERNEL2x1_E2


	xsmaddadp		vs32,	vs8,	vs28

	xsmaddadp		vs40,	vs8,	vs29

.endm

.macro KERNEL2x1_SUBI1

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO
	lxsdx	vs25,	o8,	BO

	addi		AO, AO, 8
	addi		BO, BO, 16


	xsmuldp			vs32,	vs0,	vs24

	xsmuldp			vs40,	vs0,	vs25

.endm

.macro KERNEL2x1_SUB1

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO
	lxsdx	vs25,	o8,	BO

	addi		AO, AO, 8
	addi		BO, BO, 16


	xsmaddadp		vs32,	vs0,	vs24

	xsmaddadp		vs40,	vs0,	vs25

.endm

.macro SAVE2x1

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxsdx		vs0,	0,	T1
#endif

#ifndef TRMMKERNEL
	xsmaddadp	vs0,	vs32,	alpha_r
#else
	xsmuldp		vs0,	vs32,	alpha_r
#endif

	stxsdx		vs0,	0,	T1

	add		T1,	T1,	LDC

#ifndef TRMMKERNEL
	lxsdx		vs8,	0,	T1
#endif

#ifndef TRMMKERNEL
	xsmaddadp	vs8,	vs40,	alpha_r
#else
	xsmuldp		vs8,	vs40,	alpha_r
#endif

	stxsdx		vs8,	0,	T1

	addi		CO,	CO,	8

.endm

/*********************************************************************
* Macros for N=1, M=16                                               *
*********************************************************************/

.macro LOAD1x16_1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8

	lxvd2x	vs4,	0,	AO
	lxvd2x	vs5,	o16,	AO
	lxvd2x	vs6,	o32,	AO
	lxvd2x	vs7,	o48,	AO

	addi		AO, AO, 64

.endm

.macro KERNEL1x16_I1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO
	lxvd2x	vs10,	o32,	AO
	lxvd2x	vs11,	o48,	AO

	lxvdsx	vs28,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8

	lxvd2x	vs12,	0,	AO
	lxvd2x	vs13,	o16,	AO
	lxvd2x	vs14,	o32,	AO
	lxvd2x	vs15,	o48,	AO

	addi		AO, AO, 64


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24
	xvmuldp			vs36,	vs4,	vs24
	xvmuldp			vs37,	vs5,	vs24
	xvmuldp			vs38,	vs6,	vs24
	xvmuldp			vs39,	vs7,	vs24

.endm

.macro KERNEL1x16_1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO
	lxvd2x	vs10,	o32,	AO
	lxvd2x	vs11,	o48,	AO

	lxvdsx	vs28,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8

	lxvd2x	vs12,	0,	AO
	lxvd2x	vs13,	o16,	AO
	lxvd2x	vs14,	o32,	AO
	lxvd2x	vs15,	o48,	AO

	addi		AO, AO, 64


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24
	xvmaddadp		vs36,	vs4,	vs24
	xvmaddadp		vs37,	vs5,	vs24
	xvmaddadp		vs38,	vs6,	vs24
	xvmaddadp		vs39,	vs7,	vs24

.endm

.macro KERNEL1x16_2

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8

	lxvd2x	vs4,	0,	AO
	lxvd2x	vs5,	o16,	AO
	lxvd2x	vs6,	o32,	AO
	lxvd2x	vs7,	o48,	AO

	addi		AO, AO, 64


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28
	xvmaddadp		vs36,	vs12,	vs28
	xvmaddadp		vs37,	vs13,	vs28
	xvmaddadp		vs38,	vs14,	vs28
	xvmaddadp		vs39,	vs15,	vs28

.endm

.macro KERNEL1x16_E2


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28
	xvmaddadp		vs36,	vs12,	vs28
	xvmaddadp		vs37,	vs13,	vs28
	xvmaddadp		vs38,	vs14,	vs28
	xvmaddadp		vs39,	vs15,	vs28

.endm

.macro KERNEL1x16_SUBI1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8

	lxvd2x	vs4,	0,	AO
	lxvd2x	vs5,	o16,	AO
	lxvd2x	vs6,	o32,	AO
	lxvd2x	vs7,	o48,	AO

	addi		AO, AO, 64


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24
	xvmuldp			vs36,	vs4,	vs24
	xvmuldp			vs37,	vs5,	vs24
	xvmuldp			vs38,	vs6,	vs24
	xvmuldp			vs39,	vs7,	vs24

.endm

.macro KERNEL1x16_SUB1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8

	lxvd2x	vs4,	0,	AO
	lxvd2x	vs5,	o16,	AO
	lxvd2x	vs6,	o32,	AO
	lxvd2x	vs7,	o48,	AO

	addi		AO, AO, 64


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24
	xvmaddadp		vs36,	vs4,	vs24
	xvmaddadp		vs37,	vs5,	vs24
	xvmaddadp		vs38,	vs6,	vs24
	xvmaddadp		vs39,	vs7,	vs24

.endm

.macro SAVE1x16

	mr		T1,	CO
	addi		T2,	T1,	64

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
	lxvd2x		vs1,	o16,	T1
	lxvd2x		vs2,	o32,	T1
	lxvd2x		vs3,	o48,	T1

	lxvd2x		vs4,	0,	T2
	lxvd2x		vs5,	o16,	T2
	lxvd2x		vs6,	o32,	T2
	lxvd2x		vs7,	o48,	T2
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
	xvmaddadp	vs1,	vs33,	alpha_r
	xvmaddadp	vs2,	vs34,	alpha_r
	xvmaddadp	vs3,	vs35,	alpha_r
	xvmaddadp	vs4,	vs36,	alpha_r
	xvmaddadp	vs5,	vs37,	alpha_r
	xvmaddadp	vs6,	vs38,	alpha_r
	xvmaddadp	vs7,	vs39,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
	xvmuldp		vs1,	vs33,	alpha_r
	xvmuldp		vs2,	vs34,	alpha_r
	xvmuldp		vs3,	vs35,	alpha_r
	xvmuldp		vs4,	vs36,	alpha_r
	xvmuldp		vs5,	vs37,	alpha_r
	xvmuldp		vs6,	vs38,	alpha_r
	xvmuldp		vs7,	vs39,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1
	stxvd2x		vs1,	o16,	T1
	stxvd2x		vs2,	o32,	T1
	stxvd2x		vs3,	o48,	T1

	stxvd2x		vs4,	0,	T2
	stxvd2x		vs5,	o16,	T2
	stxvd2x		vs6,	o32,	T2
	stxvd2x		vs7,	o48,	T2

	addi		CO,	CO,	128

.endm

/*********************************************************************
* Macros for N=4, M=8                                                *
*********************************************************************/

.macro LOAD1x8_1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8

.endm

.macro KERNEL1x8_I1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO
	lxvd2x	vs10,	o32,	AO
	lxvd2x	vs11,	o48,	AO

	lxvdsx	vs28,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24

.endm

.macro KERNEL1x8_1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO
	lxvd2x	vs10,	o32,	AO
	lxvd2x	vs11,	o48,	AO

	lxvdsx	vs28,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24

.endm

.macro KERNEL1x8_2

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28

.endm

.macro KERNEL1x8_E2


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28
	xvmaddadp		vs34,	vs10,	vs28
	xvmaddadp		vs35,	vs11,	vs28

.endm

.macro KERNEL1x8_SUBI1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24
	xvmuldp			vs34,	vs2,	vs24
	xvmuldp			vs35,	vs3,	vs24

.endm

.macro KERNEL1x8_SUB1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO
	lxvd2x	vs2,	o32,	AO
	lxvd2x	vs3,	o48,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 64
	addi		BO, BO, 8


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24
	xvmaddadp		vs34,	vs2,	vs24
	xvmaddadp		vs35,	vs3,	vs24

.endm

.macro SAVE1x8

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
	lxvd2x		vs1,	o16,	T1
	lxvd2x		vs2,	o32,	T1
	lxvd2x		vs3,	o48,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
	xvmaddadp	vs1,	vs33,	alpha_r
	xvmaddadp	vs2,	vs34,	alpha_r
	xvmaddadp	vs3,	vs35,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
	xvmuldp		vs1,	vs33,	alpha_r
	xvmuldp		vs2,	vs34,	alpha_r
	xvmuldp		vs3,	vs35,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1
	stxvd2x		vs1,	o16,	T1
	stxvd2x		vs2,	o32,	T1
	stxvd2x		vs3,	o48,	T1

	addi		CO,	CO,	64

.endm

/*********************************************************************
* Macros for N=1, M=4                                                *
*********************************************************************/

.macro LOAD1x4_1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 32
	addi		BO, BO, 8

.endm

.macro KERNEL1x4_I1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO

	lxvdsx	vs28,	0,	BO

	addi		AO, AO, 32
	addi		BO, BO, 8


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24

.endm

.macro KERNEL1x4_1

	lxvd2x	vs8,	0,	AO
	lxvd2x	vs9,	o16,	AO

	lxvdsx	vs28,	0,	BO

	addi		AO, AO, 32
	addi		BO, BO, 8


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24

.endm

.macro KERNEL1x4_2

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 32
	addi		BO, BO, 8


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28

.endm

.macro KERNEL1x4_E2


	xvmaddadp		vs32,	vs8,	vs28
	xvmaddadp		vs33,	vs9,	vs28

.endm

.macro KERNEL1x4_SUBI1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 32
	addi		BO, BO, 8


	xvmuldp			vs32,	vs0,	vs24
	xvmuldp			vs33,	vs1,	vs24

.endm

.macro KERNEL1x4_SUB1

	lxvd2x	vs0,	0,	AO
	lxvd2x	vs1,	o16,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 32
	addi		BO, BO, 8


	xvmaddadp		vs32,	vs0,	vs24
	xvmaddadp		vs33,	vs1,	vs24

.endm

.macro SAVE1x4

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
	lxvd2x		vs1,	o16,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
	xvmaddadp	vs1,	vs33,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
	xvmuldp		vs1,	vs33,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1
	stxvd2x		vs1,	o16,	T1

	addi		CO,	CO,	32

.endm

/*********************************************************************
* Macros for N=1, M=2                                                *
*********************************************************************/

.macro LOAD1x2_1

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 16
	addi		BO, BO, 8

.endm

.macro KERNEL1x2_I1

	lxvd2x	vs8,	0,	AO

	lxvdsx	vs28,	0,	BO

	addi		AO, AO, 16
	addi		BO, BO, 8


	xvmuldp			vs32,	vs0,	vs24

.endm

.macro KERNEL1x2_1

	lxvd2x	vs8,	0,	AO

	lxvdsx	vs28,	0,	BO

	addi		AO, AO, 16
	addi		BO, BO, 8


	xvmaddadp		vs32,	vs0,	vs24

.endm

.macro KERNEL1x2_2

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 16
	addi		BO, BO, 8


	xvmaddadp		vs32,	vs8,	vs28

.endm

.macro KERNEL1x2_E2


	xvmaddadp		vs32,	vs8,	vs28

.endm

.macro KERNEL1x2_SUBI1

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 16
	addi		BO, BO, 8


	xvmuldp			vs32,	vs0,	vs24

.endm

.macro KERNEL1x2_SUB1

	lxvd2x	vs0,	0,	AO

	lxvdsx	vs24,	0,	BO

	addi		AO, AO, 16
	addi		BO, BO, 8


	xvmaddadp		vs32,	vs0,	vs24

.endm

.macro SAVE1x2

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxvd2x		vs0,	0,	T1
#endif

#ifndef TRMMKERNEL
	xvmaddadp	vs0,	vs32,	alpha_r
#else
	xvmuldp		vs0,	vs32,	alpha_r
#endif

	stxvd2x		vs0,	0,	T1

	addi		CO,	CO,	16

.endm

/*********************************************************************
* Macros for N=1, M=1                                                *
*********************************************************************/

.macro LOAD1x1_1

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO

	addi		AO, AO, 8
	addi		BO, BO, 8

.endm

.macro KERNEL1x1_I1

	lxsdx	vs8,	0,	AO

	lxsdx	vs28,	0,	BO

	addi		AO, AO, 8
	addi		BO, BO, 8


	xsmuldp			vs32,	vs0,	vs24

.endm

.macro KERNEL1x1_1

	lxsdx	vs8,	0,	AO

	lxsdx	vs28,	0,	BO

	addi		AO, AO, 8
	addi		BO, BO, 8


	xsmaddadp		vs32,	vs0,	vs24

.endm

.macro KERNEL1x1_2

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO

	addi		AO, AO, 8
	addi		BO, BO, 8


	xsmaddadp		vs32,	vs8,	vs28

.endm

.macro KERNEL1x1_E2


	xsmaddadp		vs32,	vs8,	vs28

.endm

.macro KERNEL1x1_SUBI1

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO

	addi		AO, AO, 8
	addi		BO, BO, 8


	xsmuldp			vs32,	vs0,	vs24

.endm

.macro KERNEL1x1_SUB1

	lxsdx	vs0,	0,	AO

	lxsdx	vs24,	0,	BO

	addi		AO, AO, 8
	addi		BO, BO, 8


	xsmaddadp		vs32,	vs0,	vs24

.endm

.macro SAVE1x1

	mr		T1,	CO

#ifndef TRMMKERNEL
	lxsdx		vs0,	0,	T1
#endif

#ifndef TRMMKERNEL
	xsmaddadp	vs0,	vs32,	alpha_r
#else
	xsmuldp		vs0,	vs32,	alpha_r
#endif

	stxsdx		vs0,	0,	T1

	addi		CO,	CO,	8

.endm

