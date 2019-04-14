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
* Macros for N=4 and M=16
**********************************************************************************************/

.macro COPY_4x16

	lxvw4x		vs32,	o0,	A0
	lxvw4x		vs33,	o16,	A0
	lxvw4x		vs34,	o32,	A0
	lxvw4x		vs35,	o48,	A0

	lxvw4x		vs36,	o0,	A1
	lxvw4x		vs37,	o16,	A1
	lxvw4x		vs38,	o32,	A1
	lxvw4x		vs39,	o48,	A1

	lxvw4x		vs40,	o0,	A2
	lxvw4x		vs41,	o16,	A2
	lxvw4x		vs42,	o32,	A2
	lxvw4x		vs43,	o48,	A2

	lxvw4x		vs44,	o0,	A3
	lxvw4x		vs45,	o16,	A3
	lxvw4x		vs46,	o32,	A3
	lxvw4x		vs47,	o48,	A3

	mr		T1,	BO

	stxvw4x		vs32,	o0,	T1
	stxvw4x		vs33,	o16,	T1
	stxvw4x		vs34,	o32,	T1
	stxvw4x		vs35,	o48,	T1

	addi		T1,	T1,	64

	stxvw4x		vs36,	o0,	T1
	stxvw4x		vs37,	o16,	T1
	stxvw4x		vs38,	o32,	T1
	stxvw4x		vs39,	o48,	T1

	addi		T1,	T1,	64

	stxvw4x		vs40,	o0,	T1
	stxvw4x		vs41,	o16,	T1
	stxvw4x		vs42,	o32,	T1
	stxvw4x		vs43,	o48,	T1

	addi		T1,	T1,	64

	stxvw4x		vs44,	o0,	T1
	stxvw4x		vs45,	o16,	T1
	stxvw4x		vs46,	o32,	T1
	stxvw4x		vs47,	o48,	T1

.endm

/**********************************************************************************************
* Macros for N=4 and M=8
**********************************************************************************************/

.macro COPY_4x8

	lxvw4x		vs32,	o0,	A0
	lxvw4x		vs33,	o16,	A0

	lxvw4x		vs34,	o0,	A1
	lxvw4x		vs35,	o16,	A1

	lxvw4x		vs36,	o0,	A2
	lxvw4x		vs37,	o16,	A2

	lxvw4x		vs38,	o0,	A3
	lxvw4x		vs39,	o16,	A3

	mr		T1,	BO

	stxvw4x		vs32,	o0,	T1
	stxvw4x		vs33,	o16,	T1

	stxvw4x		vs34,	o32,	T1
	stxvw4x		vs35,	o48,	T1

	addi		T1,	T1,	64

	stxvw4x		vs36,	o0,	T1
	stxvw4x		vs37,	o16,	T1

	stxvw4x		vs38,	o32,	T1
	stxvw4x		vs39,	o48,	T1

.endm

/**********************************************************************************************
* Macros for N=4 and M=4
**********************************************************************************************/

.macro COPY_4x4

	lxvw4x		vs32,	o0,	A0

	lxvw4x		vs33,	o0,	A1

	lxvw4x		vs34,	o0,	A2

	lxvw4x		vs35,	o0,	A3

	mr		T1,	BO

	stxvw4x		vs32,	o0,	T1

	stxvw4x		vs33,	o16,	T1

	stxvw4x		vs34,	o32,	T1

	stxvw4x		vs35,	o48,	T1

.endm

/**********************************************************************************************
* Macros for N=4 and M=2
**********************************************************************************************/

.macro COPY_4x2

	lxsspx		vs32,	o0,	A0
	lxsspx		vs33,	o4,	A0

	lxsspx		vs34,	o0,	A1
	lxsspx		vs35,	o4,	A1

	lxsspx		vs36,	o0,	A2
	lxsspx		vs37,	o4,	A2

	lxsspx		vs38,	o0,	A3
	lxsspx		vs39,	o4,	A3

	mr		T1,	BO

	stxsspx		vs32,	o0,	T1
	stxsspx		vs33,	o4,	T1

	addi		T1,	T1,	8

	stxsspx		vs34,	o0,	T1
	stxsspx		vs35,	o4,	T1

	addi		T1,	T1,	8

	stxsspx		vs36,	o0,	T1
	stxsspx		vs37,	o4,	T1

	addi		T1,	T1,	8

	stxsspx		vs38,	o0,	T1
	stxsspx		vs39,	o4,	T1

.endm

/**********************************************************************************************
* Macros for N=4 and M=1
**********************************************************************************************/

.macro COPY_4x1

	lxsspx		vs32,	o0,	A0

	lxsspx		vs33,	o0,	A1

	lxsspx		vs34,	o0,	A2

	lxsspx		vs35,	o0,	A3

	mr		T1,	BO

	stxsspx		vs32,	o0,	T1

	stxsspx		vs33,	o4,	T1

	addi		T1,	T1,	8

	stxsspx		vs34,	o0,	T1

	stxsspx		vs35,	o4,	T1

.endm

/**********************************************************************************************
* Macros for N=2 and M=16
**********************************************************************************************/

.macro COPY_2x16

	lxvw4x		vs32,	o0,	A0
	lxvw4x		vs33,	o16,	A0
	lxvw4x		vs34,	o32,	A0
	lxvw4x		vs35,	o48,	A0

	lxvw4x		vs36,	o0,	A1
	lxvw4x		vs37,	o16,	A1
	lxvw4x		vs38,	o32,	A1
	lxvw4x		vs39,	o48,	A1

	mr		T1,	BO

	stxvw4x		vs32,	o0,	T1
	stxvw4x		vs33,	o16,	T1
	stxvw4x		vs34,	o32,	T1
	stxvw4x		vs35,	o48,	T1

	addi		T1,	T1,	64

	stxvw4x		vs36,	o0,	T1
	stxvw4x		vs37,	o16,	T1
	stxvw4x		vs38,	o32,	T1
	stxvw4x		vs39,	o48,	T1

.endm

/**********************************************************************************************
* Macros for N=2 and M=8
**********************************************************************************************/

.macro COPY_2x8

	lxvw4x		vs32,	o0,	A0
	lxvw4x		vs33,	o16,	A0

	lxvw4x		vs34,	o0,	A1
	lxvw4x		vs35,	o16,	A1

	mr		T1,	BO

	stxvw4x		vs32,	o0,	T1
	stxvw4x		vs33,	o16,	T1

	stxvw4x		vs34,	o32,	T1
	stxvw4x		vs35,	o48,	T1

.endm

/**********************************************************************************************
* Macros for N=2 and M=4
**********************************************************************************************/

.macro COPY_2x4

	lxvw4x		vs32,	o0,	A0

	lxvw4x		vs33,	o0,	A1

	mr		T1,	BO

	stxvw4x		vs32,	o0,	T1

	stxvw4x		vs33,	o16,	T1

.endm

/**********************************************************************************************
* Macros for N=2 and M=2
**********************************************************************************************/

.macro COPY_2x2

	lxsspx		vs32,	o0,	A0
	lxsspx		vs33,	o4,	A0

	lxsspx		vs34,	o0,	A1
	lxsspx		vs35,	o4,	A1

	mr		T1,	BO

	stxsspx		vs32,	o0,	T1
	stxsspx		vs33,	o4,	T1

	addi		T1,	T1,	8

	stxsspx		vs34,	o0,	T1
	stxsspx		vs35,	o4,	T1

.endm

/**********************************************************************************************
* Macros for N=2 and M=1
**********************************************************************************************/

.macro COPY_2x1

	lxsspx		vs32,	o0,	A0

	lxsspx		vs33,	o0,	A1

	mr		T1,	BO

	stxsspx		vs32,	o0,	T1

	stxsspx		vs33,	o4,	T1

.endm

/**********************************************************************************************
* Macros for N=1 and M=16
**********************************************************************************************/

.macro COPY_1x16

	lxvw4x		vs32,	o0,	A0
	lxvw4x		vs33,	o16,	A0
	lxvw4x		vs34,	o32,	A0
	lxvw4x		vs35,	o48,	A0

	mr		T1,	BO

	stxvw4x		vs32,	o0,	T1
	stxvw4x		vs33,	o16,	T1
	stxvw4x		vs34,	o32,	T1
	stxvw4x		vs35,	o48,	T1

.endm

/**********************************************************************************************
* Macros for N=1 and M=8
**********************************************************************************************/

.macro COPY_1x8

	lxvw4x		vs32,	o0,	A0
	lxvw4x		vs33,	o16,	A0

	mr		T1,	BO

	stxvw4x		vs32,	o0,	T1
	stxvw4x		vs33,	o16,	T1

.endm

/**********************************************************************************************
* Macros for N=1 and M=4
**********************************************************************************************/

.macro COPY_1x4

	lxvw4x		vs32,	o0,	A0

	mr		T1,	BO

	stxvw4x		vs32,	o0,	T1

.endm

/**********************************************************************************************
* Macros for N=1 and M=2
**********************************************************************************************/

.macro COPY_1x2

	lxsspx		vs32,	o0,	A0
	lxsspx		vs33,	o4,	A0

	mr		T1,	BO

	stxsspx		vs32,	o0,	T1
	stxsspx		vs33,	o4,	T1

.endm

/**********************************************************************************************
* Macros for N=1 and M=1
**********************************************************************************************/

.macro COPY_1x1

	lxsspx		vs32,	o0,	A0

	mr		T1,	BO

	stxsspx		vs32,	o0,	T1

.endm

