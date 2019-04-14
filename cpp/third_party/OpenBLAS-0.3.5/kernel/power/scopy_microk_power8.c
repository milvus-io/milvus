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
* 2016/03/25 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/

#define HAVE_KERNEL_32 1

static void scopy_kernel_32 (long n, float *x, float *y)
{
  __asm__
    (
       "lxvd2x		40, 0, %2	\n\t"
       "lxvd2x		41, %5, %2	\n\t"
       "lxvd2x		42, %6, %2	\n\t"
       "lxvd2x		43, %7, %2	\n\t"
       "lxvd2x		44, %8, %2	\n\t"
       "lxvd2x		45, %9, %2	\n\t"
       "lxvd2x		46, %10, %2	\n\t"
       "lxvd2x		47, %11, %2	\n\t"

       "addi		%2, %2, 128	\n\t"

       "addic.		%1, %1, -32	\n\t"
       "ble		2f		\n\t"

       ".p2align	5		\n"
     "1:				\n\t"

       "stxvd2x		40, 0, %3	\n\t"
       "stxvd2x		41, %5, %3	\n\t"
       "lxvd2x		40, 0, %2	\n\t"
       "lxvd2x		41, %5, %2	\n\t"
       "stxvd2x		42, %6, %3	\n\t"
       "stxvd2x		43, %7, %3	\n\t"
       "lxvd2x		42, %6, %2	\n\t"
       "lxvd2x		43, %7, %2	\n\t"
       "stxvd2x		44, %8, %3	\n\t"
       "stxvd2x		45, %9, %3	\n\t"
       "lxvd2x		44, %8, %2	\n\t"
       "lxvd2x		45, %9, %2	\n\t"
       "stxvd2x		46, %10, %3	\n\t"
       "stxvd2x		47, %11, %3	\n\t"
       "lxvd2x		46, %10, %2	\n\t"
       "lxvd2x		47, %11, %2	\n\t"

       "addi		%3, %3, 128	\n\t"
       "addi		%2, %2, 128	\n\t"

       "addic.		%1, %1, -32	\n\t"
       "bgt		1b		\n"

     "2:				\n\t"

       "stxvd2x		40, 0, %3	\n\t"
       "stxvd2x		41, %5, %3	\n\t"
       "stxvd2x		42, %6, %3	\n\t"
       "stxvd2x		43, %7, %3	\n\t"
       "stxvd2x		44, %8, %3	\n\t"
       "stxvd2x		45, %9, %3	\n\t"
       "stxvd2x		46, %10, %3	\n\t"
       "stxvd2x		47, %11, %3	\n"

     "#n=%1 x=%4=%2 y=%0=%3 o16=%5 o32=%6 o48=%7 o64=%8 o80=%9 o96=%10 o112=%11"
     :
       "=m" (*y),
       "+r" (n),	// 1
       "+b" (x),	// 2
       "+b" (y)		// 3
     :
       "m" (*x),
       "b" (16),	// 5
       "b" (32),	// 6
       "b" (48),	// 7
       "b" (64),	// 8
       "b" (80),	// 9
       "b" (96),	// 10
       "b" (112)	// 11
     :
       "cr0",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47"
     );
}
