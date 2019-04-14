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
* 2016/03/27 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/

#define HAVE_KERNEL_32 1

static void cswap_kernel_32 (long n, float *x, float *y)
{
  __asm__
    (
       ".p2align	5		\n"
     "1:				\n\t"

       "lxvd2x		32, 0, %4	\n\t"
       "lxvd2x		33, %5, %4	\n\t"
       "lxvd2x		34, %6, %4	\n\t"
       "lxvd2x		35, %7, %4	\n\t"
       "lxvd2x		36, %8, %4	\n\t"
       "lxvd2x		37, %9, %4	\n\t"
       "lxvd2x		38, %10, %4	\n\t"
       "lxvd2x		39, %11, %4	\n\t"

       "addi		%4, %4, 128	\n\t"

       "lxvd2x		40, 0, %4	\n\t"
       "lxvd2x		41, %5, %4	\n\t"
       "lxvd2x		42, %6, %4	\n\t"
       "lxvd2x		43, %7, %4	\n\t"
       "lxvd2x		44, %8, %4	\n\t"
       "lxvd2x		45, %9, %4	\n\t"
       "lxvd2x		46, %10, %4	\n\t"
       "lxvd2x		47, %11, %4	\n\t"

       "addi		%4, %4, -128	\n\t"

       "lxvd2x		48, 0, %3	\n\t"
       "lxvd2x		49, %5, %3	\n\t"
       "lxvd2x		50, %6, %3	\n\t"
       "lxvd2x		51, %7, %3	\n\t"
       "lxvd2x		0, %8, %3	\n\t"
       "lxvd2x		1, %9, %3	\n\t"
       "lxvd2x		2, %10, %3	\n\t"
       "lxvd2x		3, %11, %3	\n\t"

       "addi		%3, %3, 128	\n\t"

       "lxvd2x		4, 0, %3	\n\t"
       "lxvd2x		5, %5, %3	\n\t"
       "lxvd2x		6, %6, %3	\n\t"
       "lxvd2x		7, %7, %3	\n\t"
       "lxvd2x		8, %8, %3	\n\t"
       "lxvd2x		9, %9, %3	\n\t"
       "lxvd2x		10, %10, %3	\n\t"
       "lxvd2x		11, %11, %3	\n\t"

       "addi		%3, %3, -128	\n\t"

       "stxvd2x		32, 0, %3	\n\t"
       "stxvd2x		33, %5, %3	\n\t"
       "stxvd2x		34, %6, %3	\n\t"
       "stxvd2x		35, %7, %3	\n\t"
       "stxvd2x		36, %8, %3	\n\t"
       "stxvd2x		37, %9, %3	\n\t"
       "stxvd2x		38, %10, %3	\n\t"
       "stxvd2x		39, %11, %3	\n\t"

       "addi		%3, %3, 128	\n\t"

       "stxvd2x		40, 0, %3	\n\t"
       "stxvd2x		41, %5, %3	\n\t"
       "stxvd2x		42, %6, %3	\n\t"
       "stxvd2x		43, %7, %3	\n\t"
       "stxvd2x		44, %8, %3	\n\t"
       "stxvd2x		45, %9, %3	\n\t"
       "stxvd2x		46, %10, %3	\n\t"
       "stxvd2x		47, %11, %3	\n\t"

       "addi		%3, %3, 128	\n\t"

       "stxvd2x		48, 0, %4	\n\t"
       "stxvd2x		49, %5, %4	\n\t"
       "stxvd2x		50, %6, %4	\n\t"
       "stxvd2x		51, %7, %4	\n\t"
       "stxvd2x		0, %8, %4	\n\t"
       "stxvd2x		1, %9, %4	\n\t"
       "stxvd2x		2, %10, %4	\n\t"
       "stxvd2x		3, %11, %4	\n\t"

       "addi		%4, %4, 128	\n\t"

       "stxvd2x		4, 0, %4	\n\t"
       "stxvd2x		5, %5, %4	\n\t"
       "stxvd2x		6, %6, %4	\n\t"
       "stxvd2x		7, %7, %4	\n\t"
       "stxvd2x		8, %8, %4	\n\t"
       "stxvd2x		9, %9, %4	\n\t"
       "stxvd2x		10, %10, %4	\n\t"
       "stxvd2x		11, %11, %4	\n\t"

       "addi		%4, %4, 128	\n\t"

       "addic.		%2, %2, -32	\n\t"
       "bgt		1b		\n"

     "#n=%2 x=%0=%3 y=%1=%4 o16=%5 o32=%6 o48=%7 o64=%8 o80=%9 o96=%10 o112=%11"
     :
       "+m" (*x),
       "+m" (*y),
       "+r" (n),	// 2
       "+b" (x),	// 3
       "+b" (y)		// 4
     :
       "b" (16),	// 5
       "b" (32),	// 6
       "b" (48),	// 7
       "b" (64),	// 8
       "b" (80),	// 9
       "b" (96),	// 10
       "b" (112)	// 11
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37","vs38","vs39",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47",
       "vs48","vs49","vs50","vs51","vs0","vs1","vs2","vs3",
       "vs4","vs5","vs6","vs7","vs8","vs9","vs10","vs11"
     );
}
