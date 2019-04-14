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

#define HAVE_KERNEL_16 1

static void sscal_kernel_16 (long n, float *x, float alpha)
{
  __asm__
    (
       "dcbt		0, %2		\n\t"

       "xscvdpspn	%x3, %x3	\n\t"
       "xxspltw		%x3, %x3, 0	\n\t"

       "lxvd2x		32, 0, %2	\n\t"
       "lxvd2x		33, %4, %2	\n\t"
       "lxvd2x		34, %5, %2	\n\t"
       "lxvd2x		35, %6, %2	\n\t"
       "lxvd2x		36, %7, %2	\n\t"
       "lxvd2x		37, %8, %2	\n\t"
       "lxvd2x		38, %9, %2	\n\t"
       "lxvd2x		39, %10, %2	\n\t"

       "addi		%2, %2, 128	\n\t"

       "addic.		%1, %1, -32	\n\t"
       "ble		2f		\n\t"

       ".p2align	5		\n"
     "1:				\n\t"

       "xvmulsp		40, 32, %x3	\n\t"
       "xvmulsp		41, 33, %x3	\n\t"
       "lxvd2x		32, 0, %2	\n\t"
       "lxvd2x		33, %4, %2	\n\t"
       "xvmulsp		42, 34, %x3	\n\t"
       "xvmulsp		43, 35, %x3	\n\t"
       "lxvd2x		34, %5, %2	\n\t"
       "lxvd2x		35, %6, %2	\n\t"
       "xvmulsp		44, 36, %x3	\n\t"
       "xvmulsp		45, 37, %x3	\n\t"
       "lxvd2x		36, %7, %2	\n\t"
       "lxvd2x		37, %8, %2	\n\t"
       "xvmulsp		46, 38, %x3	\n\t"
       "xvmulsp		47, 39, %x3	\n\t"
       "lxvd2x		38, %9, %2	\n\t"
       "lxvd2x		39, %10, %2	\n\t"

       "addi		%2, %2, -128	\n\t"

       "stxvd2x		40, 0, %2	\n\t"
       "stxvd2x		41, %4, %2	\n\t"
       "stxvd2x		42, %5, %2	\n\t"
       "stxvd2x		43, %6, %2	\n\t"
       "stxvd2x		44, %7, %2	\n\t"
       "stxvd2x		45, %8, %2	\n\t"
       "stxvd2x		46, %9, %2	\n\t"
       "stxvd2x		47, %10, %2	\n\t"

       "addi		%2, %2, 256	\n\t"

       "addic.		%1, %1, -32	\n\t"
       "bgt		1b		\n"

     "2:				\n\t"

       "xvmulsp		40, 32, %x3	\n\t"
       "xvmulsp		41, 33, %x3	\n\t"
       "xvmulsp		42, 34, %x3	\n\t"
       "xvmulsp		43, 35, %x3	\n\t"

       "addi		%2, %2, -128	\n\t"

       "xvmulsp		44, 36, %x3	\n\t"
       "xvmulsp		45, 37, %x3	\n\t"
       "xvmulsp		46, 38, %x3	\n\t"
       "xvmulsp		47, 39, %x3	\n\t"

       "stxvd2x		40, 0, %2	\n\t"
       "stxvd2x		41, %4, %2	\n\t"
       "stxvd2x		42, %5, %2	\n\t"
       "stxvd2x		43, %6, %2	\n\t"
       "stxvd2x		44, %7, %2	\n\t"
       "stxvd2x		45, %8, %2	\n\t"
       "stxvd2x		46, %9, %2	\n\t"
       "stxvd2x		47, %10, %2	\n"

     "#n=%1 alpha=%3 x=%0=%2 o16=%4 o32=%5 o48=%6 o64=%7 o80=%8 o96=%9 o112=%10"
     :
       "+m" (*x),
       "+r" (n),	// 1
       "+b" (x),	// 2
       "+f" (alpha)	// 3
     :
       "b" (16),	// 4
       "b" (32),	// 5
       "b" (48),	// 6
       "b" (64),	// 7
       "b" (80),	// 8
       "b" (96),	// 9
       "b" (112)	// 10
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37","vs38","vs39",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47"
     );
}


static void sscal_kernel_16_zero (long n, float *x)
{
  __vector float t0;

  __asm__
    (
       "xxlxor		%x3, %x3, %x3	\n\t"

       ".p2align	5		\n"
     "1:				\n\t"

       "stxvd2x		%x3, 0, %2	\n\t"
       "stxvd2x		%x3, %4, %2	\n\t"
       "stxvd2x		%x3, %5, %2	\n\t"
       "stxvd2x		%x3, %6, %2	\n\t"
       "stxvd2x		%x3, %7, %2	\n\t"
       "stxvd2x		%x3, %8, %2	\n\t"
       "stxvd2x		%x3, %9, %2	\n\t"
       "stxvd2x		%x3, %10, %2	\n\t"

       "addi		%2, %2, 128	\n\t"

       "addic.		%1, %1, -32	\n\t"
       "bgt		1b		\n"

     "#n=%1 x=%0=%2 t0=%x3 o16=%4 o32=%5 o48=%6 o64=%7 o80=%8 o96=%9 o112=%10"
     :
       "=m" (*x),
       "+r" (n),	// 1
       "+b" (x),	// 2
       "=wa" (t0)	// 3
     :
       "b" (16),	// 4
       "b" (32),	// 5
       "b" (48),	// 6
       "b" (64),	// 7
       "b" (80),	// 8
       "b" (96),	// 9
       "b" (112)	// 10
     :
       "cr0"
     );
}
