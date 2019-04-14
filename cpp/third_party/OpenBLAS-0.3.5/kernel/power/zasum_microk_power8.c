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
* 2016/03/28 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/

#define HAVE_KERNEL_8 1

static double zasum_kernel_8 (long n, double *x)
{
  double sum;
  __vector double t0;
  __vector double t1;
  __vector double t2;
  __vector double t3;

  __asm__
    (
       "dcbt		0, %2		\n\t"

       "xxlxor		32, 32,	32	\n\t"
       "xxlxor		33, 33,	33	\n\t"
       "xxlxor		34, 34,	34	\n\t"
       "xxlxor		35, 35,	35	\n\t"
       "xxlxor		36, 36,	36	\n\t"
       "xxlxor		37, 37,	37	\n\t"
       "xxlxor		38, 38,	38	\n\t"
       "xxlxor		39, 39,	39	\n\t"

       "lxvd2x		40, 0, %2	\n\t"
       "lxvd2x		41, %8, %2	\n\t"
       "lxvd2x		42, %9, %2	\n\t"
       "lxvd2x		43, %10, %2	\n\t"
       "lxvd2x		44, %11, %2	\n\t"
       "lxvd2x		45, %12, %2	\n\t"
       "lxvd2x		46, %13, %2	\n\t"
       "lxvd2x		47, %14, %2	\n\t"

       "addi		%2, %2, 128	\n\t"

       "addic.		%1, %1, -8	\n\t"
       "ble		2f		\n\t"

       ".p2align	5		\n"
     "1:				\n\t"

       "xvabsdp		48, 40		\n\t"
       "xvabsdp		49, 41		\n\t"
       "xvabsdp		50, 42		\n\t"
       "xvabsdp		51, 43		\n\t"

       "lxvd2x		40, 0, %2	\n\t"
       "lxvd2x		41, %8, %2	\n\t"

       "xvabsdp		%x3, 44		\n\t"
       "xvabsdp		%x4, 45		\n\t"

       "lxvd2x		42, %9, %2	\n\t"
       "lxvd2x		43, %10, %2	\n\t"

       "xvabsdp		%x5, 46		\n\t"
       "xvabsdp		%x6, 47		\n\t"

       "lxvd2x		44, %11, %2	\n\t"
       "lxvd2x		45, %12, %2	\n\t"

       "xvadddp		32, 32, 48	\n\t"
       "xvadddp		33, 33, 49	\n\t"

       "lxvd2x		46, %13, %2	\n\t"
       "lxvd2x		47, %14, %2	\n\t"

       "xvadddp		34, 34, 50	\n\t"
       "xvadddp		35, 35, 51	\n\t"
       "addi		%2, %2, 128	\n\t"
       "xvadddp		36, 36, %x3	\n\t"
       "xvadddp		37, 37, %x4	\n\t"
       "addic.		%1, %1, -8	\n\t"
       "xvadddp		38, 38, %x5	\n\t"
       "xvadddp		39, 39, %x6	\n\t"

       "bgt		1b		\n"

     "2:				\n\t"

       "xvabsdp		48, 40		\n\t"
       "xvabsdp		49, 41		\n\t"
       "xvabsdp		50, 42		\n\t"
       "xvabsdp		51, 43		\n\t"
       "xvabsdp		%x3, 44		\n\t"
       "xvabsdp		%x4, 45		\n\t"
       "xvabsdp		%x5, 46		\n\t"
       "xvabsdp		%x6, 47		\n\t"

       "xvadddp		32, 32, 48	\n\t"
       "xvadddp		33, 33, 49	\n\t"
       "xvadddp		34, 34, 50	\n\t"
       "xvadddp		35, 35, 51	\n\t"
       "xvadddp		36, 36, %x3	\n\t"
       "xvadddp		37, 37, %x4	\n\t"
       "xvadddp		38, 38, %x5	\n\t"
       "xvadddp		39, 39, %x6	\n\t"

       "xvadddp		32, 32, 33	\n\t"
       "xvadddp		34, 34, 35	\n\t"
       "xvadddp		36, 36, 37	\n\t"
       "xvadddp		38, 38, 39	\n\t"

       "xvadddp		32, 32, 34	\n\t"
       "xvadddp		36, 36, 38	\n\t"

       "xvadddp		32, 32, 36	\n\t"

       "xxswapd		33, 32		\n\t"
       "xsadddp		%x0, 32, 33	\n"

     "#n=%1 x=%3=%2 sum=%0 o16=%8 o32=%9 o48=%10 o64=%11 o80=%12 o96=%13 o112=%14\n"
     "#t0=%x3 t1=%x4 t2=%x5 t3=%x6"
     :
       "=d" (sum),	// 0
       "+r" (n),	// 1
       "+b" (x),	// 2
       "=wa" (t0),	// 3
       "=wa" (t1),	// 4
       "=wa" (t2),	// 5
       "=wa" (t3)	// 6
     :
       "m" (*x),
       "b" (16),	// 8
       "b" (32),	// 9
       "b" (48),	// 10
       "b" (64),	// 11
       "b" (80),	// 12
       "b" (96),	// 13
       "b" (112)	// 14
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37","vs38","vs39",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47",
       "vs48","vs49","vs50","vs51"
     );

  return sum;
}
