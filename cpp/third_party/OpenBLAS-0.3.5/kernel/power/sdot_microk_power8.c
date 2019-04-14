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
* 2016/03/21 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/

#define HAVE_KERNEL_16 1

static float sdot_kernel_16 (long n, float *x, float *y)
{
  float dot;
  __vector float t0;
  __vector float t1;
  __vector float t2;
  __vector float t3;

  __asm__
    (
       "dcbt		0, %2		\n\t"
       "dcbt		0, %3		\n\t"

       "xxlxor		32, 32,	32	\n\t"
       "xxlxor		33, 33,	33	\n\t"
       "xxlxor		34, 34,	34	\n\t"
       "xxlxor		35, 35,	35	\n\t"
       "xxlxor		36, 36,	36	\n\t"
       "xxlxor		37, 37,	37	\n\t"
       "xxlxor		38, 38,	38	\n\t"
       "xxlxor		39, 39,	39	\n\t"

       "lxvd2x		40, 0, %2	\n\t"
       "lxvd2x		48, 0, %3	\n\t"
       "lxvd2x		41, %10, %2	\n\t"
       "lxvd2x		49, %10, %3	\n\t"
       "lxvd2x		42, %11, %2	\n\t"
       "lxvd2x		50, %11, %3	\n\t"
       "lxvd2x		43, %12, %2	\n\t"
       "lxvd2x		51, %12, %3	\n\t"
       "lxvd2x		44, %13, %2	\n\t"
       "lxvd2x		%x4, %13, %3	\n\t"
       "lxvd2x		45, %14, %2	\n\t"
       "lxvd2x		%x5, %14, %3	\n\t"
       "lxvd2x		46, %15, %2	\n\t"
       "lxvd2x		%x6, %15, %3	\n\t"
       "lxvd2x		47, %16, %2	\n\t"
       "lxvd2x		%x7, %16, %3	\n\t"

       "addi		%2, %2, 128	\n\t"
       "addi		%3, %3, 128	\n\t"

       "addic.		%1, %1, -32	\n\t"
       "ble		2f		\n\t"

       ".p2align	5		\n"
     "1:				\n\t"

       "xvmaddasp	32, 40, 48	\n\t"
       "lxvd2x		40, 0, %2	\n\t"
       "lxvd2x		48, 0, %3	\n\t"
       "xvmaddasp	33, 41, 49	\n\t"
       "lxvd2x		41, %10, %2	\n\t"
       "lxvd2x		49, %10, %3	\n\t"
       "xvmaddasp	34, 42, 50	\n\t"
       "lxvd2x		42, %11, %2	\n\t"
       "lxvd2x		50, %11, %3	\n\t"
       "xvmaddasp	35, 43, 51	\n\t"
       "lxvd2x		43, %12, %2	\n\t"
       "lxvd2x		51, %12, %3	\n\t"
       "xvmaddasp	36, 44, %x4	\n\t"
       "lxvd2x		44, %13, %2	\n\t"
       "lxvd2x		%x4, %13, %3	\n\t"
       "xvmaddasp	37, 45, %x5	\n\t"
       "lxvd2x		45, %14, %2	\n\t"
       "lxvd2x		%x5, %14, %3	\n\t"
       "xvmaddasp	38, 46, %x6	\n\t"
       "lxvd2x		46, %15, %2	\n\t"
       "lxvd2x		%x6, %15, %3	\n\t"
       "xvmaddasp	39, 47, %x7	\n\t"
       "lxvd2x		47, %16, %2	\n\t"
       "lxvd2x		%x7, %16, %3	\n\t"

       "addi		%2, %2, 128	\n\t"
       "addi		%3, %3, 128	\n\t"

       "addic.		%1, %1, -32	\n\t"
       "bgt		1b		\n"

     "2:				\n\t"

       "xvmaddasp	32, 40, 48	\n\t"
       "xvmaddasp	33, 41, 49	\n\t"
       "xvmaddasp	34, 42, 50	\n\t"
       "xvmaddasp	35, 43, 51	\n\t"
       "xvmaddasp	36, 44, %x4	\n\t"
       "xvmaddasp	37, 45, %x5	\n\t"
       "xvmaddasp	38, 46, %x6	\n\t"
       "xvmaddasp	39, 47, %x7	\n\t"

       "xvaddsp		32, 32, 33	\n\t"
       "xvaddsp		34, 34, 35	\n\t"
       "xvaddsp		36, 36, 37	\n\t"
       "xvaddsp		38, 38, 39	\n\t"

       "xvaddsp		32, 32, 34	\n\t"
       "xvaddsp		36, 36, 38	\n\t"

       "xvaddsp		32, 32, 36	\n\t"

       "xxsldwi		33, 32, 32, 2	\n\t"
       "xvaddsp		32, 32, 33	\n\t"

       "xxsldwi		33, 32, 32, 1	\n\t"
       "xvaddsp		32, 32, 33	\n\t"

       "xscvspdp	%x0, 32		\n"

     "#dot=%0 n=%1 x=%8=%2 y=%9=%3 o16=%10 o32=%11 o48=%12 o64=%13 o80=%14 o96=%15 o122=%16\n"
     "#t0=%x4 t1=%x5 t2=%x6 t3=%x7"
     :
       "=f" (dot),	// 0
       "+r" (n),	// 1
       "+b" (x),	// 2
       "+b" (y),	// 3
       "=wa" (t0),	// 4
       "=wa" (t1),	// 5
       "=wa" (t2),	// 6
       "=wa" (t3)	// 7
     :
       "m" (*x),
       "m" (*y),
       "b" (16),	// 10
       "b" (32),	// 11
       "b" (48),	// 12
       "b" (64),	// 13
       "b" (80),	// 14
       "b" (96),	// 15
       "b" (112)	// 16
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37","vs38","vs39",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47",
       "vs48","vs49","vs50","vs51"
     );

  return dot;
}
