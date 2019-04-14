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
*
* I don't use fused multiply-add ( precision problems with lapack )
*
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/

#define HAVE_KERNEL_16 1

static void srot_kernel_16 (long n, float *x, float *y, float c, float s)
{
  __vector float t0;
  __vector float t1;
  __vector float t2;
  __vector float t3;
  __vector float t4;
  __vector float t5;
  __vector float t6;
  __vector float t7;

  __asm__
    (
       "xscvdpspn	36, %x13	\n\t"	// load c to all words
       "xxspltw		36, 36, 0	\n\t"

       "xscvdpspn	37, %x14	\n\t"	// load s to all words
       "xxspltw		37, 37, 0	\n\t"

       "lxvd2x		32, 0, %3	\n\t"	// load x
       "lxvd2x		33, %15, %3	\n\t"
       "lxvd2x		34, %16, %3	\n\t"
       "lxvd2x		35, %17, %3	\n\t"

       "lxvd2x		48, 0, %4	\n\t"	// load y
       "lxvd2x		49, %15, %4	\n\t"
       "lxvd2x		50, %16, %4	\n\t"
       "lxvd2x		51, %17, %4	\n\t"

       "addi		%3, %3, 64	\n\t"
       "addi		%4, %4, 64	\n\t"

       "addic.		%2, %2, -16	\n\t"
       "ble		2f		\n\t"

       ".p2align	5		\n"
     "1:				\n\t"

       "xvmulsp		40, 32, 36	\n\t"	// c * x
       "xvmulsp		41, 33, 36	\n\t"
       "xvmulsp		42, 34, 36	\n\t"
       "xvmulsp		43, 35, 36	\n\t"

       "xvmulsp		%x5, 48, 36	\n\t"	// c * y
       "xvmulsp		%x6, 49, 36	\n\t"
       "xvmulsp		%x7, 50, 36	\n\t"
       "xvmulsp		%x8, 51, 36	\n\t"

       "xvmulsp		44, 32, 37	\n\t"	// s * x
       "xvmulsp		45, 33, 37	\n\t"

       "lxvd2x		32, 0, %3	\n\t"	// load x
       "lxvd2x		33, %15, %3	\n\t"

       "xvmulsp		46, 34, 37	\n\t"
       "xvmulsp		47, 35, 37	\n\t"

       "lxvd2x		34, %16, %3	\n\t"
       "lxvd2x		35, %17, %3	\n\t"

       "xvmulsp		%x9, 48, 37	\n\t"	// s * y
       "xvmulsp		%x10, 49, 37	\n\t"

       "lxvd2x		48, 0, %4	\n\t"	// load y
       "lxvd2x		49, %15, %4	\n\t"

       "xvmulsp		%x11, 50, 37	\n\t"
       "xvmulsp		%x12, 51, 37	\n\t"

       "lxvd2x		50, %16, %4	\n\t"
       "lxvd2x		51, %17, %4	\n\t"

       "xvaddsp		40, 40, %x9	\n\t"	// c * x + s * y
       "xvaddsp		41, 41, %x10	\n\t"	// c * x + s * y

       "addi		%3, %3, -64	\n\t"
       "addi		%4, %4, -64	\n\t"

       "xvaddsp		42, 42, %x11	\n\t"	// c * x + s * y
       "xvaddsp		43, 43, %x12	\n\t"	// c * x + s * y

       "xvsubsp		%x5, %x5, 44	\n\t"	// c * y - s * x
       "xvsubsp		%x6, %x6, 45	\n\t"	// c * y - s * x
       "xvsubsp		%x7, %x7, 46	\n\t"	// c * y - s * x
       "xvsubsp		%x8, %x8, 47	\n\t"	// c * y - s * x

       "stxvd2x		40, 0, %3	\n\t"	// store x
       "stxvd2x		41, %15, %3	\n\t"
       "stxvd2x		42, %16, %3	\n\t"
       "stxvd2x		43, %17, %3	\n\t"

       "stxvd2x		%x5, 0, %4	\n\t"	// store y
       "stxvd2x		%x6, %15, %4	\n\t"
       "stxvd2x		%x7, %16, %4	\n\t"
       "stxvd2x		%x8, %17, %4	\n\t"

       "addi		%3, %3, 128	\n\t"
       "addi		%4, %4, 128	\n\t"

       "addic.		%2, %2, -16	\n\t"
       "bgt		1b		\n"

     "2:				\n\t"

       "xvmulsp		40, 32, 36	\n\t"	// c * x
       "xvmulsp		41, 33, 36	\n\t"
       "xvmulsp		42, 34, 36	\n\t"
       "xvmulsp		43, 35, 36	\n\t"

       "xvmulsp		%x5, 48, 36	\n\t"	// c * y
       "xvmulsp		%x6, 49, 36	\n\t"
       "xvmulsp		%x7, 50, 36	\n\t"
       "xvmulsp		%x8, 51, 36	\n\t"

       "xvmulsp		44, 32, 37	\n\t"	// s * x
       "xvmulsp		45, 33, 37	\n\t"
       "xvmulsp		46, 34, 37	\n\t"
       "xvmulsp		47, 35, 37	\n\t"

       "xvmulsp		%x9, 48, 37	\n\t"	// s * y
       "xvmulsp		%x10, 49, 37	\n\t"
       "xvmulsp		%x11, 50, 37	\n\t"
       "xvmulsp		%x12, 51, 37	\n\t"

       "addi		%3, %3, -64	\n\t"
       "addi		%4, %4, -64	\n\t"

       "xvaddsp		40, 40, %x9	\n\t"	// c * x + s * y
       "xvaddsp		41, 41, %x10	\n\t"	// c * x + s * y
       "xvaddsp		42, 42, %x11	\n\t"	// c * x + s * y
       "xvaddsp		43, 43, %x12	\n\t"	// c * x + s * y

       "xvsubsp		%x5, %x5, 44	\n\t"	// c * y - s * x
       "xvsubsp		%x6, %x6, 45	\n\t"	// c * y - s * x
       "xvsubsp		%x7, %x7, 46	\n\t"	// c * y - s * x
       "xvsubsp		%x8, %x8, 47	\n\t"	// c * y - s * x

       "stxvd2x		40, 0, %3	\n\t"	// store x
       "stxvd2x		41, %15, %3	\n\t"
       "stxvd2x		42, %16, %3	\n\t"
       "stxvd2x		43, %17, %3	\n\t"

       "stxvd2x		%x5, 0, %4	\n\t"	// store y
       "stxvd2x		%x6, %15, %4	\n\t"
       "stxvd2x		%x7, %16, %4	\n\t"
       "stxvd2x		%x8, %17, %4	\n"

     "#n=%2 x=%0=%3 y=%1=%4 c=%13 s=%14 o16=%15 o32=%16 o48=%17\n"
     "#t0=%x5 t1=%x6 t2=%x7 t3=%x8 t4=%x9 t5=%x10 t6=%x11 t7=%x12"
     :
       "+m" (*x),
       "+m" (*y),
       "+r" (n),	// 2
       "+b" (x),	// 3
       "+b" (y),	// 4
       "=wa" (t0),	// 5
       "=wa" (t1),	// 6
       "=wa" (t2),	// 7
       "=wa" (t3),	// 8
       "=wa" (t4),	// 9
       "=wa" (t5),	// 10
       "=wa" (t6),	// 11
       "=wa" (t7)	// 12
     :
       "f" (c),		// 13
       "f" (s),		// 14
       "b" (16),	// 15
       "b" (32),	// 16
       "b" (48)		// 17
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47",
       "vs48","vs49","vs50","vs51"
     );
}
