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
*
* I don't use fused multipy-add ( lapack precision problems )
*
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/

#define HAVE_KERNEL_8 1

static void zscal_kernel_8 (long n, double *x, double alpha_r, double alpha_i)
{
  __vector double t0;
  __vector double t1;
  __vector double t2;
  __vector double t3;
  __vector double t4;
  __vector double t5;
  __vector double t6;
  __vector double t7;
  __vector double t8;
  __vector double t9;
  __vector double t10;
  __vector double t11;

  __asm__
    (
       "dcbt		0, %2		\n\t"

       "xsnegdp		33, %x16	\n\t"	// -alpha_i
       "xxspltd		32, %x15, 0	\n\t"	// alpha_r , alpha_r
       "xxmrghd		33, 33, %x16	\n\t"	// -alpha_i , alpha_i

       "lxvd2x		40, 0, %2	\n\t"	// x0_r, x0_i
       "lxvd2x		41, %17, %2	\n\t"
       "lxvd2x		42, %18, %2	\n\t"
       "lxvd2x		43, %19, %2	\n\t"
       "lxvd2x		44, %20, %2	\n\t"
       "lxvd2x		45, %21, %2	\n\t"
       "lxvd2x		46, %22, %2	\n\t"
       "lxvd2x		47, %23, %2	\n\t"

       "addi		%2, %2, 128	\n\t"

       "addic.		%1, %1, -8	\n\t"
       "ble		2f		\n\t"

       ".p2align	5		\n"
     "1:				\n\t"

       "xvmuldp		48, 40, 32	\n\t"	// x0_r * alpha_r, x0_i * alpha_r
       "xvmuldp		49, 41, 32	\n\t"
       "xvmuldp		50, 42, 32	\n\t"
       "xvmuldp		51, 43, 32	\n\t"
       "xvmuldp		%x3, 44, 32	\n\t"
       "xvmuldp		%x4, 45, 32	\n\t"
       "xvmuldp		%x5, 46, 32	\n\t"
       "xvmuldp		%x6, 47, 32	\n\t"

       "xxswapd		%x7, 40		\n\t"
       "xxswapd		%x8, 41		\n\t"
       "xxswapd		%x9, 42		\n\t"
       "xxswapd		%x10, 43		\n\t"
       "xxswapd		%x11, 44		\n\t"
       "xxswapd		%x12, 45	\n\t"
       "xxswapd		%x13, 46	\n\t"
       "xxswapd		%x14, 47	\n\t"

       "xvmuldp		%x7, %x7, 33	\n\t"	// x0_i * -alpha_i, x0_r * alpha_i
       "xvmuldp		%x8, %x8, 33	\n\t"

       "lxvd2x		40, 0, %2	\n\t"	// x0_r, x0_i
       "lxvd2x		41, %17, %2	\n\t"

       "xvmuldp		%x9, %x9, 33	\n\t"
       "xvmuldp		%x10, %x10, 33	\n\t"

       "lxvd2x		42, %18, %2	\n\t"
       "lxvd2x		43, %19, %2	\n\t"

       "xvmuldp		%x11, %x11, 33	\n\t"
       "xvmuldp		%x12, %x12, 33	\n\t"

       "lxvd2x		44, %20, %2	\n\t"
       "lxvd2x		45, %21, %2	\n\t"

       "xvmuldp		%x13, %x13, 33	\n\t"
       "xvmuldp		%x14, %x14, 33	\n\t"

       "lxvd2x		46, %22, %2	\n\t"
       "lxvd2x		47, %23, %2	\n\t"

       "addi		%2, %2, -128	\n\t"

       "xvadddp		48, 48, %x7	\n\t"
       "xvadddp		49, 49, %x8	\n\t"
       "xvadddp		50, 50, %x9	\n\t"
       "xvadddp		51, 51, %x10	\n\t"

       "stxvd2x		48, 0, %2	\n\t"
       "stxvd2x		49, %17, %2	\n\t"

       "xvadddp		%x3, %x3, %x11	\n\t"
       "xvadddp		%x4, %x4, %x12	\n\t"

       "stxvd2x		50, %18, %2	\n\t"
       "stxvd2x		51, %19, %2	\n\t"

       "xvadddp		%x5, %x5, %x13	\n\t"
       "xvadddp		%x6, %x6, %x14	\n\t"

       "stxvd2x		%x3, %20, %2	\n\t"
       "stxvd2x		%x4, %21, %2	\n\t"
       "stxvd2x		%x5, %22, %2	\n\t"
       "stxvd2x		%x6, %23, %2	\n\t"

       "addi		%2, %2, 256	\n\t"

       "addic.		%1, %1, -8	\n\t"
       "bgt		1b		\n"

     "2:				\n\t"

       "xvmuldp		48, 40, 32	\n\t"	// x0_r * alpha_r, x0_i * alpha_r
       "xvmuldp		49, 41, 32	\n\t"
       "xvmuldp		50, 42, 32	\n\t"
       "xvmuldp		51, 43, 32	\n\t"
       "xvmuldp		%x3, 44, 32	\n\t"
       "xvmuldp		%x4, 45, 32	\n\t"
       "xvmuldp		%x5, 46, 32	\n\t"
       "xvmuldp		%x6, 47, 32	\n\t"

       "xxswapd		%x7, 40		\n\t"
       "xxswapd		%x8, 41		\n\t"
       "xxswapd		%x9, 42		\n\t"
       "xxswapd		%x10, 43		\n\t"
       "xxswapd		%x11, 44		\n\t"
       "xxswapd		%x12, 45	\n\t"
       "xxswapd		%x13, 46	\n\t"
       "xxswapd		%x14, 47	\n\t"

       "addi		%2, %2, -128	\n\t"

       "xvmuldp		%x7, %x7, 33	\n\t"	// x0_i * -alpha_i, x0_r * alpha_i
       "xvmuldp		%x8, %x8, 33	\n\t"
       "xvmuldp		%x9, %x9, 33	\n\t"
       "xvmuldp		%x10, %x10, 33	\n\t"
       "xvmuldp		%x11, %x11, 33	\n\t"
       "xvmuldp		%x12, %x12, 33	\n\t"
       "xvmuldp		%x13, %x13, 33	\n\t"
       "xvmuldp		%x14, %x14, 33	\n\t"

       "xvadddp		48, 48, %x7	\n\t"
       "xvadddp		49, 49, %x8	\n\t"
       "xvadddp		50, 50, %x9	\n\t"
       "xvadddp		51, 51, %x10	\n\t"

       "stxvd2x		48, 0, %2	\n\t"
       "stxvd2x		49, %17, %2	\n\t"

       "xvadddp		%x3, %x3, %x11	\n\t"
       "xvadddp		%x4, %x4, %x12	\n\t"

       "stxvd2x		50, %18, %2	\n\t"
       "stxvd2x		51, %19, %2	\n\t"

       "xvadddp		%x5, %x5, %x13	\n\t"
       "xvadddp		%x6, %x6, %x14	\n\t"

       "stxvd2x		%x3, %20, %2	\n\t"
       "stxvd2x		%x4, %21, %2	\n\t"
       "stxvd2x		%x5, %22, %2	\n\t"
       "stxvd2x		%x6, %23, %2	\n"

     "#n=%1 x=%0=%2 alpha=(%15,%16) o16=%17 o32=%18 o48=%19 o64=%20 o80=%21 o96=%22 o112=%23\n"
     "#t0=%x3 t1=%x4 t2=%x5 t3=%x6 t4=%x7 t5=%x8 t6=%x9 t7=%x10 t8=%x11 t9=%x12 t10=%x13 t11=%x14"
     :
       "+m" (*x),
       "+r" (n),	// 1
       "+b" (x),	// 2
       "=wa" (t0),	// 3
       "=wa" (t1),	// 4
       "=wa" (t2),	// 5
       "=wa" (t3),	// 6
       "=wa" (t4),	// 7
       "=wa" (t5),	// 8
       "=wa" (t6),	// 9
       "=wa" (t7),	// 10
       "=wa" (t8),	// 11
       "=wa" (t9),	// 12
       "=wa" (t10),	// 13
       "=wa" (t11)	// 14
     :
       "d" (alpha_r),	// 15
       "d" (alpha_i),	// 16
       "b" (16),	// 17
       "b" (32),	// 18
       "b" (48),	// 19
       "b" (64),	// 20
       "b" (80),	// 21
       "b" (96),	// 22
       "b" (112)	// 23
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37","vs38","vs39",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47",
       "vs48","vs49","vs50","vs51"
     );
}
