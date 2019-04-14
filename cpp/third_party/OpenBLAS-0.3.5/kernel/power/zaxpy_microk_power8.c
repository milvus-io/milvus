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
* 2016/03/23 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/


#define HAVE_KERNEL_4 1
static void zaxpy_kernel_4 (long n, double *x, double *y,
			    double alpha_r, double alpha_i)
{
#if !defined(CONJ)
  static const double mvec[2] = { -1.0, 1.0 };
#else
  static const double mvec[2] = { 1.0, -1.0 };
#endif
  const double *mvecp = mvec;

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
  long ytmp;

  __asm__
    (
       "xxspltd		32, %x19, 0	\n\t"	// alpha_r
       "xxspltd		33, %x20, 0	\n\t"	// alpha_i

       "lxvd2x		36, 0, %21	\n\t"	// mvec

#if !defined(CONJ)
       "xvmuldp		33, 33, 36	\n\t"	// alpha_i * mvec
#else
       "xvmuldp		32, 32, 36	\n\t"	// alpha_r * mvec
#endif

       "mr		%16, %3		\n\t"
       "dcbt		0, %2		\n\t"
       "dcbt		0, %3		\n\t"


       "lxvd2x		40, 0, %2	\n\t"	// x0
       "lxvd2x		41, %22, %2	\n\t"	// x1
       "lxvd2x		42, %23, %2	\n\t"	// x2
       "lxvd2x		43, %24, %2	\n\t"	// x3

       "lxvd2x		48, 0, %3	\n\t"	// y0
       "lxvd2x		49, %22, %3	\n\t"	// y1
       "lxvd2x		50, %23, %3	\n\t"	// y2
       "lxvd2x		51, %24, %3	\n\t"	// y3

       "xxswapd		%x8, 40		\n\t"	// exchange real and imag part
       "xxswapd		%x9, 41		\n\t"	// exchange real and imag part
       "xxswapd		%x10, 42	\n\t"	// exchange real and imag part
       "xxswapd		%x11, 43	\n\t"	// exchange real and imag part

       "addi		%2, %2, 64	\n\t"
       "addi		%3, %3, 64	\n\t"

       "lxvd2x		44, 0, %2	\n\t"	// x4
       "lxvd2x		45, %22, %2	\n\t"	// x5
       "lxvd2x		46, %23, %2	\n\t"	// x6
       "lxvd2x		47, %24, %2	\n\t"	// x7

       "lxvd2x		%x4, 0, %3	\n\t"	// y4
       "lxvd2x		%x5, %22, %3	\n\t"	// y5
       "lxvd2x		%x6, %23, %3	\n\t"	// y6
       "lxvd2x		%x7, %24, %3	\n\t"	// y7

       "xxswapd		%x12, 44	\n\t"	// exchange real and imag part
       "xxswapd		%x13, 45	\n\t"	// exchange real and imag part
       "xxswapd		%x14, 46	\n\t"	// exchange real and imag part
       "xxswapd		%x15, 47	\n\t"	// exchange real and imag part

       "addi		%2, %2, 64	\n\t"
       "addi		%3, %3, 64	\n\t"

       "addic.		%1, %1, -8	\n\t"
       "ble		2f		\n\t"

       ".p2align	5		\n"
       "1:				\n\t"

       "xvmaddadp	48, 40, 32	\n\t"	// alpha_r * x0_r , alpha_r * x0_i
       "xvmaddadp	49, 41, 32	\n\t"
       "lxvd2x		40, 0, %2	\n\t"	// x0
       "lxvd2x		41, %22, %2	\n\t"	// x1
       "xvmaddadp	50, 42, 32	\n\t"
       "xvmaddadp	51, 43, 32	\n\t"
       "lxvd2x		42, %23, %2	\n\t"	// x2
       "lxvd2x		43, %24, %2	\n\t"	// x3

       "xvmaddadp	%x4, 44, 32	\n\t"
       "addi		%2, %2, 64	\n\t"
       "xvmaddadp	%x5, 45, 32	\n\t"
       "lxvd2x		44, 0, %2	\n\t"	// x4
       "lxvd2x		45, %22, %2	\n\t"	// x5
       "xvmaddadp	%x6, 46, 32	\n\t"
       "xvmaddadp	%x7, 47, 32	\n\t"
       "lxvd2x		46, %23, %2	\n\t"	// x6
       "lxvd2x		47, %24, %2	\n\t"	// x7

       "xvmaddadp	48, %x8, 33	\n\t"	// alpha_i * x0_i , alpha_i * x0_r
       "addi		%2, %2, 64	\n\t"
       "xvmaddadp	49, %x9, 33	\n\t"
       "xvmaddadp	50, %x10, 33	\n\t"
       "xvmaddadp	51, %x11, 33	\n\t"

       "xvmaddadp	%x4, %x12, 33	\n\t"
       "xvmaddadp	%x5, %x13, 33	\n\t"
       "xvmaddadp	%x6, %x14, 33	\n\t"
       "xvmaddadp	%x7, %x15, 33	\n\t"

       "stxvd2x		48, 0, %16	\n\t"
       "stxvd2x		49, %22, %16	\n\t"
       "stxvd2x		50, %23, %16	\n\t"
       "stxvd2x		51, %24, %16	\n\t"

       "addi		%16, %16, 64	\n\t"

       "stxvd2x		%x4, 0, %16	\n\t"
       "stxvd2x		%x5, %22, %16	\n\t"
       "stxvd2x		%x6, %23, %16	\n\t"
       "stxvd2x		%x7, %24, %16	\n\t"

       "addi		%16, %16, 64	\n\t"

       "xxswapd		%x8, 40		\n\t"	// exchange real and imag part
       "xxswapd		%x9, 41		\n\t"	// exchange real and imag part
       "lxvd2x		48, 0, %3	\n\t"	// y0
       "lxvd2x		49, %22, %3	\n\t"	// y1
       "xxswapd		%x10, 42	\n\t"	// exchange real and imag part
       "xxswapd		%x11, 43	\n\t"	// exchange real and imag part
       "lxvd2x		50, %23, %3	\n\t"	// y2
       "lxvd2x		51, %24, %3	\n\t"	// y3

       "xxswapd		%x12, 44	\n\t"	// exchange real and imag part
       "addi		%3, %3, 64	\n\t"
       "xxswapd		%x13, 45	\n\t"	// exchange real and imag part
       "lxvd2x		%x4, 0, %3	\n\t"	// y4
       "lxvd2x		%x5, %22, %3	\n\t"	// y5
       "xxswapd		%x14, 46	\n\t"	// exchange real and imag part
       "xxswapd		%x15, 47	\n\t"	// exchange real and imag part
       "lxvd2x		%x6, %23, %3	\n\t"	// y6
       "lxvd2x		%x7, %24, %3	\n\t"	// y7

       "addi		%3, %3, 64	\n\t"

       "addic.		%1, %1, -8	\n\t"
       "bgt		1b		\n"

       "2:				\n\t"

       "xvmaddadp	48, 40, 32	\n\t"	// alpha_r * x0_r , alpha_r * x0_i
       "xvmaddadp	49, 41, 32	\n\t"
       "xvmaddadp	50, 42, 32	\n\t"
       "xvmaddadp	51, 43, 32	\n\t"

       "xvmaddadp	%x4, 44, 32	\n\t"
       "xvmaddadp	%x5, 45, 32	\n\t"
       "xvmaddadp	%x6, 46, 32	\n\t"
       "xvmaddadp	%x7, 47, 32	\n\t"

       "xvmaddadp	48, %x8, 33	\n\t"	// alpha_i * x0_i , alpha_i * x0_r
       "xvmaddadp	49, %x9, 33	\n\t"
       "xvmaddadp	50, %x10, 33	\n\t"
       "xvmaddadp	51, %x11, 33	\n\t"

       "xvmaddadp	%x4, %x12, 33	\n\t"
       "xvmaddadp	%x5, %x13, 33	\n\t"
       "xvmaddadp	%x6, %x14, 33	\n\t"
       "xvmaddadp	%x7, %x15, 33	\n\t"

       "stxvd2x		48, 0, %16	\n\t"
       "stxvd2x		49, %22, %16	\n\t"
       "stxvd2x		50, %23, %16	\n\t"
       "stxvd2x		51, %24, %16	\n\t"

       "addi		%16, %16, 64	\n\t"

       "stxvd2x		%x4, 0, %16	\n\t"
       "stxvd2x		%x5, %22, %16	\n\t"
       "stxvd2x		%x6, %23, %16	\n\t"
       "stxvd2x		%x7, %24, %16	\n"

     "#n=%1 x=%17=%2 y=%0=%3 alpha=(%19,%20) mvecp=%18=%16 o16=%22 o32=%23 o48=%24 ytmp=%16\n"
     "#t0=%x4 t1=%x5 t2=%x6 t3=%x7 t4=%x8 t5=%x9 t6=%x10 t7=%x11 t8=%x12 t9=%x13 t10=%x14 t11=%x15"
     :
       "+m" (*y),
       "+r" (n),	// 1
       "+b" (x),	// 2
       "+b" (y),	// 3
       "=wa" (t0),	// 4
       "=wa" (t1),	// 5
       "=wa" (t2),	// 6
       "=wa" (t3),	// 7
       "=wa" (t4),	// 8
       "=wa" (t5),	// 9
       "=wa" (t6),	// 10
       "=wa" (t7),	// 11
       "=wa" (t8),	// 12
       "=wa" (t9),	// 13
       "=wa" (t10),	// 14
       "=wa" (t11),	// 15
       "=b" (ytmp)	// 16
     :
       "m" (*x),
       "m" (*mvecp),
       "d" (alpha_r),	// 19
       "d" (alpha_i),	// 20
       "16" (mvecp),	// 21
       "b" (16),	// 22
       "b" (32),	// 23
       "b" (48)		// 24
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37","vs38","vs39",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47",
       "vs48","vs49","vs50","vs51"
     );
}
