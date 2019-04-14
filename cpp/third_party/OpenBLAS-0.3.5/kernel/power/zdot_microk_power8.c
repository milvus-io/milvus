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

#define HAVE_KERNEL_8 1

static void zdot_kernel_8 (long n, double *x, double *y, double *dot)
{
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

       "lxvd2x		40, 0, %2	\n\t"	// x0_r, x0_i
       "lxvd2x		48, 0, %3	\n\t"	// y0_r, y0_i
       "lxvd2x		41, %7, %2	\n\t"	// x1_r, x1_i
       "lxvd2x		49, %7, %3	\n\t"	// y1_r, y1_i
       "lxvd2x		42, %8, %2	\n\t"	// x2_r, x2_i
       "lxvd2x		50, %8, %3	\n\t"	// y2_r, y2_i
       "lxvd2x		43, %9, %2	\n\t"	// x3_r, x3_i
       "lxvd2x		51, %9, %3	\n\t"	// y3_r, y3_i

       "xxswapd		0, 48		\n\t"	// y0_i, y0_r
       "xxswapd		1, 49		\n\t"	// y1_i, y1_r
       "xxswapd		2, 50		\n\t"	// y2_i, y2_r
       "xxswapd		3, 51		\n\t"	// y3_i, y3_r

       "addi		%2, %2, 64	\n\t"
       "addi		%3, %3, 64	\n\t"

       "lxvd2x		44, 0, %2	\n\t"	// x0_r, x0_i
       "lxvd2x		4, 0, %3	\n\t"	// y0_r, y0_i
       "lxvd2x		45, %7, %2	\n\t"	// x1_r, x1_i
       "lxvd2x		5, %7, %3	\n\t"	// y1_r, y1_i
       "lxvd2x		46, %8, %2	\n\t"	// x2_r, x2_i
       "lxvd2x		6, %8, %3	\n\t"	// y2_r, y2_i
       "lxvd2x		47, %9, %2	\n\t"	// x3_r, x3_i
       "lxvd2x		7, %9, %3	\n\t"	// y3_r, y3_i

       "xxswapd		8, 4		\n\t"	// y0_i, y0_r
       "xxswapd		9, 5		\n\t"	// y1_i, y1_r
       "xxswapd		10, 6		\n\t"	// y2_i, y2_r
       "xxswapd		11, 7		\n\t"	// y3_i, y3_r

       "addi		%2, %2, 64	\n\t"
       "addi		%3, %3, 64	\n\t"

       "addic.		%1, %1, -8	\n\t"
       "ble		2f		\n\t"

       ".p2align	5		\n"
     "1:				\n\t"

       "xvmaddadp	32, 40, 48	\n\t"	// x0_r * y0_r , x0_i * y0_i
       "lxvd2x		48, 0, %3	\n\t"	// y0_r, y0_i
       "xvmaddadp	34, 41, 49	\n\t"	// x1_r * y1_r , x1_i * y1_i
       "lxvd2x		49, %7, %3	\n\t"	// y1_r, y1_i

       "xvmaddadp	36, 42, 50	\n\t"	// x2_r * y2_r , x2_i * y2_i
       "lxvd2x		50, %8, %3	\n\t"	// y2_r, y2_i
       "xvmaddadp	38, 43, 51	\n\t"	// x3_r * y3_r , x3_i * y3_i
       "lxvd2x		51, %9, %3	\n\t"	// y3_r, y3_i

       "xvmaddadp	33, 40, 0	\n\t"	// x0_r * y0_i , x0_i * y0_r
       "lxvd2x		40, 0, %2	\n\t"	// x0_r, x0_i
       "xvmaddadp	35, 41, 1	\n\t"	// x1_r * y1_i , x1_i * y1_r
       "lxvd2x		41, %7, %2	\n\t"	// x1_r, x1_i

       "xvmaddadp	37, 42, 2	\n\t"	// x2_r * y2_i , x2_i * y2_r
       "lxvd2x		42, %8, %2	\n\t"	// x2_r, x2_i
       "xvmaddadp	39, 43, 3	\n\t"	// x3_r * y3_i , x3_i * y3_r
       "lxvd2x		43, %9, %2	\n\t"	// x3_r, x3_i

       "xxswapd		0,48		\n\t"	// y0_i, y0_r
       "xxswapd		1,49		\n\t"	// y1_i, y1_r

       "addi		%2, %2, 64	\n\t"
       "addi		%3, %3, 64	\n\t"

       "xxswapd		2,50		\n\t"	// y2_i, y2_r
       "xxswapd		3,51		\n\t"	// y3_i, y3_r

       "xvmaddadp	32, 44, 4	\n\t"	// x0_r * y0_r , x0_i * y0_i
       "lxvd2x		4, 0, %3	\n\t"	// y0_r, y0_i
       "xvmaddadp	34, 45, 5	\n\t"	// x1_r * y1_r , x1_i * y1_i
       "lxvd2x		5, %7, %3	\n\t"	// y1_r, y1_i
       "xvmaddadp	36, 46, 6	\n\t"	// x2_r * y2_r , x2_i * y2_i
       "lxvd2x		6, %8, %3	\n\t"	// y2_r, y2_i
       "xvmaddadp	38, 47, 7	\n\t"	// x3_r * y3_r , x3_i * y3_i
       "lxvd2x		7, %9, %3	\n\t"	// y3_r, y3_i

       "xvmaddadp	33, 44, 8	\n\t"	// x0_r * y0_i , x0_i * y0_r
       "lxvd2x		44, 0, %2	\n\t"	// x0_r, x0_i
       "xvmaddadp	35, 45, 9	\n\t"	// x1_r * y1_i , x1_i * y1_r
       "lxvd2x		45, %7, %2	\n\t"	// x1_r, x1_i
       "xvmaddadp	37, 46, 10	\n\t"	// x2_r * y2_i , x2_i * y2_r
       "lxvd2x		46, %8, %2	\n\t"	// x2_r, x2_i
       "xvmaddadp	39, 47, 11	\n\t"	// x3_r * y3_i , x3_i * y3_r
       "lxvd2x		47, %9, %2	\n\t"	// x3_r, x3_i

       "xxswapd		8,4		\n\t"	// y0_i, y0_r
       "xxswapd		9,5		\n\t"	// y1_i, y1_r

       "addi		%2, %2, 64	\n\t"
       "addi		%3, %3, 64	\n\t"

       "xxswapd		10,6		\n\t"	// y2_i, y2_r
       "xxswapd		11,7		\n\t"	// y3_i, y3_r

       "addic.		%1, %1, -8	\n\t"
       "bgt		1b		\n"

     "2:				\n\t"

       "xvmaddadp	32, 40, 48	\n\t"	// x0_r * y0_r , x0_i * y0_i
       "xvmaddadp	34, 41, 49	\n\t"	// x1_r * y1_r , x1_i * y1_i
       "xvmaddadp	36, 42, 50	\n\t"	// x2_r * y2_r , x2_i * y2_i
       "xvmaddadp	38, 43, 51	\n\t"	// x3_r * y3_r , x3_i * y3_i

       "xvmaddadp	33, 40, 0	\n\t"	// x0_r * y0_i , x0_i * y0_r
       "xvmaddadp	35, 41, 1	\n\t"	// x1_r * y1_i , x1_i * y1_r
       "xvmaddadp	37, 42, 2	\n\t"	// x2_r * y2_i , x2_i * y2_r
       "xvmaddadp	39, 43, 3	\n\t"	// x3_r * y3_i , x3_i * y3_r

       "xvmaddadp	32, 44, 4	\n\t"	// x0_r * y0_r , x0_i * y0_i
       "xvmaddadp	34, 45, 5	\n\t"	// x1_r * y1_r , x1_i * y1_i
       "xvmaddadp	36, 46, 6	\n\t"	// x2_r * y2_r , x2_i * y2_i
       "xvmaddadp	38, 47, 7	\n\t"	// x3_r * y3_r , x3_i * y3_i

       "xvmaddadp	33, 44, 8	\n\t"	// x0_r * y0_i , x0_i * y0_r
       "xvmaddadp	35, 45, 9	\n\t"	// x1_r * y1_i , x1_i * y1_r
       "xvmaddadp	37, 46, 10	\n\t"	// x2_r * y2_i , x2_i * y2_r
       "xvmaddadp	39, 47, 11	\n\t"	// x3_r * y3_i , x3_i * y3_r

       "xvadddp		32, 32, 34	\n\t"
       "xvadddp		36, 36, 38	\n\t"

       "xvadddp		33, 33, 35	\n\t"
       "xvadddp		37, 37, 39	\n\t"

       "xvadddp		32, 32, 36	\n\t"
       "xvadddp		33, 33, 37	\n\t"

       "stxvd2x		32, 0, %6	\n\t"
       "stxvd2x		33, %7, %6	\n"

     "#n=%1 x=%4=%2 y=%5=%3 dot=%0=%6 o16=%7 o32=%8 o48=%9"
     :
       "=m" (*dot),
       "+r" (n),	// 1
       "+b" (x),	// 2
       "+b" (y)		// 3
     :
       "m" (*x),
       "m" (*y),
       "b" (dot),	// 6
       "b" (16),	// 7
       "b" (32),	// 8
       "b" (48)		// 9
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37","vs38","vs39",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47",
       "vs48","vs49","vs50","vs51","vs0","vs1","vs2","vs3",
       "vs4","vs5","vs6","vs7","vs8","vs9","vs10","vs11"
     );
}
