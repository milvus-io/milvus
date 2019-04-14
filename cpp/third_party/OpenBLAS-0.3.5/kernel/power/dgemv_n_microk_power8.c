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
* 2016/03/30 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/

#define HAVE_KERNEL_4x4 1

static void dgemv_kernel_4x4 (long n, double *ap, long lda, double *x, double *y, double alpha)
{
  double *a0;
  double *a1;
  double *a2;
  double *a3;

  __asm__
    (
       "lxvd2x		34, 0, %10	\n\t"	// x0, x1
       "lxvd2x		35, %11, %10	\n\t"	// x2, x3
       "xxspltd		32, %x9, 0	\n\t"	// alpha, alpha

       "sldi		%6, %13, 3	\n\t"	// lda * sizeof (double)

       "xvmuldp		34, 34, 32	\n\t"	// x0 * alpha, x1 * alpha
       "xvmuldp		35, 35, 32	\n\t"	// x2 * alpha, x3 * alpha

       "add		%4, %3, %6	\n\t"	// a0 = ap, a1 = a0 + lda
       "add		%6, %6, %6	\n\t"	// 2 * lda

       "xxspltd		32, 34, 0	\n\t"	// x0 * alpha, x0 * alpha
       "xxspltd		33, 34, 1	\n\t"	// x1 * alpha, x1 * alpha
       "xxspltd		34, 35, 0	\n\t"	// x2 * alpha, x2 * alpha
       "xxspltd		35, 35, 1	\n\t"	// x3 * alpha, x3 * alpha

       "add		%5, %3, %6	\n\t"	// a2 = a0 + 2 * lda
       "add		%6, %4, %6	\n\t"	// a3 = a1 + 2 * lda

       "dcbt		0, %3		\n\t"
       "dcbt		0, %4		\n\t"
       "dcbt		0, %5		\n\t"
       "dcbt		0, %6		\n\t"

       "lxvd2x		40, 0, %3	\n\t"	// a0[0], a0[1]
       "lxvd2x		41, %11, %3	\n\t"	// a0[2], a0[3]

       "lxvd2x		42, 0, %4	\n\t"	// a1[0], a1[1]
       "lxvd2x		43, %11, %4	\n\t"	// a1[2], a1[3]

       "lxvd2x		44, 0, %5	\n\t"	// a2[0], a2[1]
       "lxvd2x		45, %11, %5	\n\t"	// a2[2], a2[3]

       "lxvd2x		46, 0, %6	\n\t"	// a3[0], a3[1]
       "lxvd2x		47, %11, %6	\n\t"	// a3[2], a3[3]

       "dcbt		0, %2		\n\t"

       "addi		%3, %3, 32	\n\t"
       "addi		%4, %4, 32	\n\t"
       "addi		%5, %5, 32	\n\t"
       "addi		%6, %6, 32	\n\t"

       "addic.		%1, %1, -4	\n\t"
       "ble		2f		\n\t"

       ".p2align	5		\n"
     "1:				\n\t"

       "lxvd2x		36, 0, %2	\n\t"	// y0, y1
       "lxvd2x		37, %11, %2	\n\t"	// y2, y3

       "xvmaddadp 	36, 40, 32	\n\t"
       "xvmaddadp 	37, 41, 32	\n\t"

       "lxvd2x		40, 0, %3	\n\t"	// a0[0], a0[1]
       "lxvd2x		41, %11, %3	\n\t"	// a0[2], a0[3]

       "xvmaddadp 	36, 42, 33	\n\t"
       "addi		%3, %3, 32	\n\t"
       "xvmaddadp 	37, 43, 33	\n\t"

       "lxvd2x		42, 0, %4	\n\t"	// a1[0], a1[1]
       "lxvd2x		43, %11, %4	\n\t"	// a1[2], a1[3]

       "xvmaddadp 	36, 44, 34	\n\t"
       "addi		%4, %4, 32	\n\t"
       "xvmaddadp 	37, 45, 34	\n\t"

       "lxvd2x		44, 0, %5	\n\t"	// a2[0], a2[1]
       "lxvd2x		45, %11, %5	\n\t"	// a2[2], a2[3]

       "xvmaddadp 	36, 46, 35	\n\t"
       "addi		%5, %5, 32	\n\t"
       "xvmaddadp 	37, 47, 35	\n\t"

       "stxvd2x		36, 0, %2	\n\t"	// y0, y1
       "stxvd2x		37, %11, %2	\n\t"	// y2, y3

       "lxvd2x		46, 0, %6	\n\t"	// a3[0], a3[1]
       "lxvd2x		47, %11, %6	\n\t"	// a3[2], a3[3]

       "addi		%6, %6, 32	\n\t"
       "addi		%2, %2, 32	\n\t"

       "addic.		%1, %1, -4	\n\t"
       "ble		2f		\n\t"


       "lxvd2x		36, 0, %2	\n\t"	// y0, y1
       "lxvd2x		37, %11, %2	\n\t"	// y2, y3

       "xvmaddadp 	36, 40, 32	\n\t"
       "xvmaddadp 	37, 41, 32	\n\t"

       "lxvd2x		40, 0, %3	\n\t"	// a0[0], a0[1]
       "lxvd2x		41, %11, %3	\n\t"	// a0[2], a0[3]

       "xvmaddadp 	36, 42, 33	\n\t"
       "addi		%3, %3, 32	\n\t"
       "xvmaddadp 	37, 43, 33	\n\t"

       "lxvd2x		42, 0, %4	\n\t"	// a1[0], a1[1]
       "lxvd2x		43, %11, %4	\n\t"	// a1[2], a1[3]

       "xvmaddadp 	36, 44, 34	\n\t"
       "addi		%4, %4, 32	\n\t"
       "xvmaddadp 	37, 45, 34	\n\t"

       "lxvd2x		44, 0, %5	\n\t"	// a2[0], a2[1]
       "lxvd2x		45, %11, %5	\n\t"	// a2[2], a2[3]

       "xvmaddadp 	36, 46, 35	\n\t"
       "addi		%5, %5, 32	\n\t"
       "xvmaddadp 	37, 47, 35	\n\t"

       "stxvd2x		36, 0, %2	\n\t"	// y0, y1
       "stxvd2x		37, %11, %2	\n\t"	// y2, y3

       "lxvd2x		46, 0, %6	\n\t"	// a3[0], a3[1]
       "lxvd2x		47, %11, %6	\n\t"	// a3[2], a3[3]

       "addi		%6, %6, 32	\n\t"
       "addi		%2, %2, 32	\n\t"

       "addic.		%1, %1, -4	\n\t"
       "ble		2f		\n\t"


       "lxvd2x		36, 0, %2	\n\t"	// y0, y1
       "lxvd2x		37, %11, %2	\n\t"	// y2, y3

       "xvmaddadp 	36, 40, 32	\n\t"
       "xvmaddadp 	37, 41, 32	\n\t"

       "lxvd2x		40, 0, %3	\n\t"	// a0[0], a0[1]
       "lxvd2x		41, %11, %3	\n\t"	// a0[2], a0[3]

       "xvmaddadp 	36, 42, 33	\n\t"
       "addi		%3, %3, 32	\n\t"
       "xvmaddadp 	37, 43, 33	\n\t"

       "lxvd2x		42, 0, %4	\n\t"	// a1[0], a1[1]
       "lxvd2x		43, %11, %4	\n\t"	// a1[2], a1[3]

       "xvmaddadp 	36, 44, 34	\n\t"
       "addi		%4, %4, 32	\n\t"
       "xvmaddadp 	37, 45, 34	\n\t"

       "lxvd2x		44, 0, %5	\n\t"	// a2[0], a2[1]
       "lxvd2x		45, %11, %5	\n\t"	// a2[2], a2[3]

       "xvmaddadp 	36, 46, 35	\n\t"
       "addi		%5, %5, 32	\n\t"
       "xvmaddadp 	37, 47, 35	\n\t"

       "stxvd2x		36, 0, %2	\n\t"	// y0, y1
       "stxvd2x		37, %11, %2	\n\t"	// y2, y3

       "lxvd2x		46, 0, %6	\n\t"	// a3[0], a3[1]
       "lxvd2x		47, %11, %6	\n\t"	// a3[2], a3[3]

       "addi		%6, %6, 32	\n\t"
       "addi		%2, %2, 32	\n\t"

       "addic.		%1, %1, -4	\n\t"
       "ble		2f		\n\t"


       "lxvd2x		36, 0, %2	\n\t"	// y0, y1
       "lxvd2x		37, %11, %2	\n\t"	// y2, y3

       "xvmaddadp 	36, 40, 32	\n\t"
       "xvmaddadp 	37, 41, 32	\n\t"

       "lxvd2x		40, 0, %3	\n\t"	// a0[0], a0[1]
       "lxvd2x		41, %11, %3	\n\t"	// a0[2], a0[3]

       "xvmaddadp 	36, 42, 33	\n\t"
       "addi		%3, %3, 32	\n\t"
       "xvmaddadp 	37, 43, 33	\n\t"

       "lxvd2x		42, 0, %4	\n\t"	// a1[0], a1[1]
       "lxvd2x		43, %11, %4	\n\t"	// a1[2], a1[3]

       "xvmaddadp 	36, 44, 34	\n\t"
       "addi		%4, %4, 32	\n\t"
       "xvmaddadp 	37, 45, 34	\n\t"

       "lxvd2x		44, 0, %5	\n\t"	// a2[0], a2[1]
       "lxvd2x		45, %11, %5	\n\t"	// a2[2], a2[3]

       "xvmaddadp 	36, 46, 35	\n\t"
       "addi		%5, %5, 32	\n\t"
       "xvmaddadp 	37, 47, 35	\n\t"

       "stxvd2x		36, 0, %2	\n\t"	// y0, y1
       "stxvd2x		37, %11, %2	\n\t"	// y2, y3

       "lxvd2x		46, 0, %6	\n\t"	// a3[0], a3[1]
       "lxvd2x		47, %11, %6	\n\t"	// a3[2], a3[3]

       "addi		%6, %6, 32	\n\t"
       "addi		%2, %2, 32	\n\t"

       "addic.		%1, %1, -4	\n\t"
       "bgt		1b		\n"

     "2:				\n\t"

       "lxvd2x		36, 0, %2	\n\t"	// y0, y1
       "lxvd2x		37, %11, %2	\n\t"	// y2, y3

       "xvmaddadp 	36, 40, 32	\n\t"
       "xvmaddadp 	37, 41, 32	\n\t"

       "xvmaddadp 	36, 42, 33	\n\t"
       "xvmaddadp 	37, 43, 33	\n\t"

       "xvmaddadp 	36, 44, 34	\n\t"
       "xvmaddadp 	37, 45, 34	\n\t"

       "xvmaddadp 	36, 46, 35	\n\t"
       "xvmaddadp 	37, 47, 35	\n\t"

       "stxvd2x		36, 0, %2	\n\t"	// y0, y1
       "stxvd2x		37, %11, %2	\n"	// y2, y3

     "#n=%1 ap=%8=%12 lda=%13 x=%7=%10 y=%0=%2 alpha=%9 o16=%11\n"
     "#a0=%3 a1=%4 a2=%5 a3=%6"
     :
       "+m" (*y),
       "+r" (n),	// 1
       "+b" (y),	// 2
       "=b" (a0),	// 3
       "=b" (a1),	// 4
       "=&b" (a2),	// 5
       "=&b" (a3)	// 6
     :
       "m" (*x),
       "m" (*ap),
       "d" (alpha),	// 9
       "r" (x),		// 10
       "b" (16),	// 11
       "3" (ap),	// 12
       "4" (lda)	// 13
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47"
     );
}
