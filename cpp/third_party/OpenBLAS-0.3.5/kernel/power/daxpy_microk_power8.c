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
* 2016/03/22 Werner Saar (wernsaar@googlemail.com)
* 	 BLASTEST 		: OK
* 	 CTEST			: OK
* 	 TEST			: OK
*	 LAPACK-TEST		: OK
**************************************************************************************/


#define HAVE_KERNEL_8 1

static void daxpy_kernel_8 (long n, double *x, double *y, double alpha)
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
  __vector double t12;
  __vector double t13;
  __vector double t14;
  __vector double t15;
  __vector double t16;

  __asm__
    (
       "xxspltd		%x4, %x22, 0	\n\t"

       "dcbt		0, %2		\n\t"
       "dcbt		0, %3		\n\t"

       "lxvd2x		%x5, 0, %2	\n\t"
       "lxvd2x		%x6, %23, %2	\n\t"
       "lxvd2x		%x7, %24, %2	\n\t"
       "lxvd2x		%x8, %25, %2	\n\t"

       "lxvd2x		%x13, 0, %3	\n\t"
       "lxvd2x		%x14, %23, %3	\n\t"
       "lxvd2x		%x15, %24, %3	\n\t"
       "lxvd2x		%x16, %25, %3	\n\t"

       "addi		%2, %2, 64	\n\t"
       "addi		%3, %3, 64	\n\t"

       "lxvd2x		%x9, 0, %2	\n\t"
       "lxvd2x		%x10, %23, %2	\n\t"
       "lxvd2x		%x11, %24, %2	\n\t"
       "lxvd2x		%x12, %25, %2	\n\t"

       "lxvd2x		%x17, 0, %3	\n\t"
       "lxvd2x		%x18, %23, %3	\n\t"
       "lxvd2x		%x19, %24, %3	\n\t"
       "lxvd2x		%x20, %25, %3	\n\t"

       "addi		%2, %2, 64	\n\t"
       "addi		%3, %3, -64	\n\t"

       "addic.		%1, %1, -16	\n\t"
       "ble		2f		\n\t"

       ".align 5			\n"
     "1:				\n\t"

       "xvmaddadp	%x13, %x5, %x4	\n\t"
       "xvmaddadp	%x14, %x6, %x4	\n\t"

       "lxvd2x		%x5, 0, %2	\n\t"
       "lxvd2x		%x6, %23, %2	\n\t"

       "stxvd2x		%x13, 0, %3	\n\t"
       "stxvd2x		%x14, %23, %3	\n\t"

       "xvmaddadp	%x15, %x7, %x4	\n\t"
       "xvmaddadp	%x16, %x8, %x4	\n\t"

       "lxvd2x		%x7, %24, %2	\n\t"
       "lxvd2x		%x8, %25, %2	\n\t"

       "stxvd2x		%x15, %24, %3	\n\t"
       "stxvd2x		%x16, %25, %3	\n\t"

       "addi		%2, %2, 64	\n\t"
       "addi		%3, %3, 128	\n\t"

       "lxvd2x		%x13, 0, %3	\n\t"
       "lxvd2x		%x14, %23, %3	\n\t"
       "lxvd2x		%x15, %24, %3	\n\t"
       "lxvd2x		%x16, %25, %3	\n\t"

       "addi		%3, %3, -64	\n\t"

       "xvmaddadp	%x17, %x9, %x4	\n\t"
       "xvmaddadp	%x18, %x10, %x4	\n\t"

       "lxvd2x		%x9, 0, %2	\n\t"
       "lxvd2x		%x10, %23, %2	\n\t"

       "stxvd2x		%x17, 0, %3	\n\t"
       "stxvd2x		%x18, %23, %3	\n\t"

       "xvmaddadp	%x19, %x11, %x4	\n\t"
       "xvmaddadp	%x20, %x12, %x4	\n\t"

       "lxvd2x		%x11, %24, %2	\n\t"
       "lxvd2x		%x12, %25, %2	\n\t"

       "stxvd2x		%x19, %24, %3	\n\t"
       "stxvd2x		%x20, %25, %3	\n\t"

       "addi		%2, %2, 64	\n\t"
       "addi		%3, %3, 128	\n\t"

       "lxvd2x		%x17, 0, %3	\n\t"
       "lxvd2x		%x18, %23, %3	\n\t"
       "lxvd2x		%x19, %24, %3	\n\t"
       "lxvd2x		%x20, %25, %3	\n\t"

       "addi		%3, %3, -64	\n\t"

       "addic.		%1, %1, -16	\n\t"
       "bgt		1b		\n"

     "2:				\n\t"

       "xvmaddadp	%x13, %x5, %x4	\n\t"
       "xvmaddadp	%x14, %x6, %x4	\n\t"
       "xvmaddadp	%x15, %x7, %x4	\n\t"
       "xvmaddadp	%x16, %x8, %x4	\n\t"

       "xvmaddadp	%x17, %x9, %x4	\n\t"
       "xvmaddadp	%x18, %x10, %x4	\n\t"
       "xvmaddadp	%x19, %x11, %x4	\n\t"
       "xvmaddadp	%x20, %x12, %x4	\n\t"

       "stxvd2x		%x13, 0, %3	\n\t"
       "stxvd2x		%x14, %23, %3	\n\t"
       "stxvd2x		%x15, %24, %3	\n\t"
       "stxvd2x		%x16, %25, %3	\n\t"

       "addi		%3, %3, 64	\n\t"

       "stxvd2x		%x17, 0, %3	\n\t"
       "stxvd2x		%x18, %23, %3	\n\t"
       "stxvd2x		%x19, %24, %3	\n\t"
       "stxvd2x		%x20, %25, %3	\n"

     "#n=%1 x=%21=%2 y=%0=%3 alpha=%22 o16=%23 o32=%24 o48=%25\n"
     "#t0=%x4 t1=%x5 t2=%x6 t3=%x7 t4=%x8 t5=%x9 t6=%x10 t7=%x11 t8=%x12 t9=%x13 t10=%x14 t11=%x15 t12=%x16 t13=%x17 t14=%x18 t15=%x19 t16=%x20"
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
       "=wa" (t12),	// 16
       "=wa" (t13),	// 17
       "=wa" (t14),	// 18
       "=wa" (t15),	// 19
       "=wa" (t16)	// 20
     :
       "m" (*x),
       "d" (alpha),	// 22
       "b" (16),	// 23
       "b" (32),	// 24
       "b" (48)		// 25
     :
       "cr0"
     );

}


