/***************************************************************************
Copyright (c) 2017, The OpenBLAS Project
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

#include "common.h"

#include <arm_neon.h>
#define	N	"x0"	/* vector length */
#define	X	"x1"	/* X vector address */
#define	INC_X	"x2"	/* X stride */
#define	Y	"x3"	/* Y vector address */
#define	INC_Y	"x4"	/* Y stride */
#define J	"x5"	/* loop variable */

/*******************************************************************************
* Macro definitions
*******************************************************************************/
#if !defined(COMPLEX)
#if !defined(DOUBLE)
#define TMPF		"s0"
#define INC_SHIFT	"2"
#define N_DIV_SHIFT	"2"
#define N_REM_MASK	"3"
#else
#define TMPF		"d0"
#define INC_SHIFT	"3"
#define N_DIV_SHIFT	"1"
#define N_REM_MASK	"1"
#endif
#else
#if !defined(DOUBLE)
#define TMPF		"d0"
#define INC_SHIFT	"3"
#define N_DIV_SHIFT	"1"
#define N_REM_MASK	"1"
#else
#define TMPF		"q0"
#define INC_SHIFT	"4"
#define N_DIV_SHIFT	"0"
#define N_REM_MASK	"0"
#endif
#endif

#define KERNEL_F1					\
	"ldr	"TMPF", ["X"]			\n"	\
	"add	"X", "X", "INC_X"		\n"	\
	"str	"TMPF", ["Y"]			\n"	\
	"add	"Y", "Y", "INC_Y"		\n"

#define KERNEL_F					\
	"ldr	q0, ["X"], #16			\n"	\
	"str	q0, ["Y"], #16			\n"

#define INIT						\
	"lsl	"INC_X", "INC_X", #"INC_SHIFT"	\n"	\
	"lsl	"INC_Y", "INC_Y", #"INC_SHIFT"	\n"


static int do_copy(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
{
	if ( n < 0 )  return 0;

	__asm__ __volatile__ (
	"	mov	"N", %[N_]			\n"
	"	mov	"X", %[X_]			\n"
	"	mov	"INC_X", %[INCX_]		\n"
	"	mov	"Y", %[Y_]			\n"
	"	mov	"INC_Y", %[INCY_]		\n"
	"	cmp	"N", xzr			\n"
	"	ble	8f //copy_kernel_L999		\n"
	"	cmp	"INC_X", #1			\n"
	"	bne	4f //copy_kernel_S_BEGIN	\n"
	"	cmp	"INC_Y", #1			\n"
	"	bne	4f //copy_kernel_S_BEGIN	\n"

	"// .Lcopy_kernel_F_BEGIN:			\n"
	"	"INIT"					\n"
	"	asr	"J", "N", #"N_DIV_SHIFT"	\n"
	"	cmp	"J", xzr			\n"
	"	beq	2f //copy_kernel_F1		\n"
	"	.align 5				\n"

	"1: //copy_kernel_F:				\n"
	"	"KERNEL_F"				\n"
	"	subs	"J", "J", #1			\n"
	"	bne	1b //copy_kernel_F		\n"

	"2: //copy_kernel_F1:				\n"
#if defined(COMPLEX) && defined(DOUBLE)
	"	b	8f //copy_kernel_L999		\n"
#else
	"	ands	"J", "N", #"N_REM_MASK"		\n"
	"	ble	8f //copy_kernel_L999		\n"
#endif

	"3: //copy_kernel_F10:				\n"
	"	"KERNEL_F1"				\n"
	"	subs	"J", "J", #1			\n"
	"	bne	3b //copy_kernel_F10		\n"
	"	b	8f //copy_kernel_L999		\n"

	"4: //copy_kernel_S_BEGIN:			\n"
	"	"INIT"					\n"
	"	asr	"J", "N", #2			\n"
	"	cmp	"J", xzr			\n"
	"	ble	6f //copy_kernel_S1		\n"

	"5: //copy_kernel_S4:				\n"
	"	"KERNEL_F1"				\n"
	"	"KERNEL_F1"				\n"
	"	"KERNEL_F1"				\n"
	"	"KERNEL_F1"				\n"
	"	subs	"J", "J", #1			\n"
	"	bne	5b //copy_kernel_S4		\n"

	"6: //copy_kernel_S1:				\n"
	"	ands	"J", "N", #3			\n"
	"	ble	8f //copy_kernel_L999		\n"

	"7: //copy_kernel_S10:				\n"
	"	"KERNEL_F1"				\n"
	"	subs	"J", "J", #1			\n"
	"	bne	7b //copy_kernel_S10		\n"

	"8: //copy_kernel_L999:				\n"

	:
	: [N_]    "r"  (n),		//%1
	  [X_]    "r"  (x),		//%2
	  [INCX_] "r"  (inc_x),		//%3
	  [Y_]    "r"  (y),		//%4
	  [INCY_] "r"  (inc_y)		//%5
	: "cc",
	  "memory",
	  "x0", "x1", "x2", "x3", "x4", "x5",
	  "d0"
	);

	return 0;
}

#if defined(SMP)
static int copy_thread_function(BLASLONG n, BLASLONG dummy0,
	BLASLONG dummy1, FLOAT dummy2, FLOAT *x, BLASLONG inc_x, FLOAT *y,
	BLASLONG inc_y, FLOAT *dummy3, BLASLONG dummy4)
{
	do_copy(n, x, inc_x, y, inc_y);

	return 0;
}
#endif

int CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
{
#if defined(SMP)
	int nthreads;
	FLOAT dummy_alpha;
#endif

	if (n <= 0) return 0;

#if defined(SMP)
	if (inc_x == 0 || n <= 10000)
		nthreads = 1;
	else
		nthreads = num_cpu_avail(1);

	if (nthreads == 1) {
		do_copy(n, x, inc_x, y, inc_y);
	} else {
		int mode = 0;

#if !defined(COMPLEX)
		mode = BLAS_REAL;
#else
		mode = BLAS_COMPLEX;
#endif
#if !defined(DOUBLE)
		mode |= BLAS_SINGLE;
#else
		mode |= BLAS_DOUBLE;
#endif

		blas_level1_thread(mode, n, 0, 0, &dummy_alpha,
				   x, inc_x, y, inc_y, NULL, 0,
				   ( void *)copy_thread_function, nthreads);
	}
#else
	do_copy(n, x, inc_x, y, inc_y);
#endif

	return 0;
}
