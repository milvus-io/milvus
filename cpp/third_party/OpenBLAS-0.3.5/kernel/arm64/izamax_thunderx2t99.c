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

#define N		"x0"	/* vector length */
#define X		"x1"	/* "X" vector address */
#define INC_X		"x2"	/* "X" stride */
#define INDEX		"x3"	/* index of max/min value */
#define Z		"x4"	/* vector index */
#define J		"x5"	/* loop variable */

#if !defined(DOUBLE)
#define MAXF		"s0"
#define TMPF0		"s1"
#define TMPF0V		"v1.2s"
#define TMPF1		"d4"
#define TMPF1V		"v4.2s"
#define N_KERNEL_SIZE	"32"
#define SZ		"8"
#define N_DIV_SHIFT	"5"
#define N_REM_MASK	"31"
#define INC_SHIFT	"3"
#else
#define MAXF		"d0"
#define TMPF0		"d1"
#define TMPF0V		"v1.2d"
#define TMPF1		"q4"
#define TMPF1V		"v4.2d"
#define N_KERNEL_SIZE	"16"
#define SZ		"16"
#define N_DIV_SHIFT	"4"
#define N_REM_MASK	"15"
#define INC_SHIFT	"4"
#endif

/******************************************************************************/

#if !defined(DOUBLE)
#define KERNEL_F						\
	"ldp	q2, q3, ["X"]				\n"	\
	"ldp	q4, q5, ["X", #32]			\n"	\
	"ldp	q6, q7, ["X", #64]			\n"	\
	"ldp	q16, q17, ["X", #96]			\n"	\
	"ldp	q18, q19, ["X", #128]			\n"	\
	"ldp	q20, q21, ["X", #160]			\n"	\
	"ldp	q22, q23, ["X", #192]			\n"	\
	"ldp	q24, q25, ["X", #224]			\n"	\
	"add	"X", "X", #256				\n"	\
	"fabs	v2.4s, v2.4s				\n"	\
	"fabs	v3.4s, v3.4s				\n"	\
	"fabs	v4.4s, v4.4s				\n"	\
	"fabs	v5.4s, v5.4s				\n"	\
	"fabs	v6.4s, v6.4s				\n"	\
	"fabs	v7.4s, v7.4s				\n"	\
	"fabs	v16.4s, v16.4s				\n"	\
	"fabs	v17.4s, v17.4s				\n"	\
	"fabs	v18.4s, v18.4s				\n"	\
	"fabs	v19.4s, v19.4s				\n"	\
	"fabs	v20.4s, v20.4s				\n"	\
	"fabs	v21.4s, v21.4s				\n"	\
	"fabs	v22.4s, v22.4s				\n"	\
	"fabs	v23.4s, v23.4s				\n"	\
	"fabs	v24.4s, v24.4s				\n"	\
	"fabs	v25.4s, v25.4s				\n"	\
	"faddp	v2.4s, v2.4s, v3.4s			\n"	\
	"faddp	v4.4s, v4.4s, v5.4s			\n"	\
	"faddp	v6.4s, v6.4s, v7.4s			\n"	\
	"faddp	v16.4s, v16.4s, v17.4s			\n"	\
	"faddp	v18.4s, v18.4s, v19.4s			\n"	\
	"faddp	v20.4s, v20.4s, v21.4s			\n"	\
	"faddp	v22.4s, v22.4s, v23.4s			\n"	\
	"faddp	v24.4s, v24.4s, v25.4s			\n"	\
	"fmax	v2.4s, v2.4s, v4.4s			\n"	\
	"fmax	v6.4s, v6.4s, v16.4s			\n"	\
	"fmax	v18.4s, v18.4s, v20.4s			\n"	\
	"fmax	v22.4s, v22.4s, v24.4s			\n"	\
	"PRFM	PLDL1KEEP, ["X", #1024]			\n"	\
	"PRFM	PLDL1KEEP, ["X", #1024+64]		\n"	\
	"PRFM	PLDL1KEEP, ["X", #1024+128]		\n"	\
	"PRFM	PLDL1KEEP, ["X", #1024+192]		\n"	\
	"fmax	v2.4s, v2.4s, v6.4s			\n"	\
	"fmax	v18.4s, v18.4s, v22.4s			\n"	\
	"fmax	v2.4s, v2.4s, v18.4s			\n"	\
	"fmaxv	"TMPF0", v2.4s				\n"	\
	"fcmp	"MAXF", "TMPF0"				\n"	\
	"fcsel	"MAXF", "MAXF", "TMPF0", ge		\n"	\
	"csel	"INDEX", "INDEX", "Z", ge		\n"	\
	"add	"Z", "Z", #"N_KERNEL_SIZE"		\n"

#else

#define KERNEL_F						\
	"ldp	q2, q3, ["X"]				\n"	\
	"ldp	q4, q5, ["X", #32]			\n"	\
	"ldp	q6, q7, ["X", #64]			\n"	\
	"ldp	q16, q17, ["X", #96]			\n"	\
	"ldp	q18, q19, ["X", #128]			\n"	\
	"ldp	q20, q21, ["X", #160]			\n"	\
	"ldp	q22, q23, ["X", #192]			\n"	\
	"ldp	q24, q25, ["X", #224]			\n"	\
	"add	"X", "X", #256				\n"	\
	"fabs	v2.2d, v2.2d				\n"	\
	"fabs	v3.2d, v3.2d				\n"	\
	"fabs	v4.2d, v4.2d				\n"	\
	"fabs	v5.2d, v5.2d				\n"	\
	"fabs	v6.2d, v6.2d				\n"	\
	"fabs	v7.2d, v7.2d				\n"	\
	"fabs	v16.2d, v16.2d				\n"	\
	"fabs	v17.2d, v17.2d				\n"	\
	"fabs	v18.2d, v18.2d				\n"	\
	"fabs	v19.2d, v19.2d				\n"	\
	"fabs	v20.2d, v20.2d				\n"	\
	"fabs	v21.2d, v21.2d				\n"	\
	"fabs	v22.2d, v22.2d				\n"	\
	"fabs	v23.2d, v23.2d				\n"	\
	"fabs	v24.2d, v24.2d				\n"	\
	"fabs	v25.2d, v25.2d				\n"	\
	"faddp	v2.2d, v2.2d, v3.2d			\n"	\
	"faddp	v4.2d, v4.2d, v5.2d			\n"	\
	"faddp	v6.2d, v6.2d, v7.2d			\n"	\
	"faddp	v16.2d, v16.2d, v17.2d			\n"	\
	"faddp	v18.2d, v18.2d, v19.2d			\n"	\
	"faddp	v20.2d, v20.2d, v21.2d			\n"	\
	"faddp	v22.2d, v22.2d, v23.2d			\n"	\
	"faddp	v24.2d, v24.2d, v25.2d			\n"	\
	"fmax	v2.2d, v2.2d, v4.2d			\n"	\
	"fmax	v6.2d, v6.2d, v16.2d			\n"	\
	"fmax	v18.2d, v18.2d, v20.2d			\n"	\
	"fmax	v22.2d, v22.2d, v24.2d			\n"	\
	"PRFM	PLDL1KEEP, ["X", #1024]			\n"	\
	"PRFM	PLDL1KEEP, ["X", #1024+64]		\n"	\
	"PRFM	PLDL1KEEP, ["X", #1024+128]		\n"	\
	"PRFM	PLDL1KEEP, ["X", #1024+192]		\n"	\
	"fmax	v2.2d, v2.2d, v6.2d			\n"	\
	"fmax	v18.2d, v18.2d, v22.2d			\n"	\
	"fmax	v2.2d, v2.2d, v18.2d			\n"	\
	"ins	v3.d[0], v2.d[1]			\n"	\
	"fmax	"TMPF0", d3, d2				\n"	\
	"fcmp	"MAXF", "TMPF0"				\n"	\
	"fcsel	"MAXF", "MAXF", "TMPF0", ge		\n"	\
	"csel	"INDEX", "INDEX", "Z", ge		\n"	\
	"add	"Z", "Z", #"N_KERNEL_SIZE"		\n"
#endif

#define KERNEL_F_FINALIZE					\
	"sub	x6, "INDEX", #1				\n"	\
	"lsl	x6, x6, #"INC_SHIFT" 			\n"	\
	"add	x7, x7, x6				\n"	\
	"mov	x6, #0					\n"	\
	"1:						\n"	\
	"add	x6, x6, #1				\n"	\
	"cmp	x6, #"N_KERNEL_SIZE"			\n"	\
	"bge	2f					\n"	\
	"ldr	"TMPF1", [x7] 				\n"	\
	"fabs	"TMPF1V", "TMPF1V"			\n"	\
	"faddp	"TMPF0V", "TMPF1V", "TMPF1V"		\n"	\
	"fcmp	"MAXF", "TMPF0"				\n"	\
	"add	x7, x7, #"SZ"				\n"	\
	"bne	1b					\n"	\
	"2:						\n"	\
	"sub	x6, x6, #1				\n"	\
	"add	"INDEX", "INDEX", x6			\n"


#define INIT							\
	"lsl	"INC_X", "INC_X", #"INC_SHIFT"		\n"	\
	"ldr	"TMPF1", ["X"] 				\n"	\
	"fabs	"TMPF1V", "TMPF1V"			\n"	\
	"faddp	"TMPF0V", "TMPF1V", "TMPF1V"		\n"	\
	"fmov	"MAXF" , "TMPF0"			\n"	\
	"add	"X", "X", "INC_X"			\n"	\
	"mov	"Z", #1					\n"	\
	"mov	"INDEX", "Z"				\n"	\
	"fabs	"MAXF", "MAXF"				\n"


#define KERNEL_S1						\
	"ldr	"TMPF1", ["X"]				\n"	\
	"add	"X", "X", "INC_X"			\n"	\
	"add	"Z", "Z", #1				\n"	\
	"fabs	"TMPF1V", "TMPF1V"			\n"	\
	"faddp	"TMPF0V", "TMPF1V", "TMPF1V"		\n"	\
	"fcmp	"MAXF", "TMPF0"				\n"	\
	"fcsel	"MAXF", "MAXF", "TMPF0", ge		\n"	\
	"csel	"INDEX", "INDEX", "Z", ge		\n"


#if defined(SMP)
extern int blas_level1_thread_with_return_value(int mode, BLASLONG m, BLASLONG n,
	BLASLONG k, void *alpha, void *a, BLASLONG lda, void *b, BLASLONG ldb,
	void *c, BLASLONG ldc, int (*function)(), int nthreads);
#endif


static BLASLONG izamax_compute(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
	BLASLONG index = 0;

	if ( n < 0 )  return index;

	__asm__ __volatile__ (
	"	mov	"N", %[N_]				\n"
	"	mov	"X", %[X_]				\n"
	"	mov	"INC_X", %[INCX_]			\n"

	"	cmp	"N", xzr				\n"
	"	ble	10f //izamax_kernel_zero		\n"
	"	cmp	"INC_X", xzr				\n"
	"	ble	10f //izamax_kernel_zero		\n"
	"	cmp	"INC_X", #1				\n"
	"	bne	5f //izamax_kernel_S_BEGIN		\n"
	"	mov	x7, "X"					\n"

	"1: //izamax_kernel_F_BEGIN:				\n"
	"	"INIT"						\n"
	"	subs	"N", "N", #1				\n"
	"	ble	9f //izamax_kernel_L999			\n"
	"	asr	"J", "N", #"N_DIV_SHIFT"		\n"
	"	cmp	"J", xzr				\n"
	"	beq	3f //izamax_kernel_F1			\n"
	"	add	"Z", "Z", #1				\n"

	"2: //izamax_kernel_F:					\n"
	"	"KERNEL_F"					\n"
	"	subs	"J", "J", #1				\n"
	"	bne	2b //izamax_kernel_F			\n"
	"	"KERNEL_F_FINALIZE"				\n"
	"	sub	"Z", "Z", #1				\n"

	"3: //izamax_kernel_F1:					\n"
	"	ands	"J", "N", #"N_REM_MASK"			\n"
	"	ble	9f //izamax_kernel_L999			\n"

	"4: //izamax_kernel_F10:				\n"
	"	"KERNEL_S1"					\n"
	"	subs	"J", "J", #1				\n"
	"	bne	4b //izamax_kernel_F10			\n"
	"	b	9f //izamax_kernel_L999			\n"

	"5: //izamax_kernel_S_BEGIN:				\n"
	"	"INIT"						\n"
	"	subs	"N", "N", #1				\n"
	"	ble	9f //izamax_kernel_L999			\n"
	"	asr	"J", "N", #2				\n"
	"	cmp	"J", xzr				\n"
	"	ble	7f //izamax_kernel_S1			\n"

	"6: //izamax_kernel_S4:					\n"
	"	"KERNEL_S1"					\n"
	"	"KERNEL_S1"					\n"
	"	"KERNEL_S1"					\n"
	"	"KERNEL_S1"					\n"
	"	subs	"J", "J", #1				\n"
	"	bne	6b //izamax_kernel_S4			\n"

	"7: //izamax_kernel_S1:					\n"
	"	ands	"J", "N", #3				\n"
	"	ble	9f //izamax_kernel_L999			\n"

	"8: //izamax_kernel_S10:				\n"
	"	"KERNEL_S1"					\n"
	"	subs	"J", "J", #1				\n"
	"	bne	8b //izamax_kernel_S10			\n"

	"9: //izamax_kernel_L999:				\n"
	"	mov	x0, "INDEX"				\n"
	"	b	11f //izamax_kernel_DONE		\n"

	"10: //izamax_kernel_zero:				\n"
	"	mov	x0, xzr					\n"

	"11: //izamax_kernel_DONE:				\n"
	"	mov	%[INDEX_], "INDEX"			\n"

	: [INDEX_] "=r" (index)		//%0
	: [N_]    "r"  (n),		//%1
	  [X_]    "r"  (x),		//%2
	  [INCX_] "r"  (inc_x)		//%3
	: "cc",
	  "memory",
	  "x0", "x1", "x2", "x3", "x4", "x5", "x6", "x7",
	  "d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7"
	);

	return index;
}

#if defined(SMP)
static int izamax_thread_function(BLASLONG n, BLASLONG dummy0,
	BLASLONG dummy1, FLOAT dummy2, FLOAT *x, BLASLONG inc_x, FLOAT *y,
	BLASLONG inc_y, FLOAT *result, BLASLONG dummy3)
{
	*(BLASLONG *)result = izamax_compute(n, x, inc_x);

	return 0;
}
#endif

BLASLONG CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
#if defined(SMP)
	int nthreads;
	FLOAT dummy_alpha[2];
#endif
	BLASLONG max_index = 0;

#if defined(SMP)
	if (inc_x == 0 || n <= 10000)
		nthreads = 1;
	else
		nthreads = num_cpu_avail(1);

	if (nthreads == 1) {
		max_index = izamax_compute(n, x, inc_x);
	} else {
		BLASLONG i, width, cur_index;
		int num_cpu;
		int mode;
		char result[MAX_CPU_NUMBER * sizeof(double) * 2];
		FLOAT max = -1.0;

#if !defined(DOUBLE)
		mode = BLAS_SINGLE | BLAS_COMPLEX;
#else
		mode = BLAS_DOUBLE | BLAS_COMPLEX;
#endif

		blas_level1_thread_with_return_value(mode, n, 0, 0, &dummy_alpha,
				   x, inc_x, NULL, 0, result, 0,
				   ( void *)izamax_thread_function, nthreads);

		num_cpu = 0;
		i = n;
		cur_index = 0;

		while (i > 0) {
			FLOAT elem_r, elem_i;
			BLASLONG cur_max_index;

			cur_max_index = *(BLASLONG *)&result[num_cpu * sizeof(double) * 2];
			elem_r = x[((cur_index + cur_max_index - 1) * inc_x * 2) + 0];
			elem_i = x[((cur_index + cur_max_index - 1) * inc_x * 2) + 1];
			elem_r = fabs(elem_r) + fabs(elem_i);

			if (elem_r >= max) {
				max = elem_r;
				max_index = cur_index + cur_max_index;
			}

			width = blas_quickdivide(i + nthreads - num_cpu - 1,
				 nthreads - num_cpu);
			i -= width;
			cur_index += width;
			num_cpu ++;
		}
	}
#else
	max_index = izamax_compute(n, x, inc_x);
#endif

	return max_index;
}
