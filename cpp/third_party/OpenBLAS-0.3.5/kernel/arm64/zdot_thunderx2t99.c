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

#define N		"x0"	/* vector length */
#define X		"x1"	/* "X" vector address */
#define INC_X		"x2"	/* "X" stride */
#define Y		"x3"	/* "Y" vector address */
#define INC_Y		"x4"	/* "Y" stride */
#define J		"x5"	/* loop variable */

#if !defined(DOUBLE)
#define REG0		"wzr"
#define DOTF		"s0"
#define DOTI		"s1"
#define INC_SHIFT	"3"
#define N_DIV_SHIFT	"4"
#define N_REM_MASK	"15"
#else
#define REG0		"xzr"
#define DOTF		"d0"
#define DOTI		"d1"
#define INC_SHIFT	"4"
#define N_DIV_SHIFT	"3"
#define N_REM_MASK	"7"
#endif

#if !defined(CONJ)
#define f_ii		"fmls"
#define f_ir		"fmla"
#define a_ii		"fsub"
#define a_ir		"fadd"
#else
#define f_ii		"fmla"
#define f_ir		"fmls"
#define a_ii		"fadd"
#define a_ir		"fsub"
#endif

#if !defined(DOUBLE)
#define KERNEL_F1						\
	"	ldr	d16, ["X"]			\n"	\
	"	ldr	d24, ["Y"]			\n"	\
	"	add	"X", "X", "INC_X"		\n"	\
	"	add	"Y", "Y", "INC_Y"		\n"	\
	"	ins	v17.s[0], v16.s[1]		\n"	\
	"	fmla	"DOTF", s16, v24.s[0]		\n"	\
	"	"f_ii"	"DOTF", s17, v24.s[1]		\n"	\
	"	"f_ir"	"DOTI", s17, v24.s[0]		\n"	\
	"	fmla	"DOTI", s16, v24.s[1]		\n"

#define KERNEL_F						\
	"	ld2	{v16.4s, v17.4s}, ["X"]		\n"	\
	"	ld2	{v24.4s, v25.4s}, ["Y"]		\n"	\
	"	add	"X", "X", #32			\n"	\
	"	add	"Y", "Y", #32			\n"	\
	"	ld2	{v18.4s, v19.4s}, ["X"]		\n"	\
	"	ld2	{v26.4s, v27.4s}, ["Y"]		\n"	\
	"	add	"X", "X", #32			\n"	\
	"	add	"Y", "Y", #32			\n"	\
	"	fmla	v0.4s, v16.4s, v24.4s		\n"	\
	"	fmla	v1.4s, v17.4s, v25.4s		\n"	\
	"	fmla	v2.4s, v16.4s, v25.4s		\n"	\
	"	fmla	v3.4s, v17.4s, v24.4s		\n"	\
	"	ld2	{v20.4s, v21.4s}, ["X"]		\n"	\
	"	ld2	{v28.4s, v29.4s}, ["Y"]		\n"	\
	"	add	"X", "X", #32			\n"	\
	"	add	"Y", "Y", #32			\n"	\
	"	fmla	v4.4s, v18.4s, v26.4s		\n"	\
	"	fmla	v5.4s, v19.4s, v27.4s		\n"	\
	"	fmla	v6.4s, v18.4s, v27.4s		\n"	\
	"	fmla	v7.4s, v19.4s, v26.4s		\n"	\
	"	ld2	{v22.4s, v23.4s}, ["X"]		\n"	\
	"	ld2	{v30.4s, v31.4s}, ["Y"]		\n"	\
	"	fmla	v0.4s, v20.4s, v28.4s		\n"	\
	"	fmla	v1.4s, v21.4s, v29.4s		\n"	\
	"	fmla	v2.4s, v20.4s, v29.4s		\n"	\
	"	fmla	v3.4s, v21.4s, v28.4s		\n"	\
	"	add	"X", "X", #32			\n"	\
	"	add	"Y", "Y", #32			\n"	\
	"	PRFM	PLDL1KEEP, ["X", #1024]		\n"	\
	"	PRFM	PLDL1KEEP, ["Y", #1024]		\n"	\
	"	PRFM	PLDL1KEEP, ["X", #1024+64]	\n"	\
	"	PRFM	PLDL1KEEP, ["Y", #1024+64]	\n"	\
	"	fmla	v4.4s, v22.4s, v30.4s		\n"	\
	"	fmla	v5.4s, v23.4s, v31.4s		\n"	\
	"	fmla	v6.4s, v22.4s, v31.4s		\n"	\
	"	fmla	v7.4s, v23.4s, v30.4s		\n"

#define KERNEL_F_FINALIZE					\
	"	fadd	v0.4s, v0.4s, v4.4s		\n"	\
	"	fadd	v1.4s, v1.4s, v5.4s		\n"	\
	"	fadd	v2.4s, v2.4s, v6.4s		\n"	\
	"	fadd	v3.4s, v3.4s, v7.4s		\n"	\
	"	"a_ii"	v0.4s, v0.4s, v1.4s		\n"	\
	"	"a_ir"	v1.4s, v2.4s, v3.4s		\n"	\
	"	faddp	v0.4s, v0.4s, v0.4s		\n"	\
	"	faddp	v0.4s, v0.4s, v0.4s		\n"	\
	"	faddp	v1.4s, v1.4s, v1.4s		\n"	\
	"	faddp	v1.4s, v1.4s, v1.4s		\n"

#else

#define KERNEL_F1						\
	"	ldr	q16, ["X"]			\n"	\
	"	ldr	q24, ["Y"]			\n"	\
	"	add	"X", "X", "INC_X"		\n"	\
	"	add	"Y", "Y", "INC_Y"		\n"	\
	"	ins	v17.d[0], v16.d[1]		\n"	\
	"	fmla	"DOTF", d16, v24.d[0]		\n"	\
	"	"f_ii"	"DOTF", d17, v24.d[1]		\n"	\
	"	"f_ir"	"DOTI", d17, v24.d[0]		\n"	\
	"	fmla	"DOTI", d16, v24.d[1]		\n"

#define KERNEL_F						\
	"	ld2	{v16.2d, v17.2d}, ["X"]		\n"	\
	"	ld2	{v24.2d, v25.2d}, ["Y"]		\n"	\
	"	add	"X", "X", #32			\n"	\
	"	add	"Y", "Y", #32			\n"	\
	"	ld2	{v18.2d, v19.2d}, ["X"]		\n"	\
	"	ld2	{v26.2d, v27.2d}, ["Y"]		\n"	\
	"	add	"X", "X", #32			\n"	\
	"	add	"Y", "Y", #32			\n"	\
	"	fmla	v0.2d, v16.2d, v24.2d		\n"	\
	"	fmla	v1.2d, v17.2d, v25.2d		\n"	\
	"	fmla	v2.2d, v16.2d, v25.2d		\n"	\
	"	fmla	v3.2d, v17.2d, v24.2d		\n"	\
	"	ld2	{v20.2d, v21.2d}, ["X"]		\n"	\
	"	ld2	{v28.2d, v29.2d}, ["Y"]		\n"	\
	"	add	"X", "X", #32			\n"	\
	"	add	"Y", "Y", #32			\n"	\
	"	fmla	v4.2d, v18.2d, v26.2d		\n"	\
	"	fmla	v5.2d, v19.2d, v27.2d		\n"	\
	"	fmla	v6.2d, v18.2d, v27.2d		\n"	\
	"	fmla	v7.2d, v19.2d, v26.2d		\n"	\
	"	ld2	{v22.2d, v23.2d}, ["X"]		\n"	\
	"	ld2	{v30.2d, v31.2d}, ["Y"]		\n"	\
	"	fmla	v0.2d, v20.2d, v28.2d		\n"	\
	"	fmla	v1.2d, v21.2d, v29.2d		\n"	\
	"	fmla	v2.2d, v20.2d, v29.2d		\n"	\
	"	fmla	v3.2d, v21.2d, v28.2d		\n"	\
	"	add	"X", "X", #32			\n"	\
	"	add	"Y", "Y", #32			\n"	\
	"	PRFM	PLDL1KEEP, ["X", #1024]		\n"	\
	"	PRFM	PLDL1KEEP, ["Y", #1024]		\n"	\
	"	PRFM	PLDL1KEEP, ["X", #1024+64]	\n"	\
	"	PRFM	PLDL1KEEP, ["Y", #1024+64]	\n"	\
	"	fmla	v4.2d, v22.2d, v30.2d		\n"	\
	"	fmla	v5.2d, v23.2d, v31.2d		\n"	\
	"	fmla	v6.2d, v22.2d, v31.2d		\n"	\
	"	fmla	v7.2d, v23.2d, v30.2d		\n"

#define KERNEL_F_FINALIZE					\
	"	fadd	v0.2d, v0.2d, v4.2d		\n"	\
	"	fadd	v1.2d, v1.2d, v5.2d		\n"	\
	"	fadd	v2.2d, v2.2d, v6.2d		\n"	\
	"	fadd	v3.2d, v3.2d, v7.2d		\n"	\
	"	"a_ii"	v0.2d, v0.2d, v1.2d		\n"	\
	"	"a_ir"	v1.2d, v2.2d, v3.2d		\n"	\
	"	faddp	"DOTF", v0.2d			\n"	\
	"	faddp	"DOTI", v1.2d			\n"
#endif

#if defined(SMP)
extern int blas_level1_thread_with_return_value(int mode, BLASLONG m, BLASLONG n,
	BLASLONG k, void *alpha, void *a, BLASLONG lda, void *b, BLASLONG ldb,
	void *c, BLASLONG ldc, int (*function)(), int nthreads);
#endif

static void zdot_compute(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, OPENBLAS_COMPLEX_FLOAT *result)
{
	FLOAT dotr = 0.0, doti = 0.0;
	CREAL(*result) = 0.0;
	CIMAG(*result) = 0.0;

	if ( n < 0 ) return;

	__asm__ __volatile__ (
	"	mov	"N", %[N_]			\n"
	"	mov	"X", %[X_]			\n"
	"	mov	"INC_X", %[INCX_]		\n"
	"	mov	"Y", %[Y_]			\n"
	"	mov	"INC_Y", %[INCY_]		\n"
	"	fmov	"DOTF", "REG0"			\n"
	"	fmov	"DOTI", "REG0"			\n"
	"	fmov	d2, xzr				\n"
	"	fmov	d3, xzr				\n"
	"	fmov	d4, xzr				\n"
	"	fmov	d5, xzr				\n"
	"	fmov	d6, xzr				\n"
	"	fmov	d7, xzr				\n"
	"	cmp	"N", xzr			\n"
	"	ble	9f //dot_kernel_L999		\n"
	"	cmp	"INC_X", #1			\n"
	"	bne	5f //dot_kernel_S_BEGIN		\n"
	"	cmp	"INC_Y", #1			\n"
	"	bne	5f //dot_kernel_S_BEGIN		\n"

	"1: //dot_kernel_F_BEGIN:			\n"
	"	lsl	"INC_X", "INC_X", "INC_SHIFT"	\n"
	"	lsl	"INC_Y", "INC_Y", "INC_SHIFT"	\n"
	"	asr	"J", "N", #"N_DIV_SHIFT"	\n"
	"	cmp	"J", xzr			\n"
	"	beq	3f //dot_kernel_F1		\n"

	"	.align 5				\n"
	"2: //dot_kernel_F:				\n"
	"	"KERNEL_F"				\n"
	"	subs	"J", "J", #1			\n"
	"	bne	2b //dot_kernel_F		\n"
	"	"KERNEL_F_FINALIZE"			\n"

	"3: //dot_kernel_F1:				\n"
	"	ands	"J", "N", #"N_REM_MASK"		\n"
	"	ble	9f //dot_kernel_L999		\n"

	"4: //dot_kernel_F10:				\n"
	"	"KERNEL_F1"				\n"
	"	subs	"J", "J", #1			\n"
	"	bne	4b //dot_kernel_F10		\n"
	"	b	9f //dot_kernel_L999		\n"

	"5: //dot_kernel_S_BEGIN:			\n"
	"	lsl	"INC_X", "INC_X", "INC_SHIFT"	\n"
	"	lsl	"INC_Y", "INC_Y", "INC_SHIFT"	\n"
	"	asr	"J", "N", #2			\n"
	"	cmp	"J", xzr			\n"
	"	ble	7f //dot_kernel_S1		\n"

	"6: //dot_kernel_S4:				\n"
	"	"KERNEL_F1"				\n"
	"	"KERNEL_F1"				\n"
	"	"KERNEL_F1"				\n"
	"	"KERNEL_F1"				\n"
	"	subs	"J", "J", #1			\n"
	"	bne	6b //dot_kernel_S4		\n"

	"7: //dot_kernel_S1:				\n"
	"	ands	"J", "N", #3			\n"
	"	ble	9f //dot_kernel_L999		\n"

	"8: //dot_kernel_S10:				\n"
	"	"KERNEL_F1"				\n"
	"	subs	"J", "J", #1			\n"
	"	bne	8b //dot_kernel_S10		\n"

	"9: //dot_kernel_L999:				\n"
	"	str	"DOTF", [%[DOTR_]]		\n"
	"	str	"DOTI", [%[DOTI_]]		\n"

	:
	: [DOTR_]  "r"  (&dotr),	//%0
	  [DOTI_]  "r"  (&doti),	//%1
	  [N_]     "r"  (n),		//%2
	  [X_]     "r"  (x),		//%3
	  [INCX_]  "r"  (inc_x),	//%4
	  [Y_]     "r"  (y),		//%5
	  [INCY_]  "r"  (inc_y)		//%6
	: "cc",
	  "memory",
	  "x0", "x1", "x2", "x3", "x4", "x5",
	  "d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7"
	);

	CREAL(*result) = dotr;
	CIMAG(*result) = doti;
	return;
}

#if defined(SMP)
static int zdot_thread_function(BLASLONG n, BLASLONG dummy0,
	BLASLONG dummy1, FLOAT dummy2, FLOAT *x, BLASLONG inc_x, FLOAT *y,
	BLASLONG inc_y, FLOAT *result, BLASLONG dummy3)
{
	zdot_compute(n, x, inc_x, y, inc_y, (void *)result);

	return 0;
}
#endif

OPENBLAS_COMPLEX_FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
{
#if defined(SMP)
	int nthreads;
	FLOAT dummy_alpha;
#endif
	OPENBLAS_COMPLEX_FLOAT zdot;
       CREAL(zdot) = 0.0;
       CIMAG(zdot) = 0.0;

#if defined(SMP)
	if (inc_x == 0 || inc_y == 0 || n <= 10000)
		nthreads = 1;
	else
		nthreads = num_cpu_avail(1);

	if (nthreads == 1) {
		zdot_compute(n, x, inc_x, y, inc_y, &zdot);
	} else {
		int mode, i;
		char result[MAX_CPU_NUMBER * sizeof(double) * 2];
		OPENBLAS_COMPLEX_FLOAT *ptr;

#if !defined(DOUBLE)
		mode = BLAS_SINGLE  | BLAS_COMPLEX;
#else
		mode = BLAS_DOUBLE  | BLAS_COMPLEX;
#endif

		blas_level1_thread_with_return_value(mode, n, 0, 0, &dummy_alpha,
				   x, inc_x, y, inc_y, result, 0,
				   ( void *)zdot_thread_function, nthreads);

		ptr = (OPENBLAS_COMPLEX_FLOAT *)result;
		for (i = 0; i < nthreads; i++) {
			CREAL(zdot) = CREAL(zdot) + CREAL(*ptr);
			CIMAG(zdot) = CIMAG(zdot) + CIMAG(*ptr);
			ptr = (void *)(((char *)ptr) + sizeof(double) * 2);
		}
	}
#else
	zdot_compute(n, x, inc_x, y, inc_y, &zdot);
#endif

	return zdot;
}
