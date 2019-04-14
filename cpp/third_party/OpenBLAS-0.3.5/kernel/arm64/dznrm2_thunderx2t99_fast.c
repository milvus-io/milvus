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

#if defined(SMP)
extern int blas_level1_thread_with_return_value(int mode, BLASLONG m, BLASLONG n,
	BLASLONG k, void *alpha, void *a, BLASLONG lda, void *b, BLASLONG ldb,
	void *c, BLASLONG ldc, int (*function)(), int nthreads);
#endif

#define N		"x0"	/* vector length */
#define X		"x1"	/* X vector address */
#define INC_X		"x2"	/* X stride */
#define J		"x5"	/* loop variable */

#define TMPF		"d16"
#define SSQ		"d0"

#if !defined(COMPLEX)
#define N_DIV_SHIFT	"5"
#define N_REM_MASK	"31"
#define INC_SHIFT	"3"
#else
#define N_DIV_SHIFT	"4"
#define N_REM_MASK	"15"
#define INC_SHIFT	"4"
#endif


#define KERNEL_F					\
	"ldp	q16, q17, ["X"]			\n"	\
	"ldp	q18, q19, ["X", #32]		\n"	\
	"ldp	q20, q21, ["X", #64]		\n"	\
	"ldp	q22, q23, ["X", #96]		\n"	\
	"ldp	q24, q25, ["X", #128]		\n"	\
	"ldp	q26, q27, ["X", #160]		\n"	\
	"ldp	q28, q29, ["X", #192]		\n"	\
	"ldp	q30, q31, ["X", #224]		\n"	\
	"add	"X", "X", #256			\n"	\
	"fmla	v0.2d, v16.2d, v16.2d		\n"	\
	"fmla	v1.2d, v17.2d, v17.2d		\n"	\
	"fmla	v2.2d, v18.2d, v18.2d		\n"	\
	"fmla	v3.2d, v19.2d, v19.2d		\n"	\
	"prfm	PLDL1KEEP, ["X", #1024]		\n"	\
	"prfm	PLDL1KEEP, ["X", #1024+64]	\n"	\
	"fmla	v4.2d, v20.2d, v20.2d		\n"	\
	"fmla	v5.2d, v21.2d, v21.2d		\n"	\
	"fmla	v6.2d, v22.2d, v22.2d		\n"	\
	"fmla	v7.2d, v23.2d, v23.2d		\n"	\
	"prfm	PLDL1KEEP, ["X", #1024+128]	\n"	\
	"prfm	PLDL1KEEP, ["X", #1024+192]	\n"	\
	"fmla	v0.2d, v24.2d, v24.2d		\n"	\
	"fmla	v1.2d, v25.2d, v25.2d		\n"	\
	"fmla	v2.2d, v26.2d, v26.2d		\n"	\
	"fmla	v3.2d, v27.2d, v27.2d		\n"	\
	"fmla	v4.2d, v28.2d, v28.2d		\n"	\
	"fmla	v5.2d, v29.2d, v29.2d		\n"	\
	"fmla	v6.2d, v30.2d, v30.2d		\n"	\
	"fmla	v7.2d, v31.2d, v31.2d		\n"


#if !defined(COMPLEX)
#define KERNEL_F1					\
	"ldr	"TMPF", ["X"]			\n"	\
	"add	"X", "X", "INC_X"		\n"	\
	"fmadd	"SSQ", "TMPF", "TMPF", "SSQ"	\n"

#define KERNEL_F_FINALIZE				\
	"fadd	v0.2d, v0.2d, v1.2d		\n"	\
	"fadd	v2.2d, v2.2d, v3.2d		\n"	\
	"fadd	v4.2d, v4.2d, v5.2d		\n"	\
	"fadd	v6.2d, v6.2d, v7.2d		\n"	\
	"fadd	v0.2d, v0.2d, v2.2d		\n"	\
	"fadd	v4.2d, v4.2d, v6.2d		\n"	\
	"fadd	v0.2d, v0.2d, v4.2d		\n"	\
	"faddp	"SSQ", v0.2d			\n"

#define KERNEL_FINALIZE					\
	""
#else
#define KERNEL_F1					\
	"ldr	q16, ["X"]			\n"	\
	"add	"X", "X", "INC_X"		\n"	\
	"fmla	v0.2d, v16.2d, v16.2d		\n"

#define KERNEL_F_FINALIZE				\
	"fadd	v0.2d, v0.2d, v1.2d		\n"	\
	"fadd	v2.2d, v2.2d, v3.2d		\n"	\
	"fadd	v4.2d, v4.2d, v5.2d		\n"	\
	"fadd	v6.2d, v6.2d, v7.2d		\n"	\
	"fadd	v0.2d, v0.2d, v2.2d		\n"	\
	"fadd	v4.2d, v4.2d, v6.2d		\n"	\
	"fadd	v0.2d, v0.2d, v4.2d		\n"

#define KERNEL_FINALIZE					\
	"faddp	"SSQ", v0.2d			\n"
#endif

static double nrm2_compute(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
	double  ret = 0.0 ;

	if (n <= 0)  return ret;

	__asm__ __volatile__ (
	"	mov	"N", %[N_]			\n"
	"	mov	"X", %[X_]			\n"
	"	mov	"INC_X", %[INCX_]		\n"
	"	fmov	"SSQ", xzr			\n"
	"	fmov	d1, xzr				\n"
	"	fmov	d2, xzr				\n"
	"	fmov	d3, xzr				\n"
	"	fmov	d4, xzr				\n"
	"	fmov	d5, xzr				\n"
	"	fmov	d6, xzr				\n"
	"	fmov	d7, xzr				\n"
	"	cmp	"N", xzr			\n"
	"	ble	.Lnrm2_kernel_L999		\n"
	"	cmp	"INC_X", xzr			\n"
	"	ble	.Lnrm2_kernel_L999		\n"
	"	cmp	"INC_X", #1			\n"
	"	bne	.Lnrm2_kernel_S_BEGIN		\n"

	".Lnrm2_kernel_F_BEGIN:				\n"
	"	lsl	"INC_X", "INC_X", #"INC_SHIFT"	\n"
	"	asr	"J", "N", #"N_DIV_SHIFT"	\n"
	"	cmp	"J", xzr			\n"
	"	beq	.Lnrm2_kernel_F1		\n"

	"	.align 5				\n"
	".Lnrm2_kernel_F:				\n"
	"	"KERNEL_F"				\n"
	"	subs	"J", "J", #1			\n"
	"	bne	.Lnrm2_kernel_F			\n"
	"	"KERNEL_F_FINALIZE"			\n"

	".Lnrm2_kernel_F1:				\n"
	"	ands	"J", "N", #"N_REM_MASK"		\n"
	"	ble	.Lnrm2_kernel_L999		\n"

	".Lnrm2_kernel_F10:				\n"
	"	"KERNEL_F1"				\n"
	"	subs	"J", "J", #1			\n"
	"	bne	.Lnrm2_kernel_F10		\n"
	"	b	.Lnrm2_kernel_L999		\n"

	".Lnrm2_kernel_S_BEGIN:				\n"
	"	lsl	"INC_X", "INC_X", #"INC_SHIFT"	\n"
	"	asr	"J", "N", #2			\n"
	"	cmp	"J", xzr			\n"
	"	ble	.Lnrm2_kernel_S1		\n"

	".Lnrm2_kernel_S4:				\n"
	"	"KERNEL_F1"				\n"
	"	"KERNEL_F1"				\n"
	"	"KERNEL_F1"				\n"
	"	"KERNEL_F1"				\n"
	"	subs	"J", "J", #1			\n"
	"	bne	.Lnrm2_kernel_S4		\n"

	".Lnrm2_kernel_S1:				\n"
	"	ands	"J", "N", #3			\n"
	"	ble	.Lnrm2_kernel_L999		\n"

	".Lnrm2_kernel_S10:				\n"
	"	"KERNEL_F1"				\n"
	"	subs	"J", "J", #1			\n"
	"	bne	.Lnrm2_kernel_S10		\n"

	".Lnrm2_kernel_L999:				\n"
	"	"KERNEL_FINALIZE"			\n"
	"	str	"SSQ", [%[RET_]]		\n"

	:
	: [RET_]  "r"  (&ret),		//%0
	  [N_]    "r"  (n),		//%1
	  [X_]    "r"  (x),		//%2
	  [INCX_] "r"  (inc_x)		//%3
	: "cc",
	  "memory",
	  "x0", "x1", "x2", "x3", "x4", "x5",
	  "d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7"
	);

	return ret;
}

#if defined(SMP)
static int nrm2_thread_function(BLASLONG n, BLASLONG dummy0,
	BLASLONG dummy1, FLOAT dummy2, FLOAT *x, BLASLONG inc_x, FLOAT *dummy3,
	BLASLONG dummy4, FLOAT *result, BLASLONG dummy5)
{
	*(double *)result = nrm2_compute(n, x, inc_x);

	return 0;
}
#endif

FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
#if defined(SMP)
	int nthreads;
	FLOAT dummy_alpha[2];
#endif
	FLOAT nrm2 = 0.0;

	if (n <= 0 || inc_x <= 0) return 0.0;

#if defined(SMP)
	if (n <= 10000)
		nthreads = 1;
	else
		nthreads = num_cpu_avail(1);

	if (nthreads == 1) {
		nrm2 = nrm2_compute(n, x, inc_x);
	} else {
		int mode, i;
		char result[MAX_CPU_NUMBER * sizeof(double) * 2];
		double *ptr;

#if !defined(COMPLEX)
		mode = BLAS_DOUBLE  | BLAS_REAL;
#else
		mode = BLAS_DOUBLE  | BLAS_COMPLEX;
#endif

		blas_level1_thread_with_return_value(mode, n, 0, 0, &dummy_alpha,
				   x, inc_x, NULL, 0, result, 0,
				   ( void *)nrm2_thread_function, nthreads);

		ptr = (double *)result;
		for (i = 0; i < nthreads; i++) {
			nrm2 = nrm2 + (*ptr);
			ptr = (double *)(((char *)ptr) + sizeof(double) * 2);
		}
	}
#else
	nrm2 = nrm2_compute(n, x, inc_x);
#endif
	nrm2 = sqrt(nrm2);

	return nrm2;
}
