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
#define J		"x3"	/* loop variable */
#define K		"x4"	/* loop variable */

#if !defined(COMPLEX)
#define INC_SHIFT	"3"
#define SZ		"8"
#else
#define INC_SHIFT	"4"
#define SZ		"16"
#endif

#define SSQ		"d0"
#define SCALE		"d1"
#define REGZERO		"d5"
#define REGONE		"d6"
#define CUR_MAX		"d7"
#define CUR_MAXINV	"d8"
#define CUR_MAXINV_V	"v8.2d"
#define CUR_MAX_V	"v8.2d"

static void nrm2_compute(BLASLONG n, FLOAT *x, BLASLONG inc_x,
		         double *ssq, double *scale)
{
	*ssq = 0.0;
	*scale = 0.0;

	if (n <= 0)  return;

	__asm__ __volatile__ (
	"	mov	"N", %[N_]				\n"
	"	mov	"X", %[X_]				\n"
	"	mov	"INC_X", %[INCX_]			\n"
	"	fmov	"SCALE", xzr				\n"
	"	fmov	"SSQ", #1.0				\n"
	"	cmp	"N", xzr				\n"
	"	ble	9f //nrm2_kernel_L999			\n"
	"	cmp	"INC_X", xzr				\n"
	"	ble	9f //nrm2_kernel_L999			\n"

	"1: //nrm2_kernel_F_BEGIN:				\n"
	"	fmov	"REGZERO", xzr				\n"
	"	fmov	"REGONE", #1.0				\n"
	"	lsl	"INC_X", "INC_X", #"INC_SHIFT"		\n"
	"	mov	"J", "N"				\n"
	"	cmp	"J", xzr				\n"
	"	beq	9f //nrm2_kernel_L999			\n"

	"2: //nrm2_kernel_F_ZERO_SKIP:				\n"
	"	ldr	d4, ["X"]				\n"
	"	fcmp	d4, "REGZERO"				\n"
	"	bne	3f //nrm2_kernel_F_INIT			\n"
#if defined(COMPLEX)
	"	ldr	d4, ["X", #8]				\n"
	"	fcmp	d4, "REGZERO"				\n"
	"	bne	4f //nrm2_kernel_F_INIT_I		\n"
#endif
	"	add	"X", "X", "INC_X"			\n"
	"	subs	"J", "J", #1				\n"
	"	beq	9f //nrm2_kernel_L999			\n"
	"	b	2b //nrm2_kernel_F_ZERO_SKIP		\n"

	"3: //nrm2_kernel_F_INIT:				\n"
	"	ldr	d4, ["X"]				\n"
	"	fabs	d4, d4					\n"
	"	fmax	"CUR_MAX", "SCALE", d4			\n"
	"	fdiv	"SCALE", "SCALE", "CUR_MAX"		\n"
	"	fmul	"SCALE", "SCALE", "SCALE"		\n"
	"	fmul	"SSQ", "SSQ", "SCALE"			\n"
	"	fdiv	d4, d4, "CUR_MAX"			\n"
	"	fmul	d4, d4, d4				\n"
	"	fadd	"SSQ", "SSQ", d4			\n"
	"	fmov	"SCALE", "CUR_MAX"			\n"
#if defined(COMPLEX)
	"4: //nrm2_kernel_F_INIT_I:				\n"
	"	ldr	d3, ["X", #8]				\n"
	"	fabs	d3, d3					\n"
	"	fmax	"CUR_MAX", "SCALE", d3			\n"
	"	fdiv	"SCALE", "SCALE", "CUR_MAX"		\n"
	"	fmul	"SCALE", "SCALE", "SCALE"		\n"
	"	fmul	"SSQ", "SSQ", "SCALE"			\n"
	"	fdiv	d3, d3, "CUR_MAX"			\n"
	"	fmul	d3, d3, d3				\n"
	"	fadd	"SSQ", "SSQ", d3			\n"
	"	fmov	"SCALE", "CUR_MAX"			\n"
#endif
	"	add	"X", "X", "INC_X"			\n"
	"	subs	"J", "J", #1				\n"
	"	beq	9f //nrm2_kernel_L999			\n"

	"5: //nrm2_kernel_F_START:				\n"
	"	cmp	"INC_X", #"SZ"				\n"
	"	bne	8f //nrm2_kernel_F1			\n"
	"	asr	"K", "J", #4				\n"
	"	cmp	"K", xzr				\n"
	"	beq	8f //nrm2_kernel_F1			\n"

	"6: //nrm2_kernel_F:					\n"
	"	ldp	q16, q17,  ["X"]			\n"
	"	ldp	q18, q19,  ["X", #32]			\n"
	"	ldp	q20, q21,  ["X", #64]			\n"
	"	ldp	q22, q23,  ["X", #96]			\n"
	"	add	"X", "X", #128				\n"
	"	fabs	v16.2d, v16.2d				\n"
	"	fabs	v17.2d, v17.2d				\n"
	"	fabs	v18.2d, v18.2d				\n"
	"	fabs	v19.2d, v19.2d				\n"
	"	fabs	v20.2d, v20.2d				\n"
	"	fabs	v21.2d, v21.2d				\n"
	"	fabs	v22.2d, v22.2d				\n"
	"	fabs	v23.2d, v23.2d				\n"
	"	fmaxp	v24.2d, v16.2d, v17.2d			\n"
	"	fmaxp	v25.2d, v18.2d, v19.2d			\n"
	"	fmaxp	v26.2d, v20.2d, v21.2d			\n"
	"	fmaxp	v27.2d, v22.2d, v23.2d			\n"
	"	fmaxp	v24.2d, v24.2d, v25.2d			\n"
	"	fmaxp	v26.2d, v26.2d, v27.2d			\n"
	"	fmaxp	v24.2d, v24.2d, v26.2d			\n"
	"	fmaxp	v24.2d, v24.2d, v24.2d			\n"
	"	fmax	"CUR_MAX", "SCALE", d24			\n"
	"	fdiv	"CUR_MAXINV", "REGONE", "CUR_MAX"	\n"
	"	//dup	"CUR_MAX_V", v7.d[0]			\n"
	"	fdiv	"SCALE", "SCALE", "CUR_MAX"		\n"
	"	fmul	"SCALE", "SCALE", "SCALE"		\n"
	"	fmul	"SSQ", "SSQ", "SCALE"			\n"
	"	dup	"CUR_MAXINV_V", v8.d[0]			\n"
	"	fmul	v16.2d, v16.2d, "CUR_MAXINV_V"		\n"
	"	fmul	v17.2d, v17.2d, "CUR_MAXINV_V"		\n"
	"	fmul	v18.2d, v18.2d, "CUR_MAXINV_V"		\n"
	"	fmul	v19.2d, v19.2d, "CUR_MAXINV_V"		\n"
	"	fmul	v20.2d, v20.2d, "CUR_MAXINV_V"		\n"
	"	fmul	v21.2d, v21.2d, "CUR_MAXINV_V"		\n"
	"	fmul	v22.2d, v22.2d, "CUR_MAXINV_V"		\n"
	"	fmul	v23.2d, v23.2d, "CUR_MAXINV_V"		\n"
	"	//fdiv	v16.2d, v16.2d, "CUR_MAX_V"		\n"
	"	//fdiv	v17.2d, v17.2d, "CUR_MAX_V"		\n"
	"	//fdiv	v18.2d, v18.2d, "CUR_MAX_V"		\n"
	"	//fdiv	v19.2d, v19.2d, "CUR_MAX_V"		\n"
	"	//fdiv	v20.2d, v20.2d, "CUR_MAX_V"		\n"
	"	//fdiv	v21.2d, v21.2d, "CUR_MAX_V"		\n"
	"	//fdiv	v22.2d, v22.2d, "CUR_MAX_V"		\n"
	"	//fdiv	v23.2d, v23.2d, "CUR_MAX_V"		\n"
	"	fmul	v24.2d, v16.2d, v16.2d			\n"
	"	fmul	v25.2d, v17.2d, v17.2d			\n"
	"	fmul	v26.2d, v18.2d, v18.2d			\n"
	"	fmul	v27.2d, v19.2d, v19.2d			\n"
	"	fmla	v24.2d, v20.2d, v20.2d			\n"
	"	fmla	v25.2d, v21.2d, v21.2d			\n"
	"	fmla	v26.2d, v22.2d, v22.2d			\n"
	"	fmla	v27.2d, v23.2d, v23.2d			\n"
	"	fadd	v24.2d, v24.2d, v25.2d			\n"
	"	fadd	v26.2d, v26.2d, v27.2d			\n"
	"	fadd	v24.2d, v24.2d, v26.2d			\n"
	"	faddp	d24, v24.2d				\n"
	"	fadd	"SSQ", "SSQ", d24			\n"
	"	fmov	"SCALE", "CUR_MAX"			\n"
#if defined(COMPLEX)
	"	ldp	q16, q17,  ["X"]			\n"
	"	ldp	q18, q19,  ["X", #32]			\n"
	"	ldp	q20, q21,  ["X", #64]			\n"
	"	ldp	q22, q23,  ["X", #96]			\n"
	"	add	"X", "X", #128				\n"
	"	fabs	v16.2d, v16.2d				\n"
	"	fabs	v17.2d, v17.2d				\n"
	"	fabs	v18.2d, v18.2d				\n"
	"	fabs	v19.2d, v19.2d				\n"
	"	fabs	v20.2d, v20.2d				\n"
	"	fabs	v21.2d, v21.2d				\n"
	"	fabs	v22.2d, v22.2d				\n"
	"	fabs	v23.2d, v23.2d				\n"
	"	fmaxp	v24.2d, v16.2d, v17.2d			\n"
	"	fmaxp	v25.2d, v18.2d, v19.2d			\n"
	"	fmaxp	v26.2d, v20.2d, v21.2d			\n"
	"	fmaxp	v27.2d, v22.2d, v23.2d			\n"
	"	fmaxp	v24.2d, v24.2d, v25.2d			\n"
	"	fmaxp	v26.2d, v26.2d, v27.2d			\n"
	"	fmaxp	v24.2d, v24.2d, v26.2d			\n"
	"	fmaxp	v24.2d, v24.2d, v24.2d			\n"
	"	fmax	"CUR_MAX", "SCALE", d24			\n"
	"	fdiv	"CUR_MAXINV", "REGONE", "CUR_MAX"	\n"
	"	//dup	"CUR_MAX_V", v7.d[0]			\n"
	"	fdiv	"SCALE", "SCALE", "CUR_MAX"		\n"
	"	fmul	"SCALE", "SCALE", "SCALE"		\n"
	"	fmul	"SSQ", "SSQ", "SCALE"			\n"
	"	dup	"CUR_MAXINV_V", v8.d[0]			\n"
	"	fmul	v16.2d, v16.2d, "CUR_MAXINV_V"		\n"
	"	fmul	v17.2d, v17.2d, "CUR_MAXINV_V"		\n"
	"	fmul	v18.2d, v18.2d, "CUR_MAXINV_V"		\n"
	"	fmul	v19.2d, v19.2d, "CUR_MAXINV_V"		\n"
	"	fmul	v20.2d, v20.2d, "CUR_MAXINV_V"		\n"
	"	fmul	v21.2d, v21.2d, "CUR_MAXINV_V"		\n"
	"	fmul	v22.2d, v22.2d, "CUR_MAXINV_V"		\n"
	"	fmul	v23.2d, v23.2d, "CUR_MAXINV_V"		\n"
	"	//fdiv	v16.2d, v16.2d, "CUR_MAX_V"		\n"
	"	//fdiv	v17.2d, v17.2d, "CUR_MAX_V"		\n"
	"	//fdiv	v18.2d, v18.2d, "CUR_MAX_V"		\n"
	"	//fdiv	v19.2d, v19.2d, "CUR_MAX_V"		\n"
	"	//fdiv	v20.2d, v20.2d, "CUR_MAX_V"		\n"
	"	//fdiv	v21.2d, v21.2d, "CUR_MAX_V"		\n"
	"	//fdiv	v22.2d, v22.2d, "CUR_MAX_V"		\n"
	"	//fdiv	v23.2d, v23.2d, "CUR_MAX_V"		\n"
	"	fmul	v24.2d, v16.2d, v16.2d			\n"
	"	fmul	v25.2d, v17.2d, v17.2d			\n"
	"	fmul	v26.2d, v18.2d, v18.2d			\n"
	"	fmul	v27.2d, v19.2d, v19.2d			\n"
	"	fmla	v24.2d, v20.2d, v20.2d			\n"
	"	fmla	v25.2d, v21.2d, v21.2d			\n"
	"	fmla	v26.2d, v22.2d, v22.2d			\n"
	"	fmla	v27.2d, v23.2d, v23.2d			\n"
	"	fadd	v24.2d, v24.2d, v25.2d			\n"
	"	fadd	v26.2d, v26.2d, v27.2d			\n"
	"	fadd	v24.2d, v24.2d, v26.2d			\n"
	"	faddp	d24, v24.2d				\n"
	"	fadd	"SSQ", "SSQ", d24			\n"
	"	fmov	"SCALE", "CUR_MAX"			\n"
#endif
	"	subs	"K", "K", #1				\n"
	"	bne	6b //nrm2_kernel_F			\n"

	"7: //nrm2_kernel_F_DONE:				\n"
	"	ands	"J", "J", #15				\n"
	"	beq	9f //nrm2_kernel_L999			\n"

	"8: //nrm2_kernel_F1:					\n"
	"	ldr	d4, ["X"]				\n"
	"	fabs	d4, d4					\n"
	"	fmax	"CUR_MAX", "SCALE", d4			\n"
	"	fdiv	"SCALE", "SCALE", "CUR_MAX"		\n"
	"	fmul	"SCALE", "SCALE", "SCALE"		\n"
	"	fmul	"SSQ", "SSQ", "SCALE"			\n"
	"	fdiv	d4, d4, "CUR_MAX"			\n"
	"	fmul	d4, d4, d4				\n"
	"	fadd	"SSQ", "SSQ", d4			\n"
	"	fmov	"SCALE", "CUR_MAX"			\n"
#if defined(COMPLEX)
	"	ldr	d3, ["X", #8]				\n"
	"	fabs	d3, d3					\n"
	"	fmax	"CUR_MAX", "SCALE", d3			\n"
	"	fdiv	"SCALE", "SCALE", "CUR_MAX"		\n"
	"	fmul	"SCALE", "SCALE", "SCALE"		\n"
	"	fmul	"SSQ", "SSQ", "SCALE"			\n"
	"	fdiv	d3, d3, "CUR_MAX"			\n"
	"	fmul	d3, d3, d3				\n"
	"	fadd	"SSQ", "SSQ", d3			\n"
	"	fmov	"SCALE", "CUR_MAX"			\n"
#endif
	"	add	"X", "X", "INC_X"			\n"
	"	subs	"J", "J", #1				\n"
	"	bne	8b //nrm2_kernel_F1			\n"

	"9: //nrm2_kernel_L999:					\n"
	"	str	"SSQ", [%[SSQ_]]			\n"
	"	str	"SCALE", [%[SCALE_]]			\n"

	:
	: [SSQ_]    "r"  (ssq),			//%0
	  [SCALE_]  "r"  (scale),		//%1
	  [N_]      "r"  (n),			//%2
	  [X_]      "r"  (x),			//%3
	  [INCX_]   "r"  (inc_x)		//%4
	: "cc",
	  "memory",
	  "x0", "x1", "x2", "x3", "x4", "x5",
	  "d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8"
	);

}

#if defined(SMP)
static int nrm2_thread_function(BLASLONG n, BLASLONG dummy0,
	BLASLONG dummy1, FLOAT dummy2, FLOAT *x, BLASLONG inc_x, FLOAT *dummy3,
	BLASLONG dummy4, FLOAT *result, BLASLONG dummy5)
{
	nrm2_compute(n, x, inc_x, result, result + 1);

	return 0;
}
#endif

FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
#if defined(SMP)
	int nthreads;
	FLOAT dummy_alpha[2];
#endif
	FLOAT ssq, scale;

	if (n <= 0 || inc_x <= 0) return 0.0;

#if defined(SMP)
	if (n <= 10000)
		nthreads = 1;
	else
		nthreads = num_cpu_avail(1);

	if (nthreads == 1) {
		nrm2_compute(n, x, inc_x, &ssq, &scale);
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

		scale = 0.0;
		ssq = 1.0;
		ptr = (double *)result;
		for (i = 0; i < nthreads; i++) {
			FLOAT cur_scale, cur_ssq;

			cur_ssq = *ptr;
			cur_scale = *(ptr + 1);

			if (cur_scale != 0) {
				if (cur_scale > scale) {
					scale = (scale / cur_scale);
					ssq = ssq * scale * scale;
					ssq += cur_ssq;
					scale = cur_scale;
				} else {
					cur_scale = (cur_scale / scale);
					cur_ssq = cur_ssq * cur_scale * cur_scale;
					ssq += cur_ssq;
				}
			}

			ptr = (double *)(((char *)ptr) + sizeof(double) * 2);
		}
	}
#else
	nrm2_compute(n, x, inc_x, &ssq, &scale);
#endif
	ssq = sqrt(ssq) * scale;

	return ssq;
}
