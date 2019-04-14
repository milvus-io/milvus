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
#define	X	"x1"	/* "X" vector address */
#define	INC_X	"x2"	/* "X" stride */
#define J	"x5"	/* loop variable */

#define REG0	"xzr"
#define SUMF	"d0"
#define TMPF	"d1"

/******************************************************************************/

#define KERNEL_F1					\
	"ldr	q1, ["X"]			\n"	\
	"add	"X", "X", #16			\n"	\
	"fabs	v1.2d, v1.2d			\n"	\
	"faddp	d1, v1.2d			\n"	\
	"fadd	"SUMF", "SUMF", d1		\n"

#define KERNEL_F16					\
	"ldr	q16, ["X"]			\n"	\
	"ldr	q17, ["X", #16]			\n"	\
	"ldr	q18, ["X", #32]			\n"	\
	"ldr	q19, ["X", #48]			\n"	\
	"ldp	q20, q21, ["X", #64]		\n"	\
	"ldp	q22, q23, ["X", #96]		\n"	\
	"fabs	v16.2d, v16.2d			\n"	\
	"fabs	v17.2d, v17.2d			\n"	\
	"fabs	v18.2d, v18.2d			\n"	\
	"fabs	v19.2d, v19.2d			\n"	\
	"ldp	q24, q25, ["X", #128]		\n"	\
	"ldp	q26, q27, ["X", #160]		\n"	\
	"fabs	v20.2d, v20.2d			\n"	\
	"fabs	v21.2d, v21.2d			\n"	\
	"fabs	v22.2d, v22.2d			\n"	\
	"fabs	v23.2d, v23.2d			\n"	\
	"fadd	v16.2d, v16.2d, v17.2d		\n"	\
	"fadd	v18.2d, v18.2d, v19.2d		\n"	\
	"ldp	q28, q29, ["X", #192]		\n"	\
	"ldp	q30, q31, ["X", #224]		\n"	\
	"fabs	v24.2d, v24.2d			\n"	\
	"fabs	v25.2d, v25.2d			\n"	\
	"fabs	v26.2d, v26.2d			\n"	\
	"fabs	v27.2d, v27.2d			\n"	\
	"add	"X", "X", #256			\n"	\
	"fadd	v20.2d, v20.2d, v21.2d		\n"	\
	"fadd	v22.2d, v22.2d, v23.2d		\n"	\
	"fabs	v28.2d, v28.2d			\n"	\
	"fabs	v29.2d, v29.2d			\n"	\
	"fabs	v30.2d, v30.2d			\n"	\
	"fabs	v31.2d, v31.2d			\n"	\
	"PRFM	PLDL1KEEP, ["X", #1024]		\n"	\
	"PRFM	PLDL1KEEP, ["X", #1024+64]	\n"	\
	"fadd	v24.2d, v24.2d, v25.2d		\n"	\
	"fadd	v26.2d, v26.2d, v27.2d		\n"	\
	"fadd	v28.2d, v28.2d, v29.2d		\n"	\
	"fadd	v30.2d, v30.2d, v31.2d		\n"	\
	"fadd	v0.2d, v0.2d, v16.2d		\n"	\
	"fadd	v1.2d, v1.2d, v18.2d		\n"	\
	"fadd	v2.2d, v2.2d, v20.2d		\n"	\
	"fadd	v3.2d, v3.2d, v22.2d		\n"	\
	"PRFM	PLDL1KEEP, ["X", #1024+128]	\n"	\
	"PRFM	PLDL1KEEP, ["X", #1024+192]	\n"	\
	"fadd	v4.2d, v4.2d, v24.2d		\n"	\
	"fadd	v5.2d, v5.2d, v26.2d		\n"	\
	"fadd	v6.2d, v6.2d, v28.2d		\n"	\
	"fadd	v7.2d, v7.2d, v30.2d		\n"

#define KERNEL_F16_FINALIZE				\
	"fadd	v0.2d, v0.2d, v1.2d		\n"	\
	"fadd	v2.2d, v2.2d, v3.2d		\n"	\
	"fadd	v4.2d, v4.2d, v5.2d		\n"	\
	"fadd	v6.2d, v6.2d, v7.2d		\n"	\
	"fadd	v0.2d, v0.2d, v2.2d		\n"	\
	"fadd	v4.2d, v4.2d, v6.2d		\n"	\
	"fadd	v0.2d, v0.2d, v4.2d		\n"	\
	"faddp	"SUMF", v0.2d			\n"

#define INIT_S						\
	"lsl	"INC_X", "INC_X", #4		\n"

#define KERNEL_S1					\
	"ldr	q1, ["X"]			\n"	\
	"add	"X", "X", "INC_X"		\n"	\
	"fabs	v1.2d, v1.2d			\n"	\
	"faddp	d1, v1.2d			\n"	\
	"fadd	"SUMF", "SUMF", d1		\n"


#if defined(SMP)
extern int blas_level1_thread_with_return_value(int mode, BLASLONG m, BLASLONG n,
	BLASLONG k, void *alpha, void *a, BLASLONG lda, void *b, BLASLONG ldb,
	void *c, BLASLONG ldc, int (*function)(), int nthreads);
#endif


static FLOAT zasum_compute(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
	FLOAT  asum = 0.0 ;

	if ( n < 0 )  return(asum);

	__asm__ __volatile__ (
	"	mov	"N", %[N_]			\n"
	"	mov	"X", %[X_]			\n"
	"	mov	"INC_X", %[INCX_]		\n"
	"	fmov	"SUMF", "REG0"			\n"
	"	fmov	d1, "REG0"			\n"
	"	fmov	d2, "REG0"			\n"
	"	fmov	d3, "REG0"			\n"
	"	fmov	d4, "REG0"			\n"
	"	fmov	d5, "REG0"			\n"
	"	fmov	d6, "REG0"			\n"
	"	fmov	d7, "REG0"			\n"
	"	cmp	"N", xzr			\n"
	"	ble	9f //asum_kernel_L999		\n"
	"	cmp	"INC_X", xzr			\n"
	"	ble	9f //asum_kernel_L999		\n"
	"	cmp	"INC_X", #1			\n"
	"	bne	5f //asum_kernel_S_BEGIN	\n"

	"1: //asum_kernel_F_BEGIN:			\n"
	"	asr	"J", "N", #4			\n"
	"	cmp	"J", xzr			\n"
	"	beq	3f //asum_kernel_F1		\n"

	".align 5					\n"
	"2: //asum_kernel_F16:				\n"
	"	"KERNEL_F16"				\n"
	"	subs	"J", "J", #1			\n"
	"	bne	2b //asum_kernel_F16		\n"
	"	"KERNEL_F16_FINALIZE"			\n"

	"3: //asum_kernel_F1:				\n"
	"	ands	"J", "N", #15			\n"
	"	ble	9f //asum_kernel_L999		\n"

	"4: //asum_kernel_F10:				\n"
	"	"KERNEL_F1"				\n"
	"	subs    "J", "J", #1			\n"
	"	bne	4b //asum_kernel_F10		\n"
	"	b	9f //asum_kernel_L999		\n"

	"5: //asum_kernel_S_BEGIN:			\n"
	"	"INIT_S"				\n"
	"	asr	"J", "N", #2			\n"
	"	cmp	"J", xzr			\n"
	"	ble	7f //asum_kernel_S1		\n"

	"6: //asum_kernel_S4:				\n"
	"	"KERNEL_S1"				\n"
	"	"KERNEL_S1"				\n"
	"	"KERNEL_S1"				\n"
	"	"KERNEL_S1"				\n"
	"	subs	"J", "J", #1			\n"
	"	bne	6b //asum_kernel_S4		\n"

	"7: //asum_kernel_S1:				\n"
	"	ands	"J", "N", #3			\n"
	"	ble	9f //asum_kernel_L999		\n"

	"8: //asum_kernel_S10:				\n"
	"	"KERNEL_S1"				\n"
	"	subs	"J", "J", #1			\n"
	"	bne	8b //asum_kernel_S10		\n"

	"9: //asum_kernel_L999:				\n"
	"	fmov	%[ASUM_], "SUMF"		\n"

	: [ASUM_] "=r" (asum)		//%0
	: [N_]    "r"  (n),		//%1
	  [X_]    "r"  (x),		//%2
	  [INCX_] "r"  (inc_x)		//%3
	: "cc",
	  "memory",
	  "x0", "x1", "x2", "x3", "x4", "x5",
	  "d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7"
	);

	return asum;
}

#if defined(SMP)
static int zasum_thread_function(BLASLONG n, BLASLONG dummy0,
	BLASLONG dummy1, FLOAT dummy2, FLOAT *x, BLASLONG inc_x, FLOAT *y,
	BLASLONG inc_y, FLOAT *result, BLASLONG dummy3)
{
	*result = zasum_compute(n, x, inc_x);

	return 0;
}
#endif

FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
#if defined(SMP)
	int nthreads;
	FLOAT dummy_alpha;
#endif
	FLOAT asum = 0.0;

#if defined(SMP)
	if (inc_x == 0 || n <= 10000)
		nthreads = 1;
	else
		nthreads = num_cpu_avail(1);

	if (nthreads == 1) {
		asum = zasum_compute(n, x, inc_x);
	} else {
		int mode, i;
		char result[MAX_CPU_NUMBER * sizeof(double) * 2];
		FLOAT *ptr;

		mode = BLAS_DOUBLE | BLAS_COMPLEX;

		blas_level1_thread_with_return_value(mode, n, 0, 0, &dummy_alpha,
				   x, inc_x, NULL, 0, result, 0,
				   ( void *)zasum_thread_function, nthreads);

		ptr = (FLOAT *)result;
		for (i = 0; i < nthreads; i++) {
			asum = asum + (*ptr);
			ptr = (FLOAT *)(((char *)ptr) + sizeof(double) * 2);
		}
	}
#else
	asum = zasum_compute(n, x, inc_x);
#endif

	return asum;
}
