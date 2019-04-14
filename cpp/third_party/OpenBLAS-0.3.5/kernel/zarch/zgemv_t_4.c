/***************************************************************************
Copyright (c) 2018, The OpenBLAS Project
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

#define NBMAX 1024
#define HAVE_KERNEL_4x4_VEC 1
#define HAVE_KERNEL_4x2_VEC 1
#define HAVE_KERNEL_4x1_VEC 1

#if defined(HAVE_KERNEL_4x4_VEC) || defined(HAVE_KERNEL_4x2_VEC) || defined(HAVE_KERNEL_4x1_VEC)
#include <vecintrin.h> 
#endif

#ifdef HAVE_KERNEL_4x4_VEC_ASM

#elif HAVE_KERNEL_4x4_VEC

static void zgemv_kernel_4x4(BLASLONG n, BLASLONG lda, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT alpha_r, FLOAT alpha_i) {
    BLASLONG i;
    FLOAT *a0, *a1, *a2, *a3;
    a0 = ap;
    a1 = ap + lda;
    a2 = a1 + lda;
    a3 = a2 + lda;
    //p for positive(real*real,image*image) r for image (real*image,image*real)
    register __vector double vtemp0_p = {0.0, 0.0};
    register __vector double vtemp0_r = {0.0, 0.0};
    register __vector double vtemp1_p = {0.0, 0.0};
    register __vector double vtemp1_r = {0.0, 0.0};
    register __vector double vtemp2_p = {0.0, 0.0};
    register __vector double vtemp2_r = {0.0, 0.0};
    register __vector double vtemp3_p = {0.0, 0.0};
    register __vector double vtemp3_r = {0.0, 0.0};
    i = 0;
    n = n << 1;
    while (i < n) {
//        __builtin_prefetch(&x[i]);
//        __builtin_prefetch(&a0[i]);   
//        __builtin_prefetch(&a1[i]);
//        __builtin_prefetch(&a2[i]);
//        __builtin_prefetch(&a3[i]);
        register __vector double vx_0 = *(__vector double*) (&x[i]);
        register __vector double vx_1 = *(__vector double*) (&x[i + 2]);
        register __vector double vx_2 = *(__vector double*) (&x[i + 4]);
        register __vector double vx_3 = *(__vector double*) (&x[i + 6]);

        register __vector double va0 = *(__vector double*) (&a0[i]);
        register __vector double va0_1 = *(__vector double*) (&a0[i + 2]);
        register __vector double va0_2 = *(__vector double*) (&a0[i + 4]);
        register __vector double va0_3 = *(__vector double*) (&a0[i + 6]);

        register __vector double va1 = *(__vector double*) (&a1[i]);
        register __vector double va1_1 = *(__vector double*) (&a1[i + 2]);
        register __vector double va1_2 = *(__vector double*) (&a1[i + 4]);
        register __vector double va1_3 = *(__vector double*) (&a1[i + 6]);

        register __vector double va2 = *(__vector double*) (&a2[i]);
        register __vector double va2_1 = *(__vector double*) (&a2[i + 2]);
        register __vector double va2_2 = *(__vector double*) (&a2[i + 4]);
        register __vector double va2_3 = *(__vector double*) (&a2[i + 6]);

        register __vector double va3 = *(__vector double*) (&a3[i]);
        register __vector double va3_1 = *(__vector double*) (&a3[i + 2]);
        register __vector double va3_2 = *(__vector double*) (&a3[i + 4]);
        register __vector double va3_3 = *(__vector double*) (&a3[i + 6]);

        register __vector double vxr_0 = vec_permi(vx_0, vx_0, 2);
        register __vector double vxr_1 = vec_permi(vx_1, vx_1, 2);

        i += 8;

        vtemp0_p += vx_0*va0;
        vtemp0_r += vxr_0*va0;

        vtemp1_p += vx_0*va1;
        vtemp1_r += vxr_0*va1;

        vtemp2_p += vx_0*va2;
        vtemp2_r += vxr_0*va2;

        vtemp3_p += vx_0*va3;
        vtemp3_r += vxr_0*va3;

        vtemp0_p += vx_1*va0_1;
        vtemp0_r += vxr_1*va0_1;

        vtemp1_p += vx_1*va1_1;
        vtemp1_r += vxr_1*va1_1;
        vxr_0 = vec_permi(vx_2, vx_2, 2);
        vtemp2_p += vx_1*va2_1;
        vtemp2_r += vxr_1*va2_1;

        vtemp3_p += vx_1*va3_1;
        vtemp3_r += vxr_1*va3_1;

        vtemp0_p += vx_2*va0_2;
        vtemp0_r += vxr_0*va0_2;
        vxr_1 = vec_permi(vx_3, vx_3, 2);

        vtemp1_p += vx_2*va1_2;
        vtemp1_r += vxr_0*va1_2;

        vtemp2_p += vx_2*va2_2;
        vtemp2_r += vxr_0*va2_2;

        vtemp3_p += vx_2*va3_2;
        vtemp3_r += vxr_0*va3_2;

        vtemp0_p += vx_3*va0_3;
        vtemp0_r += vxr_1*va0_3;

        vtemp1_p += vx_3*va1_3;
        vtemp1_r += vxr_1*va1_3;

        vtemp2_p += vx_3*va2_3;
        vtemp2_r += vxr_1*va2_3;

        vtemp3_p += vx_3*va3_3;
        vtemp3_r += vxr_1*va3_3;

    }

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )

    register FLOAT temp_r0 = vtemp0_p[0] - vtemp0_p[1];
    register FLOAT temp_i0 = vtemp0_r[0] + vtemp0_r[1];

    register FLOAT temp_r1 = vtemp1_p[0] - vtemp1_p[1];
    register FLOAT temp_i1 = vtemp1_r[0] + vtemp1_r[1];

    register FLOAT temp_r2 = vtemp2_p[0] - vtemp2_p[1];
    register FLOAT temp_i2 = vtemp2_r[0] + vtemp2_r[1];

    register FLOAT temp_r3 = vtemp3_p[0] - vtemp3_p[1];
    register FLOAT temp_i3 = vtemp3_r[0] + vtemp3_r[1];

#else
    register FLOAT temp_r0 = vtemp0_p[0] + vtemp0_p[1];
    register FLOAT temp_i0 = vtemp0_r[0] - vtemp0_r[1];

    register FLOAT temp_r1 = vtemp1_p[0] + vtemp1_p[1];
    register FLOAT temp_i1 = vtemp1_r[0] - vtemp1_r[1];

    register FLOAT temp_r2 = vtemp2_p[0] + vtemp2_p[1];
    register FLOAT temp_i2 = vtemp2_r[0] - vtemp2_r[1];

    register FLOAT temp_r3 = vtemp3_p[0] + vtemp3_p[1];
    register FLOAT temp_i3 = vtemp3_r[0] - vtemp3_r[1];

#endif    

#if !defined(XCONJ)

    y[0] += alpha_r * temp_r0 - alpha_i * temp_i0;
    y[1] += alpha_r * temp_i0 + alpha_i * temp_r0;
    y[2] += alpha_r * temp_r1 - alpha_i * temp_i1;
    y[3] += alpha_r * temp_i1 + alpha_i * temp_r1;
    y[4] += alpha_r * temp_r2 - alpha_i * temp_i2;
    y[5] += alpha_r * temp_i2 + alpha_i * temp_r2;
    y[6] += alpha_r * temp_r3 - alpha_i * temp_i3;
    y[7] += alpha_r * temp_i3 + alpha_i * temp_r3;

#else

    y[0] += alpha_r * temp_r0 + alpha_i * temp_i0;
    y[1] -= alpha_r * temp_i0 - alpha_i * temp_r0;
    y[2] += alpha_r * temp_r1 + alpha_i * temp_i1;
    y[3] -= alpha_r * temp_i1 - alpha_i * temp_r1;
    y[4] += alpha_r * temp_r2 + alpha_i * temp_i2;
    y[5] -= alpha_r * temp_i2 - alpha_i * temp_r2;
    y[6] += alpha_r * temp_r3 + alpha_i * temp_i3;
    y[7] -= alpha_r * temp_i3 - alpha_i * temp_r3;

#endif
}

#else

static void zgemv_kernel_4x4(BLASLONG n, BLASLONG lda, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT alpha_r, FLOAT alpha_i) {
    BLASLONG i;
    FLOAT *a0, *a1, *a2, *a3;
    a0 = ap;
    a1 = ap + lda;
    a2 = a1 + lda;
    a3 = a2 + lda;

    FLOAT temp_r0 = 0.0;
    FLOAT temp_r1 = 0.0;
    FLOAT temp_r2 = 0.0;
    FLOAT temp_r3 = 0.0;
    FLOAT temp_i0 = 0.0;
    FLOAT temp_i1 = 0.0;
    FLOAT temp_i2 = 0.0;
    FLOAT temp_i3 = 0.0;

    for (i = 0; i < 2 * n; i += 2) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        temp_r0 += a0[i] * x[i] - a0[i + 1] * x[i + 1];
        temp_i0 += a0[i] * x[i + 1] + a0[i + 1] * x[i];
        temp_r1 += a1[i] * x[i] - a1[i + 1] * x[i + 1];
        temp_i1 += a1[i] * x[i + 1] + a1[i + 1] * x[i];
        temp_r2 += a2[i] * x[i] - a2[i + 1] * x[i + 1];
        temp_i2 += a2[i] * x[i + 1] + a2[i + 1] * x[i];
        temp_r3 += a3[i] * x[i] - a3[i + 1] * x[i + 1];
        temp_i3 += a3[i] * x[i + 1] + a3[i + 1] * x[i];
#else
        temp_r0 += a0[i] * x[i] + a0[i + 1] * x[i + 1];
        temp_i0 += a0[i] * x[i + 1] - a0[i + 1] * x[i];
        temp_r1 += a1[i] * x[i] + a1[i + 1] * x[i + 1];
        temp_i1 += a1[i] * x[i + 1] - a1[i + 1] * x[i];
        temp_r2 += a2[i] * x[i] + a2[i + 1] * x[i + 1];
        temp_i2 += a2[i] * x[i + 1] - a2[i + 1] * x[i];
        temp_r3 += a3[i] * x[i] + a3[i + 1] * x[i + 1];
        temp_i3 += a3[i] * x[i + 1] - a3[i + 1] * x[i];
#endif
    }

#if !defined(XCONJ)

    y[0] += alpha_r * temp_r0 - alpha_i * temp_i0;
    y[1] += alpha_r * temp_i0 + alpha_i * temp_r0;
    y[2] += alpha_r * temp_r1 - alpha_i * temp_i1;
    y[3] += alpha_r * temp_i1 + alpha_i * temp_r1;
    y[4] += alpha_r * temp_r2 - alpha_i * temp_i2;
    y[5] += alpha_r * temp_i2 + alpha_i * temp_r2;
    y[6] += alpha_r * temp_r3 - alpha_i * temp_i3;
    y[7] += alpha_r * temp_i3 + alpha_i * temp_r3;

#else

    y[0] += alpha_r * temp_r0 + alpha_i * temp_i0;
    y[1] -= alpha_r * temp_i0 - alpha_i * temp_r0;
    y[2] += alpha_r * temp_r1 + alpha_i * temp_i1;
    y[3] -= alpha_r * temp_i1 - alpha_i * temp_r1;
    y[4] += alpha_r * temp_r2 + alpha_i * temp_i2;
    y[5] -= alpha_r * temp_i2 - alpha_i * temp_r2;
    y[6] += alpha_r * temp_r3 + alpha_i * temp_i3;
    y[7] -= alpha_r * temp_i3 - alpha_i * temp_r3;

#endif
}

#endif

#ifdef HAVE_KERNEL_4x2_VEC

static void zgemv_kernel_4x2(BLASLONG n, BLASLONG lda, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT alpha_r, FLOAT alpha_i) {
    BLASLONG i;
    FLOAT *a0, *a1;
    a0 = ap;
    a1 = ap + lda; 
    //p for positive(real*real,image*image) r for image (real*image,image*real)
    register __vector double vtemp0_p = {0.0, 0.0};
    register __vector double vtemp0_r = {0.0, 0.0};
    register __vector double vtemp1_p = {0.0, 0.0};
    register __vector double vtemp1_r = {0.0, 0.0}; 
    i = 0;
    n = n << 1;
    while (i < n) {

        register __vector double vx_0 = *(__vector double*) (&x[i]);
        register __vector double vx_1 = *(__vector double*) (&x[i + 2]);
        register __vector double vx_2 = *(__vector double*) (&x[i + 4]);
        register __vector double vx_3 = *(__vector double*) (&x[i + 6]);

        register __vector double va0 = *(__vector double*) (&a0[i]);
        register __vector double va0_1 = *(__vector double*) (&a0[i + 2]);
        register __vector double va0_2 = *(__vector double*) (&a0[i + 4]);
        register __vector double va0_3 = *(__vector double*) (&a0[i + 6]);

        register __vector double va1 = *(__vector double*) (&a1[i]);
        register __vector double va1_1 = *(__vector double*) (&a1[i + 2]);
        register __vector double va1_2 = *(__vector double*) (&a1[i + 4]);
        register __vector double va1_3 = *(__vector double*) (&a1[i + 6]);

        register __vector double vxr_0 = vec_permi(vx_0, vx_0, 2);
        register __vector double vxr_1 = vec_permi(vx_1, vx_1, 2);

        i += 8;

        vtemp0_p += vx_0*va0;
        vtemp0_r += vxr_0*va0;

        vtemp1_p += vx_0*va1;
        vtemp1_r += vxr_0*va1;

        vxr_0 = vec_permi(vx_2, vx_2, 2);  
        vtemp0_p += vx_1*va0_1;
        vtemp0_r += vxr_1*va0_1;

        vtemp1_p += vx_1*va1_1;
        vtemp1_r += vxr_1*va1_1;
        vxr_1 = vec_permi(vx_3, vx_3, 2);

        vtemp0_p += vx_2*va0_2;
        vtemp0_r += vxr_0*va0_2;

        vtemp1_p += vx_2*va1_2;
        vtemp1_r += vxr_0*va1_2;

        vtemp0_p += vx_3*va0_3;
        vtemp0_r += vxr_1*va0_3;

        vtemp1_p += vx_3*va1_3;
        vtemp1_r += vxr_1*va1_3;
 
    }

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
    register FLOAT temp_r0 = vtemp0_p[0] - vtemp0_p[1];
    register FLOAT temp_i0 = vtemp0_r[0] + vtemp0_r[1];

    register FLOAT temp_r1 = vtemp1_p[0] - vtemp1_p[1];
    register FLOAT temp_i1 = vtemp1_r[0] + vtemp1_r[1]; 

#else
    register FLOAT temp_r0 = vtemp0_p[0] + vtemp0_p[1];
    register FLOAT temp_i0 = vtemp0_r[0] - vtemp0_r[1];

    register FLOAT temp_r1 = vtemp1_p[0] + vtemp1_p[1];
    register FLOAT temp_i1 = vtemp1_r[0] - vtemp1_r[1];

#endif    

#if !defined(XCONJ)

    y[0] += alpha_r * temp_r0 - alpha_i * temp_i0;
    y[1] += alpha_r * temp_i0 + alpha_i * temp_r0;
    y[2] += alpha_r * temp_r1 - alpha_i * temp_i1;
    y[3] += alpha_r * temp_i1 + alpha_i * temp_r1;

#else

    y[0] += alpha_r * temp_r0 + alpha_i * temp_i0;
    y[1] -= alpha_r * temp_i0 - alpha_i * temp_r0;
    y[2] += alpha_r * temp_r1 + alpha_i * temp_i1;
    y[3] -= alpha_r * temp_i1 - alpha_i * temp_r1;

#endif
}

#else

static void zgemv_kernel_4x2(BLASLONG n, BLASLONG lda, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT alpha_r, FLOAT alpha_i) {
    BLASLONG i;
    FLOAT *a0, *a1;
    a0 = ap;
    a1 = ap + lda;

    FLOAT temp_r0 = 0.0;
    FLOAT temp_r1 = 0.0;
    FLOAT temp_i0 = 0.0;
    FLOAT temp_i1 = 0.0;

    for (i = 0; i < 2 * n; i += 2) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        temp_r0 += a0[i] * x[i] - a0[i + 1] * x[i + 1];
        temp_i0 += a0[i] * x[i + 1] + a0[i + 1] * x[i];
        temp_r1 += a1[i] * x[i] - a1[i + 1] * x[i + 1];
        temp_i1 += a1[i] * x[i + 1] + a1[i + 1] * x[i];
#else
        temp_r0 += a0[i] * x[i] + a0[i + 1] * x[i + 1];
        temp_i0 += a0[i] * x[i + 1] - a0[i + 1] * x[i];
        temp_r1 += a1[i] * x[i] + a1[i + 1] * x[i + 1];
        temp_i1 += a1[i] * x[i + 1] - a1[i + 1] * x[i];
#endif
    }

#if !defined(XCONJ)

    y[0] += alpha_r * temp_r0 - alpha_i * temp_i0;
    y[1] += alpha_r * temp_i0 + alpha_i * temp_r0;
    y[2] += alpha_r * temp_r1 - alpha_i * temp_i1;
    y[3] += alpha_r * temp_i1 + alpha_i * temp_r1;

#else

    y[0] += alpha_r * temp_r0 + alpha_i * temp_i0;
    y[1] -= alpha_r * temp_i0 - alpha_i * temp_r0;
    y[2] += alpha_r * temp_r1 + alpha_i * temp_i1;
    y[3] -= alpha_r * temp_i1 - alpha_i * temp_r1;

#endif
}

#endif

#ifdef HAVE_KERNEL_4x1_VEC

static void zgemv_kernel_4x1(BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT alpha_r, FLOAT alpha_i) {
    BLASLONG i;
    FLOAT *a0 ;
    a0 = ap;  
    //p for positive(real*real,image*image) r for image (real*image,image*real)
    register __vector double vtemp0_p = {0.0, 0.0};
    register __vector double vtemp0_r = {0.0, 0.0};
    i = 0;
    n = n << 1;
    while (i < n) {

        register __vector double vx_0 = *(__vector double*) (&x[i]);
        register __vector double vx_1 = *(__vector double*) (&x[i + 2]);
        register __vector double vx_2 = *(__vector double*) (&x[i + 4]);
        register __vector double vx_3 = *(__vector double*) (&x[i + 6]);

        register __vector double va0 = *(__vector double*) (&a0[i]);
        register __vector double va0_1 = *(__vector double*) (&a0[i + 2]);
        register __vector double va0_2 = *(__vector double*) (&a0[i + 4]);
        register __vector double va0_3 = *(__vector double*) (&a0[i + 6]);
       
        register __vector double vxr_0 = vec_permi(vx_0, vx_0, 2);
        register __vector double vxr_1 = vec_permi(vx_1, vx_1, 2);

        i += 8;

        vtemp0_p += vx_0*va0;
        vtemp0_r += vxr_0*va0;
 
        vxr_0 = vec_permi(vx_2, vx_2, 2);  
        vtemp0_p += vx_1*va0_1;
        vtemp0_r += vxr_1*va0_1;
 
        vxr_1 = vec_permi(vx_3, vx_3, 2);

        vtemp0_p += vx_2*va0_2;
        vtemp0_r += vxr_0*va0_2;
 
        vtemp0_p += vx_3*va0_3;
        vtemp0_r += vxr_1*va0_3;
 
    }

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
    register FLOAT temp_r0 = vtemp0_p[0] - vtemp0_p[1];
    register FLOAT temp_i0 = vtemp0_r[0] + vtemp0_r[1];

#else
    register FLOAT temp_r0 = vtemp0_p[0] + vtemp0_p[1];
    register FLOAT temp_i0 = vtemp0_r[0] - vtemp0_r[1]; 

#endif    

#if !defined(XCONJ)

    y[0] += alpha_r * temp_r0 - alpha_i * temp_i0;
    y[1] += alpha_r * temp_i0 + alpha_i * temp_r0; 

#else

    y[0] += alpha_r * temp_r0 + alpha_i * temp_i0;
    y[1] -= alpha_r * temp_i0 - alpha_i * temp_r0; 
#endif

}

#else

static void zgemv_kernel_4x1(BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y, FLOAT alpha_r, FLOAT alpha_i) {
    BLASLONG i;
    FLOAT *a0;
    a0 = ap;

    FLOAT temp_r0 = 0.0;
    FLOAT temp_i0 = 0.0;

    for (i = 0; i < 2 * n; i += 2) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        temp_r0 += a0[i] * x[i] - a0[i + 1] * x[i + 1];
        temp_i0 += a0[i] * x[i + 1] + a0[i + 1] * x[i];
#else
        temp_r0 += a0[i] * x[i] + a0[i + 1] * x[i + 1];
        temp_i0 += a0[i] * x[i + 1] - a0[i + 1] * x[i];
#endif
    }

#if !defined(XCONJ)

    y[0] += alpha_r * temp_r0 - alpha_i * temp_i0;
    y[1] += alpha_r * temp_i0 + alpha_i * temp_r0;

#else

    y[0] += alpha_r * temp_r0 + alpha_i * temp_i0;
    y[1] -= alpha_r * temp_i0 - alpha_i * temp_r0;

#endif

}

#endif

static __attribute__((always_inline)) void copy_x(BLASLONG n, FLOAT *src, FLOAT *dest, BLASLONG inc_src) {
    BLASLONG i;
    for (i = 0; i < n; i++) {
        *dest = *src;
        *(dest + 1) = *(src + 1);
        dest += 2;
        src += inc_src;
    }
}

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha_r, FLOAT alpha_i, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *buffer) {
    BLASLONG i;
    BLASLONG j;
    FLOAT *a_ptr;
    FLOAT *x_ptr;
    FLOAT *y_ptr;

    BLASLONG n1;
    BLASLONG m1;
    BLASLONG m2;
    BLASLONG m3;
    BLASLONG n2;

    FLOAT ybuffer[8], *xbuffer;

    if (m < 1) return (0);
    if (n < 1) return (0);

    inc_x <<= 1;
    inc_y <<= 1;
    lda <<= 1;

    xbuffer = buffer;

    n1 = n >> 2;
    n2 = n & 3;

    m3 = m & 3;
    m1 = m - m3;
    m2 = (m & (NBMAX - 1)) - m3;

    BLASLONG NB = NBMAX;

    while (NB == NBMAX) {

        m1 -= NB;
        if (m1 < 0) {
            if (m2 == 0) break;
            NB = m2;
        }

        y_ptr = y;
        a_ptr = a;
        x_ptr = x;

        if (inc_x != 2)
            copy_x(NB, x_ptr, xbuffer, inc_x);
        else
            xbuffer = x_ptr;

        if (inc_y == 2) {

            for (i = 0; i < n1; i++) {
                zgemv_kernel_4x4(NB, lda, a_ptr, xbuffer, y_ptr, alpha_r, alpha_i);
                a_ptr += lda << 2;
                y_ptr += 8;

            }

            if (n2 & 2) {
                zgemv_kernel_4x2(NB, lda, a_ptr, xbuffer, y_ptr, alpha_r, alpha_i);
                a_ptr += lda << 1;
                y_ptr += 4;

            }

            if (n2 & 1) {
                zgemv_kernel_4x1(NB, a_ptr, xbuffer, y_ptr, alpha_r, alpha_i);
                a_ptr += lda;
                y_ptr += 2;

            }

        } else {

            for (i = 0; i < n1; i++) {
                memset(ybuffer, 0, sizeof (ybuffer));
                zgemv_kernel_4x4(NB, lda, a_ptr, xbuffer, ybuffer, alpha_r, alpha_i);

                a_ptr += lda << 2;

                y_ptr[0] += ybuffer[0];
                y_ptr[1] += ybuffer[1];
                y_ptr += inc_y;
                y_ptr[0] += ybuffer[2];
                y_ptr[1] += ybuffer[3];
                y_ptr += inc_y;
                y_ptr[0] += ybuffer[4];
                y_ptr[1] += ybuffer[5];
                y_ptr += inc_y;
                y_ptr[0] += ybuffer[6];
                y_ptr[1] += ybuffer[7];
                y_ptr += inc_y;

            }

            for (i = 0; i < n2; i++) {
                memset(ybuffer, 0, sizeof (ybuffer));
                zgemv_kernel_4x1(NB, a_ptr, xbuffer, ybuffer, alpha_r, alpha_i);
                a_ptr += lda;
                y_ptr[0] += ybuffer[0];
                y_ptr[1] += ybuffer[1];
                y_ptr += inc_y;

            }

        }
        a += 2 * NB;
        x += NB * inc_x;
    }

    if (m3 == 0) return (0);

    x_ptr = x;
    j = 0;
    a_ptr = a;
    y_ptr = y;

    if (m3 == 3) {

        FLOAT temp_r;
        FLOAT temp_i;
        FLOAT x0 = x_ptr[0];
        FLOAT x1 = x_ptr[1];
        x_ptr += inc_x;
        FLOAT x2 = x_ptr[0];
        FLOAT x3 = x_ptr[1];
        x_ptr += inc_x;
        FLOAT x4 = x_ptr[0];
        FLOAT x5 = x_ptr[1];
        while (j < n) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
            temp_r = a_ptr[0] * x0 - a_ptr[1] * x1;
            temp_i = a_ptr[0] * x1 + a_ptr[1] * x0;
            temp_r += a_ptr[2] * x2 - a_ptr[3] * x3;
            temp_i += a_ptr[2] * x3 + a_ptr[3] * x2;
            temp_r += a_ptr[4] * x4 - a_ptr[5] * x5;
            temp_i += a_ptr[4] * x5 + a_ptr[5] * x4;
#else

            temp_r = a_ptr[0] * x0 + a_ptr[1] * x1;
            temp_i = a_ptr[0] * x1 - a_ptr[1] * x0;
            temp_r += a_ptr[2] * x2 + a_ptr[3] * x3;
            temp_i += a_ptr[2] * x3 - a_ptr[3] * x2;
            temp_r += a_ptr[4] * x4 + a_ptr[5] * x5;
            temp_i += a_ptr[4] * x5 - a_ptr[5] * x4;
#endif

#if !defined(XCONJ) 
            y_ptr[0] += alpha_r * temp_r - alpha_i * temp_i;
            y_ptr[1] += alpha_r * temp_i + alpha_i * temp_r;
#else
            y_ptr[0] += alpha_r * temp_r + alpha_i * temp_i;
            y_ptr[1] -= alpha_r * temp_i - alpha_i * temp_r;
#endif

            a_ptr += lda;
            y_ptr += inc_y;
            j++;
        }
        return (0);
    }

    if (m3 == 2) {

        FLOAT temp_r;
        FLOAT temp_i;
        FLOAT temp_r1;
        FLOAT temp_i1;
        FLOAT x0 = x_ptr[0];
        FLOAT x1 = x_ptr[1];
        x_ptr += inc_x;
        FLOAT x2 = x_ptr[0];
        FLOAT x3 = x_ptr[1];

        while (j < (n & -2)) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
            temp_r = a_ptr[0] * x0 - a_ptr[1] * x1;
            temp_i = a_ptr[0] * x1 + a_ptr[1] * x0;
            temp_r += a_ptr[2] * x2 - a_ptr[3] * x3;
            temp_i += a_ptr[2] * x3 + a_ptr[3] * x2;
            a_ptr += lda;
            temp_r1 = a_ptr[0] * x0 - a_ptr[1] * x1;
            temp_i1 = a_ptr[0] * x1 + a_ptr[1] * x0;
            temp_r1 += a_ptr[2] * x2 - a_ptr[3] * x3;
            temp_i1 += a_ptr[2] * x3 + a_ptr[3] * x2;
#else

            temp_r = a_ptr[0] * x0 + a_ptr[1] * x1;
            temp_i = a_ptr[0] * x1 - a_ptr[1] * x0;
            temp_r += a_ptr[2] * x2 + a_ptr[3] * x3;
            temp_i += a_ptr[2] * x3 - a_ptr[3] * x2;
            a_ptr += lda;
            temp_r1 = a_ptr[0] * x0 + a_ptr[1] * x1;
            temp_i1 = a_ptr[0] * x1 - a_ptr[1] * x0;
            temp_r1 += a_ptr[2] * x2 + a_ptr[3] * x3;
            temp_i1 += a_ptr[2] * x3 - a_ptr[3] * x2;
#endif

#if !defined(XCONJ) 
            y_ptr[0] += alpha_r * temp_r - alpha_i * temp_i;
            y_ptr[1] += alpha_r * temp_i + alpha_i * temp_r;
            y_ptr += inc_y;
            y_ptr[0] += alpha_r * temp_r1 - alpha_i * temp_i1;
            y_ptr[1] += alpha_r * temp_i1 + alpha_i * temp_r1;
#else
            y_ptr[0] += alpha_r * temp_r + alpha_i * temp_i;
            y_ptr[1] -= alpha_r * temp_i - alpha_i * temp_r;
            y_ptr += inc_y;
            y_ptr[0] += alpha_r * temp_r1 + alpha_i * temp_i1;
            y_ptr[1] -= alpha_r * temp_i1 - alpha_i * temp_r1;
#endif

            a_ptr += lda;
            y_ptr += inc_y;
            j += 2;
        }

        while (j < n) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
            temp_r = a_ptr[0] * x0 - a_ptr[1] * x1;
            temp_i = a_ptr[0] * x1 + a_ptr[1] * x0;
            temp_r += a_ptr[2] * x2 - a_ptr[3] * x3;
            temp_i += a_ptr[2] * x3 + a_ptr[3] * x2;
#else

            temp_r = a_ptr[0] * x0 + a_ptr[1] * x1;
            temp_i = a_ptr[0] * x1 - a_ptr[1] * x0;
            temp_r += a_ptr[2] * x2 + a_ptr[3] * x3;
            temp_i += a_ptr[2] * x3 - a_ptr[3] * x2;
#endif

#if !defined(XCONJ) 
            y_ptr[0] += alpha_r * temp_r - alpha_i * temp_i;
            y_ptr[1] += alpha_r * temp_i + alpha_i * temp_r;
#else
            y_ptr[0] += alpha_r * temp_r + alpha_i * temp_i;
            y_ptr[1] -= alpha_r * temp_i - alpha_i * temp_r;
#endif

            a_ptr += lda;
            y_ptr += inc_y;
            j++;
        }

        return (0);
    }

    if (m3 == 1) {

        FLOAT temp_r;
        FLOAT temp_i;
        FLOAT temp_r1;
        FLOAT temp_i1;
        FLOAT x0 = x_ptr[0];
        FLOAT x1 = x_ptr[1];

        while (j < (n & -2)) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
            temp_r = a_ptr[0] * x0 - a_ptr[1] * x1;
            temp_i = a_ptr[0] * x1 + a_ptr[1] * x0;
            a_ptr += lda;
            temp_r1 = a_ptr[0] * x0 - a_ptr[1] * x1;
            temp_i1 = a_ptr[0] * x1 + a_ptr[1] * x0;
#else

            temp_r = a_ptr[0] * x0 + a_ptr[1] * x1;
            temp_i = a_ptr[0] * x1 - a_ptr[1] * x0;
            a_ptr += lda;
            temp_r1 = a_ptr[0] * x0 + a_ptr[1] * x1;
            temp_i1 = a_ptr[0] * x1 - a_ptr[1] * x0;
#endif

#if !defined(XCONJ) 
            y_ptr[0] += alpha_r * temp_r - alpha_i * temp_i;
            y_ptr[1] += alpha_r * temp_i + alpha_i * temp_r;
            y_ptr += inc_y;
            y_ptr[0] += alpha_r * temp_r1 - alpha_i * temp_i1;
            y_ptr[1] += alpha_r * temp_i1 + alpha_i * temp_r1;
#else
            y_ptr[0] += alpha_r * temp_r + alpha_i * temp_i;
            y_ptr[1] -= alpha_r * temp_i - alpha_i * temp_r;
            y_ptr += inc_y;
            y_ptr[0] += alpha_r * temp_r1 + alpha_i * temp_i1;
            y_ptr[1] -= alpha_r * temp_i1 - alpha_i * temp_r1;
#endif

            a_ptr += lda;
            y_ptr += inc_y;
            j += 2;
        }

        while (j < n) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
            temp_r = a_ptr[0] * x0 - a_ptr[1] * x1;
            temp_i = a_ptr[0] * x1 + a_ptr[1] * x0;
#else

            temp_r = a_ptr[0] * x0 + a_ptr[1] * x1;
            temp_i = a_ptr[0] * x1 - a_ptr[1] * x0;
#endif

#if !defined(XCONJ) 
            y_ptr[0] += alpha_r * temp_r - alpha_i * temp_i;
            y_ptr[1] += alpha_r * temp_i + alpha_i * temp_r;
#else
            y_ptr[0] += alpha_r * temp_r + alpha_i * temp_i;
            y_ptr[1] -= alpha_r * temp_i - alpha_i * temp_r;
#endif

            a_ptr += lda;
            y_ptr += inc_y;
            j++;
        }
        return (0);
    }

    return (0);

}

