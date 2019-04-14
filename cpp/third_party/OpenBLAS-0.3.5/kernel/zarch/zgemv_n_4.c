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

#include <stdlib.h>
#include <stdio.h>
#include "common.h"

#define HAVE_KERNEL_4x4_VEC 1
#define HAVE_KERNEL_4x2_VEC 1
#define HAVE_KERNEL_4x1_VEC 1
#define HAVE_KERNEL_ADDY 1

#if defined(HAVE_KERNEL_4x4_VEC) || defined(HAVE_KERNEL_4x2_VEC) || defined(HAVE_KERNEL_4x1_VEC)
#include <vecintrin.h> 
#endif

// 
#define NBMAX 1024

#ifdef HAVE_KERNEL_4x4_VEC_ASM

#elif HAVE_KERNEL_4x4_VEC

static void zgemv_kernel_4x4(BLASLONG n, BLASLONG lda, FLOAT *ap, FLOAT *x, FLOAT *y) {
    BLASLONG i;
    FLOAT *a0, *a1, *a2, *a3;
    a0 = ap;
    a1 = ap + lda;
    a2 = a1 + lda;
    a3 = a2 + lda;

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )

    register __vector double vx0_r = {x[0], x[0]};
    register __vector double vx0_i = {-x[1], x[1]};
    register __vector double vx1_r = {x[2], x[2]};
    register __vector double vx1_i = {-x[3], x[3]};
    register __vector double vx2_r = {x[4], x[4]};
    register __vector double vx2_i = {-x[5], x[5]};
    register __vector double vx3_r = {x[6], x[6]};
    register __vector double vx3_i = {-x[7], x[7]};

#else
    register __vector double vx0_r = {x[0], -x[0]};
    register __vector double vx0_i = {x[1], x[1]};
    register __vector double vx1_r = {x[2], -x[2]};
    register __vector double vx1_i = {x[3], x[3]};
    register __vector double vx2_r = {x[4], -x[4]};
    register __vector double vx2_i = {x[5], x[5]};
    register __vector double vx3_r = {x[6], -x[6]};
    register __vector double vx3_i = {x[7], x[7]};
#endif

    register __vector double *vy = (__vector double *) y;
    register __vector double *vptr_a0 = (__vector double *) a0;
    register __vector double *vptr_a1 = (__vector double *) a1;
    register __vector double *vptr_a2 = (__vector double *) a2;
    register __vector double *vptr_a3 = (__vector double *) a3;

    for (i = 0; i < n; i += 4) {

        register __vector double vy_0 = vy[i];
        register __vector double vy_1 = vy[i + 1];
        register __vector double vy_2 = vy[i + 2];
        register __vector double vy_3 = vy[i + 3];

        register __vector double va0 = vptr_a0[i];
        register __vector double va0_1 = vptr_a0[i + 1];
        register __vector double va0_2 = vptr_a0[i + 2];
        register __vector double va0_3 = vptr_a0[i + 3];

        register __vector double va1 = vptr_a1[i];
        register __vector double va1_1 = vptr_a1[i + 1];
        register __vector double va1_2 = vptr_a1[i + 2];
        register __vector double va1_3 = vptr_a1[i + 3];

        register __vector double va2 = vptr_a2[i];
        register __vector double va2_1 = vptr_a2[i + 1];
        register __vector double va2_2 = vptr_a2[i + 2];
        register __vector double va2_3 = vptr_a2[i + 3];

        register __vector double va3 = vptr_a3[i];
        register __vector double va3_1 = vptr_a3[i + 1];
        register __vector double va3_2 = vptr_a3[i + 2];
        register __vector double va3_3 = vptr_a3[i + 3];

        vy_0 += va0*vx0_r;
        vy_1 += va0_1*vx0_r;
        vy_2 += va0_2*vx0_r;
        vy_3 += va0_3*vx0_r;

        vy_0 += va1*vx1_r;
        vy_1 += va1_1*vx1_r;
        vy_2 += va1_2*vx1_r;
        vy_3 += va1_3*vx1_r;

        va0 = vec_permi(va0, va0, 2);
        va0_1 = vec_permi(va0_1, va0_1, 2);
        va0_2 = vec_permi(va0_2, va0_2, 2);
        va0_3 = vec_permi(va0_3, va0_3, 2);

        vy_0 += va2*vx2_r;
        vy_1 += va2_1*vx2_r;
        vy_2 += va2_2*vx2_r;
        vy_3 += va2_3*vx2_r;

        va1 = vec_permi(va1, va1, 2);
        va1_1 = vec_permi(va1_1, va1_1, 2);
        va1_2 = vec_permi(va1_2, va1_2, 2);
        va1_3 = vec_permi(va1_3, va1_3, 2);

        vy_0 += va3*vx3_r;
        vy_1 += va3_1*vx3_r;
        vy_2 += va3_2*vx3_r;
        vy_3 += va3_3*vx3_r;

        va2 = vec_permi(va2, va2, 2);
        va2_1 = vec_permi(va2_1, va2_1, 2);
        va2_2 = vec_permi(va2_2, va2_2, 2);
        va2_3 = vec_permi(va2_3, va2_3, 2);

        vy_0 += va0*vx0_i;
        vy_1 += va0_1*vx0_i;
        vy_2 += va0_2*vx0_i;
        vy_3 += va0_3*vx0_i;

        va3 = vec_permi(va3, va3, 2);
        va3_1 = vec_permi(va3_1, va3_1, 2);
        va3_2 = vec_permi(va3_2, va3_2, 2);
        va3_3 = vec_permi(va3_3, va3_3, 2);

        vy_0 += va1*vx1_i;
        vy_1 += va1_1*vx1_i;
        vy_2 += va1_2*vx1_i;
        vy_3 += va1_3*vx1_i;

        vy_0 += va2*vx2_i;
        vy_1 += va2_1*vx2_i;
        vy_2 += va2_2*vx2_i;
        vy_3 += va2_3*vx2_i;

        vy_0 += va3*vx3_i;
        vy_1 += va3_1*vx3_i;
        vy_2 += va3_2*vx3_i;
        vy_3 += va3_3*vx3_i;

        vy[i] = vy_0;
        vy[i + 1] = vy_1;
        vy[i + 2] = vy_2;
        vy[i + 3] = vy_3;

    }
}
#else

static void zgemv_kernel_4x4(BLASLONG n, BLASLONG lda, FLOAT *ap, FLOAT *x, FLOAT *y) {
    BLASLONG i;
    FLOAT *a0, *a1, *a2, *a3;
    a0 = ap;
    a1 = ap + lda;
    a2 = a1 + lda;
    a3 = a2 + lda;

    for (i = 0; i < 2 * n; i += 2) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        y[i] += a0[i] * x[0] - a0[i + 1] * x[1];
        y[i + 1] += a0[i] * x[1] + a0[i + 1] * x[0];
        y[i] += a1[i] * x[2] - a1[i + 1] * x[3];
        y[i + 1] += a1[i] * x[3] + a1[i + 1] * x[2];
        y[i] += a2[i] * x[4] - a2[i + 1] * x[5];
        y[i + 1] += a2[i] * x[5] + a2[i + 1] * x[4];
        y[i] += a3[i] * x[6] - a3[i + 1] * x[7];
        y[i + 1] += a3[i] * x[7] + a3[i + 1] * x[6];
#else 
        y[i] += a0[i] * x[0] + a0[i + 1] * x[1];
        y[i + 1] += a0[i] * x[1] - a0[i + 1] * x[0];
        y[i] += a1[i] * x[2] + a1[i + 1] * x[3];
        y[i + 1] += a1[i] * x[3] - a1[i + 1] * x[2];
        y[i] += a2[i] * x[4] + a2[i + 1] * x[5];
        y[i + 1] += a2[i] * x[5] - a2[i + 1] * x[4];
        y[i] += a3[i] * x[6] + a3[i + 1] * x[7];
        y[i + 1] += a3[i] * x[7] - a3[i + 1] * x[6];
#endif
    }
}

#endif

#ifdef  HAVE_KERNEL_4x2_VEC

static void zgemv_kernel_4x2(BLASLONG n, BLASLONG lda, FLOAT *ap, FLOAT *x, FLOAT *y) {
    BLASLONG i;
    FLOAT *a0, *a1;
    a0 = ap;
    a1 = ap + lda;


#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )

    register __vector double vx0_r = {x[0], x[0]};
    register __vector double vx0_i = {-x[1], x[1]};
    register __vector double vx1_r = {x[2], x[2]};
    register __vector double vx1_i = {-x[3], x[3]};

#else
    register __vector double vx0_r = {x[0], -x[0]};
    register __vector double vx0_i = {x[1], x[1]};
    register __vector double vx1_r = {x[2], -x[2]};
    register __vector double vx1_i = {x[3], x[3]};
#endif


    register __vector double *vy = (__vector double *) y;
    register __vector double *vptr_a0 = (__vector double *) a0;
    register __vector double *vptr_a1 = (__vector double *) a1;

    for (i = 0; i < n; i += 4) {

        register __vector double vy_0 = vy[i];
        register __vector double vy_1 = vy[i + 1];
        register __vector double vy_2 = vy[i + 2];
        register __vector double vy_3 = vy[i + 3];

        register __vector double va0 = vptr_a0[i];
        register __vector double va0_1 = vptr_a0[i + 1];
        register __vector double va0_2 = vptr_a0[i + 2];
        register __vector double va0_3 = vptr_a0[i + 3];

        register __vector double va1 = vptr_a1[i];
        register __vector double va1_1 = vptr_a1[i + 1];
        register __vector double va1_2 = vptr_a1[i + 2];
        register __vector double va1_3 = vptr_a1[i + 3];

        vy_0 += va0*vx0_r;
        vy_1 += va0_1*vx0_r;
        vy_2 += va0_2*vx0_r;
        vy_3 += va0_3*vx0_r;

        va0 = vec_permi(va0, va0, 2);
        va0_1 = vec_permi(va0_1, va0_1, 2);
        va0_2 = vec_permi(va0_2, va0_2, 2);
        va0_3 = vec_permi(va0_3, va0_3, 2);

        vy_0 += va1*vx1_r;
        vy_1 += va1_1*vx1_r;
        vy_2 += va1_2*vx1_r;
        vy_3 += va1_3*vx1_r;

        va1 = vec_permi(va1, va1, 2);
        va1_1 = vec_permi(va1_1, va1_1, 2);
        va1_2 = vec_permi(va1_2, va1_2, 2);
        va1_3 = vec_permi(va1_3, va1_3, 2);

        vy_0 += va0*vx0_i;
        vy_1 += va0_1*vx0_i;
        vy_2 += va0_2*vx0_i;
        vy_3 += va0_3*vx0_i;

        vy_0 += va1*vx1_i;
        vy_1 += va1_1*vx1_i;
        vy_2 += va1_2*vx1_i;
        vy_3 += va1_3*vx1_i;

        vy[i] = vy_0;
        vy[i + 1] = vy_1;
        vy[i + 2] = vy_2;
        vy[i + 3] = vy_3;

    }
}
#else

static void zgemv_kernel_4x2(BLASLONG n, BLASLONG lda, FLOAT *ap, FLOAT *x, FLOAT *y) {
    BLASLONG i;
    FLOAT *a0, *a1;
    a0 = ap;
    a1 = ap + lda;

    for (i = 0; i < 2 * n; i += 2) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        y[i] += a0[i] * x[0] - a0[i + 1] * x[1];
        y[i + 1] += a0[i] * x[1] + a0[i + 1] * x[0];
        y[i] += a1[i] * x[2] - a1[i + 1] * x[3];
        y[i + 1] += a1[i] * x[3] + a1[i + 1] * x[2];
#else 
        y[i] += a0[i] * x[0] + a0[i + 1] * x[1];
        y[i + 1] += a0[i] * x[1] - a0[i + 1] * x[0];
        y[i] += a1[i] * x[2] + a1[i + 1] * x[3];
        y[i + 1] += a1[i] * x[3] - a1[i + 1] * x[2];
#endif
    }
}

#endif

#ifdef  HAVE_KERNEL_4x1_VEC

static void zgemv_kernel_4x1(BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y) {
    BLASLONG i;
    FLOAT *a0;
    a0 = ap;


#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )

    register __vector double vx0_r = {x[0], x[0]};
    register __vector double vx0_i = {-x[1], x[1]};

#else
    register __vector double vx0_r = {x[0], -x[0]};
    register __vector double vx0_i = {x[1], x[1]};
#endif


    register __vector double *vy = (__vector double *) y;
    register __vector double *vptr_a0 = (__vector double *) a0;

    for (i = 0; i < n; i += 4) {

        register __vector double vy_0 = vy[i];
        register __vector double vy_1 = vy[i + 1];
        register __vector double vy_2 = vy[i + 2];
        register __vector double vy_3 = vy[i + 3];

        register __vector double va0 = vptr_a0[i];
        register __vector double va0_1 = vptr_a0[i + 1];
        register __vector double va0_2 = vptr_a0[i + 2];
        register __vector double va0_3 = vptr_a0[i + 3];

        vy_0 += va0*vx0_r;
        vy_1 += va0_1*vx0_r;
        vy_2 += va0_2*vx0_r;
        vy_3 += va0_3*vx0_r;

        va0 = vec_permi(va0, va0, 2);
        va0_1 = vec_permi(va0_1, va0_1, 2);
        va0_2 = vec_permi(va0_2, va0_2, 2);
        va0_3 = vec_permi(va0_3, va0_3, 2);

        vy_0 += va0*vx0_i;
        vy_1 += va0_1*vx0_i;
        vy_2 += va0_2*vx0_i;
        vy_3 += va0_3*vx0_i;

        vy[i] = vy_0;
        vy[i + 1] = vy_1;
        vy[i + 2] = vy_2;
        vy[i + 3] = vy_3;

    }
}

#else

static void zgemv_kernel_4x1(BLASLONG n, FLOAT *ap, FLOAT *x, FLOAT *y) {
    BLASLONG i;
    FLOAT *a0;
    a0 = ap;

    for (i = 0; i < 2 * n; i += 2) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
        y[i] += a0[i] * x[0] - a0[i + 1] * x[1];
        y[i + 1] += a0[i] * x[1] + a0[i + 1] * x[0];
#else 
        y[i] += a0[i] * x[0] + a0[i + 1] * x[1];
        y[i + 1] += a0[i] * x[1] - a0[i + 1] * x[0];
#endif

    }
}

#endif

#ifdef HAVE_KERNEL_ADDY

static void add_y(BLASLONG n, FLOAT *src, FLOAT *dest, BLASLONG inc_dest, FLOAT alpha_r, FLOAT alpha_i) {
    BLASLONG i;


#if   !defined(XCONJ) 

    register __vector double valpha_r = {alpha_r, alpha_r};
    register __vector double valpha_i = {-alpha_i, alpha_i};

#else
    register __vector double valpha_r = {alpha_r, -alpha_r};
    register __vector double valpha_i = {alpha_i, alpha_i};
#endif

    register __vector double *vptr_src = (__vector double *) src;
    if (inc_dest != 2) {
        register __vector double *vptr_y = (__vector double *) dest;
        //note that inc_dest is already 2x. so we should add it to double*
        register __vector double *vptr_y1 = (__vector double *) (dest + inc_dest);
        register __vector double *vptr_y2 = (__vector double *) (dest + 2 * inc_dest);
        register __vector double *vptr_y3 = (__vector double *) (dest + 3 * inc_dest);
        BLASLONG dest_t=0;
        BLASLONG add_dest=inc_dest<<1; //inc_dest is already multiplied by 2, so for vector 4  we just multiply 2 times
        for (i = 0; i < n; i += 4) {

            register __vector double vy_0=vptr_y[dest_t];
            register __vector double vy_1=vptr_y1[dest_t];
            register __vector double vy_2=vptr_y2[dest_t];
            register __vector double vy_3=vptr_y3[dest_t];

            register __vector double vsrc = vptr_src[i];
            register __vector double vsrc_1 = vptr_src[i + 1];
            register __vector double vsrc_2 = vptr_src[i + 2];
            register __vector double vsrc_3 = vptr_src[i + 3];

            vy_0 += vsrc*valpha_r;
            vy_1 += vsrc_1*valpha_r;
            vy_2 += vsrc_2*valpha_r;
            vy_3 += vsrc_3*valpha_r;

            vsrc = vec_permi(vsrc, vsrc, 2);
            vsrc_1 = vec_permi(vsrc_1, vsrc_1, 2);
            vsrc_2 = vec_permi(vsrc_2, vsrc_2, 2);
            vsrc_3 = vec_permi(vsrc_3, vsrc_3, 2);

            vy_0 += vsrc*valpha_i;
            vy_1 += vsrc_1*valpha_i;
            vy_2 += vsrc_2*valpha_i;
            vy_3 += vsrc_3*valpha_i;

            vptr_y[dest_t] = vy_0;
            vptr_y1[dest_t ] = vy_1;
            vptr_y2[dest_t] = vy_2;
            vptr_y3[dest_t] = vy_3;
            
            dest_t+=add_dest;

        }

        return;
    } else {
        register __vector double *vptr_y = (__vector double *) dest;
        for (i = 0; i < n; i += 4) {

            register __vector double vy_0=vptr_y[i];
            register __vector double vy_1=vptr_y[i+1];
            register __vector double vy_2=vptr_y[i+2];
            register __vector double vy_3=vptr_y[i+3];

            register __vector double vsrc = vptr_src[i];
            register __vector double vsrc_1 = vptr_src[i + 1];
            register __vector double vsrc_2 = vptr_src[i + 2];
            register __vector double vsrc_3 = vptr_src[i + 3];

            vy_0 += vsrc*valpha_r;
            vy_1 += vsrc_1*valpha_r;
            vy_2 += vsrc_2*valpha_r;
            vy_3 += vsrc_3*valpha_r;

            vsrc = vec_permi(vsrc, vsrc, 2);
            vsrc_1 = vec_permi(vsrc_1, vsrc_1, 2);
            vsrc_2 = vec_permi(vsrc_2, vsrc_2, 2);
            vsrc_3 = vec_permi(vsrc_3, vsrc_3, 2);

            vy_0 += vsrc*valpha_i;
            vy_1 += vsrc_1*valpha_i;
            vy_2 += vsrc_2*valpha_i;
            vy_3 += vsrc_3*valpha_i;

            vptr_y[i] = vy_0;
            vptr_y[i + 1 ] = vy_1;
            vptr_y[i + 2] = vy_2;
            vptr_y[i + 3] = vy_3;

        }

        return;
    }
    return;
}

#else

static void add_y(BLASLONG n, FLOAT *src, FLOAT *dest, BLASLONG inc_dest, FLOAT alpha_r, FLOAT alpha_i) {
    BLASLONG i;

    if (inc_dest != 2) {

        FLOAT temp_r;
        FLOAT temp_i;
        for (i = 0; i < n; i++) {
#if !defined(XCONJ) 
            temp_r = alpha_r * src[0] - alpha_i * src[1];
            temp_i = alpha_r * src[1] + alpha_i * src[0];
#else
            temp_r = alpha_r * src[0] + alpha_i * src[1];
            temp_i = -alpha_r * src[1] + alpha_i * src[0];
#endif

            *dest += temp_r;
            *(dest + 1) += temp_i;

            src += 2;
            dest += inc_dest;
        }
        return;
    }

    FLOAT temp_r0;
    FLOAT temp_i0;
    FLOAT temp_r1;
    FLOAT temp_i1;
    FLOAT temp_r2;
    FLOAT temp_i2;
    FLOAT temp_r3;
    FLOAT temp_i3;
    for (i = 0; i < n; i += 4) {
#if !defined(XCONJ) 
        temp_r0 = alpha_r * src[0] - alpha_i * src[1];
        temp_i0 = alpha_r * src[1] + alpha_i * src[0];
        temp_r1 = alpha_r * src[2] - alpha_i * src[3];
        temp_i1 = alpha_r * src[3] + alpha_i * src[2];
        temp_r2 = alpha_r * src[4] - alpha_i * src[5];
        temp_i2 = alpha_r * src[5] + alpha_i * src[4];
        temp_r3 = alpha_r * src[6] - alpha_i * src[7];
        temp_i3 = alpha_r * src[7] + alpha_i * src[6];
#else
        temp_r0 = alpha_r * src[0] + alpha_i * src[1];
        temp_i0 = -alpha_r * src[1] + alpha_i * src[0];
        temp_r1 = alpha_r * src[2] + alpha_i * src[3];
        temp_i1 = -alpha_r * src[3] + alpha_i * src[2];
        temp_r2 = alpha_r * src[4] + alpha_i * src[5];
        temp_i2 = -alpha_r * src[5] + alpha_i * src[4];
        temp_r3 = alpha_r * src[6] + alpha_i * src[7];
        temp_i3 = -alpha_r * src[7] + alpha_i * src[6];
#endif

        dest[0] += temp_r0;
        dest[1] += temp_i0;
        dest[2] += temp_r1;
        dest[3] += temp_i1;
        dest[4] += temp_r2;
        dest[5] += temp_i2;
        dest[6] += temp_r3;
        dest[7] += temp_i3;

        src += 8;
        dest += 8;
    }
    return;
}
#endif

    int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha_r, FLOAT alpha_i, FLOAT *a, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT * buffer) {
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

        FLOAT xbuffer[8], *ybuffer;

        if (m < 1) return (0);
        if (n < 1) return (0);

        ybuffer = buffer;

        inc_x *= 2;
        inc_y *= 2;
        lda *= 2;

        n1 = n / 4;
        n2 = n % 4;

        m3 = m % 4;
        m1 = m - (m % 4);
        m2 = (m % NBMAX) - (m % 4);

        y_ptr = y;

        BLASLONG NB = NBMAX;

        while (NB == NBMAX) {

            m1 -= NB;
            if (m1 < 0) {
                if (m2 == 0) break;
                NB = m2;
            }

            a_ptr = a;

            x_ptr = x;
            //zero_y(NB,ybuffer);
            memset(ybuffer, 0, NB * 16);

            if (inc_x == 2) {

                for (i = 0; i < n1; i++) {
                    zgemv_kernel_4x4(NB, lda, a_ptr, x_ptr, ybuffer);

                    a_ptr += lda << 2;
                    x_ptr += 8;
                }

                if (n2 & 2) {
                    zgemv_kernel_4x2(NB, lda, a_ptr, x_ptr, ybuffer);
                    x_ptr += 4;
                    a_ptr += 2 * lda;

                }

                if (n2 & 1) {
                    zgemv_kernel_4x1(NB, a_ptr, x_ptr, ybuffer);
                    x_ptr += 2;
                    a_ptr += lda;

                }
            } else {

                for (i = 0; i < n1; i++) {

                    xbuffer[0] = x_ptr[0];
                    xbuffer[1] = x_ptr[1];
                    x_ptr += inc_x;
                    xbuffer[2] = x_ptr[0];
                    xbuffer[3] = x_ptr[1];
                    x_ptr += inc_x;
                    xbuffer[4] = x_ptr[0];
                    xbuffer[5] = x_ptr[1];
                    x_ptr += inc_x;
                    xbuffer[6] = x_ptr[0];
                    xbuffer[7] = x_ptr[1];
                    x_ptr += inc_x;

                    zgemv_kernel_4x4(NB, lda, a_ptr, xbuffer, ybuffer);

                    a_ptr += lda << 2;
                }

                for (i = 0; i < n2; i++) {
                    xbuffer[0] = x_ptr[0];
                    xbuffer[1] = x_ptr[1];
                    x_ptr += inc_x;
                    zgemv_kernel_4x1(NB, a_ptr, xbuffer, ybuffer);
                    a_ptr += lda;

                }

            }

            add_y(NB, ybuffer, y_ptr, inc_y, alpha_r, alpha_i);
            a += 2 * NB;
            y_ptr += NB * inc_y;
        }

        if (m3 == 0) return (0);

        if (m3 == 1) {
            a_ptr = a;
            x_ptr = x;
            FLOAT temp_r = 0.0;
            FLOAT temp_i = 0.0;

            if (lda == 2 && inc_x == 2) {

                for (i = 0; i < (n & -2); i += 2) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
                    temp_r += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
                    temp_i += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
                    temp_r += a_ptr[2] * x_ptr[2] - a_ptr[3] * x_ptr[3];
                    temp_i += a_ptr[2] * x_ptr[3] + a_ptr[3] * x_ptr[2];
#else
                    temp_r += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
                    temp_i += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
                    temp_r += a_ptr[2] * x_ptr[2] + a_ptr[3] * x_ptr[3];
                    temp_i += a_ptr[2] * x_ptr[3] - a_ptr[3] * x_ptr[2];
#endif

                    a_ptr += 4;
                    x_ptr += 4;
                }

                for (; i < n; i++) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
                    temp_r += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
                    temp_i += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
#else
                    temp_r += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
                    temp_i += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
#endif

                    a_ptr += 2;
                    x_ptr += 2;
                }

            } else {

                for (i = 0; i < n; i++) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
                    temp_r += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
                    temp_i += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
#else
                    temp_r += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
                    temp_i += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
#endif

                    a_ptr += lda;
                    x_ptr += inc_x;
                }

            }
#if !defined(XCONJ) 
            y_ptr[0] += alpha_r * temp_r - alpha_i * temp_i;
            y_ptr[1] += alpha_r * temp_i + alpha_i * temp_r;
#else
            y_ptr[0] += alpha_r * temp_r + alpha_i * temp_i;
            y_ptr[1] -= alpha_r * temp_i - alpha_i * temp_r;
#endif
            return (0);
        }

        if (m3 == 2) {
            a_ptr = a;
            x_ptr = x;
            FLOAT temp_r0 = 0.0;
            FLOAT temp_i0 = 0.0;
            FLOAT temp_r1 = 0.0;
            FLOAT temp_i1 = 0.0;

            if (lda == 4 && inc_x == 2) {

                for (i = 0; i < (n & -2); i += 2) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )

                    temp_r0 += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
                    temp_i0 += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
                    temp_r1 += a_ptr[2] * x_ptr[0] - a_ptr[3] * x_ptr[1];
                    temp_i1 += a_ptr[2] * x_ptr[1] + a_ptr[3] * x_ptr[0];

                    temp_r0 += a_ptr[4] * x_ptr[2] - a_ptr[5] * x_ptr[3];
                    temp_i0 += a_ptr[4] * x_ptr[3] + a_ptr[5] * x_ptr[2];
                    temp_r1 += a_ptr[6] * x_ptr[2] - a_ptr[7] * x_ptr[3];
                    temp_i1 += a_ptr[6] * x_ptr[3] + a_ptr[7] * x_ptr[2];
#else
                    temp_r0 += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
                    temp_i0 += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
                    temp_r1 += a_ptr[2] * x_ptr[0] + a_ptr[3] * x_ptr[1];
                    temp_i1 += a_ptr[2] * x_ptr[1] - a_ptr[3] * x_ptr[0];

                    temp_r0 += a_ptr[4] * x_ptr[2] + a_ptr[5] * x_ptr[3];
                    temp_i0 += a_ptr[4] * x_ptr[3] - a_ptr[5] * x_ptr[2];
                    temp_r1 += a_ptr[6] * x_ptr[2] + a_ptr[7] * x_ptr[3];
                    temp_i1 += a_ptr[6] * x_ptr[3] - a_ptr[7] * x_ptr[2];
#endif

                    a_ptr += 8;
                    x_ptr += 4;
                }

                for (; i < n; i++) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
                    temp_r0 += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
                    temp_i0 += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
                    temp_r1 += a_ptr[2] * x_ptr[0] - a_ptr[3] * x_ptr[1];
                    temp_i1 += a_ptr[2] * x_ptr[1] + a_ptr[3] * x_ptr[0];
#else
                    temp_r0 += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
                    temp_i0 += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
                    temp_r1 += a_ptr[2] * x_ptr[0] + a_ptr[3] * x_ptr[1];
                    temp_i1 += a_ptr[2] * x_ptr[1] - a_ptr[3] * x_ptr[0];
#endif

                    a_ptr += 4;
                    x_ptr += 2;
                }

            } else {

                for (i = 0; i < n; i++) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
                    temp_r0 += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
                    temp_i0 += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
                    temp_r1 += a_ptr[2] * x_ptr[0] - a_ptr[3] * x_ptr[1];
                    temp_i1 += a_ptr[2] * x_ptr[1] + a_ptr[3] * x_ptr[0];
#else
                    temp_r0 += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
                    temp_i0 += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
                    temp_r1 += a_ptr[2] * x_ptr[0] + a_ptr[3] * x_ptr[1];
                    temp_i1 += a_ptr[2] * x_ptr[1] - a_ptr[3] * x_ptr[0];
#endif

                    a_ptr += lda;
                    x_ptr += inc_x;
                }

            }
#if !defined(XCONJ) 
            y_ptr[0] += alpha_r * temp_r0 - alpha_i * temp_i0;
            y_ptr[1] += alpha_r * temp_i0 + alpha_i * temp_r0;
            y_ptr += inc_y;
            y_ptr[0] += alpha_r * temp_r1 - alpha_i * temp_i1;
            y_ptr[1] += alpha_r * temp_i1 + alpha_i * temp_r1;
#else
            y_ptr[0] += alpha_r * temp_r0 + alpha_i * temp_i0;
            y_ptr[1] -= alpha_r * temp_i0 - alpha_i * temp_r0;
            y_ptr += inc_y;
            y_ptr[0] += alpha_r * temp_r1 + alpha_i * temp_i1;
            y_ptr[1] -= alpha_r * temp_i1 - alpha_i * temp_r1;
#endif
            return (0);
        }

        if (m3 == 3) {
            a_ptr = a;
            x_ptr = x;
            FLOAT temp_r0 = 0.0;
            FLOAT temp_i0 = 0.0;
            FLOAT temp_r1 = 0.0;
            FLOAT temp_i1 = 0.0;
            FLOAT temp_r2 = 0.0;
            FLOAT temp_i2 = 0.0;

            if (lda == 6 && inc_x == 2) {

                for (i = 0; i < n; i++) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
                    temp_r0 += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
                    temp_i0 += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
                    temp_r1 += a_ptr[2] * x_ptr[0] - a_ptr[3] * x_ptr[1];
                    temp_i1 += a_ptr[2] * x_ptr[1] + a_ptr[3] * x_ptr[0];
                    temp_r2 += a_ptr[4] * x_ptr[0] - a_ptr[5] * x_ptr[1];
                    temp_i2 += a_ptr[4] * x_ptr[1] + a_ptr[5] * x_ptr[0];
#else
                    temp_r0 += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
                    temp_i0 += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
                    temp_r1 += a_ptr[2] * x_ptr[0] + a_ptr[3] * x_ptr[1];
                    temp_i1 += a_ptr[2] * x_ptr[1] - a_ptr[3] * x_ptr[0];
                    temp_r2 += a_ptr[4] * x_ptr[0] + a_ptr[5] * x_ptr[1];
                    temp_i2 += a_ptr[4] * x_ptr[1] - a_ptr[5] * x_ptr[0];
#endif

                    a_ptr += 6;
                    x_ptr += 2;
                }

            } else {

                for (i = 0; i < n; i++) {
#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
                    temp_r0 += a_ptr[0] * x_ptr[0] - a_ptr[1] * x_ptr[1];
                    temp_i0 += a_ptr[0] * x_ptr[1] + a_ptr[1] * x_ptr[0];
                    temp_r1 += a_ptr[2] * x_ptr[0] - a_ptr[3] * x_ptr[1];
                    temp_i1 += a_ptr[2] * x_ptr[1] + a_ptr[3] * x_ptr[0];
                    temp_r2 += a_ptr[4] * x_ptr[0] - a_ptr[5] * x_ptr[1];
                    temp_i2 += a_ptr[4] * x_ptr[1] + a_ptr[5] * x_ptr[0];
#else
                    temp_r0 += a_ptr[0] * x_ptr[0] + a_ptr[1] * x_ptr[1];
                    temp_i0 += a_ptr[0] * x_ptr[1] - a_ptr[1] * x_ptr[0];
                    temp_r1 += a_ptr[2] * x_ptr[0] + a_ptr[3] * x_ptr[1];
                    temp_i1 += a_ptr[2] * x_ptr[1] - a_ptr[3] * x_ptr[0];
                    temp_r2 += a_ptr[4] * x_ptr[0] + a_ptr[5] * x_ptr[1];
                    temp_i2 += a_ptr[4] * x_ptr[1] - a_ptr[5] * x_ptr[0];
#endif

                    a_ptr += lda;
                    x_ptr += inc_x;
                }

            }
#if !defined(XCONJ) 
            y_ptr[0] += alpha_r * temp_r0 - alpha_i * temp_i0;
            y_ptr[1] += alpha_r * temp_i0 + alpha_i * temp_r0;
            y_ptr += inc_y;
            y_ptr[0] += alpha_r * temp_r1 - alpha_i * temp_i1;
            y_ptr[1] += alpha_r * temp_i1 + alpha_i * temp_r1;
            y_ptr += inc_y;
            y_ptr[0] += alpha_r * temp_r2 - alpha_i * temp_i2;
            y_ptr[1] += alpha_r * temp_i2 + alpha_i * temp_r2;
#else
            y_ptr[0] += alpha_r * temp_r0 + alpha_i * temp_i0;
            y_ptr[1] -= alpha_r * temp_i0 - alpha_i * temp_r0;
            y_ptr += inc_y;
            y_ptr[0] += alpha_r * temp_r1 + alpha_i * temp_i1;
            y_ptr[1] -= alpha_r * temp_i1 - alpha_i * temp_r1;
            y_ptr += inc_y;
            y_ptr[0] += alpha_r * temp_r2 + alpha_i * temp_i2;
            y_ptr[1] -= alpha_r * temp_i2 - alpha_i * temp_r2;
#endif
            return (0);
        }

        return (0);
    }

