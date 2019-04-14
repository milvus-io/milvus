/*******************************************************************************
Copyright (c) 2016, The OpenBLAS Project
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
*******************************************************************************/

#include "common.h"
#include "macros_msa.h"

#undef OP0
#undef OP1
#undef OP2

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
    #define OP0  -=
    #define OP1  +=
    #define OP2  +=
#else
    #define OP0  +=
    #define OP1  +=
    #define OP2  -=
#endif

#define CGEMV_T_8x4()                        \
    LD_SP4(pa0 + k, 4, t0, t1, t2, t3);      \
    LD_SP4(pa1 + k, 4, t4, t5, t6, t7);      \
    LD_SP4(pa2 + k, 4, t8, t9, t10, t11);    \
    LD_SP4(pa3 + k, 4, t12, t13, t14, t15);  \
                                             \
    PCKEVOD_W2_SP(t1, t0, src0r, src0i);     \
    PCKEVOD_W2_SP(t3, t2, src1r, src1i);     \
    PCKEVOD_W2_SP(t5, t4, src2r, src2i);     \
    PCKEVOD_W2_SP(t7, t6, src3r, src3i);     \
    PCKEVOD_W2_SP(t9, t8, src4r, src4i);     \
    PCKEVOD_W2_SP(t11, t10, src5r, src5i);   \
    PCKEVOD_W2_SP(t13, t12, src6r, src6i);   \
    PCKEVOD_W2_SP(t15, t14, src7r, src7i);   \
                                             \
    tp0r += src0r * x0r;                     \
    tp0r += src1r * x1r;                     \
    tp0r OP0 src0i * x0i;                    \
    tp0r OP0 src1i * x1i;                    \
                                             \
    tp1r += src2r * x0r;                     \
    tp1r += src3r * x1r;                     \
    tp1r OP0 src2i * x0i;                    \
    tp1r OP0 src3i * x1i;                    \
                                             \
    tp2r += src4r * x0r;                     \
    tp2r += src5r * x1r;                     \
    tp2r OP0 src4i * x0i;                    \
    tp2r OP0 src5i * x1i;                    \
                                             \
    tp3r += src6r * x0r;                     \
    tp3r += src7r * x1r;                     \
    tp3r OP0 src6i * x0i;                    \
    tp3r OP0 src7i * x1i;                    \
                                             \
    tp0i OP1 src0r * x0i;                    \
    tp0i OP1 src1r * x1i;                    \
    tp0i OP2 src0i * x0r;                    \
    tp0i OP2 src1i * x1r;                    \
                                             \
    tp1i OP1 src2r * x0i;                    \
    tp1i OP1 src3r * x1i;                    \
    tp1i OP2 src2i * x0r;                    \
    tp1i OP2 src3i * x1r;                    \
                                             \
    tp2i OP1 src4r * x0i;                    \
    tp2i OP1 src5r * x1i;                    \
    tp2i OP2 src4i * x0r;                    \
    tp2i OP2 src5i * x1r;                    \
                                             \
    tp3i OP1 src6r * x0i;                    \
    tp3i OP1 src7r * x1i;                    \
    tp3i OP2 src6i * x0r;                    \
    tp3i OP2 src7i * x1r;                    \

#define CGEMV_T_8x2()                     \
    LD_SP4(pa0 + k, 4, t0, t1, t2, t3);   \
    LD_SP4(pa1 + k, 4, t4, t5, t6, t7);   \
                                          \
    PCKEVOD_W2_SP(t1, t0, src0r, src0i);  \
    PCKEVOD_W2_SP(t3, t2, src1r, src1i);  \
    PCKEVOD_W2_SP(t5, t4, src2r, src2i);  \
    PCKEVOD_W2_SP(t7, t6, src3r, src3i);  \
                                          \
    tp0r += src0r * x0r;                  \
    tp0r += src1r * x1r;                  \
    tp0r OP0 src0i * x0i;                 \
    tp0r OP0 src1i * x1i;                 \
                                          \
    tp1r += src2r * x0r;                  \
    tp1r += src3r * x1r;                  \
    tp1r OP0 src2i * x0i;                 \
    tp1r OP0 src3i * x1i;                 \
                                          \
    tp0i OP1 src0r * x0i;                 \
    tp0i OP1 src1r * x1i;                 \
    tp0i OP2 src0i * x0r;                 \
    tp0i OP2 src1i * x1r;                 \
                                          \
    tp1i OP1 src2r * x0i;                 \
    tp1i OP1 src3r * x1i;                 \
    tp1i OP2 src2i * x0r;                 \
    tp1i OP2 src3i * x1r;                 \

#define CGEMV_T_8x1()                     \
    LD_SP4(pa0 + k, 4, t0, t1, t2, t3);   \
                                          \
    PCKEVOD_W2_SP(t1, t0, src0r, src0i);  \
    PCKEVOD_W2_SP(t3, t2, src1r, src1i);  \
                                          \
    tp0r += src0r * x0r;                  \
    tp0r += src1r * x1r;                  \
    tp0r OP0 src0i * x0i;                 \
    tp0r OP0 src1i * x1i;                 \
                                          \
    tp0i OP1 src0r * x0i;                 \
    tp0i OP1 src1r * x1i;                 \
    tp0i OP2 src0i * x0r;                 \
    tp0i OP2 src1i * x1r;                 \

#define CGEMV_T_4x4()                       \
    LD_SP2(pa0 + k, 4, t0, t1);             \
    LD_SP2(pa1 + k, 4, t4, t5);             \
    LD_SP2(pa2 + k, 4, t8, t9);             \
    LD_SP2(pa3 + k, 4, t12, t13);           \
                                            \
    PCKEVOD_W2_SP(t1, t0, src0r, src0i);    \
    PCKEVOD_W2_SP(t5, t4, src2r, src2i);    \
    PCKEVOD_W2_SP(t9, t8, src4r, src4i);    \
    PCKEVOD_W2_SP(t13, t12, src6r, src6i);  \
                                            \
    tp0r += src0r * x0r;                    \
    tp0r OP0 src0i * x0i;                   \
                                            \
    tp1r += src2r * x0r;                    \
    tp1r OP0 src2i * x0i;                   \
                                            \
    tp2r += src4r * x0r;                    \
    tp2r OP0 src4i * x0i;                   \
                                            \
    tp3r += src6r * x0r;                    \
    tp3r OP0 src6i * x0i;                   \
                                            \
    tp0i OP1 src0r * x0i;                   \
    tp0i OP2 src0i * x0r;                   \
                                            \
    tp1i OP1 src2r * x0i;                   \
    tp1i OP2 src2i * x0r;                   \
                                            \
    tp2i OP1 src4r * x0i;                   \
    tp2i OP2 src4i * x0r;                   \
                                            \
    tp3i OP1 src6r * x0i;                   \
    tp3i OP2 src6i * x0r;                   \

#define CGEMV_T_4x2()                     \
    LD_SP2(pa0 + k, 4, t0, t1);           \
    LD_SP2(pa1 + k, 4, t4, t5);           \
                                          \
    PCKEVOD_W2_SP(t1, t0, src0r, src0i);  \
    PCKEVOD_W2_SP(t5, t4, src2r, src2i);  \
                                          \
    tp0r += src0r * x0r;                  \
    tp0r OP0 src0i * x0i;                 \
                                          \
    tp1r += src2r * x0r;                  \
    tp1r OP0 src2i * x0i;                 \
                                          \
    tp0i OP1 src0r * x0i;                 \
    tp0i OP2 src0i * x0r;                 \
                                          \
    tp1i OP1 src2r * x0i;                 \
    tp1i OP2 src2i * x0r;                 \

#define CGEMV_T_4x1()                     \
    LD_SP2(pa0 + k, 4, t0, t1);           \
                                          \
    PCKEVOD_W2_SP(t1, t0, src0r, src0i);  \
                                          \
    tp0r += src0r * x0r;                  \
    tp0r OP0 src0i * x0i;                 \
                                          \
    tp0i OP1 src0r * x0i;                 \
    tp0i OP2 src0i * x0r;                 \

#define CGEMV_T_1x4()                           \
    temp0r  += pa0[k + 0] * x[0 * inc_x2];      \
    temp0r OP0 pa0[k + 1] * x[0 * inc_x2 + 1];  \
    temp1r  += pa1[k + 0] * x[0 * inc_x2];      \
    temp1r OP0 pa1[k + 1] * x[0 * inc_x2 + 1];  \
    temp2r  += pa2[k + 0] * x[0 * inc_x2];      \
    temp2r OP0 pa2[k + 1] * x[0 * inc_x2 + 1];  \
    temp3r  += pa3[k + 0] * x[0 * inc_x2];      \
    temp3r OP0 pa3[k + 1] * x[0 * inc_x2 + 1];  \
                                                \
    temp0i OP1 pa0[k + 0] * x[0 * inc_x2 + 1];  \
    temp0i OP2 pa0[k + 1] * x[0 * inc_x2];      \
    temp1i OP1 pa1[k + 0] * x[0 * inc_x2 + 1];  \
    temp1i OP2 pa1[k + 1] * x[0 * inc_x2];      \
    temp2i OP1 pa2[k + 0] * x[0 * inc_x2 + 1];  \
    temp2i OP2 pa2[k + 1] * x[0 * inc_x2];      \
    temp3i OP1 pa3[k + 0] * x[0 * inc_x2 + 1];  \
    temp3i OP2 pa3[k + 1] * x[0 * inc_x2];      \

#define CGEMV_T_1x2()                           \
    temp0r  += pa0[k + 0] * x[0 * inc_x2];      \
    temp0r OP0 pa0[k + 1] * x[0 * inc_x2 + 1];  \
    temp1r  += pa1[k + 0] * x[0 * inc_x2];      \
    temp1r OP0 pa1[k + 1] * x[0 * inc_x2 + 1];  \
                                                \
    temp0i OP1 pa0[k + 0] * x[0 * inc_x2 + 1];  \
    temp0i OP2 pa0[k + 1] * x[0 * inc_x2];      \
    temp1i OP1 pa1[k + 0] * x[0 * inc_x2 + 1];  \
    temp1i OP2 pa1[k + 1] * x[0 * inc_x2];      \

#define CGEMV_T_1x1()                           \
    temp0r  += pa0[k + 0] * x[0 * inc_x2];      \
    temp0r OP0 pa0[k + 1] * x[0 * inc_x2 + 1];  \
                                                \
    temp0i OP1 pa0[k + 0] * x[0 * inc_x2 + 1];  \
    temp0i OP2 pa0[k + 1] * x[0 * inc_x2];      \

#define CSCALE_STORE_Y4_GP()    \
    res0r = y[0 * inc_y2];      \
    res1r = y[1 * inc_y2];      \
    res2r = y[2 * inc_y2];      \
    res3r = y[3 * inc_y2];      \
                                \
    res0i = y[0 * inc_y2 + 1];  \
    res1i = y[1 * inc_y2 + 1];  \
    res2i = y[2 * inc_y2 + 1];  \
    res3i = y[3 * inc_y2 + 1];  \
                                \
    res0r  += alphar * temp0r;  \
    res0r OP0 alphai * temp0i;  \
    res1r  += alphar * temp1r;  \
    res1r OP0 alphai * temp1i;  \
    res2r  += alphar * temp2r;  \
    res2r OP0 alphai * temp2i;  \
    res3r  += alphar * temp3r;  \
    res3r OP0 alphai * temp3i;  \
                                \
    res0i OP1 alphar * temp0i;  \
    res0i OP2 alphai * temp0r;  \
    res1i OP1 alphar * temp1i;  \
    res1i OP2 alphai * temp1r;  \
    res2i OP1 alphar * temp2i;  \
    res2i OP2 alphai * temp2r;  \
    res3i OP1 alphar * temp3i;  \
    res3i OP2 alphai * temp3r;  \
                                \
    y[0 * inc_y2] = res0r;      \
    y[1 * inc_y2] = res1r;      \
    y[2 * inc_y2] = res2r;      \
    y[3 * inc_y2] = res3r;      \
                                \
    y[0 * inc_y2 + 1] = res0i;  \
    y[1 * inc_y2 + 1] = res1i;  \
    y[2 * inc_y2 + 1] = res2i;  \
    y[3 * inc_y2 + 1] = res3i;  \

#define CSCALE_STORE_Y2_GP()    \
    res0r = y[0 * inc_y2];      \
    res1r = y[1 * inc_y2];      \
                                \
    res0i = y[0 * inc_y2 + 1];  \
    res1i = y[1 * inc_y2 + 1];  \
                                \
    res0r  += alphar * temp0r;  \
    res0r OP0 alphai * temp0i;  \
    res1r  += alphar * temp1r;  \
    res1r OP0 alphai * temp1i;  \
                                \
    res0i OP1 alphar * temp0i;  \
    res0i OP2 alphai * temp0r;  \
    res1i OP1 alphar * temp1i;  \
    res1i OP2 alphai * temp1r;  \
                                \
    y[0 * inc_y2] = res0r;      \
    y[1 * inc_y2] = res1r;      \
                                \
    y[0 * inc_y2 + 1] = res0i;  \
    y[1 * inc_y2 + 1] = res1i;  \


#define CSCALE_STORE_Y1_GP()    \
    res0r = y[0 * inc_y2];      \
    res0i = y[0 * inc_y2 + 1];  \
                                \
    res0r  += alphar * temp0r;  \
    res0r OP0 alphai * temp0i;  \
                                \
    res0i OP1 alphar * temp0i;  \
    res0i OP2 alphai * temp0r;  \
                                \
    y[0 * inc_y2] = res0r;      \
    y[0 * inc_y2 + 1] = res0i;  \

#define CLOAD_X8_VECTOR()             \
    LD_SP4(x, 4, x0, x1, x2, x3);     \
    PCKEVOD_W2_SP(x1, x0, x0r, x0i);  \
    PCKEVOD_W2_SP(x3, x2, x1r, x1i);  \

#define CLOAD_X4_VECTOR()             \
    LD_SP2(x, 4, x0, x1);             \
    PCKEVOD_W2_SP(x1, x0, x0r, x0i);  \

#define CLOAD_X8_GP()                                                                \
    x0r = (v4f32) __msa_insert_w((v4i32) tp0r, 0, *((int *) (x + 0 * inc_x2)));      \
    x0r = (v4f32) __msa_insert_w((v4i32) x0r,  1, *((int *) (x + 1 * inc_x2)));      \
    x0r = (v4f32) __msa_insert_w((v4i32) x0r,  2, *((int *) (x + 2 * inc_x2)));      \
    x0r = (v4f32) __msa_insert_w((v4i32) x0r,  3, *((int *) (x + 3 * inc_x2)));      \
    x1r = (v4f32) __msa_insert_w((v4i32) tp0r, 0, *((int *) (x + 4 * inc_x2)));      \
    x1r = (v4f32) __msa_insert_w((v4i32) x1r,  1, *((int *) (x + 5 * inc_x2)));      \
    x1r = (v4f32) __msa_insert_w((v4i32) x1r,  2, *((int *) (x + 6 * inc_x2)));      \
    x1r = (v4f32) __msa_insert_w((v4i32) x1r,  3, *((int *) (x + 7 * inc_x2)));      \
    x0i = (v4f32) __msa_insert_w((v4i32) tp0r, 0, *((int *) (x + 0 * inc_x2 + 1)));  \
    x0i = (v4f32) __msa_insert_w((v4i32) x0i,  1, *((int *) (x + 1 * inc_x2 + 1)));  \
    x0i = (v4f32) __msa_insert_w((v4i32) x0i,  2, *((int *) (x + 2 * inc_x2 + 1)));  \
    x0i = (v4f32) __msa_insert_w((v4i32) x0i,  3, *((int *) (x + 3 * inc_x2 + 1)));  \
    x1i = (v4f32) __msa_insert_w((v4i32) tp0r, 0, *((int *) (x + 4 * inc_x2 + 1)));  \
    x1i = (v4f32) __msa_insert_w((v4i32) x1i,  1, *((int *) (x + 5 * inc_x2 + 1)));  \
    x1i = (v4f32) __msa_insert_w((v4i32) x1i,  2, *((int *) (x + 6 * inc_x2 + 1)));  \
    x1i = (v4f32) __msa_insert_w((v4i32) x1i,  3, *((int *) (x + 7 * inc_x2 + 1)));  \

#define CLOAD_X4_GP()                                                                \
    x0r = (v4f32) __msa_insert_w((v4i32) tp0r, 0, *((int *) (x + 0 * inc_x2)));      \
    x0r = (v4f32) __msa_insert_w((v4i32) x0r,  1, *((int *) (x + 1 * inc_x2)));      \
    x0r = (v4f32) __msa_insert_w((v4i32) x0r,  2, *((int *) (x + 2 * inc_x2)));      \
    x0r = (v4f32) __msa_insert_w((v4i32) x0r,  3, *((int *) (x + 3 * inc_x2)));      \
    x0i = (v4f32) __msa_insert_w((v4i32) tp0r, 0, *((int *) (x + 0 * inc_x2 + 1)));  \
    x0i = (v4f32) __msa_insert_w((v4i32) x0i,  1, *((int *) (x + 1 * inc_x2 + 1)));  \
    x0i = (v4f32) __msa_insert_w((v4i32) x0i,  2, *((int *) (x + 2 * inc_x2 + 1)));  \
    x0i = (v4f32) __msa_insert_w((v4i32) x0i,  3, *((int *) (x + 3 * inc_x2 + 1)));  \

#define CGEMV_T_MSA()                                \
    for (j = (n >> 2); j--;)                         \
    {                                                \
        tp0r = tp1r = tp2r = tp3r = zero;            \
        tp0i = tp1i = tp2i = tp3i = zero;            \
                                                     \
        k = 0;                                       \
        k_pref = pref_offset;                        \
        x = srcx_org;                                \
                                                     \
        for (i = (m >> 3); i--;)                     \
        {                                            \
            PREFETCH(pa0 + k_pref + 16 + 0);         \
            PREFETCH(pa0 + k_pref + 16 + 8);         \
            PREFETCH(pa1 + k_pref + 16 + 0);         \
            PREFETCH(pa1 + k_pref + 16 + 8);         \
            PREFETCH(pa2 + k_pref + 16 + 0);         \
            PREFETCH(pa2 + k_pref + 16 + 8);         \
            PREFETCH(pa3 + k_pref + 16 + 0);         \
            PREFETCH(pa3 + k_pref + 16 + 8);         \
                                                     \
            CLOAD_X8()                               \
            CGEMV_T_8x4();                           \
                                                     \
            k += 2 * 8;                              \
            k_pref += 2 * 8;                         \
            x += inc_x2 * 8;                         \
        }                                            \
                                                     \
        if (m & 4)                                   \
        {                                            \
            CLOAD_X4();                              \
                                                     \
            CGEMV_T_4x4();                           \
                                                     \
            k += 2 * 4;                              \
            x += inc_x2 * 4;                         \
        }                                            \
                                                     \
        TRANSPOSE4x4_SP_SP(tp0r, tp1r, tp2r, tp3r,   \
                           tp0r, tp1r, tp2r, tp3r);  \
        TRANSPOSE4x4_SP_SP(tp0i, tp1i, tp2i, tp3i,   \
                           tp0i, tp1i, tp2i, tp3i);  \
                                                     \
        tp0r += tp1r;                                \
        tp0r += tp2r;                                \
        tp0r += tp3r;                                \
        tp0i += tp1i;                                \
        tp0i += tp2i;                                \
        tp0i += tp3i;                                \
                                                     \
        temp0r = tp0r[0];                            \
        temp1r = tp0r[1];                            \
        temp2r = tp0r[2];                            \
        temp3r = tp0r[3];                            \
        temp0i = tp0i[0];                            \
        temp1i = tp0i[1];                            \
        temp2i = tp0i[2];                            \
        temp3i = tp0i[3];                            \
                                                     \
        for (i = (m & 3); i--;)                      \
        {                                            \
            CGEMV_T_1x4();                           \
                                                     \
            k += 2;                                  \
            x += inc_x2;                             \
        }                                            \
                                                     \
        CSCALE_STORE_Y4_GP();                        \
                                                     \
        pa0 += 4 * lda2;                             \
        pa1 += 4 * lda2;                             \
        pa2 += 4 * lda2;                             \
        pa3 += 4 * lda2;                             \
        y += 4 * inc_y2;                             \
    }                                                \
                                                     \
    if (n & 2)                                       \
    {                                                \
        tp0r = tp1r = zero;                          \
        tp0i = tp1i = zero;                          \
                                                     \
        k = 0;                                       \
        x = srcx_org;                                \
                                                     \
        for (i = (m >> 3); i--;)                     \
        {                                            \
            CLOAD_X8();                              \
                                                     \
            CGEMV_T_8x2();                           \
                                                     \
            k += 2 * 8;                              \
            x += inc_x2 * 8;                         \
        }                                            \
                                                     \
        if (m & 4)                                   \
        {                                            \
            CLOAD_X4();                              \
                                                     \
            CGEMV_T_4x2();                           \
                                                     \
            k += 2 * 4;                              \
            x += inc_x2 * 4;                         \
        }                                            \
                                                     \
        TRANSPOSE4x4_SP_SP(tp0r, tp1r, tp0i, tp1i,   \
                           tp0r, tp1r, tp0i, tp1i);  \
                                                     \
        tp0r += tp1r;                                \
        tp0r += tp0i;                                \
        tp0r += tp1i;                                \
                                                     \
        temp0r = tp0r[0];                            \
        temp1r = tp0r[1];                            \
        temp0i = tp0r[2];                            \
        temp1i = tp0r[3];                            \
                                                     \
        for (i = (m & 3); i--;)                      \
        {                                            \
            CGEMV_T_1x2();                           \
                                                     \
            k += 2;                                  \
            x += inc_x2;                             \
        }                                            \
                                                     \
        CSCALE_STORE_Y2_GP();                        \
                                                     \
        pa0 += 2 * lda2;                             \
        pa1 += 2 * lda2;                             \
        y += 2 * inc_y2;                             \
    }                                                \
                                                     \
    if (n & 1)                                       \
    {                                                \
        tp0r = zero;                                 \
        tp0i = zero;                                 \
                                                     \
        k = 0;                                       \
        x = srcx_org;                                \
                                                     \
        for (i = (m >> 3); i--;)                     \
        {                                            \
            CLOAD_X8();                              \
                                                     \
            CGEMV_T_8x1();                           \
                                                     \
            k += 2 * 8;                              \
            x += inc_x2 * 8;                         \
        }                                            \
                                                     \
        if (m & 4)                                   \
        {                                            \
            CLOAD_X4();                              \
                                                     \
            CGEMV_T_4x1();                           \
                                                     \
            k += 2 * 4;                              \
            x += inc_x2 * 4;                         \
        }                                            \
                                                     \
        ILVRL_W2_SP(tp0i, tp0r, t0, t1);             \
                                                     \
        t0 += t1;                                    \
                                                     \
        temp0r = t0[0] + t0[2];                      \
        temp0i = t0[1] + t0[3];                      \
                                                     \
        for (i = (m & 3); i--;)                      \
        {                                            \
            CGEMV_T_1x1();                           \
                                                     \
            k += 2;                                  \
            x += inc_x2;                             \
        }                                            \
                                                     \
        CSCALE_STORE_Y1_GP();                        \
                                                     \
        pa0 += lda2;                                 \
        y += inc_y2;                                 \
    }                                                \

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alphar, FLOAT alphai,
          FLOAT *A, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y,
          BLASLONG inc_y, FLOAT *buffer)
{
    BLASLONG i, j, k, k_pref, pref_offset;
    FLOAT *pa0, *pa1, *pa2, *pa3;
    FLOAT *srcx_org = x;
    FLOAT temp0r, temp0i, temp2r, temp2i, temp1r, temp1i, temp3r, temp3i;
    FLOAT res0r, res0i, res2r, res2i, res1r, res1i, res3r, res3i;
    BLASLONG inc_x2, inc_y2, lda2;
    v4f32 zero = {0};
    v4f32 x0, x1, x2, x3, x0r, x1r, x0i, x1i;
    v4f32 t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15;
    v4f32 src0r, src1r, src2r, src3r, src4r, src5r, src6r, src7r;
    v4f32 src0i, src1i, src2i, src3i, src4i, src5i, src6i, src7i;
    v4f32 tp0r, tp1r, tp2r, tp3r, tp0i, tp1i, tp2i, tp3i;

    lda2 = 2 * lda;

    pref_offset = (uintptr_t)A & (L1_DATA_LINESIZE - 1);
    pref_offset = L1_DATA_LINESIZE - pref_offset;
    pref_offset = pref_offset / sizeof(FLOAT);

    pa0 = A;
    pa1 = A + lda2;
    pa2 = A + 2 * lda2;
    pa3 = A + 3 * lda2;

    inc_x2 = 2 * inc_x;
    inc_y2 = 2 * inc_y;

    if (2 == inc_x2)
    {
        #define CLOAD_X8  CLOAD_X8_VECTOR
        #define CLOAD_X4  CLOAD_X4_VECTOR

        CGEMV_T_MSA();

        #undef CLOAD_X8
        #undef CLOAD_X4
    }
    else
    {
        #define CLOAD_X8  CLOAD_X8_GP
        #define CLOAD_X4  CLOAD_X4_GP

        CGEMV_T_MSA();

        #undef CLOAD_X8
        #undef CLOAD_X4
    }

    return(0);
}

#undef OP0
#undef OP1
#undef OP2
