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
#undef OP3
#undef OP4

#if !defined(XCONJ)
    #define OP3  -=
    #define OP4  +=
#else
    #define OP3  +=
    #define OP4  -=
#endif

#if !defined(CONJ)
    #if !defined(XCONJ)
        #define OP0  -=
        #define OP1  +=
        #define OP2  +=
    #else
        #define OP0  +=
        #define OP1  +=
        #define OP2  -=
    #endif
#else
    #if !defined(XCONJ)
        #define OP0  +=
        #define OP1  -=
        #define OP2  -=
    #else
        #define OP0  -=
        #define OP1  -=
        #define OP2  +=
    #endif
#endif

#define ZGEMV_N_4x4()                        \
    LD_DP4(pa0 + k, 2, t0, t1, t2, t3);      \
    LD_DP4(pa1 + k, 2, t4, t5, t6, t7);      \
    LD_DP4(pa2 + k, 2, t8, t9, t10, t11);    \
    LD_DP4(pa3 + k, 2, t12, t13, t14, t15);  \
                                             \
    PCKEVOD_D2_DP(t1, t0, src0r, src0i);     \
    PCKEVOD_D2_DP(t3, t2, src1r, src1i);     \
    PCKEVOD_D2_DP(t5, t4, src2r, src2i);     \
    PCKEVOD_D2_DP(t7, t6, src3r, src3i);     \
    PCKEVOD_D2_DP(t9, t8, src4r, src4i);     \
    PCKEVOD_D2_DP(t11, t10, src5r, src5i);   \
    PCKEVOD_D2_DP(t13, t12, src6r, src6i);   \
    PCKEVOD_D2_DP(t15, t14, src7r, src7i);   \
                                             \
    y0r += tp0r * src0r;                     \
    y1r += tp0r * src1r;                     \
    y0r += tp1r * src2r;                     \
    y1r += tp1r * src3r;                     \
    y0r += tp2r * src4r;                     \
    y1r += tp2r * src5r;                     \
    y0r += tp3r * src6r;                     \
    y1r += tp3r * src7r;                     \
                                             \
    y0r OP0 tp0i * src0i;                    \
    y1r OP0 tp0i * src1i;                    \
    y0r OP0 tp1i * src2i;                    \
    y1r OP0 tp1i * src3i;                    \
    y0r OP0 tp2i * src4i;                    \
    y1r OP0 tp2i * src5i;                    \
    y0r OP0 tp3i * src6i;                    \
    y1r OP0 tp3i * src7i;                    \
                                             \
    y0i OP1 tp0r * src0i;                    \
    y1i OP1 tp0r * src1i;                    \
    y0i OP1 tp1r * src2i;                    \
    y1i OP1 tp1r * src3i;                    \
    y0i OP1 tp2r * src4i;                    \
    y1i OP1 tp2r * src5i;                    \
    y0i OP1 tp3r * src6i;                    \
    y1i OP1 tp3r * src7i;                    \
                                             \
    y0i OP2 tp0i * src0r;                    \
    y1i OP2 tp0i * src1r;                    \
    y0i OP2 tp1i * src2r;                    \
    y1i OP2 tp1i * src3r;                    \
    y0i OP2 tp2i * src4r;                    \
    y1i OP2 tp2i * src5r;                    \
    y0i OP2 tp3i * src6r;                    \
    y1i OP2 tp3i * src7r;                    \

#define ZGEMV_N_2x4()                       \
    LD_DP2(pa0 + k, 2, t0, t1);             \
    LD_DP2(pa1 + k, 2, t4, t5);             \
    LD_DP2(pa2 + k, 2, t8, t9);             \
    LD_DP2(pa3 + k, 2, t12, t13);           \
                                            \
    PCKEVOD_D2_DP(t1, t0, src0r, src0i);    \
    PCKEVOD_D2_DP(t5, t4, src2r, src2i);    \
    PCKEVOD_D2_DP(t9, t8, src4r, src4i);    \
    PCKEVOD_D2_DP(t13, t12, src6r, src6i);  \
                                            \
    y0r += tp0r * src0r;                    \
    y0r += tp1r * src2r;                    \
    y0r += tp2r * src4r;                    \
    y0r += tp3r * src6r;                    \
                                            \
    y0r OP0 tp0i * src0i;                   \
    y0r OP0 tp1i * src2i;                   \
    y0r OP0 tp2i * src4i;                   \
    y0r OP0 tp3i * src6i;                   \
                                            \
    y0i OP1 tp0r * src0i;                   \
    y0i OP1 tp1r * src2i;                   \
    y0i OP1 tp2r * src4i;                   \
    y0i OP1 tp3r * src6i;                   \
                                            \
    y0i OP2 tp0i * src0r;                   \
    y0i OP2 tp1i * src2r;                   \
    y0i OP2 tp2i * src4r;                   \
    y0i OP2 tp3i * src6r;                   \

#define ZGEMV_N_1x4()               \
    res0 = y[0 * inc_y2];           \
    res1 = y[0 * inc_y2 + 1];       \
                                    \
    res0  += temp0_r * pa0[k];      \
    res0 OP0 temp0_i * pa0[k + 1];  \
    res0  += temp1_r * pa1[k];      \
    res0 OP0 temp1_i * pa1[k + 1];  \
    res0  += temp2_r * pa2[k];      \
    res0 OP0 temp2_i * pa2[k + 1];  \
    res0  += temp3_r * pa3[k];      \
    res0 OP0 temp3_i * pa3[k + 1];  \
                                    \
    res1 OP1 temp0_r * pa0[k + 1];  \
    res1 OP2 temp0_i * pa0[k];      \
    res1 OP1 temp1_r * pa1[k + 1];  \
    res1 OP2 temp1_i * pa1[k];      \
    res1 OP1 temp2_r * pa2[k + 1];  \
    res1 OP2 temp2_i * pa2[k];      \
    res1 OP1 temp3_r * pa3[k + 1];  \
    res1 OP2 temp3_i * pa3[k];      \
                                    \
    y[0 * inc_y2]     = res0;       \
    y[0 * inc_y2 + 1] = res1;       \

#define ZGEMV_N_4x2()                     \
    LD_DP4(pa0 + k, 2, t0, t1, t2, t3);   \
    LD_DP4(pa1 + k, 2, t4, t5, t6, t7);   \
                                          \
    PCKEVOD_D2_DP(t1, t0, src0r, src0i);  \
    PCKEVOD_D2_DP(t3, t2, src1r, src1i);  \
    PCKEVOD_D2_DP(t5, t4, src2r, src2i);  \
    PCKEVOD_D2_DP(t7, t6, src3r, src3i);  \
                                          \
    y0r += tp0r * src0r;                  \
    y1r += tp0r * src1r;                  \
    y0r += tp1r * src2r;                  \
    y1r += tp1r * src3r;                  \
                                          \
    y0r OP0 tp0i * src0i;                 \
    y1r OP0 tp0i * src1i;                 \
    y0r OP0 tp1i * src2i;                 \
    y1r OP0 tp1i * src3i;                 \
                                          \
    y0i OP1 tp0r * src0i;                 \
    y1i OP1 tp0r * src1i;                 \
    y0i OP1 tp1r * src2i;                 \
    y1i OP1 tp1r * src3i;                 \
                                          \
    y0i OP2 tp0i * src0r;                 \
    y1i OP2 tp0i * src1r;                 \
    y0i OP2 tp1i * src2r;                 \
    y1i OP2 tp1i * src3r;                 \

#define ZGEMV_N_2x2()                     \
    LD_DP2(pa0 + k, 2, t0, t1);           \
    LD_DP2(pa1 + k, 2, t4, t5);           \
                                          \
    PCKEVOD_D2_DP(t1, t0, src0r, src0i);  \
    PCKEVOD_D2_DP(t5, t4, src2r, src2i);  \
                                          \
    y0r += tp0r * src0r;                  \
    y0r += tp1r * src2r;                  \
                                          \
    y0r OP0 tp0i * src0i;                 \
    y0r OP0 tp1i * src2i;                 \
                                          \
    y0i OP1 tp0r * src0i;                 \
    y0i OP1 tp1r * src2i;                 \
                                          \
    y0i OP2 tp0i * src0r;                 \
    y0i OP2 tp1i * src2r;                 \

#define ZGEMV_N_1x2()               \
    res0 = y[0 * inc_y2];           \
    res1 = y[0 * inc_y2 + 1];       \
                                    \
    res0  += temp0_r * pa0[k];      \
    res0 OP0 temp0_i * pa0[k + 1];  \
    res0  += temp1_r * pa1[k];      \
    res0 OP0 temp1_i * pa1[k + 1];  \
                                    \
    res1 OP1 temp0_r * pa0[k + 1];  \
    res1 OP2 temp0_i * pa0[k];      \
    res1 OP1 temp1_r * pa1[k + 1];  \
    res1 OP2 temp1_i * pa1[k];      \
                                    \
    y[0 * inc_y2]     = res0;       \
    y[0 * inc_y2 + 1] = res1;       \

#define ZGEMV_N_4x1()                     \
    LD_DP4(pa0 + k, 2, t0, t1, t2, t3);   \
                                          \
    PCKEVOD_D2_DP(t1, t0, src0r, src0i);  \
    PCKEVOD_D2_DP(t3, t2, src1r, src1i);  \
                                          \
    y0r += tp0r * src0r;                  \
    y1r += tp0r * src1r;                  \
                                          \
    y0r OP0 tp0i * src0i;                 \
    y1r OP0 tp0i * src1i;                 \
                                          \
    y0i OP1 tp0r * src0i;                 \
    y1i OP1 tp0r * src1i;                 \
                                          \
    y0i OP2 tp0i * src0r;                 \
    y1i OP2 tp0i * src1r;                 \

#define ZGEMV_N_2x1()                     \
    LD_DP2(pa0 + k, 2, t0, t1);           \
                                          \
    PCKEVOD_D2_DP(t1, t0, src0r, src0i);  \
                                          \
    y0r += tp0r * src0r;                  \
    y0r OP0 tp0i * src0i;                 \
    y0i OP1 tp0r * src0i;                 \
    y0i OP2 tp0i * src0r;                 \

#define ZGEMV_N_1x1()               \
    res0 = y[0 * inc_y2];           \
    res1 = y[0 * inc_y2 + 1];       \
                                    \
    res0  += temp0_r * pa0[k];      \
    res0 OP0 temp0_i * pa0[k + 1];  \
                                    \
    res1 OP1 temp0_r * pa0[k + 1];  \
    res1 OP2 temp0_i * pa0[k];      \
                                    \
    y[0 * inc_y2]     = res0;       \
    y[0 * inc_y2 + 1] = res1;       \

#define ZLOAD_X4_SCALE_VECTOR()       \
    LD_DP4(x, 2, x0, x1, x2, x3);     \
                                      \
    PCKEVOD_D2_DP(x1, x0, x0r, x0i);  \
    PCKEVOD_D2_DP(x3, x2, x1r, x1i);  \
                                      \
    tp4r   = alphar * x0r;            \
    tp4r OP3 alphai * x0i;            \
    tp4i   = alphar * x0i;            \
    tp4i OP4 alphai * x0r;            \
                                      \
    tp5r   = alphar * x1r;            \
    tp5r OP3 alphai * x1i;            \
    tp5i   = alphar * x1i;            \
    tp5i OP4 alphai * x1r;            \
                                      \
    SPLATI_D2_DP(tp4r, tp0r, tp1r);   \
    SPLATI_D2_DP(tp5r, tp2r, tp3r);   \
    SPLATI_D2_DP(tp4i, tp0i, tp1i);   \
    SPLATI_D2_DP(tp5i, tp2i, tp3i);   \

#define ZLOAD_X2_SCALE_VECTOR()       \
    LD_DP2(x, 2, x0, x1);             \
                                      \
    PCKEVOD_D2_DP(x1, x0, x0r, x0i);  \
                                      \
    tp4r   = alphar * x0r;            \
    tp4r OP3 alphai * x0i;            \
    tp4i   = alphar * x0i;            \
    tp4i OP4 alphai * x0r;            \
                                      \
    SPLATI_D2_DP(tp4r, tp0r, tp1r);   \
    SPLATI_D2_DP(tp4i, tp0i, tp1i);   \

#define ZLOAD_X4_SCALE_GP()                                                               \
    x0r = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((BLASLONG *)(x + 0 * inc_x2)));       \
    x0r = (v2f64) __msa_insert_d((v2i64) x0r,  1, *((BLASLONG *)(x + 1 * inc_x2)));       \
    x1r = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((BLASLONG *)(x + 2 * inc_x2)));       \
    x1r = (v2f64) __msa_insert_d((v2i64) x1r,  1, *((BLASLONG *)(x + 3 * inc_x2)));       \
    x0i = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((BLASLONG *)(x + 0 * inc_x2 + 1)));   \
    x0i = (v2f64) __msa_insert_d((v2i64) x0i,  1, *((BLASLONG *)(x + 1 * inc_x2 + 1)));   \
    x1i = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((BLASLONG *)(x + 2 * inc_x2 + 1)));   \
    x1i = (v2f64) __msa_insert_d((v2i64) x1i,  1, *((BLASLONG *)(x + 3 * inc_x2 + 1)));   \
                                                                                          \
    tp4r   = alphar * x0r;                                                                \
    tp4r OP3 alphai * x0i;                                                                \
    tp4i   = alphar * x0i;                                                                \
    tp4i OP4 alphai * x0r;                                                                \
                                                                                          \
    tp5r   = alphar * x1r;                                                                \
    tp5r OP3 alphai * x1i;                                                                \
    tp5i   = alphar * x1i;                                                                \
    tp5i OP4 alphai * x1r;                                                                \
                                                                                          \
    SPLATI_D2_DP(tp4r, tp0r, tp1r);                                                       \
    SPLATI_D2_DP(tp5r, tp2r, tp3r);                                                       \
    SPLATI_D2_DP(tp4i, tp0i, tp1i);                                                       \
    SPLATI_D2_DP(tp5i, tp2i, tp3i);                                                       \

#define ZLOAD_X2_SCALE_GP()                                                               \
    x0r = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((BLASLONG *)(x + 0 * inc_x2)));       \
    x0r = (v2f64) __msa_insert_d((v2i64) x0r,  1, *((BLASLONG *)(x + 1 * inc_x2)));       \
    x0i = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((BLASLONG *)(x + 0 * inc_x2 + 1)));   \
    x0i = (v2f64) __msa_insert_d((v2i64) x0i,  1, *((BLASLONG *)(x + 1 * inc_x2 + 1)));   \
                                                                                          \
    tp4r   = alphar * x0r;                                                                \
    tp4r OP3 alphai * x0i;                                                                \
    tp4i   = alphar * x0i;                                                                \
    tp4i OP4 alphai * x0r;                                                                \
                                                                                          \
    SPLATI_D2_DP(tp4r, tp0r, tp1r);                                                       \
    SPLATI_D2_DP(tp4i, tp0i, tp1i);                                                       \

#define ZLOAD_X1_SCALE_GP()                         \
    temp0_r   = alpha_r * x[0 * inc_x2];            \
    temp0_r OP3 alpha_i * x[0 * inc_x2 + 1];        \
    temp0_i   = alpha_r * x[0 * inc_x2 + 1];        \
    temp0_i OP4 alpha_i * x[0 * inc_x2];            \
                                                    \
    tp0r = (v2f64) COPY_DOUBLE_TO_VECTOR(temp0_r);  \
    tp0i = (v2f64) COPY_DOUBLE_TO_VECTOR(temp0_i);  \

#define ZLOAD_Y4_VECTOR()             \
    LD_DP4(y, 2, y0, y1, y2, y3);     \
    PCKEVOD_D2_DP(y1, y0, y0r, y0i);  \
    PCKEVOD_D2_DP(y3, y2, y1r, y1i);  \

#define ZLOAD_Y2_VECTOR()             \
    LD_DP2(y, 2, y0, y1);             \
    PCKEVOD_D2_DP(y1, y0, y0r, y0i);  \

#define ZSTORE_Y4_VECTOR()          \
    ILVRL_D2_DP(y0i, y0r, y0, y1);  \
    ILVRL_D2_DP(y1i, y1r, y2, y3);  \
    ST_DP4(y0, y1, y2, y3, y, 2);   \

#define ZSTORE_Y2_VECTOR()          \
    ILVRL_D2_DP(y0i, y0r, y0, y1);  \
    ST_DP2(y0, y1, y, 2);           \

#define ZLOAD_Y4_GP()                                                                     \
    y0r = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((BLASLONG *)(y +  0 * inc_y2)));      \
    y0r = (v2f64) __msa_insert_d((v2i64) y0r,  1, *((BLASLONG *)(y +  1 * inc_y2)));      \
    y1r = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((BLASLONG *)(y +  2 * inc_y2)));      \
    y1r = (v2f64) __msa_insert_d((v2i64) y1r,  1, *((BLASLONG *)(y +  3 * inc_y2)));      \
    y0i = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((BLASLONG *)(y +  0 * inc_y2 + 1)));  \
    y0i = (v2f64) __msa_insert_d((v2i64) y0i,  1, *((BLASLONG *)(y +  1 * inc_y2 + 1)));  \
    y1i = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((BLASLONG *)(y +  2 * inc_y2 + 1)));  \
    y1i = (v2f64) __msa_insert_d((v2i64) y1i,  1, *((BLASLONG *)(y +  3 * inc_y2 + 1)));  \

#define ZLOAD_Y2_GP()                                                                     \
    y0r = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((BLASLONG *)(y +  0 * inc_y2)));      \
    y0r = (v2f64) __msa_insert_d((v2i64) y0r,  1, *((BLASLONG *)(y +  1 * inc_y2)));      \
    y0i = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((BLASLONG *)(y +  0 * inc_y2 + 1)));  \
    y0i = (v2f64) __msa_insert_d((v2i64) y0i,  1, *((BLASLONG *)(y +  1 * inc_y2 + 1)));  \

#define ZSTORE_Y4_GP()                                                     \
    *((BLASLONG *)(y + 0 * inc_y2)) = __msa_copy_s_d((v2i64) y0r, 0);      \
    *((BLASLONG *)(y + 1 * inc_y2)) = __msa_copy_s_d((v2i64) y0r, 1);      \
    *((BLASLONG *)(y + 2 * inc_y2)) = __msa_copy_s_d((v2i64) y1r, 0);      \
    *((BLASLONG *)(y + 3 * inc_y2)) = __msa_copy_s_d((v2i64) y1r, 1);      \
    *((BLASLONG *)(y + 0 * inc_y2 + 1)) = __msa_copy_s_d((v2i64) y0i, 0);  \
    *((BLASLONG *)(y + 1 * inc_y2 + 1)) = __msa_copy_s_d((v2i64) y0i, 1);  \
    *((BLASLONG *)(y + 2 * inc_y2 + 1)) = __msa_copy_s_d((v2i64) y1i, 0);  \
    *((BLASLONG *)(y + 3 * inc_y2 + 1)) = __msa_copy_s_d((v2i64) y1i, 1);  \

#define ZSTORE_Y2_GP()                                                     \
    *((BLASLONG *)(y + 0 * inc_y2)) = __msa_copy_s_d((v2i64) y0r, 0);      \
    *((BLASLONG *)(y + 1 * inc_y2)) = __msa_copy_s_d((v2i64) y0r, 1);      \
    *((BLASLONG *)(y + 0 * inc_y2 + 1)) = __msa_copy_s_d((v2i64) y0i, 0);  \
    *((BLASLONG *)(y + 1 * inc_y2 + 1)) = __msa_copy_s_d((v2i64) y0i, 1);  \

#define ZGEMV_N_MSA()                        \
    for (j = (n >> 2); j--;)                 \
    {                                        \
        ZLOAD_X4_SCALE()                     \
                                             \
        k = 0;                               \
        k_pref = pref_offset;                \
        y = y_org;                           \
                                             \
        for (i = (m >> 2); i--;)             \
        {                                    \
            PREFETCH(pa0 + k_pref + 8 + 0);  \
            PREFETCH(pa0 + k_pref + 8 + 4);  \
            PREFETCH(pa1 + k_pref + 8 + 0);  \
            PREFETCH(pa1 + k_pref + 8 + 4);  \
            PREFETCH(pa2 + k_pref + 8 + 0);  \
            PREFETCH(pa2 + k_pref + 8 + 4);  \
            PREFETCH(pa3 + k_pref + 8 + 0);  \
            PREFETCH(pa3 + k_pref + 8 + 4);  \
                                             \
            ZLOAD_Y4()                       \
            ZGEMV_N_4x4()                    \
            ZSTORE_Y4()                      \
                                             \
            k += 2 * 4;                      \
            k_pref += 2 * 4;                 \
            y += inc_y2 * 4;                 \
        }                                    \
                                             \
        if (m & 2)                           \
        {                                    \
            ZLOAD_Y2()                       \
            ZGEMV_N_2x4()                    \
            ZSTORE_Y2()                      \
                                             \
            k += 2 * 2;                      \
            y += inc_y2 * 2;                 \
        }                                    \
                                             \
        if (m & 1)                           \
        {                                    \
            temp0_r = tp4r[0];               \
            temp1_r = tp4r[1];               \
            temp2_r = tp5r[0];               \
            temp3_r = tp5r[1];               \
                                             \
            temp0_i = tp4i[0];               \
            temp1_i = tp4i[1];               \
            temp2_i = tp5i[0];               \
            temp3_i = tp5i[1];               \
                                             \
            ZGEMV_N_1x4()                    \
            k += 2;                          \
            y += inc_y2;                     \
        }                                    \
                                             \
        pa0 += 4 * lda2;                     \
        pa1 += 4 * lda2;                     \
        pa2 += 4 * lda2;                     \
        pa3 += 4 * lda2;                     \
                                             \
        x += 4 * inc_x2;                     \
    }                                        \
                                             \
    if (n & 2)                               \
    {                                        \
        ZLOAD_X2_SCALE()                     \
                                             \
        k = 0;                               \
        y = y_org;                           \
                                             \
        for (i = (m >> 2); i--;)             \
        {                                    \
            ZLOAD_Y4()                       \
            ZGEMV_N_4x2()                    \
            ZSTORE_Y4()                      \
                                             \
            k += 2 * 4;                      \
            y += inc_y2 * 4;                 \
        }                                    \
                                             \
        if (m & 2)                           \
        {                                    \
            ZLOAD_Y2()                       \
            ZGEMV_N_2x2()                    \
            ZSTORE_Y2()                      \
                                             \
            k += 2 * 2;                      \
            y += inc_y2 * 2;                 \
        }                                    \
                                             \
        if (m & 1)                           \
        {                                    \
            temp0_r = tp4r[0];               \
            temp1_r = tp4r[1];               \
                                             \
            temp0_i = tp4i[0];               \
            temp1_i = tp4i[1];               \
                                             \
            ZGEMV_N_1x2()                    \
                                             \
            k += 2;                          \
            y += inc_y2;                     \
        }                                    \
                                             \
        pa0 += 2 * lda2;                     \
        pa1 += 2 * lda2;                     \
                                             \
        x += 2 * inc_x2;                     \
    }                                        \
                                             \
    if (n & 1)                               \
    {                                        \
        ZLOAD_X1_SCALE()                     \
                                             \
        k = 0;                               \
        y = y_org;                           \
                                             \
        for (i = (m >> 2); i--;)             \
        {                                    \
            ZLOAD_Y4()                       \
            ZGEMV_N_4x1()                    \
            ZSTORE_Y4()                      \
                                             \
            k += 2 * 4;                      \
            y += inc_y2 * 4;                 \
        }                                    \
                                             \
        if (m & 2)                           \
        {                                    \
            ZLOAD_Y2()                       \
            ZGEMV_N_2x1()                    \
            ZSTORE_Y2()                      \
                                             \
            k += 2 * 2;                      \
            y += inc_y2 * 2;                 \
        }                                    \
                                             \
        if (m & 1)                           \
        {                                    \
            ZGEMV_N_1x1()                    \
                                             \
            k += 2;                          \
            y += inc_y2;                     \
        }                                    \
                                             \
        pa0 += lda2;                         \
        x += inc_x2;                         \
    }                                        \

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha_r, FLOAT alpha_i,
          FLOAT *A, BLASLONG lda2, FLOAT *x, BLASLONG inc_x2, FLOAT *y,
          BLASLONG inc_y2, FLOAT *buffer)
{
    BLASLONG i, j, k, k_pref, pref_offset;
    FLOAT *y_org = y;
    FLOAT *pa0, *pa1, *pa2, *pa3;
    FLOAT temp0_r, temp1_r, temp2_r, temp3_r, temp0_i, temp1_i, temp2_i;
    FLOAT temp3_i, res0, res1;
    v2f64 alphar, alphai;
    v2f64 x0, x1, x2, x3, y0, y1, y2, y3;
    v2f64 x0r, x1r, x0i, x1i, y0r, y1r, y0i, y1i;
    v2f64 t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15;
    v2f64 src0r, src1r, src2r, src3r, src4r, src5r, src6r, src7r;
    v2f64 src0i, src1i, src2i, src3i, src4i, src5i, src6i, src7i;
    v2f64 tp0r, tp1r, tp2r, tp3r, tp4r, tp5r, tp0i, tp1i, tp2i, tp3i, tp4i, tp5i;

    lda2   = 2 * lda2;
    inc_x2 = 2 * inc_x2;
    inc_y2 = 2 * inc_y2;

    pref_offset = (uintptr_t)A & (L1_DATA_LINESIZE - 1);
    pref_offset = L1_DATA_LINESIZE - pref_offset;
    pref_offset = pref_offset / sizeof(FLOAT);

    pa0 = A;
    pa1 = A + lda2;
    pa2 = A + 2 * lda2;
    pa3 = A + 3 * lda2;

    alphar = COPY_DOUBLE_TO_VECTOR(alpha_r);
    alphai = COPY_DOUBLE_TO_VECTOR(alpha_i);

    if ((2 == inc_x2) && (2 == inc_y2))
    {
        #define ZLOAD_X4_SCALE  ZLOAD_X4_SCALE_VECTOR
        #define ZLOAD_X2_SCALE  ZLOAD_X2_SCALE_VECTOR
        #define ZLOAD_X1_SCALE  ZLOAD_X1_SCALE_GP
        #define ZLOAD_Y4        ZLOAD_Y4_VECTOR
        #define ZLOAD_Y2        ZLOAD_Y2_VECTOR
        #define ZSTORE_Y4       ZSTORE_Y4_VECTOR
        #define ZSTORE_Y2       ZSTORE_Y2_VECTOR

        ZGEMV_N_MSA();

        #undef ZLOAD_X4_SCALE
        #undef ZLOAD_X2_SCALE
        #undef ZLOAD_X1_SCALE
        #undef ZLOAD_Y4
        #undef ZLOAD_Y2
        #undef ZSTORE_Y4
        #undef ZSTORE_Y2
    }
    else if (2 == inc_x2)
    {
        #define ZLOAD_X4_SCALE  ZLOAD_X4_SCALE_VECTOR
        #define ZLOAD_X2_SCALE  ZLOAD_X2_SCALE_VECTOR
        #define ZLOAD_X1_SCALE  ZLOAD_X1_SCALE_GP
        #define ZLOAD_Y4        ZLOAD_Y4_GP
        #define ZLOAD_Y2        ZLOAD_Y2_GP
        #define ZSTORE_Y4       ZSTORE_Y4_GP
        #define ZSTORE_Y2       ZSTORE_Y2_GP

        ZGEMV_N_MSA();

        #undef ZLOAD_X4_SCALE
        #undef ZLOAD_X2_SCALE
        #undef ZLOAD_X1_SCALE
        #undef ZLOAD_Y4
        #undef ZLOAD_Y2
        #undef ZSTORE_Y4
        #undef ZSTORE_Y2
    }
    else if (2 == inc_y2)
    {
        #define ZLOAD_X4_SCALE  ZLOAD_X4_SCALE_GP
        #define ZLOAD_X2_SCALE  ZLOAD_X2_SCALE_GP
        #define ZLOAD_X1_SCALE  ZLOAD_X1_SCALE_GP
        #define ZLOAD_Y4        ZLOAD_Y4_VECTOR
        #define ZLOAD_Y2        ZLOAD_Y2_VECTOR
        #define ZSTORE_Y4       ZSTORE_Y4_VECTOR
        #define ZSTORE_Y2       ZSTORE_Y2_VECTOR

        ZGEMV_N_MSA();

        #undef ZLOAD_X4_SCALE
        #undef ZLOAD_X2_SCALE
        #undef ZLOAD_X1_SCALE
        #undef ZLOAD_Y4
        #undef ZLOAD_Y2
        #undef ZSTORE_Y4
        #undef ZSTORE_Y2
    }
    else
    {
        #define ZLOAD_X4_SCALE  ZLOAD_X4_SCALE_GP
        #define ZLOAD_X2_SCALE  ZLOAD_X2_SCALE_GP
        #define ZLOAD_X1_SCALE  ZLOAD_X1_SCALE_GP
        #define ZLOAD_Y4        ZLOAD_Y4_GP
        #define ZLOAD_Y2        ZLOAD_Y2_GP
        #define ZSTORE_Y4       ZSTORE_Y4_GP
        #define ZSTORE_Y2       ZSTORE_Y2_GP

        ZGEMV_N_MSA();

        #undef ZLOAD_X4_SCALE
        #undef ZLOAD_X2_SCALE
        #undef ZLOAD_X1_SCALE
        #undef ZLOAD_Y4
        #undef ZLOAD_Y2
        #undef ZSTORE_Y4
        #undef ZSTORE_Y2
    }
    return(0);
}

#undef OP0
#undef OP1
#undef OP2
#undef OP3
#undef OP4
