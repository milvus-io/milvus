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

#define SGEMV_T_8x8()              \
{                                  \
    LD_SP2(pa0 + k, 4, t0, t1);    \
    LD_SP2(pa1 + k, 4, t2, t3);    \
    LD_SP2(pa2 + k, 4, t4, t5);    \
    LD_SP2(pa3 + k, 4, t6, t7);    \
    LD_SP2(pa4 + k, 4, t8, t9);    \
    LD_SP2(pa5 + k, 4, t10, t11);  \
    LD_SP2(pa6 + k, 4, t12, t13);  \
    LD_SP2(pa7 + k, 4, t14, t15);  \
                                   \
    tp0 += x0 * t0;                \
    tp0 += x1 * t1;                \
                                   \
    tp1 += x0 * t2;                \
    tp1 += x1 * t3;                \
                                   \
    tp2 += x0 * t4;                \
    tp2 += x1 * t5;                \
                                   \
    tp3 += x0 * t6;                \
    tp3 += x1 * t7;                \
                                   \
    tp4 += x0 * t8;                \
    tp4 += x1 * t9;                \
                                   \
    tp5 += x0 * t10;               \
    tp5 += x1 * t11;               \
                                   \
    tp6 += x0 * t12;               \
    tp6 += x1 * t13;               \
                                   \
    tp7 += x0 * t14;               \
    tp7 += x1 * t15;               \
}

#define SGEMV_T_8x4()      \
{                          \
    t0  = LD_SP(pa0 + k);  \
    t2  = LD_SP(pa1 + k);  \
    t4  = LD_SP(pa2 + k);  \
    t6  = LD_SP(pa3 + k);  \
    t8  = LD_SP(pa4 + k);  \
    t10 = LD_SP(pa5 + k);  \
    t12 = LD_SP(pa6 + k);  \
    t14 = LD_SP(pa7 + k);  \
                           \
    tp0 += x0 * t0;        \
    tp1 += x0 * t2;        \
    tp2 += x0 * t4;        \
    tp3 += x0 * t6;        \
    tp4 += x0 * t8;        \
    tp5 += x0 * t10;       \
    tp6 += x0 * t12;       \
    tp7 += x0 * t14;       \
}

#define SGEMV_T_4x8()            \
{                                \
    LD_SP2(pa0 + k, 4, t0, t1);  \
    LD_SP2(pa1 + k, 4, t2, t3);  \
    LD_SP2(pa2 + k, 4, t4, t5);  \
    LD_SP2(pa3 + k, 4, t6, t7);  \
                                 \
    tp0 += x0 * t0;              \
    tp0 += x1 * t1;              \
                                 \
    tp1 += x0 * t2;              \
    tp1 += x1 * t3;              \
                                 \
    tp2 += x0 * t4;              \
    tp2 += x1 * t5;              \
                                 \
    tp3 += x0 * t6;              \
    tp3 += x1 * t7;              \
}

#define SGEMV_T_4x4()     \
{                         \
    t0 = LD_SP(pa0 + k);  \
    t2 = LD_SP(pa1 + k);  \
    t4 = LD_SP(pa2 + k);  \
    t6 = LD_SP(pa3 + k);  \
                          \
    tp0 += x0 * t0;       \
    tp1 += x0 * t2;       \
    tp2 += x0 * t4;       \
    tp3 += x0 * t6;       \
}

#define SGEMV_T_2x8()            \
{                                \
    LD_SP2(pa0 + k, 4, t0, t1);  \
    LD_SP2(pa1 + k, 4, t2, t3);  \
                                 \
    tp0 += x0 * t0;              \
    tp0 += x1 * t1;              \
                                 \
    tp1 += x0 * t2;              \
    tp1 += x1 * t3;              \
}

#define SGEMV_T_2x4()     \
{                         \
    t0 = LD_SP(pa0 + k);  \
    t2 = LD_SP(pa1 + k);  \
                          \
    tp0 += x0 * t0;       \
    tp1 += x0 * t2;       \
}

#define SLOAD_X8_GP()                                                        \
    x0 = (v4f32) __msa_insert_w((v4i32) tp0, 0, *((int *)(x + 0 * inc_x)));  \
    x0 = (v4f32) __msa_insert_w((v4i32) x0,  1, *((int *)(x + 1 * inc_x)));  \
    x0 = (v4f32) __msa_insert_w((v4i32) x0,  2, *((int *)(x + 2 * inc_x)));  \
    x0 = (v4f32) __msa_insert_w((v4i32) x0,  3, *((int *)(x + 3 * inc_x)));  \
    x1 = (v4f32) __msa_insert_w((v4i32) tp0, 0, *((int *)(x + 4 * inc_x)));  \
    x1 = (v4f32) __msa_insert_w((v4i32) x1,  1, *((int *)(x + 5 * inc_x)));  \
    x1 = (v4f32) __msa_insert_w((v4i32) x1,  2, *((int *)(x + 6 * inc_x)));  \
    x1 = (v4f32) __msa_insert_w((v4i32) x1,  3, *((int *)(x + 7 * inc_x)));  \

#define SLOAD_X4_GP()                                                        \
    x0 = (v4f32) __msa_insert_w((v4i32) tp0, 0, *((int *)(x + 0 * inc_x)));  \
    x0 = (v4f32) __msa_insert_w((v4i32) x0,  1, *((int *)(x + 1 * inc_x)));  \
    x0 = (v4f32) __msa_insert_w((v4i32) x0,  2, *((int *)(x + 2 * inc_x)));  \
    x0 = (v4f32) __msa_insert_w((v4i32) x0,  3, *((int *)(x + 3 * inc_x)));  \

#define SLOAD_X8_VECTOR()  LD_SP2(x, 4, x0, x1);
#define SLOAD_X4_VECTOR()  x0 = LD_SP(x);

#define SGEMV_T_MSA()                            \
    for (j = (n >> 3); j--;)                     \
    {                                            \
        tp0 = zero;                              \
        tp1 = zero;                              \
        tp2 = zero;                              \
        tp3 = zero;                              \
        tp4 = zero;                              \
        tp5 = zero;                              \
        tp6 = zero;                              \
        tp7 = zero;                              \
                                                 \
        k = 0;                                   \
        x = srcx_org;                            \
                                                 \
        for (i = (m >> 3); i--;)                 \
        {                                        \
            SLOAD_X8();                          \
            SGEMV_T_8x8();                       \
                                                 \
            x += 8 * inc_x;                      \
            k += 8;                              \
        }                                        \
                                                 \
        if (m & 4)                               \
        {                                        \
            SLOAD_X4();                          \
            SGEMV_T_8x4();                       \
                                                 \
            x += 4 * inc_x;                      \
            k += 4;                              \
        }                                        \
                                                 \
        TRANSPOSE4x4_SP_SP(tp0, tp1, tp2, tp3,   \
                           tp0, tp1, tp2, tp3);  \
        TRANSPOSE4x4_SP_SP(tp4, tp5, tp6, tp7,   \
                           tp4, tp5, tp6, tp7);  \
        tp0 += tp1;                              \
        tp0 += tp2;                              \
        tp0 += tp3;                              \
        tp4 += tp5;                              \
        tp4 += tp6;                              \
        tp4 += tp7;                              \
                                                 \
        temp0 = tp0[0];                          \
        temp1 = tp0[1];                          \
        temp2 = tp0[2];                          \
        temp3 = tp0[3];                          \
        temp4 = tp4[0];                          \
        temp5 = tp4[1];                          \
        temp6 = tp4[2];                          \
        temp7 = tp4[3];                          \
                                                 \
        for (i = (m & 3); i--;)                  \
        {                                        \
            temp0 += pa0[k] * x[0];              \
            temp1 += pa1[k] * x[0];              \
            temp2 += pa2[k] * x[0];              \
            temp3 += pa3[k] * x[0];              \
            temp4 += pa4[k] * x[0];              \
            temp5 += pa5[k] * x[0];              \
            temp6 += pa6[k] * x[0];              \
            temp7 += pa7[k] * x[0];              \
                                                 \
            x += inc_x;                          \
            k++;                                 \
        }                                        \
                                                 \
        res0 = y[0 * inc_y];                     \
        res1 = y[1 * inc_y];                     \
        res2 = y[2 * inc_y];                     \
        res3 = y[3 * inc_y];                     \
        res4 = y[4 * inc_y];                     \
        res5 = y[5 * inc_y];                     \
        res6 = y[6 * inc_y];                     \
        res7 = y[7 * inc_y];                     \
                                                 \
        res0 += alpha * temp0;                   \
        res1 += alpha * temp1;                   \
        res2 += alpha * temp2;                   \
        res3 += alpha * temp3;                   \
        res4 += alpha * temp4;                   \
        res5 += alpha * temp5;                   \
        res6 += alpha * temp6;                   \
        res7 += alpha * temp7;                   \
                                                 \
        y[0 * inc_y] = res0;                     \
        y[1 * inc_y] = res1;                     \
        y[2 * inc_y] = res2;                     \
        y[3 * inc_y] = res3;                     \
        y[4 * inc_y] = res4;                     \
        y[5 * inc_y] = res5;                     \
        y[6 * inc_y] = res6;                     \
        y[7 * inc_y] = res7;                     \
                                                 \
        y += 8 * inc_y;                          \
                                                 \
        pa0 += 8 * lda;                          \
        pa1 += 8 * lda;                          \
        pa2 += 8 * lda;                          \
        pa3 += 8 * lda;                          \
        pa4 += 8 * lda;                          \
        pa5 += 8 * lda;                          \
        pa6 += 8 * lda;                          \
        pa7 += 8 * lda;                          \
    }                                            \
                                                 \
    if (n & 4)                                   \
    {                                            \
        tp0 = zero;                              \
        tp1 = zero;                              \
        tp2 = zero;                              \
        tp3 = zero;                              \
                                                 \
        k = 0;                                   \
        x = srcx_org;                            \
                                                 \
        for (i = (m >> 3); i--;)                 \
        {                                        \
            SLOAD_X8();                          \
            SGEMV_T_4x8();                       \
                                                 \
            x += 8 * inc_x;                      \
            k += 8;                              \
        }                                        \
                                                 \
        if (m & 4)                               \
        {                                        \
            SLOAD_X4();                          \
            SGEMV_T_4x4();                       \
                                                 \
            x += 4 * inc_x;                      \
            k += 4;                              \
        }                                        \
                                                 \
        TRANSPOSE4x4_SP_SP(tp0, tp1, tp2, tp3,   \
                           tp0, tp1, tp2, tp3);  \
        tp0 += tp1;                              \
        tp0 += tp2;                              \
        tp0 += tp3;                              \
                                                 \
        temp0 = tp0[0];                          \
        temp1 = tp0[1];                          \
        temp2 = tp0[2];                          \
        temp3 = tp0[3];                          \
                                                 \
        for (i = (m & 3); i--;)                  \
        {                                        \
            temp0 += pa0[k] * x[0];              \
            temp1 += pa1[k] * x[0];              \
            temp2 += pa2[k] * x[0];              \
            temp3 += pa3[k] * x[0];              \
                                                 \
            x += inc_x;                          \
            k++;                                 \
        }                                        \
                                                 \
        res0 = y[0 * inc_y];                     \
        res1 = y[1 * inc_y];                     \
        res2 = y[2 * inc_y];                     \
        res3 = y[3 * inc_y];                     \
                                                 \
        res0 += alpha * temp0;                   \
        res1 += alpha * temp1;                   \
        res2 += alpha * temp2;                   \
        res3 += alpha * temp3;                   \
                                                 \
        y[0 * inc_y] = res0;                     \
        y[1 * inc_y] = res1;                     \
        y[2 * inc_y] = res2;                     \
        y[3 * inc_y] = res3;                     \
                                                 \
        y += 4 * inc_y;                          \
                                                 \
        pa0 += 4 * lda;                          \
        pa1 += 4 * lda;                          \
        pa2 += 4 * lda;                          \
        pa3 += 4 * lda;                          \
    }                                            \
                                                 \
    if (n & 2)                                   \
    {                                            \
        tp0 = zero;                              \
        tp1 = zero;                              \
                                                 \
        k = 0;                                   \
        x = srcx_org;                            \
                                                 \
        for (i = (m >> 3); i--;)                 \
        {                                        \
            SLOAD_X8();                          \
            SGEMV_T_2x8();                       \
                                                 \
            x += 8 * inc_x;                      \
            k += 8;                              \
        }                                        \
                                                 \
        if (m & 4)                               \
        {                                        \
            SLOAD_X4();                          \
            SGEMV_T_2x4();                       \
                                                 \
            x += 4 * inc_x;                      \
            k += 4;                              \
        }                                        \
                                                 \
        ILVRL_W2_SP(tp1, tp0, tp2, tp3);         \
                                                 \
        tp2 += tp3;                              \
                                                 \
        temp0 = tp2[0] + tp2[2];                 \
        temp1 = tp2[1] + tp2[3];                 \
                                                 \
        for (i = (m & 3); i--;)                  \
        {                                        \
            temp0 += pa0[k] * x[0];              \
            temp1 += pa1[k] * x[0];              \
                                                 \
            x += inc_x;                          \
            k++;                                 \
        }                                        \
                                                 \
        res0 = y[0 * inc_y];                     \
        res1 = y[1 * inc_y];                     \
                                                 \
        res0 += alpha * temp0;                   \
        res1 += alpha * temp1;                   \
                                                 \
        y[0 * inc_y] = res0;                     \
        y[1 * inc_y] = res1;                     \
                                                 \
        y += 2 * inc_y;                          \
                                                 \
        pa0 += 2 * lda;                          \
        pa1 += 2 * lda;                          \
    }                                            \
                                                 \
    if (n & 1)                                   \
    {                                            \
        temp0 = 0.0;                             \
                                                 \
        k = 0;                                   \
        x = srcx_org;                            \
                                                 \
        for (i = m; i--;)                        \
        {                                        \
            temp0 += pa0[k] * x[0];              \
                                                 \
            x += inc_x;                          \
            k++;                                 \
        }                                        \
                                                 \
        y[0] += alpha * temp0;                   \
        y += inc_y;                              \
        pa0 += lda;                              \
    }

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha, FLOAT *A,
          BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y,
          FLOAT *buffer)
{
    BLASLONG i, j, k;
    FLOAT *srcx_org = x;
    FLOAT *pa0, *pa1, *pa2, *pa3, *pa4, *pa5, *pa6, *pa7;
    FLOAT temp0, temp1, temp2, temp3, temp4, temp5, temp6, temp7;
    FLOAT res0, res1, res2, res3, res4, res5, res6, res7;
    v4f32 x0, x1;
    v4f32 t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15;
    v4f32 tp0, tp1, tp2, tp3, tp4, tp5, tp6, tp7;
    v4f32 zero = {0};

    pa0 = A + 0 * lda;
    pa1 = A + 1 * lda;
    pa2 = A + 2 * lda;
    pa3 = A + 3 * lda;
    pa4 = A + 4 * lda;
    pa5 = A + 5 * lda;
    pa6 = A + 6 * lda;
    pa7 = A + 7 * lda;

    if (1 == inc_x)
    {
        #define SLOAD_X8  SLOAD_X8_VECTOR
        #define SLOAD_X4  SLOAD_X4_VECTOR

        SGEMV_T_MSA();

        #undef SLOAD_X8
        #undef SLOAD_X4
    }
    else
    {
        #define SLOAD_X8  SLOAD_X8_GP
        #define SLOAD_X4  SLOAD_X4_GP

        SGEMV_T_MSA();

        #undef SLOAD_X8
        #undef SLOAD_X4
    }

    return(0);
}
