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

#define DGEMV_T_8x8()                        \
{                                            \
    LD_DP4(pa0 + k, 2, t0, t1, t2, t3);      \
    LD_DP4(pa1 + k, 2, t4, t5, t6, t7);      \
    LD_DP4(pa2 + k, 2, t8, t9, t10, t11);    \
    LD_DP4(pa3 + k, 2, t12, t13, t14, t15);  \
    LD_DP4(pa4 + k, 2, t16, t17, t18, t19);  \
    LD_DP4(pa5 + k, 2, t20, t21, t22, t23);  \
    LD_DP4(pa6 + k, 2, t24, t25, t26, t27);  \
    LD_DP4(pa7 + k, 2, t28, t29, t30, t31);  \
                                             \
    tp0 += x0 * t0;                          \
    tp0 += x1 * t1;                          \
    tp0 += x2 * t2;                          \
    tp0 += x3 * t3;                          \
                                             \
    tp1 += x0 * t4;                          \
    tp1 += x1 * t5;                          \
    tp1 += x2 * t6;                          \
    tp1 += x3 * t7;                          \
                                             \
    tp2 += x0 * t8;                          \
    tp2 += x1 * t9;                          \
    tp2 += x2 * t10;                         \
    tp2 += x3 * t11;                         \
                                             \
    tp3 += x0 * t12;                         \
    tp3 += x1 * t13;                         \
    tp3 += x2 * t14;                         \
    tp3 += x3 * t15;                         \
                                             \
    tp4 += x0 * t16;                         \
    tp4 += x1 * t17;                         \
    tp4 += x2 * t18;                         \
    tp4 += x3 * t19;                         \
                                             \
    tp5 += x0 * t20;                         \
    tp5 += x1 * t21;                         \
    tp5 += x2 * t22;                         \
    tp5 += x3 * t23;                         \
                                             \
    tp6 += x0 * t24;                         \
    tp6 += x1 * t25;                         \
    tp6 += x2 * t26;                         \
    tp6 += x3 * t27;                         \
                                             \
    tp7 += x0 * t28;                         \
    tp7 += x1 * t29;                         \
    tp7 += x2 * t30;                         \
    tp7 += x3 * t31;                         \
}

#define DGEMV_T_8x4()              \
{                                  \
    LD_DP2(pa0 + k, 2, t0, t1);    \
    LD_DP2(pa1 + k, 2, t4, t5);    \
    LD_DP2(pa2 + k, 2, t8, t9);    \
    LD_DP2(pa3 + k, 2, t12, t13);  \
    LD_DP2(pa4 + k, 2, t16, t17);  \
    LD_DP2(pa5 + k, 2, t20, t21);  \
    LD_DP2(pa6 + k, 2, t24, t25);  \
    LD_DP2(pa7 + k, 2, t28, t29);  \
                                   \
    tp0 += x0 * t0;                \
    tp0 += x1 * t1;                \
                                   \
    tp1 += x0 * t4;                \
    tp1 += x1 * t5;                \
                                   \
    tp2 += x0 * t8;                \
    tp2 += x1 * t9;                \
                                   \
    tp3 += x0 * t12;               \
    tp3 += x1 * t13;               \
                                   \
    tp4 += x0 * t16;               \
    tp4 += x1 * t17;               \
                                   \
    tp5 += x0 * t20;               \
    tp5 += x1 * t21;               \
                                   \
    tp6 += x0 * t24;               \
    tp6 += x1 * t25;               \
                                   \
    tp7 += x0 * t28;               \
    tp7 += x1 * t29;               \
}

#define DGEMV_T_8x2()      \
{                          \
    t0  = LD_DP(pa0 + k);  \
    t4  = LD_DP(pa1 + k);  \
    t8  = LD_DP(pa2 + k);  \
    t12 = LD_DP(pa3 + k);  \
    t16 = LD_DP(pa4 + k);  \
    t20 = LD_DP(pa5 + k);  \
    t24 = LD_DP(pa6 + k);  \
    t28 = LD_DP(pa7 + k);  \
                           \
    tp0 += x0 * t0;        \
    tp1 += x0 * t4;        \
    tp2 += x0 * t8;        \
    tp3 += x0 * t12;       \
    tp4 += x0 * t16;       \
    tp5 += x0 * t20;       \
    tp6 += x0 * t24;       \
    tp7 += x0 * t28;       \
}

#define DGEMV_T_4x8()                        \
{                                            \
    LD_DP4(pa0 + k, 2, t0, t1, t2, t3);      \
    LD_DP4(pa1 + k, 2, t4, t5, t6, t7);      \
    LD_DP4(pa2 + k, 2, t8, t9, t10, t11);    \
    LD_DP4(pa3 + k, 2, t12, t13, t14, t15);  \
                                             \
    tp0 += x0 * t0;                          \
    tp0 += x1 * t1;                          \
    tp0 += x2 * t2;                          \
    tp0 += x3 * t3;                          \
                                             \
    tp1 += x0 * t4;                          \
    tp1 += x1 * t5;                          \
    tp1 += x2 * t6;                          \
    tp1 += x3 * t7;                          \
                                             \
    tp2 += x0 * t8;                          \
    tp2 += x1 * t9;                          \
    tp2 += x2 * t10;                         \
    tp2 += x3 * t11;                         \
                                             \
    tp3 += x0 * t12;                         \
    tp3 += x1 * t13;                         \
    tp3 += x2 * t14;                         \
    tp3 += x3 * t15;                         \
}

#define DGEMV_T_4x4()              \
{                                  \
    LD_DP2(pa0 + k, 2, t0, t1);    \
    LD_DP2(pa1 + k, 2, t4, t5);    \
    LD_DP2(pa2 + k, 2, t8, t9);    \
    LD_DP2(pa3 + k, 2, t12, t13);  \
                                   \
    tp0 += x0 * t0;                \
    tp0 += x1 * t1;                \
                                   \
    tp1 += x0 * t4;                \
    tp1 += x1 * t5;                \
                                   \
    tp2 += x0 * t8;                \
    tp2 += x1 * t9;                \
                                   \
    tp3 += x0 * t12;               \
    tp3 += x1 * t13;               \
}

#define DGEMV_T_4x2()      \
{                          \
    t0  = LD_DP(pa0 + k);  \
    t4  = LD_DP(pa1 + k);  \
    t8  = LD_DP(pa2 + k);  \
    t12 = LD_DP(pa3 + k);  \
                           \
    tp0 += x0 * t0;        \
    tp1 += x0 * t4;        \
    tp2 += x0 * t8;        \
    tp3 += x0 * t12;       \
}

#define DGEMV_T_2x8()                    \
{                                        \
    LD_DP4(pa0 + k, 2, t0, t1, t2, t3);  \
    LD_DP4(pa1 + k, 2, t4, t5, t6, t7);  \
                                         \
    tp0 += x0 * t0;                      \
    tp0 += x1 * t1;                      \
    tp0 += x2 * t2;                      \
    tp0 += x3 * t3;                      \
                                         \
    tp1 += x0 * t4;                      \
    tp1 += x1 * t5;                      \
    tp1 += x2 * t6;                      \
    tp1 += x3 * t7;                      \
}

#define DGEMV_T_2x4()            \
{                                \
    LD_DP2(pa0 + k, 2, t0, t1);  \
    LD_DP2(pa1 + k, 2, t4, t5);  \
                                 \
    tp0 += x0 * t0;              \
    tp0 += x1 * t1;              \
                                 \
    tp1 += x0 * t4;              \
    tp1 += x1 * t5;              \
}

#define DGEMV_T_2x2()     \
{                         \
    t0 = LD_DP(pa0 + k);  \
    t4 = LD_DP(pa1 + k);  \
                          \
    tp0 += x0 * t0;       \
    tp1 += x0 * t4;       \
}

#define DLOAD_X8_GP()                                                              \
    x0 = (v2f64) __msa_insert_d((v2i64) tp0, 0, *((long long *)(x + 0 * inc_x)));  \
    x0 = (v2f64) __msa_insert_d((v2i64) x0,  1, *((long long *)(x + 1 * inc_x)));  \
    x1 = (v2f64) __msa_insert_d((v2i64) tp0, 0, *((long long *)(x + 2 * inc_x)));  \
    x1 = (v2f64) __msa_insert_d((v2i64) x1,  1, *((long long *)(x + 3 * inc_x)));  \
    x2 = (v2f64) __msa_insert_d((v2i64) tp0, 0, *((long long *)(x + 4 * inc_x)));  \
    x2 = (v2f64) __msa_insert_d((v2i64) x2,  1, *((long long *)(x + 5 * inc_x)));  \
    x3 = (v2f64) __msa_insert_d((v2i64) tp0, 0, *((long long *)(x + 6 * inc_x)));  \
    x3 = (v2f64) __msa_insert_d((v2i64) x3,  1, *((long long *)(x + 7 * inc_x)));  \

#define DLOAD_X4_GP()                                                              \
    x0 = (v2f64) __msa_insert_d((v2i64) tp0, 0, *((long long *)(x + 0 * inc_x)));  \
    x0 = (v2f64) __msa_insert_d((v2i64) x0,  1, *((long long *)(x + 1 * inc_x)));  \
    x1 = (v2f64) __msa_insert_d((v2i64) tp0, 0, *((long long *)(x + 2 * inc_x)));  \
    x1 = (v2f64) __msa_insert_d((v2i64) x1,  1, *((long long *)(x + 3 * inc_x)));  \

#define DLOAD_X2_GP()                                                               \
    x0 = (v2f64) __msa_insert_d((v2i64) tp0,  0, *((long long *)(x + 0 * inc_x)));  \
    x0 = (v2f64) __msa_insert_d((v2i64) x0, 1, *((long long *)(x + 1 * inc_x)));    \

#define DLOAD_X8_VECTOR()  LD_DP4(x, 2, x0, x1, x2, x3);
#define DLOAD_X4_VECTOR()  LD_DP2(x, 2, x0, x1);
#define DLOAD_X2_VECTOR()  x0 = LD_DP(x);

#define DGEMV_T_MSA()                   \
    for (j = (n >> 3); j--;)            \
    {                                   \
        tp0 = zero;                     \
        tp1 = zero;                     \
        tp2 = zero;                     \
        tp3 = zero;                     \
        tp4 = zero;                     \
        tp5 = zero;                     \
        tp6 = zero;                     \
        tp7 = zero;                     \
                                        \
        k = 0;                          \
        x = srcx_org;                   \
                                        \
        for (i = (m >> 3); i--;)        \
        {                               \
            DLOAD_X8();                 \
            DGEMV_T_8x8();              \
                                        \
            x += 8 * inc_x;             \
            k += 8;                     \
        }                               \
                                        \
        if (m & 4)                      \
        {                               \
            DLOAD_X4();                 \
            DGEMV_T_8x4();              \
                                        \
            x += 4 * inc_x;             \
            k += 4;                     \
        }                               \
                                        \
        if (m & 2)                      \
        {                               \
            DLOAD_X2();                 \
            DGEMV_T_8x2();              \
                                        \
            x += 2 * inc_x;             \
            k += 2;                     \
        }                               \
                                        \
        ILVRL_D2_DP(tp1, tp0, t0, t4);  \
        ILVRL_D2_DP(tp3, tp2, t1, t5);  \
        ILVRL_D2_DP(tp5, tp4, t2, t6);  \
        ILVRL_D2_DP(tp7, tp6, t3, t7);  \
        ADD2(t0, t4, t1, t5, t0, t1);   \
        ADD2(t2, t6, t3, t7, t2, t3);   \
                                        \
        temp0 = t0[0];                  \
        temp1 = t0[1];                  \
        temp2 = t1[0];                  \
        temp3 = t1[1];                  \
        temp4 = t2[0];                  \
        temp5 = t2[1];                  \
        temp6 = t3[0];                  \
        temp7 = t3[1];                  \
                                        \
        if (m & 1)                      \
        {                               \
            temp0 += pa0[k] * x[0];     \
            temp1 += pa1[k] * x[0];     \
            temp2 += pa2[k] * x[0];     \
            temp3 += pa3[k] * x[0];     \
            temp4 += pa4[k] * x[0];     \
            temp5 += pa5[k] * x[0];     \
            temp6 += pa6[k] * x[0];     \
            temp7 += pa7[k] * x[0];     \
                                        \
            x += inc_x;                 \
            k++;                        \
        }                               \
                                        \
        res0 = y[0 * inc_y];            \
        res1 = y[1 * inc_y];            \
        res2 = y[2 * inc_y];            \
        res3 = y[3 * inc_y];            \
        res4 = y[4 * inc_y];            \
        res5 = y[5 * inc_y];            \
        res6 = y[6 * inc_y];            \
        res7 = y[7 * inc_y];            \
                                        \
        res0 += alpha * temp0;          \
        res1 += alpha * temp1;          \
        res2 += alpha * temp2;          \
        res3 += alpha * temp3;          \
        res4 += alpha * temp4;          \
        res5 += alpha * temp5;          \
        res6 += alpha * temp6;          \
        res7 += alpha * temp7;          \
                                        \
        y[0 * inc_y] = res0;            \
        y[1 * inc_y] = res1;            \
        y[2 * inc_y] = res2;            \
        y[3 * inc_y] = res3;            \
        y[4 * inc_y] = res4;            \
        y[5 * inc_y] = res5;            \
        y[6 * inc_y] = res6;            \
        y[7 * inc_y] = res7;            \
                                        \
        y += 8 * inc_y;                 \
                                        \
        pa0 += 8 * lda;                 \
        pa1 += 8 * lda;                 \
        pa2 += 8 * lda;                 \
        pa3 += 8 * lda;                 \
        pa4 += 8 * lda;                 \
        pa5 += 8 * lda;                 \
        pa6 += 8 * lda;                 \
        pa7 += 8 * lda;                 \
    }                                   \
                                        \
    if (n & 4)                          \
    {                                   \
        tp0 = zero;                     \
        tp1 = zero;                     \
        tp2 = zero;                     \
        tp3 = zero;                     \
                                        \
        k = 0;                          \
        x = srcx_org;                   \
                                        \
        for (i = (m >> 3); i--;)        \
        {                               \
            DLOAD_X8();                 \
            DGEMV_T_4x8();              \
                                        \
            x += 8 * inc_x;             \
            k += 8;                     \
        }                               \
                                        \
        if (m & 4)                      \
        {                               \
            DLOAD_X4();                 \
            DGEMV_T_4x4();              \
                                        \
            x += 4 * inc_x;             \
            k += 4;                     \
        }                               \
                                        \
        if (m & 2)                      \
        {                               \
            DLOAD_X2();                 \
            DGEMV_T_4x2();              \
                                        \
            x += 2 * inc_x;             \
            k += 2;                     \
        }                               \
                                        \
        ILVRL_D2_DP(tp1, tp0, t0, t4);  \
        ILVRL_D2_DP(tp3, tp2, t1, t5);  \
        ADD2(t0, t4, t1, t5, t0, t1);   \
                                        \
        temp0 = t0[0];                  \
        temp1 = t0[1];                  \
        temp2 = t1[0];                  \
        temp3 = t1[1];                  \
                                        \
        if (m & 1)                      \
        {                               \
            temp0 += pa0[k] * x[0];     \
            temp1 += pa1[k] * x[0];     \
            temp2 += pa2[k] * x[0];     \
            temp3 += pa3[k] * x[0];     \
                                        \
            x += inc_x;                 \
            k++;                        \
        }                               \
                                        \
        res0 = y[0 * inc_y];            \
        res1 = y[1 * inc_y];            \
        res2 = y[2 * inc_y];            \
        res3 = y[3 * inc_y];            \
                                        \
        res0 += alpha * temp0;          \
        res1 += alpha * temp1;          \
        res2 += alpha * temp2;          \
        res3 += alpha * temp3;          \
                                        \
        y[0 * inc_y] = res0;            \
        y[1 * inc_y] = res1;            \
        y[2 * inc_y] = res2;            \
        y[3 * inc_y] = res3;            \
                                        \
        y += 4 * inc_y;                 \
                                        \
        pa0 += 4 * lda;                 \
        pa1 += 4 * lda;                 \
        pa2 += 4 * lda;                 \
        pa3 += 4 * lda;                 \
    }                                   \
                                        \
    if (n & 2)                          \
    {                                   \
        tp0 = zero;                     \
        tp1 = zero;                     \
                                        \
        k = 0;                          \
        x = srcx_org;                   \
                                        \
        for (i = (m >> 3); i--;)        \
        {                               \
            DLOAD_X8();                 \
            DGEMV_T_2x8();              \
                                        \
            x += 8 * inc_x;             \
            k += 8;                     \
        }                               \
                                        \
        if (m & 4)                      \
        {                               \
            DLOAD_X4();                 \
            DGEMV_T_2x4();              \
                                        \
            x += 4 * inc_x;             \
            k += 4;                     \
        }                               \
                                        \
        if (m & 2)                      \
        {                               \
            DLOAD_X2();                 \
            DGEMV_T_2x2();              \
                                        \
            x += 2 * inc_x;             \
            k += 2;                     \
        }                               \
                                        \
        ILVRL_D2_DP(tp1, tp0, t0, t4);  \
                                        \
        t0 += t4;                       \
                                        \
        temp0 = t0[0];                  \
        temp1 = t0[1];                  \
                                        \
        if (m & 1)                      \
        {                               \
            temp0 += pa0[k] * x[0];     \
            temp1 += pa1[k] * x[0];     \
            x += inc_x;                 \
            k++;                        \
        }                               \
                                        \
        res0 = y[0 * inc_y];            \
        res1 = y[1 * inc_y];            \
                                        \
        res0 += alpha * temp0;          \
        res1 += alpha * temp1;          \
                                        \
        y[0 * inc_y] = res0;            \
        y[1 * inc_y] = res1;            \
                                        \
        y += 2 * inc_y;                 \
                                        \
        pa0 += 2 * lda;                 \
        pa1 += 2 * lda;                 \
    }                                   \
                                        \
    if (n & 1)                          \
    {                                   \
        temp0 = 0.0;                    \
                                        \
        k = 0;                          \
        x = srcx_org;                   \
                                        \
        for (i = m; i--;)               \
        {                               \
            temp0 += pa0[k] * x[0];     \
            x += inc_x;                 \
            k++;                        \
        }                               \
                                        \
        y[0] += alpha * temp0;          \
        y += inc_y;                     \
        pa0 += lda;                     \
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
    v2f64 x0, x1, x2, x3;
    v2f64 t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15;
    v2f64 t16, t17, t18, t19, t20, t21, t22, t23, t24, t25, t26, t27, t28, t29;
    v2f64 t30, t31, tp0, tp1, tp2, tp3, tp4, tp5, tp6, tp7;
    v2f64 zero = {0};

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
        #define DLOAD_X8  DLOAD_X8_VECTOR
        #define DLOAD_X4  DLOAD_X4_VECTOR
        #define DLOAD_X2  DLOAD_X2_VECTOR

        DGEMV_T_MSA();

        #undef DLOAD_X8
        #undef DLOAD_X4
        #undef DLOAD_X2
    }
    else
    {
        #define DLOAD_X8  DLOAD_X8_GP
        #define DLOAD_X4  DLOAD_X4_GP
        #define DLOAD_X2  DLOAD_X2_GP

        DGEMV_T_MSA();

        #undef DLOAD_X8
        #undef DLOAD_X4
        #undef DLOAD_X2
    }

    return(0);
}
