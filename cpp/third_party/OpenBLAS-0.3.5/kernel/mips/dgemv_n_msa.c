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

#define DGEMV_N_8x8()                        \
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
    y0 += tp0 * t0;                          \
    y1 += tp0 * t1;                          \
    y2 += tp0 * t2;                          \
    y3 += tp0 * t3;                          \
                                             \
    y0 += tp1 * t4;                          \
    y1 += tp1 * t5;                          \
    y2 += tp1 * t6;                          \
    y3 += tp1 * t7;                          \
                                             \
    y0 += tp2 * t8;                          \
    y1 += tp2 * t9;                          \
    y2 += tp2 * t10;                         \
    y3 += tp2 * t11;                         \
                                             \
    y0 += tp3 * t12;                         \
    y1 += tp3 * t13;                         \
    y2 += tp3 * t14;                         \
    y3 += tp3 * t15;                         \
                                             \
    y0 += tp4 * t16;                         \
    y1 += tp4 * t17;                         \
    y2 += tp4 * t18;                         \
    y3 += tp4 * t19;                         \
                                             \
    y0 += tp5 * t20;                         \
    y1 += tp5 * t21;                         \
    y2 += tp5 * t22;                         \
    y3 += tp5 * t23;                         \
                                             \
    y0 += tp6 * t24;                         \
    y1 += tp6 * t25;                         \
    y2 += tp6 * t26;                         \
    y3 += tp6 * t27;                         \
                                             \
    y0 += tp7 * t28;                         \
    y1 += tp7 * t29;                         \
    y2 += tp7 * t30;                         \
    y3 += tp7 * t31;                         \
}

#define DGEMV_N_4x8()              \
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
    y0 += tp0 * t0;                \
    y1 += tp0 * t1;                \
                                   \
    y0 += tp1 * t4;                \
    y1 += tp1 * t5;                \
                                   \
    y0 += tp2 * t8;                \
    y1 += tp2 * t9;                \
                                   \
    y0 += tp3 * t12;               \
    y1 += tp3 * t13;               \
                                   \
    y0 += tp4 * t16;               \
    y1 += tp4 * t17;               \
                                   \
    y0 += tp5 * t20;               \
    y1 += tp5 * t21;               \
                                   \
    y0 += tp6 * t24;               \
    y1 += tp6 * t25;               \
                                   \
    y0 += tp7 * t28;               \
    y1 += tp7 * t29;               \
}

#define DGEMV_N_8x4()                        \
{                                            \
    LD_DP4(pa0 + k, 2, t0, t1, t2, t3);      \
    LD_DP4(pa1 + k, 2, t4, t5, t6, t7);      \
    LD_DP4(pa2 + k, 2, t8, t9, t10, t11);    \
    LD_DP4(pa3 + k, 2, t12, t13, t14, t15);  \
                                             \
    y0 += tp0 * t0;                          \
    y1 += tp0 * t1;                          \
    y2 += tp0 * t2;                          \
    y3 += tp0 * t3;                          \
                                             \
    y0 += tp1 * t4;                          \
    y1 += tp1 * t5;                          \
    y2 += tp1 * t6;                          \
    y3 += tp1 * t7;                          \
                                             \
    y0 += tp2 * t8;                          \
    y1 += tp2 * t9;                          \
    y2 += tp2 * t10;                         \
    y3 += tp2 * t11;                         \
                                             \
    y0 += tp3 * t12;                         \
    y1 += tp3 * t13;                         \
    y2 += tp3 * t14;                         \
    y3 += tp3 * t15;                         \
}

#define DGEMV_N_4x4()              \
{                                  \
    LD_DP2(pa0 + k, 2, t0, t1);    \
    LD_DP2(pa1 + k, 2, t4, t5);    \
    LD_DP2(pa2 + k, 2, t8, t9);    \
    LD_DP2(pa3 + k, 2, t12, t13);  \
                                   \
    y0 += tp0 * t0;                \
    y1 += tp0 * t1;                \
                                   \
    y0 += tp1 * t4;                \
    y1 += tp1 * t5;                \
                                   \
    y0 += tp2 * t8;                \
    y1 += tp2 * t9;                \
                                   \
    y0 += tp3 * t12;               \
    y1 += tp3 * t13;               \
}

#define DGEMV_N_8x2()                    \
{                                        \
    LD_DP4(pa0 + k, 2, t0, t1, t2, t3);  \
    LD_DP4(pa1 + k, 2, t4, t5, t6, t7);  \
                                         \
    y0 += tp0 * t0;                      \
    y1 += tp0 * t1;                      \
    y2 += tp0 * t2;                      \
    y3 += tp0 * t3;                      \
                                         \
    y0 += tp1 * t4;                      \
    y1 += tp1 * t5;                      \
    y2 += tp1 * t6;                      \
    y3 += tp1 * t7;                      \
}

#define DGEMV_N_4x2()            \
{                                \
    LD_DP2(pa0 + k, 2, t0, t1);  \
    LD_DP2(pa1 + k, 2, t4, t5);  \
                                 \
    y0 += tp0 * t0;              \
    y1 += tp0 * t1;              \
                                 \
    y0 += tp1 * t4;              \
    y1 += tp1 * t5;              \
}

#define DLOAD_X8_SCALE_GP()             \
   temp0 = alpha * x[0 * inc_x];        \
   temp1 = alpha * x[1 * inc_x];        \
   temp2 = alpha * x[2 * inc_x];        \
   temp3 = alpha * x[3 * inc_x];        \
   temp4 = alpha * x[4 * inc_x];        \
   temp5 = alpha * x[5 * inc_x];        \
   temp6 = alpha * x[6 * inc_x];        \
   temp7 = alpha * x[7 * inc_x];        \
                                        \
   tp0 = COPY_DOUBLE_TO_VECTOR(temp0);  \
   tp1 = COPY_DOUBLE_TO_VECTOR(temp1);  \
   tp2 = COPY_DOUBLE_TO_VECTOR(temp2);  \
   tp3 = COPY_DOUBLE_TO_VECTOR(temp3);  \
   tp4 = COPY_DOUBLE_TO_VECTOR(temp4);  \
   tp5 = COPY_DOUBLE_TO_VECTOR(temp5);  \
   tp6 = COPY_DOUBLE_TO_VECTOR(temp6);  \
   tp7 = COPY_DOUBLE_TO_VECTOR(temp7);  \

#define  DLOAD_X4_SCALE_GP()             \
    temp0 = alpha * x[0 * inc_x];        \
    temp1 = alpha * x[1 * inc_x];        \
    temp2 = alpha * x[2 * inc_x];        \
    temp3 = alpha * x[3 * inc_x];        \
                                         \
    tp0 = COPY_DOUBLE_TO_VECTOR(temp0);  \
    tp1 = COPY_DOUBLE_TO_VECTOR(temp1);  \
    tp2 = COPY_DOUBLE_TO_VECTOR(temp2);  \
    tp3 = COPY_DOUBLE_TO_VECTOR(temp3);  \

#define DLOAD_X8_SCALE_VECTOR()    \
    LD_DP4(x, 2, x0, x1, x2, x3);  \
                                   \
    x0 = x0 * v_alpha;             \
    x1 = x1 * v_alpha;             \
    x2 = x2 * v_alpha;             \
    x3 = x3 * v_alpha;             \
                                   \
    SPLATI_D2_DP(x0, tp0, tp1);    \
    SPLATI_D2_DP(x1, tp2, tp3);    \
    SPLATI_D2_DP(x2, tp4, tp5);    \
    SPLATI_D2_DP(x3, tp6, tp7);    \

#define DLOAD_X4_SCALE_VECTOR()  \
    LD_DP2(x, 2, x0, x1);        \
                                 \
    x0 = x0 * v_alpha;           \
    x1 = x1 * v_alpha;           \
                                 \
    SPLATI_D2_DP(x0, tp0, tp1);  \
    SPLATI_D2_DP(x1, tp2, tp3);  \

#define DLOAD_Y8_GP()                                                              \
    y0 = (v2f64) __msa_insert_d((v2i64) tp0, 0, *((long long *)(y + 0 * inc_y)));  \
    y0 = (v2f64) __msa_insert_d((v2i64) y0,  1, *((long long *)(y + 1 * inc_y)));  \
    y1 = (v2f64) __msa_insert_d((v2i64) tp0, 0, *((long long *)(y + 2 * inc_y)));  \
    y1 = (v2f64) __msa_insert_d((v2i64) y1,  1, *((long long *)(y + 3 * inc_y)));  \
    y2 = (v2f64) __msa_insert_d((v2i64) tp0, 0, *((long long *)(y + 4 * inc_y)));  \
    y2 = (v2f64) __msa_insert_d((v2i64) y2,  1, *((long long *)(y + 5 * inc_y)));  \
    y3 = (v2f64) __msa_insert_d((v2i64) tp0, 0, *((long long *)(y + 6 * inc_y)));  \
    y3 = (v2f64) __msa_insert_d((v2i64) y3,  1, *((long long *)(y + 7 * inc_y)));  \

#define DLOAD_Y4_GP()                                                              \
    y0 = (v2f64) __msa_insert_d((v2i64) tp0, 0, *((long long *)(y + 0 * inc_y)));  \
    y0 = (v2f64) __msa_insert_d((v2i64) y0,  1, *((long long *)(y + 1 * inc_y)));  \
    y1 = (v2f64) __msa_insert_d((v2i64) tp0, 0, *((long long *)(y + 2 * inc_y)));  \
    y1 = (v2f64) __msa_insert_d((v2i64) y1,  1, *((long long *)(y + 3 * inc_y)));  \

#define DLOAD_Y8_VECTOR()  LD_DP4(y, 2, y0, y1, y2, y3);
#define DLOAD_Y4_VECTOR()  LD_DP2(y, 2, y0, y1);

#define DSTORE_Y8_GP()                                                \
    *((long long *)(y + 0 * inc_y)) = __msa_copy_s_d((v2i64) y0, 0);  \
    *((long long *)(y + 1 * inc_y)) = __msa_copy_s_d((v2i64) y0, 1);  \
    *((long long *)(y + 2 * inc_y)) = __msa_copy_s_d((v2i64) y1, 0);  \
    *((long long *)(y + 3 * inc_y)) = __msa_copy_s_d((v2i64) y1, 1);  \
    *((long long *)(y + 4 * inc_y)) = __msa_copy_s_d((v2i64) y2, 0);  \
    *((long long *)(y + 5 * inc_y)) = __msa_copy_s_d((v2i64) y2, 1);  \
    *((long long *)(y + 6 * inc_y)) = __msa_copy_s_d((v2i64) y3, 0);  \
    *((long long *)(y + 7 * inc_y)) = __msa_copy_s_d((v2i64) y3, 1);  \

#define DSTORE_Y4_GP()                                                \
    *((long long *)(y + 0 * inc_y)) = __msa_copy_s_d((v2i64) y0, 0);  \
    *((long long *)(y + 1 * inc_y)) = __msa_copy_s_d((v2i64) y0, 1);  \
    *((long long *)(y + 2 * inc_y)) = __msa_copy_s_d((v2i64) y1, 0);  \
    *((long long *)(y + 3 * inc_y)) = __msa_copy_s_d((v2i64) y1, 1);  \

#define DSTORE_Y8_VECTOR()  ST_DP4(y0, y1, y2, y3, y, 2);
#define DSTORE_Y4_VECTOR()  ST_DP2(y0, y1, y, 2);

#define DGEMV_N_MSA()                        \
    for (j = (n >> 3); j--;)                 \
    {                                        \
        DLOAD_X8_SCALE();                    \
                                             \
        k = 0;                               \
        y = y_org;                           \
                                             \
        for (i = (m >> 3); i--;)             \
        {                                    \
            DLOAD_Y8();                      \
            DGEMV_N_8x8();                   \
            DSTORE_Y8();                     \
                                             \
            y += 8 * inc_y;                  \
            k += 8;                          \
        }                                    \
                                             \
        if (m & 4)                           \
        {                                    \
            DLOAD_Y4();                      \
            DGEMV_N_4x8();                   \
            DSTORE_Y4();                     \
                                             \
            y += 4 * inc_y;                  \
            k += 4;                          \
        }                                    \
                                             \
        if (m & 3)                           \
        {                                    \
            temp0 = alpha * x[0 * inc_x];    \
            temp1 = alpha * x[1 * inc_x];    \
            temp2 = alpha * x[2 * inc_x];    \
            temp3 = alpha * x[3 * inc_x];    \
            temp4 = alpha * x[4 * inc_x];    \
            temp5 = alpha * x[5 * inc_x];    \
            temp6 = alpha * x[6 * inc_x];    \
            temp7 = alpha * x[7 * inc_x];    \
                                             \
            for (i = (m & 3); i--;)          \
            {                                \
                temp = y[0];                 \
                temp += temp0 * pa0[k];      \
                temp += temp1 * pa1[k];      \
                temp += temp2 * pa2[k];      \
                temp += temp3 * pa3[k];      \
                temp += temp4 * pa4[k];      \
                temp += temp5 * pa5[k];      \
                temp += temp6 * pa6[k];      \
                temp += temp7 * pa7[k];      \
                y[0] = temp;                 \
                                             \
                y += inc_y;                  \
                k++;                         \
            }                                \
        }                                    \
        pa0 += 8 * lda;                      \
        pa1 += 8 * lda;                      \
        pa2 += 8 * lda;                      \
        pa3 += 8 * lda;                      \
        pa4 += 8 * lda;                      \
        pa5 += 8 * lda;                      \
        pa6 += 8 * lda;                      \
        pa7 += 8 * lda;                      \
                                             \
        x += 8 * inc_x;                      \
    }                                        \
                                             \
    if (n & 4)                               \
    {                                        \
        DLOAD_X4_SCALE();                    \
                                             \
        k = 0;                               \
        y = y_org;                           \
                                             \
        for (i = (m >> 3); i--;)             \
        {                                    \
            DLOAD_Y8();                      \
            DGEMV_N_8x4();                   \
            DSTORE_Y8();                     \
                                             \
            y += 8 * inc_y;                  \
            k += 8;                          \
        }                                    \
                                             \
        if (m & 4)                           \
        {                                    \
            DLOAD_Y4();                      \
            DGEMV_N_4x4();                   \
            DSTORE_Y4();                     \
                                             \
            y += 4 * inc_y;                  \
            k += 4;                          \
        }                                    \
                                             \
        if (m & 3)                           \
        {                                    \
            temp0 = alpha * x[0 * inc_x];    \
            temp1 = alpha * x[1 * inc_x];    \
            temp2 = alpha * x[2 * inc_x];    \
            temp3 = alpha * x[3 * inc_x];    \
                                             \
            for (i = (m & 3); i--;)          \
            {                                \
                temp = y[0];                 \
                temp += temp0 * pa0[k];      \
                temp += temp1 * pa1[k];      \
                temp += temp2 * pa2[k];      \
                temp += temp3 * pa3[k];      \
                y[0] = temp;                 \
                                             \
                y += inc_y;                  \
                k++;                         \
            }                                \
        }                                    \
                                             \
        pa0 += 4 * lda;                      \
        pa1 += 4 * lda;                      \
        pa2 += 4 * lda;                      \
        pa3 += 4 * lda;                      \
                                             \
        x += 4 * inc_x;                      \
    }                                        \
                                             \
    if (n & 2)                               \
    {                                        \
        temp0 = alpha * x[0 * inc_x];        \
        temp1 = alpha * x[1 * inc_x];        \
                                             \
        tp0 = COPY_DOUBLE_TO_VECTOR(temp0);  \
        tp1 = COPY_DOUBLE_TO_VECTOR(temp1);  \
                                             \
        k = 0;                               \
        y = y_org;                           \
                                             \
        for (i = (m >> 3); i--;)             \
        {                                    \
            DLOAD_Y8();                      \
            DGEMV_N_8x2();                   \
            DSTORE_Y8();                     \
                                             \
            y += 8 * inc_y;                  \
            k += 8;                          \
        }                                    \
                                             \
        if (m & 4)                           \
        {                                    \
            DLOAD_Y4();                      \
            DGEMV_N_4x2();                   \
            DSTORE_Y4();                     \
                                             \
            y += 4 * inc_y;                  \
            k += 4;                          \
        }                                    \
                                             \
        if (m & 3)                           \
        {                                    \
            temp0 = alpha * x[0 * inc_x];    \
            temp1 = alpha * x[1 * inc_x];    \
                                             \
            for (i = (m & 3); i--;)          \
            {                                \
                temp = y[0];                 \
                temp += temp0 * pa0[k];      \
                temp += temp1 * pa1[k];      \
                y[0] = temp;                 \
                                             \
                y += inc_y;                  \
                k++;                         \
            }                                \
        }                                    \
                                             \
        pa0 += 2 * lda;                      \
        pa1 += 2 * lda;                      \
                                             \
        x += 2 * inc_x;                      \
    }                                        \
                                             \
    if (n & 1)                               \
    {                                        \
        temp = alpha * x[0];                 \
                                             \
        k = 0;                               \
        y = y_org;                           \
                                             \
        for (i = m; i--;)                    \
        {                                    \
           y[0] += temp * pa0[k];            \
           y += inc_y;                       \
           k++;                              \
        }                                    \
    }                                        \

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha, FLOAT *A,
          BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y,
          FLOAT *buffer)
{
    BLASLONG i, j, k;
    FLOAT *y_org = y;
    FLOAT *pa0, *pa1, *pa2, *pa3, *pa4, *pa5, *pa6, *pa7;
    FLOAT temp, temp0, temp1, temp2, temp3, temp4, temp5, temp6, temp7;
    v2f64 v_alpha;
    v2f64 x0, x1, x2, x3, y0 = {0,0}, y1 = {0,0}, y2 = {0,0}, y3 = {0,0};
    v2f64 t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15;
    v2f64 t16, t17, t18, t19, t20, t21, t22, t23, t24, t25, t26, t27, t28, t29;
    v2f64 t30, t31, tp0 = {0,0}, tp1 = {0,0}, tp2 = {0,0}, tp3 = {0,0}, tp4 = {0,0}, tp5 = {0,0}, tp6 = {0,0}, tp7 = {0,0};

    v_alpha = COPY_DOUBLE_TO_VECTOR(alpha);

    pa0 = A;
    pa1 = A + lda;
    pa2 = A + 2 * lda;
    pa3 = A + 3 * lda;
    pa4 = A + 4 * lda;
    pa5 = A + 5 * lda;
    pa6 = A + 6 * lda;
    pa7 = A + 7 * lda;

    if ((1 == inc_x) && (1 == inc_y))
    {
        #define DLOAD_X8_SCALE   DLOAD_X8_SCALE_VECTOR
        #define DLOAD_X4_SCALE   DLOAD_X4_SCALE_VECTOR
        #define DLOAD_Y8   DLOAD_Y8_VECTOR
        #define DLOAD_Y4   DLOAD_Y4_VECTOR
        #define DSTORE_Y8  DSTORE_Y8_VECTOR
        #define DSTORE_Y4  DSTORE_Y4_VECTOR

        DGEMV_N_MSA();

        #undef DLOAD_X8_SCALE
        #undef DLOAD_X4_SCALE
        #undef DLOAD_Y8
        #undef DLOAD_Y4
        #undef DSTORE_Y8
        #undef DSTORE_Y4
    }
    else if (1 == inc_y)
    {
        #define DLOAD_X8_SCALE   DLOAD_X8_SCALE_GP
        #define DLOAD_X4_SCALE   DLOAD_X4_SCALE_GP
        #define DLOAD_Y8   DLOAD_Y8_VECTOR
        #define DLOAD_Y4   DLOAD_Y4_VECTOR
        #define DSTORE_Y8  DSTORE_Y8_VECTOR
        #define DSTORE_Y4  DSTORE_Y4_VECTOR

        DGEMV_N_MSA();

        #undef DLOAD_X8_SCALE
        #undef DLOAD_X4_SCALE
        #undef DLOAD_Y8
        #undef DLOAD_Y4
        #undef DSTORE_Y8
        #undef DSTORE_Y4
    }
    else if (1 == inc_x)
    {
        #define DLOAD_X8_SCALE   DLOAD_X8_SCALE_VECTOR
        #define DLOAD_X4_SCALE   DLOAD_X4_SCALE_VECTOR
        #define DLOAD_Y8   DLOAD_Y8_GP
        #define DLOAD_Y4   DLOAD_Y4_GP
        #define DSTORE_Y8  DSTORE_Y8_GP
        #define DSTORE_Y4  DSTORE_Y4_GP

        DGEMV_N_MSA();

        #undef DLOAD_X8_SCALE
        #undef DLOAD_X4_SCALE
        #undef DLOAD_Y8
        #undef DLOAD_Y4
        #undef DSTORE_Y8
        #undef DSTORE_Y4
    }
    else
    {
        #define DLOAD_X8_SCALE   DLOAD_X8_SCALE_GP
        #define DLOAD_X4_SCALE   DLOAD_X4_SCALE_GP
        #define DLOAD_Y8   DLOAD_Y8_GP
        #define DLOAD_Y4   DLOAD_Y4_GP
        #define DSTORE_Y8  DSTORE_Y8_GP
        #define DSTORE_Y4  DSTORE_Y4_GP

        DGEMV_N_MSA();

        #undef DLOAD_X8_SCALE
        #undef DLOAD_X4_SCALE
        #undef DLOAD_Y8
        #undef DLOAD_Y4
        #undef DSTORE_Y8
        #undef DSTORE_Y4
    }

    return(0);
}
