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

#define SGEMV_N_8x8()              \
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
    y0 += tp0 * t0;                \
    y1 += tp0 * t1;                \
                                   \
    y0 += tp1 * t2;                \
    y1 += tp1 * t3;                \
                                   \
    y0 += tp2 * t4;                \
    y1 += tp2 * t5;                \
                                   \
    y0 += tp3 * t6;                \
    y1 += tp3 * t7;                \
                                   \
    y0 += tp4 * t8;                \
    y1 += tp4 * t9;                \
                                   \
    y0 += tp5 * t10;               \
    y1 += tp5 * t11;               \
                                   \
    y0 += tp6 * t12;               \
    y1 += tp6 * t13;               \
                                   \
    y0 += tp7 * t14;               \
    y1 += tp7 * t15;               \
}

#define SGEMV_N_4x8()      \
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
    y0 += tp0 * t0;        \
    y0 += tp1 * t2;        \
    y0 += tp2 * t4;        \
    y0 += tp3 * t6;        \
    y0 += tp4 * t8;        \
    y0 += tp5 * t10;       \
    y0 += tp6 * t12;       \
    y0 += tp7 * t14;       \
}

#define SGEMV_N_8x4()            \
{                                \
    LD_SP2(pa0 + k, 4, t0, t1);  \
    LD_SP2(pa1 + k, 4, t2, t3);  \
    LD_SP2(pa2 + k, 4, t4, t5);  \
    LD_SP2(pa3 + k, 4, t6, t7);  \
                                 \
    y0 += tp0 * t0;              \
    y1 += tp0 * t1;              \
                                 \
    y0 += tp1 * t2;              \
    y1 += tp1 * t3;              \
                                 \
    y0 += tp2 * t4;              \
    y1 += tp2 * t5;              \
                                 \
    y0 += tp3 * t6;              \
    y1 += tp3 * t7;              \
}

#define SGEMV_N_4x4()      \
{                          \
    t0  = LD_SP(pa0 + k);  \
    t2  = LD_SP(pa1 + k);  \
    t4  = LD_SP(pa2 + k);  \
    t6  = LD_SP(pa3 + k);  \
                           \
    y0 += tp0 * t0;        \
    y0 += tp1 * t2;        \
    y0 += tp2 * t4;        \
    y0 += tp3 * t6;        \
}

#define SGEMV_N_8x2()            \
{                                \
    LD_SP2(pa0 + k, 4, t0, t1);  \
    LD_SP2(pa1 + k, 4, t2, t3);  \
                                 \
    y0 += tp0 * t0;              \
    y1 += tp0 * t1;              \
                                 \
    y0 += tp1 * t2;              \
    y1 += tp1 * t3;              \
}

#define SGEMV_N_4x2()      \
{                          \
    t0  = LD_SP(pa0 + k);  \
    t2  = LD_SP(pa1 + k);  \
                           \
    y0 += tp0 * t0;        \
    y0 += tp1 * t2;        \
}

#define SLOAD_X8_SCALE_GP()             \
    temp0 = alpha * x[0 * inc_x];       \
    temp1 = alpha * x[1 * inc_x];       \
    temp2 = alpha * x[2 * inc_x];       \
    temp3 = alpha * x[3 * inc_x];       \
    temp4 = alpha * x[4 * inc_x];       \
    temp5 = alpha * x[5 * inc_x];       \
    temp6 = alpha * x[6 * inc_x];       \
    temp7 = alpha * x[7 * inc_x];       \
                                        \
    tp0 = COPY_FLOAT_TO_VECTOR(temp0);  \
    tp1 = COPY_FLOAT_TO_VECTOR(temp1);  \
    tp2 = COPY_FLOAT_TO_VECTOR(temp2);  \
    tp3 = COPY_FLOAT_TO_VECTOR(temp3);  \
    tp4 = COPY_FLOAT_TO_VECTOR(temp4);  \
    tp5 = COPY_FLOAT_TO_VECTOR(temp5);  \
    tp6 = COPY_FLOAT_TO_VECTOR(temp6);  \
    tp7 = COPY_FLOAT_TO_VECTOR(temp7);  \

#define SLOAD_X4_SCALE_GP()             \
    temp0 = alpha * x[0 * inc_x];       \
    temp1 = alpha * x[1 * inc_x];       \
    temp2 = alpha * x[2 * inc_x];       \
    temp3 = alpha * x[3 * inc_x];       \
                                        \
    tp0 = COPY_FLOAT_TO_VECTOR(temp0);  \
    tp1 = COPY_FLOAT_TO_VECTOR(temp1);  \
    tp2 = COPY_FLOAT_TO_VECTOR(temp2);  \
    tp3 = COPY_FLOAT_TO_VECTOR(temp3);  \

#define SLOAD_X8_SCALE_VECTOR()            \
    LD_SP2(x, 4, x0, x1);                  \
                                           \
    x0 = x0 * v_alpha;                     \
    x1 = x1 * v_alpha;                     \
                                           \
    SPLATI_W4_SP(x0, tp0, tp1, tp2, tp3);  \
    SPLATI_W4_SP(x1, tp4, tp5, tp6, tp7);  \

#define SLOAD_X4_SCALE_VECTOR()            \
    x0 = LD_SP(x);                         \
    x0 = x0 * v_alpha;                     \
    SPLATI_W4_SP(x0, tp0, tp1, tp2, tp3);  \

#define SLOAD_Y8_GP()                                                        \
    y0 = (v4f32) __msa_insert_w((v4i32) tp0, 0, *((int *)(y + 0 * inc_y)));  \
    y0 = (v4f32) __msa_insert_w((v4i32) y0,  1, *((int *)(y + 1 * inc_y)));  \
    y0 = (v4f32) __msa_insert_w((v4i32) y0,  2, *((int *)(y + 2 * inc_y)));  \
    y0 = (v4f32) __msa_insert_w((v4i32) y0,  3, *((int *)(y + 3 * inc_y)));  \
    y1 = (v4f32) __msa_insert_w((v4i32) tp0, 0, *((int *)(y + 4 * inc_y)));  \
    y1 = (v4f32) __msa_insert_w((v4i32) y1,  1, *((int *)(y + 5 * inc_y)));  \
    y1 = (v4f32) __msa_insert_w((v4i32) y1,  2, *((int *)(y + 6 * inc_y)));  \
    y1 = (v4f32) __msa_insert_w((v4i32) y1,  3, *((int *)(y + 7 * inc_y)));  \

#define SLOAD_Y4_GP()                                                        \
    y0 = (v4f32) __msa_insert_w((v4i32) tp0, 0, *((int *)(y + 0 * inc_y)));  \
    y0 = (v4f32) __msa_insert_w((v4i32) y0,  1, *((int *)(y + 1 * inc_y)));  \
    y0 = (v4f32) __msa_insert_w((v4i32) y0,  2, *((int *)(y + 2 * inc_y)));  \
    y0 = (v4f32) __msa_insert_w((v4i32) y0,  3, *((int *)(y + 3 * inc_y)));  \

#define SLOAD_Y8_VECTOR()  LD_SP2(y, 4, y0, y1);
#define SLOAD_Y4_VECTOR()  y0 = LD_SP(y);

#define SSTORE_Y8_GP()                                          \
    *((int *)(y + 0 * inc_y)) = __msa_copy_s_w((v4i32) y0, 0);  \
    *((int *)(y + 1 * inc_y)) = __msa_copy_s_w((v4i32) y0, 1);  \
    *((int *)(y + 2 * inc_y)) = __msa_copy_s_w((v4i32) y0, 2);  \
    *((int *)(y + 3 * inc_y)) = __msa_copy_s_w((v4i32) y0, 3);  \
    *((int *)(y + 4 * inc_y)) = __msa_copy_s_w((v4i32) y1, 0);  \
    *((int *)(y + 5 * inc_y)) = __msa_copy_s_w((v4i32) y1, 1);  \
    *((int *)(y + 6 * inc_y)) = __msa_copy_s_w((v4i32) y1, 2);  \
    *((int *)(y + 7 * inc_y)) = __msa_copy_s_w((v4i32) y1, 3);  \

#define SSTORE_Y4_GP()                                          \
    *((int *)(y + 0 * inc_y)) = __msa_copy_s_w((v4i32) y0, 0);  \
    *((int *)(y + 1 * inc_y)) = __msa_copy_s_w((v4i32) y0, 1);  \
    *((int *)(y + 2 * inc_y)) = __msa_copy_s_w((v4i32) y0, 2);  \
    *((int *)(y + 3 * inc_y)) = __msa_copy_s_w((v4i32) y0, 3);  \

#define SSTORE_Y8_VECTOR()  ST_SP2(y0, y1, y, 4);
#define SSTORE_Y4_VECTOR()  ST_SP(y0, y);

#define SGEMV_N_MSA()                       \
    for (j = (n >> 3); j--;)                \
    {                                       \
        SLOAD_X8_SCALE();                   \
                                            \
        k = 0;                              \
        y = y_org;                          \
                                            \
        for (i = (m >> 3); i--;)            \
        {                                   \
            SLOAD_Y8();                     \
            SGEMV_N_8x8();                  \
            SSTORE_Y8();                    \
                                            \
            y += 8 * inc_y;                 \
            k += 8;                         \
        }                                   \
                                            \
        if (m & 4)                          \
        {                                   \
            SLOAD_Y4();                     \
            SGEMV_N_4x8();                  \
            SSTORE_Y4();                    \
                                            \
            y += 4 * inc_y;                 \
            k += 4;                         \
        }                                   \
                                            \
        if (m & 3)                          \
        {                                   \
            temp0 = alpha * x[0 * inc_x];   \
            temp1 = alpha * x[1 * inc_x];   \
            temp2 = alpha * x[2 * inc_x];   \
            temp3 = alpha * x[3 * inc_x];   \
            temp4 = alpha * x[4 * inc_x];   \
            temp5 = alpha * x[5 * inc_x];   \
            temp6 = alpha * x[6 * inc_x];   \
            temp7 = alpha * x[7 * inc_x];   \
                                            \
            for (i = (m & 3); i--;)         \
            {                               \
                temp = y[0];                \
                temp += temp0 * pa0[k];     \
                temp += temp1 * pa1[k];     \
                temp += temp2 * pa2[k];     \
                temp += temp3 * pa3[k];     \
                temp += temp4 * pa4[k];     \
                temp += temp5 * pa5[k];     \
                temp += temp6 * pa6[k];     \
                temp += temp7 * pa7[k];     \
                y[0] = temp;                \
                                            \
                y += inc_y;                 \
                k++;                        \
            }                               \
        }                                   \
        pa0 += 8 * lda;                     \
        pa1 += 8 * lda;                     \
        pa2 += 8 * lda;                     \
        pa3 += 8 * lda;                     \
        pa4 += 8 * lda;                     \
        pa5 += 8 * lda;                     \
        pa6 += 8 * lda;                     \
        pa7 += 8 * lda;                     \
                                            \
        x += 8 * inc_x;                     \
    }                                       \
                                            \
    if (n & 4)                              \
    {                                       \
        SLOAD_X4_SCALE();                   \
                                            \
        k = 0;                              \
        y = y_org;                          \
                                            \
        for (i = (m >> 3); i--;)            \
        {                                   \
            SLOAD_Y8();                     \
            SGEMV_N_8x4();                  \
            SSTORE_Y8();                    \
                                            \
            y += 8 * inc_y;                 \
            k += 8;                         \
        }                                   \
                                            \
        if (m & 4)                          \
        {                                   \
            SLOAD_Y4();                     \
            SGEMV_N_4x4();                  \
            SSTORE_Y4();                    \
                                            \
            y += 4 * inc_y;                 \
            k += 4;                         \
        }                                   \
                                            \
        if (m & 3)                          \
        {                                   \
            temp0 = alpha * x[0 * inc_x];   \
            temp1 = alpha * x[1 * inc_x];   \
            temp2 = alpha * x[2 * inc_x];   \
            temp3 = alpha * x[3 * inc_x];   \
                                            \
            for (i = (m & 3); i--;)         \
            {                               \
                temp = y[0];                \
                temp += temp0 * pa0[k];     \
                temp += temp1 * pa1[k];     \
                temp += temp2 * pa2[k];     \
                temp += temp3 * pa3[k];     \
                y[0] = temp;                \
                                            \
                y += inc_y;                 \
                k++;                        \
            }                               \
        }                                   \
                                            \
        pa0 += 4 * lda;                     \
        pa1 += 4 * lda;                     \
        pa2 += 4 * lda;                     \
        pa3 += 4 * lda;                     \
                                            \
        x += 4 * inc_x;                     \
    }                                       \
                                            \
    if (n & 2)                              \
    {                                       \
        temp0 = alpha * x[0 * inc_x];       \
        temp1 = alpha * x[1 * inc_x];       \
                                            \
        tp0 = COPY_FLOAT_TO_VECTOR(temp0);  \
        tp1 = COPY_FLOAT_TO_VECTOR(temp1);  \
                                            \
        k = 0;                              \
        y = y_org;                          \
                                            \
        for (i = (m >> 3); i--;)            \
        {                                   \
            SLOAD_Y8();                     \
            SGEMV_N_8x2();                  \
            SSTORE_Y8();                    \
                                            \
            y += 8 * inc_y;                 \
            k += 8;                         \
        }                                   \
                                            \
        if (m & 4)                          \
        {                                   \
            SLOAD_Y4();                     \
            SGEMV_N_4x2();                  \
            SSTORE_Y4();                    \
                                            \
            y += 4 * inc_y;                 \
            k += 4;                         \
        }                                   \
                                            \
        if (m & 3)                          \
        {                                   \
            temp0 = alpha * x[0 * inc_x];   \
            temp1 = alpha * x[1 * inc_x];   \
                                            \
            for (i = (m & 3); i--;)         \
            {                               \
                temp = y[0];                \
                temp += temp0 * pa0[k];     \
                temp += temp1 * pa1[k];     \
                y[0] = temp;                \
                                            \
                y += inc_y;                 \
                k++;                        \
            }                               \
        }                                   \
                                            \
        pa0 += 2 * lda;                     \
        pa1 += 2 * lda;                     \
                                            \
        x += 2 * inc_x;                     \
    }                                       \
                                            \
    if (n & 1)                              \
    {                                       \
        temp = alpha * x[0];                \
                                            \
        k = 0;                              \
        y = y_org;                          \
                                            \
        for (i = m; i--;)                   \
        {                                   \
           y[0] += temp * pa0[k];           \
                                            \
           y += inc_y;                      \
           k++;                             \
        }                                   \
    }                                       \

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alpha, FLOAT *A,
          BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y,
          FLOAT *buffer)
{
    BLASLONG i, j, k;
    FLOAT *y_org = y;
    FLOAT *pa0, *pa1, *pa2, *pa3, *pa4, *pa5, *pa6, *pa7;
    FLOAT temp, temp0, temp1, temp2, temp3, temp4, temp5, temp6, temp7;
    v4f32 v_alpha, x0, x1, y0 = {0,0,0,0}, y1 = {0,0,0,0};
    v4f32 t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15;
    v4f32 tp0 = {0,0,0,0}, tp1 = {0,0,0,0}, tp2 = {0,0,0,0}, tp3 = {0,0,0,0}, tp4 = {0,0,0,0}, tp5 = {0,0,0,0}, tp6 = {0,0,0,0}, tp7 = {0,0,0,0};

    v_alpha = COPY_FLOAT_TO_VECTOR(alpha);

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
        #define SLOAD_X8_SCALE   SLOAD_X8_SCALE_VECTOR
        #define SLOAD_X4_SCALE   SLOAD_X4_SCALE_VECTOR
        #define SLOAD_Y8   SLOAD_Y8_VECTOR
        #define SLOAD_Y4   SLOAD_Y4_VECTOR
        #define SSTORE_Y8  SSTORE_Y8_VECTOR
        #define SSTORE_Y4  SSTORE_Y4_VECTOR

        SGEMV_N_MSA();

        #undef SLOAD_X8_SCALE
        #undef SLOAD_X4_SCALE
        #undef SLOAD_Y8
        #undef SLOAD_Y4
        #undef SSTORE_Y8
        #undef SSTORE_Y4
    }
    else if (1 == inc_y)
    {
        #define SLOAD_X8_SCALE   SLOAD_X8_SCALE_GP
        #define SLOAD_X4_SCALE   SLOAD_X4_SCALE_GP
        #define SLOAD_Y8   SLOAD_Y8_VECTOR
        #define SLOAD_Y4   SLOAD_Y4_VECTOR
        #define SSTORE_Y8  SSTORE_Y8_VECTOR
        #define SSTORE_Y4  SSTORE_Y4_VECTOR

        SGEMV_N_MSA();

        #undef SLOAD_X8_SCALE
        #undef SLOAD_X4_SCALE
        #undef SLOAD_Y8
        #undef SLOAD_Y4
        #undef SSTORE_Y8
        #undef SSTORE_Y4
    }
    else if (1 == inc_x)
    {
        #define SLOAD_X8_SCALE   SLOAD_X8_SCALE_VECTOR
        #define SLOAD_X4_SCALE   SLOAD_X4_SCALE_VECTOR
        #define SLOAD_Y8   SLOAD_Y8_GP
        #define SLOAD_Y4   SLOAD_Y4_GP
        #define SSTORE_Y8  SSTORE_Y8_GP
        #define SSTORE_Y4  SSTORE_Y4_GP

        SGEMV_N_MSA();

        #undef SLOAD_X8_SCALE
        #undef SLOAD_X4_SCALE
        #undef SLOAD_Y8
        #undef SLOAD_Y4
        #undef SSTORE_Y8
        #undef SSTORE_Y4
    }
    else
    {
        #define SLOAD_X8_SCALE   SLOAD_X8_SCALE_GP
        #define SLOAD_X4_SCALE   SLOAD_X4_SCALE_GP
        #define SLOAD_Y8   SLOAD_Y8_GP
        #define SLOAD_Y4   SLOAD_Y4_GP
        #define SSTORE_Y8  SSTORE_Y8_GP
        #define SSTORE_Y4  SSTORE_Y4_GP

        SGEMV_N_MSA();

        #undef SLOAD_X8_SCALE
        #undef SLOAD_X4_SCALE
        #undef SLOAD_Y8
        #undef SLOAD_Y4
        #undef SSTORE_Y8
        #undef SSTORE_Y4
    }

    return(0);
}
