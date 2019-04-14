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

#if ( !defined(CONJ) && !defined(XCONJ) ) || ( defined(CONJ) && defined(XCONJ) )
    #define OP0  -=
    #define OP1  +=
    #define OP2  +=
#else
    #define OP0  +=
    #define OP1  +=
    #define OP2  -=
#endif

#define ZGEMV_T_8x1()                     \
    LD_DP4(pa0, 2, t0, t1, t2, t3);       \
    LD_DP4(pa0 + 8, 2, t4, t5, t6, t7);   \
                                          \
    PCKEVOD_D2_DP(t1, t0, src0r, src0i);  \
    PCKEVOD_D2_DP(t3, t2, src1r, src1i);  \
    PCKEVOD_D2_DP(t5, t4, src2r, src2i);  \
    PCKEVOD_D2_DP(t7, t6, src3r, src3i);  \
                                          \
    tp0r += src0r * x0r;                  \
    tp0i OP1 src0r * x0i;                 \
    tp0r OP0 src0i * x0i;                 \
    tp0i OP2 src0i * x0r;                 \
                                          \
    tp0r += src2r * x2r;                  \
    tp0i OP1 src2r * x2i;                 \
    tp0r OP0 src2i * x2i;                 \
    tp0i OP2 src2i * x2r;                 \
                                          \
    tp0r += src1r * x1r;                  \
    tp0i OP1 src1r * x1i;                 \
    tp0r OP0 src1i * x1i;                 \
    tp0i OP2 src1i * x1r;                 \
                                          \
    tp0r += src3r * x3r;                  \
    tp0i OP1 src3r * x3i;                 \
    tp0r OP0 src3i * x3i;                 \
    tp0i OP2 src3i * x3r;                 \

#define ZGEMV_T_4x1()                     \
    LD_DP4(pa0, 2, t0, t1, t2, t3);       \
                                          \
    PCKEVOD_D2_DP(t1, t0, src0r, src0i);  \
    PCKEVOD_D2_DP(t3, t2, src1r, src1i);  \
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

#define ZGEMV_T_2x1()                     \
    LD_DP2(pa0, 2, t0, t1);               \
                                          \
    PCKEVOD_D2_DP(t1, t0, src0r, src0i);  \
                                          \
    tp0r += src0r * x0r;                  \
    tp0r OP0 src0i * x0i;                 \
                                          \
    tp0i OP1 src0r * x0i;                 \
    tp0i OP2 src0i * x0r;                 \

#define ZGEMV_T_1x1()                       \
    temp0r  += pa0[0] * x[0 * inc_x2];      \
    temp0r OP0 pa0[1] * x[0 * inc_x2 + 1];  \
                                            \
    temp0i OP1 pa0[0] * x[0 * inc_x2 + 1];  \
    temp0i OP2 pa0[1] * x[0 * inc_x2];      \

#define ZSCALE_STORE_Y1_GP()    \
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

#define ZLOAD_X8_VECTOR()             \
    LD_DP4(x, 2, x0, x1, x2, x3);     \
    LD_DP4(x + 8, 2, x4, x5, x6, x7); \
                                      \
    PCKEVOD_D2_DP(x1, x0, x0r, x0i);  \
    PCKEVOD_D2_DP(x3, x2, x1r, x1i);  \
    PCKEVOD_D2_DP(x5, x4, x2r, x2i);  \
    PCKEVOD_D2_DP(x7, x6, x3r, x3i);  \

#define ZLOAD_X4_VECTOR()             \
    LD_DP4(x, 2, x0, x1, x2, x3);     \
    PCKEVOD_D2_DP(x1, x0, x0r, x0i);  \
    PCKEVOD_D2_DP(x3, x2, x1r, x1i);  \

#define ZLOAD_X2_VECTOR()             \
    LD_DP2(x, 2, x0, x1);             \
    PCKEVOD_D2_DP(x1, x0, x0r, x0i);  \

#define ZLOAD_X8_GP()                                                                      \
    x0r = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((long long *) (x + 0 * inc_x2)));      \
    x0r = (v2f64) __msa_insert_d((v2i64) x0r,  1, *((long long *) (x + 1 * inc_x2)));      \
    x1r = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((long long *) (x + 2 * inc_x2)));      \
    x1r = (v2f64) __msa_insert_d((v2i64) x1r,  1, *((long long *) (x + 3 * inc_x2)));      \
    x2r = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((long long *) (x + 4 * inc_x2)));      \
    x2r = (v2f64) __msa_insert_d((v2i64) x2r,  1, *((long long *) (x + 5 * inc_x2)));      \
    x3r = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((long long *) (x + 6 * inc_x2)));      \
    x3r = (v2f64) __msa_insert_d((v2i64) x3r,  1, *((long long *) (x + 7 * inc_x2)));      \
    x0i = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((long long *) (x + 0 * inc_x2 + 1)));  \
    x0i = (v2f64) __msa_insert_d((v2i64) x0i,  1, *((long long *) (x + 1 * inc_x2 + 1)));  \
    x1i = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((long long *) (x + 2 * inc_x2 + 1)));  \
    x1i = (v2f64) __msa_insert_d((v2i64) x1i,  1, *((long long *) (x + 3 * inc_x2 + 1)));  \
    x2i = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((long long *) (x + 4 * inc_x2 + 1)));  \
    x2i = (v2f64) __msa_insert_d((v2i64) x2i,  1, *((long long *) (x + 5 * inc_x2 + 1)));  \
    x3i = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((long long *) (x + 6 * inc_x2 + 1)));  \
    x3i = (v2f64) __msa_insert_d((v2i64) x3i,  1, *((long long *) (x + 7 * inc_x2 + 1)));  \

#define ZLOAD_X4_GP()                                                                      \
    x0r = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((long long *) (x + 0 * inc_x2)));      \
    x0r = (v2f64) __msa_insert_d((v2i64) x0r,  1, *((long long *) (x + 1 * inc_x2)));      \
    x1r = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((long long *) (x + 2 * inc_x2)));      \
    x1r = (v2f64) __msa_insert_d((v2i64) x1r,  1, *((long long *) (x + 3 * inc_x2)));      \
    x0i = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((long long *) (x + 0 * inc_x2 + 1)));  \
    x0i = (v2f64) __msa_insert_d((v2i64) x0i,  1, *((long long *) (x + 1 * inc_x2 + 1)));  \
    x1i = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((long long *) (x + 2 * inc_x2 + 1)));  \
    x1i = (v2f64) __msa_insert_d((v2i64) x1i,  1, *((long long *) (x + 3 * inc_x2 + 1)));  \

#define ZLOAD_X2_GP()                                                                      \
    x0r = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((long long *) (x + 0 * inc_x2)));      \
    x0r = (v2f64) __msa_insert_d((v2i64) x0r,  1, *((long long *) (x + 1 * inc_x2)));      \
    x0i = (v2f64) __msa_insert_d((v2i64) tp0r, 0, *((long long *) (x + 0 * inc_x2 + 1)));  \
    x0i = (v2f64) __msa_insert_d((v2i64) x0i,  1, *((long long *) (x + 1 * inc_x2 + 1)));  \

#define ZGEMV_T_MSA()                                                   \
    for (j = n; j--;)                                                   \
    {                                                                   \
        tp0r = zero;                                                    \
        tp0i = zero;                                                    \
        tp1r = zero;                                                    \
        tp1i = zero;                                                    \
        tp2r = zero;                                                    \
        tp2i = zero;                                                    \
        tp3r = zero;                                                    \
        tp3i = zero;                                                    \
                                                                        \
        pa0 = A;                                                        \
        x = srcx_org;                                                   \
                                                                        \
        if (m >> 4)                                                     \
        {                                                               \
            x0 = LD_DP(x);                                              \
            x1 = LD_DP(x + 1 * inc_x2);                                 \
            t0 = LD_DP(pa0);                                            \
            t1 = LD_DP(pa0 + 2);                                        \
                                                                        \
            x4 = LD_DP(x + 4 * inc_x2);                                 \
            x5 = LD_DP(x + 5 * inc_x2);                                 \
            t4 = LD_DP(pa0 + 8);                                        \
            t5 = LD_DP(pa0 + 10);                                       \
                                                                        \
            for (i = (m >> 4) - 1; i--;)                                \
            {                                                           \
                pa0_pref = pa0 + pref_offset;                           \
                                                                        \
                PREFETCH(pa0_pref + 36);                                \
                PREFETCH(pa0_pref + 44);                                \
                PREFETCH(pa0_pref + 48);                                \
                PREFETCH(pa0_pref + 52);                                \
                PREFETCH(pa0_pref + 56);                                \
                PREFETCH(pa0_pref + 60);                                \
                PREFETCH(pa0_pref + 64);                                \
                PREFETCH(pa0_pref + 72);                                \
                                                                        \
                x0r = (v2f64) __msa_pckev_d((v2i64) x1, (v2i64) x0);    \
                x0i = (v2f64) __msa_pckod_d((v2i64) x1, (v2i64) x0);    \
                src0r = (v2f64) __msa_pckev_d((v2i64) t1, (v2i64) t0);  \
                src0i = (v2f64) __msa_pckod_d((v2i64) t1, (v2i64) t0);  \
                                                                        \
                tp0r += src0r * x0r;                                    \
                x2 = LD_DP(x + 2 * inc_x2);                             \
                x2r = (v2f64) __msa_pckev_d((v2i64) x5, (v2i64) x4);    \
                                                                        \
                tp0i OP1 src0r * x0i;                                   \
                x3 = LD_DP(x + 3 * inc_x2);                             \
                x2i = (v2f64) __msa_pckod_d((v2i64) x5, (v2i64) x4);    \
                                                                        \
                tp1r OP0 src0i * x0i;                                   \
                t2 = LD_DP(pa0 + 4);                                    \
                src2r = (v2f64) __msa_pckev_d((v2i64) t5, (v2i64) t4);  \
                                                                        \
                tp1i OP2 src0i * x0r;                                   \
                t3 = LD_DP(pa0 + 6);                                    \
                src2i = (v2f64) __msa_pckod_d((v2i64) t5, (v2i64) t4);  \
                                                                        \
                tp2r += src2r * x2r;                                    \
                x6 = LD_DP(x + 6 * inc_x2);                             \
                                                                        \
                tp2i OP1 src2r * x2i;                                   \
                x7 = LD_DP(x + 7 * inc_x2);                             \
                                                                        \
                tp3r OP0 src2i * x2i;                                   \
                t6 = LD_DP(pa0 + 12);                                   \
                                                                        \
                tp3i OP2 src2i * x2r;                                   \
                t7 = LD_DP(pa0 + 14);                                   \
                                                                        \
                x1r = (v2f64) __msa_pckev_d((v2i64) x3, (v2i64) x2);    \
                x1i = (v2f64) __msa_pckod_d((v2i64) x3, (v2i64) x2);    \
                src1r = (v2f64) __msa_pckev_d((v2i64) t3, (v2i64) t2);  \
                src1i = (v2f64) __msa_pckod_d((v2i64) t3, (v2i64) t2);  \
                                                                        \
                tp0r += src1r * x1r;                                    \
                x0 = LD_DP(x +  8 * inc_x2);                            \
                x3r = (v2f64) __msa_pckev_d((v2i64) x7, (v2i64) x6);    \
                                                                        \
                tp0i OP1 src1r * x1i;                                   \
                x1 = LD_DP(x +  9 * inc_x2);                            \
                x3i = (v2f64) __msa_pckod_d((v2i64) x7, (v2i64) x6);    \
                                                                        \
                tp1r OP0 src1i * x1i;                                   \
                t0 = LD_DP(pa0 + 16);                                   \
                src3r = (v2f64) __msa_pckev_d((v2i64) t7, (v2i64) t6);  \
                                                                        \
                tp1i OP2 src1i * x1r;                                   \
                t1 = LD_DP(pa0 + 18);                                   \
                src3i = (v2f64) __msa_pckod_d((v2i64) t7, (v2i64) t6);  \
                                                                        \
                tp2r += src3r * x3r;                                    \
                x4 = LD_DP(x + 12 * inc_x2);                            \
                                                                        \
                tp2i OP1 src3r * x3i;                                   \
                x5 = LD_DP(x + 13 * inc_x2);                            \
                                                                        \
                tp3r OP0 src3i * x3i;                                   \
                t4 = LD_DP(pa0 + 24);                                   \
                                                                        \
                tp3i OP2 src3i * x3r;                                   \
                t5 = LD_DP(pa0 + 26);                                   \
                                                                        \
                x0r = (v2f64) __msa_pckev_d((v2i64) x1, (v2i64) x0);    \
                x0i = (v2f64) __msa_pckod_d((v2i64) x1, (v2i64) x0);    \
                src0r = (v2f64) __msa_pckev_d((v2i64) t1, (v2i64) t0);  \
                src0i = (v2f64) __msa_pckod_d((v2i64) t1, (v2i64) t0);  \
                                                                        \
                tp0r += src0r * x0r;                                    \
                x2 = LD_DP(x + 10 * inc_x2);                            \
                x2r = (v2f64) __msa_pckev_d((v2i64) x5, (v2i64) x4);    \
                                                                        \
                tp0i OP1 src0r * x0i;                                   \
                x3 = LD_DP(x + 11 * inc_x2);                            \
                x2i = (v2f64) __msa_pckod_d((v2i64) x5, (v2i64) x4);    \
                                                                        \
                tp1r OP0 src0i * x0i;                                   \
                t2 = LD_DP(pa0 + 20);                                   \
                src2r = (v2f64) __msa_pckev_d((v2i64) t5, (v2i64) t4);  \
                                                                        \
                tp1i OP2 src0i * x0r;                                   \
                t3 = LD_DP(pa0 + 22);                                   \
                src2i = (v2f64) __msa_pckod_d((v2i64) t5, (v2i64) t4);  \
                                                                        \
                tp2r += src2r * x2r;                                    \
                x6 = LD_DP(x + 14 * inc_x2);                            \
                                                                        \
                tp2i OP1 src2r * x2i;                                   \
                x7 = LD_DP(x + 15 * inc_x2);                            \
                                                                        \
                tp3r OP0 src2i * x2i;                                   \
                t6 = LD_DP(pa0 + 28);                                   \
                                                                        \
                tp3i OP2 src2i * x2r;                                   \
                t7 = LD_DP(pa0 + 30);                                   \
                                                                        \
                x1r = (v2f64) __msa_pckev_d((v2i64) x3, (v2i64) x2);    \
                x1i = (v2f64) __msa_pckod_d((v2i64) x3, (v2i64) x2);    \
                src1r = (v2f64) __msa_pckev_d((v2i64) t3, (v2i64) t2);  \
                src1i = (v2f64) __msa_pckod_d((v2i64) t3, (v2i64) t2);  \
                                                                        \
                tp0r += src1r * x1r;                                    \
                x0 = LD_DP(x + inc_x2 * 16);                            \
                x3r = (v2f64) __msa_pckev_d((v2i64) x7, (v2i64) x6);    \
                                                                        \
                tp0i OP1 src1r * x1i;                                   \
                x1 = LD_DP(x + inc_x2 * 16 + 1 * inc_x2);               \
                x3i = (v2f64) __msa_pckod_d((v2i64) x7, (v2i64) x6);    \
                                                                        \
                tp1r OP0 src1i * x1i;                                   \
                t0 = LD_DP(pa0 + 2 * 16);                               \
                src3r = (v2f64) __msa_pckev_d((v2i64) t7, (v2i64) t6);  \
                                                                        \
                tp1i OP2 src1i * x1r;                                   \
                t1 = LD_DP(pa0 + 2 * 16 + 2);                           \
                src3i = (v2f64) __msa_pckod_d((v2i64) t7, (v2i64) t6);  \
                                                                        \
                tp2r += src3r * x3r;                                    \
                x4 = LD_DP(x + inc_x2 * 16 + 4 * inc_x2);               \
                                                                        \
                tp2i OP1 src3r * x3i;                                   \
                x5 = LD_DP(x + inc_x2 * 16 + 5 * inc_x2);               \
                                                                        \
                tp3r OP0 src3i * x3i;                                   \
                t4 = LD_DP(pa0 + 2 * 16 + 8);                           \
                                                                        \
                tp3i OP2 src3i * x3r;                                   \
                t5 = LD_DP(pa0 + 2 * 16 + 10);                          \
                                                                        \
                pa0 += 2 * 16;                                          \
                x += inc_x2 * 16;                                       \
            }                                                           \
                                                                        \
            x0r = (v2f64) __msa_pckev_d((v2i64) x1, (v2i64) x0);        \
            x0i = (v2f64) __msa_pckod_d((v2i64) x1, (v2i64) x0);        \
            src0r = (v2f64) __msa_pckev_d((v2i64) t1, (v2i64) t0);      \
            src0i = (v2f64) __msa_pckod_d((v2i64) t1, (v2i64) t0);      \
                                                                        \
            tp0r += src0r * x0r;                                        \
            x2 = LD_DP(x + 2 * inc_x2);                                 \
            x2r = (v2f64) __msa_pckev_d((v2i64) x5, (v2i64) x4);        \
                                                                        \
            tp0i OP1 src0r * x0i;                                       \
            x3 = LD_DP(x + 3 * inc_x2);                                 \
            x2i = (v2f64) __msa_pckod_d((v2i64) x5, (v2i64) x4);        \
                                                                        \
            tp1r OP0 src0i * x0i;                                       \
            t2 = LD_DP(pa0 + 4);                                        \
            src2r = (v2f64) __msa_pckev_d((v2i64) t5, (v2i64) t4);      \
                                                                        \
            tp1i OP2 src0i * x0r;                                       \
            t3 = LD_DP(pa0 + 6);                                        \
            src2i = (v2f64) __msa_pckod_d((v2i64) t5, (v2i64) t4);      \
                                                                        \
            tp2r += src2r * x2r;                                        \
            x6 = LD_DP(x + 6 * inc_x2);                                 \
                                                                        \
            tp2i OP1 src2r * x2i;                                       \
            x7 = LD_DP(x + 7 * inc_x2);                                 \
                                                                        \
            tp3r OP0 src2i * x2i;                                       \
            t6 = LD_DP(pa0 + 12);                                       \
                                                                        \
            tp3i OP2 src2i * x2r;                                       \
            t7 = LD_DP(pa0 + 14);                                       \
                                                                        \
            x1r = (v2f64) __msa_pckev_d((v2i64) x3, (v2i64) x2);        \
            x1i = (v2f64) __msa_pckod_d((v2i64) x3, (v2i64) x2);        \
            src1r = (v2f64) __msa_pckev_d((v2i64) t3, (v2i64) t2);      \
            src1i = (v2f64) __msa_pckod_d((v2i64) t3, (v2i64) t2);      \
                                                                        \
            tp0r += src1r * x1r;                                        \
            x0 = LD_DP(x +  8 * inc_x2);                                \
            x3r = (v2f64) __msa_pckev_d((v2i64) x7, (v2i64) x6);        \
                                                                        \
            tp0i OP1 src1r * x1i;                                       \
            x1 = LD_DP(x +  9 * inc_x2);                                \
            x3i = (v2f64) __msa_pckod_d((v2i64) x7, (v2i64) x6);        \
                                                                        \
            tp1r OP0 src1i * x1i;                                       \
            t0 = LD_DP(pa0 + 16);                                       \
            src3r = (v2f64) __msa_pckev_d((v2i64) t7, (v2i64) t6);      \
                                                                        \
            tp1i OP2 src1i * x1r;                                       \
            t1 = LD_DP(pa0 + 18);                                       \
            src3i = (v2f64) __msa_pckod_d((v2i64) t7, (v2i64) t6);      \
                                                                        \
            tp2r += src3r * x3r;                                        \
            x4 = LD_DP(x + 12 * inc_x2);                                \
                                                                        \
            tp2i OP1 src3r * x3i;                                       \
            x5 = LD_DP(x + 13 * inc_x2);                                \
                                                                        \
            tp3r OP0 src3i * x3i;                                       \
            t4 = LD_DP(pa0 + 24);                                       \
                                                                        \
            tp3i OP2 src3i * x3r;                                       \
            t5 = LD_DP(pa0 + 26);                                       \
                                                                        \
            x0r = (v2f64) __msa_pckev_d((v2i64) x1, (v2i64) x0);        \
            x0i = (v2f64) __msa_pckod_d((v2i64) x1, (v2i64) x0);        \
            src0r = (v2f64) __msa_pckev_d((v2i64) t1, (v2i64) t0);      \
            src0i = (v2f64) __msa_pckod_d((v2i64) t1, (v2i64) t0);      \
                                                                        \
            tp0r += src0r * x0r;                                        \
            x2 = LD_DP(x + 10 * inc_x2);                                \
            x2r = (v2f64) __msa_pckev_d((v2i64) x5, (v2i64) x4);        \
                                                                        \
            tp0i OP1 src0r * x0i;                                       \
            x3 = LD_DP(x + 11 * inc_x2);                                \
            x2i = (v2f64) __msa_pckod_d((v2i64) x5, (v2i64) x4);        \
                                                                        \
            tp1r OP0 src0i * x0i;                                       \
            t2 = LD_DP(pa0 + 20);                                       \
            src2r = (v2f64) __msa_pckev_d((v2i64) t5, (v2i64) t4);      \
                                                                        \
            tp1i OP2 src0i * x0r;                                       \
            t3 = LD_DP(pa0 + 22);                                       \
            src2i = (v2f64) __msa_pckod_d((v2i64) t5, (v2i64) t4);      \
                                                                        \
            tp2r += src2r * x2r;                                        \
            x6 = LD_DP(x + 14 * inc_x2);                                \
                                                                        \
            tp2i OP1 src2r * x2i;                                       \
            x7 = LD_DP(x + 15 * inc_x2);                                \
                                                                        \
            tp3r OP0 src2i * x2i;                                       \
            t6 = LD_DP(pa0 + 28);                                       \
                                                                        \
            tp3i OP2 src2i * x2r;                                       \
            t7 = LD_DP(pa0 + 30);                                       \
                                                                        \
            x1r = (v2f64) __msa_pckev_d((v2i64) x3, (v2i64) x2);        \
            x1i = (v2f64) __msa_pckod_d((v2i64) x3, (v2i64) x2);        \
            src1r = (v2f64) __msa_pckev_d((v2i64) t3, (v2i64) t2);      \
            src1i = (v2f64) __msa_pckod_d((v2i64) t3, (v2i64) t2);      \
                                                                        \
            tp0r += src1r * x1r;                                        \
            x3r = (v2f64) __msa_pckev_d((v2i64) x7, (v2i64) x6);        \
                                                                        \
            tp0i OP1 src1r * x1i;                                       \
            x3i = (v2f64) __msa_pckod_d((v2i64) x7, (v2i64) x6);        \
                                                                        \
            tp1r OP0 src1i * x1i;                                       \
            src3r = (v2f64) __msa_pckev_d((v2i64) t7, (v2i64) t6);      \
                                                                        \
            tp1i OP2 src1i * x1r;                                       \
            src3i = (v2f64) __msa_pckod_d((v2i64) t7, (v2i64) t6);      \
                                                                        \
            tp2r += src3r * x3r;                                        \
            tp2i OP1 src3r * x3i;                                       \
            tp3r OP0 src3i * x3i;                                       \
            tp3i OP2 src3i * x3r;                                       \
                                                                        \
            pa0 += 2 * 16;                                              \
            x += inc_x2 * 16;                                           \
                                                                        \
            tp0r += tp1r + tp2r + tp3r;                                 \
            tp0i += tp1i + tp2i + tp3i;                                 \
        }                                                               \
                                                                        \
        if (m & 8)                                                      \
        {                                                               \
            ZLOAD_X8();                                                 \
            ZGEMV_T_8x1();                                              \
                                                                        \
            pa0 += 2 * 8;                                               \
            x += inc_x2 * 8;                                            \
        }                                                               \
                                                                        \
        if (m & 4)                                                      \
        {                                                               \
            ZLOAD_X4();                                                 \
            ZGEMV_T_4x1();                                              \
                                                                        \
            pa0 += 2 * 4;                                               \
            x += inc_x2 * 4;                                            \
        }                                                               \
                                                                        \
        if (m & 2)                                                      \
        {                                                               \
            ZLOAD_X2();                                                 \
            ZGEMV_T_2x1();                                              \
                                                                        \
            pa0 += 2 * 2;                                               \
            x += inc_x2 * 2;                                            \
        }                                                               \
                                                                        \
        temp0r = tp0r[0] + tp0r[1];                                     \
        temp0i = tp0i[0] + tp0i[1];                                     \
                                                                        \
        if (m & 1)                                                      \
        {                                                               \
            ZGEMV_T_1x1();                                              \
                                                                        \
            pa0 += 2;                                                   \
            x += inc_x2;                                                \
        }                                                               \
                                                                        \
        ZSCALE_STORE_Y1_GP();                                           \
                                                                        \
        A += lda2;                                                      \
        y += inc_y2;                                                    \
    }                                                                   \

int CNAME(BLASLONG m, BLASLONG n, BLASLONG dummy1, FLOAT alphar, FLOAT alphai,
          FLOAT *A, BLASLONG lda, FLOAT *x, BLASLONG inc_x, FLOAT *y,
          BLASLONG inc_y, FLOAT *buffer)
{
    BLASLONG i, j, pref_offset;
    BLASLONG inc_x2, inc_y2, lda2;
    FLOAT *pa0, *pa0_pref;
    FLOAT *srcx_org = x;
    FLOAT temp0r, temp0i;
    FLOAT res0r, res0i;
    v2f64 zero = {0};
    v2f64 x0, x1, x2, x3, x0r, x1r, x0i, x1i;
    v2f64 x4, x5, x6, x7, x2r, x3r, x2i, x3i;
    v2f64 t0, t1, t2, t3, t4, t5, t6, t7;
    v2f64 src0r, src1r, src2r, src3r;
    v2f64 src0i, src1i, src2i, src3i;
    v2f64 tp0r, tp1r, tp2r, tp3r, tp0i, tp1i, tp2i, tp3i;

    lda2 = 2 * lda;

    inc_x2 = 2 * inc_x;
    inc_y2 = 2 * inc_y;

    pref_offset = (uintptr_t)A & L1_DATA_LINESIZE;
    pref_offset = L1_DATA_LINESIZE - pref_offset;
    pref_offset = pref_offset / sizeof(FLOAT);

    if (2 == inc_x2)
    {
        #define ZLOAD_X8  ZLOAD_X8_VECTOR
        #define ZLOAD_X4  ZLOAD_X4_VECTOR
        #define ZLOAD_X2  ZLOAD_X2_VECTOR

        ZGEMV_T_MSA();

        #undef ZLOAD_X8
        #undef ZLOAD_X4
        #undef ZLOAD_X2
    }
    else
    {
        #define ZLOAD_X8  ZLOAD_X8_GP
        #define ZLOAD_X4  ZLOAD_X4_GP
        #define ZLOAD_X2  ZLOAD_X2_GP

        ZGEMV_T_MSA();

        #undef ZLOAD_X8
        #undef ZLOAD_X4
        #undef ZLOAD_X2
    }
    return(0);
}

#undef OP0
#undef OP1
#undef OP2
