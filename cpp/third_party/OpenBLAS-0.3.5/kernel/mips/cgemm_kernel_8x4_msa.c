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

#define CGEMM_KERNEL_8X4_MSA(OP0, OP1, OP2, OP3, OP4)    \
{                                                        \
    LD_SP4_INC(pa0, 4, src_a0, src_a1, src_a2, src_a3);  \
    LD_SP2_INC(pb0, 4, src_b0, src_b1);                  \
                                                         \
    PCKEVOD_W2_SP(src_a1, src_a0, src_a0r, src_a0i);     \
    PCKEVOD_W2_SP(src_a3, src_a2, src_a1r, src_a1i);     \
                                                         \
    /* 0th col */                                        \
    SPLATI_W2_SP(src_b0, 0, src_br, src_bi);             \
    res0_r OP0## = src_a0r * src_br;                     \
    res0_r OP1## = src_a0i * src_bi;                     \
    res0_i OP2## = (OP4 src_a0r) * src_bi;               \
    res0_i OP3## = src_a0i * src_br;                     \
                                                         \
    res1_r OP0## = src_a1r * src_br;                     \
    res1_r OP1## = src_a1i * src_bi;                     \
    res1_i OP2## = (OP4 src_a1r) * src_bi;               \
    res1_i OP3## = src_a1i * src_br;                     \
                                                         \
    /* 1st col */                                        \
    SPLATI_W2_SP(src_b0, 2, src_br, src_bi);             \
    res2_r OP0## = src_a0r * src_br;                     \
    res2_r OP1## = src_a0i * src_bi;                     \
    res2_i OP2## = (OP4 src_a0r) * src_bi;               \
    res2_i OP3## = src_a0i * src_br;                     \
                                                         \
    res3_r OP0## = src_a1r * src_br;                     \
    res3_r OP1## = src_a1i * src_bi;                     \
    res3_i OP2## = (OP4 src_a1r) * src_bi;               \
    res3_i OP3## = src_a1i * src_br;                     \
                                                         \
    /* 2nd col */                                        \
    SPLATI_W2_SP(src_b1, 0, src_br, src_bi);             \
    res4_r OP0## = src_a0r * src_br;                     \
    res4_r OP1## = src_a0i * src_bi;                     \
    res4_i OP2## = (OP4 src_a0r) * src_bi;               \
    res4_i OP3## = src_a0i * src_br;                     \
                                                         \
    res5_r OP0## = src_a1r * src_br;                     \
    res5_r OP1## = src_a1i * src_bi;                     \
    res5_i OP2## = (OP4 src_a1r) * src_bi;               \
    res5_i OP3## = src_a1i * src_br;                     \
                                                         \
    /* 3rd col */                                        \
    SPLATI_W2_SP(src_b1, 2, src_br, src_bi);             \
    res6_r OP0## = src_a0r * src_br;                     \
    res6_r OP1## = src_a0i * src_bi;                     \
    res6_i OP2## = (OP4 src_a0r) * src_bi;               \
    res6_i OP3## = src_a0i * src_br;                     \
                                                         \
    res7_r OP0## = src_a1r * src_br;                     \
    res7_r OP1## = src_a1i * src_bi;                     \
    res7_i OP2## = (OP4 src_a1r) * src_bi;               \
    res7_i OP3## = src_a1i * src_br;                     \
}

#define CGEMM_KERNEL_8X2_MSA(OP0, OP1, OP2, OP3, OP4)    \
{                                                        \
    LD_SP4_INC(pa0, 4, src_a0, src_a1, src_a2, src_a3);  \
    src_b0 = LD_SP(pb0);                                 \
                                                         \
    PCKEVOD_W2_SP(src_a1, src_a0, src_a0r, src_a0i);     \
    PCKEVOD_W2_SP(src_a3, src_a2, src_a1r, src_a1i);     \
                                                         \
    /* 0th col */                                        \
    SPLATI_W2_SP(src_b0, 0, src_br, src_bi);             \
    res0_r OP0## = src_a0r * src_br;                     \
    res0_r OP1## = src_a0i * src_bi;                     \
    res0_i OP2## = (OP4 src_a0r) * src_bi;               \
    res0_i OP3## = src_a0i * src_br;                     \
                                                         \
    res1_r OP0## = src_a1r * src_br;                     \
    res1_r OP1## = src_a1i * src_bi;                     \
    res1_i OP2## = (OP4 src_a1r) * src_bi;               \
    res1_i OP3## = src_a1i * src_br;                     \
                                                         \
    /* 1st col */                                        \
    SPLATI_W2_SP(src_b0, 2, src_br, src_bi);             \
    res2_r OP0## = src_a0r * src_br;                     \
    res2_r OP1## = src_a0i * src_bi;                     \
    res2_i OP2## = (OP4 src_a0r) * src_bi;               \
    res2_i OP3## = src_a0i * src_br;                     \
                                                         \
    res3_r OP0## = src_a1r * src_br;                     \
    res3_r OP1## = src_a1i * src_bi;                     \
    res3_i OP2## = (OP4 src_a1r) * src_bi;               \
    res3_i OP3## = src_a1i * src_br;                     \
}

#define CGEMM_KERNEL_8X1_MSA(OP0, OP1, OP2, OP3, OP4)                 \
{                                                                     \
    LD_SP4_INC(pa0, 4, src_a0, src_a1, src_a2, src_a3);               \
    src_bi = (v4f32) __msa_cast_to_vector_double(*((double *) pb0));  \
    SPLATI_W2_SP(src_bi, 0, src_br, src_bi);                          \
                                                                      \
    PCKEVOD_W2_SP(src_a1, src_a0, src_a0r, src_a0i);                  \
    PCKEVOD_W2_SP(src_a3, src_a2, src_a1r, src_a1i);                  \
                                                                      \
    /* 0th col */                                                     \
    res0_r OP0## = src_a0r * src_br;                                  \
    res0_r OP1## = src_a0i * src_bi;                                  \
    res0_i OP2## = (OP4 src_a0r) * src_bi;                            \
    res0_i OP3## = src_a0i * src_br;                                  \
                                                                      \
    res1_r OP0## = src_a1r * src_br;                                  \
    res1_r OP1## = src_a1i * src_bi;                                  \
    res1_i OP2## = (OP4 src_a1r) * src_bi;                            \
    res1_i OP3## = src_a1i * src_br;                                  \
}

#define CGEMM_KERNEL_4X4_MSA(OP0, OP1, OP2, OP3, OP4)  \
{                                                      \
    LD_SP2_INC(pa0, 4, src_a0, src_a1);                \
    LD_SP2_INC(pb0, 4, src_b0, src_b1);                \
                                                       \
    PCKEVOD_W2_SP(src_a1, src_a0, src_a0r, src_a0i);   \
                                                       \
    /* 0th col */                                      \
    SPLATI_W2_SP(src_b0, 0, src_br, src_bi);           \
    res0_r OP0## = src_a0r * src_br;                   \
    res0_r OP1## = src_a0i * src_bi;                   \
    res0_i OP2## = OP4 src_a0r * src_bi;               \
    res0_i OP3## = src_a0i * src_br;                   \
                                                       \
    /* 1st col */                                      \
    SPLATI_W2_SP(src_b0, 2, src_br, src_bi);           \
    res2_r OP0## = src_a0r * src_br;                   \
    res2_r OP1## = src_a0i * src_bi;                   \
    res2_i OP2## = OP4 src_a0r * src_bi;               \
    res2_i OP3## = src_a0i * src_br;                   \
                                                       \
    /* 2nd col */                                      \
    SPLATI_W2_SP(src_b1, 0, src_br, src_bi);           \
    res4_r OP0## = src_a0r * src_br;                   \
    res4_r OP1## = src_a0i * src_bi;                   \
    res4_i OP2## = OP4 src_a0r * src_bi;               \
    res4_i OP3## = src_a0i * src_br;                   \
                                                       \
    /* 3rd col */                                      \
    SPLATI_W2_SP(src_b1, 2, src_br, src_bi);           \
    res6_r OP0## = src_a0r * src_br;                   \
    res6_r OP1## = src_a0i * src_bi;                   \
    res6_i OP2## = OP4 src_a0r * src_bi;               \
    res6_i OP3## = src_a0i * src_br;                   \
}

#define CGEMM_KERNEL_4X2_MSA(OP0, OP1, OP2, OP3, OP4)  \
{                                                      \
    LD_SP2_INC(pa0, 4, src_a0, src_a1);                \
    src_b0 = LD_SP(pb0);                               \
                                                       \
    PCKEVOD_W2_SP(src_a1, src_a0, src_a0r, src_a0i);   \
                                                       \
    /* 0th col */                                      \
    SPLATI_W2_SP(src_b0, 0, src_br, src_bi);           \
    res0_r OP0## = src_a0r * src_br;                   \
    res0_r OP1## = src_a0i * src_bi;                   \
    res0_i OP2## = OP4 src_a0r * src_bi;               \
    res0_i OP3## = src_a0i * src_br;                   \
                                                       \
    /* 1st col */                                      \
    SPLATI_W2_SP(src_b0, 2, src_br, src_bi);           \
    res2_r OP0## = src_a0r * src_br;                   \
    res2_r OP1## = src_a0i * src_bi;                   \
    res2_i OP2## = OP4 src_a0r * src_bi;               \
    res2_i OP3## = src_a0i * src_br;                   \
}

#define CGEMM_KERNEL_4X1_MSA(OP0, OP1, OP2, OP3, OP4)                 \
{                                                                     \
    LD_SP2_INC(pa0, 4, src_a0, src_a1);                               \
    src_bi = (v4f32) __msa_cast_to_vector_double(*((double *) pb0));  \
    SPLATI_W2_SP(src_bi, 0, src_br, src_bi);                          \
                                                                      \
    PCKEVOD_W2_SP(src_a1, src_a0, src_a0r, src_a0i);                  \
                                                                      \
    /* 0th col */                                                     \
    res0_r OP0## = src_a0r * src_br;                                  \
    res0_r OP1## = src_a0i * src_bi;                                  \
    res0_i OP2## = OP4 src_a0r * src_bi;                              \
    res0_i OP3## = src_a0i * src_br;                                  \
}

#define CGEMM_KERNEL_2X4(OP0, OP1, OP2, OP3, OP4)  \
{                                                  \
    a0_r = pa0[0];                                 \
    a0_i = pa0[1];                                 \
    b0_r = pb0[0];                                 \
    b0_i = pb0[1];                                 \
                                                   \
    res0 OP0## = a0_r * b0_r;                      \
    res0 OP1## = a0_i * b0_i;                      \
    res1 OP2## = OP4 a0_r * b0_i;                  \
    res1 OP3## = a0_i * b0_r;                      \
                                                   \
    a1_r = pa0[2];                                 \
    a1_i = pa0[3];                                 \
    res2 OP0## = a1_r * b0_r;                      \
    res2 OP1## = a1_i * b0_i;                      \
    res3 OP2## = OP4 a1_r * b0_i;                  \
    res3 OP3## = a1_i * b0_r;                      \
                                                   \
    /* 1st col */                                  \
    b1_r = pb0[2];                                 \
    b1_i = pb0[3];                                 \
    res4 OP0## = a0_r * b1_r;                      \
    res4 OP1## = a0_i * b1_i;                      \
    res5 OP2## = OP4 a0_r * b1_i;                  \
    res5 OP3## = a0_i * b1_r;                      \
                                                   \
    res6 OP0## = a1_r * b1_r;                      \
    res6 OP1## = a1_i * b1_i;                      \
    res7 OP2## = OP4 a1_r * b1_i;                  \
    res7 OP3## = a1_i * b1_r;                      \
                                                   \
    /* 2nd col */                                  \
    b2_r = pb0[4];                                 \
    b2_i = pb0[5];                                 \
    res8 OP0## = a0_r * b2_r;                      \
    res8 OP1## = a0_i * b2_i;                      \
    res9 OP2## = OP4 a0_r * b2_i;                  \
    res9 OP3## = a0_i * b2_r;                      \
                                                   \
    res10 OP0## = a1_r * b2_r;                     \
    res10 OP1## = a1_i * b2_i;                     \
    res11 OP2## = OP4 a1_r * b2_i;                 \
    res11 OP3## = a1_i * b2_r;                     \
                                                   \
    /* 3rd col */                                  \
    b3_r = pb0[6];                                 \
    b3_i = pb0[7];                                 \
    res12 OP0## = a0_r * b3_r;                     \
    res12 OP1## = a0_i * b3_i;                     \
    res13 OP2## = OP4 a0_r * b3_i;                 \
    res13 OP3## = a0_i * b3_r;                     \
                                                   \
    res14 OP0## = a1_r * b3_r;                     \
    res14 OP1## = a1_i * b3_i;                     \
    res15 OP2## = OP4 a1_r * b3_i;                 \
    res15 OP3## = a1_i * b3_r;                     \
}

#define CGEMM_KERNEL_2X2(OP0, OP1, OP2, OP3, OP4)  \
{                                                  \
    /* 0th col */                                  \
    a0_r = pa0[0];                                 \
    a0_i = pa0[1];                                 \
    b0_r = pb0[0];                                 \
    b0_i = pb0[1];                                 \
                                                   \
    res0 OP0## = a0_r * b0_r;                      \
    res0 OP1## = a0_i * b0_i;                      \
    res1 OP2## = OP4 a0_r * b0_i;                  \
    res1 OP3## = a0_i * b0_r;                      \
                                                   \
    a1_r = pa0[2];                                 \
    a1_i = pa0[3];                                 \
    res2 OP0## = a1_r * b0_r;                      \
    res2 OP1## = a1_i * b0_i;                      \
    res3 OP2## = OP4 a1_r * b0_i;                  \
    res3 OP3## = a1_i * b0_r;                      \
                                                   \
    /* 1st col */                                  \
    b1_r = pb0[2];                                 \
    b1_i = pb0[3];                                 \
    res4 OP0## = a0_r * b1_r;                      \
    res4 OP1## = a0_i * b1_i;                      \
    res5 OP2## = OP4 a0_r * b1_i;                  \
    res5 OP3## = a0_i * b1_r;                      \
                                                   \
    res6 OP0## = a1_r * b1_r;                      \
    res6 OP1## = a1_i * b1_i;                      \
    res7 OP2## = OP4 a1_r * b1_i;                  \
    res7 OP3## = a1_i * b1_r;                      \
}

#define CGEMM_KERNEL_2X1(OP0, OP1, OP2, OP3, OP4)  \
{                                                  \
    /* 0th col */                                  \
    a0_r = pa0[0];                                 \
    a0_i = pa0[1];                                 \
    b0_r = pb0[0];                                 \
    b0_i = pb0[1];                                 \
                                                   \
    res0 OP0## = a0_r * b0_r;                      \
    res0 OP1## = a0_i * b0_i;                      \
    res1 OP2## = OP4 a0_r * b0_i;                  \
    res1 OP3## = a0_i * b0_r;                      \
                                                   \
    a1_r = pa0[2];                                 \
    a1_i = pa0[3];                                 \
    res2 OP0## = a1_r * b0_r;                      \
    res2 OP1## = a1_i * b0_i;                      \
    res3 OP2## = OP4 a1_r * b0_i;                  \
    res3 OP3## = a1_i * b0_r;                      \
}

#define CGEMM_KERNEL_1X4(OP0, OP1, OP2, OP3, OP4)  \
{                                                  \
    /* 0th col */                                  \
    a0_r = pa0[0];                                 \
    a0_i = pa0[1];                                 \
    b0_r = pb0[0];                                 \
    b0_i = pb0[1];                                 \
                                                   \
    res0 OP0## = a0_r * b0_r;                      \
    res0 OP1## = a0_i * b0_i;                      \
    res1 OP2## = OP4 a0_r * b0_i;                  \
    res1 OP3## = a0_i * b0_r;                      \
                                                   \
    /* 1st col */                                  \
    b1_r = pb0[2];                                 \
    b1_i = pb0[3];                                 \
    res2 OP0## = a0_r * b1_r;                      \
    res2 OP1## = a0_i * b1_i;                      \
    res3 OP2## = OP4 a0_r * b1_i;                  \
    res3 OP3## = a0_i * b1_r;                      \
                                                   \
    /* 2nd col */                                  \
    b2_r = pb0[4];                                 \
    b2_i = pb0[5];                                 \
    res4 OP0## = a0_r * b2_r;                      \
    res4 OP1## = a0_i * b2_i;                      \
    res5 OP2## = OP4 a0_r * b2_i;                  \
    res5 OP3## = a0_i * b2_r;                      \
                                                   \
    /* 3rd col */                                  \
    b3_r = pb0[6];                                 \
    b3_i = pb0[7];                                 \
    res6 OP0## = a0_r * b3_r;                      \
    res6 OP1## = a0_i * b3_i;                      \
    res7 OP2## = OP4 a0_r * b3_i;                  \
    res7 OP3## = a0_i * b3_r;                      \
}

#define CGEMM_KERNEL_1X2(OP0, OP1, OP2, OP3, OP4)  \
{                                                  \
    /* 0th col */                                  \
    a0_r = pa0[0];                                 \
    a0_i = pa0[1];                                 \
    b0_r = pb0[0];                                 \
    b0_i = pb0[1];                                 \
                                                   \
    res0 OP0## = a0_r * b0_r;                      \
    res0 OP1## = a0_i * b0_i;                      \
    res1 OP2## = OP4 a0_r * b0_i;                  \
    res1 OP3## = a0_i * b0_r;                      \
                                                   \
    /* 1st col */                                  \
    b1_r = pb0[2];                                 \
    b1_i = pb0[3];                                 \
    res2 OP0## = a0_r * b1_r;                      \
    res2 OP1## = a0_i * b1_i;                      \
    res3 OP2## = OP4 a0_r * b1_i;                  \
    res3 OP3## = a0_i * b1_r;                      \
}

#define CGEMM_KERNEL_1X1(OP0, OP1, OP2, OP3, OP4)  \
{                                                  \
    /* 0th col */                                  \
    a0_r = pa0[0];                                 \
    a0_i = pa0[1];                                 \
    b0_r = pb0[0];                                 \
    b0_i = pb0[1];                                 \
                                                   \
    res0 OP0## = a0_r * b0_r;                      \
    res0 OP1## = a0_i * b0_i;                      \
    res1 OP2## = OP4 a0_r * b0_i;                  \
    res1 OP3## = a0_i * b0_r;                      \
}

#define CGEMM_SCALE_8X4_MSA                      \
{                                                \
    LD_SP4(pc0, 4, dst0, dst1, dst2, dst3);      \
                                                 \
    PCKEVOD_W2_SP(dst1, dst0, dst0_r, dst0_i);   \
    PCKEVOD_W2_SP(dst3, dst2, dst1_r, dst1_i);   \
                                                 \
    dst0_r += alpha_r * res0_r;                  \
    dst0_r -= alpha_i * res0_i;                  \
    dst0_i += alpha_r * res0_i;                  \
    dst0_i += alpha_i * res0_r;                  \
                                                 \
    dst1_r += alpha_r * res1_r;                  \
    dst1_r -= alpha_i * res1_i;                  \
    dst1_i += alpha_r * res1_i;                  \
    dst1_i += alpha_i * res1_r;                  \
                                                 \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_W2_SP(dst1_i, dst1_r, dst2, dst3);     \
                                                 \
    ST_SP4_INC(dst0, dst1, dst2, dst3, pc0, 4);  \
                                                 \
    LD_SP4(pc1, 4, dst0, dst1, dst2, dst3);      \
                                                 \
    PCKEVOD_W2_SP(dst1, dst0, dst0_r, dst0_i);   \
    PCKEVOD_W2_SP(dst3, dst2, dst1_r, dst1_i);   \
                                                 \
    dst0_r += alpha_r * res2_r;                  \
    dst0_r -= alpha_i * res2_i;                  \
    dst0_i += alpha_r * res2_i;                  \
    dst0_i += alpha_i * res2_r;                  \
                                                 \
    dst1_r += alpha_r * res3_r;                  \
    dst1_r -= alpha_i * res3_i;                  \
    dst1_i += alpha_r * res3_i;                  \
    dst1_i += alpha_i * res3_r;                  \
                                                 \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_W2_SP(dst1_i, dst1_r, dst2, dst3);     \
                                                 \
    ST_SP4_INC(dst0, dst1, dst2, dst3, pc1, 4);  \
                                                 \
    LD_SP4(pc2, 4, dst0, dst1, dst2, dst3);      \
                                                 \
    PCKEVOD_W2_SP(dst1, dst0, dst0_r, dst0_i);   \
    PCKEVOD_W2_SP(dst3, dst2, dst1_r, dst1_i);   \
                                                 \
    dst0_r += alpha_r * res4_r;                  \
    dst0_r -= alpha_i * res4_i;                  \
    dst0_i += alpha_r * res4_i;                  \
    dst0_i += alpha_i * res4_r;                  \
                                                 \
    dst1_r += alpha_r * res5_r;                  \
    dst1_r -= alpha_i * res5_i;                  \
    dst1_i += alpha_r * res5_i;                  \
    dst1_i += alpha_i * res5_r;                  \
                                                 \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_W2_SP(dst1_i, dst1_r, dst2, dst3);     \
                                                 \
    ST_SP4_INC(dst0, dst1, dst2, dst3, pc2, 4);  \
                                                 \
    LD_SP4(pc3, 4, dst0, dst1, dst2, dst3);      \
                                                 \
    PCKEVOD_W2_SP(dst1, dst0, dst0_r, dst0_i);   \
    PCKEVOD_W2_SP(dst3, dst2, dst1_r, dst1_i);   \
                                                 \
    dst0_r += alpha_r * res6_r;                  \
    dst0_r -= alpha_i * res6_i;                  \
    dst0_i += alpha_r * res6_i;                  \
    dst0_i += alpha_i * res6_r;                  \
                                                 \
    dst1_r += alpha_r * res7_r;                  \
    dst1_r -= alpha_i * res7_i;                  \
    dst1_i += alpha_r * res7_i;                  \
    dst1_i += alpha_i * res7_r;                  \
                                                 \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_W2_SP(dst1_i, dst1_r, dst2, dst3);     \
                                                 \
    ST_SP4_INC(dst0, dst1, dst2, dst3, pc3, 4);  \
}

#define CGEMM_SCALE_8X2_MSA                      \
{                                                \
    LD_SP4(pc0, 4, dst0, dst1, dst2, dst3);      \
                                                 \
    PCKEVOD_W2_SP(dst1, dst0, dst0_r, dst0_i);   \
    PCKEVOD_W2_SP(dst3, dst2, dst1_r, dst1_i);   \
                                                 \
    dst0_r += alpha_r * res0_r;                  \
    dst0_r -= alpha_i * res0_i;                  \
    dst0_i += alpha_r * res0_i;                  \
    dst0_i += alpha_i * res0_r;                  \
                                                 \
    dst1_r += alpha_r * res1_r;                  \
    dst1_r -= alpha_i * res1_i;                  \
    dst1_i += alpha_r * res1_i;                  \
    dst1_i += alpha_i * res1_r;                  \
                                                 \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_W2_SP(dst1_i, dst1_r, dst2, dst3);     \
                                                 \
    ST_SP4_INC(dst0, dst1, dst2, dst3, pc0, 4);  \
                                                 \
    LD_SP4(pc1, 4, dst0, dst1, dst2, dst3);      \
                                                 \
    PCKEVOD_W2_SP(dst1, dst0, dst0_r, dst0_i);   \
    PCKEVOD_W2_SP(dst3, dst2, dst1_r, dst1_i);   \
                                                 \
    dst0_r += alpha_r * res2_r;                  \
    dst0_r -= alpha_i * res2_i;                  \
    dst0_i += alpha_r * res2_i;                  \
    dst0_i += alpha_i * res2_r;                  \
                                                 \
    dst1_r += alpha_r * res3_r;                  \
    dst1_r -= alpha_i * res3_i;                  \
    dst1_i += alpha_r * res3_i;                  \
    dst1_i += alpha_i * res3_r;                  \
                                                 \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_W2_SP(dst1_i, dst1_r, dst2, dst3);     \
                                                 \
    ST_SP4_INC(dst0, dst1, dst2, dst3, pc1, 4);  \
}

#define CGEMM_SCALE_8X1_MSA                      \
{                                                \
    LD_SP4(pc0, 4, dst0, dst1, dst2, dst3);      \
                                                 \
    PCKEVOD_W2_SP(dst1, dst0, dst0_r, dst0_i);   \
    PCKEVOD_W2_SP(dst3, dst2, dst1_r, dst1_i);   \
                                                 \
    dst0_r += alpha_r * res0_r;                  \
    dst0_r -= alpha_i * res0_i;                  \
    dst0_i += alpha_r * res0_i;                  \
    dst0_i += alpha_i * res0_r;                  \
                                                 \
    dst1_r += alpha_r * res1_r;                  \
    dst1_r -= alpha_i * res1_i;                  \
    dst1_i += alpha_r * res1_i;                  \
    dst1_i += alpha_i * res1_r;                  \
                                                 \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_W2_SP(dst1_i, dst1_r, dst2, dst3);     \
                                                 \
    ST_SP4_INC(dst0, dst1, dst2, dst3, pc0, 4);  \
}

#define CGEMM_SCALE_4X4_MSA                     \
{                                               \
    LD_SP2(pc0, 4, dst0, dst1);                 \
                                                \
    PCKEVOD_W2_SP(dst1, dst0, dst0_r, dst0_i);  \
                                                \
    dst0_r += alpha_r * res0_r;                 \
    dst0_r -= alpha_i * res0_i;                 \
    dst0_i += alpha_r * res0_i;                 \
    dst0_i += alpha_i * res0_r;                 \
                                                \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);    \
                                                \
    ST_SP2_INC(dst0, dst1, pc0, 4);             \
                                                \
    LD_SP2(pc1, 4, dst0, dst1);                 \
                                                \
    PCKEVOD_W2_SP(dst1, dst0, dst0_r, dst0_i);  \
                                                \
    dst0_r += alpha_r * res2_r;                 \
    dst0_r -= alpha_i * res2_i;                 \
    dst0_i += alpha_r * res2_i;                 \
    dst0_i += alpha_i * res2_r;                 \
                                                \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);    \
                                                \
    ST_SP2_INC(dst0, dst1, pc1, 4);             \
                                                \
    LD_SP2(pc2, 4, dst0, dst1);                 \
                                                \
    PCKEVOD_W2_SP(dst1, dst0, dst0_r, dst0_i);  \
                                                \
    dst0_r += alpha_r * res4_r;                 \
    dst0_r -= alpha_i * res4_i;                 \
    dst0_i += alpha_r * res4_i;                 \
    dst0_i += alpha_i * res4_r;                 \
                                                \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);    \
                                                \
    ST_SP2_INC(dst0, dst1, pc2, 4);             \
                                                \
    LD_SP2(pc3, 4, dst0, dst1);                 \
                                                \
    PCKEVOD_W2_SP(dst1, dst0, dst0_r, dst0_i);  \
                                                \
    dst0_r += alpha_r * res6_r;                 \
    dst0_r -= alpha_i * res6_i;                 \
    dst0_i += alpha_r * res6_i;                 \
    dst0_i += alpha_i * res6_r;                 \
                                                \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);    \
                                                \
    ST_SP2_INC(dst0, dst1, pc3, 4);             \
}

#define CGEMM_SCALE_4X2_MSA                     \
{                                               \
    LD_SP2(pc0, 4, dst0, dst1);                 \
                                                \
    PCKEVOD_W2_SP(dst1, dst0, dst0_r, dst0_i);  \
                                                \
    dst0_r += alpha_r * res0_r;                 \
    dst0_r -= alpha_i * res0_i;                 \
    dst0_i += alpha_r * res0_i;                 \
    dst0_i += alpha_i * res0_r;                 \
                                                \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);    \
                                                \
    ST_SP2_INC(dst0, dst1, pc0, 4);             \
                                                \
    LD_SP2(pc1, 4, dst0, dst1);                 \
                                                \
    PCKEVOD_W2_SP(dst1, dst0, dst0_r, dst0_i);  \
                                                \
    dst0_r += alpha_r * res2_r;                 \
    dst0_r -= alpha_i * res2_i;                 \
    dst0_i += alpha_r * res2_i;                 \
    dst0_i += alpha_i * res2_r;                 \
                                                \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);    \
                                                \
    ST_SP2_INC(dst0, dst1, pc1, 4);             \
}

#define CGEMM_SCALE_4X1_MSA                     \
{                                               \
    LD_SP2(pc0, 4, dst0, dst1);                 \
                                                \
    PCKEVOD_W2_SP(dst1, dst0, dst0_r, dst0_i);  \
                                                \
    dst0_r += alpha_r * res0_r;                 \
    dst0_r -= alpha_i * res0_i;                 \
    dst0_i += alpha_r * res0_i;                 \
    dst0_i += alpha_i * res0_r;                 \
                                                \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);    \
                                                \
    ST_SP2_INC(dst0, dst1, pc0, 4);             \
}

#define CGEMM_SCALE_2X4        \
{                              \
    /* 0th col */              \
    pc0[0] += alphar * res0;   \
    pc0[0] -= alphai * res1;   \
    pc0[1] += alphar * res1;   \
    pc0[1] += alphai * res0;   \
    pc0[2] += alphar * res2;   \
    pc0[2] -= alphai * res3;   \
    pc0[3] += alphar * res3;   \
    pc0[3] += alphai * res2;   \
                               \
    /* 1st col */              \
    pc1[0] += alphar * res4;   \
    pc1[0] -= alphai * res5;   \
    pc1[1] += alphar * res5;   \
    pc1[1] += alphai * res4;   \
    pc1[2] += alphar * res6;   \
    pc1[2] -= alphai * res7;   \
    pc1[3] += alphar * res7;   \
    pc1[3] += alphai * res6;   \
                               \
    /* 2nd col */              \
    pc2[0] += alphar * res8;   \
    pc2[0] -= alphai * res9;   \
    pc2[1] += alphar * res9;   \
    pc2[1] += alphai * res8;   \
    pc2[2] += alphar * res10;  \
    pc2[2] -= alphai * res11;  \
    pc2[3] += alphar * res11;  \
    pc2[3] += alphai * res10;  \
                               \
    /* 3rd col */              \
    pc3[0] += alphar * res12;  \
    pc3[0] -= alphai * res13;  \
    pc3[1] += alphar * res13;  \
    pc3[1] += alphai * res12;  \
    pc3[2] += alphar * res14;  \
    pc3[2] -= alphai * res15;  \
    pc3[3] += alphar * res15;  \
    pc3[3] += alphai * res14;  \
}

#define CGEMM_SCALE_2X2       \
{                             \
    /* 0th col */             \
    pc0[0] += alphar * res0;  \
    pc0[0] -= alphai * res1;  \
    pc0[1] += alphar * res1;  \
    pc0[1] += alphai * res0;  \
    pc0[2] += alphar * res2;  \
    pc0[2] -= alphai * res3;  \
    pc0[3] += alphar * res3;  \
    pc0[3] += alphai * res2;  \
                              \
    /* 1st col */             \
    pc1[0] += alphar * res4;  \
    pc1[0] -= alphai * res5;  \
    pc1[1] += alphar * res5;  \
    pc1[1] += alphai * res4;  \
    pc1[2] += alphar * res6;  \
    pc1[2] -= alphai * res7;  \
    pc1[3] += alphar * res7;  \
    pc1[3] += alphai * res6;  \
}

#define CGEMM_SCALE_2X1       \
{                             \
    pc0[0] += alphar * res0;  \
    pc0[0] -= alphai * res1;  \
    pc0[1] += alphar * res1;  \
    pc0[1] += alphai * res0;  \
                              \
    pc0[2] += alphar * res2;  \
    pc0[2] -= alphai * res3;  \
    pc0[3] += alphar * res3;  \
    pc0[3] += alphai * res2;  \
}

#define CGEMM_SCALE_1X4       \
{                             \
    pc0[0] += alphar * res0;  \
    pc0[0] -= alphai * res1;  \
    pc0[1] += alphar * res1;  \
    pc0[1] += alphai * res0;  \
                              \
    pc1[0] += alphar * res2;  \
    pc1[0] -= alphai * res3;  \
    pc1[1] += alphar * res3;  \
    pc1[1] += alphai * res2;  \
                              \
    pc2[0] += alphar * res4;  \
    pc2[0] -= alphai * res5;  \
    pc2[1] += alphar * res5;  \
    pc2[1] += alphai * res4;  \
                              \
    pc3[0] += alphar * res6;  \
    pc3[0] -= alphai * res7;  \
    pc3[1] += alphar * res7;  \
    pc3[1] += alphai * res6;  \
}

#define CGEMM_SCALE_1X2       \
{                             \
    pc0[0] += alphar * res0;  \
    pc0[0] -= alphai * res1;  \
    pc0[1] += alphar * res1;  \
    pc0[1] += alphai * res0;  \
                              \
    pc1[2] += alphar * res2;  \
    pc1[2] -= alphai * res3;  \
    pc1[3] += alphar * res3;  \
    pc1[3] += alphai * res2;  \
}

#define CGEMM_SCALE_1X1       \
{                             \
    pc0[0] += alphar * res0;  \
    pc0[0] -= alphai * res1;  \
    pc0[1] += alphar * res1;  \
    pc0[1] += alphai * res0;  \
}

#define CGEMM_TRMM_SCALE_8X4_MSA                 \
{                                                \
    dst0_r = alpha_r * res0_r;                   \
    dst0_r -= alpha_i * res0_i;                  \
    dst0_i = alpha_r * res0_i;                   \
    dst0_i += alpha_i * res0_r;                  \
                                                 \
    dst1_r = alpha_r * res1_r;                   \
    dst1_r -= alpha_i * res1_i;                  \
    dst1_i = alpha_r * res1_i;                   \
    dst1_i += alpha_i * res1_r;                  \
                                                 \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_W2_SP(dst1_i, dst1_r, dst2, dst3);     \
                                                 \
    ST_SP4_INC(dst0, dst1, dst2, dst3, pc0, 4);  \
                                                 \
    dst0_r = alpha_r * res2_r;                   \
    dst0_r -= alpha_i * res2_i;                  \
    dst0_i = alpha_r * res2_i;                   \
    dst0_i += alpha_i * res2_r;                  \
                                                 \
    dst1_r = alpha_r * res3_r;                   \
    dst1_r -= alpha_i * res3_i;                  \
    dst1_i = alpha_r * res3_i;                   \
    dst1_i += alpha_i * res3_r;                  \
                                                 \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_W2_SP(dst1_i, dst1_r, dst2, dst3);     \
                                                 \
    ST_SP4_INC(dst0, dst1, dst2, dst3, pc1, 4);  \
                                                 \
    dst0_r = alpha_r * res4_r;                   \
    dst0_r -= alpha_i * res4_i;                  \
    dst0_i = alpha_r * res4_i;                   \
    dst0_i += alpha_i * res4_r;                  \
                                                 \
    dst1_r = alpha_r * res5_r;                   \
    dst1_r -= alpha_i * res5_i;                  \
    dst1_i = alpha_r * res5_i;                   \
    dst1_i += alpha_i * res5_r;                  \
                                                 \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_W2_SP(dst1_i, dst1_r, dst2, dst3);     \
                                                 \
    ST_SP4_INC(dst0, dst1, dst2, dst3, pc2, 4);  \
                                                 \
    dst0_r = alpha_r * res6_r;                   \
    dst0_r -= alpha_i * res6_i;                  \
    dst0_i = alpha_r * res6_i;                   \
    dst0_i += alpha_i * res6_r;                  \
                                                 \
    dst1_r = alpha_r * res7_r;                   \
    dst1_r -= alpha_i * res7_i;                  \
    dst1_i = alpha_r * res7_i;                   \
    dst1_i += alpha_i * res7_r;                  \
                                                 \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_W2_SP(dst1_i, dst1_r, dst2, dst3);     \
                                                 \
    ST_SP4_INC(dst0, dst1, dst2, dst3, pc3, 4);  \
}

#define CGEMM_TRMM_SCALE_8X2_MSA                 \
{                                                \
    dst0_r = alpha_r * res0_r;                   \
    dst0_r -= alpha_i * res0_i;                  \
    dst0_i = alpha_r * res0_i;                   \
    dst0_i += alpha_i * res0_r;                  \
                                                 \
    dst1_r = alpha_r * res1_r;                   \
    dst1_r -= alpha_i * res1_i;                  \
    dst1_i = alpha_r * res1_i;                   \
    dst1_i += alpha_i * res1_r;                  \
                                                 \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_W2_SP(dst1_i, dst1_r, dst2, dst3);     \
                                                 \
    ST_SP4_INC(dst0, dst1, dst2, dst3, pc0, 4);  \
                                                 \
    dst0_r = alpha_r * res2_r;                   \
    dst0_r -= alpha_i * res2_i;                  \
    dst0_i = alpha_r * res2_i;                   \
    dst0_i += alpha_i * res2_r;                  \
                                                 \
    dst1_r = alpha_r * res3_r;                   \
    dst1_r -= alpha_i * res3_i;                  \
    dst1_i = alpha_r * res3_i;                   \
    dst1_i += alpha_i * res3_r;                  \
                                                 \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_W2_SP(dst1_i, dst1_r, dst2, dst3);     \
                                                 \
    ST_SP4_INC(dst0, dst1, dst2, dst3, pc1, 4);  \
}

#define CGEMM_TRMM_SCALE_8X1_MSA                 \
{                                                \
    dst0_r = alpha_r * res0_r;                   \
    dst0_r -= alpha_i * res0_i;                  \
    dst0_i = alpha_r * res0_i;                   \
    dst0_i += alpha_i * res0_r;                  \
                                                 \
    dst1_r = alpha_r * res1_r;                   \
    dst1_r -= alpha_i * res1_i;                  \
    dst1_i = alpha_r * res1_i;                   \
    dst1_i += alpha_i * res1_r;                  \
                                                 \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_W2_SP(dst1_i, dst1_r, dst2, dst3);     \
                                                 \
    ST_SP4_INC(dst0, dst1, dst2, dst3, pc0, 4);  \
}

#define CGEMM_TRMM_SCALE_4X4_MSA              \
{                                             \
    dst0_r = alpha_r * res0_r;                \
    dst0_r -= alpha_i * res0_i;               \
    dst0_i = alpha_r * res0_i;                \
    dst0_i += alpha_i * res0_r;               \
                                              \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);  \
                                              \
    ST_SP2_INC(dst0, dst1, pc0, 4);           \
                                              \
    dst0_r = alpha_r * res2_r;                \
    dst0_r -= alpha_i * res2_i;               \
    dst0_i = alpha_r * res2_i;                \
    dst0_i += alpha_i * res2_r;               \
                                              \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);  \
                                              \
    ST_SP2_INC(dst0, dst1, pc1, 4);           \
                                              \
    dst0_r = alpha_r * res4_r;                \
    dst0_r -= alpha_i * res4_i;               \
    dst0_i = alpha_r * res4_i;                \
    dst0_i += alpha_i * res4_r;               \
                                              \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);  \
                                              \
    ST_SP2_INC(dst0, dst1, pc2, 4);           \
                                              \
    dst0_r = alpha_r * res6_r;                \
    dst0_r -= alpha_i * res6_i;               \
    dst0_i = alpha_r * res6_i;                \
    dst0_i += alpha_i * res6_r;               \
                                              \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);  \
                                              \
    ST_SP2_INC(dst0, dst1, pc3, 4);           \
}

#define CGEMM_TRMM_SCALE_4X2_MSA              \
{                                             \
    dst0_r = alpha_r * res0_r;                \
    dst0_r -= alpha_i * res0_i;               \
    dst0_i = alpha_r * res0_i;                \
    dst0_i += alpha_i * res0_r;               \
                                              \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);  \
                                              \
    ST_SP2_INC(dst0, dst1, pc0, 4);           \
                                              \
    dst0_r = alpha_r * res2_r;                \
    dst0_r -= alpha_i * res2_i;               \
    dst0_i = alpha_r * res2_i;                \
    dst0_i += alpha_i * res2_r;               \
                                              \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);  \
                                              \
    ST_SP2_INC(dst0, dst1, pc1, 4);           \
}

#define CGEMM_TRMM_SCALE_4X1_MSA              \
{                                             \
    dst0_r = alpha_r * res0_r;                \
    dst0_r -= alpha_i * res0_i;               \
    dst0_i = alpha_r * res0_i;                \
    dst0_i += alpha_i * res0_r;               \
                                              \
    ILVRL_W2_SP(dst0_i, dst0_r, dst0, dst1);  \
                                              \
    ST_SP2_INC(dst0, dst1, pc0, 4);           \
}

#define CGEMM_TRMM_SCALE_2X4   \
{                              \
    /* 0th col */              \
    pc0[0] = alphar * res0;    \
    pc0[0] -= alphai * res1;   \
    pc0[1] = alphar * res1;    \
    pc0[1] += alphai * res0;   \
    pc0[2] = alphar * res2;    \
    pc0[2] -= alphai * res3;   \
    pc0[3] = alphar * res3;    \
    pc0[3] += alphai * res2;   \
                               \
    /* 1st col */              \
    pc1[0] = alphar * res4;    \
    pc1[0] -= alphai * res5;   \
    pc1[1] = alphar * res5;    \
    pc1[1] += alphai * res4;   \
    pc1[2] = alphar * res6;    \
    pc1[2] -= alphai * res7;   \
    pc1[3] = alphar * res7;    \
    pc1[3] += alphai * res6;   \
                               \
    /* 2nd col */              \
    pc2[0] = alphar * res8;    \
    pc2[0] -= alphai * res9;   \
    pc2[1] = alphar * res9;    \
    pc2[1] += alphai * res8;   \
    pc2[2] = alphar * res10;   \
    pc2[2] -= alphai * res11;  \
    pc2[3] = alphar * res11;   \
    pc2[3] += alphai * res10;  \
                               \
    /* 3rd col */              \
    pc3[0] = alphar * res12;   \
    pc3[0] -= alphai * res13;  \
    pc3[1] = alphar * res13;   \
    pc3[1] += alphai * res12;  \
    pc3[2] = alphar * res14;   \
    pc3[2] -= alphai * res15;  \
    pc3[3] = alphar * res15;   \
    pc3[3] += alphai * res14;  \
}

#define CGEMM_TRMM_SCALE_2X2  \
{                             \
    /* 0th col */             \
    pc0[0] = alphar * res0;   \
    pc0[0] -= alphai * res1;  \
    pc0[1] = alphar * res1;   \
    pc0[1] += alphai * res0;  \
    pc0[2] = alphar * res2;   \
    pc0[2] -= alphai * res3;  \
    pc0[3] = alphar * res3;   \
    pc0[3] += alphai * res2;  \
                              \
    /* 1st col */             \
    pc1[0] = alphar * res4;   \
    pc1[0] -= alphai * res5;  \
    pc1[1] = alphar * res5;   \
    pc1[1] += alphai * res4;  \
    pc1[2] = alphar * res6;   \
    pc1[2] -= alphai * res7;  \
    pc1[3] = alphar * res7;   \
    pc1[3] += alphai * res6;  \
}

#define CGEMM_TRMM_SCALE_2X1  \
{                             \
    pc0[0] = alphar * res0;   \
    pc0[0] -= alphai * res1;  \
    pc0[1] = alphar * res1;   \
    pc0[1] += alphai * res0;  \
                              \
    pc0[2] = alphar * res2;   \
    pc0[2] -= alphai * res3;  \
    pc0[3] = alphar * res3;   \
    pc0[3] += alphai * res2;  \
}

#define CGEMM_TRMM_SCALE_1X4  \
{                             \
    pc0[0] = alphar * res0;   \
    pc0[0] -= alphai * res1;  \
    pc0[1] = alphar * res1;   \
    pc0[1] += alphai * res0;  \
                              \
    pc1[0] = alphar * res2;   \
    pc1[0] -= alphai * res3;  \
    pc1[1] = alphar * res3;   \
    pc1[1] += alphai * res2;  \
                              \
    pc2[0] = alphar * res4;   \
    pc2[0] -= alphai * res5;  \
    pc2[1] = alphar * res5;   \
    pc2[1] += alphai * res4;  \
                              \
    pc3[0] = alphar * res6;   \
    pc3[0] -= alphai * res7;  \
    pc3[1] = alphar * res7;   \
    pc3[1] += alphai * res6;  \
}

#define CGEMM_TRMM_SCALE_1X2  \
{                             \
    pc0[0] = alphar * res0;   \
    pc0[0] -= alphai * res1;  \
    pc0[1] = alphar * res1;   \
    pc0[1] += alphai * res0;  \
                              \
    pc1[2] = alphar * res2;   \
    pc1[2] -= alphai * res3;  \
    pc1[3] = alphar * res3;   \
    pc1[3] += alphai * res2;  \
}

#define CGEMM_TRMM_SCALE_1X1  \
{                             \
    pc0[0] = alphar * res0;   \
    pc0[0] -= alphai * res1;  \
    pc0[1] = alphar * res1;   \
    pc0[1] += alphai * res0;  \
}

int CNAME(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT alphar, FLOAT alphai,
          FLOAT *A, FLOAT *B, FLOAT *C, BLASLONG ldc
#ifdef TRMMKERNEL
         , BLASLONG offset
#endif
          )
{
    BLASLONG i, j, l, temp;
#if defined(TRMMKERNEL)
    BLASLONG off;
#endif
    FLOAT *pc0, *pc1, *pc2, *pc3, *pa0, *pb0;
    FLOAT res0, res1, res2, res3, res4, res5, res6, res7;
    FLOAT res8, res9, res10, res11, res12, res13, res14, res15;
    FLOAT a0_r, a1_r, a0_i, a1_i, b0_i, b1_i, b2_i, b3_i;
    FLOAT b0_r, b1_r, b2_r, b3_r;
    v4f32 src_a0, src_a1, src_a2, src_a3, src_b0, src_b1;
    v4f32 src_a0r, src_a0i, src_a1r, src_a1i, src_br, src_bi;
    v4f32 dst0, dst1, dst2, dst3, alpha_r, alpha_i;
    v4f32 res0_r, res0_i, res1_r, res1_i, res2_r, res2_i, res3_r, res3_i;
    v4f32 res4_r, res4_i, res5_r, res5_i, res6_r, res6_i, res7_r, res7_i;
    v4f32 dst0_r, dst0_i, dst1_r, dst1_i;

    alpha_r = COPY_FLOAT_TO_VECTOR(alphar);
    alpha_i = COPY_FLOAT_TO_VECTOR(alphai);

#if defined(TRMMKERNEL) && !defined(LEFT)
    off = -offset;
#endif

    for (j = (n >> 2); j--;)
    {
        pc0 = C;
        pc1 = pc0 + 2 * ldc;
        pc2 = pc1 + 2 * ldc;
        pc3 = pc2 + 2 * ldc;

#if defined(TRMMKERNEL) && defined(LEFT)
        off = offset;
#endif

        pa0 = A;

        for (i = (m >> 3); i--;)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 2 * 8;
            pb0 = B + off * 2 * 4;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 8; // number of values in A
#else
            temp = off + 4; // number of values in B
#endif
#else
            pb0 = B;
            temp = k;
#endif

#ifdef ENABLE_PREFETCH
            __asm__ __volatile__(
                "pref   0,   64(%[pa0])   \n\t"
                "pref   0,   96(%[pa0])   \n\t"
                "pref   0,   32(%[pb0])   \n\t"

                :
                : [pa0] "r" (pa0), [pb0] "r" (pb0)
            );
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
            CGEMM_KERNEL_8X4_MSA(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            CGEMM_KERNEL_8X4_MSA(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            CGEMM_KERNEL_8X4_MSA(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            CGEMM_KERNEL_8X4_MSA(, -, , -, -);
#endif

            for (l = (temp - 1); l--;)
            {
#ifdef ENABLE_PREFETCH
                __asm__ __volatile__(
                    "pref   0,   64(%[pa0])   \n\t"
                    "pref   0,   96(%[pa0])   \n\t"
                    "pref   0,   32(%[pb0])   \n\t"

                    :
                    : [pa0] "r" (pa0), [pb0] "r" (pb0)
                );
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                CGEMM_KERNEL_8X4_MSA(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                CGEMM_KERNEL_8X4_MSA(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                CGEMM_KERNEL_8X4_MSA(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                CGEMM_KERNEL_8X4_MSA(+, -, -, -,);
#endif
            }

#if defined(TRMMKERNEL)
            CGEMM_TRMM_SCALE_8X4_MSA
#else
            CGEMM_SCALE_8X4_MSA
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 8; // number of values in A
#else
            temp -= 4; // number of values in B
#endif
            pa0 += temp * 2 * 8;
            pb0 += temp * 2 * 4;
#endif

#ifdef LEFT
            off += 8; // number of values in A
#endif
#endif
        }

        if (m & 4)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 2 * 4;
            pb0 = B + off * 2 * 4;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 4; // number of values in A
#else
            temp = off + 4; // number of values in B
#endif
#else
            pb0 = B;
            temp = k;
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
            CGEMM_KERNEL_4X4_MSA(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            CGEMM_KERNEL_4X4_MSA(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            CGEMM_KERNEL_4X4_MSA(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            CGEMM_KERNEL_4X4_MSA(, -, , -, -);
#endif

            for (l = (temp - 1); l--;)
            {
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                CGEMM_KERNEL_4X4_MSA(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                CGEMM_KERNEL_4X4_MSA(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                CGEMM_KERNEL_4X4_MSA(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                CGEMM_KERNEL_4X4_MSA(+, -, -, -,);
#endif
            }

#if defined(TRMMKERNEL)
            CGEMM_TRMM_SCALE_4X4_MSA
#else
            CGEMM_SCALE_4X4_MSA
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 4; // number of values in A
#else
            temp -= 4; // number of values in B
#endif
            pa0 += temp * 2 * 4;
            pb0 += temp * 2 * 4;
#endif

#ifdef LEFT
            off += 4; // number of values in A
#endif
#endif
        }

        if (m & 2)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 2 * 2;
            pb0 = B + off * 2 * 4;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 2; // number of values in A
#else
            temp = off + 4; // number of values in B
#endif
#else
            pb0 = B;
            temp = k;
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
            CGEMM_KERNEL_2X4(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            CGEMM_KERNEL_2X4(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            CGEMM_KERNEL_2X4(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            CGEMM_KERNEL_2X4(, -, , -, -);
#endif

            pa0 += 4;
            pb0 += 8;

            for (l = (temp - 1); l--;)
            {
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                CGEMM_KERNEL_2X4(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                CGEMM_KERNEL_2X4(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                CGEMM_KERNEL_2X4(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                CGEMM_KERNEL_2X4(+, -, -, -,);
#endif

                pa0 += 4;
                pb0 += 8;
            }

#if defined(TRMMKERNEL)
            CGEMM_TRMM_SCALE_2X4
#else
            CGEMM_SCALE_2X4
#endif
            pc0 += 4;
            pc1 += 4;
            pc2 += 4;
            pc3 += 4;

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 2; // number of values in A
#else
            temp -= 4; // number of values in B
#endif
            pa0 += temp * 2 * 2;
            pb0 += temp * 2 * 4;
#endif

#ifdef LEFT
            off += 2; // number of values in A
#endif
#endif
        }

        if (m & 1)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 2 * 1;
            pb0 = B + off * 2 * 4;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 1; // number of values in A
#else
            temp = off + 4; // number of values in B
#endif
#else
            pb0 = B;
            temp = k;
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
            CGEMM_KERNEL_1X4(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            CGEMM_KERNEL_1X4(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            CGEMM_KERNEL_1X4(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            CGEMM_KERNEL_1X4(, -, , -, -);
#endif

            pa0 += 2;
            pb0 += 8;

            for (l = (temp - 1); l--;)
            {
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                CGEMM_KERNEL_1X4(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                CGEMM_KERNEL_1X4(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                CGEMM_KERNEL_1X4(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                CGEMM_KERNEL_1X4(+, -, -, -,);
#endif

                pa0 += 2;
                pb0 += 8;
            }

#if defined(TRMMKERNEL)
            CGEMM_TRMM_SCALE_1X4
#else
            CGEMM_SCALE_1X4
#endif
            pc0 += 2;
            pc1 += 2;
            pc2 += 2;
            pc3 += 2;

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 1; // number of values in A
#else
            temp -= 4; // number of values in B
#endif
            pa0 += temp * 2 * 1;
            pb0 += temp * 2 * 4;
#endif

#ifdef LEFT
            off += 1; // number of values in A
#endif
#endif
        }

#if defined(TRMMKERNEL) && !defined(LEFT)
        off += 4; // number of values in A
#endif

        B += (k << 3);
        C += (ldc << 3);
    }

    if (n & 2)
    {
        pc0 = C;
        pc1 = pc0 + 2 * ldc;

#if defined(TRMMKERNEL) && defined(LEFT)
        off = offset;
#endif

        pa0 = A;

        for (i = (m >> 3); i--;)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 2 * 8;
            pb0 = B + off * 2 * 2;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 8; // number of values in A
#else
            temp = off + 2; // number of values in B
#endif
#else
            pb0 = B;
            temp = k;
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
            CGEMM_KERNEL_8X2_MSA(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            CGEMM_KERNEL_8X2_MSA(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            CGEMM_KERNEL_8X2_MSA(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            CGEMM_KERNEL_8X2_MSA(, -, , -, -);
#endif

            pb0 += 4;

            for (l = (temp - 1); l--;)
            {
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                CGEMM_KERNEL_8X2_MSA(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                CGEMM_KERNEL_8X2_MSA(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                CGEMM_KERNEL_8X2_MSA(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                CGEMM_KERNEL_8X2_MSA(+, -, -, -,);
#endif

                pb0 += 4;
            }

#if defined(TRMMKERNEL)
            CGEMM_TRMM_SCALE_8X2_MSA
#else
            CGEMM_SCALE_8X2_MSA
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 8; // number of values in A
#else
            temp -= 2; // number of values in B
#endif
            pa0 += temp * 2 * 8;
            pb0 += temp * 2 * 2;
#endif

#ifdef LEFT
            off += 8; // number of values in A
#endif
#endif
        }

        if (m & 4)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 2 * 4;
            pb0 = B + off * 2 * 2;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 4; // number of values in A
#else
            temp = off + 2; // number of values in B
#endif
#else
            pb0 = B;
            temp = k;
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
            CGEMM_KERNEL_4X2_MSA(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            CGEMM_KERNEL_4X2_MSA(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            CGEMM_KERNEL_4X2_MSA(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            CGEMM_KERNEL_4X2_MSA(, -, , -, -);
#endif

            pb0 += 4;

            for (l = (temp - 1); l--;)
            {
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                CGEMM_KERNEL_4X2_MSA(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                CGEMM_KERNEL_4X2_MSA(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                CGEMM_KERNEL_4X2_MSA(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                CGEMM_KERNEL_4X2_MSA(+, -, -, -,);
#endif

                pb0 += 4;
            }

#if defined(TRMMKERNEL)
            CGEMM_TRMM_SCALE_4X2_MSA
#else
            CGEMM_SCALE_4X2_MSA
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 4; // number of values in A
#else
            temp -= 2; // number of values in B
#endif
            pa0 += temp * 2 * 4;
            pb0 += temp * 2 * 2;
#endif

#ifdef LEFT
            off += 4; // number of values in A
#endif
#endif
        }

        if (m & 2)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 2 * 2;
            pb0 = B + off * 2 * 2;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 2; // number of values in A
#else
            temp = off + 2; // number of values in B
#endif
#else
            pb0 = B;
            temp = k;
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
            CGEMM_KERNEL_2X2(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            CGEMM_KERNEL_2X2(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            CGEMM_KERNEL_2X2(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            CGEMM_KERNEL_2X2(, -, , -, -);
#endif

            pa0 += 4;
            pb0 += 4;

            for (l = (temp - 1); l--;)
            {
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                CGEMM_KERNEL_2X2(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                CGEMM_KERNEL_2X2(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                CGEMM_KERNEL_2X2(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                CGEMM_KERNEL_2X2(+, -, -, -,);
#endif

                pa0 += 4;
                pb0 += 4;
            }

#if defined(TRMMKERNEL)
            CGEMM_TRMM_SCALE_2X2
#else
            CGEMM_SCALE_2X2
#endif
            pc0 += 4;
            pc1 += 4;

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 2; // number of values in A
#else
            temp -= 2; // number of values in B
#endif
            pa0 += temp * 2 * 2;
            pb0 += temp * 2 * 2;
#endif

#ifdef LEFT
            off += 2; // number of values in A
#endif
#endif
        }

        if (m & 1)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 2 * 1;
            pb0 = B + off * 2 * 2;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 1; // number of values in A
#else
            temp = off + 2; // number of values in B
#endif
#else
            pb0 = B;
            temp = k;
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
            CGEMM_KERNEL_1X2(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            CGEMM_KERNEL_1X2(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            CGEMM_KERNEL_1X2(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            CGEMM_KERNEL_1X2(, -, , -, -);
#endif

            pa0 += 2;
            pb0 += 4;

            for (l = (temp - 1); l--;)
            {
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                CGEMM_KERNEL_1X2(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                CGEMM_KERNEL_1X2(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                CGEMM_KERNEL_1X2(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                CGEMM_KERNEL_1X2(+, -, -, -,);
#endif

                pa0 += 2;
                pb0 += 4;
            }

#if defined(TRMMKERNEL)
            CGEMM_TRMM_SCALE_1X2
#else
            CGEMM_SCALE_1X2
#endif
            pc0 += 2;
            pc1 += 2;

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 1; // number of values in A
#else
            temp -= 2; // number of values in B
#endif
            pa0 += temp * 2 * 1;
            pb0 += temp * 2 * 2;
#endif

#ifdef LEFT
            off += 1; // number of values in A
#endif
#endif
        }

#if defined(TRMMKERNEL) && !defined(LEFT)
        off += 2; // number of values in A
#endif

        B += (k << 2);
        C += (ldc << 2);
    }

    if (n & 1)
    {
        pc0 = C;

#if defined(TRMMKERNEL) && defined(LEFT)
        off = offset;
#endif

        pa0 = A;

        for (i = (m >> 3); i--;)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 2 * 8;
            pb0 = B + off * 2 * 1;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 8; // number of values in A
#else
            temp = off + 1; // number of values in B
#endif
#else
            pb0 = B;
            temp = k;
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
            CGEMM_KERNEL_8X1_MSA(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            CGEMM_KERNEL_8X1_MSA(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            CGEMM_KERNEL_8X1_MSA(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            CGEMM_KERNEL_8X1_MSA(, -, , -, -);
#endif

            pb0 += 2;

            for (l = (temp - 1); l--;)
            {
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                CGEMM_KERNEL_8X1_MSA(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                CGEMM_KERNEL_8X1_MSA(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                CGEMM_KERNEL_8X1_MSA(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                CGEMM_KERNEL_8X1_MSA(+, -, -, -,);
#endif

                pb0 += 2;
            }

#if defined(TRMMKERNEL)
            CGEMM_TRMM_SCALE_8X1_MSA
#else
            CGEMM_SCALE_8X1_MSA
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 8; // number of values in A
#else
            temp -= 1; // number of values in B
#endif
            pa0 += temp * 2 * 8;
            pb0 += temp * 2 * 1;
#endif

#ifdef LEFT
            off += 8; // number of values in A
#endif
#endif
        }

        if (m & 4)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 2 * 4;
            pb0 = B + off * 2 * 1;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 4; // number of values in A
#else
            temp = off + 1; // number of values in B
#endif
#else
            pb0 = B;
            temp = k;
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
            CGEMM_KERNEL_4X1_MSA(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            CGEMM_KERNEL_4X1_MSA(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            CGEMM_KERNEL_4X1_MSA(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            CGEMM_KERNEL_4X1_MSA(, -, , -, -);
#endif

            pb0 += 2;

            for (l = (temp - 1); l--;)
            {
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                CGEMM_KERNEL_4X1_MSA(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                CGEMM_KERNEL_4X1_MSA(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                CGEMM_KERNEL_4X1_MSA(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                CGEMM_KERNEL_4X1_MSA(+, -, -, -,);
#endif

                pb0 += 2;
            }

#if defined(TRMMKERNEL)
            CGEMM_TRMM_SCALE_4X1_MSA
#else
            CGEMM_SCALE_4X1_MSA
#endif

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 4; // number of values in A
#else
            temp -= 1; // number of values in B
#endif
            pa0 += temp * 2 * 4;
            pb0 += temp * 2 * 1;
#endif

#ifdef LEFT
            off += 4; // number of values in A
#endif
#endif
        }

        if (m & 2)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 2 * 2;
            pb0 = B + off * 2 * 1;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 2; // number of values in A
#else
            temp = off + 1; // number of values in B
#endif
#else
            pb0 = B;
            temp = k;
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
            CGEMM_KERNEL_2X1(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            CGEMM_KERNEL_2X1(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            CGEMM_KERNEL_2X1(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            CGEMM_KERNEL_2X1(, -, , -, -);
#endif

            pa0 += 4;
            pb0 += 2;

            for (l = (temp - 1); l--;)
            {
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                CGEMM_KERNEL_2X1(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                CGEMM_KERNEL_2X1(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                CGEMM_KERNEL_2X1(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                CGEMM_KERNEL_2X1(+, -, -, -,);
#endif

                pa0 += 4;
                pb0 += 2;
            }

#if defined(TRMMKERNEL)
            CGEMM_TRMM_SCALE_2X1
#else
            CGEMM_SCALE_2X1
#endif
            pc0 += 4;

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 2; // number of values in A
#else
            temp -= 1; // number of values in B
#endif
            pa0 += temp * 2 * 2;
            pb0 += temp * 2 * 1;
#endif

#ifdef LEFT
            off += 2; // number of values in A
#endif
#endif
        }

        if (m & 1)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 2 * 1;
            pb0 = B + off * 2 * 1;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 1; // number of values in A
#else
            temp = off + 1; // number of values in B
#endif
#else
            pb0 = B;
            temp = k;
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
            CGEMM_KERNEL_1X1(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            CGEMM_KERNEL_1X1(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            CGEMM_KERNEL_1X1(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            CGEMM_KERNEL_1X1(, -, , -, -);
#endif

            pa0 += 2;
            pb0 += 2;

            for (l = (temp - 1); l--;)
            {
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                CGEMM_KERNEL_1X1(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                CGEMM_KERNEL_1X1(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                CGEMM_KERNEL_1X1(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                CGEMM_KERNEL_1X1(+, -, -, -,);
#endif

                pa0 += 2;
                pb0 += 2;
            }

#if defined(TRMMKERNEL)
            CGEMM_TRMM_SCALE_1X1
#else
            CGEMM_SCALE_1X1
#endif
            pc0 += 2;

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 1; // number of values in A
#else
            temp -= 1; // number of values in B
#endif
            pa0 += temp * 2 * 1;
            pb0 += temp * 2 * 1;
#endif

#ifdef LEFT
            off += 1; // number of values in A
#endif
#endif
        }

#if defined(TRMMKERNEL) && !defined(LEFT)
        off += 1; // number of values in A
#endif

        B += (k << 1);
        C += (ldc << 1);
    }

    return 0;
}
