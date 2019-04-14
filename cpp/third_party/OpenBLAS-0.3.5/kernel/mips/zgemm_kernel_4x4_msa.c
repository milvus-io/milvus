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

#define ZGEMM_KERNEL_4X4_MSA(OP0, OP1, OP2, OP3, OP4)    \
{                                                        \
    LD_DP4_INC(pa0, 2, src_a0, src_a1, src_a2, src_a3);  \
    LD_DP4_INC(pb0, 2, src_b0, src_b1, src_b2, src_b3);  \
                                                         \
    PCKEVOD_D2_DP(src_a1, src_a0, src_a0r, src_a0i);     \
    PCKEVOD_D2_DP(src_a3, src_a2, src_a1r, src_a1i);     \
                                                         \
    /* 0th col */                                        \
    SPLATI_D2_DP(src_b0, src_br, src_bi);                \
    res0_r OP0## = src_a0r * src_br;                     \
    res0_r OP1## = src_a0i * src_bi;                     \
    res0_i OP2## = OP4 src_a0r * src_bi;                 \
    res0_i OP3## = src_a0i * src_br;                     \
                                                         \
    res1_r OP0## = src_a1r * src_br;                     \
    res1_r OP1## = src_a1i * src_bi;                     \
    res1_i OP2## = OP4 src_a1r * src_bi;                 \
    res1_i OP3## = src_a1i * src_br;                     \
                                                         \
    /* 1st col */                                        \
    SPLATI_D2_DP(src_b1, src_br, src_bi);                \
    res2_r OP0## = src_a0r * src_br;                     \
    res2_r OP1## = src_a0i * src_bi;                     \
    res2_i OP2## = OP4 src_a0r * src_bi;                 \
    res2_i OP3## = src_a0i * src_br;                     \
                                                         \
    res3_r OP0## = src_a1r * src_br;                     \
    res3_r OP1## = src_a1i * src_bi;                     \
    res3_i OP2## = OP4 src_a1r * src_bi;                 \
    res3_i OP3## = src_a1i * src_br;                     \
                                                         \
    /* 2nd col */                                        \
    SPLATI_D2_DP(src_b2, src_br, src_bi);                \
    res4_r OP0## = src_a0r * src_br;                     \
    res4_r OP1## = src_a0i * src_bi;                     \
    res4_i OP2## = OP4 src_a0r * src_bi;                 \
    res4_i OP3## = src_a0i * src_br;                     \
                                                         \
    res5_r OP0## = src_a1r * src_br;                     \
    res5_r OP1## = src_a1i * src_bi;                     \
    res5_i OP2## = OP4 src_a1r * src_bi;                 \
    res5_i OP3## = src_a1i * src_br;                     \
                                                         \
    /* 3rd col */                                        \
    SPLATI_D2_DP(src_b3, src_br, src_bi);                \
    res6_r OP0## = src_a0r * src_br;                     \
    res6_r OP1## = src_a0i * src_bi;                     \
    res6_i OP2## = OP4 src_a0r * src_bi;                 \
    res6_i OP3## = src_a0i * src_br;                     \
                                                         \
    res7_r OP0## = src_a1r * src_br;                     \
    res7_r OP1## = src_a1i * src_bi;                     \
    res7_i OP2## = OP4 src_a1r * src_bi;                 \
    res7_i OP3## = src_a1i * src_br;                     \
}

#define ZGEMM_KERNEL_2X4_MSA(OP0, OP1, OP2, OP3, OP4)    \
{                                                        \
    LD_DP2_INC(pa0, 2, src_a0, src_a1);                  \
    LD_DP4_INC(pb0, 2, src_b0, src_b1, src_b2, src_b3);  \
                                                         \
    PCKEVOD_D2_DP(src_a1, src_a0, src_a0r, src_a0i);     \
                                                         \
    /* 0th col */                                        \
    SPLATI_D2_DP(src_b0, src_br, src_bi);                \
    res0_r OP0## = src_a0r * src_br;                     \
    res0_r OP1## = src_a0i * src_bi;                     \
    res0_i OP2## = OP4 src_a0r * src_bi;                 \
    res0_i OP3## = src_a0i * src_br;                     \
                                                         \
    /* 1st col */                                        \
    SPLATI_D2_DP(src_b1, src_br, src_bi);                \
    res2_r OP0## = src_a0r * src_br;                     \
    res2_r OP1## = src_a0i * src_bi;                     \
    res2_i OP2## = OP4 src_a0r * src_bi;                 \
    res2_i OP3## = src_a0i * src_br;                     \
                                                         \
    /* 2nd col */                                        \
    SPLATI_D2_DP(src_b2, src_br, src_bi);                \
    res4_r OP0## = src_a0r * src_br;                     \
    res4_r OP1## = src_a0i * src_bi;                     \
    res4_i OP2## = OP4 src_a0r * src_bi;                 \
    res4_i OP3## = src_a0i * src_br;                     \
                                                         \
    /* 3rd col */                                        \
    SPLATI_D2_DP(src_b3, src_br, src_bi);                \
    res6_r OP0## = src_a0r * src_br;                     \
    res6_r OP1## = src_a0i * src_bi;                     \
    res6_i OP2## = OP4 src_a0r * src_bi;                 \
    res6_i OP3## = src_a0i * src_br;                     \
}

#define ZGEMM_KERNEL_1X4_MSA(OP0, OP1, OP2, OP3, OP4)    \
{                                                        \
    src_a0 = LD_DP(pa0);                                 \
    LD_DP4_INC(pb0, 2, src_b0, src_b1, src_b2, src_b3);  \
                                                         \
    PCKEVOD_D2_DP(src_a0, src_a0, src_a0r, src_a0i);     \
                                                         \
    /* 0th and 1st col */                                \
    PCKEVOD_D2_DP(src_b1, src_b0, src_br, src_bi);       \
    res0_r OP0## = src_a0r * src_br;                     \
    res0_r OP1## = src_a0i * src_bi;                     \
    res0_i OP2## = OP4 src_a0r * src_bi;                 \
    res0_i OP3## = src_a0i * src_br;                     \
                                                         \
    /* 2nd and 3rd col */                                \
    PCKEVOD_D2_DP(src_b3, src_b2, src_br, src_bi);       \
    res1_r OP0## = src_a0r * src_br;                     \
    res1_r OP1## = src_a0i * src_bi;                     \
    res1_i OP2## = OP4 src_a0r * src_bi;                 \
    res1_i OP3## = src_a0i * src_br;                     \
}

#define ZGEMM_KERNEL_4X2_MSA(OP0, OP1, OP2, OP3, OP4)    \
{                                                        \
    LD_DP4_INC(pa0, 2, src_a0, src_a1, src_a2, src_a3);  \
    LD_DP2_INC(pb0, 2, src_b0, src_b1);                  \
                                                         \
    PCKEVOD_D2_DP(src_a1, src_a0, src_a0r, src_a0i);     \
    PCKEVOD_D2_DP(src_a3, src_a2, src_a1r, src_a1i);     \
                                                         \
    /* 0th col */                                        \
    SPLATI_D2_DP(src_b0, src_br, src_bi);                \
    res0_r OP0## = src_a0r * src_br;                     \
    res0_r OP1## = src_a0i * src_bi;                     \
    res0_i OP2## = OP4 src_a0r * src_bi;                 \
    res0_i OP3## = src_a0i * src_br;                     \
                                                         \
    res1_r OP0## = src_a1r * src_br;                     \
    res1_r OP1## = src_a1i * src_bi;                     \
    res1_i OP2## = OP4 src_a1r * src_bi;                 \
    res1_i OP3## = src_a1i * src_br;                     \
                                                         \
    /* 1st col */                                        \
    SPLATI_D2_DP(src_b1, src_br, src_bi);                \
    res2_r OP0## = src_a0r * src_br;                     \
    res2_r OP1## = src_a0i * src_bi;                     \
    res2_i OP2## = OP4 src_a0r * src_bi;                 \
    res2_i OP3## = src_a0i * src_br;                     \
                                                         \
    res3_r OP0## = src_a1r * src_br;                     \
    res3_r OP1## = src_a1i * src_bi;                     \
    res3_i OP2## = OP4 src_a1r * src_bi;                 \
    res3_i OP3## = src_a1i * src_br;                     \
}

#define ZGEMM_KERNEL_2X2_MSA(OP0, OP1, OP2, OP3, OP4)  \
{                                                      \
    LD_DP2_INC(pa0, 2, src_a0, src_a1);                \
    LD_DP2_INC(pb0, 2, src_b0, src_b1);                \
                                                       \
    PCKEVOD_D2_DP(src_a1, src_a0, src_a0r, src_a0i);   \
                                                       \
    /* 0th col */                                      \
    SPLATI_D2_DP(src_b0, src_br, src_bi);              \
    res0_r OP0## = src_a0r * src_br;                   \
    res0_r OP1## = src_a0i * src_bi;                   \
    res0_i OP2## = OP4 src_a0r * src_bi;               \
    res0_i OP3## = src_a0i * src_br;                   \
                                                       \
    /* 1st col */                                      \
    SPLATI_D2_DP(src_b1, src_br, src_bi);              \
    res2_r OP0## = src_a0r * src_br;                   \
    res2_r OP1## = src_a0i * src_bi;                   \
    res2_i OP2## = OP4 src_a0r * src_bi;               \
    res2_i OP3## = src_a0i * src_br;                   \
}

#define ZGEMM_KERNEL_1X2_MSA(OP0, OP1, OP2, OP3, OP4)  \
{                                                      \
    src_a0 = LD_DP(pa0);                               \
    LD_DP2_INC(pb0, 2, src_b0, src_b1);                \
                                                       \
    PCKEVOD_D2_DP(src_a0, src_a0, src_a0r, src_a0i);   \
                                                       \
    /* 0th and 1st col */                              \
    PCKEVOD_D2_DP(src_b1, src_b0, src_br, src_bi);     \
    res0_r OP0## = src_a0r * src_br;                   \
    res0_r OP1## = src_a0i * src_bi;                   \
    res0_i OP2## = OP4 src_a0r * src_bi;               \
    res0_i OP3## = src_a0i * src_br;                   \
}

#define ZGEMM_KERNEL_4X1_MSA(OP0, OP1, OP2, OP3, OP4)    \
{                                                        \
    LD_DP4_INC(pa0, 2, src_a0, src_a1, src_a2, src_a3);  \
    src_b0 = LD_DP(pb0);                                 \
                                                         \
    PCKEVOD_D2_DP(src_a1, src_a0, src_a0r, src_a0i);     \
    PCKEVOD_D2_DP(src_a3, src_a2, src_a1r, src_a1i);     \
                                                         \
    /* 0th col */                                        \
    SPLATI_D2_DP(src_b0, src_br, src_bi);                \
    res0_r OP0## = src_a0r * src_br;                     \
    res0_r OP1## = src_a0i * src_bi;                     \
    res0_i OP2## = OP4 src_a0r * src_bi;                 \
    res0_i OP3## = src_a0i * src_br;                     \
                                                         \
    res1_r OP0## = src_a1r * src_br;                     \
    res1_r OP1## = src_a1i * src_bi;                     \
    res1_i OP2## = OP4 src_a1r * src_bi;                 \
    res1_i OP3## = src_a1i * src_br;                     \
}

#define ZGEMM_KERNEL_2X1_MSA(OP0, OP1, OP2, OP3, OP4)  \
{                                                      \
    LD_DP2_INC(pa0, 2, src_a0, src_a1);                \
    src_b0 = LD_DP(pb0);                               \
                                                       \
    PCKEVOD_D2_DP(src_a1, src_a0, src_a0r, src_a0i);   \
                                                       \
    /* 0th col */                                      \
    SPLATI_D2_DP(src_b0, src_br, src_bi);              \
    res0_r OP0## = src_a0r * src_br;                   \
    res0_r OP1## = src_a0i * src_bi;                   \
    res0_i OP2## = OP4 src_a0r * src_bi;               \
    res0_i OP3## = src_a0i * src_br;                   \
}

#define ZGEMM_KERNEL_1X1(OP0, OP1, OP2, OP3, OP4)  \
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

#define ZGEMM_SCALE_4X4_MSA                      \
{                                                \
    LD_DP4(pc0, 2, dst0, dst1, dst2, dst3);      \
                                                 \
    PCKEVOD_D2_DP(dst1, dst0, dst0_r, dst0_i);   \
    PCKEVOD_D2_DP(dst3, dst2, dst1_r, dst1_i);   \
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
    ILVRL_D2_DP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_D2_DP(dst1_i, dst1_r, dst2, dst3);     \
                                                 \
    LD_DP4(pc1, 2, dst4, dst5, dst6, dst7);      \
                                                 \
    PCKEVOD_D2_DP(dst5, dst4, dst0_r, dst0_i);   \
    PCKEVOD_D2_DP(dst7, dst6, dst1_r, dst1_i);   \
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
    ILVRL_D2_DP(dst0_i, dst0_r, dst4, dst5);     \
    ILVRL_D2_DP(dst1_i, dst1_r, dst6, dst7);     \
                                                 \
    ST_DP4_INC(dst0, dst1, dst2, dst3, pc0, 2);  \
    ST_DP4_INC(dst4, dst5, dst6, dst7, pc1, 2);  \
                                                 \
    LD_DP4(pc2, 2, dst0, dst1, dst2, dst3);      \
                                                 \
    PCKEVOD_D2_DP(dst1, dst0, dst0_r, dst0_i);   \
    PCKEVOD_D2_DP(dst3, dst2, dst1_r, dst1_i);   \
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
    ILVRL_D2_DP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_D2_DP(dst1_i, dst1_r, dst2, dst3);     \
                                                 \
    LD_DP4(pc3, 2, dst4, dst5, dst6, dst7);      \
                                                 \
    PCKEVOD_D2_DP(dst5, dst4, dst0_r, dst0_i);   \
    PCKEVOD_D2_DP(dst7, dst6, dst1_r, dst1_i);   \
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
    ILVRL_D2_DP(dst0_i, dst0_r, dst4, dst5);     \
    ILVRL_D2_DP(dst1_i, dst1_r, dst6, dst7);     \
                                                 \
    ST_DP4_INC(dst0, dst1, dst2, dst3, pc2, 2);  \
    ST_DP4_INC(dst4, dst5, dst6, dst7, pc3, 2);  \
}

#define ZGEMM_SCALE_2X4_MSA                     \
{                                               \
    LD_DP2(pc0, 2, dst0, dst1);                 \
                                                \
    PCKEVOD_D2_DP(dst1, dst0, dst0_r, dst0_i);  \
                                                \
    dst0_r += alpha_r * res0_r;                 \
    dst0_r -= alpha_i * res0_i;                 \
    dst0_i += alpha_r * res0_i;                 \
    dst0_i += alpha_i * res0_r;                 \
                                                \
    ILVRL_D2_DP(dst0_i, dst0_r, dst0, dst1);    \
                                                \
    LD_DP2(pc1, 2, dst2, dst3);                 \
                                                \
    PCKEVOD_D2_DP(dst3, dst2, dst0_r, dst0_i);  \
                                                \
    dst0_r += alpha_r * res2_r;                 \
    dst0_r -= alpha_i * res2_i;                 \
    dst0_i += alpha_r * res2_i;                 \
    dst0_i += alpha_i * res2_r;                 \
                                                \
    ILVRL_D2_DP(dst0_i, dst0_r, dst2, dst3);    \
                                                \
    ST_DP2_INC(dst0, dst1, pc0, 2);             \
    ST_DP2_INC(dst2, dst3, pc1, 2);             \
                                                \
    LD_DP2(pc2, 2, dst0, dst1);                 \
                                                \
    PCKEVOD_D2_DP(dst1, dst0, dst0_r, dst0_i);  \
                                                \
    dst0_r += alpha_r * res4_r;                 \
    dst0_r -= alpha_i * res4_i;                 \
    dst0_i += alpha_r * res4_i;                 \
    dst0_i += alpha_i * res4_r;                 \
                                                \
    ILVRL_D2_DP(dst0_i, dst0_r, dst0, dst1);    \
                                                \
    LD_DP2(pc3, 2, dst2, dst3);                 \
                                                \
    PCKEVOD_D2_DP(dst3, dst2, dst0_r, dst0_i);  \
                                                \
    dst0_r += alpha_r * res6_r;                 \
    dst0_r -= alpha_i * res6_i;                 \
    dst0_i += alpha_r * res6_i;                 \
    dst0_i += alpha_i * res6_r;                 \
                                                \
    ILVRL_D2_DP(dst0_i, dst0_r, dst2, dst3);    \
                                                \
    ST_DP2_INC(dst0, dst1, pc2, 2);             \
    ST_DP2_INC(dst2, dst3, pc3, 2);             \
}

#define ZGEMM_SCALE_1X4_MSA                     \
{                                               \
    dst0 = LD_DP(pc0);                          \
    dst1 = LD_DP(pc1);                          \
                                                \
    PCKEVOD_D2_DP(dst1, dst0, dst0_r, dst0_i);  \
                                                \
    dst0_r += alpha_r * res0_r;                 \
    dst0_r -= alpha_i * res0_i;                 \
    dst0_i += alpha_r * res0_i;                 \
    dst0_i += alpha_i * res0_r;                 \
                                                \
    ILVRL_D2_DP(dst0_i, dst0_r, dst0, dst1);    \
                                                \
    dst2 = LD_DP(pc2);                          \
    dst3 = LD_DP(pc3);                          \
                                                \
    PCKEVOD_D2_DP(dst3, dst2, dst0_r, dst0_i);  \
                                                \
    dst0_r += alpha_r * res1_r;                 \
    dst0_r -= alpha_i * res1_i;                 \
    dst0_i += alpha_r * res1_i;                 \
    dst0_i += alpha_i * res1_r;                 \
                                                \
    ILVRL_D2_DP(dst0_i, dst0_r, dst2, dst3);    \
                                                \
    ST_DP(dst0, pc0);                           \
    ST_DP(dst1, pc1);                           \
    ST_DP(dst2, pc2);                           \
    ST_DP(dst3, pc3);                           \
}

#define ZGEMM_SCALE_4X2_MSA                      \
{                                                \
    LD_DP4(pc0, 2, dst0, dst1, dst2, dst3);      \
                                                 \
    PCKEVOD_D2_DP(dst1, dst0, dst0_r, dst0_i);   \
    PCKEVOD_D2_DP(dst3, dst2, dst1_r, dst1_i);   \
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
    ILVRL_D2_DP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_D2_DP(dst1_i, dst1_r, dst2, dst3);     \
                                                 \
    LD_DP4(pc1, 2, dst4, dst5, dst6, dst7);      \
                                                 \
    PCKEVOD_D2_DP(dst5, dst4, dst0_r, dst0_i);   \
    PCKEVOD_D2_DP(dst7, dst6, dst1_r, dst1_i);   \
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
    ILVRL_D2_DP(dst0_i, dst0_r, dst4, dst5);     \
    ILVRL_D2_DP(dst1_i, dst1_r, dst6, dst7);     \
                                                 \
    ST_DP4_INC(dst0, dst1, dst2, dst3, pc0, 2);  \
    ST_DP4_INC(dst4, dst5, dst6, dst7, pc1, 2);  \
}

#define ZGEMM_SCALE_2X2_MSA                     \
{                                               \
    LD_DP2(pc0, 2, dst0, dst1);                 \
                                                \
    PCKEVOD_D2_DP(dst1, dst0, dst0_r, dst0_i);  \
                                                \
    dst0_r += alpha_r * res0_r;                 \
    dst0_r -= alpha_i * res0_i;                 \
    dst0_i += alpha_r * res0_i;                 \
    dst0_i += alpha_i * res0_r;                 \
                                                \
    ILVRL_D2_DP(dst0_i, dst0_r, dst0, dst1);    \
                                                \
    ST_DP2_INC(dst0, dst1, pc0, 2);             \
                                                \
    LD_DP2(pc1, 2, dst2, dst3);                 \
                                                \
    PCKEVOD_D2_DP(dst3, dst2, dst0_r, dst0_i);  \
                                                \
    dst0_r += alpha_r * res2_r;                 \
    dst0_r -= alpha_i * res2_i;                 \
    dst0_i += alpha_r * res2_i;                 \
    dst0_i += alpha_i * res2_r;                 \
                                                \
    ILVRL_D2_DP(dst0_i, dst0_r, dst2, dst3);    \
                                                \
    ST_DP2_INC(dst2, dst3, pc1, 2);             \
}

#define ZGEMM_SCALE_1X2_MSA                     \
{                                               \
    dst0 = LD_DP(pc0);                          \
    dst1 = LD_DP(pc1);                          \
                                                \
    PCKEVOD_D2_DP(dst1, dst0, dst0_r, dst0_i);  \
                                                \
    dst0_r += alpha_r * res0_r;                 \
    dst0_r -= alpha_i * res0_i;                 \
    dst0_i += alpha_r * res0_i;                 \
    dst0_i += alpha_i * res0_r;                 \
                                                \
    ILVRL_D2_DP(dst0_i, dst0_r, dst0, dst1);    \
                                                \
    ST_DP(dst0, pc0);                           \
    ST_DP(dst1, pc1);                           \
}

#define ZGEMM_SCALE_4X1_MSA                      \
{                                                \
    LD_DP4(pc0, 2, dst0, dst1, dst2, dst3);      \
                                                 \
    PCKEVOD_D2_DP(dst1, dst0, dst0_r, dst0_i);   \
    PCKEVOD_D2_DP(dst3, dst2, dst1_r, dst1_i);   \
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
    ILVRL_D2_DP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_D2_DP(dst1_i, dst1_r, dst2, dst3);     \
                                                 \
    ST_DP4_INC(dst0, dst1, dst2, dst3, pc0, 2);  \
}

#define ZGEMM_SCALE_2X1_MSA                     \
{                                               \
    LD_DP2(pc0, 2, dst0, dst1);                 \
                                                \
    PCKEVOD_D2_DP(dst1, dst0, dst0_r, dst0_i);  \
                                                \
    dst0_r += alpha_r * res0_r;                 \
    dst0_r -= alpha_i * res0_i;                 \
    dst0_i += alpha_r * res0_i;                 \
    dst0_i += alpha_i * res0_r;                 \
                                                \
    ILVRL_D2_DP(dst0_i, dst0_r, dst0, dst1);    \
                                                \
    ST_DP2_INC(dst0, dst1, pc0, 2);             \
}

#define ZGEMM_SCALE_1X1       \
{                             \
    pc0[0] += alphar * res0;  \
    pc0[0] -= alphai * res1;  \
    pc0[1] += alphar * res1;  \
    pc0[1] += alphai * res0;  \
}

#define ZGEMM_TRMM_SCALE_4X4_MSA                 \
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
    ILVRL_D2_DP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_D2_DP(dst1_i, dst1_r, dst2, dst3);     \
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
    ILVRL_D2_DP(dst0_i, dst0_r, dst4, dst5);     \
    ILVRL_D2_DP(dst1_i, dst1_r, dst6, dst7);     \
                                                 \
    ST_DP4_INC(dst0, dst1, dst2, dst3, pc0, 2);  \
    ST_DP4_INC(dst4, dst5, dst6, dst7, pc1, 2);  \
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
    ILVRL_D2_DP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_D2_DP(dst1_i, dst1_r, dst2, dst3);     \
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
    ILVRL_D2_DP(dst0_i, dst0_r, dst4, dst5);     \
    ILVRL_D2_DP(dst1_i, dst1_r, dst6, dst7);     \
                                                 \
    ST_DP4_INC(dst0, dst1, dst2, dst3, pc2, 2);  \
    ST_DP4_INC(dst4, dst5, dst6, dst7, pc3, 2);  \
}

#define ZGEMM_TRMM_SCALE_2X4_MSA              \
{                                             \
    dst0_r = alpha_r * res0_r;                \
    dst0_r -= alpha_i * res0_i;               \
    dst0_i = alpha_r * res0_i;                \
    dst0_i += alpha_i * res0_r;               \
                                              \
    ILVRL_D2_DP(dst0_i, dst0_r, dst0, dst1);  \
                                              \
    dst0_r = alpha_r * res2_r;                \
    dst0_r -= alpha_i * res2_i;               \
    dst0_i = alpha_r * res2_i;                \
    dst0_i += alpha_i * res2_r;               \
                                              \
    ILVRL_D2_DP(dst0_i, dst0_r, dst2, dst3);  \
                                              \
    ST_DP2_INC(dst0, dst1, pc0, 2);           \
    ST_DP2_INC(dst2, dst3, pc1, 2);           \
                                              \
    dst0_r = alpha_r * res4_r;                \
    dst0_r -= alpha_i * res4_i;               \
    dst0_i = alpha_r * res4_i;                \
    dst0_i += alpha_i * res4_r;               \
                                              \
    ILVRL_D2_DP(dst0_i, dst0_r, dst0, dst1);  \
                                              \
    dst0_r = alpha_r * res6_r;                \
    dst0_r -= alpha_i * res6_i;               \
    dst0_i = alpha_r * res6_i;                \
    dst0_i += alpha_i * res6_r;               \
                                              \
    ILVRL_D2_DP(dst0_i, dst0_r, dst2, dst3);  \
                                              \
    ST_DP2_INC(dst0, dst1, pc2, 2);           \
    ST_DP2_INC(dst2, dst3, pc3, 2);           \
}

#define ZGEMM_TRMM_SCALE_1X4_MSA              \
{                                             \
    dst0_r = alpha_r * res0_r;                \
    dst0_r -= alpha_i * res0_i;               \
    dst0_i = alpha_r * res0_i;                \
    dst0_i += alpha_i * res0_r;               \
                                              \
    ILVRL_D2_DP(dst0_i, dst0_r, dst0, dst1);  \
                                              \
    dst0_r = alpha_r * res1_r;                \
    dst0_r -= alpha_i * res1_i;               \
    dst0_i = alpha_r * res1_i;                \
    dst0_i += alpha_i * res1_r;               \
                                              \
    ILVRL_D2_DP(dst0_i, dst0_r, dst2, dst3);  \
                                              \
    ST_DP(dst0, pc0);                         \
    ST_DP(dst1, pc1);                         \
    ST_DP(dst2, pc2);                         \
    ST_DP(dst3, pc3);                         \
}

#define ZGEMM_TRMM_SCALE_4X2_MSA                 \
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
    ILVRL_D2_DP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_D2_DP(dst1_i, dst1_r, dst2, dst3);     \
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
    ILVRL_D2_DP(dst0_i, dst0_r, dst4, dst5);     \
    ILVRL_D2_DP(dst1_i, dst1_r, dst6, dst7);     \
                                                 \
    ST_DP4_INC(dst0, dst1, dst2, dst3, pc0, 2);  \
    ST_DP4_INC(dst4, dst5, dst6, dst7, pc1, 2);  \
}

#define ZGEMM_TRMM_SCALE_2X2_MSA              \
{                                             \
    dst0_r = alpha_r * res0_r;                \
    dst0_r -= alpha_i * res0_i;               \
    dst0_i = alpha_r * res0_i;                \
    dst0_i += alpha_i * res0_r;               \
                                              \
    ILVRL_D2_DP(dst0_i, dst0_r, dst0, dst1);  \
                                              \
    ST_DP2_INC(dst0, dst1, pc0, 2);           \
                                              \
    dst0_r = alpha_r * res2_r;                \
    dst0_r -= alpha_i * res2_i;               \
    dst0_i = alpha_r * res2_i;                \
    dst0_i += alpha_i * res2_r;               \
                                              \
    ILVRL_D2_DP(dst0_i, dst0_r, dst2, dst3);  \
                                              \
    ST_DP2_INC(dst2, dst3, pc1, 2);           \
}

#define ZGEMM_TRMM_SCALE_1X2_MSA              \
{                                             \
    dst0_r = alpha_r * res0_r;                \
    dst0_r -= alpha_i * res0_i;               \
    dst0_i = alpha_r * res0_i;                \
    dst0_i += alpha_i * res0_r;               \
                                              \
    ILVRL_D2_DP(dst0_i, dst0_r, dst0, dst1);  \
                                              \
    ST_DP(dst0, pc0);                         \
    ST_DP(dst1, pc1);                         \
}

#define ZGEMM_TRMM_SCALE_4X1_MSA                 \
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
    ILVRL_D2_DP(dst0_i, dst0_r, dst0, dst1);     \
    ILVRL_D2_DP(dst1_i, dst1_r, dst2, dst3);     \
                                                 \
    ST_DP4_INC(dst0, dst1, dst2, dst3, pc0, 2);  \
}

#define ZGEMM_TRMM_SCALE_2X1_MSA              \
{                                             \
    dst0_r = alpha_r * res0_r;                \
    dst0_r -= alpha_i * res0_i;               \
    dst0_i = alpha_r * res0_i;                \
    dst0_i += alpha_i * res0_r;               \
                                              \
    ILVRL_D2_DP(dst0_i, dst0_r, dst0, dst1);  \
                                              \
    ST_DP2_INC(dst0, dst1, pc0, 2);           \
}

#define ZGEMM_TRMM_SCALE_1X1  \
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
    FLOAT res0, res1, a0_r, a0_i, b0_r, b0_i;
    v2f64 src_a0, src_a1, src_a2, src_a3, src_b0, src_b1, src_b2, src_b3;
    v2f64 src_a0r, src_a0i, src_a1r, src_a1i, src_br, src_bi;
    v2f64 dst0, dst1, dst2, dst3, dst4, dst5, dst6, dst7;
    v2f64 dst0_r, dst0_i, dst1_r, dst1_i, alpha_r, alpha_i;
    v2f64 res0_r, res0_i, res1_r, res1_i, res2_r, res2_i, res3_r, res3_i;
    v2f64 res4_r, res4_i, res5_r, res5_i, res6_r, res6_i, res7_r, res7_i;

    alpha_r = COPY_DOUBLE_TO_VECTOR(alphar);
    alpha_i = COPY_DOUBLE_TO_VECTOR(alphai);

#if defined(TRMMKERNEL) && !defined(LEFT)
    off = -offset;
#endif

    for (j = (n >> 2); j--;)
    {
        pc0 = C;
        pc1 = pc0 + 2 * ldc;
        pc2 = pc1 + 2 * ldc;
        pc3 = pc2 + 2 * ldc;

        pa0 = A;

#if defined(TRMMKERNEL) && defined(LEFT)
        off = offset;
#endif

        for (i = (m >> 2); i--;)
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

#ifdef ENABLE_PREFETCH
            __asm__ __volatile__(
                "pref   0,   64(%[pa0])   \n\t"
                "pref   0,   96(%[pa0])   \n\t"
                "pref   0,   64(%[pb0])   \n\t"
                "pref   0,   96(%[pb0])   \n\t"

                :
                : [pa0] "r" (pa0), [pb0] "r" (pb0)
            );
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
            ZGEMM_KERNEL_4X4_MSA(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            ZGEMM_KERNEL_4X4_MSA(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            ZGEMM_KERNEL_4X4_MSA(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            ZGEMM_KERNEL_4X4_MSA(, -, , -, -);
#endif

            for (l = (temp - 1); l--;)
            {
#ifdef ENABLE_PREFETCH
            __asm__ __volatile__(
                "pref   0,   64(%[pa0])   \n\t"
                "pref   0,   96(%[pa0])   \n\t"
                "pref   0,   64(%[pb0])   \n\t"
                "pref   0,   96(%[pb0])   \n\t"

                :
                : [pa0] "r" (pa0), [pb0] "r" (pb0)
            );
#endif

#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                ZGEMM_KERNEL_4X4_MSA(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                ZGEMM_KERNEL_4X4_MSA(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                ZGEMM_KERNEL_4X4_MSA(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                ZGEMM_KERNEL_4X4_MSA(+, -, -, -,);
#endif
            }

#if defined(TRMMKERNEL)
            ZGEMM_TRMM_SCALE_4X4_MSA
#else
            ZGEMM_SCALE_4X4_MSA
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
            ZGEMM_KERNEL_2X4_MSA(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            ZGEMM_KERNEL_2X4_MSA(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            ZGEMM_KERNEL_2X4_MSA(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            ZGEMM_KERNEL_2X4_MSA(, -, , -, -);
#endif

            for (l = (temp - 1); l--;)
            {
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                ZGEMM_KERNEL_2X4_MSA(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                ZGEMM_KERNEL_2X4_MSA(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                ZGEMM_KERNEL_2X4_MSA(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                ZGEMM_KERNEL_2X4_MSA(+, -, -, -,);
#endif
            }

#if defined(TRMMKERNEL)
            ZGEMM_TRMM_SCALE_2X4_MSA
#else
            ZGEMM_SCALE_2X4_MSA
#endif

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
            ZGEMM_KERNEL_1X4_MSA(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            ZGEMM_KERNEL_1X4_MSA(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            ZGEMM_KERNEL_1X4_MSA(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            ZGEMM_KERNEL_1X4_MSA(, -, , -, -);
#endif

            pa0 += 2;

            for (l = (temp - 1); l--;)
            {
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                ZGEMM_KERNEL_1X4_MSA(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                ZGEMM_KERNEL_1X4_MSA(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                ZGEMM_KERNEL_1X4_MSA(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                ZGEMM_KERNEL_1X4_MSA(+, -, -, -,);
#endif

                pa0 += 2;
            }

#if defined(TRMMKERNEL)
            ZGEMM_TRMM_SCALE_1X4_MSA
#else
            ZGEMM_SCALE_1X4_MSA
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

        pa0 = A;

#if defined(TRMMKERNEL) && defined(LEFT)
        off = offset;
#endif

        for (i = (m >> 2); i--;)
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
            ZGEMM_KERNEL_4X2_MSA(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            ZGEMM_KERNEL_4X2_MSA(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            ZGEMM_KERNEL_4X2_MSA(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            ZGEMM_KERNEL_4X2_MSA(, -, , -, -);
#endif

            for (l = (temp - 1); l--;)
            {
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                ZGEMM_KERNEL_4X2_MSA(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                ZGEMM_KERNEL_4X2_MSA(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                ZGEMM_KERNEL_4X2_MSA(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                ZGEMM_KERNEL_4X2_MSA(+, -, -, -,);
#endif
            }

#if defined(TRMMKERNEL)
            ZGEMM_TRMM_SCALE_4X2_MSA
#else
            ZGEMM_SCALE_4X2_MSA
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
            ZGEMM_KERNEL_2X2_MSA(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            ZGEMM_KERNEL_2X2_MSA(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            ZGEMM_KERNEL_2X2_MSA(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            ZGEMM_KERNEL_2X2_MSA(, -, , -, -);
#endif

            for (l = (temp - 1); l--;)
            {
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                ZGEMM_KERNEL_2X2_MSA(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                ZGEMM_KERNEL_2X2_MSA(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                ZGEMM_KERNEL_2X2_MSA(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                ZGEMM_KERNEL_2X2_MSA(+, -, -, -,);
#endif
            }

#if defined(TRMMKERNEL)
            ZGEMM_TRMM_SCALE_2X2_MSA
#else
            ZGEMM_SCALE_2X2_MSA
#endif

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
            ZGEMM_KERNEL_1X2_MSA(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            ZGEMM_KERNEL_1X2_MSA(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            ZGEMM_KERNEL_1X2_MSA(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            ZGEMM_KERNEL_1X2_MSA(, -, , -, -);
#endif

            pa0 += 2;

            for (l = (temp - 1); l--;)
            {
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                ZGEMM_KERNEL_1X2_MSA(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                ZGEMM_KERNEL_1X2_MSA(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                ZGEMM_KERNEL_1X2_MSA(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                ZGEMM_KERNEL_1X2_MSA(+, -, -, -,);
#endif

                pa0 += 2;
            }

#if defined(TRMMKERNEL)
            ZGEMM_TRMM_SCALE_1X2_MSA
#else
            ZGEMM_SCALE_1X2_MSA
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
        pa0 = A;

#if defined(TRMMKERNEL) && defined(LEFT)
        off = offset;
#endif

        for (i = (m >> 2); i--;)
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
            ZGEMM_KERNEL_4X1_MSA(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            ZGEMM_KERNEL_4X1_MSA(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            ZGEMM_KERNEL_4X1_MSA(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            ZGEMM_KERNEL_4X1_MSA(, -, , -, -);
#endif

            pb0 += 2;

            for (l = (temp - 1); l--;)
            {
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                ZGEMM_KERNEL_4X1_MSA(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                ZGEMM_KERNEL_4X1_MSA(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                ZGEMM_KERNEL_4X1_MSA(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                ZGEMM_KERNEL_4X1_MSA(+, -, -, -,);
#endif

                pb0 += 2;
            }

#if defined(TRMMKERNEL)
            ZGEMM_TRMM_SCALE_4X1_MSA
#else
            ZGEMM_SCALE_4X1_MSA
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
            ZGEMM_KERNEL_2X1_MSA(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            ZGEMM_KERNEL_2X1_MSA(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            ZGEMM_KERNEL_2X1_MSA(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            ZGEMM_KERNEL_2X1_MSA(, -, , -, -);
#endif

            pb0 += 2;

            for (l = (temp - 1); l--;)
            {
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                ZGEMM_KERNEL_2X1_MSA(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                ZGEMM_KERNEL_2X1_MSA(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                ZGEMM_KERNEL_2X1_MSA(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                ZGEMM_KERNEL_2X1_MSA(+, -, -, -,);
#endif

                pb0 += 2;
            }

#if defined(TRMMKERNEL)
            ZGEMM_TRMM_SCALE_2X1_MSA
#else
            ZGEMM_SCALE_2X1_MSA
#endif

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
            ZGEMM_KERNEL_1X1(, -, , +, +);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
            ZGEMM_KERNEL_1X1(, +, , +, -);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
            ZGEMM_KERNEL_1X1(, +, , -, +);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
            ZGEMM_KERNEL_1X1(, -, , -, -);
#endif

            pa0 += 2;
            pb0 += 2;

            for (l = (temp - 1); l--;)
            {
#if defined(NN) || defined(NT) || defined(TN) || defined(TT)
                ZGEMM_KERNEL_1X1(+, -, +, +,);
#endif
#if defined(NR) || defined(NC) || defined(TR) || defined(TC)
                ZGEMM_KERNEL_1X1(+, +, -, +,);
#endif
#if defined(RN) || defined(RT) || defined(CN) || defined(CT)
                ZGEMM_KERNEL_1X1(+, +, +, -,);
#endif
#if defined(RR) || defined(RC) || defined(CR) || defined(CC)
                ZGEMM_KERNEL_1X1(+, -, -, -,);
#endif

                pa0 += 2;
                pb0 += 2;
            }

#if defined(TRMMKERNEL)
            ZGEMM_TRMM_SCALE_1X1
#else
            ZGEMM_SCALE_1X1
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
