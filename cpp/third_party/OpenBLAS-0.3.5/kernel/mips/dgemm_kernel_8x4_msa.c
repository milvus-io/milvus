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

static void __attribute__ ((noinline))
dgemmkernel_8x4_core_msa(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT alpha,
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
    v2f64 v_alpha = {alpha, alpha};
    v2f64 src_a0, src_a1, src_a2, src_a3, src_b, src_b0, src_b1;
    v2f64 dst0, dst1, dst2, dst3, dst4, dst5, dst6, dst7;
    v2f64 res0, res1, res2, res3, res4, res5, res6, res7;
    v2f64 res8, res9, res10, res11, res12, res13, res14, res15;

#if defined(TRMMKERNEL) && !defined(LEFT)
    off = -offset;
#endif

    for (j = (n >> 2); j--;)
    {
        pc0 = C;
        pc1 = pc0 + ldc;
        pc2 = pc1 + ldc;
        pc3 = pc2 + ldc;

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
            pa0 += off * 8;
            pb0 = B + off * 4;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 8; // number of values in A
#else
            temp = off + 4; // number of values in B
#endif
#else  // #if !defined(TRMMKERNEL)
            pb0 = B;
            temp = k;
#endif

#ifdef ENABLE_PREFETCH
            __asm__ __volatile__(
                "pref   0,   64(%[pa0])   \n\t"
                "pref   0,   96(%[pa0])   \n\t"
                "pref   0,  128(%[pa0])   \n\t"
                "pref   0,  160(%[pa0])   \n\t"
                "pref   0,   32(%[pb0])   \n\t"
                "pref   0,   64(%[pb0])   \n\t"
                "pref   0,   96(%[pb0])   \n\t"

                :
                : [pa0] "r" (pa0), [pb0] "r" (pb0)
            );
#endif

            LD_DP4_INC(pa0, 2, src_a0, src_a1, src_a2, src_a3);
            LD_DP2_INC(pb0, 2, src_b0, src_b1);

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
            res0 = src_a0 * src_b;
            res1 = src_a1 * src_b;
            res2 = src_a2 * src_b;
            res3 = src_a3 * src_b;

            src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
            res4 = src_a0 * src_b;
            res5 = src_a1 * src_b;
            res6 = src_a2 * src_b;
            res7 = src_a3 * src_b;

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b1, (v2i64) src_b1);
            res8  = src_a0 * src_b;
            res9  = src_a1 * src_b;
            res10 = src_a2 * src_b;
            res11 = src_a3 * src_b;

            src_b = (v2f64) __msa_ilvl_d((v2i64) src_b1, (v2i64) src_b1);
            res12 = src_a0 * src_b;
            res13 = src_a1 * src_b;
            res14 = src_a2 * src_b;
            res15 = src_a3 * src_b;

            for (l = ((temp - 1) >> 1); l--;)
            {
#ifdef ENABLE_PREFETCH
                __asm__ __volatile__(
                    "pref   0,  128(%[pa0])   \n\t"
                    "pref   0,  160(%[pa0])   \n\t"
                    "pref   0,  192(%[pa0])   \n\t"
                    "pref   0,  224(%[pa0])   \n\t"

                    :
                    : [pa0] "r" (pa0)
                );
#endif
                LD_DP4_INC(pa0, 2, src_a0, src_a1, src_a2, src_a3);
                LD_DP2_INC(pb0, 2, src_b0, src_b1);

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;
                res2 += src_a2 * src_b;
                res3 += src_a3 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
                res4 += src_a0 * src_b;
                res5 += src_a1 * src_b;
                res6 += src_a2 * src_b;
                res7 += src_a3 * src_b;

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b1, (v2i64) src_b1);
                res8  += src_a0 * src_b;
                res9  += src_a1 * src_b;
                res10 += src_a2 * src_b;
                res11 += src_a3 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b1, (v2i64) src_b1);
                res12 += src_a0 * src_b;
                res13 += src_a1 * src_b;
                res14 += src_a2 * src_b;
                res15 += src_a3 * src_b;

                LD_DP4_INC(pa0, 2, src_a0, src_a1, src_a2, src_a3);
#ifdef ENABLE_PREFETCH
                __asm__ __volatile__(
                    "pref   0,   64(%[pb0])   \n\t"
                    "pref   0,   96(%[pb0])   \n\t"

                    :
                    : [pb0] "r" (pb0)
                );
#endif
                LD_DP2_INC(pb0, 2, src_b0, src_b1);

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;
                res2 += src_a2 * src_b;
                res3 += src_a3 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
                res4 += src_a0 * src_b;
                res5 += src_a1 * src_b;
                res6 += src_a2 * src_b;
                res7 += src_a3 * src_b;

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b1, (v2i64) src_b1);
                res8  += src_a0 * src_b;
                res9  += src_a1 * src_b;
                res10 += src_a2 * src_b;
                res11 += src_a3 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b1, (v2i64) src_b1);
                res12 += src_a0 * src_b;
                res13 += src_a1 * src_b;
                res14 += src_a2 * src_b;
                res15 += src_a3 * src_b;
            }

            if ((temp - 1) & 1)
            {
                LD_DP4_INC(pa0, 2, src_a0, src_a1, src_a2, src_a3);
                LD_DP2_INC(pb0, 2, src_b0, src_b1);

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;
                res2 += src_a2 * src_b;
                res3 += src_a3 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
                res4 += src_a0 * src_b;
                res5 += src_a1 * src_b;
                res6 += src_a2 * src_b;
                res7 += src_a3 * src_b;

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b1, (v2i64) src_b1);
                res8  += src_a0 * src_b;
                res9  += src_a1 * src_b;
                res10 += src_a2 * src_b;
                res11 += src_a3 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b1, (v2i64) src_b1);
                res12 += src_a0 * src_b;
                res13 += src_a1 * src_b;
                res14 += src_a2 * src_b;
                res15 += src_a3 * src_b;
            }

#ifdef ENABLE_PREFETCH
            __asm__ __volatile__(
                "pref   0,  64(%[pc0])   \n\t"
                "pref   0,  64(%[pc1])   \n\t"
                "pref   0,  64(%[pc2])   \n\t"
                "pref   0,  64(%[pc3])   \n\t"

                :
                : [pc0] "r" (pc0), [pc1] "r" (pc1),
                  [pc2] "r" (pc2), [pc3] "r" (pc3)
            );
#endif

#if defined(TRMMKERNEL)
            dst0 = res0 * v_alpha;
            dst1 = res1 * v_alpha;
            dst2 = res2 * v_alpha;
            dst3 = res3 * v_alpha;
            dst4 = res4 * v_alpha;
            dst5 = res5 * v_alpha;
            dst6 = res6 * v_alpha;
            dst7 = res7 * v_alpha;
#else
            LD_DP4(pc0, 2, dst0, dst1, dst2, dst3);
            LD_DP4(pc1, 2, dst4, dst5, dst6, dst7);

            dst0 += res0 * v_alpha;
            dst1 += res1 * v_alpha;
            dst2 += res2 * v_alpha;
            dst3 += res3 * v_alpha;
            dst4 += res4 * v_alpha;
            dst5 += res5 * v_alpha;
            dst6 += res6 * v_alpha;
            dst7 += res7 * v_alpha;
#endif
            ST_DP4_INC(dst0, dst1, dst2, dst3, pc0, 2);
            ST_DP4_INC(dst4, dst5, dst6, dst7, pc1, 2);

#if defined(TRMMKERNEL)
            dst0 = res8 * v_alpha;
            dst1 = res9 * v_alpha;
            dst2 = res10 * v_alpha;
            dst3 = res11 * v_alpha;
            dst4 = res12 * v_alpha;
            dst5 = res13 * v_alpha;
            dst6 = res14 * v_alpha;
            dst7 = res15 * v_alpha;
#else
            LD_DP4(pc2, 2, dst0, dst1, dst2, dst3);
            LD_DP4(pc3, 2, dst4, dst5, dst6, dst7);

            dst0 += res8 * v_alpha;
            dst1 += res9 * v_alpha;
            dst2 += res10 * v_alpha;
            dst3 += res11 * v_alpha;
            dst4 += res12 * v_alpha;
            dst5 += res13 * v_alpha;
            dst6 += res14 * v_alpha;
            dst7 += res15 * v_alpha;
#endif
            ST_DP4_INC(dst0, dst1, dst2, dst3, pc2, 2);
            ST_DP4_INC(dst4, dst5, dst6, dst7, pc3, 2);

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 8; // number of values in A
#else
            temp -= 4; // number of values in B
#endif
            pa0 += temp * 8;
            pb0 += temp * 4;
#endif

#ifdef LEFT
            off += 8; // number of values in A
#endif
#endif  // #if defined(TRMMKERNEL)
        }

#if defined(TRMMKERNEL) && !defined(LEFT)
        off += 4; // number of values in A
#endif

        B += (k << 2);
        C += (ldc << 2);
    }
}

static void __attribute__ ((noinline))
dgemmkernel_7x4_core_msa(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT alpha,
                         FLOAT *A, FLOAT *B, FLOAT *C, BLASLONG ldc
#ifdef TRMMKERNEL
          , BLASLONG offset
#endif
                         )
{
    BLASLONG j, l, temp;
#if defined(TRMMKERNEL)
    BLASLONG off;
#endif
    FLOAT *pc0, *pc1, *pc2, *pc3, *pa0, *pb0;
    FLOAT tmp0, tmp1, tmp2, tmp3;
    FLOAT a0, b0, b1, b2, b3;
    v2f64 v_alpha = {alpha, alpha};
    v2f64 src_a0, src_a1, src_b, src_b0, src_b1;
    v2f64 dst0, dst1, dst2, dst3, dst4, dst5, dst6, dst7;
    v2f64 res0, res1, res2, res3, res4, res5, res6, res7;

#if defined(TRMMKERNEL) && !defined(LEFT)
    off = -offset;
#endif

    for (j = (n >> 2); j--;)
    {
#if defined(TRMMKERNEL)
        pc0 = C;
        pc1 = pc0 + ldc;
        pc2 = pc1 + ldc;
        pc3 = pc2 + ldc;

        pa0 = A;

#if defined(LEFT)
        off = offset;
#endif

        for (l = (m >> 3); l--;)
        {
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 8;
            pb0 = B + off * 4;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 8; // number of values in A
#else
            temp = off + 4; // number of values in B
#endif

            pc0 += 8;
            pc1 += 8;
            pc2 += 8;
            pc3 += 8;
            pa0 += 8 * temp;
            pb0 += 4 * temp;

#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 8; // number of values in A
#else
            temp -= 4; // number of values in B
#endif
            pa0 += temp * 8;
            pb0 += temp * 4;
#endif

#ifdef LEFT
            off += 8; // number of values in A
#endif
        }
#else  // #if !defined(TRMMKERNEL)
        pc0 = C + 8 * (m >> 3);
        pc1 = pc0 + ldc;
        pc2 = pc1 + ldc;
        pc3 = pc2 + ldc;

        pa0 = A + k * 8 * (m >> 3);
#endif

        if (m & 4)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 4;
            pb0 = B + off * 4;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 4; // number of values in A
#else
            temp = off + 4; // number of values in B
#endif
#else  // #if !defined(TRMMKERNEL)
            pb0 = B;
            temp = k;
#endif

            LD_DP2_INC(pa0, 2, src_a0, src_a1);
            LD_DP2_INC(pb0, 2, src_b0, src_b1);

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
            res0 = src_a0 * src_b;
            res1 = src_a1 * src_b;

            src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
            res2 = src_a0 * src_b;
            res3 = src_a1 * src_b;

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b1, (v2i64) src_b1);
            res4 = src_a0 * src_b;
            res5 = src_a1 * src_b;

            src_b = (v2f64) __msa_ilvl_d((v2i64) src_b1, (v2i64) src_b1);
            res6 = src_a0 * src_b;
            res7 = src_a1 * src_b;

            for (l = ((temp - 1) >> 1); l--;)
            {
                LD_DP2_INC(pa0, 2, src_a0, src_a1);
                LD_DP2_INC(pb0, 2, src_b0, src_b1);

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
                res2 += src_a0 * src_b;
                res3 += src_a1 * src_b;

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b1, (v2i64) src_b1);
                res4 += src_a0 * src_b;
                res5 += src_a1 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b1, (v2i64) src_b1);
                res6 += src_a0 * src_b;
                res7 += src_a1 * src_b;

                LD_DP2_INC(pa0, 2, src_a0, src_a1);
                LD_DP2_INC(pb0, 2, src_b0, src_b1);

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
                res2 += src_a0 * src_b;
                res3 += src_a1 * src_b;

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b1, (v2i64) src_b1);
                res4 += src_a0 * src_b;
                res5 += src_a1 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b1, (v2i64) src_b1);
                res6 += src_a0 * src_b;
                res7 += src_a1 * src_b;
            }

            if ((temp - 1) & 1)
            {
                LD_DP2_INC(pa0, 2, src_a0, src_a1);
                LD_DP2_INC(pb0, 2, src_b0, src_b1);

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
                res2 += src_a0 * src_b;
                res3 += src_a1 * src_b;

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b1, (v2i64) src_b1);
                res4 += src_a0 * src_b;
                res5 += src_a1 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b1, (v2i64) src_b1);
                res6 += src_a0 * src_b;
                res7 += src_a1 * src_b;
            }

#if defined(TRMMKERNEL)
            dst0 = res0 * v_alpha;
            dst1 = res1 * v_alpha;
            dst2 = res2 * v_alpha;
            dst3 = res3 * v_alpha;
            dst4 = res4 * v_alpha;
            dst5 = res5 * v_alpha;
            dst6 = res6 * v_alpha;
            dst7 = res7 * v_alpha;
#else
            LD_DP2(pc0, 2, dst0, dst1);
            LD_DP2(pc1, 2, dst2, dst3);
            LD_DP2(pc2, 2, dst4, dst5);
            LD_DP2(pc3, 2, dst6, dst7);

            dst0 += res0 * v_alpha;
            dst1 += res1 * v_alpha;
            dst2 += res2 * v_alpha;
            dst3 += res3 * v_alpha;
            dst4 += res4 * v_alpha;
            dst5 += res5 * v_alpha;
            dst6 += res6 * v_alpha;
            dst7 += res7 * v_alpha;
#endif
            ST_DP2_INC(dst0, dst1, pc0, 2);
            ST_DP2_INC(dst2, dst3, pc1, 2);
            ST_DP2_INC(dst4, dst5, pc2, 2);
            ST_DP2_INC(dst6, dst7, pc3, 2);

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 4; // number of values in A
#else
            temp -= 4; // number of values in B
#endif
            pa0 += temp * 4;
            pb0 += temp * 4;
#endif

#ifdef LEFT
            off += 4; // number of values in A
#endif
#endif  // #if defined(TRMMKERNEL)
        }

        if (m & 2)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 2;
            pb0 = B + off * 4;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 2; // number of values in A
#else
            temp = off + 4; // number of values in B
#endif
#else  // #if !defined(TRMMKERNEL)
            pb0 = B;
            temp = k;
#endif

            src_a0 = LD_DP(pa0);
            pa0 += 2;
            LD_DP2_INC(pb0, 2, src_b0, src_b1);

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
            res0 = src_a0 * src_b;

            src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
            res1 = src_a0 * src_b;

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b1, (v2i64) src_b1);
            res2 = src_a0 * src_b;

            src_b = (v2f64) __msa_ilvl_d((v2i64) src_b1, (v2i64) src_b1);
            res3 = src_a0 * src_b;

            for (l = ((temp - 1) >> 1); l--;)
            {
                src_a0 = LD_DP(pa0);
                pa0 += 2;
                LD_DP2_INC(pb0, 2, src_b0, src_b1);

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
                res0 += src_a0 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
                res1 += src_a0 * src_b;

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b1, (v2i64) src_b1);
                res2 += src_a0 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b1, (v2i64) src_b1);
                res3 += src_a0 * src_b;

                src_a0 = LD_DP(pa0);
                pa0 += 2;
                LD_DP2_INC(pb0, 2, src_b0, src_b1);

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
                res0 += src_a0 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
                res1 += src_a0 * src_b;

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b1, (v2i64) src_b1);
                res2 += src_a0 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b1, (v2i64) src_b1);
                res3 += src_a0 * src_b;
            }

            if ((temp - 1) & 1)
            {
                src_a0 = LD_DP(pa0);
                pa0 += 2;
                LD_DP2_INC(pb0, 2, src_b0, src_b1);

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
                res0 += src_a0 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
                res1 += src_a0 * src_b;

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b1, (v2i64) src_b1);
                res2 += src_a0 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b1, (v2i64) src_b1);
                res3 += src_a0 * src_b;
            }

#if defined(TRMMKERNEL)
            dst0 = res0 * v_alpha;
            dst1 = res1 * v_alpha;
            dst2 = res2 * v_alpha;
            dst3 = res3 * v_alpha;
#else
            dst0 = LD_DP(pc0);
            dst1 = LD_DP(pc1);
            dst2 = LD_DP(pc2);
            dst3 = LD_DP(pc3);

            dst0 += res0 * v_alpha;
            dst1 += res1 * v_alpha;
            dst2 += res2 * v_alpha;
            dst3 += res3 * v_alpha;
#endif
            ST_DP(dst0, pc0);
            ST_DP(dst1, pc1);
            ST_DP(dst2, pc2);
            ST_DP(dst3, pc3);

            pc0 += 2;
            pc1 += 2;
            pc2 += 2;
            pc3 += 2;

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 2; // number of values in A
#else
            temp -= 4; // number of values in B
#endif
            pa0 += temp * 2;
            pb0 += temp * 4;
#endif

#ifdef LEFT
            off += 2; // number of values in A
#endif
#endif  // #if defined(TRMMKERNEL)
        }

        if (m & 1)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 1;
            pb0 = B + off * 4;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 1; // number of values in A
#else
            temp = off + 4; // number of values in B
#endif
#else  // #if !defined(TRMMKERNEL)
            pb0 = B;
            temp = k;
#endif

            a0 = pa0[0];
            b0 = pb0[0];
            tmp0 = a0 * b0;

            b1 = pb0[1];
            tmp1 = a0 * b1;

            b2 = pb0[2];
            tmp2 = a0 * b2;

            b3 = pb0[3];
            tmp3 = a0 * b3;

            pa0 += 1;
            pb0 += 4;

            for (l = ((temp - 1) >> 1); l--;)
            {
                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                b1 = pb0[1];
                tmp1 += a0 * b1;

                b2 = pb0[2];
                tmp2 += a0 * b2;

                b3 = pb0[3];
                tmp3 += a0 * b3;

                pa0 += 1;
                pb0 += 4;

                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                b1 = pb0[1];
                tmp1 += a0 * b1;

                b2 = pb0[2];
                tmp2 += a0 * b2;

                b3 = pb0[3];
                tmp3 += a0 * b3;

                pa0 += 1;
                pb0 += 4;
            }

            if ((temp - 1) & 1)
            {
                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                b1 = pb0[1];
                tmp1 += a0 * b1;

                b2 = pb0[2];
                tmp2 += a0 * b2;

                b3 = pb0[3];
                tmp3 += a0 * b3;

                pa0 += 1;
                pb0 += 4;
            }

            tmp0 = alpha * tmp0;
            tmp1 = alpha * tmp1;
            tmp2 = alpha * tmp2;
            tmp3 = alpha * tmp3;

#if defined(TRMMKERNEL)
            pc0[0] = tmp0;
            pc1[0] = tmp1;
            pc2[0] = tmp2;
            pc3[0] = tmp3;
#else
            pc0[0] += tmp0;
            pc1[0] += tmp1;
            pc2[0] += tmp2;
            pc3[0] += tmp3;
#endif
            pc0 += 1;
            pc1 += 1;
            pc2 += 1;
            pc3 += 1;

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 1; // number of values in A
#else
            temp -= 4; // number of values in B
#endif
            pa0 += temp * 1;
            pb0 += temp * 4;
#endif

#ifdef LEFT
            off += 1; // number of values in A
#endif
#endif  // #if defined(TRMMKERNEL)
        }

#if defined(TRMMKERNEL) && !defined(LEFT)
        off += 4; // number of values in A
#endif

        B += (k << 2);
        C += (ldc << 2);
    }
}

static void __attribute__ ((noinline))
dgemmkernel_8x4_non_core_msa(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT alpha,
                             FLOAT *A, FLOAT *B, FLOAT *C, BLASLONG ldc
#ifdef TRMMKERNEL
          , BLASLONG offset
#endif
                             )
{
    BLASLONG i, l, temp;
#if defined(TRMMKERNEL)
    BLASLONG off;
#endif
    FLOAT *pc0, *pc1, *pa0, *pb0;
    FLOAT tmp0, tmp1;
    FLOAT a0, b0, b1;
    v2f64 v_alpha = {alpha, alpha};
    v2f64 src_a0, src_a1, src_a2, src_a3, src_b, src_b0;
    v2f64 dst0, dst1, dst2, dst3, dst4, dst5, dst6, dst7;
    v2f64 res0, res1, res2, res3, res4, res5, res6, res7;

#if defined(TRMMKERNEL) && !defined(LEFT)
    off = -offset + (4 * (n >> 2));
#endif

    if (n & 2)
    {
        pc0 = C;
        pc1 = pc0 + ldc;

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
            pa0 += off * 8;
            pb0 = B + off * 2;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 8; // number of values in A
#else
            temp = off + 2; // number of values in B
#endif
#else  // #if !defined(TRMMKERNEL)
            pb0 = B;
            temp = k;
#endif

            LD_DP4_INC(pa0, 2, src_a0, src_a1, src_a2, src_a3);
            src_b0 = LD_DP(pb0);
            pb0 += 2;

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
            res0 = src_a0 * src_b;
            res1 = src_a1 * src_b;
            res2 = src_a2 * src_b;
            res3 = src_a3 * src_b;

            src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
            res4 = src_a0 * src_b;
            res5 = src_a1 * src_b;
            res6 = src_a2 * src_b;
            res7 = src_a3 * src_b;

            for (l = ((temp - 1) >> 1); l--;)
            {
                LD_DP4_INC(pa0, 2, src_a0, src_a1, src_a2, src_a3);
                src_b0 = LD_DP(pb0);
                pb0 += 2;

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;
                res2 += src_a2 * src_b;
                res3 += src_a3 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
                res4 += src_a0 * src_b;
                res5 += src_a1 * src_b;
                res6 += src_a2 * src_b;
                res7 += src_a3 * src_b;

                LD_DP4_INC(pa0, 2, src_a0, src_a1, src_a2, src_a3);
                src_b0 = LD_DP(pb0);
                pb0 += 2;

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;
                res2 += src_a2 * src_b;
                res3 += src_a3 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
                res4 += src_a0 * src_b;
                res5 += src_a1 * src_b;
                res6 += src_a2 * src_b;
                res7 += src_a3 * src_b;
            }

            if ((temp - 1) & 1)
            {
                LD_DP4_INC(pa0, 2, src_a0, src_a1, src_a2, src_a3);
                src_b0 = LD_DP(pb0);
                pb0 += 2;

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;
                res2 += src_a2 * src_b;
                res3 += src_a3 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
                res4 += src_a0 * src_b;
                res5 += src_a1 * src_b;
                res6 += src_a2 * src_b;
                res7 += src_a3 * src_b;
            }

#if defined(TRMMKERNEL)
            dst0 = res0 * v_alpha;
            dst1 = res1 * v_alpha;
            dst2 = res2 * v_alpha;
            dst3 = res3 * v_alpha;
            dst4 = res4 * v_alpha;
            dst5 = res5 * v_alpha;
            dst6 = res6 * v_alpha;
            dst7 = res7 * v_alpha;
#else
            LD_DP4(pc0, 2, dst0, dst1, dst2, dst3);
            LD_DP4(pc1, 2, dst4, dst5, dst6, dst7);

            dst0 += res0 * v_alpha;
            dst1 += res1 * v_alpha;
            dst2 += res2 * v_alpha;
            dst3 += res3 * v_alpha;
            dst4 += res4 * v_alpha;
            dst5 += res5 * v_alpha;
            dst6 += res6 * v_alpha;
            dst7 += res7 * v_alpha;
#endif
            ST_DP4_INC(dst0, dst1, dst2, dst3, pc0, 2);
            ST_DP4_INC(dst4, dst5, dst6, dst7, pc1, 2);

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 8; // number of values in A
#else
            temp -= 2; // number of values in B
#endif
            pa0 += temp * 8;
            pb0 += temp * 2;
#endif

#ifdef LEFT
            off += 8; // number of values in A
#endif
#endif  // #if defined(TRMMKERNEL)
        }

        if (m & 4)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 4;
            pb0 = B + off * 2;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 4; // number of values in A
#else
            temp = off + 2; // number of values in B
#endif
#else  // #if !defined(TRMMKERNEL)
            pb0 = B;
            temp = k;
#endif

            LD_DP2_INC(pa0, 2, src_a0, src_a1);
            src_b0 = LD_DP(pb0);
            pb0 += 2;

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
            res0 = src_a0 * src_b;
            res1 = src_a1 * src_b;

            src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
            res2 = src_a0 * src_b;
            res3 = src_a1 * src_b;

            for (l = ((temp - 1) >> 1); l--;)
            {
                LD_DP2_INC(pa0, 2, src_a0, src_a1);
                src_b0 = LD_DP(pb0);
                pb0 += 2;

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
                res2 += src_a0 * src_b;
                res3 += src_a1 * src_b;

                LD_DP2_INC(pa0, 2, src_a0, src_a1);
                src_b0 = LD_DP(pb0);
                pb0 += 2;

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
                res2 += src_a0 * src_b;
                res3 += src_a1 * src_b;
            }

            if ((temp - 1) & 1)
            {
                LD_DP2_INC(pa0, 2, src_a0, src_a1);
                src_b0 = LD_DP(pb0);
                pb0 += 2;

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
                res2 += src_a0 * src_b;
                res3 += src_a1 * src_b;
            }

#if defined(TRMMKERNEL)
            dst0 = res0 * v_alpha;
            dst1 = res1 * v_alpha;
            dst2 = res2 * v_alpha;
            dst3 = res3 * v_alpha;
#else
            LD_DP2(pc0, 2, dst0, dst1);
            LD_DP2(pc1, 2, dst2, dst3);

            dst0 += res0 * v_alpha;
            dst1 += res1 * v_alpha;
            dst2 += res2 * v_alpha;
            dst3 += res3 * v_alpha;
#endif
            ST_DP2_INC(dst0, dst1, pc0, 2);
            ST_DP2_INC(dst2, dst3, pc1, 2);

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 4; // number of values in A
#else
            temp -= 2; // number of values in B
#endif
            pa0 += temp * 4;
            pb0 += temp * 2;
#endif

#ifdef LEFT
            off += 4; // number of values in A
#endif
#endif  // #if defined(TRMMKERNEL)
        }

        if (m & 2)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 2;
            pb0 = B + off * 2;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 2; // number of values in A
#else
            temp = off + 2; // number of values in B
#endif
#else  // #if !defined(TRMMKERNEL)
            pb0 = B;
            temp = k;
#endif

            src_a0 = LD_DP(pa0);
            pa0 += 2;
            src_b0 = LD_DP(pb0);
            pb0 += 2;

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
            res0 = src_a0 * src_b;

            src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
            res1 = src_a0 * src_b;

            for (l = ((temp - 1) >> 1); l--;)
            {
                src_a0 = LD_DP(pa0);
                pa0 += 2;
                src_b0 = LD_DP(pb0);
                pb0 += 2;

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
                res0 += src_a0 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
                res1 += src_a0 * src_b;

                src_a0 = LD_DP(pa0);
                pa0 += 2;
                src_b0 = LD_DP(pb0);
                pb0 += 2;

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
                res0 += src_a0 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
                res1 += src_a0 * src_b;
            }

            if ((temp - 1) & 1)
            {
                src_a0 = LD_DP(pa0);
                pa0 += 2;
                src_b0 = LD_DP(pb0);
                pb0 += 2;

                src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
                res0 += src_a0 * src_b;

                src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
                res1 += src_a0 * src_b;
            }

#if defined(TRMMKERNEL)
            dst0 = res0 * v_alpha;
            dst1 = res1 * v_alpha;
#else
            dst0 = LD_DP(pc0);
            dst1 = LD_DP(pc1);

            dst0 += res0 * v_alpha;
            dst1 += res1 * v_alpha;
#endif
            ST_DP(dst0, pc0);
            ST_DP(dst1, pc1);

            pc0 += 2;
            pc1 += 2;

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 2; // number of values in A
#else
            temp -= 2; // number of values in B
#endif
            pa0 += temp * 2;
            pb0 += temp * 2;
#endif

#ifdef LEFT
            off += 2; // number of values in A
#endif
#endif  // #if defined(TRMMKERNEL)
        }

        if (m & 1)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 1;
            pb0 = B + off * 2;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 1; // number of values in A
#else
            temp = off + 2; // number of values in B
#endif
#else  // #if !defined(TRMMKERNEL)
            pb0 = B;
            temp = k;
#endif

            a0 = pa0[0];
            b0 = pb0[0];
            tmp0 = a0 * b0;

            b1 = pb0[1];
            tmp1 = a0 * b1;

            pa0 += 1;
            pb0 += 2;

            for (l = ((temp - 1) >> 1); l--;)
            {
                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                b1 = pb0[1];
                tmp1 += a0 * b1;

                pa0 += 1;
                pb0 += 2;

                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                b1 = pb0[1];
                tmp1 += a0 * b1;

                pa0 += 1;
                pb0 += 2;
            }

            if ((temp - 1) & 1)
            {
                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                b1 = pb0[1];
                tmp1 += a0 * b1;

                pa0 += 1;
                pb0 += 2;
            }

            tmp0 = alpha * tmp0;
            tmp1 = alpha * tmp1;

#if defined(TRMMKERNEL)
            pc0[0] = tmp0;
            pc1[0] = tmp1;
#else
            pc0[0] += tmp0;
            pc1[0] += tmp1;
#endif
            pc0 += 1;
            pc1 += 1;

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 1; // number of values in A
#else
            temp -= 2; // number of values in B
#endif
            pa0 += temp * 1;
            pb0 += temp * 2;
#endif

#ifdef LEFT
            off += 1; // number of values in A
#endif
#endif  // #if defined(TRMMKERNEL)
        }

#if defined(TRMMKERNEL) && !defined(LEFT)
        off += 2; // number of values in A
#endif

        B += (k << 1);
        C += (ldc << 1);
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
            pa0 += off * 8;
            pb0 = B + off * 1;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 8; // number of values in A
#else
            temp = off + 1; // number of values in B
#endif
#else  // #if !defined(TRMMKERNEL)
            pb0 = B;
            temp = k;
#endif

            LD_DP4_INC(pa0, 2, src_a0, src_a1, src_a2, src_a3);
            src_b[0] = pb0[0];
            src_b[1] = pb0[0];

            res0 = src_a0 * src_b;
            res1 = src_a1 * src_b;
            res2 = src_a2 * src_b;
            res3 = src_a3 * src_b;

            pb0 += 1;

            for (l = ((temp - 1) >> 1); l--;)
            {
                LD_DP4_INC(pa0, 2, src_a0, src_a1, src_a2, src_a3);
                src_b[0] = pb0[0];
                src_b[1] = pb0[0];

                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;
                res2 += src_a2 * src_b;
                res3 += src_a3 * src_b;

                pb0 += 1;

                LD_DP4_INC(pa0, 2, src_a0, src_a1, src_a2, src_a3);
                src_b[0] = pb0[0];
                src_b[1] = pb0[0];

                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;
                res2 += src_a2 * src_b;
                res3 += src_a3 * src_b;

                pb0 += 1;
            }

            if ((temp - 1) & 1)
            {
                LD_DP4_INC(pa0, 2, src_a0, src_a1, src_a2, src_a3);
                src_b[0] = pb0[0];
                src_b[1] = pb0[0];

                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;
                res2 += src_a2 * src_b;
                res3 += src_a3 * src_b;

                pb0 += 1;
            }

#if defined(TRMMKERNEL)
            dst0 = res0 * v_alpha;
            dst1 = res1 * v_alpha;
            dst2 = res2 * v_alpha;
            dst3 = res3 * v_alpha;
#else
            LD_DP4(pc0, 2, dst0, dst1, dst2, dst3);

            dst0 += res0 * v_alpha;
            dst1 += res1 * v_alpha;
            dst2 += res2 * v_alpha;
            dst3 += res3 * v_alpha;
#endif
            ST_DP4_INC(dst0, dst1, dst2, dst3, pc0, 2);

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 8; // number of values in A
#else
            temp -= 1; // number of values in B
#endif
            pa0 += temp * 8;
            pb0 += temp * 1;
#endif

#ifdef LEFT
            off += 8; // number of values in A
#endif
#endif  // #if defined(TRMMKERNEL)
        }

        if (m & 4)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 4;
            pb0 = B + off * 1;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 4; // number of values in A
#else
            temp = off + 1; // number of values in B
#endif
#else  // #if !defined(TRMMKERNEL)
            pb0 = B;
            temp = k;
#endif

            LD_DP2_INC(pa0, 2, src_a0, src_a1);
            src_b[0] = pb0[0];
            src_b[1] = pb0[0];

            res0 = src_a0 * src_b;
            res1 = src_a1 * src_b;

            pb0 += 1;

            for (l = ((temp - 1) >> 1); l--;)
            {
                LD_DP2_INC(pa0, 2, src_a0, src_a1);
                src_b[0] = pb0[0];
                src_b[1] = pb0[0];

                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                pb0 += 1;

                LD_DP2_INC(pa0, 2, src_a0, src_a1);
                src_b[0] = pb0[0];
                src_b[1] = pb0[0];

                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                pb0 += 1;
            }

            if ((temp - 1) & 1)
            {
                LD_DP2_INC(pa0, 2, src_a0, src_a1);
                src_b[0] = pb0[0];
                src_b[1] = pb0[0];

                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                pb0 += 1;
            }

#if defined(TRMMKERNEL)
            dst0 = res0 * v_alpha;
            dst1 = res1 * v_alpha;
#else
            LD_DP2(pc0, 2, dst0, dst1);

            dst0 += res0 * v_alpha;
            dst1 += res1 * v_alpha;
#endif
            ST_DP2_INC(dst0, dst1, pc0, 2);

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 4; // number of values in A
#else
            temp -= 1; // number of values in B
#endif
            pa0 += temp * 4;
            pb0 += temp * 1;
#endif

#ifdef LEFT
            off += 4; // number of values in A
#endif
#endif  // #if defined(TRMMKERNEL)
        }

        if (m & 2)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 2;
            pb0 = B + off * 1;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 2; // number of values in A
#else
            temp = off + 1; // number of values in B
#endif
#else  // #if !defined(TRMMKERNEL)
            pb0 = B;
            temp = k;
#endif

            src_a0 = LD_DP(pa0);
            src_b[0] = pb0[0];
            src_b[1] = pb0[0];

            res0 = src_a0 * src_b;

            pa0 += 2;
            pb0 += 1;

            for (l = ((temp - 1) >> 1); l--;)
            {
                src_a0 = LD_DP(pa0);
                src_b[0] = pb0[0];
                src_b[1] = pb0[0];

                res0 += src_a0 * src_b;

                pa0 += 2;
                pb0 += 1;

                src_a0 = LD_DP(pa0);
                src_b[0] = pb0[0];
                src_b[1] = pb0[0];

                res0 += src_a0 * src_b;

                pa0 += 2;
                pb0 += 1;
            }

            if ((temp - 1) & 1)
            {
                src_a0 = LD_DP(pa0);
                src_b[0] = pb0[0];
                src_b[1] = pb0[0];

                res0 += src_a0 * src_b;

                pa0 += 2;
                pb0 += 1;
            }

#if defined(TRMMKERNEL)
            dst0 = res0 * v_alpha;
#else
            dst0 = LD_DP(pc0);

            dst0 += res0 * v_alpha;
#endif
            ST_DP(dst0, pc0);

            pc0 += 2;

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 2; // number of values in A
#else
            temp -= 1; // number of values in B
#endif
            pa0 += temp * 2;
            pb0 += temp * 1;
#endif

#ifdef LEFT
            off += 2; // number of values in A
#endif
#endif  // #if defined(TRMMKERNEL)
        }

        if (m & 1)
        {
#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            pb0 = B;
#else
            pa0 += off * 1;
            pb0 = B + off * 1;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 1; // number of values in A
#else
            temp = off + 1; // number of values in B
#endif
#else  // #if !defined(TRMMKERNEL)
            pb0 = B;
            temp = k;
#endif

            a0 = pa0[0];
            b0 = pb0[0];
            tmp0 = a0 * b0;

            pa0 += 1;
            pb0 += 1;

            for (l = ((temp - 1) >> 1); l--;)
            {
                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                pa0 += 1;
                pb0 += 1;

                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                pa0 += 1;
                pb0 += 1;
            }

            if ((temp - 1) & 1)
            {
                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                pa0 += 1;
                pb0 += 1;
            }

#if defined(TRMMKERNEL)
            pc0[0] = alpha * tmp0;
#else
            pc0[0] += alpha * tmp0;
#endif
        }
    }
}

int CNAME(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT alpha, FLOAT *A, FLOAT *B,
          FLOAT *C, BLASLONG ldc
#ifdef TRMMKERNEL
          , BLASLONG offset
#endif
          )
{
    if (n >> 2)
    {
        if (m >> 3)
#ifdef TRMMKERNEL
            dgemmkernel_8x4_core_msa(m, n, k, alpha, A, B, C, ldc, offset);
#else
            dgemmkernel_8x4_core_msa(m, n, k, alpha, A, B, C, ldc);
#endif

        if (m & 7)
#ifdef TRMMKERNEL
            dgemmkernel_7x4_core_msa(m, n, k, alpha, A, B, C, ldc, offset);
#else
            dgemmkernel_7x4_core_msa(m, n, k, alpha, A, B, C, ldc);
#endif
    }

    if (n & 3)
    {
        B = B + (k << 2) * (n >> 2);
        C = C + (ldc << 2) * (n >> 2);

#ifdef TRMMKERNEL
        dgemmkernel_8x4_non_core_msa(m, n, k, alpha, A, B, C, ldc, offset);
#else
        dgemmkernel_8x4_non_core_msa(m, n, k, alpha, A, B, C, ldc);
#endif
    }

    return 0;
}
