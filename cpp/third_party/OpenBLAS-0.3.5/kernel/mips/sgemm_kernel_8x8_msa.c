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

int CNAME(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT alpha, FLOAT *A, FLOAT *B,
          FLOAT *C, BLASLONG ldc
#ifdef TRMMKERNEL
          , BLASLONG offset
#endif
          )
{
    BLASLONG i, j, l, temp;
#if defined(TRMMKERNEL)
    BLASLONG off;
#endif
    FLOAT *pc0, *pc1, *pc2, *pc3, *pc4, *pc5, *pc6, *pc7;
    FLOAT *pa0, *pb0;
    FLOAT tmp0, tmp1, tmp2, tmp3, tmp4, tmp5, tmp6, tmp7;
    FLOAT tmp8, tmp9, tmp10, tmp11, tmp12, tmp13, tmp14, tmp15;
    FLOAT a0, a1, b0, b1, b2, b3, b4, b5, b6, b7;
    v4f32 v_alpha = {alpha, alpha, alpha, alpha};
    v4f32 src_a0, src_a1, src_b, src_b0, src_b1;
    v4f32 dst0, dst1, dst2, dst3, dst4, dst5, dst6, dst7;
    v4f32 res0, res1, res2, res3, res4, res5, res6, res7;
    v4f32 res8, res9, res10, res11, res12, res13, res14, res15;

#if defined(TRMMKERNEL) && !defined(LEFT)
    off = -offset;
#endif

    for (j = (n >> 3); j--;)
    {
        pc0 = C;
        pc1 = pc0 + ldc;
        pc2 = pc1 + ldc;
        pc3 = pc2 + ldc;
        pc4 = pc3 + ldc;
        pc5 = pc4 + ldc;
        pc6 = pc5 + ldc;
        pc7 = pc6 + ldc;

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
            pb0 = B + off * 8;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 8; // number of values in A
#else
            temp = off + 8; // number of values in B
#endif
#else
            pb0 = B;
            temp = k;
#endif
#ifdef ENABLE_PREFETCH
            __asm__ __volatile__(
                "pref   0,   32(%[pa0])   \n\t"
                "pref   0,   32(%[pb0])   \n\t"

                :
                : [pa0] "r" (pa0), [pb0] "r" (pb0)
            );
#endif

            LD_SP2_INC(pa0, 4, src_a0, src_a1);
            LD_SP2_INC(pb0, 4, src_b0, src_b1);

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
            res0 = src_a0 * src_b;
            res1 = src_a1 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
            res2 = src_a0 * src_b;
            res3 = src_a1 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xAA);
            res4 = src_a0 * src_b;
            res5 = src_a1 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xFF);
            res6 = src_a0 * src_b;
            res7 = src_a1 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0);
            res8 = src_a0 * src_b;
            res9 = src_a1 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0x55);
            res10 = src_a0 * src_b;
            res11 = src_a1 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0xAA);
            res12 = src_a0 * src_b;
            res13 = src_a1 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0xFF);
            res14 = src_a0 * src_b;
            res15 = src_a1 * src_b;

            for (l = ((temp - 1) >> 1); l--;)
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

                LD_SP2_INC(pa0, 4, src_a0, src_a1);
                LD_SP2_INC(pb0, 4, src_b0, src_b1);

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
                res2 += src_a0 * src_b;
                res3 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xAA);
                res4 += src_a0 * src_b;
                res5 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xFF);
                res6 += src_a0 * src_b;
                res7 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0);
                res8 += src_a0 * src_b;
                res9 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0x55);
                res10 += src_a0 * src_b;
                res11 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0xAA);
                res12 += src_a0 * src_b;
                res13 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0xFF);
                res14 += src_a0 * src_b;
                res15 += src_a1 * src_b;

                LD_SP2_INC(pa0, 4, src_a0, src_a1);
                LD_SP2_INC(pb0, 4, src_b0, src_b1);

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
                res2 += src_a0 * src_b;
                res3 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xAA);
                res4 += src_a0 * src_b;
                res5 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xFF);
                res6 += src_a0 * src_b;
                res7 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0);
                res8 += src_a0 * src_b;
                res9 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0x55);
                res10 += src_a0 * src_b;
                res11 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0xAA);
                res12 += src_a0 * src_b;
                res13 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0xFF);
                res14 += src_a0 * src_b;
                res15 += src_a1 * src_b;
            }

            if ((temp - 1) & 1)
            {
                LD_SP2_INC(pa0, 4, src_a0, src_a1);
                LD_SP2_INC(pb0, 4, src_b0, src_b1);

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
                res2 += src_a0 * src_b;
                res3 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xAA);
                res4 += src_a0 * src_b;
                res5 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xFF);
                res6 += src_a0 * src_b;
                res7 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0);
                res8 += src_a0 * src_b;
                res9 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0x55);
                res10 += src_a0 * src_b;
                res11 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0xAA);
                res12 += src_a0 * src_b;
                res13 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0xFF);
                res14 += src_a0 * src_b;
                res15 += src_a1 * src_b;
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
            LD_SP2(pc0, 4, dst0, dst1);
            LD_SP2(pc1, 4, dst2, dst3);
            LD_SP2(pc2, 4, dst4, dst5);
            LD_SP2(pc3, 4, dst6, dst7);

            dst0 += res0 * v_alpha;
            dst1 += res1 * v_alpha;
            dst2 += res2 * v_alpha;
            dst3 += res3 * v_alpha;
            dst4 += res4 * v_alpha;
            dst5 += res5 * v_alpha;
            dst6 += res6 * v_alpha;
            dst7 += res7 * v_alpha;
#endif
            ST_SP2_INC(dst0, dst1, pc0, 4);
            ST_SP2_INC(dst2, dst3, pc1, 4);
            ST_SP2_INC(dst4, dst5, pc2, 4);
            ST_SP2_INC(dst6, dst7, pc3, 4);

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
            LD_SP2(pc4, 4, dst0, dst1);
            LD_SP2(pc5, 4, dst2, dst3);
            LD_SP2(pc6, 4, dst4, dst5);
            LD_SP2(pc7, 4, dst6, dst7);

            dst0 += res8 * v_alpha;
            dst1 += res9 * v_alpha;
            dst2 += res10 * v_alpha;
            dst3 += res11 * v_alpha;
            dst4 += res12 * v_alpha;
            dst5 += res13 * v_alpha;
            dst6 += res14 * v_alpha;
            dst7 += res15 * v_alpha;
#endif
            ST_SP2_INC(dst0, dst1, pc4, 4);
            ST_SP2_INC(dst2, dst3, pc5, 4);
            ST_SP2_INC(dst4, dst5, pc6, 4);
            ST_SP2_INC(dst6, dst7, pc7, 4);

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 8; // number of values in A
#else
            temp -= 8; // number of values in B
#endif
            pa0 += temp * 8;
            pb0 += temp * 8;
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
            pa0 += off * 4;
            pb0 = B + off * 8;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 4; // number of values in A
#else
            temp = off + 8; // number of values in B
#endif
#else
            pb0 = B;
            temp = k;
#endif

            src_a0 = LD_SP(pa0);
            LD_SP2_INC(pb0, 4, src_b0, src_b1);

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
            res0 = src_a0 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
            res1 = src_a0 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xAA);
            res2 = src_a0 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xFF);
            res3 = src_a0 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0);
            res4 = src_a0 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0x55);
            res5 = src_a0 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0xAA);
            res6 = src_a0 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0xFF);
            res7 = src_a0 * src_b;

            pa0 += 4;

            for (l = ((temp - 1) >> 1); l--;)
            {
                src_a0 = LD_SP(pa0);
                LD_SP2_INC(pb0, 4, src_b0, src_b1);

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
                res1 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xAA);
                res2 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xFF);
                res3 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0);
                res4 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0x55);
                res5 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0xAA);
                res6 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0xFF);
                res7 += src_a0 * src_b;

                pa0 += 4;

                src_a0 = LD_SP(pa0);
                LD_SP2_INC(pb0, 4, src_b0, src_b1);

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
                res1 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xAA);
                res2 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xFF);
                res3 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0);
                res4 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0x55);
                res5 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0xAA);
                res6 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0xFF);
                res7 += src_a0 * src_b;

                pa0 += 4;
            }

            if ((temp - 1) & 1)
            {
                src_a0 = LD_SP(pa0);
                LD_SP2_INC(pb0, 4, src_b0, src_b1);

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
                res1 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xAA);
                res2 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xFF);
                res3 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0);
                res4 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0x55);
                res5 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0xAA);
                res6 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b1, 0xFF);
                res7 += src_a0 * src_b;

                pa0 += 4;
            }

#if defined(TRMMKERNEL)
            dst0 = res0 * v_alpha;
            dst1 = res1 * v_alpha;
            dst2 = res2 * v_alpha;
            dst3 = res3 * v_alpha;
#else
            dst0 = LD_SP(pc0);
            dst1 = LD_SP(pc1);
            dst2 = LD_SP(pc2);
            dst3 = LD_SP(pc3);

            dst0 += res0 * v_alpha;
            dst1 += res1 * v_alpha;
            dst2 += res2 * v_alpha;
            dst3 += res3 * v_alpha;
#endif
            ST_SP(dst0, pc0);
            ST_SP(dst1, pc1);
            ST_SP(dst2, pc2);
            ST_SP(dst3, pc3);

#if defined(TRMMKERNEL)
            dst0 = res4 * v_alpha;
            dst1 = res5 * v_alpha;
            dst2 = res6 * v_alpha;
            dst3 = res7 * v_alpha;
#else
            dst0 = LD_SP(pc4);
            dst1 = LD_SP(pc5);
            dst2 = LD_SP(pc6);
            dst3 = LD_SP(pc7);

            dst0 += res4 * v_alpha;
            dst1 += res5 * v_alpha;
            dst2 += res6 * v_alpha;
            dst3 += res7 * v_alpha;
#endif
            ST_SP(dst0, pc4);
            ST_SP(dst1, pc5);
            ST_SP(dst2, pc6);
            ST_SP(dst3, pc7);

            pc0 += 4;
            pc1 += 4;
            pc2 += 4;
            pc3 += 4;
            pc4 += 4;
            pc5 += 4;
            pc6 += 4;
            pc7 += 4;

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 4; // number of values in A
#else
            temp -= 8; // number of values in B
#endif
            pa0 += temp * 4;
            pb0 += temp * 8;
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
            pa0 += off * 2;
            pb0 = B + off * 8;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 2; // number of values in A
#else
            temp = off + 8; // number of values in B
#endif
#else
            pb0 = B;
            temp = k;
#endif

            a0 = pa0[0];
            b0 = pb0[0];
            tmp0 = a0 * b0;

            a1 = pa0[1];
            tmp1 = a1 * b0;

            b1 = pb0[1];
            tmp2 = a0 * b1;
            tmp3 = a1 * b1;

            b2 = pb0[2];
            tmp4 = a0 * b2;
            tmp5 = a1 * b2;

            b3 = pb0[3];
            tmp6 = a0 * b3;
            tmp7 = a1 * b3;

            b4 = pb0[4];
            tmp8 = a0 * b4;
            tmp9 = a1 * b4;

            b5 = pb0[5];
            tmp10 = a0 * b5;
            tmp11 = a1 * b5;

            b6 = pb0[6];
            tmp12 = a0 * b6;
            tmp13 = a1 * b6;

            b7 = pb0[7];
            tmp14 = a0 * b7;
            tmp15 = a1 * b7;

            pa0 += 2;
            pb0 += 8;

            for (l = ((temp - 1) >> 1); l--;)
            {
                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                a1 = pa0[1];
                tmp1 += a1 * b0;

                b1 = pb0[1];
                tmp2 += a0 * b1;
                tmp3 += a1 * b1;

                b2 = pb0[2];
                tmp4 += a0 * b2;
                tmp5 += a1 * b2;

                b3 = pb0[3];
                tmp6 += a0 * b3;
                tmp7 += a1 * b3;

                b4 = pb0[4];
                tmp8 += a0 * b4;
                tmp9 += a1 * b4;

                b5 = pb0[5];
                tmp10 += a0 * b5;
                tmp11 += a1 * b5;

                b6 = pb0[6];
                tmp12 += a0 * b6;
                tmp13 += a1 * b6;

                b7 = pb0[7];
                tmp14 += a0 * b7;
                tmp15 += a1 * b7;

                pa0 += 2;
                pb0 += 8;

                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                a1 = pa0[1];
                tmp1 += a1 * b0;

                b1 = pb0[1];
                tmp2 += a0 * b1;
                tmp3 += a1 * b1;

                b2 = pb0[2];
                tmp4 += a0 * b2;
                tmp5 += a1 * b2;

                b3 = pb0[3];
                tmp6 += a0 * b3;
                tmp7 += a1 * b3;

                b4 = pb0[4];
                tmp8 += a0 * b4;
                tmp9 += a1 * b4;

                b5 = pb0[5];
                tmp10 += a0 * b5;
                tmp11 += a1 * b5;

                b6 = pb0[6];
                tmp12 += a0 * b6;
                tmp13 += a1 * b6;

                b7 = pb0[7];
                tmp14 += a0 * b7;
                tmp15 += a1 * b7;

                pa0 += 2;
                pb0 += 8;
            }

            if ((temp - 1) & 1)
            {
                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                a1 = pa0[1];
                tmp1 += a1 * b0;

                b1 = pb0[1];
                tmp2 += a0 * b1;
                tmp3 += a1 * b1;

                b2 = pb0[2];
                tmp4 += a0 * b2;
                tmp5 += a1 * b2;

                b3 = pb0[3];
                tmp6 += a0 * b3;
                tmp7 += a1 * b3;

                b4 = pb0[4];
                tmp8 += a0 * b4;
                tmp9 += a1 * b4;

                b5 = pb0[5];
                tmp10 += a0 * b5;
                tmp11 += a1 * b5;

                b6 = pb0[6];
                tmp12 += a0 * b6;
                tmp13 += a1 * b6;

                b7 = pb0[7];
                tmp14 += a0 * b7;
                tmp15 += a1 * b7;

                pa0 += 2;
                pb0 += 8;
            }

            tmp0 = alpha * tmp0;
            tmp2 = alpha * tmp2;
            tmp4 = alpha * tmp4;
            tmp6 = alpha * tmp6;
            tmp8 = alpha * tmp8;
            tmp10 = alpha * tmp10;
            tmp12 = alpha * tmp12;
            tmp14 = alpha * tmp14;

#if defined(TRMMKERNEL)
            pc0[0] = tmp0;
            pc1[0] = tmp2;
            pc2[0] = tmp4;
            pc3[0] = tmp6;
            pc4[0] = tmp8;
            pc5[0] = tmp10;
            pc6[0] = tmp12;
            pc7[0] = tmp14;
#else
            pc0[0] += tmp0;
            pc1[0] += tmp2;
            pc2[0] += tmp4;
            pc3[0] += tmp6;
            pc4[0] += tmp8;
            pc5[0] += tmp10;
            pc6[0] += tmp12;
            pc7[0] += tmp14;
#endif
            tmp1 = alpha * tmp1;
            tmp3 = alpha * tmp3;
            tmp5 = alpha * tmp5;
            tmp7 = alpha * tmp7;
            tmp9 = alpha * tmp9;
            tmp11 = alpha * tmp11;
            tmp13 = alpha * tmp13;
            tmp15 = alpha * tmp15;

#if defined(TRMMKERNEL)
            pc0[1] = tmp1;
            pc1[1] = tmp3;
            pc2[1] = tmp5;
            pc3[1] = tmp7;
            pc4[1] = tmp9;
            pc5[1] = tmp11;
            pc6[1] = tmp13;
            pc7[1] = tmp15;
#else
            pc0[1] += tmp1;
            pc1[1] += tmp3;
            pc2[1] += tmp5;
            pc3[1] += tmp7;
            pc4[1] += tmp9;
            pc5[1] += tmp11;
            pc6[1] += tmp13;
            pc7[1] += tmp15;
#endif
            pc0 += 2;
            pc1 += 2;
            pc2 += 2;
            pc3 += 2;
            pc4 += 2;
            pc5 += 2;
            pc6 += 2;
            pc7 += 2;

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 2; // number of values in A
#else
            temp -= 8; // number of values in B
#endif
            pa0 += temp * 2;
            pb0 += temp * 8;
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
            pa0 += off * 1;
            pb0 = B + off * 8;
#endif

#if (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
            temp = k - off;
#elif defined(LEFT)
            temp = off + 1; // number of values in A
#else
            temp = off + 8; // number of values in B
#endif
#else
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

            b4 = pb0[4];
            tmp4 = a0 * b4;

            b5 = pb0[5];
            tmp5 = a0 * b5;

            b6 = pb0[6];
            tmp6 = a0 * b6;

            b7 = pb0[7];
            tmp7 = a0 * b7;

            pa0 += 1;
            pb0 += 8;

            for (l = ((temp - 1) >> 1); l--;)
            {
                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                b1 = pb0[1];
                tmp1  += a0 * b1;

                b2 = pb0[2];
                tmp2 += a0 * b2;

                b3 = pb0[3];
                tmp3 += a0 * b3;

                b4 = pb0[4];
                tmp4 += a0 * b4;

                b5 = pb0[5];
                tmp5 += a0 * b5;

                b6 = pb0[6];
                tmp6 += a0 * b6;

                b7 = pb0[7];
                tmp7 += a0 * b7;

                pa0 += 1;
                pb0 += 8;

                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                b1 = pb0[1];
                tmp1  += a0 * b1;

                b2 = pb0[2];
                tmp2 += a0 * b2;

                b3 = pb0[3];
                tmp3 += a0 * b3;

                b4 = pb0[4];
                tmp4 += a0 * b4;

                b5 = pb0[5];
                tmp5 += a0 * b5;

                b6 = pb0[6];
                tmp6 += a0 * b6;

                b7 = pb0[7];
                tmp7 += a0 * b7;

                pa0 += 1;
                pb0 += 8;
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

                b4 = pb0[4];
                tmp4 += a0 * b4;

                b5 = pb0[5];
                tmp5 += a0 * b5;

                b6 = pb0[6];
                tmp6 += a0 * b6;

                b7 = pb0[7];
                tmp7 += a0 * b7;

                pa0 += 1;
                pb0 += 8;
            }

            tmp0 = alpha * tmp0;
            tmp1 = alpha * tmp1;
            tmp2 = alpha * tmp2;
            tmp3 = alpha * tmp3;
            tmp4 = alpha * tmp4;
            tmp5 = alpha * tmp5;
            tmp6 = alpha * tmp6;
            tmp7 = alpha * tmp7;

#if defined(TRMMKERNEL)
            pc0[0] = tmp0;
            pc1[0] = tmp1;
            pc2[0] = tmp2;
            pc3[0] = tmp3;
            pc4[0] = tmp4;
            pc5[0] = tmp5;
            pc6[0] = tmp6;
            pc7[0] = tmp7;
#else
            pc0[0] += tmp0;
            pc1[0] += tmp1;
            pc2[0] += tmp2;
            pc3[0] += tmp3;
            pc4[0] += tmp4;
            pc5[0] += tmp5;
            pc6[0] += tmp6;
            pc7[0] += tmp7;
#endif
            pc0 += 1;
            pc1 += 1;
            pc2 += 1;
            pc3 += 1;
            pc4 += 1;
            pc5 += 1;
            pc6 += 1;
            pc7 += 1;

#if defined(TRMMKERNEL)
#if (defined(LEFT) && defined(TRANSA)) || (!defined(LEFT) && !defined(TRANSA))
            temp = k - off;
#ifdef LEFT
            temp -= 1; // number of values in A
#else
            temp -= 8; // number of values in B
#endif
            pa0 += temp * 1;
            pb0 += temp * 8;
#endif

#ifdef LEFT
            off += 1; // number of values in A
#endif
#endif
        }

#if defined(TRMMKERNEL) && !defined(LEFT)
        off += 8; // number of values in A
#endif

        B += (k << 3);
        C += (ldc << 3);
    }

    if (n & 4)
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
#else
            pb0 = B;
            temp = k;
#endif

            LD_SP2_INC(pa0, 4, src_a0, src_a1);
            src_b0 = LD_SP(pb0);

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
            res0 = src_a0 * src_b;
            res1 = src_a1 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
            res2 = src_a0 * src_b;
            res3 = src_a1 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xAA);
            res4 = src_a0 * src_b;
            res5 = src_a1 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xFF);
            res6 = src_a0 * src_b;
            res7 = src_a1 * src_b;

            pb0 += 4;

            for (l = ((temp - 1) >> 1); l--;)
            {
                LD_SP2_INC(pa0, 4, src_a0, src_a1);
                src_b0 = LD_SP(pb0);

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
                res2 += src_a0 * src_b;
                res3 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xAA);
                res4 += src_a0 * src_b;
                res5 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xFF);
                res6 += src_a0 * src_b;
                res7 += src_a1 * src_b;

                pb0 += 4;

                LD_SP2_INC(pa0, 4, src_a0, src_a1);
                src_b0 = LD_SP(pb0);

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
                res2 += src_a0 * src_b;
                res3 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xAA);
                res4 += src_a0 * src_b;
                res5 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xFF);
                res6 += src_a0 * src_b;
                res7 += src_a1 * src_b;

                pb0 += 4;
            }

            if ((temp - 1) & 1)
            {
                LD_SP2_INC(pa0, 4, src_a0, src_a1);
                src_b0 = LD_SP(pb0);

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
                res2 += src_a0 * src_b;
                res3 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xAA);
                res4 += src_a0 * src_b;
                res5 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xFF);
                res6 += src_a0 * src_b;
                res7 += src_a1 * src_b;

                pb0 += 4;
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
            LD_SP2(pc0, 4, dst0, dst1);
            LD_SP2(pc1, 4, dst2, dst3);
            LD_SP2(pc2, 4, dst4, dst5);
            LD_SP2(pc3, 4, dst6, dst7);

            dst0 += res0 * v_alpha;
            dst1 += res1 * v_alpha;
            dst2 += res2 * v_alpha;
            dst3 += res3 * v_alpha;
            dst4 += res4 * v_alpha;
            dst5 += res5 * v_alpha;
            dst6 += res6 * v_alpha;
            dst7 += res7 * v_alpha;
#endif
            ST_SP2_INC(dst0, dst1, pc0, 4);
            ST_SP2_INC(dst2, dst3, pc1, 4);
            ST_SP2_INC(dst4, dst5, pc2, 4);
            ST_SP2_INC(dst6, dst7, pc3, 4);

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
#endif
        }

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
#else
            pb0 = B;
            temp = k;
#endif

            src_a0 = LD_SP(pa0);
            src_b0 = LD_SP(pb0);

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
            res0 = src_a0 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
            res1 = src_a0 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xAA);
            res2 = src_a0 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xFF);
            res3 = src_a0 * src_b;

            pa0 += 4;
            pb0 += 4;

            for (l = ((temp - 1) >> 1); l--;)
            {
                src_a0 = LD_SP(pa0);
                src_b0 = LD_SP(pb0);

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
                res1 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xAA);
                res2 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xFF);
                res3 += src_a0 * src_b;

                pa0 += 4;
                pb0 += 4;

                src_a0 = LD_SP(pa0);
                src_b0 = LD_SP(pb0);

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
                res1 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xAA);
                res2 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xFF);
                res3 += src_a0 * src_b;

                pa0 += 4;
                pb0 += 4;
            }

            if ((temp - 1) & 1)
            {
                src_a0 = LD_SP(pa0);
                src_b0 = LD_SP(pb0);

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
                res1 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xAA);
                res2 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0xFF);
                res3 += src_a0 * src_b;

                pa0 += 4;
                pb0 += 4;
            }

#if defined(TRMMKERNEL)
            dst0 = res0 * v_alpha;
            dst1 = res1 * v_alpha;
            dst2 = res2 * v_alpha;
            dst3 = res3 * v_alpha;
#else
            dst0 = LD_SP(pc0);
            dst1 = LD_SP(pc1);
            dst2 = LD_SP(pc2);
            dst3 = LD_SP(pc3);

            dst0 += res0 * v_alpha;
            dst1 += res1 * v_alpha;
            dst2 += res2 * v_alpha;
            dst3 += res3 * v_alpha;
#endif
            ST_SP(dst0, pc0);
            ST_SP(dst1, pc1);
            ST_SP(dst2, pc2);
            ST_SP(dst3, pc3);

            pc0 += 4;
            pc1 += 4;
            pc2 += 4;
            pc3 += 4;

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
#endif
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
#else
            pb0 = B;
            temp = k;
#endif

            a0 = pa0[0];
            b0 = pb0[0];
            tmp0 = a0 * b0;

            a1 = pa0[1];
            tmp1 = a1 * b0;

            b1 = pb0[1];
            tmp2 = a0 * b1;
            tmp3 = a1 * b1;

            b2 = pb0[2];
            tmp4 = a0 * b2;
            tmp5 = a1 * b2;

            b3 = pb0[3];
            tmp6 = a0 * b3;
            tmp7 = a1 * b3;

            pa0 += 2;
            pb0 += 4;

            for (l = ((temp - 1) >> 1); l--;)
            {
                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                a1 = pa0[1];
                tmp1 += a1 * b0;

                b1 = pb0[1];
                tmp2  += a0 * b1;
                tmp3  += a1 * b1;

                b2 = pb0[2];
                tmp4 += a0 * b2;
                tmp5 += a1 * b2;

                b3 = pb0[3];
                tmp6 += a0 * b3;
                tmp7 += a1 * b3;

                pa0 += 2;
                pb0 += 4;

                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                a1 = pa0[1];
                tmp1 += a1 * b0;

                b1 = pb0[1];
                tmp2  += a0 * b1;
                tmp3  += a1 * b1;

                b2 = pb0[2];
                tmp4 += a0 * b2;
                tmp5 += a1 * b2;

                b3 = pb0[3];
                tmp6 += a0 * b3;
                tmp7 += a1 * b3;

                pa0 += 2;
                pb0 += 4;
            }

            if ((temp - 1) & 1)
            {
                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                a1 = pa0[1];
                tmp1 += a1 * b0;

                b1 = pb0[1];
                tmp2  += a0 * b1;
                tmp3  += a1 * b1;

                b2 = pb0[2];
                tmp4 += a0 * b2;
                tmp5 += a1 * b2;

                b3 = pb0[3];
                tmp6 += a0 * b3;
                tmp7 += a1 * b3;

                pa0 += 2;
                pb0 += 4;
            }

            tmp0 = alpha * tmp0;
            tmp2 = alpha * tmp2;
            tmp4 = alpha * tmp4;
            tmp6 = alpha * tmp6;

#if defined(TRMMKERNEL)
            pc0[0] = tmp0;
            pc1[0] = tmp2;
            pc2[0] = tmp4;
            pc3[0] = tmp6;
#else
            pc0[0] += tmp0;
            pc1[0] += tmp2;
            pc2[0] += tmp4;
            pc3[0] += tmp6;
#endif
            tmp1 = alpha * tmp1;
            tmp3 = alpha * tmp3;
            tmp5 = alpha * tmp5;
            tmp7 = alpha * tmp7;

#if defined(TRMMKERNEL)
            pc0[1] = tmp1;
            pc1[1] = tmp3;
            pc2[1] = tmp5;
            pc3[1] = tmp7;
#else
            pc0[1] += tmp1;
            pc1[1] += tmp3;
            pc2[1] += tmp5;
            pc3[1] += tmp7;
#endif
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
#endif
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
#else
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
                tmp1  += a0 * b1;

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
                tmp1  += a0 * b1;

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
                tmp1  += a0 * b1;

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
#endif
        }

#if defined(TRMMKERNEL) && !defined(LEFT)
        off += 4; // number of values in A
#endif

        B += (k << 2);
        C += (ldc << 2);
    }

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
#else
            pb0 = B;
            temp = k;
#endif

            LD_SP2_INC(pa0, 4, src_a0, src_a1);
            src_b0[0] = pb0[0];
            src_b0[1] = pb0[1];

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
            res0 = src_a0 * src_b;
            res1 = src_a1 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
            res2 = src_a0 * src_b;
            res3 = src_a1 * src_b;

            pb0 += 2;

            for (l = ((temp - 1) >> 1); l--;)
            {
                LD_SP2_INC(pa0, 4, src_a0, src_a1);
                src_b0[0] = pb0[0];
                src_b0[1] = pb0[1];

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
                res2 += src_a0 * src_b;
                res3 += src_a1 * src_b;

                pb0 += 2;

                LD_SP2_INC(pa0, 4, src_a0, src_a1);
                src_b0[0] = pb0[0];
                src_b0[1] = pb0[1];

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
                res2 += src_a0 * src_b;
                res3 += src_a1 * src_b;

                pb0 += 2;
            }

            if ((temp - 1) & 1)
            {
                LD_SP2_INC(pa0, 4, src_a0, src_a1);
                src_b0[0] = pb0[0];
                src_b0[1] = pb0[1];

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
                res2 += src_a0 * src_b;
                res3 += src_a1 * src_b;

                pb0 += 2;
            }

#if defined(TRMMKERNEL)
            dst0 = res0 * v_alpha;
            dst1 = res1 * v_alpha;
            dst2 = res2 * v_alpha;
            dst3 = res3 * v_alpha;
#else
            LD_SP2(pc0, 4, dst0, dst1);
            LD_SP2(pc1, 4, dst2, dst3);

            dst0 += res0 * v_alpha;
            dst1 += res1 * v_alpha;
            dst2 += res2 * v_alpha;
            dst3 += res3 * v_alpha;
#endif
            ST_SP2_INC(dst0, dst1, pc0, 4);
            ST_SP2_INC(dst2, dst3, pc1, 4);

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
#endif
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
#else
            pb0 = B;
            temp = k;
#endif

            src_a0 = LD_SP(pa0);
            src_b0[0] = pb0[0];
            src_b0[1] = pb0[1];

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
            res0 = src_a0 * src_b;

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
            res1 = src_a0 * src_b;

            pa0 += 4;
            pb0 += 2;

            for (l = ((temp - 1) >> 1); l--;)
            {
                src_a0 = LD_SP(pa0);
                src_b0[0] = pb0[0];
                src_b0[1] = pb0[1];

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
                res1 += src_a0 * src_b;

                pa0 += 4;
                pb0 += 2;

                src_a0 = LD_SP(pa0);
                src_b0[0] = pb0[0];
                src_b0[1] = pb0[1];

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
                res1 += src_a0 * src_b;

                pa0 += 4;
                pb0 += 2;
            }

            if ((temp - 1) & 1)
            {
                src_a0 = LD_SP(pa0);
                src_b0[0] = pb0[0];
                src_b0[1] = pb0[1];

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0x55);
                res1 += src_a0 * src_b;

                pa0 += 4;
                pb0 += 2;
            }

#if defined(TRMMKERNEL)
            dst0 = res0 * v_alpha;
            dst1 = res1 * v_alpha;
#else
            dst0 = LD_SP(pc0);
            dst1 = LD_SP(pc1);

            dst0 += res0 * v_alpha;
            dst1 += res1 * v_alpha;
#endif
            ST_SP(dst0, pc0);
            ST_SP(dst1, pc1);

            pc0 += 4;
            pc1 += 4;

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
#endif
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
#else
            pb0 = B;
            temp = k;
#endif

            a0 = pa0[0];
            b0 = pb0[0];
            tmp0 = a0 * b0;

            a1 = pa0[1];
            tmp1 = a1 * b0;

            b1 = pb0[1];
            tmp2 = a0 * b1;
            tmp3 = a1 * b1;

            pa0 += 2;
            pb0 += 2;

            for (l = ((temp - 1) >> 1); l--;)
            {
                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                a1 = pa0[1];
                tmp1 += a1 * b0;

                b1 = pb0[1];
                tmp2  += a0 * b1;
                tmp3  += a1 * b1;

                pa0 += 2;
                pb0 += 2;

                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                a1 = pa0[1];
                tmp1 += a1 * b0;

                b1 = pb0[1];
                tmp2  += a0 * b1;
                tmp3  += a1 * b1;

                pa0 += 2;
                pb0 += 2;
            }

            if ((temp - 1) & 1)
            {
                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                a1 = pa0[1];
                tmp1 += a1 * b0;

                b1 = pb0[1];
                tmp2  += a0 * b1;
                tmp3  += a1 * b1;

                pa0 += 2;
                pb0 += 2;
            }

            tmp0 = alpha * tmp0;
            tmp1 = alpha * tmp1;
            tmp2 = alpha * tmp2;
            tmp3 = alpha * tmp3;

#if defined(TRMMKERNEL)
            pc0[0] = tmp0;
            pc1[0] = tmp2;
            pc0[1] = tmp1;
            pc1[1] = tmp3;
#else
            pc0[0] += tmp0;
            pc1[0] += tmp2;
            pc0[1] += tmp1;
            pc1[1] += tmp3;
#endif
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
#endif
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
#else
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
                tmp1  += a0 * b1;

                pa0 += 1;
                pb0 += 2;

                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                b1 = pb0[1];
                tmp1  += a0 * b1;

                pa0 += 1;
                pb0 += 2;
            }

            if ((temp - 1) & 1)
            {
                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                b1 = pb0[1];
                tmp1  += a0 * b1;

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
#endif
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
#else
            pb0 = B;
            temp = k;
#endif

            LD_SP2_INC(pa0, 4, src_a0, src_a1);
            src_b0[0] = pb0[0];

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
            res0 = src_a0 * src_b;
            res1 = src_a1 * src_b;

            pb0 += 1;

            for (l = ((temp - 1) >> 1); l--;)
            {
                LD_SP2_INC(pa0, 4, src_a0, src_a1);
                src_b0[0] = pb0[0];

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                pb0 += 1;

                LD_SP2_INC(pa0, 4, src_a0, src_a1);
                src_b0[0] = pb0[0];

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                pb0 += 1;
            }

            if ((temp - 1) & 1)
            {
                LD_SP2_INC(pa0, 4, src_a0, src_a1);
                src_b0[0] = pb0[0];

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;
                res1 += src_a1 * src_b;

                pb0 += 1;
            }

#if defined(TRMMKERNEL)
            dst0 = res0 * v_alpha;
            dst1 = res1 * v_alpha;
#else
            LD_SP2(pc0, 4, dst0, dst1);

            dst0 += res0 * v_alpha;
            dst1 += res1 * v_alpha;
#endif
            ST_SP2_INC(dst0, dst1, pc0, 4);

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
#endif
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
#else
            pb0 = B;
            temp = k;
#endif

            src_a0 = LD_SP(pa0);
            src_b0[0] = pb0[0];

            src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
            res0 = src_a0 * src_b;

            pa0 += 4;
            pb0 += 1;

            for (l = ((temp - 1) >> 1); l--;)
            {
                src_a0 = LD_SP(pa0);
                src_b0[0] = pb0[0];

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;

                pa0 += 4;
                pb0 += 1;

                src_a0 = LD_SP(pa0);
                src_b0[0] = pb0[0];

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;

                pa0 += 4;
                pb0 += 1;
            }

            if ((temp - 1) & 1)
            {
                src_a0 = LD_SP(pa0);
                src_b0[0] = pb0[0];

                src_b = (v4f32) __msa_shf_w((v4i32) src_b0, 0);
                res0 += src_a0 * src_b;

                pa0 += 4;
                pb0 += 1;
            }

#if defined(TRMMKERNEL)
            dst0 = res0 * v_alpha;
#else
            dst0 = LD_SP(pc0);

            dst0 += res0 * v_alpha;
#endif
            ST_SP(dst0, pc0);

            pc0 += 4;

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
#endif
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
#else
            pb0 = B;
            temp = k;
#endif

            a0 = pa0[0];
            b0 = pb0[0];
            tmp0 = a0 * b0;

            a1 = pa0[1];
            tmp1 = a1 * b0;

            pa0 += 2;
            pb0 += 1;

            for (l = ((temp - 1) >> 1); l--;)
            {
                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                a1 = pa0[1];
                tmp1 += a1 * b0;

                pa0 += 2;
                pb0 += 1;

                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                a1 = pa0[1];
                tmp1 += a1 * b0;

                pa0 += 2;
                pb0 += 1;
            }

            if ((temp - 1) & 1)
            {
                a0 = pa0[0];
                b0 = pb0[0];
                tmp0 += a0 * b0;

                a1 = pa0[1];
                tmp1 += a1 * b0;

                pa0 += 2;
                pb0 += 1;
            }

            tmp0 = alpha * tmp0;
            tmp1 = alpha * tmp1;

#if defined(TRMMKERNEL)
            pc0[0] = tmp0;
            pc0[1] = tmp1;
#else
            pc0[0] += tmp0;
            pc0[1] += tmp1;
#endif
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
#endif
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
#else
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

    return 0;
}
