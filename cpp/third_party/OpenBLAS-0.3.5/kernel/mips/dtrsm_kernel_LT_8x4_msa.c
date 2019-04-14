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

static __attribute__ ((noinline))
void dsolve_8x4_lt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    v2f64 src_c0, src_c1, src_c2, src_c3, src_c4, src_c5, src_c6, src_c7;
    v2f64 src_c8, src_c9, src_c10, src_c11, src_c12, src_c13, src_c14, src_c15;
    v2f64 res_c0, res_c1, res_c2, res_c3, res_c4, res_c5, res_c6, res_c7;
    v2f64 res_c8, res_c9, res_c10, res_c11, res_c12, res_c13, res_c14, res_c15;
    v2f64 src_a0, src_a1, src_a2, src_a3, src_a4, src_a5, src_a6, src_a7;
    v2f64 src_a9, src_a10, src_a11, src_a12, src_a13, src_a14, src_a15, src_a18;
    v2f64 src_a19, src_a20, src_a21, src_a22, src_a23, src_a27, src_a28;
    v2f64 src_a29, src_a30, src_a31, src_a36, src_a37, src_a38, src_a39;
    v2f64 src_a45, src_a46, src_a47, src_a54, src_a55, src_a63;
    FLOAT *c_nxt1line = c + ldc;
    FLOAT *c_nxt2line = c + 2 * ldc;
    FLOAT *c_nxt3line = c + 3 * ldc;

    a += bk * 8;
    PREF_OFFSET(a,   0);
    PREF_OFFSET(a,  32);
    PREF_OFFSET(a,  72);
    PREF_OFFSET(a, 104);
    PREF_OFFSET(a, 144);
    PREF_OFFSET(a, 176);
    PREF_OFFSET(a, 216);
    PREF_OFFSET(a, 248);
    PREF_OFFSET(a, 288);
    PREF_OFFSET(a, 360);
    PREF_OFFSET(a, 504);
    PREF_OFFSET(a, 432);
    a -= bk * 8;

    LD_DP4(c, 2, src_c0, src_c1, src_c2, src_c3);
    LD_DP4(c_nxt1line, 2, src_c4, src_c5, src_c6, src_c7);
    LD_DP4(c_nxt2line, 2, src_c8, src_c9, src_c10, src_c11);
    LD_DP4(c_nxt3line, 2, src_c12, src_c13, src_c14, src_c15);

    if (bk)
    {
        BLASLONG i, pref_offset;
        FLOAT *pa0_pref;
        v2f64 src_b, src_b0, src_b1;

        pref_offset = (uintptr_t)a & (L1_DATA_LINESIZE - 1);

        if (pref_offset)
        {
            pref_offset = L1_DATA_LINESIZE - pref_offset;
            pref_offset = pref_offset / sizeof(FLOAT);
        }

        pa0_pref = a + pref_offset;

        for (i = (bk >> 1); i--;)
        {
            PREF_OFFSET(pa0_pref, 128);
            PREF_OFFSET(pa0_pref, 160);
            PREF_OFFSET(pa0_pref, 192);
            PREF_OFFSET(pa0_pref, 224);

            LD_DP4_INC(a, 2, src_a0, src_a1, src_a2, src_a3);
            LD_DP2_INC(b, 2, src_b0, src_b1);

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
            src_c0 -= src_a0 * src_b;
            src_c1 -= src_a1 * src_b;
            src_c2 -= src_a2 * src_b;
            src_c3 -= src_a3 * src_b;

            src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
            src_c4 -= src_a0 * src_b;
            src_c5 -= src_a1 * src_b;
            src_c6 -= src_a2 * src_b;
            src_c7 -= src_a3 * src_b;

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b1, (v2i64) src_b1);
            src_c8  -= src_a0 * src_b;
            src_c9  -= src_a1 * src_b;
            src_c10 -= src_a2 * src_b;
            src_c11 -= src_a3 * src_b;

            src_b = (v2f64) __msa_ilvl_d((v2i64) src_b1, (v2i64) src_b1);
            src_c12 -= src_a0 * src_b;
            src_c13 -= src_a1 * src_b;
            src_c14 -= src_a2 * src_b;
            src_c15 -= src_a3 * src_b;

            LD_DP4_INC(a, 2, src_a0, src_a1, src_a2, src_a3);
            LD_DP2_INC(b, 2, src_b0, src_b1);

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
            src_c0 -= src_a0 * src_b;
            src_c1 -= src_a1 * src_b;
            src_c2 -= src_a2 * src_b;
            src_c3 -= src_a3 * src_b;

            src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
            src_c4 -= src_a0 * src_b;
            src_c5 -= src_a1 * src_b;
            src_c6 -= src_a2 * src_b;
            src_c7 -= src_a3 * src_b;

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b1, (v2i64) src_b1);
            src_c8  -= src_a0 * src_b;
            src_c9  -= src_a1 * src_b;
            src_c10 -= src_a2 * src_b;
            src_c11 -= src_a3 * src_b;

            src_b = (v2f64) __msa_ilvl_d((v2i64) src_b1, (v2i64) src_b1);
            src_c12 -= src_a0 * src_b;
            src_c13 -= src_a1 * src_b;
            src_c14 -= src_a2 * src_b;
            src_c15 -= src_a3 * src_b;

            pa0_pref += 16;
        }

        if (bk & 1)
        {
            LD_DP4_INC(a, 2, src_a0, src_a1, src_a2, src_a3);
            LD_DP2_INC(b, 2, src_b0, src_b1);

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
            src_c0 -= src_a0 * src_b;
            src_c1 -= src_a1 * src_b;
            src_c2 -= src_a2 * src_b;
            src_c3 -= src_a3 * src_b;

            src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
            src_c4 -= src_a0 * src_b;
            src_c5 -= src_a1 * src_b;
            src_c6 -= src_a2 * src_b;
            src_c7 -= src_a3 * src_b;

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b1, (v2i64) src_b1);
            src_c8  -= src_a0 * src_b;
            src_c9  -= src_a1 * src_b;
            src_c10 -= src_a2 * src_b;
            src_c11 -= src_a3 * src_b;

            src_b = (v2f64) __msa_ilvl_d((v2i64) src_b1, (v2i64) src_b1);
            src_c12 -= src_a0 * src_b;
            src_c13 -= src_a1 * src_b;
            src_c14 -= src_a2 * src_b;
            src_c15 -= src_a3 * src_b;
        }
    }

    ILVRL_D2_DP(src_c4, src_c0, res_c0, res_c1);
    ILVRL_D2_DP(src_c5, src_c1, res_c2, res_c3);
    ILVRL_D2_DP(src_c6, src_c2, res_c4, res_c5);
    ILVRL_D2_DP(src_c7, src_c3, res_c6, res_c7);
    ILVRL_D2_DP(src_c12, src_c8, res_c8, res_c9);
    ILVRL_D2_DP(src_c13, src_c9, res_c10, res_c11);
    ILVRL_D2_DP(src_c14, src_c10, res_c12, res_c13);
    ILVRL_D2_DP(src_c15, src_c11, res_c14, res_c15);

    src_a0 = LD_DP(a + 0);
    src_a1 = (v2f64) __msa_splati_d((v2i64) src_a0, 1);
    src_a0 = (v2f64) __msa_splati_d((v2i64) src_a0, 0);
    src_a2 = LD_DP(a + 2);
    src_a3 = (v2f64) __msa_splati_d((v2i64) src_a2, 1);
    src_a2 = (v2f64) __msa_splati_d((v2i64) src_a2, 0);
    src_a4 = LD_DP(a + 4);
    src_a5 = (v2f64) __msa_splati_d((v2i64) src_a4, 1);
    src_a4 = (v2f64) __msa_splati_d((v2i64) src_a4, 0);
    src_a6 = LD_DP(a + 6);
    src_a7 = (v2f64) __msa_splati_d((v2i64) src_a6, 1);
    src_a6 = (v2f64) __msa_splati_d((v2i64) src_a6, 0);

    res_c0 *= src_a0;
    res_c1 -= res_c0 * src_a1;
    res_c2 -= res_c0 * src_a2;
    res_c3 -= res_c0 * src_a3;
    res_c4 -= res_c0 * src_a4;
    res_c5 -= res_c0 * src_a5;
    res_c6 -= res_c0 * src_a6;
    res_c7 -= res_c0 * src_a7;

    res_c8 *= src_a0;
    res_c9 -= res_c8 * src_a1;
    res_c10 -= res_c8 * src_a2;
    res_c11 -= res_c8 * src_a3;
    res_c12 -= res_c8 * src_a4;
    res_c13 -= res_c8 * src_a5;
    res_c14 -= res_c8 * src_a6;
    res_c15 -= res_c8 * src_a7;

    src_a9 = __msa_cast_to_vector_double(*(a + 9));
    src_a9 = (v2f64) __msa_splati_d((v2i64) src_a9, 0);
    src_a10 = LD_DP(a + 10);
    src_a11 = (v2f64) __msa_splati_d((v2i64) src_a10, 1);
    src_a10 = (v2f64) __msa_splati_d((v2i64) src_a10, 0);
    src_a12 = LD_DP(a + 12);
    src_a13 = (v2f64) __msa_splati_d((v2i64) src_a12, 1);
    src_a12 = (v2f64) __msa_splati_d((v2i64) src_a12, 0);
    src_a14 = LD_DP(a + 14);
    src_a15 = (v2f64) __msa_splati_d((v2i64) src_a14, 1);
    src_a14 = (v2f64) __msa_splati_d((v2i64) src_a14, 0);

    res_c1 *= src_a9;
    res_c2 -= res_c1 * src_a10;
    res_c3 -= res_c1 * src_a11;
    res_c4 -= res_c1 * src_a12;
    res_c5 -= res_c1 * src_a13;
    res_c6 -= res_c1 * src_a14;
    res_c7 -= res_c1 * src_a15;

    res_c9 *= src_a9;
    res_c10 -= res_c9 * src_a10;
    res_c11 -= res_c9 * src_a11;
    res_c12 -= res_c9 * src_a12;
    res_c13 -= res_c9 * src_a13;
    res_c14 -= res_c9 * src_a14;
    res_c15 -= res_c9 * src_a15;

    ST_DP(res_c0, b + 0);
    ST_DP(res_c8, b + 2);
    ST_DP(res_c1, b + 4);
    ST_DP(res_c9, b + 6);

    ILVRL_D2_DP(res_c1, res_c0, src_c0, src_c4);
    ILVRL_D2_DP(res_c9, res_c8, src_c8, src_c12);

    ST_DP(src_c0, c);
    ST_DP(src_c4, c_nxt1line);
    ST_DP(src_c8, c_nxt2line);
    ST_DP(src_c12, c_nxt3line);

    src_a18 = LD_DP(a + 18);
    src_a19 = (v2f64) __msa_splati_d((v2i64) src_a18, 1);
    src_a18 = (v2f64) __msa_splati_d((v2i64) src_a18, 0);
    src_a20 = LD_DP(a + 20);
    src_a21 = (v2f64) __msa_splati_d((v2i64) src_a20, 1);
    src_a20 = (v2f64) __msa_splati_d((v2i64) src_a20, 0);
    src_a22 = LD_DP(a + 22);
    src_a23 = (v2f64) __msa_splati_d((v2i64) src_a22, 1);
    src_a22 = (v2f64) __msa_splati_d((v2i64) src_a22, 0);

    res_c2 *= src_a18;
    res_c3 -= res_c2 * src_a19;
    res_c4 -= res_c2 * src_a20;
    res_c5 -= res_c2 * src_a21;
    res_c6 -= res_c2 * src_a22;
    res_c7 -= res_c2 * src_a23;

    res_c10 *= src_a18;
    res_c11 -= res_c10 * src_a19;
    res_c12 -= res_c10 * src_a20;
    res_c13 -= res_c10 * src_a21;
    res_c14 -= res_c10 * src_a22;
    res_c15 -= res_c10 * src_a23;

    src_a27 = __msa_cast_to_vector_double(*(a + 27));
    src_a27 = (v2f64) __msa_splati_d((v2i64) src_a27, 0);
    src_a28 = LD_DP(a + 28);
    src_a29 = (v2f64) __msa_splati_d((v2i64) src_a28, 1);
    src_a28 = (v2f64) __msa_splati_d((v2i64) src_a28, 0);
    src_a30 = LD_DP(a + 30);
    src_a31 = (v2f64) __msa_splati_d((v2i64) src_a30, 1);
    src_a30 = (v2f64) __msa_splati_d((v2i64) src_a30, 0);

    res_c3 *= src_a27;
    res_c4 -= res_c3 * src_a28;
    res_c5 -= res_c3 * src_a29;
    res_c6 -= res_c3 * src_a30;
    res_c7 -= res_c3 * src_a31;

    res_c11 *= src_a27;
    res_c12 -= res_c11 * src_a28;
    res_c13 -= res_c11 * src_a29;
    res_c14 -= res_c11 * src_a30;
    res_c15 -= res_c11 * src_a31;

    ST_DP(res_c2, b + 8);
    ST_DP(res_c10, b + 10);
    ST_DP(res_c3, b + 12);
    ST_DP(res_c11, b + 14);

    ILVRL_D2_DP(res_c3, res_c2, src_c1, src_c5);
    ILVRL_D2_DP(res_c11, res_c10, src_c9, src_c13);

    src_a36 = LD_DP(a + 36);
    src_a37 = (v2f64) __msa_splati_d((v2i64) src_a36, 1);
    src_a36 = (v2f64) __msa_splati_d((v2i64) src_a36, 0);
    src_a38 = LD_DP(a + 38);
    src_a39 = (v2f64) __msa_splati_d((v2i64) src_a38, 1);
    src_a38 = (v2f64) __msa_splati_d((v2i64) src_a38, 0);

    res_c4 *= src_a36;
    res_c5 -= res_c4 * src_a37;
    res_c6 -= res_c4 * src_a38;
    res_c7 -= res_c4 * src_a39;

    res_c12 *= src_a36;
    res_c13 -= res_c12 * src_a37;
    res_c14 -= res_c12 * src_a38;
    res_c15 -= res_c12 * src_a39;

    src_a45 = __msa_cast_to_vector_double(*(a + 45));
    src_a45 = (v2f64) __msa_splati_d((v2i64) src_a45, 0);
    src_a46 = LD_DP(a + 46);
    src_a47 = (v2f64) __msa_splati_d((v2i64) src_a46, 1);
    src_a46 = (v2f64) __msa_splati_d((v2i64) src_a46, 0);

    res_c5 *= src_a45;
    res_c6 -= res_c5 * src_a46;
    res_c7 -= res_c5 * src_a47;

    res_c13 *= src_a45;
    res_c14 -= res_c13 * src_a46;
    res_c15 -= res_c13 * src_a47;

    ST_DP(src_c1, c + 2);
    ST_DP(src_c5, c_nxt1line + 2);
    ST_DP(src_c9, c_nxt2line + 2);
    ST_DP(src_c13, c_nxt3line + 2);

    ST_DP(res_c4, b + 16);
    ST_DP(res_c12, b + 18);
    ST_DP(res_c5, b + 20);
    ST_DP(res_c13, b + 22);

    ILVRL_D2_DP(res_c5, res_c4, src_c2, src_c6);
    ILVRL_D2_DP(res_c13, res_c12, src_c10, src_c14);

    src_a63 = __msa_cast_to_vector_double(*(a + 63));
    src_a63 = (v2f64) __msa_splati_d((v2i64) src_a63, 0);
    src_a54 = LD_DP(a + 54);
    src_a55 = (v2f64) __msa_splati_d((v2i64) src_a54, 1);
    src_a54 = (v2f64) __msa_splati_d((v2i64) src_a54, 0);

    res_c6 *= src_a54;
    res_c7 -= res_c6 * src_a55;

    res_c14 *= src_a54;
    res_c15 -= res_c14 * src_a55;

    res_c7 *= src_a63;
    res_c15 *= src_a63;

    ST_DP(src_c2, c + 4);
    ST_DP(src_c6, c_nxt1line + 4);
    ST_DP(src_c10, c_nxt2line + 4);
    ST_DP(src_c14, c_nxt3line + 4);

    ST_DP(res_c6, b + 24);
    ST_DP(res_c14, b + 26);
    ST_DP(res_c7, b + 28);
    ST_DP(res_c15, b + 30);

    ILVRL_D2_DP(res_c7, res_c6, src_c3, src_c7);
    ILVRL_D2_DP(res_c15, res_c14, src_c11, src_c15);

    ST_DP(src_c3, c + 6);
    ST_DP(src_c7, c_nxt1line + 6);
    ST_DP(src_c11, c_nxt2line + 6);
    ST_DP(src_c15, c_nxt3line + 6);
}

static void dsolve_8x2_lt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    v2f64 src_c0, src_c1, src_c2, src_c3, src_c4, src_c5, src_c6, src_c7;
    v2f64 res_c0, res_c1, res_c2, res_c3, res_c4, res_c5, res_c6, res_c7;
    v2f64 src_a0, src_a1, src_a2, src_a3, src_a4, src_a5, src_a6, src_a7;
    v2f64 src_a9, src_a10, src_a11, src_a12, src_a13, src_a14, src_a15, src_a18;
    v2f64 src_a19, src_a20, src_a21, src_a22, src_a23, src_a27, src_a28;
    v2f64 src_a29, src_a30, src_a31, src_a36, src_a37, src_a38, src_a39;
    v2f64 src_a45, src_a46, src_a47, src_a54, src_a55, src_a63;

    LD_DP4(c, 2, src_c0, src_c1, src_c2, src_c3);
    LD_DP4(c + ldc, 2, src_c4, src_c5, src_c6, src_c7);

    if (bk)
    {
        BLASLONG i;
        v2f64 src_b, src_b0, src_b1;

        LD_DP4(a, 2, src_a0, src_a1, src_a2, src_a3);
        src_b0 = LD_DP(b);

        a += 8;
        b += 2;

        for (i = (bk - 1); i--;)
        {
            LD_DP4(a, 2, src_a4, src_a5, src_a6, src_a7);
            src_b1 = LD_DP(b);

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
            src_c0 -= src_a0 * src_b;
            src_c1 -= src_a1 * src_b;
            src_c2 -= src_a2 * src_b;
            src_c3 -= src_a3 * src_b;

            src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
            src_c4 -= src_a0 * src_b;
            src_c5 -= src_a1 * src_b;
            src_c6 -= src_a2 * src_b;
            src_c7 -= src_a3 * src_b;

            src_a0 = src_a4;
            src_a1 = src_a5;
            src_a2 = src_a6;
            src_a3 = src_a7;
            src_b0 = src_b1;

            a += 8;
            b += 2;
        }

        src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
        src_c0 -= src_a0 * src_b;
        src_c1 -= src_a1 * src_b;
        src_c2 -= src_a2 * src_b;
        src_c3 -= src_a3 * src_b;

        src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
        src_c4 -= src_a0 * src_b;
        src_c5 -= src_a1 * src_b;
        src_c6 -= src_a2 * src_b;
        src_c7 -= src_a3 * src_b;
    }

    ILVRL_D2_DP(src_c4, src_c0, res_c0, res_c1);
    ILVRL_D2_DP(src_c5, src_c1, res_c2, res_c3);
    ILVRL_D2_DP(src_c6, src_c2, res_c4, res_c5);
    ILVRL_D2_DP(src_c7, src_c3, res_c6, res_c7);

    src_a0 = LD_DP(a + 0);
    src_a1 = (v2f64) __msa_splati_d((v2i64) src_a0, 1);
    src_a0 = (v2f64) __msa_splati_d((v2i64) src_a0, 0);
    src_a2 = LD_DP(a + 2);
    src_a3 = (v2f64) __msa_splati_d((v2i64) src_a2, 1);
    src_a2 = (v2f64) __msa_splati_d((v2i64) src_a2, 0);
    src_a4 = LD_DP(a + 4);
    src_a5 = (v2f64) __msa_splati_d((v2i64) src_a4, 1);
    src_a4 = (v2f64) __msa_splati_d((v2i64) src_a4, 0);
    src_a6 = LD_DP(a + 6);
    src_a7 = (v2f64) __msa_splati_d((v2i64) src_a6, 1);
    src_a6 = (v2f64) __msa_splati_d((v2i64) src_a6, 0);

    res_c0 *= src_a0;
    res_c1 -= res_c0 * src_a1;
    res_c2 -= res_c0 * src_a2;
    res_c3 -= res_c0 * src_a3;
    res_c4 -= res_c0 * src_a4;
    res_c5 -= res_c0 * src_a5;
    res_c6 -= res_c0 * src_a6;
    res_c7 -= res_c0 * src_a7;

    src_a9 = __msa_cast_to_vector_double(*(a + 9));
    src_a9 = (v2f64) __msa_splati_d((v2i64) src_a9, 0);
    src_a10 = LD_DP(a + 10);
    src_a11 = (v2f64) __msa_splati_d((v2i64) src_a10, 1);
    src_a10 = (v2f64) __msa_splati_d((v2i64) src_a10, 0);
    src_a12 = LD_DP(a + 12);
    src_a13 = (v2f64) __msa_splati_d((v2i64) src_a12, 1);
    src_a12 = (v2f64) __msa_splati_d((v2i64) src_a12, 0);
    src_a14 = LD_DP(a + 14);
    src_a15 = (v2f64) __msa_splati_d((v2i64) src_a14, 1);
    src_a14 = (v2f64) __msa_splati_d((v2i64) src_a14, 0);

    res_c1 *= src_a9;
    res_c2 -= res_c1 * src_a10;
    res_c3 -= res_c1 * src_a11;
    res_c4 -= res_c1 * src_a12;
    res_c5 -= res_c1 * src_a13;
    res_c6 -= res_c1 * src_a14;
    res_c7 -= res_c1 * src_a15;

    src_a18 = LD_DP(a + 18);
    src_a19 = (v2f64) __msa_splati_d((v2i64) src_a18, 1);
    src_a18 = (v2f64) __msa_splati_d((v2i64) src_a18, 0);
    src_a20 = LD_DP(a + 20);
    src_a21 = (v2f64) __msa_splati_d((v2i64) src_a20, 1);
    src_a20 = (v2f64) __msa_splati_d((v2i64) src_a20, 0);
    src_a22 = LD_DP(a + 22);
    src_a23 = (v2f64) __msa_splati_d((v2i64) src_a22, 1);
    src_a22 = (v2f64) __msa_splati_d((v2i64) src_a22, 0);

    res_c2 *= src_a18;
    res_c3 -= res_c2 * src_a19;
    res_c4 -= res_c2 * src_a20;
    res_c5 -= res_c2 * src_a21;
    res_c6 -= res_c2 * src_a22;
    res_c7 -= res_c2 * src_a23;

    src_a27 = __msa_cast_to_vector_double(*(a + 27));
    src_a27 = (v2f64) __msa_splati_d((v2i64) src_a27, 0);
    src_a28 = LD_DP(a + 28);
    src_a29 = (v2f64) __msa_splati_d((v2i64) src_a28, 1);
    src_a28 = (v2f64) __msa_splati_d((v2i64) src_a28, 0);
    src_a30 = LD_DP(a + 30);
    src_a31 = (v2f64) __msa_splati_d((v2i64) src_a30, 1);
    src_a30 = (v2f64) __msa_splati_d((v2i64) src_a30, 0);

    res_c3 *= src_a27;
    res_c4 -= res_c3 * src_a28;
    res_c5 -= res_c3 * src_a29;
    res_c6 -= res_c3 * src_a30;
    res_c7 -= res_c3 * src_a31;

    ST_DP(res_c0, b + 0);
    ST_DP(res_c1, b + 2);
    ST_DP(res_c2, b + 4);
    ST_DP(res_c3, b + 6);

    ILVRL_D2_DP(res_c1, res_c0, src_c0, src_c4);
    ILVRL_D2_DP(res_c3, res_c2, src_c1, src_c5);

    ST_DP2(src_c0, src_c1, c, 2);
    ST_DP2(src_c4, src_c5, c + ldc, 2);

    src_a36 = LD_DP(a + 36);
    src_a37 = (v2f64) __msa_splati_d((v2i64) src_a36, 1);
    src_a36 = (v2f64) __msa_splati_d((v2i64) src_a36, 0);
    src_a38 = LD_DP(a + 38);
    src_a39 = (v2f64) __msa_splati_d((v2i64) src_a38, 1);
    src_a38 = (v2f64) __msa_splati_d((v2i64) src_a38, 0);

    res_c4 *= src_a36;
    res_c5 -= res_c4 * src_a37;
    res_c6 -= res_c4 * src_a38;
    res_c7 -= res_c4 * src_a39;

    src_a45 = __msa_cast_to_vector_double(*(a + 45));
    src_a45 = (v2f64) __msa_splati_d((v2i64) src_a45, 0);
    src_a46 = LD_DP(a + 46);
    src_a47 = (v2f64) __msa_splati_d((v2i64) src_a46, 1);
    src_a46 = (v2f64) __msa_splati_d((v2i64) src_a46, 0);

    res_c5 *= src_a45;
    res_c6 -= res_c5 * src_a46;
    res_c7 -= res_c5 * src_a47;

    src_a63 = __msa_cast_to_vector_double(*(a + 63));
    src_a63 = (v2f64) __msa_splati_d((v2i64) src_a63, 0);
    src_a54 = LD_DP(a + 54);
    src_a55 = (v2f64) __msa_splati_d((v2i64) src_a54, 1);
    src_a54 = (v2f64) __msa_splati_d((v2i64) src_a54, 0);

    res_c6 *= src_a54;
    res_c7 -= res_c6 * src_a55;

    res_c7 *= src_a63;

    ST_DP(res_c4, b + 8);
    ST_DP(res_c5, b + 10);
    ST_DP(res_c6, b + 12);
    ST_DP(res_c7, b + 14);

    ILVRL_D2_DP(res_c5, res_c4, src_c2, src_c6);
    ILVRL_D2_DP(res_c7, res_c6, src_c3, src_c7);

    ST_DP2(src_c2, src_c3, c + 4, 2);
    ST_DP2(src_c6, src_c7, c + 4 + ldc, 2);
}

static void dsolve_8x1_lt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG bk)
{
    FLOAT a0, a1, a2, a3, a4, a5, a6, a7, a9, a10, a11, a12, a13, a14, a15, a18;
    FLOAT a19, a20, a21, a22, a23, a27, a28, a29, a30, a31, a36, a37, a38, a39;
    FLOAT a45, a46, a47, a54, a55, a63, c0, c1, c2, c3, c4, c5, c6, c7;

    c0 = *(c + 0);
    c1 = *(c + 1);
    c2 = *(c + 2);
    c3 = *(c + 3);
    c4 = *(c + 4);
    c5 = *(c + 5);
    c6 = *(c + 6);
    c7 = *(c + 7);

    if (bk)
    {
        BLASLONG i;

        for (i = bk; i--; )
        {
            c0 -= a[0] * b[0];
            c1 -= a[1] * b[0];
            c2 -= a[2] * b[0];
            c3 -= a[3] * b[0];
            c4 -= a[4] * b[0];
            c5 -= a[5] * b[0];
            c6 -= a[6] * b[0];
            c7 -= a[7] * b[0];

            a += 8;
            b += 1;
        }
    }

    a0 = *(a + 0);
    a1 = *(a + 1);
    a2 = *(a + 2);
    a3 = *(a + 3);
    a4 = *(a + 4);
    a5 = *(a + 5);
    a6 = *(a + 6);
    a7 = *(a + 7);
    a9 = *(a + 9);
    a10 = *(a + 10);
    a11 = *(a + 11);
    a12 = *(a + 12);
    a13 = *(a + 13);
    a14 = *(a + 14);
    a15 = *(a + 15);
    a18 = *(a + 18);
    a19 = *(a + 19);
    a20 = *(a + 20);
    a21 = *(a + 21);
    a22 = *(a + 22);
    a23 = *(a + 23);
    a27 = *(a + 27);
    a28 = *(a + 28);
    a29 = *(a + 29);
    a30 = *(a + 30);
    a31 = *(a + 31);
    a36 = *(a + 36);
    a37 = *(a + 37);
    a38 = *(a + 38);
    a39 = *(a + 39);
    a45 = *(a + 45);
    a46 = *(a + 46);
    a47 = *(a + 47);
    a54 = *(a + 54);
    a55 = *(a + 55);
    a63 = *(a + 63);

    c0 *= a0;

    c1 -= c0 * a1;
    c1 *= a9;

    c2 -= c0 * a2;
    c2 -= c1 * a10;
    c2 *= a18;

    c3 -= c0 * a3;
    c3 -= c1 * a11;
    c3 -= c2 * a19;
    c3 *= a27;

    c4 -= c0 * a4;
    c4 -= c1 * a12;
    c4 -= c2 * a20;
    c4 -= c3 * a28;
    c4 *= a36;

    c5 -= c0 * a5;
    c5 -= c1 * a13;
    c5 -= c2 * a21;
    c5 -= c3 * a29;
    c5 -= c4 * a37;
    c5 *= a45;

    c6 -= c0 * a6;
    c6 -= c1 * a14;
    c6 -= c2 * a22;
    c6 -= c3 * a30;
    c6 -= c4 * a38;
    c6 -= c5 * a46;
    c6 *= a54;

    c7 -= c0 * a7;
    c7 -= c1 * a15;
    c7 -= c2 * a23;
    c7 -= c3 * a31;
    c7 -= c4 * a39;
    c7 -= c5 * a47;
    c7 -= c6 * a55;
    c7 *= a63;

    *(c + 0) = c0;
    *(c + 1) = c1;
    *(c + 2) = c2;
    *(c + 3) = c3;
    *(c + 4) = c4;
    *(c + 5) = c5;
    *(c + 6) = c6;
    *(c + 7) = c7;

    *(b + 0) = c0;
    *(b + 1) = c1;
    *(b + 2) = c2;
    *(b + 3) = c3;
    *(b + 4) = c4;
    *(b + 5) = c5;
    *(b + 6) = c6;
    *(b + 7) = c7;
}

static void dsolve_4x4_lt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    v2f64 src_c0, src_c1, src_c2, src_c3, src_c4, src_c5, src_c6, src_c7;
    v2f64 res_c0, res_c1, res_c2, res_c3, res_c4, res_c5, res_c6, res_c7;
    v2f64 src_a0, src_a1, src_a2, src_a3, src_a5, src_a6, src_a7;
    v2f64 src_a10, src_a11, src_a15;

    LD_DP2(c, 2, src_c0, src_c1);
    LD_DP2(c + ldc, 2, src_c2, src_c3);
    LD_DP2(c + 2 * ldc, 2, src_c4, src_c5);
    LD_DP2(c + 3 * ldc, 2, src_c6, src_c7);

    if (bk)
    {
        BLASLONG i;
        v2f64 src_a0, src_a1, src_b, src_b0, src_b1;

        for (i = bk; i--;)
        {
            LD_DP2(a, 2, src_a0, src_a1);
            LD_DP2(b, 2, src_b0, src_b1);

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
            src_c0 -= src_a0 * src_b;
            src_c1 -= src_a1 * src_b;

            src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
            src_c2 -= src_a0 * src_b;
            src_c3 -= src_a1 * src_b;

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b1, (v2i64) src_b1);
            src_c4 -= src_a0 * src_b;
            src_c5 -= src_a1 * src_b;

            src_b = (v2f64) __msa_ilvl_d((v2i64) src_b1, (v2i64) src_b1);
            src_c6 -= src_a0 * src_b;
            src_c7 -= src_a1 * src_b;

            a += 4;
            b += 4;
        }
    }

    ILVRL_D2_DP(src_c2, src_c0, res_c0, res_c1);
    ILVRL_D2_DP(src_c3, src_c1, res_c2, res_c3);
    ILVRL_D2_DP(src_c6, src_c4, res_c4, res_c5);
    ILVRL_D2_DP(src_c7, src_c5, res_c6, res_c7);

    src_a0 = LD_DP(a + 0);
    src_a1 = (v2f64) __msa_splati_d((v2i64) src_a0, 1);
    src_a0 = (v2f64) __msa_splati_d((v2i64) src_a0, 0);
    src_a2 = LD_DP(a + 2);
    src_a3 = (v2f64) __msa_splati_d((v2i64) src_a2, 1);
    src_a2 = (v2f64) __msa_splati_d((v2i64) src_a2, 0);

    res_c0 *= src_a0;
    res_c1 -= res_c0 * src_a1;
    res_c2 -= res_c0 * src_a2;
    res_c3 -= res_c0 * src_a3;

    res_c4 *= src_a0;
    res_c5 -= res_c4 * src_a1;
    res_c6 -= res_c4 * src_a2;
    res_c7 -= res_c4 * src_a3;

    src_a5 = __msa_cast_to_vector_double(*(a + 5));
    src_a5 = (v2f64) __msa_splati_d((v2i64) src_a5, 0);
    src_a6 = LD_DP(a + 6);
    src_a7 = (v2f64) __msa_splati_d((v2i64) src_a6, 1);
    src_a6 = (v2f64) __msa_splati_d((v2i64) src_a6, 0);

    res_c1 *= src_a5;
    res_c2 -= res_c1 * src_a6;
    res_c3 -= res_c1 * src_a7;

    res_c5 *= src_a5;
    res_c6 -= res_c5 * src_a6;
    res_c7 -= res_c5 * src_a7;

    src_a10 = LD_DP(a + 10);
    src_a11 = (v2f64) __msa_splati_d((v2i64) src_a10, 1);
    src_a10 = (v2f64) __msa_splati_d((v2i64) src_a10, 0);
    src_a15 = __msa_cast_to_vector_double(*(a + 15));
    src_a15 = (v2f64) __msa_splati_d((v2i64) src_a15, 0);

    res_c2 *= src_a10;
    res_c3 -= res_c2 * src_a11;
    res_c3 *= src_a15;

    res_c6 *= src_a10;
    res_c7 -= res_c6 * src_a11;
    res_c7 *= src_a15;

    ST_DP(res_c0, b + 0);
    ST_DP(res_c4, b + 2);
    ST_DP(res_c1, b + 4);
    ST_DP(res_c5, b + 6);
    ST_DP(res_c2, b + 8);
    ST_DP(res_c6, b + 10);
    ST_DP(res_c3, b + 12);
    ST_DP(res_c7, b + 14);

    ILVRL_D2_DP(res_c1, res_c0, src_c0, src_c2);
    ILVRL_D2_DP(res_c3, res_c2, src_c1, src_c3);
    ILVRL_D2_DP(res_c5, res_c4, src_c4, src_c6);
    ILVRL_D2_DP(res_c7, res_c6, src_c5, src_c7);

    ST_DP2(src_c0, src_c1, c, 2);
    ST_DP2(src_c2, src_c3, c + ldc, 2);
    ST_DP2(src_c4, src_c5, c + 2 * ldc, 2);
    ST_DP2(src_c6, src_c7, c + 3 * ldc, 2);
}

static void dsolve_4x2_lt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    v2f64 src_c0, src_c1, src_c2, src_c3, res_c0, res_c1, res_c2, res_c3;
    v2f64 src_a0, src_a1, src_a2, src_a3, src_a5, src_a6, src_a7;
    v2f64 src_a10, src_a11, src_a15;

    LD_DP2(c, 2, src_c0, src_c1);
    LD_DP2(c + ldc, 2, src_c2, src_c3);

    if (bk)
    {
        BLASLONG i;
        v2f64 src_a0, src_a1, src_b, src_b0;

        for (i = bk; i--;)
        {
            LD_DP2(a, 2, src_a0, src_a1);
            src_b0 = LD_DP(b);

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
            src_c0 -= src_a0 * src_b;
            src_c1 -= src_a1 * src_b;

            src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
            src_c2 -= src_a0 * src_b;
            src_c3 -= src_a1 * src_b;

            a += 4;
            b += 2;
        }
    }

    ILVRL_D2_DP(src_c2, src_c0, res_c0, res_c1);
    ILVRL_D2_DP(src_c3, src_c1, res_c2, res_c3);

    src_a0 = LD_DP(a + 0);
    src_a1 = (v2f64) __msa_splati_d((v2i64) src_a0, 1);
    src_a0 = (v2f64) __msa_splati_d((v2i64) src_a0, 0);
    src_a2 = LD_DP(a + 2);
    src_a3 = (v2f64) __msa_splati_d((v2i64) src_a2, 1);
    src_a2 = (v2f64) __msa_splati_d((v2i64) src_a2, 0);

    res_c0 *= src_a0;
    res_c1 -= res_c0 * src_a1;
    res_c2 -= res_c0 * src_a2;
    res_c3 -= res_c0 * src_a3;

    src_a5 = __msa_cast_to_vector_double(*(a + 5));
    src_a5 = (v2f64) __msa_splati_d((v2i64) src_a5, 0);
    src_a6 = LD_DP(a + 6);
    src_a7 = (v2f64) __msa_splati_d((v2i64) src_a6, 1);
    src_a6 = (v2f64) __msa_splati_d((v2i64) src_a6, 0);

    res_c1 *= src_a5;
    res_c2 -= res_c1 * src_a6;
    res_c3 -= res_c1 * src_a7;

    src_a10 = LD_DP(a + 10);
    src_a11 = (v2f64) __msa_splati_d((v2i64) src_a10, 1);
    src_a10 = (v2f64) __msa_splati_d((v2i64) src_a10, 0);
    src_a15 = __msa_cast_to_vector_double(*(a + 15));
    src_a15 = (v2f64) __msa_splati_d((v2i64) src_a15, 0);

    res_c2 *= src_a10;
    res_c3 -= res_c2 * src_a11;
    res_c3 *= src_a15;

    ST_DP(res_c0, b + 0);
    ST_DP(res_c1, b + 2);
    ST_DP(res_c2, b + 4);
    ST_DP(res_c3, b + 6);

    ILVRL_D2_DP(res_c1, res_c0, src_c0, src_c2);
    ILVRL_D2_DP(res_c3, res_c2, src_c1, src_c3);

    ST_DP2(src_c0, src_c1, c, 2);
    ST_DP2(src_c2, src_c3, c + ldc, 2);
}

static void dsolve_4x1_lt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG bk)
{
    FLOAT a0, a1, a2, a3, a5, a6, a7, a10, a11, a15, c0, c1, c2, c3;

    c0 = *(c + 0);
    c1 = *(c + 1);
    c2 = *(c + 2);
    c3 = *(c + 3);

    if (bk)
    {
        BLASLONG i;

        for (i = bk; i--;)
        {
            c0 -= a[0] * b[0];
            c1 -= a[1] * b[0];
            c2 -= a[2] * b[0];
            c3 -= a[3] * b[0];

            a += 4;
            b += 1;
        }
    }

    a0 = *(a + 0);
    a1 = *(a + 1);
    a2 = *(a + 2);
    a3 = *(a + 3);
    a5 = *(a + 5);
    a6 = *(a + 6);
    a7 = *(a + 7);
    a10 = *(a + 10);
    a11 = *(a + 11);
    a15 = *(a + 15);

    c0 *= a0;

    c1 -= c0 * a1;
    c1 *= a5;

    c2 -= c0 * a2;
    c2 -= c1 * a6;
    c2 *= a10;

    c3 -= c0 * a3;
    c3 -= c1 * a7;
    c3 -= c2 * a11;
    c3 *= a15;

    *(b + 0) = c0;
    *(b + 1) = c1;
    *(b + 2) = c2;
    *(b + 3) = c3;

    *(c + 0) = c0;
    *(c + 1) = c1;
    *(c + 2) = c2;
    *(c + 3) = c3;
}

static void dsolve_2x4_lt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    FLOAT a0, a1, a3, c0, c1, c0_nxt1, c1_nxt1;
    FLOAT c0_nxt2, c1_nxt2, c0_nxt3, c1_nxt3;

    c0 = *(c + 0);
    c1 = *(c + 1);
    c0_nxt1 = *(c + ldc);
    c1_nxt1 = *(c + 1 + ldc);
    c0_nxt2 = *(c + 2 * ldc);
    c1_nxt2 = *(c + 1 + 2 * ldc);
    c0_nxt3 = *(c + 3 * ldc);
    c1_nxt3 = *(c + 1 + 3 * ldc);

    if (bk)
    {
        BLASLONG i;

        for (i = bk; i--;)
        {
            c0 -= a[0] * b[0];
            c1 -= a[1] * b[0];
            c0_nxt1 -= a[0] * b[1];
            c1_nxt1 -= a[1] * b[1];
            c0_nxt2 -= a[0] * b[2];
            c1_nxt2 -= a[1] * b[2];
            c0_nxt3 -= a[0] * b[3];
            c1_nxt3 -= a[1] * b[3];

            a += 2;
            b += 4;
        }
    }

    a0 = *a;
    a1 = *(a + 1);
    a3 = *(a + 3);

    c0 *= a0;
    c1 -= c0 * a1;
    c1 *= a3;

    c0_nxt1 *= a0;
    c1_nxt1 -= c0_nxt1 * a1;
    c1_nxt1 *= a3;

    c0_nxt2 *= a0;
    c1_nxt2 -= c0_nxt2 * a1;
    c1_nxt2 *= a3;

    c0_nxt3 *= a0;
    c1_nxt3 -= c0_nxt3 * a1;
    c1_nxt3 *= a3;

    *(b + 0) = c0;
    *(b + 1) = c0_nxt1;
    *(b + 2) = c0_nxt2;
    *(b + 3) = c0_nxt3;
    *(b + 4) = c1;
    *(b + 5) = c1_nxt1;
    *(b + 6) = c1_nxt2;
    *(b + 7) = c1_nxt3;

    *(c + 0) = c0;
    *(c + 1) = c1;
    *(c + 0 + ldc) = c0_nxt1;
    *(c + 1 + ldc) = c1_nxt1;
    *(c + 0 + 2 * ldc) = c0_nxt2;
    *(c + 1 + 2 * ldc) = c1_nxt2;
    *(c + 0 + 3 * ldc) = c0_nxt3;
    *(c + 1 + 3 * ldc) = c1_nxt3;
}

static void dsolve_2x2_lt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    FLOAT a0, a1, a3, c0, c1, c0_nxt, c1_nxt;

    c0 = *(c + 0);
    c1 = *(c + 1);

    c0_nxt = *(c + ldc);
    c1_nxt = *(c + 1 + ldc);

    if (bk)
    {
        BLASLONG i;

        for (i = bk; i--;)
        {
            c0 -= a[0] * b[0];
            c1 -= a[1] * b[0];

            c0_nxt -= a[0] * b[1];
            c1_nxt -= a[1] * b[1];

            a += 2;
            b += 2;
        }
    }

    a0 = *a;
    a1 = *(a + 1);
    a3 = *(a + 3);

    c0 *= a0;
    c1 -= c0 * a1;
    c1 *= a3;

    c0_nxt *= a0;
    c1_nxt -= c0_nxt * a1;
    c1_nxt *= a3;

    *(b + 0) = c0;
    *(b + 1) = c0_nxt;
    *(b + 2) = c1;
    *(b + 3) = c1_nxt;

    *(c + 0) = c0;
    *(c + 1) = c1;

    *(c + 0 + ldc) = c0_nxt;
    *(c + 1 + ldc) = c1_nxt;
}

static void dsolve_2x1_lt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG bk)
{
    FLOAT a0, a1, a3, c0, c1;

    c0 = *(c + 0);
    c1 = *(c + 1);

    if (bk)
    {
        BLASLONG i;

        for (i = bk; i--;)
        {
            c0 -= a[0] * b[0];
            c1 -= a[1] * b[0];

            a += 2;
            b += 1;
        }
    }

    a0 = *(a + 0);
    a1 = *(a + 1);
    a3 = *(a + 3);

    c0 *= a0;
    c1 -= c0 * a1;
    c1 *= a3;

    *(b + 0) = c0;
    *(b + 1) = c1;

    *(c + 0) = c0;
    *(c + 1) = c1;
}

static void dsolve_1x4_lt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    FLOAT c0, c1, c2, c3;

    c0 = *(c + 0);
    c1 = *(c + 1 * ldc);
    c2 = *(c + 2 * ldc);
    c3 = *(c + 3 * ldc);

    if (bk)
    {
        BLASLONG i;

        for (i = bk; i--;)
        {
            c0 -= a[0] * b[0];
            c1 -= a[0] * b[1];
            c2 -= a[0] * b[2];
            c3 -= a[0] * b[3];

            a += 1;
            b += 4;
        }
    }

    c0 *= *a;
    c1 *= *a;
    c2 *= *a;
    c3 *= *a;

    *(c + 0 * ldc) = c0;
    *(c + 1 * ldc) = c1;
    *(c + 2 * ldc) = c2;
    *(c + 3 * ldc) = c3;

    *(b + 0) = c0;
    *(b + 1) = c1;
    *(b + 2) = c2;
    *(b + 3) = c3;
}

static void dsolve_1x2_lt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    FLOAT c0, c1;

    c0 = *c;
    c1 = *(c + ldc);

    if (bk)
    {
        BLASLONG i;

        for (i = bk; i--;)
        {
            c0 -= *a * b[0];
            c1 -= *a * b[1];

            a += 1;
            b += 2;
        }
    }

    c0 *= *a;
    c1 *= *a;

    *(b + 0) = c0;
    *(b + 1) = c1;

    *(c + 0) = c0;
    *(c + ldc) = c1;
}

static void dgmm_dsolve_1x1_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG bk)
{
    if (bk)
    {
        BLASLONG i;

        for (i = bk; i--;)
        {
            *c -= *a * *b;

            a += 1;
            b += 1;
        }
    }

    *c *= *a;
    *b = *c;
}

int CNAME(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT dummy1, FLOAT *a, FLOAT *b,
          FLOAT *c, BLASLONG ldc, BLASLONG offset)
{
    BLASLONG i, j, kk;
    FLOAT *aa, *cc;

    for (j = (n >> 2); j--;)
    {
        kk = offset;
        aa = a;
        cc = c;

        for (i = (m >> 3); i--;)
        {
            dsolve_8x4_lt_msa(aa, b, cc, ldc, kk);

            aa += 8 * k;
            cc += 8;
            kk += 8;
        }

        if (m & 7)
        {
            if (m & 4)
            {
                dsolve_4x4_lt_msa(aa, b, cc, ldc, kk);

                aa += 4 * k;
                cc += 4;
                kk += 4;
            }

            if (m & 2)
            {
                dsolve_2x4_lt_msa(aa, b, cc, ldc, kk);

                aa += 2 * k;
                cc += 2;
                kk += 2;
            }

            if (m & 1)
            {
                dsolve_1x4_lt_msa(aa, b, cc, ldc, kk);

                aa += k;
                cc += 1;
                kk += 1;
            }
        }

        b += 4 * k;
        c += 4 * ldc;
    }

    if (n & 3)
    {
        if (n & 2)
        {
            kk = offset;
            aa = a;
            cc = c;

            for (i = (m >> 3); i--;)
            {
                dsolve_8x2_lt_msa(aa, b, cc, ldc, kk);

                aa += 8 * k;
                cc += 8;
                kk += 8;
            }

            if (m & 7)
            {
                if (m & 4)
                {
                    dsolve_4x2_lt_msa(aa, b, cc, ldc, kk);

                    aa += 4 * k;
                    cc += 4;
                    kk += 4;
                }

                if (m & 2)
                {
                    dsolve_2x2_lt_msa(aa, b, cc, ldc, kk);

                    aa += 2 * k;
                    cc += 2;
                    kk += 2;
                }

                if (m & 1)
                {
                    dsolve_1x2_lt_msa(aa, b, cc, ldc, kk);

                    aa += k;
                    cc += 1;
                    kk += 1;
                }
            }

            b += 2 * k;
            c += 2 * ldc;
        }

        if (n & 1)
        {
            kk = offset;
            aa = a;
            cc = c;

            for (i = (m >> 3); i--;)
            {
                dsolve_8x1_lt_msa(aa, b, cc, kk);

                aa += 8 * k;
                cc += 8;
                kk += 8;
            }

            if (m & 7)
            {
                if (m & 4)
                {
                    dsolve_4x1_lt_msa(aa, b, cc, kk);

                    aa += 4 * k;
                    cc += 4;
                    kk += 4;
                }

                if (m & 2)
                {
                    dsolve_2x1_lt_msa(aa, b, cc, kk);

                    aa += 2 * k;
                    cc += 2;
                    kk += 2;
                }

                if (m & 1)
                {
                    dgmm_dsolve_1x1_msa(aa, b, cc, kk);

                    aa += k;
                    cc += 1;
                    kk += 1;
                }
            }

            b += k;
            c += ldc;
        }
    }

    return 0;
}
