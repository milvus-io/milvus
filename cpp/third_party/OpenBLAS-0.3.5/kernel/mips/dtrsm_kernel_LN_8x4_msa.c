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
void dsolve_8x4_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    v2f64 src_c0, src_c1, src_c2, src_c3, src_c4, src_c5, src_c6, src_c7;
    v2f64 res_c0, res_c1, res_c2, res_c3, res_c4, res_c5, res_c6, res_c7;
    v2f64 src_c8, src_c9, src_c10, src_c11, src_c12, src_c13, src_c14, src_c15;
    v2f64 res_c8, res_c9, res_c10, res_c11, res_c12, res_c13, res_c14, res_c15;
    v2f64 src_a0, src_a1, src_a2, src_a3, src_a8, src_a9, src_a16, src_a17;
    v2f64 src_a18, src_a24, src_a25, src_a26, src_a27, src_a32, src_a33;
    v2f64 src_a34, src_a35, src_a36, src_a40, src_a41, src_a42, src_a43;
    v2f64 src_a44, src_a45, src_a48, src_a49, src_a50, src_a51, src_a52;
    v2f64 src_a53, src_a54, src_a56, src_a57, src_a58, src_a59, src_a60;
    v2f64 src_a61, src_a62, src_a63;
    FLOAT *c_nxt1line = c + ldc;
    FLOAT *c_nxt2line = c + 2 * ldc;
    FLOAT *c_nxt3line = c + 3 * ldc;

    PREF_OFFSET(a,  -96);
    PREF_OFFSET(a,  -32);
    PREF_OFFSET(a, -160);
    PREF_OFFSET(a, -224);
    PREF_OFFSET(a,  -64);
    PREF_OFFSET(a, -128);
    PREF_OFFSET(a, -192);
    PREF_OFFSET(a, -256);
    PREF_OFFSET(a, -320);
    PREF_OFFSET(a, -384);
    PREF_OFFSET(a, -448);
    PREF_OFFSET(a, -512);

    LD_DP4(c, 2, src_c0, src_c1, src_c2, src_c3);
    LD_DP4(c_nxt1line, 2, src_c4, src_c5, src_c6, src_c7);
    LD_DP4(c_nxt2line, 2, src_c8, src_c9, src_c10, src_c11);
    LD_DP4(c_nxt3line, 2, src_c12, src_c13, src_c14, src_c15);

    if (bk > 0)
    {
        BLASLONG i, pref_offset;
        FLOAT *pba = a, *pbb = b, *pa0_pref;
        v2f64 src_b, src_b0, src_b1;

        pref_offset = (uintptr_t)a & (L1_DATA_LINESIZE - 1);

        if (pref_offset)
        {
            pref_offset = L1_DATA_LINESIZE - pref_offset;
            pref_offset = pref_offset / sizeof(FLOAT);
        }

        pa0_pref = a + pref_offset;

        for (i = bk >> 1; i--;)
        {
            PREF_OFFSET(pa0_pref, 128);
            PREF_OFFSET(pa0_pref, 160);
            PREF_OFFSET(pa0_pref, 192);
            PREF_OFFSET(pa0_pref, 224);

            LD_DP4_INC(pba, 2, src_a0, src_a1, src_a2, src_a3);
            LD_DP2_INC(pbb, 2, src_b0, src_b1);

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

            LD_DP4_INC(pba, 2, src_a0, src_a1, src_a2, src_a3);
            LD_DP2_INC(pbb, 2, src_b0, src_b1);

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
            LD_DP4_INC(pba, 2, src_a0, src_a1, src_a2, src_a3);
            LD_DP2_INC(pbb, 2, src_b0, src_b1);

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

    a -= 64;
    b -= 32;

    ILVRL_D2_DP(src_c4, src_c0, res_c0, res_c1);
    ILVRL_D2_DP(src_c5, src_c1, res_c2, res_c3);
    ILVRL_D2_DP(src_c6, src_c2, res_c4, res_c5);
    ILVRL_D2_DP(src_c7, src_c3, res_c6, res_c7);
    ILVRL_D2_DP(src_c12, src_c8, res_c8, res_c9);
    ILVRL_D2_DP(src_c13, src_c9, res_c10, res_c11);
    ILVRL_D2_DP(src_c14, src_c10, res_c12, res_c13);
    ILVRL_D2_DP(src_c15, src_c11, res_c14, res_c15);

    src_a54 = __msa_cast_to_vector_double(*(a + 54));
    src_a54 = (v2f64) __msa_splati_d((v2i64) src_a54, 0);
    src_a62 = LD_DP(a + 62);
    src_a63 = (v2f64) __msa_splati_d((v2i64) src_a62, 1);
    src_a62 = (v2f64) __msa_splati_d((v2i64) src_a62, 0);
    src_a60 = LD_DP(a + 60);
    src_a61 = (v2f64) __msa_splati_d((v2i64) src_a60, 1);
    src_a60 = (v2f64) __msa_splati_d((v2i64) src_a60, 0);
    src_a52 = LD_DP(a + 52);
    src_a53 = (v2f64) __msa_splati_d((v2i64) src_a52, 1);
    src_a52 = (v2f64) __msa_splati_d((v2i64) src_a52, 0);
    src_a44 = LD_DP(a + 44);
    src_a45 = (v2f64) __msa_splati_d((v2i64) src_a44, 1);
    src_a44 = (v2f64) __msa_splati_d((v2i64) src_a44, 0);
    src_a36 = __msa_cast_to_vector_double(*(a + 36));
    src_a36 = (v2f64) __msa_splati_d((v2i64) src_a36, 0);

    res_c7 *= src_a63;
    res_c6 -= res_c7 * src_a62;
    res_c6 *= src_a54;

    res_c15 *= src_a63;
    res_c14 -= res_c15 * src_a62;
    res_c14 *= src_a54;

    ST_DP(res_c7, b + 28);
    ST_DP(res_c6, b + 24);
    ST_DP(res_c15, b + 30);
    ST_DP(res_c14, b + 26);
    ILVRL_D2_DP(res_c7, res_c6, src_c3, src_c7);
    ILVRL_D2_DP(res_c15, res_c14, src_c11, src_c15);
    ST_DP(src_c3, c + 6);
    ST_DP(src_c7, c_nxt1line + 6);
    ST_DP(src_c11, c_nxt2line + 6);
    ST_DP(src_c15, c_nxt3line + 6);

    res_c5 -= res_c7 * src_a61;
    res_c5 -= res_c6 * src_a53;
    res_c5 *= src_a45;

    res_c4 -= res_c7 * src_a60;
    res_c4 -= res_c6 * src_a52;
    res_c4 -= res_c5 * src_a44;
    res_c4 *= src_a36;

    res_c13 -= res_c15 * src_a61;
    res_c13 -= res_c14 * src_a53;
    res_c13 *= src_a45;

    res_c12 -= res_c15 * src_a60;
    res_c12 -= res_c14 * src_a52;
    res_c12 -= res_c13 * src_a44;
    res_c12 *= src_a36;

    src_a56 = LD_DP(a + 56);
    src_a57 = (v2f64) __msa_splati_d((v2i64) src_a56, 1);
    src_a56 = (v2f64) __msa_splati_d((v2i64) src_a56, 0);
    src_a58 = LD_DP(a + 58);
    src_a59 = (v2f64) __msa_splati_d((v2i64) src_a58, 1);
    src_a58 = (v2f64) __msa_splati_d((v2i64) src_a58, 0);

    ST_DP(res_c4, b + 16);
    ST_DP(res_c5, b + 20);
    ST_DP(res_c12, b + 18);
    ST_DP(res_c13, b + 22);

    ILVRL_D2_DP(res_c5, res_c4, src_c2, src_c6);
    ILVRL_D2_DP(res_c13, res_c12, src_c10, src_c14);
    ST_DP(src_c2, c + 4);
    ST_DP(src_c6, c_nxt1line + 4);
    ST_DP(src_c10, c_nxt2line + 4);
    ST_DP(src_c14, c_nxt3line + 4);

    src_a50 = LD_DP(a + 50);
    src_a51 = (v2f64) __msa_splati_d((v2i64) src_a50, 1);
    src_a50 = (v2f64) __msa_splati_d((v2i64) src_a50, 0);
    src_a42 = LD_DP(a + 42);
    src_a43 = (v2f64) __msa_splati_d((v2i64) src_a42, 1);
    src_a42 = (v2f64) __msa_splati_d((v2i64) src_a42, 0);
    src_a34 = LD_DP(a + 34);
    src_a35 = (v2f64) __msa_splati_d((v2i64) src_a34, 1);
    src_a34 = (v2f64) __msa_splati_d((v2i64) src_a34, 0);
    src_a26 = LD_DP(a + 26);
    src_a27 = (v2f64) __msa_splati_d((v2i64) src_a26, 1);
    src_a26 = (v2f64) __msa_splati_d((v2i64) src_a26, 0);
    src_a18 = __msa_cast_to_vector_double(*(a + 18));
    src_a18 = (v2f64) __msa_splati_d((v2i64) src_a18, 0);

    res_c3 -= res_c7 * src_a59;
    res_c2 -= res_c7 * src_a58;
    res_c1 -= res_c7 * src_a57;
    res_c0 -= res_c7 * src_a56;

    res_c11 -= res_c15 * src_a59;
    res_c10 -= res_c15 * src_a58;
    res_c9 -= res_c15 * src_a57;
    res_c8 -= res_c15 * src_a56;

    res_c3 -= res_c6 * src_a51;
    res_c3 -= res_c5 * src_a43;
    res_c3 -= res_c4 * src_a35;
    res_c3 *= src_a27;

    res_c2 -= res_c6 * src_a50;
    res_c2 -= res_c5 * src_a42;
    res_c2 -= res_c4 * src_a34;
    res_c2 -= res_c3 * src_a26;
    res_c2 *= src_a18;

    res_c11 -= res_c14 * src_a51;
    res_c11 -= res_c13 * src_a43;
    res_c11 -= res_c12 * src_a35;
    res_c11 *= src_a27;

    res_c10 -= res_c14 * src_a50;
    res_c10 -= res_c13 * src_a42;
    res_c10 -= res_c12 * src_a34;
    res_c10 -= res_c11 * src_a26;
    res_c10 *= src_a18;

    src_a48 = LD_DP(a + 48);
    src_a49 = (v2f64) __msa_splati_d((v2i64) src_a48, 1);
    src_a48 = (v2f64) __msa_splati_d((v2i64) src_a48, 0);
    src_a40 = LD_DP(a + 40);
    src_a41 = (v2f64) __msa_splati_d((v2i64) src_a40, 1);
    src_a40 = (v2f64) __msa_splati_d((v2i64) src_a40, 0);

    ST_DP(res_c2, b + 8);
    ST_DP(res_c3, b + 12);
    ST_DP(res_c10, b + 10);
    ST_DP(res_c11, b + 14);

    src_a32 = LD_DP(a + 32);
    src_a33 = (v2f64) __msa_splati_d((v2i64) src_a32, 1);
    src_a32 = (v2f64) __msa_splati_d((v2i64) src_a32, 0);
    src_a24 = LD_DP(a + 24);
    src_a25 = (v2f64) __msa_splati_d((v2i64) src_a24, 1);
    src_a24 = (v2f64) __msa_splati_d((v2i64) src_a24, 0);

    ILVRL_D2_DP(res_c3, res_c2, src_c1, src_c5);
    ILVRL_D2_DP(res_c11, res_c10, src_c9, src_c13);
    ST_DP(src_c1, c + 2);
    ST_DP(src_c5, c_nxt1line + 2);
    ST_DP(src_c9, c_nxt2line + 2);
    ST_DP(src_c13, c_nxt3line + 2);

    res_c1 -= res_c6 * src_a49;
    res_c1 -= res_c5 * src_a41;
    res_c1 -= res_c4 * src_a33;
    res_c1 -= res_c3 * src_a25;

    res_c0 -= res_c6 * src_a48;
    res_c0 -= res_c5 * src_a40;
    res_c0 -= res_c4 * src_a32;
    res_c0 -= res_c3 * src_a24;

    res_c9 -= res_c14 * src_a49;
    res_c9 -= res_c13 * src_a41;
    res_c9 -= res_c12 * src_a33;
    res_c9 -= res_c11 * src_a25;

    res_c8 -= res_c14 * src_a48;
    res_c8 -= res_c13 * src_a40;
    res_c8 -= res_c12 * src_a32;
    res_c8 -= res_c11 * src_a24;

    src_a16 = LD_DP(a + 16);
    src_a17 = (v2f64) __msa_splati_d((v2i64) src_a16, 1);
    src_a16 = (v2f64) __msa_splati_d((v2i64) src_a16, 0);
    src_a8 = LD_DP(a + 8);
    src_a9 = (v2f64) __msa_splati_d((v2i64) src_a8, 1);
    src_a8 = (v2f64) __msa_splati_d((v2i64) src_a8, 0);
    src_a0 = __msa_cast_to_vector_double(*(a + 0));
    src_a0 = (v2f64) __msa_splati_d((v2i64) src_a0, 0);

    res_c1 -= res_c2 * src_a17;
    res_c1 *= src_a9;

    res_c9 -= res_c10 * src_a17;
    res_c9 *= src_a9;

    res_c0 -= res_c2 * src_a16;
    res_c0 -= res_c1 * src_a8;
    res_c0 *= src_a0;

    res_c8 -= res_c10 * src_a16;
    res_c8 -= res_c9 * src_a8;
    res_c8 *= src_a0;

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
}

static void dsolve_8x2_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    v2f64 src_c0, src_c1, src_c2, src_c3, src_c4, src_c5, src_c6, src_c7;
    v2f64 res_c0, res_c1, res_c2, res_c3, res_c4, res_c5, res_c6, res_c7;
    v2f64 src_a0, src_a1, src_a2, src_a3, src_a8, src_a9, src_a16, src_a17;
    v2f64 src_a18, src_a24, src_a25, src_a26, src_a27, src_a32, src_a33;
    v2f64 src_a34, src_a35, src_a36, src_a40, src_a41, src_a42, src_a43;
    v2f64 src_a44, src_a45, src_a48, src_a49, src_a50, src_a51, src_a52;
    v2f64 src_a53, src_a54, src_a56, src_a57, src_a58, src_a59, src_a60;
    v2f64 src_a61, src_a62, src_a63;

    LD_DP4(c, 2, src_c0, src_c1, src_c2, src_c3);
    LD_DP4(c + ldc, 2, src_c4, src_c5, src_c6, src_c7);

    if (bk > 0)
    {
        BLASLONG i;
        FLOAT *pba = a, *pbb = b;
        v2f64 src_b, src_b0, src_b1;

        LD_DP4(pba, 2, src_a0, src_a1, src_a2, src_a3);
        src_b0 = LD_DP(pbb);

        for (i = bk - 1; i--;)
        {
            pba += 8;
            pbb += 2;

            LD_DP4(pba, 2, src_a8, src_a9, src_a16, src_a17);
            src_b1 = LD_DP(pbb);

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

            src_a0 = src_a8;
            src_a1 = src_a9;
            src_a2 = src_a16;
            src_a3 = src_a17;
            src_b0 = src_b1;
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

    src_a56 = LD_DP(a - 8);
    src_a57 = (v2f64) __msa_splati_d((v2i64) src_a56, 1);
    src_a56 = (v2f64) __msa_splati_d((v2i64) src_a56, 0);
    src_a58 = LD_DP(a - 6);
    src_a59 = (v2f64) __msa_splati_d((v2i64) src_a58, 1);
    src_a58 = (v2f64) __msa_splati_d((v2i64) src_a58, 0);
    src_a60 = LD_DP(a - 4);
    src_a61 = (v2f64) __msa_splati_d((v2i64) src_a60, 1);
    src_a60 = (v2f64) __msa_splati_d((v2i64) src_a60, 0);
    src_a62 = LD_DP(a - 2);
    src_a63 = (v2f64) __msa_splati_d((v2i64) src_a62, 1);
    src_a62 = (v2f64) __msa_splati_d((v2i64) src_a62, 0);

    res_c7 *= src_a63;
    res_c6 -= res_c7 * src_a62;
    res_c5 -= res_c7 * src_a61;
    res_c4 -= res_c7 * src_a60;
    res_c3 -= res_c7 * src_a59;
    res_c2 -= res_c7 * src_a58;
    res_c1 -= res_c7 * src_a57;
    res_c0 -= res_c7 * src_a56;

    src_a48 = LD_DP(a - 16);
    src_a49 = (v2f64) __msa_splati_d((v2i64) src_a48, 1);
    src_a48 = (v2f64) __msa_splati_d((v2i64) src_a48, 0);
    src_a50 = LD_DP(a - 14);
    src_a51 = (v2f64) __msa_splati_d((v2i64) src_a50, 1);
    src_a50 = (v2f64) __msa_splati_d((v2i64) src_a50, 0);
    src_a52 = LD_DP(a - 12);
    src_a53 = (v2f64) __msa_splati_d((v2i64) src_a52, 1);
    src_a52 = (v2f64) __msa_splati_d((v2i64) src_a52, 0);
    src_a54 = __msa_cast_to_vector_double(*(a - 10));
    src_a54 = (v2f64) __msa_splati_d((v2i64) src_a54, 0);

    src_a40 = LD_DP(a - 24);
    src_a41 = (v2f64) __msa_splati_d((v2i64) src_a40, 1);
    src_a40 = (v2f64) __msa_splati_d((v2i64) src_a40, 0);
    src_a42 = LD_DP(a - 22);
    src_a43 = (v2f64) __msa_splati_d((v2i64) src_a42, 1);
    src_a42 = (v2f64) __msa_splati_d((v2i64) src_a42, 0);
    src_a44 = LD_DP(a - 20);
    src_a45 = (v2f64) __msa_splati_d((v2i64) src_a44, 1);
    src_a44 = (v2f64) __msa_splati_d((v2i64) src_a44, 0);

    res_c6 *= src_a54;
    res_c5 -= res_c6 * src_a53;
    res_c4 -= res_c6 * src_a52;
    res_c3 -= res_c6 * src_a51;
    res_c2 -= res_c6 * src_a50;
    res_c1 -= res_c6 * src_a49;
    res_c0 -= res_c6 * src_a48;

    res_c5 *= src_a45;
    res_c4 -= res_c5 * src_a44;
    res_c3 -= res_c5 * src_a43;
    res_c2 -= res_c5 * src_a42;
    res_c1 -= res_c5 * src_a41;
    res_c0 -= res_c5 * src_a40;

    ST_DP(res_c7, b - 2);
    ST_DP(res_c6, b - 4);
    ST_DP(res_c5, b - 6);

    src_a32 = LD_DP(a - 32);
    src_a33 = (v2f64) __msa_splati_d((v2i64) src_a32, 1);
    src_a32 = (v2f64) __msa_splati_d((v2i64) src_a32, 0);
    src_a34 = LD_DP(a - 30);
    src_a35 = (v2f64) __msa_splati_d((v2i64) src_a34, 1);
    src_a34 = (v2f64) __msa_splati_d((v2i64) src_a34, 0);
    src_a36 = __msa_cast_to_vector_double(*(a - 28));
    src_a36 = (v2f64) __msa_splati_d((v2i64) src_a36, 0);

    res_c4 *= src_a36;
    res_c3 -= res_c4 * src_a35;
    res_c2 -= res_c4 * src_a34;
    res_c1 -= res_c4 * src_a33;
    res_c0 -= res_c4 * src_a32;

    src_a24 = LD_DP(a - 40);
    src_a25 = (v2f64) __msa_splati_d((v2i64) src_a24, 1);
    src_a24 = (v2f64) __msa_splati_d((v2i64) src_a24, 0);
    src_a26 = LD_DP(a - 38);
    src_a27 = (v2f64) __msa_splati_d((v2i64) src_a26, 1);
    src_a26 = (v2f64) __msa_splati_d((v2i64) src_a26, 0);
    src_a16 = LD_DP(a - 48);
    src_a17 = (v2f64) __msa_splati_d((v2i64) src_a16, 1);
    src_a16 = (v2f64) __msa_splati_d((v2i64) src_a16, 0);
    src_a18 = __msa_cast_to_vector_double(*(a - 46));
    src_a18 = (v2f64) __msa_splati_d((v2i64) src_a18, 0);
    src_a0 = __msa_cast_to_vector_double(*(a - 64));
    src_a0 = (v2f64) __msa_splati_d((v2i64) src_a0, 0);
    src_a8 = LD_DP(a - 56);
    src_a9 = (v2f64) __msa_splati_d((v2i64) src_a8, 1);
    src_a8 = (v2f64) __msa_splati_d((v2i64) src_a8, 0);

    res_c3 *= src_a27;
    res_c2 -= res_c3 * src_a26;
    res_c1 -= res_c3 * src_a25;
    res_c0 -= res_c3 * src_a24;

    res_c2 *= src_a18;
    res_c1 -= res_c2 * src_a17;
    res_c0 -= res_c2 * src_a16;

    res_c1 *= src_a9;
    res_c0 -= res_c1 * src_a8;

    res_c0 *= src_a0;

    ST_DP(res_c4, b - 8);
    ST_DP(res_c3, b - 10);
    ST_DP(res_c2, b - 12);
    ST_DP(res_c1, b - 14);
    ST_DP(res_c0, b - 16);

    ILVRL_D2_DP(res_c1, res_c0, src_c0, src_c4);
    ILVRL_D2_DP(res_c3, res_c2, src_c1, src_c5);
    ILVRL_D2_DP(res_c5, res_c4, src_c2, src_c6);
    ILVRL_D2_DP(res_c7, res_c6, src_c3, src_c7);

    ST_DP4(src_c0, src_c1, src_c2, src_c3, c, 2);
    ST_DP4(src_c4, src_c5, src_c6, src_c7, c + ldc, 2);
}

static void dsolve_8x1_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG bk)
{
    FLOAT a0, a8, a9, a16, a17, a18, a24, a25, a26, a27, a32, a33, a34, a35;
    FLOAT a36, a40, a41, a42, a43, a44, a45, a48, a49, a50, a51, a52, a53;
    FLOAT a54, a56, a57, a58, a59, a60, a61, a62, a63;
    FLOAT c0, c1, c2, c3, c4, c5, c6, c7;

    c0 = *(c + 0);
    c1 = *(c + 1);
    c2 = *(c + 2);
    c3 = *(c + 3);
    c4 = *(c + 4);
    c5 = *(c + 5);
    c6 = *(c + 6);
    c7 = *(c + 7);

    if (bk > 0)
    {
        BLASLONG i;
        FLOAT *aa = a, *bb = b;

        for (i = bk; i--; )
        {
            c0 -= aa[0] * bb[0];
            c1 -= aa[1] * bb[0];
            c2 -= aa[2] * bb[0];
            c3 -= aa[3] * bb[0];
            c4 -= aa[4] * bb[0];
            c5 -= aa[5] * bb[0];
            c6 -= aa[6] * bb[0];
            c7 -= aa[7] * bb[0];

            aa += 8;
            bb += 1;
        }
    }

    a -= 64;
    b -= 8;

    a0 = *(a + 0);
    a8 = *(a + 8);
    a9 = *(a + 9);
    a16 = *(a + 16);
    a17 = *(a + 17);
    a18 = *(a + 18);
    a24 = *(a + 24);
    a25 = *(a + 25);
    a26 = *(a + 26);
    a27 = *(a + 27);
    a32 = *(a + 32);
    a33 = *(a + 33);
    a34 = *(a + 34);
    a35 = *(a + 35);
    a36 = *(a + 36);
    a40 = *(a + 40);
    a41 = *(a + 41);
    a42 = *(a + 42);
    a43 = *(a + 43);
    a44 = *(a + 44);
    a45 = *(a + 45);
    a48 = *(a + 48);
    a49 = *(a + 49);
    a50 = *(a + 50);
    a51 = *(a + 51);
    a52 = *(a + 52);
    a53 = *(a + 53);
    a54 = *(a + 54);
    a56 = *(a + 56);
    a57 = *(a + 57);
    a58 = *(a + 58);
    a59 = *(a + 59);
    a60 = *(a + 60);
    a61 = *(a + 61);
    a62 = *(a + 62);
    a63 = *(a + 63);

    c7 *= a63;

    c6 -= c7 * a62;
    c6 *= a54;

    c5 -= c7 * a61;
    c5 -= c6 * a53;
    c5 *= a45;

    c4 -= c7 * a60;
    c4 -= c6 * a52;
    c4 -= c5 * a44;
    c4 *= a36;

    c3 -= c7 * a59;
    c3 -= c6 * a51;
    c3 -= c5 * a43;
    c3 -= c4 * a35;
    c3 *= a27;

    c2 -= c7 * a58;
    c2 -= c6 * a50;
    c2 -= c5 * a42;
    c2 -= c4 * a34;
    c2 -= c3 * a26;
    c2 *= a18;

    c1 -= c7 * a57;
    c1 -= c6 * a49;
    c1 -= c5 * a41;
    c1 -= c4 * a33;
    c1 -= c3 * a25;
    c1 -= c2 * a17;
    c1 *= a9;

    c0 -= c7 * a56;
    c0 -= c6 * a48;
    c0 -= c5 * a40;
    c0 -= c4 * a32;
    c0 -= c3 * a24;
    c0 -= c2 * a16;
    c0 -= c1 * a8;
    c0 *= a0;

    *(b + 7) = c7;
    *(b + 6) = c6;
    *(b + 5) = c5;
    *(b + 4) = c4;
    *(b + 3) = c3;
    *(b + 2) = c2;
    *(b + 1) = c1;
    *(b + 0) = c0;

    *(c + 7) = c7;
    *(c + 6) = c6;
    *(c + 5) = c5;
    *(c + 4) = c4;
    *(c + 3) = c3;
    *(c + 2) = c2;
    *(c + 1) = c1;
    *(c + 0) = c0;
}

static void dsolve_4x4_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    v2f64 src_c0, src_c1, src_c2, src_c3, src_c4, src_c5, src_c6, src_c7;
    v2f64 res_c0, res_c1, res_c2, res_c3, res_c4, res_c5, res_c6, res_c7;
    v2f64 src_a0, src_a4, src_a5, src_a8, src_a9, src_a10, src_a12, src_a13;
    v2f64 src_a14, src_a15;

    LD_DP2(c, 2, src_c0, src_c1);
    LD_DP2(c + ldc, 2, src_c2, src_c3);
    LD_DP2(c + 2 * ldc, 2, src_c4, src_c5);
    LD_DP2(c + 3 * ldc, 2, src_c6, src_c7);

    if (bk > 0)
    {
        BLASLONG i;
        FLOAT *aa = a, *bb = b;
        v2f64 src_a0, src_a1, src_b, src_b0, src_b1;

        for (i = bk; i--;)
        {
            LD_DP2(aa, 2, src_a0, src_a1);
            LD_DP2(bb, 2, src_b0, src_b1);

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

            aa += 4;
            bb += 4;
        }
    }

    a -= 16;
    b -= 16;

    ILVRL_D2_DP(src_c2, src_c0, res_c0, res_c1);
    ILVRL_D2_DP(src_c3, src_c1, res_c2, res_c3);
    ILVRL_D2_DP(src_c6, src_c4, res_c4, res_c5);
    ILVRL_D2_DP(src_c7, src_c5, res_c6, res_c7);

    src_a14 = LD_DP(a + 14);
    src_a15 = (v2f64) __msa_splati_d((v2i64) src_a14, 1);
    src_a14 = (v2f64) __msa_splati_d((v2i64) src_a14, 0);

    src_a12 = LD_DP(a + 12);
    src_a13 = (v2f64) __msa_splati_d((v2i64) src_a12, 1);
    src_a12 = (v2f64) __msa_splati_d((v2i64) src_a12, 0);

    src_a9 = LD_DP(a + 9);
    src_a10 = (v2f64) __msa_splati_d((v2i64) src_a9, 1);
    src_a9 = (v2f64) __msa_splati_d((v2i64) src_a9, 0);

    src_a8 = __msa_cast_to_vector_double(*(a + 8));
    src_a0 = __msa_cast_to_vector_double(*(a + 0));

    src_a8 = (v2f64) __msa_splati_d((v2i64) src_a8, 0);
    src_a0 = (v2f64) __msa_splati_d((v2i64) src_a0, 0);

    src_a4 = LD_DP(a + 4);
    src_a5 = (v2f64) __msa_splati_d((v2i64) src_a4, 1);
    src_a4 = (v2f64) __msa_splati_d((v2i64) src_a4, 0);

    res_c3 *= src_a15;
    res_c7 *= src_a15;

    res_c2 -= res_c3 * src_a14;
    res_c6 -= res_c7 * src_a14;
    res_c2 *= src_a10;
    res_c6 *= src_a10;

    res_c1 -= res_c3 * src_a13;
    res_c5 -= res_c7 * src_a13;
    res_c1 -= res_c2 * src_a9;
    res_c5 -= res_c6 * src_a9;
    res_c1 *= src_a5;
    res_c5 *= src_a5;

    res_c0 -= res_c3 * src_a12;
    res_c4 -= res_c7 * src_a12;
    res_c0 -= res_c2 * src_a8;
    res_c4 -= res_c6 * src_a8;
    res_c0 -= res_c1 * src_a4;
    res_c4 -= res_c5 * src_a4;
    res_c0 *= src_a0;
    res_c4 *= src_a0;

    ST_DP(res_c7, b + 14);
    ST_DP(res_c3, b + 12);
    ST_DP(res_c6, b + 10);
    ST_DP(res_c2, b + 8);
    ST_DP(res_c5, b + 6);
    ST_DP(res_c1, b + 4);
    ST_DP(res_c4, b + 2);
    ST_DP(res_c0, b + 0);

    ILVRL_D2_DP(res_c1, res_c0, src_c0, src_c2);
    ILVRL_D2_DP(res_c3, res_c2, src_c1, src_c3);
    ILVRL_D2_DP(res_c5, res_c4, src_c4, src_c6);
    ILVRL_D2_DP(res_c7, res_c6, src_c5, src_c7);

    ST_DP2(src_c0, src_c1, c, 2);
    ST_DP2(src_c2, src_c3, c + ldc, 2);
    ST_DP2(src_c4, src_c5, c + 2 * ldc, 2);
    ST_DP2(src_c6, src_c7, c + 3 * ldc, 2);
}

static void dsolve_4x2_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    v2f64 src_c0, src_c1, src_c2, src_c3, res_c0, res_c1, res_c2, res_c3;
    v2f64 src_a0, src_a4, src_a5, src_a8, src_a9, src_a10, src_a12, src_a13;
    v2f64 src_a14, src_a15;

    LD_DP2(c, 2, src_c0, src_c1);
    LD_DP2(c + ldc, 2, src_c2, src_c3);

    if (bk > 0)
    {
        BLASLONG i;
        FLOAT *aa = a, *bb = b;
        v2f64 src_a0, src_a1, src_b, src_b0;

        for (i = bk; i--;)
        {
            LD_DP2(aa, 2, src_a0, src_a1);
            src_b0 = LD_DP(bb);

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b0, (v2i64) src_b0);
            src_c0 -= src_a0 * src_b;
            src_c1 -= src_a1 * src_b;

            src_b = (v2f64) __msa_ilvl_d((v2i64) src_b0, (v2i64) src_b0);
            src_c2 -= src_a0 * src_b;
            src_c3 -= src_a1 * src_b;

            aa += 4;
            bb += 2;
        }
    }

    a -= 16;
    b -= 8;

    ILVRL_D2_DP(src_c2, src_c0, res_c0, res_c1);
    ILVRL_D2_DP(src_c3, src_c1, res_c2, res_c3);

    src_a14 = LD_DP(a + 14);
    src_a15 = (v2f64) __msa_splati_d((v2i64) src_a14, 1);
    src_a14 = (v2f64) __msa_splati_d((v2i64) src_a14, 0);

    src_a12 = LD_DP(a + 12);
    src_a13 = (v2f64) __msa_splati_d((v2i64) src_a12, 1);
    src_a12 = (v2f64) __msa_splati_d((v2i64) src_a12, 0);

    src_a9 = LD_DP(a + 9);
    src_a10 = (v2f64) __msa_splati_d((v2i64) src_a9, 1);
    src_a9 = (v2f64) __msa_splati_d((v2i64) src_a9, 0);

    src_a8 = __msa_cast_to_vector_double(*(a + 8));
    src_a0 = __msa_cast_to_vector_double(*(a + 0));

    src_a8 = (v2f64) __msa_splati_d((v2i64) src_a8, 0);
    src_a0 = (v2f64) __msa_splati_d((v2i64) src_a0, 0);

    src_a4 = LD_DP(a + 4);
    src_a5 = (v2f64) __msa_splati_d((v2i64) src_a4, 1);
    src_a4 = (v2f64) __msa_splati_d((v2i64) src_a4, 0);

    res_c3 *= src_a15;

    res_c2 -= res_c3 * src_a14;
    res_c2 *= src_a10;

    res_c1 -= res_c3 * src_a13;
    res_c1 -= res_c2 * src_a9;
    res_c1 *= src_a5;

    res_c0 -= res_c3 * src_a12;
    res_c0 -= res_c2 * src_a8;
    res_c0 -= res_c1 * src_a4;
    res_c0 *= src_a0;

    ST_DP(res_c3, b + 6);
    ST_DP(res_c2, b + 4);
    ST_DP(res_c1, b + 2);
    ST_DP(res_c0, b + 0);

    ILVRL_D2_DP(res_c1, res_c0, src_c0, src_c2);
    ILVRL_D2_DP(res_c3, res_c2, src_c1, src_c3);

    ST_DP2(src_c0, src_c1, c, 2);
    ST_DP2(src_c2, src_c3, c + ldc, 2);
}

static void dsolve_4x1_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG bk)
{
    FLOAT a0, a4, a5, a8, a9, a10, a12, a13, a14, a15, c0, c1, c2, c3;

    c0 = *(c + 0);
    c1 = *(c + 1);
    c2 = *(c + 2);
    c3 = *(c + 3);

    if (bk > 0)
    {
        BLASLONG i;
        FLOAT *aa = a, *bb = b;

        for (i = bk; i--;)
        {
            c0 -= aa[0] * bb[0];
            c1 -= aa[1] * bb[0];
            c2 -= aa[2] * bb[0];
            c3 -= aa[3] * bb[0];

            aa += 4;
            bb += 1;
        }
    }

    a -= 16;
    b -= 4;

    a0 = *(a + 0);
    a4 = *(a + 4);
    a5 = *(a + 5);
    a8 = *(a + 8);
    a9 = *(a + 9);
    a10 = *(a + 10);
    a12 = *(a + 12);
    a13 = *(a + 13);
    a14 = *(a + 14);
    a15 = *(a + 15);

    c3 *= a15;

    c2 -= c3 * a14;
    c2 *= a10;

    c1 -= c3 * a13;
    c1 -= c2 * a9;
    c1 *= a5;

    c0 -= c3 * a12;
    c0 -= c2 * a8;
    c0 -= c1 * a4;
    c0 *= a0;

    *(b + 0) = c0;
    *(b + 1) = c1;
    *(b + 2) = c2;
    *(b + 3) = c3;

    *(c + 0) = c0;
    *(c + 1) = c1;
    *(c + 2) = c2;
    *(c + 3) = c3;
}

static void dsolve_2x4_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    FLOAT a0, a2, a3, c0, c1, c0_nxt1, c1_nxt1;
    FLOAT c0_nxt2, c1_nxt2, c0_nxt3, c1_nxt3;

    c0 = *(c + 0);
    c1 = *(c + 1);
    c0_nxt1 = *(c + 0 + ldc);
    c1_nxt1 = *(c + 1 + ldc);
    c0_nxt2 = *(c + 0 + 2 * ldc);
    c1_nxt2 = *(c + 1 + 2 * ldc);
    c0_nxt3 = *(c + 0 + 3 * ldc);
    c1_nxt3 = *(c + 1 + 3 * ldc);

    if (bk > 0)
    {
        BLASLONG i;
        FLOAT *aa = a, *bb = b;

        for (i = bk; i--;)
        {
            c0 -= aa[0] * bb[0];
            c1 -= aa[1] * bb[0];
            c0_nxt1 -= aa[0] * bb[1];
            c1_nxt1 -= aa[1] * bb[1];
            c0_nxt2 -= aa[0] * bb[2];
            c1_nxt2 -= aa[1] * bb[2];
            c0_nxt3 -= aa[0] * bb[3];
            c1_nxt3 -= aa[1] * bb[3];

            aa += 2;
            bb += 4;
        }
    }

    a -= 4;
    b -= 8;

    a0 = *(a + 0);
    a2 = *(a + 2);
    a3 = *(a + 3);

    c1 *= a3;
    c0 -= c1 * a2;
    c0 *= a0;

    c1_nxt1 *= a3;
    c0_nxt1 -= c1_nxt1 * a2;
    c0_nxt1 *= a0;

    c1_nxt2 *= a3;
    c0_nxt2 -= c1_nxt2 * a2;
    c0_nxt2 *= a0;

    c1_nxt3 *= a3;
    c0_nxt3 -= c1_nxt3 * a2;
    c0_nxt3 *= a0;

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

static void dsolve_2x2_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    FLOAT a0, a2, a3, c0, c1, c0_nxt, c1_nxt;

    c0 = *(c + 0);
    c1 = *(c + 1);

    c0_nxt = *(c + 0 + ldc);
    c1_nxt = *(c + 1 + ldc);

    if (bk > 0)
    {
        BLASLONG i;
        FLOAT *aa = a, *bb = b;

        for (i = bk; i--;)
        {
            c0 -= aa[0] * bb[0];
            c1 -= aa[1] * bb[0];

            c0_nxt -= aa[0] * bb[1];
            c1_nxt -= aa[1] * bb[1];

            aa += 2;
            bb += 2;
        }
    }

    a -= 4;
    b -= 4;

    a0 = *(a + 0);
    a2 = *(a + 2);
    a3 = *(a + 3);

    c1 *= a3;

    c0 -= c1 * a2;
    c0 *= a0;

    c1_nxt *= a3;

    c0_nxt -= c1_nxt * a2;
    c0_nxt *= a0;

    *(b + 0) = c0;
    *(b + 1) = c0_nxt;
    *(b + 2) = c1;
    *(b + 3) = c1_nxt;

    *(c + 0) = c0;
    *(c + 1) = c1;

    *(c + 0 + ldc) = c0_nxt;
    *(c + 1 + ldc) = c1_nxt;
}

static void dsolve_2x1_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG bk)
{
    FLOAT a0, a2, a3, c0, c1;

    c0 = *(c + 0);
    c1 = *(c + 1);

    if (bk > 0)
    {
        BLASLONG i;
        FLOAT *aa = a, *bb = b;

        for (i = bk; i--;)
        {
            c0 -= aa[0] * bb[0];
            c1 -= aa[1] * bb[0];

            aa += 2;
            bb += 1;
        }
    }

    a0 = *(a - 4);
    a2 = *(a - 2);
    a3 = *(a - 1);

    c1 *= a3;
    c0 -= c1 * a2;
    c0 *= a0;

    *(b - 2) = c0;
    *(b - 1) = c1;

    *(c + 0) = c0;
    *(c + 1) = c1;
}

static void dsolve_1x4_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    FLOAT c0, c1, c2, c3;

    c0 = *(c + 0);
    c1 = *(c + 1 * ldc);
    c2 = *(c + 2 * ldc);
    c3 = *(c + 3 * ldc);

    if (bk > 0)
    {
        BLASLONG i;
        FLOAT *aa = a, *bb = b;

        for (i = bk; i--;)
        {
            c0 -= aa[0] * bb[0];
            c1 -= aa[0] * bb[1];
            c2 -= aa[0] * bb[2];
            c3 -= aa[0] * bb[3];

            aa += 1;
            bb += 4;
        }
    }

    c0 *= *(a - 1);
    c1 *= *(a - 1);
    c2 *= *(a - 1);
    c3 *= *(a - 1);

    *(c + 0 * ldc) = c0;
    *(c + 1 * ldc) = c1;
    *(c + 2 * ldc) = c2;
    *(c + 3 * ldc) = c3;

    *(b - 4) = c0;
    *(b - 3) = c1;
    *(b - 2) = c2;
    *(b - 1) = c3;
}

static void dsolve_1x2_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc)
{
    *c *= *a;
    *(c + ldc) = *a * *(c + ldc);

    *b = *c;
    *(b + 1) = *(c + ldc);
}

int CNAME(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT dummy1, FLOAT *a, FLOAT *b,
          FLOAT *c, BLASLONG ldc, BLASLONG offset)
{
    BLASLONG kk, i, j;
    FLOAT *aa, *bb, *cc;

    for (j = (n >> 2); j--;)
    {
        kk = m + offset;

        if (m & 7)
        {
            if (m & 1)
            {
                aa = a + (m - 1) * k + kk;
                bb = b + 4 * kk;
                cc = c + (m - 1);

                dsolve_1x4_ln_msa(aa, bb, cc, ldc, (k - kk));

                kk -= 1;
            }

            if (m & 2)
            {
                aa = a + ((m & -2) - 2) * k + 2 * kk;
                bb = b + 4 * kk;
                cc = c + ((m & -2) - 2);

                dsolve_2x4_ln_msa(aa, bb, cc, ldc, (k - kk));

                kk -= 2;
            }

            if (m & 4)
            {
                aa = a + ((m & -4) - 4) * k + 4 * kk;
                bb = b + 4 * kk;
                cc = c + ((m & -4) - 4);

                dsolve_4x4_ln_msa(aa, bb, cc, ldc, (k - kk));

                kk -= 4;
            }
        }

        i = (m >> 3);
        if (i > 0)
        {
            aa = a + ((m & -8) - 8) * k;
            cc = c + ((m & -8) - 8);

            do
            {
                dsolve_8x4_ln_msa(aa + 8 * kk, b + 4 * kk, cc, ldc, (k - kk));

                aa -= 8 * k;
                cc -= 8;
                kk -= 8;
                i --;
            } while (i > 0);
        }

        b += 4 * k;
        c += 4 * ldc;
    }

    if (n & 3)
    {
        if (n & 2)
        {
            kk = m + offset;

            if (m & 7)
            {
                if (m & 1)
                {
                    aa = a + ((m & -1) - 1) * k;
                    cc = c + ((m & -1) - 1);

                    dsolve_1x2_ln_msa(aa + kk - 1, b + kk * 2 - 2, cc, ldc);

                    kk -= 1;
                }

                if (m & 2)
                {
                    aa = a + ((m & -2) - 2) * k;
                    cc = c + ((m & -2) - 2);

                    dsolve_2x2_ln_msa(aa + kk * 2, b + kk * 2, cc, ldc, (k - kk));

                    kk -= 2;
                }

                if (m & 4)
                {
                    aa = a + ((m & -4) - 4) * k;
                    cc = c + ((m & -4) - 4);

                    dsolve_4x2_ln_msa(aa + kk * 4, b + kk * 2, cc, ldc, (k - kk));

                    kk -= 4;
                }
            }

            i = (m >> 3);
            if (i > 0)
            {
                aa = a + ((m & -8) - 8) * k;
                cc = c + ((m & -8) - 8);

                do
                {
                    dsolve_8x2_ln_msa(aa + kk * 8, b + kk * 2, cc, ldc, (k - kk));

                    aa -= 8 * k;
                    cc -= 8;
                    kk -= 8;
                    i --;
                } while (i > 0);
            }

            b += 2 * k;
            c += 2 * ldc;
        }

        if (n & 1)
        {
            kk = m + offset;

            if (m & 7)
            {
                if (m & 1)
                {
                    kk -= 1;
                    aa = a + ((m & -1) - 1) * k + kk;
                    cc = c + ((m & -1) - 1);

                    *cc *= *aa;
                    *(b + kk) = *cc;
                }

                if (m & 2)
                {
                    aa = a + ((m & -2) - 2) * k + kk * 2;
                    cc = c + ((m & -2) - 2);

                    dsolve_2x1_ln_msa(aa, b + kk, cc, (k - kk));

                    kk -= 2;
                }

                if (m & 4)
                {
                    aa = a + ((m & -4) - 4) * k;
                    cc = c + ((m & -4) - 4);

                    dsolve_4x1_ln_msa(aa + 4 * kk, b + kk, cc, (k - kk));

                    kk -= 4;
                }
            }

            i = (m >> 3);
            if (i > 0)
            {
                aa = a + ((m & -8) - 8) * k;
                cc = c + ((m & -8) - 8);

                do
                {
                    dsolve_8x1_ln_msa(aa + 8 * kk, b + kk, cc, (k - kk));

                    aa -= 8 * k;
                    cc -= 8;
                    kk -= 8;
                    i --;
                } while (i > 0);
            }
        }
    }

    return 0;
}
