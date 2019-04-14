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
void dsolve_8x4_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    v2f64 src_c0, src_c1, src_c2, src_c3, src_c4, src_c5, src_c6, src_c7;
    v2f64 src_c8, src_c9, src_c10, src_c11, src_c12, src_c13, src_c14, src_c15;
    v2f64 src_b0, src_b1, src_b2, src_b3, src_b5, src_b6, src_b7;
    v2f64 src_b10, src_b11, src_b15;
    FLOAT *c_nxt1line = c + ldc;
    FLOAT *c_nxt2line = c + 2 * ldc;
    FLOAT *c_nxt3line = c + 3 * ldc;

    LD_DP4(c, 2, src_c0, src_c1, src_c2, src_c3);
    LD_DP4(c_nxt1line, 2, src_c4, src_c5, src_c6, src_c7);
    LD_DP4(c_nxt2line, 2, src_c8, src_c9, src_c10, src_c11);
    LD_DP4(c_nxt3line, 2, src_c12, src_c13, src_c14, src_c15);

    if (bk)
    {
        BLASLONG i, pref_offset;
        FLOAT *pa0_pref;
        v2f64 src_a0, src_a1, src_a2, src_a3, src_b;

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

    src_b0 = LD_DP(b + 0);
    src_b1 = (v2f64) __msa_splati_d((v2i64) src_b0, 1);
    src_b0 = (v2f64) __msa_splati_d((v2i64) src_b0, 0);
    src_b2 = LD_DP(b + 2);
    src_b3 = (v2f64) __msa_splati_d((v2i64) src_b2, 1);
    src_b2 = (v2f64) __msa_splati_d((v2i64) src_b2, 0);
    src_b5 = __msa_cast_to_vector_double(*(b + 5));
    src_b5 = (v2f64) __msa_splati_d((v2i64) src_b5, 0);
    src_b6 = LD_DP(b + 6);
    src_b7 = (v2f64) __msa_splati_d((v2i64) src_b6, 1);
    src_b6 = (v2f64) __msa_splati_d((v2i64) src_b6, 0);
    src_b10 = LD_DP(b + 10);
    src_b11 = (v2f64) __msa_splati_d((v2i64) src_b10, 1);
    src_b10 = (v2f64) __msa_splati_d((v2i64) src_b10, 0);
    src_b15 = __msa_cast_to_vector_double(*(b + 15));
    src_b15 = (v2f64) __msa_splati_d((v2i64) src_b15, 0);

    src_c0 *= src_b0;
    src_c1 *= src_b0;
    src_c2 *= src_b0;
    src_c3 *= src_b0;

    src_c4 -= src_c0 * src_b1;
    src_c5 -= src_c1 * src_b1;
    src_c6 -= src_c2 * src_b1;
    src_c7 -= src_c3 * src_b1;

    src_c4 *= src_b5;
    src_c5 *= src_b5;
    src_c6 *= src_b5;
    src_c7 *= src_b5;

    src_c8 -= src_c0 * src_b2;
    src_c9 -= src_c1 * src_b2;
    src_c10 -= src_c2 * src_b2;
    src_c11 -= src_c3 * src_b2;

    src_c8 -= src_c4 * src_b6;
    src_c9 -= src_c5 * src_b6;
    src_c10 -= src_c6 * src_b6;
    src_c11 -= src_c7 * src_b6;

    src_c8 *= src_b10;
    src_c9 *= src_b10;
    src_c10 *= src_b10;
    src_c11 *= src_b10;

    src_c12 -= src_c0 * src_b3;
    src_c13 -= src_c1 * src_b3;
    src_c14 -= src_c2 * src_b3;
    src_c15 -= src_c3 * src_b3;

    src_c12 -= src_c4 * src_b7;
    src_c13 -= src_c5 * src_b7;
    src_c14 -= src_c6 * src_b7;
    src_c15 -= src_c7 * src_b7;

    src_c12 -= src_c8 * src_b11;
    src_c13 -= src_c9 * src_b11;
    src_c14 -= src_c10 * src_b11;
    src_c15 -= src_c11 * src_b11;

    src_c12 *= src_b15;
    src_c13 *= src_b15;
    src_c14 *= src_b15;
    src_c15 *= src_b15;

    ST_DP4(src_c0, src_c1, src_c2, src_c3, c, 2);
    ST_DP4(src_c0, src_c1, src_c2, src_c3, a, 2);
    ST_DP4(src_c4, src_c5, src_c6, src_c7, c_nxt1line, 2);
    ST_DP4(src_c4, src_c5, src_c6, src_c7, a + 8, 2);
    ST_DP4(src_c8, src_c9, src_c10, src_c11, c_nxt2line, 2);
    ST_DP4(src_c8, src_c9, src_c10, src_c11, a + 16, 2);
    ST_DP4(src_c12, src_c13, src_c14, src_c15, c_nxt3line, 2);
    ST_DP4(src_c12, src_c13, src_c14, src_c15, a + 24, 2);
}

static void dsolve_8x2_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    v2f64 src_c0, src_c1, src_c2, src_c3, src_c4, src_c5, src_c6, src_c7;
    v2f64 src_b0, src_b1, src_b3, src_b;

    LD_DP4(c, 2, src_c0, src_c1, src_c2, src_c3);
    LD_DP4(c + ldc, 2, src_c4, src_c5, src_c6, src_c7);

    if (bk)
    {
        BLASLONG i;
        v2f64 src_a0, src_a1, src_a2, src_a3, src_a4, src_a5, src_a6, src_a7;

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

    src_b0 = LD_DP(b + 0);
    src_b1 = (v2f64) __msa_splati_d((v2i64) src_b0, 1);
    src_b0 = (v2f64) __msa_splati_d((v2i64) src_b0, 0);
    src_b3 = __msa_cast_to_vector_double(*(b + 3));
    src_b3 = (v2f64) __msa_splati_d((v2i64) src_b3, 0);

    src_c0 *= src_b0;
    src_c1 *= src_b0;
    src_c2 *= src_b0;
    src_c3 *= src_b0;

    src_c4 -= src_c0 * src_b1;
    src_c5 -= src_c1 * src_b1;
    src_c6 -= src_c2 * src_b1;
    src_c7 -= src_c3 * src_b1;

    src_c4 *= src_b3;
    src_c5 *= src_b3;
    src_c6 *= src_b3;
    src_c7 *= src_b3;

    ST_DP4(src_c0, src_c1, src_c2, src_c3, c, 2);
    ST_DP4(src_c4, src_c5, src_c6, src_c7, c + ldc, 2);

    ST_DP4(src_c0, src_c1, src_c2, src_c3, a, 2);
    ST_DP4(src_c4, src_c5, src_c6, src_c7, a + 8, 2);
}

static void dsolve_8x1_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG bk)
{
    v2f64 src_c0, src_c1, src_c2, src_c3;
    v2f64 src_b0;

    LD_DP4(c, 2, src_c0, src_c1, src_c2, src_c3);

    if (bk)
    {
        BLASLONG i;
        v2f64 src_a0, src_a1, src_a2, src_a3, src_b;

        for (i = bk; i--;)
        {
            LD_DP4(a, 2, src_a0, src_a1, src_a2, src_a3);
            src_b = LD_DP(b);

            src_b = (v2f64) __msa_ilvr_d((v2i64) src_b, (v2i64) src_b);
            src_c0 -= src_a0 * src_b;
            src_c1 -= src_a1 * src_b;
            src_c2 -= src_a2 * src_b;
            src_c3 -= src_a3 * src_b;

            a += 8;
            b += 1;
        }
    }

    src_b0 = __msa_cast_to_vector_double(*b);
    src_b0 = (v2f64) __msa_splati_d((v2i64) src_b0, 0);

    src_c0 *= src_b0;
    src_c1 *= src_b0;
    src_c2 *= src_b0;
    src_c3 *= src_b0;

    ST_DP4(src_c0, src_c1, src_c2, src_c3, c, 2);
    ST_DP4(src_c0, src_c1, src_c2, src_c3, a, 2);
}

static void dsolve_4x4_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    v2f64 src_c0, src_c1, src_c2, src_c3, src_c4, src_c5, src_c6, src_c7;
    v2f64 src_b0, src_b1, src_b2, src_b3, src_b5, src_b6, src_b7;
    v2f64 src_b10, src_b11, src_b15;

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

    src_b0 = LD_DP(b + 0);
    src_b1 = (v2f64) __msa_splati_d((v2i64) src_b0, 1);
    src_b0 = (v2f64) __msa_splati_d((v2i64) src_b0, 0);
    src_b2 = LD_DP(b + 2);
    src_b3 = (v2f64) __msa_splati_d((v2i64) src_b2, 1);
    src_b2 = (v2f64) __msa_splati_d((v2i64) src_b2, 0);
    src_b5 = __msa_cast_to_vector_double(*(b + 5));
    src_b5 = (v2f64) __msa_splati_d((v2i64) src_b5, 0);
    src_b6 = LD_DP(b + 6);
    src_b7 = (v2f64) __msa_splati_d((v2i64) src_b6, 1);
    src_b6 = (v2f64) __msa_splati_d((v2i64) src_b6, 0);
    src_b10 = LD_DP(b + 10);
    src_b11 = (v2f64) __msa_splati_d((v2i64) src_b10, 1);
    src_b10 = (v2f64) __msa_splati_d((v2i64) src_b10, 0);
    src_b15 = __msa_cast_to_vector_double(*(b + 15));
    src_b15 = (v2f64) __msa_splati_d((v2i64) src_b15, 0);

    src_c0 *= src_b0;
    src_c1 *= src_b0;

    src_c2 -= src_c0 * src_b1;
    src_c3 -= src_c1 * src_b1;

    src_c2 *= src_b5;
    src_c3 *= src_b5;

    src_c4 -= src_c0 * src_b2;
    src_c5 -= src_c1 * src_b2;

    src_c4 -= src_c2 * src_b6;
    src_c5 -= src_c3 * src_b6;

    src_c4 *= src_b10;
    src_c5 *= src_b10;

    src_c6 -= src_c0 * src_b3;
    src_c7 -= src_c1 * src_b3;

    src_c6 -= src_c2 * src_b7;
    src_c7 -= src_c3 * src_b7;

    src_c6 -= src_c4 * src_b11;
    src_c7 -= src_c5 * src_b11;

    src_c6 *= src_b15;
    src_c7 *= src_b15;

    ST_DP2(src_c0, src_c1, c, 2);
    ST_DP2(src_c2, src_c3, c + ldc, 2);
    ST_DP2(src_c4, src_c5, c + 2 * ldc, 2);
    ST_DP2(src_c6, src_c7, c + 3 * ldc, 2);

    ST_DP4(src_c0, src_c1, src_c2, src_c3, a, 2);
    ST_DP4(src_c4, src_c5, src_c6, src_c7, a + 8, 2);
}

static void dsolve_4x2_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    v2f64 src_c0, src_c1, src_c2, src_c3, src_b0, src_b1, src_b3;

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

    src_b0 = LD_DP(b + 0);
    src_b1 = (v2f64) __msa_splati_d((v2i64) src_b0, 1);
    src_b0 = (v2f64) __msa_splati_d((v2i64) src_b0, 0);
    src_b3 = __msa_cast_to_vector_double(*(b + 3));
    src_b3 = (v2f64) __msa_splati_d((v2i64) src_b3, 0);

    src_c0 *= src_b0;
    src_c1 *= src_b0;

    src_c2 -= src_c0 * src_b1;
    src_c3 -= src_c1 * src_b1;

    src_c2 *= src_b3;
    src_c3 *= src_b3;

    ST_DP2(src_c0, src_c1, c, 2);
    ST_DP2(src_c2, src_c3, c + ldc, 2);

    ST_DP4(src_c0, src_c1, src_c2, src_c3, a, 2);
}

static void dsolve_4x1_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG bk)
{
    FLOAT c0, c1, c2, c3;

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

    c0 *= *b;
    c1 *= *b;
    c2 *= *b;
    c3 *= *b;

    *(a + 0) = c0;
    *(a + 1) = c1;
    *(a + 2) = c2;
    *(a + 3) = c3;

    *(c + 0) = c0;
    *(c + 1) = c1;
    *(c + 2) = c2;
    *(c + 3) = c3;
}

static void dsolve_2x4_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    FLOAT b0, b1, b2, b3, b5, b6, b7, b10, b11, b15;
    FLOAT c0, c0_nxt1, c0_nxt2, c0_nxt3;
    FLOAT c1, c1_nxt1, c1_nxt2, c1_nxt3;

    c0 = *(c + 0);
    c1 = *(c + 1);
    c0_nxt1 = *(c + 0 + 1 * ldc);
    c1_nxt1 = *(c + 1 + 1 * ldc);
    c0_nxt2 = *(c + 0 + 2 * ldc);
    c1_nxt2 = *(c + 1 + 2 * ldc);
    c0_nxt3 = *(c + 0 + 3 * ldc);
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

    b0 = *(b + 0);
    b1 = *(b + 1);
    b2 = *(b + 2);
    b3 = *(b + 3);
    b5 = *(b + 5);
    b6 = *(b + 6);
    b7 = *(b + 7);
    b10 = *(b + 10);
    b11 = *(b + 11);
    b15 = *(b + 15);

    c0 *= b0;
    c1 *= b0;

    c0_nxt1 -= c0 * b1;
    c1_nxt1 -= c1 * b1;
    c0_nxt1 *= b5;
    c1_nxt1 *= b5;

    c0_nxt2 -= c0 * b2;
    c1_nxt2 -= c1 * b2;
    c0_nxt2 -= c0_nxt1 * b6;
    c1_nxt2 -= c1_nxt1 * b6;
    c0_nxt2 *= b10;
    c1_nxt2 *= b10;

    c0_nxt3 -= c0 * b3;
    c1_nxt3 -= c1 * b3;
    c0_nxt3 -= c0_nxt1 * b7;
    c1_nxt3 -= c1_nxt1 * b7;
    c0_nxt3 -= c0_nxt2 * b11;
    c1_nxt3 -= c1_nxt2 * b11;
    c0_nxt3 *= b15;
    c1_nxt3 *= b15;

    *(a + 0) = c0;
    *(a + 1) = c1;
    *(a + 2) = c0_nxt1;
    *(a + 3) = c1_nxt1;
    *(a + 4) = c0_nxt2;
    *(a + 5) = c1_nxt2;
    *(a + 6) = c0_nxt3;
    *(a + 7) = c1_nxt3;

    *(c + 0) = c0;
    *(c + 1) = c1;
    *(c + 1 * ldc) = c0_nxt1;
    *(c + 1 + 1 * ldc) = c1_nxt1;
    *(c + 2 * ldc) = c0_nxt2;
    *(c + 1 + 2 * ldc) = c1_nxt2;
    *(c + 3 * ldc) = c0_nxt3;
    *(c + 1 + 3 * ldc) = c1_nxt3;
}

static void dsolve_2x2_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    FLOAT b0, b1, b3, c0, c0_nxt, c1, c1_nxt;

    c0 = *(c + 0);
    c1 = *(c + 1);
    c0_nxt = *(c + 0 + ldc);
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

    b0 = *(b + 0);
    b1 = *(b + 1);
    b3 = *(b + 3);

    c0 *= b0;
    c1 *= b0;

    c0_nxt -= c0 * b1;
    c1_nxt -= c1 * b1;

    c0_nxt *= b3;
    c1_nxt *= b3;

    *(a + 0) = c0;
    *(a + 1) = c1;
    *(a + 2) = c0_nxt;
    *(a + 3) = c1_nxt;

    *(c + 0) = c0;
    *(c + 1) = c1;
    *(c + 0 + ldc) = c0_nxt;
    *(c + 1 + ldc) = c1_nxt;
}

static void dsolve_2x1_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG bk)
{
    FLOAT b0, c0, c1;

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

    b0 = *b;

    c0 *= b0;
    c1 *= b0;

    *(a + 0) = c0;
    *(a + 1) = c1;

    *(c + 0) = c0;
    *(c + 1) = c1;
}

static void dsolve_1x4_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    FLOAT b0, b1, b2, b3, b5, b6, b7, b10, b11, b15, c0, c1, c2, c3;

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

    b0 = *(b + 0);
    b1 = *(b + 1);
    b2 = *(b + 2);
    b3 = *(b + 3);
    b5 = *(b + 5);
    b6 = *(b + 6);
    b7 = *(b + 7);
    b10 = *(b + 10);
    b11 = *(b + 11);
    b15 = *(b + 15);

    c0 *= b0;

    c1 -= c0 * b1;
    c1 *= b5;

    c2 -= c0 * b2;
    c2 -= c1 * b6;
    c2 *= b10;

    c3 -= c0 * b3;
    c3 -= c1 * b7;
    c3 -= c2 * b11;
    c3 *= b15;

    *(a + 0) = c0;
    *(a + 1) = c1;
    *(a + 2) = c2;
    *(a + 3) = c3;

    *(c + 0) = c0;
    *(c + 1 * ldc) = c1;
    *(c + 2 * ldc) = c2;
    *(c + 3 * ldc) = c3;
}

static void dsolve_1x2_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    FLOAT b0, b1, b3, c0, c1;

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

    b0 = *(b + 0);
    b1 = *(b + 1);
    b3 = *(b + 3);

    c0 *= b0;

    c1 -= c0 * b1;
    c1 *= b3;

    *(a + 0) = c0;
    *(a + 1) = c1;

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

    kk = -offset;

    for (j = (n >> 2); j--;)
    {
        aa = a;
        cc = c;

        for (i = (m >> 3); i--;)
        {
            dsolve_8x4_rn_msa(aa, b, cc, ldc, kk);

            aa += 8 * k;
            cc += 8;
        }

        if (m & 7)
        {
            if (m & 4)
            {
                dsolve_4x4_rn_msa(aa, b, cc, ldc, kk);

                aa += 4 * k;
                cc += 4;
            }

            if (m & 2)
            {
                dsolve_2x4_rn_msa(aa, b, cc, ldc, kk);

                aa += 2 * k;
                cc += 2;
            }

            if (m & 1)
            {
                dsolve_1x4_rn_msa(aa, b, cc, ldc, kk);

                aa += k;
                cc += 1;
            }
        }

        kk += 4;
        b += 4 * k;
        c += 4 * ldc;
    }

    if (n & 3)
    {
        if (n & 2)
        {
            aa = a;
            cc = c;

            for (i = (m >> 3); i--;)
            {
                dsolve_8x2_rn_msa(aa, b, cc, ldc, kk);

                aa += 8 * k;
                cc += 8;
            }

            if (m & 7)
            {
                if (m & 4)
                {
                    dsolve_4x2_rn_msa(aa, b, cc, ldc, kk);

                    aa += 4 * k;
                    cc += 4;
                }

                if (m & 2)
                {
                    dsolve_2x2_rn_msa(aa, b, cc, ldc, kk);

                    aa += 2 * k;
                    cc += 2;
                }

                if (m & 1)
                {
                    dsolve_1x2_rn_msa(aa, b, cc, ldc, kk);

                    aa += k;
                    cc += 1;
                }
            }

            b += 2 * k;
            c += 2 * ldc;
            kk += 2;
        }

        if (n & 1)
        {
            aa = a;
            cc = c;

            for (i = (m >> 3); i--;)
            {
                dsolve_8x1_rn_msa(aa, b, cc, kk);

                aa += 8 * k;
                cc += 8;
            }

            if (m & 7)
            {
                if (m & 4)
                {
                    dsolve_4x1_rn_msa(aa, b, cc, kk);

                    aa += 4 * k;
                    cc += 4;
                }

                if (m & 2)
                {
                    dsolve_2x1_rn_msa(aa, b, cc, kk);

                    aa += 2 * k;
                    cc += 2;
                }

                if (m & 1)
                {
                    dgmm_dsolve_1x1_msa(b, aa, cc, kk);

                    aa += k;
                    cc += 1;
                }
            }

            b += k;
            c += ldc;
            kk += 1;
        }
    }

    return 0;
}
