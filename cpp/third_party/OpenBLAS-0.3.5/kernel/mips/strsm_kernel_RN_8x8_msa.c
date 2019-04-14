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

static void ssolve_8x8_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    v4f32 src_c0, src_c1, src_c2, src_c3, src_c4, src_c5, src_c6, src_c7;
    v4f32 src_c8, src_c9, src_c10, src_c11, src_c12, src_c13, src_c14, src_c15;
    v4f32 src_b0, src_b1, src_b2, src_b3, src_b4, src_b5, src_b6, src_b7;
    v4f32 src_b9, src_b10, src_b11, src_b12, src_b13, src_b14, src_b15, src_b18;
    v4f32 src_b19, src_b20, src_b21, src_b22, src_b23, src_b27, src_b28;
    v4f32 src_b29, src_b30, src_b31, src_b36, src_b37, src_b38, src_b39;
    v4f32 src_b45, src_b46, src_b47, src_b54, src_b55, src_b63, src_b;
    FLOAT *c_nxt1line = c + ldc;
    FLOAT *c_nxt2line = c + 2 * ldc;
    FLOAT *c_nxt3line = c + 3 * ldc;
    FLOAT *c_nxt4line = c + 4 * ldc;
    FLOAT *c_nxt5line = c + 5 * ldc;
    FLOAT *c_nxt6line = c + 6 * ldc;
    FLOAT *c_nxt7line = c + 7 * ldc;

    LD_SP2(c, 4, src_c0, src_c1);
    LD_SP2(c_nxt1line, 4, src_c2, src_c3);
    LD_SP2(c_nxt2line, 4, src_c4, src_c5);
    LD_SP2(c_nxt3line, 4, src_c6, src_c7);
    LD_SP2(c_nxt4line, 4, src_c8, src_c9);
    LD_SP2(c_nxt5line, 4, src_c10, src_c11);
    LD_SP2(c_nxt6line, 4, src_c12, src_c13);
    LD_SP2(c_nxt7line, 4, src_c14, src_c15);

    if (bk > 0)
    {
        BLASLONG k, pref_offset;
        FLOAT *pa0_pref;
        v4f32 src_a0, src_a1, src_bb0, src_bb1;

        pref_offset = (uintptr_t)a & (L1_DATA_LINESIZE - 1);

        if (pref_offset)
        {
            pref_offset = L1_DATA_LINESIZE - pref_offset;
            pref_offset = pref_offset / sizeof(FLOAT);
        }

        pa0_pref = a + pref_offset;

        for (k = 0; k < (bk >> 1); k++)
        {
            PREF_OFFSET(pa0_pref, 64);
            PREF_OFFSET(pa0_pref, 96);

            LD_SP2_INC(a, 4, src_a0, src_a1);
            LD_SP2_INC(b, 4, src_bb0, src_bb1);

            SPLATI_W4_SP(src_bb0, src_b0, src_b1, src_b2, src_b3);
            src_c0 -= src_a0 * src_b0;
            src_c1 -= src_a1 * src_b0;
            src_c2 -= src_a0 * src_b1;
            src_c3 -= src_a1 * src_b1;
            src_c4 -= src_a0 * src_b2;
            src_c5 -= src_a1 * src_b2;
            src_c6 -= src_a0 * src_b3;
            src_c7 -= src_a1 * src_b3;

            SPLATI_W4_SP(src_bb1, src_b0, src_b1, src_b2, src_b3);
            src_c8 -= src_a0 * src_b0;
            src_c9 -= src_a1 * src_b0;
            src_c10 -= src_a0 * src_b1;
            src_c11 -= src_a1 * src_b1;
            src_c12 -= src_a0 * src_b2;
            src_c13 -= src_a1 * src_b2;
            src_c14 -= src_a0 * src_b3;
            src_c15 -= src_a1 * src_b3;

            LD_SP2_INC(a, 4, src_a0, src_a1);
            LD_SP2_INC(b, 4, src_bb0, src_bb1);

            SPLATI_W4_SP(src_bb0, src_b0, src_b1, src_b2, src_b3);
            src_c0 -= src_a0 * src_b0;
            src_c1 -= src_a1 * src_b0;
            src_c2 -= src_a0 * src_b1;
            src_c3 -= src_a1 * src_b1;
            src_c4 -= src_a0 * src_b2;
            src_c5 -= src_a1 * src_b2;
            src_c6 -= src_a0 * src_b3;
            src_c7 -= src_a1 * src_b3;

            SPLATI_W4_SP(src_bb1, src_b0, src_b1, src_b2, src_b3);
            src_c8 -= src_a0 * src_b0;
            src_c9 -= src_a1 * src_b0;
            src_c10 -= src_a0 * src_b1;
            src_c11 -= src_a1 * src_b1;
            src_c12 -= src_a0 * src_b2;
            src_c13 -= src_a1 * src_b2;
            src_c14 -= src_a0 * src_b3;
            src_c15 -= src_a1 * src_b3;

            pa0_pref += 16;
        }

        if (bk & 1)
        {
            LD_SP2_INC(a, 4, src_a0, src_a1);
            LD_SP2_INC(b, 4, src_bb0, src_bb1);

            SPLATI_W4_SP(src_bb0, src_b0, src_b1, src_b2, src_b3);
            src_c0 -= src_a0 * src_b0;
            src_c1 -= src_a1 * src_b0;
            src_c2 -= src_a0 * src_b1;
            src_c3 -= src_a1 * src_b1;
            src_c4 -= src_a0 * src_b2;
            src_c5 -= src_a1 * src_b2;
            src_c6 -= src_a0 * src_b3;
            src_c7 -= src_a1 * src_b3;

            SPLATI_W4_SP(src_bb1, src_b0, src_b1, src_b2, src_b3);
            src_c8 -= src_a0 * src_b0;
            src_c9 -= src_a1 * src_b0;
            src_c10 -= src_a0 * src_b1;
            src_c11 -= src_a1 * src_b1;
            src_c12 -= src_a0 * src_b2;
            src_c13 -= src_a1 * src_b2;
            src_c14 -= src_a0 * src_b3;
            src_c15 -= src_a1 * src_b3;
        }
    }

    src_b = LD_SP(b + 0);
    SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);
    src_b = LD_SP(b + 4);
    SPLATI_W4_SP(src_b, src_b4, src_b5, src_b6, src_b7);

    src_b = LD_SP(b + 9);
    SPLATI_W4_SP(src_b, src_b9, src_b10, src_b11, src_b12);
    src_b13 = LD_SP(b + 13);
    src_b15 = (v4f32) __msa_splati_w((v4i32) src_b13, 2);
    src_b14 = (v4f32) __msa_splati_w((v4i32) src_b13, 1);
    src_b13 = (v4f32) __msa_splati_w((v4i32) src_b13, 0);

    src_c0 *= src_b0;
    src_c1 *= src_b0;
    src_c2 -= src_c0 * src_b1;
    src_c3 -= src_c1 * src_b1;
    src_c4 -= src_c0 * src_b2;
    src_c5 -= src_c1 * src_b2;
    src_c6 -= src_c0 * src_b3;
    src_c7 -= src_c1 * src_b3;
    src_c8 -= src_c0 * src_b4;
    src_c9 -= src_c1 * src_b4;
    src_c10 -= src_c0 * src_b5;
    src_c11 -= src_c1 * src_b5;
    src_c12 -= src_c0 * src_b6;
    src_c13 -= src_c1 * src_b6;
    src_c14 -= src_c0 * src_b7;
    src_c15 -= src_c1 * src_b7;

    ST_SP2(src_c0, src_c1, a, 4);
    ST_SP2(src_c0, src_c1, c, 4);

    src_c2 *= src_b9;
    src_c3 *= src_b9;
    src_c4 -= src_c2 * src_b10;
    src_c5 -= src_c3 * src_b10;
    src_c6 -= src_c2 * src_b11;
    src_c7 -= src_c3 * src_b11;
    src_c8 -= src_c2 * src_b12;
    src_c9 -= src_c3 * src_b12;
    src_c10 -= src_c2 * src_b13;
    src_c11 -= src_c3 * src_b13;
    src_c12 -= src_c2 * src_b14;
    src_c13 -= src_c3 * src_b14;
    src_c14 -= src_c2 * src_b15;
    src_c15 -= src_c3 * src_b15;

    ST_SP2(src_c2, src_c3, a + 8, 4);
    ST_SP2(src_c2, src_c3, c_nxt1line, 4);

    src_b = LD_SP(b + 18);
    SPLATI_W4_SP(src_b, src_b18, src_b19, src_b20, src_b21);
    src_b22 = LD_SP(b + 22);
    src_b23 = (v4f32) __msa_splati_w((v4i32) src_b22, 1);
    src_b22 = (v4f32) __msa_splati_w((v4i32) src_b22, 0);

    src_b = LD_SP(b + 27);
    SPLATI_W4_SP(src_b, src_b27, src_b28, src_b29, src_b30);
    src_b31 = COPY_FLOAT_TO_VECTOR(*(b + 31));

    src_c4 *= src_b18;
    src_c5 *= src_b18;
    src_c6 -= src_c4 * src_b19;
    src_c7 -= src_c5 * src_b19;
    src_c8 -= src_c4 * src_b20;
    src_c9 -= src_c5 * src_b20;
    src_c10 -= src_c4 * src_b21;
    src_c11 -= src_c5 * src_b21;
    src_c12 -= src_c4 * src_b22;
    src_c13 -= src_c5 * src_b22;
    src_c14 -= src_c4 * src_b23;
    src_c15 -= src_c5 * src_b23;

    ST_SP2(src_c4, src_c5, a + 16, 4);
    ST_SP2(src_c4, src_c5, c_nxt2line, 4);

    src_c6 *= src_b27;
    src_c7 *= src_b27;
    src_c8 -= src_c6 * src_b28;
    src_c9 -= src_c7 * src_b28;
    src_c10 -= src_c6 * src_b29;
    src_c11 -= src_c7 * src_b29;
    src_c12 -= src_c6 * src_b30;
    src_c13 -= src_c7 * src_b30;
    src_c14 -= src_c6 * src_b31;
    src_c15 -= src_c7 * src_b31;

    ST_SP2(src_c6, src_c7, a + 24, 4);
    ST_SP2(src_c6, src_c7, c_nxt3line, 4);

    src_b = LD_SP(b + 36);
    SPLATI_W4_SP(src_b, src_b36, src_b37, src_b38, src_b39);

    src_b45 = LD_SP(b + 45);
    src_b47 = (v4f32) __msa_splati_w((v4i32) src_b45, 2);
    src_b46 = (v4f32) __msa_splati_w((v4i32) src_b45, 1);
    src_b45 = (v4f32) __msa_splati_w((v4i32) src_b45, 0);

    src_b54 = COPY_FLOAT_TO_VECTOR(*(b + 54));
    src_b55 = COPY_FLOAT_TO_VECTOR(*(b + 55));
    src_b63 = COPY_FLOAT_TO_VECTOR(*(b + 63));

    src_c8 *= src_b36;
    src_c9 *= src_b36;
    src_c10 -= src_c8 * src_b37;
    src_c11 -= src_c9 * src_b37;
    src_c12 -= src_c8 * src_b38;
    src_c13 -= src_c9 * src_b38;
    src_c14 -= src_c8 * src_b39;
    src_c15 -= src_c9 * src_b39;

    ST_SP2(src_c8, src_c9, a + 32, 4);
    ST_SP2(src_c8, src_c9, c_nxt4line, 4);

    src_c10 *= src_b45;
    src_c11 *= src_b45;
    src_c12 -= src_c10 * src_b46;
    src_c13 -= src_c11 * src_b46;
    src_c14 -= src_c10 * src_b47;
    src_c15 -= src_c11 * src_b47;

    ST_SP2(src_c10, src_c11, a + 40, 4);
    ST_SP2(src_c10, src_c11, c_nxt5line, 4);

    src_c12 *= src_b54;
    src_c13 *= src_b54;
    src_c14 -= src_c12 * src_b55;
    src_c15 -= src_c13 * src_b55;

    ST_SP2(src_c12, src_c13, a + 48, 4);
    ST_SP2(src_c12, src_c13, c_nxt6line, 4);

    src_c14 *= src_b63;
    src_c15 *= src_b63;

    ST_SP2(src_c14, src_c15, a + 56, 4);
    ST_SP2(src_c14, src_c15, c_nxt7line, 4);
}

static void ssolve_8x4_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    v4f32 src_c0, src_c1, src_c2, src_c3, src_c4, src_c5, src_c6, src_c7;
    v4f32 src_b0, src_b1, src_b2, src_b3, src_b5, src_b6, src_b7;
    v4f32 src_b10, src_b11, src_b15, src_b, src_a0, src_a1;
    FLOAT *c_nxt1line = c + ldc;
    FLOAT *c_nxt2line = c + 2 * ldc;
    FLOAT *c_nxt3line = c + 3 * ldc;

    LD_SP2(c, 4, src_c0, src_c1);
    LD_SP2(c_nxt1line, 4, src_c2, src_c3);
    LD_SP2(c_nxt2line, 4, src_c4, src_c5);
    LD_SP2(c_nxt3line, 4, src_c6, src_c7);

    for (k = 0; k < (bk >> 1); k++)
    {
        LD_SP2(a, 4, src_a0, src_a1);

        src_b = LD_SP(b + 0);
        SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);
        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a1 * src_b0;
        src_c2 -= src_a0 * src_b1;
        src_c3 -= src_a1 * src_b1;
        src_c4 -= src_a0 * src_b2;
        src_c5 -= src_a1 * src_b2;
        src_c6 -= src_a0 * src_b3;
        src_c7 -= src_a1 * src_b3;

        a += 8;
        b += 4;

        LD_SP2(a, 4, src_a0, src_a1);

        src_b = LD_SP(b + 0);
        SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);
        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a1 * src_b0;
        src_c2 -= src_a0 * src_b1;
        src_c3 -= src_a1 * src_b1;
        src_c4 -= src_a0 * src_b2;
        src_c5 -= src_a1 * src_b2;
        src_c6 -= src_a0 * src_b3;
        src_c7 -= src_a1 * src_b3;

        a += 8;
        b += 4;
    }

    if ((bk & 1) && (bk > 0))
    {
        LD_SP2(a, 4, src_a0, src_a1);

        src_b = LD_SP(b + 0);
        SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);
        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a1 * src_b0;
        src_c2 -= src_a0 * src_b1;
        src_c3 -= src_a1 * src_b1;
        src_c4 -= src_a0 * src_b2;
        src_c5 -= src_a1 * src_b2;
        src_c6 -= src_a0 * src_b3;
        src_c7 -= src_a1 * src_b3;

        a += 8;
        b += 4;
    }

    src_b = LD_SP(b + 0);
    SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);
    src_b5 = LD_SP(b + 5);
    src_b7 = (v4f32) __msa_splati_w((v4i32) src_b5, 2);
    src_b6 = (v4f32) __msa_splati_w((v4i32) src_b5, 1);
    src_b5 = (v4f32) __msa_splati_w((v4i32) src_b5, 0);
    src_b10 = COPY_FLOAT_TO_VECTOR(*(b + 10));
    src_b11 = COPY_FLOAT_TO_VECTOR(*(b + 11));
    src_b15 = COPY_FLOAT_TO_VECTOR(*(b + 15));

    src_c0 *= src_b0;
    src_c1 *= src_b0;
    src_c2 -= src_c0 * src_b1;
    src_c3 -= src_c1 * src_b1;
    src_c4 -= src_c0 * src_b2;
    src_c5 -= src_c1 * src_b2;
    src_c6 -= src_c0 * src_b3;
    src_c7 -= src_c1 * src_b3;

    src_c2 *= src_b5;
    src_c3 *= src_b5;
    src_c4 -= src_c2 * src_b6;
    src_c5 -= src_c3 * src_b6;
    src_c6 -= src_c2 * src_b7;
    src_c7 -= src_c3 * src_b7;

    src_c4 *= src_b10;
    src_c5 *= src_b10;
    src_c6 -= src_c4 * src_b11;
    src_c7 -= src_c5 * src_b11;

    src_c6 *= src_b15;
    src_c7 *= src_b15;

    ST_SP4(src_c0, src_c1, src_c2, src_c3, a, 4);
    ST_SP4(src_c4, src_c5, src_c6, src_c7, a + 16, 4);

    ST_SP2(src_c0, src_c1, c, 4);
    ST_SP2(src_c2, src_c3, c_nxt1line, 4);
    ST_SP2(src_c4, src_c5, c_nxt2line, 4);
    ST_SP2(src_c6, src_c7, c_nxt3line, 4);
}

static void ssolve_8x2_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    v4f32 src_a0, src_a1;
    v4f32 src_c0, src_c1, src_c2, src_c3, src_b0, src_b1, src_b3;
    FLOAT *c_nxt1line = c + ldc;

    LD_SP2(c, 4, src_c0, src_c1);
    LD_SP2(c_nxt1line, 4, src_c2, src_c3);

    for (k = 0; k < (bk >> 1); k++)
    {
        LD_SP2(a, 4, src_a0, src_a1);

        src_b0 = COPY_FLOAT_TO_VECTOR(*(b + 0));
        src_b1 = COPY_FLOAT_TO_VECTOR(*(b + 1));

        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a1 * src_b0;
        src_c2 -= src_a0 * src_b1;
        src_c3 -= src_a1 * src_b1;

        a += 8;
        b += 2;

        LD_SP2(a, 4, src_a0, src_a1);

        src_b0 = COPY_FLOAT_TO_VECTOR(*(b + 0));
        src_b1 = COPY_FLOAT_TO_VECTOR(*(b + 1));

        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a1 * src_b0;
        src_c2 -= src_a0 * src_b1;
        src_c3 -= src_a1 * src_b1;

        a += 8;
        b += 2;
    }

    if ((bk & 1) && (bk > 0))
    {
        LD_SP2(a, 4, src_a0, src_a1);

        src_b0 = COPY_FLOAT_TO_VECTOR(*(b + 0));
        src_b1 = COPY_FLOAT_TO_VECTOR(*(b + 1));

        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a1 * src_b0;
        src_c2 -= src_a0 * src_b1;
        src_c3 -= src_a1 * src_b1;

        a += 8;
        b += 2;
    }

    src_b0 = COPY_FLOAT_TO_VECTOR(*(b + 0));
    src_b1 = COPY_FLOAT_TO_VECTOR(*(b + 1));
    src_b3 = COPY_FLOAT_TO_VECTOR(*(b + 3));

    src_c0 *= src_b0;
    src_c1 *= src_b0;
    src_c2 -= src_c0 * src_b1;
    src_c3 -= src_c1 * src_b1;
    src_c2 *= src_b3;
    src_c3 *= src_b3;

    ST_SP4(src_c0, src_c1, src_c2, src_c3, a, 4);
    ST_SP2(src_c0, src_c1, c, 4);
    ST_SP2(src_c2, src_c3, c_nxt1line, 4);
}

static void ssolve_8x1_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    v4f32 src_a0, src_a1, src_c0, src_c1, src_b0;

    LD_SP2(c, 4, src_c0, src_c1);

    for (k = 0; k < (bk >> 2); k++)
    {
        LD_SP2(a, 4, src_a0, src_a1);

        src_b0 = COPY_FLOAT_TO_VECTOR(*(b + 0));

        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a1 * src_b0;

        a += 8;
        b += 1;

        LD_SP2(a, 4, src_a0, src_a1);

        src_b0 = COPY_FLOAT_TO_VECTOR(*(b + 0));

        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a1 * src_b0;

        a += 8;
        b += 1;

        LD_SP2(a, 4, src_a0, src_a1);

        src_b0 = COPY_FLOAT_TO_VECTOR(*(b + 0));

        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a1 * src_b0;

        a += 8;
        b += 1;

        LD_SP2(a, 4, src_a0, src_a1);

        src_b0 = COPY_FLOAT_TO_VECTOR(*(b + 0));

        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a1 * src_b0;

        a += 8;
        b += 1;
    }

    if ((bk & 3) && (bk > 0))
    {
        if (bk & 2)
        {
            LD_SP2(a, 4, src_a0, src_a1);

            src_b0 = COPY_FLOAT_TO_VECTOR(*(b + 0));

            src_c0 -= src_a0 * src_b0;
            src_c1 -= src_a1 * src_b0;

            a += 8;
            b += 1;

            LD_SP2(a, 4, src_a0, src_a1);

            src_b0 = COPY_FLOAT_TO_VECTOR(*(b + 0));

            src_c0 -= src_a0 * src_b0;
            src_c1 -= src_a1 * src_b0;

            a += 8;
            b += 1;
        }

        if (bk & 1)
        {
            LD_SP2(a, 4, src_a0, src_a1);

            src_b0 = COPY_FLOAT_TO_VECTOR(*(b + 0));

            src_c0 -= src_a0 * src_b0;
            src_c1 -= src_a1 * src_b0;

            a += 8;
            b += 1;
        }
    }

    src_b0 = COPY_FLOAT_TO_VECTOR(*(b + 0));

    src_c0 *= src_b0;
    src_c1 *= src_b0;

    ST_SP2(src_c0, src_c1, a, 4);
    ST_SP2(src_c0, src_c1, c, 4);
}

static void ssolve_4x8_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    v4f32 src_c0, src_c1, src_c2, src_c3, src_c4, src_c5, src_c6, src_c7;
    v4f32 src_b0, src_b1, src_b2, src_b3, src_b4, src_b5, src_b6, src_b7;
    v4f32 src_b9, src_b10, src_b11, src_b12, src_b13, src_b14, src_b15, src_b18;
    v4f32 src_b19, src_b20, src_b21, src_b22, src_b23, src_b27, src_b28;
    v4f32 src_b29, src_b30, src_b31, src_b36, src_b37, src_b38, src_b39;
    v4f32 src_b45, src_b46, src_b47, src_b54, src_b55, src_b63, src_b, src_a0;
    FLOAT *c_nxt1line = c + ldc;
    FLOAT *c_nxt2line = c + 2 * ldc;
    FLOAT *c_nxt3line = c + 3 * ldc;
    FLOAT *c_nxt4line = c + 4 * ldc;
    FLOAT *c_nxt5line = c + 5 * ldc;
    FLOAT *c_nxt6line = c + 6 * ldc;
    FLOAT *c_nxt7line = c + 7 * ldc;

    src_c0 = LD_SP(c);
    src_c1 = LD_SP(c_nxt1line);
    src_c2 = LD_SP(c_nxt2line);
    src_c3 = LD_SP(c_nxt3line);
    src_c4 = LD_SP(c_nxt4line);
    src_c5 = LD_SP(c_nxt5line);
    src_c6 = LD_SP(c_nxt6line);
    src_c7 = LD_SP(c_nxt7line);

    for (k = 0; k < bk; k++)
    {
        src_a0 = LD_SP(a);

        src_b = LD_SP(b + 0);
        SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);
        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a0 * src_b1;
        src_c2 -= src_a0 * src_b2;
        src_c3 -= src_a0 * src_b3;

        src_b = LD_SP(b + 4);
        SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);
        src_c4 -= src_a0 * src_b0;
        src_c5 -= src_a0 * src_b1;
        src_c6 -= src_a0 * src_b2;
        src_c7 -= src_a0 * src_b3;

        a += 4;
        b += 8;
    }

    src_b = LD_SP(b + 0);
    SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);
    src_b = LD_SP(b + 4);
    SPLATI_W4_SP(src_b, src_b4, src_b5, src_b6, src_b7);

    src_b = LD_SP(b + 9);
    SPLATI_W4_SP(src_b, src_b9, src_b10, src_b11, src_b12);
    src_b13 = LD_SP(b + 13);
    src_b15 = (v4f32) __msa_splati_w((v4i32) src_b13, 2);
    src_b14 = (v4f32) __msa_splati_w((v4i32) src_b13, 1);
    src_b13 = (v4f32) __msa_splati_w((v4i32) src_b13, 0);

    src_b = LD_SP(b + 18);
    SPLATI_W4_SP(src_b, src_b18, src_b19, src_b20, src_b21);
    src_b22 = LD_SP(b + 22);
    src_b23 = (v4f32) __msa_splati_w((v4i32) src_b22, 1);
    src_b22 = (v4f32) __msa_splati_w((v4i32) src_b22, 0);

    src_b = LD_SP(b + 27);
    SPLATI_W4_SP(src_b, src_b27, src_b28, src_b29, src_b30);
    src_b31 = COPY_FLOAT_TO_VECTOR(*(b + 31));

    src_b = LD_SP(b + 36);
    SPLATI_W4_SP(src_b, src_b36, src_b37, src_b38, src_b39);

    src_b45 = LD_SP(b + 45);
    src_b47 = (v4f32) __msa_splati_w((v4i32) src_b45, 2);
    src_b46 = (v4f32) __msa_splati_w((v4i32) src_b45, 1);
    src_b45 = (v4f32) __msa_splati_w((v4i32) src_b45, 0);

    src_b54 = COPY_FLOAT_TO_VECTOR(*(b + 54));
    src_b55 = COPY_FLOAT_TO_VECTOR(*(b + 55));
    src_b63 = COPY_FLOAT_TO_VECTOR(*(b + 63));

    src_c0 *= src_b0;
    src_c1 -= src_c0 * src_b1;
    src_c2 -= src_c0 * src_b2;
    src_c3 -= src_c0 * src_b3;
    src_c4 -= src_c0 * src_b4;
    src_c5 -= src_c0 * src_b5;
    src_c6 -= src_c0 * src_b6;
    src_c7 -= src_c0 * src_b7;

    src_c1 *= src_b9;
    src_c2 -= src_c1 * src_b10;
    src_c3 -= src_c1 * src_b11;
    src_c4 -= src_c1 * src_b12;
    src_c5 -= src_c1 * src_b13;
    src_c6 -= src_c1 * src_b14;
    src_c7 -= src_c1 * src_b15;

    src_c2 *= src_b18;
    src_c3 -= src_c2 * src_b19;
    src_c4 -= src_c2 * src_b20;
    src_c5 -= src_c2 * src_b21;
    src_c6 -= src_c2 * src_b22;
    src_c7 -= src_c2 * src_b23;

    src_c3 *= src_b27;
    src_c4 -= src_c3 * src_b28;
    src_c5 -= src_c3 * src_b29;
    src_c6 -= src_c3 * src_b30;
    src_c7 -= src_c3 * src_b31;

    src_c4 *= src_b36;
    src_c5 -= src_c4 * src_b37;
    src_c6 -= src_c4 * src_b38;
    src_c7 -= src_c4 * src_b39;

    src_c5 *= src_b45;
    src_c6 -= src_c5 * src_b46;
    src_c7 -= src_c5 * src_b47;

    src_c6 *= src_b54;
    src_c7 -= src_c6 * src_b55;

    src_c7 *= src_b63;

    ST_SP4(src_c0, src_c1, src_c2, src_c3, a, 4);
    ST_SP4(src_c4, src_c5, src_c6, src_c7, a + 16, 4);

    ST_SP(src_c0, c);
    ST_SP(src_c1, c_nxt1line);
    ST_SP(src_c2, c_nxt2line);
    ST_SP(src_c3, c_nxt3line);
    ST_SP(src_c4, c_nxt4line);
    ST_SP(src_c5, c_nxt5line);
    ST_SP(src_c6, c_nxt6line);
    ST_SP(src_c7, c_nxt7line);
}

static void ssolve_4x4_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    v4f32 src_c0, src_c1, src_c2, src_c3,  src_b0, src_b1, src_b2, src_b3;
    v4f32 src_b5, src_b6, src_b7, src_b10, src_b11, src_b15, src_b, src_a0;
    FLOAT *c_nxt1line = c + ldc;
    FLOAT *c_nxt2line = c + 2 * ldc;
    FLOAT *c_nxt3line = c + 3 * ldc;

    src_c0 = LD_SP(c);
    src_c1 = LD_SP(c_nxt1line);
    src_c2 = LD_SP(c_nxt2line);
    src_c3 = LD_SP(c_nxt3line);

    for (k = 0; k < (bk >> 1); k++)
    {
        src_a0 = LD_SP(a);

        src_b = LD_SP(b + 0);
        SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);
        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a0 * src_b1;
        src_c2 -= src_a0 * src_b2;
        src_c3 -= src_a0 * src_b3;

        a += 4;
        b += 4;

        src_a0 = LD_SP(a);

        src_b = LD_SP(b + 0);
        SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);
        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a0 * src_b1;
        src_c2 -= src_a0 * src_b2;
        src_c3 -= src_a0 * src_b3;

        a += 4;
        b += 4;
    }

    if ((bk & 1) && (bk > 0))
    {
        src_a0 = LD_SP(a);

        src_b = LD_SP(b + 0);
        SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);
        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a0 * src_b1;
        src_c2 -= src_a0 * src_b2;
        src_c3 -= src_a0 * src_b3;

        a += 4;
        b += 4;
    }

    src_b = LD_SP(b + 0);
    SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);
    src_b5 = LD_SP(b + 5);
    src_b7 = (v4f32) __msa_splati_w((v4i32) src_b5, 2);
    src_b6 = (v4f32) __msa_splati_w((v4i32) src_b5, 1);
    src_b5 = (v4f32) __msa_splati_w((v4i32) src_b5, 0);
    src_b10 = COPY_FLOAT_TO_VECTOR(*(b + 10));
    src_b11 = COPY_FLOAT_TO_VECTOR(*(b + 11));
    src_b15 = COPY_FLOAT_TO_VECTOR(*(b + 15));

    src_c0 *= src_b0;
    src_c1 -= src_c0 * src_b1;
    src_c2 -= src_c0 * src_b2;
    src_c3 -= src_c0 * src_b3;

    src_c1 *= src_b5;
    src_c2 -= src_c1 * src_b6;
    src_c3 -= src_c1 * src_b7;

    src_c2 *= src_b10;
    src_c3 -= src_c2 * src_b11;

    src_c3 *= src_b15;

    ST_SP4(src_c0, src_c1, src_c2, src_c3, a, 4);

    ST_SP(src_c0, c);
    ST_SP(src_c1, c_nxt1line);
    ST_SP(src_c2, c_nxt2line);
    ST_SP(src_c3, c_nxt3line);
}

static void ssolve_4x2_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    v4f32 src_a, src_c0, src_c1, src_b0, src_b1, src_b3;
    FLOAT *c_nxt1line = c + ldc;

    src_c0 = LD_SP(c);
    src_c1 = LD_SP(c_nxt1line);

    for (k = 0; k < (bk >> 2); k++)
    {
        src_a = LD_SP(a);
        src_b0 = LD_SP(b);
        src_b1 = (v4f32) __msa_splati_w((v4i32) src_b0, 1);
        src_b0 = (v4f32) __msa_splati_w((v4i32) src_b0, 0);

        src_c0 -= src_a * src_b0;
        src_c1 -= src_a * src_b1;

        a += 4;
        b += 2;

        src_a = LD_SP(a);
        src_b0 = LD_SP(b);
        src_b1 = (v4f32) __msa_splati_w((v4i32) src_b0, 1);
        src_b0 = (v4f32) __msa_splati_w((v4i32) src_b0, 0);

        src_c0 -= src_a * src_b0;
        src_c1 -= src_a * src_b1;

        a += 4;
        b += 2;

        src_a = LD_SP(a);
        src_b0 = LD_SP(b);
        src_b1 = (v4f32) __msa_splati_w((v4i32) src_b0, 1);
        src_b0 = (v4f32) __msa_splati_w((v4i32) src_b0, 0);

        src_c0 -= src_a * src_b0;
        src_c1 -= src_a * src_b1;

        a += 4;
        b += 2;

        src_a = LD_SP(a);
        src_b0 = LD_SP(b);
        src_b1 = (v4f32) __msa_splati_w((v4i32) src_b0, 1);
        src_b0 = (v4f32) __msa_splati_w((v4i32) src_b0, 0);

        src_c0 -= src_a * src_b0;
        src_c1 -= src_a * src_b1;

        a += 4;
        b += 2;
    }

    if ((bk & 3) && (bk > 0))
    {
        if (bk & 2)
        {
            src_a = LD_SP(a);
            src_b0 = LD_SP(b);
            src_b1 = (v4f32) __msa_splati_w((v4i32) src_b0, 1);
            src_b0 = (v4f32) __msa_splati_w((v4i32) src_b0, 0);

            src_c0 -= src_a * src_b0;
            src_c1 -= src_a * src_b1;

            a += 4;
            b += 2;

            src_a = LD_SP(a);
            src_b0 = LD_SP(b);
            src_b1 = (v4f32) __msa_splati_w((v4i32) src_b0, 1);
            src_b0 = (v4f32) __msa_splati_w((v4i32) src_b0, 0);

            src_c0 -= src_a * src_b0;
            src_c1 -= src_a * src_b1;

            a += 4;
            b += 2;
        }

        if (bk & 1)
        {
            src_a = LD_SP(a);
            src_b0 = LD_SP(b);
            src_b1 = (v4f32) __msa_splati_w((v4i32) src_b0, 1);
            src_b0 = (v4f32) __msa_splati_w((v4i32) src_b0, 0);

            src_c0 -= src_a * src_b0;
            src_c1 -= src_a * src_b1;

            a += 4;
            b += 2;
        }
    }

    src_b0 = COPY_FLOAT_TO_VECTOR(*(b + 0));
    src_b1 = COPY_FLOAT_TO_VECTOR(*(b + 1));
    src_b3 = COPY_FLOAT_TO_VECTOR(*(b + 3));

    src_c0 *= src_b0;
    src_c1 -= src_c0 * src_b1;
    src_c1 *= src_b3;

    ST_SP2(src_c0, src_c1, a, 4);

    ST_SP(src_c0, c);
    ST_SP(src_c1, c_nxt1line);
}

static void ssolve_4x1_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT b0, c0, c1, c2, c3;

    c0 = *(c + 0);
    c1 = *(c + 1);
    c2 = *(c + 2);
    c3 = *(c + 3);

    for (k = 0; k < bk; k++)
    {
        c0 -= a[0] * b[0];
        c1 -= a[1] * b[0];
        c2 -= a[2] * b[0];
        c3 -= a[3] * b[0];

        a += 4;
        b += 1;
    }

    b0 = *(b + 0);

    c0 *= b0;
    c1 *= b0;
    c2 *= b0;
    c3 *= b0;

    *(a + 0) = c0;
    *(a + 1) = c1;
    *(a + 2) = c2;
    *(a + 3) = c3;

    *(c + 0) = c0;
    *(c + 1) = c1;
    *(c + 2) = c2;
    *(c + 3) = c3;
}

static void ssolve_2x8_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT b0, b1, b2, b3, b4, b5, b6, b7, b9, b10, b11, b12, b13, b14, b15;
    FLOAT b18, b19, b20, b21, b22, b23, b27, b28, b29, b30, b31;
    FLOAT b36, b37, b38, b39, b45, b46, b47, b54, b55, b63;
    FLOAT c0, c1, c0_nxt1, c1_nxt1, c0_nxt2, c1_nxt2, c0_nxt3, c1_nxt3;
    FLOAT c0_nxt4, c1_nxt4, c0_nxt5, c1_nxt5, c0_nxt6, c1_nxt6;
    FLOAT c0_nxt7, c1_nxt7;

    c0 = *(c + 0);
    c1 = *(c + 1);
    c0_nxt1 = *(c + 0 + 1 * ldc);
    c1_nxt1 = *(c + 1 + 1 * ldc);
    c0_nxt2 = *(c + 0 + 2 * ldc);
    c1_nxt2 = *(c + 1 + 2 * ldc);
    c0_nxt3 = *(c + 0 + 3 * ldc);
    c1_nxt3 = *(c + 1 + 3 * ldc);
    c0_nxt4 = *(c + 0 + 4 * ldc);
    c1_nxt4 = *(c + 1 + 4 * ldc);
    c0_nxt5 = *(c + 0 + 5 * ldc);
    c1_nxt5 = *(c + 1 + 5 * ldc);
    c0_nxt6 = *(c + 0 + 6 * ldc);
    c1_nxt6 = *(c + 1 + 6 * ldc);
    c0_nxt7 = *(c + 0 + 7 * ldc);
    c1_nxt7 = *(c + 1 + 7 * ldc);

    for (k = 0; k < bk; k++)
    {
        c0 -= a[0] * b[0];
        c1 -= a[1] * b[0];
        c0_nxt1 -= a[0] * b[1];
        c1_nxt1 -= a[1] * b[1];
        c0_nxt2 -= a[0] * b[2];
        c1_nxt2 -= a[1] * b[2];
        c0_nxt3 -= a[0] * b[3];
        c1_nxt3 -= a[1] * b[3];
        c0_nxt4 -= a[0] * b[4];
        c1_nxt4 -= a[1] * b[4];
        c0_nxt5 -= a[0] * b[5];
        c1_nxt5 -= a[1] * b[5];
        c0_nxt6 -= a[0] * b[6];
        c1_nxt6 -= a[1] * b[6];
        c0_nxt7 -= a[0] * b[7];
        c1_nxt7 -= a[1] * b[7];

        a += 2;
        b += 8;
    }

    b0 = *(b + 0);
    b1 = *(b + 1);
    b2 = *(b + 2);
    b3 = *(b + 3);
    b4 = *(b + 4);
    b5 = *(b + 5);
    b6 = *(b + 6);
    b7 = *(b + 7);
    b9 = *(b + 9);
    b10 = *(b + 10);
    b11 = *(b + 11);
    b12 = *(b + 12);
    b13 = *(b + 13);
    b14 = *(b + 14);
    b15 = *(b + 15);
    b18 = *(b + 18);
    b19 = *(b + 19);
    b20 = *(b + 20);
    b21 = *(b + 21);
    b22 = *(b + 22);
    b23 = *(b + 23);
    b27 = *(b + 27);
    b28 = *(b + 28);
    b29 = *(b + 29);
    b30 = *(b + 30);
    b31 = *(b + 31);
    b36 = *(b + 36);
    b37 = *(b + 37);
    b38 = *(b + 38);
    b39 = *(b + 39);
    b45 = *(b + 45);
    b46 = *(b + 46);
    b47 = *(b + 47);
    b54 = *(b + 54);
    b55 = *(b + 55);
    b63 = *(b + 63);

    c0 *= b0;
    c1 *= b0;

    c0_nxt1 -= c0 * b1;
    c1_nxt1 -= c1 * b1;

    c0_nxt2 -= c0 * b2;
    c1_nxt2 -= c1 * b2;

    c0_nxt3 -= c0 * b3;
    c1_nxt3 -= c1 * b3;

    c0_nxt4 -= c0 * b4;
    c1_nxt4 -= c1 * b4;

    c0_nxt5 -= c0 * b5;
    c1_nxt5 -= c1 * b5;

    c0_nxt6 -= c0 * b6;
    c1_nxt6 -= c1 * b6;

    c0_nxt7 -= c0 * b7;
    c1_nxt7 -= c1 * b7;

    c0_nxt1 *= b9;
    c1_nxt1 *= b9;

    c0_nxt2 -= c0_nxt1 * b10;
    c1_nxt2 -= c1_nxt1 * b10;

    c0_nxt3 -= c0_nxt1 * b11;
    c1_nxt3 -= c1_nxt1 * b11;

    c0_nxt4 -= c0_nxt1 * b12;
    c1_nxt4 -= c1_nxt1 * b12;

    c0_nxt5 -= c0_nxt1 * b13;
    c1_nxt5 -= c1_nxt1 * b13;

    c0_nxt6 -= c0_nxt1 * b14;
    c1_nxt6 -= c1_nxt1 * b14;

    c0_nxt7 -= c0_nxt1 * b15;
    c1_nxt7 -= c1_nxt1 * b15;

    c0_nxt2 *= b18;
    c1_nxt2 *= b18;

    c0_nxt3 -= c0_nxt2 * b19;
    c1_nxt3 -= c1_nxt2 * b19;

    c0_nxt4 -= c0_nxt2 * b20;
    c1_nxt4 -= c1_nxt2 * b20;

    c0_nxt5 -= c0_nxt2 * b21;
    c1_nxt5 -= c1_nxt2 * b21;

    c0_nxt6 -= c0_nxt2 * b22;
    c1_nxt6 -= c1_nxt2 * b22;

    c0_nxt7 -= c0_nxt2 * b23;
    c1_nxt7 -= c1_nxt2 * b23;

    c0_nxt3 *= b27;
    c1_nxt3 *= b27;

    c0_nxt4 -= c0_nxt3 * b28;
    c1_nxt4 -= c1_nxt3 * b28;

    c0_nxt5 -= c0_nxt3 * b29;
    c1_nxt5 -= c1_nxt3 * b29;

    c0_nxt6 -= c0_nxt3 * b30;
    c1_nxt6 -= c1_nxt3 * b30;

    c0_nxt7 -= c0_nxt3 * b31;
    c1_nxt7 -= c1_nxt3 * b31;

    c0_nxt4 *= b36;
    c1_nxt4 *= b36;

    c0_nxt5 -= c0_nxt4 * b37;
    c1_nxt5 -= c1_nxt4 * b37;

    c0_nxt6 -= c0_nxt4 * b38;
    c1_nxt6 -= c1_nxt4 * b38;

    c0_nxt7 -= c0_nxt4 * b39;
    c1_nxt7 -= c1_nxt4 * b39;

    c0_nxt5 *= b45;
    c1_nxt5 *= b45;

    c0_nxt6 -= c0_nxt5 * b46;
    c1_nxt6 -= c1_nxt5 * b46;

    c0_nxt7 -= c0_nxt5 * b47;
    c1_nxt7 -= c1_nxt5 * b47;

    c0_nxt6 *= b54;
    c1_nxt6 *= b54;

    c0_nxt7 -= c0_nxt6 * b55;
    c1_nxt7 -= c1_nxt6 * b55;

    c0_nxt7 *= b63;
    c1_nxt7 *= b63;

    *(a + 0) = c0;
    *(a + 1) = c1;
    *(a + 2) = c0_nxt1;
    *(a + 3) = c1_nxt1;
    *(a + 4) = c0_nxt2;
    *(a + 5) = c1_nxt2;
    *(a + 6) = c0_nxt3;
    *(a + 7) = c1_nxt3;
    *(a + 8) = c0_nxt4;
    *(a + 9) = c1_nxt4;
    *(a + 10) = c0_nxt5;
    *(a + 11) = c1_nxt5;
    *(a + 12) = c0_nxt6;
    *(a + 13) = c1_nxt6;
    *(a + 14) = c0_nxt7;
    *(a + 15) = c1_nxt7;

    *(c + 0) = c0;
    *(c + 1) = c1;
    *(c + 0 + 1 * ldc) = c0_nxt1;
    *(c + 1 + 1 * ldc) = c1_nxt1;
    *(c + 0 + 2 * ldc) = c0_nxt2;
    *(c + 1 + 2 * ldc) = c1_nxt2;
    *(c + 0 + 3 * ldc) = c0_nxt3;
    *(c + 1 + 3 * ldc) = c1_nxt3;
    *(c + 0 + 4 * ldc) = c0_nxt4;
    *(c + 1 + 4 * ldc) = c1_nxt4;
    *(c + 0 + 5 * ldc) = c0_nxt5;
    *(c + 1 + 5 * ldc) = c1_nxt5;
    *(c + 0 + 6 * ldc) = c0_nxt6;
    *(c + 1 + 6 * ldc) = c1_nxt6;
    *(c + 0 + 7 * ldc) = c0_nxt7;
    *(c + 1 + 7 * ldc) = c1_nxt7;
}

static void ssolve_2x4_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT b0, b1, b2, b3, b5, b6, b7, b10, b11, b15, c0, c1;
    FLOAT c0_nxt1, c0_nxt2, c0_nxt3, c1_nxt1, c1_nxt2, c1_nxt3;

    c0 = *(c + 0);
    c1 = *(c + 1);
    c0_nxt1 = *(c + 0 + 1 * ldc);
    c1_nxt1 = *(c + 1 + 1 * ldc);
    c0_nxt2 = *(c + 0 + 2 * ldc);
    c1_nxt2 = *(c + 1 + 2 * ldc);
    c0_nxt3 = *(c + 0 + 3 * ldc);
    c1_nxt3 = *(c + 1 + 3 * ldc);

    for (k = 0; k < bk; k++)
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

static void ssolve_2x2_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT b0, b1, b3, c0, c0_nxt, c1, c1_nxt;

    c0 = *(c + 0);
    c1 = *(c + 1);
    c0_nxt = *(c + 0 + ldc);
    c1_nxt = *(c + 1 + ldc);

    for (k = 0; k < bk; k++)
    {
        c0 -= a[0] * b[0];
        c1 -= a[1] * b[0];
        c0_nxt -= a[0] * b[1];
        c1_nxt -= a[1] * b[1];

        a += 2;
        b += 2;
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
    *(c + ldc) = c0_nxt;
    *(c + 1 + ldc) = c1_nxt;
}

static void ssolve_2x1_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT b0, c0, c1;

    c0 = *(c + 0);
    c1 = *(c + 1);

    for (k = 0; k < bk; k++)
    {
        c0 -= a[0] * b[0];
        c1 -= a[1] * b[0];

        a += 2;
        b += 1;
    }

    b0 = *(b + 0);

    c0 *= b0;
    c1 *= b0;

    *(a + 0) = c0;
    *(a + 1) = c1;

    *(c + 0) = c0;
    *(c + 1) = c1;
}

static void ssolve_1x8_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT b0, b1, b2, b3, b4, b5, b6, b7, b9, b10, b11, b12, b13, b14, b15;
    FLOAT b18, b19, b20, b21, b22, b23, b27, b28, b29, b30, b31, b36, b37, b38;
    FLOAT b39, b45, b46, b47, b54, b55, b63, c0, c1, c2, c3, c4, c5, c6, c7;

    c0 = *(c + 0);
    c1 = *(c + 1 * ldc);
    c2 = *(c + 2 * ldc);
    c3 = *(c + 3 * ldc);
    c4 = *(c + 4 * ldc);
    c5 = *(c + 5 * ldc);
    c6 = *(c + 6 * ldc);
    c7 = *(c + 7 * ldc);

    for (k = 0; k < bk; k++)
    {
        c0 -= a[0] * b[0];
        c1 -= a[0] * b[1];
        c2 -= a[0] * b[2];
        c3 -= a[0] * b[3];
        c4 -= a[0] * b[4];
        c5 -= a[0] * b[5];
        c6 -= a[0] * b[6];
        c7 -= a[0] * b[7];

        a += 1;
        b += 8;
    }

    b0 = *(b + 0);
    b1 = *(b + 1);
    b2 = *(b + 2);
    b3 = *(b + 3);
    b4 = *(b + 4);
    b5 = *(b + 5);
    b6 = *(b + 6);
    b7 = *(b + 7);
    b9 = *(b + 9);
    b10 = *(b + 10);
    b11 = *(b + 11);
    b12 = *(b + 12);
    b13 = *(b + 13);
    b14 = *(b + 14);
    b15 = *(b + 15);
    b18 = *(b + 18);
    b19 = *(b + 19);
    b20 = *(b + 20);
    b21 = *(b + 21);
    b22 = *(b + 22);
    b23 = *(b + 23);
    b27 = *(b + 27);
    b28 = *(b + 28);
    b29 = *(b + 29);
    b30 = *(b + 30);
    b31 = *(b + 31);
    b36 = *(b + 36);
    b37 = *(b + 37);
    b38 = *(b + 38);
    b39 = *(b + 39);
    b45 = *(b + 45);
    b46 = *(b + 46);
    b47 = *(b + 47);
    b54 = *(b + 54);
    b55 = *(b + 55);
    b63 = *(b + 63);

    c0 *= b0;

    c1 -= c0 * b1;
    c1 *= b9;

    c2 -= c0 * b2;
    c2 -= c1 * b10;
    c2 *= b18;

    c3 -= c0 * b3;
    c3 -= c1 * b11;
    c3 -= c2 * b19;
    c3 *= b27;

    c4 -= c0 * b4;
    c4 -= c1 * b12;
    c4 -= c2 * b20;
    c4 -= c3 * b28;
    c4 *= b36;

    c5 -= c0 * b5;
    c5 -= c1 * b13;
    c5 -= c2 * b21;
    c5 -= c3 * b29;
    c5 -= c4 * b37;
    c5 *= b45;

    c6 -= c0 * b6;
    c6 -= c1 * b14;
    c6 -= c2 * b22;
    c6 -= c3 * b30;
    c6 -= c4 * b38;
    c6 -= c5 * b46;
    c6 *= b54;

    c7 -= c0 * b7;
    c7 -= c1 * b15;
    c7 -= c2 * b23;
    c7 -= c3 * b31;
    c7 -= c4 * b39;
    c7 -= c5 * b47;
    c7 -= c6 * b55;
    c7 *= b63;

    *(a + 0) = c0;
    *(a + 1) = c1;
    *(a + 2) = c2;
    *(a + 3) = c3;
    *(a + 4) = c4;
    *(a + 5) = c5;
    *(a + 6) = c6;
    *(a + 7) = c7;

    *(c + 0) = c0;
    *(c + 1 * ldc) = c1;
    *(c + 2 * ldc) = c2;
    *(c + 3 * ldc) = c3;
    *(c + 4 * ldc) = c4;
    *(c + 5 * ldc) = c5;
    *(c + 6 * ldc) = c6;
    *(c + 7 * ldc) = c7;
}

static void ssolve_1x4_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT b0, b1, b2, b3, b5, b6, b7, b10, b11, b15, c0, c1, c2, c3;

    c0 = *(c + 0);
    c1 = *(c + 1 * ldc);
    c2 = *(c + 2 * ldc);
    c3 = *(c + 3 * ldc);

    for (k = 0; k < bk; k++)
    {
        c0 -= a[0] * b[0];
        c1 -= a[0] * b[1];
        c2 -= a[0] * b[2];
        c3 -= a[0] * b[3];

        a += 1;
        b += 4;
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

static void ssolve_1x2_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT b0, b1, b3, c0, c1;

    c0 = *c;
    c1 = *(c + ldc);

    for (k = 0; k < bk; k++)
    {
        c0 -= a[0] * b[0];
        c1 -= a[0] * b[1];

        a += 1;
        b += 2;
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

static void ssolve_1x1_rn_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG bk)
{
    BLASLONG k;

    for (k = 0; k < bk; k++)
    {
        *c -= a[0] * b[0];

        a++;
        b++;
    }

    *c *= *b;
    *a = *c;
}

int CNAME(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT dummy1, FLOAT *a, FLOAT *b,
          FLOAT *c, BLASLONG ldc, BLASLONG offset)
{
    FLOAT *aa, *cc;
    BLASLONG i, j, kk;

    kk = -offset;

    for (j = (n >> 3); j--;)
    {
        aa = a;
        cc = c;

        for (i = (m >> 3); i--;)
        {
            ssolve_8x8_rn_msa(aa, b, cc, ldc, kk);

            aa += 8 * k;
            cc += 8;
        }

        if (m & 7)
        {
            if (m & 4)
            {
                ssolve_4x8_rn_msa(aa, b, cc, ldc, kk);

                aa += 4 * k;
                cc += 4;
            }

            if (m & 2)
            {
                ssolve_2x8_rn_msa(aa, b, cc, ldc, kk);

                aa += 2 * k;
                cc += 2;
            }

            if (m & 1)
            {
                ssolve_1x8_rn_msa(aa, b, cc, ldc, kk);

                aa += k;
                cc += 1;
            }
        }

        kk += 8;
        b += 8 * k;
        c += 8 * ldc;
    }

    if (n & 7)
    {
        if (n & 4)
        {
            aa = a;
            cc = c;

            for (i = (m >> 3); i--;)
            {
                ssolve_8x4_rn_msa(aa, b, cc, ldc, kk);

                aa += 8 * k;
                cc += 8;
            }

            if (m & 7)
            {
                if (m & 4)
                {
                    ssolve_4x4_rn_msa(aa, b, cc, ldc, kk);

                    aa += 4 * k;
                    cc += 4;
                }

                if (m & 2)
                {
                    ssolve_2x4_rn_msa(aa, b, cc, ldc, kk);

                    aa += 2 * k;
                    cc += 2;
                }

                if (m & 1)
                {
                    ssolve_1x4_rn_msa(aa, b, cc, ldc, kk);

                    aa += k;
                    cc += 1;
                }
            }

            b += 4 * k;
            c += 4 * ldc;
            kk += 4;
        }

        if (n & 2)
        {
            aa = a;
            cc = c;

            for (i = (m >> 3); i--;)
            {
                ssolve_8x2_rn_msa(aa, b, cc, ldc, kk);

                aa += 8 * k;
                cc += 8;
            }

            if (m & 7)
            {
                if (m & 4)
                {
                    ssolve_4x2_rn_msa(aa, b, cc, ldc, kk);

                    aa += 4 * k;
                    cc += 4;
                }

                if (m & 2)
                {
                    ssolve_2x2_rn_msa(aa, b, cc, ldc, kk);

                    aa += 2 * k;
                    cc += 2;
                }

                if (m & 1)
                {
                    ssolve_1x2_rn_msa(aa, b, cc, ldc, kk);

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
                ssolve_8x1_rn_msa(aa, b, cc, ldc, kk);

                aa += 8 * k;
                cc += 8;
            }

            if (m & 7)
            {
                if (m & 4)
                {
                    ssolve_4x1_rn_msa(aa, b, cc, ldc, kk);

                    aa += 4 * k;
                    cc += 4;
                }

                if (m & 2)
                {
                    ssolve_2x1_rn_msa(aa, b, cc, ldc, kk);

                    aa += 2 * k;
                    cc += 2;
                }

                if (m & 1)
                {
                    ssolve_1x1_rn_msa(aa, b, cc, kk);

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
