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

static void ssolve_8x8_rt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    v4f32 src_c0, src_c1, src_c2, src_c3, src_c4, src_c5, src_c6, src_c7;
    v4f32 src_c8, src_c9, src_c10, src_c11, src_c12, src_c13, src_c14, src_c15;
    v4f32 src_b, src_b0, src_b8, src_b9, src_b16, src_b17, src_b18, src_b24;
    v4f32 src_b25, src_b26, src_b27, src_b32, src_b33, src_b34, src_b35;
    v4f32 src_b36, src_b40, src_b41, src_b42, src_b43, src_b44, src_b45;
    v4f32 src_b48, src_b49, src_b50, src_b51, src_b52, src_b53, src_b54;
    v4f32 src_b56, src_b57, src_b58, src_b59, src_b60, src_b61, src_b62, src_b63;
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
        FLOAT *aa = a, *bb = b, *pa0_pref;
        v4f32 src_a0, src_a1, src_b1, src_b2, src_b3, src_bb0, src_bb1;

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

            LD_SP2_INC(aa, 4, src_a0, src_a1);
            LD_SP2_INC(bb, 4, src_bb0, src_bb1);

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

            LD_SP2_INC(aa, 4, src_a0, src_a1);
            LD_SP2_INC(bb, 4, src_bb0, src_bb1);

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
            LD_SP2_INC(aa, 4, src_a0, src_a1);
            LD_SP2_INC(bb, 4, src_bb0, src_bb1);

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

    b -= 64;

    src_b = LD_SP(b + 60);
    SPLATI_W4_SP(src_b, src_b60, src_b61, src_b62, src_b63);
    src_b = LD_SP(b + 56);
    SPLATI_W4_SP(src_b, src_b56, src_b57, src_b58, src_b59);

    src_c15 *= src_b63;
    src_c14 *= src_b63;
    src_c13 -= src_c15 * src_b62;
    src_c12 -= src_c14 * src_b62;
    src_c11 -= src_c15 * src_b61;
    src_c10 -= src_c14 * src_b61;
    src_c9 -= src_c15 * src_b60;
    src_c8 -= src_c14 * src_b60;
    src_c7 -= src_c15 * src_b59;
    src_c6 -= src_c14 * src_b59;
    src_c5 -= src_c15 * src_b58;
    src_c4 -= src_c14 * src_b58;
    src_c3 -= src_c15 * src_b57;
    src_c2 -= src_c14 * src_b57;
    src_c1 -= src_c15 * src_b56;
    src_c0 -= src_c14 * src_b56;

    src_b = LD_SP(b + 48);
    SPLATI_W4_SP(src_b, src_b48, src_b49, src_b50, src_b51);
    src_b52 = LD_SP(b + 52);
    src_b54 = (v4f32) __msa_splati_w((v4i32) src_b52, 2);
    src_b53 = (v4f32) __msa_splati_w((v4i32) src_b52, 1);
    src_b52 = (v4f32) __msa_splati_w((v4i32) src_b52, 0);

    src_c12 *= src_b54;
    src_c13 *= src_b54;
    src_c10 -= src_c12 * src_b53;
    src_c11 -= src_c13 * src_b53;
    src_c8 -= src_c12 * src_b52;
    src_c9 -= src_c13 * src_b52;
    src_c6 -= src_c12 * src_b51;
    src_c7 -= src_c13 * src_b51;
    src_c4 -= src_c12 * src_b50;
    src_c5 -= src_c13 * src_b50;
    src_c2 -= src_c12 * src_b49;
    src_c3 -= src_c13 * src_b49;
    src_c0 -= src_c12 * src_b48;
    src_c1 -= src_c13 * src_b48;

    ST_SP4(src_c12, src_c13, src_c14, src_c15, a - 16, 4);
    ST_SP2(src_c12, src_c13, c_nxt6line, 4);
    ST_SP2(src_c14, src_c15, c_nxt7line, 4);

    src_b = LD_SP(b + 40);
    SPLATI_W4_SP(src_b, src_b40, src_b41, src_b42, src_b43);
    src_b44 = LD_SP(b + 44);
    src_b45 = (v4f32) __msa_splati_w((v4i32) src_b44, 1);
    src_b44 = (v4f32) __msa_splati_w((v4i32) src_b44, 0);

    src_c10 *= src_b45;
    src_c11 *= src_b45;
    src_c8 -= src_c10 * src_b44;
    src_c9 -= src_c11 * src_b44;
    src_c6 -= src_c10 * src_b43;
    src_c7 -= src_c11 * src_b43;
    src_c4 -= src_c10 * src_b42;
    src_c5 -= src_c11 * src_b42;
    src_c2 -= src_c10 * src_b41;
    src_c3 -= src_c11 * src_b41;
    src_c0 -= src_c10 * src_b40;
    src_c1 -= src_c11 * src_b40;

    src_b = LD_SP(b + 32);
    SPLATI_W4_SP(src_b, src_b32, src_b33, src_b34, src_b35);
    src_b36 = COPY_FLOAT_TO_VECTOR(*(b + 36));

    src_c8 *= src_b36;
    src_c9 *= src_b36;
    src_c6 -= src_c8 * src_b35;
    src_c7 -= src_c9 * src_b35;
    src_c4 -= src_c8 * src_b34;
    src_c5 -= src_c9 * src_b34;
    src_c2 -= src_c8 * src_b33;
    src_c3 -= src_c9 * src_b33;
    src_c0 -= src_c8 * src_b32;
    src_c1 -= src_c9 * src_b32;

    ST_SP4(src_c8, src_c9, src_c10, src_c11, a - 32, 4);
    ST_SP2(src_c8, src_c9, c_nxt4line, 4);
    ST_SP2(src_c10, src_c11, c_nxt5line, 4);

    src_b = LD_SP(b + 24);
    SPLATI_W4_SP(src_b, src_b24, src_b25, src_b26, src_b27);

    src_c6 *= src_b27;
    src_c7 *= src_b27;
    src_c4 -= src_c6 * src_b26;
    src_c5 -= src_c7 * src_b26;
    src_c2 -= src_c6 * src_b25;
    src_c3 -= src_c7 * src_b25;
    src_c0 -= src_c6 * src_b24;
    src_c1 -= src_c7 * src_b24;

    src_b16 = LD_SP(b + 16);
    src_b18 = (v4f32) __msa_splati_w((v4i32) src_b16, 2);
    src_b17 = (v4f32) __msa_splati_w((v4i32) src_b16, 1);
    src_b16 = (v4f32) __msa_splati_w((v4i32) src_b16, 0);

    src_c4 *= src_b18;
    src_c5 *= src_b18;
    src_c2 -= src_c4 * src_b17;
    src_c3 -= src_c5 * src_b17;
    src_c0 -= src_c4 * src_b16;
    src_c1 -= src_c5 * src_b16;

    ST_SP4(src_c4, src_c5, src_c6, src_c7, a - 48, 4);
    ST_SP2(src_c4, src_c5, c_nxt2line, 4);
    ST_SP2(src_c6, src_c7, c_nxt3line, 4);

    src_b9 = COPY_FLOAT_TO_VECTOR(*(b + 9));
    src_b8 = COPY_FLOAT_TO_VECTOR(*(b + 8));
    src_b0 = COPY_FLOAT_TO_VECTOR(*(b + 0));

    src_c2 *= src_b9;
    src_c3 *= src_b9;
    src_c0 -= src_c2 * src_b8;
    src_c1 -= src_c3 * src_b8;

    src_c0 *= src_b0;
    src_c1 *= src_b0;

    ST_SP4(src_c0, src_c1, src_c2, src_c3, a - 64, 4);

    ST_SP2(src_c0, src_c1, c, 4);
    ST_SP2(src_c2, src_c3, c_nxt1line, 4);
}

static void ssolve_8x4_rt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    v4f32 src_a0, src_a1, src_b1, src_b2, src_b3;
    v4f32 src_c0, src_c1, src_c2, src_c3, src_c4, src_c5, src_c6, src_c7;
    v4f32 src_b, src_b0, src_b4, src_b5, src_b8, src_b9, src_b10, src_b12;
    v4f32 src_b13, src_b14, src_b15;
    FLOAT *c_nxt1line = c + ldc;
    FLOAT *c_nxt2line = c + 2 * ldc;
    FLOAT *c_nxt3line = c + 3 * ldc;

    LD_SP2(c, 4, src_c0, src_c1);
    LD_SP2(c_nxt1line, 4, src_c2, src_c3);
    LD_SP2(c_nxt2line, 4, src_c4, src_c5);
    LD_SP2(c_nxt3line, 4, src_c6, src_c7);

    for (k = 0; k < (bk >> 1); k++)
    {
        LD_SP2(aa, 4, src_a0, src_a1);

        src_b = LD_SP(bb + 0);
        SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);
        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a1 * src_b0;
        src_c2 -= src_a0 * src_b1;
        src_c3 -= src_a1 * src_b1;
        src_c4 -= src_a0 * src_b2;
        src_c5 -= src_a1 * src_b2;
        src_c6 -= src_a0 * src_b3;
        src_c7 -= src_a1 * src_b3;

        aa += 8;
        bb += 4;

        LD_SP2(aa, 4, src_a0, src_a1);

        src_b = LD_SP(bb + 0);
        SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);
        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a1 * src_b0;
        src_c2 -= src_a0 * src_b1;
        src_c3 -= src_a1 * src_b1;
        src_c4 -= src_a0 * src_b2;
        src_c5 -= src_a1 * src_b2;
        src_c6 -= src_a0 * src_b3;
        src_c7 -= src_a1 * src_b3;

        aa += 8;
        bb += 4;
    }

    if ((bk & 1) && (bk > 0))
    {
        LD_SP2(aa, 4, src_a0, src_a1);

        src_b = LD_SP(bb + 0);
        SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);
        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a1 * src_b0;
        src_c2 -= src_a0 * src_b1;
        src_c3 -= src_a1 * src_b1;
        src_c4 -= src_a0 * src_b2;
        src_c5 -= src_a1 * src_b2;
        src_c6 -= src_a0 * src_b3;
        src_c7 -= src_a1 * src_b3;
    }

    a -= 32;
    b -= 16;

    src_b = LD_SP(b + 12);
    SPLATI_W4_SP(src_b, src_b12, src_b13, src_b14, src_b15);
    src_b8 = LD_SP(b + 8);
    src_b10 = (v4f32) __msa_splati_w((v4i32) src_b8, 2);
    src_b9 = (v4f32) __msa_splati_w((v4i32) src_b8, 1);
    src_b8 = (v4f32) __msa_splati_w((v4i32) src_b8, 0);
    src_b5 = COPY_FLOAT_TO_VECTOR(*(b + 5));
    src_b4 = COPY_FLOAT_TO_VECTOR(*(b + 4));
    src_b0 = COPY_FLOAT_TO_VECTOR(*(b + 0));

    src_c7 *= src_b15;
    src_c6 *= src_b15;
    src_c5 -= src_c7 * src_b14;
    src_c4 -= src_c6 * src_b14;
    src_c3 -= src_c7 * src_b13;
    src_c2 -= src_c6 * src_b13;
    src_c1 -= src_c7 * src_b12;
    src_c0 -= src_c6 * src_b12;

    src_c5 *= src_b10;
    src_c4 *= src_b10;
    src_c3 -= src_c5 * src_b9;
    src_c2 -= src_c4 * src_b9;
    src_c1 -= src_c5 * src_b8;
    src_c0 -= src_c4 * src_b8;

    src_c3 *= src_b5;
    src_c2 *= src_b5;
    src_c1 -= src_c3 * src_b4;
    src_c0 -= src_c2 * src_b4;

    src_c1 *= src_b0;
    src_c0 *= src_b0;

    ST_SP4(src_c0, src_c1, src_c2, src_c3, a, 4);
    ST_SP4(src_c4, src_c5, src_c6, src_c7, a + 16, 4);

    ST_SP2(src_c0, src_c1, c, 4);
    ST_SP2(src_c2, src_c3, c_nxt1line, 4);
    ST_SP2(src_c4, src_c5, c_nxt2line, 4);
    ST_SP2(src_c6, src_c7, c_nxt3line, 4);
}

static void ssolve_8x2_rt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    v4f32 src_a0, src_a1, src_b1;
    v4f32 src_c0, src_c1, src_c2, src_c3, src_b0, src_b2, src_b3;
    FLOAT *c_nxt1line = c + ldc;

    LD_SP2(c, 4, src_c0, src_c1);
    LD_SP2(c_nxt1line, 4, src_c2, src_c3);

    for (k = 0; k < (bk >> 1); k++)
    {
        LD_SP2(aa, 4, src_a0, src_a1);

        src_b0 = COPY_FLOAT_TO_VECTOR(*(bb + 0));
        src_b1 = COPY_FLOAT_TO_VECTOR(*(bb + 1));

        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a1 * src_b0;
        src_c2 -= src_a0 * src_b1;
        src_c3 -= src_a1 * src_b1;

        aa += 8;
        bb += 2;

        LD_SP2(aa, 4, src_a0, src_a1);

        src_b0 = COPY_FLOAT_TO_VECTOR(*(bb + 0));
        src_b1 = COPY_FLOAT_TO_VECTOR(*(bb + 1));

        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a1 * src_b0;
        src_c2 -= src_a0 * src_b1;
        src_c3 -= src_a1 * src_b1;

        aa += 8;
        bb += 2;
    }

    if ((bk & 1) && (bk > 0))
    {
        LD_SP2(aa, 4, src_a0, src_a1);

        src_b0 = COPY_FLOAT_TO_VECTOR(*(bb + 0));
        src_b1 = COPY_FLOAT_TO_VECTOR(*(bb + 1));

        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a1 * src_b0;
        src_c2 -= src_a0 * src_b1;
        src_c3 -= src_a1 * src_b1;
    }

    a -= 16;
    b -= 4;

    src_b0 = COPY_FLOAT_TO_VECTOR(*(b + 0));
    src_b2 = COPY_FLOAT_TO_VECTOR(*(b + 2));
    src_b3 = COPY_FLOAT_TO_VECTOR(*(b + 3));

    src_c2 *= src_b3;
    src_c3 *= src_b3;
    src_c0 -= src_c2 * src_b2;
    src_c1 -= src_c3 * src_b2;
    src_c0 *= src_b0;
    src_c1 *= src_b0;

    ST_SP4(src_c0, src_c1, src_c2, src_c3, a, 4);
    ST_SP2(src_c0, src_c1, c, 4);
    ST_SP2(src_c2, src_c3, c_nxt1line, 4);
}

static void ssolve_8x1_rt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    v4f32 src_a0, src_a1, src_c0, src_c1, src_b0;

    LD_SP2(c, 4, src_c0, src_c1);

    for (k = 0; k < (bk >> 2); k++)
    {
        LD_SP2(aa, 4, src_a0, src_a1);

        src_b0 = COPY_FLOAT_TO_VECTOR(*(bb + 0));

        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a1 * src_b0;

        aa += 8;
        bb += 1;

        LD_SP2(aa, 4, src_a0, src_a1);

        src_b0 = COPY_FLOAT_TO_VECTOR(*(bb + 0));

        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a1 * src_b0;

        aa += 8;
        bb += 1;

        LD_SP2(aa, 4, src_a0, src_a1);

        src_b0 = COPY_FLOAT_TO_VECTOR(*(bb + 0));

        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a1 * src_b0;

        aa += 8;
        bb += 1;

        LD_SP2(aa, 4, src_a0, src_a1);

        src_b0 = COPY_FLOAT_TO_VECTOR(*(bb + 0));

        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a1 * src_b0;

        aa += 8;
        bb += 1;
    }

    if ((bk & 3) && (bk > 0))
    {
        if (bk & 2)
        {
            LD_SP2(aa, 4, src_a0, src_a1);

            src_b0 = COPY_FLOAT_TO_VECTOR(*(bb + 0));

            src_c0 -= src_a0 * src_b0;
            src_c1 -= src_a1 * src_b0;

            aa += 8;
            bb += 1;

            LD_SP2(aa, 4, src_a0, src_a1);

            src_b0 = COPY_FLOAT_TO_VECTOR(*(bb + 0));

            src_c0 -= src_a0 * src_b0;
            src_c1 -= src_a1 * src_b0;

            aa += 8;
            bb += 1;
        }

        if (bk & 1)
        {
            LD_SP2(aa, 4, src_a0, src_a1);

            src_b0 = COPY_FLOAT_TO_VECTOR(*(bb + 0));

            src_c0 -= src_a0 * src_b0;
            src_c1 -= src_a1 * src_b0;
        }
    }

    a -= 8;
    b -= 1;

    src_b0 = COPY_FLOAT_TO_VECTOR(*(b + 0));

    src_c0 *= src_b0;
    src_c1 *= src_b0;

    ST_SP2(src_c0, src_c1, a, 4);
    ST_SP2(src_c0, src_c1, c, 4);
}

static void ssolve_4x8_rt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    v4f32 src_a0, src_b1, src_b2, src_b3;
    v4f32 src_c0, src_c1, src_c2, src_c3, src_c4, src_c5, src_c6, src_c7;
    v4f32 src_b, src_b0, src_b8, src_b9, src_b16, src_b17, src_b18, src_b24;
    v4f32 src_b25, src_b26, src_b27, src_b32, src_b33, src_b34, src_b35;
    v4f32 src_b36, src_b40, src_b41, src_b42, src_b43, src_b44, src_b45;
    v4f32 src_b48, src_b49, src_b50, src_b51, src_b52, src_b53, src_b54;
    v4f32 src_b56, src_b57, src_b58, src_b59, src_b60, src_b61, src_b62, src_b63;
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
        src_a0 = LD_SP(aa);

        src_b = LD_SP(bb + 0);
        SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);
        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a0 * src_b1;
        src_c2 -= src_a0 * src_b2;
        src_c3 -= src_a0 * src_b3;

        src_b = LD_SP(bb + 4);
        SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);
        src_c4 -= src_a0 * src_b0;
        src_c5 -= src_a0 * src_b1;
        src_c6 -= src_a0 * src_b2;
        src_c7 -= src_a0 * src_b3;

        aa += 4;
        bb += 8;
    }

    a -= 32;
    b -= 64;

    src_b = LD_SP(b + 60);
    SPLATI_W4_SP(src_b, src_b60, src_b61, src_b62, src_b63);
    src_b = LD_SP(b + 56);
    SPLATI_W4_SP(src_b, src_b56, src_b57, src_b58, src_b59);

    src_b = LD_SP(b + 48);
    SPLATI_W4_SP(src_b, src_b48, src_b49, src_b50, src_b51);
    src_b52 = LD_SP(b + 52);
    src_b54 = (v4f32) __msa_splati_w((v4i32) src_b52, 2);
    src_b53 = (v4f32) __msa_splati_w((v4i32) src_b52, 1);
    src_b52 = (v4f32) __msa_splati_w((v4i32) src_b52, 0);

    src_b = LD_SP(b + 40);
    SPLATI_W4_SP(src_b, src_b40, src_b41, src_b42, src_b43);
    src_b44 = LD_SP(b + 44);
    src_b45 = (v4f32) __msa_splati_w((v4i32) src_b44, 1);
    src_b44 = (v4f32) __msa_splati_w((v4i32) src_b44, 0);

    src_b = LD_SP(b + 32);
    SPLATI_W4_SP(src_b, src_b32, src_b33, src_b34, src_b35);
    src_b36 = COPY_FLOAT_TO_VECTOR(*(b + 36));

    src_b = LD_SP(b + 24);
    SPLATI_W4_SP(src_b, src_b24, src_b25, src_b26, src_b27);

    src_b16 = LD_SP(b + 16);
    src_b18 = (v4f32) __msa_splati_w((v4i32) src_b16, 2);
    src_b17 = (v4f32) __msa_splati_w((v4i32) src_b16, 1);
    src_b16 = (v4f32) __msa_splati_w((v4i32) src_b16, 0);

    src_b9 = COPY_FLOAT_TO_VECTOR(*(b + 9));
    src_b8 = COPY_FLOAT_TO_VECTOR(*(b + 8));
    src_b0 = COPY_FLOAT_TO_VECTOR(*(b + 0));

    src_c7 *= src_b63;
    src_c6 -= src_c7 * src_b62;
    src_c5 -= src_c7 * src_b61;
    src_c4 -= src_c7 * src_b60;
    src_c3 -= src_c7 * src_b59;
    src_c2 -= src_c7 * src_b58;
    src_c1 -= src_c7 * src_b57;
    src_c0 -= src_c7 * src_b56;

    src_c6 *= src_b54;
    src_c5 -= src_c6 * src_b53;
    src_c4 -= src_c6 * src_b52;
    src_c3 -= src_c6 * src_b51;
    src_c2 -= src_c6 * src_b50;
    src_c1 -= src_c6 * src_b49;
    src_c0 -= src_c6 * src_b48;

    src_c5 *= src_b45;
    src_c4 -= src_c5 * src_b44;
    src_c3 -= src_c5 * src_b43;
    src_c2 -= src_c5 * src_b42;
    src_c1 -= src_c5 * src_b41;
    src_c0 -= src_c5 * src_b40;

    src_c4 *= src_b36;
    src_c3 -= src_c4 * src_b35;
    src_c2 -= src_c4 * src_b34;
    src_c1 -= src_c4 * src_b33;
    src_c0 -= src_c4 * src_b32;

    src_c3 *= src_b27;
    src_c2 -= src_c3 * src_b26;
    src_c1 -= src_c3 * src_b25;
    src_c0 -= src_c3 * src_b24;

    src_c2 *= src_b18;
    src_c1 -= src_c2 * src_b17;
    src_c0 -= src_c2 * src_b16;

    src_c1 *= src_b9;
    src_c0 -= src_c1 * src_b8;

    src_c0 *= src_b0;

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

static void ssolve_4x4_rt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    v4f32 src_c0, src_c1, src_c2, src_c3, src_b;
    v4f32 src_b0, src_b4, src_b5, src_b8, src_b9, src_b10, src_b12, src_b13;
    v4f32 src_b14, src_b15, src_a, src_b1, src_b2, src_b3;
    FLOAT *c_nxt1line = c + ldc;
    FLOAT *c_nxt2line = c + 2 * ldc;
    FLOAT *c_nxt3line = c + 3 * ldc;

    src_c0 = LD_SP(c);
    src_c1 = LD_SP(c_nxt1line);
    src_c2 = LD_SP(c_nxt2line);
    src_c3 = LD_SP(c_nxt3line);

    for (k = 0; k < (bk >> 1); k++)
    {
        src_a = LD_SP(aa);

        src_b = LD_SP(bb);
        SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);

        src_c0 -= src_a * src_b0;
        src_c1 -= src_a * src_b1;
        src_c2 -= src_a * src_b2;
        src_c3 -= src_a * src_b3;

        aa += 4;
        bb += 4;

        src_a = LD_SP(aa);

        src_b = LD_SP(bb);
        SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);

        src_c0 -= src_a * src_b0;
        src_c1 -= src_a * src_b1;
        src_c2 -= src_a * src_b2;
        src_c3 -= src_a * src_b3;

        aa += 4;
        bb += 4;
    }

    if ((bk & 1) && (bk > 0))
    {
        src_a = LD_SP(aa);

        src_b = LD_SP(bb);
        SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);

        src_c0 -= src_a * src_b0;
        src_c1 -= src_a * src_b1;
        src_c2 -= src_a * src_b2;
        src_c3 -= src_a * src_b3;
    }

    a -= 16;
    b -= 16;

    src_b = LD_SP(b + 12);
    SPLATI_W4_SP(src_b, src_b12, src_b13, src_b14, src_b15);
    src_b8 = LD_SP(b + 8);
    src_b10 = (v4f32) __msa_splati_w((v4i32) src_b8, 2);
    src_b9 = (v4f32) __msa_splati_w((v4i32) src_b8, 1);
    src_b8 = (v4f32) __msa_splati_w((v4i32) src_b8, 0);
    src_b5 = COPY_FLOAT_TO_VECTOR(*(b + 5));
    src_b4 = COPY_FLOAT_TO_VECTOR(*(b + 4));
    src_b0 = COPY_FLOAT_TO_VECTOR(*(b + 0));

    src_c3 *= src_b15;
    src_c2 -= src_c3 * src_b14;
    src_c1 -= src_c3 * src_b13;
    src_c0 -= src_c3 * src_b12;

    src_c2 *= src_b10;
    src_c1 -= src_c2 * src_b9;
    src_c0 -= src_c2 * src_b8;

    src_c1 *= src_b5;
    src_c0 -= src_c1 * src_b4;

    src_c0 *= src_b0;

    ST_SP4(src_c0, src_c1, src_c2, src_c3, a, 4);

    ST_SP(src_c0, c);
    ST_SP(src_c1, c_nxt1line);
    ST_SP(src_c2, c_nxt2line);
    ST_SP(src_c3, c_nxt3line);
}

static void ssolve_4x2_rt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    v4f32 src_a, src_b1, src_c0, src_c1, src_b0, src_b2, src_b3;
    FLOAT *c_nxt1line = c + ldc;

    src_c0 = LD_SP(c);
    src_c1 = LD_SP(c_nxt1line);

    for (k = 0; k < (bk >> 2); k++)
    {
        src_a = LD_SP(aa);
        src_b0 = LD_SP(bb);
        src_b1 = (v4f32) __msa_splati_w((v4i32) src_b0, 1);
        src_b0 = (v4f32) __msa_splati_w((v4i32) src_b0, 0);

        src_c0 -= src_a * src_b0;
        src_c1 -= src_a * src_b1;

        aa += 4;
        bb += 2;

        src_a = LD_SP(aa);
        src_b0 = LD_SP(bb);
        src_b1 = (v4f32) __msa_splati_w((v4i32) src_b0, 1);
        src_b0 = (v4f32) __msa_splati_w((v4i32) src_b0, 0);

        src_c0 -= src_a * src_b0;
        src_c1 -= src_a * src_b1;

        aa += 4;
        bb += 2;

        src_a = LD_SP(aa);
        src_b0 = LD_SP(bb);
        src_b1 = (v4f32) __msa_splati_w((v4i32) src_b0, 1);
        src_b0 = (v4f32) __msa_splati_w((v4i32) src_b0, 0);

        src_c0 -= src_a * src_b0;
        src_c1 -= src_a * src_b1;

        aa += 4;
        bb += 2;

        src_a = LD_SP(aa);
        src_b0 = LD_SP(bb);
        src_b1 = (v4f32) __msa_splati_w((v4i32) src_b0, 1);
        src_b0 = (v4f32) __msa_splati_w((v4i32) src_b0, 0);

        src_c0 -= src_a * src_b0;
        src_c1 -= src_a * src_b1;

        aa += 4;
        bb += 2;
    }

    if ((bk & 3) && (bk > 0))
    {
        if (bk & 2)
        {
            src_a = LD_SP(aa);
            src_b0 = LD_SP(bb);
            src_b1 = (v4f32) __msa_splati_w((v4i32) src_b0, 1);
            src_b0 = (v4f32) __msa_splati_w((v4i32) src_b0, 0);

            src_c0 -= src_a * src_b0;
            src_c1 -= src_a * src_b1;

            aa += 4;
            bb += 2;

            src_a = LD_SP(aa);
            src_b0 = LD_SP(bb);
            src_b1 = (v4f32) __msa_splati_w((v4i32) src_b0, 1);
            src_b0 = (v4f32) __msa_splati_w((v4i32) src_b0, 0);

            src_c0 -= src_a * src_b0;
            src_c1 -= src_a * src_b1;

            aa += 4;
            bb += 2;
        }

        if (bk & 1)
        {
            src_a = LD_SP(aa);
            src_b0 = LD_SP(bb);
            src_b1 = (v4f32) __msa_splati_w((v4i32) src_b0, 1);
            src_b0 = (v4f32) __msa_splati_w((v4i32) src_b0, 0);

            src_c0 -= src_a * src_b0;
            src_c1 -= src_a * src_b1;
        }
    }

    a -= 8;
    b -= 4;

    src_b3 = COPY_FLOAT_TO_VECTOR(*(b + 3));
    src_b2 = COPY_FLOAT_TO_VECTOR(*(b + 2));
    src_b0 = COPY_FLOAT_TO_VECTOR(*(b + 0));

    src_c1 *= src_b3;
    src_c0 -= src_c1 * src_b2;
    src_c0 *= src_b0;

    ST_SP2(src_c0, src_c1, a, 4);

    ST_SP(src_c0, c);
    ST_SP(src_c1, c_nxt1line);
}

static void ssolve_4x1_rt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    FLOAT b0, c0, c1, c2, c3;

    c0 = *(c + 0);
    c1 = *(c + 1);
    c2 = *(c + 2);
    c3 = *(c + 3);

    for (k = 0; k < bk; k++)
    {
        c0 -= aa[0] * bb[0];
        c1 -= aa[1] * bb[0];
        c2 -= aa[2] * bb[0];
        c3 -= aa[3] * bb[0];

        aa += 4;
        bb += 1;
    }

    a -= 4;
    b -= 1;

    b0 = *b;

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

static void ssolve_2x8_rt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    FLOAT b0, b8, b9, b16, b17, b18, b24, b25, b26, b27, b32, b33, b34, b35;
    FLOAT b36, b40, b41, b42, b43, b44, b45, b48, b49, b50, b51, b52, b53, b54;
    FLOAT b56, b57, b58, b59, b60, b61, b62, b63, c0_nxt7, c1_nxt7;
    FLOAT c0, c1, c0_nxt1, c1_nxt1, c0_nxt2, c1_nxt2, c0_nxt3, c1_nxt3;
    FLOAT c0_nxt4, c1_nxt4, c0_nxt5, c1_nxt5, c0_nxt6, c1_nxt6;

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
        c0 -= aa[0] * bb[0];
        c1 -= aa[1] * bb[0];
        c0_nxt1 -= aa[0] * bb[1];
        c1_nxt1 -= aa[1] * bb[1];
        c0_nxt2 -= aa[0] * bb[2];
        c1_nxt2 -= aa[1] * bb[2];
        c0_nxt3 -= aa[0] * bb[3];
        c1_nxt3 -= aa[1] * bb[3];
        c0_nxt4 -= aa[0] * bb[4];
        c1_nxt4 -= aa[1] * bb[4];
        c0_nxt5 -= aa[0] * bb[5];
        c1_nxt5 -= aa[1] * bb[5];
        c0_nxt6 -= aa[0] * bb[6];
        c1_nxt6 -= aa[1] * bb[6];
        c0_nxt7 -= aa[0] * bb[7];
        c1_nxt7 -= aa[1] * bb[7];

        aa += 2;
        bb += 8;
    }

    a -= 16;
    b -= 64;

    b0 = *(b + 0);
    b8 = *(b + 8);
    b9 = *(b + 9);
    b16 = *(b + 16);
    b17 = *(b + 17);
    b18 = *(b + 18);
    b24 = *(b + 24);
    b25 = *(b + 25);
    b26 = *(b + 26);
    b27 = *(b + 27);
    b32 = *(b + 32);
    b33 = *(b + 33);
    b34 = *(b + 34);
    b35 = *(b + 35);
    b36 = *(b + 36);
    b40 = *(b + 40);
    b41 = *(b + 41);
    b42 = *(b + 42);
    b43 = *(b + 43);
    b44 = *(b + 44);
    b45 = *(b + 45);
    b48 = *(b + 48);
    b49 = *(b + 49);
    b50 = *(b + 50);
    b51 = *(b + 51);
    b52 = *(b + 52);
    b53 = *(b + 53);
    b54 = *(b + 54);
    b56 = *(b + 56);
    b57 = *(b + 57);
    b58 = *(b + 58);
    b59 = *(b + 59);
    b60 = *(b + 60);
    b61 = *(b + 61);
    b62 = *(b + 62);
    b63 = *(b + 63);

    c0_nxt7 *= b63;
    c1_nxt7 *= b63;

    c0_nxt6 -= c0_nxt7 * b62;
    c1_nxt6 -= c1_nxt7 * b62;

    c0_nxt6 *= b54;
    c1_nxt6 *= b54;

    c0_nxt5 -= c0_nxt7 * b61;
    c1_nxt5 -= c1_nxt7 * b61;

    c0_nxt5 -= c0_nxt6 * b53;
    c1_nxt5 -= c1_nxt6 * b53;

    c0_nxt5 *= b45;
    c1_nxt5 *= b45;

    c0_nxt4 -= c0_nxt7 * b60;
    c1_nxt4 -= c1_nxt7 * b60;

    c0_nxt4 -= c0_nxt6 * b52;
    c1_nxt4 -= c1_nxt6 * b52;

    c0_nxt4 -= c0_nxt5 * b44;
    c1_nxt4 -= c1_nxt5 * b44;

    c0_nxt4 *= b36;
    c1_nxt4 *= b36;

    c0_nxt3 -= c0_nxt7 * b59;
    c1_nxt3 -= c1_nxt7 * b59;

    c0_nxt3 -= c0_nxt6 * b51;
    c1_nxt3 -= c1_nxt6 * b51;

    c0_nxt3 -= c0_nxt5 * b43;
    c1_nxt3 -= c1_nxt5 * b43;

    c0_nxt3 -= c0_nxt4 * b35;
    c1_nxt3 -= c1_nxt4 * b35;

    c0_nxt3 *= b27;
    c1_nxt3 *= b27;

    c0_nxt2 -= c0_nxt7 * b58;
    c1_nxt2 -= c1_nxt7 * b58;

    c0_nxt2 -= c0_nxt6 * b50;
    c1_nxt2 -= c1_nxt6 * b50;

    c0_nxt2 -= c0_nxt5 * b42;
    c1_nxt2 -= c1_nxt5 * b42;

    c0_nxt2 -= c0_nxt4 * b34;
    c1_nxt2 -= c1_nxt4 * b34;

    c0_nxt2 -= c0_nxt3 * b26;
    c1_nxt2 -= c1_nxt3 * b26;

    c0_nxt2 *= b18;
    c1_nxt2 *= b18;

    c0_nxt1 -= c0_nxt7 * b57;
    c1_nxt1 -= c1_nxt7 * b57;

    c0_nxt1 -= c0_nxt6 * b49;
    c1_nxt1 -= c1_nxt6 * b49;

    c0_nxt1 -= c0_nxt5 * b41;
    c1_nxt1 -= c1_nxt5 * b41;

    c0_nxt1 -= c0_nxt4 * b33;
    c1_nxt1 -= c1_nxt4 * b33;

    c0_nxt1 -= c0_nxt3 * b25;
    c1_nxt1 -= c1_nxt3 * b25;

    c0_nxt1 -= c0_nxt2 * b17;
    c1_nxt1 -= c1_nxt2 * b17;

    c0_nxt1 *= b9;
    c1_nxt1 *= b9;

    c0 -= c0_nxt7 * b56;
    c1 -= c1_nxt7 * b56;

    c0 -= c0_nxt6 * b48;
    c1 -= c1_nxt6 * b48;

    c0 -= c0_nxt5 * b40;
    c1 -= c1_nxt5 * b40;

    c0 -= c0_nxt4 * b32;
    c1 -= c1_nxt4 * b32;

    c0 -= c0_nxt3 * b24;
    c1 -= c1_nxt3 * b24;

    c0 -= c0_nxt2 * b16;
    c1 -= c1_nxt2 * b16;

    c0 -= c0_nxt1 * b8;
    c1 -= c1_nxt1 * b8;

    c0 *= b0;
    c1 *= b0;

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

static void ssolve_2x4_rt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    FLOAT b0, b4, b5, b8, b9, b10, b12, b13, b14, b15;
    FLOAT c0, c1, c0_nxt1, c1_nxt1, c0_nxt2, c1_nxt2, c0_nxt3, c1_nxt3;

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

    a -= 8;
    b -= 16;

    b0 = *b;
    b4 = *(b + 4);
    b5 = *(b + 5);
    b8 = *(b + 8);
    b9 = *(b + 9);
    b10 = *(b + 10);
    b12 = *(b + 12);
    b13 = *(b + 13);
    b14 = *(b + 14);
    b15 = *(b + 15);

    c0_nxt3 *= b15;
    c1_nxt3 *= b15;

    c0_nxt2 = (c0_nxt2 - c0_nxt3 * b14) * b10;
    c1_nxt2 = (c1_nxt2 - c1_nxt3 * b14) * b10;

    c0_nxt1 = ((c0_nxt1 - c0_nxt3 * b13) - c0_nxt2 * b9) * b5;
    c1_nxt1 = ((c1_nxt1 - c1_nxt3 * b13) - c1_nxt2 * b9) * b5;

    c0 = (((c0 - c0_nxt3 * b12) - c0_nxt2 * b8) - c0_nxt1 * b4) * b0;
    c1 = (((c1 - c1_nxt3 * b12) - c1_nxt2 * b8) - c1_nxt1 * b4) * b0;

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
    *(c + 0 + 1 * ldc) = c0_nxt1;
    *(c + 1 + 1 * ldc) = c1_nxt1;
    *(c + 0 + 2 * ldc) = c0_nxt2;
    *(c + 1 + 2 * ldc) = c1_nxt2;
    *(c + 0 + 3 * ldc) = c0_nxt3;
    *(c + 1 + 3 * ldc) = c1_nxt3;
}

static void ssolve_2x2_rt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    FLOAT b0, b2, b3, c0, c1, c0_nxt, c1_nxt;

    c0 = *(c + 0);
    c1 = *(c + 1);
    c0_nxt = *(c + 0 + ldc);
    c1_nxt = *(c + 1 + ldc);

    for (k = 0; k < bk; k++)
    {
        c0 -= aa[0] * bb[0];
        c1 -= aa[1] * bb[0];
        c0_nxt -= aa[0] * bb[1];
        c1_nxt -= aa[1] * bb[1];

        aa += 2;
        bb += 2;
    }

    a -= 4;
    b -= 4;

    b3 = *(b + 3);
    b2 = *(b + 2);
    b0 = *b;

    c0_nxt *= b3;
    c1_nxt *= b3;

    c0 -= c0_nxt * b2;
    c1 -= c1_nxt * b2;

    c0 *= b0;
    c1 *= b0;

    *(a + 0) = c0;
    *(a + 1) = c1;
    *(a + 2) = c0_nxt;
    *(a + 3) = c1_nxt;

    *(c + 0) = c0;
    *(c + 1) = c1;
    *(c + 0 + ldc) = c0_nxt;
    *(c + 1 + ldc) = c1_nxt;
}

static void ssolve_2x1_rt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    FLOAT b0, c0, c1;

    c0 = *(c + 0);
    c1 = *(c + 1);

    for (k = 0; k < bk; k++)
    {
        c0 -= aa[0] * bb[0];
        c1 -= aa[1] * bb[0];

        aa += 2;
        bb += 1;
    }

    a -= 2;
    b -= 1;

    b0 = *b;

    c0 *= b0;
    c1 *= b0;

    *(a + 0) = c0;
    *(a + 1) = c1;

    *(c + 0) = c0;
    *(c + 1) = c1;
}

static void ssolve_1x8_rt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    FLOAT b0, b8, b9, b16, b17, b18, b24, b25, b26, b27, b32, b33, b34, b35;
    FLOAT b36, b40, b41, b42, b43, b44, b45, b48, b49, b50, b51, b52, b53, b54;
    FLOAT b56, b57, b58, b59, b60, b61, b62, b63;
    FLOAT c0, c1, c2, c3, c4, c5, c6, c7;

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
        c0 -= aa[0] * bb[0];
        c1 -= aa[0] * bb[1];
        c2 -= aa[0] * bb[2];
        c3 -= aa[0] * bb[3];
        c4 -= aa[0] * bb[4];
        c5 -= aa[0] * bb[5];
        c6 -= aa[0] * bb[6];
        c7 -= aa[0] * bb[7];

        aa += 1;
        bb += 8;
    }

    a -= 8;
    b -= 64;

    b0 = *(b + 0);
    b8 = *(b + 8);
    b9 = *(b + 9);
    b16 = *(b + 16);
    b17 = *(b + 17);
    b18 = *(b + 18);
    b24 = *(b + 24);
    b25 = *(b + 25);
    b26 = *(b + 26);
    b27 = *(b + 27);
    b32 = *(b + 32);
    b33 = *(b + 33);
    b34 = *(b + 34);
    b35 = *(b + 35);
    b36 = *(b + 36);
    b40 = *(b + 40);
    b41 = *(b + 41);
    b42 = *(b + 42);
    b43 = *(b + 43);
    b44 = *(b + 44);
    b45 = *(b + 45);
    b48 = *(b + 48);
    b49 = *(b + 49);
    b50 = *(b + 50);
    b51 = *(b + 51);
    b52 = *(b + 52);
    b53 = *(b + 53);
    b54 = *(b + 54);
    b56 = *(b + 56);
    b57 = *(b + 57);
    b58 = *(b + 58);
    b59 = *(b + 59);
    b60 = *(b + 60);
    b61 = *(b + 61);
    b62 = *(b + 62);
    b63 = *(b + 63);

    c7 *= b63;

    c6 -= c7 * b62;
    c6 *= b54;

    c5 -= c7 * b61;
    c5 -= c6 * b53;
    c5 *= b45;

    c4 -= c7 * b60;
    c4 -= c6 * b52;
    c4 -= c5 * b44;
    c4 *= b36;

    c3 -= c7 * b59;
    c3 -= c6 * b51;
    c3 -= c5 * b43;
    c3 -= c4 * b35;
    c3 *= b27;

    c2 -= c7 * b58;
    c2 -= c6 * b50;
    c2 -= c5 * b42;
    c2 -= c4 * b34;
    c2 -= c3 * b26;
    c2 *= b18;

    c1 -= c7 * b57;
    c1 -= c6 * b49;
    c1 -= c5 * b41;
    c1 -= c4 * b33;
    c1 -= c3 * b25;
    c1 -= c2 * b17;
    c1 *= b9;

    c0 -= c7 * b56;
    c0 -= c6 * b48;
    c0 -= c5 * b40;
    c0 -= c4 * b32;
    c0 -= c3 * b24;
    c0 -= c2 * b16;
    c0 -= c1 * b8;
    c0 *= b0;

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

static void ssolve_1x4_rt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    FLOAT b0, b4, b5, b8, b9, b10, b12, b13, b14, b15;
    FLOAT c0, c1, c2, c3;

    c0 = *(c + 0);
    c1 = *(c + 1 * ldc);
    c2 = *(c + 2 * ldc);
    c3 = *(c + 3 * ldc);

    for (k = 0; k < bk; k++)
    {
        c0 -= aa[0] * bb[0];
        c1 -= aa[0] * bb[1];
        c2 -= aa[0] * bb[2];
        c3 -= aa[0] * bb[3];

        aa += 1;
        bb += 4;
    }

    a -= 4;
    b -= 16;

    b0 = *b;
    b4 = *(b + 4);
    b5 = *(b + 5);
    b8 = *(b + 8);
    b9 = *(b + 9);
    b10 = *(b + 10);
    b12 = *(b + 12);
    b13 = *(b + 13);
    b14 = *(b + 14);
    b15 = *(b + 15);

    c3 *= b15;
    c2 = (c2 - c3 * b14) * b10;
    c1 = ((c1 - c3 * b13) - c2 * b9) * b5;
    c0 = (((c0 - c3 * b12) - c2 * b8) - c1 * b4) * b0;

    *(a + 0) = c0;
    *(a + 1) = c1;
    *(a + 2) = c2;
    *(a + 3) = c3;

    *(c) = c0;
    *(c + 1 * ldc) = c1;
    *(c + 2 * ldc) = c2;
    *(c + 3 * ldc) = c3;
}

static void ssolve_1x2_rt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    FLOAT b0, b2, b3, c0, c1;

    c0 = *(c + 0);
    c1 = *(c + ldc);

    for (k = 0; k < bk; k++)
    {
        c0 -= aa[0] * bb[0];
        c1 -= aa[0] * bb[1];

        aa += 1;
        bb += 2;
    }

    a -= 2;
    b -= 4;

    b3 = *(b + 3);
    b2 = *(b + 2);
    b0 = *b;

    c1 *= b3;

    c0 -= c1 * b2;
    c0 *= b0;

    *(a + 0) = c0;
    *(a + 1) = c1;

    *(c + 0) = c0;
    *(c + ldc) = c1;
}

static void ssolve_1x1_rt_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG bk)
{
    BLASLONG k;

    for (k = 0; k < bk; k++)
    {
        *c -= a[k] * b[k];
    }

    *c *= *(a - 1);
    *(b - 1) = *c;
}

int CNAME(BLASLONG m, BLASLONG n, BLASLONG k, FLOAT dummy1, FLOAT *a, FLOAT *b,
          FLOAT *c, BLASLONG ldc, BLASLONG offset)
{
    FLOAT *aa, *cc;
    BLASLONG i, j, kk;

    kk = n - offset;
    c += n * ldc;
    b += n * k;

    if (n & 7)
    {
        if (n & 1)
        {
            aa = a;
            b -= k;
            c -= ldc;
            cc = c;

            for (i = (m >> 3); i--;)
            {
                ssolve_8x1_rt_msa(aa + 8 * kk, b + kk, cc, (k - kk));

                aa += 8 * k;
                cc += 8;
            }

            if (m & 7)
            {
                if (m & 4)
                {
                    ssolve_4x1_rt_msa(aa + 4 * kk, b + kk, cc, (k - kk));

                    aa += 4 * k;
                    cc += 4;
                }

                if (m & 2)
                {
                    ssolve_2x1_rt_msa(aa + 2 * kk, b + kk, cc, (k - kk));

                    aa += 2 * k;
                    cc += 2;
                }

                if (m & 1)
                {
                    ssolve_1x1_rt_msa(b + kk, aa + kk, cc, (k - kk));

                    aa += k;
                    cc += 1;
                }
            }

            kk -= 1;
        }

        if (n & 2)
        {
            aa = a;
            b -= 2 * k;
            c -= 2 * ldc;
            cc = c;

            for (i = (m >> 3); i--;)
            {
                ssolve_8x2_rt_msa(aa + 8 * kk, b + 2 * kk, cc, ldc, (k - kk));

                aa += 8 * k;
                cc += 8;
            }

            if (m & 7)
            {
                if (m & 4)
                {
                    ssolve_4x2_rt_msa(aa + 4 * kk, b + 2 * kk, cc, ldc, (k - kk));

                    aa += 4 * k;
                    cc += 4;
                }

                if (m & 2)
                {
                    ssolve_2x2_rt_msa(aa + 2 * kk, b + 2 * kk, cc, ldc, (k - kk));

                    aa += 2 * k;
                    cc += 2;
                }

                if (m & 1)
                {
                    ssolve_1x2_rt_msa(aa + kk, b + 2 * kk, cc, ldc, (k - kk));

                    aa += k;
                    cc += 1;
                }
            }

            kk -= 2;
        }

        if (n & 4)
        {
            aa = a;
            b -= 4 * k;
            c -= 4 * ldc;
            cc = c;

            for (i = (m >> 3); i--;)
            {
                ssolve_8x4_rt_msa(aa + 8 * kk, b + 4 * kk, cc, ldc, (k - kk));

                aa += 8 * k;
                cc += 8;
            }

            if (m & 7)
            {
                if (m & 4)
                {
                    ssolve_4x4_rt_msa(aa + 4 * kk, b + 4 * kk, cc, ldc, (k - kk));

                    aa += 4 * k;
                    cc += 4;
                }

                if (m & 2)
                {
                    ssolve_2x4_rt_msa(aa + 2 * kk, b + 4 * kk, cc, ldc, (k - kk));

                    aa += 2 * k;
                    cc += 2;
                }

                if (m & 1)
                {
                    ssolve_1x4_rt_msa(aa + kk, b + 4 * kk, cc, ldc, (k - kk));

                    aa += k;
                    cc += 1;
                }
            }

            kk -= 4;
        }
    }

    for (j = (n >> 3); j--;)
    {
        aa = a;
        b -= 8 * k;
        c -= 8 * ldc;
        cc = c;

        for (i = (m >> 3); i--;)
        {
            ssolve_8x8_rt_msa(aa + 8 * kk, b + 8 * kk, cc, ldc, (k - kk));

            aa += 8 * k;
            cc += 8;
        }

        if (m & 7)
        {
            if (m & 4)
            {
                ssolve_4x8_rt_msa(aa + 4 * kk, b + 8 * kk, cc, ldc, (k - kk));

                aa += 4 * k;
                cc += 4;
            }

            if (m & 2)
            {
                ssolve_2x8_rt_msa(aa + 2 * kk, b + 8 * kk, cc, ldc, (k - kk));

                aa += 2 * k;
                cc += 2;
            }

            if (m & 1)
            {
                ssolve_1x8_rt_msa(aa + kk, b + 8 * kk, cc, ldc, (k - kk));

                aa += k;
                cc += 1;
            }
        }

        kk -= 8;
    }

    return 0;
}
