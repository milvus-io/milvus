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

static void ssolve_8x8_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    v4f32 src_c0, src_c1, src_c2, src_c3, src_c4, src_c5, src_c6, src_c7;
    v4f32 src_c8, src_c9, src_c10, src_c11, src_c12, src_c13, src_c14, src_c15;
    v4f32 res_c0, res_c1, res_c2, res_c3, res_c4, res_c5, res_c6, res_c7;
    v4f32 res_c8, res_c9, res_c10, res_c11, res_c12, res_c13, res_c14, res_c15;
    v4f32 src_a, src_a0, src_a8, src_a9, src_a16, src_a17, src_a18, src_a24;
    v4f32 src_a25, src_a26, src_a27, src_a32, src_a33, src_a34, src_a35, src_a36;
    v4f32 src_a40, src_a41, src_a42, src_a43, src_a44, src_a45;
    v4f32 src_a48, src_a49, src_a50, src_a51, src_a52, src_a53, src_a54;
    v4f32 src_a56, src_a57, src_a58, src_a59, src_a60, src_a61, src_a62, src_a63;
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
        v4f32 src_a1, src_b0, src_b1, src_b2, src_b3, src_bb0, src_bb1;

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

    a -= 64;
    b -= 64;

    TRANSPOSE4x4_SP_SP(src_c1, src_c3, src_c5, src_c7,
                       res_c4, res_c5, res_c6, res_c7);
    TRANSPOSE4x4_SP_SP(src_c9, src_c11, src_c13, src_c15,
                       res_c12, res_c13, res_c14, res_c15);
    TRANSPOSE4x4_SP_SP(src_c0, src_c2, src_c4, src_c6,
                       res_c0, res_c1, res_c2, res_c3);
    TRANSPOSE4x4_SP_SP(src_c8, src_c10, src_c12, src_c14,
                       res_c8, res_c9, res_c10, res_c11);

    src_a = LD_SP(a + 60);
    SPLATI_W4_SP(src_a, src_a60, src_a61, src_a62, src_a63);
    src_a = LD_SP(a + 56);
    SPLATI_W4_SP(src_a, src_a56, src_a57, src_a58, src_a59);

    res_c7 *= src_a63;
    res_c15 *= src_a63;
    res_c6 -= res_c7 * src_a62;
    res_c14 -= res_c15 * src_a62;
    res_c5 -= res_c7 * src_a61;
    res_c13 -= res_c15 * src_a61;
    res_c4 -= res_c7 * src_a60;
    res_c12 -= res_c15 * src_a60;
    res_c3 -= res_c7 * src_a59;
    res_c11 -= res_c15 * src_a59;
    res_c2 -= res_c7 * src_a58;
    res_c10 -= res_c15 * src_a58;
    res_c1 -= res_c7 * src_a57;
    res_c9 -= res_c15 * src_a57;
    res_c0 -= res_c7 * src_a56;
    res_c8 -= res_c15 * src_a56;

    src_a = LD_SP(a + 48);
    SPLATI_W4_SP(src_a, src_a48, src_a49, src_a50, src_a51);
    src_a52 = LD_SP(a + 52);
    src_a54 = (v4f32) __msa_splati_w((v4i32) src_a52, 2);
    src_a53 = (v4f32) __msa_splati_w((v4i32) src_a52, 1);
    src_a52 = (v4f32) __msa_splati_w((v4i32) src_a52, 0);

    res_c6 *= src_a54;
    res_c14 *= src_a54;
    res_c5 -= res_c6 * src_a53;
    res_c13 -= res_c14 * src_a53;
    res_c4 -= res_c6 * src_a52;
    res_c12 -= res_c14 * src_a52;
    res_c3 -= res_c6 * src_a51;
    res_c11 -= res_c14 * src_a51;
    res_c2 -= res_c6 * src_a50;
    res_c10 -= res_c14 * src_a50;
    res_c1 -= res_c6 * src_a49;
    res_c9 -= res_c14 * src_a49;
    res_c0 -= res_c6 * src_a48;
    res_c8 -= res_c14 * src_a48;

    src_a = LD_SP(a + 40);
    SPLATI_W4_SP(src_a, src_a40, src_a41, src_a42, src_a43);
    src_a44 = LD_SP(a + 44);
    src_a45 = (v4f32) __msa_splati_w((v4i32) src_a44, 1);
    src_a44 = (v4f32) __msa_splati_w((v4i32) src_a44, 0);

    res_c5 *= src_a45;
    res_c13 *= src_a45;
    res_c4 -= res_c5 * src_a44;
    res_c12 -= res_c13 * src_a44;
    res_c3 -= res_c5 * src_a43;
    res_c11 -= res_c13 * src_a43;
    res_c2 -= res_c5 * src_a42;
    res_c10 -= res_c13 * src_a42;
    res_c1 -= res_c5 * src_a41;
    res_c9 -= res_c13 * src_a41;
    res_c0 -= res_c5 * src_a40;
    res_c8 -= res_c13 * src_a40;

    src_a = LD_SP(a + 32);
    SPLATI_W4_SP(src_a, src_a32, src_a33, src_a34, src_a35);
    src_a36 = COPY_FLOAT_TO_VECTOR(*(a + 36));

    res_c4 *= src_a36;
    res_c12 *= src_a36;
    res_c3 -= res_c4 * src_a35;
    res_c11 -= res_c12 * src_a35;
    res_c2 -= res_c4 * src_a34;
    res_c10 -= res_c12 * src_a34;
    res_c1 -= res_c4 * src_a33;
    res_c9 -= res_c12 * src_a33;
    res_c0 -= res_c4 * src_a32;
    res_c8 -= res_c12 * src_a32;

    ST_SP4(res_c4, res_c12, res_c5, res_c13, b + 32, 4);
    ST_SP4(res_c6, res_c14, res_c7, res_c15, b + 48, 4);

    TRANSPOSE4x4_SP_SP(res_c4, res_c5, res_c6, res_c7,
                       src_c1, src_c3, src_c5, src_c7);
    TRANSPOSE4x4_SP_SP(res_c12, res_c13, res_c14, res_c15,
                       src_c9, src_c11, src_c13, src_c15);

    ST_SP(src_c1, c + 4);
    ST_SP(src_c3, c_nxt1line + 4);
    ST_SP(src_c5, c_nxt2line + 4);
    ST_SP(src_c7, c_nxt3line + 4);
    ST_SP(src_c9, c_nxt4line + 4);
    ST_SP(src_c11, c_nxt5line + 4);
    ST_SP(src_c13, c_nxt6line + 4);
    ST_SP(src_c15, c_nxt7line + 4);

    src_a = LD_SP(a + 24);
    SPLATI_W4_SP(src_a, src_a24, src_a25, src_a26, src_a27);

    res_c3 *= src_a27;
    res_c11 *= src_a27;
    res_c2 -= res_c3 * src_a26;
    res_c10 -= res_c11 * src_a26;
    res_c1 -= res_c3 * src_a25;
    res_c9 -= res_c11 * src_a25;
    res_c0 -= res_c3 * src_a24;
    res_c8 -= res_c11 * src_a24;

    src_a16 = LD_SP(a + 16);
    src_a18 = (v4f32) __msa_splati_w((v4i32) src_a16, 2);
    src_a17 = (v4f32) __msa_splati_w((v4i32) src_a16, 1);
    src_a16 = (v4f32) __msa_splati_w((v4i32) src_a16, 0);

    res_c2 *= src_a18;
    res_c10 *= src_a18;
    res_c1 -= res_c2 * src_a17;
    res_c9 -= res_c10 * src_a17;
    res_c0 -= res_c2 * src_a16;
    res_c8 -= res_c10 * src_a16;

    src_a9 = COPY_FLOAT_TO_VECTOR(*(a + 9));
    src_a8 = COPY_FLOAT_TO_VECTOR(*(a + 8));
    src_a0 = COPY_FLOAT_TO_VECTOR(*(a + 0));

    res_c1 *= src_a9;
    res_c9 *= src_a9;
    res_c0 -= res_c1 * src_a8;
    res_c8 -= res_c9 * src_a8;

    res_c0 *= src_a0;
    res_c8 *= src_a0;

    ST_SP4(res_c0, res_c8, res_c1, res_c9, b, 4);
    ST_SP4(res_c2, res_c10, res_c3, res_c11, b + 16, 4);

    TRANSPOSE4x4_SP_SP(res_c0, res_c1, res_c2, res_c3,
                       src_c0, src_c2, src_c4, src_c6);
    TRANSPOSE4x4_SP_SP(res_c8, res_c9, res_c10, res_c11,
                       src_c8, src_c10, src_c12, src_c14);

    ST_SP(src_c0, c);
    ST_SP(src_c2, c_nxt1line);
    ST_SP(src_c4, c_nxt2line);
    ST_SP(src_c6, c_nxt3line);
    ST_SP(src_c8, c_nxt4line);
    ST_SP(src_c10, c_nxt5line);
    ST_SP(src_c12, c_nxt6line);
    ST_SP(src_c14, c_nxt7line);
}

static void ssolve_8x4_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    v4f32 src_b, src_b0, src_b1, src_b2, src_b3, src_a1;
    v4f32 src_c0, src_c1, src_c2, src_c3, src_c4, src_c5, src_c6, src_c7;
    v4f32 res_c0, res_c1, res_c2, res_c3, res_c4, res_c5, res_c6, res_c7;
    v4f32 src_a, src_a0, src_a8, src_a9, src_a16, src_a17, src_a18, src_a24;
    v4f32 src_a25, src_a26, src_a27, src_a32, src_a33, src_a34, src_a35;
    v4f32 src_a36, src_a40, src_a41, src_a42, src_a43, src_a44, src_a45;
    v4f32 src_a48, src_a49, src_a50, src_a51, src_a52, src_a53, src_a54;
    v4f32 src_a56, src_a57, src_a58, src_a59, src_a60, src_a61, src_a62, src_a63;
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

    a -= 64;
    b -= 32;

    TRANSPOSE4x4_SP_SP(src_c0, src_c2, src_c4, src_c6,
                       res_c0, res_c1, res_c2, res_c3);
    TRANSPOSE4x4_SP_SP(src_c1, src_c3, src_c5, src_c7,
                       res_c4, res_c5, res_c6, res_c7);

    src_a = LD_SP(a + 60);
    SPLATI_W4_SP(src_a, src_a60, src_a61, src_a62, src_a63);
    src_a = LD_SP(a + 56);
    SPLATI_W4_SP(src_a, src_a56, src_a57, src_a58, src_a59);

    src_a = LD_SP(a + 48);
    SPLATI_W4_SP(src_a, src_a48, src_a49, src_a50, src_a51);
    src_a52 = LD_SP(a + 52);
    src_a54 = (v4f32) __msa_splati_w((v4i32) src_a52, 2);
    src_a53 = (v4f32) __msa_splati_w((v4i32) src_a52, 1);
    src_a52 = (v4f32) __msa_splati_w((v4i32) src_a52, 0);

    res_c7 *= src_a63;
    res_c6 -= res_c7 * src_a62;
    res_c5 -= res_c7 * src_a61;
    res_c4 -= res_c7 * src_a60;
    res_c3 -= res_c7 * src_a59;
    res_c2 -= res_c7 * src_a58;
    res_c1 -= res_c7 * src_a57;
    res_c0 -= res_c7 * src_a56;

    res_c6 *= src_a54;
    res_c5 -= res_c6 * src_a53;
    res_c4 -= res_c6 * src_a52;
    res_c3 -= res_c6 * src_a51;
    res_c2 -= res_c6 * src_a50;
    res_c1 -= res_c6 * src_a49;
    res_c0 -= res_c6 * src_a48;

    src_a = LD_SP(a + 40);
    SPLATI_W4_SP(src_a, src_a40, src_a41, src_a42, src_a43);
    src_a44 = LD_SP(a + 44);
    src_a45 = (v4f32) __msa_splati_w((v4i32) src_a44, 1);
    src_a44 = (v4f32) __msa_splati_w((v4i32) src_a44, 0);

    res_c5 *= src_a45;
    res_c4 -= res_c5 * src_a44;
    res_c3 -= res_c5 * src_a43;
    res_c2 -= res_c5 * src_a42;
    res_c1 -= res_c5 * src_a41;
    res_c0 -= res_c5 * src_a40;

    src_a = LD_SP(a + 32);
    SPLATI_W4_SP(src_a, src_a32, src_a33, src_a34, src_a35);
    src_a36 = COPY_FLOAT_TO_VECTOR(*(a + 36));

    res_c4 *= src_a36;
    res_c3 -= res_c4 * src_a35;
    res_c2 -= res_c4 * src_a34;
    res_c1 -= res_c4 * src_a33;
    res_c0 -= res_c4 * src_a32;

    src_a = LD_SP(a + 24);
    SPLATI_W4_SP(src_a, src_a24, src_a25, src_a26, src_a27);

    res_c3 *= src_a27;
    res_c2 -= res_c3 * src_a26;
    res_c1 -= res_c3 * src_a25;
    res_c0 -= res_c3 * src_a24;

    src_a16 = LD_SP(a + 16);
    src_a18 = (v4f32) __msa_splati_w((v4i32) src_a16, 2);
    src_a17 = (v4f32) __msa_splati_w((v4i32) src_a16, 1);
    src_a16 = (v4f32) __msa_splati_w((v4i32) src_a16, 0);

    res_c2 *= src_a18;
    res_c1 -= res_c2 * src_a17;
    res_c0 -= res_c2 * src_a16;

    src_a9 = COPY_FLOAT_TO_VECTOR(*(a + 9));
    src_a8 = COPY_FLOAT_TO_VECTOR(*(a + 8));
    src_a0 = COPY_FLOAT_TO_VECTOR(*(a + 0));

    res_c1 *= src_a9;
    res_c0 -= res_c1 * src_a8;

    res_c0 *= src_a0;

    ST_SP4(res_c0, res_c1, res_c2, res_c3, b, 4);
    ST_SP4(res_c4, res_c5, res_c6, res_c7, b + 16, 4);

    TRANSPOSE4x4_SP_SP(res_c0, res_c1, res_c2, res_c3,
                       src_c0, src_c2, src_c4, src_c6);
    TRANSPOSE4x4_SP_SP(res_c4, res_c5, res_c6, res_c7,
                       src_c1, src_c3, src_c5, src_c7);

    ST_SP2(src_c0, src_c1, c, 4);
    ST_SP2(src_c2, src_c3, c_nxt1line, 4);
    ST_SP2(src_c4, src_c5, c_nxt2line, 4);
    ST_SP2(src_c6, src_c7, c_nxt3line, 4);
}

static void ssolve_8x2_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    FLOAT a0, a8, a9, a16, a17, a18, a24, a25, a26, a27, a32, a33, a34, a35;
    FLOAT a36, a40, a41, a42, a43, a44, a45, a48, a49, a50, a51, a52, a53;
    FLOAT a54, a56, a57, a58, a59, a60, a61, a62, a63;
    FLOAT c0, c1, c2, c3, c4, c5, c6, c7;
    FLOAT c0_nxt, c1_nxt, c2_nxt, c3_nxt, c4_nxt, c5_nxt, c6_nxt, c7_nxt;

    c0 = *(c + 0);
    c1 = *(c + 1);
    c2 = *(c + 2);
    c3 = *(c + 3);
    c4 = *(c + 4);
    c5 = *(c + 5);
    c6 = *(c + 6);
    c7 = *(c + 7);
    c0_nxt = *(c + 0 + ldc);
    c1_nxt = *(c + 1 + ldc);
    c2_nxt = *(c + 2 + ldc);
    c3_nxt = *(c + 3 + ldc);
    c4_nxt = *(c + 4 + ldc);
    c5_nxt = *(c + 5 + ldc);
    c6_nxt = *(c + 6 + ldc);
    c7_nxt = *(c + 7 + ldc);

    for (k = 0; k < bk; k++)
    {
        c0 -= aa[0] * bb[0];
        c1 -= aa[1] * bb[0];
        c2 -= aa[2] * bb[0];
        c3 -= aa[3] * bb[0];
        c4 -= aa[4] * bb[0];
        c5 -= aa[5] * bb[0];
        c6 -= aa[6] * bb[0];
        c7 -= aa[7] * bb[0];
        c0_nxt -= aa[0] * bb[1];
        c1_nxt -= aa[1] * bb[1];
        c2_nxt -= aa[2] * bb[1];
        c3_nxt -= aa[3] * bb[1];
        c4_nxt -= aa[4] * bb[1];
        c5_nxt -= aa[5] * bb[1];
        c6_nxt -= aa[6] * bb[1];
        c7_nxt -= aa[7] * bb[1];

        aa += 8;
        bb += 2;
    }

    a -= 64;
    b -= 16;

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
    c7_nxt *= a63;
    c6 -= c7 * a62;
    c6_nxt -= c7_nxt * a62;
    c5 -= c7 * a61;
    c5_nxt -= c7_nxt * a61;
    c4 -= c7 * a60;
    c4_nxt -= c7_nxt * a60;
    c3 -= c7 * a59;
    c3_nxt -= c7_nxt * a59;
    c2 -= c7 * a58;
    c2_nxt -= c7_nxt * a58;
    c1 -= c7 * a57;
    c1_nxt -= c7_nxt * a57;
    c0 -= c7 * a56;
    c0_nxt -= c7_nxt * a56;

    c6 *= a54;
    c6_nxt *= a54;
    c5 -= c6 * a53;
    c5_nxt -= c6_nxt * a53;
    c4 -= c6 * a52;
    c4_nxt -= c6_nxt * a52;
    c3 -= c6 * a51;
    c3_nxt -= c6_nxt * a51;
    c2 -= c6 * a50;
    c2_nxt -= c6_nxt * a50;
    c1 -= c6 * a49;
    c1_nxt -= c6_nxt * a49;
    c0 -= c6 * a48;
    c0_nxt -= c6_nxt * a48;

    c5 *= a45;
    c5_nxt *= a45;
    c4 -= c5 * a44;
    c4_nxt -= c5_nxt * a44;
    c3 -= c5 * a43;
    c3_nxt -= c5_nxt * a43;
    c2 -= c5 * a42;
    c2_nxt -= c5_nxt * a42;
    c1 -= c5 * a41;
    c1_nxt -= c5_nxt * a41;
    c0 -= c5 * a40;
    c0_nxt -= c5_nxt * a40;

    c4 *= a36;
    c4_nxt *= a36;
    c3 -= c4 * a35;
    c3_nxt -= c4_nxt * a35;
    c2 -= c4 * a34;
    c2_nxt -= c4_nxt * a34;
    c1 -= c4 * a33;
    c1_nxt -= c4_nxt * a33;
    c0 -= c4 * a32;
    c0_nxt -= c4_nxt * a32;

    c3 *= a27;
    c3_nxt *= a27;
    c2 -= c3 * a26;
    c2_nxt -= c3_nxt * a26;
    c1 -= c3 * a25;
    c1_nxt -= c3_nxt * a25;
    c0 -= c3 * a24;
    c0_nxt -= c3_nxt * a24;

    c2 *= a18;
    c2_nxt *= a18;
    c1 -= c2 * a17;
    c1_nxt -= c2_nxt * a17;
    c0 -= c2 * a16;
    c0_nxt -= c2_nxt * a16;

    c1 *= a9;
    c1_nxt *= a9;
    c0 -= c1 * a8;
    c0_nxt -= c1_nxt * a8;

    c0 *= a0;
    c0_nxt *= a0;

    *(b + 0) = c0;
    *(b + 1) = c0_nxt;
    *(b + 2) = c1;
    *(b + 3) = c1_nxt;
    *(b + 4) = c2;
    *(b + 5) = c2_nxt;
    *(b + 6) = c3;
    *(b + 7) = c3_nxt;
    *(b + 8) = c4;
    *(b + 9) = c4_nxt;
    *(b + 10) = c5;
    *(b + 11) = c5_nxt;
    *(b + 12) = c6;
    *(b + 13) = c6_nxt;
    *(b + 14) = c7;
    *(b + 15) = c7_nxt;

    *(c + 0) = c0;
    *(c + 1) = c1;
    *(c + 2) = c2;
    *(c + 3) = c3;
    *(c + 4) = c4;
    *(c + 5) = c5;
    *(c + 6) = c6;
    *(c + 7) = c7;
    *(c + 0 + ldc) = c0_nxt;
    *(c + 1 + ldc) = c1_nxt;
    *(c + 2 + ldc) = c2_nxt;
    *(c + 3 + ldc) = c3_nxt;
    *(c + 4 + ldc) = c4_nxt;
    *(c + 5 + ldc) = c5_nxt;
    *(c + 6 + ldc) = c6_nxt;
    *(c + 7 + ldc) = c7_nxt;
}

static void ssolve_8x1_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
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

    for (k = 0; k < bk; k++)
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

    *(b + 0) = c0;
    *(b + 1) = c1;
    *(b + 2) = c2;
    *(b + 3) = c3;
    *(b + 4) = c4;
    *(b + 5) = c5;
    *(b + 6) = c6;
    *(b + 7) = c7;

    *(c + 0) = c0;
    *(c + 1) = c1;
    *(c + 2) = c2;
    *(c + 3) = c3;
    *(c + 4) = c4;
    *(c + 5) = c5;
    *(c + 6) = c6;
    *(c + 7) = c7;
}

static void ssolve_4x8_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    v4f32 src_b, src_b0, src_b1, src_b2, src_b3;
    v4f32 src_c0, src_c1, src_c2, src_c3, src_c4, src_c5, src_c6, src_c7;
    v4f32 res_c0, res_c1, res_c2, res_c3, res_c4, res_c5, res_c6, res_c7;
    v4f32 src_a, src_a0, src_a4, src_a5, src_a8, src_a9, src_a10, src_a12;
    v4f32 src_a13, src_a14, src_a15;
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

        src_b = LD_SP(bb);
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

    a -= 16;
    b -= 32;

    TRANSPOSE4x4_SP_SP(src_c0, src_c1, src_c2, src_c3,
                       res_c0, res_c1, res_c2, res_c3);
    TRANSPOSE4x4_SP_SP(src_c4, src_c5, src_c6, src_c7,
                       res_c4, res_c5, res_c6, res_c7);

    src_a = LD_SP(a + 12);
    SPLATI_W4_SP(src_a, src_a12, src_a13, src_a14, src_a15);
    src_a8 = LD_SP(a + 8);
    src_a10 = (v4f32) __msa_splati_w((v4i32) src_a8, 2);
    src_a9 = (v4f32) __msa_splati_w((v4i32) src_a8, 1);
    src_a8 = (v4f32) __msa_splati_w((v4i32) src_a8, 0);

    src_a5 = COPY_FLOAT_TO_VECTOR(*(a + 5));
    src_a4 = COPY_FLOAT_TO_VECTOR(*(a + 4));
    src_a0 = COPY_FLOAT_TO_VECTOR(*(a + 0));

    res_c3 *= src_a15;
    res_c7 *= src_a15;
    res_c2 -= res_c3 * src_a14;
    res_c6 -= res_c7 * src_a14;
    res_c1 -= res_c3 * src_a13;
    res_c5 -= res_c7 * src_a13;
    res_c0 -= res_c3 * src_a12;
    res_c4 -= res_c7 * src_a12;

    res_c2 *= src_a10;
    res_c6 *= src_a10;
    res_c1 -= res_c2 * src_a9;
    res_c5 -= res_c6 * src_a9;
    res_c0 -= res_c2 * src_a8;
    res_c4 -= res_c6 * src_a8;

    res_c1 *= src_a5;
    res_c5 *= src_a5;
    res_c0 -= res_c1 * src_a4;
    res_c4 -= res_c5 * src_a4;

    res_c0 *= src_a0;
    res_c4 *= src_a0;

    ST_SP4(res_c0, res_c4, res_c1, res_c5, b, 4);
    ST_SP4(res_c2, res_c6, res_c3, res_c7, b + 16, 4);

    TRANSPOSE4x4_SP_SP(res_c0, res_c1, res_c2, res_c3,
                       src_c0, src_c1, src_c2, src_c3);
    TRANSPOSE4x4_SP_SP(res_c4, res_c5, res_c6, res_c7,
                       src_c4, src_c5, src_c6, src_c7);

    ST_SP(src_c0, c);
    ST_SP(src_c1, c_nxt1line);
    ST_SP(src_c2, c_nxt2line);
    ST_SP(src_c3, c_nxt3line);
    ST_SP(src_c4, c_nxt4line);
    ST_SP(src_c5, c_nxt5line);
    ST_SP(src_c6, c_nxt6line);
    ST_SP(src_c7, c_nxt7line);
}

static void ssolve_4x4_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    v4f32 src_b, src_b0, src_b1, src_b2, src_b3;
    v4f32 src_c0, src_c1, src_c2, src_c3, res_c0, res_c1, res_c2, res_c3;
    v4f32 src_a, src_a0, src_a4, src_a5, src_a8, src_a9, src_a10, src_a12;
    v4f32 src_a13, src_a14, src_a15;
    FLOAT *c_nxt1line = c + ldc;
    FLOAT *c_nxt2line = c + 2 * ldc;
    FLOAT *c_nxt3line = c + 3 * ldc;

    src_c0 = LD_SP(c);
    src_c1 = LD_SP(c_nxt1line);
    src_c2 = LD_SP(c_nxt2line);
    src_c3 = LD_SP(c_nxt3line);

    for (k = 0; k < (bk >> 1); k++)
    {
        src_a0 = LD_SP(aa);

        src_b = LD_SP(bb);
        SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);
        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a0 * src_b1;
        src_c2 -= src_a0 * src_b2;
        src_c3 -= src_a0 * src_b3;

        aa += 4;
        bb += 4;

        src_a0 = LD_SP(aa);

        src_b = LD_SP(bb);
        SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);
        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a0 * src_b1;
        src_c2 -= src_a0 * src_b2;
        src_c3 -= src_a0 * src_b3;

        aa += 4;
        bb += 4;
    }

    if ((bk & 1) && (bk > 0))
    {
        src_a0 = LD_SP(aa);

        src_b = LD_SP(bb);
        SPLATI_W4_SP(src_b, src_b0, src_b1, src_b2, src_b3);
        src_c0 -= src_a0 * src_b0;
        src_c1 -= src_a0 * src_b1;
        src_c2 -= src_a0 * src_b2;
        src_c3 -= src_a0 * src_b3;
    }

    a -= 16;
    b -= 16;

    TRANSPOSE4x4_SP_SP(src_c0, src_c1, src_c2, src_c3,
                       res_c0, res_c1, res_c2, res_c3);

    src_a = LD_SP(a + 12);
    SPLATI_W4_SP(src_a, src_a12, src_a13, src_a14, src_a15);
    src_a8 = LD_SP(a + 8);
    src_a10 = (v4f32) __msa_splati_w((v4i32) src_a8, 2);
    src_a9 = (v4f32) __msa_splati_w((v4i32) src_a8, 1);
    src_a8 = (v4f32) __msa_splati_w((v4i32) src_a8, 0);
    src_a5 = COPY_FLOAT_TO_VECTOR(*(a + 5));
    src_a4 = COPY_FLOAT_TO_VECTOR(*(a + 4));
    src_a0 = COPY_FLOAT_TO_VECTOR(*(a + 0));

    res_c3 *= src_a15;
    res_c2 -= res_c3 * src_a14;
    res_c1 -= res_c3 * src_a13;
    res_c0 -= res_c3 * src_a12;

    res_c2 *= src_a10;
    res_c1 -= res_c2 * src_a9;
    res_c0 -= res_c2 * src_a8;

    res_c1 *= src_a5;
    res_c0 -= res_c1 * src_a4;

    res_c0 *= src_a0;

    ST_SP4(res_c0, res_c1, res_c2, res_c3, b, 4);

    TRANSPOSE4x4_SP_SP(res_c0, res_c1, res_c2, res_c3,
                       src_c0, src_c1, src_c2, src_c3);

    ST_SP(src_c0, c);
    ST_SP(src_c1, c_nxt1line);
    ST_SP(src_c2, c_nxt2line);
    ST_SP(src_c3, c_nxt3line);
}

static void ssolve_4x2_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    FLOAT a0, a4, a5, a8, a9, a10, a12, a13, a14, a15;
    FLOAT c0, c1, c2, c3, c0_nxt, c1_nxt, c2_nxt, c3_nxt;

    c0 = *(c + 0);
    c1 = *(c + 1);
    c2 = *(c + 2);
    c3 = *(c + 3);
    c0_nxt = *(c + 0 + ldc);
    c1_nxt = *(c + 1 + ldc);
    c2_nxt = *(c + 2 + ldc);
    c3_nxt = *(c + 3 + ldc);

    for (k = 0; k < bk; k++)
    {
        c0 -= aa[0] * bb[0];
        c1 -= aa[1] * bb[0];
        c2 -= aa[2] * bb[0];
        c3 -= aa[3] * bb[0];
        c0_nxt -= aa[0] * bb[1];
        c1_nxt -= aa[1] * bb[1];
        c2_nxt -= aa[2] * bb[1];
        c3_nxt -= aa[3] * bb[1];

        aa += 4;
        bb += 2;
    }

    a -= 16;
    b -= 8;

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
    c3_nxt *= a15;

    c2 -= c3 * a14;
    c2_nxt -= c3_nxt * a14;

    c2 *= a10;
    c2_nxt *= a10;

    c1 -= c3 * a13;
    c1_nxt -= c3_nxt * a13;

    c1 -= c2 * a9;
    c1_nxt -= c2_nxt * a9;

    c1 *= a5;
    c1_nxt *= a5;

    c0 -= c3 * a12;
    c0_nxt -= c3_nxt * a12;

    c0 -= c2 * a8;
    c0_nxt -= c2_nxt * a8;

    c0 -= c1 * a4;
    c0_nxt -= c1_nxt * a4;

    c0 *= a0;
    c0_nxt *= a0;

    *(b + 0) = c0;
    *(b + 1) = c0_nxt;
    *(b + 2) = c1;
    *(b + 3) = c1_nxt;
    *(b + 4) = c2;
    *(b + 5) = c2_nxt;
    *(b + 6) = c3;
    *(b + 7) = c3_nxt;

    *(c + 0) = c0;
    *(c + 1) = c1;
    *(c + 2) = c2;
    *(c + 3) = c3;
    *(c + 0 + ldc) = c0_nxt;
    *(c + 1 + ldc) = c1_nxt;
    *(c + 2 + ldc) = c2_nxt;
    *(c + 3 + ldc) = c3_nxt;
}

static void ssolve_4x1_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    FLOAT a0, a4, a5, a8, a9, a10, a12, a13, a14, a15, c0, c1, c2, c3;

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

static void ssolve_2x8_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    FLOAT a0, a2, a3, c0, c1, c0_nxt1, c1_nxt1, c0_nxt2, c1_nxt2, c0_nxt3;
    FLOAT c1_nxt3, c0_nxt4, c1_nxt4, c0_nxt5, c1_nxt5, c0_nxt6, c1_nxt6;
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

    a -= 4;
    b -= 16;

    a0 = *(a + 0);
    a2 = *(a + 2);
    a3 = *(a + 3);

    c1 *= a3;
    c1_nxt1 *= a3;
    c1_nxt2 *= a3;
    c1_nxt3 *= a3;
    c1_nxt4 *= a3;
    c1_nxt5 *= a3;
    c1_nxt6 *= a3;
    c1_nxt7 *= a3;

    c0 -= c1 * a2;
    c0_nxt1 -= c1_nxt1 * a2;
    c0_nxt2 -= c1_nxt2 * a2;
    c0_nxt3 -= c1_nxt3 * a2;
    c0_nxt4 -= c1_nxt4 * a2;
    c0_nxt5 -= c1_nxt5 * a2;
    c0_nxt6 -= c1_nxt6 * a2;
    c0_nxt7 -= c1_nxt7 * a2;

    c0 *= a0;
    c0_nxt1 *= a0;
    c0_nxt2 *= a0;
    c0_nxt3 *= a0;
    c0_nxt4 *= a0;
    c0_nxt5 *= a0;
    c0_nxt6 *= a0;
    c0_nxt7 *= a0;

    *(b + 0) = c0;
    *(b + 1) = c0_nxt1;
    *(b + 2) = c0_nxt2;
    *(b + 3) = c0_nxt3;
    *(b + 4) = c0_nxt4;
    *(b + 5) = c0_nxt5;
    *(b + 6) = c0_nxt6;
    *(b + 7) = c0_nxt7;
    *(b + 8) = c1;
    *(b + 9)  = c1_nxt1;
    *(b + 10) = c1_nxt2;
    *(b + 11) = c1_nxt3;
    *(b + 12) = c1_nxt4;
    *(b + 13) = c1_nxt5;
    *(b + 14) = c1_nxt6;
    *(b + 15) = c1_nxt7;

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

static void ssolve_2x4_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
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

    a -= 4;
    b -= 8;

    a0 = *(a + 0);
    a2 = *(a + 2);
    a3 = *(a + 3);

    c1 *= a3;
    c1_nxt1 *= a3;
    c1_nxt2 *= a3;
    c1_nxt3 *= a3;

    c0 -= c1 * a2;
    c0_nxt1 -= c1_nxt1 * a2;
    c0_nxt2 -= c1_nxt2 * a2;
    c0_nxt3 -= c1_nxt3 * a2;

    c0 *= a0;
    c0_nxt1 *= a0;
    c0_nxt2 *= a0;
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

static void ssolve_2x2_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    FLOAT a0, a2, a3, c0, c1, c0_nxt, c1_nxt;

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

    a0 = *(a + 0);
    a2 = *(a + 2);
    a3 = *(a + 3);

    c1 *= a3;
    c1_nxt *= a3;

    c0 -= c1 * a2;
    c0_nxt -= c1_nxt * a2;

    c0 *= a0;
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

static void ssolve_2x1_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    FLOAT a0, a2, a3, c0, c1;

    c0 = *(c + 0);
    c1 = *(c + 1);

    for (k = 0; k < bk; k++)
    {
        c0 -= aa[0] * bb[0];
        c1 -= aa[1] * bb[0];

        aa += 2;
        bb += 1;
    }

    a -= 4;
    b -= 2;

    a0 = *(a + 0);
    a2 = *(a + 2);
    a3 = *(a + 3);

    c1 *= a3;

    c0 -= c1 * a2;
    c0 *= a0;

    *(b + 0) = c0;
    *(b + 1) = c1;

    *(c + 0) = c0;
    *(c + 1) = c1;
}

static void ssolve_1x8_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    FLOAT a0, c0, c1, c2, c3, c4, c5, c6, c7;

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

    a0 = *(a - 1);

    c0 *= a0;
    c1 *= a0;
    c2 *= a0;
    c3 *= a0;
    c4 *= a0;
    c5 *= a0;
    c6 *= a0;
    c7 *= a0;

    *(b - 8) = c0;
    *(b - 7) = c1;
    *(b - 6) = c2;
    *(b - 5) = c3;
    *(b - 4) = c4;
    *(b - 3) = c5;
    *(b - 2) = c6;
    *(b - 1) = c7;

    *(c + 0 * ldc) = c0;
    *(c + 1 * ldc) = c1;
    *(c + 2 * ldc) = c2;
    *(c + 3 * ldc) = c3;
    *(c + 4 * ldc) = c4;
    *(c + 5 * ldc) = c5;
    *(c + 6 * ldc) = c6;
    *(c + 7 * ldc) = c7;
}

static void ssolve_1x4_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    FLOAT a0, c0, c1, c2, c3;

    c0 = *(c + 0 * ldc);
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

    a0 = *(a - 1);

    c0 *= a0;
    c1 *= a0;
    c2 *= a0;
    c3 *= a0;

    *(b - 4) = c0;
    *(b - 3) = c1;
    *(b - 2) = c2;
    *(b - 1) = c3;

    *(c + 0 * ldc) = c0;
    *(c + 1 * ldc) = c1;
    *(c + 2 * ldc) = c2;
    *(c + 3 * ldc) = c3;
}

static void ssolve_1x2_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG ldc, BLASLONG bk)
{
    BLASLONG k;
    FLOAT *aa = a, *bb = b;
    FLOAT a0, c0, c1;

    c0 = *c;
    c1 = *(c + ldc);

    for (k = 0; k < bk; k++)
    {
        c0 -= aa[0] * bb[0];
        c1 -= aa[0] * bb[1];

        aa += 1;
        bb += 2;
    }

    a0 = *(a - 1);

    c0 *= a0;
    c1 *= a0;

    *(b - 2) = c0;
    *(b - 1) = c1;

    *(c + 0 * ldc) = c0;
    *(c + 1 * ldc) = c1;
}

static void ssolve_1x1_ln_msa(FLOAT *a, FLOAT *b, FLOAT *c, BLASLONG bk)
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

    for (j = (n >> 3); j--;)
    {
        kk = m + offset;
        if (m & 7)
        {
            if (m & 1)
            {
                aa = a + (m - 1) * k + kk;
                cc = c + (m - 1);

                ssolve_1x8_ln_msa(aa, b + 8 * kk, cc, ldc, (k - kk));

                kk -= 1;
            }

            if (m & 2)
            {
                aa = a + ((m & ~1) - 2) * k + 2 * kk;
                cc = c + ((m & ~1) - 2);

                ssolve_2x8_ln_msa(aa, b + 8 * kk, cc, ldc, (k - kk));

                kk -= 2;
            }

            if (m & 4)
            {
                aa = a + ((m & ~3) - 4) * k + 4 * kk;
                cc = c + ((m & ~3) - 4);

                ssolve_4x8_ln_msa(aa, b + 8 * kk, cc, ldc, (k - kk));

                kk -= 4;
            }
        }

        i = (m >> 3);
        if (i > 0)
        {
            aa = a + ((m & ~7) - 8) * k;
            cc = c + ((m & ~7) - 8);

            do
            {
                ssolve_8x8_ln_msa(aa + 8 * kk, b + 8 * kk, cc, ldc, (k - kk));

                aa -= 8 * k;
                cc -= 8;
                kk -= 8;
                i --;
            } while (i > 0);
        }

        b += 8 * k;
        c += 8 * ldc;
    }

    if (n & 7)
    {
        if (n & 4)
        {
            kk = m + offset;

            if (m & 7)
            {
                if (m & 1)
                {
                    aa = a + (m - 1) * k + kk;
                    cc = c + (m - 1);

                    ssolve_1x4_ln_msa(aa, b + 4 * kk, cc, ldc, (k - kk));

                    kk -= 1;
                }

                if (m & 2)
                {
                    aa = a + ((m & ~1) - 2) * k + 2 * kk;
                    cc = c + ((m & ~1) - 2);

                    ssolve_2x4_ln_msa(aa, b + 4 * kk, cc, ldc, (k - kk));

                    kk -= 2;
                }

                if (m & 4)
                {
                    aa = a + ((m & ~3) - 4) * k + 4 * kk;
                    cc = c + ((m & ~3) - 4);

                    ssolve_4x4_ln_msa(aa, b + 4 * kk, cc, ldc, (k - kk));

                    kk -= 4;
                }
            }

            i = (m >> 3);
            if (i > 0)
            {
                aa = a + ((m & ~7) - 8) * k;
                cc = c + ((m & ~7) - 8);

                do
                {
                    ssolve_8x4_ln_msa(aa + 8 * kk, b + 4 * kk, cc, ldc, (k - kk));

                    aa -= 8 * k;
                    cc -= 8;
                    kk -= 8;
                    i --;
                } while (i > 0);
            }

            b += 4 * k;
            c += 4 * ldc;
        }

        if (n & 2)
        {
            kk = m + offset;

            if (m & 7)
            {
                if (m & 1)
                {
                    aa = a + (m - 1) * k + kk;
                    cc = c + (m - 1);

                    ssolve_1x2_ln_msa(aa, b + 2 * kk, cc, ldc, (k - kk));

                    kk -= 1;
                }

                if (m & 2)
                {
                    aa = a + ((m & ~1) - 2) * k + 2 * kk;
                    cc = c + ((m & ~1) - 2);

                    ssolve_2x2_ln_msa(aa, b + 2 * kk, cc, ldc, (k - kk));

                    kk -= 2;
                }

                if (m & 4)
                {
                    aa = a + ((m & ~3) - 4) * k + 4 * kk;
                    cc = c + ((m & ~3) - 4);

                    ssolve_4x2_ln_msa(aa, b + 2 * kk, cc, ldc, (k - kk));

                    kk -= 4;
                }
            }

            i = (m >> 3);
            if (i > 0)
            {
                aa = a + ((m & ~7) - 8) * k;
                cc = c + ((m & ~7) - 8);

                do
                {
                    ssolve_8x2_ln_msa(aa + 8 * kk, b + 2 * kk, cc, ldc, (k - kk));

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
                    aa = a + (m - 1) * k + kk;
                    cc = c + (m - 1);

                    ssolve_1x1_ln_msa(aa, b + kk, cc, (k - kk));

                    kk -= 1;
                }

                if (m & 2)
                {
                    aa = a + ((m & ~1) - 2) * k + 2 * kk;
                    cc = c + ((m & ~1) - 2);

                    ssolve_2x1_ln_msa(aa, b + kk, cc, (k - kk));

                    kk -= 2;
                }

                if (m & 4)
                {
                    aa = a + ((m & ~3) - 4) * k + 4 * kk;
                    cc = c + ((m & ~3) - 4);

                    ssolve_4x1_ln_msa(aa, b + kk, cc, (k - kk));

                    kk -= 4;
                }
            }

            i = (m >> 3);
            if (i > 0)
            {
                aa = a + ((m & ~7) - 8) * k;
                cc = c + ((m & ~7) - 8);

                do
                {
                    ssolve_8x1_ln_msa(aa + 8 * kk, b + kk, cc, (k - kk));

                    aa -= 8 * k;
                    cc -= 8;
                    kk -= 8;
                    i --;
                } while (i > 0);
            }

            b += k;
            c += ldc;
        }
    }

    return 0;
}
