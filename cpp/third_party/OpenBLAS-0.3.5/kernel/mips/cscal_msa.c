/*******************************************************************************
Copyright (c) 2017, The OpenBLAS Project
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

/* This will shuffle the elements in 'in' vector as (mask needed :: 10 11 00 01)
   0  1  2  3  =>  1  0  3  2 */
#define SHF_177   177

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da_r, FLOAT da_i,
          FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy,
          BLASLONG dummy2)
{
    BLASLONG i, inc_x2;
    FLOAT *px;
    FLOAT tp0, tp1, tp2, tp3, f0, f1, f2, f3;
    v4f32 x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15;
    v4f32 d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15;
    v4f32 da_i_vec, da_i_vec_neg, da_r_vec;

    px = x;

    if (1 == inc_x)
    {
        if ((0.0 == da_r) && (0.0 == da_i))
        {
            v4f32 zero_v = __msa_cast_to_vector_float(0);
            zero_v = (v4f32) __msa_insert_w((v4i32) zero_v, 0, 0.0);
            zero_v = (v4f32) __msa_insert_w((v4i32) zero_v, 1, 0.0);
            zero_v = (v4f32) __msa_insert_w((v4i32) zero_v, 2, 0.0);
            zero_v = (v4f32) __msa_insert_w((v4i32) zero_v, 3, 0.0);

            for (i = (n >> 5); i--;)
            {
                ST_SP8_INC(zero_v, zero_v, zero_v, zero_v, zero_v, zero_v,
                           zero_v, zero_v, x, 4);
                ST_SP8_INC(zero_v, zero_v, zero_v, zero_v, zero_v, zero_v,
                           zero_v, zero_v, x, 4);
            }

            if (n & 31)
            {
                if (n & 16)
                {
                    ST_SP8_INC(zero_v, zero_v, zero_v, zero_v, zero_v, zero_v,
                               zero_v, zero_v, x, 4);
                }

                if (n & 8)
                {
                    ST_SP4_INC(zero_v, zero_v, zero_v, zero_v, x, 4);
                }

                if (n & 4)
                {
                    ST_SP2_INC(zero_v, zero_v, x, 4);
                }

                if (n & 2)
                {
                    ST_SP(zero_v, x); x += 4;
                }

                if (n & 1)
                {
                    *x = 0; x += 1;
                    *x = 0;
                }
            }
        }
        else if (0.0 == da_r)
        {
            da_i_vec = COPY_FLOAT_TO_VECTOR(da_i);
            da_i_vec_neg = -da_i_vec;
            da_i_vec = (v4f32) __msa_ilvev_w((v4i32) da_i_vec_neg, (v4i32) da_i_vec);

            if (n > 31)
            {
                FLOAT *x_pref;
                BLASLONG pref_offset;

                pref_offset = (BLASLONG)x & (L1_DATA_LINESIZE - 1);
                if (pref_offset > 0)
                {
                    pref_offset = L1_DATA_LINESIZE - pref_offset;
                    pref_offset = pref_offset / sizeof(FLOAT);
                }
                x_pref = x + pref_offset + 64 + 32;

                LD_SP8_INC(px, 4, x0, x1, x2, x3, x4, x5, x6, x7);
                for (i = (n >> 5)- 1; i--;)
                {
                    PREF_OFFSET(x_pref, 0);
                    PREF_OFFSET(x_pref, 32);
                    PREF_OFFSET(x_pref, 64);
                    PREF_OFFSET(x_pref, 96);
                    PREF_OFFSET(x_pref, 128);
                    PREF_OFFSET(x_pref, 160);
                    PREF_OFFSET(x_pref, 192);
                    PREF_OFFSET(x_pref, 224);
                    x_pref += 64;

                    x8 = LD_SP(px); px += 4;
                    x0 *= da_i_vec;
                    x9 = LD_SP(px); px += 4;
                    x1 *= da_i_vec;
                    x10 = LD_SP(px); px += 4;
                    x2 *= da_i_vec;
                    x11 = LD_SP(px); px += 4;
                    x3 *= da_i_vec;
                    x12 = LD_SP(px); px += 4;
                    x4 *= da_i_vec;
                    x13 = LD_SP(px); px += 4;
                    x5 *= da_i_vec;
                    x0 = (v4f32) __msa_shf_w((v4i32) x0, SHF_177);
                    x14 = LD_SP(px); px += 4;
                    x6 *= da_i_vec;
                    x1 = (v4f32) __msa_shf_w((v4i32) x1, SHF_177);
                    x15 = LD_SP(px); px += 4;
                    x7 *= da_i_vec;
                    x2 = (v4f32) __msa_shf_w((v4i32) x2, SHF_177);
                    x8 *= da_i_vec;
                    x3 = (v4f32) __msa_shf_w((v4i32) x3, SHF_177);
                    ST_SP(x0, x); x += 4;
                    x9 *= da_i_vec;
                    x4 = (v4f32) __msa_shf_w((v4i32) x4, SHF_177);
                    ST_SP(x1, x); x += 4;
                    x10 *= da_i_vec;
                    x5 = (v4f32) __msa_shf_w((v4i32) x5, SHF_177);
                    ST_SP(x2, x); x += 4;
                    x11 *= da_i_vec;
                    x6 = (v4f32) __msa_shf_w((v4i32) x6, SHF_177);
                    ST_SP(x3, x); x += 4;
                    x12 *= da_i_vec;
                    x7 = (v4f32) __msa_shf_w((v4i32) x7, SHF_177);
                    ST_SP(x4, x); x += 4;
                    x13 *= da_i_vec;
                    x8 = (v4f32) __msa_shf_w((v4i32) x8, SHF_177);
                    ST_SP(x5, x); x += 4;
                    x14 *= da_i_vec;
                    x9 = (v4f32) __msa_shf_w((v4i32) x9, SHF_177);
                    ST_SP(x6, x); x += 4;
                    x15 *= da_i_vec;
                    x10 = (v4f32) __msa_shf_w((v4i32) x10, SHF_177);
                    ST_SP(x7, x); x += 4;
                    x11 = (v4f32) __msa_shf_w((v4i32) x11, SHF_177);
                    ST_SP(x8, x); x += 4;
                    x0 = LD_SP(px); px += 4;
                    x12 = (v4f32) __msa_shf_w((v4i32) x12, SHF_177);
                    ST_SP(x9, x); x += 4;
                    x1 = LD_SP(px); px += 4;
                    x13 = (v4f32) __msa_shf_w((v4i32) x13, SHF_177);
                    ST_SP(x10, x); x += 4;
                    x2 = LD_SP(px); px += 4;
                    x14 = (v4f32) __msa_shf_w((v4i32) x14, SHF_177);
                    ST_SP(x11, x); x += 4;
                    x3 = LD_SP(px); px += 4;
                    x15 = (v4f32) __msa_shf_w((v4i32) x15, SHF_177);
                    ST_SP(x12, x); x += 4;
                    x4 = LD_SP(px); px += 4;
                    ST_SP(x13, x); x += 4;
                    x5 = LD_SP(px); px += 4;
                    ST_SP(x14, x); x += 4;
                    x6 = LD_SP(px); px += 4;
                    ST_SP(x15, x); x += 4;
                    x7 = LD_SP(px); px += 4;
                }

                LD_SP8_INC(px, 4, x8, x9, x10, x11, x12, x13, x14, x15);
                MUL4(x0, da_i_vec, x1, da_i_vec, x2, da_i_vec, x3, da_i_vec,
                     x0, x1, x2, x3);
                MUL4(x4, da_i_vec, x5, da_i_vec, x6, da_i_vec, x7, da_i_vec,
                     x4, x5, x6, x7);
                MUL4(x8, da_i_vec, x9, da_i_vec, x10, da_i_vec, x11, da_i_vec,
                     x8, x9, x10, x11);
                MUL4(x12, da_i_vec, x13, da_i_vec, x14, da_i_vec, x15, da_i_vec,
                     x12, x13, x14, x15);
                SHF_W4_SP(x0, x1, x2, x3, x0, x1, x2, x3, SHF_177);
                SHF_W4_SP(x4, x5, x6, x7, x4, x5, x6, x7, SHF_177);
                SHF_W4_SP(x8, x9, x10, x11, x8, x9, x10, x11, SHF_177);
                SHF_W4_SP(x12, x13, x14, x15, x12, x13, x14, x15, SHF_177);
                ST_SP16_INC(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11,
                            x12, x13, x14, x15, x, 4);
            }

            if (n & 31)
            {
                if (n & 16)
                {
                    LD_SP8_INC(px, 4, x0, x1, x2, x3, x4, x5, x6, x7);
                    MUL4(x0, da_i_vec, x1, da_i_vec, x2, da_i_vec, x3, da_i_vec,
                         x0, x1, x2, x3);
                    MUL4(x4, da_i_vec, x5, da_i_vec, x6, da_i_vec, x7, da_i_vec,
                         x4, x5, x6, x7);
                    SHF_W4_SP(x0, x1, x2, x3, x0, x1, x2, x3, SHF_177);
                    SHF_W4_SP(x4, x5, x6, x7, x4, x5, x6, x7, SHF_177);
                    ST_SP8_INC(x0, x1, x2, x3, x4, x5, x6, x7, x, 4);
                }

                if (n & 8)
                {
                    LD_SP4_INC(px, 4, x0, x1, x2, x3);
                    MUL4(x0, da_i_vec, x1, da_i_vec, x2, da_i_vec, x3, da_i_vec,
                         x0, x1, x2, x3);
                    SHF_W4_SP(x0, x1, x2, x3, x0, x1, x2, x3, SHF_177);
                    ST_SP4_INC(x0, x1, x2, x3, x, 4);
                }

                if (n & 4)
                {
                    LD_SP2_INC(px, 4, x0, x1);
                    MUL2(x0, da_i_vec, x1, da_i_vec, x0, x1);
                    SHF_W2_SP(x0, x1, x0, x1, SHF_177);
                    ST_SP2_INC(x0, x1, x, 4);
                }

                if (n & 2)
                {
                    LD_GP4_INC(px, 1, f0, f1, f2, f3);
                    MUL4(f0, da_i, f1, -da_i, f2, da_i, f3, -da_i,
                         f0, f1, f2, f3);
                    ST_GP4_INC(f1, f0, f3, f2, x, 1);
                }

                if (n & 1)
                {
                    LD_GP2_INC(px, 1, f0, f1);
                    MUL2(f0, da_i, f1, -da_i, f0, f1);
                    ST_GP2_INC(f1, f0, x, 1);
                }
            }
        }
        else if (0.0 == da_i)
        {
            da_r_vec = COPY_FLOAT_TO_VECTOR(da_r);

            if (n > 31)
            {
                FLOAT *x_pref;
                BLASLONG pref_offset;

                pref_offset = (BLASLONG)x & (L1_DATA_LINESIZE - 1);
                if (pref_offset > 0)
                {
                    pref_offset = L1_DATA_LINESIZE - pref_offset;
                    pref_offset = pref_offset / sizeof(FLOAT);
                }
                x_pref = x + pref_offset + 64 + 32;

                LD_SP8_INC(px, 4, x0, x1, x2, x3, x4, x5, x6, x7);
                for (i = (n >> 5)- 1; i--;)
                {
                    PREF_OFFSET(x_pref, 0);
                    PREF_OFFSET(x_pref, 32);
                    PREF_OFFSET(x_pref, 64);
                    PREF_OFFSET(x_pref, 96);
                    PREF_OFFSET(x_pref, 128);
                    PREF_OFFSET(x_pref, 160);
                    PREF_OFFSET(x_pref, 192);
                    PREF_OFFSET(x_pref, 224);
                    x_pref += 64;

                    x8 = LD_SP(px); px += 4;
                    x0 *= da_r_vec;
                    x9 = LD_SP(px); px += 4;
                    x1 *= da_r_vec;
                    x10 = LD_SP(px); px += 4;
                    x2 *= da_r_vec;
                    x11 = LD_SP(px); px += 4;
                    x3 *= da_r_vec;
                    x12 = LD_SP(px); px += 4;
                    x4 *= da_r_vec;
                    x13 = LD_SP(px); px += 4;
                    x5 *= da_r_vec;
                    ST_SP(x0, x); x += 4;
                    x14 = LD_SP(px); px += 4;
                    x6 *= da_r_vec;
                    ST_SP(x1, x); x += 4;
                    x15 = LD_SP(px); px += 4;
                    x7 *= da_r_vec;
                    ST_SP(x2, x); x += 4;
                    x8 *= da_r_vec;
                    ST_SP(x3, x); x += 4;
                    x9 *= da_r_vec;
                    ST_SP(x4, x); x += 4;
                    x10 *= da_r_vec;
                    ST_SP(x5, x); x += 4;
                    x11 *= da_r_vec;
                    ST_SP(x6, x); x += 4;
                    x12 *= da_r_vec;
                    ST_SP(x7, x); x += 4;
                    x13 *= da_r_vec;
                    ST_SP(x8, x); x += 4;
                    x0 = LD_SP(px); px += 4;
                    x14 *= da_r_vec;
                    ST_SP(x9, x); x += 4;
                    x1 = LD_SP(px); px += 4;
                    x15 *= da_r_vec;
                    ST_SP(x10, x); x += 4;
                    x2 = LD_SP(px); px += 4;
                    ST_SP(x11, x); x += 4;
                    x3 = LD_SP(px); px += 4;
                    ST_SP(x12, x); x += 4;
                    x4 = LD_SP(px); px += 4;
                    ST_SP(x13, x); x += 4;
                    x5 = LD_SP(px); px += 4;
                    ST_SP(x14, x); x += 4;
                    x6 = LD_SP(px); px += 4;
                    ST_SP(x15, x); x += 4;
                    x7 = LD_SP(px); px += 4;
                }

                LD_SP8_INC(px, 4, x8, x9, x10, x11, x12, x13, x14, x15);
                MUL4(x0, da_r_vec, x1, da_r_vec, x2, da_r_vec, x3, da_r_vec,
                     x0, x1, x2, x3);
                MUL4(x4, da_r_vec, x5, da_r_vec, x6, da_r_vec, x7, da_r_vec,
                     x4, x5, x6, x7);
                MUL4(x8, da_r_vec, x9, da_r_vec, x10, da_r_vec, x11, da_r_vec,
                     x8, x9, x10, x11);
                MUL4(x12, da_r_vec, x13, da_r_vec, x14, da_r_vec, x15, da_r_vec,
                     x12, x13, x14, x15);
                ST_SP16_INC(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11,
                            x12, x13, x14, x15, x, 4);
            }

            if (n & 31)
            {
                if (n & 16)
                {
                    LD_SP8_INC(px, 4, x0, x1, x2, x3, x4, x5, x6, x7);
                    MUL4(x0, da_r_vec, x1, da_r_vec, x2, da_r_vec, x3, da_r_vec,
                         x0, x1, x2, x3);
                    MUL4(x4, da_r_vec, x5, da_r_vec, x6, da_r_vec, x7, da_r_vec,
                         x4, x5, x6, x7);
                    ST_SP8_INC(x0, x1, x2, x3, x4, x5, x6, x7, x, 4);
                }

                if (n & 8)
                {
                    LD_SP4_INC(px, 4, x0, x1, x2, x3);
                    MUL4(x0, da_r_vec, x1, da_r_vec, x2, da_r_vec, x3, da_r_vec,
                         x0, x1, x2, x3);
                    ST_SP4_INC(x0, x1, x2, x3, x, 4);
                }

                if (n & 4)
                {
                    LD_SP2_INC(px, 4, x0, x1);
                    MUL2(x0, da_r_vec, x1, da_r_vec, x0, x1);
                    ST_SP2_INC(x0, x1, x, 4);
                }

                if (n & 2)
                {
                    LD_GP4_INC(px, 1, f0, f1, f2, f3);
                    MUL4(f0, da_r, f1, da_r, f2, da_r, f3, da_r, f0, f1, f2, f3);
                    ST_GP4_INC(f0, f1, f2, f3, x, 1);
                }

                if (n & 1)
                {
                    LD_GP2_INC(px, 1, f0, f1);
                    MUL2(f0, da_r, f1, da_r, f0, f1);
                    ST_GP2_INC(f0, f1, x, 1);
                }
            }
        }
        else
        {
            FLOAT *x_pref;
            BLASLONG pref_offset;

            pref_offset = (BLASLONG)x & (L1_DATA_LINESIZE - 1);
            if (pref_offset > 0)
            {
                pref_offset = L1_DATA_LINESIZE - pref_offset;
                pref_offset = pref_offset / sizeof(FLOAT);
            }
            x_pref = x + pref_offset + 64;

            da_i_vec = COPY_FLOAT_TO_VECTOR(da_i);
            da_i_vec_neg = -da_i_vec;
            da_i_vec = (v4f32) __msa_ilvev_w((v4i32) da_i_vec_neg, (v4i32) da_i_vec);

            da_r_vec = COPY_FLOAT_TO_VECTOR(da_r);

            for (i = (n >> 5); i--;)
            {
                PREF_OFFSET(x_pref, 0);
                PREF_OFFSET(x_pref, 32);
                PREF_OFFSET(x_pref, 64);
                PREF_OFFSET(x_pref, 96);
                PREF_OFFSET(x_pref, 128);
                PREF_OFFSET(x_pref, 160);
                PREF_OFFSET(x_pref, 192);
                PREF_OFFSET(x_pref, 224);
                x_pref += 64;

                LD_SP16_INC(px, 4, x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10,
                            x11, x12, x13, x14, x15);
                MUL4(x0, da_i_vec, x1, da_i_vec, x2, da_i_vec, x3, da_i_vec,
                     d0, d1, d2, d3);
                MUL4(x4, da_i_vec, x5, da_i_vec, x6, da_i_vec, x7, da_i_vec,
                     d4, d5, d6, d7);
                MUL4(x8, da_i_vec, x9, da_i_vec, x10, da_i_vec, x11, da_i_vec,
                     d8, d9, d10, d11);
                MUL4(x12, da_i_vec, x13, da_i_vec, x14, da_i_vec, x15, da_i_vec,
                     d12, d13, d14, d15);
                SHF_W4_SP(d0, d1, d2, d3, d0, d1, d2, d3, SHF_177);
                SHF_W4_SP(d4, d5, d6, d7, d4, d5, d6, d7, SHF_177);
                SHF_W4_SP(d8, d9, d10, d11, d8, d9, d10, d11, SHF_177);
                SHF_W4_SP(d12, d13, d14, d15, d12, d13, d14, d15, SHF_177);
                FMADD4(x0, x1, x2, x3, da_r_vec, d0, d1, d2, d3);
                FMADD4(x4, x5, x6, x7, da_r_vec, d4, d5, d6, d7);
                FMADD4(x8, x9, x10, x11, da_r_vec, d8, d9, d10, d11);
                FMADD4(x12, x13, x14, x15, da_r_vec, d12, d13, d14, d15);
                ST_SP16_INC(d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11,
                            d12, d13, d14, d15, x, 4);
            }

            if (n & 31)
            {
                if (n & 16)
                {
                    LD_SP8_INC(px, 4, x0, x1, x2, x3, x4, x5, x6, x7);
                    MUL4(x0, da_i_vec, x1, da_i_vec, x2, da_i_vec, x3, da_i_vec,
                         d0, d1, d2, d3);
                    MUL4(x4, da_i_vec, x5, da_i_vec, x6, da_i_vec, x7, da_i_vec,
                         d4, d5, d6, d7);
                    SHF_W4_SP(d0, d1, d2, d3, d0, d1, d2, d3, SHF_177);
                    SHF_W4_SP(d4, d5, d6, d7, d4, d5, d6, d7, SHF_177);
                    FMADD4(x0, x1, x2, x3, da_r_vec, d0, d1, d2, d3);
                    FMADD4(x4, x5, x6, x7, da_r_vec, d4, d5, d6, d7);
                    ST_SP8_INC(d0, d1, d2, d3, d4, d5, d6, d7, x, 4);
                }

                if (n & 8)
                {
                    LD_SP4_INC(px, 4, x0, x1, x2, x3);
                    MUL4(x0, da_i_vec, x1, da_i_vec, x2, da_i_vec, x3, da_i_vec,
                         d0, d1, d2, d3);
                    SHF_W4_SP(d0, d1, d2, d3, d0, d1, d2, d3, SHF_177);
                    FMADD4(x0, x1, x2, x3, da_r_vec, d0, d1, d2, d3);
                    ST_SP4_INC(d0, d1, d2, d3, x, 4);
                }

                if (n & 4)
                {
                    LD_SP2_INC(px, 4, x0, x1);
                    MUL2(x0, da_i_vec, x1, da_i_vec, d0, d1);
                    SHF_W2_SP(d0, d1, d0, d1, SHF_177);
                    FMADD2(x0, x1, da_r_vec, d0, d1);
                    ST_SP2_INC(d0, d1, x, 4);
                }

                if (n & 2)
                {
                    LD_GP4_INC(px, 1, f0, f1, f2, f3);

                    tp0 = da_r * f0;
                    tp0 -= da_i * f1;
                    tp1 = da_r * f1;
                    tp1 += da_i * f0;
                    tp2 = da_r * f2;
                    tp2 -= da_i * f3;
                    tp3 = da_r * f3;
                    tp3 += da_i * f2;

                    ST_GP4_INC(tp0, tp1, tp2, tp3, x, 1);
                }

                if (n & 1)
                {
                    LD_GP2_INC(px, 1, f0, f1);

                    tp0 = da_r * f0;
                    tp0 -= da_i * f1;
                    tp1 = da_r * f1;
                    tp1 += da_i * f0;

                    ST_GP2_INC(tp0, tp1, x, 1);
                }
            }
        }
    }
    else
    {
        inc_x2 = 2 * inc_x;

        if ((0.0 == da_r) && (0.0 == da_i))
        {
            for (i = n; i--;)
            {
                *x       = 0;
                *(x + 1) = 0;

                x += inc_x2;
            }
        }
        else if (0.0 == da_r)
        {
            da_i_vec = COPY_FLOAT_TO_VECTOR(da_i);
            da_i_vec_neg = -da_i_vec;
            da_i_vec = (v4f32) __msa_ilvev_w((v4i32) da_i_vec_neg, (v4i32) da_i_vec);

            for (i = (n >> 4); i--;)
            {
                LD_SP16_INC(px, inc_x2, x0, x1, x2, x3, x4, x5, x6, x7, x8, x9,
                            x10, x11, x12, x13, x14, x15);
                PCKEV_D4_SP(x1, x0, x3, x2, x5, x4, x7, x6, d0, d1, d2, d3);
                PCKEV_D4_SP(x9, x8, x11, x10, x13, x12, x15, x14, d4, d5, d6, d7);
                MUL4(d0, da_i_vec, d1, da_i_vec, d2, da_i_vec, d3, da_i_vec,
                     d0, d1, d2, d3);
                MUL4(d4, da_i_vec, d5, da_i_vec, d6, da_i_vec, d7, da_i_vec,
                     d4, d5, d6, d7);

                *x       = d0[1];
                *(x + 1) = d0[0];
                x += inc_x2;
                *x       = d0[3];
                *(x + 1) = d0[2];
                x += inc_x2;
                *x       = d1[1];
                *(x + 1) = d1[0];
                x += inc_x2;
                *x       = d1[3];
                *(x + 1) = d1[2];
                x += inc_x2;
                *x       = d2[1];
                *(x + 1) = d2[0];
                x += inc_x2;
                *x       = d2[3];
                *(x + 1) = d2[2];
                x += inc_x2;
                *x       = d3[1];
                *(x + 1) = d3[0];
                x += inc_x2;
                *x       = d3[3];
                *(x + 1) = d3[2];
                x += inc_x2;
                *x       = d4[1];
                *(x + 1) = d4[0];
                x += inc_x2;
                *x       = d4[3];
                *(x + 1) = d4[2];
                x += inc_x2;
                *x       = d5[1];
                *(x + 1) = d5[0];
                x += inc_x2;
                *x       = d5[3];
                *(x + 1) = d5[2];
                x += inc_x2;
                *x       = d6[1];
                *(x + 1) = d6[0];
                x += inc_x2;
                *x       = d6[3];
                *(x + 1) = d6[2];
                x += inc_x2;
                *x       = d7[1];
                *(x + 1) = d7[0];
                x += inc_x2;
                *x       = d7[3];
                *(x + 1) = d7[2];
                x += inc_x2;
            }

            if (n & 15)
            {
                if (n & 8)
                {
                    LD_SP8_INC(px, inc_x2, x0, x1, x2, x3, x4, x5, x6, x7);
                    PCKEV_D4_SP(x1, x0, x3, x2, x5, x4, x7, x6, d0, d1, d2, d3);
                    MUL4(d0, da_i_vec, d1, da_i_vec, d2, da_i_vec, d3, da_i_vec,
                         d0, d1, d2, d3);

                    *x       = d0[1];
                    *(x + 1) = d0[0];
                    x += inc_x2;
                    *x       = d0[3];
                    *(x + 1) = d0[2];
                    x += inc_x2;
                    *x       = d1[1];
                    *(x + 1) = d1[0];
                    x += inc_x2;
                    *x       = d1[3];
                    *(x + 1) = d1[2];
                    x += inc_x2;
                    *x       = d2[1];
                    *(x + 1) = d2[0];
                    x += inc_x2;
                    *x       = d2[3];
                    *(x + 1) = d2[2];
                    x += inc_x2;
                    *x       = d3[1];
                    *(x + 1) = d3[0];
                    x += inc_x2;
                    *x       = d3[3];
                    *(x + 1) = d3[2];
                    x += inc_x2;
                }

                if (n & 4)
                {
                    LD_SP4_INC(px, inc_x2, x0, x1, x2, x3);
                    PCKEV_D2_SP(x1, x0, x3, x2, d0, d1);
                    MUL2(d0, da_i_vec, d1, da_i_vec, d0, d1);

                    *x       = d0[1];
                    *(x + 1) = d0[0];
                    x += inc_x2;
                    *x       = d0[3];
                    *(x + 1) = d0[2];
                    x += inc_x2;
                    *x       = d1[1];
                    *(x + 1) = d1[0];
                    x += inc_x2;
                    *x       = d1[3];
                    *(x + 1) = d1[2];
                    x += inc_x2;
                }

                if (n & 2)
                {
                    f0 = *px;
                    f1 = *(px + 1);
                    px += inc_x2;
                    f2 = *px;
                    f3 = *(px + 1);
                    px += inc_x2;

                    MUL4(f0, da_i, f1, -da_i, f2, da_i, f3, -da_i, f0, f1, f2, f3);

                    *x       = f1;
                    *(x + 1) = f0;
                    x += inc_x2;
                    *x       = f3;
                    *(x + 1) = f2;
                    x += inc_x2;
                }

                if (n & 1)
                {
                    f0 = *x;
                    f1 = *(x + 1);

                    MUL2(f0, da_i, f1, -da_i, f0, f1);

                    *x       = f1;
                    *(x + 1) = f0;
                }
            }
        }
        else if (0.0 == da_i)
        {
            da_r_vec = COPY_FLOAT_TO_VECTOR(da_r);

            for (i = (n >> 4); i--;)
            {
                LD_SP16_INC(px, inc_x2, x0, x1, x2, x3, x4, x5, x6, x7, x8, x9,
                            x10, x11, x12, x13, x14, x15);
                PCKEV_D4_SP(x1, x0, x3, x2, x5, x4, x7, x6, d0, d1, d2, d3);
                PCKEV_D4_SP(x9, x8, x11, x10, x13, x12, x15, x14, d4, d5, d6, d7);
                MUL4(d0, da_r_vec, d1, da_r_vec, d2, da_r_vec, d3, da_r_vec,
                     d0, d1, d2, d3);
                MUL4(d4, da_r_vec, d5, da_r_vec, d6, da_r_vec, d7, da_r_vec,
                     d4, d5, d6, d7);

                *x       = d0[0];
                *(x + 1) = d0[1];
                x += inc_x2;
                *x       = d0[2];
                *(x + 1) = d0[3];
                x += inc_x2;
                *x       = d1[0];
                *(x + 1) = d1[1];
                x += inc_x2;
                *x       = d1[2];
                *(x + 1) = d1[3];
                x += inc_x2;
                *x       = d2[0];
                *(x + 1) = d2[1];
                x += inc_x2;
                *x       = d2[2];
                *(x + 1) = d2[3];
                x += inc_x2;
                *x       = d3[0];
                *(x + 1) = d3[1];
                x += inc_x2;
                *x       = d3[2];
                *(x + 1) = d3[3];
                x += inc_x2;
                *x       = d4[0];
                *(x + 1) = d4[1];
                x += inc_x2;
                *x       = d4[2];
                *(x + 1) = d4[3];
                x += inc_x2;
                *x       = d5[0];
                *(x + 1) = d5[1];
                x += inc_x2;
                *x       = d5[2];
                *(x + 1) = d5[3];
                x += inc_x2;
                *x       = d6[0];
                *(x + 1) = d6[1];
                x += inc_x2;
                *x       = d6[2];
                *(x + 1) = d6[3];
                x += inc_x2;
                *x       = d7[0];
                *(x + 1) = d7[1];
                x += inc_x2;
                *x       = d7[2];
                *(x + 1) = d7[3];
                x += inc_x2;
            }

            if (n & 15)
            {
                if (n & 8)
                {
                    LD_SP8_INC(px, inc_x2, x0, x1, x2, x3, x4, x5, x6, x7);
                    PCKEV_D4_SP(x1, x0, x3, x2, x5, x4, x7, x6, d0, d1, d2, d3);
                    MUL4(d0, da_r_vec, d1, da_r_vec, d2, da_r_vec, d3, da_r_vec,
                         d0, d1, d2, d3);

                    *x       = d0[0];
                    *(x + 1) = d0[1];
                    x += inc_x2;
                    *x       = d0[2];
                    *(x + 1) = d0[3];
                    x += inc_x2;
                    *x       = d1[0];
                    *(x + 1) = d1[1];
                    x += inc_x2;
                    *x       = d1[2];
                    *(x + 1) = d1[3];
                    x += inc_x2;
                    *x       = d2[0];
                    *(x + 1) = d2[1];
                    x += inc_x2;
                    *x       = d2[2];
                    *(x + 1) = d2[3];
                    x += inc_x2;
                    *x       = d3[0];
                    *(x + 1) = d3[1];
                    x += inc_x2;
                    *x       = d3[2];
                    *(x + 1) = d3[3];
                    x += inc_x2;
                }

                if (n & 4)
                {
                    LD_SP4_INC(px, inc_x2, x0, x1, x2, x3);
                    PCKEV_D2_SP(x1, x0, x3, x2, d0, d1);
                    MUL2(d0, da_r_vec, d1, da_r_vec, d0, d1);

                    *x       = d0[0];
                    *(x + 1) = d0[1];
                    x += inc_x2;
                    *x       = d0[2];
                    *(x + 1) = d0[3];
                    x += inc_x2;
                    *x       = d1[0];
                    *(x + 1) = d1[1];
                    x += inc_x2;
                    *x       = d1[2];
                    *(x + 1) = d1[3];
                    x += inc_x2;
                }

                if (n & 2)
                {
                    f0 = *px;
                    f1 = *(px + 1);
                    px += inc_x2;
                    f2 = *px;
                    f3 = *(px + 1);
                    px += inc_x2;

                    MUL4(f0, da_r, f1, da_r, f2, da_r, f3, da_r, f0, f1, f2, f3);

                    *x       = f0;
                    *(x + 1) = f1;
                    x += inc_x2;
                    *x       = f2;
                    *(x + 1) = f3;
                    x += inc_x2;
                }

                if (n & 1)
                {
                    f0 = *x;
                    f1 = *(x + 1);

                    MUL2(f0, da_r, f1, da_r, f0, f1);

                    *x       = f0;
                    *(x + 1) = f1;
                }
            }
        }
        else
        {
            da_i_vec = COPY_FLOAT_TO_VECTOR(da_i);
            da_i_vec_neg = -da_i_vec;
            da_i_vec = (v4f32) __msa_ilvev_w((v4i32) da_i_vec_neg, (v4i32) da_i_vec);

            da_r_vec = COPY_FLOAT_TO_VECTOR(da_r);

            for (i = (n >> 4); i--;)
            {
                LD_SP16_INC(px, inc_x2, x0, x1, x2, x3, x4, x5, x6, x7, x8, x9,
                            x10, x11, x12, x13, x14, x15);
                PCKEV_D4_SP(x1, x0, x3, x2, x5, x4, x7, x6, d0, d1, d2, d3);
                PCKEV_D4_SP(x9, x8, x11, x10, x13, x12, x15, x14, d4, d5, d6, d7);
                MUL4(d0, da_i_vec, d1, da_i_vec, d2, da_i_vec, d3, da_i_vec,
                     x0, x1, x2, x3);
                MUL4(d4, da_i_vec, d5, da_i_vec, d6, da_i_vec, d7, da_i_vec,
                     x4, x5, x6, x7);
                MUL4(d0, da_r_vec, d1, da_r_vec, d2, da_r_vec, d3, da_r_vec,
                     d0, d1, d2, d3);
                MUL4(d4, da_r_vec, d5, da_r_vec, d6, da_r_vec, d7, da_r_vec,
                     d4, d5, d6, d7);
                SHF_W4_SP(x0, x1, x2, x3, x0, x1, x2, x3, SHF_177);
                SHF_W4_SP(x4, x5, x6, x7, x4, x5, x6, x7, SHF_177);
                ADD4(d0, x0, d1, x1, d2, x2, d3, x3, d0, d1, d2, d3);
                ADD4(d4, x4, d5, x5, d6, x6, d7, x7, d4, d5, d6, d7);

                *x       = d0[0];
                *(x + 1) = d0[1];
                x += inc_x2;
                *x       = d0[2];
                *(x + 1) = d0[3];
                x += inc_x2;
                *x       = d1[0];
                *(x + 1) = d1[1];
                x += inc_x2;
                *x       = d1[2];
                *(x + 1) = d1[3];
                x += inc_x2;
                *x       = d2[0];
                *(x + 1) = d2[1];
                x += inc_x2;
                *x       = d2[2];
                *(x + 1) = d2[3];
                x += inc_x2;
                *x       = d3[0];
                *(x + 1) = d3[1];
                x += inc_x2;
                *x       = d3[2];
                *(x + 1) = d3[3];
                x += inc_x2;
                *x       = d4[0];
                *(x + 1) = d4[1];
                x += inc_x2;
                *x       = d4[2];
                *(x + 1) = d4[3];
                x += inc_x2;
                *x       = d5[0];
                *(x + 1) = d5[1];
                x += inc_x2;
                *x       = d5[2];
                *(x + 1) = d5[3];
                x += inc_x2;
                *x       = d6[0];
                *(x + 1) = d6[1];
                x += inc_x2;
                *x       = d6[2];
                *(x + 1) = d6[3];
                x += inc_x2;
                *x       = d7[0];
                *(x + 1) = d7[1];
                x += inc_x2;
                *x       = d7[2];
                *(x + 1) = d7[3];
                x += inc_x2;
            }

            if (n & 15)
            {
                if (n & 8)
                {
                    LD_SP8_INC(px, inc_x2, x0, x1, x2, x3, x4, x5, x6, x7);
                    PCKEV_D4_SP(x1, x0, x3, x2, x5, x4, x7, x6, d0, d1, d2, d3);
                    MUL4(d0, da_i_vec, d1, da_i_vec, d2, da_i_vec, d3, da_i_vec,
                         x0, x1, x2, x3);
                    MUL4(d0, da_r_vec, d1, da_r_vec, d2, da_r_vec, d3, da_r_vec,
                         d0, d1, d2, d3);
                    SHF_W4_SP(x0, x1, x2, x3, x0, x1, x2, x3, SHF_177);
                    ADD4(d0, x0, d1, x1, d2, x2, d3, x3, d0, d1, d2, d3);

                    *x       = d0[0];
                    *(x + 1) = d0[1];
                    x += inc_x2;
                    *x       = d0[2];
                    *(x + 1) = d0[3];
                    x += inc_x2;
                    *x       = d1[0];
                    *(x + 1) = d1[1];
                    x += inc_x2;
                    *x       = d1[2];
                    *(x + 1) = d1[3];
                    x += inc_x2;
                    *x       = d2[0];
                    *(x + 1) = d2[1];
                    x += inc_x2;
                    *x       = d2[2];
                    *(x + 1) = d2[3];
                    x += inc_x2;
                    *x       = d3[0];
                    *(x + 1) = d3[1];
                    x += inc_x2;
                    *x       = d3[2];
                    *(x + 1) = d3[3];
                    x += inc_x2;
                }

                if (n & 4)
                {
                    LD_SP4_INC(px, inc_x2, x0, x1, x2, x3);
                    PCKEV_D2_SP(x1, x0, x3, x2, d0, d1);
                    MUL2(d0, da_i_vec, d1, da_i_vec, x0, x1);
                    MUL2(d0, da_r_vec, d1, da_r_vec, d0, d1);
                    SHF_W2_SP(x0, x1, x0, x1, SHF_177);
                    ADD2(d0, x0, d1, x1, d0, d1);

                    *x       = d0[0];
                    *(x + 1) = d0[1];
                    x += inc_x2;
                    *x       = d0[2];
                    *(x + 1) = d0[3];
                    x += inc_x2;
                    *x       = d1[0];
                    *(x + 1) = d1[1];
                    x += inc_x2;
                    *x       = d1[2];
                    *(x + 1) = d1[3];
                    x += inc_x2;
                }

                if (n & 2)
                {
                    f0 = *px;;
                    f1 = *(px + 1);
                    px += inc_x2;
                    f2 = *px;
                    f3 = *(px + 1);
                    px += inc_x2;

                    tp0 = da_r * f0;
                    tp0 -= da_i * f1;
                    tp1 = da_r * f1;
                    tp1 += da_i * f0;
                    tp2 = da_r * f2;
                    tp2 -= da_i * f3;
                    tp3 = da_r * f3;
                    tp3 += da_i * f2;

                    *x       = tp0;
                    *(x + 1) = tp1;
                    x += inc_x2;
                    *x       = tp2;
                    *(x + 1) = tp3;
                    x += inc_x2;
                }

                if (n & 1)
                {
                    f0 = *px; px += 1;
                    f1 = *px;

                    tp0 = da_r * f0;
                    tp0 -= da_i * f1;
                    tp1 = da_r * f1;
                    tp1 += da_i * f0;

                    *x = tp0; x += 1;
                    *x = tp1;
                }
            }
        }
    }

    return (0);
}
