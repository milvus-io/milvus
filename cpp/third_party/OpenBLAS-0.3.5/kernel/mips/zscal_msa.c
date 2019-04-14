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

/* This will shuffle the elements in 'in' vector as (mask needed :: 01 00 11 10)
   0  1  2  3  =>  2  3  0  1 */
#define SHF_78    78

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da_r, FLOAT da_i,
          FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy,
          BLASLONG dummy2)
{
    BLASLONG i, inc_x2;
    FLOAT *px;
    FLOAT tp0, tp1, f0, f1;
    v2f64 x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15;
    v2f64 d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11, d12, d13, d14, d15;
    v2f64 da_i_vec, da_i_vec_neg, da_r_vec;

    px = x;

    if (1 == inc_x)
    {
        if ((0.0 == da_r) && (0.0 == da_i))
        {
            v2f64 zero_v = __msa_cast_to_vector_double(0);
            zero_v = (v2f64) __msa_insert_d((v2i64) zero_v, 0, 0.0);
            zero_v = (v2f64) __msa_insert_d((v2i64) zero_v, 1, 0.0);

            for (i = (n >> 4); i--;)
            {
                ST_DP8_INC(zero_v, zero_v, zero_v, zero_v, zero_v, zero_v,
                           zero_v, zero_v, x, 2);
                ST_DP8_INC(zero_v, zero_v, zero_v, zero_v, zero_v, zero_v,
                           zero_v, zero_v, x, 2);
            }

            if (n & 15)
            {
                if (n & 8)
                {
                    ST_DP8_INC(zero_v, zero_v, zero_v, zero_v, zero_v, zero_v,
                               zero_v, zero_v, x, 2);
                }

                if (n & 4)
                {
                    ST_DP4_INC(zero_v, zero_v, zero_v, zero_v, x, 2);
                }

                if (n & 2)
                {
                    ST_DP2_INC(zero_v, zero_v, x, 2);
                }

                if (n & 1)
                {
                    ST_DP(zero_v, x);
                }
            }
        }
        else if (0.0 == da_r)
        {
            da_i_vec = COPY_DOUBLE_TO_VECTOR(da_i);
            da_i_vec_neg = -da_i_vec;
            da_i_vec = (v2f64) __msa_ilvev_d((v2i64) da_i_vec_neg, (v2i64) da_i_vec);

            if (n > 15)
            {
                FLOAT *x_pref;
                BLASLONG pref_offset;

                pref_offset = (BLASLONG)x & (L1_DATA_LINESIZE - 1);
                if (pref_offset > 0)
                {
                    pref_offset = L1_DATA_LINESIZE - pref_offset;
                    pref_offset = pref_offset / sizeof(FLOAT);
                }
                x_pref = x + pref_offset + 32 + 16;

                LD_DP8_INC(px, 2, x0, x1, x2, x3, x4, x5, x6, x7);
                for (i = (n >> 4)- 1; i--;)
                {
                    PREF_OFFSET(x_pref, 0);
                    PREF_OFFSET(x_pref, 32);
                    PREF_OFFSET(x_pref, 64);
                    PREF_OFFSET(x_pref, 96);
                    PREF_OFFSET(x_pref, 128);
                    PREF_OFFSET(x_pref, 160);
                    PREF_OFFSET(x_pref, 192);
                    PREF_OFFSET(x_pref, 224);
                    x_pref += 32;

                    x8 = LD_DP(px); px += 2;
                    x0 *= da_i_vec;
                    x9 = LD_DP(px); px += 2;
                    x1 *= da_i_vec;
                    x10 = LD_DP(px); px += 2;
                    x2 *= da_i_vec;
                    x11 = LD_DP(px); px += 2;
                    x3 *= da_i_vec;
                    x12 = LD_DP(px); px += 2;
                    x4 *= da_i_vec;
                    x13 = LD_DP(px); px += 2;
                    x5 *= da_i_vec;
                    x0 = (v2f64) __msa_shf_w((v4i32) x0, SHF_78);
                    x14 = LD_DP(px); px += 2;
                    x6 *= da_i_vec;
                    x1 = (v2f64) __msa_shf_w((v4i32) x1, SHF_78);
                    x15 = LD_DP(px); px += 2;
                    x7 *= da_i_vec;
                    x2 = (v2f64) __msa_shf_w((v4i32) x2, SHF_78);
                    x8 *= da_i_vec;
                    x3 = (v2f64) __msa_shf_w((v4i32) x3, SHF_78);
                    ST_DP(x0, x); x += 2;
                    x9 *= da_i_vec;
                    x4 = (v2f64) __msa_shf_w((v4i32) x4, SHF_78);
                    ST_DP(x1, x); x += 2;
                    x10 *= da_i_vec;
                    x5 = (v2f64) __msa_shf_w((v4i32) x5, SHF_78);
                    ST_DP(x2, x); x += 2;
                    x11 *= da_i_vec;
                    x6 = (v2f64) __msa_shf_w((v4i32) x6, SHF_78);
                    ST_DP(x3, x); x += 2;
                    x12 *= da_i_vec;
                    x7 = (v2f64) __msa_shf_w((v4i32) x7, SHF_78);
                    ST_DP(x4, x); x += 2;
                    x13 *= da_i_vec;
                    x8 = (v2f64) __msa_shf_w((v4i32) x8, SHF_78);
                    ST_DP(x5, x); x += 2;
                    x14 *= da_i_vec;
                    x9 = (v2f64) __msa_shf_w((v4i32) x9, SHF_78);
                    ST_DP(x6, x); x += 2;
                    x15 *= da_i_vec;
                    x10 = (v2f64) __msa_shf_w((v4i32) x10, SHF_78);
                    ST_DP(x7, x); x += 2;
                    x11 = (v2f64) __msa_shf_w((v4i32) x11, SHF_78);
                    ST_DP(x8, x); x += 2;
                    x0 = LD_DP(px); px += 2;
                    x12 = (v2f64) __msa_shf_w((v4i32) x12, SHF_78);
                    ST_DP(x9, x); x += 2;
                    x1 = LD_DP(px); px += 2;
                    x13 = (v2f64) __msa_shf_w((v4i32) x13, SHF_78);
                    ST_DP(x10, x); x += 2;
                    x2 = LD_DP(px); px += 2;
                    x14 = (v2f64) __msa_shf_w((v4i32) x14, SHF_78);
                    ST_DP(x11, x); x += 2;
                    x3 = LD_DP(px); px += 2;
                    x15 = (v2f64) __msa_shf_w((v4i32) x15, SHF_78);
                    ST_DP(x12, x); x += 2;
                    x4 = LD_DP(px); px += 2;
                    ST_DP(x13, x); x += 2;
                    x5 = LD_DP(px); px += 2;
                    ST_DP(x14, x); x += 2;
                    x6 = LD_DP(px); px += 2;
                    ST_DP(x15, x); x += 2;
                    x7 = LD_DP(px); px += 2;
                }

                LD_DP8_INC(px, 2, x8, x9, x10, x11, x12, x13, x14, x15);
                MUL4(x0, da_i_vec, x1, da_i_vec, x2, da_i_vec, x3, da_i_vec,
                     x0, x1, x2, x3);
                MUL4(x4, da_i_vec, x5, da_i_vec, x6, da_i_vec, x7, da_i_vec,
                     x4, x5, x6, x7);
                MUL4(x8, da_i_vec, x9, da_i_vec, x10, da_i_vec, x11, da_i_vec,
                     x8, x9, x10, x11);
                MUL4(x12, da_i_vec, x13, da_i_vec, x14, da_i_vec, x15, da_i_vec,
                     x12, x13, x14, x15);
                SHF_W4_DP(x0, x1, x2, x3, x0, x1, x2, x3, SHF_78);
                SHF_W4_DP(x4, x5, x6, x7, x4, x5, x6, x7, SHF_78);
                SHF_W4_DP(x8, x9, x10, x11, x8, x9, x10, x11, SHF_78);
                SHF_W4_DP(x12, x13, x14, x15, x12, x13, x14, x15, SHF_78);
                ST_DP16_INC(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11,
                            x12, x13, x14, x15, x, 2);
            }

            if (n & 15)
            {
                if (n & 8)
                {
                    LD_DP8_INC(px, 2, x0, x1, x2, x3, x4, x5, x6, x7);
                    MUL4(x0, da_i_vec, x1, da_i_vec, x2, da_i_vec, x3, da_i_vec,
                         x0, x1, x2, x3);
                    MUL4(x4, da_i_vec, x5, da_i_vec, x6, da_i_vec, x7, da_i_vec,
                         x4, x5, x6, x7);
                    SHF_W4_DP(x0, x1, x2, x3, x0, x1, x2, x3, SHF_78);
                    SHF_W4_DP(x4, x5, x6, x7, x4, x5, x6, x7, SHF_78);
                    ST_DP8_INC(x0, x1, x2, x3, x4, x5, x6, x7, x, 2);
                }

                if (n & 4)
                {
                    LD_DP4_INC(px, 2, x0, x1, x2, x3);
                    MUL4(x0, da_i_vec, x1, da_i_vec, x2, da_i_vec, x3, da_i_vec,
                         x0, x1, x2, x3);
                    SHF_W4_DP(x0, x1, x2, x3, x0, x1, x2, x3, SHF_78);
                    ST_DP4_INC(x0, x1, x2, x3, x, 2);
                }

                if (n & 2)
                {
                    LD_DP2_INC(px, 2, x0, x1);
                    MUL2(x0, da_i_vec, x1, da_i_vec, x0, x1);
                    SHF_W2_DP(x0, x1, x0, x1, SHF_78);
                    ST_DP2_INC(x0, x1, x, 2);
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
            da_r_vec = COPY_DOUBLE_TO_VECTOR(da_r);

            if (n > 15)
            {
                FLOAT *x_pref;
                BLASLONG pref_offset;

                pref_offset = (BLASLONG)x & (L1_DATA_LINESIZE - 1);
                if (pref_offset > 0)
                {
                    pref_offset = L1_DATA_LINESIZE - pref_offset;
                    pref_offset = pref_offset / sizeof(FLOAT);
                }
                x_pref = x + pref_offset + 32 + 16;

                LD_DP8_INC(px, 2, x0, x1, x2, x3, x4, x5, x6, x7);
                for (i = (n >> 4)- 1; i--;)
                {
                    PREF_OFFSET(x_pref, 0);
                    PREF_OFFSET(x_pref, 32);
                    PREF_OFFSET(x_pref, 64);
                    PREF_OFFSET(x_pref, 96);
                    PREF_OFFSET(x_pref, 128);
                    PREF_OFFSET(x_pref, 160);
                    PREF_OFFSET(x_pref, 192);
                    PREF_OFFSET(x_pref, 224);
                    x_pref += 32;

                    x8 = LD_DP(px); px += 2;
                    x0 *= da_r_vec;
                    x9 = LD_DP(px); px += 2;
                    x1 *= da_r_vec;
                    x10 = LD_DP(px); px += 2;
                    x2 *= da_r_vec;
                    x11 = LD_DP(px); px += 2;
                    x3 *= da_r_vec;
                    x12 = LD_DP(px); px += 2;
                    x4 *= da_r_vec;
                    x13 = LD_DP(px); px += 2;
                    x5 *= da_r_vec;
                    ST_DP(x0, x); x += 2;
                    x14 = LD_DP(px); px += 2;
                    x6 *= da_r_vec;
                    ST_DP(x1, x); x += 2;
                    x15 = LD_DP(px); px += 2;
                    x7 *= da_r_vec;
                    ST_DP(x2, x); x += 2;
                    x8 *= da_r_vec;
                    ST_DP(x3, x); x += 2;
                    x9 *= da_r_vec;
                    ST_DP(x4, x); x += 2;
                    x10 *= da_r_vec;
                    ST_DP(x5, x); x += 2;
                    x11 *= da_r_vec;
                    ST_DP(x6, x); x += 2;
                    x12 *= da_r_vec;
                    ST_DP(x7, x); x += 2;
                    x13 *= da_r_vec;
                    ST_DP(x8, x); x += 2;
                    x0 = LD_DP(px); px += 2;
                    x14 *= da_r_vec;
                    ST_DP(x9, x); x += 2;
                    x1 = LD_DP(px); px += 2;
                    x15 *= da_r_vec;
                    ST_DP(x10, x); x += 2;
                    x2 = LD_DP(px); px += 2;
                    ST_DP(x11, x); x += 2;
                    x3 = LD_DP(px); px += 2;
                    ST_DP(x12, x); x += 2;
                    x4 = LD_DP(px); px += 2;
                    ST_DP(x13, x); x += 2;
                    x5 = LD_DP(px); px += 2;
                    ST_DP(x14, x); x += 2;
                    x6 = LD_DP(px); px += 2;
                    ST_DP(x15, x); x += 2;
                    x7 = LD_DP(px); px += 2;
                }

                LD_DP8_INC(px, 2, x8, x9, x10, x11, x12, x13, x14, x15);
                MUL4(x0, da_r_vec, x1, da_r_vec, x2, da_r_vec, x3, da_r_vec,
                     x0, x1, x2, x3);
                MUL4(x4, da_r_vec, x5, da_r_vec, x6, da_r_vec, x7, da_r_vec,
                     x4, x5, x6, x7);
                MUL4(x8, da_r_vec, x9, da_r_vec, x10, da_r_vec, x11, da_r_vec,
                     x8, x9, x10, x11);
                MUL4(x12, da_r_vec, x13, da_r_vec, x14, da_r_vec, x15, da_r_vec,
                     x12, x13, x14, x15);
                ST_DP16_INC(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11,
                            x12, x13, x14, x15, x, 2);
            }

            if (n & 15)
            {
                if (n & 8)
                {
                    LD_DP8_INC(px, 2, x0, x1, x2, x3, x4, x5, x6, x7);
                    MUL4(x0, da_r_vec, x1, da_r_vec, x2, da_r_vec, x3, da_r_vec,
                         x0, x1, x2, x3);
                    MUL4(x4, da_r_vec, x5, da_r_vec, x6, da_r_vec, x7, da_r_vec,
                         x4, x5, x6, x7);
                    ST_DP8_INC(x0, x1, x2, x3, x4, x5, x6, x7, x, 2);
                }

                if (n & 4)
                {
                    LD_DP4_INC(px, 2, x0, x1, x2, x3);
                    MUL4(x0, da_r_vec, x1, da_r_vec, x2, da_r_vec, x3, da_r_vec,
                         x0, x1, x2, x3);
                    ST_DP4_INC(x0, x1, x2, x3, x, 2);
                }

                if (n & 2)
                {
                    LD_DP2_INC(px, 2, x0, x1);
                    MUL2(x0, da_r_vec, x1, da_r_vec, x0, x1);
                    ST_DP2_INC(x0, x1, x, 2);
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
            x_pref = x + pref_offset + 32;

            da_i_vec = COPY_DOUBLE_TO_VECTOR(da_i);
            da_i_vec_neg = -da_i_vec;
            da_i_vec = (v2f64) __msa_ilvev_d((v2i64) da_i_vec_neg, (v2i64) da_i_vec);

            da_r_vec = COPY_DOUBLE_TO_VECTOR(da_r);

            for (i = (n >> 4); i--;)
            {
                PREF_OFFSET(x_pref, 0);
                PREF_OFFSET(x_pref, 32);
                PREF_OFFSET(x_pref, 64);
                PREF_OFFSET(x_pref, 96);
                PREF_OFFSET(x_pref, 128);
                PREF_OFFSET(x_pref, 160);
                PREF_OFFSET(x_pref, 192);
                PREF_OFFSET(x_pref, 224);
                x_pref += 32;

                LD_DP16_INC(px, 2, x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10,
                            x11, x12, x13, x14, x15);
                MUL4(x0, da_i_vec, x1, da_i_vec, x2, da_i_vec, x3, da_i_vec,
                     d0, d1, d2, d3);
                MUL4(x4, da_i_vec, x5, da_i_vec, x6, da_i_vec, x7, da_i_vec,
                     d4, d5, d6, d7);
                MUL4(x8, da_i_vec, x9, da_i_vec, x10, da_i_vec, x11, da_i_vec,
                     d8, d9, d10, d11);
                MUL4(x12, da_i_vec, x13, da_i_vec, x14, da_i_vec, x15, da_i_vec,
                     d12, d13, d14, d15);
                SHF_W4_DP(d0, d1, d2, d3, d0, d1, d2, d3, SHF_78);
                SHF_W4_DP(d4, d5, d6, d7, d4, d5, d6, d7, SHF_78);
                SHF_W4_DP(d8, d9, d10, d11, d8, d9, d10, d11, SHF_78);
                SHF_W4_DP(d12, d13, d14, d15, d12, d13, d14, d15, SHF_78);
                FMADD4(x0, x1, x2, x3, da_r_vec, d0, d1, d2, d3);
                FMADD4(x4, x5, x6, x7, da_r_vec, d4, d5, d6, d7);
                FMADD4(x8, x9, x10, x11, da_r_vec, d8, d9, d10, d11);
                FMADD4(x12, x13, x14, x15, da_r_vec, d12, d13, d14, d15);
                ST_DP16_INC(d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11,
                            d12, d13, d14, d15, x, 2);
            }

            if (n & 15)
            {
                if (n & 8)
                {
                    LD_DP8_INC(px, 2, x0, x1, x2, x3, x4, x5, x6, x7);
                    MUL4(x0, da_i_vec, x1, da_i_vec, x2, da_i_vec, x3, da_i_vec,
                         d0, d1, d2, d3);
                    MUL4(x4, da_i_vec, x5, da_i_vec, x6, da_i_vec, x7, da_i_vec,
                         d4, d5, d6, d7);
                    SHF_W4_DP(d0, d1, d2, d3, d0, d1, d2, d3, SHF_78);
                    SHF_W4_DP(d4, d5, d6, d7, d4, d5, d6, d7, SHF_78);
                    FMADD4(x0, x1, x2, x3, da_r_vec, d0, d1, d2, d3);
                    FMADD4(x4, x5, x6, x7, da_r_vec, d4, d5, d6, d7);
                    ST_DP8_INC(d0, d1, d2, d3, d4, d5, d6, d7, x, 2);
                }

                if (n & 4)
                {
                    LD_DP4_INC(px, 2, x0, x1, x2, x3);
                    MUL4(x0, da_i_vec, x1, da_i_vec, x2, da_i_vec, x3, da_i_vec,
                         d0, d1, d2, d3);
                    SHF_W4_DP(d0, d1, d2, d3, d0, d1, d2, d3, SHF_78);
                    FMADD4(x0, x1, x2, x3, da_r_vec, d0, d1, d2, d3);
                    ST_DP4_INC(d0, d1, d2, d3, x, 2);
                }

                if (n & 2)
                {
                    LD_DP2_INC(px, 2, x0, x1);
                    MUL2(x0, da_i_vec, x1, da_i_vec, d0, d1);
                    SHF_W2_DP(d0, d1, d0, d1, SHF_78);
                    FMADD2(x0, x1, da_r_vec, d0, d1);
                    ST_DP2_INC(d0, d1, x, 2);
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
            v2f64 zero_v = __msa_cast_to_vector_double(0);
            zero_v = (v2f64) __msa_insert_d((v2i64) zero_v, 0, 0.0);
            zero_v = (v2f64) __msa_insert_d((v2i64) zero_v, 1, 0.0);

            for (i = (n >> 4); i--;)
            {
                ST_DP8_INC(zero_v, zero_v, zero_v, zero_v, zero_v, zero_v,
                           zero_v, zero_v, x, inc_x2);
                ST_DP8_INC(zero_v, zero_v, zero_v, zero_v, zero_v, zero_v,
                           zero_v, zero_v, x, inc_x2);
            }

            if (n & 15)
            {
                if (n & 8)
                {
                    ST_DP8_INC(zero_v, zero_v, zero_v, zero_v, zero_v, zero_v,
                               zero_v, zero_v, x, inc_x2);
                }

                if (n & 4)
                {
                    ST_DP4_INC(zero_v, zero_v, zero_v, zero_v, x, inc_x2);
                }

                if (n & 2)
                {
                    ST_DP2_INC(zero_v, zero_v, x, inc_x2);
                }

                if (n & 1)
                {
                    ST_DP(zero_v, x);
                }
            }
        }
        else if (0.0 == da_r)
        {
            da_i_vec = COPY_DOUBLE_TO_VECTOR(da_i);
            da_i_vec_neg = -da_i_vec;
            da_i_vec = (v2f64) __msa_ilvev_d((v2i64) da_i_vec_neg, (v2i64) da_i_vec);

            for (i = (n >> 4); i--;)
            {
                LD_DP16_INC(px, inc_x2, x0, x1, x2, x3, x4, x5, x6, x7, x8, x9,
                            x10, x11, x12, x13, x14, x15);
                MUL4(x0, da_i_vec, x1, da_i_vec, x2, da_i_vec, x3, da_i_vec,
                     x0, x1, x2, x3);
                MUL4(x4, da_i_vec, x5, da_i_vec, x6, da_i_vec, x7, da_i_vec,
                     x4, x5, x6, x7);
                MUL4(x8, da_i_vec, x9, da_i_vec, x10, da_i_vec, x11, da_i_vec,
                     x8, x9, x10, x11);
                MUL4(x12, da_i_vec, x13, da_i_vec, x14, da_i_vec, x15, da_i_vec,
                     x12, x13, x14, x15);
                SHF_W4_DP(x0, x1, x2, x3, x0, x1, x2, x3, SHF_78);
                SHF_W4_DP(x4, x5, x6, x7, x4, x5, x6, x7, SHF_78);
                SHF_W4_DP(x8, x9, x10, x11, x8, x9, x10, x11, SHF_78);
                SHF_W4_DP(x12, x13, x14, x15, x12, x13, x14, x15, SHF_78);
                ST_DP16_INC(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11,
                            x12, x13, x14, x15, x, inc_x2);
            }

            if (n & 15)
            {
                if (n & 8)
                {
                    LD_DP8_INC(px, inc_x2, x0, x1, x2, x3, x4, x5, x6, x7);
                    MUL4(x0, da_i_vec, x1, da_i_vec, x2, da_i_vec, x3, da_i_vec,
                         x0, x1, x2, x3);
                    MUL4(x4, da_i_vec, x5, da_i_vec, x6, da_i_vec, x7, da_i_vec,
                         x4, x5, x6, x7);
                    SHF_W4_DP(x0, x1, x2, x3, x0, x1, x2, x3, SHF_78);
                    SHF_W4_DP(x4, x5, x6, x7, x4, x5, x6, x7, SHF_78);
                    ST_DP8_INC(x0, x1, x2, x3, x4, x5, x6, x7, x, inc_x2);
                }

                if (n & 4)
                {
                    LD_DP4_INC(px, inc_x2, x0, x1, x2, x3);
                    MUL4(x0, da_i_vec, x1, da_i_vec, x2, da_i_vec, x3, da_i_vec,
                         x0, x1, x2, x3);
                    SHF_W4_DP(x0, x1, x2, x3, x0, x1, x2, x3, SHF_78);
                    ST_DP4_INC(x0, x1, x2, x3, x, inc_x2);
                }

                if (n & 2)
                {
                    LD_DP2_INC(px, inc_x2, x0, x1);
                    MUL2(x0, da_i_vec, x1, da_i_vec, x0, x1);
                    SHF_W2_DP(x0, x1, x0, x1, SHF_78);
                    ST_DP2_INC(x0, x1, x, inc_x2);
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
            da_r_vec = COPY_DOUBLE_TO_VECTOR(da_r);

            for (i = (n >> 4); i--;)
            {
                LD_DP16_INC(px, inc_x2, x0, x1, x2, x3, x4, x5, x6, x7, x8, x9,
                            x10, x11, x12, x13, x14, x15);
                MUL4(x0, da_r_vec, x1, da_r_vec, x2, da_r_vec, x3, da_r_vec,
                     x0, x1, x2, x3);
                MUL4(x4, da_r_vec, x5, da_r_vec, x6, da_r_vec, x7, da_r_vec,
                     x4, x5, x6, x7);
                MUL4(x8, da_r_vec, x9, da_r_vec, x10, da_r_vec, x11, da_r_vec,
                     x8, x9, x10, x11);
                MUL4(x12, da_r_vec, x13, da_r_vec, x14, da_r_vec, x15, da_r_vec,
                     x12, x13, x14, x15);
                ST_DP16_INC(x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11,
                            x12, x13, x14, x15, x, inc_x2);
            }

            if (n & 15)
            {
                if (n & 8)
                {
                    LD_DP8_INC(px, inc_x2, x0, x1, x2, x3, x4, x5, x6, x7);
                    MUL4(x0, da_r_vec, x1, da_r_vec, x2, da_r_vec, x3, da_r_vec,
                         x0, x1, x2, x3);
                    MUL4(x4, da_r_vec, x5, da_r_vec, x6, da_r_vec, x7, da_r_vec,
                         x4, x5, x6, x7);
                    ST_DP8_INC(x0, x1, x2, x3, x4, x5, x6, x7, x, inc_x2);
                }

                if (n & 4)
                {
                    LD_DP4_INC(px, inc_x2, x0, x1, x2, x3);
                    MUL4(x0, da_r_vec, x1, da_r_vec, x2, da_r_vec, x3, da_r_vec,
                         x0, x1, x2, x3);
                    ST_DP4_INC(x0, x1, x2, x3, x, inc_x2);
                }

                if (n & 2)
                {
                    LD_DP2_INC(px, inc_x2, x0, x1);
                    MUL2(x0, da_r_vec, x1, da_r_vec, x0, x1);
                    ST_DP2_INC(x0, x1, x, inc_x2);
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
            da_i_vec = COPY_DOUBLE_TO_VECTOR(da_i);
            da_i_vec_neg = -da_i_vec;
            da_i_vec = (v2f64) __msa_ilvev_d((v2i64) da_i_vec_neg, (v2i64) da_i_vec);

            da_r_vec = COPY_DOUBLE_TO_VECTOR(da_r);

            for (i = (n >> 4); i--;)
            {
                LD_DP16_INC(px, inc_x2, x0, x1, x2, x3, x4, x5, x6, x7, x8, x9,
                            x10, x11, x12, x13, x14, x15);
                MUL4(x0, da_i_vec, x1, da_i_vec, x2, da_i_vec, x3, da_i_vec,
                     d0, d1, d2, d3);
                MUL4(x4, da_i_vec, x5, da_i_vec, x6, da_i_vec, x7, da_i_vec,
                     d4, d5, d6, d7);
                MUL4(x8, da_i_vec, x9, da_i_vec, x10, da_i_vec, x11, da_i_vec,
                     d8, d9, d10, d11);
                MUL4(x12, da_i_vec, x13, da_i_vec, x14, da_i_vec, x15, da_i_vec,
                     d12, d13, d14, d15);
                SHF_W4_DP(d0, d1, d2, d3, d0, d1, d2, d3, SHF_78);
                SHF_W4_DP(d4, d5, d6, d7, d4, d5, d6, d7, SHF_78);
                SHF_W4_DP(d8, d9, d10, d11, d8, d9, d10, d11, SHF_78);
                SHF_W4_DP(d12, d13, d14, d15, d12, d13, d14, d15, SHF_78);
                FMADD4(x0, x1, x2, x3, da_r_vec, d0, d1, d2, d3);
                FMADD4(x4, x5, x6, x7, da_r_vec, d4, d5, d6, d7);
                FMADD4(x8, x9, x10, x11, da_r_vec, d8, d9, d10, d11);
                FMADD4(x12, x13, x14, x15, da_r_vec, d12, d13, d14, d15);
                ST_DP16_INC(d0, d1, d2, d3, d4, d5, d6, d7, d8, d9, d10, d11,
                            d12, d13, d14, d15, x, inc_x2);
            }

            if (n & 15)
            {
                if (n & 8)
                {
                    LD_DP8_INC(px, inc_x2, x0, x1, x2, x3, x4, x5, x6, x7);
                    MUL4(x0, da_i_vec, x1, da_i_vec, x2, da_i_vec, x3, da_i_vec,
                         d0, d1, d2, d3);
                    MUL4(x4, da_i_vec, x5, da_i_vec, x6, da_i_vec, x7, da_i_vec,
                         d4, d5, d6, d7);
                    SHF_W4_DP(d0, d1, d2, d3, d0, d1, d2, d3, SHF_78);
                    SHF_W4_DP(d4, d5, d6, d7, d4, d5, d6, d7, SHF_78);
                    FMADD4(x0, x1, x2, x3, da_r_vec, d0, d1, d2, d3);
                    FMADD4(x4, x5, x6, x7, da_r_vec, d4, d5, d6, d7);
                    ST_DP8_INC(d0, d1, d2, d3, d4, d5, d6, d7, x, inc_x2);
                }

                if (n & 4)
                {
                    LD_DP4_INC(px, inc_x2, x0, x1, x2, x3);
                    MUL4(x0, da_i_vec, x1, da_i_vec, x2, da_i_vec, x3, da_i_vec,
                         d0, d1, d2, d3);
                    SHF_W4_DP(d0, d1, d2, d3, d0, d1, d2, d3, SHF_78);
                    FMADD4(x0, x1, x2, x3, da_r_vec, d0, d1, d2, d3);
                    ST_DP4_INC(d0, d1, d2, d3, x, inc_x2);
                }

                if (n & 2)
                {
                    LD_DP2_INC(px, inc_x2, x0, x1);
                    MUL2(x0, da_i_vec, x1, da_i_vec, d0, d1);
                    SHF_W2_DP(d0, d1, d0, d1, SHF_78);
                    FMADD2(x0, x1, da_r_vec, d0, d1);
                    ST_DP2_INC(d0, d1, x, inc_x2);
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

    return (0);
}
