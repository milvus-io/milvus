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

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da, FLOAT *x,
          BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy,
          BLASLONG dummy2)
{
    BLASLONG i;
    FLOAT *px;
    FLOAT f0, f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11, f12, f13, f14, f15;
    v2f64 x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15;
    v2f64 da_vec;

    px = x;

    if (1 == inc_x)
    {
        if (0.0 == da)
        {
            v2f64 zero_v = __msa_cast_to_vector_double(0);
            zero_v = (v2f64) __msa_insert_d((v2i64) zero_v, 0, 0.0);
            zero_v = (v2f64) __msa_insert_d((v2i64) zero_v, 1, 0.0);

            for (i = (n >> 5); i--;)
            {
                ST_DP8_INC(zero_v, zero_v, zero_v, zero_v, zero_v, zero_v,
                           zero_v, zero_v, x, 2);
                ST_DP8_INC(zero_v, zero_v, zero_v, zero_v, zero_v, zero_v,
                           zero_v, zero_v, x, 2);
            }

            if (n & 31)
            {
                if (n & 16)
                {
                    ST_DP8_INC(zero_v, zero_v, zero_v, zero_v, zero_v, zero_v,
                               zero_v, zero_v, x, 2);
                }

                if (n & 8)
                {
                    ST_DP4_INC(zero_v, zero_v, zero_v, zero_v, x, 2);
                }

                if (n & 4)
                {
                    ST_DP2_INC(zero_v, zero_v, x, 2);
                }

                if (n & 2)
                {
                    *x = 0; x += 1;
                    *x = 0; x += 1;
                }

                if (n & 1)
                {
                    *x = 0;
                }
            }
        }
        else
        {
            da_vec = COPY_DOUBLE_TO_VECTOR(da);

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
                x_pref = x + pref_offset + 32 + 16;

                LD_DP8_INC(px, 2, x0, x1, x2, x3, x4, x5, x6, x7);
                for (i = 0; i < (n >> 5) - 1; i++)
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
                    x0 *= da_vec;
                    x9 = LD_DP(px); px += 2;
                    x1 *= da_vec;
                    x10 = LD_DP(px); px += 2;
                    x2 *= da_vec;
                    x11 = LD_DP(px); px += 2;
                    x3 *= da_vec;
                    x12 = LD_DP(px); px += 2;
                    x4 *= da_vec;
                    x13 = LD_DP(px); px += 2;
                    x5 *= da_vec;
                    x14 = LD_DP(px); px += 2;
                    x6 *= da_vec;
                    x15 = LD_DP(px); px += 2;
                    x7 *= da_vec;
                    x8 *= da_vec;
                    ST_DP(x0, x); x += 2;
                    x9 *= da_vec;
                    ST_DP(x1, x); x += 2;
                    x10 *= da_vec;
                    ST_DP(x2, x); x += 2;
                    x11 *= da_vec;
                    ST_DP(x3, x); x += 2;
                    x12 *= da_vec;
                    ST_DP(x4, x); x += 2;
                    x13 *= da_vec;
                    ST_DP(x5, x); x += 2;
                    x14 *= da_vec;
                    ST_DP(x6, x); x += 2;
                    x15 *= da_vec;
                    ST_DP(x7, x); x += 2;
                    ST_DP(x8, x); x += 2;
                    x0 = LD_DP(px); px += 2;
                    ST_DP(x9, x); x += 2;
                    x1 = LD_DP(px); px += 2;
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

                x8 = LD_DP(px); px += 2;
                x0 *= da_vec;
                x9 = LD_DP(px); px += 2;
                x1 *= da_vec;
                x10 = LD_DP(px); px += 2;
                x2 *= da_vec;
                x11 = LD_DP(px); px += 2;
                x3 *= da_vec;
                x12 = LD_DP(px); px += 2;
                x4 *= da_vec;
                x13 = LD_DP(px); px += 2;
                x5 *= da_vec;
                x14 = LD_DP(px); px += 2;
                x6 *= da_vec;
                x15 = LD_DP(px); px += 2;
                x7 *= da_vec;
                x8 *= da_vec;
                ST_DP(x0, x); x += 2;
                x9 *= da_vec;
                ST_DP(x1, x); x += 2;
                x10 *= da_vec;
                ST_DP(x2, x); x += 2;
                x11 *= da_vec;
                ST_DP(x3, x); x += 2;
                x12 *= da_vec;
                ST_DP(x4, x); x += 2;
                x13 *= da_vec;
                ST_DP(x5, x); x += 2;
                x15 *= da_vec;
                ST_DP(x6, x); x += 2;
                x14 *= da_vec;
                ST_DP(x7, x); x += 2;

                ST_DP8_INC(x8, x9, x10, x11, x12, x13, x14, x15, x, 2);
            }

            if (n & 31)
            {
                if (n & 16)
                {
                    LD_DP8_INC(px, 2, x0, x1, x2, x3, x4, x5, x6, x7);
                    MUL4(x0, da_vec, x1, da_vec, x2, da_vec, x3, da_vec, x0, x1, x2, x3);
                    MUL4(x4, da_vec, x5, da_vec, x6, da_vec, x7, da_vec, x4, x5, x6, x7);
                    ST_DP8_INC(x0, x1, x2, x3, x4, x5, x6, x7, x, 2);
                }

                if (n & 8)
                {
                    LD_DP4_INC(px, 2, x0, x1, x2, x3);
                    MUL4(x0, da_vec, x1, da_vec, x2, da_vec, x3, da_vec, x0, x1, x2, x3);
                    ST_DP4_INC(x0, x1, x2, x3, x, 2);
                }

                if (n & 4)
                {
                    LD_DP2_INC(px, 2, x0, x1);
                    MUL2(x0, da_vec, x1, da_vec, x0, x1);
                    ST_DP2_INC(x0, x1, x, 2);
                }

                if (n & 2)
                {
                    LD_GP2_INC(px, 1, f0, f1);
                    MUL2(f0, da, f1, da, f0, f1);
                    ST_GP2_INC(f0, f1, x, 1);
                }

                if (n & 1)
                {
                    *x *= da;
                }
            }
        }
    }
    else
    {
        if (da == 0.0)
        {
            for (i = n; i--;)
            {
                *x = 0.0;

                x += inc_x;
            }
        }
        else
        {
            if (n > 15)
            {
                LD_GP8_INC(px, inc_x, f0, f1, f2, f3, f4, f5, f6, f7);
                for (i = 0; i < (n >> 4) - 1; i++)
                {
                    LD_GP8_INC(px, inc_x, f8, f9, f10, f11, f12, f13, f14, f15);
                    MUL4(f0, da, f1, da, f2, da, f3, da, f0, f1, f2, f3);

                    f4 *= da;
                    f5 *= da;
                    *x = f0; x += inc_x;
                    f6 *= da;
                    *x = f1; x += inc_x;
                    f7 *= da;
                    *x = f2; x += inc_x;
                    f8 *= da;
                    *x = f3; x += inc_x;
                    f9 *= da;
                    *x = f4; x += inc_x;
                    f10 *= da;
                    *x = f5; x += inc_x;
                    f11 *= da;
                    *x = f6; x += inc_x;
                    f12 *= da;
                    *x = f7; x += inc_x;
                    f13 *= da;
                    *x = f8; x += inc_x;
                    f14 *= da;
                    *x = f9; x += inc_x;
                    f15 *= da;
                    *x = f10; x += inc_x;
                    *x = f11; x += inc_x;
                    f0 = *px; px += inc_x;
                    *x = f12; x += inc_x;
                    f1 = *px; px += inc_x;
                    *x = f13; x += inc_x;
                    f2 = *px; px += inc_x;
                    *x = f14; x += inc_x;
                    f3 = *px; px += inc_x;
                    *x = f15; x += inc_x;
                    f4 = *px; px += inc_x;
                    f5 = *px; px += inc_x;
                    f6 = *px; px += inc_x;
                    f7 = *px; px += inc_x;
                }

                LD_GP8_INC(px, inc_x, f8, f9, f10, f11, f12, f13, f14, f15);
                MUL4(f0, da, f1, da, f2, da, f3, da, f0, f1, f2, f3);

                f4 *= da;
                f5 *= da;
                *x = f0; x += inc_x;
                f6 *= da;
                *x = f1; x += inc_x;
                f7 *= da;
                *x = f2; x += inc_x;
                f8 *= da;
                *x = f3; x += inc_x;
                f9 *= da;
                *x = f4; x += inc_x;
                f10 *= da;
                *x = f5; x += inc_x;
                f11 *= da;
                *x = f6; x += inc_x;
                f12 *= da;
                *x = f7; x += inc_x;
                f13 *= da;
                *x = f8; x += inc_x;
                f14 *= da;
                *x = f9; x += inc_x;
                f15 *= da;
                *x = f10; x += inc_x;
                *x = f11; x += inc_x;
                *x = f12; x += inc_x;
                *x = f13; x += inc_x;
                *x = f14; x += inc_x;
                *x = f15; x += inc_x;
            }

            if (n & 15)
            {
                if (n & 8)
                {
                    LD_GP8_INC(px, inc_x, f0, f1, f2, f3, f4, f5, f6, f7);
                    MUL4(f0, da, f1, da, f2, da, f3, da, f0, f1, f2, f3);
                    MUL4(f4, da, f5, da, f6, da, f7, da, f4, f5, f6, f7);
                    ST_GP8_INC(f0, f1, f2, f3, f4, f5, f6, f7, x, inc_x);
                }

                if (n & 4)
                {
                    LD_GP4_INC(px, inc_x, f0, f1, f2, f3);
                    MUL4(f0, da, f1, da, f2, da, f3, da, f0, f1, f2, f3);
                    ST_GP4_INC(f0, f1, f2, f3, x, inc_x);
                }

                if (n & 2)
                {
                    LD_GP2_INC(px, inc_x, f0, f1);
                    MUL2(f0, da, f1, da, f0, f1);
                    ST_GP2_INC(f0, f1, x, inc_x);
                }

                if (n & 1)
                {
                    *x *= da;
                }
            }
        }
    }

    return 0;
}
