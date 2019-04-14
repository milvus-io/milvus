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

int CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
{
    BLASLONG i;
    v4f32 x0, x1, x2, x3, x4, x5, x6, x7, x8, x9, x10, x11, x12, x13, x14, x15;
    FLOAT f0, f1, f2, f3, f4, f5, f6, f7;

    if (n < 0)  return (0);

    if ((1 == inc_x) && (1 == inc_y))
    {
        if (n > 63)
        {
            FLOAT *x_pref;
            BLASLONG pref_offset;

            pref_offset = (BLASLONG)x & (L1_DATA_LINESIZE - 1);
            if (pref_offset > 0)
            {
                pref_offset = L1_DATA_LINESIZE - pref_offset;
                pref_offset = pref_offset / sizeof(FLOAT);
            }
            x_pref = x + pref_offset + 128 + 32;

            LD_SP8_INC(x, 4, x0, x1, x2, x3, x4, x5, x6, x7);
            for (i = (n >> 6) - 1; i--;)
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

                x8 = LD_SP(x); x += 4;
                ST_SP(x0, y); y += 4;
                x9 = LD_SP(x); x += 4;
                ST_SP(x1, y); y += 4;
                x10 = LD_SP(x); x += 4;
                ST_SP(x2, y); y += 4;
                x11 = LD_SP(x); x += 4;
                ST_SP(x3, y); y += 4;
                x12 = LD_SP(x); x += 4;
                ST_SP(x4, y); y += 4;
                x13 = LD_SP(x); x += 4;
                ST_SP(x5, y); y += 4;
                x14 = LD_SP(x); x += 4;
                ST_SP(x6, y); y += 4;
                x15 = LD_SP(x); x += 4;
                ST_SP(x7, y); y += 4;
                x0 = LD_SP(x); x += 4;
                ST_SP(x8, y); y += 4;
                x1 = LD_SP(x); x += 4;
                ST_SP(x9, y); y += 4;
                x2 = LD_SP(x); x += 4;
                ST_SP(x10, y); y += 4;
                x3 = LD_SP(x); x += 4;
                ST_SP(x11, y); y += 4;
                x4 = LD_SP(x); x += 4;
                ST_SP(x12, y); y += 4;
                x5 = LD_SP(x); x += 4;
                ST_SP(x13, y); y += 4;
                x6 = LD_SP(x); x += 4;
                ST_SP(x14, y); y += 4;
                x7 = LD_SP(x); x += 4;
                ST_SP(x15, y); y += 4;
            }

            x8 = LD_SP(x); x += 4;
            x9 = LD_SP(x); x += 4;
            ST_SP(x0, y); y += 4;
            x10 = LD_SP(x); x += 4;
            ST_SP(x1, y); y += 4;
            x11 = LD_SP(x); x += 4;
            ST_SP(x2, y); y += 4;
            x12 = LD_SP(x); x += 4;
            ST_SP(x3, y); y += 4;
            x13 = LD_SP(x); x += 4;
            ST_SP(x4, y); y += 4;
            x14 = LD_SP(x); x += 4;
            ST_SP(x5, y); y += 4;
            x15 = LD_SP(x); x += 4;
            ST_SP(x6, y); y += 4;
            ST_SP(x7, y); y += 4;

            ST_SP8_INC(x8, x9, x10, x11, x12, x13, x14, x15, y, 4);
        }

        if (n & 63)
        {
            if (n & 32)
            {
                LD_SP8_INC(x, 4, x0, x1, x2, x3, x4, x5, x6, x7);
                ST_SP8_INC(x0, x1, x2, x3, x4, x5, x6, x7, y, 4);
            }

            if (n & 16)
            {
                LD_SP4_INC(x, 4, x0, x1, x2, x3);
                ST_SP4_INC(x0, x1, x2, x3, y, 4);
            }

            if (n & 8)
            {
                LD_SP2_INC(x, 4, x0, x1);
                ST_SP2_INC(x0, x1, y, 4);
            }

            if (n & 4)
            {
                LD_GP4_INC(x, 1, f0, f1, f2, f3);
                ST_GP4_INC(f0, f1, f2, f3, y, 1);
            }

            if (n & 2)
            {
                LD_GP2_INC(x, 1, f0, f1);
                ST_GP2_INC(f0, f1, y, 1);
            }

            if (n & 1)
            {
                *y = *x;
            }
        }
    }
    else
    {
        for (i = (n >> 3); i--;)
        {
            LD_GP8_INC(x, inc_x, f0, f1, f2, f3, f4, f5, f6, f7);
            ST_GP8_INC(f0, f1, f2, f3, f4, f5, f6, f7, y, inc_y);
        }

        if (n & 4)
        {
            LD_GP4_INC(x, inc_x, f0, f1, f2, f3);
            ST_GP4_INC(f0, f1, f2, f3, y, inc_y);
        }

        if (n & 2)
        {
            LD_GP2_INC(x, inc_x, f0, f1);
            ST_GP2_INC(f0, f1, y, inc_y);
        }

        if (n & 1)
        {
            *y = *x;
        }
    }

    return (0);
}
