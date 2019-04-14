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

#if defined(DSDOT)
double CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
#else
FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
#endif
{
    BLASLONG i = 0;
    double dot = 0.0;
    FLOAT x0, x1, x2, x3, y0, y1, y2, y3;
    v4f32 vx0, vx1, vx2, vx3, vx4, vx5, vx6, vx7;
    v4f32 vy0, vy1, vy2, vy3, vy4, vy5, vy6, vy7;
    v4f32 dot0 = {0, 0, 0, 0};
    v4f32 dot1 = {0, 0, 0, 0};
    v4f32 dot2 = {0, 0, 0, 0};
    v4f32 dot3 = {0, 0, 0, 0};

    if (n < 1) return (dot);

    if ((1 == inc_x) && (1 == inc_y))
    {
        FLOAT *x_pref, *y_pref;
        BLASLONG pref_offset;

        pref_offset = (BLASLONG)x & (L1_DATA_LINESIZE - 1);
        if (pref_offset > 0)
        {
            pref_offset = L1_DATA_LINESIZE - pref_offset;
            pref_offset = pref_offset / sizeof(FLOAT);
        }
        x_pref = x + pref_offset + 64;

        pref_offset = (BLASLONG)y & (L1_DATA_LINESIZE - 1);
        if (pref_offset > 0)
        {
            pref_offset = L1_DATA_LINESIZE - pref_offset;
            pref_offset = pref_offset / sizeof(FLOAT);
        }
        y_pref = y + pref_offset + 64;

        for (i = (n >> 5); i--;)
        {
            LD_SP8_INC(x, 4, vx0, vx1, vx2, vx3, vx4, vx5, vx6, vx7);
            LD_SP8_INC(y, 4, vy0, vy1, vy2, vy3, vy4, vy5, vy6, vy7);

            PREF_OFFSET(x_pref, 0);
            PREF_OFFSET(x_pref, 32);
            PREF_OFFSET(x_pref, 64);
            PREF_OFFSET(x_pref, 96);
            PREF_OFFSET(y_pref, 0);
            PREF_OFFSET(y_pref, 32);
            PREF_OFFSET(y_pref, 64);
            PREF_OFFSET(y_pref, 96);
            x_pref += 32;
            y_pref += 32;

            dot0 += (vy0 * vx0);
            dot1 += (vy1 * vx1);
            dot2 += (vy2 * vx2);
            dot3 += (vy3 * vx3);
            dot0 += (vy4 * vx4);
            dot1 += (vy5 * vx5);
            dot2 += (vy6 * vx6);
            dot3 += (vy7 * vx7);
        }

        if (n & 31)
        {
            if (n & 16)
            {
                LD_SP4_INC(x, 4, vx0, vx1, vx2, vx3);
                LD_SP4_INC(y, 4, vy0, vy1, vy2, vy3);

                dot0 += (vy0 * vx0);
                dot1 += (vy1 * vx1);
                dot2 += (vy2 * vx2);
                dot3 += (vy3 * vx3);
            }

            if (n & 8)
            {
                LD_SP2_INC(x, 4, vx0, vx1);
                LD_SP2_INC(y, 4, vy0, vy1);

                dot0 += (vy0 * vx0);
                dot1 += (vy1 * vx1);
            }

            if (n & 4)
            {
                vx0 = LD_SP(x); x += 4;
                vy0 = LD_SP(y); y += 4;

                dot0 += (vy0 * vx0);
            }

            if (n & 2)
            {
                LD_GP2_INC(x, 1, x0, x1);
                LD_GP2_INC(y, 1, y0, y1);

                dot += (y0 * x0);
                dot += (y1 * x1);
            }

            if (n & 1)
            {
                x0 = *x;
                y0 = *y;

                dot += (y0 * x0);
            }
        }

        dot0 += dot1 + dot2 + dot3;

        dot += dot0[0];
        dot += dot0[1];
        dot += dot0[2];
        dot += dot0[3];
    }
    else
    {
        for (i = (n >> 2); i--;)
        {
            LD_GP4_INC(x, inc_x, x0, x1, x2, x3);
            LD_GP4_INC(y, inc_y, y0, y1, y2, y3);

            dot += (y0 * x0);
            dot += (y1 * x1);
            dot += (y2 * x2);
            dot += (y3 * x3);
        }

        if (n & 2)
        {
            LD_GP2_INC(x, inc_x, x0, x1);
            LD_GP2_INC(y, inc_y, y0, y1);

            dot += (y0 * x0);
            dot += (y1 * x1);
        }

        if (n & 1)
        {
            x0 = *x;
            y0 = *y;

            dot += (y0 * x0);
        }
    }

    return (dot);
}
