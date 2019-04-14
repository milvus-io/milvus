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

int CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y,
          FLOAT c, FLOAT s)
{
    BLASLONG i, j;
    FLOAT *px, *py;
    FLOAT tp0, tp1, tp2, tp3, tp4, tp5, tp6, tp7;
    FLOAT fx0, fx1, fx2, fx3, fy0, fy1, fy2, fy3;
    BLASLONG inc_x2, inc_y2;
    v4f32 x0, x1, x2, x3, x4, x5, x6, x7, y0, y1, y2, y3, y4, y5, y6, y7;
    v4f32 out0, out1, out2, out3, out4, out5, out6, out7;
    v4f32 out8, out9, out10, out11, out12, out13, out14, out15, c0, s0;

    if (n <= 0)  return (0);

    px = x;
    py = y;

    if ((1 == inc_x) && (1 == inc_y))
    {
        if ((0 == c) && (0 == s))
        {
            v4f32 zero = __msa_cast_to_vector_float(0);
            zero = (v4f32) __msa_insert_w((v4i32) zero, 0, 0.0);
            zero = (v4f32) __msa_insert_w((v4i32) zero, 1, 0.0);
            zero = (v4f32) __msa_insert_w((v4i32) zero, 2, 0.0);
            zero = (v4f32) __msa_insert_w((v4i32) zero, 3, 0.0);

            /* process 2 elements */
            for (j = (n >> 1); j--;)
            {
                ST_SP(zero, px);
                ST_SP(zero, py);

                px += 4;
                py += 4;
            }
            if (n & 1)
            {
                px[0] = 0;
                px[1] = 0;
                py[0] = 0;
                py[1] = 0;
            }
        }
        else if ((1 == c) && (1 == s))
        {
            if (n >> 4)
            {
                BLASLONG pref_offsetx, pref_offsety;

                pref_offsetx = (BLASLONG)px & (L1_DATA_LINESIZE - 1);
                if (pref_offsetx > 0)
                {
                    pref_offsetx = L1_DATA_LINESIZE - pref_offsetx;
                    pref_offsetx = pref_offsetx / sizeof(FLOAT);
                }

                pref_offsety = (BLASLONG)py & (L1_DATA_LINESIZE - 1);
                if (pref_offsety > 0)
                {
                    pref_offsety = L1_DATA_LINESIZE - pref_offsety;
                    pref_offsety = pref_offsety / sizeof(FLOAT);
                }

                x0 = LD_SP(px); px += 4;
                x1 = LD_SP(px); px += 4;
                x2 = LD_SP(px); px += 4;
                x3 = LD_SP(px); px += 4;
                y0 = LD_SP(py); py += 4;
                y1 = LD_SP(py); py += 4;
                y2 = LD_SP(py); py += 4;
                y3 = LD_SP(py); py += 4;

                for (j = (n >> 4) - 1; j--;)
                {
                    PREFETCH(px + pref_offsetx + 32);
                    PREFETCH(px + pref_offsetx + 40);
                    PREFETCH(px + pref_offsetx + 48);
                    PREFETCH(px + pref_offsetx + 56);
                    PREFETCH(py + pref_offsety + 32);
                    PREFETCH(py + pref_offsety + 40);
                    PREFETCH(py + pref_offsety + 48);
                    PREFETCH(py + pref_offsety + 56);

                    out0 = x0 + y0;
                    x4 = LD_SP(px); px += 4;
                    out1 = y0 - x0;
                    x5 = LD_SP(px); px += 4;
                    out2 = x1 + y1;
                    x6 = LD_SP(px); px += 4;
                    out3 = y1 - x1;
                    x7 = LD_SP(px); px += 4;
                    out4 = x2 + y2;
                    y4 = LD_SP(py); py += 4;
                    out5 = y2 - x2;
                    y5 = LD_SP(py); py += 4;
                    out6 = x3 + y3;
                    y6 = LD_SP(py); py += 4;
                    out7 = y3 - x3;
                    y7 = LD_SP(py); py += 4;

                    ST_SP(out0, x); x += 4;
                    out8 = x4 + y4;
                    ST_SP(out1, y); y += 4;
                    out9 = y4 - x4;
                    ST_SP(out2, x); x += 4;
                    out10 = x5 + y5;
                    ST_SP(out3, y); y += 4;
                    out11 = y5 - x5;
                    ST_SP(out4, x); x += 4;
                    out12 = x6 + y6;
                    ST_SP(out5, y); y += 4;
                    out13 = y6 - x6;
                    ST_SP(out6, x); x += 4;
                    out14 = x7 + y7;
                    ST_SP(out7, y); y += 4;
                    out15 = y7 - x7;

                    x0 = LD_SP(px); px += 4;
                    ST_SP(out8, x); x += 4;
                    x1 = LD_SP(px); px += 4;
                    ST_SP(out10, x); x += 4;
                    x2 = LD_SP(px); px += 4;
                    ST_SP(out12, x); x += 4;
                    x3 = LD_SP(px); px += 4;
                    ST_SP(out14, x); x += 4;

                    y0 = LD_SP(py); py += 4;
                    ST_SP(out9, y); y += 4;
                    y1 = LD_SP(py); py += 4;
                    ST_SP(out11, y); y += 4;
                    y2 = LD_SP(py); py += 4;
                    ST_SP(out13, y); y += 4;
                    y3 = LD_SP(py); py += 4;
                    ST_SP(out15, y); y += 4;
                }

                x4 = LD_SP(px); px += 4;
                x5 = LD_SP(px); px += 4;
                x6 = LD_SP(px); px += 4;
                x7 = LD_SP(px); px += 4;
                y4 = LD_SP(py); py += 4;
                y5 = LD_SP(py); py += 4;
                y6 = LD_SP(py); py += 4;
                y7 = LD_SP(py); py += 4;

                out0 = x0 + y0;
                out1 = y0 - x0;
                out2 = x1 + y1;
                out3 = y1 - x1;
                out4 = x2 + y2;
                out5 = y2 - x2;
                out6 = x3 + y3;
                out7 = y3 - x3;
                out8 = x4 + y4;
                out9 = y4 - x4;
                out10 = x5 + y5;
                out11 = y5 - x5;
                out12 = x6 + y6;
                out13 = y6 - x6;
                out14 = x7 + y7;
                out15 = y7 - x7;

                ST_SP8_INC(out0, out2, out4, out6, out8, out10, out12, out14, x, 4);
                ST_SP8_INC(out1, out3, out5, out7, out9, out11, out13, out15, y, 4);
            }
            if (n & 8)
            {
                LD_SP4_INC(px, 4, x0, x1, x2, x3);
                LD_SP4_INC(py, 4, y0, y1, y2, y3);

                out0 = x0 + y0;
                out1 = y0 - x0;
                out2 = x1 + y1;
                out3 = y1 - x1;
                out4 = x2 + y2;
                out5 = y2 - x2;
                out6 = x3 + y3;
                out7 = y3 - x3;

                ST_SP4_INC(out0, out2, out4, out6, x, 4);
                ST_SP4_INC(out1, out3, out5, out7, y, 4);
            }
            if (n & 4)
            {
                LD_SP2_INC(px, 4, x0, x1);
                LD_SP2_INC(py, 4, y0, y1);

                out0 = x0 + y0;
                out1 = y0 - x0;
                out2 = x1 + y1;
                out3 = y1 - x1;

                ST_SP2_INC(out0, out2, x, 4);
                ST_SP2_INC(out1, out3, y, 4);
            }
            if (n & 2)
            {
                x0 = LD_SP(px);
                y0 = LD_SP(py);
                px += 4;
                py += 4;

                out0 = x0 + y0;
                out1 = y0 - x0;

                ST_SP(out0, x);
                ST_SP(out1, y);
                x += 4;
                y += 4;
            }
            if (n & 1)
            {
                LD_GP2_INC(px, 1, fx0, fx1);
                LD_GP2_INC(py, 1, fy0, fy1);

                tp0 = fx0 + fy0;
                tp1 = fy0 - fx0;
                tp2 = fx1 + fy1;
                tp3 = fy1 - fx1;

                ST_GP2_INC(tp0, tp2, x, 1);
                ST_GP2_INC(tp1, tp3, y, 1);
            }
        }
        else if (0 == s)
        {

            c0 = COPY_FLOAT_TO_VECTOR(c);

            if (n >> 4)
            {
                BLASLONG pref_offsetx, pref_offsety;

                pref_offsetx = (BLASLONG)px & (L1_DATA_LINESIZE - 1);
                if (pref_offsetx > 0)
                {
                    pref_offsetx = L1_DATA_LINESIZE - pref_offsetx;
                    pref_offsetx = pref_offsetx / sizeof(FLOAT);
                }

                pref_offsety = (BLASLONG)py & (L1_DATA_LINESIZE - 1);
                if (pref_offsety > 0)
                {
                    pref_offsety = L1_DATA_LINESIZE - pref_offsety;
                    pref_offsety = pref_offsety / sizeof(FLOAT);
                }

                LD_SP8_INC(px, 4, x0, x1, x2, x3, x4, x5, x6, x7);

                for (j = (n >> 4) - 1; j--;)
                {
                    PREFETCH(px + pref_offsetx + 32);
                    PREFETCH(px + pref_offsetx + 40);
                    PREFETCH(px + pref_offsetx + 48);
                    PREFETCH(px + pref_offsetx + 56);
                    PREFETCH(py + pref_offsety + 32);
                    PREFETCH(py + pref_offsety + 40);
                    PREFETCH(py + pref_offsety + 48);
                    PREFETCH(py + pref_offsety + 56);

                    y0 = LD_SP(py); py += 4;
                    x0 *= c0;
                    y1 = LD_SP(py); py += 4;
                    x1 *= c0;
                    y2 = LD_SP(py); py += 4;
                    x2 *= c0;
                    y3 = LD_SP(py); py += 4;
                    x3 *= c0;
                    y4 = LD_SP(py); py += 4;
                    x4 *= c0;
                    y5 = LD_SP(py); py += 4;
                    x5 *= c0;
                    y6 = LD_SP(py); py += 4;
                    x6 *= c0;
                    y7 = LD_SP(py); py += 4;
                    x7 *= c0;

                    ST_SP(x0, x); x += 4;
                    y0 *= c0;
                    ST_SP(x1, x); x += 4;
                    y1 *= c0;
                    ST_SP(x2, x); x += 4;
                    y2 *= c0;
                    ST_SP(x3, x); x += 4;
                    y3 *= c0;
                    ST_SP(x4, x); x += 4;
                    y4 *= c0;
                    ST_SP(x5, x); x += 4;
                    y5 *= c0;
                    ST_SP(x6, x); x += 4;
                    y6 *= c0;
                    ST_SP(x7, x); x += 4;
                    y7 *= c0;

                    x0 = LD_SP(px); px += 4;
                    ST_SP(y0, y); y += 4;
                    x1 = LD_SP(px); px += 4;
                    ST_SP(y1, y); y += 4;
                    x2 = LD_SP(px); px += 4;
                    ST_SP(y2, y); y += 4;
                    x3 = LD_SP(px); px += 4;
                    ST_SP(y3, y); y += 4;
                    x4 = LD_SP(px); px += 4;
                    ST_SP(y4, y); y += 4;
                    x5 = LD_SP(px); px += 4;
                    ST_SP(y5, y); y += 4;
                    x6 = LD_SP(px); px += 4;
                    ST_SP(y6, y); y += 4;
                    x7 = LD_SP(px); px += 4;
                    ST_SP(y7, y); y += 4;
                }

                LD_SP8_INC(py, 4, y0, y1, y2, y3, y4, y5, y6, y7);

                x0 *= c0;
                y0 *= c0;
                x1 *= c0;
                y1 *= c0;
                x2 *= c0;
                y2 *= c0;
                x3 *= c0;
                y3 *= c0;
                x4 *= c0;
                y4 *= c0;
                x5 *= c0;
                y5 *= c0;
                x6 *= c0;
                y6 *= c0;
                x7 *= c0;
                y7 *= c0;

                ST_SP8_INC(x0, x1, x2, x3, x4, x5, x6, x7, x, 4);
                ST_SP8_INC(y0, y1, y2, y3, y4, y5, y6, y7, y, 4);
            }
            if (n & 8)
            {
                LD_SP4_INC(px, 4, x0, x1, x2, x3);
                LD_SP4_INC(py, 4, y0, y1, y2, y3);

                x0 *= c0;
                y0 *= c0;
                x1 *= c0;
                y1 *= c0;
                x2 *= c0;
                y2 *= c0;
                x3 *= c0;
                y3 *= c0;

                ST_SP4_INC(x0, x1, x2, x3, x, 4);
                ST_SP4_INC(y0, y1, y2, y3, y, 4);
            }
            if (n & 4)
            {
                LD_SP2_INC(px, 4, x0, x1);
                LD_SP2_INC(py, 4, y0, y1);

                x0 *= c0;
                y0 *= c0;
                x1 *= c0;
                y1 *= c0;

                ST_SP2_INC(x0, x1, x, 4);
                ST_SP2_INC(y0, y1, y, 4);
            }
            if (n & 2)
            {
                x0 = LD_SP(px);
                y0 = LD_SP(py);
                px += 4;
                py += 4;

                x0 *= c0;
                y0 *= c0;

                ST_SP(x0, x);
                ST_SP(y0, y);
                x += 4;
                y += 4;
            }
            if (n & 1)
            {
                LD_GP2_INC(px, 1, fx0, fx1);
                LD_GP2_INC(py, 1, fy0, fy1);

                tp0 = (c * fx0);
                tp1 = (c * fy0);
                tp2 = (c * fx1);
                tp3 = (c * fy1);

                ST_GP2_INC(tp0, tp2, x, 1);
                ST_GP2_INC(tp1, tp3, y, 1);
            }
        }
        else if (0 == c)
        {
            s0 = COPY_FLOAT_TO_VECTOR(s);

            /* process 16 floats */
            if (n >> 4)
            {
                BLASLONG pref_offsetx, pref_offsety;

                pref_offsetx = (BLASLONG)px & (L1_DATA_LINESIZE - 1);
                if (pref_offsetx > 0)
                {
                    pref_offsetx = L1_DATA_LINESIZE - pref_offsetx;
                    pref_offsetx = pref_offsetx / sizeof(FLOAT);
                }

                pref_offsety = (BLASLONG)py & (L1_DATA_LINESIZE - 1);
                if (pref_offsety > 0)
                {
                    pref_offsety = L1_DATA_LINESIZE - pref_offsety;
                    pref_offsety = pref_offsety / sizeof(FLOAT);
                }

                LD_SP4_INC(px, 4, x0, x1, x2, x3);
                LD_SP4_INC(py, 4, y0, y1, y2, y3);

                for (j = (n >> 4) - 1; j--;)
                {
                    PREFETCH(px + pref_offsetx + 32);
                    PREFETCH(px + pref_offsetx + 40);
                    PREFETCH(px + pref_offsetx + 48);
                    PREFETCH(px + pref_offsetx + 56);
                    PREFETCH(py + pref_offsety + 32);
                    PREFETCH(py + pref_offsety + 40);
                    PREFETCH(py + pref_offsety + 48);
                    PREFETCH(py + pref_offsety + 56);

                    x4 = LD_SP(px); px += 4;
                    out0 = s0 * y0;
                    x5 = LD_SP(px); px += 4;
                    out2 = s0 * y1;
                    x6 = LD_SP(px); px += 4;
                    out4 = s0 * y2;
                    x7 = LD_SP(px); px += 4;
                    out6 = s0 * y3;
                    y4 = LD_SP(py); py += 4;
                    out1 = -(s0 * x0);
                    y5 = LD_SP(py); py += 4;
                    out3 = -(s0 * x1);
                    y6 = LD_SP(py); py += 4;
                    out5 = -(s0 * x2);
                    y7 = LD_SP(py); py += 4;
                    out7 = -(s0 * x3);

                    ST_SP(out0, x); x += 4;
                    out0 = s0 * y4;
                    ST_SP(out2, x); x += 4;
                    out2 = s0 * y5;
                    ST_SP(out4, x); x += 4;
                    out4 = s0 * y6;
                    ST_SP(out6, x); x += 4;
                    out6 = s0 * y7;
                    ST_SP(out1, y); y += 4;
                    out1 = -(s0 * x4);
                    ST_SP(out3, y); y += 4;
                    out3 = -(s0 * x5);
                    ST_SP(out5, y); y += 4;
                    out5 = -(s0 * x6);
                    ST_SP(out7, y); y += 4;
                    out7 = -(s0 * x7);

                    x0 = LD_SP(px); px += 4;
                    ST_SP(out0, x); x += 4;
                    x1 = LD_SP(px); px += 4;
                    ST_SP(out2, x); x += 4;
                    x2 = LD_SP(px); px += 4;
                    ST_SP(out4, x); x += 4;
                    x3 = LD_SP(px); px += 4;
                    ST_SP(out6, x); x += 4;
                    y0 = LD_SP(py); py += 4;
                    ST_SP(out1, y); y += 4;
                    y1 = LD_SP(py); py += 4;
                    ST_SP(out3, y); y += 4;
                    y2 = LD_SP(py); py += 4;
                    ST_SP(out5, y); y += 4;
                    y3 = LD_SP(py); py += 4;
                    ST_SP(out7, y); y += 4;
                }

                out0 = s0 * y0;
                out2 = s0 * y1;
                out4 = s0 * y2;
                out6 = s0 * y3;
                out1 = -(s0 * x0);
                out3 = -(s0 * x1);
                out5 = -(s0 * x2);
                out7 = -(s0 * x3);

                ST_SP4_INC(out0, out2, out4, out6, x, 4);
                ST_SP4_INC(out1, out3, out5, out7, y, 4);

                LD_SP4_INC(px, 4, x4, x5, x6, x7);
                LD_SP4_INC(py, 4, y4, y5, y6, y7);

                out0 = s0 * y4;
                out2 = s0 * y5;
                out4 = s0 * y6;
                out6 = s0 * y7;
                out1 = -(s0 * x4);
                out3 = -(s0 * x5);
                out5 = -(s0 * x6);
                out7 = -(s0 * x7);

                ST_SP4_INC(out0, out2, out4, out6, x, 4);
                ST_SP4_INC(out1, out3, out5, out7, y, 4);
            }
            if (n & 8)
            {
                LD_SP4_INC(px, 4, x0, x1, x2, x3);
                LD_SP4_INC(py, 4, y0, y1, y2, y3);

                out0 = s0 * y0;
                out1 = - (s0 * x0);
                out2 = s0 * y1;
                out3 = - (s0 * x1);
                out4 = s0 * y2;
                out5 = - (s0 * x2);
                out6 = s0 * y3;
                out7 = - (s0 * x3);

                ST_SP4_INC(out0, out2, out4, out6, x, 4);
                ST_SP4_INC(out1, out3, out5, out7, y, 4);
            }
            if (n & 4)
            {
                LD_SP2_INC(px, 4, x0, x1);
                LD_SP2_INC(py, 4, y0, y1);

                out0 = s0 * y0;
                out1 = - (s0 * x0);
                out2 = s0 * y1;
                out3 = - (s0 * x1);

                ST_SP2_INC(out0, out2, x, 4);
                ST_SP2_INC(out1, out3, y, 4);
            }
            if (n & 2)
            {
                x0 = LD_SP(px); px += 4;
                y0 = LD_SP(py); py += 4;

                out0 = s0 * y0;
                out1 = - (s0 * x0);

                ST_SP(out0, x); x += 4;
                ST_SP(out1, y); y += 4;
            }
            if (n & 1)
            {
                LD_GP2_INC(px, 1, fx0, fx1);
                LD_GP2_INC(py, 1, fy0, fy1);

                tp0 = s * fy0;
                tp1 = - (s * fx0);
                tp2 = s * fy1;
                tp3 = - (s * fx1);

                ST_GP2_INC(tp0, tp2, x, 1);
                ST_GP2_INC(tp1, tp3, y, 1);
            }
        }
        else
        {
            c0 = COPY_FLOAT_TO_VECTOR(c);
            s0 = COPY_FLOAT_TO_VECTOR(s);

            if (n >> 4)
            {
                BLASLONG pref_offsetx, pref_offsety;

                pref_offsetx = (BLASLONG)px & (L1_DATA_LINESIZE - 1);
                if (pref_offsetx > 0)
                {
                    pref_offsetx = L1_DATA_LINESIZE - pref_offsetx;
                    pref_offsetx = pref_offsetx / sizeof(FLOAT);
                }

                pref_offsety = (BLASLONG)py & (L1_DATA_LINESIZE - 1);
                if (pref_offsety > 0)
                {
                    pref_offsety = L1_DATA_LINESIZE - pref_offsety;
                    pref_offsety = pref_offsety / sizeof(FLOAT);
                }

                LD_SP4_INC(px, 4, x0, x1, x2, x3);
                LD_SP4_INC(py, 4, y0, y1, y2, y3);

                for (j = (n >> 4) - 1; j--;)
                {
                    PREFETCH(px + pref_offsetx + 32);
                    PREFETCH(px + pref_offsetx + 40);
                    PREFETCH(px + pref_offsetx + 48);
                    PREFETCH(px + pref_offsetx + 56);
                    PREFETCH(py + pref_offsety + 32);
                    PREFETCH(py + pref_offsety + 40);
                    PREFETCH(py + pref_offsety + 48);
                    PREFETCH(py + pref_offsety + 56);

                    x4 = LD_SP(px); px += 4;
                    out0 = c0 * x0;
                    x5 = LD_SP(px); px += 4;
                    out1 = c0 * y0;
                    x6 = LD_SP(px); px += 4;
                    out2 = c0 * x1;
                    x7 = LD_SP(px); px += 4;
                    out3 = c0 * y1;
                    y4 = LD_SP(py); py += 4;
                    out4 = c0 * x2;
                    y5 = LD_SP(py); py += 4;
                    out5 = c0 * y2;
                    y6 = LD_SP(py); py += 4;
                    out6 = c0 * x3;
                    y7 = LD_SP(py); py += 4;
                    out7 = c0 * y3;

                    out0 += s0 * y0;
                    out1 -= s0 * x0;
                    out2 += s0 * y1;
                    out3 -= s0 * x1;
                    out4 += s0 * y2;
                    out5 -= s0 * x2;
                    out6 += s0 * y3;
                    out7 -= s0 * x3;

                    ST_SP(out0, x); x += 4;
                    out8 = c0 * x4;
                    ST_SP(out2, x); x += 4;
                    out9 = c0 * y4;
                    ST_SP(out4, x); x += 4;
                    out10 = c0 * x5;
                    ST_SP(out6, x); x += 4;
                    out11 = c0 * y5;
                    ST_SP(out1, y); y += 4;
                    out12 = c0 * x6;
                    ST_SP(out3, y); y += 4;
                    out13 = c0 * y6;
                    ST_SP(out5, y); y += 4;
                    out14 = c0 * x7;
                    ST_SP(out7, y); y += 4;
                    out15 = c0 * y7;

                    x0 = LD_SP(px); px += 4;
                    out8 += s0 * y4;
                    x1 = LD_SP(px); px += 4;
                    out9 -= s0 * x4;
                    x2 = LD_SP(px); px += 4;
                    out10 += s0 * y5;
                    x3 = LD_SP(px); px += 4;
                    out11 -= s0 * x5;
                    y0 = LD_SP(py); py += 4;
                    out12 += s0 * y6;
                    y1 = LD_SP(py); py += 4;
                    out13 -= s0 * x6;
                    y2 = LD_SP(py); py += 4;
                    out14 += s0 * y7;
                    y3 = LD_SP(py); py += 4;
                    out15 -= s0 * x7;

                    ST_SP(out8,  x); x += 4;
                    ST_SP(out10, x); x += 4;
                    ST_SP(out12, x); x += 4;
                    ST_SP(out14, x); x += 4;
                    ST_SP(out9,  y); y += 4;
                    ST_SP(out11, y); y += 4;
                    ST_SP(out13, y); y += 4;
                    ST_SP(out15, y); y += 4;
                }

                out0 = c0 * x0;
                out0 += s0 * y0;
                out1 = c0 * y0;
                out1 -= s0 * x0;
                out2 = c0 * x1;
                out2 += s0 * y1;
                out3 = c0 * y1;
                out3 -= s0 * x1;
                out4 = c0 * x2;
                out4 += s0 * y2;
                out5 = c0 * y2;
                out5 -= s0 * x2;
                out6 = c0 * x3;
                out6 += s0 * y3;
                out7 = c0 * y3;
                out7 -= s0 * x3;

                ST_SP4_INC(out0, out2, out4, out6, x, 4);
                ST_SP4_INC(out1, out3, out5, out7, y, 4);

                LD_SP4_INC(px, 4, x4, x5, x6, x7);
                LD_SP4_INC(py, 4, y4, y5, y6, y7);

                out8 = c0 * x4;
                out8 += s0 * y4;
                out9 = c0 * y4;
                out9 -= s0 * x4;
                out10 = c0 * x5;
                out10 += s0 * y5;
                out11 = c0 * y5;
                out11 -= s0 * x5;
                out12 = c0 * x6;
                out12 += s0 * y6;
                out13 = c0 * y6;
                out13 -= s0 * x6;
                out14 = c0 * x7;
                out14 += s0 * y7;
                out15 = c0 * y7;
                out15 -= s0 * x7;

                ST_SP4_INC(out8, out10, out12, out14, x, 4);
                ST_SP4_INC(out9, out11, out13, out15, y, 4);
            }
            if (n & 8)
            {
                LD_SP4_INC(px, 4, x0, x1, x2, x3);
                LD_SP4_INC(py, 4, y0, y1, y2, y3);

                out0 = (c0 * x0) + (s0 * y0);
                out1 = (c0 * y0) - (s0 * x0);
                out2 = (c0 * x1) + (s0 * y1);
                out3 = (c0 * y1) - (s0 * x1);
                out4 = (c0 * x2) + (s0 * y2);
                out5 = (c0 * y2) - (s0 * x2);
                out6 = (c0 * x3) + (s0 * y3);
                out7 = (c0 * y3) - (s0 * x3);

                ST_SP4_INC(out0, out2, out4, out6, x, 4);
                ST_SP4_INC(out1, out3, out5, out7, y, 4);
            }
            if (n & 4)
            {
                LD_SP2_INC(px, 4, x0, x1);
                LD_SP2_INC(py, 4, y0, y1);

                out0 = (c0 * x0) + (s0 * y0);
                out1 = (c0 * y0) - (s0 * x0);
                out2 = (c0 * x1) + (s0 * y1);
                out3 = (c0 * y1) - (s0 * x1);

                ST_SP2_INC(out0, out2, x, 4);
                ST_SP2_INC(out1, out3, y, 4);
            }
            if (n & 2)
            {
                x0 = LD_SP(px);
                y0 = LD_SP(py);
                px += 4;
                py += 4;

                out0 = (c0 * x0) + (s0 * y0);
                out1 = (c0 * y0) - (s0 * x0);

                ST_SP(out0, x);
                ST_SP(out1, y);
                x += 4;
                y += 4;
            }
            if (n & 1)
            {
                LD_GP2_INC(px, 1, fx0, fx1);
                LD_GP2_INC(py, 1, fy0, fy1);

                tp0 = (c * fx0) + (s * fy0);
                tp1 = (c * fy0) - (s * fx0);
                tp2 = (c * fx1) + (s * fy1);
                tp3 = (c * fy1) - (s * fx1);

                ST_GP2_INC(tp0, tp2, x, 1);
                ST_GP2_INC(tp1, tp3, y, 1);
            }
        }
    }
    else
    {
        inc_x2 = 2 * inc_x;
        inc_y2 = 2 * inc_y;

        if ((0 == c) && (0 == s))
        {
            for (i = n; i--;)
            {
                *x = 0;
                *(x + 1) = 0;
                *y = 0;
                *(y + 1) = 0;

                x += inc_x2;
                y += inc_y2;
            }
        }
        else if ((1 == c) && (1 == s))
        {
            if (n >> 1)
            {
                fx0 = *px;
                fx1 = *(px+1); px += inc_x2;
                fx2 = *px;
                fx3 = *(px+1); px += inc_x2;

                fy0 = *py;
                fy1 = *(py+1); py += inc_y2;
                fy2 = *py;
                fy3 = *(py+1); py += inc_y2;

                for (i = (n >> 1) - 1; i--;)
                {
                    tp0 = fx0 + fy0;
                    tp1 = fx1 + fy1;
                    tp2 = fy0 - fx0;
                    tp3 = fy1 - fx1;
                    tp4 = fx2 + fy2;
                    tp5 = fx3 + fy3;
                    tp6 = fy2 - fx2;
                    tp7 = fy3 - fx3;

                    fx0 = *px;
                    *x = tp0;
                    fx1 = *(px+1); px += inc_x2;
                    *(x+1) = tp1; x += inc_x2;
                    fx2 = *px;
                    *x = tp4;
                    fx3 = *(px+1); px += inc_x2;
                    *(x+1) = tp5; x += inc_x2;

                    fy0 = *py;
                    *y = tp2;
                    fy1 = *(py+1); py += inc_y2;
                    *(y+1) = tp3; y += inc_y2;
                    fy2 = *py;
                    *y = tp6;
                    fy3 = *(py+1); py += inc_y2;
                    *(y+1) = tp7; y += inc_y2;
                }

                tp0 = fx0 + fy0;
                tp1 = fx1 + fy1;
                tp2 = fy0 - fx0;
                tp3 = fy1 - fx1;
                tp4 = fx2 + fy2;
                tp5 = fx3 + fy3;
                tp6 = fy2 - fx2;
                tp7 = fy3 - fx3;

                *x = tp0;
                *(x+1) = tp1; x += inc_x2;
                *x = tp4;
                *(x+1) = tp5; x += inc_x2;

                *y = tp2;
                *(y+1) = tp3; y += inc_y2;
                *y = tp6;
                *(y+1) = tp7; y += inc_y2;
            }
            if (n & 1)
            {
                fx0 = *px;
                fx1 = *(px+1);

                fy0 = *py;
                fy1 = *(py+1);

                tp0 = fx0 + fy0;
                tp1 = fx1 + fy1;
                tp2 = fy0 - fx0;
                tp3 = fy1 - fx1;

                *x = tp0;
                *(x+1) = tp1;

                *y = tp2;
                *(y+1) = tp3;
            }
        }
        else if (0 == s)
        {
            if (n >> 1)
            {
                fx0 = *px;
                fx1 = *(px+1); px += inc_x2;
                fx2 = *px;
                fx3 = *(px+1); px += inc_x2;

                fy0 = *py;
                fy1 = *(py+1); py += inc_y2;
                fy2 = *py;
                fy3 = *(py+1); py += inc_y2;

                for (i = (n >> 1) - 1; i--;)
                {
                    tp0 = c * fx0;
                    tp1 = c * fx1;
                    tp2 = c * fx2;
                    tp3 = c * fx3;
                    tp4 = c * fy0;
                    tp5 = c * fy1;
                    tp6 = c * fy2;
                    tp7 = c * fy3;

                    fx0 = *px;
                    *x = tp0;
                    fx1 = *(px+1); px += inc_x2;
                    *(x+1) = tp1; x += inc_x2;
                    fx2 = *px;
                    *x = tp2;
                    fx3 = *(px+1); px += inc_x2;
                    *(x+1) = tp3; x += inc_x2;
                    fy0 = *py;
                    *y = tp4;
                    fy1 = *(py+1); py += inc_y2;
                    *(y+1) = tp5; y += inc_y2;
                    fy2 = *py;
                    *y = tp6;
                    fy3 = *(py+1); py += inc_y2;
                    *(y+1) = tp7; y += inc_y2;
                }

                tp0 = c * fx0;
                tp1 = c * fx1;
                tp2 = c * fx2;
                tp3 = c * fx3;
                tp4 = c * fy0;
                tp5 = c * fy1;
                tp6 = c * fy2;
                tp7 = c * fy3;

                *x = tp0;
                *(x+1) = tp1; x += inc_x2;
                *x = tp2;
                *(x+1) = tp3; x += inc_x2;

                *y = tp4;
                *(y+1) = tp5; y += inc_y2;
                *y = tp6;
                *(y+1) = tp7; y += inc_y2;
            }
            if (n & 1)
            {
                fx0 = *px;
                fx1 = *(px+1);

                fy0 = *py;
                fy1 = *(py+1);

                tp0 = c * fx0;
                tp1 = c * fx1;
                tp2 = c * fy0;
                tp3 = c * fy1;

                *x = tp0;
                *(x+1) = tp1;

                *y = tp2;
                *(y+1) = tp3;
            }
        }
        else
        {
            if (n >> 1)
            {
                fx0 = *px;
                fx1 = *(px+1); px += inc_x2;
                fx2 = *px;
                fx3 = *(px+1); px += inc_x2;
                fy0 = *py;
                fy1 = *(py+1); py += inc_y2;
                fy2 = *py;
                fy3 = *(py+1); py += inc_y2;

                for (i = (n >> 1) - 1; i--;)
                {
                    tp0 = c * fx0 + s * fy0;
                    tp1 = c * fx1 + s * fy1;
                    tp2 = c * fy0 - s * fx0;
                    tp3 = c * fy1 - s * fx1;
                    tp4 = c * fx2 + s * fy2;
                    tp5 = c * fx3 + s * fy3;
                    tp6 = c * fy2 - s * fx2;
                    tp7 = c * fy3 - s * fx3;

                    fx0 = *px;
                    *x = tp0;
                    fx1 = *(px+1); px += inc_x2;
                    *(x+1) = tp1; x += inc_x2;
                    fx2 = *px;
                    *x = tp4;
                    fx3 = *(px+1); px += inc_x2;
                    *(x+1) = tp5; x += inc_x2;
                    fy0 = *py;
                    *y = tp2;
                    fy1 = *(py+1); py += inc_y2;
                    *(y+1) = tp3; y += inc_y2;
                    fy2 = *py;
                    *y = tp6;
                    fy3 = *(py+1); py += inc_y2;
                    *(y+1) = tp7; y += inc_y2;
                }

                tp0 = c * fx0 + s * fy0;
                tp1 = c * fx1 + s * fy1;
                tp2 = c * fy0 - s * fx0;
                tp3 = c * fy1 - s * fx1;
                tp4 = c * fx2 + s * fy2;
                tp5 = c * fx3 + s * fy3;
                tp6 = c * fy2 - s * fx2;
                tp7 = c * fy3 - s * fx3;

                *x = tp0;
                *(x+1) = tp1; x += inc_x2;
                *x = tp4;
                *(x+1) = tp5; x += inc_x2;
                *y = tp2;
                *(y+1) = tp3; y += inc_y2;
                *y = tp6;
                *(y+1) = tp7; y += inc_y2;
            }
            if (n & 1)
            {
                fx0 = *px;
                fx1 = *(px+1);

                fy0 = *py;
                fy1 = *(py+1);

                tp0 = c * fx0 + s * fy0;
                tp1 = c * fx1 + s * fy1;
                tp2 = c * fy0 - s * fx0;
                tp3 = c * fy1 - s * fx1;

                *x = tp0;
                *(x+1) = tp1;

                *y = tp2;
                *(y+1) = tp3;
            }
        }
    }

    return 0;
}
