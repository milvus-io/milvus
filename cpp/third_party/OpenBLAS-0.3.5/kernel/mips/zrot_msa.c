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

#define PROCESS_ZROT(inc_x2, inc_y2)                                                   \
    if ((0 == c) && (0 == s))                                                          \
    {                                                                                  \
        v2f64 zero = {0, 0};                                                           \
        zero = (v2f64) __msa_insert_d((v2i64) zero, 0, 0.0);                           \
        zero = (v2f64) __msa_insert_d((v2i64) zero, 1, 0.0);                           \
                                                                                       \
        /* process 4 floats */                                                         \
        for (j = (n >> 1); j--;)                                                       \
        {                                                                              \
            ST_DP2_INC(zero, zero, px, inc_x2);                                        \
            ST_DP2_INC(zero, zero, py, inc_y2);                                        \
        }                                                                              \
                                                                                       \
        if (n & 1)                                                                     \
        {                                                                              \
            ST_DP(zero, px);                                                           \
            ST_DP(zero, py);                                                           \
        }                                                                              \
    }                                                                                  \
    else if ((1 == c) && (1 == s))                                                     \
    {                                                                                  \
        /* process 8 elements */                                                       \
        if (n >> 3)                                                                    \
        {                                                                              \
            BLASLONG pref_offsetx, pref_offsety;                                           \
                                                                                       \
            pref_offsetx = (BLASLONG)px & (L1_DATA_LINESIZE - 1);                          \
            if (pref_offsetx > 0)                                                      \
            {                                                                          \
                pref_offsetx = L1_DATA_LINESIZE - pref_offsetx;                        \
                pref_offsetx = pref_offsetx / sizeof(FLOAT);                           \
            }                                                                          \
                                                                                       \
            pref_offsety = (BLASLONG)py & (L1_DATA_LINESIZE - 1);                          \
            if (pref_offsety > 0)                                                      \
            {                                                                          \
                pref_offsety = L1_DATA_LINESIZE - pref_offsety;                        \
                pref_offsety = pref_offsety / sizeof(FLOAT);                           \
            }                                                                          \
                                                                                       \
            x0 = LD_DP(px); px += inc_x2;                                              \
            x1 = LD_DP(px); px += inc_x2;                                              \
            x2 = LD_DP(px); px += inc_x2;                                              \
            x3 = LD_DP(px); px += inc_x2;                                              \
            y0 = LD_DP(py); py += inc_y2;                                              \
            y1 = LD_DP(py); py += inc_y2;                                              \
            y2 = LD_DP(py); py += inc_y2;                                              \
            y3 = LD_DP(py); py += inc_y2;                                              \
                                                                                       \
            for (j = (n >> 3) - 1; j--;)                                               \
            {                                                                          \
                PREFETCH(px + pref_offsetx + 16);                                      \
                PREFETCH(px + pref_offsetx + 20);                                      \
                PREFETCH(px + pref_offsetx + 24);                                      \
                PREFETCH(px + pref_offsetx + 28);                                      \
                PREFETCH(py + pref_offsety + 16);                                      \
                PREFETCH(py + pref_offsety + 20);                                      \
                PREFETCH(py + pref_offsety + 24);                                      \
                PREFETCH(py + pref_offsety + 28);                                      \
                                                                                       \
                out0 = x0 + y0;                                                        \
                x4 = LD_DP(px); px += inc_x2;                                          \
                out1 = y0 - x0;                                                        \
                x5 = LD_DP(px); px += inc_x2;                                          \
                out2 = x1 + y1;                                                        \
                x6 = LD_DP(px); px += inc_x2;                                          \
                out3 = y1 - x1;                                                        \
                x7 = LD_DP(px); px += inc_x2;                                          \
                out4 = x2 + y2;                                                        \
                y4 = LD_DP(py); py += inc_y2;                                          \
                out5 = y2 - x2;                                                        \
                y5 = LD_DP(py); py += inc_y2;                                          \
                out6 = x3 + y3;                                                        \
                y6 = LD_DP(py); py += inc_y2;                                          \
                out7 = y3 - x3;                                                        \
                y7 = LD_DP(py); py += inc_y2;                                          \
                                                                                       \
                ST_DP(out0, x); x += inc_x2;                                           \
                out8 = x4 + y4;                                                        \
                ST_DP(out1, y); y += inc_y2;                                           \
                out9 = y4 - x4;                                                        \
                ST_DP(out2, x); x += inc_x2;                                           \
                out10 = x5 + y5;                                                       \
                ST_DP(out3, y); y += inc_y2;                                           \
                out11 = y5 - x5;                                                       \
                ST_DP(out4, x); x += inc_x2;                                           \
                out12 = x6 + y6;                                                       \
                ST_DP(out5, y); y += inc_y2;                                           \
                out13 = y6 - x6;                                                       \
                ST_DP(out6, x); x += inc_x2;                                           \
                out14 = x7 + y7;                                                       \
                ST_DP(out7, y); y += inc_y2;                                           \
                out15 = y7 - x7;                                                       \
                                                                                       \
                x0 = LD_DP(px); px += inc_x2;                                          \
                ST_DP(out8, x); x += inc_x2;                                           \
                x1 = LD_DP(px); px += inc_x2;                                          \
                ST_DP(out10, x); x += inc_x2;                                          \
                x2 = LD_DP(px); px += inc_x2;                                          \
                ST_DP(out12, x); x += inc_x2;                                          \
                x3 = LD_DP(px); px += inc_x2;                                          \
                ST_DP(out14, x); x += inc_x2;                                          \
                                                                                       \
                y0 = LD_DP(py); py += inc_y2;                                          \
                ST_DP(out9, y); y += inc_y2;                                           \
                y1 = LD_DP(py); py += inc_y2;                                          \
                ST_DP(out11, y); y += inc_y2;                                          \
                y2 = LD_DP(py); py += inc_y2;                                          \
                ST_DP(out13, y); y += inc_y2;                                          \
                y3 = LD_DP(py); py += inc_y2;                                          \
                ST_DP(out15, y); y += inc_y2;                                          \
            }                                                                          \
                                                                                       \
            x4 = LD_DP(px); px += inc_x2;                                              \
            x5 = LD_DP(px); px += inc_x2;                                              \
            x6 = LD_DP(px); px += inc_x2;                                              \
            x7 = LD_DP(px); px += inc_x2;                                              \
            y4 = LD_DP(py); py += inc_y2;                                              \
            y5 = LD_DP(py); py += inc_y2;                                              \
            y6 = LD_DP(py); py += inc_y2;                                              \
            y7 = LD_DP(py); py += inc_y2;                                              \
                                                                                       \
            out0 = x0 + y0;                                                            \
            out1 = y0 - x0;                                                            \
            out2 = x1 + y1;                                                            \
            out3 = y1 - x1;                                                            \
            out4 = x2 + y2;                                                            \
            out5 = y2 - x2;                                                            \
            out6 = x3 + y3;                                                            \
            out7 = y3 - x3;                                                            \
            out8 = x4 + y4;                                                            \
            out9 = y4 - x4;                                                            \
            out10 = x5 + y5;                                                           \
            out11 = y5 - x5;                                                           \
            out12 = x6 + y6;                                                           \
            out13 = y6 - x6;                                                           \
            out14 = x7 + y7;                                                           \
            out15 = y7 - x7;                                                           \
                                                                                       \
            ST_DP8_INC(out0, out2, out4, out6, out8, out10, out12, out14, x, inc_x2);  \
            ST_DP8_INC(out1, out3, out5, out7, out9, out11, out13, out15, y, inc_y2);  \
        }                                                                              \
        if (n & 4)                                                                     \
        {                                                                              \
            LD_DP4_INC(px, inc_x2, x0, x1, x2, x3);                                    \
            LD_DP4_INC(py, inc_y2, y0, y1, y2, y3);                                    \
                                                                                       \
            out0 = x0 + y0;                                                            \
            out1 = y0 - x0;                                                            \
            out2 = x1 + y1;                                                            \
            out3 = y1 - x1;                                                            \
            out4 = x2 + y2;                                                            \
            out5 = y2 - x2;                                                            \
            out6 = x3 + y3;                                                            \
            out7 = y3 - x3;                                                            \
                                                                                       \
            ST_DP4_INC(out0, out2, out4, out6, x, inc_x2);                             \
            ST_DP4_INC(out1, out3, out5, out7, y, inc_y2);                             \
        }                                                                              \
        if (n & 2)                                                                     \
        {                                                                              \
            LD_DP2_INC(px, inc_x2, x0, x1);                                            \
            LD_DP2_INC(py, inc_y2, y0, y1);                                            \
                                                                                       \
            out0 = x0 + y0;                                                            \
            out1 = y0 - x0;                                                            \
            out2 = x1 + y1;                                                            \
            out3 = y1 - x1;                                                            \
                                                                                       \
            ST_DP2_INC(out0, out2, x, inc_x2);                                         \
            ST_DP2_INC(out1, out3, y, inc_y2);                                         \
        }                                                                              \
        if (n & 1)                                                                     \
        {                                                                              \
            x0 = LD_DP(px);                                                            \
            y0 = LD_DP(py);                                                            \
                                                                                       \
            out0 = x0 + y0;                                                            \
            out1 = y0 - x0;                                                            \
                                                                                       \
            ST_DP(out0, px);                                                           \
            ST_DP(out1, py);                                                           \
        }                                                                              \
    }                                                                                  \
    else if (0 == s)                                                                   \
    {                                                                                  \
        c0 = COPY_DOUBLE_TO_VECTOR(c);                                                 \
                                                                                       \
        if (n >> 3)                                                                    \
        {                                                                              \
            BLASLONG pref_offsetx, pref_offsety;                                           \
                                                                                       \
            pref_offsetx = (BLASLONG)px & (L1_DATA_LINESIZE - 1);                          \
            if (pref_offsetx > 0)                                                      \
            {                                                                          \
                pref_offsetx = L1_DATA_LINESIZE - pref_offsetx;                        \
                pref_offsetx = pref_offsetx / sizeof(FLOAT);                           \
            }                                                                          \
                                                                                       \
            pref_offsety = (BLASLONG)py & (L1_DATA_LINESIZE - 1);                          \
            if (pref_offsety > 0)                                                      \
            {                                                                          \
                pref_offsety = L1_DATA_LINESIZE - pref_offsety;                        \
                pref_offsety = pref_offsety / sizeof(FLOAT);                           \
            }                                                                          \
                                                                                       \
            LD_DP8_INC(px, inc_x2, x0, x1, x2, x3, x4, x5, x6, x7);                    \
                                                                                       \
            for (j = (n >> 3) - 1; j--;)                                               \
            {                                                                          \
                PREFETCH(px + pref_offsetx + 16);                                      \
                PREFETCH(px + pref_offsetx + 20);                                      \
                PREFETCH(px + pref_offsetx + 24);                                      \
                PREFETCH(px + pref_offsetx + 28);                                      \
                PREFETCH(py + pref_offsety + 16);                                      \
                PREFETCH(py + pref_offsety + 20);                                      \
                PREFETCH(py + pref_offsety + 24);                                      \
                PREFETCH(py + pref_offsety + 28);                                      \
                                                                                       \
                y0 = LD_DP(py); py += inc_y2;                                          \
                x0 *= c0;                                                              \
                y1 = LD_DP(py); py += inc_y2;                                          \
                x1 *= c0;                                                              \
                y2 = LD_DP(py); py += inc_y2;                                          \
                x2 *= c0;                                                              \
                y3 = LD_DP(py); py += inc_y2;                                          \
                x3 *= c0;                                                              \
                y4 = LD_DP(py); py += inc_y2;                                          \
                x4 *= c0;                                                              \
                y5 = LD_DP(py); py += inc_y2;                                          \
                x5 *= c0;                                                              \
                y6 = LD_DP(py); py += inc_y2;                                          \
                x6 *= c0;                                                              \
                y7 = LD_DP(py); py += inc_y2;                                          \
                x7 *= c0;                                                              \
                                                                                       \
                ST_DP(x0, x); x += inc_x2;                                             \
                y0 *= c0;                                                              \
                ST_DP(x1, x); x += inc_x2;                                             \
                y1 *= c0;                                                              \
                ST_DP(x2, x); x += inc_x2;                                             \
                y2 *= c0;                                                              \
                ST_DP(x3, x); x += inc_x2;                                             \
                y3 *= c0;                                                              \
                ST_DP(x4, x); x += inc_x2;                                             \
                y4 *= c0;                                                              \
                ST_DP(x5, x); x += inc_x2;                                             \
                y5 *= c0;                                                              \
                ST_DP(x6, x); x += inc_x2;                                             \
                y6 *= c0;                                                              \
                ST_DP(x7, x); x += inc_x2;                                             \
                y7 *= c0;                                                              \
                                                                                       \
                x0 = LD_DP(px); px += inc_x2;                                          \
                ST_DP(y0, y); y += inc_y2;                                             \
                x1 = LD_DP(px); px += inc_x2;                                          \
                ST_DP(y1, y); y += inc_y2;                                             \
                x2 = LD_DP(px); px += inc_x2;                                          \
                ST_DP(y2, y); y += inc_y2;                                             \
                x3 = LD_DP(px); px += inc_x2;                                          \
                ST_DP(y3, y); y += inc_y2;                                             \
                x4 = LD_DP(px); px += inc_x2;                                          \
                ST_DP(y4, y); y += inc_y2;                                             \
                x5 = LD_DP(px); px += inc_x2;                                          \
                ST_DP(y5, y); y += inc_y2;                                             \
                x6 = LD_DP(px); px += inc_x2;                                          \
                ST_DP(y6, y); y += inc_y2;                                             \
                x7 = LD_DP(px); px += inc_x2;                                          \
                ST_DP(y7, y); y += inc_y2;                                             \
            }                                                                          \
                                                                                       \
            LD_DP8_INC(py, inc_y2, y0, y1, y2, y3, y4, y5, y6, y7);                    \
                                                                                       \
            x0 *= c0;                                                                  \
            y0 *= c0;                                                                  \
            x1 *= c0;                                                                  \
            y1 *= c0;                                                                  \
            x2 *= c0;                                                                  \
            y2 *= c0;                                                                  \
            x3 *= c0;                                                                  \
            y3 *= c0;                                                                  \
            x4 *= c0;                                                                  \
            y4 *= c0;                                                                  \
            x5 *= c0;                                                                  \
            y5 *= c0;                                                                  \
            x6 *= c0;                                                                  \
            y6 *= c0;                                                                  \
            x7 *= c0;                                                                  \
            y7 *= c0;                                                                  \
                                                                                       \
            ST_DP8_INC(x0, x1, x2, x3, x4, x5, x6, x7, x, inc_x2);                     \
            ST_DP8_INC(y0, y1, y2, y3, y4, y5, y6, y7, y, inc_y2);                     \
        }                                                                              \
                                                                                       \
        if (n & 4)                                                                     \
        {                                                                              \
            LD_DP4_INC(px, inc_x2, x0, x1, x2, x3);                                    \
            LD_DP4_INC(py, inc_y2, y0, y1, y2, y3);                                    \
                                                                                       \
            out0 = c0 * x0;                                                            \
            out1 = c0 * y0;                                                            \
            out2 = c0 * x1;                                                            \
            out3 = c0 * y1;                                                            \
            out4 = c0 * x2;                                                            \
            out5 = c0 * y2;                                                            \
            out6 = c0 * x3;                                                            \
            out7 = c0 * y3;                                                            \
                                                                                       \
            ST_DP4_INC(out0, out2, out4, out6, x, inc_x2);                             \
            ST_DP4_INC(out1, out3, out5, out7, y, inc_y2);                             \
        }                                                                              \
        if (n & 2)                                                                     \
        {                                                                              \
            LD_DP2_INC(px, inc_x2, x0, x1);                                            \
            LD_DP2_INC(py, inc_y2, y0, y1);                                            \
                                                                                       \
            out0 = c0 * x0;                                                            \
            out1 = c0 * y0;                                                            \
            out2 = c0 * x1;                                                            \
            out3 = c0 * y1;                                                            \
                                                                                       \
            ST_DP2_INC(out0, out2, x, inc_x2);                                         \
            ST_DP2_INC(out1, out3, y, inc_y2);                                         \
        }                                                                              \
        if (n & 1)                                                                     \
        {                                                                              \
            x0 = LD_DP(px);                                                            \
            y0 = LD_DP(py);                                                            \
                                                                                       \
            out0 = c0 * x0;                                                            \
            out1 = c0 * y0;                                                            \
                                                                                       \
            ST_DP(out0, px);                                                           \
            ST_DP(out1, py);                                                           \
        }                                                                              \
    }                                                                                  \
    else if (0 == c)                                                                   \
    {                                                                                  \
        s0 = COPY_DOUBLE_TO_VECTOR(s);                                                 \
                                                                                       \
        /* process 16 floats */                                                        \
        if (n >> 3)                                                                    \
        {                                                                              \
            BLASLONG pref_offsetx, pref_offsety;                                           \
                                                                                       \
            pref_offsetx = (BLASLONG)px & (L1_DATA_LINESIZE - 1);                          \
            if (pref_offsetx > 0)                                                      \
            {                                                                          \
                pref_offsetx = L1_DATA_LINESIZE - pref_offsetx;                        \
                pref_offsetx = pref_offsetx / sizeof(FLOAT);                           \
            }                                                                          \
                                                                                       \
            pref_offsety = (BLASLONG)py & (L1_DATA_LINESIZE - 1);                          \
            if (pref_offsety > 0)                                                      \
            {                                                                          \
                pref_offsety = L1_DATA_LINESIZE - pref_offsety;                        \
                pref_offsety = pref_offsety / sizeof(FLOAT);                           \
            }                                                                          \
                                                                                       \
            LD_DP4_INC(px, inc_x2, x0, x1, x2, x3);                                    \
            LD_DP4_INC(py, inc_y2, y0, y1, y2, y3);                                    \
                                                                                       \
            for (j = (n >> 3) - 1; j--;)                                               \
            {                                                                          \
                PREFETCH(px + pref_offsetx + 16);                                      \
                PREFETCH(px + pref_offsetx + 20);                                      \
                PREFETCH(px + pref_offsetx + 24);                                      \
                PREFETCH(px + pref_offsetx + 28);                                      \
                PREFETCH(py + pref_offsety + 16);                                      \
                PREFETCH(py + pref_offsety + 20);                                      \
                PREFETCH(py + pref_offsety + 24);                                      \
                PREFETCH(py + pref_offsety + 28);                                      \
                                                                                       \
                x4 = LD_DP(px); px += inc_x2;                                          \
                out0 = s0 * y0;                                                        \
                x5 = LD_DP(px); px += inc_x2;                                          \
                out2 = s0 * y1;                                                        \
                x6 = LD_DP(px); px += inc_x2;                                          \
                out4 = s0 * y2;                                                        \
                x7 = LD_DP(px); px += inc_x2;                                          \
                out6 = s0 * y3;                                                        \
                y4 = LD_DP(py); py += inc_y2;                                          \
                out1 = -(s0 * x0);                                                     \
                y5 = LD_DP(py); py += inc_y2;                                          \
                out3 = -(s0 * x1);                                                     \
                y6 = LD_DP(py); py += inc_y2;                                          \
                out5 = -(s0 * x2);                                                     \
                y7 = LD_DP(py); py += inc_y2;                                          \
                out7 = -(s0 * x3);                                                     \
                                                                                       \
                ST_DP(out0, x); x += inc_y2;                                           \
                out0 = s0 * y4;                                                        \
                ST_DP(out2, x); x += inc_y2;                                           \
                out2 = s0 * y5;                                                        \
                ST_DP(out4, x); x += inc_y2;                                           \
                out4 = s0 * y6;                                                        \
                ST_DP(out6, x); x += inc_y2;                                           \
                out6 = s0 * y7;                                                        \
                ST_DP(out1, y); y += inc_y2;                                           \
                out1 = -(s0 * x4);                                                     \
                ST_DP(out3, y); y += inc_y2;                                           \
                out3 = -(s0 * x5);                                                     \
                ST_DP(out5, y); y += inc_y2;                                           \
                out5 = -(s0 * x6);                                                     \
                ST_DP(out7, y); y += inc_y2;                                           \
                out7 = -(s0 * x7);                                                     \
                                                                                       \
                x0 = LD_DP(px); px += inc_x2;                                          \
                ST_DP(out0, x); x += inc_y2;                                           \
                x1 = LD_DP(px); px += inc_x2;                                          \
                ST_DP(out2, x); x += inc_y2;                                           \
                x2 = LD_DP(px); px += inc_x2;                                          \
                ST_DP(out4, x); x += inc_y2;                                           \
                x3 = LD_DP(px); px += inc_x2;                                          \
                ST_DP(out6, x); x += inc_y2;                                           \
                y0 = LD_DP(py); py += inc_y2;                                          \
                ST_DP(out1, y); y += inc_y2;                                           \
                y1 = LD_DP(py); py += inc_y2;                                          \
                ST_DP(out3, y); y += inc_y2;                                           \
                y2 = LD_DP(py); py += inc_y2;                                          \
                ST_DP(out5, y); y += inc_y2;                                           \
                y3 = LD_DP(py); py += inc_y2;                                          \
                ST_DP(out7, y); y += inc_y2;                                           \
            }                                                                          \
                                                                                       \
            out0 = s0 * y0;                                                            \
            out2 = s0 * y1;                                                            \
            out4 = s0 * y2;                                                            \
            out6 = s0 * y3;                                                            \
            out1 = -(s0 * x0);                                                         \
            out3 = -(s0 * x1);                                                         \
            out5 = -(s0 * x2);                                                         \
            out7 = -(s0 * x3);                                                         \
                                                                                       \
            ST_DP4_INC(out0, out2, out4, out6, x, inc_x2);                             \
            ST_DP4_INC(out1, out3, out5, out7, y, inc_y2);                             \
                                                                                       \
            LD_DP4_INC(px, inc_x2, x4, x5, x6, x7);                                    \
            LD_DP4_INC(py, inc_y2, y4, y5, y6, y7);                                    \
                                                                                       \
            out0 = s0 * y4;                                                            \
            out2 = s0 * y5;                                                            \
            out4 = s0 * y6;                                                            \
            out6 = s0 * y7;                                                            \
            out1 = -(s0 * x4);                                                         \
            out3 = -(s0 * x5);                                                         \
            out5 = -(s0 * x6);                                                         \
            out7 = -(s0 * x7);                                                         \
                                                                                       \
            ST_DP4_INC(out0, out2, out4, out6, x, inc_x2);                             \
            ST_DP4_INC(out1, out3, out5, out7, y, inc_y2);                             \
        }                                                                              \
        if (n & 4)                                                                     \
        {                                                                              \
            LD_DP4_INC(px, inc_x2, x0, x1, x2, x3);                                    \
            LD_DP4_INC(py, inc_y2, y0, y1, y2, y3);                                    \
                                                                                       \
            out0 = s0 * y0;                                                            \
            out1 = - (s0 * x0);                                                        \
            out2 = s0 * y1;                                                            \
            out3 = - (s0 * x1);                                                        \
            out4 = s0 * y2;                                                            \
            out5 = - (s0 * x2);                                                        \
            out6 = s0 * y3;                                                            \
            out7 = - (s0 * x3);                                                        \
                                                                                       \
            ST_DP4_INC(out0, out2, out4, out6, x, inc_x2);                             \
            ST_DP4_INC(out1, out3, out5, out7, y, inc_y2);                             \
        }                                                                              \
        if (n & 2)                                                                     \
        {                                                                              \
            LD_DP2_INC(px, inc_x2, x0, x1);                                            \
            LD_DP2_INC(py, inc_y2, y0, y1);                                            \
                                                                                       \
            out0 = s0 * y0;                                                            \
            out1 = - (s0 * x0);                                                        \
            out2 = s0 * y1;                                                            \
            out3 = - (s0 * x1);                                                        \
                                                                                       \
            ST_DP2_INC(out0, out2, x, inc_x2);                                         \
            ST_DP2_INC(out1, out3, y, inc_y2);                                         \
        }                                                                              \
        if (n & 1)                                                                     \
        {                                                                              \
            x0 = LD_DP(px); px += inc_x2;                                              \
            y0 = LD_DP(py); py += inc_y2;                                              \
                                                                                       \
            out0 = s0 * y0;                                                            \
            out1 = - (s0 * x0);                                                        \
                                                                                       \
            ST_DP(out0, x); x += inc_x2;                                               \
            ST_DP(out1, y); y += inc_y2;                                               \
        }                                                                              \
    }                                                                                  \
    else                                                                               \
    {                                                                                  \
        c0 = COPY_DOUBLE_TO_VECTOR(c);                                                 \
        s0 = COPY_DOUBLE_TO_VECTOR(s);                                                 \
                                                                                       \
        if (n >> 3)                                                                    \
        {                                                                              \
            BLASLONG pref_offsetx, pref_offsety;                                           \
                                                                                       \
            pref_offsetx = (BLASLONG)px & (L1_DATA_LINESIZE - 1);                          \
            if (pref_offsetx > 0)                                                      \
            {                                                                          \
                pref_offsetx = L1_DATA_LINESIZE - pref_offsetx;                        \
                pref_offsetx = pref_offsetx / sizeof(FLOAT);                           \
            }                                                                          \
                                                                                       \
            pref_offsety = (BLASLONG)py & (L1_DATA_LINESIZE - 1);                          \
            if (pref_offsety > 0)                                                      \
            {                                                                          \
                pref_offsety = L1_DATA_LINESIZE - pref_offsety;                        \
                pref_offsety = pref_offsety / sizeof(FLOAT);                           \
            }                                                                          \
                                                                                       \
            LD_DP4_INC(px, inc_x2, x0, x1, x2, x3);                                    \
            LD_DP4_INC(py, inc_y2, y0, y1, y2, y3);                                    \
                                                                                       \
            for (j = (n >> 3) - 1; j--;)                                               \
            {                                                                          \
                PREFETCH(px + pref_offsetx + 16);                                      \
                PREFETCH(px + pref_offsetx + 20);                                      \
                PREFETCH(px + pref_offsetx + 24);                                      \
                PREFETCH(px + pref_offsetx + 28);                                      \
                PREFETCH(py + pref_offsety + 16);                                      \
                PREFETCH(py + pref_offsety + 20);                                      \
                PREFETCH(py + pref_offsety + 24);                                      \
                PREFETCH(py + pref_offsety + 28);                                      \
                                                                                       \
                x4 = LD_DP(px); px += inc_x2;                                          \
                out0 = c0 * x0;                                                        \
                x5 = LD_DP(px); px += inc_x2;                                          \
                out2 = c0 * x1;                                                        \
                x6 = LD_DP(px); px += inc_x2;                                          \
                out4 = c0 * x2;                                                        \
                x7 = LD_DP(px); px += inc_x2;                                          \
                out6 = c0 * x3;                                                        \
                y4 = LD_DP(py); py += inc_y2;                                          \
                out1 = c0 * y0;                                                        \
                y5 = LD_DP(py); py += inc_y2;                                          \
                out3 = c0 * y1;                                                        \
                y6 = LD_DP(py); py += inc_y2;                                          \
                out5 = c0 * y2;                                                        \
                y7 = LD_DP(py); py += inc_y2;                                          \
                out7 = c0 * y3;                                                        \
                                                                                       \
                out0 += s0 * y0;                                                       \
                out2 += s0 * y1;                                                       \
                out4 += s0 * y2;                                                       \
                out6 += s0 * y3;                                                       \
                out1 -= s0 * x0;                                                       \
                out3 -= s0 * x1;                                                       \
                out5 -= s0 * x2;                                                       \
                out7 -= s0 * x3;                                                       \
                                                                                       \
                ST_DP(out0, x); x += inc_x2;                                           \
                out0 = c0 * x4;                                                        \
                ST_DP(out2, x); x += inc_x2;                                           \
                out2 = c0 * x5;                                                        \
                ST_DP(out4, x); x += inc_x2;                                           \
                out4 = c0 * x6;                                                        \
                ST_DP(out6, x); x += inc_x2;                                           \
                out6 = c0 * x7;                                                        \
                ST_DP(out1, y); y += inc_y2;                                           \
                out1 = c0 * y4;                                                        \
                ST_DP(out3, y); y += inc_y2;                                           \
                out3 = c0 * y5;                                                        \
                ST_DP(out5, y); y += inc_y2;                                           \
                out5 = c0 * y6;                                                        \
                ST_DP(out7, y); y += inc_y2;                                           \
                out7 = c0 * y7;                                                        \
                                                                                       \
                x0 = LD_DP(px); px += inc_x2;                                          \
                out0 += s0 * y4;                                                       \
                x1 = LD_DP(px); px += inc_x2;                                          \
                out2 += s0 * y5;                                                       \
                x2 = LD_DP(px); px += inc_x2;                                          \
                out4 += s0 * y6;                                                       \
                x3 = LD_DP(px); px += inc_x2;                                          \
                out6 += s0 * y7;                                                       \
                y0 = LD_DP(py); py += inc_y2;                                          \
                out1 -= s0 * x4;                                                       \
                y1 = LD_DP(py); py += inc_y2;                                          \
                out3 -= s0 * x5;                                                       \
                y2 = LD_DP(py); py += inc_y2;                                          \
                out5 -= s0 * x6;                                                       \
                y3 = LD_DP(py); py += inc_y2;                                          \
                out7 -= s0 * x7;                                                       \
                                                                                       \
                ST_DP4_INC(out0, out2, out4, out6, x, inc_x2);                         \
                ST_DP4_INC(out1, out3, out5, out7, y, inc_y2);                         \
            }                                                                          \
                                                                                       \
            out0 = c0 * x0;                                                            \
            out0 += s0 * y0;                                                           \
            out1 = c0 * y0;                                                            \
            out1 -= s0 * x0;                                                           \
            out2 = c0 * x1;                                                            \
            out2 += s0 * y1;                                                           \
            out3 = c0 * y1;                                                            \
            out3 -= s0 * x1;                                                           \
            out4 = c0 * x2;                                                            \
            out4 += s0 * y2;                                                           \
            out5 = c0 * y2;                                                            \
            out5 -= s0 * x2;                                                           \
            out6 = c0 * x3;                                                            \
            out6 += s0 * y3;                                                           \
            out7 = c0 * y3;                                                            \
            out7 -= s0 * x3;                                                           \
                                                                                       \
            ST_DP4_INC(out0, out2, out4, out6, x, inc_x2);                             \
            ST_DP4_INC(out1, out3, out5, out7, y, inc_y2);                             \
                                                                                       \
            LD_DP4_INC(px, inc_x2, x4, x5, x6, x7);                                    \
            LD_DP4_INC(py, inc_y2, y4, y5, y6, y7);                                    \
                                                                                       \
            out8 = c0 * x4;                                                            \
            out8 += s0 * y4;                                                           \
            out9 = c0 * y4;                                                            \
            out9 -= s0 * x4;                                                           \
            out10 = c0 * x5;                                                           \
            out10 += s0 * y5;                                                          \
            out11 = c0 * y5;                                                           \
            out11 -= s0 * x5;                                                          \
            out12 = c0 * x6;                                                           \
            out12 += s0 * y6;                                                          \
            out13 = c0 * y6;                                                           \
            out13 -= s0 * x6;                                                          \
            out14 = c0 * x7;                                                           \
            out14 += s0 * y7;                                                          \
            out15 = c0 * y7;                                                           \
            out15 -= s0 * x7;                                                          \
                                                                                       \
            ST_DP4_INC(out8, out10, out12, out14, x, inc_x2);                          \
            ST_DP4_INC(out9, out11, out13, out15, y, inc_y2);                          \
        }                                                                              \
        if (n & 4)                                                                     \
        {                                                                              \
            LD_DP4_INC(px, inc_x2, x0, x1, x2, x3);                                    \
            LD_DP4_INC(py, inc_y2, y0, y1, y2, y3);                                    \
                                                                                       \
            out0 = (c0 * x0) + (s0 * y0);                                              \
            out1 = (c0 * y0) - (s0 * x0);                                              \
            out2 = (c0 * x1) + (s0 * y1);                                              \
            out3 = (c0 * y1) - (s0 * x1);                                              \
            out4 = (c0 * x2) + (s0 * y2);                                              \
            out5 = (c0 * y2) - (s0 * x2);                                              \
            out6 = (c0 * x3) + (s0 * y3);                                              \
            out7 = (c0 * y3) - (s0 * x3);                                              \
                                                                                       \
            ST_DP4_INC(out0, out2, out4, out6, x, inc_x2);                             \
            ST_DP4_INC(out1, out3, out5, out7, y, inc_y2);                             \
        }                                                                              \
        if (n & 2)                                                                     \
        {                                                                              \
            LD_DP2_INC(px, inc_x2, x0, x1);                                            \
            LD_DP2_INC(py, inc_y2, y0, y1);                                            \
                                                                                       \
            out0 = (c0 * x0) + (s0 * y0);                                              \
            out1 = (c0 * y0) - (s0 * x0);                                              \
            out2 = (c0 * x1) + (s0 * y1);                                              \
            out3 = (c0 * y1) - (s0 * x1);                                              \
                                                                                       \
            ST_DP2_INC(out0, out2, x, inc_x2);                                         \
            ST_DP2_INC(out1, out3, y, inc_y2);                                         \
        }                                                                              \
        if (n & 1)                                                                     \
        {                                                                              \
            x0 = LD_DP(px);                                                            \
            y0 = LD_DP(py);                                                            \
                                                                                       \
            out0 = (c0 * x0) + (s0 * y0);                                              \
            out1 = (c0 * y0) - (s0 * x0);                                              \
                                                                                       \
            ST_DP(out0, px);                                                           \
            ST_DP(out1, py);                                                           \
        }                                                                              \
    }

int CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y,
          FLOAT c, FLOAT s)
{
    BLASLONG j;
    FLOAT *px, *py;
    v2f64 x0, x1, x2, x3, x4, x5, x6, x7, y0, y1, y2, y3, y4, y5, y6, y7;
    v2f64 out0, out1, out2, out3, out4, out5, out6, out7, c0, s0;
    v2f64 out8, out9, out10, out11, out12, out13, out14, out15;

    px = x;
    py = y;

    if ((1 == inc_x) && (1 == inc_y))
    {
        PROCESS_ZROT(2, 2);
    }
    else
    {
        inc_x *= 2;
        inc_y *= 2;

        PROCESS_ZROT(inc_x, inc_y);
    }

    return 0;
}
