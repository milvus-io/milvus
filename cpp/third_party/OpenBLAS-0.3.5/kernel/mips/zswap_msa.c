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

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT dummy3,
          FLOAT dummy4, FLOAT *srcx, BLASLONG inc_x, FLOAT *srcy,
          BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
{
    BLASLONG i, inc_x2, inc_y2, pref_offsetx, pref_offsety;
    FLOAT *px, *py;
    v2f64 x0, x1, x2, x3, x4, x5, x6, x7;
    v2f64 y0, y1, y2, y3, y4, y5, y6, y7;

    if (n < 0) return (0);

    pref_offsetx = (BLASLONG)srcx & (L1_DATA_LINESIZE - 1);
    if (pref_offsetx > 0)
    {
        pref_offsetx = L1_DATA_LINESIZE - pref_offsetx;
        pref_offsetx = pref_offsetx / sizeof(FLOAT);
    }

    pref_offsety = (BLASLONG)srcy & (L1_DATA_LINESIZE - 1);
    if (pref_offsety > 0)
    {
        pref_offsety = L1_DATA_LINESIZE - pref_offsety;
        pref_offsety = pref_offsety / sizeof(FLOAT);
    }

    inc_x2 = 2 * inc_x;
    inc_y2 = 2 * inc_y;

    px = srcx;
    py = srcy;

    if ((1 == inc_x) && (1 == inc_y))
    {
        if (n >> 3)
        {
            LD_DP8_INC(px, 2, x0, x1, x2, x3, x4, x5, x6, x7);

            for (i = (n >> 3) - 1; i--;)
            {
                PREFETCH(px + pref_offsetx + 16);
                PREFETCH(px + pref_offsetx + 20);
                PREFETCH(px + pref_offsetx + 24);
                PREFETCH(px + pref_offsetx + 28);

                PREFETCH(py + pref_offsety + 16);
                PREFETCH(py + pref_offsety + 20);
                PREFETCH(py + pref_offsety + 24);
                PREFETCH(py + pref_offsety + 28);

                y0 = LD_DP(py); py += 2;
                ST_DP(x0, srcy); srcy += 2;
                y1 = LD_DP(py); py += 2;
                ST_DP(x1, srcy); srcy += 2;
                y2 = LD_DP(py); py += 2;
                ST_DP(x2, srcy); srcy += 2;
                y3 = LD_DP(py); py += 2;
                ST_DP(x3, srcy); srcy += 2;
                y4 = LD_DP(py); py += 2;
                ST_DP(x4, srcy); srcy += 2;
                y5 = LD_DP(py); py += 2;
                ST_DP(x5, srcy); srcy += 2;
                y6 = LD_DP(py); py += 2;
                ST_DP(x6, srcy); srcy += 2;
                y7 = LD_DP(py); py += 2;
                ST_DP(x7, srcy); srcy += 2;

                x0 = LD_DP(px); px += 2;
                ST_DP(y0, srcx); srcx += 2;
                x1 = LD_DP(px); px += 2;
                ST_DP(y1, srcx); srcx += 2;
                x2 = LD_DP(px); px += 2;
                ST_DP(y2, srcx); srcx += 2;
                x3 = LD_DP(px); px += 2;
                ST_DP(y3, srcx); srcx += 2;
                x4 = LD_DP(px); px += 2;
                ST_DP(y4, srcx); srcx += 2;
                x5 = LD_DP(px); px += 2;
                ST_DP(y5, srcx); srcx += 2;
                x6 = LD_DP(px); px += 2;
                ST_DP(y6, srcx); srcx += 2;
                x7 = LD_DP(px); px += 2;
                ST_DP(y7, srcx); srcx += 2;
            }

            LD_DP8_INC(py, 2, y0, y1, y2, y3, y4, y5, y6, y7);
            ST_DP8_INC(x0, x1, x2, x3, x4, x5, x6, x7, srcy, 2);
            ST_DP8_INC(y0, y1, y2, y3, y4, y5, y6, y7, srcx, 2);
        }

        if (n & 7)
        {
            if ((n & 4) && (n & 2) && (n & 1))
            {
                LD_DP7_INC(px, 2, x0, x1, x2, x3, x4, x5, x6);
                LD_DP7_INC(py, 2, y0, y1, y2, y3, y4, y5, y6);
                ST_DP7_INC(x0, x1, x2, x3, x4, x5, x6, srcy, 2);
                ST_DP7_INC(y0, y1, y2, y3, y4, y5, y6, srcx, 2);
            }
            else if ((n & 4) && (n & 2))
            {
                LD_DP6_INC(px, 2, x0, x1, x2, x3, x4, x5);
                LD_DP6_INC(py, 2, y0, y1, y2, y3, y4, y5);
                ST_DP6_INC(x0, x1, x2, x3, x4, x5, srcy, 2);
                ST_DP6_INC(y0, y1, y2, y3, y4, y5, srcx, 2);
            }
            else if ((n & 4) && (n & 1))
            {
                LD_DP5_INC(px, 2, x0, x1, x2, x3, x4);
                LD_DP5_INC(py, 2, y0, y1, y2, y3, y4);
                ST_DP5_INC(x0, x1, x2, x3, x4, srcy, 2);
                ST_DP5_INC(y0, y1, y2, y3, y4, srcx, 2);
            }
            else if ((n & 2) && (n & 1))
            {
                LD_DP3_INC(px, 2, x0, x1, x2);
                LD_DP3_INC(py, 2, y0, y1, y2);
                ST_DP3_INC(x0, x1, x2, srcy, 2);
                ST_DP3_INC(y0, y1, y2, srcx, 2);
            }
            else if (n & 4)
            {
                LD_DP4_INC(px, 2, x0, x1, x2, x3);
                LD_DP4_INC(py, 2, y0, y1, y2, y3);
                ST_DP4_INC(x0, x1, x2, x3, srcy, 2);
                ST_DP4_INC(y0, y1, y2, y3, srcx, 2);
            }
            else if (n & 2)
            {
                LD_DP2_INC(px, 2, x0, x1);
                LD_DP2_INC(py, 2, y0, y1);
                ST_DP2_INC(x0, x1, srcy, 2);
                ST_DP2_INC(y0, y1, srcx, 2);
            }
            else if (n & 1)
            {
                x0 = LD_DP(px);
                y0 = LD_DP(py);
                ST_DP(y0, srcx);
                ST_DP(x0, srcy);
            }
        }
    }
    else
    {
        for (i = (n >> 3); i--;)
        {
            LD_DP8_INC(px, inc_x2, x0, x1, x2, x3, x4, x5, x6, x7);
            LD_DP8_INC(py, inc_y2, y0, y1, y2, y3, y4, y5, y6, y7);
            ST_DP8_INC(x0, x1, x2, x3, x4, x5, x6, x7, srcy, inc_y2);
            ST_DP8_INC(y0, y1, y2, y3, y4, y5, y6, y7, srcx, inc_x2);
        }

        if (n & 7)
        {
            if ((n & 4) && (n & 2) && (n & 1))
            {
                LD_DP7_INC(px, inc_x2, x0, x1, x2, x3, x4, x5, x6);
                LD_DP7_INC(py, inc_y2, y0, y1, y2, y3, y4, y5, y6);
                ST_DP7_INC(x0, x1, x2, x3, x4, x5, x6, srcy, inc_y2);
                ST_DP7_INC(y0, y1, y2, y3, y4, y5, y6, srcx, inc_x2);
            }
            else if ((n & 4) && (n & 2))
            {
                LD_DP6_INC(px, inc_x2, x0, x1, x2, x3, x4, x5);
                LD_DP6_INC(py, inc_y2, y0, y1, y2, y3, y4, y5);
                ST_DP6_INC(x0, x1, x2, x3, x4, x5, srcy, inc_y2);
                ST_DP6_INC(y0, y1, y2, y3, y4, y5, srcx, inc_x2);
            }
            else if ((n & 4) && (n & 1))
            {
                LD_DP5_INC(px, inc_x2, x0, x1, x2, x3, x4);
                LD_DP5_INC(py, inc_y2, y0, y1, y2, y3, y4);
                ST_DP5_INC(x0, x1, x2, x3, x4, srcy, inc_y2);
                ST_DP5_INC(y0, y1, y2, y3, y4, srcx, inc_x2);
            }
            else if ((n & 2) && (n & 1))
            {
                LD_DP3_INC(px, inc_x2, x0, x1, x2);
                LD_DP3_INC(py, inc_y2, y0, y1, y2);
                ST_DP3_INC(x0, x1, x2, srcy, inc_y2);
                ST_DP3_INC(y0, y1, y2, srcx, inc_x2);
            }
            else if (n & 4)
            {
                LD_DP4_INC(px, inc_x2, x0, x1, x2, x3);
                LD_DP4_INC(py, inc_y2, y0, y1, y2, y3);
                ST_DP4_INC(x0, x1, x2, x3, srcy, inc_y2);
                ST_DP4_INC(y0, y1, y2, y3, srcx, inc_x2);
            }
            else if (n & 2)
            {
                LD_DP2_INC(px, inc_x2, x0, x1);
                LD_DP2_INC(py, inc_y2, y0, y1);
                ST_DP2_INC(x0, x1, srcy, inc_y2);
                ST_DP2_INC(y0, y1, srcx, inc_x2);
            }
            else if (n & 1)
            {
                x0 = LD_DP(px);
                y0 = LD_DP(py);
                ST_DP(y0, srcx);
                ST_DP(x0, srcy);
            }
        }
    }

    return (0);
}
