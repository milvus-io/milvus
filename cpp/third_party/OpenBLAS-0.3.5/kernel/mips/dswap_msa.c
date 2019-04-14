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
          FLOAT *srcx, BLASLONG inc_x, FLOAT *srcy, BLASLONG inc_y,
          FLOAT *dummy, BLASLONG dummy2)
{
    BLASLONG i = 0, pref_offsetx, pref_offsety;
    FLOAT *px, *py;
    FLOAT x0, x1, x2, x3, x4, x5, x6, x7;
    FLOAT y0, y1, y2, y3, y4, y5, y6, y7;
    v2f64 xv0, xv1, xv2, xv3, xv4, xv5, xv6, xv7;
    v2f64 yv0, yv1, yv2, yv3, yv4, yv5, yv6, yv7;

    if (n < 0)  return (0);

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

    px = srcx;
    py = srcy;

    if ((1 == inc_x) && (1 == inc_y))
    {
        if (n >> 4)
        {
            LD_DP8_INC(px, 2, xv0, xv1, xv2, xv3, xv4, xv5, xv6, xv7);

            for (i = (n >> 4) - 1; i--;)
            {
                PREFETCH(px + pref_offsetx + 16);
                PREFETCH(px + pref_offsetx + 20);
                PREFETCH(px + pref_offsetx + 24);
                PREFETCH(px + pref_offsetx + 28);

                PREFETCH(py + pref_offsety + 16);
                PREFETCH(py + pref_offsety + 20);
                PREFETCH(py + pref_offsety + 24);
                PREFETCH(py + pref_offsety + 28);

                yv0 = LD_DP(py); py += 2;
                ST_DP(xv0, srcy); srcy += 2;
                yv1 = LD_DP(py); py += 2;
                ST_DP(xv1, srcy); srcy += 2;
                yv2 = LD_DP(py); py += 2;
                ST_DP(xv2, srcy); srcy += 2;
                yv3 = LD_DP(py); py += 2;
                ST_DP(xv3, srcy); srcy += 2;
                yv4 = LD_DP(py); py += 2;
                ST_DP(xv4, srcy); srcy += 2;
                yv5 = LD_DP(py); py += 2;
                ST_DP(xv5, srcy); srcy += 2;
                yv6 = LD_DP(py); py += 2;
                ST_DP(xv6, srcy); srcy += 2;
                yv7 = LD_DP(py); py += 2;
                ST_DP(xv7, srcy); srcy += 2;

                xv0 = LD_DP(px); px += 2;
                ST_DP(yv0, srcx); srcx += 2;
                xv1 = LD_DP(px); px += 2;
                ST_DP(yv1, srcx); srcx += 2;
                xv2 = LD_DP(px); px += 2;
                ST_DP(yv2, srcx); srcx += 2;
                xv3 = LD_DP(px); px += 2;
                ST_DP(yv3, srcx); srcx += 2;
                xv4 = LD_DP(px); px += 2;
                ST_DP(yv4, srcx); srcx += 2;
                xv5 = LD_DP(px); px += 2;
                ST_DP(yv5, srcx); srcx += 2;
                xv6 = LD_DP(px); px += 2;
                ST_DP(yv6, srcx); srcx += 2;
                xv7 = LD_DP(px); px += 2;
                ST_DP(yv7, srcx); srcx += 2;
            }

            LD_DP8_INC(py, 2, yv0, yv1, yv2, yv3, yv4, yv5, yv6, yv7);
            ST_DP8_INC(xv0, xv1, xv2, xv3, xv4, xv5, xv6, xv7, srcy, 2);
            ST_DP8_INC(yv0, yv1, yv2, yv3, yv4, yv5, yv6, yv7, srcx, 2);
        }

        if (n & 15)
        {
            if ((n & 8) && (n & 4) && (n & 2))
            {
                LD_DP7_INC(px, 2, xv0, xv1, xv2, xv3, xv4, xv5, xv6);
                LD_DP7_INC(py, 2, yv0, yv1, yv2, yv3, yv4, yv5, yv6);
                ST_DP7_INC(xv0, xv1, xv2, xv3, xv4, xv5, xv6, srcy, 2);
                ST_DP7_INC(yv0, yv1, yv2, yv3, yv4, yv5, yv6, srcx, 2);
            }
            else if ((n & 8) && (n & 4))
            {
                LD_DP6_INC(px, 2, xv0, xv1, xv2, xv3, xv4, xv5);
                LD_DP6_INC(py, 2, yv0, yv1, yv2, yv3, yv4, yv5);
                ST_DP6_INC(xv0, xv1, xv2, xv3, xv4, xv5, srcy, 2);
                ST_DP6_INC(yv0, yv1, yv2, yv3, yv4, yv5, srcx, 2);
            }
            else if ((n & 8) && (n & 2))
            {
                LD_DP5_INC(px, 2, xv0, xv1, xv2, xv3, xv4);
                LD_DP5_INC(py, 2, yv0, yv1, yv2, yv3, yv4);
                ST_DP5_INC(xv0, xv1, xv2, xv3, xv4, srcy, 2);
                ST_DP5_INC(yv0, yv1, yv2, yv3, yv4, srcx, 2);
            }
            else if ((n & 4) && (n & 2))
            {
                LD_DP3_INC(px, 2, xv0, xv1, xv2);
                LD_DP3_INC(py, 2, yv0, yv1, yv2);
                ST_DP3_INC(xv0, xv1, xv2, srcy, 2);
                ST_DP3_INC(yv0, yv1, yv2, srcx, 2);
            }
            else if (n & 8)
            {
                LD_DP4_INC(px, 2, xv0, xv1, xv2, xv3);
                LD_DP4_INC(py, 2, yv0, yv1, yv2, yv3);
                ST_DP4_INC(xv0, xv1, xv2, xv3, srcy, 2);
                ST_DP4_INC(yv0, yv1, yv2, yv3, srcx, 2);
            }
            else if (n & 4)
            {
                LD_DP2_INC(px, 2, xv0, xv1);
                LD_DP2_INC(py, 2, yv0, yv1);
                ST_DP2_INC(xv0, xv1, srcy, 2);
                ST_DP2_INC(yv0, yv1, srcx, 2);
            }
            else if (n & 2)
            {
                xv0 = LD_DP(px);
                yv0 = LD_DP(py);

                px += 2;
                py += 2;

                ST_DP(xv0, srcy);
                ST_DP(yv0, srcx);

                srcx += 2;
                srcy += 2;
            }

            if (n & 1)
            {
                x0 = px[0];
                y0 = py[0];
                srcx[0] = y0;
                srcy[0] = x0;
            }
        }
    }
    else
    {
        for (i = (n >> 3); i--;)
        {
            LD_GP8_INC(px, inc_x, x0, x1, x2, x3, x4, x5, x6, x7);
            LD_GP8_INC(py, inc_y, y0, y1, y2, y3, y4, y5, y6, y7);
            ST_GP8_INC(x0, x1, x2, x3, x4, x5, x6, x7, srcy, inc_y);
            ST_GP8_INC(y0, y1, y2, y3, y4, y5, y6, y7, srcx, inc_x);
        }

        if (n & 7)
        {
            if ((n & 4) && (n & 2) && (n & 1))
            {
                LD_GP7_INC(px, inc_x, x0, x1, x2, x3, x4, x5, x6);
                LD_GP7_INC(py, inc_y, y0, y1, y2, y3, y4, y5, y6);
                ST_GP7_INC(x0, x1, x2, x3, x4, x5, x6, srcy, inc_y);
                ST_GP7_INC(y0, y1, y2, y3, y4, y5, y6, srcx, inc_x);
            }
            else if ((n & 4) && (n & 2))
            {
                LD_GP6_INC(px, inc_x, x0, x1, x2, x3, x4, x5);
                LD_GP6_INC(py, inc_y, y0, y1, y2, y3, y4, y5);
                ST_GP6_INC(x0, x1, x2, x3, x4, x5, srcy, inc_y);
                ST_GP6_INC(y0, y1, y2, y3, y4, y5, srcx, inc_x);
            }
            else if ((n & 4) && (n & 1))
            {
                LD_GP5_INC(px, inc_x, x0, x1, x2, x3, x4);
                LD_GP5_INC(py, inc_y, y0, y1, y2, y3, y4);
                ST_GP5_INC(x0, x1, x2, x3, x4, srcy, inc_y);
                ST_GP5_INC(y0, y1, y2, y3, y4, srcx, inc_x);
            }
            else if ((n & 2) && (n & 1))
            {
                LD_GP3_INC(px, inc_x, x0, x1, x2);
                LD_GP3_INC(py, inc_y, y0, y1, y2);
                ST_GP3_INC(x0, x1, x2, srcy, inc_y);
                ST_GP3_INC(y0, y1, y2, srcx, inc_x);
            }
            else if (n & 4)
            {
                LD_GP4_INC(px, inc_x, x0, x1, x2, x3);
                LD_GP4_INC(py, inc_y, y0, y1, y2, y3);
                ST_GP4_INC(x0, x1, x2, x3, srcy, inc_y);
                ST_GP4_INC(y0, y1, y2, y3, srcx, inc_x);
            }
            else if (n & 2)
            {
                LD_GP2_INC(px, inc_x, x0, x1);
                LD_GP2_INC(py, inc_y, y0, y1);
                ST_GP2_INC(x0, x1, srcy, inc_y);
                ST_GP2_INC(y0, y1, srcx, inc_x);
            }
            else if (n & 1)
            {
                x0 = *srcx;
                y0 = *srcy;

                *srcx = y0;
                *srcy = x0;
            }
        }
    }

    return (0);
}
