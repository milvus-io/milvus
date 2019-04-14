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

#if !defined(CONJ)
    #define OP0  +=
    #define OP1  -=
    #define OP2  +=
#else
    #define OP0  -=
    #define OP1  +=
    #define OP2  -=
#endif

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da_r, FLOAT da_i,
          FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy,
          BLASLONG dummy2)
{
    BLASLONG i, inc_x2, inc_y2;
    FLOAT *py;
    v4f32 x0, x1, x2, x3, x4, x5, x6, x7;
    v4f32 y0, y1, y2, y3, y4, y5, y6, y7, dar_vec, dai_vec;
    v4f32 x0r, x1r, x2r, x3r, x0i, x1i, x2i, x3i;
    v4f32 y0r, y1r, y2r, y3r, y0i, y1i, y2i, y3i;
    FLOAT xd0, xd1, xd2, xd3, xd4, xd5, xd6, xd7;
    FLOAT yd0, yd1, yd2, yd3, yd4, yd5, yd6, yd7;

    if (n < 0)  return(0);
    if ((da_r == 0.0) && (da_i == 0.0)) return(0);

    py = y;

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

        dar_vec = COPY_FLOAT_TO_VECTOR(da_r);
        dai_vec = COPY_FLOAT_TO_VECTOR(da_i);

        for (i = (n >> 4); i--;)
        {
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

            LD_SP8_INC(x, 4, x0, x1, x2, x3, x4, x5, x6, x7);
            LD_SP8_INC(py, 4, y0, y1, y2, y3, y4, y5, y6, y7);
            PCKEVOD_W2_SP(x1, x0, x0r, x0i);
            PCKEVOD_W2_SP(y1, y0, y0r, y0i);
            PCKEVOD_W2_SP(x3, x2, x1r, x1i);
            PCKEVOD_W2_SP(y3, y2, y1r, y1i);
            PCKEVOD_W2_SP(x5, x4, x2r, x2i);
            PCKEVOD_W2_SP(y5, y4, y2r, y2i);
            PCKEVOD_W2_SP(x7, x6, x3r, x3i);
            PCKEVOD_W2_SP(y7, y6, y3r, y3i);

            FMADD4(x0r, x1r, x2r, x3r, dar_vec, y0r, y1r, y2r, y3r);
            y0i OP0 dar_vec * x0i;
            y1i OP0 dar_vec * x1i;
            y2i OP0 dar_vec * x2i;
            y3i OP0 dar_vec * x3i;
            y0r OP1 dai_vec * x0i;
            y1r OP1 dai_vec * x1i;
            y2r OP1 dai_vec * x2i;
            y3r OP1 dai_vec * x3i;
            y0i OP2 dai_vec * x0r;
            y1i OP2 dai_vec * x1r;
            y2i OP2 dai_vec * x2r;
            y3i OP2 dai_vec * x3r;

            ILVRL_W2_SP(y0i, y0r, y0, y1);
            ILVRL_W2_SP(y1i, y1r, y2, y3);
            ILVRL_W2_SP(y2i, y2r, y4, y5);
            ILVRL_W2_SP(y3i, y3r, y6, y7);
            ST_SP8_INC(y0, y1, y2, y3, y4, y5, y6, y7, y, 4);
        }

        if (n & 15)
        {
            if (n & 8)
            {
                LD_SP4_INC(x, 4, x0, x1, x2, x3);
                LD_SP4_INC(py, 4, y0, y1, y2, y3);
                PCKEVOD_W2_SP(x1, x0, x0r, x0i);
                PCKEVOD_W2_SP(y1, y0, y0r, y0i);
                PCKEVOD_W2_SP(x3, x2, x1r, x1i);
                PCKEVOD_W2_SP(y3, y2, y1r, y1i);

                FMADD2(x0r, x1r, dar_vec, y0r, y1r);
                y0i OP0 dar_vec * x0i;
                y1i OP0 dar_vec * x1i;
                y0r OP1 dai_vec * x0i;
                y1r OP1 dai_vec * x1i;
                y0i OP2 dai_vec * x0r;
                y1i OP2 dai_vec * x1r;

                ILVRL_W2_SP(y0i, y0r, y0, y1);
                ILVRL_W2_SP(y1i, y1r, y2, y3);
                ST_SP4_INC(y0, y1, y2, y3, y, 4);
            }

            if (n & 4)
            {
                LD_SP2_INC(x, 4, x0, x1);
                LD_SP2_INC(py, 4, y0, y1);
                PCKEVOD_W2_SP(x1, x0, x0r, x0i);
                PCKEVOD_W2_SP(y1, y0, y0r, y0i);

                y0r += dar_vec * x0r;
                y0i OP0 dar_vec * x0i;
                y0r OP1 dai_vec * x0i;
                y0i OP2 dai_vec * x0r;

                ILVRL_W2_SP(y0i, y0r, y0, y1);
                ST_SP2_INC(y0, y1, y, 4);
            }

            if (n & 2)
            {
                LD_GP4_INC(x, 1, xd0, xd1, xd2, xd3);
                LD_GP4_INC(py, 1, yd0, yd1, yd2, yd3);

                FMADD2(xd0, xd2, da_r, yd0, yd2);
                yd1 OP0 da_r * xd1;
                yd3 OP0 da_r * xd3;
                yd0 OP1 da_i * xd1;
                yd2 OP1 da_i * xd3;
                yd1 OP2 da_i * xd0;
                yd3 OP2 da_i * xd2;

                ST_GP4_INC(yd0, yd1, yd2, yd3, y, 1);
            }

            if (n & 1)
            {
                LD_GP2_INC(x, 1, xd0, xd1);
                LD_GP2_INC(py, 1, yd0, yd1);

                yd0 += da_r * xd0;
                yd1 OP0 da_r * xd1;
                yd0 OP1 da_i * xd1;
                yd1 OP2 da_i * xd0;

                ST_GP2_INC(yd0, yd1, y, 1);
            }
        }
    }
    else if (1 == inc_y)
    {
        FLOAT *y_pref;
        BLASLONG pref_offset;
        v4f32 x8, x9, x10, x11, x12, x13, x14;

        pref_offset = (BLASLONG)y & (L1_DATA_LINESIZE - 1);
        if (pref_offset > 0)
        {
            pref_offset = L1_DATA_LINESIZE - pref_offset;
            pref_offset = pref_offset / sizeof(FLOAT);
        }
        y_pref = y + pref_offset + 64;

        inc_x2 = 2 * inc_x;

        dar_vec = COPY_FLOAT_TO_VECTOR(da_r);
        dai_vec = COPY_FLOAT_TO_VECTOR(da_i);

        for (i = (n >> 4); i--;)
        {
            PREF_OFFSET(y_pref, 0);
            PREF_OFFSET(y_pref, 32);
            PREF_OFFSET(y_pref, 64);
            PREF_OFFSET(y_pref, 96);
            y_pref += 32;

            LD_SP8_INC(x, inc_x2, x0, x1, x2, x3, x4, x5, x6, x14);
            LD_SP7_INC(x, inc_x2, x8, x9, x10, x11, x12, x13, x7);
            PCKEV_D2_SP(x1, x0, x3, x2, x0, x1);
            PCKEV_D2_SP(x5, x4, x14, x6, x2, x3);
            PCKEV_D2_SP(x9, x8, x11, x10, x4, x5);
            x6 = (v4f32) __msa_pckev_d((v2i64) x13, (v2i64) x12);
            x7 = (v4f32) __msa_insert_w((v4i32) x7, 2, *((int *) x));
            x7 = (v4f32) __msa_insert_w((v4i32) x7, 3, *((int *) (x + 1)));
            x += inc_x2;

            LD_SP8_INC(py, 4, y0, y1, y2, y3, y4, y5, y6, y7);
            PCKEVOD_W2_SP(x1, x0, x0r, x0i);
            PCKEVOD_W2_SP(y1, y0, y0r, y0i);
            PCKEVOD_W2_SP(x3, x2, x1r, x1i);
            PCKEVOD_W2_SP(y3, y2, y1r, y1i);
            PCKEVOD_W2_SP(x5, x4, x2r, x2i);
            PCKEVOD_W2_SP(y5, y4, y2r, y2i);
            PCKEVOD_W2_SP(x7, x6, x3r, x3i);
            PCKEVOD_W2_SP(y7, y6, y3r, y3i);

            FMADD4(x0r, x1r, x2r, x3r, dar_vec, y0r, y1r, y2r, y3r);
            y0i OP0 dar_vec * x0i;
            y1i OP0 dar_vec * x1i;
            y2i OP0 dar_vec * x2i;
            y3i OP0 dar_vec * x3i;
            y0r OP1 dai_vec * x0i;
            y1r OP1 dai_vec * x1i;
            y2r OP1 dai_vec * x2i;
            y3r OP1 dai_vec * x3i;
            y0i OP2 dai_vec * x0r;
            y1i OP2 dai_vec * x1r;
            y2i OP2 dai_vec * x2r;
            y3i OP2 dai_vec * x3r;

            ILVRL_W2_SP(y0i, y0r, y0, y1);
            ILVRL_W2_SP(y1i, y1r, y2, y3);
            ILVRL_W2_SP(y2i, y2r, y4, y5);
            ILVRL_W2_SP(y3i, y3r, y6, y7);

            ST_SP8_INC(y0, y1, y2, y3, y4, y5, y6, y7, y, 4);
        }

        if (n & 15)
        {
            if (n & 8)
            {
                LD_SP7_INC(x, inc_x2, x0, x1, x2, x6, x4, x5, x3);
                PCKEV_D2_SP(x1, x0, x6, x2, x0, x1);

                x2 = (v4f32) __msa_pckev_d((v2i64) x5, (v2i64) x4);
                x3 = (v4f32) __msa_insert_w((v4i32) x3, 2, *((int *) x));
                x3 = (v4f32) __msa_insert_w((v4i32) x3, 3, *((int *) (x + 1)));
                x += inc_x2;

                LD_SP4_INC(py, 4, y0, y1, y2, y3);
                PCKEVOD_W2_SP(x1, x0, x0r, x0i);
                PCKEVOD_W2_SP(y1, y0, y0r, y0i);
                PCKEVOD_W2_SP(x3, x2, x1r, x1i);
                PCKEVOD_W2_SP(y3, y2, y1r, y1i);

                FMADD2(x0r, x1r, dar_vec, y0r, y1r);
                y0i OP0 dar_vec * x0i;
                y1i OP0 dar_vec * x1i;
                y0r OP1 dai_vec * x0i;
                y1r OP1 dai_vec * x1i;
                y0i OP2 dai_vec * x0r;
                y1i OP2 dai_vec * x1r;

                ILVRL_W2_SP(y0i, y0r, y0, y1);
                ILVRL_W2_SP(y1i, y1r, y2, y3);
                ST_SP4_INC(y0, y1, y2, y3, y, 4);
            }

            if (n & 4)
            {
                LD_SP3_INC(x, inc_x2, x0, x2, x1);

                x0 = (v4f32) __msa_pckev_d((v2i64) x2, (v2i64) x0);
                x1 = (v4f32) __msa_insert_w((v4i32) x1, 2, *((int *) x));
                x1 = (v4f32) __msa_insert_w((v4i32) x1, 3, *((int *) (x + 1)));
                x += inc_x2;

                LD_SP2_INC(py, 4, y0, y1);
                PCKEVOD_W2_SP(x1, x0, x0r, x0i);
                PCKEVOD_W2_SP(y1, y0, y0r, y0i);

                y0r += dar_vec * x0r;
                y0i OP0 dar_vec * x0i;
                y0r OP1 dai_vec * x0i;
                y0i OP2 dai_vec * x0r;

                ILVRL_W2_SP(y0i, y0r, y0, y1);
                ST_SP2_INC(y0, y1, y, 4);
            }

            if (n & 2)
            {
                xd0 = x[0];
                xd1 = x[1];
                x += inc_x2;
                xd2 = x[0];
                xd3 = x[1];
                x += inc_x2;

                LD_GP4_INC(py, 1, yd0, yd1, yd2, yd3);

                FMADD2(xd0, xd2, da_r, yd0, yd2);
                yd1 OP0 da_r * xd1;
                yd3 OP0 da_r * xd3;
                yd0 OP1 da_i * xd1;
                yd2 OP1 da_i * xd3;
                yd1 OP2 da_i * xd0;
                yd3 OP2 da_i * xd2;

                ST_GP4_INC(yd0, yd1, yd2, yd3, y, 1);
            }

            if (n & 1)
            {
                LD_GP2_INC(x, 1, xd0, xd1);
                LD_GP2_INC(py, 1, yd0, yd1);

                yd0 += da_r * xd0;
                yd1 OP0 da_r * xd1;
                yd0 OP1 da_i * xd1;
                yd1 OP2 da_i * xd0;

                ST_GP2_INC(yd0, yd1, y, 1);
            }
        }
    }
    else
    {
        inc_x2 = 2 * inc_x;
        inc_y2 = 2 * inc_y;

        for (i = (n >> 2); i--;)
        {
            xd0 = x[0];
            xd1 = x[1];
            x += inc_x2;
            xd2 = x[0];
            xd3 = x[1];
            x += inc_x2;
            xd4 = x[0];
            xd5 = x[1];
            x += inc_x2;
            xd6 = x[0];
            xd7 = x[1];
            x += inc_x2;

            yd0 = py[0];
            yd1 = py[1];
            py += inc_y2;
            yd2 = py[0];
            yd3 = py[1];
            py += inc_y2;
            yd4 = py[0];
            yd5 = py[1];
            py += inc_y2;
            yd6 = py[0];
            yd7 = py[1];
            py += inc_y2;

            FMADD4(xd0, xd2, xd4, xd6, da_r, yd0, yd2, yd4, yd6);
            yd1 OP0 da_r * xd1;
            yd3 OP0 da_r * xd3;
            yd5 OP0 da_r * xd5;
            yd7 OP0 da_r * xd7;
            yd0 OP1 da_i * xd1;
            yd2 OP1 da_i * xd3;
            yd4 OP1 da_i * xd5;
            yd6 OP1 da_i * xd7;
            yd1 OP2 da_i * xd0;
            yd3 OP2 da_i * xd2;
            yd5 OP2 da_i * xd4;
            yd7 OP2 da_i * xd6;

            y[0] = yd0;
            y[1] = yd1;
            y += inc_y2;
            y[0] = yd2;
            y[1] = yd3;
            y += inc_y2;
            y[0] = yd4;
            y[1] = yd5;
            y += inc_y2;
            y[0] = yd6;
            y[1] = yd7;
            y += inc_y2;
        }

        if (n & 3)
        {
            if (n & 2)
            {
                xd0 = x[0];
                xd1 = x[1];
                x += inc_x2;
                xd2 = x[0];
                xd3 = x[1];
                x += inc_x2;

                yd0 = py[0];
                yd1 = py[1];
                py += inc_y2;
                yd2 = py[0];
                yd3 = py[1];
                py += inc_y2;

                FMADD2(xd0, xd2, da_r, yd0, yd2);
                yd1 OP0 da_r * xd1;
                yd3 OP0 da_r * xd3;
                yd0 OP1 da_i * xd1;
                yd2 OP1 da_i * xd3;
                yd1 OP2 da_i * xd0;
                yd3 OP2 da_i * xd2;

                y[0] = yd0;
                y[1] = yd1;
                y += inc_y2;
                y[0] = yd2;
                y[1] = yd3;
                y += inc_y2;
            }

            if (n & 1)
            {
                xd0 = x[0];
                xd1 = x[1];

                yd0 = y[0];
                yd1 = y[1];

                yd0 += da_r * xd0;
                yd1 OP0 da_r * xd1;
                yd0 OP1 da_i * xd1;
                yd1 OP2 da_i * xd0;

                y[0] = yd0;
                y[1] = yd1;
            }
        }
    }

    return (0);
}
