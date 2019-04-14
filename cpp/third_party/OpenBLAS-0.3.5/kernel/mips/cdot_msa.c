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
    #define OP1     -=
    #define OP2     +=
    #define OP3     -
    #define OP4     +
#else
    #define OP1     +=
    #define OP2     -=
    #define OP3     +
    #define OP4     -
#endif

OPENBLAS_COMPLEX_FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
{
    BLASLONG i = 0;
    FLOAT dot[2];
    BLASLONG inc_x2, inc_y2;
    FLOAT x0, x1, x2, x3, x4, x5, x6, x7, y0, y1, y2, y3, y4, y5, y6, y7;
    v4f32 vx0, vx1, vx2, vx3, vx4, vx5, vx6, vx7, vx8, vx9, vx10, vx11;
    v4f32 vy0, vy1, vy2, vy3, vy4, vy5, vy6, vy7, vy8, vy9, vy10, vy11;
    v4f32 vx0r, vx0i, vx1r, vx1i, vx2r, vx2i, vx3r, vx3i;
    v4f32 vy0r, vy0i, vy1r, vy1i, vy2r, vy2i, vy3r, vy3i;
    v4f32 dot0 = {0, 0, 0, 0};
    v4f32 dot1 = {0, 0, 0, 0};
    v4f32 dot2 = {0, 0, 0, 0};
    v4f32 dot3 = {0, 0, 0, 0};
    v4f32 dot4 = {0, 0, 0, 0};
    v4f32 dot5 = {0, 0, 0, 0};
    v4f32 dot6 = {0, 0, 0, 0};
    v4f32 dot7 = {0, 0, 0, 0};
    OPENBLAS_COMPLEX_FLOAT result;

    dot[0] = 0.0;
    dot[1] = 0.0;

    CREAL(result) = 0.0;
    CIMAG(result) = 0.0;

    if (n < 1) return (result);

    if ((1 == inc_x) && (1 == inc_y))
    {
        if (n > 15)
        {
            FLOAT *x_pref, *y_pref;
            BLASLONG pref_offset;

            pref_offset = (BLASLONG)x & (L1_DATA_LINESIZE - 1);
            if (pref_offset > 0)
            {
                pref_offset = L1_DATA_LINESIZE - pref_offset;
                pref_offset = pref_offset / sizeof(FLOAT);
            }
            x_pref = x + pref_offset + 64 + 16;

            pref_offset = (BLASLONG)y & (L1_DATA_LINESIZE - 1);
            if (pref_offset > 0)
            {
                pref_offset = L1_DATA_LINESIZE - pref_offset;
                pref_offset = pref_offset / sizeof(FLOAT);
            }
            y_pref = y + pref_offset + 64 + 16;

            LD_SP4_INC(x, 4, vx0, vx1, vx2, vx3);
            LD_SP4_INC(y, 4, vy0, vy1, vy2, vy3);

            PCKEVOD_W2_SP(vx1, vx0, vx0r, vx0i);
            PCKEVOD_W2_SP(vy1, vy0, vy0r, vy0i);

            for (i = (n >> 4) - 1; i--;)
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

                vx4 = LD_SP(x); x += 4;
                vx1r = (v4f32) __msa_pckev_w((v4i32) vx3, (v4i32) vx2);
                dot0 += (vx0r * vy0r);
                vx5 = LD_SP(x); x += 4;
                vx1i = (v4f32) __msa_pckod_w((v4i32) vx3, (v4i32) vx2);
                dot1 OP2 (vx0i * vy0r);
                vy4 = LD_SP(y); y += 4;
                vy1r = (v4f32) __msa_pckev_w((v4i32) vy3, (v4i32) vy2);
                dot2 += (vx1r * vy1r);
                vy5 = LD_SP(y); y += 4;
                vy1i = (v4f32) __msa_pckod_w((v4i32) vy3, (v4i32) vy2);
                dot3 OP2 (vx1i * vy1r);
                vx6 = LD_SP(x); x += 4;
                vx7 = LD_SP(x); x += 4;
                vy6 = LD_SP(y); y += 4;
                vy7 = LD_SP(y); y += 4;
                vx8 = LD_SP(x); x += 4;
                dot0 OP1 (vx0i * vy0i);
                vx9 = LD_SP(x); x += 4;
                vx2r = (v4f32) __msa_pckev_w((v4i32) vx5, (v4i32) vx4);
                dot1 += (vx0r * vy0i);
                vy8 = LD_SP(y); y += 4;
                vx2i = (v4f32) __msa_pckod_w((v4i32) vx5, (v4i32) vx4);
                dot2 OP1 (vx1i * vy1i);
                vy9 = LD_SP(y); y += 4;
                vy2r = (v4f32) __msa_pckev_w((v4i32) vy5, (v4i32) vy4);
                dot3 += (vx1r * vy1i);
                vx10 = LD_SP(x); x += 4;
                vy2i = (v4f32) __msa_pckod_w((v4i32) vy5, (v4i32) vy4);
                vx11 = LD_SP(x); x += 4;
                vx3r = (v4f32) __msa_pckev_w((v4i32) vx7, (v4i32) vx6);
                dot4 += (vx2r * vy2r);
                vy10 = LD_SP(y); y += 4;
                vx3i = (v4f32) __msa_pckod_w((v4i32) vx7, (v4i32) vx6);
                dot5 OP2 (vx2i * vy2r);
                vy11 = LD_SP(y); y += 4;
                vy3r = (v4f32) __msa_pckev_w((v4i32) vy7, (v4i32) vy6);
                vy3i = (v4f32) __msa_pckod_w((v4i32) vy7, (v4i32) vy6);
                dot6 += (vx3r * vy3r);
                vx0r = (v4f32) __msa_pckev_w((v4i32) vx9, (v4i32) vx8);
                dot7 OP2 (vx3i * vy3r);
                vx0i = (v4f32) __msa_pckod_w((v4i32) vx9, (v4i32) vx8);
                vy0r = (v4f32) __msa_pckev_w((v4i32) vy9, (v4i32) vy8);
                vx2 = vx10;
                vy0i = (v4f32) __msa_pckod_w((v4i32) vy9, (v4i32) vy8);
                vx3 = vx11;
                dot4 OP1 (vx2i * vy2i);
                vy2 = vy10;
                dot5 += (vx2r * vy2i);
                vy3 = vy11;
                dot6 OP1 (vx3i * vy3i);
                dot7 += (vx3r * vy3i);
            }

            vx4 = LD_SP(x); x += 4;
            vx1r = (v4f32) __msa_pckev_w((v4i32) vx3, (v4i32) vx2);
            dot0 += (vx0r * vy0r);
            vx5 = LD_SP(x); x += 4;
            vx1i = (v4f32) __msa_pckod_w((v4i32) vx3, (v4i32) vx2);
            dot1 OP2 (vx0i * vy0r);
            vy4 = LD_SP(y); y += 4;
            vy1r = (v4f32) __msa_pckev_w((v4i32) vy3, (v4i32) vy2);
            dot2 += (vx1r * vy1r);
            vy5 = LD_SP(y); y += 4;
            vy1i = (v4f32) __msa_pckod_w((v4i32) vy3, (v4i32) vy2);
            dot3 OP2 (vx1i * vy1r);
            vx6 = LD_SP(x); x += 4;
            vx7 = LD_SP(x); x += 4;
            vy6 = LD_SP(y); y += 4;
            vy7 = LD_SP(y); y += 4;
            dot0 OP1 (vx0i * vy0i);
            vx2r = (v4f32) __msa_pckev_w((v4i32) vx5, (v4i32) vx4);
            dot1 += (vx0r * vy0i);
            vx2i = (v4f32) __msa_pckod_w((v4i32) vx5, (v4i32) vx4);
            dot2 OP1 (vx1i * vy1i);
            vy2r = (v4f32) __msa_pckev_w((v4i32) vy5, (v4i32) vy4);
            dot3 += (vx1r * vy1i);
            vy2i = (v4f32) __msa_pckod_w((v4i32) vy5, (v4i32) vy4);
            vx3r = (v4f32) __msa_pckev_w((v4i32) vx7, (v4i32) vx6);
            dot4 += (vx2r * vy2r);
            vx3i = (v4f32) __msa_pckod_w((v4i32) vx7, (v4i32) vx6);
            dot5 OP2 (vx2i * vy2r);
            vy3r = (v4f32) __msa_pckev_w((v4i32) vy7, (v4i32) vy6);
            vy3i = (v4f32) __msa_pckod_w((v4i32) vy7, (v4i32) vy6);
            dot6 += (vx3r * vy3r);
            dot7 OP2 (vx3i * vy3r);
            dot4 OP1 (vx2i * vy2i);
            dot5 += (vx2r * vy2i);
            dot6 OP1 (vx3i * vy3i);
            dot7 += (vx3r * vy3i);
        }

        if (n & 15)
        {
            if (n & 8)
            {
                LD_SP4_INC(x, 4, vx0, vx1, vx2, vx3);
                LD_SP4_INC(y, 4, vy0, vy1, vy2, vy3);

                PCKEVOD_W2_SP(vx1, vx0, vx0r, vx0i);
                PCKEVOD_W2_SP(vx3, vx2, vx1r, vx1i);

                PCKEVOD_W2_SP(vy1, vy0, vy0r, vy0i);
                PCKEVOD_W2_SP(vy3, vy2, vy1r, vy1i);

                dot0 += (vx0r * vy0r);
                dot0 OP1 (vx0i * vy0i);
                dot1 OP2 (vx0i * vy0r);
                dot1 += (vx0r * vy0i);

                dot2 += (vx1r * vy1r);
                dot2 OP1 (vx1i * vy1i);
                dot3 OP2 (vx1i * vy1r);
                dot3 += (vx1r * vy1i);
            }

            if (n & 4)
            {
                LD_SP2_INC(x, 4, vx0, vx1);
                LD_SP2_INC(y, 4, vy0, vy1);
                PCKEVOD_W2_SP(vx1, vx0, vx0r, vx0i);
                PCKEVOD_W2_SP(vy1, vy0, vy0r, vy0i);

                dot0 += (vx0r * vy0r);
                dot0 OP1 (vx0i * vy0i);
                dot1 OP2 (vx0i * vy0r);
                dot1 += (vx0r * vy0i);
            }

            if (n & 2)
            {
                LD_GP4_INC(x, 1, x0, x1, x2, x3);
                LD_GP4_INC(y, 1, y0, y1, y2, y3);

                dot[0] += (x0 * y0 OP3 x1 * y1);
                dot[1] OP2 (x1 * y0 OP4 x0 * y1);

                dot[0] += (x2 * y2 OP3 x3 * y3);
                dot[1] OP2 (x3 * y2 OP4 x2 * y3);
            }

            if (n & 1)
            {
                LD_GP2_INC(x, 1, x0, x1);
                LD_GP2_INC(y, 1, y0, y1);

                dot[0] += (x0 * y0 OP3 x1 * y1);
                dot[1] OP2 (x1 * y0 OP4 x0 * y1);
            }
        }

        dot0 += dot2 + dot4 + dot6;
        dot1 += dot3 + dot5 + dot7;

        dot[0] += (dot0[0] + dot0[1] + dot0[2] + dot0[3]);
        dot[1] += (dot1[0] + dot1[1] + dot1[2] + dot1[3]);
    }
    else
    {
        inc_x2 = 2 * inc_x;
        inc_y2 = 2 * inc_y;

        for (i = (n >> 2); i--;)
        {
            x0 = *x;
            x1 = *(x + 1);
            x += inc_x2;
            x2 = *x;
            x3 = *(x + 1);
            x += inc_x2;
            x4 = *x;
            x5 = *(x + 1);
            x += inc_x2;
            x6 = *x;
            x7 = *(x + 1);
            x += inc_x2;

            y0 = *y;
            y1 = *(y + 1);
            y += inc_y2;
            y2 = *y;
            y3 = *(y + 1);
            y += inc_y2;
            y4 = *y;
            y5 = *(y + 1);
            y += inc_y2;
            y6 = *y;
            y7 = *(y + 1);
            y += inc_y2;

            dot[0] += (x0 * y0 OP3 x1 * y1);
            dot[1] OP2 (x1 * y0 OP4 x0 * y1);

            dot[0] += (x2 * y2 OP3 x3 * y3);
            dot[1] OP2 (x3 * y2 OP4 x2 * y3);

            dot[0] += (x4 * y4 OP3 x5 * y5);
            dot[1] OP2 (x5 * y4 OP4 x4 * y5);

            dot[0] += (x6 * y6 OP3 x7 * y7);
            dot[1] OP2 (x7 * y6 OP4 x6 * y7);
        }

        if (n & 2)
        {
            x0 = *x;
            x1 = *(x + 1);
            x += inc_x2;
            x2 = *x;
            x3 = *(x + 1);
            x += inc_x2;

            y0 = *y;
            y1 = *(y + 1);
            y += inc_y2;
            y2 = *y;
            y3 = *(y + 1);
            y += inc_y2;

            dot[0] += (x0 * y0 OP3 x1 * y1);
            dot[1] OP2 (x1 * y0 OP4 x0 * y1);

            dot[0] += (x2 * y2 OP3 x3 * y3);
            dot[1] OP2 (x3 * y2 OP4 x2 * y3);
        }

        if (n & 1)
        {
            x0 = *x;
            x1 = *(x + 1);
            x += inc_x2;

            y0 = *y;
            y1 = *(y + 1);
            y += inc_y2;

            dot[0] += (x0 * y0 OP3 x1 * y1);
            dot[1] OP2 (x1 * y0 OP4 x0 * y1);
        }
    }

    CREAL(result) = dot[0];
    CIMAG(result) = dot[1];

    return (result);
}
