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
    v2f64 vx0, vx1, vx2, vx3, vx4, vx5, vx6, vx7, vx8, vx9, vx10, vx11;
    v2f64 vy0, vy1, vy2, vy3, vy4, vy5, vy6, vy7, vy8, vy9, vy10, vy11;
    v2f64 vx0r, vx0i, vx1r, vx1i, vx2r, vx2i, vx3r, vx3i;
    v2f64 vy0r, vy0i, vy1r, vy1i, vy2r, vy2i, vy3r, vy3i;
    v2f64 dot0 = {0, 0};
    v2f64 dot1 = {0, 0};
    v2f64 dot2 = {0, 0};
    v2f64 dot3 = {0, 0};
    v2f64 dot4 = {0, 0};
    v2f64 dot5 = {0, 0};
    v2f64 dot6 = {0, 0};
    v2f64 dot7 = {0, 0};
    v2f64 zero = {0, 0};
    OPENBLAS_COMPLEX_FLOAT result;

    dot[0] = 0.0;
    dot[1] = 0.0;

    CREAL(result) = 0.0;
    CIMAG(result) = 0.0;

    if (n < 1) return (result);

    inc_x2 = 2 * inc_x;
    inc_y2 = 2 * inc_y;

    if ((1 == inc_x) && (1 == inc_y))
    {
        if (n > 7)
        {
            FLOAT *x_pref, *y_pref;
            BLASLONG pref_offset;

            pref_offset = (BLASLONG)x & (L1_DATA_LINESIZE - 1);
            if (pref_offset > 0)
            {
                pref_offset = L1_DATA_LINESIZE - pref_offset;
                pref_offset = pref_offset / sizeof(FLOAT);
            }
            x_pref = x + pref_offset + 32 + 8;

            pref_offset = (BLASLONG)y & (L1_DATA_LINESIZE - 1);
            if (pref_offset > 0)
            {
                pref_offset = L1_DATA_LINESIZE - pref_offset;
                pref_offset = pref_offset / sizeof(FLOAT);
            }
            y_pref = y + pref_offset + 32 + 8;

            LD_DP4_INC(x, 2, vx0, vx1, vx2, vx3);
            LD_DP4_INC(y, 2, vy0, vy1, vy2, vy3);

            PCKEVOD_D2_DP(vx1, vx0, vx0r, vx0i);
            PCKEVOD_D2_DP(vy1, vy0, vy0r, vy0i);

            for (i = (n >> 3) - 1; i--;)
            {
                PREF_OFFSET(x_pref, 0);
                PREF_OFFSET(x_pref, 32);
                PREF_OFFSET(x_pref, 64);
                PREF_OFFSET(x_pref, 96);
                PREF_OFFSET(y_pref, 0);
                PREF_OFFSET(y_pref, 32);
                PREF_OFFSET(y_pref, 64);
                PREF_OFFSET(y_pref, 96);
                x_pref += 16;
                y_pref += 16;

                vx4 = LD_DP(x); x += 2;
                vx1r = (v2f64) __msa_pckev_d((v2i64) vx3, (v2i64) vx2);
                dot0 += (vx0r * vy0r);
                vx5 = LD_DP(x); x += 2;
                vx1i = (v2f64) __msa_pckod_d((v2i64) vx3, (v2i64) vx2);
                dot1 OP2 (vx0i * vy0r);
                vy4 = LD_DP(y); y += 2;
                vy1r = (v2f64) __msa_pckev_d((v2i64) vy3, (v2i64) vy2);
                dot2 += (vx1r * vy1r);
                vy5 = LD_DP(y); y += 2;
                vy1i = (v2f64) __msa_pckod_d((v2i64) vy3, (v2i64) vy2);
                dot3 OP2 (vx1i * vy1r);
                vx6 = LD_DP(x); x += 2;
                vx7 = LD_DP(x); x += 2;
                vy6 = LD_DP(y); y += 2;
                vy7 = LD_DP(y); y += 2;
                vx8 = LD_DP(x); x += 2;
                dot0 OP1 (vx0i * vy0i);
                vx9 = LD_DP(x); x += 2;
                vx2r = (v2f64) __msa_pckev_d((v2i64) vx5, (v2i64) vx4);
                dot1 += (vx0r * vy0i);
                vy8 = LD_DP(y); y += 2;
                vx2i = (v2f64) __msa_pckod_d((v2i64) vx5, (v2i64) vx4);
                dot2 OP1 (vx1i * vy1i);
                vy9 = LD_DP(y); y += 2;
                vy2r = (v2f64) __msa_pckev_d((v2i64) vy5, (v2i64) vy4);
                dot3 += (vx1r * vy1i);
                vx10 = LD_DP(x); x += 2;
                vy2i = (v2f64) __msa_pckod_d((v2i64) vy5, (v2i64) vy4);
                vx11 = LD_DP(x); x += 2;
                vx3r = (v2f64) __msa_pckev_d((v2i64) vx7, (v2i64) vx6);
                dot4 += (vx2r * vy2r);
                vy10 = LD_DP(y); y += 2;
                vx3i = (v2f64) __msa_pckod_d((v2i64) vx7, (v2i64) vx6);
                dot5 OP2 (vx2i * vy2r);
                vy11 = LD_DP(y); y += 2;
                vy3r = (v2f64) __msa_pckev_d((v2i64) vy7, (v2i64) vy6);
                vy3i = (v2f64) __msa_pckod_d((v2i64) vy7, (v2i64) vy6);
                dot6 += (vx3r * vy3r);
                vx0r = (v2f64) __msa_pckev_d((v2i64) vx9, (v2i64) vx8);
                dot7 OP2 (vx3i * vy3r);
                vx0i = (v2f64) __msa_pckod_d((v2i64) vx9, (v2i64) vx8);
                vy0r = (v2f64) __msa_pckev_d((v2i64) vy9, (v2i64) vy8);
                vx2 = vx10;
                vy0i = (v2f64) __msa_pckod_d((v2i64) vy9, (v2i64) vy8);
                vx3 = vx11;
                dot4 OP1 (vx2i * vy2i);
                vy2 = vy10;
                dot5 += (vx2r * vy2i);
                vy3 = vy11;
                dot6 OP1 (vx3i * vy3i);
                dot7 += (vx3r * vy3i);
            }

            vx4 = LD_DP(x); x += 2;
            vx1r = (v2f64) __msa_pckev_d((v2i64) vx3, (v2i64) vx2);
            dot0 += (vx0r * vy0r);
            vx5 = LD_DP(x); x += 2;
            vx1i = (v2f64) __msa_pckod_d((v2i64) vx3, (v2i64) vx2);
            dot1 OP2 (vx0i * vy0r);
            vy4 = LD_DP(y); y += 2;
            vy1r = (v2f64) __msa_pckev_d((v2i64) vy3, (v2i64) vy2);
            dot2 += (vx1r * vy1r);
            vy5 = LD_DP(y); y += 2;
            vy1i = (v2f64) __msa_pckod_d((v2i64) vy3, (v2i64) vy2);
            dot3 OP2 (vx1i * vy1r);
            vx6 = LD_DP(x); x += 2;
            vx7 = LD_DP(x); x += 2;
            vy6 = LD_DP(y); y += 2;
            vy7 = LD_DP(y); y += 2;
            dot0 OP1 (vx0i * vy0i);
            vx2r = (v2f64) __msa_pckev_d((v2i64) vx5, (v2i64) vx4);
            dot1 += (vx0r * vy0i);
            vx2i = (v2f64) __msa_pckod_d((v2i64) vx5, (v2i64) vx4);
            dot2 OP1 (vx1i * vy1i);
            vy2r = (v2f64) __msa_pckev_d((v2i64) vy5, (v2i64) vy4);
            dot3 += (vx1r * vy1i);
            vy2i = (v2f64) __msa_pckod_d((v2i64) vy5, (v2i64) vy4);
            vx3r = (v2f64) __msa_pckev_d((v2i64) vx7, (v2i64) vx6);
            dot4 += (vx2r * vy2r);
            vx3i = (v2f64) __msa_pckod_d((v2i64) vx7, (v2i64) vx6);
            dot5 OP2 (vx2i * vy2r);
            vy3r = (v2f64) __msa_pckev_d((v2i64) vy7, (v2i64) vy6);
            vy3i = (v2f64) __msa_pckod_d((v2i64) vy7, (v2i64) vy6);
            dot6 += (vx3r * vy3r);
            dot7 OP2 (vx3i * vy3r);
            dot4 OP1 (vx2i * vy2i);
            dot5 += (vx2r * vy2i);
            dot6 OP1 (vx3i * vy3i);
            dot7 += (vx3r * vy3i);
        }
    }
    else if (n > 7)
    {
        LD_DP4_INC(x, inc_x2, vx0, vx1, vx2, vx3);
        LD_DP4_INC(y, inc_y2, vy0, vy1, vy2, vy3);

        PCKEVOD_D2_DP(vx1, vx0, vx0r, vx0i);
        PCKEVOD_D2_DP(vy1, vy0, vy0r, vy0i);

        for (i = (n >> 3) - 1; i--;)
        {
            vx4 = LD_DP(x); x += inc_x2;
            vx1r = (v2f64) __msa_pckev_d((v2i64) vx3, (v2i64) vx2);
            dot0 += (vx0r * vy0r);
            vx5 = LD_DP(x); x += inc_x2;
            vx1i = (v2f64) __msa_pckod_d((v2i64) vx3, (v2i64) vx2);
            dot1 OP2 (vx0i * vy0r);
            vy4 = LD_DP(y); y += inc_y2;
            vy1r = (v2f64) __msa_pckev_d((v2i64) vy3, (v2i64) vy2);
            dot2 += (vx1r * vy1r);
            vy5 = LD_DP(y); y += inc_y2;
            vy1i = (v2f64) __msa_pckod_d((v2i64) vy3, (v2i64) vy2);
            dot3 OP2 (vx1i * vy1r);
            vx6 = LD_DP(x); x += inc_x2;
            vx7 = LD_DP(x); x += inc_x2;
            vy6 = LD_DP(y); y += inc_y2;
            vy7 = LD_DP(y); y += inc_y2;
            vx8 = LD_DP(x); x += inc_x2;
            dot0 OP1 (vx0i * vy0i);
            vx9 = LD_DP(x); x += inc_x2;
            vx2r = (v2f64) __msa_pckev_d((v2i64) vx5, (v2i64) vx4);
            dot1 += (vx0r * vy0i);
            vy8 = LD_DP(y); y += inc_y2;
            vx2i = (v2f64) __msa_pckod_d((v2i64) vx5, (v2i64) vx4);
            dot2 OP1 (vx1i * vy1i);
            vy9 = LD_DP(y); y += inc_y2;
            vy2r = (v2f64) __msa_pckev_d((v2i64) vy5, (v2i64) vy4);
            dot3 += (vx1r * vy1i);
            vx10 = LD_DP(x); x += inc_x2;
            vy2i = (v2f64) __msa_pckod_d((v2i64) vy5, (v2i64) vy4);
            vx11 = LD_DP(x); x += inc_x2;
            vx3r = (v2f64) __msa_pckev_d((v2i64) vx7, (v2i64) vx6);
            dot4 += (vx2r * vy2r);
            vy10 = LD_DP(y); y += inc_y2;
            vx3i = (v2f64) __msa_pckod_d((v2i64) vx7, (v2i64) vx6);
            dot5 OP2 (vx2i * vy2r);
            vy11 = LD_DP(y); y += inc_y2;
            vy3r = (v2f64) __msa_pckev_d((v2i64) vy7, (v2i64) vy6);
            vy3i = (v2f64) __msa_pckod_d((v2i64) vy7, (v2i64) vy6);
            dot6 += (vx3r * vy3r);
            vx0r = (v2f64) __msa_pckev_d((v2i64) vx9, (v2i64) vx8);
            dot7 OP2 (vx3i * vy3r);
            vx0i = (v2f64) __msa_pckod_d((v2i64) vx9, (v2i64) vx8);
            vy0r = (v2f64) __msa_pckev_d((v2i64) vy9, (v2i64) vy8);
            vx2 = vx10;
            vy0i = (v2f64) __msa_pckod_d((v2i64) vy9, (v2i64) vy8);
            vx3 = vx11;
            dot4 OP1 (vx2i * vy2i);
            vy2 = vy10;
            dot5 += (vx2r * vy2i);
            vy3 = vy11;
            dot6 OP1 (vx3i * vy3i);
            dot7 += (vx3r * vy3i);
        }

        vx4 = LD_DP(x); x += inc_x2;
        vx1r = (v2f64) __msa_pckev_d((v2i64) vx3, (v2i64) vx2);
        dot0 += (vx0r * vy0r);
        vx5 = LD_DP(x); x += inc_x2;
        vx1i = (v2f64) __msa_pckod_d((v2i64) vx3, (v2i64) vx2);
        dot1 OP2 (vx0i * vy0r);
        vy4 = LD_DP(y); y += inc_y2;
        vy1r = (v2f64) __msa_pckev_d((v2i64) vy3, (v2i64) vy2);
        dot2 += (vx1r * vy1r);
        vy5 = LD_DP(y); y += inc_y2;
        vy1i = (v2f64) __msa_pckod_d((v2i64) vy3, (v2i64) vy2);
        dot3 OP2 (vx1i * vy1r);
        vx6 = LD_DP(x); x += inc_x2;
        vx7 = LD_DP(x); x += inc_x2;
        vy6 = LD_DP(y); y += inc_y2;
        vy7 = LD_DP(y); y += inc_y2;
        dot0 OP1 (vx0i * vy0i);
        vx2r = (v2f64) __msa_pckev_d((v2i64) vx5, (v2i64) vx4);
        dot1 += (vx0r * vy0i);
        vx2i = (v2f64) __msa_pckod_d((v2i64) vx5, (v2i64) vx4);
        dot2 OP1 (vx1i * vy1i);
        vy2r = (v2f64) __msa_pckev_d((v2i64) vy5, (v2i64) vy4);
        dot3 += (vx1r * vy1i);
        vy2i = (v2f64) __msa_pckod_d((v2i64) vy5, (v2i64) vy4);
        vx3r = (v2f64) __msa_pckev_d((v2i64) vx7, (v2i64) vx6);
        dot4 += (vx2r * vy2r);
        vx3i = (v2f64) __msa_pckod_d((v2i64) vx7, (v2i64) vx6);
        dot5 OP2 (vx2i * vy2r);
        vy3r = (v2f64) __msa_pckev_d((v2i64) vy7, (v2i64) vy6);
        vy3i = (v2f64) __msa_pckod_d((v2i64) vy7, (v2i64) vy6);
        dot6 += (vx3r * vy3r);
        dot7 OP2 (vx3i * vy3r);
        dot4 OP1 (vx2i * vy2i);
        dot5 += (vx2r * vy2i);
        dot6 OP1 (vx3i * vy3i);
        dot7 += (vx3r * vy3i);
    }

    if (n & 7)
    {
        if (n & 4)
        {
            LD_DP4_INC(x, inc_x2, vx0, vx1, vx2, vx3);
            LD_DP4_INC(y, inc_y2, vy0, vy1, vy2, vy3);

            PCKEVOD_D2_DP(vx1, vx0, vx0r, vx0i);
            PCKEVOD_D2_DP(vx3, vx2, vx1r, vx1i);

            PCKEVOD_D2_DP(vy1, vy0, vy0r, vy0i);
            PCKEVOD_D2_DP(vy3, vy2, vy1r, vy1i);

            dot0 += (vx0r * vy0r);
            dot0 OP1 (vx0i * vy0i);
            dot1 OP2 (vx0i * vy0r);
            dot1 += (vx0r * vy0i);

            dot2 += (vx1r * vy1r);
            dot2 OP1 (vx1i * vy1i);
            dot3 OP2 (vx1i * vy1r);
            dot3 += (vx1r * vy1i);
        }

        if (n & 2)
        {
            LD_DP2_INC(x, inc_x2, vx0, vx1);
            LD_DP2_INC(y, inc_y2, vy0, vy1);
            PCKEVOD_D2_DP(vx1, vx0, vx0r, vx0i);
            PCKEVOD_D2_DP(vy1, vy0, vy0r, vy0i);

            dot0 += (vx0r * vy0r);
            dot0 OP1 (vx0i * vy0i);
            dot1 OP2 (vx0i * vy0r);
            dot1 += (vx0r * vy0i);
        }

        if (n & 1)
        {
            vx0 = LD_DP(x);
            vy0 = LD_DP(y);
            PCKEVOD_D2_DP(zero, vx0, vx0r, vx0i);
            PCKEVOD_D2_DP(zero, vy0, vy0r, vy0i);

            dot0 += (vx0r * vy0r);
            dot0 OP1 (vx0i * vy0i);
            dot1 OP2 (vx0i * vy0r);
            dot1 += (vx0r * vy0i);
        }
    }

    dot0 += dot2 + dot4 + dot6;
    dot1 += dot3 + dot5 + dot7;

    dot[0] += (dot0[0] + dot0[1]);
    dot[1] += (dot1[0] + dot1[1]);

    CREAL(result) = dot[0];
    CIMAG(result) = dot[1];

    return (result);
}
