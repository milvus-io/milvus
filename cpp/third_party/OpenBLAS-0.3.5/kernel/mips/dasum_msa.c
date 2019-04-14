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
#include <math.h>
#include "macros_msa.h"

#define AND_VEC_D(in)   ((v2f64) ((v2i64) in & and_vec))

FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
    BLASLONG i;
    FLOAT sumf = 0.0;
    v2f64 src0, src1, src2, src3, src4, src5, src6, src7;
    v2f64 src8, src9, src10, src11, src12, src13, src14, src15;
    v2f64 sum_abs0 = {0, 0};
    v2f64 sum_abs1 = {0, 0};
    v2f64 sum_abs2 = {0, 0};
    v2f64 sum_abs3 = {0, 0};
    v2i64 and_vec = {0x7FFFFFFFFFFFFFFF, 0x7FFFFFFFFFFFFFFF};

    if (n <= 0 || inc_x <= 0) return (sumf);

    if (1 == inc_x)
    {
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
            x_pref = x + pref_offset + 64 + 16;

            LD_DP8_INC(x, 2, src0, src1, src2, src3, src4, src5, src6, src7);
            for (i = (n >> 5) - 1; i--;)
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

                LD_DP8_INC(x, 2, src8, src9, src10, src11, src12, src13, src14, src15);

                sum_abs0 += AND_VEC_D(src0);
                sum_abs1 += AND_VEC_D(src1);
                sum_abs2 += AND_VEC_D(src2);
                sum_abs3 += AND_VEC_D(src3);
                sum_abs0 += AND_VEC_D(src4);
                sum_abs1 += AND_VEC_D(src5);
                sum_abs2 += AND_VEC_D(src6);
                sum_abs3 += AND_VEC_D(src7);

                LD_DP8_INC(x, 2, src0, src1, src2, src3, src4, src5, src6, src7);

                sum_abs0 += AND_VEC_D(src8);
                sum_abs1 += AND_VEC_D(src9);
                sum_abs2 += AND_VEC_D(src10);
                sum_abs3 += AND_VEC_D(src11);
                sum_abs0 += AND_VEC_D(src12);
                sum_abs1 += AND_VEC_D(src13);
                sum_abs2 += AND_VEC_D(src14);
                sum_abs3 += AND_VEC_D(src15);
            }

            LD_DP8_INC(x, 2, src8, src9, src10, src11, src12, src13, src14, src15);

            sum_abs0 += AND_VEC_D(src0);
            sum_abs1 += AND_VEC_D(src1);
            sum_abs2 += AND_VEC_D(src2);
            sum_abs3 += AND_VEC_D(src3);
            sum_abs0 += AND_VEC_D(src4);
            sum_abs1 += AND_VEC_D(src5);
            sum_abs2 += AND_VEC_D(src6);
            sum_abs3 += AND_VEC_D(src7);
            sum_abs0 += AND_VEC_D(src8);
            sum_abs1 += AND_VEC_D(src9);
            sum_abs2 += AND_VEC_D(src10);
            sum_abs3 += AND_VEC_D(src11);
            sum_abs0 += AND_VEC_D(src12);
            sum_abs1 += AND_VEC_D(src13);
            sum_abs2 += AND_VEC_D(src14);
            sum_abs3 += AND_VEC_D(src15);
        }

        if (n & 31)
        {
            if (n & 16)
            {
                LD_DP8_INC(x, 2, src0, src1, src2, src3, src4, src5, src6, src7);

                sum_abs0 += AND_VEC_D(src0);
                sum_abs1 += AND_VEC_D(src1);
                sum_abs2 += AND_VEC_D(src2);
                sum_abs3 += AND_VEC_D(src3);
                sum_abs0 += AND_VEC_D(src4);
                sum_abs1 += AND_VEC_D(src5);
                sum_abs2 += AND_VEC_D(src6);
                sum_abs3 += AND_VEC_D(src7);
            }

            if (n & 8)
            {
                LD_DP4_INC(x, 2, src0, src1, src2, src3);

                sum_abs0 += AND_VEC_D(src0);
                sum_abs1 += AND_VEC_D(src1);
                sum_abs2 += AND_VEC_D(src2);
                sum_abs3 += AND_VEC_D(src3);
            }

            if (n & 4)
            {
                LD_DP2_INC(x, 2, src0, src1);

                sum_abs0 += AND_VEC_D(src0);
                sum_abs1 += AND_VEC_D(src1);
            }

            if (n & 2)
            {
                src0 = LD_DP(x); x += 2;

                sum_abs0 += AND_VEC_D(src0);
            }

            if (n & 1)
            {
                sumf += fabs(*x);
            }
        }

        sum_abs0 += sum_abs1 + sum_abs2 + sum_abs3;

        sumf += sum_abs0[0] + sum_abs0[1];
    }
    else
    {
        if (n > 16)
        {
            LD_DP8_INC(x, inc_x, src0, src1, src2, src3, src4, src5, src6, src7);
            for (i = (n >> 4) - 1; i--;)
            {
                LD_DP8_INC(x, inc_x, src8, src9, src10, src11, src12, src13, src14, src15);

                sum_abs0 += AND_VEC_D(src0);
                sum_abs1 += AND_VEC_D(src1);
                sum_abs2 += AND_VEC_D(src2);
                sum_abs3 += AND_VEC_D(src3);
                sum_abs0 += AND_VEC_D(src4);
                sum_abs1 += AND_VEC_D(src5);
                sum_abs2 += AND_VEC_D(src6);
                sum_abs3 += AND_VEC_D(src7);

                LD_DP8_INC(x, inc_x, src0, src1, src2, src3, src4, src5, src6, src7);

                sum_abs0 += AND_VEC_D(src8);
                sum_abs1 += AND_VEC_D(src9);
                sum_abs2 += AND_VEC_D(src10);
                sum_abs3 += AND_VEC_D(src11);
                sum_abs0 += AND_VEC_D(src12);
                sum_abs1 += AND_VEC_D(src13);
                sum_abs2 += AND_VEC_D(src14);
                sum_abs3 += AND_VEC_D(src15);
            }

            LD_DP8_INC(x, inc_x, src8, src9, src10, src11, src12, src13, src14, src15);

            sum_abs0 += AND_VEC_D(src0);
            sum_abs1 += AND_VEC_D(src1);
            sum_abs2 += AND_VEC_D(src2);
            sum_abs3 += AND_VEC_D(src3);
            sum_abs0 += AND_VEC_D(src4);
            sum_abs1 += AND_VEC_D(src5);
            sum_abs2 += AND_VEC_D(src6);
            sum_abs3 += AND_VEC_D(src7);
            sum_abs0 += AND_VEC_D(src8);
            sum_abs1 += AND_VEC_D(src9);
            sum_abs2 += AND_VEC_D(src10);
            sum_abs3 += AND_VEC_D(src11);
            sum_abs0 += AND_VEC_D(src12);
            sum_abs1 += AND_VEC_D(src13);
            sum_abs2 += AND_VEC_D(src14);
            sum_abs3 += AND_VEC_D(src15);
        }

        if (n & 15)
        {
            if (n & 8)
            {
                LD_DP8_INC(x, inc_x, src0, src1, src2, src3, src4, src5, src6, src7);

                sum_abs0 += AND_VEC_D(src0);
                sum_abs1 += AND_VEC_D(src1);
                sum_abs2 += AND_VEC_D(src2);
                sum_abs3 += AND_VEC_D(src3);
                sum_abs0 += AND_VEC_D(src4);
                sum_abs1 += AND_VEC_D(src5);
                sum_abs2 += AND_VEC_D(src6);
                sum_abs3 += AND_VEC_D(src7);
            }

            if (n & 4)
            {
                LD_DP4_INC(x, inc_x, src0, src1, src2, src3);

                sum_abs0 += AND_VEC_D(src0);
                sum_abs1 += AND_VEC_D(src1);
                sum_abs2 += AND_VEC_D(src2);
                sum_abs3 += AND_VEC_D(src3);
            }

            if (n & 2)
            {
                LD_DP2_INC(x, inc_x, src0, src1);

                sum_abs0 += AND_VEC_D(src0);
                sum_abs1 += AND_VEC_D(src1);
            }

            if (n & 1)
            {
                src0 = LD_DP(x);

                sum_abs0 += AND_VEC_D(src0);
            }
        }

        sum_abs0 += sum_abs1 + sum_abs2 + sum_abs3;

        sumf = sum_abs0[0];
    }

    return (sumf);
}
