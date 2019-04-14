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

int CNAME(BLASLONG m, BLASLONG n, FLOAT * __restrict src, BLASLONG lda,
          FLOAT * __restrict dst)
{
    BLASLONG i, j;
    FLOAT *psrc0, *psrc1, *psrc2, *psrc3, *psrc4;
    FLOAT *pdst0, *pdst1, *pdst2, *pdst3;
    v2f64 src0, src1, src2, src3, src4, src5, src6, src7;

    psrc0 = src;
    pdst0 = dst;

    pdst2 = dst + m * (n & ~3);
    pdst3 = dst + m * (n & ~1);

    for (j = (m >> 2); j--;)
    {
        psrc1 = psrc0;
        psrc2 = psrc1 + lda;
        psrc3 = psrc2 + lda;
        psrc4 = psrc3 + lda;
        psrc0 += 4 * lda;

        pdst1 = pdst0;
        pdst0 += 16;

        for (i = (n >> 2); i--;)
        {
            LD_DP2_INC(psrc1, 2, src0, src1);
            LD_DP2_INC(psrc2, 2, src2, src3);
            LD_DP2_INC(psrc3, 2, src4, src5);
            LD_DP2_INC(psrc4, 2, src6, src7);

            ST_DP8(src0, src1, src2, src3, src4, src5, src6, src7, pdst1, 2);
            pdst1 += m * 4;
        }

        if (n & 2)
        {
            src0 = LD_DP(psrc1);
            src1 = LD_DP(psrc2);
            src2 = LD_DP(psrc3);
            src3 = LD_DP(psrc4);
            psrc1 += 2;
            psrc2 += 2;
            psrc3 += 2;
            psrc4 += 2;

            ST_DP4_INC(src0, src1, src2, src3, pdst2, 2);
        }

        if (n & 1)
        {
            *pdst3++ = *psrc1++;
            *pdst3++ = *psrc2++;
            *pdst3++ = *psrc3++;
            *pdst3++ = *psrc4++;
        }
    }

    if (m & 2)
    {
        psrc1 = psrc0;
        psrc2 = psrc1 + lda;
        psrc0 += 2 * lda;

        pdst1 = pdst0;
        pdst0 += 8;

        for (i = (n >> 2); i--;)
        {
            LD_DP2_INC(psrc1, 2, src0, src1);
            LD_DP2_INC(psrc2, 2, src2, src3);

            ST_DP4(src0, src1, src2, src3, pdst1, 2);
            pdst1 += m * 4;
        }

        if (n & 2)
        {
            src0 = LD_DP(psrc1);
            src1 = LD_DP(psrc2);
            psrc1 += 2;
            psrc2 += 2;

            ST_DP2_INC(src0, src1, pdst2, 2);
        }

        if (n & 1)
        {
            *pdst3++ = *psrc1++;
            *pdst3++ = *psrc2++;
        }
    }

    if (m & 1)
    {
        psrc1 = psrc0;
        pdst1 = pdst0;

        for (i = (n >> 2); i--;)
        {
            LD_DP2_INC(psrc1, 2, src0, src1);

            ST_DP2(src0, src1, pdst1, 2);
            pdst1 += 4 * m;
        }

        if (n & 2)
        {
            src0 = LD_DP(psrc1);
            psrc1 += 2;

            ST_DP(src0, pdst2);
        }

        if (n & 1)
        {
            *pdst3 = *psrc1;
        }
    }

    return 0;
}
