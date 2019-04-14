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
    FLOAT *psrc5, *psrc6, *psrc7, *psrc8;
    FLOAT *pdst0, *pdst1, *pdst2, *pdst3, *pdst4;
    v2f64 src0, src1, src2, src3, src4, src5, src6, src7;
    v2f64 src8, src9, src10, src11, src12, src13, src14, src15;

    psrc0 = src;
    pdst0 = dst;

    pdst2 = dst + m * (n & ~7);
    pdst3 = dst + m * (n & ~3);
    pdst4 = dst + m * (n & ~1);

    for (j = (m >> 3); j--;)
    {
        psrc1 = psrc0;
        psrc2 = psrc1 + lda;
        psrc3 = psrc2 + lda;
        psrc4 = psrc3 + lda;
        psrc5 = psrc4 + lda;
        psrc6 = psrc5 + lda;
        psrc7 = psrc6 + lda;
        psrc8 = psrc7 + lda;
        psrc0 += 8 * lda;

        pdst1 = pdst0;
        pdst0 += 64;

        for (i = (n >> 3); i--;)
        {
            LD_DP4_INC(psrc1, 2, src0, src1, src2, src3);
            LD_DP4_INC(psrc2, 2, src4, src5, src6, src7);
            LD_DP4_INC(psrc3, 2, src8, src9, src10, src11);
            LD_DP4_INC(psrc4, 2, src12, src13, src14, src15);

            ST_DP8(src0, src1, src2, src3, src4, src5, src6, src7, pdst1, 2);
            ST_DP8(src8, src9, src10, src11, src12, src13, src14, src15,
                   pdst1 + 16, 2);

            LD_DP4_INC(psrc5, 2, src0, src1, src2, src3);
            LD_DP4_INC(psrc6, 2, src4, src5, src6, src7);
            LD_DP4_INC(psrc7, 2, src8, src9, src10, src11);
            LD_DP4_INC(psrc8, 2, src12, src13, src14, src15);

            ST_DP8(src0, src1, src2, src3, src4, src5, src6, src7, pdst1 + 32,
                   2);
            ST_DP8(src8, src9, src10, src11, src12, src13, src14, src15,
                   pdst1 + 48, 2);
            pdst1 += m * 8;
        }

        if (n & 4)
        {
            LD_DP2_INC(psrc1, 2, src0, src1);
            LD_DP2_INC(psrc2, 2, src2, src3);
            LD_DP2_INC(psrc3, 2, src4, src5);
            LD_DP2_INC(psrc4, 2, src6, src7);
            LD_DP2_INC(psrc5, 2, src8, src9);
            LD_DP2_INC(psrc6, 2, src10, src11);
            LD_DP2_INC(psrc7, 2, src12, src13);
            LD_DP2_INC(psrc8, 2, src14, src15);

            ST_DP8_INC(src0, src1, src2, src3, src4, src5, src6, src7, pdst2, 2);
            ST_DP8_INC(src8, src9, src10, src11, src12, src13, src14, src15,
                       pdst2, 2);
        }

        if (n & 2)
        {
            src0 = LD_DP(psrc1);
            src1 = LD_DP(psrc2);
            src2 = LD_DP(psrc3);
            src3 = LD_DP(psrc4);
            src4 = LD_DP(psrc5);
            src5 = LD_DP(psrc6);
            src6 = LD_DP(psrc7);
            src7 = LD_DP(psrc8);
            psrc1 += 2;
            psrc2 += 2;
            psrc3 += 2;
            psrc4 += 2;
            psrc5 += 2;
            psrc6 += 2;
            psrc7 += 2;
            psrc8 += 2;

            ST_DP8_INC(src0, src1, src2, src3, src4, src5, src6, src7, pdst3, 2);
        }

        if (n & 1)
        {
            *pdst4++ = *psrc1++;
            *pdst4++ = *psrc2++;
            *pdst4++ = *psrc3++;
            *pdst4++ = *psrc4++;
            *pdst4++ = *psrc5++;
            *pdst4++ = *psrc6++;
            *pdst4++ = *psrc7++;
            *pdst4++ = *psrc8++;
        }
    }

    if (m & 4)
    {
        psrc1 = psrc0;
        psrc2 = psrc1 + lda;
        psrc3 = psrc2 + lda;
        psrc4 = psrc3 + lda;
        psrc0 += 4 * lda;

        pdst1 = pdst0;
        pdst0 += 32;

        for (i = (n >> 3); i--;)
        {
            LD_DP4_INC(psrc1, 2, src0, src1, src2, src3);
            LD_DP4_INC(psrc2, 2, src4, src5, src6, src7);
            LD_DP4_INC(psrc3, 2, src8, src9, src10, src11);
            LD_DP4_INC(psrc4, 2, src12, src13, src14, src15);

            ST_DP8(src0, src1, src2, src3, src4, src5, src6, src7, pdst1, 2);
            ST_DP8(src8, src9, src10, src11, src12, src13, src14, src15,
                   pdst1 + 16, 2);
            pdst1 += 8 * m;
        }

        if (n & 4)
        {
            LD_DP2_INC(psrc1, 2, src0, src1);
            LD_DP2_INC(psrc2, 2, src2, src3);
            LD_DP2_INC(psrc3, 2, src4, src5);
            LD_DP2_INC(psrc4, 2, src6, src7);

            ST_DP8_INC(src0, src1, src2, src3, src4, src5, src6, src7, pdst2, 2);
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

            ST_DP4_INC(src0, src1, src2, src3, pdst3, 2);
        }

        if (n & 1)
        {
            *pdst4++ = *psrc1++;
            *pdst4++ = *psrc2++;
            *pdst4++ = *psrc3++;
            *pdst4++ = *psrc4++;
        }
    }

    if (m & 2)
    {
        psrc1 = psrc0;
        psrc2 = psrc1 + lda;
        psrc0 += 2 * lda;

        pdst1 = pdst0;
        pdst0 += 16;

        for (i = (n >> 3); i--;)
        {
            LD_DP4_INC(psrc1, 2, src0, src1, src2, src3);
            LD_DP4_INC(psrc2, 2, src4, src5, src6, src7);

            ST_DP8(src0, src1, src2, src3, src4, src5, src6, src7, pdst1, 2);
            pdst1 += 8 * m;
        }

        if (n & 4)
        {
            LD_DP2_INC(psrc1, 2, src0, src1);
            LD_DP2_INC(psrc2, 2, src2, src3);

            ST_DP4_INC(src0, src1, src2, src3, pdst2, 2);
        }

        if (n & 2)
        {
            src0 = LD_DP(psrc1);
            src1 = LD_DP(psrc2);
            psrc1 += 2;
            psrc2 += 2;

            ST_DP2_INC(src0, src1, pdst3, 2);
        }

        if (n & 1)
        {
            *pdst4++ = *psrc1++;
            *pdst4++ = *psrc2++;
        }
    }

    if (m & 1)
    {
        psrc1 = psrc0;
        psrc0 += lda;

        pdst1 = pdst0;
        pdst0 += 8;

        for (i = (n >> 3); i--;)
        {
            LD_DP4_INC(psrc1, 2, src0, src1, src2, src3);

            ST_DP4(src0, src1, src2, src3, pdst1, 2);
            pdst1 += 8 * m;
        }

        if (n & 4)
        {
            LD_DP2_INC(psrc1, 2, src0, src1);

            ST_DP2_INC(src0, src1, pdst2, 2);
        }

        if (n & 2)
        {
            src0 = LD_DP(psrc1);
            psrc1 += 2;

            ST_DP(src0, pdst3);
            pdst3 += 2;
        }

        if (n & 1)
        {
            *pdst4++ = *psrc1++;
        }
    }

    return 0;
}
