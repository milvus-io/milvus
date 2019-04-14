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
          FLOAT *  __restrict dst)
{
    BLASLONG i, j;
    FLOAT *psrc0, *psrc1, *psrc2, *psrc3, *psrc4, *psrc5, *psrc6, *psrc7;
    FLOAT *psrc8, *pdst;
    v2f64 src0, src1, src2, src3, src4, src5, src6, src7;
    v2f64 src8, src9, src10, src11, src12, src13, src14, src15;
    v2f64 dst0, dst1, dst2, dst3, dst4, dst5, dst6, dst7;

    psrc0 = src;
    pdst = dst;

    for (j = (n >> 3); j--;)
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

        for (i = (m >> 3); i--;)
        {
            LD_DP2_INC(psrc1, 2, src0, src1);
            LD_DP2_INC(psrc2, 2, src2, src3);
            LD_DP2_INC(psrc3, 2, src4, src5);
            LD_DP2_INC(psrc4, 2, src6, src7);
            LD_DP2_INC(psrc5, 2, src8, src9);
            LD_DP2_INC(psrc6, 2, src10, src11);
            LD_DP2_INC(psrc7, 2, src12, src13);
            LD_DP2_INC(psrc8, 2, src14, src15);

            ILVRL_D2_DP(src2, src0, dst0, dst4);
            ILVRL_D2_DP(src6, src4, dst1, dst5);
            ILVRL_D2_DP(src10, src8, dst2, dst6);
            ILVRL_D2_DP(src14, src12, dst3, dst7);

            ST_DP8_INC(dst0, dst1, dst2, dst3, dst4, dst5, dst6, dst7, pdst, 2);

            ILVRL_D2_DP(src3, src1, dst0, dst4);
            ILVRL_D2_DP(src7, src5, dst1, dst5);
            ILVRL_D2_DP(src11, src9, dst2, dst6);
            ILVRL_D2_DP(src15, src13, dst3, dst7);

            ST_DP8_INC(dst0, dst1, dst2, dst3, dst4, dst5, dst6, dst7, pdst, 2);

            LD_DP2_INC(psrc1, 2, src0, src1);
            LD_DP2_INC(psrc2, 2, src2, src3);
            LD_DP2_INC(psrc3, 2, src4, src5);
            LD_DP2_INC(psrc4, 2, src6, src7);
            LD_DP2_INC(psrc5, 2, src8, src9);
            LD_DP2_INC(psrc6, 2, src10, src11);
            LD_DP2_INC(psrc7, 2, src12, src13);
            LD_DP2_INC(psrc8, 2, src14, src15);

            ILVRL_D2_DP(src2, src0, dst0, dst4);
            ILVRL_D2_DP(src6, src4, dst1, dst5);
            ILVRL_D2_DP(src10, src8, dst2, dst6);
            ILVRL_D2_DP(src14, src12, dst3, dst7);

            ST_DP8_INC(dst0, dst1, dst2, dst3, dst4, dst5, dst6, dst7, pdst, 2);

            ILVRL_D2_DP(src3, src1, dst0, dst4);
            ILVRL_D2_DP(src7, src5, dst1, dst5);
            ILVRL_D2_DP(src11, src9, dst2, dst6);
            ILVRL_D2_DP(src15, src13, dst3, dst7);

            ST_DP8_INC(dst0, dst1, dst2, dst3, dst4, dst5, dst6, dst7, pdst, 2);
        }

        for (i = (m & 7); i--;)
        {
            *pdst++ = *psrc1++;
            *pdst++ = *psrc2++;
            *pdst++ = *psrc3++;
            *pdst++ = *psrc4++;
            *pdst++ = *psrc5++;
            *pdst++ = *psrc6++;
            *pdst++ = *psrc7++;
            *pdst++ = *psrc8++;
        }
    }

    if (n & 4)
    {
        psrc1 = psrc0;
        psrc2 = psrc1 + lda;
        psrc3 = psrc2 + lda;
        psrc4 = psrc3 + lda;
        psrc0 += 4 * lda;

        for (i = (m >> 2); i--;)
        {
            LD_DP2_INC(psrc1, 2, src0, src1);
            LD_DP2_INC(psrc2, 2, src2, src3);
            LD_DP2_INC(psrc3, 2, src4, src5);
            LD_DP2_INC(psrc4, 2, src6, src7);

            ILVRL_D2_DP(src2, src0, dst0, dst4);
            ILVRL_D2_DP(src6, src4, dst1, dst5);
            ILVRL_D2_DP(src3, src1, dst2, dst6);
            ILVRL_D2_DP(src7, src5, dst3, dst7);

            ST_DP8_INC(dst0, dst1, dst4, dst5, dst2, dst3, dst6, dst7, pdst, 2);
        }

        for (i = (m & 3); i--;)
        {
            *pdst++ = *psrc1++;
            *pdst++ = *psrc2++;
            *pdst++ = *psrc3++;
            *pdst++ = *psrc4++;
        }
    }

    if (n & 2)
    {
        psrc1 = psrc0;
        psrc2 = psrc1 + lda;
        psrc0 += 2 * lda;

        for (i = (m >> 1); i--;)
        {
            src0 = LD_DP(psrc1);
            src1 = LD_DP(psrc2);
            psrc1 += 2;
            psrc2 += 2;

            ILVRL_D2_DP(src1, src0, dst0, dst1);

            ST_DP2_INC(dst0, dst1, pdst, 2);
        }

        if (m & 1)
        {
            *pdst++ = *psrc1++;
            *pdst++ = *psrc2++;
        }
    }

    if (n & 1)
    {
        psrc1 = psrc0;

        for (i = m; i--;)
        {
            *pdst++ = *psrc1++;
        }
    }

    return 0;
}
