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

int CNAME(BLASLONG m, BLASLONG n, FLOAT *src, BLASLONG lda, FLOAT *dst)
{
    BLASLONG i, j;
    FLOAT *psrc0, *psrc1, *psrc2, *psrc3, *psrc4, *psrc5, *psrc6, *psrc7;
    FLOAT *psrc8, *pdst;
    v4f32 src0, src1, src2, src3, src4, src5, src6, src7;
    v4f32 src8, src9, src10, src11, src12, src13, src14, src15;
    v4f32 dst0, dst1, dst2, dst3, dst4, dst5, dst6, dst7;
    v4f32 dst8, dst9, dst10, dst11, dst12, dst13, dst14, dst15;

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
            LD_SP2_INC(psrc1, 4, src0, src1);
            LD_SP2_INC(psrc2, 4, src2, src3);
            LD_SP2_INC(psrc3, 4, src4, src5);
            LD_SP2_INC(psrc4, 4, src6, src7);
            LD_SP2_INC(psrc5, 4, src8, src9);
            LD_SP2_INC(psrc6, 4, src10, src11);
            LD_SP2_INC(psrc7, 4, src12, src13);
            LD_SP2_INC(psrc8, 4, src14, src15);

            TRANSPOSE4x4_SP_SP(src0, src2, src4, src6, dst0, dst2, dst4, dst6);
            TRANSPOSE4x4_SP_SP(src8, src10, src12, src14, dst1, dst3, dst5,
                               dst7);
            TRANSPOSE4x4_SP_SP(src1, src3, src5, src7, dst8, dst10, dst12,
                               dst14);
            TRANSPOSE4x4_SP_SP(src9, src11, src13, src15, dst9, dst11, dst13,
                               dst15);

            ST_SP2_INC(dst0, dst1, pdst, 4);
            ST_SP2_INC(dst2, dst3, pdst, 4);
            ST_SP2_INC(dst4, dst5, pdst, 4);
            ST_SP2_INC(dst6, dst7, pdst, 4);
            ST_SP2_INC(dst8, dst9, pdst, 4);
            ST_SP2_INC(dst10, dst11, pdst, 4);
            ST_SP2_INC(dst12, dst13, pdst, 4);
            ST_SP2_INC(dst14, dst15, pdst, 4);
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
            src0 = LD_SP(psrc1);
            src1 = LD_SP(psrc2);
            src2 = LD_SP(psrc3);
            src3 = LD_SP(psrc4);
            psrc1 += 4;
            psrc2 += 4;
            psrc3 += 4;
            psrc4 += 4;

            TRANSPOSE4x4_SP_SP(src0, src1, src2, src3, dst0, dst1, dst2, dst3);

            ST_SP2_INC(dst0, dst1, pdst, 4);
            ST_SP2_INC(dst2, dst3, pdst, 4);
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
            *pdst++ = *psrc1++;
            *pdst++ = *psrc2++;
            *pdst++ = *psrc1++;
            *pdst++ = *psrc2++;
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
