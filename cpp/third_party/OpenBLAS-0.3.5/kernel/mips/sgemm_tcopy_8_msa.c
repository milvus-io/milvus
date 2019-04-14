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
    FLOAT *psrc8, *pdst0,  *pdst1, *pdst2, *pdst3, *pdst4;
    v4f32 src0, src1, src2, src3, src4, src5, src6, src7;
    v4f32 src8, src9, src10, src11, src12, src13, src14, src15;

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
            LD_SP2_INC(psrc1, 4, src0, src1);
            LD_SP2_INC(psrc2, 4, src2, src3);
            LD_SP2_INC(psrc3, 4, src4, src5);
            LD_SP2_INC(psrc4, 4, src6, src7);
            LD_SP2_INC(psrc5, 4, src8, src9);
            LD_SP2_INC(psrc6, 4, src10, src11);
            LD_SP2_INC(psrc7, 4, src12, src13);
            LD_SP2_INC(psrc8, 4, src14, src15);

            ST_SP8(src0, src1, src2, src3, src4, src5, src6, src7, pdst1, 4);
            ST_SP8(src8, src9, src10, src11, src12, src13, src14, src15,
                   pdst1 + 32, 4);
            pdst1 += m * 8;
        }

        if (n & 4)
        {
            src0 = LD_SP(psrc1);
            src1 = LD_SP(psrc2);
            src2 = LD_SP(psrc3);
            src3 = LD_SP(psrc4);
            src4 = LD_SP(psrc5);
            src5 = LD_SP(psrc6);
            src6 = LD_SP(psrc7);
            src7 = LD_SP(psrc8);
            psrc1 += 4;
            psrc2 += 4;
            psrc3 += 4;
            psrc4 += 4;
            psrc5 += 4;
            psrc6 += 4;
            psrc7 += 4;
            psrc8 += 4;

            ST_SP8_INC(src0, src1, src2, src3, src4, src5, src6, src7, pdst2, 4);
        }

        if (n & 2)
        {
            *pdst3++ = *psrc1++;
            *pdst3++ = *psrc1++;
            *pdst3++ = *psrc2++;
            *pdst3++ = *psrc2++;
            *pdst3++ = *psrc3++;
            *pdst3++ = *psrc3++;
            *pdst3++ = *psrc4++;
            *pdst3++ = *psrc4++;
            *pdst3++ = *psrc5++;
            *pdst3++ = *psrc5++;
            *pdst3++ = *psrc6++;
            *pdst3++ = *psrc6++;
            *pdst3++ = *psrc7++;
            *pdst3++ = *psrc7++;
            *pdst3++ = *psrc8++;
            *pdst3++ = *psrc8++;
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

        pdst1  = pdst0;
        pdst0 += 32;

        for (i = (n >> 3); i--;)
        {
            LD_SP2_INC(psrc1, 4, src0, src1);
            LD_SP2_INC(psrc2, 4, src2, src3);
            LD_SP2_INC(psrc3, 4, src4, src5);
            LD_SP2_INC(psrc4, 4, src6, src7);

            ST_SP8(src0, src1, src2, src3, src4, src5, src6, src7, pdst1, 4);
            pdst1 += 8 * m;
        }

        if (n & 4)
        {
            src0 = LD_SP(psrc1);
            src1 = LD_SP(psrc2);
            src2 = LD_SP(psrc3);
            src3 = LD_SP(psrc4);
            psrc1 += 4;
            psrc2 += 4;
            psrc3 += 4;
            psrc4 += 4;

            ST_SP4_INC(src0, src1, src2, src3, pdst2, 4);
        }

        if (n & 2)
        {
            *pdst3++ = *psrc1++;
            *pdst3++ = *psrc1++;
            *pdst3++ = *psrc2++;
            *pdst3++ = *psrc2++;
            *pdst3++ = *psrc3++;
            *pdst3++ = *psrc3++;
            *pdst3++ = *psrc4++;
            *pdst3++ = *psrc4++;
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
            LD_SP2_INC(psrc1, 4, src0, src1);
            LD_SP2_INC(psrc2, 4, src2, src3);

            ST_SP4(src0, src1, src2, src3, pdst1, 4);
            pdst1 += 8 * m;
        }

        if (n & 4)
        {
            src0 = LD_SP(psrc1);
            src1 = LD_SP(psrc2);
            psrc1 += 4;
            psrc2 += 4;

            ST_SP2_INC(src0, src1, pdst2, 4);
        }

        if (n & 2)
        {
            *pdst3++ = *psrc1++;
            *pdst3++ = *psrc1++;
            *pdst3++ = *psrc2++;
            *pdst3++ = *psrc2++;
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
            LD_SP2_INC(psrc1, 4, src0, src1);

            ST_SP2(src0, src1, pdst1, 4);
            pdst1 += 8 * m;
        }

        if (n & 4)
        {
            src0 = LD_SP(psrc1);
            psrc1 += 4;

            ST_SP(src0, pdst2);
            pdst2 += 4;
        }

        if (n & 2)
        {
            *pdst3++ = *psrc1++;
            *pdst3++ = *psrc1++;
        }

        if (n & 1)
        {
            *pdst4++ = *psrc1++;
        }
    }

    return 0;
}
