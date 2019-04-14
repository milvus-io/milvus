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
    FLOAT *psrc0;
    FLOAT *psrc1, *psrc2;
    FLOAT *pdst0;
    FLOAT ctemp01, ctemp02, ctemp03, ctemp04;
    v4f32 src0, src1, src2, src3;

    psrc0 = src;
    pdst0 = dst;
    lda *= 2;

    for (j = (n >> 2); j--;)
    {
        psrc1 = psrc0;
        psrc2 = psrc0 + lda;
        psrc0 += 8;

        for (i = (m >> 1); i--;)
        {
            LD_SP2(psrc1, 4, src0, src1);
            LD_SP2(psrc2, 4, src2, src3);
            ST_SP4_INC(src0, src1, src2, src3, pdst0, 4);
            psrc1 += 2 * lda;
            psrc2 += 2 * lda;
        }

        if (m & 1)
        {
            LD_SP2(psrc1, 4, src0, src1);
            ST_SP2_INC(src0, src1, pdst0, 4);
        }
    }

    if (n & 2)
    {
        psrc1 = psrc0;
        psrc2 = psrc0 + lda;
        psrc0 += 4;

        for (i = (m >> 1); i--;)
        {
            src0 = LD_SP(psrc1);
            src1 = LD_SP(psrc2);
            ST_SP2_INC(src0, src1, pdst0, 4);

            psrc1 += 2 * lda;
            psrc2 += 2 * lda;
        }

        if (m & 1)
        {
            src0 = LD_SP(psrc1);
            ST_SP(src0, pdst0);
            pdst0 += 4;
        }
    }

    if (n & 1)
    {
        psrc1 = psrc0;
        psrc2 = psrc0 + lda;
        psrc0 += 2;

        for (i = (m >> 1); i--;)
        {
            ctemp01 = *(psrc1 + 0);
            ctemp02 = *(psrc1 + 1);
            ctemp03 = *(psrc2 + 0);
            ctemp04 = *(psrc2 + 1);

            *(pdst0 + 0) = ctemp01;
            *(pdst0 + 1) = ctemp02;
            *(pdst0 + 2) = ctemp03;
            *(pdst0 + 3) = ctemp04;

            psrc1 += 2 * lda;
            psrc2 += 2 * lda;
            pdst0 += 4;
        }

        if (m & 1)
        {
            ctemp01 = *(psrc1 + 0);
            ctemp02 = *(psrc1 + 1);

            *(pdst0 + 0) = ctemp01;
            *(pdst0 + 1) = ctemp02;
            pdst0 += 2;
        }
    }

    return 0;
}
