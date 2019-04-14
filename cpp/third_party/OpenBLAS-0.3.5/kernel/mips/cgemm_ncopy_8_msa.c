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
    FLOAT ctemp01, ctemp02, ctemp03, ctemp04, ctemp05, ctemp06, ctemp07;
    FLOAT ctemp08, ctemp09, ctemp10, ctemp11, ctemp12, ctemp13, ctemp14;
    FLOAT ctemp15, ctemp16;
    v4f32 src0, src1, src2, src3, src4, src5, src6, src7;
    v4f32 src8, src9, src10, src11, src12, src13, src14, src15;
    v4f32 dst0, dst1, dst2, dst3, dst4, dst5, dst6, dst7;

    psrc0 = src;
    pdst = dst;
    lda *= 2;

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

        for (i = (m >> 2); i--;)
        {
            LD_SP2_INC(psrc1, 4, src0, src1);
            LD_SP2_INC(psrc2, 4, src2, src3);
            LD_SP2_INC(psrc3, 4, src4, src5);
            LD_SP2_INC(psrc4, 4, src6, src7);
            LD_SP2_INC(psrc5, 4, src8, src9);
            LD_SP2_INC(psrc6, 4, src10, src11);
            LD_SP2_INC(psrc7, 4, src12, src13);
            LD_SP2_INC(psrc8, 4, src14, src15);

            ILVRL_D2_SP(src2, src0, dst0, dst4);
            ILVRL_D2_SP(src6, src4, dst1, dst5);
            ILVRL_D2_SP(src10, src8, dst2, dst6);
            ILVRL_D2_SP(src14, src12, dst3, dst7);

            ST_SP8_INC(dst0, dst1, dst2, dst3, dst4, dst5, dst6, dst7, pdst, 4);

            ILVRL_D2_SP(src3, src1, dst0, dst4);
            ILVRL_D2_SP(src7, src5, dst1, dst5);
            ILVRL_D2_SP(src11, src9, dst2, dst6);
            ILVRL_D2_SP(src15, src13, dst3, dst7);

            ST_SP8_INC(dst0, dst1, dst2, dst3, dst4, dst5, dst6, dst7, pdst, 4);
        }

        if (m & 2)
        {
            src0 = LD_SP(psrc1);
            src2 = LD_SP(psrc2);
            src4 = LD_SP(psrc3);
            src6 = LD_SP(psrc4);
            src8 = LD_SP(psrc5);
            src10 = LD_SP(psrc6);
            src12 = LD_SP(psrc7);
            src14 = LD_SP(psrc8);
            psrc1 += 4;
            psrc2 += 4;
            psrc3 += 4;
            psrc4 += 4;
            psrc5 += 4;
            psrc6 += 4;
            psrc7 += 4;
            psrc8 += 4;

            ILVRL_D2_SP(src2, src0, dst0, dst4);
            ILVRL_D2_SP(src6, src4, dst1, dst5);
            ILVRL_D2_SP(src10, src8, dst2, dst6);
            ILVRL_D2_SP(src14, src12, dst3, dst7);

            ST_SP8_INC(dst0, dst1, dst2, dst3, dst4, dst5, dst6, dst7, pdst, 4);
        }

        if (m & 1)
        {
            ctemp01 = *(psrc1 + 0);
            ctemp02 = *(psrc1 + 1);
            ctemp03 = *(psrc2 + 0);
            ctemp04 = *(psrc2 + 1);
            ctemp05 = *(psrc3 + 0);
            ctemp06 = *(psrc3 + 1);
            ctemp07 = *(psrc4 + 0);
            ctemp08 = *(psrc4 + 1);
            ctemp09 = *(psrc5 + 0);
            ctemp10 = *(psrc5 + 1);
            ctemp11 = *(psrc6 + 0);
            ctemp12 = *(psrc6 + 1);
            ctemp13 = *(psrc7 + 0);
            ctemp14 = *(psrc7 + 1);
            ctemp15 = *(psrc8 + 0);
            ctemp16 = *(psrc8 + 1);
            psrc1 += 2;
            psrc2 += 2;
            psrc3 += 2;
            psrc4 += 2;
            psrc5 += 2;
            psrc6 += 2;
            psrc7 += 2;
            psrc8 += 2;

            *(pdst +  0) = ctemp01;
            *(pdst +  1) = ctemp02;
            *(pdst +  2) = ctemp03;
            *(pdst +  3) = ctemp04;
            *(pdst +  4) = ctemp05;
            *(pdst +  5) = ctemp06;
            *(pdst +  6) = ctemp07;
            *(pdst +  7) = ctemp08;
            *(pdst +  8) = ctemp09;
            *(pdst +  9) = ctemp10;
            *(pdst + 10) = ctemp11;
            *(pdst + 11) = ctemp12;
            *(pdst + 12) = ctemp13;
            *(pdst + 13) = ctemp14;
            *(pdst + 14) = ctemp15;
            *(pdst + 15) = ctemp16;
            pdst += 16;
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
            LD_SP2_INC(psrc1, 4, src0, src1);
            LD_SP2_INC(psrc2, 4, src2, src3);
            LD_SP2_INC(psrc3, 4, src4, src5);
            LD_SP2_INC(psrc4, 4, src6, src7);

            ILVRL_D2_SP(src2, src0, dst0, dst4);
            ILVRL_D2_SP(src6, src4, dst1, dst5);

            ST_SP4_INC(dst0, dst1, dst4, dst5, pdst, 4);

            ILVRL_D2_SP(src3, src1, dst0, dst4);
            ILVRL_D2_SP(src7, src5, dst1, dst5);

            ST_SP4_INC(dst0, dst1, dst4, dst5, pdst, 4);
        }

        if (m & 2)
        {
            src0 = LD_SP(psrc1);
            src2 = LD_SP(psrc2);
            src4 = LD_SP(psrc3);
            src6 = LD_SP(psrc4);
            psrc1 += 4;
            psrc2 += 4;
            psrc3 += 4;
            psrc4 += 4;

            ILVRL_D2_SP(src2, src0, dst0, dst4);
            ILVRL_D2_SP(src6, src4, dst1, dst5);

            ST_SP4_INC(dst0, dst1, dst4, dst5, pdst, 4);
        }

        if (m & 1)
        {
            ctemp01 = *(psrc1 + 0);
            ctemp02 = *(psrc1 + 1);
            ctemp03 = *(psrc2 + 0);
            ctemp04 = *(psrc2 + 1);
            ctemp05 = *(psrc3 + 0);
            ctemp06 = *(psrc3 + 1);
            ctemp07 = *(psrc4 + 0);
            ctemp08 = *(psrc4 + 1);
            psrc1 += 2;
            psrc2 += 2;
            psrc3 += 2;
            psrc4 += 2;

            *(pdst + 0) = ctemp01;
            *(pdst + 1) = ctemp02;
            *(pdst + 2) = ctemp03;
            *(pdst + 3) = ctemp04;
            *(pdst + 4) = ctemp05;
            *(pdst + 5) = ctemp06;
            *(pdst + 6) = ctemp07;
            *(pdst + 7) = ctemp08;
            pdst += 8;
        }
    }

    if (n & 2)
    {
        psrc1 = psrc0;
        psrc2 = psrc1 + lda;
        psrc0 += 2 * lda;

        for (i = (m >> 2); i--;)
        {
            LD_SP2_INC(psrc1, 4, src0, src1);
            LD_SP2_INC(psrc2, 4, src2, src3);

            ILVRL_D2_SP(src2, src0, dst0, dst4);

            ST_SP2_INC(dst0, dst4, pdst, 4);

            ILVRL_D2_SP(src3, src1, dst0, dst4);

            ST_SP2_INC(dst0, dst4, pdst, 4);
        }

        if (m & 2)
        {
            src0 = LD_SP(psrc1);
            src2 = LD_SP(psrc2);
            psrc1 += 4;
            psrc2 += 4;

            ILVRL_D2_SP(src2, src0, dst0, dst4);

            ST_SP2_INC(dst0, dst4, pdst, 4);
        }

        if (m & 1)
        {
            ctemp01 = *(psrc1 + 0);
            ctemp02 = *(psrc1 + 1);
            ctemp03 = *(psrc2 + 0);
            ctemp04 = *(psrc2 + 1);
            psrc1 += 2;
            psrc2 += 2;

            *(pdst + 0) = ctemp01;
            *(pdst + 1) = ctemp02;
            *(pdst + 2) = ctemp03;
            *(pdst + 3) = ctemp04;
            pdst  += 4;
        }
    }

    if (n & 1)
    {
        psrc1 = psrc0;

        for (i = (m >> 2); i--;)
        {
            LD_SP2_INC(psrc1, 4, src0, src1);
            ST_SP2_INC(src0, src1, pdst, 4);
        }

        if (m & 2)
        {
            src0 = LD_SP(psrc1);
            psrc1 += 4;

            ST_SP(src0, pdst);
            pdst += 4;
        }

        if (m & 1)
        {
            ctemp01 = *(psrc1 + 0);
            ctemp02 = *(psrc1 + 1);
            psrc1 += 2;

            *(pdst + 0) = ctemp01;
            *(pdst + 1) = ctemp02;
            pdst += 2;
        }
    }

    return 0;
}
