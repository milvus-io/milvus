/*****************************************************************************
Copyright (c) 2011-2014, The OpenBLAS Project
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
      derived from this software without specific prior written 
      permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE
USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

 **********************************************************************************/

#include <stdio.h>
#include "common.h"

int CNAME(BLASLONG row,BLASLONG col,FLOAT* src,BLASLONG srcdim,FLOAT* dest)
{
   BLASLONG i,j;
   BLASLONG idx=0;
   BLASLONG ii;
   FLOAT *src0,*src1,*src2,*src3,*src4,*src5,*src6,*src7,*dest0;
   for (j=0; j<col/8; j+=1)
     {
        src0 = src;
        src1 = src0+2*srcdim;
        src2 = src1+2*srcdim;
        src3 = src2+2*srcdim;
        src4 = src3+2*srcdim;
        src5 = src4+2*srcdim;
        src6 = src5+2*srcdim;
        src7 = src6+2*srcdim;
        src = src7+2*srcdim;
        dest0 = dest;
        ii = (row<<4);
        dest = dest+ii;
        for (i=0; i<row/4; i+=1)
          {
             dest0[0] = src0[0];
             dest0[1] = src0[1];
             dest0[2] = src1[0];
             dest0[3] = src1[1];
             dest0[4] = src2[0];
             dest0[5] = src2[1];
             dest0[6] = src3[0];
             dest0[7] = src3[1];
             dest0[8] = src4[0];
             dest0[9] = src4[1];
             dest0[10] = src5[0];
             dest0[11] = src5[1];
             dest0[12] = src6[0];
             dest0[13] = src6[1];
             dest0[14] = src7[0];
             dest0[15] = src7[1];
             dest0[16] = src0[2];
             dest0[17] = src0[3];
             dest0[18] = src1[2];
             dest0[19] = src1[3];
             dest0[20] = src2[2];
             dest0[21] = src2[3];
             dest0[22] = src3[2];
             dest0[23] = src3[3];
             dest0[24] = src4[2];
             dest0[25] = src4[3];
             dest0[26] = src5[2];
             dest0[27] = src5[3];
             dest0[28] = src6[2];
             dest0[29] = src6[3];
             dest0[30] = src7[2];
             dest0[31] = src7[3];
             dest0[32] = src0[4];
             dest0[33] = src0[5];
             dest0[34] = src1[4];
             dest0[35] = src1[5];
             dest0[36] = src2[4];
             dest0[37] = src2[5];
             dest0[38] = src3[4];
             dest0[39] = src3[5];
             dest0[40] = src4[4];
             dest0[41] = src4[5];
             dest0[42] = src5[4];
             dest0[43] = src5[5];
             dest0[44] = src6[4];
             dest0[45] = src6[5];
             dest0[46] = src7[4];
             dest0[47] = src7[5];
             dest0[48] = src0[6];
             dest0[49] = src0[7];
             dest0[50] = src1[6];
             dest0[51] = src1[7];
             dest0[52] = src2[6];
             dest0[53] = src2[7];
             dest0[54] = src3[6];
             dest0[55] = src3[7];
             dest0[56] = src4[6];
             dest0[57] = src4[7];
             dest0[58] = src5[6];
             dest0[59] = src5[7];
             dest0[60] = src6[6];
             dest0[61] = src6[7];
             dest0[62] = src7[6];
             dest0[63] = src7[7];
             src0 = src0+8;
             src1 = src1+8;
             src2 = src2+8;
             src3 = src3+8;
             src4 = src4+8;
             src5 = src5+8;
             src6 = src6+8;
             src7 = src7+8;
             ii = (4<<4);
             dest0 = dest0+ii;
          }
        if (row&2)
          {
             dest0[0] = src0[0];
             dest0[1] = src0[1];
             dest0[2] = src1[0];
             dest0[3] = src1[1];
             dest0[4] = src2[0];
             dest0[5] = src2[1];
             dest0[6] = src3[0];
             dest0[7] = src3[1];
             dest0[8] = src4[0];
             dest0[9] = src4[1];
             dest0[10] = src5[0];
             dest0[11] = src5[1];
             dest0[12] = src6[0];
             dest0[13] = src6[1];
             dest0[14] = src7[0];
             dest0[15] = src7[1];
             dest0[16] = src0[2];
             dest0[17] = src0[3];
             dest0[18] = src1[2];
             dest0[19] = src1[3];
             dest0[20] = src2[2];
             dest0[21] = src2[3];
             dest0[22] = src3[2];
             dest0[23] = src3[3];
             dest0[24] = src4[2];
             dest0[25] = src4[3];
             dest0[26] = src5[2];
             dest0[27] = src5[3];
             dest0[28] = src6[2];
             dest0[29] = src6[3];
             dest0[30] = src7[2];
             dest0[31] = src7[3];
             src0 = src0+4;
             src1 = src1+4;
             src2 = src2+4;
             src3 = src3+4;
             src4 = src4+4;
             src5 = src5+4;
             src6 = src6+4;
             src7 = src7+4;
             ii = (2<<4);
             dest0 = dest0+ii;
          }
        if (row&1)
          {
             dest0[0] = src0[0];
             dest0[1] = src0[1];
             dest0[2] = src1[0];
             dest0[3] = src1[1];
             dest0[4] = src2[0];
             dest0[5] = src2[1];
             dest0[6] = src3[0];
             dest0[7] = src3[1];
             dest0[8] = src4[0];
             dest0[9] = src4[1];
             dest0[10] = src5[0];
             dest0[11] = src5[1];
             dest0[12] = src6[0];
             dest0[13] = src6[1];
             dest0[14] = src7[0];
             dest0[15] = src7[1];
             src0 = src0+2;
             src1 = src1+2;
             src2 = src2+2;
             src3 = src3+2;
             src4 = src4+2;
             src5 = src5+2;
             src6 = src6+2;
             src7 = src7+2;
             ii = (1<<4);
             dest0 = dest0+ii;
          }
     }
   if (col&4)
     {
        src0 = src;
        src1 = src0+2*srcdim;
        src2 = src1+2*srcdim;
        src3 = src2+2*srcdim;
        src = src3+2*srcdim;
        dest0 = dest;
        ii = (row<<3);
        dest = dest+ii;
        for (i=0; i<row/4; i+=1)
          {
             dest0[0] = src0[0];
             dest0[1] = src0[1];
             dest0[2] = src1[0];
             dest0[3] = src1[1];
             dest0[4] = src2[0];
             dest0[5] = src2[1];
             dest0[6] = src3[0];
             dest0[7] = src3[1];
             dest0[8] = src0[2];
             dest0[9] = src0[3];
             dest0[10] = src1[2];
             dest0[11] = src1[3];
             dest0[12] = src2[2];
             dest0[13] = src2[3];
             dest0[14] = src3[2];
             dest0[15] = src3[3];
             dest0[16] = src0[4];
             dest0[17] = src0[5];
             dest0[18] = src1[4];
             dest0[19] = src1[5];
             dest0[20] = src2[4];
             dest0[21] = src2[5];
             dest0[22] = src3[4];
             dest0[23] = src3[5];
             dest0[24] = src0[6];
             dest0[25] = src0[7];
             dest0[26] = src1[6];
             dest0[27] = src1[7];
             dest0[28] = src2[6];
             dest0[29] = src2[7];
             dest0[30] = src3[6];
             dest0[31] = src3[7];
             src0 = src0+8;
             src1 = src1+8;
             src2 = src2+8;
             src3 = src3+8;
             ii = (4<<3);
             dest0 = dest0+ii;
          }
        if (row&2)
          {
             dest0[0] = src0[0];
             dest0[1] = src0[1];
             dest0[2] = src1[0];
             dest0[3] = src1[1];
             dest0[4] = src2[0];
             dest0[5] = src2[1];
             dest0[6] = src3[0];
             dest0[7] = src3[1];
             dest0[8] = src0[2];
             dest0[9] = src0[3];
             dest0[10] = src1[2];
             dest0[11] = src1[3];
             dest0[12] = src2[2];
             dest0[13] = src2[3];
             dest0[14] = src3[2];
             dest0[15] = src3[3];
             src0 = src0+4;
             src1 = src1+4;
             src2 = src2+4;
             src3 = src3+4;
             ii = (2<<3);
             dest0 = dest0+ii;
          }
        if (row&1)
          {
             dest0[0] = src0[0];
             dest0[1] = src0[1];
             dest0[2] = src1[0];
             dest0[3] = src1[1];
             dest0[4] = src2[0];
             dest0[5] = src2[1];
             dest0[6] = src3[0];
             dest0[7] = src3[1];
             src0 = src0+2;
             src1 = src1+2;
             src2 = src2+2;
             src3 = src3+2;
             ii = (1<<3);
             dest0 = dest0+ii;
          }
     }
   if (col&2)
     {
        src0 = src;
        src1 = src0+2*srcdim;
        src = src1+2*srcdim;
        dest0 = dest;
        ii = (row<<2);
        dest = dest+ii;
        for (i=0; i<row/4; i+=1)
          {
             dest0[0] = src0[0];
             dest0[1] = src0[1];
             dest0[2] = src1[0];
             dest0[3] = src1[1];
             dest0[4] = src0[2];
             dest0[5] = src0[3];
             dest0[6] = src1[2];
             dest0[7] = src1[3];
             dest0[8] = src0[4];
             dest0[9] = src0[5];
             dest0[10] = src1[4];
             dest0[11] = src1[5];
             dest0[12] = src0[6];
             dest0[13] = src0[7];
             dest0[14] = src1[6];
             dest0[15] = src1[7];
             src0 = src0+8;
             src1 = src1+8;
             ii = (4<<2);
             dest0 = dest0+ii;
          }
        if (row&2)
          {
             dest0[0] = src0[0];
             dest0[1] = src0[1];
             dest0[2] = src1[0];
             dest0[3] = src1[1];
             dest0[4] = src0[2];
             dest0[5] = src0[3];
             dest0[6] = src1[2];
             dest0[7] = src1[3];
             src0 = src0+4;
             src1 = src1+4;
             ii = (2<<2);
             dest0 = dest0+ii;
          }
        if (row&1)
          {
             dest0[0] = src0[0];
             dest0[1] = src0[1];
             dest0[2] = src1[0];
             dest0[3] = src1[1];
             src0 = src0+2;
             src1 = src1+2;
             ii = (1<<2);
             dest0 = dest0+ii;
          }
     }
   if (col&1)
     {
        src0 = src;
        src = src0+2*srcdim;
        dest0 = dest;
        ii = (row<<1);
        dest = dest+ii;
        for (i=0; i<row/4; i+=1)
          {
             dest0[0] = src0[0];
             dest0[1] = src0[1];
             dest0[2] = src0[2];
             dest0[3] = src0[3];
             dest0[4] = src0[4];
             dest0[5] = src0[5];
             dest0[6] = src0[6];
             dest0[7] = src0[7];
             src0 = src0+8;
             ii = (4<<1);
             dest0 = dest0+ii;
          }
        if (row&2)
          {
             dest0[0] = src0[0];
             dest0[1] = src0[1];
             dest0[2] = src0[2];
             dest0[3] = src0[3];
             src0 = src0+4;
             ii = (2<<1);
             dest0 = dest0+ii;
          }
        if (row&1)
          {
             dest0[0] = src0[0];
             dest0[1] = src0[1];
             src0 = src0+2;
             ii = (1<<1);
             dest0 = dest0+ii;
          }
     }
   return 0;
}
