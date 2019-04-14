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
   FLOAT *src0,*src1,*src2,*src3,*dest0;
   FLOAT *dest1,*dest2;
   ii = col&-4;
   ii = ii*(2*row);
   dest2 = dest+ii;
   ii = col&-2;
   ii = ii*(2*row);
   dest1 = dest+ii;
   for (j=0; j<row/4; j+=1)
     {
        src0 = src;
        src1 = src0+2*srcdim;
        src2 = src1+2*srcdim;
        src3 = src2+2*srcdim;
        src = src3+2*srcdim;
        dest0 = dest;
        ii = (4<<3);
        dest = dest+ii;
        for (i=0; i<col/4; i+=1)
          {
             dest0[0] = src0[0];
             dest0[1] = src0[1];
             dest0[2] = src0[2];
             dest0[3] = src0[3];
             dest0[4] = src0[4];
             dest0[5] = src0[5];
             dest0[6] = src0[6];
             dest0[7] = src0[7];
             dest0[8] = src1[0];
             dest0[9] = src1[1];
             dest0[10] = src1[2];
             dest0[11] = src1[3];
             dest0[12] = src1[4];
             dest0[13] = src1[5];
             dest0[14] = src1[6];
             dest0[15] = src1[7];
             dest0[16] = src2[0];
             dest0[17] = src2[1];
             dest0[18] = src2[2];
             dest0[19] = src2[3];
             dest0[20] = src2[4];
             dest0[21] = src2[5];
             dest0[22] = src2[6];
             dest0[23] = src2[7];
             dest0[24] = src3[0];
             dest0[25] = src3[1];
             dest0[26] = src3[2];
             dest0[27] = src3[3];
             dest0[28] = src3[4];
             dest0[29] = src3[5];
             dest0[30] = src3[6];
             dest0[31] = src3[7];
             src0 = src0+8;
             src1 = src1+8;
             src2 = src2+8;
             src3 = src3+8;
             ii = (row<<3);
             dest0 = dest0+ii;
          }
        if (col&2)
          {
             dest2[0] = src0[0];
             dest2[1] = src0[1];
             dest2[2] = src0[2];
             dest2[3] = src0[3];
             dest2[4] = src1[0];
             dest2[5] = src1[1];
             dest2[6] = src1[2];
             dest2[7] = src1[3];
             dest2[8] = src2[0];
             dest2[9] = src2[1];
             dest2[10] = src2[2];
             dest2[11] = src2[3];
             dest2[12] = src3[0];
             dest2[13] = src3[1];
             dest2[14] = src3[2];
             dest2[15] = src3[3];
             src0 = src0+4;
             src1 = src1+4;
             src2 = src2+4;
             src3 = src3+4;
             dest2 = dest2+16;
          }
        if (col&1)
          {
             dest1[0] = src0[0];
             dest1[1] = src0[1];
             dest1[2] = src1[0];
             dest1[3] = src1[1];
             dest1[4] = src2[0];
             dest1[5] = src2[1];
             dest1[6] = src3[0];
             dest1[7] = src3[1];
             src0 = src0+2;
             src1 = src1+2;
             src2 = src2+2;
             src3 = src3+2;
             dest1 = dest1+8;
          }
     }
   if (row&2)
     {
        src0 = src;
        src1 = src0+2*srcdim;
        src = src1+2*srcdim;
        dest0 = dest;
        ii = (2<<3);
        dest = dest+ii;
        for (i=0; i<col/4; i+=1)
          {
             dest0[0] = src0[0];
             dest0[1] = src0[1];
             dest0[2] = src0[2];
             dest0[3] = src0[3];
             dest0[4] = src0[4];
             dest0[5] = src0[5];
             dest0[6] = src0[6];
             dest0[7] = src0[7];
             dest0[8] = src1[0];
             dest0[9] = src1[1];
             dest0[10] = src1[2];
             dest0[11] = src1[3];
             dest0[12] = src1[4];
             dest0[13] = src1[5];
             dest0[14] = src1[6];
             dest0[15] = src1[7];
             src0 = src0+8;
             src1 = src1+8;
             ii = (row<<3);
             dest0 = dest0+ii;
          }
        if (col&2)
          {
             dest2[0] = src0[0];
             dest2[1] = src0[1];
             dest2[2] = src0[2];
             dest2[3] = src0[3];
             dest2[4] = src1[0];
             dest2[5] = src1[1];
             dest2[6] = src1[2];
             dest2[7] = src1[3];
             src0 = src0+4;
             src1 = src1+4;
             dest2 = dest2+8;
          }
        if (col&1)
          {
             dest1[0] = src0[0];
             dest1[1] = src0[1];
             dest1[2] = src1[0];
             dest1[3] = src1[1];
             src0 = src0+2;
             src1 = src1+2;
             dest1 = dest1+4;
          }
     }
   if (row&1)
     {
        src0 = src;
        src = src0+2*srcdim;
        dest0 = dest;
        ii = (1<<3);
        dest = dest+ii;
        for (i=0; i<col/4; i+=1)
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
             ii = (row<<3);
             dest0 = dest0+ii;
          }
        if (col&2)
          {
             dest2[0] = src0[0];
             dest2[1] = src0[1];
             dest2[2] = src0[2];
             dest2[3] = src0[3];
             src0 = src0+4;
             dest2 = dest2+4;
          }
        if (col&1)
          {
             dest1[0] = src0[0];
             dest1[1] = src0[1];
             src0 = src0+2;
             dest1 = dest1+2;
          }
     }

   return 0;
}
