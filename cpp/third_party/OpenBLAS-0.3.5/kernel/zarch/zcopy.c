/***************************************************************************
Copyright (c) 2013-2017, The OpenBLAS Project
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
*****************************************************************************/
 
#include "common.h"
 
static void  zcopy_kernel_16(BLASLONG n, FLOAT *x, FLOAT *y) {

    __asm__ volatile(
            "pfd   1, 0(%[ptr_x]) \n\t"
            "pfd   2, 0(%[ptr_y]) \n\t"
            "srlg  %[n_tmp],%[n_tmp],4      \n\t"
            "xgr   %%r1,%%r1       \n\t"
            ".align 16 \n\t"
            "1:    \n\t"
            "pfd   1, 256(%%r1,%[ptr_x]) \n\t"
            "pfd   2, 256(%%r1,%[ptr_y]) \n\t"

            "vl    %%v24, 0(%%r1,%[ptr_x])   \n\t"
            "vst   %%v24, 0(%%r1,%[ptr_y])   \n\t"
            "vl    %%v25, 16(%%r1,%[ptr_x])  \n\t"
            "vst   %%v25, 16(%%r1,%[ptr_y])  \n\t"
            "vl    %%v26, 32(%%r1,%[ptr_x])  \n\t"
            "vst   %%v26, 32(%%r1,%[ptr_y])  \n\t"
            "vl    %%v27, 48(%%r1,%[ptr_x])  \n\t"
            "vst   %%v27, 48(%%r1,%[ptr_y])  \n\t"

            "vl    %%v28, 64(%%r1,%[ptr_x])  \n\t"
            "vst   %%v28, 64(%%r1,%[ptr_y])  \n\t"
            "vl    %%v29, 80(%%r1,%[ptr_x])  \n\t"
            "vst   %%v29, 80(%%r1,%[ptr_y])  \n\t"
            "vl    %%v30, 96(%%r1,%[ptr_x])  \n\t"
            "vst   %%v30, 96(%%r1,%[ptr_y])  \n\t"
            "vl    %%v31, 112(%%r1,%[ptr_x]) \n\t"
            "vst   %%v31, 112(%%r1,%[ptr_y]) \n\t"


            "vl    %%v24, 128(%%r1,%[ptr_x]) \n\t"
            "vst   %%v24, 128(%%r1,%[ptr_y]) \n\t"

            "vl    %%v25, 144(%%r1,%[ptr_x]) \n\t"
            "vst   %%v25, 144(%%r1,%[ptr_y]) \n\t"

            "vl    %%v26, 160(%%r1,%[ptr_x]) \n\t"
            "vst   %%v26, 160(%%r1,%[ptr_y]) \n\t"

            "vl    %%v27, 176(%%r1,%[ptr_x]) \n\t"
            "vst   %%v27, 176(%%r1,%[ptr_y]) \n\t"

            "vl    %%v28, 192(%%r1,%[ptr_x]) \n\t"
            "vst   %%v28, 192(%%r1,%[ptr_y]) \n\t"
            "vl    %%v29, 208(%%r1,%[ptr_x]) \n\t"
            "vst   %%v29, 208(%%r1,%[ptr_y]) \n\t"
            "vl    %%v30, 224(%%r1,%[ptr_x]) \n\t"
            "vst   %%v30, 224(%%r1,%[ptr_y]) \n\t"
            "vl    %%v31, 240(%%r1,%[ptr_x]) \n\t"
            "vst   %%v31, 240(%%r1,%[ptr_y]) \n\t"
            "la    %%r1,256(%%r1)      \n\t"
            "brctg %[n_tmp],1b"
            : [mem_y] "=m" (*(double (*)[2*n])y), [n_tmp] "+&r"(n)
            : [mem_x] "m" (*(const double (*)[2*n])x), [ptr_x] "a"(x), [ptr_y] "a"(y)
            : "cc",  "r1", "v24","v25","v26","v27","v28","v29","v30","v31" 
            );
    return; 

}


int CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
{
    BLASLONG i=0;
    BLASLONG ix=0,iy=0;

    if ( n <= 0     )  return(0);

    if ( (inc_x == 1) && (inc_y == 1 ))
    {

        BLASLONG n1 = n & -16;
        if ( n1 > 0 )
        {
            zcopy_kernel_16(n1, x, y);
            i=n1;
            ix=n1*2;
            iy=n1*2;
        }

        while(i < n)
        {
            y[iy] = x[iy] ;
            y[iy+1] = x[ix+1] ;
            ix+=2;
            iy+=2;
            i++ ;

        }


    }
    else
    {

        BLASLONG inc_x2 = 2 * inc_x;
        BLASLONG inc_y2 = 2 * inc_y;

        while(i < n)
        {
            y[iy] = x[ix] ;
            y[iy+1] = x[ix+1] ;
            ix += inc_x2 ;
            iy += inc_y2 ;
            i++ ;

        }

    }
    return(0);
    

}


