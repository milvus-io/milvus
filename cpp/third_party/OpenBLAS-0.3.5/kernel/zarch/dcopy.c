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

#if defined(Z13mvc)

static void  dcopy_kernel_32(BLASLONG n, FLOAT *x, FLOAT *y) {

    __asm__ volatile(
            "pfd   1, 0(%[ptr_x])    \n\t"
            "pfd   2, 0(%[ptr_y])    \n\t"
            "srlg  %[n_tmp],%[n_tmp],5  \n\t"
            ".align 16 \n\t"
            "1: \n\t"
            "mvc   0(256,%[ptr_y]),0(%[ptr_x]) \n\t"
            "la    %[ptr_x],256(%[ptr_x])       \n\t"
            "la    %[ptr_y],256(%[ptr_y])       \n\t"
            "brctg %[n_tmp],1b"
            : [mem_y] "=m" (*(double (*)[n])y), [n_tmp] "+&r"(n),
              [ptr_x] "+&a"(x), [ptr_y] "+&a"(y)
            : [mem_x] "m" (*(const double (*)[n])x)
            : "cc" 
            );
    return;

}
#else

static void  dcopy_kernel_32(BLASLONG n, FLOAT *x, FLOAT *y) {

    __asm__ volatile(
            "pfd   1, 0(%[ptr_x]) \n\t"
            "pfd   2, 0(%[ptr_y]) \n\t"
            "srlg  %[n_tmp],%[n_tmp],5      \n\t"
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

            "vl    %%v24, 64(%%r1,%[ptr_x])  \n\t"
            "vst   %%v24, 64(%%r1,%[ptr_y])  \n\t"
            "vl    %%v25, 80(%%r1,%[ptr_x])  \n\t"
            "vst   %%v25, 80(%%r1,%[ptr_y])  \n\t"
            "vl    %%v26, 96(%%r1,%[ptr_x])  \n\t"
            "vst   %%v26, 96(%%r1,%[ptr_y])  \n\t"
            "vl    %%v27, 112(%%r1,%[ptr_x]) \n\t"
            "vst   %%v27, 112(%%r1,%[ptr_y]) \n\t"


            "vl    %%v24, 128(%%r1,%[ptr_x]) \n\t"
            "vst   %%v24, 128(%%r1,%[ptr_y]) \n\t"

            "vl    %%v25, 144(%%r1,%[ptr_x]) \n\t"
            "vst   %%v25, 144(%%r1,%[ptr_y]) \n\t"

            "vl    %%v26, 160(%%r1,%[ptr_x]) \n\t"
            "vst   %%v26, 160(%%r1,%[ptr_y]) \n\t"

            "vl    %%v27, 176(%%r1,%[ptr_x]) \n\t"
            "vst   %%v27, 176(%%r1,%[ptr_y]) \n\t"

            "vl    %%v24, 192(%%r1,%[ptr_x]) \n\t"
            "vst   %%v24, 192(%%r1,%[ptr_y]) \n\t"
            "vl    %%v25, 208(%%r1,%[ptr_x]) \n\t"
            "vst   %%v25, 208(%%r1,%[ptr_y]) \n\t"
            "vl    %%v26, 224(%%r1,%[ptr_x]) \n\t"
            "vst   %%v26, 224(%%r1,%[ptr_y]) \n\t"
            "vl    %%v27, 240(%%r1,%[ptr_x]) \n\t"
            "vst   %%v27, 240(%%r1,%[ptr_y]) \n\t"
            "la    %%r1,256(%%r1)      \n\t"
            "brctg %[n_tmp],1b"
            : [mem_y] "=m" (*(double (*)[n])y), [n_tmp] "+&r"(n)
            : [mem_x] "m" (*(const double (*)[n])x), [ptr_x] "a"(x), [ptr_y] "a"(y)
            : "cc",  "r1", "v24","v25","v26","v27"
            );
    return;

}
#endif

int CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y) {
    BLASLONG i = 0;
    BLASLONG ix = 0, iy = 0;

    if (n <= 0) return 0;

    if ((inc_x == 1) && (inc_y == 1)) {

        BLASLONG n1 = n & -32;
        if (n1 > 0) {
            dcopy_kernel_32(n1, x, y);
            i = n1;
        }

        while (i < n) {
            y[i] = x[i];
            i++;

        }


    } else {

        BLASLONG n1 = n & -4;

        while (i < n1) {

            y[iy] = x[ix];
            y[iy + inc_y] = x[ix + inc_x];
            y[iy + 2 * inc_y] = x[ix + 2 * inc_x];
            y[iy + 3 * inc_y] = x[ix + 3 * inc_x];

            ix += inc_x * 4;
            iy += inc_y * 4;
            i += 4;

        }

        while (i < n) {

            y[iy] = x[ix];
            ix += inc_x;
            iy += inc_y;
            i++;

        }

    }
    return 0;


}


