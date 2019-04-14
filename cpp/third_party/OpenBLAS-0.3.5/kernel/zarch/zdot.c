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
#if defined(Z13)

static void zdot_kernel_8(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT *d) {

    __asm__ volatile(
            "pfd   1, 0(%[ptr_x_tmp]) \n\t"
            "pfd   1, 0(%[ptr_y_tmp]) \n\t"
            "vzero %%v24  \n\t"
            "vzero %%v25  \n\t"
            "vzero %%v26  \n\t"
            "vzero %%v27  \n\t"
            "srlg  %[n_tmp],%[n_tmp],3      \n\t"
            "xgr   %%r1,%%r1       \n\t"
            ".align 16 \n\t"
            "1:     \n\t"
            "pfd    1,    256(%%r1,%[ptr_x_tmp])  \n\t"
            "pfd    1,    256(%%r1,%[ptr_y_tmp])  \n\t"
            "vl     %%v16,  0(%%r1,%[ptr_x_tmp])  \n\t"
            "vl     %%v17, 16(%%r1,%[ptr_x_tmp])  \n\t"
            "vl     %%v18, 32(%%r1,%[ptr_x_tmp])  \n\t"
            "vl     %%v19, 48(%%r1,%[ptr_x_tmp])  \n\t"
            "vl     %%v28,  0(%%r1,%[ptr_y_tmp])  \n\t"
            "vl     %%v29, 16(%%r1,%[ptr_y_tmp])  \n\t"
            "vl     %%v30, 32(%%r1,%[ptr_y_tmp])  \n\t"
            "vl     %%v31, 48(%%r1,%[ptr_y_tmp])  \n\t"
            "vpdi   %%v20,%%v16,%%v16,4 \n\t"
            "vpdi   %%v21,%%v17,%%v17,4 \n\t"
            "vpdi   %%v22,%%v18,%%v18,4 \n\t"
            "vpdi   %%v23,%%v19,%%v19,4 \n\t"


            "vfmadb %%v24,%%v16,%%v28,%%v24  \n\t"
            "vfmadb %%v25,%%v20,%%v28,%%v25  \n\t"
            "vfmadb %%v26,%%v17,%%v29,%%v26  \n\t"
            "vfmadb %%v27,%%v21,%%v29,%%v27  \n\t"
            "vfmadb %%v24,%%v18,%%v30,%%v24  \n\t"
            "vfmadb %%v25,%%v22,%%v30,%%v25  \n\t"
            "vfmadb %%v26,%%v19,%%v31,%%v26  \n\t"
            "vfmadb %%v27,%%v23,%%v31,%%v27  \n\t"



            "vl     %%v16, 64(%%r1,%[ptr_x_tmp]) \n\t"
            "vl     %%v17, 80(%%r1,%[ptr_x_tmp]) \n\t"
            "vl     %%v18, 96(%%r1,%[ptr_x_tmp]) \n\t"
            "vl     %%v19,112(%%r1,%[ptr_x_tmp]) \n\t"
            "vl     %%v28, 64(%%r1,%[ptr_y_tmp]) \n\t"
            "vl     %%v29, 80(%%r1,%[ptr_y_tmp]) \n\t"
            "vl     %%v30, 96(%%r1,%[ptr_y_tmp]) \n\t"
            "vl     %%v31,112(%%r1,%[ptr_y_tmp]) \n\t"
            "vpdi   %%v20,%%v16,%%v16,4 \n\t"
            "vpdi   %%v21,%%v17,%%v17,4 \n\t"
            "vpdi   %%v22,%%v18,%%v18,4 \n\t"
            "vpdi   %%v23,%%v19,%%v19,4 \n\t"
            "vfmadb %%v24,%%v16,%%v28,%%v24  \n\t"
            "vfmadb %%v25,%%v20,%%v28,%%v25  \n\t"
            "vfmadb %%v26,%%v17,%%v29,%%v26  \n\t"
            "vfmadb %%v27,%%v21,%%v29,%%v27  \n\t"
            "vfmadb %%v24,%%v18,%%v30,%%v24  \n\t"
            "vfmadb %%v25,%%v22,%%v30,%%v25  \n\t"
            "vfmadb %%v26,%%v19,%%v31,%%v26  \n\t"
            "vfmadb %%v27,%%v23,%%v31,%%v27  \n\t"


            "la     %%r1,128(%%r1)       \n\t"
            "brctg  %[n_tmp],1b          \n\t"
            "vfadb  %%v24,%%v26,%%v24    \n\t"
            "vfadb  %%v25,%%v25,%%v27    \n\t"
            "vsteg  %%v24, 0(%[ptr_d]),0    \n\t"
            "vsteg  %%v24, 8(%[ptr_d]),1    \n\t"
            "vsteg  %%v25,16(%[ptr_d]),1    \n\t"
            "vsteg  %%v25,24(%[ptr_d]),0    \n\t"
            : [mem_out] "=m"(*(double (*)[4])d ) ,[n_tmp] "+&r"(n) 
            : [mem_x] "m"( *(const double (*)[2*n])x),
              [mem_y] "m"( *(const double (*)[2*n])y),
              [ptr_x_tmp] "a"(x), [ptr_y_tmp] "a"(y), [ptr_d] "a"(d)
            : "cc", "r1","v16",
            "v17","v18","v19","v20","v21","v22","v23","v24","v25","v26","v27","v28","v29","v30","v31" 
            );

}

#else

static  void zdot_kernel_8(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT *d) {
    BLASLONG register i = 0;
    FLOAT dot[4] = {0.0, 0.0, 0.0, 0.0};
    BLASLONG j = 0;

    while (i < n) {

        dot[0] += x[j] * y[j];
        dot[1] += x[j + 1] * y[j + 1];
        dot[2] += x[j] * y[j + 1];
        dot[3] += x[j + 1] * y[j];

        dot[0] += x[j + 2] * y[j + 2];
        dot[1] += x[j + 3] * y[j + 3];
        dot[2] += x[j + 2] * y[j + 3];
        dot[3] += x[j + 3] * y[j + 2];

        dot[0] += x[j + 4] * y[j + 4];
        dot[1] += x[j + 5] * y[j + 5];
        dot[2] += x[j + 4] * y[j + 5];
        dot[3] += x[j + 5] * y[j + 4];

        dot[0] += x[j + 6] * y[j + 6];
        dot[1] += x[j + 7] * y[j + 7];
        dot[2] += x[j + 6] * y[j + 7];
        dot[3] += x[j + 7] * y[j + 6];

        j += 8;
        i += 4;

    }
    d[0] = dot[0];
    d[1] = dot[1];
    d[2] = dot[2];
    d[3] = dot[3];

}

#endif

OPENBLAS_COMPLEX_FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y) {
    BLASLONG i = 0;
    BLASLONG ix=0, iy=0;
    OPENBLAS_COMPLEX_FLOAT result;
    FLOAT dot[4] __attribute__ ((aligned(16))) = {0.0, 0.0, 0.0, 0.0};

    if (n <= 0) {
        CREAL(result) = 0.0;
        CIMAG(result) = 0.0;
        return (result);

    }

    if ((inc_x == 1) && (inc_y == 1)) {

        BLASLONG n1 = n & -8;
        BLASLONG j=0; 

        if (n1){
            zdot_kernel_8(n1, x, y, dot);
            i = n1;
            j = n1 <<1;
        }
 

        while (i < n) {

            dot[0] += x[j] * y[j];
            dot[1] += x[j + 1] * y[j + 1];
            dot[2] += x[j] * y[j + 1];
            dot[3] += x[j + 1] * y[j];

            j += 2;
            i++;

        }


    } else {
        i = 0;
        ix = 0;
        iy = 0;
        inc_x <<= 1;
        inc_y <<= 1;
        while (i < n) {

            dot[0] += x[ix] * y[iy];
            dot[1] += x[ix + 1] * y[iy + 1];
            dot[2] += x[ix] * y[iy + 1];
            dot[3] += x[ix + 1] * y[iy];

            ix += inc_x;
            iy += inc_y;
            i++;

        }
    }

#if !defined(CONJ)
    CREAL(result) = dot[0] - dot[1];
    CIMAG(result) = dot[2] + dot[3];
#else
    CREAL(result) = dot[0] + dot[1];
    CIMAG(result) = dot[2] - dot[3];

#endif

    return (result);

}


