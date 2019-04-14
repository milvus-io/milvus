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
#include <math.h>

#if defined(DOUBLE)

#define ABS fabs

#else

#define ABS fabsf

#endif

/**
 * Find  minimum index 
 * Warning: requirements n>0  and n % 32 == 0
 * @param n     
 * @param x     pointer to the vector
 * @param minf  (out) minimum absolute value .( only for output )
 * @return minimum index 
 */
static BLASLONG diamin_kernel_32(BLASLONG n, FLOAT *x, FLOAT *minf) {
     BLASLONG index;
    __asm__( 
            "pfd    1, 0(%[ptr_x]) \n\t"
            "sllg   %%r0,%[n],3    \n\t" 
            "agr    %%r0,%[ptr_x]  \n\t"
            "vleig  %%v20,0,0  \n\t"
            "vleig  %%v20,1,1  \n\t"
            "vleig  %%v21,2,0  \n\t"
            "vleig  %%v21,3,1  \n\t"
            "vleig  %%v22,4,0  \n\t"
            "vleig  %%v22,5,1  \n\t"
            "vleig  %%v23,6,0  \n\t"
            "vleig  %%v23,7,1  \n\t"
            "vrepig %%v4,8     \n\t"
            "vlrepg %%v18,0(%[ptr_x])   \n\t"
            "vzero  %%v5        \n\t" 
            "vflpdb %%v18, %%v18 \n\t"
            "vzero  %%v19          \n\t"
            ".align 16 \n\t"
            "1: \n\t"
            "pfd     1, 256(%[ptr_tmp] ) \n\t"
            "vlm     %%v24,%%v31, 0(%[ptr_tmp] ) \n\t"

            "vflpdb  %%v24, %%v24 \n\t"
            "vflpdb  %%v25, %%v25 \n\t"
            "vflpdb  %%v26, %%v26 \n\t"
            "vflpdb  %%v27, %%v27 \n\t"
            "vflpdb  %%v28, %%v28 \n\t"
            "vflpdb  %%v29, %%v29 \n\t"
            "vflpdb  %%v30, %%v30 \n\t"
            "vflpdb  %%v31, %%v31 \n\t"

            "vfchdb  %%v16,%%v24,%%v25  \n\t "
            "vfchdb  %%v17,%%v26 ,%%v27 \n\t "
            "vsel    %%v1,%%v21,%%v20,%%v16 \n\t"
            "vsel    %%v0,%%v25,%%v24,%%v16 \n\t"
            "vsel    %%v2,%%v23,%%v22,%%v17 \n\t"
            "vsel    %%v3,%%v27,%%v26,%%v17 \n\t"
            "vfchdb  %%v16,%%v28, %%v29 \n\t "
            "vfchdb  %%v17,%%v30,%%v31  \n\t"
            "vsel    %%v24,%%v21,%%v20,%%v16 \n\t"
            "vsel    %%v25,%%v29,%%v28,%%v16 \n\t"
            "vsel    %%v26,%%v23,%%v22,%%v17 \n\t"
            "vsel    %%v27,%%v31,%%v30,%%v17 \n\t"


            "vfchdb  %%v28,%%v0 , %%v3       \n\t"
            "vfchdb  %%v29, %%v25,%%v27      \n\t"
            "vsel    %%v1,%%v2,%%v1,%%v28    \n\t"
            "vsel    %%v0,%%v3,%%v0,%%v28    \n\t"
            "vsel    %%v24,%%v26,%%v24,%%v29 \n\t"
            "vsel    %%v25,%%v27,%%v25,%%v29 \n\t"

            "vag     %%v1,%%v1,%%v5   \n\t"
            "vag     %%v24,%%v24,%%v5   \n\t"
            "vag     %%v24,%%v24,%%v4   \n\t"

            "vfchdb  %%v16, %%v0,%%v25      \n\t"
            "vag     %%v5,%%v5,%%v4         \n\t"
            "vsel    %%v29,%%v25,%%v0,%%v16 \n\t"
            "vsel    %%v28,%%v24,%%v1,%%v16 \n\t"

            "vfchdb  %%v17,%%v18, %%v29      \n\t"
            "vsel    %%v19,%%v28,%%v19,%%v17 \n\t"
            "vsel    %%v18,%%v29,%%v18,%%v17 \n\t"

            "vag     %%v5,%%v5,%%v4 \n\t"

            "vlm     %%v24,%%v31,128(%[ptr_tmp] ) \n\t"
            "vflpdb  %%v24, %%v24 \n\t"
            "vflpdb  %%v25, %%v25 \n\t"
            "vflpdb  %%v26, %%v26 \n\t"
            "vflpdb  %%v27, %%v27 \n\t"
            "vflpdb  %%v28, %%v28 \n\t"
            "vflpdb  %%v29, %%v29 \n\t"
            "vflpdb  %%v30, %%v30 \n\t"
            "vflpdb  %%v31, %%v31 \n\t"

            "vfchdb  %%v16,%%v24,%%v25  \n\t"
            "vfchdb  %%v17,%%v26 ,%%v27 \n\t"
            "vsel    %%v1,%%v21,%%v20,%%v16 \n\t"
            "vsel    %%v0,%%v25,%%v24,%%v16 \n\t"
            "vsel    %%v2,%%v23,%%v22,%%v17 \n\t"
            "vsel    %%v3,%%v27,%%v26,%%v17 \n\t"
            "vfchdb  %%v16,%%v28 ,%%v29 \n\t"
            "vfchdb  %%v17,%%v30,%%v31  \n\t"
            "vsel    %%v24,%%v21,%%v20,%%v16 \n\t"
            "vsel    %%v25,%%v29,%%v28,%%v16 \n\t"
            "vsel    %%v26,%%v23,%%v22,%%v17 \n\t"
            "vsel    %%v27,%%v31,%%v30,%%v17 \n\t"


            "vfchdb  %%v28,%%v0 , %%v3       \n\t"
            "vfchdb  %%v29, %%v25,%%v27      \n\t"
            "vsel    %%v1,%%v2,%%v1,%%v28    \n\t"
            "vsel    %%v0,%%v3,%%v0,%%v28    \n\t"
            "vsel    %%v24,%%v26,%%v24,%%v29 \n\t"
            "vsel    %%v25,%%v27,%%v25,%%v29 \n\t"

            "vag     %%v1,%%v1,%%v5     \n\t"
            "vag     %%v24,%%v24,%%v5   \n\t"
            "la      %[ptr_tmp],256(%[ptr_tmp])   \n\t"
            "vag     %%v24,%%v24,%%v4   \n\t"

            "vfchdb  %%v16, %%v0,%%v25      \n\t"
            "vag     %%v5,%%v5,%%v4         \n\t"
            "vsel    %%v29,%%v25,%%v0,%%v16 \n\t"
            "vsel    %%v28,%%v24,%%v1,%%v16 \n\t"

            "vfchdb  %%v17,%%v18, %%v29      \n\t"
            "vsel    %%v19,%%v28,%%v19,%%v17 \n\t"
            "vsel    %%v18,%%v29,%%v18,%%v17 \n\t"

            "vag     %%v5,%%v5,%%v4 \n\t"

            "clgrjl  %[ptr_tmp],%%r0,1b \n\t"


            "vrepg   %%v26,%%v18,1   \n\t"
            "vrepg   %%v5,%%v19,1    \n\t"
            "wfcdb   %%v26,%%v18     \n\t"
            "jne 2f  \n\t"
            "vsteg   %%v18,%[minf],0  \n\t"
            "vmnlg   %%v1,%%v5,%%v19  \n\t"
            "j 3f    \n\t"
            
            "2:      \n\t"
            "wfchdb  %%v16,%%v18 ,%%v26     \n\t "
            "vsel    %%v1,%%v5,%%v19,%%v16  \n\t"
            "vsel    %%v0,%%v26,%%v18,%%v16 \n\t"
            "std     %%f0,%[minf]     \n\t"            

            "3:   \n\t"
            "vlgvg   %[index],%%v1,0  \n\t" 

            : [index] "+r"(index) ,[minf] "=m"(*minf), [ptr_tmp] "+&a"(x)
            : [mem] "m"( *(const double (*)[n])x), [n] "r"(n), [ptr_x] "r"(x) 
            : "cc","r0", "f0","v0","v1","v2","v3","v4","v5","v6","v7","v16",
            "v17","v18","v19","v20","v21","v22","v23","v24","v25","v26","v27","v28","v29","v30","v31"

            );
    
    return index;

}



BLASLONG CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x) {
    BLASLONG i = 0;
    BLASLONG j = 0;
    BLASLONG ix = 0;
    BLASLONG min = 0;
    FLOAT minf = 0.0;
    
    if (n <= 0 || inc_x <= 0) return (min);
    minf = ABS(x[0]); //index's not incremented,though it will make first comparision redundant
    if (inc_x == 1) {

        BLASLONG n1 = n & -32;
        if (n1 > 0) {

            min = diamin_kernel_32(n1, x, &minf);
            i = n1;
        }

        while (i < n) {
            if (ABS(x[i]) < minf) {
                min = i;
                minf = ABS(x[i]);
            }
            i++;
        }
        return (min + 1);

    } else {

        BLASLONG n1 = n & -4;
        while (j < n1) {

            if (ABS(x[i]) < minf) {
                min = j;
                minf = ABS(x[i]);
            }
            if (ABS(x[i + inc_x]) < minf) {
                min = j + 1;
                minf = ABS(x[i + inc_x]);
            }
            if (ABS(x[i + 2 * inc_x]) < minf) {
                min = j + 2;
                minf = ABS(x[i + 2 * inc_x]);
            }
            if (ABS(x[i + 3 * inc_x]) < minf) {
                min = j + 3;
                minf = ABS(x[i + 3 * inc_x]);
            }

            i += inc_x * 4;

            j += 4;

        }


        while (j < n) {
            if (ABS(x[i]) < minf) {
                min = j;
                minf = ABS(x[i]);
            }
            i += inc_x;
            j++;
        }
        return (min + 1);
    }
}
