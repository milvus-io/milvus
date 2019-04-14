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


 

static   FLOAT  dasum_kernel_32(BLASLONG n, FLOAT *x) {
    FLOAT asum    ; 
    __asm__  (
            "pfd     1, 0(%[ptr_x])   \n\t"
            "sllg    %%r0,%[n],3  \n\t"
            "agr     %%r0,%[ptr_x]    \n\t"   
            "vzero   %%v0       \n\t"
            "vzero   %%v1       \n\t"
            "vzero   %%v2       \n\t"
            "vzero   %%v3       \n\t"   
            ".align 16 \n\t"
            "1:      \n\t"
            "pfd     1, 256(%[ptr_temp] ) \n\t"
            "vlm     %%v24,%%v31, 0(%[ptr_temp] ) \n\t"  
    
            "vflpdb  %%v24, %%v24 \n\t"
            "vflpdb  %%v25, %%v25 \n\t"
            "vflpdb  %%v26, %%v26 \n\t"
            "vflpdb  %%v27, %%v27 \n\t"
            "vflpdb  %%v28, %%v28 \n\t"
            "vflpdb  %%v29, %%v29 \n\t"
            "vflpdb  %%v30, %%v30 \n\t"
            "vflpdb  %%v31, %%v31 \n\t"
    
            "vfadb   %%v0,%%v0,%%v24    \n\t"
            "vfadb   %%v1,%%v1,%%v25    \n\t"
            "vfadb   %%v2,%%v2,%%v26    \n\t"
            "vfadb   %%v3,%%v3,%%v27    \n\t" 
            "vfadb   %%v0,%%v0,%%v28    \n\t"
            "vfadb   %%v1,%%v1,%%v29    \n\t"
            "vfadb   %%v2,%%v2,%%v30    \n\t"
            "vfadb   %%v3,%%v3,%%v31    \n\t" 
    
            "vlm     %%v24,%%v31, 128(%[ptr_temp]) \n\t"  
    
            "vflpdb  %%v24, %%v24       \n\t"
            "vflpdb  %%v25, %%v25       \n\t"
            "vflpdb  %%v26, %%v26       \n\t"
            "vflpdb  %%v27, %%v27       \n\t"
            "vflpdb  %%v28, %%v28       \n\t"
            "vflpdb  %%v29, %%v29       \n\t"
            "vflpdb  %%v30, %%v30       \n\t"
            "vflpdb  %%v31, %%v31       \n\t"
            "la      %[ptr_temp],256(%[ptr_temp])  \n\t"  
            "vfadb   %%v0,%%v0,%%v24    \n\t"
            "vfadb   %%v1,%%v1,%%v25    \n\t"
            "vfadb   %%v2,%%v2,%%v26    \n\t"
            "vfadb   %%v3,%%v3,%%v27    \n\t" 
            "vfadb   %%v0,%%v0,%%v28    \n\t"
            "vfadb   %%v1,%%v1,%%v29    \n\t"
            "vfadb   %%v2,%%v2,%%v30    \n\t"
            "vfadb   %%v3,%%v3,%%v31    \n\t"  
            
            "clgrjl  %[ptr_temp],%%r0,1b           \n\t"
            "vfadb   %%v24,%%v0,%%v1    \n\t"
            "vfadb   %%v25,%%v2,%%v3    \n\t"
            "vfadb   %%v0,%%v25,%%v24   \n\t"
            "vrepg   %%v1,%%v0,1        \n\t"
            "adbr    %%f0,%%f1          \n\t"
            "ldr     %[asum],%%f0       \n\t"
            : [asum] "=f"(asum),[ptr_temp] "+&a"(x)
            : [mem] "m"( *(const double (*)[n])x ), [n] "r"(n), [ptr_x] "a"(x)
            : "cc", "r0" ,"f0","f1","v0","v1","v2","v3","v24","v25","v26","v27","v28","v29","v30","v31"
            );
      return asum;

}




FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x) {
    BLASLONG i = 0;
    BLASLONG j = 0;
    FLOAT sumf = 0.0;
    BLASLONG n1;

    if (n <= 0 || inc_x <= 0) return sumf;

    if (inc_x == 1) {

        n1 = n & -32;
               
        if (n1 > 0) {

            sumf = dasum_kernel_32(n1, x);
            i = n1;
        }

        while (i < n) {
            sumf += ABS(x[i]);
            i++;
        }

    } else {
        BLASLONG n1 = n & -4;
        register FLOAT sum1, sum2;
        sum1 = 0.0;
        sum2 = 0.0;
        while (j < n1) {

            sum1 += ABS(x[i]);
            sum2 += ABS(x[i + inc_x]);
            sum1 += ABS(x[i + 2 * inc_x]);
            sum2 += ABS(x[i + 3 * inc_x]);

            i += inc_x * 4;
            j += 4;

        }
        sumf = sum1 + sum2;
        while (j < n) {

            sumf += ABS(x[i]);
            i += inc_x;
            j++;
        }


    }
    return sumf;
}


