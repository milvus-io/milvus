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


static FLOAT zasum_kernel_16(BLASLONG n, FLOAT *x) {
    
    FLOAT asum;
    __asm__ (
            "pfd    1, 0(%[ptr_x]) \n\t"
            "sllg   %%r0,%[n],4    \n\t"
            "agr    %%r0,%[ptr_x]  \n\t"   
            "vzero  %%v0      \n\t"
            "vzero  %%v1      \n\t"
            "vzero  %%v22     \n\t"
            "vzero  %%v23     \n\t"   
            ".align 16 \n\t"
            "1:     \n\t"
            "pfd    1, 256(%[ptr_tmp] ) \n\t"
            "vlm    %%v24,%%v31,0(%[ptr_tmp]) \n\t"  
    
            "vflpdb %%v24, %%v24 \n\t"
            "vflpdb %%v25, %%v25 \n\t"
            "vflpdb %%v26, %%v26 \n\t"
            "vflpdb %%v27, %%v27 \n\t"
            "vflpdb %%v28, %%v28 \n\t"
            "vflpdb %%v29, %%v29 \n\t"
            "vflpdb %%v30, %%v30 \n\t"
            "vflpdb %%v31, %%v31 \n\t"
    
            "vfadb  %%v0,%%v0,%%v24    \n\t"
            "vfadb  %%v1,%%v1,%%v25    \n\t"
            "vfadb  %%v23,%%v23,%%v26  \n\t"
            "vfadb  %%v22,%%v22,%%v27  \n\t" 
            "vfadb  %%v0,%%v0,%%v28    \n\t"
            "vfadb  %%v1,%%v1,%%v29    \n\t"
            "vfadb  %%v23,%%v23,%%v30  \n\t"
            "vfadb  %%v22,%%v22,%%v31  \n\t" 
    
            "vlm    %%v24,%%v31, 128(%[ptr_tmp]) \n\t"  
    
            "vflpdb %%v24, %%v24 \n\t"
            "vflpdb %%v25, %%v25 \n\t"
            "vflpdb %%v26, %%v26 \n\t"
            "vflpdb %%v27, %%v27 \n\t"
            "vflpdb %%v28, %%v28 \n\t"
            "vflpdb %%v29, %%v29 \n\t"
            "vflpdb %%v30, %%v30 \n\t"
            "vflpdb %%v31, %%v31 \n\t"
            "la     %[ptr_tmp],256(%[ptr_tmp]) \n\t"  
            "vfadb  %%v0,%%v0,%%v24   \n\t"
            "vfadb  %%v1,%%v1,%%v25   \n\t"
            "vfadb  %%v23,%%v23,%%v26 \n\t"
            "vfadb  %%v22,%%v22,%%v27 \n\t" 
            "vfadb  %%v0,%%v0,%%v28   \n\t"
            "vfadb  %%v1,%%v1,%%v29   \n\t"
            "vfadb  %%v23,%%v23,%%v30 \n\t"
            "vfadb  %%v22,%%v22,%%v31 \n\t"  
            
            "clgrjl %[ptr_tmp],%%r0,1b \n\t"
            "vfadb  %%v24,%%v0,%%v1    \n\t"
            "vfadb  %%v25,%%v23,%%v22  \n\t"
            "vfadb  %%v0,%%v25,%%v24   \n\t"
            "vrepg  %%v1,%%v0,1        \n\t"
            "adbr   %%f0,%%f1          \n\t"
            "ldr    %[asum] ,%%f0"
            : [asum] "=f"(asum),[ptr_tmp] "+&a"(x)
            : [mem] "m"( *(const double (*)[2*n])x ), [n] "r"(n), [ptr_x] "a"(x) 
            : "cc",  "r0","f0","f1","v0","v1","v22","v23","v24","v25","v26","v27","v28","v29","v30","v31"
            );
    return asum;

}

 

FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x)
{
    BLASLONG i=0;
    BLASLONG ip=0;
    FLOAT sumf = 0.0; 
    BLASLONG n1;
    BLASLONG inc_x2;

    if (n <= 0 || inc_x <= 0) return(sumf);

    if ( inc_x == 1 )
    {

        n1 = n & -16;
        if ( n1 > 0 )
        {

            sumf=zasum_kernel_16(n1, x ); 
            i=n1;
            ip=2*n1;
        }

        while(i < n)
        {
            sumf += ABS(x[ip]) + ABS(x[ip+1]);
            i++;
            ip+=2;
        }

    }
    else
    {
        inc_x2 = 2* inc_x;

        while(i < n)
        {
            sumf += ABS(x[ip]) + ABS(x[ip+1]);
            ip+=inc_x2;
            i++;
        }

    }
    return(sumf);
}


