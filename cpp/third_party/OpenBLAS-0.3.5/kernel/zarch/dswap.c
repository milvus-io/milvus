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



#if defined(Z13_SWAP_A)
static void   dswap_kernel_32(BLASLONG n, FLOAT *x, FLOAT *y)
{
         __asm__ volatile(
            "pfd  1, 0(%[ptr_x]) \n\t"
            "pfd  2, 0(%[ptr_y]) \n\t"
            "srlg %[n_tmp],%[n_tmp],5      \n\t"
            "xgr  %%r1,%%r1      \n\t"
            ".align 16 \n\t"
            "1:  \n\t"
            "pfd 2, 256(%%r1,%[ptr_x]) \n\t"
            "pfd 2, 256(%%r1,%[ptr_y]) \n\t"
            
            "vl  %%v24, 0(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v16, 0(%%r1,%[ptr_y]) \n\t" 
            "vst %%v24, 0(%%r1,%[ptr_y]) \n\t" 
            "vst %%v16, 0(%%r1,%[ptr_x]) \n\t" 

            "vl  %%v25, 16(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v17, 16(%%r1,%[ptr_y]) \n\t" 
            "vst %%v25, 16(%%r1,%[ptr_y]) \n\t" 
            "vst %%v17, 16(%%r1,%[ptr_x]) \n\t" 

            "vl  %%v26, 32(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v18, 32(%%r1,%[ptr_y]) \n\t" 
            "vst %%v26, 32(%%r1,%[ptr_y]) \n\t" 
            "vst %%v18, 32(%%r1,%[ptr_x]) \n\t"           

            "vl  %%v27, 48(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v19, 48(%%r1,%[ptr_y]) \n\t" 
            "vst %%v27, 48(%%r1,%[ptr_y]) \n\t" 
            "vst %%v19, 48(%%r1,%[ptr_x]) \n\t"    

            "vl  %%v28, 64(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v20, 64(%%r1,%[ptr_y]) \n\t" 
            "vst %%v28, 64(%%r1,%[ptr_y]) \n\t" 
            "vst %%v20, 64(%%r1,%[ptr_x]) \n\t"   

            "vl  %%v29, 80(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v21, 80(%%r1,%[ptr_y]) \n\t" 
            "vst %%v29, 80(%%r1,%[ptr_y]) \n\t" 
            "vst %%v21, 80(%%r1,%[ptr_x]) \n\t" 

            "vl  %%v30, 96(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v22, 96(%%r1,%[ptr_y]) \n\t" 
            "vst %%v30, 96(%%r1,%[ptr_y]) \n\t" 
            "vst %%v22, 96(%%r1,%[ptr_x]) \n\t" 

            "vl  %%v31, 112(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v23, 112(%%r1,%[ptr_y]) \n\t" 
            "vst %%v31, 112(%%r1,%[ptr_y]) \n\t" 
            "vst %%v23, 112(%%r1,%[ptr_x]) \n\t" 

            "vl  %%v24, 128(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v16, 128(%%r1,%[ptr_y]) \n\t" 
            "vst %%v24, 128(%%r1,%[ptr_y]) \n\t" 
            "vst %%v16, 128(%%r1,%[ptr_x]) \n\t" 

            "vl  %%v25, 144(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v17, 144(%%r1,%[ptr_y]) \n\t" 
            "vst %%v25, 144(%%r1,%[ptr_y]) \n\t" 
            "vst %%v17, 144(%%r1,%[ptr_x]) \n\t" 

            "vl  %%v26, 160(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v18, 160(%%r1,%[ptr_y]) \n\t" 
            "vst %%v26, 160(%%r1,%[ptr_y]) \n\t" 
            "vst %%v18, 160(%%r1,%[ptr_x]) \n\t"           

            "vl  %%v27, 176(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v19, 176(%%r1,%[ptr_y]) \n\t" 
            "vst %%v27, 176(%%r1,%[ptr_y]) \n\t" 
            "vst %%v19, 176(%%r1,%[ptr_x]) \n\t"    

            "vl  %%v28, 192(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v20, 192(%%r1,%[ptr_y]) \n\t" 
            "vst %%v28, 192(%%r1,%[ptr_y]) \n\t" 
            "vst %%v20, 192(%%r1,%[ptr_x]) \n\t"   

            "vl  %%v29, 208(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v21, 208(%%r1,%[ptr_y]) \n\t" 
            "vst %%v29, 208(%%r1,%[ptr_y]) \n\t" 
            "vst %%v21, 208(%%r1,%[ptr_x]) \n\t" 

            "vl  %%v30, 224(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v22, 224(%%r1,%[ptr_y]) \n\t" 
            "vst %%v30, 224(%%r1,%[ptr_y]) \n\t" 
            "vst %%v22, 224(%%r1,%[ptr_x]) \n\t" 

            "vl  %%v31, 240(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v23, 240(%%r1,%[ptr_y]) \n\t" 
            "vst %%v31, 240(%%r1,%[ptr_y]) \n\t" 
            "vst %%v23, 240(%%r1,%[ptr_x]) \n\t"    
          
            "la  %%r1,256(%%r1) \n\t"
            "brctg %[n_tmp],1b"
            : [mem_x] "+m" (*(double (*)[n])x),
              [mem_y] "+m" (*(double (*)[n])y),
              [n_tmp] "+&r"(n)
            : [ptr_x] "a"(x), [ptr_y] "a"(y) 
            : "cc", "r1", "v16","v17","v18","v19","v20","v21","v22","v23"
              ,"v24","v25","v26","v27","v28","v29","v30","v31"
            );
    return;

}

#else

static void   dswap_kernel_32(BLASLONG n, FLOAT *x, FLOAT *y)
{
         __asm__ volatile(
            "pfd 2, 0(%[ptr_x]) \n\t"
            "pfd 2, 0(%[ptr_y]) \n\t"
            "srlg %[n_tmp],%[n_tmp],5       \n\t"
            "xgr %%r1,%%r1       \n\t"
            ".align 16 \n\t"
            "1: \n\t"
            "pfd 2, 256(%%r1,%[ptr_x]) \n\t"
            "pfd 2, 256(%%r1,%[ptr_y]) \n\t"
            
            "vl  %%v16, 0(%%r1,%[ptr_x])   \n\t" 
            "vl  %%v17, 16(%%r1,%[ptr_x])  \n\t" 
            "vl  %%v18, 32(%%r1,%[ptr_x])  \n\t" 
            "vl  %%v19, 48(%%r1,%[ptr_x])  \n\t"  
            "vl  %%v20, 64(%%r1,%[ptr_x])  \n\t" 
            "vl  %%v21, 80(%%r1,%[ptr_x])  \n\t" 
            "vl  %%v22, 96(%%r1,%[ptr_x])  \n\t" 
            "vl  %%v23, 112(%%r1,%[ptr_x]) \n\t"
            "vl  %%v24, 128(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v25, 144(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v26, 160(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v27, 176(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v28, 192(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v29, 208(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v30, 224(%%r1,%[ptr_x]) \n\t" 
            "vl  %%v31, 240(%%r1,%[ptr_x]) \n\t"


            "vl  %%v0, 0(%%r1,%[ptr_y])    \n\t" 
            "vl  %%v1, 16(%%r1,%[ptr_y])   \n\t" 
            "vl  %%v2, 32(%%r1,%[ptr_y])   \n\t" 
            "vl  %%v3, 48(%%r1,%[ptr_y])   \n\t"  
            "vl  %%v4, 64(%%r1,%[ptr_y])   \n\t" 
            "vl  %%v5, 80(%%r1,%[ptr_y])   \n\t" 
            "vl  %%v6, 96(%%r1,%[ptr_y])   \n\t" 
            "vl  %%v7, 112(%%r1,%[ptr_y])  \n\t"
            "vst %%v0,  0(%%r1,%[ptr_x])   \n\t" 
            "vst %%v1, 16(%%r1,%[ptr_x])   \n\t" 
            "vst %%v2, 32(%%r1,%[ptr_x])   \n\t" 
            "vst %%v3, 48(%%r1,%[ptr_x])   \n\t"  
            "vst %%v4, 64(%%r1,%[ptr_x])   \n\t" 
            "vst %%v5, 80(%%r1,%[ptr_x])   \n\t" 
            "vst %%v6, 96(%%r1,%[ptr_x])   \n\t" 
            "vst %%v7, 112(%%r1,%[ptr_x])  \n\t" 

            "vl  %%v0, 128(%%r1,%[ptr_y])  \n\t" 
            "vl  %%v1, 144(%%r1,%[ptr_y])  \n\t" 
            "vl  %%v2, 160(%%r1,%[ptr_y])  \n\t" 
            "vl  %%v3, 176(%%r1,%[ptr_y])  \n\t"  
            "vl  %%v4, 192(%%r1,%[ptr_y])  \n\t" 
            "vl  %%v5, 208(%%r1,%[ptr_y])  \n\t" 
            "vl  %%v6, 224(%%r1,%[ptr_y])  \n\t" 
            "vl  %%v7, 240(%%r1,%[ptr_y])  \n\t" 
            "vst %%v0, 128(%%r1,%[ptr_x])  \n\t" 
            "vst %%v1, 144(%%r1,%[ptr_x])  \n\t" 
            "vst %%v2, 160(%%r1,%[ptr_x])  \n\t" 
            "vst %%v3, 176(%%r1,%[ptr_x])  \n\t"  
            "vst %%v4, 192(%%r1,%[ptr_x])  \n\t" 
            "vst %%v5, 208(%%r1,%[ptr_x])  \n\t" 
            "vst %%v6, 224(%%r1,%[ptr_x])  \n\t" 
            "vst %%v7, 240(%%r1,%[ptr_x])  \n\t"

            "vst %%v16,  0(%%r1,%[ptr_y])  \n\t" 
            "vst %%v17, 16(%%r1,%[ptr_y])  \n\t" 
            "vst %%v18, 32(%%r1,%[ptr_y])  \n\t" 
            "vst %%v19, 48(%%r1,%[ptr_y])  \n\t"  
            "vst %%v20, 64(%%r1,%[ptr_y])  \n\t" 
            "vst %%v21, 80(%%r1,%[ptr_y])  \n\t" 
            "vst %%v22, 96(%%r1,%[ptr_y])  \n\t" 
            "vst %%v23, 112(%%r1,%[ptr_y]) \n\t"
            "vst %%v24, 128(%%r1,%[ptr_y]) \n\t" 
            "vst %%v25, 144(%%r1,%[ptr_y]) \n\t" 
            "vst %%v26, 160(%%r1,%[ptr_y]) \n\t" 
            "vst %%v27, 176(%%r1,%[ptr_y]) \n\t" 
            "vst %%v28, 192(%%r1,%[ptr_y]) \n\t" 
            "vst %%v29, 208(%%r1,%[ptr_y]) \n\t" 
            "vst %%v30, 224(%%r1,%[ptr_y]) \n\t" 
            "vst %%v31, 240(%%r1,%[ptr_y]) \n\t"
 
          
            "la  %%r1,256(%%r1) \n\t"
            "brctg %[n_tmp],1b"
            : [mem_x] "+m" (*(double (*)[n])x),
              [mem_y] "+m" (*(double (*)[n])y),
              [n_tmp] "+&r"(n)
            : [ptr_x] "a"(x), [ptr_y] "a"(y) 
            : "cc", "r1", "v0","v1","v2","v3","v4","v5","v6","v7","v16",
            "v17","v18","v19","v20","v21","v22","v23","v24","v25","v26","v27","v28","v29","v30","v31" 
            );
    return;

}

#endif

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT dummy3, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
{
    BLASLONG i=0;
    BLASLONG ix=0,iy=0;
    FLOAT temp;

    if ( n <= 0     )  return(0);

    if ( (inc_x == 1) && (inc_y == 1 ))
    {

        BLASLONG n1 = n & -32;
        if ( n1 > 0 )
        {
            dswap_kernel_32(n1, x, y);
            i=n1;
        }

        while(i < n)
        {
            temp = y[i];    
            y[i] = x[i] ;
            x[i] = temp;
            i++ ;

        }


    }
    else
    {

        while(i < n)
        {
            temp  = y[iy];
            y[iy] = x[ix] ;
            x[ix] = temp;
            ix += inc_x ;
            iy += inc_y ;
            i++ ;

        }

    }
    return(0);
    

}


