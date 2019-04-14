/***************************************************************************
Copyright (c) 2017, The OpenBLAS Project
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

 
static void  zaxpy_kernel_8(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT da_r,FLOAT da_i) {

    BLASLONG tempR1 ;
    __asm__ ("pfd   1, 0(%[x_tmp]) \n\t"
             "pfd    2, 0(%[y_tmp]) \n\t" 
#if !defined(CONJ)
            "lgdr   %[t1],%[alpha_r]    \n\t" 
            "vlvgp  %%v28,%[t1],%[t1]   \n\t" //load both from disjoint          
            "lgdr   %[t1],%[alpha_i]    \n\t"  
            "vlvgp  %%v29,%[t1],%[t1]   \n\t" //load both from disjoint   
            "vflcdb %%v29,%%v29       \n\t" //complement both
            "vlvgg  %%v29,%[t1],1     \n\t" //restore 2nd  so that  {-alpha_i, alpha_i}   

#else
            "lgdr   %[t1],%[alpha_i]    \n\t"  
            "vlvgp  %%v29,%[t1],%[t1]   \n\t" //load both from disjoint        
            "lgdr   %[t1],%[alpha_r]    \n\t" 
            "vlvgp  %%v28,%[t1],%[t1]   \n\t" //load both from disjoint    
            "vflcdb %%v28,%%v28       \n\t" //complement both
            "vlvgg  %%v28,%[t1],0     \n\t" //restore 1st  so that  {alpha_r,-alpha_r}   
#endif           
                               
            "xgr     %[t1],%[t1]  \n\t" 
            "sllg   %[tmp],%[tmp],4    \n\t" 
            "vl   %%v30 ,  0(%[t1],%[y_tmp]) \n\t" 
            "vl   %%v31 , 16(%[t1],%[y_tmp]) \n\t" 
            "vl   %%v6 , 32(%[t1],%[y_tmp]) \n\t" 
            "vl   %%v7 , 48(%[t1],%[y_tmp]) \n\t" 
            "vl   %%v20 ,  0(%[t1],%[x_tmp]) \n\t" 
            "vl   %%v21 , 16(%[t1],%[x_tmp]) \n\t" 
            "vl   %%v22 , 32(%[t1],%[x_tmp]) \n\t" 
            "vl   %%v23 , 48(%[t1],%[x_tmp]) \n\t"                         
            "lay  %[tmp],-64 (%[tmp]) \n\t" //tmp-=64 so that t1+64 can break tmp condition
            "j 2f \n\t"
            ".align 16 \n\t"
            "1:     \n\t"
  
            "vpdi   %%v24 , %%v20, %%v20, 4     \n\t"
            "vpdi   %%v25 , %%v21, %%v21, 4     \n\t"
            "vpdi   %%v26 , %%v22, %%v22, 4     \n\t"
            "vpdi   %%v27 , %%v23, %%v23, 4     \n\t" 
            "vfmadb %%v16,  %%v20, %%v28, %%v16 \n\t"
            "vfmadb %%v17,  %%v21, %%v28, %%v17 \n\t"
            "vfmadb %%v18,  %%v22, %%v28, %%v18 \n\t"
            "vfmadb %%v19,  %%v23, %%v28, %%v19 \n\t"
            "vl     %%v30,  64(%[t1],%[y_tmp])  \n\t" 
            "vl     %%v31,  80(%[t1],%[y_tmp])  \n\t" 
            "vl     %%v6 ,  96(%[t1],%[y_tmp])  \n\t" 
            "vl     %%v7 , 112(%[t1],%[y_tmp])  \n\t" 
            "vfmadb %%v16,  %%v24, %%v29, %%v16 \n\t"
            "vfmadb %%v17,  %%v25, %%v29, %%v17 \n\t" 
            "vfmadb %%v18,  %%v26, %%v29, %%v18 \n\t"
            "vfmadb %%v19,  %%v27, %%v29, %%v19 \n\t"
            "vl     %%v20 , 64(%[t1],%[x_tmp])  \n\t" 
            "vl     %%v21 , 80(%[t1],%[x_tmp])  \n\t"      
            "vl     %%v22 , 96(%[t1],%[x_tmp])  \n\t" 
            "vl     %%v23 ,112(%[t1],%[x_tmp])  \n\t" 

            "vst    %%v16 ,  0(%[t1],%[y_tmp])  \n\t" 
            "vst    %%v17 , 16(%[t1],%[y_tmp])  \n\t" 
            "vst    %%v18 , 32(%[t1],%[y_tmp])  \n\t" 
            "vst    %%v19 , 48(%[t1],%[y_tmp])  \n\t"   
    
            "la     %[t1],64(%[t1] ) \n\t" 
            "2:  \n\t"
            "pfd    1, 256(%[t1],%[x_tmp])  \n\t"
            "pfd    2, 256(%[t1],%[y_tmp])  \n\t"  
            "vpdi   %%v24 , %%v20, %%v20, 4     \n\t"
            "vpdi   %%v25 , %%v21, %%v21, 4     \n\t"
            "vpdi   %%v26 , %%v22, %%v22, 4     \n\t"
            "vpdi   %%v27 , %%v23, %%v23, 4     \n\t" 

            "vfmadb %%v30,  %%v20, %%v28, %%v30 \n\t"
            "vfmadb %%v31,  %%v21, %%v28, %%v31 \n\t"
            "vfmadb %%v6,  %%v22, %%v28, %%v6   \n\t"
            "vfmadb %%v7,  %%v23, %%v28, %%v7   \n\t"
            "vl     %%v16,  64(%[t1],%[y_tmp])  \n\t" 
            "vl     %%v17,  80(%[t1],%[y_tmp])  \n\t" 
            "vl     %%v18,  96(%[t1],%[y_tmp])  \n\t" 
            "vl     %%v19, 112(%[t1],%[y_tmp])  \n\t" 
            "vfmadb %%v30,  %%v24, %%v29, %%v30 \n\t"
            "vfmadb %%v31,  %%v25, %%v29, %%v31 \n\t"
            "vfmadb %%v6,  %%v26, %%v29, %%v6   \n\t"
            "vfmadb %%v7,  %%v27, %%v29, %%v7   \n\t"

            "vl     %%v20 , 64(%[t1],%[x_tmp])  \n\t" 
            "vl     %%v21 , 80(%[t1],%[x_tmp])  \n\t"  
            "vl     %%v22 , 96(%[t1],%[x_tmp])  \n\t" 
            "vl     %%v23 ,112(%[t1],%[x_tmp])  \n\t" 

            "vst    %%v30 ,  0(%[t1],%[y_tmp])  \n\t" 
            "vst    %%v31 , 16(%[t1],%[y_tmp])  \n\t" 
            "vst    %%v6 ,  32(%[t1],%[y_tmp])  \n\t" 
            "vst    %%v7 ,  48(%[t1],%[y_tmp])  \n\t"  
 
            "la     %[t1],64(%[t1] ) \n\t"
          

             "clgrjl %[t1],%[tmp],1b         \n\t"   
//----------------------------------------------------------------------
            "vfmadb %%v16,  %%v20, %%v28, %%v16 \n\t"
            "vfmadb %%v17,  %%v21, %%v28, %%v17 \n\t"
            "vfmadb %%v18,  %%v22, %%v28, %%v18 \n\t"
            "vfmadb %%v19,  %%v23, %%v28, %%v19 \n\t" 
            "vpdi   %%v24 , %%v20, %%v20, 4     \n\t"
            "vpdi   %%v25 , %%v21, %%v21, 4     \n\t" 
            "vpdi   %%v26 , %%v22, %%v22, 4     \n\t"
            "vpdi   %%v27 , %%v23, %%v23, 4     \n\t"             
            "vfmadb %%v16,  %%v24, %%v29, %%v16 \n\t"
            "vfmadb %%v17,  %%v25, %%v29, %%v17 \n\t"
            "vfmadb %%v18,  %%v26, %%v29, %%v18 \n\t"
            "vfmadb %%v19,  %%v27, %%v29, %%v19 \n\t"

            "vst   %%v16 ,  0(%[t1],%[y_tmp])   \n\t" 
            "vst   %%v17 , 16(%[t1],%[y_tmp])   \n\t" 
            "vst   %%v18 , 32(%[t1],%[y_tmp])   \n\t" 
            "vst   %%v19 , 48(%[t1],%[y_tmp])   \n\t"   

            : [mem_y] "+m" (*(double (*)[2*n])y),[tmp]"+&r"(n) ,  [t1] "=&a" (tempR1) 
            : [mem_x] "m" (*(const double (*)[2*n])x), [x_tmp] "a"(x), [y_tmp] "a"(y), [alpha_r] "f"(da_r),[alpha_i] "f"(da_i)
            : "cc",  "v6","v7", "v16",
            "v17","v18","v19","v20","v21","v22","v23","v24","v25","v26","v27","v28","v29","v30","v31"
            );

}


int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da_r, FLOAT da_i, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2) {
    BLASLONG i = 0;
    BLASLONG ix = 0, iy = 0;

    if (n <= 0) return (0);

    if ((inc_x == 1) && (inc_y == 1)) {

        BLASLONG n1 = n & -8;

        if (n1) { 
            zaxpy_kernel_8(n1, x, y, da_r,da_i);
            ix = 2 * n1;
        }
        i = n1;
        while (i < n) {
#if !defined(CONJ)
            y[ix] += (da_r * x[ix] - da_i * x[ix + 1]);
            y[ix + 1] += (da_r * x[ix + 1] + da_i * x[ix]);
#else
            y[ix] += (da_r * x[ix] + da_i * x[ix + 1]);
            y[ix + 1] -= (da_r * x[ix + 1] - da_i * x[ix]);
#endif
            i++;
            ix += 2;

        }
        return (0);


    }

    inc_x *= 2;
    inc_y *= 2;

    while (i < n) {

#if !defined(CONJ)
        y[iy] += (da_r * x[ix] - da_i * x[ix + 1]);
        y[iy + 1] += (da_r * x[ix + 1] + da_i * x[ix]);
#else
        y[iy] += (da_r * x[ix] + da_i * x[ix + 1]);
        y[iy + 1] -= (da_r * x[ix + 1] - da_i * x[ix]);
#endif
        ix += inc_x;
        iy += inc_y;
        i++;

    }
    return (0);

}


