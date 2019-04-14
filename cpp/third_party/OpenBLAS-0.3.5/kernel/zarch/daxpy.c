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

#define PREFETCH_INS 1
#if defined(Z13_A)
#include <vecintrin.h>

static void daxpy_kernel_32(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT alpha)
{
    BLASLONG  i = 0;
    __vector double v_a = {alpha,alpha}; 
    __vector double * v_y=(__vector double *)y;
    __vector double * v_x=(__vector double *)x;
        
    for(; i<n/2; i+=16){

        v_y[i]    += v_a * v_x[i];
        v_y[i+1]  += v_a * v_x[i+1];
        v_y[i+2]  += v_a * v_x[i+2];
        v_y[i+3]  += v_a * v_x[i+3];
        v_y[i+4]  += v_a * v_x[i+4];
        v_y[i+5]  += v_a * v_x[i+5];
        v_y[i+6]  += v_a * v_x[i+6];
        v_y[i+7]  += v_a * v_x[i+7]; 
        v_y[i+8]  += v_a * v_x[i+8];
        v_y[i+9]  += v_a * v_x[i+9];
        v_y[i+10] += v_a * v_x[i+10];
        v_y[i+11] += v_a * v_x[i+11];
        v_y[i+12] += v_a * v_x[i+12];
        v_y[i+13] += v_a * v_x[i+13];
        v_y[i+14] += v_a * v_x[i+14];
        v_y[i+15] += v_a * v_x[i+15];
    }

}
#else
static void   daxpy_kernel_32(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT alpha)
{ 
        
         __asm__ volatile( 
#if defined(PREFETCH_INS)         
                   "pfd      1, 0(%[x_tmp])   \n\t"
                   "pfd      2, 0(%[y_tmp])   \n\t"
#endif
                    "lgdr    %%r0,%[alpha]    \n\t"
                    "vlvgp   %%v0,%%r0,%%r0   \n\t"   
                    "srlg    %%r0,%[n],5      \n\t" 
                    "vlr     %%v1,%%v0        \n\t"
                    ".align 16 \n\t"    
                    "1: \n\t"
#if defined(PREFETCH_INS)         
                    "pfd      1, 256(%[x_tmp]) \n\t"
                    "pfd      2, 256(%[y_tmp]) \n\t"
#endif                  
                    "vlm      %%v16,%%v23,  0(%[x_tmp])    \n\t"
                    "vlm      %%v24, %%v31, 0(%[y_tmp])    \n\t"
                    "vfmadb   %%v16,%%v0,%%v16,%%v24       \n\t"
                    "vfmadb   %%v17,%%v1,%%v17,%%v25       \n\t"  
                    "vfmadb   %%v18,%%v0,%%v18,%%v26       \n\t"
                    "vfmadb   %%v19,%%v1,%%v19,%%v27       \n\t"
                    "vfmadb   %%v20,%%v0,%%v20,%%v28       \n\t"
                    "vfmadb   %%v21,%%v1,%%v21,%%v29       \n\t"  
                    "vfmadb   %%v22,%%v0,%%v22,%%v30       \n\t"
                    "vfmadb   %%v23,%%v1,%%v23,%%v31       \n\t"
                    "vstm     %%v16,%%v23,   0(%[y_tmp])   \n\t" 
                    "vlm      %%v24,%%v31, 128(%[x_tmp])   \n\t"
                    "vlm      %%v16,%%v23, 128(%[y_tmp])   \n\t"
                    "vfmadb   %%v24,%%v0,%%v24,%%v16       \n\t"
                    "vfmadb   %%v25,%%v1,%%v25,%%v17       \n\t"  
                    "vfmadb   %%v26,%%v0,%%v26,%%v18       \n\t"
                    "vfmadb   %%v27,%%v1,%%v27,%%v19       \n\t"
                    "vfmadb   %%v28,%%v0,%%v28,%%v20       \n\t"
                    "vfmadb   %%v29,%%v1,%%v29,%%v21       \n\t"  
                    "vfmadb   %%v30,%%v0,%%v30,%%v22       \n\t"
                    "vfmadb   %%v31,%%v1,%%v31,%%v23       \n\t"  
                    "la       %[x_tmp],256(%[x_tmp])       \n\t"
                    "vstm     %%v24, %%v31, 128(%[y_tmp])  \n\t"
                    "la       %[y_tmp],256(%[y_tmp])       \n\t" 
                    "brctg    %%r0,1b"
                    : [mem_y] "+m" (*(double (*)[n])y), [x_tmp] "+&a"(x), [y_tmp] "+&a"(y)
                    : [mem_x] "m" (*(const double (*)[n])x), [n] "r"(n), [alpha] "f"(alpha)
                    :"cc", "r0", "v0","v1","v16","v17","v18","v19","v20","v21",
                    "v22","v23","v24","v25","v26","v27","v28","v29","v30","v31"
                 );
 

}
 
 

#endif
 

int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
{
    BLASLONG i=0;
    BLASLONG ix=0,iy=0;

    if ( n <= 0 )  return 0 ;

    if ( (inc_x == 1) && (inc_y == 1) )
    {

        BLASLONG n1 = n & -32;

        if ( n1 )
            daxpy_kernel_32(n1, x, y , da );

        i = n1;
        while(i < n)
        {

            y[i] += da * x[i] ;
            i++ ;

        }
        return 0 ;


    }

    BLASLONG n1 = n & -4;

    while(i < n1)
    {

        FLOAT m1      = da * x[ix] ;
        FLOAT m2      = da * x[ix+inc_x] ;
        FLOAT m3      = da * x[ix+2*inc_x] ;
        FLOAT m4      = da * x[ix+3*inc_x] ;

        y[iy]         += m1 ;
        y[iy+inc_y]   += m2 ;
        y[iy+2*inc_y] += m3 ;
        y[iy+3*inc_y] += m4 ;

        ix  += inc_x*4 ;
        iy  += inc_y*4 ;
        i+=4 ;

    }

    while(i < n)
    {

        y[iy] += da * x[ix] ;
        ix  += inc_x ;
        iy  += inc_y ;
        i++ ;

    }
    return 0 ;

}


