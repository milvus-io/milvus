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


#if  defined(Z13)
static  FLOAT  ddot_kernel_16(BLASLONG n, FLOAT *x, FLOAT *y)
{
    FLOAT dot;
         __asm__ volatile( 
            "pfd   1, 0(%[ptr_x_tmp]) \n\t"
            "pfd   1, 0(%[ptr_y_tmp]) \n\t"      
            "vzero %%v24  \n\t"
            "vzero %%v25  \n\t" 
            "vzero %%v26  \n\t"
            "vzero %%v27  \n\t"                  
            "srlg  %[n_tmp],%[n_tmp],4    \n\t" 
            "xgr   %%r1,%%r1    \n\t"
            ".align 16 \n\t"    
            "1:    \n\t"
            "pfd    1,    256(%%r1,%[ptr_x_tmp]) \n\t"
            "pfd    1,    256(%%r1,%[ptr_y_tmp]) \n\t"                
            "vl     %%v16,  0(%%r1,%[ptr_x_tmp]) \n\t"
            "vl     %%v17, 16(%%r1,%[ptr_x_tmp]) \n\t"
            "vl     %%v18, 32(%%r1,%[ptr_x_tmp]) \n\t"
            "vl     %%v19, 48(%%r1,%[ptr_x_tmp]) \n\t"

            "vl     %%v28,  0(%%r1,%[ptr_y_tmp]) \n\t"
            "vfmadb %%v24,%%v16,%%v28,%%v24      \n\t"  
            "vl     %%v29, 16(%%r1,%[ptr_y_tmp]) \n\t"
            "vfmadb %%v25,%%v17,%%v29,%%v25      \n\t"   
     
            "vl     %%v30, 32(%%r1,%[ptr_y_tmp]) \n\t"
            "vfmadb %%v26,%%v18,%%v30,%%v26      \n\t"      
            "vl     %%v31, 48(%%r1,%[ptr_y_tmp]) \n\t" 
            "vfmadb %%v27,%%v19,%%v31,%%v27      \n\t"   
 
            "vl     %%v16,  64(%%r1 ,%[ptr_x_tmp]) \n\t"
            "vl     %%v17,  80(%%r1,%[ptr_x_tmp])  \n\t"
            "vl     %%v18,  96(%%r1,%[ptr_x_tmp])  \n\t"
            "vl     %%v19, 112(%%r1,%[ptr_x_tmp])  \n\t"

            "vl     %%v28, 64(%%r1,%[ptr_y_tmp]) \n\t"
            "vfmadb %%v24,%%v16,%%v28,%%v24      \n\t"  
            "vl     %%v29, 80(%%r1,%[ptr_y_tmp]) \n\t"
            "vfmadb %%v25,%%v17,%%v29,%%v25      \n\t"  
          
     
            "vl     %%v30, 96(%%r1,%[ptr_y_tmp])  \n\t"
            "vfmadb %%v26,%%v18,%%v30,%%v26       \n\t" 
            "vl     %%v31, 112(%%r1,%[ptr_y_tmp]) \n\t" 
            "vfmadb %%v27,%%v19,%%v31,%%v27       \n\t"  
             
            
            "la     %%r1,128(%%r1) \n\t"
            "brctg  %[n_tmp],1b \n\t"
            "vfadb  %%v24,%%v25,%%v24    \n\t"
            "vfadb  %%v24,%%v26,%%v24    \n\t"
            "vfadb  %%v24,%%v27,%%v24    \n\t"                 
            "vrepg  %%v1,%%v24,1         \n\t"
            "vfadb  %%v1,%%v24,%%v1      \n\t"  
            "ldr    %[dot],  %%f1     \n\t"  
            : [dot] "=f"(dot) ,[n_tmp] "+&r"(n)
            : [mem_x] "m"( *(const double (*)[n])x),
              [mem_y] "m"( *(const double (*)[n])y),
              [ptr_x_tmp]"a"(x), [ptr_y_tmp] "a"(y) 
            :"cc" , "r1","f1","v16", "v17","v18","v19","v20","v21","v22","v23",
            "v24","v25","v26","v27","v28","v29","v30","v31"

         );
    return dot;        

}


#else

static FLOAT ddot_kernel_16(BLASLONG n, FLOAT *x, FLOAT *y )
{
    BLASLONG register i = 0;
    FLOAT dot = 0.0;

    while(i < n)
        {
            dot +=  y[i]  * x[i]
                  + y[i+1] * x[i+1]
                  + y[i+2] * x[i+2]
                  + y[i+3] * x[i+3]
                  + y[i+4] * x[i+4]
                  + y[i+5] * x[i+5]
                  + y[i+6] * x[i+6]
                  + y[i+7] * x[i+7] ;
            dot +=  y[i+8]  * x[i+8]
                  + y[i+9] * x[i+9]
                  + y[i+10] * x[i+10]
                  + y[i+11] * x[i+11]
                  + y[i+12] * x[i+12]
                  + y[i+13] * x[i+13]
                  + y[i+14] * x[i+14]
                  + y[i+15] * x[i+15] ;
    

            i+=16 ;

       }
    return dot;
    
}

#endif

FLOAT CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y)
{
    BLASLONG i=0;
    BLASLONG ix=0,iy=0;

    FLOAT  dot = 0.0 ;

    if ( n <= 0 )  return(dot);

    if ( (inc_x == 1) && (inc_y == 1) )
    {

        BLASLONG n1 = n & -16;
        
        if ( n1 ){
            dot = ddot_kernel_16(n1, x, y  );
            i = n1;
        }

        
        while(i < n)
        {

            dot += y[i] * x[i] ;
            i++ ;

        } 
        return(dot);


    }

    FLOAT temp1 = 0.0;
    FLOAT temp2 = 0.0;

    BLASLONG n1 = n & -4;    

    while(i < n1)
    {

        FLOAT m1 = y[iy]       * x[ix] ;
        FLOAT m2 = y[iy+inc_y] * x[ix+inc_x] ;

        FLOAT m3 = y[iy+2*inc_y] * x[ix+2*inc_x] ;
        FLOAT m4 = y[iy+3*inc_y] * x[ix+3*inc_x] ;

        ix  += inc_x*4 ;
        iy  += inc_y*4 ;

        temp1 += m1+m3;
        temp2 += m2+m4;

        i+=4 ;

    }

    while(i < n)
    {

        temp1 += y[iy] * x[ix] ;
        ix  += inc_x ;
        iy  += inc_y ;
        i++ ;

    }
    dot = temp1 + temp2;
    return(dot);

}


