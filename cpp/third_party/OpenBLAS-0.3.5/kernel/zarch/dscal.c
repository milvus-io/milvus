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

#ifdef Z13_A
static void   dscal_kernel_32( BLASLONG n, FLOAT  da , FLOAT *x )
{

          
             __asm__ ("pfd    2, 0(%[x_ptr])   \n\t"
                      "lgdr   %%r0,%[alpha]    \n\t"
                      "vlvgp  %%v0,%%r0,%%r0   \n\t"
                      "srlg   %[n],%[n],4 \n\t"
                      "vlr    %%v1,%%v0        \n\t"
                      "vlm    %%v16,%%v23, 0(%[x_ptr])          \n\t"
                      "la     %[x_ptr], 128(%[x_ptr])     \n\t"
                      "aghik  %[n], %[n], -1             \n\t"
                      "jle     2f     \n\t"
                       ".align 16 \n\t"
                      "1:          \n\t"
                      "vfmdb  %%v24, %%v16, %%v0          \n\t"
                      "vfmdb  %%v25, %%v17, %%v0          \n\t"
                      "vfmdb  %%v26, %%v18, %%v0          \n\t"
                      "vfmdb  %%v27, %%v19, %%v1          \n\t"
                      "vlm     %%v16,%%v19, 0(%[x_ptr])         \n\t"
                      "vfmdb  %%v28, %%v20, %%v0          \n\t"
                      "vfmdb  %%v29, %%v21, %%v1          \n\t"
                      "vfmdb  %%v30, %%v22, %%v0          \n\t"
                      "vfmdb  %%v31, %%v23, %%v1          \n\t"
                      "vlm     %%v20,%%v23, 64(%[x_ptr])         \n\t"
                      "lay    %[x_ptr], -128(%[x_ptr])    \n\t"
                      "vstm   %%v24,%%v31, 0(%[x_ptr])          \n\t"
                      "la     %[x_ptr],256(%[x_ptr])      \n\t"
                      "brctg %[n],1b     \n\t"
                      "2:            \n\t"
                      "vfmdb  %%v24, %%v16, %%v0          \n\t"
                      "vfmdb  %%v25, %%v17, %%v1          \n\t"
                      "vfmdb  %%v26, %%v18, %%v0          \n\t"
                      "vfmdb  %%v27, %%v19, %%v1          \n\t"
                      "lay    %[x_ptr] , -128(%[x_ptr])   \n\t"
                      "vfmdb  %%v28, %%v20, %%v0          \n\t"
                      "vfmdb  %%v29, %%v21, %%v1          \n\t"
                      "vfmdb  %%v30, %%v22, %%v0          \n\t"
                      "vfmdb  %%v31, %%v23, %%v1          \n\t"
                      "vstm   %%v24,%%v31, 0(%[x_ptr])         \n\t"
                      : [mem] "+m" (*(double (*)[n])x) ,[x_ptr] "+&a"(x),[n] "+&r"(n)
                                       : [alpha] "f"(da)
                                       :"cc" ,  "r0","v0","v1","v16","v17","v18","v19","v20","v21",
                                       "v22","v23","v24","v25","v26","v27","v28","v29","v30","v31"
                 );
 }
#else
static void   dscal_kernel_32( BLASLONG n, FLOAT  da , FLOAT *x )
{

             /* faster than sequence of triples(vl vfmd vst) (tested OPENBLAS_LOOPS=10000) */
             __asm__ ("pfd    2, 0(%[x_ptr])   \n\t"      
                      "lgdr   %%r0,%[alpha]    \n\t"
                      "vlvgp  %%v0,%%r0,%%r0   \n\t"
                      "vlr    %%v1,%%v0        \n\t"
                      "sllg   %%r0,%[n],3      \n\t" 
                      "agr    %%r0,%[x_ptr]    \n\t"
                      ".align 16 \n\t"    
                      "1:     \n\t" 
                      "pfd    2,         256(%[x_ptr])     \n\t"    
                      "vlm    %%v16,%%v23, 0(%[x_ptr])     \n\t"
                      "vfmdb  %%v16,%%v16,%%v0 \n\t"
                      "vfmdb  %%v17,%%v17,%%v1 \n\t"
                      "vfmdb  %%v18,%%v18,%%v0 \n\t"
                      "vfmdb  %%v19,%%v19,%%v1 \n\t"
                      "vfmdb  %%v20,%%v20,%%v0 \n\t"
                      "vfmdb  %%v21,%%v21,%%v1 \n\t"
                      "vfmdb  %%v22,%%v22,%%v0 \n\t"
                      "vfmdb  %%v23,%%v23,%%v1 \n\t" 
                      "vstm   %%v16,%%v23, 0(%[x_ptr])      \n\t"  
                      "vlm    %%v24,%%v31,128(%[x_ptr])     \n\t"                                              
                      "vfmdb  %%v24,%%v24,%%v0 \n\t"       
                      "vfmdb  %%v25,%%v25,%%v1 \n\t"
                      "vfmdb  %%v26,%%v26,%%v0 \n\t"
                      "vfmdb  %%v27,%%v27,%%v1 \n\t"
                      "vfmdb  %%v28,%%v28,%%v0 \n\t"
                      "vfmdb  %%v29,%%v29,%%v1 \n\t"
                      "vfmdb  %%v30,%%v30,%%v0 \n\t"
                      "vfmdb  %%v31,%%v31,%%v1 \n\t"                                     
                      "vstm   %%v24,%%v31,128(%[x_ptr])    \n\t"  
                      "la     %[x_ptr],  256(%[x_ptr])    \n\t"
                      "clgrjl %[x_ptr],%%r0,1b \n\t"  
                      : [mem] "+m" (*(double (*)[n])x) ,[x_ptr] "+&a"(x)
                      : [n] "r"(n),[alpha] "f"(da)
                      :"cc" ,  "r0","v0","v1","v16","v17","v18","v19","v20","v21",
                      "v22","v23","v24","v25","v26","v27","v28","v29","v30","v31"
                 );

 }
#endif
static void   dscal_kernel_32_zero( BLASLONG n,  FLOAT *x )
{
   
             __asm__ ("pfd    2, 0(%[x_ptr])   \n\t"      
                      "vzero  %%v24            \n\t"
                      "sllg   %%r0,%[n],3      \n\t" 
                      "vzero  %%v25            \n\t"
                      "agr    %%r0,%[x_ptr]    \n\t"
                      ".align 16 \n\t"    
                      "1:        \n\t" 
                      "pfd    2,      256(%[x_ptr])  \n\t"     
                      "vst    %%v24,    0(%[x_ptr])  \n\t" 
                      "vst    %%v25,    16(%[x_ptr]) \n\t" 
                      "vst    %%v24,    32(%[x_ptr]) \n\t"   
                      "vst    %%v25,    48(%[x_ptr]) \n\t"  
                      "vst    %%v24,    64(%[x_ptr]) \n\t" 
                      "vst    %%v25,    80(%[x_ptr]) \n\t" 
                      "vst    %%v24,    96(%[x_ptr]) \n\t"  
                      "vst    %%v25,   112(%[x_ptr]) \n\t"  
                      "vst    %%v24,   128(%[x_ptr]) \n\t" 
                      "vst    %%v25,   144(%[x_ptr]) \n\t" 
                      "vst    %%v24,   160(%[x_ptr]) \n\t"   
                      "vst    %%v25,   176(%[x_ptr]) \n\t"  
                      "vst    %%v24,   192(%[x_ptr]) \n\t" 
                      "vst    %%v25,   208(%[x_ptr]) \n\t" 
                      "vst    %%v24,   224(%[x_ptr]) \n\t"  
                      "vst    %%v25,   240(%[x_ptr]) \n\t"                        
                      "la     %[x_ptr],256(%[x_ptr]) \n\t"
                      "clgrjl %[x_ptr],%%r0,1b \n\t"
                      : [mem] "=m" (*(double (*)[n])x) ,[x_ptr] "+&a"(x)
                      : [n] "r"(n)
                      :"cc" ,  "r0", "v24" ,"v25"
                 );
}
 



int CNAME(BLASLONG n, BLASLONG dummy0, BLASLONG dummy1, FLOAT da, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT *dummy, BLASLONG dummy2)
{
    BLASLONG i=0,j=0;
    if ( n <= 0 || inc_x <=0 )
        return(0);

 
    if ( inc_x == 1 )
    {

        if ( da == 0.0 )
        {        

            BLASLONG n1 = n & -32;
            if ( n1 > 0 )
            {
                
                dscal_kernel_32_zero(n1 ,  x);
                j=n1;
            }

            while(j < n)
            {

                x[j]=0.0;
                j++;
            }

        }
        else
        {

            BLASLONG n1 = n & -32;
            if ( n1 > 0 )
            { 
                dscal_kernel_32(n1 , da , x);
                j=n1;
            }
            while(j < n)
            {

                x[j] = da * x[j] ;
                j++;
            }
        }


    }
    else
    {

        if ( da == 0.0 )
        {        

                        BLASLONG n1 = n & -4;

                        while (j < n1) {

                            x[i]=0.0;
                            x[i + inc_x]=0.0;
                            x[i + 2 * inc_x]=0.0;
                            x[i + 3 * inc_x]=0.0;

                            i += inc_x * 4; 
                            j += 4;

                        } 
            while(j < n)
            {

                x[i]=0.0;
                i += inc_x ;
                j++;
            }

        }
        else
        {
                        BLASLONG n1 = n & -4;

                        while (j < n1) {

                            x[i] = da * x[i] ;
                            x[i + inc_x] = da * x[i + inc_x];
                            x[i + 2 * inc_x] = da *  x[i + 2 * inc_x];
                            x[i + 3 * inc_x] = da * x[i + 3 * inc_x];

                            i += inc_x * 4; 
                            j += 4;

                        }  

            while(j < n)
            {

                x[i] = da * x[i] ;
                i += inc_x ;
                j++;
            }
        }

    }
    return 0;

}