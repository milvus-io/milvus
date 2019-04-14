/***************************************************************************
Copyright (c) 2018, The OpenBLAS Project
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

static void   zrot_kernel_4(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT cosA, FLOAT sinA)
{
  __vector double t0;
  __vector double t1;
  __vector double t2;
  __vector double t3;
  __vector double t4;
  __vector double t5;
  __vector double t6;
  __vector double t7;

 __asm__
    (
       "xxspltd     36, %x[cos], 0 \n\t"   // load c to both dwords
       "xxspltd     37, %x[sin], 0 \n\t"   // load s to both dwords 

       "lxvd2x      32,  0,     %[x_ptr] \n\t"   // load x
       "lxvd2x      33, %[i16], %[x_ptr] \n\t"
       "lxvd2x      34, %[i32], %[x_ptr] \n\t"
       "lxvd2x      35, %[i48],  %[x_ptr] \n\t" 

       "lxvd2x      48, 0,      %[y_ptr]   \n\t"   // load y
       "lxvd2x      49, %[i16], %[y_ptr] \n\t"
       "lxvd2x      50, %[i32], %[y_ptr] \n\t"
       "lxvd2x      51, %[i48], %[y_ptr] \n\t"  

       "addi        %[x_ptr], %[x_ptr], 64  \n\t"
       "addi        %[y_ptr], %[y_ptr], 64  \n\t"

       "addic.          %[temp_n], %[temp_n], -4    \n\t"
       "ble       2f          \n\t"

       ".p2align  5           \n"
     "1:                      \n\t"

       "xvmuldp         40, 32, 36  \n\t" // c * x
       "xvmuldp         41, 33, 36  \n\t"
       "xvmuldp         42, 34, 36  \n\t"
       "xvmuldp         43, 35, 36  \n\t"

       "xvmuldp         %x[x0], 48, 36    \n\t" // c * y
       "xvmuldp         %x[x1], 49, 36    \n\t"
       "xvmuldp         %x[x2], 50, 36    \n\t"
       "xvmuldp         %x[x3], 51, 36    \n\t"

       "xvmuldp         44, 32, 37  \n\t" // s * x
       "xvmuldp         45, 33, 37  \n\t"

       "lxvd2x          32, 0,     %[x_ptr]  \n\t" // load x
       "lxvd2x          33, %[i16],%[x_ptr]  \n\t"

       "xvmuldp         46, 34, 37  \n\t"
       "xvmuldp         47, 35, 37  \n\t"

       "lxvd2x          34, %[i32], %[x_ptr] \n\t"
       "lxvd2x          35, %[i48], %[x_ptr] \n\t"

       "xvmuldp         %x[x4], 48, 37    \n\t" // s * y
       "xvmuldp         %x[x5], 49, 37    \n\t"

       "lxvd2x          48, 0,      %[y_ptr] \n\t" // load y
       "lxvd2x          49, %[i16], %[y_ptr] \n\t"

       "xvmuldp         %x[x6], 50, 37    \n\t"
       "xvmuldp         %x[x7], 51, 37    \n\t"

       "lxvd2x          50, %[i32], %[y_ptr] \n\t"
       "lxvd2x          51, %[i48], %[y_ptr] \n\t"

       "xvadddp         40, 40, %x[x4]    \n\t" // c * x + s * y
       "xvadddp         41, 41, %x[x5]    \n\t" // c * x + s * y

       "addi            %[x_ptr], %[x_ptr], -64 \n\t"
       "addi            %[y_ptr], %[y_ptr], -64 \n\t"

       "xvadddp         42, 42, %x[x6]    \n\t" // c * x + s * y
       "xvadddp         43, 43, %x[x7]    \n\t" // c * x + s * y

       "xvsubdp         %x[x0], %x[x0], 44      \n\t" // c * y - s * x
       "xvsubdp         %x[x1], %x[x1], 45      \n\t" // c * y - s * x
       "xvsubdp         %x[x2], %x[x2], 46      \n\t" // c * y - s * x
       "xvsubdp         %x[x3], %x[x3], 47      \n\t" // c * y - s * x

       "stxvd2x         40, 0,      %[x_ptr] \n\t" // store x
       "stxvd2x         41, %[i16], %[x_ptr] \n\t"
       "stxvd2x         42, %[i32], %[x_ptr] \n\t"
       "stxvd2x         43, %[i48], %[x_ptr] \n\t"

       "stxvd2x         %x[x0], 0,      %[y_ptr]   \n\t" // store y
       "stxvd2x         %x[x1], %[i16], %[y_ptr]   \n\t"
       "stxvd2x         %x[x2], %[i32], %[y_ptr]   \n\t"
       "stxvd2x         %x[x3], %[i48], %[y_ptr]   \n\t"

       "addi            %[x_ptr], %[x_ptr], 128 \n\t"
       "addi            %[y_ptr], %[y_ptr], 128 \n\t"

       "addic.          %[temp_n], %[temp_n], -4      \n\t"
       "bgt+            1b          \n"

     "2:                      \n\t"

       "xvmuldp         40, 32, 36  \n\t" // c * x
       "xvmuldp         41, 33, 36  \n\t"
       "xvmuldp         42, 34, 36  \n\t"
       "xvmuldp         43, 35, 36  \n\t"

       "xvmuldp         %x[x0], 48, 36    \n\t" // c * y
       "xvmuldp         %x[x1], 49, 36    \n\t"
       "xvmuldp         %x[x2], 50, 36    \n\t"
       "xvmuldp         %x[x3], 51, 36    \n\t"

       "xvmuldp         44, 32, 37  \n\t" // s * x
       "xvmuldp         45, 33, 37  \n\t"
       "xvmuldp         46, 34, 37  \n\t"
       "xvmuldp         47, 35, 37  \n\t"

       "xvmuldp         %x[x4], 48, 37    \n\t" // s * y
       "xvmuldp         %x[x5], 49, 37    \n\t"
       "xvmuldp         %x[x6], 50, 37    \n\t"
       "xvmuldp         %x[x7], 51, 37    \n\t"

       "addi            %[x_ptr], %[x_ptr], -64 \n\t"
       "addi            %[y_ptr], %[y_ptr], -64 \n\t"

       "xvadddp         40, 40, %x[x4]    \n\t" // c * x + s * y
       "xvadddp         41, 41, %x[x5]    \n\t" // c * x + s * y
       "xvadddp         42, 42, %x[x6]    \n\t" // c * x + s * y
       "xvadddp         43, 43, %x[x7]    \n\t" // c * x + s * y

       "xvsubdp         %x[x0], %x[x0], 44      \n\t" // c * y - s * x
       "xvsubdp         %x[x1], %x[x1], 45      \n\t" // c * y - s * x
       "xvsubdp         %x[x2], %x[x2], 46      \n\t" // c * y - s * x
       "xvsubdp         %x[x3], %x[x3], 47      \n\t" // c * y - s * x

       "stxvd2x         40, 0,      %[x_ptr]   \n\t" // store x
       "stxvd2x         41, %[i16], %[x_ptr] \n\t"
       "stxvd2x         42, %[i32], %[x_ptr] \n\t"
       "stxvd2x         43, %[i48], %[x_ptr] \n\t"

       "stxvd2x         %x[x0], 0,      %[y_ptr]      \n\t" // store y
       "stxvd2x         %x[x1], %[i16], %[y_ptr]      \n\t"
       "stxvd2x         %x[x2], %[i32], %[y_ptr]      \n\t"
       "stxvd2x         %x[x3], %[i48], %[y_ptr]      \n\t"

  
     :
       [mem_x] "+m" (*(double (*)[2*n])x),
       [mem_y] "+m" (*(double (*)[2*n])y),
       [temp_n] "+&r" (n),     
       [x_ptr] "+&b"(x), [y_ptr]  "+&b"(y),
       [x0] "=wa" (t0), 
       [x1] "=wa" (t1), 
       [x2] "=wa" (t2), 
       [x3] "=wa" (t3), 
       [x4] "=wa" (t4), 
       [x5] "=wa" (t5),  
       [x6] "=wa" (t6), 
       [x7] "=wa" (t7)   
     :
       [cos] "d" (cosA),            
       [sin] "d" (sinA),            
       [i16] "b" (16),  
       [i32] "b" (32),  
       [i48] "b" (48)         
     :
       "cr0",
       "vs32","vs33","vs34","vs35","vs36","vs37",
       "vs40","vs41","vs42","vs43","vs44","vs45","vs46","vs47",
       "vs48","vs49","vs50","vs51"
     );
             
      return;

}

int CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x, FLOAT *y, BLASLONG inc_y, FLOAT c, FLOAT s)
{
    BLASLONG i=0;
    BLASLONG ix=0,iy=0;
    FLOAT temp[2];
    BLASLONG inc_x2;
    BLASLONG inc_y2;

    if ( n <= 0     )  return(0); 

    if ( (inc_x == 1) && (inc_y == 1) )
    {

        BLASLONG n1 = n & -4;
        if ( n1 > 0 )
        { 
            zrot_kernel_4(n1, x, y, c, s);
            i=n1; 
            ix=2*n1; 
        }

         while(i < n)
           {
                temp[0]   = c*x[ix]   + s*y[ix] ;
                temp[1]   = c*x[ix+1] + s*y[ix+1] ;
                y[ix]     = c*y[ix]   - s*x[ix] ;
                y[ix+1]   = c*y[ix+1] - s*x[ix+1] ;
                x[ix]     = temp[0] ;
                x[ix+1]   = temp[1] ;

                ix += 2 ; 
                i++ ;

            }

    }
    else
    {
        inc_x2 = 2 * inc_x ;
        inc_y2 = 2 * inc_y ;
        while(i < n)
        {
            temp[0]   = c*x[ix]   + s*y[iy] ;
            temp[1]   = c*x[ix+1] + s*y[iy+1] ;
            y[iy]     = c*y[iy]   - s*x[ix] ;
            y[iy+1]   = c*y[iy+1] - s*x[ix+1] ;
            x[ix]     = temp[0] ;
            x[ix+1]   = temp[1] ;

            ix += inc_x2 ;
            iy += inc_y2 ;
            i++ ;

        }

    }
    return(0);
 
}

 