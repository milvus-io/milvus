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

static void   zrot_kernel_16(BLASLONG n, FLOAT *x, FLOAT *y, FLOAT cosA, FLOAT sinA)
{
          __asm__  (
            "pfd    2, 0(%[ptr_x]) \n\t"
            "pfd    2, 0(%[ptr_y]) \n\t"
            "lgdr   %%r1,%[cos]    \n\t"
            "vlvgp  %%v0,%%r1,%%r1 \n\t"
            "lgdr   %%r1,%[sin]    \n\t"
            "vlvgp  %%v1,%%r1,%%r1 \n\t"
            "sllg   %[tmp],%[tmp],4    \n\t"
            "xgr    %%r1,%%r1     \n\t"
            ".align 16 \n\t"
            "1:     \n\t"
            "pfd    2, 256(%%r1,%[ptr_x]) \n\t"
            "pfd    2, 256(%%r1,%[ptr_y]) \n\t"
            "vl     %%v24,  0(%%r1,%[ptr_x]) \n\t" 
            "vl     %%v25, 16(%%r1,%[ptr_x]) \n\t" 
            "vl     %%v26, 32(%%r1,%[ptr_x]) \n\t" 
            "vl     %%v27, 48(%%r1,%[ptr_x]) \n\t" 
            "vl     %%v16,  0(%%r1,%[ptr_y]) \n\t" 
            "vl     %%v17, 16(%%r1,%[ptr_y]) \n\t" 
            "vl     %%v18, 32(%%r1,%[ptr_y]) \n\t" 
            "vl     %%v19, 48(%%r1,%[ptr_y]) \n\t"  
           
            "vfmdb  %%v28,%%v24,%%v0 \n\t"
            "vfmdb  %%v29,%%v25,%%v0 \n\t"
            "vfmdb  %%v20,%%v24,%%v1 \n\t" /* yn=x*s  */
            "vfmdb  %%v21,%%v25,%%v1 \n\t" /* yn=x*s  */
            "vfmdb  %%v30,%%v26,%%v0 \n\t"
            "vfmdb  %%v22,%%v26,%%v1 \n\t" /* yn=x*s  */
            "vfmdb  %%v31,%%v27,%%v0 \n\t"
            "vfmdb  %%v23,%%v27,%%v1 \n\t" /* yn=x*s  */
            /* 2nd parts*/
            "vfmadb %%v28,%%v16,%%v1,%%v28 \n\t"  
            "vfmsdb %%v20,%%v16,%%v0,%%v20 \n\t"  /* yn=y*c-yn */ 
            "vfmadb %%v29,%%v17,%%v1,%%v29 \n\t"  
            "vfmsdb %%v21,%%v17,%%v0,%%v21 \n\t"  /* yn=y*c-yn */ 
            "vfmadb %%v30,%%v18,%%v1,%%v30 \n\t" 
            "vfmsdb %%v22,%%v18,%%v0,%%v22 \n\t"  /* yn=y*c-yn */ 
            "vfmadb %%v31,%%v19,%%v1,%%v31 \n\t" 
            "vfmsdb %%v23,%%v19,%%v0,%%v23 \n\t"  /* yn=y*c-yn */

            "vst    %%v28, 0(%%r1,%[ptr_x]) \n\t" 
            "vst    %%v29, 16(%%r1,%[ptr_x]) \n\t" 
            "vst    %%v30, 32(%%r1,%[ptr_x]) \n\t" 
            "vst    %%v31, 48(%%r1,%[ptr_x]) \n\t" 
            "vst    %%v20, 0(%%r1,%[ptr_y]) \n\t" 
            "vst    %%v21, 16(%%r1,%[ptr_y]) \n\t" 
            "vst    %%v22, 32(%%r1,%[ptr_y]) \n\t" 
            "vst    %%v23, 48(%%r1,%[ptr_y]) \n\t"  
           
            "vl     %%v24, 64(%%r1,%[ptr_x]) \n\t" 
            "vl     %%v25, 80(%%r1,%[ptr_x]) \n\t" 
            "vl     %%v26, 96(%%r1,%[ptr_x]) \n\t" 
            "vl     %%v27,112(%%r1,%[ptr_x]) \n\t" 
            "vl     %%v16, 64(%%r1,%[ptr_y]) \n\t" 
            "vl     %%v17, 80(%%r1,%[ptr_y]) \n\t" 
            "vl     %%v18, 96(%%r1,%[ptr_y]) \n\t" 
            "vl     %%v19,112(%%r1,%[ptr_y]) \n\t"  
           
            "vfmdb  %%v28,%%v24,%%v0 \n\t"
            "vfmdb  %%v29,%%v25,%%v0 \n\t"
            "vfmdb  %%v20,%%v24,%%v1 \n\t" /* yn=x*s  */
            "vfmdb  %%v21,%%v25,%%v1 \n\t" /* yn=x*s  */
            "vfmdb  %%v30,%%v26,%%v0 \n\t"
            "vfmdb  %%v22,%%v26,%%v1 \n\t" /* yn=x*s  */
            "vfmdb  %%v31,%%v27,%%v0 \n\t"
            "vfmdb  %%v23,%%v27,%%v1 \n\t" /* yn=x*s  */
            /* 2nd parts*/
            "vfmadb %%v28,%%v16,%%v1,%%v28 \n\t"  
            "vfmsdb %%v20,%%v16,%%v0,%%v20 \n\t"  /* yn=y*c-yn */ 
            "vfmadb %%v29,%%v17,%%v1,%%v29 \n\t"  
            "vfmsdb %%v21,%%v17,%%v0,%%v21 \n\t"  /* yn=y*c-yn */ 
            "vfmadb %%v30,%%v18,%%v1,%%v30 \n\t" 
            "vfmsdb %%v22,%%v18,%%v0,%%v22 \n\t"  /* yn=y*c-yn */ 
            "vfmadb %%v31,%%v19,%%v1,%%v31 \n\t" 
            "vfmsdb %%v23,%%v19,%%v0,%%v23 \n\t"  /* yn=y*c-yn */

            "vst    %%v28, 64(%%r1,%[ptr_x])  \n\t" 
            "vst    %%v29, 80(%%r1,%[ptr_x])  \n\t" 
            "vst    %%v30, 96(%%r1,%[ptr_x])  \n\t" 
            "vst    %%v31, 112(%%r1,%[ptr_x]) \n\t" 
            "vst    %%v20, 64(%%r1,%[ptr_y])  \n\t" 
            "vst    %%v21, 80(%%r1,%[ptr_y])  \n\t" 
            "vst    %%v22, 96(%%r1,%[ptr_y])  \n\t" 
            "vst    %%v23, 112(%%r1,%[ptr_y]) \n\t"
           
            "vl     %%v24, 128(%%r1,%[ptr_x]) \n\t" 
            "vl     %%v25, 144(%%r1,%[ptr_x]) \n\t" 
            "vl     %%v26, 160(%%r1,%[ptr_x]) \n\t" 
            "vl     %%v27, 176(%%r1,%[ptr_x]) \n\t" 
            "vl     %%v16, 128(%%r1,%[ptr_y]) \n\t" 
            "vl     %%v17, 144(%%r1,%[ptr_y]) \n\t" 
            "vl     %%v18, 160(%%r1,%[ptr_y]) \n\t" 
            "vl     %%v19, 176(%%r1,%[ptr_y]) \n\t"  
           
            "vfmdb  %%v28,%%v24,%%v0 \n\t"
            "vfmdb  %%v29,%%v25,%%v0 \n\t"
            "vfmdb  %%v20,%%v24,%%v1 \n\t" /* yn=x*s  */
            "vfmdb  %%v21,%%v25,%%v1 \n\t" /* yn=x*s  */
            "vfmdb  %%v30,%%v26,%%v0 \n\t"
            "vfmdb  %%v22,%%v26,%%v1 \n\t" /* yn=x*s  */
            "vfmdb  %%v31,%%v27,%%v0 \n\t"
            "vfmdb  %%v23,%%v27,%%v1 \n\t" /* yn=x*s  */
            /* 2nd parts*/
            "vfmadb %%v28,%%v16,%%v1,%%v28 \n\t"  
            "vfmsdb %%v20,%%v16,%%v0,%%v20 \n\t"  /* yn=y*c-yn */ 
            "vfmadb %%v29,%%v17,%%v1,%%v29 \n\t"  
            "vfmsdb %%v21,%%v17,%%v0,%%v21 \n\t"  /* yn=y*c-yn */ 
            "vfmadb %%v30,%%v18,%%v1,%%v30 \n\t" 
            "vfmsdb %%v22,%%v18,%%v0,%%v22 \n\t"  /* yn=y*c-yn */ 
            "vfmadb %%v31,%%v19,%%v1,%%v31 \n\t" 
            "vfmsdb %%v23,%%v19,%%v0,%%v23 \n\t"  /* yn=y*c-yn */

            "vst    %%v28, 128(%%r1,%[ptr_x]) \n\t" 
            "vst    %%v29, 144(%%r1,%[ptr_x]) \n\t" 
            "vst    %%v30, 160(%%r1,%[ptr_x]) \n\t" 
            "vst    %%v31, 176(%%r1,%[ptr_x]) \n\t" 
            "vst    %%v20, 128(%%r1,%[ptr_y]) \n\t" 
            "vst    %%v21, 144(%%r1,%[ptr_y]) \n\t" 
            "vst    %%v22, 160(%%r1,%[ptr_y]) \n\t" 
            "vst    %%v23, 176(%%r1,%[ptr_y]) \n\t"  
           
            "vl     %%v24, 192(%%r1,%[ptr_x]) \n\t" 
            "vl     %%v25, 208(%%r1,%[ptr_x]) \n\t" 
            "vl     %%v26, 224(%%r1,%[ptr_x]) \n\t" 
            "vl     %%v27, 240(%%r1,%[ptr_x]) \n\t" 
            "vl     %%v16, 192(%%r1,%[ptr_y]) \n\t" 
            "vl     %%v17, 208(%%r1,%[ptr_y]) \n\t" 
            "vl     %%v18, 224(%%r1,%[ptr_y]) \n\t" 
            "vl     %%v19, 240(%%r1,%[ptr_y]) \n\t"  
           
            "vfmdb  %%v28,%%v24,%%v0 \n\t"
            "vfmdb  %%v29,%%v25,%%v0 \n\t"
            "vfmdb  %%v20,%%v24,%%v1 \n\t" /* yn=x*s  */
            "vfmdb  %%v21,%%v25,%%v1 \n\t" /* yn=x*s  */
            "vfmdb  %%v30,%%v26,%%v0 \n\t"
            "vfmdb  %%v22,%%v26,%%v1 \n\t" /* yn=x*s  */
            "vfmdb  %%v31,%%v27,%%v0 \n\t"
            "vfmdb  %%v23,%%v27,%%v1 \n\t" /* yn=x*s  */
            /* 2nd parts*/
            "vfmadb %%v28,%%v16,%%v1,%%v28 \n\t"  
            "vfmsdb %%v20,%%v16,%%v0,%%v20 \n\t"  /* yn=y*c-yn */ 
            "vfmadb %%v29,%%v17,%%v1,%%v29 \n\t"  
            "vfmsdb %%v21,%%v17,%%v0,%%v21 \n\t"  /* yn=y*c-yn */ 
            "vfmadb %%v30,%%v18,%%v1,%%v30 \n\t" 
            "vfmsdb %%v22,%%v18,%%v0,%%v22 \n\t"  /* yn=y*c-yn */ 
            "vfmadb %%v31,%%v19,%%v1,%%v31 \n\t" 
            "vfmsdb %%v23,%%v19,%%v0,%%v23 \n\t"  /* yn=y*c-yn */

            "vst    %%v28, 192(%%r1,%[ptr_x]) \n\t" 
            "vst    %%v29, 208(%%r1,%[ptr_x]) \n\t" 
            "vst    %%v30, 224(%%r1,%[ptr_x]) \n\t" 
            "vst    %%v31, 240(%%r1,%[ptr_x]) \n\t" 
            "vst    %%v20, 192(%%r1,%[ptr_y]) \n\t" 
            "vst    %%v21, 208(%%r1,%[ptr_y]) \n\t" 
            "vst    %%v22, 224(%%r1,%[ptr_y]) \n\t" 
            "vst    %%v23, 240(%%r1,%[ptr_y]) \n\t"

            "la    %%r1,256(%%r1) \n\t"
            "clgrjl %%r1,%[tmp],1b        \n\t" 
            : [mem_x] "+m" (*(double (*)[2*n])x),
              [mem_y] "+m" (*(double (*)[2*n])y),
              [tmp] "+&r"(n)
            : [ptr_x] "a"(x), [ptr_y]  "a"(y),[cos] "f"(cosA),[sin] "f"(sinA) 
            : "cc","r1" ,"v0","v1","v16",
            "v17","v18","v19","v20","v21","v22","v23","v24","v25","v26","v27","v28","v29","v30","v31"
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

        BLASLONG n1 = n & -16;
        if ( n1 > 0 )
        { 
            zrot_kernel_16(n1, x, y, c, s);
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

