/***************************************************************************
Copyright (c) 2013-2018, The OpenBLAS Project
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
#include <altivec.h>
#if defined(DOUBLE)

#define ABS fabs

#else

#define ABS fabsf

#endif

/**
 * Find  maximum index 
 * Warning: requirements n>0  and n % 32 == 0
 * @param n     
 * @param x     pointer to the vector
 * @param maxf  (out) maximum absolute value .( only for output )
 * @return  index 
 */
static BLASLONG diamax_kernel_32(BLASLONG n, FLOAT *x, FLOAT *maxf) {
    BLASLONG index;
    register __vector long long start = {1,0};
    register __vector long long temp_add_index = {2, 2}; 
    __asm__(
 

            "lxvd2x  44,      0,%[ptr_tmp] \n\t"
            "lxvd2x  45, %[i16],%[ptr_tmp] \n\t"
            "lxvd2x  46, %[i32],%[ptr_tmp] \n\t"
            "lxvd2x  47, %[i48],%[ptr_tmp] \n\t" 
            "lxvd2x  48, %[i64],%[ptr_tmp] \n\t"
            "lxvd2x  49, %[i80],%[ptr_tmp] \n\t"
            "lxvd2x  50, %[i96],%[ptr_tmp] \n\t"
            "lxvd2x  51,%[i112],%[ptr_tmp] \n\t"

            "xxlor  40,%x[start],%x[start]  \n\t" //{ 1,0} vs40 | v8 
            "vaddudm  9,8,%[adder]   \n\t" //{3,2} vs41 
            "xxlxor  37,37 ,37       \n\t" //v5 v37 index_count
            "vaddudm  10,9,%[adder]  \n\t" //{5,4} vs42
            "xxlxor  38 ,38 ,38      \n\t"  // v6 | vs38 vec_max_index
            "vaddudm  11,10,%[adder] \n\t" //{7,6} vs43
            "xxlxor  39,39,39        \n\t" //   vs39 vec_max_value
            "vaddudm 4,11,  %[adder] \n\t" // {9,8} -{8;8} vs36 | v4
            "xxspltd   36,36,0       \n\t"
    
            "xvabsdp  44, 44 \n\t"
            "xvabsdp  45, 45 \n\t"
            "xvabsdp  46, 46 \n\t"
            "xvabsdp  47, 47 \n\t"                         
            "xvabsdp  48, 48 \n\t"
            "xvabsdp  49, 49 \n\t"
            "xvabsdp  50, 50 \n\t"
            "xvabsdp  51, 51 \n\t"
  
            //jump first half forward 
            "b 2f  \n\t"

//===================================================================

            ".p2align   5            \n\t"

            "1: \n\t"
            "xvcmpgtdp  2,45,44  \n\t "
            "xvcmpgtdp  3,47,46  \n\t "
            "xvcmpgtdp  4,49,48  \n\t "
            "xvcmpgtdp  5,51,50  \n\t"

            "xxsel    32,40,41,2 \n\t"
            "xxsel     0,44,45,2 \n\t" 
            "xxsel    33,42,43,3 \n\t"
            "xxsel     1,46,47,3 \n\t" 
            "xxsel    34,40,41,4 \n\t"
            "xxsel    45,48,49,4 \n\t"
            "xxsel    35,42,43,5 \n\t"
            "xxsel    47,50,51,5 \n\t"

            "xvcmpgtdp 2, 1,0     \n\t"
            "xvcmpgtdp 3,47, 45   \n\t"

            "addi     %[ptr_tmp] ,%[ptr_tmp] , 128 \n\t" 
    
            "xxsel     32,32,33,2 \n\t"
            "xxsel       0 ,0,1,2 \n\t"
            "xxsel     34,34,35,3 \n\t"
            "xxsel     5,45,47,3 \n\t"
 
          
            //load next 64
            "lxvd2x  44,      0,%[ptr_tmp] \n\t"
            "lxvd2x  45, %[i16],%[ptr_tmp] \n\t"
    
            // for {second 8 elements } we have to add 8 to each so that it became {from 8 to 16}
            "vaddudm     2,2,4  \n\t" // vs34=vs34 + vs36{8,8} 
    
            "lxvd2x  46, %[i32],%[ptr_tmp] \n\t"
            "lxvd2x  47, %[i48],%[ptr_tmp] \n\t" 

            //choose bigger from first and second part
            "xvcmpgtdp 4,5 , 0    \n\t" 
            "xxsel     3, 0,5,4  \n\t"
            "xxsel     33,32,34,4  \n\t"

            //load next 64
            "lxvd2x  48, %[i64],%[ptr_tmp] \n\t"
            "lxvd2x  49, %[i80],%[ptr_tmp] \n\t"

            "vaddudm  1,1,5  \n\t"  //  get real index for first bigger  

            "lxvd2x  50, %[i96],%[ptr_tmp] \n\t"
            "lxvd2x  51,%[i112],%[ptr_tmp] \n\t"

            //compare with previous to get vec_max_index(v6 | vs38 ) and vec_max_value (vs39)   
            "xvcmpgtdp 2, 3,39    \n\t"
            "xxsel     39,39,3,2  \n\t"
            "xxsel     38,38,33,2  \n\t"
    
            //update index += 8 
            "vaddudm   5,5,4       \n\t"  

            "xvabsdp  44, 44 \n\t"
            "xvabsdp  45, 45 \n\t"
            "xvabsdp  46, 46 \n\t"
            "xvabsdp  47, 47 \n\t"   

            //update index += 8 
            "vaddudm   5,5,4       \n\t"  

            "xvabsdp  48, 48 \n\t"
            "xvabsdp  49, 49 \n\t"
            "xvabsdp  50, 50 \n\t"
            "xvabsdp  51, 51 \n\t"

//<-----------jump here from first load
             "2:                  \n\t"
    
            "xvcmpgtdp  2,45,44  \n\t "
            "xvcmpgtdp  3,47,46  \n\t "
            "xvcmpgtdp  4,49,48  \n\t "
            "xvcmpgtdp  5,51,50  \n\t"

            "xxsel    32,40,41,2 \n\t"
            "xxsel     0,44,45,2 \n\t" 
            "xxsel    33,42,43,3 \n\t"
            "xxsel     1,46,47,3 \n\t" 
            "xxsel    34,40,41,4 \n\t"
            "xxsel    45,48,49,4 \n\t"
            "xxsel    35,42,43,5 \n\t"
            "xxsel    47,50,51,5 \n\t"

            "xvcmpgtdp 2, 1,0     \n\t"
            "xvcmpgtdp 3,47, 45   \n\t"
            "xxsel     32,32,33,2 \n\t"
            "xxsel       0 ,0,1,2 \n\t"
            "xxsel     34,34,35,3 \n\t"
            "xxsel     5,45,47,3 \n\t"

            "addi     %[ptr_tmp] ,%[ptr_tmp] , 128 \n\t" 
            // for {second 8 elements } we have to add 8 to each so that it became {from 8 to 16}
            "vaddudm     2,2,4  \n\t" // vs34=vs34 + vs36{8,8} 
          
            //load next 64
            "lxvd2x  44,      0,%[ptr_tmp] \n\t"
            "lxvd2x  45, %[i16],%[ptr_tmp] \n\t"
            "lxvd2x  46, %[i32],%[ptr_tmp] \n\t"
            "lxvd2x  47, %[i48],%[ptr_tmp] \n\t" 

            //choose bigger from first and second part
            "xvcmpgtdp 4,5 , 0    \n\t" 
            "xxsel     3, 0,5,4  \n\t"
            "xxsel     33,32,34,4  \n\t"

            //load next 64
            "lxvd2x  48, %[i64],%[ptr_tmp] \n\t"
            "lxvd2x  49, %[i80],%[ptr_tmp] \n\t"

            "vaddudm  1,1,5  \n\t"  //  get real index for first bigger  

            "lxvd2x  50, %[i96],%[ptr_tmp] \n\t"
            "lxvd2x  51,%[i112],%[ptr_tmp] \n\t"

 

            //compare with previous to get vec_max_index(v6 | vs38 ) and vec_max_value (vs39)   
            "xvcmpgtdp 2, 3,39    \n\t"
            "xxsel     39,39,3,2  \n\t"
            "xxsel     38,38,33,2  \n\t"
    
            //update index += 8 
            "vaddudm   5,5,4       \n\t"  

            "xvabsdp  44, 44 \n\t"
            "xvabsdp  45, 45 \n\t"
            "xvabsdp  46, 46 \n\t"
            "xvabsdp  47, 47 \n\t"   

            //update index += 8 
            "vaddudm   5,5,4       \n\t"  

            "xvabsdp  48, 48 \n\t"
            "xvabsdp  49, 49 \n\t"
            "xvabsdp  50, 50 \n\t"
            "xvabsdp  51, 51 \n\t"

            //decrement n
            "addic.    %[n], %[n], -32 \n\t"
           
            //Loop back if >0
            "bgt+ 1b  \n\t"

//==============================================================================

           "xvcmpgtdp  2,45,44  \n\t "
            "xvcmpgtdp  3,47,46  \n\t "
            "xvcmpgtdp  4,49,48  \n\t "
            "xvcmpgtdp  5,51,50  \n\t"

            "xxsel    32,40,41,2 \n\t"
            "xxsel     0,44,45,2 \n\t" 
            "xxsel    33,42,43,3 \n\t"
            "xxsel     1,46,47,3 \n\t" 
            "xxsel    34,40,41,4 \n\t"
            "xxsel    45,48,49,4 \n\t"
            "xxsel    35,42,43,5 \n\t"
            "xxsel    47,50,51,5 \n\t"

            "xvcmpgtdp 2, 1,0     \n\t"
            "xvcmpgtdp 3,47, 45   \n\t"
  
    
            "xxsel     32,32,33,2 \n\t"
            "xxsel       0 ,0,1,2 \n\t"
            "xxsel     34,34,35,3 \n\t"
            "xxsel     5,45,47,3 \n\t" 
    
            // for {second 8 elements } we have to add 8 to each so that it became {from 8 to 16}
            "vaddudm     2,2,4  \n\t" // vs34=vs34 + vs36{8,8}  
            //choose bigger from first and second part
            "xvcmpgtdp 4,5 , 0    \n\t" 
            "xxsel     3, 0,5,4   \n\t"
            "xxsel     33,32,34,4 \n\t" 

            "vaddudm  1,1,5  \n\t"  //  get real index for first bigger   

            //compare with previous to get vec_max_index(v6 | vs38 ) and vec_max_value (vs39)   
            "xvcmpgtdp 2, 3,39    \n\t"
            "xxsel     39,39,3,2  \n\t"
            "xxsel     38,38,33,2  \n\t"

            ///////extract max value and max index from vector

            "xxspltd   32,38,1     \n\t"
            "xxspltd   40,39,1     \n\t"
            "xvcmpeqdp.  2, 40,39  \n\t"
    
            //cr6 0 bit set if all true, cr6=4*6+bit_ind=24,0011at CR(BI)==1, at=10 hint that it occurs rarely
             //0b001110=14
            "bc 14,24, 3f  \n\t" 
            "xvcmpgtdp  4, 40,39  \n\t"
            "xxsel    0,39,40,4           \n\t"
            "xxsel    1,38,32,4  \n\t"
            "stxsdx    0,0,%[ptr_maxf]     \n\t" 
            "b 4f    \n\t"

            "3:      \n\t" 
                //if elements value are equal then choose minimum index
            "xxspltd  0,40,0          \n\t"
            "vminud   0,0,6    \n\t"  //vs32 vs38
            "xxlor 1,32,32     \n\t"
            "stxsdx   0,0,%[ptr_maxf]  \n\t"
          

            "4:      \n\t"
            "mfvsrd   %[index],1 \n\t"

            : [maxf] "=m"(*maxf),[ptr_tmp] "+&b"(x),[index] "=r"(index), [n] "+&r"(n)
            : [mem] "m"(*(const double (*)[n])x), [ptr_x] "b"(x), [ptr_maxf] "b"(maxf) ,
            [i16] "b"(16), [i32] "b"(32), [i48] "b"(48),
            [i64] "b"(64), [i80] "b"(80), [i96] "b"(96), [i112] "b"(112),
            [start] "v"(start),  [adder] "v"(temp_add_index)
            : "cc", "vs0", "vs1","vs2","vs3", "vs4","vs5","vs32", "vs33", "vs34", "vs35", "vs36",
            "vs37", "vs38", "vs39", "vs40", "vs41", "vs42", "vs43", "vs44", "vs45", "vs46", "vs47", "vs48", "vs49", "vs50", "vs51"
            );

 
    return index;

}

BLASLONG CNAME(BLASLONG n, FLOAT *x, BLASLONG inc_x) {
    BLASLONG i = 0;
    BLASLONG j = 0;
    FLOAT maxf = 0.0;
    BLASLONG max = 0;

    if (n <= 0 || inc_x <= 0) return (max);

    if (inc_x == 1) {

        BLASLONG n1 = n & -32;
        if (n1 > 0) {

            max = diamax_kernel_32(n1, x, &maxf);

            i = n1;
        }

        while (i < n) {
            if (ABS(x[i]) > maxf) {
                max = i;
                maxf = ABS(x[i]);
            }
            i++;
        }
        return (max + 1);

    } else {

        BLASLONG n1 = n & -4;
        while (j < n1) {

            if (ABS(x[i]) > maxf) {
                max = j;
                maxf = ABS(x[i]);
            }
            if (ABS(x[i + inc_x]) > maxf) {
                max = j + 1;
                maxf = ABS(x[i + inc_x]);
            }
            if (ABS(x[i + 2 * inc_x]) > maxf) {
                max = j + 2;
                maxf = ABS(x[i + 2 * inc_x]);
            }
            if (ABS(x[i + 3 * inc_x]) > maxf) {
                max = j + 3;
                maxf = ABS(x[i + 3 * inc_x]);
            }

            i += inc_x * 4;

            j += 4;

        }


        while (j < n) {
            if (ABS(x[i]) > maxf) {
                max = j;
                maxf = ABS(x[i]);
            }
            i += inc_x;
            j++;
        }
        return (max + 1);
    }
}
