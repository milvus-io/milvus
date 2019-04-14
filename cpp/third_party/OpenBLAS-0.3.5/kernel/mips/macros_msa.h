/*******************************************************************************
Copyright (c) 2016, The OpenBLAS Project
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
*******************************************************************************/

#ifndef __MACROS_MSA_H__
#define __MACROS_MSA_H__

#include <stdint.h>
#include <msa.h>

#define ENABLE_PREFETCH

#ifdef ENABLE_PREFETCH
inline static void prefetch_load_lf(unsigned char *src)
{
    __asm__ __volatile__("pref   0,  0(%[src])   \n\t" : : [src] "r" (src));
}

#define PREFETCH(PTR)   prefetch_load_lf((unsigned char *)(PTR));

#define STRNG(X) #X
#define PREF_OFFSET(src_ptr, offset)		      \
    __asm__ __volatile__("pref 0, " STRNG(offset) "(%[src]) \n\t" : : [src] "r" (src_ptr));

#else
#define PREFETCH(PTR)
#define PREF_OFFSET(src_ptr, offset)
#endif

#define LD_W(RTYPE, psrc) *((RTYPE *)(psrc))
#define LD_SP(...) LD_W(v4f32, __VA_ARGS__)

#define LD_D(RTYPE, psrc) *((RTYPE *)(psrc))
#define LD_DP(...) LD_D(v2f64, __VA_ARGS__)

#define ST_W(RTYPE, in, pdst) *((RTYPE *)(pdst)) = (in)
#define ST_SP(...) ST_W(v4f32, __VA_ARGS__)

#define ST_D(RTYPE, in, pdst) *((RTYPE *)(pdst)) = (in)
#define ST_DP(...) ST_D(v2f64, __VA_ARGS__)

#define COPY_FLOAT_TO_VECTOR(a) ( {                \
    v4f32  out;                                    \
    out = __msa_cast_to_vector_float(a);           \
    out = (v4f32) __msa_splati_w((v4i32) out, 0);  \
    out;                                           \
} )

#define COPY_DOUBLE_TO_VECTOR(a) ( {               \
    v2f64  out;                                    \
    out = __msa_cast_to_vector_double(a);          \
    out = (v2f64) __msa_splati_d((v2i64) out, 0);  \
    out;                                           \
} )

/* Description : Load 2 variables with stride
   Arguments   : Inputs  - psrc, stride
                 Outputs - out0, out1
*/
#define LD_GP2_INC(psrc, stride, out0, out1)  \
{                                             \
    out0 = *(psrc);                           \
    (psrc) += stride;                         \
    out1 = *(psrc);                           \
    (psrc) += stride;                         \
}

#define LD_GP3_INC(psrc, stride, out0,     \
                   out1, out2)             \
{                                          \
    LD_GP2_INC(psrc, stride, out0, out1);  \
    out2 = *(psrc);                        \
    (psrc) += stride;                      \
}

#define LD_GP4_INC(psrc, stride, out0,     \
                   out1, out2, out3)       \
{                                          \
    LD_GP2_INC(psrc, stride, out0, out1);  \
    LD_GP2_INC(psrc, stride, out2, out3);  \
}

#define LD_GP5_INC(psrc, stride, out0,      \
                   out1, out2, out3, out4)  \
{                                           \
    LD_GP2_INC(psrc, stride, out0, out1);   \
    LD_GP2_INC(psrc, stride, out2, out3);   \
    out4 = *(psrc);                         \
    (psrc) += stride;                       \
}

#define LD_GP6_INC(psrc, stride, out0,     \
                   out1, out2, out3,       \
                   out4, out5)             \
{                                          \
    LD_GP2_INC(psrc, stride, out0, out1);  \
    LD_GP2_INC(psrc, stride, out2, out3);  \
    LD_GP2_INC(psrc, stride, out4, out5);  \
}

#define LD_GP7_INC(psrc, stride, out0,     \
                   out1, out2, out3,       \
                   out4, out5, out6)       \
{                                          \
    LD_GP2_INC(psrc, stride, out0, out1);  \
    LD_GP2_INC(psrc, stride, out2, out3);  \
    LD_GP2_INC(psrc, stride, out4, out5);  \
    out6 = *(psrc);                        \
    (psrc) += stride;                      \
}

#define LD_GP8_INC(psrc, stride, out0, out1, out2,     \
                   out3, out4, out5, out6, out7)       \
{                                                      \
    LD_GP4_INC(psrc, stride, out0, out1, out2, out3);  \
    LD_GP4_INC(psrc, stride, out4, out5, out6, out7);  \
}

/* Description : Load 2 vectors of single precision floating point elements with stride
   Arguments   : Inputs  - psrc, stride
                 Outputs - out0, out1
                 Return Type - single precision floating point
*/
#define LD_SP2(psrc, stride, out0, out1)  \
{                                         \
    out0 = LD_SP((psrc));                 \
    out1 = LD_SP((psrc) + stride);        \
}

#define LD_SP4(psrc, stride, out0, out1, out2, out3)  \
{                                                     \
    LD_SP2(psrc, stride, out0, out1)                  \
    LD_SP2(psrc + 2 * stride, stride, out2, out3)     \
}

#define LD_SP2_INC(psrc, stride, out0, out1)  \
{                                             \
    out0 = LD_SP((psrc));                     \
    (psrc) += stride;                         \
    out1 = LD_SP((psrc));                     \
    (psrc) += stride;                         \
}

#define LD_SP3_INC(psrc, stride, out0,     \
                   out1, out2)             \
{                                          \
    LD_SP2_INC(psrc, stride, out0, out1);  \
    out2 = LD_SP((psrc));                  \
    (psrc) += stride;                      \
}

#define LD_SP4_INC(psrc, stride, out0,     \
                   out1, out2, out3)       \
{                                          \
    LD_SP2_INC(psrc, stride, out0, out1);  \
    LD_SP2_INC(psrc, stride, out2, out3);  \
}

#define LD_SP5_INC(psrc, stride, out0,      \
                   out1, out2, out3, out4)  \
{                                           \
    LD_SP2_INC(psrc, stride, out0, out1);   \
    LD_SP2_INC(psrc, stride, out2, out3);   \
    out4 = LD_SP((psrc));                   \
    (psrc) += stride;                       \
}

#define LD_SP6_INC(psrc, stride, out0,     \
                   out1, out2, out3,       \
                   out4, out5)             \
{                                          \
    LD_SP2_INC(psrc, stride, out0, out1);  \
    LD_SP2_INC(psrc, stride, out2, out3);  \
    LD_SP2_INC(psrc, stride, out4, out5);  \
}

#define LD_SP7_INC(psrc, stride, out0,     \
                   out1, out2, out3,       \
                   out4, out5, out6)       \
{                                          \
    LD_SP2_INC(psrc, stride, out0, out1);  \
    LD_SP2_INC(psrc, stride, out2, out3);  \
    LD_SP2_INC(psrc, stride, out4, out5);  \
    out6 = LD_SP((psrc));                  \
    (psrc) += stride;                      \
}

#define LD_SP8_INC(psrc, stride, out0, out1, out2,     \
                   out3, out4, out5, out6, out7)       \
{                                                      \
    LD_SP4_INC(psrc, stride, out0, out1, out2, out3);  \
    LD_SP4_INC(psrc, stride, out4, out5, out6, out7);  \
}

#define LD_SP16_INC(psrc, stride, out0, out1, out2,      \
                    out3, out4, out5, out6, out7, out8,  \
                    out9, out10, out11, out12, out13,    \
                    out14, out15)                        \
{                                                        \
    LD_SP8_INC(psrc, stride, out0, out1, out2,           \
               out3, out4, out5, out6, out7);            \
    LD_SP8_INC(psrc, stride, out8, out9, out10,          \
               out11, out12, out13, out14, out15);       \
}

/* Description : Load 2 vectors of double precision floating point elements with stride
   Arguments   : Inputs  - psrc, stride
                 Outputs - out0, out1
                 Return Type - double precision floating point
*/
#define LD_DP2(psrc, stride, out0, out1)  \
{                                         \
    out0 = LD_DP((psrc));                 \
    out1 = LD_DP((psrc) + stride);        \
}

#define LD_DP4(psrc, stride, out0, out1, out2, out3)  \
{                                                     \
    LD_DP2(psrc, stride, out0, out1)                  \
    LD_DP2(psrc + 2 * stride, stride, out2, out3)     \
}

#define LD_DP2_INC(psrc, stride, out0, out1)  \
{                                             \
    out0 = LD_DP(psrc);                       \
    (psrc) += stride;                         \
    out1 = LD_DP(psrc);                       \
    (psrc) += stride;                         \
}

#define LD_DP3_INC(psrc, stride, out0,     \
                   out1, out2)             \
{                                          \
    LD_DP2_INC(psrc, stride, out0, out1);  \
    out2 = LD_DP((psrc));                  \
    (psrc) += stride;                      \
}

#define LD_DP4_INC(psrc, stride, out0,     \
                   out1, out2, out3)       \
{                                          \
    LD_DP2_INC(psrc, stride, out0, out1);  \
    LD_DP2_INC(psrc, stride, out2, out3);  \
}

#define LD_DP5_INC(psrc, stride, out0,      \
                   out1, out2, out3, out4)  \
{                                           \
    LD_DP2_INC(psrc, stride, out0, out1);   \
    LD_DP2_INC(psrc, stride, out2, out3);   \
    out4 = LD_DP((psrc));                   \
    (psrc) += stride;                       \
}

#define LD_DP6_INC(psrc, stride, out0,     \
                   out1, out2, out3,       \
                   out4, out5)             \
{                                          \
    LD_DP2_INC(psrc, stride, out0, out1);  \
    LD_DP2_INC(psrc, stride, out2, out3);  \
    LD_DP2_INC(psrc, stride, out4, out5);  \
}

#define LD_DP7_INC(psrc, stride, out0,     \
                   out1, out2, out3,       \
                   out4, out5, out6)       \
{                                          \
    LD_DP2_INC(psrc, stride, out0, out1);  \
    LD_DP2_INC(psrc, stride, out2, out3);  \
    LD_DP2_INC(psrc, stride, out4, out5);  \
    out6 = LD_DP((psrc));                  \
    (psrc) += stride;                      \
}

#define LD_DP8_INC(psrc, stride, out0, out1, out2,     \
                   out3, out4, out5, out6, out7)       \
{                                                      \
    LD_DP4_INC(psrc, stride, out0, out1, out2, out3);  \
    LD_DP4_INC(psrc, stride, out4, out5, out6, out7);  \
}

#define LD_DP16_INC(psrc, stride, out0, out1, out2,      \
                    out3, out4, out5, out6, out7, out8,  \
                    out9, out10, out11, out12, out13,    \
                    out14, out15)                        \
{                                                        \
    LD_DP8_INC(psrc, stride, out0, out1, out2,           \
               out3, out4, out5, out6, out7);            \
    LD_DP8_INC(psrc, stride, out8, out9, out10,          \
               out11, out12, out13, out14, out15);       \
}

/* Description : Store GP variable with stride
   Arguments   : Inputs - in0, in1, pdst, stride
   Details     : Store 4 single precision floating point elements from 'in0' to (pdst)
                 Store 4 single precision floating point elements from 'in1' to (pdst + stride)
*/
#define ST_GP2_INC(in0, in1,      \
                   pdst, stride)  \
{                                 \
    *(pdst) = in0;                \
    (pdst) += stride;             \
    *(pdst) = in1;                \
    (pdst) += stride;             \
}

#define ST_GP3_INC(in0, in1, in2,        \
                   pdst, stride)         \
{                                        \
    ST_GP2_INC(in0, in1, pdst, stride);  \
    *(pdst) = in2;                       \
    (pdst) += stride;                    \
}

#define ST_GP4_INC(in0, in1, in2, in3,   \
                   pdst, stride)         \
{                                        \
    ST_GP2_INC(in0, in1, pdst, stride);  \
    ST_GP2_INC(in2, in3, pdst, stride);  \
}

#define ST_GP5_INC(in0, in1, in2, in3,   \
                   in4, pdst, stride)    \
{                                        \
    ST_GP2_INC(in0, in1, pdst, stride);  \
    ST_GP2_INC(in2, in3, pdst, stride);  \
    *(pdst) = in4;                       \
    (pdst) += stride;                    \
}

#define ST_GP6_INC(in0, in1, in2, in3,     \
                   in4, in5, pdst, stride) \
{                                          \
    ST_GP2_INC(in0, in1, pdst, stride);    \
    ST_GP2_INC(in2, in3, pdst, stride);    \
    ST_GP2_INC(in4, in5, pdst, stride);    \
}

#define ST_GP7_INC(in0, in1, in2, in3, in4,  \
                   in5, in6, pdst, stride)   \
{                                            \
    ST_GP2_INC(in0, in1, pdst, stride);      \
    ST_GP2_INC(in2, in3, pdst, stride);      \
    ST_GP2_INC(in4, in5, pdst, stride);      \
    *(pdst) = in6;                           \
    (pdst) += stride;                        \
}

#define ST_GP8_INC(in0, in1, in2, in3, in4, in5,   \
                   in6, in7, pdst, stride)         \
{                                                  \
    ST_GP4_INC(in0, in1, in2, in3, pdst, stride);  \
    ST_GP4_INC(in4, in5, in6, in7, pdst, stride);  \
}

/* Description : Store vectors of single precision floating point elements with stride
   Arguments   : Inputs - in0, in1, pdst, stride
   Details     : Store 4 single precision floating point elements from 'in0' to (pdst)
                 Store 4 single precision floating point elements from 'in1' to (pdst + stride)
*/
#define ST_SP2(in0, in1, pdst, stride)  \
{                                       \
    ST_SP(in0, (pdst));                 \
    ST_SP(in1, (pdst) + stride);        \
}

#define ST_SP4(in0, in1, in2, in3, pdst, stride)    \
{                                                   \
    ST_SP2(in0, in1, (pdst), stride);               \
    ST_SP2(in2, in3, (pdst + 2 * stride), stride);  \
}

#define ST_SP8(in0, in1, in2, in3, in4, in5, in6, in7, pdst, stride)  \
{                                                                     \
    ST_SP4(in0, in1, in2, in3, (pdst), stride);                       \
    ST_SP4(in4, in5, in6, in7, (pdst + 4 * stride), stride);          \
}

#define ST_SP2_INC(in0, in1, pdst, stride)  \
{                                           \
    ST_SP(in0, (pdst));                     \
    (pdst) += stride;                       \
    ST_SP(in1, (pdst));                     \
    (pdst) += stride;                       \
}

#define ST_SP3_INC(in0, in1, in2,        \
                   pdst, stride)         \
{                                        \
    ST_SP2_INC(in0, in1, pdst, stride);  \
    ST_SP(in2, (pdst));                  \
    (pdst) += stride;                    \
}

#define ST_SP4_INC(in0, in1, in2, in3,   \
                   pdst, stride)         \
{                                        \
    ST_SP2_INC(in0, in1, pdst, stride);  \
    ST_SP2_INC(in2, in3, pdst, stride);  \
}

#define ST_SP5_INC(in0, in1, in2, in3,   \
                   in4, pdst, stride)    \
{                                        \
    ST_SP2_INC(in0, in1, pdst, stride);  \
    ST_SP2_INC(in2, in3, pdst, stride);  \
    ST_SP(in4, (pdst));                  \
    (pdst) += stride;                    \
}

#define ST_SP6_INC(in0, in1, in2, in3,     \
                   in4, in5, pdst, stride) \
{                                          \
    ST_SP2_INC(in0, in1, pdst, stride);    \
    ST_SP2_INC(in2, in3, pdst, stride);    \
    ST_SP2_INC(in4, in5, pdst, stride);    \
}

#define ST_SP7_INC(in0, in1, in2, in3, in4,  \
                   in5, in6, pdst, stride)   \
{                                            \
    ST_SP2_INC(in0, in1, pdst, stride);      \
    ST_SP2_INC(in2, in3, pdst, stride);      \
    ST_SP2_INC(in4, in5, pdst, stride);      \
    ST_SP(in6, (pdst));                      \
    (pdst) += stride;                        \
}

#define ST_SP8_INC(in0, in1, in2, in3, in4, in5,   \
                   in6, in7, pdst, stride)         \
{                                                  \
    ST_SP4_INC(in0, in1, in2, in3, pdst, stride);  \
    ST_SP4_INC(in4, in5, in6, in7, pdst, stride);  \
}

#define ST_SP16_INC(in0, in1, in2, in3, in4, in5, in6,  \
                    in7, in8, in9, in10, in11, in12,    \
                    in13, in14, in15, pdst, stride)     \
{                                                       \
    ST_SP8_INC(in0, in1, in2, in3, in4, in5, in6,       \
               in7, pdst, stride);                      \
    ST_SP8_INC(in8, in9, in10, in11, in12, in13, in14,  \
               in15, pdst, stride);                     \
}

/* Description : Store vectors of double precision floating point elements with stride
   Arguments   : Inputs - in0, in1, pdst, stride
   Details     : Store 2 double precision floating point elements from 'in0' to (pdst)
                 Store 2 double precision floating point elements from 'in1' to (pdst + stride)
*/
#define ST_DP2(in0, in1, pdst, stride)  \
{                                       \
    ST_DP(in0, (pdst));                 \
    ST_DP(in1, (pdst) + stride);        \
}

#define ST_DP4(in0, in1, in2, in3, pdst, stride)   \
{                                                  \
    ST_DP2(in0, in1, (pdst), stride);              \
    ST_DP2(in2, in3, (pdst) + 2 * stride, stride); \
}

#define ST_DP8(in0, in1, in2, in3, in4, in5, in6, in7, pdst, stride)  \
{                                                                     \
    ST_DP4(in0, in1, in2, in3, (pdst), stride);                       \
    ST_DP4(in4, in5, in6, in7, (pdst) + 4 * stride, stride);          \
}

#define ST_DP2_INC(in0, in1, pdst, stride)  \
{                                           \
    ST_DP(in0, (pdst));                     \
    (pdst) += stride;                       \
    ST_DP(in1, (pdst));                     \
    (pdst) += stride;                       \
}

#define ST_DP3_INC(in0, in1, in2,        \
                   pdst, stride)         \
{                                        \
    ST_DP2_INC(in0, in1, pdst, stride);  \
    ST_DP(in2, (pdst));                  \
    (pdst) += stride;                    \
}

#define ST_DP4_INC(in0, in1, in2, in3,   \
                   pdst, stride)         \
{                                        \
    ST_DP2_INC(in0, in1, pdst, stride);  \
    ST_DP2_INC(in2, in3, pdst, stride);  \
}

#define ST_DP5_INC(in0, in1, in2, in3,   \
                   in4, pdst, stride)    \
{                                        \
    ST_DP2_INC(in0, in1, pdst, stride);  \
    ST_DP2_INC(in2, in3, pdst, stride);  \
    ST_DP(in4, (pdst));                  \
    (pdst) += stride;                    \
}

#define ST_DP6_INC(in0, in1, in2, in3,     \
                   in4, in5, pdst, stride) \
{                                          \
    ST_DP2_INC(in0, in1, pdst, stride);    \
    ST_DP2_INC(in2, in3, pdst, stride);    \
    ST_DP2_INC(in4, in5, pdst, stride);    \
}

#define ST_DP7_INC(in0, in1, in2, in3, in4,  \
                   in5, in6, pdst, stride)   \
{                                            \
    ST_DP2_INC(in0, in1, pdst, stride);      \
    ST_DP2_INC(in2, in3, pdst, stride);      \
    ST_DP2_INC(in4, in5, pdst, stride);      \
    ST_DP(in6, (pdst));                      \
    (pdst) += stride;                        \
}

#define ST_DP8_INC(in0, in1, in2, in3, in4, in5,   \
                   in6, in7, pdst, stride)         \
{                                                  \
    ST_DP4_INC(in0, in1, in2, in3, pdst, stride);  \
    ST_DP4_INC(in4, in5, in6, in7, pdst, stride);  \
}

#define ST_DP16_INC(in0, in1, in2, in3, in4, in5, in6,  \
                    in7, in8, in9, in10, in11, in12,    \
                    in13, in14, in15, pdst, stride)     \
{                                                       \
    ST_DP8_INC(in0, in1, in2, in3, in4, in5, in6,       \
               in7, pdst, stride);                      \
    ST_DP8_INC(in8, in9, in10, in11, in12, in13, in14,  \
               in15, pdst, stride);                     \
}

/* Description : shuffle elements in vector as shf_val
   Arguments   : Inputs  - in0, in1
                 Outputs - out0, out1
                 Return Type - as per RTYPE
*/
#define SHF_W2(RTYPE, in0, in1, out0, out1, shf_val)   \
{                                                      \
    out0 = (RTYPE) __msa_shf_w((v4i32) in0, shf_val);  \
    out1 = (RTYPE) __msa_shf_w((v4i32) in1, shf_val);  \
}
#define SHF_W2_SP(...) SHF_W2(v4f32, __VA_ARGS__)
#define SHF_W2_DP(...) SHF_W2(v2f64, __VA_ARGS__)

#define SHF_W3(RTYPE, in0, in1, in2, out0, out1, out2,  \
               shf_val)                                 \
{                                                       \
    out0 = (RTYPE) __msa_shf_w((v4i32) in0, shf_val);   \
    out1 = (RTYPE) __msa_shf_w((v4i32) in1, shf_val);   \
    out2 = (RTYPE) __msa_shf_w((v4i32) in2, shf_val);   \
}
#define SHF_W3_SP(...) SHF_W3(v4f32, __VA_ARGS__)

#define SHF_W4(RTYPE, in0, in1, in2, in3,           \
               out0, out1, out2, out3, shf_val)     \
{                                                   \
    SHF_W2(RTYPE, in0, in1, out0, out1, shf_val);   \
    SHF_W2(RTYPE, in2, in3, out2, out3, shf_val);   \
}
#define SHF_W4_SP(...) SHF_W4(v4f32, __VA_ARGS__)
#define SHF_W4_DP(...) SHF_W4(v2f64, __VA_ARGS__)

/* Description : Interleave both left and right half of input vectors
   Arguments   : Inputs  - in0, in1
                 Outputs - out0, out1
                 Return Type - as per RTYPE
   Details     : Right half of byte elements from 'in0' and 'in1' are
                 interleaved and written to 'out0'
*/
#define ILVRL_W2(RTYPE, in0, in1, out0, out1)               \
{                                                           \
    out0 = (RTYPE) __msa_ilvr_w((v4i32) in0, (v4i32) in1);  \
    out1 = (RTYPE) __msa_ilvl_w((v4i32) in0, (v4i32) in1);  \
}
#define ILVRL_W2_SW(...) ILVRL_W2(v4i32, __VA_ARGS__)
#define ILVRL_W2_SP(...) ILVRL_W2(v4f32, __VA_ARGS__)

#define ILVRL_D2(RTYPE, in0, in1, out0, out1)               \
{                                                           \
    out0 = (RTYPE) __msa_ilvr_d((v2i64) in0, (v2i64) in1);  \
    out1 = (RTYPE) __msa_ilvl_d((v2i64) in0, (v2i64) in1);  \
}
#define ILVRL_D2_SP(...) ILVRL_D2(v4f32, __VA_ARGS__)
#define ILVRL_D2_DP(...) ILVRL_D2(v2f64, __VA_ARGS__)

/* Description : Indexed word element values are replicated to all
                 elements in output vector
   Arguments   : Inputs  - in, stidx
                 Outputs - out0, out1
                 Return Type - as per RTYPE
   Details     : 'stidx' element value from 'in' vector is replicated to all
                 elements in 'out0' vector
                 'stidx + 1' element value from 'in' vector is replicated to all
                 elements in 'out1' vector
                 Valid index range for word operation is 0-3
*/
#define SPLATI_W2(RTYPE, in, stidx, out0, out1)            \
{                                                          \
    out0 = (RTYPE) __msa_splati_w((v4i32) in, stidx);      \
    out1 = (RTYPE) __msa_splati_w((v4i32) in, (stidx+1));  \
}
#define SPLATI_W2_SP(...) SPLATI_W2(v4f32, __VA_ARGS__)

#define SPLATI_W4(RTYPE, in, out0, out1, out2, out3)  \
{                                                     \
    SPLATI_W2(RTYPE, in, 0, out0, out1);              \
    SPLATI_W2(RTYPE, in, 2, out2, out3);              \
}
#define SPLATI_W4_SP(...) SPLATI_W4(v4f32, __VA_ARGS__)

#define SPLATI_D2(RTYPE, in, out0, out1)           \
{                                                  \
    out0 = (RTYPE) __msa_splati_d((v2i64) in, 0);  \
    out1 = (RTYPE) __msa_splati_d((v2i64) in, 1);  \
}
#define SPLATI_D2_DP(...) SPLATI_D2(v2f64, __VA_ARGS__)

/* Description : Pack even double word elements of vector pairs
   Arguments   : Inputs  - in0, in1, in2, in3
                 Outputs - out0, out1
                 Return Type - as per RTYPE
   Details     : Even double word elements of 'in0' are copied to the left half
                 of 'out0' & even double word elements of 'in1' are copied to
                 the right half of 'out0'.
*/
#define PCKEV_D2(RTYPE, in0, in1, in2, in3, out0, out1)      \
{                                                            \
    out0 = (RTYPE) __msa_pckev_d((v2i64) in0, (v2i64) in1);  \
    out1 = (RTYPE) __msa_pckev_d((v2i64) in2, (v2i64) in3);  \
}
#define PCKEV_D2_SP(...) PCKEV_D2(v4f32, __VA_ARGS__)
#define PCKEV_D2_SD(...) PCKEV_D2(v2f64, __VA_ARGS__)

#define PCKEV_D3(RTYPE, in0, in1, in2, in3, in4, in5,        \
                 out0, out1, out2)                           \
{                                                            \
    out0 = (RTYPE) __msa_pckev_d((v2i64) in0, (v2i64) in1);  \
    out1 = (RTYPE) __msa_pckev_d((v2i64) in2, (v2i64) in3);  \
    out2 = (RTYPE) __msa_pckev_d((v2i64) in4, (v2i64) in5);  \
}
#define PCKEV_D3_SP(...) PCKEV_D3(v4f32, __VA_ARGS__)

#define PCKEV_D4(RTYPE, in0, in1, in2, in3, in4, in5, in6, in7,  \
                 out0, out1, out2, out3)                         \
{                                                                \
    PCKEV_D2(RTYPE, in0, in1, in2, in3, out0, out1);             \
    PCKEV_D2(RTYPE, in4, in5, in6, in7, out2, out3);             \
}
#define PCKEV_D4_SP(...) PCKEV_D4(v4f32, __VA_ARGS__)

/* Description : pack both even and odd half of input vectors
   Arguments   : Inputs  - in0, in1
                 Outputs - out0, out1
                 Return Type - as per RTYPE
   Details     : Even double word elements of 'in0' and 'in1' are copied to the
                 'out0' & odd double word elements of 'in0' and 'in1' are
                 copied to the 'out1'.
*/
#define PCKEVOD_W2(RTYPE, in0, in1, out0, out1)              \
{                                                            \
    out0 = (RTYPE) __msa_pckev_w((v4i32) in0, (v4i32) in1);  \
    out1 = (RTYPE) __msa_pckod_w((v4i32) in0, (v4i32) in1);  \
}
#define PCKEVOD_W2_SP(...) PCKEVOD_W2(v4f32, __VA_ARGS__)

#define PCKEVOD_D2(RTYPE, in0, in1, out0, out1)              \
{                                                            \
    out0 = (RTYPE) __msa_pckev_d((v2i64) in0, (v2i64) in1);  \
    out1 = (RTYPE) __msa_pckod_d((v2i64) in0, (v2i64) in1);  \
}
#define PCKEVOD_D2_DP(...) PCKEVOD_D2(v2f64, __VA_ARGS__)

/* Description : Multiplication of pairs of vectors
   Arguments   : Inputs  - in0, in1, in2, in3
                 Outputs - out0, out1
   Details     : Each element from 'in0' is multiplied with elements from 'in1'
                 and the result is written to 'out0'
*/
#define MUL2(in0, in1, in2, in3, out0, out1)  \
{                                             \
    out0 = in0 * in1;                         \
    out1 = in2 * in3;                         \
}
#define MUL3(in0, in1, in2, in3, in4, in5,  \
             out0, out1, out2)              \
{                                           \
    out0 = in0 * in1;                       \
    out1 = in2 * in3;                       \
    out2 = in4 * in5;                       \
}
#define MUL4(in0, in1, in2, in3, in4, in5, in6, in7,  \
             out0, out1, out2, out3)                  \
{                                                     \
    MUL2(in0, in1, in2, in3, out0, out1);             \
    MUL2(in4, in5, in6, in7, out2, out3);             \
}

/* Description : Multiplication of pairs of vectors and added in output
   Arguments   : Inputs  - in0, in1, vec, out0, out1
                 Outputs - out0, out1
   Details     : Each element from 'in0' is multiplied with elements from 'vec'
                 and the result is added to 'out0'
*/
#define FMADD2(in0, in1, vec, inout0, inout1)  \
{                                              \
    inout0 += in0 * vec;                       \
    inout1 += in1 * vec;                       \
}
#define FMADD3(in0, in1, in2, vec,      \
               inout0, inout1, inout2)  \
{                                       \
    inout0 += in0 * vec;                \
    inout1 += in1 * vec;                \
    inout2 += in2 * vec;                \
}
#define FMADD4(in0, in1, in2, in3, vec,         \
               inout0, inout1, inout2, inout3)  \
{                                               \
    FMADD2(in0, in1, vec, inout0, inout1);      \
    FMADD2(in2, in3, vec, inout2, inout3);      \
}

/* Description : Addition of 2 pairs of variables
   Arguments   : Inputs  - in0, in1, in2, in3
                 Outputs - out0, out1
   Details     : Each element in 'in0' is added to 'in1' and result is written
                 to 'out0'.
*/
#define ADD2(in0, in1, in2, in3, out0, out1)  \
{                                             \
    out0 = in0 + in1;                         \
    out1 = in2 + in3;                         \
}
#define ADD3(in0, in1, in2, in3, in4, in5,  \
             out0, out1, out2)              \
{                                           \
    out0 = in0 + in1;                       \
    out1 = in2 + in3;                       \
    out2 = in4 + in5;                       \
}
#define ADD4(in0, in1, in2, in3, in4, in5, in6, in7,  \
             out0, out1, out2, out3)                  \
{                                                     \
    ADD2(in0, in1, in2, in3, out0, out1);             \
    ADD2(in4, in5, in6, in7, out2, out3);             \
}

/* Description : Transpose 4x4 block with word elements in vectors
   Arguments   : Inputs  - in0, in1, in2, in3
                 Outputs - out0, out1, out2, out3
                 Return Type - as per RTYPE
*/
#define TRANSPOSE4x4_W(RTYPE, in0, in1, in2, in3,  \
                       out0, out1, out2, out3)     \
{                                                  \
    v4i32 s0_m, s1_m, s2_m, s3_m;                  \
                                                   \
    ILVRL_W2_SW(in1, in0, s0_m, s1_m);             \
    ILVRL_W2_SW(in3, in2, s2_m, s3_m);             \
    ILVRL_D2(RTYPE, s2_m, s0_m, out0, out1);       \
    ILVRL_D2(RTYPE, s3_m, s1_m, out2, out3);       \
}
#define TRANSPOSE4x4_SP_SP(...) TRANSPOSE4x4_W(v4f32, __VA_ARGS__)

#endif  /* __MACROS_MSA_H__ */
