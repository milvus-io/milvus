/***************************************************************************
Copyright (c) 2014, The OpenBLAS Project
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

#define HAVE_KERNEL_4x4 1
static void dsymv_kernel_4x4( BLASLONG from, BLASLONG to, FLOAT **a, FLOAT *x, FLOAT *y, FLOAT *temp1, FLOAT *temp2) __attribute__ ((noinline));

static void dsymv_kernel_4x4(BLASLONG from, BLASLONG to, FLOAT **a, FLOAT *x, FLOAT *y, FLOAT *temp1, FLOAT *temp2)
{


	__asm__  __volatile__
	(
	"xorpd          %%xmm0 , %%xmm0              \n\t"	// temp2[0]
	"xorpd          %%xmm1 , %%xmm1              \n\t"	// temp2[1]
	"xorpd          %%xmm2 , %%xmm2              \n\t"	// temp2[2]
	"xorpd          %%xmm3 , %%xmm3              \n\t"	// temp2[3]
	"movsd	       (%8),    %%xmm4	             \n\t"	// temp1[0]
	"movsd        8(%8),    %%xmm5	             \n\t"	// temp1[1]
	"movsd       16(%8),    %%xmm6	             \n\t"	// temp1[2]
	"movsd       24(%8),    %%xmm7	             \n\t"	// temp1[3]
	"shufpd $0,  %%xmm4, %%xmm4                  \n\t"
	"shufpd $0,  %%xmm5, %%xmm5                  \n\t"
	"shufpd $0,  %%xmm6, %%xmm6                  \n\t"
	"shufpd $0,  %%xmm7, %%xmm7                  \n\t"

	".p2align 4		  		       \n\t"
	"1:				       \n\t"
	"movups	            (%4,%0,8), %%xmm12	       \n\t"	// 2 * a
	"movups	            (%2,%0,8), %%xmm8	       \n\t"	// 2 * x
	"movups		    %%xmm12  , %%xmm11	       \n\t"
	"movups	            (%3,%0,8), %%xmm9         \n\t"	// 2 * y
	"movups	            (%5,%0,8), %%xmm13	       \n\t"	// 2 * a

	"mulpd		    %%xmm4   , %%xmm11	       \n\t"    // temp1 * a
	"addpd		    %%xmm11  , %%xmm9	       \n\t"    // y += temp1 * a
	"mulpd		    %%xmm8   , %%xmm12	       \n\t"    // a * x
	"addpd		    %%xmm12  , %%xmm0 	       \n\t"    // temp2 += x * a

	"movups	            (%6,%0,8), %%xmm14	       \n\t"	// 2 * a
	"movups	            (%7,%0,8), %%xmm15	       \n\t"	// 2 * a

	"movups		    %%xmm13  , %%xmm11	       \n\t"
	"mulpd		    %%xmm5   , %%xmm11	       \n\t"    // temp1 * a
	"addpd		    %%xmm11  , %%xmm9	       \n\t"    // y += temp1 * a
	"mulpd		    %%xmm8   , %%xmm13	       \n\t"    // a * x
	"addpd		    %%xmm13  , %%xmm1 	       \n\t"    // temp2 += x * a

	"movups		    %%xmm14  , %%xmm11	       \n\t"
	"mulpd		    %%xmm6   , %%xmm11	       \n\t"    // temp1 * a
	"addpd		    %%xmm11  , %%xmm9	       \n\t"    // y += temp1 * a
	"mulpd		    %%xmm8   , %%xmm14	       \n\t"    // a * x
	"addpd		    %%xmm14  , %%xmm2 	       \n\t"    // temp2 += x * a

        "addq		$2 , %0	  	 	      \n\t"
	"movups		    %%xmm15  , %%xmm11	       \n\t"
	"mulpd		    %%xmm7   , %%xmm11	       \n\t"    // temp1 * a
	"addpd		    %%xmm11  , %%xmm9	       \n\t"    // y += temp1 * a
	"mulpd		    %%xmm8   , %%xmm15	       \n\t"    // a * x
	"addpd		    %%xmm15  , %%xmm3 	       \n\t"    // temp2 += x * a

	"movups             %%xmm9,-16(%3,%0,8)       \n\t"    // 2 * y

	"cmpq	        %0 , %1			      \n\t"		
	"jnz		1b		      \n\t"

	"movsd	       (%9),    %%xmm4	             \n\t"	// temp1[0]
	"movsd        8(%9),    %%xmm5	             \n\t"	// temp1[1]
	"movsd       16(%9),    %%xmm6	             \n\t"	// temp1[2]
	"movsd       24(%9),    %%xmm7	             \n\t"	// temp1[3]

	"haddpd        %%xmm0, %%xmm0  \n\t"
	"haddpd        %%xmm1, %%xmm1  \n\t"
	"haddpd        %%xmm2, %%xmm2  \n\t"
	"haddpd        %%xmm3, %%xmm3  \n\t"

	"addsd         %%xmm4, %%xmm0  \n\t"
	"addsd         %%xmm5, %%xmm1  \n\t"
	"addsd         %%xmm6, %%xmm2  \n\t"
	"addsd         %%xmm7, %%xmm3  \n\t"

	"movsd         %%xmm0 ,   (%9)		\n\t"	// save temp2
	"movsd         %%xmm1 ,  8(%9)		\n\t"	// save temp2
	"movsd         %%xmm2 , 16(%9)		\n\t"	// save temp2
	"movsd         %%xmm3 , 24(%9)		\n\t"	// save temp2

	:
        : 
          "r" (from),	// 0	
	  "r" (to),  	// 1
          "r" (x),      // 2
          "r" (y),      // 3
          "r" (a[0]),   // 4
          "r" (a[1]),   // 5
          "r" (a[2]),   // 6
          "r" (a[3]),   // 7
          "r" (temp1),  // 8
          "r" (temp2)   // 9
	: "cc", 
	  "%xmm0", "%xmm1", "%xmm2", "%xmm3", 
	  "%xmm4", "%xmm5", "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11", 
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

} 


