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
static void dgemv_kernel_4x4( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y, FLOAT *alpha) __attribute__ ((noinline));

static void dgemv_kernel_4x4( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y, FLOAT *alpha)
{

	BLASLONG register i = 0;

	__asm__  __volatile__
	(
	"movsd    (%2), %%xmm12	 \n\t"	// x0 
	"movsd   8(%2), %%xmm13	 \n\t"	// x1 
	"movsd  16(%2), %%xmm14	 \n\t"	// x2 
	"movsd  24(%2), %%xmm15	 \n\t"	// x3 
	"shufpd $0,  %%xmm12, %%xmm12\n\t"	
	"shufpd $0,  %%xmm13, %%xmm13\n\t"	
	"shufpd $0,  %%xmm14, %%xmm14\n\t"	
	"shufpd $0,  %%xmm15, %%xmm15\n\t"	

	"movsd    (%8), %%xmm6	     \n\t"	// alpha 
	"shufpd $0,  %%xmm6 , %%xmm6 \n\t"	

	"movups             (%4,%0,8), %%xmm8          \n\t" 
	"movups           16(%4,%0,8), %%xmm0          \n\t" 
	"movups             (%5,%0,8), %%xmm9          \n\t" 
	"movups           16(%5,%0,8), %%xmm1          \n\t" 
	"movups             (%6,%0,8), %%xmm10         \n\t" 
	"movups           16(%6,%0,8), %%xmm2          \n\t" 
	"movups             (%7,%0,8), %%xmm11         \n\t" 
	"movups           16(%7,%0,8), %%xmm3          \n\t" 

        "addq		$4 , %0	  	 	       \n\t"
	"subq	        $4 , %1			       \n\t"		
	"jz		2f		       \n\t"

	".p2align 4				 \n\t"
	"1:				 \n\t"

	"xorpd           %%xmm4 , %%xmm4	 \n\t"
	"xorpd           %%xmm5 , %%xmm5	 \n\t"
	"movups	    -32(%3,%0,8), %%xmm7	 \n\t"	// 2 * y

	"mulpd		%%xmm12, %%xmm8		       \n\t"
	"mulpd		%%xmm12, %%xmm0		       \n\t"
	"addpd		%%xmm8 , %%xmm4		       \n\t"
	"addpd		%%xmm0 , %%xmm5		       \n\t"

	"movups             (%4,%0,8), %%xmm8          \n\t" 
	"movups           16(%4,%0,8), %%xmm0          \n\t" 

	"mulpd		%%xmm13, %%xmm9		       \n\t"
	"mulpd		%%xmm13, %%xmm1		       \n\t"
	"addpd		%%xmm9 , %%xmm4		       \n\t"
	"addpd		%%xmm1 , %%xmm5		       \n\t"

	"movups             (%5,%0,8), %%xmm9          \n\t" 
	"movups           16(%5,%0,8), %%xmm1          \n\t" 

	"mulpd		%%xmm14, %%xmm10	       \n\t"
	"mulpd		%%xmm14, %%xmm2 	       \n\t"
	"addpd		%%xmm10 , %%xmm4	       \n\t"
	"addpd		%%xmm2  , %%xmm5	       \n\t"

	"movups             (%6,%0,8), %%xmm10         \n\t" 
	"movups           16(%6,%0,8), %%xmm2          \n\t" 

	"mulpd		%%xmm15, %%xmm11	       \n\t"
	"mulpd		%%xmm15, %%xmm3 	       \n\t"
	"addpd		%%xmm11 , %%xmm4	       \n\t"
	"addpd		%%xmm3  , %%xmm5	       \n\t"

	"movups             (%7,%0,8), %%xmm11         \n\t" 
	"movups           16(%7,%0,8), %%xmm3          \n\t" 


	"mulpd		%%xmm6 , %%xmm4 	       \n\t" 
	"addpd		%%xmm7 , %%xmm4  	       \n\t"
	"movups	    -16(%3,%0,8), %%xmm7	 \n\t"	// 2 * y
	"movups         %%xmm4 ,  -32(%3,%0,8)	       \n\t"	// 2 * y

	"mulpd		%%xmm6 , %%xmm5 	       \n\t" 
	"addpd		%%xmm7 , %%xmm5  	       \n\t"
	"movups         %%xmm5 ,  -16(%3,%0,8)	       \n\t"	// 2 * y

        "addq		$4 , %0	  	 	       \n\t"
	"subq	        $4 , %1			       \n\t"		
	"jnz		1b		       \n\t"

	"2:				 \n\t"

	"xorpd           %%xmm4 , %%xmm4	 \n\t"
	"xorpd           %%xmm5 , %%xmm5	 \n\t"

	"mulpd		%%xmm12, %%xmm8		       \n\t"
	"addpd		%%xmm8 , %%xmm4		       \n\t"
	"mulpd		%%xmm13, %%xmm9		       \n\t"
	"addpd		%%xmm9 , %%xmm4		       \n\t"
	"mulpd		%%xmm14, %%xmm10	       \n\t"
	"addpd		%%xmm10 , %%xmm4	       \n\t"
	"mulpd		%%xmm15, %%xmm11	       \n\t"
	"addpd		%%xmm11 , %%xmm4	       \n\t"

	"mulpd		%%xmm12, %%xmm0		       \n\t"
	"addpd		%%xmm0 , %%xmm5		       \n\t"
	"mulpd		%%xmm13, %%xmm1		       \n\t"
	"addpd		%%xmm1 , %%xmm5		       \n\t"
	"mulpd		%%xmm14, %%xmm2 	       \n\t"
	"addpd		%%xmm2 , %%xmm5	       \n\t"
	"mulpd		%%xmm15, %%xmm3 	       \n\t"
	"addpd		%%xmm3 , %%xmm5	       \n\t"

	"movups	    -32(%3,%0,8), %%xmm7	 \n\t"	// 2 * y
	"mulpd		%%xmm6 , %%xmm4 	       \n\t" 
	"addpd		%%xmm7 , %%xmm4  	       \n\t"
	"movups         %%xmm4 ,  -32(%3,%0,8)	       \n\t"	// 2 * y

	"movups	    -16(%3,%0,8), %%xmm7	 \n\t"	// 2 * y
	"mulpd		%%xmm6 , %%xmm5 	       \n\t" 
	"addpd		%%xmm7 , %%xmm5  	       \n\t"
	"movups         %%xmm5 ,  -16(%3,%0,8)	       \n\t"	// 2 * y

	:
          "+r" (i),	// 0	
	  "+r" (n)  	// 1
        : 
          "r" (x),      // 2
          "r" (y),      // 3
          "r" (ap[0]),  // 4
          "r" (ap[1]),  // 5
          "r" (ap[2]),  // 6
          "r" (ap[3]),  // 7
          "r" (alpha)   // 8
	: "cc", 
	  "%xmm0", "%xmm1", "%xmm2", "%xmm3", 
	  "%xmm4", "%xmm5", "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11",
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

} 


