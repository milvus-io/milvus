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
static void dgemv_kernel_4x4( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y) __attribute__ ((noinline));

static void dgemv_kernel_4x4( BLASLONG n, FLOAT **ap, FLOAT *x, FLOAT *y)
{

	BLASLONG register i = 0;

	__asm__  __volatile__
	(
	"vzeroupper			         \n\t"
	"vxorpd		%%ymm4 , %%ymm4, %%ymm4  \n\t"
	"vxorpd		%%ymm5 , %%ymm5, %%ymm5  \n\t"
	"vxorpd		%%ymm6 , %%ymm6, %%ymm6  \n\t"
	"vxorpd		%%ymm7 , %%ymm7, %%ymm7  \n\t"

        "testq          $0x04, %1                      \n\t"
        "jz             2f                    \n\t"

	"vmovups	(%2,%0,8), %%ymm12       \n\t"	// 4 * x

	"vfmadd231pd   (%4,%0,8), %%ymm12, %%ymm4      \n\t" 
	"vfmadd231pd   (%5,%0,8), %%ymm12, %%ymm5      \n\t" 
	"vfmadd231pd   (%6,%0,8), %%ymm12, %%ymm6      \n\t" 
	"vfmadd231pd   (%7,%0,8), %%ymm12, %%ymm7      \n\t" 

        "addq		$4 , %0	  	 	      \n\t"
	"subq	        $4 , %1			      \n\t"		

        "2:                                  \n\t"

        "cmpq           $0, %1                         \n\t"
        "je             3f                      \n\t"

	// ".align 16				 \n\t"
	"1:				 \n\t"
	// "prefetcht0	 384(%2,%0,8)		 \n\t"
	"vmovups	(%2,%0,8), %%ymm12       \n\t"	// 4 * x
	"vmovups      32(%2,%0,8), %%ymm13       \n\t"	// 4 * x

	// "prefetcht0	 384(%4,%0,8)		       \n\t"
	"vfmadd231pd   (%4,%0,8), %%ymm12, %%ymm4      \n\t" 
	"vfmadd231pd   (%5,%0,8), %%ymm12, %%ymm5      \n\t" 
	// "prefetcht0	 384(%5,%0,8)		       \n\t"
	"vfmadd231pd   (%6,%0,8), %%ymm12, %%ymm6      \n\t" 
	"vfmadd231pd   (%7,%0,8), %%ymm12, %%ymm7      \n\t" 
	// "prefetcht0	 384(%6,%0,8)		       \n\t"
	"vfmadd231pd 32(%4,%0,8), %%ymm13, %%ymm4      \n\t" 
	"vfmadd231pd 32(%5,%0,8), %%ymm13, %%ymm5      \n\t" 
        "addq		$8 , %0	  	 	       \n\t"
	// "prefetcht0	 384(%7,%0,8)		       \n\t"
	"vfmadd231pd -32(%6,%0,8), %%ymm13, %%ymm6     \n\t" 
	"subq	        $8 , %1			       \n\t"		
	"vfmadd231pd -32(%7,%0,8), %%ymm13, %%ymm7     \n\t" 

	"jnz		1b		      \n\t"

        "3:                                   \n\t"

	"vextractf128   $1 , %%ymm4, %%xmm12	      \n\t"
	"vextractf128   $1 , %%ymm5, %%xmm13	      \n\t"
	"vextractf128   $1 , %%ymm6, %%xmm14	      \n\t"
	"vextractf128   $1 , %%ymm7, %%xmm15	      \n\t"

	"vaddpd		%%xmm4, %%xmm12, %%xmm4       \n\t"
	"vaddpd		%%xmm5, %%xmm13, %%xmm5       \n\t"
	"vaddpd		%%xmm6, %%xmm14, %%xmm6       \n\t"
	"vaddpd		%%xmm7, %%xmm15, %%xmm7       \n\t"

        "vhaddpd        %%xmm4, %%xmm4, %%xmm4  \n\t"
        "vhaddpd        %%xmm5, %%xmm5, %%xmm5  \n\t"
        "vhaddpd        %%xmm6, %%xmm6, %%xmm6  \n\t"
        "vhaddpd        %%xmm7, %%xmm7, %%xmm7  \n\t"

        "vmovsd         %%xmm4,    (%3)         \n\t"
        "vmovsd         %%xmm5,   8(%3)         \n\t"
        "vmovsd         %%xmm6,  16(%3)         \n\t"
        "vmovsd         %%xmm7,  24(%3)         \n\t"

	"vzeroupper			 \n\t"

	:
          "+r" (i),	// 0	
	  "+r" (n)  	// 1
	:
          "r" (x),      // 2
          "r" (y),      // 3
          "r" (ap[0]),  // 4
          "r" (ap[1]),  // 5
          "r" (ap[2]),  // 6
          "r" (ap[3])   // 7
	: "cc", 
	  "%xmm4", "%xmm5", "%xmm6", "%xmm7",
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

} 


