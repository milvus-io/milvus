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
static void ssymv_kernel_4x4( BLASLONG n, FLOAT *a0, FLOAT *a1, FLOAT *a2, FLOAT *a3, FLOAT *x, FLOAT *y, FLOAT *temp1, FLOAT *temp2) __attribute__ ((noinline));

static void ssymv_kernel_4x4(BLASLONG n, FLOAT *a0, FLOAT *a1, FLOAT *a2, FLOAT *a3, FLOAT *x, FLOAT *y, FLOAT *temp1, FLOAT *temp2)
{

	BLASLONG register i = 0;

	__asm__  __volatile__
	(
	"vxorps		%%xmm0 , %%xmm0 , %%xmm0     \n\t"	// temp2[0]
	"vxorps		%%xmm1 , %%xmm1 , %%xmm1     \n\t"	// temp2[1]
	"vxorps		%%xmm2 , %%xmm2 , %%xmm2     \n\t"	// temp2[2]
	"vxorps		%%xmm3 , %%xmm3 , %%xmm3     \n\t"	// temp2[3]
	"vbroadcastss	(%8),    %%xmm4	             \n\t"	// temp1[0]
	"vbroadcastss  4(%8),    %%xmm5	             \n\t"	// temp1[1]
	"vbroadcastss  8(%8),    %%xmm6	             \n\t"	// temp1[1]
	"vbroadcastss 12(%8),    %%xmm7	             \n\t"	// temp1[1]

	"xorq		%0,%0			     \n\t"

	".align 16				     \n\t"
	"1:				     \n\t"

	"vmovups	(%2,%0,4), %%xmm8	           \n\t"  // 4 * x
	"vmovups	(%3,%0,4), %%xmm9	           \n\t"  // 4 * y

	"vmovups	(%4,%0,4), %%xmm12	           \n\t"  // 4 * a
	"vmovups	(%5,%0,4), %%xmm13	           \n\t"  // 4 * a

	"vfmaddps	%%xmm0 , %%xmm8, %%xmm12 , %%xmm0  \n\t"  // temp2 += x * a
	"vfmaddps	%%xmm9 , %%xmm4, %%xmm12 , %%xmm9  \n\t"  // y     += temp1 * a

	"vfmaddps	%%xmm1 , %%xmm8, %%xmm13 , %%xmm1  \n\t"  // temp2 += x * a
	"vmovups	(%6,%0,4), %%xmm14	           \n\t"  // 4 * a
	"vfmaddps	%%xmm9 , %%xmm5, %%xmm13 , %%xmm9  \n\t"  // y     += temp1 * a

	"vfmaddps	%%xmm2 , %%xmm8, %%xmm14 , %%xmm2  \n\t"  // temp2 += x * a
	"vmovups	(%7,%0,4), %%xmm15	           \n\t"  // 4 * a
	"vfmaddps	%%xmm9 , %%xmm6, %%xmm14 , %%xmm9  \n\t"  // y     += temp1 * a

	"vfmaddps	%%xmm3 , %%xmm8, %%xmm15 , %%xmm3  \n\t"  // temp2 += x * a
	"vfmaddps	%%xmm9 , %%xmm7, %%xmm15 , %%xmm9  \n\t"  // y     += temp1 * a

	"vmovups	%%xmm9 , (%3,%0,4)		   \n\t"

	"addq		$4 , %0	  	 	      \n\t"
	"subq	        $4 , %1			      \n\t"		
	"jnz		1b		      \n\t"

	"vhaddps        %%xmm0, %%xmm0, %%xmm0  \n\t"
	"vhaddps        %%xmm1, %%xmm1, %%xmm1  \n\t"
	"vhaddps        %%xmm2, %%xmm2, %%xmm2  \n\t"
	"vhaddps        %%xmm3, %%xmm3, %%xmm3  \n\t"
	"vhaddps        %%xmm0, %%xmm0, %%xmm0  \n\t"
	"vhaddps        %%xmm1, %%xmm1, %%xmm1  \n\t"
	"vhaddps        %%xmm2, %%xmm2, %%xmm2  \n\t"
	"vhaddps        %%xmm3, %%xmm3, %%xmm3  \n\t"

	"vmovss         %%xmm0 ,  (%9)		\n\t"	// save temp2
	"vmovss         %%xmm1 , 4(%9)		\n\t"	// save temp2
	"vmovss         %%xmm2 , 8(%9)		\n\t"	// save temp2
	"vmovss         %%xmm3 ,12(%9)		\n\t"	// save temp2

	:
        : 
          "r" (i),	// 0	
	  "r" (n),  	// 1
          "r" (x),      // 2
          "r" (y),      // 3
          "r" (a0),     // 4
          "r" (a1),     // 5
          "r" (a2),     // 6
          "r" (a3),     // 7
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


