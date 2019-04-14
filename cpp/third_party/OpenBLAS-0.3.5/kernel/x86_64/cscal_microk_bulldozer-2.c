/***************************************************************************
Copyright (c) 2014-2015, The OpenBLAS Project
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


#define HAVE_KERNEL_16 1

static void cscal_kernel_16( BLASLONG n, FLOAT *alpha, FLOAT *x) __attribute__ ((noinline));

static void cscal_kernel_16( BLASLONG n, FLOAT *alpha, FLOAT *x)
{


	__asm__  __volatile__
	(
	"vbroadcastss    		(%2), %%xmm0		    \n\t"  // da_r	
	"vbroadcastss              4(%2), %%xmm1		    \n\t"  // da_i 	

	"addq	$128, %1				    \n\t"

	"vmovups	-128(%1), %%xmm4		    \n\t"
	"vmovups	-112(%1), %%xmm5		    \n\t"
	"vmovups	 -96(%1), %%xmm6		    \n\t"
	"vmovups	 -80(%1), %%xmm7		    \n\t"

	"vpermilps	$0xb1 , %%xmm4, %%xmm12		    \n\t"
	"vpermilps	$0xb1 , %%xmm5, %%xmm13		    \n\t"
	"vpermilps	$0xb1 , %%xmm6, %%xmm14 	    \n\t"
	"vpermilps	$0xb1 , %%xmm7, %%xmm15		    \n\t"

	"subq	        $8 , %0			            \n\t"		
	"jz	2f					    \n\t"

	".align 16				            \n\t"
	"1:				            	    \n\t"

	"prefetcht0     320(%1)				    \n\t"
	// ".align 2				            \n\t"

	"vmulps		%%xmm0, %%xmm4 , %%xmm8		    \n\t" // da_r*x0 , da_r *x1
	"vmovups	 -64(%1), %%xmm4		    \n\t"
	"vmulps		%%xmm0, %%xmm5 , %%xmm9		    \n\t"
	"vmovups	 -48(%1), %%xmm5		    \n\t"
	"vmulps		%%xmm0, %%xmm6 , %%xmm10	    \n\t" 
	"vmovups	 -32(%1), %%xmm6		    \n\t"
	"vmulps		%%xmm0, %%xmm7 , %%xmm11	    \n\t" 
	"vmovups	 -16(%1), %%xmm7		    \n\t"

	"vmulps		%%xmm1, %%xmm12, %%xmm12	    \n\t" // da_i*x1 , da_i *x0
	"vaddsubps	%%xmm12 , %%xmm8 , %%xmm8	    \n\t"
	"vmulps		%%xmm1, %%xmm13, %%xmm13	    \n\t" 
	"vaddsubps	%%xmm13 , %%xmm9 , %%xmm9	    \n\t"
	"vmulps		%%xmm1, %%xmm14, %%xmm14	    \n\t" 
	"vaddsubps	%%xmm14 , %%xmm10, %%xmm10	    \n\t"
	"vmulps		%%xmm1, %%xmm15, %%xmm15	    \n\t" 
	"vaddsubps	%%xmm15 , %%xmm11, %%xmm11	    \n\t"

	"vmovups	%%xmm8 , -128(%1)		    \n\t"
	"vmovups	%%xmm9 , -112(%1)		    \n\t"
	"vpermilps	$0xb1 , %%xmm4, %%xmm12		    \n\t"
	"vpermilps	$0xb1 , %%xmm5, %%xmm13		    \n\t"
	"vmovups	%%xmm10,  -96(%1)		    \n\t"
	"vmovups	%%xmm11,  -80(%1)		    \n\t"
	"vpermilps	$0xb1 , %%xmm6, %%xmm14 	    \n\t"
	"vpermilps	$0xb1 , %%xmm7, %%xmm15		    \n\t"

	"addq		$64  ,%1  	 	            \n\t"
	"subq	        $8 , %0			            \n\t"		
	"jnz		1b		             	    \n\t"

	"2:				            	    \n\t"


	"vmulps		%%xmm0, %%xmm4 , %%xmm8		    \n\t" // da_r*x0 , da_r *x1
	"vmulps		%%xmm0, %%xmm5 , %%xmm9		    \n\t"
	"vmulps		%%xmm0, %%xmm6 , %%xmm10	    \n\t" 
	"vmulps		%%xmm0, %%xmm7 , %%xmm11	    \n\t" 

	"vmulps		%%xmm1, %%xmm12, %%xmm12	    \n\t" // da_i*x1 , da_i *x0
	"vaddsubps	%%xmm12 , %%xmm8 , %%xmm8	    \n\t"
	"vmulps		%%xmm1, %%xmm13, %%xmm13	    \n\t" 
	"vaddsubps	%%xmm13 , %%xmm9 , %%xmm9	    \n\t"
	"vmulps		%%xmm1, %%xmm14, %%xmm14	    \n\t" 
	"vaddsubps	%%xmm14 , %%xmm10, %%xmm10	    \n\t"
	"vmulps		%%xmm1, %%xmm15, %%xmm15	    \n\t" 
	"vaddsubps	%%xmm15 , %%xmm11, %%xmm11	    \n\t"

	"vmovups	%%xmm8 , -128(%1)		    \n\t"
	"vmovups	%%xmm9 , -112(%1)		    \n\t"
	"vmovups	%%xmm10,  -96(%1)		    \n\t"
	"vmovups	%%xmm11,  -80(%1)		    \n\t"

	"vzeroupper					    \n\t"

	:
        : 
	  "r" (n),  	// 0
          "r" (x),      // 1
          "r" (alpha)   // 2
	: "cc", //"%0", "%1",
	  "%xmm0", "%xmm1", "%xmm2", "%xmm3", 
	  "%xmm4", "%xmm5", "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11", 
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

} 

static void cscal_kernel_16_zero_r( BLASLONG n, FLOAT *alpha, FLOAT *x) __attribute__ ((noinline));

static void cscal_kernel_16_zero_r( BLASLONG n, FLOAT *alpha, FLOAT *x)
{


	__asm__  __volatile__
	(
	"vxorps	           %%xmm0, %%xmm0, %%xmm0	    \n\t"
	"vbroadcastss              4(%2), %%xmm1		    \n\t"  // da_i 	

	"addq	$128, %1				    \n\t"

	"vmovups	-128(%1), %%xmm4		    \n\t"
	"vmovups	-112(%1), %%xmm5		    \n\t"
	"vmovups	 -96(%1), %%xmm6		    \n\t"
	"vmovups	 -80(%1), %%xmm7		    \n\t"

	"vpermilps	$0xb1 , %%xmm4, %%xmm12		    \n\t"
	"vpermilps	$0xb1 , %%xmm5, %%xmm13		    \n\t"
	"vpermilps	$0xb1 , %%xmm6, %%xmm14 	    \n\t"
	"vpermilps	$0xb1 , %%xmm7, %%xmm15		    \n\t"

	"subq	        $8 , %0			            \n\t"		
	"jz	2f					    \n\t"

	".align 16				            \n\t"
	"1:				            	    \n\t"

	//"prefetcht0     128(%1)				    \n\t"
	// ".align 2				            \n\t"

	"vmovups	 -64(%1), %%xmm4		    \n\t"
	"vmovups	 -48(%1), %%xmm5		    \n\t"
	"vmovups	 -32(%1), %%xmm6		    \n\t"
	"vmovups	 -16(%1), %%xmm7		    \n\t"

	"vmulps		%%xmm1, %%xmm12, %%xmm12	    \n\t" // da_i*x1 , da_i *x0
	"vaddsubps	%%xmm12 , %%xmm0 , %%xmm8	    \n\t"
	"vmulps		%%xmm1, %%xmm13, %%xmm13	    \n\t" 
	"vaddsubps	%%xmm13 , %%xmm0 , %%xmm9	    \n\t"
	"vmulps		%%xmm1, %%xmm14, %%xmm14	    \n\t" 
	"vaddsubps	%%xmm14 , %%xmm0 , %%xmm10	    \n\t"
	"vmulps		%%xmm1, %%xmm15, %%xmm15	    \n\t" 
	"vaddsubps	%%xmm15 , %%xmm0 , %%xmm11	    \n\t"

	"vmovups	%%xmm8 , -128(%1)		    \n\t"
	"vpermilps	$0xb1 , %%xmm4, %%xmm12		    \n\t"
	"vmovups	%%xmm9 , -112(%1)		    \n\t"
	"vpermilps	$0xb1 , %%xmm5, %%xmm13		    \n\t"
	"vmovups	%%xmm10,  -96(%1)		    \n\t"
	"vpermilps	$0xb1 , %%xmm6, %%xmm14 	    \n\t"
	"vmovups	%%xmm11,  -80(%1)		    \n\t"
	"vpermilps	$0xb1 , %%xmm7, %%xmm15		    \n\t"

	"addq		$64  ,%1  	 	            \n\t"
	"subq	        $8 , %0			            \n\t"		
	"jnz		1b		             	    \n\t"

	"2:				            	    \n\t"

	"vmulps		%%xmm1, %%xmm12, %%xmm12	    \n\t" // da_i*x1 , da_i *x0
	"vaddsubps	%%xmm12 , %%xmm0 , %%xmm8	    \n\t"
	"vmulps		%%xmm1, %%xmm13, %%xmm13	    \n\t" 
	"vaddsubps	%%xmm13 , %%xmm0 , %%xmm9	    \n\t"
	"vmulps		%%xmm1, %%xmm14, %%xmm14	    \n\t" 
	"vaddsubps	%%xmm14 , %%xmm0 , %%xmm10	    \n\t"
	"vmulps		%%xmm1, %%xmm15, %%xmm15	    \n\t" 
	"vaddsubps	%%xmm15 , %%xmm0 , %%xmm11	    \n\t"

	"vmovups	%%xmm8 , -128(%1)		    \n\t"
	"vmovups	%%xmm9 , -112(%1)		    \n\t"
	"vmovups	%%xmm10,  -96(%1)		    \n\t"
	"vmovups	%%xmm11,  -80(%1)		    \n\t"

	"vzeroupper					    \n\t"

	:
        : 
	  "r" (n),  	// 0
          "r" (x),      // 1
          "r" (alpha)   // 2
	: "cc", //"%0", "%1",
	  "%xmm0", "%xmm1", "%xmm2", "%xmm3", 
	  "%xmm4", "%xmm5", "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11", 
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

} 



static void cscal_kernel_16_zero_i( BLASLONG n, FLOAT *alpha, FLOAT *x) __attribute__ ((noinline));

static void cscal_kernel_16_zero_i( BLASLONG n, FLOAT *alpha, FLOAT *x)
{


	__asm__  __volatile__
	(
	"vbroadcastss    		(%2), %%xmm0		    \n\t"  // da_r	

	"addq	$128, %1				    \n\t"

	"vmovups	-128(%1), %%xmm4		    \n\t"
	"vmovups	-112(%1), %%xmm5		    \n\t"
	"vmovups	 -96(%1), %%xmm6		    \n\t"
	"vmovups	 -80(%1), %%xmm7		    \n\t"


	"subq	        $8 , %0			            \n\t"		
	"jz	2f					    \n\t"

	".align 16				            \n\t"
	"1:				            	    \n\t"

	//"prefetcht0     128(%1)				    \n\t"
	// ".align 2				            \n\t"

	"vmulps		%%xmm0, %%xmm4 , %%xmm8		    \n\t" // da_r*x0 , da_r *x1
	"vmovups	 -64(%1), %%xmm4		    \n\t"
	"vmulps		%%xmm0, %%xmm5 , %%xmm9		    \n\t"
	"vmovups	 -48(%1), %%xmm5		    \n\t"
	"vmulps		%%xmm0, %%xmm6 , %%xmm10	    \n\t" 
	"vmovups	 -32(%1), %%xmm6		    \n\t"
	"vmulps		%%xmm0, %%xmm7 , %%xmm11	    \n\t" 
	"vmovups	 -16(%1), %%xmm7		    \n\t"

	"vmovups	%%xmm8 , -128(%1)		    \n\t"
	"vmovups	%%xmm9 , -112(%1)		    \n\t"
	"vmovups	%%xmm10,  -96(%1)		    \n\t"
	"vmovups	%%xmm11,  -80(%1)		    \n\t"

	"addq		$64  ,%1  	 	            \n\t"
	"subq	        $8 , %0			            \n\t"		
	"jnz		1b		             	    \n\t"

	"2:				            	    \n\t"


	"vmulps		%%xmm0, %%xmm4 , %%xmm8		    \n\t" // da_r*x0 , da_r *x1
	"vmulps		%%xmm0, %%xmm5 , %%xmm9		    \n\t"
	"vmulps		%%xmm0, %%xmm6 , %%xmm10	    \n\t" 
	"vmulps		%%xmm0, %%xmm7 , %%xmm11	    \n\t" 

	"vmovups	%%xmm8 , -128(%1)		    \n\t"
	"vmovups	%%xmm9 , -112(%1)		    \n\t"
	"vmovups	%%xmm10,  -96(%1)		    \n\t"
	"vmovups	%%xmm11,  -80(%1)		    \n\t"

	"vzeroupper					    \n\t"

	:
        : 
	  "r" (n),  	// 0
          "r" (x),      // 1
          "r" (alpha)   // 2
	: "cc", //"%0", "%1",
	  "%xmm0", "%xmm1", "%xmm2", "%xmm3", 
	  "%xmm4", "%xmm5", "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11", 
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

} 


static void cscal_kernel_16_zero( BLASLONG n, FLOAT *alpha, FLOAT *x) __attribute__ ((noinline));

static void cscal_kernel_16_zero( BLASLONG n, FLOAT *alpha, FLOAT *x)
{


	__asm__  __volatile__
	(
	"vxorps	           %%xmm0, %%xmm0, %%xmm0	    \n\t"

	"addq	$128, %1				    \n\t"

	".align 16				            \n\t"
	"1:				            	    \n\t"

	//"prefetcht0     128(%1)				    \n\t"
	// ".align 2				            \n\t"

	"vmovups	%%xmm0 , -128(%1)		    \n\t"
	"vmovups	%%xmm0 , -112(%1)		    \n\t"
	"vmovups	%%xmm0 ,  -96(%1)		    \n\t"
	"vmovups	%%xmm0 ,  -80(%1)		    \n\t"

	"addq		$64  ,%1  	 	            \n\t"
	"subq	        $8 , %0			            \n\t"		
	"jnz		1b		             	    \n\t"

	"vzeroupper					    \n\t"

	:
        : 
	  "r" (n),  	// 0
          "r" (x),      // 1
          "r" (alpha)   // 2
	: "cc", //"%0", "%1",
	  "%xmm0", "%xmm1", "%xmm2", "%xmm3", 
	  "%xmm4", "%xmm5", "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11", 
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15",
	  "memory"
	);

} 



