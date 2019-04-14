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

#define HAVE_KERNEL_8 1
static void caxpy_kernel_8( BLASLONG n, FLOAT *x, FLOAT *y , FLOAT *alpha) __attribute__ ((noinline));

static void caxpy_kernel_8( BLASLONG n, FLOAT *x, FLOAT *y, FLOAT *alpha)
{

#if !defined(CONJ)
	FLOAT mvec[8] = { -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0 };
#else
	FLOAT mvec[8] = { 1.0, -1.0, 1.0, -1.0, 1.0, -1.0, 1.0, -1.0 };
#endif

	BLASLONG register i = 0;

	__asm__  __volatile__
	(
	"vzeroupper					    \n\t"
	"vbroadcastss		(%4), %%ymm0		    \n\t"  // real part of alpha
	"vbroadcastss	       4(%4), %%ymm1		    \n\t"  // imag part of alpha
#if !defined(CONJ)
	"vmulps		(%5), %%ymm1 , %%ymm1		    \n\t"
#else
	"vmulps		(%5), %%ymm0 , %%ymm0		    \n\t"
#endif

	".p2align 4				            \n\t"
	"1:				            \n\t"

	"vmovups        (%2,%0,4), %%ymm5                   \n\t" // 4 complex values from x
	".p2align 1					    \n\t"
	"vmovups      32(%2,%0,4), %%ymm7                   \n\t" // 4 complex values from x
	"vmovups      64(%2,%0,4), %%ymm9                   \n\t" // 4 complex values from x
	"vmovups      96(%2,%0,4), %%ymm11                  \n\t" // 4 complex values from x

	"vpermilps	$0xb1 , %%ymm5 , %%ymm4 	    \n\t"  // exchange real and imag part
	"vpermilps	$0xb1 , %%ymm7 , %%ymm6 	    \n\t"  // exchange real and imag part
	"vpermilps	$0xb1 , %%ymm9 , %%ymm8 	    \n\t"  // exchange real and imag part
	"vpermilps	$0xb1 , %%ymm11, %%ymm10 	    \n\t"  // exchange real and imag part

	"vmulps	        %%ymm5 , %%ymm0 , %%ymm5	    \n\t"
	"vmulps	        %%ymm7 , %%ymm0 , %%ymm7	    \n\t"
	"vmulps	        %%ymm9 , %%ymm0 , %%ymm9	    \n\t"
	"vmulps	        %%ymm11, %%ymm0 , %%ymm11           \n\t"

	"vaddps	        (%3,%0,4), %%ymm5 , %%ymm5          \n\t"
	"vaddps	      32(%3,%0,4), %%ymm7 , %%ymm7          \n\t"
	"vaddps	      64(%3,%0,4), %%ymm9 , %%ymm9          \n\t"
	"vaddps	      96(%3,%0,4), %%ymm11, %%ymm11         \n\t"

	"vmulps	        %%ymm4 , %%ymm1 , %%ymm4	    \n\t"
	"vmulps	        %%ymm6 , %%ymm1 , %%ymm6	    \n\t"
	"vmulps	        %%ymm8 , %%ymm1 , %%ymm8	    \n\t"
	"vmulps	        %%ymm10, %%ymm1 , %%ymm10           \n\t"

	"vaddps         %%ymm4 , %%ymm5 , %%ymm5            \n\t"
	"vaddps         %%ymm6 , %%ymm7 , %%ymm7            \n\t"
	"vaddps         %%ymm8 , %%ymm9 , %%ymm9            \n\t"
	"vaddps         %%ymm10, %%ymm11, %%ymm11           \n\t"

	"vmovups	%%ymm5 ,   (%3,%0,4)		    \n\t"
	".p2align 1					    \n\t"
	"vmovups	%%ymm7 , 32(%3,%0,4)		    \n\t"
	"vmovups	%%ymm9 , 64(%3,%0,4)		    \n\t"
	"vmovups	%%ymm11, 96(%3,%0,4)		    \n\t"

	"addq		$32, %0	  	 	             \n\t"
	"subq	        $16, %1			             \n\t"		
	"jnz		1b		             \n\t"
	"vzeroupper					    \n\t"

	:
        : 
          "r" (i),	// 0	
	  "r" (n),  	// 1
          "r" (x),      // 2
          "r" (y),      // 3
          "r" (alpha),  // 4
          "r" (mvec)    // 5
	: "cc", 
	  "%xmm0", "%xmm1",
	  "%xmm4", "%xmm5", "%xmm6", "%xmm7", 
	  "%xmm8", "%xmm9", "%xmm10", "%xmm11", 
	  "%xmm12", "%xmm13", "%xmm14", "%xmm15", 
	  "memory"
	);


} 

