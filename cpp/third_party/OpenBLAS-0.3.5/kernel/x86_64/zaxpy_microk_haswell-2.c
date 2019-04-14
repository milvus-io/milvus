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

#define HAVE_KERNEL_4 1
static void zaxpy_kernel_4( BLASLONG n, FLOAT *x, FLOAT *y , FLOAT *alpha) __attribute__ ((noinline));

static void zaxpy_kernel_4( BLASLONG n, FLOAT *x, FLOAT *y, FLOAT *alpha)
{

#if !defined(CONJ)
	FLOAT mvec[4] = { -1.0, 1.0, -1.0, 1.0 };
#else
	FLOAT mvec[4] = { 1.0, -1.0, 1.0, -1.0 };
#endif

	BLASLONG register i = 0;

	__asm__  __volatile__
	(
	"vzeroupper					    \n\t"
	"vbroadcastsd		(%4), %%ymm0		    \n\t"  // real part of alpha
	"vbroadcastsd	       8(%4), %%ymm1		    \n\t"  // imag part of alpha
#if !defined(CONJ)
	"vmulpd		(%5), %%ymm1 , %%ymm1		    \n\t"
#else
	"vmulpd		(%5), %%ymm0 , %%ymm0		    \n\t"
#endif

	".p2align 4				            \n\t"
	"1:				            \n\t"

	"vmovups        (%2,%0,8), %%ymm5                   \n\t" // 2 complex values from x
	".p2align 1					    \n\t"
	"vmovups      32(%2,%0,8), %%ymm7                   \n\t" // 2 complex values from x
	"vmovups      64(%2,%0,8), %%ymm9                   \n\t" // 2 complex values from x
	"vmovups      96(%2,%0,8), %%ymm11                  \n\t" // 2 complex values from x

	"vmovups     128(%2,%0,8), %%ymm12                  \n\t" // 2 complex values from x
	"vmovups     160(%2,%0,8), %%ymm13                  \n\t" // 2 complex values from x
	"vmovups     192(%2,%0,8), %%ymm14                  \n\t" // 2 complex values from x
	"vmovups     224(%2,%0,8), %%ymm15                  \n\t" // 2 complex values from x

	"vpermilpd	$0x5 , %%ymm5 , %%ymm4 	    \n\t"  // exchange real and imag part
	"vpermilpd	$0x5 , %%ymm7 , %%ymm6 	    \n\t"  // exchange real and imag part
	"vpermilpd	$0x5 , %%ymm9 , %%ymm8 	    \n\t"  // exchange real and imag part
	"vpermilpd	$0x5 , %%ymm11, %%ymm10 	    \n\t"  // exchange real and imag part

	"vfmadd213pd    (%3,%0,8), %%ymm0 , %%ymm5          \n\t"
	".p2align 1					    \n\t"
	"vfmadd213pd  32(%3,%0,8), %%ymm0 , %%ymm7          \n\t"
	"vfmadd213pd  64(%3,%0,8), %%ymm0 , %%ymm9          \n\t"
	"vfmadd213pd  96(%3,%0,8), %%ymm0 , %%ymm11         \n\t"

	"vfmadd231pd	%%ymm1 , %%ymm4 , %%ymm5   \n\t"
	"vfmadd231pd	%%ymm1 , %%ymm6 , %%ymm7   \n\t"
	"vfmadd231pd	%%ymm1 , %%ymm8 , %%ymm9   \n\t"
	"vfmadd231pd	%%ymm1 , %%ymm10, %%ymm11  \n\t"

	"vpermilpd	$0x5 , %%ymm12, %%ymm4 	    \n\t"  // exchange real and imag part
	"vpermilpd	$0x5 , %%ymm13, %%ymm6 	    \n\t"  // exchange real and imag part
	"vpermilpd	$0x5 , %%ymm14, %%ymm8 	    \n\t"  // exchange real and imag part
	"vpermilpd	$0x5 , %%ymm15, %%ymm10 	    \n\t"  // exchange real and imag part

	"vfmadd213pd 128(%3,%0,8), %%ymm0 , %%ymm12         \n\t"
	"vfmadd213pd 160(%3,%0,8), %%ymm0 , %%ymm13         \n\t"
	"vfmadd213pd 192(%3,%0,8), %%ymm0 , %%ymm14         \n\t"
	"vfmadd213pd 224(%3,%0,8), %%ymm0 , %%ymm15         \n\t"

	"vfmadd231pd	%%ymm1 , %%ymm4 , %%ymm12  \n\t"
	"vfmadd231pd	%%ymm1 , %%ymm6 , %%ymm13  \n\t"
	"vfmadd231pd	%%ymm1 , %%ymm8 , %%ymm14  \n\t"
	"vfmadd231pd	%%ymm1 , %%ymm10, %%ymm15  \n\t"

	"vmovups	%%ymm5 ,   (%3,%0,8)		    \n\t"
	".p2align 1					    \n\t"
	"vmovups	%%ymm7 , 32(%3,%0,8)		    \n\t"
	"vmovups	%%ymm9 , 64(%3,%0,8)		    \n\t"
	"vmovups	%%ymm11, 96(%3,%0,8)		    \n\t"

	"vmovups	%%ymm12,128(%3,%0,8)		    \n\t"
	"vmovups	%%ymm13,160(%3,%0,8)		    \n\t"
	"vmovups	%%ymm14,192(%3,%0,8)		    \n\t"
	"vmovups	%%ymm15,224(%3,%0,8)		    \n\t"

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

