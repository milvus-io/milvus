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
	FLOAT mvec[2] = { -1.0, 1.0 };
#else
	FLOAT mvec[2] = { 1.0, -1.0 };
#endif

	BLASLONG register i = 0;

  if ( n < 384 )
  {

	__asm__  __volatile__
	(
	"vzeroupper					    \n\t"
	"vmovddup		(%4), %%xmm0		    \n\t"  // real part of alpha
	"vmovddup	       8(%4), %%xmm1		    \n\t"  // imag part of alpha
#if !defined(CONJ)
	"vmulpd		(%5), %%xmm1 , %%xmm1		    \n\t"
#else
	"vmulpd		(%5), %%xmm0 , %%xmm0		    \n\t"
#endif

	".align 16				            \n\t"
	"1:				            \n\t"

	"vmovups        (%2,%0,8), %%xmm5                   \n\t" // 1 complex values from x
	".align 2					    \n\t"
	"vmovups      16(%2,%0,8), %%xmm7                   \n\t" // 1 complex values from x
	"vmovups      32(%2,%0,8), %%xmm9                   \n\t" // 1 complex values from x
	"vmovups      48(%2,%0,8), %%xmm11                  \n\t" // 1 complex values from x

	"vmovups      64(%2,%0,8), %%xmm12                  \n\t" // 1 complex values from x
	"vmovups      80(%2,%0,8), %%xmm13                  \n\t" // 1 complex values from x
	"vmovups      96(%2,%0,8), %%xmm14                  \n\t" // 1 complex values from x
	"vmovups     112(%2,%0,8), %%xmm15                  \n\t" // 1 complex values from x

	"vpermilpd	$0x1 , %%xmm5 , %%xmm4 	    \n\t"  // exchange real and imag part
	"vpermilpd	$0x1 , %%xmm7 , %%xmm6 	    \n\t"  // exchange real and imag part
	"vpermilpd	$0x1 , %%xmm9 , %%xmm8 	    \n\t"  // exchange real and imag part
	"vpermilpd	$0x1 , %%xmm11, %%xmm10 	    \n\t"  // exchange real and imag part

	"vfmaddpd    (%3,%0,8), %%xmm0 , %%xmm5, %%xmm5     \n\t"
	".align 2					    \n\t"
	"vfmaddpd  16(%3,%0,8), %%xmm0 , %%xmm7, %%xmm7     \n\t"
	"vfmaddpd  32(%3,%0,8), %%xmm0 , %%xmm9, %%xmm9     \n\t"
	"vfmaddpd  48(%3,%0,8), %%xmm0 , %%xmm11,%%xmm11    \n\t"

	"vfmaddpd	%%xmm5 , %%xmm1 , %%xmm4 , %%xmm5   \n\t"
	"vfmaddpd	%%xmm7 , %%xmm1 , %%xmm6 , %%xmm7   \n\t"
	"vfmaddpd	%%xmm9 , %%xmm1 , %%xmm8 , %%xmm9   \n\t"
	"vfmaddpd	%%xmm11, %%xmm1 , %%xmm10, %%xmm11  \n\t"

	"vpermilpd	$0x1 , %%xmm12, %%xmm4 	    \n\t"  // exchange real and imag part
	"vpermilpd	$0x1 , %%xmm13, %%xmm6 	    \n\t"  // exchange real and imag part
	"vpermilpd	$0x1 , %%xmm14, %%xmm8 	    \n\t"  // exchange real and imag part
	"vpermilpd	$0x1 , %%xmm15, %%xmm10 	    \n\t"  // exchange real and imag part

	"vfmaddpd  64(%3,%0,8), %%xmm0 , %%xmm12, %%xmm12   \n\t"
	"vfmaddpd  80(%3,%0,8), %%xmm0 , %%xmm13, %%xmm13   \n\t"
	"vfmaddpd  96(%3,%0,8), %%xmm0 , %%xmm14, %%xmm14   \n\t"
	"vfmaddpd 112(%3,%0,8), %%xmm0 , %%xmm15, %%xmm15   \n\t"

	"vfmaddpd	%%xmm12, %%xmm1 , %%xmm4 , %%xmm12  \n\t"
	"vfmaddpd	%%xmm13, %%xmm1 , %%xmm6 , %%xmm13  \n\t"
	"vfmaddpd	%%xmm14, %%xmm1 , %%xmm8 , %%xmm14  \n\t"
	"vfmaddpd	%%xmm15, %%xmm1 , %%xmm10, %%xmm15  \n\t"

	"vmovups	%%xmm5 ,   (%3,%0,8)		    \n\t"
	".align 2					    \n\t"
	"vmovups	%%xmm7 , 16(%3,%0,8)		    \n\t"
	"vmovups	%%xmm9 , 32(%3,%0,8)		    \n\t"
	"vmovups	%%xmm11, 48(%3,%0,8)		    \n\t"
	"vmovups	%%xmm12, 64(%3,%0,8)		    \n\t"
	"vmovups	%%xmm13, 80(%3,%0,8)		    \n\t"
	"vmovups	%%xmm14, 96(%3,%0,8)		    \n\t"
	"vmovups	%%xmm15,112(%3,%0,8)		    \n\t"

	"addq		$16, %0	  	 	             \n\t"
	"subq	        $8 , %1			             \n\t"		
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
	return;
  }

	__asm__  __volatile__
	(
	"vzeroupper					    \n\t"
	"vmovddup		(%4), %%xmm0		    \n\t"  // real part of alpha
	"vmovddup	       8(%4), %%xmm1		    \n\t"  // imag part of alpha
#if !defined(CONJ)
	"vmulpd		(%5), %%xmm1 , %%xmm1		    \n\t"
#else
	"vmulpd		(%5), %%xmm0 , %%xmm0		    \n\t"
#endif

	".align 16				            \n\t"
	"1:				            \n\t"

	"prefetcht0	512(%2,%0,8)			    \n\t"
	"vmovups        (%2,%0,8), %%xmm5                   \n\t" // 1 complex values from x
	".align 2					    \n\t"
	"vmovups      16(%2,%0,8), %%xmm7                   \n\t" // 1 complex values from x
	"vmovups      32(%2,%0,8), %%xmm9                   \n\t" // 1 complex values from x
	"vmovups      48(%2,%0,8), %%xmm11                  \n\t" // 1 complex values from x

	"vpermilpd	$0x1 , %%xmm5 , %%xmm4 	    \n\t"  // exchange real and imag part
	"vpermilpd	$0x1 , %%xmm7 , %%xmm6 	    \n\t"  // exchange real and imag part
	"vpermilpd	$0x1 , %%xmm9 , %%xmm8 	    \n\t"  // exchange real and imag part
	"vpermilpd	$0x1 , %%xmm11, %%xmm10 	    \n\t"  // exchange real and imag part

	"prefetcht0	512(%3,%0,8)			    \n\t"
	"vfmaddpd    (%3,%0,8), %%xmm0 , %%xmm5, %%xmm5     \n\t"
	".align 2					    \n\t"
	"vfmaddpd  16(%3,%0,8), %%xmm0 , %%xmm7, %%xmm7     \n\t"
	"vfmaddpd  32(%3,%0,8), %%xmm0 , %%xmm9, %%xmm9     \n\t"
	"vfmaddpd  48(%3,%0,8), %%xmm0 , %%xmm11,%%xmm11    \n\t"

	"vfmaddpd	%%xmm5 , %%xmm1 , %%xmm4 , %%xmm5   \n\t"
	"vfmaddpd	%%xmm7 , %%xmm1 , %%xmm6 , %%xmm7   \n\t"
	"vfmaddpd	%%xmm9 , %%xmm1 , %%xmm8 , %%xmm9   \n\t"
	"vfmaddpd	%%xmm11, %%xmm1 , %%xmm10, %%xmm11  \n\t"

	"vmovups	%%xmm5 ,   (%3,%0,8)		    \n\t"
	".align 2					    \n\t"
	"vmovups	%%xmm7 , 16(%3,%0,8)		    \n\t"
	"vmovups	%%xmm9 , 32(%3,%0,8)		    \n\t"
	"vmovups	%%xmm11, 48(%3,%0,8)		    \n\t"

	"addq		$8 , %0	  	 	             \n\t"
	"subq	        $4, %1			             \n\t"		
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
	  "memory"
	);


} 

