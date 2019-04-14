/*********************************************************************************
Copyright (c) 2013, The OpenBLAS Project
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
**********************************************************************************/

/*********************************************************************
* 2014/07/29 Saar
*        BLASTEST               : OK
*        CTEST                  : OK
*        TEST                   : OK
*
* 2013/10/28 Saar
* Parameter:
*       CGEMM_DEFAULT_UNROLL_N  2
*       CGEMM_DEFAULT_UNROLL_M  8
*       CGEMM_DEFAULT_P         768
*       CGEMM_DEFAULT_Q         512
*       A_PR1                   512
*       B_PR1                   512
*
* 2014/07/29 Saar
* Performance at 6192x6192x6192:
*       1 thread:       49 GFLOPS       (MKL:   52)
*       2 threads:      99 GFLOPS       (MKL:  102)
*       3 threads:     148 GFLOPS       (MKL:  150)
*       4 threads:     195 GFLOPS       (MKL:  194)
*       8 threads:     354 GFLOPS       (MKL:  317)
*
*
*********************************************************************/


#define ASSEMBLER
#include "common.h"
 
#define OLD_M	%rdi
#define OLD_N	%rsi
#define M	%r13
#define J	%r14
#define OLD_K	%rdx

#define A	%rcx
#define B	%r8
#define C	%r9
#define LDC	%r10
	
#define I	%r11
#define AO	%rdi
#define BO	%rsi
#define	CO1	%r15
#define K	%r12
#define BI	%rbp
#define	SP	%rbx

#define BO1	%rdi
#define BO2	%r15

#ifndef WINDOWS_ABI

#define STACKSIZE 96

#else

#define STACKSIZE 320

#define OLD_ALPHA_I     40 + STACKSIZE(%rsp)
#define OLD_A           48 + STACKSIZE(%rsp)
#define OLD_B           56 + STACKSIZE(%rsp)
#define OLD_C           64 + STACKSIZE(%rsp)
#define OLD_LDC         72 + STACKSIZE(%rsp)
#define OLD_OFFSET      80 + STACKSIZE(%rsp)

#endif

#define L_BUFFER_SIZE 8192

#define Ndiv6	 24(%rsp)
#define Nmod6	 32(%rsp)
#define N	 40(%rsp)
#define ALPHA_R  48(%rsp)
#define ALPHA_I  56(%rsp)
#define OFFSET   64(%rsp)
#define KK       72(%rsp)
#define KKK      80(%rsp)
#define BUFFER1	           128(%rsp)

#if defined(OS_WINDOWS)
#if   L_BUFFER_SIZE > 16384
#define STACK_TOUCH \
        movl    $ 0,  4096 * 4(%rsp);\
        movl    $ 0,  4096 * 3(%rsp);\
        movl    $ 0,  4096 * 2(%rsp);\
        movl    $ 0,  4096 * 1(%rsp);
#elif L_BUFFER_SIZE > 12288
#define STACK_TOUCH \
        movl    $ 0,  4096 * 3(%rsp);\
        movl    $ 0,  4096 * 2(%rsp);\
        movl    $ 0,  4096 * 1(%rsp);
#elif L_BUFFER_SIZE > 8192
#define STACK_TOUCH \
        movl    $ 0,  4096 * 2(%rsp);\
        movl    $ 0,  4096 * 1(%rsp);
#elif L_BUFFER_SIZE > 4096
#define STACK_TOUCH \
        movl    $ 0,  4096 * 1(%rsp);
#else
#define STACK_TOUCH
#endif
#else
#define STACK_TOUCH
#endif



#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)

#define	VFMADDPS_YR( y0,y1,y2 ) \
                               vmulps y1,y2,%ymm2;\
                               vaddps y0,%ymm2,y0

#define	VFMADDPS_YI( y0,y1,y2 ) \
                               vmulps y1,y2,%ymm3;\
                               vaddps y0,%ymm3,y0

#define	VFMADDPS_R( y0,y1,y2 ) \
                               vmulps y1,y2,%xmm2;\
                               vaddps y0,%xmm2,y0

#define	VFMADDPS_I( y0,y1,y2 ) \
                               vmulps y1,y2,%xmm3;\
                               vaddps y0,%xmm3,y0


#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)

#define	VFMADDPS_YR( y0,y1,y2 ) \
                               vmulps y1,y2,%ymm2;\
                               vsubps %ymm2,y0,y0

#define	VFMADDPS_YI( y0,y1,y2 ) \
                               vmulps y1,y2,%ymm3;\
                               vaddps y0,%ymm3,y0

#define	VFMADDPS_R( y0,y1,y2 ) \
                               vmulps y1,y2,%xmm2;\
                               vsubps %xmm2,y0,y0

#define	VFMADDPS_I( y0,y1,y2 ) \
                               vmulps y1,y2,%xmm3;\
                               vaddps y0,%xmm3,y0


#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)

#define	VFMADDPS_YR( y0,y1,y2 ) \
                               vmulps y1,y2,%ymm2;\
                               vaddps y0,%ymm2,y0

#define	VFMADDPS_YI( y0,y1,y2 ) \
                               vmulps y1,y2,%ymm3;\
                               vsubps %ymm3,y0,y0

#define	VFMADDPS_R( y0,y1,y2 ) \
                               vmulps y1,y2,%xmm2;\
                               vaddps y0,%xmm2,y0

#define	VFMADDPS_I( y0,y1,y2 ) \
                               vmulps y1,y2,%xmm3;\
                               vsubps %xmm3,y0,y0


#else

#define	VFMADDPS_YR( y0,y1,y2 ) \
                               vmulps y1,y2,%ymm2;\
                               vsubps %ymm2,y0,y0

#define	VFMADDPS_YI( y0,y1,y2 ) \
                               vmulps y1,y2,%ymm3;\
                               vsubps %ymm3,y0,y0

#define	VFMADDPS_R( y0,y1,y2 ) \
                               vmulps y1,y2,%xmm2;\
                               vsubps %xmm2,y0,y0

#define	VFMADDPS_I( y0,y1,y2 ) \
                               vmulps y1,y2,%xmm3;\
                               vsubps %xmm3,y0,y0


#endif


#define	A_PR1	512
#define	B_PR1	512

/***************************************************************************************************************************/

.macro KERNEL8x2_1

        vmovups         -16 * SIZE(AO, %rax, SIZE), %ymm0
        vbroadcastss         -8 * SIZE(BO, BI, SIZE), %ymm4
        vmovups          -8 * SIZE(AO, %rax, SIZE), %ymm1
        vbroadcastss         -7 * SIZE(BO, BI, SIZE), %ymm5
	prefetcht0	A_PR1(AO, %rax, SIZE)

        VFMADDPS_YR(        %ymm8,%ymm4,%ymm0  )
        vbroadcastss         -6 * SIZE(BO, BI, SIZE), %ymm6
        VFMADDPS_YI(        %ymm9,%ymm5,%ymm0  )
        vbroadcastss         -5 * SIZE(BO, BI, SIZE), %ymm7
        VFMADDPS_YR(        %ymm12,%ymm4,%ymm1 )
        VFMADDPS_YI(        %ymm13,%ymm5,%ymm1 )


        VFMADDPS_YR(        %ymm10,%ymm6,%ymm0 )
        vbroadcastss         -4 * SIZE(BO, BI, SIZE), %ymm4
        VFMADDPS_YI(        %ymm11,%ymm7,%ymm0 )
        vbroadcastss         -3 * SIZE(BO, BI, SIZE), %ymm5
        VFMADDPS_YR(        %ymm14,%ymm6,%ymm1 )
        vmovups           0 * SIZE(AO, %rax, SIZE), %ymm0
        VFMADDPS_YI(        %ymm15,%ymm7,%ymm1 )

        vmovups           8 * SIZE(AO, %rax, SIZE), %ymm1
	prefetcht0	A_PR1+64(AO, %rax, SIZE)

        VFMADDPS_YR(        %ymm8,%ymm4,%ymm0  )
        vbroadcastss         -2 * SIZE(BO, BI, SIZE), %ymm6
        VFMADDPS_YI(        %ymm9,%ymm5,%ymm0  )
        vbroadcastss         -1 * SIZE(BO, BI, SIZE), %ymm7
        VFMADDPS_YR(        %ymm12,%ymm4,%ymm1 )
        VFMADDPS_YI(        %ymm13,%ymm5,%ymm1 )


        VFMADDPS_YR(        %ymm10,%ymm6,%ymm0 )
        vbroadcastss          0 * SIZE(BO, BI, SIZE), %ymm4
        VFMADDPS_YI(        %ymm11,%ymm7,%ymm0 )
        vbroadcastss          1 * SIZE(BO, BI, SIZE), %ymm5
        VFMADDPS_YR(        %ymm14,%ymm6,%ymm1 )
        vmovups          16 * SIZE(AO, %rax, SIZE), %ymm0
        VFMADDPS_YI(        %ymm15,%ymm7,%ymm1 )

        vmovups          24 * SIZE(AO, %rax, SIZE), %ymm1
	prefetcht0	A_PR1+128(AO, %rax, SIZE)

        VFMADDPS_YR(        %ymm8,%ymm4,%ymm0  )
        vbroadcastss          2 * SIZE(BO, BI, SIZE), %ymm6
        VFMADDPS_YI(        %ymm9,%ymm5,%ymm0  )
        vbroadcastss          3 * SIZE(BO, BI, SIZE), %ymm7
        VFMADDPS_YR(        %ymm12,%ymm4,%ymm1 )
        VFMADDPS_YI(        %ymm13,%ymm5,%ymm1 )


        VFMADDPS_YR(        %ymm10,%ymm6,%ymm0 )
        vbroadcastss          4 * SIZE(BO, BI, SIZE), %ymm4
        VFMADDPS_YI(        %ymm11,%ymm7,%ymm0 )
        vbroadcastss          5 * SIZE(BO, BI, SIZE), %ymm5
        VFMADDPS_YR(        %ymm14,%ymm6,%ymm1 )
        vmovups          32 * SIZE(AO, %rax, SIZE), %ymm0
        VFMADDPS_YI(        %ymm15,%ymm7,%ymm1 )

        vmovups          40 * SIZE(AO, %rax, SIZE), %ymm1
	prefetcht0	A_PR1+192(AO, %rax, SIZE)

        VFMADDPS_YR(        %ymm8,%ymm4,%ymm0  )
        vbroadcastss          6 * SIZE(BO, BI, SIZE), %ymm6
        VFMADDPS_YI(        %ymm9,%ymm5,%ymm0  )
        vbroadcastss          7 * SIZE(BO, BI, SIZE), %ymm7
        VFMADDPS_YR(        %ymm12,%ymm4,%ymm1 )
        VFMADDPS_YI(        %ymm13,%ymm5,%ymm1 )

        VFMADDPS_YR(        %ymm10,%ymm6,%ymm0 )
        VFMADDPS_YI(        %ymm11,%ymm7,%ymm0 )
        addq    $ 16, BI                           
        VFMADDPS_YR(        %ymm14,%ymm6,%ymm1 )
        VFMADDPS_YI(        %ymm15,%ymm7,%ymm1 )

        addq    $ 64, %rax                         
.endm


.macro KERNEL8x2_SUB

        vmovups         -16 * SIZE(AO, %rax, SIZE), %ymm0
        vmovups          -8 * SIZE(AO, %rax, SIZE), %ymm1
        vbroadcastss         -8 * SIZE(BO, BI, SIZE), %ymm4
        vbroadcastss         -7 * SIZE(BO, BI, SIZE), %ymm5

        VFMADDPS_YR(        %ymm8,%ymm4,%ymm0  )
        vbroadcastss         -6 * SIZE(BO, BI, SIZE), %ymm6
        VFMADDPS_YI(        %ymm9,%ymm5,%ymm0  )
        vbroadcastss         -5 * SIZE(BO, BI, SIZE), %ymm7
        VFMADDPS_YR(        %ymm12,%ymm4,%ymm1 )
        VFMADDPS_YI(        %ymm13,%ymm5,%ymm1 )


        VFMADDPS_YR(        %ymm10,%ymm6,%ymm0 )
        VFMADDPS_YI(        %ymm11,%ymm7,%ymm0 )
        VFMADDPS_YR(        %ymm14,%ymm6,%ymm1 )
        VFMADDPS_YI(        %ymm15,%ymm7,%ymm1 )

        addq    $ 4 , BI                           
        addq    $ 16, %rax                         
.endm

.macro SAVE8x2

	vbroadcastss	ALPHA_R, %ymm0
	vbroadcastss	ALPHA_I, %ymm1

	// swap high and low 64 bytes
        vshufps $ 0xb1, %ymm9 , %ymm9, %ymm9
        vshufps $ 0xb1, %ymm11, %ymm11, %ymm11
        vshufps $ 0xb1, %ymm13, %ymm13, %ymm13
        vshufps $ 0xb1, %ymm15, %ymm15, %ymm15

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubps %ymm9, %ymm8 , %ymm8
        vaddsubps %ymm11,%ymm10, %ymm10
        vaddsubps %ymm13,%ymm12, %ymm12
        vaddsubps %ymm15,%ymm14, %ymm14

        vshufps $ 0xb1, %ymm8 , %ymm8, %ymm9
        vshufps $ 0xb1, %ymm10, %ymm10, %ymm11
        vshufps $ 0xb1, %ymm12, %ymm12, %ymm13
        vshufps $ 0xb1, %ymm14, %ymm14, %ymm15

#else
        vaddsubps %ymm8,  %ymm9 ,%ymm9
        vaddsubps %ymm10, %ymm11,%ymm11
        vaddsubps %ymm12, %ymm13,%ymm13
        vaddsubps %ymm14, %ymm15,%ymm15

        vmovaps   %ymm9,  %ymm8
        vmovaps   %ymm11, %ymm10
        vmovaps   %ymm13, %ymm12
        vmovaps   %ymm15, %ymm14

	// swap high and low 64 bytes
        vshufps $ 0xb1, %ymm9 , %ymm9, %ymm9
        vshufps $ 0xb1, %ymm11, %ymm11, %ymm11
        vshufps $ 0xb1, %ymm13, %ymm13, %ymm13
        vshufps $ 0xb1, %ymm15, %ymm15, %ymm15

#endif

	// multiply with ALPHA_R
        vmulps  %ymm8 , %ymm0, %ymm8
        vmulps  %ymm10, %ymm0, %ymm10
        vmulps  %ymm12, %ymm0, %ymm12
        vmulps  %ymm14, %ymm0, %ymm14

	// multiply with ALPHA_I
        vmulps  %ymm9 , %ymm1, %ymm9
        vmulps  %ymm11, %ymm1, %ymm11
        vmulps  %ymm13, %ymm1, %ymm13
        vmulps  %ymm15, %ymm1, %ymm15

	vaddsubps %ymm9, %ymm8 , %ymm8
        vaddsubps %ymm11,%ymm10, %ymm10
        vaddsubps %ymm13,%ymm12, %ymm12
        vaddsubps %ymm15,%ymm14, %ymm14



#ifndef TRMMKERNEL

	vaddps 	 	(CO1), %ymm8 , %ymm8
	vaddps  8 * SIZE(CO1), %ymm12, %ymm12

	vaddps 	 	(CO1, LDC), %ymm10, %ymm10
	vaddps  8 * SIZE(CO1, LDC), %ymm14, %ymm14

#endif

	vmovups	%ymm8 ,  	(CO1)
	vmovups	%ymm12 , 8 * SIZE(CO1)

	vmovups	%ymm10 ,  	(CO1, LDC)
	vmovups	%ymm14 , 8 * SIZE(CO1, LDC)

	prefetcht0	64(CO1)
	prefetcht0	64(CO1, LDC)

.endm

/***************************************************************************************************************************/

.macro KERNEL4x2_SUB
        vmovups         -16 * SIZE(AO, %rax, SIZE), %xmm0
        vbroadcastss         -8 * SIZE(BO, BI, SIZE), %xmm4
        VFMADDPS_R(        %xmm8,%xmm4,%xmm0  )
        vmovups         -12 * SIZE(AO, %rax, SIZE), %xmm1
        VFMADDPS_R(        %xmm12,%xmm4,%xmm1 )
        vbroadcastss         -7 * SIZE(BO, BI, SIZE), %xmm5
        VFMADDPS_I(        %xmm9,%xmm5,%xmm0  )
        VFMADDPS_I(        %xmm13,%xmm5,%xmm1 )
        vbroadcastss         -6 * SIZE(BO, BI, SIZE), %xmm6
        VFMADDPS_R(        %xmm10,%xmm6,%xmm0 )
        VFMADDPS_R(        %xmm14,%xmm6,%xmm1 )
        vbroadcastss         -5 * SIZE(BO, BI, SIZE), %xmm7
        VFMADDPS_I(        %xmm11,%xmm7,%xmm0 )
        VFMADDPS_I(        %xmm15,%xmm7,%xmm1 )
        addq    $ 4, BI                           
        addq    $ 8, %rax                         
.endm

.macro SAVE4x2

	vbroadcastss	ALPHA_R, %xmm0
	vbroadcastss	ALPHA_I, %xmm1

	// swap high and low 64 bytes
        vshufps $ 0xb1, %xmm9 , %xmm9, %xmm9
        vshufps $ 0xb1, %xmm11, %xmm11, %xmm11
        vshufps $ 0xb1, %xmm13, %xmm13, %xmm13
        vshufps $ 0xb1, %xmm15, %xmm15, %xmm15

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubps %xmm9, %xmm8 , %xmm8
        vaddsubps %xmm11,%xmm10, %xmm10
        vaddsubps %xmm13,%xmm12, %xmm12
        vaddsubps %xmm15,%xmm14, %xmm14

        vshufps $ 0xb1, %xmm8 , %xmm8, %xmm9
        vshufps $ 0xb1, %xmm10, %xmm10, %xmm11
        vshufps $ 0xb1, %xmm12, %xmm12, %xmm13
        vshufps $ 0xb1, %xmm14, %xmm14, %xmm15

#else
        vaddsubps %xmm8,  %xmm9 ,%xmm9
        vaddsubps %xmm10, %xmm11,%xmm11
        vaddsubps %xmm12, %xmm13,%xmm13
        vaddsubps %xmm14, %xmm15,%xmm15

        vmovaps   %xmm9,  %xmm8
        vmovaps   %xmm11, %xmm10
        vmovaps   %xmm13, %xmm12
        vmovaps   %xmm15, %xmm14

	// swap high and low 64 bytes
        vshufps $ 0xb1, %xmm9 , %xmm9, %xmm9
        vshufps $ 0xb1, %xmm11, %xmm11, %xmm11
        vshufps $ 0xb1, %xmm13, %xmm13, %xmm13
        vshufps $ 0xb1, %xmm15, %xmm15, %xmm15

#endif

	// multiply with ALPHA_R
        vmulps  %xmm8 , %xmm0, %xmm8
        vmulps  %xmm10, %xmm0, %xmm10
        vmulps  %xmm12, %xmm0, %xmm12
        vmulps  %xmm14, %xmm0, %xmm14

	// multiply with ALPHA_I
        vmulps  %xmm9 , %xmm1, %xmm9
        vmulps  %xmm11, %xmm1, %xmm11
        vmulps  %xmm13, %xmm1, %xmm13
        vmulps  %xmm15, %xmm1, %xmm15

	vaddsubps %xmm9, %xmm8 , %xmm8
        vaddsubps %xmm11,%xmm10, %xmm10
        vaddsubps %xmm13,%xmm12, %xmm12
        vaddsubps %xmm15,%xmm14, %xmm14

#ifndef TRMMKERNEL

	vaddps 	 	(CO1), %xmm8 , %xmm8
	vaddps  4 * SIZE(CO1), %xmm12, %xmm12

	vaddps 	 	(CO1, LDC), %xmm10, %xmm10
	vaddps  4 * SIZE(CO1, LDC), %xmm14, %xmm14

#endif

	vmovups	%xmm8 ,  	(CO1)
	vmovups	%xmm12 , 4 * SIZE(CO1)

	vmovups	%xmm10 ,  	(CO1, LDC)
	vmovups	%xmm14 , 4 * SIZE(CO1, LDC)

.endm

/************************************************************************************************/

.macro KERNEL2x2_SUB
        vmovups         -16 * SIZE(AO, %rax, SIZE), %xmm0
        vbroadcastss         -8 * SIZE(BO, BI, SIZE), %xmm4
        VFMADDPS_R(        %xmm8,%xmm4,%xmm0  )
        vbroadcastss         -7 * SIZE(BO, BI, SIZE), %xmm5
        VFMADDPS_I(        %xmm9,%xmm5,%xmm0  )
        vbroadcastss         -6 * SIZE(BO, BI, SIZE), %xmm6
        VFMADDPS_R(        %xmm10,%xmm6,%xmm0 )
        vbroadcastss         -5 * SIZE(BO, BI, SIZE), %xmm7
        VFMADDPS_I(        %xmm11,%xmm7,%xmm0 )
        addq    $ 4, BI                           
        addq    $ 4, %rax                         
.endm

.macro SAVE2x2

	vbroadcastss	ALPHA_R, %xmm0
	vbroadcastss	ALPHA_I, %xmm1

	// swap high and low 4 bytes
        vshufps $ 0xb1, %xmm9 , %xmm9, %xmm9
        vshufps $ 0xb1, %xmm11, %xmm11, %xmm11

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubps %xmm9, %xmm8 , %xmm8
        vaddsubps %xmm11,%xmm10, %xmm10

        vshufps $ 0xb1, %xmm8 , %xmm8, %xmm9
        vshufps $ 0xb1, %xmm10, %xmm10, %xmm11

#else
        vaddsubps %xmm8,  %xmm9 ,%xmm9
        vaddsubps %xmm10, %xmm11,%xmm11

        vmovaps   %xmm9,  %xmm8
        vmovaps   %xmm11, %xmm10

	// swap high and low 4 bytes
        vshufps $ 0xb1, %xmm9 , %xmm9, %xmm9
        vshufps $ 0xb1, %xmm11, %xmm11, %xmm11

#endif

	// multiply with ALPHA_R
        vmulps  %xmm8 , %xmm0, %xmm8
        vmulps  %xmm10, %xmm0, %xmm10

	// multiply with ALPHA_I
        vmulps  %xmm9 , %xmm1, %xmm9
        vmulps  %xmm11, %xmm1, %xmm11

	vaddsubps %xmm9, %xmm8 , %xmm8
        vaddsubps %xmm11,%xmm10, %xmm10

#ifndef TRMMKERNEL

	vaddps 	 	(CO1), %xmm8 , %xmm8

	vaddps 	 	(CO1, LDC), %xmm10, %xmm10

#endif

	vmovups	%xmm8 ,  	(CO1)

	vmovups	%xmm10 ,  	(CO1, LDC)

.endm

/************************************************************************************************/

.macro KERNEL1x2_SUB
        vmovsd         -16 * SIZE(AO, %rax, SIZE), %xmm0
        vbroadcastss         -8 * SIZE(BO, BI, SIZE), %xmm4
        VFMADDPS_R(        %xmm8,%xmm4,%xmm0  )
        vbroadcastss         -7 * SIZE(BO, BI, SIZE), %xmm5
        VFMADDPS_I(        %xmm9,%xmm5,%xmm0  )
        vbroadcastss         -6 * SIZE(BO, BI, SIZE), %xmm6
        VFMADDPS_R(        %xmm10,%xmm6,%xmm0 )
        vbroadcastss         -5 * SIZE(BO, BI, SIZE), %xmm7
        VFMADDPS_I(        %xmm11,%xmm7,%xmm0 )
        addq    $ 4, BI                           
        addq    $ 2, %rax                         
.endm

.macro SAVE1x2

	vbroadcastss	ALPHA_R, %xmm0
	vbroadcastss	ALPHA_I, %xmm1

	// swap high and low 64 bytes
        vshufps $ 0xb1, %xmm9 , %xmm9, %xmm9
        vshufps $ 0xb1, %xmm11, %xmm11, %xmm11

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubps %xmm9, %xmm8 , %xmm8
        vaddsubps %xmm11,%xmm10, %xmm10

        vshufps $ 0xb1, %xmm8 , %xmm8, %xmm9
        vshufps $ 0xb1, %xmm10, %xmm10, %xmm11

#else
        vaddsubps %xmm8,  %xmm9 ,%xmm9
        vaddsubps %xmm10, %xmm11,%xmm11

        vmovaps   %xmm9,  %xmm8
        vmovaps   %xmm11, %xmm10

	// swap high and low 64 bytes
        vshufps $ 0xb1, %xmm9 , %xmm9, %xmm9
        vshufps $ 0xb1, %xmm11, %xmm11, %xmm11

#endif

	// multiply with ALPHA_R
        vmulps  %xmm8 , %xmm0, %xmm8
        vmulps  %xmm10, %xmm0, %xmm10

	// multiply with ALPHA_I
        vmulps  %xmm9 , %xmm1, %xmm9
        vmulps  %xmm11, %xmm1, %xmm11

	vaddsubps %xmm9, %xmm8 , %xmm8
        vaddsubps %xmm11,%xmm10, %xmm10

#ifndef TRMMKERNEL

	vmovsd		(CO1), %xmm14
	vaddps 	 	%xmm14, %xmm8 , %xmm8

	vmovsd		(CO1, LDC), %xmm15
	vaddps 	 	%xmm15, %xmm10, %xmm10

#endif

	vmovsd	%xmm8 ,  	(CO1)
	vmovsd	%xmm10 ,  	(CO1, LDC)

.endm

/************************************************************************************************/

.macro KERNEL8x1_SUB
        vmovups         -16 * SIZE(AO, %rax, SIZE), %ymm0
        vmovups          -8 * SIZE(AO, %rax, SIZE), %ymm1
        vbroadcastss         -4 * SIZE(BO, BI, SIZE), %ymm4
        VFMADDPS_YR(        %ymm8,%ymm4,%ymm0  )
        VFMADDPS_YR(        %ymm12,%ymm4,%ymm1 )
        vbroadcastss         -3 * SIZE(BO, BI, SIZE), %ymm5
        VFMADDPS_YI(        %ymm9,%ymm5,%ymm0  )
        VFMADDPS_YI(        %ymm13,%ymm5,%ymm1 )
        addq    $ 2 , BI                           
        addq    $ 16, %rax                         
.endm

.macro SAVE8x1

	vbroadcastss	ALPHA_R, %ymm0
	vbroadcastss	ALPHA_I, %ymm1

	// swap high and low 64 bytes
        vshufps $ 0xb1, %ymm9 , %ymm9, %ymm9
        vshufps $ 0xb1, %ymm13, %ymm13, %ymm13

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubps %ymm9, %ymm8 , %ymm8
        vaddsubps %ymm13,%ymm12, %ymm12

        vshufps $ 0xb1, %ymm8 , %ymm8, %ymm9
        vshufps $ 0xb1, %ymm12, %ymm12, %ymm13

#else
        vaddsubps %ymm8,  %ymm9 ,%ymm9
        vaddsubps %ymm12, %ymm13,%ymm13

        vmovaps   %ymm9,  %ymm8
        vmovaps   %ymm13, %ymm12

	// swap high and low 64 bytes
        vshufps $ 0xb1, %ymm9 , %ymm9, %ymm9
        vshufps $ 0xb1, %ymm13, %ymm13, %ymm13

#endif

	// multiply with ALPHA_R
        vmulps  %ymm8 , %ymm0, %ymm8
        vmulps  %ymm12, %ymm0, %ymm12

	// multiply with ALPHA_I
        vmulps  %ymm9 , %ymm1, %ymm9
        vmulps  %ymm13, %ymm1, %ymm13

	vaddsubps %ymm9, %ymm8 , %ymm8
        vaddsubps %ymm13,%ymm12, %ymm12



#ifndef TRMMKERNEL

	vaddps 	 	(CO1), %ymm8 , %ymm8
	vaddps  8 * SIZE(CO1), %ymm12, %ymm12

#endif

	vmovups	%ymm8 ,  	(CO1)
	vmovups	%ymm12 , 8 * SIZE(CO1)

.endm


/************************************************************************************************/

.macro KERNEL4x1_SUB
        vmovups         -16 * SIZE(AO, %rax, SIZE), %xmm0
        vbroadcastss         -4 * SIZE(BO, BI, SIZE), %xmm4
        VFMADDPS_R(        %xmm8,%xmm4,%xmm0  )
        vmovups         -12 * SIZE(AO, %rax, SIZE), %xmm1
        VFMADDPS_R(        %xmm12,%xmm4,%xmm1 )
        vbroadcastss         -3 * SIZE(BO, BI, SIZE), %xmm5
        VFMADDPS_I(        %xmm9,%xmm5,%xmm0  )
        VFMADDPS_I(        %xmm13,%xmm5,%xmm1 )
        addq    $ 2, BI                           
        addq    $ 8, %rax                         
.endm

.macro SAVE4x1

	vbroadcastss	ALPHA_R, %xmm0
	vbroadcastss	ALPHA_I, %xmm1

	// swap high and low 4 bytes
        vshufps $ 0xb1, %xmm9 , %xmm9, %xmm9
        vshufps $ 0xb1, %xmm13, %xmm13, %xmm13

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubps %xmm9, %xmm8 , %xmm8
        vaddsubps %xmm13,%xmm12, %xmm12

        vshufps $ 0xb1, %xmm8 , %xmm8, %xmm9
        vshufps $ 0xb1, %xmm12, %xmm12, %xmm13

#else
        vaddsubps %xmm8,  %xmm9 ,%xmm9
        vaddsubps %xmm12, %xmm13,%xmm13

        vmovaps   %xmm9,  %xmm8
        vmovaps   %xmm13, %xmm12

	// swap high and low 4 bytes
        vshufps $ 0xb1, %xmm9 , %xmm9, %xmm9
        vshufps $ 0xb1, %xmm13, %xmm13, %xmm13

#endif

	// multiply with ALPHA_R
        vmulps  %xmm8 , %xmm0, %xmm8
        vmulps  %xmm12, %xmm0, %xmm12

	// multiply with ALPHA_I
        vmulps  %xmm9 , %xmm1, %xmm9
        vmulps  %xmm13, %xmm1, %xmm13

	vaddsubps %xmm9, %xmm8 , %xmm8
        vaddsubps %xmm13,%xmm12, %xmm12

#ifndef TRMMKERNEL

	vaddps 	 	(CO1), %xmm8 , %xmm8
	vaddps  4 * SIZE(CO1), %xmm12, %xmm12

#endif

	vmovups	%xmm8 ,  	(CO1)
	vmovups	%xmm12 , 4 * SIZE(CO1)

.endm

/************************************************************************************************/

.macro KERNEL2x1_SUB
        vmovups         -16 * SIZE(AO, %rax, SIZE), %xmm0
        vbroadcastss         -4 * SIZE(BO, BI, SIZE), %xmm4
        VFMADDPS_R(        %xmm8,%xmm4,%xmm0  )
        vbroadcastss         -3 * SIZE(BO, BI, SIZE), %xmm5
        VFMADDPS_I(        %xmm9,%xmm5,%xmm0  )
        addq    $ 2, BI                           
        addq    $ 4, %rax                         
.endm

.macro SAVE2x1

	vbroadcastss	ALPHA_R, %xmm0
	vbroadcastss	ALPHA_I, %xmm1

	// swap high and low 64 bytes
        vshufps $ 0xb1, %xmm9 , %xmm9, %xmm9

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubps %xmm9, %xmm8 , %xmm8

        vshufps $ 0xb1, %xmm8 , %xmm8, %xmm9

#else
        vaddsubps %xmm8,  %xmm9 ,%xmm9

        vmovaps   %xmm9,  %xmm8

	// swap high and low 64 bytes
        vshufps $ 0xb1, %xmm9 , %xmm9, %xmm9

#endif

	// multiply with ALPHA_R
        vmulps  %xmm8 , %xmm0, %xmm8

	// multiply with ALPHA_I
        vmulps  %xmm9 , %xmm1, %xmm9

	vaddsubps %xmm9, %xmm8 , %xmm8

#ifndef TRMMKERNEL

	vaddps 	 	(CO1), %xmm8 , %xmm8

#endif

	vmovups	%xmm8 ,  	(CO1)

.endm

/************************************************************************************************/

.macro KERNEL1x1_SUB
        vmovsd         -16 * SIZE(AO, %rax, SIZE), %xmm0
        vbroadcastss         -4 * SIZE(BO, BI, SIZE), %xmm4
        VFMADDPS_R(        %xmm8,%xmm4,%xmm0 )
        vbroadcastss         -3 * SIZE(BO, BI, SIZE), %xmm5
        VFMADDPS_I(        %xmm9,%xmm5,%xmm0 )
        addq    $ 2, BI                           
        addq    $ 2, %rax                         
.endm

.macro SAVE1x1

	vbroadcastss	ALPHA_R, %xmm0
	vbroadcastss	ALPHA_I, %xmm1

	// swap high and low 64 bytes
        vshufps $ 0xb1, %xmm9 , %xmm9, %xmm9

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubps %xmm9, %xmm8 , %xmm8

        vshufps $ 0xb1, %xmm8 , %xmm8, %xmm9

#else
        vaddsubps %xmm8,  %xmm9 ,%xmm9

        vmovaps   %xmm9,  %xmm8

	// swap high and low 64 bytes
        vshufps $ 0xb1, %xmm9 , %xmm9, %xmm9

#endif

	// multiply with ALPHA_R
        vmulps  %xmm8 , %xmm0, %xmm8

	// multiply with ALPHA_I
        vmulps  %xmm9 , %xmm1, %xmm9

	vaddsubps %xmm9, %xmm8 , %xmm8

#ifndef TRMMKERNEL

	vmovsd		(CO1), %xmm14
	vaddps 	 	%xmm14, %xmm8 , %xmm8

#endif

	vmovsd	%xmm8 ,  	(CO1)

.endm

/************************************************************************************************/




	PROLOGUE
	PROFCODE
	
	subq	$ STACKSIZE, %rsp
	movq	%rbx,   (%rsp)
	movq	%rbp,  8(%rsp)
	movq	%r12, 16(%rsp)
	movq	%r13, 24(%rsp)
	movq	%r14, 32(%rsp)
	movq	%r15, 40(%rsp)

	vzeroupper

#ifdef WINDOWS_ABI
	movq	%rdi,    48(%rsp)
	movq	%rsi,    56(%rsp)
	vmovups	%xmm6,   64(%rsp)
	vmovups	%xmm7,   80(%rsp)
	vmovups	%xmm8,   96(%rsp)
	vmovups	%xmm9,  112(%rsp)
	vmovups	%xmm10, 128(%rsp)
	vmovups	%xmm11, 144(%rsp)
	vmovups	%xmm12, 160(%rsp)
	vmovups	%xmm13, 176(%rsp)
	vmovups	%xmm14, 192(%rsp)
	vmovups	%xmm15, 208(%rsp)

	movq	ARG1,      OLD_M
	movq	ARG2,      OLD_N
	movq	ARG3,      OLD_K
	movq	OLD_A,     A
	movq	OLD_B,     B
	movq	OLD_C,     C
	movq	OLD_LDC,   LDC
#ifdef TRMMKERNEL
	movsd	OLD_OFFSET, %xmm12
#endif
	vmovaps	%xmm3, %xmm0
	vmovsd   OLD_ALPHA_I, %xmm1

#else
	movq	STACKSIZE +  8(%rsp), LDC
#ifdef TRMMKERNEL
	movsd	STACKSIZE + 16(%rsp), %xmm12
#endif

#endif

	movq    %rsp, SP      # save old stack
        subq    $ 128 + L_BUFFER_SIZE, %rsp
        andq    $ -4096, %rsp    # align stack

        STACK_TOUCH

	cmpq	$ 0, OLD_M
	je	.L999

	cmpq	$ 0, OLD_N
	je	.L999

	cmpq	$ 0, OLD_K
	je	.L999

	movq	OLD_M, M
	movq	OLD_N, N
	movq	OLD_K, K

	vmovss	 %xmm0, ALPHA_R
	vmovss	 %xmm1, ALPHA_I

	salq	$ ZBASE_SHIFT, LDC

	movq    N, %rax
        xorq    %rdx, %rdx
        movq    $ 2,  %rdi
        divq    %rdi                    //    N / 2
        movq    %rax, Ndiv6             //    N / 2
        movq    %rdx, Nmod6             //    N % 2

	

#ifdef TRMMKERNEL
	vmovsd	%xmm12, OFFSET
	vmovsd	%xmm12, KK
#ifndef LEFT
	negq	KK
#endif	
#endif

.L2_0:

	movq	Ndiv6,  J
	cmpq	$ 0, J
	je	.L1_0
	ALIGN_4



.L2_01:
	// copy to sub buffer
	movq	B, BO1
	leaq    BUFFER1, BO		// first buffer to BO
	movq	K, %rax
	ALIGN_4

.L2_02b:

	vmovups	(BO1), %xmm0
	vmovups	%xmm0,       (BO)
	addq	$ 4*SIZE,BO1
	addq	$ 4*SIZE,BO
	decq	%rax
	jnz	.L2_02b

.L2_02c:

	movq	BO1, B			// next offset of B

.L2_10:
	movq	C, CO1
	leaq	(C, LDC, 2), C		// c += 2 * ldc

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
        movq    %rax, KK
#endif
	
	movq	A, AO		 	// aoffset = a
	addq	$ 16 * SIZE, AO

	movq	M,  I
	sarq	$ 3, I			// i = (m >> 3)
	je	.L2_4_10

	ALIGN_4
/**********************************************************************************************************/

.L2_8_11:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leaq	BUFFER1, BO		// first buffer to BO
	addq	$ 8 * SIZE, BO
#else
        movq    KK, %rax
	leaq	BUFFER1, BO			// first buffer to BO
	addq	$ 8 * SIZE, BO
	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,4), BI                     //  BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$ 4, %rax			// rax = rax *16 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif

	vzeroall

#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $ 8, %rax        // number of values in AO
#else
        addq    $ 2, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$ -8, %rax			//  K = K - ( K % 8 )
	je	.L2_8_16
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values

	salq	$ 4, %rax			// rax = rax *16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_8_12:

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL8x2_1

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL8x2_1

	je	.L2_8_16

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL8x2_1

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL8x2_1

	je	.L2_8_16

	jmp	.L2_8_12
	ALIGN_4

.L2_8_16:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$ 7, %rax		# if (k & 1)
	je .L2_8_19

	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values

	salq	$ 4, %rax			// rax = rax *16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_8_17:

	KERNEL8x2_SUB

	jl	.L2_8_17
	ALIGN_4


.L2_8_19:

	SAVE8x2


#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$ 4, %rax			// rax = rax *16 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $ 8, KK
#endif

	addq	$ 16 * SIZE, CO1		# coffset += 16
	decq	I			# i --
	jg	.L2_8_11
	ALIGN_4	


/**********************************************************************************************************/




.L2_4_10:
	testq	$ 7, M		
	jz	.L2_4_60		// to next 2 lines of N

	testq	$ 4, M		
	jz	.L2_4_20
	ALIGN_4


.L2_4_11:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leaq	BUFFER1, BO		// first buffer to BO
	addq	$ 8 * SIZE, BO
#else
        movq    KK, %rax
	leaq	BUFFER1, BO			// first buffer to BO
	addq	$ 8 * SIZE, BO
	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,4), BI                     //  BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$ 3, %rax			// rax = rax * 8 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif

	vzeroall

#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $ 4, %rax        // number of values in AO
#else
        addq    $ 2, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$ -8, %rax			//  K = K - ( K % 8 )
	je	.L2_4_16
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values

	salq	$ 3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_4_12:

	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	je	.L2_4_16

	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	je	.L2_4_16

	jmp	.L2_4_12
	ALIGN_4

.L2_4_16:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$ 7, %rax		# if (k & 1)
	je .L2_4_19

	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values

	salq	$ 3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_4_17:

	KERNEL4x2_SUB

	jl	.L2_4_17
	ALIGN_4


.L2_4_19:

	SAVE4x2

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$ 3, %rax			// rax = rax * 8 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $ 4, KK
#endif

	addq	$ 8 * SIZE, CO1		# coffset += 8
	ALIGN_4	



/**************************************************************************
* Rest of M 
***************************************************************************/

.L2_4_20:

	testq	$ 2, M		
	jz	.L2_4_40
	ALIGN_4

.L2_4_21:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leaq	BUFFER1, BO		// first buffer to BO
	addq	$ 8 * SIZE, BO
#else
        movq    KK, %rax
	leaq	BUFFER1, BO			// first buffer to BO
	addq	$ 8 * SIZE, BO
	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,4), BI                     //  BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$ 2, %rax			// rax = rax * 4 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif

	vzeroall

#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $ 2, %rax        // number of values in AO
#else
        addq    $ 2, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$ -8, %rax			//  K = K - ( K % 8 )
	je	.L2_4_26
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values

	salq	$ 2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_4_22:

	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	je	.L2_4_26

	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	je	.L2_4_26

	jmp	.L2_4_22
	ALIGN_4

.L2_4_26:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$ 7, %rax		# if (k & 1)
	je .L2_4_29

	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values

	salq	$ 2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_4_27:

	KERNEL2x2_SUB

	jl	.L2_4_27
	ALIGN_4


.L2_4_29:

	vbroadcastss	ALPHA_R, %xmm0
	vbroadcastss	ALPHA_I, %xmm1

	// swap high and low 64 bytes
        vshufps $ 0xb1, %xmm9 , %xmm9, %xmm9
        vshufps $ 0xb1, %xmm11, %xmm11, %xmm11

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubps %xmm9, %xmm8 , %xmm8
        vaddsubps %xmm11,%xmm10, %xmm10

        vshufps $ 0xb1, %xmm8 , %xmm8, %xmm9
        vshufps $ 0xb1, %xmm10, %xmm10, %xmm11

#else
        vaddsubps %xmm8,  %xmm9 ,%xmm9
        vaddsubps %xmm10, %xmm11,%xmm11

        vmovaps   %xmm9,  %xmm8
        vmovaps   %xmm11, %xmm10

	// swap high and low 64 bytes
        vshufps $ 0xb1, %xmm9 , %xmm9, %xmm9
        vshufps $ 0xb1, %xmm11, %xmm11, %xmm11

#endif

	// multiply with ALPHA_R
        vmulps  %xmm8 , %xmm0, %xmm8
        vmulps  %xmm10, %xmm0, %xmm10

	// multiply with ALPHA_I
        vmulps  %xmm9 , %xmm1, %xmm9
        vmulps  %xmm11, %xmm1, %xmm11

	vaddsubps %xmm9, %xmm8 , %xmm8
        vaddsubps %xmm11,%xmm10, %xmm10



#ifndef TRMMKERNEL

	vaddps 	 	(CO1), %xmm8 , %xmm8

	vaddps 	 	(CO1, LDC), %xmm10, %xmm10

#endif

	vmovups	%xmm8 ,  	(CO1)

	vmovups	%xmm10 ,  	(CO1, LDC)



#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$ 2, %rax			// rax = rax * 4 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $ 2, KK
#endif

	addq	$ 4 * SIZE, CO1		# coffset += 4
	decq	I			# i --
	jg	.L2_4_21
	ALIGN_4	



/**************************************************************************/
.L2_4_40:
	testq	$ 1, M		
	jz	.L2_4_60		// to next 2 lines of N

	ALIGN_4

.L2_4_41:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leaq	BUFFER1, BO		// first buffer to BO
	addq	$ 8 * SIZE, BO
#else
        movq    KK, %rax
	leaq	BUFFER1, BO			// first buffer to BO
	addq	$ 8 * SIZE, BO
	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,4), BI                     //  BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$ 1, %rax			// rax = rax * 2 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif

	vzeroall

#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $ 1, %rax        // number of values in AO
#else
        addq    $ 2, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$ -8, %rax			//  K = K - ( K % 8 )
	je	.L2_4_46
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values

	salq	$ 1, %rax			// rax = rax * 2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_4_42:

	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	je	.L2_4_46

	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	je	.L2_4_46

	jmp	.L2_4_42
	ALIGN_4

.L2_4_46:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$ 7, %rax		# if (k & 1)
	je .L2_4_49

	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values

	salq	$ 1, %rax			// rax = rax * 2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_4_47:

	KERNEL1x2_SUB

	jl	.L2_4_47
	ALIGN_4


.L2_4_49:

	SAVE1x2

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$ 1, %rax			// rax = rax * 2 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $ 1, KK
#endif

	addq	$ 2 * SIZE, CO1		# coffset += 2
	decq	I			# i --
	jg	.L2_4_41
	ALIGN_4	



	
.L2_4_60:
#if defined(TRMMKERNEL) && !defined(LEFT)
        addq    $ 2, KK
#endif

	decq	J			// j --
	jg	.L2_01			// next 2 lines of N



.L1_0:

/************************************************************************************************
* Loop for Nmod6 % 2 > 0
*************************************************************************************************/

	movq	Nmod6, J		
	andq	$ 1, J			// j % 2
	je	.L999
	ALIGN_4

.L1_01:
	// copy to sub buffer
	movq	B, BO1
	leaq    BUFFER1, BO		// first buffer to BO
	movq	K, %rax
	ALIGN_4

.L1_02b:

	vmovsd		(BO1), %xmm0
	vmovsd	%xmm0,       (BO)
	addq	$ 2*SIZE,BO1
	addq	$ 2*SIZE,BO
	decq	%rax
	jnz	.L1_02b

.L1_02c:

	movq	BO1, B			// next offset of B

.L1_10:
	movq	C, CO1
	leaq	(C, LDC, 1), C		// c += 1 * ldc

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
        movq    %rax, KK
#endif
	
	movq	A, AO		 	// aoffset = a
	addq	$ 16 * SIZE, AO

	movq	M,  I
	sarq	$ 3, I			// i = (m >> 3)
	je	.L1_4_10

	ALIGN_4

/**************************************************************************************************/

.L1_8_11:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leaq	BUFFER1, BO		// first buffer to BO
	addq	$ 4 * SIZE, BO
#else
        movq    KK, %rax
	leaq	BUFFER1, BO			// first buffer to BO
	addq	$ 4 * SIZE, BO
	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,2), BI                     //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$ 4, %rax			// rax = rax *16 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif

	vzeroall

#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $ 8, %rax        // number of values in AO
#else
        addq    $ 1, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$ -8, %rax			//  K = K - ( K % 8 )
	je	.L1_8_16
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values

	salq	$ 4, %rax			// rax = rax *16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_8_12:

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL8x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL8x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL8x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL8x1_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL8x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL8x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL8x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL8x1_SUB

	je	.L1_8_16

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL8x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL8x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL8x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL8x1_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL8x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL8x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL8x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL8x1_SUB

	je	.L1_8_16

	jmp	.L1_8_12
	ALIGN_4

.L1_8_16:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$ 7, %rax		# if (k & 1)
	je .L1_8_19

	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 4 ; number of values

	salq	$ 4, %rax			// rax = rax *16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_8_17:

	KERNEL8x1_SUB

	jl	.L1_8_17
	ALIGN_4


.L1_8_19:

	SAVE8x1


#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$ 4, %rax			// rax = rax *16 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $ 8, KK
#endif

	addq	$ 16 * SIZE, CO1		# coffset += 16
	decq	I			# i --
	jg	.L1_8_11
	ALIGN_4	



/**************************************************************************************************/
.L1_4_10:

	testq	$ 7, M		
	jz	.L999

	testq	$ 4, M		
	jz	.L1_4_20


.L1_4_11:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leaq	BUFFER1, BO		// first buffer to BO
	addq	$ 4 * SIZE, BO
#else
        movq    KK, %rax
	leaq	BUFFER1, BO			// first buffer to BO
	addq	$ 4 * SIZE, BO
	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,2), BI                     //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$ 3, %rax			// rax = rax * 8 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif

	vzeroall

#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $ 4, %rax        // number of values in AO
#else
        addq    $ 1, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$ -8, %rax			//  K = K - ( K % 8 )
	je	.L1_4_16
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values

	salq	$ 3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_4_12:

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	je	.L1_4_16

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	je	.L1_4_16

	jmp	.L1_4_12
	ALIGN_4

.L1_4_16:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$ 7, %rax		# if (k & 1)
	je .L1_4_19

	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 4 ; number of values

	salq	$ 3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_4_17:

	KERNEL4x1_SUB

	jl	.L1_4_17
	ALIGN_4


.L1_4_19:

	SAVE4x1

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$ 3, %rax			// rax = rax * 8 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $ 4, KK
#endif

	addq	$ 8 * SIZE, CO1		# coffset += 8
	ALIGN_4	



/**************************************************************************
* Rest of M 
***************************************************************************/

.L1_4_20:

	testq	$ 2, M		
	jz	.L1_4_40
	ALIGN_4

.L1_4_21:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leaq	BUFFER1, BO		// first buffer to BO
	addq	$ 4 * SIZE, BO
#else
        movq    KK, %rax
	leaq	BUFFER1, BO			// first buffer to BO
	addq	$ 4 * SIZE, BO
	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,2), BI                     //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$ 2, %rax			// rax = rax * 4 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif

	vzeroall

#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $ 2, %rax        // number of values in AO
#else
        addq    $ 1, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$ -8, %rax			//  K = K - ( K % 8 )
	je	.L1_4_26
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values

	salq	$ 2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_4_22:

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	je	.L1_4_26

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	je	.L1_4_26

	jmp	.L1_4_22
	ALIGN_4

.L1_4_26:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$ 7, %rax		# if (k & 1)
	je .L1_4_29

	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2; number of values

	salq	$ 2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_4_27:

	KERNEL2x1_SUB

	jl	.L1_4_27
	ALIGN_4


.L1_4_29:

	SAVE2x1

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$ 2, %rax			// rax = rax * 4 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $ 2, KK
#endif

	addq	$ 4 * SIZE, CO1		# coffset += 4
	ALIGN_4	



/**************************************************************************/
.L1_4_40:
	testq	$ 1, M		
	jz	.L999		// to next 2 lines of N

	ALIGN_4

.L1_4_41:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leaq	BUFFER1, BO		// first buffer to BO
	addq	$ 4 * SIZE, BO
#else
        movq    KK, %rax
	leaq	BUFFER1, BO			// first buffer to BO
	addq	$ 4 * SIZE, BO
	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,2), BI                     //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$ 1, %rax			// rax = rax * 2 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif

	vzeroall

#ifndef TRMMKERNEL
        movq    K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
        movq    K, %rax
        subq    KK, %rax
        movq    %rax, KKK
#else
        movq    KK, %rax
#ifdef LEFT
        addq    $ 1, %rax        // number of values in AO
#else
        addq    $ 1, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$ -8, %rax			//  K = K - ( K % 8 )
	je	.L1_4_46
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values

	salq	$ 1, %rax			// rax = rax * 2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_4_42:

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	je	.L1_4_46

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	je	.L1_4_46

	jmp	.L1_4_42
	ALIGN_4

.L1_4_46:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$ 7, %rax		# if (k & 1)
	je .L1_4_49

	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values

	salq	$ 1, %rax			// rax = rax * 2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_4_47:

	KERNEL1x1_SUB

	jl	.L1_4_47
	ALIGN_4


.L1_4_49:

	SAVE1x1



#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$ 1, %rax			// rax = rax * 2 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $ 1, KK
#endif

	addq	$ 2 * SIZE, CO1		# coffset += 2
	ALIGN_4	


.L999:
	vzeroupper

	movq   		SP, %rsp
	movq	   (%rsp), %rbx
	movq	  8(%rsp), %rbp
	movq	 16(%rsp), %r12
	movq	 24(%rsp), %r13
	movq	 32(%rsp), %r14
	movq	 40(%rsp), %r15

#ifdef WINDOWS_ABI
	movq	 48(%rsp), %rdi
	movq	 56(%rsp), %rsi
	vmovups	 64(%rsp), %xmm6
	vmovups	 80(%rsp), %xmm7
	vmovups	 96(%rsp), %xmm8
	vmovups	112(%rsp), %xmm9
	vmovups	128(%rsp), %xmm10
	vmovups	144(%rsp), %xmm11
	vmovups	160(%rsp), %xmm12
	vmovups	176(%rsp), %xmm13
	vmovups	192(%rsp), %xmm14
	vmovups	208(%rsp), %xmm15
#endif

	addq	$ STACKSIZE, %rsp
	ret

	EPILOGUE
