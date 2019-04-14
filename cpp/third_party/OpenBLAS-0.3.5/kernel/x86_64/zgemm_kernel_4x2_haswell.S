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

/********************************************************************************
* 2014/07/28 Saar
*        BLASTEST               : OK
*        CTEST                  : OK
*        TEST                   : OK
*
* 2013/10/28 Saar
* Parameter:
*       ZGEMM_DEFAULT_UNROLL_N  2
*       ZGEMM_DEFAULT_UNROLL_M  4 
*       ZGEMM_DEFAULT_P         256
*       ZGEMM_DEFAULT_Q         128
*	A_PR1			512
*	B_PR1			512
*
* 2014/07/28 Saar
* Performance at 4608x4608x4608:
*       1 thread:       53 GFLOPS	(SANDYBRIDGE:  29)	(MKL:   53)
*       2 threads:     101 GFLOPS	(SANDYBRIDGE:  59)	(MKL:  100)
*       3 threads:     146 GFLOPS	(SANDYBRIDGE:  86)	(MKL:  138)
*       4 threads:     184 GFLOPS	(SANDYBRIDGE: 108)	(MKL:  172)
*
********************************************************************************/


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


#if defined(BULLDOZER) 

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)

#define	VFMADDPD_R( y0,y1,y2 ) vfmaddpd y0,y1,y2,y0

#define	VFMADDPD_I( y0,y1,y2 ) vfmaddpd y0,y1,y2,y0

#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)

#define	VFMADDPD_R( y0,y1,y2 ) vfnmaddpd y0,y1,y2,y0

#define	VFMADDPD_I( y0,y1,y2 ) vfmaddpd y0,y1,y2,y0

#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)

#define	VFMADDPD_R( y0,y1,y2 ) vfmaddpd y0,y1,y2,y0

#define	VFMADDPD_I( y0,y1,y2 ) vfnmaddpd y0,y1,y2,y0

#else

#define	VFMADDPD_R( y0,y1,y2 ) vfnmaddpd y0,y1,y2,y0

#define	VFMADDPD_I( y0,y1,y2 ) vfnmaddpd y0,y1,y2,y0

#endif

#else

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)

#define	VFMADDPD_R( y0,y1,y2 ) vfmadd231pd y1,y2,y0

#define	VFMADDPD_I( y0,y1,y2 ) vfmadd231pd y1,y2,y0

#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)

#define	VFMADDPD_R( y0,y1,y2 ) vfnmadd231pd y1,y2,y0

#define	VFMADDPD_I( y0,y1,y2 ) vfmadd231pd y1,y2,y0

#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)

#define	VFMADDPD_R( y0,y1,y2 ) vfmadd231pd y1,y2,y0

#define	VFMADDPD_I( y0,y1,y2 ) vfnmadd231pd y1,y2,y0

#else

#define	VFMADDPD_R( y0,y1,y2 ) vfnmadd231pd y1,y2,y0

#define	VFMADDPD_I( y0,y1,y2 ) vfnmadd231pd y1,y2,y0

#endif

#endif

#define	A_PR1	512
#define	B_PR1	512



/***************************************************************************************************/

.macro KERNEL4x3_SUB
        vmovups                   (AO), %ymm0
        vmovups           4 * SIZE(AO), %ymm1
	prefetcht0	  A_PR1(AO)

        vbroadcastsd              (BO),   %ymm2
        vbroadcastsd      1 * SIZE(BO),   %ymm3
        VFMADDPD_R(        %ymm8 ,%ymm2,%ymm0 )
        VFMADDPD_R(        %ymm12,%ymm2,%ymm1 )
        VFMADDPD_I(        %ymm9 ,%ymm3,%ymm0 )
        VFMADDPD_I(        %ymm13,%ymm3,%ymm1 )

        vbroadcastsd      2 * SIZE(BO),   %ymm2
        vbroadcastsd      3 * SIZE(BO),   %ymm3
        VFMADDPD_R(        %ymm10,%ymm2,%ymm0 )
        VFMADDPD_R(        %ymm14,%ymm2,%ymm1 )
        VFMADDPD_I(        %ymm11,%ymm3,%ymm0 )
        VFMADDPD_I(        %ymm15,%ymm3,%ymm1 )

        vbroadcastsd      4 * SIZE(BO),   %ymm2
        vbroadcastsd      5 * SIZE(BO),   %ymm3
        VFMADDPD_R(        %ymm4 ,%ymm2,%ymm0 )
        VFMADDPD_R(        %ymm6 ,%ymm2,%ymm1 )
        VFMADDPD_I(        %ymm5 ,%ymm3,%ymm0 )
        VFMADDPD_I(        %ymm7 ,%ymm3,%ymm1 )

        addq    $ 6*SIZE, BO                           
        addq    $ 8*SIZE, AO                           
        decq	%rax                         
.endm

.macro SAVE4x3

	vbroadcastsd	ALPHA_R, %ymm0
	vbroadcastsd	ALPHA_I, %ymm1

	// swap high and low 8 bytes
        vshufpd $ 0x05, %ymm9 , %ymm9, %ymm9
        vshufpd $ 0x05, %ymm11, %ymm11, %ymm11
        vshufpd $ 0x05, %ymm13, %ymm13, %ymm13
        vshufpd $ 0x05, %ymm15, %ymm15, %ymm15
        vshufpd $ 0x05, %ymm5 , %ymm5 , %ymm5
        vshufpd $ 0x05, %ymm7 , %ymm7 , %ymm7

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubpd %ymm9, %ymm8 , %ymm8
        vaddsubpd %ymm11,%ymm10, %ymm10
        vaddsubpd %ymm13,%ymm12, %ymm12
        vaddsubpd %ymm15,%ymm14, %ymm14
        vaddsubpd %ymm5 ,%ymm4 , %ymm4
        vaddsubpd %ymm7 ,%ymm6 , %ymm6

        vshufpd $ 0x05, %ymm8 , %ymm8 , %ymm9
        vshufpd $ 0x05, %ymm10, %ymm10, %ymm11
        vshufpd $ 0x05, %ymm12, %ymm12, %ymm13
        vshufpd $ 0x05, %ymm14, %ymm14, %ymm15
        vshufpd $ 0x05, %ymm4 , %ymm4 , %ymm5
        vshufpd $ 0x05, %ymm6 , %ymm6 , %ymm7

#else
        vaddsubpd %ymm8,  %ymm9 ,%ymm9
        vaddsubpd %ymm10, %ymm11,%ymm11
        vaddsubpd %ymm12, %ymm13,%ymm13
        vaddsubpd %ymm14, %ymm15,%ymm15
        vaddsubpd %ymm4 , %ymm5 ,%ymm5
        vaddsubpd %ymm6 , %ymm7 ,%ymm7

        vmovapd   %ymm9,  %ymm8
        vmovapd   %ymm11, %ymm10
        vmovapd   %ymm13, %ymm12
        vmovapd   %ymm15, %ymm14
        vmovapd   %ymm5 , %ymm4
        vmovapd   %ymm7 , %ymm6

	// swap high and low 8 bytes
        vshufpd $ 0x05, %ymm9 , %ymm9, %ymm9
        vshufpd $ 0x05, %ymm11, %ymm11, %ymm11
        vshufpd $ 0x05, %ymm13, %ymm13, %ymm13
        vshufpd $ 0x05, %ymm15, %ymm15, %ymm15
        vshufpd $ 0x05, %ymm5 , %ymm5 , %ymm5
        vshufpd $ 0x05, %ymm7 , %ymm7 , %ymm7

#endif

	// multiply with ALPHA_R
        vmulpd  %ymm8 , %ymm0, %ymm8
        vmulpd  %ymm10, %ymm0, %ymm10
        vmulpd  %ymm12, %ymm0, %ymm12
        vmulpd  %ymm14, %ymm0, %ymm14
        vmulpd  %ymm4 , %ymm0, %ymm4
        vmulpd  %ymm6 , %ymm0, %ymm6

	// multiply with ALPHA_I
        vmulpd  %ymm9 , %ymm1, %ymm9
        vmulpd  %ymm11, %ymm1, %ymm11
        vmulpd  %ymm13, %ymm1, %ymm13
        vmulpd  %ymm15, %ymm1, %ymm15
        vmulpd  %ymm5 , %ymm1, %ymm5
        vmulpd  %ymm7 , %ymm1, %ymm7

	vaddsubpd %ymm9, %ymm8 , %ymm8
        vaddsubpd %ymm11,%ymm10, %ymm10
        vaddsubpd %ymm13,%ymm12, %ymm12
        vaddsubpd %ymm15,%ymm14, %ymm14
        vaddsubpd %ymm5 ,%ymm4 , %ymm4
        vaddsubpd %ymm7 ,%ymm6 , %ymm6



#ifndef TRMMKERNEL

	vaddpd 	 	(CO1), %ymm8 , %ymm8
	vaddpd  4 * SIZE(CO1), %ymm12, %ymm12

	vaddpd 	 	(CO1, LDC), %ymm10, %ymm10
	vaddpd  4 * SIZE(CO1, LDC), %ymm14, %ymm14

	vaddpd 	 	(CO1, LDC,2), %ymm4 , %ymm4
	vaddpd  4 * SIZE(CO1, LDC,2), %ymm6 , %ymm6
#endif

	vmovups	%ymm8 ,  	 (CO1)
	vmovups	%ymm12 , 4 * SIZE(CO1)

	vmovups	%ymm10 ,  	 (CO1, LDC)
	vmovups	%ymm14 , 4 * SIZE(CO1, LDC)

	vmovups	%ymm4  ,  	 (CO1, LDC, 2)
	vmovups	%ymm6  , 4 * SIZE(CO1, LDC, 2)

	prefetcht0	64(CO1)
	prefetcht0	64(CO1, LDC)

.endm



/***************************************************************************************************/

.macro KERNEL2x3_SUB
        vmovups                  (AO), %xmm0
        vmovups          2 * SIZE(AO), %xmm1
        vmovddup                 (BO), %xmm2
        vmovddup         1 * SIZE(BO), %xmm3

        VFMADDPD_R(        %xmm8 ,%xmm2,%xmm0 )
        VFMADDPD_R(        %xmm12,%xmm2,%xmm1 )
        VFMADDPD_I(        %xmm9 ,%xmm3,%xmm0 )
        VFMADDPD_I(        %xmm13,%xmm3,%xmm1 )

        vmovddup         2 * SIZE(BO), %xmm2
        vmovddup         3 * SIZE(BO), %xmm3
        VFMADDPD_R(        %xmm10,%xmm2,%xmm0 )
        VFMADDPD_R(        %xmm14,%xmm2,%xmm1 )
        VFMADDPD_I(        %xmm11,%xmm3,%xmm0 )
        VFMADDPD_I(        %xmm15,%xmm3,%xmm1 )

        vmovddup         4 * SIZE(BO), %xmm2
        vmovddup         5 * SIZE(BO), %xmm3
        VFMADDPD_R(        %xmm4 ,%xmm2,%xmm0 )
        VFMADDPD_R(        %xmm6 ,%xmm2,%xmm1 )
        VFMADDPD_I(        %xmm5 ,%xmm3,%xmm0 )
        VFMADDPD_I(        %xmm7 ,%xmm3,%xmm1 )

        addq    $ 6*SIZE, BO                           
        addq    $ 4*SIZE, AO                           
        decq    %rax                         
.endm

.macro SAVE2x3

	vmovddup	ALPHA_R, %xmm0
	vmovddup	ALPHA_I, %xmm1

	// swap high and low 64 bytes
        vshufpd $ 0x01, %xmm9 , %xmm9, %xmm9
        vshufpd $ 0x01, %xmm11, %xmm11, %xmm11
        vshufpd $ 0x01, %xmm13, %xmm13, %xmm13
        vshufpd $ 0x01, %xmm15, %xmm15, %xmm15
        vshufpd $ 0x01, %xmm5 , %xmm5 , %xmm5
        vshufpd $ 0x01, %xmm7 , %xmm7 , %xmm7

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubpd %xmm9, %xmm8 , %xmm8
        vaddsubpd %xmm11,%xmm10, %xmm10
        vaddsubpd %xmm13,%xmm12, %xmm12
        vaddsubpd %xmm15,%xmm14, %xmm14
        vaddsubpd %xmm5, %xmm4 , %xmm4
        vaddsubpd %xmm7, %xmm6 , %xmm6

        vshufpd $ 0x01, %xmm8 , %xmm8, %xmm9
        vshufpd $ 0x01, %xmm10, %xmm10, %xmm11
        vshufpd $ 0x01, %xmm12, %xmm12, %xmm13
        vshufpd $ 0x01, %xmm14, %xmm14, %xmm15
        vshufpd $ 0x01, %xmm4 , %xmm4, %xmm5
        vshufpd $ 0x01, %xmm6 , %xmm6, %xmm7

#else
        vaddsubpd %xmm8,  %xmm9 ,%xmm9
        vaddsubpd %xmm10, %xmm11,%xmm11
        vaddsubpd %xmm12, %xmm13,%xmm13
        vaddsubpd %xmm14, %xmm15,%xmm15
        vaddsubpd %xmm4,  %xmm5 ,%xmm5
        vaddsubpd %xmm6,  %xmm7 ,%xmm7

        vmovapd   %xmm9,  %xmm8
        vmovapd   %xmm11, %xmm10
        vmovapd   %xmm13, %xmm12
        vmovapd   %xmm15, %xmm14
        vmovapd   %xmm5,  %xmm4
        vmovapd   %xmm7,  %xmm6

	// swap high and low 64 bytes
        vshufpd $ 0x01, %xmm9 , %xmm9, %xmm9
        vshufpd $ 0x01, %xmm11, %xmm11, %xmm11
        vshufpd $ 0x01, %xmm13, %xmm13, %xmm13
        vshufpd $ 0x01, %xmm15, %xmm15, %xmm15
        vshufpd $ 0x01, %xmm5 , %xmm5, %xmm5
        vshufpd $ 0x01, %xmm7 , %xmm7, %xmm7

#endif

	// multiply with ALPHA_R
        vmulpd  %xmm8 , %xmm0, %xmm8
        vmulpd  %xmm10, %xmm0, %xmm10
        vmulpd  %xmm12, %xmm0, %xmm12
        vmulpd  %xmm14, %xmm0, %xmm14
        vmulpd  %xmm4 , %xmm0, %xmm4
        vmulpd  %xmm6 , %xmm0, %xmm6

	// multiply with ALPHA_I
        vmulpd  %xmm9 , %xmm1, %xmm9
        vmulpd  %xmm11, %xmm1, %xmm11
        vmulpd  %xmm13, %xmm1, %xmm13
        vmulpd  %xmm15, %xmm1, %xmm15
        vmulpd  %xmm5 , %xmm1, %xmm5
        vmulpd  %xmm7 , %xmm1, %xmm7

	vaddsubpd %xmm9, %xmm8 , %xmm8
        vaddsubpd %xmm11,%xmm10, %xmm10
        vaddsubpd %xmm13,%xmm12, %xmm12
        vaddsubpd %xmm15,%xmm14, %xmm14
	vaddsubpd %xmm5, %xmm4 , %xmm4
	vaddsubpd %xmm7, %xmm6 , %xmm6

#ifndef TRMMKERNEL

	vaddpd 	 	(CO1), %xmm8 , %xmm8
	vaddpd  2 * SIZE(CO1), %xmm12, %xmm12

	vaddpd 	 	(CO1, LDC), %xmm10, %xmm10
	vaddpd  2 * SIZE(CO1, LDC), %xmm14, %xmm14

	vaddpd 	 	(CO1, LDC,2), %xmm4 , %xmm4
	vaddpd  2 * SIZE(CO1, LDC,2), %xmm6 , %xmm6

#endif

	vmovups	%xmm8 ,  	(CO1)
	vmovups	%xmm12 , 2 * SIZE(CO1)

	vmovups	%xmm10 ,  	(CO1, LDC)
	vmovups	%xmm14 , 2 * SIZE(CO1, LDC)

	vmovups	%xmm4  ,  	(CO1, LDC,2)
	vmovups	%xmm6  , 2 * SIZE(CO1, LDC,2)

.endm


/************************************************************************************************/


.macro KERNEL1x3_SUB
        vmovups                  (AO), %xmm0
        vmovddup                 (BO), %xmm2
        vmovddup         1 * SIZE(BO), %xmm3

        VFMADDPD_R(        %xmm8,%xmm2,%xmm0 )
        VFMADDPD_I(        %xmm9,%xmm3,%xmm0 )

        vmovddup         2 * SIZE(BO), %xmm2
        vmovddup         3 * SIZE(BO), %xmm3
        VFMADDPD_R(        %xmm10,%xmm2,%xmm0 )
        VFMADDPD_I(        %xmm11,%xmm3,%xmm0 )

        vmovddup         4 * SIZE(BO), %xmm2
        vmovddup         5 * SIZE(BO), %xmm3
        VFMADDPD_R(        %xmm4 ,%xmm2,%xmm0 )
        VFMADDPD_I(        %xmm5 ,%xmm3,%xmm0 )

        addq    $ 6*SIZE, BO                           
        addq    $ 2*SIZE, AO                           
        decq    %rax                         
.endm

.macro SAVE1x3

	vmovddup	ALPHA_R, %xmm0
	vmovddup	ALPHA_I, %xmm1

	// swap high and low 64 bytes
        vshufpd $ 0x01, %xmm9 , %xmm9, %xmm9
        vshufpd $ 0x01, %xmm11, %xmm11, %xmm11
        vshufpd $ 0x01, %xmm5 , %xmm5, %xmm5

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubpd %xmm9, %xmm8 , %xmm8
        vaddsubpd %xmm11,%xmm10, %xmm10
        vaddsubpd %xmm5, %xmm4 , %xmm4

        vshufpd $ 0x01, %xmm8 , %xmm8, %xmm9
        vshufpd $ 0x01, %xmm10, %xmm10, %xmm11
        vshufpd $ 0x01, %xmm4 , %xmm4, %xmm5

#else
        vaddsubpd %xmm8, %xmm9, %xmm9
        vaddsubpd %xmm10,%xmm11, %xmm11
        vaddsubpd %xmm4, %xmm5, %xmm5

        vmovapd   %xmm9,  %xmm8
        vmovapd   %xmm11, %xmm10
        vmovapd   %xmm5,  %xmm4

	// swap high and low 64 bytes
        vshufpd $ 0x01, %xmm9 , %xmm9, %xmm9
        vshufpd $ 0x01, %xmm11, %xmm11, %xmm11
        vshufpd $ 0x01, %xmm5 , %xmm5, %xmm5

#endif

	// multiply with ALPHA_R
        vmulpd  %xmm8 , %xmm0, %xmm8
        vmulpd  %xmm10, %xmm0, %xmm10
        vmulpd  %xmm4 , %xmm0, %xmm4

	// multiply with ALPHA_I
        vmulpd  %xmm9 , %xmm1, %xmm9
        vmulpd  %xmm11, %xmm1, %xmm11
        vmulpd  %xmm5 , %xmm1, %xmm5

	vaddsubpd %xmm9, %xmm8 , %xmm8
        vaddsubpd %xmm11,%xmm10, %xmm10
	vaddsubpd %xmm5, %xmm4 , %xmm4

#ifndef TRMMKERNEL

	vaddpd 	 	(CO1)        , %xmm8 , %xmm8
	vaddpd 	 	(CO1, LDC)   , %xmm10, %xmm10
	vaddpd 	 	(CO1, LDC,2) , %xmm4 , %xmm4

#endif

	vmovups	%xmm8 ,  	(CO1)
	vmovups	%xmm10 ,  	(CO1, LDC)
	vmovups	%xmm4  ,  	(CO1, LDC,2)

.endm




/***************************************************************************************************/

.macro KERNEL4x2_SUB
        vmovups           -8 * SIZE(AO, %rax, SIZE), %ymm0
        vmovups           -4 * SIZE(AO, %rax, SIZE), %ymm1

        vbroadcastsd      -8 * SIZE(BO, BI, SIZE),   %ymm4
        vbroadcastsd      -7 * SIZE(BO, BI, SIZE),   %ymm5
        VFMADDPD_R(        %ymm8 ,%ymm4,%ymm0 )
        VFMADDPD_R(        %ymm12,%ymm4,%ymm1 )
        vbroadcastsd      -6 * SIZE(BO, BI, SIZE),   %ymm6
        VFMADDPD_I(        %ymm9 ,%ymm5,%ymm0 )
        VFMADDPD_I(        %ymm13,%ymm5,%ymm1 )
        vbroadcastsd      -5 * SIZE(BO, BI, SIZE),   %ymm7
        VFMADDPD_R(        %ymm10,%ymm6,%ymm0 )
        VFMADDPD_R(        %ymm14,%ymm6,%ymm1 )
        VFMADDPD_I(        %ymm11,%ymm7,%ymm0 )
        VFMADDPD_I(        %ymm15,%ymm7,%ymm1 )

        addq    $ 4, BI                           
        addq    $ 8, %rax                         
.endm

.macro SAVE4x2

	vbroadcastsd	ALPHA_R, %ymm0
	vbroadcastsd	ALPHA_I, %ymm1

	// swap high and low 8 bytes
        vshufpd $ 0x05, %ymm9 , %ymm9, %ymm9
        vshufpd $ 0x05, %ymm11, %ymm11, %ymm11
        vshufpd $ 0x05, %ymm13, %ymm13, %ymm13
        vshufpd $ 0x05, %ymm15, %ymm15, %ymm15

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubpd %ymm9, %ymm8 , %ymm8
        vaddsubpd %ymm11,%ymm10, %ymm10
        vaddsubpd %ymm13,%ymm12, %ymm12
        vaddsubpd %ymm15,%ymm14, %ymm14

        vshufpd $ 0x05, %ymm8 , %ymm8, %ymm9
        vshufpd $ 0x05, %ymm10, %ymm10, %ymm11
        vshufpd $ 0x05, %ymm12, %ymm12, %ymm13
        vshufpd $ 0x05, %ymm14, %ymm14, %ymm15

#else
        vaddsubpd %ymm8,  %ymm9 ,%ymm9
        vaddsubpd %ymm10, %ymm11,%ymm11
        vaddsubpd %ymm12, %ymm13,%ymm13
        vaddsubpd %ymm14, %ymm15,%ymm15

        vmovapd   %ymm9,  %ymm8
        vmovapd   %ymm11, %ymm10
        vmovapd   %ymm13, %ymm12
        vmovapd   %ymm15, %ymm14

	// swap high and low 8 bytes
        vshufpd $ 0x05, %ymm9 , %ymm9, %ymm9
        vshufpd $ 0x05, %ymm11, %ymm11, %ymm11
        vshufpd $ 0x05, %ymm13, %ymm13, %ymm13
        vshufpd $ 0x05, %ymm15, %ymm15, %ymm15

#endif

	// multiply with ALPHA_R
        vmulpd  %ymm8 , %ymm0, %ymm8
        vmulpd  %ymm10, %ymm0, %ymm10
        vmulpd  %ymm12, %ymm0, %ymm12
        vmulpd  %ymm14, %ymm0, %ymm14

	// multiply with ALPHA_I
        vmulpd  %ymm9 , %ymm1, %ymm9
        vmulpd  %ymm11, %ymm1, %ymm11
        vmulpd  %ymm13, %ymm1, %ymm13
        vmulpd  %ymm15, %ymm1, %ymm15

	vaddsubpd %ymm9, %ymm8 , %ymm8
        vaddsubpd %ymm11,%ymm10, %ymm10
        vaddsubpd %ymm13,%ymm12, %ymm12
        vaddsubpd %ymm15,%ymm14, %ymm14



#ifndef TRMMKERNEL

	vaddpd 	 	(CO1), %ymm8 , %ymm8
	vaddpd  4 * SIZE(CO1), %ymm12, %ymm12

	vaddpd 	 	(CO1, LDC), %ymm10, %ymm10
	vaddpd  4 * SIZE(CO1, LDC), %ymm14, %ymm14

#endif

	vmovups	%ymm8 ,  	(CO1)
	vmovups	%ymm12 , 4 * SIZE(CO1)

	vmovups	%ymm10 ,  	(CO1, LDC)
	vmovups	%ymm14 , 4 * SIZE(CO1, LDC)

	prefetcht0	64(CO1)
	prefetcht0	64(CO1, LDC)

.endm

/***************************************************************************************************/

.macro KERNEL2x2_SUB
        vmovups          -8 * SIZE(AO, %rax, SIZE), %xmm0
        vmovddup         -8 * SIZE(BO, BI, SIZE), %xmm4
        vmovups          -6 * SIZE(AO, %rax, SIZE), %xmm1
        VFMADDPD_R(        %xmm8,%xmm4,%xmm0  )
        VFMADDPD_R(        %xmm12,%xmm4,%xmm1 )
        vmovddup         -7 * SIZE(BO, BI, SIZE), %xmm5
        VFMADDPD_I(        %xmm9,%xmm5,%xmm0  )
        VFMADDPD_I(        %xmm13,%xmm5,%xmm1 )
        vmovddup         -6 * SIZE(BO, BI, SIZE), %xmm6
        VFMADDPD_R(        %xmm10,%xmm6,%xmm0 )
        VFMADDPD_R(        %xmm14,%xmm6,%xmm1 )
        vmovddup         -5 * SIZE(BO, BI, SIZE), %xmm7
        VFMADDPD_I(        %xmm11,%xmm7,%xmm0 )
        VFMADDPD_I(        %xmm15,%xmm7,%xmm1 )
        addq    $ 4, BI                           
        addq    $ 4, %rax                         
.endm

.macro SAVE2x2

	vmovddup	ALPHA_R, %xmm0
	vmovddup	ALPHA_I, %xmm1

	// swap high and low 64 bytes
        vshufpd $ 0x01, %xmm9 , %xmm9, %xmm9
        vshufpd $ 0x01, %xmm11, %xmm11, %xmm11
        vshufpd $ 0x01, %xmm13, %xmm13, %xmm13
        vshufpd $ 0x01, %xmm15, %xmm15, %xmm15

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubpd %xmm9, %xmm8 , %xmm8
        vaddsubpd %xmm11,%xmm10, %xmm10
        vaddsubpd %xmm13,%xmm12, %xmm12
        vaddsubpd %xmm15,%xmm14, %xmm14

        vshufpd $ 0x01, %xmm8 , %xmm8, %xmm9
        vshufpd $ 0x01, %xmm10, %xmm10, %xmm11
        vshufpd $ 0x01, %xmm12, %xmm12, %xmm13
        vshufpd $ 0x01, %xmm14, %xmm14, %xmm15

#else
        vaddsubpd %xmm8,  %xmm9 ,%xmm9
        vaddsubpd %xmm10, %xmm11,%xmm11
        vaddsubpd %xmm12, %xmm13,%xmm13
        vaddsubpd %xmm14, %xmm15,%xmm15

        vmovapd   %xmm9,  %xmm8
        vmovapd   %xmm11, %xmm10
        vmovapd   %xmm13, %xmm12
        vmovapd   %xmm15, %xmm14

	// swap high and low 64 bytes
        vshufpd $ 0x01, %xmm9 , %xmm9, %xmm9
        vshufpd $ 0x01, %xmm11, %xmm11, %xmm11
        vshufpd $ 0x01, %xmm13, %xmm13, %xmm13
        vshufpd $ 0x01, %xmm15, %xmm15, %xmm15

#endif

	// multiply with ALPHA_R
        vmulpd  %xmm8 , %xmm0, %xmm8
        vmulpd  %xmm10, %xmm0, %xmm10
        vmulpd  %xmm12, %xmm0, %xmm12
        vmulpd  %xmm14, %xmm0, %xmm14

	// multiply with ALPHA_I
        vmulpd  %xmm9 , %xmm1, %xmm9
        vmulpd  %xmm11, %xmm1, %xmm11
        vmulpd  %xmm13, %xmm1, %xmm13
        vmulpd  %xmm15, %xmm1, %xmm15

	vaddsubpd %xmm9, %xmm8 , %xmm8
        vaddsubpd %xmm11,%xmm10, %xmm10
        vaddsubpd %xmm13,%xmm12, %xmm12
        vaddsubpd %xmm15,%xmm14, %xmm14



#ifndef TRMMKERNEL

	vaddpd 	 	(CO1), %xmm8 , %xmm8
	vaddpd  2 * SIZE(CO1), %xmm12, %xmm12

	vaddpd 	 	(CO1, LDC), %xmm10, %xmm10
	vaddpd  2 * SIZE(CO1, LDC), %xmm14, %xmm14

#endif

	vmovups	%xmm8 ,  	(CO1)
	vmovups	%xmm12 , 2 * SIZE(CO1)

	vmovups	%xmm10 ,  	(CO1, LDC)
	vmovups	%xmm14 , 2 * SIZE(CO1, LDC)

.endm

/************************************************************************************************/

/************************************************************************************************/


.macro KERNEL1x2_SUB
        vmovups          -8 * SIZE(AO, %rax, SIZE), %xmm0
        vmovddup         -8 * SIZE(BO, BI, SIZE), %xmm4
        vmovddup         -7 * SIZE(BO, BI, SIZE), %xmm5
        VFMADDPD_R(        %xmm8,%xmm4,%xmm0 )
        VFMADDPD_I(        %xmm9,%xmm5,%xmm0 )
        vmovddup         -6 * SIZE(BO, BI, SIZE), %xmm6
        vmovddup         -5 * SIZE(BO, BI, SIZE), %xmm7
        VFMADDPD_R(        %xmm10,%xmm6,%xmm0 )
        VFMADDPD_I(        %xmm11,%xmm7,%xmm0 )
        addq    $ 4, BI                           
        addq    $ 2, %rax                         
.endm

.macro SAVE1x2

	vmovddup	ALPHA_R, %xmm0
	vmovddup	ALPHA_I, %xmm1

	// swap high and low 64 bytes
        vshufpd $ 0x01, %xmm9 , %xmm9, %xmm9
        vshufpd $ 0x01, %xmm11, %xmm11, %xmm11

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubpd %xmm9, %xmm8 , %xmm8
        vaddsubpd %xmm11,%xmm10, %xmm10

        vshufpd $ 0x01, %xmm8 , %xmm8, %xmm9
        vshufpd $ 0x01, %xmm10, %xmm10, %xmm11

#else
        vaddsubpd %xmm8, %xmm9, %xmm9
        vaddsubpd %xmm10,%xmm11, %xmm11

        vmovapd   %xmm9,  %xmm8
        vmovapd   %xmm11, %xmm10

	// swap high and low 64 bytes
        vshufpd $ 0x01, %xmm9 , %xmm9, %xmm9
        vshufpd $ 0x01, %xmm11, %xmm11, %xmm11

#endif

	// multiply with ALPHA_R
        vmulpd  %xmm8 , %xmm0, %xmm8
        vmulpd  %xmm10, %xmm0, %xmm10

	// multiply with ALPHA_I
        vmulpd  %xmm9 , %xmm1, %xmm9
        vmulpd  %xmm11, %xmm1, %xmm11

	vaddsubpd %xmm9, %xmm8 , %xmm8
        vaddsubpd %xmm11,%xmm10, %xmm10

#ifndef TRMMKERNEL

	vaddpd 	 	(CO1), %xmm8 , %xmm8
	vaddpd 	 	(CO1, LDC), %xmm10, %xmm10

#endif

	vmovups	%xmm8 ,  	(CO1)
	vmovups	%xmm10 ,  	(CO1, LDC)

.endm


/************************************************************************************************/

.macro KERNEL4x1_SUB
        vmovups          -8 * SIZE(AO, %rax, SIZE), %ymm0
        vmovups          -4 * SIZE(AO, %rax, SIZE), %ymm1
        vbroadcastsd     -4 * SIZE(BO, BI, SIZE)  , %ymm4
        vbroadcastsd     -3 * SIZE(BO, BI, SIZE)  , %ymm5
        VFMADDPD_R(        %ymm8 ,%ymm4,%ymm0 )
        VFMADDPD_R(        %ymm12,%ymm4,%ymm1 )
        VFMADDPD_I(        %ymm9 ,%ymm5,%ymm0 )
        VFMADDPD_I(        %ymm13,%ymm5,%ymm1 )

        addq    $ 2, BI                           
        addq    $ 8, %rax                         
.endm

.macro SAVE4x1

	vbroadcastsd	ALPHA_R, %ymm0
	vbroadcastsd	ALPHA_I, %ymm1

	// swap high and low 8 bytes
        vshufpd $ 0x05, %ymm9 , %ymm9, %ymm9
        vshufpd $ 0x05, %ymm13, %ymm13, %ymm13

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubpd %ymm9, %ymm8  , %ymm8
        vaddsubpd %ymm13,%ymm12 , %ymm12

        vshufpd $ 0x05, %ymm8 , %ymm8, %ymm9
        vshufpd $ 0x05, %ymm12, %ymm12, %ymm13

#else
        vaddsubpd %ymm8, %ymm9 , %ymm9
        vaddsubpd %ymm12,%ymm13, %ymm13

        vmovapd   %ymm9,  %ymm8
        vmovapd   %ymm13, %ymm12

	// swap high and low 8 bytes
        vshufpd $ 0x05, %ymm9 , %ymm9, %ymm9
        vshufpd $ 0x05, %ymm13, %ymm13, %ymm13

#endif

	// multiply with ALPHA_R
        vmulpd  %ymm8 , %ymm0, %ymm8
        vmulpd  %ymm12, %ymm0, %ymm12

	// multiply with ALPHA_I
        vmulpd  %ymm9 , %ymm1, %ymm9
        vmulpd  %ymm13, %ymm1, %ymm13

	vaddsubpd %ymm9,  %ymm8 , %ymm8
        vaddsubpd %ymm13, %ymm12, %ymm12



#ifndef TRMMKERNEL

	vaddpd 	 	(CO1), %ymm8 , %ymm8
	vaddpd  4 * SIZE(CO1), %ymm12, %ymm12

#endif

	vmovups	%ymm8 ,  	(CO1)
	vmovups	%ymm12 ,4 * SIZE(CO1)

.endm



/************************************************************************************************/

.macro KERNEL2x1_SUB
        vmovups          -8 * SIZE(AO, %rax, SIZE), %xmm0
        vmovddup         -4 * SIZE(BO, BI, SIZE), %xmm4
        VFMADDPD_R(        %xmm8,%xmm4,%xmm0  )
        vmovups          -6 * SIZE(AO, %rax, SIZE), %xmm1
        VFMADDPD_R(        %xmm12,%xmm4,%xmm1 )
        vmovddup         -3 * SIZE(BO, BI, SIZE), %xmm5
        VFMADDPD_I(        %xmm9,%xmm5,%xmm0  )
        VFMADDPD_I(        %xmm13,%xmm5,%xmm1 )
        addq    $ 2, BI                           
        addq    $ 4, %rax                         
.endm

.macro SAVE2x1

	vmovddup	ALPHA_R, %xmm0
	vmovddup	ALPHA_I, %xmm1

	// swap high and low 64 bytes
        vshufpd $ 0x01, %xmm9 , %xmm9, %xmm9
        vshufpd $ 0x01, %xmm13, %xmm13, %xmm13

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubpd %xmm9, %xmm8  , %xmm8
        vaddsubpd %xmm13,%xmm12 , %xmm12

        vshufpd $ 0x01, %xmm8 , %xmm8, %xmm9
        vshufpd $ 0x01, %xmm12, %xmm12, %xmm13

#else
        vaddsubpd %xmm8, %xmm9 , %xmm9
        vaddsubpd %xmm12,%xmm13, %xmm13

        vmovapd   %xmm9,  %xmm8
        vmovapd   %xmm13, %xmm12

	// swap high and low 64 bytes
        vshufpd $ 0x01, %xmm9 , %xmm9, %xmm9
        vshufpd $ 0x01, %xmm13, %xmm13, %xmm13

#endif

	// multiply with ALPHA_R
        vmulpd  %xmm8 , %xmm0, %xmm8
        vmulpd  %xmm12, %xmm0, %xmm12

	// multiply with ALPHA_I
        vmulpd  %xmm9 , %xmm1, %xmm9
        vmulpd  %xmm13, %xmm1, %xmm13

	vaddsubpd %xmm9,  %xmm8 , %xmm8
        vaddsubpd %xmm13, %xmm12, %xmm12

#ifndef TRMMKERNEL

	vaddpd 	 	(CO1), %xmm8 , %xmm8
	vaddpd  2 * SIZE(CO1), %xmm12, %xmm12

#endif

	vmovups	%xmm8 ,  	(CO1)
	vmovups	%xmm12 , 2 * SIZE(CO1)

.endm


/************************************************************************************************/

.macro KERNEL1x1_SUB
        vmovups          -8 * SIZE(AO, %rax, SIZE), %xmm0
        vmovddup         -4 * SIZE(BO, BI, SIZE), %xmm4
        VFMADDPD_R(        %xmm8,%xmm4,%xmm0 )
        vmovddup         -3 * SIZE(BO, BI, SIZE), %xmm5
        VFMADDPD_I(        %xmm9,%xmm5,%xmm0 )
        addq    $ 2, BI                           
        addq    $ 2, %rax                         
.endm

.macro SAVE1x1

	vmovddup	ALPHA_R, %xmm0
	vmovddup	ALPHA_I, %xmm1

	// swap high and low 64 bytes
        vshufpd $ 0x01, %xmm9 , %xmm9, %xmm9

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubpd %xmm9, %xmm8,  %xmm8

        vshufpd $ 0x01, %xmm8 , %xmm8, %xmm9

#else
        vaddsubpd %xmm8, %xmm9,  %xmm9

        vmovapd   %xmm9,  %xmm8

	// swap high and low 64 bytes
        vshufpd $ 0x01, %xmm9 , %xmm9, %xmm9

#endif

	// multiply with ALPHA_R
        vmulpd  %xmm8 , %xmm0, %xmm8

	// multiply with ALPHA_I
        vmulpd  %xmm9 , %xmm1, %xmm9

	vaddsubpd %xmm9 ,%xmm8,  %xmm8

#ifndef TRMMKERNEL

	vaddpd 	 	(CO1), %xmm8 , %xmm8

#endif

	vmovups	%xmm8 ,  	(CO1)

.endm


/************************************************************************************************/



#if !defined(TRMMKERNEL)


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

	vmovsd	 %xmm0, ALPHA_R
	vmovsd	 %xmm1, ALPHA_I

	salq	$ ZBASE_SHIFT, LDC

	movq    N, %rax
        xorq    %rdx, %rdx
        movq    $ 6,  %rdi
        divq    %rdi                    //    N / 6
        movq    %rax, Ndiv6             //    N / 6
        movq    %rdx, Nmod6             //    N % 6

	

/************************************************************************************************/
.L6_00_0:

	movq	Ndiv6,  J
	cmpq	$ 0, J
	je	.L2_00_0
	ALIGN_4



.L6_00_01:
	// copy to sub buffer
	movq	B, BO1
	leaq    BUFFER1, BO		// first buffer to BO
	movq	K, %rax
	salq	$2, %rax		// 2 * COMPSIZE
	leaq	(B, %rax,8), BO2 
	movq	BO2, B			// next offset of B
	movq	K, %rax
	ALIGN_4

.L6_00_02b:

	vmovups		(BO1), %xmm0
	vmovups	2 * SIZE(BO1), %xmm1
	vmovups	        (BO2), %xmm2
	vmovups	%xmm0,         (BO)
	vmovups	%xmm1, 2 * SIZE(BO)
	vmovups	%xmm2, 4 * SIZE(BO)
	addq	$ 4*SIZE,BO1
	addq	$ 4*SIZE,BO2
	addq	$ 6*SIZE,BO
	decq	%rax
	jnz	.L6_00_02b

.L6_00_02c:



.L6_00_10:
	movq	C, CO1
	leaq	(C, LDC, 2), C		// c += 2 * ldc
	leaq	(C, LDC, 1), C		// c += 1 * ldc

	movq	A, AO		 	// aoffset = a

	movq	M,  I
	sarq	$ 2, I			// i = (m >> 2)
	je	.L6_2_10

	ALIGN_4

/******************************************************************************************************************/

.L6_4_11:

	leaq	BUFFER1, BO		// first buffer to BO

	vzeroall

        movq    K, %rax

	andq	$ -8, %rax			//  K = K - ( K % 8 )
	je	.L6_4_16
	ALIGN_4

.L6_4_12:

	KERNEL4x3_SUB
	KERNEL4x3_SUB
	KERNEL4x3_SUB
	KERNEL4x3_SUB

	KERNEL4x3_SUB
	KERNEL4x3_SUB
	KERNEL4x3_SUB
	KERNEL4x3_SUB

	je	.L6_4_16

	KERNEL4x3_SUB
	KERNEL4x3_SUB
	KERNEL4x3_SUB
	KERNEL4x3_SUB

	KERNEL4x3_SUB
	KERNEL4x3_SUB
	KERNEL4x3_SUB
	KERNEL4x3_SUB

	je	.L6_4_16

	jmp	.L6_4_12
	ALIGN_4

.L6_4_16:
        movq    K, %rax

	andq	$ 7, %rax		# if (k & 1)
	je .L6_4_19
	ALIGN_4

.L6_4_17:

	KERNEL4x3_SUB

	jnz	.L6_4_17
	ALIGN_4


.L6_4_19:

	SAVE4x3

	addq	$ 8 * SIZE, CO1		# coffset += 8
	decq	I			# i --
	jg	.L6_4_11
	ALIGN_4	


/**************************************************************************
* Rest of M 
***************************************************************************/


/******************************************************************************************************************/
.L6_2_10:
	testq	$ 2, M		
	jz	.L6_2_40		// to next 2 lines of N

.L6_2_11:

	leaq	BUFFER1, BO		// first buffer to BO

	vzeroall

        movq    K, %rax

	andq	$ -8, %rax			//  K = K - ( K % 8 )
	je	.L6_2_16
	ALIGN_4

.L6_2_12:

	KERNEL2x3_SUB
	KERNEL2x3_SUB
	KERNEL2x3_SUB
	KERNEL2x3_SUB

	KERNEL2x3_SUB
	KERNEL2x3_SUB
	KERNEL2x3_SUB
	KERNEL2x3_SUB

	je	.L6_2_16

	KERNEL2x3_SUB
	KERNEL2x3_SUB
	KERNEL2x3_SUB
	KERNEL2x3_SUB

	KERNEL2x3_SUB
	KERNEL2x3_SUB
	KERNEL2x3_SUB
	KERNEL2x3_SUB

	je	.L6_2_16

	jmp	.L6_2_12
	ALIGN_4

.L6_2_16:
        movq    K, %rax

	andq	$ 7, %rax		# if (k & 1)
	je .L6_2_19
	ALIGN_4

.L6_2_17:

	KERNEL2x3_SUB

	jnz	.L6_2_17
	ALIGN_4


.L6_2_19:

	SAVE2x3

	addq	$ 4 * SIZE, CO1		# coffset += 4
	ALIGN_4	


/**************************************************************************
* Rest of M 
***************************************************************************/
.L6_2_40:
	testq	$ 1, M		
	jz	.L6_2_60		// to next 2 lines of N

	ALIGN_4

.L6_2_41:

	leaq	BUFFER1, BO		// first buffer to BO

	vzeroall

        movq    K, %rax

	andq	$ -8, %rax			//  K = K - ( K % 8 )
	je	.L6_2_46

	ALIGN_4

.L6_2_42:

	KERNEL1x3_SUB
	KERNEL1x3_SUB
	KERNEL1x3_SUB
	KERNEL1x3_SUB

	KERNEL1x3_SUB
	KERNEL1x3_SUB
	KERNEL1x3_SUB
	KERNEL1x3_SUB

	je	.L6_2_46

	KERNEL1x3_SUB
	KERNEL1x3_SUB
	KERNEL1x3_SUB
	KERNEL1x3_SUB

	KERNEL1x3_SUB
	KERNEL1x3_SUB
	KERNEL1x3_SUB
	KERNEL1x3_SUB

	je	.L6_2_46

	jmp	.L6_2_42
	ALIGN_4

.L6_2_46:
        movq    K, %rax

	andq	$ 7, %rax		# if (k & 1)
	je .L6_2_49

	ALIGN_4

.L6_2_47:

	KERNEL1x3_SUB

	jnz	.L6_2_47
	ALIGN_4


.L6_2_49:

	SAVE1x3

	addq	$ 2 * SIZE, CO1		# coffset += 2
	decq	I			# i --
	jg	.L6_2_41
	ALIGN_4	



	
.L6_2_60:


/************************************************************************************************/

/************************************************************************************************/


.L7_00_01:
	// copy to sub buffer
	movq	B, BO1
	leaq    BUFFER1, BO		// first buffer to BO
	movq	K, %rax
	salq	$2, %rax		// 2 * COMPSIZE
	leaq	(B, %rax,8), BO2 
	movq	K, %rax
	ALIGN_4

.L7_00_02b:

	vmovups	2 * SIZE(BO1), %xmm0
	vmovups	        (BO2), %xmm1
	vmovups	2 * SIZE(BO2), %xmm2
	vmovups	%xmm0,         (BO)
	vmovups	%xmm1, 2 * SIZE(BO)
	vmovups	%xmm2, 4 * SIZE(BO)
	addq	$ 4*SIZE,BO1
	addq	$ 4*SIZE,BO2
	addq	$ 6*SIZE,BO
	decq	%rax
	jnz	.L7_00_02b

.L7_00_02c:

	movq	BO2, B			// next offset of B


.L7_00_10:
	movq	C, CO1
	leaq	(C, LDC, 2), C		// c += 2 * ldc
	leaq	(C, LDC, 1), C		// c += 1 * ldc

	movq	A, AO		 	// aoffset = a

	movq	M,  I
	sarq	$ 2, I			// i = (m >> 2)
	je	.L7_2_10

	ALIGN_4

/******************************************************************************************************************/

.L7_4_11:

	leaq	BUFFER1, BO		// first buffer to BO

	vzeroall

        movq    K, %rax

	andq	$ -8, %rax			//  K = K - ( K % 8 )
	je	.L7_4_16
	ALIGN_4

.L7_4_12:

	KERNEL4x3_SUB
	KERNEL4x3_SUB
	KERNEL4x3_SUB
	KERNEL4x3_SUB

	KERNEL4x3_SUB
	KERNEL4x3_SUB
	KERNEL4x3_SUB
	KERNEL4x3_SUB

	je	.L7_4_16

	KERNEL4x3_SUB
	KERNEL4x3_SUB
	KERNEL4x3_SUB
	KERNEL4x3_SUB

	KERNEL4x3_SUB
	KERNEL4x3_SUB
	KERNEL4x3_SUB
	KERNEL4x3_SUB

	je	.L7_4_16

	jmp	.L7_4_12
	ALIGN_4

.L7_4_16:
        movq    K, %rax

	andq	$ 7, %rax		# if (k & 1)
	je .L7_4_19

	ALIGN_4

.L7_4_17:

	KERNEL4x3_SUB

	jnz	.L7_4_17
	ALIGN_4


.L7_4_19:

	SAVE4x3

	addq	$ 8 * SIZE, CO1		# coffset += 8
	decq	I			# i --
	jg	.L7_4_11
	ALIGN_4	


/**************************************************************************
* Rest of M 
***************************************************************************/


/******************************************************************************************************************/
.L7_2_10:
	testq	$ 2, M		
	jz	.L7_2_40		// to next 2 lines of N

.L7_2_11:

	leaq	BUFFER1, BO		// first buffer to BO

	vzeroall

        movq    K, %rax

	andq	$ -8, %rax			//  K = K - ( K % 8 )
	je	.L7_2_16
	ALIGN_4

.L7_2_12:

	KERNEL2x3_SUB
	KERNEL2x3_SUB
	KERNEL2x3_SUB
	KERNEL2x3_SUB

	KERNEL2x3_SUB
	KERNEL2x3_SUB
	KERNEL2x3_SUB
	KERNEL2x3_SUB

	je	.L7_2_16

	KERNEL2x3_SUB
	KERNEL2x3_SUB
	KERNEL2x3_SUB
	KERNEL2x3_SUB

	KERNEL2x3_SUB
	KERNEL2x3_SUB
	KERNEL2x3_SUB
	KERNEL2x3_SUB

	je	.L7_2_16

	jmp	.L7_2_12
	ALIGN_4

.L7_2_16:
        movq    K, %rax

	andq	$ 7, %rax		# if (k & 1)
	je .L7_2_19

	ALIGN_4

.L7_2_17:

	KERNEL2x3_SUB

	jnz	.L7_2_17
	ALIGN_4


.L7_2_19:

	SAVE2x3

	addq	$ 4 * SIZE, CO1		# coffset += 4
	ALIGN_4	


/**************************************************************************
* Rest of M 
***************************************************************************/
.L7_2_40:
	testq	$ 1, M		
	jz	.L7_2_60		// to next 2 lines of N

	ALIGN_4

.L7_2_41:

	leaq	BUFFER1, BO		// first buffer to BO

	vzeroall

        movq    K, %rax

	andq	$ -8, %rax			//  K = K - ( K % 8 )
	je	.L7_2_46

	ALIGN_4

.L7_2_42:

	KERNEL1x3_SUB
	KERNEL1x3_SUB
	KERNEL1x3_SUB
	KERNEL1x3_SUB

	KERNEL1x3_SUB
	KERNEL1x3_SUB
	KERNEL1x3_SUB
	KERNEL1x3_SUB

	je	.L7_2_46

	KERNEL1x3_SUB
	KERNEL1x3_SUB
	KERNEL1x3_SUB
	KERNEL1x3_SUB

	KERNEL1x3_SUB
	KERNEL1x3_SUB
	KERNEL1x3_SUB
	KERNEL1x3_SUB

	je	.L7_2_46

	jmp	.L7_2_42
	ALIGN_4

.L7_2_46:
        movq    K, %rax

	andq	$ 7, %rax		# if (k & 1)
	je .L7_2_49
	ALIGN_4

.L7_2_47:

	KERNEL1x3_SUB

	jnz	.L7_2_47
	ALIGN_4


.L7_2_49:

	SAVE1x3

	addq	$ 2 * SIZE, CO1		# coffset += 2
	decq	I			# i --
	jg	.L7_2_41
	ALIGN_4	



	
.L7_2_60:

	decq	J			// j --
	jg	.L6_00_01		// next 6 lines of N

/************************************************************************************************/



/************************************************************************************************/
.L2_00_0:

	movq	Nmod6,  J
	sarq	$1, J		// j = j / 2
	cmpq	$ 0, J
	je	.L1_2_0
	ALIGN_4



.L2_00_01:
	// copy to sub buffer
	movq	B, BO1
	leaq    BUFFER1, BO		// first buffer to BO
	movq	K, %rax
	ALIGN_4

.L2_00_02b:

	vmovups		(BO1), %xmm0
	vmovups	2 * SIZE(BO1), %xmm1
	vmovups	%xmm0,       (BO)
	vmovups	%xmm1, 2 * SIZE(BO)
	addq	$ 4*SIZE,BO1
	addq	$ 4*SIZE,BO
	decq	%rax
	jnz	.L2_00_02b

.L2_00_02c:

	movq	BO1, B			// next offset of B


.L2_00_10:
	movq	C, CO1
	leaq	(C, LDC, 2), C		// c += 2 * ldc

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
        movq    %rax, KK
#endif
	
	movq	A, AO		 	// aoffset = a
	addq	$ 8 * SIZE, AO

	movq	M,  I
	sarq	$ 2, I			// i = (m >> 2)
	je	.L2_2_10

	ALIGN_4

/******************************************************************************************************************/

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

        prefetcht0      A_PR1(AO,%rax,SIZE)
        prefetcht0      B_PR1(BO,BI  ,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
        prefetcht0      B_PR1(BO,BI  ,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB

        prefetcht0      A_PR1(AO,%rax,SIZE)
        prefetcht0      B_PR1(BO,BI  ,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
        prefetcht0      B_PR1(BO,BI  ,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB

	je	.L2_4_16

        prefetcht0      A_PR1(AO,%rax,SIZE)
        prefetcht0      B_PR1(BO,BI  ,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
        prefetcht0      B_PR1(BO,BI  ,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB

        prefetcht0      A_PR1(AO,%rax,SIZE)
        prefetcht0      B_PR1(BO,BI  ,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
        prefetcht0      B_PR1(BO,BI  ,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
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
	decq	I			# i --
	jg	.L2_4_11
	ALIGN_4	


/**************************************************************************
* Rest of M 
***************************************************************************/


/******************************************************************************************************************/
.L2_2_10:
	testq	$ 2, M		
	jz	.L2_2_40		// to next 2 lines of N

.L2_2_11:

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
	je	.L2_2_16
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values

	salq	$ 2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_2_12:

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	je	.L2_2_16

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	je	.L2_2_16

	jmp	.L2_2_12
	ALIGN_4

.L2_2_16:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$ 7, %rax		# if (k & 1)
	je .L2_2_19

	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values

	salq	$ 2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_2_17:

	KERNEL2x2_SUB

	jl	.L2_2_17
	ALIGN_4


.L2_2_19:

	SAVE2x2

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
	ALIGN_4	


/**************************************************************************
* Rest of M 
***************************************************************************/
.L2_2_40:
	testq	$ 1, M		
	jz	.L2_2_60		// to next 2 lines of N

	ALIGN_4

.L2_2_41:

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
	je	.L2_2_46
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values

	salq	$ 1, %rax			// rax = rax * 2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_2_42:

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	je	.L2_2_46

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	je	.L2_2_46

	jmp	.L2_2_42
	ALIGN_4

.L2_2_46:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$ 7, %rax		# if (k & 1)
	je .L2_2_49

	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values

	salq	$ 1, %rax			// rax = rax * 2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_2_47:

	KERNEL1x2_SUB

	jl	.L2_2_47
	ALIGN_4


.L2_2_49:

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
	jg	.L2_2_41
	ALIGN_4	



	
.L2_2_60:
#if defined(TRMMKERNEL) && !defined(LEFT)
        addq    $ 2, KK
#endif

	decq	J			// j --
	jg	.L2_00_01			// next 2 lines of N



.L1_2_0:

/************************************************************************************************
* Loop for Nmod6 % 2 > 0
*************************************************************************************************/

	movq	Nmod6, J		
	andq	$ 1, J			// j % 2
	je	.L999
	ALIGN_4

.L1_00_01:
	// copy to sub buffer
	movq	B, BO1
	leaq    BUFFER1, BO		// first buffer to BO
	movq	K, %rax
	ALIGN_4

.L1_00_02b:

	vmovups		(BO1), %xmm0
	vmovups	%xmm0,       (BO)
	addq	$ 2*SIZE,BO1
	addq	$ 2*SIZE,BO
	decq	%rax
	jnz	.L1_00_02b

.L1_00_02c:

	movq	BO1, B			// next offset of B

.L1_00_10:
	movq	C, CO1
	leaq	(C, LDC, 1), C		// c += 1 * ldc

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
        movq    %rax, KK
#endif
	
	movq	A, AO		 	// aoffset = a
	addq	$ 8 * SIZE, AO

	movq	M,  I
	sarq	$ 2, I			// i = (m >> 2)
	je	.L1_2_10

	ALIGN_4

/*******************************************************************************************************/


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

	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	je	.L1_4_16

	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	KERNEL4x1_SUB
	KERNEL4x1_SUB
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
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values

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
	decq	I			# i --
	jg	.L1_4_11
	ALIGN_4	




/*******************************************************************************************************/
.L1_2_10:
	testq	$ 2, M		
	jz	.L1_2_40


.L1_2_11:

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
	je	.L1_2_16
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values

	salq	$ 2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_2_12:

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	je	.L1_2_16

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	je	.L1_2_16

	jmp	.L1_2_12
	ALIGN_4

.L1_2_16:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$ 7, %rax		# if (k & 1)
	je .L1_2_19

	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values

	salq	$ 2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_2_17:

	KERNEL2x1_SUB

	jl	.L1_2_17
	ALIGN_4


.L1_2_19:

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


/**************************************************************************
* Rest of M 
***************************************************************************/
.L1_2_40:
	testq	$ 1, M		
	jz	.L999

	ALIGN_4

.L1_2_41:

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
	je	.L1_2_46
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values

	salq	$ 1, %rax			// rax = rax * 2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_2_42:

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	je	.L1_2_46

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	je	.L1_2_46

	jmp	.L1_2_42
	ALIGN_4

.L1_2_46:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$ 7, %rax		# if (k & 1)
	je .L1_2_49

	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values

	salq	$ 1, %rax			// rax = rax * 2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_2_47:

	KERNEL1x1_SUB

	jl	.L1_2_47
	ALIGN_4


.L1_2_49:

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
	decq	I			# i --
	jg	.L1_2_41
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


#else
/************************************************************************************************
 TRMM Kernel
************************************************************************************************/

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

	vmovsd	 %xmm0, ALPHA_R
	vmovsd	 %xmm1, ALPHA_I

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

.L2_00_0:

	movq	Ndiv6,  J
	cmpq	$ 0, J
	je	.L1_2_0
	ALIGN_4



.L2_00_01:
	// copy to sub buffer
	movq	B, BO1
	leaq    BUFFER1, BO		// first buffer to BO
	movq	K, %rax
	ALIGN_4

.L2_00_02b:

	vmovups		(BO1), %xmm0
	vmovups	2 * SIZE(BO1), %xmm1
	vmovups	%xmm0,       (BO)
	vmovups	%xmm1, 2 * SIZE(BO)
	addq	$ 4*SIZE,BO1
	addq	$ 4*SIZE,BO
	decq	%rax
	jnz	.L2_00_02b

.L2_00_02c:

	movq	BO1, B			// next offset of B


.L2_00_10:
	movq	C, CO1
	leaq	(C, LDC, 2), C		// c += 2 * ldc

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
        movq    %rax, KK
#endif
	
	movq	A, AO		 	// aoffset = a
	addq	$ 8 * SIZE, AO

	movq	M,  I
	sarq	$ 2, I			// i = (m >> 2)
	je	.L2_2_10

	ALIGN_4

/******************************************************************************************************************/

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

        prefetcht0      A_PR1(AO,%rax,SIZE)
        prefetcht0      B_PR1(BO,BI  ,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
        prefetcht0      B_PR1(BO,BI  ,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB

        prefetcht0      A_PR1(AO,%rax,SIZE)
        prefetcht0      B_PR1(BO,BI  ,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
        prefetcht0      B_PR1(BO,BI  ,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB

	je	.L2_4_16

        prefetcht0      A_PR1(AO,%rax,SIZE)
        prefetcht0      B_PR1(BO,BI  ,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
        prefetcht0      B_PR1(BO,BI  ,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB

        prefetcht0      A_PR1(AO,%rax,SIZE)
        prefetcht0      B_PR1(BO,BI  ,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
        prefetcht0      B_PR1(BO,BI  ,SIZE)
	KERNEL4x2_SUB
        prefetcht0      A_PR1(AO,%rax,SIZE)
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
	decq	I			# i --
	jg	.L2_4_11
	ALIGN_4	


/**************************************************************************
* Rest of M 
***************************************************************************/


/******************************************************************************************************************/
.L2_2_10:
	testq	$ 2, M		
	jz	.L2_2_40		// to next 2 lines of N

.L2_2_11:

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
	je	.L2_2_16
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values

	salq	$ 2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_2_12:

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	je	.L2_2_16

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	je	.L2_2_16

	jmp	.L2_2_12
	ALIGN_4

.L2_2_16:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$ 7, %rax		# if (k & 1)
	je .L2_2_19

	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values

	salq	$ 2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_2_17:

	KERNEL2x2_SUB

	jl	.L2_2_17
	ALIGN_4


.L2_2_19:

	SAVE2x2

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
	ALIGN_4	


/**************************************************************************
* Rest of M 
***************************************************************************/
.L2_2_40:
	testq	$ 1, M		
	jz	.L2_2_60		// to next 2 lines of N

	ALIGN_4

.L2_2_41:

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
	je	.L2_2_46
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values

	salq	$ 1, %rax			// rax = rax * 2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_2_42:

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	je	.L2_2_46

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	je	.L2_2_46

	jmp	.L2_2_42
	ALIGN_4

.L2_2_46:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$ 7, %rax		# if (k & 1)
	je .L2_2_49

	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values

	salq	$ 1, %rax			// rax = rax * 2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_2_47:

	KERNEL1x2_SUB

	jl	.L2_2_47
	ALIGN_4


.L2_2_49:

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
	jg	.L2_2_41
	ALIGN_4	



	
.L2_2_60:
#if defined(TRMMKERNEL) && !defined(LEFT)
        addq    $ 2, KK
#endif

	decq	J			// j --
	jg	.L2_00_01			// next 2 lines of N



.L1_2_0:

/************************************************************************************************
* Loop for Nmod6 % 2 > 0
*************************************************************************************************/

	movq	Nmod6, J		
	andq	$ 1, J			// j % 2
	je	.L999
	ALIGN_4

.L1_00_01:
	// copy to sub buffer
	movq	B, BO1
	leaq    BUFFER1, BO		// first buffer to BO
	movq	K, %rax
	ALIGN_4

.L1_00_02b:

	vmovups		(BO1), %xmm0
	vmovups	%xmm0,       (BO)
	addq	$ 2*SIZE,BO1
	addq	$ 2*SIZE,BO
	decq	%rax
	jnz	.L1_00_02b

.L1_00_02c:

	movq	BO1, B			// next offset of B

.L1_00_10:
	movq	C, CO1
	leaq	(C, LDC, 1), C		// c += 1 * ldc

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
        movq    %rax, KK
#endif
	
	movq	A, AO		 	// aoffset = a
	addq	$ 8 * SIZE, AO

	movq	M,  I
	sarq	$ 2, I			// i = (m >> 2)
	je	.L1_2_10

	ALIGN_4

/*******************************************************************************************************/


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

	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	je	.L1_4_16

	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	KERNEL4x1_SUB
	KERNEL4x1_SUB
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
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values

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
	decq	I			# i --
	jg	.L1_4_11
	ALIGN_4	




/*******************************************************************************************************/
.L1_2_10:
	testq	$ 2, M		
	jz	.L1_2_40


.L1_2_11:

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
	je	.L1_2_16
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values

	salq	$ 2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_2_12:

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	je	.L1_2_16

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	prefetcht0	A_PR1(AO,%rax,SIZE)
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	je	.L1_2_16

	jmp	.L1_2_12
	ALIGN_4

.L1_2_16:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$ 7, %rax		# if (k & 1)
	je .L1_2_19

	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values

	salq	$ 2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_2_17:

	KERNEL2x1_SUB

	jl	.L1_2_17
	ALIGN_4


.L1_2_19:

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


/**************************************************************************
* Rest of M 
***************************************************************************/
.L1_2_40:
	testq	$ 1, M		
	jz	.L999

	ALIGN_4

.L1_2_41:

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
	je	.L1_2_46
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values

	salq	$ 1, %rax			// rax = rax * 2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_2_42:

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	je	.L1_2_46

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	prefetcht0	A_PR1(AO,%rax,SIZE)
	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	je	.L1_2_46

	jmp	.L1_2_42
	ALIGN_4

.L1_2_46:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$ 7, %rax		# if (k & 1)
	je .L1_2_49

	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values

	salq	$ 1, %rax			// rax = rax * 2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_2_47:

	KERNEL1x1_SUB

	jl	.L1_2_47
	ALIGN_4


.L1_2_49:

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
	decq	I			# i --
	jg	.L1_2_41
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

#endif


