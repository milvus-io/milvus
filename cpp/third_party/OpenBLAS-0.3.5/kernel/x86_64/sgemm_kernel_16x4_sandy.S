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
#define	CO2	%rdx

#ifndef WINDOWS_ABI

#define STACKSIZE 96

#else

#define STACKSIZE 256

#define OLD_A		40 + STACKSIZE(%rsp)
#define OLD_B		48 + STACKSIZE(%rsp)
#define OLD_C		56 + STACKSIZE(%rsp)
#define OLD_LDC		64 + STACKSIZE(%rsp)
#define OLD_OFFSET	72 + STACKSIZE(%rsp)

#endif

#define L_BUFFER_SIZE 8192

#define Ndiv6	 24(%rsp)
#define Nmod6	 32(%rsp)
#define N	 40(%rsp)
#define ALPHA	 48(%rsp)
#define OFFSET	 56(%rsp)
#define KK	 64(%rsp)
#define KKK	 72(%rsp)
#define BUFFER1	           128(%rsp)

#if defined(OS_WINDOWS)
#if   L_BUFFER_SIZE > 16384
#define STACK_TOUCH \
        movl    $0,  4096 * 4(%rsp);\
        movl    $0,  4096 * 3(%rsp);\
        movl    $0,  4096 * 2(%rsp);\
        movl    $0,  4096 * 1(%rsp);
#elif L_BUFFER_SIZE > 12288
#define STACK_TOUCH \
        movl    $0,  4096 * 3(%rsp);\
        movl    $0,  4096 * 2(%rsp);\
        movl    $0,  4096 * 1(%rsp);
#elif L_BUFFER_SIZE > 8192
#define STACK_TOUCH \
        movl    $0,  4096 * 2(%rsp);\
        movl    $0,  4096 * 1(%rsp);
#elif L_BUFFER_SIZE > 4096
#define STACK_TOUCH \
        movl    $0,  4096 * 1(%rsp);
#else
#define STACK_TOUCH
#endif
#else
#define STACK_TOUCH
#endif

#define	A_PR1	512
#define	B_PR1	512

/*******************************************************************************************
* 4 lines of N
*******************************************************************************************/

.macro KERNEL16x4_SUB
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %ymm0
	vmovups 	 -8 * SIZE(AO, %rax, SIZE), %ymm1
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %ymm2
	vbroadcastss	 -3 * SIZE(BO, BI, SIZE), %ymm3
	vmulps		%ymm2 , %ymm0 , %ymm12
	vmulps		%ymm2 , %ymm1 , %ymm13
	vmulps		%ymm3 , %ymm0 , %ymm14
	vmulps		%ymm3 , %ymm1 , %ymm15
	vaddps 	        %ymm12, %ymm4 , %ymm4
	vaddps 	        %ymm13, %ymm5 , %ymm5
	vaddps 	        %ymm14, %ymm6 , %ymm6
	vaddps 	        %ymm15, %ymm7 , %ymm7
	vbroadcastss	 -2 * SIZE(BO, BI, SIZE), %ymm2
	vbroadcastss	 -1 * SIZE(BO, BI, SIZE), %ymm3
	vmulps		%ymm2 , %ymm0 , %ymm12
	vmulps		%ymm2 , %ymm1 , %ymm13
	vmulps		%ymm3 , %ymm0 , %ymm14
	vmulps		%ymm3 , %ymm1 , %ymm15
	vaddps 	        %ymm12, %ymm8 , %ymm8
	vaddps 	        %ymm13, %ymm9 , %ymm9
	vaddps 	        %ymm14, %ymm10, %ymm10
	vaddps 	        %ymm15, %ymm11, %ymm11
	addq	$ 4 , BI	
	addq	$ 16, %rax 
.endm

.macro SAVE16x4

	vbroadcastss	ALPHA, %ymm0

	vmulps	%ymm0 , %ymm4 , %ymm4
	vmulps	%ymm0 , %ymm5 , %ymm5
	vmulps	%ymm0 , %ymm6 , %ymm6
	vmulps	%ymm0 , %ymm7 , %ymm7
	vmulps	%ymm0 , %ymm8 , %ymm8
	vmulps	%ymm0 , %ymm9 , %ymm9
	vmulps	%ymm0 , %ymm10, %ymm10
	vmulps	%ymm0 , %ymm11, %ymm11


#if !defined(TRMMKERNEL)

	vaddps 	        (CO1), %ymm4,%ymm4
	vaddps  8 * SIZE(CO1), %ymm5,%ymm5

	vaddps 	        (CO1, LDC), %ymm6,%ymm6
	vaddps  8 * SIZE(CO1, LDC), %ymm7,%ymm7

	vaddps 	        (CO2), %ymm8,%ymm8
	vaddps  8 * SIZE(CO2), %ymm9,%ymm9

	vaddps 	        (CO2, LDC), %ymm10,%ymm10
	vaddps  8 * SIZE(CO2, LDC), %ymm11,%ymm11

#endif

	vmovups	%ymm4 ,  	(CO1)
	vmovups	%ymm5 , 8 * SIZE(CO1)

	vmovups	%ymm6 ,  	(CO1, LDC)
	vmovups	%ymm7 , 8 * SIZE(CO1, LDC)

	vmovups	%ymm8 ,  	(CO2)
	vmovups	%ymm9 , 8 * SIZE(CO2)

	vmovups	%ymm10,  	(CO2, LDC)
	vmovups	%ymm11, 8 * SIZE(CO2, LDC)

	prefetcht0	64(CO1)
	prefetcht0	64(CO1, LDC)
	prefetcht0	64(CO2)
	prefetcht0	64(CO2, LDC)

.endm



/*******************************************************************************************/

.macro KERNEL8x4_SUB
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %ymm0
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %ymm2
	vbroadcastss	 -3 * SIZE(BO, BI, SIZE), %ymm3
	vmulps		%ymm2 , %ymm0 , %ymm12
	vmulps		%ymm3 , %ymm0 , %ymm14
	vaddps 	        %ymm12, %ymm4 , %ymm4
	vaddps 	        %ymm14, %ymm6 , %ymm6
	vbroadcastss	 -2 * SIZE(BO, BI, SIZE), %ymm2
	vbroadcastss	 -1 * SIZE(BO, BI, SIZE), %ymm3
	vmulps		%ymm2 , %ymm0 , %ymm12
	vmulps		%ymm3 , %ymm0 , %ymm14
	vaddps 	        %ymm12, %ymm8 , %ymm8
	vaddps 	        %ymm14, %ymm10, %ymm10
	addq	$ 4 , BI	
	addq	$ 8 , %rax 
.endm

.macro SAVE8x4

	vbroadcastss	ALPHA, %ymm0

	vmulps	%ymm0 , %ymm4 , %ymm4
	vmulps	%ymm0 , %ymm6 , %ymm6
	vmulps	%ymm0 , %ymm8 , %ymm8
	vmulps	%ymm0 , %ymm10, %ymm10


#if !defined(TRMMKERNEL)

	vaddps 	        (CO1), %ymm4,%ymm4
	vaddps 	        (CO1, LDC), %ymm6,%ymm6
	vaddps 	        (CO2), %ymm8,%ymm8
	vaddps 	        (CO2, LDC), %ymm10,%ymm10

#endif

	vmovups	%ymm4 ,  	(CO1)
	vmovups	%ymm6 ,  	(CO1, LDC)
	vmovups	%ymm8 ,  	(CO2)
	vmovups	%ymm10,  	(CO2, LDC)

.endm



/*******************************************************************************************/

.macro KERNEL4x4_SUB
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %xmm0
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %xmm2
	vbroadcastss	 -3 * SIZE(BO, BI, SIZE), %xmm3
	vmulps		%xmm2 , %xmm0 , %xmm12
	vmulps		%xmm3 , %xmm0 , %xmm14
	vaddps 	        %xmm12, %xmm4 , %xmm4
	vaddps 	        %xmm14, %xmm6 , %xmm6
	vbroadcastss	 -2 * SIZE(BO, BI, SIZE), %xmm2
	vbroadcastss	 -1 * SIZE(BO, BI, SIZE), %xmm3
	vmulps		%xmm2 , %xmm0 , %xmm12
	vmulps		%xmm3 , %xmm0 , %xmm14
	vaddps 	        %xmm12, %xmm8 , %xmm8
	vaddps 	        %xmm14, %xmm10, %xmm10
	addq	$ 4 , BI	
	addq	$ 4 , %rax 
.endm

.macro SAVE4x4

	vbroadcastss	ALPHA, %xmm0

	vmulps	%xmm0 , %xmm4 , %xmm4
	vmulps	%xmm0 , %xmm6 , %xmm6
	vmulps	%xmm0 , %xmm8 , %xmm8
	vmulps	%xmm0 , %xmm10, %xmm10


#if !defined(TRMMKERNEL)

	vaddps 	        (CO1), %xmm4,%xmm4
	vaddps 	        (CO1, LDC), %xmm6,%xmm6
	vaddps 	        (CO2), %xmm8,%xmm8
	vaddps 	        (CO2, LDC), %xmm10,%xmm10

#endif

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm6 ,  	(CO1, LDC)
	vmovups	%xmm8 ,  	(CO2)
	vmovups	%xmm10,  	(CO2, LDC)

.endm


/*******************************************************************************************/

.macro KERNEL2x4_SUB
	vmovss 	-16 * SIZE(AO, %rax, SIZE), %xmm0
	vmovss 	-15 * SIZE(AO, %rax, SIZE), %xmm1
	vmovss	 -4 * SIZE(BO, BI, SIZE), %xmm2
	vmovss	 -3 * SIZE(BO, BI, SIZE), %xmm3
	vmulss		%xmm2 , %xmm0 , %xmm12
	vmulss		%xmm2 , %xmm1 , %xmm13
	vmulss		%xmm3 , %xmm0 , %xmm14
	vmulss		%xmm3 , %xmm1 , %xmm15
	vaddss 	        %xmm12, %xmm4 , %xmm4
	vaddss 	        %xmm13, %xmm5 , %xmm5
	vaddss 	        %xmm14, %xmm6 , %xmm6
	vaddss 	        %xmm15, %xmm7 , %xmm7
	vmovss	 -2 * SIZE(BO, BI, SIZE), %xmm2
	vmovss	 -1 * SIZE(BO, BI, SIZE), %xmm3
	vmulss		%xmm2 , %xmm0 , %xmm12
	vmulss		%xmm2 , %xmm1 , %xmm13
	vmulss		%xmm3 , %xmm0 , %xmm14
	vmulss		%xmm3 , %xmm1 , %xmm15
	vaddss 	        %xmm12, %xmm8 , %xmm8
	vaddss 	        %xmm13, %xmm9 , %xmm9
	vaddss 	        %xmm14, %xmm10, %xmm10
	vaddss 	        %xmm15, %xmm11, %xmm11
	addq	$ 4 , BI	
	addq	$ 2, %rax 
.endm

.macro SAVE2x4

	vmovss	ALPHA, %xmm0

	vmulss	%xmm0 , %xmm4 , %xmm4
	vmulss	%xmm0 , %xmm5 , %xmm5
	vmulss	%xmm0 , %xmm6 , %xmm6
	vmulss	%xmm0 , %xmm7 , %xmm7
	vmulss	%xmm0 , %xmm8 , %xmm8
	vmulss	%xmm0 , %xmm9 , %xmm9
	vmulss	%xmm0 , %xmm10, %xmm10
	vmulss	%xmm0 , %xmm11, %xmm11


#if !defined(TRMMKERNEL)

	vaddss 	        (CO1), %xmm4,%xmm4
	vaddss  1 * SIZE(CO1), %xmm5,%xmm5

	vaddss 	        (CO1, LDC), %xmm6,%xmm6
	vaddss  1 * SIZE(CO1, LDC), %xmm7,%xmm7

	vaddss 	        (CO2), %xmm8,%xmm8
	vaddss  1 * SIZE(CO2), %xmm9,%xmm9

	vaddss 	        (CO2, LDC), %xmm10,%xmm10
	vaddss  1 * SIZE(CO2, LDC), %xmm11,%xmm11

#endif

	vmovss	%xmm4 ,  	(CO1)
	vmovss	%xmm5 , 1 * SIZE(CO1)

	vmovss	%xmm6 ,  	(CO1, LDC)
	vmovss	%xmm7 , 1 * SIZE(CO1, LDC)

	vmovss	%xmm8 ,  	(CO2)
	vmovss	%xmm9 , 1 * SIZE(CO2)

	vmovss	%xmm10,  	(CO2, LDC)
	vmovss	%xmm11, 1 * SIZE(CO2, LDC)

.endm


/*******************************************************************************************/

.macro KERNEL1x4_SUB
	vmovss 	-16 * SIZE(AO, %rax, SIZE), %xmm0
	vmovss	 -4 * SIZE(BO, BI, SIZE), %xmm2
	vmovss	 -3 * SIZE(BO, BI, SIZE), %xmm3
	vmulss		%xmm2 , %xmm0 , %xmm12
	vmulss		%xmm3 , %xmm0 , %xmm14
	vaddss 	        %xmm12, %xmm4 , %xmm4
	vaddss 	        %xmm14, %xmm6 , %xmm6
	vmovss	 -2 * SIZE(BO, BI, SIZE), %xmm2
	vmovss	 -1 * SIZE(BO, BI, SIZE), %xmm3
	vmulss		%xmm2 , %xmm0 , %xmm12
	vmulss		%xmm3 , %xmm0 , %xmm14
	vaddss 	        %xmm12, %xmm8 , %xmm8
	vaddss 	        %xmm14, %xmm10, %xmm10
	addq	$ 4 , BI	
	addq	$ 1, %rax 
.endm

.macro SAVE1x4

	vmovss	ALPHA, %xmm0

	vmulss	%xmm0 , %xmm4 , %xmm4
	vmulss	%xmm0 , %xmm6 , %xmm6
	vmulss	%xmm0 , %xmm8 , %xmm8
	vmulss	%xmm0 , %xmm10, %xmm10


#if !defined(TRMMKERNEL)

	vaddss 	        (CO1), %xmm4,%xmm4
	vaddss 	        (CO1, LDC), %xmm6,%xmm6
	vaddss 	        (CO2), %xmm8,%xmm8
	vaddss 	        (CO2, LDC), %xmm10,%xmm10

#endif

	vmovss	%xmm4 ,  	(CO1)
	vmovss	%xmm6 ,  	(CO1, LDC)
	vmovss	%xmm8 ,  	(CO2)
	vmovss	%xmm10,  	(CO2, LDC)

.endm


/*******************************************************************************************/

/*******************************************************************************************
* 2 lines of N
*******************************************************************************************/

.macro KERNEL16x2_SUB
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %ymm0
	vmovups 	 -8 * SIZE(AO, %rax, SIZE), %ymm1
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %ymm2
	vbroadcastss	 -3 * SIZE(BO, BI, SIZE), %ymm3
	vmulps		%ymm2 , %ymm0 , %ymm12
	vmulps		%ymm2 , %ymm1 , %ymm13
	vmulps		%ymm3 , %ymm0 , %ymm14
	vmulps		%ymm3 , %ymm1 , %ymm15
	vaddps 	        %ymm12, %ymm4 , %ymm4
	vaddps 	        %ymm13, %ymm5 , %ymm5
	vaddps 	        %ymm14, %ymm6 , %ymm6
	vaddps 	        %ymm15, %ymm7 , %ymm7
	addq	$ 2 , BI	
	addq	$ 16, %rax 
.endm

.macro SAVE16x2

	vbroadcastss	ALPHA, %ymm0

	vmulps	%ymm0 , %ymm4 , %ymm4
	vmulps	%ymm0 , %ymm5 , %ymm5
	vmulps	%ymm0 , %ymm6 , %ymm6
	vmulps	%ymm0 , %ymm7 , %ymm7


#if !defined(TRMMKERNEL)

	vaddps 	        (CO1), %ymm4,%ymm4
	vaddps  8 * SIZE(CO1), %ymm5,%ymm5

	vaddps 	        (CO1, LDC), %ymm6,%ymm6
	vaddps  8 * SIZE(CO1, LDC), %ymm7,%ymm7

#endif

	vmovups	%ymm4 ,  	(CO1)
	vmovups	%ymm5 , 8 * SIZE(CO1)

	vmovups	%ymm6 ,  	(CO1, LDC)
	vmovups	%ymm7 , 8 * SIZE(CO1, LDC)

.endm



/*******************************************************************************************/

.macro KERNEL8x2_SUB
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %ymm0
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %ymm2
	vbroadcastss	 -3 * SIZE(BO, BI, SIZE), %ymm3
	vmulps		%ymm2 , %ymm0 , %ymm12
	vmulps		%ymm3 , %ymm0 , %ymm14
	vaddps 	        %ymm12, %ymm4 , %ymm4
	vaddps 	        %ymm14, %ymm6 , %ymm6
	addq	$ 2 , BI	
	addq	$ 8 , %rax 
.endm

.macro SAVE8x2

	vbroadcastss	ALPHA, %ymm0

	vmulps	%ymm0 , %ymm4 , %ymm4
	vmulps	%ymm0 , %ymm6 , %ymm6

#if !defined(TRMMKERNEL)

	vaddps 	        (CO1), %ymm4,%ymm4
	vaddps 	        (CO1, LDC), %ymm6,%ymm6

#endif

	vmovups	%ymm4 ,  	(CO1)
	vmovups	%ymm6 ,  	(CO1, LDC)

.endm



/*******************************************************************************************/

.macro KERNEL4x2_SUB
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %xmm0
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %xmm2
	vbroadcastss	 -3 * SIZE(BO, BI, SIZE), %xmm3
	vmulps		%xmm2 , %xmm0 , %xmm12
	vmulps		%xmm3 , %xmm0 , %xmm14
	vaddps 	        %xmm12, %xmm4 , %xmm4
	vaddps 	        %xmm14, %xmm6 , %xmm6
	addq	$ 2 , BI	
	addq	$ 4 , %rax 
.endm

.macro SAVE4x2

	vbroadcastss	ALPHA, %xmm0

	vmulps	%xmm0 , %xmm4 , %xmm4
	vmulps	%xmm0 , %xmm6 , %xmm6

#if !defined(TRMMKERNEL)

	vaddps 	        (CO1), %xmm4,%xmm4
	vaddps 	        (CO1, LDC), %xmm6,%xmm6

#endif

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm6 ,  	(CO1, LDC)

.endm


/*******************************************************************************************/

.macro KERNEL2x2_SUB
	vmovss 	-16 * SIZE(AO, %rax, SIZE), %xmm0
	vmovss 	-15 * SIZE(AO, %rax, SIZE), %xmm1
	vmovss	 -4 * SIZE(BO, BI, SIZE), %xmm2
	vmovss	 -3 * SIZE(BO, BI, SIZE), %xmm3
	vmulss		%xmm2 , %xmm0 , %xmm12
	vmulss		%xmm2 , %xmm1 , %xmm13
	vmulss		%xmm3 , %xmm0 , %xmm14
	vmulss		%xmm3 , %xmm1 , %xmm15
	vaddss 	        %xmm12, %xmm4 , %xmm4
	vaddss 	        %xmm13, %xmm5 , %xmm5
	vaddss 	        %xmm14, %xmm6 , %xmm6
	vaddss 	        %xmm15, %xmm7 , %xmm7
	addq	$ 2 , BI	
	addq	$ 2, %rax 
.endm

.macro SAVE2x2

	vmovss	ALPHA, %xmm0

	vmulss	%xmm0 , %xmm4 , %xmm4
	vmulss	%xmm0 , %xmm5 , %xmm5
	vmulss	%xmm0 , %xmm6 , %xmm6
	vmulss	%xmm0 , %xmm7 , %xmm7


#if !defined(TRMMKERNEL)

	vaddss 	        (CO1), %xmm4,%xmm4
	vaddss  1 * SIZE(CO1), %xmm5,%xmm5

	vaddss 	        (CO1, LDC), %xmm6,%xmm6
	vaddss  1 * SIZE(CO1, LDC), %xmm7,%xmm7

#endif

	vmovss	%xmm4 ,  	(CO1)
	vmovss	%xmm5 , 1 * SIZE(CO1)

	vmovss	%xmm6 ,  	(CO1, LDC)
	vmovss	%xmm7 , 1 * SIZE(CO1, LDC)

.endm


/*******************************************************************************************/

.macro KERNEL1x2_SUB
	vmovss 	-16 * SIZE(AO, %rax, SIZE), %xmm0
	vmovss	 -4 * SIZE(BO, BI, SIZE), %xmm2
	vmovss	 -3 * SIZE(BO, BI, SIZE), %xmm3
	vmulss		%xmm2 , %xmm0 , %xmm12
	vmulss		%xmm3 , %xmm0 , %xmm14
	vaddss 	        %xmm12, %xmm4 , %xmm4
	vaddss 	        %xmm14, %xmm6 , %xmm6
	addq	$ 2 , BI	
	addq	$ 1, %rax 
.endm

.macro SAVE1x2

	vmovss	ALPHA, %xmm0

	vmulss	%xmm0 , %xmm4 , %xmm4
	vmulss	%xmm0 , %xmm6 , %xmm6

#if !defined(TRMMKERNEL)

	vaddss 	        (CO1), %xmm4,%xmm4
	vaddss 	        (CO1, LDC), %xmm6,%xmm6

#endif

	vmovss	%xmm4 ,  	(CO1)
	vmovss	%xmm6 ,  	(CO1, LDC)

.endm


/*******************************************************************************************/

/*******************************************************************************************
* 1 line of N
*******************************************************************************************/

.macro KERNEL16x1_SUB
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %ymm0
	vmovups 	 -8 * SIZE(AO, %rax, SIZE), %ymm1
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %ymm2
	vmulps		%ymm2 , %ymm0 , %ymm12
	vmulps		%ymm2 , %ymm1 , %ymm13
	vaddps 	        %ymm12, %ymm4 , %ymm4
	vaddps 	        %ymm13, %ymm5 , %ymm5
	addq	$ 1 , BI	
	addq	$ 16, %rax 
.endm

.macro SAVE16x1

	vbroadcastss	ALPHA, %ymm0

	vmulps	%ymm0 , %ymm4 , %ymm4
	vmulps	%ymm0 , %ymm5 , %ymm5

#if !defined(TRMMKERNEL)

	vaddps 	        (CO1), %ymm4,%ymm4
	vaddps  8 * SIZE(CO1), %ymm5,%ymm5

#endif

	vmovups	%ymm4 ,  	(CO1)
	vmovups	%ymm5 , 8 * SIZE(CO1)

.endm


/*******************************************************************************************/

.macro KERNEL8x1_SUB
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %ymm0
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %ymm2
	vmulps		%ymm2 , %ymm0 , %ymm12
	vaddps 	        %ymm12, %ymm4 , %ymm4
	addq	$ 1 , BI	
	addq	$ 8 , %rax 
.endm

.macro SAVE8x1

	vbroadcastss	ALPHA, %ymm0

	vmulps	%ymm0 , %ymm4 , %ymm4

#if !defined(TRMMKERNEL)

	vaddps 	        (CO1), %ymm4,%ymm4

#endif

	vmovups	%ymm4 ,  	(CO1)

.endm



/*******************************************************************************************/

.macro KERNEL4x1_SUB
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %xmm0
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %xmm2
	vmulps		%xmm2 , %xmm0 , %xmm12
	vaddps 	        %xmm12, %xmm4 , %xmm4
	addq	$ 1 , BI	
	addq	$ 4 , %rax 
.endm

.macro SAVE4x1

	vbroadcastss	ALPHA, %xmm0

	vmulps	%xmm0 , %xmm4 , %xmm4

#if !defined(TRMMKERNEL)

	vaddps 	        (CO1), %xmm4,%xmm4

#endif

	vmovups	%xmm4 ,  	(CO1)

.endm


/*******************************************************************************************/

.macro KERNEL2x1_SUB
	vmovss 	-16 * SIZE(AO, %rax, SIZE), %xmm0
	vmovss 	-15 * SIZE(AO, %rax, SIZE), %xmm1
	vmovss	 -4 * SIZE(BO, BI, SIZE), %xmm2
	vmulss		%xmm2 , %xmm0 , %xmm12
	vmulss		%xmm2 , %xmm1 , %xmm13
	vaddss 	        %xmm12, %xmm4 , %xmm4
	vaddss 	        %xmm13, %xmm5 , %xmm5
	addq	$ 1 , BI	
	addq	$ 2 , %rax 
.endm

.macro SAVE2x1

	vmovss	ALPHA, %xmm0

	vmulss	%xmm0 , %xmm4 , %xmm4
	vmulss	%xmm0 , %xmm5 , %xmm5

#if !defined(TRMMKERNEL)

	vaddss 	        (CO1), %xmm4,%xmm4
	vaddss  1 * SIZE(CO1), %xmm5,%xmm5

#endif

	vmovss	%xmm4 ,  	(CO1)
	vmovss	%xmm5 , 1 * SIZE(CO1)

.endm


/*******************************************************************************************/

.macro KERNEL1x1_SUB
	vmovss 	-16 * SIZE(AO, %rax, SIZE), %xmm0
	vmovss	 -4 * SIZE(BO, BI, SIZE), %xmm2
	vmulss		%xmm2 , %xmm0 , %xmm12
	vaddss 	        %xmm12, %xmm4 , %xmm4
	addq	$ 1 , BI	
	addq	$ 1 , %rax 
.endm

.macro SAVE1x1

	vmovss	ALPHA, %xmm0

	vmulss	%xmm0 , %xmm4 , %xmm4

#if !defined(TRMMKERNEL)

	vaddss 	        (CO1), %xmm4,%xmm4

#endif

	vmovss	%xmm4 ,  	(CO1)

.endm


/*******************************************************************************************/

/*************************************************************************************
* TRMM Kernel
*************************************************************************************/


	PROLOGUE
	PROFCODE
	
	subq	$STACKSIZE, %rsp
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
	movups	%xmm6,   64(%rsp)
	movups	%xmm7,   80(%rsp)
	movups	%xmm8,   96(%rsp)
	movups	%xmm9,  112(%rsp)
	movups	%xmm10, 128(%rsp)
	movups	%xmm11, 144(%rsp)
	movups	%xmm12, 160(%rsp)
	movups	%xmm13, 176(%rsp)
	movups	%xmm14, 192(%rsp)
	movups	%xmm15, 208(%rsp)

	movq	ARG1,      OLD_M
	movq	ARG2,      OLD_N
	movq	ARG3,      OLD_K
	movq	OLD_A,     A
	movq	OLD_B,     B
	movq	OLD_C,     C
	movq	OLD_LDC,   LDC
#ifdef TRMMKERNEL
	vmovsd	OLD_OFFSET, %xmm12
#endif
	vmovaps	%xmm3, %xmm0

#else
	movq	STACKSIZE +  8(%rsp), LDC
#ifdef TRMMKERNEL
	movsd	STACKSIZE + 16(%rsp), %xmm12
#endif

#endif

	movq    %rsp, SP      # save old stack
        subq    $128 + L_BUFFER_SIZE, %rsp
        andq    $-4096, %rsp    # align stack

        STACK_TOUCH

	cmpq	$0, OLD_M
	je	.L999

	cmpq	$0, OLD_N
	je	.L999

	cmpq	$0, OLD_K
	je	.L999

	movq	OLD_M, M
	movq	OLD_N, N
	movq	OLD_K, K

	vmovss	 %xmm0, ALPHA

	salq	$BASE_SHIFT, LDC

	movq    N, %rax
        xorq    %rdx, %rdx
        movq    $4,  %rdi
        divq    %rdi                    //    N / 4
        movq    %rax, Ndiv6             //    N / 4
        movq    %rdx, Nmod6             //    N % 4

	

#ifdef TRMMKERNEL
	vmovsd	%xmm12, OFFSET
	vmovsd	%xmm12, KK
#ifndef LEFT
	negq	KK
#endif	
#endif

	movq	Ndiv6,  J
	cmpq	$0, J
	je	.L2_0
	ALIGN_4

/*******************************************************************************************/

.L4_01:
	// copy to sub buffer
	movq	B, BO1
	leaq    BUFFER1, BO		// first buffer to BO
	movq	K, %rax
	sarq	$2, %rax		// K / 4
	jz	.L4_01b
	ALIGN_4


.L4_01a:
        prefetcht0 512(BO1)
        prefetchw  512(BO)

	vmovups	       (BO1), %xmm0
	vmovups	 4*SIZE(BO1), %xmm1
	vmovups	 8*SIZE(BO1), %xmm2
	vmovups	12*SIZE(BO1), %xmm3

	vmovups	%xmm0,       (BO)
	vmovups	%xmm1, 4*SIZE(BO)
	vmovups	%xmm2, 8*SIZE(BO)
	vmovups	%xmm3,12*SIZE(BO)

	addq	$ 16*SIZE,BO1
	addq	$ 16*SIZE,BO
	decq	%rax
	jnz	.L4_01a


.L4_01b:

        movq    K, %rax
        andq    $3, %rax                // K % 4
        jz      .L4_02d
        ALIGN_4

.L4_02c:

	vmovups	(BO1), %xmm0
	vmovups	%xmm0, (BO)
	addq	$ 4*SIZE,BO1
	addq	$ 4*SIZE,BO
	decq	%rax
	jnz	.L4_02c

.L4_02d:

	movq	BO1, B			// next offset of B

.L4_10:
	movq	 C, CO1
	leaq	(C, LDC, 2), CO2	
	leaq	(C, LDC, 4), C		// c += 4 * ldc

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
        movq    %rax, KK
#endif
	
	movq	A, AO		 	// aoffset = a
	addq	$ 16 * SIZE, AO

	movq	M,  I
	sarq	$4, I			// i = (m >> 4)
	je	.L4_20

	ALIGN_4

.L4_11:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             	// first buffer to BO
        addq    $4 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
	leaq	(,BI, 4), BI			// BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO
        salq    $4, %rax                        // rax = rax * 16 ; number of values
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
        addq    $16, %rax	// number of values in AO
#else
        addq    $4, %rax	// number of values in BO
#endif
        movq    %rax, KKK
#endif

	andq	$-8, %rax			//  K = K - ( K % 8 )
	je	.L4_16
	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,4) , BI                   	//  BI = BI * 4 ; number of values

	salq	$4, %rax			// rax = rax * 16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L4_12:

	prefetcht0	A_PR1(AO, %rax, SIZE)
	prefetcht0	B_PR1(BO, BI  , SIZE)
	KERNEL16x4_SUB
	prefetcht0	A_PR1(AO, %rax, SIZE)
	KERNEL16x4_SUB
	prefetcht0	A_PR1(AO, %rax, SIZE)
	KERNEL16x4_SUB
	prefetcht0	A_PR1(AO, %rax, SIZE)
	KERNEL16x4_SUB

	prefetcht0	A_PR1(AO, %rax, SIZE)
	prefetcht0	B_PR1(BO, BI  , SIZE)
	KERNEL16x4_SUB
	prefetcht0	A_PR1(AO, %rax, SIZE)
	KERNEL16x4_SUB
	prefetcht0	A_PR1(AO, %rax, SIZE)
	KERNEL16x4_SUB
	prefetcht0	A_PR1(AO, %rax, SIZE)
	KERNEL16x4_SUB

	je	.L4_16

	prefetcht0	A_PR1(AO, %rax, SIZE)
	prefetcht0	B_PR1(BO, BI  , SIZE)
	KERNEL16x4_SUB
	prefetcht0	A_PR1(AO, %rax, SIZE)
	KERNEL16x4_SUB
	prefetcht0	A_PR1(AO, %rax, SIZE)
	KERNEL16x4_SUB
	prefetcht0	A_PR1(AO, %rax, SIZE)
	KERNEL16x4_SUB

	prefetcht0	A_PR1(AO, %rax, SIZE)
	prefetcht0	B_PR1(BO, BI  , SIZE)
	KERNEL16x4_SUB
	prefetcht0	A_PR1(AO, %rax, SIZE)
	KERNEL16x4_SUB
	prefetcht0	A_PR1(AO, %rax, SIZE)
	KERNEL16x4_SUB
	prefetcht0	A_PR1(AO, %rax, SIZE)
	KERNEL16x4_SUB

	je	.L4_16

	jmp	.L4_12
	ALIGN_4

.L4_16:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L4_19

	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,4), BI                   	//  BI = BI * 4 ; number of values

	salq	$4, %rax			// rax = rax * 16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L4_17:

	KERNEL16x4_SUB

	jl	.L4_17
	ALIGN_4


.L4_19:

	SAVE16x4

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
	leaq	(,BI, 4), BI			// BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO
        salq    $4, %rax                        // rax = rax * 16 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $16, KK				
#endif

	addq	$16 * SIZE, CO1		# coffset += 16
	addq	$16 * SIZE, CO2		# coffset += 16
	decq	I			# i --
	jg	.L4_11
	ALIGN_4	

/**************************************************************************
* Rest of M 
***************************************************************************/
.L4_20:
	// Test rest of M

	testq	$15, M
	jz	.L4_60		// to next 3 lines of N

	testq	$8, M		
	jz	.L4_21pre
	ALIGN_4

/**************************************************************************/

.L4_20_1:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
	leaq	(,BI, 4), BI			// BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO
        salq    $3, %rax                        // rax = rax * 8 ; number of values
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
        addq    $8, %rax        // number of values in A
#else
        addq    $4, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$-8, %rax
	je	.L4_20_6
	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,4), BI                   	//  BI = BI * 4 ; number of values

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L4_20_2:

	KERNEL8x4_SUB
	KERNEL8x4_SUB
	KERNEL8x4_SUB
	KERNEL8x4_SUB

	KERNEL8x4_SUB
	KERNEL8x4_SUB
	KERNEL8x4_SUB
	KERNEL8x4_SUB

	je	.L4_20_6

	KERNEL8x4_SUB
	KERNEL8x4_SUB
	KERNEL8x4_SUB
	KERNEL8x4_SUB

	KERNEL8x4_SUB
	KERNEL8x4_SUB
	KERNEL8x4_SUB
	KERNEL8x4_SUB

	je	.L4_20_6

	jmp	.L4_20_2
	ALIGN_4

.L4_20_6:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L4_20_9

	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,4), BI                   	//  BI = BI * 4 ; number of values

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L4_20_7:

	KERNEL8x4_SUB

	jl	.L4_20_7
	ALIGN_4


.L4_20_9:

	SAVE8x4

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
	leaq	(,BI, 4), BI			// BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO         
        salq    $3, %rax                        // rax = rax * 8 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif  


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $8, KK
#endif

	addq	$8 * SIZE, CO1		# coffset += 8
	addq	$8 * SIZE, CO2		# coffset += 8
	ALIGN_4
	


/**************************************************************************/

.L4_21pre:

	testq	$4, M		
	jz	.L4_30
	ALIGN_4

.L4_21:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
	leaq	(,BI, 4), BI			// BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO
        salq    $2, %rax                        // rax = rax * 4 ; number of values
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
        addq    $4, %rax        // number of values in A
#else
        addq    $4, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$-8, %rax
	je	.L4_26
	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,4), BI                   	//  BI = BI * 4 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L4_22:

	KERNEL4x4_SUB
	KERNEL4x4_SUB
	KERNEL4x4_SUB
	KERNEL4x4_SUB

	KERNEL4x4_SUB
	KERNEL4x4_SUB
	KERNEL4x4_SUB
	KERNEL4x4_SUB

	je	.L4_26

	KERNEL4x4_SUB
	KERNEL4x4_SUB
	KERNEL4x4_SUB
	KERNEL4x4_SUB

	KERNEL4x4_SUB
	KERNEL4x4_SUB
	KERNEL4x4_SUB
	KERNEL4x4_SUB

	je	.L4_26

	jmp	.L4_22
	ALIGN_4

.L4_26:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L4_29

	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,4), BI                   	//  BI = BI * 4 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L4_27:

	KERNEL4x4_SUB

	jl	.L4_27
	ALIGN_4


.L4_29:

	SAVE4x4

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
	leaq	(,BI, 4), BI			// BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO         
        salq    $2, %rax                        // rax = rax * 4 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif  


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $4, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	addq	$4 * SIZE, CO2		# coffset += 4
	ALIGN_4
	

.L4_30:
	testq	$2, M		
	jz	.L4_40

	ALIGN_4

.L4_31:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
	leaq	(,BI, 4), BI			// BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO
        salq    $1, %rax                        // rax = rax * 2 ; number of values
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
        addq    $2, %rax        // number of values in AO
#else
        addq    $4, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$-8, %rax
	je	.L4_36
	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,4), BI                   	//  BI = BI * 4 ; number of values

	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L4_32:

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	je	.L4_36

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB
	KERNEL2x4_SUB

	je	.L4_36

	jmp	.L4_32
	ALIGN_4

.L4_36:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L4_39

	movq    %rax, BI                        //  Index for BO
	leaq	(,BI, 4), BI			// BI = BI * 4 ; number of values
	
	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L4_37:

	KERNEL2x4_SUB

	jl	.L4_37
	ALIGN_4


.L4_39:

	SAVE2x4

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
	leaq	(,BI, 4), BI			// BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO         
        salq    $1, %rax                        // rax = rax * 2 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif  


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $2, KK
#endif

	addq	$2 * SIZE, CO1		# coffset += 2
	addq	$2 * SIZE, CO2		# coffset += 2
	ALIGN_4

.L4_40:
	testq	$1, M		
	jz	.L4_60		// to next 4 lines of N

	ALIGN_4

.L4_41:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
	leaq	(,BI, 4), BI			// BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO
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
        addq    $1, %rax        // number of values in AO
#else
        addq    $4, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif

	andq	$-8, %rax
	je	.L4_46
	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,4), BI                   	//  BI = BI * 4 ; number of values

	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L4_42:

	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	je	.L4_46

	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB
	KERNEL1x4_SUB

	je	.L4_46

	jmp	.L4_42
	ALIGN_4

.L4_46:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L4_49

	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,4), BI                   	//  BI = BI * 4 ; number of values

	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L4_47:

	KERNEL1x4_SUB

	jl	.L4_47
	ALIGN_4


.L4_49:

	SAVE1x4

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
	leaq	(,BI, 4), BI			// BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO         
        leaq    (AO, %rax, SIZE), AO
#endif  


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $1, KK
#endif

	addq	$1 * SIZE, CO1		# coffset += 1
	addq	$1 * SIZE, CO2		# coffset += 1
	ALIGN_4
	



	
.L4_60:
#if defined(TRMMKERNEL) && !defined(LEFT)
        addq    $4, KK
#endif

	decq	J			// j --
	jg	.L4_01			// next 4 lines of N



/*******************************************************************************************/
.L2_0:

	movq	Nmod6, J		
	andq	$3, J			// j % 4
	je	.L999

	movq	Nmod6, J		
	andq	$2, J			// j % 4
	je	.L1_0

.L2_01:

	// copy to sub buffer
	movq	B, BO1
	leaq    BUFFER1, BO		// first buffer to BO
	movq	K, %rax
	sarq	$2, %rax		// K / 4
	jz	.L2_01b
	ALIGN_4

.L2_01a:

	vmovsd	      (BO1), %xmm0
	vmovsd	2*SIZE(BO1), %xmm1
	vmovsd	4*SIZE(BO1), %xmm2
	vmovsd	6*SIZE(BO1), %xmm3

	vmovsd	%xmm0,       (BO)
	vmovsd	%xmm1, 2*SIZE(BO)
	vmovsd	%xmm2, 4*SIZE(BO)
	vmovsd	%xmm3, 6*SIZE(BO)

	addq	$8*SIZE,BO1
	addq	$8*SIZE,BO
	decq	%rax
	jnz	.L2_01a


.L2_01b:

        movq    K, %rax
        andq    $3, %rax                // K % 4
        jz      .L2_02d
        ALIGN_4

.L2_02c:

	vmovsd 	(BO1), %xmm0
	vmovsd 	%xmm0, (BO)
	addq	$2*SIZE,BO1
	addq	$2*SIZE,BO
	decq	%rax
	jnz	.L2_02c

.L2_02d:

	movq	BO1, B			// next offset of B

.L2_10:
	movq	C, CO1
	leaq	(C, LDC, 2), C		// c += 2 * ldc

#if defined(TRMMKERNEL) && defined(LEFT)
        movq    OFFSET, %rax
        movq    %rax, KK
#endif
	
	movq	A, AO		 	// aoffset = a
	addq	$16 * SIZE, AO

	movq	M,  I
	sarq	$4, I			// i = (m >> 4)
	je	.L2_20

	ALIGN_4

.L2_11:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
        salq    $4, %rax                        // rax = rax * 16 ; number of values
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
        addq    $16, %rax	// number of values in AO
#else
        addq    $2, %rax	// number of values in BO
#endif
        movq    %rax, KKK
#endif

	andq	$-8, %rax			//  K = K - ( K % 8 )
	je	.L2_16
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$4, %rax			// rax = rax * 16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_12:

	KERNEL16x2_SUB
	KERNEL16x2_SUB
	KERNEL16x2_SUB
	KERNEL16x2_SUB

	KERNEL16x2_SUB
	KERNEL16x2_SUB
	KERNEL16x2_SUB
	KERNEL16x2_SUB

	je	.L2_16

	KERNEL16x2_SUB
	KERNEL16x2_SUB
	KERNEL16x2_SUB
	KERNEL16x2_SUB

	KERNEL16x2_SUB
	KERNEL16x2_SUB
	KERNEL16x2_SUB
	KERNEL16x2_SUB

	je	.L2_16

	jmp	.L2_12
	ALIGN_4

.L2_16:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L2_19

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$4, %rax			// rax = rax * 16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_17:

	KERNEL16x2_SUB

	jl	.L2_17
	ALIGN_4


.L2_19:

	SAVE16x2

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
        salq    $4, %rax                        // rax = rax * 16 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $16, KK				
#endif

	addq	$16 * SIZE, CO1		# coffset += 16
	decq	I			# i --
	jg	.L2_11
	ALIGN_4	

/**************************************************************************
* Rest of M 
***************************************************************************/
.L2_20:
	// Test rest of M

	testq	$15, M
	jz	.L2_60		// to next 2 lines of N

	testq	$8, M		
	jz	.L2_21pre
	ALIGN_4

/**************************************************************************/

.L2_20_1:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
        salq    $3, %rax                        // rax = rax * 8 ; number of values
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
        addq    $8, %rax        // number of values in A
#else
        addq    $2, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$-8, %rax
	je	.L2_20_6
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_20_2:


	KERNEL8x2_SUB
	KERNEL8x2_SUB
	KERNEL8x2_SUB
	KERNEL8x2_SUB

	KERNEL8x2_SUB
	KERNEL8x2_SUB
	KERNEL8x2_SUB
	KERNEL8x2_SUB

	je	.L2_20_6

	KERNEL8x2_SUB
	KERNEL8x2_SUB
	KERNEL8x2_SUB
	KERNEL8x2_SUB

	KERNEL8x2_SUB
	KERNEL8x2_SUB
	KERNEL8x2_SUB
	KERNEL8x2_SUB

	je	.L2_20_6

	jmp	.L2_20_2
	ALIGN_4

.L2_20_6:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L2_20_9

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_20_7:

	KERNEL8x2_SUB

	jl	.L2_20_7
	ALIGN_4


.L2_20_9:

	SAVE8x2

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO         
        salq    $3, %rax                        // rax = rax * 8 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif  


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $8, KK
#endif

	addq	$8 * SIZE, CO1		# coffset += 8
	ALIGN_4
	


/**************************************************************************/

.L2_21pre:

	testq	$4, M		
	jz	.L2_30
	ALIGN_4

.L2_21:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
        salq    $2, %rax                        // rax = rax * 4 ; number of values
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
        addq    $4, %rax        // number of values in A
#else
        addq    $2, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$-8, %rax
	je	.L2_26
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 1 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_22:


	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	je	.L2_26

	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB
	KERNEL4x2_SUB

	je	.L2_26

	jmp	.L2_22
	ALIGN_4

.L2_26:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L2_29

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_27:

	KERNEL4x2_SUB

	jl	.L2_27
	ALIGN_4


.L2_29:

	SAVE4x2

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO         
        salq    $2, %rax                        // rax = rax * 4 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif  


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $4, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	ALIGN_4
	

.L2_30:
	testq	$2, M		
	jz	.L2_40

	ALIGN_4

.L2_31:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
        salq    $1, %rax                        // rax = rax * 2 ; number of values
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
        addq    $2, %rax        // number of values in AO
#else
        addq    $2, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$-8, %rax
	je	.L2_36
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_32:

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	je	.L2_36

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB
	KERNEL2x2_SUB

	je	.L2_36

	jmp	.L2_32
	ALIGN_4

.L2_36:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L2_39

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
	
	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_37:

	KERNEL2x2_SUB

	jl	.L2_37
	ALIGN_4


.L2_39:

	SAVE2x2

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO         
        salq    $1, %rax                        // rax = rax * 2 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif  


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $2, KK
#endif

	addq	$2 * SIZE, CO1		# coffset += 2
	ALIGN_4

.L2_40:
	testq	$1, M		
	jz	.L2_60		// to next 2 lines of N

	ALIGN_4

.L2_41:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
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
        addq    $1, %rax        // number of values in AO
#else
        addq    $2, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif

	andq	$-8, %rax
	je	.L2_46
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_42:

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	je	.L2_46

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB
	KERNEL1x2_SUB

	je	.L2_46

	jmp	.L2_42
	ALIGN_4

.L2_46:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L2_49

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values

	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_47:

	KERNEL1x2_SUB

	jl	.L2_47
	ALIGN_4


.L2_49:

	SAVE1x2

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,1), BI                   //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO         
        leaq    (AO, %rax, SIZE), AO
#endif  


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $1, KK
#endif

	addq	$1 * SIZE, CO1		# coffset += 1
	ALIGN_4
	



	
.L2_60:
#if defined(TRMMKERNEL) && !defined(LEFT)
        addq    $2, KK
#endif




.L1_0:

/************************************************************************************************
* Loop for Nmod6 % 2 > 0
*************************************************************************************************/

	movq	Nmod6, J		
	andq	$1, J			// j % 2
	je	.L999
	ALIGN_4

.L1_01:
	// copy to sub buffer
	movq	B, BO1
	leaq    BUFFER1, BO		// first buffer to BO
	movq	K, %rax
	ALIGN_4

.L1_02b:

	vmovss	(BO1), %xmm0
	vmovss	%xmm0,       (BO)
	addq	$1*SIZE,BO1
	addq	$1*SIZE,BO
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
	addq	$16 * SIZE, AO

	movq	M,  I
	sarq	$4, I			// i = (m >> 4)
	je	.L1_20

	ALIGN_4

.L1_11:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, SIZE), BO
        salq    $4, %rax                        // rax = rax * 16 ; number of values
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
        addq    $16, %rax	// number of values in AO
#else
        addq    $1, %rax	// number of values in BO
#endif
        movq    %rax, KKK
#endif

	andq	$-8, %rax			//  K = K - ( K % 8 )
	je	.L1_16
	movq    %rax, BI                        //  Index for BO

	salq	$4, %rax			// rax = rax * 16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_12:

	KERNEL16x1_SUB
	KERNEL16x1_SUB
	KERNEL16x1_SUB
	KERNEL16x1_SUB

	KERNEL16x1_SUB
	KERNEL16x1_SUB
	KERNEL16x1_SUB
	KERNEL16x1_SUB

	je	.L1_16

	KERNEL16x1_SUB
	KERNEL16x1_SUB
	KERNEL16x1_SUB
	KERNEL16x1_SUB

	KERNEL16x1_SUB
	KERNEL16x1_SUB
	KERNEL16x1_SUB
	KERNEL16x1_SUB

	je	.L1_16

	jmp	.L1_12
	ALIGN_4

.L1_16:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L1_19

	movq    %rax, BI                        //  Index for BO

	salq	$4, %rax			// rax = rax * 16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_17:

	KERNEL16x1_SUB

	jl	.L1_17
	ALIGN_4


.L1_19:

	SAVE16x1

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, SIZE), BO
        salq    $4, %rax                        // rax = rax * 16 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $16, KK				
#endif

	addq	$16 * SIZE, CO1		# coffset += 16
	decq	I			# i --
	jg	.L1_11
	ALIGN_4	

/**************************************************************************
* Rest of M 
***************************************************************************/
.L1_20:
	// Test rest of M

	testq	$15, M
	jz	.L999

	testq	$8, M		
	jz	.L1_21pre
	ALIGN_4

/**************************************************************************/

.L1_20_1:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, SIZE), BO
        salq    $3, %rax                        // rax = rax * 8 ; number of values
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
        addq    $8, %rax        // number of values in A
#else
        addq    $1, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$-8, %rax
	je	.L1_20_6
	movq    %rax, BI                        //  Index for BO

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_20_2:

	KERNEL8x1_SUB
	KERNEL8x1_SUB
	KERNEL8x1_SUB
	KERNEL8x1_SUB

	KERNEL8x1_SUB
	KERNEL8x1_SUB
	KERNEL8x1_SUB
	KERNEL8x1_SUB

	je	.L1_20_6

	KERNEL8x1_SUB
	KERNEL8x1_SUB
	KERNEL8x1_SUB
	KERNEL8x1_SUB

	KERNEL8x1_SUB
	KERNEL8x1_SUB
	KERNEL8x1_SUB
	KERNEL8x1_SUB

	je	.L1_20_6

	jmp	.L1_20_2
	ALIGN_4

.L1_20_6:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L1_20_9

	movq    %rax, BI                        //  Index for BO

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_20_7:

	KERNEL8x1_SUB

	jl	.L1_20_7
	ALIGN_4


.L1_20_9:

	SAVE8x1

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, SIZE), BO         
        salq    $3, %rax                        // rax = rax * 8 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif  


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $8, KK
#endif

	addq	$8 * SIZE, CO1		# coffset += 8
	ALIGN_4
	


/**************************************************************************/

.L1_21pre:

	testq	$4, M		
	jz	.L1_30
	ALIGN_4

.L1_21:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, SIZE), BO
        salq    $2, %rax                        // rax = rax * 4 ; number of values
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
        addq    $4, %rax        // number of values in A
#else
        addq    $1, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$-8, %rax
	je	.L1_26
	movq    %rax, BI                        //  Index for BO

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_22:

	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	je	.L1_26

	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB
	KERNEL4x1_SUB

	je	.L1_26

	jmp	.L1_22
	ALIGN_4

.L1_26:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L1_29

	movq    %rax, BI                        //  Index for BO

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_27:

	KERNEL4x1_SUB

	jl	.L1_27
	ALIGN_4


.L1_29:

	SAVE4x1

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, SIZE), BO         
        salq    $2, %rax                        // rax = rax * 4 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif  


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $4, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	ALIGN_4
	

.L1_30:
	testq	$2, M		
	jz	.L1_40

	ALIGN_4

.L1_31:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, SIZE), BO
        salq    $1, %rax                        // rax = rax * 2 ; number of values
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
        addq    $2, %rax        // number of values in AO
#else
        addq    $1, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif


	andq	$-8, %rax
	je	.L1_36
	movq    %rax, BI                        //  Index for BO

	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_32:

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	je	.L1_36

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB
	KERNEL2x1_SUB

	je	.L1_36

	jmp	.L1_32
	ALIGN_4

.L1_36:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L1_39

	movq    %rax, BI                        //  Index for BO
	
	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_37:

	KERNEL2x1_SUB

	jl	.L1_37
	ALIGN_4


.L1_39:

	SAVE2x1

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, SIZE), BO         
        salq    $1, %rax                        // rax = rax * 2 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif  


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $2, KK
#endif

	addq	$2 * SIZE, CO1		# coffset += 2
	ALIGN_4

.L1_40:
	testq	$1, M		
	jz	.L999

	ALIGN_4

.L1_41:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, SIZE), BO
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
        addq    $1, %rax        // number of values in AO
#else
        addq    $1, %rax        // number of values in BO
#endif
        movq    %rax, KKK
#endif

	andq	$-8, %rax
	je	.L1_46
	movq    %rax, BI                        //  Index for BO

	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_42:

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	je	.L1_46

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB
	KERNEL1x1_SUB

	je	.L1_46

	jmp	.L1_42
	ALIGN_4

.L1_46:
#ifndef TRMMKERNEL
        movq    K, %rax
#else
        movq    KKK, %rax
#endif

	andq	$7, %rax		# if (k & 1)
	je .L1_49

	movq    %rax, BI                        //  Index for BO

	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_47:

	KERNEL1x1_SUB

	jl	.L1_47
	ALIGN_4


.L1_49:

	SAVE1x1

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax 
        subq    KKK, %rax
        movq    %rax, BI                        //  Index for BO
        leaq    (BO, BI, SIZE), BO         
        leaq    (AO, %rax, SIZE), AO
#endif  


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $1, KK
#endif

	addq	$1 * SIZE, CO1		# coffset += 1
	ALIGN_4
	

.L999:
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
	movups	 64(%rsp), %xmm6
	movups	 80(%rsp), %xmm7
	movups	 96(%rsp), %xmm8
	movups	112(%rsp), %xmm9
	movups	128(%rsp), %xmm10
	movups	144(%rsp), %xmm11
	movups	160(%rsp), %xmm12
	movups	176(%rsp), %xmm13
	movups	192(%rsp), %xmm14
	movups	208(%rsp), %xmm15
#endif

	addq	$STACKSIZE, %rsp
	ret

	EPILOGUE





