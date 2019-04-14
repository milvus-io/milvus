/*********************************************************************/
/* Copyright 2009, 2010 The University of Texas at Austin.           */
/* All rights reserved.                                              */
/*                                                                   */
/* Redistribution and use in source and binary forms, with or        */
/* without modification, are permitted provided that the following   */
/* conditions are met:                                               */
/*                                                                   */
/*   1. Redistributions of source code must retain the above         */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer.                                                  */
/*                                                                   */
/*   2. Redistributions in binary form must reproduce the above      */
/*      copyright notice, this list of conditions and the following  */
/*      disclaimer in the documentation and/or other materials       */
/*      provided with the distribution.                              */
/*                                                                   */
/*    THIS  SOFTWARE IS PROVIDED  BY THE  UNIVERSITY OF  TEXAS AT    */
/*    AUSTIN  ``AS IS''  AND ANY  EXPRESS OR  IMPLIED WARRANTIES,    */
/*    INCLUDING, BUT  NOT LIMITED  TO, THE IMPLIED  WARRANTIES OF    */
/*    MERCHANTABILITY  AND FITNESS FOR  A PARTICULAR  PURPOSE ARE    */
/*    DISCLAIMED.  IN  NO EVENT SHALL THE UNIVERSITY  OF TEXAS AT    */
/*    AUSTIN OR CONTRIBUTORS BE  LIABLE FOR ANY DIRECT, INDIRECT,    */
/*    INCIDENTAL,  SPECIAL, EXEMPLARY,  OR  CONSEQUENTIAL DAMAGES    */
/*    (INCLUDING, BUT  NOT LIMITED TO,  PROCUREMENT OF SUBSTITUTE    */
/*    GOODS  OR  SERVICES; LOSS  OF  USE,  DATA,  OR PROFITS;  OR    */
/*    BUSINESS INTERRUPTION) HOWEVER CAUSED  AND ON ANY THEORY OF    */
/*    LIABILITY, WHETHER  IN CONTRACT, STRICT  LIABILITY, OR TORT    */
/*    (INCLUDING NEGLIGENCE OR OTHERWISE)  ARISING IN ANY WAY OUT    */
/*    OF  THE  USE OF  THIS  SOFTWARE,  EVEN  IF ADVISED  OF  THE    */
/*    POSSIBILITY OF SUCH DAMAGE.                                    */
/*                                                                   */
/* The views and conclusions contained in the software and           */
/* documentation are those of the authors and should not be          */
/* interpreted as representing official policies, either expressed   */
/* or implied, of The University of Texas at Austin.                 */
/*********************************************************************/


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

#define STACKSIZE 256

#define OLD_A		40 + STACKSIZE(%rsp)
#define OLD_B		48 + STACKSIZE(%rsp)
#define OLD_C		56 + STACKSIZE(%rsp)
#define OLD_LDC		64 + STACKSIZE(%rsp)
#define OLD_OFFSET	72 + STACKSIZE(%rsp)

#endif

#define L_BUFFER_SIZE 8192
#define LB2_OFFSET    4096

#define Ndiv6	 24(%rsp)
#define Nmod6	 32(%rsp)
#define N	 40(%rsp)
#define ALPHA	 48(%rsp)
#define OFFSET	 56(%rsp)
#define KK	 64(%rsp)
#define KKK	 72(%rsp)
#define BUFFER1	           128(%rsp)
#define BUFFER2	LB2_OFFSET+128(%rsp)

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



#define	A_PR1	384
#define	B_PR1	192

/*******************************************************************************************
* 3 lines of N
*******************************************************************************************/

#define KERNEL16x3_1(xx) \
	vbroadcastss	 -6 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	 -5 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %xmm3 ;\
	nop						;\
	vfmaddps  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddps  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	prefetcht0	A_PR1(AO,%rax,SIZE)	;\
	vfmaddps  	%xmm9,%xmm3,%xmm0,%xmm9 ;\
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vfmaddps  	%xmm11,%xmm2,%xmm0,%xmm11 ;\
	vfmaddps  	%xmm12,%xmm3,%xmm0,%xmm12 ;\
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm13,%xmm1,%xmm0,%xmm13 ;\
	vfmaddps  	%xmm14,%xmm2,%xmm0,%xmm14 ;\
	vbroadcastss	 -3 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vbroadcastss	 -2 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm15,%xmm3,%xmm0,%xmm15 ;\

#define KERNEL16x3_2(xx) \
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vbroadcastss	 -1 * SIZE(BO, BI, SIZE), %xmm3 ;\
	nop						;\
	vfmaddps  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	vmovups 	-12 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddps  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	prefetcht0	A_PR1+64(AO,%rax,SIZE)	;\
	vfmaddps  	%xmm9,%xmm3,%xmm0,%xmm9 ;\
	vmovups 	 -8 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vfmaddps  	%xmm11,%xmm2,%xmm0,%xmm11 ;\
	vfmaddps  	%xmm12,%xmm3,%xmm0,%xmm12 ;\
	vmovups 	 -4 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm13,%xmm1,%xmm0,%xmm13 ;\
	vfmaddps  	%xmm14,%xmm2,%xmm0,%xmm14 ;\
	vbroadcastss	  0 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vbroadcastss	  1 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm15,%xmm3,%xmm0,%xmm15 ;\

#define KERNEL16x3_3(xx) \
	vmovups 	  0 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vbroadcastss	  2 * SIZE(BO, BI, SIZE), %xmm3 ;\
	nop						;\
	vfmaddps  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	vmovups 	  4 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddps  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	prefetcht0	A_PR1+128(AO,%rax,SIZE)	;\
	vfmaddps  	%xmm9,%xmm3,%xmm0,%xmm9 ;\
	vmovups 	  8 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vfmaddps  	%xmm11,%xmm2,%xmm0,%xmm11 ;\
	vfmaddps  	%xmm12,%xmm3,%xmm0,%xmm12 ;\
	vmovups 	 12 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm13,%xmm1,%xmm0,%xmm13 ;\
	vfmaddps  	%xmm14,%xmm2,%xmm0,%xmm14 ;\
	vbroadcastss	  3 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vbroadcastss	  4 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm15,%xmm3,%xmm0,%xmm15 ;\

#define KERNEL16x3_4(xx) \
	vmovups 	 16 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vbroadcastss	  5 * SIZE(BO, BI, SIZE), %xmm3 ;\
	nop						;\
	vfmaddps  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	vmovups 	 20 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddps  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	prefetcht0	A_PR1+192(AO,%rax,SIZE)	;\
	vfmaddps  	%xmm9,%xmm3,%xmm0,%xmm9 ;\
	vmovups 	 24 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vfmaddps  	%xmm11,%xmm2,%xmm0,%xmm11 ;\
	addq	$12, BI				  ;\
	vfmaddps  	%xmm12,%xmm3,%xmm0,%xmm12 ;\
	vmovups 	 28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm13,%xmm1,%xmm0,%xmm13 ;\
	vfmaddps  	%xmm14,%xmm2,%xmm0,%xmm14 ;\
	addq	$64, %rax 			  ;\
	vfmaddps  	%xmm15,%xmm3,%xmm0,%xmm15 ;\

#define KERNEL16x3_SUB(xx) \
	vbroadcastss	 -6 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	 -5 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %xmm3 ;\
	nop						;\
	vfmaddps  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddps  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vfmaddps  	%xmm9,%xmm3,%xmm0,%xmm9 ;\
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vfmaddps  	%xmm11,%xmm2,%xmm0,%xmm11 ;\
	vfmaddps  	%xmm12,%xmm3,%xmm0,%xmm12 ;\
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm13,%xmm1,%xmm0,%xmm13 ;\
	vfmaddps  	%xmm14,%xmm2,%xmm0,%xmm14 ;\
	vfmaddps  	%xmm15,%xmm3,%xmm0,%xmm15 ;\


/*******************************************************************************************/

#define KERNEL8x3_1(xx) \
	prefetcht0	A_PR1(AO,%rax,SIZE)	;\
	vbroadcastss	 -6 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	 -5 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %xmm3 ;\
	nop						;\
	vfmaddps  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddps  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vfmaddps  	%xmm9,%xmm3,%xmm0,%xmm9 ;\

#define KERNEL8x3_2(xx) \
	vbroadcastss	 -3 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	 -2 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vbroadcastss	 -1 * SIZE(BO, BI, SIZE), %xmm3 ;\
	nop						;\
	vfmaddps  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddps  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vfmaddps  	%xmm9,%xmm3,%xmm0,%xmm9 ;\

#define KERNEL8x3_3(xx) \
	prefetcht0	A_PR1+64(AO,%rax,SIZE)	;\
	vbroadcastss	  0 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	  1 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vbroadcastss	  2 * SIZE(BO, BI, SIZE), %xmm3 ;\
	nop						;\
	vfmaddps  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	vmovups 	-12 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddps  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vfmaddps  	%xmm9,%xmm3,%xmm0,%xmm9 ;\

#define KERNEL8x3_4(xx) \
	vbroadcastss	  3 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	 -8 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	  4 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vbroadcastss	  5 * SIZE(BO, BI, SIZE), %xmm3 ;\
	nop						;\
	vfmaddps  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	vmovups 	 -4 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddps  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vfmaddps  	%xmm9,%xmm3,%xmm0,%xmm9 ;\
	addq	$12, BI				  ;\
	addq	$32, %rax 			  ;\

#define KERNEL8x3_SUB(xx) \
	vbroadcastss	 -6 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	 -5 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %xmm3 ;\
	nop						;\
	vfmaddps  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddps  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vfmaddps  	%xmm9,%xmm3,%xmm0,%xmm9 ;\


/*******************************************************************************************/

#define KERNEL4x3_1(xx) \
	prefetcht0	A_PR1(AO,%rax,SIZE)	;\
	vbroadcastss	 -6 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	 -5 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %xmm3 ;\
	vfmaddps  	%xmm6,%xmm3,%xmm0,%xmm6 ;\

#define KERNEL4x3_2(xx) \
	vbroadcastss	 -3 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	 -2 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vbroadcastss	 -1 * SIZE(BO, BI, SIZE), %xmm3 ;\
	vfmaddps  	%xmm6,%xmm3,%xmm0,%xmm6 ;\

#define KERNEL4x3_3(xx) \
	vbroadcastss	  0 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	  1 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vbroadcastss	  2 * SIZE(BO, BI, SIZE), %xmm3 ;\
	vfmaddps  	%xmm6,%xmm3,%xmm0,%xmm6 ;\

#define KERNEL4x3_4(xx) \
	vbroadcastss	  3 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	  4 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vbroadcastss	  5 * SIZE(BO, BI, SIZE), %xmm3 ;\
	vfmaddps  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	addq	$12, BI				  ;\
	addq	$16, %rax 			  ;\

#define KERNEL4x3_SUB(xx) \
	vbroadcastss	 -6 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	 -5 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %xmm3 ;\
	vfmaddps  	%xmm6,%xmm3,%xmm0,%xmm6 ;\

/*******************************************************************************************/

#define KERNEL2x3_1(xx) \
	vmovss	 -6 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss	 -5 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddss  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovss	 -4 * SIZE(BO, BI, SIZE), %xmm3 ;\
	vfmaddss  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	vmovss 	-31 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm8,%xmm1,%xmm0,%xmm8 ;\
	vfmaddss  	%xmm10,%xmm2,%xmm0,%xmm10 ;\
	vfmaddss  	%xmm12,%xmm3,%xmm0,%xmm12 ;\

#define KERNEL2x3_2(xx) \
	vmovss	 -3 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-30 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss	 -2 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddss  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovss	 -1 * SIZE(BO, BI, SIZE), %xmm3 ;\
	vfmaddss  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	vmovss 	-29 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm8,%xmm1,%xmm0,%xmm8 ;\
	vfmaddss  	%xmm10,%xmm2,%xmm0,%xmm10 ;\
	vfmaddss  	%xmm12,%xmm3,%xmm0,%xmm12 ;\

#define KERNEL2x3_3(xx) \
	vmovss	  0 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss	  1 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddss  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovss	  2 * SIZE(BO, BI, SIZE), %xmm3 ;\
	vfmaddss  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	vmovss 	-27 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm8,%xmm1,%xmm0,%xmm8 ;\
	vfmaddss  	%xmm10,%xmm2,%xmm0,%xmm10 ;\
	vfmaddss  	%xmm12,%xmm3,%xmm0,%xmm12 ;\

#define KERNEL2x3_4(xx) \
	vmovss	  3 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-26 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss	  4 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddss  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovss	  5 * SIZE(BO, BI, SIZE), %xmm3 ;\
	vfmaddss  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	vmovss 	-25 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm8,%xmm1,%xmm0,%xmm8 ;\
	vfmaddss  	%xmm10,%xmm2,%xmm0,%xmm10 ;\
	vfmaddss  	%xmm12,%xmm3,%xmm0,%xmm12 ;\
	addq	$12, BI				  ;\
	addq	$8, %rax 			  ;\

#define KERNEL2x3_SUB(xx) \
	vmovss	 -6 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss	 -5 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddss  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovss	 -4 * SIZE(BO, BI, SIZE), %xmm3 ;\
	vfmaddss  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	vmovss 	-31 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm8,%xmm1,%xmm0,%xmm8 ;\
	vfmaddss  	%xmm10,%xmm2,%xmm0,%xmm10 ;\
	vfmaddss  	%xmm12,%xmm3,%xmm0,%xmm12 ;\

/*******************************************************************************************/

#define KERNEL1x3_1(xx) \
	vmovss	 -6 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss	 -5 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddss  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovss	 -4 * SIZE(BO, BI, SIZE), %xmm3 ;\
	vfmaddss  	%xmm6,%xmm3,%xmm0,%xmm6 ;\

#define KERNEL1x3_2(xx) \
	vmovss	 -3 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-31 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss	 -2 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddss  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovss	 -1 * SIZE(BO, BI, SIZE), %xmm3 ;\
	vfmaddss  	%xmm6,%xmm3,%xmm0,%xmm6 ;\

#define KERNEL1x3_3(xx) \
	vmovss	  0 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-30 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss	  1 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddss  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovss	  2 * SIZE(BO, BI, SIZE), %xmm3 ;\
	vfmaddss  	%xmm6,%xmm3,%xmm0,%xmm6 ;\

#define KERNEL1x3_4(xx) \
	vmovss	  3 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-29 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss	  4 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddss  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovss	  5 * SIZE(BO, BI, SIZE), %xmm3 ;\
	vfmaddss  	%xmm6,%xmm3,%xmm0,%xmm6 ;\
	addq	$12, BI				  ;\
	addq	$4, %rax 			  ;\

#define KERNEL1x3_SUB(xx) \
	vmovss	 -6 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss	 -5 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddss  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovss	 -4 * SIZE(BO, BI, SIZE), %xmm3 ;\
	vfmaddss  	%xmm6,%xmm3,%xmm0,%xmm6 ;\

/*******************************************************************************************/

/*******************************************************************************************
* 2 lines of N
*******************************************************************************************/

#define KERNEL16x2_1(xx) \
	prefetcht0	A_PR1(AO,%rax,SIZE)	;\
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	 -3 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddps  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vfmaddps  	%xmm11,%xmm2,%xmm0,%xmm11 ;\
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm13,%xmm1,%xmm0,%xmm13 ;\
	vfmaddps  	%xmm14,%xmm2,%xmm0,%xmm14 ;\

#define KERNEL16x2_2(xx) \
	prefetcht0	A_PR1+64(AO,%rax,SIZE)	;\
	vbroadcastss	 -2 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	 -1 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovups 	-12 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddps  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vmovups 	 -8 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vfmaddps  	%xmm11,%xmm2,%xmm0,%xmm11 ;\
	vmovups 	 -4 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm13,%xmm1,%xmm0,%xmm13 ;\
	vfmaddps  	%xmm14,%xmm2,%xmm0,%xmm14 ;\

#define KERNEL16x2_3(xx) \
	prefetcht0	A_PR1+128(AO,%rax,SIZE)	;\
	vbroadcastss	  0 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	  0 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	  1 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovups 	  4 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddps  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vmovups 	  8 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vfmaddps  	%xmm11,%xmm2,%xmm0,%xmm11 ;\
	vmovups 	 12 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm13,%xmm1,%xmm0,%xmm13 ;\
	vfmaddps  	%xmm14,%xmm2,%xmm0,%xmm14 ;\

#define KERNEL16x2_4(xx) \
	prefetcht0	A_PR1+192(AO,%rax,SIZE)	;\
	vbroadcastss	  2 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	 16 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	  3 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovups 	 20 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddps  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vmovups 	 24 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vfmaddps  	%xmm11,%xmm2,%xmm0,%xmm11 ;\
	vmovups 	 28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm13,%xmm1,%xmm0,%xmm13 ;\
	vfmaddps  	%xmm14,%xmm2,%xmm0,%xmm14 ;\
	addq	$8, BI				  ;\
	addq	$64, %rax 			  ;\

#define KERNEL16x2_SUB(xx) \
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	 -3 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddps  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vfmaddps  	%xmm11,%xmm2,%xmm0,%xmm11 ;\
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm13,%xmm1,%xmm0,%xmm13 ;\
	vfmaddps  	%xmm14,%xmm2,%xmm0,%xmm14 ;\


/*******************************************************************************************/

#define KERNEL8x2_1(xx) \
	prefetcht0	A_PR1(AO,%rax,SIZE)	;\
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	 -3 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddps  	%xmm8,%xmm2,%xmm0,%xmm8 ;\

#define KERNEL8x2_2(xx) \
	vbroadcastss	 -2 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	 -1 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddps  	%xmm8,%xmm2,%xmm0,%xmm8 ;\

#define KERNEL8x2_3(xx) \
	prefetcht0	A_PR1+64(AO,%rax,SIZE)	;\
	vbroadcastss	  0 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	  1 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovups 	-12 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddps  	%xmm8,%xmm2,%xmm0,%xmm8 ;\

#define KERNEL8x2_4(xx) \
	vbroadcastss	  2 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	 -8 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	  3 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovups 	 -4 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddps  	%xmm8,%xmm2,%xmm0,%xmm8 ;\
	addq	$8, BI				  ;\
	addq	$32, %rax 			  ;\

#define KERNEL8x2_SUB(xx) \
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	 -3 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vfmaddps  	%xmm8,%xmm2,%xmm0,%xmm8 ;\


/*******************************************************************************************/

#define KERNEL4x2_1(xx) \
	prefetcht0	A_PR1(AO,%rax,SIZE)	;\
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	 -3 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\

#define KERNEL4x2_2(xx) \
	vbroadcastss	 -2 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	 -1 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\

#define KERNEL4x2_3(xx) \
	vbroadcastss	  0 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	  1 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\

#define KERNEL4x2_4(xx) \
	vbroadcastss	  2 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	  3 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	addq	$8, BI				  ;\
	addq	$16, %rax 			  ;\

#define KERNEL4x2_SUB(xx) \
	vbroadcastss	 -4 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vbroadcastss	 -3 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddps  	%xmm5,%xmm2,%xmm0,%xmm5 ;\

/*******************************************************************************************/

#define KERNEL2x2_1(xx) \
	vmovss	 -4 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss	 -3 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddss  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovss 	-31 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm8,%xmm1,%xmm0,%xmm8 ;\
	vfmaddss  	%xmm10,%xmm2,%xmm0,%xmm10 ;\

#define KERNEL2x2_2(xx) \
	vmovss	 -2 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-30 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss	 -1 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddss  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovss 	-29 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm8,%xmm1,%xmm0,%xmm8 ;\
	vfmaddss  	%xmm10,%xmm2,%xmm0,%xmm10 ;\

#define KERNEL2x2_3(xx) \
	vmovss	  0 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss	  1 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddss  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovss 	-27 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm8,%xmm1,%xmm0,%xmm8 ;\
	vfmaddss  	%xmm10,%xmm2,%xmm0,%xmm10 ;\

#define KERNEL2x2_4(xx) \
	vmovss	  2 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-26 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss	  3 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddss  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovss 	-25 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm8,%xmm1,%xmm0,%xmm8 ;\
	vfmaddss  	%xmm10,%xmm2,%xmm0,%xmm10 ;\
	addq	$8, BI				  ;\
	addq	$8, %rax 			  ;\

#define KERNEL2x2_SUB(xx) \
	vmovss	 -4 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss	 -3 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddss  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	vmovss 	-31 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm8,%xmm1,%xmm0,%xmm8 ;\
	vfmaddss  	%xmm10,%xmm2,%xmm0,%xmm10 ;\

/*******************************************************************************************/

#define KERNEL1x2_1(xx) \
	vmovss	 -4 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss	 -3 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddss  	%xmm5,%xmm2,%xmm0,%xmm5 ;\

#define KERNEL1x2_2(xx) \
	vmovss	 -2 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-31 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss	 -1 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddss  	%xmm5,%xmm2,%xmm0,%xmm5 ;\

#define KERNEL1x2_3(xx) \
	vmovss	  0 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-30 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss	  1 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddss  	%xmm5,%xmm2,%xmm0,%xmm5 ;\

#define KERNEL1x2_4(xx) \
	vmovss	  2 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-29 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss	  3 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddss  	%xmm5,%xmm2,%xmm0,%xmm5 ;\
	addq	$8, BI				  ;\
	addq	$4, %rax 			  ;\

#define KERNEL1x2_SUB(xx) \
	vmovss	 -4 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss	 -3 * SIZE(BO, BI, SIZE), %xmm2 ;\
	vfmaddss  	%xmm5,%xmm2,%xmm0,%xmm5 ;\

/*******************************************************************************************/

/*******************************************************************************************
* 1 line of N
*******************************************************************************************/

#define KERNEL16x1_1(xx) \
	prefetcht0	A_PR1(AO,%rax,SIZE)	;\
	vbroadcastss	 -2 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm13,%xmm1,%xmm0,%xmm13 ;\

#define KERNEL16x1_2(xx) \
	prefetcht0	A_PR1+64(AO,%rax,SIZE)	;\
	vbroadcastss	 -1 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovups 	-12 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vmovups 	 -8 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vmovups 	 -4 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm13,%xmm1,%xmm0,%xmm13 ;\

#define KERNEL16x1_3(xx) \
	prefetcht0	A_PR1+128(AO,%rax,SIZE)	;\
	vbroadcastss	  0 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	  0 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovups 	  4 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vmovups 	  8 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vmovups 	 12 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm13,%xmm1,%xmm0,%xmm13 ;\

#define KERNEL16x1_4(xx) \
	prefetcht0	A_PR1+192(AO,%rax,SIZE)	;\
	vbroadcastss	  1 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	 16 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovups 	 20 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vmovups 	 24 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vmovups 	 28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm13,%xmm1,%xmm0,%xmm13 ;\
	addq	$4, BI				  ;\
	addq	$64, %rax 			  ;\

#define KERNEL16x1_SUB(xx) \
	vbroadcastss	 -2 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm10,%xmm1,%xmm0,%xmm10 ;\
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm13,%xmm1,%xmm0,%xmm13 ;\


/*******************************************************************************************/

#define KERNEL8x1_1(xx) \
	prefetcht0	A_PR1(AO,%rax,SIZE)	;\
	vbroadcastss	 -2 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\

#define KERNEL8x1_2(xx) \
	vbroadcastss	 -1 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\

#define KERNEL8x1_3(xx) \
	prefetcht0	A_PR1+64(AO,%rax,SIZE)	;\
	vbroadcastss	  0 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-16 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovups 	-12 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\

#define KERNEL8x1_4(xx) \
	vbroadcastss	  1 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	 -8 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovups 	 -4 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\
	addq	$4, BI				  ;\
	addq	$32, %rax 			  ;\

#define KERNEL8x1_SUB(xx) \
	vbroadcastss	 -2 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm7,%xmm1,%xmm0,%xmm7 ;\


/*******************************************************************************************/

#define KERNEL4x1_1(xx) \
	prefetcht0	A_PR1(AO,%rax,SIZE)	;\
	vbroadcastss	 -2 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\

#define KERNEL4x1_2(xx) \
	vbroadcastss	 -1 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\

#define KERNEL4x1_3(xx) \
	vbroadcastss	  0 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-24 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\

#define KERNEL4x1_4(xx) \
	vbroadcastss	  1 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-20 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	addq	$4, BI				  ;\
	addq	$16, %rax 			  ;\

#define KERNEL4x1_SUB(xx) \
	vbroadcastss	 -2 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovups 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddps  	%xmm4,%xmm1,%xmm0,%xmm4 ;\

/*******************************************************************************************/

#define KERNEL2x1_1(xx) \
	vmovss	 -2 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss 	-31 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm8,%xmm1,%xmm0,%xmm8 ;\

#define KERNEL2x1_2(xx) \
	vmovss	 -1 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-30 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss 	-29 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm8,%xmm1,%xmm0,%xmm8 ;\

#define KERNEL2x1_3(xx) \
	vmovss	  0 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-28 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss 	-27 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm8,%xmm1,%xmm0,%xmm8 ;\

#define KERNEL2x1_4(xx) \
	vmovss	  1 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-26 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss 	-25 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm8,%xmm1,%xmm0,%xmm8 ;\
	addq	$4, BI				  ;\
	addq	$8, %rax 			  ;\

#define KERNEL2x1_SUB(xx) \
	vmovss	 -2 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	vmovss 	-31 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm8,%xmm1,%xmm0,%xmm8 ;\

/*******************************************************************************************/

#define KERNEL1x1_1(xx) \
	vmovss	 -2 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\

#define KERNEL1x1_2(xx) \
	vmovss	 -1 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-31 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\

#define KERNEL1x1_3(xx) \
	vmovss	  0 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-30 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\

#define KERNEL1x1_4(xx) \
	vmovss	  1 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-29 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\
	addq	$4, BI				  ;\
	addq	$4, %rax 			  ;\

#define KERNEL1x1_SUB(xx) \
	vmovss	 -2 * SIZE(BO, BI, SIZE), %xmm1 ;\
	vmovss 	-32 * SIZE(AO, %rax, SIZE), %xmm0 ;\
	vfmaddss  	%xmm4,%xmm1,%xmm0,%xmm4 ;\

/*******************************************************************************************/

#if !defined(TRMMKERNEL)


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

	vmovaps	%xmm3, %xmm0

#else
	movq	STACKSIZE +  8(%rsp), LDC

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

	vmovsd	 %xmm0, ALPHA

	salq	$BASE_SHIFT, LDC

	movq    N, %rax
        xorq    %rdx, %rdx
        movq    $6,  %rdi
        divq    %rdi                    //    N / 6
        movq    %rax, Ndiv6             //    N / 6
        movq    %rdx, Nmod6             //    N % 6


	movq	Ndiv6,  J
	cmpq	$0, J
	je	.L2_0
	ALIGN_4

.L6_01:
        // copy to sub buffer
        movq    K, %rax
        salq    $1,%rax                 // K * 2 ; read 2 values
        movq    B, BO1
        leaq    (B,%rax, SIZE), BO2     // next offset to BO2
        leaq    BUFFER1, BO             // first buffer to BO
        movq    K, %rax
	sarq	$3 , %rax		// K / 8
	jz	.L6_01a_2
        ALIGN_4

.L6_01a_1:

        prefetcht0 512(BO1)
        prefetcht0 512(BO2)
        prefetchw  512(BO)

	vmovsd	0 * SIZE(BO1), %xmm0
	vmovsd	2 * SIZE(BO1), %xmm2
	vmovsd	4 * SIZE(BO1), %xmm4
	vmovsd	6 * SIZE(BO1), %xmm6
	vmovss  0 * SIZE(BO2), %xmm1
	vmovss  2 * SIZE(BO2), %xmm3
	vmovss  4 * SIZE(BO2), %xmm5
	vmovss  6 * SIZE(BO2), %xmm7
	vmovsd	%xmm0, 0*SIZE(BO)
	vmovss	%xmm1, 2*SIZE(BO)
	vmovsd	%xmm2, 3*SIZE(BO)
	vmovss	%xmm3, 5*SIZE(BO)
	vmovsd	%xmm4, 6*SIZE(BO)
	vmovss	%xmm5, 8*SIZE(BO)
	vmovsd	%xmm6, 9*SIZE(BO)
	vmovss	%xmm7,11*SIZE(BO)
	addq	$8*SIZE,BO1
	addq	$8*SIZE,BO2
	addq	$12*SIZE,BO

	vmovsd	0 * SIZE(BO1), %xmm0
	vmovsd	2 * SIZE(BO1), %xmm2
	vmovsd	4 * SIZE(BO1), %xmm4
	vmovsd	6 * SIZE(BO1), %xmm6
	vmovss  0 * SIZE(BO2), %xmm1
	vmovss  2 * SIZE(BO2), %xmm3
	vmovss  4 * SIZE(BO2), %xmm5
	vmovss  6 * SIZE(BO2), %xmm7
	vmovsd	%xmm0, 0*SIZE(BO)
	vmovss	%xmm1, 2*SIZE(BO)
	vmovsd	%xmm2, 3*SIZE(BO)
	vmovss	%xmm3, 5*SIZE(BO)
	vmovsd	%xmm4, 6*SIZE(BO)
	vmovss	%xmm5, 8*SIZE(BO)
	vmovsd	%xmm6, 9*SIZE(BO)
	vmovss	%xmm7,11*SIZE(BO)
	addq	$8*SIZE,BO1
	addq	$8*SIZE,BO2
	addq	$12*SIZE,BO

	decq	%rax
	jnz	.L6_01a_1



.L6_01a_2:

	movq    K, %rax
        andq    $7, %rax                // K % 8
        jz      .L6_02c
        ALIGN_4


.L6_02b:

	vmovsd	0 * SIZE(BO1), %xmm0
	vmovss  0 * SIZE(BO2), %xmm2
	vmovsd	%xmm0, 0*SIZE(BO)
	vmovss	%xmm2, 2*SIZE(BO)
	addq	$2*SIZE,BO1
	addq	$2*SIZE,BO2
	addq	$3*SIZE,BO
	decq	%rax
	jnz	.L6_02b

.L6_02c:

	movq	K, %rax
	salq	$1,%rax			// K * 2
	leaq	(B,%rax, SIZE), BO1	// next offset to BO1
	leaq	(BO1,%rax, SIZE), BO2	// next offset to BO2
	leaq    BUFFER2, BO		// second buffer to BO
	movq	K, %rax
	sarq	$3 , %rax		// K / 8
	jz	.L6_02c_2
	ALIGN_4

.L6_02c_1:

	prefetcht0 512(BO2)
        prefetchw  512(BO)

	vmovsd	0 * SIZE(BO2), %xmm0
	vmovsd	2 * SIZE(BO2), %xmm2
	vmovsd	4 * SIZE(BO2), %xmm4
	vmovsd	6 * SIZE(BO2), %xmm6
	vmovss  1 * SIZE(BO1), %xmm1
	vmovss  3 * SIZE(BO1), %xmm3
	vmovss  5 * SIZE(BO1), %xmm5
	vmovss  7 * SIZE(BO1), %xmm7
	vmovss	%xmm1, 0*SIZE(BO)
	vmovsd	%xmm0, 1*SIZE(BO)
	vmovss	%xmm3, 3*SIZE(BO)
	vmovsd	%xmm2, 4*SIZE(BO)
	vmovss	%xmm5, 6*SIZE(BO)
	vmovsd	%xmm4, 7*SIZE(BO)
	vmovss	%xmm7, 9*SIZE(BO)
	vmovsd	%xmm6,10*SIZE(BO)
	addq	$8*SIZE,BO1
	addq	$8*SIZE,BO2
	addq	$12*SIZE,BO


	vmovsd	0 * SIZE(BO2), %xmm0
	vmovsd	2 * SIZE(BO2), %xmm2
	vmovsd	4 * SIZE(BO2), %xmm4
	vmovsd	6 * SIZE(BO2), %xmm6
	vmovss  1 * SIZE(BO1), %xmm1
	vmovss  3 * SIZE(BO1), %xmm3
	vmovss  5 * SIZE(BO1), %xmm5
	vmovss  7 * SIZE(BO1), %xmm7
	vmovss	%xmm1, 0*SIZE(BO)
	vmovsd	%xmm0, 1*SIZE(BO)
	vmovss	%xmm3, 3*SIZE(BO)
	vmovsd	%xmm2, 4*SIZE(BO)
	vmovss	%xmm5, 6*SIZE(BO)
	vmovsd	%xmm4, 7*SIZE(BO)
	vmovss	%xmm7, 9*SIZE(BO)
	vmovsd	%xmm6,10*SIZE(BO)
	addq	$8*SIZE,BO1
	addq	$8*SIZE,BO2
	addq	$12*SIZE,BO

	decq	%rax
	jnz	.L6_02c_1


.L6_02c_2:

	movq    K, %rax
        andq    $7, %rax                // K % 8
        jz      .L6_03c
        ALIGN_4

.L6_03b:

	vmovss	  1*SIZE(BO1), %xmm0
	vmovsd	  0*SIZE(BO2), %xmm1
	vmovss	%xmm0, 0*SIZE(BO)
	vmovsd	%xmm1, 1*SIZE(BO)
	addq	$2*SIZE,BO1
	addq	$2*SIZE,BO2
	addq	$3*SIZE,BO
	decq	%rax
	jnz	.L6_03b


.L6_03c:

	movq	BO2, B			// next offset of B

.L6_10:
	movq	C, CO1
	leaq	(C, LDC, 2), C		 
	leaq	(C, LDC, 1), C		// c += 3 * ldc

	
	movq	A, AO		 	// aoffset = a
	addq	$32 * SIZE, AO

	movq	M,  I
	sarq	$4, I			// i = (m >> 4)
	je	.L6_20

	ALIGN_4

.L6_11:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $6 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax			//  K = K - ( K % 8 )
	je	.L6_16
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$4, %rax			// rax = rax * 16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L6_12:

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL16x3_1(xxx)
	KERNEL16x3_2(xxx)
	KERNEL16x3_3(xxx)
	KERNEL16x3_4(xxx)

	KERNEL16x3_1(xxx)
	prefetcht0	B_PR1+16(BO,BI, SIZE)
	KERNEL16x3_2(xxx)
	KERNEL16x3_3(xxx)
	KERNEL16x3_4(xxx)

	je	.L6_16

	KERNEL16x3_1(xxx)
	KERNEL16x3_2(xxx)
	prefetcht0	B_PR1+32(BO,BI, SIZE)
	KERNEL16x3_3(xxx)
	KERNEL16x3_4(xxx)

	KERNEL16x3_1(xxx)
	KERNEL16x3_2(xxx)
	KERNEL16x3_3(xxx)
	KERNEL16x3_4(xxx)

	je	.L6_16

	jmp	.L6_12
	ALIGN_4

.L6_16:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L6_19

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$4, %rax			// rax = rax * 16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L6_17:

	KERNEL16x3_SUB(xxx)
	addq	$3, BI
	addq	$16, %rax
	jl	.L6_17
	ALIGN_4


.L6_19:

	vbroadcastss	ALPHA, %xmm0

	vfmaddps 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddps 4 * SIZE(CO1),%xmm0, %xmm7,%xmm7
	vfmaddps 8 * SIZE(CO1),%xmm0, %xmm10,%xmm10
	vfmaddps 12 * SIZE(CO1),%xmm0, %xmm13,%xmm13

	vfmaddps 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddps 4 * SIZE(CO1, LDC),%xmm0, %xmm8,%xmm8
	vfmaddps 8 * SIZE(CO1, LDC),%xmm0, %xmm11,%xmm11
	vfmaddps 12 * SIZE(CO1, LDC),%xmm0, %xmm14,%xmm14

	vfmaddps 	 (CO1, LDC, 2),%xmm0, %xmm6,%xmm6
	vfmaddps 4 * SIZE(CO1, LDC, 2),%xmm0, %xmm9,%xmm9
	vfmaddps 8 * SIZE(CO1, LDC, 2),%xmm0, %xmm12,%xmm12
	vfmaddps 12 * SIZE(CO1, LDC, 2),%xmm0, %xmm15,%xmm15


	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 4 * SIZE(CO1)
	vmovups	%xmm10, 8 * SIZE(CO1)
	vmovups	%xmm13,12 * SIZE(CO1)

	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm8 , 4 * SIZE(CO1, LDC)
	vmovups	%xmm11, 8 * SIZE(CO1, LDC)
	vmovups	%xmm14,12 * SIZE(CO1, LDC)

	vmovups	%xmm6 ,  	(CO1, LDC, 2)
	vmovups	%xmm9 , 4 * SIZE(CO1, LDC, 2)
	vmovups	%xmm12, 8 * SIZE(CO1, LDC, 2)
	vmovups	%xmm15,12 * SIZE(CO1, LDC, 2)


	addq	$16 * SIZE, CO1		# coffset += 16
	decq	I			# i --
	jg	.L6_11
	ALIGN_4	

/**************************************************************************
* Rest of M 
***************************************************************************/
.L6_20:
	// Test rest of M

	testq	$15, M
	jz	.L7_10		// to next 3 lines of N

	testq	$8, M		
	jz	.L6_21pre
	ALIGN_4

/**************************************************************************/

.L6_20_1:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $6 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L6_20_6
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L6_20_2:

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL8x3_1(xxx)
	KERNEL8x3_2(xxx)
	KERNEL8x3_3(xxx)
	KERNEL8x3_4(xxx)

	KERNEL8x3_1(xxx)
	prefetcht0	B_PR1+16(BO,BI, SIZE)
	KERNEL8x3_2(xxx)
	KERNEL8x3_3(xxx)
	KERNEL8x3_4(xxx)

	je	.L6_20_6

	KERNEL8x3_1(xxx)
	KERNEL8x3_2(xxx)
	prefetcht0	B_PR1+32(BO,BI, SIZE)
	KERNEL8x3_3(xxx)
	KERNEL8x3_4(xxx)

	KERNEL8x3_1(xxx)
	KERNEL8x3_2(xxx)
	KERNEL8x3_3(xxx)
	KERNEL8x3_4(xxx)

	je	.L6_20_6

	jmp	.L6_20_2
	ALIGN_4

.L6_20_6:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L6_20_9

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L6_20_7:

	KERNEL8x3_SUB(xxx)
	addq	$3, BI
	addq	$8, %rax
	jl	.L6_20_7
	ALIGN_4


.L6_20_9:

	vbroadcastss	ALPHA, %xmm0

	vfmaddps 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddps 4 * SIZE(CO1),%xmm0, %xmm7,%xmm7

	vfmaddps 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddps 4 * SIZE(CO1, LDC),%xmm0, %xmm8,%xmm8

	vfmaddps 	 (CO1, LDC, 2),%xmm0, %xmm6,%xmm6
	vfmaddps 4 * SIZE(CO1, LDC, 2),%xmm0, %xmm9,%xmm9


	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 4 * SIZE(CO1)

	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm8 , 4 * SIZE(CO1, LDC)

	vmovups	%xmm6 ,  	(CO1, LDC, 2)
	vmovups	%xmm9 , 4 * SIZE(CO1, LDC, 2)


	addq	$8 * SIZE, CO1		# coffset += 8
	ALIGN_4
	


/**************************************************************************/

.L6_21pre:

	testq	$4, M		
	jz	.L6_30
	ALIGN_4

.L6_21:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $6 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L6_26
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L6_22:

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL4x3_1(xxx)
	KERNEL4x3_2(xxx)
	KERNEL4x3_3(xxx)
	KERNEL4x3_4(xxx)

	KERNEL4x3_1(xxx)
	prefetcht0	B_PR1+16(BO,BI, SIZE)
	KERNEL4x3_2(xxx)
	KERNEL4x3_3(xxx)
	KERNEL4x3_4(xxx)

	je	.L6_26

	KERNEL4x3_1(xxx)
	KERNEL4x3_2(xxx)
	prefetcht0	B_PR1+32(BO,BI, SIZE)
	KERNEL4x3_3(xxx)
	KERNEL4x3_4(xxx)

	KERNEL4x3_1(xxx)
	KERNEL4x3_2(xxx)
	KERNEL4x3_3(xxx)
	KERNEL4x3_4(xxx)

	je	.L6_26

	jmp	.L6_22
	ALIGN_4

.L6_26:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L6_29

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L6_27:

	KERNEL4x3_SUB(xxx)
	addq	$3, BI
	addq	$4, %rax
	jl	.L6_27
	ALIGN_4


.L6_29:

	vbroadcastss	ALPHA, %xmm0

	vfmaddps 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddps 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddps 	 (CO1, LDC, 2),%xmm0, %xmm6,%xmm6


	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm6 ,  	(CO1, LDC, 2)

	addq	$4 * SIZE, CO1		# coffset += 4
	ALIGN_4
	

.L6_30:
	testq	$2, M		
	jz	.L6_40

	ALIGN_4

.L6_31:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $6 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L6_36
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L6_32:

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x3_1(xxx)
	KERNEL2x3_2(xxx)
	KERNEL2x3_3(xxx)
	KERNEL2x3_4(xxx)

	KERNEL2x3_1(xxx)
	prefetcht0	B_PR1+16(BO,BI,SIZE)
	KERNEL2x3_2(xxx)
	KERNEL2x3_3(xxx)
	KERNEL2x3_4(xxx)

	je	.L6_36

	KERNEL2x3_1(xxx)
	KERNEL2x3_2(xxx)
	prefetcht0	B_PR1+32(BO,BI,SIZE)
	KERNEL2x3_3(xxx)
	KERNEL2x3_4(xxx)

	KERNEL2x3_1(xxx)
	KERNEL2x3_2(xxx)
	KERNEL2x3_3(xxx)
	KERNEL2x3_4(xxx)

	je	.L6_36

	jmp	.L6_32
	ALIGN_4

.L6_36:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L6_39

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values
	
	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L6_37:

	KERNEL2x3_SUB(xxx)
	addq	$3, BI
	addq	$2, %rax
	jl	.L6_37
	ALIGN_4


.L6_39:

	vmovss	ALPHA, %xmm0

	vfmaddss 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddss 1 * SIZE(CO1),%xmm0, %xmm8,%xmm8
	vfmaddss 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddss 1 * SIZE(CO1, LDC),%xmm0, %xmm10,%xmm10
	vfmaddss 	 (CO1, LDC, 2),%xmm0, %xmm6,%xmm6
	vfmaddss 1 * SIZE(CO1, LDC, 2),%xmm0, %xmm12,%xmm12


	vmovss	%xmm4 ,  	(CO1)
	vmovss	%xmm8 , 1 * SIZE(CO1)
	vmovss	%xmm5 ,  	(CO1, LDC)
	vmovss	%xmm10, 1 * SIZE(CO1, LDC)
	vmovss	%xmm6 ,  	(CO1, LDC, 2)
	vmovss	%xmm12, 1 * SIZE(CO1, LDC, 2)

	addq	$2 * SIZE, CO1		# coffset += 2
	ALIGN_4

.L6_40:
	testq	$1, M		
	jz	.L7_10		// to next 3 lines of N

	ALIGN_4

.L6_41:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $6 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L6_46
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L6_42:

	KERNEL1x3_1(xxx)
	KERNEL1x3_2(xxx)
	KERNEL1x3_3(xxx)
	KERNEL1x3_4(xxx)

	KERNEL1x3_1(xxx)
	KERNEL1x3_2(xxx)
	KERNEL1x3_3(xxx)
	KERNEL1x3_4(xxx)

	je	.L6_46

	KERNEL1x3_1(xxx)
	KERNEL1x3_2(xxx)
	KERNEL1x3_3(xxx)
	KERNEL1x3_4(xxx)

	KERNEL1x3_1(xxx)
	KERNEL1x3_2(xxx)
	KERNEL1x3_3(xxx)
	KERNEL1x3_4(xxx)

	je	.L6_46

	jmp	.L6_42
	ALIGN_4

.L6_46:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L6_49

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L6_47:

	KERNEL1x3_SUB(xxx)
	addq	$3, BI
	addq	$1, %rax
	jl	.L6_47
	ALIGN_4


.L6_49:

	vmovss	ALPHA, %xmm0

	vfmaddss 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddss 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddss 	 (CO1, LDC, 2),%xmm0, %xmm6,%xmm6


	vmovss	%xmm4 ,  	(CO1)
	vmovss	%xmm5 ,  	(CO1, LDC)
	vmovss	%xmm6 ,  	(CO1, LDC, 2)

	addq	$1 * SIZE, CO1		# coffset += 1
	ALIGN_4
	



/***************************************************************************************************************/

.L7_10:
	movq	C, CO1
	leaq	(C, LDC, 2), C		 
	leaq	(C, LDC, 1), C		// c += 3 * ldc

	
	movq	A, AO		 	// aoffset = a
	addq	$32 * SIZE, AO

	movq	M,  I
	sarq	$4, I			// i = (m >> 4)
	je	.L7_20

	ALIGN_4

.L7_11:
        leaq    BUFFER2, BO             // second buffer to BO
        addq    $6 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax			//  K = K - ( K % 8 )
	je	.L7_16
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$4, %rax			// rax = rax * 16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L7_12:

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL16x3_1(xxx)
	KERNEL16x3_2(xxx)
	KERNEL16x3_3(xxx)
	KERNEL16x3_4(xxx)

	KERNEL16x3_1(xxx)
	prefetcht0	B_PR1+16(BO,BI, SIZE)
	KERNEL16x3_2(xxx)
	KERNEL16x3_3(xxx)
	KERNEL16x3_4(xxx)

	je	.L7_16

	KERNEL16x3_1(xxx)
	KERNEL16x3_2(xxx)
	prefetcht0	B_PR1+32(BO,BI, SIZE)
	KERNEL16x3_3(xxx)
	KERNEL16x3_4(xxx)

	KERNEL16x3_1(xxx)
	KERNEL16x3_2(xxx)
	KERNEL16x3_3(xxx)
	KERNEL16x3_4(xxx)

	je	.L7_16

	jmp	.L7_12
	ALIGN_4

.L7_16:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L7_19

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$4, %rax			// rax = rax * 16 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L7_17:

	KERNEL16x3_SUB(xxx)
	addq	$3, BI
	addq	$16, %rax
	jl	.L7_17
	ALIGN_4


.L7_19:

	vbroadcastss	ALPHA, %xmm0

	vfmaddps 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddps 4 * SIZE(CO1),%xmm0, %xmm7,%xmm7
	vfmaddps 8 * SIZE(CO1),%xmm0, %xmm10,%xmm10
	vfmaddps 12 * SIZE(CO1),%xmm0, %xmm13,%xmm13

	vfmaddps 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddps 4 * SIZE(CO1, LDC),%xmm0, %xmm8,%xmm8
	vfmaddps 8 * SIZE(CO1, LDC),%xmm0, %xmm11,%xmm11
	vfmaddps 12 * SIZE(CO1, LDC),%xmm0, %xmm14,%xmm14

	vfmaddps 	 (CO1, LDC, 2),%xmm0, %xmm6,%xmm6
	vfmaddps 4 * SIZE(CO1, LDC, 2),%xmm0, %xmm9,%xmm9
	vfmaddps 8 * SIZE(CO1, LDC, 2),%xmm0, %xmm12,%xmm12
	vfmaddps 12 * SIZE(CO1, LDC, 2),%xmm0, %xmm15,%xmm15


	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 4 * SIZE(CO1)
	vmovups	%xmm10, 8 * SIZE(CO1)
	vmovups	%xmm13,12 * SIZE(CO1)

	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm8 , 4 * SIZE(CO1, LDC)
	vmovups	%xmm11, 8 * SIZE(CO1, LDC)
	vmovups	%xmm14,12 * SIZE(CO1, LDC)

	vmovups	%xmm6 ,  	(CO1, LDC, 2)
	vmovups	%xmm9 , 4 * SIZE(CO1, LDC, 2)
	vmovups	%xmm12, 8 * SIZE(CO1, LDC, 2)
	vmovups	%xmm15,12 * SIZE(CO1, LDC, 2)


	addq	$16 * SIZE, CO1		# coffset += 16
	decq	I			# i --
	jg	.L7_11
	ALIGN_4	

/**************************************************************************
* Rest of M 
***************************************************************************/
.L7_20:
	// Test rest of M

	testq	$15, M
	jz	.L7_60		// to next 3 lines of N

	testq	$8, M		
	jz	.L7_21pre
	ALIGN_4

/**************************************************************************/

.L7_20_1:
        leaq    BUFFER2, BO             // first buffer to BO
        addq    $6 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L7_20_6
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L7_20_2:

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL8x3_1(xxx)
	KERNEL8x3_2(xxx)
	KERNEL8x3_3(xxx)
	KERNEL8x3_4(xxx)

	KERNEL8x3_1(xxx)
	prefetcht0	B_PR1+16(BO,BI, SIZE)
	KERNEL8x3_2(xxx)
	KERNEL8x3_3(xxx)
	KERNEL8x3_4(xxx)

	je	.L7_20_6

	KERNEL8x3_1(xxx)
	KERNEL8x3_2(xxx)
	prefetcht0	B_PR1+32(BO,BI, SIZE)
	KERNEL8x3_3(xxx)
	KERNEL8x3_4(xxx)

	KERNEL8x3_1(xxx)
	KERNEL8x3_2(xxx)
	KERNEL8x3_3(xxx)
	KERNEL8x3_4(xxx)

	je	.L7_20_6

	jmp	.L7_20_2
	ALIGN_4

.L7_20_6:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L7_20_9

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$3, %rax			// rax = rax * 8 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L7_20_7:

	KERNEL8x3_SUB(xxx)
	addq	$3, BI
	addq	$8, %rax
	jl	.L7_20_7
	ALIGN_4

.L7_20_9:


	vbroadcastss	ALPHA, %xmm0

	vfmaddps 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddps 4 * SIZE(CO1),%xmm0, %xmm7,%xmm7

	vfmaddps 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddps 4 * SIZE(CO1, LDC),%xmm0, %xmm8,%xmm8

	vfmaddps 	 (CO1, LDC, 2),%xmm0, %xmm6,%xmm6
	vfmaddps 4 * SIZE(CO1, LDC, 2),%xmm0, %xmm9,%xmm9

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 4 * SIZE(CO1)

	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm8 , 4 * SIZE(CO1, LDC)

	vmovups	%xmm6 ,  	(CO1, LDC, 2)
	vmovups	%xmm9 , 4 * SIZE(CO1, LDC, 2)


	addq	$8 * SIZE, CO1		# coffset += 8
	ALIGN_4
	


/**************************************************************************/

.L7_21pre:

	testq	$4, M		
	jz	.L7_30
	ALIGN_4

.L7_21:
        leaq    BUFFER2, BO             // second buffer to BO
        addq    $6 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L7_26
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L7_22:

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL4x3_1(xxx)
	KERNEL4x3_2(xxx)
	KERNEL4x3_3(xxx)
	KERNEL4x3_4(xxx)

	KERNEL4x3_1(xxx)
	prefetcht0	B_PR1+16(BO,BI, SIZE)
	KERNEL4x3_2(xxx)
	KERNEL4x3_3(xxx)
	KERNEL4x3_4(xxx)

	je	.L7_26

	KERNEL4x3_1(xxx)
	KERNEL4x3_2(xxx)
	prefetcht0	B_PR1+32(BO,BI, SIZE)
	KERNEL4x3_3(xxx)
	KERNEL4x3_4(xxx)

	KERNEL4x3_1(xxx)
	KERNEL4x3_2(xxx)
	KERNEL4x3_3(xxx)
	KERNEL4x3_4(xxx)

	je	.L7_26

	jmp	.L7_22
	ALIGN_4

.L7_26:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L7_29

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L7_27:

	KERNEL4x3_SUB(xxx)
	addq	$3, BI
	addq	$4, %rax
	jl	.L7_27
	ALIGN_4


.L7_29:

	vbroadcastss	ALPHA, %xmm0

	vfmaddps 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddps 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddps 	 (CO1, LDC, 2),%xmm0, %xmm6 ,%xmm6

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm6 ,  	(CO1, LDC, 2)

	addq	$4 * SIZE, CO1		# coffset += 4
	ALIGN_4
	

.L7_30:
	testq	$2, M		
	jz	.L7_40

	ALIGN_4

.L7_31:
        leaq    BUFFER2, BO             // second buffer to BO
        addq    $6 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L7_36
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L7_32:

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x3_1(xxx)
	KERNEL2x3_2(xxx)
	KERNEL2x3_3(xxx)
	KERNEL2x3_4(xxx)

	KERNEL2x3_1(xxx)
	prefetcht0	B_PR1+16(BO,BI,SIZE)
	KERNEL2x3_2(xxx)
	KERNEL2x3_3(xxx)
	KERNEL2x3_4(xxx)

	je	.L7_36

	KERNEL2x3_1(xxx)
	KERNEL2x3_2(xxx)
	prefetcht0	B_PR1+32(BO,BI,SIZE)
	KERNEL2x3_3(xxx)
	KERNEL2x3_4(xxx)

	KERNEL2x3_1(xxx)
	KERNEL2x3_2(xxx)
	KERNEL2x3_3(xxx)
	KERNEL2x3_4(xxx)

	je	.L7_36

	jmp	.L7_32
	ALIGN_4

.L7_36:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L7_39

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values
	
	salq	$1, %rax			// rax = rax *2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L7_37:

	KERNEL2x3_SUB(xxx)
	addq	$3, BI
	addq	$2, %rax
	jl	.L7_37
	ALIGN_4


.L7_39:

	vmovss	ALPHA, %xmm0

	vfmaddss 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddss 1 * SIZE(CO1),%xmm0, %xmm8,%xmm8
	vfmaddss 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddss 1 * SIZE(CO1, LDC),%xmm0, %xmm10,%xmm10
	vfmaddss 	 (CO1, LDC, 2),%xmm0, %xmm6,%xmm6
	vfmaddss 1 * SIZE(CO1, LDC, 2),%xmm0, %xmm12,%xmm12

	vmovss	%xmm4 ,  	(CO1)
	vmovss	%xmm8 , 1 * SIZE(CO1)
	vmovss	%xmm5 ,  	(CO1, LDC)
	vmovss	%xmm10, 1 * SIZE(CO1, LDC)
	vmovss	%xmm6 ,  	(CO1, LDC, 2)
	vmovss	%xmm12, 1 * SIZE(CO1, LDC, 2)

	addq	$2 * SIZE, CO1		# coffset += 2
	ALIGN_4

.L7_40:
	testq	$1, M		
	jz	.L7_60		// to next 3 lines of N

	ALIGN_4

.L7_41:
        leaq    BUFFER2, BO             // second buffer to BO
        addq    $6 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L7_46
	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L7_42:

	KERNEL1x3_1(xxx)
	KERNEL1x3_2(xxx)
	KERNEL1x3_3(xxx)
	KERNEL1x3_4(xxx)

	KERNEL1x3_1(xxx)
	KERNEL1x3_2(xxx)
	KERNEL1x3_3(xxx)
	KERNEL1x3_4(xxx)

	je	.L7_46

	KERNEL1x3_1(xxx)
	KERNEL1x3_2(xxx)
	KERNEL1x3_3(xxx)
	KERNEL1x3_4(xxx)

	KERNEL1x3_1(xxx)
	KERNEL1x3_2(xxx)
	KERNEL1x3_3(xxx)
	KERNEL1x3_4(xxx)

	je	.L7_46

	jmp	.L7_42
	ALIGN_4

.L7_46:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L7_49

	movq    %rax, BI                        //  Index for BO
        leaq    (BI,BI,2), BI                   //  BI = BI * 3 ; number of values

	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L7_47:

	KERNEL1x3_SUB(xxx)
	addq	$3, BI
	addq	$1, %rax
	jl	.L7_47
	ALIGN_4


.L7_49:

	vmovss	ALPHA, %xmm0

	vfmaddss 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddss 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddss 	 (CO1, LDC, 2),%xmm0, %xmm6,%xmm6

	vmovss	%xmm4 ,  	(CO1)
	vmovss	%xmm5 ,  	(CO1, LDC)
	vmovss	%xmm6 ,  	(CO1, LDC, 2)

	addq	$1 * SIZE, CO1		# coffset += 1
	ALIGN_4
	


.L7_60:

	decq	J			// j --
	jg	.L6_01


.L2_0:
	cmpq	$0, Nmod6		// N % 6 == 0
	je	.L999

/************************************************************************************************
* Loop for Nmod6 / 2 > 0
*************************************************************************************************/

	movq	Nmod6, J		
	sarq	$1, J			// j = j / 2
	je	.L1_0
	ALIGN_4

.L2_01:
	// copy to sub buffer
	movq	B, BO1
	leaq    BUFFER1, BO		// first buffer to BO
	movq	K, %rax
	ALIGN_4

.L2_02b:

	vmovsd	(BO1), %xmm0
	vmovsd	%xmm0,       (BO)
	addq	$2*SIZE,BO1
	addq	$2*SIZE,BO
	decq	%rax
	jnz	.L2_02b

.L2_02c:

	movq	BO1, B			// next offset of B

.L2_10:
	movq	C, CO1
	leaq	(C, LDC, 2), C		// c += 2 * ldc

	
	movq	A, AO		 	// aoffset = a
	addq	$32 * SIZE, AO

	movq	M,  I
	sarq	$4, I			// i = (m >> 4)
	je	.L2_20

	ALIGN_4

.L2_11:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO

	vzeroall

        movq    K, %rax

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

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL16x2_1(xxx)
	KERNEL16x2_2(xxx)
	KERNEL16x2_3(xxx)
	KERNEL16x2_4(xxx)

	KERNEL16x2_1(xxx)
	KERNEL16x2_2(xxx)
	KERNEL16x2_3(xxx)
	KERNEL16x2_4(xxx)

	je	.L2_16

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL16x2_1(xxx)
	KERNEL16x2_2(xxx)
	KERNEL16x2_3(xxx)
	KERNEL16x2_4(xxx)

	KERNEL16x2_1(xxx)
	KERNEL16x2_2(xxx)
	KERNEL16x2_3(xxx)
	KERNEL16x2_4(xxx)

	je	.L2_16

	jmp	.L2_12
	ALIGN_4

.L2_16:
        movq    K, %rax

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

	KERNEL16x2_SUB(xxx)
	addq	$2, BI
	addq	$16, %rax
	jl	.L2_17
	ALIGN_4


.L2_19:

	vbroadcastss	ALPHA, %xmm0

	vfmaddps 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddps 4 * SIZE(CO1),%xmm0, %xmm7,%xmm7
	vfmaddps 8 * SIZE(CO1),%xmm0, %xmm10,%xmm10
	vfmaddps 12 * SIZE(CO1),%xmm0, %xmm13,%xmm13

	vfmaddps 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddps 4 * SIZE(CO1, LDC),%xmm0, %xmm8,%xmm8
	vfmaddps 8 * SIZE(CO1, LDC),%xmm0, %xmm11,%xmm11
	vfmaddps 12 * SIZE(CO1, LDC),%xmm0, %xmm14,%xmm14

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 4 * SIZE(CO1)
	vmovups	%xmm10, 8 * SIZE(CO1)
	vmovups	%xmm13,12 * SIZE(CO1)

	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm8 , 4 * SIZE(CO1, LDC)
	vmovups	%xmm11, 8 * SIZE(CO1, LDC)
	vmovups	%xmm14,12 * SIZE(CO1, LDC)

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
	jz	.L2_60		// to next 3 lines of N

	testq	$8, M		
	jz	.L2_21pre
	ALIGN_4

/**************************************************************************/

.L2_20_1:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO

	vzeroall

        movq    K, %rax

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

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL8x2_1(xxx)
	KERNEL8x2_2(xxx)
	KERNEL8x2_3(xxx)
	KERNEL8x2_4(xxx)

	KERNEL8x2_1(xxx)
	KERNEL8x2_2(xxx)
	KERNEL8x2_3(xxx)
	KERNEL8x2_4(xxx)

	je	.L2_20_6

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL8x2_1(xxx)
	KERNEL8x2_2(xxx)
	KERNEL8x2_3(xxx)
	KERNEL8x2_4(xxx)

	KERNEL8x2_1(xxx)
	KERNEL8x2_2(xxx)
	KERNEL8x2_3(xxx)
	KERNEL8x2_4(xxx)

	je	.L2_20_6

	jmp	.L2_20_2
	ALIGN_4

.L2_20_6:
        movq    K, %rax

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

	KERNEL8x2_SUB(xxx)
	addq	$2, BI
	addq	$8, %rax
	jl	.L2_20_7
	ALIGN_4


.L2_20_9:

	vbroadcastss	ALPHA, %xmm0

	vfmaddps 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddps 4 * SIZE(CO1),%xmm0, %xmm7,%xmm7

	vfmaddps 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddps 4 * SIZE(CO1, LDC),%xmm0, %xmm8,%xmm8

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 4 * SIZE(CO1)

	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm8 , 4 * SIZE(CO1, LDC)


	addq	$8 * SIZE, CO1		# coffset += 8
	ALIGN_4
	


/**************************************************************************/

.L2_21pre:

	testq	$4, M		
	jz	.L2_30
	ALIGN_4

.L2_21:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO

	vzeroall

        movq    K, %rax

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

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL4x2_1(xxx)
	KERNEL4x2_2(xxx)
	KERNEL4x2_3(xxx)
	KERNEL4x2_4(xxx)

	KERNEL4x2_1(xxx)
	KERNEL4x2_2(xxx)
	KERNEL4x2_3(xxx)
	KERNEL4x2_4(xxx)

	je	.L2_26

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL4x2_1(xxx)
	KERNEL4x2_2(xxx)
	KERNEL4x2_3(xxx)
	KERNEL4x2_4(xxx)

	KERNEL4x2_1(xxx)
	KERNEL4x2_2(xxx)
	KERNEL4x2_3(xxx)
	KERNEL4x2_4(xxx)

	je	.L2_26

	jmp	.L2_22
	ALIGN_4

.L2_26:
        movq    K, %rax

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

	KERNEL4x2_SUB(xxx)
	addq	$2, BI
	addq	$4, %rax
	jl	.L2_27
	ALIGN_4


.L2_29:

	vbroadcastss	ALPHA, %xmm0

	vfmaddps 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddps 	 (CO1, LDC),%xmm0, %xmm5,%xmm5

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm5 ,  	(CO1, LDC)


	addq	$4 * SIZE, CO1		# coffset += 4
	ALIGN_4
	

.L2_30:
	testq	$2, M		
	jz	.L2_40

	ALIGN_4

.L2_31:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO

	vzeroall

        movq    K, %rax

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

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_1(xxx)
	KERNEL2x2_2(xxx)
	KERNEL2x2_3(xxx)
	KERNEL2x2_4(xxx)

	KERNEL2x2_1(xxx)
	KERNEL2x2_2(xxx)
	KERNEL2x2_3(xxx)
	KERNEL2x2_4(xxx)

	je	.L2_36

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_1(xxx)
	KERNEL2x2_2(xxx)
	KERNEL2x2_3(xxx)
	KERNEL2x2_4(xxx)

	KERNEL2x2_1(xxx)
	KERNEL2x2_2(xxx)
	KERNEL2x2_3(xxx)
	KERNEL2x2_4(xxx)

	je	.L2_36

	jmp	.L2_32
	ALIGN_4

.L2_36:
        movq    K, %rax

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

	KERNEL2x2_SUB(xxx)
	addq	$2, BI
	addq	$2, %rax
	jl	.L2_37
	ALIGN_4


.L2_39:

	vmovss	ALPHA, %xmm0

	vfmaddss 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddss 1 * SIZE(CO1),%xmm0, %xmm8,%xmm8
	vfmaddss 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddss 1 * SIZE(CO1, LDC),%xmm0, %xmm10,%xmm10

	vmovss	%xmm4 ,  	(CO1)
	vmovss	%xmm8 , 1 * SIZE(CO1)
	vmovss	%xmm5 ,  	(CO1, LDC)
	vmovss	%xmm10, 1 * SIZE(CO1, LDC)

	addq	$2 * SIZE, CO1		# coffset += 2
	ALIGN_4

.L2_40:
	testq	$1, M		
	jz	.L2_60		// to next 2 lines of N

	ALIGN_4

.L2_41:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $4 * SIZE, BO

	vzeroall

        movq    K, %rax

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

	KERNEL1x2_1(xxx)
	KERNEL1x2_2(xxx)
	KERNEL1x2_3(xxx)
	KERNEL1x2_4(xxx)

	KERNEL1x2_1(xxx)
	KERNEL1x2_2(xxx)
	KERNEL1x2_3(xxx)
	KERNEL1x2_4(xxx)

	je	.L2_46

	KERNEL1x2_1(xxx)
	KERNEL1x2_2(xxx)
	KERNEL1x2_3(xxx)
	KERNEL1x2_4(xxx)

	KERNEL1x2_1(xxx)
	KERNEL1x2_2(xxx)
	KERNEL1x2_3(xxx)
	KERNEL1x2_4(xxx)

	je	.L2_46

	jmp	.L2_42
	ALIGN_4

.L2_46:
        movq    K, %rax

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

	KERNEL1x2_SUB(xxx)
	addq	$2, BI
	addq	$1, %rax
	jl	.L2_47
	ALIGN_4


.L2_49:

	vmovss	ALPHA, %xmm0

	vfmaddss 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddss 	 (CO1, LDC),%xmm0, %xmm5,%xmm5

	vmovss	%xmm4 ,  	(CO1)
	vmovss	%xmm5 ,  	(CO1, LDC)

	addq	$1 * SIZE, CO1		# coffset += 1
	ALIGN_4
	
.L2_60:

	decq	J			// j --
	jg	.L2_01			// next 2 lines of N



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

	
	movq	A, AO		 	// aoffset = a
	addq	$32 * SIZE, AO

	movq	M,  I
	sarq	$4, I			// i = (m >> 4)
	je	.L1_20

	ALIGN_4

.L1_11:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO

	vzeroall

        movq    K, %rax

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

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL16x1_1(xxx)
	KERNEL16x1_2(xxx)
	KERNEL16x1_3(xxx)
	KERNEL16x1_4(xxx)

	KERNEL16x1_1(xxx)
	KERNEL16x1_2(xxx)
	KERNEL16x1_3(xxx)
	KERNEL16x1_4(xxx)

	je	.L1_16

	KERNEL16x1_1(xxx)
	KERNEL16x1_2(xxx)
	KERNEL16x1_3(xxx)
	KERNEL16x1_4(xxx)

	KERNEL16x1_1(xxx)
	KERNEL16x1_2(xxx)
	KERNEL16x1_3(xxx)
	KERNEL16x1_4(xxx)

	je	.L1_16

	jmp	.L1_12
	ALIGN_4

.L1_16:
        movq    K, %rax

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

	KERNEL16x1_SUB(xxx)
	addq	$1, BI
	addq	$16, %rax
	jl	.L1_17
	ALIGN_4


.L1_19:

	vbroadcastss	ALPHA, %xmm0

	vfmaddps 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddps 4 * SIZE(CO1),%xmm0, %xmm7,%xmm7
	vfmaddps 8 * SIZE(CO1),%xmm0, %xmm10,%xmm10
	vfmaddps 12 * SIZE(CO1),%xmm0, %xmm13,%xmm13

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 4 * SIZE(CO1)
	vmovups	%xmm10, 8 * SIZE(CO1)
	vmovups	%xmm13,12 * SIZE(CO1)

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
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO

	vzeroall

        movq    K, %rax

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

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL8x1_1(xxx)
	KERNEL8x1_2(xxx)
	KERNEL8x1_3(xxx)
	KERNEL8x1_4(xxx)

	KERNEL8x1_1(xxx)
	KERNEL8x1_2(xxx)
	KERNEL8x1_3(xxx)
	KERNEL8x1_4(xxx)

	je	.L1_20_6

	KERNEL8x1_1(xxx)
	KERNEL8x1_2(xxx)
	KERNEL8x1_3(xxx)
	KERNEL8x1_4(xxx)

	KERNEL8x1_1(xxx)
	KERNEL8x1_2(xxx)
	KERNEL8x1_3(xxx)
	KERNEL8x1_4(xxx)

	je	.L1_20_6

	jmp	.L1_20_2
	ALIGN_4

.L1_20_6:
        movq    K, %rax

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

	KERNEL8x1_SUB(xxx)
	addq	$1, BI
	addq	$8, %rax
	jl	.L1_20_7
	ALIGN_4


.L1_20_9:

	vbroadcastss	ALPHA, %xmm0

	vfmaddps 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddps 4 * SIZE(CO1),%xmm0, %xmm7,%xmm7

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 4 * SIZE(CO1)

	addq	$8 * SIZE, CO1		# coffset += 8
	ALIGN_4
	


/**************************************************************************/

.L1_21pre:

	testq	$4, M		
	jz	.L1_30
	ALIGN_4

.L1_21:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO

	vzeroall

        movq    K, %rax

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

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL4x1_1(xxx)
	KERNEL4x1_2(xxx)
	KERNEL4x1_3(xxx)
	KERNEL4x1_4(xxx)

	KERNEL4x1_1(xxx)
	KERNEL4x1_2(xxx)
	KERNEL4x1_3(xxx)
	KERNEL4x1_4(xxx)

	je	.L1_26

	KERNEL4x1_1(xxx)
	KERNEL4x1_2(xxx)
	KERNEL4x1_3(xxx)
	KERNEL4x1_4(xxx)

	KERNEL4x1_1(xxx)
	KERNEL4x1_2(xxx)
	KERNEL4x1_3(xxx)
	KERNEL4x1_4(xxx)

	je	.L1_26

	jmp	.L1_22
	ALIGN_4

.L1_26:
        movq    K, %rax

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

	KERNEL4x1_SUB(xxx)
	addq	$1, BI
	addq	$4, %rax
	jl	.L1_27
	ALIGN_4


.L1_29:

	vbroadcastss	ALPHA, %xmm0

	vfmaddps 	 (CO1),%xmm0, %xmm4,%xmm4

	vmovups	%xmm4 ,  	(CO1)

	addq	$4 * SIZE, CO1		# coffset += 4
	ALIGN_4
	

.L1_30:
	testq	$2, M		
	jz	.L1_40

	ALIGN_4

.L1_31:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO

	vzeroall

        movq    K, %rax

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

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x1_1(xxx)
	KERNEL2x1_2(xxx)
	KERNEL2x1_3(xxx)
	KERNEL2x1_4(xxx)

	KERNEL2x1_1(xxx)
	KERNEL2x1_2(xxx)
	KERNEL2x1_3(xxx)
	KERNEL2x1_4(xxx)

	je	.L1_36

	KERNEL2x1_1(xxx)
	KERNEL2x1_2(xxx)
	KERNEL2x1_3(xxx)
	KERNEL2x1_4(xxx)

	KERNEL2x1_1(xxx)
	KERNEL2x1_2(xxx)
	KERNEL2x1_3(xxx)
	KERNEL2x1_4(xxx)

	je	.L1_36

	jmp	.L1_32
	ALIGN_4

.L1_36:
        movq    K, %rax

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

	KERNEL2x1_SUB(xxx)
	addq	$1, BI
	addq	$2, %rax
	jl	.L1_37
	ALIGN_4


.L1_39:

	vmovss	ALPHA, %xmm0

	vfmaddss 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddss 1 * SIZE(CO1),%xmm0, %xmm8,%xmm8

	vmovss	%xmm4 ,  	(CO1)
	vmovss	%xmm8 , 1 * SIZE(CO1)

	addq	$2 * SIZE, CO1		# coffset += 2
	ALIGN_4

.L1_40:
	testq	$1, M		
	jz	.L999

	ALIGN_4

.L1_41:
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO

	vzeroall

        movq    K, %rax

	andq	$-8, %rax
	je	.L1_46
	movq    %rax, BI                        //  Index for BO

	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_42:

	KERNEL1x1_1(xxx)
	KERNEL1x1_2(xxx)
	KERNEL1x1_3(xxx)
	KERNEL1x1_4(xxx)

	KERNEL1x1_1(xxx)
	KERNEL1x1_2(xxx)
	KERNEL1x1_3(xxx)
	KERNEL1x1_4(xxx)

	je	.L1_46

	KERNEL1x1_1(xxx)
	KERNEL1x1_2(xxx)
	KERNEL1x1_3(xxx)
	KERNEL1x1_4(xxx)

	KERNEL1x1_1(xxx)
	KERNEL1x1_2(xxx)
	KERNEL1x1_3(xxx)
	KERNEL1x1_4(xxx)

	je	.L1_46

	jmp	.L1_42
	ALIGN_4

.L1_46:
        movq    K, %rax

	andq	$7, %rax		# if (k & 1)
	je .L1_49

	movq    %rax, BI                        //  Index for BO

	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_47:

	KERNEL1x1_SUB(xxx)
	addq	$1, BI
	addq	$1, %rax
	jl	.L1_47
	ALIGN_4


.L1_49:

	vmovss	ALPHA, %xmm0

	vfmaddss 	 (CO1),%xmm0, %xmm4,%xmm4

	vmovss	%xmm4 ,  	(CO1)

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


#else
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
	movsd	OLD_OFFSET, %xmm12
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

	vmovsd	 %xmm0, ALPHA

	salq	$BASE_SHIFT, LDC

	movq    N, %rax
        xorq    %rdx, %rdx
        movq    $2,  %rdi
        divq    %rdi                    //    N / 6
        movq    %rax, Ndiv6             //    N / 6
        movq    %rdx, Nmod6             //    N % 6

	

#ifdef TRMMKERNEL
	vmovsd	%xmm12, OFFSET
	vmovsd	%xmm12, KK
#ifndef LEFT
	negq	KK
#endif	
#endif

	movq	Ndiv6,  J
	cmpq	$0, J
	je	.L1_0
	ALIGN_4

.L2_01:
	// copy to sub buffer
	movq	B, BO1
	leaq    BUFFER1, BO		// first buffer to BO
	movq	K, %rax
	ALIGN_4

.L2_02b:

	vmovsd	(BO1), %xmm0
	vmovsd	%xmm0,       (BO)
	addq	$2*SIZE,BO1
	addq	$2*SIZE,BO
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
	addq	$32 * SIZE, AO

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

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL16x2_1(xxx)
	KERNEL16x2_2(xxx)
	KERNEL16x2_3(xxx)
	KERNEL16x2_4(xxx)

	KERNEL16x2_1(xxx)
	KERNEL16x2_2(xxx)
	KERNEL16x2_3(xxx)
	KERNEL16x2_4(xxx)

	je	.L2_16

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL16x2_1(xxx)
	KERNEL16x2_2(xxx)
	KERNEL16x2_3(xxx)
	KERNEL16x2_4(xxx)

	KERNEL16x2_1(xxx)
	KERNEL16x2_2(xxx)
	KERNEL16x2_3(xxx)
	KERNEL16x2_4(xxx)

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

	KERNEL16x2_SUB(xxx)
	addq	$2, BI
	addq	$16, %rax
	jl	.L2_17
	ALIGN_4


.L2_19:

	vbroadcastss	ALPHA, %xmm0

#ifndef TRMMKERNEL

	vfmaddps 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddps 4 * SIZE(CO1),%xmm0, %xmm7,%xmm7
	vfmaddps 8 * SIZE(CO1),%xmm0, %xmm10,%xmm10
	vfmaddps 12 * SIZE(CO1),%xmm0, %xmm13,%xmm13

	vfmaddps 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddps 4 * SIZE(CO1, LDC),%xmm0, %xmm8,%xmm8
	vfmaddps 8 * SIZE(CO1, LDC),%xmm0, %xmm11,%xmm11
	vfmaddps 12 * SIZE(CO1, LDC),%xmm0, %xmm14,%xmm14

#else
	vmulps	%xmm0, %xmm4,%xmm4
	vmulps	%xmm0, %xmm7,%xmm7
	vmulps	%xmm0, %xmm10,%xmm10
	vmulps	%xmm0, %xmm13,%xmm13

	vmulps	%xmm0, %xmm5,%xmm5
	vmulps	%xmm0, %xmm8,%xmm8
	vmulps	%xmm0, %xmm11,%xmm11
	vmulps	%xmm0, %xmm14,%xmm14

#endif

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 4 * SIZE(CO1)
	vmovups	%xmm10, 8 * SIZE(CO1)
	vmovups	%xmm13,12 * SIZE(CO1)

	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm8 , 4 * SIZE(CO1, LDC)
	vmovups	%xmm11, 8 * SIZE(CO1, LDC)
	vmovups	%xmm14,12 * SIZE(CO1, LDC)


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
	jz	.L2_60		// to next 3 lines of N

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

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL8x2_1(xxx)
	KERNEL8x2_2(xxx)
	KERNEL8x2_3(xxx)
	KERNEL8x2_4(xxx)

	KERNEL8x2_1(xxx)
	KERNEL8x2_2(xxx)
	KERNEL8x2_3(xxx)
	KERNEL8x2_4(xxx)

	je	.L2_20_6

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL8x2_1(xxx)
	KERNEL8x2_2(xxx)
	KERNEL8x2_3(xxx)
	KERNEL8x2_4(xxx)

	KERNEL8x2_1(xxx)
	KERNEL8x2_2(xxx)
	KERNEL8x2_3(xxx)
	KERNEL8x2_4(xxx)

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

	KERNEL8x2_SUB(xxx)
	addq	$2, BI
	addq	$8, %rax
	jl	.L2_20_7
	ALIGN_4


.L2_20_9:

	vbroadcastss	ALPHA, %xmm0

#ifndef TRMMKERNEL

	vfmaddps 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddps 4 * SIZE(CO1),%xmm0, %xmm7,%xmm7

	vfmaddps 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddps 4 * SIZE(CO1, LDC),%xmm0, %xmm8,%xmm8

#else
	vmulps	%xmm0, %xmm4,%xmm4
	vmulps	%xmm0, %xmm7,%xmm7

	vmulps	%xmm0, %xmm5,%xmm5
	vmulps	%xmm0, %xmm8,%xmm8

#endif

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 4 * SIZE(CO1)

	vmovups	%xmm5 ,  	(CO1, LDC)
	vmovups	%xmm8 , 4 * SIZE(CO1, LDC)


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

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL4x2_1(xxx)
	KERNEL4x2_2(xxx)
	KERNEL4x2_3(xxx)
	KERNEL4x2_4(xxx)

	KERNEL4x2_1(xxx)
	KERNEL4x2_2(xxx)
	KERNEL4x2_3(xxx)
	KERNEL4x2_4(xxx)

	je	.L2_26

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL4x2_1(xxx)
	KERNEL4x2_2(xxx)
	KERNEL4x2_3(xxx)
	KERNEL4x2_4(xxx)

	KERNEL4x2_1(xxx)
	KERNEL4x2_2(xxx)
	KERNEL4x2_3(xxx)
	KERNEL4x2_4(xxx)

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

	KERNEL4x2_SUB(xxx)
	addq	$2, BI
	addq	$4, %rax
	jl	.L2_27
	ALIGN_4


.L2_29:

	vbroadcastss	ALPHA, %xmm0

#ifndef TRMMKERNEL

	vfmaddps 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddps 	 (CO1, LDC),%xmm0, %xmm5,%xmm5

#else
	vmulps	%xmm0, %xmm4,%xmm4
	vmulps	%xmm0, %xmm5,%xmm5

#endif

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm5 ,  	(CO1, LDC)


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

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_1(xxx)
	KERNEL2x2_2(xxx)
	KERNEL2x2_3(xxx)
	KERNEL2x2_4(xxx)

	KERNEL2x2_1(xxx)
	KERNEL2x2_2(xxx)
	KERNEL2x2_3(xxx)
	KERNEL2x2_4(xxx)

	je	.L2_36

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_1(xxx)
	KERNEL2x2_2(xxx)
	KERNEL2x2_3(xxx)
	KERNEL2x2_4(xxx)

	KERNEL2x2_1(xxx)
	KERNEL2x2_2(xxx)
	KERNEL2x2_3(xxx)
	KERNEL2x2_4(xxx)

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

	KERNEL2x2_SUB(xxx)
	addq	$2, BI
	addq	$2, %rax
	jl	.L2_37
	ALIGN_4


.L2_39:

	vmovss	ALPHA, %xmm0

#ifndef TRMMKERNEL

	vfmaddss 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddss 1 * SIZE(CO1),%xmm0, %xmm8,%xmm8
	vfmaddss 	 (CO1, LDC),%xmm0, %xmm5,%xmm5
	vfmaddss 1 * SIZE(CO1, LDC),%xmm0, %xmm10,%xmm10

#else
	vmulss	%xmm0, %xmm4,%xmm4
	vmulss	%xmm0, %xmm8,%xmm8
	vmulss	%xmm0, %xmm5,%xmm5
	vmulss	%xmm0, %xmm10,%xmm10

#endif

	vmovss	%xmm4 ,  	(CO1)
	vmovss	%xmm8 , 1 * SIZE(CO1)
	vmovss	%xmm5 ,  	(CO1, LDC)
	vmovss	%xmm10, 1 * SIZE(CO1, LDC)

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

	KERNEL1x2_1(xxx)
	KERNEL1x2_2(xxx)
	KERNEL1x2_3(xxx)
	KERNEL1x2_4(xxx)

	KERNEL1x2_1(xxx)
	KERNEL1x2_2(xxx)
	KERNEL1x2_3(xxx)
	KERNEL1x2_4(xxx)

	je	.L2_46

	KERNEL1x2_1(xxx)
	KERNEL1x2_2(xxx)
	KERNEL1x2_3(xxx)
	KERNEL1x2_4(xxx)

	KERNEL1x2_1(xxx)
	KERNEL1x2_2(xxx)
	KERNEL1x2_3(xxx)
	KERNEL1x2_4(xxx)

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

	KERNEL1x2_SUB(xxx)
	addq	$2, BI
	addq	$1, %rax
	jl	.L2_47
	ALIGN_4


.L2_49:

	vmovss	ALPHA, %xmm0

#ifndef TRMMKERNEL

	vfmaddss 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddss 	 (CO1, LDC),%xmm0, %xmm5,%xmm5

#else
	vmulss	%xmm0, %xmm4,%xmm4
	vmulss	%xmm0, %xmm5,%xmm5

#endif

	vmovss	%xmm4 ,  	(CO1)
	vmovss	%xmm5 ,  	(CO1, LDC)

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

	decq	J			// j --
	jg	.L2_01			// next 2 lines of N



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
	addq	$32 * SIZE, AO

	movq	M,  I
	sarq	$4, I			// i = (m >> 4)
	je	.L1_20

	ALIGN_4

.L1_11:
#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO
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

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL16x1_1(xxx)
	KERNEL16x1_2(xxx)
	KERNEL16x1_3(xxx)
	KERNEL16x1_4(xxx)

	KERNEL16x1_1(xxx)
	KERNEL16x1_2(xxx)
	KERNEL16x1_3(xxx)
	KERNEL16x1_4(xxx)

	je	.L1_16

	KERNEL16x1_1(xxx)
	KERNEL16x1_2(xxx)
	KERNEL16x1_3(xxx)
	KERNEL16x1_4(xxx)

	KERNEL16x1_1(xxx)
	KERNEL16x1_2(xxx)
	KERNEL16x1_3(xxx)
	KERNEL16x1_4(xxx)

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

	KERNEL16x1_SUB(xxx)
	addq	$1, BI
	addq	$16, %rax
	jl	.L1_17
	ALIGN_4


.L1_19:

	vbroadcastss	ALPHA, %xmm0

#ifndef TRMMKERNEL

	vfmaddps 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddps 4 * SIZE(CO1),%xmm0, %xmm7,%xmm7
	vfmaddps 8 * SIZE(CO1),%xmm0, %xmm10,%xmm10
	vfmaddps 12 * SIZE(CO1),%xmm0, %xmm13,%xmm13

#else
	vmulps	%xmm0, %xmm4,%xmm4
	vmulps	%xmm0, %xmm7,%xmm7
	vmulps	%xmm0, %xmm10,%xmm10
	vmulps	%xmm0, %xmm13,%xmm13

#endif

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 4 * SIZE(CO1)
	vmovups	%xmm10, 8 * SIZE(CO1)
	vmovups	%xmm13,12 * SIZE(CO1)


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
        addq    $2 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO
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

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL8x1_1(xxx)
	KERNEL8x1_2(xxx)
	KERNEL8x1_3(xxx)
	KERNEL8x1_4(xxx)

	KERNEL8x1_1(xxx)
	KERNEL8x1_2(xxx)
	KERNEL8x1_3(xxx)
	KERNEL8x1_4(xxx)

	je	.L1_20_6

	KERNEL8x1_1(xxx)
	KERNEL8x1_2(xxx)
	KERNEL8x1_3(xxx)
	KERNEL8x1_4(xxx)

	KERNEL8x1_1(xxx)
	KERNEL8x1_2(xxx)
	KERNEL8x1_3(xxx)
	KERNEL8x1_4(xxx)

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

	KERNEL8x1_SUB(xxx)
	addq	$1, BI
	addq	$8, %rax
	jl	.L1_20_7
	ALIGN_4


.L1_20_9:

	vbroadcastss	ALPHA, %xmm0

#ifndef TRMMKERNEL

	vfmaddps 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddps 4 * SIZE(CO1),%xmm0, %xmm7,%xmm7

#else
	vmulps	%xmm0, %xmm4,%xmm4
	vmulps	%xmm0, %xmm7,%xmm7

#endif

	vmovups	%xmm4 ,  	(CO1)
	vmovups	%xmm7 , 4 * SIZE(CO1)

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
        addq    $2 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO
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

	prefetcht0	B_PR1(BO,BI, SIZE)
	KERNEL4x1_1(xxx)
	KERNEL4x1_2(xxx)
	KERNEL4x1_3(xxx)
	KERNEL4x1_4(xxx)

	KERNEL4x1_1(xxx)
	KERNEL4x1_2(xxx)
	KERNEL4x1_3(xxx)
	KERNEL4x1_4(xxx)

	je	.L1_26

	KERNEL4x1_1(xxx)
	KERNEL4x1_2(xxx)
	KERNEL4x1_3(xxx)
	KERNEL4x1_4(xxx)

	KERNEL4x1_1(xxx)
	KERNEL4x1_2(xxx)
	KERNEL4x1_3(xxx)
	KERNEL4x1_4(xxx)

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

	KERNEL4x1_SUB(xxx)
	addq	$1, BI
	addq	$4, %rax
	jl	.L1_27
	ALIGN_4


.L1_29:

	vbroadcastss	ALPHA, %xmm0

#ifndef TRMMKERNEL

	vfmaddps 	 (CO1),%xmm0, %xmm4,%xmm4

#else
	vmulps	%xmm0, %xmm4,%xmm4

#endif

	vmovups	%xmm4 ,  	(CO1)


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
        addq    $2 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO
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

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x1_1(xxx)
	KERNEL2x1_2(xxx)
	KERNEL2x1_3(xxx)
	KERNEL2x1_4(xxx)

	KERNEL2x1_1(xxx)
	KERNEL2x1_2(xxx)
	KERNEL2x1_3(xxx)
	KERNEL2x1_4(xxx)

	je	.L1_36

	KERNEL2x1_1(xxx)
	KERNEL2x1_2(xxx)
	KERNEL2x1_3(xxx)
	KERNEL2x1_4(xxx)

	KERNEL2x1_1(xxx)
	KERNEL2x1_2(xxx)
	KERNEL2x1_3(xxx)
	KERNEL2x1_4(xxx)

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

	KERNEL2x1_SUB(xxx)
	addq	$1, BI
	addq	$2, %rax
	jl	.L1_37
	ALIGN_4


.L1_39:

	vmovss	ALPHA, %xmm0

#ifndef TRMMKERNEL

	vfmaddss 	 (CO1),%xmm0, %xmm4,%xmm4
	vfmaddss 1 * SIZE(CO1),%xmm0, %xmm8,%xmm8

#else
	vmulss	%xmm0, %xmm4,%xmm4
	vmulss	%xmm0, %xmm8,%xmm8

#endif

	vmovss	%xmm4 ,  	(CO1)
	vmovss	%xmm8 , 1 * SIZE(CO1)

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
        addq    $2 * SIZE, BO
#else
        movq    KK, %rax
        leaq    BUFFER1, BO             // first buffer to BO
        addq    $2 * SIZE, BO
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

	KERNEL1x1_1(xxx)
	KERNEL1x1_2(xxx)
	KERNEL1x1_3(xxx)
	KERNEL1x1_4(xxx)

	KERNEL1x1_1(xxx)
	KERNEL1x1_2(xxx)
	KERNEL1x1_3(xxx)
	KERNEL1x1_4(xxx)

	je	.L1_46

	KERNEL1x1_1(xxx)
	KERNEL1x1_2(xxx)
	KERNEL1x1_3(xxx)
	KERNEL1x1_4(xxx)

	KERNEL1x1_1(xxx)
	KERNEL1x1_2(xxx)
	KERNEL1x1_3(xxx)
	KERNEL1x1_4(xxx)

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

	KERNEL1x1_SUB(xxx)
	addq	$1, BI
	addq	$1, %rax
	jl	.L1_47
	ALIGN_4


.L1_49:

	vmovss	ALPHA, %xmm0

#ifndef TRMMKERNEL

	vfmaddss 	 (CO1),%xmm0, %xmm4,%xmm4

#else
	vmulss	%xmm0, %xmm4,%xmm4

#endif

	vmovss	%xmm4 ,  	(CO1)

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





#endif
