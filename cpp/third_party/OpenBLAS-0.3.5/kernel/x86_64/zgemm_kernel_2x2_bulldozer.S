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


#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define VFMADD_R    vfmaddpd
#define VFMADD_I    vfmaddpd
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define VFMADD_R    vfnmaddpd
#define VFMADD_I    vfmaddpd
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define VFMADD_R    vfmaddpd
#define VFMADD_I    vfnmaddpd
#else
#define VFMADD_R    vfnmaddpd
#define VFMADD_I    vfnmaddpd
#endif


#define	A_PR1	384
#define	B_PR1	192

#define KERNEL2x2_1(xx) \
        prefetcht0      A_PR1(AO,%rax,SIZE)        ;\
        vmovups          -8 * SIZE(AO, %rax, SIZE), %xmm0 ;\
        vmovddup         -8 * SIZE(BO, BI, SIZE), %xmm4 ;\
        VFMADD_R        %xmm8,%xmm4,%xmm0,%xmm8 ;\
        vmovups          -6 * SIZE(AO, %rax, SIZE), %xmm1 ;\
        VFMADD_R        %xmm12,%xmm4,%xmm1,%xmm12 ;\
        vmovddup         -7 * SIZE(BO, BI, SIZE), %xmm5 ;\
        VFMADD_I        %xmm9,%xmm5,%xmm0,%xmm9 ;\
        VFMADD_I        %xmm13,%xmm5,%xmm1,%xmm13 ;\
        vmovddup         -6 * SIZE(BO, BI, SIZE), %xmm6 ;\
        VFMADD_R        %xmm10,%xmm6,%xmm0,%xmm10 ;\
        VFMADD_R        %xmm14,%xmm6,%xmm1,%xmm14 ;\
        vmovddup         -5 * SIZE(BO, BI, SIZE), %xmm7 ;\
        VFMADD_I        %xmm11,%xmm7,%xmm0,%xmm11 ;\
        VFMADD_I        %xmm15,%xmm7,%xmm1,%xmm15 ;\

#define KERNEL2x2_2(xx) \
        vmovups          -4 * SIZE(AO, %rax, SIZE), %xmm0 ;\
        vmovddup         -4 * SIZE(BO, BI, SIZE), %xmm4 ;\
        VFMADD_R        %xmm8,%xmm4,%xmm0,%xmm8 ;\
        vmovups          -2 * SIZE(AO, %rax, SIZE), %xmm1 ;\
        VFMADD_R        %xmm12,%xmm4,%xmm1,%xmm12 ;\
        vmovddup         -3 * SIZE(BO, BI, SIZE), %xmm5 ;\
        VFMADD_I        %xmm9,%xmm5,%xmm0,%xmm9 ;\
        VFMADD_I        %xmm13,%xmm5,%xmm1,%xmm13 ;\
        vmovddup         -2 * SIZE(BO, BI, SIZE), %xmm6 ;\
        VFMADD_R        %xmm10,%xmm6,%xmm0,%xmm10 ;\
        VFMADD_R        %xmm14,%xmm6,%xmm1,%xmm14 ;\
        vmovddup         -1 * SIZE(BO, BI, SIZE), %xmm7 ;\
        VFMADD_I        %xmm11,%xmm7,%xmm0,%xmm11 ;\
        VFMADD_I        %xmm15,%xmm7,%xmm1,%xmm15 ;\

#define KERNEL2x2_3(xx) \
	prefetcht0      A_PR1+64(AO,%rax,SIZE)     ;\
        vmovups           0 * SIZE(AO, %rax, SIZE), %xmm0 ;\
        vmovddup          0 * SIZE(BO, BI, SIZE), %xmm4 ;\
        VFMADD_R        %xmm8,%xmm4,%xmm0,%xmm8 ;\
        vmovups           2 * SIZE(AO, %rax, SIZE), %xmm1 ;\
        VFMADD_R        %xmm12,%xmm4,%xmm1,%xmm12 ;\
        vmovddup          1 * SIZE(BO, BI, SIZE), %xmm5 ;\
        VFMADD_I        %xmm9,%xmm5,%xmm0,%xmm9 ;\
        VFMADD_I        %xmm13,%xmm5,%xmm1,%xmm13 ;\
        vmovddup          2 * SIZE(BO, BI, SIZE), %xmm6 ;\
        VFMADD_R        %xmm10,%xmm6,%xmm0,%xmm10 ;\
        VFMADD_R        %xmm14,%xmm6,%xmm1,%xmm14 ;\
        vmovddup          3 * SIZE(BO, BI, SIZE), %xmm7 ;\
        VFMADD_I        %xmm11,%xmm7,%xmm0,%xmm11 ;\
        VFMADD_I        %xmm15,%xmm7,%xmm1,%xmm15 ;\

#define KERNEL2x2_4(xx) \
        vmovups           4 * SIZE(AO, %rax, SIZE), %xmm0 ;\
        vmovddup          4 * SIZE(BO, BI, SIZE), %xmm4 ;\
        VFMADD_R        %xmm8,%xmm4,%xmm0,%xmm8 ;\
        vmovups           6 * SIZE(AO, %rax, SIZE), %xmm1 ;\
        VFMADD_R        %xmm12,%xmm4,%xmm1,%xmm12 ;\
        vmovddup          5 * SIZE(BO, BI, SIZE), %xmm5 ;\
        VFMADD_I        %xmm9,%xmm5,%xmm0,%xmm9 ;\
        VFMADD_I        %xmm13,%xmm5,%xmm1,%xmm13 ;\
        vmovddup          6 * SIZE(BO, BI, SIZE), %xmm6 ;\
        VFMADD_R        %xmm10,%xmm6,%xmm0,%xmm10 ;\
        VFMADD_R        %xmm14,%xmm6,%xmm1,%xmm14 ;\
        vmovddup          7 * SIZE(BO, BI, SIZE), %xmm7 ;\
        VFMADD_I        %xmm11,%xmm7,%xmm0,%xmm11 ;\
        VFMADD_I        %xmm15,%xmm7,%xmm1,%xmm15 ;\
        addq    $16, BI                            ;\
        addq    $16, %rax                          ;\


#define KERNEL2x2_SUB(xx) \
        vmovups          -8 * SIZE(AO, %rax, SIZE), %xmm0 ;\
        vmovddup         -8 * SIZE(BO, BI, SIZE), %xmm4 ;\
        VFMADD_R        %xmm8,%xmm4,%xmm0,%xmm8 ;\
        vmovups          -6 * SIZE(AO, %rax, SIZE), %xmm1 ;\
        VFMADD_R        %xmm12,%xmm4,%xmm1,%xmm12 ;\
        vmovddup         -7 * SIZE(BO, BI, SIZE), %xmm5 ;\
        VFMADD_I        %xmm9,%xmm5,%xmm0,%xmm9 ;\
        VFMADD_I        %xmm13,%xmm5,%xmm1,%xmm13 ;\
        vmovddup         -6 * SIZE(BO, BI, SIZE), %xmm6 ;\
        VFMADD_R        %xmm10,%xmm6,%xmm0,%xmm10 ;\
        VFMADD_R        %xmm14,%xmm6,%xmm1,%xmm14 ;\
        vmovddup         -5 * SIZE(BO, BI, SIZE), %xmm7 ;\
        VFMADD_I        %xmm11,%xmm7,%xmm0,%xmm11 ;\
        VFMADD_I        %xmm15,%xmm7,%xmm1,%xmm15 ;\
        addq    $4, BI                            ;\
        addq    $4, %rax                          ;\

/************************************************************************************************/

#define KERNEL1x2_1(xx) \
        prefetcht0      A_PR1(AO,%rax,SIZE)        ;\
        vmovups          -8 * SIZE(AO, %rax, SIZE), %xmm0 ;\
        vmovddup         -8 * SIZE(BO, BI, SIZE), %xmm4 ;\
        VFMADD_R        %xmm8,%xmm4,%xmm0,%xmm8 ;\
        vmovddup         -7 * SIZE(BO, BI, SIZE), %xmm5 ;\
        VFMADD_I        %xmm9,%xmm5,%xmm0,%xmm9 ;\
        vmovddup         -6 * SIZE(BO, BI, SIZE), %xmm6 ;\
        VFMADD_R        %xmm10,%xmm6,%xmm0,%xmm10 ;\
        vmovddup         -5 * SIZE(BO, BI, SIZE), %xmm7 ;\
        VFMADD_I        %xmm11,%xmm7,%xmm0,%xmm11 ;\

#define KERNEL1x2_2(xx) \
        vmovups          -6 * SIZE(AO, %rax, SIZE), %xmm0 ;\
        vmovddup         -4 * SIZE(BO, BI, SIZE), %xmm4 ;\
        VFMADD_R        %xmm8,%xmm4,%xmm0,%xmm8 ;\
        VFMADD_R        %xmm12,%xmm4,%xmm1,%xmm12 ;\
        vmovddup         -3 * SIZE(BO, BI, SIZE), %xmm5 ;\
        VFMADD_I        %xmm9,%xmm5,%xmm0,%xmm9 ;\
        vmovddup         -2 * SIZE(BO, BI, SIZE), %xmm6 ;\
        VFMADD_R        %xmm10,%xmm6,%xmm0,%xmm10 ;\
        vmovddup         -1 * SIZE(BO, BI, SIZE), %xmm7 ;\
        VFMADD_I        %xmm11,%xmm7,%xmm0,%xmm11 ;\

#define KERNEL1x2_3(xx) \
        vmovups          -4 * SIZE(AO, %rax, SIZE), %xmm0 ;\
        vmovddup          0 * SIZE(BO, BI, SIZE), %xmm4 ;\
        VFMADD_R        %xmm8,%xmm4,%xmm0,%xmm8 ;\
        vmovddup          1 * SIZE(BO, BI, SIZE), %xmm5 ;\
        VFMADD_I        %xmm9,%xmm5,%xmm0,%xmm9 ;\
        vmovddup          2 * SIZE(BO, BI, SIZE), %xmm6 ;\
        VFMADD_R        %xmm10,%xmm6,%xmm0,%xmm10 ;\
        vmovddup          3 * SIZE(BO, BI, SIZE), %xmm7 ;\
        VFMADD_I        %xmm11,%xmm7,%xmm0,%xmm11 ;\

#define KERNEL1x2_4(xx) \
        vmovups          -2 * SIZE(AO, %rax, SIZE), %xmm0 ;\
        vmovddup          4 * SIZE(BO, BI, SIZE), %xmm4 ;\
        VFMADD_R        %xmm8,%xmm4,%xmm0,%xmm8 ;\
        vmovddup          5 * SIZE(BO, BI, SIZE), %xmm5 ;\
        VFMADD_I        %xmm9,%xmm5,%xmm0,%xmm9 ;\
        vmovddup          6 * SIZE(BO, BI, SIZE), %xmm6 ;\
        VFMADD_R        %xmm10,%xmm6,%xmm0,%xmm10 ;\
        vmovddup          7 * SIZE(BO, BI, SIZE), %xmm7 ;\
        VFMADD_I        %xmm11,%xmm7,%xmm0,%xmm11 ;\
        addq    $16, BI                            ;\
        addq    $8 , %rax                          ;\


#define KERNEL1x2_SUB(xx) \
        vmovups          -8 * SIZE(AO, %rax, SIZE), %xmm0 ;\
        vmovddup         -8 * SIZE(BO, BI, SIZE), %xmm4 ;\
        VFMADD_R        %xmm8,%xmm4,%xmm0,%xmm8 ;\
        vmovddup         -7 * SIZE(BO, BI, SIZE), %xmm5 ;\
        VFMADD_I        %xmm9,%xmm5,%xmm0,%xmm9 ;\
        vmovddup         -6 * SIZE(BO, BI, SIZE), %xmm6 ;\
        VFMADD_R        %xmm10,%xmm6,%xmm0,%xmm10 ;\
        vmovddup         -5 * SIZE(BO, BI, SIZE), %xmm7 ;\
        VFMADD_I        %xmm11,%xmm7,%xmm0,%xmm11 ;\
        addq    $4, BI                            ;\
        addq    $2, %rax                          ;\

/************************************************************************************************/

#define KERNEL2x1_1(xx) \
        prefetcht0      A_PR1(AO,%rax,SIZE)        ;\
        vmovups          -8 * SIZE(AO, %rax, SIZE), %xmm0 ;\
        vmovddup         -4 * SIZE(BO, BI, SIZE), %xmm4 ;\
        VFMADD_R        %xmm8,%xmm4,%xmm0,%xmm8 ;\
        vmovups          -6 * SIZE(AO, %rax, SIZE), %xmm1 ;\
        VFMADD_R        %xmm12,%xmm4,%xmm1,%xmm12 ;\
        vmovddup         -3 * SIZE(BO, BI, SIZE), %xmm5 ;\
        VFMADD_I        %xmm9,%xmm5,%xmm0,%xmm9 ;\
        VFMADD_I        %xmm13,%xmm5,%xmm1,%xmm13 ;\

#define KERNEL2x1_2(xx) \
        vmovups          -4 * SIZE(AO, %rax, SIZE), %xmm0 ;\
        vmovddup         -2 * SIZE(BO, BI, SIZE), %xmm4 ;\
        VFMADD_R        %xmm8,%xmm4,%xmm0,%xmm8 ;\
        vmovups          -2 * SIZE(AO, %rax, SIZE), %xmm1 ;\
        VFMADD_R        %xmm12,%xmm4,%xmm1,%xmm12 ;\
        vmovddup         -1 * SIZE(BO, BI, SIZE), %xmm5 ;\
        VFMADD_I        %xmm9,%xmm5,%xmm0,%xmm9 ;\
        VFMADD_I        %xmm13,%xmm5,%xmm1,%xmm13 ;\

#define KERNEL2x1_3(xx) \
	prefetcht0      A_PR1+64(AO,%rax,SIZE)     ;\
        vmovups           0 * SIZE(AO, %rax, SIZE), %xmm0 ;\
        vmovddup          0 * SIZE(BO, BI, SIZE), %xmm4 ;\
        VFMADD_R        %xmm8,%xmm4,%xmm0,%xmm8 ;\
        vmovups           2 * SIZE(AO, %rax, SIZE), %xmm1 ;\
        VFMADD_R        %xmm12,%xmm4,%xmm1,%xmm12 ;\
        vmovddup          1 * SIZE(BO, BI, SIZE), %xmm5 ;\
        VFMADD_I        %xmm9,%xmm5,%xmm0,%xmm9 ;\
        VFMADD_I        %xmm13,%xmm5,%xmm1,%xmm13 ;\

#define KERNEL2x1_4(xx) \
        vmovups           4 * SIZE(AO, %rax, SIZE), %xmm0 ;\
        vmovddup          2 * SIZE(BO, BI, SIZE), %xmm4 ;\
        VFMADD_R        %xmm8,%xmm4,%xmm0,%xmm8 ;\
        vmovups           6 * SIZE(AO, %rax, SIZE), %xmm1 ;\
        VFMADD_R        %xmm12,%xmm4,%xmm1,%xmm12 ;\
        vmovddup          3 * SIZE(BO, BI, SIZE), %xmm5 ;\
        VFMADD_I        %xmm9,%xmm5,%xmm0,%xmm9 ;\
        VFMADD_I        %xmm13,%xmm5,%xmm1,%xmm13 ;\
        addq    $8, BI                            ;\
        addq    $16, %rax                          ;\


#define KERNEL2x1_SUB(xx) \
        vmovups          -8 * SIZE(AO, %rax, SIZE), %xmm0 ;\
        vmovddup         -4 * SIZE(BO, BI, SIZE), %xmm4 ;\
        VFMADD_R        %xmm8,%xmm4,%xmm0,%xmm8 ;\
        vmovups          -6 * SIZE(AO, %rax, SIZE), %xmm1 ;\
        VFMADD_R        %xmm12,%xmm4,%xmm1,%xmm12 ;\
        vmovddup         -3 * SIZE(BO, BI, SIZE), %xmm5 ;\
        VFMADD_I        %xmm9,%xmm5,%xmm0,%xmm9 ;\
        VFMADD_I        %xmm13,%xmm5,%xmm1,%xmm13 ;\
        addq    $2, BI                            ;\
        addq    $4, %rax                          ;\


/************************************************************************************************/

#define KERNEL1x1_1(xx) \
        prefetcht0      A_PR1(AO,%rax,SIZE)        ;\
        vmovups          -8 * SIZE(AO, %rax, SIZE), %xmm0 ;\
        vmovddup         -4 * SIZE(BO, BI, SIZE), %xmm4 ;\
        VFMADD_R        %xmm8,%xmm4,%xmm0,%xmm8 ;\
        vmovddup         -3 * SIZE(BO, BI, SIZE), %xmm5 ;\
        VFMADD_I        %xmm9,%xmm5,%xmm0,%xmm9 ;\

#define KERNEL1x1_2(xx) \
        vmovups          -6 * SIZE(AO, %rax, SIZE), %xmm0 ;\
        vmovddup         -2 * SIZE(BO, BI, SIZE), %xmm4 ;\
        VFMADD_R        %xmm8,%xmm4,%xmm0,%xmm8 ;\
        vmovddup         -1 * SIZE(BO, BI, SIZE), %xmm5 ;\
        VFMADD_I        %xmm9,%xmm5,%xmm0,%xmm9 ;\

#define KERNEL1x1_3(xx) \
        vmovups          -4 * SIZE(AO, %rax, SIZE), %xmm0 ;\
        vmovddup          0 * SIZE(BO, BI, SIZE), %xmm4 ;\
        VFMADD_R        %xmm8,%xmm4,%xmm0,%xmm8 ;\
        vmovddup          1 * SIZE(BO, BI, SIZE), %xmm5 ;\
        VFMADD_I        %xmm9,%xmm5,%xmm0,%xmm9 ;\

#define KERNEL1x1_4(xx) \
        vmovups          -2 * SIZE(AO, %rax, SIZE), %xmm0 ;\
        vmovddup          2 * SIZE(BO, BI, SIZE), %xmm4 ;\
        VFMADD_R        %xmm8,%xmm4,%xmm0,%xmm8 ;\
        vmovddup          3 * SIZE(BO, BI, SIZE), %xmm5 ;\
        VFMADD_I        %xmm9,%xmm5,%xmm0,%xmm9 ;\
        addq    $8, BI                            ;\
        addq    $8, %rax                          ;\


#define KERNEL1x1_SUB(xx) \
        vmovups          -8 * SIZE(AO, %rax, SIZE), %xmm0 ;\
        vmovddup         -4 * SIZE(BO, BI, SIZE), %xmm4 ;\
        VFMADD_R        %xmm8,%xmm4,%xmm0,%xmm8 ;\
        vmovddup         -3 * SIZE(BO, BI, SIZE), %xmm5 ;\
        VFMADD_I        %xmm9,%xmm5,%xmm0,%xmm9 ;\
        addq    $2, BI                            ;\
        addq    $2, %rax                          ;\


/************************************************************************************************/




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
	vmovsd	OLD_OFFSET, %xmm12
#endif
	vmovaps	%xmm3, %xmm0
	vmovsd   OLD_ALPHA_I, %xmm1

#else
	movq	STACKSIZE +  8(%rsp), LDC
#ifdef TRMMKERNEL
	vmovsd	STACKSIZE + 16(%rsp), %xmm12
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

	vmovsd	 %xmm0, ALPHA_R
	vmovsd	 %xmm1, ALPHA_I

	salq	$ZBASE_SHIFT, LDC

	movq    N, %rax
        xorq    %rdx, %rdx
        movq    $2,  %rdi
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

	vmovups		(BO1), %xmm0
	vmovups	2 * SIZE(BO1), %xmm1
	vmovups	%xmm0,       (BO)
	vmovups	%xmm1, 2 * SIZE(BO)
	addq	$4*SIZE,BO1
	addq	$4*SIZE,BO
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
	addq	$8 * SIZE, AO

	movq	M,  I
	sarq	$1, I			// i = (m >> 1)
	je	.L2_40

	ALIGN_4

.L2_11:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leaq	BUFFER1, BO		// first buffer to BO
	addq	$8 * SIZE, BO
#else
        movq    KK, %rax
	leaq	BUFFER1, BO			// first buffer to BO
	addq	$8 * SIZE, BO
	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,4), BI                     //  BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$2, %rax			// rax = rax * 4 ; number of values
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


	andq	$-8, %rax			//  K = K - ( K % 8 )
	je	.L2_16
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_12:

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_1(xxx)
	KERNEL2x2_2(xxx)
	prefetcht0	B_PR1+64(BO,BI,SIZE)
	KERNEL2x2_3(xxx)
	KERNEL2x2_4(xxx)

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_1(xxx)
	KERNEL2x2_2(xxx)
	prefetcht0	B_PR1+64(BO,BI,SIZE)
	KERNEL2x2_3(xxx)
	KERNEL2x2_4(xxx)

	je	.L2_16

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_1(xxx)
	KERNEL2x2_2(xxx)
	prefetcht0	B_PR1+64(BO,BI,SIZE)
	KERNEL2x2_3(xxx)
	KERNEL2x2_4(xxx)

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x2_1(xxx)
	KERNEL2x2_2(xxx)
	prefetcht0	B_PR1+64(BO,BI,SIZE)
	KERNEL2x2_3(xxx)
	KERNEL2x2_4(xxx)

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
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_17:

	KERNEL2x2_SUB(xxx)
	jl	.L2_17
	ALIGN_4


.L2_19:

	vmovddup	ALPHA_R, %xmm0
	vmovddup	ALPHA_I, %xmm1

	// swap high and low 64 bytes
        vshufpd $0x01, %xmm9 , %xmm9, %xmm9
        vshufpd $0x01, %xmm11, %xmm11, %xmm11
        vshufpd $0x01, %xmm13, %xmm13, %xmm13
        vshufpd $0x01, %xmm15, %xmm15, %xmm15

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubpd %xmm9, %xmm8 , %xmm8
        vaddsubpd %xmm11,%xmm10, %xmm10
        vaddsubpd %xmm13,%xmm12, %xmm12
        vaddsubpd %xmm15,%xmm14, %xmm14

        vshufpd $0x01, %xmm8 , %xmm8, %xmm9
        vshufpd $0x01, %xmm10, %xmm10, %xmm11
        vshufpd $0x01, %xmm12, %xmm12, %xmm13
        vshufpd $0x01, %xmm14, %xmm14, %xmm15

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
        vshufpd $0x01, %xmm9 , %xmm9, %xmm9
        vshufpd $0x01, %xmm11, %xmm11, %xmm11
        vshufpd $0x01, %xmm13, %xmm13, %xmm13
        vshufpd $0x01, %xmm15, %xmm15, %xmm15

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

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$2, %rax			// rax = rax * 4 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $2, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	decq	I			# i --
	jg	.L2_11
	ALIGN_4	


/**************************************************************************
* Rest of M 
***************************************************************************/
.L2_40:
	testq	$1, M		
	jz	.L2_60		// to next 2 lines of N

	ALIGN_4

.L2_41:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leaq	BUFFER1, BO		// first buffer to BO
	addq	$8 * SIZE, BO
#else
        movq    KK, %rax
	leaq	BUFFER1, BO			// first buffer to BO
	addq	$8 * SIZE, BO
	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,4), BI                     //  BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$1, %rax			// rax = rax * 2 ; number of values
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


	andq	$-8, %rax			//  K = K - ( K % 8 )
	je	.L2_46
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values

	salq	$1, %rax			// rax = rax * 2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_42:

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x2_1(xxx)
	KERNEL1x2_2(xxx)
	prefetcht0	B_PR1+64(BO,BI,SIZE)
	KERNEL1x2_3(xxx)
	KERNEL1x2_4(xxx)

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x2_1(xxx)
	KERNEL1x2_2(xxx)
	prefetcht0	B_PR1+64(BO,BI,SIZE)
	KERNEL1x2_3(xxx)
	KERNEL1x2_4(xxx)

	je	.L2_46

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x2_1(xxx)
	KERNEL1x2_2(xxx)
	prefetcht0	B_PR1+64(BO,BI,SIZE)
	KERNEL1x2_3(xxx)
	KERNEL1x2_4(xxx)

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x2_1(xxx)
	KERNEL1x2_2(xxx)
	prefetcht0	B_PR1+64(BO,BI,SIZE)
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
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values

	salq	$1, %rax			// rax = rax * 2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L2_47:

	KERNEL1x2_SUB(xxx)
	jl	.L2_47
	ALIGN_4


.L2_49:

	vmovddup	ALPHA_R, %xmm0
	vmovddup	ALPHA_I, %xmm1

	// swap high and low 64 bytes
        vshufpd $0x01, %xmm9 , %xmm9, %xmm9
        vshufpd $0x01, %xmm11, %xmm11, %xmm11

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubpd %xmm9, %xmm8 , %xmm8
        vaddsubpd %xmm11,%xmm10, %xmm10

        vshufpd $0x01, %xmm8 , %xmm8, %xmm9
        vshufpd $0x01, %xmm10, %xmm10, %xmm11

#else
        vaddsubpd %xmm8, %xmm9, %xmm9
        vaddsubpd %xmm10,%xmm11, %xmm11

        vmovapd   %xmm9,  %xmm8
        vmovapd   %xmm11, %xmm10

	// swap high and low 64 bytes
        vshufpd $0x01, %xmm9 , %xmm9, %xmm9
        vshufpd $0x01, %xmm11, %xmm11, %xmm11

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

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,4), BI                    //  BI = BI * 4 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$1, %rax			// rax = rax * 2 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $1, KK
#endif

	addq	$2 * SIZE, CO1		# coffset += 2
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

	vmovups		(BO1), %xmm0
	vmovups	%xmm0,       (BO)
	addq	$2*SIZE,BO1
	addq	$2*SIZE,BO
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
	addq	$8 * SIZE, AO

	movq	M,  I
	sarq	$1, I			// i = (m >> 1)
	je	.L1_40

	ALIGN_4

.L1_11:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leaq	BUFFER1, BO		// first buffer to BO
	addq	$4 * SIZE, BO
#else
        movq    KK, %rax
	leaq	BUFFER1, BO			// first buffer to BO
	addq	$4 * SIZE, BO
	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,2), BI                     //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$2, %rax			// rax = rax * 4 ; number of values
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


	andq	$-8, %rax			//  K = K - ( K % 8 )
	je	.L1_16
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_12:

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x1_1(xxx)
	KERNEL2x1_2(xxx)
	KERNEL2x1_3(xxx)
	KERNEL2x1_4(xxx)

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x1_1(xxx)
	KERNEL2x1_2(xxx)
	KERNEL2x1_3(xxx)
	KERNEL2x1_4(xxx)

	je	.L1_16

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x1_1(xxx)
	KERNEL2x1_2(xxx)
	KERNEL2x1_3(xxx)
	KERNEL2x1_4(xxx)

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL2x1_1(xxx)
	KERNEL2x1_2(xxx)
	KERNEL2x1_3(xxx)
	KERNEL2x1_4(xxx)

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
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values

	salq	$2, %rax			// rax = rax * 4 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_17:

	KERNEL2x1_SUB(xxx)
	jl	.L1_17
	ALIGN_4


.L1_19:

	vmovddup	ALPHA_R, %xmm0
	vmovddup	ALPHA_I, %xmm1

	// swap high and low 64 bytes
        vshufpd $0x01, %xmm9 , %xmm9, %xmm9
        vshufpd $0x01, %xmm13, %xmm13, %xmm13

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubpd %xmm9, %xmm8  , %xmm8
        vaddsubpd %xmm13,%xmm12 , %xmm12

        vshufpd $0x01, %xmm8 , %xmm8, %xmm9
        vshufpd $0x01, %xmm12, %xmm12, %xmm13

#else
        vaddsubpd %xmm8, %xmm9 , %xmm9
        vaddsubpd %xmm12,%xmm13, %xmm13

        vmovapd   %xmm9,  %xmm8
        vmovapd   %xmm13, %xmm12

	// swap high and low 64 bytes
        vshufpd $0x01, %xmm9 , %xmm9, %xmm9
        vshufpd $0x01, %xmm13, %xmm13, %xmm13

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


#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$2, %rax			// rax = rax * 4 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $2, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	decq	I			# i --
	jg	.L1_11
	ALIGN_4	


/**************************************************************************
* Rest of M 
***************************************************************************/
.L1_40:
	testq	$1, M		
	jz	.L999

	ALIGN_4

.L1_41:

#if !defined(TRMMKERNEL) || \
        (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
        (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	leaq	BUFFER1, BO		// first buffer to BO
	addq	$4 * SIZE, BO
#else
        movq    KK, %rax
	leaq	BUFFER1, BO			// first buffer to BO
	addq	$4 * SIZE, BO
	movq    %rax, BI                        //  Index for BO
        leaq    (,BI,2), BI                     //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$1, %rax			// rax = rax * 2 ; number of values
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


	andq	$-8, %rax			//  K = K - ( K % 8 )
	je	.L1_46
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values

	salq	$1, %rax			// rax = rax * 2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_42:

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x1_1(xxx)
	KERNEL1x1_2(xxx)
	KERNEL1x1_3(xxx)
	KERNEL1x1_4(xxx)

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x1_1(xxx)
	KERNEL1x1_2(xxx)
	KERNEL1x1_3(xxx)
	KERNEL1x1_4(xxx)

	je	.L1_46

	prefetcht0	B_PR1(BO,BI,SIZE)
	KERNEL1x1_1(xxx)
	KERNEL1x1_2(xxx)
	KERNEL1x1_3(xxx)
	KERNEL1x1_4(xxx)

	prefetcht0	B_PR1(BO,BI,SIZE)
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
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values

	salq	$1, %rax			// rax = rax * 2 ; number of values
	leaq	(AO, %rax, SIZE), AO
	leaq	(BO, BI, SIZE), BO
	negq	BI
	negq	%rax
	ALIGN_4

.L1_47:

	KERNEL1x1_SUB(xxx)
	jl	.L1_47
	ALIGN_4


.L1_49:

	vmovddup	ALPHA_R, %xmm0
	vmovddup	ALPHA_I, %xmm1

	// swap high and low 64 bytes
        vshufpd $0x01, %xmm9 , %xmm9, %xmm9

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)

        vaddsubpd %xmm9, %xmm8,  %xmm8

        vshufpd $0x01, %xmm8 , %xmm8, %xmm9

#else
        vaddsubpd %xmm8, %xmm9,  %xmm9

        vmovapd   %xmm9,  %xmm8

	// swap high and low 64 bytes
        vshufpd $0x01, %xmm9 , %xmm9, %xmm9

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

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
        movq    K, %rax
        subq    KKK, %rax
	movq    %rax, BI                        //  Index for BO
        leaq    ( ,BI,2), BI                    //  BI = BI * 2 ; number of values
        leaq    (BO, BI, SIZE), BO
	salq	$1, %rax			// rax = rax * 2 ; number of values
        leaq    (AO, %rax, SIZE), AO
#endif


#if defined(TRMMKERNEL) && defined(LEFT)
        addq    $1, KK
#endif

	addq	$2 * SIZE, CO1		# coffset += 2
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

	addq	$STACKSIZE, %rsp
	ret

	EPILOGUE
