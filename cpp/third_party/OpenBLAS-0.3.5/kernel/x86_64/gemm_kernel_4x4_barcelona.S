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
#define N	%r14
#define K	%rdx

#define A	%rcx
#define B	%r8
#define C	%r9
#define LDC	%r10

#define I	%r11
#define AO	%rdi
#define BO	%rsi
#define	CO1	%r15
#define CO2	%r12
#define BB	%rbp
#define	J	%rbx

#ifndef WINDOWS_ABI

#define STACKSIZE 96

#define ALPHA	 48(%rsp)
#define OFFSET	 56(%rsp)
#define KK	 64(%rsp)
#define KKK	 72(%rsp)

#else

#define STACKSIZE 256

#define OLD_A		40 + STACKSIZE(%rsp)
#define OLD_B		48 + STACKSIZE(%rsp)
#define OLD_C		56 + STACKSIZE(%rsp)
#define OLD_LDC		64 + STACKSIZE(%rsp)
#define OLD_OFFSET	72 + STACKSIZE(%rsp)

#define ALPHA	224(%rsp)
#define OFFSET	232(%rsp)
#define KK	240(%rsp)
#define KKK	248(%rsp)

#endif

#define movapd	movaps
#define movupd	movups

#define KERNEL1(xx) \
	mulpd	%xmm1, %xmm0 ;\
	addpd	%xmm0, %xmm8 ;\
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm1 ;\
	movapd	%xmm2, %xmm0 ;\
	addpd	%xmm1, %xmm12 ;\
	movddup	-14 * SIZE(BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm3 ;\
	addpd	%xmm2, %xmm9 ;\
	movapd	%xmm0, %xmm2 ;\
	addpd	%xmm3, %xmm13 ;\
	movddup	-13 * SIZE(BO, %rax, 4), %xmm3 ;\
	mulpd	%xmm1, %xmm0 ;\
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm1 ;\
	addpd	%xmm0, %xmm10 ;\
	movapd	-12 * SIZE(AO, %rax, 4), %xmm0 ;\
	addpd	%xmm1, %xmm14 ;\
	movddup	-12 * SIZE(BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm3 ;\
	addpd	%xmm2, %xmm11 ;\
	addpd	%xmm3, %xmm15 ;\
 	movddup	-11 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm0, %xmm2

#define KERNEL2(xx) \
	mulpd	%xmm1, %xmm0 ;\
	addpd	%xmm0, %xmm8 ;\
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm1 ;\
	movapd	%xmm2, %xmm0 ;\
	addpd	%xmm1, %xmm12 ;\
/*A*/	movapd	  (AO, %rax, 4), %xmm6 ;\
	movddup	-10 * SIZE(BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm3 ;\
	addpd	%xmm2, %xmm9 ;\
	movapd	%xmm0, %xmm2 ;\
	addpd	%xmm3, %xmm13 ;\
	movddup	 -9 * SIZE(BO, %rax, 4), %xmm3 ;\
	mulpd	%xmm1, %xmm0 ;\
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm1 ;\
	addpd	%xmm0, %xmm10 ;\
	addpd	%xmm1, %xmm14 ;\
/**/	movddup	  (BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm3 ;\
	addpd	%xmm2, %xmm11 ;\
	addpd	%xmm3, %xmm15 ;\
 	movddup	 -7 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm4, %xmm2

#define KERNEL3(xx) \
	mulpd	%xmm5, %xmm4 ;\
	addpd	%xmm4, %xmm8 ;\
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm5 ;\
	movapd	%xmm2, %xmm4 ;\
	addpd	%xmm5, %xmm12 ;\
	movddup	 -6 * SIZE(BO, %rax, 4), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm3 ;\
	addpd	%xmm2, %xmm9 ;\
	movapd	%xmm4, %xmm2 ;\
	addpd	%xmm3, %xmm13 ;\
	movddup	 -5 * SIZE(BO, %rax, 4), %xmm3 ;\
	mulpd	%xmm5, %xmm4 ;\
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm5 ;\
	addpd	%xmm4, %xmm10 ;\
	movapd	 -4 * SIZE(AO, %rax, 4), %xmm4 ;\
	addpd	%xmm5, %xmm14 ;\
	movddup	 -4 * SIZE(BO, %rax, 4), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm3 ;\
	addpd	%xmm2, %xmm11 ;\
	addpd	%xmm3, %xmm15 ;\
 	movddup	 -3 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm4, %xmm2

#define KERNEL4(xx) \
	mulpd	%xmm5, %xmm4 ;\
	addpd	%xmm4, %xmm8 ;\
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm5 ;\
	movapd	%xmm2, %xmm4 ;\
	addpd	%xmm5, %xmm12 ;\
/*A*/	movapd	  8 * SIZE(AO, %rax, 4), %xmm7 ;\
	movddup	 -2 * SIZE(BO, %rax, 4), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm3 ;\
	addpd	%xmm2, %xmm9 ;\
	movapd	%xmm4, %xmm2 ;\
	addpd	%xmm3, %xmm13 ;\
	movddup	 -1 * SIZE(BO, %rax, 4), %xmm3 ;\
	mulpd	%xmm5, %xmm4 ;\
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm5 ;\
	addpd	%xmm4, %xmm10 ;\
	addpd	%xmm5, %xmm14 ;\
/**/	movddup	  8 * SIZE(BO, %rax, 4), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm3 ;\
	addpd	%xmm2, %xmm11 ;\
	addpd	%xmm3, %xmm15 ;\
 	movddup	  1 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm6, %xmm2

#define KERNEL5(xx) \
	mulpd	%xmm1, %xmm6 ;\
	addpd	%xmm6, %xmm8 ;\
	mulpd	  2 * SIZE(AO, %rax, 4), %xmm1 ;\
	movapd	%xmm2, %xmm6 ;\
	addpd	%xmm1, %xmm12 ;\
	movddup	  2 * SIZE(BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	  2 * SIZE(AO, %rax, 4), %xmm3 ;\
	addpd	%xmm2, %xmm9 ;\
	movapd	%xmm6, %xmm2 ;\
	addpd	%xmm3, %xmm13 ;\
	movddup	  3 * SIZE(BO, %rax, 4), %xmm3 ;\
	mulpd	%xmm1, %xmm6 ;\
	mulpd	  2 * SIZE(AO, %rax, 4), %xmm1 ;\
	addpd	%xmm6, %xmm10 ;\
	movapd	  4 * SIZE(AO, %rax, 4), %xmm6 ;\
	addpd	%xmm1, %xmm14 ;\
	movddup	  4 * SIZE(BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	  2 * SIZE(AO, %rax, 4), %xmm3 ;\
	addpd	%xmm2, %xmm11 ;\
	addpd	%xmm3, %xmm15 ;\
 	movddup	  5 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm6, %xmm2

#define KERNEL6(xx) \
	mulpd	%xmm1, %xmm6 ;\
	addpd	%xmm6, %xmm8 ;\
	mulpd	  6 * SIZE(AO, %rax, 4), %xmm1 ;\
	movapd	%xmm2, %xmm6 ;\
	addpd	%xmm1, %xmm12 ;\
/*A*/	movapd	 16 * SIZE(AO, %rax, 4), %xmm0 ;\
	movddup	  6 * SIZE(BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	  6 * SIZE(AO, %rax, 4), %xmm3 ;\
	addpd	%xmm2, %xmm9 ;\
	movapd	%xmm6, %xmm2 ;\
	addpd	%xmm3, %xmm13 ;\
	movddup	  7 * SIZE(BO, %rax, 4), %xmm3 ;\
	mulpd	%xmm1, %xmm6 ;\
	mulpd	  6 * SIZE(AO, %rax, 4), %xmm1 ;\
	addpd	%xmm6, %xmm10 ;\
 	addpd	%xmm1, %xmm14 ;\
/**/	movddup	 16 * SIZE(BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	  6 * SIZE(AO, %rax, 4), %xmm3 ;\
	addpd	%xmm2, %xmm11 ;\
	addpd	%xmm3, %xmm15 ;\
 	movddup	  9 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm7, %xmm2

#define KERNEL7(xx) \
	mulpd	%xmm5, %xmm7 ;\
	addpd	%xmm7, %xmm8 ;\
	mulpd	 10 * SIZE(AO, %rax, 4), %xmm5 ;\
	movapd	%xmm2, %xmm7 ;\
	addpd	%xmm5, %xmm12 ;\
	movddup	 10 * SIZE(BO, %rax, 4), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 10 * SIZE(AO, %rax, 4), %xmm3 ;\
	addpd	%xmm2, %xmm9 ;\
	movapd	%xmm7, %xmm2 ;\
	addpd	%xmm3, %xmm13 ;\
	movddup	 11 * SIZE(BO, %rax, 4), %xmm3 ;\
	mulpd	%xmm5, %xmm7 ;\
	mulpd	 10 * SIZE(AO, %rax, 4), %xmm5 ;\
	addpd	%xmm7, %xmm10 ;\
	movapd	 12 * SIZE(AO, %rax, 4), %xmm7 ;\
	addpd	%xmm5, %xmm14 ;\
	movddup	 12 * SIZE(BO, %rax, 4), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 10 * SIZE(AO, %rax, 4), %xmm3 ;\
	addpd	%xmm2, %xmm11 ;\
	addpd	%xmm3, %xmm15 ;\
 	movddup	 13 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm7, %xmm2

#define KERNEL8(xx) \
	mulpd	%xmm5, %xmm7 ;\
	addpd	%xmm7, %xmm8 ;\
	mulpd	 14 * SIZE(AO, %rax, 4), %xmm5 ;\
	movapd	%xmm2, %xmm7 ;\
	addpd	%xmm5, %xmm12 ;\
/*A*/	movapd	 24 * SIZE(AO, %rax, 4), %xmm4 ;\
	movddup	 14 * SIZE(BO, %rax, 4), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 14 * SIZE(AO, %rax, 4), %xmm3 ;\
	addpd	%xmm2, %xmm9 ;\
	movapd	%xmm7, %xmm2 ;\
	addpd	%xmm3, %xmm13 ;\
	movddup	 15 * SIZE(BO, %rax, 4), %xmm3 ;\
	mulpd	%xmm5, %xmm7 ;\
	mulpd	 14 * SIZE(AO, %rax, 4), %xmm5 ;\
	addpd	%xmm7, %xmm10 ;\
	addpd	%xmm5, %xmm14 ;\
/**/	movddup	 24 * SIZE(BO, %rax, 4), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 14 * SIZE(AO, %rax, 4), %xmm3 ;\
	addpd	%xmm2, %xmm11 ;\
	addpd	%xmm3, %xmm15 ;\
 	movddup	 17 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm0, %xmm2 ;\
	addq	$8 * SIZE, %rax ;\

#define KERNEL_SUB1(xx) \
	mulpd	%xmm1, %xmm0 ;\
	mulpd	-14 * SIZE(AO), %xmm1 ;\
	addpd	%xmm0, %xmm8 ;\
	movapd	%xmm2, %xmm0 ;\
	addpd	%xmm1, %xmm12 ;\
	movddup	-14 * SIZE(BO), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	-14 * SIZE(AO), %xmm3 ;\
	addpd	%xmm2, %xmm9 ;\
	movapd	%xmm0, %xmm2 ;\
	addpd	%xmm3, %xmm13 ;\
	movddup	-13 * SIZE(BO), %xmm3 ;\
	mulpd	%xmm1, %xmm0 ;\
	mulpd	-14 * SIZE(AO), %xmm1 ;\
	addpd	%xmm0, %xmm10 ;\
	movapd	-12 * SIZE(AO), %xmm0 ;\
	addpd	%xmm1, %xmm14 ;\
	movddup	-12 * SIZE(BO), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	-14 * SIZE(AO), %xmm3 ;\
	addpd	%xmm2, %xmm11 ;\
	addpd	%xmm3, %xmm15 ;\
 	movddup	-11 * SIZE(BO), %xmm3 ;\
	movapd	%xmm0, %xmm2

#define KERNEL_SUB2(xx) \
	mulpd	%xmm1, %xmm0 ;\
	mulpd	-10 * SIZE(AO), %xmm1 ;\
	addpd	%xmm0, %xmm8 ;\
	movapd	%xmm2, %xmm0 ;\
	addpd	%xmm1, %xmm12 ;\
	movddup	-10 * SIZE(BO), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	-10 * SIZE(AO), %xmm3 ;\
	addpd	%xmm2, %xmm9 ;\
	movapd	%xmm0, %xmm2 ;\
	addpd	%xmm3, %xmm13 ;\
	movddup	 -9 * SIZE(BO), %xmm3 ;\
	mulpd	%xmm1, %xmm0 ;\
	mulpd	-10 * SIZE(AO), %xmm1 ;\
	addpd	%xmm0, %xmm10 ;\
	movapd	  (AO), %xmm0 ;\
	addpd	%xmm1, %xmm14 ;\
	movddup	  (BO), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	-10 * SIZE(AO), %xmm3 ;\
	addpd	%xmm2, %xmm11 ;\
	addpd	%xmm3, %xmm15 ;\
 	movddup	 -7 * SIZE(BO), %xmm3 ;\
	movapd	%xmm4, %xmm2

#define KERNEL_SUB3(xx) \
	mulpd	%xmm5, %xmm4 ;\
	mulpd	 -6 * SIZE(AO), %xmm5 ;\
	addpd	%xmm4, %xmm8 ;\
	movapd	%xmm2, %xmm4 ;\
	addpd	%xmm5, %xmm12 ;\
	movddup	 -6 * SIZE(BO), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 -6 * SIZE(AO), %xmm3 ;\
	addpd	%xmm2, %xmm9 ;\
	movapd	%xmm4, %xmm2 ;\
	addpd	%xmm3, %xmm13 ;\
	movddup	 -5 * SIZE(BO), %xmm3 ;\
	mulpd	%xmm5, %xmm4 ;\
	mulpd	 -6 * SIZE(AO), %xmm5 ;\
	addpd	%xmm4, %xmm10 ;\
	movapd	 -4 * SIZE(AO), %xmm4 ;\
	addpd	%xmm5, %xmm14 ;\
	movddup	 -4 * SIZE(BO), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 -6 * SIZE(AO), %xmm3 ;\
	addpd	%xmm2, %xmm11 ;\
	addpd	%xmm3, %xmm15 ;\
 	movddup	 -3 * SIZE(BO), %xmm3 ;\
	movapd	%xmm4, %xmm2

#define KERNEL_SUB4(xx) \
	mulpd	%xmm5, %xmm4 ;\
	mulpd	 -2 * SIZE(AO), %xmm5 ;\
	addpd	%xmm4, %xmm8 ;\
	movapd	%xmm2, %xmm4 ;\
	addpd	%xmm5, %xmm12 ;\
	movddup	 -2 * SIZE(BO), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 -2 * SIZE(AO), %xmm3 ;\
	addpd	%xmm2, %xmm9 ;\
	movapd	%xmm4, %xmm2 ;\
	addpd	%xmm3, %xmm13 ;\
	movddup	 -1 * SIZE(BO), %xmm3 ;\
	mulpd	%xmm5, %xmm4 ;\
	mulpd	 -2 * SIZE(AO), %xmm5 ;\
	addpd	%xmm4, %xmm10 ;\
	addpd	%xmm5, %xmm14 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 -2 * SIZE(AO), %xmm3 ;\
	addpd	%xmm2, %xmm11 ;\
	addpd	%xmm3, %xmm15 ;\
 	movddup	  1 * SIZE(BO), %xmm3 ;\
	movapd	%xmm0, %xmm2

	PROLOGUE
	PROFCODE

	subq	$STACKSIZE, %rsp
	movq	%rbx,   (%rsp)
	movq	%rbp,  8(%rsp)
	movq	%r12, 16(%rsp)
	movq	%r13, 24(%rsp)
	movq	%r14, 32(%rsp)
	movq	%r15, 40(%rsp)

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
	movq	ARG3,      K
	movq	OLD_A,     A
	movq	OLD_B,     B
	movq	OLD_C,     C
	movq	OLD_LDC,   LDC
#ifdef TRMMKERNEL
	movsd	OLD_OFFSET, %xmm12
#endif
	movaps	%xmm3, %xmm0

#else
	movq	STACKSIZE +  8(%rsp), LDC
#ifdef TRMMKERNEL
	movsd	STACKSIZE + 16(%rsp), %xmm12
#endif

#endif

	movq	OLD_M, M
	movq	OLD_N, N

	subq	$-16 * SIZE, A
	subq	$-16 * SIZE, B

	movsd	 %xmm0, ALPHA

	salq	$BASE_SHIFT, LDC

#ifdef TRMMKERNEL
	movsd	%xmm12, OFFSET
	movsd	%xmm12, KK
#ifndef LEFT
	negq	KK
#endif
#endif
	movq	N,  J
	sarq	$2, J		# j = (n >> 2)
	jle	.L40
	ALIGN_4

.L01:
	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 2), CO2	# coffset2 = c + ldc

	leaq	(C, LDC, 4), C		# c += 4 * ldc

#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	A, AO		# aoffset = a

	movq	K, %rax
	salq	$BASE_SHIFT + 2, %rax
	leaq	 (B, %rax), BB

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L20
	ALIGN_4

.L11:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	B, BO
#else
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 4), BO
#endif

	movapd	-16 * SIZE(AO), %xmm0
	xorps	%xmm8, %xmm8
	movddup	-16 * SIZE(BO), %xmm1
	xorps	%xmm9, %xmm9
 	movddup	-15 * SIZE(BO), %xmm3
	xorps	%xmm10, %xmm10
	movapd	 -8 * SIZE(AO), %xmm4
	xorps	%xmm11, %xmm11
	movddup	 -8 * SIZE(BO), %xmm5
	xorps	%xmm12, %xmm12

	prefetchw      3 * SIZE(CO1)
	xorps	%xmm13, %xmm13
	prefetchw      7 * SIZE(CO1, LDC)
	xorps	%xmm14, %xmm14
	prefetchw      3 * SIZE(CO2)
	xorps	%xmm15, %xmm15
	prefetchw      7 * SIZE(CO2, LDC)
	movapd	%xmm0, %xmm2

	prefetch	 -16 * SIZE(BB)

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$4, %rax
#else
	addq	$4, %rax
#endif
	movq	%rax, KKK
#endif

	andq	$-8, %rax
	salq	$BASE_SHIFT, %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
	negq	%rax
	NOBRANCH
	je	.L15
	ALIGN_4

.L12:
	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)
	NOBRANCH
	je	.L15
	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)
	NOBRANCH
	je	.L15
	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)
	NOBRANCH
	je	.L15
	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)
	NOBRANCH
	je	.L15
	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)
	NOBRANCH
	je	.L15
	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)
	NOBRANCH
	je	.L15
	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)
	NOBRANCH
	je	.L15
	KERNEL1(16 *  0)
	KERNEL2(16 *  0)
	KERNEL3(16 *  0)
	KERNEL4(16 *  0)
	KERNEL5(16 *  0)
	KERNEL6(16 *  0)
	KERNEL7(16 *  0)
	KERNEL8(16 *  0)
	jl	.L12
	ALIGN_4

.L15:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	testq	$4, %rax
	je .L16
	ALIGN_4

	KERNEL_SUB1(16 *  0)
	KERNEL_SUB2(16 *  0)
	KERNEL_SUB3(16 *  0)
	KERNEL_SUB4(16 *  0)

	subq	$-16 * SIZE, BO
	subq	$-16 * SIZE, AO
	ALIGN_4

.L16:
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L19

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
	negq	%rax
	ALIGN_4

.L17:
	mulpd	%xmm1, %xmm0
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm1
	addpd	%xmm0, %xmm8
	movapd	%xmm2, %xmm0
	addpd	%xmm1, %xmm12
	movddup	-14 * SIZE(BO, %rax, 4), %xmm1
	mulpd	%xmm3, %xmm2
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm3
	addpd	%xmm2, %xmm9
	movapd	%xmm0, %xmm2
	addpd	%xmm3, %xmm13
	movddup	-13 * SIZE(BO, %rax, 4), %xmm3
	mulpd	%xmm1, %xmm0
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm1
	addpd	%xmm0, %xmm10
	movapd	-12 * SIZE(AO, %rax, 4), %xmm0
	addpd	%xmm1, %xmm14
	movddup	-12 * SIZE(BO, %rax, 4), %xmm1
	mulpd	%xmm3, %xmm2
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm3
	addpd	%xmm2, %xmm11
	addpd	%xmm3, %xmm15
 	movddup	-11 * SIZE(BO, %rax, 4), %xmm3
	movapd	%xmm0, %xmm2

	addq	$SIZE, %rax
	jl	.L17
	ALIGN_4

.L19:
	prefetch	   -8 * SIZE(BB)
	subq		 $-16 * SIZE, BB

#ifndef TRMMKERNEL
	movupd	(CO1), %xmm0
	movupd	2 * SIZE(CO1), %xmm1
#endif

	mulpd	%xmm7, %xmm8
	mulpd	%xmm7, %xmm12
#ifndef TRMMKERNEL
	addpd	%xmm0, %xmm8
	addpd	%xmm1, %xmm12
#endif

	movsd	%xmm8,  (CO1)
	movhps	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm12, 2 * SIZE(CO1)
	movhps	%xmm12, 3 * SIZE(CO1)

#ifndef TRMMKERNEL
	movupd	(CO1, LDC), %xmm2
	movupd	2 * SIZE(CO1, LDC), %xmm3
#endif

	mulpd	%xmm7, %xmm9
	mulpd	%xmm7, %xmm13
#ifndef TRMMKERNEL
	addpd	%xmm2, %xmm9
	addpd	%xmm3, %xmm13
#endif

	movsd	%xmm9,  (CO1, LDC)
	movhps	%xmm9,  1 * SIZE(CO1, LDC)
	movsd	%xmm13, 2 * SIZE(CO1, LDC)
	movhps	%xmm13, 3 * SIZE(CO1, LDC)

#ifndef TRMMKERNEL
	movupd	(CO2), %xmm0
	movupd	2 * SIZE(CO2), %xmm1
#endif

	mulpd	%xmm7, %xmm10
	mulpd	%xmm7, %xmm14
#ifndef TRMMKERNEL
	addpd	%xmm0, %xmm10
	addpd	%xmm1, %xmm14
#endif

	movsd	%xmm10, (CO2)
	movhps	%xmm10, 1 * SIZE(CO2)
	movsd	%xmm14, 2 * SIZE(CO2)
	movhps	%xmm14, 3 * SIZE(CO2)

#ifndef TRMMKERNEL
	movupd	(CO2, LDC), %xmm2
	movupd	2 * SIZE(CO2, LDC), %xmm3
#endif

	mulpd	%xmm7, %xmm11
	mulpd	%xmm7, %xmm15
#ifndef TRMMKERNEL
	addpd	%xmm2, %xmm11
	addpd	%xmm3, %xmm15
#endif

	movsd	%xmm11, (CO2, LDC)
	movhps	%xmm11, 1 * SIZE(CO2, LDC)
	movsd	%xmm15, 2 * SIZE(CO2, LDC)
	movhps	%xmm15, 3 * SIZE(CO2, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	addq	$4 * SIZE, CO2		# coffset += 4
	decq	I			# i --
	BRANCH
	jg	.L11
	ALIGN_4

.L20:
	testq	$3, M
	je	.L39

	testq	$2, M
	je	.L30
	ALIGN_4

.L21:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	B, BO
#else
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 4), BO
#endif

	movapd	-16 * SIZE(AO), %xmm0
	xorps	%xmm8, %xmm8
	movapd	-12 * SIZE(AO), %xmm2
	xorps	%xmm9, %xmm9
	movddup	-16 * SIZE(BO), %xmm1
	xorps	%xmm10, %xmm10
	movddup	-15 * SIZE(BO), %xmm5
	xorps	%xmm11, %xmm11
	movddup	 -8 * SIZE(BO), %xmm3

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$2, %rax
#else
	addq	$4, %rax
#endif
	movq	%rax, KKK
#endif

	andq	$-4, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
	negq	%rax
	NOBRANCH
	je	.L26
	ALIGN_4

.L22:
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movddup	-14 * SIZE(BO, %rax, 4), %xmm1
	mulpd	%xmm0, %xmm5
	addpd	%xmm5, %xmm9
	movddup	-13 * SIZE(BO, %rax, 4), %xmm5
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm10
	movddup	-12 * SIZE(BO, %rax, 4), %xmm1
	mulpd	%xmm0, %xmm5
	movapd	-14 * SIZE(AO, %rax, 2), %xmm0
	addpd	%xmm5, %xmm11
	movddup	-11 * SIZE(BO, %rax, 4), %xmm5
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movddup	-10 * SIZE(BO, %rax, 4), %xmm1
	mulpd	%xmm0, %xmm5
	addpd	%xmm5, %xmm9
	movddup	 -9 * SIZE(BO, %rax, 4), %xmm5
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm10
	movddup	  (BO, %rax, 4), %xmm1
	mulpd	%xmm0, %xmm5
	movapd	 -8 * SIZE(AO, %rax, 2), %xmm0
	addpd	%xmm5, %xmm11
	movddup	 -7 * SIZE(BO, %rax, 4), %xmm5
	mulpd	%xmm2, %xmm3
	addpd	%xmm3, %xmm8
	movddup	 -6 * SIZE(BO, %rax, 4), %xmm3
	mulpd	%xmm2, %xmm5
	addpd	%xmm5, %xmm9
	movddup	 -5 * SIZE(BO, %rax, 4), %xmm5
	mulpd	%xmm2, %xmm3
	addpd	%xmm3, %xmm10
	movddup	 -4 * SIZE(BO, %rax, 4), %xmm3
	mulpd	%xmm2, %xmm5
	movapd	-10 * SIZE(AO, %rax, 2), %xmm2
	addpd	%xmm5, %xmm11
	movddup	 -3 * SIZE(BO, %rax, 4), %xmm5
	mulpd	%xmm2, %xmm3
	addpd	%xmm3, %xmm8
	movddup	 -2 * SIZE(BO, %rax, 4), %xmm3
	mulpd	%xmm2, %xmm5
	addpd	%xmm5, %xmm9
	movddup	 -1 * SIZE(BO, %rax, 4), %xmm5
	mulpd	%xmm2, %xmm3
	addpd	%xmm3, %xmm10
	movddup	  8 * SIZE(BO, %rax, 4), %xmm3
	mulpd	%xmm2, %xmm5
	movapd	 -4 * SIZE(AO, %rax, 2), %xmm2
	addpd	%xmm5, %xmm11
	movddup	  1 * SIZE(BO, %rax, 4), %xmm5

	addq	$4 * SIZE, %rax
	BRANCH
	jl	.L22
	ALIGN_4

.L26:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L29

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
	negq	%rax
	ALIGN_4

.L27:
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movddup	-14 * SIZE(BO, %rax, 4), %xmm1
	mulpd	%xmm0, %xmm5
	addpd	%xmm5, %xmm9
	movddup	-13 * SIZE(BO, %rax, 4), %xmm5
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm10
	movddup	-12 * SIZE(BO, %rax, 4), %xmm1
	mulpd	%xmm0, %xmm5
	movapd	-14 * SIZE(AO, %rax, 2), %xmm0
	addpd	%xmm5, %xmm11
	movddup	-11 * SIZE(BO, %rax, 4), %xmm5

	addq	$SIZE, %rax
	jl	.L27
	ALIGN_4

.L29:
#ifndef TRMMKERNEL
	movupd	(CO1), %xmm0
	movupd	(CO1, LDC), %xmm2
	movupd	(CO2), %xmm4
	movupd	(CO2, LDC), %xmm6
#endif

	mulpd	%xmm7, %xmm8
	mulpd	%xmm7, %xmm9
	mulpd	%xmm7, %xmm10
	mulpd	%xmm7, %xmm11

#ifndef TRMMKERNEL
	addpd	%xmm0, %xmm8
	addpd	%xmm2, %xmm9
	addpd	%xmm4, %xmm10
	addpd	%xmm6, %xmm11
#endif

	movsd	%xmm8,  (CO1)
	movhps	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm9,  (CO1, LDC)
	movhps	%xmm9,  1 * SIZE(CO1, LDC)

	movsd	%xmm10, (CO2)
	movhps	%xmm10, 1 * SIZE(CO2)
	movsd	%xmm11, (CO2, LDC)
	movhps	%xmm11, 1 * SIZE(CO2, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$2 * SIZE, CO1
	addq	$2 * SIZE, CO2
	ALIGN_4

.L30:
	testq	$1, M
	je	.L39

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	B, BO
#else
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 4), BO
#endif

	movddup	-16 * SIZE(AO), %xmm0
	xorps	%xmm8, %xmm8
	movddup	-14 * SIZE(AO), %xmm2
	xorps	%xmm9, %xmm9
	movddup	-15 * SIZE(AO), %xmm4
	xorps	%xmm10, %xmm10
	movapd	-16 * SIZE(BO), %xmm1
	xorps	%xmm11, %xmm11
	movapd	 -8 * SIZE(BO), %xmm3

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$1, %rax
#else
	addq	$4, %rax
#endif
	movq	%rax, KKK
#endif

	andq	$-4, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
	negq	%rax
	NOBRANCH
	je	.L36
	ALIGN_4

.L32:
	mulpd	%xmm0, %xmm1
	mulpd	-14 * SIZE(BO, %rax, 4), %xmm0
	addpd	%xmm1, %xmm8
	movapd	-12 * SIZE(BO, %rax, 4), %xmm1
	addpd	%xmm0, %xmm9
	movddup	-12 * SIZE(AO, %rax, 1), %xmm0
	mulpd	%xmm4, %xmm1
	mulpd	-10 * SIZE(BO, %rax, 4), %xmm4
	addpd	%xmm1, %xmm10
	movapd	  (BO, %rax, 4), %xmm1
	addpd	%xmm4, %xmm11
	movddup	-11 * SIZE(AO, %rax, 1), %xmm4
	mulpd	%xmm2, %xmm3
	mulpd	 -6 * SIZE(BO, %rax, 4), %xmm2
	addpd	%xmm3, %xmm8
	movapd	 -4 * SIZE(BO, %rax, 4), %xmm3
	addpd	%xmm2, %xmm9
	movddup	-13 * SIZE(AO, %rax, 1), %xmm2
	mulpd	%xmm2, %xmm3
	mulpd	 -2 * SIZE(BO, %rax, 4), %xmm2
	addpd	%xmm3, %xmm10
	movapd	  8 * SIZE(BO, %rax, 4), %xmm3
	addpd	%xmm2, %xmm11
	movddup	-10 * SIZE(AO, %rax, 1), %xmm2

	addq	$4 * SIZE, %rax
	BRANCH
	jl	.L32
	ALIGN_4

.L36:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L38

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
	negq	%rax
	ALIGN_4

.L37:
	mulpd	%xmm0, %xmm1
	mulpd	-14 * SIZE(BO, %rax, 4), %xmm0
	addpd	%xmm1, %xmm8
	movapd	-12 * SIZE(BO, %rax, 4), %xmm1
	addpd	%xmm0, %xmm9
	movddup	-15 * SIZE(AO, %rax, 1), %xmm0

	addq	$SIZE, %rax
	jl	.L37
	ALIGN_4

.L38:
	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9

#ifndef TRMMKERNEL
	movsd	(CO1), %xmm0
	movhps	(CO1, LDC), %xmm0
	movsd	(CO2), %xmm1
	movhps	(CO2, LDC), %xmm1
#endif

	mulpd	%xmm7, %xmm8
	mulpd	%xmm7, %xmm9

#ifndef TRMMKERNEL
	addpd	%xmm0, %xmm8
	addpd	%xmm1, %xmm9
#endif

	movsd	%xmm8,  (CO1)
	movhps	%xmm8,  (CO1, LDC)
	movsd	%xmm9,  (CO2)
	movhps	%xmm9,  (CO2, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif
	ALIGN_4

.L39:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addq	$4, KK
#endif

	movq	BO, B

	decq	J			# j --
	jg	.L01
	ALIGN_4

.L40:
	testq	$3, N
	je	.L999

	testq	$2, N
	je	.L80
	ALIGN_4

.L41:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc
	movq	A, AO		# aoffset = a

	movq	K, %rax
	salq	$BASE_SHIFT + 1, %rax
	leaq	(B, %rax), BB

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L60
	ALIGN_4

.L51:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	B, BO
#else
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 2), BO
#endif

	movddup	-16 * SIZE(BO), %xmm1
	movddup	-15 * SIZE(BO), %xmm5
	xorps	%xmm8, %xmm8
	movddup	-12 * SIZE(BO), %xmm3
	xorps	%xmm9, %xmm9
	movapd	-16 * SIZE(AO), %xmm0
	xorps	%xmm12, %xmm12
	movapd	 -8 * SIZE(AO), %xmm4
	xorps	%xmm13, %xmm13
	prefetchw      3 * SIZE(CO1)
	movapd	%xmm0, %xmm2
	prefetchw      3 * SIZE(CO2)

	prefetch	 -16 * SIZE(BB)
	subq		 $-8 * SIZE, BB

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$4, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
#endif

	andq	$-4, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
	negq	%rax
	NOBRANCH
	je	.L56
	ALIGN_4

.L52:
	mulpd	%xmm1, %xmm0
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm1
	addpd	%xmm0, %xmm8
	movapd	-12 * SIZE(AO, %rax, 4), %xmm0
	addpd	%xmm1, %xmm12
	movddup	-14 * SIZE(BO, %rax, 2), %xmm1
	mulpd	%xmm5, %xmm2
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm5
	addpd	%xmm2, %xmm9
	addpd	%xmm5, %xmm13
	movddup	-13 * SIZE(BO, %rax, 2), %xmm5
	movapd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm0
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm1
	addpd	%xmm0, %xmm8
	movapd	  (AO, %rax, 4), %xmm0
	addpd	%xmm1, %xmm12
	movddup	 -8 * SIZE(BO, %rax, 2), %xmm1
	mulpd	%xmm5, %xmm2
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm5
	addpd	%xmm2, %xmm9
	addpd	%xmm5, %xmm13
	movddup	-11 * SIZE(BO, %rax, 2), %xmm5
	movapd	%xmm4, %xmm2
	mulpd	%xmm3, %xmm4
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm3
	addpd	%xmm4, %xmm8
	movapd	 -4 * SIZE(AO, %rax, 4), %xmm4
	addpd	%xmm3, %xmm12
	movddup	-10 * SIZE(BO, %rax, 2), %xmm3
	mulpd	%xmm5, %xmm2
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm5
	addpd	%xmm2, %xmm9
	addpd	%xmm5, %xmm13
	movddup	 -9 * SIZE(BO, %rax, 2), %xmm5
	movapd	%xmm4, %xmm2
	mulpd	%xmm3, %xmm4
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm3
	addpd	%xmm4, %xmm8
	movapd	  8 * SIZE(AO, %rax, 4), %xmm4
	addpd	%xmm3, %xmm12
	movddup	 -4 * SIZE(BO, %rax, 2), %xmm3
	mulpd	%xmm5, %xmm2
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm5
	addpd	%xmm2, %xmm9
	addpd	%xmm5, %xmm13
	movddup	 -7 * SIZE(BO, %rax, 2), %xmm5
	movapd	%xmm0, %xmm2

	addq	$4 * SIZE, %rax
	BRANCH
	jl	.L52
	ALIGN_4

.L56:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L59

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
	negq	%rax
	ALIGN_4

.L57:
	mulpd	%xmm1, %xmm0
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm1
	addpd	%xmm0, %xmm8
	movapd	-12 * SIZE(AO, %rax, 4), %xmm0
	addpd	%xmm1, %xmm12
	movddup	-14 * SIZE(BO, %rax, 2), %xmm1
	mulpd	%xmm5, %xmm2
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm5
	addpd	%xmm2, %xmm9
	addpd	%xmm5, %xmm13
	movddup	-13 * SIZE(BO, %rax, 2), %xmm5
	movapd	%xmm0, %xmm2

	addq	$SIZE, %rax
	jl	.L57
	ALIGN_4

.L59:
#ifndef TRMMKERNEL
	movupd	(CO1), %xmm0
	movupd	2 * SIZE(CO1), %xmm1
	movupd	(CO2), %xmm2
	movupd	2 * SIZE(CO2), %xmm3
#endif

	mulpd	%xmm7, %xmm8
	mulpd	%xmm7, %xmm9
	mulpd	%xmm7, %xmm12
	mulpd	%xmm7, %xmm13

#ifndef TRMMKERNEL
	addpd	%xmm0, %xmm8
	addpd	%xmm1, %xmm12
	addpd	%xmm2, %xmm9
	addpd	%xmm3, %xmm13
#endif

	movsd	%xmm8,  (CO1)
	movhps	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm12, 2 * SIZE(CO1)
	movhps	%xmm12, 3 * SIZE(CO1)

	movsd	%xmm9,  (CO2)
	movhps	%xmm9,  1 * SIZE(CO2)
	movsd	%xmm13, 2 * SIZE(CO2)
	movhps	%xmm13, 3 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	addq	$4 * SIZE, CO2		# coffset += 4
	decq	I			# i --
	jg	.L51
	ALIGN_4

.L60:
	testq	$2, M
	je	.L70
	ALIGN_4

.L61:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	B, BO
#else
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 2), BO
#endif

	movapd	-16 * SIZE(AO), %xmm0
	xorps	%xmm8, %xmm8
	movapd	-12 * SIZE(AO), %xmm2
	xorps	%xmm9, %xmm9
	movddup	-16 * SIZE(BO), %xmm1
	xorps	%xmm10, %xmm10
	movddup	-15 * SIZE(BO), %xmm3
	xorps	%xmm11, %xmm11

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$2, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
#endif

	andq	$-4, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
	negq	%rax
	NOBRANCH
	je	.L66
	ALIGN_4

.L62:
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movddup	-14 * SIZE(BO, %rax, 2), %xmm1
	mulpd	%xmm0, %xmm3
	movapd	-14 * SIZE(AO, %rax, 2), %xmm0
	addpd	%xmm3, %xmm9
	movddup	-13 * SIZE(BO, %rax, 2), %xmm3
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm10
	movddup	-12 * SIZE(BO, %rax, 2), %xmm1
	mulpd	%xmm0, %xmm3
	movapd	 -8 * SIZE(AO, %rax, 2), %xmm0
	addpd	%xmm3, %xmm11
	movddup	-11 * SIZE(BO, %rax, 2), %xmm3
	mulpd	%xmm2, %xmm1
	addpd	%xmm1, %xmm8
	movddup	-10 * SIZE(BO, %rax, 2), %xmm1
	mulpd	%xmm2, %xmm3
	movapd	-10 * SIZE(AO, %rax, 2), %xmm2
	addpd	%xmm3, %xmm9
	movddup	 -9 * SIZE(BO, %rax, 2), %xmm3
	mulpd	%xmm2, %xmm1
	addpd	%xmm1, %xmm10
	movddup	 -8 * SIZE(BO, %rax, 2), %xmm1
	mulpd	%xmm2, %xmm3
	movapd	 -4 * SIZE(AO, %rax, 2), %xmm2
	addpd	%xmm3, %xmm11
	movddup	 -7 * SIZE(BO, %rax, 2), %xmm3

	addq	$4 * SIZE, %rax
	BRANCH
	jl	.L62
	ALIGN_4

.L66:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L69

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
	negq	%rax
	ALIGN_4

.L67:
	mulpd	%xmm0, %xmm1
	addpd	%xmm1, %xmm8
	movddup	-14 * SIZE(BO, %rax, 2), %xmm1
	mulpd	%xmm0, %xmm3
	movapd	-14 * SIZE(AO, %rax, 2), %xmm0
	addpd	%xmm3, %xmm9
	movddup	-13 * SIZE(BO, %rax, 2), %xmm3

	addq	$SIZE, %rax
	jl	.L67
	ALIGN_4

.L69:
	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9

#ifndef TRMMKERNEL
	movupd	(CO1), %xmm0
	movupd	(CO2), %xmm2
#endif

	mulpd	%xmm7, %xmm8
	mulpd	%xmm7, %xmm9

#ifndef TRMMKERNEL
	addpd	%xmm0, %xmm8
	addpd	%xmm2, %xmm9
#endif

	movsd	%xmm8,  (CO1)
	movhps	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm9,  (CO2)
	movhps	%xmm9,  1 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$2 * SIZE, CO1		# coffset += 4
	addq	$2 * SIZE, CO2		# coffset += 4
	ALIGN_4

.L70:
	testq	$1, M
	je	.L79
	ALIGN_4

.L71:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	B, BO
#else
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 2), BO
#endif

	movddup	-16 * SIZE(AO), %xmm0
	xorps	%xmm8, %xmm8
	movddup	-15 * SIZE(AO), %xmm1
	xorps	%xmm9, %xmm9
	movddup	-14 * SIZE(AO), %xmm2
	xorps	%xmm10, %xmm10
	movddup	-13 * SIZE(AO), %xmm3
	xorps	%xmm11, %xmm11

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$1, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
#endif

	andq	$-4, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
	negq	%rax
	NOBRANCH
	je	.L76
	ALIGN_4

.L72:
	mulpd	-16 * SIZE(BO, %rax, 2), %xmm0
	addpd	%xmm0, %xmm8
	movddup	-12 * SIZE(AO, %rax, 1), %xmm0

	mulpd	-14 * SIZE(BO, %rax, 2), %xmm1
	addpd	%xmm1, %xmm9
	movddup	-11 * SIZE(AO, %rax, 1), %xmm1

	mulpd	-12 * SIZE(BO, %rax, 2), %xmm2
	addpd	%xmm2, %xmm10
	movddup	-10 * SIZE(AO, %rax, 1), %xmm2

	mulpd	-10 * SIZE(BO, %rax, 2), %xmm3
	addpd	%xmm3, %xmm11
	movddup	 -9 * SIZE(AO, %rax, 1), %xmm3

	addq	$4 * SIZE, %rax
	BRANCH
	jl	.L72
	ALIGN_4

.L76:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L78

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
	negq	%rax
	ALIGN_4

.L77:
	mulpd	-16 * SIZE(BO, %rax, 2), %xmm0
	addpd	%xmm0, %xmm8
	movddup	-15 * SIZE(AO, %rax, 1), %xmm0

	addq	$SIZE, %rax
	jl	.L77
	ALIGN_4

.L78:
	addpd	%xmm9,  %xmm8
	addpd	%xmm11, %xmm10
	addpd	%xmm10, %xmm8

#ifndef TRMMKERNEL
	movsd	(CO1), %xmm0
	movhps	(CO2), %xmm0
#endif

	mulpd	%xmm7, %xmm8

#ifndef TRMMKERNEL
	addpd	%xmm0, %xmm8
#endif

	movsd	%xmm8,  (CO1)
	movhps	%xmm8,  (CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif
	ALIGN_4

.L79:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addq	$2, KK
#endif

	movq	BO, B

	leaq	(C, LDC, 2), C
	ALIGN_4

.L80:
	testq	$1, N
	je	.L999
	ALIGN_4

.L81:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	C, CO1			# coffset1 = c
	movq	A, AO		# aoffset = a

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L100
	ALIGN_4

.L91:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	B, BO
#else
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 1), BO
#endif

	movapd	 -8 * SIZE(AO), %xmm2
	xorps	%xmm8, %xmm8
	movapd	-16 * SIZE(AO), %xmm0
	xorps	%xmm9, %xmm9
	movddup	-16 * SIZE(BO), %xmm1
	xorps	%xmm12, %xmm12
	movddup	-14 * SIZE(BO), %xmm3
	xorps	%xmm13, %xmm13
	movddup	-15 * SIZE(BO), %xmm5

	prefetchw      3 * SIZE(CO1)

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$4, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif

	andq	$-4, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 1), BO
	negq	%rax
	NOBRANCH
	je	.L96
	ALIGN_4

.L92:
	mulpd	%xmm1, %xmm0
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm1
	addpd	%xmm0, %xmm8
	movapd	-12 * SIZE(AO, %rax, 4), %xmm0
	addpd	%xmm1, %xmm12
	movddup	-12 * SIZE(BO, %rax, 1), %xmm1
	mulpd	%xmm5, %xmm0
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm5
	addpd	%xmm0, %xmm9
	movapd	  (AO, %rax, 4), %xmm0
	addpd	%xmm5, %xmm13
	movddup	-13 * SIZE(BO, %rax, 1), %xmm5
	mulpd	%xmm3, %xmm2
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm3
	addpd	%xmm2, %xmm8
	movapd	 -4 * SIZE(AO, %rax, 4), %xmm2
	addpd	%xmm3, %xmm12
	movddup	-10 * SIZE(BO, %rax, 1), %xmm3
	mulpd	%xmm5, %xmm2
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm5
	addpd	%xmm2, %xmm9
	movapd	  8 * SIZE(AO, %rax, 4), %xmm2
	addpd	%xmm5, %xmm13
	movddup	-11 * SIZE(BO, %rax, 1), %xmm5

	addq	$4 * SIZE, %rax
	BRANCH
	jl	.L92
	ALIGN_4

.L96:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L99

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 1), BO
	negq	%rax
	ALIGN_4

.L97:
	mulpd	%xmm1, %xmm0
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm1
	addpd	%xmm0, %xmm8
	movapd	-12 * SIZE(AO, %rax, 4), %xmm0
	addpd	%xmm1, %xmm12
	movddup	-15 * SIZE(BO, %rax, 1), %xmm1

	addq	$SIZE, %rax
	jl	.L97
	ALIGN_4

.L99:
	addpd	%xmm9,  %xmm8
	addpd	%xmm13, %xmm12

#ifndef TRMMKERNEL
	movupd	(CO1), %xmm0
	movupd	2 * SIZE(CO1), %xmm1
#endif

	mulpd	%xmm7, %xmm8
	mulpd	%xmm7, %xmm12

#ifndef TRMMKERNEL
	addpd	%xmm0, %xmm8
	addpd	%xmm1, %xmm12
#endif

	movsd	%xmm8,  (CO1)
	movhps	%xmm8,  1 * SIZE(CO1)
	movsd	%xmm12, 2 * SIZE(CO1)
	movhps	%xmm12, 3 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 1), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$4, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	decq	I			# i --
	jg	.L91
	ALIGN_4

.L100:
	testq	$2, M
	je	.L110
	ALIGN_4

.L101:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	B, BO
#else
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 1), BO
#endif

	movddup	-16 * SIZE(BO), %xmm0
	xorps	%xmm8, %xmm8
	movddup	-15 * SIZE(BO), %xmm1
	xorps	%xmm9, %xmm9
	movddup	-14 * SIZE(BO), %xmm2
	xorps	%xmm10, %xmm10
	movddup	-13 * SIZE(BO), %xmm3
	xorps	%xmm11, %xmm11

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$2, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif

	andq	$-4, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 1), BO
	negq	%rax
	NOBRANCH
	je	.L106
	ALIGN_4

.L102:
	mulpd	-16 * SIZE(AO, %rax, 2), %xmm0
	addpd	%xmm0, %xmm8
	movddup	-12 * SIZE(BO, %rax, 1), %xmm0

	mulpd	-14 * SIZE(AO, %rax, 2), %xmm1
	addpd	%xmm1, %xmm9
	movddup	-11 * SIZE(BO, %rax, 1), %xmm1

	mulpd	-12 * SIZE(AO, %rax, 2), %xmm2
	addpd	%xmm2, %xmm10
	movddup	-10 * SIZE(BO, %rax, 1), %xmm2

	mulpd	-10 * SIZE(AO, %rax, 2), %xmm3
	addpd	%xmm3, %xmm11
	movddup	 -9 * SIZE(BO, %rax, 1), %xmm3

	addq	$4 * SIZE, %rax
	BRANCH
	jl	.L102
	ALIGN_4

.L106:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L109

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 1), BO
	negq	%rax
	ALIGN_4

.L107:
	movddup	-16 * SIZE(BO, %rax, 1), %xmm0
	mulpd	-16 * SIZE(AO, %rax, 2), %xmm0
	addpd	%xmm0, %xmm8

	addq	$SIZE, %rax
	jl	.L107
	ALIGN_4

.L109:
	addpd	%xmm9,  %xmm8
	addpd	%xmm11, %xmm10
	addpd	%xmm10, %xmm8

#ifndef TRMMKERNEL
	movupd	(CO1), %xmm0
#endif

	mulpd	%xmm7, %xmm8

#ifndef TRMMKERNEL
	addpd	%xmm0, %xmm8
#endif

	movsd	%xmm8,  (CO1)
	movhps	%xmm8,  1 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 1), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$2 * SIZE, CO1		# coffset += 4

	ALIGN_4

.L110:
	testq	$1, M
	je	.L999
	ALIGN_4

.L111:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	B, BO
#else
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 1), BO
#endif

	movapd	-16 * SIZE(AO), %xmm0
	xorps	%xmm8, %xmm8
	movapd	-14 * SIZE(AO), %xmm1
	xorps	%xmm9, %xmm9

#ifndef TRMMKERNEL
	movq	K, %rax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movq	K, %rax
	subq	KK, %rax
	movq	%rax, KKK
#else
	movq	KK, %rax
#ifdef LEFT
	addq	$1, %rax
#else
	addq	$1, %rax
#endif
	movq	%rax, KKK
#endif

	andq	$-4, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 1), BO
	negq	%rax
	NOBRANCH
	je	.L116
	ALIGN_4

.L112:
	mulpd	-16 * SIZE(BO, %rax, 1), %xmm0
	addpd	%xmm0, %xmm8
	movapd	-12 * SIZE(AO, %rax, 1), %xmm0

	mulpd	-14 * SIZE(BO, %rax, 1), %xmm1
	addpd	%xmm1, %xmm9
	movapd	-10 * SIZE(AO, %rax, 1), %xmm1

	addq	$4 * SIZE, %rax
	BRANCH
	jl	.L112
	ALIGN_4

.L116:
	movddup	ALPHA, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L118

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 1), BO
	negq	%rax
	ALIGN_4

.L117:
	mulsd	-16 * SIZE(BO, %rax, 1), %xmm0
	addsd	%xmm0, %xmm8
	movsd	-15 * SIZE(AO, %rax, 1), %xmm0

	addq	$SIZE, %rax
	jl	.L117
	ALIGN_4

.L118:
	addpd	%xmm9, %xmm8
	haddpd	%xmm8, %xmm8

#ifndef TRMMKERNEL
	movsd	(CO1), %xmm0
#endif

	mulsd	%xmm7, %xmm8

#ifndef TRMMKERNEL
	addsd	%xmm0, %xmm8
#endif

	movsd	%xmm8,  (CO1)
	ALIGN_4

.L999:
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
