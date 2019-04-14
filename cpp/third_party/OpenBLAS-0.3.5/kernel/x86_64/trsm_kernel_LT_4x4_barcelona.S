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

#define OFFSET	 48(%rsp)
#define AORIG	 56(%rsp)
#define KK	 64(%rsp)
#define KKK	 72(%rsp)

#else

#define STACKSIZE 256

#define OLD_A		40 + STACKSIZE(%rsp)
#define OLD_B		48 + STACKSIZE(%rsp)
#define OLD_C		56 + STACKSIZE(%rsp)
#define OLD_LDC		64 + STACKSIZE(%rsp)
#define OLD_OFFSET	72 + STACKSIZE(%rsp)

#define OFFSET	224(%rsp)
#define AORIG	232(%rsp)
#define KK	240(%rsp)
#define KKK	248(%rsp)

#endif

#define PREFETCH     prefetch
#define PREFETCHSIZE  (8 *  7 + 0)

#define movlpd	movsd
#define movapd	movups
#define movupd	movups

#define KERNEL1(xx) \
	mulpd	%xmm1, %xmm0 ;\
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm1 ;\
	addpd	%xmm0, %xmm8 ;\
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
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm1 ;\
	addpd	%xmm0, %xmm8 ;\
	movapd	%xmm2, %xmm0 ;\
/**/	movapd	  (AO, %rax, 4), %xmm6 ;\
	addpd	%xmm1, %xmm12 ;\
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
	mulpd	%xmm3, %xmm2 ;\
/**/	movddup	  (BO, %rax, 4), %xmm1 ;\
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm3 ;\
	addpd	%xmm2, %xmm11 ;\
	addpd	%xmm3, %xmm15 ;\
 	movddup	 -7 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm4, %xmm2

#define KERNEL3(xx) \
	mulpd	%xmm5, %xmm4 ;\
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm5 ;\
	addpd	%xmm4, %xmm8 ;\
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
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm5 ;\
	addpd	%xmm4, %xmm8 ;\
	movapd	%xmm2, %xmm4 ;\
/**/	movapd	  8 * SIZE(AO, %rax, 4), %xmm7 ;\
	addpd	%xmm5, %xmm12 ;\
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
	mulpd	  2 * SIZE(AO, %rax, 4), %xmm1 ;\
	addpd	%xmm6, %xmm8 ;\
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
	mulpd	  6 * SIZE(AO, %rax, 4), %xmm1 ;\
	addpd	%xmm6, %xmm8 ;\
	movapd	%xmm2, %xmm6 ;\
/***/	movapd	 16 * SIZE(AO, %rax, 4), %xmm0 ;\
	addpd	%xmm1, %xmm12 ;\
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
	mulpd	 10 * SIZE(AO, %rax, 4), %xmm5 ;\
	addpd	%xmm7, %xmm8 ;\
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
	mulpd	 14 * SIZE(AO, %rax, 4), %xmm5 ;\
	addpd	%xmm7, %xmm8 ;\
	movapd	%xmm2, %xmm7 ;\
/**/	movapd	 24 * SIZE(AO, %rax, 4), %xmm4 ;\
	addpd	%xmm5, %xmm12 ;\
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
	addq	$8 * SIZE, %rax

#define KERNEL_SUB1(xx) \
	mulpd	%xmm1, %xmm0 ;\
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm1 ;\
	addpd	%xmm0, %xmm8 ;\
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

#define KERNEL_SUB2(xx) \
	mulpd	%xmm1, %xmm0 ;\
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm1 ;\
	addpd	%xmm0, %xmm8 ;\
	movapd	%xmm2, %xmm0 ;\
	addpd	%xmm1, %xmm12 ;\
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
	movapd	  (AO, %rax, 4), %xmm0 ;\
	addpd	%xmm1, %xmm14 ;\
	movddup	  (BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm3 ;\
	addpd	%xmm2, %xmm11 ;\
	addpd	%xmm3, %xmm15 ;\
 	movddup	 -7 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm4, %xmm2

#define KERNEL_SUB3(xx) \
	mulpd	%xmm5, %xmm4 ;\
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm5 ;\
	addpd	%xmm4, %xmm8 ;\
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

#define KERNEL_SUB4(xx) \
	mulpd	%xmm5, %xmm4 ;\
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm5 ;\
	addpd	%xmm4, %xmm8 ;\
	movapd	%xmm2, %xmm4 ;\
	addpd	%xmm5, %xmm12 ;\
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
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm3 ;\
	addpd	%xmm2, %xmm11 ;\
	addpd	%xmm3, %xmm15 ;\
 	movddup	  1 * SIZE(BO, %rax, 4), %xmm3 ;\
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
	movsd	OLD_OFFSET, %xmm12
#else
	movq	STACKSIZE +  8(%rsp), LDC
	movsd	STACKSIZE + 16(%rsp), %xmm12
#endif

	movq	OLD_M, M
	movq	OLD_N, N

	subq	$-16 * SIZE, A
	subq	$-16 * SIZE, B

	movsd	%xmm12, OFFSET
	movsd	%xmm12, KK

	leaq	(, LDC, SIZE), LDC

#ifdef LN
       leaq	(, M, SIZE), %rax
       addq	%rax, C
       imulq	K, %rax
       addq	%rax, A
#endif

#ifdef RT
       leaq	(, N, SIZE), %rax
       imulq	K, %rax
       addq	%rax, B
       movq	N, %rax
       imulq	LDC, %rax
       addq	%rax, C
#endif

#ifdef RN
	negq	KK
#endif

#ifdef RT
       movq	N, %rax
       subq	OFFSET, %rax
       movq	%rax, KK
#endif

	movq	N,  J
	sarq	$2, J		# j = (n >> 2)
	jle	.L40

.L01:
#if defined(LT) || defined(RN)
	movq	A, AO
#else
	movq	A, AORIG
#endif

#ifdef RT
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       subq	%rax, B

       leaq	(, LDC, 4), %rax
       subq	%rax, C
#endif

	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc
#ifndef RT
	leaq	(C, LDC, 4), C
#endif

#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

	movq	K, %rax
	salq	$BASE_SHIFT + 2, %rax
	leaq	(B, %rax), BB

#if defined(LT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L20
	ALIGN_4

.L11:
#ifdef LN
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
#endif

	movq	B, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(BO, %rax, 4), BO
#endif

	movapd	-16 * SIZE(AO), %xmm0
	movddup	-16 * SIZE(BO), %xmm1
	pxor	%xmm8, %xmm8
 	movddup	-15 * SIZE(BO), %xmm3
	pxor	%xmm9, %xmm9
	movapd	 -8 * SIZE(AO), %xmm4
	pxor	%xmm10, %xmm10
	movddup	 -8 * SIZE(BO), %xmm5
	pxor	%xmm11, %xmm11

#ifndef LN
	prefetchw      3 * SIZE(CO1)
	pxor	%xmm12, %xmm12
	prefetchw      7 * SIZE(CO2)
	pxor	%xmm13, %xmm13
	prefetchw      3 * SIZE(CO1, LDC, 2)
	pxor	%xmm14, %xmm14
	prefetchw      7 * SIZE(CO2, LDC, 2)
	pxor	%xmm15, %xmm15
	movapd	%xmm0, %xmm2
#else
	prefetchw     -8 * SIZE(CO1)
	pxor	%xmm12, %xmm12
	prefetchw     -8 * SIZE(CO2)
	pxor	%xmm13, %xmm13
	prefetchw     -8 * SIZE(CO1, LDC, 2)
	pxor	%xmm14, %xmm14
	prefetchw     -8 * SIZE(CO2, LDC, 2)
	pxor	%xmm15, %xmm15
	movapd	%xmm0, %xmm2
#endif

	prefetch	 -16 * SIZE(BB)

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif

	andq	$-8, %rax
	leaq	(, %rax, SIZE), %rax
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
	BRANCH
	jl	.L12
	ALIGN_4

.L15:
	prefetch	  -8 * SIZE(BB)
	subq		 $-16 * SIZE, BB

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
#endif
	testq	$4, %rax
	je .L16
	xorq	%rax, %rax
	ALIGN_4

	KERNEL_SUB1(16 *  0)
	KERNEL_SUB2(16 *  0)
	KERNEL_SUB3(16 *  0)
	KERNEL_SUB4(16 *  0)

	subq	$-16 * SIZE, BO
	subq	$-16 * SIZE, AO
	ALIGN_4

.L16:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
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
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$4, %rax
#else
	subq	$4, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm8, %xmm0
	unpcklpd %xmm9, %xmm8
	unpckhpd %xmm9, %xmm0

	movapd	%xmm10, %xmm2
	unpcklpd %xmm11, %xmm10
	unpckhpd %xmm11, %xmm2

	movapd	%xmm12, %xmm4
	unpcklpd %xmm13, %xmm12
	unpckhpd %xmm13, %xmm4

	movapd	%xmm14, %xmm6
	unpcklpd %xmm15, %xmm14
	unpckhpd %xmm15, %xmm6

	movapd	-16 * SIZE(BO), %xmm9
	movapd	-14 * SIZE(BO), %xmm11
	movapd	-12 * SIZE(BO), %xmm13
	movapd	-10 * SIZE(BO), %xmm15
	movapd	 -8 * SIZE(BO), %xmm1
	movapd	 -6 * SIZE(BO), %xmm3
	movapd	 -4 * SIZE(BO), %xmm5
	movapd	 -2 * SIZE(BO), %xmm7

	subpd	%xmm8,  %xmm9
	subpd	%xmm10, %xmm11
	subpd	%xmm0,  %xmm13
	subpd	%xmm2,  %xmm15
	subpd	%xmm12, %xmm1
	subpd	%xmm14, %xmm3
	subpd	%xmm4,  %xmm5
	subpd	%xmm6,  %xmm7
#else
	movapd	-16 * SIZE(AO), %xmm0
	movapd	-14 * SIZE(AO), %xmm1
	movapd	-12 * SIZE(AO), %xmm2
	movapd	-10 * SIZE(AO), %xmm3

	movapd	 -8 * SIZE(AO), %xmm4
	movapd	 -6 * SIZE(AO), %xmm5
	movapd	 -4 * SIZE(AO), %xmm6
	movapd	 -2 * SIZE(AO), %xmm7

	subpd	%xmm8,  %xmm0
	subpd	%xmm12, %xmm1
	subpd	%xmm9,  %xmm2
	subpd	%xmm13, %xmm3
	subpd	%xmm10, %xmm4
	subpd	%xmm14, %xmm5
	subpd	%xmm11, %xmm6
	subpd	%xmm15, %xmm7
#endif

#ifdef LN
	movddup	 -1 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm5
	mulpd	 %xmm8, %xmm7

	movddup	 -2 * SIZE(AO), %xmm10
	mulpd	 %xmm5, %xmm10
	subpd	 %xmm10, %xmm1
	movddup	 -2 * SIZE(AO), %xmm10
	mulpd	 %xmm7, %xmm10
	subpd	 %xmm10, %xmm3

	movddup	 -3 * SIZE(AO), %xmm12
	mulpd	 %xmm5, %xmm12
	subpd	 %xmm12, %xmm13
	movddup	 -3 * SIZE(AO), %xmm12
	mulpd	 %xmm7, %xmm12
	subpd	 %xmm12, %xmm15

	movddup	 -4 * SIZE(AO), %xmm14
	mulpd	 %xmm5, %xmm14
	subpd	 %xmm14, %xmm9
	movddup	 -4 * SIZE(AO), %xmm14
	mulpd	 %xmm7, %xmm14
	subpd	 %xmm14, %xmm11

	movddup	 -6 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm1
	mulpd	 %xmm8, %xmm3

	movddup	 -7 * SIZE(AO), %xmm10
	mulpd	 %xmm1, %xmm10
	subpd	 %xmm10, %xmm13
	movddup	 -7 * SIZE(AO), %xmm10
	mulpd	 %xmm3, %xmm10
	subpd	 %xmm10, %xmm15

	movddup	 -8 * SIZE(AO), %xmm12
	mulpd	 %xmm1, %xmm12
	subpd	 %xmm12, %xmm9
	movddup	 -8 * SIZE(AO), %xmm12
	mulpd	 %xmm3, %xmm12
	subpd	 %xmm12, %xmm11

	movddup	-11 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm13
	mulpd	 %xmm8, %xmm15

	movddup	-12 * SIZE(AO), %xmm10
	mulpd	 %xmm13, %xmm10
	subpd	 %xmm10, %xmm9
	movddup	-12 * SIZE(AO), %xmm10
	mulpd	 %xmm15, %xmm10
	subpd	 %xmm10, %xmm11

	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm9
	mulpd	 %xmm8, %xmm11
#endif

#ifdef LT
	movddup -16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm9
	mulpd	 %xmm8, %xmm11

	movddup	-15 * SIZE(AO), %xmm10
	mulpd	 %xmm9, %xmm10
	subpd	 %xmm10, %xmm13

	movddup	-15 * SIZE(AO), %xmm10
	mulpd	 %xmm11, %xmm10
	subpd	 %xmm10, %xmm15

	movddup	-14 * SIZE(AO), %xmm12
	mulpd	 %xmm9, %xmm12
	subpd	 %xmm12, %xmm1
	movddup	-14 * SIZE(AO), %xmm12
	mulpd	 %xmm11, %xmm12
	subpd	 %xmm12, %xmm3

	movddup	-13 * SIZE(AO), %xmm14
	mulpd	 %xmm9, %xmm14
	subpd	 %xmm14, %xmm5
	movddup	-13 * SIZE(AO), %xmm14
	mulpd	 %xmm11, %xmm14
	subpd	 %xmm14, %xmm7

	movddup	-11 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm13
	mulpd	 %xmm8, %xmm15

	movddup	-10 * SIZE(AO), %xmm10
	mulpd	 %xmm13, %xmm10
	subpd	 %xmm10, %xmm1
	movddup	-10 * SIZE(AO), %xmm10
	mulpd	 %xmm15, %xmm10
	subpd	 %xmm10, %xmm3

	movddup	 -9 * SIZE(AO), %xmm12
	mulpd	 %xmm13, %xmm12
	subpd	 %xmm12, %xmm5
	movddup	 -9 * SIZE(AO), %xmm12
	mulpd	 %xmm15, %xmm12
	subpd	 %xmm12, %xmm7

	movddup	 -6 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm1
	mulpd	 %xmm8, %xmm3

	movddup	 -5 * SIZE(AO), %xmm10
	mulpd	 %xmm1, %xmm10
	subpd	 %xmm10, %xmm5
	movddup	 -5 * SIZE(AO), %xmm10
	mulpd	 %xmm3, %xmm10
	subpd	 %xmm10, %xmm7

	movddup	 -1 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm5
	mulpd	 %xmm8, %xmm7
#endif

#ifdef RN
	movddup	-16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm0
	mulpd	 %xmm8, %xmm1

	movddup	-15 * SIZE(BO), %xmm9
	mulpd	 %xmm0, %xmm9
	subpd	 %xmm9, %xmm2
	movddup	-15 * SIZE(BO), %xmm9
	mulpd	 %xmm1, %xmm9
	subpd	 %xmm9, %xmm3

	movddup	-14 * SIZE(BO), %xmm10
	mulpd	 %xmm0, %xmm10
	subpd	 %xmm10, %xmm4
	movddup	-14 * SIZE(BO), %xmm10
	mulpd	 %xmm1, %xmm10
	subpd	 %xmm10, %xmm5

	movddup	 -13 * SIZE(BO), %xmm11
	mulpd	 %xmm0, %xmm11
	subpd	 %xmm11, %xmm6
	movddup	 -13 * SIZE(BO), %xmm11
	mulpd	 %xmm1, %xmm11
	subpd	 %xmm11, %xmm7

	movddup	 -11 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm2
	mulpd	 %xmm8, %xmm3

	movddup	 -10 * SIZE(BO), %xmm9
	mulpd	 %xmm2, %xmm9
	subpd	 %xmm9, %xmm4
	movddup	 -10 * SIZE(BO), %xmm9
	mulpd	 %xmm3, %xmm9
	subpd	 %xmm9, %xmm5

	movddup	  -9 * SIZE(BO), %xmm10
	mulpd	 %xmm2, %xmm10
	subpd	 %xmm10, %xmm6
	movddup	  -9 * SIZE(BO), %xmm10
	mulpd	 %xmm3, %xmm10
	subpd	 %xmm10, %xmm7

	movddup	 -6 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm4
	mulpd	 %xmm8, %xmm5

	movddup	 -5 * SIZE(BO), %xmm9
	mulpd	 %xmm4, %xmm9
	subpd	 %xmm9, %xmm6
	movddup	 -5 * SIZE(BO), %xmm9
	mulpd	 %xmm5, %xmm9
	subpd	 %xmm9, %xmm7

	movddup	 -1 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm6
	mulpd	 %xmm8, %xmm7
#endif

#ifdef RT
	movddup	 -1 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm6
	mulpd	 %xmm8, %xmm7

	movddup	 -2 * SIZE(BO), %xmm9
	mulpd	 %xmm6, %xmm9
	subpd	 %xmm9, %xmm4
	movddup	 -2 * SIZE(BO), %xmm9
	mulpd	 %xmm7, %xmm9
	subpd	 %xmm9, %xmm5

	movddup	 -3 * SIZE(BO), %xmm10
	mulpd	 %xmm6, %xmm10
	subpd	 %xmm10, %xmm2
	movddup	 -3 * SIZE(BO), %xmm10
	mulpd	 %xmm7, %xmm10
	subpd	 %xmm10, %xmm3

	movddup	 -4 * SIZE(BO), %xmm11
	mulpd	 %xmm6, %xmm11
	subpd	 %xmm11, %xmm0
	movddup	 -4 * SIZE(BO), %xmm11
	mulpd	 %xmm7, %xmm11
	subpd	 %xmm11, %xmm1

	movddup	 -6 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm4
	mulpd	 %xmm8, %xmm5

	movddup	 -7 * SIZE(BO), %xmm9
	mulpd	 %xmm4, %xmm9
	subpd	 %xmm9, %xmm2
	movddup	 -7 * SIZE(BO), %xmm9
	mulpd	 %xmm5, %xmm9
	subpd	 %xmm9, %xmm3

	movddup	 -8 * SIZE(BO), %xmm10
	mulpd	 %xmm4, %xmm10
	subpd	 %xmm10, %xmm0
	movddup	 -8 * SIZE(BO), %xmm10
	mulpd	 %xmm5, %xmm10
	subpd	 %xmm10, %xmm1

	movddup	-11 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm2
	mulpd	 %xmm8, %xmm3

	movddup	-12 * SIZE(BO), %xmm9
	mulpd	 %xmm2, %xmm9
	subpd	 %xmm9, %xmm0
	movddup	-12 * SIZE(BO), %xmm9
	mulpd	 %xmm3, %xmm9
	subpd	 %xmm9, %xmm1

	movddup	-16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm0
	mulpd	 %xmm8, %xmm1
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movlpd	%xmm9,  0 * SIZE(CO1)
	movlpd	%xmm13, 1 * SIZE(CO1)
	movlpd	%xmm1,  2 * SIZE(CO1)
	movlpd	%xmm5,  3 * SIZE(CO1)

	movhpd	%xmm9,  0 * SIZE(CO2)
	movhpd	%xmm13, 1 * SIZE(CO2)
	movhpd	%xmm1,  2 * SIZE(CO2)
	movhpd	%xmm5,  3 * SIZE(CO2)

	movlpd	%xmm11, 0 * SIZE(CO1, LDC, 2)
	movlpd	%xmm15, 1 * SIZE(CO1, LDC, 2)
	movlpd	%xmm3,  2 * SIZE(CO1, LDC, 2)
	movlpd	%xmm7,  3 * SIZE(CO1, LDC, 2)

	movhpd	%xmm11, 0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm15, 1 * SIZE(CO2, LDC, 2)
	movhpd	%xmm3,  2 * SIZE(CO2, LDC, 2)
	movhpd	%xmm7,  3 * SIZE(CO2, LDC, 2)
#else
	movlpd	%xmm0,  0 * SIZE(CO1)
	movhpd	%xmm0,  1 * SIZE(CO1)
	movlpd	%xmm1,  2 * SIZE(CO1)
	movhpd	%xmm1,  3 * SIZE(CO1)

	movlpd	%xmm2,  0 * SIZE(CO2)
	movhpd	%xmm2,  1 * SIZE(CO2)
	movlpd	%xmm3,  2 * SIZE(CO2)
	movhpd	%xmm3,  3 * SIZE(CO2)

	movlpd	%xmm4,  0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm4,  1 * SIZE(CO1, LDC, 2)
	movlpd	%xmm5,  2 * SIZE(CO1, LDC, 2)
	movhpd	%xmm5,  3 * SIZE(CO1, LDC, 2)

	movlpd	%xmm6,  0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm6,  1 * SIZE(CO2, LDC, 2)
	movlpd	%xmm7,  2 * SIZE(CO2, LDC, 2)
	movhpd	%xmm7,  3 * SIZE(CO2, LDC, 2)
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm9,  -16 * SIZE(BO)
	movaps	%xmm11, -14 * SIZE(BO)
	movaps	%xmm13, -12 * SIZE(BO)
	movaps	%xmm15, -10 * SIZE(BO)
	movaps	%xmm1,   -8 * SIZE(BO)
	movaps	%xmm3,   -6 * SIZE(BO)
	movaps	%xmm5,   -4 * SIZE(BO)
	movaps	%xmm7,   -2 * SIZE(BO)
#else
	movaps	%xmm0,  -16 * SIZE(AO)
	movaps	%xmm1,  -14 * SIZE(AO)
	movaps	%xmm2,  -12 * SIZE(AO)
	movaps	%xmm3,  -10 * SIZE(AO)
	movaps	%xmm4,   -8 * SIZE(AO)
	movaps	%xmm5,   -6 * SIZE(AO)
	movaps	%xmm6,   -4 * SIZE(AO)
	movaps	%xmm7,   -2 * SIZE(AO)
#endif

#ifndef LN
	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

#ifdef LN
	subq	$4, KK
#endif

#ifdef LT
	addq	$4, KK
#endif

#ifdef RT
	movq	K, %rax
	salq	$2 + BASE_SHIFT, %rax
	addq	%rax, AORIG
#endif

	decq	I			# i --
	jg	.L11
	ALIGN_4

.L20:
	testq	$3, M
	je	.L39

	testq	$2, M
	je	.L30
	ALIGN_4

.L21:
#ifdef LN
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
#endif

	movq	B, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(BO, %rax, 4), BO
#endif

	movapd	-16 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movapd	-12 * SIZE(AO), %xmm2
	pxor	%xmm9, %xmm9
	movddup	-16 * SIZE(BO), %xmm1
	pxor	%xmm10, %xmm10
	movddup	-15 * SIZE(BO), %xmm5
	pxor	%xmm11, %xmm11
	movddup	 -8 * SIZE(BO), %xmm3

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
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
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
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
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$2, %rax
#else
	subq	$4, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movapd	 %xmm8,  %xmm0
	unpcklpd %xmm9,  %xmm8
	unpckhpd %xmm9,  %xmm0

	movapd	 %xmm10, %xmm2
	unpcklpd %xmm11, %xmm10
	unpckhpd %xmm11, %xmm2

	movapd	-16 * SIZE(BO), %xmm9
	movapd	-14 * SIZE(BO), %xmm11
	movapd	-12 * SIZE(BO), %xmm13
	movapd	-10 * SIZE(BO), %xmm15

	subpd	%xmm8,  %xmm9
	subpd	%xmm10, %xmm11
	subpd	%xmm0,  %xmm13
	subpd	%xmm2,  %xmm15
#else
	movapd	-16 * SIZE(AO), %xmm0
	movapd	-14 * SIZE(AO), %xmm2
	movapd	-12 * SIZE(AO), %xmm4
	movapd	-10 * SIZE(AO), %xmm6

	subpd	%xmm8,  %xmm0
	subpd	%xmm9,  %xmm2
	subpd	%xmm10, %xmm4
	subpd	%xmm11, %xmm6
#endif

#ifdef LN
	movddup	-13 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm13
	mulpd	 %xmm8, %xmm15

	movddup	-14 * SIZE(AO), %xmm10
	mulpd	 %xmm13, %xmm10
	subpd	 %xmm10, %xmm9
	movddup	-14 * SIZE(AO), %xmm10
	mulpd	 %xmm15, %xmm10
	subpd	 %xmm10, %xmm11

	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm9
	mulpd	 %xmm8, %xmm11
#endif

#ifdef LT
	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm9
	mulpd	 %xmm8, %xmm11

	movddup	-15 * SIZE(AO), %xmm10
	mulpd	 %xmm9, %xmm10
	subpd	 %xmm10, %xmm13
	movddup	-15 * SIZE(AO), %xmm10
	mulpd	 %xmm11, %xmm10
	subpd	 %xmm10, %xmm15

	movddup	-13 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm13
	mulpd	 %xmm8, %xmm15
#endif

#ifdef RN
	movddup	-16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm0

	movddup	-15 * SIZE(BO), %xmm9
	mulpd	 %xmm0, %xmm9
	subpd	 %xmm9, %xmm2
	movddup	-14 * SIZE(BO), %xmm10
	mulpd	 %xmm0, %xmm10
	subpd	 %xmm10, %xmm4
	movddup	-13 * SIZE(BO), %xmm11
	mulpd	 %xmm0, %xmm11
	subpd	 %xmm11, %xmm6

	movddup	-11 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm2
	movddup	-10 * SIZE(BO), %xmm9
	mulpd	 %xmm2, %xmm9
	subpd	 %xmm9, %xmm4
	movddup	 -9 * SIZE(BO), %xmm10
	mulpd	 %xmm2, %xmm10
	subpd	 %xmm10, %xmm6

	movddup	 -6 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm4

	movddup	 -5 * SIZE(BO), %xmm9
	mulpd	 %xmm4, %xmm9
	subpd	 %xmm9, %xmm6

	movddup	 -1 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm6
#endif

#ifdef RT
	movddup	 -1 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm6

	movddup	 -2 * SIZE(BO), %xmm9
	mulpd	 %xmm6, %xmm9
	subpd	 %xmm9, %xmm4
	movddup	 -3 * SIZE(BO), %xmm10
	mulpd	 %xmm6, %xmm10
	subpd	 %xmm10, %xmm2
	movddup	 -4 * SIZE(BO), %xmm11
	mulpd	 %xmm6, %xmm11
	subpd	 %xmm11, %xmm0

	movddup	 -6 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm4
	movddup	 -7 * SIZE(BO), %xmm9
	mulpd	 %xmm4, %xmm9
	subpd	 %xmm9, %xmm2
	movddup	 -8 * SIZE(BO), %xmm10
	mulpd	 %xmm4, %xmm10
	subpd	 %xmm10, %xmm0

	movddup	-11 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm2
	movddup	-12 * SIZE(BO), %xmm9
	mulpd	 %xmm2, %xmm9
	subpd	 %xmm9, %xmm0

	movddup	-16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm0
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movlpd	%xmm9,   0 * SIZE(CO1)
	movlpd	%xmm13,  1 * SIZE(CO1)

	movhpd	%xmm9,   0 * SIZE(CO2)
	movhpd	%xmm13,  1 * SIZE(CO2)

	movlpd	%xmm11,  0 * SIZE(CO1, LDC, 2)
	movlpd	%xmm15,  1 * SIZE(CO1, LDC, 2)

	movhpd	%xmm11,  0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm15,  1 * SIZE(CO2, LDC, 2)
#else
	movlpd	%xmm0,  0 * SIZE(CO1)
	movhpd	%xmm0,  1 * SIZE(CO1)

	movlpd	%xmm2,  0 * SIZE(CO2)
	movhpd	%xmm2,  1 * SIZE(CO2)

	movlpd	%xmm4,  0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm4,  1 * SIZE(CO1, LDC, 2)

	movlpd	%xmm6,  0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm6,  1 * SIZE(CO2, LDC, 2)
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm9,  -16 * SIZE(BO)
	movaps	%xmm11, -14 * SIZE(BO)
	movaps	%xmm13, -12 * SIZE(BO)
	movaps	%xmm15, -10 * SIZE(BO)
#else
	movaps	%xmm0,  -16 * SIZE(AO)
	movaps	%xmm2,  -14 * SIZE(AO)
	movaps	%xmm4,  -12 * SIZE(AO)
	movaps	%xmm6,  -10 * SIZE(AO)
#endif

#ifndef LN
	addq	$2 * SIZE, CO1
	addq	$2 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

#ifdef LN
	subq	$2, KK
#endif

#ifdef LT
	addq	$2, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L30:
	testq	$1, M
	je	.L39

#ifdef LN
       movq	K, %rax
       salq	$0 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
#endif

	movq	B, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(BO, %rax, 4), BO
#endif

	movddup	-16 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movddup	-14 * SIZE(AO), %xmm2
	pxor	%xmm9, %xmm9
	movddup	-15 * SIZE(AO), %xmm4
	pxor	%xmm10, %xmm10
	movapd	-16 * SIZE(BO), %xmm1
	pxor	%xmm11, %xmm11
	movapd	 -8 * SIZE(BO), %xmm3

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
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
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
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

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$1, %rax
#else
	subq	$4, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 4), BO
#endif

#if defined(LN) || defined(LT)
	movapd	-16 * SIZE(BO), %xmm2
	movapd	-14 * SIZE(BO), %xmm3

	subpd	%xmm8,  %xmm2
	subpd	%xmm9,  %xmm3
#else
	movapd	-16 * SIZE(AO), %xmm2
	movapd	-14 * SIZE(AO), %xmm3

	subpd	%xmm8, %xmm2
	subpd	%xmm9, %xmm3
#endif

#if defined(LN) || defined(LT)
	movddup	-16 * SIZE(AO), %xmm0
	mulpd	 %xmm0, %xmm2
	mulpd	 %xmm0, %xmm3
#endif

#ifdef RN
	movapd	%xmm2, %xmm0
        unpckhpd %xmm0, %xmm0

	movapd	%xmm3, %xmm1
        unpckhpd %xmm1, %xmm1

	movsd	-16 * SIZE(BO), %xmm4
	mulsd	 %xmm4, %xmm2

	movsd	-15 * SIZE(BO), %xmm5
	mulsd	 %xmm2, %xmm5
	subsd	 %xmm5, %xmm0
	movsd	-14 * SIZE(BO), %xmm6
	mulsd	 %xmm2, %xmm6
	subsd	 %xmm6, %xmm3
	movsd	-13 * SIZE(BO), %xmm7
	mulsd	 %xmm2, %xmm7
	subsd	 %xmm7, %xmm1

	movsd	-11 * SIZE(BO), %xmm4
	mulsd	 %xmm4, %xmm0

	movsd	-10 * SIZE(BO), %xmm5
	mulsd	 %xmm0, %xmm5
	subsd	 %xmm5, %xmm3
	movsd	 -9 * SIZE(BO), %xmm6
	mulsd	 %xmm0, %xmm6
	subsd	 %xmm6, %xmm1

	movsd	 -6 * SIZE(BO), %xmm4
	mulsd	 %xmm4, %xmm3

	movsd	 -5 * SIZE(BO), %xmm5
	mulsd	 %xmm3, %xmm5
	subsd	 %xmm5, %xmm1

	movsd	 -1 * SIZE(BO), %xmm4
	mulsd	 %xmm4, %xmm1

	unpcklpd %xmm0, %xmm2
	unpcklpd %xmm1, %xmm3
#endif

#ifdef RT
	movapd	%xmm2, %xmm0
        unpckhpd %xmm0, %xmm0

	movapd	%xmm3, %xmm1
        unpckhpd %xmm1, %xmm1

	movsd	 -1 * SIZE(BO), %xmm4
	mulsd	 %xmm4, %xmm1

	movsd	 -2 * SIZE(BO), %xmm5
	mulsd	 %xmm1, %xmm5
	subsd	 %xmm5, %xmm3
	movsd	 -3 * SIZE(BO), %xmm6
	mulsd	 %xmm1, %xmm6
	subsd	 %xmm6, %xmm0
	movsd	 -4 * SIZE(BO), %xmm7
	mulsd	 %xmm1, %xmm7
	subsd	 %xmm7, %xmm2

	movsd	 -6 * SIZE(BO), %xmm4
	mulsd	 %xmm4, %xmm3

	movsd	 -7 * SIZE(BO), %xmm5
	mulsd	 %xmm3, %xmm5
	subsd	 %xmm5, %xmm0
	movsd	 -8 * SIZE(BO), %xmm6
	mulsd	 %xmm3, %xmm6
	subsd	 %xmm6, %xmm2

	movsd	-11 * SIZE(BO), %xmm4
	mulsd	 %xmm4, %xmm0

	movsd	-12 * SIZE(BO), %xmm5
	mulsd	 %xmm0, %xmm5
	subsd	 %xmm5, %xmm2

	movsd	-16 * SIZE(BO), %xmm4
	mulsd	 %xmm4, %xmm2

	unpcklpd %xmm0, %xmm2
	unpcklpd %xmm1, %xmm3

#endif

#ifdef LN
	subq	$1 * SIZE, CO1
	subq	$1 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movlpd	%xmm2,  0 * SIZE(CO1)
	movhpd	%xmm2,  0 * SIZE(CO2)
	movlpd	%xmm3,  0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm3,  0 * SIZE(CO2, LDC, 2)
#else
	movlpd	%xmm2,  0 * SIZE(CO1)
	movhpd	%xmm2,  0 * SIZE(CO2)
	movlpd	%xmm3,  0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm3,  0 * SIZE(CO2, LDC, 2)
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm2, -16 * SIZE(BO)
	movaps	%xmm3, -14 * SIZE(BO)
#else
	movaps	%xmm2, -16 * SIZE(AO)
	movaps	%xmm3, -14 * SIZE(AO)
#endif

#ifndef LN
	addq	$1 * SIZE, CO1
	addq	$1 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 4), BO
#endif

#ifdef LN
	subq	$1, KK
#endif

#ifdef LT
	addq	$1, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$0 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L39:
#ifdef LN
       leaq	(, K, SIZE), %rax
       leaq	(B, %rax, 4), B
#endif

#if defined(LT) || defined(RN)
	movq	BO, B
#endif

#ifdef RN
	addq	$4, KK
#endif

#ifdef RT
	subq	$4, KK
#endif

	decq	J			# j --
	jg	.L01
	ALIGN_4

.L40:
	testq	$2, N
	je	.L80

#if defined(LT) || defined(RN)
	movq	A, AO
#else
	movq	A, AORIG
#endif

#ifdef RT
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       subq	%rax, B

       leaq	(, LDC, 2), %rax
       subq	%rax, C
#endif

	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc
#ifndef RT
	leaq	(C, LDC, 2), C
#endif

#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

#if defined(LT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L60
	ALIGN_4

.L51:
#ifdef LN
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
#endif

	movq	B, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(BO, %rax, 2), BO
#endif

	movddup	-16 * SIZE(BO), %xmm1
	movddup	-15 * SIZE(BO), %xmm5
	pxor	%xmm8, %xmm8
	movddup	-12 * SIZE(BO), %xmm3
	pxor	%xmm9, %xmm9
	movapd	-16 * SIZE(AO), %xmm0
	pxor	%xmm12, %xmm12
	movapd	 -8 * SIZE(AO), %xmm4
	pxor	%xmm13, %xmm13

#ifndef LN
	prefetchw      3 * SIZE(CO1)
	movapd	%xmm0, %xmm2
	prefetchw      3 * SIZE(CO2)
#else
	prefetchw     -8 * SIZE(CO1)
	movapd	%xmm0, %xmm2
	prefetchw     -8 * SIZE(CO2)
#endif


#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
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
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
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
#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$4, %rax
#else
	subq	$2, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 2), BO
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm8, %xmm0
	unpcklpd %xmm9, %xmm8
	unpckhpd %xmm9, %xmm0

	movapd	%xmm12, %xmm4
	unpcklpd %xmm13, %xmm12
	unpckhpd %xmm13, %xmm4

	movapd	-16 * SIZE(BO), %xmm9
	movapd	-14 * SIZE(BO), %xmm13
	movapd	-12 * SIZE(BO), %xmm1
	movapd	-10 * SIZE(BO), %xmm5

	subpd	%xmm8,  %xmm9
	subpd	%xmm0,  %xmm13
	subpd	%xmm12, %xmm1
	subpd	%xmm4,  %xmm5
#else
	movapd	-16 * SIZE(AO), %xmm0
	movapd	-14 * SIZE(AO), %xmm1
	movapd	-12 * SIZE(AO), %xmm2
	movapd	-10 * SIZE(AO), %xmm3

	subpd	%xmm8, %xmm0
	subpd	%xmm12, %xmm1
	subpd	%xmm9, %xmm2
	subpd	%xmm13, %xmm3
#endif

#ifdef LN
	movddup	 -1 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm5
	movddup	 -2 * SIZE(AO), %xmm10
	mulpd	 %xmm5, %xmm10
	subpd	 %xmm10, %xmm1
	movddup	 -3 * SIZE(AO), %xmm12
	mulpd	 %xmm5, %xmm12
	subpd	 %xmm12, %xmm13
	movddup	 -4 * SIZE(AO), %xmm14
	mulpd	 %xmm5, %xmm14
	subpd	 %xmm14, %xmm9

	movddup	 -6 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm1
	movddup	 -7 * SIZE(AO), %xmm10
	mulpd	 %xmm1, %xmm10
	subpd	 %xmm10, %xmm13
	movddup	 -8 * SIZE(AO), %xmm12
	mulpd	 %xmm1, %xmm12
	subpd	 %xmm12, %xmm9

	movddup	-11 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm13
	movddup	-12 * SIZE(AO), %xmm10
	mulpd	 %xmm13, %xmm10
	subpd	 %xmm10, %xmm9

	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm9
#endif

#ifdef LT
	movddup -16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm9
	movddup	-15 * SIZE(AO), %xmm10
	mulpd	 %xmm9, %xmm10
	subpd	 %xmm10, %xmm13
	movddup	-14 * SIZE(AO), %xmm12
	mulpd	 %xmm9, %xmm12
	subpd	 %xmm12, %xmm1
	movddup	-13 * SIZE(AO), %xmm14
	mulpd	 %xmm9, %xmm14
	subpd	 %xmm14, %xmm5


	movddup	-11 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm13

	movddup	-10 * SIZE(AO), %xmm10
	mulpd	 %xmm13, %xmm10
	subpd	 %xmm10, %xmm1
	movddup	 -9 * SIZE(AO), %xmm12
	mulpd	 %xmm13, %xmm12
	subpd	 %xmm12, %xmm5

	movddup	 -6 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm1
	movddup	 -5 * SIZE(AO), %xmm10
	mulpd	 %xmm1, %xmm10
	subpd	 %xmm10, %xmm5

	movddup	 -1 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm5
#endif

#ifdef RN
	movddup	-16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm0
	mulpd	 %xmm8, %xmm1

	movddup	-15 * SIZE(BO), %xmm9
	mulpd	 %xmm0, %xmm9
	subpd	 %xmm9, %xmm2
	movddup	-15 * SIZE(BO), %xmm9
	mulpd	 %xmm1, %xmm9
	subpd	 %xmm9, %xmm3

	movddup	-13 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm2
	mulpd	 %xmm8, %xmm3
#endif

#ifdef RT
	movddup	-13 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm2
	mulpd	 %xmm8, %xmm3

	movddup	-14 * SIZE(BO), %xmm9
	mulpd	 %xmm2, %xmm9
	subpd	 %xmm9, %xmm0
	movddup	-14 * SIZE(BO), %xmm9
	mulpd	 %xmm3, %xmm9
	subpd	 %xmm9, %xmm1

	movddup	-16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm0
	mulpd	 %xmm8, %xmm1
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
	subq	$4 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movlpd	%xmm9,  0 * SIZE(CO1)
	movlpd	%xmm13, 1 * SIZE(CO1)
	movlpd	%xmm1,  2 * SIZE(CO1)
	movlpd	%xmm5,  3 * SIZE(CO1)

	movhpd	%xmm9,  0 * SIZE(CO2)
	movhpd	%xmm13, 1 * SIZE(CO2)
	movhpd	%xmm1,  2 * SIZE(CO2)
	movhpd	%xmm5,  3 * SIZE(CO2)
#else
	movlpd	%xmm0,  0 * SIZE(CO1)
	movhpd	%xmm0,  1 * SIZE(CO1)
	movlpd	%xmm1,  2 * SIZE(CO1)
	movhpd	%xmm1,  3 * SIZE(CO1)

	movlpd	%xmm2,  0 * SIZE(CO2)
	movhpd	%xmm2,  1 * SIZE(CO2)
	movlpd	%xmm3,  2 * SIZE(CO2)
	movhpd	%xmm3,  3 * SIZE(CO2)
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm9, -16 * SIZE(BO)
	movaps	%xmm13,-14 * SIZE(BO)
	movaps	%xmm1, -12 * SIZE(BO)
	movaps	%xmm5, -10 * SIZE(BO)
#else
	movaps	%xmm0, -16 * SIZE(AO)
	movaps	%xmm1, -14 * SIZE(AO)
	movaps	%xmm2, -12 * SIZE(AO)
	movaps	%xmm3, -10 * SIZE(AO)
#endif

#ifndef LN
	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

#ifdef LN
	subq	$4, KK
#endif

#ifdef LT
	addq	$4, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif

	decq	I			# i --
	jg	.L51
	ALIGN_4

.L60:
	testq	$2, M
	je	.L70

#ifdef LN
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
#endif

	movq	B, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(BO, %rax, 2), BO
#endif

	movapd	-16 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movapd	-12 * SIZE(AO), %xmm2
	pxor	%xmm9, %xmm9
	movddup	-16 * SIZE(BO), %xmm1
	pxor	%xmm10, %xmm10
	movddup	-15 * SIZE(BO), %xmm3
	pxor	%xmm11, %xmm11

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
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
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
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

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$2, %rax
#else
	subq	$2, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 2), BO
#endif

#if defined(LN) || defined(LT)
	movapd	%xmm8, %xmm0
	unpcklpd %xmm9, %xmm8
	unpckhpd %xmm9, %xmm0

	movapd	-16 * SIZE(BO), %xmm9
	movapd	-14 * SIZE(BO), %xmm13

	subpd	%xmm8,  %xmm9
	subpd	%xmm0,  %xmm13
#else
	movapd	-16 * SIZE(AO), %xmm0
	movapd	-14 * SIZE(AO), %xmm2

	subpd	%xmm8, %xmm0
	subpd	%xmm9, %xmm2
#endif


#ifdef LN
	movddup	-13 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm13

	movddup	-14 * SIZE(AO), %xmm10
	mulpd	 %xmm13, %xmm10
	subpd	 %xmm10, %xmm9

	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm9
#endif

#ifdef LT
	movddup	-16 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm9

	movddup	-15 * SIZE(AO), %xmm10
	mulpd	 %xmm9, %xmm10
	subpd	 %xmm10, %xmm13

	movddup	-13 * SIZE(AO), %xmm8
	mulpd	 %xmm8, %xmm13
#endif

#ifdef RN
	movddup	-16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm0

	movddup	-15 * SIZE(BO), %xmm9
	mulpd	 %xmm0, %xmm9
	subpd	 %xmm9, %xmm2

	movddup	-13 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm2
#endif

#ifdef RT
	movddup	-13 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm2

	movddup	-14 * SIZE(BO), %xmm9
	mulpd	 %xmm2, %xmm9
	subpd	 %xmm9, %xmm0

	movddup	-16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm0
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
	subq	$2 * SIZE, CO2
#endif

#if defined(LN) || defined(LT)
	movlpd	%xmm9,   0 * SIZE(CO1)
	movlpd	%xmm13,  1 * SIZE(CO1)

	movhpd	%xmm9,   0 * SIZE(CO2)
	movhpd	%xmm13,  1 * SIZE(CO2)
#else
	movlpd	%xmm0,   0 * SIZE(CO1)
	movhpd	%xmm0,   1 * SIZE(CO1)

	movlpd	%xmm2,   0 * SIZE(CO2)
	movhpd	%xmm2,   1 * SIZE(CO2)
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm9,  -16 * SIZE(BO)
	movaps	%xmm13, -14 * SIZE(BO)
#else
	movaps	%xmm0,  -16 * SIZE(AO)
	movaps	%xmm2,  -14 * SIZE(AO)
#endif

#ifndef LN
	addq	$2 * SIZE, CO1
	addq	$2 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
#endif

#ifdef LN
	subq	$2, KK
#endif

#ifdef LT
	addq	$2, KK
#endif

#ifdef RT
	movq	K, %rax
	salq	$1 + BASE_SHIFT, %rax
	addq	%rax, AORIG
#endif
	ALIGN_4

.L70:
	testq	$1, M
	je	.L79
	ALIGN_4

.L71:
#ifdef LN
       movq	K, %rax
       salq	$0 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
#endif

	movq	B, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	salq	$1 + BASE_SHIFT, %rax
	leaq	(BO, %rax, 1), BO
#endif

	movddup	-16 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movddup	-15 * SIZE(AO), %xmm1
	pxor	%xmm9, %xmm9
	movddup	-14 * SIZE(AO), %xmm2
	pxor	%xmm10, %xmm10
	movddup	-13 * SIZE(AO), %xmm3
	pxor	%xmm11, %xmm11

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
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
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
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

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$1, %rax
#else
	subq	$2, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 2), BO
#endif

#if defined(LN) || defined(LT)
	movapd	-16 * SIZE(BO), %xmm2
#else
	movapd	-16 * SIZE(AO), %xmm2
#endif

	subpd	%xmm8,  %xmm2

#if defined(LN) || defined(LT)
	movddup	-16 * SIZE(AO), %xmm0

	mulpd	 %xmm0, %xmm2
#endif

#ifdef RN
	movapd	%xmm2,  %xmm0
        unpckhpd %xmm0, %xmm0

	mulsd	-16 * SIZE(BO), %xmm2
	movsd	-15 * SIZE(BO), %xmm4
	mulsd	 %xmm2, %xmm4
	subsd	 %xmm4, %xmm0

	mulsd	-13 * SIZE(BO), %xmm0
	unpcklpd %xmm0, %xmm2
#endif

#ifdef RT
	movapd	%xmm2,  %xmm0
        unpckhpd %xmm0, %xmm0

	mulsd	-13 * SIZE(BO), %xmm0

	movlpd	-14 * SIZE(BO), %xmm4
	mulsd	 %xmm0, %xmm4
	subsd	 %xmm4, %xmm2

	mulsd	-16 * SIZE(BO), %xmm2
	unpcklpd %xmm0, %xmm2
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
	subq	$1 * SIZE, CO2
#endif

	movlpd	%xmm2,  0 * SIZE(CO1)
	movhpd	%xmm2,  0 * SIZE(CO2)

#if defined(LN) || defined(LT)
	movaps	%xmm2, -16 * SIZE(BO)
#else
	movaps	%xmm2, -16 * SIZE(AO)
#endif

#ifndef LN
	addq	$1 * SIZE, CO1
	addq	$1 * SIZE, CO2
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
#endif

#ifdef LN
	subq	$1, KK
#endif

#ifdef LT
	addq	$1, KK
#endif

#ifdef RT
	movq	K, %rax
	salq	$0 + BASE_SHIFT, %rax
	addq	%rax, AORIG
#endif
	ALIGN_4

.L79:
#ifdef LN
       leaq	(, K, SIZE), %rax
       leaq	(B, %rax, 2), B
#endif

#if defined(LT) || defined(RN)
	movq	BO, B
#endif

#ifdef RN
	addq	$2, KK
#endif

#ifdef RT
	subq	$2, KK
#endif
	ALIGN_4

.L80:
	testq	$1, N
	je	.L999

#if defined(LT) || defined(RN)
	movq	A, AO
#else
	movq	A, AORIG
#endif

#ifdef RT
	movq	K, %rax
	salq	$0 + BASE_SHIFT, %rax
	subq	%rax, B

	subq	LDC, C
#endif

	movq	C, CO1			# coffset1 = c
#ifndef RT
	addq	LDC, C
#endif

#ifdef LN
	movq	OFFSET, %rax
	addq	M, %rax
	movq	%rax, KK
#endif

#ifdef LT
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L100
	ALIGN_4

.L91:
#ifdef LN
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
#endif

	movq	B, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(BO, %rax, SIZE), BO
#endif

	movapd	-16 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movapd	 -8 * SIZE(AO), %xmm2
	pxor	%xmm9, %xmm9
	movddup	-16 * SIZE(BO), %xmm1
	pxor	%xmm10, %xmm10
	movddup	-15 * SIZE(BO), %xmm5
	pxor	%xmm11, %xmm11
	movddup	-14 * SIZE(BO), %xmm3

#ifndef LN
	prefetchw      3 * SIZE(CO1)
#else
	prefetchw     -8 * SIZE(CO1)
#endif

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
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
	addpd	%xmm1, %xmm9
	movddup	-12 * SIZE(BO, %rax, 1), %xmm1
	mulpd	%xmm5, %xmm0
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm5
	addpd	%xmm0, %xmm10
	movapd	  (AO, %rax, 4), %xmm0
	addpd	%xmm5, %xmm11
	movddup	-13 * SIZE(BO, %rax, 1), %xmm5
	mulpd	%xmm3, %xmm2
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm3
	addpd	%xmm2, %xmm8
	movapd	 -4 * SIZE(AO, %rax, 4), %xmm2
	addpd	%xmm3, %xmm9
	movddup	-10 * SIZE(BO, %rax, 1), %xmm3
	mulpd	%xmm5, %xmm2
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm5
	addpd	%xmm2, %xmm10
	movapd	  8 * SIZE(AO, %rax, 4), %xmm2
	addpd	%xmm5, %xmm11
	movddup	-11 * SIZE(BO, %rax, 1), %xmm5

	addq	$4 * SIZE, %rax
	BRANCH
	jl	.L92
	ALIGN_4

.L96:
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
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
	addpd	%xmm1, %xmm9
	movddup	-15 * SIZE(BO, %rax, 1), %xmm1

	addq	$SIZE, %rax
	jl	.L97
	ALIGN_4
.L99:
	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$4, %rax
#else
	subq	$1, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 4), AO
	leaq	(B,  %rax, 1), BO
#endif

#if defined(LN) || defined(LT)
	movapd	-16 * SIZE(BO), %xmm10
	movapd	-14 * SIZE(BO), %xmm11

	subpd	%xmm8,  %xmm10
	subpd	%xmm9,  %xmm11
#else
	movapd	-16 * SIZE(AO), %xmm10
	movapd	-14 * SIZE(AO), %xmm11

	subpd	%xmm8, %xmm10
	subpd	%xmm9, %xmm11
#endif

#ifdef LN
	movapd	%xmm10, %xmm8
        unpckhpd %xmm8, %xmm8

	movapd	%xmm11, %xmm9
        unpckhpd %xmm9, %xmm9

	movsd	 -1 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm9

	movsd	 -2 * SIZE(AO), %xmm13
	mulsd	 %xmm9, %xmm13
	subsd	 %xmm13, %xmm11
	movsd	 -3 * SIZE(AO), %xmm14
	mulsd	 %xmm9, %xmm14
	subsd	 %xmm14, %xmm8
	movsd	 -4 * SIZE(AO), %xmm15
	mulsd	 %xmm9, %xmm15
	subsd	 %xmm15, %xmm10

	movsd	 -6 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm11

	movsd	 -7 * SIZE(AO), %xmm13
	mulsd	 %xmm11, %xmm13
	subsd	 %xmm13, %xmm8
	movsd	 -8 * SIZE(AO), %xmm14
	mulsd	 %xmm11, %xmm14
	subsd	 %xmm14, %xmm10

	movsd	-11 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm8

	movsd	-12 * SIZE(AO), %xmm13
	mulsd	 %xmm8, %xmm13
	subsd	 %xmm13, %xmm10

	movsd	-16 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm10

	unpcklpd %xmm8, %xmm10
	unpcklpd %xmm9, %xmm11
#endif

#ifdef LT
	movapd	%xmm10, %xmm8
        unpckhpd %xmm8, %xmm8

	movapd	%xmm11, %xmm9
        unpckhpd %xmm9, %xmm9

	movsd	-16 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm10

	movsd	-15 * SIZE(AO), %xmm13
	mulsd	 %xmm10, %xmm13
	subsd	 %xmm13, %xmm8
	movsd	-14 * SIZE(AO), %xmm14
	mulsd	 %xmm10, %xmm14
	subsd	 %xmm14, %xmm11
	movsd	-13 * SIZE(AO), %xmm15
	mulsd	 %xmm10, %xmm15
	subsd	 %xmm15, %xmm9

	movsd	-11 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm8

	movsd	-10 * SIZE(AO), %xmm13
	mulsd	 %xmm8, %xmm13
	subsd	 %xmm13, %xmm11
	movsd	 -9 * SIZE(AO), %xmm14
	mulsd	 %xmm8, %xmm14
	subsd	 %xmm14, %xmm9

	movsd	 -6 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm11

	movsd	 -5 * SIZE(AO), %xmm13
	mulsd	 %xmm11, %xmm13
	subsd	 %xmm13, %xmm9

	movsd	 -1 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm9

	unpcklpd %xmm8, %xmm10
	unpcklpd %xmm9, %xmm11
#endif

#ifdef RN
	movddup	-16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm10
	mulpd	 %xmm8, %xmm11
#endif

#ifdef RT
	movddup	-16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm10
	mulpd	 %xmm8, %xmm11
#endif

#ifdef LN
	subq	$4 * SIZE, CO1
#endif

	movlpd	%xmm10,  0 * SIZE(CO1)
	movhpd	%xmm10,  1 * SIZE(CO1)
	movlpd	%xmm11,  2 * SIZE(CO1)
	movhpd	%xmm11,  3 * SIZE(CO1)

#if defined(LN) || defined(LT)
	movaps	%xmm10, -16 * SIZE(BO)
	movaps	%xmm11, -14 * SIZE(BO)
#else
	movaps	%xmm10, -16 * SIZE(AO)
	movaps	%xmm11, -14 * SIZE(AO)
#endif

#ifndef LN
	addq	$4 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	addq	%rax, BO
#endif

#ifdef LN
	subq	$4, KK
#endif

#ifdef LT
	addq	$4, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$2 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif

	decq	I			# i --
	jg	.L91
	ALIGN_4

.L100:
	testq	$2, M
	je	.L110

#ifdef LN
      movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
#endif

	movq	B, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(BO, %rax, SIZE), BO
#endif

	movddup	-16 * SIZE(BO), %xmm0
	pxor	%xmm8, %xmm8
	movddup	-15 * SIZE(BO), %xmm1
	pxor	%xmm9, %xmm9
	movddup	-14 * SIZE(BO), %xmm2
	pxor	%xmm10, %xmm10
	movddup	-13 * SIZE(BO), %xmm3
	pxor	%xmm11, %xmm11

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
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
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
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
	addpd	%xmm9, %xmm8
	addpd	%xmm11, %xmm10
	addpd	%xmm10, %xmm8

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$2, %rax
#else
	subq	$1, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 2), AO
	leaq	(B,  %rax, 1), BO
#endif

#if defined(LN) || defined(LT)
	movapd	-16 * SIZE(BO), %xmm10
	subpd	%xmm8,  %xmm10
#else
	movapd	-16 * SIZE(AO), %xmm10
	subpd	%xmm8, %xmm10
#endif

#ifdef LN
	movapd	%xmm10, %xmm8
        unpckhpd %xmm8, %xmm8

	movsd	-13 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm8

	movsd	-14 * SIZE(AO), %xmm13
	mulsd	 %xmm8, %xmm13
	subsd	 %xmm13, %xmm10

	movsd	-16 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm10

	unpcklpd %xmm8, %xmm10
#endif

#ifdef LT
	movapd	%xmm10, %xmm8
        unpckhpd %xmm8, %xmm8

	movsd	-16 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm10

	movsd	-15 * SIZE(AO), %xmm13
	mulsd	 %xmm10, %xmm13
	subsd	 %xmm13, %xmm8

	movsd	-13 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm8

	unpcklpd %xmm8, %xmm10
#endif

#ifdef RN
	movddup	-16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm10
#endif

#ifdef RT
	movddup	-16 * SIZE(BO), %xmm8
	mulpd	 %xmm8, %xmm10
#endif

#ifdef LN
	subq	$2 * SIZE, CO1
#endif

#if defined(LN) || defined(LT)
	movlpd	%xmm10,  0 * SIZE(CO1)
	movhpd	%xmm10,  1 * SIZE(CO1)
#else
	movlpd	%xmm10,  0 * SIZE(CO1)
	movhpd	%xmm10,  1 * SIZE(CO1)
#endif

#if defined(LN) || defined(LT)
	movaps	%xmm10, -16 * SIZE(BO)
#else
	movaps	%xmm10, -16 * SIZE(AO)
#endif

#ifndef LN
	addq	$2 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	addq	%rax, BO
#endif

#ifdef LN
	subq	$2, KK
#endif

#ifdef LT
	addq	$2, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$1 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L110:
	testq	$1, M
	je	.L119
	ALIGN_4

.L111:
#ifdef LN
       movq	K, %rax
       salq	$0 + BASE_SHIFT, %rax
       subq	%rax, AORIG
#endif

#if defined(LN) || defined(RT)
	movq	KK, %rax
	movq	AORIG, AO
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
#endif

	movq	B, BO

#if defined(LN) || defined(RT)
	movq	KK, %rax
	leaq	(BO, %rax, SIZE), BO
#endif

	movapd	-16 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movapd	-14 * SIZE(AO), %xmm1
	pxor	%xmm9, %xmm9

#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
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
#if defined(LT) || defined(RN)
	movq	KK, %rax
#else
	movq	K, %rax
	subq	KK, %rax
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

#if defined(LN) || defined(RT)
	movq	KK, %rax
#ifdef LN
	subq	$1, %rax
#else
	subq	$1, %rax
#endif

	leaq	(, %rax, SIZE), %rax

	movq	AORIG, AO
	leaq	(AO, %rax, 1), AO
	leaq	(B,  %rax, 1), BO
#endif

#if defined(LN) || defined(LT)
	movsd	-16 * SIZE(BO), %xmm10
	subsd	%xmm8,  %xmm10
#else
	movsd	-16 * SIZE(AO), %xmm10
	subsd	%xmm8, %xmm10
#endif

#if defined(LN) || defined(LT)
	movsd	-16 * SIZE(AO), %xmm12
	mulsd	 %xmm12, %xmm10
#endif

#if defined(RN) || defined(RT)
	movsd	-16 * SIZE(BO), %xmm8
	mulsd	 %xmm8, %xmm10
#endif

#ifdef LN
	subq	$1 * SIZE, CO1
#endif

	movsd	%xmm10,  0 * SIZE(CO1)

#if defined(LN) || defined(LT)
	movlpd	%xmm10, -16 * SIZE(BO)
#else
	movlpd	%xmm10, -16 * SIZE(AO)
#endif

#ifndef LN
	addq	$1 * SIZE, CO1
#endif

#if defined(LT) || defined(RN)
	movq	K,  %rax
	subq	KK, %rax
	leaq	(,%rax, SIZE), %rax
	addq	%rax, AO
	addq	%rax, BO
#endif

#ifdef LN
	subq	$1, KK
#endif

#ifdef LT
	addq	$1, KK
#endif

#ifdef RT
       movq	K, %rax
       salq	$0 + BASE_SHIFT, %rax
       addq	%rax, AORIG
#endif
	ALIGN_4

.L119:
#ifdef LN
       leaq	(B, K, SIZE), B
#endif

#if defined(LT) || defined(RN)
	movq	BO, B
#endif

#ifdef RN
	addq	$1, KK
#endif

#ifdef RT
	subq	$1, KK
#endif
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
