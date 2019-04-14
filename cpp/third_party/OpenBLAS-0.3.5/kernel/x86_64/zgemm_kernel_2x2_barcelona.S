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
#define	CO1	%rbp
#define CO2	%rbx
#define BB	%r12
#define J	%r15

#ifndef WINDOWS_ABI

#define STACKSIZE 96

#define ALPHA_R	 48(%rsp)
#define ALPHA_I	 56(%rsp)
#define OFFSET	 64(%rsp)
#define KK	 72(%rsp)
#define KKK	 80(%rsp)

#else

#define STACKSIZE 320

#define OLD_ALPHA_I	40 + STACKSIZE(%rsp)
#define OLD_A		48 + STACKSIZE(%rsp)
#define OLD_B		56 + STACKSIZE(%rsp)
#define OLD_C		64 + STACKSIZE(%rsp)
#define OLD_LDC		72 + STACKSIZE(%rsp)
#define OLD_OFFSET	80 + STACKSIZE(%rsp)

#define ALPHA_R	224(%rsp)
#define ALPHA_I	232(%rsp)
#define OFFSET	240(%rsp)
#define KK	248(%rsp)
#define KKK	256(%rsp)

#endif

#define movlpd	movsd
#define movapd	movups
#define movupd	movups

#if   defined(NN) || defined(NT) || defined(TN) || defined(TT)
#define ADD1	addpd
#define ADD2	addpd
#elif defined(RN) || defined(RT) || defined(CN) || defined(CT)
#define ADD1	subpd
#define ADD2	addpd
#elif defined(NR) || defined(NC) || defined(TR) || defined(TC)
#define ADD1	addpd
#define ADD2	subpd
#else
#define ADD1	subpd
#define ADD2	subpd
#endif

#define KERNEL1(xx) \
	mulpd	%xmm1, %xmm0 ;\
	ADD1	%xmm0, %xmm8 ;\
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm1 ;\
	movapd	%xmm2, %xmm0 ;\
	ADD1	%xmm1, %xmm12 ;\
	movddup	-14 * SIZE(BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm9 ;\
	movapd	%xmm0, %xmm2 ;\
	ADD2	%xmm3, %xmm13 ;\
	movddup	-13 * SIZE(BO, %rax, 4), %xmm3 ;\
	mulpd	%xmm1, %xmm0 ;\
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm1 ;\
	ADD1	%xmm0, %xmm10 ;\
	movapd	-12 * SIZE(AO, %rax, 4), %xmm0 ;\
	ADD1	%xmm1, %xmm14 ;\
	movddup	-12 * SIZE(BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm11 ;\
	ADD2	%xmm3, %xmm15 ;\
 	movddup	-11 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm0, %xmm2

#define KERNEL2(xx) \
	mulpd	%xmm1, %xmm0 ;\
	ADD1	%xmm0, %xmm8 ;\
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm1 ;\
	movapd	%xmm2, %xmm0 ;\
	ADD1	%xmm1, %xmm12 ;\
/*A*/	movapd	  (AO, %rax, 4), %xmm6 ;\
	movddup	-10 * SIZE(BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm9 ;\
	movapd	%xmm0, %xmm2 ;\
	ADD2	%xmm3, %xmm13 ;\
	movddup	 -9 * SIZE(BO, %rax, 4), %xmm3 ;\
	mulpd	%xmm1, %xmm0 ;\
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm1 ;\
	ADD1	%xmm0, %xmm10 ;\
	ADD1	%xmm1, %xmm14 ;\
/**/	movddup	  (BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm11 ;\
	ADD2	%xmm3, %xmm15 ;\
 	movddup	 -7 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm4, %xmm2

#define KERNEL3(xx) \
	mulpd	%xmm5, %xmm4 ;\
	ADD1	%xmm4, %xmm8 ;\
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm5 ;\
	movapd	%xmm2, %xmm4 ;\
	ADD1	%xmm5, %xmm12 ;\
	movddup	 -6 * SIZE(BO, %rax, 4), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm9 ;\
	movapd	%xmm4, %xmm2 ;\
	ADD2	%xmm3, %xmm13 ;\
	movddup	 -5 * SIZE(BO, %rax, 4), %xmm3 ;\
	mulpd	%xmm5, %xmm4 ;\
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm5 ;\
	ADD1	%xmm4, %xmm10 ;\
	movapd	 -4 * SIZE(AO, %rax, 4), %xmm4 ;\
	ADD1	%xmm5, %xmm14 ;\
	movddup	 -4 * SIZE(BO, %rax, 4), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm11 ;\
	ADD2	%xmm3, %xmm15 ;\
 	movddup	 -3 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm4, %xmm2

#define KERNEL4(xx) \
	mulpd	%xmm5, %xmm4 ;\
	ADD1	%xmm4, %xmm8 ;\
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm5 ;\
	movapd	%xmm2, %xmm4 ;\
	ADD1	%xmm5, %xmm12 ;\
/*A*/	movapd	  8 * SIZE(AO, %rax, 4), %xmm7 ;\
	movddup	 -2 * SIZE(BO, %rax, 4), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm9 ;\
	movapd	%xmm4, %xmm2 ;\
	ADD2	%xmm3, %xmm13 ;\
	movddup	 -1 * SIZE(BO, %rax, 4), %xmm3 ;\
	mulpd	%xmm5, %xmm4 ;\
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm5 ;\
	ADD1	%xmm4, %xmm10 ;\
	ADD1	%xmm5, %xmm14 ;\
/**/	movddup	  8 * SIZE(BO, %rax, 4), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm11 ;\
	ADD2	%xmm3, %xmm15 ;\
 	movddup	  1 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm6, %xmm2

#define KERNEL5(xx) \
	mulpd	%xmm1, %xmm6 ;\
	ADD1	%xmm6, %xmm8 ;\
	mulpd	  2 * SIZE(AO, %rax, 4), %xmm1 ;\
	movapd	%xmm2, %xmm6 ;\
	ADD1	%xmm1, %xmm12 ;\
	movddup	  2 * SIZE(BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	  2 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm9 ;\
	movapd	%xmm6, %xmm2 ;\
	ADD2	%xmm3, %xmm13 ;\
	movddup	  3 * SIZE(BO, %rax, 4), %xmm3 ;\
	mulpd	%xmm1, %xmm6 ;\
	mulpd	  2 * SIZE(AO, %rax, 4), %xmm1 ;\
	ADD1	%xmm6, %xmm10 ;\
	movapd	  4 * SIZE(AO, %rax, 4), %xmm6 ;\
	ADD1	%xmm1, %xmm14 ;\
	movddup	  4 * SIZE(BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	  2 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm11 ;\
	ADD2	%xmm3, %xmm15 ;\
 	movddup	  5 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm6, %xmm2

#define KERNEL6(xx) \
	mulpd	%xmm1, %xmm6 ;\
	ADD1	%xmm6, %xmm8 ;\
	mulpd	  6 * SIZE(AO, %rax, 4), %xmm1 ;\
	movapd	%xmm2, %xmm6 ;\
	ADD1	%xmm1, %xmm12 ;\
/*A*/	movapd	 16 * SIZE(AO, %rax, 4), %xmm0 ;\
	movddup	  6 * SIZE(BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	  6 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm9 ;\
	movapd	%xmm6, %xmm2 ;\
	ADD2	%xmm3, %xmm13 ;\
	movddup	  7 * SIZE(BO, %rax, 4), %xmm3 ;\
	mulpd	%xmm1, %xmm6 ;\
	mulpd	  6 * SIZE(AO, %rax, 4), %xmm1 ;\
	ADD1	%xmm6, %xmm10 ;\
 	ADD1	%xmm1, %xmm14 ;\
/**/	movddup	 16 * SIZE(BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	  6 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm11 ;\
	ADD2	%xmm3, %xmm15 ;\
 	movddup	  9 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm7, %xmm2

#define KERNEL7(xx) \
	mulpd	%xmm5, %xmm7 ;\
	ADD1	%xmm7, %xmm8 ;\
	mulpd	 10 * SIZE(AO, %rax, 4), %xmm5 ;\
	movapd	%xmm2, %xmm7 ;\
	ADD1	%xmm5, %xmm12 ;\
	movddup	 10 * SIZE(BO, %rax, 4), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 10 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm9 ;\
	movapd	%xmm7, %xmm2 ;\
	ADD2	%xmm3, %xmm13 ;\
	movddup	 11 * SIZE(BO, %rax, 4), %xmm3 ;\
	mulpd	%xmm5, %xmm7 ;\
	mulpd	 10 * SIZE(AO, %rax, 4), %xmm5 ;\
	ADD1	%xmm7, %xmm10 ;\
	movapd	 12 * SIZE(AO, %rax, 4), %xmm7 ;\
	ADD1	%xmm5, %xmm14 ;\
	movddup	 12 * SIZE(BO, %rax, 4), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 10 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm11 ;\
	ADD2	%xmm3, %xmm15 ;\
 	movddup	 13 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm7, %xmm2

#define KERNEL8(xx) \
	mulpd	%xmm5, %xmm7 ;\
	ADD1	%xmm7, %xmm8 ;\
	mulpd	 14 * SIZE(AO, %rax, 4), %xmm5 ;\
	movapd	%xmm2, %xmm7 ;\
	ADD1	%xmm5, %xmm12 ;\
/*A*/	movapd	 24 * SIZE(AO, %rax, 4), %xmm4 ;\
	movddup	 14 * SIZE(BO, %rax, 4), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 14 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm9 ;\
	movapd	%xmm7, %xmm2 ;\
	ADD2	%xmm3, %xmm13 ;\
	movddup	 15 * SIZE(BO, %rax, 4), %xmm3 ;\
	mulpd	%xmm5, %xmm7 ;\
	mulpd	 14 * SIZE(AO, %rax, 4), %xmm5 ;\
	ADD1	%xmm7, %xmm10 ;\
	ADD1	%xmm5, %xmm14 ;\
/**/	movddup	 24 * SIZE(BO, %rax, 4), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 14 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm11 ;\
	ADD2	%xmm3, %xmm15 ;\
 	movddup	 17 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm0, %xmm2 ;\
	addq	$8 * SIZE, %rax ;\

#define KERNEL_SUB1(xx) \
	mulpd	%xmm1, %xmm0 ;\
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm1 ;\
	ADD1	%xmm0, %xmm8 ;\
	movapd	%xmm2, %xmm0 ;\
	ADD1	%xmm1, %xmm12 ;\
	movddup	-14 * SIZE(BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm9 ;\
	movapd	%xmm0, %xmm2 ;\
	ADD2	%xmm3, %xmm13 ;\
	movddup	-13 * SIZE(BO, %rax, 4), %xmm3 ;\
	mulpd	%xmm1, %xmm0 ;\
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm1 ;\
	ADD1	%xmm0, %xmm10 ;\
	movapd	-12 * SIZE(AO, %rax, 4), %xmm0 ;\
	ADD1	%xmm1, %xmm14 ;\
	movddup	-12 * SIZE(BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm11 ;\
	ADD2	%xmm3, %xmm15 ;\
 	movddup	-11 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm0, %xmm2

#define KERNEL_SUB2(xx) \
	mulpd	%xmm1, %xmm0 ;\
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm1 ;\
	ADD1	%xmm0, %xmm8 ;\
	movapd	%xmm2, %xmm0 ;\
	ADD1	%xmm1, %xmm12 ;\
	movddup	-10 * SIZE(BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm9 ;\
	movapd	%xmm0, %xmm2 ;\
	ADD2	%xmm3, %xmm13 ;\
	movddup	 -9 * SIZE(BO, %rax, 4), %xmm3 ;\
	mulpd	%xmm1, %xmm0 ;\
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm1 ;\
	ADD1	%xmm0, %xmm10 ;\
	movapd	  (AO, %rax, 4), %xmm0 ;\
	ADD1	%xmm1, %xmm14 ;\
	movddup	  (BO, %rax, 4), %xmm1 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm11 ;\
	ADD2	%xmm3, %xmm15 ;\
 	movddup	 -7 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm4, %xmm2

#define KERNEL_SUB3(xx) \
	mulpd	%xmm5, %xmm4 ;\
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm5 ;\
	ADD1	%xmm4, %xmm8 ;\
	movapd	%xmm2, %xmm4 ;\
	ADD1	%xmm5, %xmm12 ;\
	movddup	 -6 * SIZE(BO, %rax, 4), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm9 ;\
	movapd	%xmm4, %xmm2 ;\
	ADD2	%xmm3, %xmm13 ;\
	movddup	 -5 * SIZE(BO, %rax, 4), %xmm3 ;\
	mulpd	%xmm5, %xmm4 ;\
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm5 ;\
	ADD1	%xmm4, %xmm10 ;\
	movapd	 -4 * SIZE(AO, %rax, 4), %xmm4 ;\
	ADD1	%xmm5, %xmm14 ;\
	movddup	 -4 * SIZE(BO, %rax, 4), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm11 ;\
	ADD2	%xmm3, %xmm15 ;\
 	movddup	 -3 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm4, %xmm2

#define KERNEL_SUB4(xx) \
	mulpd	%xmm5, %xmm4 ;\
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm5 ;\
	ADD1	%xmm4, %xmm8 ;\
	movapd	%xmm2, %xmm4 ;\
	ADD1	%xmm5, %xmm12 ;\
	movddup	 -2 * SIZE(BO, %rax, 4), %xmm5 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm9 ;\
	movapd	%xmm4, %xmm2 ;\
	ADD2	%xmm3, %xmm13 ;\
	movddup	 -1 * SIZE(BO, %rax, 4), %xmm3 ;\
	mulpd	%xmm5, %xmm4 ;\
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm5 ;\
	ADD1	%xmm4, %xmm10 ;\
	ADD1	%xmm5, %xmm14 ;\
	mulpd	%xmm3, %xmm2 ;\
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm3 ;\
	ADD2	%xmm2, %xmm11 ;\
	ADD2	%xmm3, %xmm15 ;\
 	movddup	  1 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm0, %xmm2

#if defined(OS_LINUX) && defined(CORE_BARCELONA) && !defined(TRMMKERNEL)
	.align 32768
#endif
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
	movaps	%xmm3,       %xmm0
	movsd	OLD_ALPHA_I, %xmm1
#else
	movq	STACKSIZE +  8(%rsp), LDC
#ifdef TRMMKERNEL
	movsd	STACKSIZE + 16(%rsp), %xmm12
#endif

#endif

	movq	OLD_M, M
	movq	OLD_N, N

	movlpd	 %xmm0, ALPHA_R
	movlpd	 %xmm1, ALPHA_I

#ifdef TRMMKERNEL
	movlpd	%xmm12, OFFSET
	movlpd	%xmm12, KK
#ifndef LEFT
	negq	KK
#endif
#endif

	subq	$-16 * SIZE, A
	subq	$-16 * SIZE, B

	salq	$ZBASE_SHIFT, LDC

	movq	N,  J
	sarq	$1, J
	jle	.L100
	ALIGN_4

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc

	movq	A, AO		# aoffset = a

	movq	K, %rax
	salq	$ZBASE_SHIFT + 1, %rax
	leaq	(B, %rax), BB

	movq	M,  I
	sarq	$1, I		# i = (m >> 2)
	jle	.L30
	ALIGN_4

.L10:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	B, BO
#else
	movq	B, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
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

	prefetchw      3 * SIZE(CO1)
	pxor	%xmm12, %xmm12
	pxor	%xmm13, %xmm13
	prefetchw      7 * SIZE(CO2)
	pxor	%xmm14, %xmm14
	pxor	%xmm15, %xmm15
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
	addq	$2, %rax
#else
	addq	$2, %rax
#endif
	movq	%rax, KKK
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
#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
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
	movddup	ALPHA_R, %xmm6
	movddup	ALPHA_I, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax
	je .L19

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
	negq	%rax
	ALIGN_4

.L17:
	mulpd	%xmm1, %xmm0
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm1
	ADD1	%xmm0, %xmm8
	movapd	%xmm2, %xmm0
	ADD1	%xmm1, %xmm12
	movddup	-14 * SIZE(BO, %rax, 4), %xmm1
	mulpd	%xmm3, %xmm2
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm3
	ADD2	%xmm2, %xmm9
	movapd	%xmm0, %xmm2
	ADD2	%xmm3, %xmm13
	movddup	-13 * SIZE(BO, %rax, 4), %xmm3
	mulpd	%xmm1, %xmm0
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm1
	ADD1	%xmm0, %xmm10
	movapd	-12 * SIZE(AO, %rax, 4), %xmm0
	ADD1	%xmm1, %xmm14
	movddup	-12 * SIZE(BO, %rax, 4), %xmm1
	mulpd	%xmm3, %xmm2
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm3
	ADD2	%xmm2, %xmm11
	ADD2	%xmm3, %xmm15
 	movddup	-11 * SIZE(BO, %rax, 4), %xmm3
	movapd	%xmm0, %xmm2

	addq	$SIZE, %rax
	jl	.L17
	ALIGN_4

.L19:
	prefetch	   -8 * SIZE(BB)
	subq		 $-16 * SIZE, BB

#ifndef TRMMKERNEL
	movupd	        (CO1), %xmm0
	movupd	2 * SIZE(CO1), %xmm2
	movupd	        (CO2), %xmm1
	movupd	2 * SIZE(CO2), %xmm3
#endif

	SHUFPD_1 %xmm9,  %xmm9
	SHUFPD_1 %xmm11, %xmm11
	SHUFPD_1 %xmm13, %xmm13
	SHUFPD_1 %xmm15, %xmm15

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	addsubpd %xmm9,  %xmm8
	addsubpd %xmm11, %xmm10
	addsubpd %xmm13, %xmm12
	addsubpd %xmm15, %xmm14

	pshufd	$0x4e, %xmm8,  %xmm9
	pshufd	$0x4e, %xmm10, %xmm11
	pshufd	$0x4e, %xmm12, %xmm13
	pshufd	$0x4e, %xmm14, %xmm15

#else
	addsubpd %xmm8,  %xmm9
	addsubpd %xmm10, %xmm11
	addsubpd %xmm12, %xmm13
	addsubpd %xmm14, %xmm15

	movapd	 %xmm9,  %xmm8
	pshufd	$0x4e, %xmm9,  %xmm9
	movapd	 %xmm11, %xmm10
	pshufd	$0x4e, %xmm11, %xmm11
	movapd	 %xmm13, %xmm12
	pshufd	$0x4e, %xmm13, %xmm13
	movapd	 %xmm15, %xmm14
	pshufd	$0x4e, %xmm15, %xmm15
#endif

	mulpd	%xmm6, %xmm8
	mulpd	%xmm7, %xmm9
	mulpd	%xmm6, %xmm10
	mulpd	%xmm7, %xmm11

	mulpd	%xmm6, %xmm12
	mulpd	%xmm7, %xmm13
	mulpd	%xmm6, %xmm14
	mulpd	%xmm7, %xmm15

	addsubpd %xmm9,  %xmm8
	addsubpd %xmm11, %xmm10
	addsubpd %xmm13, %xmm12
	addsubpd %xmm15, %xmm14

#ifndef TRMMKERNEL
	addpd	%xmm0,  %xmm8
	addpd	%xmm2,  %xmm12
	addpd	%xmm1,  %xmm10
	addpd	%xmm3,  %xmm14
#endif

	movlpd	%xmm8,          (CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
	movlpd	%xmm12, 2 * SIZE(CO1)
	movhpd	%xmm12, 3 * SIZE(CO1)

	movlpd	%xmm10,         (CO2)
	movhpd	%xmm10, 1 * SIZE(CO2)
	movlpd	%xmm14, 2 * SIZE(CO2)
	movhpd	%xmm14, 3 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	addq	$4 * SIZE, CO2		# coffset += 4
	decq	I			# i --
	jg	.L10
	ALIGN_4

.L30:
	testq	$1, M
	jle	.L99

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	B, BO
#else
	movq	B, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
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
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
	negq	%rax
	NOBRANCH
	je	.L46
	ALIGN_4

.L42:
	mulpd	%xmm0, %xmm1
	ADD1	%xmm1, %xmm8
	movddup	-14 * SIZE(BO, %rax, 4), %xmm1
	mulpd	%xmm0, %xmm5
	ADD2	%xmm5, %xmm9
	movddup	-13 * SIZE(BO, %rax, 4), %xmm5
	mulpd	%xmm0, %xmm1
	ADD1	%xmm1, %xmm10
	movddup	-12 * SIZE(BO, %rax, 4), %xmm1
	mulpd	%xmm0, %xmm5
	movapd	-14 * SIZE(AO, %rax, 2), %xmm0
	ADD2	%xmm5, %xmm11
	movddup	-11 * SIZE(BO, %rax, 4), %xmm5
	mulpd	%xmm0, %xmm1
	ADD1	%xmm1, %xmm8
	movddup	-10 * SIZE(BO, %rax, 4), %xmm1
	mulpd	%xmm0, %xmm5
	ADD2	%xmm5, %xmm9
	movddup	 -9 * SIZE(BO, %rax, 4), %xmm5
	mulpd	%xmm0, %xmm1
	ADD1	%xmm1, %xmm10
	movddup	  (BO, %rax, 4), %xmm1
	mulpd	%xmm0, %xmm5
	movapd	 -8 * SIZE(AO, %rax, 2), %xmm0
	ADD2	%xmm5, %xmm11
	movddup	 -7 * SIZE(BO, %rax, 4), %xmm5
	mulpd	%xmm2, %xmm3
	ADD1	%xmm3, %xmm8
	movddup	 -6 * SIZE(BO, %rax, 4), %xmm3
	mulpd	%xmm2, %xmm5
	ADD2	%xmm5, %xmm9
	movddup	 -5 * SIZE(BO, %rax, 4), %xmm5
	mulpd	%xmm2, %xmm3
	ADD1	%xmm3, %xmm10
	movddup	 -4 * SIZE(BO, %rax, 4), %xmm3
	mulpd	%xmm2, %xmm5
	movapd	-10 * SIZE(AO, %rax, 2), %xmm2
	ADD2	%xmm5, %xmm11
	movddup	 -3 * SIZE(BO, %rax, 4), %xmm5
	mulpd	%xmm2, %xmm3
	ADD1	%xmm3, %xmm8
	movddup	 -2 * SIZE(BO, %rax, 4), %xmm3
	mulpd	%xmm2, %xmm5
	ADD2	%xmm5, %xmm9
	movddup	 -1 * SIZE(BO, %rax, 4), %xmm5
	mulpd	%xmm2, %xmm3
	ADD1	%xmm3, %xmm10
	movddup	  8 * SIZE(BO, %rax, 4), %xmm3
	mulpd	%xmm2, %xmm5
	movapd	 -4 * SIZE(AO, %rax, 2), %xmm2
	ADD2	%xmm5, %xmm11
	movddup	  1 * SIZE(BO, %rax, 4), %xmm5

	addq	$4 * SIZE, %rax
	BRANCH
	jl	.L42
	ALIGN_4

.L46:
	movddup	ALPHA_R, %xmm6
	movddup	ALPHA_I, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L49

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
	negq	%rax
	ALIGN_4

.L47:
	mulpd	%xmm0, %xmm1
	ADD1	%xmm1, %xmm8
	movddup	-14 * SIZE(BO, %rax, 4), %xmm1
	mulpd	%xmm0, %xmm5
	ADD2	%xmm5, %xmm9
	movddup	-13 * SIZE(BO, %rax, 4), %xmm5
	mulpd	%xmm0, %xmm1
	ADD1	%xmm1, %xmm10
	movddup	-12 * SIZE(BO, %rax, 4), %xmm1
	mulpd	%xmm0, %xmm5
	movapd	-14 * SIZE(AO, %rax, 2), %xmm0
	ADD2	%xmm5, %xmm11
	movddup	-11 * SIZE(BO, %rax, 4), %xmm5

	addq	$SIZE, %rax
	jl	.L47
	ALIGN_4

.L49:
#ifndef TRMMKERNEL
	movupd	(CO1), %xmm0
	movupd	(CO2), %xmm1
#endif

	SHUFPD_1 %xmm9, %xmm9
	SHUFPD_1 %xmm11, %xmm11

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	addsubpd %xmm9,  %xmm8
	addsubpd %xmm11, %xmm10

	pshufd	$0x4e, %xmm8,  %xmm9
	pshufd	$0x4e, %xmm10, %xmm11
#else
	addsubpd %xmm8,  %xmm9
	addsubpd %xmm10, %xmm11

	movapd	 %xmm9,  %xmm8
	pshufd	$0x4e, %xmm9,  %xmm9
	movapd	 %xmm11, %xmm10
	pshufd	$0x4e, %xmm11, %xmm11
#endif

	mulpd	%xmm6, %xmm8
	mulpd	%xmm7, %xmm9
	mulpd	%xmm6, %xmm10
	mulpd	%xmm7, %xmm11

	addsubpd %xmm9, %xmm8
	addsubpd %xmm11, %xmm10

#ifndef TRMMKERNEL
	addpd	%xmm0,  %xmm8
	addpd	%xmm1,  %xmm10
#endif

	movlpd	%xmm8,          (CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
	movlpd	%xmm10,         (CO2)
	movhpd	%xmm10, 1 * SIZE(CO2)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 4), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$1, KK
#endif
	ALIGN_4

.L99:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	movq	BO, B

	leaq	(C, LDC, 2), C		# c += 2 * ldc
	decq	J			# j --
	jg	.L01

.L100:
	testq	$1, N
	jle	.L999

.L101:
#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

	movq	C, CO1		# coffset1 = c
	movq	A, AO		# aoffset = a

	movq	M,  I
	sarq	$1, I		# i = (m >> 2)
	jle	.L130
	ALIGN_4

.L110:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	B, BO
#else
	movq	B, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
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
	prefetchw      3 * SIZE(CO1)
	movapd	%xmm0, %xmm2

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
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
	negq	%rax
	NOBRANCH
	je	.L116
	ALIGN_4

.L112:
	mulpd	%xmm1, %xmm0
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm1
	ADD1	%xmm0, %xmm8
	movapd	-12 * SIZE(AO, %rax, 4), %xmm0
	ADD1	%xmm1, %xmm12
	movddup	-14 * SIZE(BO, %rax, 2), %xmm1
	mulpd	%xmm5, %xmm2
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm5
	ADD2	%xmm2, %xmm9
	ADD2	%xmm5, %xmm13
	movddup	-13 * SIZE(BO, %rax, 2), %xmm5
	movapd	%xmm0, %xmm2
	mulpd	%xmm1, %xmm0
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm1
	ADD1	%xmm0, %xmm8
	movapd	  (AO, %rax, 4), %xmm0
	ADD1	%xmm1, %xmm12
	movddup	 -8 * SIZE(BO, %rax, 2), %xmm1
	mulpd	%xmm5, %xmm2
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm5
	ADD2	%xmm2, %xmm9
	ADD2	%xmm5, %xmm13
	movddup	-11 * SIZE(BO, %rax, 2), %xmm5
	movapd	%xmm4, %xmm2
	mulpd	%xmm3, %xmm4
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm3
	ADD1	%xmm4, %xmm8
	movapd	 -4 * SIZE(AO, %rax, 4), %xmm4
	ADD1	%xmm3, %xmm12
	movddup	-10 * SIZE(BO, %rax, 2), %xmm3
	mulpd	%xmm5, %xmm2
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm5
	ADD2	%xmm2, %xmm9
	ADD2	%xmm5, %xmm13
	movddup	 -9 * SIZE(BO, %rax, 2), %xmm5
	movapd	%xmm4, %xmm2
	mulpd	%xmm3, %xmm4
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm3
	ADD1	%xmm4, %xmm8
	movapd	  8 * SIZE(AO, %rax, 4), %xmm4
	ADD1	%xmm3, %xmm12
	movddup	 -4 * SIZE(BO, %rax, 2), %xmm3
	mulpd	%xmm5, %xmm2
	mulpd	 -2 * SIZE(AO, %rax, 4), %xmm5
	ADD2	%xmm2, %xmm9
	ADD2	%xmm5, %xmm13
	movddup	 -7 * SIZE(BO, %rax, 2), %xmm5
	movapd	%xmm0, %xmm2

	addq	$4 * SIZE, %rax
	BRANCH
	jl	.L112
	ALIGN_4

.L116:
	movddup	ALPHA_R, %xmm6
	movddup	ALPHA_I, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L119

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
	negq	%rax
	ALIGN_4

.L117:
	mulpd	%xmm1, %xmm0
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm1
	ADD1	%xmm0, %xmm8
	movapd	-12 * SIZE(AO, %rax, 4), %xmm0
	ADD1	%xmm1, %xmm12
	movddup	-14 * SIZE(BO, %rax, 2), %xmm1
	mulpd	%xmm5, %xmm2
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm5
	ADD2	%xmm2, %xmm9
	ADD2	%xmm5, %xmm13
	movddup	-13 * SIZE(BO, %rax, 2), %xmm5
	movapd	%xmm0, %xmm2

	addq	$SIZE, %rax
	jl	.L117
	ALIGN_4

.L119:
#ifndef TRMMKERNEL
	movupd	(CO1), %xmm0
	movupd	2 * SIZE(CO1), %xmm2
#endif

	SHUFPD_1 %xmm9, %xmm9
	SHUFPD_1 %xmm13, %xmm13

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	addsubpd %xmm9,  %xmm8
	addsubpd %xmm13, %xmm12

	pshufd	$0x4e, %xmm8,  %xmm9
	pshufd	$0x4e, %xmm12, %xmm13

#else
	addsubpd %xmm8,  %xmm9
	addsubpd %xmm12, %xmm13

	movapd	 %xmm9,  %xmm8
	pshufd	$0x4e, %xmm9,  %xmm9
	movapd	 %xmm13, %xmm12
	pshufd	$0x4e, %xmm13, %xmm13
#endif

	mulpd	%xmm6, %xmm8
	mulpd	%xmm7, %xmm9
	mulpd	%xmm6, %xmm12
	mulpd	%xmm7, %xmm13

	addsubpd %xmm9, %xmm8
	addsubpd %xmm13, %xmm12

#ifndef TRMMKERNEL
	addpd	%xmm0,  %xmm8
	addpd	%xmm2, %xmm12
#endif

	movlpd	%xmm8,          (CO1)
	movhpd	%xmm8,  1 * SIZE(CO1)
	movlpd	%xmm12, 2 * SIZE(CO1)
	movhpd	%xmm12, 3 * SIZE(CO1)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	K, %rax
	subq	KKK, %rax
	leaq	(,%rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 2), BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addq	$2, KK
#endif

	addq	$4 * SIZE, CO1		# coffset += 4
	decq	I			# i --
	jg	.L110
	ALIGN_4

.L130:
	testq	$1, M
	jle	.L999

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movq	B, BO
#else
	movq	B, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
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
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
	negq	%rax
	NOBRANCH
	je	.L146
	ALIGN_4

.L142:
	mulpd	%xmm0, %xmm1
	ADD1	%xmm1, %xmm8
	movddup	-14 * SIZE(BO, %rax, 2), %xmm1
	mulpd	%xmm0, %xmm3
	movapd	-14 * SIZE(AO, %rax, 2), %xmm0
	ADD2	%xmm3, %xmm9
	movddup	-13 * SIZE(BO, %rax, 2), %xmm3
	mulpd	%xmm0, %xmm1
	ADD1	%xmm1, %xmm10
	movddup	-12 * SIZE(BO, %rax, 2), %xmm1
	mulpd	%xmm0, %xmm3
	movapd	 -8 * SIZE(AO, %rax, 2), %xmm0
	ADD2	%xmm3, %xmm11
	movddup	-11 * SIZE(BO, %rax, 2), %xmm3
	mulpd	%xmm2, %xmm1
	ADD1	%xmm1, %xmm8
	movddup	-10 * SIZE(BO, %rax, 2), %xmm1
	mulpd	%xmm2, %xmm3
	movapd	-10 * SIZE(AO, %rax, 2), %xmm2
	ADD2	%xmm3, %xmm9
	movddup	 -9 * SIZE(BO, %rax, 2), %xmm3
	mulpd	%xmm2, %xmm1
	ADD1	%xmm1, %xmm10
	movddup	 -8 * SIZE(BO, %rax, 2), %xmm1
	mulpd	%xmm2, %xmm3
	movapd	 -4 * SIZE(AO, %rax, 2), %xmm2
	ADD2	%xmm3, %xmm11
	movddup	 -7 * SIZE(BO, %rax, 2), %xmm3

	addq	$4 * SIZE, %rax
	BRANCH
	jl	.L142
	ALIGN_4

.L146:
	movddup	ALPHA_R, %xmm6
	movddup	ALPHA_I, %xmm7

#ifndef TRMMKERNEL
	movq	K, %rax
#else
	movq	KKK, %rax
#endif
	andq	$3, %rax		# if (k & 1)
	je .L148

	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 2), BO
	negq	%rax
	ALIGN_4

.L147:
	mulpd	%xmm0, %xmm1
	ADD1	%xmm1, %xmm8
	movddup	-14 * SIZE(BO, %rax, 2), %xmm1
	mulpd	%xmm0, %xmm3
	movapd	-14 * SIZE(AO, %rax, 2), %xmm0
	ADD2	%xmm3, %xmm9
	movddup	-13 * SIZE(BO, %rax, 2), %xmm3

	addq	$SIZE, %rax
	jl	.L147
	ALIGN_4

.L148:
#ifndef TRMMKERNEL
	movupd	(CO1), %xmm0
#endif

	addpd	%xmm10, %xmm8
	addpd	%xmm11, %xmm9

	SHUFPD_1 %xmm9, %xmm9

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(NR) || defined(NC) || defined(TR) || defined(TC)
	addsubpd %xmm9,  %xmm8
	pshufd	$0x4e, %xmm8,  %xmm9
#else
	addsubpd %xmm8,  %xmm9
	movapd	 %xmm9,  %xmm8
	pshufd	$0x4e, %xmm9,  %xmm9
#endif

	mulpd	%xmm6, %xmm8
	mulpd	%xmm7, %xmm9

	addsubpd %xmm9, %xmm8

#ifndef TRMMKERNEL
	addpd	%xmm0,  %xmm8
#endif

	movlpd	%xmm8,         (CO1)
	movhpd	%xmm8, 1 * SIZE(CO1)
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
