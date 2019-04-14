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

#define BUFFERED

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

#ifndef WINDOWS_ABI

#define STACKSIZE 64

#define OLD_LDC		 8 + STACKSIZE(%rsp)
#define OLD_OFFSET	16 + STACKSIZE(%rsp)

#else

#define STACKSIZE 256

#define OLD_ALPHA_I	40 + STACKSIZE(%rsp)
#define OLD_A		48 + STACKSIZE(%rsp)
#define OLD_B		56 + STACKSIZE(%rsp)
#define OLD_C		64 + STACKSIZE(%rsp)
#define OLD_LDC		72 + STACKSIZE(%rsp)
#define OLD_OFFSET	80 + STACKSIZE(%rsp)

#endif

#define ALPHA	  0(%rsp)
#define J	 16(%rsp)
#define OFFSET	 24(%rsp)
#define KK	 32(%rsp)
#define KKK	 40(%rsp)
#define BUFFER  512(%rsp)

#define PREFETCH     prefetch
#define PREFETCHSIZE (8 *  21 + 0)

#define RPREFETCHSIZE (8 * 14 + 0)
#define WPREFETCHSIZE (8 *  6 + 0)

#define movlpd	movsd
#define movapd	movups
#define movupd	movups

#define KERNEL1(xx) \
	mulpd	%xmm1, %xmm0 ;\
	mulpd	-14 * SIZE(AO, %rax, 4), %xmm1 ;\
	addpd	%xmm0, %xmm8 ;\
	movapd	%xmm2, %xmm0 ;\
	addpd	%xmm1, %xmm12 ;\
	PREFETCH (PREFETCHSIZE +  0) * SIZE(AO, %rax, 4) ;\
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
	mulpd	-10 * SIZE(AO, %rax, 4), %xmm3 ;\
	addpd	%xmm2, %xmm11 ;\
	addpd	%xmm3, %xmm15 ;\
 	movddup	 -7 * SIZE(BO, %rax, 4), %xmm3 ;\
	movapd	%xmm4, %xmm2

#define KERNEL3(xx) \
	mulpd	%xmm5, %xmm4 ;\
	mulpd	 -6 * SIZE(AO, %rax, 4), %xmm5 ;\
	addpd	%xmm4, %xmm8 ;\
	movddup	  (BO, %rax, 4), %xmm1 ;\
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
	movapd	  (AO, %rax, 4), %xmm6 ;\
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
	movddup	  8 * SIZE(BO, %rax, 4), %xmm5 ;\
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
	movapd	  8 * SIZE(AO, %rax, 4), %xmm7 ;\
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
	movapd	 16 * SIZE(AO, %rax, 4), %xmm0 ;\
	addpd	%xmm1, %xmm14 ;\
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
	movddup	 16 * SIZE(BO, %rax, 4), %xmm1 ;\
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
	movapd	 24 * SIZE(AO, %rax, 4), %xmm4 ;\
	addpd	%xmm5, %xmm14 ;\
	movddup	 24 * SIZE(BO, %rax, 4), %xmm5 ;\
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

#ifndef __APPLE__
	.align 512
#endif
#if defined(OS_LINUX) && defined(CORE_BARCELONA)
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
	movaps	%xmm3, %xmm0
	movsd	OLD_ALPHA_I, %xmm1
#else
	movq	OLD_LDC,   LDC
#ifdef TRMMKERNEL
	movsd	OLD_OFFSET, %xmm12
#endif
#endif

	movq	%rsp, %rbx	# save old stack
	subq	$1024 + LOCAL_BUFFER_SIZE, %rsp
	andq	$-4096, %rsp	# align stack

	STACK_TOUCHING

	movq	OLD_M, M
	movq	OLD_N, N

	subq	$-16 * SIZE, A
#ifndef BUFFERED
	subq	$-16 * SIZE, B
#endif

	movsd	 %xmm0, 0 + ALPHA
	movsd	 %xmm1, 8 + ALPHA

	salq	$ZBASE_SHIFT, LDC

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
#ifdef BUFFERED
	leaq	16 * SIZE + BUFFER, BO
#endif
	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc

#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

#ifdef BUFFERED
	movq	K, %rax
	sarq	$2, %rax
	jle	.L03
	ALIGN_3

.L02:
	prefetch	 (RPREFETCHSIZE +  0)  * SIZE(B)

	movaps	 (B), %xmm0
	movaps	 2 * SIZE(B), %xmm1

	movaps	%xmm0, -16 * SIZE(BO)
	movaps	%xmm1, -14 * SIZE(BO)

	prefetch	 (RPREFETCHSIZE +  8)  * SIZE(B)

	movaps	 4 * SIZE(B), %xmm2
	movaps	 6 * SIZE(B), %xmm3

	movaps	%xmm2, -12 * SIZE(BO)
	movaps	%xmm3, -10 * SIZE(BO)

	prefetchw	 (WPREFETCHSIZE +  0)  * SIZE(BO)

	movaps	 8 * SIZE(B), %xmm4
	movaps	10 * SIZE(B), %xmm5

	movaps	%xmm4,  -8 * SIZE(BO)
	movaps	%xmm5,  -6 * SIZE(BO)

	prefetchw	 (WPREFETCHSIZE +  8)  * SIZE(BO)

	movaps	12 * SIZE(B), %xmm6
	movaps	14 * SIZE(B), %xmm7

	movaps	%xmm6,  -4 * SIZE(BO)
	movaps	%xmm7,  -2 * SIZE(BO)

	subq	$-16 * SIZE, BO
	subq	$-16 * SIZE, B

	subq	$1, %rax
	jne	.L02
	ALIGN_3

.L03:
	movq	K, %rax
	andq	$3, %rax
	BRANCH
	jle	.L10
	ALIGN_3

.L04:
	movaps	 (B), %xmm0
	movaps	%xmm0, -16 * SIZE(BO)

	movaps	 2 * SIZE(B), %xmm1
	movaps	%xmm1, -14 * SIZE(BO)

	addq	$4 * SIZE, B
	addq	$4 * SIZE, BO
	subq	$1, %rax
	jne	.L04
	ALIGN_4

.L10:
#endif
	movq	A, AO		# aoffset = a
	movq	B, BB

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L20
	ALIGN_4

.L11:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

#ifdef BUFFERED
	leaq	16 * SIZE + BUFFER, BO
#else
	movq	B, BO
#endif
#else
	leaq	16 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 4), BO
#endif

	prefetch	 (RPREFETCHSIZE +  0)  * SIZE(BB)
	prefetch	 (RPREFETCHSIZE +  8)  * SIZE(BB)
	prefetch	 (RPREFETCHSIZE + 16)  * SIZE(BB)
	subq		 $-16 * SIZE, BB

	movapd	-16 * SIZE(AO), %xmm0
	movddup	-16 * SIZE(BO), %xmm1
	pxor	%xmm8, %xmm8
 	movddup	-15 * SIZE(BO), %xmm3
	pxor	%xmm9, %xmm9
	movapd	 -8 * SIZE(AO), %xmm4
	pxor	%xmm10, %xmm10
	movddup	 -8 * SIZE(BO), %xmm5
	pxor	%xmm11, %xmm11

	prefetchw      7 * SIZE(CO1)
	pxor	%xmm12, %xmm12
	prefetchw      7 * SIZE(CO2)
	pxor	%xmm13, %xmm13
	prefetchw      7 * SIZE(CO1, LDC, 2)
	pxor	%xmm14, %xmm14
	prefetchw      7 * SIZE(CO2, LDC, 2)
	pxor	%xmm15, %xmm15
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
	addq	$4, %rax
#else
	addq	$4, %rax
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
	BRANCH
	jl	.L12
	ALIGN_4

.L15:
	movapd	ALPHA, %xmm7

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
	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm1
	movhpd	3 * SIZE(CO1), %xmm1

	movsd	4 * SIZE(CO1), %xmm2
	movhpd	5 * SIZE(CO1), %xmm2
	movsd	6 * SIZE(CO1), %xmm3
	movhpd	7 * SIZE(CO1), %xmm3

	movddup	 %xmm8,  %xmm4
	unpckhpd %xmm8,  %xmm8
	movddup	 %xmm12, %xmm5
	unpckhpd %xmm12, %xmm12

	mulpd	 %xmm7,  %xmm4
	mulpd	 %xmm7,  %xmm8
	mulpd	 %xmm7,  %xmm5
	mulpd	 %xmm7,  %xmm12

	addpd	 %xmm4,  %xmm0
	addpd	 %xmm8,  %xmm1
	addpd	 %xmm5,  %xmm2
	addpd	 %xmm12, %xmm3

	movsd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 1 * SIZE(CO1)
	movsd	%xmm1, 2 * SIZE(CO1)
	movhpd	%xmm1, 3 * SIZE(CO1)

	movsd	%xmm2, 4 * SIZE(CO1)
	movhpd	%xmm2, 5 * SIZE(CO1)
	movsd	%xmm3, 6 * SIZE(CO1)
	movhpd	%xmm3, 7 * SIZE(CO1)

	movsd	0 * SIZE(CO2), %xmm0
	movhpd	1 * SIZE(CO2), %xmm0
	movsd	2 * SIZE(CO2), %xmm1
	movhpd	3 * SIZE(CO2), %xmm1

	movsd	4 * SIZE(CO2), %xmm2
	movhpd	5 * SIZE(CO2), %xmm2
	movsd	6 * SIZE(CO2), %xmm3
	movhpd	7 * SIZE(CO2), %xmm3

	movddup	 %xmm9,  %xmm4
	unpckhpd %xmm9,  %xmm9
	movddup	 %xmm13, %xmm5
	unpckhpd %xmm13, %xmm13

	mulpd	 %xmm7,  %xmm4
	mulpd	 %xmm7,  %xmm9
	mulpd	 %xmm7,  %xmm5
	mulpd	 %xmm7,  %xmm13

	addpd	 %xmm4,  %xmm0
	addpd	 %xmm9,  %xmm1
	addpd	 %xmm5,  %xmm2
	addpd	 %xmm13, %xmm3

	movsd	%xmm0, 0 * SIZE(CO2)
	movhpd	%xmm0, 1 * SIZE(CO2)
	movsd	%xmm1, 2 * SIZE(CO2)
	movhpd	%xmm1, 3 * SIZE(CO2)

	movsd	%xmm2, 4 * SIZE(CO2)
	movhpd	%xmm2, 5 * SIZE(CO2)
	movsd	%xmm3, 6 * SIZE(CO2)
	movhpd	%xmm3, 7 * SIZE(CO2)

	movsd	0 * SIZE(CO1, LDC, 2), %xmm0
	movhpd	1 * SIZE(CO1, LDC, 2), %xmm0
	movsd	2 * SIZE(CO1, LDC, 2), %xmm1
	movhpd	3 * SIZE(CO1, LDC, 2), %xmm1

	movsd	4 * SIZE(CO1, LDC, 2), %xmm2
	movhpd	5 * SIZE(CO1, LDC, 2), %xmm2
	movsd	6 * SIZE(CO1, LDC, 2), %xmm3
	movhpd	7 * SIZE(CO1, LDC, 2), %xmm3

	movddup	 %xmm10, %xmm4
	unpckhpd %xmm10, %xmm10
	movddup	 %xmm14, %xmm5
	unpckhpd %xmm14, %xmm14

	mulpd	 %xmm7,  %xmm4
	mulpd	 %xmm7,  %xmm10
	mulpd	 %xmm7,  %xmm5
	mulpd	 %xmm7,  %xmm14

	addpd	 %xmm4,  %xmm0
	addpd	 %xmm10, %xmm1
	addpd	 %xmm5,  %xmm2
	addpd	 %xmm14, %xmm3

	movsd	%xmm0, 0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm0, 1 * SIZE(CO1, LDC, 2)
	movsd	%xmm1, 2 * SIZE(CO1, LDC, 2)
	movhpd	%xmm1, 3 * SIZE(CO1, LDC, 2)

	movsd	%xmm2, 4 * SIZE(CO1, LDC, 2)
	movhpd	%xmm2, 5 * SIZE(CO1, LDC, 2)
	movsd	%xmm3, 6 * SIZE(CO1, LDC, 2)
	movhpd	%xmm3, 7 * SIZE(CO1, LDC, 2)

	movsd	0 * SIZE(CO2, LDC, 2), %xmm0
	movhpd	1 * SIZE(CO2, LDC, 2), %xmm0
	movsd	2 * SIZE(CO2, LDC, 2), %xmm1
	movhpd	3 * SIZE(CO2, LDC, 2), %xmm1

	movsd	4 * SIZE(CO2, LDC, 2), %xmm2
	movhpd	5 * SIZE(CO2, LDC, 2), %xmm2
	movsd	6 * SIZE(CO2, LDC, 2), %xmm3
	movhpd	7 * SIZE(CO2, LDC, 2), %xmm3

	movddup	 %xmm11, %xmm4
	unpckhpd %xmm11, %xmm11
	movddup	 %xmm15, %xmm5
	unpckhpd %xmm15, %xmm15

	mulpd	 %xmm7,  %xmm4
	mulpd	 %xmm7,  %xmm11
	mulpd	 %xmm7,  %xmm5
	mulpd	 %xmm7,  %xmm15

	addpd	 %xmm4,  %xmm0
	addpd	 %xmm11, %xmm1
	addpd	 %xmm5,  %xmm2
	addpd	 %xmm15, %xmm3

	movsd	%xmm0, 0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm0, 1 * SIZE(CO2, LDC, 2)
	movsd	%xmm1, 2 * SIZE(CO2, LDC, 2)
	movhpd	%xmm1, 3 * SIZE(CO2, LDC, 2)

	movsd	%xmm2, 4 * SIZE(CO2, LDC, 2)
	movhpd	%xmm2, 5 * SIZE(CO2, LDC, 2)
	movsd	%xmm3, 6 * SIZE(CO2, LDC, 2)
	movhpd	%xmm3, 7 * SIZE(CO2, LDC, 2)

	addq	$8 * SIZE, CO1		# coffset += 4
	addq	$8 * SIZE, CO2		# coffset += 4

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

#ifdef BUFFERED
	leaq	16 * SIZE + BUFFER, BO
#else
	movq	B, BO
#endif
#else
	leaq	16 * SIZE + BUFFER, BO
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
	movapd	ALPHA, %xmm7

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
	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm1
	movhpd	3 * SIZE(CO1), %xmm1

	movddup	 %xmm8,  %xmm4
	unpckhpd %xmm8,  %xmm8

	mulpd	 %xmm7,  %xmm4
	mulpd	 %xmm7,  %xmm8
	addpd	 %xmm4,  %xmm0
	addpd	 %xmm8,  %xmm1

	movsd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 1 * SIZE(CO1)
	movsd	%xmm1, 2 * SIZE(CO1)
	movhpd	%xmm1, 3 * SIZE(CO1)

	movsd	0 * SIZE(CO2), %xmm0
	movhpd	1 * SIZE(CO2), %xmm0
	movsd	2 * SIZE(CO2), %xmm1
	movhpd	3 * SIZE(CO2), %xmm1

	movddup	 %xmm9,  %xmm4
	unpckhpd %xmm9,  %xmm9

	mulpd	 %xmm7,  %xmm4
	mulpd	 %xmm7,  %xmm9
	addpd	 %xmm4,  %xmm0
	addpd	 %xmm9,  %xmm1

	movsd	%xmm0, 0 * SIZE(CO2)
	movhpd	%xmm0, 1 * SIZE(CO2)
	movsd	%xmm1, 2 * SIZE(CO2)
	movhpd	%xmm1, 3 * SIZE(CO2)

	movsd	0 * SIZE(CO1, LDC, 2), %xmm0
	movhpd	1 * SIZE(CO1, LDC, 2), %xmm0
	movsd	2 * SIZE(CO1, LDC, 2), %xmm1
	movhpd	3 * SIZE(CO1, LDC, 2), %xmm1

	movddup	 %xmm10, %xmm4
	unpckhpd %xmm10, %xmm10

	mulpd	 %xmm7,  %xmm4
	mulpd	 %xmm7,  %xmm10
	addpd	 %xmm4,  %xmm0
	addpd	 %xmm10, %xmm1

	movsd	%xmm0, 0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm0, 1 * SIZE(CO1, LDC, 2)
	movsd	%xmm1, 2 * SIZE(CO1, LDC, 2)
	movhpd	%xmm1, 3 * SIZE(CO1, LDC, 2)

	movsd	0 * SIZE(CO2, LDC, 2), %xmm0
	movhpd	1 * SIZE(CO2, LDC, 2), %xmm0
	movsd	2 * SIZE(CO2, LDC, 2), %xmm1
	movhpd	3 * SIZE(CO2, LDC, 2), %xmm1

	movddup	 %xmm11, %xmm4
	unpckhpd %xmm11, %xmm11

	mulpd	 %xmm7,  %xmm4
	mulpd	 %xmm7,  %xmm11
	addpd	 %xmm4,  %xmm0
	addpd	 %xmm11, %xmm1

	movsd	%xmm0, 0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm0, 1 * SIZE(CO2, LDC, 2)
	movsd	%xmm1, 2 * SIZE(CO2, LDC, 2)
	movhpd	%xmm1, 3 * SIZE(CO2, LDC, 2)

	addq	$4 * SIZE, CO1
	addq	$4 * SIZE, CO2
	ALIGN_4

.L30:
	testq	$1, M
	je	.L39

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

#ifdef BUFFERED
	leaq	16 * SIZE + BUFFER, BO
#else
	movq	B, BO
#endif
#else
	leaq	16 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
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
	movapd	ALPHA, %xmm7

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


	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0

	movddup	 %xmm8,  %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	movsd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 1 * SIZE(CO1)

	movsd	0 * SIZE(CO2), %xmm0
	movhpd	1 * SIZE(CO2), %xmm0

	unpckhpd %xmm8,  %xmm8
	mulpd	 %xmm7,  %xmm8
	addpd	 %xmm8,  %xmm0

	movsd	%xmm0, 0 * SIZE(CO2)
	movhpd	%xmm0, 1 * SIZE(CO2)

	movsd	0 * SIZE(CO1, LDC, 2), %xmm0
	movhpd	1 * SIZE(CO1, LDC, 2), %xmm0

	movddup	 %xmm9,  %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	movsd	%xmm0, 0 * SIZE(CO1, LDC, 2)
	movhpd	%xmm0, 1 * SIZE(CO1, LDC, 2)

	movsd	0 * SIZE(CO2, LDC, 2), %xmm0
	movhpd	1 * SIZE(CO2, LDC, 2), %xmm0

	unpckhpd %xmm9,  %xmm9
	mulpd	 %xmm7,  %xmm9
	addpd	 %xmm9,  %xmm0

	movsd	%xmm0, 0 * SIZE(CO2, LDC, 2)
	movhpd	%xmm0, 1 * SIZE(CO2, LDC, 2)

	ALIGN_4

.L39:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$4, KK
#endif

#ifndef BUFFERED
	movq	BO, B
#endif

	leaq	(C, LDC, 4), C		# c += 4 * ldc
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
#ifdef BUFFERED
	leaq	16 * SIZE + BUFFER, BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

#ifdef BUFFERED
	movq	K, %rax
	sarq	$2, %rax
	jle	.L43
	ALIGN_4

.L42:
	prefetchnta	 (RPREFETCHSIZE +  0)  * SIZE(B)

	movaps	 (B), %xmm0
	movaps	%xmm0, -16 * SIZE(BO)

	movaps	 2 * SIZE(B), %xmm1
	movaps	%xmm1, -14 * SIZE(BO)

	prefetchw	 (WPREFETCHSIZE +  0)  * SIZE(BO)

	movaps	 4 * SIZE(B), %xmm2
	movaps	%xmm2, -12 * SIZE(BO)

	movaps	 6 * SIZE(B), %xmm3
	movaps	%xmm3, -10 * SIZE(BO)

	subq	$-8 * SIZE, BO
	subq	$-8 * SIZE, B

	subq	$1, %rax
	jne	.L42
	ALIGN_4

.L43:
	movq	K, %rax
	andq	$3, %rax
	BRANCH
	jle	.L50
	ALIGN_4

.L44:
	movaps	 (B), %xmm0
	movaps	%xmm0, -16 * SIZE(BO)

	addq	$2 * SIZE, B
	addq	$2 * SIZE, BO
	subq	$1, %rax
	jne	.L44
	ALIGN_4

.L50:
#endif
	movq	C, CO1			# coffset1 = c
	leaq	(C, LDC, 1), CO2	# coffset2 = c + ldc
	movq	A, AO		# aoffset = a

	movq	M,  I
	sarq	$2, I	# i = (m >> 2)
	jle	.L60
	ALIGN_4

.L51:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

#ifdef BUFFERED
	leaq	16 * SIZE + BUFFER, BO
#else
	movq	B, BO
#endif
#else
	leaq	16 * SIZE + BUFFER, BO
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
	prefetchw      7 * SIZE(CO1)
	movapd	%xmm0, %xmm2
	prefetchw      7 * SIZE(CO2)

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
	movapd	ALPHA, %xmm7

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
	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm1
	movhpd	3 * SIZE(CO1), %xmm1

	movsd	4 * SIZE(CO1), %xmm2
	movhpd	5 * SIZE(CO1), %xmm2
	movsd	6 * SIZE(CO1), %xmm3
	movhpd	7 * SIZE(CO1), %xmm3

	movddup	 %xmm8,  %xmm4
	unpckhpd %xmm8,  %xmm8
	movddup	 %xmm12, %xmm5
	unpckhpd %xmm12, %xmm12

	mulpd	 %xmm7,  %xmm4
	mulpd	 %xmm7,  %xmm8
	mulpd	 %xmm7,  %xmm5
	mulpd	 %xmm7,  %xmm12

	addpd	 %xmm4,  %xmm0
	addpd	 %xmm8,  %xmm1
	addpd	 %xmm5,  %xmm2
	addpd	 %xmm12, %xmm3

	movsd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 1 * SIZE(CO1)
	movsd	%xmm1, 2 * SIZE(CO1)
	movhpd	%xmm1, 3 * SIZE(CO1)

	movsd	%xmm2, 4 * SIZE(CO1)
	movhpd	%xmm2, 5 * SIZE(CO1)
	movsd	%xmm3, 6 * SIZE(CO1)
	movhpd	%xmm3, 7 * SIZE(CO1)

	movsd	0 * SIZE(CO2), %xmm0
	movhpd	1 * SIZE(CO2), %xmm0
	movsd	2 * SIZE(CO2), %xmm1
	movhpd	3 * SIZE(CO2), %xmm1

	movsd	4 * SIZE(CO2), %xmm2
	movhpd	5 * SIZE(CO2), %xmm2
	movsd	6 * SIZE(CO2), %xmm3
	movhpd	7 * SIZE(CO2), %xmm3

	movddup	 %xmm9,  %xmm4
	unpckhpd %xmm9,  %xmm9
	movddup	 %xmm13, %xmm5
	unpckhpd %xmm13, %xmm13

	mulpd	 %xmm7,  %xmm4
	mulpd	 %xmm7,  %xmm9
	mulpd	 %xmm7,  %xmm5
	mulpd	 %xmm7,  %xmm13

	addpd	 %xmm4,  %xmm0
	addpd	 %xmm9,  %xmm1
	addpd	 %xmm5,  %xmm2
	addpd	 %xmm13, %xmm3

	movsd	%xmm0, 0 * SIZE(CO2)
	movhpd	%xmm0, 1 * SIZE(CO2)
	movsd	%xmm1, 2 * SIZE(CO2)
	movhpd	%xmm1, 3 * SIZE(CO2)

	movsd	%xmm2, 4 * SIZE(CO2)
	movhpd	%xmm2, 5 * SIZE(CO2)
	movsd	%xmm3, 6 * SIZE(CO2)
	movhpd	%xmm3, 7 * SIZE(CO2)

	addq	$8 * SIZE, CO1		# coffset += 4
	addq	$8 * SIZE, CO2		# coffset += 4

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

#ifdef BUFFERED
	leaq	16 * SIZE + BUFFER, BO
#else
	movq	B, BO
#endif
#else
	leaq	16 * SIZE + BUFFER, BO
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
	movapd	ALPHA, %xmm7

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

	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm1
	movhpd	3 * SIZE(CO1), %xmm1

	movddup	 %xmm8,  %xmm4
	unpckhpd %xmm8,  %xmm8

	mulpd	 %xmm7,  %xmm4
	mulpd	 %xmm7,  %xmm8
	addpd	 %xmm4,  %xmm0
	addpd	 %xmm8,  %xmm1

	movsd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 1 * SIZE(CO1)
	movsd	%xmm1, 2 * SIZE(CO1)
	movhpd	%xmm1, 3 * SIZE(CO1)

	movsd	0 * SIZE(CO2), %xmm0
	movhpd	1 * SIZE(CO2), %xmm0
	movsd	2 * SIZE(CO2), %xmm1
	movhpd	3 * SIZE(CO2), %xmm1

	movddup	 %xmm9,  %xmm4
	unpckhpd %xmm9,  %xmm9

	mulpd	 %xmm7,  %xmm4
	mulpd	 %xmm7,  %xmm9
	addpd	 %xmm4,  %xmm0
	addpd	 %xmm9,  %xmm1

	movsd	%xmm0, 0 * SIZE(CO2)
	movhpd	%xmm0, 1 * SIZE(CO2)
	movsd	%xmm1, 2 * SIZE(CO2)
	movhpd	%xmm1, 3 * SIZE(CO2)

	addq	$4 * SIZE, CO1		# coffset += 4
	addq	$4 * SIZE, CO2		# coffset += 4
	ALIGN_4

.L70:
	testq	$1, M
	je	.L79
	ALIGN_4

.L71:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

#ifdef BUFFERED
	leaq	16 * SIZE + BUFFER, BO
#else
	movq	B, BO
#endif
#else
	leaq	16 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 2), BO
#endif

	movddup	-16 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movddup	-15 * SIZE(AO), %xmm1
	pxor	%xmm9, %xmm9
	movddup	-14 * SIZE(AO), %xmm2
	pxor	%xmm10, %xmm10
	movddup	-13 * SIZE(AO), %xmm3
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
	movapd	ALPHA, %xmm7

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

	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0

	movddup	 %xmm8,  %xmm4
	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	movsd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 1 * SIZE(CO1)

	movsd	0 * SIZE(CO2), %xmm0
	movhpd	1 * SIZE(CO2), %xmm0

	unpckhpd %xmm8,  %xmm8
	mulpd	 %xmm7,  %xmm8
	addpd	 %xmm8,  %xmm0

	movsd	%xmm0, 0 * SIZE(CO2)
	movhpd	%xmm0, 1 * SIZE(CO2)
	ALIGN_4

.L79:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

#ifndef BUFFERED
	movq	BO, B
#endif

	leaq	(C, LDC, 2), C
	ALIGN_4

.L80:
	testq	$1, N
	je	.L999
	ALIGN_4

.L81:
#ifdef BUFFERED
	leaq	16 * SIZE + BUFFER, BO
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	movq	OFFSET, %rax
	movq	%rax, KK
#endif

#ifdef BUFFERED
	movq	K, %rax
	sarq	$3, %rax
	jle	.L83
	ALIGN_4

.L82:
	prefetchnta	 (RPREFETCHSIZE +  0)  * SIZE(B)

	movaps	 (B), %xmm0
	movaps	%xmm0, -16 * SIZE(BO)

	movaps	 2 * SIZE(B), %xmm1
	movaps	%xmm1, -14 * SIZE(BO)

	prefetchw	 (WPREFETCHSIZE +  0)  * SIZE(BO)

	movaps	 4 * SIZE(B), %xmm2
	movaps	%xmm2, -12 * SIZE(BO)

	movaps	 6 * SIZE(B), %xmm3
	movaps	%xmm3, -10 * SIZE(BO)

	subq	$-8 * SIZE, BO
	subq	$-8 * SIZE, B

	subq	$1, %rax
	jne	.L82
	ALIGN_4

.L83:
	movq	K, %rax
	andq	$7, %rax
	BRANCH
	jle	.L90
	ALIGN_4

.L84:
	movsd	 (B), %xmm0
	movlpd	%xmm0, -16 * SIZE(BO)

	addq	$1 * SIZE, B
	addq	$1 * SIZE, BO
	decq	%rax
	jne	.L84
	ALIGN_4

.L90:
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

#ifdef BUFFERED
	leaq	16 * SIZE + BUFFER, BO
#else
	movq	B, BO
#endif
#else
	leaq	16 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 4), AO
	leaq	(BO, %rax, 1), BO
#endif

	movapd	 -8 * SIZE(AO), %xmm2
	pxor	%xmm8, %xmm8
	movapd	-16 * SIZE(AO), %xmm0
	pxor	%xmm9, %xmm9
	movddup	-16 * SIZE(BO), %xmm1
	pxor	%xmm12, %xmm12
	movddup	-14 * SIZE(BO), %xmm3
	pxor	%xmm13, %xmm13
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
	movapd	ALPHA, %xmm7

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

	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm1
	movhpd	3 * SIZE(CO1), %xmm1

	movsd	4 * SIZE(CO1), %xmm2
	movhpd	5 * SIZE(CO1), %xmm2
	movsd	6 * SIZE(CO1), %xmm3
	movhpd	7 * SIZE(CO1), %xmm3

	movddup	 %xmm8,  %xmm4
	unpckhpd %xmm8,  %xmm8
	movddup	 %xmm12, %xmm5
	unpckhpd %xmm12, %xmm12

	mulpd	 %xmm7,  %xmm4
	mulpd	 %xmm7,  %xmm8
	mulpd	 %xmm7,  %xmm5
	mulpd	 %xmm7,  %xmm12

	addpd	 %xmm4,  %xmm0
	addpd	 %xmm8,  %xmm1
	addpd	 %xmm5,  %xmm2
	addpd	 %xmm12, %xmm3

	movsd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 1 * SIZE(CO1)
	movsd	%xmm1, 2 * SIZE(CO1)
	movhpd	%xmm1, 3 * SIZE(CO1)

	movsd	%xmm2, 4 * SIZE(CO1)
	movhpd	%xmm2, 5 * SIZE(CO1)
	movsd	%xmm3, 6 * SIZE(CO1)
	movhpd	%xmm3, 7 * SIZE(CO1)

	addq	$8 * SIZE, CO1		# coffset += 4

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

#ifdef BUFFERED
	leaq	16 * SIZE + BUFFER, BO
#else
	movq	B, BO
#endif
#else
	leaq	16 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 2), AO
	leaq	(BO, %rax, 1), BO
#endif

	movddup	-16 * SIZE(BO), %xmm0
	pxor	%xmm8, %xmm8
	movddup	-15 * SIZE(BO), %xmm1
	pxor	%xmm9, %xmm9
	movddup	-14 * SIZE(BO), %xmm2
	pxor	%xmm10, %xmm10
	movddup	-13 * SIZE(BO), %xmm3
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
	movapd	ALPHA, %xmm7

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

	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0
	movsd	2 * SIZE(CO1), %xmm1
	movhpd	3 * SIZE(CO1), %xmm1

	movddup	 %xmm8,  %xmm4
	unpckhpd %xmm8,  %xmm8

	mulpd	 %xmm7,  %xmm4
	mulpd	 %xmm7,  %xmm8
	addpd	 %xmm4,  %xmm0
	addpd	 %xmm8,  %xmm1

	movsd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 1 * SIZE(CO1)
	movsd	%xmm1, 2 * SIZE(CO1)
	movhpd	%xmm1, 3 * SIZE(CO1)

	addq	$4 * SIZE, CO1
	ALIGN_4

.L110:
	testq	$1, M
	je	.L999
	ALIGN_4

.L111:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

#ifdef BUFFERED
	leaq	16 * SIZE + BUFFER, BO
#else
	movq	B, BO
#endif
#else
	leaq	16 * SIZE + BUFFER, BO
	movq	KK, %rax
	leaq	(, %rax, SIZE), %rax
	leaq	(AO, %rax, 1), AO
	leaq	(BO, %rax, 1), BO
#endif

	movapd	-16 * SIZE(AO), %xmm0
	pxor	%xmm8, %xmm8
	movapd	-14 * SIZE(AO), %xmm1
	pxor	%xmm9, %xmm9

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
	movapd	ALPHA, %xmm7

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

	movsd	0 * SIZE(CO1), %xmm0
	movhpd	1 * SIZE(CO1), %xmm0

	movddup	 %xmm8,  %xmm4

	mulpd	 %xmm7,  %xmm4
	addpd	 %xmm4,  %xmm0

	movsd	%xmm0, 0 * SIZE(CO1)
	movhpd	%xmm0, 1 * SIZE(CO1)
	ALIGN_3

.L999:
	movq	%rbx, %rsp

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
