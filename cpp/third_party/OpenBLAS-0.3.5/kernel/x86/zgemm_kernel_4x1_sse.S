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

#if !defined(HAVE_SSE) || !defined(HAVE_MMX)
#error  You have to check your configuration.
#endif

#define STACK	16
#define ARGS	 0

#define STACK_M	 4 + STACK + ARGS(%esi)
#define STACK_N	 8 + STACK + ARGS(%esi)
#define STACK_K	12 + STACK + ARGS(%esi)
#define STACK_ALPHA_R	16 + STACK + ARGS(%esi)
#define STACK_ALPHA_I	20 + STACK + ARGS(%esi)
#define STACK_A	24 + STACK + ARGS(%esi)
#define STACK_B	28 + STACK + ARGS(%esi)
#define STACK_C	32 + STACK + ARGS(%esi)
#define STACK_LDC	36 + STACK + ARGS(%esi)
#define STACK_OFFT	40 + STACK + ARGS(%esi)

#define POSINV	 0(%esp)
#define ALPHA_R	16(%esp)
#define ALPHA_I	32(%esp)
#define K	48(%esp)
#define N	52(%esp)
#define M	56(%esp)
#define A	60(%esp)
#define C	64(%esp)
#define J	68(%esp)
#define OLD_STACK 72(%esp)
#define TEMP    76(%esp)
#define OFFSET  80(%esp)
#define KK	84(%esp)
#define KKK	88(%esp)
#define BUFFER 128(%esp)

#define B	%edi
#define LDC	%ebp

#define STACK_ALIGN	4096
#define STACK_OFFSET	1024

#define AA	%edx
#define BB	%ecx

#if !defined(HAVE_SSE2) || defined(OPTERON)
#define movsd	movlps
#endif

#ifdef HAVE_SSE2
#define xorps	pxor
#endif

#define KERNEL1(address) \
	mulps	%xmm0, %xmm2; \
	mulps	 4 * SIZE + (address) * SIZE * 2(BB), %xmm0; \
	addps	%xmm2, %xmm4; \
	movaps	 0 * SIZE + (address) * SIZE * 2(BB), %xmm2; \
	addps	%xmm0, %xmm5; \
	movaps	 4 * SIZE + (address) * SIZE * 2(AA), %xmm0; \
	mulps	%xmm0, %xmm2; \
	mulps	 4 * SIZE + (address) * SIZE * 2(BB), %xmm0; \
	addps	%xmm2, %xmm6; \
	movaps	 8 * SIZE + (address) * SIZE * 2(BB), %xmm2; \
	addps	%xmm0, %xmm7; \
	movaps	 8 * SIZE + (address) * SIZE * 2(AA), %xmm0

#define KERNEL2(address) \
	mulps	%xmm0, %xmm2; \
	mulps	12 * SIZE + (address) * SIZE * 2(BB), %xmm0; \
	addps	%xmm2, %xmm4; \
	movaps	 8 * SIZE + (address) * SIZE * 2(BB), %xmm2; \
	addps	%xmm0, %xmm5; \
	movaps	12 * SIZE + (address) * SIZE * 2(AA), %xmm0; \
	mulps	%xmm0, %xmm2; \
	mulps	12 * SIZE + (address) * SIZE * 2(BB), %xmm0; \
	addps	%xmm2, %xmm6; \
	movaps	32 * SIZE + (address) * SIZE * 2(BB), %xmm2; \
	addps	%xmm0, %xmm7; \
	movaps	32 * SIZE + (address) * SIZE * 2(AA), %xmm0

#define KERNEL3(address) \
	mulps	%xmm1, %xmm3; \
	mulps	20 * SIZE + (address) * SIZE * 2(BB), %xmm1; \
	addps	%xmm3, %xmm4; \
	movaps	16 * SIZE + (address) * SIZE * 2(BB), %xmm3; \
	addps	%xmm1, %xmm5; \
	movaps	20 * SIZE + (address) * SIZE * 2(AA), %xmm1; \
	mulps	%xmm1, %xmm3; \
	mulps	20 * SIZE + (address) * SIZE * 2(BB), %xmm1; \
	addps	%xmm3, %xmm6; \
	movaps	24 * SIZE + (address) * SIZE * 2(BB), %xmm3; \
	addps	%xmm1, %xmm7; \
	movaps	24 * SIZE + (address) * SIZE * 2(AA), %xmm1

#define KERNEL4(address) \
	mulps	%xmm1, %xmm3; \
	mulps	28 * SIZE + (address) * SIZE * 2(BB), %xmm1; \
	addps	%xmm3, %xmm4; \
	movaps	24 * SIZE + (address) * SIZE * 2(BB), %xmm3; \
	addps	%xmm1, %xmm5; \
	movaps	28 * SIZE + (address) * SIZE * 2(AA), %xmm1; \
	mulps	%xmm1, %xmm3; \
	mulps	28 * SIZE + (address) * SIZE * 2(BB), %xmm1; \
	addps	%xmm3, %xmm6; \
	movaps	48 * SIZE + (address) * SIZE * 2(BB), %xmm3; \
	addps	%xmm1, %xmm7; \
	movaps	48 * SIZE + (address) * SIZE * 2(AA), %xmm1

#define KERNEL5(address) \
	mulps	%xmm0, %xmm2; \
	mulps	36 * SIZE + (address) * SIZE * 2(BB), %xmm0; \
	addps	%xmm2, %xmm4; \
	movaps	32 * SIZE + (address) * SIZE * 2(BB), %xmm2; \
	addps	%xmm0, %xmm5; \
	movaps	36 * SIZE + (address) * SIZE * 2(AA), %xmm0; \
	mulps	%xmm0, %xmm2; \
	mulps	36 * SIZE + (address) * SIZE * 2(BB), %xmm0; \
	addps	%xmm2, %xmm6; \
	movaps	40 * SIZE + (address) * SIZE * 2(BB), %xmm2; \
	addps	%xmm0, %xmm7; \
	movaps	40 * SIZE + (address) * SIZE * 2(AA), %xmm0

#define KERNEL6(address) \
	mulps	%xmm0, %xmm2; \
	mulps	44 * SIZE + (address) * SIZE * 2(BB), %xmm0; \
	addps	%xmm2, %xmm4; \
	movaps	40 * SIZE + (address) * SIZE * 2(BB), %xmm2; \
	addps	%xmm0, %xmm5; \
	movaps	44 * SIZE + (address) * SIZE * 2(AA), %xmm0; \
	mulps	%xmm0, %xmm2; \
	mulps	44 * SIZE + (address) * SIZE * 2(BB), %xmm0; \
	addps	%xmm2, %xmm6; \
	movaps	64 * SIZE + (address) * SIZE * 2(BB), %xmm2; \
	addps	%xmm0, %xmm7; \
	movaps	64 * SIZE + (address) * SIZE * 2(AA), %xmm0

#define KERNEL7(address) \
	mulps	%xmm1, %xmm3; \
	mulps	52 * SIZE + (address) * SIZE * 2(BB), %xmm1; \
	addps	%xmm3, %xmm4; \
	movaps	48 * SIZE + (address) * SIZE * 2(BB), %xmm3; \
	addps	%xmm1, %xmm5; \
	movaps	52 * SIZE + (address) * SIZE * 2(AA), %xmm1; \
	mulps	%xmm1, %xmm3; \
	mulps	52 * SIZE + (address) * SIZE * 2(BB), %xmm1; \
	addps	%xmm3, %xmm6; \
	movaps	56 * SIZE + (address) * SIZE * 2(BB), %xmm3; \
	addps	%xmm1, %xmm7; \
	movaps	56 * SIZE + (address) * SIZE * 2(AA), %xmm1

#define KERNEL8(address) \
	mulps	%xmm1, %xmm3; \
	mulps	60 * SIZE + (address) * SIZE * 2(BB), %xmm1; \
	addps	%xmm3, %xmm4; \
	movaps	56 * SIZE + (address) * SIZE * 2(BB), %xmm3; \
	addps	%xmm1, %xmm5; \
	movaps	60 * SIZE + (address) * SIZE * 2(AA), %xmm1; \
	mulps	%xmm1, %xmm3; \
	mulps	60 * SIZE + (address) * SIZE * 2(BB), %xmm1; \
	addps	%xmm3, %xmm6; \
	movaps	80 * SIZE + (address) * SIZE * 2(BB), %xmm3; \
	addps	%xmm1, %xmm7; \
	movaps	80 * SIZE + (address) * SIZE * 2(AA), %xmm1

	PROLOGUE

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	EMMS

	movl	%esp, %esi	# save old stack

	subl	$128 + LOCAL_BUFFER_SIZE + STACK_OFFSET, %esp
	andl	$-STACK_ALIGN, %esp	# align stack
	addl	$STACK_OFFSET, %esp

	STACK_TOUCHING

	movd	STACK_M, %mm0
	movl	STACK_N, %eax
	movd	STACK_K, %mm1
	movd	STACK_A, %mm2
	movl	STACK_B, B
	movd	STACK_C, %mm3
	movl	STACK_LDC, LDC
#ifdef TRMMKERNEL
	movd	STACK_OFFT, %mm4
#endif

	movd	%mm1, K
	movd	%mm0, M
	movl	%eax, N
	movd	%mm2, A
	movd	%mm3, C
	movl	%esi, OLD_STACK
#ifdef TRMMKERNEL
	movd	%mm4, OFFSET
	movd	%mm4, KK
#ifndef LEFT
	negl	KK
#endif
#endif

	leal	(, LDC, SIZE * 2), LDC

	movss	STACK_ALPHA_R, %xmm0
	movss	STACK_ALPHA_I, %xmm1

#ifdef HAVE_SSE2
	pxor	%xmm7, %xmm7
	cmpeqps	%xmm7, %xmm7
	pslld	$31, %xmm7	# Generate mask
#else
	movl	$0x80000000, TEMP
	movss	TEMP, %xmm7
	shufps	$0, %xmm7, %xmm7
#endif
	xorps	%xmm2, %xmm2

	shufps	$0, %xmm0, %xmm0

	movaps	 %xmm0,  0 + ALPHA_R
	movss	 %xmm1,  4 + ALPHA_I
	movss	 %xmm1, 12 + ALPHA_I
	xorps	 %xmm7, %xmm1
	movss	 %xmm1,  0 + ALPHA_I
	movss	 %xmm1,  8 + ALPHA_I

#if   defined(NN) || defined(NT) || defined(NR) || defined(NC) || \
      defined(TN) || defined(TT) || defined(TR) || defined(TC)
	movss	  %xmm7,   0 + POSINV
	movss	  %xmm2,   4 + POSINV
	movss	  %xmm7,   8 + POSINV
	movss	  %xmm2,  12 + POSINV
#else
	movss	  %xmm2,   0 + POSINV
	movss	  %xmm7,   4 + POSINV
	movss	  %xmm2,   8 + POSINV
	movss	  %xmm7,  12 + POSINV
#endif

	movl	%eax, J			# j = n
	testl	%eax, %eax
	jle	.L999

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

 	leal	BUFFER, BB
	movaps	POSINV, %xmm7

	movl	K, %eax
	sarl	$2, %eax
	jle	.L03

.L02:
	movss	0 * SIZE(B), %xmm0
	movss	1 * SIZE(B), %xmm1
	movss	2 * SIZE(B), %xmm2
	movss	3 * SIZE(B), %xmm3

	shufps	$0, %xmm0, %xmm0
	shufps	$0, %xmm1, %xmm1
	shufps	$0, %xmm2, %xmm2
	shufps	$0, %xmm3, %xmm3

#if defined(NN) || defined(NT) || defined(NR) || defined(NC)  || \
    defined(TN) || defined(TT) || defined(TR) || defined(TC)
	xorps	 %xmm7, %xmm1
	xorps	 %xmm7, %xmm3
#else
	xorps	 %xmm7, %xmm0
	xorps	 %xmm7, %xmm2
#endif

	movaps	%xmm0,  0 * SIZE(BB)
	movaps	%xmm1,  4 * SIZE(BB)
	movaps	%xmm2,  8 * SIZE(BB)
	movaps	%xmm3, 12 * SIZE(BB)

	movss	4 * SIZE(B), %xmm0
	movss	5 * SIZE(B), %xmm1
	movss	6 * SIZE(B), %xmm2
	movss	7 * SIZE(B), %xmm3

	shufps	$0, %xmm0, %xmm0
	shufps	$0, %xmm1, %xmm1
	shufps	$0, %xmm2, %xmm2
	shufps	$0, %xmm3, %xmm3

#if defined(NN) || defined(NT) || defined(NR) || defined(NC)  || \
    defined(TN) || defined(TT) || defined(TR) || defined(TC)
	xorps	 %xmm7, %xmm1
	xorps	 %xmm7, %xmm3
#else
	xorps	 %xmm7, %xmm0
	xorps	 %xmm7, %xmm2
#endif

	movaps	%xmm0, 16 * SIZE(BB)
	movaps	%xmm1, 20 * SIZE(BB)
	movaps	%xmm2, 24 * SIZE(BB)
	movaps	%xmm3, 28 * SIZE(BB)

	prefetcht0	 104 * SIZE(B)

	addl	$ 8 * SIZE, B
	addl	$32 * SIZE, BB
	decl	%eax
	jne	.L02

.L03:
	movl	K, %eax
	andl	$3, %eax
	BRANCH
	jle	.L05

.L04:
	movss	0 * SIZE(B), %xmm0
	movss	1 * SIZE(B), %xmm1

	shufps	$0, %xmm0, %xmm0
	shufps	$0, %xmm1, %xmm1

#if defined(NN) || defined(NT) || defined(NR) || defined(NC)  || \
    defined(TN) || defined(TT) || defined(TR) || defined(TC)
	xorps	 %xmm7, %xmm1
#else
	xorps	 %xmm7, %xmm0
#endif

	movaps	%xmm0,  0 * SIZE(BB)
	movaps	%xmm1,  4 * SIZE(BB)

	addl	$2 * SIZE, B
	addl	$8 * SIZE, BB
	decl	%eax
	jne	.L04
	ALIGN_4

.L05:
	movl	C, %esi		# coffset = c
	movl	A, AA		# aoffset = a
	movl	M,  %ebx
	sarl	$2, %ebx	# i = (m >> 2)
	jle	.L50
	ALIGN_4

.L10:

#ifdef PENTIUM4

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
	movaps	 0 * SIZE + BUFFER, %xmm2
	xorps	%xmm4, %xmm4
	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 16 * SIZE + BUFFER, %xmm3
	xorps	%xmm6, %xmm6
	movaps	 16 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7

#else

	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 4), BB /* because it's doubled */

	movaps	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 16 * SIZE(BB), %xmm3
	xorps	%xmm6, %xmm6
	movaps	 16 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7

#endif

	prefetchnta    8 * SIZE(%esi)

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$4, %eax
#else
	addl	$1, %eax
#endif
	movl	%eax, KKK
#endif
	andl	$-8, %eax
	je	.L12
	sall	$3, %eax

.L1X:
	KERNEL1(32  *  0)
	KERNEL2(32  *  0)
	KERNEL3(32  *  0)
	KERNEL4(32  *  0)
	KERNEL5(32  *  0)
	KERNEL6(32  *  0)
	KERNEL7(32  *  0)
	KERNEL8(32  *  0)
	cmpl	$64 *  1, %eax
	NOBRANCH
	jle	.L11
	KERNEL1(32  *  1)
	KERNEL2(32  *  1)
	KERNEL3(32  *  1)
	KERNEL4(32  *  1)
	KERNEL5(32  *  1)
	KERNEL6(32  *  1)
	KERNEL7(32  *  1)
	KERNEL8(32  *  1)
	cmpl	$64 *  2, %eax
	NOBRANCH
	jle	.L11
	KERNEL1(32  *  2)
	KERNEL2(32  *  2)
	KERNEL3(32  *  2)
	KERNEL4(32  *  2)
	KERNEL5(32  *  2)
	KERNEL6(32  *  2)
	KERNEL7(32  *  2)
	KERNEL8(32  *  2)
	cmpl	$64 *  3, %eax
	NOBRANCH
	jle	.L11
	KERNEL1(32  *  3)
	KERNEL2(32  *  3)
	KERNEL3(32  *  3)
	KERNEL4(32  *  3)
	KERNEL5(32  *  3)
	KERNEL6(32  *  3)
	KERNEL7(32  *  3)
	KERNEL8(32  *  3)
	cmpl	$64 *  4, %eax
	NOBRANCH
	jle	.L11
	KERNEL1(32  *  4)
	KERNEL2(32  *  4)
	KERNEL3(32  *  4)
	KERNEL4(32  *  4)
	KERNEL5(32  *  4)
	KERNEL6(32  *  4)
	KERNEL7(32  *  4)
	KERNEL8(32  *  4)
	cmpl	$64 *  5, %eax
	NOBRANCH
	jle	.L11
	KERNEL1(32  *  5)
	KERNEL2(32  *  5)
	KERNEL3(32  *  5)
	KERNEL4(32  *  5)
	KERNEL5(32  *  5)
	KERNEL6(32  *  5)
	KERNEL7(32  *  5)
	KERNEL8(32  *  5)
	cmpl	$64 *  6, %eax
	NOBRANCH
	jle	.L11
	KERNEL1(32  *  6)
	KERNEL2(32  *  6)
	KERNEL3(32  *  6)
	KERNEL4(32  *  6)
	KERNEL5(32  *  6)
	KERNEL6(32  *  6)
	KERNEL7(32  *  6)
	KERNEL8(32  *  6)
	cmpl	$64 *  7, %eax
	NOBRANCH
	jle	.L11
	KERNEL1(32  *  7)
	KERNEL2(32  *  7)
	KERNEL3(32  *  7)
	KERNEL4(32  *  7)
	KERNEL5(32  *  7)
	KERNEL6(32  *  7)
	KERNEL7(32  *  7)
	KERNEL8(32  *  7)

	addl	$128 * 4  * SIZE, AA
	addl	$128 * 4  * SIZE, BB
	subl	$ 64 * 8, %eax
	BRANCH
	jg	.L1X

.L11:
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 4), BB

#else

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
	movaps	 0 * SIZE + BUFFER, %xmm2
	xorps	%xmm4, %xmm4
	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 8 * SIZE + BUFFER, %xmm3
	xorps	%xmm6, %xmm6
	movaps	 8 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7

#else

	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 4), BB /* because it's doubled */

	movaps	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 8 * SIZE(BB), %xmm3
	xorps	%xmm6, %xmm6
	movaps	 8 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7

#endif

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$4, %eax
#else
	addl	$1, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	prefetcht0   8 * SIZE(%esi)
	je	.L12
	ALIGN_4

#define PREFETCHSIZE 48

.L11:
#ifdef CORE_KATMAI
	prefetcht0	PREFETCHSIZE * SIZE(AA)
#endif

	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	 0 * SIZE(BB), %xmm2

	addps	%xmm0, %xmm5
	movaps	 4 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0

	addps	%xmm2, %xmm6
	movaps	16 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movaps	16 * SIZE(AA), %xmm0

#ifdef CORE_KATMAI
	prefetcht0	(PREFETCHSIZE + 8) * SIZE(AA)
#endif

	mulps	%xmm1, %xmm3
	mulps	12 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm4
	movaps	 8 * SIZE(BB), %xmm3

	addps	%xmm1, %xmm5
	movaps	12 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	mulps	12 * SIZE(BB), %xmm1

	addps	%xmm3, %xmm6
	movaps	24 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movaps	24 * SIZE(AA), %xmm1

#ifdef CORE_KATMAI
	prefetcht0	(PREFETCHSIZE + 16) * SIZE(AA)
#endif

	mulps	%xmm0, %xmm2
	mulps	20 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	16 * SIZE(BB), %xmm2

	addps	%xmm0, %xmm5
	movaps	20 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm2
	mulps	20 * SIZE(BB), %xmm0

	addps	%xmm2, %xmm6
	movaps	32 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movaps	32 * SIZE(AA), %xmm0

#ifdef CORE_KATMAI
	prefetcht0	(PREFETCHSIZE + 24) * SIZE(AA)
#endif

	mulps	%xmm1, %xmm3
	mulps	28 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm4
	movaps	24 * SIZE(BB), %xmm3

	addps	%xmm1, %xmm5
	movaps	28 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	mulps	28 * SIZE(BB), %xmm1

	addps	%xmm3, %xmm6
	movaps	40 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movaps	40 * SIZE(AA), %xmm1

#ifdef CORE_KATMAI
	prefetcht0	(PREFETCHSIZE + 32) * SIZE(AA)
#endif

	mulps	%xmm0, %xmm2
	mulps	36 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	32 * SIZE(BB), %xmm2

	addps	%xmm0, %xmm5
	movaps	36 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm2
	mulps	36 * SIZE(BB), %xmm0

	addps	%xmm2, %xmm6
	movaps	48 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movaps	48 * SIZE(AA), %xmm0

#ifdef CORE_KATMAI
	prefetcht0	(PREFETCHSIZE + 40) * SIZE(AA)
#endif

	mulps	%xmm1, %xmm3
	mulps	44 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm4
	movaps	40 * SIZE(BB), %xmm3

	addps	%xmm1, %xmm5
	movaps	44 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	mulps	44 * SIZE(BB), %xmm1

	addps	%xmm3, %xmm6
	movaps	56 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movaps	56 * SIZE(AA), %xmm1

#ifdef CORE_KATMAI
	prefetcht0	(PREFETCHSIZE + 48) * SIZE(AA)
#endif

	mulps	%xmm0, %xmm2
	mulps	52 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	48 * SIZE(BB), %xmm2

	addps	%xmm0, %xmm5
	movaps	52 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm2
	mulps	52 * SIZE(BB), %xmm0

	addps	%xmm2, %xmm6
	movaps	64 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movaps	64 * SIZE(AA), %xmm0

#ifdef CORE_KATMAI
	prefetcht0	(PREFETCHSIZE + 56) * SIZE(AA)
#endif

	mulps	%xmm1, %xmm3
	mulps	60 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm4
	movaps	56 * SIZE(BB), %xmm3

	addps	%xmm1, %xmm5
	movaps	60 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	mulps	60 * SIZE(BB), %xmm1

	addps	%xmm3, %xmm6
	movaps	72 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movaps	72 * SIZE(AA), %xmm1

	addl	$64 * SIZE, BB
	addl	$64 * SIZE, AA
	decl	%eax
	jne	.L11
#endif

.L12:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA_R, %xmm1
	movaps	ALPHA_I, %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L14

.L13:
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	 0 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	 4 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm6
	movaps	 8 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movaps	 8 * SIZE(AA), %xmm0

	addl	$8 * SIZE, AA		# aoffset  += 8
	addl	$8 * SIZE, BB		# boffset1 += 8

	decl	%eax
	jg	.L13

.L14:
	shufps	$0xb1, %xmm5, %xmm5
	shufps	$0xb1, %xmm7, %xmm7

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subps	%xmm5, %xmm4
	subps	%xmm7, %xmm6
#else
	addps	%xmm5, %xmm4
	addps	%xmm7, %xmm6
#endif

	movaps	%xmm4, %xmm5
	movaps	%xmm6, %xmm7

	shufps	$0xb1, %xmm4, %xmm4
	shufps	$0xb1, %xmm6, %xmm6

	mulps	%xmm1, %xmm5
	mulps	%xmm3, %xmm4
	mulps	%xmm1, %xmm7
	mulps	%xmm3, %xmm6

	addps	%xmm5, %xmm4
	addps	%xmm7, %xmm6

	shufps	$0xe4, %xmm4, %xmm4
	shufps	$0xe4, %xmm6, %xmm6

#ifndef TRMMKERNEL
	movsd	0 * SIZE(%esi), %xmm0
	movhps	2 * SIZE(%esi), %xmm0
	movsd	4 * SIZE(%esi), %xmm2
	movhps	6 * SIZE(%esi), %xmm2

	addps	%xmm0, %xmm4
	addps	%xmm2, %xmm6
#endif

	movsd	%xmm4, 0 * SIZE(%esi)
	movhps	%xmm4, 2 * SIZE(%esi)
	movsd	%xmm6, 4 * SIZE(%esi)
	movhps	%xmm6, 6 * SIZE(%esi)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax,    8), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$4, KK
#endif

	addl	$8 * SIZE, %esi		# coffset += 4
	decl	%ebx			# i --
	jg	.L10
	ALIGN_2

.L50:
	movl	M,  %ebx
	testl	$2, %ebx
	jle	.L70


#if (L1_DATA_LINESIZE == 64)

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
	movaps	 0 * SIZE + BUFFER, %xmm2
	xorps	%xmm4, %xmm4
	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 16 * SIZE + BUFFER, %xmm3
	xorps	%xmm6, %xmm6
	movaps	 16 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7

#else

	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB /* because it's doubled */

	movaps	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 16 * SIZE(BB), %xmm3
	xorps	%xmm6, %xmm6
	movaps	 16 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7

#endif

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$2, %eax
#else
	addl	$1, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L52
	ALIGN_4

.L51:
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	 8 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	 4 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm2
	mulps	12 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm6
	movaps	32 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movaps	 8 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm3
	mulps	20 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm4
	movaps	24 * SIZE(BB), %xmm3
	addps	%xmm0, %xmm5
	movaps	12 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm3
	mulps	28 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm6
	movaps	48 * SIZE(BB), %xmm3
	addps	%xmm0, %xmm7
	movaps	32 * SIZE(AA), %xmm0
	mulps	%xmm1, %xmm2
	mulps	36 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm4
	movaps	40 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm5
	movaps	20 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm2
	mulps	44 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm6
	movaps	64 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm7
	movaps	24 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	mulps	52 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm4
	movaps	56 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm5
	movaps	28 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	mulps	60 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm6
	movaps	80 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movaps	48 * SIZE(AA), %xmm1

	addl	$32 * SIZE, AA
	addl	$64 * SIZE, BB
	decl	%eax
	jne	.L51
	ALIGN_2

#else

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
	movaps	 0 * SIZE + BUFFER, %xmm2
	xorps	%xmm4, %xmm4
	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 8 * SIZE + BUFFER, %xmm3
	xorps	%xmm6, %xmm6
	movaps	 8 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7

#else

	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB /* because it's doubled */

	movaps	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 8 * SIZE(BB), %xmm3
	xorps	%xmm6, %xmm6
	movaps	 8 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7

#endif

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$2, %eax
#else
	addl	$1, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L52
	ALIGN_4

.L51:
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	16 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	 4 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm3
	mulps	12 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm6
	movaps	24 * SIZE(BB), %xmm3
	addps	%xmm0, %xmm7
	movaps	16 * SIZE(AA), %xmm0
	mulps	%xmm1, %xmm2
	mulps	20 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm4
	movaps	32 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm5
	movaps	12 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	mulps	28 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm6
	movaps	40 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movaps	24 * SIZE(AA), %xmm1
	mulps	%xmm0, %xmm2
	mulps	36 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	48 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	20 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm3
	mulps	44 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm6
	movaps	56 * SIZE(BB), %xmm3
	addps	%xmm0, %xmm7
	movaps	 32 * SIZE(AA), %xmm0
	mulps	%xmm1, %xmm2
	mulps	52 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm4
	movaps	 64 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm5
	movaps	28 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	mulps	60 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm6
	movaps	 72 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movaps	 40 * SIZE(AA), %xmm1

	addl	$32 * SIZE, AA
	addl	$64 * SIZE, BB
	decl	%eax
	jne	.L51
#endif

.L52:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA_R, %xmm1
	movaps	ALPHA_I, %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L54

.L53:
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	 8 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	 4 * SIZE(AA), %xmm0

	addl	$4 * SIZE, AA		# aoffset  += 8
	addl	$8 * SIZE, BB		# boffset1 += 8
	decl	%eax
	jg	.L53

.L54:
	addps	%xmm6, %xmm4
	addps	%xmm7, %xmm5

	shufps	$0xb1, %xmm5, %xmm5

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subps	%xmm5, %xmm4
#else
	addps	%xmm5, %xmm4
#endif

	movaps	%xmm4, %xmm5

	shufps	$0xb1, %xmm4, %xmm4

	mulps	%xmm1, %xmm5
	mulps	%xmm3, %xmm4

	addps	%xmm5, %xmm4

#ifndef TRMMKERNEL
	movsd	0 * SIZE(%esi), %xmm0
	movhps	2 * SIZE(%esi), %xmm0

	addps	%xmm0, %xmm4
#endif

	movlps	%xmm4, 0 * SIZE(%esi)
	movhps	%xmm4, 2 * SIZE(%esi)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax,    8), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif
	addl	$4 * SIZE, %esi		# coffset += 4
	ALIGN_2

.L70:
	testl	$1, %ebx
	jle	.L99


#if (L1_DATA_LINESIZE == 64)

#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
	movaps	 0 * SIZE + BUFFER, %xmm2
	xorps	%xmm4, %xmm4
#ifdef	movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 16 * SIZE + BUFFER, %xmm3
	xorps	%xmm6, %xmm6
#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	 8 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7

#else

	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB /* because it's doubled */

	movaps	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
#ifdef	movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 16 * SIZE(BB), %xmm3
	xorps	%xmm6, %xmm6
#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	 8 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7

#endif

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
	addl	$1, %eax
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L72
	ALIGN_4

.L71:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm6
	movaps	12 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm7
	movaps	32 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm4
	movaps	20 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movsd	 6 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	movaps	24 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm6
	movaps	28 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movsd	16 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm7
	movaps	48 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm4
	movaps	36 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movsd	10 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm5
	movaps	40 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm6
	movaps	44 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movsd	12 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm7
	movaps	64 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movaps	52 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	14 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movaps	56 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm6
	movaps	60 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	24 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm7
	movaps	80 * SIZE(BB), %xmm3

	addl	$16 * SIZE, AA
	addl	$64 * SIZE, BB
	decl	%eax
	jne	.L71
	ALIGN_2

#else
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
	movaps	 0 * SIZE + BUFFER, %xmm2
	xorps	%xmm4, %xmm4
	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 8 * SIZE + BUFFER, %xmm3
	xorps	%xmm6, %xmm6
	movaps	 8 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7

#else

	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB /* because it's doubled */

	movaps	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
#ifdef	movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 8 * SIZE(BB), %xmm3
	xorps	%xmm6, %xmm6
#ifdef	movsd
	xorps	%xmm1, %xmm1
#endif
	movsd	 8 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7

#endif


#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
	addl	$1, %eax
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L72
	ALIGN_4

.L71:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	16 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm4
	movaps	12 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movsd	 4 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	movaps	24 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	20 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 6 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	32 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm4
	movaps	28 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movsd	16 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	movaps	40 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm4
	movaps	36 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movsd	10 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm5
	movaps	48 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movaps	44 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	12 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movaps	56 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm4
	movaps	52 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movsd	14 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm5
	movaps	64 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movaps	60 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
 	movsd	24 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movaps	72 * SIZE(BB), %xmm3

	addl	$16 * SIZE, AA
	addl	$64 * SIZE, BB
	decl	%eax
	jne	.L71
	ALIGN_2
#endif

.L72:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA_R, %xmm1
	movaps	ALPHA_I, %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L74

.L73:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA		# aoffset  += 8
	addl	$8 * SIZE, BB		# boffset1 += 8
	decl	%eax
	jg	.L73

.L74:
	addps	%xmm6, %xmm4
	addps	%xmm7, %xmm5

	shufps	$0xb1, %xmm5, %xmm5

#if defined(NN) || defined(NT) || defined(TN) || defined(TT) || \
    defined(RR) || defined(RC) || defined(CR) || defined(CC)
	subps	%xmm5, %xmm4
#else
	addps	%xmm5, %xmm4
#endif

	movaps	%xmm4, %xmm5

	shufps	$0xb1, %xmm4, %xmm4

	mulps	%xmm1, %xmm5
	mulps	%xmm3, %xmm4

	addps	%xmm5, %xmm4

#ifndef TRMMKERNEL
#ifdef	movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	0 * SIZE(%esi), %xmm0

	addps	%xmm0, %xmm4
#endif

	movlps	%xmm4, 0 * SIZE(%esi)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax,    8), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif

	ALIGN_2

.L99:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$1, KK
#endif

	addl	LDC, C			# c += ldc
	decl	J			# j --
	jg	.L01
	ALIGN_2

.L999:
	movl	OLD_STACK, %esp

	EMMS

	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
