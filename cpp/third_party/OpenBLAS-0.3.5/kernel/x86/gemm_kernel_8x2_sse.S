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
#define STACK_ALPHA	16 + STACK + ARGS(%esi)
#define STACK_A	20 + STACK + ARGS(%esi)
#define STACK_B	24 + STACK + ARGS(%esi)
#define STACK_C	28 + STACK + ARGS(%esi)
#define STACK_LDC	32 + STACK + ARGS(%esi)
#define STACK_OFFT	36 + STACK + ARGS(%esi)

#define ALPHA	 0(%esp)
#define K	16(%esp)
#define N	20(%esp)
#define M	24(%esp)
#define A	28(%esp)
#define C	32(%esp)
#define J	36(%esp)
#define OLD_STACK 40(%esp)
#define OFFSET  44(%esp)
#define KK	48(%esp)
#define KKK	52(%esp)
#define BUFFER 128(%esp)

#define B	%edi
#define LDC	%ebp

#define STACK_ALIGN	4096
#define STACK_OFFSET	1024

#define PREFETCHSIZE 48   /* for PIII */

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
	andl	$-STACK_ALIGN, %esp
	addl	$STACK_OFFSET, %esp

	STACK_TOUCHING

	movd	STACK_M, %mm0
	movl	STACK_N, %eax
	movd	STACK_K, %mm1
	movd	STACK_A, %mm2
	movd	STACK_ALPHA,  %mm7
	movl	STACK_B, B
	movd	STACK_C, %mm3
	movl	STACK_LDC, LDC
#ifdef TRMMKERNEL
	movd	STACK_OFFT, %mm4
#endif

	movd	%mm7, 0 * SIZE + ALPHA
	movd	%mm7, 1 * SIZE + ALPHA
	movd	%mm7, 2 * SIZE + ALPHA
	movd	%mm7, 3 * SIZE + ALPHA

	movd	%mm1, K
	movl	%eax, N
	movd	%mm0, M
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

	leal	(, LDC, SIZE), LDC

	sarl	$1, %eax	# j = (n >> 1)
	movl	%eax, J
	jle	.L100
	ALIGN_2

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

/* Copying to Sub Buffer */
	movl	K, %eax
	leal	BUFFER, %ecx
	sarl	$2, %eax
	jle	.L03
	ALIGN_4

.L02:
	movss	0 * SIZE(B), %xmm0
	movss	1 * SIZE(B), %xmm1
	movss	2 * SIZE(B), %xmm2
	movss	3 * SIZE(B), %xmm3
	movss	4 * SIZE(B), %xmm4
	movss	5 * SIZE(B), %xmm5
	movss	6 * SIZE(B), %xmm6
	movss	7 * SIZE(B), %xmm7

	shufps	$0, %xmm0, %xmm0
	shufps	$0, %xmm1, %xmm1
	shufps	$0, %xmm2, %xmm2
	shufps	$0, %xmm3, %xmm3
	shufps	$0, %xmm4, %xmm4
	shufps	$0, %xmm5, %xmm5
	shufps	$0, %xmm6, %xmm6
	shufps	$0, %xmm7, %xmm7

	movaps	%xmm0,  0 * SIZE(%ecx)
	movaps	%xmm1,  4 * SIZE(%ecx)
	movaps	%xmm2,  8 * SIZE(%ecx)
	movaps	%xmm3, 12 * SIZE(%ecx)
	movaps	%xmm4, 16 * SIZE(%ecx)
	movaps	%xmm5, 20 * SIZE(%ecx)
	movaps	%xmm6, 24 * SIZE(%ecx)
	movaps	%xmm7, 28 * SIZE(%ecx)

	prefetcht0     104 * SIZE(B)

	addl	$ 8 * SIZE, B
	addl	$32 * SIZE, %ecx
	decl	%eax
	BRANCH
	jne	.L02
	ALIGN_2

.L03:
	movl	K, %eax
	andl	$3, %eax
	BRANCH
	jle	.L05
	ALIGN_2

.L04:
	movss	0 * SIZE(B), %xmm0
	movss	1 * SIZE(B), %xmm1
	addl	$2 * SIZE, B

	shufps	$0, %xmm0, %xmm0
	shufps	$0, %xmm1, %xmm1

	movaps	%xmm0,  0 * SIZE(%ecx)
	movaps	%xmm1,  4 * SIZE(%ecx)
	addl	$8 * SIZE, %ecx
	decl	%eax
	jne	.L04
	ALIGN_4

.L05:
	movl	C, %esi		# coffset = c
	movl	A, AA		# aoffset = a
	movl	M,  %ebx
	sarl	$3, %ebx	# i = (m >> 2)
	jle	.L30
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

	prefetchnta   7 * SIZE(%esi)
	prefetchnta   7 * SIZE(%esi, %ebp)

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$8, %eax
#else
	addl	$2, %eax
#endif
	movl	%eax, KKK
#endif
	andl	$-8, %eax
	NOBRANCH
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

	addl	$64 * 8  * SIZE, AA
	addl	$64 * 8  * SIZE, BB
	subl	$64 * 8, %eax
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

	prefetchnta   8 * SIZE(%esi)
	prefetchnta   8 * SIZE(%esi, %ebp)

#ifndef TRMMKERNEL
	movl	K, %eax
#elif (defined(LEFT) && !defined(TRANSA)) || (!defined(LEFT) && defined(TRANSA))
	movl	K, %eax
	subl	KK, %eax
	movl	%eax, KKK
#else
	movl	KK, %eax
#ifdef LEFT
	addl	$8, %eax
#else
	addl	$2, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L12
	ALIGN_2

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

	addl   $64 * SIZE, BB
	addl   $64 * SIZE, AA
	decl   %eax
	jne    .L11
	ALIGN_2
#endif

.L12:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L14

.L13:
	movaps	 4 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 0 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm1
	movaps	 4 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm5
	movaps	 4 * SIZE(BB), %xmm1
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm6
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm1
	movaps	 8 * SIZE(AA), %xmm0
	addps	%xmm1, %xmm7

	addl	$8 * SIZE, AA
	addl	$8 * SIZE, BB
	subl	$1, %eax
	jg	.L13
	ALIGN_4

.L14:
	mulps	%xmm3, %xmm4
	mulps	%xmm3, %xmm5
	mulps	%xmm3, %xmm6
	mulps	%xmm3, %xmm7

#ifndef TRMMKERNEL
	shufps	$0xe4, %xmm4, %xmm4
	shufps	$0xe4, %xmm5, %xmm5

	movsd	0 * SIZE(%esi), %xmm0
	movhps	2 * SIZE(%esi), %xmm0
	movsd	4 * SIZE(%esi), %xmm1
	movhps	6 * SIZE(%esi), %xmm1

	shufps	$0xe4, %xmm6, %xmm6
	shufps	$0xe4, %xmm7, %xmm7

	movsd	0 * SIZE(%esi, LDC), %xmm2
	movhps	2 * SIZE(%esi, LDC), %xmm2
	movsd	4 * SIZE(%esi, LDC), %xmm3
	movhps	6 * SIZE(%esi, LDC), %xmm3

 	addps	%xmm0, %xmm4
	addps	%xmm1, %xmm6
	addps	%xmm2, %xmm5
	addps	%xmm3, %xmm7
#endif

	movsd	%xmm4, 0 * SIZE(%esi)
	movhps	%xmm4, 2 * SIZE(%esi)
	movsd	%xmm6, 4 * SIZE(%esi)
	movhps	%xmm6, 6 * SIZE(%esi)

	movsd	%xmm5, 0 * SIZE(%esi, LDC)
	movhps	%xmm5, 2 * SIZE(%esi, LDC)
	movsd	%xmm7, 4 * SIZE(%esi, LDC)
	movhps	%xmm7, 6 * SIZE(%esi, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax,    8), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$8, KK
#endif

	addl	$8 * SIZE, %esi
	BRANCH
	decl	%ebx			# i --
	jg	.L10
	ALIGN_2

.L30:
	movl	M,  %ebx
	andl	$7, %ebx
	jle	.L99

	testl	$4, %ebx
	jle	.L50

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
	addl	$4, %eax
#else
	addl	$2, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L32
	ALIGN_2

.L31:
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

	addl   $32 * SIZE, AA
	addl   $64 * SIZE, BB
	decl   %eax
	jne    .L31
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
	addl	$4, %eax
#else
	addl	$2, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L32
	ALIGN_2

.L31:
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
	movaps	32 * SIZE(AA), %xmm0
	mulps	%xmm1, %xmm2
	mulps	52 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm4
	movaps	64 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm5
	movaps	28 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	mulps	60 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm6
	movaps	72 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movaps	40 * SIZE(AA), %xmm1

	addl   $32 * SIZE, AA
	addl   $64 * SIZE, BB
	decl   %eax
	jne    .L31
	ALIGN_2
#endif

.L32:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L34

.L33:
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	 8 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	 4 * SIZE(AA), %xmm0

	addl	$4 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L33
	ALIGN_4

.L34:
	addps	%xmm6, %xmm4
	addps	%xmm7, %xmm5

	mulps	%xmm3, %xmm4
	mulps	%xmm3, %xmm5

#ifndef TRMMKERNEL
	movsd	0 * SIZE(%esi), %xmm0
	movhps	2 * SIZE(%esi), %xmm0
	addps	%xmm0, %xmm4

	movsd	0 * SIZE(%esi, LDC), %xmm0
	movhps	2 * SIZE(%esi, LDC), %xmm0
	addps	%xmm0, %xmm5
#endif

#ifdef HAVE_SSE2
	movsd	%xmm4, 0 * SIZE(%esi)
	unpckhpd %xmm4, %xmm4
	movsd	%xmm4, 2 * SIZE(%esi)

	movsd	%xmm5, 0 * SIZE(%esi, LDC)
	unpckhpd %xmm5, %xmm5
	movsd	%xmm5, 2 * SIZE(%esi, LDC)
#else
	movlps	%xmm4, 0 * SIZE(%esi)
	movhps	%xmm4, 2 * SIZE(%esi)

	movlps	%xmm5, 0 * SIZE(%esi, LDC)
	movhps	%xmm5, 2 * SIZE(%esi, LDC)
#endif

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax,    8), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$4, KK
#endif

	addl	$4 * SIZE, %esi
	ALIGN_2

.L50:
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
	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 16 * SIZE(BB), %xmm3
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
	addl	$2, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L52
	ALIGN_2

.L51:
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

	addl   $16 * SIZE, AA
	addl   $64 * SIZE, BB
	decl   %eax
	jne    .L51
	ALIGN_2

#else
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
	movaps	 0 * SIZE + BUFFER, %xmm2
	xorps	%xmm4, %xmm4
	movsd	 0 * SIZE(AA), %xmm0
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
	addl	$2, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L52
	ALIGN_2

.L51:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	16 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm6
	movaps	12 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movsd	 4 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm7
	movaps	24 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	20 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 6 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	32 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm6
	movaps	28 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movsd	16 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm7
	movaps	40 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm4
	movaps	36 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movsd	10 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm5
	movaps	48 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm6
	movaps	44 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	12 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm7
	movaps	56 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm4
	movaps	52 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movsd	14 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm5
	movaps	64 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm6
	movaps	60 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	24 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm7
	movaps	72 * SIZE(BB), %xmm3

	addl   $16 * SIZE, AA
	addl   $64 * SIZE, BB
	decl   %eax
	jne    .L51
	ALIGN_2
#endif

.L52:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L54

.L53:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L53
	ALIGN_4

.L54:
	addps	%xmm6, %xmm4
	addps	%xmm7, %xmm5

	mulps	%xmm3, %xmm4
	mulps	%xmm3, %xmm5

#ifndef TRMMKERNEL
#ifdef	movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	0 * SIZE(%esi), %xmm0
	addps	%xmm0, %xmm4
#ifdef	movsd
	xorps	%xmm0, %xmm0
#endif
	movsd	0 * SIZE(%esi, LDC), %xmm0
	addps	%xmm0, %xmm5
#endif

	movlps	%xmm4, 0 * SIZE(%esi)
	movlps	%xmm5, 0 * SIZE(%esi, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax,    8), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif

	addl	$2 * SIZE, %esi
	ALIGN_2

.L70:
	testl	$1, %ebx
	jle	.L99

#if (L1_DATA_LINESIZE == 64)
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
	movss	 0 * SIZE + BUFFER, %xmm2
	xorps	%xmm4, %xmm4
	movss	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movss	 16 * SIZE + BUFFER, %xmm3
	xorps	%xmm6, %xmm6
	movss	 4 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7
#else
	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax,   4), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 8), BB /* because it's doubled */

	movss	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
	movss	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movss	 16 * SIZE(BB), %xmm3
	xorps	%xmm6, %xmm6
	movss	 4 * SIZE(AA), %xmm1
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
	addl	$1, %eax
#else
	addl	$2, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L72
	ALIGN_2

.L71:
	mulss	%xmm0, %xmm2
	mulss	 4 * SIZE(BB), %xmm0
	addss	%xmm2, %xmm4
	movss	 8 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm5
	movss	 1 * SIZE(AA), %xmm0
	mulss	%xmm0, %xmm2
	mulss	12 * SIZE(BB), %xmm0
	addss	%xmm2, %xmm6
	movss	32 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm7
	movss	 2 * SIZE(AA), %xmm0
	mulss	%xmm0, %xmm3
	mulss	20 * SIZE(BB), %xmm0
	addss	%xmm3, %xmm4
	movss	24 * SIZE(BB), %xmm3
	addss	%xmm0, %xmm5
	movss	 3 * SIZE(AA), %xmm0
	mulss	%xmm0, %xmm3
	mulss	28 * SIZE(BB), %xmm0
	addss	%xmm3, %xmm6
	movss	48 * SIZE(BB), %xmm3
	addss	%xmm0, %xmm7
	movss	 8 * SIZE(AA), %xmm0
	mulss	%xmm1, %xmm2
	mulss	36 * SIZE(BB), %xmm1
	addss	%xmm2, %xmm4
	movss	40 * SIZE(BB), %xmm2
	addss	%xmm1, %xmm5
	movss	 5 * SIZE(AA), %xmm1
	mulss	%xmm1, %xmm2
	mulss	44 * SIZE(BB), %xmm1
	addss	%xmm2, %xmm6
	movss	64 * SIZE(BB), %xmm2
	addss	%xmm1, %xmm7
	movss	 6 * SIZE(AA), %xmm1
	mulss	%xmm1, %xmm3
	mulss	52 * SIZE(BB), %xmm1
	addss	%xmm3, %xmm4
	movss	56 * SIZE(BB), %xmm3
	addss	%xmm1, %xmm5
	movss	 7 * SIZE(AA), %xmm1
	mulss	%xmm1, %xmm3
	mulss	60 * SIZE(BB), %xmm1
	addss	%xmm3, %xmm6
	movss	80 * SIZE(BB), %xmm3
	addss	%xmm1, %xmm7
	movss	12 * SIZE(AA), %xmm1

	addl   $ 8 * SIZE, AA
	addl   $64 * SIZE, BB
	decl   %eax
	jne    .L71
	ALIGN_2

#else
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
	movss	 0 * SIZE + BUFFER, %xmm2
	xorps	%xmm4, %xmm4
	movss	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movss	 8 * SIZE + BUFFER, %xmm3
	xorps	%xmm6, %xmm6
	movss	 4 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7
#else
	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax,   4), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 8), BB /* because it's doubled */

	movss	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
	movss	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movss	 8 * SIZE(BB), %xmm3
	xorps	%xmm6, %xmm6
	movss	 4 * SIZE(AA), %xmm1
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
	addl	$1, %eax
#else
	addl	$2, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L72
	ALIGN_2

.L71:
	mulss	%xmm0, %xmm2
	mulss	 4 * SIZE(BB), %xmm0
	addss	%xmm2, %xmm4
	movss	16 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm5
	movss	 1 * SIZE(AA), %xmm0
	mulss	%xmm0, %xmm3
	mulss	12 * SIZE(BB), %xmm0
	addss	%xmm3, %xmm6
	movss	24 * SIZE(BB), %xmm3
	addss	%xmm0, %xmm7
	movss	 2 * SIZE(AA), %xmm0
	mulss	%xmm0, %xmm2
	mulss	20 * SIZE(BB), %xmm0
	addss	%xmm2, %xmm4
	movss	32 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm5
	movss	 3 * SIZE(AA), %xmm0
	mulss	%xmm0, %xmm3
	mulss	28 * SIZE(BB), %xmm0
	addss	%xmm3, %xmm6
	movss	40 * SIZE(BB), %xmm3
	addss	%xmm0, %xmm7
	movss	 8 * SIZE(AA), %xmm0
	mulss	%xmm1, %xmm2
	mulss	36 * SIZE(BB), %xmm1
	addss	%xmm2, %xmm4
	movss	48 * SIZE(BB), %xmm2
	addss	%xmm1, %xmm5
	movss	 5 * SIZE(AA), %xmm1
	mulss	%xmm1, %xmm3
	mulss	44 * SIZE(BB), %xmm1
	addss	%xmm3, %xmm6
	movss	56 * SIZE(BB), %xmm3
	addss	%xmm1, %xmm7
	movss	 6 * SIZE(AA), %xmm1
	mulss	%xmm1, %xmm2
	mulss	52 * SIZE(BB), %xmm1
	addss	%xmm2, %xmm4
	movss	64 * SIZE(BB), %xmm2
	addss	%xmm1, %xmm5
	movss	 7 * SIZE(AA), %xmm1
	mulss	%xmm1, %xmm3
	mulss	60 * SIZE(BB), %xmm1
	addss	%xmm3, %xmm6
	movss	72 * SIZE(BB), %xmm3
	addss	%xmm1, %xmm7
	movss	12 * SIZE(AA), %xmm1

	addl   $ 8 * SIZE, AA
	addl   $64 * SIZE, BB
	decl   %eax
	jne    .L71
	ALIGN_2
#endif

.L72:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movss	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L74

.L73:
	mulss	%xmm0, %xmm2
	mulss	 4 * SIZE(BB), %xmm0
	addss	%xmm2, %xmm4
	movss	 8 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm5
	movss	 1 * SIZE(AA), %xmm0

	addl	$1 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L73
	ALIGN_4

.L74:
	addss	%xmm6, %xmm4
	addss	%xmm7, %xmm5

	mulss	%xmm3, %xmm4
	mulss	%xmm3, %xmm5

#ifndef TRMMKERNEL
	addss	0 * SIZE(%esi), %xmm4
	addss	0 * SIZE(%esi, LDC), %xmm5
#endif

	movss	%xmm4, 0 * SIZE(%esi)
	movss	%xmm5, 0 * SIZE(%esi, LDC)

	addl	$1 * SIZE, %esi

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax,    4), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 8), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif
	ALIGN_2

.L99:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	leal	(, LDC, 2), %eax
	addl	%eax, C			# c += 2 * ldc
	BRANCH
	decl	J			# j --
	jg	.L01
	ALIGN_2

.L100:
	movl	N, %eax
	testl	$1, %eax
	jle	.L999
	ALIGN_2

.L101:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

/* Copying to Sub Buffer */
	movl	K, %eax
	leal	BUFFER, %ecx
	sarl	$3, %eax
	jle	.L103
	ALIGN_4

.L102:
	prefetchnta	96 * SIZE(B)

	movss	0 * SIZE(B), %xmm0
	movss	1 * SIZE(B), %xmm1
	movss	2 * SIZE(B), %xmm2
	movss	3 * SIZE(B), %xmm3
	movss	4 * SIZE(B), %xmm4
	movss	5 * SIZE(B), %xmm5
	movss	6 * SIZE(B), %xmm6
	movss	7 * SIZE(B), %xmm7
	addl	$ 8 * SIZE, B

	shufps	$0, %xmm0, %xmm0
	shufps	$0, %xmm1, %xmm1
	shufps	$0, %xmm2, %xmm2
	shufps	$0, %xmm3, %xmm3
	shufps	$0, %xmm4, %xmm4
	shufps	$0, %xmm5, %xmm5
	shufps	$0, %xmm6, %xmm6
	shufps	$0, %xmm7, %xmm7

	movaps	%xmm0,  0 * SIZE(%ecx)
	movaps	%xmm1,  4 * SIZE(%ecx)
	movaps	%xmm2,  8 * SIZE(%ecx)
	movaps	%xmm3, 12 * SIZE(%ecx)
	movaps	%xmm4, 16 * SIZE(%ecx)
	movaps	%xmm5, 20 * SIZE(%ecx)
	movaps	%xmm6, 24 * SIZE(%ecx)
	movaps	%xmm7, 28 * SIZE(%ecx)
	addl	$32 * SIZE, %ecx

	decl	%eax
	BRANCH
	jne	.L102
	ALIGN_2

.L103:
	movl	K, %eax
	andl	$7, %eax
	BRANCH
	jle	.L105
	ALIGN_2

.L104:
	movss	0 * SIZE(B), %xmm0
	addl	$1 * SIZE, B

	shufps	$0, %xmm0, %xmm0

	movaps	%xmm0,  0 * SIZE(%ecx)
	addl	$4 * SIZE, %ecx
	decl	%eax
	jne	.L104
	ALIGN_4

.L105:
	movl	C, %esi		# coffset = c
	movl	A, AA		# aoffset = a
	movl	M,  %ebx
	sarl	$3, %ebx	# i = (m >> 2)
	jle	.L130
	ALIGN_4

.L110:
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
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 2), BB /* because it's doubled */

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
	addl	$8, %eax
#else
	addl	$1, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L112
	ALIGN_2

.L111:
	mulps	%xmm2, %xmm0
	mulps	 4 * SIZE(AA), %xmm2
	addps	%xmm0, %xmm4
	movaps	 8 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm2, %xmm0
	mulps	12 * SIZE(AA), %xmm2
	addps	%xmm0, %xmm6
	movaps	32 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm7
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm2, %xmm1
	mulps	20 * SIZE(AA), %xmm2
	addps	%xmm1, %xmm4
	movaps	24 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm5
	movaps	12 * SIZE(BB), %xmm2
	mulps	%xmm2, %xmm1
	mulps	28 * SIZE(AA), %xmm2
	addps	%xmm1, %xmm6
	movaps	48 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm7
	movaps	32 * SIZE(BB), %xmm2
	mulps	%xmm3, %xmm0
	mulps	36 * SIZE(AA), %xmm3
	addps	%xmm0, %xmm4
	movaps	40 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	movaps	20 * SIZE(BB), %xmm3
	mulps	%xmm3, %xmm0
	mulps	44 * SIZE(AA), %xmm3
	addps	%xmm0, %xmm6
	movaps	64 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm7
	movaps	24 * SIZE(BB), %xmm3
	mulps	%xmm3, %xmm1
	mulps	52 * SIZE(AA), %xmm3
	addps	%xmm1, %xmm4
	movaps	56 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movaps	28 * SIZE(BB), %xmm3
	mulps	%xmm3, %xmm1
	mulps	60 * SIZE(AA), %xmm3
	addps	%xmm1, %xmm6
	movaps	80 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm7
	movaps	48 * SIZE(BB), %xmm3

	addl   $64 * SIZE, AA
	addl   $32 * SIZE, BB
	decl   %eax
	jne    .L111
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
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 2), BB /* because it's doubled */

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
	addl	$8, %eax
#else
	addl	$1, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L112
	ALIGN_2

.L111:
	mulps	%xmm2, %xmm0
	mulps	 4 * SIZE(AA), %xmm2
	addps	%xmm0, %xmm4
	movaps	16 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm2, %xmm1
	mulps	12 * SIZE(AA), %xmm2
	addps	%xmm1, %xmm6
	movaps	24 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm7
	movaps	16 * SIZE(BB), %xmm2
	mulps	%xmm3, %xmm0
	mulps	20 * SIZE(AA), %xmm3
	addps	%xmm0, %xmm4
	movaps	32 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	movaps	12 * SIZE(BB), %xmm3
	mulps	%xmm3, %xmm1
	mulps	28 * SIZE(AA), %xmm3
	addps	%xmm1, %xmm6
	movaps	40 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm7
	movaps	24 * SIZE(BB), %xmm3
	mulps	%xmm2, %xmm0
	mulps	36 * SIZE(AA), %xmm2
	addps	%xmm0, %xmm4
	movaps	48 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	20 * SIZE(BB), %xmm2
	mulps	%xmm2, %xmm1
	mulps	44 * SIZE(AA), %xmm2
	addps	%xmm1, %xmm6
	movaps	56 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm7
	movaps	32 * SIZE(BB), %xmm2
	mulps	%xmm3, %xmm0
	mulps	52 * SIZE(AA), %xmm3
	addps	%xmm0, %xmm4
	movaps	64 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	movaps	28 * SIZE(BB), %xmm3
	mulps	%xmm3, %xmm1
	mulps	60 * SIZE(AA), %xmm3
	addps	%xmm1, %xmm6
	movaps	72 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm7
	movaps	40 * SIZE(BB), %xmm3

	addl   $64 * SIZE, AA
	addl   $32 * SIZE, BB
	decl   %eax
	jne    .L111
	ALIGN_2
#endif

.L112:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L114

.L113:
	movaps	 0 * SIZE(BB), %xmm2
	movaps	 0 * SIZE(AA), %xmm0
	mulps	%xmm2, %xmm0
	addps	%xmm0, %xmm4
	mulps	 4 * SIZE(AA), %xmm2
	addps	%xmm2, %xmm5

	addl	$8 * SIZE, AA
	addl	$4 * SIZE, BB
	subl	$1, %eax
	jg	.L113
	ALIGN_4

.L114:
	addps	%xmm6, %xmm4
	addps	%xmm7, %xmm5

	mulps	%xmm3, %xmm4
	mulps	%xmm3, %xmm5

#ifndef TRMMKERNEL
	movsd	0 * SIZE(%esi), %xmm0
	movhps	2 * SIZE(%esi), %xmm0
	addps	%xmm0, %xmm4

	movsd	4 * SIZE(%esi), %xmm0
	movhps	6 * SIZE(%esi), %xmm0
	addps	%xmm0, %xmm5
#endif

#ifdef HAVE_SSE2
	movsd	%xmm4, 0 * SIZE(%esi)
	unpckhpd %xmm4, %xmm4
	movsd	%xmm4, 2 * SIZE(%esi)

	movsd	%xmm5, 4 * SIZE(%esi)
	unpckhpd %xmm5, %xmm5
	movsd	%xmm5, 6 * SIZE(%esi)
#else
	movlps	%xmm4, 0 * SIZE(%esi)
	movhps	%xmm4, 2 * SIZE(%esi)

	movlps	%xmm5, 4 * SIZE(%esi)
	movhps	%xmm5, 6 * SIZE(%esi)
#endif

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax,    8), %eax
	leal	(AA, %eax, 4), AA
	leal	(BB, %eax, 2), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$8, KK
#endif

	addl	$8 * SIZE, %esi
	BRANCH
	decl	%ebx			# i --
	jg	.L110
	ALIGN_2

.L130:
	movl	M,  %ebx
	andl	$7, %ebx
	jle	.L999

	testl	$4, %ebx
	jle	.L150

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
	leal	(BB, %eax, 2), BB /* because it's doubled */

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
	addl	$4, %eax
#else
	addl	$1, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L132
	ALIGN_2

.L131:
	mulps	%xmm0, %xmm2
	movaps	 4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	mulps	 4 * SIZE(BB), %xmm0
	movaps	32 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	 8 * SIZE(AA), %xmm0
	mulps	 8 * SIZE(BB), %xmm0
	addps	%xmm0, %xmm6
	movaps	12 * SIZE(AA), %xmm0
	mulps	12 * SIZE(BB), %xmm0
	addps	%xmm0, %xmm7
	movaps	32 * SIZE(AA), %xmm0
	mulps	%xmm1, %xmm3
	movaps	20 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm4
	mulps	20 * SIZE(BB), %xmm1
	movaps	48 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm5
	movaps	24 * SIZE(AA), %xmm1
	mulps	24 * SIZE(BB), %xmm1
	addps	%xmm1, %xmm6
	movaps	28 * SIZE(AA), %xmm1
	mulps	28 * SIZE(BB), %xmm1
	addps	%xmm1, %xmm7
	movaps	48 * SIZE(AA), %xmm1

	addl   $32 * SIZE, AA
	addl   $32 * SIZE, BB
	decl   %eax
	jne    .L131
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
	leal	(BB, %eax, 2), BB /* because it's doubled */

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
	je	.L132
	ALIGN_2

.L131:
	mulps	%xmm0, %xmm2
	movaps	 4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	mulps	 4 * SIZE(BB), %xmm0
	movaps	16 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	16 * SIZE(AA), %xmm0
	mulps	%xmm1, %xmm3
	movaps	12 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm6
	mulps	12 * SIZE(BB), %xmm1
	movaps	24 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movaps	24 * SIZE(AA), %xmm1
	mulps	%xmm0, %xmm2
	movaps	20 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	mulps	20 * SIZE(BB), %xmm0
	movaps	32 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	32 * SIZE(AA), %xmm0
	mulps	%xmm1, %xmm3
	movaps	28 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm6
	mulps	28 * SIZE(BB), %xmm1
	movaps	40 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movaps	40 * SIZE(AA), %xmm1

	addl   $32 * SIZE, AA
	addl   $32 * SIZE, BB
	decl   %eax
	jne    .L131
	ALIGN_2
#endif

.L132:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L134

.L133:
	movaps	 0 * SIZE(BB), %xmm2
	movaps	 0 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4

	addl	$4 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L133
	ALIGN_4

.L134:
	addps	%xmm5, %xmm4
	addps	%xmm7, %xmm6
	addps	%xmm6, %xmm4

	mulps	%xmm3, %xmm4

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
	leal	(BB, %eax, 2), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$4, KK
#endif
	addl	$4 * SIZE, %esi
	ALIGN_2

.L150:
	testl	$2, %ebx
	jle	.L170

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
	movaps	 8 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7
#else
	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 2), BB /* because it's doubled */

	movaps	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
	movaps	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movaps	 16 * SIZE(BB), %xmm3
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
	je	.L152
	ALIGN_2

.L151:
	mulps	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 6 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm6
	movaps	12 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	16 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm7
	movaps	32 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm3
	movsd	10 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm4
	movaps	20 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	12 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movaps	24 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	14 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm6
	movaps	28 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	24 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm7
	movaps	48 * SIZE(BB), %xmm3

	addl   $16 * SIZE, AA
	addl   $32 * SIZE, BB
	decl   %eax
	jne    .L151
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
	leal	(BB, %eax, 2), BB /* because it's doubled */

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
	je	.L152
	ALIGN_2

.L151:
	mulps	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movsd	 4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	16 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm3
	movsd	 6 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm6
	movaps	12 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movsd	16 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm7
	movaps	24 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm2
	movsd	10 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm4
	movaps	20 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movsd	12 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm5
	movaps	32 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm3
	movsd	14 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm6
	movaps	28 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movsd	24 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm7
	movaps	40 * SIZE(BB), %xmm3

	addl   $16 * SIZE, AA
	addl   $32 * SIZE, BB
	decl   %eax
	jne    .L151
	ALIGN_2
#endif

.L152:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L154

.L153:
	mulps	%xmm0, %xmm2
	movsd	 2 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2

	addl	$2 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L153
	ALIGN_4

.L154:
	addps	%xmm5, %xmm4
	addps	%xmm7, %xmm6
	addps	%xmm6, %xmm4

	mulps	%xmm3, %xmm4

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
	leal	(BB, %eax, 2), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif
	addl	$2 * SIZE, %esi
	ALIGN_2

.L170:
	testl	$1, %ebx
	jle	.L999

#if (L1_DATA_LINESIZE == 64)
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
	movss	 0 * SIZE + BUFFER, %xmm2
	xorps	%xmm4, %xmm4
	movss	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movss	 16 * SIZE + BUFFER, %xmm3
	xorps	%xmm6, %xmm6
	movss	 4 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7
#else
	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax,   4), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB /* because it's doubled */

	movss	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
	movss	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movss	 16 * SIZE(BB), %xmm3
	xorps	%xmm6, %xmm6
	movss	 4 * SIZE(AA), %xmm1
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
	addl	$1, %eax
#else
	addl	$1, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L172
	ALIGN_2

.L171:
	mulss	%xmm0, %xmm2
	movss	 1 * SIZE(AA), %xmm0
	addss	%xmm2, %xmm4
	mulss	 4 * SIZE(BB), %xmm0
	movss	32 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm5
	movss	 2 * SIZE(AA), %xmm0
	mulss	 8 * SIZE(BB), %xmm0
	addss	%xmm0, %xmm6
	movss	 3 * SIZE(AA), %xmm0
	mulss	12 * SIZE(BB), %xmm0
	addss	%xmm0, %xmm7
	movss	 8 * SIZE(AA), %xmm0
	mulss	%xmm1, %xmm3
	movss	 5 * SIZE(AA), %xmm1
	addss	%xmm3, %xmm4
	mulss	20 * SIZE(BB), %xmm1
	movss	48 * SIZE(BB), %xmm3
	addss	%xmm1, %xmm5
	movss	 6 * SIZE(AA), %xmm1
	mulss	24 * SIZE(BB), %xmm1
	addss	%xmm1, %xmm6
	movss	 7 * SIZE(AA), %xmm1
	mulss	28 * SIZE(BB), %xmm1
	addss	%xmm1, %xmm7
	movss	12 * SIZE(AA), %xmm1

	addl   $ 8 * SIZE, AA
	addl   $32 * SIZE, BB
	decl   %eax
	jne    .L171
	ALIGN_2

#else
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB
	movss	 0 * SIZE + BUFFER, %xmm2
	xorps	%xmm4, %xmm4
	movss	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movss	 8 * SIZE + BUFFER, %xmm3
	xorps	%xmm6, %xmm6
	movss	 4 * SIZE(AA), %xmm1
	xorps	%xmm7, %xmm7
#else
	leal	BUFFER, BB
	movl	KK, %eax
	leal	(, %eax,   4), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB /* because it's doubled */

	movss	 0 * SIZE(BB), %xmm2
	xorps	%xmm4, %xmm4
	movss	 0 * SIZE(AA), %xmm0
	xorps	%xmm5, %xmm5
	movss	 8 * SIZE(BB), %xmm3
	xorps	%xmm6, %xmm6
	movss	 4 * SIZE(AA), %xmm1
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
	addl	$1, %eax
#else
	addl	$1, %eax
#endif
	movl	%eax, KKK
#endif
	sarl	$3, %eax
	je	.L172
	ALIGN_2

.L171:
	mulss	%xmm0, %xmm2
	movss	 1 * SIZE(AA), %xmm0
	addss	%xmm2, %xmm4
	mulss	 4 * SIZE(BB), %xmm0
	movss	16 * SIZE(BB), %xmm2
	addss	%xmm0, %xmm5
	movss	 2 * SIZE(AA), %xmm0
	mulss	%xmm0, %xmm3
	movss	 3 * SIZE(AA), %xmm0
	addss	%xmm3, %xmm6
	mulss	12 * SIZE(BB), %xmm0
	movss	24 * SIZE(BB), %xmm3
	addss	%xmm0, %xmm7
	movss	 8 * SIZE(AA), %xmm0
	mulss	%xmm1, %xmm2
	movss	 5 * SIZE(AA), %xmm1
	addss	%xmm2, %xmm4
	mulss	20 * SIZE(BB), %xmm1
	movss	32 * SIZE(BB), %xmm2
	addss	%xmm1, %xmm5
	movss	 6 * SIZE(AA), %xmm1
	mulss	%xmm1, %xmm3
	movss	 7 * SIZE(AA), %xmm1
	addss	%xmm3, %xmm6
	mulss	28 * SIZE(BB), %xmm1
	movss	40 * SIZE(BB), %xmm3
	addss	%xmm1, %xmm7
	movss	12 * SIZE(AA), %xmm1

	addl   $ 8 * SIZE, AA
	addl   $32 * SIZE, BB
	decl   %eax
	jne    .L171
	ALIGN_2
#endif

.L172:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movss	ALPHA,  %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L174

.L173:
	movss	 0 * SIZE(AA), %xmm0
	movss	 0 * SIZE(BB), %xmm2
	mulss	%xmm0, %xmm2
	addss	%xmm2, %xmm4

	addl	$1 * SIZE, AA
	addl	$4 * SIZE, BB
	decl	%eax
	jg	.L173
	ALIGN_4

.L174:
	addss	%xmm5, %xmm4
	addss	%xmm7, %xmm6
	addss	%xmm6, %xmm4
	mulss	%xmm3, %xmm4

#ifndef TRMMKERNEL
	addss	0 * SIZE(%esi), %xmm4
#endif
	movss	%xmm4, 0 * SIZE(%esi)
	ALIGN_2

.L999:
	movl	OLD_STACK, %esp

	EMMS

	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret
	ALIGN_2


	EPILOGUE
