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
#define OFFSET  76(%esp)
#define KK	80(%esp)
#define KKK	84(%esp)
#define BUFFER 128(%esp)

#define B	%edi
#define	LDC	%ebp
#define AA	%edx
#define BB	%ecx

#define STACK_ALIGN	4096
#define STACK_OFFSET	1024

#define PREFETCH     prefetch
#define PREFETCHSIZE  (16 * 17 + 0)

#define RPREFETCHSIZE (16 *  9 + 0)
#define WPREFETCHSIZE (16 *  9 + 0)

#define KERNEL1(address) \
	mulps	%xmm0, %xmm2; \
	PREFETCH (PREFETCHSIZE +  0) * SIZE + (address) * 1 * SIZE(AA); \
	addps	%xmm2, %xmm4; \
	movaps	 4 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	addps	%xmm2, %xmm5; \
	movaps	 8 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	mulps	12 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addps	%xmm2, %xmm6; \
	movaps	32 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addps	%xmm0, %xmm7; \
	movaps	 4 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL2(address) \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm4; \
	movaps	20 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm5; \
	movaps	24 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	mulps	28 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addps	%xmm3, %xmm6; \
	movaps	48 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addps	%xmm0, %xmm7; \
	movaps	 8 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL3(address) \
	mulps	%xmm0, %xmm2; \
	addps	%xmm2, %xmm4; \
	movaps	36 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	addps	%xmm2, %xmm5; \
	movaps	40 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm0, %xmm2; \
	mulps	44 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addps	%xmm2, %xmm6; \
	movaps	64 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addps	%xmm0, %xmm7; \
	movaps	12 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL4(address) \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm4; \
	movaps	52 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	addps	%xmm3, %xmm5; \
	movaps	56 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm0, %xmm3; \
	mulps	60 * SIZE + (address) * 4 * SIZE(BB), %xmm0; \
	addps	%xmm3, %xmm6; \
	movaps	80 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addps	%xmm0, %xmm7; \
	movaps	 32 * SIZE + (address) * 1 * SIZE(AA), %xmm0

#define KERNEL5(address) \
	PREFETCH (PREFETCHSIZE + 16) * SIZE + (address) * 1 * SIZE(AA); \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm4; \
	movaps	68 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm5; \
	movaps	72 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	mulps	76 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addps	%xmm2, %xmm6; \
	movaps	 96 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addps	%xmm1, %xmm7; \
	movaps	20 * SIZE + (address) * 1 * SIZE(AA), %xmm1

#define KERNEL6(address) \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm4; \
	movaps	84 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm5; \
	movaps	88 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	mulps	92 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addps	%xmm3, %xmm6; \
	movaps	112 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addps	%xmm1, %xmm7; \
	movaps	 24 * SIZE + (address) * 1 * SIZE(AA), %xmm1

#define KERNEL7(address) \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm4; \
	movaps	100 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	addps	%xmm2, %xmm5; \
	movaps	104 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	mulps	%xmm1, %xmm2; \
	mulps	108 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addps	%xmm2, %xmm6; \
	movaps	128 * SIZE + (address) * 4 * SIZE(BB), %xmm2; \
	addps	%xmm1, %xmm7; \
	movaps	28 * SIZE + (address) * 1 * SIZE(AA), %xmm1

#define KERNEL8(address) \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm4; \
	movaps	116 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	addps	%xmm3, %xmm5; \
	movaps	120 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	mulps	%xmm1, %xmm3; \
	mulps	124 * SIZE + (address) * 4 * SIZE(BB), %xmm1; \
	addps	%xmm3, %xmm6; \
	movaps	144 * SIZE + (address) * 4 * SIZE(BB), %xmm3; \
	addps	%xmm1, %xmm7; \
	movaps	 48 * SIZE + (address) * 1 * SIZE(AA), %xmm1;

	PROLOGUE

	pushl	%ebp
	pushl	%edi
	pushl	%esi
	pushl	%ebx

	PROFCODE

	movl	%esp, %esi	# save old stack

	subl	$128 + LOCAL_BUFFER_SIZE + STACK_OFFSET, %esp
	andl	$-STACK_ALIGN, %esp	# align stack
	addl	$STACK_OFFSET, %esp

	STACK_TOUCHING

	movl	STACK_M, %ebx
	movl	STACK_N, %eax
	movl	STACK_K, %ecx
	movl	STACK_A, %edx

	movl	%ebx, M
	movl	%eax, N
	movl	%ecx, K
	movl	%edx, A
	movl	%esi, OLD_STACK

	movl	STACK_B, %edi
	movl	STACK_C, %ebx
#ifdef TRMMKERNEL
	movss	STACK_OFFT, %xmm4
#endif

	movss	STACK_ALPHA_R, %xmm0
	movss	STACK_ALPHA_I, %xmm1

	xorps	%xmm7, %xmm7
	cmpeqps	%xmm7, %xmm7
	pslld	$31, %xmm7	# Generate mask
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

	EMMS

	movl	%ebx, C
	movl	STACK_LDC, LDC

#ifdef TRMMKERNEL
	movss	%xmm4, OFFSET
	movss	%xmm4, KK
#ifndef LEFT
	negl	KK
#endif
#endif

	sall	$ZBASE_SHIFT, LDC
	movl	%eax, J			# j = n
	sarl	$1, J
	jle	.L100
	ALIGN_4

.L01:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

/* Copying to Sub Buffer */
	leal	BUFFER, %ecx

	movaps	POSINV, %xmm7

	movl	K, %eax
	sarl	$1, %eax
	jle	.L03
	ALIGN_4

.L02:
	prefetch	 (RPREFETCHSIZE +  0)  * SIZE(%edi)

	movaps	 0 * SIZE(%edi), %xmm3

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

#if defined(NN) || defined(NT) || defined(NR) || defined(NC)  || \
    defined(TN) || defined(TT) || defined(TR) || defined(TC)
	xorps	 %xmm7, %xmm1
	xorps	 %xmm7, %xmm3
#else
	xorps	 %xmm7, %xmm0
	xorps	 %xmm7, %xmm2
#endif

	prefetchw	 (WPREFETCHSIZE +  0)  * SIZE(%ecx)

	movaps	%xmm0,  0 * SIZE(%ecx)
	movaps	%xmm1,  4 * SIZE(%ecx)
	movaps	%xmm2,  8 * SIZE(%ecx)
	movaps	%xmm3, 12 * SIZE(%ecx)

	movaps	 4 * SIZE(%edi), %xmm3

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

#if defined(NN) || defined(NT) || defined(NR) || defined(NC)  || \
    defined(TN) || defined(TT) || defined(TR) || defined(TC)
	xorps	 %xmm7, %xmm1
	xorps	 %xmm7, %xmm3
#else
	xorps	 %xmm7, %xmm0
	xorps	 %xmm7, %xmm2
#endif

	prefetchw	 (WPREFETCHSIZE +  0)  * SIZE(%ecx)

	movaps	%xmm0, 16 * SIZE(%ecx)
	movaps	%xmm1, 20 * SIZE(%ecx)
	movaps	%xmm2, 24 * SIZE(%ecx)
	movaps	%xmm3, 28 * SIZE(%ecx)

	addl	$  8 * SIZE, %edi
	subl	$-32 * SIZE, %ecx

	decl	%eax
	jne	.L02
	ALIGN_4

.L03:
	movl	K, %eax
	andl	$1, %eax
	BRANCH
	jle	.L05
	ALIGN_4

.L04:
	movaps	 0 * SIZE(%edi), %xmm3

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

#if defined(NN) || defined(NT) || defined(NR) || defined(NC)  || \
    defined(TN) || defined(TT) || defined(TR) || defined(TC)
	xorps	 %xmm7, %xmm1
	xorps	 %xmm7, %xmm3
#else
	xorps	 %xmm7, %xmm0
	xorps	 %xmm7, %xmm2
#endif

	movaps	%xmm0,  0 * SIZE(%ecx)
	movaps	%xmm1,  4 * SIZE(%ecx)
	movaps	%xmm2,  8 * SIZE(%ecx)
	movaps	%xmm3, 12 * SIZE(%ecx)

	addl	$ 4 * SIZE, %edi
	ALIGN_4

.L05:
	movl	C,  %esi
	movl	A,  %edx
	movl	M,  %ebx
	sarl	$1, %ebx
	jle	.L30
	ALIGN_4

.L10:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,   8), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 8), BB
#endif

	movaps	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movaps	16 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movaps	 0 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	movaps	16 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

	prefetchw   3 * SIZE(%esi)
	prefetchw   3 * SIZE(%esi, LDC)

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

	andl	$-8, %eax
	sall	$4, %eax
	je	.L15
.L1X:
	KERNEL1(32  *  0)
	KERNEL2(32  *  0)
	KERNEL3(32  *  0)
	KERNEL4(32  *  0)
	KERNEL5(32  *  0)
	KERNEL6(32  *  0)
	KERNEL7(32  *  0)
	KERNEL8(32  *  0)
	cmpl	$128 *  1, %eax
	jle	.L12
	KERNEL1(32  *  1)
	KERNEL2(32  *  1)
	KERNEL3(32  *  1)
	KERNEL4(32  *  1)
	KERNEL5(32  *  1)
	KERNEL6(32  *  1)
	KERNEL7(32  *  1)
	KERNEL8(32  *  1)
	cmpl	$128 *  2, %eax
	jle	.L12
	KERNEL1(32  *  2)
	KERNEL2(32  *  2)
	KERNEL3(32  *  2)
	KERNEL4(32  *  2)
	KERNEL5(32  *  2)
	KERNEL6(32  *  2)
	KERNEL7(32  *  2)
	KERNEL8(32  *  2)
	cmpl	$128 *  3, %eax
	jle	.L12
	KERNEL1(32  *  3)
	KERNEL2(32  *  3)
	KERNEL3(32  *  3)
	KERNEL4(32  *  3)
	KERNEL5(32  *  3)
	KERNEL6(32  *  3)
	KERNEL7(32  *  3)
	KERNEL8(32  *  3)
	cmpl	$128 *  4, %eax
	jle	.L12
	KERNEL1(32  *  4)
	KERNEL2(32  *  4)
	KERNEL3(32  *  4)
	KERNEL4(32  *  4)
	KERNEL5(32  *  4)
	KERNEL6(32  *  4)
	KERNEL7(32  *  4)
	KERNEL8(32  *  4)
	cmpl	$128 *  5, %eax
	jle	.L12
	KERNEL1(32  *  5)
	KERNEL2(32  *  5)
	KERNEL3(32  *  5)
	KERNEL4(32  *  5)
	KERNEL5(32  *  5)
	KERNEL6(32  *  5)
	KERNEL7(32  *  5)
	KERNEL8(32  *  5)
	cmpl	$128 *  6, %eax
	jle	.L12
	KERNEL1(32  *  6)
	KERNEL2(32  *  6)
	KERNEL3(32  *  6)
	KERNEL4(32  *  6)
	KERNEL5(32  *  6)
	KERNEL6(32  *  6)
	KERNEL7(32  *  6)
	KERNEL8(32  *  6)
	cmpl	$128 *  7, %eax
	jle	.L12
	KERNEL1(32  *  7)
	KERNEL2(32  *  7)
	KERNEL3(32  *  7)
	KERNEL4(32  *  7)
	KERNEL5(32  *  7)
	KERNEL6(32  *  7)
	KERNEL7(32  *  7)
	KERNEL8(32  *  7)

	addl	$128 * 8  * SIZE, BB
	addl	$128 * 2  * SIZE, AA
	subl	$128 * 8, %eax
	jg	.L1X
	jmp	.L15

.L12:
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 4), BB
	ALIGN_4

.L15:
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
	ALIGN_4

.L13:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	mulps	12 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm6
	movaps	16 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movaps	 4 * SIZE(AA), %xmm0

	addl	$ 4 * SIZE, AA
	addl	$16 * SIZE, BB
	decl	%eax
	jg	.L13
	ALIGN_4

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

#ifndef TRMMKERNEL
	shufps	$0xe4, %xmm0, %xmm0
	movsd	0 * SIZE(%esi), %xmm0
	movhps	2 * SIZE(%esi), %xmm0

	shufps	$0xe4, %xmm2, %xmm2
	movsd	0 * SIZE(%esi, LDC), %xmm2
	movhps	2 * SIZE(%esi, LDC), %xmm2

	addps	%xmm0, %xmm4
	addps	%xmm2, %xmm6
#endif

	movlps	%xmm4, 0 * SIZE(%esi)
	movhps	%xmm4, 2 * SIZE(%esi)
	movlps	%xmm6, 0 * SIZE(%esi, LDC)
	movhps	%xmm6, 2 * SIZE(%esi, LDC)

#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, 8), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 8), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif

	addl	$4 * SIZE, %esi		# coffset += 4
	decl	%ebx			# i --
	jg	.L10
	ALIGN_4

.L30:
	movl	M,  %ebx
	andl	$1, %ebx
	jle	.L99
	ALIGN_4

.L40:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,    8), %eax
	leal	(AA, %eax,  1), AA
	leal	(BB,  %eax, 8), BB
#endif

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

	movsd	 0 * SIZE(AA), %xmm0
	movsd	 8 * SIZE(AA), %xmm1
	movaps	 0 * SIZE(BB), %xmm2
	movaps	16 * SIZE(BB), %xmm3

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
	je	.L42
	ALIGN_4

.L41:
	mulps	%xmm0, %xmm2
	prefetcht1	(PREFETCHSIZE +  0) * SIZE(AA)
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	mulps	12 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm6
	movaps	32 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movsd	 2 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm4
	movaps	20 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm5
	movaps	24 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	mulps	28 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm6
	movaps	48 * SIZE(BB), %xmm3
	addps	%xmm0, %xmm7
	movsd	 4 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	36 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm5
	movaps	40 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	mulps	44 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm6
	movaps	64 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movsd	 6 * SIZE(AA), %xmm0
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm4
	movaps	52 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm5
	movaps	56 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	mulps	60 * SIZE(BB), %xmm0
	addps	%xmm3, %xmm6
	movaps	80 * SIZE(BB), %xmm3
	addps	%xmm0, %xmm7
	movsd	16 * SIZE(AA), %xmm0
	mulps	%xmm1, %xmm2
	prefetcht1     (PREFETCHSIZE + 16) * SIZE(AA)
	addps	%xmm2, %xmm4
	movaps	68 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm5
	movaps	72 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	mulps	76 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm6
	movaps	96 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm7
	movsd	10 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movaps	84 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm5
	movaps	88 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	mulps	92 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm6
	movaps	112 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movsd	12 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm4
	movaps	100 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm5
	movaps	104 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	mulps	108 * SIZE(BB), %xmm1
	addps	%xmm2, %xmm6
	movaps	128 * SIZE(BB), %xmm2
	addps	%xmm1, %xmm7
	movsd	14 * SIZE(AA), %xmm1
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movaps	116 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm5
	movaps	120 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	mulps	124 * SIZE(BB), %xmm1
	addps	%xmm3, %xmm6
	movaps	144 * SIZE(BB), %xmm3
	addps	%xmm1, %xmm7
	movsd	 24 * SIZE(AA), %xmm1
	addl	$ 16 * SIZE, AA
	addl	$128 * SIZE, BB
	decl	%eax
	jne	.L41
	ALIGN_4

.L42:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA_R, %xmm1
	movaps	ALPHA_I, %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L44
	ALIGN_4

.L43:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	mulps	12 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm6
	movaps	16 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm7
	movsd	 2 * SIZE(AA), %xmm0

	addl	$ 2 * SIZE, AA
	addl	$16 * SIZE, BB
	decl	%eax
	jg	.L43
	ALIGN_4

.L44:
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

#ifndef TRMMKERNEL
	shufps	$0xe4, %xmm4, %xmm4
	shufps	$0xe4, %xmm6, %xmm6

	movsd	0 * SIZE(%esi), %xmm0
	movsd	0 * SIZE(%esi, LDC), %xmm2

	addps	%xmm0, %xmm4
	addps	%xmm2, %xmm6
#endif

	movlps	%xmm4, 0 * SIZE(%esi)
	movlps	%xmm6, 0 * SIZE(%esi, LDC)


#if (defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
    (defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))
	movl	K, %eax
	subl	KKK, %eax
	leal	(,%eax, 8), %eax
	leal	(AA, %eax, 1), AA
	leal	(BB, %eax, 8), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$1, KK
#endif
	ALIGN_4

.L99:
#if defined(TRMMKERNEL) && !defined(LEFT)
	addl	$2, KK
#endif

	leal	(LDC, LDC), %eax
	addl	%eax, C			# c += 2 * ldc
	decl	J			# j --
	jg	.L01
	ALIGN_4

.L100:
	movl	N, %eax
	andl	$1, %eax
	jle	.L999
	ALIGN_4

.L101:
#if defined(TRMMKERNEL) && defined(LEFT)
	movl	OFFSET, %eax
	movl	%eax, KK
#endif

/* Copying to Sub Buffer */
	leal	BUFFER, %ecx
	movaps	POSINV, %xmm7

	movl	K, %eax
	sarl	$2, %eax
	jle	.L103
	ALIGN_4

.L102:
	prefetch	 (RPREFETCHSIZE +  0)  * SIZE(%edi)

	movaps	 0 * SIZE(%edi), %xmm3

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

#if defined(NN) || defined(NT) || defined(NR) || defined(NC)  || \
    defined(TN) || defined(TT) || defined(TR) || defined(TC)
	xorps	 %xmm7, %xmm1
	xorps	 %xmm7, %xmm3
#else
	xorps	 %xmm7, %xmm0
	xorps	 %xmm7, %xmm2
#endif

	prefetchw	 (WPREFETCHSIZE +  0)  * SIZE(%ecx)

	movaps	%xmm0,  0 * SIZE(%ecx)
	movaps	%xmm1,  4 * SIZE(%ecx)
	movaps	%xmm2,  8 * SIZE(%ecx)
	movaps	%xmm3, 12 * SIZE(%ecx)

	movaps	 4 * SIZE(%edi), %xmm3

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1
	pshufd	 $0xaa, %xmm3, %xmm2
	pshufd	 $0xff, %xmm3, %xmm3

#if defined(NN) || defined(NT) || defined(NR) || defined(NC)  || \
    defined(TN) || defined(TT) || defined(TR) || defined(TC)
	xorps	 %xmm7, %xmm1
	xorps	 %xmm7, %xmm3
#else
	xorps	 %xmm7, %xmm0
	xorps	 %xmm7, %xmm2
#endif

	prefetchw	 (WPREFETCHSIZE +  0)  * SIZE(%ecx)

	movaps	%xmm0, 16 * SIZE(%ecx)
	movaps	%xmm1, 20 * SIZE(%ecx)
	movaps	%xmm2, 24 * SIZE(%ecx)
	movaps	%xmm3, 28 * SIZE(%ecx)

	addl	$  8 * SIZE, B
	subl	$-32 * SIZE, BB
	decl	%eax
	jne	.L102
	ALIGN_4

.L103:
	movl	K, %eax
	andl	$3, %eax
	BRANCH
	jle	.L105
	ALIGN_4

.L104:
	movsd	 0 * SIZE(%edi), %xmm3

	pshufd	 $0x00, %xmm3, %xmm0
	pshufd	 $0x55, %xmm3, %xmm1

#if defined(NN) || defined(NT) || defined(NR) || defined(NC)  || \
    defined(TN) || defined(TT) || defined(TR) || defined(TC)
	xorps	 %xmm7, %xmm1
#else
	xorps	 %xmm7, %xmm0
#endif

	movaps	%xmm0,  0 * SIZE(%ecx)
	movaps	%xmm1,  4 * SIZE(%ecx)

	addl	$ 2 * SIZE, %edi
	addl	$ 8 * SIZE, %ecx
	decl	%eax
	jne	.L104
	ALIGN_4

.L105:
	movl	C,  %esi
	movl	A,  AA
	movl	M,  %ebx
	sarl	$1, %ebx
	jle	.L130
	ALIGN_4

.L110:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,    8), %eax
	leal	(AA, %eax,  2), AA
	leal	(BB,  %eax, 4), BB
#endif

	pxor	%xmm4, %xmm4
	pxor	%xmm5, %xmm5
	pxor	%xmm6, %xmm6
	pxor	%xmm7, %xmm7

	movaps	 0 * SIZE(AA), %xmm0
	movaps	16 * SIZE(AA), %xmm1
	movaps	 0 * SIZE(BB), %xmm2
	movaps	16 * SIZE(BB), %xmm3

	prefetchw   3 * SIZE(%esi)

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
	je	.L112
	ALIGN_4

.L111:
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm4
	movaps	 4 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movaps	 4 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm5
	movaps	 8 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	addps	%xmm2, %xmm6
	movaps	12 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm2
	movaps	 8 * SIZE(AA), %xmm0
	addps	%xmm2, %xmm7
	movaps	32 * SIZE(BB), %xmm2
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm4
	movaps	20 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movaps	12  * SIZE(AA), %xmm0
	addps	%xmm3, %xmm5
	movaps	24 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	addps	%xmm3, %xmm6
	movaps	28 * SIZE(BB), %xmm3
	mulps	%xmm0, %xmm3
	movaps	32 * SIZE(AA), %xmm0
	addps	%xmm3, %xmm7
	movaps	48 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm4
	movaps	36 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movaps	20 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm5
	movaps	40 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	addps	%xmm2, %xmm6
	movaps	44 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm2
	movaps	24 * SIZE(AA), %xmm1
	addps	%xmm2, %xmm7
	movaps	64 * SIZE(BB), %xmm2
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm4
	movaps	52 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movaps	28 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm5
	movaps	56 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	addps	%xmm3, %xmm6
	movaps	60 * SIZE(BB), %xmm3
	mulps	%xmm1, %xmm3
	movaps	48 * SIZE(AA), %xmm1
	addps	%xmm3, %xmm7
	movaps	80 * SIZE(BB), %xmm3

	addl	$ 32 * SIZE, AA
	addl	$ 64 * SIZE, BB
	decl	%eax
	jne	.L111
	ALIGN_4

.L112:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA_R, %xmm1
	movaps	ALPHA_I, %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L114
	ALIGN_4

.L113:
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	 8 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movaps	 4 * SIZE(AA), %xmm0

	addl	$ 4 * SIZE, AA
	addl	$ 8 * SIZE, BB
	decl	%eax
	jg	.L113
	ALIGN_4

.L114:
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
	shufps	$0xe4, %xmm4, %xmm4

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
	leal	(,%eax, 8), %eax
	leal	(AA, %eax, 2), AA
	leal	(BB, %eax, 4), BB
#endif

#if defined(TRMMKERNEL) && defined(LEFT)
	addl	$2, KK
#endif

	addl	$4 * SIZE, %esi		# coffset += 4
	decl	%ebx			# i --
	jg	.L110
	ALIGN_4

.L130:
	movl	M,  %ebx
	andl	$1, %ebx
	jle	.L999
	ALIGN_4

.L140:
#if !defined(TRMMKERNEL) || \
	(defined(TRMMKERNEL) &&  defined(LEFT) &&  defined(TRANSA)) || \
	(defined(TRMMKERNEL) && !defined(LEFT) && !defined(TRANSA))

	leal	BUFFER, BB	# boffset1 = boffset
#else
	leal	BUFFER, BB	# boffset1 = boffset
	movl	KK, %eax
	leal	(, %eax,    8), %eax
	leal	(AA, %eax,  1), AA
	leal	(BB,  %eax, 4), BB
#endif

	movsd	 0 * SIZE(AA), %xmm0
	pxor	%xmm4, %xmm4
	movsd	 8 * SIZE(AA), %xmm1
	pxor	%xmm5, %xmm5
	movaps	 0 * SIZE(BB), %xmm2
	pxor	%xmm6, %xmm6
	movaps	16 * SIZE(BB), %xmm3
	pxor	%xmm7, %xmm7

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
	je	.L142
	ALIGN_4

.L141:
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

	addl	$ 16 * SIZE, AA
	addl	$ 64 * SIZE, BB
	decl	%eax
	jne	.L141
	ALIGN_4

.L142:
#ifndef TRMMKERNEL
	movl	K, %eax
#else
	movl	KKK, %eax
#endif
	movaps	ALPHA_R, %xmm1
	movaps	ALPHA_I, %xmm3
	andl	$7, %eax		# if (k & 1)
	BRANCH
	je .L144
	ALIGN_4

.L143:
	mulps	%xmm0, %xmm2
	mulps	 4 * SIZE(BB), %xmm0
	addps	%xmm2, %xmm4
	movaps	 8 * SIZE(BB), %xmm2
	addps	%xmm0, %xmm5
	movsd	 2 * SIZE(AA), %xmm0

	addl	$2 * SIZE, AA
	addl	$8 * SIZE, BB
	decl	%eax
	jg	.L143
	ALIGN_4

.L144:
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
	shufps	$0xe4, %xmm4, %xmm4

	movsd	0 * SIZE(%esi), %xmm0

	addps	%xmm0, %xmm4
#endif

	movlps	%xmm4, 0 * SIZE(%esi)
	ALIGN_4

.L999:
	EMMS

	movl	OLD_STACK, %esp
	popl	%ebx
	popl	%esi
	popl	%edi
	popl	%ebp
	ret

	EPILOGUE
